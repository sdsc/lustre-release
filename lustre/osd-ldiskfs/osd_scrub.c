/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2012 Whamcloud, Inc.
 */
/*
 * lustre/osd-ldiskfs/osd_scrub.c
 *
 * Top-level entry points into osd module
 *
 * OI Scrub is part of LFSCK for rebuilding Object Index files when restores
 * MDT from file-level backup.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <lustre/lustre_idl.h>
#include <lustre_disk.h>

#include "osd_internal.h"
#include "osd_oi.h"

static const char osd_scrub[] = "scrub.0";

enum oi_scrub_flags {
        /* The OI files should be rebuild ASAP, the OSD layer scrubber
         * will not fill iteration cache used for up layer LFSCK. */
        OSF_URGENT      = 1 << 0,
};

const char *oi_scrub_flags_names[] = {
        "urgent",
        NULL
};

struct osd_oi_scrub_data {
        /* The formated position for the latest OI scrub started from. */
        __u32   oosd_pos_latest_start;
};

static void osd_scrub_set_start_pos(struct scrub_exec_unit *seu, int dryrun,
                                    __u32 min)
{
        struct scrub_unit       *su   = &seu->seu_unit;
        __u32                    ino0 = 0;
        __u32                    ino1 = 0;
        struct scrub_statistics *ss0;
        struct scrub_statistics *ss1;

        if (dryrun == 0) {
                ss0 = &su->su_repair;
                ss1 = &su->su_dryrun;
        } else {
                ss0 = &su->su_dryrun;
                ss1 = &su->su_repair;
        }

        if (!scrub_key_is_empty(ss0->ss_pos_last_checkpoint)) {
                osd_scrub_str2pos(&ino0, ss0->ss_pos_last_checkpoint);
                ino0++;
        }

        if (ss1->ss_items_failed == 0 &&
            !scrub_key_is_empty(ss1->ss_pos_last_checkpoint)) {
                osd_scrub_str2pos(&ino1, ss1->ss_pos_last_checkpoint);
                ino1++;
        }

        if (ino1 > ino0)
                ino0 = ino1;

        if (!scrub_key_is_empty(ss1->ss_pos_first_inconsistent))
                osd_scrub_str2pos(&ino1, ss1->ss_pos_first_inconsistent);

        if (ino1 > ino0)
                ino0 = ino1;

        if (ino0 < min)
                ino0 = min;

        osd_scrub_pos2str(su->su_pos_latest_start, ino0);
}

static int osd_oi_scrub_prep_policy(const struct lu_env *env,
                                    struct scrub_exec_head *seh,
                                    struct scrub_exec_unit *seu)
{
        struct osd_scrub_it      *osi  =
                                (struct osd_scrub_it *)seh->seh_private;
        struct scrub_unit_desc   *sud  = &seh->seh_head.sh_desc[seu->seu_idx];
        struct scrub_unit        *su   = &seu->seu_unit;
        struct osd_oi_scrub_data *oosd;
        ENTRY;

        OBD_ALLOC_PTR(oosd);
        if (unlikely(oosd == NULL))
                RETURN(-ENOMEM);

        seu->seu_private = oosd;
        osd_scrub_set_start_pos(seu, sud->sud_flags & SUF_DRYRUN,
                                LDISKFS_FIRST_INO(osi->osi_sb));
        osd_scrub_str2pos(&oosd->oosd_pos_latest_start,
                          su->su_pos_latest_start);

        cfs_spin_lock(&seh->seh_lock);
        if (seu->seu_unit.su_flags & OSF_URGENT) {
                if (osi->osi_it_start)
                        osi->osi_pos_urgent = osi->osi_pos_current;
                osi->osi_urgent_mode = 1;
        }

        if (!osi->osi_it_start) {
                if (osi->osi_pos_current > oosd->oosd_pos_latest_start)
                        osi->osi_pos_current = oosd->oosd_pos_latest_start;
        } else {
                if (oosd->oosd_pos_latest_start < osi->osi_pos_current) {
                        oosd->oosd_pos_latest_start = osi->osi_pos_current;
                        osd_scrub_pos2str(su->su_pos_latest_start,
                                          osi->osi_pos_current);
                }
                su->su_time_latest_start = cfs_time_current_sec();
        }
        cfs_spin_unlock(&seh->seh_lock);

        seu->seu_dirty = 1;
        if (osi->osi_urgent_mode && osi->osi_it_start)
                cfs_waitq_broadcast(&osi->osi_thread.t_ctl_waitq);

        RETURN(0);
}

static int osd_oi_scrub_update(const struct lu_env *env,
                               struct osd_scrub_it *osi,
                               const struct osd_idmap_cache *oic)
{
        struct osd_device      *dev    = osi->osi_dev;
        struct dt_device       *dt     = &dev->od_dt_dev;
        struct osd_thread_info *info   = osd_oti_get(env);
        struct lu_fid          *oi_fid = &info->oti_fid;
        struct osd_inode_id    *oi_id  = &info->oti_id;
        struct iam_container   *bag    =
                        &osd_fid2oi(dev, &oic->oic_fid)->oi_dir.od_container;
        struct thandle         *handle = NULL;
        struct iam_path_descr  *ipd;
        struct osd_thandle     *oh;
#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_t               save   = cfs_curproc_cap_pack();
#endif
        int                     rc;
        ENTRY;

        fid_cpu_to_be(oi_fid, &oic->oic_fid);
        osd_id_pack(oi_id, &oic->oic_lid);
        handle = osd_trans_create(env, dt);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for oi update: rc = %d\n", rc);
                RETURN(rc);
        }

        oh = container_of0(handle, struct osd_thandle, ot_super);
        oh->ot_credits = osd_dto_credits_noquota[DTO_INDEX_UPDATE];
        rc = osd_trans_start(env, dt, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for oi update: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        ipd = osd_idx_ipd_get(env, bag);
        if (unlikely(ipd == NULL))
                GOTO(stop, rc = -ENOMEM);

#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_raise(CFS_CAP_SYS_RESOURCE);
#endif
        rc = iam_update(oh->ot_handle, bag, (const struct iam_key *)oi_fid,
                        (struct iam_rec *)oi_id, ipd);
#ifdef HAVE_QUOTA_SUPPORT
        cfs_curproc_cap_unpack(save);
#endif
        osd_ipd_put(env, bag, ipd);

        GOTO(stop, rc);

stop:
        handle->th_result = rc;
        osd_trans_stop(env, handle);
        return rc;
}

static int osd_oi_scrub_exec_policy(const struct lu_env *env,
                                    struct scrub_exec_head *seh,
                                    struct scrub_exec_unit *seu, int rc)
{
        struct osd_scrub_it      *osi  =
                                (struct osd_scrub_it *)seh->seh_private;
        struct osd_oi_scrub_data *oosd =
                                (struct osd_oi_scrub_data *)seu->seu_private;
        struct osd_device        *dev  = osi->osi_dev;
        struct scrub_unit        *su   = &seu->seu_unit;
        struct osd_idmap_cache   *oic  = osi->osi_oicp;
        struct lu_fid            *fid  = &oic->oic_fid;
        struct osd_inode_id      *lid  = &oic->oic_lid;
        struct osd_inode_id      *lid2 = &osd_oti_get(env)->oti_id;
        struct scrub_unit_desc   *sud  = &seh->seh_head.sh_desc[seu->seu_idx];
        struct scrub_statistics  *ss;
        ENTRY;

        if (unlikely(osi->osi_urgent_mode && !(su->su_flags & OSF_URGENT))) {
                su->su_flags |= OSF_URGENT;
                seu->seu_dirty = 1;
        }

        if (lid->oii_ino < oosd->oosd_pos_latest_start &&
            !osi->osi_in_prior)
                RETURN(0);

        seu->seu_new_checked++;
        if (!fid_is_norm(fid))
                RETURN(0);

        if (sud->sud_flags & SUF_DRYRUN)
                ss = &su->su_dryrun;
        else
                ss = &su->su_repair;

        if (rc != 0)
                GOTO(out, rc);

        rc = osd_oi_lookup(osd_oti_get(env), dev, fid, lid2);
        if (rc != 0) {
                if (rc != -ENOENT)
                        GOTO(out, rc);

                /* XXX: Either the inode is removed by others or someone
                 *      just created the inode w/o inserting it into OI yet. */
                RETURN(0);
        }

        if (osd_id_eq(lid, lid2))
                RETURN(0);

        if ((sud->sud_flags & SUF_DRYRUN) && !osi->osi_in_prior) {
                ss->ss_items_updated++;
                if (unlikely(scrub_key_is_empty(ss->ss_pos_first_inconsistent)))
                        osd_scrub_pos2str(ss->ss_pos_first_inconsistent,
                                          lid->oii_ino);
                RETURN(0);
        }

        rc = osd_oi_scrub_update(env, osi, oic);
        if (rc == 0) {
                if (osi->osi_in_prior)
                        ss->ss_items_updated_prior++;
                else
                        ss->ss_items_updated++;
        }

        GOTO(out, rc = (rc == -ENOENT ? 0 : rc));

out:
        if (rc != 0) {
                ss->ss_items_failed++;
                if (unlikely(scrub_key_is_empty(ss->ss_pos_first_inconsistent)))
                        osd_scrub_pos2str(ss->ss_pos_first_inconsistent,
                                          lid->oii_ino);
        }
        return rc;
}

static void osd_oi_scrub_post_policy(const struct lu_env *env,
                                     struct scrub_exec_head *seh,
                                     struct scrub_exec_unit *seu, int rc)
{
        struct scrub_unit_desc   *sud  = &seh->seh_head.sh_desc[seu->seu_idx];
        struct scrub_unit        *su   = &seu->seu_unit;
        struct osd_oi_scrub_data *oosd =
                                (struct osd_oi_scrub_data *)seu->seu_private;
        ENTRY;

        if (rc > 0 && su->su_flags & OSF_URGENT) {
                if ((sud->sud_flags & SUF_DRYRUN &&
                     su->su_dryrun.ss_items_failed == 0) ||
                    (!(sud->sud_flags & SUF_DRYRUN) &&
                       su->su_repair.ss_items_failed == 0)) {
                        su->su_flags &= ~OSF_URGENT;
                        seu->seu_dirty = 1;
                }
        }

        if (oosd != NULL) {
                OBD_FREE_PTR(oosd);
                seu->seu_private = NULL;
        }

        EXIT;
}

static struct scrub_exec_unit *
osd_scrub_register(const struct lu_env *env, struct osd_device *dev,
                   struct osd_scrub_it *osi, struct scrub_exec_head *seh,
                   __u8 status, __u8 flags, int reset, int by_rpc)
{
        struct scrub_exec_unit *seu;
        struct scrub_unit_desc *sud;
        int                     idx;
        int                     rc;
        ENTRY;

        seu = scrub_unit_new(osd_oi_scrub_prep_policy, osd_oi_scrub_exec_policy,
                             osd_oi_scrub_post_policy);
        if (unlikely(seu == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        sud = scrub_desc_find(&seh->seh_head, ST_OI_SCRUB, &idx);
        if (sud != NULL) {
                rc = scrub_unit_load(env, seh, sud->sud_offset, seu);
                if (rc != 0)
                        GOTO(out, rc);

                if ((reset != 0) ||
                    (by_rpc != 0 && !(sud->sud_flags & SUF_DRYRUN) &&
                     sud->sud_status == SS_COMPLETED))
                        scrub_unit_reset(seu);
                if (by_rpc != 0)
                        flags &= ~SUF_DRYRUN;
                flags |= (sud->sud_flags & ~SUF_DRYRUN);
        } else {
                scrub_unit_init(seu, ST_OI_SCRUB);
        }

        /* For the case of scrub triggered by RPC during other LFSCK scanning,
         * prep_policy will not be called, so prep() when registers. */
        if (osi != NULL) {
                rc = osd_oi_scrub_prep_policy(env, seh, seu);
                if (rc != 0)
                        GOTO(out, rc);
        }

        rc = scrub_unit_register(seh, seu, status, flags);
        if (rc != 0 && osi != NULL)
                osd_oi_scrub_post_policy(env, seh, seu, rc);

        GOTO(out, rc);

out:
        if (rc != 0) {
                scrub_unit_free(seu);
                seu = ERR_PTR(rc);
        } else if (flags & SUF_INCONSISTENT) {
                dev->od_fl_restored = 1;
        }
        return seu;
}

static inline struct osd_scrub_it *osi_get(struct osd_scrub_it *osi)
{
        cfs_atomic_inc(&osi->osi_refcount);
        return osi;
}

static inline void osi_put(struct osd_scrub_it *osi)
{
        if (cfs_atomic_dec_and_test(&osi->osi_refcount)) {
                lu_env_fini(&osi->osi_env);
                OBD_FREE_PTR(osi);
        }
}

static int osd_scrub_store_fid(const struct lu_env *env, struct dt_object *obj)
{
        struct osd_thread_info      *info   = osd_oti_get(env);
        struct lu_fid               *fid    = &info->oti_fid;
        struct osd_inode_id         *id     = &info->oti_id;
        struct osd_device           *osd    =
                                osd_dt_dev(lu2dt_dev(obj->do_lu.lo_dev));
        struct super_block          *sb     = osd_sb(osd);
        struct inode                *dir    = sb->s_root->d_inode;
        struct inode                *inode;
        struct dentry               *dentry;
        struct buffer_head          *bh;
        struct ldiskfs_dir_entry_2  *de;
        handle_t                    *jh;
        struct ldiskfs_dentry_param *ldp    =
                                (struct ldiskfs_dentry_param *)info->oti_ldp;
        int                          rc;
        ENTRY;

        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub,
                                           strlen(osd_scrub));
        bh = osd_ldiskfs_find_entry(dir, dentry, &de, NULL);
        if (unlikely(bh == NULL)) {
                CERROR("Fail to find [%s] for scrub_store_fid\n", osd_scrub);
                RETURN(-ENOENT);
        }

        jh = ldiskfs_journal_start_sb(sb, 100);
        if (IS_ERR(jh)) {
                brelse(bh);
                rc = PTR_ERR(jh);
                CERROR("Fail to start trans for scrub_store_fid: "
                       "rc = %d\n", rc);
                RETURN(rc);
        }

        osd_id_gen(id, le32_to_cpu(de->inode), OSD_OII_NOGEN);
        /* Delete the old name entry, which does not contain fid. */
        rc = ldiskfs_delete_entry(jh, dir, de, bh);
        brelse(bh);
        if (rc != 0) {
                ldiskfs_journal_stop(jh);
                CERROR("Fail to delete old name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                RETURN(rc);
        }

        inode = osd_iget(info, osd, id, fid, 0);
        LU_IGIF_BUILD(fid, inode->i_ino, inode->i_generation);
        osd_get_ldiskfs_dirent_param(ldp, (const struct dt_rec *)fid);
        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub,
                                           strlen(osd_scrub));
        dentry->d_fsdata = (void *)ldp;

        /* Insert the name entry with the same name, then the fid will be stored
         * when new entry inserted. */
        rc = osd_ldiskfs_add_entry(jh, dentry, inode, NULL);
        ldiskfs_journal_stop(jh);
        iput(inode);
        if (rc != 0) {
                CERROR("Fail to insert new name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                RETURN(rc);
        }

        RETURN(0);
}

static struct scrub_exec_head *
osd_scrub_init_seh(const struct lu_env *env, struct osd_device *dev,
                   struct dt_object *obj, int restored)
{
        struct scrub_exec_head *seh;
        struct scrub_unit_desc *sud;
        struct scrub_exec_unit *seu = NULL;
        int                     rc;
        ENTRY;

        seh = scrub_head_new(obj);
        if (unlikely(seh == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        cfs_down_write(&seh->seh_rwsem);
        rc = scrub_head_load(env, seh);
        if (rc == -ENOENT) {
                scrub_head_init(seh);
                rc = 0;
        } else if (rc != 0) {
                GOTO(out, rc);
        }

        if (restored != 0) {
                seu = osd_scrub_register(env, dev, NULL, seh, SS_INIT,
                                         SUF_INCONSISTENT, 1, 0);
                if (IS_ERR(seu))
                        GOTO(out, rc = PTR_ERR(seu));
        } else {
                sud = &seh->seh_head.sh_desc[0];
                if (sud->sud_status == SS_SCANNING) {
                        sud->sud_status = SS_CRASHED;
                        seh->seh_dirty = 1;
                }
        }
        rc = scrub_head_store(env, seh);

        GOTO(out, rc);

out:
        if (rc != 0) {
                scrub_head_free(env, seh);
                seh = ERR_PTR(rc);
        } else if (seu != NULL) {
                scrub_unit_degister(seh, seu);
                scrub_unit_free(seu);
        }
        cfs_up_write(&seh->seh_rwsem);
        return seh;
}

static struct dt_object *
osd_scrub_init_obj(const struct lu_env *env, struct dt_device *dt,
                   int *restored)
{
        struct lu_fid               *fid     = &osd_oti_get(env)->oti_fid;
        struct super_block          *sb      = osd_sb(osd_dt_dev(dt));
        struct inode                *dir     = sb->s_root->d_inode;
        struct inode                *inode;
        struct dentry               *dentry;
        struct buffer_head          *bh;
        struct ldiskfs_dir_entry_2  *de;
        handle_t                    *jh;
        struct ldiskfs_dentry_param *ldp     =
                (struct ldiskfs_dentry_param *)osd_oti_get(env)->oti_ldp;
        struct dt_object            *obj;
        int                          rc;
        ENTRY;

        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub,
                                           strlen(osd_scrub));
        bh = osd_ldiskfs_find_entry(dir, dentry, &de, NULL);
        if (bh) {
                rc = osd_get_fid_from_dentry(de, (struct dt_rec *)fid);
                if (rc == -ENODATA)
                        *restored = 1;
                if (rc != 0)
                        rc = osd_ea_fid_get(env, dt,
                                            le32_to_cpu(de->inode), fid);
                brelse(bh);
                if (rc != 0) {
                        CERROR("Fail to find [%s], rc = %d\n", osd_scrub, rc);
                        RETURN(ERR_PTR(rc));
                }
                goto locate;
        }

        jh = ldiskfs_journal_start_sb(sb, 100);
        if (IS_ERR(jh)) {
                CERROR("Fail to start trans for create [%s], rc = %ld\n",
                       osd_scrub, PTR_ERR(jh));
                RETURN((struct dt_object *)jh);
        }

        inode = ldiskfs_create_inode(jh, dir, (S_IFREG | S_IRUGO | S_IWUSR));
        if (IS_ERR(inode)) {
                ldiskfs_journal_stop(jh);
                CERROR("Fail to create [%s], rc = %ld\n",
                       osd_scrub, PTR_ERR(inode));
                RETURN((struct dt_object *)inode);
        }

        LU_IGIF_BUILD(fid, inode->i_ino, inode->i_generation);
        osd_get_ldiskfs_dirent_param(ldp, (const struct dt_rec *)fid);
        dentry->d_fsdata = (void *)ldp;

        rc = osd_ldiskfs_add_entry(jh, dentry, inode, NULL);
        ldiskfs_journal_stop(jh);
        iput(inode);
        if (rc != 0) {
                CERROR("Fail to add entry [%s], rc = %d\n", osd_scrub, rc);
                RETURN(ERR_PTR(rc));
        }

locate:
        obj = dt_locate(env, dt, fid);
        if (unlikely(IS_ERR(obj)))
                CERROR("Fail to locate [%s] fid = "DFID" rc = %ld\n",
                       osd_scrub, PFID(fid), PTR_ERR(obj));

        RETURN(obj);
}

int osd_scrub_setup(const struct lu_env *env, struct dt_device *dt)
{
        struct osd_device      *dev      = osd_dt_dev(dt);
        struct dt_object       *obj;
        struct scrub_exec_head *seh;
        int                     restored = 0;
        int                     rc       = 0;
        ENTRY;

        obj = osd_scrub_init_obj(env, dt, &restored);
        if (IS_ERR(obj))
                RETURN(PTR_ERR(obj));

        seh = osd_scrub_init_seh(env, dev, obj, restored);
        if (IS_ERR(seh))
                GOTO(out, rc = PTR_ERR(seh));

        dev->od_scrub_seh = seh;
        if (restored != 0)
                rc = osd_scrub_store_fid(env, obj);

        GOTO(out, rc);

out:
        lu_object_put(env, &obj->do_lu);
        return rc;
}

void osd_scrub_cleanup(const struct lu_env *env, struct osd_device *dev)
{
        struct scrub_exec_head *seh = dev->od_scrub_seh;

        if (seh != NULL) {
                cfs_down_write(&seh->seh_rwsem);
                scrub_head_free(env, seh);
                cfs_up_write(&seh->seh_rwsem);
                dev->od_scrub_seh = NULL;
        }
}

int osd_oii_insert(struct osd_device *dev, struct osd_idmap_cache *oic)
{
        struct scrub_exec_head       *seh = dev->od_scrub_seh;
        struct osd_scrub_it          *osi = NULL;
        struct osd_inconsistent_item *oii;
        ENTRY;

        cfs_down_read(&seh->seh_rwsem);
        if (!(seh->seh_active & ST_OI_SCRUB)) {
                cfs_up_read(&seh->seh_rwsem);
                RETURN(-EAGAIN);
        }

        OBD_ALLOC_PTR(oii);
        if (unlikely(oii == NULL)) {
                cfs_up_read(&seh->seh_rwsem);
                RETURN(-ENOMEM);
        }

        CFS_INIT_LIST_HEAD(&oii->oii_list);
        oii->oii_cache = *oic;

        cfs_spin_lock(&seh->seh_lock);
        if (cfs_list_empty(&dev->od_inconsistent_items))
                osi = osi_get(seh->seh_private);

        cfs_list_add_tail(&oii->oii_list, &dev->od_inconsistent_items);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_up_read(&seh->seh_rwsem);

        if (osi != NULL) {
                cfs_waitq_broadcast(&osi->osi_thread.t_ctl_waitq);
                osi_put(osi);
        }

        RETURN(0);
}
