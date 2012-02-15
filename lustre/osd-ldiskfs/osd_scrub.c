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
                               const struct osd_idmap_cache *oic, int insert)
{
        struct osd_device      *dev    = osi->osi_dev;
        struct dt_device       *dt     = &dev->od_dt_dev;
        struct osd_thread_info *info   = osd_oti_get(env);
        struct lu_object_conf  *conf   = &info->oti_conf;
        struct lu_fid          *oi_fid = &info->oti_fid;
        struct osd_inode_id    *oi_id  = &info->oti_id;
        struct lu_object       *obj;
        struct lu_object       *tmp;
        struct dt_object       *idx    = osd_fid2oi(dev, &oic->oic_fid)->oi_dir;
        struct iam_container   *bag    = &osd_dt_obj(idx)->oo_dir->od_container;
        struct thandle         *handle = NULL;
        struct iam_path_descr  *ipd;
        struct osd_thandle     *oh;
#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_t               save   = cfs_curproc_cap_pack();
#endif
        int                     rc     = 0;
        int                     skip   = 0;
        ENTRY;

        fid_cpu_to_be(oi_fid, &oic->oic_fid);
        osd_id_pack(oi_id, &oic->oic_lid);
        memcpy(&info->oti_cache, oic, sizeof(*oic));
        conf->loc_flags = LOC_F_SCRUB;
        obj = lu_object_find(env, osd2lu_dev(dev), &oic->oic_fid, conf);
        if (IS_ERR(obj))
                RETURN(PTR_ERR(obj));

        if (unlikely(!lu_object_exists(obj)))
                GOTO(put, skip = 1);

        tmp = lu_object_locate(obj->lo_header, dt->dd_lu_dev.ld_type);
        if (unlikely(tmp == NULL)) {
                LU_OBJECT_DEBUG(D_SCRUB, env, obj, "lu_object can't be located"
                                DFID"\n", PFID(&oic->oic_fid));
                GOTO(put, skip = 1);
        }

        if (osd_obj(tmp)->oo_skip_scrub)
                GOTO(put, skip = 1);

        handle = dt_trans_create(env, dt);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for oi update: rc = %d\n", rc);
                GOTO(out, rc);
        }

        rc = dt_declare_insert(env, idx, (const struct dt_rec *)oi_id,
                               (const struct dt_key *)oi_fid, handle);
        if (rc != 0) {
                CERROR("Fail to declare trans for oi update: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        rc = dt_trans_start(env, dt, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for oi update: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        OSD_EXEC_OP(handle, insert);

        ipd = osd_idx_ipd_get(env, bag);
        if (unlikely(ipd == NULL))
                GOTO(stop, rc = -ENOMEM);

        oh = container_of0(handle, struct osd_thandle, ot_super);
        LASSERT(oh->ot_handle != NULL);
        LASSERT(oh->ot_handle->h_transaction != NULL);

#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_raise(CFS_CAP_SYS_RESOURCE);
#endif
        if (insert == 0)
                rc = iam_update(oh->ot_handle, bag, (const struct iam_key *)oi_fid,
                                (struct iam_rec *)oi_id, ipd);
        else
                rc = iam_insert(oh->ot_handle, bag, (const struct iam_key *)oi_fid,
                                (struct iam_rec *)oi_id, ipd);
#ifdef HAVE_QUOTA_SUPPORT
        cfs_curproc_cap_unpack(save);
#endif
        osd_ipd_put(env, bag, ipd);

        /* Any unlink operation need to lookup the object before unlink it.
         * So nobody can remove it during the OI Scrub. */
        LASSERT(rc <= 0 && rc != -ENOENT);

        GOTO(stop, rc);

stop:
        if (rc == 0) {
                handle->th_oi_scrub = 1;
                info->oti_obj_rebuild = obj;
        }
        handle->th_result = rc;
        dt_trans_stop(env, dt, handle);
out:
        if (rc != 0) {
                lu_object_set_inconsistent(obj);
                lu_object_signal_scrub(obj->lo_header);
        } else if (unlikely(info->oti_obj_rebuild != NULL)) {
                lu_object_signal_scrub(obj->lo_header);
                info->oti_obj_rebuild = NULL;
        }
put:
        if (skip != 0)
                lu_object_signal_scrub(obj->lo_header);
        lu_object_put(env, obj);
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

        rc = osd_oi_lookup(osd_oti_get(env), osd_fid2oi(dev, fid), fid, lid2);
        if (rc != 0) {
                if (rc != -ENOENT)
                        GOTO(out, rc);

                LASSERT(osi->osi_inode != NULL);

                /* It is normal that if the inode is removed by others. */
                if (osd_inode_unlinked(osi->osi_inode))
                        RETURN(0);

                goto update;
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

update:
        rc = osd_oi_scrub_update(env, osi, oic, rc == -ENOENT);
        if (rc == 0) {
                if (osi->osi_in_prior)
                        ss->ss_items_updated_prior++;
                else
                        ss->ss_items_updated++;
        }

        GOTO(out, rc);

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

static inline scrub_prep_policy osd_scrub_prep_policy_table(__u16 type)
{
        switch (type) {
        case ST_OI_SCRUB:
                return osd_oi_scrub_prep_policy;
        default:
                return (scrub_prep_policy)NULL;
        }
}

static inline scrub_exec_policy osd_scrub_exec_policy_table(__u16 type)
{
        switch (type) {
        case ST_OI_SCRUB:
                return osd_oi_scrub_exec_policy;
        default:
                return (scrub_exec_policy)NULL;
        }
}

static inline scrub_post_policy osd_scrub_post_policy_table(__u16 type)
{
        switch (type) {
        case ST_OI_SCRUB:
                return osd_oi_scrub_post_policy;
        default:
                return (scrub_post_policy)NULL;
        }
}

static inline int is_recognized_type(__u16 type)
{
        switch (type) {
        case ST_OI_SCRUB:
                return 1;
        default:
                return 0;
        }
}

static struct scrub_exec_unit *
osd_scrub_register(const struct lu_env *env, struct osd_device *dev,
                   struct osd_scrub_it *osi, struct scrub_exec_head *seh,
                   __u16 type, __u8 status, __u8 flags, int reset, int by_rpc)
{
        scrub_prep_policy       prep = osd_scrub_prep_policy_table(type);
        scrub_post_policy       post = osd_scrub_post_policy_table(type);
        struct scrub_exec_unit *seu;
        struct scrub_unit_desc *sud;
        int                     idx;
        int                     rc;
        ENTRY;

        seu = scrub_unit_new(prep, osd_scrub_exec_policy_table(type), post);
        if (unlikely(seu == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        sud = scrub_desc_find(&seh->seh_head, type, &idx);
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
                scrub_unit_init(seu, type);
        }

        /* For the case of scrub triggered by RPC during other LFSCK scanning,
         * prep_policy will not be called, so prep() when registers. */
        if (osi != NULL) {
                if (prep != NULL) {
                        rc = prep(env, seh, seu);
                        if (rc != 0)
                                GOTO(out, rc);
                }
        }

        rc = scrub_unit_register(seh, seu, status, flags);
        if (rc != 0 && osi != NULL && post != NULL)
                post(env, seh, seu, rc);

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

static struct osd_scrub_it *osi_init(struct osd_device *dev)
{
        struct super_block  *sb   = osd_sb(dev);
        struct osd_scrub_it *osi;
        int                  rc;

        OBD_ALLOC_PTR(osi);
        if (unlikely(osi == NULL))
                return ERR_PTR(-ENOMEM);

        rc = lu_env_init(&osi->osi_env, LCT_MD_THREAD | LCT_DT_THREAD);
        if (rc != 0) {
                OBD_FREE_PTR(osi);
                return ERR_PTR(rc);
        }

        cfs_spin_lock_init(&osi->osi_cache_lock);
        osi->osi_inodes_per_block = LDISKFS_BLOCK_SIZE(sb) /
                                    LDISKFS_INODE_SIZE(sb);
        osi->osi_sb = sb;
        osi->osi_dev = dev;
        cfs_waitq_init(&osi->osi_thread.t_ctl_waitq);
        cfs_atomic_set(&osi->osi_refcount, 1);
        osi->osi_pos_current = ~0U;
        osi->osi_pos_urgent = ~0U;
        return osi;
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

static int osd_scrub_main(void *args)
{
        struct osd_scrub_it          *osi    =
                        osi_get((struct osd_scrub_it *)args);
        struct lu_env                *env    = &osi->osi_env;
        struct osd_device            *dev    = osi->osi_dev;
        struct ptlrpc_thread         *thread = &osi->osi_thread;
        struct scrub_exec_head       *seh    = dev->od_scrub_seh;
        struct l_wait_info            lwi    = { 0 };
        struct super_block           *sb     = osi->osi_sb;
        struct osd_thread_info       *info   = osd_oti_get(env);
        struct osd_inconsistent_item *oii    = NULL;
        __u32                         max    =
                        le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        int                           rc     = 0;
        ENTRY;

        cfs_daemonize("osd_scrub");

        CDEBUG(D_SCRUB, "OSD scrub: flags = 0x%x, pid = %d\n",
               (__u32)seh->seh_head.sh_param_flags, cfs_curproc_pid());

        /* XXX: Prepare before wakeup the sponsor. */
        cfs_down_write(&seh->seh_rwsem);
        rc = scrub_prep(env, seh);

        cfs_spin_lock(&seh->seh_lock);
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (rc != 0)
                GOTO(out, rc);

        cfs_up_write(&seh->seh_rwsem);
        if (!osi->osi_urgent_mode) {
                l_wait_event(thread->t_ctl_waitq,
                             osi->osi_it_start || !thread_is_running(thread),
                             &lwi);
                if (unlikely(!thread_is_running(thread)))
                        GOTO(post, rc = 0);
        } else {
                /* Wakeup the possible up layer iteration. */
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
        }

        CDEBUG(D_SCRUB, "OSD scrub iteration start: pos = %u\n",
               osi->osi_pos_current);

again:
        while (osi->osi_pos_current <= max ||
               !cfs_list_empty(&dev->od_inconsistent_items)) {
                struct buffer_head *bitmap;
                struct inode *inode;
                ldiskfs_group_t bg = (osi->osi_pos_current - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (osi->osi_pos_current - 1) %
                               LDISKFS_INODES_PER_GROUP(sb);
                __u32 gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(sb);
                int noslot = 0;

                bitmap = ldiskfs_read_inode_bitmap(sb, bg);
                if (bitmap == NULL) {
                        CERROR("Fail to read bitmap for pos = %u, bg = %u, "
                               "scrub will stop.\n",
                               osi->osi_pos_current, (__u32)bg);
                        GOTO(post, rc = -EIO);
                }

                while (offset < LDISKFS_INODES_PER_GROUP(sb) ||
                       !cfs_list_empty(&dev->od_inconsistent_items)) {
                        if (OBD_FAIL_CHECK(OBD_FAIL_OSD_SCRUB_FAILURE)) {
                                brelse(bitmap);
                                GOTO(post, rc = -EINVAL);
                        }

                        if (OBD_FAIL_CHECK(OBD_FAIL_OSD_SCRUB_DELAY)) {
                                struct l_wait_info tlwi;

                                tlwi = LWI_TIMEOUT_INTR(cfs_time_seconds(3),
                                                        NULL, NULL, NULL);
                                l_wait_event(thread->t_ctl_waitq,
                                             !cfs_list_empty(
                                                &dev->od_inconsistent_items) ||
                                             !thread_is_running(thread),
                                             &tlwi);
                        }

                        if (unlikely(!thread_is_running(thread))) {
                                brelse(bitmap);
                                GOTO(post, rc = 0);
                        }

                        if (!cfs_list_empty(&dev->od_inconsistent_items)) {
                                oii = cfs_list_entry(
                                                dev->od_inconsistent_items.next,
                                                struct osd_inconsistent_item,
                                                oii_list);
                                osi->osi_oicp = &oii->oii_cache;
                                osi->osi_in_prior = 1;
                                goto exec;
                        }

                        if (offset>= LDISKFS_INODES_PER_GROUP(sb) ||
                            unlikely(osi->osi_pos_current > max))
                                break;

                        if (!osi->osi_urgent_mode && noslot != 0)
                                goto wait;

                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                osi->osi_pos_current = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        if (osi->osi_urgent_mode)
                                osi->osi_oicp = &osi->osi_oic_urgent;
                        else
                                osi->osi_oicp = &osi->osi_producer_slot;
                        osi->osi_pos_current = gbase + offset;
                        osd_id_gen(&osi->osi_oicp->oic_lid,
                                   osi->osi_pos_current, OSD_OII_NOGEN);
                        inode = osd_iget(osi->osi_dev, &osi->osi_oicp->oic_lid,
                                         &osi->osi_oicp->oic_fid,
                                         &info->oti_child_dentry,
                                         &info->oti_mdt_attrs,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        osi->osi_skip_obj = 1;
                                        goto checkpoint;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d\n",
                                       bg, osi->osi_pos_current, rc);
                        } else {
                                osi->osi_inode = inode;
                        }

exec:
                        cfs_down_read(&seh->seh_rwsem);
                        rc = scrub_exec(env, seh, rc);
                        cfs_up_read(&seh->seh_rwsem);

                        if (osi->osi_inode != NULL) {
                                iput(osi->osi_inode);
                                osi->osi_inode = NULL;
                        }

                        if (oii != NULL) {
                                cfs_spin_lock(&seh->seh_lock);
                                cfs_list_del_init(&oii->oii_list);
                                cfs_spin_unlock(&seh->seh_lock);
                                lu_object_put(env, oii->oii_obj);
                                OBD_FREE_PTR(oii);
                                oii = NULL;
                        }

                        if (rc != 0) {
                                brelse(bitmap);
                                GOTO(post, rc);
                        }

checkpoint:
                        if (cfs_time_beforeq(seh->seh_time_next_checkpoint,
                                             cfs_time_current())) {
                                osd_scrub_pos2str(seh->seh_pos_checkpoint,
                                                  osi->osi_pos_current);
                                cfs_down_write(&seh->seh_rwsem);
                                rc = scrub_checkpoint(env, seh);
                                cfs_up_write(&seh->seh_rwsem);
                                if (rc != 0) {
                                        CERROR("Fail to checkpoint: pos = %u, "
                                               "rc = %d\n",
                                               osi->osi_pos_current, rc);
                                        brelse(bitmap);
                                        GOTO(post, rc);
                                }
                        }

                        if (osi->osi_in_prior) {
                                osi->osi_in_prior = 0;
                                continue;
                        }

                        offset++;
                        osi->osi_pos_current = gbase + offset;
                        if (osi->osi_skip_obj) {
                                osi->osi_skip_obj = 0;
                                continue;
                        }

                        if (!osi->osi_urgent_mode) {

wait:
                                l_wait_event(thread->t_ctl_waitq,
                                             osi->osi_cached_items <
                                                OSD_SCRUB_IT_CACHE_SIZE ||
                                             dev->od_fl_restored ||
                                             !cfs_list_empty(
                                                &dev->od_inconsistent_items) ||
                                             !thread_is_running(thread),
                                             &lwi);

                                if (osi->osi_cached_items <
                                        OSD_SCRUB_IT_CACHE_SIZE) {
                                        int wakeup = 0;

                                        noslot = 0;
                                        cfs_spin_lock(&osi->osi_cache_lock);
                                        if (osi->osi_cached_items == 0)
                                                wakeup = 1;
                                        osi->osi_cache[osi->osi_producer_idx] =
                                                osi->osi_producer_slot;
                                        osi->osi_cached_items++;
                                        cfs_spin_unlock(&osi->osi_cache_lock);
                                        if (wakeup != 0)
                                                cfs_waitq_broadcast(
                                                        &thread->t_ctl_waitq);
                                        osi->osi_producer_idx =
                                                (osi->osi_producer_idx + 1) &
                                                ~OSD_SCRUB_IT_CACHE_MASK;
                                        continue;
                                }

                                noslot = 1;
                                if (dev->od_fl_restored) {
                                        cfs_spin_lock(&seh->seh_lock);
                                        osi->osi_pos_urgent =
                                                        osi->osi_pos_current;
                                        osi->osi_urgent_mode = 1;
                                        cfs_spin_unlock(&seh->seh_lock);
                                        cfs_waitq_broadcast(&thread->t_ctl_waitq);
                                        continue;
                                }
                        }
                }
        }

        if (osi->osi_pos_current > max) {
                if (!osi->osi_urgent_mode) {
                        osi->osi_all_cached = 1;
                        cfs_waitq_broadcast(&thread->t_ctl_waitq);
                }
                rc = 1;
        }

        GOTO(post, rc);

post:
        dev->od_fl_restored = 0;
        if (osi->osi_urgent_mode) {
                cfs_spin_lock(&seh->seh_lock);
                osi->osi_urgent_mode = 0;
                cfs_spin_unlock(&seh->seh_lock);
        }
        osd_scrub_pos2str(seh->seh_pos_checkpoint, osi->osi_pos_current);
        cfs_down_write(&seh->seh_rwsem);
        scrub_post(env, seh, rc);
        if (rc == 1 && !osi->osi_all_cached) {
                cfs_up_write(&seh->seh_rwsem);
                /* The up layer iteration preload may be unfinised yet. */
                l_wait_event(thread->t_ctl_waitq,
                             osi->osi_all_cached ||
                             !osi->osi_self_preload ||
                             !cfs_list_empty(&dev->od_inconsistent_items) ||
                             !thread_is_running(thread),
                             &lwi);
                if (osi->osi_all_cached ||
                    unlikely(!thread_is_running(thread))) {
                        cfs_down_write(&seh->seh_rwsem);
                        goto out;
                }

                cfs_spin_lock(&seh->seh_lock);
                if (!osi->osi_self_preload)
                        osi->osi_pos_current = osi->osi_pos_urgent;
                else
                        osi->osi_urgent_mode = 1;
                cfs_spin_unlock(&seh->seh_lock);
                goto again;
        }

        CDEBUG(D_SCRUB, "OSD scrub iteration stop: pos = %u, rc = %d\n",
               osi->osi_pos_current, rc);
out:
        scrub_head_reset(env, seh);
        osi->osi_exit_value = rc;

        while (!cfs_list_empty(&dev->od_inconsistent_items)) {
                struct lu_object *obj;

                oii = cfs_list_entry(dev->od_inconsistent_items.next,
                                     struct osd_inconsistent_item,
                                     oii_list);
                cfs_list_del_init(&oii->oii_list);
                obj = oii->oii_obj;
                lu_object_set_inconsistent(obj);
                lu_object_signal_scrub(obj->lo_header);
                lu_object_put(env, obj);
                OBD_FREE_PTR(oii);
        }

        cfs_spin_lock(&seh->seh_lock);
        thread_set_flags(thread, SVC_STOPPED);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        cfs_up_write(&seh->seh_rwsem);
        osi_put(osi);
        return rc;
}

static int osd_scrub_lookup(const struct lu_env *env, struct dt_object *dt,
                            struct dt_rec *rec, const struct dt_key *key,
                            struct lustre_capa *capa)
{
        return -ENOSYS;
}

static int osd_scrub_declare_insert(const struct lu_env *env,
                                    struct dt_object *dt,
                                    const struct dt_rec *rec,
                                    const struct dt_key *key,
                                    struct thandle *handle)
{
        return -ENOSYS;
}

static int osd_scrub_insert(const struct lu_env *env, struct dt_object *dt,
                            const struct dt_rec *rec, const struct dt_key *key,
                            struct thandle *handle, struct lustre_capa *capa,
                            int ignore_quota)
{
        return -ENOSYS;
}

static int osd_scrub_declare_delete(const struct lu_env *env,
                                    struct dt_object *dt,
                                    const struct dt_key *key,
                                    struct thandle *handle)
{
        return -ENOSYS;
}

static int osd_scrub_delete(const struct lu_env *env, struct dt_object *dt,
                            const struct dt_key *key, struct thandle *handle,
                            struct lustre_capa *capa)
{
        return -ENOSYS;
}

static struct dt_it *osd_scrub_it_init(const struct lu_env *env,
                                       struct dt_object *dt, void *args,
                                       struct lustre_capa *capa)
{
        struct dt_scrub_args   *dsa     = (struct dt_scrub_args *)args;
        struct osd_device      *dev     = osd_dev(dt->do_lu.lo_dev);
        struct scrub_exec_head *seh     = dev->od_scrub_seh;
        struct scrub_head      *sh      = &seh->seh_head;
        struct scrub_unit_desc *sud;
        struct scrub_exec_unit *seu     = NULL;
        struct osd_scrub_it    *osi;
        struct l_wait_info      lwi     = { 0 };
        int                     rc      = 0;
        int                     i;
        int                     dirty   = 1;
        int                     append  = 0;
        int                     rwsem   = 1;
        __u16                   actived = 0;
        ENTRY;

        cfs_down(&seh->seh_sem);
        cfs_down_write(&seh->seh_rwsem);
        osi = seh->seh_private;
        if (osi != NULL) {
                dirty = 0;
                /* XXX: Currently, only OI scrub can be triggering by RPC during
                 *      other LFSCK process. */
                if (dsa->dsa_active != ST_OI_SCRUB ||
                    dsa->dsa_sponsor != SCRUB_TRIGGERED_BY_RPC ||
                    seh->seh_active & ST_OI_SCRUB)
                        GOTO(out, rc = -EALREADY);

                append = 1;
                seu = osd_scrub_register(env, dev, osi, seh, ST_OI_SCRUB,
                                         SS_SCANNING, 0, 0, 1);
                if (IS_ERR(seu))
                        GOTO(out, rc = PTR_ERR(seu));

                actived = ST_OI_SCRUB;
                if (likely(thread_is_running(&osi->osi_thread))) {
                        GOTO(out, rc = 0);
                } else {
                        dirty = 1;
                        goto start;
                }
        }

        if (dsa->dsa_valid & SSV_CHECKPOING_INTERVAL &&
            sh->sh_param_checkpoint_interval !=
            dsa->dsa_param_checkpoint_interval) {
                sh->sh_param_checkpoint_interval =
                        dsa->dsa_param_checkpoint_interval;
                seh->seh_dirty = 1;
        }

        if (dsa->dsa_valid & SSV_ERROR_HANDLE) {
                if ((dsa->dsa_param_flags & SPF_FAILOUT) &&
                   !(sh->sh_param_flags & SPF_FAILOUT)) {
                        sh->sh_param_flags |= SPF_FAILOUT;
                        seh->seh_dirty = 1;
                } else if ((sh->sh_param_flags & SPF_FAILOUT) &&
                          !(dsa->dsa_param_flags & SPF_FAILOUT)) {
                        sh->sh_param_flags &= ~SPF_FAILOUT;
                        seh->seh_dirty = 1;
                }
        }

        if (dsa->dsa_valid & SSV_DRYRUN) {
                if ((dsa->dsa_param_flags & SPF_DRYRUN) &&
                   !(sh->sh_param_flags & SPF_DRYRUN)) {
                        sh->sh_param_flags |= SPF_DRYRUN;
                        seh->seh_dirty = 1;
                } else if ((sh->sh_param_flags & SPF_DRYRUN) &&
                          !(dsa->dsa_param_flags & SPF_DRYRUN)) {
                        sh->sh_param_flags &= ~SPF_DRYRUN;
                        seh->seh_dirty = 1;
                }
        }

        if (dsa->dsa_active == SCRUB_TYPES_DEF) {
                for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                        sud = &sh->sh_desc[i];
                        if (!is_recognized_type(sud->sud_type) ||
                            ((sud->sud_status != SS_CRASHED) &&
                            !(sud->sud_flags & SUF_INCONSISTENT)))
                                continue;

                        seu = osd_scrub_register(env, dev, NULL, seh,
                                                 sud->sud_type, SS_SCANNING,
                                                 sud->sud_flags,
                                                 dsa->dsa_param_flags &
                                                        SPF_RESET,
                                                 dsa->dsa_sponsor ==
                                                        SCRUB_TRIGGERED_BY_RPC);
                        if (IS_ERR(seu))
                                GOTO(out, rc = PTR_ERR(seu));

                        actived |= sud->sud_type;
                }
        } else {
                for (i = 0; i < SCRUB_DESC_COUNT && dsa->dsa_active != 0; i++) {
                        __u16 type = 1 << i;

                        if (!(type & dsa->dsa_active) ||
                            !is_recognized_type(type))
                                continue;

                        seu = osd_scrub_register(env, dev, NULL, seh, type,
                                                 SS_SCANNING,
                                                 dsa->dsa_param_flags &
                                                 SPF_DRYRUN ? SUF_DRYRUN : 0,
                                                 dsa->dsa_param_flags &
                                                        SPF_RESET,
                                                 dsa->dsa_sponsor ==
                                                        SCRUB_TRIGGERED_BY_RPC);
                        if (IS_ERR(seu))
                                GOTO(out, rc = PTR_ERR(seu));

                        actived |= type;
                        dsa->dsa_active &= ~type;
                }
        }

        osi = osi_init(dev);
        if (unlikely(IS_ERR(osi)))
                GOTO(out, rc = PTR_ERR(osi));

        seh->seh_private = osi;

start:
        rc = cfs_create_thread(osd_scrub_main, osi, 0);
        if (rc < 0) {
                CERROR("Cannot start osd scrub thread: rc %d\n", rc);
                if (append == 0) {
                        seh->seh_private = NULL;
                        osi_put(osi);
                }
                GOTO(out, rc);
        }

        cfs_up_write(&seh->seh_rwsem);
        l_wait_event(osi->osi_thread.t_ctl_waitq,
                     thread_is_running(&osi->osi_thread) ||
                     thread_is_stopped(&osi->osi_thread),
                     &lwi);
        rwsem = 0;

        GOTO(out, rc = 0);

out:
        if (rc == 0) {
                dsa->dsa_active = actived;
        } else {
                if (dirty != 0)
                        scrub_head_reset(env, seh);
                osi = ERR_PTR(rc);
        }
        if (rwsem != 0)
                cfs_up_write(&seh->seh_rwsem);
        cfs_up(&seh->seh_sem);
        return (struct dt_it *)osi;
}

static void osd_scrub_it_fini(const struct lu_env *env, struct dt_it *di)
{
        struct osd_scrub_it    *osi    = (struct osd_scrub_it *)di;
        struct scrub_exec_head *seh    = osi->osi_dev->od_scrub_seh;
        struct ptlrpc_thread   *thread = &osi->osi_thread;
        struct l_wait_info      lwi    = { 0 };
        ENTRY;

        cfs_down(&seh->seh_sem);
        LASSERT(seh->seh_private != NULL);

        cfs_spin_lock(&seh->seh_lock);
        if (!thread_is_stopped(thread)) {
                thread_set_flags(thread, SVC_STOPPING);
                cfs_spin_unlock(&seh->seh_lock);
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
        } else {
                cfs_spin_unlock(&seh->seh_lock);
        }
        seh->seh_private = NULL;
        cfs_up(&seh->seh_sem);
        osi_put(osi);

        EXIT;
}

/* Set the OSD layer scrub iteration start position as the specified key.
 *
 * The LFSCK out of OSD layer does not know the detail of the key,
 * so if there are several keys, they cannot be compared out of OSD,
 * so the caller call ".get()" for each key, and OSD will select the
 * smallest one by itself. */
static int osd_scrub_it_get(const struct lu_env *env,
                            struct dt_it *di, const struct dt_key *key)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;
        __u32                ino;
        int                  rc;
        ENTRY;

        /* Forbid to set scrub position after iteration started. */
        if (osi->osi_it_start)
                RETURN(-EPERM);

        rc = osd_scrub_str2pos(&ino, (__u8 *)key);
        if (rc == 0) {
                if (osi->osi_pos_urgent > ino)
                        osi->osi_pos_urgent = ino;
        }

        RETURN(rc);
}

static void osd_scrub_it_put(const struct lu_env *env, struct dt_it *di)
{
        /* Do nothing. */
}

static int osd_scrub_self_preload(const struct lu_env *env,
                                  struct osd_scrub_it *osi)
{
        struct super_block     *sb     = osi->osi_sb;
        struct osd_thread_info *info   = osd_oti_get(env);
        struct buffer_head     *bitmap = NULL;
        __u32                   max    =
                        le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        int                     rc;
        ENTRY;

        while (osi->osi_pos_urgent <= max &&
               osi->osi_cached_items < OSD_SCRUB_IT_CACHE_SIZE) {
                struct osd_idmap_cache *oic;
                struct inode *inode;
                ldiskfs_group_t bg = (osi->osi_pos_urgent - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (osi->osi_pos_urgent - 1) %
                               LDISKFS_INODES_PER_GROUP(sb);
                __u32 gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(sb);

                bitmap = ldiskfs_read_inode_bitmap(sb, bg);
                if (bitmap == NULL) {
                        CERROR("Fail to read bitmap for %u, "
                               "scrub will stop, urgent mode.\n", (__u32)bg);
                        RETURN(-EIO);
                }

                while (osi->osi_cached_items < OSD_SCRUB_IT_CACHE_SIZE) {
                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                osi->osi_pos_urgent = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        oic = &osi->osi_cache[osi->osi_producer_idx];
                        osi->osi_pos_urgent = gbase + offset;
                        osd_id_gen(&oic->oic_lid, osi->osi_pos_urgent,
                                   OSD_OII_NOGEN);
                        inode = osd_iget(osi->osi_dev, &oic->oic_lid,
                                         &oic->oic_fid, &info->oti_child_dentry,
                                         &info->oti_mdt_attrs,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        offset++;
                                        osi->osi_pos_urgent = gbase + offset;
                                        continue;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d, urgent mode.\n",
                                       bg, osi->osi_pos_urgent, rc);
                                brelse(bitmap);
                                /* If up layer LFSCK ignore the failure,
                                 * we can skip the inode next time. */
                                osi->osi_pos_urgent = gbase + offset + 1;
                                RETURN(rc);
                        }

                        iput(inode);
                        offset++;
                        osi->osi_pos_urgent = gbase + offset;
                        osi->osi_cached_items++;
                        osi->osi_producer_idx = (osi->osi_producer_idx + 1) &
                                                ~OSD_SCRUB_IT_CACHE_MASK;
                }
        }

        if (osi->osi_cached_items < OSD_SCRUB_IT_CACHE_SIZE) {
                CDEBUG(D_SCRUB, "OSD self pre-loaded all: pid = %d\n",
                       cfs_curproc_pid());

                osi->osi_all_cached = 1;
                cfs_waitq_broadcast(&osi->osi_thread.t_ctl_waitq);
        } else {
                brelse(bitmap);
        }

        RETURN(0);
}

/**
 * \retval  +1: OSD layer iteration completed.
 * \retval   0: navigated to next position.
 * \retval -ve: some failure occurred, caused OSD layer iterator exit.
 */
static int osd_scrub_it_next(const struct lu_env *env, struct dt_it *di)
{
        struct osd_scrub_it    *osi    = (struct osd_scrub_it *)di;
        struct scrub_exec_head *seh    = osi->osi_dev->od_scrub_seh;
        struct ptlrpc_thread   *thread = &osi->osi_thread;
        struct l_wait_info      lwi    = { 0 };
        int                     rc;
        ENTRY;

        LASSERT(osi->osi_it_start);

again:
        l_wait_event(thread->t_ctl_waitq,
                     osi->osi_cached_items > 0 ||
                     osi->osi_all_cached ||
                     !thread_is_running(thread) ||
                     osi->osi_urgent_mode,
                     &lwi);

        if (osi->osi_cached_items > 0) {
                int wakeup = 0;

                cfs_spin_lock(&osi->osi_cache_lock);
                if (osi->osi_cached_items == OSD_SCRUB_IT_CACHE_SIZE)
                        wakeup = 1;
                osi->osi_consumer_slot = osi->osi_cache[osi->osi_consumer_idx];
                osi->osi_cached_items--;
                cfs_spin_unlock(&osi->osi_cache_lock);
                if (wakeup != 0)
                        cfs_waitq_broadcast(&thread->t_ctl_waitq);
                osi->osi_consumer_idx = (osi->osi_consumer_idx + 1) &
                                        ~OSD_SCRUB_IT_CACHE_MASK;
                RETURN(DSRV_CONTINUE);
        }

        if (osi->osi_all_cached) {
                if (!thread_is_running(thread))
                        RETURN(DSRV_COMPLETED);
                else
                        RETURN(DSRV_WAIT);
        }

        if (!thread_is_running(thread)) {
                if (osi->osi_exit_value > 0)
                        RETURN(DSRV_COMPLETED);
                else if (osi->osi_exit_value == 0)
                        RETURN(DSRV_PAUSED);
                else
                        RETURN(DSRV_FAILURE);
        }

        cfs_spin_lock(&seh->seh_lock);
        if (unlikely(!osi->osi_urgent_mode)) {
                cfs_spin_unlock(&seh->seh_lock);
                goto again;
        }

        /* Under urgent mode, iterates by itself. */
        osi->osi_self_preload = 1;
        cfs_spin_unlock(&seh->seh_lock);

        rc = osd_scrub_self_preload(env, osi);
        cfs_spin_lock(&seh->seh_lock);
        osi->osi_self_preload = 0;
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (rc >= 0)
                goto again;

        RETURN(rc);
}

/* To unify the interfaces and hide backend filesystem detail, the scrub
 * key will represented as printable string. */
static struct dt_key *osd_scrub_it_key(const struct lu_env *env,
                                       const struct dt_it *di)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;

        osd_scrub_pos2str(osi->osi_key, osi->osi_consumer_slot.oic_lid.oii_ino);
        return (struct dt_key *)osi->osi_key;
}

static int osd_scrub_it_key_size(const struct lu_env *env,
                                 const struct dt_it *di)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;
        return sizeof(osi->osi_key);
}

static int osd_scrub_it_rec(const struct lu_env *env,
                            const struct dt_it *di,
                            struct dt_rec *rec, __u32 attr)
{
        struct osd_scrub_it   *osi = (struct osd_scrub_it *)di;
        struct lu_fid         *fid = (struct lu_fid *)rec;

        *fid = osi->osi_consumer_slot.oic_fid;
        return 0;
}

static __u64 osd_scrub_it_store(const struct lu_env *env,
                                const struct dt_it *di)
{
        return -ENOSYS;
}

/* Trigger OSD layer iteration. */
static int osd_scrub_it_load(const struct lu_env *env,
                             const struct dt_it *di, __u64 hash)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;
        int                  rc  = 0;

        if (!osi->osi_it_start) {
                if (osi->osi_pos_urgent == ~0U)
                        osi->osi_pos_urgent = osi->osi_pos_current;
                else if (osi->osi_pos_urgent == 0)
                        osi->osi_pos_urgent = LDISKFS_FIRST_INO(osi->osi_sb);
                else
                        /* Skip the one that has been processed last time. */
                        osi->osi_pos_urgent++;

                cfs_spin_lock(&osi->osi_dev->od_scrub_seh->seh_lock);
                if (!osi->osi_urgent_mode)
                        osi->osi_pos_current = osi->osi_pos_urgent;
                osi->osi_it_start = 1;
                cfs_spin_unlock(&osi->osi_dev->od_scrub_seh->seh_lock);
                cfs_waitq_broadcast(&osi->osi_thread.t_ctl_waitq);

                /* Unplug OSD layer iteration by the first next() call. */
                rc = osd_scrub_it_next(env, (struct dt_it *)di);
        }
        return rc;
}

static int osd_scrub_it_key_rec(const struct lu_env *env,
                                const struct dt_it *di, void* key_rec)
{
        return -ENOSYS;
}

const struct dt_index_operations osd_scrub_ops = {
        .dio_lookup         = osd_scrub_lookup,
        .dio_declare_insert = osd_scrub_declare_insert,
        .dio_insert         = osd_scrub_insert,
        .dio_declare_delete = osd_scrub_declare_delete,
        .dio_delete         = osd_scrub_delete,
        .dio_it             = {
                .init     = osd_scrub_it_init,
                .fini     = osd_scrub_it_fini,
                .get      = osd_scrub_it_get,
                .put      = osd_scrub_it_put,
                .next     = osd_scrub_it_next,
                .key      = osd_scrub_it_key,
                .key_size = osd_scrub_it_key_size,
                .rec      = osd_scrub_it_rec,
                .store    = osd_scrub_it_store,
                .load     = osd_scrub_it_load,
                .key_rec  = osd_scrub_it_key_rec,
        }
};

static int osd_declare_scrub_store_fid(const struct lu_env *env,
                                       struct dt_object *dir,
                                       const struct dt_rec *rec,
                                       const struct dt_key *key,
                                       struct thandle *handle)
{
        int rc;

        rc = dt_declare_delete(env, dir, key, handle);
        if (rc != 0)
                return rc;

        rc = dt_declare_insert(env, dir, rec, key, handle);
        return rc;
}

static int osd_scrub_store_fid(const struct lu_env *env, struct dt_object *obj)
{
        struct dt_device                 *dt     = lu2dt_dev(obj->do_lu.lo_dev);
        struct lu_fid                    *fid    = &osd_oti_get(env)->oti_fid;
        struct dt_object                 *dir;
        struct thandle                   *handle = NULL;
        const struct dt_index_operations *dio;
        struct dt_key                    *key    = (struct dt_key *)osd_scrub;
        int                               rc;
        ENTRY;

        dir = dt_store_resolve(env, dt, "", fid);
        if (IS_ERR(dir)) {
                rc = PTR_ERR(dir);
                CERROR("Fail to parse root for scrub_store_fid: rc = %d\n", rc);
                GOTO(out, rc);
        }

        rc = dt_try_as_dir(env, dir);
        if (rc == 0)
                GOTO(out, rc = -ENOTDIR);

        dio = dir->do_index_ops;
        fid = (struct lu_fid *)lu_object_fid(&obj->do_lu);

        handle = dt_trans_create(env, dt);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        rc = osd_declare_scrub_store_fid(env, dir, (struct dt_rec *)fid, key,
                                         handle);
        if (rc != 0) {
                CERROR("Fail to declare trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        rc = dt_trans_start(env, dt, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        /* Delete the old name entry, which does not contain fid. */
        rc = dio->dio_delete(env, dir, key, handle, BYPASS_CAPA);
        if (rc != 0) {
                CERROR("Fail to delete old name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                GOTO(out, rc);
        }

        /* Insert the name entry with the same name, then the fid will be stored
         * when new entry inserted. */
        rc = dio->dio_insert(env, dir, (const struct dt_rec *)fid, key, handle,
                             BYPASS_CAPA, 1);
        if (rc != 0) {
                CERROR("Fail to insert new name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                GOTO(out, rc);
        }

        GOTO(out, rc);

out:
        if (handle != NULL && !IS_ERR(handle)) {
                handle->th_result = rc;
                dt_trans_stop(env, dt, handle);
        }
        if (dir != NULL && !IS_ERR(dir))
                lu_object_put(env, &dir->do_lu);
        return rc;
}

static struct scrub_exec_head *
osd_scrub_init_seh(const struct lu_env *env, struct osd_device *dev,
                   struct dt_object *obj, int restored)
{
        struct scrub_exec_head *seh;
        struct scrub_head      *sh;
        struct scrub_unit_desc *sud;
        struct scrub_exec_unit *seu = NULL;
        int                     i;
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
                seu = osd_scrub_register(env, dev, NULL, seh, ST_OI_SCRUB,
                                         SS_INIT, SUF_INCONSISTENT, 1, 0);
                if (IS_ERR(seu))
                        GOTO(out, rc = PTR_ERR(seu));
        }

        sh = &seh->seh_head;
        for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                sud = &sh->sh_desc[i];
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
        struct osd_thread_info     *oti;
        struct lu_fid              *fid;
        struct dt_object           *dt_dir;
        struct osd_object          *osd_dir;
        struct inode               *dir;
        struct dentry              *dentry;
        struct htree_lock          *hlock    = NULL;
        struct buffer_head         *bh;
        struct ldiskfs_dir_entry_2 *de;
        struct dt_object           *obj;
        int                         rc;
        ENTRY;

        oti = osd_oti_get(env);
        fid = &oti->oti_fid;
        dt_dir = dt_store_resolve(env, dt, "", fid);
        if (unlikely(IS_ERR(dt_dir))) {
                CERROR("Fail to resolve root, rc = %ld\n", PTR_ERR(dt_dir));
                RETURN(dt_dir);
        }

        osd_dir = osd_dt_obj(dt_dir);
        dir = osd_dir->oo_inode;
        dentry = osd_child_dentry_get(env, osd_dir, osd_scrub, strlen(osd_scrub));
        if (osd_dir->oo_hl_head != NULL) {
                hlock = oti->oti_hlock;
                ldiskfs_htree_lock(hlock, osd_dir->oo_hl_head,
                                   dir, LDISKFS_HLOCK_LOOKUP);
        } else {
                cfs_down_read(&osd_dir->oo_ext_idx_sem);
        }

        bh = osd_ldiskfs_find_entry(dir, dentry, &de, hlock);
        if (bh) {
                *restored = osd_get_fid_from_dentry(de, (struct dt_rec *)fid);
                if (*restored != 0)
                        rc = osd_ea_fid_get(env, osd_dir,
                                            le32_to_cpu(de->inode), fid);
        } else {
                rc = -ENOENT;
        }

        brelse(bh);
        if (hlock != NULL)
                ldiskfs_htree_unlock(hlock);
        else
                cfs_up_read(&osd_dir->oo_ext_idx_sem);

        lu_object_put(env, &dt_dir->do_lu);
        if (rc != 0) {
                CERROR("Fail to find [%s], rc = %d\n", osd_scrub, rc);
                RETURN(ERR_PTR(rc));
        }

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

int osd_oii_insert(struct osd_device *dev, struct osd_object *obj,
                   struct osd_idmap_cache *oic)
{
        struct scrub_exec_head *seh = dev->od_scrub_seh;
        struct osd_scrub_it *osi = NULL;
        struct osd_inconsistent_item *oii;
        ENTRY;

        OBD_ALLOC_PTR(oii);
        if (unlikely(oii == NULL))
                RETURN(-ENOMEM);

        CFS_INIT_LIST_HEAD(&oii->oii_list);
        oii->oii_cache = *oic;

        cfs_down_read(&seh->seh_rwsem);
        if (!(seh->seh_active & ST_OI_SCRUB)) {
                cfs_up_read(&seh->seh_rwsem);
                OBD_FREE_PTR(oii);
                RETURN(-EAGAIN);
        }

        oii->oii_obj = &obj->oo_dt.do_lu;
        lu_object_get(oii->oii_obj);
        lu_object_set_scrub(oii->oii_obj->lo_header);

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
