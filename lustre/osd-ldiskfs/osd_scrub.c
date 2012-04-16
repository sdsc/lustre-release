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

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
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
 * The OI scrub is used for rebuilding Object Index files when restores MDT from
 * file-level backup.
 *
 * The otable based iterator scans ldiskfs inode table to feed OI scrub and up
 * layer LFSCK.
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
#include "osd_scrub.h"

#define HALF_SEC        (CFS_HZ >> 1)

static inline void
osd_scrub_build_lbuf(struct lu_buf *lbuf, void *area, size_t len)
{
        lbuf->lb_buf = area;
        lbuf->lb_len = len;
}

static void osd_scrub_file_to_cpu(struct scrub_file *des,
                                  struct scrub_file *src)
{
        des->sf_version = be16_to_cpu(src->sf_version);
        des->sf_status  = be16_to_cpu(src->sf_status);
        des->sf_flags   = be16_to_cpu(src->sf_flags);
        des->sf_param   = be16_to_cpu(src->sf_param);
        des->sf_time_last_complete      =
                                be64_to_cpu(src->sf_time_last_complete);
        des->sf_time_latest_start       =
                                be64_to_cpu(src->sf_time_latest_start);
        des->sf_time_last_checkpoint    =
                                be64_to_cpu(src->sf_time_last_checkpoint);
        des->sf_pos_latest_start        =
                                be64_to_cpu(src->sf_pos_latest_start);
        des->sf_pos_last_checkpoint     =
                                be64_to_cpu(src->sf_pos_last_checkpoint);
        des->sf_pos_first_inconsistent  =
                                be64_to_cpu(src->sf_pos_first_inconsistent);
        des->sf_items_checked           =
                                be64_to_cpu(src->sf_items_checked);
        des->sf_items_updated           =
                                be64_to_cpu(src->sf_items_updated);
        des->sf_items_failed            =
                                be64_to_cpu(src->sf_items_failed);
        des->sf_items_updated_prior     =
                                be64_to_cpu(src->sf_items_updated_prior);
        des->sf_success_count   = be32_to_cpu(src->sf_success_count);
        des->sf_run_time        = be32_to_cpu(src->sf_run_time);
}

static void osd_scrub_file_to_be(struct scrub_file *des,
                                 struct scrub_file *src)
{
        des->sf_version = cpu_to_be16(src->sf_version);
        des->sf_status  = cpu_to_be16(src->sf_status);
        des->sf_flags   = cpu_to_be16(src->sf_flags);
        des->sf_param   = cpu_to_be16(src->sf_param);
        des->sf_time_last_complete      =
                                cpu_to_be64(src->sf_time_last_complete);
        des->sf_time_latest_start       =
                                cpu_to_be64(src->sf_time_latest_start);
        des->sf_time_last_checkpoint    =
                                cpu_to_be64(src->sf_time_last_checkpoint);
        des->sf_pos_latest_start        =
                                cpu_to_be64(src->sf_pos_latest_start);
        des->sf_pos_last_checkpoint     =
                                cpu_to_be64(src->sf_pos_last_checkpoint);
        des->sf_pos_first_inconsistent  =
                                cpu_to_be64(src->sf_pos_first_inconsistent);
        des->sf_items_checked           =
                                cpu_to_be64(src->sf_items_checked);
        des->sf_items_updated           =
                                cpu_to_be64(src->sf_items_updated);
        des->sf_items_failed            =
                                cpu_to_be64(src->sf_items_failed);
        des->sf_items_updated_prior     =
                                cpu_to_be64(src->sf_items_updated_prior);
        des->sf_success_count   = cpu_to_be32(src->sf_success_count);
        des->sf_run_time        = cpu_to_be32(src->sf_run_time);
}

static void osd_scrub_file_init(const struct lu_env *env,
                                struct osd_scrub *scrub)
{
        struct scrub_file *sf = &scrub->os_file;

        memset(sf, 0, sizeof(*sf));
        sf->sf_version = SCRUB_VERSION_V1;
        sf->sf_status = SS_INIT;
        scrub->os_dirty = 1;
}

static int osd_scrub_file_load(const struct lu_env *env,
                               struct osd_scrub *scrub)
{
        struct lu_buf lbuf;
        loff_t        pos  = 0;
        int           rc;

        osd_scrub_build_lbuf(&lbuf, &scrub->os_file_disk,
                             sizeof(scrub->os_file_disk));
        rc = osd_read(env, scrub->os_obj, &lbuf, &pos, BYPASS_CAPA);
        if (rc == lbuf.lb_len) {
                osd_scrub_file_to_cpu(&scrub->os_file, &scrub->os_file_disk);
                rc = 0;
        } else if (rc != 0) {
                CERROR("Fail to load scrub file: rc = %d, expected = %d.\n",
                       rc, (int)lbuf.lb_len);
                if (rc > 0)
                        rc = -EFAULT;
        } else {
                /* return -ENOENT for empty scrub file case. */
                rc = -ENOENT;
        }

        return rc;
}

static int osd_scrub_file_store(const struct lu_env *env,
                                struct osd_scrub *scrub)
{
        struct dt_object   *obj    = scrub->os_obj;
        struct dt_device   *dt     = lu2dt_dev(obj->do_lu.lo_dev);
        struct thandle     *handle;
        struct osd_thandle *oh;
        struct lu_buf       lbuf;
        loff_t              offset = 0;
        int                 rc;

        if (!scrub->os_dirty)
                return 0;

        handle = osd_trans_create(env, dt);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for scrub store: rc = %d\n", rc);
                RETURN(rc);
        }

        oh = container_of0(handle, struct osd_thandle, ot_super);
        oh->ot_credits = osd_dto_credits_noquota[DTO_WRITE_BASE] +
                         osd_dto_credits_noquota[DTO_WRITE_BLOCK];
        rc = osd_trans_start(env, dt, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for scrub store: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        osd_scrub_build_lbuf(&lbuf, &scrub->os_file_disk,
                             sizeof(scrub->os_file_disk));
        osd_scrub_file_to_be(&scrub->os_file_disk, &scrub->os_file);
        rc = osd_write(env, obj, &lbuf, &offset, handle, BYPASS_CAPA, 1);
        if (rc == lbuf.lb_len)
                rc = 0;
        else if (rc >= 0)
                rc = -ENOSPC;

        GOTO(stop, rc);

stop:
        handle->th_result = rc;
        osd_trans_stop(env, handle);
        if (rc == 0)
                scrub->os_dirty = 0;
        return rc;
}

static void osd_scrub_prep(const struct lu_env *env, struct osd_device *dev,
                           struct osd_otable_it *it)
{
        struct osd_scrub  *scrub = dev->od_scrub;
        struct scrub_file *sf    = &scrub->os_file;
        int                rc;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if (sf->sf_pos_last_checkpoint != 0)
                sf->sf_pos_latest_start = sf->sf_pos_last_checkpoint + 1;
        else
                sf->sf_pos_latest_start = LDISKFS_FIRST_INO(it->ooi_sb);

        cfs_spin_lock(&dev->od_otable_it_lock);
        if (!it->ooi_user_ready) {
                if (it->ooi_pos_current > sf->sf_pos_latest_start)
                        it->ooi_pos_current = sf->sf_pos_latest_start;
        } else {
                if (sf->sf_pos_latest_start < it->ooi_pos_current)
                        sf->sf_pos_latest_start = it->ooi_pos_current;
        }

        if (sf->sf_flags & SF_RESTORED)
                it->ooi_no_cache = 1;

        if (it->ooi_no_cache && it->ooi_user_ready)
                it->ooi_pos_self_preload = it->ooi_pos_current;
        cfs_spin_unlock(&dev->od_otable_it_lock);

        if (it->ooi_no_cache)
                cfs_waitq_broadcast(&it->ooi_thread.t_ctl_waitq);

        sf->sf_status = SS_SCANNING;
        sf->sf_time_latest_start = cfs_time_current_sec();
        sf->sf_time_last_checkpoint = sf->sf_time_latest_start;
        scrub->os_dirty = 1;
        rc = osd_scrub_file_store(env, scrub);
        if (rc < 0)
                CERROR("Fail to osd_scrub_prep: rc = %d\n", rc);

        /* Ignore the store failure. */
        scrub->os_time_last_checkpoint = cfs_time_current();
        scrub->os_time_next_checkpoint = scrub->os_time_last_checkpoint +
                                cfs_time_seconds(SCRUB_CHECKPOING_INTERVAL);
        cfs_up_write(&scrub->os_rwsem);

        EXIT;
}

static int osd_scrub_exec(const struct lu_env *env, struct osd_device *dev,
                          int result)
{
        struct osd_otable_it   *it     = dev->od_otable_it;
        struct osd_scrub       *scrub  = dev->od_scrub;
        struct scrub_file      *sf     = &scrub->os_file;
        struct osd_idmap_cache *oic    = it->ooi_oicp;
        struct lu_fid          *fid    = &oic->oic_fid;
        struct osd_inode_id    *lid    = &oic->oic_lid;
        struct osd_inode_id    *lid2   = &osd_oti_get(env)->oti_id;
        struct dt_device       *dt     = &dev->od_dt_dev;
        struct osd_thread_info *info   = osd_oti_get(env);
        struct lu_fid          *oi_fid = &info->oti_fid;
        struct osd_inode_id    *oi_id  = &info->oti_id;
        struct thandle         *handle = NULL;
        struct iam_container   *bag;
        struct iam_path_descr  *ipd;
        struct osd_thandle     *oh;
#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_t               save   = cfs_curproc_cap_pack();
#endif
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if (lid->oii_ino < sf->sf_pos_latest_start && !it->ooi_in_prior)
                GOTO(out, result = 0);

        scrub->os_new_checked++;
        if (fid_is_igif(fid) || fid_is_idif(fid) ||
            fid_seq(fid) == FID_SEQ_LLOG || fid_seq(fid) == FID_SEQ_LOCAL_FILE)
                GOTO(out, result = 0);

        if (result != 0)
                GOTO(out, result);

        result = osd_oi_lookup(osd_oti_get(env), dev, fid, lid2);
        if (result != 0)
                /* XXX: Either the inode is removed by others or someone just
                 *      created the inode, but not inserted it into OI yet. */
                GOTO(out, result = (result == -ENOENT ? 0 : result));

        if (osd_id_eq(lid, lid2))
                GOTO(out, result = 0);

        fid_cpu_to_be(oi_fid, &oic->oic_fid);
        osd_id_pack(oi_id, &oic->oic_lid);
        handle = osd_trans_create(env, dt);
        if (IS_ERR(handle)) {
                result = PTR_ERR(handle);
                CERROR("Fail to create trans for OI update: rc = %d\n", result);
                GOTO(out, result);
        }

        oh = container_of0(handle, struct osd_thandle, ot_super);
        oh->ot_credits = osd_dto_credits_noquota[DTO_INDEX_UPDATE];
        result = osd_trans_start(env, dt, handle);
        if (result != 0) {
                CERROR("Fail to start trans for OI update: rc = %d\n", result);
                GOTO(stop, result);
        }

        bag = &osd_fid2oi(dev, &oic->oic_fid)->oi_dir.od_container;
        ipd = osd_idx_ipd_get(env, bag);
        if (unlikely(ipd == NULL))
                GOTO(stop, result = -ENOMEM);

#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_raise(CFS_CAP_SYS_RESOURCE);
#endif
        result = iam_update(oh->ot_handle, bag, (const struct iam_key *)oi_fid,
                        (struct iam_rec *)oi_id, ipd);
#ifdef HAVE_QUOTA_SUPPORT
        cfs_curproc_cap_unpack(save);
#endif
        osd_ipd_put(env, bag, ipd);

        GOTO(stop, result);

stop:
        handle->th_result = result;
        osd_trans_stop(env, handle);
        if (result == 0) {
                if (it->ooi_in_prior)
                        sf->sf_items_updated_prior++;
                else
                        sf->sf_items_updated++;
        }

        GOTO(out, result = (result == -ENOENT ? 0 : result));

out:
        if (result != 0) {
                sf->sf_items_failed++;
                if (sf->sf_pos_first_inconsistent == 0 ||
                    sf->sf_pos_first_inconsistent > lid->oii_ino)
                        sf->sf_pos_first_inconsistent = lid->oii_ino;
        }
        cfs_up_write(&scrub->os_rwsem);
        return sf->sf_param & SP_FAILOUT ? result : 0;
}

static int osd_scrub_checkpoint(const struct lu_env *env,
                                struct osd_device *dev)
{
        struct osd_scrub  *scrub = dev->od_scrub;
        struct scrub_file *sf    = &scrub->os_file;
        int                rc;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if (scrub->os_new_checked > 0) {
                sf->sf_items_checked += scrub->os_new_checked;
                scrub->os_new_checked = 0;
                sf->sf_pos_last_checkpoint = dev->od_otable_it->ooi_pos_current;
                scrub->os_dirty = 1;
        }
        if (scrub->os_dirty)
                sf->sf_time_last_checkpoint = cfs_time_current_sec();
        sf->sf_run_time += cfs_duration_sec(cfs_time_current() -
                                scrub->os_time_last_checkpoint + HALF_SEC);
        rc = osd_scrub_file_store(env, scrub);
        scrub->os_time_last_checkpoint = cfs_time_current();
        scrub->os_time_next_checkpoint = scrub->os_time_last_checkpoint +
                                cfs_time_seconds(SCRUB_CHECKPOING_INTERVAL);
        cfs_up_write(&scrub->os_rwsem);

        RETURN(rc);
}

static void osd_scrub_post(const struct lu_env *env, struct osd_device *dev,
                           int result)
{
        struct osd_scrub  *scrub    = dev->od_scrub;
        struct scrub_file *sf       = &scrub->os_file;
        __u64              ctime;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        ctime = cfs_time_current_sec();
        if (scrub->os_new_checked > 0) {
                sf->sf_items_checked += scrub->os_new_checked;
                scrub->os_new_checked = 0;
                sf->sf_pos_last_checkpoint = dev->od_otable_it->ooi_pos_current;
        }
        sf->sf_time_last_checkpoint = ctime;
        if (result > 0) {
                sf->sf_status = SS_COMPLETED;
                sf->sf_flags &= ~(SF_RESTORED | SF_BYRPC);
                sf->sf_time_last_complete = ctime;
                sf->sf_success_count++;
        } else if (result == 0) {
                sf->sf_status = SS_PAUSED;
        } else {
                sf->sf_status = SS_FAILED;
        }
        sf->sf_run_time += cfs_duration_sec(cfs_time_current() -
                                scrub->os_time_last_checkpoint + HALF_SEC);
        scrub->os_dirty = 1;
        result = osd_scrub_file_store(env, scrub);
        if (result < 0)
                CERROR("Fail to osd_scrub_post: rc = %d\n", result);

        /* For stop case, cleanup the environment in spite of whether
         * write succeeded or not, because we have no chance to write
         * again even if the osd_scrub_file_store() failed. */
        scrub->os_time_last_checkpoint = 0;
        scrub->os_time_next_checkpoint = 0;
        scrub->os_new_checked = 0;
        scrub->os_dirty = 0;
        cfs_up_write(&scrub->os_rwsem);

        EXIT;
}

static inline struct osd_otable_it *ooi_get(struct osd_otable_it *it)
{
        cfs_atomic_inc(&it->ooi_refcount);
        return it;
}

static inline void ooi_put(struct osd_otable_it *it)
{
        struct osd_device *dev = it->ooi_dev;

        if (cfs_atomic_dec_and_lock(&it->ooi_refcount,
                                    &dev->od_otable_it_lock)) {
                if (unlikely(cfs_atomic_read(&it->ooi_refcount) > 0)) {
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        return;
                }

                if (likely(dev->od_otable_it == it))
                        dev->od_otable_it = NULL;
                cfs_spin_unlock(&dev->od_otable_it_lock);
                lu_env_fini(&it->ooi_env);
                OBD_FREE_PTR(it);
        }
}

static struct osd_otable_it *ooi_init(struct osd_device *dev)
{
        struct super_block   *sb  = osd_sb(dev);
        struct osd_otable_it *it;
        int                   rc;

        OBD_ALLOC_PTR(it);
        if (unlikely(it == NULL))
                return ERR_PTR(-ENOMEM);

        rc = lu_env_init(&it->ooi_env, LCT_MD_THREAD | LCT_DT_THREAD);
        if (rc != 0) {
                OBD_FREE_PTR(it);
                return ERR_PTR(rc);
        }

        cfs_waitq_init(&it->ooi_thread.t_ctl_waitq);
        cfs_atomic_set(&it->ooi_refcount, 1);
        it->ooi_sb = sb;
        it->ooi_dev = dev;
        it->ooi_inodes_per_block = LDISKFS_BLOCK_SIZE(sb) /
                                    LDISKFS_INODE_SIZE(sb);
        it->ooi_pos_current = ~0U;
        it->ooi_pos_self_preload = ~0U;
        return it;
}

static int osd_scrub_main(void *args)
{
        struct osd_otable_it         *it     =
                        ooi_get((struct osd_otable_it *)args);
        struct lu_env                *env    = &it->ooi_env;
        struct osd_device            *dev    = it->ooi_dev;
        struct osd_scrub             *scrub  = dev->od_scrub;
        struct scrub_file            *sf     = &scrub->os_file;
        struct ptlrpc_thread         *thread = &it->ooi_thread;
        struct l_wait_info            lwi    = { 0 };
        struct super_block           *sb     = it->ooi_sb;
        struct osd_thread_info       *info   = osd_oti_get(env);
        struct osd_inconsistent_item *oii    = NULL;
        struct osd_otable_cache      *ooc    = &it->ooi_cache;
        __u32                         max    =
                        le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        int                           rc     = 0;
        int                           noslot = 0;
        ENTRY;

        cfs_daemonize("OI_scrub");

        CDEBUG(D_SCRUB, "OI scrub: flags = 0x%x, param = 0x%x, pid = %d\n",
               sf->sf_flags, sf->sf_param, cfs_curproc_pid());
        if (it->ooi_oi_scrub)
                osd_scrub_prep(env, dev, it);

        cfs_spin_lock(&dev->od_otable_it_lock);
        LASSERT(dev->od_otable_it == NULL);

        dev->od_otable_it = it;
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&dev->od_otable_it_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (!it->ooi_no_cache) {
                l_wait_event(thread->t_ctl_waitq,
                             it->ooi_user_ready || it->ooi_no_cache ||
                             !thread_is_running(thread),
                             &lwi);
                if (unlikely(!thread_is_running(thread)))
                        GOTO(post, rc = 0);
        } else {
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
        }

        CDEBUG(D_SCRUB, "OSD scrub iteration start: pos = %u\n",
               it->ooi_pos_current);

again:
        while (it->ooi_pos_current <= max ||
               !cfs_list_empty(&dev->od_inconsistent_items)) {
                struct inode *inode;
                struct buffer_head *bitmap = NULL;
                ldiskfs_group_t bg = (it->ooi_pos_current - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (it->ooi_pos_current - 1) %
                               LDISKFS_INODES_PER_GROUP(sb);
                __u32 gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(sb);

                if (cfs_list_empty(&dev->od_inconsistent_items)) {
                        bitmap = ldiskfs_read_inode_bitmap(sb, bg);
                        if (bitmap == NULL) {
                                CERROR("Fail to read bitmap for pos = %u, "
                                       "bg = %u, scrub will stop.\n",
                                       it->ooi_pos_current, (__u32)bg);
                                GOTO(post, rc = -EIO);
                        }
                }

                while (offset < LDISKFS_INODES_PER_GROUP(sb) ||
                       !cfs_list_empty(&dev->od_inconsistent_items)) {
                        if (unlikely(!thread_is_running(thread))) {
                                brelse(bitmap);
                                GOTO(post, rc = 0);
                        }

                        if (!cfs_list_empty(&dev->od_inconsistent_items)) {
                                oii = cfs_list_entry(
                                                dev->od_inconsistent_items.next,
                                                struct osd_inconsistent_item,
                                                oii_list);
                                it->ooi_oicp = &oii->oii_cache;
                                it->ooi_in_prior = 1;
                                goto exec;
                        }

                        if (unlikely(bitmap == NULL))
                                break;

                        if (unlikely(it->ooi_pos_current > max)) {
                                brelse(bitmap);
                                break;
                        }

                        if (!it->ooi_no_cache && noslot != 0)
                                goto wait;

                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                it->ooi_pos_current = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        if (it->ooi_no_cache)
                                it->ooi_oicp = &it->ooi_oic_urgent;
                        else
                                it->ooi_oicp = &ooc->ooc_producer_slot;
                        it->ooi_pos_current = gbase + offset;
                        osd_id_gen(&it->ooi_oicp->oic_lid,
                                   it->ooi_pos_current, OSD_OII_NOGEN);
                        inode = osd_iget(info, it->ooi_dev,
                                         &it->ooi_oicp->oic_lid,
                                         &it->ooi_oicp->oic_fid,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        it->ooi_skip_obj = 1;
                                        goto checkpoint;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d\n",
                                       bg, it->ooi_pos_current, rc);
                        } else {
                                iput(inode);
                        }

exec:
                        if (it->ooi_oi_scrub)
                                rc = osd_scrub_exec(env, dev, rc);

                        if (oii != NULL) {
                                cfs_spin_lock(&dev->od_otable_it_lock);
                                cfs_list_del_init(&oii->oii_list);
                                cfs_spin_unlock(&dev->od_otable_it_lock);
                                lu_object_put(env, oii->oii_obj);
                                OBD_FREE_PTR(oii);
                                oii = NULL;
                        }

                        if (rc != 0) {
                                brelse(bitmap);
                                GOTO(post, rc);
                        }

checkpoint:
                        if (it->ooi_oi_scrub &&
                            cfs_time_beforeq(scrub->os_time_next_checkpoint,
                                             cfs_time_current())) {
                                rc = osd_scrub_checkpoint(env, dev);
                                if (rc != 0) {
                                        CERROR("Fail to checkpoint: pos = %u, "
                                               "rc = %d\n",
                                               it->ooi_pos_current, rc);
                                        brelse(bitmap);
                                        GOTO(post, rc);
                                }
                        }

                        if (it->ooi_in_prior) {
                                it->ooi_in_prior = 0;
                                continue;
                        }

                        it->ooi_pos_current = gbase + ++offset;
                        if (it->ooi_skip_obj) {
                                it->ooi_skip_obj = 0;
                                continue;
                        }

                        if (!it->ooi_no_cache) {

wait:
                                l_wait_event(thread->t_ctl_waitq,
                                             ooc->ooc_cached_items <
                                                OSD_OTABLE_IT_CACHE_SIZE ||
                                             !cfs_list_empty(
                                                &dev->od_inconsistent_items) ||
                                             it->ooi_no_cache ||
                                             !thread_is_running(thread),
                                             &lwi);

                                if (ooc->ooc_cached_items <
                                        OSD_OTABLE_IT_CACHE_SIZE) {
                                        int wakeup = 0;

                                        noslot = 0;
                                        cfs_spin_lock(&dev->od_otable_it_lock);
                                        if (ooc->ooc_cached_items == 0)
                                                wakeup = 1;
                                        ooc->ooc_cache[ooc->ooc_producer_idx] =
                                                ooc->ooc_producer_slot;
                                        ooc->ooc_cached_items++;
                                        cfs_spin_unlock(
                                                &dev->od_otable_it_lock);
                                        if (wakeup != 0)
                                                cfs_waitq_broadcast(
                                                        &thread->t_ctl_waitq);
                                        ooc->ooc_producer_idx =
                                                (ooc->ooc_producer_idx + 1) &
                                                ~OSD_OTABLE_IT_CACHE_MASK;
                                        continue;
                                }

                                noslot = 1;
                        } else if (it->ooi_wait_next) {
                                it->ooi_wait_next = 0;
                                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                        }
                }
        }

        if (it->ooi_pos_current > max) {
                if (!it->ooi_no_cache) {
                        it->ooi_all_cached = 1;
                        cfs_waitq_broadcast(&thread->t_ctl_waitq);
                }
                rc = 1;
        }

        GOTO(post, rc);

post:
        if (it->ooi_no_cache) {
                cfs_spin_lock(&dev->od_otable_it_lock);
                it->ooi_no_cache = 0;
                cfs_spin_unlock(&dev->od_otable_it_lock);
        }

        if (it->ooi_oi_scrub) {
                osd_scrub_post(env, dev, rc);
                cfs_spin_lock(&dev->od_otable_it_lock);
                it->ooi_oi_scrub = 0;
                cfs_spin_unlock(&dev->od_otable_it_lock);
        }

        if (rc == 1 && it->ooi_full_scan && !it->ooi_all_cached) {
                l_wait_event(thread->t_ctl_waitq,
                             it->ooi_all_cached || !it->ooi_self_preload ||
                             !cfs_list_empty(&dev->od_inconsistent_items) ||
                             !thread_is_running(thread),
                             &lwi);
                if (!it->ooi_all_cached && thread_is_running(thread)) {
                        cfs_spin_lock(&dev->od_otable_it_lock);
                        if (!it->ooi_self_preload)
                                it->ooi_pos_current = it->ooi_pos_self_preload;
                        else
                                it->ooi_no_cache = 1;
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        goto again;
                }
        }

        it->ooi_exit_value = rc;
        CDEBUG(D_SCRUB, "OSD scrub iteration stop: pos = %u, rc = %d\n",
               it->ooi_pos_current, rc);

        while (!cfs_list_empty(&dev->od_inconsistent_items)) {
                oii = cfs_list_entry(dev->od_inconsistent_items.next,
                                     struct osd_inconsistent_item,
                                     oii_list);
                cfs_list_del_init(&oii->oii_list);
                lu_object_put(env, oii->oii_obj);
                OBD_FREE_PTR(oii);
        }

        cfs_spin_lock(&dev->od_otable_it_lock);
        thread_set_flags(thread, SVC_STOPPED);
        cfs_spin_unlock(&dev->od_otable_it_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);
        ooi_put(it);
        return rc;
}

static void osd_scrub_reset(struct osd_scrub *scrub, __u16 flags)
{
        struct scrub_file *sf = &scrub->os_file;

        CDEBUG(D_SCRUB, "Reset OI scrub file, flags = 0x%x\n", flags);
        cfs_down_write(&scrub->os_rwsem);
        sf->sf_status = SS_INIT;
        sf->sf_flags = flags;
        sf->sf_param = 0;
        sf->sf_run_time = 0;
        sf->sf_time_latest_start = 0;
        sf->sf_time_last_checkpoint = 0;
        sf->sf_pos_latest_start = 0;
        sf->sf_pos_last_checkpoint = 0;
        sf->sf_pos_first_inconsistent = 0;
        sf->sf_items_checked = 0;
        sf->sf_items_updated = 0;
        sf->sf_items_failed = 0;
        sf->sf_items_updated_prior = 0;
        scrub->os_time_last_checkpoint = 0;
        scrub->os_time_next_checkpoint = 0;
        scrub->os_new_checked = 0;
        scrub->os_dirty = 1;
        cfs_up_write(&scrub->os_rwsem);
}

int osd_scrub_start(const struct lu_env *env, struct osd_device *dev, int byrpc)
{
        struct osd_scrub     *scrub = dev->od_scrub;
        struct scrub_file    *sf    = &scrub->os_file;
        struct osd_otable_it *it;
        struct l_wait_info    lwi   = { 0 };
        int                   rc;
        ENTRY;

        cfs_mutex_lock(&dev->od_otable_it_mutex);
        cfs_spin_lock(&dev->od_otable_it_lock);
        it = dev->od_otable_it;
        if (it != NULL) {
                LASSERT(byrpc != 0);

                if (it->ooi_oi_scrub) {
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        cfs_mutex_unlock(&dev->od_otable_it_mutex);
                        RETURN(0);
                }

                cfs_spin_unlock(&dev->od_otable_it_lock);
                if (sf->sf_flags == SS_COMPLETED)
                        osd_scrub_reset(scrub, SF_BYRPC);
                osd_scrub_prep(env, dev, it);
                it->ooi_oi_scrub = 1;
                cfs_mutex_unlock(&dev->od_otable_it_mutex);
                RETURN(0);
        }

        cfs_spin_unlock(&dev->od_otable_it_lock);
        it = ooi_init(dev);
        if (unlikely(IS_ERR(it)))
                GOTO(out, rc = PTR_ERR(it));

        if (sf->sf_flags == SS_COMPLETED) {
                LASSERT(byrpc != 0);

                osd_scrub_reset(scrub, SF_BYRPC);
        }

        if (byrpc != 0 && !(sf->sf_flags & SF_BYRPC)) {
                sf->sf_flags |= SF_BYRPC;
                scrub->os_dirty = 1;
        }

        it->ooi_no_cache = 1;
        it->ooi_oi_scrub = 1;
        rc = cfs_create_thread(osd_scrub_main, it, 0);
        if (rc < 0) {
                CERROR("Cannot start iteration thread: rc %d\n", rc);
                GOTO(out, rc);
        }

        l_wait_event(it->ooi_thread.t_ctl_waitq,
                     thread_is_running(&it->ooi_thread) ||
                     thread_is_stopped(&it->ooi_thread),
                     &lwi);

        GOTO(out, rc = 0);

out:
        if (rc != 0 && scrub->os_dirty != 0) {
                osd_scrub_file_to_cpu(sf, &scrub->os_file_disk);
                scrub->os_dirty = 0;
        }
        if (it != NULL)
                /* Release reference count, otherwise nobody else can do. */
                ooi_put(it);
        cfs_mutex_unlock(&dev->od_otable_it_mutex);
        return rc;
}

static void osd_scrub_stop(const struct lu_env *env, struct osd_device *dev)
{
        struct osd_otable_it *it;
        struct ptlrpc_thread *thread;
        struct l_wait_info    lwi    = { 0 };
        ENTRY;

        cfs_mutex_lock(&dev->od_otable_it_mutex);
        cfs_spin_lock(&dev->od_otable_it_lock);
        if (dev->od_otable_it == NULL) {
                cfs_spin_unlock(&dev->od_otable_it_lock);
                cfs_mutex_unlock(&dev->od_otable_it_mutex);
                RETURN_EXIT;
        }

        it = ooi_get(dev->od_otable_it);
        thread = &it->ooi_thread;
        if (!thread_is_stopped(thread)) {
                thread_set_flags(thread, SVC_STOPPING);
                cfs_spin_unlock(&dev->od_otable_it_lock);
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
                cfs_spin_lock(&dev->od_otable_it_lock);
        }
        dev->od_otable_it = NULL;
        cfs_spin_unlock(&dev->od_otable_it_lock);
        cfs_mutex_unlock(&dev->od_otable_it_mutex);
        ooi_put(it);

        EXIT;
}

static const char osd_scrub_name[] = "OI_scrub";

static struct osd_scrub *
osd_scrub_init(const struct lu_env *env, struct dt_device *dt, int *restored)
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
        struct osd_scrub            *scrub;
        struct scrub_file           *sf;
        int                          rc;
        ENTRY;

        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub_name,
                                           strlen(osd_scrub_name));
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
                        CERROR("Fail to find [%s], rc = %d\n",
                               osd_scrub_name, rc);
                        RETURN(ERR_PTR(rc));
                }
                goto locate;
        }

        jh = ldiskfs_journal_start_sb(sb, 100);
        if (IS_ERR(jh)) {
                CERROR("Fail to start trans for create [%s], rc = %ld\n",
                       osd_scrub_name, PTR_ERR(jh));
                RETURN((struct osd_scrub *)jh);
        }

        inode = ldiskfs_create_inode(jh, dir, (S_IFREG | S_IRUGO | S_IWUSR));
        if (IS_ERR(inode)) {
                ldiskfs_journal_stop(jh);
                CERROR("Fail to create [%s], rc = %ld\n",
                       osd_scrub_name, PTR_ERR(inode));
                RETURN((struct osd_scrub *)inode);
        }

        LU_IGIF_BUILD(fid, inode->i_ino, inode->i_generation);
        osd_get_ldiskfs_dirent_param(ldp, (const struct dt_rec *)fid);
        dentry->d_fsdata = (void *)ldp;

        rc = osd_ldiskfs_add_entry(jh, dentry, inode, NULL);
        ldiskfs_journal_stop(jh);
        iput(inode);
        if (rc != 0) {
                CERROR("Fail to add entry [%s], rc = %d\n",
                       osd_scrub_name, rc);
                RETURN(ERR_PTR(rc));
        }

locate:
        obj = dt_locate(env, dt, fid);
        if (unlikely(IS_ERR(obj))) {
                CERROR("Fail to locate [%s] fid = "DFID" rc = %ld\n",
                       osd_scrub_name, PFID(fid), PTR_ERR(obj));
                RETURN((struct osd_scrub *)obj);
        }

        OBD_ALLOC_PTR(scrub);
        if (unlikely(scrub == NULL)) {
                lu_object_put(env, &obj->do_lu);
                RETURN(ERR_PTR(-ENOMEM));
        }

        cfs_init_rwsem(&scrub->os_rwsem);
        scrub->os_obj = obj;
        rc = osd_scrub_file_load(env, scrub);
        if (rc == -ENOENT) {
                osd_scrub_file_init(env, scrub);
                rc = 0;
        } else if (rc != 0) {
                GOTO(out, rc);
        }

        sf = &scrub->os_file;
        if (*restored != 0) {
                osd_scrub_reset(scrub, SF_RESTORED);
        } else if (sf->sf_status == SS_SCANNING) {
                sf->sf_status = SS_CRASHED;
                scrub->os_dirty = 1;
        }

        rc = osd_scrub_file_store(env, scrub);
        GOTO(out, rc);

out:
        if (rc != 0) {
                lu_object_put(env, &obj->do_lu);
                OBD_FREE_PTR(scrub);
                scrub = ERR_PTR(rc);
        }
        return scrub;
}

static int osd_scrub_restore(const struct lu_env *env, struct osd_device *dev)
{
        struct osd_thread_info      *info   = osd_oti_get(env);
        struct lu_fid               *fid    = &info->oti_fid;
        struct osd_inode_id         *id     = &info->oti_id;
        struct super_block          *sb     = osd_sb(dev);
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

        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub_name,
                                           strlen(osd_scrub_name));
        bh = osd_ldiskfs_find_entry(dir, dentry, &de, NULL);
        if (unlikely(bh == NULL)) {
                CERROR("Fail to find [%s] for scrub_store_fid\n",
                       osd_scrub_name);
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

        inode = osd_iget(info, dev, id, fid, 0);
        LU_IGIF_BUILD(fid, inode->i_ino, inode->i_generation);
        osd_get_ldiskfs_dirent_param(ldp, (const struct dt_rec *)fid);
        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub_name,
                                           strlen(osd_scrub_name));
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

int osd_scrub_setup(const struct lu_env *env, struct osd_device *dev)
{
        struct osd_scrub  *scrub;
        struct scrub_file *sf;
        int                restored = 0;
        int                rc       = 0;
        ENTRY;

        scrub = osd_scrub_init(env, &dev->od_dt_dev, &restored);
        if (IS_ERR(scrub))
                RETURN(PTR_ERR(scrub));

        dev->od_scrub = scrub;
        if (restored != 0)
                rc = osd_scrub_restore(env, dev);

        sf = &scrub->os_file;
        if (rc == 0 && !dev->od_fl_noauto_scrub &&
            ((sf->sf_status == SS_CRASHED && (sf->sf_flags & SF_RESTORED ||
                                              sf->sf_flags & SF_BYRPC)) ||
             (sf->sf_status == SS_INIT && sf->sf_flags & SF_RESTORED)))
                rc = osd_scrub_start(env, dev, 0);

        RETURN(rc);
}

void osd_scrub_cleanup(const struct lu_env *env, struct osd_device *dev)
{
        if (dev->od_otable_it != NULL)
                osd_scrub_stop(env, dev);

        LASSERT(cfs_list_empty(&dev->od_inconsistent_items));

        if (dev->od_scrub != NULL) {
                struct osd_scrub *scrub = dev->od_scrub;

                dev->od_scrub = NULL;
                if (scrub->os_obj != NULL)
                        lu_object_put(env, &scrub->os_obj->do_lu);
                OBD_FREE_PTR(scrub);
        }
}

static struct dt_it *osd_otable_it_init(const struct lu_env *env,
                                       struct dt_object *dt, __u32 attr,
                                       struct lustre_capa *capa)
{
        enum dt_scrub_flags     flags   = attr >> DT_SCRUB_FLAGS_SHIFT;
        enum dt_scrub_valid     valid   = attr & ~DT_SCRUB_FLAGS_MASK;
        struct osd_device      *dev     = osd_dev(dt->do_lu.lo_dev);
        struct osd_scrub       *scrub   = dev->od_scrub;
        struct scrub_file      *sf      = &scrub->os_file;
        struct osd_otable_it   *it;
        struct l_wait_info      lwi     = { 0 };
        int                     rc      = 0;
        ENTRY;

        cfs_mutex_lock(&dev->od_otable_it_mutex);
        cfs_spin_lock(&dev->od_otable_it_lock);
        it = dev->od_otable_it;
        if (it != NULL) {
                if (it->ooi_used_outside) {
                        it = ERR_PTR(-EALREADY);
                } else {
                        LASSERT(it->ooi_oi_scrub);

                        it->ooi_used_outside = 1;
                        if (flags & DSF_FULL_SCAN)
                                it->ooi_full_scan = 1;
                        ooi_get(it);
                }
                cfs_spin_unlock(&dev->od_otable_it_lock);
                cfs_mutex_unlock(&dev->od_otable_it_mutex);
                RETURN((struct dt_it *)it);
        }

        cfs_spin_unlock(&dev->od_otable_it_lock);
        it = ooi_init(dev);
        if (unlikely(IS_ERR(it)))
                GOTO(out, rc = PTR_ERR(it));

        if (sf->sf_flags & SF_BYRPC) {
                sf->sf_flags &= ~SF_BYRPC;
                scrub->os_dirty = 1;
        }

        if (valid & DSV_ERROR_HANDLE) {
                if ((flags & DSF_FAILOUT) &&
                   !(sf->sf_param & SP_FAILOUT)) {
                        sf->sf_param |= SP_FAILOUT;
                        scrub->os_dirty = 1;
                } else if ((sf->sf_param & SP_FAILOUT) &&
                          !(flags & DSF_FAILOUT)) {
                        sf->sf_param &= ~SP_FAILOUT;
                        scrub->os_dirty = 1;
                }
        }

        it->ooi_used_outside = 1;
        if ((valid & DSV_OI_SCRUB && flags & DSF_OI_SCRUB) ||
            (!(valid & DSV_OI_SCRUB) && (sf->sf_status == SS_CRASHED ||
                                         sf->sf_flags & SF_RESTORED)))
                it->ooi_oi_scrub = 1;
        if (flags & DSF_FULL_SCAN)
                it->ooi_full_scan = 1;

        rc = cfs_create_thread(osd_scrub_main, it, 0);
        if (rc < 0) {
                CERROR("Cannot start iteration thread: rc %d\n", rc);
                ooi_put(it);
                GOTO(out, rc);
        }

        l_wait_event(it->ooi_thread.t_ctl_waitq,
                     thread_is_running(&it->ooi_thread) ||
                     thread_is_stopped(&it->ooi_thread),
                     &lwi);

        GOTO(out, rc = 0);

out:
        if (rc != 0) {
                if (scrub->os_dirty != 0) {
                        osd_scrub_file_to_cpu(sf, &scrub->os_file_disk);
                        scrub->os_dirty = 0;
                }
                it = ERR_PTR(rc);
        }
        cfs_mutex_unlock(&dev->od_otable_it_mutex);
        return (struct dt_it *)it;
}

static void osd_otable_it_fini(const struct lu_env *env, struct dt_it *di)
{
        struct osd_otable_it *it     = (struct osd_otable_it *)di;
        struct ptlrpc_thread *thread = &it->ooi_thread;
        struct osd_device    *dev    = it->ooi_dev;
        struct l_wait_info    lwi    = { 0 };
        ENTRY;

        cfs_mutex_lock(&dev->od_otable_it_mutex);
        cfs_spin_lock(&dev->od_otable_it_lock);
        LASSERT(dev->od_otable_it != NULL);

        if (!thread_is_stopped(thread)) {
                thread_set_flags(thread, SVC_STOPPING);
                cfs_spin_unlock(&dev->od_otable_it_lock);
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
                cfs_spin_lock(&dev->od_otable_it_lock);
        }
        dev->od_otable_it = NULL;
        cfs_spin_unlock(&dev->od_otable_it_lock);
        cfs_mutex_unlock(&dev->od_otable_it_mutex);
        ooi_put(it);

        EXIT;
}

/**
 * Set the OSD layer iteration start pooition as the specified key.
 *
 * The LFSCK out of OSD layer does not know the detail of the key, so if there
 * are several keys, they cannot be compared out of OSD, so call "::get()" for
 * each key, and OSD will select the smallest one by itself.
 */
static int osd_otable_it_get(const struct lu_env *env,
                            struct dt_it *di, const struct dt_key *key)
{
        struct osd_otable_it *it  = (struct osd_otable_it *)di;
        const char           *str = (const char *)key;
        __u32                 ino = 0;
        int                   rc  = 0;
        ENTRY;

        /* Forbid to set iteration position after iteration started. */
        if (it->ooi_user_ready)
                RETURN(-EPERM);

        if (str[0] != '\0')
                rc = sscanf(str, "%u", &ino);
        if (rc == 0 && it->ooi_pos_self_preload > ino)
                it->ooi_pos_self_preload = ino;

        RETURN(rc);
}

static int osd_scrub_self_preload(const struct lu_env *env,
                                  struct osd_otable_it *it)
{
        struct osd_otable_cache *ooc    = &it->ooi_cache;
        struct ptlrpc_thread    *thread = &it->ooi_thread;
        struct super_block      *sb     = it->ooi_sb;
        struct osd_thread_info  *info   = osd_oti_get(env);
        struct buffer_head      *bitmap = NULL;
        __u32                    max    =
                        le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        int                      rc;
        ENTRY;

        while (it->ooi_pos_self_preload <= max &&
               ooc->ooc_cached_items < OSD_OTABLE_IT_CACHE_SIZE &&
               (!thread_is_running(thread) ||
                it->ooi_pos_self_preload < it->ooi_pos_current)) {
                struct osd_idmap_cache *oic;
                struct inode *inode;
                ldiskfs_group_t bg = (it->ooi_pos_self_preload - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (it->ooi_pos_self_preload - 1) %
                               LDISKFS_INODES_PER_GROUP(sb);
                __u32 gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(sb);

                bitmap = ldiskfs_read_inode_bitmap(sb, bg);
                if (bitmap == NULL) {
                        CERROR("Fail to read bitmap for %u, "
                               "scrub will stop, urgent mode.\n", (__u32)bg);
                        RETURN(-EIO);
                }

                while (offset < LDISKFS_INODES_PER_GROUP(sb) &&
                       ooc->ooc_cached_items < OSD_OTABLE_IT_CACHE_SIZE &&
                       (!thread_is_running(thread) ||
                        it->ooi_pos_self_preload < it->ooi_pos_current)) {
                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                it->ooi_pos_self_preload = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        oic = &ooc->ooc_cache[ooc->ooc_producer_idx];
                        it->ooi_pos_self_preload = gbase + offset;
                        osd_id_gen(&oic->oic_lid, it->ooi_pos_self_preload,
                                   OSD_OII_NOGEN);
                        inode = osd_iget(info, it->ooi_dev, &oic->oic_lid,
                                         &oic->oic_fid,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        it->ooi_pos_self_preload = gbase +
                                                                   ++offset;
                                        continue;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d, urgent mode.\n",
                                       bg, it->ooi_pos_self_preload, rc);
                                brelse(bitmap);
                                /* If up layer LFSCK ignore the failure,
                                 * we can skip the inode next time. */
                                it->ooi_pos_self_preload = gbase + offset + 1;
                                RETURN(rc);
                        }

                        iput(inode);
                        it->ooi_pos_self_preload = gbase + ++offset;
                        ooc->ooc_cached_items++;
                        ooc->ooc_producer_idx = (ooc->ooc_producer_idx + 1) &
                                                ~OSD_OTABLE_IT_CACHE_MASK;
                }
        }

        if (it->ooi_pos_self_preload > max) {
                CDEBUG(D_SCRUB, "OSD self pre-loaded all: pid = %d\n",
                       cfs_curproc_pid());

                it->ooi_all_cached = 1;
                cfs_waitq_broadcast(&it->ooi_thread.t_ctl_waitq);
        } else {
                brelse(bitmap);
        }

        RETURN(ooc->ooc_cached_items);
}

static int osd_otable_it_next(const struct lu_env *env, struct dt_it *di)
{
        struct osd_otable_it    *it     = (struct osd_otable_it *)di;
        struct osd_device       *dev    = it->ooi_dev;
        struct osd_otable_cache *ooc    = &it->ooi_cache;
        struct ptlrpc_thread    *thread = &it->ooi_thread;
        struct l_wait_info       lwi    = { 0 };
        int                      rc;
        ENTRY;

        LASSERT(it->ooi_user_ready);

again:
        it->ooi_wait_next = 1;
        l_wait_event(thread->t_ctl_waitq,
                     ooc->ooc_cached_items > 0 || it->ooi_all_cached ||
                     (it->ooi_no_cache &&
                      it->ooi_pos_self_preload < it->ooi_pos_current) ||
                     !thread_is_running(thread),
                     &lwi);
        it->ooi_wait_next = 0;

        if (!thread_is_running(thread) && !it->ooi_full_scan)
                RETURN(+1);

        if (ooc->ooc_cached_items > 0) {
                int wakeup = 0;

                if (!it->ooi_no_cache) {
                        cfs_spin_lock(&dev->od_otable_it_lock);
                        if (ooc->ooc_cached_items == OSD_OTABLE_IT_CACHE_SIZE)
                                wakeup = 1;
                }
                ooc->ooc_consumer_slot = ooc->ooc_cache[ooc->ooc_consumer_idx];
                ooc->ooc_cached_items--;
                if (!it->ooi_no_cache) {
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        if (wakeup != 0)
                                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                }
                ooc->ooc_consumer_idx = (ooc->ooc_consumer_idx + 1) &
                                        ~OSD_OTABLE_IT_CACHE_MASK;
                RETURN(0);
        }

        if (it->ooi_all_cached) {
                if (unlikely(ooc->ooc_cached_items > 0))
                        goto again;
                RETURN(+1);
        }

        cfs_spin_lock(&dev->od_otable_it_lock);
        if (thread_is_running(thread)) {
                if (unlikely(!it->ooi_no_cache)) {
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        goto again;
                }
        } else {
                if (unlikely(!it->ooi_full_scan)) {
                        cfs_spin_unlock(&dev->od_otable_it_lock);
                        RETURN(+1);
                }
        }

        it->ooi_self_preload = 1;
        cfs_spin_unlock(&dev->od_otable_it_lock);

        rc = osd_scrub_self_preload(env, it);
        cfs_spin_lock(&dev->od_otable_it_lock);
        it->ooi_self_preload = 0;
        cfs_spin_unlock(&dev->od_otable_it_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (rc >= 0)
                goto again;
        RETURN(rc);
}

static struct dt_key *osd_otable_it_key(const struct lu_env *env,
                                        const struct dt_it *di)
{
        struct osd_otable_it *it = (struct osd_otable_it *)di;

        sprintf(it->ooi_key, "%u",
                it->ooi_cache.ooc_consumer_slot.oic_lid.oii_ino);
        return (struct dt_key *)it->ooi_key;
}

static int osd_otable_it_key_size(const struct lu_env *env,
                                  const struct dt_it *di)
{
        return sizeof(((struct osd_otable_it *)di)->ooi_key);
}

static int osd_otable_it_rec(const struct lu_env *env, const struct dt_it *di,
                             struct dt_rec *rec, __u32 attr)
{
        *(struct lu_fid *)rec =
                ((struct osd_otable_it *)di)->ooi_cache.ooc_consumer_slot.oic_fid;
        return 0;
}

static int osd_otable_it_load(const struct lu_env *env,
                              const struct dt_it *di, __u64 hash)
{
        struct osd_otable_it *it = (struct osd_otable_it *)di;

        if (it->ooi_user_ready)
                return 0;

        cfs_spin_lock(&it->ooi_dev->od_otable_it_lock);
        if (it->ooi_pos_self_preload == ~0U)
                it->ooi_pos_self_preload = it->ooi_pos_current;
        else if (it->ooi_pos_self_preload < LDISKFS_FIRST_INO(it->ooi_sb))
                it->ooi_pos_self_preload = LDISKFS_FIRST_INO(it->ooi_sb);
        else
                /* Skip the one that has been processed last time. */
                it->ooi_pos_self_preload++;

        if (!it->ooi_no_cache)
                it->ooi_pos_current = it->ooi_pos_self_preload;
        it->ooi_user_ready = 1;
        cfs_spin_unlock(&it->ooi_dev->od_otable_it_lock);
        cfs_waitq_broadcast(&it->ooi_thread.t_ctl_waitq);

        /* Unplug OSD layer iteration by the first next() call. */
        return osd_otable_it_next(env, (struct dt_it *)it);
}

const struct dt_index_operations osd_otable_ops = {
        .dio_it             = {
                .init     = osd_otable_it_init,
                .fini     = osd_otable_it_fini,
                .get      = osd_otable_it_get,
                .next     = osd_otable_it_next,
                .key      = osd_otable_it_key,
                .key_size = osd_otable_it_key_size,
                .rec      = osd_otable_it_rec,
                .load     = osd_otable_it_load,
        }
};
