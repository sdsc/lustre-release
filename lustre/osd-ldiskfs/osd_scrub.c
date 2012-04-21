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
 * The otable based iterator scans ldiskfs inode table to feed up layer LFSCK.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <lustre/lustre_idl.h>
#include <lustre_disk.h>
#include <dt_object.h>

#include "osd_internal.h"
#include "osd_oi.h"
#include "osd_scrub.h"

#define HALF_SEC        (CFS_HZ >> 1)

static void osd_scrub_file_to_cpu(struct scrub_file *des,
                                  struct scrub_file *src)
{
        memcpy(des->sf_uuid, src->sf_uuid, 16);
        des->sf_version = le16_to_cpu(src->sf_version);
        des->sf_status  = le16_to_cpu(src->sf_status);
        des->sf_flags   = le16_to_cpu(src->sf_flags);
        des->sf_param   = le16_to_cpu(src->sf_param);
        des->sf_time_last_complete      =
                                le64_to_cpu(src->sf_time_last_complete);
        des->sf_time_latest_start       =
                                le64_to_cpu(src->sf_time_latest_start);
        des->sf_time_last_checkpoint    =
                                le64_to_cpu(src->sf_time_last_checkpoint);
        des->sf_pos_latest_start        =
                                le64_to_cpu(src->sf_pos_latest_start);
        des->sf_pos_last_checkpoint     =
                                le64_to_cpu(src->sf_pos_last_checkpoint);
        des->sf_pos_first_inconsistent  =
                                le64_to_cpu(src->sf_pos_first_inconsistent);
        des->sf_items_checked           =
                                le64_to_cpu(src->sf_items_checked);
        des->sf_items_updated           =
                                le64_to_cpu(src->sf_items_updated);
        des->sf_items_failed            =
                                le64_to_cpu(src->sf_items_failed);
        des->sf_items_updated_prior     =
                                le64_to_cpu(src->sf_items_updated_prior);
        des->sf_success_count   = le32_to_cpu(src->sf_success_count);
        des->sf_run_time        = le32_to_cpu(src->sf_run_time);
}

static void osd_scrub_file_to_le(struct scrub_file *des,
                                 struct scrub_file *src)
{
        memcpy(des->sf_uuid, src->sf_uuid, 16);
        des->sf_version = cpu_to_le16(src->sf_version);
        des->sf_status  = cpu_to_le16(src->sf_status);
        des->sf_flags   = cpu_to_le16(src->sf_flags);
        des->sf_param   = cpu_to_le16(src->sf_param);
        des->sf_time_last_complete      =
                                cpu_to_le64(src->sf_time_last_complete);
        des->sf_time_latest_start       =
                                cpu_to_le64(src->sf_time_latest_start);
        des->sf_time_last_checkpoint    =
                                cpu_to_le64(src->sf_time_last_checkpoint);
        des->sf_pos_latest_start        =
                                cpu_to_le64(src->sf_pos_latest_start);
        des->sf_pos_last_checkpoint     =
                                cpu_to_le64(src->sf_pos_last_checkpoint);
        des->sf_pos_first_inconsistent  =
                                cpu_to_le64(src->sf_pos_first_inconsistent);
        des->sf_items_checked           =
                                cpu_to_le64(src->sf_items_checked);
        des->sf_items_updated           =
                                cpu_to_le64(src->sf_items_updated);
        des->sf_items_failed            =
                                cpu_to_le64(src->sf_items_failed);
        des->sf_items_updated_prior     =
                                cpu_to_le64(src->sf_items_updated_prior);
        des->sf_success_count   = cpu_to_le32(src->sf_success_count);
        des->sf_run_time        = cpu_to_le32(src->sf_run_time);
}

static void osd_scrub_file_init(struct osd_scrub *scrub, __u8 *uuid)
{
        struct scrub_file *sf = &scrub->os_file;

        memset(sf, 0, sizeof(*sf));
        memcpy(sf->sf_uuid, uuid, 16);
        sf->sf_version = SCRUB_VERSION_V1;
        sf->sf_status = SS_INIT;
        scrub->os_dirty = 1;
}

static void osd_scrub_file_reset(struct osd_scrub *scrub, __u8 *uuid,
                                 __u16 flags)
{
        struct scrub_file *sf = &scrub->os_file;

        CDEBUG(D_SCRUB, "Reset OI scrub file, flags = 0x%x\n", flags);
        memcpy(sf->sf_uuid, uuid, 16);
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
        scrub->os_dirty = 1;
}

static int osd_scrub_file_load(struct osd_scrub *scrub)
{
        loff_t pos = 0;
        int    len = sizeof(scrub->os_file_disk);
        int    rc;

        rc = osd_ldiskfs_read(scrub->os_inode, &scrub->os_file_disk, len, &pos);
        if (rc == len) {
                osd_scrub_file_to_cpu(&scrub->os_file, &scrub->os_file_disk);
                rc = 0;
        } else if (rc != 0) {
                CERROR("Fail to load scrub file: rc = %d, expected = %d.\n",
                       rc, len);
                if (rc > 0)
                        rc = -EFAULT;
        } else {
                /* return -ENOENT for empty scrub file case. */
                rc = -ENOENT;
        }

        return rc;
}

static int osd_scrub_file_store(struct osd_scrub *scrub)
{
        struct osd_device *dev;
        handle_t          *jh;
        loff_t             pos     = 0;
        int                len     = sizeof(scrub->os_file_disk);
        int                credits;
        int                rc;

        if (!scrub->os_dirty)
                return 0;

        dev = container_of0(scrub, struct osd_device, od_scrub);
        credits = osd_dto_credits_noquota[DTO_WRITE_BASE] +
                  osd_dto_credits_noquota[DTO_WRITE_BLOCK];
        jh = ldiskfs_journal_start_sb(osd_sb(dev), credits);
        if (IS_ERR(jh)) {
                rc = PTR_ERR(jh);
                CERROR("Fail to start trans for scrub store: rc = %d\n", rc);
                return rc;
        }

        osd_scrub_file_to_le(&scrub->os_file_disk, &scrub->os_file);
        rc = osd_ldiskfs_write_record(scrub->os_inode, &scrub->os_file_disk,
                                      len, &pos, jh);
        ldiskfs_journal_stop(jh);
        if (rc == 0)
                scrub->os_dirty = 0;
        else
                CERROR("Fail to store scrub file: rc = %d, expected = %d.\n",
                       rc, len);

        return rc;
}

static int osd_scrub_prep(struct osd_device *dev)
{
        struct osd_scrub  *scrub = &dev->od_scrub;
        struct scrub_file *sf    = &scrub->os_file;
        __u32              flags = scrub->os_start_flags;
        int                rc;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if ((flags & SS_SET_FAILOUT) && !(sf->sf_param & SP_FAILOUT))
                sf->sf_param |= SP_FAILOUT;

        if ((flags & SS_CLEAR_FAILOUT) && (sf->sf_param & SP_FAILOUT))
                sf->sf_param &= ~SP_FAILOUT;

        if (flags & SS_RESET)
                osd_scrub_file_reset(scrub,
                        LDISKFS_SB(osd_sb(dev))->s_es->s_uuid, sf->sf_flags);

        if (flags & SS_AUTO) {
                scrub->os_full_speed = 1;
                if (!(sf->sf_flags & SF_AUTO))
                        sf->sf_flags |= SF_AUTO;
        } else {
                scrub->os_full_speed = 0;
        }

        if (sf->sf_flags & SF_RESTORED)
                scrub->os_full_speed = 1;

        scrub->os_in_prior = 0;
        scrub->os_skip_obj = 0;
        scrub->os_waiting = 0;
        scrub->os_new_checked = 0;
        if (sf->sf_pos_last_checkpoint != 0)
                sf->sf_pos_latest_start = sf->sf_pos_last_checkpoint + 1;
        else
                sf->sf_pos_latest_start = LDISKFS_FIRST_INO(osd_sb(dev));

        scrub->os_pos_current = sf->sf_pos_latest_start;
        sf->sf_status = SS_SCANNING;
        sf->sf_time_latest_start = cfs_time_current_sec();
        sf->sf_time_last_checkpoint = sf->sf_time_latest_start;
        scrub->os_dirty = 1;
        rc = osd_scrub_file_store(scrub);
        scrub->os_time_last_checkpoint = cfs_time_current();
        scrub->os_time_next_checkpoint = scrub->os_time_last_checkpoint +
                                cfs_time_seconds(SCRUB_CHECKPOING_INTERVAL);
        cfs_up_write(&scrub->os_rwsem);

        RETURN(rc);
}

static int osd_scrub_update(const struct lu_env *env, struct osd_device *dev,
                            int result)
{
        struct osd_scrub       *scrub  = &dev->od_scrub;
        struct scrub_file      *sf     = &scrub->os_file;
        struct osd_idmap_cache *oic    = scrub->os_oicp;
        struct lu_fid          *fid    = &oic->oic_fid;
        struct osd_inode_id    *lid    = &oic->oic_lid;
        struct osd_thread_info *info   = osd_oti_get(env);
        struct osd_inode_id    *lid2   = &info->oti_id;
        struct lu_fid          *oi_fid = &info->oti_fid;
        struct osd_inode_id    *oi_id  = &info->oti_id;
        handle_t               *jh     = NULL;
        struct iam_container   *bag;
        struct iam_path_descr  *ipd;
        ENTRY;

        if (lid->oii_ino < sf->sf_pos_latest_start && !scrub->os_in_prior)
                RETURN(0);

        cfs_down_write(&scrub->os_rwsem);
        scrub->os_new_checked++;
        if (fid_is_igif(fid) || fid_is_idif(fid) ||
            fid_seq(fid) == FID_SEQ_LLOG || fid_seq(fid) == FID_SEQ_LOCAL_FILE)
                GOTO(out, result = 0);

        if (result != 0)
                GOTO(out, result);

        result = osd_oi_lookup(info, dev, fid, lid2);
        if (result != 0)
                /* XXX: Either the inode is removed by others or someone just
                 *      created the inode, but not inserted it into OI yet. */
                GOTO(out, result = (result == -ENOENT ? 0 : result));

        if (osd_id_eq(lid, lid2))
                GOTO(out, result = 0);

        fid_cpu_to_be(oi_fid, fid);
        osd_id_pack(oi_id, &oic->oic_lid);
        jh = ldiskfs_journal_start_sb(osd_sb(dev),
                                osd_dto_credits_noquota[DTO_INDEX_UPDATE]);
        if (IS_ERR(jh)) {
                result = PTR_ERR(jh);
                CERROR("Fail to start trans for scrub store: rc = %d\n", result);
                GOTO(out, result);
        }

        bag = &osd_fid2oi(dev, fid)->oi_dir.od_container;
        ipd = osd_idx_ipd_get(env, bag);
        if (unlikely(ipd == NULL)) {
                ldiskfs_journal_stop(jh);
                CERROR("Fail to get ipd for scrub store\n");
                GOTO(out, result = -ENOMEM);
        }

        result = iam_update(jh, bag, (const struct iam_key *)oi_fid,
                            (struct iam_rec *)oi_id, ipd);
        osd_ipd_put(env, bag, ipd);
        ldiskfs_journal_stop(jh);
        if (result == 0) {
                if (scrub->os_in_prior)
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

static int do_osd_scrub_checkpoint(struct osd_scrub *scrub)
{
        struct scrub_file *sf = &scrub->os_file;
        int                rc;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if (scrub->os_new_checked > 0) {
                sf->sf_items_checked += scrub->os_new_checked;
                scrub->os_new_checked = 0;
                sf->sf_pos_last_checkpoint = scrub->os_pos_current;
                scrub->os_dirty = 1;
        }
        if (scrub->os_dirty)
                sf->sf_time_last_checkpoint = cfs_time_current_sec();
        sf->sf_run_time += cfs_duration_sec(cfs_time_current() -
                                scrub->os_time_last_checkpoint + HALF_SEC);
        rc = osd_scrub_file_store(scrub);
        scrub->os_time_last_checkpoint = cfs_time_current();
        scrub->os_time_next_checkpoint = scrub->os_time_last_checkpoint +
                                cfs_time_seconds(SCRUB_CHECKPOING_INTERVAL);
        cfs_up_write(&scrub->os_rwsem);

        RETURN(rc);
}

static inline int osd_scrub_checkpoint(struct osd_scrub *scrub)
{
        if (cfs_time_beforeq(scrub->os_time_next_checkpoint,
                             cfs_time_current()))
                return do_osd_scrub_checkpoint(scrub);
        return 0;
}

static void osd_scrub_post(struct osd_scrub *scrub, int result)
{
        struct scrub_file *sf    = &scrub->os_file;
        __u64              ctime;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        ctime = cfs_time_current_sec();
        if (scrub->os_new_checked > 0) {
                sf->sf_items_checked += scrub->os_new_checked;
                scrub->os_new_checked = 0;
                sf->sf_pos_last_checkpoint = scrub->os_pos_current;
        }
        sf->sf_time_last_checkpoint = ctime;
        if (result > 0) {
                sf->sf_status = SS_COMPLETED;
                sf->sf_flags &= ~(SF_RESTORED | SF_AUTO);
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
        result = osd_scrub_file_store(scrub);
        if (result < 0)
                CERROR("Fail to osd_scrub_post: rc = %d\n", result);

        scrub->os_dirty = 0;
        cfs_up_write(&scrub->os_rwsem);

        EXIT;
}

static inline int osd_scrub_has_window(struct osd_scrub *scrub,
                                       struct osd_otable_cache *ooc)
{
        return scrub->os_pos_current < ooc->ooc_pos_preload + SCRUB_WINDOW_SIZE;
}

static int osd_scrub_main(void *args)
{
        struct lu_env                 env;
        struct osd_thread_info       *info;
        struct osd_device            *dev    = (struct osd_device *)args;
        struct osd_scrub             *scrub  = &dev->od_scrub;
        struct ptlrpc_thread         *thread = &scrub->os_thread;
        cfs_list_t                   *list   = &scrub->os_inconsistent_items;
        struct osd_otable_it         *it     = dev->od_otable_it;
        struct l_wait_info            lwi    = { 0 };
        struct super_block           *sb     = osd_sb(dev);
        struct osd_inconsistent_item *oii    = NULL;
        struct osd_otable_cache      *ooc    = NULL;
        int                           noslot = 0;
        int                           rc;
        __u32                         max;
        ENTRY;

        cfs_daemonize("OI_scrub");
        rc = lu_env_init(&env, LCT_DT_THREAD);
        if (rc != 0) {
                CERROR("OI scrub: fail to init env: rc = %d\n", rc);
                GOTO(noenv, rc);
        }

        info = osd_oti_get(&env);
        rc = osd_scrub_prep(dev);
        if (rc != 0) {
                CERROR("OI scrub: fail to scrub prep: rc = %d\n", rc);
                GOTO(out, rc);
        }

        cfs_spin_lock(&scrub->os_lock);
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&scrub->os_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (!scrub->os_full_speed) {
                LASSERT(it != NULL);

                ooc = &it->ooi_cache;
                l_wait_event(thread->t_ctl_waitq,
                             it->ooi_user_ready || !thread_is_running(thread),
                             &lwi);
                if (unlikely(!thread_is_running(thread)))
                        GOTO(post, rc = 0);

                LASSERT(scrub->os_pos_current >= ooc->ooc_pos_preload);
                scrub->os_pos_current = ooc->ooc_pos_preload;
        }

        CDEBUG(D_SCRUB, "OI scrub: flags = 0x%x, pos = %u\n",
               scrub->os_start_flags, scrub->os_pos_current);

        max = le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        while (scrub->os_pos_current <= max) {
                struct inode *inode;
                struct buffer_head *bitmap = NULL;
                ldiskfs_group_t bg = (scrub->os_pos_current - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (scrub->os_pos_current - 1) %
                               LDISKFS_INODES_PER_GROUP(sb);
                __u32 gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(sb);

                bitmap = ldiskfs_read_inode_bitmap(sb, bg);
                if (bitmap == NULL) {
                        CERROR("Fail to read bitmap at pos = %u, bg = %u, "
                               "scrub will stop.\n",
                               scrub->os_pos_current, (__u32)bg);
                        GOTO(post, rc = -EIO);
                }

                while (offset < LDISKFS_INODES_PER_GROUP(sb)) {
                        if (unlikely(!thread_is_running(thread))) {
                                brelse(bitmap);
                                GOTO(post, rc = 0);
                        }

                        /* High-prior items firstly. */
                        if (!cfs_list_empty(list)) {
                                oii = cfs_list_entry(list->next,
                                        struct osd_inconsistent_item, oii_list);
                                scrub->os_oicp = &oii->oii_cache;
                                scrub->os_in_prior = 1;
                                goto update;
                        } else if (!scrub->os_full_speed && noslot != 0) {
                                goto wait;
                        }

                        /* Normal scan next. */
                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                scrub->os_pos_current = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        scrub->os_oicp = &scrub->os_oic;
                        scrub->os_pos_current = gbase + offset;
                        osd_id_gen(&scrub->os_oicp->oic_lid,
                                   scrub->os_pos_current, OSD_OII_NOGEN);
                        inode = osd_iget(info, dev, &scrub->os_oicp->oic_lid,
                                         &scrub->os_oicp->oic_fid,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        scrub->os_skip_obj = 1;
                                        goto checkpoint;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d\n",
                                       bg, scrub->os_pos_current, rc);
                        } else {
                                iput(inode);
                        }

update:
                        rc = osd_scrub_update(&env, dev, rc);
                        if (oii != NULL) {
                                cfs_spin_lock(&scrub->os_lock);
                                cfs_list_del_init(&oii->oii_list);
                                cfs_spin_unlock(&scrub->os_lock);
                                lu_object_put(&env, oii->oii_obj);
                                OBD_FREE_PTR(oii);
                                oii = NULL;
                        }

                        if (rc != 0) {
                                brelse(bitmap);
                                GOTO(post, rc);
                        }

checkpoint:
                        rc = osd_scrub_checkpoint(scrub);
                        if (rc != 0) {
                                CERROR("Fail to checkpoint: pos = %u,rc = %d\n",
                                       scrub->os_pos_current, rc);
                                brelse(bitmap);
                                GOTO(post, rc);
                        }

                        if (scrub->os_in_prior) {
                                scrub->os_in_prior = 0;
                                continue;
                        }

                        scrub->os_pos_current = gbase + ++offset;
                        if (scrub->os_skip_obj) {
                                scrub->os_skip_obj = 0;
                                continue;
                        }

                        if (dev->od_otable_it != NULL &&
                            dev->od_otable_it->ooi_waiting) {
                                dev->od_otable_it->ooi_waiting = 0;
                                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                        }

                        if (scrub->os_full_speed)
                                continue;

wait:
                        if (osd_scrub_has_window(scrub, ooc)) {
                                noslot = 0;
                                continue;
                        }

                        scrub->os_waiting = 1;
                        l_wait_event(thread->t_ctl_waitq,
                                     osd_scrub_has_window(scrub, ooc) ||
                                     !cfs_list_empty(list) ||
                                     !thread_is_running(thread),
                                     &lwi);
                        scrub->os_waiting = 0;

                        if (osd_scrub_has_window(scrub, ooc))
                                noslot = 0;
                        else
                                noslot = 1;
                }
        }

        GOTO(post, rc = (scrub->os_pos_current > max ? 1 : rc));

post:
        osd_scrub_post(scrub, rc);
        CDEBUG(D_SCRUB, "OI scrub: stop, rc = %d, pos = %u\n",
               rc, scrub->os_pos_current);

out:
        cfs_spin_lock(&scrub->os_lock);
        thread_set_flags(thread, SVC_STOPPING);
        cfs_spin_unlock(&scrub->os_lock);
        while (!cfs_list_empty(list)) {
                oii = cfs_list_entry(list->next,
                                     struct osd_inconsistent_item, oii_list);
                cfs_list_del_init(&oii->oii_list);
                lu_object_put(&env, oii->oii_obj);
                OBD_FREE_PTR(oii);
        }
        lu_env_fini(&env);

noenv:
        cfs_spin_lock(&scrub->os_lock);
        thread_set_flags(thread, SVC_STOPPED);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);
        cfs_spin_unlock(&scrub->os_lock);
        return rc;
}

static int do_osd_scrub_start(struct osd_device *dev, __u32 flags)
{
        struct osd_scrub     *scrub  = &dev->od_scrub;
        struct ptlrpc_thread *thread = &scrub->os_thread;
        struct l_wait_info    lwi    = { 0 };
        int                   rc;
        ENTRY;

again:
        cfs_spin_lock(&scrub->os_lock);
        if (thread_is_running(thread)) {
                cfs_spin_unlock(&scrub->os_lock);
                RETURN(-EALREADY);
        } else if (unlikely(thread_is_stopping(thread))) {
                cfs_spin_unlock(&scrub->os_lock);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
                goto again;
        }
        cfs_spin_unlock(&scrub->os_lock);

        scrub->os_start_flags = flags;
        thread_set_flags(thread, 0);
        rc = cfs_create_thread(osd_scrub_main, dev, 0);
        if (rc < 0) {
                CERROR("Cannot start iteration thread: rc %d\n", rc);
                RETURN(rc);
        }

        l_wait_event(thread->t_ctl_waitq,
                     thread_is_running(thread) || thread_is_stopped(thread),
                     &lwi);

        RETURN(0);
}

int osd_scrub_start(struct osd_device *dev)
{
        __u32 flags = SS_AUTO;
        int   rc;
        ENTRY;

        if (dev->od_scrub.os_file.sf_status == SS_COMPLETED)
                flags |= SS_RESET;

        cfs_mutex_lock(&dev->od_mutex);
        rc = do_osd_scrub_start(dev, flags);
        cfs_mutex_unlock(&dev->od_mutex);

        RETURN(rc == -EALREADY ? 0 : rc);
}

static void do_osd_scrub_stop(struct osd_scrub *scrub)
{
        struct ptlrpc_thread *thread = &scrub->os_thread;
        struct l_wait_info    lwi    = { 0 };

        /* os_lock: sync status between stop and scrub thread */
        cfs_spin_lock(&scrub->os_lock);
        if (!thread_is_init(thread) && !thread_is_stopped(thread)) {
                thread_set_flags(thread, SVC_STOPPING);
                cfs_spin_unlock(&scrub->os_lock);
                cfs_waitq_broadcast(&thread->t_ctl_waitq);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
                /*XXX: Do not skip the last lock/unlock, which can guarantee
                 *     that the stop thread cannot return until the OI scrub
                 *     thread exit. */
                cfs_spin_lock(&scrub->os_lock);
        }
       cfs_spin_unlock(&scrub->os_lock);
}

static void osd_scrub_stop(struct osd_device *dev)
{
        /* od_mtext: prevent curcurrent start/stop */
        cfs_mutex_lock(&dev->od_mutex);
        do_osd_scrub_stop(&dev->od_scrub);
        cfs_mutex_unlock(&dev->od_mutex);
}

static const char osd_scrub_name[] = "OI_scrub";

int osd_scrub_setup(const struct lu_env *env, struct osd_device *dev)
{
        struct osd_scrub           *scrub  = &dev->od_scrub;
        struct scrub_file          *sf     = &scrub->os_file;
        struct osd_thread_info     *info   = osd_oti_get(env);
        struct osd_inode_id        *id     = &info->oti_id;
        struct super_block         *sb     = osd_sb(dev);
        struct inode               *dir    = sb->s_root->d_inode;
        struct ldiskfs_super_block *es     = LDISKFS_SB(sb)->s_es;
        struct dentry              *dentry;
        struct buffer_head         *bh;
        struct ldiskfs_dir_entry_2 *de;
        struct inode               *inode;
        handle_t                   *jh;
        int                         rc;
        ENTRY;

        cfs_waitq_init(&scrub->os_thread.t_ctl_waitq);
        cfs_init_rwsem(&scrub->os_rwsem);
        cfs_spin_lock_init(&scrub->os_lock);
        CFS_INIT_LIST_HEAD(&scrub->os_inconsistent_items);
        if (get_mount_flags(dev->od_mount->lmi_sb) & LMD_FLG_NOAUTO_SCRUB)
                scrub->os_noauto_scrub = 1;

        dentry = osd_child_dentry_by_inode(env, dir, osd_scrub_name,
                                           strlen(osd_scrub_name));
        bh = osd_ldiskfs_find_entry(dir, dentry, &de, NULL);
        if (bh) {
                osd_id_gen(id, le32_to_cpu(de->inode), OSD_OII_NOGEN);
                brelse(bh);
                inode = osd_iget(info, dev, id, NULL, 0);
                if (IS_ERR(inode)) {
                        rc = PTR_ERR(inode);
                        CERROR("Fail to find [%s], rc = %d\n",
                               osd_scrub_name, rc);
                        RETURN(rc);
                }

                scrub->os_inode = inode;
                rc = osd_scrub_file_load(scrub);
                if (rc == -ENOENT) {
                        osd_scrub_file_init(scrub, es->s_uuid);
                } else if (rc != 0) {
                        RETURN(rc);
                } else {
                        if (memcmp(sf->sf_uuid, es->s_uuid, 16) != 0) {
                                osd_scrub_file_reset(scrub, es->s_uuid,
                                                     SF_RESTORED);
                        } else if (sf->sf_status == SS_SCANNING) {
                                sf->sf_status = SS_CRASHED;
                                scrub->os_dirty = 1;
                        }
                }
        } else {
                jh = ldiskfs_journal_start_sb(sb, 100);
                if (IS_ERR(jh)) {
                        rc = PTR_ERR(jh);
                        CERROR("Fail to start trans for create [%s], rc = %d\n",
                               osd_scrub_name, rc);
                        RETURN(rc);
                }

                inode = ldiskfs_create_inode(jh, dir,
                                             S_IFREG | S_IRUGO | S_IWUSR);
                if (IS_ERR(inode)) {
                        ldiskfs_journal_stop(jh);
                        rc = PTR_ERR(inode);
                        CERROR("Fail to create [%s], rc = %d\n",
                               osd_scrub_name, rc);
                        RETURN(rc);
                }

                scrub->os_inode = inode;
                rc = osd_ldiskfs_add_entry(jh, dentry, inode, NULL);
                ldiskfs_journal_stop(jh);
                if (rc != 0) {
                        /* Do not care lost one inode. */
                        CERROR("Fail to add entry [%s], rc = %d\n",
                               osd_scrub_name, rc);
                        RETURN(rc);
                }

                osd_scrub_file_init(scrub, es->s_uuid);
        }

        rc = osd_scrub_file_store(scrub);
        if (rc != 0)
                RETURN(rc);

        if (rc == 0 && !scrub->os_noauto_scrub &&
            ((sf->sf_status == SS_CRASHED && (sf->sf_flags & SF_RESTORED ||
                                              sf->sf_flags & SF_AUTO)) ||
             (sf->sf_status == SS_INIT && sf->sf_flags & SF_RESTORED)))
                rc = osd_scrub_start(dev);

        RETURN(rc);
}

void osd_scrub_cleanup(const struct lu_env *env, struct osd_device *dev)
{
        struct osd_scrub *scrub = &dev->od_scrub;

        LASSERT(dev->od_otable_it == NULL);

        if (scrub->os_inode != NULL) {
                osd_scrub_stop(dev);
                iput(scrub->os_inode);
                scrub->os_inode = NULL;
        }
}
