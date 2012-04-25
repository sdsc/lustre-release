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
        if (rc != 0)
                CERROR("Fail to store scrub file: rc = %d, expected = %d.\n",
                       rc, len);
        scrub->os_time_last_checkpoint = cfs_time_current();
        scrub->os_time_next_checkpoint = scrub->os_time_last_checkpoint +
                                cfs_time_seconds(SCRUB_CHECKPOING_INTERVAL);
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
        rc = osd_scrub_file_store(scrub);
        cfs_up_write(&scrub->os_rwsem);

        RETURN(rc);
}

static int osd_scrub_check_update(const struct lu_env *env,
                                  struct osd_device *dev, int result)
{
        struct osd_scrub             *scrub  = &dev->od_scrub;
        struct scrub_file            *sf     = &scrub->os_file;
        struct osd_thread_info       *info   = osd_oti_get(env);
        struct osd_inode_id          *lid2   = &info->oti_id;
        struct lu_fid                *oi_fid = &info->oti_fid;
        struct osd_inode_id          *oi_id  = &info->oti_id;
        handle_t                     *jh     = NULL;
        struct osd_inconsistent_item *oii    = NULL;
        struct osd_idmap_cache       *oic;
        struct lu_fid                *fid;
        struct osd_inode_id          *lid;
        struct iam_container         *bag;
        struct iam_path_descr        *ipd;
        ENTRY;

        if (scrub->os_in_prior) {
                LASSERT(!cfs_list_empty(&scrub->os_inconsistent_items));

                cfs_spin_lock(&scrub->os_lock);
                oii = cfs_list_entry(scrub->os_inconsistent_items.next,
                                     struct osd_inconsistent_item, oii_list);
                cfs_list_del_init(&oii->oii_list);
                cfs_spin_unlock(&scrub->os_lock);
                oic = &oii->oii_cache;
        } else {
                oic = &scrub->os_oic;
        }

        lid = &oic->oic_lid;
        if (lid->oii_ino < sf->sf_pos_latest_start && !scrub->os_in_prior)
                RETURN(0);

        fid = &oic->oic_fid;
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
        } else if (result == -ENOENT) {
                result = 0;
        }

        GOTO(out, result);

out:
        if (result != 0) {
                sf->sf_items_failed++;
                if (sf->sf_pos_first_inconsistent == 0 ||
                    sf->sf_pos_first_inconsistent > lid->oii_ino)
                        sf->sf_pos_first_inconsistent = lid->oii_ino;
        }
        cfs_up_write(&scrub->os_rwsem);

        if (oii != NULL) {
                lu_object_put(env, oii->oii_obj);
                OBD_FREE_PTR(oii);
        }
        RETURN(sf->sf_param & SP_FAILOUT ? result : 0);
}

static int do_osd_scrub_checkpoint(struct osd_scrub *scrub)
{
        struct scrub_file *sf = &scrub->os_file;
        int                rc;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        sf->sf_items_checked += scrub->os_new_checked;
        scrub->os_new_checked = 0;
        sf->sf_pos_last_checkpoint = scrub->os_pos_current;
        sf->sf_time_last_checkpoint = cfs_time_current_sec();
        sf->sf_run_time += cfs_duration_sec(cfs_time_current() -
                                scrub->os_time_last_checkpoint + HALF_SEC);
        rc = osd_scrub_file_store(scrub);
        cfs_up_write(&scrub->os_rwsem);

        RETURN(rc);
}

static inline int osd_scrub_checkpoint(struct osd_scrub *scrub)
{
        if (cfs_time_beforeq(scrub->os_time_next_checkpoint,
                             cfs_time_current()) && scrub->os_new_checked > 0)
                return do_osd_scrub_checkpoint(scrub);
        return 0;
}

static void osd_scrub_post(struct osd_scrub *scrub, int result)
{
        struct scrub_file *sf = &scrub->os_file;
        ENTRY;

        cfs_down_write(&scrub->os_rwsem);
        if (scrub->os_new_checked > 0) {
                sf->sf_items_checked += scrub->os_new_checked;
                scrub->os_new_checked = 0;
                sf->sf_pos_last_checkpoint = scrub->os_pos_current;
        }
        sf->sf_time_last_checkpoint = cfs_time_current_sec();
        if (result > 0) {
                sf->sf_status = SS_COMPLETED;
                sf->sf_flags &= ~(SF_RESTORED | SF_AUTO);
                sf->sf_time_last_complete = sf->sf_time_last_checkpoint;
                sf->sf_success_count++;
        } else if (result == 0) {
                sf->sf_status = SS_PAUSED;
        } else {
                sf->sf_status = SS_FAILED;
        }
        sf->sf_run_time += cfs_duration_sec(cfs_time_current() -
                                scrub->os_time_last_checkpoint + HALF_SEC);
        result = osd_scrub_file_store(scrub);
        if (result < 0)
                CERROR("Fail to osd_scrub_post: rc = %d\n", result);
        cfs_up_write(&scrub->os_rwsem);

        EXIT;
}

#define SCRUB_NEXT_BREAK        1
#define SCRUB_NEXT_CONTINUE     2

static int osd_scrub_next(struct osd_thread_info *info, struct osd_device *dev,
                          struct osd_scrub *scrub, struct super_block *sb,
                          ldiskfs_group_t bg, struct buffer_head *bitmap,
                          __u32 gbase, __u32 *offset)
{
        struct osd_idmap_cache *oic   = &scrub->os_oic;
        struct inode           *inode;
        int                     rc    = 0;

        *offset = ldiskfs_find_next_bit((unsigned long *)bitmap->b_data,
                                        LDISKFS_INODES_PER_GROUP(sb), *offset);
        if (*offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                brelse(bitmap);
                scrub->os_pos_current = 1 + (bg + 1) *
                                        LDISKFS_INODES_PER_GROUP(sb);
                return SCRUB_NEXT_BREAK;
        }

        scrub->os_pos_current = gbase + *offset;
        osd_id_gen(&oic->oic_lid, scrub->os_pos_current, OSD_OII_NOGEN);
        inode = osd_iget(info, dev, &oic->oic_lid, &oic->oic_fid,
                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
        if (IS_ERR(inode)) {
                rc = PTR_ERR(inode);
                /* The inode may be removed after bitmap searching, or the
                 * file is new created without inode initialized yet. */
                if (rc == -ENOENT || rc == -ESTALE) {
                        scrub->os_pos_current = gbase + ++*offset;
                        return SCRUB_NEXT_CONTINUE;
                }

                CERROR("Fail to read inode: group = %u, ino# = %u, rc = %d\n",
                       bg, scrub->os_pos_current, rc);
        } else {
                iput(inode);
        }
        return rc;
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

                        if (!cfs_list_empty(list)) {
                                scrub->os_in_prior = 1;
                                rc = 0;
                                goto check_update;
                        }

                        if (!scrub->os_full_speed && noslot != 0)
                                goto wait;

                        rc = osd_scrub_next(info, dev, scrub, sb, bg,
                                            bitmap, gbase, &offset);
                        if (rc == SCRUB_NEXT_BREAK)
                                break;
                        else if (rc == SCRUB_NEXT_CONTINUE)
                                continue;

check_update:
                        rc = osd_scrub_check_update(&env, dev, rc);
                        if (rc != 0) {
                                brelse(bitmap);
                                GOTO(post, rc);
                        }

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
                struct osd_inconsistent_item *oii;

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
        struct ldiskfs_super_block *es     = LDISKFS_SB(osd_sb(dev))->s_es;
        struct inode               *inode;
        int                         dirty  = 0;
        int                         rc     = 0;
        ENTRY;

        cfs_waitq_init(&scrub->os_thread.t_ctl_waitq);
        cfs_init_rwsem(&scrub->os_rwsem);
        cfs_spin_lock_init(&scrub->os_lock);
        CFS_INIT_LIST_HEAD(&scrub->os_inconsistent_items);
        if (get_mount_flags(dev->od_mount->lmi_sb) & LMD_FLG_NOAUTO_SCRUB)
                scrub->os_noauto_scrub = 1;

        inode = osd_local_find(env, dev, osd_scrub_name, NULL);
        if (IS_ERR(inode))
                RETURN(PTR_ERR(inode));

        scrub->os_inode = inode;
        rc = osd_scrub_file_load(scrub);
        if (rc == -ENOENT) {
                osd_scrub_file_init(scrub, es->s_uuid);
                dirty = 1;
        } else if (rc != 0) {
                RETURN(rc);
        } else {
                if (memcmp(sf->sf_uuid, es->s_uuid, 16) != 0) {
                        osd_scrub_file_reset(scrub, es->s_uuid, SF_RESTORED);
                        dirty = 1;
                } else if (sf->sf_status == SS_SCANNING) {
                        sf->sf_status = SS_CRASHED;
                        dirty = 1;
                }
        }

        if (dirty != 0)
                rc = osd_scrub_file_store(scrub);

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

static struct dt_it *osd_otable_it_init(const struct lu_env *env,
                                       struct dt_object *dt, __u32 attr,
                                       struct lustre_capa *capa)
{
        enum dt_otable_it_flags flags = attr >> DT_OTABLE_IT_FLAGS_SHIFT;
        enum dt_otable_it_valid valid = attr & ~DT_OTABLE_IT_FLAGS_MASK;
        struct osd_device      *dev   = osd_dev(dt->do_lu.lo_dev);
        struct osd_scrub       *scrub = &dev->od_scrub;
        struct osd_otable_it   *it;
        __u32                   start = 0;
        int                     rc;
        ENTRY;

        cfs_mutex_lock(&dev->od_mutex);
        if (dev->od_otable_it != NULL)
                GOTO(out, it = ERR_PTR(-EALREADY));

        OBD_ALLOC_PTR(it);
        if (it == NULL)
                GOTO(out, it = ERR_PTR(-ENOMEM));

        dev->od_otable_it = it;
        it->ooi_dev = dev;
        it->ooi_cache.ooc_consumer_idx = -1;
        if (flags & DOIF_OUTUSED)
                it->ooi_used_outside = 1;

        if (flags & DOIF_RESET)
                start |= SS_RESET;

        if (valid & DOIV_ERROR_HANDLE) {
                if (flags & DOIF_FAILOUT)
                        start |= SS_SET_FAILOUT;
                else
                        start |= SS_CLEAR_FAILOUT;
        }

        rc = do_osd_scrub_start(dev, start);
        if (rc == -EALREADY) {
                it->ooi_cache.ooc_pos_preload = scrub->os_pos_current - 1;
        } else if (rc < 0) {
                dev->od_otable_it = NULL;
                OBD_FREE_PTR(it);
                GOTO(out, it = ERR_PTR(-EALREADY));
        } else {
                it->ooi_cache.ooc_pos_preload = scrub->os_pos_current;
        }

        GOTO(out, it);

out:
        cfs_mutex_unlock(&dev->od_mutex);
        return (struct dt_it *)it;
}

static void osd_otable_it_fini(const struct lu_env *env, struct dt_it *di)
{
        struct osd_otable_it *it  = (struct osd_otable_it *)di;
        struct osd_device    *dev = it->ooi_dev;

        /* od_mtext: prevent curcurrent start/stop */
        cfs_mutex_lock(&dev->od_mutex);
        do_osd_scrub_stop(&dev->od_scrub);
        LASSERT(dev->od_otable_it == it);

        dev->od_otable_it = NULL;
        cfs_mutex_unlock(&dev->od_mutex);
        OBD_FREE_PTR(it);
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
        struct osd_otable_it    *it  = (struct osd_otable_it *)di;
        struct osd_otable_cache *ooc = &it->ooi_cache;
        const char              *str = (const char *)key;
        __u32                    ino;
        ENTRY;

        /* Forbid to set iteration position after iteration started. */
        if (it->ooi_user_ready)
                RETURN(-EPERM);

        if (str[0] != '\0')
                RETURN(-EINVAL);

        if (sscanf(str, "%u", &ino) <= 0)
                RETURN(-EINVAL);

        /* Skip the one that has been processed last time. */
        if (ooc->ooc_pos_preload > ++ino)
                ooc->ooc_pos_preload = ino;

        RETURN(0);
}

static int osd_scrub_preload(const struct lu_env *env,
                             struct osd_otable_it *it)
{
        struct osd_otable_cache *ooc    = &it->ooi_cache;
        struct osd_device       *dev    = it->ooi_dev;
        struct osd_scrub        *scrub  = &dev->od_scrub;
        struct ptlrpc_thread    *thread = &scrub->os_thread;
        struct super_block      *sb     = osd_sb(dev);
        struct osd_thread_info  *info   = osd_oti_get(env);
        struct buffer_head      *bitmap = NULL;
        __u32                    max;
        int                      rc;
        ENTRY;

        max = le32_to_cpu(LDISKFS_SB(sb)->s_es->s_inodes_count);
        while (ooc->ooc_pos_preload <= max &&
               ooc->ooc_cached_items < OSD_OTABLE_IT_CACHE_SIZE &&
               (!thread_is_running(thread) ||
                ooc->ooc_pos_preload < scrub->os_pos_current)) {
                struct osd_idmap_cache *oic;
                struct inode *inode;
                ldiskfs_group_t bg = (ooc->ooc_pos_preload - 1) /
                                     LDISKFS_INODES_PER_GROUP(sb);
                __u32 offset = (ooc->ooc_pos_preload - 1) %
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
                        ooc->ooc_pos_preload < scrub->os_pos_current)) {
                        offset = ldiskfs_find_next_bit((unsigned long *)
                                                bitmap->b_data,
                                                LDISKFS_INODES_PER_GROUP(sb),
                                                offset);
                        if (offset >= LDISKFS_INODES_PER_GROUP(sb)) {
                                brelse(bitmap);
                                ooc->ooc_pos_preload = 1 + (bg + 1) *
                                                LDISKFS_INODES_PER_GROUP(sb);
                                break;
                        }

                        oic = &ooc->ooc_cache[ooc->ooc_producer_idx];
                        ooc->ooc_pos_preload = gbase + offset;
                        osd_id_gen(&oic->oic_lid, ooc->ooc_pos_preload,
                                   OSD_OII_NOGEN);
                        inode = osd_iget(info, dev, &oic->oic_lid,&oic->oic_fid,
                                         OSD_IF_GEN_LID | OSD_IF_RET_FID);
                        if (IS_ERR(inode)) {
                                rc = PTR_ERR(inode);
                                /* The inode may be removed after bitmap
                                 * searching, or the file is new created
                                 * without inode initialized yet. */
                                if (rc == -ENOENT || rc == -ESTALE) {
                                        ooc->ooc_pos_preload = gbase + ++offset;
                                        continue;
                                }

                                CERROR("Fail to read inode: group = %u, "
                                       "ino# = %u, rc = %d, urgent mode.\n",
                                       bg, ooc->ooc_pos_preload, rc);
                                brelse(bitmap);
                                /* If up layer LFSCK ignore the failure,
                                 * we can skip the inode next time. */
                                ooc->ooc_pos_preload = gbase + offset + 1;
                                RETURN(rc);
                        }

                        iput(inode);
                        ooc->ooc_pos_preload = gbase + ++offset;
                        ooc->ooc_cached_items++;
                        ooc->ooc_producer_idx = (ooc->ooc_producer_idx + 1) &
                                                ~OSD_OTABLE_IT_CACHE_MASK;
                }
        }

        if (ooc->ooc_pos_preload > max) {
                CDEBUG(D_SCRUB, "OSD pre-loaded all: pid = %d\n",
                       cfs_curproc_pid());

                it->ooi_all_cached = 1;
        } else {
                brelse(bitmap);
        }

        RETURN(ooc->ooc_cached_items);
}

static int osd_otable_it_next(const struct lu_env *env, struct dt_it *di)
{
        struct osd_otable_it    *it     = (struct osd_otable_it *)di;
        struct osd_device       *dev    = it->ooi_dev;
        struct osd_scrub        *scrub  = &dev->od_scrub;
        struct osd_otable_cache *ooc    = &it->ooi_cache;
        struct ptlrpc_thread    *thread = &scrub->os_thread;
        struct l_wait_info       lwi    = { 0 };
        int                      rc;
        ENTRY;

        LASSERT(it->ooi_user_ready);

again:
        if ((!thread_is_running(thread) && !it->ooi_used_outside) ||
            it->ooi_all_cached)
                RETURN(+1);

        if (ooc->ooc_cached_items > 0) {
                ooc->ooc_cached_items--;
                ooc->ooc_consumer_idx = (ooc->ooc_consumer_idx + 1) &
                                        ~OSD_OTABLE_IT_CACHE_MASK;
                if (scrub->os_waiting) {
                        scrub->os_waiting = 0;
                        cfs_waitq_broadcast(&thread->t_ctl_waitq);
                }
                RETURN(0);
        }

        it->ooi_waiting = 1;
        l_wait_event(thread->t_ctl_waitq,
                     ooc->ooc_pos_preload < scrub->os_pos_current ||
                     !thread_is_running(thread),
                     &lwi);
        it->ooi_waiting = 0;

        if (!thread_is_running(thread) && !it->ooi_used_outside)
                RETURN(+1);

        rc = osd_scrub_preload(env, it);
        if (rc >= 0)
                goto again;

        RETURN(rc);
}

static struct dt_key *osd_otable_it_key(const struct lu_env *env,
                                        const struct dt_it *di)
{
        struct osd_otable_it    *it  = (struct osd_otable_it *)di;
        struct osd_otable_cache *ooc = &it->ooi_cache;

        sprintf(it->ooi_key, "%u",
                ooc->ooc_cache[ooc->ooc_consumer_idx].oic_lid.oii_ino);
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
        struct osd_otable_it    *it  = (struct osd_otable_it *)di;
        struct osd_otable_cache *ooc = &it->ooi_cache;

        *(struct lu_fid *)rec = ooc->ooc_cache[ooc->ooc_consumer_idx].oic_fid;
        return 0;
}

static int osd_otable_it_load(const struct lu_env *env,
                              const struct dt_it *di, __u64 hash)
{
        struct osd_otable_it    *it    = (struct osd_otable_it *)di;
        struct osd_device       *dev   = it->ooi_dev;
        struct osd_otable_cache *ooc   = &it->ooi_cache;
        struct osd_scrub        *scrub = &dev->od_scrub;

        if (it->ooi_user_ready)
                return 0;

        if (ooc->ooc_pos_preload < LDISKFS_FIRST_INO(osd_sb(dev)))
                ooc->ooc_pos_preload = LDISKFS_FIRST_INO(osd_sb(dev));
        it->ooi_user_ready = 1;
        if (!scrub->os_full_speed)
                cfs_waitq_broadcast(&scrub->os_thread.t_ctl_waitq);

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

int osd_oii_insert(struct osd_device *dev, struct lu_object *obj,
                   struct osd_idmap_cache *oic)
{
        struct osd_scrub             *scrub  = &dev->od_scrub;
        struct ptlrpc_thread         *thread = &scrub->os_thread;
        struct scrub_file            *sf     = &scrub->os_file;
        struct osd_inconsistent_item *oii;
        int                           wakeup = 0;
        ENTRY;

        if (!thread_is_running(thread) || sf->sf_status != SS_SCANNING)
                RETURN(-EAGAIN);

        OBD_ALLOC_PTR(oii);
        if (unlikely(oii == NULL))
                RETURN(-ENOMEM);

        CFS_INIT_LIST_HEAD(&oii->oii_list);
        oii->oii_cache = *oic;
        oii->oii_obj = obj;

        cfs_spin_lock(&scrub->os_lock);
        if (unlikely(!thread_is_running(thread) ||
                     sf->sf_status != SS_SCANNING)) {
                cfs_spin_unlock(&scrub->os_lock);
                OBD_FREE_PTR(oii);
                RETURN(-EAGAIN);
        }

        if (cfs_list_empty(&scrub->os_inconsistent_items))
                wakeup = 1;
        lu_object_get(oii->oii_obj);
        cfs_list_add_tail(&oii->oii_list, &scrub->os_inconsistent_items);
        cfs_spin_unlock(&scrub->os_lock);

        if (wakeup != 0)
                cfs_waitq_broadcast(&thread->t_ctl_waitq);

        RETURN(0);
}
