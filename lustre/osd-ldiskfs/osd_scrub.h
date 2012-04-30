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
 * lustre/osd-ldiskfs/osd_scrub.h
 *
 * Shared definitions and declarations for OI scrub.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef _OSD_SCRUB_H
# define _OSD_SCRUB_H

#define SCRUB_VERSION_V1                10
#define SCRUB_CHECKPOING_INTERVAL       60

enum scrub_status {
        /* The scrub file is new created, for new MDT, upgrading from old disk,
         * or re-creating the scrub file manually. */
        SS_INIT         = 0,

        /* The scrub is checking/repairing the OI files. */
        SS_SCANNING     = 1,

        /* The scrub checked/repaired the OI files successfully. */
        SS_COMPLETED    = 2,

        /* The scrub failed to check/repair the OI files. */
        SS_FAILED       = 3,

        /* The scrub is stopped manually, the OI files may be inconsistent. */
        SS_PAUSED       = 4,

        /* The scrub crashed during the scanning, should be restarted. */
        SS_CRASHED      = 5,
};

enum scrub_flags {
        /* OI files have never been checked, do not know whether valid. */
        SF_NONSCRUBBED  = 1 << 0,

        /* OI files are invalid, should be rebuild ASAP */
        SF_INCONSISTENT = 1 << 1,

        /* OI scrub is triggered automatically. */
        SF_AUTO         = 1 << 2,
};

enum scrub_param {
        /* Exit when fail. */
        SP_FAILOUT      = 1 << 0,
};

enum scrub_start {
        /* Set failout flag. */
        SS_SET_FAILOUT          = 1 << 0,

        /* Clear failout flag. */
        SS_CLEAR_FAILOUT        = 1 << 1,

        /* Reset scrub start position. */
        SS_RESET                = 1 << 2,

        /* Trigger scrub automatically. */
        SS_AUTO                 = 1 << 3,
};

struct scrub_file {
        /* 128-bit uuid for volume. */
        __u8    sf_uuid[16];

        /* The scrub version. */
        __u16   sf_version;

        /* See 'enum scrub_status'. */
        __u16   sf_status;

        /* See 'enum scrub_flags'. */
        __u16   sf_flags;

        /* See 'enum scrub_param'. */
        __u16   sf_param;

        /* The time for the last OI scrub completed. */
        __u64   sf_time_last_complete;

        /* The time for the latest OI scrub ran. */
        __u64   sf_time_latest_start;

        /* The time for the last OI scrub checkpoint. */
        __u64   sf_time_last_checkpoint;

        /* The position for the latest OI scrub started from. */
        __u64   sf_pos_latest_start;

        /* The position for the last OI scrub checkpoint. */
        __u64   sf_pos_last_checkpoint;

        /* The position for the first should be updated object. */
        __u64   sf_pos_first_inconsistent;

        /* How many objects have been checked. */
        __u64   sf_items_checked;

        /* How many objects have been updated. */
        __u64   sf_items_updated;

        /* How many objects failed to be processed. */
        __u64   sf_items_failed;

        /* How many prior objects have been updated during scanning. */
        __u64   sf_items_updated_prior;

        /* How many completed OI scrub ran on the device. */
        __u32   sf_success_count;

        /* How long the OI scrub has run. */
        __u32   sf_run_time;
};

#define SCRUB_WINDOW_SIZE       1024

struct osd_scrub {
        struct lvfs_run_ctxt    os_ctxt;
        struct ptlrpc_thread    os_thread;
        struct osd_idmap_cache  os_oic;
        cfs_list_t              os_inconsistent_items;

        /* write lock for scrub prep/update/post/checkpoint,
         * read lock for scrub dump. */
        cfs_rw_semaphore_t      os_rwsem;
        cfs_spinlock_t          os_lock;

        /* Scrub file in memory. */
        struct scrub_file       os_file;

        /* Buffer for scrub file load/store. */
        struct scrub_file       os_file_disk;

        /* Inode for the scrub file. */
        struct inode           *os_inode;

        /* The time for last checkpoint, jiffies */
        cfs_time_t              os_time_last_checkpoint;

        /* The time for next checkpoint, jiffies */
        cfs_time_t              os_time_next_checkpoint;

        /* How many objects have been checked since last checkpoint. */
        __u32                   os_new_checked;
        __u32                   os_pos_current;
        __u32                   os_start_flags;
        unsigned int            os_in_prior:1, /* process inconsistent item
                                                * found by RPC prior */
                                os_waiting:1, /* Waiting for scan window. */
                                os_full_speed:1, /* run w/o speed limit */
                                os_noauto_scrub:1; /* no auto trigger OI scrub*/
};

#endif /* _OSD_SCRUB_H */
