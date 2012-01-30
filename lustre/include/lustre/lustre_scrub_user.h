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
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * lustre/include/lustre/lustre_scrub_user.h
 *
 * OI Scrub is used to rebuild Object Index file when restore MDT from
 * file-level backup. And also can be part of consistency routine check.
 *
 * Lustre public user-space interface definitions for scrub.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef _LUSTRE_SCRUB_USER_H
# define _LUSTRE_SCRUB_USER_H

#define SCRUB_MAGIC             0xCB102011
#define SCRUB_VERSION_V1        10

/**
 * Different backend filesystem maybe use different local identifier for object.
 * The user space tools do not know which type identifier will be used. So here,
 * it reserves 128 bits for that. */
union scrub_key {
        __u64   sk_u64[2];
        __u32   sk_u32[4];
        __u8    sk_u8[16];
};

enum scrub_method {
        /* Inode table based iteration. */
        SM_ITABLE       = 1,

        /* Namespace based scanning. NOT support yet. */
        SM_NAMESPACE    = 2,
};

enum scrub_param_flags {
        /* Exit when fail. */
        SPF_FAILOUT     = 1 << 0,

        /* Dry run without modification. */
        SPF_DRYRUN      = 1 << 1,

        /* Rebuild the OI file. */
        SPF_OI_REBUILD  = 1 << 2,

        /* Return igif fid for 1.8 inode, not support yet. */
        SPF_IGIF        = 1 << 3,
};

#define SPM_CHECKPOING_INTERVAL 60
#define SPD_CHECKPOING_INTERVAL 5
#define SPD_SPEED_LIMIT         0
#define SPD_PIPELINE_WINDOW     100000

struct scrub_param {
        /* Object iteration method. */
        __u16   sp_method;

        /* The interval time between two checkpoints, the unit is sec. */
        __u16   sp_checkpoint_interval;

        /* How many items can be scanned at most per second. */
        __u32   sp_speed_limit;

        /* For pipeline iterators, how many items the low layer iterator can
         * be ahead of the up layer iterator. */
        __u32   sp_pipeline_window;

        /* Scrub flags. */
        __u32   sp_flags;
};

enum scrub_param_valid {
        SSP_METHOD              = 1 << 0,
        SSP_CHECKPOING_INTERVAL = 1 << 1,
        SSP_SPEED_LIMIT         = 1 << 2,
        SSP_ITERATOR_WINDOW     = 1 << 3,

        SSP_ERROR_HANDLE        = 1 << 4,
        SSP_DRYRUN              = 1 << 5,
        SSP_RESET               = 1 << 6,

        SSP_OI_REBUILD          = 1 << 7,
        SSP_IGIF                = 1 << 8,
};

struct start_scrub_param {
        /* Input & output parameters. */
        struct scrub_param      ssp_param;

        /* Input: user space tools version.
         * Output: kernel space scrub version. */
        __u16                   ssp_version;

        /* Output: keysize userd by the scrub. */
        __u16                   ssp_keysize;

        /* Input: which parameters are specified. */
        __u32                   ssp_valid;

        /* Output: the position for the scrub started from. */
        union scrub_key         ssp_start_point;
};

enum scrub_status {
        /* The scrub file is new created, for new MDT, upgrading from old disk,
         * or re-creating the scrub file manually. */
        SS_INIT                 = 0,

        /* The MDT is restored from file-level backup, some components,
         * like the OI file(s) should be rebuild. */
        SS_RESTORED             = 1,

        /* The scrub is rebuilding related system components. */
        SS_REBUILDING           = 2,

        /* The scrub rebuild related system components successfully. */
        SS_COMPLETED            = 3,

        /* The scrub failed to rbuild related system components. */
        SS_FAILED               = 4,

        /* The scrub is stopped manually, the system maybe inconsistent. */
        SS_PAUSED               = 5,

        /* The scrub is scanning the system under dryrun mode. */
        SS_DRURUN_SACNNING      = 6,

        /* The scrub finished to scan the system under dryrun mode. */
        SS_DRURUN_SACNNED       = 7,
};

struct scrub_info_normal {
        /* The time for the last scrub checkpoint. */
        __u64           sin_time_last_checkpoint;

        /* The position for the last scrub checkpoint. */
        union scrub_key sin_position_last_checkpoint;

        /* How many objects have been scanned:
         * updated + failed to be processed + no need to be updated (skipped).*/
        __u64           sin_items_scanned;

        /* How many objects have been updated. */
        __u64           sin_items_updated;

        /* How many objects failed to be processed. */
        __u64           sin_items_failed;

        /* How long the OI Scrub has run. */
        __u32           sin_time;

        /* The average speed since last checkpoint. Total average speed can be
         * calculated by sin_items_scanned / sin_time. */
        __u32           sin_speed;
};

struct scrub_info_dryrun {
        /* The time for the last scrub checkpoint. */
        __u64           sid_time_last_checkpoint;

        /* The position for the last scrub checkpoint. */
        union scrub_key sid_position_last_checkpoint;

        /* The position for the first to be updated object. */
        union scrub_key sid_position_first_unmatched;

        /* How many objects have been scanned:
         * unmatched + prior updated + failed to be processed +
         * no need to be updated (skipped). */
        __u64           sid_items_scanned;

        /* How many objects to be updated. */
        __u64           sid_items_unmatched;

        /* How many objects failed to be processed. */
        __u64           sid_items_failed;

        /* How many prior objects have been updated during dryrun. */
        __u64           sid_items_updated_prior;

        /* How long the OI Scrub has dryrun. */
        __u32           sid_time;

        /* The average speed since last checkpoint. Total average speed can be
         * calculated by sid_items_scanned / sid_time. */
        __u32           sid_speed;
};

struct scrub_header {
        /* Scrub magic. */
        __u32                           sh_magic;

        /* Scrub version. */
        __u16                           sh_version;

        /* Keysize userd by the scrub. */
        __u16                           sh_keysize;

        /* How many completed scrub ran on the device. */
        __u32                           sh_success_count;

        /* The scrub status. */
        __u32                           sh_current_status;

        /* The time for the last scrub completed. */
        __u64                           sh_time_last_complete;

        /* The time for the last scrub started from the beginning of the
         * device. */
        __u64                           sh_time_initial_start;

        /* The time for the latest scrub ran. */
        __u64                           sh_time_latest_start;

        /* The position for the latest scrub started from. */
        union scrub_key                 sh_position_latest_start;

        /* Scrub parameters. */
        struct scrub_param              sh_param;

        /* Scrub information for normal mode. */
        struct scrub_info_normal        sh_normal;

        /* Scrub information for dryrun mode. */
        struct scrub_info_dryrun        sh_dryrun;
};

struct scrub_show {
        /* Scrub header. */
        struct scrub_header     ss_header;

        /* The position for current scrub processing. */
        union scrub_key         ss_current_position;
};

#endif /* _LUSTRE_SCRUB_USER_H */
