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
 * lustre/include/lustre_scrub_disk.h
 *
 * Lustre scrub on-disk structures.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef _LUSTRE_SCRUB_DISK_H
# define _LUSTRE_SCRUB_DISK_H

/* To unify the interfaces and hide backend filesystem detail, the scrub
 * bookmark will represented as printable string. */
#define SCRUB_BOOKMARK_MAXLEN           64

/* Please update scrub_status_names[] when change the enum. */
enum scrub_status {
        /* The scrub file is new created, for new MDT, upgrading from old disk,
         * or re-creating the scrub file manually. */
        SS_INIT                 = 0,

        /* The scrub is scanning related system components. */
        SS_SCANNING             = 1,

        /* The scrub scanned related system components successfully. */
        SS_COMPLETED            = 2,

        /* The scrub failed to scan related system components. */
        SS_FAILED               = 3,

        /* The scrub is stopped manually, the system may be inconsistent. */
        SS_PAUSED               = 4,

        /* The scrub crashed during the scanning, should be restarted. */
        SS_CRASHED              = 5,
};

/* Please update scrub_unit_flags_names[] when change the enum. */
enum scrub_unit_flags {
        /* Whether the scrub_unit is under dryrun mode or not. */
        SUF_DRYRUN              = 1 << 0,

        /* The Lustre component corresponding to the scrubber is inconsistent,
         * Should be fixed ASAP. For example, OI files should be rebuild affer
         * MDT file-level backup/restore by OI scrub. */
        SUF_INCONSISTENT        = 1 << 1,
};

struct scrub_statistics {
        /* The position for the last scrub checkpoint. */
        __u8    ss_pos_last_checkpoint[SCRUB_BOOKMARK_MAXLEN];

        /* The position for the first should be updated object. */
        __u8    ss_pos_first_inconsistent[SCRUB_BOOKMARK_MAXLEN];

        /* How many objects have been checked. */
        __u64   ss_items_checked;

        /* How many objects have been updated under repair mode
         * or should be updated under dryrun mode. */
        __u64   ss_items_updated;

        /* How many objects failed to be processed. */
        __u64   ss_items_failed;

        /* How many prior objects have been updated during scanning. */
        __u64   ss_items_updated_prior;

        /* How long the scrub has run. */
        __u32   ss_time;

        /* For 64-bits aligned. */
        __u32   ss_padding;
};

/* Scrub data unit for tracing the scrub processing. */
struct scrub_unit {
        /* The scrub type, see 'enum scrub_type'. */
        __u16   su_type;

        /* Special flags for the scrub, depends on scrub type. */
        __u16   su_flags;

        /* How many completed scrub ran on the device. */
        __u32   su_success_count;

        /* The time for the last scrub completed. */
        __u64   su_time_last_complete;

        /* The time for the latest scrub ran. */
        __u64   su_time_latest_start;

        /* The time for the last scrub checkpoint. */
        __u64   su_time_last_checkpoint;

        /* The position for the latest scrub started from. */
        __u8    su_pos_latest_start[SCRUB_BOOKMARK_MAXLEN];

        /* The scrub statistics for repair mode. */
        struct scrub_statistics su_repair;

        /* The scrub statistics for dryrun mode. */
        struct scrub_statistics su_dryrun;
};

/* Descript scrub_unit in the scrub_head. */
struct scrub_unit_desc {
        /* The scrub type, see 'enum scrub_type'. */
        __u16   sud_type;

        /* The scrub status, see 'enum scrub_status'. */
        __u8    sud_status;

        /* General flags for the scrub, see 'enum scrub_unit_flags'. */
        __u8    sud_flags;

        /* Offset of the scrub_unit in the scrub file,
         * depends on scrub type. */
        __u32   sud_offset;
};

/* General head for scrub tracing file, for LFSCK in both MDD and OSD. */
struct scrub_head {
        /* The scrub version. */
        __u16                   sh_version;

        /* How many scrub_unit in the scrub file. */
        __u16                   sh_units_count;

        /* Param: switches for the scrub, see 'enum scrub_param_flags'. */
        __u16                   sh_param_flags;

        /* Param: object iteration method, see 'enum scrub_method'. */
        __u16                   sh_param_method;

        /* Param: how many items can be scanned at most per second. */
        __u32                   sh_param_speed_limit;

        /* For 64-bits aligned. */
        __u32                   sh_padding;

#define SCRUB_DESC_COUNT        16
        /* The descriptors for all the scrub_unit in the scrub file.
         * Since 'scrub_type' is 16 bits (__u16), the descriptors array
         * size is 16 also, and use 'scrub_type_shift' as index. */
        struct scrub_unit_desc  sh_desc[SCRUB_DESC_COUNT];
};

#endif /* _LUSTRE_SCRUB_DISK_H */
