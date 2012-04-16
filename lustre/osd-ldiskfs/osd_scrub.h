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
        /* The device is restored from file-level backup, the OI files
         * are invalid, and should be rebuild ASAP. */
        SF_RESTORED     = 1 << 0,

        /* OI scrub is triggered by RPC automatically. */
        SF_BYRPC        = 1 << 1,
};

enum scrub_param {
        /* Exit when fail. */
        SP_FAILOUT      = 1 << 0,
};

struct scrub_file {
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

#endif /* _OSD_SCRUB_H */
