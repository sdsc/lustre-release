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
 * lustre/include/lustre/lustre_scrub_user.h
 *
 * Lustre scrub userspace interfaces.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef _LUSTRE_SCRUB_USER_H
# define _LUSTRE_SCRUB_USER_H

#define SCRUB_VERSION_V1                10

/* Flags used as scrub parameters. Please update scrub_param_flags_names[]
 * when change the enum. */
enum scrub_param_flags {
        /* Reset scrub iterator position to the device beginning. */
        SPF_RESET       = 1 << 0,

        /* Exit when fail. */
        SPF_FAILOUT     = 1 << 1,

        /* Dryrun mode, only check without modification */
        SPF_DRYRUN      = 1 << 2,
};

/* Please update scrub_method_names[] when change the enum. */
enum scrub_method {
        /* Object table based iteration, depends on backend filesystem.
         * For ldiskfs, it is inode table based iteration. */
        SM_OTABLE       = 1,

        /* Namespace based scanning. NOT support yet. */
        SM_NAMESPACE    = 2,
};

enum scrub_type {
        /* For OI files rebuilding. */
        ST_OI_SCRUB     = 1 << 0,

        /* For MDT-OST consistency verification. */
        ST_LAYOUT_SCRUB = 1 << 1,

        /* For MDT-MDT consistency verification. */
        ST_DNE_SCRUB    = 1 << 2,
};

#define SCRUB_TYPES_ALL         ((__u16)(~0))
#define SCRUB_TYPES_DEF         ((__u16)0)

/**
 * The scrub can be triggered through three ways:
 *
 * 1) When MDT mounts up, it will detect whether needs to run scrub or not.
 *    For example: rebuilding OI files when MDT is restored from file-level
 *    backup, resuming failed scrub from the lastest checkpoint, and so on.
 *
 * 2) When some inconsistent idmap is found during OI lookup, it need to
 *    trigger OI scrub to rebuild corrupt OI files.
 *
 * If specify "noauto_scrub" when MDT mounts up, then above two modes are
 * disabled.
 *
 * 3) Trigger scrub by user command, the mount options "noauto_scrub" is
 *    ignored under such mode.
 */
enum scrub_sponsor {
        /* The scrub is triggered automatically when mount. */
        SCRUB_TRIGGERED_BY_MOUNT        = 1,

        /* The scrub is triggered by RPC after mount up. */
        SCRUB_TRIGGERED_BY_RPC          = 2,

        /* The scrub is triggered by user command.*/
        SCRUB_TRIGGERED_BY_COMMAND      = 3,
};

#define SP_CHECKPOING_INTERVAL_MAX      60
#define SP_CHECKPOING_INTERVAL_DEF      5
#define SP_SPEED_NO_LIMIT               0
#define SP_SPEED_LIMIT_DEF              SP_SPEED_NO_LIMIT

enum scrub_start_valid {
        SSV_SPEED_LIMIT         = 1 << 0,
        SSV_CHECKPOING_INTERVAL = 1 << 1,
        SSV_METHOD              = 1 << 2,
        SSV_ERROR_HANDLE        = 1 << 3,
        SSV_DRYRUN              = 1 << 4,
};

/* Arguments from/to user space for starting scrub. */
struct scrub_start {
        /* Input: which arguments are valid, see 'enum scrub_start_valid'. */
        __u32   ss_valid;

        /* Input: user space tools version.
         * Output: kernel space scrub version. */
        __u32   ss_version;

        /* Input & output: which scrubbers to be or have been started. */
        __u16   ss_active;

        /* Input & output: flags for the scrub, see 'enum scrub_param_flags'. */
        __u16   ss_flags;

        /* Input & output: object iteration method, see 'enum scrub_method'. */
        __u16   ss_method;

        /* Input & output: interval time (seconds) between two checkpoints. */
        __u16   ss_checkpoint_interval;

        /* Input & output: how many items can be scanned at most per second. */
        __u32   ss_speed_limit;

        /* Who triggers the scrub, see 'enum scrub_sponsor'. */
        __u16   ss_sponsor;

        /* For 64-bits aligned. */
        __u16   ss_padding;
};

#endif /* _LUSTRE_SCRUB_USER_H */
