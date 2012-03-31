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
 * lustre/include/lustre_scrub.h
 *
 * Common functions for LFSCK components.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef _LUSTRE_SCRUB_H
# define _LUSTRE_SCRUB_H

#include <lu_object.h>
#include <dt_object.h>
#include <lustre_scrub_disk.h>
#include <lustre/lustre_scrub_user.h>

/* Lock order: seh_sem -> seh_rwsem -> seh_lock. */
struct scrub_exec_head {
        /* List for scrub_exec_unit. */
        cfs_list_t              seh_units;

        /* For non-blocked scrub ops. */
        cfs_spinlock_t          seh_lock;

        /* For units register/degister, and I/O. */
        cfs_rw_semaphore_t      seh_rwsem;

        /* For scrub start/stop. */
        cfs_semaphore_t         seh_sem;

        /* scrub_head in memory. */
        struct scrub_head       seh_head;

        /* Buffer for scrub_head load/store. */
        struct scrub_head       seh_head_buf;

        /* Buffer for scrub_unit load/store. */
        struct scrub_unit       seh_unit_buf;

        /* Object for the scrub file. */
        struct dt_object       *seh_obj;

        /* How many objects have been scanned since last checkpoint.
         * Mainly for controlling speed. */
        __u64                   seh_new_scanned;

        /* The time for last checkpoint, jiffies */
        cfs_time_t              seh_time_last_checkpoint;

        /* The time for next checkpoint, jiffies */
        cfs_time_t              seh_time_next_checkpoint;

        /* Current checkpoint bookmark. */
        __u8                    seh_pos_checkpoint[SCRUB_BOOKMARK_MAXLEN];

        /* For scrub dump. */
        __u8                    seh_pos_dump[SCRUB_BOOKMARK_MAXLEN];

        /* Schedule for every N objects. */
        __u32                   seh_sleep_rate;

        /* Sleep N jiffies for each schedule. */
        __u32                   seh_sleep_jif;

        /* Private data for this head. */
        void                   *seh_private;

        unsigned int            seh_dirty:1; /* The scrub head needs to be
                                              * stored back to disk. */

        /* Which unit(s) is/are in active. */
        __u16                   seh_active;
};

struct scrub_exec_unit;

typedef int (*scrub_prep_policy)(const struct lu_env *,
                                 struct scrub_exec_head *,
                                 struct scrub_exec_unit *);
typedef int (*scrub_exec_policy)(const struct lu_env *,
                                 struct scrub_exec_head *,
                                 struct scrub_exec_unit *, int);
typedef void (*scrub_post_policy)(const struct lu_env *,
                                  struct scrub_exec_head *,
                                  struct scrub_exec_unit *, int);

struct scrub_exec_unit {
        /* Link into scrub_exec_head::seh_units. */
        cfs_list_t              seu_list;

        /* scrub_unit for this exec body. */
        struct scrub_unit       seu_unit;

        /* Prepare before scrub start. */
        scrub_prep_policy       seu_prep_policy;

        /* Scrub main function. */
        scrub_exec_policy       seu_exec_policy;

        /* Cleanup before scrub stop. */
        scrub_post_policy       seu_post_policy;

        /* The index of the unit in the scrub_head description table. */
        int                     seu_idx;

        /* How many objects have been checked since last checkpoint.
         * Mainly for statistics. */
        __u32                   seu_new_checked;

        /* Private data for this unit. */
        void                   *seu_private;

        unsigned int            seu_dirty:1; /* The scrub unit needs to be
                                              * stored back to disk. */
};

struct scrub_unit_desc *scrub_desc_find(struct scrub_head *head,
                                        __u16 type, int *idx);
void scrub_set_speed(struct scrub_exec_head *seh);
void scrub_check_speed(struct scrub_exec_head *seh,
                       struct ptlrpc_thread *thread);

struct scrub_exec_unit *scrub_unit_new(scrub_prep_policy prep,
                                       scrub_exec_policy exec,
                                       scrub_post_policy post);
void scrub_unit_free(struct scrub_exec_unit *seu);
void scrub_unit_init(struct scrub_exec_unit *seu, __u16 type);
void scrub_unit_reset(struct scrub_exec_unit *seu);
int scrub_unit_load(const struct lu_env *env, struct scrub_exec_head *seh,
                    __u32 pos, struct scrub_exec_unit *seu);
int scrub_unit_store(const struct lu_env *env, struct scrub_exec_head *seh,
                     __u32 pos, struct scrub_exec_unit *seu);
int scrub_unit_register(struct scrub_exec_head *seh,
                        struct scrub_exec_unit *seu,
                        __u8 status, __u8 flags);
void scrub_unit_degister(struct scrub_exec_head *seh,
                         struct scrub_exec_unit *seu);
int scrub_unit_dump(struct scrub_exec_head *seh, char *buf, int len,
                    __u16 type, const char *flags[]);

struct scrub_exec_head *scrub_head_new(struct dt_object *obj);
void scrub_head_free(const struct lu_env *env, struct scrub_exec_head *seh);
void scrub_head_init(struct scrub_exec_head *seh);
int scrub_head_load(const struct lu_env *env, struct scrub_exec_head *seh);
int scrub_head_store(const struct lu_env *env, struct scrub_exec_head *seh);
void scrub_head_reset(const struct lu_env *env, struct scrub_exec_head *seh);

int scrub_prep(const struct lu_env *env, struct scrub_exec_head *seh);
int scrub_exec(const struct lu_env *env, struct scrub_exec_head *seh, int rc);
void scrub_post(const struct lu_env *env, struct scrub_exec_head *seh, int rc);
int scrub_checkpoint(const struct lu_env *env, struct scrub_exec_head *seh);

static inline int scrub_key_is_empty(__u8 *key)
{
        return key[0] == '\0';
}

static inline void scrub_key_set_empty(__u8 *key)
{
        key[0] = '\0';
}

#endif /* _LUSTRE_SCRUB_H */
