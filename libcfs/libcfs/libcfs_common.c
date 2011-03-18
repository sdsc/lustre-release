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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Author: liang@whamcloud.com
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LNET

#include <libcfs/libcfs.h>

/* destroy scalable lock, see libcfs_private.h for more detail */
void
cfs_sclock_free(cfs_scale_lock_t *sclock)
{
        LASSERT(sclock->sl_psize >= sizeof(cfs_private_lock_t));
        LASSERT(sclock->sl_locks != NULL);
        LASSERT(!sclock->sl_locked);

        cfs_percpu_free(sclock->sl_locks);
        LIBCFS_FREE_ALIGNED(sclock, sizeof(cfs_scale_lock_t));
}

CFS_EXPORT_SYMBOL(cfs_sclock_free);

/*
 * create sclock(scalable lock), see libcfs_private.h for more detail.
 *
 * sclock is designed for large-scale SMP system, so we need to
 * reduce cacheline conflict as possible as we can, that's the
 * reason we always allocate cacheline-aligned memory block.
 */
cfs_scale_lock_t *
__cfs_sclock_alloc(cfs_cpumap_t *cpumap, unsigned int psize)
{
        cfs_private_lock_t *plock;
        cfs_scale_lock_t   *sclock;
        int                 i;

        /* NB: cpumap can be NULL, sclock will be for HW CPUs on that case */
        LIBCFS_ALLOC_ALIGNED(sclock, sizeof(cfs_scale_lock_t));
        if (sclock == NULL)
                return NULL;

        cfs_rwlock_init(&sclock->sl_rwlock);
        sclock->sl_cpumap = cpumap;
        sclock->sl_psize  = offsetof(cfs_private_lock_t, pl_data[psize]);
        sclock->sl_locks  = __cfs_percpu_alloc(cpumap, sclock->sl_psize, 1);
        if (sclock->sl_locks == NULL) {
                LIBCFS_FREE_ALIGNED(sclock, sizeof(cfs_scale_lock_t));
                return NULL;
        }

        cfs_percpu_for_each(plock, i, sclock->sl_locks)
                cfs_spin_lock_init(&plock->pl_lock);

        for (i = 0; (1 << i) < cfs_percpu_count(sclock->sl_locks); i++) {}

        sclock->sl_bits = i;

        return sclock;
}

CFS_EXPORT_SYMBOL(__cfs_sclock_alloc);

/*
 * index != CFS_SCLOCK_EXCL
 *     hold private lock indexed by @index
 *
 * index == CFS_SCLOCK_EXCL
 *     exclusively lock @sclock and nobody can take private lock
 */
int
cfs_sclock_lock(cfs_scale_lock_t *sclock, int index)
{
        int     num    = cfs_percpu_count(sclock->sl_locks);
        int     lockid = index;

        LASSERT(index == CFS_SCLOCK_EXCL ||
                index == CFS_SCLOCK_CURRENT ||
                (index >= 0 && index < num));

        if (index == CFS_SCLOCK_CURRENT)
                lockid = index = cfs_sclock_cur_index(sclock);
        else if (unlikely(index == CFS_SCLOCK_EXCL && num == 1))
                index = 0; /* don't need rwlock dance, less overhead */

        if (likely(index != CFS_SCLOCK_EXCL)) { /* private lock */
                int locked = sclock->sl_locked;

                if (unlikely(locked)) /* serialize with exclusive lock */
                        cfs_read_lock(&sclock->sl_rwlock);

                cfs_spin_lock(&sclock->sl_locks[index]->pl_lock);
                if (unlikely(locked))
                        cfs_read_unlock(&sclock->sl_rwlock);

        } else { /* exclusive lock */
                int     i;

                cfs_write_lock(&sclock->sl_rwlock);
                LASSERT(!sclock->sl_locked);
                sclock->sl_locked = 1;
                /* nobody should take private lock now so I wouldn't starve
                 * for too long time */
                for (i = 0; i < num; i++)
                        cfs_spin_lock(&sclock->sl_locks[i]->pl_lock);
        }

        return lockid;
}

CFS_EXPORT_SYMBOL(cfs_sclock_lock);

void
cfs_sclock_unlock(cfs_scale_lock_t *sclock, int index)
{
        int     num = cfs_percpu_count(sclock->sl_locks);

        LASSERT(index == CFS_SCLOCK_EXCL ||
                (index >= 0 && index < num));

        if (index == CFS_SCLOCK_EXCL && num == 1)
                index = 0; /* don't need rwlock dance */

        if (likely(index != CFS_SCLOCK_EXCL)) {
                cfs_spin_unlock(&sclock->sl_locks[index]->pl_lock);

        } else { /* exclusive locked */
                int     i;

                for (i = 0; i < num; i++)
                        cfs_spin_unlock(&sclock->sl_locks[i]->pl_lock);

                LASSERT(sclock->sl_locked);
                sclock->sl_locked = 0;
                cfs_write_unlock(&sclock->sl_rwlock);
        }
}

CFS_EXPORT_SYMBOL(cfs_sclock_unlock);

/* free percpu refcount */
void
cfs_percpu_ref_free(cfs_atomic_t **refs)
{
        cfs_percpu_free(refs);
}

CFS_EXPORT_SYMBOL(cfs_percpu_ref_free);

/* allocate percpu refcount with initial value @init_val */
cfs_atomic_t **
__cfs_percpu_ref_alloc(cfs_cpumap_t *cpumap, int init_val)
{
        cfs_atomic_t  **refs;
        cfs_atomic_t   *ref;
        int             i;

        refs = __cfs_percpu_alloc(cpumap, sizeof(*ref), 1);
        if (refs == NULL)
                return NULL;

        cfs_percpu_for_each(ref, i, refs)
                cfs_atomic_set(ref, init_val);
        return refs;
}

CFS_EXPORT_SYMBOL(__cfs_percpu_ref_alloc);

/* return sum of percpu refs */
int
cfs_percpu_ref_value(cfs_atomic_t **refs)
{
        cfs_atomic_t   *ref;
        int             i;
        int             val = 0;

        cfs_percpu_for_each(ref, i, refs)
                val += cfs_atomic_read(ref);

        return val;
}

CFS_EXPORT_SYMBOL(cfs_percpu_ref_value);

/* destroy list_head table */
void
cfs_list_table_free(cfs_list_t *table, unsigned int count, int assert_empty)
{
        if (assert_empty) {
                int     i;

                for (i = 0; i < count; i++)
                        LASSERT(cfs_list_empty(&table[i]));
        }

        LIBCFS_FREE(table, count * sizeof(cfs_list_t));
}

CFS_EXPORT_SYMBOL(cfs_list_table_free);

/* allocate list_head table and initialize it */
cfs_list_t *
cfs_list_table_alloc(unsigned int count)
{
        cfs_list_t     *table;
        int             i;

        LIBCFS_ALLOC(table, count * sizeof(cfs_list_t));
        if (table == NULL)
                return NULL;

        for (i = 0; i < count; i++)
                CFS_INIT_LIST_HEAD(&table[i]);

        return table;
}

CFS_EXPORT_SYMBOL(cfs_list_table_alloc);

/* roundup @val to power2 */
int
cfs_power2_roundup(int val)
{
        if (val < 0)
                return -1;

        if (val != LOWEST_BIT_SET(val)) {  /* not a power of 2 already */
                do {
                        val &= ~LOWEST_BIT_SET(val);
                } while (val != LOWEST_BIT_SET(val));
                val <<= 1;                             /* ...and round up */
                if (val == 0)
                        return -1;
        }
        return val;
}

CFS_EXPORT_SYMBOL(cfs_power2_roundup);
