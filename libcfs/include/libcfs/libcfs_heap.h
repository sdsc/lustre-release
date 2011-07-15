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
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/heap.h
 *
 * Author: Eric Barton  <eeb@whamcloud.com>
 *         Liang Zhen   <liang@whamcloud.com>
 */

#ifndef __LIBCFS_HEAP_H__
#define __LIBCFS_HEAP_H__

typedef struct {
        unsigned int    chn_index;
} cfs_binheap_node_t;

#define CBH_SHIFT       9
#define CBH_SIZE       (1 << CBH_SHIFT)             /* # ptrs per level */
#define CBH_MASK       (CBH_SIZE - 1)
#define CBH_NOB        (CBH_SIZE * sizeof(cfs_binheap_node_t *))

#define CBH_POISON      0xdeadbeef

enum {
        CBH_FLAG_ATOMIC_GROW    = 1,
};

struct cfs_binheap;

typedef struct {
        int             (*hop_enter)(struct cfs_binheap *h,
                                     cfs_binheap_node_t *e);
        void            (*hop_exit)(struct cfs_binheap *h,
                                    cfs_binheap_node_t *e);
        int             (*hop_compare)(cfs_binheap_node_t *a,
                                       cfs_binheap_node_t *b);
} cfs_binheap_ops_t;

typedef struct cfs_binheap {
        /** Triple indirect */
        cfs_binheap_node_t  ****cbh_elements3;
        /** double indirect */
        cfs_binheap_node_t   ***cbh_elements2;
        /** single indirect */
        cfs_binheap_node_t    **cbh_elements1;
        /** # elements referenced */
        unsigned int            cbh_nelements;
        /** high water mark */
        unsigned int            cbh_hwm;
        /** user flags */
        unsigned int            cbh_flags;
        /** operations table */
        cfs_binheap_ops_t      *cbh_ops;
        /** private data */
        void                   *cbh_private;
} cfs_binheap_t;

void cfs_binheap_destroy(cfs_binheap_t *h);
cfs_binheap_t *cfs_binheap_create(cfs_binheap_ops_t *ops, unsigned int flags,
                                  unsigned count, void *arg);
cfs_binheap_node_t *cfs_binheap_find(cfs_binheap_t *h, unsigned int idx);
int cfs_binheap_insert(cfs_binheap_t *h, cfs_binheap_node_t *e);
void cfs_binheap_remove(cfs_binheap_t *h, cfs_binheap_node_t *e);

static inline int
cfs_binheap_size(cfs_binheap_t *h)
{
        return h->cbh_nelements;
}

static inline int
cfs_binheap_is_empty(cfs_binheap_t *h)
{
        return h->cbh_nelements == 0;
}

static inline cfs_binheap_node_t *
cfs_binheap_root(cfs_binheap_t *h)
{
        return cfs_binheap_find(h, 0);
}

static inline cfs_binheap_node_t *
cfs_binheap_remove_root(cfs_binheap_t *h)
{
        cfs_binheap_node_t *e = cfs_binheap_find(h, 0);

        if (e != NULL)
                cfs_binheap_remove(h, e);
        return e;
}

#endif /* __LIBCFS_HEAP_H__ */
