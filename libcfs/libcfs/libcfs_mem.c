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

typedef struct {
        unsigned int    va_count;       /* # of buffers */
        unsigned int    va_size:31;     /* size of each var */
        unsigned int    va_aligned:1;   /* cacheline aligned */
        cfs_cpumap_t   *va_cpumap;      /* cpumap */
        void           *va_vars[0];     /* buffer addresses */
} cfs_var_array_t;

#ifdef __KERNEL__

#ifndef CFS_ALLOC_CACHE_ALIGN
/*
 * Only for platform without cacheline alignment allocator.
 *
 * These functions are NOT used by Linux, because all the general
 * caches are L1 aligned on Linux.
 */
#define CFS_CACHE_VAR_MAGIC             0xbabecafe
#define CFS_CACHE_VAR_POISON            0xdeadbeef

typedef struct {
        __u32           cv_magic;       /* magic */
        int             cv_node;        /* cpu node, reserved for debug */
        cfs_cpumap_t   *cv_cpumap;      /* cpumap, reserved for debug */
        void           *cv_header;      /* address of buffer */
} cfs_cache_var_t;

void
cfs_free_cache_aligned(void *var, unsigned int size)
{
        cfs_cache_var_t *cv;

        /* NB: size must be accurate */
        size = (int)cfs_cacheline_align(size + sizeof(cfs_cache_var_t));
        cv = var + size - sizeof(cfs_cache_var_t);
        LASSERT (cv->cv_magic == CFS_CACHE_VAR_MAGIC);
        cv->cv_magic = CFS_CACHE_VAR_POISON;

        cfs_free(cv->cv_header);
}

/*
 * memory block allocated by this function should:
 * 1) have cacheline aligned address
 *    so nobody will share the same cacheline with starting
 *    address of this memory block
 * 2) round up size to cacheline size  
 *    nobody can share the same cacheline at ending address
 *    of this memory block
 */
void *
__cfs_alloc_cache_aligned(cfs_cpumap_t *cpumap, int node, unsigned int size)
{
        cfs_cache_var_t  *cv;
        void             *hdr;
        void             *var;
        int               rsize;

        size = (int)cfs_cacheline_align(size + sizeof(cfs_cache_var_t));
        /* need an extra cacheline-bytes to guarantee starting address be
         * cacheline aligned */
        rsize = (int)cfs_cacheline_align(size + 1);

        hdr = __cfs_node_alloc(cpumap, node, rsize,
                               CFS_ALLOC_IO | CFS_ALLOC_ZERO, 0);
        if (hdr == NULL)
                return NULL;

        var = (void *)cfs_cacheline_align((__u64)hdr);
        cv  = var + size - sizeof(cfs_cache_var_t);

        cv->cv_magic  = CFS_CACHE_VAR_MAGIC;
        cv->cv_node   = node;
        cv->cv_cpumap = cpumap;
        cv->cv_header = hdr;

        return var;
}

CFS_EXPORT_SYMBOL(__cfs_alloc_cache_aligned);
CFS_EXPORT_SYMBOL(cfs_free_cache_aligned);

#endif /* !CFS_ALLOC_CACHE_ALIGN */
#endif /* __KERNEL__ */

/*
 * free per-cpu data, see more detail in __cfs_percpu_alloc
 */
void
cfs_percpu_free(void *vars)
{
        cfs_var_array_t        *varr;
        int                     i;

        varr = container_of(vars, cfs_var_array_t, va_vars[0]);

        for (i = 0; i < varr->va_count; i++) {
                if (varr->va_vars[i] == NULL)
                        continue;

                if (varr->va_aligned) {
                        LIBCFS_NODE_FREE_ALIGNED(varr->va_vars[i],
                                                 varr->va_size);
                } else {
                        LIBCFS_NODE_FREE(varr->va_vars[i],
                                         varr->va_size);
                }
        }

        LIBCFS_FREE_ALIGNED(varr, offsetof(cfs_var_array_t,
                                           va_vars[varr->va_count]));
}

CFS_EXPORT_SYMBOL(cfs_percpu_free);

/*
 * allocate percpu variable, returned value is an array of pointers,
 * variable can be indexed by CPU ID, i.e:
 *
 *      varr = __cfs_percpu_alloc(cfs_cpumap, size, 1);
 *      then caller can access memory block for CPU 0 by varr[0],
 *      memory block for CPU 1 by varr[1]...
 *      memory block for CPU N by varr[N]...
 *
 * if @aligned is true, address and size of each variable are
 * cacheline aligned.
 */
void *
__cfs_percpu_alloc(cfs_cpumap_t *cpumap, unsigned int size, int aligned)
{
        cfs_var_array_t        *varr;
        int                     count;
        int                     i;

        /* NB: @cpumap != NULL: size of array is node_num of @cpumap
         *     @cpumap == NULL: size of array is number of HW cores
         */
        count = cpumap != NULL ?
                __cfs_cpu_node_num(cpumap) : cfs_hw_cpus_possible();

        LIBCFS_ALLOC_ALIGNED(varr, offsetof(cfs_var_array_t, va_vars[count]));
        if (varr == NULL)
                return NULL;

        varr->va_aligned = aligned;
        varr->va_size    = size;
        varr->va_count   = count;
        varr->va_cpumap  = cpumap;
        for (i = 0; i < count; i++) {
                if (aligned) { /* cacheline alignment? */
                        LIBCFS_NODE_ALLOC_ALIGNED_VERBOSE(varr->va_vars[i],
                                                          cpumap, i, size);
                } else {
                        LIBCFS_NODE_ALLOC_VERBOSE(varr->va_vars[i],
                                                  cpumap, i, size,
                                                  (CFS_ALLOC_IO |
                                                   CFS_ALLOC_ZERO));
                }
                if (varr->va_vars[i] == NULL) {
                        cfs_percpu_free((void *)&varr->va_vars[0]);
                        return NULL;
                }
        }
        return (void *)&varr->va_vars[0];
}

CFS_EXPORT_SYMBOL(__cfs_percpu_alloc);

/*
 * return number of CPUs (or number of elements in per-cpu data)
 * according to cpumap of @vars
 */
int
cfs_percpu_count(void *vars)
{
        cfs_var_array_t        *varr;

        varr = container_of(vars, cfs_var_array_t, va_vars[0]);

        LASSERT(varr->va_cpumap == NULL ||
                varr->va_count == __cfs_cpu_node_num(varr->va_cpumap));
        return varr->va_count;
}

CFS_EXPORT_SYMBOL(cfs_percpu_count);

/*
 * return memory block shadowed from current CPU
 */
void *
cfs_percpu_current(void *vars)
{
        cfs_var_array_t        *varr;

        varr = container_of(vars, cfs_var_array_t, va_vars[0]);

        return varr->va_cpumap == NULL ?
               varr->va_vars[cfs_hw_cpu_id()] :
               varr->va_vars[__cfs_cpu_current(varr->va_cpumap)];
}

CFS_EXPORT_SYMBOL(cfs_percpu_current);

/*
 * free variable array, see more detail in cfs_array_alloc
 */
void
cfs_array_free(void *vars)
{
        cfs_var_array_t        *varr;
        int                     i;

        varr = container_of(vars, cfs_var_array_t, va_vars[0]);

        for (i = 0; i < varr->va_count; i++) {
                if (varr->va_vars[i] == NULL)
                        continue;

                if (varr->va_aligned)
                        LIBCFS_FREE_ALIGNED(varr->va_vars[i], varr->va_size);
                else
                        LIBCFS_FREE(varr->va_vars[i], varr->va_size);
        }

        LIBCFS_FREE_ALIGNED(varr, offsetof(cfs_var_array_t,
                                           va_vars[varr->va_count]));
}

CFS_EXPORT_SYMBOL(cfs_array_free);

/*
 * allocate a variable array, returned value is an array of pointers.
 * Caller can specify length of array by @count, @size is size of each
 * memory block in array, if @aligned != 0, all memory blocks in array
 * will be cacheline size aligned
 */
void *
cfs_array_alloc(int count, unsigned int size, int aligned)
{
        cfs_var_array_t        *varr;
        int                     i;

        LIBCFS_ALLOC_ALIGNED(varr, offsetof(cfs_var_array_t, va_vars[count]));
        if (varr == NULL)
                return NULL;

        varr->va_count   = count;
        varr->va_size    = size;
        varr->va_aligned = aligned;
        for (i = 0; i < count; i++) {
                if (aligned)
                        LIBCFS_ALLOC_ALIGNED(varr->va_vars[i], size);
                else
                        LIBCFS_ALLOC(varr->va_vars[i], size);

                if (varr->va_vars[i] != NULL)
                        continue;

                cfs_array_free((void *)&varr->va_vars[0]);
                return NULL;
        }

        return (void *)&varr->va_vars[0];
}

CFS_EXPORT_SYMBOL(cfs_array_alloc);
