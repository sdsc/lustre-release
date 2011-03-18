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
 */
#define DEBUG_SUBSYSTEM S_LNET

#include <linux/mm.h>
#include <linux/vmalloc.h>
#include <linux/slab.h>
#include <linux/highmem.h>
#include <libcfs/libcfs.h>

static unsigned int cfs_alloc_flags_to_gfp(unsigned flags)
{
	unsigned int mflags = 0;

        if (flags & CFS_ALLOC_ATOMIC)
                mflags |= __GFP_HIGH;
        else
                mflags |= __GFP_WAIT;
        if (flags & CFS_ALLOC_NOWARN)
                mflags |= __GFP_NOWARN;
        if (flags & CFS_ALLOC_IO)
                mflags |= __GFP_IO;
        if (flags & CFS_ALLOC_FS)
                mflags |= __GFP_FS;
        if (flags & CFS_ALLOC_HIGH)
                mflags |= __GFP_HIGH;
        return mflags;
}

void *
 cfs_alloc(size_t nr_bytes, unsigned flags)
{
        return ((flags & CFS_ALLOC_ZERO) != 0) ?
               kzalloc(nr_bytes, cfs_alloc_flags_to_gfp(flags)) :
               kmalloc(nr_bytes, cfs_alloc_flags_to_gfp(flags));
}

void
cfs_free(void *addr)
{
	kfree(addr);
}

void *
cfs_alloc_large(size_t nr_bytes, unsigned flags)
{
        void *ptr = vmalloc(nr_bytes);

        if (ptr != NULL && (flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
        return ptr;
}

void
cfs_free_large(void *addr)
{
	vfree(addr);
}

static int
cfs_cpu_hwnode(cfs_cpumap_t *cpumap, int cpunode)
{
        int     hwcpu  = -1;
        int     hwnode;

        if (cpumap == NULL) /* no cpumap, cpunode is HW cpu id */
                hwcpu = cpunode;
        else if (cpunode >= 0 && cpunode < cpumap->cpm_node_num)
                hwcpu = first_cpu(cpumap->cpm_nodes[cpunode].cpn_mask);

        if (hwcpu < 0 && hwcpu >= NR_CPUS)
                return -1;

        hwnode = cpu_to_node(hwcpu);
        return node_online(hwnode) ? hwnode : -1;
}

void *
__cfs_node_alloc(cfs_cpumap_t *cpumap, int cpunode,
                 size_t nr_bytes, unsigned flags)
{
        unsigned int  f = cfs_alloc_flags_to_gfp(flags);
        void         *ptr;

        ptr = kmalloc_node(nr_bytes, f, cfs_cpu_hwnode(cpumap, cpunode));
        if ((flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
        return ptr;
}

EXPORT_SYMBOL(__cfs_node_alloc);

void *
__cfs_node_alloc_large(cfs_cpumap_t *cpumap, int cpunode,
                       size_t nr_bytes, unsigned flags)
{
        void         *ptr;

        ptr = vmalloc_node(nr_bytes, cfs_cpu_hwnode(cpumap, cpunode));
        if ((flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
        return ptr;
}

EXPORT_SYMBOL(__cfs_node_alloc_large);

void *
__cfs_node_alloc_aligned(cfs_cpumap_t *cpumap, int cpunode, unsigned int size)
{
        void   *var;
        int     rsize = cfs_cacheline_align(size);

        /* all the general caches are L1 aligned */
        var = __cfs_node_alloc(cpumap, cpunode, rsize,
                               CFS_ALLOC_IO | CFS_ALLOC_ZERO);
        return var;
}

EXPORT_SYMBOL(__cfs_node_alloc_aligned);

void
cfs_node_free_aligned(void *ptr, unsigned int size)
{
        kfree(ptr);
}

EXPORT_SYMBOL(cfs_node_free_aligned);

cfs_page_t *cfs_alloc_page(unsigned int flags)
{
        /*
         * XXX nikita: do NOT call portals_debug_msg() (CDEBUG/ENTRY/EXIT)
         * from here: this will lead to infinite recursion.
         */
        return alloc_page(cfs_alloc_flags_to_gfp(flags));
}

void cfs_free_page(cfs_page_t *page)
{
        __free_page(page);
}

cfs_page_t *__cfs_node_alloc_page(cfs_cpumap_t *cpumap, int cpunode,
                                   unsigned int flags)
{
        return alloc_pages_node(cfs_cpu_hwnode(cpumap, cpunode),
                                cfs_alloc_flags_to_gfp(flags), 0);
}

EXPORT_SYMBOL(__cfs_node_alloc_page);

cfs_mem_cache_t *
cfs_mem_cache_create (const char *name, size_t size, size_t offset,
                      unsigned long flags)
{
#ifdef HAVE_KMEM_CACHE_CREATE_DTOR
        return kmem_cache_create(name, size, offset, flags, NULL, NULL);
#else
        return kmem_cache_create(name, size, offset, flags, NULL);
#endif
}

int
cfs_mem_cache_destroy (cfs_mem_cache_t * cachep)
{
#ifdef HAVE_KMEM_CACHE_DESTROY_INT
        return kmem_cache_destroy(cachep);
#else
        kmem_cache_destroy(cachep);
        return 0;
#endif
}

void *
cfs_mem_cache_alloc(cfs_mem_cache_t *cachep, int flags)
{
        return kmem_cache_alloc(cachep, cfs_alloc_flags_to_gfp(flags));
}

void
cfs_mem_cache_free(cfs_mem_cache_t *cachep, void *objp)
{
        return kmem_cache_free(cachep, objp);
}

/**
 * Returns true if \a addr is an address of an allocated object in a slab \a
 * kmem. Used in assertions. This check is optimistically imprecise, i.e., it
 * occasionally returns true for the incorrect addresses, but if it returns
 * false, then the addresses is guaranteed to be incorrect.
 */
int cfs_mem_is_in_cache(const void *addr, const cfs_mem_cache_t *kmem)
{
#ifdef CONFIG_SLAB
        struct page *page;

        /*
         * XXX Copy of mm/slab.c:virt_to_cache(). It won't work with other
         * allocators, like slub and slob.
         */
        page = virt_to_page(addr);
        if (unlikely(PageCompound(page)))
                page = (struct page *)page->private;
        return PageSlab(page) && ((void *)page->lru.next) == kmem;
#else
        return 1;
#endif
}

EXPORT_SYMBOL(cfs_mem_is_in_cache);

EXPORT_SYMBOL(cfs_alloc);
EXPORT_SYMBOL(cfs_free);
EXPORT_SYMBOL(cfs_alloc_large);
EXPORT_SYMBOL(cfs_free_large);
EXPORT_SYMBOL(cfs_alloc_page);
EXPORT_SYMBOL(cfs_free_page);
EXPORT_SYMBOL(cfs_mem_cache_create);
EXPORT_SYMBOL(cfs_mem_cache_destroy);
EXPORT_SYMBOL(cfs_mem_cache_alloc);
EXPORT_SYMBOL(cfs_mem_cache_free);
