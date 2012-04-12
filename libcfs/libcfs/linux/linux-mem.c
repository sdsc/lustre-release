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

static unsigned int cfs_alloc_flags_to_gfp(unsigned int flags)
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
        /* NB: __GFP_ZERO will be ignored by general allocators in kernel
         * versions before 2.6.22 */
        if (flags & CFS_ALLOC_ZERO)
                mflags |= __GFP_ZERO;
        return mflags;
}

void *
cfs_alloc(size_t nr_bytes, unsigned int flags)
{
        return ((flags & CFS_ALLOC_ZERO) == 0) ?
               kmalloc(nr_bytes, cfs_alloc_flags_to_gfp(flags)) :
               kzalloc(nr_bytes, cfs_alloc_flags_to_gfp(flags));
}
EXPORT_SYMBOL(cfs_alloc);

void *
cfs_numa_alloc(struct cfs_cpt_table *cptab, int cpt,
               size_t nr_bytes, unsigned flags)
{
        void   *ptr;

        ptr = kmalloc_node(nr_bytes, cfs_alloc_flags_to_gfp(flags),
                           cfs_cpt_spread_node(cptab, cpt));
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,6,22))
        /* can't support __GFP_ZERO in general allocators */
        if (ptr != NULL && (flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
#endif
        return ptr;
}
EXPORT_SYMBOL(cfs_numa_alloc);

void
cfs_free(void *addr)
{
	kfree(addr);
}
EXPORT_SYMBOL(cfs_free);

void *
cfs_alloc_large(size_t nr_bytes, unsigned int flags)
{
        void *ptr;

        /* NB: new kernel has vzalloc */
        ptr = vmalloc(nr_bytes);
        if (ptr != NULL && (flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
        return ptr;
}
EXPORT_SYMBOL(cfs_alloc_large);

void *
cfs_numa_alloc_large(struct cfs_cpt_table *cptab, int cpt,
                     size_t nr_bytes, unsigned int flags)
{
        void         *ptr;

        /* NB: new kernel has vzalloc_node */
        ptr = vmalloc_node(nr_bytes, cfs_cpt_spread_node(cptab, cpt));
        if (ptr != NULL && (flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
        return ptr;
}
EXPORT_SYMBOL(cfs_numa_alloc_large);

void
cfs_free_large(void *addr)
{
	vfree(addr);
}
EXPORT_SYMBOL(cfs_free_large);

cfs_page_t *cfs_alloc_page(unsigned int flags)
{
        /*
         * XXX nikita: do NOT call portals_debug_msg() (CDEBUG/ENTRY/EXIT)
         * from here: this will lead to infinite recursion.
         */
        return alloc_page(cfs_alloc_flags_to_gfp(flags));
}
EXPORT_SYMBOL(cfs_alloc_page);

cfs_page_t *cfs_numa_alloc_page(struct cfs_cpt_table *cptab,
                                int cpt, unsigned int flags)
{
        return alloc_pages_node(cfs_cpt_spread_node(cptab, cpt),
                                cfs_alloc_flags_to_gfp(flags), 0);
}
EXPORT_SYMBOL(cfs_numa_alloc_page);

void cfs_free_page(cfs_page_t *page)
{
        __free_page(page);
}
EXPORT_SYMBOL(cfs_free_page);

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
EXPORT_SYMBOL(cfs_mem_cache_create);

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
EXPORT_SYMBOL(cfs_mem_cache_destroy);

void *
cfs_mem_cache_alloc(cfs_mem_cache_t *cachep,
                    size_t nr_bytes, unsigned int flags)
{
        return ((flags & CFS_ALLOC_ZERO) == 0) ?
               kmem_cache_alloc(cachep, cfs_alloc_flags_to_gfp(flags)) :
               kmem_cache_zalloc(cachep, cfs_alloc_flags_to_gfp(flags));
}
EXPORT_SYMBOL(cfs_mem_cache_alloc);

void *
cfs_mem_cache_numa_alloc(struct cfs_cpt_table *cptab, int cpt,
                         cfs_mem_cache_t *cachep, size_t nr_bytes,
                         unsigned int flags)
{
        void *ptr;

        ptr = kmem_cache_alloc_node(cachep, cfs_alloc_flags_to_gfp(flags),
                                    cfs_cpt_spread_node(cptab, cpt));
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,6,22))
        /* can't support __GFP_ZERO in general allocators */
        if (ptr != NULL && (flags & CFS_ALLOC_ZERO) != 0)
                memset(ptr, 0, nr_bytes);
#endif
        return ptr;
}
EXPORT_SYMBOL(cfs_mem_cache_numa_alloc);

void
cfs_mem_cache_free(cfs_mem_cache_t *cachep, void *objp)
{
        return kmem_cache_free(cachep, objp);
}
EXPORT_SYMBOL(cfs_mem_cache_free);

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
