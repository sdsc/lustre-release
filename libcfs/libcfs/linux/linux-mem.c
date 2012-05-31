/*
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
cfs_mem_cache_alloc(cfs_mem_cache_t *cachep, int flags)
{
	return kmem_cache_alloc(cachep, flags);
}
EXPORT_SYMBOL(cfs_mem_cache_alloc);

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

void *
cfs_cpt_malloc(struct cfs_cpt_table *cptab, int cpt,
	       size_t nr_bytes, unsigned int flags)
{
	return kmalloc_node(nr_bytes, flags, cfs_cpt_spread_node(cptab, cpt));
}
EXPORT_SYMBOL(cfs_cpt_malloc);

void *
cfs_cpt_vmalloc(struct cfs_cpt_table *cptab, int cpt, size_t nr_bytes)
{
	return vmalloc_node(nr_bytes, cfs_cpt_spread_node(cptab, cpt));
}
EXPORT_SYMBOL(cfs_cpt_vmalloc);

struct page *
cfs_page_cpt_alloc(struct cfs_cpt_table *cptab, int cpt, unsigned int flags)
{
	return alloc_pages_node(cfs_cpt_spread_node(cptab, cpt), flags, 0);
}
EXPORT_SYMBOL(cfs_page_cpt_alloc);

void *
cfs_mem_cache_cpt_alloc(cfs_mem_cache_t *cachep, struct cfs_cpt_table *cptab,
			int cpt, unsigned int flags)
{
	return kmem_cache_alloc_node(cachep, flags,
				     cfs_cpt_spread_node(cptab, cpt));
}
EXPORT_SYMBOL(cfs_mem_cache_cpt_alloc);
