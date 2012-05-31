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
 *
 * Copyright (c) 2011, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/linux/linux-mem.h
 *
 * Basic library routines.
 */

#ifndef __LIBCFS_LINUX_CFS_MEM_H__
#define __LIBCFS_LINUX_CFS_MEM_H__

#ifndef __LIBCFS_LIBCFS_H__
#error Do not #include this file directly. #include <libcfs/libcfs.h> instead
#endif

#ifndef __KERNEL__
#error This include is only for kernel use.
#endif

#include <linux/mm.h>
#include <linux/vmalloc.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#ifdef HAVE_MM_INLINE
# include <linux/mm_inline.h>
#endif

#ifndef GFP_IOFS
#define GFP_IOFS	               (__GFP_IO | __GFP_FS)
#endif

#define CFS_ALLOC_FLAGS_MAPPED_TO_GFP   (1)
/* memory allocation flags */
/* allocation is not allowed to block */
/* allocation is allowed to block */
/* allocation should return zeroed memory */
/* allocation is allowed to call file-system code to free/clean memory */
/* allocation is allowed to do io to free/clean memory */
/* don't report allocation failure to the console */
/* standard allocator flag combination */
#define GFP_KERNEL                  GFP_USER
#define CFS_ALLOC_NOFS                  GFP_NOFS
#define CFS_ALLOC_KERNEL                GFP_KERNEL
/* flags for cfs_page_alloc() */
/* allow to return page beyond KVM. It has to be mapped into KVM by
 * cfs_page_map(); */

typedef struct page                     page_t;
#define PAGE_CACHE_MASK                   (~((__u64)PAGE_CACHE_SIZE-1))




/*
 * Memory allocator
 * XXX Liang: move these declare to public file
 */

#define memory_pressure_get() (current->flags & PF_MEMALLOC)
#define memory_pressure_set() do { current->flags |= PF_MEMALLOC; } while (0)
#define memory_pressure_clr() do { current->flags &= ~PF_MEMALLOC; } while (0)

#if BITS_PER_LONG == 32
/* limit to lowmem on 32-bit systems */
#define NUM_CACHEPAGES \
	min(num_physpages, 1UL << (30 - PAGE_CACHE_SHIFT) * 3 / 4)
#else
#define NUM_CACHEPAGES num_physpages
#endif

/*
 * In Linux there is no way to determine whether current execution context is
 * blockable.
 */
#define ALLOC_ATOMIC_TRY   GFP_ATOMIC

/*
 * SLAB allocator
 * XXX Liang: move these declare to public file
 */
typedef struct kmem_cache cfs_mem_cache_t;
extern cfs_mem_cache_t * cfs_mem_cache_create (const char *, size_t, size_t, unsigned long);
extern int cfs_mem_cache_destroy ( cfs_mem_cache_t * );
extern void *cfs_mem_cache_alloc ( cfs_mem_cache_t *, int);
extern void cfs_mem_cache_free ( cfs_mem_cache_t *, void *);
extern int cfs_mem_is_in_cache(const void *addr, const cfs_mem_cache_t *kmem);

#define DECL_MMSPACE                mm_segment_t __oldfs
#define MMSPACE_OPEN \
        do { __oldfs = get_fs(); set_fs(get_ds());} while(0)
#define MMSPACE_CLOSE               set_fs(__oldfs)


/*
 * NUMA allocators
 *
 * NB: we will rename these functions in a separate patch:
 * - rename kmalloc to cfs_malloc
 * - rename kmalloc/free_page to cfs_page_alloc/free
 * - rename kmalloc/free_large to cfs_vmalloc/vfree
 */
extern void *cfs_cpt_malloc(struct cfs_cpt_table *cptab, int cpt,
			    size_t nr_bytes, unsigned int flags);
extern void *cfs_cpt_vmalloc(struct cfs_cpt_table *cptab, int cpt,
			     size_t nr_bytes);
extern page_t *cfs_page_cpt_alloc(struct cfs_cpt_table *cptab,
				      int cpt, unsigned int flags);
extern void *cfs_mem_cache_cpt_alloc(cfs_mem_cache_t *cachep,
				     struct cfs_cpt_table *cptab,
				     int cpt, unsigned int flags);

/*
 * Shrinker
 */

#ifdef HAVE_SHRINK_CONTROL
# define SHRINKER_ARGS(sc, nr_to_scan, gfp_mask)  \
                       struct shrinker *shrinker, \
                       struct shrink_control *sc
# define shrink_param(sc, var) ((sc)->var)
#else
# ifdef HAVE_SHRINKER_WANT_SHRINK_PTR
#  define SHRINKER_ARGS(sc, nr_to_scan, gfp_mask)  \
                        struct shrinker *shrinker, \
                        int nr_to_scan, gfp_t gfp_mask
# else
#  define SHRINKER_ARGS(sc, nr_to_scan, gfp_mask)  \
                        int nr_to_scan, gfp_t gfp_mask
# endif
# define shrink_param(sc, var) (var)
#endif

#ifdef HAVE_REGISTER_SHRINKER
typedef int (*shrinker_t)(SHRINKER_ARGS(sc, nr_to_scan, gfp_mask));

static inline
struct shrinker *set_shrinker(int seek, shrinker_t func)
{
        struct shrinker *s;

        s = kmalloc(sizeof(*s), GFP_KERNEL);
        if (s == NULL)
                return (NULL);

        s->shrink = func;
        s->seeks = seek;

        register_shrinker(s);

        return s;
}

static inline
void remove_shrinker(struct shrinker *shrinker)
{
        if (shrinker == NULL)
                return;

        unregister_shrinker(shrinker);
        kfree(shrinker);
}
#else
#endif

#endif /* __LINUX_CFS_MEM_H__ */
