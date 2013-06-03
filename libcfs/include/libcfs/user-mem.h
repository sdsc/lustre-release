/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */

#ifndef __LIBCFS_USER_MEM_H__
#define __LIBCFS_USER_MEM_H__

#ifndef __LIBCFS_LIBCFS_H__
#error Do not #include this file directly. #include <libcfs/libcfs.h> instead
#endif

#ifdef __KERNEL__
#error "This is only for user space."
#endif


/* XXX
 * for this moment, liblusre will not rely OST for non-page-aligned write
 */
#define LIBLUSTRE_HANDLE_UNALIGNED_PAGE

struct page {
        void   *addr;
        unsigned long index;
        cfs_list_t list;
        unsigned long private;

        /* internally used by liblustre file i/o */
        int     _offset;
        int     _count;
#ifdef LIBLUSTRE_HANDLE_UNALIGNED_PAGE
        int     _managed;
#endif
        cfs_list_t _node;
};


/* 4K */
#define PAGE_CACHE_SHIFT 12
#define PAGE_CACHE_SIZE (1UL << PAGE_CACHE_SHIFT)
#define CFS_PAGE_MASK (~((__u64)PAGE_CACHE_SIZE-1))

struct page *alloc_page(unsigned int flags);
void __free_page(struct page *pg);
void *page_address(struct page *pg);
void *kmap(struct page *pg);
void kunmap(struct page *pg);

#define get_page(p)			__I_should_not_be_called__(at_all)
#define page_count(p)		__I_should_not_be_called__(at_all)
#define page_index(p)               ((p)->index)
#define page_cache_get(page) do { } while (0)
#define page_cache_release(page) do { } while (0)

/*
 * Memory allocator
 * Inline function, so utils can use them without linking of libcfs
 */
#define __ALLOC_ZERO    (1 << 2)
static inline void *kmalloc(size_t nr_bytes, u_int32_t flags)
{
        void *result;

        result = malloc(nr_bytes);
        if (result != NULL && (flags & __ALLOC_ZERO))
                memset(result, 0, nr_bytes);
        return result;
}

#define kfree(addr)  free(addr)
#define vmalloc(nr_bytes) kmalloc(nr_bytes, 0)
#define vfree(addr) kfree(addr)

#define ALLOC_ATOMIC_TRY   (0)
/*
 * SLAB allocator
 */
typedef struct {
         int size;
} cfs_mem_cache_t;

#define SLAB_HWCACHE_ALIGN 0
#define SLAB_DESTROY_BY_RCU 0
#define SLAB_KERNEL 0
#define SLAB_NOFS 0

cfs_mem_cache_t *
cfs_mem_cache_create(const char *, size_t, size_t, unsigned long);
int cfs_mem_cache_destroy(cfs_mem_cache_t *c);
void *cfs_mem_cache_alloc(cfs_mem_cache_t *c, int gfp);
void cfs_mem_cache_free(cfs_mem_cache_t *c, void *addr);
int cfs_mem_is_in_cache(const void *addr, const cfs_mem_cache_t *kmem);

/*
 * NUMA allocators
 */
#define cfs_cpt_malloc(cptab, cpt, bytes, flags)	\
	kmalloc(bytes, flags)
#define cfs_cpt_vmalloc(cptab, cpt, bytes)		\
	kmalloc(bytes)
#define cfs_page_cpt_alloc(cptab, cpt, mask)		\
	alloc_page(mask)
#define cfs_mem_cache_cpt_alloc(cache, cptab, cpt, gfp)	\
	cfs_mem_cache_alloc(cache, gfp)

#define smp_rmb()	do {} while (0)

/*
 * Copy to/from user
 */
static inline int copy_from_user(void *a, void *b, int c)
{
	memcpy(a,b,c);
	return 0;
}

static inline int copy_to_user(void *a, void *b, int c)
{
	memcpy(a,b,c);
	return 0;
}

#endif
