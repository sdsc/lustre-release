/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) 2002 Cluster File Systems, Inc.
 *   Author: Phil Schwan <phil@clusterfs.com>
 *
 *   This file is part of Lustre, http://www.lustre.org/
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#define DEBUG_SUBSYSTEM S_CLASS
#ifdef __KERNEL__
# include <linux/types.h>
# include <linux/random.h>
#else 
# include <liblustre.h>
#endif 

#include <linux/obd_support.h>
#include <linux/lustre_handles.h>

static spinlock_t handle_lock = SPIN_LOCK_UNLOCKED;
static __u64 handle_base;
#define HANDLE_INCR 7
static struct list_head *handle_hash = NULL;
static int handle_count = 0;

#define HANDLE_HASH_SIZE (1 << 14)
#define HANDLE_HASH_MASK (HANDLE_HASH_SIZE - 1)

void class_handle_hash(struct portals_handle *h, portals_handle_addref_cb cb)
{
        struct list_head *bucket;
        ENTRY;

        LASSERT(h != NULL);
        LASSERT(list_empty(&h->h_link));

        spin_lock(&handle_lock);
        h->h_cookie = handle_base;
        handle_base += HANDLE_INCR;
        spin_unlock(&handle_lock);

        h->h_addref = cb;
        bucket = handle_hash + (h->h_cookie & HANDLE_HASH_MASK);
        CDEBUG(D_INFO, "adding object %p with handle "LPX64" to hash\n",
               h, h->h_cookie);

        spin_lock(&handle_lock);
        list_add(&h->h_link, bucket);
        handle_count++;
        spin_unlock(&handle_lock);
        EXIT;
}

static void class_handle_unhash_nolock(struct portals_handle *h)
{
        if (list_empty(&h->h_link)) {
                CERROR("removing an already-removed handle ("LPX64")\n",
                       h->h_cookie);
                return;
        }

        CDEBUG(D_INFO, "removing object %p with handle "LPX64" from hash\n",
               h, h->h_cookie);

        handle_count--;
        list_del_init(&h->h_link);
}

void class_handle_unhash(struct portals_handle *h)
{
        spin_lock(&handle_lock);
        class_handle_unhash_nolock(h);
        spin_unlock(&handle_lock);
}

void *class_handle2object(__u64 cookie)
{
        struct list_head *bucket, *tmp;
        void *retval = NULL;
        ENTRY;

        LASSERT(handle_hash != NULL);

        bucket = handle_hash + (cookie & HANDLE_HASH_MASK);

        spin_lock(&handle_lock);
        list_for_each(tmp, bucket) {
                struct portals_handle *h;
                h = list_entry(tmp, struct portals_handle, h_link);

                if (h->h_cookie == cookie) {
                        h->h_addref(h);
                        retval = h;
                        break;
                }
        }
        spin_unlock(&handle_lock);

        RETURN(retval);
}

int class_handle_init(void)
{
        struct list_head *bucket;

        LASSERT(handle_hash == NULL);

        OBD_VMALLOC(handle_hash, sizeof(*handle_hash) * HANDLE_HASH_SIZE);
        if (handle_hash == NULL)
                return -ENOMEM;

        for (bucket = handle_hash + HANDLE_HASH_SIZE - 1; bucket >= handle_hash;
             bucket--)
                INIT_LIST_HEAD(bucket);

        get_random_bytes(&handle_base, sizeof(handle_base));
        LASSERT(handle_base != 0ULL);

        return 0;
}

static void cleanup_all_handles(void)
{
        int i;

        spin_lock(&handle_lock);
        for (i = 0; i < HANDLE_HASH_SIZE; i++) {
                struct list_head *tmp, *pos;
                list_for_each_safe(tmp, pos, &(handle_hash[i])) {
                        struct portals_handle *h;
                        h = list_entry(tmp, struct portals_handle, h_link);

                        CERROR("forcing cleanup for handle "LPX64"\n",
                               h->h_cookie);

                        class_handle_unhash_nolock(h);
                }
        }
        spin_unlock(&handle_lock);
}

void class_handle_cleanup(void)
{
        LASSERT(handle_hash != NULL);

        if (handle_count != 0) {
                CERROR("handle_count at cleanup: %d\n", handle_count);
                cleanup_all_handles();
        }

        OBD_VFREE(handle_hash, sizeof(*handle_hash) * HANDLE_HASH_SIZE);
        handle_hash = NULL;

        if (handle_count)
                CERROR("leaked %d handles\n", handle_count);
}
