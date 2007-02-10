/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */

#ifndef _UPCALL_CACHE_H
#define _UPCALL_CACHE_H

#define UC_CACHE_NEW            0x01
#define UC_CACHE_ACQUIRING      0x02
#define UC_CACHE_INVALID        0x04
#define UC_CACHE_EXPIRED        0x08

#define UC_CACHE_IS_NEW(i)          ((i)->ue_flags & UC_CACHE_NEW)
#define UC_CACHE_IS_INVALID(i)      ((i)->ue_flags & UC_CACHE_INVALID)
#define UC_CACHE_IS_ACQUIRING(i)    ((i)->ue_flags & UC_CACHE_ACQUIRING)
#define UC_CACHE_IS_EXPIRED(i)      ((i)->ue_flags & UC_CACHE_EXPIRED)
#define UC_CACHE_IS_VALID(i)        ((i)->ue_flags == 0)

#define UC_CACHE_SET_NEW(i)         (i)->ue_flags |= UC_CACHE_NEW
#define UC_CACHE_SET_INVALID(i)     (i)->ue_flags |= UC_CACHE_INVALID
#define UC_CACHE_SET_ACQUIRING(i)   (i)->ue_flags |= UC_CACHE_ACQUIRING
#define UC_CACHE_SET_EXPIRED(i)     (i)->ue_flags |= UC_CACHE_EXPIRED
#define UC_CACHE_SET_VALID(i)       (i)->ue_flags = 0

#define UC_CACHE_CLEAR_NEW(i)       (i)->ue_flags &= ~UC_CACHE_NEW
#define UC_CACHE_CLEAR_ACQUIRING(i) (i)->ue_flags &= ~UC_CACHE_ACQUIRING
#define UC_CACHE_CLEAR_INVALID(i)   (i)->ue_flags &= ~UC_CACHE_INVALID
#define UC_CACHE_CLEAR_EXPIRED(i)   (i)->ue_flags &= ~UC_CACHE_EXPIRED

struct upcall_cache_entry {
        struct list_head        ue_hash;
        __u64                   ue_key;
        __u64                   ue_primary;
        struct group_info      *ue_group_info;
        atomic_t                ue_refcount;
        int                     ue_flags;
        cfs_waitq_t             ue_waitq;
        cfs_time_t              ue_acquire_expire;
        cfs_time_t              ue_expire;
};

#define UC_CACHE_HASH_SIZE        (128)
#define UC_CACHE_HASH_INDEX(id)   ((id) & (UC_CACHE_HASH_SIZE - 1))
#define UC_CACHE_UPCALL_MAXPATH   (1024UL)

struct upcall_cache {
        struct list_head        uc_hashtable[UC_CACHE_HASH_SIZE];
        spinlock_t              uc_lock;

        char                    uc_name[40];            /* for upcall */
        char                    uc_upcall[UC_CACHE_UPCALL_MAXPATH];
        cfs_time_t              uc_acquire_expire;      /* jiffies */
        cfs_time_t              uc_entry_expire;        /* jiffies */
};

struct upcall_cache_entry *upcall_cache_get_entry(struct upcall_cache *hash,
                                                  __u64 key, __u32 primary,
                                                  __u32 ngroups, __u32 *groups);
void upcall_cache_put_entry(struct upcall_cache *hash,
                            struct upcall_cache_entry *entry);
int upcall_cache_downcall(struct upcall_cache *hash, __u32 err, __u64 key,
                          __u32 primary, __u32 ngroups, __u32 *groups);
void upcall_cache_flush_idle(struct upcall_cache *cache);
void upcall_cache_flush_all(struct upcall_cache *cache);
struct upcall_cache *upcall_cache_init(const char *name);
void upcall_cache_cleanup(struct upcall_cache *hash);

#endif /* _UPCALL_CACHE_H */
