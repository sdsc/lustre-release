#ifndef _LINUX_DYNLOCKS_H
#define _LINUX_DYNLOCKS_H

#include <linux/list.h>
#include <linux/wait.h>

struct dynlock_handle;

/*
 * lock's namespace:
 *   - list of locks
 *   - lock to protect this list
 */
struct dynlock {
	unsigned		dl_magic;
	struct list_head	dl_list;
	spinlock_t		dl_list_lock;
};

enum dynlock_type {
	DLT_WRITE,
	DLT_READ
};

int dynlock_cache_init(void);
void dynlock_cache_exit(void);
void dynlock_init(struct dynlock *dl);
struct dynlock_handle *dynlock_lock(struct dynlock *dl, unsigned long value,
				    enum dynlock_type lt, gfp_t gfp);
void dynlock_unlock(struct dynlock *dl, struct dynlock_handle *lock);
int dynlock_is_locked(struct dynlock *dl, unsigned long value);

#endif

