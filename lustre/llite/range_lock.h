#ifndef _RANGE_LOCK_H__
#define _RANGE_LOCK_H__

#include <libcfs/libcfs.h>
#include <interval_tree.h>

/* TODO: Add documentation */

typedef struct range_lock {
	struct interval_node	node;
	cfs_waitq_t		waitq;
	cfs_waitlink_t		link;
} range_lock_t;

typedef struct range_lock_tree {
	struct interval_node	*root;
	spinlock_t		 lock;
} range_lock_tree_t;

void range_lock_tree_init(range_lock_tree_t *tree);
void range_lock_init(range_lock_t *lock, __u64 start, __u64 end);
void range_lock(range_lock_tree_t *tree, range_lock_t *lock);
void range_unlock(range_lock_tree_t *tree, range_lock_t *lock);
#endif
