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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Range lock is used to allow multiple threads writing a single shared
 * file given each thread is writing to a non-overlapping portion of the
 * file.
 *
 * Refer to the possible upstream kernel version of range lock by
 * Jan Kara <jack@suse.cz>: https://lkml.org/lkml/2013/1/31/480
 *
 * This file could later replaced by the upstream kernel version.
 */
/*
 * Author: Prakash Surya <surya1@llnl.gov>
 * Author: Bobi Jam <bobijam.xu@intel.com>
 */
#include "range_lock.h"

/**
 * Initialize a range lock tree
 *
 * \param tree [in]	an empty range lock tree
 *
 * Pre:  Caller should have allocated the range lock tree.
 * Post: The range lock tree is ready to function.
 */
void range_lock_tree_init(struct range_lock_tree *tree)
{
	tree->rlt_root = NULL;
	spin_lock_init(&tree->rlt_lock);
}

/**
 * Intialize a range lock node
 *
 * \param lock  [in]	an empty range lock node
 * \param start [in]	start of the covering region
 * \param end   [in]	end of the covering region
 *
 * Pre:  Caller should have allocated the range lock node.
 * Post: The range lock node is meant to cover [start, end] region
 */
void range_lock_init(struct range_lock *lock, __u64 start, __u64 end)
{
	interval_init(&lock->rl_node);
	interval_set(&lock->rl_node, start << PAGE_SHIFT, end << PAGE_SHIFT);
	INIT_LIST_HEAD(&lock->rl_next_lock);
	lock->rl_lock_count = 0;
	lock->rl_blocking_ranges = 0;
}

static inline struct range_lock *next_lock(struct range_lock *lock)
{
	return list_entry(lock->rl_next_lock.next, typeof(*lock), rl_next_lock);
}

/**
 * Helper function of range_unlock()
 *
 * \param node [in]	a range lock found overlapped during interval node
 *			search
 * \param arg [in]	the range lock to be tested, not used
 *
 * \retval INTERVAL_ITER_CONT	indicate to continue the search for next
 *				overlapping range node
 * \retval INTERVAL_ITER_STOP	indicate to stop the search
 */
enum interval_iter range_unlock_cb(struct interval_node *node, void *arg)
{
	struct range_lock *lock = arg;
	struct range_lock *overlap = node2rangelock(node);
	struct range_lock *iter;
	ENTRY;

	list_for_each_entry(iter, &overlap->rl_next_lock, rl_next_lock) {
		if (iter->rl_sequence > lock->rl_sequence) {
			--iter->rl_blocking_ranges;
			LASSERT(iter->rl_blocking_ranges > 0);
		}
	}
	if (overlap->rl_sequence > lock->rl_sequence) {
		--overlap->rl_blocking_ranges;
		if (overlap->rl_blocking_ranges == 0)
			wake_up_process(overlap->rl_task);
	}
	RETURN(INTERVAL_ITER_CONT);
}

/**
 * Unlock a region
 *
 * \param tree [in]	range lock tree
 * \param lock [in]	range lock to be released
 *
 * When release a range lock, wake up those locks only blocked by this lock.
 */
void range_unlock(struct range_lock_tree *tree, struct range_lock *lock)
{
	ENTRY;

	spin_lock(&tree->rlt_lock);
	interval_erase(&lock->rl_node, &tree->rlt_root);
	if (!list_empty(&lock->rl_next_lock)) {
		struct range_lock *next;

		/* Insert the next same range lock into the tree */
		next = next_lock(lock);
		list_del_init(&lock->rl_next_lock);
		next->rl_lock_count = lock->rl_lock_count - 1;
		interval_insert(&next->rl_node, &tree->rlt_root);
	}
	interval_search(tree->rlt_root, &lock->rl_node.in_extent,
			range_unlock_cb, lock);
	spin_unlock(&tree->rlt_lock);

	EXIT;
}

/**
 * Cancel a range lock
 *
 * \param tree [in]	range lock tree
 * \param lock [in]	range lock to be deleted
 *
 * \Pre: the range lock is queued but not granted yet; tree lock held.
 */
static void range_cancel(struct range_lock_tree *tree, struct range_lock *lock)
{
	if (!list_empty(&lock->rl_next_lock)) {
		struct range_lock *tmp;

		if (interval_is_intree(&lock->rl_node)) { /* first lock */
			tmp = next_lock(lock);
			tmp->rl_lock_count = lock->rl_lock_count - 1;
			interval_erase(&lock->rl_node, &tree->rlt_root);
			interval_insert(&tmp->rl_node, &tree->rlt_root);
		} else {
			/* find the first lock in tree */
			list_for_each_entry(tmp, &lock->rl_next_lock,
					    rl_next_lock) {
				if (!interval_is_intree(&tmp->rl_node))
					continue;

				LASSERT(tmp->rl_lock_count > 0);
				tmp->rl_lock_count--;
				break;
			}
		}
		list_del_init(&lock->rl_next_lock);
	} else {
		LASSERT(interval_is_intree(&lock->rl_node));
		interval_erase(&lock->rl_node, &tree->rlt_root);
	}

	interval_search(tree->rlt_root, &lock->rl_node.in_extent,
			range_unlock_cb, lock);
}

/**
 * Helper function of range_lock()
 *
 * \param node [in]	a range lock found overlapped during interval node
 *			search
 * \param arg [in]	the range lock to be tested
 *
 * \retval INTERVAL_ITER_CONT	indicate to continue the search for next
 *				overlapping range node
 * \retval INTERVAL_ITER_STOP	indicate to stop the search
 */
enum interval_iter range_lock_cb(struct interval_node *node, void *arg)
{
	struct range_lock *lock = (struct range_lock *)arg;
	struct range_lock *overlap = node2rangelock(node);

	lock->rl_blocking_ranges += overlap->rl_lock_count + 1;
	RETURN(INTERVAL_ITER_CONT);
}

/**
 * Lock a region
 *
 * \param tree [in]	range lock tree
 * \param lock [in]	range lock node containing the region span
 *
 * \retval 0	get the range lock
 * \retval <0	error code while not getting the range lock
 *
 * If there exists overlapping range lock, the new lock will wait and
 * retry, if later it find that it is not the chosen one to wake up,
 * it wait again.
 */
int range_lock(struct range_lock_tree *tree, struct range_lock *lock)
{
	struct interval_node *node;
	int rc = 0;
	ENTRY;

	spin_lock(&tree->rlt_lock);
	/*
	 * We need to check for all conflicting intervals
	 * already in the tree.
	 */
	interval_search(tree->rlt_root, &lock->rl_node.in_extent,
			range_lock_cb, lock);
	/*
	 * Insert to the tree if I am unique, otherwise I've been linked to
	 * the rl_next_lock of another lock which has the same range as mine
	 * in range_lock_cb().
	 */
	node = interval_insert(&lock->rl_node, &tree->rlt_root);
	if (node != NULL) {
		struct range_lock *tmp = node2rangelock(node);

		list_add_tail(&lock->rl_next_lock, &tmp->rl_next_lock);
		tmp->rl_lock_count++;
	}
	lock->rl_sequence = ++tree->rlt_sequence;

	while (lock->rl_blocking_ranges > 0) {
		lock->rl_task = current;
		__set_current_state(TASK_INTERRUPTIBLE);
		spin_unlock(&tree->rlt_lock);
		schedule();

		spin_lock(&tree->rlt_lock);
		if (signal_pending(current)) {
			range_cancel(tree, lock);
			rc = -EINTR;
			break;
		}
	}
	spin_unlock(&tree->rlt_lock);

	RETURN(rc);
}
