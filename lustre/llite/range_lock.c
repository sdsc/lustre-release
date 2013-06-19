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

/*
 * Initialize a range lock tree
 *
 * \param tree [in]	an empty range lock tree
 *
 * Pre:  Caller should have allocated the range lock tree.
 * Post: The range lock tree is ready to functioning.
 */
void range_lock_tree_init(struct range_lock_tree *tree)
{
	tree->rlt_root = NULL;
	spin_lock_init(&tree->rlt_lock);
}

/*
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
	interval_set(&lock->rl_node, start, end);
	INIT_LIST_HEAD(&lock->rl_next_block);
	lock->rl_task = current;
	lock->rl_blocked = false;
}

/*
 * Helper function of range_lock()
 *
 * \param node [in]	a range lock found overlapped during interval node
 *			search
 * \param arg [out]	the range lock found overlapped
 *
 * \retval INTERVAL_ITER_CONT	indicate to continue the search for next
 *				overlapping range node
 * \retval INTERVAL_ITER_STOP	indicate to stop the search
 */
enum interval_iter range_lock_cb(struct interval_node *node, void *arg)
{
	ENTRY;

	*(struct range_lock **)arg = node2rangelock(node);

	RETURN(INTERVAL_ITER_STOP);
}

/*
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
	struct range_lock *overlap;

	ENTRY;

	spin_lock(&tree->rlt_lock);

	while (1) {
		overlap = NULL;
		/* We need to check for any conflicting intervals
		 * already in the tree, range_lock_cb will set overlap
		 * to point to the first conflicting lock found. */
		interval_search(tree->rlt_root, &lock->rl_node.in_extent,
				range_lock_cb, &overlap);

		/* overlap will point to a conflicting lock if an
		 * overlapping interval is found (via range_lock_cb).
		 * If no conflicting lock is found in the tree, then
		 * break the loop and insert our lock to claim
		 * exclusivity over the interval. */
		if (overlap == NULL)
			break;

		/* An overlap must have been found, thus we can't claim
		 * exclusive access to this range. Sleep and then try
		 * again when the overlapping region is removed. */
		if (list_empty(&lock->rl_next_block)) {
			list_add(&lock->rl_next_block, &overlap->rl_next_block);
		} else {
			list_splice(&lock->rl_next_block,
				    &overlap->rl_next_block);
		}
		lock->rl_blocked = true;
		spin_unlock(&tree->rlt_lock);
sleep_again:
		__set_current_state(TASK_INTERRUPTIBLE);
		schedule();
		if (signal_pending(current)) {
			spin_lock(&tree->rlt_lock);
			list_del_init(&lock->rl_next_block);
			spin_unlock(&tree->rlt_lock);
			RETURN(-EINTR);
		}

		spin_lock(&tree->rlt_lock);
		/* blocking lock still exists */
		if (lock->rl_blocked) {
			spin_unlock(&tree->rlt_lock);
			goto sleep_again;
		}
	}

	interval_insert(&lock->rl_node, &tree->rlt_root);

	spin_unlock(&tree->rlt_lock);

	RETURN(0);
}

/*
 * Unlock a region
 *
 * \param tree [in]	range lock tree
 * \param lock [in]	range lock to be released
 *
 * When release a range lock, choose only one lock to traverse the lock
 * tree, remaining blocked locks are kept on the chosen one's ::rl_next_block
 * list.
 */

void range_unlock(struct range_lock_tree *tree, struct range_lock *lock)
{
	struct range_lock *towake;
	ENTRY;

	spin_lock(&tree->rlt_lock);
	/* pick one lock to wake up */
	if (!list_empty(&lock->rl_next_block)) {
		towake = list_entry(lock->rl_next_block.next,
				  struct range_lock, rl_next_block);
		towake->rl_blocked = false;
		/*
		 * @lock is not in the blocking list, while @towake could still
		 * hold the list of remaining blocking locks.
		 */
		list_del_init(&lock->rl_next_block);
		wake_up_process(towake->rl_task);
	}
	interval_erase(&lock->rl_node, &tree->rlt_root);
	spin_unlock(&tree->rlt_lock);

	EXIT;
}
