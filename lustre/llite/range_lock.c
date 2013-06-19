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
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/include/interval_tree.h
 *
 * Author: Prakash Surya <surya1@llnl.gov>
 */
#include "range_lock.h"

void range_lock_tree_init(range_lock_tree_t *tree)
{
	tree->root = NULL;
	spin_lock_init(&tree->lock);
}
EXPORT_SYMBOL(range_lock_tree_init);

void range_lock_init(range_lock_t *lock, __u64 start, __u64 end)
{
	interval_init(&lock->node);
	interval_set(&lock->node, start, end);
}
EXPORT_SYMBOL(range_lock_init);

enum interval_iter range_lock_cb(struct interval_node *node, void *arg)
{
	ENTRY;

	*(range_lock_t **)arg = (range_lock_t *)node;

	RETURN(INTERVAL_ITER_STOP);
}

void range_lock(range_lock_tree_t *tree, range_lock_t *lock)
{
	range_lock_t *overlap;

	ENTRY;

	spin_lock(&tree->lock);

	while (1) {
		overlap = NULL;
		/* We need to check for any conflicting intervals
		 * already in the tree, range_lock_cb will set overlap
		 * to point to the first conflicting lock found. */
		interval_search(tree->root, &lock->node.in_extent,
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
		spin_unlock(&tree->lock);
		schedule();
		spin_lock(&tree->lock);
	}

	interval_insert(&lock->node, &tree->root);

	spin_unlock(&tree->lock);

	EXIT;
}
EXPORT_SYMBOL(range_lock);

void range_unlock(range_lock_tree_t *tree, range_lock_t *lock)
{
	ENTRY;

	spin_lock(&tree->lock);
	interval_erase(&lock->node, &tree->root);
	spin_unlock(&tree->lock);

	EXIT;
}
EXPORT_SYMBOL(range_unlock);
