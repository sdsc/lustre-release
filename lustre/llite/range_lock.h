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
#ifndef _RANGE_LOCK_H
#define _RANGE_LOCK_H

#include <libcfs/libcfs.h>
#include <interval_tree.h>

#define RL_FMT "["LPU64", "LPU64"]"
#define RL_PARA(range)			\
	(range)->node.in_extent.start,	\
	(range)->node.in_extent.end

typedef struct range_lock {
	struct interval_node	node;
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
