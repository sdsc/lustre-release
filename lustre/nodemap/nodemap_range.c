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
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#include <linux/version.h>
#include <asm/statfs.h>

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>

#include "nodemap_internal.h"

int range_remove_tree(struct rb_root root)
{
	struct range_node *p, *n;

	nm_rbtree_postorder_for_each_entry_safe(p, n, &root, rn_node) {
		rb_erase(&(p->rn_node), &root);
		kfree(p);
	}

	return 0;
}

int nid_range_compare(lnet_nid_t *nid, struct range_node *node)
{
	__u32 network = LNET_NIDNET(*nid);
	__u32 addr = LNET_NIDADDR(*nid);
	__u32 node_network = LNET_NIDNET(node->rn_start_nid);
	__u32 node_start = LNET_NIDADDR(node->rn_start_nid);
	__u32 node_end = LNET_NIDADDR(node->rn_end_nid);

	if (network < node_network)
		return -1;
	else if (network > node_network)
		return 1;

	if ((addr >= node_start) && (addr <= node_end))
		return 0;
	else if (addr < node_start)
		return -1;
	else if (addr > node_end)
		return 1;

	return 1;
}

int range_compare(struct range_node *new, struct range_node *this)
{
	__u32 new_network = LNET_NIDNET(new->rn_start_nid);
	__u32 this_network = LNET_NIDNET(this->rn_start_nid);
	__u32 new_start = LNET_NIDADDR(new->rn_start_nid);
	__u32 new_end = LNET_NIDADDR(new->rn_end_nid);
	__u32 this_start = LNET_NIDADDR(this->rn_start_nid);
	__u32 this_end = LNET_NIDADDR(this->rn_end_nid);

	if (new_network < this_network)
		return -1;
	else if (new_network > this_network)
		return 1;

	if (((new_start >= this_start) && (new_start <= this_end)) ||
	    ((new_end >= this_start) && (new_end <= this_end))) {
		/* Overlapping range, a portion of new is in this */
		return 0;
	} else if (((this_start >= new_start) && (this_start <= new_end)) ||
		   ((this_end >= new_start) && (this_end <= new_end))) {
		/* Overlapping range, a portion of this is in new */
		return 0;
	} else if ((new_start < this_start) && (new_end < this_start)) {
		/* new is less than this */
		return -1;
	} else if ((new_start > this_end) && (new_end > this_end)) {
		/* new is greater than this */
		return 1;
	} else {
		/* something isn't right, return a value that will not insert */
		return 0;
	}
}

int range_insert(struct rb_root *root, struct range_node *data)
{
	struct rb_node **new = &(root->rb_node), *parent = NULL;
	struct range_node *this;
	int result = 1;

	while (*new) {
		this = container_of(*new, struct range_node, rn_node);
		result = range_compare(data, this);

		parent = *new;
		if (result < 0)
			new = &((*new)->rb_left);
		else if (result > 0)
			new = &((*new)->rb_right);
		else
			return 1;
	}

	rb_link_node(&data->rn_node, parent, new);
	rb_insert_color(&data->rn_node, root);

	return 0;
}

struct range_node *range_search(struct rb_root *root, lnet_nid_t *nid)
{
	struct range_node *range;
	struct rb_node *node = root->rb_node;

	int result = 1;

	while (node) {
		range = container_of(node, struct range_node, rn_node);
		result = nid_range_compare(nid, range);

		if (result < 0)
			node = node->rb_left;
		else if (result > 0)
			node = node->rb_right;
		else
			return range;
	}

	return NULL;
}

int nodemap_check_nid(lnet_nid_t *nid)
{
	struct nodemap *nodemap;
	struct range_node *range;
	cfs_list_t *p;

	cfs_list_for_each(p, &nodemap_list) {
		nodemap = cfs_list_entry(p, struct nodemap, nm_list);

		range = range_search(&(nodemap->nm_ranges), nid);

		if (range != NULL)
			return 1;
	}

	return 0;
}
