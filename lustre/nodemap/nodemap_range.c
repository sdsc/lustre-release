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

#include <linux/rbtree.h>
#include <lustre_net.h>
#include "nodemap_internal.h"

static struct rb_root nodemap_range_tree = RB_ROOT;
static int nodemap_range_highest_id = 1;

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

	/* Check for greater or less than network values
	 * returning if new_network != this_network
	 */

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
	}

	/* something isn't right, return a value that will not insert */
	return 0;
}

struct range_node *range_create(lnet_nid_t min, lnet_nid_t max,
				struct nodemap *nodemap)
{
	struct range_node *range;

	OBD_ALLOC_PTR(range);

	if (range == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		       "for range_node\n", sizeof(struct range_node));
		return NULL;
	}

	range->rn_id = nodemap_range_highest_id;
	if (nodemap_range_highest_id < 0)
		nodemap_range_highest_id = 0;
	nodemap_range_highest_id++;
	range->rn_start_nid = min;
	range->rn_end_nid = max;
	range->rn_nodemap = nodemap;

	return range;
}

void range_destroy(struct range_node *range)
{
	OBD_FREE_PTR(range);
}

int range_insert(struct range_node *data)
{
	struct rb_root *root = &nodemap_range_tree;
	struct rb_node **new_node = &(root->rb_node), *parent = NULL;

	while (*new_node) {
		struct range_node *this_node = container_of(*new_node,
							    struct range_node,
							    rn_node);
		int result = range_compare(data, this_node);

		parent = *new_node;
		if (result < 0)
			new_node = &((*new_node)->rb_left);
		else if (result > 0)
			new_node = &((*new_node)->rb_right);
		else
			return 1;
	}

	rb_link_node(&data->rn_node, parent, new_node);
	rb_insert_color(&data->rn_node, root);

	return 0;
}

int range_delete(struct range_node *data)
{
	rb_erase(&(data->rn_node), &(nodemap_range_tree));
	return 0;
}

struct range_node *range_search(lnet_nid_t *nid)
{
	struct rb_root *root = &nodemap_range_tree;
	struct rb_node *node = root->rb_node;

	while (node) {
		struct range_node *range = container_of(node, struct range_node,
							rn_node);
		int result = nid_range_compare(nid, range);

		if (result < 0)
			node = node->rb_left;
		else if (result > 0)
			node = node->rb_right;
		else
			return range;
	}

	return NULL;
}
