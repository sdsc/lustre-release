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

/**
 * allocate the lu_idmap structure
 *
 * \param	client_id		client uid or gid
 * \param	fs_id			filesystem uid or gid
 *
 * \retval	alloated lu_idmap structure on success, NULL otherwise
 */
struct lu_idmap *idmap_create(const __u32 client_id, const __u32 fs_id)
{
	struct lu_idmap	*idmap;

	OBD_ALLOC_PTR(idmap);
	if (idmap == NULL) {
		CERROR("cannot allocate lu_idmap of size %zu bytes",
		       sizeof(idmap));
		return NULL;
	}

	idmap->id_client = client_id;
	idmap->id_fs = fs_id;
	RB_CLEAR_NODE(&idmap->id_client_to_fs);
	RB_CLEAR_NODE(&idmap->id_fs_to_client);
	return idmap;
}

static void idmap_destroy(struct lu_idmap *idmap)

{
	LASSERT(RB_EMPTY_NODE(&idmap->id_fs_to_client) == 0);
	LASSERT(RB_EMPTY_NODE(&idmap->id_client_to_fs) == 0);
	OBD_FREE_PTR(idmap);
}

/**
 * insert idmap into the proper trees
 *
 * \param	node_type		0 for UID
 *					1 for GID
 * \param	idmap			lu_idmap structure to insert
 * \param	nodemap			nodemap to associate with the map
 *
 * \retval	0 on success
 *
 * if the mapping exists, this function will delete it and replace
 * it with the new idmap structure
 */
int idmap_insert(const int node_type, struct lu_idmap *idmap,
		 struct lu_nodemap *nodemap)
{
	struct lu_idmap		*cur;
	struct rb_node		**node;
	struct rb_node		*parent;
	struct rb_root		*fwd_root;
	struct rb_root		*bck_root;
	int			result = 1;

	if (node_type == NODEMAP_UID) {
		fwd_root = &nodemap->nm_client_to_fs_uidmap;
		bck_root = &nodemap->nm_fs_to_client_uidmap;
	} else {
		fwd_root = &nodemap->nm_client_to_fs_gidmap;
		bck_root = &nodemap->nm_fs_to_client_gidmap;
	}

	node = &fwd_root->rb_node;
	parent = NULL;

	cur = idmap_search(nodemap, NODEMAP_FS2CL, node_type,
			   idmap->id_fs);

	if (cur != NULL)
		idmap_delete(node_type, cur, nodemap);

	cur = NULL;

	while (*node) {
		cur = rb_entry(*node, struct lu_idmap,
			       id_client_to_fs);
		if (idmap->id_client < cur->id_client)
			result = -1;
		else if (idmap->id_client > cur->id_client)
			result = 1;

		parent = *node;
		if (result < 0)
			node = &((*node)->rb_left);
		else if (result > 0)
			node = &((*node)->rb_right);
	}

	rb_link_node(&idmap->id_client_to_fs, parent, node);
	rb_insert_color(&idmap->id_client_to_fs, fwd_root);

	node = &bck_root->rb_node;
	parent = NULL;
	cur = NULL;

	while (*node) {
		cur = rb_entry(*node, struct lu_idmap,
			       id_fs_to_client);

		if (idmap->id_fs < cur->id_fs)
			result = -1;
		else if (idmap->id_fs > cur->id_fs)
			result = 1;

		parent = *node;
		if (result < 0)
			node = &((*node)->rb_left);
		else if (result > 0)
			node = &((*node)->rb_right);
	}

	rb_link_node(&idmap->id_fs_to_client, parent, node);
	rb_insert_color(&idmap->id_fs_to_client, bck_root);

	return 0;
}

/**
 * delete idmap from the correct nodemap tree
 *
 * \param	node_type		0 for UID
 *					1 for GID
 * \param	idmap			idmap to delete
 * \param	nodemap			assoicated idmap
 */
void idmap_delete(const int node_type, struct lu_idmap *idmap,
		  struct lu_nodemap *nodemap)
{
	struct rb_root	*fwd_root;
	struct rb_root	*bck_root;

	if (node_type == NODEMAP_UID) {
		fwd_root = &nodemap->nm_client_to_fs_uidmap;
		bck_root = &nodemap->nm_fs_to_client_uidmap;
	} else {
		fwd_root = &nodemap->nm_client_to_fs_gidmap;
		bck_root = &nodemap->nm_fs_to_client_gidmap;
	}

	rb_erase(&idmap->id_client_to_fs, fwd_root);
	rb_erase(&idmap->id_fs_to_client, bck_root);

	idmap_destroy(idmap);
}

/**
 * search for an existing id in the nodemap trees
 *
 * \param	nodemap		nodemap trees to search
 * \param	tree_type	0 for filesystem to client maps
 *				1 for client to filesystem maps
 * \param	node_type	0 for UID
 *				1 for GID
 * \param	id		numeric id for which to search
 *
 * \retval	lu_idmap structure with the map on success
 */
struct lu_idmap *idmap_search(struct lu_nodemap *nodemap,
			      const int tree_type, const int node_type,
			      const __u32 id)
{
	struct rb_node	*node;
	struct rb_root	*root = NULL;
	struct lu_idmap	*idmap;

	if (node_type == NODEMAP_UID && tree_type == NODEMAP_FS2CL)
		root = &nodemap->nm_fs_to_client_uidmap;
	else if (node_type == NODEMAP_UID && tree_type == NODEMAP_CL2FS)
		root = &nodemap->nm_client_to_fs_uidmap;
	else if (node_type == NODEMAP_GID && tree_type == NODEMAP_FS2CL)
		root = &nodemap->nm_fs_to_client_gidmap;
	else if (node_type == NODEMAP_GID && tree_type == NODEMAP_CL2FS)
		root = &nodemap->nm_client_to_fs_gidmap;

	node = root->rb_node;

	if (tree_type == NODEMAP_FS2CL) {
		while (node) {
			idmap = rb_entry(node, struct lu_idmap,
					 id_fs_to_client);
			if (id < idmap->id_fs)
				node = node->rb_left;
			else if (id > idmap->id_fs)
				node = node->rb_right;
			else
				return idmap;
		}
	} else {
		while (node) {
			idmap = rb_entry(node, struct lu_idmap,
					 id_client_to_fs);
			if (id < idmap->id_client)
				node = node->rb_left;
			else if (id > idmap->id_client)
				node = node->rb_right;
			else
				return idmap;
		}
	}

	return NULL;
}

/*
 * delete all idmap trees from a nodemap
 *
 * \param	nodemap		nodemap to delete trees from
 *
 * This uses the postorder safe traversal code that is commited
 * in a later kernel. Each lu_idmap strucuture is destroyed.
 */
void idmap_delete_tree(struct lu_nodemap *nodemap)
{
	struct lu_idmap		*idmap;
	struct lu_idmap		*temp;
	struct rb_root		root;

	root = nodemap->nm_fs_to_client_uidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_fs_to_client) {
		idmap_destroy(idmap);
	}

	root = nodemap->nm_client_to_fs_gidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_client_to_fs) {
		idmap_destroy(idmap);
	}
}
