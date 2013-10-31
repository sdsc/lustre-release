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
#define EXPORT_SYMTAB
#endif

#include <linux/version.h>
#include <asm/statfs.h>

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>

#include "nodemap_internal.h"

struct idmap_node *idmap_init(char *idmap_string)
{
	struct idmap_node *idmap = NULL;
	char *local_string, *remote_string;
	__u32 remote, local;

	remote_string = strsep(&idmap_string, ":");
	local_string = strsep(&idmap_string, ":");

	if ((remote_string == NULL) || (local_string == NULL))
		return NULL;

	remote = simple_strtoul(remote_string, NULL, 10);
	local = simple_strtoul(local_string, NULL, 10);

	OBD_ALLOC(idmap, sizeof(struct idmap_node));

	if (idmap == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		       "for idmap", sizeof(struct idmap_node));
		return NULL;
	}

	idmap->id_remote = remote;
	idmap->id_local = local;

	return idmap;
}

struct idmap_node *idmap_search(struct nodemap *nodemap, int tree_type,
				int node_type, __u32 id)
{
	struct rb_node *node;
	struct rb_root *root = NULL;
	struct idmap_node *idmap;

	if ((node_type == NM_UID) &&
	    (tree_type == NM_LOCAL_TO_REMOTE))
		root = &(nodemap->nm_local_to_remote_uidmap);
	else if ((node_type == NM_GID) &&
		 (tree_type == NM_LOCAL_TO_REMOTE))
		root = &(nodemap->nm_local_to_remote_gidmap);
	else if ((node_type == NM_UID) &&
		 (tree_type == NM_REMOTE_TO_LOCAL))
		root = &(nodemap->nm_remote_to_local_uidmap);
	else if ((node_type == NM_GID) &&
		 (tree_type == NM_REMOTE_TO_LOCAL))
		root = &(nodemap->nm_remote_to_local_gidmap);

	node = root->rb_node;

	if (tree_type == NM_LOCAL_TO_REMOTE) {
		while (node) {
			idmap = container_of(node, struct idmap_node,
					     id_local_to_remote);
			if (id < idmap->id_local)
				node = node->rb_left;
			else if (id > idmap->id_local)
				node = node->rb_right;
			else
				return idmap;
		}
	} else if (tree_type == NM_REMOTE_TO_LOCAL) {
		while (node) {
			idmap = container_of(node, struct idmap_node,
					     id_remote_to_local);
			if (id < idmap->id_remote)
				node = node->rb_left;
			else if (id > idmap->id_remote)
				node = node->rb_right;
			else
				return idmap;
		}
	}

	return NULL;
}

int idmap_insert(struct nodemap *nodemap, int node_type,
		 struct idmap_node *idmap)
{
	struct rb_node **new, *parent;
	struct idmap_node *this;
	struct rb_root *root;
	int rc, result = 1;

	if (node_type == NM_UID)
		root = &(nodemap->nm_local_to_remote_uidmap);
	else
		root = &(nodemap->nm_local_to_remote_gidmap);

	new = &(root->rb_node);
	parent = NULL;

	this = idmap_search(nodemap, NM_LOCAL_TO_REMOTE,
			    node_type, idmap->id_local);

	if (this != NULL)
		rc = idmap_delete(nodemap, node_type, this);

	this = idmap_search(nodemap, NM_REMOTE_TO_LOCAL,
			    node_type, idmap->id_remote);

	if (this != NULL)
		rc = idmap_delete(nodemap, node_type, this);

	this = NULL;

	while (*new) {
		this = container_of(*new, struct idmap_node,
				    id_local_to_remote);
		if (idmap->id_local < this->id_local)
			result = -1;
		else if (idmap->id_local > this->id_local)
			result = 1;

		parent = *new;
		if (result < 0)
			new = &((*new)->rb_left);
		else if (result > 0)
			new = &((*new)->rb_right);
	}

	rb_link_node(&idmap->id_local_to_remote, parent, new);
	rb_insert_color(&(idmap->id_local_to_remote), root);

	if (node_type == NM_UID)
		root = &(nodemap->nm_remote_to_local_uidmap);
	else
		root = &(nodemap->nm_remote_to_local_gidmap);

	new = &(root->rb_node);
	parent = NULL;

	while (*new) {
		this = container_of(*new, struct idmap_node,
				    id_remote_to_local);

		if (idmap->id_remote < this->id_remote)
			result = -1;
		else if (idmap->id_remote > this->id_remote)
			result = 1;

		parent = *new;
		if (result < 0)
			new = &((*new)->rb_left);
		else if (result > 0)
			new = &((*new)->rb_right);
	}

	rb_link_node(&idmap->id_remote_to_local, parent, new);
	rb_insert_color(&idmap->id_remote_to_local, root);
	return 0;
}

int idmap_delete(struct nodemap *nodemap, int node_type,
		struct idmap_node *idmap)
{
	struct rb_root *root;

	if (node_type == NM_UID)
		root = &(nodemap->nm_local_to_remote_uidmap);
	else
		root = &(nodemap->nm_local_to_remote_gidmap);

	rb_erase(&(idmap->id_local_to_remote),
		 &(nodemap->nm_local_to_remote_uidmap));

	if (node_type == NM_UID)
		root = &(nodemap->nm_remote_to_local_uidmap);
	else
		root = &(nodemap->nm_remote_to_local_gidmap);

	rb_erase(&(idmap->id_remote_to_local),
		 &(nodemap->nm_remote_to_local_uidmap));

	OBD_FREE(idmap, sizeof(struct idmap_node));

	return 0;
}

int idmap_delete_tree(struct nodemap *nodemap, int node_type)
{
	struct idmap_node *p, *n;
	struct rb_root root;

	if (node_type == NM_UID)
		root = nodemap->nm_local_to_remote_uidmap;
	else
		root = nodemap->nm_local_to_remote_gidmap;

	nm_rbtree_postorder_for_each_entry_safe(p, n, &root,
					id_local_to_remote) {
		OBD_FREE(p, sizeof(struct idmap_node));
	}

	return 0;
}
