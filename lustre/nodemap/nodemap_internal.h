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

#ifndef _NODEMAP_INTERNAL_H
#define _NODEMAP_INTERNAL_H

#include <lustre_nodemap.h>
#include <interval_tree.h>

#define MODULE_STRING "nodemap"

/* Default nobody uid and gid values */

#define NODEMAP_NOBODY_UID 99
#define NODEMAP_NOBODY_GID 99

struct lprocfs_static_vars;

/* nodemap root proc directory under fs/lustre */
extern struct proc_dir_entry *proc_lustre_nodemap_root;
/* flag if nodemap is active */
extern bool nodemap_active;

struct lu_nid_range {
	/* unique id set by mgs */
	unsigned int		 rn_id;
	/* lu_nodemap containing this range */
	struct lu_nodemap	*rn_nodemap;
	/* list for nodemap */
	struct list_head	 rn_list;
	/* nid interval tree */
	struct interval_node	 rn_node;
};

struct lu_idmap {
	/* uid/gid of client */
	__u32		id_client;
	/* uid/gid on filesystem */
	__u32		id_fs;
	/* tree mapping client ids to filesystem ids */
	struct rb_node	id_client_to_fs;
	/* tree mappung filesystem to client */
	struct rb_node	id_fs_to_client;
};

/* first 4 bits of the key is the index type */
struct cluster_key {
	__u32 id;
	__u32 unused;
};

struct range_key {
	__u32 nodemap_id;
	__u32 range_id;
};

struct idmap_key {
	__u32 nodemap_id;
	__u32 id_client;
};

struct nodemap_key {
	union {
		struct cluster_key	n;
		struct range_key	r;
		struct idmap_key	i;
		__u64			global;
	} uid;
};
#define nm_idx_get_type(id) (id >> 28)
#define nm_idx_set_type(id, t) (((id) & 0x0FFFFFFF) | (t << 28))

enum nodemap_idx_type {
	NODEMAP_EMPTY_IDX = 0,
	NODEMAP_CLUSTER_IDX = 1,
	NODEMAP_RANGE_IDX = 2,
	NODEMAP_UIDMAP_IDX = 3,
	NODEMAP_GIDMAP_IDX = 4,
	NODEMAP_GLOBAL_IDX = 15,
};

void nodemap_cleanup_all(void);
void nodemap_putref(struct lu_nodemap *nodemap);
int nodemap_lookup(const char *name, struct lu_nodemap **nodemap);
int nodemap_procfs_init(void);
int lprocfs_nodemap_register(const char *name, bool is_default_nodemap,
			     struct lu_nodemap *nodemap);
struct lu_nid_range *range_create(lnet_nid_t min, lnet_nid_t max,
				  struct lu_nodemap *nodemap);
void range_destroy(struct lu_nid_range *range);
int range_insert(struct lu_nid_range *data);
void range_delete(struct lu_nid_range *data);
struct lu_nid_range *range_search(lnet_nid_t nid);
struct lu_nid_range *range_find(lnet_nid_t start_nid, lnet_nid_t end_nid);
int range_parse_nidstring(char *range_string, lnet_nid_t *start_nid,
			  lnet_nid_t *end_nid);
void range_init_tree(void);
struct lu_idmap *idmap_create(__u32 client_id, __u32 fs_id);
void idmap_insert(enum nodemap_id_type id_type, struct lu_idmap *idmap,
		 struct lu_nodemap *nodemap);
void idmap_delete(enum nodemap_id_type id_type,  struct lu_idmap *idmap,
		  struct lu_nodemap *nodemap);
void idmap_delete_tree(struct lu_nodemap *nodemap);
struct lu_idmap *idmap_search(struct lu_nodemap *nodemap,
			      enum nodemap_tree_type,
			      enum nodemap_id_type id_type,
			      __u32 id);
int member_init_hash(struct lu_nodemap *nodemap);
void member_add(struct lu_nodemap *nodemap, struct obd_export *exp);
int member_del(struct lu_nodemap *nodemap, struct obd_export *exp);
void member_delete_hash(struct lu_nodemap *nodemap);
void member_reclassify_nodemap(struct lu_nodemap *nodemap);
void member_revoke_locks(struct lu_nodemap *nodemap);

void nodemap_set_id(struct lu_nodemap *nodemap, unsigned int nodemap_id);
int nodemap_add_idmap_helper(struct lu_nodemap *nodemap,
			     enum nodemap_id_type id_type,
			     const __u32 map[2]);
int nodemap_add_range_helper(struct lu_nodemap *nodemap,
			     const lnet_nid_t nid[2]);
struct lu_nodemap *nodemap_get_by_id(struct lu_nodemap *nodemap_head,
				     int nodemap_id);

struct rb_node *nm_rb_next_postorder(const struct rb_node *node);
struct rb_node *nm_rb_first_postorder(const struct rb_root *root);

#define nm_rbtree_postorder_for_each_entry_safe(pos, n,			\
						root, field)		\
	for (pos = nm_rb_first_postorder(root) ?			\
		rb_entry(nm_rb_first_postorder(root), typeof(*pos),	\
		field) : NULL,						\
		n = (pos && nm_rb_next_postorder(&pos->field)) ?	\
		rb_entry(nm_rb_next_postorder(&pos->field),		\
		typeof(*pos), field) : NULL;				\
		pos != NULL;						\
		pos = n,						\
		n = (pos && nm_rb_next_postorder(&pos->field)) ?	\
		rb_entry(nm_rb_next_postorder(&pos->field),		\
		typeof(*pos), field) : NULL)
#endif  /* _NODEMAP_INTERNAL_H */
