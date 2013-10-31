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
 * GPL HEADER END
 */
/*
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#ifndef _LUSTRE_NODEMAP_H
#define _LUSTRE_NODEMAP_H

#define LUSTRE_NODEMAP_NAME "nodemap"
#define LUSTRE_NODEMAP_NAME_LENGTH 16

#define LUSTRE_NODEMAP_DEFAULT_ID 0

#define NM_UID 0
#define NM_GID 1
#define NM_LOCAL_TO_REMOTE 0
#define NM_REMOTE_TO_LOCAL 1

struct nodemap_flags {
	unsigned int nmf_trusted:1;	/* If set, we trust the client IDs */
	unsigned int nmf_admin:1;	/* If set, we allow UID/GID 0
					 * unsquashed */
	unsigned int nmf_block:1;	/* If set, we block lookups */
	unsigned int nmf_hmac:1;	/* if set, require hmac */
	unsigned int nmf_encrypted:1;	/* if set, require encryption */
	unsigned int nmf_future:27;
};

/* The nodemap id 0 will be the default nodemap. It will have a configuration
 * set by the MGS, but no ranges will be allowed as all NIDs that do not map
 * will be added to the default nodemap */

struct nodemap {
	char nm_name[LUSTRE_NODEMAP_NAME_LENGTH + 1]; /* human readable ID */
	unsigned int nm_id;			      /* unique ID set by MGS */
	uid_t nm_squash_uid;			      /* UID to squash root */
	gid_t nm_squash_gid;			      /* GID to squash root */
	uid_t nm_admin_uid;			      /* not used yet */
	struct list_head nm_ranges;		      /* range list */
	struct rb_root nm_local_to_remote_uidmap;     /* UID map keyed by
						       * local */
	struct rb_root nm_remote_to_local_uidmap;     /* UID map keyed by
						       * remote */
	struct rb_root nm_local_to_remote_gidmap;     /* GID map keyed by
						       * local */
	struct rb_root nm_remote_to_local_gidmap;     /* GID map keyed by
						       * remote */
	struct nodemap_flags nm_flags;		      /* nodemap flags */
	struct proc_dir_entry *nm_proc_entry;	      /* proc directory entry */
	struct list_head nm_exports;		      /* attached clients */
	cfs_atomic_t nm_refcount;		      /* nodemap ref counter */
	cfs_hlist_node_t nm_hash;		      /* access by nodemap
						       * name */
};

struct range_node {
	unsigned int rn_id;			      /* Unique ID set by MGS */
	lnet_nid_t rn_start_nid;		      /* Inclusive starting
						       * NID */
	lnet_nid_t rn_end_nid;			      /* Inclusive ending NID */
	struct nodemap *rn_nodemap;		      /* Member of nodemap */
	struct list_head rn_list;		      /* List for nodemap */
	struct rb_node rn_node;			      /* Global tree */
};

struct idmap_node {
	__u32 id_remote;			      /* Client id */
	__u32 id_local;				      /* Local filesystem id */
	struct rb_node id_local_to_remote;
	struct rb_node id_remote_to_local;
};

int nodemap_add(char *nodemap_name);
int nodemap_del(char *nodemap_name);
struct nodemap *nodemap_classify_nid(lnet_nid_t nid);
int nodemap_add_range(char *nodemap_name, char *nodemap_range);
int nodemap_del_range(char *nodemap_name, char *nodemap_range);
int nodemap_map_id(struct nodemap *nodemap, int tree_type,
		   int node_type, __u32 id);
int nodemap_add_idmap(char *nodemap_name, int node_type, char *map);
int nodemap_del_idmap(char *nodemap_name, int node_type, char *remote_id);
int nodemap_admin(char *nodemap_name, char *admin_string);
int nodemap_trusted(char *nodemap_name, char *trusted_string);
int nodemap_squash_uid(char *nodemap_name, char *squash_string);
int nodemap_squash_gid(char *nodemap_name, char *squash_string);

#endif
