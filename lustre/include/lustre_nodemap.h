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

/* The nodemap id 0 will be the default nodemap. It will have a configuration
 * set by the MGS, but no ranges will be allowed as all NIDs that do not map
 * will be added to the default nodemap */

struct nodemap {
	char nm_name[LUSTRE_NODEMAP_NAME_LENGTH + 1]; /* human readable ID */
	unsigned int nm_id;			      /* unique ID set by MGS */
	uid_t nm_squash_uid;			      /* UID to squash *
						       * unmapped UIDs */
	gid_t nm_squash_gid;			      /* GID to squash *
						       * unmapped GIDs */
	struct list_head nm_ranges;		      /* NID range list */
	struct rb_root nm_local_to_remote_uidmap;     /* UID map keyed by
						       * local */
	struct rb_root nm_remote_to_local_uidmap;     /* UID map keyed by
						       * remote */
	struct rb_root nm_local_to_remote_gidmap;     /* GID map keyed by
						       * local */
	struct rb_root nm_remote_to_local_gidmap;     /* GID map keyed by
						       * remote */
	struct {
		unsigned int nmf_trust_client_ids:1;
		unsigned int nmf_allow_root_access:1;
		unsigned int nmf_block_lookups:1;
		unsigned int nmf_hmac_required:1;
		unsigned int nmf_encryption_required:1;
	} nm_flags;				      /* flags to govern *
							 behavior */
	struct proc_dir_entry *nm_proc_entry;	      /* proc directory entry */
	struct list_head nm_exports;		      /* attached clients */
	atomic_t nm_refcount;			      /* nodemap ref counter */
	cfs_hlist_node_t nm_hash;		      /* access by nodemap
						       * name */
};

struct range_node {
	unsigned int rn_id;			     /* Unique ID set by MGS */
	lnet_nid_t rn_start_nid;		     /* Inclusive starting
						      * NID */
	lnet_nid_t rn_end_nid;			     /* Inclusive ending NID */
	struct nodemap *rn_nodemap;		     /* Member of nodemap */
	struct list_head rn_list;		     /* List for nodemap */
	struct rb_node rn_node;			     /* Global tree */
};

int nodemap_add(const char *name);
int nodemap_del(const char *name);
int nodemap_add_range(const char *name, char *nodemap_range);
int nodemap_del_range(const char *name, char *nodemap_range);
int nodemap_admin(const char *name, const char *admin_string);
int nodemap_trusted(const char *name, const char *trust_string);
int nodemap_squash_uid(const char *name, char *uid_string);
int nodemap_squash_gid(const char *name, char *gid_string);

#endif
