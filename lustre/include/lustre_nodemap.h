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

/** The nodemap id 0 will be the default nodemap. It will have a configuration
 * set by the MGS, but no ranges will be allowed as all NIDs that do not map
 * will be added to the default nodemap
 */

struct lu_nodemap {
	/* human readable ID */
	char			nm_name[LUSTRE_NODEMAP_NAME_LENGTH + 1];
	/* flags to govern nodemap behavior */
	bool			nmf_trust_client_ids:1,
				nmf_allow_root_access:1,
				nmf_block_lookups:1,
				nmf_hmac_required:1,
				nmf_encryption_required:1;
	/* unique ID set by MGS */
	int			nm_id;
	/* nodemap ref counter */
	atomic_t		nm_refcount;
	/* UID to squash unmapped UIDs */
	uid_t			nm_squash_uid;
	/* GID to squash unmapped GIDs */
	gid_t			nm_squash_gid;
	/* NID range list */
	struct list_head	nm_ranges;
	/* UID map keyed by local UID */
	struct rb_root		nm_fs_to_client_uidmap;
	/* UID map keyed by remote UID */
	struct rb_root		nm_client_to_fs_uidmap;
	/* GID map keyed by local UID */
	struct rb_root		nm_fs_to_client_gidmap;
	/* GID map keyed by remote UID */
	struct rb_root		nm_client_to_fs_gidmap;
	/* proc directory entry */
	struct proc_dir_entry	*nm_proc_entry;
	/* attached client members of this nodemap */
	struct list_head	nm_exports;
	/* access by nodemap name */
	cfs_hlist_node_t	nm_hash;
};

void nodemap_activate(const bool value);
int nodemap_add(const char *nodemap_name);
int nodemap_del(const char *nodemap_name);
struct lu_nodemap *nodemap_classify_nid(const lnet_nid_t nid);
int nodemap_parse_range(const char *range_string, lnet_nid_t range[2]);
int nodemap_parse_idmap(const char *idmap_string, __u32 idmap[2]);
int nodemap_add_range(const char *name, const lnet_nid_t nid[2]);
int nodemap_del_range(const char *name, const lnet_nid_t nid[2]);
int nodemap_admin(const char *name, const bool allow_root);
int nodemap_trusted(const char *name, const bool trust_client_ids);
int nodemap_squash_uid(const char *name, const uid_t uid);
int nodemap_squash_gid(const char *name, const gid_t gid);
int nodemap_add_idmap(const char *name, const int cmd, const __u32 map[2]);
int nodemap_del_idmap(const char *name, const int cmd, const __u32 map[2]);
__u32 nodemap_map_id(struct lu_nodemap *nodemap, const int node_type,
		     const int tree_type, const __u32 id);

#endif
