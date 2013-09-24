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

/* Define constant needed for nodemaps */

#define NODEMAP_LOCAL 0
#define NODEMAP_REMOTE 1

#define UIDMAP 0
#define GIDMAP 1

#define NODEMAP 0;
#define RANGE 1;
#define UID 2;
#define GID 4;

#define LUSTRE_NODEMAP_NAME "nodemap"

extern const struct dt_index_features nodemap_idx_features;

struct nodemap_version_list {
	struct nodemap *nvl_nodemap;
	struct list_head nvl_list;
};

struct exports_list {
	/* struct obd_export *export; */
	struct list_head exl_list;
};

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
	char nm_name[LUSTRE_NODEMAP_NAME_LENGTH]; /* Human readable ID */
	unsigned int nm_id;			  /* Unique ID set by MGS */
	uid_t nm_squash_uid;			  /* UID to squash root */
	gid_t nm_squash_gid;			  /* GID to squash root */
	uid_t nm_admin_uid;			  /* Not used yet */
	struct rb_root nm_ranges;		  /* Tree of NID ranges */
	struct rb_root nm_local_to_remote_uidmap; /* UID map keyed by local */
	struct rb_root nm_remote_to_local_uidmap; /* UID map keyed by remote */
	struct rb_root nm_local_to_remote_gidmap; /* GID map keyed by local */
	struct rb_root nm_remote_to_local_gidmap; /* GID map keyed by remote */
	struct nodemap_flags nm_flags;		  /* nodemap flags */
	struct proc_dir_entry *nm_proc_entry;	  /* proc directory entry */
	struct list_head nm_exports;		  /* attached clients */
	struct list_head nm_list;
};

struct range_node {
	unsigned int rn_id;			  /* Unique ID set by MGS */
	lnet_nid_t rn_start_nid;		  /* Inclusive starting NID */
	lnet_nid_t rn_end_nid;			  /* Inclusive ending NID */
	struct rb_node rn_node;
};

struct uidmap_node {
	uid_t un_remote_uid;			  /* UID from client req */
	uid_t un_local_uid;			  /* Canonical UID on FS */
	struct rb_node un_local_to_remote_node;
	struct rb_node un_remote_to_local_node;
};

struct gidmap_node {
	gid_t gn_remote_gid;			  /* GID from client req */
	gid_t gn_local_gid;			  /* Canonical GID on FS */
	struct rb_node gn_local_to_remote_node;
	struct rb_node gn_remote_to_local_node;
};

struct nodemap *nodemap_search_by_nid(lnet_nid_t *nid);
int nodemap_activate(char *);
int nodemap_add(char *nodemap_name);
int nodemap_del(char *nodemap_name);
int nodemap_add_range(char *nodemap_name, char *range);
int nodemap_del_range(char *nodemap_name, char *range);
int nodemap_add_uidmap(char *nodemap_name, char *map);
int nodemap_del_uidmap(char *nodemap_name, char *map);
int nodemap_add_gidmap(char *nodemap_name, char *map);
int nodemap_del_gidmap(char *nodemap_name, char *map);
int nodemap_admin(char *nodemap_name, char *admin_string);
int nodemap_trusted(char *nodemap_name, char *trusted_string);
int nodemap_squash_uid(char *nodemap_name, char *squash_string);
int nodemap_squash_gid(char *nodemap_name, char *squash_string);
int string_to_ids(char *, __u32 *, __u32 *);

#endif
