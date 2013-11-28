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
#include <linux/module.h>
#include <lustre_net.h>
#include "nodemap_internal.h"

#define HASH_NODEMAP_BKT_BITS 3
#define HASH_NODEMAP_CUR_BITS 3
#define HASH_NODEMAP_MAX_BITS 7

struct proc_dir_entry *proc_lustre_nodemap_root;
static unsigned int nodemap_highest_id;

/* This will be replaced or set by a config variable when
 * integration is complete */

unsigned int nodemap_idmap_active;
static struct nodemap *default_nodemap;

static cfs_hash_t *nodemap_hash;

static void nodemap_destroy(struct nodemap *nodemap)
{
	OBD_FREE_PTR(nodemap);
}

static void nodemap_getref(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	atomic_inc(&nodemap->nm_refcount);
}

void nodemap_putref_locked(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	LASSERT(atomic_read(&nodemap->nm_refcount) > 1);

	atomic_dec(&nodemap->nm_refcount);

	if (atomic_read(&nodemap->nm_refcount) == 0)
		nodemap_destroy(nodemap);
}

static __u32 nodemap_hashfn(cfs_hash_t *hash_body,
			    const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(((struct nodemap *)key)->nm_name,
				    sizeof(((struct nodemap *)key)->nm_name),
				    mask);
}

static void *nodemap_hs_key(cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	return nodemap->nm_name;
}

static int nodemap_hs_keycmp(const void *key,
			     cfs_hlist_node_t *compared_hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(compared_hnode, struct nodemap, nm_hash);
	return !strcmp((const char *)key, nodemap->nm_name);
}

static void *nodemap_hs_hashobject(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nodemap, nm_hash);
}

static void nodemap_hs_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	nodemap_getref(nodemap);
}

static void nodemap_hs_put_locked(cfs_hash_t *hs,
				  cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	nodemap_putref_locked(nodemap);
}

static cfs_hash_ops_t nodemap_hash_operations = {
	.hs_hash	= nodemap_hashfn,
	.hs_key		= nodemap_hs_key,
	.hs_keycmp	= nodemap_hs_keycmp,
	.hs_object	= nodemap_hs_hashobject,
	.hs_get		= nodemap_hs_get,
	.hs_put_locked	= nodemap_hs_put_locked,
};

static int nodemap_cleanup_iter_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				   cfs_hlist_node_t *hnode, void *data)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);

	LASSERT(atomic_read(&nodemap->nm_refcount) > 0);

	cfs_hash_bd_del_locked(nodemap_hash, bd, hnode);

	lprocfs_remove(&(nodemap->nm_proc_entry));

	nodemap_destroy(nodemap);

	return 0;
}

int nodemap_cleanup_all(void)
{
	cfs_hash_for_each_safe(nodemap_hash, nodemap_cleanup_iter_cb, NULL);

	cfs_hash_putref(nodemap_hash);

	return 0;
}

static int nodemap_init_hash(void)
{
	nodemap_hash = cfs_hash_create("NODEMAP", HASH_NODEMAP_CUR_BITS,
				       HASH_NODEMAP_MAX_BITS,
				       HASH_NODEMAP_BKT_BITS, 0,
				       CFS_HASH_MIN_THETA,
				       CFS_HASH_MAX_THETA,
				       &nodemap_hash_operations,
				       CFS_HASH_DEFAULT);

	if (nodemap_hash == NULL) {
		CERROR("nodemap_init_hash: failed to create nodemap hash "
		       "table\n");

		return -ENOMEM;
	}

	return 0;
}

static int nodemap_create(const char *name, int is_default_nodemap)
{
	int rc;
	struct nodemap *nodemap;

	OBD_ALLOC_PTR(nodemap);

	if (nodemap == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		       "for nodemap %s\n", sizeof(*nodemap),
		       name);

		return -ENOMEM;
	}

	snprintf(nodemap->nm_name, sizeof(nodemap->nm_name), "%s",
		 name);

	INIT_LIST_HEAD(&(nodemap->nm_ranges));
	nodemap->nm_local_to_remote_uidmap = RB_ROOT;
	nodemap->nm_remote_to_local_uidmap = RB_ROOT;
	nodemap->nm_local_to_remote_gidmap = RB_ROOT;
	nodemap->nm_remote_to_local_gidmap = RB_ROOT;

	if (is_default_nodemap == 1) {
		nodemap->nm_id = LUSTRE_NODEMAP_DEFAULT_ID;
		nodemap->nm_flags.nmf_trust_client_ids = 0;
		nodemap->nm_flags.nmf_allow_root_access = 0;
		nodemap->nm_flags.nmf_block_lookups = 0;

		nodemap->nm_squash_uid = NODEMAP_NOBODY_UID;
		nodemap->nm_squash_gid = NODEMAP_NOBODY_GID;

		lprocfs_nodemap_register(name, is_default_nodemap, nodemap);

		default_nodemap = nodemap;
	} else {
		nodemap_highest_id++;
		nodemap->nm_id = nodemap_highest_id;
		nodemap->nm_flags.nmf_trust_client_ids =
				default_nodemap->nm_flags.nmf_trust_client_ids;
		nodemap->nm_flags.nmf_allow_root_access =
				default_nodemap->nm_flags.nmf_allow_root_access;
		nodemap->nm_flags.nmf_block_lookups =
				default_nodemap->nm_flags.nmf_block_lookups;

		nodemap->nm_squash_uid = default_nodemap->nm_squash_uid;
		nodemap->nm_squash_gid = default_nodemap->nm_squash_gid;

		lprocfs_nodemap_register(name, is_default_nodemap, nodemap);
	}

	CFS_INIT_HLIST_NODE(&nodemap->nm_hash);
	nodemap_getref(nodemap);
	rc = cfs_hash_add_unique(nodemap_hash, name, &nodemap->nm_hash);

	if (rc < 0)
		GOTO(out_err, rc);

	return 0;
out_err:
	CDEBUG(D_CONFIG, "%s existing nodemap\n", name);
	nodemap_destroy(nodemap);

	return rc;
}

int nodemap_add(const char *name)
{
	int rc;

	rc = nodemap_create(name, 0);

	if (rc != 0)
		return -EFAULT;

	return 0;
}
EXPORT_SYMBOL(nodemap_add);

int nodemap_del(const char *name)
{
	struct nodemap *nodemap;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if (nodemap == NULL)
		return -ENOENT;

	if (nodemap->nm_id == LUSTRE_NODEMAP_DEFAULT_ID)
		return -EPERM;

	cfs_hash_del(nodemap_hash, name, &(nodemap->nm_hash));

	lprocfs_remove(&(nodemap->nm_proc_entry));

	nodemap_destroy(nodemap);

	return 0;
}
EXPORT_SYMBOL(nodemap_del);

static int __init nodemap_mod_init(void)
{
	int rc;

	rc = nodemap_init_hash();
	nodemap_procfs_init();
	rc = nodemap_create("default", 1);

	return rc;
}

static void __exit nodemap_mod_exit(void)
{
	nodemap_cleanup_all();
	lprocfs_remove(&proc_lustre_nodemap_root);
}


MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Lustre Client Nodemap Management Module");
MODULE_AUTHOR("Joshua Walgenbach <jjw@iu.edu>");

module_init(nodemap_mod_init);
module_exit(nodemap_mod_exit);
