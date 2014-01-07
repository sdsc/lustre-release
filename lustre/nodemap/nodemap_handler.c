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

#define DEFAULT_NODEMAP "default"

/* nodemap proc root proc directory under fs/lustre */
struct proc_dir_entry *proc_lustre_nodemap_root;

/* Highest numerical lu_nodemap.nm_id defined */
static atomic_t nodemap_highest_id;

/* Simple flag to determine if nodemaps are active */
bool nodemap_idmap_active;

/**
 * pointer to default nodemap kept to keep from
 * lookup it up in the hash since it is needed
 * more often
 */
static struct lu_nodemap *default_nodemap;

/**
 * Hash keyed on nodemap name containing all
 * nodemaps
 */
static cfs_hash_t *nodemap_hash;

/**
 * Nodemap destructor
 *
 * \param	nodemap		nodemap to destroy
 */
static void nodemap_destroy(struct lu_nodemap *nodemap)
{
	lprocfs_remove(&(nodemap->nm_proc_entry));
	OBD_FREE_PTR(nodemap);
}

/**
 * Functions used for the cfs_hash
 */
static void nodemap_getref(struct lu_nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	atomic_inc(&nodemap->nm_refcount);
}

void nodemap_putref(struct lu_nodemap *nodemap)
{
	LASSERT(nodemap != NULL);
	LASSERT(cfs_atomic_read(&nodemap->nm_refcount) > 0);

	if (atomic_dec_and_test(&nodemap->nm_refcount))
		nodemap_destroy(nodemap);
}

static __u32 nodemap_hashfn(cfs_hash_t *hash_body,
			    const void *key, unsigned mask)
{
	const struct lu_nodemap *nodemap = key;

	return cfs_hash_djb2_hash(nodemap->nm_name, strlen(nodemap->nm_name),
				  mask);
}

static void *nodemap_hs_key(cfs_hlist_node_t *hnode)
{
	struct lu_nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct lu_nodemap, nm_hash);
	return nodemap->nm_name;
}

static int nodemap_hs_keycmp(const void *key,
			     cfs_hlist_node_t *compared_hnode)
{
	struct lu_nodemap *nodemap;

	nodemap = nodemap_hs_key(compared_hnode);
	return !strcmp(key, nodemap->nm_name);
}

static void *nodemap_hs_hashobject(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct lu_nodemap, nm_hash);
}

static void nodemap_hs_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct lu_nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct lu_nodemap, nm_hash);
	nodemap_getref(nodemap);
}

static void nodemap_hs_put_locked(cfs_hash_t *hs,
				  cfs_hlist_node_t *hnode)
{
	struct lu_nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct lu_nodemap, nm_hash);
	nodemap_putref(nodemap);
}

static cfs_hash_ops_t nodemap_hash_operations = {
	.hs_hash	= nodemap_hashfn,
	.hs_key		= nodemap_hs_key,
	.hs_keycmp	= nodemap_hs_keycmp,
	.hs_object	= nodemap_hs_hashobject,
	.hs_get		= nodemap_hs_get,
	.hs_put_locked	= nodemap_hs_put_locked,
};

/* end of cfs_hash functions */

/**
 * Helper iterator to clean up nodemap on module exit.
 *
 * \param	hs		hash structure
 * \param	bd		bucket descriptor
 * \param	hnode		hash node
 * \param	data		not used here
 */
static int nodemap_cleanup_iter_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				   cfs_hlist_node_t *hnode, void *data)
{
	struct lu_nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct lu_nodemap, nm_hash);
	nodemap_putref(nodemap);

	return 0;
}

/**
 * Walk the nodemap_hash and remove all nodemaps.
 */
void nodemap_cleanup_all(void)
{
	cfs_hash_for_each_safe(nodemap_hash, nodemap_cleanup_iter_cb, NULL);
	cfs_hash_putref(nodemap_hash);
}

/**
 * Initialize nodemap_hash
 *
 * \retval	0		success
 * \retval	-ENOMEM		cannot create hash
 */
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
		CERROR("cannot create nodemap_hash table\n");
		return -ENOMEM;
	}

	return 0;
}

/**
 * Check for valid nodemap name
 *
 * \param	name		nodemap name
 * \retval	true		valid
 * \retval	false		invalid
 */
static bool nodemap_name_is_valid(const char *name)
{
	for (; *name != '\0'; name++) {
		if (!isalnum(*name) && *name != '_')
			return false;
	}
	return true;
}

/**
 * Nodemap lookup
 *
 * Look nodemap up in the nodemap hash
 *
 * \param	name		name of nodemap
 * \paramA	nodemap		found nodemap or NULL
 * \retval	lu_nodemap	named nodemap
 * \retval	NULL		nodemap doesn't exist
 */
static int nodemap_lookup(const char *name, struct lu_nodemap **nodemap)
{
	int rc = 0;

	if (!nodemap_name_is_valid(name))
		GOTO(out, rc = -EINVAL);

	*nodemap = cfs_hash_lookup(nodemap_hash, name);

out:
	return rc;
}

/**
 * Nodemap constructor
 *
 * Creates an lu_nodemap structure and assigns sane default
 * member values. If this is the default nodemap, the defaults
 * are the most restictive in xterms of mapping behavior. Otherwise
 * the default flags should be inherited from the default nodemap.
 * The adds nodemap to nodemap_hash.
 *
 * \param	name		name of nodemap
 * \param	is_default	true if default nodemap
 * \retval	0		success
 * \retval	-EINVAL		invalid nodemap name
 * \retval	-EEXIST		nodemap already exists
 * \retval	-ENOMEM		cannot allocate memory for nodemap
 */
static int nodemap_create(const char *name, bool is_default)
{
	struct	lu_nodemap *nodemap = NULL;
	int	rc = 0;

	rc = nodemap_lookup(name, &nodemap);
	if (rc < 0)
		goto out;

	if (nodemap != NULL) {
		nodemap_putref(nodemap);
		GOTO(out, rc = -EEXIST);
	}

	OBD_ALLOC_PTR(nodemap);

	if (nodemap == NULL) {
		CERROR("cannot allocate memory (%zu bytes)"
		       "for nodemap '%s'\n", sizeof(*nodemap),
		       name);
		GOTO(out, rc = -ENOMEM);
	}

	snprintf(nodemap->nm_name, sizeof(nodemap->nm_name), "%s", name);

	INIT_LIST_HEAD(&(nodemap->nm_ranges));
	nodemap->nm_local_to_remote_uidmap = RB_ROOT;
	nodemap->nm_remote_to_local_uidmap = RB_ROOT;
	nodemap->nm_local_to_remote_gidmap = RB_ROOT;
	nodemap->nm_remote_to_local_gidmap = RB_ROOT;

	if (is_default) {
		nodemap->nm_id = LUSTRE_NODEMAP_DEFAULT_ID;
		nodemap->nmf_trust_client_ids = 0;
		nodemap->nmf_allow_root_access = 0;
		nodemap->nmf_block_lookups = 0;

		nodemap->nm_squash_uid = NODEMAP_NOBODY_UID;
		nodemap->nm_squash_gid = NODEMAP_NOBODY_GID;

		lprocfs_nodemap_register(name, is_default, nodemap);

		default_nodemap = nodemap;
	} else {
		nodemap->nm_id = atomic_inc_return(&nodemap_highest_id);
		nodemap->nmf_trust_client_ids =
				default_nodemap->nmf_trust_client_ids;
		nodemap->nmf_allow_root_access =
				default_nodemap->nmf_allow_root_access;
		nodemap->nmf_block_lookups =
				default_nodemap->nmf_block_lookups;

		nodemap->nm_squash_uid = default_nodemap->nm_squash_uid;
		nodemap->nm_squash_gid = default_nodemap->nm_squash_gid;

		lprocfs_nodemap_register(name, is_default, nodemap);
	}

	atomic_set(&nodemap->nm_refcount, 1);
	rc = cfs_hash_add_unique(nodemap_hash, name, &nodemap->nm_hash);

	if (rc == 0)
		goto out;

	CERROR("cannot add nodemap: '%s': rc = %d\n", name, rc);
	nodemap_destroy(nodemap);

out:
	return rc;
}

/**
 * Add a nodemap
 *
 * \param	name		name of nodemap
 * \retval	0		success
 * \retval	-EINVAL		invalid nodemap name
 * \retval	-EEXIST		nodemap already exists
 * \retval	-ENOMEM		cannot allocate memory for nodemap
 */
int nodemap_add(const char *name)
{
	return nodemap_create(name, 0);
}
EXPORT_SYMBOL(nodemap_add);

/**
 * Delete a nodemap
 *
 * \param	name		name of nodemmap
 * \retval	0		success
 * \retval	-EINVAL		invalid input
 * \retval	-ENOENT		no existing nodemap
 */
int nodemap_del(const char *name)
{
	struct	lu_nodemap *nodemap;
	int	rc = 0;

	if (strcmp(name, DEFAULT_NODEMAP) == 0)
		GOTO(out, rc = -EINVAL);

	nodemap = cfs_hash_del_key(nodemap_hash, name);
	if (nodemap == NULL)
		GOTO(out, rc = -ENOENT);

	nodemap_putref(nodemap);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_del);

/**
 * Cleanup nodemap module on exit
 */
static void nodemap_mod_exit(void)
{
	nodemap_cleanup_all();
	lprocfs_remove(&proc_lustre_nodemap_root);
}

/**
 * Initialize the nodemap module
 */
static int __init nodemap_mod_init(void)
{
	int rc = 0;

	rc = nodemap_init_hash();
	if (rc != 0)
		goto cleanup;

	nodemap_procfs_init();
	rc = nodemap_create(DEFAULT_NODEMAP, 1);

cleanup:
	if (rc != 0)
		nodemap_mod_exit();

	return rc;
}

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Lustre Client Nodemap Management Module");
MODULE_AUTHOR("Joshua Walgenbach <jjw@iu.edu>");

module_init(nodemap_mod_init);
module_exit(nodemap_mod_exit);
