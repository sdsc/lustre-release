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

unsigned int nodemap_idmap_active;
struct nodemap *default_nodemap;

static cfs_hash_t *nodemap_hash;

static void nodemap_destroy(struct nodemap *nodemap)
{
	struct range_node *range, *temp;

	list_for_each_entry_safe(range, temp, &(nodemap->nm_ranges),
				 rn_list) {
		/* Remove from nodemap list of ranges */
		list_del(&(range->rn_list));
		/* Remove from global range tree */
		range_delete(range);
		/* Delete the range node */
		range_destroy(range);
	}

	lprocfs_remove(&(nodemap->nm_proc_entry));
	OBD_FREE_PTR(nodemap);
}

static void nodemap_getref(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	atomic_inc(&nodemap->nm_refcount);
}

void nodemap_putref(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	atomic_dec(&nodemap->nm_refcount);
}

void nodemap_putref_locked(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	LASSERT(atomic_read(&nodemap->nm_refcount) > 1);

	if (atomic_dec_and_test(&nodemap->nm_refcount) == 0)
		nodemap_destroy(nodemap);
}

static __u32 nodemap_hashfn(cfs_hash_t *hash_body,
			    const void *key, unsigned mask)
{
	const struct nodemap *nodemap = key;

	return cfs_hash_djb2_hash(nodemap->nm_name, strlen(nodemap->nm_name),
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

	nodemap = nodemap_hs_key(compared_hnode);
	return !strcmp(key, nodemap->nm_name);
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

int nodemap_add_range(const char *name, char *range_str)
{
	struct nodemap *nodemap;
	struct range_node *range;
	char *min_string, *max_string;
	lnet_nid_t min, max;
	int rc;

	if ((strlen(name) > LUSTRE_NODEMAP_NAME_LENGTH) ||
	    (strlen(name) == 0))
		goto out;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		goto out;

	min_string = strsep(&range_str, ":");
	max_string = strsep(&range_str, ":");

	if ((min_string == NULL) || (max_string == NULL))
		goto out;

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		goto out;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		goto out;

	range = range_create(min, max, nodemap);

	if (range == NULL)
		return -ENOMEM;

	/* Add range to the global range tree */
	rc = range_insert(range);

	if (rc != 0) {
		CERROR("nodemap range insert failed for %s: rc = %d",
		       nodemap->nm_name, rc);
		range_destroy(range);
		return -ENOMEM;
	}

	/* Add range to the nodemap list of ranges */
	INIT_LIST_HEAD(&range->rn_list);
	list_add(&(range->rn_list), &(nodemap->nm_ranges));

	return rc;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_add_range);

int nodemap_del_range(const char *name, char *range_str)
{
	struct nodemap *nodemap;
	struct range_node *range;
	char *min_string, *max_string;
	lnet_nid_t min, max;

	if ((strlen(name) > LUSTRE_NODEMAP_NAME_LENGTH) ||
	    (strlen(name) == 0))
		goto out;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		goto out;

	min_string = strsep(&range_str, ":");
	max_string = strsep(&range_str, ":");

	if ((min_string == NULL) || (max_string == NULL))
		goto out;

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	/* Do some range and network test here */

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		goto out;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		goto out;

	range = range_search(&min);

	if (range == NULL)
		return -EINVAL;

	if ((range->rn_start_nid == min) && (range->rn_end_nid == max)) {
		range_delete(range);
		list_del(&(range->rn_list));
		range_destroy(range);
	}

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_del_range);

static int nodemap_create(const char *name, bool is_default)
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
		nodemap_highest_id++;
		nodemap->nm_id = nodemap_highest_id;
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

	CFS_INIT_HLIST_NODE(&nodemap->nm_hash);
	nodemap_getref(nodemap);
	rc = cfs_hash_add_unique(nodemap_hash, name, &nodemap->nm_hash);

	if (rc < 0)
		GOTO(out_err, rc);

	return 0;
out_err:
	CDEBUG(D_CONFIG, "cannot add nodemap: '%s': rc = %d\n", name, rc);
	nodemap_destroy(nodemap);

	return rc;
}

int nodemap_admin(const char *name, const char *admin_string)
{
	struct nodemap *nodemap;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if (nodemap == NULL)
		goto out;

	if (strlen(admin_string) == 0)
		goto out;

	if (strncmp(admin_string, "0", 1) == 0)
		nodemap->nmf_allow_root_access = 0;
	else
		nodemap->nmf_allow_root_access = 1;

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_admin);

int nodemap_trusted(const char *name, const char *trust_string)
{
	struct nodemap *nodemap;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if (nodemap == NULL)
		goto out;

	if (strlen(trust_string) == 0)
		goto out;

	if (strncmp(trust_string, "0", 1) == 0)
		nodemap->nmf_trust_client_ids = 0;
	else
		nodemap->nmf_trust_client_ids = 1;

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_trusted);

int nodemap_squash_uid(const char *name, char *uid_string)
{
	struct nodemap *nodemap;
	uid_t uid;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if (nodemap == NULL)
		goto out;

	if (sscanf(uid_string, "%u", &uid) != 1)
		goto out;

	nodemap->nm_squash_uid = uid;

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_squash_uid);

int nodemap_squash_gid(const char *name, char *gid_string)
{
	struct nodemap *nodemap;
	gid_t gid;

	nodemap = cfs_hash_lookup(nodemap_hash, name);

	if (nodemap == NULL)
		goto out;

	if (sscanf(gid_string, "%u", &gid) != 1)
		goto out;

	nodemap->nm_squash_gid = gid;

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_squash_gid);

int nodemap_add(const char *name)
{
	return nodemap_create(name, 0);
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
	nodemap_putref(nodemap);

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
