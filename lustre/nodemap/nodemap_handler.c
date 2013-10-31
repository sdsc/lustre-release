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

#include "nodemap_internal.h"

struct proc_dir_entry *proc_lustre_nodemap_root;
unsigned int nodemap_highest_id;
unsigned int nodemap_range_highest_id = 1;

/* This will be replaced or set by a config variable when
 * integration is complete */

unsigned int nodemap_idmap_active;
struct nodemap *default_nodemap;

cfs_hash_t *nodemap_hash;

/*
 * hash function using a Rotating Hash algorithm
 * Knuth, D. The Art of Computer Programming,
 * Volume 3: Sorting and Searching,
 * Chapter 6.4.
 * Addison Wesley, 1973
 * Also used in lustre/lov/lov_pool.c
 */

static void nodemap_getref(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	cfs_atomic_inc(&nodemap->nm_refcount);
}

void nodemap_putref(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	if (cfs_atomic_dec_and_test(&nodemap->nm_refcount)) {
		LASSERT(cfs_hlist_unhashed(&nodemap->nm_hash));
		OBD_FREE_PTR(nodemap);
		EXIT;
	}
}

void nodemap_putref_locked(struct nodemap *nodemap)
{
	CDEBUG(D_INFO, "nodemap %p\n", nodemap);
	LASSERT(cfs_atomic_read(&nodemap->nm_refcount) > 1);

	cfs_atomic_dec(&nodemap->nm_refcount);
}

static __u32 nodemap_hashfn(cfs_hash_t *hash_body,
			    const void *key, unsigned mask)
{
	int i;
	__u32 result;
	char *nodemap_name;

	result = 0;
	nodemap_name = (char *)key;

	for (i = 0; i < LUSTRE_NODEMAP_NAME_LENGTH; i++) {
		if (nodemap_name[i] == '\0')
			break;
		result = (result << 4)^(result >> 28) ^ nodemap_name[i];
	}
	return result % mask;
}

static void *nodemap_key(cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	return nodemap->nm_name;
}

static int nodemap_hashkey_keycmp(const void *key,
				  cfs_hlist_node_t *compared_hnode)
{
	char *nodemap_name;
	struct nodemap *nodemap;

	nodemap_name = (char *)key;
	nodemap = cfs_hlist_entry(compared_hnode, struct nodemap, nm_hash);
	return !strncmp(nodemap_name, nodemap->nm_name,
			LUSTRE_NODEMAP_NAME_LENGTH);
}

static void *nodemap_hashobject(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nodemap, nm_hash);
}

static void nodemap_hashrefcount_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	nodemap_getref(nodemap);
}

static void nodemap_hashrefcount_put_locked(cfs_hash_t *hs,
					   cfs_hlist_node_t *hnode)
{
	struct nodemap *nodemap;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);
	nodemap_putref_locked(nodemap);
}

cfs_hash_ops_t nodemap_hash_operations = {
	.hs_hash	= nodemap_hashfn,
	.hs_key		= nodemap_key,
	.hs_keycmp	= nodemap_hashkey_keycmp,
	.hs_object	= nodemap_hashobject,
	.hs_get		= nodemap_hashrefcount_get,
	.hs_put_locked	= nodemap_hashrefcount_put_locked,
};

static int nodemap_iter_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
			   cfs_hlist_node_t *hnode, void *data)
{
	struct nodemap *nodemap;
	struct range_node *range, *temp;

	nodemap = cfs_hlist_entry(hnode, struct nodemap, nm_hash);

	LASSERT(atomic_read(&nodemap->nm_refcount) > 0);

	cfs_hash_bd_del_locked(nodemap_hash, bd, hnode);

	lprocfs_remove(&(nodemap->nm_proc_entry));

	list_for_each_entry_safe(range, temp, &(nodemap->nm_ranges),
				 rn_list) {
		list_del(&(range->rn_list));
		range_delete(range);
		OBD_FREE(range, sizeof(struct range_node));
	}

	idmap_delete_tree(nodemap, NM_UID);
	idmap_delete_tree(nodemap, NM_GID);
	OBD_FREE(nodemap, sizeof(struct nodemap));

	return 0;
}

int nodemap_cleanup_nodemaps()
{
	cfs_hash_for_each_safe(nodemap_hash, nodemap_iter_cb, NULL);
	cfs_hash_putref(nodemap_hash);

	return 0;
}

int nodemap_init_hash(void)
{
	nodemap_hash = cfs_hash_create("NODEMAP", HASH_NODEMAP_CUR_BITS,
				       HASH_NODEMAP_MAX_BITS,
				       HASH_NODEMAP_BKT_BITS, 0,
				       CFS_HASH_MIN_THETA,
				       CFS_HASH_MAX_THETA,
				       &nodemap_hash_operations,
				       CFS_HASH_DEFAULT);

	return 0;
}

int nodemap_add_idmap(char *nodemap_name, int node_type, char *map)
{
	struct nodemap *nodemap;
	struct idmap_node *idmap;
	int rc;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		return -EINVAL;

	idmap = idmap_init(map);

	if (idmap == NULL)
		return -EINVAL;

	rc = idmap_insert(nodemap, node_type, idmap);

	if (rc != 0) {
		CERROR("Could not insert idmap into tree\n");
		return -EINVAL;
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_add_idmap);

int nodemap_del_idmap(char *nodemap_name, int node_type, char *local_id_str)
{
	struct nodemap *nodemap;
	struct idmap_node *idmap;
	__u32 local_id;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		goto out;

	if (local_id_str == NULL)
		goto out;

	local_id = simple_strtoul(local_id_str, NULL, 10);

	idmap = idmap_search(nodemap, node_type, NM_LOCAL_TO_REMOTE,
			     local_id);

	if (idmap == NULL)
		goto out;

	idmap_delete(nodemap, node_type, idmap);

	return 0;
out:
	return -EINVAL;
}
EXPORT_SYMBOL(nodemap_del_idmap);

struct nodemap *nodemap_classify_nid(lnet_nid_t nid)
{
	struct range_node *range;

	range = range_search(&nid);

	if (range == NULL)
		return default_nodemap;
	else
		return range->rn_nodemap;
}
EXPORT_SYMBOL(nodemap_classify_nid);

int nodemap_map_id(struct nodemap *nodemap, int tree_type,
		   int node_type, __u32 id)
{
	struct idmap_node *idmap = NULL;

	if (nodemap_idmap_active == 0)
		return id;

	if (nodemap == NULL)
		return -EFAULT;

	/* Handle root */
	if (id == 0) {
		if ((node_type == NM_UID) &&
		    (nodemap->nm_flags.nmf_admin == 0))
			return nodemap->nm_squash_uid;

		if ((node_type == NM_GID) &&
		    (nodemap->nm_flags.nmf_admin == 0))
			return nodemap->nm_squash_gid;

		return id;
	}

	if (nodemap->nm_flags.nmf_trusted == 1)
		return id;

	idmap = idmap_search(nodemap, tree_type, node_type, id);

	if (idmap == NULL) {
		if (node_type == NM_UID)
			return nodemap->nm_squash_uid;
		else
			return nodemap->nm_squash_gid;
	}

	if (tree_type == NM_LOCAL_TO_REMOTE)
		return idmap->id_remote;
	else
		return idmap->id_local;
}
EXPORT_SYMBOL(nodemap_map_id);

int nodemap_add_range(char *nodemap_name, char *range_str)
{
	struct nodemap *nodemap;
	struct range_node *range;
	char *min_string, *max_string;
	lnet_nid_t min, max;
	int rc;

	if (strlen(nodemap_name) > LUSTRE_NODEMAP_NAME_LENGTH)
		return -EINVAL;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		return -EINVAL;

	min_string = strsep(&range_str, ":");
	max_string = strsep(&range_str, ":");

	if ((min_string == NULL) || (max_string == NULL))
		return -EINVAL;

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		return -EINVAL;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		return -EINVAL;

	OBD_ALLOC(range, sizeof(struct range_node));

	if (range == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		      "for range_node", sizeof(struct range_node));
		return -ENOMEM;
	}

	range->rn_id = nodemap_range_highest_id;
	nodemap_range_highest_id++;
	range->rn_start_nid = min;
	range->rn_end_nid = max;
	range->rn_nodemap = nodemap;

	rc = range_insert(range);

	if (rc != 0) {
		CERROR("nodemap range insert failed for %s: rc = %d",
		      nodemap->nm_name, rc);
		OBD_FREE(range, sizeof(struct range_node));
		return -ENOMEM;
	}

	INIT_LIST_HEAD(&range->rn_list);
	list_add(&(range->rn_list), &(nodemap->nm_ranges));

	return rc;
}
EXPORT_SYMBOL(nodemap_add_range);

int nodemap_del_range(char *nodemap_name, char *range_str)
{
	struct nodemap *nodemap;
	struct range_node *range;
	char *min_string, *max_string;
	lnet_nid_t min, max;

	if (strlen(nodemap_name) > LUSTRE_NODEMAP_NAME_LENGTH)
		return -EINVAL;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		return -EINVAL;

	min_string = strsep(&range_str, ":");
	max_string = strsep(&range_str, ":");

	if ((min_string == NULL) || (max_string == NULL))
		return -EINVAL;

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	/* Do some range and network test here */

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		return -EINVAL;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		return -EINVAL;

	range = range_search(&min);

	if (range == NULL)
		return -EINVAL;

	if ((range->rn_start_nid == min) && (range->rn_end_nid == max)) {
		range_delete(range);
		list_del(&(range->rn_list));
		OBD_FREE(range, sizeof(struct range_node));
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_del_range);

struct nodemap *nodemap_init_nodemap(char *nodemap_name, int def_nodemap)
{
	struct nodemap *nodemap;
	int rc;

	OBD_ALLOC(nodemap, sizeof(struct nodemap));

	if (nodemap == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		       "for nodemap %s", sizeof(struct nodemap),
		       nodemap_name);
		return NULL;
	}

	snprintf(nodemap->nm_name, LUSTRE_NODEMAP_NAME_LENGTH,
		nodemap_name);

	INIT_LIST_HEAD(&(nodemap->nm_ranges));
	nodemap->nm_local_to_remote_uidmap = RB_ROOT;
	nodemap->nm_remote_to_local_uidmap = RB_ROOT;
	nodemap->nm_local_to_remote_gidmap = RB_ROOT;
	nodemap->nm_remote_to_local_gidmap = RB_ROOT;

	if (def_nodemap) {
		nodemap->nm_id = LUSTRE_NODEMAP_DEFAULT_ID;
		nodemap->nm_flags.nmf_trusted = 0;
		nodemap->nm_flags.nmf_admin = 0;
		nodemap->nm_flags.nmf_block = 0;

		nodemap->nm_squash_uid = NODEMAP_NOBODY_UID;
		nodemap->nm_squash_gid = NODEMAP_NOBODY_GID;

		lprocfs_nodemap_register(nodemap_name, def_nodemap, nodemap);

		default_nodemap = nodemap;
	} else {
		nodemap_highest_id++;
		nodemap->nm_id = nodemap_highest_id;
		nodemap->nm_flags.nmf_trusted =
				default_nodemap->nm_flags.nmf_trusted;
		nodemap->nm_flags.nmf_admin =
				default_nodemap->nm_flags.nmf_admin;
		nodemap->nm_flags.nmf_block =
				default_nodemap->nm_flags.nmf_block;

		nodemap->nm_squash_uid = default_nodemap->nm_squash_uid;
		nodemap->nm_squash_gid = default_nodemap->nm_squash_gid;

		lprocfs_nodemap_register(nodemap_name, def_nodemap, nodemap);
	}

	CFS_INIT_HLIST_NODE(&nodemap->nm_hash);
	nodemap_getref(nodemap);
	rc = cfs_hash_add_unique(nodemap_hash, nodemap_name, &nodemap->nm_hash);

	if (rc)
		GOTO(out_err, rc = -EEXIST);

	return nodemap;
out_err:
	CDEBUG(D_CONFIG, "%s existing nodemap", nodemap_name);
	OBD_FREE(nodemap, sizeof(struct nodemap));
	return NULL;
}

int nodemap_admin(char *nodemap_name, char *admin_string)
{
	struct nodemap *nodemap;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if (nodemap == NULL)
		return -EFAULT;

	if (strcmp(admin_string, "0") == 0)
		nodemap->nm_flags.nmf_admin = 0;
	else
		nodemap->nm_flags.nmf_admin = 1;

	return 0;
}
EXPORT_SYMBOL(nodemap_admin);

int nodemap_trusted(char *nodemap_name, char *trusted_string)
{
	struct nodemap *nodemap;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if (nodemap == NULL)
		return -EFAULT;

	if (strcmp(trusted_string, "0") == 0)
		nodemap->nm_flags.nmf_trusted = 0;
	else
		nodemap->nm_flags.nmf_trusted = 1;

	return 0;
}
EXPORT_SYMBOL(nodemap_trusted);

int nodemap_squash_uid(char *nodemap_name, char *uid_string)
{
	struct nodemap *nodemap;
	uid_t uid = NODEMAP_NOBODY_UID;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if (nodemap == NULL)
		return -EFAULT;

	if (sscanf(uid_string, "%u", &uid) != 1)
		return -EFAULT;

	nodemap->nm_squash_uid = uid;

	return 0;
}
EXPORT_SYMBOL(nodemap_squash_uid);

int nodemap_squash_gid(char *nodemap_name, char *gid_string)
{
	struct nodemap *nodemap;
	gid_t gid = NODEMAP_NOBODY_GID;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if (nodemap == NULL)
		return -EFAULT;

	if (sscanf(gid_string, "%u", &gid) != 1)
		return -EFAULT;

	nodemap->nm_squash_gid = gid;

	return 0;
}
EXPORT_SYMBOL(nodemap_squash_gid);

int nodemap_add(char *nodemap_name)
{
	struct nodemap *nodemap = NULL;

	nodemap = nodemap_init_nodemap(nodemap_name, 0);

	if (nodemap == NULL) {
		CERROR("nodemap initialization failed\n");
		return 1;
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_add);

int nodemap_del(char *nodemap_name)
{
	struct nodemap *nodemap;
	struct range_node *range, *temp;

	nodemap = cfs_hash_lookup(nodemap_hash, nodemap_name);

	if (nodemap == NULL)
		return -ENOENT;

	if (nodemap->nm_id == LUSTRE_NODEMAP_DEFAULT_ID)
		return -EPERM;

	list_for_each_entry_safe(range, temp, &(nodemap->nm_ranges),
				 rn_list) {
		list_del(&(range->rn_list));
		range_delete(range);
		OBD_FREE(range, sizeof(struct range_node));
	}

	idmap_delete_tree(nodemap, NM_UID);
	idmap_delete_tree(nodemap, NM_GID);

	cfs_hash_del(nodemap_hash, nodemap_name, &(nodemap->nm_hash));

	lprocfs_remove(&(nodemap->nm_proc_entry));

	OBD_FREE(nodemap, sizeof(struct nodemap));

	return 0;
}
EXPORT_SYMBOL(nodemap_del);

static int __init nodemap_mod_init(void)
{
	int rc = 0;

	rc = nodemap_init_hash();
	nodemap_procfs_init();
	default_nodemap = nodemap_init_nodemap("default", 1);

	if (default_nodemap == NULL)
		return -EINVAL;

	return rc;
}

static void __exit nodemap_mod_exit(void)
{
	nodemap_cleanup_nodemaps();
	lprocfs_remove(&proc_lustre_nodemap_root);
}


MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Lustre Client Nodemap Management Module");
MODULE_AUTHOR("Joshua Walgenbach <jjw@iu.edu>");

module_init(nodemap_mod_init);
module_exit(nodemap_mod_exit);
