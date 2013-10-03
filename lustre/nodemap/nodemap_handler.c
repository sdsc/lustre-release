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

struct list_head nodemap_list;

static int __init nodemap_mod_init(void)
{
	int rc = 0;

	INIT_LIST_HEAD(&nodemap_list);
	nodemap_procfs_init();

	return rc;
}

static void __exit nodemap_mod_exit(void)
{
	nodemap_cleanup_nodemaps();
	lprocfs_remove(&proc_lustre_nodemap_root);
}

struct nodemap *nodemap_search_by_id(unsigned int id)
{
	struct list_head *p, *q;
	struct nodemap *nodemap;

	list_for_each_safe(p, q, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);
		if (nodemap->nm_id == id)
			return nodemap;
	}

	return NULL;
}

struct nodemap *nodemap_search_by_name(char *nodemap_name)
{
	cfs_list_t *p;
	struct nodemap *nodemap;

	if (strlen(nodemap_name) > LUSTRE_NODEMAP_NAME_LENGTH)
		return NULL;

	cfs_list_for_each(p, &nodemap_list) {
		nodemap = cfs_list_entry(p, struct nodemap, nm_list);

		if (strlen(nodemap_name) <= LUSTRE_NODEMAP_NAME_LENGTH)
			if (strcmp(nodemap->nm_name, nodemap_name) == 0)
				return nodemap;
	}

	return NULL;
}

struct nodemap *nodemap_search_by_nid(lnet_nid_t *nid)
{
	struct list_head *p, *q;
	struct nodemap *nodemap;
	struct range_node *range;

	list_for_each_safe(p, q, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);

		range = range_search(&(nodemap->nm_ranges), nid);

		if (range != NULL)
			return nodemap;
	}

	/* No nodemap containing the NID found, so
	 * return the default nodemap
	 */

	nodemap = nodemap_search_by_id(0);

	return nodemap;
}
EXPORT_SYMBOL(nodemap_search_by_nid);

int nodemap_add_range(char *nodemap_name, char *range_str)
{
	struct nodemap *nodemap;
	struct range_node *range;
	char *min_string, *max_string;
	lnet_nid_t min, max;
	int rc;

	if (strlen(nodemap_name) > LUSTRE_NODEMAP_NAME_LENGTH)
		return -EINVAL;

	nodemap = nodemap_search_by_name(nodemap_name);

	if ((nodemap == NULL) || (nodemap->nm_id == 0))
		return -EINVAL;

	min_string = strsep(&range_str, ":");
	max_string = strsep(&range_str, ":");

	if ((min_string == NULL) || (max_string == NULL))
		return -EINVAL;

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	if ((nodemap_check_nid(&min) != 0) ||
	   (nodemap_check_nid(&max) != 0))
		return -EINVAL;

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		return -EINVAL;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		return -EINVAL;

	range = kmalloc(sizeof(struct range_node), GFP_KERNEL);

	if (range == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		      "for range_node", sizeof(struct range_node));
		return -ENOMEM;
	}

	range->rn_id = nodemap_range_highest_id;
	nodemap_range_highest_id++;
	range->rn_start_nid = min;
	range->rn_end_nid = max;

	rc = range_insert(&(nodemap->nm_ranges), range);

	if (rc != 0)
		CERROR("nodemap range insert failed for %s: rc = %d",
		      nodemap->nm_name, rc);

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

	nodemap = nodemap_search_by_name(nodemap_name);

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

	range = range_search(&(nodemap->nm_ranges), &min);

	if (range == NULL)
		return -EINVAL;

	if ((range->rn_start_nid == min) && (range->rn_end_nid == max)) {
		rb_erase(&(range->rn_node), &(nodemap->nm_ranges));
		kfree(range);
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_del_range);

int nodemap_admin(char *nodemap_name, char *admin_string)
{
	struct nodemap *nodemap;

	nodemap = nodemap_search_by_name(nodemap_name);

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

	nodemap = nodemap_search_by_name(nodemap_name);

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

	nodemap = nodemap_search_by_name(nodemap_name);

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

	nodemap = nodemap_search_by_name(nodemap_name);

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
	struct list_head *p;
	struct nodemap *nodemap = NULL;
	int rc;

	/* For now, use the nodemap name as the unique key */

	list_for_each(p, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);

		if (strcmp(nodemap->nm_name, nodemap_name) == 0)
			return 1;
	}

	rc = nodemap_init_nodemap(nodemap_name, 0, nodemap);

	if (rc != 0) {
		CERROR("nodemap initialization failed: rc = %d\n", rc);
		return 1;
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_add);

int nodemap_del(char *nodemap_name)
{
	struct list_head *p, *q;
	struct nodemap *nodemap;

	list_for_each_safe(p, q, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);

		if (strcmp(nodemap->nm_name, nodemap_name) == 0) {

			if (nodemap->nm_id == 0)
				return -EPERM;

			lprocfs_remove(&(nodemap->nm_proc_entry));
			list_del(p);
			kfree(nodemap);
		}
	}

	return 0;
}
EXPORT_SYMBOL(nodemap_del);

int nodemap_cleanup_nodemaps()
{
	struct list_head *p, *q;
	struct nodemap *nodemap;

	list_for_each_safe(p, q, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);
		lprocfs_remove(&(nodemap->nm_proc_entry));
		list_del(p);
		kfree(nodemap);
	}

	return 0;
}

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Lustre Client Nodemap Management Module");
MODULE_AUTHOR("Joshua Walgenbach <jjw@iu.edu>");

module_init(nodemap_mod_init);
module_exit(nodemap_mod_exit);
