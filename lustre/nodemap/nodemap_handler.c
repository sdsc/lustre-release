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
