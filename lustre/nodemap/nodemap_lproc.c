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

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#include <linux/version.h>
#include <asm/statfs.h>

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>

#include <lprocfs_status.h>

#include "nodemap_internal.h"

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{ "active", nodemap_rd_active, nodemap_wr_active, 0 },
	{ "add_nodemap", 0, nodemap_proc_add_nodemap, 0 },
	{ "remove_nodemap", 0, nodemap_proc_del_nodemap, 0 },
	{ 0 }
};

static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "trusted_nodemap", nodemap_rd_trusted, nodemap_wr_trusted, 0 },
	{ "admin_nodemap", nodemap_rd_admin, nodemap_wr_admin, 0 },
	{ "squash_uid", nodemap_rd_squash_uid, nodemap_wr_squash_uid, 0},
	{ "squash_gid", nodemap_rd_squash_gid, nodemap_wr_squash_gid, 0},
	{ 0 }
};

static struct lprocfs_vars lprocfs_default_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "trusted_nodemap", nodemap_rd_trusted, nodemap_wr_trusted, 0 },
	{ "admin_nodemap", nodemap_rd_admin, nodemap_wr_admin, 0 },
	{ "squash_uid", nodemap_rd_squash_uid, nodemap_wr_squash_uid, 0},
	{ "squash_gid", nodemap_rd_squash_gid, nodemap_wr_squash_gid, 0},
	{ 0 }
};

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		     int *eof, void *data)
{
	return sprintf(page, "%u\n", nodemap_idmap_active);
}

int nodemap_wr_active(struct file *file, const char __user *buffer,
		     unsigned long count, void *data)
{
	char active_string[count + 1];
	char *buf_p;
	uid_t active;

	if (copy_from_user(active_string, buffer, count))
		return -EFAULT;

	if (count > 0)
		active_string[count] = 0;
	else
		return count;

	buf_p = active_string;

	if (sscanf(buf_p, "%u", &active) != 1)
		return -EINVAL;

	if (active == 0)
		nodemap_idmap_active = 0;
	else
		nodemap_idmap_active = 1;

	return count;
}

int nodemap_rd_id(char *page, char **start, off_t off, int count,
		 int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_id);
}

int nodemap_rd_squash_uid(char *page, char **start, off_t off, int count,
			 int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_squash_uid);
}

int nodemap_wr_squash_uid(struct file *file, const char __user *buffer,
			 unsigned long count, void *data)
{
	char squash[count + 1];
	char *buf_p;
	struct nodemap *nodemap;
	uid_t squash_uid;

	if (copy_from_user(squash, buffer, count))
		return -EFAULT;

	if (count > 0)
		squash[count] = 0;
	else
		return count;

	nodemap = (struct nodemap *)data;

	buf_p = squash;

	if (sscanf(buf_p, "%u", &squash_uid) != 1)
		return -EINVAL;

	nodemap->nm_squash_uid = squash_uid;

	return count;
}

int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			 int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_squash_gid);
}

int nodemap_wr_squash_gid(struct file *file, const char __user *buffer,
			 unsigned long count, void *data)
{
	char squash[count + 1];
	char *buf_p;
	struct nodemap *nodemap;
	gid_t squash_gid;

	if (copy_from_user(squash, buffer, count))
		return -EFAULT;

	if (count > 0)
		squash[count] = 0;
	else
		return count;

	nodemap = (struct nodemap *)data;

	buf_p = squash;

	if (sscanf(buf_p, "%u", &squash_gid) != 1)
		return -EINVAL;

	nodemap->nm_squash_gid = squash_gid;

	return count;
}

int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		      int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%d\n", nodemap->nm_flags.nmf_trusted);
}

int nodemap_wr_trusted(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char trusted[count + 1];
	char *buf_p;
	struct nodemap *nodemap;
	unsigned int trusted_flag;

	if (copy_from_user(trusted, buffer, count))
		return -EFAULT;

	if (count > 0)
		trusted[count] = 0;
	else
		return count;

	nodemap = (struct nodemap *)data;

	buf_p = trusted;

	if (sscanf(buf_p, "%u", &trusted_flag) != 1)
		return -EINVAL;

	nodemap->nm_flags.nmf_trusted = !!trusted_flag;

	return count;
}

int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		    int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_flags.nmf_admin);
}

int nodemap_wr_admin(struct file *file, const char __user *buffer,
		    unsigned long count, void *data)
{
	char admin[count + 1];
	char *buf_p;
	struct nodemap *nodemap;
	unsigned int admin_flag;
	if (copy_from_user(admin, buffer, count))
		return -EFAULT;

	if (count > 0)
		admin[count] = 0;
	else
		return count;

	nodemap = (struct nodemap *)data;

	buf_p = admin;

	if (sscanf(buf_p, "%u", &admin_flag) != 1)
		return -EINVAL;

	nodemap->nm_flags.nmf_admin = !!admin_flag;

	return count;
}

int nodemap_procfs_init()
{
	int rc = 0;
	ENTRY;

	proc_lustre_nodemap_root = lprocfs_register(LUSTRE_NODEMAP_NAME,
						   proc_lustre_root,
						   lprocfs_nodemap_module_vars,
						   NULL);

	rc = nodemap_init_nodemap("default", 1, default_nodemap);

	if (rc != 0)
		CERROR("failed to initialize default nodemap: rc = %d\n", rc);

	return rc;
}

void lprocfs_nodemap_init_vars(struct lprocfs_static_vars *lvars)
{
	lvars->module_vars = lprocfs_nodemap_module_vars;
	lvars->obd_vars = lprocfs_nodemap_vars;
}

/* Once nodemap configurations are written to the MDT, these values
 * should be passed and defaults overwritten. */

int nodemap_init_nodemap(char *nodemap_name, int def_nodemap,
			       struct nodemap *nodemap)
{
	struct proc_dir_entry *nodemap_proc_entry;

	nodemap = kmalloc(sizeof(struct nodemap), GFP_KERNEL);
	if (nodemap == NULL) {
		CERROR("Cannot allocate memory (%zu o)"
		      "for nodemap %s", sizeof(struct nodemap),
		      nodemap_name);
		return -ENOMEM;
	}

	sprintf(nodemap->nm_name, nodemap_name);

	nodemap->nm_ranges = RB_ROOT;
	nodemap->nm_local_to_remote_uidmap = RB_ROOT;
	nodemap->nm_remote_to_local_uidmap = RB_ROOT;
	nodemap->nm_local_to_remote_gidmap = RB_ROOT;
	nodemap->nm_remote_to_local_gidmap = RB_ROOT;

	nodemap->nm_flags.nmf_trusted = 0;
	nodemap->nm_flags.nmf_admin = 0;
	nodemap->nm_flags.nmf_block = 0;

	nodemap->nm_squash_uid = NODEMAP_NOBODY_UID;
	nodemap->nm_squash_gid = NODEMAP_NOBODY_GID;

	if (def_nodemap == 1) {
		nodemap_proc_entry =
			lprocfs_register(nodemap_name,
					proc_lustre_nodemap_root,
					lprocfs_default_nodemap_vars,
					nodemap);
		/* default nodemap should always be 0 */
		nodemap->nm_id = 0;
	} else {
		nodemap_highest_id++;
		nodemap->nm_id = nodemap_highest_id;
		nodemap_proc_entry = lprocfs_register(nodemap_name,
						     proc_lustre_nodemap_root,
						     lprocfs_nodemap_vars,
						     nodemap);
	}

	nodemap->nm_proc_entry = nodemap_proc_entry;

	INIT_LIST_HEAD(&nodemap->nm_list);
	list_add(&nodemap->nm_list, &nodemap_list);

	return 0;
}

int nodemap_proc_add_nodemap(struct file *file, const char __user *buffer,
		       unsigned long count, void *data)
{
	char nodemap_name[count + 1];
	struct nodemap *nodemap = NULL;
	int rc;

	if (copy_from_user(nodemap_name, buffer, count))
		return -EFAULT;

	/* Check syntax for nodemap names here */

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	rc = nodemap_init_nodemap(nodemap_name, 0, nodemap);

	if (rc != 0)
		CERROR("failed to initialize nodemap: rc = %d\n", rc);

	return count;
}

int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			  unsigned long count, void *data)
{
	char nodemap_name[count + 1];
	struct list_head *p, *q;
	struct nodemap *nodemap;

	if (copy_from_user(nodemap_name, buffer, count))
		return -EFAULT;

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	/* should free any lists, hashes or trees in the nodemap here */

	list_for_each_safe(p, q, &nodemap_list) {
		nodemap = list_entry(p, struct nodemap, nm_list);

		if (strcmp(nodemap->nm_name, nodemap_name) == 0) {
			lprocfs_remove(&(nodemap->nm_proc_entry));
			list_del(p);
			kfree(nodemap);
		}
	}
	return count;
}
