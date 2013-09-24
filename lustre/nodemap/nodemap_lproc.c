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

#define NODEMAP_LPROC_ID_LEN 16
#define NODEMAP_LPROC_FLAG_LEN 2

#include <linux/version.h>
#include <linux/module.h>

#include <lprocfs_status.h>

#include "nodemap_internal.h"

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{ "active", nodemap_rd_active, nodemap_wr_active, 0 },
#ifdef NODEMAP_PROC_DEBUG
	{ "add_nodemap", 0, nodemap_proc_add_nodemap, 0 },
	{ "remove_nodemap", 0, nodemap_proc_del_nodemap, 0 },
#endif /* NODEMAP_PROC_DEBUG */
	{ 0 }
};

#ifdef NODEMAP_PROC_DEBUG
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
#else
static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "trusted_nodemap", nodemap_rd_trusted, 0, 0 },
	{ "admin_nodemap", nodemap_rd_admin, 0, 0 },
	{ "squash_uid", nodemap_rd_squash_uid, 0, 0},
	{ "squash_gid", nodemap_rd_squash_gid, 0, 0},
	{ 0 }
};

static struct lprocfs_vars lprocfs_default_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "trusted_nodemap", nodemap_rd_trusted, 0, 0 },
	{ "admin_nodemap", nodemap_rd_admin, 0, 0 },
	{ "squash_uid", nodemap_rd_squash_uid, 0, 0},
	{ "squash_gid", nodemap_rd_squash_gid, 0, 0},
	{ 0 }
};
#endif /* NODEMAP_PROC_DEBUG */

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		      int *eof, void *data)
{
	return sprintf(page, "%u\n", nodemap_idmap_active);
}

int nodemap_wr_active(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char active_string[NODEMAP_LPROC_FLAG_LEN + 1];
	__u32 active;

	if (count == 0)
		return count;

	active_string[NODEMAP_LPROC_FLAG_LEN] = '\0';

	if (copy_from_user(active_string, buffer, NODEMAP_LPROC_FLAG_LEN))
		return -EFAULT;

	active = simple_strtoul(active_string, NULL, 10);

	nodemap_idmap_active = !!active;

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

int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			  int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_squash_gid);
}

int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		       int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%d\n", nodemap->nm_flags.nmf_trusted);
}

int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		     int *eof, void *data)
{
	struct nodemap *nodemap;

	nodemap = (struct nodemap *)data;

	return sprintf(page, "%u\n", nodemap->nm_flags.nmf_admin);
}

int nodemap_procfs_init()
{
	int rc = 0;
	ENTRY;

	proc_lustre_nodemap_root = lprocfs_register(LUSTRE_NODEMAP_NAME,
						    proc_lustre_root,
						    lprocfs_nodemap_module_vars,
						    NULL);

	return rc;
}

void lprocfs_nodemap_init_vars(struct lprocfs_static_vars *lvars)
{
	lvars->module_vars = lprocfs_nodemap_module_vars;
	lvars->obd_vars = lprocfs_nodemap_vars;
}

void lprocfs_nodemap_register(char *nodemap_name,
			      int def_nodemap,
			      struct nodemap *nodemap)
{
	struct proc_dir_entry *nodemap_proc_entry;

	if (def_nodemap)
		nodemap_proc_entry =
			lprocfs_register(nodemap_name,
					 proc_lustre_nodemap_root,
					 lprocfs_default_nodemap_vars,
					 nodemap);
	else
		nodemap_proc_entry = lprocfs_register(nodemap_name,
						      proc_lustre_nodemap_root,
						      lprocfs_nodemap_vars,
						      nodemap);

	nodemap->nm_proc_entry = nodemap_proc_entry;
}

#ifdef NODEMAP_PROC_DEBUG
int nodemap_wr_squash_uid(struct file *file, const char __user *buffer,
			  unsigned long count, void *data)
{
	char squash[NODEMAP_LPROC_ID_LEN + 1];
	struct nodemap *nodemap;
	uid_t squash_uid;

	if (count == 0)
		return count;

	squash[NODEMAP_LPROC_ID_LEN] = '\0';

	if (copy_from_user(squash, buffer, NODEMAP_LPROC_ID_LEN))
		return -EFAULT;

	nodemap = (struct nodemap *)data;
	squash_uid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_uid = squash_uid;

	return count;
}

int nodemap_wr_squash_gid(struct file *file, const char __user *buffer,
			  unsigned long count, void *data)
{
	char squash[NODEMAP_LPROC_ID_LEN + 1];
	struct nodemap *nodemap;
	gid_t squash_gid;

	if (count == 0)
		return count;

	squash[NODEMAP_LPROC_ID_LEN] = '\0';

	if (copy_from_user(squash, buffer, NODEMAP_LPROC_ID_LEN))
		return -EFAULT;

	nodemap = (struct nodemap *)data;
	squash_gid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_gid = squash_gid;

	return count;
}

int nodemap_wr_trusted(struct file *file, const char __user *buffer,
		       unsigned long count, void *data)
{
	char trusted[NODEMAP_LPROC_FLAG_LEN + 1];
	struct nodemap *nodemap;
	unsigned int trusted_flag;

	if (count == 0)
		return count;

	trusted[NODEMAP_LPROC_FLAG_LEN] = '\0';

	if (copy_from_user(trusted, buffer, NODEMAP_LPROC_FLAG_LEN))
		return -EFAULT;

	nodemap = (struct nodemap *)data;
	trusted_flag = simple_strtoul(trusted, NULL, 10);

	nodemap->nm_flags.nmf_trusted = !!trusted_flag;

	return count;
}

int nodemap_wr_admin(struct file *file, const char __user *buffer,
		     unsigned long count, void *data)
{
	char admin[NODEMAP_LPROC_FLAG_LEN + 1];
	struct nodemap *nodemap;
	unsigned int admin_flag;

	if (count == 0)
		return count;

	admin[NODEMAP_LPROC_FLAG_LEN] = '\0';

	if (copy_from_user(admin, buffer, NODEMAP_LPROC_FLAG_LEN))
		return -EFAULT;

	nodemap = (struct nodemap *)data;
	admin_flag = simple_strtoul(admin, NULL, 10);

	nodemap->nm_flags.nmf_admin = !!admin_flag;

	return count;
}

int nodemap_proc_add_nodemap(struct file *file, const char __user *buffer,
			     unsigned long count, void *data)
{
	nodemap_name[NODEMAP_LUSTRE_NAME_LENGTH + 1];

	if (copy_from_user(nodemap_name, buffer, LUSTRE_NODEMAP_NAME_LENGTH))
		return -EFAULT;

	/* Check syntax for nodemap names here */

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	return nodemap_add(nodemap_name) ?: count;
}

int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data)
{
	char nodemap_name[LUSTRE_NODEMAP_NAME_LENGTH + 1];

	if (copy_from_user(nodemap_name, buffer, LUSTRE_NODEMAP_NAME_LENGTH))
		return -EFAULT;

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	return nodemap_del(nodemap_name) ?: count;
}
#endif /* NODEMAP_PROC_DEBUG */
