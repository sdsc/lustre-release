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
#include <asm/statfs.h>

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>

#include <lprocfs_status.h>

#include "nodemap_internal.h"

static int ranges_open(struct inode *inode, struct file *file);
static int ranges_show(struct seq_file *file, void *data);

static lnet_nid_t test_nid;

const struct file_operations proc_range_operations = {
	.open		= ranges_open,
	.read		= seq_read,
	.llseek         = seq_lseek,
	.release	= single_release,
};

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{ "active", nodemap_rd_active, nodemap_wr_active, 0 },
	{ "add_nodemap", 0, nodemap_proc_add_nodemap, 0 },
	{ "remove_nodemap", 0, nodemap_proc_del_nodemap, 0 },
	{ "test_nid", rd_nid_test, wr_nid_test, 0 },
	{ 0 }
};

static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "add_range", 0, nodemap_proc_add_range, 0 },
	{ "remove_range", 0, nodemap_proc_del_range, 0 },
	{ "ranges", 0, 0, 0, &proc_range_operations },
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

int rd_nid_test(char *page, char **start, off_t off, int count,
		int *eof, void *data)
{
	int len;
	struct nodemap *nodemap;
	struct range_node *range;

	range = range_search(&test_nid);

	if (range == NULL)
		nodemap = default_nodemap;
	else
		nodemap = range->rn_nodemap;

	if (nodemap == NULL)
		return 0;

	if (nodemap->nm_id == 0) {
		len = sprintf(page, "%s:0\n", nodemap->nm_name);
		return len;
	}

	len = sprintf(page, "%s:%u\n", nodemap->nm_name, range->rn_id);

	return len;
}

int wr_nid_test(struct file *file, const char __user *buffer,
		unsigned long count, void *data)
{
	char string[count + 1];

	if (copy_from_user(string, buffer, count))
		return -EFAULT;

	if (count > 0)
		string[count - 1] = 0;
	else
		return count;

	test_nid = libcfs_str2nid(string);

	return count;
}

static int ranges_open(struct inode *inode, struct file *file)
{
	cfs_proc_dir_entry_t *dir;
	struct nodemap *nodemap;

	dir = PDE(inode);
	nodemap = (struct nodemap *) dir->data;

	return single_open(file, ranges_show, nodemap);
}

static int ranges_show(struct seq_file *file, void *data)
{
	struct nodemap *nodemap;
	struct range_node *range;

	nodemap = (struct nodemap *) file->private;

	list_for_each_entry(range, &(nodemap->nm_ranges), rn_list) {
		seq_printf(file, "%u %s : %s\n", range->rn_id,
			   libcfs_nid2str(range->rn_start_nid),
			   libcfs_nid2str(range->rn_end_nid));
	}

	return 0;
}

int nodemap_proc_add_range(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char range_str[count + 1];
	struct nodemap *nodemap;
	int rc;

	if (copy_from_user(range_str, buffer, count))
		return -EFAULT;

	/* Check syntax for nodemap names here */

	if (count > 2) {
		range_str[count] = 0;
		range_str[count - 1] = 0;
	} else
		return count;

	nodemap = (struct nodemap *) data;

	rc = nodemap_add_range(nodemap->nm_name, range_str);

	if (rc != 0) {
		CERROR("nodemap range insert failed for %s: rc = %d",
		       nodemap->nm_name, rc);
	}

	return count;
}

int nodemap_proc_del_range(struct file *file, const char __user *buffer,
			 unsigned long count, void *data)
{
	char range_str[count + 1];
	struct nodemap *nodemap;
	int rc;

	if (copy_from_user(range_str, buffer, count))
		return -EFAULT;

	/* Check syntax for nodemap names here */

	if (count > 2) {
		range_str[count] = 0;
		range_str[count - 1] = 0;
	} else {
		return count;
	}

	nodemap = (struct nodemap *) data;

	rc = nodemap_del_range(nodemap->nm_name, range_str);

	if (rc != 0) {
		CERROR("nodemap range delete failed for %s: rc = %d",
		       nodemap->nm_name, rc);
	}

	return count;
}

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

int nodemap_proc_add_nodemap(struct file *file, const char __user *buffer,
			     unsigned long count, void *data)
{
	char nodemap_name[count + 1];

	if (copy_from_user(nodemap_name, buffer, count))
		return -EFAULT;

	/* Check syntax for nodemap names here */

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	nodemap_add(nodemap_name);

	return count;
}

int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data)
{
	char nodemap_name[count + 1];

	if (copy_from_user(nodemap_name, buffer, count))
		return -EFAULT;

	if (count > 2) {
		nodemap_name[count] = 0;
		nodemap_name[count - 1] = 0;
	} else {
		return count;
	}

	nodemap_del(nodemap_name);

	return count;
}
