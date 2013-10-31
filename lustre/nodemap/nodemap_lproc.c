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
static int uidmap_open(struct inode *inode, struct file *file);
static int uidmap_show(struct seq_file *file, void *data);
static int gidmap_open(struct inode *inode, struct file *file);
static int gidmap_show(struct seq_file *file, void *data);

static lnet_nid_t nodemap_test_nid;
static __u32 nodemap_test_uid;
static __u32 nodemap_test_gid;

const struct file_operations proc_range_operations = {
	.open		= ranges_open,
	.read		= seq_read,
	.llseek         = seq_lseek,
	.release	= single_release,
};

static struct file_operations proc_uidmap_operations = {
	.open           = uidmap_open,
	.read           = seq_read,
	.llseek         = seq_lseek,
	.release        = single_release,
};

static struct file_operations proc_gidmap_operations = {
	.open           = gidmap_open,
	.read           = seq_read,
	.llseek         = seq_lseek,
	.release        = single_release,
};

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{ "active", nodemap_rd_active, nodemap_wr_active, 0 },
	{ "test_nid", rd_nid_test, wr_nid_test, 0 },
	{ "test_uid_map", rd_uidmap_test, wr_uidmap_test, 0 },
	{ "test_gid_map", rd_gidmap_test, wr_gidmap_test, 0 },
	{ 0 }
};

static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{ "id", nodemap_rd_id, 0, 0 },
	{ "ranges", 0, 0, 0, &proc_range_operations },
	{ "uidmap", 0, 0, 0, &proc_uidmap_operations },
	{ "gidmap", 0, 0, 0, &proc_gidmap_operations },
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

int rd_gidmap_test(char *page, char **start, off_t off, int count,
		int *eof, void *data)
{
	struct nodemap *nodemap;
	__u32 local_gid;
	int len;

	nodemap = nodemap_classify_nid(nodemap_test_nid);

	local_gid = nodemap_map_id(nodemap,
				   NM_REMOTE_TO_LOCAL,
				   NM_GID,
				   nodemap_test_gid);

	len = sprintf(page, "%s:%u %u\n", nodemap->nm_name, nodemap_test_gid,
		      local_gid);

	return len;
}

int wr_gidmap_test(struct file *file, const char __user *buffer,
		unsigned long count, void *data)
{
	char string[count + 1];
	char *bp;
	char *nid_string = NULL;
	char *gid_string = NULL;

	if (copy_from_user(string, buffer, count))
		return -EFAULT;

	if (count > 0)
		string[count - 1] = 0;
	else
		return count;

	bp = string;
	nid_string = strsep(&bp, " ");
	gid_string = strsep(&bp, " ");

	if ((gid_string == NULL) || (nid_string == NULL))
		return -EINVAL;

	nodemap_test_nid = libcfs_str2nid(nid_string);
	nodemap_test_gid = simple_strtoul(gid_string, NULL, 10);

	return count;
}

int rd_uidmap_test(char *page, char **start, off_t off, int count,
		int *eof, void *data)
{
	struct nodemap *nodemap;
	__u32 local_uid;
	int len;

	nodemap = nodemap_classify_nid(nodemap_test_nid);

	local_uid = nodemap_map_id(nodemap,
				   NM_REMOTE_TO_LOCAL,
				   NM_UID,
				   nodemap_test_uid);

	len = sprintf(page, "%s:%u %u\n", nodemap->nm_name, nodemap_test_uid,
		      local_uid);

	return len;
}

int wr_uidmap_test(struct file *file, const char __user *buffer,
		unsigned long count, void *data)
{
	char string[count + 1];
	char *bp;
	char *nid_string = NULL;
	char *uid_string = NULL;

	if (copy_from_user(string, buffer, count))
		return -EFAULT;

	if (count > 0)
		string[count - 1] = 0;
	else
		return count;

	bp = string;
	nid_string = strsep(&bp, " ");
	uid_string = strsep(&bp, " ");

	if ((uid_string == NULL) || (nid_string == NULL))
		return -EINVAL;

	nodemap_test_nid = libcfs_str2nid(nid_string);
	nodemap_test_uid = simple_strtoul(uid_string, NULL, 10);

	return count;
}

int rd_nid_test(char *page, char **start, off_t off, int count,
		int *eof, void *data)
{
	int len;
	struct nodemap *nodemap;
	struct range_node *range;

	range = range_search(&nodemap_test_nid);

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

	nodemap_test_nid = libcfs_str2nid(string);

	return count;
}

static int ranges_open(struct inode *inode, struct file *file)
{
	struct proc_dir_entry *dir;
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

static int uidmap_open(struct inode *inode, struct file *file)
{
	struct proc_dir_entry *dir;
	struct nodemap *nodemap;

	dir = PDE(inode);
	nodemap = (struct nodemap *) dir->data;

	return single_open(file, uidmap_show, nodemap);
}

static int uidmap_show(struct seq_file *file, void *data)
{
	struct nodemap *nodemap;
	struct idmap_node *idmap = NULL;
	struct rb_node *node;

	nodemap = (struct nodemap *) file->private;

	for (node = rb_first(&(nodemap->nm_local_to_remote_uidmap)); node;
	     node = rb_next(node)) {
		idmap = rb_entry(node, struct idmap_node, id_local_to_remote);
		if (idmap != NULL)
			seq_printf(file, "%u : %u\n", idmap->id_local,
				   idmap->id_remote);
	}

	return 0;
}

static int gidmap_open(struct inode *inode, struct file *file)
{
	struct proc_dir_entry *dir;
	struct nodemap *nodemap;

	dir = PDE(inode);
	nodemap = (struct nodemap *) dir->data;

	return single_open(file, gidmap_show, nodemap);
}

static int gidmap_show(struct seq_file *file, void *data)
{
	struct nodemap *nodemap;
	struct idmap_node *idmap = NULL;
	struct rb_node *node;

	nodemap = (struct nodemap *) file->private;

	for (node = rb_first(&(nodemap->nm_local_to_remote_gidmap)); node;
	     node = rb_next(node)) {
		idmap = rb_entry(node, struct idmap_node, id_local_to_remote);
		seq_printf(file, "%u : %u\n", idmap->id_local,
			   idmap->id_remote);
	}

	return 0;
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
