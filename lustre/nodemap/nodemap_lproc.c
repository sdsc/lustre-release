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

static int ranges_open(struct inode *, struct file *);
static int ranges_show(struct seq_file *, void *);
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

	nodemap = nodemap_search_by_nid(&test_nid);

	if (nodemap == NULL)
		return 0;

	if (nodemap->nm_id == 0) {
		len = sprintf(page, "%s:0\n", nodemap->nm_name);
		return len;
	}

	range = range_search(&nodemap->nm_ranges, &test_nid);

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

static int ranges_show(struct seq_file *m, void *v)
{
	struct nodemap *nodemap;
	struct range_node *range = NULL;
	struct rb_node *node;

	nodemap = (struct nodemap *) m->private;

	for (node = rb_first(&(nodemap->nm_ranges)); node;
			     node = rb_next(node)) {
		range = rb_entry(node, struct range_node, rn_node);
		seq_printf(m, "%u %s : %s\n", range->rn_id,
				libcfs_nid2str(range->rn_start_nid),
				libcfs_nid2str(range->rn_end_nid));
	}

	return 0;
}

int nodemap_proc_add_range(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char range_str[count + 1];
	char *buf_p, *min_string, *max_string;
	struct nodemap *nodemap;
	lnet_nid_t min, max;
	struct range_node *range;
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

	buf_p = range_str;
	min_string = strsep(&buf_p, " ");
	max_string = strsep(&buf_p, " ");

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	if ((nodemap_check_nid(&min) != 0) ||
	    (nodemap_check_nid(&max) != 0))
		return -EINVAL;

	/* Do some range and network test here */

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		return -EINVAL;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		return -EINVAL;

	range = kmalloc(sizeof(struct range_node), GFP_KERNEL);

	if (range == NULL) {
		CERROR("Cannot allocate memory (%lu o)"
		      "for range_node", sizeof(struct range_node));
		return -ENOMEM;
	}

	range->rn_start_nid = min;
	range->rn_end_nid = max;

	rc = range_insert(&(nodemap->nm_ranges), range);

	if (rc != 0) {
		CERROR("nodemap range insert failed for %s: rc = %d",
		      nodemap->nm_name, rc);
		kfree(range);
	}

	return count;
}

int nodemap_proc_del_range(struct file *file, const char __user *buffer,
			 unsigned long count, void *data)
{
	char range_str[count + 1];
	char *buf_p, *min_string, *max_string;
	struct nodemap *nodemap;
	lnet_nid_t min, max;
	struct range_node *range;

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

	buf_p = range_str;
	min_string = strsep(&buf_p, " ");
	max_string = strsep(&buf_p, " ");

	min = libcfs_str2nid(min_string);
	max = libcfs_str2nid(max_string);

	/* Do some range and network test here */

	if (LNET_NIDNET(min) != LNET_NIDNET(max))
		return -EINVAL;

	if (LNET_NIDADDR(min) > LNET_NIDADDR(max))
		return -EINVAL;

	range = range_search(&(nodemap->nm_ranges), &min);

	if (range == NULL)
		return count;

	if ((range->rn_start_nid == min) && (range->rn_end_nid == max)) {
		rb_erase(&(range->rn_node), &(nodemap->nm_ranges));
		kfree(range);
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
