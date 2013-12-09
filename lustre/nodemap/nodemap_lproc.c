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

#define NODEMAP_LPROC_ID_LEN 16
#define NODEMAP_LPROC_FLAG_LEN 2

#include <lprocfs_status.h>
#include <lustre_net.h>
#include "nodemap_internal.h"

static int nodemap_ranges_open(struct inode *inode, struct file *file);
static int nodemap_ranges_show(struct seq_file *file, void *data);

static lnet_nid_t nodemap_test_nid;

const struct file_operations nodemap_range_operations = {
	.open		= nodemap_ranges_open,
	.read		= seq_read,
	.llseek		= seq_lseek,
	.release	= single_release,
};

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{
		.name		= "active",
		.read_fptr	= nodemap_rd_active,
		.write_fptr	= nodemap_wr_active,
	},
	{
		.name		= "test_nid",
		.read_fptr	= nodemap_rd_nid_test,
		.write_fptr	= nodemap_wr_nid_test,
	},
#ifdef NODEMAP_PROC_DEBUG
	{
		.name		= "add_nodemap",
		.write_fptr	= nodemap_proc_add_nodemap,
	},
	{
		.name		= "remove_nodemap",
		.write_fptr	= nodemap_proc_del_nodemap,
	},
#endif /* NODEMAP_PROC_DEBUG */
	{
		NULL
	}
};

static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
	{
		.name		= "ranges",
		.fops		= &nodemap_range_operations,
	},
#ifdef NODEMAP_PROC_DEBUG
	{
		.name		= "trusted_nodemap",
		.read_fptr	= nodemap_rd_trusted,
		.write_fptr	= nodemap_wr_trusted,
	},
	{
		.name		= "admin_nodemap",
		.read_fptr	= nodemap_rd_admin,
		.write_fptr	= nodemap_wr_admin,
	},
	{
		.name		= "squash_uid",
		.read_fptr	= nodemap_rd_squash_uid,
		.write_fptr	= nodemap_wr_squash_uid,
	},
	{
		.name		= "squash_gid",
		.read_fptr	= nodemap_rd_squash_gid,
		.write_fptr	= nodemap_wr_squash_gid,
	},
#else
	{
		.name		= "trusted_nodemap",
		.read_fptr	= nodemap_rd_trusted,
	},
	{
		.name		= "admin_nodemap",
		.read_fptr	= nodemap_rd_admin,
	},
	{
		.name		= "squash_uid",
		.read_fptr	= nodemap_rd_squash_uid,
	},
	{
		.name		= "squash_gid",
		.read_fptr	= nodemap_rd_squash_gid,
	},
#endif /* NODEMAP_PROC_DEBUG */
	{
		NULL
	}
};

static struct lprocfs_vars lprocfs_default_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
#ifdef NODEMAP_PROC_DEBUG
	{
		.name		= "trusted_nodemap",
		.read_fptr	= nodemap_rd_trusted,
		.write_fptr	= nodemap_wr_trusted,
	},
	{
		.name		= "admin_nodemap",
		.read_fptr	= nodemap_rd_admin,
		.write_fptr	= nodemap_wr_admin,
	},
	{
		.name		= "squash_uid",
		.read_fptr	= nodemap_rd_squash_uid,
		.write_fptr	= nodemap_wr_squash_uid,
	},
	{
		.name		= "squash_gid",
		.read_fptr	= nodemap_rd_squash_gid,
		.write_fptr	= nodemap_wr_squash_gid,
	},
#else
	{
		.name		= "trusted_nodemap",
		.read_fptr	= nodemap_rd_trusted,
	},
	{
		.name		= "admin_nodemap",
		.read_fptr	= nodemap_rd_admin,
	},
	{
		.name		= "squash_uid",
		.read_fptr	= nodemap_rd_squash_uid,
	},
	{
		.name		= "squash_gid",
		.read_fptr	= nodemap_rd_squash_gid,
	},
#endif /* NODEMAP_PROC_DEBUG */
	{
		NULL
	}
};

int nodemap_rd_nid_test(char *page, char **start, off_t off, int count,
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
		len = snprintf(page, count, "%s:0\n", nodemap->nm_name);
		return len;
	}

	len = snprintf(page, count, "%s:%u\n", nodemap->nm_name,
		       range->rn_id);

	return len;
}

int nodemap_wr_nid_test(struct file *file, const char __user *buffer,
			unsigned long count, void *data)
{
	char string[NODEMAP_NIDSTRING_LENGTH];

	if (count == 0)
		return count;
	if (count >= NODEMAP_NIDSTRING_LENGTH)
		goto out;

	if (copy_from_user(string, buffer, count))
		goto out;

	if (count > 0)
		string[count - 1] = 0;
	else
		return count;

	nodemap_test_nid = libcfs_str2nid(string);

	return count;
out:
	return -EFAULT;
}


static int nodemap_ranges_open(struct inode *inode, struct file *file)
{
	struct proc_dir_entry *dir;
	struct nodemap *nodemap;

	dir = PDE(inode);
	nodemap = (struct nodemap *) dir->data;

	return single_open(file, nodemap_ranges_show, nodemap);
}

/* Prints list of NID ranegs in a nodemap in YAML format
 *
 * id: 1 { start_nid: 192.168.1.1@tcp0, end_nid: 192.168.1.100@tcp0 }
 * id: 2 { start_nid: 192.168.2.1@tcp0, end_nid: 192.168.2.253@tcp0 }
 * id: 3 { start_nid: 192.168.10.1@tcp0, end_nid: 192.168.10.100@tcp0 }
 *
 */

static int nodemap_ranges_show(struct seq_file *file, void *data)
{
	struct nodemap *nodemap;
	struct range_node *range;

	nodemap = (struct nodemap *) file->private;

	list_for_each_entry(range, &(nodemap->nm_ranges), rn_list) {
		seq_printf(file, "id %u: { start_nid: %s, end_nid: %s }\n",
			   range->rn_id, libcfs_nid2str(range->rn_start_nid),
			   libcfs_nid2str(range->rn_end_nid));
	}

	return 0;
}

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		      int *eof, void *data)
{
	return snprintf(page, count, "%u\n", nodemap_idmap_active);
}

int nodemap_wr_active(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char active_string[NODEMAP_LPROC_FLAG_LEN + 1];
	__u32 active;

	if (count == 0)
		return count;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		return -EINVAL;

	if (copy_from_user(active_string, buffer, count))
		return -EFAULT;

	active_string[count] = '\0';

	active = simple_strtoul(active_string, NULL, 10);

	nodemap_idmap_active = !!active;

	return count;
}

int nodemap_rd_id(char *page, char **start, off_t off, int count,
		  int *eof, void *data)
{
	struct nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_id);
}

int nodemap_rd_squash_uid(char *page, char **start, off_t off, int count,
			  int *eof, void *data)
{
	struct nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_squash_uid);
}

int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			  int *eof, void *data)
{
	struct nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_squash_gid);
}

int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		       int *eof, void *data)
{
	struct nodemap *nodemap = data;

	return snprintf(page, count, "%d\n",
			nodemap->nmf_trust_client_ids);
}

int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		     int *eof, void *data)
{
	struct nodemap *nodemap = data;

	return snprintf(page, count, "%u\n",
			nodemap->nmf_allow_root_access);
}

int nodemap_procfs_init(void)
{
	int rc = 0;

	proc_lustre_nodemap_root = lprocfs_register(LUSTRE_NODEMAP_NAME,
						    proc_lustre_root,
						    lprocfs_nodemap_module_vars,
						    NULL);

	if (IS_ERR(proc_lustre_nodemap_root)) {
		rc = PTR_ERR(proc_lustre_nodemap_root);
		CERROR("cannot create 'nodemap' directory, rc = %d\n",
		       rc);
		proc_lustre_nodemap_root = NULL;
	}

	return rc;
}

void lprocfs_nodemap_init_vars(struct lprocfs_static_vars *lvars)
{
	lvars->module_vars = lprocfs_nodemap_module_vars;
	lvars->obd_vars = lprocfs_nodemap_vars;
}

int lprocfs_nodemap_register(const char *name,
			     bool is_default_nodemap,
			     struct nodemap *nodemap)
{
	int rc = 0;
	struct proc_dir_entry *nodemap_proc_entry;

	if (is_default_nodemap)
		nodemap_proc_entry =
			lprocfs_register(name,
					 proc_lustre_nodemap_root,
					 lprocfs_default_nodemap_vars,
					 nodemap);
	else
		nodemap_proc_entry = lprocfs_register(name,
						      proc_lustre_nodemap_root,
						      lprocfs_nodemap_vars,
						      nodemap);

	if (IS_ERR(nodemap_proc_entry)) {
		rc = PTR_ERR(nodemap_proc_entry);
		CERROR("cannot create 'nodemap/%s'"
		       "rc = %d\n", name, rc);
		nodemap_proc_entry = NULL;
	}

	nodemap->nm_proc_entry = nodemap_proc_entry;

	return rc;
}

#ifdef NODEMAP_PROC_DEBUG
int nodemap_wr_squash_uid(struct file *file, const char __user *buffer,
			  unsigned long count, void *data)
{
	char squash[NODEMAP_LPROC_ID_LEN + 1];
	struct nodemap *nodemap = data;
	uid_t squash_uid;

	if (count == 0)
		return count;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		return -EINVAL;

	if (copy_from_user(squash, buffer, count))
		return -EFAULT;

	squash[count] = '\0';

	squash_uid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_uid = squash_uid;

	return count;
}

int nodemap_wr_squash_gid(struct file *file, const char __user *buffer,
			  unsigned long count, void *data)
{
	char squash[NODEMAP_LPROC_ID_LEN + 1];
	struct nodemap *nodemap = data;
	gid_t squash_gid;

	if (count == 0)
		return count;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		return -EINVAL;

	if (copy_from_user(squash, buffer, count))
		return -EFAULT;

	squash[count] = '\0';

	squash_gid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_gid = squash_gid;

	return count;
}

int nodemap_wr_trusted(struct file *file, const char __user *buffer,
		       unsigned long count, void *data)
{
	char trusted[NODEMAP_LPROC_FLAG_LEN + 1];
	struct nodemap *nodemap = data;
	unsigned int trusted_flag;

	if (count == 0)
		return count;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		return -EINVAL;

	if (copy_from_user(trusted, buffer, count))
		return -EFAULT;

	trusted[count] = '\0';

	trusted_flag = simple_strtoul(trusted, NULL, 10);

	nodemap->nmf_trust_client_ids = trusted_flag;

	return count;
}

int nodemap_wr_admin(struct file *file, const char __user *buffer,
		     unsigned long count, void *data)
{
	char admin[NODEMAP_LPROC_FLAG_LEN + 1];
	struct nodemap *nodemap = data;
	unsigned int admin_flag;

	if (count == 0)
		return count;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		return -EINVAL;

	if (copy_from_user(admin, buffer, count))
		return -EFAULT;

	admin[count] = '\0';

	admin_flag = simple_strtoul(admin, NULL, 10);

	nodemap->nmf_allow_root_access = admin_flag;

	return count;
}

int nodemap_proc_add_nodemap(struct file *file, const char __user *buffer,
			     unsigned long count, void *data)
{
	char *name, *buf = NULL;
	int rc = count;

	if (count == 0)
		return count;

	if (copy_from_user(buf, buffer, count))
		return -EFAULT;

	OBD_ALLOC(buf, LUSTRE_NODEMAP_NAME_LENGTH);

	if (buf == NULL)
		return -EINVAL;

	name = strsep(&buf, " ");

	if (name == NULL) {
		rc = -EINVAL;
		goto out;
	}

	rc = nodemap_add(name);
out:
	OBD_FREE(buf, LUSTRE_NODEMAP_NAME_LENGTH);
	return rc;
}

int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data)
{
	char *name, *buf = NULL;
	int rc = count;

	if (count == 0)
		return count;

	if (copy_from_user(buf, buffer, count))
		return -EFAULT;

	OBD_ALLOC(buf, LUSTRE_NODEMAP_NAME_LENGTH);

	if (buf == NULL)
		return -EINVAL;

	name = strsep(&buf, " ");

	if (name == NULL) {
		rc = -EINVAL;
		goto out;
	}

	rc = nodemap_del(name);
out:
	OBD_FREE(buf, LUSTRE_NODEMAP_NAME_LENGTH);
	return rc;

}
#endif /* NODEMAP_PROC_DEBUG */
