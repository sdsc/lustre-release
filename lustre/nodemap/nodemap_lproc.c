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

static struct lprocfs_vars lprocfs_nodemap_module_vars[] = {
	{
		.name		= "active",
		.read_fptr	= nodemap_rd_active,
		.write_fptr	= nodemap_wr_active,
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

#ifdef NODEMAP_PROC_DEBUG
static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
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
	{
		NULL
	}
};

static struct lprocfs_vars lprocfs_default_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
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
	{
		NULL
	}
};
#else
static struct lprocfs_vars lprocfs_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
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
	{
		NULL
	}
};

static struct lprocfs_vars lprocfs_default_nodemap_vars[] = {
	{
		.name		= "id",
		.read_fptr	= nodemap_rd_id,
	},
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
	{
		NULL
	}
};
#endif /* NODEMAP_PROC_DEBUG */

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		      int *eof, void *data)
{
	return snprintf(page, count, "%u\n", nodemap_idmap_active);
}

int nodemap_wr_active(struct file *file, const char __user *buffer,
		      unsigned long count, void *data)
{
	char	active_string[NODEMAP_LPROC_FLAG_LEN + 1];
	__u32	active;
	int	rc = count;

	if (count == 0)
		goto out;

	if (count > NODEMAP_LPROC_FLAG_LEN)
		GOTO(out, rc = -EINVAL);

	if (copy_from_user(active_string, buffer, count))
		GOTO(out, rc = -EFAULT);

	active_string[count] = '\0';

	active = simple_strtoul(active_string, NULL, 10);

	nodemap_idmap_active = !!active;
out:
	return rc;
}

int nodemap_rd_id(char *page, char **start, off_t off, int count,
		  int *eof, void *data)
{
	struct lu_nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_id);
}

int nodemap_rd_squash_uid(char *page, char **start, off_t off, int count,
			  int *eof, void *data)
{
	struct lu_nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_squash_uid);
}

int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			  int *eof, void *data)
{
	struct lu_nodemap *nodemap = data;

	return snprintf(page, count, "%u\n", nodemap->nm_squash_gid);
}

int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		       int *eof, void *data)
{
	struct lu_nodemap *nodemap = data;

	return snprintf(page, count, "%d\n",
			(int)nodemap->nmf_trust_client_ids);
}

int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		     int *eof, void *data)
{
	struct lu_nodemap *nodemap = data;

	return snprintf(page, count, "%d\n",
			(int)nodemap->nmf_allow_root_access);
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
		CERROR("cannot create 'nodemap' directory: rc = %d\n",
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

/**
 * lprocfs_nodemap_register() - register the proc directory for a nodemap
 * @name:		name of nodemap
 * @is_default:		1 if default nodemap
 * nodemap:		lu_nodemap structure assocaiated with proc entry
 */

int lprocfs_nodemap_register(const char *name,
			     bool is_default,
			     struct lu_nodemap *nodemap)
{
	struct proc_dir_entry	*nodemap_proc_entry;
	int			rc = 0;

	if (is_default)
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
		CERROR("cannot create 'nodemap/%s': "
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
	char			squash[NODEMAP_LPROC_ID_LEN + 1];
	struct lu_nodemap	*nodemap = data;
	uid_t			squash_uid;
	int			rc = count;

	if (count == 0)
		GOTO(out);

	if (count > NODEMAP_LPROC_FLAG_LEN)
		GOTO(out, rc = -EINVAL);

	if (copy_from_user(squash, buffer, count))
		GOTO(out, rc = -EFAULT);

	squash[count] = '\0';

	squash_uid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_uid = squash_uid;

out:
	return rc;
}

int nodemap_wr_squash_gid(struct file *file, const char __user *buffer,
			  unsigned long count, void *data)
{
	char			squash[NODEMAP_LPROC_ID_LEN + 1];
	struct lu_nodemap	*nodemap = data;
	gid_t			squash_gid;
	int			rc = count;

	if (count == 0)
		GOTO(out);

	if (count > NODEMAP_LPROC_FLAG_LEN)
		GOTO(out, rc = -EINVAL);

	if (copy_from_user(squash, buffer, count))
		GOTO(out, rc = -EFAULT);

	squash[count] = '\0';

	squash_gid = simple_strtoul(squash, NULL, 10);

	nodemap->nm_squash_gid = squash_gid;
out:
	return rc;
}

int nodemap_wr_trusted(struct file *file, const char __user *buffer,
		       unsigned long count, void *data)
{
	char			trusted[NODEMAP_LPROC_FLAG_LEN + 1];
	struct lu_nodemap	*nodemap = data;
	unsigned int		trusted_flag;
	int			rc = count;

	if (count == 0)
		GOTO(out);

	if (count > NODEMAP_LPROC_FLAG_LEN)
		GOTO(out, rc = -EINVAL);

	if (copy_from_user(trusted, buffer, count))
		GOTO(out, rc = -EFAULT);

	trusted[count] = '\0';

	trusted_flag = simple_strtoul(trusted, NULL, 10);

	nodemap->nmf_trust_client_ids = trusted_flag;

out:
	return rc;
}

int nodemap_wr_admin(struct file *file, const char __user *buffer,
		     unsigned long count, void *data)
{
	char			admin[NODEMAP_LPROC_FLAG_LEN + 1];
	struct lu_nodemap	*nodemap = data;
	unsigned int		admin_flag;
	int			rc = count;

	if (count == 0)
		GOTO(out);

	if (count > NODEMAP_LPROC_FLAG_LEN)
		GOTO(out, rc = -EINVAL);

	if (copy_from_user(admin, buffer, count))
		GOTO(out, rc = -EFAULT);

	admin[count] = '\0';

	admin_flag = simple_strtoul(admin, NULL, 10);

	nodemap->nmf_allow_root_access = admin_flag;

out:
	return rc;
}

int nodemap_proc_add_nodemap(struct file *file, const char __user *buffer,
			     unsigned long count, void *data)
{
	char	*cpybuf = NULL;
	char	*buf = NULL
	char	*name;
	int	rc = count;

	if (count == 0)
		GOTO(out);

	OBD_ALLOC(buf, LUSTRE_NODEMAP_NAME_LENGTH);

	if (copy_from_user(buf, buffer, count))
		GOTO(out, rc = -EFAULT);

	if (sizeof(buf) == 0)
		GOTO(out, rc = -ENOMEM);

	cpybuf = buf;
	name = strsep(&cpybuf, " ");
	if (name == NULL)
		GOTO(free_buf, rc = -EINVAL);

	rc = nodemap_add(name);
free_buf:
	OBD_FREE(buf, LUSTRE_NODEMAP_NAME_LENGTH);
out:
	return rc;
}

int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data)
{
	char	*cpybuf = NULL;
	char	*buf = NULL
	char	*name;
	int	rc = count;

	if (count == 0)
		goto(out);

	OBD_ALLOC(buf, LUSTRE_NODEMAP_NAME_LENGTH);

	if (copy_from_user(buf, buffer, count))
		goto(out, rc = -EFAULT);

	cpybuf = buf;
	name = strsep(&cpybuf, " ");
	if (name == NULL)
		GOTO(free_buf, rc = -EINVAL);

	rc = nodemap_del(name);
free_buf:
	OBD_FREE(buf, LUSTRE_NODEMAP_NAME_LENGTH);
out:
	return rc;

}
#endif /* NODEMAP_PROC_DEBUG */
