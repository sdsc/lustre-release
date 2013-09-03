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
 *
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
#ifdef __KERNEL__

#define DEBUG_SUBSYSTEM S_FILTER

#include <linux/version.h>
#include <linux/fs.h>
#include <asm/unistd.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <linux/quotaops.h>
#include <linux/version.h>
#include <libcfs/libcfs.h>
#include <obd.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/lustre_compat25.h>
#include <lvfs.h>

#include <obd.h>
#include <lustre_lib.h>

/* refine later and change to seqlock or simlar from libcfs */
/* Debugging check only needed during development */
#ifdef OBD_CTXT_DEBUG
# define ASSERT_CTXT_MAGIC(magic) LASSERT((magic) == OBD_RUN_CTXT_MAGIC)
# define ASSERT_NOT_KERNEL_CTXT(msg) LASSERTF(!segment_eq(get_fs(), get_ds()),\
					      msg)
# define ASSERT_KERNEL_CTXT(msg) LASSERTF(segment_eq(get_fs(), get_ds()), msg)
#else
# define ASSERT_CTXT_MAGIC(magic) do {} while(0)
# define ASSERT_NOT_KERNEL_CTXT(msg) do {} while(0)
# define ASSERT_KERNEL_CTXT(msg) do {} while(0)
#endif

static void push_group_info(struct lvfs_run_ctxt *save,
			    struct group_info *ginfo)
{
	if (!ginfo) {
		save->ngroups = current_ngroups;
		current_ngroups = 0;
	} else {
		struct cred *cred;
		task_lock(current);
		save->group_info = current_cred()->group_info;
		if ((cred = prepare_creds())) {
			cred->group_info = ginfo;
			commit_creds(cred);
		}
		task_unlock(current);
	}
}

static void pop_group_info(struct lvfs_run_ctxt *save,
			   struct group_info *ginfo)
{
	if (!ginfo) {
		current_ngroups = save->ngroups;
	} else {
		struct cred *cred;
		task_lock(current);
		if ((cred = prepare_creds())) {
			cred->group_info = save->group_info;
			commit_creds(cred);
		}
		task_unlock(current);
	}
}

/* push / pop to root of obd store */
void push_ctxt(struct lvfs_run_ctxt *save, struct lvfs_run_ctxt *new_ctx,
	       struct lvfs_ucred *uc)
{
	/* if there is underlaying dt_device then push_ctxt is not needed */
	if (new_ctx->dt != NULL)
		return;

	//ASSERT_NOT_KERNEL_CTXT("already in kernel context!\n");
	ASSERT_CTXT_MAGIC(new_ctx->magic);
	OBD_SET_CTXT_MAGIC(save);

	save->fs = get_fs();
	LASSERT(d_refcount(current->fs->pwd.dentry));
	LASSERT(d_refcount(new_ctx->pwd));
	save->pwd = dget(current->fs->pwd.dentry);
	save->pwdmnt = mntget(current->fs->pwd.mnt);
	save->luc.luc_umask = current_umask();
	save->ngroups = current_cred()->group_info->ngroups;

	LASSERT(save->pwd);
	LASSERT(save->pwdmnt);
	LASSERT(new_ctx->pwd);
	LASSERT(new_ctx->pwdmnt);

	if (uc) {
		struct cred *cred;
		save->luc.luc_uid = current_uid();
		save->luc.luc_gid = current_gid();
		save->luc.luc_fsuid = current_fsuid();
		save->luc.luc_fsgid = current_fsgid();
		save->luc.luc_cap = current_cap();

		if ((cred = prepare_creds())) {
			cred->uid = uc->luc_uid;
			cred->gid = uc->luc_gid;
			cred->fsuid = uc->luc_fsuid;
			cred->fsgid = uc->luc_fsgid;
			cred->cap_effective = uc->luc_cap;
			commit_creds(cred);
		}

		push_group_info(save,
				uc->luc_ginfo ?:
				uc->luc_identity ? uc->luc_identity->mi_ginfo :
						   NULL);
	}
	current->fs->umask = 0; /* umask already applied on client */
	set_fs(new_ctx->fs);
	ll_set_fs_pwd(current->fs, new_ctx->pwdmnt, new_ctx->pwd);
}
EXPORT_SYMBOL(push_ctxt);

void pop_ctxt(struct lvfs_run_ctxt *saved, struct lvfs_run_ctxt *new_ctx,
	      struct lvfs_ucred *uc)
{
	/* if there is underlaying dt_device then pop_ctxt is not needed */
	if (new_ctx->dt != NULL)
		return;

	ASSERT_CTXT_MAGIC(saved->magic);
	ASSERT_KERNEL_CTXT("popping non-kernel context!\n");

	LASSERTF(current->fs->pwd.dentry == new_ctx->pwd, "%p != %p\n",
		 current->fs->pwd.dentry, new_ctx->pwd);
	LASSERTF(current->fs->pwd.mnt == new_ctx->pwdmnt, "%p != %p\n",
		 current->fs->pwd.mnt, new_ctx->pwdmnt);

	set_fs(saved->fs);
	ll_set_fs_pwd(current->fs, saved->pwdmnt, saved->pwd);

	dput(saved->pwd);
	mntput(saved->pwdmnt);
	current->fs->umask = saved->luc.luc_umask;
	if (uc) {
		struct cred *cred;
		if ((cred = prepare_creds())) {
			cred->uid = saved->luc.luc_uid;
			cred->gid = saved->luc.luc_gid;
			cred->fsuid = saved->luc.luc_fsuid;
			cred->fsgid = saved->luc.luc_fsgid;
			cred->cap_effective = saved->luc.luc_cap;
			commit_creds(cred);
		}

		pop_group_info(saved,
			       uc->luc_ginfo ?:
			       uc->luc_identity ? uc->luc_identity->mi_ginfo :
						  NULL);
	}
}
EXPORT_SYMBOL(pop_ctxt);

/* utility to rename a file */
int lustre_rename(struct dentry *dir, struct vfsmount *mnt,
		  char *oldname, char *newname)
{
	struct dentry *dchild_old, *dchild_new;
	int err = 0;
	ENTRY;

	ASSERT_KERNEL_CTXT("kernel doing rename outside kernel context\n");
	CDEBUG(D_INODE, "renaming file %.*s to %.*s\n",
	       (int)strlen(oldname), oldname, (int)strlen(newname), newname);

	dchild_old = ll_lookup_one_len(oldname, dir, strlen(oldname));
	if (IS_ERR(dchild_old))
		RETURN(PTR_ERR(dchild_old));

	if (!dchild_old->d_inode)
		GOTO(put_old, err = -ENOENT);

	dchild_new = ll_lookup_one_len(newname, dir, strlen(newname));
	if (IS_ERR(dchild_new))
		GOTO(put_old, err = PTR_ERR(dchild_new));

	err = vfs_rename(dir->d_inode, dchild_old, dir->d_inode, dchild_new);

	dput(dchild_new);
put_old:
	dput(dchild_old);
	RETURN(err);
}
EXPORT_SYMBOL(lustre_rename);
#endif /* __KERNEL__ */
