/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/lvfs/lvfs_linux.c
 *
 * Author: Andreas Dilger <adilger@clusterfs.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#define DEBUG_SUBSYSTEM S_FILTER

#include <linux/version.h>
#include <linux/fs.h>
#include <asm/unistd.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <linux/quotaops.h>
#include <linux/version.h>
#include <libcfs/kp30.h>
#include <lustre_fsfilt.h>
#include <obd.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/lustre_compat25.h>
#include <lvfs.h>
#include "lvfs_internal.h"

#include <obd.h>
#include <lustre_lib.h>
#include <lustre_quota.h>

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
                            struct upcall_cache_entry *uce)
{
        struct group_info *ginfo = uce ? uce->ue_group_info : NULL;

        if (!ginfo) {
                save->ngroups = current_ngroups;
                current_ngroups = 0;
        } else {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,4)
                struct cred *cred;
                task_lock(current);
                save->group_info = current_cred()->group_info;
                if ((cred = prepare_creds())) {
                        cred->group_info = ginfo;
                        commit_creds(cred);
                }
                task_unlock(current);
#else
                LASSERT(ginfo->ngroups <= NGROUPS);
                LASSERT(current->ngroups <= NGROUPS_SMALL);
                /* save old */
                save->group_info.ngroups = current->ngroups;
                if (current->ngroups)
                        memcpy(save->group_info.small_block, current->groups,
                               current->ngroups * sizeof(gid_t));
                /* push new */
                current->ngroups = ginfo->ngroups;
                if (ginfo->ngroups)
                        memcpy(current->groups, ginfo->small_block,
                               current->ngroups * sizeof(gid_t));
#endif
        }
}

static void pop_group_info(struct lvfs_run_ctxt *save,
                           struct upcall_cache_entry *uce)
{
        struct group_info *ginfo = uce ? uce->ue_group_info : NULL;

        if (!ginfo) {
                current_ngroups = save->ngroups;
        } else {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,4)
                struct cred *cred;
                task_lock(current);
                if ((cred = prepare_creds())) {
                        cred->group_info = save->group_info;
                        commit_creds(cred);
                }
                task_unlock(current);
#else
                current->ngroups = save->group_info.ngroups;
                if (current->ngroups)
                        memcpy(current->groups, save->group_info.small_block,
                               current->ngroups * sizeof(gid_t));
#endif
        }
}

/* push / pop to root of obd store */
void push_ctxt(struct lvfs_run_ctxt *save, struct lvfs_run_ctxt *new_ctx,
               struct lvfs_ucred *uc)
{
        //ASSERT_NOT_KERNEL_CTXT("already in kernel context!\n");
        ASSERT_CTXT_MAGIC(new_ctx->magic);
        OBD_SET_CTXT_MAGIC(save);

        /*
        CDEBUG(D_INFO,
               "= push %p->%p = cur fs %p pwd %p:d%d:i%d (%.*s), pwdmnt %p:%d\n",
               save, current, current->fs, current->fs->pwd,
               atomic_read(&current->fs->pwd->d_count),
               atomic_read(&current->fs->pwd->d_inode->i_count),
               current->fs->pwd->d_name.len, current->fs->pwd->d_name.name,
               current->fs->pwdmnt,
               atomic_read(&current->fs->pwdmnt->mnt_count));
        */

        save->fs = get_fs();
        LASSERT(atomic_read(&cfs_fs_pwd(current->fs)->d_count));
        LASSERT(atomic_read(&new_ctx->pwd->d_count));
        save->pwd = dget(cfs_fs_pwd(current->fs));
        save->pwdmnt = mntget(cfs_fs_mnt(current->fs));
        save->luc.luc_umask = current->fs->umask;

        LASSERT(save->pwd);
        LASSERT(save->pwdmnt);
        LASSERT(new_ctx->pwd);
        LASSERT(new_ctx->pwdmnt);

        if (uc) {
                struct cred *cred;
                save->luc.luc_fsuid = current_fsuid();
                save->luc.luc_fsgid = current_fsgid();
                save->luc.luc_cap = current_cap();

                if ((cred = prepare_creds())) {
                        cred->fsuid = uc->luc_fsuid;
                        cred->fsgid = uc->luc_fsgid;
                        cred->cap_effective = uc->luc_cap;
                        commit_creds(cred);
                }

                push_group_info(save, uc->luc_uce);
        }

        set_fs(new_ctx->fs);
        ll_set_fs_pwd(current->fs, new_ctx->pwdmnt, new_ctx->pwd);

        /*
        CDEBUG(D_INFO,
               "= push %p->%p = cur fs %p pwd %p:d%d:i%d (%.*s), pwdmnt %p:%d\n",
               new_ctx, current, current->fs, current->fs->pwd,
               atomic_read(&current->fs->pwd->d_count),
               atomic_read(&current->fs->pwd->d_inode->i_count),
               current->fs->pwd->d_name.len, current->fs->pwd->d_name.name,
               current->fs->pwdmnt,
               atomic_read(&current->fs->pwdmnt->mnt_count));
        */
}
EXPORT_SYMBOL(push_ctxt);

void pop_ctxt(struct lvfs_run_ctxt *saved, struct lvfs_run_ctxt *new_ctx,
              struct lvfs_ucred *uc)
{
        //printk("pc0");
        ASSERT_CTXT_MAGIC(saved->magic);
        //printk("pc1");
        ASSERT_KERNEL_CTXT("popping non-kernel context!\n");

        /*
        CDEBUG(D_INFO,
               " = pop  %p==%p = cur %p pwd %p:d%d:i%d (%.*s), pwdmnt %p:%d\n",
               new_ctx, current, current->fs, current->fs->pwd,
               atomic_read(&current->fs->pwd->d_count),
               atomic_read(&current->fs->pwd->d_inode->i_count),
               current->fs->pwd->d_name.len, current->fs->pwd->d_name.name,
               current->fs->pwdmnt,
               atomic_read(&current->fs->pwdmnt->mnt_count));
        */

        LASSERTF(cfs_fs_pwd(current->fs) == new_ctx->pwd, "%p != %p\n",
                 cfs_fs_pwd(current->fs), new_ctx->pwd);
        LASSERTF(cfs_fs_mnt(current->fs) == new_ctx->pwdmnt, "%p != %p\n",
                 cfs_fs_mnt(current->fs), new_ctx->pwdmnt);

        set_fs(saved->fs);
        ll_set_fs_pwd(current->fs, saved->pwdmnt, saved->pwd);

        dput(saved->pwd);
        mntput(saved->pwdmnt);
        current->fs->umask = saved->luc.luc_umask;
        if (uc) {
                struct cred *cred;
                if ((cred = prepare_creds())) {
                        cred->fsuid = saved->luc.luc_fsuid;
                        cred->fsgid = saved->luc.luc_fsgid;
                        cred->cap_effective = saved->luc.luc_cap;
                        commit_creds(cred);
                }

                pop_group_info(saved, uc->luc_uce);
        }

        /*
        CDEBUG(D_INFO,
               "= pop  %p->%p = cur fs %p pwd %p:d%d:i%d (%.*s), pwdmnt %p:%d\n",
               saved, current, current->fs, current->fs->pwd,
               atomic_read(&current->fs->pwd->d_count),
               atomic_read(&current->fs->pwd->d_inode->i_count),
               current->fs->pwd->d_name.len, current->fs->pwd->d_name.name,
               current->fs->pwdmnt,
               atomic_read(&current->fs->pwdmnt->mnt_count));
        */
}
EXPORT_SYMBOL(pop_ctxt);

/* utility to make a file */
struct dentry *simple_mknod(struct dentry *dir, char *name, int mode, int fix)
{
        struct dentry *dchild;
        int err = 0;
        ENTRY;

        ASSERT_KERNEL_CTXT("kernel doing mknod outside kernel context\n");
        CDEBUG(D_INODE, "creating file %.*s\n", (int)strlen(name), name);

        dchild = ll_lookup_one_len(name, dir, strlen(name));
        if (IS_ERR(dchild))
                GOTO(out_up, dchild);

        if (dchild->d_inode) {
                int old_mode = dchild->d_inode->i_mode;
                if (!S_ISREG(old_mode))
                        GOTO(out_err, err = -EEXIST);

                /* Fixup file permissions if necessary */
                if (fix && (old_mode & S_IALLUGO) != (mode & S_IALLUGO)) {
                        CWARN("fixing permissions on %s from %o to %o\n",
                              name, old_mode, mode);
                        dchild->d_inode->i_mode = (mode & S_IALLUGO) |
                                                  (old_mode & ~S_IALLUGO);
                        mark_inode_dirty(dchild->d_inode);
                }
                GOTO(out_up, dchild);
        }

        err = ll_vfs_create(dir->d_inode, dchild, (mode & ~S_IFMT) | S_IFREG,
                            NULL);
        if (err)
                GOTO(out_err, err);

        RETURN(dchild);

out_err:
        dput(dchild);
        dchild = ERR_PTR(err);
out_up:
        return dchild;
}
EXPORT_SYMBOL(simple_mknod);

/* utility to make a directory */
struct dentry *simple_mkdir(struct dentry *dir, struct vfsmount *mnt, 
                            char *name, int mode, int fix)
{
        struct dentry *dchild;
        int err = 0;
        ENTRY;

        ASSERT_KERNEL_CTXT("kernel doing mkdir outside kernel context\n");
        CDEBUG(D_INODE, "creating directory %.*s\n", (int)strlen(name), name);
        dchild = ll_lookup_one_len(name, dir, strlen(name));
        if (IS_ERR(dchild))
                GOTO(out_up, dchild);

        if (dchild->d_inode) {
                int old_mode = dchild->d_inode->i_mode;
                if (!S_ISDIR(old_mode)) {
                        CERROR("found %s (%lu/%u) is mode %o\n", name,
                               dchild->d_inode->i_ino,
                               dchild->d_inode->i_generation, old_mode);
                        GOTO(out_err, err = -ENOTDIR);
                }

                /* Fixup directory permissions if necessary */
                if (fix && (old_mode & S_IALLUGO) != (mode & S_IALLUGO)) {
                        CDEBUG(D_CONFIG, 
                               "fixing permissions on %s from %o to %o\n",
                               name, old_mode, mode);
                        dchild->d_inode->i_mode = (mode & S_IALLUGO) |
                                                  (old_mode & ~S_IALLUGO);
                        mark_inode_dirty(dchild->d_inode);
                }
                GOTO(out_up, dchild);
        }

        err = ll_vfs_mkdir(dir->d_inode, dchild, mnt, mode);
        if (err)
                GOTO(out_err, err);

        RETURN(dchild);

out_err:
        dput(dchild);
        dchild = ERR_PTR(err);
out_up:
        return dchild;
}
EXPORT_SYMBOL(simple_mkdir);

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

        err = ll_vfs_rename(dir->d_inode, dchild_old, mnt, 
                            dir->d_inode, dchild_new, mnt);

        dput(dchild_new);
put_old:
        dput(dchild_old);
        RETURN(err);
}
EXPORT_SYMBOL(lustre_rename);

/*
 * Read a file from within kernel context.  Prior to calling this
 * function we should already have done a push_ctxt().
 */
int lustre_fread(struct file *file, void *buf, int len, loff_t *off)
{
        ASSERT_KERNEL_CTXT("kernel doing read outside kernel context\n");
        if (!file || !file->f_op || !file->f_op->read || !off)
                RETURN(-ENOSYS);

        return file->f_op->read(file, buf, len, off);
}
EXPORT_SYMBOL(lustre_fread);

/*
 * Write a file from within kernel context.  Prior to calling this
 * function we should already have done a push_ctxt().
 */
int lustre_fwrite(struct file *file, const void *buf, int len, loff_t *off)
{
        ENTRY;
        ASSERT_KERNEL_CTXT("kernel doing write outside kernel context\n");
        if (!file)
                RETURN(-ENOENT);
        if (!file->f_op)
                RETURN(-ENOSYS);
        if (!off)
                RETURN(-EINVAL);

        if (!file->f_op->write)
                RETURN(-EROFS);

        RETURN(file->f_op->write(file, buf, len, off));
}
EXPORT_SYMBOL(lustre_fwrite);

/*
 * Sync a file from within kernel context.  Prior to calling this
 * function we should already have done a push_ctxt().
 */
int lustre_fsync(struct file *file)
{
        ENTRY;
        ASSERT_KERNEL_CTXT("kernel doing sync outside kernel context\n");
        if (!file || !file->f_op || !file->f_op->fsync)
                RETURN(-ENOSYS);

        RETURN(file->f_op->fsync(file, file->f_dentry, 0));
}
EXPORT_SYMBOL(lustre_fsync);

/* Note:  dput(dchild) will be called if there is an error */
struct l_file *l_dentry_open(struct lvfs_run_ctxt *ctxt, struct l_dentry *de,
                             int flags)
{
        mntget(ctxt->pwdmnt);
        return ll_dentry_open(de, ctxt->pwdmnt, flags, current_cred());
}
EXPORT_SYMBOL(l_dentry_open);

#ifdef HAVE_VFS_READDIR_U64_INO
static int l_filldir(void *__buf, const char *name, int namlen, loff_t offset,
                     u64 ino, unsigned int d_type)
#else
static int l_filldir(void *__buf, const char *name, int namlen, loff_t offset,
                     ino_t ino, unsigned int d_type)
#endif
{
        struct l_linux_dirent *dirent;
        struct l_readdir_callback *buf = (struct l_readdir_callback *)__buf;

        dirent = buf->lrc_dirent;
        if (dirent)
               dirent->lld_off = offset;

        OBD_ALLOC(dirent, sizeof(*dirent));

        if (!dirent)
                return -ENOMEM;

        list_add_tail(&dirent->lld_list, buf->lrc_list);

        buf->lrc_dirent = dirent;
        dirent->lld_ino = ino;
        LASSERT(sizeof(dirent->lld_name) >= namlen + 1);
        memcpy(dirent->lld_name, name, namlen);

        return 0;
}

long l_readdir(struct file *file, struct list_head *dentry_list)
{
        struct l_linux_dirent *lastdirent;
        struct l_readdir_callback buf;
        int error;

        buf.lrc_dirent = NULL;
        buf.lrc_list = dentry_list; 

        error = vfs_readdir(file, l_filldir, &buf);
        if (error < 0)
                return error;

        lastdirent = buf.lrc_dirent;
        if (lastdirent)
                lastdirent->lld_off = file->f_pos;

        return 0; 
}
EXPORT_SYMBOL(l_readdir);

int l_notify_change(struct vfsmount *mnt, struct dentry *dchild,
                    struct iattr *newattrs)
{
        int rc;

        LOCK_INODE_MUTEX(dchild->d_inode);
#ifdef HAVE_SECURITY_PLUG
        rc = notify_change(dchild, mnt, newattrs);
#else
        rc = notify_change(dchild, newattrs);
#endif
        UNLOCK_INODE_MUTEX(dchild->d_inode);
        return rc;
}
EXPORT_SYMBOL(l_notify_change);

/* utility to truncate a file */
int simple_truncate(struct dentry *dir, struct vfsmount *mnt, 
                    char *name, loff_t length)
{
        struct dentry *dchild;
        struct iattr newattrs;
        int err = 0;
        ENTRY;

        CDEBUG(D_INODE, "truncating file %.*s to %lld\n", (int)strlen(name),
               name, (long long)length);
        dchild = ll_lookup_one_len(name, dir, strlen(name));
        if (IS_ERR(dchild))
                GOTO(out, err = PTR_ERR(dchild));

        if (dchild->d_inode) {
                int old_mode = dchild->d_inode->i_mode;
                if (S_ISDIR(old_mode)) {
                        CERROR("found %s (%lu/%u) is mode %o\n", name,
                               dchild->d_inode->i_ino,
                               dchild->d_inode->i_generation, old_mode);
                        GOTO(out_dput, err = -EISDIR);
                }

                newattrs.ia_size = length;
                newattrs.ia_valid = ATTR_SIZE;
                err = l_notify_change(mnt, dchild, &newattrs);
        }
        EXIT;
out_dput:
        dput(dchild);
out:
        return err;
}
EXPORT_SYMBOL(simple_truncate);

#ifdef LUSTRE_KERNEL_VERSION
#ifndef HAVE_CLEAR_RDONLY_ON_PUT
#error rdonly patchset must be updated [cfs bz11248]
#endif

void dev_set_rdonly(lvfs_sbdev_type dev);
int dev_check_rdonly(lvfs_sbdev_type dev);

void __lvfs_set_rdonly(lvfs_sbdev_type dev, lvfs_sbdev_type jdev)
{
        lvfs_sbdev_sync(dev);
        if (jdev && (jdev != dev)) {
                CDEBUG(D_IOCTL | D_HA, "set journal dev %lx rdonly\n",
                       (long)jdev);
                dev_set_rdonly(jdev);
        }
        CDEBUG(D_IOCTL | D_HA, "set dev %lx rdonly\n", (long)dev);
        dev_set_rdonly(dev);
}

int lvfs_check_rdonly(lvfs_sbdev_type dev)
{
        return dev_check_rdonly(dev);
}

EXPORT_SYMBOL(__lvfs_set_rdonly);
EXPORT_SYMBOL(lvfs_check_rdonly);
#endif /* LUSTRE_KERNEL_VERSION */

int lvfs_check_io_health(struct obd_device *obd, struct file *file)
{
        char *write_page = NULL;
        loff_t offset = 0;
        int rc = 0;
        ENTRY;

        OBD_ALLOC(write_page, CFS_PAGE_SIZE);
        if (!write_page)
                RETURN(-ENOMEM);
        
        rc = fsfilt_write_record(obd, file, write_page, CFS_PAGE_SIZE, &offset, 1);
       
        OBD_FREE(write_page, CFS_PAGE_SIZE);

        CDEBUG(D_INFO, "write 1 page synchronously for checking io rc %d\n",rc);
        RETURN(rc); 
}
EXPORT_SYMBOL(lvfs_check_io_health);

MODULE_AUTHOR("Sun Microsystems, Inc. <http://www.lustre.org/>");
MODULE_DESCRIPTION("Lustre VFS Filesystem Helper v0.1");
MODULE_LICENSE("GPL");
