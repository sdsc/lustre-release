/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (c) 2004 - 2005 Cluster File Systems, Inc.
 *
 *   This file is part of Lustre, http://www.lustre.org.
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/mm.h>
#include <linux/smp_lock.h>

#define DEBUG_SUBSYSTEM S_LLITE

#include <obd_support.h>
#include <lustre_lite.h>
#include <lustre_dlm.h>
#include <linux/lustre_version.h>

#ifndef POSIX_ACL_XATTR_ACCESS
#ifndef XATTR_NAME_ACL_ACCESS
#define XATTR_NAME_ACL_ACCESS   "system.posix_acl_access"
#endif
#define POSIX_ACL_XATTR_ACCESS XATTR_NAME_ACL_ACCESS
#endif
#ifndef POSIX_ACL_XATTR_DEFAULT
#ifndef XATTR_NAME_ACL_DEFAULT
#define XATTR_NAME_ACL_DEFAULT  "system.posix_acl_default"
#endif
#define POSIX_ACL_XATTR_DEFAULT XATTR_NAME_ACL_DEFAULT
#endif

#include "llite_internal.h"

#define XATTR_USER_PREFIX       "user."
#define XATTR_TRUSTED_PREFIX    "trusted."
#define XATTR_SECURITY_PREFIX   "security."

#define XATTR_USER_T            (1)
#define XATTR_TRUSTED_T         (2)
#define XATTR_SECURITY_T        (3)
#define XATTR_ACL_ACCESS_T      (4)
#define XATTR_ACL_DEFAULT_T     (5)
#define XATTR_OTHER_T           (6)

static
int get_xattr_type(const char *name)
{
        if (!strcmp(name, POSIX_ACL_XATTR_ACCESS))
                return XATTR_ACL_ACCESS_T;

        if (!strcmp(name, POSIX_ACL_XATTR_DEFAULT))
                return XATTR_ACL_DEFAULT_T;

        if (!strncmp(name, XATTR_USER_PREFIX,
                     sizeof(XATTR_USER_PREFIX) - 1))
                return XATTR_USER_T;

        if (!strncmp(name, XATTR_TRUSTED_PREFIX,
                     sizeof(XATTR_TRUSTED_PREFIX) - 1))
                return XATTR_TRUSTED_T;

        if (!strncmp(name, XATTR_SECURITY_PREFIX,
                     sizeof(XATTR_SECURITY_PREFIX) - 1))
                return XATTR_SECURITY_T;

        return XATTR_OTHER_T;
}

static
int xattr_type_filter(struct ll_sb_info *sbi, int xattr_type)
{
        if ((xattr_type == XATTR_ACL_ACCESS_T ||
             xattr_type == XATTR_ACL_DEFAULT_T) &&
            !(sbi->ll_flags & LL_SBI_ACL))
                return -EOPNOTSUPP;

        if (xattr_type == XATTR_USER_T && !(sbi->ll_flags & LL_SBI_USER_XATTR))
                return -EOPNOTSUPP;
        if (xattr_type == XATTR_TRUSTED_T && !capable(CAP_SYS_ADMIN))
                return -EPERM;
        if (xattr_type == XATTR_OTHER_T)
                return -EOPNOTSUPP;

        return 0;
}

static
int ll_setxattr_common(struct inode *inode, const char *name,
                       const void *value, size_t size,
                       int flags, __u64 valid)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct ptlrpc_request *req;
        struct ll_fid fid;
        int xattr_type, rc;
        ENTRY;

        lprocfs_counter_incr(sbi->ll_stats, LPROC_LL_SETXATTR);

        xattr_type = get_xattr_type(name);
        rc = xattr_type_filter(sbi, xattr_type);
        if (rc)
                RETURN(rc);

        /* b10667: ignore lustre special xattr for now */
        if (xattr_type == XATTR_TRUSTED_T && strcmp(name, "trusted.lov") == 0)
                RETURN(0);

        ll_inode2fid(&fid, inode);
        rc = mdc_setxattr(sbi->ll_mdc_exp, &fid, valid,
                          name, value, size, 0, flags, &req);
        if (rc) {
                if (rc == -EOPNOTSUPP && xattr_type == XATTR_USER_T) {
                        LCONSOLE_INFO("Disabling user_xattr feature because "
                                      "it is not supported on the server\n"); 
                        sbi->ll_flags &= ~LL_SBI_USER_XATTR;
                }
                RETURN(rc);
        }

        ptlrpc_req_finished(req);
        RETURN(0);
}

int ll_setxattr(struct dentry *dentry, const char *name,
                const void *value, size_t size, int flags)
{
        struct inode *inode = dentry->d_inode;

        LASSERT(inode);
        LASSERT(name);

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p), xattr %s\n",
               inode->i_ino, inode->i_generation, inode, name);

        ll_vfs_ops_tally(ll_i2sbi(inode), VFS_OPS_SETXATTR);

        if (strncmp(name, XATTR_TRUSTED_PREFIX, 8) == 0 &&
            strcmp(name + 8, "lov") == 0) {
                struct lov_user_md *lump = (struct lov_user_md *)value;
                int rc = 0;

                if (S_ISREG(inode->i_mode)) {
                        struct file f;
                        int flags = FMODE_WRITE;
                        
                        f.f_dentry = dentry;
                        rc = ll_lov_setstripe_ea_info(inode, &f, flags, 
                                                      lump, sizeof(*lump));
                        /* b10667: rc always be 0 here for now */
                        rc = 0;
                } else if (S_ISDIR(inode->i_mode)) {
                        rc = ll_dir_setstripe(inode, lump);
                }
                
                return rc;
        }

        return ll_setxattr_common(inode, name, value, size, flags,
                                  OBD_MD_FLXATTR);
}

int ll_removexattr(struct dentry *dentry, const char *name)
{
        struct inode *inode = dentry->d_inode;

        LASSERT(inode);
        LASSERT(name);

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p), xattr %s\n",
               inode->i_ino, inode->i_generation, inode, name);

        ll_vfs_ops_tally(ll_i2sbi(inode), VFS_OPS_REMOVEXATTR);
        return ll_setxattr_common(inode, name, NULL, 0, 0,
                                  OBD_MD_FLXATTRRM);
}

static
int ll_getxattr_common(struct inode *inode, const char *name,
                       void *buffer, size_t size, __u64 valid)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct ptlrpc_request *req = NULL;
        struct mds_body *body;
        struct ll_fid fid;
        void *xdata;
        int xattr_type, rc;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p)\n",
               inode->i_ino, inode->i_generation, inode);

        lprocfs_counter_incr(sbi->ll_stats, LPROC_LL_GETXATTR);

        /* listxattr have slightly different behavior from of ext3:
         * without 'user_xattr' ext3 will list all xattr names but
         * filtered out "^user..*"; we list them all for simplicity.
         */
        if (!name) {
                xattr_type = XATTR_OTHER_T;
                goto do_getxattr;
        }

        xattr_type = get_xattr_type(name);
        rc = xattr_type_filter(sbi, xattr_type);
        if (rc)
                RETURN(rc);

        /* posix acl is under protection of LOOKUP lock. when calling to this,
         * we just have path resolution to the target inode, so we have great
         * chance that cached ACL is uptodate.
         */
#ifdef CONFIG_FS_POSIX_ACL
        if (xattr_type == XATTR_ACL_ACCESS_T) {
                struct ll_inode_info *lli = ll_i2info(inode);
                struct posix_acl *acl;

                spin_lock(&lli->lli_lock);
                acl = posix_acl_dup(lli->lli_posix_acl);
                spin_unlock(&lli->lli_lock);

                if (!acl)
                        RETURN(-ENODATA);

                rc = posix_acl_to_xattr(acl, buffer, size);
                posix_acl_release(acl);
                RETURN(rc);
        }
#endif

do_getxattr:
        ll_inode2fid(&fid, inode);
        rc = mdc_getxattr(sbi->ll_mdc_exp, &fid, valid, name, NULL, 0, size,
                          &req);
        if (rc) {
                if (rc == -EOPNOTSUPP && xattr_type == XATTR_USER_T) {
                        LCONSOLE_INFO("Disabling user_xattr feature because "
                                      "it is not supported on the server\n"); 
                        sbi->ll_flags &= ~LL_SBI_USER_XATTR;
                }
                RETURN(rc);
        }

        body = lustre_msg_buf(req->rq_repmsg, REPLY_REC_OFF, sizeof(*body));
        LASSERT(body);
        LASSERT_REPSWABBED(req, REPLY_REC_OFF);

        /* only detect the xattr size */
        if (size == 0)
                GOTO(out, rc = body->eadatasize);

        if (size < body->eadatasize) {
                CERROR("server bug: replied size %u > %u\n",
                       body->eadatasize, (int)size);
                GOTO(out, rc = -ERANGE);
        }

        if (lustre_msg_bufcount(req->rq_repmsg) < 3) {
                CERROR("reply bufcount %u\n",
                       lustre_msg_bufcount(req->rq_repmsg));
                GOTO(out, rc = -EFAULT);
        }

        /* do not need swab xattr data */
        LASSERT_REPSWAB(req, REPLY_REC_OFF + 1);
        xdata = lustre_msg_buf(req->rq_repmsg, REPLY_REC_OFF + 1,
                               body->eadatasize);
        if (!xdata) {
                CERROR("can't extract: %u : %u\n", body->eadatasize,
                       lustre_msg_buflen(req->rq_repmsg, REPLY_REC_OFF + 1));
                GOTO(out, rc = -EFAULT);
        }

        LASSERT(buffer);
        memcpy(buffer, xdata, body->eadatasize);
        rc = body->eadatasize;
out:
        ptlrpc_req_finished(req);
        RETURN(rc);
}

ssize_t ll_getxattr(struct dentry *dentry, const char *name,
                    void *buffer, size_t size)
{
        struct inode *inode = dentry->d_inode;

        LASSERT(inode);
        LASSERT(name);

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p), xattr %s\n",
               inode->i_ino, inode->i_generation, inode, name);

        ll_vfs_ops_tally(ll_i2sbi(inode), VFS_OPS_GETXATTR);

         if (strncmp(name, XATTR_TRUSTED_PREFIX, 8) == 0 &&
             strcmp(name + 8, "lov") == 0) {
                 struct lov_user_md *lump;
                 struct lov_mds_md *lmm = NULL;
                 struct ptlrpc_request *request = NULL;
                 int rc = 0, lmmsize;

                 if (S_ISREG(inode->i_mode)) {
                         rc = ll_lov_getstripe_ea_info(dentry->d_parent->d_inode, 
                                                       dentry->d_name.name, &lmm, 
                                                       &lmmsize, &request);
                 } else if (S_ISDIR(inode->i_mode)) {
                         rc = ll_dir_getstripe(inode, &lmm, &lmmsize, &request);
                 }

                 if (rc < 0)
                        GOTO(out, rc);
                 if (size == 0)
                        GOTO(out, rc = lmmsize);

                 if (size < lmmsize) {
                         CERROR("server bug: replied size %u > %u\n",
                                lmmsize, (int)size);
                         GOTO(out, rc = -ERANGE);
                 }

                 lump = (struct lov_user_md *)buffer;
                 memcpy(lump, lmm, lmmsize);

                 rc = lmmsize;
out:
                 ptlrpc_req_finished(request);
                 return(rc);
        }

        return ll_getxattr_common(inode, name, buffer, size, OBD_MD_FLXATTR);
}

ssize_t ll_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
        struct inode *inode = dentry->d_inode;
        int rc = 0, rc2 = 0;

        LASSERT(inode);

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p)\n",
               inode->i_ino, inode->i_generation, inode);

        ll_vfs_ops_tally(ll_i2sbi(inode), VFS_OPS_LISTXATTR);

        rc = ll_getxattr_common(inode, NULL, buffer, size, OBD_MD_FLXATTRLS);

        if (!capable(CAP_SYS_ADMIN)) {
                struct lov_mds_md *lmm = NULL;
                struct ptlrpc_request *request = NULL;
                int lmmsize;

                if (S_ISREG(inode->i_mode)) {
                        struct ll_inode_info *lli = ll_i2info(inode);
                        struct lov_stripe_md *lsm = NULL;
                        lsm = lli->lli_smd;
                        if (lsm == NULL)
                                rc2 = -1; 
                } else if (S_ISDIR(inode->i_mode)) {
                        rc2 = ll_dir_getstripe(inode, &lmm, &lmmsize, &request);
                }

                if (rc2 < 0) {
                        GOTO(out, rc2 = 0);
                } else {
                        const int prefix_len = sizeof(XATTR_TRUSTED_PREFIX)-1;
                        const size_t name_len   = sizeof("lov") - 1;
                        const size_t total_len  = prefix_len + name_len + 1;

                        if (buffer && (rc + total_len) <= size) {
                                buffer += rc;
                                memcpy(buffer,XATTR_TRUSTED_PREFIX, prefix_len);
                                memcpy(buffer+prefix_len, "lov", name_len);
                                buffer[prefix_len + name_len] = '\0';
                        }
                        rc2 = total_len;
                }
out:
                ptlrpc_req_finished(request);
                rc = rc + rc2;
        }
        
        return rc;
}

