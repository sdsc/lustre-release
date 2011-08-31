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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011 Whamcloud, Inc.
 *
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/lustre/llite/llite_nfs.c
 *
 * NFS export of Lustre Light File System
 *
 * Author: Yury Umanets <umka@clusterfs.com>
 * Author: Huang Hua <huanghua@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_LLITE
#include <lustre_lite.h>
#include "llite_internal.h"
#ifdef HAVE_LINUX_EXPORTFS_H
#include <linux/exportfs.h>
#endif

__u32 get_uuid2int(const char *name, int len)
{
        __u32 key0 = 0x12a3fe2d, key1 = 0x37abe8f9;
        while (len--) {
                __u32 key = key1 + (key0 ^ (*name++ * 7152373));
                if (key & 0x80000000) key -= 0x7fffffff;
                key1 = key0;
                key0 = key;
        }
        return (key0 << 1);
}

static int ll_nfs_test_inode(struct inode *inode, void *opaque)
{
        return lu_fid_eq(&ll_i2info(inode)->lli_fid,
                         (struct lu_fid *)opaque);
}

static struct inode *search_inode_for_lustre(struct super_block *sb,
                                             const struct lu_fid *fid)
{
        struct ll_sb_info     *sbi = ll_s2sbi(sb);
        struct ptlrpc_request *req = NULL;
        struct inode          *inode = NULL;
        int                   eadatalen = 0;
        unsigned long         hash = (unsigned long) cl_fid_build_ino(fid, 0);
        struct  md_op_data    *op_data;
        int                   rc;
        ENTRY;

        CDEBUG(D_INFO, "searching inode for:(%lu,"DFID")\n", hash, PFID(fid));

        inode = ilookup5(sb, hash, ll_nfs_test_inode, (void *)fid);
        if (inode)
                RETURN(inode);

        rc = ll_get_max_mdsize(sbi, &eadatalen);
        if (rc)
                RETURN(ERR_PTR(rc));

        /* Because inode is NULL, ll_prep_md_op_data can not
         * be used here. So we allocate op_data ourselves */
        OBD_ALLOC_PTR(op_data);
        if (op_data == NULL)
                return ERR_PTR(-ENOMEM);

        op_data->op_fid1 = *fid;
        op_data->op_mode = eadatalen;
        op_data->op_valid = OBD_MD_FLEASIZE;

        /* mds_fid2dentry ignores f_type */
        rc = md_getattr(sbi->ll_md_exp, op_data, &req);
        OBD_FREE_PTR(op_data);
        if (rc) {
                CERROR("can't get object attrs, fid "DFID", rc %d\n",
                       PFID(fid), rc);
                RETURN(ERR_PTR(rc));
        }
        rc = ll_prep_inode(&inode, req, sb);
        ptlrpc_req_finished(req);
        if (rc)
                RETURN(ERR_PTR(rc));

        RETURN(inode);
}

struct lustre_nfs_fid {
        struct lu_fid   lnf_child;
        struct lu_fid   lnf_parent;
};

static struct dentry *
ll_iget_for_nfs(struct super_block *sb, struct lu_fid *fid, struct lu_fid *parent)
{
        struct inode  *inode;
        struct dentry *result;
        ENTRY;

        CDEBUG(D_INFO, "Get dentry for fid: "DFID"\n", PFID(fid));
        if (!fid_is_sane(fid))
                RETURN(ERR_PTR(-ESTALE));

        inode = search_inode_for_lustre(sb, fid);
        if (IS_ERR(inode))
                RETURN(ERR_PTR(PTR_ERR(inode)));

        if (is_bad_inode(inode)) {
                /* we didn't find the right inode.. */
                iput(inode);
                RETURN(ERR_PTR(-ESTALE));
        }

        /**
         * It is an anonymous dentry without OST objects created yet.
         * We have to find the parent to tell MDS how to init lov objects.
         */
        if (S_ISREG(inode->i_mode) && ll_i2info(inode)->lli_smd == NULL &&
            parent != NULL) {
                struct ll_inode_info *lli = ll_i2info(inode);

                cfs_spin_lock(&lli->lli_lock);
                lli->lli_pfid = *parent;
                cfs_spin_unlock(&lli->lli_lock);
        }

        result = d_obtain_alias(inode);
        if (IS_ERR(result))
                RETURN(result);

        ll_dops_init(result, 1, 0);

        RETURN(result);
}

#define LUSTRE_NFS_FID          0x97

/**
 * \a connectable - is nfsd will connect himself or this should be done
 *                  at lustre
 *
 * The return value is file handle type:
 * 1 -- contains child file handle;
 * 2 -- contains child file handle and parent file handle;
 * 255 -- error.
 */
static int ll_encode_fh(struct dentry *de, __u32 *fh, int *plen,
                        int connectable)
{
        struct inode *inode = de->d_inode;
        struct inode *parent = de->d_parent->d_inode;
        struct lustre_nfs_fid *nfs_fid = (void *)fh;
        ENTRY;

        CDEBUG(D_INFO, "encoding for (%lu,"DFID") maxlen=%d minlen=%d\n",
              inode->i_ino, PFID(ll_inode2fid(inode)), *plen,
              (int)sizeof(struct lustre_nfs_fid));

        if (*plen < sizeof(struct lustre_nfs_fid)/4)
                RETURN(255);

        nfs_fid->lnf_child = *ll_inode2fid(inode);
        nfs_fid->lnf_parent = *ll_inode2fid(parent);
        *plen = sizeof(struct lustre_nfs_fid)/4;

        RETURN(LUSTRE_NFS_FID);
}

static int ll_nfs_get_name_filldir(void *cookie, const char *name, int namelen,
                                   loff_t hash, u64 ino, unsigned type)
{
        /* It is hack to access lde_fid for comparison with lgd_fid.
         * So the input 'name' must be part of the 'lu_dirent'. */
        struct lu_dirent *lde = container_of0(name, struct lu_dirent, lde_name);
        struct ll_getname_data *lgd = cookie;
        struct lu_fid fid;

        fid_le_to_cpu(&fid, &lde->lde_fid);
        if (lu_fid_eq(&fid, &lgd->lgd_fid)) {
                memcpy(lgd->lgd_name, name, namelen);
                lgd->lgd_name[namelen] = 0;
                lgd->lgd_found = 1;
        }
        return lgd->lgd_found;
}

static int ll_get_name(struct dentry *dentry, char *name,
                       struct dentry *child)
{
        struct inode *dir = dentry->d_inode;
        struct file *filp;
        struct ll_getname_data lgd;
        int rc;
        ENTRY;

        if (!dir || !S_ISDIR(dir->i_mode))
                GOTO(out, rc = -ENOTDIR);

        if (!dir->i_fop)
                GOTO(out, rc = -EINVAL);

        filp = ll_dentry_open(dget(dentry), mntget(ll_i2sbi(dir)->ll_mnt),
                              O_RDONLY, current_cred());
        if (IS_ERR(filp))
                GOTO(out, rc = PTR_ERR(filp));

        if (!filp->f_op->readdir)
                GOTO(out_close, rc = -EINVAL);

        lgd.lgd_name = name;
        lgd.lgd_fid = ll_i2info(child->d_inode)->lli_fid;
        lgd.lgd_found = 0;

        cfs_mutex_lock(&dir->i_mutex);
        rc = ll_readdir(filp, &lgd, ll_nfs_get_name_filldir);
        cfs_mutex_unlock(&dir->i_mutex);
        if (!rc && !lgd.lgd_found)
                rc = -ENOENT;
        EXIT;

out_close:
        fput(filp);
out:
        return rc;
}

#ifdef HAVE_FH_TO_DENTRY
static struct dentry *ll_fh_to_dentry(struct super_block *sb, struct fid *fid,
                                      int fh_len, int fh_type)
{
        struct lustre_nfs_fid *nfs_fid = (struct lustre_nfs_fid *)fid;

        if (fh_type != LUSTRE_NFS_FID)
                RETURN(ERR_PTR(-EPROTO));

        RETURN(ll_iget_for_nfs(sb, &nfs_fid->lnf_child, &nfs_fid->lnf_parent));
}

static struct dentry *ll_fh_to_parent(struct super_block *sb, struct fid *fid,
                                      int fh_len, int fh_type)
{
        struct lustre_nfs_fid *nfs_fid = (struct lustre_nfs_fid *)fid;

        if (fh_type != LUSTRE_NFS_FID)
                RETURN(ERR_PTR(-EPROTO));

        RETURN(ll_iget_for_nfs(sb, &nfs_fid->lnf_parent, NULL));
}

#else

/*
 * This length is counted as amount of __u32,
 *  It is composed of a fid and a mode
 */
static struct dentry *ll_decode_fh(struct super_block *sb, __u32 *fh, int fh_len,
                                   int fh_type,
                                   int (*acceptable)(void *, struct dentry *),
                                   void *context)
{
        struct lustre_nfs_fid *nfs_fid = (void *)fh;
        struct dentry *entry;
        ENTRY;

        CDEBUG(D_INFO, "decoding for "DFID" fh_len=%d fh_type=%x\n", 
                PFID(&nfs_fid->lnf_child), fh_len, fh_type);

        if (fh_type != LUSTRE_NFS_FID)
                RETURN(ERR_PTR(-EPROTO));

        entry = sb->s_export_op->find_exported_dentry(sb, &nfs_fid->lnf_child,
                                                      &nfs_fid->lnf_parent,
                                                      acceptable, context);
        RETURN(entry);
}

static struct dentry *ll_get_dentry(struct super_block *sb, void *data)
{
        struct dentry *entry;
        ENTRY;

        entry = ll_iget_for_nfs(sb, data, NULL);
        RETURN(entry);
}
#endif
static struct dentry *ll_get_parent(struct dentry *dchild)
{
        struct ptlrpc_request *req = NULL;
        struct inode          *dir = dchild->d_inode;
        struct ll_sb_info     *sbi;
        struct dentry         *result = NULL;
        struct mdt_body       *body;
        static char           dotdot[] = "..";
        struct md_op_data     *op_data;
        int                   rc;
        ENTRY;

        LASSERT(dir && S_ISDIR(dir->i_mode));

        sbi = ll_s2sbi(dir->i_sb);

        CDEBUG(D_INFO, "getting parent for (%lu,"DFID")\n",
                        dir->i_ino, PFID(ll_inode2fid(dir)));

        op_data = ll_prep_md_op_data(NULL, dir, NULL, dotdot,
                                     strlen(dotdot), 0,
                                     LUSTRE_OPC_ANY, NULL);
        if (op_data == NULL)
                RETURN(ERR_PTR(-ENOMEM));

        rc = md_getattr_name(sbi->ll_md_exp, op_data, &req);
        ll_finish_md_op_data(op_data);
        if (rc) {
                CERROR("failure %d inode %lu get parent\n", rc, dir->i_ino);
                RETURN(ERR_PTR(rc));
        }
        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
        LASSERT(body->valid & OBD_MD_FLID);

        CDEBUG(D_INFO, "parent for "DFID" is "DFID"\n",
                PFID(ll_inode2fid(dir)), PFID(&body->fid1));

        result = ll_iget_for_nfs(dir->i_sb, &body->fid1, NULL);

        ptlrpc_req_finished(req);
        RETURN(result);
}

struct export_operations lustre_export_operations = {
       .get_parent = ll_get_parent,
       .encode_fh  = ll_encode_fh,
       .get_name   = ll_get_name,
#ifdef HAVE_FH_TO_DENTRY
        .fh_to_dentry = ll_fh_to_dentry,
        .fh_to_parent = ll_fh_to_parent,
#else
       .get_dentry = ll_get_dentry,
       .decode_fh  = ll_decode_fh,
#endif
};

#ifdef CONFIG_PNFSD
#include <linux/sunrpc/svc.h>
//#include <linux/nfsd/nfsd.h>
//#include <linux/nfsd/cache.h>
#include <linux/nfs4.h>
#include <linux/nfs_fs.h>
//#include <linux/nfsd/state.h>
//#include <linux/nfsd/xdr4.h>
#include <linux/nfsd/nfs4layoutxdr.h>
#include <linux/nfsd/nfsd4_pnfs.h>
//#include <linux/nfs4_pnfs.h>
#include <linux/nfsd/nfsfh.h>

/* XXX: copied from fs/nfsd/nfsfh.h */
enum nfsd_fsid {
        FSID_DEV = 0,
        FSID_NUM,
        FSID_MAJOR_MINOR,
        FSID_ENCODE_DEV,
        FSID_UUID4_INUM,
        FSID_UUID8,
        FSID_UUID16,
        FSID_UUID16_INUM,
        FSID_MAX
};

#define IP_BUF_LEN 29
#define PORT 2049
#define NETTYPE "tcp"
#define NETTYPE_LEN 3

static char *hack_ip[] = { "192.168.163.177.8.1", "192.168.163.176.8.1" };

static int pack_devaddr(struct pnfs_filelayout_devaddr *devaddr,
                        __u32 nid, int i)
{
        int rc = 0;
        // int port = PORT;
        ENTRY;

        CDEBUG(D_INFO, "pack devaddr %p nid %u\n", devaddr, nid);

        OBD_ALLOC(devaddr->r_netid.data, NETTYPE_LEN);
        if (devaddr->r_netid.data == NULL)
                GOTO(out, rc = -ENOMEM);

        devaddr->r_netid.len = NETTYPE_LEN;
        memcpy(devaddr->r_netid.data, NETTYPE, NETTYPE_LEN);

        OBD_ALLOC(devaddr->r_addr.data, IP_BUF_LEN);
        if (devaddr->r_addr.data == NULL) {
                OBD_FREE(devaddr->r_netid.data, NETTYPE_LEN);
                GOTO(out, rc = -ENOMEM);
        }
#if 0
       snprintf(devaddr->r_addr.data, IP_BUF_LEN, "%u.%u.%u.%u.%u.%u",
                (nid >> 24) & 0xff, (nid >> 16) & 0xff, (nid >> 8) & 0xff,
                nid & 0xff, (port >> 8) & 0xff, port & 0xff);
#else
               snprintf(devaddr->r_addr.data, strlen(hack_ip[i]) + 1, hack_ip[i]);
#endif
       devaddr->r_addr.len = strlen(devaddr->r_addr.data);

       CDEBUG(D_INFO, "pack address %.*s type %.*s \n", devaddr->r_addr.len,
              devaddr->r_addr.data, devaddr->r_netid.len,devaddr->r_netid.data);
out:
       if (rc) {
               if (devaddr->r_netid.data)
                       OBD_FREE(devaddr->r_netid.data, NETTYPE_LEN);
               if (devaddr->r_addr.data)
                       OBD_FREE(devaddr->r_addr.data, IP_BUF_LEN);
       }
       RETURN(rc);
}

static int ll_get_deviceinfo(struct super_block *sb, struct exp_xdr_stream *xdr,
                             u32 layout_type,
                             const struct nfsd4_pnfs_deviceid *devid)
{
        int ost_count = 0;
        __u32 *ip_address = NULL, *pip, tmp_len;
        int rc = 0, i;
        struct pnfs_filelayout_device *pfd = NULL;
        ENTRY;

        OBD_ALLOC_PTR(pfd);
        if (!pfd)
                GOTO(out, rc = -ENOMEM);

        /*get ip address */
        tmp_len = sizeof(ost_count);
        rc = obd_get_info(ll_s2dtexp(sb), strlen(KEY_OST_COUNT), KEY_OST_COUNT,
                          &tmp_len, &ost_count, NULL);
        if (rc) {
                CERROR("Can not get OST/device count: %d\n", rc);
                GOTO(out, rc);
        }
        CDEBUG(D_INFO, "get ost_count %d \n", ost_count);

        LASSERT(ost_count > 0);

        pfd->fl_stripeindices_length = ost_count;
        pfd->fl_device_length = ost_count;

        OBD_ALLOC(pfd->fl_stripeindices_list, ost_count * sizeof(u32));
        if (!pfd->fl_stripeindices_list)
                GOTO(out, rc = -ENOMEM);

        OBD_ALLOC(pfd->fl_device_list,
                  ost_count * sizeof(struct pnfs_filelayout_multipath));
        if (!pfd->fl_device_list)
                GOTO(out, rc = -ENOMEM);

        OBD_ALLOC(ip_address, sizeof(*ip_address) * ost_count);
        if (!ip_address)
                GOTO(out, rc = -ENOMEM);

        tmp_len = sizeof(*ip_address) * ost_count;
        rc = obd_get_info(ll_s2dtexp(sb), strlen(KEY_NFS_IP), KEY_NFS_IP,
                          &tmp_len, ip_address, NULL);
        if (rc) {
                CERROR("Can not get ip address of ost \n");
                GOTO(out, rc);
        }

        pip = ip_address;

        for (i = 0; i < ost_count ; i++, pip++) {
                pfd->fl_stripeindices_list[i] = i;
                /* XXX no multipath for now */
                pfd->fl_device_list[i].fl_multipath_length = 1;
                OBD_ALLOC_PTR(pfd->fl_device_list[i].fl_multipath_list);
                if (!pfd->fl_device_list[i].fl_multipath_list)
                        GOTO(out, rc = -ENOMEM);
                pack_devaddr(pfd->fl_device_list[i].fl_multipath_list,
                             *pip, i);
        }

        /* encode the device data */
        rc = filelayout_encode_devinfo(xdr, pfd);
out:
        if (ip_address)
                OBD_FREE(ip_address, sizeof(*ip_address) * ost_count);

        for (i = 0; i < ost_count ; i++, pip++) {
                /* XXX - free in some helper? */
                OBD_FREE(pfd->fl_device_list[i].fl_multipath_list->r_addr.data, IP_BUF_LEN);
                OBD_FREE(pfd->fl_device_list[i].fl_multipath_list->r_netid.data, NETTYPE_LEN);
                OBD_FREE_PTR(pfd->fl_device_list[i].fl_multipath_list);
        }
        if (pfd->fl_device_list)
                OBD_FREE(pfd->fl_device_list,
                          ost_count * sizeof(struct pnfs_filelayout_multipath));
        if (pfd->fl_stripeindices_list)
                OBD_FREE(pfd->fl_stripeindices_list, ost_count * sizeof(u32));
        OBD_FREE_PTR(pfd);

       RETURN(rc);
}

int ll_get_device_iter(struct super_block *sb, u32 layout_type,
                       struct nfsd4_pnfs_dev_iter_res *gd_res)
{
        int ost_count = 0;
        u32 tmplen;
        int rc = 0;

        /* cookie is our ost idx */

        tmplen = sizeof(ost_count);
        rc = obd_get_info(ll_s2mdexp(sb), strlen(KEY_OST_COUNT), KEY_OST_COUNT,
                          &tmplen, &ost_count, NULL);
        if (rc) {
                CERROR("Can not get OST/device count %d \n", ost_count);
                GOTO(out, rc);
        }

        if (gd_res->gd_cookie > 0) {
                gd_res->gd_eof = 1;
        } else {
                gd_res->gd_devid = gd_res->gd_cookie;
                gd_res->gd_cookie++;
                gd_res->gd_verf = 1;
                gd_res->gd_eof = 0;
        }
out:
        RETURN(rc);
}


static int ll_layout_type(struct super_block *sb)
{
        ENTRY;

        return LAYOUT_NFSV4_1_FILES;
}

static enum nfsstat4 ll_get_layout(struct inode *inode,
                                  struct exp_xdr_stream *xdr,
                                  const struct nfsd4_pnfs_layoutget_arg *lg_arg,
                                  struct nfsd4_pnfs_layoutget_res *lg_res)
{
        struct ll_inode_info *lli = ll_i2info(inode);
        struct lov_stripe_md *lsm;
        struct pnfs_filelayout_layout *flp;
        int rc = 0;
        ENTRY;

        if (!lli || !lli->lli_smd) {
                struct ptlrpc_request *req = NULL;
                struct ll_sb_info *sbi = ll_s2sbi(inode->i_sb);
                obd_valid valid = OBD_MD_FLGETATTR;
                struct md_op_data *op_data;
                int ealen = 0;

                rc = ll_get_max_mdsize(sbi, &ealen);
                if (rc)
                        RETURN(rc);
                valid |= OBD_MD_FLEASIZE | OBD_MD_FLMODEASIZE;

                op_data = ll_prep_md_op_data(NULL, inode, NULL, NULL,
                                             0, ealen, LUSTRE_OPC_ANY,
                                             NULL);
                if (op_data == NULL)
                        RETURN(-ENOMEM);

                op_data->op_valid = valid;
                /* Once OBD_CONNECT_ATTRFID is not supported, we can't find one
                 * capa for this inode. Because we only keep capas of dirs
                 * fresh. */
                rc = md_getattr(sbi->ll_md_exp, op_data, &req);
                ll_finish_md_op_data(op_data);
                if (rc) {
                        CERROR("can't get object attrs, fid "DFID", rc %d\n",
                               PFID(&op_data->op_fid1), rc);
                        RETURN(rc);
                }

                rc = ll_prep_inode(&inode, req, NULL);
                ptlrpc_req_finished(req);
                if (rc)
                        RETURN(rc);
        }
        lli = ll_i2info(inode);

        lsm = lli->lli_smd;

        /* no striping info - no layout, return some error */
        if (!lsm)
                RETURN(-EINVAL);

        OBD_ALLOC_PTR(flp);
        if (!flp)
               GOTO(out, rc = -ENOMEM);

        /* We can use same nfs handle always */
        flp->lg_fh_length = 1;
        OBD_ALLOC_PTR(flp->lg_fh_list);
        if (!flp->lg_fh_list)
                GOTO(out, rc = -ENOMEM); // XXX
        memcpy(flp->lg_fh_list, lg_arg->lg_fh, sizeof(struct knfsd_fh));

        /* XXX - we should not poke inside of fh like this. */
        flp->lg_fh_list->fh_base.fh_new.fb_fsid_type += FSID_MAX;
        flp->lg_commit_through_mds = 0; // not needed as we zero our allocs
        flp->lg_layout_type = 1; /* XXX */
        flp->lg_pattern_offset = 0; /* XXX - not true for JOINFILE! */
        flp->lg_first_stripe_index = 0;
        flp->lg_stripe_type = STRIPE_SPARSE;
        flp->lg_stripe_unit = lsm->lsm_stripe_size;
        flp->device_id.sbid = lg_arg->lg_sbid;
        flp->device_id.devid = 0; // Always work on device 0

        CDEBUG(D_INFO, "Created file layout, encoding \n");

        rc = filelayout_encode_layout(xdr, flp);
out:
        OBD_FREE_PTR(flp->lg_fh_list);
        OBD_FREE_PTR(flp);
        RETURN(rc);
}

static int ll_commit_layout(struct inode *inode,
                            const struct nfsd4_pnfs_layoutcommit_arg *arg,
                            struct nfsd4_pnfs_layoutcommit_res *res)
{
        return 0;
}

static int ll_pnfs_ds_get_state(struct inode *inode, struct knfsd_fh *fh,
                                struct pnfs_get_state *state)
{
        return 0;
}

struct pnfs_export_operations lustre_pnfs_export_ops = {
        .layout_type     = ll_layout_type,
        .get_device_iter = ll_get_device_iter,
        .get_device_info = ll_get_deviceinfo,
        .layout_get      = ll_get_layout,
        .layout_commit   = ll_commit_layout,
        .get_state       = ll_pnfs_ds_get_state,
};
#endif

