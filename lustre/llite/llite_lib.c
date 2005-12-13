/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Lustre Light Super operations
 *
 *  Copyright (c) 2002-2005 Cluster File Systems, Inc.
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

#define DEBUG_SUBSYSTEM S_LLITE

#include <linux/module.h>
#include <linux/types.h>
#include <linux/random.h>
#include <linux/version.h>

#include <linux/lustre_lite.h>
#include <linux/lustre_ha.h>
#include <linux/lustre_ver.h>
#include <linux/lustre_dlm.h>
#include <linux/lprocfs_status.h>
#include "llite_internal.h"

kmem_cache_t *ll_file_data_slab;

LIST_HEAD(ll_super_blocks);
spinlock_t ll_sb_lock = SPIN_LOCK_UNLOCKED;

extern struct address_space_operations ll_aops;
extern struct address_space_operations ll_dir_aops;

#ifndef log2
#define log2(n) ffz(~(n))
#endif

/* We need to have some extra twiddling here because some systems have
 * no random state when they start up. */
static void
lustre_generate_random_uuid(class_uuid_t uuid)
{
        struct timeval t;
        int *i, j, k;

        ENTRY;
        LASSERT(sizeof(class_uuid_t) % sizeof(*i) == 0);

        j = jiffies;
        do_gettimeofday(&t);
        k = t.tv_usec;

        generate_random_uuid(uuid);

        for (i = (int *)uuid; (char *)i < (char *)uuid + sizeof(class_uuid_t); i++) {
                *i ^= j ^ k;
                j = ((j << 8) & 0xffffff00) | ((j >> 24) & 0x000000ff);
                k = ((k >> 8) & 0x00ffffff) | ((k << 24) & 0xff000000);
        }

        EXIT;
}

struct ll_sb_info *lustre_init_sbi(struct super_block *sb)
{
        struct ll_sb_info *sbi = NULL;
        class_uuid_t uuid;
        ENTRY;

        OBD_ALLOC(sbi, sizeof(*sbi));
        if (!sbi)
                RETURN(NULL);

        spin_lock_init(&sbi->ll_lock);
        spin_lock_init(&sbi->ll_lco.lco_lock);
        INIT_LIST_HEAD(&sbi->ll_pglist);
        sbi->ll_pglist_gen = 0;
        if (num_physpages >> (20 - PAGE_SHIFT) < 512)
                sbi->ll_async_page_max = num_physpages / 2;
        else
                sbi->ll_async_page_max = (num_physpages / 4) * 3;
        sbi->ll_ra_info.ra_max_pages = min(num_physpages / 8,
                                           SBI_DEFAULT_READAHEAD_MAX);

        INIT_LIST_HEAD(&sbi->ll_conn_chain);
        INIT_HLIST_HEAD(&sbi->ll_orphan_dentry_list);
        ll_s2sbi_nocast(sb) = sbi;

        lustre_generate_random_uuid(uuid);
        class_uuid_unparse(uuid, &sbi->ll_sb_uuid);
        CDEBUG(D_HA, "generated uuid: %s\n", sbi->ll_sb_uuid.uuid);

        spin_lock(&ll_sb_lock);
        list_add_tail(&sbi->ll_list, &ll_super_blocks);
        spin_unlock(&ll_sb_lock);
        RETURN(sbi);
}

void lustre_free_sbi(struct super_block *sb)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        ENTRY;

        if (sbi != NULL) {
                spin_lock(&ll_sb_lock);
                list_del(&sbi->ll_list);
                spin_unlock(&ll_sb_lock);
                OBD_FREE(sbi, sizeof(*sbi));
        }
        ll_s2sbi_nocast(sb) = NULL;
        EXIT;
}

int lustre_common_fill_super(struct super_block *sb, char *mdc, char *osc)
{
        struct inode *root = 0;
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        struct obd_device *obd;
        struct ll_fid rootfid;
        struct obd_statfs osfs;
        struct ptlrpc_request *request = NULL;
        struct lustre_handle osc_conn = {0, };
        struct lustre_handle mdc_conn = {0, };
        struct lustre_md md;
        struct obd_connect_data *data = NULL;
        int err;
        ENTRY;

        obd = class_name2obd(mdc);
        if (!obd) {
                CERROR("MDC %s: not setup or attached\n", mdc);
                RETURN(-EINVAL);
        }

        OBD_ALLOC(data, sizeof(*data));
        if (data == NULL)
                RETURN(-ENOMEM);

        if (proc_lustre_fs_root) {
                err = lprocfs_register_mountpoint(proc_lustre_fs_root, sb,
                                                  osc, mdc);
                if (err < 0)
                        CERROR("could not register mount in /proc/lustre");
        }

        /* indicate that inodebits locking is supported by this client */
        data->ocd_connect_flags |= OBD_CONNECT_IBITS;
        data->ocd_ibits_known = MDS_INODELOCK_FULL;

        if (sb->s_flags & MS_RDONLY)
                data->ocd_connect_flags |= OBD_CONNECT_RDONLY;
        if (sbi->ll_flags & LL_SBI_USER_XATTR)
                data->ocd_connect_flags |= OBD_CONNECT_XATTR;
        data->ocd_connect_flags |= OBD_CONNECT_ACL | OBD_CONNECT_JOIN;

        if (sbi->ll_flags & LL_SBI_FLOCK) {
                sbi->ll_fop = &ll_file_operations_flock;
        } else {
                sbi->ll_fop = &ll_file_operations;
        }

        data->ocd_connect_flags |= OBD_CONNECT_VERSION;
        data->ocd_version = LUSTRE_VERSION_CODE;

        err = obd_connect(&mdc_conn, obd, &sbi->ll_sb_uuid, data);
        if (err == -EBUSY) {
                CERROR("An MDS (mdc %s) is performing recovery, of which this"
                       " client is not a part.  Please wait for recovery to "
                       "complete, abort, or time out.\n", mdc);
                GOTO(out, err);
        } else if (err) {
                CERROR("cannot connect to %s: rc = %d\n", mdc, err);
                GOTO(out, err);
        }
        sbi->ll_mdc_exp = class_conn2export(&mdc_conn);

        err = obd_statfs(obd, &osfs, jiffies - HZ);
        if (err)
                GOTO(out_mdc, err);

        /* async connect is surely finished by now */
        *data = class_exp2cliimp(sbi->ll_mdc_exp)->imp_connect_data;

        LASSERT(osfs.os_bsize);
        sb->s_blocksize = osfs.os_bsize;
        sb->s_blocksize_bits = log2(osfs.os_bsize);
        sb->s_magic = LL_SUPER_MAGIC;
        sb->s_maxbytes = PAGE_CACHE_MAXBYTES;
        sbi->ll_namelen = osfs.os_namelen;

        if ((sbi->ll_flags & LL_SBI_USER_XATTR) &&
            !(data->ocd_connect_flags & OBD_CONNECT_XATTR)) {
                LCONSOLE_INFO("Disabling user_xattr feature because "
                              "it is not supported on the server\n"); 
                sbi->ll_flags &= ~LL_SBI_USER_XATTR;
        }

        if (data->ocd_connect_flags & OBD_CONNECT_ACL) {
#ifdef MS_POSIXACL
                sb->s_flags |= MS_POSIXACL;
#endif
                sbi->ll_flags |= LL_SBI_ACL;
        } else
                sbi->ll_flags &= ~LL_SBI_ACL;

        if (data->ocd_connect_flags & OBD_CONNECT_JOIN)
                sbi->ll_flags |= LL_SBI_JOIN;

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,6,0))
        /* We set sb->s_dev equal on all lustre clients in order to support
         * NFS export clustering.  NFSD requires that the FSID be the same
         * on all clients. */
        /* s_dev is also used in lt_compare() to compare two fs, but that is
         * only a node-local comparison. */
        sb->s_dev = get_uuid2int(sbi2mdc(sbi)->cl_import->imp_target_uuid.uuid,
                         strlen(sbi2mdc(sbi)->cl_import->imp_target_uuid.uuid));
#endif

        obd = class_name2obd(osc);
        if (!obd) {
                CERROR("OSC %s: not setup or attached\n", osc);
                GOTO(out_mdc, err);
        }

        data->ocd_connect_flags =
                OBD_CONNECT_GRANT|OBD_CONNECT_VERSION|OBD_CONNECT_REQPORTAL;

        CDEBUG(D_RPCTRACE, "ocd_connect_flags: "LPX64" ocd_version: %d "
               "ocd_grant: %d\n", data->ocd_connect_flags,
               data->ocd_version, data->ocd_grant);

        obd->obd_upcall.onu_owner = &sbi->ll_lco;
        obd->obd_upcall.onu_upcall = ll_ocd_update;

        err = obd_connect(&osc_conn, obd, &sbi->ll_sb_uuid, data);
        if (err == -EBUSY) {
                CERROR("An OST (osc %s) is performing recovery, of which this"
                       " client is not a part.  Please wait for recovery to "
                       "complete, abort, or time out.\n", osc);
                GOTO(out, err);
        } else if (err) {
                CERROR("cannot connect to %s: rc = %d\n", osc, err);
                GOTO(out_mdc, err);
        }
        sbi->ll_osc_exp = class_conn2export(&osc_conn);
        spin_lock(&sbi->ll_lco.lco_lock);
        sbi->ll_lco.lco_flags = data->ocd_connect_flags;
        spin_unlock(&sbi->ll_lco.lco_lock);

        mdc_init_ea_size(sbi->ll_mdc_exp, sbi->ll_osc_exp);

        if (!ll_async_page_slab) {
                ll_async_page_slab_size =
                        size_round(sizeof(struct ll_async_page)) +
                        obd_prep_async_page(sbi->ll_osc_exp, NULL, NULL, NULL,
                                            0, NULL, NULL, NULL);
                ll_async_page_slab = kmem_cache_create("ll_async_page",
                                                       ll_async_page_slab_size,
                                                       0, 0, NULL, NULL);
                if (!ll_async_page_slab)
                        GOTO(out_osc, -ENOMEM);
        }

        err = mdc_getstatus(sbi->ll_mdc_exp, &rootfid);
        if (err) {
                CERROR("cannot mds_connect: rc = %d\n", err);
                GOTO(out_osc, err);
        }
        CDEBUG(D_SUPER, "rootfid "LPU64"\n", rootfid.id);
        sbi->ll_rootino = rootfid.id;

        sb->s_op = &lustre_super_operations;

        /* make root inode
         * XXX: move this to after cbd setup? */
        err = mdc_getattr(sbi->ll_mdc_exp, &rootfid,
                          OBD_MD_FLGETATTR | OBD_MD_FLBLOCKS |
                          (sbi->ll_flags & LL_SBI_ACL ? OBD_MD_FLACL : 0),
                          0, &request);
        if (err) {
                CERROR("mdc_getattr failed for root: rc = %d\n", err);
                GOTO(out_osc, err);
        }

        err = mdc_req2lustre_md(request, 0, sbi->ll_osc_exp, &md);
        if (err) {
                CERROR("failed to understand root inode md: rc = %d\n",err);
                ptlrpc_req_finished (request);
                GOTO(out_osc, err);
        }

        LASSERT(sbi->ll_rootino != 0);
        root = ll_iget(sb, sbi->ll_rootino, &md);

        ptlrpc_req_finished(request);

        if (root == NULL || is_bad_inode(root)) {
                mdc_free_lustre_md(sbi->ll_osc_exp, &md);
                CERROR("lustre_lite: bad iget4 for root\n");
                GOTO(out_root, err = -EBADF);
        }

        err = ll_close_thread_start(&sbi->ll_lcq);
        if (err) {
                CERROR("cannot start close thread: rc %d\n", err);
                GOTO(out_root, err);
        }

        /* making vm readahead 0 for 2.4.x. In the case of 2.6.x,
           backing dev info assigned to inode mapping is used for
           determining maximal readahead. */
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,6,0)) && \
    !defined(KERNEL_HAS_AS_MAX_READAHEAD)
        /* bug 2805 - set VM readahead to zero */
        vm_max_readahead = vm_min_readahead = 0;
#endif

        sb->s_root = d_alloc_root(root);
        if (data != NULL)
                OBD_FREE(data, sizeof(*data));
        RETURN(err);

out_root:
        if (root)
                iput(root);
out_osc:
        obd_disconnect(sbi->ll_osc_exp);
out_mdc:
        obd_disconnect(sbi->ll_mdc_exp);
out:
        if (data != NULL)
                OBD_FREE(data, sizeof(*data));
        lprocfs_unregister_mountpoint(sbi);
        RETURN(err);
}

int ll_get_max_mdsize(struct ll_sb_info *sbi, int *lmmsize)
{
        int size, rc;

        *lmmsize = obd_size_diskmd(sbi->ll_osc_exp, NULL);
        size = sizeof(int);
        rc = obd_get_info(sbi->ll_mdc_exp, strlen("max_easize"), "max_easize", 
                          &size, lmmsize);
        if (rc) 
                CERROR("Get max mdsize error rc %d \n", rc);
        
        RETURN(rc);
}

void ll_dump_inode(struct inode *inode)
{
        struct list_head *tmp;
        int dentry_count = 0;

        LASSERT(inode != NULL);

        list_for_each(tmp, &inode->i_dentry)
                dentry_count++;

        CERROR("inode %p dump: dev=%s ino=%lu mode=%o count=%u, %d dentries\n",
               inode, ll_i2mdcexp(inode)->exp_obd->obd_name, inode->i_ino,
               inode->i_mode, atomic_read(&inode->i_count), dentry_count);
}

void lustre_dump_dentry(struct dentry *dentry, int recur)
{
        struct list_head *tmp;
        int subdirs = 0;

        LASSERT(dentry != NULL);

        list_for_each(tmp, &dentry->d_subdirs)
                subdirs++;

        CERROR("dentry %p dump: name=%.*s parent=%.*s (%p), inode=%p, count=%u,"
               " flags=0x%x, fsdata=%p, %d subdirs\n", dentry,
               dentry->d_name.len, dentry->d_name.name,
               dentry->d_parent->d_name.len, dentry->d_parent->d_name.name,
               dentry->d_parent, dentry->d_inode, atomic_read(&dentry->d_count),
               dentry->d_flags, dentry->d_fsdata, subdirs);
        if (dentry->d_inode != NULL)
                ll_dump_inode(dentry->d_inode);

        if (recur == 0)
                return;

        list_for_each(tmp, &dentry->d_subdirs) {
                struct dentry *d = list_entry(tmp, struct dentry, d_child);
                lustre_dump_dentry(d, recur - 1);
        }
}

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
void lustre_throw_orphan_dentries(struct super_block *sb)
{
        struct hlist_node *tmp, *next;
        struct ll_sb_info *sbi = ll_s2sbi(sb);

        /* We do this to get rid of orphaned dentries. That is not really trw. */
        hlist_for_each_safe(tmp, next, &sbi->ll_orphan_dentry_list) {
                struct dentry *dentry = hlist_entry(tmp, struct dentry, d_hash);
                CWARN("found orphan dentry %.*s (%p->%p) at unmount, dumping "
                      "before and after shrink_dcache_parent\n",
                      dentry->d_name.len, dentry->d_name.name, dentry, next);
                lustre_dump_dentry(dentry, 1);
                shrink_dcache_parent(dentry);
                lustre_dump_dentry(dentry, 1);
        }
}
#else
#define lustre_throw_orphan_dentries(sb)
#endif

void lustre_common_put_super(struct super_block *sb)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        ENTRY;

        ll_close_thread_shutdown(sbi->ll_lcq);

        list_del(&sbi->ll_conn_chain);
        obd_disconnect(sbi->ll_osc_exp);

        lprocfs_unregister_mountpoint(sbi);
        if (sbi->ll_proc_root) {
                lprocfs_remove(sbi->ll_proc_root);
                sbi->ll_proc_root = NULL;
        }

        obd_disconnect(sbi->ll_mdc_exp);

        lustre_throw_orphan_dentries(sb);
        EXIT;
}

char *ll_read_opt(const char *opt, char *data)
{
        char *value;
        char *retval;
        ENTRY;

        CDEBUG(D_SUPER, "option: %s, data %s\n", opt, data);
        if (strncmp(opt, data, strlen(opt)))
                RETURN(NULL);
        if ((value = strchr(data, '=')) == NULL)
                RETURN(NULL);

        value++;
        OBD_ALLOC(retval, strlen(value) + 1);
        if (!retval) {
                CERROR("out of memory!\n");
                RETURN(NULL);
        }

        memcpy(retval, value, strlen(value)+1);
        CDEBUG(D_SUPER, "Assigned option: %s, value %s\n", opt, retval);
        RETURN(retval);
}

int ll_set_opt(const char *opt, char *data, int fl)
{
        ENTRY;

        CDEBUG(D_SUPER, "option: %s, data %s\n", opt, data);
        if (strncmp(opt, data, strlen(opt)))
                RETURN(0);
        else
                RETURN(fl);
}

void ll_options(char *options, char **ost, char **mdc, int *flags)
{
        int tmp;
        char *this_char;
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
        char *opt_ptr = options;
#endif
        ENTRY;

        if (!options) {
                EXIT;
                return;
        }

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
        for (this_char = strtok (options, ",");
             this_char != NULL;
             this_char = strtok (NULL, ","))
#else
        while ((this_char = strsep (&opt_ptr, ",")) != NULL)
#endif
        {
                CDEBUG(D_SUPER, "this_char %s\n", this_char);
                if (!*ost && (*ost = ll_read_opt(LUSTRE_OSC_NAME, this_char)))
                        continue;
                if (!*mdc && (*mdc = ll_read_opt(LUSTRE_MDC_NAME, this_char)))
                        continue;
                tmp = ll_set_opt("nolock", this_char, LL_SBI_NOLCK);
                if (tmp) {
                        *flags |= tmp;
                        continue;
                }
                tmp = ll_set_opt("flock", this_char, LL_SBI_FLOCK);
                if (tmp) {
                        *flags |= tmp;
                        continue;
                }
                tmp = ll_set_opt("noflock", this_char, LL_SBI_FLOCK);
                if (tmp) {
                        *flags &= ~tmp;
                        continue;
                }
                tmp = ll_set_opt("user_xattr", this_char, LL_SBI_USER_XATTR);
                if (tmp) {
                        *flags |= tmp;
                        continue;
                }
                tmp = ll_set_opt("nouser_xattr", this_char, LL_SBI_USER_XATTR);
                if (tmp) {
                        *flags &= ~tmp;
                        continue;
                }
                tmp = ll_set_opt("acl", this_char, LL_SBI_ACL);
                if (tmp) {
                        /* Ignore deprecated mount option.  The client will
                         * always try to mount with ACL support, whether this
                         * is used depends on whether server supports it. */
                        continue;
                }
                tmp = ll_set_opt("noacl", this_char, LL_SBI_ACL);
                if (tmp) {
                        continue;
                }
        }
        EXIT;
}

void ll_lli_init(struct ll_inode_info *lli)
{
        sema_init(&lli->lli_open_sem, 1);
        sema_init(&lli->lli_size_sem, 1);
        lli->lli_flags = 0;
        lli->lli_maxbytes = PAGE_CACHE_MAXBYTES;
        spin_lock_init(&lli->lli_lock);
        INIT_LIST_HEAD(&lli->lli_pending_write_llaps);
        lli->lli_inode_magic = LLI_INODE_MAGIC;
}

int ll_fill_super(struct super_block *sb, void *data, int silent)
{
        struct ll_sb_info *sbi;
        char *osc = NULL;
        char *mdc = NULL;
        int err;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op: sb %p\n", sb);

        sbi = lustre_init_sbi(sb);
        if (!sbi)
                RETURN(-ENOMEM);

        ll_options(data, &osc, &mdc, &sbi->ll_flags);

        if (!osc) {
                CERROR("no osc\n");
                GOTO(out, err = -EINVAL);
        }

        if (!mdc) {
                CERROR("no mdc\n");
                GOTO(out, err = -EINVAL);
        }

        err = lustre_common_fill_super(sb, mdc, osc);
out:
        if (err)
                lustre_free_sbi(sb);

        if (mdc)
                OBD_FREE(mdc, strlen(mdc) + 1);
        if (osc)
                OBD_FREE(osc, strlen(osc) + 1);

        RETURN(err);
} /* ll_read_super */

static int do_lcfg(char *cfgname, lnet_nid_t nid, int cmd,
                   char *s1, char *s2)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg    * lcfg = NULL;
        int err;
               
        CDEBUG(D_TRACE, "lcfg %s %#x %s %s\n", cfgname, cmd, s1, s2); 

        lustre_cfg_bufs_reset(&bufs, cfgname);
        if (s1) 
                lustre_cfg_bufs_set_string(&bufs, 1, s1);
        if (s2) 
                lustre_cfg_bufs_set_string(&bufs, 2, s2);

        lcfg = lustre_cfg_new(cmd, &bufs);
        lcfg->lcfg_nid = nid;
        err = class_process_config(lcfg);
        lustre_cfg_free(lcfg);
        return(err);
}

static int lustre_process_log(struct lustre_mount_data *lmd, char * profile,
                       struct config_llog_instance *cfg)
{
        struct obd_device *obd;
        struct lustre_handle mdc_conn = {0, };
        struct obd_export *exp;
        char * name = "mdc_dev";
        class_uuid_t uuid;
        struct obd_uuid mdc_uuid;
        struct llog_ctxt *ctxt;
        struct obd_connect_data ocd = { 0 };
        lnet_nid_t nid;
        int i, rc = 0, recov_bk = 1;
        int err;
        ENTRY;

        if (lmd_bad_magic(lmd))
                RETURN(-EINVAL);

        lustre_generate_random_uuid(uuid);
        class_uuid_unparse(uuid, &mdc_uuid);
        CDEBUG(D_HA, "generated uuid: %s\n", mdc_uuid.uuid);
        
        nid = lmd->lmd_nid[0];
        LASSERT(nid != LNET_NID_ANY);
        rc = do_lcfg(name, nid, LCFG_ADD_UUID, libcfs_nid2str(nid), 0);
        if (rc < 0)
                GOTO(out, rc);

        rc = do_lcfg(name, 0, LCFG_ATTACH, LUSTRE_MDC_NAME, mdc_uuid.uuid);
        if (rc < 0)
                GOTO(out_del_uuid, rc);

        rc = do_lcfg(name, 0, LCFG_SETUP, lmd->lmd_mds, libcfs_nid2str(nid));
        if (rc < 0) {
                LCONSOLE_ERROR("I couldn't establish a connection with the MDS."
                               " Check that the MDS host NID is correct and the"
                               " networks are up.\n");
                GOTO(out_detach, rc);
        }

        obd = class_name2obd(name);
        if (obd == NULL)
                GOTO(out_cleanup, rc = -EINVAL);

        /* Add the redundant MDS nids */
        for (i = 1; i < lmd->lmd_nid_count; i++) {
                nid = lmd->lmd_nid[i];
                rc = do_lcfg(name, nid, LCFG_ADD_UUID, libcfs_nid2str(nid), 0);
                if (rc) {
                        CERROR("Add uuid for %s failed %d\n", 
                               libcfs_nid2str(nid), rc);
                        continue;
                }
                rc = do_lcfg(name, 0, LCFG_ADD_CONN, libcfs_nid2str(nid), 0);
                if (rc) 
                        CERROR("Add conn for %s failed %d\n", 
                               libcfs_nid2str(nid), rc);
        }

        /* Try all connections, but only once. */
        rc = obd_set_info(obd->obd_self_export,
                          strlen("init_recov_bk"), "init_recov_bk",
                          sizeof(recov_bk), &recov_bk);
        if (rc)
                GOTO(out_cleanup, rc);

        ocd.ocd_connect_flags = OBD_CONNECT_ACL;

        rc = obd_connect(&mdc_conn, obd, &mdc_uuid, &ocd);
        if (rc) {
                CERROR("cannot connect to %s: rc = %d\n", lmd->lmd_mds, rc);
                GOTO(out_cleanup, rc);
        }

        exp = class_conn2export(&mdc_conn);

        ctxt = llog_get_context(exp->exp_obd, LLOG_CONFIG_REPL_CTXT);
#if 1
        rc = class_config_parse_llog(ctxt, profile, cfg);
#else
        /*
         * For debugging, it's useful to just dump the log
         */
        rc = class_config_dump_llog(ctxt, profile, cfg);
#endif
        switch (rc) {
        case 0:
                break;
        case -EINVAL:
                LCONSOLE_ERROR("%s: The configuration '%s' could not be read "
                               "from the MDS.  Make sure this client and the "
                               "MDS are running compatible versions of "
                               "Lustre.\n",
                               obd->obd_name, profile);
                /* fall through */
        default:
                CERROR("class_config_parse_llog failed: rc = %d\n", rc);
                break;
        }

        /* We don't so much care about errors in cleaning up the config llog
         * connection, as we have already read the config by this point. */
        err = obd_disconnect(exp);
        if (err)
                CERROR("obd_disconnect failed: rc = %d\n", err);

out_cleanup:
        err = do_lcfg(name, 0, LCFG_CLEANUP, 0, 0);
        if (err)
                CERROR("mdc_cleanup failed: rc = %d\n", err);

out_detach:
        err = do_lcfg(name, 0, LCFG_DETACH, 0, 0);
        if (err)
                CERROR("mdc_detach failed: rc = %d\n", err);

out_del_uuid:
        /* class_add_uuid adds a nid even if the same uuid exists; we might
           delete any copy here.  So they all better match. */
        for (i = 0; i < lmd->lmd_nid_count; i++) {
                nid = lmd->lmd_nid[i];
                err = do_lcfg(name, nid, LCFG_DEL_UUID, libcfs_nid2str(nid), 0);
                if (err)
                        CERROR("del MDC UUID %s failed: rc = %d\n", 
                               libcfs_nid2str(nid), err);
        }
        /* class_import_put will get rid of the additional connections */
out:
        RETURN(rc);
}

static void lustre_manual_cleanup(struct ll_sb_info *sbi)
{
        struct obd_device *obd;
        int next = 0;

        while ((obd = class_devices_in_group(&sbi->ll_sb_uuid, &next)) !=NULL) {
                class_manual_cleanup(obd);
        }

        if (sbi->ll_lmd != NULL)
                class_del_profile(sbi->ll_lmd->lmd_profile);
}

int lustre_fill_super(struct super_block *sb, void *data, int silent)
{
        struct lustre_mount_data * lmd = data;
        struct ll_sb_info *sbi;
        char *osc = NULL;
        char *mdc = NULL;
        int err;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op: sb %p\n", sb);
        if (lmd_bad_magic(lmd))
                RETURN(-EINVAL);

        sbi = lustre_init_sbi(sb);
        if (!sbi)
                RETURN(-ENOMEM);

        if (lmd->lmd_profile) {
                struct lustre_profile *lprof;
                struct config_llog_instance cfg;
                char ll_instance[sizeof(sb) * 2 + 1];

                if (lmd->lmd_mds[0] == '\0') {
                        CERROR("no mds name\n");
                        GOTO(out_free, err = -EINVAL);
                }

                OBD_ALLOC(sbi->ll_lmd, sizeof(*sbi->ll_lmd));
                if (sbi->ll_lmd == NULL)
                        GOTO(out_free, err = -ENOMEM);
                memcpy(sbi->ll_lmd, lmd, sizeof(*lmd));
                if (lmd->lmd_flags & LMD_FLG_FLOCK)
                        sbi->ll_flags |= LL_SBI_FLOCK;
                if (lmd->lmd_flags & LMD_FLG_USER_XATTR)
                        sbi->ll_flags |= LL_SBI_USER_XATTR;

                /* generate a string unique to this super, let's try
                 the address of the super itself.*/
                sprintf(ll_instance, "%p", sb);

                cfg.cfg_instance = ll_instance;
                cfg.cfg_uuid = sbi->ll_sb_uuid;
                err = lustre_process_log(lmd, lmd->lmd_profile, &cfg);
                if (err < 0) {
                        CERROR("Unable to process log: %s\n", lmd->lmd_profile);
                        GOTO(out_free, err);
                }

                lprof = class_get_profile(lmd->lmd_profile);
                if (lprof == NULL) {
                        CERROR("No profile found: %s\n", lmd->lmd_profile);
                        GOTO(out_free, err = -EINVAL);
                }
                if (osc)
                        OBD_FREE(osc, strlen(osc) + 1);
                OBD_ALLOC(osc, strlen(lprof->lp_osc) +
                          strlen(ll_instance) + 2);
                sprintf(osc, "%s-%s", lprof->lp_osc, ll_instance);

                if (mdc)
                        OBD_FREE(mdc, strlen(mdc) + 1);
                OBD_ALLOC(mdc, strlen(lprof->lp_mdc) +
                          strlen(ll_instance) + 2);
                sprintf(mdc, "%s-%s", lprof->lp_mdc, ll_instance);
        }

        if (!osc) {
                CERROR("no osc\n");
                GOTO(out_free, err = -EINVAL);
        }

        if (!mdc) {
                CERROR("no mdc\n");
                GOTO(out_free, err = -EINVAL);
        }

        err = lustre_common_fill_super(sb, mdc, osc);

        if (err)
                GOTO(out_free, err);

out_dev:
        if (mdc)
                OBD_FREE(mdc, strlen(mdc) + 1);
        if (osc)
                OBD_FREE(osc, strlen(osc) + 1);

        RETURN(err);

out_free:
        if (sbi->ll_lmd) {
                lustre_manual_cleanup(sbi);
                OBD_FREE(sbi->ll_lmd, sizeof(*sbi->ll_lmd));
        }
        lustre_free_sbi(sb);

        goto out_dev;
} /* lustre_fill_super */

void lustre_put_super(struct super_block *sb)
{
        struct obd_device *obd;
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        int force = 0;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op: sb %p\n", sb);
        obd = class_exp2obd(sbi->ll_mdc_exp);
        if (obd) {
                int next = 0;
                /* We need to set force before the lov_disconnect in
                lustre_common_put_super, since l_d cleans up osc's as well. */
                force = obd->obd_no_recov;
                while ((obd = class_devices_in_group(&sbi->ll_sb_uuid, &next))
                       !=NULL) {
                        obd->obd_force = force;
                }
        }

        lustre_common_put_super(sb);

        if (sbi->ll_lmd != NULL) {
                lustre_manual_cleanup(sbi);
                OBD_FREE(sbi->ll_lmd, sizeof(*sbi->ll_lmd));
        }

        lustre_free_sbi(sb);

        EXIT;
} /* lustre_put_super */

#ifdef HAVE_REGISTER_CACHE
#include <linux/cache_def.h>
#ifdef HAVE_CACHE_RETURN_INT
static int
#else
static void
#endif
ll_shrink_cache(int priority, unsigned int gfp_mask)
{
        struct ll_sb_info *sbi;
        int count = 0;

        list_for_each_entry(sbi, &ll_super_blocks, ll_list)
                count += llap_shrink_cache(sbi, priority);

#ifdef HAVE_CACHE_RETURN_INT
        return count;
#endif
}

struct cache_definition ll_cache_definition = {
        .name = "llap_cache",
        .shrink = ll_shrink_cache
};
#endif /* HAVE_REGISTER_CACHE */

struct inode *ll_inode_from_lock(struct ldlm_lock *lock)
{
        struct inode *inode = NULL;
        l_lock(&lock->l_resource->lr_namespace->ns_lock);
        if (lock->l_ast_data) {
                struct ll_inode_info *lli = ll_i2info(lock->l_ast_data);
                if (lli->lli_inode_magic == LLI_INODE_MAGIC) {
                        inode = igrab(lock->l_ast_data);
                } else {
                        inode = lock->l_ast_data;
                        __LDLM_DEBUG(inode->i_state & I_FREEING ?
                                     D_INFO : D_WARNING, lock,
                                     "l_ast_data %p is bogus: magic %08x",
                                     lock->l_ast_data, lli->lli_inode_magic);
                        inode = NULL;
                }
        }
        l_unlock(&lock->l_resource->lr_namespace->ns_lock);
        return inode;
}

static int null_if_equal(struct ldlm_lock *lock, void *data)
{
        if (data == lock->l_ast_data) {
                lock->l_ast_data = NULL;

                if (lock->l_req_mode != lock->l_granted_mode)
                        LDLM_ERROR(lock,"clearing inode with ungranted lock");
        }

        return LDLM_ITER_CONTINUE;
}

void ll_clear_inode(struct inode *inode)
{
        struct ll_fid fid;
        struct ll_inode_info *lli = ll_i2info(inode);
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p)\n", inode->i_ino,
               inode->i_generation, inode);

        ll_inode2fid(&fid, inode);
        clear_bit(LLI_F_HAVE_MDS_SIZE_LOCK, &(ll_i2info(inode)->lli_flags));
        mdc_change_cbdata(sbi->ll_mdc_exp, &fid, null_if_equal, inode);

        if (lli->lli_smd) {
                obd_change_cbdata(sbi->ll_osc_exp, lli->lli_smd,
                                  null_if_equal, inode);

                obd_free_memmd(sbi->ll_osc_exp, &lli->lli_smd);
                lli->lli_smd = NULL;
        }

        if (lli->lli_symlink_name) {
                OBD_FREE(lli->lli_symlink_name,
                         strlen(lli->lli_symlink_name) + 1);
                lli->lli_symlink_name = NULL;
        }

#ifdef CONFIG_FS_POSIX_ACL
        if (lli->lli_posix_acl) {
                LASSERT(atomic_read(&lli->lli_posix_acl->a_refcount) == 1);
                posix_acl_release(lli->lli_posix_acl);
                lli->lli_posix_acl = NULL;
        }
#endif

        lli->lli_inode_magic = LLI_INODE_DEAD;

        EXIT;
}

/* If this inode has objects allocated to it (lsm != NULL), then the OST
 * object(s) determine the file size and mtime.  Otherwise, the MDS will
 * keep these values until such a time that objects are allocated for it.
 * We do the MDS operations first, as it is checking permissions for us.
 * We don't to the MDS RPC if there is nothing that we want to store there,
 * otherwise there is no harm in updating mtime/atime on the MDS if we are
 * going to do an RPC anyways.
 *
 * If we are doing a truncate, we will send the mtime and ctime updates
 * to the OST with the punch RPC, otherwise we do an explicit setattr RPC.
 * I don't believe it is possible to get e.g. ATTR_MTIME_SET and ATTR_SIZE
 * at the same time.
 */
int ll_setattr_raw(struct inode *inode, struct iattr *attr)
{
        struct ll_inode_info *lli = ll_i2info(inode);
        struct lov_stripe_md *lsm = lli->lli_smd;
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct ptlrpc_request *request = NULL;
        struct mdc_op_data op_data;
        int ia_valid = attr->ia_valid;
        int rc = 0;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu valid %x\n", inode->i_ino,
               attr->ia_valid);
        lprocfs_counter_incr(ll_i2sbi(inode)->ll_stats, LPROC_LL_SETATTR);

        if (ia_valid & ATTR_SIZE) {
                if (attr->ia_size > ll_file_maxbytes(inode)) {
                        CDEBUG(D_INODE, "file too large %llu > "LPU64"\n",
                               attr->ia_size, ll_file_maxbytes(inode));
                        RETURN(-EFBIG);
                }

                attr->ia_valid |= ATTR_MTIME | ATTR_CTIME;
        }

        /* POSIX: check before ATTR_*TIME_SET set (from inode_change_ok) */
        if (ia_valid & (ATTR_MTIME_SET | ATTR_ATIME_SET)) {
                if (current->fsuid != inode->i_uid && !capable(CAP_FOWNER))
                        RETURN(-EPERM);
        }

        /* We mark all of the fields "set" so MDS/OST does not re-set them */
        if (attr->ia_valid & ATTR_CTIME) {
                attr->ia_ctime = CURRENT_TIME;
                attr->ia_valid |= ATTR_CTIME_SET;
        }
        if (!(ia_valid & ATTR_ATIME_SET) && (attr->ia_valid & ATTR_ATIME)) {
                attr->ia_atime = CURRENT_TIME;
                attr->ia_valid |= ATTR_ATIME_SET;
        }
        if (!(ia_valid & ATTR_MTIME_SET) && (attr->ia_valid & ATTR_MTIME)) {
                attr->ia_mtime = CURRENT_TIME;
                attr->ia_valid |= ATTR_MTIME_SET;
        }

        if (attr->ia_valid & (ATTR_MTIME | ATTR_CTIME))
                CDEBUG(D_INODE, "setting mtime %lu, ctime %lu, now = %lu\n",
                       LTIME_S(attr->ia_mtime), LTIME_S(attr->ia_ctime),
                       CURRENT_SECONDS);


        /* NB: ATTR_SIZE will only be set after this point if the size
         * resides on the MDS, ie, this file has no objects. */
        if (lsm)
                attr->ia_valid &= ~ATTR_SIZE;

        /* If only OST attributes being set on objects, don't do MDS RPC.
         * In that case, we need to check permissions and update the local
         * inode ourselves so we can call obdo_from_inode() always. */
        if (ia_valid & (lsm ? ~(ATTR_SIZE | ATTR_FROM_OPEN | ATTR_RAW) : ~0)) {
                struct lustre_md md;
                ll_prepare_mdc_op_data(&op_data, inode, NULL, NULL, 0, 0);

                rc = mdc_setattr(sbi->ll_mdc_exp, &op_data,
                                 attr, NULL, 0, NULL, 0, &request);

                if (rc) {
                        ptlrpc_req_finished(request);
                        if (rc != -EPERM && rc != -EACCES)
                                CERROR("mdc_setattr fails: rc = %d\n", rc);
                        RETURN(rc);
                }

                rc = mdc_req2lustre_md(request, 0, sbi->ll_osc_exp, &md);
                if (rc) {
                        ptlrpc_req_finished(request);
                        RETURN(rc);
                }

                /* We call inode_setattr to adjust timestamps.
                 * If there is at least some data in file, we cleared ATTR_SIZE
                 * above to avoid invoking vmtruncate, otherwise it is important
                 * to call vmtruncate in inode_setattr to update inode->i_size
                 * (bug 6196) */
                rc = inode_setattr(inode, attr);

                ll_update_inode(inode, &md);
                ptlrpc_req_finished(request);

                if (!lsm || !S_ISREG(inode->i_mode)) {
                        CDEBUG(D_INODE, "no lsm: not setting attrs on OST\n");
                        RETURN(rc);
                }
        } else {
                /* The OST doesn't check permissions, but the alternative is
                 * a gratuitous RPC to the MDS.  We already rely on the client
                 * to do read/write/truncate permission checks, so is mtime OK?
                 */
                if (ia_valid & (ATTR_MTIME | ATTR_ATIME)) {
                        /* from sys_utime() */
                        if (!(ia_valid & (ATTR_MTIME_SET | ATTR_ATIME_SET))) {
                                if (current->fsuid != inode->i_uid &&
                                    (rc=ll_permission(inode,MAY_WRITE,NULL))!=0)
                                        RETURN(rc);
                        } else {
                                /* from inode_change_ok() */
                                if (current->fsuid != inode->i_uid &&
                                    !capable(CAP_FOWNER))
                                        RETURN(-EPERM);
                        }
                }

                /* Won't invoke vmtruncate, as we already cleared ATTR_SIZE */
                rc = inode_setattr(inode, attr);
        }

        /* We really need to get our PW lock before we change inode->i_size.
         * If we don't we can race with other i_size updaters on our node, like
         * ll_file_read.  We can also race with i_size propogation to other
         * nodes through dirtying and writeback of final cached pages.  This
         * last one is especially bad for racing o_append users on other
         * nodes. */
        if (ia_valid & ATTR_SIZE) {
                ldlm_policy_data_t policy = { .l_extent = {attr->ia_size,
                                                           OBD_OBJECT_EOF } };
                struct lustre_handle lockh = { 0 };
                int err, ast_flags = 0;
                /* XXX when we fix the AST intents to pass the discard-range
                 * XXX extent, make ast_flags always LDLM_AST_DISCARD_DATA
                 * XXX here. */
                if (attr->ia_size == 0)
                        ast_flags = LDLM_AST_DISCARD_DATA;

                up(&inode->i_sem);
                UP_WRITE_I_ALLOC_SEM(inode);
                rc = ll_extent_lock(NULL, inode, lsm, LCK_PW, &policy, &lockh,
                                    ast_flags);
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
                DOWN_WRITE_I_ALLOC_SEM(inode);
                down(&inode->i_sem);
#else
                down(&inode->i_sem);
                DOWN_WRITE_I_ALLOC_SEM(inode);
#endif
                if (rc != 0)
                        RETURN(rc);

                /* Only ll_inode_size_lock is taken at this level.
                 * lov_stripe_lock() is grabbed by ll_truncate() only over
                 * call to obd_adjust_kms().  If vmtruncate returns 0, then
                 * ll_truncate dropped ll_inode_size_lock() */
                ll_inode_size_lock(inode, 0);
                rc = vmtruncate(inode, attr->ia_size);
                if (rc != 0) {
                        LASSERT(atomic_read(&lli->lli_size_sem.count) <= 0);
                        ll_inode_size_unlock(inode, 0);
                }

                err = ll_extent_unlock(NULL, inode, lsm, LCK_PW, &lockh);
                if (err) {
                        CERROR("ll_extent_unlock failed: %d\n", err);
                        if (!rc)
                                rc = err;
                }
        } else if (ia_valid & (ATTR_MTIME | ATTR_MTIME_SET)) {
                obd_flag flags;
                struct obdo oa;

                CDEBUG(D_INODE, "set mtime on OST inode %lu to %lu\n",
                       inode->i_ino, LTIME_S(attr->ia_mtime));
                
                oa.o_id = lsm->lsm_object_id;
                oa.o_valid = OBD_MD_FLID;

                flags = OBD_MD_FLTYPE | OBD_MD_FLATIME |
                        OBD_MD_FLMTIME | OBD_MD_FLCTIME |
                        OBD_MD_FLFID | OBD_MD_FLGENER;
                
                obdo_from_inode(&oa, inode, flags);
                rc = obd_setattr(sbi->ll_osc_exp, &oa, lsm, NULL);
                if (rc)
                        CERROR("obd_setattr fails: rc=%d\n", rc);
        }
        RETURN(rc);
}

int ll_setattr(struct dentry *de, struct iattr *attr)
{
        LBUG(); /* code is unused, but leave this in case of VFS changes */
        RETURN(-ENOSYS);
}

int ll_statfs_internal(struct super_block *sb, struct obd_statfs *osfs,
                       unsigned long max_age)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        struct obd_statfs obd_osfs;
        int rc;
        ENTRY;

        rc = obd_statfs(class_exp2obd(sbi->ll_mdc_exp), osfs, max_age);
        if (rc) {
                CERROR("mdc_statfs fails: rc = %d\n", rc);
                RETURN(rc);
        }

        osfs->os_type = sb->s_magic;

        CDEBUG(D_SUPER, "MDC blocks "LPU64"/"LPU64" objects "LPU64"/"LPU64"\n",
               osfs->os_bavail, osfs->os_blocks, osfs->os_ffree,osfs->os_files);

        rc = obd_statfs(class_exp2obd(sbi->ll_osc_exp), &obd_osfs, max_age);
        if (rc) {
                CERROR("obd_statfs fails: rc = %d\n", rc);
                RETURN(rc);
        }

        CDEBUG(D_SUPER, "OSC blocks "LPU64"/"LPU64" objects "LPU64"/"LPU64"\n",
               obd_osfs.os_bavail, obd_osfs.os_blocks, obd_osfs.os_ffree,
               obd_osfs.os_files);

        osfs->os_blocks = obd_osfs.os_blocks;
        osfs->os_bfree = obd_osfs.os_bfree;
        osfs->os_bavail = obd_osfs.os_bavail;

        /* If we don't have as many objects free on the OST as inodes
         * on the MDS, we reduce the total number of inodes to
         * compensate, so that the "inodes in use" number is correct.
         */
        if (obd_osfs.os_ffree < osfs->os_ffree) {
                osfs->os_files = (osfs->os_files - osfs->os_ffree) +
                        obd_osfs.os_ffree;
                osfs->os_ffree = obd_osfs.os_ffree;
        }

        RETURN(rc);
}

int ll_statfs(struct super_block *sb, struct kstatfs *sfs)
{
        struct obd_statfs osfs;
        int rc;

        CDEBUG(D_VFSTRACE, "VFS Op:\n");
        lprocfs_counter_incr(ll_s2sbi(sb)->ll_stats, LPROC_LL_STAFS);

        /* For now we will always get up-to-date statfs values, but in the
         * future we may allow some amount of caching on the client (e.g.
         * from QOS or lprocfs updates). */
        rc = ll_statfs_internal(sb, &osfs, jiffies - 1);
        if (rc)
                return rc;

        statfs_unpack(sfs, &osfs);

        if (sizeof(sfs->f_blocks) == 4) {
                while (osfs.os_blocks > ~0UL) {
                        sfs->f_bsize <<= 1;

                        osfs.os_blocks >>= 1;
                        osfs.os_bfree >>= 1;
                        osfs.os_bavail >>= 1;
                }
        }

        sfs->f_blocks = osfs.os_blocks;
        sfs->f_bfree = osfs.os_bfree;
        sfs->f_bavail = osfs.os_bavail;

        return 0;
}

void ll_inode_size_lock(struct inode *inode, int lock_lsm)
{
        struct ll_inode_info *lli;
        struct lov_stripe_md *lsm;

        lli = ll_i2info(inode);
        LASSERT(lli->lli_size_sem_owner != current);
        down(&lli->lli_size_sem);
        LASSERT(lli->lli_size_sem_owner == NULL);
        lli->lli_size_sem_owner = current;
        lsm = lli->lli_smd;
        LASSERTF(lsm != NULL || lock_lsm == 0, "lsm %p, lock_lsm %d\n",
                 lsm, lock_lsm);
        if (lock_lsm)
                lov_stripe_lock(lsm);
}

void ll_inode_size_unlock(struct inode *inode, int unlock_lsm)
{
        struct ll_inode_info *lli;
        struct lov_stripe_md *lsm;

        lli = ll_i2info(inode);
        lsm = lli->lli_smd;
        LASSERTF(lsm != NULL || unlock_lsm == 0, "lsm %p, lock_lsm %d\n",
                 lsm, unlock_lsm);
        if (unlock_lsm)
                lov_stripe_unlock(lsm);
        LASSERT(lli->lli_size_sem_owner == current);
        lli->lli_size_sem_owner = NULL;
        up(&lli->lli_size_sem);
}

static void ll_replace_lsm(struct inode *inode, struct lov_stripe_md *lsm)
{
        struct ll_inode_info *lli = ll_i2info(inode);
 
        dump_lsm(D_INODE, lsm);
        dump_lsm(D_INODE, lli->lli_smd); 
        LASSERTF(lsm->lsm_magic == LOV_MAGIC_JOIN, 
                 "lsm must be joined lsm %p\n", lsm);
        obd_free_memmd(ll_i2obdexp(inode), &lli->lli_smd);
        CDEBUG(D_INODE, "replace lsm %p to lli_smd %p for inode %lu%u(%p)\n",
               lsm, lli->lli_smd, inode->i_ino, inode->i_generation, inode);
        lli->lli_smd = lsm;
        lli->lli_maxbytes = lsm->lsm_maxbytes;
        if (lli->lli_maxbytes > PAGE_CACHE_MAXBYTES)
                lli->lli_maxbytes = PAGE_CACHE_MAXBYTES;
}

void ll_update_inode(struct inode *inode, struct lustre_md *md)
{
        struct ll_inode_info *lli = ll_i2info(inode);
        struct mds_body *body = md->body;
        struct lov_stripe_md *lsm = md->lsm;

        LASSERT ((lsm != NULL) == ((body->valid & OBD_MD_FLEASIZE) != 0));
        if (lsm != NULL) {
                if (lli->lli_smd == NULL) {
                        if (lsm->lsm_magic != LOV_MAGIC && 
                            lsm->lsm_magic != LOV_MAGIC_JOIN) {
                                dump_lsm(D_ERROR, lsm);
                                LBUG();
                        }
                        CDEBUG(D_INODE, "adding lsm %p to inode %lu/%u(%p)\n",
                               lsm, inode->i_ino, inode->i_generation, inode);
                        /* ll_inode_size_lock() requires it is only called
                         * with lli_smd != NULL or lock_lsm == 0 or we can
                         * race between lock/unlock.  bug 9547 */
                        lli->lli_smd = lsm;
                        lli->lli_maxbytes = lsm->lsm_maxbytes;
                        if (lli->lli_maxbytes > PAGE_CACHE_MAXBYTES)
                                lli->lli_maxbytes = PAGE_CACHE_MAXBYTES;
                } else {
                        if (lli->lli_smd->lsm_magic == lsm->lsm_magic &&
                             lli->lli_smd->lsm_stripe_count == 
                                        lsm->lsm_stripe_count) {
                                if (lov_stripe_md_cmp(lli->lli_smd, lsm)) {
                                        CERROR("lsm mismatch for inode %ld\n",
                                                inode->i_ino);
                                        CERROR("lli_smd:\n");
                                        dump_lsm(D_ERROR, lli->lli_smd);
                                        CERROR("lsm:\n");
                                        dump_lsm(D_ERROR, lsm);
                                        LBUG();
                                }
                        } else 
                                ll_replace_lsm(inode, lsm);
                }
                /* bug 2844 - limit i_blksize for broken user-space apps */
                LASSERTF(lsm->lsm_xfersize != 0, "%lu\n", lsm->lsm_xfersize);
                inode->i_blksize = min(lsm->lsm_xfersize, LL_MAX_BLKSIZE);
                if (lli->lli_smd != lsm)
                        obd_free_memmd(ll_i2obdexp(inode), &lsm);
        } else {
                inode->i_blksize = max(inode->i_blksize,
                                       inode->i_sb->s_blocksize);
        }

#ifdef CONFIG_FS_POSIX_ACL
        LASSERT(!md->posix_acl || (body->valid & OBD_MD_FLACL));
        if (body->valid & OBD_MD_FLACL) {
                spin_lock(&lli->lli_lock);
                if (lli->lli_posix_acl)
                        posix_acl_release(lli->lli_posix_acl);
                lli->lli_posix_acl = md->posix_acl;
                spin_unlock(&lli->lli_lock);
        }
#endif

        if (body->valid & OBD_MD_FLID)
                inode->i_ino = body->ino;
        if (body->valid & OBD_MD_FLATIME)
                LTIME_S(inode->i_atime) = body->atime;
        if (body->valid & OBD_MD_FLMTIME &&
            body->mtime > LTIME_S(inode->i_mtime)) {
                CDEBUG(D_INODE, "setting ino %lu mtime from %lu to "LPU64"\n",
                       inode->i_ino, LTIME_S(inode->i_mtime), body->mtime);
                LTIME_S(inode->i_mtime) = body->mtime;
        }
        if (body->valid & OBD_MD_FLCTIME &&
            body->ctime > LTIME_S(inode->i_ctime))
                LTIME_S(inode->i_ctime) = body->ctime;
        if (body->valid & OBD_MD_FLMODE)
                inode->i_mode = (inode->i_mode & S_IFMT)|(body->mode & ~S_IFMT);
        if (body->valid & OBD_MD_FLTYPE)
                inode->i_mode = (inode->i_mode & ~S_IFMT)|(body->mode & S_IFMT);
        if (body->valid & OBD_MD_FLUID)
                inode->i_uid = body->uid;
        if (body->valid & OBD_MD_FLGID)
                inode->i_gid = body->gid;
        if (body->valid & OBD_MD_FLFLAGS)
                inode->i_flags = body->flags;
        if (body->valid & OBD_MD_FLNLINK)
                inode->i_nlink = body->nlink;
        if (body->valid & OBD_MD_FLGENER)
                inode->i_generation = body->generation;
        if (body->valid & OBD_MD_FLRDEV)
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
                inode->i_rdev = body->rdev;
#else
                inode->i_rdev = old_decode_dev(body->rdev);
#endif
        if (body->valid & OBD_MD_FLSIZE)
                inode->i_size = body->size;
        if (body->valid & OBD_MD_FLBLOCKS)
                inode->i_blocks = body->blocks;

        if (body->valid & OBD_MD_FLSIZE)
                set_bit(LLI_F_HAVE_MDS_SIZE_LOCK, &lli->lli_flags);
}

#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
static struct backing_dev_info ll_backing_dev_info = {
        .ra_pages       = 0,    /* No readahead */
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,12))
        .capabilities   = 0,    /* Does contribute to dirty memory */
#else
        .memory_backed  = 0,    /* Does contribute to dirty memory */
#endif
};
#endif

void ll_read_inode2(struct inode *inode, void *opaque)
{
        struct lustre_md *md = opaque;
        struct ll_inode_info *lli = ll_i2info(inode);
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p)\n", inode->i_ino,
               inode->i_generation, inode);

        ll_lli_init(lli);

        LASSERT(!lli->lli_smd);

        /* Core attributes from the MDS first.  This is a new inode, and
         * the VFS doesn't zero times in the core inode so we have to do
         * it ourselves.  They will be overwritten by either MDS or OST
         * attributes - we just need to make sure they aren't newer. */
        LTIME_S(inode->i_mtime) = 0;
        LTIME_S(inode->i_atime) = 0;
        LTIME_S(inode->i_ctime) = 0;
        inode->i_rdev = 0;
        ll_update_inode(inode, md);

        /* OIDEBUG(inode); */

        if (S_ISREG(inode->i_mode)) {
                struct ll_sb_info *sbi = ll_i2sbi(inode);
                inode->i_op = &ll_file_inode_operations;
                inode->i_fop = sbi->ll_fop;
                inode->i_mapping->a_ops = &ll_aops;
                EXIT;
        } else if (S_ISDIR(inode->i_mode)) {
                inode->i_op = &ll_dir_inode_operations;
                inode->i_fop = &ll_dir_operations;
                inode->i_mapping->a_ops = &ll_dir_aops;
                EXIT;
        } else if (S_ISLNK(inode->i_mode)) {
                inode->i_op = &ll_fast_symlink_inode_operations;
                EXIT;
        } else {
                inode->i_op = &ll_special_inode_operations;

#if (LINUX_VERSION_CODE > KERNEL_VERSION(2,5,0))
                init_special_inode(inode, inode->i_mode,
                                   kdev_t_to_nr(inode->i_rdev));

                /* initializing backing dev info. */
                inode->i_mapping->backing_dev_info = &ll_backing_dev_info;
#else
                init_special_inode(inode, inode->i_mode, inode->i_rdev);
#endif
                lli->ll_save_ifop = inode->i_fop;

                if (S_ISCHR(inode->i_mode))
                        inode->i_fop = &ll_special_chr_inode_fops;
                else if (S_ISBLK(inode->i_mode))
                        inode->i_fop = &ll_special_blk_inode_fops;
                else if (S_ISFIFO(inode->i_mode))
                        inode->i_fop = &ll_special_fifo_inode_fops;
                else if (S_ISSOCK(inode->i_mode))
                        inode->i_fop = &ll_special_sock_inode_fops;
                EXIT;
        }
}

int ll_iocontrol(struct inode *inode, struct file *file,
                 unsigned int cmd, unsigned long arg)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct ptlrpc_request *req = NULL;
        int rc, flags = 0;
        ENTRY;

        switch(cmd) {
        case EXT3_IOC_GETFLAGS: {
                struct ll_fid fid;
                struct mds_body *body;

                ll_inode2fid(&fid, inode);
                rc = mdc_getattr(sbi->ll_mdc_exp, &fid, OBD_MD_FLFLAGS,0,&req);
                if (rc) {
                        CERROR("failure %d inode %lu\n", rc, inode->i_ino);
                        RETURN(-abs(rc));
                }

                body = lustre_msg_buf(req->rq_repmsg, 0, sizeof(*body));

                if (body->flags & S_APPEND)
                        flags |= EXT3_APPEND_FL;
                if (body->flags & S_IMMUTABLE)
                        flags |= EXT3_IMMUTABLE_FL;
                if (body->flags & S_NOATIME)
                        flags |= EXT3_NOATIME_FL;

                ptlrpc_req_finished (req);

                RETURN(put_user(flags, (int *)arg));
        }
        case EXT3_IOC_SETFLAGS: {
                struct mdc_op_data op_data;
                struct iattr attr;
                struct obdo *oa;
                struct lov_stripe_md *lsm = ll_i2info(inode)->lli_smd;

                if (get_user(flags, (int *)arg))
                        RETURN(-EFAULT);

                oa = obdo_alloc();
                if (!oa)
                        RETURN(-ENOMEM);

                ll_prepare_mdc_op_data(&op_data, inode, NULL, NULL, 0, 0);

                memset(&attr, 0x0, sizeof(attr));
                attr.ia_attr_flags = flags;
                attr.ia_valid |= ATTR_ATTR_FLAG;

                rc = mdc_setattr(sbi->ll_mdc_exp, &op_data,
                                 &attr, NULL, 0, NULL, 0, &req);
                if (rc) {
                        ptlrpc_req_finished(req);
                        if (rc != -EPERM && rc != -EACCES)
                                CERROR("mdc_setattr fails: rc = %d\n", rc);
                        obdo_free(oa);
                        RETURN(rc);
                }
                ptlrpc_req_finished(req);

                oa->o_id = lsm->lsm_object_id;
                oa->o_flags = flags;
                oa->o_valid = OBD_MD_FLID | OBD_MD_FLFLAGS;

                obdo_from_inode(oa, inode, OBD_MD_FLFID | OBD_MD_FLGENER);
                rc = obd_setattr(sbi->ll_osc_exp, oa, lsm, NULL);
                obdo_free(oa);
                if (rc) {
                        if (rc != -EPERM && rc != -EACCES)
                                CERROR("mdc_setattr fails: rc = %d\n", rc);
                        RETURN(rc);
                }

                if (flags & EXT3_APPEND_FL)
                        inode->i_flags |= S_APPEND;
                else
                        inode->i_flags &= ~S_APPEND;
                if (flags & EXT3_IMMUTABLE_FL)
                        inode->i_flags |= S_IMMUTABLE;
                else
                        inode->i_flags &= ~S_IMMUTABLE;
                if (flags & EXT3_NOATIME_FL)
                        inode->i_flags |= S_NOATIME;
                else
                        inode->i_flags &= ~S_NOATIME;

                RETURN(0);
        }
        default:
                RETURN(-ENOSYS);
        }

        RETURN(0);
}

void ll_umount_begin(struct super_block *sb)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        struct obd_device *obd;
        struct obd_ioctl_data ioc_data = { 0 };
        ENTRY;
        CDEBUG(D_VFSTRACE, "VFS Op: superblock %p count %d active %d\n", sb,
               sb->s_count, atomic_read(&sb->s_active));

        obd = class_exp2obd(sbi->ll_mdc_exp);
        if (obd == NULL) {
                CERROR("Invalid MDC connection handle "LPX64"\n",
                       sbi->ll_mdc_exp->exp_handle.h_cookie);
                EXIT;
                return;
        }
        obd->obd_no_recov = 1;
        obd_iocontrol(IOC_OSC_SET_ACTIVE, sbi->ll_mdc_exp, sizeof ioc_data,
                      &ioc_data, NULL);

        obd = class_exp2obd(sbi->ll_osc_exp);
        if (obd == NULL) {
                CERROR("Invalid LOV connection handle "LPX64"\n",
                       sbi->ll_osc_exp->exp_handle.h_cookie);
                EXIT;
                return;
        }

        obd->obd_no_recov = 1;
        obd_iocontrol(IOC_OSC_SET_ACTIVE, sbi->ll_osc_exp, sizeof ioc_data,
                      &ioc_data, NULL);

        /* Really, we'd like to wait until there are no requests outstanding,
         * and then continue.  For now, we just invalidate the requests,
         * schedule, and hope.
         */
        schedule();

        EXIT;
}

int lustre_remount_fs(struct super_block *sb, int *flags, char *data)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        int err;
        __u32 read_only;

        if ((*flags & MS_RDONLY) != (sb->s_flags & MS_RDONLY)) {
                read_only = *flags & MS_RDONLY;
                err = obd_set_info(sbi->ll_mdc_exp, strlen("read-only"),
                                   "read-only", sizeof(read_only), &read_only);
                if (err) {
                        CERROR("Failed to change the read-only flag during "
                               "remount: %d\n", err);
                        return err;
                }

                if (read_only)
                        sb->s_flags |= MS_RDONLY;
                else
                        sb->s_flags &= ~MS_RDONLY;
        }
        return 0;
}

int ll_prep_inode(struct obd_export *exp, struct inode **inode,
                  struct ptlrpc_request *req, int offset,struct super_block *sb)
{
        struct lustre_md md;
        int rc = 0;

        rc = mdc_req2lustre_md(req, offset, exp, &md);
        if (rc)
                RETURN(rc);

        if (*inode) {
                ll_update_inode(*inode, &md);
        } else {
                LASSERT(sb);
                *inode = ll_iget(sb, md.body->ino, &md);
                if (*inode == NULL || is_bad_inode(*inode)) {
                        mdc_free_lustre_md(exp, &md);
                        rc = -ENOMEM;
                        CERROR("new_inode -fatal: rc %d\n", rc);
                        GOTO(out, rc);
                }
        }

        rc = obd_checkmd(exp, ll_i2mdcexp(*inode),
                         ll_i2info(*inode)->lli_smd);
out:
        RETURN(rc);
}

char *llap_origins[] = {
        [LLAP_ORIGIN_UNKNOWN] = "--",
        [LLAP_ORIGIN_READPAGE] = "rp",
        [LLAP_ORIGIN_READAHEAD] = "ra",
        [LLAP_ORIGIN_COMMIT_WRITE] = "cw",
        [LLAP_ORIGIN_WRITEPAGE] = "wp",
};

struct ll_async_page *llite_pglist_next_llap(struct ll_sb_info *sbi,
                                             struct list_head *list)
{
        struct ll_async_page *llap;
        struct list_head *pos;

        list_for_each(pos, list) {
                if (pos == &sbi->ll_pglist)
                        return NULL;
                llap = list_entry(pos, struct ll_async_page, llap_pglist_item);
                if (llap->llap_page == NULL)
                        continue;
                return llap;
        }
        LBUG();
        return NULL;
}
