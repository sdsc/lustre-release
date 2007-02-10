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

#include <lustre_lite.h>
#include <lustre_ha.h>
#include <lustre_dlm.h>
#include <lprocfs_status.h>
#include <lustre_disk.h>
#include <lustre_param.h>
#include "llite_internal.h"

kmem_cache_t *ll_file_data_slab;

LIST_HEAD(ll_super_blocks);
spinlock_t ll_sb_lock = SPIN_LOCK_UNLOCKED;

extern struct address_space_operations ll_aops;
extern struct address_space_operations ll_dir_aops;

#ifndef log2
#define log2(n) ffz(~(n))
#endif


static struct ll_sb_info *ll_init_sbi(void)
{
        struct ll_sb_info *sbi = NULL;
        class_uuid_t uuid;
        int i;
        ENTRY;

        OBD_ALLOC(sbi, sizeof(*sbi));
        if (!sbi)
                RETURN(NULL);

        spin_lock_init(&sbi->ll_lock);
        spin_lock_init(&sbi->ll_lco.lco_lock);
        INIT_LIST_HEAD(&sbi->ll_pglist);
        if (num_physpages >> (20 - CFS_PAGE_SHIFT) < 512)
                sbi->ll_async_page_max = num_physpages / 2;
        else
                sbi->ll_async_page_max = (num_physpages / 4) * 3;
        sbi->ll_ra_info.ra_max_pages = min(num_physpages / 8,
                                           SBI_DEFAULT_READAHEAD_MAX);
        sbi->ll_ra_info.ra_max_read_ahead_whole_pages =
                                           SBI_DEFAULT_READAHEAD_WHOLE_MAX;

        INIT_LIST_HEAD(&sbi->ll_conn_chain);
        INIT_LIST_HEAD(&sbi->ll_orphan_dentry_list);

        class_generate_random_uuid(uuid);
        class_uuid_unparse(uuid, &sbi->ll_sb_uuid);
        CDEBUG(D_HA, "generated uuid: %s\n", sbi->ll_sb_uuid.uuid);

        spin_lock(&ll_sb_lock);
        list_add_tail(&sbi->ll_list, &ll_super_blocks);
        spin_unlock(&ll_sb_lock);

        INIT_LIST_HEAD(&sbi->ll_deathrow);
        spin_lock_init(&sbi->ll_deathrow_lock);
        for (i = 0; i < LL_PROCESS_HIST_MAX; i++) { 
                spin_lock_init(&sbi->ll_rw_extents_info.pp_extents[i].pp_r_hist.oh_lock);
                spin_lock_init(&sbi->ll_rw_extents_info.pp_extents[i].pp_w_hist.oh_lock);
        }

        RETURN(sbi);
}

void ll_free_sbi(struct super_block *sb)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        ENTRY;

        if (sbi != NULL) {
                spin_lock(&ll_sb_lock);
                list_del(&sbi->ll_list);
                spin_unlock(&ll_sb_lock);
                OBD_FREE(sbi, sizeof(*sbi));
        }
        EXIT;
}

static struct dentry_operations ll_d_root_ops = {
#ifdef LUSTRE_KERNEL_VERSION
        .d_compare = ll_dcompare,
#endif
};

static int client_common_fill_super(struct super_block *sb, 
                                    char *mdc, char *osc)
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

        /* indicate the features supported by this client */
        data->ocd_connect_flags = OBD_CONNECT_IBITS | OBD_CONNECT_NODEVOH |
                                  OBD_CONNECT_JOIN |
                                  OBD_CONNECT_ATTRFID | OBD_CONNECT_VERSION;
#ifdef CONFIG_FS_POSIX_ACL
        data->ocd_connect_flags |= OBD_CONNECT_ACL;
#endif
        data->ocd_ibits_known = MDS_INODELOCK_FULL;
        data->ocd_version = LUSTRE_VERSION_CODE;

        if (sb->s_flags & MS_RDONLY)
                data->ocd_connect_flags |= OBD_CONNECT_RDONLY;
        if (sbi->ll_flags & LL_SBI_USER_XATTR)
                data->ocd_connect_flags |= OBD_CONNECT_XATTR;

#ifdef HAVE_MS_FLOCK_LOCK
        /* force vfs to use lustre handler for flock() calls - bug 10743 */
        sb->s_flags |= MS_FLOCK_LOCK;
#endif
        
        if (sbi->ll_flags & LL_SBI_FLOCK)
                sbi->ll_fop = &ll_file_operations_flock;
        else
                sbi->ll_fop = &ll_file_operations;

        err = obd_connect(&mdc_conn, obd, &sbi->ll_sb_uuid, data);
        if (err == -EBUSY) {
                LCONSOLE_ERROR("An MDT (mdc %s) is performing recovery, of "
                               "which this client is not a part.  Please wait "
                               "for recovery to complete, abort, or "
                               "time out.\n", mdc);
                GOTO(out, err);
        } else if (err) {
                CERROR("cannot connect to %s: rc = %d\n", mdc, err);
                GOTO(out, err);
        }
        sbi->ll_mdc_exp = class_conn2export(&mdc_conn);

        err = obd_statfs(obd, &osfs, cfs_time_current_64() - HZ);
        if (err)
                GOTO(out_mdc, err);

        /* MDC connect is surely finished by now because we actually sent
         * a statfs RPC, otherwise obd_connect() is asynchronous. */
        *data = class_exp2cliimp(sbi->ll_mdc_exp)->imp_connect_data;

        LASSERT(osfs.os_bsize);
        sb->s_blocksize = osfs.os_bsize;
        sb->s_blocksize_bits = log2(osfs.os_bsize);
        sb->s_magic = LL_SUPER_MAGIC;

        /* for bug 11559. in $LINUX/fs/read_write.c, function do_sendfile():
         *         retval = in_file->f_op->sendfile(...);
         *         if (*ppos > max)
         *                 retval = -EOVERFLOW;
         *
         * it will check if *ppos is greater than max. However, max equals to
         * s_maxbytes, which is a negative integer in a x86_64 box since loff_t
         * has been defined as a signed long long ineger in linux kernel. */
#if BITS_PER_LONG == 64
        sb->s_maxbytes = PAGE_CACHE_MAXBYTES >> 1;
#else
        sb->s_maxbytes = PAGE_CACHE_MAXBYTES;
#endif
        sbi->ll_namelen = osfs.os_namelen;
        sbi->ll_max_rw_chunk = LL_DEFAULT_MAX_RW_CHUNK;

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
        sb->s_dev = get_uuid2int(sbi2mdc(sbi)->cl_target_uuid.uuid,
                                 strlen(sbi2mdc(sbi)->cl_target_uuid.uuid));
#endif

        obd = class_name2obd(osc);
        if (!obd) {
                CERROR("OSC %s: not setup or attached\n", osc);
                GOTO(out_mdc, err = -ENODEV);
        }

        data->ocd_connect_flags = OBD_CONNECT_GRANT | OBD_CONNECT_VERSION |
                                  OBD_CONNECT_REQPORTAL | OBD_CONNECT_BRW_SIZE;

        CDEBUG(D_RPCTRACE, "ocd_connect_flags: "LPX64" ocd_version: %d "
               "ocd_grant: %d\n", data->ocd_connect_flags,
               data->ocd_version, data->ocd_grant);

        obd->obd_upcall.onu_owner = &sbi->ll_lco;
        obd->obd_upcall.onu_upcall = ll_ocd_update;
        data->ocd_brw_size = PTLRPC_MAX_BRW_PAGES << CFS_PAGE_SHIFT;


        err = obd_connect(&osc_conn, obd, &sbi->ll_sb_uuid, data);
        if (err == -EBUSY) {                                                
                LCONSOLE_ERROR("An OST (osc %s) is performing recovery, of "
                               "which this client is not a part.  Please wait "
                               "for recovery to complete, abort, or "
                               "time out.\n", osc);
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

        err = obd_prep_async_page(sbi->ll_osc_exp, NULL, NULL, NULL,
                                  0, NULL, NULL, NULL);
        if (err < 0) {
                LCONSOLE_ERROR("There are no OST's in this filesystem. "
                               "There must be at least one active OST for "
                               "a client to start.\n");
                GOTO(out_osc, err);
        }

        if (!ll_async_page_slab) {
                ll_async_page_slab_size =
                        size_round(sizeof(struct ll_async_page)) + err;
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
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
        sb->s_export_op = &lustre_export_operations;
#endif

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

        err = mdc_req2lustre_md(request, REPLY_REC_OFF, sbi->ll_osc_exp, &md);
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
        sb->s_root->d_op = &ll_d_root_ops;
        RETURN(err);

out_root:
        if (root)
                iput(root);
out_osc:
        obd_disconnect(sbi->ll_osc_exp);
        sbi->ll_osc_exp = NULL;
out_mdc:
        obd_disconnect(sbi->ll_mdc_exp);
        sbi->ll_mdc_exp = NULL;
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
        struct dentry *dentry, *next;
        struct ll_sb_info *sbi = ll_s2sbi(sb);

        /* Do this to get rid of orphaned dentries. That is not really trw. */
        list_for_each_entry_safe(dentry, next, &sbi->ll_orphan_dentry_list,
                                 d_hash) {
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

static void prune_dir_dentries(struct inode *inode)
{
        struct dentry *dentry, *prev = NULL;

        /* due to lustre specific logic, a directory
         * can have few dentries - a bug from VFS POV */
restart:
        spin_lock(&dcache_lock);
        if (!list_empty(&inode->i_dentry)) {
                dentry = list_entry(inode->i_dentry.prev,
                                    struct dentry, d_alias);
                /* in order to prevent infinite loops we
                 * break if previous dentry is busy */
                if (dentry != prev) {
                        prev = dentry;
                        dget_locked(dentry);
                        spin_unlock(&dcache_lock);

                        /* try to kill all child dentries */
                        lock_dentry(dentry);
                        shrink_dcache_parent(dentry);
                        unlock_dentry(dentry);
                        dput(dentry);

                        /* now try to get rid of current dentry */
                        d_prune_aliases(inode);
                        goto restart;
                }
        }
        spin_unlock(&dcache_lock);
}

static void prune_deathrow_one(struct ll_inode_info *lli)
{
        struct inode *inode = ll_info2i(lli);

        /* first, try to drop any dentries - they hold a ref on the inode */
        if (S_ISDIR(inode->i_mode))
                prune_dir_dentries(inode);
        else
                d_prune_aliases(inode);


        /* if somebody still uses it, leave it */
        LASSERT(atomic_read(&inode->i_count) > 0);
        if (atomic_read(&inode->i_count) > 1)
                goto out;

        CDEBUG(D_INODE, "inode %lu/%u(%d) looks a good candidate for prune\n",
               inode->i_ino,inode->i_generation, atomic_read(&inode->i_count));

        /* seems nobody uses it anymore */
        inode->i_nlink = 0;

out:
        iput(inode);
        return;
}

static void prune_deathrow(struct ll_sb_info *sbi, int try)
{
        struct ll_inode_info *lli;
        int empty;

        do {
                if (need_resched() && try)
                        break;

                if (try) {
                        if (!spin_trylock(&sbi->ll_deathrow_lock))
                                break;
                } else {
                        spin_lock(&sbi->ll_deathrow_lock);
                }

                empty = 1;
                lli = NULL;
                if (!list_empty(&sbi->ll_deathrow)) {
                        lli = list_entry(sbi->ll_deathrow.next,
                                         struct ll_inode_info,
                                         lli_dead_list);
                        list_del_init(&lli->lli_dead_list);
                        if (!list_empty(&sbi->ll_deathrow))
                                empty = 0;
                }
                spin_unlock(&sbi->ll_deathrow_lock);

                if (lli)
                        prune_deathrow_one(lli);

        } while (empty == 0);
}

void client_common_put_super(struct super_block *sb)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        ENTRY;

        ll_close_thread_shutdown(sbi->ll_lcq);

        lprocfs_unregister_mountpoint(sbi);

        /* destroy inodes in deathrow */
        prune_deathrow(sbi, 0);

        list_del(&sbi->ll_conn_chain);
        obd_disconnect(sbi->ll_osc_exp);
        sbi->ll_osc_exp = NULL;

        obd_disconnect(sbi->ll_mdc_exp);
        sbi->ll_mdc_exp = NULL;

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

static inline int ll_set_opt(const char *opt, char *data, int fl)
{
        if (strncmp(opt, data, strlen(opt)) != 0)
                return(0);
        else
                return(fl);
}

/* non-client-specific mount options are parsed in lmd_parse */
static int ll_options(char *options, int *flags)
{
        int tmp;
        char *s1 = options, *s2;
        ENTRY;

        if (!options) 
                RETURN(0);

        CDEBUG(D_CONFIG, "Parsing opts %s\n", options);

        while (*s1) {
                CDEBUG(D_SUPER, "next opt=%s\n", s1);
                tmp = ll_set_opt("nolock", s1, LL_SBI_NOLCK);
                if (tmp) {
                        *flags |= tmp;
                        goto next;
                }
                tmp = ll_set_opt("flock", s1, LL_SBI_FLOCK);
                if (tmp) {
                        *flags |= tmp;
                        goto next;
                }
                tmp = ll_set_opt("noflock", s1, LL_SBI_FLOCK);
                if (tmp) {
                        *flags &= ~tmp;
                        goto next;
                }
                tmp = ll_set_opt("user_xattr", s1, LL_SBI_USER_XATTR);
                if (tmp) {
                        *flags |= tmp;
                        goto next;
                }
                tmp = ll_set_opt("nouser_xattr", s1, LL_SBI_USER_XATTR);
                if (tmp) {
                        *flags &= ~tmp;
                        goto next;
                }
                tmp = ll_set_opt("acl", s1, LL_SBI_ACL);
                if (tmp) {
                        /* Ignore deprecated mount option.  The client will
                         * always try to mount with ACL support, whether this
                         * is used depends on whether server supports it. */
                        goto next;
                }
                tmp = ll_set_opt("noacl", s1, LL_SBI_ACL);
                if (tmp) {
                        goto next;
                }

                LCONSOLE_ERROR("Unknown option '%s', won't mount.\n", s1);
                RETURN(-EINVAL);

next:
                /* Find next opt */
                s2 = strchr(s1, ',');
                if (s2 == NULL) 
                        break;
                s1 = s2 + 1;
        }
        RETURN(0);
}
                
void ll_lli_init(struct ll_inode_info *lli)
{
        sema_init(&lli->lli_open_sem, 1);
        sema_init(&lli->lli_size_sem, 1);
        sema_init(&lli->lli_write_sem, 1);
        lli->lli_flags = 0;
        lli->lli_maxbytes = PAGE_CACHE_MAXBYTES;
        spin_lock_init(&lli->lli_lock);
        INIT_LIST_HEAD(&lli->lli_pending_write_llaps);
        lli->lli_inode_magic = LLI_INODE_MAGIC;
        sema_init(&lli->lli_och_sem, 1);
        lli->lli_mds_read_och = lli->lli_mds_write_och = NULL;
        lli->lli_mds_exec_och = NULL;
        lli->lli_open_fd_read_count = lli->lli_open_fd_write_count = 0;
        lli->lli_open_fd_exec_count = 0;
        INIT_LIST_HEAD(&lli->lli_dead_list);
}

/* COMPAT_146 */
#define MDCDEV "mdc_dev"
static int old_lustre_process_log(struct super_block *sb, char *newprofile,
                                  struct config_llog_instance *cfg)
{
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct obd_device *obd;
        struct lustre_handle mdc_conn = {0, };
        struct obd_export *exp;
        char *ptr, *mdt, *profile;
        char niduuid[10] = "mdtnid0";
        class_uuid_t uuid;
        struct obd_uuid mdc_uuid;
        struct llog_ctxt *ctxt;
        struct obd_connect_data ocd = { 0 };
        lnet_nid_t nid;
        int i, rc = 0, recov_bk = 1, failnodes = 0;
        ENTRY;

        class_generate_random_uuid(uuid);
        class_uuid_unparse(uuid, &mdc_uuid);
        CDEBUG(D_HA, "generated uuid: %s\n", mdc_uuid.uuid);
        
        /* Figure out the old mdt and profile name from new-style profile
           ("lustre" from "mds/lustre-client") */
        mdt = newprofile;
        profile = strchr(mdt, '/');
        if (profile == NULL) {
                CDEBUG(D_CONFIG, "Can't find MDT name in %s\n", newprofile);
                GOTO(out, rc = -EINVAL);
        }
        *profile = '\0';
        profile++;
        ptr = strrchr(profile, '-');
        if (ptr == NULL) {
                CDEBUG(D_CONFIG, "Can't find client name in %s\n", newprofile);
                GOTO(out, rc = -EINVAL);
        }
        *ptr = '\0';

        LCONSOLE_WARN("This looks like an old mount command; I will try to "
                      "contact MDT '%s' for profile '%s'\n", mdt, profile);

        /* Use nids from mount line: uml1,1@elan:uml2,2@elan:/lustre */
        i = 0;
        ptr = lsi->lsi_lmd->lmd_dev;
        while (class_parse_nid(ptr, &nid, &ptr) == 0) {
                rc = do_lcfg(MDCDEV, nid, LCFG_ADD_UUID, niduuid, 0,0,0);
                i++;
                /* Stop at the first failover nid */
                if (*ptr == ':') 
                        break;
        }
        if (i == 0) {
                CERROR("No valid MDT nids found.\n");
                GOTO(out, rc = -EINVAL);
        }
        failnodes++;

        rc = do_lcfg(MDCDEV, 0, LCFG_ATTACH, LUSTRE_MDC_NAME,mdc_uuid.uuid,0,0);
        if (rc < 0)
                GOTO(out_del_uuid, rc);

        rc = do_lcfg(MDCDEV, 0, LCFG_SETUP, mdt, niduuid, 0, 0);
        if (rc < 0) {
                LCONSOLE_ERROR("I couldn't establish a connection with the MDT."
                               " Check that the MDT host NID is correct and the"
                               " networks are up.\n");
                GOTO(out_detach, rc);
        }

        obd = class_name2obd(MDCDEV);
        if (obd == NULL)
                GOTO(out_cleanup, rc = -EINVAL);

        /* Add any failover nids */
        while (*ptr == ':') {
                /* New failover node */
                sprintf(niduuid, "mdtnid%d", failnodes);
                i = 0;
                while (class_parse_nid(ptr, &nid, &ptr) == 0) {
                        i++;
                        rc = do_lcfg(MDCDEV, nid, LCFG_ADD_UUID, niduuid,0,0,0);
                        if (rc)
                                CERROR("Add uuid for %s failed %d\n", 
                                       libcfs_nid2str(nid), rc);
                        if (*ptr == ':') 
                                break;
                }
                if (i > 0) {
                        rc = do_lcfg(MDCDEV, 0, LCFG_ADD_CONN, niduuid, 0, 0,0);
                        if (rc) 
                                CERROR("Add conn for %s failed %d\n", 
                                       libcfs_nid2str(nid), rc);
                        failnodes++;
                } else {
                        /* at ":/fsname" */
                        break;
                }
        }

        /* Try all connections, but only once. */
        rc = obd_set_info_async(obd->obd_self_export,
                                strlen("init_recov_bk"), "init_recov_bk",
                                sizeof(recov_bk), &recov_bk, NULL);
        if (rc)
                GOTO(out_cleanup, rc);

        /* If we don't have this then an ACL MDS will refuse the connection */
        ocd.ocd_connect_flags = OBD_CONNECT_ACL;

        rc = obd_connect(&mdc_conn, obd, &mdc_uuid, &ocd);
        if (rc) {
                CERROR("cannot connect to %s: rc = %d\n", mdt, rc);
                GOTO(out_cleanup, rc);
        }

        exp = class_conn2export(&mdc_conn);

        ctxt = llog_get_context(exp->exp_obd, LLOG_CONFIG_REPL_CTXT);
        
        cfg->cfg_flags |= CFG_F_COMPAT146;

#if 1
        rc = class_config_parse_llog(ctxt, profile, cfg);
#else
        /*
         * For debugging, it's useful to just dump the log
         */
        rc = class_config_dump_llog(ctxt, profile, cfg);
#endif
        switch (rc) {
        case 0: {
                /* Set the caller's profile name to the old-style */
                memcpy(newprofile, profile, strlen(profile) + 1);
                break;
        }
        case -EINVAL:
                LCONSOLE_ERROR("%s: The configuration '%s' could not be read "
                               "from the MDT '%s'.  Make sure this client and "
                               "the MDT are running compatible versions of "
                               "Lustre.\n",
                               obd->obd_name, profile, mdt);
                /* fall through */
        default:
                LCONSOLE_ERROR("%s: The configuration '%s' could not be read "
                               "from the MDT '%s'.  This may be the result of "
                               "communication errors between the client and "
                               "the MDT, or if the MDT is not running.\n",
                               obd->obd_name, profile, mdt);
                break;
        }

        /* We don't so much care about errors in cleaning up the config llog
         * connection, as we have already read the config by this point. */
        obd_disconnect(exp);

out_cleanup:
        do_lcfg(MDCDEV, 0, LCFG_CLEANUP, 0, 0, 0, 0);

out_detach:
        do_lcfg(MDCDEV, 0, LCFG_DETACH, 0, 0, 0, 0);

out_del_uuid:
        /* class_add_uuid adds a nid even if the same uuid exists; we might
           delete any copy here.  So they all better match. */
        for (i = 0; i < failnodes; i++) {
                sprintf(niduuid, "mdtnid%d", i);
                do_lcfg(MDCDEV, 0, LCFG_DEL_UUID, niduuid, 0, 0, 0);
        }
        /* class_import_put will get rid of the additional connections */
out:
        RETURN(rc);
}
/* end COMPAT_146 */

int ll_fill_super(struct super_block *sb)
{
        struct lustre_profile *lprof;
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct ll_sb_info *sbi;
        char  *osc = NULL, *mdc = NULL;
        char  *profilenm = get_profile_name(sb);
        struct config_llog_instance cfg;
        char   ll_instance[sizeof(sb) * 2 + 1];
        int    err;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op: sb %p\n", sb);

        cfs_module_get();

        /* client additional sb info */
        lsi->lsi_llsbi = sbi = ll_init_sbi();
        if (!sbi) {
                cfs_module_put();
                RETURN(-ENOMEM);
        }

        err = ll_options(lsi->lsi_lmd->lmd_opts, &sbi->ll_flags);
        if (err) 
                GOTO(out_free, err);

        /* Generate a string unique to this super, in case some joker tries
           to mount the same fs at two mount points. 
           Use the address of the super itself.*/
        sprintf(ll_instance, "%p", sb);
        cfg.cfg_instance = ll_instance;
        cfg.cfg_uuid = lsi->lsi_llsbi->ll_sb_uuid;
        cfg.cfg_last_idx = 0;

        /* set up client obds */
        err = lustre_process_log(sb, profilenm, &cfg);
        /* COMPAT_146 */
        if (err < 0) {
                char *oldname;
                int rc, oldnamelen;
                oldnamelen = strlen(profilenm) + 1;
                /* Temp storage for 1.4.6 profile name */
                OBD_ALLOC(oldname, oldnamelen);
                if (oldname) {
                        memcpy(oldname, profilenm, oldnamelen); 
                        rc = old_lustre_process_log(sb, oldname, &cfg);
                        if (rc >= 0) {
                                /* That worked - update the profile name 
                                   permanently */
                                err = rc;
                                OBD_FREE(lsi->lsi_lmd->lmd_profile, 
                                         strlen(lsi->lsi_lmd->lmd_profile) + 1);
                                OBD_ALLOC(lsi->lsi_lmd->lmd_profile, 
                                         strlen(oldname) + 1);
                                if (!lsi->lsi_lmd->lmd_profile) {
                                        OBD_FREE(oldname, oldnamelen);
                                        GOTO(out_free, err = -ENOMEM);
                                }
                                memcpy(lsi->lsi_lmd->lmd_profile, oldname,
                                       strlen(oldname) + 1); 
                                profilenm = get_profile_name(sb);
                        }
                        OBD_FREE(oldname, oldnamelen);
                }
        }
        /* end COMPAT_146 */
        if (err < 0) {
                CERROR("Unable to process log: %d\n", err);
                GOTO(out_free, err);
        }

        lprof = class_get_profile(profilenm);
        if (lprof == NULL) {
                LCONSOLE_ERROR("The client profile '%s' could not be read "
                               "from the MGS.  Does that filesystem exist?\n",
                               profilenm);
                GOTO(out_free, err = -EINVAL);
        }
        CDEBUG(D_CONFIG, "Found profile %s: mdc=%s osc=%s\n", profilenm, 
               lprof->lp_mdc, lprof->lp_osc);

        OBD_ALLOC(osc, strlen(lprof->lp_osc) +
                  strlen(ll_instance) + 2);
        if (!osc) 
                GOTO(out_free, err = -ENOMEM);
        sprintf(osc, "%s-%s", lprof->lp_osc, ll_instance);

        OBD_ALLOC(mdc, strlen(lprof->lp_mdc) +
                  strlen(ll_instance) + 2);
        if (!mdc) 
                GOTO(out_free, err = -ENOMEM);
        sprintf(mdc, "%s-%s", lprof->lp_mdc, ll_instance);
  
        /* connections, registrations, sb setup */
        err = client_common_fill_super(sb, mdc, osc);
  
out_free:
        if (mdc)
                OBD_FREE(mdc, strlen(mdc) + 1);
        if (osc)
                OBD_FREE(osc, strlen(osc) + 1);
        if (err) 
                ll_put_super(sb);
        else
                LCONSOLE_WARN("Client %s has started\n", profilenm);        

        RETURN(err);
} /* ll_fill_super */


void ll_put_super(struct super_block *sb)
{
        struct config_llog_instance cfg;
        char   ll_instance[sizeof(sb) * 2 + 1];
        struct obd_device *obd;
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        char *profilenm = get_profile_name(sb);
        int force = 1, next;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op: sb %p - %s\n", sb, profilenm);
        
        sprintf(ll_instance, "%p", sb);
        cfg.cfg_instance = ll_instance;
        lustre_end_log(sb, NULL, &cfg);
        
        if (sbi->ll_mdc_exp) {
                obd = class_exp2obd(sbi->ll_mdc_exp);
                if (obd) 
                        force = obd->obd_no_recov;
        }
        
        /* We need to set force before the lov_disconnect in 
           lustre_common_put_super, since l_d cleans up osc's as well. */
        if (force) {
                next = 0;
                while ((obd = class_devices_in_group(&sbi->ll_sb_uuid,
                                                     &next)) != NULL) {
                        obd->obd_force = force;
                }
        }                       

        if (sbi->ll_lcq) {
                /* Only if client_common_fill_super succeeded */
                client_common_put_super(sb);
        }
                
        next = 0;
        while ((obd = class_devices_in_group(&sbi->ll_sb_uuid, &next)) !=NULL) {
                class_manual_cleanup(obd);
        }                       
        
        if (profilenm) 
                class_del_profile(profilenm);

        ll_free_sbi(sb);
        lsi->lsi_llsbi = NULL;

        lustre_common_put_super(sb);

        LCONSOLE_WARN("client %s umount complete\n", ll_instance);
        
        cfs_module_put();

        EXIT;
} /* client_put_super */

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
        /* NOTE: we depend on atomic igrab() -bzzz */
        lock_res_and_lock(lock);
        if (lock->l_ast_data) {
                struct ll_inode_info *lli = ll_i2info(lock->l_ast_data);
                if (lli->lli_inode_magic == LLI_INODE_MAGIC) {
                        inode = igrab(lock->l_ast_data);
                } else {
                        inode = lock->l_ast_data;
                        ldlm_lock_debug(NULL, inode->i_state & I_FREEING ?
                                                D_INFO : D_WARNING,
                                        lock, __FILE__, __func__, __LINE__,
                                        "l_ast_data %p is bogus: magic %08x",
                                        lock->l_ast_data, lli->lli_inode_magic);
                        inode = NULL;
                }
        }
        unlock_res_and_lock(lock);
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
        clear_bit(LLI_F_HAVE_MDS_SIZE_LOCK, &lli->lli_flags);
        mdc_change_cbdata(sbi->ll_mdc_exp, &fid, null_if_equal, inode);

        LASSERT(!lli->lli_open_fd_write_count);
        LASSERT(!lli->lli_open_fd_read_count);
        LASSERT(!lli->lli_open_fd_exec_count);

        if (lli->lli_mds_write_och)
                ll_mdc_real_close(inode, FMODE_WRITE);
        if (lli->lli_mds_exec_och) {
                if (!FMODE_EXEC)
                        CERROR("No FMODE exec, bug exec och is present for "
                               "inode %ld\n", inode->i_ino);
                ll_mdc_real_close(inode, FMODE_EXEC);
        }
        if (lli->lli_mds_read_och)
                ll_mdc_real_close(inode, FMODE_READ);


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

        spin_lock(&sbi->ll_deathrow_lock);
        list_del_init(&lli->lli_dead_list);
        spin_unlock(&sbi->ll_deathrow_lock);

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
        struct lustre_md md;
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
        if ((attr->ia_valid & ATTR_CTIME) && !(attr->ia_valid & ATTR_MTIME)) {
                /* To avoid stale mtime on mds, obtain it from ost and send 
                   to mds. */
                rc = ll_glimpse_size(inode, 0);
                if (rc)
                        RETURN(rc);

                attr->ia_valid |= ATTR_MTIME_SET | ATTR_MTIME;
                attr->ia_mtime = inode->i_mtime;
        }

        if (attr->ia_valid & (ATTR_MTIME | ATTR_CTIME))
                CDEBUG(D_INODE, "setting mtime %lu, ctime %lu, now = %lu\n",
                       LTIME_S(attr->ia_mtime), LTIME_S(attr->ia_ctime),
                       CURRENT_SECONDS);

        /* NB: ATTR_SIZE will only be set after this point if the size
         * resides on the MDS, ie, this file has no objects. */
        if (lsm)
                attr->ia_valid &= ~ATTR_SIZE;

        /* We always do an MDS RPC, even if we're only changing the size;
         * only the MDS knows whether truncate() should fail with -ETXTBUSY */
        ll_prepare_mdc_op_data(&op_data, inode, NULL, NULL, 0, 0);

        rc = mdc_setattr(sbi->ll_mdc_exp, &op_data,
                         attr, NULL, 0, NULL, 0, &request);

        if (rc) {
                ptlrpc_req_finished(request);
                if (rc == -ENOENT) {
                        inode->i_nlink = 0;
                        /* Unlinked special device node?  Or just a race?
                         * Pretend we done everything. */
                        if (!S_ISREG(inode->i_mode) &&
                            !S_ISDIR(inode->i_mode))
                                rc = inode_setattr(inode, attr);
                } else if (rc != -EPERM && rc != -EACCES && rc != -ETXTBSY)
                        CERROR("mdc_setattr fails: rc = %d\n", rc);
                RETURN(rc);
        }

        rc = mdc_req2lustre_md(request, REPLY_REC_OFF, sbi->ll_osc_exp, &md);
        if (rc) {
                ptlrpc_req_finished(request);
                RETURN(rc);
        }

        /* We call inode_setattr to adjust timestamps.
         * If there is at least some data in file, we cleared ATTR_SIZE above to
         * avoid invoking vmtruncate, otherwise it is important to call
         * vmtruncate in inode_setattr to update inode->i_size (bug 6196) */
        rc = inode_setattr(inode, attr);

        ll_update_inode(inode, &md);
        ptlrpc_req_finished(request);

        if (!lsm || !S_ISREG(inode->i_mode)) {
                CDEBUG(D_INODE, "no lsm: not setting attrs on OST\n");
                RETURN(rc);
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

                UNLOCK_INODE_MUTEX(inode);
                UP_WRITE_I_ALLOC_SEM(inode);
                rc = ll_extent_lock(NULL, inode, lsm, LCK_PW, &policy, &lockh,
                                    ast_flags);
#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
                DOWN_WRITE_I_ALLOC_SEM(inode);
                LOCK_INODE_MUTEX(inode);
#else
                LOCK_INODE_MUTEX(inode);
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
                struct obd_info oinfo = { { { 0 } } };
                struct obdo *oa = obdo_alloc();

                CDEBUG(D_INODE, "set mtime on OST inode %lu to %lu\n",
                       inode->i_ino, LTIME_S(attr->ia_mtime));

                if (oa) {
                        oa->o_id = lsm->lsm_object_id;
                        oa->o_valid = OBD_MD_FLID;

                        flags = OBD_MD_FLTYPE | OBD_MD_FLATIME |
                                OBD_MD_FLMTIME | OBD_MD_FLCTIME |
                                OBD_MD_FLFID | OBD_MD_FLGENER;

                        obdo_from_inode(oa, inode, flags);

                        oinfo.oi_oa = oa;
                        oinfo.oi_md = lsm;

                        rc = obd_setattr_rqset(sbi->ll_osc_exp, &oinfo, NULL);
                        if (rc)
                                CERROR("obd_setattr_async fails: rc=%d\n", rc);
                        obdo_free(oa);
                } else {
                        rc = -ENOMEM;
                }
        }
        RETURN(rc);
}

int ll_setattr(struct dentry *de, struct iattr *attr)
{
        ll_vfs_ops_tally(ll_i2sbi(de->d_inode), VFS_OPS_SETATTR);

        if ((attr->ia_valid & (ATTR_CTIME|ATTR_SIZE|ATTR_MODE)) ==
            (ATTR_CTIME|ATTR_SIZE|ATTR_MODE))
                attr->ia_valid |= MDS_OPEN_OWNEROVERRIDE;

        return ll_setattr_raw(de->d_inode, attr);
}

int ll_statfs_internal(struct super_block *sb, struct obd_statfs *osfs,
                       __u64 max_age)
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

        rc = obd_statfs_rqset(class_exp2obd(sbi->ll_osc_exp),
                              &obd_osfs, max_age);
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
#ifndef HAVE_STATFS_DENTRY_PARAM
int ll_statfs(struct super_block *sb, struct kstatfs *sfs)
{
#else
int ll_statfs(struct dentry *de, struct kstatfs *sfs)
{
        struct super_block *sb = de->d_sb;
#endif
        struct obd_statfs osfs;
        int rc;

        CDEBUG(D_VFSTRACE, "VFS Op: at "LPU64" jiffies\n", get_jiffies_64());
        lprocfs_counter_incr(ll_s2sbi(sb)->ll_stats, LPROC_LL_STAFS);

        /* For now we will always get up-to-date statfs values, but in the
         * future we may allow some amount of caching on the client (e.g.
         * from QOS or lprocfs updates). */
        rc = ll_statfs_internal(sb, &osfs, cfs_time_current_64() - 1);
        if (rc)
                return rc;

        statfs_unpack(sfs, &osfs);

        /* We need to downshift for all 32-bit kernels, because we can't
         * tell if the kernel is being called via sys_statfs64() or not.
         * Stop before overflowing f_bsize - in which case it is better
         * to just risk EOVERFLOW if caller is using old sys_statfs(). */
        if (sizeof(long) < 8) {
                while (osfs.os_blocks > ~0UL && sfs->f_bsize < 0x40000000) {
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
                if (lli->lli_smd != lsm)
                        obd_free_memmd(ll_i2obdexp(inode), &lsm);
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
        if (body->valid & OBD_MD_FLATIME &&
            body->atime > LTIME_S(inode->i_atime))
                LTIME_S(inode->i_atime) = body->atime;
        
        /* mtime is always updated with ctime, but can be set in past.
           As write and utime(2) may happen within 1 second, and utime's
           mtime has a priority over write's one, so take mtime from mds 
           for the same ctimes. */
        if (body->valid & OBD_MD_FLCTIME &&
            body->ctime >= LTIME_S(inode->i_ctime)) {
                LTIME_S(inode->i_ctime) = body->ctime;
                if (body->valid & OBD_MD_FLMTIME) {
                        CDEBUG(D_INODE, "setting ino %lu mtime "
                               "from %lu to "LPU64"\n", inode->i_ino, 
                               LTIME_S(inode->i_mtime), body->mtime);
                        LTIME_S(inode->i_mtime) = body->mtime;
                }
        }
        if (body->valid & OBD_MD_FLMODE)
                inode->i_mode = (inode->i_mode & S_IFMT)|(body->mode & ~S_IFMT);
        if (body->valid & OBD_MD_FLTYPE)
                inode->i_mode = (inode->i_mode & ~S_IFMT)|(body->mode & S_IFMT);
        if (S_ISREG(inode->i_mode)) {
                inode->i_blkbits = min(PTLRPC_MAX_BRW_BITS+1, LL_MAX_BLKSIZE_BITS);
        } else {
                inode->i_blkbits = inode->i_sb->s_blocksize_bits;
        }
#ifdef HAVE_INODE_BLKSIZE
        inode->i_blksize = 1<<inode->i_blkbits;
#endif
        if (body->valid & OBD_MD_FLUID)
                inode->i_uid = body->uid;
        if (body->valid & OBD_MD_FLGID)
                inode->i_gid = body->gid;
        if (body->valid & OBD_MD_FLFLAGS)
                inode->i_flags = ll_ext_to_inode_flags(body->flags);
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

                body = lustre_msg_buf(req->rq_repmsg, REPLY_REC_OFF,
                                      sizeof(*body));

                /* We want to return EXT3_*_FL flags to the caller via this
                 * ioctl.  An older MDS may be sending S_* flags, fix it up. */
                flags = ll_inode_to_ext_flags(body->flags, body->flags);
                ptlrpc_req_finished (req);

                RETURN(put_user(flags, (int *)arg));
        }
        case EXT3_IOC_SETFLAGS: {
                struct mdc_op_data op_data;
                struct ll_iattr_struct attr = { 0 };
                struct obd_info oinfo = { { { 0 } } };
                struct lov_stripe_md *lsm = ll_i2info(inode)->lli_smd;

                if (get_user(flags, (int *)arg))
                        RETURN(-EFAULT);

                oinfo.oi_md = lsm;
                oinfo.oi_oa = obdo_alloc();
                if (!oinfo.oi_oa)
                        RETURN(-ENOMEM);

                ll_prepare_mdc_op_data(&op_data, inode, NULL, NULL, 0, 0);

                attr.ia_attr_flags = flags;
                ((struct iattr *)&attr)->ia_valid |= ATTR_ATTR_FLAG;

                rc = mdc_setattr(sbi->ll_mdc_exp, &op_data,
                                 (struct iattr *)&attr, NULL, 0, NULL, 0, &req);
                ptlrpc_req_finished(req);
                if (rc || lsm == NULL) {
                        obdo_free(oinfo.oi_oa);
                        RETURN(rc);
                }

                oinfo.oi_oa->o_id = lsm->lsm_object_id;
                oinfo.oi_oa->o_flags = flags;
                oinfo.oi_oa->o_valid = OBD_MD_FLID | OBD_MD_FLFLAGS;

                obdo_from_inode(oinfo.oi_oa, inode,
                                OBD_MD_FLFID | OBD_MD_FLGENER);
                rc = obd_setattr_rqset(sbi->ll_osc_exp, &oinfo, NULL);
                obdo_free(oinfo.oi_oa);
                if (rc) {
                        if (rc != -EPERM && rc != -EACCES)
                                CERROR("mdc_setattr_async fails: rc = %d\n", rc);
                        RETURN(rc);
                }

                inode->i_flags = ll_ext_to_inode_flags(flags |
                                                       MDS_BFLAG_EXT_FLAGS);
                RETURN(0);
        }
        default:
                RETURN(-ENOSYS);
        }

        RETURN(0);
}

/* umount -f client means force down, don't save state */
#ifdef HAVE_UMOUNTBEGIN_VFSMOUNT
void ll_umount_begin(struct vfsmount *vfsmnt, int flags)
{
        struct super_block *sb = vfsmnt->mnt_sb;
#else
void ll_umount_begin(struct super_block *sb)
{
#endif
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        struct obd_device *obd;
        struct obd_ioctl_data ioc_data = { 0 };
        ENTRY;

#ifdef HAVE_UMOUNTBEGIN_VFSMOUNT
        if (!(flags & MNT_FORCE)) {
                EXIT;
                return;
        }
#endif

        /* Tell the MGC we got umount -f */
        lsi->lsi_flags |= LSI_UMOUNT_FORCE;

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

int ll_remount_fs(struct super_block *sb, int *flags, char *data)
{
        struct ll_sb_info *sbi = ll_s2sbi(sb);
        int err;
        __u32 read_only;
 
        if ((*flags & MS_RDONLY) != (sb->s_flags & MS_RDONLY)) {
                read_only = *flags & MS_RDONLY;
                err = obd_set_info_async(sbi->ll_mdc_exp, strlen("read-only"),
                                         "read-only", sizeof(read_only),
                                         &read_only, NULL);
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
        struct ll_sb_info *sbi = NULL;
        int rc = 0;
        ENTRY;

        LASSERT(*inode || sb);
        sbi = sb ? ll_s2sbi(sb) : ll_i2sbi(*inode);
        prune_deathrow(sbi, 1);

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

int ll_obd_statfs(struct inode *inode, void *arg)
{
        struct ll_sb_info *sbi = NULL;
        struct obd_device *client_obd = NULL, *lov_obd = NULL;
        struct lov_obd *lov = NULL;
        struct obd_statfs stat_buf = {0};
        char *buf = NULL;
        struct obd_ioctl_data *data = NULL;
        __u32 type, index;
        int len, rc;

        if (!inode || !(sbi = ll_i2sbi(inode)))
                GOTO(out_statfs, rc = -EINVAL);

        rc = obd_ioctl_getdata(&buf, &len, arg);
        if (rc)
                GOTO(out_statfs, rc);

        data = (void*)buf;
        if (!data->ioc_inlbuf1 || !data->ioc_inlbuf2 ||
            !data->ioc_pbuf1 || !data->ioc_pbuf2)
                GOTO(out_statfs, rc = -EINVAL);

        memcpy(&type, data->ioc_inlbuf1, sizeof(__u32));
        memcpy(&index, data->ioc_inlbuf2, sizeof(__u32));

        if (type == LL_STATFS_MDC) {
                if (index > 0)
                        GOTO(out_statfs, rc = -ENODEV);
                client_obd = class_exp2obd(sbi->ll_mdc_exp);
        } else if (type == LL_STATFS_LOV) {
                lov_obd = class_exp2obd(sbi->ll_osc_exp);
                lov = &lov_obd->u.lov;

                if ((index >= lov->desc.ld_tgt_count) ||
                    !lov->lov_tgts[index])
                        GOTO(out_statfs, rc = -ENODEV);

                client_obd = class_exp2obd(lov->lov_tgts[index]->ltd_exp);
                if (!lov->lov_tgts[index]->ltd_active)
                        GOTO(out_uuid, rc = -ENODATA);
        }

        if (!client_obd)
                GOTO(out_statfs, rc = -EINVAL);

        rc = obd_statfs(client_obd, &stat_buf, cfs_time_current_64() - 1);
        if (rc)
                GOTO(out_statfs, rc);

        if (copy_to_user(data->ioc_pbuf1, &stat_buf, data->ioc_plen1))
                GOTO(out_statfs, rc = -EFAULT);

out_uuid:
        if (copy_to_user(data->ioc_pbuf2, obd2cli_tgt(client_obd),
                         data->ioc_plen2))
                rc = -EFAULT;

out_statfs:
        if (buf)
                obd_ioctl_freedata(buf, len);
        return rc;
}

int ll_process_config(struct lustre_cfg *lcfg)
{
        char *ptr;
        void *sb;
        struct lprocfs_static_vars lvars;
        unsigned long x; 
        int rc = 0;

        lprocfs_init_vars(llite, &lvars);

        /* The instance name contains the sb: lustre-client-aacfe000 */
        ptr = strrchr(lustre_cfg_string(lcfg, 0), '-');
        if (!ptr || !*(++ptr)) 
                return -EINVAL;
        if (sscanf(ptr, "%lx", &x) != 1)
                return -EINVAL;
        sb = (void *)x;
        /* This better be a real Lustre superblock! */
        LASSERT(s2lsi((struct super_block *)sb)->lsi_lmd->lmd_magic == LMD_MAGIC);

        /* Note we have not called client_common_fill_super yet, so 
           proc fns must be able to handle that! */
        rc = class_process_proc_param(PARAM_LLITE, lvars.obd_vars,
                                      lcfg, sb);
        return(rc);
}

