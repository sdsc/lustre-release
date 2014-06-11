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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LLITE

#include <linux/module.h>
#include <linux/types.h>
#include <linux/version.h>
#include <lustre_lite.h>
#include <lustre_ha.h>
#include <lustre_dlm.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <lprocfs_status.h>
#include "llite_internal.h"

static struct kmem_cache *ll_inode_cachep;

static struct inode *ll_alloc_inode(struct super_block *sb)
{
	struct ll_inode_info *lli;
	ll_stats_ops_tally(ll_s2sbi(sb), LPROC_LL_ALLOC_INODE, 1);
	OBD_SLAB_ALLOC_PTR_GFP(lli, ll_inode_cachep, GFP_NOFS);
	if (lli == NULL)
		return NULL;

	inode_init_once(&lli->lli_vfs_inode);
	return &lli->lli_vfs_inode;
}

#ifdef HAVE_INODE_I_RCU
static void ll_inode_destroy_callback(struct rcu_head *head)
{
	struct inode *inode = container_of(head, struct inode, i_rcu);
	struct ll_inode_info *ptr = ll_i2info(inode);
	OBD_SLAB_FREE_PTR(ptr, ll_inode_cachep);
}

static void ll_destroy_inode(struct inode *inode)
{
	call_rcu(&inode->i_rcu, ll_inode_destroy_callback);
}
#else
static void ll_destroy_inode(struct inode *inode)
{
	struct ll_inode_info *ptr = ll_i2info(inode);
	OBD_SLAB_FREE_PTR(ptr, ll_inode_cachep);
}
#endif

static int ll_init_inodecache(void)
{
	ll_inode_cachep = kmem_cache_create("lustre_inode_cache",
					    sizeof(struct ll_inode_info),
					    0, SLAB_HWCACHE_ALIGN, NULL);
	if (ll_inode_cachep == NULL)
		return -ENOMEM;
	return 0;
}

static void ll_destroy_inodecache(void)
{
	kmem_cache_destroy(ll_inode_cachep);
}

/* exported operations */
struct super_operations lustre_super_operations =
{
        .alloc_inode   = ll_alloc_inode,
        .destroy_inode = ll_destroy_inode,
#ifdef HAVE_SBOPS_EVICT_INODE
        .evict_inode   = ll_delete_inode,
#else
        .clear_inode   = ll_clear_inode,
        .delete_inode  = ll_delete_inode,
#endif
        .put_super     = ll_put_super,
        .statfs        = ll_statfs,
        .umount_begin  = ll_umount_begin,
        .remount_fs    = ll_remount_fs,
        .show_options  = ll_show_options,
};


void lustre_register_client_process_config(int (*cpc)(struct lustre_cfg *lcfg));


#if defined(__KERNEL__)
#if defined(HAVE_SERVER_SUPPORT)

void force_client_load(void)
{
}
EXPORT_SYMBOL(force_client_load);

#else /* !defined(HAVE_SERVER_SUPPORT) */

static void lustre_register_kill_super_cb(void (*cfs)(struct super_block *sb))
{
}

/***************** FS registration ******************/
#ifdef HAVE_FSTYPE_MOUNT
struct dentry *lustre_mount(struct file_system_type *fs_type, int flags,
			    const char *devname, void *data)
{
	struct lustre_mount_data2 lmd2 = { data, NULL };

	return mount_nodev(fs_type, flags, &lmd2, lustre_fill_super);
}
#else
int lustre_get_sb(struct file_system_type *fs_type, int flags,
		  const char *devname, void *data, struct vfsmount *mnt)
{
	struct lustre_mount_data2 lmd2 = { data, mnt };

	return get_sb_nodev(fs_type, flags, &lmd2, lustre_fill_super, mnt);
}
#endif

void lustre_kill_super(struct super_block *sb)
{
	struct lustre_sb_info *lsi = s2lsi(sb);

	if (lsi && !IS_SERVER(lsi))
		ll_kill_super(sb);

	kill_anon_super(sb);
}

/** Register the "lustre" fs type
 */
struct file_system_type lustre_fs_type = {
	.owner        = THIS_MODULE,
	.name         = "lustre",
#ifdef HAVE_FSTYPE_MOUNT
	.mount        = lustre_mount,
#else
	.get_sb       = lustre_get_sb,
#endif
	.kill_sb      = lustre_kill_super,
	.fs_flags     = FS_BINARY_MOUNTDATA | FS_REQUIRES_DEV |
			FS_HAS_FIEMAP | FS_RENAME_DOES_D_MOVE,
};
MODULE_ALIAS_FS("lustre");
#endif /* !defined(HAVE_SERVER_SUPPORT */
#endif /* defined(KERNEL) */

static int __init init_lustre_lite(void)
{
	int i, rc, seed[2];
	struct timeval tv;
	lnet_process_id_t lnet_id;

	CLASSERT(sizeof(LUSTRE_VOLATILE_HDR) == LUSTRE_VOLATILE_HDR_LEN + 1);

	/* print an address of _any_ initialized kernel symbol from this
	 * module, to allow debugging with gdb that doesn't support data
	 * symbols from modules.*/
	CDEBUG(D_INFO, "Lustre client module (%p).\n",
	       &lustre_super_operations);

        rc = ll_init_inodecache();
        if (rc)
                return -ENOMEM;
	ll_file_data_slab = kmem_cache_create("ll_file_data",
						 sizeof(struct ll_file_data), 0,
						 SLAB_HWCACHE_ALIGN, NULL);
	if (ll_file_data_slab == NULL) {
		ll_destroy_inodecache();
		return -ENOMEM;
	}

	ll_remote_perm_cachep = kmem_cache_create("ll_remote_perm_cache",
						  sizeof(struct ll_remote_perm),
						  0, 0, NULL);
	if (ll_remote_perm_cachep == NULL) {
		kmem_cache_destroy(ll_file_data_slab);
		ll_file_data_slab = NULL;
		ll_destroy_inodecache();
		return -ENOMEM;
	}

	ll_rmtperm_hash_cachep = kmem_cache_create("ll_rmtperm_hash_cache",
						   REMOTE_PERM_HASHSIZE *
						   sizeof(struct list_head),
						   0, 0, NULL);
	if (ll_rmtperm_hash_cachep == NULL) {
		kmem_cache_destroy(ll_remote_perm_cachep);
		ll_remote_perm_cachep = NULL;
		kmem_cache_destroy(ll_file_data_slab);
		ll_file_data_slab = NULL;
		ll_destroy_inodecache();
		return -ENOMEM;
	}

        proc_lustre_fs_root = proc_lustre_root ?
			      lprocfs_seq_register("llite", proc_lustre_root,
						   NULL, NULL) : NULL;
        lustre_register_client_fill_super(ll_fill_super);
        lustre_register_kill_super_cb(ll_kill_super);
        lustre_register_client_process_config(ll_process_config);

        cfs_get_random_bytes(seed, sizeof(seed));

        /* Nodes with small feet have little entropy
         * the NID for this node gives the most entropy in the low bits */
        for (i=0; ; i++) {
                if (LNetGetId(i, &lnet_id) == -ENOENT) {
                        break;
                }
                if (LNET_NETTYP(LNET_NIDNET(lnet_id.nid)) != LOLND) {
                        seed[0] ^= LNET_NIDADDR(lnet_id.nid);
                }
        }

	do_gettimeofday(&tv);
	cfs_srand(tv.tv_sec ^ seed[0], tv.tv_usec ^ seed[1]);

        init_timer(&ll_capa_timer);
        ll_capa_timer.function = ll_capa_timer_callback;
        rc = ll_capa_thread_start();
        /*
         * XXX normal cleanup is needed here.
         */
        if (rc == 0)
                rc = vvp_global_init();

	if (rc == 0)
		rc = ll_xattr_init();
#if defined(__KERNEL__) && !defined(HAVE_SERVER_SUPPORT)
	if (rc == 0)
		rc = register_filesystem(&lustre_fs_type);
#endif
        return rc;
}

static void __exit exit_lustre_lite(void)
{
#ifndef HAVE_SERVER_SUPPORT
	(void)unregister_filesystem(&lustre_fs_type);
#endif
	ll_xattr_fini();
	vvp_global_fini();
	del_timer(&ll_capa_timer);
	ll_capa_thread_stop();
	LASSERTF(capa_count[CAPA_SITE_CLIENT] == 0,
		 "client remaining capa count %d\n",
		 capa_count[CAPA_SITE_CLIENT]);
	lustre_register_client_fill_super(NULL);
	lustre_register_kill_super_cb(NULL);
	lustre_register_client_process_config(NULL);

	ll_destroy_inodecache();

	kmem_cache_destroy(ll_rmtperm_hash_cachep);
	ll_rmtperm_hash_cachep = NULL;

	kmem_cache_destroy(ll_remote_perm_cachep);
	ll_remote_perm_cachep = NULL;

	kmem_cache_destroy(ll_file_data_slab);
	if (proc_lustre_fs_root)
		lprocfs_remove(&proc_lustre_fs_root);
}

MODULE_AUTHOR("Sun Microsystems, Inc. <http://www.lustre.org/>");
MODULE_DESCRIPTION("Lustre Lite Client File System");
MODULE_LICENSE("GPL");

module_init(init_lustre_lite);
module_exit(exit_lustre_lite);
