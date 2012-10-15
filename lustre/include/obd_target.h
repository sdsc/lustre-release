/* GPL HEADER START
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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef __OBD_TARGET_H
#define __OBD_TARGET_H

/* server-side individual type definitions */

struct osd_properties {
	size_t osd_max_ea_size;
};

#define OBT_MAGIC       0xBDDECEAE
/* hold common fields for "target" device */
struct obd_device_target {
	__u32                    obt_magic;
	__u32                    obt_instance;
	struct super_block       *obt_sb;
	/** last_rcvd file */
	struct file              *obt_rcvd_filp;
	struct lu_target         *obt_lut;
	__u64                    obt_mount_count;
	cfs_rw_semaphore_t       obt_rwsem;
	struct vfsmount          *obt_vfsmnt;
	struct file              *obt_health_check_filp;
	struct osd_properties    obt_osd_properties;
	struct obd_job_stats     obt_jobstats;
};

#define FILTER_SUBDIR_COUNT      32            /* set to zero for no subdirs */

struct filter_subdirs {
	cfs_dentry_t *dentry[FILTER_SUBDIR_COUNT];
};

struct filter_ext {
	__u64                fe_start;
	__u64                fe_end;
};

struct filter_obd {
	/* NB this field MUST be first */
	struct obd_device_target fo_obt;
	const char          *fo_fstype;

	int                  fo_group_count;
	cfs_dentry_t        *fo_dentry_O;
	cfs_dentry_t       **fo_dentry_O_groups;
	struct filter_subdirs   *fo_dentry_O_sub;
	cfs_mutex_t          fo_init_lock;      /* group initialization lock */
	int                  fo_committed_group;

	cfs_spinlock_t       fo_objidlock;      /* protect fo_lastobjid */

	unsigned long        fo_destroys_in_progress;
	cfs_mutex_t          fo_create_locks[FILTER_SUBDIR_COUNT];

	cfs_list_t           fo_export_list;
	int                  fo_subdir_count;

	obd_size             fo_tot_dirty;      /* protected by obd_osfs_lock */
	obd_size             fo_tot_granted;    /* all values in bytes */
	obd_size             fo_tot_pending;
	int                  fo_tot_granted_clients;

	obd_size             fo_readcache_max_filesize;
	cfs_spinlock_t       fo_flags_lock;
	unsigned int         fo_read_cache:1,   /**< enable read-only cache */
			     fo_writethrough_cache:1,/**< read cache writes */
			     fo_mds_ost_sync:1, /**< MDS-OST orphan recovery*/
			     fo_raid_degraded:1;/**< RAID device degraded */

	struct obd_import   *fo_mdc_imp;
	struct obd_uuid      fo_mdc_uuid;
	struct lustre_handle fo_mdc_conn;
	struct file        **fo_last_objid_files;
	__u64               *fo_last_objids; /* last created objid for groups,
	                                      * protected by fo_objidlock */

	cfs_mutex_t          fo_alloc_lock;

	cfs_atomic_t         fo_r_in_flight;
	cfs_atomic_t         fo_w_in_flight;

	/*
	 * per-filter pool of kiobuf's allocated by filter_common_setup() and
	 * torn down by filter_cleanup().
	 *
	 * This pool contains kiobuf used by
	 * filter_{prep,commit}rw_{read,write}() and is shared by all OST
	 * threads.
	 *
	 * Locking: protected by internal lock of cfs_hash, pool can be
	 * found from this hash table by t_id of ptlrpc_thread.
	 */
	struct cfs_hash		*fo_iobuf_hash;

	cfs_list_t               fo_llog_list;
	cfs_spinlock_t           fo_llog_list_lock;

	struct brw_stats         fo_filter_stats;

	int                      fo_fmd_max_num; /* per exp filter_mod_data */
	int                      fo_fmd_max_age; /* jiffies to fmd expiry */
	unsigned long            fo_syncjournal:1, /* sync journal on writes */
				 fo_sync_lock_cancel:2;/* sync on lock cancel */


	/* sptlrpc stuff */
	cfs_rwlock_t             fo_sptlrpc_lock;
	struct sptlrpc_rule_set  fo_sptlrpc_rset;

	/* capability related */
	unsigned int             fo_fl_oss_capa;
	cfs_list_t               fo_capa_keys;
	cfs_hlist_head_t        *fo_capa_hash;
	struct llog_commit_master *fo_lcm;
	int                      fo_sec_level;
};

struct mds_obd {
	/* NB this field MUST be first */
	struct obd_device_target         mds_obt;
	struct ptlrpc_service           *mds_service;
	struct ptlrpc_service           *mds_setattr_service;
	struct ptlrpc_service           *mds_readpage_service;
	int                              mds_max_mdsize;
	int                              mds_max_cookiesize;
	__u64                            mds_io_epoch;
	unsigned long                    mds_atime_diff;
	struct ll_fid                    mds_rootfid;
	cfs_dentry_t                    *mds_pending_dir;
	cfs_dentry_t                    *mds_logs_dir;
	cfs_dentry_t                    *mds_objects_dir;
	struct llog_handle              *mds_cfg_llh;
	struct obd_device               *mds_lov_obd;
	struct obd_uuid                  mds_lov_uuid;
	char                            *mds_profile;
	struct obd_export               *mds_lov_exp;
	struct lov_desc                  mds_lov_desc;
	__u32                            mds_id;

	/* mark pages dirty for write. */
	cfs_bitmap_t                    *mds_lov_page_dirty;
	/* array for store pages with obd_id */
	void                           **mds_lov_page_array;
	/* file for store objid */
	struct file                     *mds_lov_objid_filp;
	__u32                            mds_lov_objid_count;
	__u32                            mds_lov_objid_max_index;
	__u32                            mds_lov_objid_lastpage;
	__u32                            mds_lov_objid_lastidx;


	cfs_rw_semaphore_t               mds_qonoff_sem;
	unsigned long                    mds_fl_user_xattr:1,
					 mds_fl_acl:1,
					 mds_evict_ost_nids:1,
					 mds_fl_cfglog:1,
					 mds_fl_synced:1,
					 mds_fl_target:1; /* mds have one or
							   * more targets */

	struct upcall_cache             *mds_identity_cache;

	/* for capability keys update */
	struct lustre_capa_key          *mds_capa_keys;
	cfs_rw_semaphore_t               mds_notify_lock;
};

/* lov objid */
extern __u32 mds_max_ost_index;

#define MDS_LOV_ALLOC_SIZE	  (CFS_PAGE_SIZE)

#define OBJID_PER_PAGE()	  (MDS_LOV_ALLOC_SIZE / sizeof(obd_id))

#define MDS_LOV_OBJID_PAGES_COUNT (mds_max_ost_index/OBJID_PER_PAGE())

extern int mds_lov_init_objids(struct obd_device *obd);
extern void mds_lov_destroy_objids(struct obd_device *obd);

struct echo_obd {
	struct obd_device_target eo_obt;
	struct obdo              eo_oa;
	cfs_spinlock_t           eo_lock;
	__u64                    eo_lastino;
	struct lustre_handle     eo_nl_lock;
	cfs_atomic_t             eo_prep;
};

struct ost_obd {
	struct ptlrpc_service    *ost_service;
	struct ptlrpc_service    *ost_create_service;
	struct ptlrpc_service    *ost_io_service;
	cfs_mutex_t              ost_health_mutex;
};
#endif /* __OBD_TARGET_H */
