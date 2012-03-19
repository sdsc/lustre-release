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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osd/osd_internal.h
 *
 * Shared definitions and declarations for osd module
 *
 * Author: Nikita Danilov <nikita@clusterfs.com>
 */

#ifndef _OSD_INTERNAL_H
#define _OSD_INTERNAL_H

#if defined(__KERNEL__)

/* struct rw_semaphore */
#include <linux/rwsem.h>
/* struct dentry */
#include <linux/dcache.h>
/* struct dirent64 */
#include <linux/dirent.h>

#ifdef HAVE_EXT4_LDISKFS
#include <ldiskfs/ldiskfs.h>
#include <ldiskfs/ldiskfs_jbd2.h>
# ifdef HAVE_LDISKFS_JOURNAL_CALLBACK_ADD
#  define journal_callback ldiskfs_journal_cb_entry
#  define osd_journal_callback_set(handle, func, jcb) ldiskfs_journal_callback_add(handle, func, jcb)
# else
#  define osd_journal_callback_set(handle, func, jcb) jbd2_journal_callback_set(handle, func, jcb)
# endif
#else
#include <linux/jbd.h>
#include <linux/ldiskfs_fs.h>
#include <linux/ldiskfs_jbd.h>
#define osd_journal_callback_set(handle, func, jcb) journal_callback_set(handle, func, jcb)
#endif


/* LUSTRE_OSD_NAME */
#include <obd.h>
/* class_register_type(), class_unregister_type(), class_get_type() */
#include <obd_class.h>
#include <lustre_disk.h>
#include <dt_object.h>
#include <lustre_scrub.h>

#include "osd_oi.h"
#include "osd_iam.h"

struct inode;

#define OSD_OII_NOGEN (0)
#define OSD_COUNTERS (0)

/** Enable thandle usage statistics */
#define OSD_THANDLE_STATS (0)

#ifdef HAVE_QUOTA_SUPPORT
struct osd_ctxt {
        __u32 oc_uid;
        __u32 oc_gid;
        cfs_kernel_cap_t oc_cap;
};
#endif

#ifdef HAVE_LDISKFS_PDO

#define osd_ldiskfs_find_entry(dir, dentry, de, lock)   \
        ll_ldiskfs_find_entry(dir, dentry, de, lock)
#define osd_ldiskfs_add_entry(handle, child, cinode, hlock) \
        ldiskfs_add_entry(handle, child, cinode, hlock)

#else /* HAVE_LDISKFS_PDO */

struct htree_lock {
        int     dummy;
};

struct htree_lock_head {
        int     dummy;
};

#define ldiskfs_htree_lock(lock, head, inode, op)  do { LBUG(); } while (0)
#define ldiskfs_htree_unlock(lock)                 do { LBUG(); } while (0)

static inline struct htree_lock_head *ldiskfs_htree_lock_head_alloc(int dep)
{
        LBUG();
        return NULL;
}

#define ldiskfs_htree_lock_head_free(lh)           do { LBUG(); } while (0)

#define LDISKFS_DUMMY_HTREE_LOCK        0xbabecafe

static inline struct htree_lock *ldiskfs_htree_lock_alloc(void)
{
        return (struct htree_lock *)LDISKFS_DUMMY_HTREE_LOCK;
}

static inline void ldiskfs_htree_lock_free(struct htree_lock *lk)
{
        LASSERT((unsigned long)lk == LDISKFS_DUMMY_HTREE_LOCK);
}

#define HTREE_HBITS_DEF         0

#define osd_ldiskfs_find_entry(dir, dentry, de, lock)   \
        ll_ldiskfs_find_entry(dir, dentry, de)
#define osd_ldiskfs_add_entry(handle, child, cinode, lock) \
        ldiskfs_add_entry(handle, child, cinode)

#endif /* HAVE_LDISKFS_PDO */

#define OSD_SCRUB_IT_CACHE_SIZE 128
#define OSD_SCRUB_IT_CACHE_MASK (~(OSD_SCRUB_IT_CACHE_SIZE - 1))

struct osd_idmap_cache {
        struct lu_fid           oic_fid;
        struct osd_inode_id     oic_lid;
};

struct osd_inconsistent_item {
        /* Link into osd_device::od_inconsistent_items. */
        cfs_list_t              oii_list;

        /* The correct "FID <=> LID" mapping. */
        struct osd_idmap_cache  oii_cache;

        /* Object for the OI mapping to be updated. */
        struct lu_object       *oii_obj;
};

struct osd_scrub_it {
        /* Special used for the OSD layer scrub. */
        struct lu_env           osi_env;

        /* Used by osd_scrub_it_key. */
        __u8                    osi_key[SCRUB_BOOKMARK_MAXLEN];

        /* Used by lprocfs_osd_rd_oi_scrub. */
        __u8                    osi_pos_dump[SCRUB_BOOKMARK_MAXLEN];

        /* Protect osi_cache. */
        cfs_spinlock_t          osi_cache_lock;

        /* Ring cache for the scrub iteration. */
        struct osd_idmap_cache  osi_cache[OSD_SCRUB_IT_CACHE_SIZE];

        /* Temp cache to hold current record before the producer get a free
         * slot in the osi_cache. */
        struct osd_idmap_cache  osi_producer_slot;

        /* Temp cache to hold the record to be returned for next ::rec(). */
        struct osd_idmap_cache  osi_consumer_slot;

        /* Index for next cache slot to be filled. */
        int                     osi_producer_idx;

        /* Index for next cache slot has been filled. */
        int                     osi_consumer_idx;

        /* How many items in osi_cache. */
        __u32                   osi_cached_items;

        /* How many inodes per block. */
        __u32                   osi_inodes_per_block;

        /* Temp oic buffer under urgent mode. */
        struct osd_idmap_cache  osi_oic_urgent;

        /* Point to osi_oic_urgent or osi_cache[x], or some inconsistent item.*/
        struct osd_idmap_cache *osi_oicp;

        /* Point to the super_block to be iterated. */
        struct super_block     *osi_sb;

        /* Point to osd_device. */
        struct osd_device      *osi_dev;

        /* Point to current object inode to be processed. */
        struct inode           *osi_inode;

        /* Osd layer interation thread. */
        struct ptlrpc_thread    osi_thread;

        /* Reference count. */
        cfs_atomic_t            osi_refcount;

        /* The position for OSD layer scrub scanning. Under normal mode,
         * The iteration for up layer LFSCK and OSD layer scrub scanning
         * share the same osi_pos_current. Under urgent mode, it will be
         * used for OSD layer scrub scanning only, as for the iteration,
         * another osi_pos_urgent will be used. */
        __u32                   osi_pos_current;

        /* Used under urgent mode for the iteration for up layer LFSCK. */
        __u32                   osi_pos_urgent;

        /* For osd layer iterator exit status. */
        int                     osi_exit_value;

        unsigned int            osi_urgent_mode:1, /* Under urgent mode, it does
                                                    * not fill osi_cache. */
                                osi_it_start:1,    /* Switch for triggering
                                                    * OSD layer iteration. */
                                osi_all_cached:1,  /* No more entries can be
                                                    * filled into cache. */
                                osi_in_prior:1,    /* Process inconsistent item
                                                    * found by RPC prior. */
                                osi_skip_obj:1,    /* Skip to process the obj.*/
                                osi_self_preload:1;/* Up layer iteration preload
                                                    * items by itself. */
};

/*
 * osd device.
 */
struct osd_device {
        /* super-class */
        struct dt_device          od_dt_dev;
        /* information about underlying file system */
        struct lustre_mount_info *od_mount;
        /*
         * XXX temporary stuff for object index: directory where every object
         * is named by its fid.
         */
        struct dt_object         *od_obj_area;
        /* object index */
        struct osd_oi            *od_oi_table;
        /* total number of OI containers */
        int                       od_oi_count;
        /**
         * The following flag indicates, if it is interop mode or not.
         * It will be initialized, using mount param.
         */
        __u32                     od_iop_mode;

        cfs_proc_dir_entry_t     *od_proc_entry;
        struct lprocfs_stats     *od_stats;
        /*
         * statfs optimization: we cache a bit.
         */
        cfs_time_t                od_osfs_age;
        cfs_kstatfs_t             od_kstatfs;
        cfs_spinlock_t            od_osfs_lock;

        cfs_list_t                od_inconsistent_items;
        struct scrub_exec_head   *od_scrub_seh;

        unsigned int              od_fl_capa:1, /* Fid Capability */
                                  od_fl_restored:1;
        __u32                     od_capa_alg;
        struct lustre_capa_key   *od_capa_keys;
        cfs_hlist_head_t         *od_capa_hash;
        unsigned long             od_capa_timeout;
};

struct osd_directory {
        struct iam_container od_container;
        struct iam_descr     od_descr;
};

struct osd_object {
        struct dt_object       oo_dt;
        /**
         * Inode for file system object represented by this osd_object. This
         * inode is pinned for the whole duration of lu_object life.
         *
         * Not modified concurrently (either setup early during object
         * creation, or assigned by osd_object_create() under write lock).
         */
        struct inode          *oo_inode;
        /**
         * to protect index ops.
         */
        struct htree_lock_head *oo_hl_head;
        cfs_rw_semaphore_t     oo_ext_idx_sem;
        cfs_rw_semaphore_t     oo_sem;
        struct osd_directory  *oo_dir;
        /** protects inode attributes. */
        cfs_spinlock_t         oo_guard;
        /**
         * Following two members are used to indicate the presence of dot and
         * dotdot in the given directory. This is required for interop mode
         * (b11826).
         */
        unsigned int           oo_compat_dot_created:1,
                               oo_compat_dotdot_created:1,
                               oo_skip_scrub:1;

        const struct lu_env   *oo_owner;
#ifdef CONFIG_LOCKDEP
        struct lockdep_map     oo_dep_map;
#endif
};

#define OSD_TRACK_DECLARES
#ifdef OSD_TRACK_DECLARES
#define OSD_DECLARE_OP(oh, op)   {                               \
        LASSERT(oh->ot_handle == NULL);                          \
        ((oh)->ot_declare_ ##op)++; }
#define OSD_EXEC_OP(handle, op)     {                            \
        struct osd_thandle *oh;                                  \
        oh = container_of0(handle, struct osd_thandle, ot_super);\
        if (((oh)->ot_declare_ ##op) > 0) {                      \
                ((oh)->ot_declare_ ##op)--;                      \
        }                                                        \
        }
#else
#define OSD_DECLARE_OP(oh, op)
#define OSD_EXEC_OP(oh, op)
#endif

/* There are at most 10 uid/gids are affected in a transaction, and
 * that's rename case:
 * - 2 for source parent uid & gid;
 * - 2 for source child uid & gid ('..' entry update when the child
 *   is directory);
 * - 2 for target parent uid & gid;
 * - 2 for target child uid & gid (if the target child exists);
 * - 2 for root uid & gid (last_rcvd, llog, etc);
 *
 * The 0 to (OSD_MAX_UGID_CNT - 1) bits of ot_id_type is for indicating
 * the id type of each id in the ot_id_array.
 */
#define OSD_MAX_UGID_CNT        10

struct osd_thandle {
        struct thandle          ot_super;
        handle_t               *ot_handle;
        struct journal_callback ot_jcb;
        cfs_list_t              ot_dcb_list;
        /* Link to the device, for debugging. */
        struct lu_ref_link     *ot_dev_link;
        unsigned short          ot_credits;
        unsigned short          ot_id_cnt;
        unsigned short          ot_id_type;
        uid_t                   ot_id_array[OSD_MAX_UGID_CNT];

#ifdef OSD_TRACK_DECLARES
        unsigned char           ot_declare_attr_set;
        unsigned char           ot_declare_punch;
        unsigned char           ot_declare_xattr_set;
        unsigned char           ot_declare_create;
        unsigned char           ot_declare_destroy;
        unsigned char           ot_declare_ref_add;
        unsigned char           ot_declare_ref_del;
        unsigned char           ot_declare_write;
        unsigned char           ot_declare_insert;
        unsigned char           ot_declare_delete;
#endif

#if OSD_THANDLE_STATS
        /** time when this handle was allocated */
        cfs_time_t oth_alloced;

        /** time when this thanle was started */
        cfs_time_t oth_started;
#endif
};

/*
 * osd dev stats
 */

#ifdef LPROCFS
enum {
#if OSD_THANDLE_STATS
        LPROC_OSD_THANDLE_STARTING,
        LPROC_OSD_THANDLE_OPEN,
        LPROC_OSD_THANDLE_CLOSING,
#endif
        LPROC_OSD_NR
};
#endif

/**
 * Storage representation for fids.
 *
 * Variable size, first byte contains the length of the whole record.
 */
struct osd_fid_pack {
        unsigned char fp_len;
        char fp_area[sizeof(struct lu_fid)];
};

struct osd_it_ea_dirent {
        struct lu_fid   oied_fid;
        __u64           oied_ino;
        __u64           oied_off;
        unsigned short  oied_namelen;
        unsigned int    oied_type;
        char            oied_name[0];
} __attribute__((packed));

/**
 * as osd_it_ea_dirent (in memory dirent struct for osd) is greater
 * than lu_dirent struct. osd readdir reads less number of dirent than
 * required for mdd dir page. so buffer size need to be increased so that
 * there  would be one ext3 readdir for every mdd readdir page.
 */

#define OSD_IT_EA_BUFSIZE       (CFS_PAGE_SIZE + CFS_PAGE_SIZE/4)

/**
 * This is iterator's in-memory data structure in interoperability
 * mode (i.e. iterator over ldiskfs style directory)
 */
struct osd_it_ea {
        struct osd_object   *oie_obj;
        /** used in ldiskfs iterator, to stored file pointer */
        struct file          oie_file;
        /** how many entries have been read-cached from storage */
        int                  oie_rd_dirent;
        /** current entry is being iterated by caller */
        int                  oie_it_dirent;
        /** current processing entry */
        struct osd_it_ea_dirent *oie_dirent;
        /** buffer to hold entries, size == OSD_IT_EA_BUFSIZE */
        void                *oie_buf;
};

/**
 * Iterator's in-memory data structure for IAM mode.
 */
struct osd_it_iam {
        struct osd_object     *oi_obj;
        struct iam_path_descr *oi_ipd;
        struct iam_iterator    oi_it;
};

struct osd_thread_info {
        const struct lu_env   *oti_env;
        /**
         * used for index operations.
         */
        struct dentry          oti_obj_dentry;
        struct dentry          oti_child_dentry;

        /** dentry for Iterator context. */
        struct dentry          oti_it_dentry;
        struct htree_lock     *oti_hlock;

        struct lu_fid          oti_fid;
        struct lu_fid          oti_fid2;
        struct osd_inode_id    oti_id;
        struct osd_inode_id    oti_id2;
        /*
         * XXX temporary: for ->i_op calls.
         */
        struct timespec        oti_time;
        /*
         * XXX temporary: fake struct file for osd_object_sync
         */
        struct file            oti_file;
        /*
         * XXX temporary: for capa operations.
         */
        struct lustre_capa_key oti_capa_key;
        struct lustre_capa     oti_capa;

        /** osd_device reference, initialized in osd_trans_start() and
            used in osd_trans_stop() */
        struct osd_device     *oti_dev;

        /**
         * following ipd and it structures are used for osd_index_iam_lookup()
         * these are defined separately as we might do index operation
         * in open iterator session.
         */

        /** osd iterator context used for iterator session */

        union {
                struct osd_it_iam      oti_it;
                /** ldiskfs iterator data structure, see osd_it_ea_{init, fini} */
                struct osd_it_ea       oti_it_ea;
        };

        /** pre-allocated buffer used by oti_it_ea, size OSD_IT_EA_BUFSIZE */
        void                  *oti_it_ea_buf;

        /** IAM iterator for index operation. */
        struct iam_iterator    oti_idx_it;

        /** union to guarantee that ->oti_ipd[] has proper alignment. */
        union {
                char           oti_it_ipd[DX_IPD_MAX_SIZE];
                long long      oti_alignment_lieutenant;
        };

        union {
                char           oti_idx_ipd[DX_IPD_MAX_SIZE];
                long long      oti_alignment_lieutenant_colonel;
        };

        struct lu_object      *oti_obj_rebuild;
        struct lu_object_conf  oti_conf;
        struct osd_idmap_cache oti_cache;
        unsigned int           oti_idmap_valid:1;

        int                    oti_r_locks;
        int                    oti_w_locks;
        int                    oti_txns;
        /** used in osd_fid_set() to put xattr */
        struct lu_buf          oti_buf;
        /** used in osd_ea_fid_set() to set fid into common ea */
        struct lustre_mdt_attrs oti_mdt_attrs;
#ifdef HAVE_QUOTA_SUPPORT
        struct osd_ctxt        oti_ctxt;
#endif
#define OSD_FID_REC_SZ 32
        char                   oti_ldp[OSD_FID_REC_SZ];
        char                   oti_ldp2[OSD_FID_REC_SZ];
};

enum osd_iget_flags {
        /* verify with the fid in LMA */
        OSD_IF_VERIFY   = 1 << 0,

        /* re-generate osd_inode_id */
        OSD_IF_GEN_LID  = 1 << 1,

        /* return fid */
        OSD_IF_RET_FID  = 1 << 2,
};

#ifdef LPROCFS
/* osd_lproc.c */
void lprocfs_osd_init_vars(struct lprocfs_static_vars *lvars);
int osd_procfs_init(struct osd_device *osd, const char *name);
int osd_procfs_fini(struct osd_device *osd);
void osd_lprocfs_time_start(const struct lu_env *env);
void osd_lprocfs_time_end(const struct lu_env *env,
                          struct osd_device *osd, int op);
#endif

/* osd_handler.c */
struct inode *osd_iget(struct osd_device *dev, struct osd_inode_id *id,
                       struct lu_fid *fid, struct dentry *dentry,
                       struct lustre_mdt_attrs *lma, enum osd_iget_flags flags);
struct iam_path_descr *osd_idx_ipd_get(const struct lu_env *env,
                                       const struct iam_container *bag);
void osd_ipd_put(const struct lu_env *env,
                 const struct iam_container *bag,
                 struct iam_path_descr *ipd);
int osd_statfs(const struct lu_env *env, struct dt_device *dev,
               cfs_kstatfs_t *sfs);
int osd_object_auth(const struct lu_env *env, struct dt_object *dt,
                    struct lustre_capa *capa, __u64 opc);
struct dentry *osd_child_dentry_get(const struct lu_env *env,
                                    struct osd_object *obj,
                                    const char *name,
                                    const int namelen);
int osd_ea_fid_get(const struct lu_env *env, struct osd_object *obj,
                   __u32 ino, struct lu_fid *fid);
int osd_get_fid_from_dentry(struct ldiskfs_dir_entry_2 *de,
                            struct dt_rec *fid);

/* osd_scrub.c */
int osd_scrub_setup(const struct lu_env *env, struct dt_device *dt);
void osd_scrub_cleanup(const struct lu_env *env, struct osd_device *dev);
int osd_oii_insert(struct osd_device *dev, struct osd_object *obj,
                   struct osd_idmap_cache *oic);

/*
 * Invariants, assertions.
 */

/*
 * XXX: do not enable this, until invariant checking code is made thread safe
 * in the face of pdirops locking.
 */
#define OSD_INVARIANT_CHECKS (0)

#if OSD_INVARIANT_CHECKS
static inline int osd_invariant(const struct osd_object *obj)
{
        return
                obj != NULL &&
                ergo(obj->oo_inode != NULL,
                     obj->oo_inode->i_sb == osd_sb(osd_obj2dev(obj)) &&
                     atomic_read(&obj->oo_inode->i_count) > 0) &&
                ergo(obj->oo_dir != NULL &&
                     obj->oo_dir->od_conationer.ic_object != NULL,
                     obj->oo_dir->od_conationer.ic_object == obj->oo_inode);
}
#else
#define osd_invariant(obj) (1)
#endif

extern const struct dt_index_operations osd_scrub_ops;
extern const struct lu_device_operations osd_lu_ops;
extern struct lu_context_key osd_key;
extern const char osd_scrub[];
extern const char *oi_scrub_flags_names[];
extern int ldiskfs_pdo;

/*
 * Helpers.
 */
static inline struct osd_thread_info *osd_oti_get(const struct lu_env *env)
{
        return lu_context_key_get(&env->le_ctx, &osd_key);
}

static inline int lu_device_is_osd(const struct lu_device *d)
{
        return ergo(d != NULL && d->ld_ops != NULL, d->ld_ops == &osd_lu_ops);
}

static inline struct osd_device *osd_dt_dev(const struct dt_device *d)
{
        LASSERT(lu_device_is_osd(&d->dd_lu_dev));
        return container_of0(d, struct osd_device, od_dt_dev);
}

static inline struct osd_device *osd_dev(const struct lu_device *d)
{
        LASSERT(lu_device_is_osd(d));
        return osd_dt_dev(container_of0(d, struct dt_device, dd_lu_dev));
}

static inline struct osd_device *osd_obj2dev(const struct osd_object *o)
{
        return osd_dev(o->oo_dt.do_lu.lo_dev);
}

static inline struct super_block *osd_sb(const struct osd_device *dev)
{
        return dev->od_mount->lmi_mnt->mnt_sb;
}

static inline int osd_object_is_root(const struct osd_object *obj)
{
        return osd_sb(osd_obj2dev(obj))->s_root->d_inode == obj->oo_inode;
}

static inline struct osd_object *osd_obj(const struct lu_object *o)
{
        LASSERT(lu_device_is_osd(o->lo_dev));
        return container_of0(o, struct osd_object, oo_dt.do_lu);
}

static inline struct osd_object *osd_dt_obj(const struct dt_object *d)
{
        return osd_obj(&d->do_lu);
}

static inline struct lu_device *osd2lu_dev(struct osd_device *osd)
{
        return &osd->od_dt_dev.dd_lu_dev;
}

static inline journal_t *osd_journal(const struct osd_device *dev)
{
        return LDISKFS_SB(osd_sb(dev))->s_journal;
}

static inline int osd_has_index(const struct osd_object *obj)
{
        return obj->oo_dt.do_index_ops != NULL;
}

static inline int osd_object_invariant(const struct lu_object *l)
{
        return osd_invariant(osd_obj(l));
}

static inline struct osd_oi *
osd_fid2oi(struct osd_device *osd, const struct lu_fid *fid)
{
        if (!fid_is_norm(fid))
                return NULL;

        LASSERT(osd->od_oi_table != NULL && osd->od_oi_count >= 1);
        /* It can work even od_oi_count equals to 1 although it's unexpected,
         * the only reason we set it to 1 is for performance measurement */
        return &osd->od_oi_table[fid->f_seq & (osd->od_oi_count - 1)];
}

/*
 * Concurrency: no concurrent access is possible that late in object
 * life-cycle (for all existing callers, that is. New callers have to provide
 * their own locking.)
 */
static inline int osd_inode_unlinked(const struct inode *inode)
{
        return inode->i_nlink == 0;
}

static inline void osd_scrub_pos2str(__u8 *key, __u32 lid)
{
        sprintf(key, "%u", lid);
}

static inline int osd_scrub_str2pos(__u32 *lid, __u8 *key)
{
        int rc = 1;

        if (scrub_key_is_empty(key))
                *lid = 0;
        else
                rc = sscanf((const char *)key, "%u", lid);

        return rc > 0 ? 0 : -EINVAL;
}

#endif /* __KERNEL__ */
#endif /* _OSD_INTERNAL_H */
