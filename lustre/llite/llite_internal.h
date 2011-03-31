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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef LLITE_INTERNAL_H
#define LLITE_INTERNAL_H

#include <lustre_acl.h>

#include <lustre_debug.h>
#include <lustre_ver.h>
#include <lustre_disk.h>  /* for s2sbi */
#include <lustre_eacl.h>

/* for struct cl_lock_descr and struct cl_io */
#include <cl_object.h>
#include <lclient.h>

#ifndef FMODE_EXEC
#define FMODE_EXEC 0
#endif

#ifndef DCACHE_LUSTRE_INVALID
#define DCACHE_LUSTRE_INVALID 0x4000000
#endif

#define LL_IT2STR(it) ((it) ? ldlm_it2str((it)->it_op) : "0")
#define LUSTRE_FPRIVATE(file) ((file)->private_data)

struct ll_dentry_data {
        int                      lld_cwd_count;
        int                      lld_mnt_count;
        struct obd_client_handle lld_cwd_och;
        struct obd_client_handle lld_mnt_och;
        struct lookup_intent    *lld_it;
        unsigned int             lld_sa_generation;
};

#define ll_d2d(de) ((struct ll_dentry_data*)((de)->d_fsdata))

extern struct file_operations ll_pgcache_seq_fops;

#define LLI_INODE_MAGIC                 0x111d0de5
#define LLI_INODE_DEAD                  0xdeadd00d

/* remote client permission cache */
#define REMOTE_PERM_HASHSIZE 16

/* llite setxid/access permission for user on remote client */
struct ll_remote_perm {
        cfs_hlist_node_t        lrp_list;
        uid_t                   lrp_uid;
        gid_t                   lrp_gid;
        uid_t                   lrp_fsuid;
        gid_t                   lrp_fsgid;
        int                     lrp_access_perm; /* MAY_READ/WRITE/EXEC, this
                                                    is access permission with
                                                    lrp_fsuid/lrp_fsgid. */
};

enum lli_flags {
        /* MDS has an authority for the Size-on-MDS attributes. */
        LLIF_MDS_SIZE_LOCK      = (1 << 0),
        /* Epoch close is postponed. */
        LLIF_EPOCH_PENDING      = (1 << 1),
        /* DONE WRITING is allowed. */
        LLIF_DONE_WRITING       = (1 << 2),
        /* Sizeon-on-MDS attributes are changed. An attribute update needs to
         * be sent to MDS. */
        LLIF_SOM_DIRTY          = (1 << 3),
        /* File is contented */
        LLIF_CONTENDED         = (1 << 4),
        /* Truncate uses server lock for this file */
        LLIF_SRVLOCK           = (1 << 5)

};

struct ll_inode_info {
        int                     lli_inode_magic;
        cfs_semaphore_t         lli_size_sem;           /* protect open and change size */
        void                   *lli_size_sem_owner;
        cfs_semaphore_t         lli_write_sem;
        cfs_rw_semaphore_t      lli_trunc_sem;
        char                   *lli_symlink_name;
        __u64                   lli_maxbytes;
        __u64                   lli_ioepoch;
        unsigned long           lli_flags;
        cfs_time_t              lli_contention_time;

        /* this lock protects posix_acl, pending_write_llaps, mmap_cnt */
        cfs_spinlock_t          lli_lock;
        cfs_list_t              lli_close_list;
        /* handle is to be sent to MDS later on done_writing and setattr.
         * Open handle data are needed for the recovery to reconstruct
         * the inode state on the MDS. XXX: recovery is not ready yet. */
        struct obd_client_handle *lli_pending_och;

        /* for writepage() only to communicate to fsync */
        int                     lli_async_rc;
        int                     lli_write_rc;

        struct posix_acl       *lli_posix_acl;

        /* remote permission hash */
        cfs_hlist_head_t       *lli_remote_perms;
        unsigned long           lli_rmtperm_utime;
        cfs_semaphore_t         lli_rmtperm_sem;

        cfs_list_t              lli_dead_list;

        cfs_semaphore_t         lli_och_sem; /* Protects access to och pointers
                                                and their usage counters, also
                                                atomicity of check-update of
                                                lli_smd */
        /* We need all three because every inode may be opened in different
           modes */
        struct obd_client_handle *lli_mds_read_och;
        __u64                   lli_open_fd_read_count;
        struct obd_client_handle *lli_mds_write_och;
        __u64                   lli_open_fd_write_count;
        struct obd_client_handle *lli_mds_exec_och;
        __u64                   lli_open_fd_exec_count;

        struct inode            lli_vfs_inode;

        /* identifying fields for both metadata and data stacks. */
        struct lu_fid           lli_fid;
        struct lov_stripe_md   *lli_smd;

        /* fid capability */
        /* open count currently used by capability only, indicate whether
         * capability needs renewal */
        cfs_atomic_t            lli_open_count;
        struct obd_capa        *lli_mds_capa;
        cfs_list_t              lli_oss_capas;

        /* metadata statahead */
        /* protect statahead stuff: lli_opendir_pid, lli_opendir_key, lli_sai,
         * and so on. */
        cfs_spinlock_t          lli_sa_lock;
        /*
         * "opendir_pid" is the token when lookup/revalid -- I am the owner of
         * dir statahead.
         */
        pid_t                   lli_opendir_pid;
        /*
         * since parent-child threads can share the same @file struct,
         * "opendir_key" is the token when dir close for case of parent exit
         * before child -- it is me should cleanup the dir readahead. */
        void                   *lli_opendir_key;
        struct ll_statahead_info *lli_sai;
        __u64                   lli_sa_pos;
        struct cl_object       *lli_clob;
        /* the most recent timestamps obtained from mds */
        struct ost_lvb          lli_lvb;
        /**
         * serialize normal readdir and statahead-readdir
         */
        cfs_semaphore_t         lli_readdir_sem;
};

/*
 * Locking to guarantee consistency of non-atomic updates to long long i_size,
 * consistency between file size and KMS, and consistency within
 * ->lli_smd->lsm_oinfo[]'s.
 *
 * Implemented by ->lli_size_sem and ->lsm_sem, nested in that order.
 */

void ll_inode_size_lock(struct inode *inode, int lock_lsm);
void ll_inode_size_unlock(struct inode *inode, int unlock_lsm);

// FIXME: replace the name of this with LL_I to conform to kernel stuff
// static inline struct ll_inode_info *LL_I(struct inode *inode)
static inline struct ll_inode_info *ll_i2info(struct inode *inode)
{
        return container_of(inode, struct ll_inode_info, lli_vfs_inode);
}

/* default to about 40meg of readahead on a given system.  That much tied
 * up in 512k readahead requests serviced at 40ms each is about 1GB/s. */
#define SBI_DEFAULT_READAHEAD_MAX (40UL << (20 - CFS_PAGE_SHIFT))

/* default to read-ahead full files smaller than 2MB on the second read */
#define SBI_DEFAULT_READAHEAD_WHOLE_MAX (2UL << (20 - CFS_PAGE_SHIFT))

enum ra_stat {
        RA_STAT_HIT = 0,
        RA_STAT_MISS,
        RA_STAT_DISTANT_READPAGE,
        RA_STAT_MISS_IN_WINDOW,
        RA_STAT_FAILED_GRAB_PAGE,
        RA_STAT_FAILED_MATCH,
        RA_STAT_DISCARDED,
        RA_STAT_ZERO_LEN,
        RA_STAT_ZERO_WINDOW,
        RA_STAT_EOF,
        RA_STAT_MAX_IN_FLIGHT,
        RA_STAT_WRONG_GRAB_PAGE,
        _NR_RA_STAT,
};

struct ll_ra_info {
        cfs_atomic_t              ra_cur_pages;
        unsigned long             ra_max_pages;
        unsigned long             ra_max_pages_per_file;
        unsigned long             ra_max_read_ahead_whole_pages;
};

/* ra_io_arg will be filled in the beginning of ll_readahead with
 * ras_lock, then the following ll_read_ahead_pages will read RA
 * pages according to this arg, all the items in this structure are
 * counted by page index.
 */
struct ra_io_arg {
        unsigned long ria_start;  /* start offset of read-ahead*/
        unsigned long ria_end;    /* end offset of read-ahead*/
        /* If stride read pattern is detected, ria_stoff means where
         * stride read is started. Note: for normal read-ahead, the
         * value here is meaningless, and also it will not be accessed*/
        pgoff_t ria_stoff;
        /* ria_length and ria_pages are the length and pages length in the
         * stride I/O mode. And they will also be used to check whether
         * it is stride I/O read-ahead in the read-ahead pages*/
        unsigned long ria_length;
        unsigned long ria_pages;
};

/* LL_HIST_MAX=32 causes an overflow */
#define LL_HIST_MAX 28
#define LL_HIST_START 12 /* buckets start at 2^12 = 4k */
#define LL_PROCESS_HIST_MAX 10
struct per_process_info {
        pid_t pid;
        struct obd_histogram pp_r_hist;
        struct obd_histogram pp_w_hist;
};

/* pp_extents[LL_PROCESS_HIST_MAX] will hold the combined process info */
struct ll_rw_extents_info {
        struct per_process_info pp_extents[LL_PROCESS_HIST_MAX + 1];
};

#define LL_OFFSET_HIST_MAX 100
struct ll_rw_process_info {
        pid_t                     rw_pid;
        int                       rw_op;
        loff_t                    rw_range_start;
        loff_t                    rw_range_end;
        loff_t                    rw_last_file_pos;
        loff_t                    rw_offset;
        size_t                    rw_smallest_extent;
        size_t                    rw_largest_extent;
        struct ll_file_data      *rw_last_file;
};

enum stats_track_type {
        STATS_TRACK_ALL = 0,  /* track all processes */
        STATS_TRACK_PID,      /* track process with this pid */
        STATS_TRACK_PPID,     /* track processes with this ppid */
        STATS_TRACK_GID,      /* track processes with this gid */
        STATS_TRACK_LAST,
};

/* flags for sbi->ll_flags */
#define LL_SBI_NOLCK             0x01 /* DLM locking disabled (directio-only) */
#define LL_SBI_CHECKSUM          0x02 /* checksum each page as it's written */
#define LL_SBI_FLOCK             0x04
#define LL_SBI_USER_XATTR        0x08 /* support user xattr */
#define LL_SBI_ACL               0x10 /* support ACL */
#define LL_SBI_RMT_CLIENT        0x40 /* remote client */
#define LL_SBI_MDS_CAPA          0x80 /* support mds capa */
#define LL_SBI_OSS_CAPA         0x100 /* support oss capa */
#define LL_SBI_LOCALFLOCK       0x200 /* Local flocks support by kernel */
#define LL_SBI_LRU_RESIZE       0x400 /* lru resize support */
#define LL_SBI_LAZYSTATFS       0x800 /* lazystatfs mount option */
#define LL_SBI_SOM_PREVIEW      0x1000 /* SOM preview mount option */
#define LL_SBI_32BIT_API        0x2000 /* generate 32 bit inodes. */

/* default value for ll_sb_info->contention_time */
#define SBI_DEFAULT_CONTENTION_SECONDS     60
/* default value for lockless_truncate_enable */
#define SBI_DEFAULT_LOCKLESS_TRUNCATE_ENABLE 1
#define RCE_HASHES      32

struct rmtacl_ctl_entry {
        cfs_list_t       rce_list;
        pid_t            rce_key; /* hash key */
        int              rce_ops; /* acl operation type */
};

struct rmtacl_ctl_table {
        cfs_spinlock_t   rct_lock;
        cfs_list_t       rct_entries[RCE_HASHES];
};

#define EE_HASHES       32

struct eacl_entry {
        cfs_list_t            ee_list;
        pid_t                 ee_key; /* hash key */
        struct lu_fid         ee_fid;
        int                   ee_type; /* ACL type for ACCESS or DEFAULT */
        ext_acl_xattr_header *ee_acl;
};

struct eacl_table {
        cfs_spinlock_t   et_lock;
        cfs_list_t       et_entries[EE_HASHES];
};

struct ll_sb_info {
        cfs_list_t                ll_list;
        /* this protects pglist and ra_info.  It isn't safe to
         * grab from interrupt contexts */
        cfs_spinlock_t            ll_lock;
        cfs_spinlock_t            ll_pp_extent_lock; /* Lock for pp_extent entries */
        cfs_spinlock_t            ll_process_lock; /* Lock for ll_rw_process_info */
        struct obd_uuid           ll_sb_uuid;
        struct obd_export        *ll_md_exp;
        struct obd_export        *ll_dt_exp;
        struct proc_dir_entry*    ll_proc_root;
        struct lu_fid             ll_root_fid; /* root object fid */

        int                       ll_flags;
        cfs_list_t                ll_conn_chain; /* per-conn chain of SBs */
        struct lustre_client_ocd  ll_lco;

        cfs_list_t                ll_orphan_dentry_list; /*please don't ask -p*/
        struct ll_close_queue    *ll_lcq;

        struct lprocfs_stats     *ll_stats; /* lprocfs stats counter */

        unsigned long             ll_async_page_max;
        unsigned long             ll_async_page_count;

        struct lprocfs_stats     *ll_ra_stats;

        struct ll_ra_info         ll_ra_info;
        unsigned int              ll_namelen;
        struct file_operations   *ll_fop;

        /* =0 - hold lock over whole read/write
         * >0 - max. chunk to be read/written w/o lock re-acquiring */
        unsigned long             ll_max_rw_chunk;

        struct lu_site           *ll_site;
        struct cl_device         *ll_cl;
        /* Statistics */
        struct ll_rw_extents_info ll_rw_extents_info;
        int                       ll_extent_process_count;
        struct ll_rw_process_info ll_rw_process_info[LL_PROCESS_HIST_MAX];
        unsigned int              ll_offset_process_count;
        struct ll_rw_process_info ll_rw_offset_info[LL_OFFSET_HIST_MAX];
        unsigned int              ll_rw_offset_entry_count;
        enum stats_track_type     ll_stats_track_type;
        int                       ll_stats_track_id;
        int                       ll_rw_stats_on;

        /* metadata stat-ahead */
        unsigned int              ll_sa_max;     /* max statahead RPCs */
        atomic_t                  ll_sa_total;   /* statahead thread started
                                                  * count */
        atomic_t                  ll_sa_wrong;   /* statahead thread stopped for
                                                  * low hit ratio */

        dev_t                     ll_sdev_orig; /* save s_dev before assign for
                                                 * clustred nfs */
        struct rmtacl_ctl_table   ll_rct;
        struct eacl_table         ll_et;
};

#define LL_DEFAULT_MAX_RW_CHUNK      (32 * 1024 * 1024)

struct ll_ra_read {
        pgoff_t             lrr_start;
        pgoff_t             lrr_count;
        struct task_struct *lrr_reader;
        cfs_list_t          lrr_linkage;
};

/*
 * per file-descriptor read-ahead data.
 */
struct ll_readahead_state {
        cfs_spinlock_t  ras_lock;
        /*
         * index of the last page that read(2) needed and that wasn't in the
         * cache. Used by ras_update() to detect seeks.
         *
         * XXX nikita: if access seeks into cached region, Lustre doesn't see
         * this.
         */
        unsigned long   ras_last_readpage;
        /*
         * number of pages read after last read-ahead window reset. As window
         * is reset on each seek, this is effectively a number of consecutive
         * accesses. Maybe ->ras_accessed_in_window is better name.
         *
         * XXX nikita: window is also reset (by ras_update()) when Lustre
         * believes that memory pressure evicts read-ahead pages. In that
         * case, it probably doesn't make sense to expand window to
         * PTLRPC_MAX_BRW_PAGES on the third access.
         */
        unsigned long   ras_consecutive_pages;
        /*
         * number of read requests after the last read-ahead window reset
         * As window is reset on each seek, this is effectively the number
         * on consecutive read request and is used to trigger read-ahead.
         */
        unsigned long   ras_consecutive_requests;
        /*
         * Parameters of current read-ahead window. Handled by
         * ras_update(). On the initial access to the file or after a seek,
         * window is reset to 0. After 3 consecutive accesses, window is
         * expanded to PTLRPC_MAX_BRW_PAGES. Afterwards, window is enlarged by
         * PTLRPC_MAX_BRW_PAGES chunks up to ->ra_max_pages.
         */
        unsigned long   ras_window_start, ras_window_len;
        /*
         * Where next read-ahead should start at. This lies within read-ahead
         * window. Read-ahead window is read in pieces rather than at once
         * because: 1. lustre limits total number of pages under read-ahead by
         * ->ra_max_pages (see ll_ra_count_get()), 2. client cannot read pages
         * not covered by DLM lock.
         */
        unsigned long   ras_next_readahead;
        /*
         * Total number of ll_file_read requests issued, reads originating
         * due to mmap are not counted in this total.  This value is used to
         * trigger full file read-ahead after multiple reads to a small file.
         */
        unsigned long   ras_requests;
        /*
         * Page index with respect to the current request, these value
         * will not be accurate when dealing with reads issued via mmap.
         */
        unsigned long   ras_request_index;
        /*
         * list of struct ll_ra_read's one per read(2) call current in
         * progress against this file descriptor. Used by read-ahead code,
         * protected by ->ras_lock.
         */
        cfs_list_t      ras_read_beads;
        /*
         * The following 3 items are used for detecting the stride I/O
         * mode.
         * In stride I/O mode,
         * ...............|-----data-----|****gap*****|--------|******|....
         *    offset      |-stride_pages-|-stride_gap-|
         * ras_stride_offset = offset;
         * ras_stride_length = stride_pages + stride_gap;
         * ras_stride_pages = stride_pages;
         * Note: all these three items are counted by pages.
         */
        unsigned long   ras_stride_length;
        unsigned long   ras_stride_pages;
        pgoff_t         ras_stride_offset;
        /*
         * number of consecutive stride request count, and it is similar as
         * ras_consecutive_requests, but used for stride I/O mode.
         * Note: only more than 2 consecutive stride request are detected,
         * stride read-ahead will be enable
         */
        unsigned long   ras_consecutive_stride_requests;
};

struct ll_file_dir {
        __u64 lfd_pos;
        __u64 lfd_next;
};

extern cfs_mem_cache_t *ll_file_data_slab;
struct lustre_handle;
struct ll_file_data {
        struct ll_readahead_state fd_ras;
        int fd_omode;
        struct ccc_grouplock fd_grouplock;
        struct ll_file_dir fd_dir;
        __u32 fd_flags;
        struct file *fd_file;
};

struct lov_stripe_md;

extern cfs_spinlock_t inode_lock;

extern struct proc_dir_entry *proc_lustre_fs_root;

static inline struct inode *ll_info2i(struct ll_inode_info *lli)
{
        return &lli->lli_vfs_inode;
}

struct it_cb_data {
        struct inode  *icbd_parent;
        struct dentry **icbd_childp;
        obd_id        hash;
};

__u32 ll_i2suppgid(struct inode *i);
void ll_i2gids(__u32 *suppgids, struct inode *i1,struct inode *i2);

static inline int ll_need_32bit_api(struct ll_sb_info *sbi)
{
#if BITS_PER_LONG == 32
        return 1;
#else
        return unlikely(cfs_curproc_is_32bit() || (sbi->ll_flags & LL_SBI_32BIT_API));
#endif
}

#define LLAP_MAGIC 98764321

extern cfs_mem_cache_t *ll_async_page_slab;
extern size_t ll_async_page_slab_size;

void ll_ra_read_in(struct file *f, struct ll_ra_read *rar);
void ll_ra_read_ex(struct file *f, struct ll_ra_read *rar);
struct ll_ra_read *ll_ra_read_get(struct file *f);

/* llite/lproc_llite.c */
#ifdef LPROCFS
int lprocfs_register_mountpoint(struct proc_dir_entry *parent,
                                struct super_block *sb, char *osc, char *mdc);
void lprocfs_unregister_mountpoint(struct ll_sb_info *sbi);
void ll_stats_ops_tally(struct ll_sb_info *sbi, int op, int count);
void lprocfs_llite_init_vars(struct lprocfs_static_vars *lvars);
#else
static inline int lprocfs_register_mountpoint(struct proc_dir_entry *parent,
                        struct super_block *sb, char *osc, char *mdc){return 0;}
static inline void lprocfs_unregister_mountpoint(struct ll_sb_info *sbi) {}
static void ll_stats_ops_tally(struct ll_sb_info *sbi, int op, int count) {}
static void lprocfs_llite_init_vars(struct lprocfs_static_vars *lvars)
{
        memset(lvars, 0, sizeof(*lvars));
}
#endif


/* llite/dir.c */
static inline void ll_put_page(struct page *page)
{
        kunmap(page);
        page_cache_release(page);
}

extern struct file_operations ll_dir_operations;
extern struct inode_operations ll_dir_inode_operations;
struct page *ll_get_dir_page(struct file *filp, struct inode *dir, __u64 hash,
                             int exact, struct ll_dir_chain *chain);

int ll_get_mdt_idx(struct inode *inode);
/* llite/namei.c */
int ll_objects_destroy(struct ptlrpc_request *request,
                       struct inode *dir);
struct inode *ll_iget(struct super_block *sb, ino_t hash,
                      struct lustre_md *lic);
int ll_md_blocking_ast(struct ldlm_lock *, struct ldlm_lock_desc *,
                       void *data, int flag);
struct lookup_intent *ll_convert_intent(struct open_intent *oit,
                                        int lookup_flags);
int ll_lookup_it_finish(struct ptlrpc_request *request,
                        struct lookup_intent *it, void *data);

/* llite/rw.c */
int ll_prepare_write(struct file *, struct page *, unsigned from, unsigned to);
int ll_commit_write(struct file *, struct page *, unsigned from, unsigned to);
int ll_writepage(struct page *page, struct writeback_control *wbc);
void ll_removepage(struct page *page);
int ll_readpage(struct file *file, struct page *page);
void ll_readahead_init(struct inode *inode, struct ll_readahead_state *ras);
void ll_truncate(struct inode *inode);
int ll_file_punch(struct inode *, loff_t, int);
ssize_t ll_file_lockless_io(struct file *, char *, size_t, loff_t *, int);
void ll_clear_file_contended(struct inode*);
int ll_sync_page_range(struct inode *, struct address_space *, loff_t, size_t);
int ll_readahead(const struct lu_env *env, struct cl_io *io, struct ll_readahead_state *ras,
                 struct address_space *mapping, struct cl_page_list *queue, int flags);

/* llite/file.c */
extern struct file_operations ll_file_operations;
extern struct file_operations ll_file_operations_flock;
extern struct file_operations ll_file_operations_noflock;
extern struct inode_operations ll_file_inode_operations;
extern int ll_inode_revalidate_it(struct dentry *, struct lookup_intent *);
extern int ll_have_md_lock(struct inode *inode, __u64 bits, ldlm_mode_t l_req_mode);
extern ldlm_mode_t ll_take_md_lock(struct inode *inode, __u64 bits,
                                   struct lustre_handle *lockh);
int __ll_inode_revalidate_it(struct dentry *, struct lookup_intent *,  __u64 bits);
int ll_revalidate_nd(struct dentry *dentry, struct nameidata *nd);
int ll_file_open(struct inode *inode, struct file *file);
int ll_file_release(struct inode *inode, struct file *file);
int ll_glimpse_ioctl(struct ll_sb_info *sbi,
                     struct lov_stripe_md *lsm, lstat_t *st);
void ll_ioepoch_open(struct ll_inode_info *lli, __u64 ioepoch);
int ll_local_open(struct file *file,
                  struct lookup_intent *it, struct ll_file_data *fd,
                  struct obd_client_handle *och);
int ll_release_openhandle(struct dentry *, struct lookup_intent *);
int ll_md_close(struct obd_export *md_exp, struct inode *inode,
                struct file *file);
int ll_md_real_close(struct inode *inode, int flags);
void ll_ioepoch_close(struct inode *inode, struct md_op_data *op_data,
                      struct obd_client_handle **och, unsigned long flags);
void ll_done_writing_attr(struct inode *inode, struct md_op_data *op_data);
int ll_som_update(struct inode *inode, struct md_op_data *op_data);
int ll_inode_getattr(struct inode *inode, struct obdo *obdo,
                     __u64 ioepoch, int sync);
int ll_md_setattr(struct inode *inode, struct md_op_data *op_data,
                  struct md_open_data **mod);
void ll_pack_inode2opdata(struct inode *inode, struct md_op_data *op_data,
                          struct lustre_handle *fh);
extern void ll_rw_stats_tally(struct ll_sb_info *sbi, pid_t pid,
                              struct ll_file_data *file, loff_t pos,
                              size_t count, int rw);
int ll_getattr_it(struct vfsmount *mnt, struct dentry *de,
               struct lookup_intent *it, struct kstat *stat);
int ll_getattr(struct vfsmount *mnt, struct dentry *de, struct kstat *stat);
struct ll_file_data *ll_file_data_get(void);
#ifndef HAVE_INODE_PERMISION_2ARGS
int ll_inode_permission(struct inode *inode, int mask, struct nameidata *nd);
#else
int ll_inode_permission(struct inode *inode, int mask);
#endif
int ll_lov_setstripe_ea_info(struct inode *inode, struct file *file,
                             int flags, struct lov_user_md *lum,
                             int lum_size);
int ll_lov_getstripe_ea_info(struct inode *inode, const char *filename,
                             struct lov_mds_md **lmm, int *lmm_size,
                             struct ptlrpc_request **request);
int ll_dir_setstripe(struct inode *inode, struct lov_user_md *lump,
                     int set_default);
int ll_dir_getstripe(struct inode *inode, struct lov_mds_md **lmm,
                     int *lmm_size, struct ptlrpc_request **request);
int ll_fsync(struct file *file, struct dentry *dentry, int data);
int ll_do_fiemap(struct inode *inode, struct ll_user_fiemap *fiemap,
              int num_bytes);
int ll_merge_lvb(struct inode *inode);
int ll_get_grouplock(struct inode *inode, struct file *file, unsigned long arg);
int ll_put_grouplock(struct inode *inode, struct file *file, unsigned long arg);
int ll_fid2path(struct obd_export *exp, void *arg);

/* llite/dcache.c */
/* llite/namei.c */
/**
 * protect race ll_find_aliases vs ll_revalidate_it vs ll_unhash_aliases
 */
int ll_dops_init(struct dentry *de, int block);
extern cfs_spinlock_t ll_lookup_lock;
extern struct dentry_operations ll_d_ops;
void ll_intent_drop_lock(struct lookup_intent *);
void ll_intent_release(struct lookup_intent *);
int ll_drop_dentry(struct dentry *dentry);
int ll_drop_dentry(struct dentry *dentry);
void ll_unhash_aliases(struct inode *);
void ll_frob_intent(struct lookup_intent **itp, struct lookup_intent *deft);
void ll_lookup_finish_locks(struct lookup_intent *it, struct dentry *dentry);
int ll_dcompare(struct dentry *parent, struct qstr *d_name, struct qstr *name);
int ll_revalidate_it_finish(struct ptlrpc_request *request,
                            struct lookup_intent *it, struct dentry *de);

/* llite/llite_lib.c */
extern struct super_operations lustre_super_operations;

char *ll_read_opt(const char *opt, char *data);
void ll_lli_init(struct ll_inode_info *lli);
int ll_fill_super(struct super_block *sb);
void ll_put_super(struct super_block *sb);
void ll_kill_super(struct super_block *sb);
struct inode *ll_inode_from_lock(struct ldlm_lock *lock);
void ll_clear_inode(struct inode *inode);
int ll_setattr_raw(struct inode *inode, struct iattr *attr);
int ll_setattr(struct dentry *de, struct iattr *attr);
#ifndef HAVE_STATFS_DENTRY_PARAM
int ll_statfs(struct super_block *sb, struct kstatfs *sfs);
#else
int ll_statfs(struct dentry *de, struct kstatfs *sfs);
#endif
int ll_statfs_internal(struct super_block *sb, struct obd_statfs *osfs,
                       __u64 max_age, __u32 flags);
void ll_update_inode(struct inode *inode, struct lustre_md *md);
void ll_read_inode2(struct inode *inode, void *opaque);
void ll_delete_inode(struct inode *inode);
int ll_iocontrol(struct inode *inode, struct file *file,
                 unsigned int cmd, unsigned long arg);
int ll_flush_ctx(struct inode *inode);
#ifdef HAVE_UMOUNTBEGIN_VFSMOUNT
void ll_umount_begin(struct vfsmount *vfsmnt, int flags);
#else
void ll_umount_begin(struct super_block *sb);
#endif
int ll_remount_fs(struct super_block *sb, int *flags, char *data);
int ll_show_options(struct seq_file *seq, struct vfsmount *vfs);
int ll_prep_inode(struct inode **inode, struct ptlrpc_request *req,
                  struct super_block *);
void lustre_dump_dentry(struct dentry *, int recur);
void lustre_dump_inode(struct inode *);
int ll_obd_statfs(struct inode *inode, void *arg);
int ll_get_max_mdsize(struct ll_sb_info *sbi, int *max_mdsize);
int ll_process_config(struct lustre_cfg *lcfg);
struct md_op_data *ll_prep_md_op_data(struct md_op_data *op_data,
                                      struct inode *i1, struct inode *i2,
                                      const char *name, int namelen,
                                      int mode, __u32 opc, void *data);
void ll_finish_md_op_data(struct md_op_data *op_data);

/* llite/llite_nfs.c */
extern struct export_operations lustre_export_operations;
__u32 get_uuid2int(const char *name, int len);

/* llite/special.c */
extern struct inode_operations ll_special_inode_operations;
extern struct file_operations ll_special_chr_inode_fops;
extern struct file_operations ll_special_chr_file_fops;
extern struct file_operations ll_special_blk_inode_fops;
extern struct file_operations ll_special_fifo_inode_fops;
extern struct file_operations ll_special_fifo_file_fops;
extern struct file_operations ll_special_sock_inode_fops;

/* llite/symlink.c */
extern struct inode_operations ll_fast_symlink_inode_operations;

/* llite/llite_close.c */
struct ll_close_queue {
        cfs_spinlock_t          lcq_lock;
        cfs_list_t              lcq_head;
        cfs_waitq_t             lcq_waitq;
        cfs_completion_t        lcq_comp;
        cfs_atomic_t            lcq_stop;
};

struct ccc_object *cl_inode2ccc(struct inode *inode);


void vvp_write_pending (struct ccc_object *club, struct ccc_page *page);
void vvp_write_complete(struct ccc_object *club, struct ccc_page *page);

/* specific achitecture can implement only part of this list */
enum vvp_io_subtype {
        /** normal IO */
        IO_NORMAL,
        /** io called from .sendfile */
        IO_SENDFILE,
        /** io started from splice_{read|write} */
        IO_SPLICE
};

/* IO subtypes */
struct vvp_io {
        /** io subtype */
        enum vvp_io_subtype    cui_io_subtype;

        union {
                struct {
                        read_actor_t      cui_actor;
                        void             *cui_target;
                } sendfile;
                struct {
                        struct pipe_inode_info *cui_pipe;
                        unsigned int            cui_flags;
                } splice;
                struct vvp_fault_io {
                        /**
                         * Inode modification time that is checked across DLM
                         * lock request.
                         */
                        time_t                 ft_mtime;
                        struct vm_area_struct *ft_vma;
                        /**
                         *  locked page returned from vvp_io
                         */
                        cfs_page_t            *ft_vmpage;
#ifndef HAVE_VM_OP_FAULT
                        struct vm_nopage_api {
                                /**
                                 * Virtual address at which fault occurred.
                                 */
                                unsigned long   ft_address;
                                /**
                                 * Fault type, as to be supplied to
                                 * filemap_nopage().
                                 */
                                int             *ft_type;
                        } nopage;
#else
                        struct vm_fault_api {
                                /**
                                 * kernel fault info
                                 */
                                struct vm_fault *ft_vmf;
                                /**
                                 * fault API used bitflags for return code.
                                 */
                                unsigned int    ft_flags;
                        } fault;
#endif
                } fault;
        } u;
        /**
         * Read-ahead state used by read and page-fault IO contexts.
         */
        struct ll_ra_read    cui_bead;
        /**
         * Set when cui_bead has been initialized.
         */
        int                  cui_ra_window_set;
        /**
         * Partially truncated page, that vvp_io_trunc_start() keeps locked
         * across truncate.
         */
        struct cl_page      *cui_partpage;
};

/**
 * IO arguments for various VFS I/O interfaces.
 */
struct vvp_io_args {
        /** normal/sendfile/splice */
        enum vvp_io_subtype via_io_subtype;

        union {
                struct {
#ifndef HAVE_FILE_WRITEV
                        struct kiocb      *via_iocb;
#endif
                        struct iovec      *via_iov;
                        unsigned long      via_nrsegs;
                } normal;
                struct {
                        read_actor_t       via_actor;
                        void              *via_target;
                } sendfile;
                struct {
                        struct pipe_inode_info  *via_pipe;
                        unsigned int       via_flags;
                } splice;
        } u;
};

struct ll_cl_context {
        void           *lcc_cookie;
        struct cl_io   *lcc_io;
        struct cl_page *lcc_page;
        struct lu_env  *lcc_env;
        int             lcc_refcheck;
        int             lcc_created;
};

struct vvp_thread_info {
        struct ost_lvb       vti_lvb;
        struct cl_2queue     vti_queue;
        struct iovec         vti_local_iov;
        struct vvp_io_args   vti_args;
        struct ra_io_arg     vti_ria;
        struct kiocb         vti_kiocb;
        struct ll_cl_context vti_io_ctx;
};

static inline struct vvp_thread_info *vvp_env_info(const struct lu_env *env)
{
        extern struct lu_context_key vvp_key;
        struct vvp_thread_info      *info;

        info = lu_context_key_get(&env->le_ctx, &vvp_key);
        LASSERT(info != NULL);
        return info;
}

static inline struct vvp_io_args *vvp_env_args(const struct lu_env *env,
                                               enum vvp_io_subtype type)
{
        struct vvp_io_args *ret = &vvp_env_info(env)->vti_args;

        ret->via_io_subtype = type;

        return ret;
}

struct vvp_session {
        struct vvp_io         vs_ios;
};

static inline struct vvp_session *vvp_env_session(const struct lu_env *env)
{
        extern struct lu_context_key vvp_session_key;
        struct vvp_session *ses;

        ses = lu_context_key_get(env->le_ses, &vvp_session_key);
        LASSERT(ses != NULL);
        return ses;
}

static inline struct vvp_io *vvp_env_io(const struct lu_env *env)
{
        return &vvp_env_session(env)->vs_ios;
}

void ll_queue_done_writing(struct inode *inode, unsigned long flags);
void ll_close_thread_shutdown(struct ll_close_queue *lcq);
int ll_close_thread_start(struct ll_close_queue **lcq_ret);

/* llite/llite_mmap.c */
typedef struct rb_root  rb_root_t;
typedef struct rb_node  rb_node_t;

struct ll_lock_tree_node;
struct ll_lock_tree {
        rb_root_t                       lt_root;
        cfs_list_t                      lt_locked_list;
        struct ll_file_data            *lt_fd;
};

int ll_teardown_mmaps(struct address_space *mapping, __u64 first, __u64 last);
int ll_file_mmap(struct file * file, struct vm_area_struct * vma);
struct ll_lock_tree_node * ll_node_from_inode(struct inode *inode, __u64 start,
                                              __u64 end, ldlm_mode_t mode);
void policy_from_vma(ldlm_policy_data_t *policy,
                struct vm_area_struct *vma, unsigned long addr, size_t count);
struct vm_area_struct *our_vma(unsigned long addr, size_t count);

#define    ll_s2sbi(sb)        (s2lsi(sb)->lsi_llsbi)

/* don't need an addref as the sb_info should be holding one */
static inline struct obd_export *ll_s2dtexp(struct super_block *sb)
{
        return ll_s2sbi(sb)->ll_dt_exp;
}

/* don't need an addref as the sb_info should be holding one */
static inline struct obd_export *ll_s2mdexp(struct super_block *sb)
{
        return ll_s2sbi(sb)->ll_md_exp;
}

static inline struct client_obd *sbi2mdc(struct ll_sb_info *sbi)
{
        struct obd_device *obd = sbi->ll_md_exp->exp_obd;
        if (obd == NULL)
                LBUG();
        return &obd->u.cli;
}

// FIXME: replace the name of this with LL_SB to conform to kernel stuff
static inline struct ll_sb_info *ll_i2sbi(struct inode *inode)
{
        return ll_s2sbi(inode->i_sb);
}

static inline struct obd_export *ll_i2dtexp(struct inode *inode)
{
        return ll_s2dtexp(inode->i_sb);
}

static inline struct obd_export *ll_i2mdexp(struct inode *inode)
{
        return ll_s2mdexp(inode->i_sb);
}

static inline struct lu_fid *ll_inode2fid(struct inode *inode)
{
        struct lu_fid *fid;
        LASSERT(inode != NULL);
        fid = &ll_i2info(inode)->lli_fid;
        LASSERT(fid_is_igif(fid) || fid_ver(fid) == 0);
        return fid;
}

static inline int ll_mds_max_easize(struct super_block *sb)
{
        return sbi2mdc(ll_s2sbi(sb))->cl_max_mds_easize;
}

static inline __u64 ll_file_maxbytes(struct inode *inode)
{
        return ll_i2info(inode)->lli_maxbytes;
}

/* llite/xattr.c */
int ll_setxattr(struct dentry *dentry, const char *name,
                const void *value, size_t size, int flags);
ssize_t ll_getxattr(struct dentry *dentry, const char *name,
                    void *buffer, size_t size);
ssize_t ll_listxattr(struct dentry *dentry, char *buffer, size_t size);
int ll_removexattr(struct dentry *dentry, const char *name);

/* llite/remote_perm.c */
extern cfs_mem_cache_t *ll_remote_perm_cachep;
extern cfs_mem_cache_t *ll_rmtperm_hash_cachep;

cfs_hlist_head_t *alloc_rmtperm_hash(void);
void free_rmtperm_hash(cfs_hlist_head_t *hash);
int ll_update_remote_perm(struct inode *inode, struct mdt_remote_perm *perm);
int lustre_check_remote_perm(struct inode *inode, int mask);

/* llite/llite_capa.c */
extern cfs_timer_t ll_capa_timer;

int ll_capa_thread_start(void);
void ll_capa_thread_stop(void);
void ll_capa_timer_callback(unsigned long unused);

struct obd_capa *ll_add_capa(struct inode *inode, struct obd_capa *ocapa);
int ll_update_capa(struct obd_capa *ocapa, struct lustre_capa *capa);

void ll_capa_open(struct inode *inode);
void ll_capa_close(struct inode *inode);

struct obd_capa *ll_mdscapa_get(struct inode *inode);
struct obd_capa *ll_osscapa_get(struct inode *inode, __u64 opc);

void ll_truncate_free_capa(struct obd_capa *ocapa);
void ll_clear_inode_capas(struct inode *inode);
void ll_print_capa_stat(struct ll_sb_info *sbi);

/* llite/llite_cl.c */
extern struct lu_device_type vvp_device_type;

/**
 * Common IO arguments for various VFS I/O interfaces.
 */

int cl_sb_init(struct super_block *sb);
int cl_sb_fini(struct super_block *sb);
int cl_inode_init(struct inode *inode, struct lustre_md *md);
void cl_inode_fini(struct inode *inode);

enum cl_lock_mode  vvp_mode_from_vma(struct vm_area_struct *vma);
void ll_io_init(struct cl_io *io, const struct file *file, int write);

void ras_update(struct ll_sb_info *sbi, struct inode *inode,
                struct ll_readahead_state *ras, unsigned long index,
                unsigned hit);
void ll_ra_count_put(struct ll_sb_info *sbi, unsigned long len);
int ll_is_file_contended(struct file *file);
void ll_ra_stats_inc(struct address_space *mapping, enum ra_stat which);

/* llite/llite_rmtacl.c */
#ifdef CONFIG_FS_POSIX_ACL
obd_valid rce_ops2valid(int ops);
struct rmtacl_ctl_entry *rct_search(struct rmtacl_ctl_table *rct, pid_t key);
int rct_add(struct rmtacl_ctl_table *rct, pid_t key, int ops);
int rct_del(struct rmtacl_ctl_table *rct, pid_t key);
void rct_init(struct rmtacl_ctl_table *rct);
void rct_fini(struct rmtacl_ctl_table *rct);

void ee_free(struct eacl_entry *ee);
int ee_add(struct eacl_table *et, pid_t key, struct lu_fid *fid, int type,
           ext_acl_xattr_header *header);
struct eacl_entry *et_search_del(struct eacl_table *et, pid_t key,
                                 struct lu_fid *fid, int type);
void et_search_free(struct eacl_table *et, pid_t key);
void et_init(struct eacl_table *et);
void et_fini(struct eacl_table *et);
#endif

/* statahead.c */

#define LL_SA_RPC_MIN   2
#define LL_SA_RPC_DEF   32
#define LL_SA_RPC_MAX   8192

/* per inode struct, for dir only */
struct ll_statahead_info {
        struct inode           *sai_inode;
        unsigned int            sai_generation; /* generation for statahead */
        cfs_atomic_t            sai_refcount;   /* when access this struct, hold
                                                 * refcount */
        unsigned int            sai_sent;       /* stat requests sent count */
        unsigned int            sai_replied;    /* stat requests which received
                                                 * reply */
        unsigned int            sai_max;        /* max ahead of lookup */
        unsigned int            sai_index;      /* index of statahead entry */
        unsigned int            sai_index_next; /* index for the next statahead
                                                 * entry to be stated */
        unsigned int            sai_hit;        /* hit count */
        unsigned int            sai_miss;       /* miss count:
                                                 * for "ls -al" case, it includes
                                                 * hidden dentry miss;
                                                 * for "ls -l" case, it does not
                                                 * include hidden dentry miss.
                                                 * "sai_miss_hidden" is used for
                                                 * the later case.
                                                 */
        unsigned int            sai_consecutive_miss; /* consecutive miss */
        unsigned int            sai_miss_hidden;/* "ls -al", but first dentry
                                                 * is not a hidden one */
        unsigned int            sai_skip_hidden;/* skipped hidden dentry count */
        unsigned int            sai_ls_all:1;   /* "ls -al", do stat-ahead for
                                                 * hidden entries */
        cfs_waitq_t             sai_waitq;      /* stat-ahead wait queue */
        struct ptlrpc_thread    sai_thread;     /* stat-ahead thread */
        cfs_list_t              sai_entries_sent;     /* entries sent out */
        cfs_list_t              sai_entries_received; /* entries returned */
        cfs_list_t              sai_entries_stated;   /* entries stated */
        pid_t                   sai_pid;        /* pid of statahead itself */
};

int do_statahead_enter(struct inode *dir, struct dentry **dentry, int lookup);
void ll_statahead_exit(struct inode *dir, struct dentry *dentry, int result);
void ll_stop_statahead(struct inode *dir, void *key);

static inline
void ll_statahead_mark(struct inode *dir, struct dentry *dentry)
{
        struct ll_inode_info  *lli;
        struct ll_dentry_data *ldd = ll_d2d(dentry);

        /* dentry has been move to other directory, no need mark */
        if (unlikely(dir != dentry->d_parent->d_inode))
                return;

        lli = ll_i2info(dir);
        /* not the same process, don't mark */
        if (lli->lli_opendir_pid != cfs_curproc_pid())
                return;

        cfs_spin_lock(&lli->lli_sa_lock);
        if (likely(lli->lli_sai != NULL && ldd != NULL))
                ldd->lld_sa_generation = lli->lli_sai->sai_generation;
        cfs_spin_unlock(&lli->lli_sa_lock);
}

static inline
int ll_statahead_enter(struct inode *dir, struct dentry **dentryp, int lookup)
{
        struct ll_inode_info  *lli;
        struct ll_dentry_data *ldd = ll_d2d(*dentryp);

        if (unlikely(dir == NULL))
                return -EAGAIN;

        if (ll_i2sbi(dir)->ll_sa_max == 0)
                return -ENOTSUPP;

        lli = ll_i2info(dir);
        /* not the same process, don't statahead */
        if (lli->lli_opendir_pid != cfs_curproc_pid())
                return -EAGAIN;

        /*
         * When "ls" a dentry, the system trigger more than once "revalidate" or
         * "lookup", for "getattr", for "getxattr", and maybe for others.
         * Under patchless client mode, the operation intent is not accurate,
         * it maybe misguide the statahead thread. For example:
         * The "revalidate" call for "getattr" and "getxattr" of a dentry maybe
         * have the same operation intent -- "IT_GETATTR".
         * In fact, one dentry should has only one chance to interact with the
         * statahead thread, otherwise the statahead windows will be confused.
         * The solution is as following:
         * Assign "lld_sa_generation" with "sai_generation" when a dentry
         * "IT_GETATTR" for the first time, and the subsequent "IT_GETATTR"
         * will bypass interacting with statahead thread for checking:
         * "lld_sa_generation == lli_sai->sai_generation"
         */
        if (ldd && lli->lli_sai &&
            ldd->lld_sa_generation == lli->lli_sai->sai_generation)
                return -EAGAIN;

        return do_statahead_enter(dir, dentryp, lookup);
}

/* llite ioctl register support rountine */
#ifdef __KERNEL__
enum llioc_iter {
        LLIOC_CONT = 0,
        LLIOC_STOP
};

#define LLIOC_MAX_CMD           256

/*
 * Rules to write a callback function:
 *
 * Parameters:
 *  @magic: Dynamic ioctl call routine will feed this vaule with the pointer
 *      returned to ll_iocontrol_register.  Callback functions should use this
 *      data to check the potential collasion of ioctl cmd. If collasion is
 *      found, callback function should return LLIOC_CONT.
 *  @rcp: The result of ioctl command.
 *
 *  Return values:
 *      If @magic matches the pointer returned by ll_iocontrol_data, the
 *      callback should return LLIOC_STOP; return LLIOC_STOP otherwise.
 */
typedef enum llioc_iter (*llioc_callback_t)(struct inode *inode,
                struct file *file, unsigned int cmd, unsigned long arg,
                void *magic, int *rcp);

enum llioc_iter ll_iocontrol_call(struct inode *inode, struct file *file,
                unsigned int cmd, unsigned long arg, int *rcp);

/* export functions */
/* Register ioctl block dynamatically for a regular file.
 *
 * @cmd: the array of ioctl command set
 * @count: number of commands in the @cmd
 * @cb: callback function, it will be called if an ioctl command is found to
 *      belong to the command list @cmd.
 *
 * Return vaule:
 *      A magic pointer will be returned if success;
 *      otherwise, NULL will be returned.
 * */
void *ll_iocontrol_register(llioc_callback_t cb, int count, unsigned int *cmd);
void ll_iocontrol_unregister(void *magic);

#endif

/* lclient compat stuff */
#define cl_inode_info ll_inode_info
#define cl_i2info(info) ll_i2info(info)
#define cl_inode_mode(inode) ((inode)->i_mode)
#define cl_i2sbi ll_i2sbi

static inline void cl_isize_lock(struct inode *inode, int lsmlock)
{
        ll_inode_size_lock(inode, lsmlock);
}

static inline void cl_isize_unlock(struct inode *inode, int lsmlock)
{
        ll_inode_size_unlock(inode, lsmlock);
}

static inline void cl_isize_write_nolock(struct inode *inode, loff_t kms)
{
        LASSERT_SEM_LOCKED(&ll_i2info(inode)->lli_size_sem);
        i_size_write(inode, kms);
}

static inline void cl_isize_write(struct inode *inode, loff_t kms)
{
        ll_inode_size_lock(inode, 0);
        i_size_write(inode, kms);
        ll_inode_size_unlock(inode, 0);
}

#define cl_isize_read(inode)             i_size_read(inode)

static inline int cl_merge_lvb(struct inode *inode)
{
        return ll_merge_lvb(inode);
}

#define cl_inode_atime(inode) LTIME_S((inode)->i_atime)
#define cl_inode_ctime(inode) LTIME_S((inode)->i_ctime)
#define cl_inode_mtime(inode) LTIME_S((inode)->i_mtime)

struct obd_capa *cl_capa_lookup(struct inode *inode, enum cl_req_type crt);

/** direct write pages */
struct ll_dio_pages {
        /** page array to be written. we don't support
         * partial pages except the last one. */
        struct page **ldp_pages;
        /* offset of each page */
        loff_t       *ldp_offsets;
        /** if ldp_offsets is NULL, it means a sequential
         * pages to be written, then this is the file offset
         * of the * first page. */
        loff_t        ldp_start_offset;
        /** how many bytes are to be written. */
        size_t        ldp_size;
        /** # of pages in the array. */
        int           ldp_nr;
};

extern ssize_t ll_direct_rw_pages(const struct lu_env *env, struct cl_io *io,
                                  int rw, struct inode *inode,
                                  struct ll_dio_pages *pv);

static inline int ll_file_nolock(const struct file *file)
{
        struct ll_file_data *fd = LUSTRE_FPRIVATE(file);
        struct inode *inode = file->f_dentry->d_inode;

        LASSERT(fd != NULL);
        return ((fd->fd_flags & LL_FILE_IGNORE_LOCK) ||
                (ll_i2sbi(inode)->ll_flags & LL_SBI_NOLCK));
}
#endif /* LLITE_INTERNAL_H */
