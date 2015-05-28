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

/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *   Copyright(c) 2015 FUJITSU LIMITED.
 *   All rights reserved.
 */
/* written by Hiroya Nozaki <nozaki.hiroya@jp.fujitsu.com> */

#define DEBUG_SUBSYSTEM S_LLITE
#ifndef __KERNEL__
# error This is kernel only
#endif

#include <lustre_dlm.h>
#include <linux/file.h>
#include <linux/kthread.h>
#include <libcfs/list.h>
#include "llite_internal.h"
#include "iosvc.h"

static struct iosvc_globals {
	enum iosvc_state  ig_state;
	atomic_t	  ig_living_threads;
	atomic_t	  ig_working_threads;
	struct completion ig_terminated;
	int		  ig_refcount;
	spinlock_t	  ig_lock;
	struct mutex      ig_sem;
	struct iosvc_thread_info *ig_iti;
	int		  ig_iosvc_enable;
	int		  ig_iosvc_iovec_cache;
	struct proc_dir_entry *ig_proc_root;
	/* just to see if iosvc works expectedly */
	unsigned long     ig_workload;
	/* is this a kind of exaggerated, isn't it ?
	 * maybe I'd better implement stats things */
	spinlock_t        ig_workload_lock;
} iosvc_globals;

/* we don't assume that all the module parameters are changed
 * after loading lustre module */
uint iosvc_nthreads_max = 32;
CFS_MODULE_PARM(iosvc_nthreads_max, "i", int, 0444,
		"max num of iosvc threads");
uint iosvc_max_iovec_mb = 16;
CFS_MODULE_PARM(iosvc_max_iovec_mb, "i", uint, 0444,
		"max mb of iovec used iosvc");
int iosvc_setup = 1;
CFS_MODULE_PARM(iosvc_setup, "i", int, 0444, "enable iosvc function");

int iosvc_corebind = 1;
CFS_MODULE_PARM(iosvc_corebind, "i", int, 0444,
		"bind iosvc threads on a particular CPU\n");
uint iosvc_iovec_mb = 16;
CFS_MODULE_PARM(iosvc_iovec_mb, "i", int, 0444,
		"initial buffer size used iosvc");
uint iosvc_iovec_nrsegs = 1;
CFS_MODULE_PARM(iosvc_iovec_nrsegs, "i", int, 0444,
		"initial buffer's segments used iosvc");
/* 0 means getting this function disabled */
uint iosvc_pcache_ratio = 80;
CFS_MODULE_PARM(iosvc_pcache_ratio, "i", int, 0444,
		"Percentage of lru slots available for iosvc");

int iosvc_is_setup(void)
{
	return !!iosvc_setup;
}

static inline int iosvc_is_enable(void)
{
	return !!iosvc_globals.ig_iosvc_enable;
}

static inline int iosvc_iovec_cache_is_enable(void)
{
	return !!iosvc_globals.ig_iosvc_iovec_cache;
}

static inline void iosvc_lock(void)
{
	spin_lock(&iosvc_globals.ig_lock);
}

static inline void iosvc_unlock(void)
{
	spin_unlock(&iosvc_globals.ig_lock);
}

static inline int iosvc_get_living_threads(void)
{
	return atomic_read(&iosvc_globals.ig_living_threads);
}

static inline int iosvc_state_is(enum iosvc_state state)
{
	return (state == iosvc_globals.ig_state);
}

static inline int iosvc_get_working_threads(void)
{
	return atomic_read(&iosvc_globals.ig_working_threads);
}

static inline void iosvc_inc_working_threads(void)
{
	atomic_inc(&iosvc_globals.ig_working_threads);
}

static inline void iosvc_dec_working_threads(void)
{
	atomic_dec(&iosvc_globals.ig_working_threads);
}

static inline unsigned long iosvc_get_workload(void)
{
	return iosvc_globals.ig_workload;
}

static inline unsigned long iosvc_inc_workload(void)
{
	unsigned long workload;

	spin_lock(&iosvc_globals.ig_workload_lock);
	workload = ++iosvc_globals.ig_workload;
	spin_unlock(&iosvc_globals.ig_workload_lock);

	return workload;
}

/*
 * return 0: fail to reserve pagae caches
 * return 1: succeed to reserve page cahces
 */
static int iosvc_reserve_pcache(struct iosvc_item *item, struct inode *inode)
{
	int rc;
	unsigned long pages;
	loff_t pos = item->ii_rw_common.crw_pos;
	size_t count = item->ii_rw_common.crw_count;
	struct ll_sb_info *sbi = ll_i2sbi(inode);
	struct cl_client_cache *cache = sbi->ll_cache;

	if (!iosvc_pcache_ratio) {
		/* don't care ccc_iosvc_pages this time */
		item->ii_rw_common.crw_pos = 0;
		item->ii_rw_common.crw_count = 0;
		return 1;
	}
	pages = ((pos + count + PAGE_CACHE_SIZE - 1) >> PAGE_CACHE_SHIFT) -
		 (pos >> PAGE_CACHE_SHIFT);

	spin_lock(&cache->ccc_iosvc_lock);
	if (cache->ccc_iosvc_pages + pages <
	    (cache->ccc_lru_max * iosvc_pcache_ratio) / 100) {
		cache->ccc_iosvc_pages += pages;
		rc = 1;
	} else {
		rc = 0;
	}
	spin_unlock(&cache->ccc_iosvc_lock);

	return rc;
}

static void iosvc_cancel_pcache(struct iosvc_item *item, struct inode *inode)
{
	unsigned long pages;
	struct ll_sb_info *sbi = ll_i2sbi(inode);
	struct cl_client_cache *cache = sbi->ll_cache;
	loff_t pos = item->ii_rw_common.crw_pos;
	size_t count = item->ii_rw_common.crw_count;

	pages = ((pos + count + PAGE_CACHE_SIZE - 1) >> PAGE_CACHE_SHIFT) -
		 (pos >> PAGE_CACHE_SHIFT);
	if (!pages)
		return; /* don't wanna get a lock */

	spin_lock(&cache->ccc_iosvc_lock);
	cache->ccc_iosvc_pages -= pages;
	spin_unlock(&cache->ccc_iosvc_lock);
	return;
}

static void iosvc_io_error(struct iosvc_item *item, int rc)
{
	struct ll_inode_info *lli = ll_i2info(item->ii_file->f_dentry->d_inode);
	iosvc_set_rc(lli, rc);
	CERROR("iosvc failed: %d\n", rc);
	return;
}

/******************* procfs *************************/
static int iosvc_enable_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%s\n",
			    iosvc_is_enable() ? "enable" : "disable");
	return rc;
}

static ssize_t iosvc_enable_seq_write(struct file *file,
				      const char __user *buffer,
				      size_t count, loff_t *off)
{
	int val, rc = 0;
	struct seq_file *seq = file->private_data;
	struct iosvc_globals *globals = seq->private;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		goto out;

	globals->ig_iosvc_enable = val;
out:
	return rc ? rc : count;
}
LPROC_SEQ_FOPS(iosvc_enable);

static int iosvc_setup_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%savailable\n",
			    iosvc_is_setup() ? "" : "un");
	return rc;
}
LPROC_SEQ_FOPS_RO(iosvc_setup);

static int iosvc_working_threads_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%d\n", iosvc_get_working_threads());
	return rc;
}

LPROC_SEQ_FOPS_RO(iosvc_working_threads);

static int iosvc_living_threads_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%d\n", iosvc_get_living_threads());
	return rc;
}
LPROC_SEQ_FOPS_RO(iosvc_living_threads);

static int iosvc_threads_max_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%d\n", iosvc_nthreads_max);
	return rc;
}
LPROC_SEQ_FOPS_RO(iosvc_threads_max);

static int iosvc_max_iovec_mb_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%u\n", iosvc_max_iovec_mb);
	return rc;
}
LPROC_SEQ_FOPS_RO(iosvc_max_iovec_mb);

static int iosvc_iovec_cache_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%s\n",
			iosvc_iovec_cache_is_enable() ? "enable" : "disable");
	return rc;
}

static ssize_t iosvc_iovec_cache_seq_write(struct file *file,
					   const char __user *buffer,
					   size_t count, loff_t *off)
{
	int val, rc = 0;
	struct seq_file *seq = file->private_data;
	struct iosvc_globals *globals = seq->private;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		goto out;

	globals->ig_iosvc_iovec_cache = val;
out:
	return rc ? rc : count;
}
LPROC_SEQ_FOPS(iosvc_iovec_cache);

static int iosvc_workload_seq_show(struct seq_file *seq, void *data)
{
	int rc = seq_printf(seq, "%lu\n", iosvc_get_workload());
	return rc;
}
LPROC_SEQ_FOPS_RO(iosvc_workload);

struct lprocfs_vars seq_iosvc_proc_list[] = {
	{ .name = "iosvc_enable",
	  .fops = &iosvc_enable_fops },
	{ .name = "iosvc_setup",
	  .fops = &iosvc_setup_fops },
	{ .name = "working_threads",
	  .fops = &iosvc_working_threads_fops },
	{ .name = "living_threads",
	  .fops = &iosvc_living_threads_fops },
	{ .name = "threads_max",
	  .fops = &iosvc_threads_max_fops },
	{ .name = "max_iovec_mb",
	  .fops = &iosvc_max_iovec_mb_fops },
	{ .name = "iovec_cache",
	  .fops = &iosvc_iovec_cache_fops },
	{ .name = "workload",
	  .fops = &iosvc_workload_fops },
	{ 0 }
};

/* item == NULL : alloc a new item
 * item != null : inc refcount */
struct iosvc_item *iosvc_get_item(struct iosvc_item *item)
{
	struct iosvc_item *ret = NULL;
	if (item) {
		LASSERT(atomic_read(&item->ii_refcount) > 0);
		atomic_inc(&item->ii_refcount);
		ret = item;
	} else {
		OBD_ALLOC_PTR(ret);
		if (!ret)
			goto out;
		memset(ret, 0, sizeof(struct iosvc_item));
		atomic_set(&ret->ii_refcount, 1);
	}
out:
	return ret;
}

struct iosvc_item *iosvc_put_item(struct iosvc_item *item)
{
	struct iosvc_item *ret = item;
	if (atomic_dec_and_test(&item->ii_refcount)) {
		LASSERT(atomic_read(&item->ii_refcount) == 0);
		if (item->ii_rlock)
			OBD_FREE_PTR(item->ii_rlock);
		if (item->ii_file)
			fput_light(item->ii_file, 1);
		OBD_FREE_PTR(item);
		ret = NULL;
	}
	return ret;
}

int iosvc_init_iovec(struct iosvc_thread_info *iti)
{
	int rc = 0, nrsegs = iosvc_iovec_nrsegs, seg;
	__kernel_size_t len = (iosvc_iovec_mb << 20);
	struct iovec *iov = NULL;
	ENTRY;

	OBD_ALLOC_LARGE(iov, sizeof(struct iovec) * nrsegs);
	if (!iov)
		GOTO(out, rc = -ENOMEM);
	memset(iov, 0, sizeof(struct iovec) * nrsegs);
	for (seg = 0; seg < nrsegs; seg++) {
		struct iovec *dest = iov + seg;
		OBD_ALLOC_LARGE(dest->iov_base, len);
		if (!dest->iov_base)
			GOTO(out, rc = -ENOMEM);
		dest->iov_len = len;
	}
	rc = iosvc_manage_iovec(iti->iti_env, iov, NULL, nrsegs, len);
out:
	if (rc < 0 && iov) {
		iosvc_free_iovec(iov, nrsegs, len, 0);
		iov = NULL;
	}

	RETURN(rc);
}


int iosvc_init_thread(struct iosvc_thread_info *iti)
{
	int rc = 0;
	ENTRY;

	atomic_inc(&iosvc_globals.ig_living_threads);
	iosvc_inc_working_threads();

	iti->iti_item = NULL;
	init_waitqueue_head(&iti->iti_waitq);
	rwlock_init(&iti->iti_rwlock);
	iti->iti_refcount = 0;
	iti->iti_pid = current->pid;
	iti->iti_cookie = current;

	iosvc_lock();
	if (iosvc_state_is(IOSVC_TERMINATE)) {
		iosvc_unlock();
		GOTO(out, rc = -EALREADY);
	}
	iosvc_unlock();

	iti->iti_env = cl_env_get(&iti->iti_refcount);
	if (IS_ERR(iti->iti_env)) {
		rc = PTR_ERR(iti->iti_env);
		iti->iti_env = NULL;
		GOTO(out, rc);
	}

	rc = iosvc_init_iovec(iti);
	if (rc) {
		CERROR("Cannot allocate iosvc's initial buffer: %d\n", rc);
		/* keep going and iovec is going to be allocated
		 * when it's demanded */
		rc = 0;
	}
	iti->iti_avail = ITI_AVAIL;
	complete(&iti->iti_completion);
out:
	RETURN(rc);
}

void iosvc_fini_thread(struct iosvc_thread_info *iti)
{
	ENTRY;

	iti->iti_avail = ITI_STOPPING;
	if (iti->iti_env) {
		cl_env_put(iti->iti_env, &iti->iti_refcount);
		iti->iti_env = NULL;
	}

	if (iti->iti_iovec) {
		iosvc_free_iovec(iti->iti_iovec_frame, iti->iti_nrsegs,
				 iti->iti_max_len, 1);
		iti->iti_iovec_frame = NULL;
		OBD_FREE_LARGE(iti->iti_iovec,
			       sizeof(struct iovec) * iti->iti_nrsegs);
		iti->iti_iovec = NULL;
		iti->iti_nrsegs = 0;
		iti->iti_max_len = 0;
	}

	iosvc_dec_working_threads();
	if (atomic_dec_and_test(&iosvc_globals.ig_living_threads)) {
		complete(&iosvc_globals.ig_terminated);
		CDEBUG(D_VFSTRACE,
		       "Now the last iosvc thread has been dead !\n");
	}
	complete(&iti->iti_completion);

	EXIT;
}

static struct iosvc_thread_info *iosvc_find_iti(const struct lu_env *env)
{
	int i;
	struct iosvc_thread_info *iti = NULL;
	for (i = 0; i < iosvc_nthreads_max; i++) {
		iti = iosvc_globals.ig_iti + i;
		if (iti && (iti->iti_env == env))
			break;
		iti = NULL;
	}
	return iti;
}

struct lu_env *iosvc_reserve(void)
{
	int i;
	struct lu_env *ret = NULL;
	ENTRY;

	for (i = 0; i < iosvc_nthreads_max; i++) {
		struct iosvc_thread_info *iti;
		iti = iosvc_globals.ig_iti + i;
		write_lock(&iti->iti_rwlock);
		if (iti->iti_avail == ITI_AVAIL) {
			iosvc_lock();
			if (!iosvc_state_is(IOSVC_READY)) {
				iosvc_unlock();
				write_unlock(&iti->iti_rwlock);
				break;
			}
			iti->iti_avail = ITI_UNAVAIL;
			iosvc_unlock();
			write_unlock(&iti->iti_rwlock);
			GOTO(out, ret = iti->iti_env);
		}
		write_unlock(&iti->iti_rwlock);
	}

out:
	RETURN(ret);
}

void iosvc_cancel(const struct lu_env *env)
{
	struct iosvc_thread_info *iti;

	if (!env)
		return;

	iti = iosvc_find_iti(env);
	if (iti) {
		write_lock(&iti->iti_rwlock);
		iti->iti_avail = ITI_AVAIL;
		write_unlock(&iti->iti_rwlock);
	} else {
		CERROR("Failed to cancel iosvc thread ... "
		       "something weird happens - env: %p\n", env);
		LBUG();
	}
	return;
}

void iosvc_cancel_thread(struct iosvc_thread_info *iti)
{
	write_lock(&iti->iti_rwlock);
	iti->iti_avail = ITI_AVAIL;
	write_unlock(&iti->iti_rwlock);
}


static int iosvc_setup_procfs_entries(void)
{
	int rc = 0;

	if (!iosvc_globals.ig_proc_root) {
		iosvc_globals.ig_proc_root =
			lprocfs_register("iosvc", proc_lustre_root,
					 seq_iosvc_proc_list,
					 &iosvc_globals);
		if (IS_ERR(iosvc_globals.ig_proc_root)) {
			rc = PTR_ERR(iosvc_globals.ig_proc_root);
			iosvc_globals.ig_proc_root = NULL;
			goto out;
		}
	}
out:
	return rc;
}

static void iosvc_cleanup_procfs_entries(void)
{
	ENTRY;
	if (iosvc_globals.ig_proc_root) {
		lprocfs_remove(&iosvc_globals.ig_proc_root);
		iosvc_globals.ig_proc_root = NULL;
	}
	EXIT;
}

int iosvc_setup_service(void)
{
	int rc = 0;
	if (iosvc_nthreads_max < IOSVC_THREAD_MIN ||
	    iosvc_nthreads_max > IOSVC_THREAD_MAX) {
		CERROR("invalid module parm - iosvc_nthreads_max: %d\n",
		       iosvc_nthreads_max);
		return -EINVAL;
	}

	if (iosvc_max_iovec_mb < 0) {
		CERROR("invalid module parm - iosvc_max_iovec_mb: %u\n",
		       iosvc_max_iovec_mb);
		return -EINVAL;
	}

	if (iosvc_iovec_nrsegs < 0) {
		CERROR("invalid module parm - iosvc_iovec_nrsegs: %d\n",
		       iosvc_iovec_nrsegs);
		return -EINVAL;
	}

	if (iosvc_iovec_mb * iosvc_iovec_nrsegs > iosvc_max_iovec_mb) {
		CERROR("invalid module parm - "
		       "(iosvc_iovec_mb:%d * iosvc_iovec_nrsegs:%d) exceeds "
		       "iosvc_max_iovec_mb:%u\n",
		       iosvc_iovec_mb, iosvc_iovec_nrsegs, iosvc_max_iovec_mb);
		return -EINVAL;
	}

	if (iosvc_pcache_ratio < 0 || iosvc_pcache_ratio > 100) {
		CERROR("invalid module parm - "
		       "iosvc_pcache_ratio should be [0, 100]\n");
		return -EINVAL;
	}

	/* init iosvc globals */
	if (iosvc_is_setup()) {
		iosvc_globals.ig_state = IOSVC_NOT_READY;
		atomic_set(&iosvc_globals.ig_living_threads, 0);
		atomic_set(&iosvc_globals.ig_working_threads, 0);
		init_completion(&iosvc_globals.ig_terminated);
		iosvc_globals.ig_refcount = 0;
		spin_lock_init(&iosvc_globals.ig_lock);
		mutex_init(&iosvc_globals.ig_sem);
		iosvc_globals.ig_iosvc_iovec_cache = 1;

		iosvc_globals.ig_workload = 0;
		spin_lock_init(&iosvc_globals.ig_workload_lock);

		rc = iosvc_setup_procfs_entries();
		if (rc) {
			CERROR("fail to setup procfs entries: %d\n", rc);
			/* keep going */
			rc = 0;
		}
	}
	return rc;
}

void iosvc_cleanup_service(void)
{
	if (iosvc_globals.ig_iti) {
		size_t size;
		size = sizeof(struct iosvc_thread_info) * iosvc_nthreads_max;
		OBD_FREE(iosvc_globals.ig_iti, size);
		iosvc_globals.ig_iti = NULL;
	}
	if (iosvc_is_setup())
		iosvc_cleanup_procfs_entries();
	return;
}

static inline int iosvc_wait_io(struct iosvc_thread_info *iti)
{
	int rc;
	struct l_wait_info lwi = { 0 };

	iosvc_dec_working_threads();
	rc = l_wait_event(iti->iti_waitq,
			  iosvc_state_is(IOSVC_TERMINATE) ||
			  iti->iti_item, &lwi);
	iosvc_inc_working_threads();

	return rc;
}

int iosvc_io_start(struct iosvc_item *item, const struct lu_env *env)
{
	int result = 0;
	struct file *file;
	struct cl_io *io;
	ENTRY;

	file = item->ii_file;
	io = &vvp_env_info(env)->vti_io;

	CDEBUG(D_VFSTRACE, "file: %p, env: %p, io: %p\n",
	       file, env, io);

	/* wait for the owner of this io leaving syscall context */
	while (io->ci_iosvc_syscall_inprogress)
		cond_resched();

	iosvc_inc_workload();
	/* return 0 in success */
	result = cl_io_single(env, io);
	ll_cl_remove(file, env);

	RETURN(result);
}

void iosvc_io_fini(struct iosvc_item *item, const struct lu_env *env)
{
	struct cl_io *io;
	ENTRY;

	io = &vvp_env_info(env)->vti_io;
	cl_io_fini(env, io);

	EXIT;
}

/*
 * returns x ...
 *  x ==  0 : nothing to do
 *  x >=  1 : short write occurred
 *  x == -1 : restart the same I/O again
 */
int iosvc_detect_swrite(struct iosvc_item *item, const struct lu_env *env,
			int result)
{
	struct cl_io *io = &vvp_env_info(env)->vti_io;
	size_t count = io->u.ci_wr.wr.crw_count,
	       nob = io->ci_nob;
	ENTRY;

	if (count > 0) {
		CERROR("write from pid:(%u) on: %s, written: %zd, "
		       "SHORT-WRITE DETCTED!! check, pick up an error and "
		       "retry the same I/O or you may lose some data !\n",
		       item->ii_syscall_pid,
		       item->ii_file->f_dentry->d_name.name, count);
		iosvc_io_error(item, -EIO);
	}

	if (nob > 0)
		result = nob;

	/* iosvc cannot handle a restart case so far ... */
	if ((result == 0 || result == -ENODATA) && io->ci_need_restart) {
		CERROR("unexpected iosvc status ... "
		       "file-layout's been changed or something: %d\n", result);
		LBUG();
	}
	RETURN(result);
}

void iosvc_io_unlock(struct iosvc_item *item, const struct lu_env *env)
{
	struct ll_inode_info *lli = ll_i2info(item->ii_file->f_dentry->d_inode);
	ENTRY;

	/* this locks were held in ll_file_io_generic */
	up_read(&lli->lli_trunc_sem);
	CDEBUG(D_VFSTRACE, "Range unlock "RL_FMT"\n", RL_PARA(item->ii_rlock));
	range_unlock(&lli->lli_write_tree, item->ii_rlock);

	EXIT;
}

void iosvc_io_complete(struct iosvc_item *item, const struct lu_env *env)
{
	struct ll_inode_info *lli = ll_i2info(item->ii_file->f_dentry->d_inode);
	ENTRY;

	spin_lock(&lli->lli_iosvc_lock);
	list_del_init(&item->ii_inode_list);
	lli->lli_iosvc_item_count--;
	spin_unlock(&lli->lli_iosvc_lock);

	complete_all(&item->ii_io_finished);
	iosvc_cancel_pcache(item, item->ii_file->f_dentry->d_inode);

	iosvc_put_item(item);

	EXIT;
}

static struct iosvc_item *iosvc_retrieve_item(struct iosvc_thread_info *iti)
{
	struct iosvc_item *item = iti->iti_item;
	iti->iti_item = NULL;
	return item;
}

/* caller should hold iosvc_lock */
int iosvc_io(struct iosvc_thread_info *iti)
{
	int rc = 0;
	const struct lu_env *env = iti->iti_env;
	ENTRY;

	OBD_FAIL_TIMEOUT(OBD_FAIL_IOSVC_PAUSE1, 1);

	LASSERT(env);
	if (iti->iti_item) {
		int result;
		struct iosvc_item *item = iosvc_retrieve_item(iti);
		CDEBUG(D_VFSTRACE, "start handling item: %p\n", item);
		/* 0 in success */
		OBD_FAIL_TIMEOUT(OBD_FAIL_IOSVC_PAUSE2, 1);

		result = iosvc_io_start(item, env);
		if (result)
			iosvc_io_error(item, result);
		iosvc_io_fini(item, env);
		OBD_FAIL_TIMEOUT(OBD_FAIL_IOSVC_PAUSE3, 1);

		iosvc_detect_swrite(item, env, result);
		/* unlock range lock and trunc sem */
		iosvc_io_unlock(item, env);

		OBD_FAIL_TIMEOUT(OBD_FAIL_IOSVC_PAUSE4, 1);
		CDEBUG(D_VFSTRACE, "stop handling item: %p\n", item);
		iosvc_io_complete(item, env);
	}
	RETURN(rc);
}

void iosvc_reset_iovec(struct iosvc_thread_info *iti)
{
	unsigned long  nr_segs = iti->iti_nrsegs;
	if (iti->iti_iovec) {
		LASSERT(iti->iti_iovec_frame);
		memcpy(iti->iti_iovec, iti->iti_iovec_frame,
		       sizeof(struct iovec) * nr_segs);
	}
}

/* return 0: stop
 * return 1: continue */
static int iosvc_continue(struct iosvc_thread_info *iti)
{
	int rc = 1;
	write_lock(&iti->iti_rwlock);
	if (iosvc_state_is(IOSVC_TERMINATE) && !iti->iti_item &&
	    (iti->iti_avail == ITI_AVAIL))
		rc = 0;
	write_unlock(&iti->iti_rwlock);

	return rc;
}

static int iosvc_main(void *arg)
{
	int rc = 0;
	struct iosvc_thread_info *iti = arg;
	ENTRY;

	iti->iti_rc = iosvc_init_thread(iti);
	if (iti->iti_rc < 0)
		GOTO(no_wait, rc = 0);

	while (1) {
		iosvc_wait_io(iti);
		/* iosvc_io retruns 0 only, don't need to care */
		rc = iosvc_io(iti);
		iosvc_reset_iovec(iti);
		iosvc_cancel_thread(iti);

		rc = iosvc_continue(iti);
		if (unlikely(!rc))
			GOTO(no_wait, rc = 0);
	}
no_wait:
	iosvc_fini_thread(iti);
	CDEBUG(D_VFSTRACE, "Exiting I/O-service Thread\n");
	RETURN(rc);
}

void iosvc_stop_threads(int wait)
{
	ENTRY;
	iosvc_lock();
	iosvc_globals.ig_state = IOSVC_TERMINATE;
	iosvc_unlock();

	/* When there's no iosvc thread, we never receives ig_terminated
	 * signal because the signal is sent by iosvc thread ... so "wait"
	 * means there're some threads we have to wait here */
	if (wait) {
		int i;
		for (i = 0; i < iosvc_nthreads_max; i++) {
			struct iosvc_thread_info *iti;
			iti = iosvc_globals.ig_iti + i;
			wake_up(&iti->iti_waitq);
		}
		wait_for_completion(&iosvc_globals.ig_terminated);
	}
	iosvc_globals.ig_iosvc_enable = 0;
	iosvc_globals.ig_state = IOSVC_NOT_READY;
	EXIT;
}

/* super simple, foolishly honest way to wait for all I/Os finishing */
static void iosvc_simple_flush(int force)
{
	int i;

	if (!iosvc_globals.ig_iti)
		return;

	for (i = 0; i < iosvc_nthreads_max; i++) {
		struct iosvc_thread_info *iti = iosvc_globals.ig_iti + i;
		while (iti->iti_avail == ITI_UNAVAIL) {
			if (force) {
				CWARN("didn't wait the io handled by "
				      "iti: %p, iti->iti_pid: %d\n",
				      iti, iti->iti_pid);
				break;
			}
			cond_resched();
		}
	}
	return;
}

void iosvc_stop_service(int force)
{
	ENTRY;

	if (iosvc_is_setup()) {
		/* flush out all I/Os in iosvc before actual umount operation */
		iosvc_simple_flush(force);
		mutex_lock(&iosvc_globals.ig_sem);
		/* we shouldn't terminate iosvc threads until the last client
		 * on the same node stops */
		if ((--iosvc_globals.ig_refcount) == 0)
			iosvc_stop_threads(iosvc_get_living_threads());
		else if (iosvc_globals.ig_refcount < 0)
			iosvc_globals.ig_refcount = 0;
		mutex_unlock(&iosvc_globals.ig_sem);
	}
	EXIT;
}

int iosvc_start_service(void)
{
	int i, rc = 0;
	ENTRY;

	if (!iosvc_is_setup())
		RETURN(rc);

	mutex_lock(&iosvc_globals.ig_sem);
	if ((++iosvc_globals.ig_refcount) == 1) {
		unsigned short cpu = 0;
		size_t size;
		struct task_struct *task;
		LASSERT(iosvc_state_is(IOSVC_NOT_READY));
		LASSERT(!iosvc_get_living_threads());

		size = sizeof(struct iosvc_thread_info) * iosvc_nthreads_max;
		if (!size)
			GOTO(out, rc = 0);

		if (iosvc_globals.ig_iti) {
			memset(iosvc_globals.ig_iti, 0, size);
		} else {
			OBD_ALLOC(iosvc_globals.ig_iti, size);
			if (!iosvc_globals.ig_iti) {
				CERROR("fails to allocate buffer\n");
				GOTO(out, rc = -ENOMEM);
			}
			memset(iosvc_globals.ig_iti, 0, size);
		}

		for (i = 0; i < iosvc_nthreads_max; i++) {
			struct iosvc_thread_info *iti;
			iti = iosvc_globals.ig_iti + i;
			init_completion(&iti->iti_completion);
			task = kthread_create(iosvc_main, iti,
					      "ll_iosvc_%03d", i);
			if (IS_ERR(task)) {
				rc = PTR_ERR(task);
				CERROR("thread failed to start: %d\n", rc);
				GOTO(out, rc);
			}
			if (!!iosvc_corebind)
				kthread_bind(task, cpu);
			wake_up_process(task);

			/* FUTURE WORK: Adds CPT support */
			iti->iti_cpu = cpu++;
			if (cpu >= num_online_cpus())
				cpu = 0;
		}

		/* see if some threads failed to run */
		for (i = 0; i < iosvc_nthreads_max; i++) {
			struct iosvc_thread_info *iti;
			iti = iosvc_globals.ig_iti + i;
			wait_for_completion(&iti->iti_completion);
			if (iti->iti_rc)
				CERROR("thread failed to start: %d\n",
					iti->iti_rc);
		}
		if (atomic_read(&iosvc_globals.ig_living_threads) > 0) {
			iosvc_globals.ig_state = IOSVC_READY;
			iosvc_globals.ig_iosvc_enable = 1;
		}
	} else {
		CDEBUG(D_VFSTRACE, "iosvc thread's already started\n");
	}
	rc = 0;
out:
	mutex_unlock(&iosvc_globals.ig_sem);
	RETURN(rc);
}

int iosvc_do_enqueue(struct file *file, const struct lu_env *env,
		     struct cl_io *io, struct iosvc_item *item)
{
	int rc = 0;
	struct iosvc_thread_info *iti;
	struct inode *inode = item->ii_file->f_dentry->d_inode;
	struct ll_inode_info *lli = ll_i2info(inode);
	struct vvp_io *vio = vvp_env_io(env);
	ENTRY;

	iti = iosvc_find_iti(env);
	if (unlikely(!iti)) {
		CERROR("Failed to get iosvc_thread_info\n");
		GOTO(out, rc = -EFAULT);
	}

	if (!iosvc_reserve_pcache(item, inode)) {
		io->ci_need_restart = 1;
		CDEBUG(D_VFSTRACE, "used too many page caches already\n");
		GOTO(out, rc = -EAGAIN);
	}

	ll_cl_add_illegal(file, env, io, iti->iti_cookie);
	io->ci_continue = 0;

	rc = cl_io_iter_init(env, io);
	if (rc == -EDQUOT) {
		io->ci_need_restart = 1;
		GOTO(err, rc);
	} else if (rc) {
		GOTO(err, rc);
	} else if (io->ci_continue) {
		io->ci_need_restart = 1;
		GOTO(err, rc = -EAGAIN);
	}

	rc = iosvc_copy_iovec(iti);
	if (rc)
		GOTO(err, rc);

	rc = cl_io_lock(env, io);
	if (rc)
		GOTO(err, rc);

	if (ll_layout_version_get(lli) != vio->vui_layout_gen) {
		CDEBUG(D_VFSTRACE, "Layout change detected - lli:%p\n", lli);
		io->ci_need_restart = 1;
		GOTO(unlock, rc = -EAGAIN);
	}

	iosvc_lock();
	if (iosvc_state_is(IOSVC_READY)) {
		LASSERT(!iti->iti_item);
		iti->iti_item = item;

		spin_lock(&lli->lli_iosvc_lock);
		list_add_tail(&item->ii_inode_list, &lli->lli_iosvc_item_head);
		lli->lli_iosvc_item_count++;
		spin_unlock(&lli->lli_iosvc_lock);
		iosvc_unlock();

		wake_up(&iti->iti_waitq);
	} else {
		iosvc_unlock();

		io->ci_need_restart = 1;
		GOTO(unlock, rc = -EALREADY);
	}
out:
	RETURN(rc);

unlock:
	cl_io_unlock(env, io);
err:
	iosvc_cancel_pcache(item, inode);
	cl_io_iter_fini(env, io);
	ll_cl_remove(file, env);
	RETURN(rc);
};

int iosvc_enqueue(struct file *file, const struct lu_env *env,
		  struct cl_io *io, struct range_lock *range)
{
	int rc = 0;
	struct iosvc_item *item = NULL;
	ENTRY;

	if (unlikely(!file))
		GOTO(out, rc = -EINVAL);

	item = iosvc_get_item(NULL);
	if (!item)
		GOTO(out, rc = -ENOMEM);

	/* kinda dirty hack */
	rcu_read_lock();
	LASSERT(file_count(file) >= 1);
	get_file(file);
	rcu_read_unlock();

	item->ii_file = file;
	item->ii_rlock = range;
	INIT_LIST_HEAD(&item->ii_inode_list);
	init_completion(&item->ii_io_finished);
	item->ii_syscall_pid = current->pid;
	memcpy(&item->ii_rw_common, &io->u.ci_rw,
	       sizeof(struct cl_io_rw_common));

	rc = iosvc_do_enqueue(file, env, io, item);
out:
	if (rc < 0) {
		if (item) {
			fput_light(item->ii_file, 1);
			OBD_FREE_PTR(item);
		}
	}
	RETURN(rc);
}

/*
 * return 0: don't allow to use iosvc
 * return 1: allow to use iosvc
 */
int iosvc_check_avail(struct cl_io *io, enum cl_io_type iot)
{
	int rc = 1;
	ENTRY;
	/* iosvc don't care I/O besides WRITE */
	if (iot != CIT_WRITE)
		GOTO(out, rc = 0);
	/* iosvc don't care O_SYNC or O_DIRECT */
	if (io->u.ci_wr.wr_sync)
		GOTO(out, rc = 0);
out:
	RETURN(rc);
}

/*
 * return 0: don't allow to use iosvc
 * return 1: allow to use iosvc
 */
int iosvc_precheck_avail(struct file *file, struct vvp_io_args *args,
			 enum cl_io_type iot, size_t count)
{
	int rc = 1;
	struct ll_file_data *lfd = LUSTRE_FPRIVATE(file);
	struct inode *inode = file->f_dentry->d_inode;
	struct ll_inode_info *lli = ll_i2info(inode);

	ENTRY;
	if (!iosvc_is_enable())
		GOTO(out, rc = 0);
	else if (unlikely(iot != CIT_WRITE))
		GOTO(out, rc = 0);
	else if (file->f_flags & (O_SYNC | O_DIRECT | O_APPEND))
		GOTO(out, rc = 0);
	else if (is_sync_kiocb(args->u.normal.via_iocb))
		GOTO(out, rc = 0);
	else if (lfd->fd_flags & LL_FILE_GROUP_LOCKED)
		GOTO(out, rc = 0);
	else if (ll_file_nolock(file))
		GOTO(out, rc = 0);
	else if (inode_newsize_ok(inode,
				  args->u.normal.via_iocb->ki_pos + count))
		GOTO(out, rc = 0);
	else if (!iosvc_state_is(IOSVC_READY))
		GOTO(out, rc = 0);
	/* To detect an error, especially ENOSPC, with IOSVC asap */
	else if (iosvc_get_rc(lli))
		GOTO(out, rc = 0);
out:
	RETURN(rc);
}

static __kernel_size_t iosvc_get_maxlen(struct iovec *iov,
					unsigned long nr_segs)
{
	unsigned long seg;
	__kernel_size_t max_len = 0;

	for (seg = 0; seg < nr_segs; seg++) {
		struct iovec *target = iov + seg;
		if (target->iov_len > max_len)
			max_len = target->iov_len;
	}
	return max_len;
}

void iosvc_free_iovec(struct iovec *iov, unsigned long nr_segs,
		      __kernel_size_t max_len, int clear)
{
	unsigned long seg;
	ENTRY;
	for (seg = 0; seg < nr_segs; seg++) {
		struct iovec *dest = iov + seg;
		if (dest->iov_base) {
			/* max_len shouldn't be zero only in this case */
			LASSERT(max_len > 0);
			if (!!clear)
				memset(dest->iov_base, 0, max_len);
			OBD_FREE_LARGE(dest->iov_base, max_len);
			dest->iov_base = NULL;
		}
	}
	OBD_FREE_LARGE(iov, sizeof(struct iovec) * nr_segs);
	EXIT;
}

int iosvc_manage_iovec(const struct lu_env *env,
		       struct iovec *iov, struct iovec *src,
		       unsigned long nr_segs, __kernel_size_t max_len)
{
	int rc = 0;
	struct iosvc_thread_info *iti;
	struct iovec *frame = NULL;
	ENTRY;

	iti = iosvc_find_iti(env);
	if (!iti) {
		CWARN("iosvc_thread_info not found: env=%p\n", env);
		GOTO(out, rc = -EFAULT);
	}
	iti->iti_iovec_src = src;

	if (!iov)
		GOTO(out, rc);
	iti->iti_iovec = iov;

	OBD_ALLOC_LARGE(frame, sizeof(struct iovec) * nr_segs);
	if (!frame) {
		iti->iti_iovec = NULL;
		iti->iti_iovec_src = NULL;
		GOTO(out, rc = -ENOMEM);
	}
	iti->iti_iovec_frame = frame;
	memcpy(frame, iov, sizeof(struct iovec) * nr_segs);
	iti->iti_nrsegs = nr_segs;
	iti->iti_max_len = max_len;
out:
	RETURN(rc);
}

struct iovec *iosvc_check_and_get_iovec(const struct lu_env *env,
					struct vvp_io_args *args)
{
	struct iovec *iov = NULL;
	unsigned long nr_segs = args->u.normal.via_nrsegs;
	__kernel_size_t max_len;
	struct iosvc_thread_info *iti = NULL;
	ENTRY;

	/* see if the iovec held in iti can be used this time */
	iti = iosvc_find_iti(env);
	if (!iti) {
		CWARN("iosvc_thread_info not found: env=%p\n", env);
		RETURN(ERR_PTR(-EFAULT));
	}

	if (!iosvc_iovec_cache_is_enable())
		GOTO(out, iov = NULL);

	if (!iti->iti_iovec)
		GOTO(out, iov = NULL);

	if (iti->iti_nrsegs < nr_segs)
		GOTO(out, iov = NULL);

	max_len = iosvc_get_maxlen(args->u.normal.via_iov, nr_segs);
	if (max_len * nr_segs > (((u64)iosvc_max_iovec_mb) << 20))
		GOTO(out, iov = ERR_PTR(-EINVAL));
	else if (iti->iti_max_len < max_len)
		GOTO(out, iov = NULL);
	iov = iti->iti_iovec;

out:
	if (!iov && iti->iti_iovec) {
		/* iti_iovec may be changed in I/O operation so we should use
		 * iti_iovec_frame, the original copy, to free memory */
		iosvc_free_iovec(iti->iti_iovec_frame, iti->iti_nrsegs,
				 iti->iti_max_len, 1);
		iti->iti_iovec_frame = NULL;
		OBD_FREE_LARGE(iti->iti_iovec,
			       sizeof(struct iovec) * iti->iti_nrsegs);
		iti->iti_iovec = NULL;
		iti->iti_nrsegs = 0;
		iti->iti_max_len = 0;
	}
	RETURN(iov);
}

int iosvc_alloc_iovec(const struct lu_env *env, struct vvp_io_args *args)
{
	int rc = 0;
	struct iovec *iov = NULL;
	unsigned long seg, nr_segs = args->u.normal.via_nrsegs;
	__kernel_size_t max_len = 0;
	ENTRY;

	OBD_ALLOC_LARGE(iov, sizeof(struct iovec) * nr_segs);
	if (!iov)
		GOTO(out, rc = -ENOMEM);
	memset(iov, 0, sizeof(struct iovec) * nr_segs);

	max_len = iosvc_get_maxlen(args->u.normal.via_iov, nr_segs);
	if (max_len <= 0)
		GOTO(out, rc = -EINVAL);

	for (seg = 0; seg < nr_segs; seg++) {
		struct iovec *dest = iov + seg;
		OBD_ALLOC_LARGE(dest->iov_base, max_len);
		if (!dest->iov_base)
			GOTO(out, rc = -ENOMEM);
	}
	rc = iosvc_manage_iovec(env, iov, args->u.normal.via_iov,
				nr_segs, max_len);
	if (rc < 0)
		GOTO(out, rc);

	args->u.normal.via_iov = iov;
out:
	if (rc < 0 && iov) {
		iosvc_free_iovec(iov, nr_segs, max_len, 0);
		iov = NULL;
	}
	RETURN(rc);
}

int iosvc_reuse_iovec(const struct lu_env *env, struct vvp_io_args *args,
		     struct iovec *iov)
{
	int rc = 0;
	unsigned long nr_segs = args->u.normal.via_nrsegs;
	__kernel_size_t max_len = 0;
	ENTRY;

	max_len = iosvc_get_maxlen(args->u.normal.via_iov, nr_segs);
	if (max_len <= 0)
		GOTO(out, rc = -EINVAL);

	rc = iosvc_manage_iovec(env, NULL, args->u.normal.via_iov, 0, 0);
	if (rc < 0)
		GOTO(out, rc);

	args->u.normal.via_iov = iov;
	/* don't need to update iosvc info held in iti here */
out:
	/* I decided not to free the iovec held in iti in this function
	 * even if error occurs */
	RETURN(rc);
}

int iosvc_copy_iovec(struct iosvc_thread_info *iti)
{
	int rc = 0;
	unsigned long seg, nrsegs = iti->iti_nrsegs;

	for (seg = 0; seg < nrsegs; seg++) {
		struct iovec *dest = iti->iti_iovec + seg,
			     *src  = iti->iti_iovec_src + seg;
		rc = copy_from_user(dest->iov_base, src->iov_base,
				    src->iov_len);
		if (rc != 0)
			GOTO(out, rc = -EFAULT);
		dest->iov_len = src->iov_len;
	}
	iti->iti_iovec_src = NULL;
out:
	RETURN(rc);
}

int iosvc_setup_iovec(const struct lu_env *env, struct vvp_io_args *args)
{
	int rc = 0;
	struct iovec *iov;
	ENTRY;

	iov = iosvc_check_and_get_iovec(env, args);
	if (IS_ERR(iov))
		GOTO(out, rc = PTR_ERR(iov));
	else if (iov)
		rc = iosvc_reuse_iovec(env, args, iov);
	else
		rc = iosvc_alloc_iovec(env, args);

out:
	RETURN(rc);
}

int iosvc_duplicate_env(const struct lu_env *env, struct vvp_io_args *args)
{
	int rc = 0;
	ENTRY;
	/* duplicate iovec */
	rc = iosvc_setup_iovec(env, args);
	if (!rc) {
		struct kiocb *iocb;
		iocb = &ll_env_info(env)->lti_kiocb;
		/* duplicate kiocb */
		memcpy(iocb, args->u.normal.via_iocb, sizeof(struct kiocb));
		args->u.normal.via_iocb = iocb;
	}
	RETURN(rc);
}

/*
 * success: returns number of items it has handled
 * fail   : error no
 */
int iosvc_sync_io(struct ll_inode_info *lli)
{
	int rc = 0, count = 0, item_count;
	size_t size;
	struct iosvc_item *item = NULL, **items = NULL;
	ENTRY;

	item_count = lli->lli_iosvc_item_count;
	if (item_count == 0)
		GOTO(out, rc = 0); /* There's nothing to do */

	size = sizeof(struct iosvc_item *) * item_count;
	OBD_ALLOC(items, size);
	if (!items)
		GOTO(out, rc = -ENOMEM);
	memset(items, 0, size);

	/* The actual number of items which are being handled by iosvc thread
	 * may be already different from 'item_count' at this time but it's a
	 * expected behavior */
	spin_lock(&lli->lli_iosvc_lock);
	list_for_each_entry(item, &lli->lli_iosvc_item_head, ii_inode_list) {
		iosvc_get_item(item);
		items[count] = item;
		/* we don't have enough space to store new data any more */
		if (++count >= item_count)
			break;
	}
	spin_unlock(&lli->lli_iosvc_lock);

	/* waits for all the I/O to complete */
	for (count = 0; count < item_count; count++) {
		item = items[count];
		/* it's possible that items have no item */
		if (item) {
			CDEBUG(D_VFSTRACE,
			       "start sync lli: %p, item: %p\n", lli, item);
			wait_for_completion(&item->ii_io_finished);
			iosvc_put_item(item);
		}
	}
	rc = count;
out:
	if (items)
		OBD_FREE(items, size);

	RETURN(rc);
}


int iosvc_get_and_clear_rc(struct ll_inode_info *lli)
{
	int rc = 0;

	spin_lock(&lli->lli_iosvc_lock);
	rc = lli->lli_iosvc_rc;
	lli->lli_iosvc_rc = 0;
	spin_unlock(&lli->lli_iosvc_lock);

	return rc;
}

int iosvc_get_rc(struct ll_inode_info *lli)
{
	int rc;
	spin_lock(&lli->lli_iosvc_lock);
	rc = lli->lli_iosvc_rc;
	spin_unlock(&lli->lli_iosvc_lock);
	return rc;
}

void iosvc_set_rc(struct ll_inode_info *lli, int error)
{
	spin_lock(&lli->lli_iosvc_lock);
	if (error < 0 && !lli->lli_iosvc_rc)
		lli->lli_iosvc_rc = error;
	spin_unlock(&lli->lli_iosvc_lock);
	return;
}
