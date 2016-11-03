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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/llite/rw.c
 *
 * Lustre Lite I/O page cache routines shared by different kernel revs
 */

#include <linux/kthread.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/string.h>
#include <linux/stat.h>
#include <linux/errno.h>
#include <linux/unistd.h>
#include <linux/writeback.h>
#include <asm/uaccess.h>

#include <linux/fs.h>
#include <linux/stat.h>
#include <asm/uaccess.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
/* current_is_kswapd() */
#include <linux/swap.h>

#define DEBUG_SUBSYSTEM S_LLITE

#include <obd_cksum.h>
#include "llite_internal.h"
#include <lustre_compat.h>

int ll_writepage(struct page *vmpage, struct writeback_control *wbc)
{
	struct inode	       *inode = vmpage->mapping->host;
	struct ll_inode_info   *lli   = ll_i2info(inode);
        struct lu_env          *env;
        struct cl_io           *io;
        struct cl_page         *page;
        struct cl_object       *clob;
	bool redirtied = false;
	bool unlocked = false;
        int result;
	__u16 refcheck;
        ENTRY;

        LASSERT(PageLocked(vmpage));
        LASSERT(!PageWriteback(vmpage));

	LASSERT(ll_i2dtexp(inode) != NULL);

	env = cl_env_get(&refcheck);
	if (IS_ERR(env))
		GOTO(out, result = PTR_ERR(env));

        clob  = ll_i2info(inode)->lli_clob;
        LASSERT(clob != NULL);

	io = vvp_env_thread_io(env);
        io->ci_obj = clob;
	io->ci_ignore_layout = 1;
        result = cl_io_init(env, io, CIT_MISC, clob);
        if (result == 0) {
                page = cl_page_find(env, clob, vmpage->index,
                                    vmpage, CPT_CACHEABLE);
		if (!IS_ERR(page)) {
			lu_ref_add(&page->cp_reference, "writepage",
				   current);
			cl_page_assume(env, io, page);
			result = cl_page_flush(env, io, page);
			if (result != 0) {
				/*
				 * Re-dirty page on error so it retries write,
				 * but not in case when IO has actually
				 * occurred and completed with an error.
				 */
				if (!PageError(vmpage)) {
					redirty_page_for_writepage(wbc, vmpage);
					result = 0;
					redirtied = true;
				}
			}
			cl_page_disown(env, io, page);
			unlocked = true;
			lu_ref_del(&page->cp_reference,
				   "writepage", current);
			cl_page_put(env, page);
		} else {
			result = PTR_ERR(page);
		}
        }
        cl_io_fini(env, io);

	if (redirtied && wbc->sync_mode == WB_SYNC_ALL) {
		loff_t offset = cl_offset(clob, vmpage->index);

		/* Flush page failed because the extent is being written out.
		 * Wait for the write of extent to be finished to avoid
		 * breaking kernel which assumes ->writepage should mark
		 * PageWriteback or clean the page. */
		result = cl_sync_file_range(inode, offset,
					    offset + PAGE_SIZE - 1,
					    CL_FSYNC_LOCAL, 1);
		if (result > 0) {
			/* actually we may have written more than one page.
			 * decreasing this page because the caller will count
			 * it. */
			wbc->nr_to_write -= result - 1;
			result = 0;
		}
	}

	cl_env_put(env, &refcheck);
	GOTO(out, result);

out:
	if (result < 0) {
		if (!lli->lli_async_rc)
			lli->lli_async_rc = result;
		SetPageError(vmpage);
		if (!unlocked)
			unlock_page(vmpage);
	}
	return result;
}

int ll_writepages(struct address_space *mapping, struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct ll_sb_info *sbi = ll_i2sbi(inode);
	loff_t start;
	loff_t end;
	enum cl_fsync_mode mode;
	int range_whole = 0;
	int result;
	int ignore_layout = 0;
	ENTRY;

	if (wbc->range_cyclic) {
		start = mapping->writeback_index << PAGE_SHIFT;
		end = OBD_OBJECT_EOF;
	} else {
		start = wbc->range_start;
		end = wbc->range_end;
		if (end == LLONG_MAX) {
			end = OBD_OBJECT_EOF;
			range_whole = start == 0;
		}
	}

	mode = CL_FSYNC_NONE;
	if (wbc->sync_mode == WB_SYNC_ALL)
		mode = CL_FSYNC_LOCAL;

	if (sbi->ll_umounting)
		/* if the mountpoint is being umounted, all pages have to be
		 * evicted to avoid hitting LBUG when truncate_inode_pages()
		 * is called later on. */
		ignore_layout = 1;

	if (ll_i2info(inode)->lli_clob == NULL)
		RETURN(0);

	result = cl_sync_file_range(inode, start, end, mode, ignore_layout);
	if (result > 0) {
		wbc->nr_to_write -= result;
		result = 0;
	 }

	if (wbc->range_cyclic || (range_whole && wbc->nr_to_write > 0)) {
		if (end == OBD_OBJECT_EOF)
			mapping->writeback_index = 0;
		else
			mapping->writeback_index = (end >> PAGE_SHIFT) + 1;
	}
	RETURN(result);
}

struct ll_cl_context *ll_cl_find(struct file *file)
{
	struct ll_file_data *fd = LUSTRE_FPRIVATE(file);
	struct ll_cl_context *lcc;
	struct ll_cl_context *found = NULL;

	read_lock(&fd->fd_lock);
	list_for_each_entry(lcc, &fd->fd_lccs, lcc_list) {
		if (lcc->lcc_cookie == current) {
			found = lcc;
			break;
		}
	}
	read_unlock(&fd->fd_lock);

	return found;
}

void ll_cl_add(struct file *file, const struct lu_env *env, struct cl_io *io,
	       enum lcc_type type)
{
	struct ll_file_data *fd = LUSTRE_FPRIVATE(file);
	struct ll_cl_context *lcc = &ll_env_info(env)->lti_io_ctx;

	memset(lcc, 0, sizeof(*lcc));
	INIT_LIST_HEAD(&lcc->lcc_list);
	lcc->lcc_cookie = current;
	lcc->lcc_env = env;
	lcc->lcc_io = io;
	lcc->lcc_type = type;

	write_lock(&fd->fd_lock);
	list_add(&lcc->lcc_list, &fd->fd_lccs);
	write_unlock(&fd->fd_lock);
}

void ll_cl_remove(struct file *file, const struct lu_env *env)
{
	struct ll_file_data *fd = LUSTRE_FPRIVATE(file);
	struct ll_cl_context *lcc = &ll_env_info(env)->lti_io_ctx;

	write_lock(&fd->fd_lock);
	list_del_init(&lcc->lcc_list);
	write_unlock(&fd->fd_lock);
}

static int ll_readpage_extend_one(const struct lu_env *env, struct cl_io *io,
				  struct cl_page_list *queue, pgoff_t index)
{
	struct cl_object *clob  = io->ci_obj;
	struct inode     *inode = vvp_object_inode(clob);
	struct page      *vmpage;
	struct cl_page   *page;
	int               rc    = 0;
	const char       *msg   = NULL;
	ENTRY;

	vmpage = grab_cache_page_nowait(inode->i_mapping, index);
	if (vmpage == NULL) {
		msg = "g_c_p_n failed";
		GOTO(out, rc = -EBUSY);
	}

	/* Check if vmpage was truncated or reclaimed */
	if (vmpage->mapping != inode->i_mapping) {
		msg = "g_c_p_n returned invalid page";
		GOTO(out, rc = -EBUSY);
	}

	page = cl_page_find(env, clob, vmpage->index, vmpage, CPT_CACHEABLE);
	if (IS_ERR(page)) {
		msg = "cl_page_find failed";
		GOTO(out, rc = PTR_ERR(page));
	}

	lu_ref_add(&page->cp_reference, "readpage", current);
	cl_page_assume(env, io, page);
	/* Page from a non-object file */
	if (PageUptodate(vmpage)) {
		cl_page_unassume(env, io, page);
		unlock_page(vmpage);
	} else {
		cl_page_list_add(queue, page);
	}
	lu_ref_del(&page->cp_reference, "readpage", current);
	cl_page_put(env, page);

out:
	if (vmpage != NULL) {
		if (rc)
			unlock_page(vmpage);
		put_page(vmpage);
	}

	if (msg != NULL)
		CDEBUG(D_READA, "%s\n", msg);

	RETURN(rc);
}

/**
 * Extend the readpage to one RA block if possible. Because ll_readpage()
 * will only be called when both async RA and sync RA failed, do not extend
 * too many pages.
 */
static void ll_readpage_extend(const struct lu_env *env, struct cl_io *io,
			       struct cl_page_list *queue, pgoff_t page_idx)
{
	pgoff_t			index;
	pgoff_t			start;
	pgoff_t			end;
	struct cl_read_ahead	ra = { 0 };
	int			rc;

	/* TODO: extend to RPC size */
	start = page_idx & LL_RA_SEQ_BLOCK_PAGE_MASK;
	end = start + LL_RA_SEQ_BLOCK_NPAGES;
	for (index = start; index < end; index++) {
		if (index == page_idx)
			continue;

		if (ra.cra_end == 0 || ra.cra_end < index) {
			cl_read_ahead_release(env, &ra);

			rc = cl_io_read_ahead(env, io, index, &ra);
			if (rc < 0)
				break;

			if (ra.cra_end < index)
				break;
		}

		rc = ll_readpage_extend_one(env, io, queue, index);
		if (rc < 0)
			break;
	}
	cl_read_ahead_release(env, &ra);

	/* TODO: call ll_async_readahead to push forward RA window */
	return;
}

int ll_readpage(struct file *file, struct page *vmpage)
{
	struct inode		*inode = file->f_path.dentry->d_inode;
	struct cl_object	*clob = ll_i2info(inode)->lli_clob;
	struct ll_cl_context	*lcc;
	const struct lu_env	*env;
	struct cl_io		*io;
	struct cl_page		*page;
	int			 rc = 0;
	int			 rc2 = 0;
	struct cl_2queue	*queue;
	ENTRY;

	CDEBUG(D_READA, "readpage at page offset %lu\n",
	       vmpage->index);

	lcc = ll_cl_find(file);
	if (lcc == NULL)
		GOTO(out, rc = -ENOMEM);

	env = lcc->lcc_env;
	io  = lcc->lcc_io;
	if (io == NULL) { /* fast read */
		rc = -ENODATA;

		/* TODO: need to verify the layout version to make sure
		 * the page is not invalid due to layout change. */
		page = cl_vmpage_page(vmpage, clob);
		if (page == NULL) {
			unlock_page(vmpage);
			RETURN(rc);
		}

		unlock_page(vmpage);
		cl_page_put(env, page);
		RETURN(rc);
	}

	LASSERT(io->ci_state == CIS_IO_GOING);
	page = cl_page_find(env, clob, vmpage->index, vmpage,
			    CPT_CACHEABLE);
	if (IS_ERR(page))
		GOTO(out, rc = PTR_ERR(page));

	/* Page from a non-object file. */
	if (PageUptodate(vmpage)) {
		cl_page_put(env, page);
		GOTO(out, rc = 0);
	}
	queue  = &io->ci_queue;
	cl_2queue_init(queue);
	cl_page_assume(env, io, page);
	cl_2queue_add(queue, page);
	cl_page_put(env, page);

	ll_readpage_extend(env, io, &queue->c2_qin, vmpage->index);

	CDEBUG(D_READA, "submit page %u at page offset %lu\n",
	       queue->c2_qin.pl_nr, vmpage->index);

	if (queue->c2_qin.pl_nr > 0)
		rc2 = cl_io_submit_rw(env, io, CRT_READ, queue);

	rc = rc ?: rc2;

	cl_page_list_disown(env, io, &queue->c2_qin);
	cl_2queue_fini(env, queue);

	RETURN(rc);
out:
	unlock_page(vmpage);
	RETURN(rc);
}

int ll_page_sync_io(const struct lu_env *env, struct cl_io *io,
		    struct cl_page *page, enum cl_req_type crt)
{
	struct cl_2queue  *queue;
	int result;

	LASSERT(io->ci_type == CIT_READ || io->ci_type == CIT_WRITE);

	queue = &io->ci_queue;
	cl_2queue_init_page(queue, page);

	result = cl_io_submit_sync(env, io, crt, queue, 0);
	LASSERT(cl_page_is_owned(page, io));

	if (crt == CRT_READ)
		/*
		 * in CRT_WRITE case page is left locked even in case of
		 * error.
		 */
		cl_page_list_disown(env, io, &queue->c2_qin);
	cl_2queue_fini(env, queue);

	return result;
}

int ll_readpages(struct file *file, struct address_space *mapping,
		 struct list_head *pages, unsigned nr_pages)
{
	struct inode		*inode = file->f_path.dentry->d_inode;
	struct cl_object	*clob = ll_i2info(inode)->lli_clob;
	struct ll_cl_context	*lcc;
	const struct lu_env	*env;
	struct cl_io		*io;
	struct cl_page		*page;
	int			 rc = 0;
	int			 rc2 = 0;
	struct cl_2queue	*queue;
	pgoff_t			 page_idx;
	unsigned		 i;
	struct page		*vmpage;
	struct cl_read_ahead	 ra = { 0 };
	ENTRY;

	CDEBUG(D_READA, "going to submit readpages\n");
	lcc = ll_cl_find(file);
	if (lcc == NULL)
		RETURN(-ENOMEM);

	env = lcc->lcc_env;
	io  = lcc->lcc_io;
	LASSERT(io);
	LASSERT(io->ci_state == CIS_IO_GOING);
	queue  = &io->ci_queue;
	cl_2queue_init(queue);

	for (i = 0; i < nr_pages; i++) {
		vmpage = list_entry(pages->prev, struct page, lru);
		LASSERT(!PageLocked(vmpage));
		list_del(&vmpage->lru);

		page_idx = vmpage->index;
		if (ra.cra_end == 0 || ra.cra_end < page_idx) {
			cl_read_ahead_release(env, &ra);

			rc = cl_io_read_ahead(env, io, page_idx, &ra);
			if (rc < 0)
				break;

			if (ra.cra_end < page_idx)
				break;
		}

		if (!add_to_page_cache_lru(vmpage, mapping, page_idx,
					   GFP_KERNEL)) {
			LASSERT(!PageUptodate(vmpage));
			page = cl_page_find(env, clob, page_idx, vmpage,
					    CPT_CACHEABLE);
			if (IS_ERR(page)) {
				rc = PTR_ERR(page);
				unlock_page(vmpage);
				put_page(vmpage);
				break;
			}

			/* Check if vmpage was truncated or reclaimed */
			if (vmpage->mapping != inode->i_mapping) {
				rc = -EBUSY;
				unlock_page(vmpage);
				put_page(vmpage);
				cl_page_put(env, page);
				break;
			}

			/* Page from a non-object file */
			if (PageUptodate(vmpage)) {
				unlock_page(vmpage);
				put_page(vmpage);
				cl_page_put(env, page);
				continue;
			}

			LASSERT(page->cp_type == CPT_CACHEABLE);
			cl_page_assume(env, io, page);
			cl_2queue_add(queue, page);
			cl_page_put(env, page);
		}
		put_page(vmpage);
	}

	cl_read_ahead_release(env, &ra);

	CDEBUG(D_READA, "submit readpages %u\n", queue->c2_qin.pl_nr);

	if (queue->c2_qin.pl_nr > 0)
		rc2 = cl_io_submit_rw(env, io, CRT_READ, queue);

	rc = rc ?: rc2;

	cl_page_list_disown(env, io, &queue->c2_qin);
	cl_2queue_fini(env, queue);

	RETURN(rc);
}

#define list_to_page(head) (list_entry((head)->prev, struct page, lru))

int __ll_do_page_cache_readahead(struct address_space *mapping,
				 struct file *filp, pgoff_t offset,
				 unsigned long nr_to_read)
{
	struct inode *inode = mapping->host;
	struct page *page;
	unsigned long end_index;	/* The last page we want to read */
	LIST_HEAD(page_pool);
	int page_idx;
	int ret = 0;
	loff_t isize = i_size_read(inode);

	if (isize == 0)
		goto out;

	end_index = ((isize - 1) >> PAGE_SHIFT);

	/*
	 * Preallocate as many pages as we will need.
	 */
	for (page_idx = 0; page_idx < nr_to_read; page_idx++) {
		pgoff_t page_offset = offset + page_idx;

		if (page_offset > end_index)
			break;

		rcu_read_lock();
		page = radix_tree_lookup(&mapping->page_tree, page_offset);
		rcu_read_unlock();
		if (page)
			continue;

		page = page_cache_alloc_readahead(mapping);
		if (!page)
			break;
		page->index = page_offset;
		LASSERT(!PageUptodate(page));
		list_add(&page->lru, &page_pool);
		ret++;
	}

	/*
	 * Now start the IO.  We ignore I/O errors - if the page is not
	 * uptodate then the caller will launch readpage again, and
	 * will then handle the error.
	 */
	if (ret)
		ret = ll_readpages(filp, mapping, &page_pool, ret);
	/* Clean up the remaining pages */
	put_pages_list(&page_pool);
	BUG_ON(!list_empty(&page_pool));
out:
	return ret;
}

void ll_readahead_init(struct inode *inode, struct ll_readahead_state *ras)
{
	struct ll_sb_info	*sbi = ll_i2sbi(inode);

	spin_lock_init(&ras->lrs_lock);
	ras->lrs_window_npages = sbi->ll_max_readahead_window;
	ras->lrs_sequential.lrs_matched_number = 0;
	ras->lrs_sequential.lrs_next_block = 0;

	ras->lrs_stride.lrs_matched_number = 0;
	ras->lrs_stride.lrs_prev_valid = false;
	ras->lrs_stride.lrs_stride_valid = false;
}

struct list_head	ll_readahead_list;
spinlock_t		ll_readahead_lock;
struct list_head	ll_readahead_thread_list;
wait_queue_head_t	ll_readahead_waitq;

static void ll_readahead_work_add(struct ll_readahead_work *work)
{
	bool	wakeup = false;

	spin_lock(&ll_readahead_lock);
	if (list_empty(&ll_readahead_list))
		wakeup = true;
	list_add_tail(&work->lrw_linkage, &ll_readahead_list);
	spin_unlock(&ll_readahead_lock);

	if (wakeup)
		wake_up_all(&ll_readahead_waitq);
}

static int ll_readahead_need_move_window(unsigned long former_index,
					 unsigned long page_index)
{
	unsigned long former_block;
	unsigned long new_block;

	/*
	 * Do not submit work unless the window has move forward a RPC
	 * size, otherwise, there will be two many works which won't do
	 * any real prefetch.
	 */
	former_block = former_index >> LL_RA_SEQ_BLOCK_PAGE_SHIFT;
	new_block = page_index >> LL_RA_SEQ_BLOCK_PAGE_SHIFT;

	if (new_block > former_block)
		return 1;
	return 0;
}

void ll_readahead_sequential_window_move(struct address_space *mapping,
					 struct file *filp,
					 unsigned long page_index,
					 unsigned long npages)
{
	struct ll_file_data		*fd = LUSTRE_FPRIVATE(filp);
	struct ll_readahead_state	*ras = &fd->fd_ras;
	unsigned long			 former_index = 0;
	unsigned long			 window_size;
	unsigned long			 moving_forward = 0;
	struct ll_readahead_work	*work;

	ll_ra_stats_inc(filp, RA_STAT_HIT);

	spin_lock(&ras->lrs_lock);
	window_size = ras->lrs_window_npages;
	former_index = ras->lrs_sequential.lrs_window_start;
	if (ras->lrs_sequential.lrs_matched_number > 0 &&
	    ll_readahead_need_move_window(former_index, page_index)) {
		CDEBUG(D_READA, "updating RA start page index from %lu "
		       "to %lu\n", former_index, page_index);
		ras->lrs_sequential.lrs_window_start = page_index;
		moving_forward = page_index - former_index;
	} else {
		spin_unlock(&ras->lrs_lock);
		return;
	}
	spin_unlock(&ras->lrs_lock);

	if (window_size == 0)
		return;

	OBD_ALLOC_PTR(work);
	if (work == NULL) {
		CERROR("failed to submit readhead work because of OOM\n");
		return;
	}
	get_file(filp);
	work->lrw_filp = filp;
	/*
	 * Submit async readahead request caused by moving window
	 * forward
	 */
	LASSERT(moving_forward > 0);
	work->lrw_page_index = former_index + window_size;
	work->lrw_npages = moving_forward;
	work->lrw_pattern = LRP_SEQUENTIAL;
	ll_readahead_work_add(work);

	return;
}

void ll_sync_readahead(struct address_space *mapping, struct file *filp,
		       unsigned long page_index, unsigned long npages)
{
	if (npages > LL_RA_SYNC_NPAGES)
		npages = LL_RA_SYNC_NPAGES;

	__ll_do_page_cache_readahead(mapping, filp,
				     page_index,
				     npages);

	ll_ra_stats_inc(filp, RA_STAT_MISS);

	return;
}

static int ll_readahead_detect_sequential(struct ll_readahead_state *ras,
					  loff_t pos,
					  size_t count,
					  struct ll_readahead_work *work)
{
	struct ll_readahead_sequential	*sequential = &ras->lrs_sequential;
	int				 rc = 0;
	unsigned long			 block_index;
	unsigned long			 page_index;
	unsigned long			 next_block;

	block_index = pos >> LL_RA_SEQ_BLOCK_BYTE_SHIFT;
	next_block = (pos + count) >> LL_RA_SEQ_BLOCK_BYTE_SHIFT;
	page_index = pos >> PAGE_SHIFT;

	if (block_index != sequential->lrs_next_block) {
		/** The start offset is out of expected range */
		sequential->lrs_matched_number = 0;
		sequential->lrs_next_block = next_block;
	} else {
		CDEBUG(D_READA, "sequential matched, number %llu, pos %llu, "
		       "count %lu, block index %lu, expected next block %lu, "
		       "readahead next block %lu\n",
		       sequential->lrs_matched_number, pos, count,
		       block_index, next_block,
		       sequential->lrs_next_block);
		sequential->lrs_next_block = next_block;
		if (sequential->lrs_matched_number == 0) {
			sequential->lrs_window_start = page_index;
			if (work != NULL) {
				rc = 1;
				work->lrw_page_index = page_index;
				work->lrw_npages = ras->lrs_window_npages;
				work->lrw_pattern = LRP_SEQUENTIAL;
			}
		}
		sequential->lrs_matched_number++;
	}
	return rc;
}

static int ll_readahead_detect_stride(struct ll_readahead_state *ras,
				      loff_t pos, size_t count,
				      struct ll_readahead_work *work)
{
	struct ll_readahead_stride	*stride = &ras->lrs_stride;
	loff_t				 prev_pos = stride->lrs_prev_byte;
	loff_t				 prev_count = stride->lrs_req_nbyte;
	int				 rc = 0;

	stride->lrs_prev_byte = pos;
	stride->lrs_req_nbyte = count;

	if (!stride->lrs_prev_valid) {
		stride->lrs_prev_valid = true;
		LASSERT(!stride->lrs_stride_valid);
		LASSERT(stride->lrs_matched_number == 0);
		CDEBUG(D_READA, "init previous offset to %llu Byte, "
		       "req size %lu\n", pos, count);
		return rc;
	}

	if (!stride->lrs_stride_valid) {
		LASSERT(stride->lrs_matched_number == 0);
		if (prev_count != count ||
		    pos < prev_pos + prev_count + LL_RA_SEQ_BLOCK_NBYTES) {
			stride->lrs_prev_valid = true;
			CDEBUG(D_READA, "re-init previous offset to %llu "
			       "Byte, req size %lu\n", pos, count);
			return rc;
		}
		stride->lrs_stride_valid = true;
		stride->lrs_stride_nbyte = pos - prev_pos;
		CDEBUG(D_READA, "init stride nbyte to %llu",
		       stride->lrs_stride_nbyte);
		return rc;
	}

	if (prev_count != count ||
	    pos != prev_pos + stride->lrs_stride_nbyte) {
		stride->lrs_matched_number = 0;
		if (pos < prev_pos + prev_count + LL_RA_SEQ_BLOCK_NBYTES) {
			stride->lrs_stride_valid = false;
			CDEBUG(D_READA, "invalidate stride, pos %llu, "
			       "prev_pos %llu, prev_count %llu\n",
			       pos, prev_pos, prev_count);
		} else {
			stride->lrs_stride_nbyte = pos - prev_pos;
			CDEBUG(D_READA, "init stride to %llu Byte, pos %llu, "
			       "prev_pos %llu, prev_count %llu\n",
			       stride->lrs_stride_nbyte, pos, prev_pos,
			       prev_count);
		}
		return rc;
	}

	stride->lrs_start_byte = pos;
	if (work != NULL) {
		rc = 1;
		if (stride->lrs_matched_number == 0) {
			work->lrw_start_byte = pos;
			work->lrw_nrequest = LA_STRIDE_NREQUEST;
				work->lrw_pattern = LRP_STRIDE;
		} else {
			work->lrw_start_byte = pos +
				LA_STRIDE_NREQUEST * stride->lrs_stride_nbyte;
			work->lrw_nrequest = 1;
			work->lrw_pattern = LRP_STRIDE;
			CDEBUG(D_READA, "add work start_byte %llu\n",
			       work->lrw_start_byte);
		}
	}
	stride->lrs_matched_number++;
	CDEBUG(D_READA, "stride matched, number %llu, stride width %llu, "
	       "req size %lu\n", stride->lrs_matched_number,
	       stride->lrs_stride_nbyte, count);
	return rc;
}

static void ll_readahead_work_free(struct ll_readahead_work *work)
{
	fput(work->lrw_filp);
	OBD_FREE(work, sizeof(*work));
}

void ll_readahead_start(struct address_space *mapping, struct file *filp,
			loff_t pos, size_t count)
{
	struct ll_file_data		*fd = LUSTRE_FPRIVATE(filp);
	struct ll_readahead_state	*ras = &fd->fd_ras;
	struct ll_readahead_work	*work;
	int				 rc;
	int				 rc2;

	OBD_ALLOC_PTR(work);
	if (work != NULL) {
		get_file(filp);
		work->lrw_filp = filp;
	} else {
		CERROR("failed to submit readhead work because of OOM\n");
	}

	spin_lock(&ras->lrs_lock);
	rc = ll_readahead_detect_sequential(ras, pos, count, work);
	rc2 = ll_readahead_detect_stride(ras, pos, count,
					 rc == 1 ? NULL : work);
	spin_unlock(&ras->lrs_lock);

	if (work != NULL) {
		if (rc == 1 || rc2 == 1)
			ll_readahead_work_add(work);
		else
			ll_readahead_work_free(work);
	}
}

static void ll_readahead_read(const struct lu_env *env, struct file *filp,
			      pgoff_t offset, unsigned long nr_to_read)
{
	struct address_space	*mapping = filp->f_mapping;
	struct vvp_io		*vio = vvp_env_io(env);
	struct ll_file_data	*fd = LUSTRE_FPRIVATE(filp);
	struct cl_io		*io;
	int			 rc;

	io = ccc_env_thread_io(env);
	ll_io_init(io, filp, false);

	/**
	 * We could add the work back, but that needs memory allocation, which
	 * could also fail in this circumstance.
	 */
	rc = cl_io_rw_init(env, io, CIT_READ, offset, nr_to_read);
	if (rc)
		GOTO(out, rc);

	/* ll_file_io_generic, ll_readpages->vvp_io_read_ahead */
	vio->vui_fd  = fd;
	ll_cl_add(filp, env, io, LCC_RW);
	io->ci_state = CIS_IO_GOING; /* cl_io_start, ll_readpages */
	/* rc = cl_io_loop(env, io); */
	CDEBUG(D_READA, "async readahead %lu pages at page offset %lu\n",
	       nr_to_read, offset);
	__ll_do_page_cache_readahead(mapping, filp, offset, nr_to_read);

	ll_cl_remove(filp, env);
out:
	cl_io_fini(env, io);
	return;
}

static void ll_readhead_handle_sequential_work(const struct lu_env *env,
					       struct ll_readahead_work *work)
{
	struct file			*filp = work->lrw_filp;
	struct ll_file_data		*fd = LUSTRE_FPRIVATE(filp);
	struct ll_readahead_state	*ras = &fd->fd_ras;
	unsigned long			 window_start;
	unsigned long			 end_index;
	unsigned long			 skip_index = 0;
	unsigned long			 index;
	unsigned long			 npages;
	__u64				 matched_number;

	spin_lock(&ras->lrs_lock);
	window_start = ras->lrs_sequential.lrs_window_start;
	matched_number = ras->lrs_sequential.lrs_matched_number;
	end_index = window_start + ras->lrs_window_npages;
	spin_unlock(&ras->lrs_lock);

	/* Not sequential pattern */
	if (matched_number == 0) {
		ll_readahead_work_free(work);
		return;
	}

	CDEBUG(D_READA, "handling work, page index %lu, npages %lu, "
	       "RA window [%lu, %lu), matched_number %llu\n",
	       work->lrw_page_index, work->lrw_npages,
	       window_start, end_index, matched_number);

	if (window_start > work->lrw_page_index)
		skip_index = window_start - work->lrw_page_index;

	/* No more work inside readahead window */
	if (work->lrw_npages <= skip_index) {
		ll_readahead_work_free(work);
		return;
	}

	/* Cut the work brefore readahead window */
	work->lrw_npages -= skip_index;
	work->lrw_page_index += skip_index;

	/* No more work inside readahead window */
	if (work->lrw_page_index >= end_index) {
		ll_readahead_work_free(work);
		return;
	}

	index = work->lrw_page_index;
	npages = work->lrw_npages;
	if (npages > LL_RA_ASYNC_NPAGES)
		npages = LL_RA_ASYNC_NPAGES;
	if (work->lrw_page_index + npages > end_index)
		npages = end_index - work->lrw_page_index;

	LASSERT(npages > 0);
	LASSERT(npages <= work->lrw_npages);
	work->lrw_page_index += npages;
	work->lrw_npages -= npages;

	/**
	 * If still some work left, put it back in case someone else is idle
	 */
	get_file(filp);
	if (work->lrw_npages == 0)
		ll_readahead_work_free(work);
	else
		ll_readahead_work_add(work);
	ll_readahead_read(env, filp, index, npages);

	fput(filp);
}

static void ll_readhead_handle_stride_work(const struct lu_env *env,
					   struct ll_readahead_work *work)
{
	struct file			*filp = work->lrw_filp;
	struct ll_file_data		*fd = LUSTRE_FPRIVATE(filp);
	struct ll_readahead_state	*ras = &fd->fd_ras;
	struct ll_readahead_stride	*stride = &ras->lrs_stride;
	__u64				 matched_number;
	loff_t				 stride_nbyte;
	size_t				 req_nbyte;
	unsigned long			 req_npage;
	loff_t				 window_start_byte;
	loff_t				 window_end_byte;
	loff_t				 skip_nbyte;
	unsigned long			 skip_nreq;
	loff_t				 start_byte;

	spin_lock(&ras->lrs_lock);
	matched_number = stride->lrs_matched_number;
	stride_nbyte = stride->lrs_stride_nbyte;
	window_start_byte = stride->lrs_start_byte;
	req_nbyte = stride->lrs_req_nbyte;
	spin_unlock(&ras->lrs_lock);

	/* Not stride pattern */
	if (matched_number == 0) {
		ll_readahead_work_free(work);
		return;
	}

	if (work->lrw_start_byte < window_start_byte) {
		skip_nbyte = window_start_byte - work->lrw_start_byte;
		skip_nreq = skip_nbyte / stride_nbyte;
		/** different stride pattern, or no work left after skip */
		if (skip_nreq * stride_nbyte != skip_nbyte ||
		    work->lrw_nrequest < skip_nreq) {
			ll_readahead_work_free(work);
			return;
		}
		work->lrw_nrequest -= skip_nreq;
		work->lrw_start_byte += stride_nbyte * skip_nreq;
	}

	window_end_byte = window_start_byte + LA_STRIDE_NREQUEST * stride_nbyte;
	CDEBUG(D_READA, "handling readahead stride at offset %llu byte, "
	       "window start byte %llu, window end byte %llu\n",
	       work->lrw_start_byte, window_start_byte, window_end_byte);

	/** Work is beyond the window */
	if (work->lrw_start_byte > window_end_byte) {
		ll_readahead_work_free(work);
		return;
	}

	start_byte = work->lrw_start_byte;
	work->lrw_start_byte += stride_nbyte;
	work->lrw_nrequest--;

	/**
	 * If still some work left, put it back in case someone else is idle
	 */
	get_file(filp);
	if (work->lrw_nrequest == 0 || work->lrw_start_byte > window_end_byte)
		ll_readahead_work_free(work);
	else
		ll_readahead_work_add(work);

	req_npage = (req_nbyte + PAGE_SIZE - 1) >> PAGE_SHIFT;
	CDEBUG(D_READA, "readahead stride at page offset %llu, "
	       "size page %lu\n", start_byte >> PAGE_SHIFT, req_npage);
	ll_readahead_read(env, filp, start_byte >> PAGE_SHIFT, req_npage);
	fput(filp);
	return;
}

static void ll_readhead_handle_work(const struct lu_env *env,
				    struct ll_readahead_work *work)
{
	switch (work->lrw_pattern) {
	case LRP_SEQUENTIAL:
		ll_readhead_handle_sequential_work(env, work);
		break;
	case LRP_STRIDE:
		ll_readhead_handle_stride_work(env, work);
		break;
	default:
		CERROR("unkown readahead pattern %d\n", work->lrw_pattern);
		break;
	}
}
static void ll_readhead_handle_works(struct ptlrpc_thread *thread)
{

	struct lu_env			*env;
	__u16				 refcheck;
	int				 rc;
	struct ll_readahead_work	*work;

	/*
	 * No module reference should be taken, otherwise unable to rmmod
	 * lustre.ko
	 */
	env = cl_env_alloc(&refcheck, LCT_NOREF);
	if (IS_ERR(env)) {
		rc = PTR_ERR(env);
		thread->t_id = rc;
	}

	spin_lock(&ll_readahead_lock);
	while (!list_empty(&ll_readahead_list)) {
		/**
		 * Check whether to exit again here since readahead is
		 * heavy.
		 */
		if (unlikely(!thread_is_running(thread)))
			break;
		work = list_entry(ll_readahead_list.next,
				  struct ll_readahead_work,
				  lrw_linkage);
		list_del_init(&work->lrw_linkage);
		spin_unlock(&ll_readahead_lock);

		ll_readhead_handle_work(env, work);

		spin_lock(&ll_readahead_lock);
	}
	spin_unlock(&ll_readahead_lock);

	cl_env_put(env, &refcheck);
}

static int ll_readahead_main(void *args)
{
	struct ptlrpc_thread		*thread = args;
	struct l_wait_info		 lwi = { 0 };
	int				 rc = 0;
	ENTRY;

	spin_lock(&ll_readahead_lock);
	thread_set_flags(thread, rc ? SVC_STOPPED : SVC_RUNNING);
	wake_up_all(&thread->t_ctl_waitq);
	spin_unlock(&ll_readahead_lock);

	if (rc)
		RETURN(rc);

	while (1) {
		spin_lock(&ll_readahead_lock);
		if (unlikely(!thread_is_running(thread))) {
			spin_unlock(&ll_readahead_lock);
			break;
		}
		spin_unlock(&ll_readahead_lock);

		if (!list_empty(&ll_readahead_list))
			ll_readhead_handle_works(thread);

		l_wait_event(ll_readahead_waitq,
			     !list_empty(&ll_readahead_list) ||
			     !thread_is_running(thread),
			     &lwi);
	}

	spin_lock(&ll_readahead_lock);
	thread_set_flags(thread, SVC_STOPPED);
	wake_up_all(&thread->t_ctl_waitq);
	spin_unlock(&ll_readahead_lock);

	RETURN(0);
}

static int ll_start_readahead_thread(int id)
{
	struct l_wait_info	 lwi = { 0 };
	struct task_struct	*task;
	int			 rc;
	struct ptlrpc_thread	*thread;

	OBD_ALLOC_PTR(thread);
	if (thread == NULL)
		RETURN(-ENOMEM);

	init_waitqueue_head(&thread->t_ctl_waitq);
	thread->t_id = id;

	thread_add_flags(thread, SVC_STARTING);
	snprintf(thread->t_name, PTLRPC_THR_NAME_LEN,
		 "ll_readahead_%02d", id);

	task = kthread_run(ll_readahead_main, thread, thread->t_name);
	if (IS_ERR(task)) {
		rc = PTR_ERR(task);
		CERROR("cannot start prefetch thread: rc = %d\n", rc);
	} else {
		rc = 0;
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_running(thread) ||
			     thread_is_stopped(thread),
			     &lwi);
		rc = thread_is_stopped(thread) ? thread->t_id : 0;
	}

	if (rc)
		OBD_FREE_PTR(thread);
	else
		list_add(&thread->t_link, &ll_readahead_thread_list);

	RETURN(rc);
}

void ll_stop_readahead_threads(void)
{
	struct ptlrpc_thread	*thread;
	struct list_head	 zombie;
	struct l_wait_info	 lwi = { 0 };

	INIT_LIST_HEAD(&zombie);

	spin_lock(&ll_readahead_lock);
	list_for_each_entry(thread, &ll_readahead_thread_list, t_link) {
		thread_set_flags(thread, SVC_STOPPING);
	}
	wake_up_all(&ll_readahead_waitq);

	while (!list_empty(&ll_readahead_thread_list)) {
		thread = list_entry(ll_readahead_thread_list.next,
				    struct ptlrpc_thread, t_link);
		if (thread_is_stopped(thread)) {
			list_del(&thread->t_link);
			list_add(&thread->t_link, &zombie);
			continue;
		}
		spin_unlock(&ll_readahead_lock);

		l_wait_event(thread->t_ctl_waitq,
			     thread_is_stopped(thread), &lwi);

		spin_lock(&ll_readahead_lock);
	}
	spin_unlock(&ll_readahead_lock);

	while (!list_empty(&zombie)) {
		thread = list_entry(zombie.next,
				    struct ptlrpc_thread, t_link);
		list_del(&thread->t_link);
		OBD_FREE_PTR(thread);
	}
}

int ll_start_readahead_threads(void)
{
	int	i;
	int	rc;

	spin_lock_init(&ll_readahead_lock);
	INIT_LIST_HEAD(&ll_readahead_list);
	init_waitqueue_head(&ll_readahead_waitq);
	INIT_LIST_HEAD(&ll_readahead_thread_list);

	for (i = 0; i < LL_RA_NTHREADS; i++) {
		rc = ll_start_readahead_thread(i);
		if (rc) {
			ll_stop_readahead_threads();
			break;
		}
	}
	return rc;
}
