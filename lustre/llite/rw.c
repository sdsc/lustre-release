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
 * Copyright (c) 2011, 2016, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/llite/rw.c
 *
 * Lustre Lite I/O page cache routines shared by different kernel revs
 */

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

#include <libcfs/libcfs_ptask.h>
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

int ll_readpage(struct file *file, struct page *vmpage)
{
	struct inode *inode = file_inode(file);
	struct cl_object *clob = ll_i2info(inode)->lli_clob;
	struct ll_cl_context *lcc;
	const struct lu_env *env;
	struct cl_io *io;
	struct cl_page *page;
	struct cl_2queue *queue;
	int rc = 0;
	ENTRY;

	lcc = ll_cl_find(file);
	if (unlikely(lcc == NULL))
		GOTO(out_unlock, rc = -EIO);

	env = lcc->lcc_env;
	io  = lcc->lcc_io;
	if (io == NULL) /* fast read: page cache miss */
		GOTO(out_unlock, rc = -ENODATA);

	LASSERT(io->ci_state == CIS_IO_GOING);

	page = cl_page_find(env, clob, vmpage->index, vmpage, CPT_CACHEABLE);
	if (unlikely(IS_ERR(page)))
		GOTO(out_unlock, rc = PTR_ERR(page));

	LASSERT(page->cp_type == CPT_CACHEABLE);

	/* Page from a non-object file. */
	if (unlikely(PageUptodate(vmpage)))
		GOTO(out_page_put, rc = 0);

	queue = &io->ci_queue;
	cl_2queue_init(queue);

	cl_page_assume(env, io, page);
	cl_2queue_add(queue, page);
	cl_page_put(env, page);

	if (queue->c2_qin.pl_nr > 0) {
		CDEBUG(D_READA, "submit %u pages at page offset %lu\n",
				queue->c2_qin.pl_nr, vmpage->index);
		rc = cl_io_submit_rw(env, io, CRT_READ, queue);
	}

	/* Unlock unsent pages in case of error. */
	cl_page_list_disown(env, io, &queue->c2_qin);
	cl_2queue_fini(env, queue);
	RETURN(0);

out_page_put:
	cl_page_put(env, page);
out_unlock:
	unlock_page(vmpage);
	RETURN(rc);
}

struct ll_readpages_desc {
	const struct lu_env	*rpd_env;
	struct cl_io		*rpd_io;
	struct cl_read_ahead	*rpd_ra;
};

static int ll_readpages_filler(void *data, struct page *vmpage)
{
	struct ll_readpages_desc *desc = data;
	const struct lu_env *env = desc->rpd_env;
	struct cl_io *io = desc->rpd_io;
	struct cl_object *clob = io->ci_obj;
	struct cl_read_ahead *ra = desc->rpd_ra;
	struct cl_page *page;
	pgoff_t page_idx = vmpage->index;
	int rc = 0;
	ENTRY;

	if (ra->cra_end == 0 || ra->cra_end < page_idx) {
		cl_read_ahead_release(env, ra);

		rc = cl_io_read_ahead(env, io, page_idx, ra);
		if (rc < 0)
			GOTO(out_unlock, rc = 0);

		CDEBUG(D_READA, "CLIO readahead: page offset: %lu, "
				"cra_end: %lu, cra_rpc_size: %lu\n",
				page_idx, ra->cra_end, ra->cra_rpc_size);

		if (ra->cra_end < page_idx)
			GOTO(out_unlock, rc = 0);
	}

	page = cl_page_find(env, clob, page_idx, vmpage, CPT_CACHEABLE);
	if (unlikely(IS_ERR(page)))
		GOTO(out_unlock, rc = PTR_ERR(page));

	LASSERT(page->cp_type == CPT_CACHEABLE);

	/* Page from a non-object file */
	if (unlikely(PageUptodate(vmpage)))
		GOTO(out_page_put, rc = 0);

	cl_page_assume(env, io, page);
	cl_2queue_add(&desc->rpd_io->ci_queue, page);
	cl_page_put(env, page);

	CDEBUG(D_READA, "queue 1 page at page offset %lu\n", page_idx);
	RETURN(0);

out_page_put:
	cl_page_put(env, page);
out_unlock:
	unlock_page(vmpage);
	RETURN(rc);
}

struct ll_readpages_ptask_args {
	struct ll_readpages_desc desc;
	struct list_head	 pages;
	struct address_space	*mapping;
	struct file		*file;
};

static int ll_readpages_ptask(struct cfs_ptask *ptask)
{
	struct ll_readpages_ptask_args *pargs = ptask->pt_cbdata;
	struct file *file = pargs->file;
	struct lu_env *env;
	struct cl_io *io;
	int rc = 0;
	int rc2 = 0;
	__u16 refcheck;
	ENTRY;

	env = cl_env_get(&refcheck);
	if (IS_ERR(env)) {
		/* Clean up the pages */
		put_pages_list(&pargs->pages);
		fput(file);
		RETURN(PTR_ERR(env));
	}

	io = vvp_env_thread_io(env);
	ll_io_init(io, file, false, true);
	if (cl_io_rw_init(env, io, CIT_READ, 0, ULLONG_MAX) == 0) {
		struct cl_2queue *queue;
		struct vvp_io *vio = vvp_env_io(env);
		struct ll_file_data *fd  = LUSTRE_FPRIVATE(file);
		struct cl_read_ahead ra = { 0 };

		vio->vui_fd = fd;
		vio->vui_io_subtype = IO_NORMAL;

		ll_cl_add(file, env, io, LCC_RW);
		/* SKIP: cl_io_iter_init(), cl_io_lock() and cl_io_start() */
		io->ci_state = CIS_IO_GOING;

		queue = &io->ci_queue;
		cl_2queue_init(queue);

		pargs->desc.rpd_env = env;
		pargs->desc.rpd_io  = io;
		pargs->desc.rpd_ra  = &ra;
		rc = read_cache_pages(pargs->mapping, &pargs->pages,
				      ll_readpages_filler, &pargs->desc);

		cl_read_ahead_release(env, &ra);

		if (queue->c2_qin.pl_nr > 0) {
			CDEBUG(D_READA, "submit %u pages\n",
					queue->c2_qin.pl_nr);
			rc2 = cl_io_submit_rw(env, io, CRT_READ, queue);
		}

		rc = rc ?: rc2;

		/* Unlock unsent pages in case of error. */
		cl_page_list_disown(env, io, &queue->c2_qin);
		cl_2queue_fini(env, queue);

		/* SKIP: cl_io_end(), cl_io_unlock() and cl_io_iter_fini() */
		io->ci_state = CIS_IT_ENDED;
		ll_cl_remove(file, env);
	} else {
		/* cl_io_rw_init() handled IO */
		rc = io->ci_result;
	}

	cl_io_fini(env, io);

	/* Clean up the pages */
	put_pages_list(&pargs->pages);
	fput(file);

	cl_env_put(env, &refcheck);
	RETURN(rc);
}

#define list_to_page(head) (list_entry((head)->prev, struct page, lru))

int ll_readpages(struct file *file, struct address_space *mapping,
		 struct list_head *pages, unsigned int nr_pages)
{
	struct cl_2queue *queue;
	struct ll_cl_context *lcc;
	struct cl_read_ahead ra = { 0 };
	struct ll_readpages_desc desc = {
		.rpd_ra = &ra,
	};
	int rc = 0;
	int rc2 = 0;
	ENTRY;

	lcc = ll_cl_find(file);
	if (lcc == NULL)
		RETURN(-EIO);

	desc.rpd_env = lcc->lcc_env;
	desc.rpd_io  = lcc->lcc_io;
	if (desc.rpd_io == NULL) { /* fast read: page cache miss */
		struct {
			struct cfs_ptask task;
			struct ll_readpages_ptask_args args;
		} *pt;
		size_t npages = cfs_ptasks_estimate_chunk(PTT_RDAH,
				nr_pages << PAGE_SHIFT, NULL) >> PAGE_SHIFT;
		size_t i = 0;

next_chunk:
		pt = kmalloc(sizeof(*pt), GFP_KERNEL);
		if (pt == NULL)
			GOTO(out, rc = -ENOMEM);

		get_file(file);
		pt->args.file = file;
		pt->args.mapping = mapping;
		INIT_LIST_HEAD(&pt->args.pages);

		for (i = 0; i < npages && !list_empty(pages); i++) {
			struct page *vmpage = list_to_page(pages);
			list_del(&vmpage->lru);
			list_add(&vmpage->lru, &pt->args.pages);
		}

		rc = cfs_ptask_init(&pt->task, ll_readpages_ptask, &pt->args,
				    PTF_AUTOFREE, smp_processor_id());
		if (!rc)
			rc = cfs_ptask_submit(&pt->task, PTT_RDAH);

		if (rc) {
			/* Clean up the pages */
			put_pages_list(&pt->args.pages);
			fput(file);
			kfree(pt);
		}
out:
		CDEBUG(D_READA, "readahead: file: %s, submit %zu of %u pages: "
				"rc: %d\n", file_dentry(file)->d_name.name,
				i, nr_pages, rc);

		if (!rc && !list_empty(pages))
			goto next_chunk;

		RETURN(-ENODATA);
	}

	LASSERT(desc.rpd_io->ci_state == CIS_IO_GOING);

	queue = &desc.rpd_io->ci_queue;
	cl_2queue_init(queue);

	rc = read_cache_pages(mapping, pages, ll_readpages_filler, &desc);

	cl_read_ahead_release(desc.rpd_env, &ra);

	if (queue->c2_qin.pl_nr > 0) {
		CDEBUG(D_READA, "submit %u of %u pages\n",
				queue->c2_qin.pl_nr, nr_pages);
		rc2 = cl_io_submit_rw(desc.rpd_env, desc.rpd_io, CRT_READ,
				      queue);
	}

	rc = rc ?: rc2;

	/* Unlock unsent pages in case of error. */
	cl_page_list_disown(desc.rpd_env, desc.rpd_io, &queue->c2_qin);
	cl_2queue_fini(desc.rpd_env, queue);

	RETURN(rc);
}
