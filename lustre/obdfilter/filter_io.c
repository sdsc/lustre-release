/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  linux/fs/obdfilter/filter_io.c
 *
 *  Copyright (c) 2001-2003 Cluster File Systems, Inc.
 *   Author: Peter Braam <braam@clusterfs.com>
 *   Author: Andreas Dilger <adilger@clusterfs.com>
 *   Author: Phil Schwan <phil@clusterfs.com>
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

#define DEBUG_SUBSYSTEM S_FILTER

#include <linux/config.h>
#include <linux/module.h>
#include <linux/pagemap.h> // XXX kill me soon
#include <linux/version.h>

#include <linux/obd_class.h>
#include <linux/lustre_fsfilt.h>
#include "filter_internal.h"

static int filter_start_page_read(struct inode *inode, struct niobuf_local *lnb)
{
        struct address_space *mapping = inode->i_mapping;
        struct page *page;
        unsigned long index = lnb->offset >> PAGE_SHIFT;
        int rc;

        page = grab_cache_page(mapping, index); /* locked page */
        if (IS_ERR(page))
                return lnb->rc = PTR_ERR(page);

        lnb->page = page;

        if (inode->i_size < lnb->offset + lnb->len - 1)
                lnb->rc = inode->i_size - lnb->offset;
        else
                lnb->rc = lnb->len;

        if (PageUptodate(page)) {
                unlock_page(page);
                return 0;
        }

        rc = mapping->a_ops->readpage(NULL, page);
        if (rc < 0) {
                CERROR("page index %lu, rc = %d\n", index, rc);
                lnb->page = NULL;
                page_cache_release(page);
                return lnb->rc = rc;
        }

        return 0;
}

static int filter_finish_page_read(struct niobuf_local *lnb)
{
        if (lnb->page == NULL)
                return 0;

        if (PageUptodate(lnb->page))
                return 0;

        wait_on_page(lnb->page);
        if (!PageUptodate(lnb->page)) {
                CERROR("page index %lu/offset "LPX64" not uptodate\n",
                       lnb->page->index, lnb->offset);
                GOTO(err_page, lnb->rc = -EIO);
        }
        if (PageError(lnb->page)) {
                CERROR("page index %lu/offset "LPX64" has error\n",
                       lnb->page->index, lnb->offset);
                GOTO(err_page, lnb->rc = -EIO);
        }

        return 0;

err_page:
        page_cache_release(lnb->page);
        lnb->page = NULL;
        return lnb->rc;
}

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
int wait_on_page_locked(struct page *page)
{
        waitfor_one_page(page);
        return 0;
}

/* We should only change the file mtime (and not the ctime, like
 * update_inode_times() in generic_file_write()) when we only change data. */
static inline void inode_update_time(struct inode *inode, int ctime_too)
{
        time_t now = CURRENT_TIME;
        if (inode->i_mtime == now && (!ctime_too || inode->i_ctime == now))
                return;
        inode->i_mtime = now;
        if (ctime_too)
                inode->i_ctime = now;
        mark_inode_dirty_sync(inode);
}
#endif

static int filter_preprw_read(struct obd_export *exp, struct obdo *obdo,
                              int objcount, struct obd_ioobj *obj,
                              int niocount, struct niobuf_remote *nb,
                              struct niobuf_local *res,
                              struct obd_trans_info *oti)
{
        struct obd_run_ctxt saved;
        struct obd_device *obd;
        struct obd_ioobj *o;
        struct niobuf_remote *rnb;
        struct niobuf_local *lnb = NULL;
        struct fsfilt_objinfo *fso;
        struct dentry *dentry;
        struct inode *inode;
        int rc = 0, i, j, tot_bytes = 0, cleanup_phase = 0;
        unsigned long now = jiffies;
        ENTRY;
        LASSERT(objcount == 1);

        obd = exp->exp_obd;
        if (obd == NULL)
                RETURN(-EINVAL);
        OBD_ALLOC(fso, objcount * sizeof(*fso));
        if (fso == NULL)
                RETURN(-ENOMEM);

        memset(res, 0, niocount * sizeof(*res));

        push_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
        for (i = 0, o = obj; i < objcount; i++, o++) {
                struct filter_dentry_data *fdd;
                LASSERT(o->ioo_bufcnt);

                dentry = filter_fid2dentry(obd, NULL, o->ioo_type, o->ioo_id);
                if (IS_ERR(dentry))
                        GOTO(cleanup, rc = PTR_ERR(dentry));

                if (dentry->d_inode == NULL) {
                        CERROR("trying to BRW to non-existent file "LPU64"\n",
                               o->ioo_id);
                        f_dput(dentry);
                        GOTO(cleanup, rc = -ENOENT);
                }

                fso[i].fso_dentry = dentry;
                fso[i].fso_bufcnt = o->ioo_bufcnt;

                fdd = dentry->d_fsdata;
                if (fdd == NULL || !atomic_read(&fdd->fdd_open_count))
                        CDEBUG(D_PAGE, "I/O to unopened object "LPU64"\n",
                               o->ioo_id);
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow preprw_read setup %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "preprw_read setup: %lu jiffies\n",
                       (jiffies - now));

        for (i = 0, o = obj, rnb = nb, lnb = res; i < objcount; i++, o++) {
                dentry = fso[i].fso_dentry;
                inode = dentry->d_inode;

                for (j = 0; j < o->ioo_bufcnt; j++, rnb++, lnb++) {
                        lnb->dentry = dget(dentry);
                        lnb->offset = rnb->offset;
                        lnb->len    = rnb->len;
                        lnb->flags  = rnb->flags;
                        lnb->start  = jiffies;

                        if (inode->i_size <= rnb->offset) {
                                /* If there's no more data, abort early.
                                 * lnb->page == NULL and lnb->rc == 0, so it's
                                 * easy to detect later. */
                                f_dput(dentry);
                                lnb->dentry = NULL;
                                break;
                        } else {
                                rc = filter_start_page_read(inode, lnb);
                        }

                        if (rc) {
                                CDEBUG(rc == -ENOSPC ? D_INODE : D_ERROR,
                                       "page err %u@"LPU64" %u/%u %p: rc %d\n",
                                       lnb->len, lnb->offset, j, o->ioo_bufcnt,
                                       dentry, rc);
                                f_dput(dentry);
                                cleanup_phase = 1;
                                GOTO(cleanup, rc);
                        }

                        tot_bytes += lnb->rc;
                        if (lnb->rc < lnb->len)
                                break; /* short read */
                }
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow start_page_read %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "start_page_read: %lu jiffies\n",
                       (jiffies - now));

        lprocfs_counter_add(obd->obd_stats, LPROC_FILTER_READ_BYTES, tot_bytes);
        while (lnb-- > res) {
                rc = filter_finish_page_read(lnb);
                if (rc) {
                        CERROR("error page %u@"LPU64" %u %p: rc %d\n", lnb->len,
                               lnb->offset, (int)(lnb - res), lnb->dentry, rc);
                        f_dput(lnb->dentry);
                        cleanup_phase = 1;
                        GOTO(cleanup, rc);
                }
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow finish_page_read %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "finish_page_read: %lu jiffies\n",
                       (jiffies - now));

        EXIT;
 cleanup:
        switch (cleanup_phase) {
        case 1:
                while (lnb-- > res) {
                        page_cache_release(lnb->page);
                        f_dput(lnb->dentry);
                }
        case 0:
                for (i = 0; i < objcount; i++) {
                        if (fso[i].fso_dentry == NULL)
                                break;
                        f_dput(fso[i].fso_dentry);
                }
                OBD_FREE(fso, objcount * sizeof(*fso));
                pop_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
        }
        return rc;
}

static int internal_start_write(struct page *page, struct niobuf_local *lnb)
{
        unsigned long index = lnb->offset >> PAGE_SHIFT;
        struct address_space *mapping = page->mapping;
        int rc;

        /* Note: Called with "O" and "PAGE_SIZE" this is essentially a no-op for
         * most filesystems, because we write the whole page.  For partial-page
         * I/O this will read in the page. */
        rc = mapping->a_ops->prepare_write(NULL, page, lnb->offset & ~PAGE_MASK,
                                           lnb->len);
        if (rc) {
                CERROR("page index %lu, rc = %d\n", index, rc);
                if (rc != -ENOSPC)
                        LBUG();
                GOTO(err_unlock, rc);
        }
        /* XXX not sure if we need this if we are overwriting page */
        if (PageError(page)) {
                CERROR("error on page index %lu, rc = %d\n", index, rc);
                LBUG();
                GOTO(err_unlock, rc = -EIO);
        }
        return rc;

err_unlock:
        unlock_page(page);
        page_cache_release(page);
        return rc;
}

static int filter_start_page_write(struct inode *inode,
                                   struct niobuf_local *lnb, int *pglocked)
{
        unsigned long index = lnb->offset >> PAGE_SHIFT;
        struct address_space *mapping = inode->i_mapping;
        struct page *page;
        int rc;
        ENTRY;

        //ASSERT_PAGE_INDEX(index, GOTO(err, rc = -EINVAL));
        if (*pglocked)
                page = grab_cache_page_nowait(mapping, index); /* locked page */
        else
                page = grab_cache_page(mapping, index); /* locked page */

        /* This page is currently locked, so get a temporary page instead. */
        if (page == NULL) {
                CDEBUG(D_ERROR,"ino %lu page %ld locked\n", inode->i_ino,index);
                page = alloc_pages(GFP_KERNEL, 0); /* locked page */
                if (page == NULL) {
                        CERROR("no memory for a temp page\n");
                        RETURN(lnb->rc = -ENOMEM);
                }
                page->index = index;
                lnb->page = page;
                lnb->flags |= N_LOCAL_TEMP_PAGE;
        } else if (!IS_ERR(page)) {
                (*pglocked)++;

                rc = internal_start_write(page, lnb);
                if (rc) {
                        if (rc != -ENOSPC)
                                CERROR("page index %lu, rc = %d\n", index, rc);
                        RETURN(lnb->rc = rc);
                }
                lnb->page = page;
        }
        RETURN(0);
}

/* We need to balance prepare_write() calls with commit_write() calls.
 * If the page has been prepared, but we have no data for it, we don't
 * want to overwrite valid data on disk, but we still need to zero out
 * data for space which was newly allocated.  Like part of what happens
 * in __block_prepare_write() for newly allocated blocks.
 *
 * XXX currently __block_prepare_write() creates buffers for all the
 *     pages, and the filesystems mark these buffers as BH_New if they
 *     were newly allocated from disk. We use the BH_New flag similarly. */
static int filter_commit_write(struct niobuf_local *lnb, int err)
{
        struct page *page = lnb->page;
        unsigned from = lnb->offset & ~PAGE_MASK;
        unsigned to = from + lnb->len;
        struct inode *inode = page->mapping->host;
        int rc;

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,5,0))
        if (err) {
                unsigned block_start, block_end;
                struct buffer_head *bh, *head = lnb->page->buffers;
                unsigned blocksize = head->b_size;

                /* debugging: just seeing if this ever happens */
                CDEBUG(err == -ENOSPC ? D_INODE : D_ERROR,
                       "called for ino %lu:%lu on err %d\n",
                       lnb->page->mapping->host->i_ino, lnb->page->index, err);

                /* Currently one buffer per page, but in the future... */
                for (bh = head, block_start = 0; bh != head || !block_start;
                     block_start = block_end, bh = bh->b_this_page) {
                        block_end = block_start + blocksize;
                        if (buffer_new(bh)) {
                                memset(kmap(lnb->page) + block_start, 0,
                                       blocksize);
                                kunmap(lnb->page);
                        }
                }
        }
#endif

        LASSERT(to <= PAGE_SIZE);
        rc = page->mapping->a_ops->commit_write(NULL, page, from, to);
        if (!rc && IS_SYNC(inode))
                rc = wait_on_page_locked(page);
        //SetPageUptodate(page); // the client commit_write will do this

        SetPageReferenced(page);
        return rc;
}

/* It is highly unlikely that we would ever get an error here.  The page we want
 * to get was previously locked, so it had to have already allocated the space,
 * and we were just writing over the same data, so there would be no hole in the
 * file.
 *
 * XXX: possibility of a race with truncate could exist, need to check that.
 *      There are no guarantees w.r.t. write order even on a local filesystem,
 *      although the normal response would be to return the number of bytes
 *      successfully written and leave the rest to the app. */
static int filter_write_locked_page(struct niobuf_local *lnb)
{
        struct address_space *mapping = lnb->dentry->d_inode->i_mapping;
        struct page *lpage;
        void *lpage_addr, *lnb_addr;
        int rc;
        ENTRY;

        lpage = grab_cache_page(mapping, lnb->page->index); /* locked page */
        if (IS_ERR(lpage)) {
                rc = PTR_ERR(lpage);
                CERROR("error getting locked page index %ld: rc = %d\n",
                       lnb->page->index, rc);
                LBUG();
                //filter_commit_write(lnb, 0);
                RETURN(rc);
        }

        rc = internal_start_write(lpage, lnb);
        if (rc) {
                CERROR("error getting locked page index %ld: rc = %d\n",
                       lnb->page->index, rc);
                LBUG();
                //filter_commit_write(lnb, 0);
                RETURN(rc);
        }

        /* 2 kmaps == vanishingly small deadlock opportunity */
        lpage_addr = kmap(lpage);
        lnb_addr = kmap(lnb->page);

        memcpy(lpage_addr, lnb_addr, PAGE_SIZE);

        kunmap(lnb->page);
        kunmap(lpage);

        page_cache_release(lnb->page);

        lnb->page = lpage;
        rc = filter_commit_write(lnb, 0);
        if (rc)
                CERROR("error committing locked page %ld: rc = %d\n",
                       lnb->page->index, rc);
        RETURN(rc);
}

static int filter_commit_page(int cmd, struct niobuf_local *lnb)
{
        int rc;
        if (cmd & OBD_BRW_READ) {
                page_cache_release(lnb->page);
                return 0;
        }

        if (lnb->flags & N_LOCAL_TEMP_PAGE)
                rc = filter_write_locked_page(lnb);
        else
                rc = filter_commit_write(lnb, 0);
        unlock_page(lnb->page);
        page_cache_release(lnb->page);
        return rc;
}

/* If we ever start to support multi-object BRW RPCs, we will need to get locks
 * on mulitple inodes.  That isn't all, because there still exists the
 * possibility of a truncate starting a new transaction while holding the ext3
 * rwsem = write while some writes (which have started their transactions here)
 * blocking on the ext3 rwsem = read => lock inversion.
 *
 * The handling gets very ugly when dealing with locked pages.  It may be easier
 * to just get rid of the locked page code (which has problems of its own) and
 * either discover we do not need it anymore (i.e. it was a symptom of another
 * bug) or ensure we get the page locks in an appropriate order. */
static int filter_preprw_write(struct obd_export *exp, struct obdo *obdo,
                               int objcount, struct obd_ioobj *obj,
                               int niocount, struct niobuf_remote *nb,
                               struct niobuf_local *res,
                               struct obd_trans_info *oti)
{
        struct obd_run_ctxt saved;
        struct obd_device *obd;
        struct obd_ioobj *o;
        struct niobuf_remote *rnb;
        struct niobuf_local *lnb = NULL;
        struct fsfilt_objinfo *fso;
        struct dentry *dentry;
        int pglocked = 0, rc = 0, i, j, tot_bytes = 0, cleanup_phase = 0;
        int nested_trans = current->journal_info != NULL;
        unsigned long now = jiffies;
        ENTRY;
        LASSERT(objcount == 1);

        obd = exp->exp_obd;
        if (obd == NULL)
                RETURN(-EINVAL);
        OBD_ALLOC(fso, objcount * sizeof(*fso));
        if (fso == NULL)
                RETURN(-ENOMEM);

        memset(res, 0, niocount * sizeof(*res));

        push_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
        for (i = 0, o = obj; i < objcount; i++, o++) {
                struct filter_dentry_data *fdd;
                LASSERT(o->ioo_bufcnt);

                dentry = filter_fid2dentry(obd, NULL, o->ioo_type, o->ioo_id);
                if (IS_ERR(dentry))
                        GOTO(cleanup, rc = PTR_ERR(dentry));

                if (dentry->d_inode == NULL) {
                        CERROR("trying to BRW to non-existent file "LPU64"\n",
                               o->ioo_id);
                        f_dput(dentry);
                        GOTO(cleanup, rc = -ENOENT);
                }

                fso[i].fso_dentry = dentry;
                fso[i].fso_bufcnt = o->ioo_bufcnt;

                down(&dentry->d_inode->i_sem);
                fdd = dentry->d_fsdata;
                if (fdd == NULL || !atomic_read(&fdd->fdd_open_count))
                        CDEBUG(D_PAGE, "I/O to unopened object "LPU64"\n",
                               o->ioo_id);
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow preprw_write setup %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "preprw_write setup: %lu jiffies\n",
                       (jiffies - now));

        LASSERT(oti != NULL);
        oti->oti_handle = fsfilt_brw_start(obd, objcount, fso, niocount, oti);
        if (IS_ERR(oti->oti_handle)) {
                rc = PTR_ERR(oti->oti_handle);
                CDEBUG(rc == -ENOSPC ? D_INODE : D_ERROR,
                       "error starting transaction: rc = %d\n", rc);
                oti->oti_handle = NULL;
                GOTO(cleanup, rc);
        }

        for (i = 0, o = obj, rnb = nb, lnb = res; i < objcount; i++, o++) {
                dentry = fso[i].fso_dentry;
                for (j = 0; j < o->ioo_bufcnt; j++, rnb++, lnb++) {
                        lnb->dentry = dget(dentry);
                        lnb->offset = rnb->offset;
                        lnb->len    = rnb->len;
                        lnb->flags  = rnb->flags;
                        lnb->start  = jiffies;

                        rc = filter_start_page_write(dentry->d_inode, lnb,
                                                     &pglocked);
                        if (rc) {
                                CDEBUG(rc == -ENOSPC ? D_INODE : D_ERROR,
                                       "page err %u@"LPU64" %u/%u %p: rc %d\n",
                                       lnb->len, lnb->offset, j, o->ioo_bufcnt,
                                       dentry, rc);
                                f_dput(fso[i].fso_dentry);
                                cleanup_phase = 2;
                                GOTO(cleanup, rc);
                        }
                }
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow start_page_write %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "start_page_write: %lu jiffies\n",
                       (jiffies - now));

        for (i = 0, o = obj, rnb = nb, lnb = res; i < objcount; i++, o++) {
                for (j = 0; j < o->ioo_bufcnt; j++, rnb++, lnb++) {
                        fsfilt_commit(obd, NULL, oti->oti_handle, 0);
                        tot_bytes += lnb->len;
                }
        }

        if (time_after(jiffies, now + 15 * HZ))
                CERROR("slow prep write commit %lus\n", (jiffies - now) / HZ);
        else
                CDEBUG(D_INFO, "fsfilt_commit setup: %lu jiffies\n",
                       (jiffies - now));

        lprocfs_counter_add(obd->obd_stats, LPROC_FILTER_WRITE_BYTES,tot_bytes);

        cleanup_phase = 1;
        EXIT;
 cleanup:
        switch (cleanup_phase) {
        case 2:
                while (lnb-- > res) {
                        filter_commit_page(OBD_BRW_WRITE, lnb);
                        f_dput(lnb->dentry);
                }
        case 1:
                fsfilt_commit(obd, NULL, oti->oti_handle, 0);
                oti->oti_handle = NULL;
        case 0:
                for (i = 0; i < objcount; i++) {
                        if (fso[i].fso_dentry == NULL)
                                break;
                        up(&fso[i].fso_dentry->d_inode->i_sem);
                        f_dput(fso[i].fso_dentry);
                }
                OBD_FREE(fso, objcount * sizeof(*fso));
                pop_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
                LASSERT(nested_trans || current->journal_info == NULL);
        }
        return rc;
}

int filter_preprw(int cmd, struct obd_export *exp, struct obdo *obdo,
                  int objcount, struct obd_ioobj *obj, int niocount,
                  struct niobuf_remote *nb, struct niobuf_local *res,
                  struct obd_trans_info *oti)
{
        if (cmd == OBD_BRW_WRITE)
                return filter_preprw_write(exp, obdo, objcount, obj, niocount,
                                           nb, res, oti);
        else if (cmd == OBD_BRW_READ)
                return filter_preprw_read(exp, obdo, objcount, obj, niocount,
                                          nb, res, oti);
        else
                LBUG();
        return 0;
}

int filter_commitrw(int cmd, struct obd_export *exp, int objcount,
                    struct obd_ioobj *obj, int niocount,
                    struct niobuf_local *res, struct obd_trans_info *oti)
{
        struct obd_run_ctxt saved;
        struct obd_ioobj *o;
        struct niobuf_local *lnb;
        struct obd_device *obd = exp->exp_obd;
        struct fsfilt_objinfo *fso = NULL;
        int rc = 0, i, j, nested_trans = current->journal_info != NULL;
        int cleanup_phase = 0;
        unsigned long now = jiffies;  /* DEBUGGING OST TIMEOUTS */
        ENTRY;

        push_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
        if (cmd & OBD_BRW_READ)
                goto shared;

        LASSERT(oti != NULL);
        LASSERT(current->journal_info == NULL ||
                current->journal_info == oti->oti_handle);
        current->journal_info = oti->oti_handle;
        OBD_ALLOC(fso, objcount * sizeof(*fso));
        if (fso == NULL)
                GOTO(cleanup, rc = -ENOMEM);

        cleanup_phase = 1;
        for (i = 0, o = obj, lnb = res; i < objcount; i++, o++) {
                fso[i].fso_dentry = lnb->dentry;
                fso[i].fso_bufcnt = o->ioo_bufcnt;
                down(&lnb->dentry->d_inode->i_sem);
        }

        oti->oti_handle = fsfilt_brw_start(obd, objcount, fso, niocount, oti);
        if (IS_ERR(oti->oti_handle)) {
                rc = PTR_ERR(oti->oti_handle);
                CDEBUG(rc == -ENOSPC ? D_INODE : D_ERROR,
                       "error starting transaction: rc = %d\n", rc);
                oti->oti_handle = NULL;
                cleanup_phase = 2;
                GOTO(cleanup, rc);
        }

        for (i = 0, o = obj, lnb = res; i < objcount; i++, o++)
                for (j = 0; j < o->ioo_bufcnt; j++, lnb++)
                        fsfilt_brw_start(obd, objcount, fso, niocount, oti);

 shared:
        for (i = 0, o = obj, lnb = res; i < objcount; i++, o++) {
                if (cmd & OBD_BRW_WRITE) {
                        inode_update_time(lnb->dentry->d_inode, 1);
                        up(&lnb->dentry->d_inode->i_sem);
                }
                for (j = 0 ; j < o->ioo_bufcnt ; j++, lnb++) {
                        if (lnb->page == NULL)
                                continue;

                        if (time_after(jiffies, lnb->start + 15 * HZ))
                                CERROR("slow commitrw %lus\n",
                                       (jiffies - lnb->start) / HZ);

                        filter_commit_page(cmd, lnb);

                        if (time_after(jiffies, lnb->start + 15 * HZ))
                                CERROR("slow commit_write %lus\n",
                                       (jiffies - lnb->start) / HZ);
                }
        }

        if (cmd & OBD_BRW_WRITE) {
                /* We just want any dentry for the commit, for now */
                struct dentry *dparent = filter_parent(obd, S_IFREG, 0);
                int err;

                rc = filter_finish_transno(exp, oti, rc);
                err = fsfilt_commit(obd, dparent->d_inode, oti->oti_handle,
                                    obd_sync_filter);
                if (err)
                        rc = err;
                if (obd_sync_filter)
                        LASSERT(oti->oti_transno <= obd->obd_last_committed);
                if (time_after(jiffies, now + 15 * HZ))
                        CERROR("slow commitrw commit %lus\n", (jiffies-now)/HZ);
        }

 cleanup:
        switch (cleanup_phase) {
        case 2:
                for (i = 0, lnb = res; i < objcount; i++)
                        up(&lnb->dentry->d_inode->i_sem);
        case 1:
                if (fso != NULL)
                        OBD_FREE(fso, objcount * sizeof(*fso));
        case 0:
                for (i = 0, o = obj, lnb = res; i < objcount; i++, o++)
                        for (j = 0 ; j < o->ioo_bufcnt ; j++, lnb++)
                                f_dput(lnb->dentry);
        }
        LASSERT(nested_trans || current->journal_info == NULL);
        pop_ctxt(&saved, &obd->u.filter.fo_ctxt, NULL);
        RETURN(rc);
}

int filter_brw(int cmd, struct lustre_handle *conn, struct lov_stripe_md *lsm,
               obd_count oa_bufs, struct brw_page *pga,
               struct obd_trans_info *oti)
{
        struct obd_export *exp = class_conn2export(conn);
        struct obd_ioobj ioo;
        struct niobuf_local *lnb;
        struct niobuf_remote *rnb;
        obd_count i;
        int ret = 0;
        ENTRY;

        if (exp == NULL)
                RETURN(-EINVAL);

        OBD_ALLOC(lnb, oa_bufs * sizeof(struct niobuf_local));
        OBD_ALLOC(rnb, oa_bufs * sizeof(struct niobuf_remote));

        if (lnb == NULL || rnb == NULL)
                GOTO(out, ret = -ENOMEM);

        for (i = 0; i < oa_bufs; i++) {
                rnb[i].offset = pga[i].off;
                rnb[i].len = pga[i].count;
        }

        ioo.ioo_id = lsm->lsm_object_id;
        ioo.ioo_gr = 0;
        ioo.ioo_type = S_IFREG;
        ioo.ioo_bufcnt = oa_bufs;

        ret = filter_preprw(cmd, exp, NULL, 1, &ioo, oa_bufs, rnb, lnb, oti);
        if (ret != 0)
                GOTO(out, ret);

        for (i = 0; i < oa_bufs; i++) {
                void *virt = kmap(pga[i].pg);
                obd_off off = pga[i].off & ~PAGE_MASK;
                void *addr = kmap(lnb[i].page);

                /* 2 kmaps == vanishingly small deadlock opportunity */

                if (cmd & OBD_BRW_WRITE)
                        memcpy(addr + off, virt + off, pga[i].count);
                else
                        memcpy(virt + off, addr + off, pga[i].count);

                kunmap(addr);
                kunmap(virt);
        }

        ret = filter_commitrw(cmd, exp, 1, &ioo, oa_bufs, lnb, oti);

out:
        if (lnb)
                OBD_FREE(lnb, oa_bufs * sizeof(struct niobuf_local));
        if (rnb)
                OBD_FREE(rnb, oa_bufs * sizeof(struct niobuf_remote));
        class_export_put(exp);
        RETURN(ret);
}
