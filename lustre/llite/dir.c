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
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/llite/dir.c
 *
 * Directory code for lustre client.
 */

#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/mm.h>
#include <linux/version.h>
#include <asm/uaccess.h>
#include <linux/buffer_head.h>   // for wait_on_buffer
#include <linux/pagevec.h>

#define DEBUG_SUBSYSTEM S_LLITE

#include <lustre/lustre_idl.h>
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_lib.h>
#include <lustre/lustre_idl.h>
#include <lustre_lite.h>
#include <lustre_dlm.h>
#include <lustre_fid.h>
#include "llite_internal.h"

/*
 * (new) readdir implementation overview.
 *
 * Original lustre readdir implementation cached exact copy of raw directory
 * pages on the client. These pages were indexed in client page cache by
 * logical offset in the directory file. This design, while very simple and
 * intuitive had some inherent problems:
 *
 *     . it implies that byte offset to the directory entry serves as a
 *     telldir(3)/seekdir(3) cookie, but that offset is not stable: in
 *     ext3/htree directory entries may move due to splits, and more
 *     importantly,
 *
 *     . it is incompatible with the design of split directories for cmd3,
 *     that assumes that names are distributed across nodes based on their
 *     hash, and so readdir should be done in hash order.
 *
 * New readdir implementation does readdir in hash order, and uses hash of a
 * file name as a telldir/seekdir cookie. This led to number of complications:
 *
 *     . hash is not unique, so it cannot be used to index cached directory
 *     pages on the client (note, that it requires a whole pageful of hash
 *     collided entries to cause two pages to have identical hashes);
 *
 *     . hash is not unique, so it cannot, strictly speaking, be used as an
 *     entry cookie. ext3/htree has the same problem and lustre implementation
 *     mimics their solution: seekdir(hash) positions directory at the first
 *     entry with the given hash.
 *
 * Client side.
 *
 * 0. caching
 *
 * Client caches directory pages using hash of the first entry as an index. As
 * noted above hash is not unique, so this solution doesn't work as is:
 * special processing is needed for "page hash chains" (i.e., sequences of
 * pages filled with entries all having the same hash value).
 *
 * First, such chains have to be detected. To this end, server returns to the
 * client the hash of the first entry on the page next to one returned. When
 * client detects that this hash is the same as hash of the first entry on the
 * returned page, page hash collision has to be handled. Pages in the
 * hash chain, except first one, are termed "overflow pages".
 *
 * Solution to index uniqueness problem is to not cache overflow
 * pages. Instead, when page hash collision is detected, all overflow pages
 * from emerging chain are immediately requested from the server and placed in
 * a special data structure (struct ll_dir_chain). This data structure is used
 * by ll_readdir() to process entries from overflow pages. When readdir
 * invocation finishes, overflow pages are discarded. If page hash collision
 * chain weren't completely processed, next call to readdir will again detect
 * page hash collision, again read overflow pages in, process next portion of
 * entries and again discard the pages. This is not as wasteful as it looks,
 * because, given reasonable hash, page hash collisions are extremely rare.
 *
 * 1. directory positioning
 *
 * When seekdir(hash) is called, original
 *
 *
 *
 *
 *
 *
 *
 *
 * Server.
 *
 * identification of and access to overflow pages
 *
 * page format
 *
 * Page in MDS_READPAGE RPC is packed in LU_PAGE_SIZE, and each page contains
 * a header lu_dirpage which describes the start/end hash, and whether this
 * page is empty (contains no dir entry) or hash collide with next page.
 * After client receives reply, several pages will be integrated into dir page
 * in CFS_PAGE_SIZE (if CFS_PAGE_SIZE greater than LU_PAGE_SIZE), and the
 * lu_dirpage for this integrated page will be adjusted.
 *
 */

/* returns the page unlocked, but with a reference */
static int ll_dir_filler(void *_hash, struct page *page0)
{
        struct inode *inode = page0->mapping->host;
        int hash64 = ll_i2sbi(inode)->ll_flags & LL_SBI_64BIT_HASH;
        struct obd_export *exp = ll_i2sbi(inode)->ll_md_exp;
        struct ptlrpc_request *request;
        struct mdt_body *body;
        struct md_op_data *op_data;
	__u64 hash = *((__u64 *)_hash);
        struct page **page_pool;
        struct page *page;
#ifndef HAVE_ADD_TO_PAGE_CACHE_LRU
        struct pagevec lru_pvec;
#endif
        struct lu_dirpage *dp;
        int max_pages = ll_i2sbi(inode)->ll_md_brw_size >> CFS_PAGE_SHIFT;
        int nrdpgs = 0; /* number of pages read actually */
        int npages;
        int i;
        int rc;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p) hash "LPU64"\n",
               inode->i_ino, inode->i_generation, inode, hash);

        LASSERT(max_pages > 0 && max_pages <= PTLRPC_MAX_BRW_PAGES);

        OBD_ALLOC(page_pool, sizeof(page) * max_pages);
        if (page_pool != NULL) {
                page_pool[0] = page0;
        } else {
                page_pool = &page0;
                max_pages = 1;
        }
        for (npages = 1; npages < max_pages; npages++) {
                page = page_cache_alloc_cold(inode->i_mapping);
                if (!page)
                        break;
                page_pool[npages] = page;
        }

        op_data = ll_prep_md_op_data(NULL, inode, NULL, NULL, 0, 0,
                                     LUSTRE_OPC_ANY, NULL);
        op_data->op_npages = npages;
        op_data->op_offset = hash;
        rc = md_readpage(exp, op_data, page_pool, &request);
        ll_finish_md_op_data(op_data);
        if (rc == 0) {
                body = req_capsule_server_get(&request->rq_pill, &RMF_MDT_BODY);
                /* Checked by mdc_readpage() */
                LASSERT(body != NULL);

                if (body->valid & OBD_MD_FLSIZE)
                        cl_isize_write(inode, body->size);

                nrdpgs = (request->rq_bulk->bd_nob_transferred+CFS_PAGE_SIZE-1)
                         >> CFS_PAGE_SHIFT;
                SetPageUptodate(page0);
        }
        unlock_page(page0);
        ptlrpc_req_finished(request);

        CDEBUG(D_VFSTRACE, "read %d/%d pages\n", nrdpgs, npages);

        ll_pagevec_init(&lru_pvec, 0);
        for (i = 1; i < npages; i++) {
                unsigned long offset;
                int ret;

                page = page_pool[i];

                if (rc < 0 || i >= nrdpgs) {
                        page_cache_release(page);
                        continue;
                }

                SetPageUptodate(page);

                dp = cfs_kmap(page);
                hash = le64_to_cpu(dp->ldp_hash_start);
                cfs_kunmap(page);

                offset = hash_x_index(hash, hash64);

                prefetchw(&page->flags);
                ret = ll_add_to_page_cache_lru(page, inode->i_mapping, offset,
                                               GFP_KERNEL);
                if (ret == 0) {
                        unlock_page(page);
                        if (ll_pagevec_add(&lru_pvec, page) == 0)
                                ll_pagevec_lru_add_file(&lru_pvec);
                } else {
                        CDEBUG(D_VFSTRACE, "page %lu add to page cache failed:"
                               " %d\n", offset, ret);
                }
                page_cache_release(page);
        }
        ll_pagevec_lru_add_file(&lru_pvec);

        if (page_pool != &page0)
                OBD_FREE(page_pool, sizeof(struct page *) * max_pages);
        EXIT;
        return rc;
}

static void ll_check_page(struct inode *dir, struct page *page)
{
        /* XXX: check page format later */
        SetPageChecked(page);
}

void ll_release_page(struct page *page, int remove)
{
        kunmap(page);
        if (remove) {
                lock_page(page);
                if (likely(page->mapping != NULL))
                        truncate_complete_page(page->mapping, page);
                unlock_page(page);
        }
        page_cache_release(page);
}

/*
 * Find, kmap and return page that contains given hash.
 */
static struct page *ll_dir_page_locate(struct inode *dir, __u64 *hash,
                                       __u64 *start, __u64 *end)
{
        int hash64 = ll_i2sbi(dir)->ll_flags & LL_SBI_64BIT_HASH;
        struct address_space *mapping = dir->i_mapping;
        /*
         * Complement of hash is used as an index so that
         * radix_tree_gang_lookup() can be used to find a page with starting
         * hash _smaller_ than one we are looking for.
         */
        unsigned long offset = hash_x_index(*hash, hash64);
        struct page *page;
        int found;

        TREE_READ_LOCK_IRQ(mapping);
        found = radix_tree_gang_lookup(&mapping->page_tree,
                                       (void **)&page, offset, 1);
        if (found > 0) {
                struct lu_dirpage *dp;

                page_cache_get(page);
                TREE_READ_UNLOCK_IRQ(mapping);
                /*
                 * In contrast to find_lock_page() we are sure that directory
                 * page cannot be truncated (while DLM lock is held) and,
                 * hence, can avoid restart.
                 *
                 * In fact, page cannot be locked here at all, because
		 * ll_dir_filler() does synchronous io.
                 */
                wait_on_page(page);
                if (PageUptodate(page)) {
                        dp = cfs_kmap(page);
                        if (BITS_PER_LONG == 32 && hash64) {
                                *start = le64_to_cpu(dp->ldp_hash_start) >> 32;
                                *end   = le64_to_cpu(dp->ldp_hash_end) >> 32;
                                *hash  = *hash >> 32;
                        } else {
                                *start = le64_to_cpu(dp->ldp_hash_start);
                                *end   = le64_to_cpu(dp->ldp_hash_end);
                        }
                        LASSERTF(*start <= *hash, "start = "LPX64",end = "
                                 LPX64",hash = "LPX64"\n", *start, *end, *hash);
                        CDEBUG(D_VFSTRACE, "page %lu [%llu %llu], hash "LPU64"\n",
                               offset, *start, *end, *hash);
                        if (*hash > *end) {
                                ll_release_page(page, 0);
                                page = NULL;
                        } else if (*end != *start && *hash == *end) {
                                /*
                                 * upon hash collision, remove this page,
                                 * otherwise put page reference, and
                                 * ll_get_dir_page() will issue RPC to fetch
                                 * the page we want.
                                 */
                                ll_release_page(page,
                                    le32_to_cpu(dp->ldp_flags) & LDF_COLLIDE);
                                page = NULL;
                        }
                } else {
                        page_cache_release(page);
                        page = ERR_PTR(-EIO);
                }

        } else {
                TREE_READ_UNLOCK_IRQ(mapping);
                page = NULL;
        }
        return page;
}

struct page *ll_get_dir_page(struct inode *dir, __u64 hash,
                             struct ll_dir_chain *chain)
{
        ldlm_policy_data_t policy = {.l_inodebits = {MDS_INODELOCK_UPDATE} };
        struct address_space *mapping = dir->i_mapping;
        struct lustre_handle lockh;
        struct lu_dirpage *dp;
        struct page *page;
        ldlm_mode_t mode;
        int rc;
        __u64 start = 0;
        __u64 end = 0;
        __u64 lhash = hash;
        struct ll_inode_info *lli = ll_i2info(dir);
        int hash64 = ll_i2sbi(dir)->ll_flags & LL_SBI_64BIT_HASH;

        mode = LCK_PR;
        rc = md_lock_match(ll_i2sbi(dir)->ll_md_exp, LDLM_FL_BLOCK_GRANTED,
                           ll_inode2fid(dir), LDLM_IBITS, &policy, mode, &lockh);
        if (!rc) {
                struct ldlm_enqueue_info einfo = { LDLM_IBITS, mode,
                       ll_md_blocking_ast, ldlm_completion_ast,
                       NULL, NULL, dir };
                struct lookup_intent it = { .it_op = IT_READDIR };
                struct ptlrpc_request *request;
                struct md_op_data *op_data;

                op_data = ll_prep_md_op_data(NULL, dir, NULL, NULL, 0, 0,
                                             LUSTRE_OPC_ANY, NULL);
                if (IS_ERR(op_data))
                        return (void *)op_data;

                rc = md_enqueue(ll_i2sbi(dir)->ll_md_exp, &einfo, &it,
                                op_data, &lockh, NULL, 0, NULL, 0);

                ll_finish_md_op_data(op_data);

                request = (struct ptlrpc_request *)it.d.lustre.it_data;
                if (request)
                        ptlrpc_req_finished(request);
                if (rc < 0) {
                        CERROR("lock enqueue: "DFID" at "LPU64": rc %d\n",
                               PFID(ll_inode2fid(dir)), hash, rc);
                        return ERR_PTR(rc);
                }
        } else {
                /* for cross-ref object, l_ast_data of the lock may not be set,
                 * we reset it here */
                md_set_lock_data(ll_i2sbi(dir)->ll_md_exp, &lockh.cookie,
                                 dir, NULL);
        }
        ldlm_lock_dump_handle(D_OTHER, &lockh);

        cfs_mutex_lock(&lli->lli_readdir_mutex);
        page = ll_dir_page_locate(dir, &lhash, &start, &end);
        if (IS_ERR(page)) {
                CERROR("dir page locate: "DFID" at "LPU64": rc %ld\n",
                       PFID(ll_inode2fid(dir)), lhash, PTR_ERR(page));
                GOTO(out_unlock, page);
        } else if (page != NULL) {
                /*
                 * XXX nikita: not entirely correct handling of a corner case:
                 * suppose hash chain of entries with hash value HASH crosses
                 * border between pages P0 and P1. First both P0 and P1 are
                 * cached, seekdir() is called for some entry from the P0 part
                 * of the chain. Later P0 goes out of cache. telldir(HASH)
                 * happens and finds P1, as it starts with matching hash
                 * value. Remaining entries from P0 part of the chain are
                 * skipped. (Is that really a bug?)
                 *
                 * Possible solutions: 0. don't cache P1 is such case, handle
                 * it as an "overflow" page. 1. invalidate all pages at
                 * once. 2. use HASH|1 as an index for P1.
                 */
                GOTO(hash_collision, page);
        }

        page = read_cache_page(mapping, hash_x_index(hash, hash64),
			       ll_dir_filler, &lhash);
        if (IS_ERR(page)) {
                CERROR("read cache page: "DFID" at "LPU64": rc %ld\n",
                       PFID(ll_inode2fid(dir)), hash, PTR_ERR(page));
                GOTO(out_unlock, page);
        }

        wait_on_page(page);
        (void)kmap(page);
        if (!PageUptodate(page)) {
                CERROR("page not updated: "DFID" at "LPU64": rc %d\n",
                       PFID(ll_inode2fid(dir)), hash, -5);
                goto fail;
        }
        if (!PageChecked(page))
                ll_check_page(dir, page);
        if (PageError(page)) {
                CERROR("page error: "DFID" at "LPU64": rc %d\n",
                       PFID(ll_inode2fid(dir)), hash, -5);
                goto fail;
        }
hash_collision:
        dp = page_address(page);
        if (BITS_PER_LONG == 32 && hash64) {
                start = le64_to_cpu(dp->ldp_hash_start) >> 32;
                end   = le64_to_cpu(dp->ldp_hash_end) >> 32;
                lhash = hash >> 32;
        } else {
                start = le64_to_cpu(dp->ldp_hash_start);
                end   = le64_to_cpu(dp->ldp_hash_end);
                lhash = hash;
        }
        if (end == start) {
                LASSERT(start == lhash);
                CWARN("Page-wide hash collision: "LPU64"\n", end);
                if (BITS_PER_LONG == 32 && hash64)
                        CWARN("Real page-wide hash collision at ["LPU64" "LPU64
                              "] with hash "LPU64"\n",
                              le64_to_cpu(dp->ldp_hash_start),
                              le64_to_cpu(dp->ldp_hash_end), hash);
                /*
                 * Fetch whole overflow chain...
                 *
                 * XXX not yet.
                 */
                goto fail;
        }
out_unlock:
        cfs_mutex_unlock(&lli->lli_readdir_mutex);
        ldlm_lock_decref(&lockh, mode);
        return page;

fail:
        ll_release_page(page, 1);
        page = ERR_PTR(-EIO);
        goto out_unlock;
}

int ll_dir_read(struct inode *inode, __u64 *_pos, void *cookie,
		filldir_t filldir)
{
        struct ll_inode_info *info       = ll_i2info(inode);
        struct ll_sb_info    *sbi        = ll_i2sbi(inode);
	__u64                 pos        = *_pos;
        int                   api32      = ll_need_32bit_api(sbi);
        int                   hash64     = sbi->ll_flags & LL_SBI_64BIT_HASH;
        struct page          *page;
        struct ll_dir_chain   chain;
	int                   done = 0;
	int                   rc = 0;
        ENTRY;

        ll_dir_chain_init(&chain);

	page = ll_get_dir_page(inode, pos, &chain);

        while (rc == 0 && !done) {
                struct lu_dirpage *dp;
                struct lu_dirent  *ent;

                if (!IS_ERR(page)) {
                        /*
                         * If page is empty (end of directory is reached),
                         * use this value.
                         */
                        __u64 hash = MDS_DIR_END_OFF;
                        __u64 next;

                        dp = page_address(page);
                        for (ent = lu_dirent_start(dp); ent != NULL && !done;
                             ent = lu_dirent_next(ent)) {
                                __u16          type;
                                int            namelen;
                                struct lu_fid  fid;
                                __u64          lhash;
                                __u64          ino;

                                /*
                                 * XXX: implement correct swabbing here.
                                 */

                                hash = le64_to_cpu(ent->lde_hash);
                                if (hash < pos)
                                        /*
                                         * Skip until we find target hash
                                         * value.
                                         */
                                        continue;

                                namelen = le16_to_cpu(ent->lde_namelen);
                                if (namelen == 0)
                                        /*
                                         * Skip dummy record.
                                         */
                                        continue;

                                if (api32 && hash64)
                                        lhash = hash >> 32;
                                else
                                        lhash = hash;
                                fid_le_to_cpu(&fid, &ent->lde_fid);
                                ino = cl_fid_build_ino(&fid, api32);
                                type = ll_dirent_type_get(ent);
                                /* For 'll_nfs_get_name_filldir()', it will try
                                 * to access the 'ent' through its 'lde_name',
                                 * so the parameter 'name' for 'filldir()' must
                                 * be part of the 'ent'. */
                                done = filldir(cookie, ent->lde_name, namelen,
                                               lhash, ino, type);
                        }
                        next = le64_to_cpu(dp->ldp_hash_end);
                        if (!done) {
                                pos = next;
                                if (pos == MDS_DIR_END_OFF) {
                                        /*
                                         * End of directory reached.
                                         */
                                        done = 1;
                                        ll_release_page(page, 0);
                                } else if (1 /* chain is exhausted*/) {
                                        /*
                                         * Normal case: continue to the next
                                         * page.
                                         */
                                        ll_release_page(page,
                                            le32_to_cpu(dp->ldp_flags) &
                                                        LDF_COLLIDE);
					next = pos;
					page = ll_get_dir_page(inode, pos,
                                                               &chain);
                                } else {
                                        /*
                                         * go into overflow page.
                                         */
                                        LASSERT(le32_to_cpu(dp->ldp_flags) &
                                                LDF_COLLIDE);
                                        ll_release_page(page, 1);
                                }
                        } else {
                                pos = hash;
                                ll_release_page(page, 0);
                        }
                } else {
                        rc = PTR_ERR(page);
                        CERROR("error reading dir "DFID" at %lu: rc %d\n",
                               PFID(&info->lli_fid), (unsigned long)pos, rc);
                }
        }

	*_pos = pos;
	ll_dir_chain_fini(&chain);
	RETURN(rc);
}

static int ll_readdir(struct file *filp, void *cookie, filldir_t filldir)
{
	struct inode		*inode	= filp->f_dentry->d_inode;
	struct ll_file_data	*lfd	= LUSTRE_FPRIVATE(filp);
	struct ll_sb_info	*sbi	= ll_i2sbi(inode);
	__u64			pos	= lfd->lfd_pos;
	int			hash64	= sbi->ll_flags & LL_SBI_64BIT_HASH;
	int			api32	= ll_need_32bit_api(sbi);
	int			rc;
	ENTRY;

	CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p) pos %lu/%llu "
	       " 32bit_api %d\n", inode->i_ino, inode->i_generation,
	       inode, (unsigned long)pos, i_size_read(inode), api32);

	if (pos == MDS_DIR_END_OFF)
		/*
		 * end-of-file.
		 */
		GOTO(out, rc = 0);

	rc = ll_dir_read(inode, &pos, cookie, filldir);
	lfd->lfd_pos = pos;
        if (pos == MDS_DIR_END_OFF) {
                if (api32)
                        filp->f_pos = LL_DIR_END_OFF_32BIT;
                else
                        filp->f_pos = LL_DIR_END_OFF;
        } else {
                if (api32 && hash64)
                        filp->f_pos = pos >> 32;
                else
                        filp->f_pos = pos;
        }
        filp->f_version = inode->i_version;
        touch_atime(filp->f_vfsmnt, filp->f_dentry);

out:
        if (!rc)
                ll_stats_ops_tally(sbi, LPROC_LL_READDIR, 1);

        RETURN(rc);
}

int ll_send_mgc_param(struct obd_export *mgc, char *string)
{
        struct mgs_send_param *msp;
        int rc = 0;

        OBD_ALLOC_PTR(msp);
        if (!msp)
                return -ENOMEM;

        strncpy(msp->mgs_param, string, MGS_PARAM_MAXLEN);
        rc = obd_set_info_async(NULL, mgc, sizeof(KEY_SET_INFO), KEY_SET_INFO,
                                sizeof(struct mgs_send_param), msp, NULL);
        if (rc)
                CERROR("Failed to set parameter: %d\n", rc);
        OBD_FREE_PTR(msp);

        return rc;
}

int ll_dir_setstripe(struct inode *inode, struct lov_user_md *lump,
                     int set_default)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct md_op_data *op_data;
        struct ptlrpc_request *req = NULL;
        int rc = 0;
        struct lustre_sb_info *lsi = s2lsi(inode->i_sb);
        struct obd_device *mgc = lsi->lsi_mgc;
        int lum_size;
	ENTRY;

        if (lump != NULL) {
                /*
                 * This is coming from userspace, so should be in
                 * local endian.  But the MDS would like it in little
                 * endian, so we swab it before we send it.
                 */
                switch (lump->lmm_magic) {
                case LOV_USER_MAGIC_V1: {
                        if (lump->lmm_magic != cpu_to_le32(LOV_USER_MAGIC_V1))
                                lustre_swab_lov_user_md_v1(lump);
                        lum_size = sizeof(struct lov_user_md_v1);
                        break;
                        }
                case LOV_USER_MAGIC_V3: {
                        if (lump->lmm_magic != cpu_to_le32(LOV_USER_MAGIC_V3))
                                lustre_swab_lov_user_md_v3(
                                        (struct lov_user_md_v3 *)lump);
                        lum_size = sizeof(struct lov_user_md_v3);
                        break;
                        }
                default: {
                        CDEBUG(D_IOCTL, "bad userland LOV MAGIC:"
                                        " %#08x != %#08x nor %#08x\n",
                                        lump->lmm_magic, LOV_USER_MAGIC_V1,
                                        LOV_USER_MAGIC_V3);
                        RETURN(-EINVAL);
                        }
               }
        } else {
                lum_size = sizeof(struct lov_user_md_v1);
        }

        op_data = ll_prep_md_op_data(NULL, inode, NULL, NULL, 0, 0,
                                     LUSTRE_OPC_ANY, NULL);
        if (IS_ERR(op_data))
                RETURN(PTR_ERR(op_data));

        /* swabbing is done in lov_setstripe() on server side */
        rc = md_setattr(sbi->ll_md_exp, op_data, lump, lum_size,
                        NULL, 0, &req, NULL);
        ll_finish_md_op_data(op_data);
        ptlrpc_req_finished(req);
        if (rc) {
                if (rc != -EPERM && rc != -EACCES)
                        CERROR("mdc_setattr fails: rc = %d\n", rc);
        }

        /* In the following we use the fact that LOV_USER_MAGIC_V1 and
         LOV_USER_MAGIC_V3 have the same initial fields so we do not
         need the make the distiction between the 2 versions */
        if (set_default && mgc->u.cli.cl_mgc_mgsexp) {
		char *param = NULL;
		char *buf;

		OBD_ALLOC(param, MGS_PARAM_MAXLEN);
		if (param == NULL)
			GOTO(end, rc = -ENOMEM);

		buf = param;
		/* Get fsname and assume devname to be -MDT0000. */
		ll_get_fsname(inode->i_sb, buf, MTI_NAME_MAXLEN);
		strcat(buf, "-MDT0000.lov");
		buf += strlen(buf);

		/* Set root stripesize */
		sprintf(buf, ".stripesize=%u",
			lump ? le32_to_cpu(lump->lmm_stripe_size) : 0);
		rc = ll_send_mgc_param(mgc->u.cli.cl_mgc_mgsexp, param);
		if (rc)
			GOTO(end, rc);

		/* Set root stripecount */
		sprintf(buf, ".stripecount=%hd",
			lump ? le16_to_cpu(lump->lmm_stripe_count) : 0);
		rc = ll_send_mgc_param(mgc->u.cli.cl_mgc_mgsexp, param);
		if (rc)
			GOTO(end, rc);

		/* Set root stripeoffset */
		sprintf(buf, ".stripeoffset=%hd",
			lump ? le16_to_cpu(lump->lmm_stripe_offset) :
			(typeof(lump->lmm_stripe_offset))(-1));
		rc = ll_send_mgc_param(mgc->u.cli.cl_mgc_mgsexp, param);

end:
		if (param != NULL)
			OBD_FREE(param, MGS_PARAM_MAXLEN);
	}
	RETURN(rc);
}

int ll_dir_getstripe(struct inode *inode, struct lov_mds_md **lmmp,
                     int *lmm_size, struct ptlrpc_request **request)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct mdt_body   *body;
        struct lov_mds_md *lmm = NULL;
        struct ptlrpc_request *req = NULL;
        int rc, lmmsize;
        struct md_op_data *op_data;

        rc = ll_get_max_mdsize(sbi, &lmmsize);
        if (rc)
                RETURN(rc);

        op_data = ll_prep_md_op_data(NULL, inode, NULL, NULL,
                                     0, lmmsize, LUSTRE_OPC_ANY,
                                     NULL);
        if (IS_ERR(op_data))
                RETURN(PTR_ERR(op_data));

        op_data->op_valid = OBD_MD_FLEASIZE | OBD_MD_FLDIREA;
        rc = md_getattr(sbi->ll_md_exp, op_data, &req);
        ll_finish_md_op_data(op_data);
        if (rc < 0) {
                CDEBUG(D_INFO, "md_getattr failed on inode "
                       "%lu/%u: rc %d\n", inode->i_ino,
                       inode->i_generation, rc);
                GOTO(out, rc);
        }

        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
        LASSERT(body != NULL);

        lmmsize = body->eadatasize;

        if (!(body->valid & (OBD_MD_FLEASIZE | OBD_MD_FLDIREA)) ||
            lmmsize == 0) {
                GOTO(out, rc = -ENODATA);
        }

        lmm = req_capsule_server_sized_get(&req->rq_pill,
                                           &RMF_MDT_MD, lmmsize);
        LASSERT(lmm != NULL);

        /*
         * This is coming from the MDS, so is probably in
         * little endian.  We convert it to host endian before
         * passing it to userspace.
         */
        /* We don't swab objects for directories */
        switch (le32_to_cpu(lmm->lmm_magic)) {
        case LOV_MAGIC_V1:
                if (LOV_MAGIC != cpu_to_le32(LOV_MAGIC))
                        lustre_swab_lov_user_md_v1((struct lov_user_md_v1 *)lmm);
                break;
        case LOV_MAGIC_V3:
                if (LOV_MAGIC != cpu_to_le32(LOV_MAGIC))
                        lustre_swab_lov_user_md_v3((struct lov_user_md_v3 *)lmm);
                break;
        default:
                CERROR("unknown magic: %lX\n", (unsigned long)lmm->lmm_magic);
                rc = -EPROTO;
        }
out:
        *lmmp = lmm;
        *lmm_size = lmmsize;
        *request = req;
        return rc;
}

/*
 *  Get MDT index for the inode.
 */
int ll_get_mdt_idx(struct inode *inode)
{
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct md_op_data *op_data;
        int rc, mdtidx;
        ENTRY;

        op_data = ll_prep_md_op_data(NULL, inode, NULL, NULL, 0,
                                     0, LUSTRE_OPC_ANY, NULL);
        if (IS_ERR(op_data))
                RETURN(PTR_ERR(op_data));

        op_data->op_valid |= OBD_MD_MDTIDX;
        rc = md_getattr(sbi->ll_md_exp, op_data, NULL);
        mdtidx = op_data->op_mds;
        ll_finish_md_op_data(op_data);
        if (rc < 0) {
                CDEBUG(D_INFO, "md_getattr_name: %d\n", rc);
                RETURN(rc);
        }
        return mdtidx;
}

/*
 * fake function
 * real one is implemented in data version patch
 */
int ll_data_version(struct inode *inode, __u64 *data_version, int extent_lock)
{
	RETURN(0);
}

/**
 * Generic handler to do any pre-copy work.
 *
 * It send a first hsm_progress (with extent length == 0) to coordinator as a
 * first information for it that real work has started.
 *
 * Moreover, for a ARCHIVE request, it will sample the file data version and
 * store it in \a copy.
 *
 * \return 0 on success.
 */
static int ll_ioc_copy_start(struct super_block *sb, struct hsm_copy *copy)
{
	struct ll_sb_info		*sbi = ll_s2sbi(sb);
	struct hsm_progress_kernel	 hpk;
	int				 rc;
	ENTRY;

	/* Forge a hsm_progress based on data from copy. */
	hpk.hpk_fid = copy->hc_hai.hai_fid;
	hpk.hpk_cookie = copy->hc_hai.hai_cookie;
	hpk.hpk_extent.offset = copy->hc_hai.hai_extent.offset;
	hpk.hpk_extent.length = 0;
	hpk.hpk_flags = 0;
	hpk.hpk_errval = 0;
	hpk.hpk_data_version = 0;


	/* For archive request, we need to read the current file version. */
	if (copy->hc_hai.hai_action == HSMA_ARCHIVE) {
		struct inode	*inode;
		__u64		 data_version = 0;

		/* Get inode for this fid */
		inode = search_inode_for_lustre(sb, &copy->hc_hai.hai_fid);
		if (IS_ERR(inode)) {
			hpk.hpk_flags |= HP_FLAG_RETRY;
			/* hpk_errval is >= 0 */
			hpk.hpk_errval = -PTR_ERR(inode);
			GOTO(progress, rc = PTR_ERR(inode));
		}

		/* Read current file data version */
		rc = ll_data_version(inode, &data_version, 1);
		iput(inode);
		if (rc != 0) {
			CDEBUG(D_HSM, "Could not read file data version. "
				      "Archive request could not be done.\n");
			hpk.hpk_flags |= HP_FLAG_RETRY;
			/* hpk_errval must be >= 0 */
			hpk.hpk_errval = -rc;
			GOTO(progress, rc);
		}

		/* Store it the hsm_copy for later copytool use.
		 * Always modified even if no lsm. */
		copy->hc_data_version = data_version;
	}

progress:
	rc = obd_iocontrol(LL_IOC_HSM_PROGRESS, sbi->ll_md_exp, sizeof(hpk),
			   &hpk, NULL);

	RETURN(rc);
}

/**
 * Generic handler to do any post-copy work.
 *
 * It will send the last hsm_progress update to coordinator to inform it
 * that copy is finished and whether it was successful or not.
 *
 * Moreover,
 * - for ARCHIVE request, it will sample the file data version and compare it
 *   with the version saved in ll_ioc_copy_start(). If they do not match, copy
 *   will be considered as failed.
 * - for RESTORE request, it will sample the file data version and send it to
 *   coordinator which is useful if the file was imported as 'released'.
 *
 * \return 0 on success.
 */
static int ll_ioc_copy_end(struct super_block *sb, struct hsm_copy *copy)
{
	struct ll_sb_info		*sbi = ll_s2sbi(sb);
	struct hsm_progress_kernel	 hpk;
	int				 rc;
	ENTRY;

	/* If you modify the logic here, also check llapi_hsm_copy_end(). */
	/* Take care: copy->hc_hai.hai_action, len, gid and data are not
	 * initialized if copy_end was called with copy == NULL.
	 */

	/* Forge a hsm_progress based on data from copy. */
	hpk.hpk_fid = copy->hc_hai.hai_fid;
	hpk.hpk_cookie = copy->hc_hai.hai_cookie;
	hpk.hpk_extent = copy->hc_hai.hai_extent;
	hpk.hpk_flags = copy->hc_flags | HP_FLAG_COMPLETED;
	hpk.hpk_errval = copy->hc_errval;
	hpk.hpk_data_version = 0;

	/* For archive request, we need to check the file data was not changed.
	 *
	 * For restore request, we need to send the file data version, this is
	 * useful when the file was created using hsm_import.
	 */
	if (((copy->hc_hai.hai_action == HSMA_ARCHIVE) ||
	     (copy->hc_hai.hai_action == HSMA_RESTORE)) &&
	    (copy->hc_errval == 0)) {
		struct inode	*inode;
		__u64		 data_version = 0;

		/* Get lsm for this fid */
		inode = search_inode_for_lustre(sb, &copy->hc_hai.hai_fid);
		if (IS_ERR(inode)) {
			hpk.hpk_flags |= HP_FLAG_RETRY;
			/* hpk_errval must be >= 0 */
			hpk.hpk_errval = -PTR_ERR(inode);
			GOTO(progress, rc = PTR_ERR(inode));
		}

		rc = ll_data_version(inode, &data_version,
				     copy->hc_hai.hai_action == HSMA_ARCHIVE);
		iput(inode);
		if (rc) {
			CDEBUG(D_HSM, "Could not read file data version. "
				      "Request could not be confirmed.\n");
			if (hpk.hpk_errval == 0)
				hpk.hpk_errval = rc;
			GOTO(progress, rc);
		}

		/* Store it the hsm_copy for later copytool use.
		 * Always modified even if no lsm. */
		hpk.hpk_data_version = data_version;

		/* File could have been stripped during archiving, so we need
		 * to check anyway. */
		if ((copy->hc_hai.hai_action == HSMA_ARCHIVE) &&
		    (copy->hc_data_version != data_version)) {
			CDEBUG(D_HSM, "File data version mismatched. "
			      "File content was changed during archiving. "
			       DFID", start:"LPX64" current:"LPX64"\n",
			       PFID(&copy->hc_hai.hai_fid),
			       copy->hc_data_version, data_version);
			/* File was changed, send error to cdt. Do not ask for
			 * retry because if a file is modified frequently,
			 * the cdt will loop on retried archive requests.
			 * The policy engine will ask for a new archive later
			 * when the file will not be modified for some tunable
			 * time */
			/* we do not notify caller */
			hpk.hpk_flags &= ~HP_FLAG_RETRY;
			/* hpk_errval must be >= 0 */
			hpk.hpk_errval = EBUSY;
		}

	}

progress:
	rc = obd_iocontrol(LL_IOC_HSM_PROGRESS, sbi->ll_md_exp, sizeof(hpk),
			   &hpk, NULL);

	RETURN(rc);
}


static int copy_and_ioctl(int cmd, struct obd_export *exp, void *data, int len)
{
        void *ptr;
        int rc;

        OBD_ALLOC(ptr, len);
        if (ptr == NULL)
                return -ENOMEM;
        if (cfs_copy_from_user(ptr, data, len)) {
                OBD_FREE(ptr, len);
                return -EFAULT;
        }
        rc = obd_iocontrol(cmd, exp, len, data, NULL);
        OBD_FREE(ptr, len);
        return rc;
}

static int quotactl_ioctl(struct ll_sb_info *sbi, struct if_quotactl *qctl)
{
        int cmd = qctl->qc_cmd;
        int type = qctl->qc_type;
        int id = qctl->qc_id;
        int valid = qctl->qc_valid;
        int rc = 0;
        ENTRY;

        switch (cmd) {
        case LUSTRE_Q_INVALIDATE:
        case LUSTRE_Q_FINVALIDATE:
        case Q_QUOTAON:
        case Q_QUOTAOFF:
        case Q_SETQUOTA:
        case Q_SETINFO:
                if (!cfs_capable(CFS_CAP_SYS_ADMIN) ||
                    sbi->ll_flags & LL_SBI_RMT_CLIENT)
                        RETURN(-EPERM);
                break;
        case Q_GETQUOTA:
                if (((type == USRQUOTA && cfs_curproc_euid() != id) ||
                     (type == GRPQUOTA && !in_egroup_p(id))) &&
                    (!cfs_capable(CFS_CAP_SYS_ADMIN) ||
                     sbi->ll_flags & LL_SBI_RMT_CLIENT))
                        RETURN(-EPERM);
                break;
        case Q_GETINFO:
                break;
        default:
                CERROR("unsupported quotactl op: %#x\n", cmd);
                RETURN(-ENOTTY);
        }

        if (valid != QC_GENERAL) {
                if (sbi->ll_flags & LL_SBI_RMT_CLIENT)
                        RETURN(-EOPNOTSUPP);

                if (cmd == Q_GETINFO)
                        qctl->qc_cmd = Q_GETOINFO;
                else if (cmd == Q_GETQUOTA)
                        qctl->qc_cmd = Q_GETOQUOTA;
                else
                        RETURN(-EINVAL);

                switch (valid) {
                case QC_MDTIDX:
                        rc = obd_iocontrol(OBD_IOC_QUOTACTL, sbi->ll_md_exp,
                                           sizeof(*qctl), qctl, NULL);
                        break;
                case QC_OSTIDX:
                        rc = obd_iocontrol(OBD_IOC_QUOTACTL, sbi->ll_dt_exp,
                                           sizeof(*qctl), qctl, NULL);
                        break;
                case QC_UUID:
                        rc = obd_iocontrol(OBD_IOC_QUOTACTL, sbi->ll_md_exp,
                                           sizeof(*qctl), qctl, NULL);
                        if (rc == -EAGAIN)
                                rc = obd_iocontrol(OBD_IOC_QUOTACTL,
                                                   sbi->ll_dt_exp,
                                                   sizeof(*qctl), qctl, NULL);
                        break;
                default:
                        rc = -EINVAL;
                        break;
                }

                if (rc)
                        RETURN(rc);

                qctl->qc_cmd = cmd;
        } else {
                struct obd_quotactl *oqctl;

                OBD_ALLOC_PTR(oqctl);
                if (oqctl == NULL)
                        RETURN(-ENOMEM);

                QCTL_COPY(oqctl, qctl);
                rc = obd_quotactl(sbi->ll_md_exp, oqctl);
                if (rc) {
                        if (rc != -EALREADY && cmd == Q_QUOTAON) {
                                oqctl->qc_cmd = Q_QUOTAOFF;
                                obd_quotactl(sbi->ll_md_exp, oqctl);
                        }
                        OBD_FREE_PTR(oqctl);
                        RETURN(rc);
                }
                /* If QIF_SPACE is not set, client should collect the
                 * space usage from OSSs by itself */
                if (cmd == Q_GETQUOTA &&
                    !(oqctl->qc_dqblk.dqb_valid & QIF_SPACE) &&
                    !oqctl->qc_dqblk.dqb_curspace) {
                        struct obd_quotactl *oqctl_tmp;

                        OBD_ALLOC_PTR(oqctl_tmp);
                        if (oqctl_tmp == NULL)
                                GOTO(out, rc = -ENOMEM);

                        oqctl_tmp->qc_cmd = Q_GETOQUOTA;
                        oqctl_tmp->qc_id = oqctl->qc_id;
                        oqctl_tmp->qc_type = oqctl->qc_type;

                        /* collect space usage from OSTs */
                        oqctl_tmp->qc_dqblk.dqb_curspace = 0;
                        rc = obd_quotactl(sbi->ll_dt_exp, oqctl_tmp);
                        if (!rc || rc == -EREMOTEIO) {
                                oqctl->qc_dqblk.dqb_curspace =
                                        oqctl_tmp->qc_dqblk.dqb_curspace;
                                oqctl->qc_dqblk.dqb_valid |= QIF_SPACE;
                        }

                        /* collect space & inode usage from MDTs */
                        oqctl_tmp->qc_dqblk.dqb_curspace = 0;
                        oqctl_tmp->qc_dqblk.dqb_curinodes = 0;
                        rc = obd_quotactl(sbi->ll_md_exp, oqctl_tmp);
                        if (!rc || rc == -EREMOTEIO) {
                                oqctl->qc_dqblk.dqb_curspace +=
                                        oqctl_tmp->qc_dqblk.dqb_curspace;
                                oqctl->qc_dqblk.dqb_curinodes =
                                        oqctl_tmp->qc_dqblk.dqb_curinodes;
                                oqctl->qc_dqblk.dqb_valid |= QIF_INODES;
                        } else {
                                oqctl->qc_dqblk.dqb_valid &= ~QIF_SPACE;
                        }

                        OBD_FREE_PTR(oqctl_tmp);
                }
out:
                QCTL_COPY(qctl, oqctl);
                OBD_FREE_PTR(oqctl);
        }

        RETURN(rc);
}

static long ll_dir_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
        struct inode *inode = file->f_dentry->d_inode;
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        struct obd_ioctl_data *data;
        int rc = 0;
        ENTRY;

        CDEBUG(D_VFSTRACE, "VFS Op:inode=%lu/%u(%p), cmd=%#x\n",
               inode->i_ino, inode->i_generation, inode, cmd);

        /* asm-ppc{,64} declares TCGETS, et. al. as type 't' not 'T' */
        if (_IOC_TYPE(cmd) == 'T' || _IOC_TYPE(cmd) == 't') /* tty ioctls */
                return -ENOTTY;

        ll_stats_ops_tally(ll_i2sbi(inode), LPROC_LL_IOCTL, 1);
        switch(cmd) {
        case FSFILT_IOC_GETFLAGS:
        case FSFILT_IOC_SETFLAGS:
                RETURN(ll_iocontrol(inode, file, cmd, arg));
        case FSFILT_IOC_GETVERSION_OLD:
        case FSFILT_IOC_GETVERSION:
                RETURN(put_user(inode->i_generation, (int *)arg));
        /* We need to special case any other ioctls we want to handle,
         * to send them to the MDS/OST as appropriate and to properly
         * network encode the arg field.
        case FSFILT_IOC_SETVERSION_OLD:
        case FSFILT_IOC_SETVERSION:
        */
        case LL_IOC_GET_MDTIDX: {
                int mdtidx;

                mdtidx = ll_get_mdt_idx(inode);
                if (mdtidx < 0)
                        RETURN(mdtidx);

                if (put_user((int)mdtidx, (int*)arg))
                        RETURN(-EFAULT);

                return 0;
        }
        case IOC_MDC_LOOKUP: {
                struct ptlrpc_request *request = NULL;
                int namelen, len = 0;
                char *buf = NULL;
                char *filename;
                struct md_op_data *op_data;

                rc = obd_ioctl_getdata(&buf, &len, (void *)arg);
                if (rc)
                        RETURN(rc);
                data = (void *)buf;

                filename = data->ioc_inlbuf1;
                namelen = strlen(filename);

                if (namelen < 1) {
                        CDEBUG(D_INFO, "IOC_MDC_LOOKUP missing filename\n");
                        GOTO(out_free, rc = -EINVAL);
                }

                op_data = ll_prep_md_op_data(NULL, inode, NULL, filename, namelen,
                                             0, LUSTRE_OPC_ANY, NULL);
                if (IS_ERR(op_data))
                        GOTO(out_free, rc = PTR_ERR(op_data));

                op_data->op_valid = OBD_MD_FLID;
                rc = md_getattr_name(sbi->ll_md_exp, op_data, &request);
                ll_finish_md_op_data(op_data);
                if (rc < 0) {
                        CDEBUG(D_INFO, "md_getattr_name: %d\n", rc);
                        GOTO(out_free, rc);
                }
                ptlrpc_req_finished(request);
                EXIT;
out_free:
                obd_ioctl_freedata(buf, len);
                return rc;
        }
        case LL_IOC_LOV_SETSTRIPE: {
                struct lov_user_md_v3 lumv3;
                struct lov_user_md_v1 *lumv1 = (struct lov_user_md_v1 *)&lumv3;
                struct lov_user_md_v1 *lumv1p = (struct lov_user_md_v1 *)arg;
                struct lov_user_md_v3 *lumv3p = (struct lov_user_md_v3 *)arg;

                int set_default = 0;

                LASSERT(sizeof(lumv3) == sizeof(*lumv3p));
                LASSERT(sizeof(lumv3.lmm_objects[0]) ==
                        sizeof(lumv3p->lmm_objects[0]));
                /* first try with v1 which is smaller than v3 */
                if (cfs_copy_from_user(lumv1, lumv1p, sizeof(*lumv1)))
                        RETURN(-EFAULT);

                if (lumv1->lmm_magic == LOV_USER_MAGIC_V3) {
                        if (cfs_copy_from_user(&lumv3, lumv3p, sizeof(lumv3)))
                                RETURN(-EFAULT);
                }

                if (inode->i_sb->s_root == file->f_dentry)
                        set_default = 1;

                /* in v1 and v3 cases lumv1 points to data */
                rc = ll_dir_setstripe(inode, lumv1, set_default);

                RETURN(rc);
        }
        case LL_IOC_OBD_STATFS:
                RETURN(ll_obd_statfs(inode, (void *)arg));
        case LL_IOC_LOV_GETSTRIPE:
        case LL_IOC_MDC_GETINFO:
        case IOC_MDC_GETFILEINFO:
        case IOC_MDC_GETFILESTRIPE: {
                struct ptlrpc_request *request = NULL;
                struct lov_user_md *lump;
                struct lov_mds_md *lmm = NULL;
                struct mdt_body *body;
                char *filename = NULL;
                int lmmsize;

                if (cmd == IOC_MDC_GETFILEINFO ||
                    cmd == IOC_MDC_GETFILESTRIPE) {
                        filename = getname((const char *)arg);
                        if (IS_ERR(filename))
                                RETURN(PTR_ERR(filename));

                        rc = ll_lov_getstripe_ea_info(inode, filename, &lmm,
                                                      &lmmsize, &request);
                } else {
                        rc = ll_dir_getstripe(inode, &lmm, &lmmsize, &request);
                }

                if (request) {
                        body = req_capsule_server_get(&request->rq_pill,
                                                      &RMF_MDT_BODY);
                        LASSERT(body != NULL);
                } else {
                        GOTO(out_req, rc);
                }

                if (rc < 0) {
                        if (rc == -ENODATA && (cmd == IOC_MDC_GETFILEINFO ||
                                               cmd == LL_IOC_MDC_GETINFO))
                                GOTO(skip_lmm, rc = 0);
                        else
                                GOTO(out_req, rc);
                }

                if (cmd == IOC_MDC_GETFILESTRIPE ||
                    cmd == LL_IOC_LOV_GETSTRIPE) {
                        lump = (struct lov_user_md *)arg;
                } else {
                        struct lov_user_mds_data *lmdp;
                        lmdp = (struct lov_user_mds_data *)arg;
                        lump = &lmdp->lmd_lmm;
                }
                if (cfs_copy_to_user(lump, lmm, lmmsize)) {
                        if (cfs_copy_to_user(lump, lmm, sizeof(*lump)))
                                GOTO(out_req, rc = -EFAULT);
                        rc = -EOVERFLOW;
                }
        skip_lmm:
                if (cmd == IOC_MDC_GETFILEINFO || cmd == LL_IOC_MDC_GETINFO) {
                        struct lov_user_mds_data *lmdp;
                        lstat_t st = { 0 };

                        st.st_dev     = inode->i_sb->s_dev;
                        st.st_mode    = body->mode;
                        st.st_nlink   = body->nlink;
                        st.st_uid     = body->uid;
                        st.st_gid     = body->gid;
                        st.st_rdev    = body->rdev;
                        st.st_size    = body->size;
                        st.st_blksize = CFS_PAGE_SIZE;
                        st.st_blocks  = body->blocks;
                        st.st_atime   = body->atime;
                        st.st_mtime   = body->mtime;
                        st.st_ctime   = body->ctime;
                        st.st_ino     = inode->i_ino;

                        lmdp = (struct lov_user_mds_data *)arg;
                        if (cfs_copy_to_user(&lmdp->lmd_st, &st, sizeof(st)))
                                GOTO(out_req, rc = -EFAULT);
                }

                EXIT;
        out_req:
                ptlrpc_req_finished(request);
                if (filename)
                        putname(filename);
                return rc;
        }
        case IOC_LOV_GETINFO: {
                struct lov_user_mds_data *lumd;
                struct lov_stripe_md *lsm;
                struct lov_user_md *lum;
                struct lov_mds_md *lmm;
                int lmmsize;
                lstat_t st;

                lumd = (struct lov_user_mds_data *)arg;
                lum = &lumd->lmd_lmm;

                rc = ll_get_max_mdsize(sbi, &lmmsize);
                if (rc)
                        RETURN(rc);

                OBD_ALLOC_LARGE(lmm, lmmsize);
                if (cfs_copy_from_user(lmm, lum, lmmsize))
                        GOTO(free_lmm, rc = -EFAULT);

                switch (lmm->lmm_magic) {
                case LOV_USER_MAGIC_V1:
                        if (LOV_USER_MAGIC_V1 == cpu_to_le32(LOV_USER_MAGIC_V1))
                                break;
                        /* swab objects first so that stripes num will be sane */
                        lustre_swab_lov_user_md_objects(
                                ((struct lov_user_md_v1 *)lmm)->lmm_objects,
                                ((struct lov_user_md_v1 *)lmm)->lmm_stripe_count);
                        lustre_swab_lov_user_md_v1((struct lov_user_md_v1 *)lmm);
                        break;
                case LOV_USER_MAGIC_V3:
                        if (LOV_USER_MAGIC_V3 == cpu_to_le32(LOV_USER_MAGIC_V3))
                                break;
                        /* swab objects first so that stripes num will be sane */
                        lustre_swab_lov_user_md_objects(
                                ((struct lov_user_md_v3 *)lmm)->lmm_objects,
                                ((struct lov_user_md_v3 *)lmm)->lmm_stripe_count);
                        lustre_swab_lov_user_md_v3((struct lov_user_md_v3 *)lmm);
                        break;
                default:
                        GOTO(free_lmm, rc = -EINVAL);
                }

                rc = obd_unpackmd(sbi->ll_dt_exp, &lsm, lmm, lmmsize);
                if (rc < 0)
                        GOTO(free_lmm, rc = -ENOMEM);

                /* Perform glimpse_size operation. */
                memset(&st, 0, sizeof(st));

                rc = ll_glimpse_ioctl(sbi, lsm, &st);
                if (rc)
                        GOTO(free_lsm, rc);

                if (cfs_copy_to_user(&lumd->lmd_st, &st, sizeof(st)))
                        GOTO(free_lsm, rc = -EFAULT);

                EXIT;
        free_lsm:
                obd_free_memmd(sbi->ll_dt_exp, &lsm);
        free_lmm:
                OBD_FREE_LARGE(lmm, lmmsize);
                return rc;
        }
        case OBD_IOC_LLOG_CATINFO: {
		RETURN(-EOPNOTSUPP);
        }
        case OBD_IOC_QUOTACHECK: {
                struct obd_quotactl *oqctl;
                int error = 0;

                if (!cfs_capable(CFS_CAP_SYS_ADMIN) ||
                    sbi->ll_flags & LL_SBI_RMT_CLIENT)
                        RETURN(-EPERM);

                OBD_ALLOC_PTR(oqctl);
                if (!oqctl)
                        RETURN(-ENOMEM);
                oqctl->qc_type = arg;
                rc = obd_quotacheck(sbi->ll_md_exp, oqctl);
                if (rc < 0) {
                        CDEBUG(D_INFO, "md_quotacheck failed: rc %d\n", rc);
                        error = rc;
                }

                rc = obd_quotacheck(sbi->ll_dt_exp, oqctl);
                if (rc < 0)
                        CDEBUG(D_INFO, "obd_quotacheck failed: rc %d\n", rc);

                OBD_FREE_PTR(oqctl);
                return error ?: rc;
        }
        case OBD_IOC_POLL_QUOTACHECK: {
                struct if_quotacheck *check;

                if (!cfs_capable(CFS_CAP_SYS_ADMIN) ||
                    sbi->ll_flags & LL_SBI_RMT_CLIENT)
                        RETURN(-EPERM);

                OBD_ALLOC_PTR(check);
                if (!check)
                        RETURN(-ENOMEM);

                rc = obd_iocontrol(cmd, sbi->ll_md_exp, 0, (void *)check,
                                   NULL);
                if (rc) {
                        CDEBUG(D_QUOTA, "mdc ioctl %d failed: %d\n", cmd, rc);
                        if (cfs_copy_to_user((void *)arg, check,
                                             sizeof(*check)))
                                CDEBUG(D_QUOTA, "cfs_copy_to_user failed\n");
                        GOTO(out_poll, rc);
                }

                rc = obd_iocontrol(cmd, sbi->ll_dt_exp, 0, (void *)check,
                                   NULL);
                if (rc) {
                        CDEBUG(D_QUOTA, "osc ioctl %d failed: %d\n", cmd, rc);
                        if (cfs_copy_to_user((void *)arg, check,
                                             sizeof(*check)))
                                CDEBUG(D_QUOTA, "cfs_copy_to_user failed\n");
                        GOTO(out_poll, rc);
                }
        out_poll:
                OBD_FREE_PTR(check);
                RETURN(rc);
        }
#if LUSTRE_VERSION_CODE < OBD_OCD_VERSION(2, 7, 50, 0)
        case LL_IOC_QUOTACTL_18: {
                /* copy the old 1.x quota struct for internal use, then copy
                 * back into old format struct.  For 1.8 compatibility. */
                struct if_quotactl_18 *qctl_18;
                struct if_quotactl *qctl_20;

                OBD_ALLOC_PTR(qctl_18);
                if (!qctl_18)
                        RETURN(-ENOMEM);

                OBD_ALLOC_PTR(qctl_20);
                if (!qctl_20)
                        GOTO(out_quotactl_18, rc = -ENOMEM);

                if (cfs_copy_from_user(qctl_18, (void *)arg, sizeof(*qctl_18)))
                        GOTO(out_quotactl_20, rc = -ENOMEM);

                QCTL_COPY(qctl_20, qctl_18);
                qctl_20->qc_idx = 0;

                /* XXX: dqb_valid was borrowed as a flag to mark that
                 *      only mds quota is wanted */
                if (qctl_18->qc_cmd == Q_GETQUOTA &&
                    qctl_18->qc_dqblk.dqb_valid) {
                        qctl_20->qc_valid = QC_MDTIDX;
                        qctl_20->qc_dqblk.dqb_valid = 0;
                } else if (qctl_18->obd_uuid.uuid[0] != '\0') {
                        qctl_20->qc_valid = QC_UUID;
                        qctl_20->obd_uuid = qctl_18->obd_uuid;
                } else {
                        qctl_20->qc_valid = QC_GENERAL;
                }

                rc = quotactl_ioctl(sbi, qctl_20);

                if (rc == 0) {
                        QCTL_COPY(qctl_18, qctl_20);
                        qctl_18->obd_uuid = qctl_20->obd_uuid;

                        if (cfs_copy_to_user((void *)arg, qctl_18,
                                             sizeof(*qctl_18)))
                                rc = -EFAULT;
                }

        out_quotactl_20:
                OBD_FREE_PTR(qctl_20);
        out_quotactl_18:
                OBD_FREE_PTR(qctl_18);
                RETURN(rc);
        }
#else
#warning "remove old LL_IOC_QUOTACTL_18 compatibility code"
#endif /* LUSTRE_VERSION_CODE < OBD_OCD_VERSION(2, 7, 50, 0) */
        case LL_IOC_QUOTACTL: {
                struct if_quotactl *qctl;

                OBD_ALLOC_PTR(qctl);
                if (!qctl)
                        RETURN(-ENOMEM);

                if (cfs_copy_from_user(qctl, (void *)arg, sizeof(*qctl)))
                        GOTO(out_quotactl, rc = -EFAULT);

                rc = quotactl_ioctl(sbi, qctl);

                if (rc == 0 && cfs_copy_to_user((void *)arg,qctl,sizeof(*qctl)))
                        rc = -EFAULT;

        out_quotactl:
                OBD_FREE_PTR(qctl);
                RETURN(rc);
        }
        case OBD_IOC_GETDTNAME:
        case OBD_IOC_GETMDNAME:
                RETURN(ll_get_obd_name(inode, cmd, arg));
        case LL_IOC_FLUSHCTX:
                RETURN(ll_flush_ctx(inode));
#ifdef CONFIG_FS_POSIX_ACL
        case LL_IOC_RMTACL: {
            if (sbi->ll_flags & LL_SBI_RMT_CLIENT &&
                inode == inode->i_sb->s_root->d_inode) {
                struct ll_file_data *fd = LUSTRE_FPRIVATE(file);

                LASSERT(fd != NULL);
                rc = rct_add(&sbi->ll_rct, cfs_curproc_pid(), arg);
                if (!rc)
                        fd->fd_flags |= LL_FILE_RMTACL;
                RETURN(rc);
            } else
                RETURN(0);
        }
#endif
        case LL_IOC_GETOBDCOUNT: {
                int count, vallen;
                struct obd_export *exp;

                if (cfs_copy_from_user(&count, (int *)arg, sizeof(int)))
                        RETURN(-EFAULT);

                /* get ost count when count is zero, get mdt count otherwise */
                exp = count ? sbi->ll_md_exp : sbi->ll_dt_exp;
                vallen = sizeof(count);
                rc = obd_get_info(NULL, exp, sizeof(KEY_TGT_COUNT),
                                  KEY_TGT_COUNT, &vallen, &count, NULL);
                if (rc) {
                        CERROR("get target count failed: %d\n", rc);
                        RETURN(rc);
                }

                if (cfs_copy_to_user((int *)arg, &count, sizeof(int)))
                        RETURN(-EFAULT);

                RETURN(0);
        }
        case LL_IOC_PATH2FID:
                if (cfs_copy_to_user((void *)arg, ll_inode2fid(inode),
                                     sizeof(struct lu_fid)))
                        RETURN(-EFAULT);
                RETURN(0);
        case LL_IOC_GET_CONNECT_FLAGS: {
                RETURN(obd_iocontrol(cmd, sbi->ll_md_exp, 0, NULL, (void*)arg));
        }
        case OBD_IOC_CHANGELOG_SEND:
        case OBD_IOC_CHANGELOG_CLEAR:
                rc = copy_and_ioctl(cmd, sbi->ll_md_exp, (void *)arg,
                                    sizeof(struct ioc_changelog));
                RETURN(rc);
        case OBD_IOC_FID2PATH:
		RETURN(ll_fid2path(inode, (void *)arg));
	case LL_IOC_HSM_PROGRESS: {
		struct hsm_progress_kernel	hpk;
		struct hsm_progress		hp;

		if (cfs_copy_from_user(&hp, (void *)arg, sizeof(hp)))
			RETURN(-EFAULT);

		hpk.hpk_fid = hp.hp_fid;
		hpk.hpk_cookie = hp.hp_cookie;
		hpk.hpk_extent = hp.hp_extent;
		hpk.hpk_flags = hp.hp_flags;
		hpk.hpk_errval = hp.hp_errval;
		hpk.hpk_data_version = 0;

		/* File may not exist in Lustre; all progress
		 * reported to Lustre root */
		rc = obd_iocontrol(cmd, sbi->ll_md_exp, sizeof(hpk), &hpk,
				   NULL);
		RETURN(rc);
	}
	case LL_IOC_HSM_CT_START:
		rc = copy_and_ioctl(cmd, sbi->ll_md_exp, (void *)arg,
				    sizeof(struct lustre_kernelcomm));
		RETURN(rc);

	case LL_IOC_HSM_COPY_START: {
		struct hsm_copy	*copy;
		int		 rc;

		OBD_ALLOC_PTR(copy);
		if (copy == NULL)
			RETURN(-ENOMEM);
		if (cfs_copy_from_user(copy, (char *)arg, sizeof(*copy))) {
			OBD_FREE_PTR(copy);
			RETURN(-EFAULT);
		}

		rc = ll_ioc_copy_start(inode->i_sb, copy);
		if (cfs_copy_to_user((char *)arg, copy, sizeof(*copy)))
			rc = -EFAULT;

		OBD_FREE_PTR(copy);
		RETURN(rc);
	}
	case LL_IOC_HSM_COPY_END: {
		struct hsm_copy	*copy;
		int		 rc;

		OBD_ALLOC_PTR(copy);
		if (copy == NULL)
			RETURN(-ENOMEM);
		if (cfs_copy_from_user(copy, (char *)arg, sizeof(*copy))) {
			OBD_FREE_PTR(copy);
			RETURN(-EFAULT);
		}

		rc = ll_ioc_copy_end(inode->i_sb, copy);
		if (cfs_copy_to_user((char *)arg, copy, sizeof(*copy)))
			rc = -EFAULT;

		OBD_FREE_PTR(copy);
		RETURN(rc);
	}

	default:
		RETURN(obd_iocontrol(cmd, sbi->ll_dt_exp, 0, NULL,
				     (void *)arg));
	}
}

static loff_t ll_dir_seek(struct file *file, loff_t offset, int origin)
{
        struct inode *inode = file->f_mapping->host;
        struct ll_file_data *fd = LUSTRE_FPRIVATE(file);
        struct ll_sb_info *sbi = ll_i2sbi(inode);
        int api32 = ll_need_32bit_api(sbi);
        loff_t ret = -EINVAL;
        ENTRY;

        cfs_mutex_lock(&inode->i_mutex);
        switch (origin) {
                case SEEK_SET:
                        break;
                case SEEK_CUR:
                        offset += file->f_pos;
                        break;
                case SEEK_END:
                        if (offset > 0)
                                GOTO(out, ret);
                        if (api32)
                                offset += LL_DIR_END_OFF_32BIT;
                        else
                                offset += LL_DIR_END_OFF;
                        break;
                default:
                        GOTO(out, ret);
        }

        if (offset >= 0 &&
            ((api32 && offset <= LL_DIR_END_OFF_32BIT) ||
             (!api32 && offset <= LL_DIR_END_OFF))) {
                if (offset != file->f_pos) {
                        if ((api32 && offset == LL_DIR_END_OFF_32BIT) ||
                            (!api32 && offset == LL_DIR_END_OFF))
				fd->lfd_pos = MDS_DIR_END_OFF;
                        else if (api32 && sbi->ll_flags & LL_SBI_64BIT_HASH)
				fd->lfd_pos = offset << 32;
                        else
				fd->lfd_pos = offset;
                        file->f_pos = offset;
                        file->f_version = 0;
                }
                ret = offset;
        }
        GOTO(out, ret);

out:
        cfs_mutex_unlock(&inode->i_mutex);
        return ret;
}

int ll_dir_open(struct inode *inode, struct file *file)
{
        ENTRY;
        RETURN(ll_file_open(inode, file));
}

int ll_dir_release(struct inode *inode, struct file *file)
{
        ENTRY;
        RETURN(ll_file_release(inode, file));
}

struct file_operations ll_dir_operations = {
        .llseek   = ll_dir_seek,
        .open     = ll_dir_open,
        .release  = ll_dir_release,
        .read     = generic_read_dir,
        .readdir  = ll_readdir,
        .unlocked_ioctl   = ll_dir_ioctl,
        .fsync    = ll_fsync,
};
