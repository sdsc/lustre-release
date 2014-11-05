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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osd-zfs/osd_io.c
 *
 * Author: Alex Zhuravlev <bzzz@whamcloud.com>
 * Author: Mike Pershin <tappro@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_OSD

#include <lustre_ver.h>
#include <libcfs/libcfs.h>
#include <obd_support.h>
#include <lustre_net.h>
#include <obd.h>
#include <obd_class.h>
#include <lustre_disk.h>
#include <lustre_fid.h>
#include <lustre/lustre_idl.h>	/* LLOG_CHUNK_SIZE definition */

#include "osd_internal.h"

#include <sys/dnode.h>
#include <sys/dbuf.h>
#include <sys/spa.h>
#include <sys/stat.h>
#include <sys/zap.h>
#include <sys/spa_impl.h>
#include <sys/zfs_znode.h>
#include <sys/dmu_tx.h>
#include <sys/dmu_objset.h>
#include <sys/dsl_prop.h>
#include <sys/sa_impl.h>
#include <sys/txg.h>

static char *osd_zerocopy_tag = "zerocopy";


static void record_start_io(struct osd_device *osd, int rw, int npages,
			    int discont_pages)
{
	struct obd_histogram *h = osd->od_brw_stats.hist;

	if (rw == READ) {
		atomic_inc(&osd->od_r_in_flight);
		lprocfs_oh_tally(&h[BRW_R_RPC_HIST],
				 atomic_read(&osd->od_r_in_flight));
		lprocfs_oh_tally_log2(&h[BRW_R_PAGES], npages);
		lprocfs_oh_tally(&h[BRW_R_DISCONT_PAGES], discont_pages);

	} else {
		atomic_inc(&osd->od_w_in_flight);
		lprocfs_oh_tally(&h[BRW_W_RPC_HIST],
				 atomic_read(&osd->od_w_in_flight));
		lprocfs_oh_tally_log2(&h[BRW_W_PAGES], npages);
		lprocfs_oh_tally(&h[BRW_W_DISCONT_PAGES], discont_pages);

	}
}

static void record_end_io(struct osd_device *osd, int rw,
			  unsigned long elapsed, int disksize)
{
	struct obd_histogram *h = osd->od_brw_stats.hist;

	if (rw == READ) {
		atomic_dec(&osd->od_r_in_flight);
		if (disksize > 0)
			lprocfs_oh_tally_log2(&h[BRW_R_DISK_IOSIZE], disksize);
		if (elapsed)
			lprocfs_oh_tally_log2(&h[BRW_R_IO_TIME], elapsed);

	} else {
		atomic_dec(&osd->od_w_in_flight);
		if (disksize > 0)
			lprocfs_oh_tally_log2(&h[BRW_W_DISK_IOSIZE], disksize);
		if (elapsed)
			lprocfs_oh_tally_log2(&h[BRW_W_IO_TIME], elapsed);
	}
}

static ssize_t osd_read(const struct lu_env *env, struct dt_object *dt,
			struct lu_buf *buf, loff_t *pos,
			struct lustre_capa *capa)
{
	struct osd_object *obj  = osd_dt_obj(dt);
	struct osd_device *osd = osd_obj2dev(obj);
	uint64_t	   old_size;
	int		   size = buf->lb_len;
	int		   rc;
	unsigned long	   start;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	start = cfs_time_current();

	read_lock(&obj->oo_attr_lock);
	old_size = obj->oo_attr.la_size;
	read_unlock(&obj->oo_attr_lock);

	if (*pos + size > old_size) {
		if (old_size < *pos)
			return 0;
		else
			size = old_size - *pos;
	}

	record_start_io(osd, READ, (size >> PAGE_CACHE_SHIFT), 0);

	rc = -dmu_read(osd->od_os, obj->oo_db->db_object, *pos, size,
			buf->lb_buf, DMU_READ_PREFETCH);

	record_end_io(osd, READ, cfs_time_current() - start, size);
	if (rc == 0) {
		rc = size;
		*pos += size;
	}
	return rc;
}

static ssize_t osd_declare_write(const struct lu_env *env, struct dt_object *dt,
				const struct lu_buf *buf, loff_t pos,
				struct thandle *th)
{
	struct osd_object  *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	uint64_t            oid;
	ENTRY;

	oh = container_of0(th, struct osd_thandle, ot_super);

	/* in some cases declare can race with creation (e.g. llog)
	 * and we need to wait till object is initialized. notice
	 * LOHA_EXISTs is supposed to be the last step in the
	 * initialization */

	/* declare possible size change. notice we can't check
	 * current size here as another thread can change it */

	if (dt_object_exists(dt)) {
		LASSERT(obj->oo_db);
		oid = obj->oo_db->db_object;

		dmu_tx_hold_sa(oh->ot_tx, obj->oo_sa_hdl, 0);
	} else {
		oid = DMU_NEW_OBJECT;
		dmu_tx_hold_sa_create(oh->ot_tx, ZFS_SA_BASE_ATTR_SIZE);
	}

	/* XXX: we still miss for append declaration support in ZFS
	 *	-1 means append which is used by llog mostly, llog
	 *	can grow upto LLOG_CHUNK_SIZE*8 records */
	if (pos == -1)
		pos = max_t(loff_t, 256 * 8 * LLOG_CHUNK_SIZE,
			    obj->oo_attr.la_size + (2 << 20));
	dmu_tx_hold_write(oh->ot_tx, oid, pos, buf->lb_len);

	/* dt_declare_write() is usually called for system objects, such
	 * as llog or last_rcvd files. We needn't enforce quota on those
	 * objects, so always set the lqi_space as 0. */
	RETURN(osd_declare_quota(env, osd, obj->oo_attr.la_uid,
				 obj->oo_attr.la_gid, 0, oh, true, NULL,
				 false));
}

static ssize_t osd_write(const struct lu_env *env, struct dt_object *dt,
			const struct lu_buf *buf, loff_t *pos,
			struct thandle *th, struct lustre_capa *capa,
			int ignore_quota)
{
	struct osd_object  *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	uint64_t            offset = *pos;
	int                 rc = 0;
	ENTRY;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	LASSERT(th != NULL);
	oh = container_of0(th, struct osd_thandle, ot_super);

	record_start_io(osd, WRITE, (buf->lb_len >> PAGE_CACHE_SHIFT), 0);

	dmu_write(osd->od_os, obj->oo_db->db_object, offset,
		(uint64_t)buf->lb_len, buf->lb_buf, oh->ot_tx);
	write_lock(&obj->oo_attr_lock);
	if (obj->oo_attr.la_size < offset + buf->lb_len) {
		obj->oo_attr.la_size = offset + buf->lb_len;
		write_unlock(&obj->oo_attr_lock);
		/* osd_object_sa_update() will be copying directly from oo_attr
		 * into dbuf.  any update within a single txg will copy the
		 * most actual */
		rc = osd_object_sa_update(obj, SA_ZPL_SIZE(osd),
					&obj->oo_attr.la_size, 8, oh);
		if (unlikely(rc))
			GOTO(out, rc);
	} else {
		write_unlock(&obj->oo_attr_lock);
	}

	if (rc == 0 && osd->od_zil_enabled)
		out_write_pack(env, &oh->ot_buf,
			       lu_object_fid(&dt->do_lu), buf, *pos, 0);

	*pos += buf->lb_len;
	rc = buf->lb_len;

out:
	record_end_io(osd, WRITE, 0, buf->lb_len);

	RETURN(rc);
}

/*
 * XXX: for the moment I don't want to use lnb_flags for osd-internal
 *      purposes as it's not very well defined ...
 *      instead I use the lowest bit of the address so that:
 *        arc buffer:  .lnb_obj = abuf          (arc we loan for write)
 *        dbuf buffer: .lnb_obj = dbuf | 1      (dbuf we get for read)
 *        copy buffer: .lnb_page->mapping = obj (page we allocate for write)
 *
 *      bzzz, to blame
 */
static int osd_bufs_put(const struct lu_env *env, struct dt_object *dt,
			struct niobuf_local *lnb, int npages)
{
	struct osd_object *obj  = osd_dt_obj(dt);
	struct osd_device *osd = osd_obj2dev(obj);
	unsigned long      ptr;
	int                i;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	for (i = 0; i < npages; i++) {
		if (lnb[i].lnb_page == NULL)
			continue;
		if (lnb[i].lnb_page->mapping == (void *)obj) {
			/* this is anonymous page allocated for copy-write */
			lnb[i].lnb_page->mapping = NULL;
			__free_page(lnb[i].lnb_page);
			atomic_dec(&osd->od_zerocopy_alloc);
		} else {
			/* see comment in osd_bufs_get_read() */
			ptr = (unsigned long)lnb[i].lnb_data;
			if (ptr & 1UL) {
				ptr &= ~1UL;
				dmu_buf_rele((void *)ptr, osd_zerocopy_tag);
				atomic_dec(&osd->od_zerocopy_pin);
				osd_unlock_offset(obj, lnb[i].lnb_file_offset);
			} else if (lnb[i].lnb_data != NULL) {
				dmu_return_arcbuf(lnb[i].lnb_data);
				atomic_dec(&osd->od_zerocopy_loan);
				osd_unlock_offset(obj, lnb[i].lnb_file_offset);
			}
		}
		lnb[i].lnb_page = NULL;
		lnb[i].lnb_data = NULL;
	}

	return 0;
}

static inline struct page *kmem_to_page(void *addr)
{
	if (is_vmalloc_addr(addr))
		return vmalloc_to_page(addr);
	else
		return virt_to_page(addr);
}

static int osd_bufs_get_read(const struct lu_env *env, struct osd_object *obj,
				loff_t off, ssize_t len, struct niobuf_local *lnb)
{
	struct osd_device *osd = osd_obj2dev(obj);
	dmu_buf_t        **dbp;
	int                rc, i, numbufs, npages = 0;
	ENTRY;

	/* grab buffers for read:
	 * OSD API let us to grab buffers first, then initiate IO(s)
	 * so that all required IOs will be done in parallel, but at the
	 * moment DMU doesn't provide us with a method to grab buffers.
	 * If we discover this is a vital for good performance we
	 * can get own replacement for dmu_buf_hold_array_by_bonus().
	 */
	while (len > 0) {

		osd_lock_offset(obj, off, __FUNCTION__);

		rc = -dmu_buf_hold_array_by_bonus(obj->oo_db, off, len, TRUE,
						  osd_zerocopy_tag, &numbufs,
						  &dbp);
		if (unlikely(rc))
			GOTO(err, rc);

		for (i = 0; i < numbufs; i++) {
			int bufoff, tocpy, thispage;
			void *dbf = dbp[i];

			LASSERT(len > 0);

			atomic_inc(&osd->od_zerocopy_pin);

			bufoff = off - dbp[i]->db_offset;
			tocpy = min_t(int, dbp[i]->db_size - bufoff, len);

			/* kind of trick to differentiate dbuf vs. arcbuf */
			LASSERT(((unsigned long)dbp[i] & 1) == 0);
			dbf = (void *) ((unsigned long)dbp[i] | 1);

			while (tocpy > 0) {
				thispage = PAGE_CACHE_SIZE;
				thispage -= bufoff & (PAGE_CACHE_SIZE - 1);
				thispage = min(tocpy, thispage);

				lnb->lnb_rc = 0;
				lnb->lnb_file_offset = off;
				lnb->lnb_page_offset = bufoff & ~CFS_PAGE_MASK;
				lnb->lnb_len = thispage;
				lnb->lnb_page = kmem_to_page(dbp[i]->db_data +
							     bufoff);
				/* mark just a single slot: we need this
				 * reference to dbuf to be release once */
				lnb->lnb_data = dbf;
				dbf = NULL;

				tocpy -= thispage;
				len -= thispage;
				bufoff += thispage;
				off += thispage;

				npages++;
				lnb++;
			}

			/* steal dbuf so dmu_buf_rele_array() cant release it */
			dbp[i] = NULL;
		}

		dmu_buf_rele_array(dbp, numbufs, osd_zerocopy_tag);
	}

	RETURN(npages);

err:
	LASSERT(rc < 0);
	osd_bufs_put(env, &obj->oo_dt, lnb - npages, npages);
	RETURN(rc);
}

static int osd_bufs_get_write(const struct lu_env *env, struct osd_object *obj,
				loff_t off, ssize_t len, struct niobuf_local *lnb)
{
	struct osd_device *osd = osd_obj2dev(obj);
	int                plen, off_in_block, sz_in_block;
	int                rc, i = 0, npages = 0;
	arc_buf_t         *abuf;
	uint32_t           bs;
	uint64_t           dummy;
	ENTRY;

	dmu_object_size_from_db(obj->oo_db, &bs, &dummy);

	/*
	 * currently only full blocks are subject to zerocopy approach:
	 * so that we're sure nobody is trying to update the same block
	 */
	while (len > 0) {
		LASSERT(npages < PTLRPC_MAX_BRW_PAGES);

		off_in_block = off & (bs - 1);
		sz_in_block = min_t(int, bs - off_in_block, len);

		if (sz_in_block == bs) {
			/* full block, try to use zerocopy */

			abuf = dmu_request_arcbuf(obj->oo_db, bs);
			if (unlikely(abuf == NULL))
				GOTO(out_err, rc = -ENOMEM);

			atomic_inc(&osd->od_zerocopy_loan);

			osd_lock_offset(obj, off, "0copy write");

			/* go over pages arcbuf contains, put them as
			 * local niobufs for ptlrpc's bulks */
			while (sz_in_block > 0) {
				plen = min_t(int, sz_in_block, PAGE_CACHE_SIZE);

				lnb[i].lnb_file_offset = off;
				lnb[i].lnb_page_offset = 0;
				lnb[i].lnb_len = plen;
				lnb[i].lnb_rc = 0;
				if (sz_in_block == bs)
					lnb[i].lnb_data = abuf;
				else
					lnb[i].lnb_data = NULL;

				/* this one is not supposed to fail */
				lnb[i].lnb_page = kmem_to_page(abuf->b_data +
							off_in_block);
				LASSERT(lnb[i].lnb_page);

				lprocfs_counter_add(osd->od_stats,
						LPROC_OSD_ZEROCOPY_IO, 1);

				sz_in_block -= plen;
				len -= plen;
				off += plen;
				off_in_block += plen;
				i++;
				npages++;
			}
		} else {
			if (off_in_block == 0 && len < bs &&
					off + len >= obj->oo_attr.la_size)
				lprocfs_counter_add(osd->od_stats,
						LPROC_OSD_TAIL_IO, 1);

			/* can't use zerocopy, allocate temp. buffers */
			while (sz_in_block > 0) {
				plen = min_t(int, sz_in_block, PAGE_CACHE_SIZE);

				lnb[i].lnb_file_offset = off;
				lnb[i].lnb_page_offset = 0;
				lnb[i].lnb_len = plen;
				lnb[i].lnb_rc = 0;
				lnb[i].lnb_data = NULL;

				lnb[i].lnb_page = alloc_page(OSD_GFP_IO);
				if (unlikely(lnb[i].lnb_page == NULL))
					GOTO(out_err, rc = -ENOMEM);

				LASSERT(lnb[i].lnb_page->mapping == NULL);
				lnb[i].lnb_page->mapping = (void *)obj;

				atomic_inc(&osd->od_zerocopy_alloc);
				lprocfs_counter_add(osd->od_stats,
						LPROC_OSD_COPY_IO, 1);

				sz_in_block -= plen;
				len -= plen;
				off += plen;
				i++;
				npages++;
			}
		}
	}

	RETURN(npages);

out_err:
	osd_bufs_put(env, &obj->oo_dt, lnb, npages);
	RETURN(rc);
}

static int osd_bufs_get(const struct lu_env *env, struct dt_object *dt,
			loff_t offset, ssize_t len, struct niobuf_local *lnb,
			int rw, struct lustre_capa *capa)
{
	struct osd_object *obj  = osd_dt_obj(dt);
	int                rc;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	if (rw == 0)
		rc = osd_bufs_get_read(env, obj, offset, len, lnb);
	else
		rc = osd_bufs_get_write(env, obj, offset, len, lnb);

	return rc;
}

static int osd_write_prep(const struct lu_env *env, struct dt_object *dt,
			struct niobuf_local *lnb, int npages)
{
	struct osd_object *obj = osd_dt_obj(dt);

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	return 0;
}

/* Return number of blocks that aren't mapped in the [start, start + size]
 * region */
static int osd_count_not_mapped(struct osd_object *obj, uint64_t start,
				uint32_t size)
{
	dmu_buf_impl_t	*dbi = (dmu_buf_impl_t *)obj->oo_db;
	dmu_buf_impl_t	*db;
	dnode_t		*dn;
	uint32_t	 blkshift;
	uint64_t	 end, blkid;
	int		 rc;
	ENTRY;

	DB_DNODE_ENTER(dbi);
	dn = DB_DNODE(dbi);

	if (dn->dn_maxblkid == 0) {
		if (start + size <= dn->dn_datablksz)
			GOTO(out, size = 0);
		if (start < dn->dn_datablksz)
			start = dn->dn_datablksz;
		/* assume largest block size */
		blkshift = SPA_MAXBLOCKSHIFT;
	} else {
		/* blocksize can't change */
		blkshift = dn->dn_datablkshift;
	}

	/* compute address of last block */
	end = (start + size - 1) >> blkshift;
	/* align start on block boundaries */
	start >>= blkshift;

	/* size is null, can't be mapped */
	if (obj->oo_attr.la_size == 0 || dn->dn_maxblkid == 0)
		GOTO(out, size = (end - start + 1) << blkshift);

	/* beyond EOF, can't be mapped */
	if (start > dn->dn_maxblkid)
		GOTO(out, size = (end - start + 1) << blkshift);

	size = 0;
	for (blkid = start; blkid <= end; blkid++) {
		if (blkid == dn->dn_maxblkid)
			/* this one is mapped for sure */
			continue;
		if (blkid > dn->dn_maxblkid) {
			size += (end - blkid + 1) << blkshift;
			GOTO(out, size);
		}

		rc = dbuf_hold_impl(dn, 0, blkid, TRUE, FTAG, &db);
		if (rc) {
			/* for ENOENT (block not mapped) and any other errors,
			 * assume the block isn't mapped */
			size += 1 << blkshift;
			continue;
		}
		dbuf_rele(db, FTAG);
	}

	GOTO(out, size);
out:
	DB_DNODE_EXIT(dbi);
	return size;
}

static int osd_declare_write_commit(const struct lu_env *env,
				struct dt_object *dt,
				struct niobuf_local *lnb, int npages,
				struct thandle *th)
{
	struct osd_object  *obj = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	uint64_t            offset = 0;
	uint32_t            size = 0;
	int		    i, rc, flags = 0;
	bool		    ignore_quota = false, synced = false;
	long long	    space = 0;
	struct page	   *last_page = NULL;
	unsigned long	    discont_pages = 0;
	ENTRY;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	LASSERT(lnb);
	LASSERT(npages > 0);

	oh = container_of0(th, struct osd_thandle, ot_super);

	for (i = 0; i < npages; i++) {
		if (last_page && lnb[i].lnb_page->index != (last_page->index + 1))
			++discont_pages;
		last_page = lnb[i].lnb_page;
		if (lnb[i].lnb_rc)
			/* ENOSPC, network RPC error, etc.
			 * We don't want to book space for pages which will be
			 * skipped in osd_write_commit(). Hence we skip pages
			 * with lnb_rc != 0 here too */
			continue;
		/* ignore quota for the whole request if any page is from
		 * client cache or written by root.
		 *
		 * XXX once we drop the 1.8 client support, the checking
		 * for whether page is from cache can be simplified as:
		 * !(lnb[i].flags & OBD_BRW_SYNC)
		 *
		 * XXX we could handle this on per-lnb basis as done by
		 * grant. */
		if ((lnb[i].lnb_flags & OBD_BRW_NOQUOTA) ||
		    (lnb[i].lnb_flags & (OBD_BRW_FROM_GRANT | OBD_BRW_SYNC)) ==
		    OBD_BRW_FROM_GRANT)
			ignore_quota = true;
		if (size == 0) {
			/* first valid lnb */
			offset = lnb[i].lnb_file_offset;
			size = lnb[i].lnb_len;
			continue;
		}
		if (offset + size == lnb[i].lnb_file_offset) {
			/* this lnb is contiguous to the previous one */
			size += lnb[i].lnb_len;
			continue;
		}

		dmu_tx_hold_write(oh->ot_tx, obj->oo_db->db_object,
				  offset, size);
		/* estimating space that will be consumed by a write is rather
		 * complicated with ZFS. As a consequence, we don't account for
		 * indirect blocks and quota overrun will be adjusted once the
		 * operation is committed, if required. */
		space += osd_count_not_mapped(obj, offset, size);

		offset = lnb[i].lnb_file_offset;
		size = lnb[i].lnb_len;
	}

	if (size) {
		dmu_tx_hold_write(oh->ot_tx, obj->oo_db->db_object,
				  offset, size);
		space += osd_count_not_mapped(obj, offset, size);
	}

	dmu_tx_hold_sa(oh->ot_tx, obj->oo_sa_hdl, 0);

	oh->ot_write_commit = 1; /* used in osd_trans_start() for fail_loc */

	/* backend zfs filesystem might be configured to store multiple data
	 * copies */
	space  *= osd->od_os->os_copies;
	space   = toqb(space);
	CDEBUG(D_QUOTA, "writting %d pages, reserving "LPD64"K of quota "
	       "space\n", npages, space);

	record_start_io(osd, WRITE, npages, discont_pages);
retry:
	/* acquire quota space if needed */
	rc = osd_declare_quota(env, osd, obj->oo_attr.la_uid,
			       obj->oo_attr.la_gid, space, oh, true, &flags,
			       ignore_quota);

	if (!synced && rc == -EDQUOT && (flags & QUOTA_FL_SYNC) != 0) {
		dt_sync(env, th->th_dev);
		synced = true;
		CDEBUG(D_QUOTA, "retry after sync\n");
		flags = 0;
		goto retry;
	}

	/* we need only to store the overquota flags in the first lnb for
	 * now, once we support multiple objects BRW, this code needs be
	 * revised. */
	if (flags & QUOTA_FL_OVER_USRQUOTA)
		lnb[0].lnb_flags |= OBD_BRW_OVER_USRQUOTA;
	if (flags & QUOTA_FL_OVER_GRPQUOTA)
		lnb[0].lnb_flags |= OBD_BRW_OVER_GRPQUOTA;

	RETURN(rc);
}

static int osd_write_commit(const struct lu_env *env, struct dt_object *dt,
			struct niobuf_local *lnb, int npages,
			struct thandle *th)
{
	struct osd_object  *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	uint64_t            new_size = 0;
	int                 i, rc = 0;
	unsigned long	   iosize = 0;
	ENTRY;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	LASSERT(th != NULL);
	oh = container_of0(th, struct osd_thandle, ot_super);

	for (i = 0; i < npages; i++) {
		CDEBUG(D_INODE, "write %u bytes at %u\n",
			(unsigned) lnb[i].lnb_len,
			(unsigned) lnb[i].lnb_file_offset);

		if (lnb[i].lnb_rc) {
			/* ENOSPC, network RPC error, etc.
			 * Unlike ldiskfs, zfs allocates new blocks on rewrite,
			 * so we skip this page if lnb_rc is set to -ENOSPC */
			CDEBUG(D_INODE, "obj "DFID": skipping lnb[%u]: rc=%d\n",
				PFID(lu_object_fid(&dt->do_lu)), i,
				lnb[i].lnb_rc);
			continue;
		}

		if (lnb[i].lnb_page->mapping == (void *)obj) {
			dmu_write(osd->od_os, obj->oo_db->db_object,
				lnb[i].lnb_file_offset, lnb[i].lnb_len,
				kmap(lnb[i].lnb_page), oh->ot_tx);
			kunmap(lnb[i].lnb_page);
			osd_zil_log_write(env, oh, dt, lnb[i].lnb_file_offset,
					  lnb[i].lnb_len);
		} else if (lnb[i].lnb_data) {
			LASSERT(((unsigned long)lnb[i].lnb_data & 1) == 0);
			/* buffer loaned for zerocopy, try to use it.
			 * notice that dmu_assign_arcbuf() is smart
			 * enough to recognize changed blocksize
			 * in this case it fallbacks to dmu_write() */
			dmu_assign_arcbuf(obj->oo_db, lnb[i].lnb_file_offset,
					  lnb[i].lnb_data, oh->ot_tx);
			/* drop the reference, otherwise osd_put_bufs()
			 * will be releasing it - bad! */
			osd_zil_log_write(env, oh, dt, lnb[i].lnb_file_offset,
					  arc_buf_size(lnb[i].lnb_data));
			osd_unlock_offset(obj, lnb[i].lnb_file_offset);
			lnb[i].lnb_data = NULL;
			atomic_dec(&osd->od_zerocopy_loan);
		}

		if (new_size < lnb[i].lnb_file_offset + lnb[i].lnb_len)
			new_size = lnb[i].lnb_file_offset + lnb[i].lnb_len;
		iosize += lnb[i].lnb_len;
	}

	if (unlikely(new_size == 0)) {
		/* no pages to write, no transno is needed */
		th->th_local = 1;
		/* it is important to return 0 even when all lnb_rc == -ENOSPC
		 * since ofd_commitrw_write() retries several times on ENOSPC */
		record_end_io(osd, WRITE, 0, 0);
		RETURN(0);
	}

	write_lock(&obj->oo_attr_lock);
	if (obj->oo_attr.la_size < new_size) {
		obj->oo_attr.la_size = new_size;
		write_unlock(&obj->oo_attr_lock);
		/* osd_object_sa_update() will be copying directly from
		 * oo_attr into dbuf. any update within a single txg will copy
		 * the most actual */
		rc = osd_object_sa_update(obj, SA_ZPL_SIZE(osd),
					  &obj->oo_attr.la_size, 8, oh);
	} else {
		write_unlock(&obj->oo_attr_lock);
	}

	record_end_io(osd, WRITE, 0, iosize);

	RETURN(rc);
}

static int osd_read_prep(const struct lu_env *env, struct dt_object *dt,
			struct niobuf_local *lnb, int npages)
{
	struct osd_object *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct lu_buf      buf;
	loff_t             offset;
	int                i;
	unsigned long	   start;
	unsigned long	   size = 0;

	LASSERT(dt_object_exists(dt));
	LASSERT(obj->oo_db);

	start = cfs_time_current();

	record_start_io(osd, READ, npages, 0);

	for (i = 0; i < npages; i++) {
		buf.lb_buf = kmap(lnb[i].lnb_page);
		buf.lb_len = lnb[i].lnb_len;
		offset = lnb[i].lnb_file_offset;

		CDEBUG(D_OTHER, "read %u bytes at %u\n",
			(unsigned) lnb[i].lnb_len,
			(unsigned) lnb[i].lnb_file_offset);
		lnb[i].lnb_rc = osd_read(env, dt, &buf, &offset, NULL);
		kunmap(lnb[i].lnb_page);

		size += lnb[i].lnb_rc;

		if (lnb[i].lnb_rc < buf.lb_len) {
			/* all subsequent rc should be 0 */
			while (++i < npages)
				lnb[i].lnb_rc = 0;
			break;
		}
	}

	record_end_io(osd, READ, cfs_time_current() - start, size);

	return 0;
}

/*
 * Punch/truncate an object
 *
 *      IN:     db  - dmu_buf of the object to free data in.
 *              off - start of section to free.
 *              len - length of section to free (DMU_OBJECT_END => to EOF).
 *
 *      RETURN: 0 if success
 *              error code if failure
 *
 * The transaction passed to this routine must have
 * dmu_tx_hold_sa() and if off < size, dmu_tx_hold_free()
 * called and then assigned to a transaction group.
 */
static int __osd_object_punch(objset_t *os, dmu_buf_t *db, dmu_tx_t *tx,
				uint64_t size, uint64_t off, uint64_t len)
{
	int rc = 0;

	/* Assert that the transaction has been assigned to a
	   transaction group. */
	LASSERT(tx->tx_txg != 0);
	/*
	 * Nothing to do if file already at desired length.
	 */
	if (len == DMU_OBJECT_END && size == off)
		return 0;

	if (off < size)
		rc = -dmu_free_range(os, db->db_object, off, len, tx);

	return rc;
}

static int osd_punch(const struct lu_env *env, struct dt_object *dt,
			__u64 start, __u64 end, struct thandle *th,
			struct lustre_capa *capa)
{
	struct osd_object  *obj = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	__u64               len;
	int                 rc = 0;
	ENTRY;

	LASSERT(dt_object_exists(dt));
	LASSERT(osd_invariant(obj));

	LASSERT(th != NULL);
	oh = container_of0(th, struct osd_thandle, ot_super);

	write_lock(&obj->oo_attr_lock);
	/* truncate */
	if (end == OBD_OBJECT_EOF || end >= obj->oo_attr.la_size)
		len = DMU_OBJECT_END;
	else
		len = end - start;
	write_unlock(&obj->oo_attr_lock);

	rc = __osd_object_punch(osd->od_os, obj->oo_db, oh->ot_tx,
				obj->oo_attr.la_size, start, len);
	/* set new size */
	if (len == DMU_OBJECT_END) {
		write_lock(&obj->oo_attr_lock);
		obj->oo_attr.la_size = start;
		write_unlock(&obj->oo_attr_lock);
		rc = osd_object_sa_update(obj, SA_ZPL_SIZE(osd),
					  &obj->oo_attr.la_size, 8, oh);
	}
#if 0
	if (rc == 0 && osd->od_zil_enabled)
		out_write_pack(env, &oh->ot_buf,
			       lu_object_fid(&dt->do_lu), buf, *pos, 0);
#endif

	RETURN(rc);
}

static int osd_declare_punch(const struct lu_env *env, struct dt_object *dt,
			__u64 start, __u64 end, struct thandle *handle)
{
	struct osd_object  *obj = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	struct osd_thandle *oh;
	__u64		    len;
	ENTRY;

	oh = container_of0(handle, struct osd_thandle, ot_super);

	read_lock(&obj->oo_attr_lock);
	if (end == OBD_OBJECT_EOF || end >= obj->oo_attr.la_size)
		len = DMU_OBJECT_END;
	else
		len = end - start;

	/* declare we'll free some blocks ... */
	if (start < obj->oo_attr.la_size) {
		read_unlock(&obj->oo_attr_lock);
		dmu_tx_hold_free(oh->ot_tx, obj->oo_db->db_object, start, len);
	} else {
		read_unlock(&obj->oo_attr_lock);
	}

	/* ... and we'll modify size attribute */
	dmu_tx_hold_sa(oh->ot_tx, obj->oo_sa_hdl, 0);

	RETURN(osd_declare_quota(env, osd, obj->oo_attr.la_uid,
				 obj->oo_attr.la_gid, 0, oh, true, NULL,
				 false));
}


struct dt_body_operations osd_body_ops = {
	.dbo_read			= osd_read,
	.dbo_declare_write		= osd_declare_write,
	.dbo_write			= osd_write,
	.dbo_bufs_get			= osd_bufs_get,
	.dbo_bufs_put			= osd_bufs_put,
	.dbo_write_prep			= osd_write_prep,
	.dbo_declare_write_commit	= osd_declare_write_commit,
	.dbo_write_commit		= osd_write_commit,
	.dbo_read_prep			= osd_read_prep,
	.dbo_declare_punch		= osd_declare_punch,
	.dbo_punch			= osd_punch,
};


static void osd_zil_get_done(zgd_t *zgd, int error)
{
	struct osd_object *obj = zgd->zgd_private;

	if (zgd->zgd_db) {
		osd_unlock_offset(obj, zgd->zgd_db->db_offset);
		dmu_buf_rele(zgd->zgd_db, zgd);
	}

	/* XXX: missing locking, see zfs_write() for the details */

	if (error == 0 && zgd->zgd_bp)
		zil_add_block(zgd->zgd_zilog, zgd->zgd_bp);

	OBD_FREE_PTR(zgd);
}

/*
 * Get data to generate a TX_WRITE intent log record.
 */
int osd_zil_get_data(void *arg, lr_write_t *lr, char *buf, zio_t *zio)
{
	struct osd_object *obj = arg;
	struct osd_device *osd = osd_obj2dev(obj);
	objset_t *os = osd->od_os;
	uint64_t object = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t size = lr->lr_length;
	blkptr_t *bp = &lr->lr_blkptr;
	dmu_buf_t *db;
	zgd_t *zgd;
	int error;

	ASSERT(zio != NULL);
	ASSERT(size != 0);

	if (lu_object_is_dying(obj->oo_dt.do_lu.lo_header))
		return (SET_ERROR(ENOENT));

	OBD_ALLOC_PTR(zgd);
	LASSERT(zgd);
	zgd->zgd_zilog = osd->od_zilog;

	/* XXX: missing locking, see zfs_write() for the details
	 *	basically we have to protect data from modifications
	 *	as it's still a part of running txg */

	/*
	 * Write records come in two flavors: immediate and indirect.
	 * For small writes it's cheaper to store the data with the
	 * log record (immediate); for large writes it's cheaper to
	 * sync the data and get a pointer to it (indirect) so that
	 * we don't have to write the data twice.
	 */
	if (buf != NULL) { /* immediate write */
		error = dmu_read(os, object, offset, size, buf,
				 DMU_READ_NO_PREFETCH);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_COPIED, 1);
	} else {
		osd_lock_offset(obj, offset, __FUNCTION__);

		offset = P2ALIGN_TYPED(offset, size, uint64_t);
		error = dmu_buf_hold(os, object, offset, zgd, &db,
		    DMU_READ_NO_PREFETCH);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_INDIRECT, 1);
		if (error == 0) {
			blkptr_t *obp = dmu_buf_get_blkptr(db);
			if (obp) {
				ASSERT(BP_IS_HOLE(bp));
				*bp = *obp;
			}

			zgd->zgd_db = db;
			zgd->zgd_bp = &lr->lr_blkptr;
			zgd->zgd_private = obj;

			ASSERT(db != NULL);
			ASSERT(db->db_offset == offset);
			ASSERT(db->db_size == size);

			error = dmu_sync(zio, lr->lr_common.lrc_txg,
			    osd_zil_get_done, zgd);

			if (error == 0)
				return 0;
		}
	}

	osd_zil_get_done(zgd, error);

	return SET_ERROR(error);
}

static void osd_zil_write_commit_cb(void *arg)
{
	itx_t *itx = (itx_t *)arg;
	struct osd_object *obj = itx->itx_private;
	struct osd_device *osd = osd_obj2dev(obj);
	atomic_dec(&osd->od_recs_to_write);
	lu_object_put(NULL, &obj->oo_dt.do_lu);
}

/*
 * We store data in the log buffers if it's small enough.
 * Otherwise we will later flush the data out via dmu_sync().
 */
ssize_t zvol_immediate_write_sz = 32768;

void osd_zil_log_write(const struct lu_env *env, struct osd_thandle *oh,
		       struct dt_object *dt, uint64_t offset, uint64_t size)
{
	struct osd_object  *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	zilog_t *zilog = osd->od_zilog;
	ssize_t immediate_write_sz;
	dnode_t		*dn;
	int		blocksize;

	if (osd->od_zil_enabled == 0)
		return;

	if (zil_replaying(zilog, oh->ot_tx))
		return;

	DB_DNODE_ENTER((dmu_buf_impl_t *)obj->oo_db);
	dn = DB_DNODE((dmu_buf_impl_t *)obj->oo_db);
	blocksize = dn->dn_datablksz;
	DB_DNODE_EXIT((dmu_buf_impl_t *)obj->oo_db);

	immediate_write_sz = (zilog->zl_logbias == ZFS_LOGBIAS_THROUGHPUT)
		? 0 : zvol_immediate_write_sz;

	while (size) {
		itx_t *itx;
		lr_write_t *lr;
		ssize_t len;
		itx_wr_state_t write_state;

		if (blocksize > immediate_write_sz &&
		    size >= blocksize && offset % blocksize == 0) {
			write_state = WR_INDIRECT; /* uses dmu_sync */
			len = blocksize;
		} else if (1) {
			write_state = WR_COPIED;
			len = MIN(ZIL_MAX_LOG_DATA, size);
		} else {
			write_state = WR_NEED_COPY;
			len = MIN(ZIL_MAX_LOG_DATA, size);
		}

		itx = zil_itx_create(TX_WRITE, sizeof(*lr) +
		    (write_state == WR_COPIED ? len : 0));
		lr = (lr_write_t *)&itx->itx_lr;
		if (write_state == WR_COPIED && dmu_read(osd->od_os,
		    obj->oo_db->db_object, offset, len, lr+1,
		    DMU_READ_NO_PREFETCH) != 0) {
			zil_itx_destroy(itx);
			itx = zil_itx_create(TX_WRITE, sizeof(*lr));
			lr = (lr_write_t *)&itx->itx_lr;
			write_state = WR_NEED_COPY;
		}

		itx->itx_wr_state = write_state;
		if (write_state == WR_NEED_COPY)
			itx->itx_sod += len;
		lr->lr_foid = obj->oo_db->db_object;
		lr->lr_offset = offset;
		lr->lr_length = len;
		lr->lr_blkoff = 0;
		BP_ZERO(&lr->lr_blkptr);

		itx->itx_callback = osd_zil_write_commit_cb;
		itx->itx_callback_data = itx;

		lu_object_get(&obj->oo_dt.do_lu);
		itx->itx_private = obj;
		itx->itx_sync = 1;

		if (itx->itx_wr_state == WR_COPIED)
			lprocfs_counter_add(osd->od_stats,
					    LPROC_OSD_ZIL_COPIED, 1);

		CDEBUG(D_CACHE,
		       "new zil %p in txg %llu: %u/%u type %u to %llu\n",
		       itx, oh->ot_tx->tx_txg, (unsigned)offset, (unsigned)len,
		       (unsigned) write_state, obj->oo_db->db_object);

		atomic_inc(&osd->od_recs_to_write);
		(void) zil_itx_assign(zilog, itx, oh->ot_tx);

		offset += len;
		size -= len;
	}
}

static int osd_zil_replay_err(struct osd_device *o, lr_t *lr, boolean_t s)
{
	return ENOTSUP;
}

void byteswap_uint64_array(void *vbuf, size_t size)
{
	uint64_t *buf = vbuf;
	size_t count = size >> 3;
	int i;

	ASSERT((size & 7) == 0);

	for (i = 0; i < count; i++)
		buf[i] = BSWAP_64(buf[i]);
}

struct osd_zil_cbdata {
	const struct lu_env *env;
	struct osd_device *osd;
};

struct lr_update {
	lr_t	lr_common;
	uint64_t lr_oid;
};


typedef int (*osd_replay_update)(const struct lu_env *env,
				 struct dt_object *o,
				 struct object_update *u,
				 struct osd_thandle *oh);

#define CHECK_AND_MOVE(ATTRIBUTE, FIELD)			\
do {								\
	if (attr->la_valid & ATTRIBUTE) {			\
		memcpy(&attr->FIELD, ptr, sizeof(attr->FIELD));	\
		ptr += sizeof(attr->FIELD);			\
	}							\
} while (0)
static void osd_unpack_lu_attr(struct lu_attr *attr, void *ptr)
{
	memset(attr, 0, sizeof(attr));
	memcpy(&attr->la_valid, ptr, sizeof(attr->la_valid));
	ptr += sizeof(attr->la_valid);

	CHECK_AND_MOVE(LA_SIZE, la_size);
	CHECK_AND_MOVE(LA_MTIME, la_mtime);
	CHECK_AND_MOVE(LA_ATIME, la_atime);
	CHECK_AND_MOVE(LA_CTIME, la_ctime);
	CHECK_AND_MOVE(LA_BLOCKS, la_blocks);
	CHECK_AND_MOVE((LA_MODE | LA_TYPE), la_mode);
	CHECK_AND_MOVE(LA_UID, la_uid);
	CHECK_AND_MOVE(LA_GID, la_gid);
	CHECK_AND_MOVE(LA_FLAGS, la_flags);
	CHECK_AND_MOVE(LA_NLINK, la_nlink);
	/*CHECK_AND_MOVE(LA_BLKBITS, la_blkbits);*/
	CHECK_AND_MOVE(LA_BLKSIZE, la_blksize);
	CHECK_AND_MOVE(LA_RDEV, la_rdev);
}
#undef CHECK_AND_MOVE

static int osd_replay_create_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	void			*ptr;
	struct lu_attr		 la;
	struct dt_object_format	 dof;
	int			 rc;
	size_t			 size;

	ptr = object_update_param_get(u, 0, &size);
	LASSERT(ptr != NULL && size >= 0);
	osd_unpack_lu_attr(&la, ptr);

	dof.dof_type = dt_mode_to_dft(la.la_mode);

	/* XXX: hint?* */

	if (oh->ot_assigned == 0)
		rc = dt_declare_create(env, o, &la, NULL, &dof, &oh->ot_super);
	else
		rc = dt_create(env, o, &la, NULL, &dof, &oh->ot_super);

	return rc;
}

static int osd_replay_destroy_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_destroy(env, o, &oh->ot_super);
	else
		rc = dt_destroy(env, o, &oh->ot_super);

	return rc;
}

static int osd_replay_ref_add_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_ref_add(env, o, &oh->ot_super);
	else
		rc = dt_ref_add(env, o, &oh->ot_super);
	return rc;
}

static int osd_replay_ref_del_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_ref_del(env, o, &oh->ot_super);
	else
		rc = dt_ref_del(env, o, &oh->ot_super);
	return rc;
}

static int osd_replay_attr_set_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	void			*ptr;
	struct lu_attr		 attr;
	int			 rc;
	size_t			 size;

	ptr = object_update_param_get(u, 0, &size);
	LASSERT(ptr != NULL && size >= 8);
	osd_unpack_lu_attr(&attr, ptr);

	if (oh->ot_assigned == 0)
		rc = dt_declare_attr_set(env, o, &attr, &oh->ot_super);
	else
		rc = dt_attr_set(env, o, &attr, &oh->ot_super, NULL);

	return rc;
}

static int osd_replay_xattr_set_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct lu_buf	 b;
	char		*name;
	int		 rc;
	size_t		 size = 0;
	int		 *fp, fl;

	name = object_update_param_get(u, 0, &size);
	if (size == 1) {
		if ((unsigned char)name[0] == 1)
			name = XATTR_NAME_LOV;
		else if ((unsigned char)name[0] == 2)
			name = XATTR_NAME_LINK;
		else if ((unsigned char)name[0] == 3)
			name = XATTR_NAME_VERSION;
		else
			LBUG();
	}
	LASSERT(name != NULL);

	b.lb_buf = object_update_param_get(u, 0, &size);
	LASSERT(b.lb_buf != NULL && size > 0);
	b.lb_len = size;

	fp = object_update_param_get(u, 2, &size);
	fl = 0;
	if (fp != NULL)
		fl = *fp;

	if (oh->ot_assigned == 0)
		rc = dt_declare_xattr_set(env, o, &b, name, fl, &oh->ot_super);
	else
		rc = dt_xattr_set(env, o, &b, name, fl, &oh->ot_super, NULL);
	return rc;
}

static int osd_replay_xattr_del_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	char		*name;
	int		 rc;

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	if (oh->ot_assigned == 0)
		rc = dt_declare_xattr_del(env, o, name, &oh->ot_super);
	else
		rc = dt_xattr_del(env, o, name, &oh->ot_super, NULL);

	return rc;
}

static int osd_replay_insert_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct dt_insert_rec rec;
	struct lu_fid *fid;
	size_t		size;
	__u32		*ptype;
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0) {
		rc = -ENOTDIR;
		return rc;
	}

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	fid = object_update_param_get(u, 1, &size);
	LASSERT(fid != NULL && size == sizeof(*fid));

	ptype = object_update_param_get(u, 2, &size);
	LASSERT(ptype != NULL && size == sizeof(*ptype));

	rec.rec_fid = fid;
	rec.rec_type = *ptype;

	if (oh->ot_assigned == 0)
		rc = dt_declare_insert(env, o, (struct dt_rec *)&rec,
					(struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_insert(env, o, (struct dt_rec *)&rec,
				(struct dt_key *)name,
				&oh->ot_super, NULL, 1);

	return rc;
}

static int osd_replay_delete_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0) {
		rc = -ENOTDIR;
		return rc;
	}

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	if (oh->ot_assigned == 0)
		rc = dt_declare_delete(env, o, (struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_delete(env, o, (struct dt_key *)name,
				&oh->ot_super, NULL);

	return rc;
}

int dt_write(const struct lu_env *env, struct dt_object *dt,
		const struct lu_buf *buf, loff_t *pos, struct thandle *th)
{
	int rc;

	LASSERTF(dt != NULL, "dt is NULL when we want to write record\n");
	LASSERT(th != NULL);
	LASSERT(dt->do_body_ops);
	LASSERT(dt->do_body_ops->dbo_write);
	rc = dt->do_body_ops->dbo_write(env, dt, buf, pos, th, BYPASS_CAPA, 1);
	return rc;
}

int dt_declare_write(const struct lu_env *env, struct dt_object *dt,
		const struct lu_buf *buf, loff_t pos, struct thandle *th)
{
	int rc;

	LASSERTF(dt != NULL, "dt is NULL when we want to write record\n");
	LASSERT(th != NULL);
	LASSERT(dt->do_body_ops);
	LASSERT(dt->do_body_ops->dbo_write);
	rc = dt->do_body_ops->dbo_declare_write(env, dt, buf, pos, th);
	return rc;
}

static int osd_replay_write_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct lu_buf lb;
	__u64 *tmp;
	__u64 pos;
	size_t size;
	int rc;

	lb.lb_buf = object_update_param_get(u, 0, &size);
	LASSERT(lb.lb_buf != NULL && size > 0);
	lb.lb_len = size;

	tmp = object_update_param_get(u, 1, &size);
	LASSERT(tmp != NULL && size == 8);
	pos = *tmp;

	if (oh->ot_assigned == 0)
		rc = dt_declare_write(env, o, &lb, pos, &oh->ot_super);
	else
		rc = dt_write(env, o, &lb, &pos, &oh->ot_super);

	return rc;
}

static osd_replay_update osd_replay_update_vec[] = {
	[OUT_CREATE]	= osd_replay_create_update,
	[OUT_DESTROY]	= osd_replay_destroy_update,
	[OUT_REF_ADD]	= osd_replay_ref_add_update,
	[OUT_REF_DEL]	= osd_replay_ref_del_update,
	[OUT_ATTR_SET]	= osd_replay_attr_set_update,
	[OUT_XATTR_SET]	= osd_replay_xattr_set_update,
	[OUT_XATTR_DEL]	= osd_replay_xattr_del_update,
	[OUT_INDEX_INSERT]	= osd_replay_insert_update,
	[OUT_INDEX_DELETE]	= osd_replay_delete_update,
	[OUT_WRITE]		= osd_replay_write_update,
};

static int osd_zil_replay_update(struct osd_zil_cbdata *cb,
				 struct lr_update *lr,
				 boolean_t byteswap)
{
	struct osd_device	*osd = cb->osd;
	struct dt_device	*dt = &osd->od_dt_dev;
	const struct lu_env	*env = cb->env;
	struct object_update_request *ureq = (void *)(lr + 1);
	struct thandle		*th;
	struct osd_thandle	*oh;
	struct dt_object	*o;
	int		 error = 0;
	int		 i, rc;

	if (byteswap && 0)
		byteswap_uint64_array(lr, sizeof(*lr));

	LASSERT(ureq->ourq_magic == UPDATE_REQUEST_MAGIC);
	CDEBUG(D_HA, "REPLAY %d updates\n", ureq->ourq_count);

	th = dt_trans_create(env, dt);
	LASSERT(!IS_ERR(th));
	oh = container_of0(th, struct osd_thandle, ot_super);

	for (i = 0; i < ureq->ourq_count; i++) {
		struct object_update		*update;
		update = object_update_request_get(ureq, i, NULL);
		LASSERT(update != NULL);
		CDEBUG(D_HA, "%d: %d to "DFID"\n", i, (int)update->ou_type,
			PFID(&update->ou_fid));

		o = dt_locate(env, dt, &update->ou_fid);
		LASSERT(!IS_ERR(o));

		rc = osd_replay_update_vec[update->ou_type](env, o,
							    update, oh);

		lu_object_put(env, &o->do_lu);
	}

	rc = dt_trans_start(env, dt, th);
	LASSERT(rc == 0);

	for (i = 0; i < ureq->ourq_count; i++) {
		struct object_update		*update;
		update = object_update_request_get(ureq, i, NULL);
		LASSERT(update != NULL);
		CDEBUG(D_HA, "%d: %d to "DFID"\n", i, (int)update->ou_type,
			PFID(&update->ou_fid));

		o = dt_locate(env, dt, &update->ou_fid);
		LASSERT(!IS_ERR(o));

		rc = osd_replay_update_vec[update->ou_type](env, o,
							    update, oh);

		lu_object_put(env, &o->do_lu);
	}

	CDEBUG(D_HA, "%d updates replayed\n", ureq->ourq_count);

	(void) zil_replaying(osd->od_zilog, oh->ot_tx);
	rc = dt_trans_stop(env, dt, th);
	LASSERT(rc == 0);

	return SET_ERROR(error);
}

static int osd_zil_replay_write(struct osd_zil_cbdata *cb, lr_write_t *lr,
				boolean_t byteswap)
{
	struct osd_device *osd = cb->osd;
	objset_t	*os = osd->od_os;
	char		*data = (char *)(lr + 1);
	uint64_t	 off = lr->lr_offset;
	uint64_t	 len = lr->lr_length;
	sa_handle_t	*sa;
	uint64_t	 end;
	dmu_tx_t	*tx;
	int		 error;

	CDEBUG(D_HA, "REPLAY %u/%u to %llu\n", (unsigned)off,
	       (unsigned)len, lr->lr_foid);

	if (byteswap)
		byteswap_uint64_array(lr, sizeof(*lr));

	error = sa_handle_get(os, lr->lr_foid, lr, SA_HDL_PRIVATE, &sa);
	if (error)
		return SET_ERROR(error);

restart:
	tx = dmu_tx_create(os);
	dmu_tx_hold_write(tx, lr->lr_foid, off, len);
	dmu_tx_hold_sa(tx, sa, 1);
	error = dmu_tx_assign(tx, TXG_WAIT);
	if (error) {
		if (error == ERESTART) {
			dmu_tx_wait(tx);
			dmu_tx_abort(tx);
			goto restart;
		}
		dmu_tx_abort(tx);
	} else {
		dmu_write(os, lr->lr_foid, off, len, data, tx);
		end = lr->lr_offset + lr->lr_length;
		sa_update(sa, SA_ZPL_SIZE(osd), (void *)&end,
			  sizeof(uint64_t), tx);
		/* Ensure the replayed seq is updated */
		(void) zil_replaying(osd->od_zilog, tx);
		dmu_tx_commit(tx);
	}

	sa_handle_destroy(sa);

	return SET_ERROR(error);
}

static int osd_zil_replay_create(struct osd_zil_cbdata *cb, lr_acl_v0_t *lr,
				boolean_t byteswap)
{
	/* used as noop, see osd_ro() */
	CWARN("CREATE replay - NOOP\n");
	return 0;
}

zil_replay_func_t osd_zil_replay_vector[TX_MAX_TYPE] = {
	(zil_replay_func_t)osd_zil_replay_err,	/* no such transaction type */
	(zil_replay_func_t)osd_zil_replay_create,/* TX_CREATE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_MKDIR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_MKXATTR */
	(zil_replay_func_t)osd_zil_replay_update,/* TX_SYMLINK */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_REMOVE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_RMDIR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_LINK */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_RENAME */
	(zil_replay_func_t)osd_zil_replay_write,	/* TX_WRITE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_TRUNCATE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_SETATTR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_ACL_V0 */
};

static int osd_zil_create_noop(struct osd_device *o)
{
	itx_t		*itx;
	dmu_tx_t	*tx;
	lr_create_t	*lr;
	int		 rc;

	tx = dmu_tx_create(o->od_os);
	rc = -dmu_tx_assign(tx, TXG_WAITED);
	if (rc != 0) {
		dmu_tx_abort(tx);
		return rc;
	}
	itx = zil_itx_create(TX_CREATE, sizeof(*lr));
	lr = (lr_create_t *)&itx->itx_lr;
	lr->lr_doid = 0;
	lr->lr_foid = 0;
	lr->lr_mode = 0;
	zil_itx_assign(o->od_zilog, itx, tx);
	dmu_tx_commit(tx);

	return 0;
}

int osd_zil_init(const struct lu_env *env, struct osd_device *o)
{
	dsl_pool_t *dp = dmu_objset_pool(o->od_os);
	struct osd_zil_cbdata cb;
	int	    rc;

	o->od_zilog = zil_open(o->od_os, osd_zil_get_data);
	o->od_zilog->zl_logbias = ZFS_LOGBIAS_LATENCY;

	cb.env = env;
	cb.osd = o;
	zil_replay(o->od_os, &cb, osd_zil_replay_vector);

	CWARN("%s: ZIL - %llu blocks, %llu recs, last synced %llu\n",
	      o->od_svname, o->od_zilog->zl_parse_blk_count,
	      o->od_zilog->zl_parse_lr_count, dp->dp_tx.tx_synced_txg);


	while (BP_IS_HOLE(&o->od_zilog->zl_header->zh_log)) {
		rc = osd_zil_create_noop(o);
		if (rc < 0) {
			CERROR("%s: can't create ZIL, rc = %d\n",
			       o->od_svname, rc);
			break;
		}
		zil_commit(o->od_zilog, 0);
	}

	return 0;
}

void osd_zil_dump_stats(struct osd_device *o)
{
	long ave;
	int i, j;
	char *buf;

	if (atomic_read(&o->od_recs_in_log) == 0)
		return;

	ave = atomic_read(&o->od_bytes_in_log);
	ave /= atomic_read(&o->od_recs_in_log);
	CWARN("%s: %ld bytes/rec ave\n", o->od_svname, ave);
	OBD_ALLOC(buf, 512);
	for (i = 0, j = 0; i < OUT_LAST; i++) {
		if (atomic_read(&o->od_updates_by_type[i]) == 0)
			continue;
		j += snprintf(buf + j, 512 - j, "  #%d: %d(%d)", i,
			      atomic_read(&o->od_updates_by_type[i]),
			      (int)(atomic_read(&o->od_bytes_by_type[i]) /
				      atomic_read(&o->od_updates_by_type[i])));
	}
	CWARN("%s: %s\n", o->od_svname, buf);
	OBD_FREE(buf, 512);
}

void osd_zil_fini(struct osd_device *o)
{
	osd_zil_dump_stats(o);

	/* normally we should be calling zil_close() here to release ZIL
	 * but this would cause all the logs to be flushed preventing
	 * testing. so we suspend ZIL, release all the ITXs and resume ZIL */
#if 0
	{
		void *cookie;
		zil_suspend(o->od_mntdev, &cookie);
		zil_close(o->od_zilog);
		zil_resume(cookie);
	}
#else
	zil_close(o->od_zilog);
#endif
}

static void osd_zil_commit_cb(void *arg)
{
	itx_t *itx = (itx_t *)arg;
	struct osd_device *osd = itx->itx_private;
	atomic_dec(&osd->od_recs_to_write);

}

void osd_zil_make_itx(struct osd_thandle *oh)
{
	struct osd_device	*osd = osd_dt_dev(oh->ot_super.th_dev);
	itx_t			*itx;
	struct lr_update	*lr;
	int			 len;
	char			*buf;
	int			 i, j;

	if (oh->ot_buf.ub_req == NULL)
		return;
	if (osd->od_zilog == NULL)
		goto out;

	for (i = 0; i < oh->ot_buf.ub_req->ourq_count; i++) {
		struct object_update		*update;
		size_t				 size;
		int				 sum;

		update = object_update_request_get(oh->ot_buf.ub_req,
						   i, NULL);
		LASSERT(update != NULL);
		LASSERT(update->ou_type < OUT_LAST);
		atomic_inc(&osd->od_updates_by_type[update->ou_type]);

		sum = offsetof(struct object_update, ou_params[0]);
		for (j = 0; j < update->ou_params_count; j++) {
			size = 0;
			object_update_param_get(update, j, &size);
			sum += size;
		}
		atomic_add(sum, &osd->od_bytes_by_type[update->ou_type]);
		CDEBUG(D_OTHER, "op %d, size %d\n", update->ou_type, sum);
	}

	len = object_update_request_size(oh->ot_buf.ub_req);
	CDEBUG(D_OTHER, "buf %d\n", len);

	itx = zil_itx_create(TX_SYMLINK, sizeof(*lr) + len);
	if (itx == NULL) {
		CERROR("can't create itx with %d bytes\n", len);
		goto out;
	}

	lr = (struct lr_update *)&itx->itx_lr;
	lr->lr_oid = 1;
	buf = (char *)(lr + 1);
	memcpy(buf, oh->ot_buf.ub_req, len);

	atomic_inc(&osd->od_recs_in_log);
	atomic_add(len, &osd->od_bytes_in_log);

	itx->itx_callback = osd_zil_commit_cb;
	itx->itx_callback_data = itx;

	itx->itx_private = osd;
	itx->itx_sync = 1;

	atomic_inc(&osd->od_recs_to_write);

	LASSERT(osd->od_zilog != NULL);
	(void) zil_itx_assign(osd->od_zilog, itx, oh->ot_tx);

out:
	OBD_FREE_LARGE(oh->ot_buf.ub_req, oh->ot_buf.ub_req_size);
}

void osd_bitlock_alloc(struct osd_object *o)
{
	unsigned long *bitmap;

	if (o->oo_bitlock != NULL)
		return;

	OBD_ALLOC(bitmap, PAGE_SIZE);
	LASSERT(bitmap != NULL);

	write_lock(&o->oo_attr_lock);
	if (o->oo_bitlock == NULL) {
		init_waitqueue_head(&o->oo_bitlock_wait);
		o->oo_bitlock = bitmap;
	} else {
		OBD_FREE(bitmap, PAGE_SIZE);
	}
	write_unlock(&o->oo_attr_lock);
}

void osd_lock_offset(struct osd_object *o, loff_t offset, const char *f)
{
	unsigned long	block;

	osd_bitlock_alloc(o);

	block = offset >> SPA_MAXBLOCKSHIFT;
	LASSERT(block < PAGE_SIZE * 8); /* XXX */

	while (test_and_set_bit(block, o->oo_bitlock))
		wait_event(o->oo_bitlock_wait, !test_bit(block, o->oo_bitlock));

	LASSERT(test_bit(block, o->oo_bitlock));

}

void osd_unlock_offset(struct osd_object *o, loff_t offset)
{
	unsigned long	block;

	block = offset >> SPA_MAXBLOCKSHIFT;

	LASSERT(test_bit(block, o->oo_bitlock) != 0);
	clear_bit(block, o->oo_bitlock);
	wake_up(&o->oo_bitlock_wait);
}
