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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * Please  visit http://www.xyratex.com/contact if you need additional
 * information or have any questions.
 *
 * GPL HEADER END
 */

/*
 * Copyright 2012 Xyratex Technology Limited
 *
 * Author: Andrew Perepechko <Andrew_Perepechko@xyratex.com>
 *
 */

#include <linux/module.h>
#include <linux/fs.h>
#include <linux/dcache.h>
#include <linux/init.h>
#include <linux/version.h>
#include <linux/sched.h>
#include <linux/mount.h>
#include <linux/buffer_head.h>

#include <obd_cksum.h>
#include <obd_class.h>
#include <obd_lov.h>
#include <lustre_dlm.h>
#include <lustre_fsfilt.h>
#include <lprocfs_status.h>
#include <lustre_log.h>
#include <libcfs/list.h>
#include <lustre_disk.h>
#include <lustre_quota.h>
#include <linux/slab.h>
#include <lustre_param.h>
#include "osd_internal.h"
#include "osd_integrity.h"

#include <linux/crc-t10dif.h>

#define MAX_ITAGS_PER_PAGE (CFS_PAGE_SIZE / 512)

/**
 * Check if DIF/DIX is supported and save the number of integrity
 * chunks per block/page.
 *
 * \param osd    [out]: save int chunks in ->t10_chunks (0 for no support)
 * \param bdev    [in]: underlying block device
 *
 * \retval 0: success
 */
int osd_setup_integrity(struct osd_device *osd, struct block_device *bdev)
{
	struct osd_integrity *oi = &osd->od_integrity;
	struct blk_integrity *bi = bdev->bd_disk->integrity;

	memset(oi, 0, sizeof(*oi));

	/* We want Type 1 or 2 and CRC (IP does not work for us) */
	if (bi && !strcmp(bi->name, "T10-DIF-TYPE1-CRC")) {
		struct osd_integrity_t10 *t10 = &oi->od_t10;

		LASSERT(bi->sector_size != 0);

		t10->t10_chunks = CFS_PAGE_SIZE / bi->sector_size;
		t10->t10_chunks_bits = ffz(~t10->t10_chunks);
		oi->od_type = OSD_INTEGRITY_T10;
	} else {
		oi->od_type = OSD_INTEGRITY_NONE;
	}

	return 0;
}

/**
 *  Check that protection information attached to the page matches its contents,
 *  copy PI in the integrity tuple.
 *
 *  \param osd           [in] : osd device
 *  \param page          [in] : the page to check integrity in
 *  \param integrity [in,out] : integrity type and vector
 *  \param n             [in] : page number in int vector in term of local pages
 *
 *  \retval       0 : success, PI is correct (or absent) and copied into tpl
 *  \retval -EINVAL : wrong PI or no PI attached to the page
 */
int osd_check_integrity(struct osd_device *osd, struct page *page,
			struct integrity *integrity, int n)
{
	__u16 *crc;
	void *data;
	int i, chunk, int_chunks;
	struct obd_integrity_dif_tuple *tpl;

	if (integrity == NULL)
		return 0;

	if (osd->od_integrity.od_type == OSD_INTEGRITY_NONE)
		return 0;

	LASSERT(osd->od_integrity.od_type == OSD_INTEGRITY_T10);

	if (integrity->type != INTEGRITY_T10_INBULK &&
	    integrity->type != INTEGRITY_T10_INPILL &&
	    integrity->type != INTEGRITY_NONE)
		return -EINVAL;

	int_chunks = osd->od_integrity.od_t10.t10_chunks;
	LASSERT(int_chunks != 0);

	tpl = integrity->oi.t10.oi_tpl;
	if (tpl == NULL)
		return 0;

	chunk = CFS_PAGE_SIZE / int_chunks;
	tpl += n * int_chunks;

	if (!PagePrivate2(page)) {
		CDEBUG(D_PAGE, "no integrity in the OST page\n");
		return -EINVAL;
	}

	crc = (__u16 *)page->private;

	data = kmap_atomic(page, KM_USER0);
	for (i = 0; i < int_chunks; i++, tpl++) {
		if (crc[i] != cpu_to_be16(crc_t10dif(data + i*chunk, chunk))) {
			CERROR("cache broken?\n");
			kunmap(page);
			return -EINVAL;
		}
		/* Is it needed for INTEGRITY_NONE? */
		tpl->guard_tag = crc[i];
	}
	kunmap_atomic(data, KM_USER0);

	return 0;
}

#define T10_DEBUG

/**
 * Attach new protection information to page cache pages.
 *
 * \param osd      : the osd device
 * \param local_nb : the page array to be updated
 * \param npages   : the number of pages in the array
 * \param oi       : integrity data to be attached
 *
 * \retval       0 : success
 * \retval -EPROTO : failed to extract integrity chunk size
 */
int osd_pull_integrity(struct osd_device *osd,
		       struct niobuf_local *local_nb,
		       int npages,
		       struct integrity *oi)
{
	struct obd_integrity_dif_tuple *tpl;
	struct osd_integrity_t10 *t10;
	int i, j;

	CDEBUG(D_PAGE, "npages = %d\n", npages);

	if (oi == NULL)
		return 0;

	if (osd->od_integrity.od_type == OSD_INTEGRITY_NONE)
		return 0;

	LASSERT(osd->od_integrity.od_type == OSD_INTEGRITY_T10);

	t10 = &osd->od_integrity.od_t10;
	tpl = oi->oi.t10.oi_tpl;

	LASSERT(t10->t10_chunks <= MAX_ITAGS_PER_PAGE);

	for (i = 0; i < npages; i++, tpl += t10->t10_chunks) {
		struct page *page = local_nb[i].page;
#ifdef T10_DEBUG
		char *data;
		int chunk = CFS_PAGE_SIZE >> t10->t10_chunks_bits;
#endif
		__u16 *tags;

		if (unlikely(page == NULL)) {
			/* short read case? */
			CDEBUG(D_PAGE, "no page idx=%d offset=%lu\n", i,
				(long)local_nb[i].lnb_file_offset);
			continue;
		}

		/* Not part of page cache, don't even try to attach integrity */
		if (page->mapping == NULL)
			continue;

		if (unlikely(page_has_buffers(page))) {
			int rc;

			/* private may be used for buffers if the page undergone
			 * partial truncation. I really hate that, but to fix
			 * things I would have to rewrite ldiskfs_truncate().
			 */

			CDEBUG(D_PAGE, "freeing buffers for page %p\n", page);

			rc = try_to_free_buffers(page);
			/* we have the page lock, so no one may be referencing
			 * page buffers, try_to_free_buffers() must succeed. */
			LASSERT(rc == 1);
		}

		tags = (__u16 *)page->private;
		if (tags == NULL) {
			tags = kmalloc(sizeof(__u16[MAX_ITAGS_PER_PAGE]),
				       GFP_KERNEL);
			page->private = (unsigned long)tags;
			SetPagePrivate2(page);
		}

#ifdef T10_DEBUG
		data = kmap_atomic(page, KM_USER0);
#endif

		for (j = 0; j < t10->t10_chunks; j++) {
#ifdef T10_DEBUG
			__u16 crc = crc_t10dif(data + j * chunk, chunk);
			if (cpu_to_be16(crc) != tpl[j].guard_tag)
				CERROR("wrong crc on save %ld (%x:%x,%p:%p)\n",
					page->index,
					(unsigned)cpu_to_be16(crc),
					(unsigned)tpl[j].guard_tag,
					data + j*chunk, &tpl[j].guard_tag);

#endif
			tags[j] = tpl[j].guard_tag;
		}
#ifdef T10_DEBUG
		kunmap_atomic(data, KM_USER0);
#endif
	}

	return 0;
}

/**
 * Calculate integrity for requests from non-T10 clients.
 *
 * \param osd      : the osd device
 * \param local_nb : the page array with data
 * \param npages   : the number of pages in the array
 * \param oi       : integrity data to be updated
 *
 * \retval       0 : success
 * \retval -EPROTO : failed to extract integrity chunk size
 */
int osd_reconstruct_integrity(struct osd_device *osd,
			      struct niobuf_local *local_nb,
			      int npages,
			      struct integrity *oi)
{
	struct obd_integrity_dif_tuple *tpl;
	int i, j;

	CDEBUG(D_PAGE, "npages = %d\n", npages);

	if (oi == NULL)
		return 0;

	/* Already have integrity? Then nothing to do */
	if (oi->type == INTEGRITY_T10_INBULK ||
	    oi->type == INTEGRITY_T10_INPILL)
		    return 0;

	if (osd->od_integrity.od_type == OSD_INTEGRITY_NONE)
		return 0;

	LASSERT(osd->od_integrity.od_type == OSD_INTEGRITY_T10);

	tpl = oi->oi.t10.oi_tpl;

	LASSERT(osd->od_integrity.od_t10.t10_chunks <= MAX_ITAGS_PER_PAGE);

	for (i = 0; i < npages; i++, tpl += osd->od_integrity.od_t10.t10_chunks) {
		struct page *page = local_nb[i].page;
		char *data;
		int chunk = CFS_PAGE_SIZE >> osd->od_integrity.od_t10.t10_chunks_bits;

		if (unlikely(page == NULL)) {
			CWARN("no page idx=%d offset=%lu\n", i,
				(long)local_nb[i].lnb_file_offset);
			continue;
		}

		data = kmap_atomic(page, KM_USER0);
		for (j = 0; j < osd->od_integrity.od_t10.t10_chunks; j++) {
			__u16 crc = crc_t10dif(data + j * chunk, chunk);
			tpl[j].guard_tag = cpu_to_be16(crc);
		}
		kunmap_atomic(data, KM_USER0);
	}

	return 0;
}

/**
 *  Attach integrity data to an I/O request.
 *
 *  \param osd      [in] : the osd device
 *  \param iobuf    [in] : iobuf with the integrity data
 *  \param bio      [in, out] : I/O request
 *  \param page_idx [in] : integrity data offset in the iobuf
 *  \param start    [in] : data start
 *  \param end      [in] : data end
 *  \param sector   [in] : starting sector (512-byte)
 *  \param hole     [in] : 1 if handling a hole
 *
 *  \retval 0 : success
 */
int osd_attach_integrity(struct osd_device *osd,
			 struct osd_iobuf *iobuf,
			 struct bio *bio, int page_idx,
			 unsigned start, unsigned end,
			 unsigned sector, int hole)
{
	struct obd_integrity_dif_tuple *tpl = iobuf->dr_integrity[page_idx];
	unsigned chunk_size;
	int n;

	/* Exit immediately if no integrity vector */
	if (tpl == NULL)
		return 0;

	if (iobuf->dr_dev->od_integrity.od_type == OSD_INTEGRITY_NONE)
		return 0;

	LASSERT(iobuf->dr_dev->od_integrity.od_type == OSD_INTEGRITY_T10);

	if (!hole && bio->bi_integrity == NULL)
		if (!bio_integrity_alloc(bio, GFP_NOIO, BIO_MAX_PAGES))
			return -ENOMEM;

	chunk_size = CFS_PAGE_SIZE >> osd->od_integrity.od_t10.t10_chunks_bits;

	start /= chunk_size;
	end   /= chunk_size;
	sector *= chunk_size >> 9;

	for (n = start; n < end; n++) {
		if (!hole) {
			tpl[n].ref_tag = cpu_to_be32(sector + (n - start));

			bio_integrity_add_page(bio, vmalloc_to_page(tpl + n),
					sizeof(struct obd_integrity_dif_tuple),
						((unsigned long)(tpl + n)) &
						(~CFS_PAGE_MASK));
		} else {
			tpl[n].guard_tag = 0; /* smart T10-CRC for a hole */
			/* don't fill ref_tag - not interested */
		}
	}

	return 0;
}

int osd_alloc_integrity(struct osd_iobuf *iobuf, int pages)
{
	lu_buf_realloc(&iobuf->dr_pg_int, pages *
				  MAX_BLOCKS_PER_PAGE *
				  sizeof(struct obd_integrity_dif_tuple *));
	iobuf->dr_integrity = iobuf->dr_pg_int.lb_buf;
	return iobuf->dr_integrity ? 0 : -ENOMEM;
}

void osd_iobuf_attach_integrity(struct osd_iobuf *iobuf, struct page *page,
			        struct integrity *oi, int n)
{
	struct osd_device *osd = iobuf->dr_dev;
	struct obd_integrity_dif_tuple **integrity = iobuf->dr_integrity;

	if (oi != NULL && osd->od_integrity.od_type == OSD_INTEGRITY_T10) {
		int int_chunk = osd->od_integrity.od_t10.t10_chunks;
		integrity[iobuf->dr_npages] = &oi->oi.t10.oi_tpl[n*int_chunk];
	} else {
		integrity[iobuf->dr_npages] = NULL;
	}
}
