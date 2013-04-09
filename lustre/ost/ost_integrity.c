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

/*
 * This file contains implementation of T10 routines for the OST layer.
 * At this layer we do the following:
 *
 *  1.  For each OSS thread we allocate a large enough integrity array for use
 *      in the same format as expected by Linux integrity API: (grd,ref,app)
 *
 *  2.  For each write request integrity data is unpacked: ost_integrity_prep()
 *  2a. T10A: RMF_INTEGRITY_T10_INPILL is unpacked in the integrity buffer
 *  2b. T10B: integrity data is transferred directly to the integrity buffer
 *
 *  3.  For each read request integrity data is packed in the reply,
 *      ost_integrity_commit()
 *  3a. T10A: RMF_INTEGRITY_T10_INPILL is filled out from the integrity buffer
 *  3b. T10B: integrity data is transferred directly from the integrity buffer
 */

#define DEBUG_SUBSYSTEM S_OST

#include <linux/module.h>
#include <obd_ost.h>
#include <lustre_net.h>
#include <lustre_dlm.h>
#include <lustre_export.h>
#include <lustre_debug.h>
#include <linux/init.h>
#include <lprocfs_status.h>
#include <libcfs/list.h>
#include "ost_internal.h"
#include "ost_integrity.h"

/**
 * Fill out integrity pages from WRITE RPC for method A, prepare for READ.
 *
 * \param rw           [in] : OBD_BRW_READ or OBD_BRW_WRITE
 * \param req          [in] : the request from which to extract PI
 * \param oi          [out] : integrity buffers into which to extract PI (WRITE)
 * \param npages       [in] : number of pages
 *
 * \retval       0 : success (extracted or nop)
 * \retval -EPROTO : integrity data are not properly packed in the request
 */
int ost_integrity_prep(int rw, struct ptlrpc_request *req,
		       int npages, struct integrity *integrity)
{
	struct obd_integrity_dif_tuple *tpl;
	int i, crcnum;
	__u16 *crc;

	LASSERT(rw == OBD_BRW_READ || rw == OBD_BRW_WRITE);
	LASSERT(req != NULL);

	if (integrity->type != OBD_CKSUM_T10A)
		return 0;

	tpl = integrity->oi.t10.oi_tpl;

	if (rw == OBD_BRW_READ) {
		__u32 int_chunks, len = sizeof(int_chunks);
		int rc;

		rc = obd_get_info(req->rq_svc_thread->t_env, req->rq_export,
				  sizeof(KEY_INTEGRITY_CHUNKS),
				  KEY_INTEGRITY_CHUNKS, &len,
				  &int_chunks, NULL);
		if (rc != 0)
			return -EPROTO;

		CDEBUG(D_PAGE, "prep integrity pages=%d chunks=%d\n",
			npages, (int)int_chunks);

		integrity->oi.t10.oi_tuples = npages * int_chunks;

		req_capsule_set_size(&req->rq_pill,
				     &RMF_INTEGRITY_T10_INPILL,
				     RCL_SERVER,
				     sizeof(__u16)*integrity->oi.t10.oi_tuples);
		return 0;
	}

	crcnum = req_capsule_get_size(&req->rq_pill,
				      &RMF_INTEGRITY_T10_INPILL,
				      RCL_CLIENT) / sizeof(__u16);

	crc = req_capsule_client_get(&req->rq_pill,
				     &RMF_INTEGRITY_T10_INPILL);
	if (crc == NULL || crcnum > T10_MAX_TUPLES_PER_BULK) {
		CERROR("%s: wrong crc number %d in the integrity capsule\n",
			req->rq_export->exp_obd->obd_name, crcnum);
		return -EPROTO;
	}

	for (i = 0; i < crcnum; i++, tpl++, crc++)
		tpl->guard_tag = *crc;

	integrity->oi.t10.oi_tuples = crcnum;

	return 0;
}

/**
 * Pack integrity data in READ RPC for method A.
 *
 * \param rw           [in] : OBD_BRW_READ or OBD_BRW_WRITE
 * \param req         [out] : the request into which to extract PI
 * \param npages       [in] : the number of data pages in the RPC
 * \param oi           [in] : integrity buffers from which to extract PI
 *
 * \retval       0 : success (extracted or nop)
 * \retval -EPROTO : integrity data cannot be packed in the request
 */
int ost_integrity_commit(int rw, struct ptlrpc_request *req,
			 long npages, struct integrity *integrity)
{
	__u32 int_chunks, len = sizeof(int_chunks);
	struct obd_integrity_dif_tuple *tpl;
	struct ost_body *repbody;
	int i, rc;
	__u16 *crc;

	LASSERT(rw == OBD_BRW_READ || rw == OBD_BRW_WRITE);
	LASSERT(req != NULL);

	repbody = req_capsule_server_get(&req->rq_pill, &RMF_OST_BODY);
	if (repbody == NULL) {
		CERROR("%s: no body pill\n", req->rq_export->exp_obd->obd_name);
		return -EPROTO;
	}

	if (integrity->type == OBD_CKSUM_T10A) {
		repbody->oa.o_flags |= OBD_FL_CKSUM_T10A;
		repbody->oa.o_valid |= OBD_MD_FLCKSUM;
	} else if (integrity->type == OBD_CKSUM_T10B) {
		repbody->oa.o_flags |= OBD_FL_CKSUM_T10B;
		repbody->oa.o_valid |= OBD_MD_FLCKSUM;
	}

	if ((integrity->type != OBD_CKSUM_T10A) || (rw != OBD_BRW_READ))
		return 0;

	tpl = integrity->oi.t10.oi_tpl;

	rc = obd_get_info(req->rq_svc_thread->t_env, req->rq_export,
			  sizeof(KEY_INTEGRITY_CHUNKS),
			  KEY_INTEGRITY_CHUNKS, &len, &int_chunks, NULL);
	if (rc != 0)
		return -EPROTO;

	req_capsule_shrink(&req->rq_pill, &RMF_INTEGRITY_T10_INPILL,
			   sizeof(__u16) * npages * int_chunks, RCL_SERVER);

	crc = req_capsule_server_get(&req->rq_pill, &RMF_INTEGRITY_T10_INPILL);
	if (crc == NULL) {
		CERROR("%s: no integrity pill\n",
			req->rq_export->exp_obd->obd_name);
		return -EPROTO;
	}

	for (i = 0; i < npages * int_chunks; i++, tpl++, crc++)
		*crc = tpl->guard_tag;

	return 0;
}

/**
 * Prepare integrity pages for bulk, method B.
 *
 * \param req        [in] : the RPC request
 * \param desc      [out] : bulk descriptor to which to add int pages
 * \param npages     [in] : the number of data pages in the RPC request
 * \param oi     [in,out] : integrity data buffer
 *
 * \retval       0 : success
 * \retval -EPROTO : protocol failure
 */

int ost_integrity_pack_bulk(struct ptlrpc_request *req,
			    struct ptlrpc_bulk_desc *desc,
			    long npages,
			    struct integrity *integrity)
{
	__u32 int_chunks, len = sizeof(int_chunks);
	unsigned long start, end;
	int rc;

	LASSERT(req != NULL);
	LASSERT(desc != NULL);
	LASSERT(integrity != NULL);
	LASSERT(((((unsigned long)integrity->oi.t10.oi_tpl) & ~CFS_PAGE_MASK) %
		sizeof(struct obd_integrity_dif_tuple)) == 0);
	CLASSERT(CFS_PAGE_SIZE % sizeof(struct obd_integrity_dif_tuple) == 0);

	rc = obd_get_info(req->rq_svc_thread->t_env, req->rq_export,
			  sizeof(KEY_INTEGRITY_CHUNKS),
			  KEY_INTEGRITY_CHUNKS, &len, &int_chunks, NULL);
	if (rc != 0)
		return -EPROTO;

	integrity->oi.t10.oi_tuples = npages * int_chunks;
	len = integrity->oi.t10.oi_tuples * sizeof(struct obd_integrity_dif_tuple);

	start = (unsigned long)integrity->oi.t10.oi_tpl;
	end = start + len;

	while (start < end) {
		unsigned long eop, llen;

		eop = (start & CFS_PAGE_MASK) + CFS_PAGE_SIZE;
		llen = eop < end ? eop - start : end - start;
		ptlrpc_prep_bulk_page_pin(desc, vmalloc_to_page((void *)start),
					  start & ~CFS_PAGE_MASK, llen);
		start = eop;
	}

	return 0;
}

int ost_integrity_alloc(struct integrity *integrity)
{
	/* Use vmalloc() so that we can use vmalloc_to_page() which
	 * does not work with kmalloc() */
	OBD_VMALLOC(integrity->oi.t10.oi_tpl,
		sizeof(struct obd_integrity_dif_tuple)*T10_MAX_TUPLES_PER_BULK);

	return integrity->oi.t10.oi_tpl ? 0 : -ENOMEM;
}

void ost_integrity_free(struct integrity *integrity)
{
	OBD_VFREE(integrity->oi.t10.oi_tpl,
		sizeof(struct obd_integrity_dif_tuple)*T10_MAX_TUPLES_PER_BULK);
}
