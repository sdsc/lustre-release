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
 * This file contains implementation of T10 routines for the OSC layer.
 * At this layer we do the following:
 *
 *  1.  Calculate CRCs and put them in the private page area on write(2):
 *      osc_page_integrity()
 *
 *  2.  CRCs are packed when the actual write request is sent to the OST:
 *      osc_integrity_prep_brw()
 *  2a. T10A: u16 guard tag array for each page is packed in
 *            RMF_INTEGRITY_T10_INPILL
 *  2b. T10B: t10 tuple array for each page is packed in the first part
 *            of the bulk, data goes next
 *
 *  3.  CRCs are unpacked from an RPC pill or a bulk and verified
 *      when the actual read request is completed: osc_integrity_fini_brw()
 *  3a. T10A: u16 guard tag array for each page is unpacked from
 *            RMF_INTEGRITY_T10_INBULK, data pages are verified against
 *  3b. T10B: t10 tuple array for each page is packed in the first part
 *            of the bulk, data goes next, data pages are verified against
 */

#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <lustre_dlm.h>
#include <lustre_net.h>
#include <obd_ost.h>
#include <lustre_log.h>
#include <lustre_debug.h>
#include <lustre_param.h>
#include "osc_internal.h"
#include "osc_cl_internal.h"
#include "osc_integrity.h"

#ifdef __KERNEL__

#include <linux/crc-t10dif.h>

#else

static __u16 crc_t10dif(char *buffer, size_t len)
{
	/* XXX: should be copied from kernel or implemented in other way */
	LASSERT(1);
	return 0;
}
#endif

static int osc_t10_prep_inpill(int cmd, struct ost_body *body,
				struct ptlrpc_request *req,
				struct brw_page **pga,
				obd_count page_count, int chunk_size)
{
	int i, j, chunks;
	__u16 *crc;

	LASSERT(chunk_size != 0);

	/* Signal integrity capability for reads and writes */
	body->oa.o_flags |= OBD_MD_FLCKSUM;
	body->oa.o_valid |= OBD_FL_CKSUM_T10A;

	if (cmd == OBD_BRW_READ)
		return 0;

	chunks = CFS_PAGE_SIZE / chunk_size;

	crc = req_capsule_client_get(&req->rq_pill, &RMF_INTEGRITY_T10_INPILL);
	LASSERT(crc != NULL);

	for (i = 0; i < page_count; i++)
		for (j = 0; j < chunks; j++)
			*(crc++) = pga[i]->integrity.t10.integrity[j];

	return 0;
}

static int osc_t10_prep_inbulk(int cmd, struct ost_body *body,
				struct ptlrpc_request *req,
				struct brw_page **pga,
				obd_count page_count, int chunk_size,
				cfs_page_t ***integrity,
				struct ptlrpc_bulk_desc *desc,
				int *nob)
{
	struct obd_integrity_dif_tuple *crc = NULL;
	unsigned chunks;
	int i, j, np = 0;
	cfs_page_t **intg;

	LASSERT(chunk_size != 0);

	chunks = CFS_PAGE_SIZE / chunk_size;

	/* Signal integrity capability for reads and writes */
	body->oa.o_valid |= OBD_MD_FLCKSUM;
	body->oa.o_flags |= OBD_FL_CKSUM_T10B;

	np = (page_count * chunks * sizeof(struct obd_integrity_dif_tuple) +
		CFS_PAGE_SIZE - 1) >> CFS_PAGE_SHIFT;

	OBD_ALLOC(intg, (np + 1) * sizeof(cfs_page_t *));
	if (intg == NULL) {
		CERROR("no memory for integrity vector (%d)\n", np);
		return -ENOMEM;
	}

	*integrity = intg;

	for (i = 0; i < page_count; i++) {
		for (j = 0; j < chunks; j++) {
			if (!((long)crc & ~CFS_PAGE_MASK)) {
				*intg = cfs_alloc_page(CFS_ALLOC_STD);
				if (*intg == NULL) {
					CERROR("page alloc failed\n");
					goto out_nomem;
				}

				crc = (void *)cfs_page_address(*intg);

				intg++;
			}
			if (cmd == OBD_BRW_WRITE)
				crc->guard_tag =
					pga[i]->integrity.t10.integrity[j];
			crc++;
		}
	}

	*intg = NULL;

	for (intg = *integrity; *intg; intg++) {
		int crc_bytes = (long)crc & ~CFS_PAGE_MASK;

		if (intg[1] || crc_bytes == 0)
			crc_bytes = CFS_PAGE_SIZE;
		ptlrpc_prep_bulk_page_pin(desc, *intg, 0, crc_bytes);
		*nob += crc_bytes;
	}

	return 0;

out_nomem:

	for (intg--; intg >= *integrity; intg--)
		cfs_free_page(*intg);

	OBD_FREE(*integrity, (np + 1) * sizeof(cfs_page_t *));

	return -ENOMEM;
}

/**
 * Fill out an integrity capsule/bulk for a brw request for methods A/B.
 *
 * \param req      [in,out] : the request into which to pack PI
 * \param cmd          [in] : OBD_BRW_READ or OBD_BRW_WRITE
 * \param integrity_fl [in] : INTEGRITY_T10_{INPILL/INBULK}
 * \param chunk_size   [in] : integrity chunk size in bytes
 * \param pga          [in] : pages with PI
 * \param page_count   [in] : number of pages
 * \param body        [out] : body
 * \param integrity   [out] : integrity pages for protocol B
 * \param desc        [out] : bulk descriptor
 * \param nob         [out] : [additonal] number of bytes to transfer
 *
 * \retval       0 : success
 * \retval -ENOMEM : not enough memory
 */
int osc_integrity_prep_brw(struct ptlrpc_request *req,
			   int cmd, long integrity_fl, int chunk_size,
			   struct brw_page **pga, obd_count page_count,
			   struct ost_body *body, cfs_page_t ***integrity,
			   struct ptlrpc_bulk_desc *desc, int *nob)
{
	int rc = 0;

	CDEBUG(D_PAGE, "%s: cmd=%d flag=%ld chunk=%d pga=%p cnt=%d b=%p\n",
		req->rq_import->imp_obd->obd_name, cmd, integrity_fl,
		chunk_size, pga, (int)page_count, body);

	if (integrity_fl == OBD_CKSUM_T10A) {
		rc = osc_t10_prep_inpill(cmd, body, req, pga, page_count,
					 chunk_size);
	} else if (integrity_fl == OBD_CKSUM_T10B) {
		rc = osc_t10_prep_inbulk(cmd, body, req, pga, page_count,
					 chunk_size, integrity, desc, nob);
	}

	return rc;
}

static int osc_t10_fini_inpill(struct ptlrpc_request *req,
				int cmd, int chunk_size,
				struct brw_page **pga, obd_count page_count)
{
	int chunks, i, j, crcnum;
	__u16 *crc;

	if (cmd != OBD_BRW_READ)
		return 0;

	chunks = CFS_PAGE_SIZE / chunk_size;
	crcnum = page_count * chunks;
	crc = req_capsule_server_sized_get(&req->rq_pill,
					   &RMF_INTEGRITY_T10_INPILL,
					   crcnum * sizeof(__u16));
	if (crc == NULL) {
		CERROR("%s: no integrity pill on brw read fini\n",
			req->rq_import->imp_obd->obd_name);
		return -EPROTO;
	}

	for (i = 0; i < page_count; i++) {
		char *data = cfs_kmap(pga[i]->pg);
		__u16 *c = pga[i]->integrity.t10.integrity;

		for (j = 0; j < chunks; j++, data += chunk_size) {
			__u16 crct = cpu_to_be16(crc_t10dif(data, chunk_size));
			if (*crc != crct) {
				cfs_kunmap(pga[i]->pg);
				CERROR("%s: wrong crc idx=%ld chunk=%d "
					"(real:%lx, exp:%lx)\n",
					req->rq_import->imp_obd->obd_name,
					pga[i]->pg->index, j,
					(long)crct, (long)*crc);
				return -EAGAIN;
			}
			*c = *crc;
			crc++; c++;
		}

		cfs_kunmap(pga[i]->pg);
	}

	return 0;
}

static int osc_t10_fini_inbulk(struct ptlrpc_request *req,
				int cmd, int chunk_size,
				struct brw_page **pga, obd_count page_count,
				cfs_page_t **integrity)
{
	struct obd_integrity_dif_tuple *crc;
	int chunks, i, j;
	cfs_page_t **i2;

	chunks = CFS_PAGE_SIZE / chunk_size;

	/* XXX: Can this happen? */
	if (integrity == NULL)
		return 0;

	i2 = integrity; crc = NULL;
	/* Check and copy CRCs for READ */
	if (cmd != OBD_BRW_READ)
		goto free_pages;

	for (i = 0; i < page_count; i++) {
		char *data = cfs_kmap(pga[i]->pg);
		for (j = 0; j < chunks; j++) {
			__u16 tag;

			if (!((long)crc & ~CFS_PAGE_MASK)) {
				crc = (void *)cfs_page_address(*i2);
				i2++;
			}

			tag = crc_t10dif(data + j*chunk_size, chunk_size);

			if (crc->guard_tag != cpu_to_be16(tag)) {
				cfs_kunmap(pga[i]->pg);
				CERROR("%s: wrong crc on fini, "
					"idx=%d chunk=%d (real:%x,exp:%x)\n",
					req->rq_import->imp_obd->obd_name,
					i, j, (unsigned)tag,
					(unsigned)crc->guard_tag);
				return -EAGAIN;
			}

			pga[i]->integrity.t10.integrity[j] = crc->guard_tag;
			crc++;
		}
		cfs_kunmap(pga[i]->pg);
	}

free_pages:
	/* Free the whole of integrity pages and the vector */
	i2 = integrity;
	while (*i2) {
		cfs_free_page(*i2);
		i2++;
	}
	OBD_FREE(integrity, (i2 - integrity + 1) * sizeof(*i2));

	return 0;
}

/**
 * Unpack an integrity capsule from a read reply for methods A, B,
 * free temporary pages (for protocol B).
 *
 * \param req          [in] : the request from which to extract PI
 * \param cmd          [in] : OBD_BRW_READ or OBD_BRW_WRITE
 * \param chunk_size   [in] : integrity chunk size in bytes
 * \param pga      [in,out] : pages with PI
 * \param page_count   [in] : number of pages
 * \param body         [in] : body
 * \param integrity   [out] : integrity pages for protocol B
 *
 * \retval       0 : success
 * \retval -EAGAIN : crc mismatch
 */

int osc_integrity_fini_brw(struct ptlrpc_request *req,
			   int cmd, int chunk_size,
			   struct brw_page **pga, obd_count page_count,
			   struct ost_body *body, cfs_page_t **integrity)
{
	int rc = 0;

	CDEBUG(D_PAGE, "%s: osc_brw_fini_integrity count=%lu\n",
		req->rq_import->imp_obd->obd_name, (long)page_count);

	if (body->oa.o_flags & OBD_FL_CKSUM_T10A)
		rc = osc_t10_fini_inpill(req, cmd, chunk_size, pga, page_count);
	else if (body->oa.o_flags & OBD_FL_CKSUM_T10B)
		rc = osc_t10_fini_inbulk(req, cmd, chunk_size, pga, page_count,
					 integrity);

	return rc;
}

/**
 * Prepare PI for an updated page cache page.
 *
 * \param exp          [in] : export
 * \param ops      [in,out] : page cache page
 *
 * \retval       0 : success
 */
static int osc_attach_integrity(struct obd_export *exp,
				struct osc_page *ops)
{
	struct client_obd *cli = &exp->exp_obd->u.cli;
	struct obd_connect_data *ocd = &cli->cl_import->imp_connect_data;
	unsigned chunks, chunk_size, i;
	char *data;

	if (!(cli->cl_checksum && cli->cl_cksum_type & OBD_CKSUM_T10AB))
		return 0;

	chunk_size = 1 << ocd->ocd_ichunk_size;
	chunks = CFS_PAGE_SIZE >> ocd->ocd_ichunk_size;

	CDEBUG(D_PAGE, "%s: attaching integrity, chunks %u size %u osc_page %p\n",
		cli->cl_import->imp_obd->obd_name, chunks, chunk_size, ops);

	data = cfs_kmap(ops->ops_oap.oap_page);
	for (i = 0; i < chunks; i++, data += chunk_size) {
		__u16 crc = crc_t10dif(data, chunk_size);
		ops->ops_integrity.t10.t10_grd[i] = cpu_to_be16(crc);
	}
	cfs_kunmap(ops->ops_oap.oap_page);

	return 0;
}

void osc_page_integrity(const struct lu_env *env,
				const struct cl_page_slice *slice)
{
	struct osc_page		*opg = cl2osc_page(slice);
	struct osc_object	*obj = cl2osc(opg->ops_cl.cpl_obj);

	osc_attach_integrity(osc_export(obj), opg);
}

void osc_integrity_prep_pill(struct req_capsule *pill, int opc, int page_count,
			     int ichunk_size, int flag) {
	int capsule;

	if (flag != OBD_CKSUM_T10A)
		return;

	capsule = page_count * sizeof(__u16) * CFS_PAGE_SIZE / ichunk_size;

	req_capsule_set_size(pill, &RMF_INTEGRITY_T10_INPILL,
			   (opc == OST_READ) ? RCL_SERVER : RCL_CLIENT,
			    capsule);
}
