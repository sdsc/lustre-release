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

#ifndef OSC_INTEGRITY_H
#define OSC_INTEGRITY_H

struct osc_integrity_t10 {
	__u16	t10_grd[CFS_PAGE_SIZE >> 9];
};

union osc_integrity {
	struct osc_integrity_t10	t10;
};

static inline int osc_integrity_size(int page_count, int ichunk_size, int proto)
{
	if (proto != INTEGRITY_T10_INBULK)
		return 0;

	return page_count * (CFS_PAGE_SIZE / ichunk_size) *
			    sizeof(struct obd_integrity_dif_tuple);
}

static inline void osc_integrity_to_oap(struct obd_export *exp,
					struct osc_async_page *oap,
					union osc_integrity *crc)
{
	struct client_obd *cli = &exp->exp_obd->u.cli;
	struct obd_connect_data *ocd = &cli->cl_import->imp_connect_data;

	if (OCD_HAS_FLAG(ocd, INTEGRITY) &&
	    (ocd->ocd_integrity & (INTEGRITY_T10_INPILL | INTEGRITY_T10_INBULK)))
		oap->oap_integrity.t10.integrity = crc->t10.t10_grd;
}

static inline int osc_integrity_bulk(int page_count, int ichunk_size, int proto)
{
	if (proto != INTEGRITY_T10_INBULK)
		return 0;

	return ((osc_integrity_size(page_count, ichunk_size, proto) +
		CFS_PAGE_SIZE - 1) >> CFS_PAGE_SHIFT);
}

int osc_integrity_prep_brw(struct ptlrpc_request *req,
			   int cmd, long integrity_fl, int chunk_size,
			   struct brw_page **pga, obd_count page_count,
			   struct ost_body *body, cfs_page_t ***integrity,
			   struct ptlrpc_bulk_desc *desc, int *nob);

int osc_integrity_fini_brw(struct ptlrpc_request *req,
			   int cmd, int chunk_size,
			   struct brw_page **pga, obd_count page_count,
			   struct ost_body *body, cfs_page_t **integrity);

void osc_page_integrity(const struct lu_env *env,
			   const struct cl_page_slice *slice);

void osc_integrity_prep_pill(struct req_capsule *pill, int opc,
			     int page_count, int ichunk_size, int flag);

#endif /* OSC_INTEGRITY_H */
