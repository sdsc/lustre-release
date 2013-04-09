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

#ifndef OST_INTEGRITY_H
#define OST_INTEGRITY_H

int ost_integrity_prep(int rw, struct ptlrpc_request *req,
		       int npages, struct integrity *integrity);

int ost_integrity_commit(int rw, struct ptlrpc_request *req,
			 long npages, struct integrity *integrity);

int ost_integrity_pack_bulk(struct ptlrpc_request *req,
			    struct ptlrpc_bulk_desc *desc,
			    long npages,
			    struct integrity *integrity);

int ost_integrity_alloc(struct integrity *integrity);
void ost_integrity_free(struct integrity *integrity);

static inline int ost_integrity_size(int page_count, int ichunk_size,
				     struct integrity *integrity)
{
	if (integrity->type != OBD_CKSUM_T10B)
		return 0;

	return page_count * (CFS_PAGE_SIZE / ichunk_size) *
			    sizeof(struct obd_integrity_dif_tuple);
}

static inline int ost_integrity_bulk(struct ptlrpc_request *req, int page_count,
				     struct integrity *integrity)
{
	__u32 int_chunks, len = sizeof(int_chunks);

	if (integrity->type != OBD_CKSUM_T10B)
		return 0;

	obd_get_info(req->rq_svc_thread->t_env, req->rq_export,
		     sizeof(KEY_INTEGRITY_CHUNKS),
		     KEY_INTEGRITY_CHUNKS, &len, &int_chunks, NULL);

	return (ost_integrity_size(page_count, CFS_PAGE_SIZE/int_chunks,
				    integrity) +
		CFS_PAGE_SIZE - 1) >> CFS_PAGE_SHIFT;
}

static inline int ost_integrity_init(struct integrity *integrity,
				     obd_valid valid)
{
	switch (valid & (OBD_FL_CKSUM_T10A|OBD_FL_CKSUM_T10B)) {
	case 0:
		integrity->type = 0;
		break;
	case OBD_FL_CKSUM_T10A:
		integrity->type = OBD_CKSUM_T10A;
		break;
	case OBD_FL_CKSUM_T10B:
		integrity->type = OBD_CKSUM_T10B;
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

#endif /* OST_INTEGRITY_H */
