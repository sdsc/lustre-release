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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Implementation of cl_page for LOV layer.
 *
 *   Author: Nikita Danilov <nikita.danilov@sun.com>
 */

#define DEBUG_SUBSYSTEM S_LOV

#include "lov_cl_internal.h"

/** \addtogroup lov
 *  @{
 */

/*****************************************************************************
 *
 * Lov page operations.
 *
 */

static void lov_page_fini(const struct lu_env *env,
                          struct cl_page_slice *slice)
{
}

static int lov_page_print(const struct lu_env *env,
                          const struct cl_page_slice *slice,
                          void *cookie, lu_printer_t printer)
{
        struct lov_page *lp = cl2lov_page(slice);

        return (*printer)(env, cookie, LUSTRE_LOV_NAME"-page@%p\n", lp);
}

static const struct cl_page_operations lov_page_ops = {
	.cpo_fini   = lov_page_fini,
        .cpo_print  = lov_page_print
};

static void lov_empty_page_fini(const struct lu_env *env,
                                struct cl_page_slice *slice)
{
        LASSERT(slice->cpl_page->cp_child == NULL);
}

int lov_page_init_raid0(const struct lu_env *env, struct cl_object *obj,
			struct cl_page *page, pgoff_t index, cfs_page_t *vmpage)
{
        struct lov_object *loo = cl2lov(obj);
        struct lov_layout_raid0 *r0 = lov_r0(loo);
	struct lu_object_header *hdr;
        struct cl_object  *subobj;
        struct cl_object  *o;
        struct lov_page   *lpg = cl_object_page_slice(obj, page);
        loff_t             offset;
        obd_off            suboff;
        int                stripe;
        int                rc;
        ENTRY;

        offset = cl_offset(obj, index);
	stripe = lov_stripe_number(loo->lo_lsm, offset);
	LASSERT(stripe < r0->lo_nr);
	rc = lov_stripe_offset(loo->lo_lsm, offset, stripe,
                                   &suboff);
        LASSERT(rc == 0);

        cl_page_slice_add(page, &lpg->lps_cl, obj, &lov_page_ops);

        subobj = lovsub2cl(r0->lo_sub[stripe]);
	hdr = subobj->co_lu.lo_header;
	cfs_list_for_each_entry(o, &subobj->co_lu.lo_header->loh_layers,
				co_lu.lo_linkage) {
		if (o->co_ops->coo_page_init != NULL) {
			rc = o->co_ops->coo_page_init(env, o,
					page, cl_index(subobj, suboff), vmpage);
			if (rc != 0)
				break;
		}
	}

        RETURN(rc);
}


static const struct cl_page_operations lov_empty_page_ops = {
        .cpo_fini   = lov_empty_page_fini,
        .cpo_print  = lov_page_print
};

int lov_page_init_empty(const struct lu_env *env, struct cl_object *obj,
			struct cl_page *page, pgoff_t index, cfs_page_t *vmpage)
{
        struct lov_page *lpg = cl_object_page_slice(obj, page);
	void *addr;
        ENTRY;

	cl_page_slice_add(page, &lpg->lps_cl, obj, &lov_empty_page_ops);
	addr = cfs_kmap(vmpage);
	memset(addr, 0, cl_page_size(obj));
	cfs_kunmap(vmpage);
	cl_page_export(env, page, 1);
        RETURN(0);
}


/** @} lov */

