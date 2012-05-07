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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Implementation of cl_page for LOVSUB layer.
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
 * Lovsub page operations.
 *
 */

static void lovsub_page_fini(const struct lu_env *env,
                             struct cl_page_slice *slice)
{
        struct lovsub_page *lsb = cl2lovsub_page(slice);
        ENTRY;
        OBD_SLAB_FREE_PTR(lsb, lovsub_page_kmem);
        EXIT;
}

static const struct cl_page_operations lovsub_page_ops = {
        .cpo_fini   = lovsub_page_fini
};

struct cl_page *lovsub_page_init(const struct lu_env *env,
                                 struct cl_object *obj,
                                 struct cl_page *page, cfs_page_t *unused)
{
        struct lovsub_page *lsb;
        int result;

        ENTRY;
        OBD_SLAB_ALLOC_PTR_GFP(lsb, lovsub_page_kmem, CFS_ALLOC_IO);
        if (lsb != NULL) {
                cl_page_slice_add(page, &lsb->lsb_cl, obj, &lovsub_page_ops);
                result = 0;
        } else
                result = -ENOMEM;
        RETURN(ERR_PTR(result));
}

/** @} lov */
