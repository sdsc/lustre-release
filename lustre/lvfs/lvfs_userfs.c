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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/lvfs/lvfs_userfs.c
 *
 * Lustre filesystem abstraction routines
 *
 * Author: Andreas Dilger <adilger@clusterfs.com>
 */

#include <liblustre.h>
#include <lvfs.h>
#include "lvfs_internal.h"

#include <obd.h>
#include <lustre_lib.h>

/* XXX currently ctxt functions should not be used ?? */
void push_ctxt(struct lvfs_run_ctxt *save, struct lvfs_run_ctxt *new_ctx,
               struct lvfs_ucred *uc)
{
        LBUG();
}

void pop_ctxt(struct lvfs_run_ctxt *saved, struct lvfs_run_ctxt *new_ctx,
              struct lvfs_ucred *uc)
{
        LBUG();
}
