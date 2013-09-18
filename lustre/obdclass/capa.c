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
 * Copyright (c) 2005, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/capa.c
 *
 * Lustre Capability Hash Management
 *
 * Author: Lai Siyao<lsy@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_SEC

#ifdef __KERNEL__
#include <linux/version.h>
#include <linux/fs.h>
#include <asm/unistd.h>
#include <linux/slab.h>
#include <linux/module.h>
#include <linux/init.h>

#include <obd_class.h>
#include <lustre_debug.h>
#include <lustre/lustre_idl.h>
#else
#include <liblustre.h>
#endif

#include <libcfs/list.h>
#include <lustre_capa.h>

/*
 * context key constructor/destructor:
 * lu_capainfo_key_init, lu_capainfo_key_fini
 */
LU_KEY_INIT_FINI(lu_capainfo, struct lu_capainfo);

struct lu_context_key lu_capainfo_key = {
	.lct_tags = LCT_SESSION,
	.lct_init = lu_capainfo_key_init,
	.lct_fini = lu_capainfo_key_fini
};

struct lu_capainfo *lu_capainfo_get(const struct lu_env *env)
{
	/* NB, in mdt_init0 */
	if (env->le_ses == NULL)
		return NULL;
	return lu_context_key_get(env->le_ses, &lu_capainfo_key);
}
EXPORT_SYMBOL(lu_capainfo_get);

/**
 * Initialization of lu_capainfo_key data.
 */
int lu_capainfo_init(void)
{
	int rc;

	LU_CONTEXT_KEY_INIT(&lu_capainfo_key);
	rc = lu_context_key_register(&lu_capainfo_key);
	return rc;
}

/**
 * Dual to lu_capainfo_init().
 */
void lu_capainfo_fini(void)
{
	lu_context_key_degister(&lu_capainfo_key);
}
