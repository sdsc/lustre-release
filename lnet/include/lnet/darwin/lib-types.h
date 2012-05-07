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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef __LNET_DARWIN_LIB_TYPES_H__
#define __LNET_DARWIN_LIB_TYPES_H__

#ifndef __LNET_LIB_TYPES_H__
#error Do not #include this file directly. #include <lnet/lib-types.h> instead
#endif

#include <sys/types.h>
#include <libcfs/libcfs.h>
#include <libcfs/list.h>

/*
 * XXX Liang:
 *
 * Temporary fix, because lnet_me_free()->cfs_free->FREE() can be blocked in xnu,
 * at then same time we've taken LNET_LOCK(), which is a spinlock.
 * by using LNET_USE_LIB_FREELIST, we can avoid calling of FREE().
 *
 * A better solution is moving lnet_me_free() out from LNET_LOCK, it's not hard
 * but need to be very careful and take some time.
 */
#define LNET_USE_LIB_FREELIST

#endif
