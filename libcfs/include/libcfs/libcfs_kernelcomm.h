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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Author: Nathan Rutman <nathan.rutman@sun.com>
 *
 * libcfs/include/libcfs/libcfs_kernelcomm.h
 *
 * Kernel <-> userspace communication routines.
 * The definitions below are used in the kernel and userspace.
 *
 */

#ifndef __LIBCFS_KERNELCOMM_H__
#define __LIBCFS_KERNELCOMM_H__

#ifndef __LIBCFS_LIBCFS_H__
#error Do not #include this file directly. #include <libcfs/libcfs.h> instead
#endif

#ifdef __KERNEL__

#include <uapi/kernel_comm.h>

/* prototype for callback function on kuc groups */
typedef int (*libcfs_kkuc_cb_t)(void *data, void *cb_arg);

/* Kernel methods */
extern int libcfs_kkuc_msg_put(struct file *fp, void *payload);
extern int libcfs_kkuc_group_put(int group, void *payload);
extern int libcfs_kkuc_group_add(struct file *fp, int uid, int group,
				 void *data);
extern int libcfs_kkuc_group_rem(int uid, int group, void **pdata);
extern int libcfs_kkuc_group_foreach(int group, libcfs_kkuc_cb_t cb_func,
				     void *cb_arg);

#endif

#endif /* __LIBCFS_KERNELCOMM_H__ */

