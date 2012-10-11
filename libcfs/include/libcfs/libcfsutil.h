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
 * libcfs/include/libcfs/libcfsutil.h
 *
 * A library used for userspace utilities.
 *
 */

#ifndef __LIBCFSUTIL_H__
#define __LIBCFSUTIL_H__

#ifndef LUSTRE_UTILS
#define LUSTRE_UTILS 1
#endif

#include <libcfs/libcfs.h>

#include <libcfs/util/platform.h>
#include <libcfs/util/parser.h>
#include <libcfs/util/libcfsutil_ioctl.h>

/* message severity level */
enum libcfs_message_level {
	LIBCFS_MSG_OFF    = 0,
	LIBCFS_MSG_FATAL  = 1,
	LIBCFS_MSG_ERROR  = 2,
	LIBCFS_MSG_WARN   = 3,
	LIBCFS_MSG_NORMAL = 4,
	LIBCFS_MSG_INFO   = 5,
	LIBCFS_MSG_DEBUG  = 6,
	LIBCFS_MSG_MAX
};

/* the bottom three bits reserved for libcfs_message_level */
#define LIBCFS_MSG_MASK		0x00000007
#define LIBCFS_MSG_NO_ERRNO	0x00000010

/*
 * Defined by libcfs/libcfs/util/util.c
 */
extern int libcfs_tcd_type_max(void);
extern void libcfs_msg_set_level(int level);
extern void libcfs_error(int level, int _rc, char *fmt, ...);
#define libcfs_err_noerrno(level, fmt, a...)                             \
	libcfs_error((level) | LIBCFS_MSG_NO_ERRNO, 0, fmt, ## a)
extern void libcfs_printf(int level, char *fmt, ...);
extern int libcfs_first_match(char *pattern, char *buffer);
extern int libcfs_get_param(const char *param_path, char *result,
			    unsigned int result_size);
extern int libcfs_set_param(const char *param_path, char *buffer,
			    unsigned int buffer_size);

#endif	/* __LIBCFSUTIL_H__ */
