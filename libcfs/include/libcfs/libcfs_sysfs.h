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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2012, Intel Corporation.
 */

#ifndef __LIBCFS_SYSFS_H__
#define __LIBCFS_SYSFS_H__

/* Don't let these inline routines appear in user space */
#ifdef __KERNEL__
#ifndef HAVE_LIBCFS_SYSFS

inline int
cfs_sysfs_create_file(void *file)
{
	return 0;
}

inline void
cfs_sysfs_remove_file(void *file)
{
}

#endif /* HAVE_LIBCFS_SYSFS */
#endif /* __KERNEL__ */
#endif /* __LIBCFS_SYSFS_H__ */
