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

#include <libcfs/libcfs.h>

/* We use the pseudo-device created in libcfs as the sysfs device */
extern cfs_psdev_t libcfs_dev;

/* creates a sysfs file for our pseudo-device */
int
cfs_sysfs_create_file(struct device_attribute *file)
{
	return device_create_file(libcfs_dev.this_device, file);
}
EXPORT_SYMBOL(cfs_sysfs_create_file);

/* deletes file for our pseudo-device */
void
cfs_sysfs_remove_file(struct device_attribute *file)
{
	device_remove_file(libcfs_dev.this_device, file);
}
EXPORT_SYMBOL(cfs_sysfs_remove_file);
