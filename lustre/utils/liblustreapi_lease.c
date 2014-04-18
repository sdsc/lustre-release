/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2014 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * lustre/utils/liblustreapi_lease.c
 *
 * lustreapi library for file leases
 *
 * Author: Henri Doreau <henri.doreau@cea.fr>
 */

#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"


static inline const char *lease_mode2str(int mode)
{
	switch (mode) {
	case F_WRLCK: return "WRITE";
	case F_RDLCK: return "READ";
	case F_UNLCK: return "UNLOCK";
	}
	return "???";
}

/**
 * Atomically put lease, swap layouts and close fd.
 *
 * \param fd	File to close.
 * \param fdv	File to swap layouts with.
 *
 * \reval 0 on success.
 * \retval -errno on error.
 */
int llapi_exclusive_close(int fd, int fdv)
{
	return llapi_fswap_layouts(fd, fdv, 0, 0, SWAP_LAYOUTS_EXCL_CLOSE);
}

/**
 * Get a lease on an open file.
 *
 * \param fd    File to get the lease on.
 * \param mode  Lease mode, either F_RDLCK or F_WRLCK.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_lease_get(int fd, int mode)
{
	int rc;

	if (mode != F_RDLCK && mode != F_WRLCK)
		return -EINVAL;

	rc = ioctl(fd, LL_IOC_SET_LEASE, mode);
	if (rc < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "cannot get %s lease",
			    lease_mode2str(mode));
	}
	return rc;
}

/**
 * Check if a lease is still set on a file.
 *
 * \param fd    File to check the lease on.
 *
 * \retval lease type if present (FMODE_READ or FMODE_WRITE).
 * \retval 0 if no lease is present.
 * \retval -errno on error.
 */
int llapi_lease_check(int fd)
{
	int rc;

	rc = ioctl(fd, LL_IOC_GET_LEASE);
	if (rc < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "cannot check lease");
	}
	return rc;
}

/**
 * Remove a lease.
 *
 * \param fd    File to remove the lease from.
 *
 * \retval type of the lease that was removed (FMODE_READ or FMODE_WRITE).
 * \retval 0 if no lease was present.
 * \retval -errno on error.
 */
int llapi_lease_put(int fd)
{
	int rc;

	rc = ioctl(fd, LL_IOC_SET_LEASE, F_UNLCK);
	if (rc < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "cannot put lease");
	}
	return rc;
}
