/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright (c) 2015, Cray Inc, all rights reserved.
 *
 * Copyright (c) 2015, Intel Corporation.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * LGPL version 2.1 or (at your discretion) any later version.
 * LGPL version 2.1 accompanies this distribution, and is available at
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
 * lustre/utils/liblustreapi_util.c
 *
 * Misc LGPL-licenced utility functions for liblustreapi.
 *
 * Author: Frank Zago <fzago@cray.com>
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdbool.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <lustre/lustreapi.h>
#include <libcfs/util/string.h>	/* only needed for compat strlcpy() */
#include <lustre_ver.h>		/* only until LUSTRE_BUILD_VERSION is gone */
#include "lustreapi_internal.h"

/*
 * Indicate whether the liblustreapi_init() constructor below has run or not.
 *
 * This can be used by external programs to ensure that the initialization
 * mechanism has actually worked.
 */
bool liblustreapi_initialized;


/**
 * Initialize the library once at startup.
 *
 * Initializes the random number generator (random()). Get
 * data from different places in case one of them fails. This
 * is enough to get reasonably random numbers, but is not
 * strong enough to be used for cryptography.
 */
static __attribute__ ((constructor)) void liblustreapi_init(void)
{
	unsigned int	seed;
	struct timeval	tv;
	int		fd;

	seed = syscall(SYS_gettid);

	if (gettimeofday(&tv, NULL) == 0) {
		seed ^= tv.tv_sec;
		seed ^= tv.tv_usec;
	}

	fd = open("/dev/urandom", O_RDONLY | O_NOFOLLOW);
	if (fd >= 0) {
		unsigned int rnumber;
		ssize_t ret;

		ret = read(fd, &rnumber, sizeof(rnumber));
		seed ^= rnumber ^ ret;
		close(fd);
	}

	srandom(seed);
	liblustreapi_initialized = true;
}

/* The "version" file in /proc currently returns something like:
 * lustre: 2.6.92
 * kernel: patchless_client
 * build: v2_6_92_0-gadb3ee4-2.6.32-431.29.2.el6_lustre.g36cd22b.x86_64
 *
 * Extract only the named \a string from \a version from this file.
 */
static int get_version_string(const char *string, char *version,
			      int version_size)
{
	char buffer[4096];
	char *ptr;
	int rc;

	if (version == NULL) {
		errno = EINVAL;
		return -EINVAL;
	}

	rc = get_param("version", buffer, sizeof(buffer));

	ptr = strstr(buffer, string);
	if (ptr != NULL) {
		llapi_chomp_string(ptr);
		ptr += strlen(string);
		while (*ptr == ' ')
			ptr++;

		if (strlcpy(version, ptr, version_size) >= version_size) {
			errno = EOVERFLOW;
			return -EOVERFLOW;
		}
	} else {
		errno = ENODATA;
		return -ENODATA;
	}

	return 0;
}

/**
 * Return the release version for Lustre.
 *
 * This returns the tagged release version of Lustre, e.g. 2.6.92.
 *
 * \param version[in,out]	buffer to store build version string
 * \param version_size[in]	size of \a version
 *
 * \retval			0 on success
 * \retval			-1 on failure, errno set
 */
int llapi_get_release_version(char *version, int version_size)
{
	return get_version_string("lustre:", version, version_size);
}

/**
 * Return the build version for Lustre.
 *
 * This returns the specific version of Lustre that includes the Lustre
 * release version, commit hash, kernel version, and architecture, e.g.
 * v2_6_92_0-gadb3ee4-CHANGED-2.6.32-431.29.2.el6_lustre.g36cd22b.x86_64.
 *
 * \param version[in,out]	buffer to store build version string
 * \param version_size[in]	size of \a version
 *
 * \retval			0 on success
 * \retval			-1 on failure, errno set
 */
int llapi_get_build_version(char *version, int version_size)
{
	return get_version_string("build:", version, version_size);
}

/**
 * Return the build version of the Lustre code.
 *
 * For historical reasons this returns the build version of the code from
 * llapi_get_build_version(), which is typically a long ugly string, rather
 * than the release version, which is a nice short version.  That probably
 * isn't what people wanted, but they may depend on the old output format.
 *
 * Callers should use one of the two newer functions instead, but give them
 * a few versions to make the switch.
 *
 * \param buffer[in]		temporary buffer to hold version string
 * \param buffer_size[in]	length of the \a buffer
 * \param version[out]		pointer to the start of build version string
 *
 * \retval			0 on success
 * \retval			-1 on error, errno set
 */
int llapi_get_version(char *buffer, int buffer_size, char **version)
{
#if LUSTRE_VERSION_CODE > OBD_OCD_VERSION(2, 9, 53, 0)
	static bool printed;
	if (!printed) {
		fprintf(stderr, "%s deprecated, use llapi_get_release_version()"
			" or llapi_get_build_version() instead\n", __func__);
		printed = true;
	}
#endif

	*version = buffer;
	return llapi_get_build_version(buffer, buffer_size);
}
