/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
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
 * libcfs/libcfs/utils/param.c
 *
 * This code handles user interaction with the configuration interface
 * to the Lustre file system to fine tune it.
 */
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/limits.h>
#include <libcfs/util/string.h>

/**
 * Get parameter path matching the pattern
 *
 * \param[in] paths	glob_t structure used to hold the final result
 * \param[in] pattern	the pattern of the path to match
 *
 * The \param pattern is appended to the default path glob to complete the
 * absolute path to the file the caller is requesting. If the results points
 * to one or more files that exists those results are stored in \param paths
 * glob_t structure that is passed by the caller.
 *
 * Lustre tunables traditionally were in /proc/{sys,fs}/{lnet,lustre}
 * but in upstream kernels starting with Linux 4.2 these parameters
 * have been moved to /sys/fs/lustre and /sys/kernel/debug/lustre
 * so the user tools need to check both locations.
 *
 * \retval	 0 for success, with results stored in \param paths.
 * \retval	-1 for failure with errno set to report the reason.
 */
int
cfs_get_param_path(glob_t *paths, const char *pattern, ...)
{
	char path[PATH_MAX] = "{/sys/{fs,class,kernel/debug}/,"
			       "/proc/{fs,sys}/{lnet,lustre}/}";
	size_t path_len = strlen(path);
	char buf[PATH_MAX];
	va_list args;
	size_t len;
	int rc;

	va_start(args, pattern);
	rc = vsnprintf(buf, sizeof(buf), pattern, args);
	va_end(args);
	if (rc < 0) {
		fprintf(stderr, "get_param_path vsnprintf failed "
			"for pattern %s\n", pattern);
		return -1;
	}

	len = strlcpy(path + path_len, buf, sizeof(path) - path_len);
	if (len >= sizeof(path) - path_len) {
		errno = E2BIG;
		return -1;
	}

	rc = glob(path, GLOB_NOSORT | GLOB_BRACE | GLOB_MARK, NULL, paths);
	if (rc == GLOB_NOMATCH) {
		errno = ENODEV;
		return -1;
	}

	if (rc != 0 || paths->gl_pathc < 1) {
		errno = ENOENT;
		return -1;
	}

	return 0;
}
