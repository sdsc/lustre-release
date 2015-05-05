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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/limits.h>
#include <libcfs/util/string.h>

/**
 * Get pathname matching the pattern
 *
 * \param name		name buffer for parameter value string
 * \param name_size	size of buffer for return value
 * \param pattern	the pattern of the path to match
 *
 * The \param pattern is appended to /{proc,sys}/{fs,sys} to complete the
 * absolute path to the file the caller is requesting. If that file exists
 * then the pathname \param name buffer that is passed by the caller.
 *
 * \retval 0 for success, with a NUL-terminated string in \param name.
 * \retval negative error number for the case of an error.
 */
int
cfs_get_procpath(char *name, size_t name_size, const char *pattern, ...)
{
	char path[PATH_MAX + 1] = "{/proc/fs,/proc/sys,/sys/fs,/sys/class/lnet}/";
	const int len = 44; /* length of above path prefix */
	glob_t glob_info;
	size_t count;
	int rc;
	char buf[PATH_MAX + 1];
	va_list args;

	va_start(args, pattern);
	rc = vsnprintf(buf, sizeof(buf), pattern, args);
	va_end(args);
	if (rc < 0)
		return -errno;

	count = strlcpy(path + len, buf, PATH_MAX + 1);
	if (count < strlen(buf))
		return -E2BIG;

	rc = glob(path, GLOB_BRACE, NULL, &glob_info);
	if (rc == GLOB_NOMATCH) {
		rc = -ENODEV;
		goto out;
	}

	if (rc != 0 || glob_info.gl_pathc < 1) {
		rc = -ENOENT;
		goto out;
	}

	count = strlcpy(name, glob_info.gl_pathv[0], name_size);
	if (count < strlen(glob_info.gl_pathv[0]))
		rc = -E2BIG;

out:
	globfree(&glob_info);
	return rc;
}
