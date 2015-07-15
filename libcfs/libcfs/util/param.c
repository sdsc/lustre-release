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
cfs_get_param_path(glob_t *paths, const char *pattern, ...)
{
	char path[PATH_MAX] = "{/sys/{fs,class,module,kernel/debug}/,"
			       "/proc/{fs,sys}/{lnet,lustre}/}";
	size_t path_len = strlen(path);
	char buf[PATH_MAX];
	va_list args;
	size_t len;
	int rc;

	va_start(args, pattern);
	rc = vsnprintf(buf, sizeof(buf), pattern, args);
	va_end(args);
	if (rc < 0)
		return -errno;

	len = strlcpy(path + path_len, buf, sizeof(path) - path_len);
	if (len >= sizeof(path) - path_len)
		return -E2BIG;

	rc = glob(path, GLOB_NOSORT | GLOB_BRACE | GLOB_MARK, NULL, paths);
	if (rc == GLOB_NOMATCH)
		return -ENODEV;

	if (rc != 0 || paths->gl_pathc < 1)
		rc = -ENOENT;

	return rc;
}
