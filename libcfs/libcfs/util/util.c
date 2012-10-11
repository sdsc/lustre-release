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
 * Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/libcfs/util/util.c
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <glob.h>

#include <libcfs/libcfsutil.h>
#include "../tracefile.h"

/* message level */
static int cfs_msg_level = CFS_MSG_MAX;

#define CFS_GLOB_SYSFS_PATTERN "/sys/class/misc/lnet/%s"
#define CFS_GLOB_PROCFS_PATTERN "/proc/{fs,sys}/{lnet,lustre}/%s"

int
libcfs_tcd_type_max(void)
{
	return CFS_TCD_TYPE_MAX;
}

void
cfs_msg_set_level(int level)
{
	/* ensure level is in the good range */
	if (level < CFS_MSG_OFF)
		cfs_msg_level = CFS_MSG_OFF;
	else if (level > CFS_MSG_MAX)
		cfs_msg_level = CFS_MSG_MAX;
	else
		cfs_msg_level = level;
}

/* cfs_error will preserve errno */
void
cfs_error(int level, int _rc, char *fmt, ...)
{
	va_list args;
	int tmp_errno = errno;
	/* to protect using errno as _rc argument */
	int rc = abs(_rc);

	if ((level & CFS_MSG_MASK) > cfs_msg_level)
		return;

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);

	if (level & CFS_MSG_NO_ERRNO)
		fprintf(stderr, "\n");
	else
		fprintf(stderr, ": %s (%d)\n", strerror(rc), rc);
	errno = tmp_errno;
}

/* cfs_printf will preserve errno */
void
cfs_printf(int level, char *fmt, ...)
{
	va_list args;
	int tmp_errno = errno;

	if ((level & CFS_MSG_MASK) > cfs_msg_level)
		return;

	va_start(args, fmt);
	vfprintf(stdout, fmt, args);
	va_end(args);
	errno = tmp_errno;
}

/* return the first file matching this pattern */
int
cfs_first_match(char *pattern, char *buffer)
{
	glob_t glob_info;

	if (glob(pattern, GLOB_BRACE, NULL, &glob_info))
		return -ENOENT;

	if (glob_info.gl_pathc < 1) {
		globfree(&glob_info);
		return -ENOENT;
	}

	/* Note: the caller must make buffer of PATH_MAX or bigger. */
	strncpy(buffer, glob_info.gl_pathv[0], PATH_MAX);
	buffer[PATH_MAX - 1] = '\0';

	globfree(&glob_info);
	return 0;
}

int
cfs_get_param(const char *param_path, char *result,
	      unsigned int result_size)
{
	char file[PATH_MAX];
	char pattern[PATH_MAX];
	char buf[result_size];
	FILE *fp = NULL;
	int rc = 0;

	/* First see if this is in sysfs. */
	snprintf(pattern, sizeof(pattern), CFS_GLOB_SYSFS_PATTERN,
		 param_path);
	pattern[sizeof(pattern) - 1] = '\0';
	rc = cfs_first_match(pattern, file);

	/* If not in sysfs, check procfs. */
	if (rc) {
		snprintf(pattern, sizeof(pattern),
			 CFS_GLOB_PROCFS_PATTERN,
			 param_path);
		pattern[sizeof(pattern) - 1] = '\0';
		rc = cfs_first_match(pattern, file);
		if (rc)
			return rc;
	}

	fp = fopen(file, "r");
	if (fp != NULL) {
		while (fgets(buf, result_size, fp) != NULL)
			strncpy(result, buf, result_size);
		result[result_size - 1] = '\0';
		fclose(fp);
	} else {
		rc = -errno;
	}
	return rc;
}

int
cfs_set_param(const char *param_path, char *buffer,
	      unsigned int buffer_size)
{
	char file[PATH_MAX];
	char pattern[PATH_MAX];
	int rc = 0;
	int fd;

	/* First see if this is in sysfs. */
	snprintf(pattern, sizeof(pattern), "/sys/class/misc/lnet/%s",
		 param_path);
	pattern[sizeof(pattern) - 1] = '\0';
	rc = cfs_first_match(pattern, file);

	/* If not in sysfs, check procfs. */
	if (rc) {
		snprintf(pattern, sizeof(pattern),
			 "/proc/{fs,sys}/{lnet,lustre}/%s",
			 param_path);
		pattern[sizeof(pattern) - 1] = '\0';
		rc = cfs_first_match(pattern, file);
		if (rc)
			return rc;
	}

	fd = open(file, O_WRONLY);
	if (fd < 0)
		return -errno;

	rc = write(fd, buffer, buffer_size);
	rc = (rc < 0) ? -errno : 0;
	close(fd);

	return rc;
}
