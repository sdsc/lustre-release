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
 * lustre/utils/liblustreapi_param.c
 *
 * This code handles user interaction with the configuration interface
 * to the Lustre file system to fine tune it.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <glob.h>
#include <string.h>

#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>
#include "lustreapi_internal.h"

/**
 * return a parameter string for a specific device type or mountpoint
 *
 * The \param param is appended to path in procfs, sysfs or debugfs to
 * complete the absolute path to the file containing the parameter data
 * the caller is requesting. If that file exist then the data is read from
 * the file and placed into a 2D array and is passed to the caller.
 *
 * \param list	a value buffer for parameter value strings
 *
 * \retval	0 for success.
 * \retval	negative error number for the case of an error.
 */
int llapi_get_param(char ***list, const char *param, ...)
{
	va_list args;
	FILE *fp = NULL;
	DIR *dir = NULL;
	struct stat st;
	struct dirent pool;
	struct dirent *cookie = NULL;
	char buf[PATH_MAX];
	char path[PATH_MAX];
	char **newlist;
	char **l = NULL;
	char *str;
	size_t size = 0;
	size_t len = 0;
	int i = 0;
	int j;
	int rc;

	va_start(args, param);
	rc = vsnprintf(buf, sizeof(buf), param, args);
	va_end(args);
	if (rc < 0)
		return -errno;

	rc = cfs_get_procpath(path, sizeof(path), "{lnet,lustre}/%s", buf);
	if (rc < 0)
		return rc;

	rc = stat(path, &st);
	if (rc != 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "error stat '%s'", path);
		return rc;
	}

	if (S_ISDIR(st.st_mode))
		dir = opendir(path);
	else
		fp = fopen(path, "r");

	if (fp == NULL && dir == NULL) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "error opening '%s'", path);
		return rc;
	}

	*list = NULL;
	while (1) {
		if (dir != NULL) {
			rc = readdir_r(dir, &pool, &cookie);
			if (rc != 0) {
				rc = -errno;
				llapi_error(LLAPI_MSG_ERROR, rc,
					    "error reading of '%s'", path);
				goto out;
			} else if (cookie == NULL) {
				break; /* end of directory */
			}
			/* ignore . and .. */
			if (!strcmp(pool.d_name, ".") ||
			    !strcmp(pool.d_name, ".."))
				continue;
		} else {
			str = fgets(buf, sizeof(buf), fp);
			if (str == NULL)
				break; /* end of file */
			if (*str == '\0')
				continue; /* ignore empty line */
		}
		if (i >= size) {
			char **newlist;

			size += 10;
			newlist = realloc(l, size * sizeof(char *));
			if (newlist == NULL) {
				llapi_err_noerrno(LLAPI_MSG_INFO,
						  "failed to allocate");
				rc = -ENOMEM;
				goto out;
			}
			l = newlist;
		}
		str = strdup(dir != NULL ? pool.d_name : buf);
		if (str == NULL) {
			llapi_err_noerrno(LLAPI_MSG_INFO, "strdup failed");
			rc = -ENOMEM;
			goto out;
		}
		l[i++] = str;
		len += strlen(str) + 1;
	}

	size = (i + 1) * sizeof(char *) + len;
	newlist = malloc(size);
	if (newlist == NULL) {
		llapi_err_noerrno(LLAPI_MSG_INFO, "failed to allocate");
		rc = -ENOMEM;
		goto out;
	}
	memset(newlist, 0, size);
	str = (char *)&newlist[i + 1];

	for (j = 0; j < i; j++) {
		len = strlen(l[j]) + 1;
		strlcpy(str, l[j], len);
		newlist[j] = str;
		str += len;
	}

	*list = newlist;
out:
	for (i--; i >= 0; i--)
		free(l[i]);
	free(l);
	if (dir != NULL)
		closedir(dir);
	if (fp != NULL)
		fclose(fp);

	return rc;
}

/**
 * set a parameter data for a specific device type or mountpoint
 *
 * \param val		a buffer for value string
 * \param val_size	the size of data buffer to send
 *
 * The \param param is appended to path in procfs, sysfs or debugfs to
 * complete the absolute path to the file containing the parameter data
 * the caller is requesting. If that file exist then the data is send to
 * the file.
 *
 * \retval	0 for success.
 * \retval	negative error number for the case of an error.
 */
int llapi_set_param(const void *val, size_t val_size, const char *param, ...)
{
	va_list args;
	char buf[PATH_MAX];
	char path[PATH_MAX];
	ssize_t size;
	int fd;
	int rc;

	va_start(args, param);
	rc = vsnprintf(buf, sizeof(buf), param, args);
	va_end(args);
	if (rc < 0)
		return -errno;

	rc = cfs_get_procpath(path, sizeof(path), "{lnet,lustre}/%s", buf);
	if (rc < 0)
		return rc;

	fd = open(path, O_WRONLY);
	if (fd < 0) {
		llapi_err_noerrno(LLAPI_MSG_ERROR, "error: opening '%s'", path);
		return -errno;
	}

	size = write(fd, val, val_size);
	rc = (size < 0) ? -errno : (size != val_size) ? -EFAULT : 0;
	close(fd);

	return rc;
}
