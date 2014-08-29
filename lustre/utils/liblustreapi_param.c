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
 * lustre/utils/liblustreapi_layout.c
 *
 * lustreapi library for layout calls for interacting with the layout of
 * Lustre files while hiding details of the internal data structures
 * from the user.
 *
 */

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <glob.h>

#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>
#include "lustreapi_internal.h"

/**
 * Get parameter name for a specific device type or mountpoint
 *
 * \param[out]	the path to the file containing parameter data
 * \param[in]	name buffer for parameter value string
 * \param[in]	name_size size of buffer for return value
 *
 * The \a param is appended to /{proc,sys}/{fs,sys}/{lnet,lustre}/ to
 * complete the absolute path to the file containing the parameter data
 * the caller is requesting. If that file exist then the data is read from
 * the file and placed into the \param name buffer that is passed by
 * the caller. Data is only copied up to the \param name_size to prevent
 * overflow of the array.
 *
 * \retval 0 for success, with a NUL-terminated string in \param result.
 * \retval negative error number for the case of an error.
 */
int get_param_path(char *name, size_t name_size, const char *param, ...)
{
	char pattern[PATH_MAX + 1] = "/{proc,sys}/{fs,sys}/{lnet,lustre}/";
	const int len = 35; /* length of above path prefix */
	glob_t glob_info;
	va_list args;
	int rc;

	va_start(args, param);

	vsnprintf(pattern + len, sizeof(pattern) - len, param, args);

	rc = glob(pattern, GLOB_BRACE, NULL, &glob_info);
	if (rc == GLOB_NOMATCH) {
		rc = -ENOENT;
		goto out;
	}
	if (rc != 0) {
		rc = -EINVAL;
		goto out;
	}

	if (glob_info.gl_pathc < 1) {
		globfree(&glob_info);
		rc = -ENOENT;
		goto out;
	}

	strncpy(name, glob_info.gl_pathv[0], name_size - 1);
	name[name_size - 1] = '\0';
	if (strlen(glob_info.gl_pathv[0]) >= name_size)
		rc = -EOVERFLOW;

out:
	globfree(&glob_info);
	va_end(args);
	return 0;
}

/**
 * return a parameter string for a specific device type or mountpoint
 *
 * \param	the path to the file containing parameter data
 * \param	a value buffer for parameter value string
 * \param	value_size size of buffer for return value
 *
 * The \param param is appended to /{proc,sys}/{fs,sys}/{lnet,lustre}/ to
 * complete the absolute path to the file containing the parameter data
 * the user is requesting. If that file exist then the data is read from
 * the file and placed into the \param result buffer that is passed by
 * the user. Data is only copied up to the \param result_size to prevent
 * overflow of the array.
 *
 * Return 0 for success, with a NUL-terminated string in \param result.
 * Return -ve value for error.
 */
int get_param(const char *param_path, char *result, size_t result_size)
{
	char buf[PATH_MAX];
	FILE *fp = NULL;
	int rc = 0;

	fp = fopen(param_path, "r");
	if (fp != NULL) {
		while (fgets(buf, sizeof(buf), fp) != NULL && result_size > 0) {
			int len = strlen(buf);

			strncat(result, buf, result_size - 1);
			result_size -= len;
			result += len;
		}
		fclose(fp);
	} else {
		rc = -errno;
	}
	return rc;
}

/**
 * return a parameter string for a specific device type or mountpoint
 *
 * \param fsname Lustre filesystem name (optional)
 * \param file_path path to file in filesystem (optional, if fsname unset)
 * \param obd_type Lustre OBD device type
 * \param param_name parameter name to fetch
 * \param value return buffer for parameter value string
 * \param val_len size of buffer for return value
 *
 * If fsname is specified then the parameter will be from that filesystem
 * (if it exists). If file_path is given and it is in a mounted Lustre
 * filesystem, then the parameter will be otherwise the value may be
 * from any mounted filesystem (if there is more than one).
 *
 * If "obd_type" matches a Lustre device then the first matching device
 * (as with "lctl dl", constrained by \param fsname or \param mount_path)
 * will be used to provide the return value, otherwise the first such
 * device found will be used.
 *
 * Return 0 for success, with a NUL-terminated string in \param buffer.
 * Return -ve value for error.
 */
int get_param_obdvar(const char *fsname, const char *file_path,
		     const char *obd_type, const char *param_name,
		     char *value, unsigned int val_len)
{
	char buf[PATH_MAX], dev[PATH_MAX] = "*", *fs = NULL;
	int type_num = 1, rc = 0, i;
	char path[PATH_MAX];
	FILE *fp = NULL;

	rc = get_param_path(path, PATH_MAX + 1, "devices");
	if (rc)
		goto out;

	fp = fopen(path, "r");
	if (fp == NULL) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "error: opening device list");
		goto out;
	}

	if (fsname == NULL && file_path != NULL) {
		fs = calloc(1, strlen(file_path) + 1);
		rc = llapi_search_fsname(file_path, fs);
		if (rc) {
			llapi_error(LLAPI_MSG_ERROR, rc,
				    "'%s' is not on a Lustre filesystem",
				    file_path);
			goto out;
		}
	} else if (fsname != NULL)
		fs = strdup(fsname);

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		char *obd_type_name = NULL;
		char *obd_name = NULL;
		char *obd_uuid = NULL;
		char *bufp = buf;

		while (bufp[0] == ' ')
			++bufp;

		for(i = 0; i < 3; i++)
			obd_type_name = strsep(&bufp, " ");

		obd_name = strsep(&bufp, " ");
		obd_uuid = strsep(&bufp, " ");

		for (i = 0; i < type_num; i++) {
			char *tmp = NULL;

			if (strcmp(obd_type_name, obd_type) != 0)
				continue;

			if (fs != NULL && strncmp(obd_name, fs, strlen(fs)))
				continue;

			if (strlen(obd_name) > sizeof(dev)-1) {
				rc = -E2BIG;
				break;
			}
			strlcpy(dev, obd_name, sizeof(dev));
			tmp = strchr(dev, ' ');
			if (tmp != NULL)
				*tmp = '\0';
			break;
		}
	}

	if (dev[0] == '*' && fs != NULL) {
		rc = snprintf(dev, sizeof(dev), "%s-*", fs);
		if (rc >= sizeof(dev)) {
			rc = -E2BIG;
			goto out;
		}
	}
	memset(value, 0, val_len);

	rc = get_param_path(path, PATH_MAX + 1, "%s/%s/%s", obd_type, dev,
			    param_name);
	if (!rc)
		rc = get_param(path, value, val_len);
out:
	if (fp != NULL)
		fclose(fp);
	if (fs != NULL)
		free(fs);
	return rc;
}

