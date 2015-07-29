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
#include <errno.h>
#include <stdint.h>

#include <libcfs/util/param.h>
#include <lustre/lustre_user.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

/**
 * return the parameter's path for a specific device type or mountpoint
 *
 * \param param		the results returned to the caller
 * \param obd_type	Lustre OBD device type
 *
 * \param filter	filter combined with the type agruments allow the
 * \param type		caller to limit the scope of the search for the
 *			parameter's path. Typical options are search by
 *			Lustre filesystem name or by the path to a file
 *			or directory in the filesystem.
 *
 * \param param_name	parameter name to fetch
 *
 * Using filter and the type argument we can limit the scope of the
 * search to either the parameter belonging to a specific lustre filesystem
 * (if it exists) or using a given file or directory path located on a
 * mounted Lustre filesystem. The last case it can do is a special search
 * based on exactly what the used passed instead of scanning file paths
 * or specific file systems.
 *
 * If "obd_type" matches a Lustre device then the first matching device
 * (as with "lctl dl", constrained by \param filter and \param type)
 * will be used to provide the return value, otherwise the first such
 * device found will be used.
 *
 * Return 0 for success, with the results stored in \param param.
 * Return -ve value for error.
 */
int
get_lustre_param_path(glob_t *param, const char *obd_type, const char *filter,
		      enum param_filter type, const char *param_name)
{
	char pattern[PATH_MAX];
	int rc;

	if (filter == NULL)
		return -EINVAL;

	switch (type) {
	case FILTER_BY_PATH:
		rc = llapi_search_fsname(filter, pattern);
		if (rc) {
			llapi_error(LLAPI_MSG_ERROR, rc,
				    "'%s' is not on a Lustre filesystem",
				    filter);
			return rc;
		}
		strncat(pattern, "-*", sizeof(pattern));
		break;
	case FILTER_BY_FS_NAME:
		rc = snprintf(pattern, sizeof(pattern) - 1, "%s-*", filter);
		if (rc < strlen(filter) + 2)
			return -EINVAL;
		rc = 0;
		break;
	default:
		strncpy(pattern, filter, sizeof(pattern));
		break;
	}

	if (param_name != NULL) {
		if (cfs_get_param_path(param, "%s/%s/%s",
				       obd_type, pattern, param_name) != 0)
			rc = -errno;
	} else {
		if (cfs_get_param_path(param, "%s/%s",
				       obd_type, pattern) != 0)
			rc = -errno;
	}

	return rc;
}

/**
 * return special client parameter's path for specific file system
 *
 * \param path		the results returned to the caller
 * \param fsname	Lustre filesystem name (optional)
 * \param file_path	path to file in filesystem (optional, if fsname unset)
 * \param param		parameter name to fetch
 *
 * If fsname is specified then the parameter will be from that filesystem
 * (if it exists). If file_path is given and it is in a mounted Lustre
 * filesystem, then the parameter will be otherwise the value may be
 * from any mounted filesystem (if there is more than one).
 *
 * This handles the special case of parameters containing clilov in its
 * name. Here we combind clilov with the file system name if provided
 * to create a filter to limit the scope of the parameter paths that
 * are returned.
 *
 * Return 0 for success, with the results stored in \param path.
 * Return -ve value for error.
 */
int get_lustre_clilov_path(glob_t *path, char *fsname, char *file_path,
			   char *param)
{
	char buf[PATH_MAX];
	int rc;

	if (fsname == NULL) {
		if (file_path == NULL)
			return -EINVAL;

		rc = llapi_search_fsname(file_path, buf);
		if (rc) {
			llapi_error(LLAPI_MSG_ERROR, rc,
				    "'%s' is not on a Lustre filesystem",
				    file_path);
			return rc;
		}
		strncat(buf, "-clilov-*", sizeof(buf));
	} else {
		snprintf(buf, sizeof(buf), "%s-clilov-*", fsname);
	}
	rc = get_lustre_param_path(path, "lov", buf,
				   FILTER_BY_EXACT, param);
	if (rc != 0)
		return -ENOENT;

	return 0;
}

/**
 * return a parameter of a single line value for a specific device type
 * or mountpoint
 *
 * \param obd_type	Lustre OBD device type
 *
 * \param filter	filter combined with the type agruments allow the
 * \param type		caller to limit the scope of the search for the
 *			parameter's path. Typical options are search by
 *			Lustre filesystem name or by the path to a file
 *			or directory in the filesystem.
 *
 * \param param_name	parameter name to fetch
 * \param value		return buffer for parameter value string
 * \param val_len	size of buffer for return value
 *
 * Using filter and the type argument we can limit the scope of the
 * search to either the parameter belonging to a specific lustre filesystem
 * (if it exists) or using a given file or directory path located on a
 * mounted Lustre filesystem. The last case it can do is a special search
 * based on exactly what the used passed instead of scanning file paths
 * or specific file systems.
 *
 * If "obd_type" matches a Lustre device then the first matching device
 * (as with "lctl dl", constrained by \param filter and \param type)
 * will be used to provide the return value, otherwise the first such
 * device found will be used.
 *
 * Return 0 for success, with a NUL-terminated string in \param val.
 * Return -ve value for error.
 */
int
get_lustre_param_value(const char *obd_type, const char *filter,
		       enum param_filter type, const char *param_name,
		       char *val, size_t val_len)
{
	FILE *fp = NULL;
	glob_t param;
	int rc = 0;

	rc = get_lustre_param_path(&param, obd_type, filter, type, param_name);
	if (rc != 0)
		return -ENOENT;

	fp = fopen(param.gl_pathv[0], "r");
	if (fp == NULL) {
		rc = -errno;
		cfs_free_param_data(&param);
		llapi_error(LLAPI_MSG_ERROR, rc, "error: opening '%s'",
			    param.gl_pathv[0]);
		return rc;
	}
	cfs_free_param_data(&param);

	if (fgets(val, val_len, fp) == NULL)
		rc = -errno;

	fclose(fp);

	return rc;
}
