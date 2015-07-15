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

int
get_lustre_param_path(glob_t *param, const char *obd_type, const char *filter,
		      enum param_filter type, const char *param_name)
{
	char pattern[PATH_MAX];
	FILE *fp = NULL;
	int rc = 0;

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
		if (rc < strlen(pattern) + 2)
			return -EINVAL;
		break;
	default :
		strncpy(pattern, filter, sizeof(pattern));
		break;
	}

	if (param_name != NULL)
		rc = cfs_get_param_path(param, "%s/%s/%s",
					obd_type, pattern, param_name);
	else
		rc = cfs_get_param_path(param, "%s/%s",
					obd_type, pattern);
	return rc;
}

int find_target_obdpath(char *path, char *tgt, enum tgt_type type)
{
	glob_t param;
	int rc;

	rc = get_lustre_param_path(&param, type == LOV_TYPE ? "lov" : "lmv",
				   tgt, FILTER_BY_EXACT, "target_obd");
	if (rc == -ENODEV)
		return rc;

	if (rc != 0)
		return -EINVAL;

	strcpy(path, param.gl_pathv[0]);
	cfs_free_param_data(&param);
	return 0;
}

/*
 * find the pool directory path
 * (can be also used to test if a fsname is known)
 */
int poolpath(char *fsname, char *file_path, char *pool_pathname)
{
	int rc = -EINVAL;
	glob_t pattern;

	if (fsname == NULL) {
		rc = get_lustre_param_path(&pattern, "lov", file_path,
					   FILTER_BY_PATH, "pools");
	} else {
		rc = get_lustre_param_path(&pattern, "lov", fsname,
					   FILTER_BY_FS_NAME, "pools");
	}
	if (rc != 0)
		return -ENOENT;

	/* in fsname test mode, pool_pathname is NULL */
	if (pool_pathname != NULL)
		strncpy(pool_pathname, pattern.gl_pathv[0],
			strlen(pattern.gl_pathv[0]));

	cfs_free_param_data(&pattern);
	return 0;
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
int
get_param_value(const char *obd_type, const char *filter,
		enum param_filter type, const char *param_name,
		char *val, size_t val_len)
{
	char pattern[PATH_MAX];
	FILE *fp = NULL;
	glob_t param;
	int rc = 0;

	rc = get_lustre_param_path(&param, obd_type, filter, type, param_name);
	if (rc < 0)
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
