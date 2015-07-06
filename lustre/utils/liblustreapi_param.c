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

int find_target_obdpath(char *path, char *tgt, enum tgt_type type)
{
	glob_t pattern;
	int rc;

	rc = cfs_get_param_path(&pattern, "lustre/%s/%s/target_obd",
				type == LOV_TYPE ? "lov" : "lmv", tgt);
	if (rc == -ENODEV)
		return rc;

	if (rc != 0)
		return -EINVAL;

	strcpy(path, pattern.gl_pathv[0]);
	cfs_free_param_path(&pattern);
	return 0;
}

/*
 * find the pool directory path
 * (can be also used to test if a fsname is known)
 */
int poolpath(char *fsname, char *pathname, char *pool_pathname)
{
	char buffer[PATH_MAX];
	glob_t pattern;
	int rc;

	if (fsname == NULL) {
		rc = llapi_search_fsname(pathname, buffer);
		if (rc != 0)
			return rc;
		fsname = buffer;
		strcpy(pathname, fsname);
	}

	rc = cfs_get_param_path(&pattern, "lustre/lov/%s-*/pools",
				fsname);
	if (rc != 0)
		return -ENOENT;

	/* in fsname test mode, pool_pathname is NULL */
	if (pool_pathname != NULL)
		strncpy(pool_pathname, pattern.gl_pathv[0],
			strlen(pattern.gl_pathv[0]));

	cfs_free_param_path(&pattern);
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
get_param_value(const char *fsname, const char *file_path,
		const char *obd_type, const char *param_name,
		char *value, size_t val_len)
{
	char fs[PATH_MAX];
	FILE *fp = NULL;
	glob_t param;
	int rc = 0;

	if (fsname == NULL && file_path != NULL) {
		rc = llapi_search_fsname(file_path, fs);
		if (rc) {
			llapi_error(LLAPI_MSG_ERROR, rc,
				    "'%s' is not on a Lustre filesystem",
				    file_path);
			return rc;
		}
	} else if (fsname != NULL) {
		rc = strlcpy(fs, fsname, sizeof(fs));
		if (rc >= sizeof(fs))
			return -E2BIG;
	}

	rc = cfs_get_param_path(&param, "lustre/%s/%s-*/%s",
				obd_type, fs, param_name);
	if (rc < 0)
		return -ENOENT;

	fp = fopen(param.gl_pathv[0], "r");
	if (fp == NULL) {
		rc = -errno;
		cfs_free_param_path(&param);
		llapi_error(LLAPI_MSG_ERROR, rc, "error: opening '%s'",
			    param.gl_pathv[0]);
		return rc;
	}
	cfs_free_param_path(&param);

	if (fgets(value, val_len, fp) == NULL)
		rc = -errno;

	fclose(fp);

	return rc;
}

/*
 * Obtain client parameter settings.
 */
int
get_param_cli(const char *type, const char *inst, const char *param,
	      char *buf, size_t buf_size)
{
	glob_t path;
	FILE *fp;
	int rc;

	rc = cfs_get_param_path(&path, "lustre/%s/%s/%s",
				type, inst, param);
	if (rc != 0)
		return -ENOENT;

	fp = fopen(path.gl_pathv[0], "r");
	if (fp == NULL)
		return -errno;

	if (fgets(buf, buf_size, fp) == NULL)
		rc = -errno;

	cfs_free_param_path(&path);
	fclose(fp);

	return rc;
}

/*
 * Given a filesystem name, or a pathname of a file on a lustre filesystem,
 * tries to determine the path to the filesystem's clilov parameters directory
 *
 * fsname is limited to MTI_NAME_MAXLEN in lustre_idl.h. Note PATH_MAX is
 * far larger than MTI_NAME_MAXLEN so it is safe to set the pattern buffer
 * to be PATH_MAX in size.
 * The NULL terminator is compensated by the additional "%s" bytes.
 *
 */
int
clilovpath(const char *fsname, const char *const pathname, char *clilovpath)
{
	char buffer[PATH_MAX + 1];
	glob_t pattern;
	int rc;

	if (fsname == NULL) {
		rc = llapi_search_fsname(pathname, buffer);
		if (rc != 0)
			return rc;
		fsname = buffer;
	}

	rc = cfs_get_param_path(&pattern, "lustre/lov/%s-clilov-*",
				fsname);
	if (rc != 0)
		return -ENOENT;

	strlcpy(clilovpath, pattern.gl_pathv[0], strlen(pattern.gl_pathv[0]));
	cfs_free_param_path(&pattern);
	return 0;
}
