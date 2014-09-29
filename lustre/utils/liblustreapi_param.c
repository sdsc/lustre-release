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

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <glob.h>
#include <string.h>

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
int
get_proc_path(char *name, size_t name_size, const char *param)
{
	char pattern[PATH_MAX + 1] = "/{proc,sys}/{fs,sys}/{lnet,lustre}/";
	const int len = 35; /* length of above path prefix */
	glob_t glob_info;
	char *tmp;
	int rc = 0;

	strcpy(pattern + len, param);

	/* We need to translate any "." into "/" */
	if (strchr(pattern, '.')) {
		tmp = pattern;
		while (*tmp != '\0') {
			if (*tmp == '.')
				*tmp = '/';
			tmp++;
		}
	}

	rc = glob(pattern, GLOB_BRACE, NULL, &glob_info);
	if (rc == GLOB_NOMATCH) {
		rc = -ENODEV;
		goto out;
	}
	if (rc != 0) {
		rc = -EINVAL;
		goto out;
	}
	if (glob_info.gl_pathc < 1) {
		rc = -ENOENT;
		goto out;
	}
	strcpy(name, glob_info.gl_pathv[0]);

out:
	globfree(&glob_info);
	return rc;
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
  * the caller is requesting. If that file exist then the data is read from
  * the file and placed into a 2D array and is passed to the caller.
  *
  * Return 0 for success.
  * Return -ve value for error.
  */
int
llapi_get_param(char ***list, size_t *list_size, const char *param, ...)
{
	char buf[PATH_MAX], *output = NULL, *resize = NULL;
	char name[PATH_MAX], pattern[PATH_MAX];
	size_t output_size = 0, count = 0;
	char *items = NULL, *temp = NULL;
	unsigned long *offsets = NULL;
	unsigned long offset = 0;
	int temp_errno = 0, i;
	FILE *fp = NULL;
	int rc = 0;
	va_list args;

	va_start(args, param);
        vsnprintf(pattern, sizeof(pattern), param, args);
	va_end(args);

	rc = get_proc_path(name, strlen(name), pattern);
	if (rc < 0)
		return rc;

	fp = fopen(name, "r");
	if (fp == NULL) {
		return -errno;
	}

	*list = NULL;
	*list_size = 0;

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		int len = strlen(buf);
		char *tmp;

		/* remove '\n' */
		tmp = strchr(buf, '\n');
		if (tmp != NULL) *tmp = '\0';

		if (offset + len > output_size) {
			output_size += PATH_MAX;
			output = realloc(resize, output_size);
			if (resize == NULL) output[0] = '\0';
			if (output == NULL) {
				temp_errno = -errno;
				break;
			}
			resize = output;
		}
		strncat(output + offset, buf, len);

		if ((count % 4) == 0) {
			size_t items_count = sizeof(offsets) * (count + 4);

			items = realloc(temp, items_count);
			if (items == NULL) {
				temp_errno = -errno;
				break;
			}
			temp = items;
			offsets = (unsigned long *) items;
		}
		offsets[count] = offset;
		offset += len + 1;
		count++;
	}

	if (temp_errno == 0) {
		size_t total_size = count * sizeof(offsets) + offset;

		offsets = realloc(items, total_size);
		if (offsets != NULL) {
			char *tmp = (char *) offsets + count * sizeof(offsets);

			memcpy(tmp, output, offset);

			*list_size = count;
			*list = (char **) offsets;
			for (i = 0; i < count; i++)
				offsets[i] += (unsigned long) tmp;
		} else {
			temp_errno = -errno;
		}
	}
	if (temp_errno != 0) {
		if (items != NULL)
			free(items);
	}
	if (output != NULL)
		free(output);
	fclose(fp);

	return rc;
}
