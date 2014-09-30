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

	strlcpy(pattern + len, param, PATH_MAX + 1);

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
		rc = -ENOENT;
		goto out;
	}
	if (glob_info.gl_pathc < 1) {
		rc = -ENOENT;
		goto out;
	}
	strlcpy(name, glob_info.gl_pathv[0], name_size);

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
llapi_get_param(char ***list, size_t *count, const char *param, ...)
{
	char buf[PATH_MAX];
	char name[PATH_MAX];
	char pattern[PATH_MAX];
	FILE *fp = NULL;
	va_list args;
	char **l;
	int listinc = 10;
	int listsize = listinc;
	int i = 0;
	int rc = 0;

	va_start(args, param);
	vsnprintf(pattern, sizeof(pattern), param, args);
	va_end(args);

	rc = get_proc_path(name, sizeof(name), pattern);
	if (rc < 0)
		return rc;

	fp = fopen(name, "r");
	if (fp == NULL)
		return -errno;

	*list = (char **)NULL;
	*count = 0;

	if ((l = (char **) malloc(listsize * sizeof(char *))) == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		if (i + 1 > listsize) {
			listsize += listinc;
				l = (char **)realloc(l,
						listsize * sizeof(char *));
				if (l == NULL) {
					rc = -ENOMEM;
					goto out;
				}
			}
			if ((l[i++] = strdup(buf)) == NULL) {
				rc = -ENOMEM;
				goto out;
                        }
	}
	if (i > 0) {
		l[i] = NULL;
		*list = l;
		rc = 0;
		*count = i;
	}
out:

	fclose(fp);
	return rc;
}

/*
 * Frees the list of names returned in llapi_get_param()
 */
void
free_get_param_list(char **list)
{
	char **n;

	if (list == NULL)
		return;
	for (n = list; n && *n; n++)
		free(*n);

	free(list);
}
