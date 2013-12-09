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
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <liblustre.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

int llapi_nodemap_exists(const char *nodemap)
{
	char mapname[PATH_MAX + 1];

	snprintf(mapname, sizeof(mapname) - 1, "nodemap/%s", nodemap);

	return get_param(mapname, NULL, 0);
}

int llapi_search_nodemap_range(char *range)
{
	char buffer[PATH_MAX + 1];
	char *start, *end;
	__u32 start_range_id = 0, end_range_id = 0;

	snprintf(buffer, PATH_MAX, "%s", range);

	start = strtok(buffer, ":");
	if (start == NULL)
		return 1;

	end = strtok(NULL, ":");
	if (end == NULL)
		return 1;

	start_range_id = llapi_search_nodemap_nid(start);
	end_range_id = llapi_search_nodemap_nid(end);

	if (start_range_id != end_range_id)
		return 1;

	if ((start_range_id != 0) || (end_range_id != 0))
		return 1;

	return 0;
}

int llapi_find_nodemap_nid(char *nid, char *nodemap)
{
	char mapname[PATH_MAX + 1], buffer[PATH_MAX + 1];

	snprintf(mapname, sizeof(mapname) - 1, "nodemap/%s/test_nid", nodemap);

	set_param(mapname, nid);
	get_param(mapname, buffer, PATH_MAX);

	if (buffer == NULL)
		return -1;

	strncpy(nodemap, buffer, PATH_MAX);

	return 0;
}

int llapi_search_nodemap_nid(char *nid)
{
	char buffer[PATH_MAX + 1], mapname[PATH_MAX + 1];
	char *nodemap, *range_id_str;

	snprintf(mapname, sizeof(mapname) - 1, "nodemap/test_nid");

	set_param(mapname, nid);
	get_param(mapname, buffer, PATH_MAX);

	if (buffer == NULL)
		return -1;

	nodemap = strtok(buffer, ":");
	if (nodemap == NULL)
		return -1;

	range_id_str = strtok(NULL, ":");
	if (range_id_str == NULL)
		return -1;

	return atoi(range_id_str);
}
