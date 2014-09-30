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
#include <libcfs/libcfs.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

int llapi_nodemap_exists(const char *nodemap)
{
	char **list = NULL;
	size_t count;
	int rc;

	rc = llapi_get_param(&list, &count, "nodemap.%s", nodemap);

	if (list != NULL)
		free_get_param_list(list);
	return rc;

}
