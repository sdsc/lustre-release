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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include <lustre/lustre_user.h>
#include <lustre/lustreapi.h>

static void usage(char *prog)
{
	printf("usage: %s [-i mdt_index] dirnamebase count\n", prog);
	exit(1);
}

int main(int argc, char ** argv)
{
	int i, rc = 0, count;
	char *dirnamebase;
	char dirname[4096];
	int c;
	int stripe_offset = 0;
	int stripe_count = 0;
	int stripe_pattern = LMV_HASH_TYPE_FNV_1A_64;
	char *end;
	mode_t mode = 0444;

	while ((c = getopt(argc, argv, "i:")) != -1) {
		switch (c) {
		case 'i':
			stripe_offset = strtoul(optarg, &end, 0);
			if (*end != '\0') {
				fprintf(stderr, "invalid MDT index '%s'\n",
					optarg);
				return 1;
			}
			break;
		}
	}

	if (argc - optind < 2)
		usage(argv[0]);

	dirnamebase = argv[argc - 2];
	if (strlen(dirnamebase) > 4080) {
		printf("name too long\n");
		return 1;
	}

	count = strtoul(argv[argc - 1], NULL, 0);

	for (i = 0; i < count; i++) {
		snprintf(dirname, 4096, "%s-%d", dirnamebase, i);
		rc = llapi_dir_create_pool(dirname, mode,
					   stripe_offset, stripe_count,
					   stripe_pattern, NULL);
		if (rc) {
			printf("llapi_dir_create_poll(%s) error: %s\n",
			       dirname, strerror(-rc));
			break;
		}
		if ((i % 10000) == 0)
		    printf(" - created %d (time %ld)\n", i, time(0));
	}
	return rc;
}
