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
 * Copyright (c) 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/utils/osdbench.c
 *
 * Author: Alexey Zhuravlev <alexey.zhuravlev@intel.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <limits.h>
#include <utime.h>

#include "obelib.h"
#include <lustre_ioctl.h>
#include <lnet/lnetctl.h>
#include <libcfs/util/string.h>
#include <libcfs/util/parser.h>
#include <lustre/lustreapi.h>

/* Not used; declared for fulfilling obd.c's dependency. */
command_t cmdlist[0];

int main(int argc, char *argv[])
{
	struct obe_request *our;
	clock_t start, end;
	int device;
	int rc, i, j;

	struct lu_fid fid, fid2;
	struct lu_attr attr;
	struct lu_buf lb, lb2;

	if (obd_initialize(argc, argv) < 0) {
		fprintf(stderr, "obd_initialize failed.\n");
		exit(-1);
	}

	device = parse_devname(argv[0], argv[1]);
	if (device < 0) {
		fprintf(stderr, "can't find device %d\n", argv[1]);
		exit(-1);
	}

	fid.f_seq = FID_SEQ_NORMAL;
	fid.f_oid = 1;
	fid.f_ver = 0;
	fid2 = fid;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	lb2.lb_buf = (void *)&attr;
	lb2.lb_len = 128;

	our = obe_new(device);
	if (our == NULL) {
		fprintf(stderr, "can't allocate request\n");
		exit(-1);
	}
	obe_add_update(our, i, create, &fid2, &attr, NULL, NULL);
	rc = obe_exec(our);
	if (rc < 0) {
		fprintf(stderr, "can't execute %d: %d\n", j, errno);
		exit(-1);
	}

	start = clock();

	for (j = 0; j < 10000; j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			exit(-1);
		}

		for (i = 1; i <= 5; i++) {
			fid.f_oid++;
			obe_add_update(our, i, create, &fid, &attr, NULL, NULL);
			obe_add_update(our, i, xattr_set, &fid, &lb, XATTR_NAME_LINK, 0);
			obe_add_update(our, i, xattr_set, &fid, &lb, XATTR_NAME_LOV, 0);
			obe_add_update(our, i, write, &fid2, &lb2, i * 128);
		}

		rc = obe_exec(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			exit(-1);
		}
	}
	end = clock();
	printf("Elapsed time: %.2f.\n", (float)(end - start) / CLOCKS_PER_SEC);

	exit(0);
}
