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
#include <pthread.h>

#include "obelib.h"
#include <lustre_ioctl.h>
#include <lnet/lnetctl.h>
#include <libcfs/util/string.h>
#include <libcfs/util/parser.h>
#include <lustre/lustreapi.h>

#define TRANSACTIONS	200000
#define TX_PER_REQ	5
#define MAX_THREADS	10

struct thread_job {
	struct lu_fid fid;
	struct lu_fid fid2;
	int thread_id;
};

/* Not used; declared for fulfilling obd.c's dependency. */
command_t cmdlist[0];
int device = -1;

void* create_with_write(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb, lb2;
	struct lu_attr attr;
	int i, j, rc, offset;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	lb2.lb_buf = (void *)&attr;
	lb2.lb_len = 128;

	offset = tj->thread_id * TX_PER_REQ * 128;

	for (j = 0; j < (TRANSACTIONS / MAX_THREADS / TX_PER_REQ); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return;
		}

		for (i = 1; i <= TX_PER_REQ; i++) {
			tj->fid.f_oid++;
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LINK, 0);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LOV, 0);
			obe_add_update(our, i, write, &tj->fid2, &lb2,
					offset + (i * 128));
		}

		rc = obe_exec(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return;
		}
	}

	return;
}

/* this test simulates last_rcvd being an index */
void* create_with_insert_delete(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb, lb2;
	struct lu_attr attr;
	int i, j, rc, offset;
	char slotname[128];

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	lb2.lb_buf = (void *)&attr;
	lb2.lb_len = 128;

	offset = tj->thread_id * TX_PER_REQ * 128;

	for (j = 0; j < (TRANSACTIONS / MAX_THREADS / TX_PER_REQ); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return;
		}

		for (i = 1; i <= TX_PER_REQ; i++) {
			tj->fid.f_oid++;
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LINK, 0);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LOV, 0);
			rc = snprintf(slotname, sizeof(slotname), "%03d-%03d",
				      tj->thread_id, i);
			obe_add_update(our, i, index_bin_insert, &tj->fid2,
					(struct dt_rec *)slotname, 32,
					(struct dt_key *)slotname, rc + 1);
		}

		rc = obe_exec(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return;
		}

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return;
		}
		for (i = 1; i <= TX_PER_REQ; i++) {
			rc = snprintf(slotname, sizeof(slotname), "%03d-%03d",
				      tj->thread_id, i);
			obe_add_update(our, i, index_bin_delete, &tj->fid2,
					(struct dt_key *)slotname, rc + 1);
		}
		rc = obe_exec(our);
		if (rc < 0) {
			fprintf(stderr, "can't delete %d: %d\n", j, errno);
			return;
		}
	}

	return;
}

int obe_create_object(struct lu_fid *fid, int mode, int type)
{
	struct obe_request *our;
	struct lu_attr attr;
	struct dt_object_format dof;
	int rc;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = mode;
	attr.la_uid = 0;
	attr.la_gid = 0;

	memset(&dof, 0, sizeof(dof));
	dof.dof_type = type;

	our = obe_new(device);
	if (our == NULL)
		return -ENOMEM;
	obe_add_update(our, 1, create, fid, &attr, NULL, &dof);
	rc = obe_exec(our);
	if (rc < 0)
		fprintf(stderr, "can't create: %d\n", errno);
	return rc;
}

int main(int argc, char *argv[])
{
	struct obe_request *our;
	clock_t start, end;
	int rc, i, j;
	struct thread_job tj[MAX_THREADS];
	pthread_t th[MAX_THREADS];

	struct lu_fid fid, fid2;
	struct lu_attr attr;

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
	rc = obe_create_object(&fid, S_IFREG, DFT_REGULAR);
	if (rc < 0) {
		fprintf(stderr, "can't create object\n");
		exit(-1);
	}

	fid.f_seq = FID_SEQ_NORMAL;
	fid.f_oid = 2;
	fid.f_ver = 0;
	fid2 = fid;
	rc = obe_create_object(&fid, S_IFREG, DFT_INDEX);
	if (rc < 0) {
		fprintf(stderr, "can't create object\n");
		exit(-1);
	}

	start = clock();

	for (i = 0; i < MAX_THREADS; i++) {
		tj[i].thread_id = i;
		tj[i].fid.f_seq = FID_SEQ_NORMAL + i + 1;
		tj[i].fid.f_oid = 1;
		tj[i].fid.f_ver = 0;
		tj[i].fid2 = fid2;
		rc = pthread_create(&th[i], NULL, create_with_write, &tj[i]);
		//rc = pthread_create(&th[i], NULL, create_with_insert_delete, &tj[i]);
		if (rc) {
			fprintf(stderr, "error create thread 1\n");
			exit(-1);
		}
	}
	for (i = 0; i < MAX_THREADS; i++)
		pthread_join(th[i], NULL);

	end = clock();
	printf("Elapsed time: %.2f.\n", (float)(end - start) / CLOCKS_PER_SEC);
	printf("%.2f transactions/sec\n",
		(float)TRANSACTIONS/(float)((end - start) / CLOCKS_PER_SEC));

	exit(0);
}
