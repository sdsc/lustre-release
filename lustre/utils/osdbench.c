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
 * a benchmark to evaluate last_rcvd being a regular file or an index
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

#define TOTAL		200000
#define PACKSIZE	10
#define THREADS		20

struct thread_job {
	struct lu_fid fid;
	struct lu_fid fid2;
	int use_index;
	int thread_id;
};

int total = TOTAL; /* total transactions */
int threads = THREADS; /* threads */
int packsize = PACKSIZE;
int use_index;
int device = -1;
__u64 sequence = FID_SEQ_NORMAL; /* starting sequence */

/* Not used; declared for fulfilling obd.c's dependency. */
command_t cmdlist[0];

/* this test simulates last_rcvd being a regular file or an index */
void *create_test(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb, lb2;
	struct lu_attr attr, attr2;
	int i, j, rc, offset;
	char slot[128];
	struct lu_fid dir_fid;
	struct dt_insert_rec direntry;
	char filename[128];

	dir_fid = tj->fid;
	rc = obe_create_object(&dir_fid, S_IFDIR, DFT_DIR);
	if (rc < 0) {
		fprintf(stderr, "can't create dir: %d\n", errno);
		return;
	}
	tj->fid.f_oid++;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	attr2.la_valid = CTIME;
	attr2.la_ctime = 11111111;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	lb2.lb_buf = (void *)&attr;
	lb2.lb_len = 128;

	offset = tj->thread_id * packsize * 128;

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			if (tj->use_index) {
				rc = snprintf(slot, sizeof(slot), "%03d-%03d",
					      tj->thread_id, i);
				obe_add_update(our, i, index_bin_insert,
					       &tj->fid2,
					       (struct dt_rec *)slot, 32,
					       (struct dt_key *)slot, rc + 1);
			}

			/* create and initialize the object */
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LINK, 0);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LOV, 0);

			/* insert name into the directory */
			snprintf(filename, sizeof(filename), "f-%d",
				 j * packsize + i);
			direntry.rec_fid = &tj->fid;
			direntry.rec_type = S_IFREG;
			obe_add_update(our, i, index_insert, &dir_fid,
				       (struct dt_rec *)&direntry,
				       (struct dt_key *)filename);
			obe_add_update(our, i, attr_set, &dir_fid, &attr2);

			/* last_rcvd update: regular file or index */
			if (tj->use_index) {
				obe_add_update(our, i, index_bin_delete,
					       &tj->fid2,
					       (struct dt_key *)slot, rc + 1);
			} else {
				obe_add_update(our, i, write, &tj->fid2, &lb2,
						offset + (i * 128));
			}
		}

		rc = obe_exec(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
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

void usage(char *progname)
{
	printf("Usage %s:\n"
	       "\t-d <device's name>\n"
	       "\t-T <total transactions>\n"
	       "\t-t <threads to use>\n"
	       "\t-p <transactions per ioctl\n"
	       "\t-s <sequence offset to FID_SEQ_NORMAL>\n"
	       "\t-I - use index as multislot\n"
	       , progname);
}

struct option opts[] = {
	{ "help",   0, NULL, 'h' },
	{ "total", 1, NULL, 'T' },
	{ "threads", 1, NULL, 't' },
	{ "packsize",   1, NULL, 'p' },
	{ "index",  0, NULL, 'I' },
	{ "device", 1, NULL, 'd' },
	{ "sequence", 1, NULL, 's' },
	{ NULL },
};

const char optstring[] = "hT:t:p:Id:s:";

int main(int argc, char *argv[])
{
	struct obe_request *our;
	clock_t start, end;
	int rc, i, j, c;
	struct thread_job *tj;
	pthread_t *th;
	float elapsed;

	struct lu_fid fid, fid2;

	if (obd_initialize(argc, argv) < 0) {
		fprintf(stderr, "obd_initialize failed.\n");
		exit(-1);
	}
	while ((c = getopt_long(argc, argv, optstring, opts, NULL)) != -1) {
		switch (c) {
		case 'h':
		case '?':
			usage(argv[0]);
			exit(0);
			break;
		case 'T':
			total = strtoul(optarg, NULL, 0);
			break;
		case 't':
			threads = strtoul(optarg, NULL, 0);
			break;
		case 'p':
			packsize = strtoul(optarg, NULL, 0);
			break;
		case 's':
			sequence += strtoul(optarg, NULL, 0);
			break;
		case 'I':
			use_index = 1;
			break;
		case 'd':
			device = parse_devname(argv[0], optarg);
			if (device < 0) {
				fprintf(stderr, "can't find device %d\n",
					optarg);
				exit(-1);
			}
			break;
		}
	}

	fid.f_seq = sequence;
	fid.f_oid = 1;
	fid.f_ver = 0;
	fid2 = fid;
	rc = obe_create_object(&fid, S_IFREG, DFT_REGULAR);
	if (rc < 0) {
		fprintf(stderr, "can't create regular object\n");
		exit(-1);
	}

	fid.f_seq = sequence;
	fid.f_oid = 2;
	fid.f_ver = 0;
	if (use_index)
		fid2 = fid;
	rc = obe_create_object(&fid, S_IFREG, DFT_INDEX);
	if (rc < 0) {
		fprintf(stderr, "can't create index object\n");
		exit(-1);
	}

	tj = malloc(sizeof(tj[0]) * threads);
	assert(tj != NULL);
	th = malloc(sizeof(th[0]) * threads);
	assert(th != NULL);

	start = clock();

	for (i = 0; i < threads; i++) {
		tj[i].thread_id = i;
		tj[i].fid.f_seq = sequence + i + 1;
		tj[i].fid.f_oid = 1;
		tj[i].fid.f_ver = 0;
		tj[i].fid2 = fid2;
		tj[i].use_index = use_index;
		rc = pthread_create(&th[i], NULL, create_test, &tj[i]);
		if (rc) {
			fprintf(stderr, "error create thread 1\n");
			exit(-1);
		}
	}
	for (i = 0; i < threads; i++)
		pthread_join(th[i], NULL);

	elapsed = (float)(clock() - start);
	printf("%llu transactions in %d threads in %.2f.sec\n",
		total, threads, elapsed / CLOCKS_PER_SEC);
	printf("%.2f transactions/sec\n",
		(float)total / (elapsed / CLOCKS_PER_SEC));

	exit(0);
}
