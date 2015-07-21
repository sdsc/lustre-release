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

#include "libobe.h"
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

unsigned long total = TOTAL; /* total transactions */
int threads = THREADS; /* threads */
int packsize = PACKSIZE;
int use_index;
int device = -1;
__u64 sequence = FID_SEQ_NORMAL; /* starting sequence */

/* Not used; declared for fulfilling obd.c's dependency. */
command_t cmdlist[0];

int obe_create_object(struct lu_fid *fid, int mode, int type,
		      int keysize, int recsize)
{
	struct obe_request *our;
	struct lu_attr attr;
	struct dt_object_format dof;
	struct dt_index_features feat;
	int rc;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = mode;
	attr.la_uid = 0;
	attr.la_gid = 0;

	memset(&dof, 0, sizeof(dof));
	dof.dof_type = type;
	if (type == DFT_INDEX) {
		feat.dif_flags = 0;
		feat.dif_keysize_min = keysize;
		feat.dif_keysize_max = keysize;
		feat.dif_recsize_min = recsize;
		feat.dif_recsize_max = recsize;
		feat.dif_ptrsize = 4;
		dof.u.dof_idx.di_feat = &feat;
	}

	our = obe_new(device);
	if (our == NULL)
		return -ENOMEM;
	obe_add_update(our, 1, create, fid, &attr, NULL, &dof);
	rc = obe_exec(our);
	obe_free(our);
	if (rc < 0)
		fprintf(stderr, "can't create: %d\n", errno);
	return rc;
}

int obe_attr_get(struct lu_fid *fid, struct lu_attr *attr)
{
	struct obe_request *our;
	struct lu_buf lb;
	int rc;

	our = obe_new(device);
	if (our == NULL)
		return -ENOMEM;
	obe_add_update(our, 1, attr_get, fid);
	rc = obe_exec(our);
	if (rc < 0)
		fprintf(stderr, "attr_get: %d\n", rc);
	rc = object_update_result_data_get(our->our_reply, &lb, 0);
	if (rc == 0) {
		LASSERT(sizeof(*attr) == lb.lb_len);
		memcpy(attr, lb.lb_buf, sizeof(*attr));
	}
	obe_free(our);
	return rc;
}

int obe_xattr_get(struct lu_fid *fid, const char *name, struct lu_buf *lb)
{
	struct obe_request *our;
	struct lu_buf tmp;
	int rc;

	our = obe_new(device);
	if (our == NULL)
		return -ENOMEM;
	obe_add_update(our, 1, xattr_get, fid, name, lb->lb_len);
	rc = obe_exec(our);
	if (rc < 0)
		fprintf(stderr, "xattr_get: %d\n", rc);
	rc = object_update_result_data_get(our->our_reply, &tmp, 0);
	if (rc >= 0) {
		if (tmp.lb_len > lb->lb_len) {
			printf("ffffffffffffff\n");
			exit(-1);
		}
		memcpy(lb->lb_buf, tmp.lb_buf, rc);
	}
	obe_free(our);
	return rc;
}

/* this test simulates last_rcvd being a regular file or an index */
void *create_file_test(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb, lb2;
	struct lu_attr attr, attr2;
	int i, j, rc, offset;
	struct lu_fid dir_fid;
	struct dt_insert_rec direntry;
	char filename[128];

	tj->fid.f_oid++;
	dir_fid = tj->fid;
	rc = obe_create_object(&dir_fid, S_IFDIR, DFT_DIR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create dir: %d\n", errno);
		return NULL;
	}

	/* insert ./.. into the directory */
	our = obe_new(device);
	snprintf(filename, sizeof(filename), ".");
	direntry.rec_fid = &tj->fid;
	direntry.rec_type = S_IFDIR;
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	snprintf(filename, sizeof(filename), "..");
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	rc = obe_exec(our);
	obe_free(our);
	if (rc < 0) {
		fprintf(stderr, "can't insert ./..: %d\n", errno);
		return NULL;
	}

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	attr2.la_valid = LA_CTIME;
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
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

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
			obe_add_update(our, i, write, &tj->fid2, &lb2,
				       offset + (i * 128));
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
}

/* this test simulates mdd_create() for 0-striped files */
void *mdd_create_mknod(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb;
	struct lu_attr attr, attr2;
	int i, j, rc;
	struct lu_fid dir_fid;
	struct dt_insert_rec direntry;
	char filename[128];

	tj->fid.f_oid++;
	dir_fid = tj->fid;
	rc = obe_create_object(&dir_fid, S_IFDIR, DFT_DIR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create dir: %d\n", errno);
		return NULL;
	}

	/* insert ./.. into the directory */
	our = obe_new(device);
	snprintf(filename, sizeof(filename), ".");
	direntry.rec_fid = &tj->fid;
	direntry.rec_type = S_IFDIR;
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	snprintf(filename, sizeof(filename), "..");
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	rc = obe_exec(our);
	obe_free(our);
	if (rc < 0) {
		fprintf(stderr, "can't insert ./..: %d\n", errno);
		return NULL;
	}

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	attr2.la_valid = LA_CTIME;
	attr2.la_ctime = 11111111;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			/* create and initialize the object */
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LINK, 0);

			/* insert name into the directory */
			snprintf(filename, sizeof(filename), "f-%d",
				 j * packsize + i);
			direntry.rec_fid = &tj->fid;
			direntry.rec_type = S_IFREG;
			obe_add_update(our, i, index_insert, &dir_fid,
				       (struct dt_rec *)&direntry,
				       (struct dt_key *)filename);
			obe_add_update(our, i, attr_set, &dir_fid, &attr2);
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
}

/* this test simulates mdd_create() for 1-striped files */
void *mdd_create_striped(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb;
	struct lu_attr attr, attr2;
	int i, j, rc;
	struct lu_fid dir_fid;
	struct dt_insert_rec direntry;
	char filename[128];

	tj->fid.f_oid++;
	dir_fid = tj->fid;
	rc = obe_create_object(&dir_fid, S_IFDIR, DFT_DIR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create dir: %d\n", errno);
		return NULL;
	}

	/* insert ./.. into the directory */
	our = obe_new(device);
	snprintf(filename, sizeof(filename), ".");
	direntry.rec_fid = &tj->fid;
	direntry.rec_type = S_IFDIR;
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	snprintf(filename, sizeof(filename), "..");
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	rc = obe_exec(our);
	obe_free(our);
	if (rc < 0) {
		fprintf(stderr, "can't insert ./..: %d\n", errno);
		return NULL;
	}

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	attr2.la_valid = LA_CTIME;
	attr2.la_ctime = 11111111;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

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
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
}

/* this test simulates mdd_create() for 1-striped files */
void *mdt_create_striped(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_buf lb, lb2;
	struct lu_attr attr, attr2;
	int i, j, rc, offset;
	struct lu_fid dir_fid;
	struct dt_insert_rec direntry;
	char filename[128];

	tj->fid.f_oid++;
	dir_fid = tj->fid;
	rc = obe_create_object(&dir_fid, S_IFDIR, DFT_DIR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create dir: %d\n", errno);
		return NULL;
	}

	/* insert ./.. into the directory */
	our = obe_new(device);
	snprintf(filename, sizeof(filename), ".");
	direntry.rec_fid = &tj->fid;
	direntry.rec_type = S_IFDIR;
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	snprintf(filename, sizeof(filename), "..");
	obe_add_update(our, 1, index_insert, &dir_fid,
			(struct dt_rec *)&direntry,
			(struct dt_key *)filename);
	rc = obe_exec(our);
	obe_free(our);
	if (rc < 0) {
		fprintf(stderr, "can't insert ./..: %d\n", errno);
		return NULL;
	}

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	attr2.la_valid = LA_CTIME;
	attr2.la_ctime = 11111111;

	lb.lb_buf = "0123456789";
	lb.lb_len = strlen(lb.lb_buf);

	lb2.lb_buf = (void *)&attr;
	lb2.lb_len = 32;
	offset = tj->thread_id * packsize * 32;

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			/* create and initialize the object */
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
			lb.lb_len = strlen(lb.lb_buf);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LINK, 0);
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_LOV, 0);
			lb.lb_len = 8;
			obe_add_update(our, i, xattr_set, &tj->fid, &lb,
				       XATTR_NAME_VERSION, 0);

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
			obe_add_update(our, i, write, &tj->fid2, &lb2,
				       offset + (i * 32));
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
}
void *create_object_test(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_attr attr;
	int i, j, rc;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			/* create and initialize the object */
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
}

void *create_destroy_object_test(void *arg)
{
	struct thread_job *tj = arg;
	struct obe_request *our;
	struct lu_attr attr;
	int i, j, rc, init_oid = tj->fid.f_oid;

	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			/* create and initialize the object */
			obe_add_update(our, i, create, &tj->fid, &attr,
				       NULL, NULL);
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	tj->fid.f_oid = init_oid;

	for (j = 0; j < (total / threads / packsize); j++) {

		our = obe_new(device);
		if (our == NULL) {
			fprintf(stderr, "can't allocate request\n");
			return NULL;
		}

		for (i = 1; i <= packsize; i++) {
			tj->fid.f_oid++;

			/* create and initialize the object */
			obe_add_update(our, i, ref_del, &tj->fid);
			obe_add_update(our, i, object_destroy, &tj->fid);
		}

		rc = obe_exec(our);
		obe_free(our);
		if (rc < 0) {
			fprintf(stderr, "can't execute %d: %d\n", j, errno);
			return NULL;
		}
	}

	return NULL;
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
	       "\t-f <test> - run specific load\n"
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
	{ "test", 1, NULL, 'f' },
	{ NULL },
};

struct obe_test {
	char *testname;
	void* (*func)(void *);
};

static struct obe_test tests[] = {
	{ "create_file", create_file_test },
	{ "mdd_create_mknod", mdd_create_mknod },
	{ "mdd_create_striped", mdd_create_striped },
	{ "mdt_create_striped", mdt_create_striped },
	{ "create_object", create_object_test },
	{ "create_destroy_object", create_destroy_object_test },
	{ NULL, NULL } };

const char optstring[] = "hT:t:p:Id:s:f:";

int main(int argc, char *argv[])
{
	int rc, i, c;
	struct thread_job *tj;
	pthread_t *th;
	void (*testfunc) = NULL;
	char *testname = NULL;
	//struct lu_attr attr;
	struct lu_fid fid2;
	struct timeval start;
	struct timeval end;
	long secs_used, elapsed;


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
					device);
				exit(-1);
			}
			break;
		case 'f':
			i = 0;
			while (tests[i].testname != NULL) {
				if (strcmp(tests[i].testname, optarg) == 0) {
					testfunc = tests[i].func;
					testname = tests[i].testname;
					break;
				}
				i++;
			}
			if (testfunc == NULL)
				fprintf(stderr, "test %s isn't found\n",
					optarg);
			break;
		}
	}

	if (testfunc == NULL) {
		usage(argv[0]);
		exit(-1);
	}

	fid2.f_seq = sequence;
	fid2.f_oid = 1;
	fid2.f_ver = 0;
	rc = obe_create_object(&fid2, S_IFREG, DFT_REGULAR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create regular object\n");
		exit(-1);
	}
#if 0
	rc = obe_attr_get(&fid, &attr);
	if (rc < 0) {
		fprintf(stderr, "can't attr_get\n");
		exit(-1);
	}
	printf("ATTR: valid %llo, mode %o, size %llu, nlink %d\n",
		attr.la_valid, attr.la_mode, attr.la_size, attr.la_nlink);
	{
		char buf[1024];
		struct lu_buf lb;
		lb.lb_buf = buf;
		lb.lb_len = sizeof(buf);
		rc = obe_xattr_get(&fid, XATTR_NAME_LMA, &lb);
		printf("LMA EA: %d\n", rc);
		lb.lb_buf = buf;
		lb.lb_len = sizeof(buf);
		rc = obe_xattr_get(&fid, XATTR_NAME_LINK, &lb);
		printf("LINK EA: %d\n", rc);

	}

	fid.f_seq = sequence;
	fid.f_oid = 2;
	fid.f_ver = 0;
	if (use_index)
		fid2 = fid;
	rc = obe_create_object(&fid, S_IFREG, DFT_INDEX, 8, 32);
	if (rc < 0) {
		fprintf(stderr, "can't create index object\n");
		exit(-1);
	}
	rc = obe_attr_get(&fid, &attr);
	if (rc < 0) {
		fprintf(stderr, "can't attr_get\n");
		exit(-1);
	}
	printf("ATTR: valid %llo, mode %o, size %llu, nlink %d\n",
		attr.la_valid, attr.la_mode, attr.la_size, attr.la_nlink);
#endif

	tj = malloc(sizeof(tj[0]) * threads);
	assert(tj != NULL);
	th = malloc(sizeof(th[0]) * threads);
	assert(th != NULL);

	printf("Running %s ...", testname); fflush(stdout);

	gettimeofday(&start, NULL);

	for (i = 0; i < threads; i++) {
		tj[i].thread_id = i;
		tj[i].fid.f_seq = sequence + (i * 10) + 1;
		tj[i].fid.f_oid = i * 1000;
		tj[i].fid.f_ver = 0;
		tj[i].fid2 = fid2;
		tj[i].use_index = use_index;
		rc = pthread_create(&th[i], NULL, testfunc, &tj[i]);
		if (rc) {
			fprintf(stderr, "error create thread 1\n");
			exit(-1);
		}
	}
	for (i = 0; i < threads; i++)
		pthread_join(th[i], NULL);

	gettimeofday(&end, NULL);
	secs_used = (end.tv_sec - start.tv_sec);
	elapsed = ((secs_used*1000000) + end.tv_usec) - (start.tv_usec);

	printf(" DONE in %lu %lu ms\n", elapsed, elapsed / 1000); fflush(stdout);

	printf("%lu transactions in %d threads in %.2f.sec - %.2f / sec\n",
		total, threads, (float)elapsed / 1000000,
		(float)total * 1000000 / elapsed);

	exit(0);
}
