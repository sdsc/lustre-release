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
 * unit tests for OSD API
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

#include "../utils/libobe.h"
#include <lustre_ioctl.h>
#include <lnet/lnetctl.h>
#include <libcfs/util/string.h>
#include <libcfs/util/parser.h>
#include <lustre/lustreapi.h>

int use_index;
int device = -1;
__u64 sequence = FID_SEQ_NORMAL; /* starting sequence */
__u64 foid = 1;

/* Not used; declared for fulfilling obd.c's dependency. */
command_t cmdlist[0];

int create_object(struct lu_fid *fid, struct lu_attr *attr, int type,
		  int keysize, int recsize)
{
	struct obe_request *our;
	struct dt_object_format dof;
	struct dt_index_features feat;
	int rc;

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
	obe_add_update(our, 1, create, fid, attr, NULL, &dof);
	rc = obe_exec(our);
	if (rc < 0)
		fprintf(stderr, "can't create: %d\n", errno);
	else
		rc = object_update_result_get_rc(our->our_reply, 0);
	obe_free(our);
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

void test_create(void)
{
	struct lu_fid fid;
	struct lu_attr attr;
	int rc;

	memset(&attr, 0, sizeof(attr));
	attr.la_valid = LA_MODE | LA_TYPE | LA_UID | LA_GID;
	attr.la_mode = S_IFREG;
	attr.la_uid = 0;
	attr.la_gid = 0;

	fid.f_seq = sequence;
	fid.f_oid = foid;
	fid.f_ver = 0;

	rc = create_object(&fid, &attr, DFT_REGULAR, 0, 0);
	if (rc < 0) {
		fprintf(stderr, "can't create regular object\n");
		exit(-1);
	}

	rc = create_object(&fid, &attr, DFT_REGULAR, 0, 0);
	if (rc != -EEXIST) {
		fprintf(stderr, "should faild: %d\n", rc);
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
	{ "sequence", 1, NULL, 's' },
	{ NULL },
};

const char optstring[] = "hd:s:";

int main(int argc, char *argv[])
{
	int c;

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
		case 's':
			sequence += strtoul(optarg, NULL, 0);
			break;
		case 'd':
			device = parse_devname(argv[0], optarg);
			if (device < 0) {
				fprintf(stderr, "can't find device %d\n",
					device);
				exit(-1);
			}
			break;
		}
	}

	test_create();

	exit(0);
}
