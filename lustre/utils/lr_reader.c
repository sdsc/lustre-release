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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/utils/lr_reader.c
 *
 * Author: Nathan Rutman <nathan@clusterfs.com>
 */
 /* Safely read the last_rcvd file from a device */

#if HAVE_CONFIG_H
#  include "config.h"
#endif /* HAVE_CONFIG_H */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <mntent.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>

#include <string.h>
#include <getopt.h>

#include <lustre_disk.h>
#include <lustre_ver.h>

char *progname;
static struct option const longopts[] = {
	{ "help", no_argument, 0, 'h' },
	{ "client", no_argument, 0, 'c' },
	{ "reply", no_argument, 0, 'r' },
	{ 0, 0, 0, 0}
};

int run_command(char *cmd)
{
        char log[] = "/tmp/mkfs_logXXXXXX";
        int fd, rc;
        
        
        if ((fd = mkstemp(log)) >= 0) {
                close(fd);
                strcat(cmd, " >");
                strcat(cmd, log);
        }
        strcat(cmd, " 2>&1");

        /* Can't use popen because we need the rv of the command */
        rc = system(cmd);
        if (rc && fd >= 0) {
                char buf[128];
                FILE *fp;
                fp = fopen(log, "r");
                if (fp) {
                        while (fgets(buf, sizeof(buf), fp) != NULL) {
                                if (rc) 
                                        printf("   %s", buf);
                        }
                        fclose(fp);
                }
        }
        if (fd >= 0) 
                remove(log);
        return rc;
}                                                       


void display_usage(void)
{
	printf("Usage: %s [OPTIONS] devicename\n", progname);
	printf("Read and print the last_rcvd file from a device\n");
	printf("(safe for mounted devices)\n");
	printf("\t-c, --client, display client information\n");
	printf("\t-h, --help,   display this help and exit\n");
	printf("\t-r, --reply,  display reply data information\n");
}


int main(int argc, char *const argv[])
{
        char tmpdir[] = "/tmp/dirXXXXXX";
        char cmd[128];
        char filepnm[128];
	char *dev;
        struct lr_server_data lsd;
	FILE *filep = NULL;
        int ret;
	int c;
	int opt_client = 0;
	int opt_reply = 0;
	bool is_mdt = false;
	struct lsd_client_data lcd;
	struct lsd_reply_header lrh;
	struct lsd_reply_data lrd;
	int client_idx;

	progname = argv[0];
	while ((c = getopt_long(argc, argv, "chr", longopts, NULL)) != -1) {
		switch (c) {
		case 'c':
			opt_client = 1;
			break;
		case 'r':
			opt_reply = 1;
			break;
		case 'h':
		default:
			display_usage();
			return -1;
		}
	}
	dev = argv[optind];
	if (!dev) {
		display_usage();
		return -1;
	}

        /* Make a temporary directory to hold Lustre data files. */
        if (!mkdtemp(tmpdir)) {
                fprintf(stderr, "%s: Can't create temporary directory %s: %s\n",
                        progname, tmpdir, strerror(errno));
                return errno;
        }

        memset(cmd, 0, sizeof(cmd));
	snprintf(cmd, sizeof(cmd),
                "%s -c -R 'dump /%s %s/%s' %s",
                DEBUGFS, LAST_RCVD, tmpdir, LAST_RCVD, dev);

        ret = run_command(cmd);
        if (ret) {
                fprintf(stderr, "%s: Unable to dump %s file\n",
                        progname, LAST_RCVD);
                goto out_rmdir;
        }

        sprintf(filepnm, "%s/%s", tmpdir, LAST_RCVD);
        filep = fopen(filepnm, "r");
        if (!filep) {
                fprintf(stderr, "%s: Unable to read old data\n",
                        progname);
                ret = -errno;
                goto out_rmdir;
        }

	/* read lr_server_data structure */
        printf("Reading %s\n", LAST_RCVD);
        ret = fread(&lsd, 1, sizeof(lsd), filep);
        if (ret < sizeof(lsd)) {
                fprintf(stderr, "%s: Short read (%d of %d)\n",
                        progname, ret, (int)sizeof(lsd));
                ret = -ferror(filep);
                if (ret) 
                        goto out_close;
        }

	/* swab structure fields of interest */
	lsd.lsd_feature_compat = le32_to_cpu(lsd.lsd_feature_compat);
	lsd.lsd_feature_incompat = le32_to_cpu(lsd.lsd_feature_incompat);
	lsd.lsd_feature_rocompat = le32_to_cpu(lsd.lsd_feature_rocompat);
	lsd.lsd_last_transno = le64_to_cpu(lsd.lsd_last_transno);
	lsd.lsd_osd_index = le32_to_cpu(lsd.lsd_osd_index);

	/* display */
	printf("UUID %s\n", lsd.lsd_uuid);
	printf("Feature compat=%#x\n", lsd.lsd_feature_compat);
	printf("Feature incompat=%#x\n", lsd.lsd_feature_incompat);
	printf("Feature rocompat=%#x\n", lsd.lsd_feature_rocompat);
	printf("Last transaction %llu\n", (long long)lsd.lsd_last_transno);
	printf("target index %u\n", lsd.lsd_osd_index);

	if ((lsd.lsd_feature_compat & OBD_COMPAT_OST) ||
	    (lsd.lsd_feature_incompat & OBD_INCOMPAT_OST)) {
		printf("OST, index %d\n", lsd.lsd_osd_index);
	} else if ((lsd.lsd_feature_compat & OBD_COMPAT_MDT) ||
		   (lsd.lsd_feature_incompat & OBD_INCOMPAT_MDT)) {
		/* We must co-locate so mgs can see old logs.
		   If user doesn't want this, they can copy the old
		   logs manually and re-tunefs. */
		printf("MDS, index %d\n", lsd.lsd_osd_index);
		is_mdt = true;
	} else  {
		/* If neither is set, we're pre-1.4.6, make a guess. */
		/* Construct debugfs command line. */
		memset(cmd, 0, sizeof(cmd));
		snprintf(cmd, sizeof(cmd), "%s -c -R 'rdump /%s %s' %s",
			 DEBUGFS, MDT_LOGS_DIR, tmpdir, dev);

		run_command(cmd);

		sprintf(filepnm, "%s/%s", tmpdir, MDT_LOGS_DIR);
		if (lsd.lsd_osd_index > 0) {
			printf("non-flagged OST, index %d\n",
			       lsd.lsd_osd_index);
		} else {
			/* If there's a LOGS dir, it's an MDT */
			if ((ret = access(filepnm, F_OK)) == 0) {
				/* Old MDT's are always index 0
				   (pre CMD) */
				printf("non-flagged MDS, index 0\n");
			} else {
				printf("non-flagged OST, index unknown\n");
			}
		}
	}

	/* read client information */
	if (opt_client) {
		lsd.lsd_client_start = le32_to_cpu(lsd.lsd_client_start);
		lsd.lsd_client_size = le32_to_cpu(lsd.lsd_client_size);
		printf("Per-client area start %u\n", lsd.lsd_client_start);
		printf("Per-client area size %u\n", lsd.lsd_client_size);

		/* seek to per-client data area */
		ret = fseek(filep, lsd.lsd_client_start, SEEK_SET);
		if (ret) {
			fprintf(stderr, "%s: seek failed. %s\n",
				progname, strerror(errno));
			ret = errno;
			goto out_close;
		}

		/* walk throuh the per-client data area */
		client_idx = -1;
		while (true) {
			client_idx++;

			/* read a per-client data area */
			ret = fread(&lcd, 1, sizeof(lcd), filep);
			if (ret < sizeof(lcd)) {
				if (feof(filep))
					break;
				fprintf(stderr, "%s: Short read (%d of %d)\n",
					progname, ret, (int)sizeof(lcd));
				ret = -ferror(filep);
				goto out_close;
			}

			if (lcd.lcd_uuid[0] == '\0')
				continue;

			/* swab structure fields */
			lcd.lcd_last_transno =
					le64_to_cpu(lcd.lcd_last_transno);
			lcd.lcd_last_xid = le64_to_cpu(lcd.lcd_last_xid);
			lcd.lcd_generation = le32_to_cpu(lcd.lcd_generation);
			if (is_mdt) {
				lcd.lcd_last_close_transno =
					le64_to_cpu(lcd.lcd_last_close_transno);
				lcd.lcd_last_close_xid =
					le64_to_cpu(lcd.lcd_last_close_xid);
			}

			/* display per-client data area */
			printf("\nClient idx %d\n", client_idx);
			printf("UUID %s\n", lcd.lcd_uuid);
			printf("generation %u\n", lcd.lcd_generation);
			printf("last transaction %llu\n",
			       (long long) lcd.lcd_last_transno);
			printf("last xid %llu\n",
			       (long long) lcd.lcd_last_xid);
			if (is_mdt) {
				printf("last close transation %llu\n",
				       (long long) lcd.lcd_last_close_transno);
				printf("last close xid %llu\n",
				       (long long) lcd.lcd_last_close_xid);
			}
		}
	}
	fclose(filep);
	filep = NULL;

	/* read reply data information */
	if (opt_reply && is_mdt) {
		snprintf(cmd, sizeof(cmd),
			 "%s -c -R 'dump /%s %s/%s' %s",
			 DEBUGFS, REPLY_DATA, tmpdir, REPLY_DATA, dev);

		ret = run_command(cmd);
		if (ret) {
			fprintf(stderr, "%s: Unable to dump %s file\n",
				progname, REPLY_DATA);
			goto out_rmdir;
		}

		snprintf(filepnm, sizeof(filepnm),
			 "%s/%s", tmpdir, REPLY_DATA);
		filep = fopen(filepnm, "r");
		if (!filep) {
			fprintf(stderr, "%s: Unable to read reply data\n",
				progname);
			ret = -errno;
			goto out_rmdir;
		}

		/* read reply_data header */
		printf("\nReading %s\n", REPLY_DATA);
		ret = fread(&lrh, 1, sizeof(lrh), filep);
		if (ret < sizeof(lrh)) {
			fprintf(stderr, "%s: Short read (%d of %d)\n",
				progname, ret, (int)sizeof(lrh));
			ret = -ferror(filep);
			if (ret)
				goto out_close;
		}

		/* check header */
		lrh.lrh_magic = le32_to_cpu(lrh.lrh_magic);
		lrh.lrh_header_size = le32_to_cpu(lrh.lrh_header_size);
		lrh.lrh_reply_size = le32_to_cpu(lrh.lrh_reply_size);
		if (lrh.lrh_magic != LRH_MAGIC ||
		    lrh.lrh_header_size != sizeof(struct lsd_reply_header) ||
		    lrh.lrh_reply_size != sizeof(struct lsd_reply_data)) {
			fprintf(stderr, "%s: Invalid header in %s file\n",
				progname, REPLY_DATA);
		}

		/* walk throuh the reply data */
		while (true) {
			/* read a reply data */
			ret = fread(&lrd, 1, sizeof(lrd), filep);
			if (ret < sizeof(lrd)) {
				if (feof(filep))
					break;
				fprintf(stderr, "%s: Short read (%d of %d)\n",
					progname, ret, (int)sizeof(lrd));
				ret = -ferror(filep);
				goto out_close;
			}

			/* XXX might be possible to filter valid reply data
			 * with client commited transno or xid */

			/* display reply data */
			lrd.lrd_transno = le64_to_cpu(lrd.lrd_transno);
			lrd.lrd_xid = le64_to_cpu(lrd.lrd_xid);
			lrd.lrd_data = le64_to_cpu(lrd.lrd_data);
			lrd.lrd_pre_versions[0] =
					le64_to_cpu(lrd.lrd_pre_versions[0]);
			lrd.lrd_pre_versions[1] =
					le64_to_cpu(lrd.lrd_pre_versions[1]);
			lrd.lrd_pre_versions[2] =
					le64_to_cpu(lrd.lrd_pre_versions[2]);
			lrd.lrd_pre_versions[3] =
					le64_to_cpu(lrd.lrd_pre_versions[3]);
			lrd.lrd_result = le32_to_cpu(lrd.lrd_result);
			lrd.lrd_client_idx = le32_to_cpu(lrd.lrd_client_idx);
			lrd.lrd_generation = le32_to_cpu(lrd.lrd_generation);
			lrd.lrd_tag = le16_to_cpu(lrd.lrd_tag);

			printf("\ntransaction %llu\n", lrd.lrd_transno);
			printf("xid %llu\n", lrd.lrd_xid);
			printf("data %llu\n", lrd.lrd_data);
			printf("pre_version0 %llu\n", lrd.lrd_pre_versions[0]);
			printf("pre_version1 %llu\n", lrd.lrd_pre_versions[1]);
			printf("pre_version2 %llu\n", lrd.lrd_pre_versions[2]);
			printf("pre_version3 %llu\n", lrd.lrd_pre_versions[3]);
			printf("result %u\n", lrd.lrd_result);
			printf("tag %hu\n", lrd.lrd_tag);
			printf("client idx %u\n", lrd.lrd_client_idx);
			printf("generation %u\n", lrd.lrd_generation);
		}

	}

out_close:
	if (filep != NULL)
		fclose(filep);

out_rmdir:
	memset(cmd, 0, sizeof(cmd));
	sprintf(cmd, "rm -rf %s", tmpdir);
	run_command(cmd);
	return ret;
}
