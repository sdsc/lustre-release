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
 *
 * Copyright (c) 2014 Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>

static void usage(char *prog)
{
	printf("usage: %s {-o [-c]|-m|-d|-l<tgt>} [-r unlinkfmt ] filenamefmt count [-seconds]\n", prog);
	printf("       %s {-o [-c]|-m|-d|-l<tgt>} [-r unlinkfmt ] filenamefmt start count [-seconds]\n", prog);
	printf("       %s {-o [-c]|-m|-d|-l<tgt>} [-r unlinkfmt ] filenamefmt -seconds\n", prog);

	printf("\t-o\topen(O_CREAT) files\n");
	printf("\t-c\tdefer close() files until after all files created\n");
	printf("\t-m\tmknod() files (don't create OST objects)\n");
	printf("\t-d\tmkdir() directories\n");
	printf("\t-l\tlink() files to existing <tgt> file\n");
	printf("\t-r\tunlink() files with new path (':' to use same path)\n");

	exit(EXIT_FAILURE);
}

static char *get_file_name(const char *fmt, long n, int has_fmt_spec)
{
        static char filename[4096];
        int bytes;

        bytes = has_fmt_spec ? snprintf(filename, 4095, fmt, n) :
                snprintf(filename, 4095, "%s%ld", fmt, n);
        if (bytes >= 4095) {
                printf("file name too long\n");
                exit(EXIT_FAILURE);
        }
        return filename;
}

double now(void)
{
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (double)tv.tv_sec + (double)tv.tv_usec / 1000000;
}

int main(int argc, char ** argv)
{
	int do_open = 0, defer_close = 0, do_link = 0, do_mkdir = 0;
	int do_unlink = 0, do_mknod = 0;
	char *filename;
	char *fmt = NULL, *fmt_unlink = NULL, *tgt = NULL;
	double start, last;
	long begin = 0, end = ~0UL >> 1, count = ~0UL >> 1;
	int has_fmt_spec = 0, unlink_has_fmt_spec = 0;
	long i, total;
	int rc = 0, c, last_fd = -1, stderr_fd;

        /* Handle the last argument in form of "-seconds" */
        if (argc > 1 && argv[argc - 1][0] == '-') {
                char *endp;

                argc--;
                end = strtol(argv[argc] + 1, &endp, 0);
                if (end <= 0 || *endp != '\0')
                        usage(argv[0]);
                end = end + time(NULL);
        }

	while ((c = getopt(argc, argv, "cdl:mor:")) != -1) {
		switch (c) {
		case 'c':
			defer_close++;
			break;
		case 'd':
			do_mkdir++;
			break;
		case 'l':
			do_link++;
			tgt = optarg;
			break;
		case 'm':
			do_mknod++;
			break;
		case 'o':
			do_open++;
			break;
		case 'r':
			do_unlink++;
			fmt_unlink = optarg;
			break;
		case '?':
			fprintf(stderr, "Unknown option '%c'\n", optopt);
			usage(argv[0]);
		}
	}

	if (do_open + do_mkdir + do_link + do_mknod != 1) {
		fprintf(stderr, "error: can only use one of -o, -m, -l, -d\n");
		usage(argv[0]);
	}

	if (do_unlink > 1) {
		fprintf(stderr, "error: can only use -r once\n");
		usage(argv[0]);
	}

	if (!do_open && defer_close) {
		fprintf(stderr, "error: can only -c with -o\n");
		usage(argv[0]);
	}

        switch (argc - optind) {
        case 3:
                begin = strtol(argv[argc - 2], NULL, 0);
        case 2:
                count = strtol(argv[argc - 1], NULL, 0);
                if (end != ~0UL >> 1)
                        usage(argv[0]);
        case 1:
                fmt = argv[optind];
                break;
        default:
                usage(argv[0]);
        }

	if (do_unlink && strcmp(fmt_unlink, "="))
		fmt_unlink = fmt;

        start = last = now();

        has_fmt_spec = strchr(fmt, '%') != NULL;
        if (do_unlink)
                unlink_has_fmt_spec = strchr(fmt_unlink, '%') != NULL;

        for (i = 0; i < count && time(NULL) < end; i++, begin++) {
                filename = get_file_name(fmt, begin, has_fmt_spec);
                if (do_open) {
                        int fd = open(filename, O_CREAT|O_RDWR, 0644);
                        if (fd < 0) {
                                printf("open(%s) error: %s\n", filename,
                                       strerror(errno));
                                rc = errno;
                                break;
                        }
			if (!defer_close)
				close(fd);
			else if (fd > last_fd)
				last_fd = fd;
                } else if (do_link) {
                        rc = link(tgt, filename);
                        if (rc) {
                                printf("link(%s, %s) error: %s\n",
                                       tgt, filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                } else if (do_mkdir) {
                        rc = mkdir(filename, 0755);
                        if (rc) {
                                printf("mkdir(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                } else {
                        rc = mknod(filename, S_IFREG| 0444, 0);
                        if (rc) {
                                printf("mknod(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                }
                if (do_unlink) {
                        filename = get_file_name(fmt_unlink, begin,
						 unlink_has_fmt_spec);
                        rc = do_mkdir ? rmdir(filename) : unlink(filename);
                        if (rc) {
                                printf("unlink(%s) error: %s\n",
                                       filename, strerror(errno));
                                rc = errno;
                                break;
                        }
                }

		if ((i != 0 && (i % 10000) == 0) || now() - last >= 10.0) {
			double tmp = now();

			printf(" - created %ld (time %.2f total %.2f last %.2f)"
			       "\n", i, tmp, tmp - start, tmp - last);
			last = now();
		}
	}
	last = now();
	total = i;
	printf("total: %ld %s%s%s in %.2f seconds: %.2f ops/second\n", total,
	       do_open ? "open" : do_mkdir ? "mkdir" :
	       do_link ? "link" : "create", defer_close ? "" : "/close",
	       do_unlink ? (do_mkdir ? "/rmdir" : "/unlink") : "",
	       last - start, ((double)total / (last - start)));

	if (!defer_close)
		return rc;

	stderr_fd = fileno(stderr);
	start = last;
	/* Assume fd is allocated in order, doing extra closes is not harmful */
	for (i = 0; i < total && last_fd > stderr_fd; i++, --last_fd) {
		close(last_fd);

		if ((i != 0 && (i % 10000) == 0) || now() - last >= 10.0) {
			double tmp = now();

			printf(" - closed %ld (time %.2f total %.2f last %.2f)"
			       "\n", i, tmp, tmp - start, tmp - last);
			last = now();
		}
	}
	last = now();

	printf("total: %ld close in %.2f seconds: %.2f close/second\n",
	       total, last - start, ((double)total / (last - start)));
	return rc;
}
