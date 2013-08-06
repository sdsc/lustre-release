/* GPL HEADER START
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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * GPL HEADER END
 */

/*
 * Copyright (C) 2013, DataDirect Networks, Inc.
 * Author: Swapnil Pimpale <spimpale@ddn.com>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <asm/unistd.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_HANDLE_SZ 128

void usage(char *prog)
{
	fprintf(stderr, "usage: %s <filepath> <mount point>\n",
		prog);
	fprintf(stderr, "filepath should be relative to the mount point\n");
	exit(1);
}

int main(int argc, char **argv)
{
	char *filename, *mount_point, *readbuf = NULL;
	int ret, rc, mnt_id, mnt_fd, fd;
	struct file_handle *fh;
	struct stat st;

	if (argc != 3)
		usage(argv[0]);

	filename = argv[1];
	mount_point = argv[2];

	/* Open mount point directory */
	mnt_fd = open(mount_point, O_DIRECTORY);
	if (mnt_fd < 0) {
		fprintf(stderr, "open(%s) error: %s\n)", mount_point,
			strerror(errno));
		rc = errno;
		goto out;
	}

	/* Allocate memory for file handle */
	fh = malloc(sizeof(struct file_handle) + MAX_HANDLE_SZ);
	if (!fh) {
		fprintf(stderr, "malloc(%d) error: %s\n", MAX_HANDLE_SZ,
			strerror(errno));
		rc = errno;
		goto out_mnt_fd;
	}
	fh->handle_bytes = MAX_HANDLE_SZ;

	/* Convert name to handle */
	ret = name_to_handle_at(mnt_fd, filename, fh, &mnt_id,
				AT_SYMLINK_FOLLOW);
	if (ret) {
		fprintf(stderr, "name_by_handle_at(%s) error: %s\n", filename,
			strerror(errno));
		rc = errno;
		goto out_f_handle;
	}

	/* Open the file handle */
	fd = open_by_handle_at(mnt_fd, fh, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open_by_handle_at(%s) error: %s\n", filename,
			strerror(errno));
		rc = errno;
		goto out_f_handle;
	}

	/* Get file size */
	rc = fstat(fd, &st);
	if (rc < 0) {
		fprintf(stderr, "fstat(%s) error: %s\n", filename,
			strerror(errno));
		rc = errno;
		goto out_fd;
	}

	if (st.st_size) {
		readbuf = malloc(st.st_size);
		if (!readbuf) {
			fprintf(stderr, "malloc(%ld) error: %s\n", st.st_size,
				strerror(errno));
			rc = errno;
			goto out_fd;
		}

		/* Read file contents */
		rc = read(fd, readbuf, st.st_size);
		if (rc < 0) {
			fprintf(stderr, "read(%s) error: %s\n", filename,
				strerror(errno));
			rc = errno;
			goto out_readbuf;
		}

		/* Print file contents */
		fprintf(stdout, "%s\n", readbuf);
	}

	rc = 0;
	fprintf(stdout, "check_fhandle_syscalls test Passed!\n");

out_readbuf:
	if (readbuf)
		free(readbuf);
out_fd:
	close(fd);
out_f_handle:
	free(fh);
out_mnt_fd:
	close(mnt_fd);
out:
	return rc;
}
