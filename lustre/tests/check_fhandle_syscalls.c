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
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html
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
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <linux/unistd.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#define MAX_HANDLE_SZ 128

#ifdef HAVE_FHANDLE_SYSCALLS
/* Because the kernel supports this functions doesn't mean that glibc does.
 * Just in case we define struct file_handle */
struct file_handle
{
	unsigned int handle_bytes;
	int handle_type;
	/* File identifier.  */
	unsigned char f_handle[0];
};
#endif

void usage(char *prog)
{
	fprintf(stderr, "usage: %s <filepath> <mount point>\n",
		prog);
	fprintf(stderr, "filepath should be relative to the mount point\n");
	exit(1);
}

int main(int argc, char **argv)
{
#ifdef HAVE_FHANDLE_SYSCALLS
	char *filename, *mount_point, *readbuf = NULL;
	int ret, rc, mnt_id, mnt_fd, fd, i, len, offset;
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
	ret = syscall(SYS_name_to_handle_at, mnt_fd, filename, fh, &mnt_id,
		      AT_SYMLINK_FOLLOW);
	if (ret) {
		fprintf(stderr, "name_by_handle_at(%s) error: %s\n", filename,
			strerror(errno));
		rc = errno;
		goto out_f_handle;
	}

	/* Print out the contents of the file handle */
	fprintf(stdout, "fh_bytes: %u\nfh_type: %d\nfh_data: ",
		fh->handle_bytes, fh->handle_type);
	for (i = 0; i < fh->handle_bytes; i++)
		fprintf(stdout, "%x ", fh->f_handle[i]);
	fprintf(stdout, "\n");

	/* Open the file handle */
	fd = syscall(SYS_open_by_handle_at, mnt_fd, fh, O_RDONLY);
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
		len = st.st_blksize;
		readbuf = malloc(len);
		if (readbuf == NULL) {
			fprintf(stderr, "malloc(%d) error: %s\n", len,
				strerror(errno));
			rc = errno;
			goto out_fd;
		}

		for (offset = 0; offset < st.st_size; offset += len) {
			/* read from the file */
			rc = read(fd, readbuf, len);
			if (rc < 0) {
				fprintf(stderr, "read(%s) error: %s\n",
					filename, strerror(errno));
				rc = errno;
				goto out_readbuf;
			}
		}
	}

	rc = 0;
	fprintf(stdout, "check_fhandle_syscalls test Passed!\n");

out_readbuf:
	if (readbuf != NULL)
		free(readbuf);
out_fd:
	close(fd);
out_f_handle:
	free(fh);
out_mnt_fd:
	close(mnt_fd);
out:
	return rc;
#else /* !HAVE_FHANDLE_SYSCALLS */
	fprintf(stderr, "HAVE_FHANDLE_SYSCALLS not defined\n");
	return 1;
#endif /* HAVE_FHANDLE_SYSCALLS */
}
