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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * GPL HEADER END
 */

/*
 * Copyright (C) 2013, DataDirect Networks, Inc.
 * Author: Swapnil Pimpale <spimpale@ddn.com>
 */

/*
 * The following test cases test the ioctl passthrough mechanism for Lustre
 * server (OST/MDT) mountpoints
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

/*
 * ioctl commands
 */
#define LDISKFS_IOC_GETFLAGS		FS_IOC_GETFLAGS
#define LDISKFS_IOC_GETVERSION		_IOR('f', 3, long)

void usage(char *prog)
{
	fprintf(stdout, "usage: %s <lustre_server_mntpt>\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	char *srv_mntpt;
	int srv_fd, ret, len;
	unsigned int iflags, igen;

	if (argc != 2)
		usage(argv[0]);

	srv_mntpt = argv[1];

	/* remove ending slashes if any */
	len = strlen(srv_mntpt);
	while (srv_mntpt[len - 1] == '/') {
		srv_mntpt[len - 1] = '\0';
		len--;
	}

	/* Open server mount point */
	srv_fd = open(srv_mntpt, O_RDONLY);
	if (srv_fd < 0) {
		fprintf(stderr, "open(%s) failed, error: %s\n", srv_mntpt,
			strerror(errno));
		ret = errno;
		goto out;
	}

	/* test LDISKFS_IOC_GETFLAGS ioctl */
	if (ioctl(srv_fd, LDISKFS_IOC_GETFLAGS, &iflags) < 0) {
		fprintf(stderr, "ioctl LDISKFS_IOC_GETFLAGS failed, "
			"error: %s\n", strerror(errno));
		ret = errno;
		goto out_srv_fd;
	}

	fprintf(stdout, "Inode Flags: 0x%x\n", iflags);

	/* test LDISKFS_IOC_GETVERSION ioctl */
	if (ioctl(srv_fd, LDISKFS_IOC_GETVERSION, &igen) < 0) {
		fprintf(stderr, "ioctl LDISKFS_IOC_GETVERSION failed, "
			"error: %s\n", strerror(errno));
		ret = errno;
		goto out_srv_fd;
	}

	fprintf(stdout, "Inode Generation: %u\n", igen);
	fprintf(stdout, "ioctl_passthru test Passed!\n");
	ret = 0;

out_srv_fd:
	close(srv_fd);
out:
	return ret;
}
