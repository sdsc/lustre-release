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
 * Copyright 2014 Cray Inc, all rights reserved.
 * Author: Frank Zago.
 *
 * A few portions are extracted from llapi_layout_test.c
 *
 * The purpose of this test is to exert the group lock ioctls.
 *
 * The program will exit as soon as a non zero error code is returned.
 */

#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>

#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>

#define ERROR(fmt, ...)							\
	fprintf(stderr, "%s: %s:%d: %s: " fmt "\n",                     \
		program_invocation_short_name, __FILE__, __LINE__,      \
		__func__, ## __VA_ARGS__);

#define DIE(fmt, ...)                  \
	do {			       \
		ERROR(fmt, ## __VA_ARGS__);	\
		exit(EXIT_FAILURE);		\
	} while (0)

#define ASSERTF(cond, fmt, ...)						\
	do {								\
		if (!(cond))						\
			DIE("assertion '%s' failed: "fmt,		\
			    #cond, ## __VA_ARGS__);			\
	} while (0)

/* Name of file/directory. Will be set once and will not changed. */
static char mainpath[PATH_MAX];
static const char *maindir = "group_lock_test_name_9585766";

static char fsmountdir[PATH_MAX];	/* Lustre mountpoint */
static char *lustre_dir;		/* Test directory inside Lustre */

/* Cleanup our test file. */
static void cleanup()
{
	unlink(mainpath);
	rmdir(mainpath);
}

/* Test lock / unlock */
static void test10(void)
{
	int rc;
	int fd;
	int gid;
	int i;

	cleanup();

	/* Create the test file, and open it. */
	fd = creat(mainpath, 0);
	ASSERTF(fd >= 0, "fd=%d, errno=%d, file=%s", fd, errno, mainpath);

	/* Valid command first. */
	gid = 1234;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	/* Again */
	gid = 768;
	for (i = 0; i < 1000; i++) {
		rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
		ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
		rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
		ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);
	}

	/* Lock twice. */
	gid = 97486;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == EINVAL, "can't lock %s: %d %d",
		mainpath, rc, errno);
	rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);
	rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == -1 && errno == EINVAL, "unexpected value: %d %d",
		rc, errno);

	/* 0 is an invalid gid */
	gid = 0;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == EINVAL, "unexpected value: %d %d",
		rc, errno);

	/* Lock/unlock with a different gid */
	gid = 3543;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	for (gid = -10; gid < 10; gid++) {
		rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
		ASSERTF(rc == -1 && errno == EINVAL, "unexpected value: %d %d",
			rc, errno);
	}
	gid = 3543;
	rc = ioctl(fd, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	close(fd);
}

/* Test open/lock/close without unlocking */
static void test11(void)
{
	int rc;
	int fd;
	int gid;
	char buf[10000];

	cleanup();

	/* Create the test file. */
	fd = creat(mainpath, 0);
	ASSERTF(fd >= 0, "fd=%d, errno=%d, file=%s", fd, errno, mainpath);
	rc = write(fd, buf, sizeof(buf));
	ASSERTF(rc == sizeof(buf), "write failed: %d %d", rc, errno);
	close(fd);

	/* Open/lock and close many times. Open with different
	 * flags. */
	for (gid = 1; gid < 10000; gid++) {
		int oflags = O_RDONLY;

		switch (gid % 9) {
		case 0:
			oflags = O_RDONLY;
			break;
		case 1:
			oflags = O_WRONLY;
			break;
		case 2:
			oflags = O_WRONLY | O_APPEND;
			break;
		case 3:
			oflags = O_WRONLY | O_CLOEXEC;
			break;
		case 4:
			oflags = O_WRONLY | O_DIRECT;
			break;
		case 5:
			oflags = O_WRONLY | O_NOATIME;
			break;
		case 6:
			oflags = O_WRONLY | O_SYNC;
			break;
		case 7:
			oflags = O_RDONLY | O_DIRECT;
			break;
		case 8:
			oflags = O_RDONLY | O_RDWR;
			break;
		}

		fd = open(mainpath, oflags);
		ASSERTF(fd >= 0, "fd=%d, errno=%d, oflags=%d",
			fd, errno, oflags);

		rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
		ASSERTF(rc == 0, "can't lock %s, oflags=%d, gid=%d: %d",
			mainpath, oflags, gid, rc);

		close(fd);
	}

	cleanup();
}

static void helper_test20(int fd)
{
	int gid;
	int rc;

	gid = 1234;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == ENOTTY, "unexpected value: %d %d",
		rc, errno);

	gid = 0;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == ENOTTY, "unexpected value: %d %d",
		rc, errno);

	gid = 1;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == ENOTTY, "unexpected value: %d %d",
		rc, errno);

	gid = -1;
	rc = ioctl(fd, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == -1 && errno == ENOTTY, "unexpected value: %d %d",
		rc, errno);
}

/* Test lock / unlock on a directory */
static void test20(void)
{
	int fd;
	int rc;
	char dname[PATH_MAX];

	cleanup();

	/* Try the mountpoint. Should fail. */
	fd = open(fsmountdir, O_DIRECTORY);
	ASSERTF(fd >= 0, "fd=%d, errno=%d, file=%s", fd, errno, mainpath);
	helper_test20(fd);
	close(fd);

	/* Try .lustre/ . Should fail. */
	rc = snprintf(dname, sizeof(dname), "%s/.lustre", fsmountdir);
	ASSERTF(rc < sizeof(dname), "Name too long");

	fd = open(fsmountdir, O_DIRECTORY);
	ASSERTF(fd >= 0, "fd=%d, errno=%d, file=%s", fd, errno, mainpath);
	helper_test20(fd);
	close(fd);

	/* A regular directory. */
	rc = mkdir(mainpath, 0600);
	ASSERTF(rc == 0, "mkdir failed: %d", errno);

	fd = open(mainpath, O_DIRECTORY);
	ASSERTF(fd >= 0, "fd=%d, errno=%d, file=%s", fd, errno, mainpath);
	helper_test20(fd);
	close(fd);
}

/* Test locking between several fds. */
static void test30(void)
{
	int fd1;
	int fd2;
	int gid;
	int gid2;
	int rc;

	cleanup();

	/* Create the test file, and open it. */
	fd1 = creat(mainpath, 0);
	ASSERTF(fd1 >= 0, "fd=%d, errno=%d, file=%s", fd1, errno, mainpath);

	/* Open a second time non blocking mode. */
	fd2 = open(mainpath, O_RDWR | O_NONBLOCK);
	ASSERTF(fd2 >= 0, "fd=%d, errno=%d, file=%s", fd1, errno, mainpath);

	/* Valid command first. */
	gid = 1234;
	rc = ioctl(fd1, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd1, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	/* Lock on one fd, unlock on the other */
	gid = 6947556;
	rc = ioctl(fd1, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd2, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected value: %d %d", rc, errno);
	rc = ioctl(fd1, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	/* Lock from both */
	gid = 89489665;
	rc = ioctl(fd1, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd2, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd2, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);
	rc = ioctl(fd1, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	/* Lock from both. Unlock in reverse order. */
	gid = 89489665;
	rc = ioctl(fd1, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd2, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	rc = ioctl(fd1, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);
	rc = ioctl(fd2, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	/* Try to lock with different gids */
	gid = 89489665;
	rc = ioctl(fd1, LL_IOC_GROUP_LOCK, gid);
	ASSERTF(rc == 0, "can't lock %s: %d", mainpath, rc);
	for (gid2 = -50; gid2 < 50; gid2++) {
		rc = ioctl(fd2, LL_IOC_GROUP_LOCK, gid2);
		if (gid2 == 0)
			ASSERTF(rc == -1 && errno == EINVAL,
				"unexpected value: %d %d", rc, errno);
		else
			ASSERTF(rc == -1 && errno == EAGAIN,
				"unexpected value: %d %d", rc, errno);
	}
	rc = ioctl(fd1, LL_IOC_GROUP_UNLOCK, gid);
	ASSERTF(rc == 0, "can't unlock %s: %d", mainpath, rc);

	close(fd1);
	close(fd2);
}

static void usage(char *prog)
{
	printf("Usage: %s [-d lustre_dir]\n", prog);
	exit(0);
}

static void process_args(int argc, char *argv[])
{
	int c;

	while ((c = getopt(argc, argv, "d:")) != -1) {
		switch (c) {
		case 'd':
			lustre_dir = optarg;
			break;
		case '?':
			fprintf(stderr, "Unknown option '%c'\n", optopt);
			usage(argv[0]);
		}
	}
}

int main(int argc, char *argv[])
{
	char fsname[8];
	int rc;

	process_args(argc, argv);
	if (lustre_dir == NULL)
		lustre_dir = "/mnt/lustre";

	rc = llapi_search_mounts(lustre_dir, 0, fsmountdir, fsname);
	if (rc != 0) {
		fprintf(stderr, "Error: %s: not a Lustre filesystem\n",
			lustre_dir);
		return EXIT_FAILURE;
	}

	/* Play nice with Lustre test scripts. Non-line buffered output
	 * stream under I/O redirection may appear incorrectly. */
	setvbuf(stdout, NULL, _IOLBF, 0);

	/* Create a test filename and reuse it. Remove possibly old files. */
	rc = snprintf(mainpath, sizeof(mainpath), "%s/%s", lustre_dir, maindir);
	ASSERTF((rc > 0 && rc < sizeof(mainpath)), "invalid name for mainpath");
	cleanup();

	atexit(cleanup);

	test10();
	test11();
	test20();
	test30();

	return EXIT_SUCCESS;
}
