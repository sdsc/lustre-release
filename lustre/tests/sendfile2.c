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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */

/*
 * Copyright 2015 Cray Inc, all rights reserved.
 * Author: Frank Zago.
 *
 * A few portions are extracted from llapi_layout_test.c
 *
 * The purpose of this test is to exert the group lock ioctls in
 * conjunction with sendfile. Some bugs were found when both were used
 * at the same time. See LU-6368 and LU-6371.
 *
 * The program will exit as soon as a non zero error code is returned.
 */

#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <poll.h>
#include <sys/sendfile.h>

#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>

#define ERROR(fmt, ...)							\
	fprintf(stderr, "%s: %s:%d: %s: " fmt "\n",			\
		program_invocation_short_name, __FILE__, __LINE__,	\
		__func__, ## __VA_ARGS__);

#define DIE(fmt, ...)				\
	do {					\
		ERROR(fmt, ## __VA_ARGS__);	\
		exit(EXIT_FAILURE);		\
	} while (0)

#define ASSERTF(cond, fmt, ...)						\
	do {								\
		if (!(cond))						\
			DIE("assertion '%s' failed: "fmt,		\
			    #cond, ## __VA_ARGS__);			\
	} while (0)

#define PERFORM(testfn) \
	do {								\
		fprintf(stderr, "Starting test " #testfn " at %lld\n",	\
			(unsigned long long)time(NULL));		\
		testfn();						\
		fprintf(stderr, "Finishing test " #testfn " at %lld\n",	\
			(unsigned long long)time(NULL));		\
	} while (0)

/* This test will copy from source_file to dest_file */
static const char *source_file;
static char *dest_file;

/* Cleanup our test file. */
static void cleanup(void)
{
	unlink(dest_file);
}

/* Helper. Copy a file with sendfile. The destination will be
 * created. If a group lock is 0, it means do not take one. */
static int sendfile_copy(const char *source, int source_gid,
			 const char *dest, int dest_gid)
{
	int rc;
	struct stat stbuf;
	size_t filesize;
	int fd_in;
	int fd_out;

	fd_in = open(source, O_RDONLY);
	ASSERTF(fd_in >= 0, "open failed for '%s': %s",
		source, strerror(errno));

	rc = fstat(fd_in, &stbuf);
	ASSERTF(rc == 0, "fstat of '%s' failed: %s", source, strerror(errno));
	filesize = stbuf.st_size;

	if (source_gid != 0) {
		rc = llapi_group_lock(fd_in, source_gid);
		ASSERTF(rc == 0, "cannot set group lock %u for '%s': %s",
			source_gid, source, strerror(-rc));
	}

	fd_out = open(dest, O_WRONLY | O_TRUNC | O_CREAT);
	ASSERTF(fd_out >= 0, "creation failed for '%s': %s",
		dest, strerror(errno));

	if (dest_gid != 0) {
		rc = llapi_group_lock(fd_out, dest_gid);
		ASSERTF(rc == 0, "cannot set group lock %u for '%s': %s",
			dest_gid, dest, strerror(-rc));
	}

	/* Transfer by 10M blocks */
	while (filesize != 0) {
		size_t to_copy = 10*1024*1024;
		ssize_t sret;

		if (to_copy > filesize)
			to_copy = filesize;

		sret = sendfile(fd_out, fd_in, NULL, to_copy);
		rc = errno;

		/* Although senfile can return less than requested,
		 * that should not happen under present conditions. At
		 * the very least, make sure that a decent size was
		 * copied. See LU-6371. */

		ASSERTF(sret != 0, "sendfile read 0 bytes");
		ASSERTF(sret > 0, "sendfile failed: %s", strerror(rc));
		ASSERTF(sret > 100*1024,
			"sendfile read too little data: %zd bytes", sret);

		if (sret != to_copy)
			fprintf(stderr,
			       "Warning: sendfile returned %zd bytes instead of %zu requested\n",
			       sret, to_copy);

		filesize -= sret;

	}

	close(fd_out);
	close(fd_in);

	return 0;

}

/* Basic sendfile, without lock taken */
static void test10(void)
{
	cleanup();
	sendfile_copy(source_file, 0, dest_file, 0);
	sync();
}

/* sendfile, source locked */
static void test11(void)
{
	cleanup();
	sendfile_copy(source_file, 85543, dest_file, 0);
	sync();
}

/* sendfile, destination locked */
static void test12(void)
{
	cleanup();
	sendfile_copy(source_file, 0, dest_file, 98765);
	sync();
}

/* sendfile, source and destination locked, with same lock number */
static void test13(void)
{
	const int gid = 8765;
	cleanup();
	sendfile_copy(source_file, gid, dest_file, gid);
	sync();
}

/* sendfile, source and destination locked, with different lock number */
static void test14(void)
{
	cleanup();
	sendfile_copy(source_file, 98765, dest_file, 34543);
	sync();
}

/* Basic sendfile, without lock taken, to /dev/null */
static void test15(void)
{
	sendfile_copy(source_file, 0, "/dev/null", 0);
	sync();
}

/* sendfile, source locked, to /dev/null */
static void test16(void)
{
	sendfile_copy(source_file, 85543, "/dev/null", 0);
	sync();
}

int main(int argc, char *argv[])
{
	int rc;

	if (argc != 2 || argv[1][0] != '/') {
		fprintf(stderr,
			"Argument must be an absolute path to a Lustre file\n");
		return EXIT_FAILURE;
	}

	source_file = argv[1];
	rc = asprintf(&dest_file, "%s-dest", source_file);
	if (rc == -1) {
		fprintf(stderr, "Allocation failure\n");
		return EXIT_FAILURE;
	}

	/* Play nice with Lustre test scripts. Non-line buffered output
	 * stream under I/O redirection may appear incorrectly. */
	setvbuf(stdout, NULL, _IOLBF, 0);

	cleanup();
	atexit(cleanup);

	PERFORM(test10);
	PERFORM(test11);
	PERFORM(test12);
	PERFORM(test13);
	PERFORM(test14);
	PERFORM(test15);
	PERFORM(test16);

	return EXIT_SUCCESS;
}
