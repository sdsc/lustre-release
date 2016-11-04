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
 * Copyright 2016 Cray Inc. All rights reserved.
 * Authors: Patrick Farrell, Frank Zago
 *
 * A few portions are extracted from llapi_layout_test.c
 *
 * The purpose of this test is to exercise the requestlock advice of ladvise.
 *
 * The program will exit as soon as a test fails.
 */

#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <poll.h>
#include <time.h>

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
		cleanup();						\
		fprintf(stderr, "Starting test " #testfn " at %lld\n",	\
			(unsigned long long)time(NULL));		\
		testfn();						\
		fprintf(stderr, "Finishing test " #testfn " at %lld\n",	\
			(unsigned long long)time(NULL));		\
		cleanup();						\
	} while (0)

/* Name of file/directory. Will be set once and will not change. */
static char mainpath[PATH_MAX];
static const char *maindir = "requestlock_test_name_65436563";

static char fsmountdir[PATH_MAX];	/* Lustre mountpoint */
static char *lustre_dir;		/* Test directory inside Lustre */

/* Cleanup our test file. */
static void cleanup(void)
{
	unlink(mainpath);
}

/* Trivial helper for one advice */
void llapi_setup_ladvise_requestlock(struct llapi_lu_ladvise *advice, int mode,
				     int flags, size_t start, size_t end)
{
	advice->lla_advice = LU_LADVISE_REQUESTLOCK;
	advice->lla_requestlock_mode = mode;
	advice->lla_peradvice_flags = flags;
	advice->lla_start = start;
	advice->lla_end = end;
	advice->lla_value3 = 0;
	advice->lla_value4 = 0;
}

/* Test valid single lock ahead request */
static void test10(void)
{
	struct llapi_lu_ladvise advice;
	const int count = 1;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	llapi_setup_ladvise_requestlock(&advice, MODE_WRITE_USER, 0, 0,
					write_size - 1);

	/* Manually set the result so we can verify it's being modified */
	advice.lla_requestlock_result = 345678;

	rc = llapi_ladvise(fd, 0, count, &advice);
	ASSERTF(rc == 0,
		"cannot requestlock '%s': %s", mainpath, strerror(errno));
	ASSERTF(advice.lla_requestlock_result == 0,
		"unexpected extent result: %d",
		advice.lla_requestlock_result);

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));


	close(fd);
}

/* Get lock, wait until lock is taken */
static void test11(void)
{
	struct llapi_lu_ladvise advice;
	const int count = 1;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];
	int i;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	llapi_setup_ladvise_requestlock(&advice, MODE_WRITE_USER, 0, 0,
					write_size - 1);

	/* Manually set the result so we can verify it's being modified */
	advice.lla_requestlock_result = 345678;

	rc = llapi_ladvise(fd, 0, count, &advice);
	ASSERTF(rc == 0,
		"cannot requestlock '%s': %s", mainpath, strerror(errno));
	ASSERTF(advice.lla_requestlock_result == 0,
		"unexpected extent result: %d",
		advice.lla_requestlock_result);

	/* Ask again until we get the lock (status 1). */
	for (i = 1; i < 100; i++) {
		usleep(100000); /* 0.1 second */

		advice.lla_requestlock_result = 456789;
		rc = llapi_ladvise(fd, 0, count, &advice);
		ASSERTF(rc == 0, "cannot requestlock '%s': %s",
			mainpath, strerror(errno));

		if (advice.lla_requestlock_result > 0)
			break;
	}

	printf("exited wait loop after %f seconds\n", i * 0.1);

	ASSERTF(advice.lla_requestlock_result > 0,
		"unexpected extent result: %d",
		advice.lla_requestlock_result);

	/* Again. This time it is always there. */
	for (i = 0; i < 100; i++) {
		advice.lla_requestlock_result = 456789;
		rc = llapi_ladvise(fd, 0, count, &advice);
		ASSERTF(rc == 0, "cannot requestlock '%s': %s",
			mainpath, strerror(errno));
		ASSERTF(advice.lla_requestlock_result > 0,
			"unexpected extent result: %d",
			advice.lla_requestlock_result);
	}

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));

	close(fd);
}

/* Test with several times the same extent */
static void test12(void)
{
	struct llapi_lu_ladvise *advice;
	const int count = 10;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];
	int i;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);

	for (i = 0; i < count; i++) {
		llapi_setup_ladvise_requestlock(&(advice[i]), MODE_WRITE_USER,
						0, 0, write_size - 1);
		advice[i].lla_requestlock_result = 98674;
	}

	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == 0,
		"cannot requestlock '%s': %s", mainpath, strerror(errno));
	for (i = 0; i < count; i++) {
		ASSERTF(advice[i].lla_requestlock_result >= 0,
			"unexpected extent result for extent %d: %d",
			i, advice[i].lla_requestlock_result);
	}

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));

	free(advice);
	close(fd);
}

/* Grow a lock forward */
static void test13(void)
{
	struct llapi_lu_ladvise *advice;
	const int count = 1;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];
	int i;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	for (i = 0; i < 100; i++) {

		advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
		llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER,
						0, 0, i * write_size - 1);
		advice[0].lla_requestlock_result = 98674;

		rc = llapi_ladvise(fd, 0, count, advice);
		ASSERTF(rc == 0, "cannot requestlock '%s' at offset %llu: %s",
			mainpath,
			advice[0].lla_end,
			strerror(errno));

		ASSERTF(advice[0].lla_requestlock_result >= 0,
			"unexpected extent result for extent %d: %d",
			i, advice[0].lla_requestlock_result);

		free(advice);
	}

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));

	close(fd);
}

/* Grow a lock backward */
static void test14(void)
{
	struct llapi_lu_ladvise *advice;
	const int count = 1;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];
	int i;
	const int num_blocks = 100;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	for (i = 0; i < num_blocks; i++) {
		size_t start = (num_blocks - i - 1) * write_size;
		size_t end = (num_blocks) * write_size - 1;

		advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
		llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER,
						0, start, end);
		advice[0].lla_requestlock_result = 98674;

		rc = llapi_ladvise(fd, 0, count, advice);
		ASSERTF(rc == 0, "cannot requestlock '%s' at offset %llu: %s",
			mainpath,
			advice[0].lla_end,
			strerror(errno));

		ASSERTF(advice[0].lla_requestlock_result >= 0,
			"unexpected extent result for extent %d: %d",
			i, advice[0].lla_requestlock_result);

		free(advice);
	}

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));

	close(fd);
}

/* Request many locks at 10MiB intervals */
static void test15(void)
{
	struct llapi_lu_ladvise *advice;
	const int count = 1;
	int fd;
	size_t write_size = 1024 * 1024;
	int rc;
	char buf[write_size];
	int i;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);

	for (i = 0; i < 20000; i++) {
		__u64 start = i * 1024 * 1024 * 10;
		__u64 end = start + 1;

		llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER,
						0, start, end);

		advice[0].lla_requestlock_result = 345678;

		rc = llapi_ladvise(fd, 0, count, advice);

		ASSERTF(rc == 0, "cannot requestlock '%s' : %s",
			mainpath, strerror(errno));
		ASSERTF(advice[0].lla_requestlock_result >= 0,
			"unexpected extent result for extent %d: %d",
			i, advice[0].lla_requestlock_result);
	}

	memset(buf, 0xaa, write_size);
	rc = write(fd, buf, write_size);
	ASSERTF(rc == sizeof(buf), "write failed for '%s': %s",
		mainpath, strerror(errno));

	free(advice);

	close(fd);
}

/* Test invalid single requestlock request */
static void test20(void)
{
	struct llapi_lu_ladvise *advice;
	const int count = 1;
	int fd;
	int rc;
	size_t start = 0;
	size_t end = 0;

	fd = open(mainpath, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
	ASSERTF(fd >= 0, "open failed for '%s': %s",
		mainpath, strerror(errno));

	/* A valid request first */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	start = 0;
	end = 1024*1024;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0, start,
					end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == 0, "cannot requestlock '%s' : %s",
		mainpath, strerror(errno));
	free(advice);

	/* No actual block */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	start = 0;
	end = 0;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0, start,
					end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for no block lock: %d %s",
		rc, strerror(errno));
	free(advice);

	/* end before start */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	start = 1024 * 1024;
	end = 0;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0, start,
					end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for reversed block: %d %s",
		rc, strerror(errno));
	free(advice);

	/* bogus lock mode - 0x65464 */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	start = 0;
	end = 1024 * 1024;
	llapi_setup_ladvise_requestlock(advice, 0x65464, 0, start, end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for bogus lock mode: %d %s",
		rc, strerror(errno));
	free(advice);

	/* bogus flags, 0x80 */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	start = 0;
	end = 1024 * 1024;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0x80, start,
					end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for bogus flags: %u %d %s",
		0x80, rc, strerror(errno));
	free(advice);

	/* bogus flags, 0xff - CEF_MASK */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	end = 1024 * 1024;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0xff, start,
					end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for bogus flags: %u %d %s",
		0xff, rc, strerror(errno));
	free(advice);

	/* bogus flags, 0xffffffff */
	advice = malloc(sizeof(struct llapi_lu_ladvise)*count);
	end = 1024 * 1024;
	llapi_setup_ladvise_requestlock(advice, MODE_WRITE_USER, 0xffffffff,
					start, end);
	rc = llapi_ladvise(fd, 0, count, advice);
	ASSERTF(rc == -1 && errno == EINVAL,
		"unexpected return for bogus flags: %u %d %s",
		0xffffffff, rc, strerror(errno));
	free(advice);

	close(fd);
}

static void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-d lustre_dir]\n", prog);
	exit(EXIT_FAILURE);
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
		default:
			fprintf(stderr, "Unknown option '%c'\n", optopt);
			usage(argv[0]);
			break;
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
		fprintf(stderr, "Error: '%s': not a Lustre filesystem\n",
			lustre_dir);
		return EXIT_FAILURE;
	}

	/* Play nice with Lustre test scripts. Non-line buffered output
	 * stream under I/O redirection may appear incorrectly. */
	setvbuf(stdout, NULL, _IOLBF, 0);

	/* Create a test filename and reuse it. Remove possibly old files. */
	rc = snprintf(mainpath, sizeof(mainpath), "%s/%s", lustre_dir, maindir);
	ASSERTF(rc > 0 && rc < sizeof(mainpath), "invalid name for mainpath");
	cleanup();

	atexit(cleanup);

	PERFORM(test10);
	PERFORM(test11);
	PERFORM(test12);
	PERFORM(test13);
	PERFORM(test14);
	PERFORM(test15);
	PERFORM(test20);

	return EXIT_SUCCESS;
}
