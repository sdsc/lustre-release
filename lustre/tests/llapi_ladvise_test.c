/*
 *  GPL HEADER START
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
 * Copyright (c) 2016, Intel Corporation.
 */

/* Some portions are extracted from llapi_layout_test.c
 * Specifically, we are using the following from llapi_layout_test.c
 * with no modification:
 * ERROR, DIE, ASSERTF
 * print_test_desc(), test(), set_tests_skipped()
*/

/* The purpose of these tests is to exercise the public API of
 * the server side hint and advice feature added to Lustre 2.9.0.
 * These tests assume a Lustre file system exists and is mounted
 * on the client running this script.
 */

#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <sys/wait.h>
#include <sys/signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <time.h>

#include <lustre/lustreapi.h>

static char fsmountdir[PATH_MAX];      /* Lustre mountpoint */
static char *lustre_dir;               /* Test directory inside Lustre */

#define ERROR(fmt, ...)\
	fprintf(stderr, "%s: %s:%d: %s: " fmt "\n",\
		program_invocation_short_name, __FILE__, __LINE__,\
		__func__, ## __VA_ARGS__);

#define DIE(fmt, ...)				\
do {						\
	ERROR(fmt, ## __VA_ARGS__);			\
	exit(EXIT_FAILURE);				\
} while (0)

#define ASSERTF(cond, fmt, ...)			\
do {						\
	if (!(cond))				\
		DIE("assertion '%s' failed: "fmt, #cond, ## __VA_ARGS__);\
} while (0)					\

#define T0_DESC "Test for number of advise hint value outside valid range"
void test0(void)
{
	struct llapi_lu_ladvise ladvise;
	unsigned long long flags = 0;
	int num_advise = 0;
	int fd = 0;
	int rc = -1;

	/* Test checks for number of advise hints < 1 and > LAH_COUNT_MAX */

	/* This call should fail with errno EINVAL*/
	rc = llapi_ladvise(fd, flags, num_advise, &ladvise);
	ASSERTF(rc == -1, "llapi_advise with number of hints %d failed: %s",
		num_advise, strerror(-rc));

	num_advise = LAH_COUNT_MAX + 1;
	/* This call should fail with errno EINVAL*/
	rc = llapi_ladvise(fd, flags, num_advise, &ladvise);
	ASSERTF(rc == -1, "llapi_advise with number of hints %d failed: %s",
		num_advise, strerror(-rc));

}

#define T1_DESC "llapi_ladvise should check for NULL struct"
void test1(void)
{
	unsigned long long flags = 0;
	int num_advise = 1;
	int fd = 0;
	int rc = -1;

	rc = llapi_ladvise(fd, flags, num_advise, NULL);
	ASSERTF(rc == -1, "llapi_ladvise failed with NULL struct: %s",
		strerror(-rc));

}

/* Create the testfile. Return a valid file descriptor. */
static int create_file(char *fname)
{
	char testfile[PATH_MAX];
	int rc;
	int fd;

	rc = snprintf(testfile, sizeof(testfile), "%s/%s",
		      lustre_dir, fname);
	ASSERTF((rc > 0 && rc < sizeof(testfile)), "invalid name for testfile");

	/* Remove old test file, if any. */
	unlink(testfile);

	fd = creat(testfile, S_IRWXU);
	ASSERTF(fd >= 0, "create failed for '%s': %s",
		testfile, strerror(errno));

	return fd;
}

#define T2_DESC "llapi_ladvise should detect invalid file descriptor"
void test2(void)
{
	struct llapi_lu_ladvise advise;
	unsigned long long flags = 0;
	char *fname = "file_T2";
	int num_advise = 1;
	int fd = -1;
	int rc = -1;

	fd = create_file(fname);
	flags = LF_ASYNC;

	advise.lla_start = 0;
	advise.lla_end = 0;
	advise.lla_advice = 0;
	advise.lla_value1 = 0;
	advise.lla_value2 = 0;
	advise.lla_value3 = 0;
	advise.lla_value4 = 0;

	rc = llapi_ladvise(fd, flags, num_advise, &advise);
	ASSERTF(rc == -1, "llapi_ladvise failed with bad file descriptor: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

#define T3_DESC "llapi_ladvise should skip unknown advice flags"
void test3(void)
{
	struct llapi_lu_ladvise advise;
	unsigned long long flags = 0;
	char *fname = "file_T3";
	int num_advise = 1;
	int fd = 0;
	int rc = -1;

	fd = create_file(fname);

	advise.lla_start = 0;
	advise.lla_end = 0;
	advise.lla_advice = 0;
	advise.lla_value1 = 0;
	advise.lla_value2 = 0;
	advise.lla_value3 = 0;
	advise.lla_value4 = 0;

	rc = llapi_ladvise(fd, flags, num_advise, &advise);
	ASSERTF(rc == -1, "llapi_ladvise failed with unknown flag: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

#define T4_DESC "llapi_ladvise should skip unknown advice types"
void test4(void)
{
	struct llapi_lu_ladvise advise;
	unsigned long long flags = 0;
	char *fname = "file_T4";
	int num_advise = 3;
	int fd = 0;
	int rc = -1;

	fd = create_file(fname);

	flags = LF_ASYNC;
	advise.lla_start = 0;
	advise.lla_end = 0;
	/* Valid advice is:
	   LU_LADVISE_INVALID 0
	   LU_LADVISE_WILLREAD 1
	   LU_LADVISE_DONTNEED 2 */
	advise.lla_advice = 5;
	advise.lla_value1 = 0;
	advise.lla_value2 = 0;
	advise.lla_value3 = 0;
	advise.lla_value4 = 0;


	/* Should return -1 with error ENOTSUPP */
	rc = llapi_ladvise(fd, flags, num_advise, &advise);
	ASSERTF(rc == -1, "llapi_ladvise succeeded with unknown advice: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

#define T5_DESC "llapi_ladvise should detect start > end parameters"
void test5(void)
{
	struct llapi_lu_ladvise advise;
	unsigned long long flags = 0;
	char *fname = "file_T5";
	int num_advise = 1;
	int fd = 0;
	int rc = -1;

	fd = create_file(fname);

	flags = LF_ASYNC;
	advise.lla_start = 5565;
	advise.lla_end = 45;
	advise.lla_advice = LU_LADVISE_WILLREAD;
	advise.lla_value1 = 0;
	advise.lla_value2 = 0;
	advise.lla_value3 = 0;
	advise.lla_value4 = 0;

	/* Should return -1 with */
	rc = llapi_ladvise(fd, flags, num_advise, &advise);
	ASSERTF(rc == -1, "llapi_ladvise succeeded with start > end: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

#define T6_DESC "Test num_advise not matching number llapi_lu_ladvise array"
void test6(void)
{
	struct llapi_lu_ladvise advise[3];
	unsigned long long flags = 0;
	char *fname = "file_T6";
	int num_advise = 35;
	int fd = 0;
	int rc = -1;

	fd = create_file(fname);

	flags = LF_ASYNC;

	advise[0].lla_start = 0;
	advise[0].lla_end = 0;
	advise[0].lla_advice = LU_LADVISE_WILLREAD;
	advise[0].lla_value1 = 0;
	advise[0].lla_value2 = 0;
	advise[0].lla_value3 = 0;
	advise[0].lla_value4 = 0;

	advise[1].lla_start = 556670;
	advise[1].lla_end = 17;
	advise[1].lla_advice = LU_LADVISE_WILLREAD;
	advise[1].lla_value1 = 0;
	advise[1].lla_value2 = 0;
	advise[1].lla_value3 = 0;
	advise[1].lla_value4 = 0;

	advise[1].lla_start = 0;
	advise[1].lla_end = 0;
	advise[1].lla_advice = LU_LADVISE_WILLREAD;
	advise[1].lla_value1 = 0;
	advise[1].lla_value2 = 0;
	advise[1].lla_value3 = 0;
	advise[1].lla_value4 = 0;

	/* Should return -1 */
	rc = llapi_ladvise(fd, flags, num_advise, advise);
	ASSERTF(rc == -1, "llapi_ladvise succeeded with bad array size: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

#define T7_DESC "llapi_ladvise should ignore values in advise value"
void test7(void)
{
	struct llapi_lu_ladvise advise;
	unsigned long long flags = 0;
	char *fname = "file_T7";
	int num_advise = 1;
	int fd = 0;
	int rc = -1;

	fd = create_file(fname);

	flags = LF_ASYNC;

	advise.lla_start = 0;
	advise.lla_end = 0;
	advise.lla_advice = LU_LADVISE_WILLREAD;
	advise.lla_value1 = 17;
	advise.lla_value2 = 1025;
	advise.lla_value3 = -1;
	advise.lla_value4 = 98;

	rc = llapi_ladvise(fd, flags, num_advise, &advise);
	ASSERTF(rc == 0, "llapi_ladvise succeeded with value*: %s",
		strerror(-rc));

	rc = close(fd);
	ASSERTF(rc == 0, "close failed: %s", strerror(-rc));

}

/* Length of test description */
#define TEST_DESC_LEN 60
struct test_tbl_entry {
	void (*tte_fn)(void);
	char tte_desc[TEST_DESC_LEN];
	bool tte_skip;
};

static struct test_tbl_entry test_tbl[] = {
	{ &test0,  T0_DESC, false },
	{ &test1,  T1_DESC, false },
	{ &test2,  T2_DESC, false },
	{ &test3,  T3_DESC, false },
	{ &test4,  T4_DESC, false },
	{ &test5,  T5_DESC, false },
	{ &test6,  T6_DESC, false },
	{ &test7,  T7_DESC, false },
};

#define NUM_TESTS (sizeof(test_tbl) / sizeof(struct test_tbl_entry))

void print_test_desc(int test_num, const char *test_desc, const char *status)
{
	int i;

	printf(" test %2d: %s ", test_num, test_desc);
	for (i = 0; i < TEST_DESC_LEN - strlen(test_desc); i++)
		printf(".");
	printf(" %s\n", status);
}

/* This function runs a single test by forking the process.  This way,
 * if there is a segfault during a test, the test program won't crash. */
int test(void (*test_fn)(), const char *test_desc, bool test_skip, int test_num)
{
	int rc = 0;
	pid_t pid;
	char status_buf[128];

	if (test_skip) {
		print_test_desc(test_num, test_desc, "skip");
		return 0;
	}

	pid = fork();
	if (pid < 0) {
		ERROR("cannot fork: %s", strerror(errno));
	} else if (pid > 0) {
		int status = 0;

		/* Non-zero value indicates failure. */
		wait(&status);
		if (status == 0) {
			strncpy(status_buf, "pass", sizeof(status_buf));
		} else if WIFSIGNALED(status) {
			snprintf(status_buf, sizeof(status_buf),
				 "fail (exit status %d, killed by SIG%d)",
				 WEXITSTATUS(status), WTERMSIG(status));
			rc = -1;
		} else {
			snprintf(status_buf, sizeof(status_buf),
				 "fail (exit status %d)", WEXITSTATUS(status));
			rc = -1;
		}
		print_test_desc(test_num, test_desc, status_buf);
	} else if (pid == 0) {
		/* Run the test in the child process.  Exit with 0 for success,
		 * non-zero for failure */
		test_fn();
		exit(0);
	}

	return rc;
}

/* 'str_tests' are the tests to be skipped, such as "1,3,4,.." */
static void set_tests_skipped(char *str_tests)
{
	char *ptr = str_tests;
	int tstno;

	if (ptr == NULL || strlen(ptr) == 0)
		return;

	while (*ptr != '\0') {
		tstno = strtoul(ptr, &ptr, 0);
		if (tstno >= 0 && tstno < NUM_TESTS)
			test_tbl[tstno].tte_skip = true;
		if (*ptr == ',')
			ptr++;
		else
			break;
	}
}

static void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-d lustre_dir] [-s $n,$m,...]\n", prog);
	exit(EXIT_FAILURE);
}

static void process_args(int argc, char *argv[])
{
	int c;

	/* If there's no args, just print usage, not 'Unknown option' message */
	if (argc <= 1)
		usage(argv[0]);

	while ((c = getopt(argc, argv, "d:hs:")) != -1) {
		switch (c) {
		case 'd':
			lustre_dir = optarg;
			break;
		case 'h':
			usage(argv[0]);
			break;
		case 's':
			set_tests_skipped(optarg);
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
	char fsname[8 + 1];
	int i;
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

	for (i = 0; i < NUM_TESTS; i++) {
		struct test_tbl_entry *tst = &test_tbl[i];
		if (test(tst->tte_fn, tst->tte_desc, tst->tte_skip, i) != 0)
			rc++;
	}

	return rc;
}
