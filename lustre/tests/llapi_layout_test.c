/*
 * These tests exercise the llapi_layout API which abstracts the layout
 * of a Lustre file behind an opaque data type.  They assume a Lustre file
 * system with at least 6 OSTs and a pool names "testpool" containing 6
 * OSTs. For example,
 *
 *  sudo lctl pool_new lustre.testpool
 *  sudo lctl pool_add lustre.testpool OST[0-5]
 *  gcc -D LUSTRE_DIR=\"/mnt/lustre/somedir\" -Wall -g -Werror \
 *      -o llapi_layout_test llapi_layout_test.c -llustrepapi
 *  sudo ./test
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/signal.h>
#include <sys/types.h>
#include <assert.h>
#include <errno.h>
#include <lustre/lustreapi.h>
#include <pwd.h>
#include <limits.h>
#include <sys/stat.h>

#define TEST(testfn) { test(&(testfn), #testfn); }

#ifndef LUSTRE_DIR
#define LUSTRE_DIR "/mnt/lustre"
#endif
#define TEST_POOL_NAME 		"testpool"

/* This function runs a single test by forking the process.  This way,
   if there is a segfault during a test, the test program won't crash */
void test(void (*testfn)(), const char *name) {
	pid_t pid = fork();
	if (pid > 0) {
		/* Check the child's return value */
		int status = 0;
		wait(&status);
		/* Non-zero value = test failed */
		printf("%s:\t%s\n", name, status ? "fail" : "pass");
	} else if (pid == 0) {
		/* Run the test in the child process.  Exit with 0 for success,
		   non-zero for failure */
		testfn();
		exit(0);
	} else {
		printf("Fork failed!\n");
    }
}

#define T1FILE			LUSTRE_DIR "/t1"
#define T1_STRIPE_COUNT		5
#define T1_STRIPE_SIZE		1048576
#define T1_OST_OFFSET		3
/* Sanity test. Read and write layout attributes then create a new file. */
void test1()
{
	int rc;
	lustre_layout_t *layout = llapi_layout_alloc();

	assert(NULL != layout);

	rc = unlink(T1FILE);
	assert(0 == rc || ENOENT == errno);

	assert(0 == llapi_layout_stripe_count_set(layout, T1_STRIPE_COUNT));
	assert(T1_STRIPE_COUNT == llapi_layout_stripe_count(layout));

	assert(0 == llapi_layout_stripe_size_set(layout, T1_STRIPE_SIZE));
	assert(T1_STRIPE_SIZE == llapi_layout_stripe_size(layout));

	assert(0 == llapi_layout_pool_name_set(layout, TEST_POOL_NAME));
	assert(0 == strcmp(llapi_layout_pool_name(layout), TEST_POOL_NAME));

	rc = llapi_layout_ost_index_set(layout, 0, T1_OST_OFFSET);
	assert(0 == rc);

	rc = llapi_layout_file_create(layout, T1FILE, 0660, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(layout);
}

void __test2_helper(lustre_layout_t *layout)
{
	int ost0;
	int ost1;
	int obj0;
	int obj1;

	assert(llapi_layout_stripe_count(layout) == T1_STRIPE_COUNT);
	assert(llapi_layout_stripe_size(layout) == T1_STRIPE_SIZE);
	ost0 = llapi_layout_ost_index(layout, 0);
	ost1 = llapi_layout_ost_index(layout, 1);
	obj0 = llapi_layout_obj_id(layout, 0);
	obj1 = llapi_layout_obj_id(layout, 1);

	assert(0 == strcmp(llapi_layout_pool_name(layout), TEST_POOL_NAME));
	assert(T1_OST_OFFSET == ost0 );
	assert(ost1 != ost0);
	assert(0 != obj0);
	assert(0 != obj1);
}

/* Read back file layout from test1 by path and verify attributes. */
void test2()
{
	lustre_layout_t *layout = llapi_layout_lookup_bypath(T1FILE);
	assert(NULL != layout);
	__test2_helper(layout);
	llapi_layout_free(layout);
}

/* Read back file layout from test1 by open fd and verify attributes. */
void test3()
{
	int fd;

	fd = open(T1FILE, O_RDONLY);
	assert(-1 != fd);

	lustre_layout_t *layout = llapi_layout_lookup_byfd(fd);
	assert(0 == close(fd));
	assert(NULL != layout);
	__test2_helper(layout);
	llapi_layout_free(layout);
}

#define T4FILE			LUSTRE_DIR "/t4"
#define T4_STRIPE_COUNT		5
#define T4_STRIPE_SIZE 		2097152
/* Create a file with 'lfs setstripe' then verify it's layout */
void test4()
{
	int rc;
	int ost0;
	int ost1;
	int obj0;
	int obj1;
	const char *lfs = getenv("LFS");
	char cmd[4096];

	if (lfs == NULL)
		lfs = "/usr/bin/lfs";

	rc = unlink(T4FILE);
	assert(0 == rc || ENOENT == errno);
	snprintf(cmd, 4096, "%s setstripe -p %s -c %d -s %d %s\n", lfs,
		 TEST_POOL_NAME, T4_STRIPE_COUNT, T4_STRIPE_SIZE, T4FILE);
	assert(0 == system(cmd));

	errno = 0;
	lustre_layout_t *layout = llapi_layout_lookup_bypath(T4FILE);
	assert(NULL != layout);
	assert(0 == errno);
	assert(T4_STRIPE_COUNT == llapi_layout_stripe_count(layout));
	assert(T4_STRIPE_SIZE == llapi_layout_stripe_size(layout));
	assert(0 == strcmp(llapi_layout_pool_name(layout), TEST_POOL_NAME));
	ost0 = llapi_layout_ost_index(layout, 0);
	ost1 = llapi_layout_ost_index(layout, 1);
	obj0 = llapi_layout_obj_id(layout, 0);
	obj1 = llapi_layout_obj_id(layout, 1);
	assert(ost0 != ost1);
	assert(0 != obj0);
	assert(0 != obj1);
	llapi_layout_free(layout);
}

#define T5FILE			LUSTRE_DIR "/t5"
/* llapi_layout_lookup_bypath() returns ENOENT in errno when expected. */
void test5()
{
	int rc;
	rc = unlink(T5FILE);
	assert(0 == rc || ENOENT == errno);
	errno = 0;
	lustre_layout_t *layout = llapi_layout_lookup_bypath(T5FILE);
	assert(NULL == layout);
	assert(ENOENT == errno);
}

/* llapi_layout_lookup_byfd() returns EBADF in errno when expected. */
void test6()
{
	int rc;
	rc = unlink(T5FILE);
	assert(0 == rc || ENOENT == errno);
	errno = 0;
	lustre_layout_t *layout = llapi_layout_lookup_byfd(9999);
	assert(NULL == layout);
	assert(EBADF == errno);
}

#define T7FILE			LUSTRE_DIR "/t7"
/* llapi_layout_lookup_bypath() returns EACCES in errno when expected. */
void test7()
{
	int fd;
	int rc;
	uid_t myuid = getuid();
	assert(0 == myuid); /* Need root for this test. */

	/* Create file as root */
	rc = unlink(T7FILE);
	assert(0 == rc || ENOENT == errno);
	fd = open(T7FILE, O_CREAT);
	assert(-1 != fd);
	assert(0 == fchmod(fd, 0400));
	assert(0 == close(fd));

	/* Become unprivileged user */
	struct passwd *pw = getpwnam("nobody");
	assert(NULL != pw);
	assert(0 == seteuid(pw->pw_uid));
	errno = 0;
	lustre_layout_t *layout = llapi_layout_lookup_bypath(T7FILE);
	assert(NULL == layout);
	assert(EACCES == errno);
	assert(0 == seteuid(myuid));
}

#define T8FILE			LUSTRE_DIR "/t8"
/* llapi_layout_lookup_bypath() returns ENODATA in errno for file with no
 * striping attributes. */
void test8()
{
	int fd;
	int rc;
	lustre_layout_t *layout;

	rc = unlink(T8FILE);
	assert(0 == rc || ENOENT == errno);
	fd = open(T8FILE, O_CREAT, 0640);
	assert(-1 != fd);
	assert(0 == close(fd));

	errno = 0;
	layout = llapi_layout_lookup_bypath(T8FILE);
	assert(NULL == layout);
	assert(ENODATA == errno);
}

/* Setting pattern > 0 returns EOPNOTSUPP in errno. */
void test9()
{
	lustre_layout_t *layout;
	int rc;

	assert(layout = llapi_layout_alloc());
	errno = 0;
	rc = llapi_layout_pattern_set(layout, 1);
	assert(-1 == rc);
	assert(EOPNOTSUPP == errno);
	llapi_layout_free(layout);
}


#define T10FILE			LUSTRE_DIR "/t10"
#define T10_STRIPE_COUNT	2
#define T10_STRIPE_SIZE 	1048576
/* llapi_layout_create_file() returns EEXIST in errno for
 * already-existing file. */
void test10()
{
	int rc;
	int fd;
	lustre_layout_t *layout;

	(void) unlink(T10FILE);
	layout = llapi_layout_alloc();
	llapi_layout_stripe_count_set(layout, T10_STRIPE_COUNT);
	llapi_layout_stripe_size_set(layout, T10_STRIPE_SIZE);
	fd = llapi_layout_file_create(layout, T10FILE, 0750, 0);
	assert(0 <= fd);
	assert(0 == close(fd));
	errno = 0;
	rc = llapi_layout_file_create(layout, T10FILE, 0750, 0);
	assert(0 > rc);
	assert(EEXIST == errno);
	llapi_layout_free(layout);
}

/* Verify stripe_count interfaces return errors as expected */
void test11()
{
	int rc;
	lustre_layout_t *layout;

	layout = llapi_layout_alloc();
	assert(NULL != layout);

	/* stripe count less than -1 (-1 means stripe as widely as possible) */
	errno = 0;
	rc = llapi_layout_stripe_count_set(layout, -2);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_count_set(NULL, 2);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_count(NULL);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* stripe count too large */
	errno = 0;
	rc = llapi_layout_stripe_count_set(layout, LOV_MAX_STRIPE_COUNT + 1);
	assert(-1 == rc);
	assert(EINVAL == errno);
	llapi_layout_free(layout);
}

/* Verify stripe_size interfaces return errors as expected */
void test12()
{
	int rc;
	lustre_layout_t *layout;

	layout = llapi_layout_alloc();
	assert(NULL != layout);

	/* negative stripe size */
	errno = 0;
	rc = llapi_layout_stripe_size_set(layout, -1);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* stripe size too big */
	errno = 0;
	rc = llapi_layout_stripe_size_set(layout, 1ULL << 32);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_size_set(NULL, 1048576);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_size(NULL);
	assert(-1 == rc);
	assert(EINVAL == errno);

	llapi_layout_free(layout);
}

/* Verify pool_name interfaces return errors as expected */
void test13()
{
	int rc;
	lustre_layout_t *layout;
	const char *poolname;

	layout = llapi_layout_alloc();
	assert(NULL != layout);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_pool_name_set(NULL, "foo");
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL pool name */
	errno = 0;
	rc = llapi_layout_pool_name_set(layout, NULL);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	poolname = llapi_layout_pool_name(NULL);
	assert(NULL == poolname);
	assert(EINVAL == errno);

	/* Pool name too long*/
	errno = 0;
	rc = llapi_layout_pool_name_set(layout, "0123456789abcdef0");
	assert(-1 == rc);
	assert(EINVAL == errno);

	llapi_layout_free(layout);
}

#define T14FILE			LUSTRE_DIR "/t14"
#define T14_STRIPE_COUNT	2
/* Verify ost_index and obj_id interfaces return errors as expected */
void test14()
{
	int rc;
	lustre_layout_t *layout;

	layout = llapi_layout_alloc();
	assert(NULL != layout);

	/* Only setting OST index for stripe 0 is supported for now. */
	errno = 0;
	rc = llapi_layout_ost_index_set(layout, 1, 1);
	assert(-1 == rc);
	assert(EOPNOTSUPP == errno);

	/* OST index less than one (-1 means let MDS choose) */
	errno = 0;
	rc = llapi_layout_ost_index_set(layout, 0, -2);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_ost_index_set(NULL, 0, 1);
	assert(-1 == rc);
	assert(EINVAL == errno);

	errno = 0;
	rc = llapi_layout_ost_index(NULL, 0);
	assert(-1 == rc);
	assert(EINVAL == errno);

	errno = 0;
	rc = llapi_layout_obj_id(NULL, 0);
	assert(-1 == rc);
	assert(EINVAL == errno);

	/* Layout not read from file so has no OST data. */
	errno = 0;
	assert(llapi_layout_stripe_count_set(layout, T14_STRIPE_COUNT) == 0);
	rc = llapi_layout_ost_index(layout, 0);
	assert(-1 == rc);
	assert(0 == errno);

	errno = 0;
	rc = llapi_layout_obj_id(layout, 0);
	assert(-1 == rc);
	assert(0 == errno);

	/* n greater than stripe count*/
	rc = unlink(T14FILE);
	assert(0 == rc || ENOENT == errno);
	assert(llapi_layout_stripe_count_set(layout, T14_STRIPE_COUNT) == 0);
	rc = llapi_layout_file_create(layout, T14FILE, 0644, 0);
	assert(0 <= rc);
	close(rc);
	llapi_layout_free(layout);
	layout = llapi_layout_lookup_bypath(T14FILE);
	errno = 0;
	rc = llapi_layout_ost_index(layout, 3);
	assert(-1 == rc);
	assert(EINVAL == errno);

	errno = 0;
	rc = llapi_layout_obj_id(layout, 3);
	assert(-1 == rc);
	assert(EINVAL == errno);

	llapi_layout_free(layout);
}

/* Verify llapi_layout_file_create() returns errors as expected */
void test15()
{
	int rc;

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_file_create(NULL, "foo", 0, 0);
	assert(-1 == rc);
	assert(EINVAL == errno);
}

#define T16FILE			LUSTRE_DIR "/t16"
#define T16_STRIPE_COUNT	2
/* Can't change striping attributes of existing file. */
void test16()
{
	int rc;
	lustre_layout_t *layout;

	rc = unlink(T16FILE);
	assert(0 == rc || ENOENT == errno);

	layout = llapi_layout_alloc();
	assert(NULL != layout);
	assert(llapi_layout_stripe_count_set(layout, T16_STRIPE_COUNT) == 0);
	errno = 0;
	rc = llapi_layout_file_create(layout, T16FILE, 0640, 0);
	assert(0 <= rc);
	assert(0 == errno);
	assert(0 == close(rc));

	assert(0 == llapi_layout_stripe_count_set(layout, T16_STRIPE_COUNT + 1));
	errno = 0;
	rc = llapi_layout_file_create(layout, T16FILE, 0640, 0);
	assert(0 > rc);
	assert(EEXIST == errno);
	llapi_layout_free(layout);

	layout = llapi_layout_lookup_bypath(T16FILE);
	assert(NULL != layout);
	assert(T16_STRIPE_COUNT == llapi_layout_stripe_count(layout));
	llapi_layout_free(layout);
}

#define T17FILE			LUSTRE_DIR "/t17"
/* Default stripe attributes are applied as expected. */
void test17()
{
	int rc;
	lustre_layout_t *dirlayout;
	lustre_layout_t *filelayout;

	rc = unlink(T17FILE);
	assert(0 == rc || ENOENT == errno);
	assert(dirlayout = llapi_layout_lookup_bypath(LUSTRE_DIR));
	assert(filelayout = llapi_layout_alloc());
	rc = llapi_layout_file_create(filelayout, T17FILE, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(filelayout);
	assert(filelayout = llapi_layout_lookup_bypath(T17FILE));
	assert(llapi_layout_stripe_count(filelayout) ==
	       llapi_layout_stripe_count(dirlayout));
	assert(llapi_layout_stripe_size(filelayout) ==
	       llapi_layout_stripe_size(dirlayout));
	llapi_layout_free(filelayout);
	llapi_layout_free(dirlayout);
}

#define T18FILE			LUSTRE_DIR "/t18"
/* Setting stripe count to -1 uses all available OSTs. */
void test18()
{
	int rc;
	int fd;
	int ost_count;
	lustre_layout_t *layout;

	rc = unlink(T18FILE);
	assert(0 == rc || ENOENT == errno);
	assert(layout = llapi_layout_alloc());
	llapi_layout_stripe_count_set(layout, -1);
	rc = llapi_layout_file_create(layout, T18FILE, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(layout);

	/* Get number of available OSTs */
	fd = open(T18FILE, O_RDONLY);
	assert(-1 != fd);
	llapi_lov_get_uuids(fd, NULL, &ost_count);
	assert(0 == close(fd));
	assert(layout = llapi_layout_lookup_bypath(T18FILE));
	assert(llapi_layout_stripe_count(layout) == ost_count);

	llapi_layout_free(layout);
}

#define T19FILE1		LUSTRE_DIR "/t191"
#define T19FILE2		LUSTRE_DIR "/t192"
/* Layout generation reflects updated layout. */
void test19()
{
	int rc;
	int gen1;
	int gen2;
	lustre_layout_t *layout;

	rc = unlink(T19FILE1);
	assert(0 == rc || ENOENT == errno);
	rc = unlink(T19FILE2);
	assert(0 == rc || ENOENT == errno);

	assert(layout = llapi_layout_alloc());
	assert(-1 == llapi_layout_generation(layout));
	rc = llapi_layout_file_create(layout, T19FILE1, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));

	rc = llapi_layout_file_create(layout, T19FILE2, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(layout);

	assert(layout = llapi_layout_lookup_bypath(T19FILE1));

	assert(layout = llapi_layout_lookup_bypath(T19FILE1));
	gen1 = llapi_layout_generation(layout);
	llapi_layout_free(layout);

	/* Swap layouts to force generation update. */
	rc = llapi_swap_layouts(T19FILE1, T19FILE2);
	assert(0 == rc);

	assert(layout = llapi_layout_lookup_bypath(T19FILE1));
	gen2 = llapi_layout_generation(layout);
	llapi_layout_free(layout);

	assert(gen1 != gen2);
}


#define T20FILE			LUSTRE_DIR "/t20"
/* Setting pool with "fsname.pool" notation. */
void test20()
{
	int rc;
	lustre_layout_t *layout = llapi_layout_alloc();

	assert(NULL != layout);

	rc = unlink(T20FILE);
	assert(0 == rc || ENOENT == errno);

	assert(0==llapi_layout_pool_name_set(layout, "lustre." TEST_POOL_NAME));
	assert(0==strcmp(llapi_layout_pool_name(layout), TEST_POOL_NAME));
	rc = llapi_layout_file_create(layout, T20FILE, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(layout);

	assert(layout = llapi_layout_lookup_bypath(T20FILE));
	assert(0 == strcmp(llapi_layout_pool_name(layout), TEST_POOL_NAME));
	llapi_layout_free(layout);
}

#define T21FILE			LUSTRE_DIR "/t21"
#define T21_STRIPE_COUNT	2
#define T21_STRIPE_SIZE 	1048576
/* Look up layout by fid. */
void test21()
{
	int rc;
	lustre_layout_t *layout;
	lustre_fid fid;
	char fidstr[4096];

	rc = unlink(T21FILE);
	assert(0 == rc || ENOENT == errno);

	layout = llapi_layout_alloc();
	assert(NULL != layout);
	rc = llapi_layout_stripe_size_set(layout, T21_STRIPE_SIZE);
	assert(0 == rc);
	rc = llapi_layout_stripe_count_set(layout, T21_STRIPE_COUNT);
	assert(0 == rc);
	rc = llapi_layout_file_create(layout, T21FILE, 0640, 0);
	assert(0 <= rc);
	assert(0 == close(rc));
	llapi_layout_free(layout);

	rc = llapi_path2fid(T21FILE, &fid);
	assert(0 == rc);
	snprintf(fidstr, sizeof(fidstr), "0x%llx:0x%x:0x%x", fid.f_seq,
		 fid.f_oid, fid.f_ver);
	errno = 0;
	layout = llapi_layout_lookup_byfid(T21FILE, fidstr);
	assert(layout);
	assert(T21_STRIPE_COUNT == llapi_layout_stripe_count(layout));
	assert(T21_STRIPE_SIZE == llapi_layout_stripe_size(layout));
	llapi_layout_free(layout);
}

/* Maximum length pool name is properly NULL-terminated. */
void test22()
{
	lustre_layout_t *layout;
	const char *str;
	char *name = "0123456789abcdef";

	assert(layout = llapi_layout_alloc());
	assert(0 == llapi_layout_pool_name_set(layout, name));
	str = llapi_layout_pool_name(layout);
	assert(strlen(name) == strlen(str));
	llapi_layout_free(layout);
}

void sigsegv(int signal) {
	printf("Segmentation fault\n");
	exit(1);
}

int main(int argc, char *argv[]) {
	signal(SIGSEGV, sigsegv);

	TEST(test1);
	TEST(test2);
	TEST(test3);
	TEST(test4);
	TEST(test5);
	TEST(test6);
	TEST(test7);
	TEST(test8);
	TEST(test9);
	TEST(test10);
	TEST(test11);
	TEST(test12);
	TEST(test13);
	TEST(test14);
	TEST(test15);
	TEST(test16);
	TEST(test17);
	TEST(test18);
	TEST(test19);
	TEST(test20);
	TEST(test21);
	TEST(test22);

	return 0;
}
