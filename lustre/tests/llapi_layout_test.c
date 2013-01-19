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
 * These tests exercise the llapi_layout API which abstracts the layout
 * of a Lustre file behind an opaque data type.  They assume a Lustre
 * file system with at least 2 OSTs and a pool containing at least the
 * first 2 OSTs.  For example,
 *
 *  sudo lctl pool_new lustre.testpool
 *  sudo lctl pool_add lustre.testpool OST[0-1]
 *  gcc -Wall -g -Werror -o llapi_layout_test llapi_layout_test.c -llustreapi
 *  sudo ./llapi_layout_test
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/signal.h>
#include <sys/types.h>
#include <errno.h>
#include <lustre/lustreapi.h>
#include <pwd.h>
#include <limits.h>
#include <sys/stat.h>
#include <getopt.h>
#include <inttypes.h>

#define ERROR(fmt, ...)							\
	fprintf(stderr, "%s: %s:%d: %s: " fmt "\n",			\
		program_invocation_short_name, __FILE__, __LINE__,	\
		__func__, ## __VA_ARGS__);

#define DIE(fmt, ...)			\
do {					\
	ERROR(fmt, ## __VA_ARGS__);	\
	exit(EXIT_FAILURE);		\
} while (0)

#define VERIFYF(cond, fmt, ...)						\
do {									\
	if (!(cond))							\
		DIE("failed '" #cond "': " fmt, ## __VA_ARGS__);	\
} while (0)								\

static char *lustre_dir;
static char *poolname;
static int num_osts = -1;

void usage(char *prog)
{
	printf("Usage: %s [-d lustre_dir] [-p pool_name] [-o num_osts]\n",
	       prog);
	exit(0);
}

/* get parent directory of a path */
void __get_parent_dir(const char *path, char *buf, size_t size)
{
	char *p = NULL;

	strncpy(buf, path, size);
	p = strrchr(buf, '/');

	if (p != NULL)
		*p = '\0';
	else
		snprintf(buf, 2, ".");
}

llapi_layout_t *__get_default_layout(const char *path)
{
	llapi_layout_t	*layout;
	char		dir[PATH_MAX];
	const char	*p = dir;
	struct stat st;
	int rc;

	rc = stat(path, &st);

	/* If path is a directory, get its layout.  If it's not a
	 * directory, or it doesn't exist, get its parent's layout. */
	if (rc == 0 && S_ISDIR(st.st_mode))
		p = path;
	else if (rc < 0 && errno != ENOENT)
		return NULL;
	else
		__get_parent_dir(path, dir, sizeof(dir));

	layout = llapi_layout_by_path(p);
	if (layout == NULL && errno != ENODATA)
		return NULL;
	else if (layout != NULL)
		return layout;

	/* If no striping data from parent, get layout of fs root */
	rc = llapi_search_mounts(path, 0, dir, NULL);
	if (rc < 0)
		return NULL;
	return llapi_layout_by_path(dir);
}

#define T0FILE			"t0"
#define T0_STRIPE_COUNT		num_osts
#define T0_STRIPE_SIZE		1048576
#define T0_OST_OFFSET		(num_osts - 1)
#define T0_DESC		"Read/write layout attributes then create a file"
void test0(void)
{
	int rc;
	int fd;
	uint64_t count;
	uint64_t size;
	llapi_layout_t *layout = llapi_layout_alloc();
	char file[PATH_MAX];
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };

	VERIFYF(layout != NULL, "errno %d", errno);

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T0FILE);

	rc = unlink(file);
	VERIFYF(rc >= 0 || errno == ENOENT, "errno = %d", errno);

	/* stripe count */
	rc = llapi_layout_stripe_count_set(layout, T0_STRIPE_COUNT);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_count_get(layout, &count);
	VERIFYF(rc == 0 && count == T0_STRIPE_COUNT, "%"PRIu64" != %d", count,
		T0_STRIPE_COUNT);

	/* stripe size */
	rc = llapi_layout_stripe_size_set(layout, T0_STRIPE_SIZE);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_size_get(layout, &size);
	VERIFYF(rc == 0 && size == T0_STRIPE_SIZE, "%"PRIu64" != %d", size,
		T0_STRIPE_SIZE);

	/* pool_name */
	rc = llapi_layout_pool_name_set(layout, poolname);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = strcmp(mypool, poolname);
	VERIFYF(rc == 0, "%s != %s", mypool, poolname);

	/* ost_index */
	rc = llapi_layout_ost_index_set(layout, 0, T0_OST_OFFSET);
	VERIFYF(rc == 0, "errno = %d", errno);

	/* create */
	fd = llapi_layout_file_create(file, 0, 0660, layout);
	VERIFYF(fd >= 0, "file = %s, errno = %d", file, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);
	llapi_layout_free(layout);
}

void __test1_helper(llapi_layout_t *layout)
{
	uint64_t ost0;
	uint64_t ost1;
	uint64_t size;
	uint64_t count;
	int rc;
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };

	rc = llapi_layout_stripe_count_get(layout, &count);
	VERIFYF(count == T0_STRIPE_COUNT, "%"PRIu64" != %d", count,
		T0_STRIPE_COUNT);

	rc = llapi_layout_stripe_size_get(layout, &size);
	VERIFYF(size == T0_STRIPE_SIZE, "%"PRIu64" != %d", size,
		T0_STRIPE_SIZE);

	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = strcmp(mypool, poolname);
	VERIFYF(rc == 0, "%s != %s", mypool, poolname);

	rc = llapi_layout_ost_index_get(layout, 0, &ost0);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_ost_index_get(layout, 1, &ost1);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(ost0 == T0_OST_OFFSET, "%"PRIu64" != %d", ost0, T0_OST_OFFSET);
	VERIFYF(ost1 != ost0, "%"PRIu64" == %"PRIu64, ost0, ost1);
}

#define T1_DESC		"Read test0 file by path and verify attributes"
void test1(void)
{
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T0FILE);
	llapi_layout_t *layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);
	__test1_helper(layout);
	llapi_layout_free(layout);
}


#define T2_DESC		"Read test0 file by FD and verify attributes"
void test2(void)
{
	int fd;
	int rc;
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T0FILE);

	fd = open(file, O_RDONLY);
	VERIFYF(fd >= 0, "open(%s): errno = %d", file, errno);

	llapi_layout_t *layout = llapi_layout_by_fd(fd);
	VERIFYF(layout != NULL, "errno = %d", errno);

	rc = close(fd);
	VERIFYF(rc == 0, "close(%s): errno = %d", file, errno);

	__test1_helper(layout);
	llapi_layout_free(layout);
}

#define T3_DESC		"Read test0 file by FID and verify attributes"
void test3(void)
{
	int rc;
	llapi_layout_t *layout;
	lustre_fid fid;
	char fidstr[4096];
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T0FILE);

	rc = llapi_path2fid(file, &fid);
	VERIFYF(rc == 0, "rc = %d, errno = %d", rc, errno);
	snprintf(fidstr, sizeof(fidstr), "0x%"PRIx64":0x%x:0x%x",
		 (uint64_t)fid.f_seq, fid.f_oid, fid.f_ver);
	errno = 0;
	layout = llapi_layout_by_fid(file, &fid);
	VERIFYF(layout != NULL, "fidstr = %s, errno = %d", fidstr, errno);

	__test1_helper(layout);
	llapi_layout_free(layout);
}


#define T4FILE			"t4"
#define T4_STRIPE_COUNT		2
#define T4_STRIPE_SIZE		2097152
#define T4_DESC		"Verify compatibility with 'lfs setstripe'"
void test4(void)
{
	int rc;
	uint64_t ost0;
	uint64_t ost1;
	uint64_t count;
	uint64_t size;
	const char *lfs = getenv("LFS");
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };
	char cmd[4096];
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T4FILE);

	if (lfs == NULL)
		lfs = "/usr/bin/lfs";

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	snprintf(cmd, sizeof(cmd), "%s setstripe %s %s -c %d -s %d %s", lfs,
		 strlen(poolname) > 0 ? "-p" : "", poolname, T4_STRIPE_COUNT,
		 T4_STRIPE_SIZE, file);
	rc = system(cmd);
	VERIFYF(rc == 0, "system(%s): exit status %d", cmd, WEXITSTATUS(rc));

	errno = 0;
	llapi_layout_t *layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);

	rc = llapi_layout_stripe_count_get(layout, &count);
	VERIFYF(count == T4_STRIPE_COUNT, "%"PRIu64" != %d", count,
		T4_STRIPE_COUNT);

	rc = llapi_layout_stripe_size_get(layout, &size);
	VERIFYF(size == T4_STRIPE_SIZE, "%"PRIu64" != %d", size,
		T4_STRIPE_SIZE);

	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = strcmp(mypool, poolname);
	VERIFYF(rc == 0, "%s != %s", mypool, poolname);

	rc = llapi_layout_ost_index_get(layout, 0, &ost0);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_ost_index_get(layout, 1, &ost1);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(ost1 != ost0, "%"PRIu64" == %"PRIu64, ost0, ost1);

	llapi_layout_free(layout);
}

#define T5FILE		"t5"
#define T5_DESC		"llapi_layout_by_path ENOENT handling"
void test5(void)
{
	int rc;
	char file[PATH_MAX];
	llapi_layout_t *layout;

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T5FILE);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	errno = 0;
	layout = llapi_layout_by_path(file);
	VERIFYF(layout == NULL && errno == ENOENT, "errno = %d", errno);
}

#define T6_DESC		"llapi_layout_by_fd EBADF handling"
void test6(void)
{
	errno = 0;
	llapi_layout_t *layout = llapi_layout_by_fd(9999);
	VERIFYF(layout == NULL && errno == EBADF, "errno = %d", errno);
}


#define T7FILE		"t7"
#define T7_DESC		"llapi_layout_by_path EACCES handling"
void test7(void)
{
	int fd;
	int rc;
	uid_t myuid = getuid();
	char file[PATH_MAX];
	const char *runas = getenv("RUNAS_ID");
	struct passwd *pw;
	uid_t uid;

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T7FILE);
	VERIFYF(myuid == 0, "myuid = %d", myuid); /* Need root for this test. */

	/* Create file as root */
	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	fd = open(file, O_CREAT, 0400);
	VERIFYF(fd > 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	/* Become unprivileged user */
	if (runas != NULL) {
		uid = atoi(runas);
		VERIFYF(uid != 0, "runas = %s", runas);
	} else {
		pw = getpwnam("nobody");
		VERIFYF(pw != NULL, "errno = %d", errno);
		uid = pw->pw_uid;
	}
	rc = seteuid(uid);
	VERIFYF(rc == 0, "errno = %d", errno);
	errno = 0;
	llapi_layout_t *layout = llapi_layout_by_path(file);
	VERIFYF(layout == NULL && errno == EACCES, "errno = %d", errno);
	rc = seteuid(myuid);
	VERIFYF(rc == 0, "errno = %d", errno);
}


/* llapi_layout_by_path() returns ENODATA in errno for file with no
 * striping attributes. */
#define T8FILE		"t8"
#define T8_DESC		"llapi_layout_by_path ENODATA handling"
void test8(void)
{
	int fd;
	int rc;
	llapi_layout_t *layout;
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T8FILE);

	rc = unlink(file);
	VERIFYF(rc >= 0 || errno == ENOENT, "errno = %d", errno);
	fd = open(file, O_CREAT, 0640);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	errno = 0;
	layout = llapi_layout_by_path(file);
	VERIFYF(layout == NULL && errno == ENODATA, "errno = %d", errno);
}

/* Setting pattern > 0 returns EOPNOTSUPP in errno. */
#define T9_DESC		"llapi_layout_pattern_set() EOPNOTSUPP handling"
void test9(void)
{
	llapi_layout_t *layout;
	int rc;

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d\n", errno);
	errno = 0;
	rc = llapi_layout_pattern_set(layout, 1);
	VERIFYF(rc == -1 && errno == EOPNOTSUPP, "rc = %d, errno = %d", rc,
		errno);
	llapi_layout_free(layout);
}


/* Verify stripe_count interfaces return errors as expected */
#define T10_DESC	"stripe_count error handling"
void test10(void)
{
	int rc;
	uint64_t count;
	llapi_layout_t *layout;

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);

	/* invalid stripe count */
	errno = 0;
	rc = llapi_layout_stripe_count_set(layout, LLAPI_LAYOUT_INVALID_ARG);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_count_set(NULL, 2);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_count_get(NULL, &count);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL count */
	errno = 0;
	rc = llapi_layout_stripe_count_get(layout, NULL);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* stripe count too large */
	errno = 0;
	rc = llapi_layout_stripe_count_set(layout, LOV_MAX_STRIPE_COUNT + 1);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);
	llapi_layout_free(layout);
}

/* Verify stripe_size interfaces return errors as expected */
#define T11_DESC	"stripe_size error handling"
void test11(void)
{
	int rc;
	uint64_t size;
	llapi_layout_t *layout;

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);

	/* negative stripe size */
	errno = 0;
	rc = llapi_layout_stripe_size_set(layout, -1);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* invalid stripe size */
	errno = 0;
	rc = llapi_layout_stripe_size_set(layout, LLAPI_LAYOUT_INVALID_ARG);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* stripe size too big */
	errno = 0;
	rc = llapi_layout_stripe_size_set(layout, (1ULL << 33));
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_stripe_size_set(NULL, 1048576);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	errno = 0;
	rc = llapi_layout_stripe_size_get(NULL, &size);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL size */
	errno = 0;
	rc = llapi_layout_stripe_size_get(layout, NULL);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	llapi_layout_free(layout);
}

/* Verify pool_name interfaces return errors as expected */
#define T12_DESC	"pool_name error handling"
void test12(void)
{
	int rc;
	llapi_layout_t *layout;
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_pool_name_set(NULL, "foo");
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL pool name */
	errno = 0;
	rc = llapi_layout_pool_name_set(layout, NULL);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_pool_name_get(NULL, mypool, sizeof(mypool));
	VERIFYF(errno == EINVAL, "poolname = %s, errno = %d", poolname, errno);

	/* NULL buffer */
	errno = 0;
	rc = llapi_layout_pool_name_get(layout, NULL, sizeof(mypool));
	VERIFYF(errno == EINVAL, "poolname = %s, errno = %d", poolname, errno);

	/* Pool name too long*/
	errno = 0;
	rc = llapi_layout_pool_name_set(layout, "0123456789abcdef0");
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	llapi_layout_free(layout);
}

/* Verify ost_index interface returns errors as expected */
#define T13FILE			"t13"
#define T13_STRIPE_COUNT	2
#define T13_DESC		"ost_index error handling"
void test13(void)
{
	int rc;
	int fd;
	uint64_t idx;
	llapi_layout_t *layout;
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T13FILE);

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);

	/* Only setting OST index for stripe 0 is supported for now. */
	errno = 0;
	rc = llapi_layout_ost_index_set(layout, 1, 1);
	VERIFYF(rc == -1 && errno == EOPNOTSUPP, "rc = %d, errno = %d",
		rc, errno);

	/* invalid OST index */
	errno = 0;
	rc = llapi_layout_ost_index_set(layout, 0, LLAPI_LAYOUT_INVALID_ARG);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL layout */
	errno = 0;
	rc = llapi_layout_ost_index_set(NULL, 0, 1);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	errno = 0;
	rc = llapi_layout_ost_index_get(NULL, 0, &idx);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* NULL index */
	errno = 0;
	rc = llapi_layout_ost_index_get(layout, 0, NULL);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* Layout not read from file so has no OST data. */
	errno = 0;
	rc = llapi_layout_stripe_count_set(layout, T13_STRIPE_COUNT);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_ost_index_get(layout, 0, &idx);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	/* n greater than stripe count*/
	rc = unlink(file);
	VERIFYF(rc >= 0 || errno == ENOENT, "errno = %d", errno);
	rc = llapi_layout_stripe_count_set(layout, T13_STRIPE_COUNT);
	VERIFYF(rc == 0, "errno = %d", errno);
	fd = llapi_layout_file_create(file, 0, 0644, layout);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);
	llapi_layout_free(layout);

	layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);
	errno = 0;
	rc = llapi_layout_ost_index_get(layout, T13_STRIPE_COUNT + 1, &idx);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	llapi_layout_free(layout);
}

/* Verify llapi_layout_file_create() returns errors as expected */
#define T14_DESC	"llapi_layout_file_create error handling"
void test14(void)
{
	int rc;
	llapi_layout_t *layout = llapi_layout_alloc();

	/* NULL path */
	errno = 0;
	rc = llapi_layout_file_create(NULL, 0, 0, layout);
	VERIFYF(rc == -1 && errno == EINVAL, "rc = %d, errno = %d", rc, errno);

	llapi_layout_free(layout);
}

/* Can't change striping attributes of existing file. */
#define T15FILE			"t15"
#define T15_STRIPE_COUNT	2
#define T15_DESC	"Can't change striping attributes of existing file"
void test15(void)
{
	int rc;
	int fd;
	uint64_t count;
	llapi_layout_t *layout;
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T15FILE);

	rc = unlink(file);
	VERIFYF(rc >= 0 || errno == ENOENT, "errno = %d", errno);

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_stripe_count_set(layout, T15_STRIPE_COUNT);
	VERIFYF(rc == 0, "errno = %d", errno);

	errno = 0;
	fd = llapi_layout_file_create(file, 0, 0640, layout);
	VERIFYF(fd >= 0 && errno == 0, "fd = %d, errno = %d", fd, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	rc = llapi_layout_stripe_count_set(layout, T15_STRIPE_COUNT - 1);
	errno = 0;
	fd = llapi_layout_file_open(file, 0, 0640, layout);
	VERIFYF(fd >= 0, "fd = %d, errno = %d", fd, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);
	llapi_layout_free(layout);

	layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_stripe_count_get(layout, &count);
	VERIFYF(rc == 0 && count == T15_STRIPE_COUNT,
		"rc = %d, %"PRIu64" != %d", rc, count, T15_STRIPE_COUNT);
	llapi_layout_free(layout);
}

/* Default stripe attributes are applied as expected. */
#define T16FILE		"t16"
#define T16_DESC	"Default stripe attributes are applied as expected"
void test16(void)
{
	int		rc;
	int		fd;
	llapi_layout_t	*deflayout;
	llapi_layout_t	*filelayout;
	char		file[PATH_MAX];
	uint64_t	fsize;
	uint64_t	fcount;
	uint64_t	dsize;
	uint64_t	dcount;

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T16FILE);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	deflayout = __get_default_layout(file);
	VERIFYF(deflayout != NULL, "errno = %d", errno);
	rc = llapi_layout_stripe_size_get(deflayout, &dsize);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_count_get(deflayout, &dcount);
	VERIFYF(rc == 0, "errno = %d", errno);

	/* First, with a default llapi_layout_t */
	filelayout = llapi_layout_alloc();
	VERIFYF(filelayout != NULL, "errno = %d", errno);

	fd = llapi_layout_file_create(file, 0, 0640, filelayout);
	VERIFYF(fd >= 0, "errno = %d", errno);

	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	llapi_layout_free(filelayout);

	filelayout = llapi_layout_by_path(file);
	VERIFYF(filelayout != NULL, "errno = %d", errno);

	rc = llapi_layout_stripe_count_get(filelayout, &fcount);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(fcount == dcount, "%"PRIu64" != %"PRIu64, fcount, dcount);

	rc = llapi_layout_stripe_size_get(filelayout, &fsize);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(fsize == dsize, "%"PRIu64" != %"PRIu64, fsize, dsize);

	/* NULL layout also implies default layout */
	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	fd = llapi_layout_file_create(file, 0, 0640, filelayout);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);
	filelayout = llapi_layout_by_path(file);
	VERIFYF(filelayout != NULL, "errno = %d", errno);

	rc = llapi_layout_stripe_count_get(filelayout, &fcount);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_size_get(filelayout, &fsize);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(fcount == dcount, "%"PRIu64" != %"PRIu64, fcount, dcount);
	VERIFYF(fsize == dsize, "%"PRIu64" != %"PRIu64, fsize, dsize);

	llapi_layout_free(filelayout);
	llapi_layout_free(deflayout);
}

/* Setting stripe count to LLAPI_USE_ALL_OSTS uses all available OSTs. */
#define T17FILE		"t17"
#define T17_DESC	"count=LLAPI_USE_ALL_OSTS uses all available OSTs"
void test17(void)
{
	int rc;
	int fd;
	int osts_all;
	uint64_t osts_layout;
	llapi_layout_t *layout;
	char file[PATH_MAX];

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T17FILE);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);
	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_stripe_count_set(layout, LLAPI_USE_ALL_OSTS);
	VERIFYF(rc == 0, "errno = %d", errno);
	fd = llapi_layout_file_create(file, 0, 0640, layout);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);
	llapi_layout_free(layout);

	/* Get number of available OSTs */
	fd = open(file, O_RDONLY);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = llapi_lov_get_uuids(fd, NULL, &osts_all);
	VERIFYF(rc == 0, "rc = %d, errno = %d", rc, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_stripe_count_get(layout, &osts_layout);
	VERIFYF(osts_layout == osts_all, "%"PRIu64" != %d", osts_layout,
		osts_all);

	llapi_layout_free(layout);
}

/* Setting pool with "fsname.pool" notation. */
#define T18FILE		"t18"
#define T18_DESC	"Setting pool with fsname.pool notation"
void test18(void)
{
	int rc;
	int fd;
	llapi_layout_t *layout = llapi_layout_alloc();
	char file[PATH_MAX];
	char pool[LOV_MAXPOOLNAME*2 + 1];
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };

	snprintf(pool, sizeof(pool), "lustre.%s", poolname);

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T18FILE);

	VERIFYF(layout != NULL, "errno = %d", errno);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	rc = llapi_layout_pool_name_set(layout, pool);
	VERIFYF(rc == 0, "errno = %d", errno);

	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = strcmp(mypool, poolname);
	VERIFYF(rc == 0, "%s != %s", mypool, poolname);
	fd = llapi_layout_file_create(file, 0, 0640, layout);
	VERIFYF(fd >= 0, "errno = %d", errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	llapi_layout_free(layout);

	layout = llapi_layout_by_path(file);
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = strcmp(mypool, poolname);
	VERIFYF(rc == 0, "%s != %s", mypool, poolname);
	llapi_layout_free(layout);
}

#define T19_DESC	"Maximum length pool name is NULL-terminated"
void test19(void)
{
	llapi_layout_t *layout;
	char *name = "0123456789abcdef";
	char mypool[LOV_MAXPOOLNAME + 1] = { '\0' };
	int rc;

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);
	rc = llapi_layout_pool_name_set(layout, name);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_pool_name_get(layout, mypool, sizeof(mypool));
	VERIFYF(strlen(name) == strlen(mypool), "name = %s, str = %s", name,
		mypool);
	llapi_layout_free(layout);
}

#define T20FILE		"t20"
#define T20_DESC	"LLAPI_USE_FS_DEFAULT is honored"
void test20(void)
{
	int		rc;
	int		fd;
	llapi_layout_t	*deflayout;
	llapi_layout_t	*filelayout;
	char		file[PATH_MAX];
	uint64_t	fsize;
	uint64_t	fcount;
	uint64_t	dsize;
	uint64_t	dcount;

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T20FILE);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	filelayout = llapi_layout_alloc();
	VERIFYF(filelayout != NULL, "errno = %d", errno);

	rc = llapi_layout_stripe_size_set(filelayout, LLAPI_USE_FS_DEFAULT);
	VERIFYF(rc == 0, "rc = %d, errno = %d", rc, errno);

	rc = llapi_layout_stripe_count_set(filelayout, LLAPI_USE_FS_DEFAULT);
	VERIFYF(rc == 0, "rc = %d, errno = %d", rc, errno);

	fd = llapi_layout_file_create(file, 0, 0640, filelayout);
	VERIFYF(fd >= 0, "errno = %d", errno);

	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", errno);

	llapi_layout_free(filelayout);

	deflayout = __get_default_layout(file);
	VERIFYF(deflayout != NULL, "errno = %d", errno);

	filelayout = llapi_layout_by_path(file);
	VERIFYF(filelayout != NULL, "errno = %d", errno);

	rc = llapi_layout_stripe_count_get(filelayout, &fcount);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_count_get(deflayout, &dcount);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(fcount == dcount, "%"PRIu64" != %"PRIu64, fcount, dcount);

	rc = llapi_layout_stripe_size_get(filelayout, &fsize);
	VERIFYF(rc == 0, "errno = %d", errno);
	rc = llapi_layout_stripe_size_get(deflayout, &dsize);
	VERIFYF(rc == 0, "errno = %d", errno);
	VERIFYF(fsize == dsize, "%"PRIu64" != %"PRIu64, fsize, dsize);

	llapi_layout_free(filelayout);
	llapi_layout_free(deflayout);
}

#define T21_DESC	"llapi_layout_file_create fails for non-Lustre file"
void test21(void)
{
	llapi_layout_t *layout;
	char template[PATH_MAX];
	int fd;
	int rc;

	snprintf(template, sizeof(template), "%s/XXXXXX", P_tmpdir);
	fd = mkstemp(template);
	VERIFYF(fd >= 0, "template = %s, errno = %d", template, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", fd);
	rc = unlink(template);
	VERIFYF(rc == 0, "errno = %d", errno);

	layout = llapi_layout_alloc();
	VERIFYF(layout != NULL, "errno = %d", errno);

	fd = llapi_layout_file_create(template, 0, 0640, layout);
	VERIFYF(fd == -1 && errno == ENOTTY,
		"fd = %d, errno = %d, template = %s", fd, errno, template);
	llapi_layout_free(layout);
}

#define T22FILE		"t22"
#define T22_DESC	"llapi_layout_file_create applied mode correctly"
void test22(void)
{
	int		rc;
	int		fd;
	char		file[PATH_MAX];
	struct stat	st;
	mode_t		modein = 0640;
	mode_t		modeout;
	mode_t		umask_orig;

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T22FILE);

	rc = unlink(file);
	VERIFYF(rc == 0 || errno == ENOENT, "errno = %d", errno);

	umask_orig = umask(S_IWGRP | S_IWOTH);

	fd = llapi_layout_file_create(file, 0, modein, NULL);
	VERIFYF(fd >= 0, "errno = %d", errno);

	(void) umask(umask_orig);

	rc = fstat(fd, &st);
	VERIFYF(rc == 0, "errno = %d", errno);

	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", fd);

	modeout = st.st_mode & ~S_IFMT;
	VERIFYF(modein == modeout, "%o != %o", modein, modeout);
}

#define T23FILE		"t23"
#define T23_DESC	"Setting non-existing poolname is an error"
void test23(void)
{
	int rc;
	int fd;
	llapi_layout_t *layout = llapi_layout_alloc();
	char file[PATH_MAX];
	char pool[LOV_MAXPOOLNAME + 1];

	snprintf(pool, sizeof(pool), "%s", "nosuchpool");

	snprintf(file, sizeof(file), "%s/%s", lustre_dir, T23FILE);

	VERIFYF(layout != NULL, "errno = %d", errno);

	rc = llapi_layout_pool_name_set(layout, pool);
	VERIFYF(rc == 0, "errno = %d", errno);

	fd = llapi_layout_file_create(file, 0, 0640, layout);
	VERIFYF(fd < 0, "errno = %d", errno);

	llapi_layout_free(layout);

	layout = llapi_layout_by_path(file);
	VERIFYF(layout == NULL && errno == ENOENT, "errno = %d", errno);

	llapi_layout_free(layout);
}

#define T24_DESC	"llapi_layout_by_path fails for non-Lustre file"
void test24(void)
{
	llapi_layout_t *layout;
	char template[PATH_MAX];
	int fd;
	int rc;

	snprintf(template, sizeof(template), "%s/XXXXXX", P_tmpdir);
	fd = mkstemp(template);
	VERIFYF(fd >= 0, "template = %s, errno = %d", template, errno);
	rc = close(fd);
	VERIFYF(rc == 0, "errno = %d", fd);

	layout = llapi_layout_by_path(template);
	VERIFYF(layout == NULL && errno == ENOTTY,
		"errno = %d, template = %s", errno, template);

	rc = unlink(template);
	VERIFYF(rc == 0, "errno = %d", errno);
}

void sigsegv(int signal)
{
	printf("Segmentation fault\n");
	exit(1);
}

#define TEST_DESC_LEN	50
struct test_tbl_entry {
	void (*test_fn)(void);
	char test_desc[TEST_DESC_LEN];
};

static struct test_tbl_entry test_tbl[] = {
	{ &test0,  T0_DESC },
	{ &test1,  T1_DESC },
	{ &test2,  T2_DESC },
	{ &test3,  T3_DESC },
	{ &test4,  T4_DESC },
	{ &test5,  T5_DESC },
	{ &test6,  T6_DESC },
	{ &test7,  T7_DESC },
	{ &test8,  T8_DESC },
	{ &test9,  T9_DESC },
	{ &test10, T10_DESC },
	{ &test11, T11_DESC },
	{ &test12, T12_DESC },
	{ &test13, T13_DESC },
	{ &test14, T14_DESC },
	{ &test15, T15_DESC },
	{ &test16, T16_DESC },
	{ &test17, T17_DESC },
	{ &test18, T18_DESC },
	{ &test19, T19_DESC },
	{ &test20, T20_DESC },
	{ &test21, T21_DESC },
	{ &test22, T22_DESC },
	{ &test23, T23_DESC },
	{ &test24, T24_DESC },
};
#define NUM_TESTS	25

/* This function runs a single test by forking the process.  This way,
 * if there is a segfault during a test, the test program won't crash. */
int test(void (*test_fn)(), const char *test_desc, int test_num)
{
	int rc = -1;
	int i;

	pid_t pid = fork();
	if (pid > 0) {
		int status;
		wait(&status);
		printf(" test %2d: %s ", test_num, test_desc);
		for (i = 0; i < TEST_DESC_LEN - strlen(test_desc); i++)
			printf(".");
		/* Non-zero value indicates failure. */
		if (status == 0)
			printf(" pass\n");
		else
			printf(" fail (status = %d)\n", WEXITSTATUS(status));
		rc = status ? -1 : 0;
	} else if (pid == 0) {
		/* Run the test in the child process.  Exit with 0 for success,
		 * non-zero for failure */
		test_fn();
		exit(0);
	} else {
		printf("Fork failed!\n");
	}
	return rc;
}

static void process_args(int argc, char *argv[])
{
	int c;

	while ((c = getopt(argc, argv, "d:p:o:")) != -1) {
		switch (c) {
		case 'd':
			lustre_dir = optarg;
			break;
		case 'p':
			poolname = optarg;
			break;
		case 'o':
			num_osts = atoi(optarg);
			break;
		case '?':
			printf("Unknown option '%c'\n", optopt);
			usage(argv[0]);
		}
	}
}

int main(int argc, char *argv[])
{
	int rc = 0;
	int i;
	struct stat s;
	char fsname[8];

	llapi_msg_set_level(LLAPI_MSG_OFF);

	process_args(argc, argv);
	if (lustre_dir == NULL)
		lustre_dir = "/mnt/lustre";
	if (poolname == NULL)
		poolname = "testpool";
	if (num_osts == -1)
		num_osts = 2;

	if (num_osts < 2) {
		fprintf(stderr, "Error: at least 2 OSTS are required\n");
		exit(EXIT_FAILURE);
	}
	if (stat(lustre_dir, &s) < 0) {
		fprintf(stderr, "Error: %s: %s\n", lustre_dir, strerror(errno));
		exit(EXIT_FAILURE);
	} else if (!S_ISDIR(s.st_mode)) {
		fprintf(stderr, "Error: %s: not a directory\n", lustre_dir);
		exit(EXIT_FAILURE);
	}

	rc = llapi_search_fsname(lustre_dir, fsname);
	if (rc != 0) {
		fprintf(stderr, "Error: %s: not a Lustre filesystem\n",
			lustre_dir);
		exit(EXIT_FAILURE);
	}

	signal(SIGSEGV, sigsegv);

	/* Play nice with Lustre test scripts. Non-line buffered output
	 * stream under I/O redirection may appear incorrectly. */
	setvbuf(stdout, NULL, _IOLBF, 0);

	for (i = 0; i < NUM_TESTS; i++) {
		if (test(test_tbl[i].test_fn, test_tbl[i].test_desc, i) != 0)
			rc++;
	}
	return rc;
}
