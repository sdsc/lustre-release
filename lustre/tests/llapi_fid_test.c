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
 * The purpose of this test is to test the llapi fid related function
 * (fid2path, path2fid, ...)
 *
 * The program will exit as soon a non zero error code is returned.
 */

#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <time.h>

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

#define PERFORM(testfn) \
	do {								\
		time_t now;						\
		now = time(NULL);					\
		printf("Starting test " #testfn " at %s", ctime(&now));	\
		testfn();						\
		now = time(NULL);					\
		printf("Finishing test " #testfn " at %s", ctime(&now)); \
	} while (0)

/* Name of file/directory. Will be set once and will not changed. */
static char mainpath[PATH_MAX];
static const char *maindir = "llapi_fid_test_name_9585766";

static char fsmountdir[PATH_MAX];	/* Lustre mountpoint */
static char *lustre_dir;		/* Test directory inside Lustre */

/* Cleanup our test file. */
static void cleanup(void)
{
	char cmd[PATH_MAX];
	int rc;

	rc = snprintf(cmd, sizeof(cmd), "rm -rf -- %s", mainpath);
	ASSERTF(rc > 0 && rc < sizeof(cmd),
		"invalid delete command for path %s", mainpath);
	system(cmd);
}

/* Helper - run path2fid and fid2path against an existing file/directory */
static void helper_fid2path(const char *filename)
{
	lustre_fid fid;
	lustre_fid fid2;
	char fidstr[FID_LEN];
	char buf[PATH_MAX];
	char buf2[PATH_MAX];
	long long recno;
	int linkno;
	int rc;

	rc = llapi_path2fid(filename, &fid);
	ASSERTF(rc == 0, "llapi_path2fid failed for %s: %s",
		filename, strerror(-rc));

	/* Without braces */
	snprintf(fidstr, sizeof(fidstr), DFID_NOBRACE, PFID(&fid));
	recno = -1;
	linkno = 0;
	rc = llapi_fid2path(lustre_dir, fidstr, buf,
			    sizeof(buf), &recno, &linkno);
	ASSERTF(rc == 0, "llapi_fid2path failed for fid %s: %s",
		fidstr, strerror(-rc));

	/* Same with braces */
	snprintf(fidstr, sizeof(fidstr), DFID, PFID(&fid));
	recno = -1;
	linkno = 0;
	rc = llapi_fid2path(lustre_dir, fidstr, buf,
			    sizeof(buf), &recno, &linkno);
	ASSERTF(rc == 0, "llapi_fid2path failed for fid %s: %s",
		fidstr, strerror(-rc));

	/* Pass the result back to fid2path and ensure the fid stays
	 * the same. */
	rc = snprintf(buf2, sizeof(buf2), "%s/%s", fsmountdir, buf);
	ASSERTF((rc > 0 && rc < sizeof(buf2)), "invalid name");
	rc = llapi_path2fid(buf2, &fid2);
	ASSERTF(rc == 0, "llapi_path2fid failed for %s: %s",
		buf2, strerror(-rc));
	ASSERTF(memcmp(&fid, &fid2, sizeof(fid)) == 0, "fids are different");
}

/* Test helper_fid2path */
static void test10(void)
{
	int rc;
	int fd;
	struct stat statbuf;

	cleanup();

	/* Against Lustre root */
	helper_fid2path(lustre_dir);

	/* Against a regular file */
	fd = creat(mainpath, 0);
	ASSERTF(fd >= 0, "can't create file %s: %s", strerror(errno), mainpath);
	close(fd);
	helper_fid2path(mainpath);
	rc = unlink(mainpath);
	ASSERTF(rc == 0, "unlink failed for %s: %s", mainpath, strerror(errno));

	/* Against a pipe */
	rc = mkfifo(mainpath, 0);
	ASSERTF(rc == 0, "mkfifo failed for %s: %s", mainpath, strerror(errno));
	helper_fid2path(mainpath);
	rc = unlink(mainpath);
	ASSERTF(rc == 0, "unlink failed for %s: %s", mainpath, strerror(errno));

	/* Against a directory */
	rc = mkdir(mainpath, 0);
	ASSERTF(rc == 0, "mkdir failed for %s: %s", mainpath, strerror(errno));
	helper_fid2path(mainpath);
	rc = rmdir(mainpath);
	ASSERTF(rc == 0, "rmdir failed for %s: %s", mainpath, strerror(errno));

	/* Against a char device. Use same as /dev/null in case things
	 * go wrong. */
	rc = stat("/dev/null", &statbuf);
	ASSERTF(rc == 0, "stat failed for /dev/null: %s", strerror(errno));
	rc = mknod(mainpath, S_IFCHR, statbuf.st_rdev);
	ASSERTF(rc == 0, "mknod failed for %s: %s", mainpath, strerror(errno));
	helper_fid2path(mainpath);
	rc = unlink(mainpath);
	ASSERTF(rc == 0, "unlink failed for %s: %s", mainpath, strerror(errno));

	/* Against a block device device. Reuse same dev. */
	rc = mknod(mainpath, S_IFBLK, statbuf.st_rdev);
	ASSERTF(rc == 0, "mknod failed for %s: %s", mainpath, strerror(errno));
	helper_fid2path(mainpath);
	rc = unlink(mainpath);
	ASSERTF(rc == 0, "unlink failed for %s: %s", mainpath, strerror(errno));

	/* Against a socket. */
	rc = mknod(mainpath, S_IFSOCK, (dev_t)0);
	ASSERTF(rc == 0, "mknod failed for %s: %s", mainpath, strerror(errno));
	helper_fid2path(mainpath);
	rc = unlink(mainpath);
	ASSERTF(rc == 0, "unlink failed for %s: %s", mainpath, strerror(errno));
}

/* Test with sub directories */
static void test20(void)
{
	char testpath[PATH_MAX];
	size_t len;
	int dir_created = 0;
	int rc;

	cleanup();

	rc = snprintf(testpath, sizeof(testpath), "%s", mainpath);
	ASSERTF((rc > 0 && rc < sizeof(testpath)),
		"invalid name for testpath: %s", mainpath);

	rc = mkdir(testpath, S_IRWXU);
	ASSERTF(rc == 0, "mkdir failed for %s: %s", testpath, strerror(errno));

	len = strlen(testpath);

	/* Create subdirectories as long as we can. Each new subdir is
	 * "/x", so we need at least 3 characters left in testpath. */
	while (len <= sizeof(testpath) - 3) {
		strncat(testpath, "/x", 2);

		len += 2;

		rc = mkdir(testpath, S_IRWXU);
		ASSERTF(rc == 0, "mkdir failed for %s: %s",
			testpath, strerror(errno));

		dir_created++;

		helper_fid2path(testpath);
	}

	/* And test the last one. */
	helper_fid2path(testpath);

	/* Make sure we have created enough directories. Even with a
	 * reasonably long mountpath, we should have created at least
	 * 2000. */
	ASSERTF(dir_created >= 2000, "dir_created=%d -- %s",
		dir_created, testpath);
}

/* Test linkno from fid2path */
static void test30(void)
{
	/* BUG if num_links gets a little bigger (eg. 200) -- See
	 * LU-5746 */
	const int num_links = 100;
	struct {
		char filename[PATH_MAX];
		bool seen;
	} links[num_links];
	char buf[PATH_MAX];
	char buf2[PATH_MAX];
	lustre_fid fid;
	char fidstr[FID_LEN];
	int rc;
	int i;
	int fd;
	int oldlinkno;
	int linkno;

	cleanup();

	/* Create the containing directory. */
	rc = mkdir(mainpath, 0);
	ASSERTF(rc == 0, "mkdir failed for %s: %s", mainpath, strerror(errno));

	/* Initializes the link array. */
	for (i = 0; i < num_links; i++) {
		rc = snprintf(links[i].filename, sizeof(links[i].filename),
			      "%s/%s/link%04d", lustre_dir, maindir, i);

		ASSERTF((rc > 0 && rc < sizeof(links[i].filename)),
			"invalid name for link");

		links[i].seen = false;
	}

	/* Create the original file. */
	fd = creat(links[0].filename, 0);
	ASSERTF(fd >= 0, "create failed for %s: %s",
		links[0].filename, strerror(errno));
	close(fd);

	rc = llapi_path2fid(links[0].filename, &fid);
	ASSERTF(rc == 0, "llapi_path2fid failed for %s: %s",
		links[0].filename, strerror(-rc));
	snprintf(fidstr, sizeof(fidstr), DFID_NOBRACE, PFID(&fid));

	/* Create the links */
	for (i = 1; i < num_links; i++) {
		rc = link(links[0].filename, links[i].filename);
		ASSERTF(rc == 0, "link failed for %s / %s: %s",
			links[0].filename, links[i].filename, strerror(errno));
	}

	/* Query the links, making sure we got all of them */
	linkno = 0;
	do {
		long long recno;
		bool found;

		/* Without braces */
		recno = -1;
		oldlinkno = linkno;
		rc = llapi_fid2path(links[0].filename, fidstr, buf,
				    sizeof(buf), &recno, &linkno);
		ASSERTF(rc == 0, "llapi_fid2path failed for fid %s: %s",
			fidstr, strerror(-rc));

		snprintf(buf2, sizeof(buf2), "%s/%s", fsmountdir, buf);

		/* Find the name in the links that were created */
		found = false;
		for (i = 0; i < num_links; i++) {
			if (strcmp(buf2, links[i].filename) == 0) {
				ASSERTF(links[i].seen == false,
					"link %s already seen",
					links[i].filename);
				links[i].seen = true;
				found = true;
				break;
			}
		}
		ASSERTF(found == true, "link %s not found", buf2);

	} while (linkno != oldlinkno);

	ASSERTF(linkno == num_links-1, "bad linkno: %d", linkno);
}

/* Test llapi_fd2parent/llapi_path2parent on mainpath (whatever its
 * type). mainpath must exists. */
static void help_test40(void)
{
	lustre_fid parent_fid;
	lustre_fid fid2;
	char buf[PATH_MAX];
	int rc;

	/* Successful call */
	memset(buf, 0x55, sizeof(buf));
	rc = llapi_path2parent(mainpath, 0, &parent_fid, buf, PATH_MAX);
	ASSERTF(rc == 0, "llapi_path2parent failed for %s: %s",
		mainpath, strerror(errno));
	ASSERTF(strcmp(buf, maindir) == 0, "strings are different: %s / %s",
		buf, maindir);

	/* Since mainpath is just under the root of Lustre, we can
	 * check the result. */
	rc = llapi_path2fid(lustre_dir, &fid2);
	ASSERTF(rc == 0, "llapi_path2fid failed for %s: %s",
		lustre_dir, strerror(-rc));
	ASSERTF(memcmp(&parent_fid, &fid2, sizeof(fid2)) == 0,
		"fids are different");

	/* Name too short */
	rc = llapi_path2parent(mainpath, 0, &parent_fid, buf, 0);
	ASSERTF(rc == -EOVERFLOW, "llapi_path2parent error: %s", strerror(-rc));

	rc = llapi_path2parent(mainpath, 0, &parent_fid, buf, 5);
	ASSERTF(rc == -EOVERFLOW, "llapi_path2parent error: %s", strerror(-rc));

	rc = llapi_path2parent(mainpath, 0, &parent_fid, buf, strlen(maindir));
	ASSERTF(rc == -EOVERFLOW, "llapi_path2parent error: %s", strerror(-rc));

	rc = llapi_path2parent(mainpath, 0, &parent_fid, buf,
			       strlen(maindir)+1);
	ASSERTF(rc == 0, "llapi_path2parent failed: %s", strerror(-rc));
}

static void test40(void)
{
	int fd;
	int rc;

	cleanup();

	/* Against a directory. */
	rc = mkdir(mainpath, 0);
	ASSERTF(rc == 0, "mkdir failed for %s: %s", mainpath, strerror(errno));
	help_test40();

	cleanup();

	/* Against a regular file */
	fd = creat(mainpath, 0);
	ASSERTF(fd >= 0, "creat failed for %s: %s", mainpath, strerror(errno));
	close(fd);
}

/* Test LL_IOC_GETPARENT directly */
static void test41(void)
{
	int rc;
	int fd;
	int i;
	union {
		struct getparent gp;
		char buf[1024];
	} u;

	cleanup();

	/* Against a regular file */
	fd = creat(mainpath, 0);
	ASSERTF(fd >= 0, "creat failed for %s: %s", mainpath, strerror(errno));

	/* Ask a few times */
	for (i = 0; i < 256; i++) {
		memset(u.buf, i, sizeof(u.buf)); /* poison */
		u.gp.gp_linkno = 0;
		u.gp.gp_name_size = 100;

		rc = ioctl(fd, LL_IOC_GETPARENT, &u.gp);
		ASSERTF(rc == 0, "LL_IOC_GETPARENT failed: %s, rc=%d",
			strerror(errno), rc);
		ASSERTF(strcmp(u.gp.gp_name, maindir) == 0,
			"strings are different: %zd, %zd",
			strlen(u.gp.gp_name), strlen(maindir));
	}

	close(fd);
}

/* Test with linkno. Create sub directories, and put a link to the
 * original file in them. */
static void test42(void)
{

	const int num_links = 100;
	struct {
		char subdir[PATH_MAX];
		lustre_fid subdir_fid;
		char filename[PATH_MAX];
		bool seen;
	} links[num_links];
	char link0[PATH_MAX];
	char buf[PATH_MAX];
	int rc;
	int i;
	int fd;
	int linkno;
	lustre_fid parent_fid;

	cleanup();

	/* Create the containing directory. */
	rc = mkdir(mainpath, 0);
	ASSERTF(rc == 0, "mkdir failed: for %s: %s", mainpath, strerror(errno));

	/* Initializes the link array. */
	for (i = 0; i < num_links; i++) {
		rc = snprintf(links[i].subdir, sizeof(links[i].subdir),
			      "%s/sub%04d", mainpath, i);
		ASSERTF((rc > 0 && rc < sizeof(links[i].subdir)),
			"invalid name for subdir");

		rc = snprintf(links[i].filename, sizeof(links[i].filename),
			      "link%04d", i);
		ASSERTF((rc > 0 && rc < sizeof(links[i].filename)),
			"invalid name for link");

		links[i].seen = false;
	}

	/* Create the subdirectories. */
	for (i = 0; i < num_links; i++) {
		rc = mkdir(links[i].subdir, S_IRWXU);
		ASSERTF(rc == 0, "mkdir failed for %s: %s",
			links[i].subdir, strerror(errno));

		rc = llapi_path2fid(links[i].subdir, &links[i].subdir_fid);
		ASSERTF(rc == 0, "llapi_path2fid failed for %s: %s",
			links[i].subdir, strerror(-rc));
	}

	/* Create the original file. */
	rc = snprintf(link0, sizeof(link0), "%s/%s",
		      links[0].subdir, links[0].filename);
	ASSERTF((rc > 0 && rc < sizeof(link0)), "invalid name for file");

	fd = creat(link0, 0);
	ASSERTF(fd >= 0, "create failed for %s: %s", link0, strerror(errno));
	close(fd);

	/* Create the links */
	for (i = 1; i < num_links; i++) {
		rc = snprintf(buf, sizeof(buf), "%s/%s",
			      links[i].subdir, links[i].filename);
		ASSERTF((rc > 0 && rc < sizeof(buf)),
			"invalid name for link %d", i);

		rc = link(link0, buf);
		ASSERTF(rc == 0, "link failed for %s / %s: %s",
			link0, buf, strerror(errno));
	}

	/* Query the links, making sure we got all of them. Do it in
	 * reverse order, just because! */
	for (linkno = num_links-1; linkno >= 0; linkno--) {
		bool found;

		rc = llapi_path2parent(link0, linkno, &parent_fid, buf,
				       sizeof(buf));
		ASSERTF(rc == 0, "llapi_path2parent failed for %s: %s",
			link0, strerror(-rc));

		/* Find the name in the links that were created */
		found = false;
		for (i = 0; i < num_links; i++) {
			if (memcmp(&parent_fid, &links[i].subdir_fid,
				   sizeof(parent_fid)) != 0)
				continue;

			ASSERTF(strcmp(links[i].filename, buf) == 0,
				"name differ: %s %s", links[i].filename, buf);
			ASSERTF(links[i].seen == false,
				"link %s already seen", links[i].filename);
			links[i].seen = true;
			found = true;
			break;
		}
		ASSERTF(found == true, "link %s not found", buf);
	}

	/* check non existent n+1 link */
	rc = llapi_path2parent(link0, num_links, &parent_fid, buf, sizeof(buf));
	ASSERTF(rc == -ENODATA, "llapi_path2parent error for %s: %s",
		link0, strerror(-rc));
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

	PERFORM(test10);
	PERFORM(test20);
	PERFORM(test30);
	PERFORM(test40);
	PERFORM(test41);
	PERFORM(test42);

	return EXIT_SUCCESS;
}
