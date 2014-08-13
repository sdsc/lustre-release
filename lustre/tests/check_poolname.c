/*
 * Set stripe data with pool name on two files: one using ioctl() and the
 * other using fsetxattr().  Check whether the poolname is saved on both
 * files.
 *
 * gcc  -Wall -g -Werror -o check_poolname check_poolname.c -llustreapi
 */

#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <lustre/lustreapi.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <attr/xattr.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE         /* See feature_test_macros(7) */
#endif

#include <errno.h>
extern char *program_invocation_short_name;

void *alloc_lum()
{
	size_t sz;

	sz = sizeof(struct lov_user_md_v3) +
		    LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
	return malloc(sz);
}

void init_lum(struct lov_user_md_v3 *lump, const char *pool)
{
	bzero(lump, sizeof(*lump));
	lump->lmm_magic		= LOV_USER_MAGIC_V3;
	lump->lmm_pattern	= 0;
	lump->lmm_stripe_size	= 1048576;
	lump->lmm_stripe_count	= 1;
	lump->lmm_stripe_offset	= -1;
	strncpy(lump->lmm_pool_name, pool, LOV_MAXPOOLNAME);
}

int pool_name_presence(const char *file, const char *pool)
{
	struct lov_user_md_v3 *lump;
	int rc;

	lump = alloc_lum();
	if (lump == NULL)
		return -1;
	rc = llapi_file_get_stripe(file, (void *)lump);
	if (rc != 0) {
		free(lump);
		return rc;
	}
	rc = strcmp(lump->lmm_pool_name, pool);
	free(lump);
	return rc;
}

int main(int argc, char *argv[])
{
	struct lov_user_md_v3 lum;
	char *file1;
	char *file2;
	char *poolname;
	int rc;
	int rc1;
	int rc2;
	int fd;

	if (argc != 4) {
		fprintf(stderr, "Usage: %s <file1> <file2> <poolname>\n",
			program_invocation_short_name);
		exit(-1);
	}

	file1 = argv[1];
	file2 = argv[2];
	poolname = argv[3];

	rc = unlink(file1);
	assert(rc == 0 || errno == ENOENT);
	rc = unlink(file2);
	assert(rc == 0 || errno == ENOENT);

	init_lum(&lum, poolname);

	/* Try ioctl(). */
	fd = open(file1, O_LOV_DELAY_CREATE | O_CREAT | O_EXCL, 0640);
	assert(fd >= 0);
	rc = ioctl(fd, LL_IOC_LOV_SETSTRIPE, &lum);
	close(fd);
	rc1 = pool_name_presence(file1, poolname);
	printf("%s: poolname '%s' %s via ioctl() for file %s\n",
	       program_invocation_short_name, poolname,
	       rc1 == 0 ? "accepted" : "rejected", file1);

	/* Try setxattr(). */
	fd = open(file2, O_LOV_DELAY_CREATE | O_CREAT | O_EXCL, 0640);
	assert(fd >= 0);
	rc = fsetxattr(fd, "lustre.lov", (void *)&lum, sizeof(lum), 0);
	close(fd);
	rc2 = pool_name_presence(file1, poolname);
	printf("%s: poolname '%s' %s via fsetxattr() for file %s\n",
	       program_invocation_short_name, poolname,
	       rc2 == 0 ? "accepted" : "rejected", file2);

	rc = rc1;
	if (rc == 0 && rc2 != 0)
		rc = rc2;

	return rc;
}
