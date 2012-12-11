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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (C) 2012 DataDirect Networks, Inc.
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <getopt.h>
#include <sys/time.h>
#include <ext2fs/ext2fs.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* _GNU_SOURCE */

#if 0
#define MTRACE(format, args...) fprintf(stderr, "DEBUG: %s(%d) %s(): "	\
	format, __FILE__,  __LINE__, __func__, ##args)
#else
#define MTRACE(format, args...)
#endif
#define MDEBUG(format, args...) fprintf(stderr, "DEBUG: %s(%d) %s(): "	\
	format, __FILE__,  __LINE__, __func__, ##args)
#define MERROR(format, args...) fprintf(stderr, "ERROR: %s(%d) %s(): "	\
	format, __FILE__,  __LINE__, __func__, ##args)
#define MWARN(format, args...)  fprintf(stderr, "WARN: %s(%d) %s():  "	\
	format, __FILE__,  __LINE__, __func__, ##args)
#define MPRINT(format, args...) fprintf(stdout, format, ##args)

#define MENTRY()							\
do {									\
	MTRACE("entered\n");						\
} while (0)

#define _MRETURN()							\
do {									\
	MTRACE("leaving\n");						\
	return;								\
} while (0)

#define MRETURN(ret)							\
do {									\
	typeof(ret) RETURN__ret = (ret);				\
	MTRACE("leaving (ret = %lu:%ld:0x%lx)\n",			\
		(long)RETURN__ret, (long)RETURN__ret, (long)RETURN__ret);\
	return RETURN__ret;                                             \
} while (0)

#define MASSERT(cond) assert(cond)

#define LDISKFS_MMP_SEQ_CLEAN 0xFF4D4D50U /* mmp_seq value for clean unmount */
#define LDISKFS_MMP_SEQ_FSCK  0xE24D4D50U /* mmp_seq value when being fscked */

/* This codes based on dump_mmp() in debugfs.c in e2fsprogs. */
int dump_mmp(ext2_filsys filesystem, int check, int nodename, int check_factor)
{
	struct ext2_super_block *sb;
	int ret = 0;
	struct mmp_struct *mmp_s;
	time_t t;
	unsigned int interval = 0;
	__u32 seq = 0;
	MENTRY();

	MASSERT(filesystem);

	sb = filesystem->super;
	if (sb->s_mmp_block <= sb->s_first_data_block ||
	    sb->s_mmp_block >= ext2fs_blocks_count(sb)) {
		fprintf(stderr,
			"MMP block index is invalid, mmp_block = %llu\n",
			sb->s_mmp_block);
		ret = -EINVAL;
		goto out;
	}

	if (filesystem->mmp_buf == NULL) {
		ret = ext2fs_get_mem(filesystem->blocksize,\
			&filesystem->mmp_buf);
		if (ret) {
			fprintf(stderr,
				"Failed to alloc MMP buffer, ret = %d\n", ret);
			if (ret > 0)
				ret = -ret;
			goto out;
		}
	}

	mmp_s = filesystem->mmp_buf;
	ret = ext2fs_mmp_read(filesystem, filesystem->super->s_mmp_block,
		filesystem->mmp_buf);

	if (ret) {
		fprintf(stderr, "Failed to read MMP block, ret = %d\n", ret);
		if (ret > 0)
			ret = -ret;
		goto out;
	}

	t = mmp_s->mmp_time;
	if (!check && !nodename) {
		fprintf(stdout, "block_number: %llu\n",\
			filesystem->super->s_mmp_block);
		fprintf(stdout, "update_interval: %d\n",\
			filesystem->super->s_mmp_update_interval);
		fprintf(stdout, "check_interval: %d\n",\
			mmp_s->mmp_check_interval);
		fprintf(stdout, "sequence: %08x\n", mmp_s->mmp_seq);
		fprintf(stdout, "time: %lld -- %s", mmp_s->mmp_time, ctime(&t));
		fprintf(stdout, "node_name: %s\n", mmp_s->mmp_nodename);
		fprintf(stdout, "device_name: %s\n", mmp_s->mmp_bdevname);
		fprintf(stdout, "magic: 0x%x\n", mmp_s->mmp_magic);
	} else {
		if (mmp_s->mmp_seq == LDISKFS_MMP_SEQ_CLEAN) {
			fprintf(stderr, "MMP is NOT updated on '%s', "
				"because it is umounted\n",
				mmp_s->mmp_nodename);
			ret = 1;
			goto out;
		} else if (mmp_s->mmp_seq == LDISKFS_MMP_SEQ_FSCK) {
			fprintf(stderr, "MMP is NOT updated on '%s', "
				"because it is fscking\n",
				mmp_s->mmp_nodename);
			ret = 1;
			goto out;
		}
		seq = mmp_s->mmp_seq;

		interval = filesystem->super->s_mmp_update_interval;
		if (interval < mmp_s->mmp_check_interval)
			interval = mmp_s->mmp_check_interval;
		interval *= check_factor;
		MASSERT(interval > 0);

		sleep(interval);

		ret = ext2fs_mmp_read(filesystem,
			filesystem->super->s_mmp_block, filesystem->mmp_buf);

		if (ret) {
			fprintf(stderr,
				"failed to read MMP block, ret = %d\n", ret);
			if (ret > 0)
				ret = -ret;
			goto out;
		}

		if (mmp_s->mmp_seq == LDISKFS_MMP_SEQ_CLEAN) {
			fprintf(stderr, "MMP is NOT updated on '%s', "
				"because it is umounted while checking\n",
				mmp_s->mmp_nodename);
			ret = 1;
		} else if (mmp_s->mmp_seq == LDISKFS_MMP_SEQ_FSCK) {
			fprintf(stderr, "MMP is NOT updated on '%s', "
				"because it is fscking while checking\n",
				mmp_s->mmp_nodename);
			ret = 1;
		} else if (mmp_s->mmp_seq == seq) {
			fprintf(stderr, "MMP is NOT updated on '%s'\n",
				mmp_s->mmp_nodename);
			ret = 1;
		} else {
			/* Updated correctly */
			if (nodename) {
				fprintf(stdout, "%s\n", mmp_s->mmp_nodename);
			} else {
				fprintf(stdout,
					"MMP is updated on '%s' in '%d' seconds\n",
					mmp_s->mmp_nodename, interval);
			}
			ret = 0;
		}
	}

out:
	MRETURN(ret);
}

void usage(const char *progname)
{
	fprintf(stderr,
		"usage: %s [-c|--check] [-f|--factor] [-n|--nodename ] device\n"
		"\t-c: check MMP\n"
		"\t-f: time factor used while checking\n"
		"\t-n: check MMP, but only output the nodename\n", progname);
}


#define DEFAULT_CHECK_FACTOR 1
/*
 * Return 0 when updated in update_interval,
 * 1 if not, and negative if error
 */
int main(int argc, char **argv)
{
	char       *device_name = NULL;
	int         open_flags = 0;
	blk64_t     superblock = 0;
	blk64_t     blocksize = 0;
	ext2_filsys filesystem = NULL;
	int         ret;
	struct option long_opts[] = {
		{"check",     no_argument, 0, 'c'},
		{"factor",    required_argument, 0, 'f'},
		{"nodename",  no_argument, 0, 'n'},
		{"help",      no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};
	int c = 0;
	int check = 0;
	int nodename = 0;
	int check_factor = DEFAULT_CHECK_FACTOR;
	char *endptr = NULL;
	MENTRY();

	while ((c = getopt_long_only(argc, argv, "cnh",
					long_opts, NULL)) >= 0) {
		switch (c) {
		case 'c':
			check++;
			break;
		case 'n':
			nodename++;
			break;
		case 'f':
			check_factor = (int)strtol(argv[optind - 1],
						&endptr, 0);
			if (check_factor < 1) {
				fprintf(stderr,
					"error: %s: factor option '%s' is invalid\n",
					argv[0], argv[optind - 1]);
				ret = -1;
				goto out;
			}
			break;
		case 'h':
			usage(argv[0]);
			ret = -1;
			goto out;
		default:
			fprintf(stderr, "error: %s: option '%s' unrecognized\n",
				argv[0], argv[optind - 1]);
			usage(argv[0]);
			ret = -1;
			goto out;
		}
	}

	if (optind != argc - 1) {
		usage(argv[0]);
		ret = -1;
		goto out;
	}

	device_name = argv[optind];

	ret = ext2fs_open(device_name, open_flags, superblock, blocksize,
			  unix_io_manager, &filesystem);
	if (ret) {
		/*
		 * !!!BUG: ext2fs_open() leaks memory if fails to open device.
		 * Calling stack is ext2fs_open2 -> unix_open -> ext2fs_get_mem.
		 * io_channel->name is not freed when unix_open()
		 * fails and cleanups.
		 */
		MERROR("failed to open filesystem [%s], ret = %d\n",
			device_name, ret);
		if (ret > 0)
			ret = -ret;
		goto out;
	}

	ret = dump_mmp(filesystem, check, nodename, check_factor);
	ext2fs_close(filesystem);
out:
	MASSERT(ret < 0 || ret == 0 || ret == 1);
	MRETURN(ret);
}
