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
 * Copyright (c) 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Check and restore catalog llog.
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <endian.h>
#include <getopt.h>

#include <time.h>
#include <libcfs/libcfs.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustreapi.h>

static inline int ext2_test_bit(int nr, const void *addr)
{
#if __BYTE_ORDER == __BIG_ENDIAN
	return ((unsigned char *) addr)[nr >> 3] & (1U << (nr & 7));
#else
	const unsigned long *tmp = addr;
	return ((1UL << (nr & (BITS_PER_LONG - 1))) &
		((tmp)[nr / BITS_PER_LONG])) != 0;

#endif
}

static inline void ext2_clear_bit(int nr, void *addr)
{
#if __BYTE_ORDER == __BIG_ENDIAN
	((unsigned char *) addr)[nr >> 3] &= ~(1U << (nr & 7));
#else
	unsigned long *tmp = addr;
	(tmp)[nr / BITS_PER_LONG] &= ~(1UL << (nr & (BITS_PER_LONG - 1)));
#endif
}

void print_llog_header(struct llog_log_hdr *hdr)
{
	time_t t;

	printf("Header size : %u\n",
	       le32_to_cpu(hdr->llh_hdr.lrh_len));

	t = le64_to_cpu(hdr->llh_timestamp);
	printf("Time : %s", ctime(&t));

	printf("Number of records: %u\n",
	       le32_to_cpu(hdr->llh_count));

	printf("First record: %u\n", le32_to_cpu(hdr->llh_cat_idx) + 1);
	printf("Last record: %u\n", le32_to_cpu(LLOG_HDR_TAIL(hdr)->lrt_index));

	/* Add the other info you want to view here */

	printf("-----------------------\n");
	return;
}

static bool rec_check_and_restore(struct llog_logid_rec *rec, int  idx)
{
	bool need_restore = false;

	/* all records in catalog must have valid header and
	 * tail no matter are they cancelled or alive */
	if (rec->lid_hdr.lrh_type != cpu_to_le32(LLOG_LOGID_MAGIC) ||
	    rec->lid_hdr.lrh_len != cpu_to_le32(sizeof(*rec)) ||
	    rec->lid_hdr.lrh_index != cpu_to_le32(idx)) {
		rec->lid_hdr.lrh_type = cpu_to_le32(LLOG_LOGID_MAGIC);
		rec->lid_hdr.lrh_len = cpu_to_le32(sizeof(*rec));
		rec->lid_hdr.lrh_index = cpu_to_le32(idx);
		rec->lid_hdr.lrh_id = 0;
		need_restore = true;
	}

	if (rec->lid_tail.lrt_len != cpu_to_le32(sizeof(*rec)) ||
	    rec->lid_tail.lrt_index != cpu_to_le32(idx)) {
		rec->lid_tail.lrt_len = cpu_to_le32(sizeof(*rec));
		rec->lid_tail.lrt_index = cpu_to_le32(idx);
		need_restore = true;
	}

	if (need_restore)
		printf("Invalid rec #%u, restoring...\n", idx);

	return need_restore;
}

unsigned check_and_fix_first_last_idx(struct llog_log_hdr *hdr,
				      unsigned *first_idx, unsigned rec_count)
{
	unsigned long *bitmap = (unsigned long *)hdr->llh_bitmap;
	int bitmap_size = LLOG_HDR_BITMAP_SIZE(hdr);
	unsigned i, idx = 0;

	idx = (*first_idx + 1) % bitmap_size;
	/* skip index 0, it is header */
	if (idx == 0)
		idx = 1;

	/* fix first_idx if its bit is unset in bitmap */
	if (!ext2_test_bit(idx, hdr->llh_bitmap)) {
		for (i = idx + 1; i < bitmap_size; i++)
			if (ext2_test_bit(i, bitmap))
				break;
		if (i == bitmap_size)
			for (i = 1; i < idx; i++)
				if (ext2_test_bit(i, bitmap))
					break;
		idx = i;
	}

	*first_idx = idx - 1;
	if (rec_count == 2)
		return idx;

	for (i = idx + 1; i < bitmap_size && rec_count != 2; i++) {
		if (ext2_test_bit(i, bitmap)) {
			idx = i;
			rec_count--;
		}
	}
	/* start from 0 */
	for (i = 1; i < *first_idx && rec_count != 2; i++) {
		if (ext2_test_bit(i, bitmap)) {
			idx = i;
			rec_count--;
		}
	}

	return idx;
}

static void usage(const char *name, int rc)
{
	fprintf(stdout,
	" Usage: %s [options]... filename\n"
	" Options:\n"
	"   --dry-run   Don't save changes, just show what is done\n"
	"   --help      Show this help\n", name);

	exit(rc);
}

int main(int argc, char **argv)
{
	int fd;
	struct llog_rec_hdr lrh;
	struct llog_log_hdr *hdr = NULL;
	struct llog_logid_rec *rec = NULL;
	char *recs_buf = NULL;
	int rc = 0, rd, c, o_dry_run;
	unsigned rec_count, first_idx, last_idx, idx, chunk_size;
	bool hdr_update = 0;
	struct option long_opts[] = {
		{"dry-run",	   no_argument,	      &o_dry_run,    1},
		{"help",	   no_argument,	      NULL,	   'h'},
		{0, 0, 0, 0}
	};
	char *path;

	setlinebuf(stdout);

	optind = 0;
	while ((c = getopt_long(argc, argv, "h", long_opts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(program_invocation_short_name, 0);
		case 0:
			break;
		default:
			exit(EXIT_FAILURE);
		}
	}

	if (argc != optind + 1) {
		rc = -EINVAL;
		llapi_error(LLAPI_MSG_ERROR, rc, "%s: no file specified.\n",
			    program_invocation_short_name);
		return EXIT_FAILURE;
	}

	path = argv[optind];

	fd = open(path, o_dry_run == 0 ? O_RDWR : O_RDONLY);
	if (fd < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "%s: cannot open '%s': %s\n",
			    program_invocation_short_name, path,
			    strerror(errno));
		goto out;
	}

	/* Since llog chunk is variable size now, read just llog_rec_hdr first
	 * to get real llog header size and then continue with it. */
	rd = pread(fd, &lrh, sizeof(lrh), 0);
	if (rd < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: cannot read file '%s': %s\n",
			    program_invocation_short_name, path,
			    strerror(errno));
		goto out_fd;
	} else if (rd < sizeof(lrh)) {
		rc = -EIO;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: file '%s' is too short to be a llog.\n",
			    program_invocation_short_name, path);
		goto out_fd;
	}

	/* Sanity checks to make sure this is a llog file */
	if (le32_to_cpu(lrh.lrh_type) != LLOG_HDR_MAGIC) {
		rc = -EINVAL;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: file '%s' is not a llog file.\n",
			    program_invocation_short_name, path);
		goto out_fd;
	}

	chunk_size = le32_to_cpu(lrh.lrh_len);

	hdr = malloc(chunk_size);
	if (hdr == NULL) {
		rc = -ENOMEM;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: cannot allocate buffer for the llog header\n",
			    program_invocation_short_name);
		goto out_fd;
	}

	rd = read(fd, hdr, chunk_size);
	if (rd < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: cannot read file '%s': %s\n",
			    program_invocation_short_name, path,
			    strerror(errno));
		goto clear_hdr_buf;
	} else if (rd < chunk_size) {
		rc = -EIO;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: file '%s' is shorter than the llog header size\n",
			    program_invocation_short_name, path);
		goto clear_hdr_buf;
	}

	print_llog_header(hdr);

	if (le32_to_cpu(hdr->llh_size) != sizeof(*rec)) {
		rc = -EINVAL;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: file '%s' is not a llog catalog, llh_size: %u, need %zu\n",
			    program_invocation_short_name, path,
			    le32_to_cpu(hdr->llh_size), sizeof(*rec));
		goto clear_hdr_buf;
	}

	first_idx = le32_to_cpu(hdr->llh_cat_idx);
	last_idx = le32_to_cpu(LLOG_HDR_TAIL(hdr)->lrt_index);

	/* Main cycle. Since catalog may loop and records are inserted
	 * without changing their header and tail, we have to restore all
	 * possible headers and tails even records are deleted.
	 * - take record by their offset
	 * - check in bitmap is it cancelled or not
	 * - check header/tail are valid
	 * - check logid is valid
	 * - check record is inside first_idx - last_idx range
	 * - restore record header/tail is needed, correct bitmap, llh_count
	 *   if needed
	 */

	recs_buf = malloc(chunk_size);
	if (recs_buf == NULL) {
		rc = -ENOMEM;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: cannot allocate buffer for llog records\n",
			    program_invocation_short_name);
		goto clear_hdr_buf;
	}

	idx = 1; /* first record is the header */
	rec_count = 1;
	while ((rd = read(fd, recs_buf, chunk_size)) > 0) {
		bool update = 0;

		if (rd % sizeof(*rec)) {
			rc = -EFAULT;
			llapi_error(LLAPI_MSG_ERROR, rc,
				    "%s: read data is not aligned by records\n",
				    program_invocation_short_name);
			goto clear_recs_buf;
		}

		for (rec = (struct llog_logid_rec *)recs_buf;
		     (char *)rec < recs_buf + rd; rec++, idx++) {
			bool restored;

			restored = rec_check_and_restore(rec, idx);
			update |= restored;
			if (ext2_test_bit(idx, hdr->llh_bitmap)) {
				if (!restored) {
					printf("Valid rec #%d: ogen=%X name="
					       DOSTID"\n", idx,
					       rec->lid_id.lgl_ogen,
					       POSTID(&rec->lid_id.lgl_oi));
					rec_count++;
				} else {
					ext2_clear_bit(idx, hdr->llh_bitmap);
					hdr_update = true;
				}
			}
		}
		if (update) {
			off_t buf_off;

			/* need to re-write buffer */
			printf("Update records buffer\n");
			if (o_dry_run)
				continue;

			buf_off = lseek(fd, 0, SEEK_CUR);
			if (buf_off < 0) {
				rc = -errno;
				llapi_error(LLAPI_MSG_ERROR, rc,
					"%s: cannot get offset of file '%s': %s\n",
					program_invocation_short_name,
					path, strerror(errno));
				goto clear_recs_buf;
			}

			buf_off -= rd;
			rc = pwrite(fd, recs_buf, rd, buf_off);
			if (rc < 0) {
				rc = -errno;
				llapi_error(LLAPI_MSG_ERROR, rc,
					"%s: cannot write file '%s': %s\n",
					program_invocation_short_name,
					path, strerror(errno));
				goto clear_recs_buf;
			} else if (rc < rd) {
				rc = -EIO;
				llapi_error(LLAPI_MSG_ERROR, rc,
					"%s: write %d bytes instead of %d\n",
					program_invocation_short_name, rc, rd);
				goto clear_recs_buf;
			}
			rc = 0;
		}
	}

	if (rd < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "%s: cannot read records from file '%s': %s\n",
			    program_invocation_short_name, path,
			    strerror(errno));
		goto clear_recs_buf;
	}

	printf("Total records: %u\n", rec_count);
	if (rec_count == 1) {
		/* there are no alive entries */
		first_idx = last_idx;
	} else {
		last_idx = check_and_fix_first_last_idx(hdr, &first_idx,
							rec_count);
	}

	if (last_idx != le32_to_cpu(LLOG_HDR_TAIL(hdr)->lrt_index)) {
		printf("Last index was %u, new %u\n",
		       le32_to_cpu(LLOG_HDR_TAIL(hdr)->lrt_index), last_idx);
		LLOG_HDR_TAIL(hdr)->lrt_index = cpu_to_le32(last_idx);
		hdr_update = true;
	}

	if (first_idx != le32_to_cpu(hdr->llh_cat_idx)) {
		printf("First index was %u, new %u\n",
		       le32_to_cpu(hdr->llh_cat_idx) + 1, first_idx + 1);
		hdr->llh_cat_idx = cpu_to_le32(first_idx);
		hdr_update = true;
	}

	if (le32_to_cpu(hdr->llh_count) != rec_count) {
		printf("Update llh_count from %u to %u\n",
		       le32_to_cpu(hdr->llh_count), rec_count);
		hdr->llh_count = cpu_to_le32(rec_count);
		hdr_update = true;
	}

	if (hdr_update) {
		printf("Update header\n");
		/* need to re-write buffer */
		if (o_dry_run)
			goto clear_recs_buf;

		rd = pwrite(fd, hdr, chunk_size, 0);
		if (rd < 0) {
			rc = -errno;
			llapi_error(LLAPI_MSG_ERROR, rc,
				"%s: cannot write llog header: %s\n",
				program_invocation_short_name,
				strerror(errno));
			goto clear_recs_buf;
		} else if (rd < chunk_size) {
			rc = -EIO;
			llapi_error(LLAPI_MSG_ERROR, rc,
				"%s: write %d bytes instead of %d\n",
				program_invocation_short_name, rc, chunk_size);
			goto clear_recs_buf;
		}
	}

clear_recs_buf:
	free(recs_buf);
clear_hdr_buf:
	free(hdr);
out_fd:
	close(fd);
out:
	return rc;
}

