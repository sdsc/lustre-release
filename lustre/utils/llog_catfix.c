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

static inline int ext2_clear_bit(int nr, void *addr)
{
#if __BYTE_ORDER == __BIG_ENDIAN
	return ((unsigned char *) addr)[nr >> 3] &= ~(1U << (nr & 7));
#else
	unsigned long *tmp = addr;
	return ((tmp)[nr / BITS_PER_LONG]) &=
		~(1UL << (nr & (BITS_PER_LONG - 1)));
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

	/* Add the other info you want to view here */

	printf("-----------------------\n");
	return;
}

static int rec_check_and_restore(struct llog_logid_rec *rec, int  idx)
{
	int need_restore = 0;

	/* all records in catalog must have valid header and
	 * tail no matter are they cancelled or alive */
	if (rec->lid_hdr.lrh_type != cpu_to_le32(LLOG_LOGID_MAGIC) ||
	    rec->lid_hdr.lrh_len != cpu_to_le32(sizeof(*rec)) ||
	    rec->lid_hdr.lrh_index != cpu_to_le32(idx)) {
		rec->lid_hdr.lrh_type = cpu_to_le32(LLOG_LOGID_MAGIC);
		rec->lid_hdr.lrh_len = cpu_to_le32(sizeof(*rec));
		rec->lid_hdr.lrh_index = cpu_to_le32(idx);
		rec->lid_hdr.lrh_id = 0;
		need_restore = 1;
	}

	if (rec->lid_tail.lrt_len != cpu_to_le32(sizeof(*rec)) ||
	    rec->lid_tail.lrt_index != cpu_to_le32(idx)) {
		rec->lid_tail.lrt_len = cpu_to_le32(sizeof(*rec));
		rec->lid_tail.lrt_index = cpu_to_le32(idx);
		need_restore = 1;
	}

	if (need_restore)
		printf("Invalid rec #%u, restoring...\n", idx);

	return need_restore;
}

int check_and_fix_first_last_idx(struct llog_log_hdr *hdr, int *first_idx,
				 int rec_count)
{
	unsigned long *bitmap = (unsigned long *)hdr->llh_bitmap;
	int bitmap_size = LLOG_HDR_BITMAP_SIZE(hdr);
	int i, idx = 0;

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
	struct llog_log_hdr *hdr = NULL;
	struct llog_logid_rec *rec = NULL;
	struct stat st;
	char *recs_buf = NULL;
	int idx, rc = 0, rd;
	int rec_count, first_idx, last_idx;
	int hdr_update = 0;
	int c, o_dry_run = 0;
	struct option long_opts[] = {
		{"dry-run",	   no_argument,	      &o_dry_run,    1},
		{"help",	   no_argument,	      NULL,	   'h'},
		{0, 0, 0, 0}
	};

	setlinebuf(stdout);

	optind = 0;
	while ((c = getopt_long(argc, argv, "h", long_opts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(argv[0], 0);
		case 0:
			break;
		default:
			exit(-EINVAL);
		}
	}

	if (argc != optind + 1) {
		rc = -EINVAL;
		printf("No file specified\n");
		return rc;
	}

	fd = open(argv[optind], O_RDWR);
	if (fd < 0) {
		printf("Could not open the file %s\n", argv[1]);
		goto out;
	}

	rc = fstat(fd, &st);
	if (rc < 0) {
		printf("Get file stat error.\n");
		goto out_fd;
	}

	hdr = (struct llog_log_hdr *)malloc(sizeof(*hdr));
	if (hdr == NULL) {
		printf("Failed to allocate buffer for llog header.\n");
		rc = -ENOMEM;
		goto out_fd;
	}

	rd = read(fd, hdr, sizeof(*hdr));
	if (rd < sizeof(*hdr)) {
		printf("Read file error.\n");
		rc = -EIO;
		goto clear_hdr_buf;
	}

	if (le32_to_cpu(hdr->llh_hdr.lrh_type) != LLOG_HDR_MAGIC) {
		printf("There is no llog_header, not llog file?\n");
		goto clear_hdr_buf;
	}

	print_llog_header(hdr);

	if (hdr->llh_size != sizeof(*rec)) {
		printf("Llog is not catalog, llh_size: %u, need %zu\n",
		       le32_to_cpu(hdr->llh_size), sizeof(*rec));
		rc = -ENOTDIR;
		goto clear_hdr_buf;
	}

	first_idx = le32_to_cpu(hdr->llh_cat_idx);
	printf("First record: %u\n", first_idx + 1);
	last_idx = le32_to_cpu(hdr->llh_tail.lrt_index);
	printf("Last record: %u\n", last_idx);

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

	recs_buf = malloc(LLOG_MIN_CHUNK_SIZE);
	if (recs_buf == NULL) {
		printf("Cannot allocate llog buffer\n");
		rc = -ENOMEM;
		goto clear_hdr_buf;
	}

	idx = 1; /* first record is the header */
	rec_count = 1;
	while ((rd = read(fd, recs_buf, LLOG_MIN_CHUNK_SIZE)) > 0) {
		int update = 0;

		if (rd % sizeof(*rec)) {
			printf("llog size is not aligned by records\n");
			/* probably it worths to truncate file to the closest
			 * rounded size */
			goto clear_recs_buf;
		}

		for (rec = (struct llog_logid_rec *)recs_buf;
		     (char *)rec < recs_buf + rd; rec++, idx++) {
			int restored;

			restored = rec_check_and_restore(rec, idx);
			update |= restored;
			if (ext2_test_bit(idx, hdr->llh_bitmap)) {
				if (restored == 0) {
					printf("Valid rec #%d: ogen=%X name="
					       DOSTID"\n", idx,
					       rec->lid_id.lgl_ogen,
					       POSTID(&rec->lid_id.lgl_oi));
					rec_count++;
				} else {
					ext2_clear_bit(idx, hdr->llh_bitmap);
					hdr_update = 1;
				}
			}
		}
		if (update && o_dry_run == 0) {
			off_t buf_off;

			/* need to re-write buffer */
			printf("Update records buffer\n");
			buf_off = lseek(fd, 0, SEEK_CUR);
			if (buf_off < 0) {
				printf("Cannot get current offset\n");
				goto clear_recs_buf;
			}
			buf_off -= rd;
			buf_off = lseek(fd, buf_off, SEEK_SET);
			if (buf_off < 0) {
				printf("Cannot set new offset\n");
				goto clear_recs_buf;
			}
			rc = write(fd, recs_buf, rd);
			if (rc != rd) {
				printf("Write less bytes than were read\n");
				goto clear_recs_buf;
			}
		}
	}

	printf("Total records: %u\n", rec_count);
	if (rec_count == 1) {
		/* there are no alive entries */
		first_idx = last_idx;
	} else {
		last_idx = check_and_fix_first_last_idx(hdr, &first_idx,
							rec_count);
	}

	if (last_idx != le32_to_cpu(hdr->llh_tail.lrt_index)) {
		printf("Last index was %u, new %u\n",
		       le32_to_cpu(hdr->llh_tail.lrt_index), last_idx);
		hdr->llh_tail.lrt_index = cpu_to_le32(last_idx);
		hdr_update = 1;
	}

	if (first_idx != le32_to_cpu(hdr->llh_cat_idx)) {
		printf("First index was %u, new %u\n",
		       le32_to_cpu(hdr->llh_cat_idx) + 1, first_idx + 1);
		hdr->llh_cat_idx = cpu_to_le32(first_idx);
		hdr_update = 1;
	}

	if (le32_to_cpu(hdr->llh_count) != rec_count) {
		printf("Update llh_count from %u to %u\n",
		       le32_to_cpu(hdr->llh_count), rec_count);
		hdr->llh_count = cpu_to_le32(rec_count);
		hdr_update = 1;
	}

	if (hdr_update && o_dry_run == 0) {
		off_t buf_off;

		printf("Update header\n");
		/* need to re-write buffer */
		buf_off = lseek(fd, 0, SEEK_SET);
		if (buf_off < 0) {
			printf("Cannot set zero offset\n");
			goto clear_recs_buf;
		}
		rc = write(fd, hdr, sizeof(*hdr));
		if (rc != sizeof(*hdr)) {
			printf("Write less bytes than header size\n");
			goto clear_recs_buf;
		}
	}
	rc = 0;
clear_recs_buf:
	free(recs_buf);
clear_hdr_buf:
	free(hdr);
out_fd:
	close(fd);
out:
	return rc;
}

