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
 * Copyright (c) 2016, DDN Storage Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/utils/ll_decode_linkea.c
 *
 * Tool for printing the MDT link_ea structure on the objects
 * in human readable form.
 *
 * Author: Li Xi <lixi@ddn.com>
 */


#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <liblustre.h>
#include <lustre/lustre_user.h>

#define BUFFER_SIZE 4096

int decode_linkea(const char *fname)
{
	char			 buf[BUFFER_SIZE];
	struct link_ea_header	*leh;
	ssize_t			 size;
	struct link_ea_entry	*lee;
	int			 i;
	__u64			 length;
	int			 reclen;
	struct lu_fid		 pfid;

	size = getxattr(fname, "trusted.link", buf, BUFFER_SIZE);
	if (size < 0) {
		if (errno == ERANGE) {
			fprintf(stderr, "%s: failed to read trusted.link "
				"xattr, the buffer size %u might be too "
				"small\n", fname, BUFFER_SIZE);
		} else {
			fprintf(stderr, "%s: failed to read trusted.link "
				"xattr: %s\n", fname, strerror(errno));
		}
		return -1;
	}

	leh = (struct link_ea_header *)buf;
	leh->leh_magic = le32_to_cpu(leh->leh_magic);
	leh->leh_reccount = le32_to_cpu(leh->leh_reccount);
	leh->leh_len = le64_to_cpu(leh->leh_len);
	if (leh->leh_magic != LINK_EA_MAGIC) {
		fprintf(stderr, "%s: magic mismatch, expected 0x%lx, "
			"got 0x%x\n", fname, LINK_EA_MAGIC, leh->leh_magic);
		return -1;
	} else if (leh->leh_reccount == 0) {
		fprintf(stderr, "%s: empty record count\n", fname);
		return -1;
	} else if (leh->leh_len > size) {
		fprintf(stderr, "%s: invalid length %llu, should smaller "
			"than %ld\n", fname, leh->leh_len, size);
		return -1;
	}

	length = sizeof(struct link_ea_header);
	lee = (struct link_ea_entry *)(leh + 1);
	printf("%s: count %u\n", fname, leh->leh_reccount);
	for (i = 0; i < leh->leh_reccount; i++) {
		reclen = (lee->lee_reclen[0] << 8) | lee->lee_reclen[1];
		length += reclen;
		if (length > leh->leh_len) {
			fprintf(stderr, "%s: length exceeded, expected %lld, "
				"got %lld\n", fname, leh->leh_len, length);
			return -1;
		}
		memcpy(&pfid, &lee->lee_parent_fid, sizeof(pfid));
		fid_be_to_cpu(&pfid, &pfid);

		printf("    pfid[%d]: "DFID"\n", i, PFID(&pfid));
		lee = (struct link_ea_entry *)((char *)lee + reclen);
	}

	if (length != leh->leh_len) {
		fprintf(stderr, "%s: length mismatch, expected %lld, "
			"got %lld\n", fname, leh->leh_len, length);
		return -1;
	}

	return 0;
}

int main(int argc, char *argv[])
{
	int	rc = 0;
	int	rc2;
	int	i;

	for (i = 1; i < argc; i++) {
		rc2 = decode_linkea(argv[i]);
		if (rc2 != 0)
			rc = rc2;
	}

	return rc;
}
