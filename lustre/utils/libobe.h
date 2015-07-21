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
 * lustre/utils/obelib.h
 *
 * Author: Alexey Zhuravlev <alexey.zhuravlev@intel.com>
 */

#ifndef __OBELIBH
#define __OBELIBH

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <limits.h>
#include <utime.h>
#include <sys/xattr.h>

#include "obdctl.h"
#include <lustre_ioctl.h>
#include <lnet/lnetctl.h>
#include <libcfs/util/string.h>
#include <libcfs/util/parser.h>
#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) ((sizeof(a)) / (sizeof((a)[0])))
#endif

struct dt_object_format;
struct dt_allocation_hint;
struct dt_key;
struct dt_rec;

struct dt_insert_rec {
	union {
		const struct lu_fid	*rec_fid;
		void			*rec_data;
	};
	union {
		struct {
			__u32		 rec_type;
			__u32		 rec_padding;
		};
		__u64			 rec_misc;
	};
};

enum la_valid {
	LA_ATIME = 1 << 0,
	LA_MTIME = 1 << 1,
	LA_CTIME = 1 << 2,
	LA_SIZE  = 1 << 3,
	LA_MODE  = 1 << 4,
	LA_UID   = 1 << 5,
	LA_GID   = 1 << 6,
	LA_BLOCKS = 1 << 7,
	LA_TYPE   = 1 << 8,
	LA_FLAGS  = 1 << 9,
	LA_NLINK  = 1 << 10,
	LA_RDEV   = 1 << 11,
	LA_BLKSIZE = 1 << 12,
	LA_KILL_SUID = 1 << 13,
	LA_KILL_SGID = 1 << 14,
};

struct lu_attr {
	/** size in bytes */
	__u64          la_size;
	/** modification time in seconds since Epoch */
	__u64		la_mtime;
	/** access time in seconds since Epoch */
	__u64		la_atime;
	/** change time in seconds since Epoch */
	__u64		la_ctime;
	/** 512-byte blocks allocated to object */
	__u64          la_blocks;
	/** permission bits and file type */
	__u32          la_mode;
	/** owner id */
	__u32          la_uid;
	/** group id */
	__u32          la_gid;
	/** object flags */
	__u32          la_flags;
	/** number of persistent references to this object */
	__u32          la_nlink;
	/** blk bits of the object*/
	__u32          la_blkbits;
	/** blk size of the object*/
	__u32          la_blksize;
	/** real device */
	__u32          la_rdev;
	/**
	 * valid bits
	 *
	 * \see enum la_valid
	 */
	__u64          la_valid;
};

struct lu_buf {
	void   *lb_buf;
	size_t  lb_len;
};

enum dt_format_type {
	DFT_REGULAR,
	DFT_DIR,
	/** for mknod */
	DFT_NODE,
	/** for special index */
	DFT_INDEX,
	/** for symbolic link */
	DFT_SYM,
};

struct dt_index_features {
	/** required feature flags from enum dt_index_flags */
	__u32 dif_flags;
	/** minimal required key size */
	size_t dif_keysize_min;
	/** maximal required key size, 0 if no limit */
	size_t dif_keysize_max;
	/** minimal required record size */
	size_t dif_recsize_min;
	/** maximal required record size, 0 if no limit */
	size_t dif_recsize_max;
	/** pointer size for record */
	size_t dif_ptrsize;
};

/**
 * object format specifier.
 */
struct dt_object_format {
	/** type for dt object */
	enum dt_format_type dof_type;
	union {
		struct dof_regular {
			int striped;
		} dof_reg;
		struct dof_dir {
		} dof_dir;
		struct dof_node {
		} dof_node;
		/**
		 * special index need feature as parameter to create
		 * special idx
		 */
		struct dof_index {
			const struct dt_index_features *di_feat;
		} dof_idx;
	} u;
};
struct obe_request {
	struct object_update_request *our_req;
	size_t			      our_size;
	void			     *our_reply;
	size_t			      our_reply_size;
	int			      our_dev;
	int			      our_rc;
};

int obe_create_pack(struct object_update *update,
		    size_t *max_update_size, const struct lu_fid *fid,
		    const struct lu_attr *attr, struct dt_allocation_hint *hint,
		    struct dt_object_format *dof);
int obe_ref_del_pack(struct object_update *update,
		     size_t *max_update_size, const struct lu_fid *fid);
int obe_ref_add_pack(struct object_update *update,
		     size_t *max_update_size, const struct lu_fid *fid);
int obe_attr_set_pack(struct object_update *update,
		      size_t *max_update_size, const struct lu_fid *fid,
		      const struct lu_attr *attr);
int obe_xattr_set_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const struct lu_buf *buf, const char *name, __u32 flag);
int obe_xattr_del_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const char *name);
int obe_index_insert_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_rec *rec, const struct dt_key *key);
int obe_index_delete_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_key *key);
int obe_object_destroy_pack(struct object_update *update,
			    size_t *max_update_size, const struct lu_fid *fid);
int obe_write_pack(struct object_update *update,
		   size_t *max_update_size, const struct lu_fid *fid,
		   const struct lu_buf *buf, __u64 pos);
int obe_index_lookup_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  struct dt_rec *rec, const struct dt_key *key);
int obe_attr_get_pack(struct object_update *update,
		      size_t *max_update_size, const struct lu_fid *fid);
int obe_xattr_get_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const char *name, const int bufsize);
int obe_read_pack(struct object_update *update,
		  size_t *max_update_size, const struct lu_fid *fid,
		  size_t size, loff_t pos);

struct object_update *obe_buffer_get_update(struct object_update_request *r,
					       unsigned int index);
int obe_object_update_request_size(const struct object_update_request *our);

int obe_update_request_size(const struct object_update_request *our);
int obe_extend_buffer(struct obe_request *our);
struct object_update_result *
object_update_result_get(const struct object_update_reply *reply,
			 unsigned int index, size_t *size);
int object_update_result_data_get(const struct object_update_reply *reply,
			      struct lu_buf *lbuf, size_t index);

#define obe_add_update(our, batchid, name, ...)				\
({									\
	struct object_update	*update;				\
	size_t			max;					\
	int ret;							\
									\
	while (1) {							\
		struct object_update_request *req = our->our_req;	\
		max = our->our_size - obe_update_request_size(req);	\
		update = obe_buffer_get_update(req, req->ourq_count);	\
		ret = obe_##name##_pack(update, &max, __VA_ARGS__);	\
		if (ret == -E2BIG) {					\
			int rc1 = obe_extend_buffer(our);		\
			if (rc1 != 0) {					\
				ret = rc1;				\
				break;					\
			}						\
			continue;					\
		} else {						\
			if (ret == 0) {					\
				our->our_req->ourq_count++;		\
				update->ou_batchid = batchid;		\
				our->our_reply_size +=			\
				sizeof(struct object_update_reply);	\
				our->our_reply_size +=			\
					update->ou_result_size;		\
				our->our_reply_size += sizeof(__u16);	\
			}						\
			break;						\
		}							\
	}								\
	if (our->our_rc == 0)						\
		our->our_rc = ret;					\
	ret;								\
})

static inline int
object_update_result_get_rc(const struct object_update_reply *reply, int index)
{
	struct object_update_result *update_result;
	if (reply == NULL)
		return -ENOBUFS;
	update_result = object_update_result_get(reply, index, NULL);
	if (update_result == NULL)
		return -ENOBUFS;
	return update_result->our_rc;
}

struct obe_request *obe_new(int device);
void obe_free(struct obe_request *our);
int obe_exec(struct obe_request *our);


#endif /* __OBELIBH */
