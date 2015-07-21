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
 * lustre/utils/osdbench.c
 *
 * Author: Alexey Zhuravlev <alexey.zhuravlev@intel.com>
 */

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
#include "obelib.h"

static inline void *
object_update_param_get(const struct object_update *update,
			size_t index, size_t *size)
{
	const struct	object_update_param *param;
	size_t		i;

	if (index >= update->ou_params_count)
		return ERR_PTR(-EINVAL);

	param = &update->ou_params[0];
	for (i = 0; i < index; i++)
		param = (struct object_update_param *)((char *)param +
			object_update_param_size(param));

	if (size != NULL)
		*size = param->oup_len;

	if (param->oup_len == 0)
		return NULL;

	return (void *)&param->oup_buf[0];
}

int obe_update_request_size(const struct object_update_request *our)
{
	unsigned long	size;
	size_t		i = 0;

	size = offsetof(struct object_update_request, ourq_updates[0]);
	for (i = 0; i < our->ourq_count; i++) {
		struct object_update *update;

		update = (struct object_update *)((char *)our + size);
		size += object_update_size(update);
	}
	return size;
}

static int obe_update_header_pack(struct object_update *update,
			   size_t *max_update_size,
			   enum update_type update_op,
			   const struct lu_fid *fid,
			   unsigned int param_count, __u16 *param_sizes)
{
	struct object_update_param	*param;
	unsigned int			i;
	size_t				update_size;

	/* Check whether the packing exceeding the maxima update length */
	update_size = sizeof(*update);
	for (i = 0; i < param_count; i++)
		update_size += cfs_size_round(sizeof(*param) + param_sizes[i]);

	if (unlikely(update_size >= *max_update_size)) {
		*max_update_size = update_size;
		return -E2BIG;
	}

	update->ou_fid = *fid;
	update->ou_type = update_op;
	update->ou_params_count = param_count;
	param = &update->ou_params[0];
	for (i = 0; i < param_count; i++) {
		param->oup_len = param_sizes[i];
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	return 0;
}

int obe_update_pack(struct object_update *update,
		    size_t *max_update_size, enum update_type op,
		    const struct lu_fid *fid, unsigned int param_count,
		    __u16 *param_sizes, const void **param_bufs)
{
	struct object_update_param	*param;
	unsigned int			i;
	int				rc;

	rc = obe_update_header_pack(update, max_update_size, op, fid,
				    param_count, param_sizes);
	if (rc != 0)
		return rc;

	param = &update->ou_params[0];
	for (i = 0; i < param_count; i++) {
		memcpy(&param->oup_buf[0], param_bufs[i], param_sizes[i]);
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	return 0;
}

#define S_IRWXUGO	(S_IRWXU|S_IRWXG|S_IRWXO)
#define S_IALLUGO	(S_ISUID|S_ISGID|S_ISVTX|S_IRWXUGO)

void obdo_from_la(struct obdo *dst, const struct lu_attr *la, __u64 valid)
{
	__u64 newvalid = 0;

	if (valid & LA_ATIME) {
		dst->o_atime = la->la_atime;
		newvalid |= OBD_MD_FLATIME;
	}
	if (valid & LA_MTIME) {
		dst->o_mtime = la->la_mtime;
		newvalid |= OBD_MD_FLMTIME;
	}
	if (valid & LA_CTIME) {
		dst->o_ctime = la->la_ctime;
		newvalid |= OBD_MD_FLCTIME;
	}
	if (valid & LA_SIZE) {
		dst->o_size = la->la_size;
		newvalid |= OBD_MD_FLSIZE;
	}
	if (valid & LA_BLOCKS) {  /* allocation of space (x512 bytes) */
		dst->o_blocks = la->la_blocks;
		newvalid |= OBD_MD_FLBLOCKS;
	}
	if (valid & LA_TYPE) {
		dst->o_mode = (dst->o_mode & S_IALLUGO) |
			(la->la_mode & S_IFMT);
		newvalid |= OBD_MD_FLTYPE;
	}
	if (valid & LA_MODE) {
		dst->o_mode = (dst->o_mode & S_IFMT) |
			(la->la_mode & S_IALLUGO);
		newvalid |= OBD_MD_FLMODE;
	}
	if (valid & LA_UID) {
		dst->o_uid = la->la_uid;
		newvalid |= OBD_MD_FLUID;
	}
	if (valid & LA_GID) {
		dst->o_gid = la->la_gid;
		newvalid |= OBD_MD_FLGID;
	}
	if (valid & LA_FLAGS) {
		dst->o_flags = la->la_flags;
		newvalid |= OBD_MD_FLFLAGS;
	}
	dst->o_valid |= newvalid;
}

void la_from_obdo(struct lu_attr *dst, const struct obdo *obdo, __u64 valid)
{
	__u64 newvalid = 0;

	valid &= obdo->o_valid;

	if (valid & OBD_MD_FLATIME) {
		dst->la_atime = obdo->o_atime;
		newvalid |= LA_ATIME;
	}
	if (valid & OBD_MD_FLMTIME) {
		dst->la_mtime = obdo->o_mtime;
		newvalid |= LA_MTIME;
	}
	if (valid & OBD_MD_FLCTIME) {
		dst->la_ctime = obdo->o_ctime;
		newvalid |= LA_CTIME;
	}
	if (valid & OBD_MD_FLSIZE) {
		dst->la_size = obdo->o_size;
		newvalid |= LA_SIZE;
	}
	if (valid & OBD_MD_FLBLOCKS) {
		dst->la_blocks = obdo->o_blocks;
		newvalid |= LA_BLOCKS;
	}
	if (valid & OBD_MD_FLTYPE) {
		dst->la_mode = (dst->la_mode & S_IALLUGO) |
			(obdo->o_mode & S_IFMT);
		newvalid |= LA_TYPE;
	}
	if (valid & OBD_MD_FLMODE) {
		dst->la_mode = (dst->la_mode & S_IFMT) |
			(obdo->o_mode & S_IALLUGO);
		newvalid |= LA_MODE;
	}
	if (valid & OBD_MD_FLUID) {
		dst->la_uid = obdo->o_uid;
		newvalid |= LA_UID;
	}
	if (valid & OBD_MD_FLGID) {
		dst->la_gid = obdo->o_gid;
		newvalid |= LA_GID;
	}
	if (valid & OBD_MD_FLFLAGS) {
		dst->la_flags = obdo->o_flags;
		newvalid |= LA_FLAGS;
	}
	dst->la_valid = newvalid;
}

int obe_create_pack(struct object_update *update,
		    size_t *max_update_size, const struct lu_fid *fid,
		    const struct lu_attr *attr, struct dt_allocation_hint *hint,
		    struct dt_object_format *dof)
{
	struct obdo		*obdo;
	__u16			sizes[3] = {sizeof(*obdo), 0, 0};
	int			buf_count = 1;
	const struct lu_fid	*parent_fid = NULL;
	int			rc;

	if (dof != NULL) {
		sizes[1] = sizeof(__u8);
		buf_count++;
	}
#if 0
	if (hint != NULL && hint->dah_parent) {
		parent_fid = lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*parent_fid);
		buf_count++;
	}
#endif

	rc = obe_update_header_pack(update, max_update_size, OUT_CREATE,
				    fid, buf_count, sizes);
	if (rc != 0)
		return rc;

	obdo = object_update_param_get(update, 0, NULL);
	LASSERT(obdo != NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	if (dof != NULL) {
		__u8 *type = object_update_param_get(update, 1, NULL);
		*type = dof->dof_type;

	}
	if (parent_fid != NULL) {
		struct lu_fid *tmp = object_update_param_get(update, 2, NULL);
		fid_cpu_to_le(tmp, parent_fid);
	}


	return 0;
}

int obe_ref_del_pack(struct object_update *update,
		     size_t *max_update_size, const struct lu_fid *fid)
{
	return obe_update_pack(update, max_update_size, OUT_REF_DEL, fid,
			       0, NULL, NULL);
}

int obe_ref_add_pack(struct object_update *update,
		     size_t *max_update_size, const struct lu_fid *fid)
{
	return obe_update_pack(update, max_update_size, OUT_REF_ADD, fid,
			       0, NULL, NULL);
}

int obe_attr_set_pack(struct object_update *update,
		      size_t *max_update_size, const struct lu_fid *fid,
		      const struct lu_attr *attr)
{
	struct obdo		*obdo;
	__u16			size = sizeof(*obdo);
	int			rc;

	rc = obe_update_header_pack(update, max_update_size,
				    OUT_ATTR_SET, fid, 1, &size);
	if (rc != 0)
		return rc;

	obdo = object_update_param_get(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	return 0;
}

int obe_xattr_set_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const struct lu_buf *buf, const char *name, __u32 flag)
{
	__u16	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(flag)};
	const void *bufs[3] = {(char *)name, (char *)buf->lb_buf,
			       (char *)&flag};

	return obe_update_pack(update, max_update_size, OUT_XATTR_SET,
			       fid, ARRAY_SIZE(sizes), sizes, bufs);
}

int obe_xattr_del_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const char *name)
{
	__u16	size = strlen(name) + 1;

	return obe_update_pack(update, max_update_size, OUT_XATTR_DEL,
			       fid, 1, &size, (const void **)&name);
}

int obe_index_insert_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_rec *rec, const struct dt_key *key)
{
	struct dt_insert_rec	   *rec1 = (struct dt_insert_rec *)rec;
	struct lu_fid		   rec_fid;
	__u32			    type = cpu_to_le32(rec1->rec_type);
	__u16			    sizes[3] = { strlen((char *)key) + 1,
						sizeof(rec_fid),
						sizeof(type) };
	const void		   *bufs[3] = { (char *)key,
						(char *)&rec_fid,
						(char *)&type };

	fid_cpu_to_le(&rec_fid, rec1->rec_fid);

	return obe_update_pack(update, max_update_size, OUT_INDEX_INSERT,
			       fid, ARRAY_SIZE(sizes), sizes, bufs);
}

int obe_index_delete_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_key *key)
{
	__u16	size = strlen((char *)key) + 1;
	const void *buf = key;

	return obe_update_pack(update, max_update_size, OUT_INDEX_DELETE,
			       fid, 1, &size, &buf);
}

int obe_object_destroy_pack(struct object_update *update,
			    size_t *max_update_size, const struct lu_fid *fid)
{
	return obe_update_pack(update, max_update_size, OUT_DESTROY, fid,
			       0, NULL, NULL);
}

int obe_write_pack(struct object_update *update,
		   size_t *max_update_size, const struct lu_fid *fid,
		   const struct lu_buf *buf, __u64 pos)
{
	__u16		sizes[2] = {buf->lb_len, sizeof(pos)};
	const void	*bufs[2] = {(char *)buf->lb_buf, (char *)&pos};
	int		rc;

	pos = cpu_to_le64(pos);
	rc = obe_update_pack(update, max_update_size, OUT_WRITE, fid,
			     ARRAY_SIZE(sizes), sizes, bufs);
	return rc;
}

int obe_index_lookup_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  struct dt_rec *rec, const struct dt_key *key)
{
	const void	*name = key;
	__u16		size = strlen((char *)name) + 1;

	return obe_update_pack(update, max_update_size, OUT_INDEX_LOOKUP,
			       fid, 1, &size, &name);
}

int obe_attr_get_pack(struct object_update *update,
		      size_t *max_update_size, const struct lu_fid *fid)
{
	return obe_update_pack(update, max_update_size, OUT_ATTR_GET,
			       fid, 0, NULL, NULL);
}

int obe_xattr_get_pack(struct object_update *update,
		       size_t *max_update_size, const struct lu_fid *fid,
		       const char *name)
{
	__u16 size = strlen(name) + 1;

	return obe_update_pack(update, max_update_size, OUT_XATTR_GET,
			       fid, 1, &size, (const void **)&name);
}

int obe_read_pack(struct object_update *update,
		  size_t *max_update_size, const struct lu_fid *fid,
		  size_t size, loff_t pos)
{
	__u16		sizes[2] = {sizeof(size), sizeof(pos)};
	const void	*bufs[2] = {&size, &pos};

	size = cpu_to_le64(size);
	pos = cpu_to_le64(pos);

	return obe_update_pack(update, max_update_size, OUT_READ, fid,
			       ARRAY_SIZE(sizes), sizes, bufs);
}

int obe_index_bin_insert_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_rec *rec, const int reclen,
			  const struct dt_key *key, const int keylen)
{
	__u16			    sizes[3] = { keylen, reclen };
	const void		   *bufs[3] = { (char *)key,
						(char *)rec };

	return obe_update_pack(update, max_update_size,
			       OUT_INDEX_BIN_INSERT, fid, ARRAY_SIZE(sizes),
			       sizes, bufs);
}

int obe_index_bin_delete_pack(struct object_update *update,
			  size_t *max_update_size, const struct lu_fid *fid,
			  const struct dt_key *key, const int keylen)
{
	__u16	size = keylen;
	const void *buf = key;

	return obe_update_pack(update, max_update_size,
			       OUT_INDEX_BIN_DELETE, fid, 1, &size, &buf);
}

struct obe_request *obe_new(int device)
{
	struct obe_request *our;
	struct object_update_request *req;

	our = malloc(sizeof(*our));
	if (our == NULL)
		return NULL;
	req = malloc(64 * 1024);
	if (req == NULL) {
		free(our);
		return NULL;
	}

	req->ourq_magic = UPDATE_REQUEST_MAGIC;
	req->ourq_count = 0;
	our->our_size = 64 * 1024;
	our->our_req = req;
	our->our_dev = device;
	our->our_rc = 0;

	return our;
}

void obe_free(struct obe_request *our)
{
	if (our == NULL)
		return;
	free(our->our_req);
	free(our);
}

int obe_extend_buffer(struct obe_request *our)
{
	size_t new_size;
	char *newbuf;
	int rc;

	/* enlarge object update request size */
	new_size = our->our_size + (16 * 1024);
	if (new_size > 128*1024)
		return -E2BIG;

	newbuf = malloc(new_size);
	if (newbuf == NULL)
		return -ENOMEM;

	memcpy(newbuf, our->our_req, our->our_size);
	free(our->our_req);
	our->our_req = (struct object_update_request *)newbuf;
	our->our_size = new_size;

	return 0;
}

struct object_update *obe_buffer_get_update(struct object_update_request *r,
					       unsigned int index)
{
	void	*ptr;
	int	i;

	if (index > r->ourq_count)
		return NULL;

	ptr = &r->ourq_updates[0];
	for (i = 0; i < index; i++)
		ptr += object_update_size(ptr);

	return ptr;
}

int obe_exec(struct obe_request *our)
{
	char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
	struct obd_ioctl_data data;
	int rc;

	if (our->our_rc != 0)
		return rc;

	memset(&data, 0, sizeof(data));
	data.ioc_dev = our->our_dev;
	data.ioc_pbuf1 = (char *)our->our_req;
	data.ioc_plen1 = our->our_size;

	memset(buf, 0, sizeof(data));

	rc = obd_ioctl_pack(&data, &buf, sizeof(rawbuf));
	if (rc) {
		fprintf(stderr, "Fail to pack ioctl data: rc = %d.\n", rc);
		return rc;
	}

	rc = l_ioctl(OBD_DEV_ID, OBD_IOC_OSDBENCH, buf);

	obe_free(our);

	return rc;
}
