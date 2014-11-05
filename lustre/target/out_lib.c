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
 * Copyright (c) 2014, Intel Corporation.
 */
/*
 * lustre/target/out_lib.c
 *
 * Author: Di Wang <di.wang@intel.com>
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <lu_target.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>

#define OUT_UPDATE_BUFFER_SIZE_ADD	4096
#define OUT_UPDATE_BUFFER_SIZE_MAX	(256 * 4096)  /* 1MB update size now */
/**
 * resize update buffer
 *
 * Extend the update buffer by new_size.
 *
 * \param[in] ubuf	update buffer to be extended
 * \param[in] new_size  new size of the update buffer
 *
 * \retval		0 if extending succeeds.
 * \retval		negative errno if extending fails.
 */
static int update_buffer_resize(struct update_buffer *ubuf, size_t new_size)
{
	struct object_update_request *ureq;

	if (new_size < ubuf->ub_req_size)
		return 0;

	OBD_ALLOC_LARGE(ureq, new_size);
	if (ureq == NULL)
		return -ENOMEM;

	memcpy(ureq, ubuf->ub_req, ubuf->ub_req_size);

	OBD_FREE_LARGE(ubuf->ub_req, ubuf->ub_req_size);

	ubuf->ub_req = ureq;
	ubuf->ub_req_size = new_size;

	return 0;
}

/**
 * Pack the header of object_update_request
 *
 * Packs updates into the update_buffer header, which will either be sent to
 * the remote MDT or stored in the local update log. The maximum update buffer
 * size is 1MB for now.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	update bufer which it will pack the update in
 * \param[in] op	update operation
 * \param[in] fid	object FID for this update
 * \param[in] param_count	parameters count for this update
 * \param[in] lens	each parameters length of this update
 * \param[in] batchid	batchid(transaction no) of this update
 *
 * \retval		0 pack update succeed.
 * \retval              negative errno pack update failed.
 **/
static struct object_update *
out_update_header_pack(const struct lu_env *env, struct update_buffer *ubuf,
		       enum update_type op, const struct lu_fid *fid,
		       int params_count, __u16 *param_sizes, __u64 batchid)
{
	struct object_update_request	*ureq = ubuf->ub_req;
	size_t				ureq_size = ubuf->ub_req_size;
	struct object_update		*obj_update;
	struct object_update_param	*param;
	size_t				update_size;
	int				rc = 0;
	unsigned int			i;
	ENTRY;

	/* Check update size to make sure it can fit into the buffer */
	ureq_size = object_update_request_size(ureq);
	update_size = offsetof(struct object_update, ou_params[0]);
	for (i = 0; i < params_count; i++)
		update_size += cfs_size_round(param_sizes[i] + sizeof(*param));

	if (unlikely(cfs_size_round(ureq_size + update_size) >
		     ubuf->ub_req_size)) {
		size_t new_size = ubuf->ub_req_size;

		/* enlarge object update request size */
		while (new_size <
		       cfs_size_round(ureq_size + update_size))
			new_size += OUT_UPDATE_BUFFER_SIZE_ADD;
		if (new_size >= OUT_UPDATE_BUFFER_SIZE_MAX)
			RETURN(ERR_PTR(-E2BIG));

		rc = update_buffer_resize(ubuf, new_size);
		if (rc < 0)
			RETURN(ERR_PTR(rc));

		ureq = ubuf->ub_req;
	}

	/* fill the update into the update buffer */
	obj_update = (struct object_update *)((char *)ureq + ureq_size);
	obj_update->ou_fid = *fid;
	obj_update->ou_type = op;
	obj_update->ou_params_count = (__u16)params_count;
	obj_update->ou_batchid = batchid;
	param = &obj_update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		param->oup_len = param_sizes[i];
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	CDEBUG(D_INFO, "%p "DFID" idx %u: op %d params %d:%d\n",
	       ureq, PFID(fid), ureq->ourq_count, op, params_count,
	       (int)update_size);
	ureq->ourq_count++;

	RETURN(obj_update);
}

/**
 * Packs one update into the update_buffer.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	bufer where update will be packed
 * \param[in] op	update operation (enum update_type)
 * \param[in] fid	object FID for this update
 * \param[in] param_count	number of parameters for this update
 * \param[in] param_sizes	array of parameters length of this update
 * \param[in] param_bufs	parameter buffers
 * \param[in] batchid	transaction no of this update, plus mdt_index, which
 *                      will be globally unique
 *
 * \retval		= 0 if updates packing succeeds
 * \retval		negative errno if updates packing fails
 **/
int out_update_pack(const struct lu_env *env, struct update_buffer *ubuf,
		    enum update_type op, const struct lu_fid *fid,
		    int params_count, __u16 *param_sizes,
		    const void **param_bufs, __u64 batchid)
{
	struct object_update		*update;
	struct object_update_param	*param;
	unsigned int			i;
	ENTRY;

	LASSERT(batchid > 0);
	update = out_update_header_pack(env, ubuf, op, fid, params_count,
					param_sizes, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	param = &update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		memcpy(&param->oup_buf[0], param_bufs[i], param_sizes[i]);
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	RETURN(0);
}
EXPORT_SYMBOL(out_update_pack);

static int out_calc_attr_size(const struct lu_attr *attr)
{
	int size;

	LASSERT(attr->la_valid != 0);
	size = sizeof(attr->la_valid);
	if (attr->la_valid & LA_ATIME)
		size += sizeof(attr->la_atime);
	if (attr->la_valid & LA_MTIME)
		size += sizeof(attr->la_mtime);
	if (attr->la_valid & LA_CTIME)
		size += sizeof(attr->la_ctime);
	if (attr->la_valid & LA_SIZE)
		size += sizeof(attr->la_size);
	if (attr->la_valid & LA_MODE)
		size += sizeof(attr->la_mode);
	if (attr->la_valid & LA_UID)
		size += sizeof(attr->la_uid);
	if (attr->la_valid & LA_GID)
		size += sizeof(attr->la_gid);
	if (attr->la_valid & LA_BLOCKS)
		size += sizeof(attr->la_blocks);
	if (attr->la_valid & LA_FLAGS)
		size += sizeof(attr->la_flags);
	if (attr->la_valid & LA_NLINK)
		size += sizeof(attr->la_nlink);
	if (attr->la_valid & LA_RDEV)
		size += sizeof(attr->la_rdev);
	if (attr->la_valid & LA_BLKSIZE)
		size += sizeof(attr->la_blksize);

	return size;
}

#define CHECK_AND_MOVE(ATTRIBUTE, FIELD)			\
do {								\
	if (attr->la_valid & ATTRIBUTE) {			\
		memcpy(ptr, &attr->FIELD, sizeof(attr->FIELD));	\
		ptr += sizeof(attr->FIELD);			\
	}							\
} while (0)

static void out_pack_lu_attr(const struct lu_attr *attr, void *ptr)
{
	CHECK_AND_MOVE(0xffffffff, la_valid);
	CHECK_AND_MOVE(LA_SIZE, la_size);
	CHECK_AND_MOVE(LA_MTIME, la_mtime);
	CHECK_AND_MOVE(LA_ATIME, la_atime);
	CHECK_AND_MOVE(LA_CTIME, la_ctime);
	CHECK_AND_MOVE(LA_BLOCKS, la_blocks);
	CHECK_AND_MOVE((LA_MODE | LA_TYPE), la_mode);
	CHECK_AND_MOVE(LA_UID, la_uid);
	CHECK_AND_MOVE(LA_GID, la_gid);
	CHECK_AND_MOVE(LA_FLAGS, la_flags);
	CHECK_AND_MOVE(LA_NLINK, la_nlink);
	/*CHECK_AND_MOVE(LA_BLKBITS, la_blkbits);*/
	CHECK_AND_MOVE(LA_BLKSIZE, la_blksize);
	CHECK_AND_MOVE(LA_RDEV, la_rdev);
}
#undef CHECK_AND_MOVE

/**
 * Pack various updates into the update_buffer.
 *
 * The following functions pack different updates into the update_buffer
 * So parameters of these API is basically same as its correspondent OSD/OSP
 * API, for detail description of these parameters see osd_handler.c or
 * osp_md_object.c.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	update buffer
 * \param[in] fid	fid of this object for the update
 * \param[in] batchid	batch id of this update
 *
 * \retval		0 if insertion succeeds.
 * \retval		negative errno if insertion fails.
 */
int out_create_pack(const struct lu_env *env, struct update_buffer *ubuf,
		    const struct lu_fid *fid, struct lu_attr *attr,
		    struct dt_allocation_hint *hint,
		    struct dt_object_format *dof, __u64 batchid)
{
	__u16			sizes[2] = {0, 0};
	int			buf_count = 1;
	const struct lu_fid	*fid1 = NULL;
	struct object_update	*update;
	void			*ptr;
	ENTRY;

	sizes[0] = out_calc_attr_size(attr);

	if (hint != NULL && hint->dah_parent) {
		fid1 = lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	update = out_update_header_pack(env, ubuf, OUT_CREATE, fid,
					buf_count, sizes, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	ptr = object_update_param_get(update, 0, NULL);
	out_pack_lu_attr(attr, ptr);

	if (fid1 != NULL) {
		struct lu_fid *fid;
		fid = object_update_param_get(update, 1, NULL);
		fid_cpu_to_le(fid, fid1);
	}

	RETURN(0);
}
EXPORT_SYMBOL(out_create_pack);

int out_ref_del_pack(const struct lu_env *env, struct update_buffer *ubuf,
		     const struct lu_fid *fid, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_REF_DEL, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_ref_del_pack);

int out_ref_add_pack(const struct lu_env *env, struct update_buffer *ubuf,
		     const struct lu_fid *fid, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_REF_ADD, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_ref_add_pack);

int out_attr_set_pack(const struct lu_env *env, struct update_buffer *ubuf,
		      const struct lu_fid *fid, const struct lu_attr *attr,
		      __u64 batchid)
{
	struct object_update	*update;
	__u16			size;
	void			*ptr;
	ENTRY;

	size = out_calc_attr_size(attr);

	update = out_update_header_pack(env, ubuf, OUT_ATTR_SET, fid, 1,
					&size, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	ptr = object_update_param_get(update, 0, NULL);
	out_pack_lu_attr(attr, ptr);

	RETURN(0);
}
EXPORT_SYMBOL(out_attr_set_pack);

int out_xattr_set_pack(const struct lu_env *env, struct update_buffer *ubuf,
		       const struct lu_fid *fid, const struct lu_buf *buf,
		       const char *name, int flag, __u64 batchid)
{
	__u16	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(flag)};
	const void *bufs[3] = {(char *)name, (char *)buf->lb_buf,
			       (char *)&flag};
	unsigned char type;

	if (strcmp(name, XATTR_NAME_LOV) == 0) {
		type = 1;
		sizes[0] = 1;
		bufs[0] = &type;
	} else if (strcmp(name, XATTR_NAME_LINK) == 0) {
		type = 2;
		sizes[0] = 1;
		bufs[0] = &type;
	} else if (strcmp(name, XATTR_NAME_VERSION) == 0) {
		type = 3;
		sizes[0] = 1;
		bufs[0] = &type;
	}

	return out_update_pack(env, ubuf, OUT_XATTR_SET, fid,
			       flag == 0 ? ARRAY_SIZE(sizes) - 1 :
			       ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_xattr_set_pack);

int out_xattr_del_pack(const struct lu_env *env, struct update_buffer *ubuf,
		       const struct lu_fid *fid, const char *name,
		       __u64 batchid)
{
	__u16	size = strlen(name) + 1;

	return out_update_pack(env, ubuf, OUT_XATTR_DEL, fid, 1, &size,
			       (const void **)&name, batchid);
}
EXPORT_SYMBOL(out_xattr_del_pack);


int out_index_insert_pack(const struct lu_env *env, struct update_buffer *ubuf,
			  const struct lu_fid *fid, const struct dt_rec *rec,
			  const struct dt_key *key, __u64 batchid)
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

	return out_update_pack(env, ubuf, OUT_INDEX_INSERT, fid,
			       ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_index_insert_pack);

int out_index_delete_pack(const struct lu_env *env, struct update_buffer *ubuf,
			  const struct lu_fid *fid, const struct dt_key *key,
			  __u64 batchid)
{
	__u16	size = strlen((char *)key) + 1;
	const void *buf = key;

	return out_update_pack(env, ubuf, OUT_INDEX_DELETE, fid, 1, &size,
			       &buf, batchid);
}
EXPORT_SYMBOL(out_index_delete_pack);

int out_object_destroy_pack(const struct lu_env *env,
			    struct update_buffer *ubuf,
			    const struct lu_fid *fid, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_DESTROY, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_object_destroy_pack);

int out_write_pack(const struct lu_env *env, struct update_buffer *ubuf,
		   const struct lu_fid *fid, const struct lu_buf *buf,
		   loff_t pos, __u64 batchid)
{
	__u16		sizes[2] = {buf->lb_len, sizeof(pos)};
	const void	*bufs[2] = {(char *)buf->lb_buf, (char *)&pos};
	int		rc;

	pos = cpu_to_le64(pos);

	rc = out_update_pack(env, ubuf, OUT_WRITE, fid, ARRAY_SIZE(sizes),
			     sizes, bufs, batchid);
	return rc;
}
EXPORT_SYMBOL(out_write_pack);

int out_punch_pack(const struct lu_env *env, struct update_buffer *ubuf,
		   const struct lu_fid *fid, loff_t start, loff_t end,
		   __u64 batchid)
{
	__u16		sizes[2] = {sizeof(start), sizeof(end)};
	const void	*bufs[2] = {(char *)&start, (char *)&end};
	int		rc;

	start = cpu_to_le64(start);
	end = cpu_to_le64(end);

	rc = out_update_pack(env, ubuf, OUT_PUNCH, fid, ARRAY_SIZE(sizes),
			     sizes, bufs, batchid);
	return rc;
}
EXPORT_SYMBOL(out_punch_pack);

/**
 * Pack various readonly updates into the update_buffer.
 *
 * The following update funcs are only used by read-only ops, lookup,
 * getattr etc, so it does not need transaction here. Currently they
 * are only used by OSP.
 *
 * \param[in] env	execution environment
 * \param[in] fid	fid of this object for the update
 * \param[in] ubuf	update buffer
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int out_index_lookup_pack(const struct lu_env *env, struct update_buffer *ubuf,
			  const struct lu_fid *fid, struct dt_rec *rec,
			  const struct dt_key *key)
{
	const void	*name = key;
	__u16		size = strlen((char *)name) + 1;

	return out_update_pack(env, ubuf, OUT_INDEX_LOOKUP, fid, 1, &size,
			       &name, 0);
}
EXPORT_SYMBOL(out_index_lookup_pack);

int out_attr_get_pack(const struct lu_env *env, struct update_buffer *ubuf,
		      const struct lu_fid *fid)
{
	return out_update_pack(env, ubuf, OUT_ATTR_GET, fid, 0, NULL, NULL, 0);
}
EXPORT_SYMBOL(out_attr_get_pack);

int out_xattr_get_pack(const struct lu_env *env, struct update_buffer *ubuf,
		       const struct lu_fid *fid, const char *name)
{
	__u16 size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;
	return out_update_pack(env, ubuf, OUT_XATTR_GET, fid, 1, &size,
			       (const void **)&name, 0);
}
EXPORT_SYMBOL(out_xattr_get_pack);
