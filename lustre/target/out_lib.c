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
 * Copyright (c) 2013, Intel Corporation.
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

struct update_opcode {
	__u16		opcode;
	const char	*opname;
} update_opcode_table[OUT_LAST] = {
	{ OUT_START,            "start" },
	{ OUT_CREATE,           "create" },
	{ OUT_DESTROY,          "destroy" },
	{ OUT_REF_ADD,          "ref_add" },
	{ OUT_REF_DEL,          "ref_del" },
	{ OUT_ATTR_SET,         "attr_set" },
	{ OUT_ATTR_GET,         "attr_get" },
	{ OUT_XATTR_SET,        "xattr_set" },
	{ OUT_XATTR_GET,        "xattr_get" },
	{ OUT_INDEX_LOOKUP,     "lookup" },
	{ OUT_INDEX_INSERT,     "insert" },
	{ OUT_INDEX_DELETE,     "delete" },
	{ OUT_WRITE,		"write" },
	{ OUT_XATTR_DEL,	"xattr_del" },
	{ OUT_READ,		"read" },
};

const char *update_op_str(__u16 opcode)
{
	LASSERTF(update_opcode_table[opcode].opcode == opcode, "%d",
		(int)opcode);
	return update_opcode_table[opcode].opname;
}

void object_update_request_dump(const struct object_update_request *ourq,
				__u32 umask)
{
	unsigned int i;
	size_t total_size = 0;

	for (i = 0; i < ourq->ourq_count; i++) {
		struct object_update	*update;
		size_t			size = 0;

		update = object_update_request_get(ourq, i, &size);
		CDEBUG(umask, "i: %u fid: "DFID" op: %s master: %d params %d"
		       "batchid: "LPU64" size %d\n", i, PFID(&update->ou_fid),
		       update_op_str(update->ou_type),
		       (int)update->ou_master_index, update->ou_params_count,
		       update->ou_batchid, (int)size);

		total_size += size;
	}

	CDEBUG(umask, "updates %p magic %x count %d size %d\n", ourq,
	       ourq->ourq_magic, ourq->ourq_count, (int)total_size);
}
EXPORT_SYMBOL(object_update_request_dump);

struct object_update_request *object_update_request_alloc(size_t size)
{
	struct object_update_request *ourq;

	OBD_ALLOC_LARGE(ourq, size);
	if (ourq == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	ourq->ourq_magic = UPDATE_REQUEST_MAGIC;
	ourq->ourq_count = 0;

	RETURN(ourq);
}
EXPORT_SYMBOL(object_update_request_alloc);

void object_update_request_free(struct object_update_request *ourq,
				size_t ourq_size)
{
	if (ourq != NULL)
		OBD_FREE_LARGE(ourq, ourq_size);
}
EXPORT_SYMBOL(object_update_request_free);

/**
 * Create dt_update_request
 *
 * dt_update_request is being used to track updates being executed on
 * this dt_device(OSD or OSP). The update buffer will be 8k initially,
 * and increased if needed.
 *
 * \param [in] dt	dt device
 *
 * \retval		dt_update_request being allocated if succeed
 *                      ERR_PTR(errno) if failed
 */
struct dt_update_request *dt_update_request_create(struct dt_device *dt)
{
	struct dt_update_request *dt_update;
	struct object_update_request *ourq;

	OBD_ALLOC_PTR(dt_update);
	if (!dt_update)
		return ERR_PTR(-ENOMEM);

	ourq = object_update_request_alloc(OUT_UPDATE_INIT_BUFFER_SIZE);
	if (IS_ERR(ourq)) {
		OBD_FREE_PTR(dt_update);
		return ERR_CAST(ourq);
	}

	dt_update->dur_buf.ub_req = ourq;
	dt_update->dur_buf.ub_req_size = OUT_UPDATE_INIT_BUFFER_SIZE;

	dt_update->dur_dt = dt;
	dt_update->dur_batchid = 0;
	INIT_LIST_HEAD(&dt_update->dur_cb_items);

	return dt_update;
}
EXPORT_SYMBOL(dt_update_request_create);

/**
 * Destroy dt_update_request
 *
 * \param [in] dt_update	dt_update_request being destroyed
 */
void dt_update_request_destroy(struct dt_update_request *dt_update)
{
	if (dt_update == NULL)
		return;

	object_update_request_free(dt_update->dur_buf.ub_req,
				   dt_update->dur_buf.ub_req_size);
	OBD_FREE_PTR(dt_update);

	return;
}
EXPORT_SYMBOL(dt_update_request_destroy);

int out_prep_update_req(const struct lu_env *env, struct obd_import *imp,
			const struct object_update_request *ureq,
			struct ptlrpc_request **reqp)
{
	struct ptlrpc_request		*req;
	struct object_update_request	*tmp;
	int				ureq_len;
	int				rc;
	ENTRY;

	req = ptlrpc_request_alloc(imp, &RQF_OUT_UPDATE);
	if (req == NULL)
		RETURN(-ENOMEM);

	object_update_request_dump(ureq, D_INFO);
	ureq_len = object_update_request_size(ureq);
	req_capsule_set_size(&req->rq_pill, &RMF_OUT_UPDATE, RCL_CLIENT,
			     ureq_len);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, OUT_UPDATE);
	if (rc != 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	req_capsule_set_size(&req->rq_pill, &RMF_OUT_UPDATE_REPLY,
			     RCL_SERVER, OUT_UPDATE_REPLY_SIZE);

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_OUT_UPDATE);
	memcpy(tmp, ureq, ureq_len);

	ptlrpc_request_set_replen(req);
	req->rq_request_portal = OUT_PORTAL;
	req->rq_reply_portal = OSC_REPLY_PORTAL;
	*reqp = req;

	RETURN(rc);
}
EXPORT_SYMBOL(out_prep_update_req);

int out_remote_sync(const struct lu_env *env, struct obd_import *imp,
		    struct dt_update_request *dt_update,
		    struct ptlrpc_request **reqp)
{
	struct ptlrpc_request	*req = NULL;
	int			rc;
	ENTRY;

	rc = out_prep_update_req(env, imp, dt_update->dur_buf.ub_req, &req);
	if (rc != 0)
		RETURN(rc);

	/* Note: some dt index api might return non-zero result here, like
	 * osd_index_ea_lookup, so we should only check rc < 0 here */
	rc = ptlrpc_queue_wait(req);
	if (rc < 0) {
		ptlrpc_req_finished(req);
		dt_update->dur_rc = rc;
		RETURN(rc);
	}

	if (reqp != NULL) {
		*reqp = req;
		RETURN(rc);
	}

	dt_update->dur_rc = rc;

	ptlrpc_req_finished(req);

	RETURN(rc);
}
EXPORT_SYMBOL(out_remote_sync);

/**
 * Fill object update header
 *
 * Only fill the object update header, and parameters will be filled later
 * in other functions.
 *
 * \params[in] env:		execution environment
 * \params[in] update:		object update to be filled
 * \params[in] max_update_size: maxima object update length, if the current
 *                              update length exceeds this number, it will
 *                              return -E2BIG.
 * \params[in] update_op:	update type
 * \params[in] fid:		object FID of the update
 * \params[in] params_count:	the count of the update parameters
 * \params[in] params_sizes:	the length of each parameters
 * \params[in] batchid:		batch id of the update
 *
 * \retval			0 if packing succeeds.
 * \retval			-E2BIG if packing exceeds the maxima length.
 */
int out_update_header_pack(const struct lu_env *env,
			   struct object_update *update, size_t max_update_size,
			   enum update_type update_op, const struct lu_fid *fid,
			   int params_count, __u16 *params_sizes, __u64 batchid)
{
	struct object_update_param	*param;
	unsigned int			i;
	size_t				update_size;

	/* Check whether the packing exceeding the maxima update length */
	update_size = sizeof(struct object_update);
	for (i = 0; i < params_count; i++)
		update_size += cfs_size_round(sizeof(*param) + params_sizes[i]);

	if (unlikely(update_size >= max_update_size))
		return -E2BIG;

	update->ou_fid = *fid;
	update->ou_type = (__u16)update_op;
	update->ou_params_count = (__u16)params_count;
	param = &update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		param->oup_len = params_sizes[i];
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	return 0;
}

/**
 * Packs one update into the update_buffer.
 *
 * \param[in] env	execution environment
 * \param[in] update	update to be packed
 * \param[in] max_update_length	maxima length of \a update
 * \param[in] op	update operation (enum update_type)
 * \param[in] fid	object FID for this update
 * \param[in] param_count	parameters count for this update
 * \param[in] param_sizes	each parameters length of this update
 * \param[in] param_bufs	parameter buffers
 * \param[in] batchid	batchid(transaction no) of this update
 *
 * \retval		= 0 if updates packing succeed
 *                      negative errno if updates packing failed
 **/
int out_update_pack(const struct lu_env *env, struct object_update *update,
		    size_t max_update_size, enum update_type op,
		    const struct lu_fid *fid, int params_count,
		    __u16 *param_sizes, const void **param_bufs,
		    __u64 batchid)
{
	struct object_update_param	*param;
	unsigned int			i;
	int				rc;
	ENTRY;

	rc = out_update_header_pack(env, update, max_update_size, op, fid,
				    params_count, param_sizes, batchid);
	if (rc != 0)
		RETURN(rc);

	param = &update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		memcpy(&param->oup_buf[0], param_bufs[i], param_sizes[i]);
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	RETURN(0);
}
EXPORT_SYMBOL(out_update_pack);

/**
 * Pack various updates into the update_buffer.
 * The following functions pack different updates into the update_buffer
 * So parameters of these API is basically same as its correspondent OSD/OSP
 * API, for detail description of these parameters see osd_handler.c or
 * osp_md_object.c.
 *
 * param[in] env	execution environment
 * param[in] ubuf	update buffer
 * param[in] fid	fid of this object for the update
 * param[in] batchid	batch id of this update
 *
 * retval		= 0 if insertion succeed
 *                      negative errno if insertion failed
 */
int out_create_pack(const struct lu_env *env, struct object_update *update,
		    size_t max_update_size, const struct lu_fid *fid,
		    const struct lu_attr *attr, struct dt_allocation_hint *hint,
		    struct dt_object_format *dof, __u64 batchid)
{
	struct obdo		*obdo;
	__u16			sizes[2] = {sizeof(*obdo), 0};
	int			buf_count = 1;
	const struct lu_fid	*fid1 = NULL;
	int			rc;
	ENTRY;

	if (hint != NULL && hint->dah_parent) {
		fid1 = lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	rc = out_update_header_pack(env, update, max_update_size, OUT_CREATE,
				    fid, buf_count, sizes, batchid);
	if (rc != 0)
		RETURN(rc);

	obdo = object_update_param_get(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	if (fid1 != NULL) {
		struct lu_fid *fid;

		fid = object_update_param_get(update, 1, NULL);
		fid_cpu_to_le(fid, fid1);
	}

	RETURN(0);
}
EXPORT_SYMBOL(out_create_pack);

int out_ref_del_pack(const struct lu_env *env, struct object_update *update,
		     size_t max_update_length, const struct lu_fid *fid,
		     __u64 batchid)
{
	return out_update_pack(env, update, max_update_length, OUT_REF_DEL, fid,
			       0, NULL, NULL, batchid);
}
EXPORT_SYMBOL(out_ref_del_pack);

int out_ref_add_pack(const struct lu_env *env, struct object_update *update,
		     size_t max_update_length, const struct lu_fid *fid,
		     __u64 batchid)
{
	return out_update_pack(env, update, max_update_length, OUT_REF_ADD, fid,
			       0, NULL, NULL, batchid);
}
EXPORT_SYMBOL(out_ref_add_pack);

int out_attr_set_pack(const struct lu_env *env, struct object_update *update,
		      size_t max_update_length, const struct lu_fid *fid,
		      const struct lu_attr *attr, __u64 batchid)
{
	struct obdo		*obdo;
	__u16			size = sizeof(*obdo);
	int			rc;
	ENTRY;

	rc = out_update_header_pack(env, update, max_update_length,
				    OUT_ATTR_SET, fid, 1, &size, batchid);
	if (rc != 0)
		RETURN(rc);

	obdo = object_update_param_get(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	RETURN(0);
}
EXPORT_SYMBOL(out_attr_set_pack);

int out_xattr_set_pack(const struct lu_env *env, struct object_update *update,
		       size_t max_update_length, const struct lu_fid *fid,
		       const struct lu_buf *buf, const char *name, int flag,
		       __u64 batchid)
{
	__u16	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(flag)};
	const void *bufs[3] = {(char *)name, (char *)buf->lb_buf,
			       (char *)&flag};

	return out_update_pack(env, update, max_update_length, OUT_XATTR_SET,
			       fid, ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_xattr_set_pack);

int out_xattr_del_pack(const struct lu_env *env, struct object_update *update,
		       size_t max_update_length, const struct lu_fid *fid,
		       const char *name, __u64 batchid)
{
	__u16	size = strlen(name) + 1;

	return out_update_pack(env, update, max_update_length, OUT_XATTR_DEL,
			       fid, 1, &size, (const void **)&name, batchid);
}
EXPORT_SYMBOL(out_xattr_del_pack);


int out_index_insert_pack(const struct lu_env *env,
			  struct object_update *update,
			  size_t max_update_length, const struct lu_fid *fid,
			  const struct dt_rec *rec, const struct dt_key *key,
			  __u64 batchid)
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

	return out_update_pack(env, update, max_update_length, OUT_INDEX_INSERT,
			       fid, ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_index_insert_pack);

int out_index_delete_pack(const struct lu_env *env,
			  struct object_update *update,
			  size_t max_update_length, const struct lu_fid *fid,
			  const struct dt_key *key, __u64 batchid)
{
	__u16	size = strlen((char *)key) + 1;
	const void *buf = key;

	return out_update_pack(env, update, max_update_length, OUT_INDEX_DELETE,
			       fid, 1, &size, &buf, batchid);
}
EXPORT_SYMBOL(out_index_delete_pack);

int out_object_destroy_pack(const struct lu_env *env,
			    struct object_update *update,
			    size_t max_update_length, const struct lu_fid *fid,
			    __u16 cookie_size, const void *cookie,
			    __u64 batchid)
{
	return out_update_pack(env, update, max_update_length, OUT_DESTROY, fid,
			       1, &cookie_size, &cookie, batchid);
}
EXPORT_SYMBOL(out_object_destroy_pack);

int out_write_pack(const struct lu_env *env, struct object_update *update,
		   size_t max_update_length, const struct lu_fid *fid,
		   const struct lu_buf *buf, loff_t pos, __u64 batchid)
{
	__u16		sizes[2] = {buf->lb_len, sizeof(pos)};
	const void	*bufs[2] = {(char *)buf->lb_buf, (char *)&pos};
	int		rc;

	pos = cpu_to_le64(pos);

	rc = out_update_pack(env, update, max_update_length, OUT_WRITE, fid,
			     ARRAY_SIZE(sizes), sizes, bufs, batchid);
	return rc;
}
EXPORT_SYMBOL(out_write_pack);

/**
 * The following update funcs are only used by read-only ops, lookup,
 * getattr etc, so it does not need transaction here. Currently they
 * are only used by OSP.
 *
 * \param[in] env	execution environment
 * \param[in] fid	fid of this object for the update
 * \param[in] ubuf	update buffer
 *
 * \retval		= 0 pack succeed.
 *                      < 0 pack failed.
 **/
int out_index_lookup_pack(const struct lu_env *env,
			  struct object_update *update,
			  size_t max_update_length, const struct lu_fid *fid,
			  struct dt_rec *rec, const struct dt_key *key)
{
	const void	*name = key;
	__u16		size = strlen((char *)name) + 1;

	return out_update_pack(env, update, max_update_length, OUT_INDEX_LOOKUP,
			       fid, 1, &size, &name, 0);
}
EXPORT_SYMBOL(out_index_lookup_pack);

int out_attr_get_pack(const struct lu_env *env, struct object_update *update,
		      size_t max_update_length, const struct lu_fid *fid)
{
	return out_update_pack(env, update, max_update_length, OUT_ATTR_GET,
			       fid, 0, NULL, NULL, 0);
}
EXPORT_SYMBOL(out_attr_get_pack);

int out_xattr_get_pack(const struct lu_env *env, struct object_update *update,
		       size_t max_update_length, const struct lu_fid *fid,
		       const char *name)
{
	__u16 size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;

	return out_update_pack(env, update, max_update_length, OUT_XATTR_GET,
			       fid, 1, &size, (const void **)&name, 0);
}
EXPORT_SYMBOL(out_xattr_get_pack);

int out_read_pack(const struct lu_env *env, struct object_update *update,
		  int max_update_length, const struct lu_fid *fid,
		  ssize_t size, loff_t pos)
{
	__u16		sizes[2] = {sizeof(size), sizeof(pos)};
	const void	*bufs[2] = {&size, &pos};

	size = cpu_to_le64(size);
	pos = cpu_to_le64(pos);

	return out_update_pack(env, update, max_update_length, OUT_READ, fid,
			       ARRAY_SIZE(sizes), sizes, bufs, 0);
}
EXPORT_SYMBOL(out_read_pack);

