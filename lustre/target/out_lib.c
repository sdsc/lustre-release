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

static struct object_update_request *object_update_request_alloc(int size)
{
	struct object_update_request *ourq;

	OBD_ALLOC_LARGE(ourq, size);
	if (ourq == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	ourq->ourq_magic = UPDATE_REQUEST_MAGIC;
	ourq->ourq_count = 0;

	RETURN(ourq);
}

static void object_update_request_free(struct object_update_request *ourq,
				       int ourq_length)
{
	if (ourq != NULL)
		OBD_FREE_LARGE(ourq, ourq_length);
}

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
struct dt_update_request *out_create_dt_update_req(struct dt_device *dt)
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
	dt_update->dur_buf.ub_req_len = OUT_UPDATE_INIT_BUFFER_SIZE;

	dt_update->dur_dt = dt;
	dt_update->dur_batchid = 0;
	INIT_LIST_HEAD(&dt_update->dur_cb_items);

	return dt_update;
}
EXPORT_SYMBOL(out_create_dt_update_req);

/**
 * Destroy dt_update_request
 *
 * \param [in] dt_update	dt_update_request being destroyed
 */
void out_destroy_dt_update_req(struct dt_update_request *dt_update)
{
	if (dt_update == NULL)
		return;

	object_update_request_free(dt_update->dur_buf.ub_req,
				   dt_update->dur_buf.ub_req_len);
	OBD_FREE_PTR(dt_update);

	return;
}
EXPORT_SYMBOL(out_destroy_dt_update_req);

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

static int update_buffer_resize(struct update_buffer *ubuf, int new_size)
{
	struct object_update_request *ureq;

	LASSERT(new_size > ubuf->ub_req_len);

	CDEBUG(D_INFO, "resize update from %d to %d\n", ubuf->ub_req_len,
	       new_size);

	OBD_ALLOC_LARGE(ureq, new_size);
	if (ureq == NULL)
		return -ENOMEM;

	memcpy(ureq, ubuf->ub_req, ubuf->ub_req_len);

	OBD_FREE_LARGE(ubuf->ub_req, ubuf->ub_req_len);

	ubuf->ub_req = ureq;
	ubuf->ub_req_len = new_size;

	return 0;
}

#define OUT_UPDATE_BUFFER_SIZE_ADD	4096
#define OUT_UPDATE_BUFFER_SIZE_MAX	(256 * 4096)  /*  1M update size now */

/**
 * Pack the header of object_update_request
 *
 * Packs updates into the update_buffer header, which will be either sent to
 * the remote MDT or stored in the update log. The maximum update buffer size
 * is 1M for now.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	update bufer which it will pack the update in.
 * \param[in] op	update operation.
 * \param[in] fid	object FID for this update.
 * \param[in] param_count	parameters count for this update.
 * \param[in] lens	each parameters length of this update.
 * \param[in] batchid	batchid(transaction no) of this update.
 *
 * \retval		= 0 pack update succeed.
 * 			< 0 pack update failed.
 **/
static struct object_update*
out_update_header_pack(const struct lu_env *env, struct update_buffer *ubuf,
		       int op, const struct lu_fid *fid, int params_count,
		       int *lens, __u64 batchid)
{
	struct object_update_request	*ureq = ubuf->ub_req;
	int				ureq_len = ubuf->ub_req_len;
	struct object_update		*obj_update;
	struct object_update_param	*param;
	int				update_length;
	int				rc = 0;
	int				i;
	ENTRY;

	/* Check update size to make sure it can fit into the buffer */
	ureq_len = object_update_request_size(ureq);
	update_length = offsetof(struct object_update, ou_params[0]);
	for (i = 0; i < params_count; i++)
		update_length += cfs_size_round(lens[i] + sizeof(*param));

	if (unlikely(cfs_size_round(ureq_len + update_length) >
		     ubuf->ub_req_len)) {
		int new_size = ubuf->ub_req_len;

		/* enlarge object update request size */
		while (new_size <
		       cfs_size_round(ureq_len + update_length))
			new_size += OUT_UPDATE_BUFFER_SIZE_ADD;
		if (new_size >= OUT_UPDATE_BUFFER_SIZE_MAX)
			RETURN(ERR_PTR(-E2BIG));

		rc = update_buffer_resize(ubuf, new_size);
		if (rc != 0)
			RETURN(ERR_PTR(rc));

		ureq = ubuf->ub_req;
	}

	/* fill the update into the update buffer */
	obj_update = (struct object_update *)((char *)ureq + ureq_len);
	obj_update->ou_fid = *fid;
	obj_update->ou_type = op;
	obj_update->ou_params_count = (__u16)params_count;
	obj_update->ou_batchid = batchid;
	param = &obj_update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		param->oup_len = lens[i];
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}
	ureq->ourq_count++;

	CDEBUG(D_INFO, "%p "DFID" idx %d: op %d params %d:%d\n",
	       ureq, PFID(fid), ureq->ourq_count, op, params_count,
	       update_length);

	RETURN(obj_update);
}

/**
 * Packs one update into the update_buffer.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	bufer where update will be pack
 * \param[in] op	update operation (enum update_type)
 * \param[in] fid	object FID for this update
 * \param[in] param_count	parameters count for this update
 * \param[in] lens	each parameters length of this update
 * \param[in] batchid	batchid(transaction no) of this update
 *
 * \retval		= 0 if updates packing succeed
 *                      negative errno if updates packing failed
 **/
int out_update_pack(const struct lu_env *env,
		    struct update_buffer *ubuf, int op,
		    const struct lu_fid *fid, int params_count, int *lens,
		    const char **bufs, __u64 batchid)
{
	struct object_update		*update;
	struct object_update_param	*param;
	int				i;
	ENTRY;

	update = out_update_header_pack(env, ubuf, op, fid, params_count, lens,
					batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	param = &update->ou_params[0];
	for (i = 0; i < params_count; i++) {
		memcpy(&param->oup_buf[0], bufs[i], lens[i]);
		param = (struct object_update_param *)((char *)param +
			 object_update_param_size(param));
	}

	RETURN(0);
}
EXPORT_SYMBOL(out_update_pack);

/**
 * Pack object create update into the update_buffer.
 *
 * \param[in] env	execution environment
 * \param[in] ubuf	update bufer which it will pack the update in.
 * \param[in] op	update operation
 * \param[in] fid	object FID for this update.
 * \param[in] param_count	parameters count for this update.
 * \param[in] lens	each parameters length of this update.
 * \param[in] batchid	batchid(transaction no) of this update.
 *
 * \retval		= 0 if create updates packing succeed.
 *                      negative errno if create updates packing failed.
 **/
int out_create_pack(const struct lu_env *env, const struct lu_fid *fid,
		    struct lu_attr *attr,
		    struct dt_allocation_hint *hint,
		    struct dt_object_format *dof,
		    struct update_buffer *ubuf, __u64 batchid)
{
	struct obdo		*obdo;
	int			sizes[2] = {sizeof(*obdo), 0};
	int			buf_count = 1;
	struct lu_fid		*fid1 = NULL;
	struct object_update	*update;
	ENTRY;

	if (hint != NULL && hint->dah_parent) {
		fid1 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	update = out_update_header_pack(env, ubuf, OUT_CREATE, fid,
					buf_count, sizes, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

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

/**
 * The following functions pack different updates into the update_buffer
 * So parameters of these API is basically same as its correspondent OSD/OSP
 * API, for detail description of these parameters see osd_handler.c or
 * osp_md_object.c.
 *
 * param[in] env	execution environment
 * param[in] fid	fid of this object for the update
 * param[in] ubuf	update buffer
 * param[in] batchid	batch id of this update
 *
 * retval		= 0 if insertion succeed
 *                      negative errno if insertion failed
 */
int out_ref_del_pack(const struct lu_env *env, const struct lu_fid *fid,
		     struct update_buffer *ubuf, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_REF_DEL, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_ref_del_pack);

int out_ref_add_pack(const struct lu_env *env, const struct lu_fid *fid,
		     struct update_buffer *ubuf, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_REF_ADD, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_ref_add_pack);

int out_attr_set_pack(const struct lu_env *env, const struct lu_fid *fid,
		      const struct lu_attr *attr, struct update_buffer *ubuf,
		      __u64 batchid)
{
	struct object_update	*update;
	struct obdo		*obdo;
	int			size = sizeof(*obdo);
	ENTRY;

	update = out_update_header_pack(env, ubuf, OUT_ATTR_SET, fid, 1,
					&size, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	obdo = object_update_param_get(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, (struct lu_attr *)attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	RETURN(0);
}
EXPORT_SYMBOL(out_attr_set_pack);

int out_xattr_set_pack(const struct lu_env *env, const struct lu_fid *fid,
		       const struct lu_buf *buf, const char *name, int flag,
		       struct update_buffer *ubuf, __u64 batchid)
{
	int	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(int)};
	const char *bufs[3] = {(char *)name, (char *)buf->lb_buf,
			       (char *)&flag};

	return out_update_pack(env, ubuf, OUT_XATTR_SET, fid,
			       ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_xattr_set_pack);

int out_xattr_del_pack(const struct lu_env *env, const struct lu_fid *fid,
		       const char *name, struct update_buffer *ubuf,
		       __u64 batchid)
{
	int	size = strlen(name) + 1;

	return out_update_pack(env, ubuf, OUT_XATTR_DEL, fid, 1, &size, &name,
			       batchid);
}
EXPORT_SYMBOL(out_xattr_del_pack);


int out_index_insert_pack(const struct lu_env *env, const struct lu_fid *fid,
			  const struct dt_rec *rec, const struct dt_key *key,
			  struct update_buffer *ubuf, __u64 batchid)
{
	int	sizes[2] = {strlen((char *)key) + 1, sizeof(struct lu_fid)};
	const char *bufs[2] = {(char *)key, (char *)rec};

	return out_update_pack(env, ubuf, OUT_INDEX_INSERT, fid,
			       ARRAY_SIZE(sizes), sizes, bufs, batchid);
}
EXPORT_SYMBOL(out_index_insert_pack);

int out_index_delete_pack(const struct lu_env *env, const struct lu_fid *fid,
			  const struct dt_key *key, struct update_buffer *ubuf,
			  __u64 batchid)
{
	int	size = strlen((char *)key) + 1;
	const char *buf = (char *)key;

	return out_update_pack(env, ubuf, OUT_INDEX_DELETE, fid, 1, &size,
			       &buf, batchid);
}
EXPORT_SYMBOL(out_index_delete_pack);

int out_object_destroy_pack(const struct lu_env *env, const struct lu_fid *fid,
			      struct update_buffer *ubuf, __u64 batchid)
{
	return out_update_pack(env, ubuf, OUT_DESTROY, fid, 0, NULL, NULL,
			       batchid);
}
EXPORT_SYMBOL(out_object_destroy_pack);

int out_write_pack(const struct lu_env *env, const struct lu_fid *fid,
		   const struct lu_buf *buf, loff_t pos,
		   struct update_buffer *ubuf, __u64 batchid)
{
	int		sizes[2] = {buf->lb_len, sizeof(pos)};
	const char	*bufs[2] = {(char *)buf->lb_buf, (char *)&pos};
	int		rc;

	pos = cpu_to_le64(pos);
	bufs[1] = (char *)&pos;

	rc = out_update_pack(env, ubuf, OUT_WRITE, fid, ARRAY_SIZE(sizes),
			     sizes, bufs, batchid);
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
int out_index_lookup_pack(const struct lu_env *env, const struct lu_fid *fid,
			  struct dt_rec *rec, const struct dt_key *key,
			  struct update_buffer *ubuf)
{
	int     size = strlen((char *)key) + 1;
	const char	*name = (const char *)key;

	return out_update_pack(env, ubuf, OUT_INDEX_LOOKUP, fid, 1, &size,
			       (const char **)&name, 0);
}
EXPORT_SYMBOL(out_index_lookup_pack);

int out_attr_get_pack(const struct lu_env *env, const struct lu_fid *fid,
		      struct update_buffer *ubuf)
{
	return out_update_pack(env, ubuf, OUT_ATTR_GET, fid, 0, NULL, NULL, 0);
}
EXPORT_SYMBOL(out_attr_get_pack);

int out_xattr_get_pack(const struct lu_env *env, const struct lu_fid *fid,
		       const char *name, struct update_buffer *ubuf)
{
	int     size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;
	return out_update_pack(env, ubuf, OUT_XATTR_GET, fid, 1, &size,
			       (const char **)&name, 0);
}
EXPORT_SYMBOL(out_xattr_get_pack);
