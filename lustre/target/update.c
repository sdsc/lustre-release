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
 * lustre/obdclass/dt_object.c
 *
 * These funcs are used by LOD/OSP/OUT to store update into the buffer.
 *
 * Author: di wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>
#include <obd_class.h>
#include <dt_object.h>
#include <lustre_fid.h>
#include <lustre_update.h>
#include <lustre_log.h>

#define OUT_UPDATE_BUFFER_SIZE_ADD	4096
#define OUT_UPDATE_BUFFER_SIZE_MAX	(64 * 4096)  /* 64KB update size now */
struct dt_update_request*
dt_find_update(struct thandle_update *tu, struct dt_device *dt_dev)
{
	struct dt_update_request   *dt_update;

	list_for_each_entry(dt_update, &tu->tu_remote_update_list,
			    dur_list) {
		if (dt_update->dur_dt == dt_dev)
			return dt_update;
	}
	return NULL;
}
EXPORT_SYMBOL(dt_find_update);

static int update_resize_update_req(struct update_buffer *ubuf, int new_size)
{
	struct object_update_request *ureq;

	LASSERT(new_size > ubuf->ub_req_len);

	CDEBUG(D_INFO, "resize update_size from %d to %d\n",
	       ubuf->ub_req_len, new_size);

	OBD_ALLOC_LARGE(ureq, new_size);
	if (ureq == NULL)
		return -ENOMEM;

	memcpy(ureq, ubuf->ub_req,
	       object_update_request_size(ubuf->ub_req));

	OBD_FREE_LARGE(ubuf->ub_req, ubuf->ub_req_len);

	ubuf->ub_req = ureq;
	ubuf->ub_req_len = new_size;

	return 0;
}

int update_alloc_update_buf(struct update_buffer *ubuf, int ourq_size)
{
	struct object_update_request *ourq;

	OBD_ALLOC_LARGE(ourq, ourq_size);
	if (ourq == NULL)
		return -ENOMEM;
	ubuf->ub_req = ourq;
	ubuf->ub_req_len = ourq_size;
	return 0;
}
EXPORT_SYMBOL(update_alloc_update_buf);

void update_init_update_buf(struct update_buffer *ubuf)
{
	ubuf->ub_req->ourq_magic = UPDATE_REQUEST_MAGIC;
	ubuf->ub_req->ourq_count = 0;
}
EXPORT_SYMBOL(update_init_update_buf);

void update_free_update_buf(struct update_buffer *ubuf)
{
	OBD_FREE_LARGE(ubuf->ub_req, ubuf->ub_req_len);
}
EXPORT_SYMBOL(update_free_update_buf);

/**
 * pack update into the update_buffer
 **/
static struct object_update
*object_update_header_pack(const struct lu_env *env, struct update_buffer *ubuf,
			   int op, const struct lu_fid *fid, int params_count,
			   int *lens, __u64 batchid)
{
	struct object_update_request	*ureq = ubuf->ub_req;
	int				ureq_len = ubuf->ub_req_len;
	struct object_update		*obj_update;
	int				update_length;
	int				rc = 0;
	int				i;
	ENTRY;

	if (params_count > OUT_UPDATE_PARAMS_MAX ||
	    ureq->ourq_count >= OUT_UPDATE_PER_TRANS_MAX)
		RETURN(ERR_PTR(-E2BIG));

	/* Check update size to make sure it can fit into the buffer */
	ureq_len = object_update_request_size(ureq);
	update_length = cfs_size_round(offsetof(struct object_update,
				       ou_bufs[0]));
	for (i = 0; i < params_count; i++)
		update_length += cfs_size_round(lens[i]);

	if (unlikely(cfs_size_round(ureq_len + update_length) >
		     ubuf->ub_req_len)) {
		int new_size = ubuf->ub_req_len;

		/* enlarge object update request size */
		while (new_size <
		       cfs_size_round(ureq_len + update_length))
			new_size += OUT_UPDATE_BUFFER_SIZE_ADD;
		if (new_size >= OUT_UPDATE_BUFFER_SIZE_MAX)
			RETURN(ERR_PTR(-E2BIG));

		rc = update_resize_update_req(ubuf, new_size);
		if (rc != 0)
			RETURN(ERR_PTR(rc));

		ureq = ubuf->ub_req;
	}

	/* fill the update into the update buffer */
	obj_update = (struct object_update *)((char *)ureq + ureq_len);
	obj_update->ou_fid = *fid;
	obj_update->ou_type = op;
	obj_update->ou_count = (__u16)params_count;
	obj_update->ou_batchid = batchid;
	for (i = 0; i < params_count; i++)
		obj_update->ou_lens[i] = lens[i];

	ureq->ourq_count++;

	CDEBUG(D_INFO, "%p "DFID" idx %d: op %d params %d:%d\n",
	       ureq, PFID(fid), ureq->ourq_count, op, params_count,
	       ureq_len + update_length);

	RETURN(obj_update);
}

int object_update_insert(const struct lu_env *env,
			 struct update_buffer *ubuf, int op,
			 const struct lu_fid *fid, int count, int *lens,
			 const char **bufs, __u64 batchid)
{
	struct object_update	*update;
	char			*ptr;
	int			i;
	ENTRY;

	update = object_update_header_pack(env, ubuf, op, fid, count, lens,
					   batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	ptr = (char *)update + cfs_size_round(offsetof(struct object_update,
						       ou_bufs[0]));
	for (i = 0; i < count; i++)
		LOGL(bufs[i], lens[i], ptr);

	RETURN(0);
}
EXPORT_SYMBOL(object_update_insert);

int update_create_insert(const struct lu_env *env, const struct dt_object *dt,
			 struct lu_attr *attr,
			 struct dt_allocation_hint *hint,
			 struct dt_object_format *dof,
			 struct update_buffer *ubuf, __u64 batchid)
{
	struct obdo		*obdo;
	int			sizes[2] = {sizeof(struct obdo), 0};
	int			buf_count = 1;
	struct lu_fid		*fid1 = NULL;
	struct object_update	*update;
	ENTRY;

	if (hint != NULL && hint->dah_parent) {
		fid1 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	update = object_update_header_pack(env, ubuf,
					   OUT_CREATE,lu_object_fid(&dt->do_lu),
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
EXPORT_SYMBOL(update_create_insert);

int update_ref_del_insert(const struct lu_env *env,
			  const struct dt_object *dt,
			  struct update_buffer *ubuf, __u64 batchid)
{
	return object_update_insert(env, ubuf,
				    OUT_REF_DEL, lu_object_fid(&dt->do_lu),
				    0, NULL, NULL, batchid);
}
EXPORT_SYMBOL(update_ref_del_insert);

int update_ref_add_insert(const struct lu_env *env,
			  const struct dt_object *dt,
			  struct update_buffer *ubuf, __u64 batchid)
{
	return object_update_insert(env, ubuf,
				    OUT_REF_ADD, lu_object_fid(&dt->do_lu),
				    0, NULL, NULL, batchid);
}
EXPORT_SYMBOL(update_ref_add_insert);

int update_attr_set_insert(const struct lu_env *env,
			   const struct dt_object *dt,
			   const struct lu_attr *attr,
			   struct update_buffer *ubuf, __u64 batchid)
{
	struct object_update	*update;
	struct obdo		*obdo;
	struct lu_fid		*fid;
	int			size = sizeof(struct obdo);
	ENTRY;

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	update = object_update_header_pack(env, ubuf, OUT_ATTR_SET, fid, 1,
					   &size, batchid);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	obdo = object_update_param_get(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, (struct lu_attr *)attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	RETURN(0);
}
EXPORT_SYMBOL(update_attr_set_insert);

int update_xattr_set_insert(const struct lu_env *env,
			    const struct dt_object *dt,
			    const struct lu_buf *buf, const char *name,
			    int flag, struct update_buffer *ubuf,
			    __u64 batchid)
{
	int	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(int)};
	const char *bufs[3] = {(char *)name, (char *)buf->lb_buf,
			       (char *)&flag};

	return object_update_insert(env, ubuf,
				    OUT_XATTR_SET, lu_object_fid(&dt->do_lu),
				    3, sizes, bufs, batchid);
}
EXPORT_SYMBOL(update_xattr_set_insert);

int update_index_insert_insert(const struct lu_env *env,
			       const struct dt_object *dt,
			       const struct dt_rec *rec,
			       const struct dt_key *key,
			       struct update_buffer *ubuf, __u64 batchid)
{
	int	sizes[2] = {strlen((char *)key) + 1, sizeof(struct lu_fid)};
	const char *bufs[2] = {(char *)key, (char *)rec};

	return object_update_insert(env, ubuf, OUT_INDEX_INSERT,
				    lu_object_fid(&dt->do_lu),
				    2, sizes, bufs, batchid);
}
EXPORT_SYMBOL(update_index_insert_insert);

int update_index_delete_insert(const struct lu_env *env,
			       const struct dt_object *dt,
			       const struct dt_key *key,
			       struct update_buffer *ubuf, __u64 batchid)
{
	int	size = strlen((char *)key) + 1;
	const char *buf = (char *)key;

	return object_update_insert(env, ubuf,
				    OUT_INDEX_DELETE, lu_object_fid(&dt->do_lu),
				    1, &size, &buf, batchid);
}
EXPORT_SYMBOL(update_index_delete_insert);

int update_object_destroy_insert(const struct lu_env *env,
				 const struct dt_object *dt,
				 struct update_buffer *ubuf, __u64 batchid)
{
	return object_update_insert(env, ubuf,
				    OUT_DESTROY, lu_object_fid(&dt->do_lu),
				    0, NULL, NULL, batchid);
}
EXPORT_SYMBOL(update_object_destroy_insert);

/**
 * The following update funcs are only used by read-only ops, lookup,
 * getattr etc, so it does not need transaction here. Currently they
 * are only used by OSP.
 **/
int update_index_lookup_insert(const struct lu_env *env,
			       struct update_buffer *ubuf,
			       const struct dt_object *dt,
			       struct dt_rec *rec, const struct dt_key *key)
{
	int     size = strlen((char *)key) + 1;
	const char	*name = (const char *)key;

	return object_update_insert(env, ubuf, OUT_INDEX_LOOKUP,
				    lu_object_fid(&dt->do_lu), 1, &size,
				    (const char **)&name, 0);
}
EXPORT_SYMBOL(update_index_lookup_insert);

int update_attr_get_insert(const struct lu_env *env,
			   struct update_buffer *ubuf,
			   const struct dt_object *dt)
{
	return object_update_insert(env, ubuf, OUT_ATTR_GET,
				    lu_object_fid(&dt->do_lu), 0, NULL,
				    NULL, 0);
}
EXPORT_SYMBOL(update_attr_get_insert);

int update_xattr_get_insert(const struct lu_env *env,
			    struct update_buffer *ubuf,
			    const struct dt_object *dt, const char *name)
{
	int     size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;
	return object_update_insert(env, ubuf, OUT_XATTR_GET,
				    lu_object_fid(&dt->do_lu), 1, &size,
				    (const char **)&name, 0);
}
EXPORT_SYMBOL(update_xattr_get_insert);
