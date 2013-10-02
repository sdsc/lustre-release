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
 * Copyright  2009 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osp/osp_md_object.c
 *
 * Lustre MDT Proxy Device
 *
 * Author: Di Wang <di.wang@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <lustre_log.h>
#include "osp_internal.h"
static const char dot[] = ".";
static const char dotdot[] = "..";

struct async_args {
	th_sync_callback_t	callback;
	struct update_request	*update;
	void			*data;
};

static int osp_md_async_interpret(const struct lu_env *env,
				  struct ptlrpc_request *req,
				  void *data, int rc)
{
	struct async_args	*aa = (struct async_args *)data;
	struct update_request	*update = aa->update;
	struct update_callback	*callback;
	struct update_callback	*tmp;
	int			rc1;

	LASSERT(aa->callback != NULL && aa->data != NULL);
	if (rc == 0) {
		spin_lock(&update->ur_cb_list_lock);
		cfs_list_for_each_entry_safe(callback, tmp, &update->ur_cb_list,
					     uc_list) {
			rc1 = callback->uc_callback(env, req, callback->uc_idx,
						    callback->uc_args, rc);
			if (rc1 != 0) {
				CERROR("%dth callback wrong %d\n",
					callback->uc_idx, rc1);
				rc = rc1;
			}
		}
		spin_unlock(&update->ur_cb_list_lock);
	}
	update->ur_rc = rc;
	return aa->callback(aa->data);
}

static int osp_add_update_callback(struct update_request *update,
				   update_callback_t cb, int idx, void *args)
{
	struct update_callback *callback;

	OBD_ALLOC_PTR(callback);
	if (callback == NULL)
		return -ENOMEM;

	CFS_INIT_LIST_HEAD(&callback->uc_list);

	callback->uc_callback = cb;
	callback->uc_idx = idx;
	callback->uc_args = args;
	cfs_list_add_tail(&callback->uc_list, &update->ur_cb_list);
	return 0;
}

static int osp_prep_update_req(const struct lu_env *env,
			       struct osp_device *osp,
			       char *buf, int buf_len,
			       struct ptlrpc_request **reqp)
{
	struct obd_import      *imp;
	struct ptlrpc_request  *req;
	char                   *tmp;
	int			rc;
	ENTRY;

	imp = osp->opd_obd->u.cli.cl_import;
	LASSERT(imp);

	req = ptlrpc_request_alloc(imp, &RQF_MDS_OBJ_UPDATE);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_EADATA, RCL_CLIENT,
			     UPDATE_BUFFER_SIZE);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_OBJ_UPDATE);
	if (rc != 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	if (req_capsule_has_field(&req->rq_pill, &RMF_EADATA, RCL_SERVER))
		req_capsule_set_size(&req->rq_pill, &RMF_EADATA,
				     RCL_SERVER, UPDATE_BUFFER_SIZE);

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_EADATA);
	memcpy(tmp, buf, buf_len);

	ptlrpc_request_set_replen(req);

	*reqp = req;

	RETURN(rc);
}

static int osp_md_sync(const struct lu_env *env, struct dt_device *dt,
		       char *buf, int buf_len, struct update_request *update,
		       th_sync_callback_t callback, void *data,
		       struct ptlrpc_request **reqp)
{
	struct osp_device      *osp = dt2osp_dev(dt);
	struct ptlrpc_request  *req = NULL;
	int                     rc;
	struct async_args      *arg;
	ENTRY;

	rc = osp_prep_update_req(env, osp, buf, buf_len, &req);
	if (rc)
		RETURN(rc);

	if (callback != NULL && data != NULL) {
		arg = ptlrpc_req_async_args(req);
		arg->callback = callback;
		arg->data = data;
		arg->update = update;
		req->rq_interpret_reply = osp_md_async_interpret;

		ptlrpcd_add_req(req, PDL_POLICY_ROUND, -1);
	} else {
		/* If callback and data are NULL, means we need send the
		 * request right away and wait the result synchrously. */
		rc = ptlrpc_queue_wait(req);
		LASSERT(reqp != NULL);
		*reqp = req;
	}

	RETURN(rc);
}

static int osp_remote_sync(const struct lu_env *env, struct dt_device *dt,
			   struct update_request *update,
			   th_sync_callback_t cb, void *data,
			   struct ptlrpc_request **req)
{
	struct update_buf *ubuf;

	ubuf = (struct update_buf *)update->ur_buf;
	ubuf->ub_count = cpu_to_le32(ubuf->ub_count);
	ubuf->ub_magic = cpu_to_le32(ubuf->ub_magic);
	return osp_md_sync(env, dt, (char *)ubuf, UPDATE_BUFFER_SIZE,
			   update, cb, data, req);
}

int osp_trans_start(const struct lu_env *env, struct dt_device *dt,
		    struct thandle *th)
{
	struct update_request *update;
	struct update_buf *ubuf;
	int rc = 0;

	LASSERT(th->th_sync_cb != NULL);
	LASSERT(th->th_sync_data != NULL);

	if (th->th_sync == 0)
		th->th_sync = 1;
	update = th->th_current_request;
	LASSERT(update != NULL && update->ur_dt == dt);
	ubuf = (struct update_buf *)update->ur_buf;
	if (ubuf->ub_count > 0)
		rc = osp_remote_sync(env, dt, update,
				     th->th_sync_cb,
				     th->th_sync_data, NULL);
	RETURN(rc);
}

int osp_trans_stop(const struct lu_env *env, struct thandle *th)
{
	struct update_request *update = th->th_current_request;
	struct update_callback *callback;
	struct update_callback *cb_tmp;
	int rc = 0;

	LASSERT(update != NULL);

	/* Delete callback of each update */
	spin_lock(&update->ur_cb_list_lock);
	cfs_list_for_each_entry_safe(callback, cb_tmp,
				     &update->ur_cb_list,
				     uc_list) {
		cfs_list_del(&callback->uc_list);
		OBD_FREE_PTR(callback);
	}
	spin_unlock(&update->ur_cb_list_lock);
	if (update->ur_rc) {
		CERROR("update on dt %p is wrong %d\n",
		       update->ur_dt, update->ur_rc);
	}
	rc = update->ur_rc;
	cfs_list_del(&update->ur_list);
	OBD_FREE_PTR(update);
	th->th_current_request = NULL;

	return rc;
}

/**
 * Create a new update request for the device.
 */
static int osp_create_update_bufs(struct dt_device *dt,
				  struct update_request **ret)
{
	struct update_request *update;
	struct update_buf     *ubuf;

	OBD_ALLOC_PTR(update);
	if (!update) {
		CERROR("%s: Can not allocate update\n",
			dt->dd_lu_dev.ld_obd->obd_name);
		RETURN(-ENOMEM);
	}

	CFS_INIT_LIST_HEAD(&update->ur_list);
	CFS_INIT_LIST_HEAD(&update->ur_cb_list);
	spin_lock_init(&update->ur_cb_list_lock);
	update->ur_dt = dt;

	ubuf = (struct update_buf *)&update->ur_buf[0];
	ubuf->ub_magic = UPDATE_BUFFER_MAGIC;
	ubuf->ub_count = 0;

	*ret = update;

	RETURN(0);
}

/**
 * Insert the update into the th_bufs for the device.
 */
static int osp_insert_update(const struct lu_env *env,
			     struct update_request *update, int op,
			     struct lu_fid *fid, int count, int *lens,
			     char **bufs)
{
	struct update_buf    *ubuf;
	struct update        *obj_update;
	char                 *ptr;
	int                   i;
	int                   update_length;
	int                   rc = 0;
	ENTRY;

	ubuf = (struct update_buf *)update->ur_buf;
	obj_update = (struct update *)((char *)ubuf +
		      cfs_size_round(update_buf_size(ubuf)));

	/* Check update size to make sure it can fill
	 * into the buffer */
	update_length = cfs_size_round(offsetof(struct update,
				       u_bufs[0]));
	for (i = 0; i < count; i++)
		update_length += cfs_size_round(lens[i]);

	if (cfs_size_round(update_buf_size(ubuf)) + update_length >
		UPDATE_BUFFER_SIZE || ubuf->ub_count >= UPDATE_MAX_OPS) {
		CERROR("%s: insert update %p, idx %d count %d length %lu\n",
			update->ur_dt->dd_lu_dev.ld_obd->obd_name, ubuf,
			update_length, ubuf->ub_count, update_buf_size(ubuf));
		RETURN(-E2BIG);
	}

	if (count > UPDATE_BUF_COUNT) {
		CERROR("%s: Insert too much params %d "DFID" op %d\n",
			update->ur_dt->dd_lu_dev.ld_obd->obd_name, count,
			PFID(fid), op);
		RETURN(-E2BIG);
	}
	/* fill the update into the update buffer */
	fid_cpu_to_le(&obj_update->u_fid, fid);
	obj_update->u_type = cpu_to_le32(op);
	for (i = 0; i < count; i++)
		obj_update->u_lens[i] = cpu_to_le32(lens[i]);

	ptr = (char *)obj_update +
			cfs_size_round(offsetof(struct update, u_bufs[0]));
	for (i = 0; i < count; i++)
		LOGL(bufs[i], lens[i], ptr);

	ubuf->ub_count++;

	CDEBUG(D_INFO, "%s: %p "DFID" count %d op, %d count %d len %lu\n",
	       update->ur_dt->dd_lu_dev.ld_obd->obd_name, ubuf, PFID(fid),
	       ubuf->ub_count, op, count, update_buf_size(ubuf));

	RETURN(rc);
}

static struct update_request
*osp_find_update(struct thandle *th, struct dt_device *dt_dev)
{
	struct update_request   *update;

	/* FIXME: this update location probably should be in
	 * LOD layer */
	cfs_list_for_each_entry(update, &th->th_remote_update_list, ur_list) {
		if (update->ur_dt == dt_dev)
			return update;
	}
	return NULL;
}

/**
 * Find one loc in th_dev/dev_obj_update for the update,
 * Because only one thread can access this thandle, no need
 * lock now.
 */
static struct update_request
*osp_find_create_update_loc(struct thandle *th, struct dt_object *dt)
{
	struct dt_device          *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct update_request	  *update;
	int                        rc = 0;
	ENTRY;

	update = osp_find_update(th, dt_dev);
	if (update != NULL)
		RETURN(update);

	rc = osp_create_update_bufs(dt_dev, &update);
	if (rc != 0)
		RETURN(ERR_PTR(rc));

	cfs_list_add_tail(&update->ur_list, &th->th_remote_update_list);

	RETURN(update);
}

static int osp_get_attr_from_req(struct ptlrpc_request *req,
				 struct lu_attr **attr, int index)
{
	int	size;
	void	*data;

	LASSERT(attr != NULL);

	data = req_capsule_server_sized_get(&req->rq_pill, &RMF_EADATA,
					    UPDATE_BUFFER_SIZE);
	LASSERT(data != NULL);

	size = update_get_reply_buf((struct update_reply *)data,
				    (void **)attr, index);
	if (size != sizeof(struct lu_attr))
		return -EPROTO;

	lu_attr_le_to_cpu(*attr, *attr);

	return 0;
}

static int osp_md_object_create_callback(const struct lu_env *env,
					 struct ptlrpc_request *req,
					 int index, void *arg, int rc)
{
	struct osp_object *osp = (struct osp_object *)arg;
	struct lu_object *obj = osp2lu_obj(osp);
	struct lu_attr	  *attr;

	rc = osp_get_attr_from_req(req, &attr, index);
	if (rc)
		return rc;

	obj->lo_header->loh_attr |= (attr->la_mode & S_IFMT);
	osp->opo_empty = 1;

	return 0;
}

static int osp_md_declare_object_create(const struct lu_env *env,
					struct dt_object *dt,
					 struct lu_attr *attr,
					 struct dt_allocation_hint *hint,
					 struct dt_object_format *dof,
					 struct thandle *th)
{
	struct osp_thread_info *osi = osp_env_info(env);
	struct update_request *update;
	struct lu_fid *fid1;
	int rc;
	int sizes[2] = {sizeof(struct lu_attr), 0};
	char *bufs[2] = {NULL, NULL};
	int buf_count;
	int idx;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	lu_attr_cpu_to_le(&osi->osi_attr, attr);
	bufs[0] = (char *)&osi->osi_attr;
	buf_count = 1;
	fid1 = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	if (hint->dah_parent) {
		struct lu_fid *fid2;
		struct lu_fid *tmp_fid = &osi->osi_fid;

		fid2 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		fid_cpu_to_le(tmp_fid, fid2);
		sizes[1] = sizeof(*tmp_fid);
		bufs[1] = (char *)tmp_fid;
		buf_count++;
	}

	rc = osp_insert_update(env, update, OBJ_CREATE, fid1, buf_count, sizes,
			       bufs);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);
		return rc;
	}

	idx = ((struct update_buf *)update->ur_buf)->ub_count - 1;
	rc = osp_add_update_callback(update, osp_md_object_create_callback, idx,
				     (void *)dt2osp_obj(dt));
	return rc;
}

static int osp_md_object_create(const struct lu_env *env, struct dt_object *dt,
				struct lu_attr *attr,
				struct dt_allocation_hint *hint,
			       struct dt_object_format *dof, struct thandle *th)
{
	CDEBUG(D_INFO, "create object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));
	return 0;
}

static int osp_md_declare_object_ref_del(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct update_request	*update;
	struct lu_fid		*fid;
	int			rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		      (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OBJ_REF_DEL, fid, 0, NULL, NULL);

	return rc;
}

static int osp_md_object_ref_del(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref del object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

static int osp_md_declare_ref_add(const struct lu_env *env,
				  struct dt_object *dt, struct thandle *th)
{
	struct update_request	*update;
	struct lu_fid		*fid;
	int			rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OBJ_REF_ADD, fid, 0, NULL, NULL);

	return rc;
}

static int osp_md_object_ref_add(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref add object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

static void osp_md_ah_init(const struct lu_env *env,
			   struct dt_allocation_hint *ah,
			   struct dt_object *parent,
			   struct dt_object *child,
			   cfs_umode_t child_mode)
{
	LASSERT(ah);

	memset(ah, 0, sizeof(*ah));
	ah->dah_parent = parent;
	ah->dah_mode = child_mode;
}

static int osp_md_declare_attr_set(const struct lu_env *env,
				   struct dt_object *dt,
				   const struct lu_attr *attr,
				   struct thandle *th)
{
	struct osp_thread_info *osi = osp_env_info(env);
	struct update_request  *update;
	struct lu_fid          *fid;
	int                     size = sizeof(struct lu_attr);
	char                   *buf;
	int                     rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	lu_attr_cpu_to_le(&osi->osi_attr, (struct lu_attr *)attr);
	buf = (char *)&osi->osi_attr;
	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OBJ_ATTR_SET, fid, 1, &size, &buf);

	return rc;
}

static int osp_md_attr_set(const struct lu_env *env, struct dt_object *dt,
			   const struct lu_attr *attr, struct thandle *th,
			   struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "attr set object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	RETURN(0);
}

static int osp_md_declare_xattr_set(const struct lu_env *env,
				    struct dt_object *dt,
				    const struct lu_buf *buf,
				    const char *name, int fl,
				    struct thandle *th)
{
	struct update_request	*update;
	struct lu_fid		*fid;
	int			sizes[4] = {strlen(name), buf->lb_len,
					     sizeof(int), sizeof(int)};
	char			*bufs[4] = {(char *)name, (char *)buf->lb_buf};
	int			buf_len;
	int			rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	buf_len = cpu_to_le32(buf->lb_len);
	bufs[2] = (char *)&buf_len;

	fl = cpu_to_le32(fl);
	bufs[3] = (char *)&fl;

	rc = osp_insert_update(env, update, OBJ_XATTR_SET, fid, 4, sizes, bufs);

	return rc;
}

static int osp_md_xattr_set(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_buf *buf, const char *name, int fl,
			    struct thandle *th, struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "xattr %s set object "DFID"\n", name,
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

static int osp_md_xattr_get(const struct lu_env *env, struct dt_object *dt,
			    struct lu_buf *buf, const char *name,
			    struct lustre_capa *capa)
{
	struct dt_device	*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct update_request	*update = NULL;
	struct ptlrpc_request	*req = NULL;
	int			rc;
	int			buf_len;
	int			size;
	void			*data;
	void		      *ea_buf;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	rc = osp_create_update_bufs(dt_dev, &update);
	if (rc != 0)
		RETURN(rc);

	LASSERT(name != NULL);
	buf_len = strlen(name);
	rc = osp_insert_update(env, update, OBJ_XATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       1, &buf_len, (char **)&name);
	if (rc) {
		CERROR("Insert update error: rc = %d\n", rc);
		GOTO(out, rc);
	}
	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);

	rc = osp_remote_sync(env, dt_dev, update, NULL, NULL, &req);
	if (rc)
		GOTO(out, rc);

	data = req_capsule_server_sized_get(&req->rq_pill, &RMF_EADATA,
					    UPDATE_BUFFER_SIZE);
	LASSERT(data != NULL);

	size = update_get_reply_buf((struct update_reply *)data,
				     &ea_buf, 0);
	if (size < 0)
		GOTO(out, rc = size);

	LASSERT(size > 0 && size < CFS_PAGE_SIZE);
	LASSERT(ea_buf != NULL);

	buf->lb_len = size;
	memcpy(buf->lb_buf, ea_buf, size);
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	if (update)
		OBD_FREE_PTR(update);

	RETURN(rc);
}

static void osp_md_object_read_lock(const struct lu_env *env,
				    struct dt_object *dt, unsigned role)
{
	struct osp_object  *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner != env);
	down_read_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
}

static void osp_md_object_write_lock(const struct lu_env *env,
				     struct dt_object *dt, unsigned role)
{
	struct osp_object *obj = dt2osp_obj(dt);

	down_write_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
	obj->opo_owner = env;
}

static void osp_md_object_read_unlock(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	up_read(&obj->opo_sem);
}

static void osp_md_object_write_unlock(const struct lu_env *env,
				       struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner == env);
	obj->opo_owner = NULL;
	up_write(&obj->opo_sem);
}

static int osp_md_object_write_locked(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	return obj->opo_owner == env;
}

static int osp_md_index_lookup(const struct lu_env *env, struct dt_object *dt,
			       struct dt_rec *rec, const struct dt_key *key,
			       struct lustre_capa *capa)
{
	struct dt_device	*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct update_request	*update;
	struct ptlrpc_request	*req = NULL;
	int			size = strlen((char *)key) + 1;
	char			*name = (char *)key;
	int			rc;
	void			*data;
	struct lu_fid		*fid;

	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	rc = osp_create_update_bufs(dt_dev, &update);
	if (rc != 0)
		RETURN(rc);

	rc = osp_insert_update(env, update, OBJ_INDEX_LOOKUP,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       1, &size, (char **)&name);
	if (rc) {
		CERROR("Insert update error: rc = %d\n", rc);
		GOTO(out, rc);
	}

	rc = osp_remote_sync(env, dt_dev, update, NULL, NULL, &req);
	if (rc < 0) {
		CERROR("lookup object "DFID" %s wrong %d\n",
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, rc);
		GOTO(out, rc);
	}

	data = req_capsule_server_sized_get(&req->rq_pill, &RMF_EADATA,
					    UPDATE_BUFFER_SIZE);
	LASSERT(data != NULL);

	rc = update_get_reply_result((struct update_reply *)data, NULL, 0);
	if (rc < 0) {
		CERROR("lookup object "DFID" %s return %d\n",
			PFID(lu_object_fid(&dt->do_lu)), (char *)key, rc);
		GOTO(out, rc);
	}

	size = update_get_reply_buf((struct update_reply *)data,
				     (void **)&fid, 0);
	if (size < 0)
		GOTO(out, rc = size);

	if (size != sizeof(struct lu_fid)) {
		CERROR("lookup object "DFID" %s wrong return size %d\n",
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, size);
		GOTO(out, rc = -EINVAL);
	}

	lustre_swab_lu_fid(fid);
	if (!fid_is_sane(fid)) {
		CERROR("lookup object "DFID" %s return invalid fid "DFID"\n",
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, PFID(fid));
		GOTO(out, rc = -EINVAL);
	}
	memcpy(rec, fid, sizeof(*fid));
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	if (update)
		OBD_FREE_PTR(update);

	RETURN(rc);
}

static int osp_md_declare_insert(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct update_request	*update;
	struct lu_fid		*fid;
	struct lu_fid		*rec_fid = (struct lu_fid *)rec;
	int	size[2] = {strlen((char *)key) + 1, sizeof(*rec_fid)};
	char	*bufs[2] = {(char *)key, (char *)rec_fid};
	int	rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	CDEBUG(D_INFO, "insert index of "DFID" %s: "DFID"\n",
	       PFID(fid), (char *)key, PFID(rec_fid));

	fid_cpu_to_le(rec_fid, rec_fid);

	/* . and .. will be inserted during the creation of new directory */
	rc = osp_insert_update(env, update, OBJ_INDEX_INSERT, fid, 2,
			       size, bufs);
	return rc;
}

static int osp_md_index_insert(const struct lu_env *env,
			       struct dt_object *dt,
			       const struct dt_rec *rec,
			       const struct dt_key *key,
			       struct thandle *th,
			       struct lustre_capa *capa,
			       int ignore_quota)
{
	return 0;
}

static int osp_md_declare_delete(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct update_request *update;
	struct lu_fid *fid;
	int size = strlen((char *)key) + 1;
	char *buf = (char *)key;
	int rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf wrong %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OBJ_INDEX_DELETE, fid, 1, &size,
			       &buf);

	return rc;
}

static int osp_md_index_delete(const struct lu_env *env,
			       struct dt_object *dt,
			       const struct dt_key *key,
			       struct thandle *th,
			       struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "index delete "DFID" %s\n",
	       PFID(&dt->do_lu.lo_header->loh_fid), (char *)key);

	return 0;
}

/**
 * Creates or initializes iterator context.
 *
 * Note: for OSP, these index iterate api is only used to check
 * whether the directory is empty now (see mdd_dir_is_empty).
 * Since dir_empty will be return by OBJ_ATTR_GET(see osp_md_attr_get/
 * out_attr_get). So the implementation of these iterator is simplied
 * to make mdd_dir_is_empty happy. The real iterator should be
 * implemented, if we need it one day.
 */
static struct dt_it *osp_it_init(const struct lu_env *env,
				 struct dt_object *dt,
				 __u32 attr,
				struct lustre_capa *capa)
{
	lu_object_get(&dt->do_lu);
	return (struct dt_it *)dt;
}

static void osp_it_fini(const struct lu_env *env, struct dt_it *di)
{
	struct dt_object *dt = (struct dt_object *)di;
	lu_object_put(env, &dt->do_lu);
}

static int osp_it_get(const struct lu_env *env,
		      struct dt_it *di, const struct dt_key *key)
{
	return 1;
}

static void osp_it_put(const struct lu_env *env, struct dt_it *di)
{
	return;
}

static int osp_it_next(const struct lu_env *env, struct dt_it *di)
{
	struct dt_object *dt = (struct dt_object *)di;
	struct osp_object *osp_obj = dt2osp_obj(dt);
	if (osp_obj->opo_empty)
		return 1;
	return 0;
}

static struct dt_key *osp_it_key(const struct lu_env *env,
				 const struct dt_it *di)
{
	LBUG();
	return NULL;
}

static int osp_it_key_size(const struct lu_env *env, const struct dt_it *di)
{
	LBUG();
	return 0;
}

static int osp_it_rec(const struct lu_env *env, const struct dt_it *di,
		      struct dt_rec *lde, __u32 attr)
{
	LBUG();
	return 0;
}

static __u64 osp_it_store(const struct lu_env *env, const struct dt_it *di)
{
	LBUG();
	return 0;
}

static int osp_it_load(const struct lu_env *env, const struct dt_it *di,
		       __u64 hash)
{
	LBUG();
	return 0;
}

static int osp_it_key_rec(const struct lu_env *env, const struct dt_it *di,
			  void *key_rec)
{
	LBUG();
	return 0;
}

static const struct dt_index_operations osp_md_index_ops = {
	.dio_lookup         = osp_md_index_lookup,
	.dio_declare_insert = osp_md_declare_insert,
	.dio_insert         = osp_md_index_insert,
	.dio_declare_delete = osp_md_declare_delete,
	.dio_delete         = osp_md_index_delete,
	.dio_it     = {
		.init     = osp_it_init,
		.fini     = osp_it_fini,
		.get      = osp_it_get,
		.put      = osp_it_put,
		.next     = osp_it_next,
		.key      = osp_it_key,
		.key_size = osp_it_key_size,
		.rec      = osp_it_rec,
		.store    = osp_it_store,
		.load     = osp_it_load,
		.key_rec  = osp_it_key_rec,
	}
};

static int osp_md_index_try(const struct lu_env *env,
			    struct dt_object *dt,
			    const struct dt_index_features *feat)
{
	dt->do_index_ops = &osp_md_index_ops;
	return 0;
}

static int osp_md_attr_get(const struct lu_env *env,
			   struct dt_object *dt, struct lu_attr *attr,
			   struct lustre_capa *capa)
{
	struct osp_object     *obj = dt2osp_obj(dt);
	struct dt_device      *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct update_request *update = NULL;
	struct lu_attr        *tmp_attr;
	struct ptlrpc_request *req = NULL;
	int rc;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	rc = osp_create_update_bufs(dt_dev, &update);
	if (rc != 0)
		RETURN(rc);

	rc = osp_insert_update(env, update, OBJ_ATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       0, NULL, NULL);
	if (rc) {
		CERROR("Insert update error: rc = %d\n", rc);
		GOTO(out, rc);
	}
	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);

	rc = osp_remote_sync(env, dt_dev, update, NULL, NULL, &req);
	if (rc)
		GOTO(out, rc);

	rc = osp_get_attr_from_req(req, &tmp_attr, 0);
	if (rc)
		GOTO(out, rc);

	memcpy(attr, tmp_attr, sizeof(*attr));
	if (attr->la_flags == 1)
		obj->opo_empty = 0;
	else
		obj->opo_empty = 1;
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	if (update)
		OBD_FREE_PTR(update);

	RETURN(rc);
}

static int osp_md_declare_object_destroy(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct osp_object  *o = dt2osp_obj(dt);
	int                 rc = 0;
	ENTRY;

	/*
	 * track objects to be destroyed via llog
	 */
	rc = osp_sync_declare_add(env, o, MDS_UNLINK64_REC, th);

	RETURN(rc);
}

static int osp_md_object_destroy(const struct lu_env *env,
				 struct dt_object *dt, struct thandle *th)
{
	struct osp_object  *o = dt2osp_obj(dt);
	int                 rc = 0;
	ENTRY;

	/*
	 * once transaction is committed put proper command on
	 * the queue going to our OST
	 */
	rc = osp_sync_add(env, o, MDS_UNLINK64_REC, th, NULL);

	/* not needed in cache any more */
	set_bit(LU_OBJECT_HEARD_BANSHEE, &dt->do_lu.lo_header->loh_flags);

	RETURN(rc);
}

struct dt_object_operations osp_md_obj_ops = {
	.do_read_lock         = osp_md_object_read_lock,
	.do_write_lock        = osp_md_object_write_lock,
	.do_read_unlock       = osp_md_object_read_unlock,
	.do_write_unlock      = osp_md_object_write_unlock,
	.do_write_locked      = osp_md_object_write_locked,
	.do_declare_create    = osp_md_declare_object_create,
	.do_create            = osp_md_object_create,
	.do_declare_ref_add   = osp_md_declare_ref_add,
	.do_ref_add           = osp_md_object_ref_add,
	.do_declare_ref_del   = osp_md_declare_object_ref_del,
	.do_ref_del           = osp_md_object_ref_del,
	.do_declare_destroy   = osp_md_declare_object_destroy,
	.do_destroy           = osp_md_object_destroy,
	.do_ah_init           = osp_md_ah_init,
	.do_attr_get	      = osp_md_attr_get,
	.do_declare_attr_set  = osp_md_declare_attr_set,
	.do_attr_set          = osp_md_attr_set,
	.do_declare_xattr_set = osp_md_declare_xattr_set,
	.do_xattr_set         = osp_md_xattr_set,
	.do_xattr_get         = osp_md_xattr_get,
	.do_index_try         = osp_md_index_try,
};

