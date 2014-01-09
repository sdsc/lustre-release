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
 * lustre/osp/osp_md_object.c
 *
 * Lustre MDT Proxy Device
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include <lustre_log.h>
#include <lustre_update.h>
#include "osp_internal.h"
static const char dot[] = ".";
static const char dotdot[] = "..";

int osp_prep_update_req(const struct lu_env *env, struct osp_device *osp,
			struct object_update_request *ureq, int ureq_len,
			struct ptlrpc_request **reqp)
{
	struct obd_import		*imp;
	struct ptlrpc_request		*req;
	struct object_update_request	*tmp;
	int				rc;
	ENTRY;

	imp = osp->opd_obd->u.cli.cl_import;
	LASSERT(imp);

	req = ptlrpc_request_alloc(imp, &RQF_OBJECT_UPDATE);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_OBJECT_UPDATE, RCL_CLIENT,
			     UPDATE_BUFFER_SIZE);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, OBJECT_UPDATE);
	if (rc != 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	req_capsule_set_size(&req->rq_pill, &RMF_OBJECT_UPDATE_REPLY,
			     RCL_SERVER, UPDATE_BUFFER_SIZE);

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_OBJECT_UPDATE);
	memcpy(tmp, ureq, ureq_len);

	ptlrpc_request_set_replen(req);

	*reqp = req;

	RETURN(rc);
}

static int osp_remote_sync(const struct lu_env *env, struct dt_device *dt,
			   struct dt_update_request *dt_update,
			   struct ptlrpc_request **reqp)
{
	struct osp_device	*osp = dt2osp_dev(dt);
	struct ptlrpc_request	*req = NULL;
	int			rc;
	ENTRY;

	rc = osp_prep_update_req(env, osp, dt_update->dur_req,
				 UPDATE_BUFFER_SIZE, &req);
	if (rc)
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

/**
 * Create a new update request for the device.
 */
struct dt_update_request *osp_create_update_req(struct dt_device *dt)
{
	struct dt_update_request *dt_update;

	OBD_ALLOC_PTR(dt_update);
	if (!dt_update)
		return ERR_PTR(-ENOMEM);

	OBD_ALLOC_LARGE(dt_update->dur_req, UPDATE_BUFFER_SIZE);
	if (dt_update->dur_req == NULL) {
		OBD_FREE_PTR(dt_update);
		return ERR_PTR(-ENOMEM);
	}

	CFS_INIT_LIST_HEAD(&dt_update->dur_list);
	dt_update->dur_dt = dt;
	dt_update->dur_batchid = 0;
	dt_update->dur_req->ourq_magic = UPDATE_REQUEST_MAGIC;
	dt_update->dur_req->ourq_count = 0;

	return dt_update;
}

void osp_destroy_update_req(struct dt_update_request *dt_update)
{
	if (dt_update == NULL)
		return;

	cfs_list_del(&dt_update->dur_list);
	if (dt_update->dur_req != NULL)
		OBD_FREE_LARGE(dt_update->dur_req, UPDATE_BUFFER_SIZE);

	OBD_FREE_PTR(dt_update);
	return;
}

int osp_trans_stop(const struct lu_env *env, struct thandle *th)
{
	int rc = 0;

	rc = th->th_current_request->dur_rc;
	osp_destroy_update_req(th->th_current_request);
	th->th_current_request = NULL;

	return rc;
}

/**
 * In DNE phase I, all remote updates will be packed into RPC (the format
 * description is in lustre_idl.h) during declare phase, all of updates
 * are attached to the transaction, one entry per OSP. Then in trans start,
 * LOD will walk through these entries and send these UPDATEs to the remote
 * MDT to be executed synchronously.
 */
int osp_trans_start(const struct lu_env *env, struct dt_device *dt,
		    struct thandle *th)
{
	struct dt_update_request *dt_update;
	int rc = 0;

	/* In phase I, if the transaction includes remote updates, the local
	 * update should be synchronized, so it will set th_sync = 1 */
	dt_update = th->th_current_request;
	LASSERT(dt_update != NULL && dt_update->dur_dt == dt);
	if (dt_update->dur_req->ourq_count > 0) {
		rc = osp_remote_sync(env, dt, dt_update, NULL);
		th->th_sync = 1;
	}

	RETURN(rc);
}

/**
 * Insert the update into the th_bufs for the device.
 */
int osp_insert_update(const struct lu_env *env,
		      struct dt_update_request *update, int op,
		      struct lu_fid *fid, int count, int *lens,
		      char **bufs)
{
	struct object_update_request	*ureq = update->dur_req;
	int				ureq_len;
	struct object_update		*obj_update;
	int				update_length;
	int				rc = 0;
	char				*ptr;
	int				i;
	ENTRY;

	if (count > OBJECT_UPDATE_PARAMS_MAX ||
	    ureq->ourq_count >= OBJECT_UPDATE_PER_RPC_MAX) {
		CERROR("%s: Too much params %d or update %d "DFID" op %d: "
		       "rc = %d\n",
		       update->dur_dt->dd_lu_dev.ld_obd->obd_name,
		       count, ureq->ourq_count, PFID(fid), op, -E2BIG);
		RETURN(-E2BIG);
	}

	/* Check update size to make sure it can fit into the buffer */
	ureq_len = object_update_request_size(ureq);
	update_length = cfs_size_round(offsetof(struct object_update,
				       ou_bufs[0]));
	for (i = 0; i < count; i++)
		update_length += cfs_size_round(lens[i]);

	if (cfs_size_round(ureq_len + update_length) > UPDATE_BUFFER_SIZE) {
		CERROR("%s: insert up %p, ulen %d cnt %d rlen %d: rc = %d\n",
			update->dur_dt->dd_lu_dev.ld_obd->obd_name, ureq,
			update_length, ureq->ourq_count, ureq_len, -E2BIG);
		RETURN(-E2BIG);
	}

	/* fill the update into the update buffer */
	obj_update = (struct object_update *)((char *)ureq + ureq_len);
	obj_update->ou_fid = *fid;
	obj_update->ou_type = op;
	obj_update->ou_batchid = update->dur_batchid;
	for (i = 0; i < count; i++)
		obj_update->ou_lens[i] = lens[i];

	ptr = (char *)obj_update +
	      cfs_size_round(offsetof(struct object_update,
				      ou_bufs[0]));
	for (i = 0; i < count; i++)
		LOGL(bufs[i], lens[i], ptr);

	ureq->ourq_count++;

	CDEBUG(D_INFO, "%s: %p "DFID" idx %d: op %d params %d:%d\n",
	       update->dur_dt->dd_lu_dev.ld_obd->obd_name, ureq, PFID(fid),
	       ureq->ourq_count, op, count, ureq_len + update_length);

	RETURN(rc);
}

static struct dt_update_request
*osp_find_update(struct thandle *th, struct dt_device *dt_dev)
{
	struct dt_update_request   *dt_update;

	/* Because transaction api does not proivde the interface
	 * to transfer the update from LOD to OSP,  we need walk
	 * remote update list to find the update, this probably
	 * should move to LOD layer, when update can be part of
	 * the trancation api parameter. XXX */
	cfs_list_for_each_entry(dt_update, &th->th_remote_update_list,
				dur_list) {
		if (dt_update->dur_dt == dt_dev)
			return dt_update;
	}
	return NULL;
}

static inline void
osp_md_add_update_batchid(struct dt_update_request *update)
{
	update->dur_batchid++;
}

/**
 * Find one loc in th_dev/dev_obj_update for the update,
 * Because only one thread can access this thandle, no need
 * lock now.
 */
static struct dt_update_request
*osp_find_create_update_loc(struct thandle *th, struct dt_object *dt)
{
	struct dt_device		*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct dt_update_request	*update;
	ENTRY;

	update = osp_find_update(th, dt_dev);
	if (update != NULL)
		RETURN(update);

	update = osp_create_update_req(dt_dev);
	if (IS_ERR(update))
		RETURN(update);

	cfs_list_add_tail(&update->dur_list, &th->th_remote_update_list);

	RETURN(update);
}

static int osp_get_attr_from_req(const struct lu_env *env,
				 struct ptlrpc_request *req,
				 struct lu_attr *attr, int index)
{
	struct object_update_reply	*reply;
	struct obdo			*lobdo = &osp_env_info(env)->osi_obdo;
	struct obdo			*wobdo;
	int				size;

	LASSERT(attr != NULL);

	reply = req_capsule_server_sized_get(&req->rq_pill,
					     &RMF_OBJECT_UPDATE_REPLY,
					     UPDATE_BUFFER_SIZE);
	if (reply == NULL || reply->ourp_magic != UPDATE_REPLY_MAGIC)
		return -EPROTO;

	size = object_update_result_data_get(reply, (void **)&wobdo,
					     index);
	if (size < 0)
		return size;
	if (size != sizeof(struct obdo))
		return -EPROTO;

	if (ptlrpc_rep_need_swab(req))
		lustre_swab_obdo(wobdo);

	lustre_get_wire_obdo(NULL, lobdo, wobdo);
	la_from_obdo(attr, lobdo, lobdo->o_valid);

	return 0;
}

static int osp_md_declare_object_create(const struct lu_env *env,
					struct dt_object *dt,
					struct lu_attr *attr,
					struct dt_allocation_hint *hint,
					struct dt_object_format *dof,
					struct thandle *th)
{
	struct osp_thread_info		*osi = osp_env_info(env);
	struct dt_update_request	*update;
	struct lu_fid			*fid1;
	int				sizes[2] = {sizeof(struct obdo), 0};
	char				*bufs[2] = {NULL, NULL};
	int				buf_count;
	int				rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	osi->osi_obdo.o_valid = 0;
	LASSERT(S_ISDIR(attr->la_mode));
	obdo_from_la(&osi->osi_obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, &osi->osi_obdo, &osi->osi_obdo);

	bufs[0] = (char *)&osi->osi_obdo;
	buf_count = 1;
	fid1 = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	if (hint != NULL && hint->dah_parent) {
		struct lu_fid *fid2;

		fid2 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid2);
		bufs[1] = (char *)fid2;
		buf_count++;
	}

	if (lu_object_exists(&dt->do_lu)) {
		/* If the object already exists, we needs to destroy
		 * this orphan object first.
		 *
		 * The scenario might happen in this case
		 *
		 * 1. client send remote create to MDT0.
		 * 2. MDT0 send create update to MDT1.
		 * 3. MDT1 finished create synchronously.
		 * 4. MDT0 failed and reboot.
		 * 5. client resend remote create to MDT0.
		 * 6. MDT0 tries to resend create update to MDT1,
		 *    but find the object already exists
		 */
		CDEBUG(D_HA, "%s: object "DFID" exists, destroy this orphan\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, PFID(fid1));

		rc = osp_insert_update(env, update, OUT_REF_DEL, fid1, 0,
				       NULL, NULL);
		if (rc != 0)
			GOTO(out, rc);

		if (S_ISDIR(lu_object_attr(&dt->do_lu))) {
			/* decrease for ".." */
			rc = osp_insert_update(env, update, OUT_REF_DEL, fid1,
					       0, NULL, NULL);
			if (rc != 0)
				GOTO(out, rc);
		}

		rc = osp_insert_update(env, update, OUT_DESTROY, fid1, 0, NULL,
				       NULL);
		if (rc != 0)
			GOTO(out, rc);

		dt->do_lu.lo_header->loh_attr &= ~LOHA_EXISTS;
		/* Increase batchid to add this orphan object deletion
		 * to separate transaction */
		osp_md_add_update_batchid(update);
	}

	rc = osp_insert_update(env, update, OUT_CREATE, fid1, buf_count, sizes,
			       bufs);
out:
	if (rc)
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);

	return rc;
}

static int osp_md_object_create(const struct lu_env *env, struct dt_object *dt,
				struct lu_attr *attr,
				struct dt_allocation_hint *hint,
				struct dt_object_format *dof,
				struct thandle *th)
{
	struct osp_object  *obj = dt2osp_obj(dt);

	CDEBUG(D_INFO, "create object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	/* Because the create update RPC will be sent during declare phase,
	 * if creation reaches here, it means the object has been created
	 * successfully */
	dt->do_lu.lo_header->loh_attr |= LOHA_EXISTS | (attr->la_mode & S_IFMT);
	obj->opo_empty = 1;

	return 0;
}

static int osp_md_declare_object_ref_del(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		      (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OUT_REF_DEL, fid, 0, NULL, NULL);

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
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OUT_REF_ADD, fid, 0, NULL, NULL);

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
			   umode_t child_mode)
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
	struct osp_thread_info		*osi = osp_env_info(env);
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				size = sizeof(struct obdo);
	char				*buf;
	int				rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	osi->osi_obdo.o_valid = 0;
	LASSERT(!(attr->la_valid & (LA_MODE | LA_TYPE)));
	obdo_from_la(&osi->osi_obdo, (struct lu_attr *)attr,
		     attr->la_valid);
	lustre_set_wire_obdo(NULL, &osi->osi_obdo, &osi->osi_obdo);

	buf = (char *)&osi->osi_obdo;
	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OUT_ATTR_SET, fid, 1, &size, &buf);

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
				    const char *name, int flag,
				    struct thandle *th)
{
	struct dt_update_request  *update;
	struct lu_fid		  *fid;
	int			  sizes[3] = {strlen(name) + 1, buf->lb_len,
					      sizeof(int)};
	char			  *bufs[3] = {(char *)name,
					      (char *)buf->lb_buf};
	int			  rc;

	LASSERT(buf->lb_len > 0 && buf->lb_buf != NULL);
	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	bufs[2] = (char *)&flag;

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	rc = osp_insert_update(env, update, OUT_XATTR_SET, fid,
			       ARRAY_SIZE(sizes), sizes, bufs);

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
	struct dt_device	   *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct dt_update_request   *update = NULL;
	struct object_update_reply *reply;
	struct ptlrpc_request	   *req = NULL;
	int			   rc;
	int			   buf_len;
	int			   size;
	void			   *ea_buf;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	update = osp_create_update_req(dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	LASSERT(name != NULL);
	buf_len = strlen(name) + 1;
	rc = osp_insert_update(env, update, OUT_XATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       1, &buf_len, (char **)&name);
	if (rc != 0) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);
		GOTO(out, rc);
	}
	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);

	rc = osp_remote_sync(env, dt_dev, update, &req);
	if (rc != 0)
		GOTO(out, rc);

	reply = req_capsule_server_sized_get(&req->rq_pill,
					     &RMF_OBJECT_UPDATE_REPLY,
					     UPDATE_BUFFER_SIZE);
	if (reply->ourp_magic != UPDATE_REPLY_MAGIC) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       reply->ourp_magic, UPDATE_REPLY_MAGIC, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	size = object_update_result_data_get(reply, &ea_buf, 0);
	if (size < 0)
		GOTO(out, rc = size);

	LASSERT(size > 0 && size < PAGE_CACHE_SIZE);
	LASSERT(ea_buf != NULL);

	rc = size;
	if (buf->lb_buf != NULL)
		memcpy(buf->lb_buf, ea_buf, size);
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	osp_destroy_update_req(update);

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
	struct dt_device	   *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct dt_update_request   *update;
	struct object_update_reply *reply;
	struct ptlrpc_request	   *req = NULL;
	int			   size = strlen((char *)key) + 1;
	char			   *name = (char *)key;
	struct lu_fid		   *fid;
	int			   rc;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	update = osp_create_update_req(dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = osp_insert_update(env, update, OUT_INDEX_LOOKUP,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       1, &size, (char **)&name);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = osp_remote_sync(env, dt_dev, update, &req);
	if (rc < 0)
		GOTO(out, rc);

	reply = req_capsule_server_sized_get(&req->rq_pill,
					     &RMF_OBJECT_UPDATE_REPLY,
					     UPDATE_BUFFER_SIZE);
	if (reply->ourp_magic != UPDATE_REPLY_MAGIC) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       reply->ourp_magic, UPDATE_REPLY_MAGIC, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	size = object_update_result_data_get(reply, (void **)&fid, 0);
	if (size < 0)
		GOTO(out, rc = size);

	if (size != sizeof(struct lu_fid)) {
		CERROR("%s: lookup "DFID" %s wrong size %d: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, size, rc);
		GOTO(out, rc = -EINVAL);
	}

	if (ptlrpc_rep_need_swab(req))
		lustre_swab_lu_fid(fid);

	if (!fid_is_sane(fid)) {
		CERROR("%s: lookup "DFID" %s invalid fid "DFID": rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, PFID(fid),
		       rc);
		GOTO(out, rc = -EINVAL);
	}
	memcpy(rec, fid, sizeof(*fid));
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	osp_destroy_update_req(update);

	RETURN(rc);
}

static int osp_md_declare_insert(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct dt_update_request *update;
	struct lu_fid		 *fid;
	struct lu_fid		 *rec_fid = (struct lu_fid *)rec;
	int			 size[2] = {strlen((char *)key) + 1,
						  sizeof(*rec_fid)};
	char			 *bufs[2] = {(char *)key, (char *)rec_fid};
	int			 rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	CDEBUG(D_INFO, "%s: insert index of "DFID" %s: "DFID"\n",
	       dt->do_lu.lo_dev->ld_obd->obd_name,
	       PFID(fid), (char *)key, PFID(rec_fid));

	rc = osp_insert_update(env, update, OUT_INDEX_INSERT, fid,
			       ARRAY_SIZE(size), size, bufs);
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
	struct dt_update_request *update;
	struct lu_fid *fid;
	int size = strlen((char *)key) + 1;
	char *buf = (char *)key;
	int rc;

	update = osp_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = osp_insert_update(env, update, OUT_INDEX_DELETE, fid, 1, &size,
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
 * Since dir_empty will be return by OUT_ATTR_GET(see osp_md_attr_get/
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
	struct osp_object *o = dt2osp_obj(dt);

	if (o->opo_empty)
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
	struct dt_update_request *update = NULL;
	struct ptlrpc_request *req = NULL;
	int rc;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	update = osp_create_update_req(dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = osp_insert_update(env, update, OUT_ATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       0, NULL, NULL);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}
	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);

	rc = osp_remote_sync(env, dt_dev, update, &req);
	if (rc < 0)
		GOTO(out, rc);

	rc = osp_get_attr_from_req(env, req, attr, 0);
	if (rc)
		GOTO(out, rc);

	if (attr->la_flags == 1)
		obj->opo_empty = 0;
	else
		obj->opo_empty = 1;
out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	osp_destroy_update_req(update);

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

	RETURN(rc);
}

static int osp_md_object_lock(const struct lu_env *env,
			      struct dt_object *dt,
			      struct lustre_handle *lh,
			      struct ldlm_enqueue_info *einfo,
			      void *policy)
{
	struct osp_thread_info	*info = osp_env_info(env);
	struct ldlm_res_id	*res_id = &info->osi_resid;
	struct dt_device	*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct osp_device	*osp = dt2osp_dev(dt_dev);
	struct ptlrpc_request	*req;
	int			rc = 0;
	__u64			flags = 0;
	ldlm_mode_t		mode;

	fid_build_reg_res_name(lu_object_fid(&dt->do_lu), res_id);

	mode = ldlm_lock_match(osp->opd_obd->obd_namespace,
			       LDLM_FL_BLOCK_GRANTED, res_id,
			       einfo->ei_type,
			       (ldlm_policy_data_t *)policy,
			       einfo->ei_mode, lh, 0);
	if (mode > 0)
		return ELDLM_OK;

	req = ldlm_enqueue_pack(osp->opd_exp, 0);
	if (IS_ERR(req))
		RETURN(PTR_ERR(req));

	rc = ldlm_cli_enqueue(osp->opd_exp, &req, einfo, res_id,
			      (const ldlm_policy_data_t *)policy,
			      &flags, NULL, 0, LVB_T_NONE, lh, 0);

	ptlrpc_req_finished(req);

	return rc == ELDLM_OK ? 0 : -EIO;
}

static int osp_md_object_unlock(const struct lu_env *env,
				struct dt_object *dt,
				struct ldlm_enqueue_info *einfo,
				void *policy)
{
	struct osp_thread_info *info = osp_env_info(env);
	struct ldlm_res_id     *res_id = &info->osi_resid;
	struct dt_device       *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct osp_device      *osp = dt2osp_dev(dt_dev);
	struct lustre_handle	lockh;
	struct ldlm_lock	*lock;
	ldlm_mode_t		mode;

	fid_build_reg_res_name(lu_object_fid(&dt->do_lu), res_id);

	mode = ldlm_lock_match(osp->opd_obd->obd_namespace,
			       LDLM_FL_BLOCK_GRANTED, res_id,
			       einfo->ei_type,
			       (ldlm_policy_data_t *)policy,
			       einfo->ei_mode, &lockh, 0);
	if (mode == 0)
		return 0;

	lock = ldlm_handle2lock(&lockh);
	LASSERT(lock != NULL);

	LDLM_LOCK_PUT(lock);
	/* match for ldlm_lock_match */
	ldlm_lock_decref(&lockh, einfo->ei_mode);

	/* unlock finally */
	ldlm_lock_decref(&lockh, einfo->ei_mode);

	return 0;
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
	.do_object_lock       = osp_md_object_lock,
	.do_object_unlock     = osp_md_object_unlock,
};
