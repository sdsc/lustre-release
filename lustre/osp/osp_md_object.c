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
			struct update_buf *ubuf, int ubuf_len,
			struct ptlrpc_request **reqp)
{
	struct obd_import      *imp;
	struct ptlrpc_request  *req;
	struct update_buf      *tmp;
	int			rc;
	ENTRY;

	imp = osp->opd_obd->u.cli.cl_import;
	LASSERT(imp);

	req = ptlrpc_request_alloc(imp, &RQF_UPDATE_OBJ);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_UPDATE, RCL_CLIENT, ubuf_len);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, UPDATE_OBJ);
	if (rc != 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	req_capsule_set_size(&req->rq_pill, &RMF_UPDATE_REPLY, RCL_SERVER,
			     UPDATE_BUFFER_SIZE);

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_UPDATE);
	memcpy(tmp, ubuf, ubuf_len);

	ptlrpc_request_set_replen(req);

	*reqp = req;

	RETURN(rc);
}

static int osp_remote_sync(const struct lu_env *env, struct dt_device *dt,
			   struct update_buf *ubuf,
			   struct ptlrpc_request **reqp,
			   struct thandle_update_dt *tud)
{
	struct osp_device	*osp = dt2osp_dev(dt);
	struct ptlrpc_request	*req = NULL;
	int			rc;
	ENTRY;

	rc = osp_prep_update_req(env, osp, ubuf, UPDATE_BUFFER_SIZE, &req);
	if (rc)
		RETURN(rc);

	/* Note: some dt index api might return non-zero result here, like
	 * osd_index_ea_lookup, so we should only check rc < 0 here */
	rc = ptlrpc_queue_wait(req);
	if (rc < 0) {
		ptlrpc_req_finished(req);
		if (tud != NULL)
			tud->tud_rc = rc;
		RETURN(rc);
	}

	if (reqp != NULL) {
		*reqp = req;
		RETURN(rc);
	}

	if (tud != NULL)
		tud->tud_rc = rc;

	ptlrpc_req_finished(req);

	RETURN(rc);
}

/**
 * Create a new update request for the device.
 */
struct thandle_update_dt
*osp_alloc_update_request(struct dt_device *dt)
{
	struct thandle_update_dt *tud;

	OBD_ALLOC_PTR(tud);
	if (!tud)
		return ERR_PTR(-ENOMEM);

	CFS_INIT_LIST_HEAD(&tud->tud_list);
	tud->tud_dt = dt;
	return tud;
}

static struct thandle_update_dt
*osp_find_update(struct thandle *th, struct dt_device *dt_dev)
{
	struct thandle_update_dt   *tud;

	/* Because transaction api does not proivde the interface
	 * to transfer the update from LOD to OSP,  we need walk
	 * remote update list to find the update, this probably
	 * should move to LOD layer, when update can be part of
	 * the trancation api parameter. XXX */
	cfs_list_for_each_entry(tud, &th->th_remote_update_list, tud_list) {
		if (tud->tud_dt == dt_dev)
			return tud;
	}
	return NULL;
}

/**
 * Update transnos according to the reply of update
 **/
static int osp_update_transno_xid(struct update_buf *buf,
				  struct ptlrpc_request *req)
{
	struct update_reply_buf *reply_buf;
	int i;

	reply_buf = req_capsule_server_sized_get(&req->rq_pill,
						 &RMF_UPDATE_REPLY,
						 UPDATE_BUFFER_SIZE);
	if (reply_buf->urb_version != UPDATE_REPLY_V2)
		return -EPROTO;

	for (i = 0; i < reply_buf->urb_count; i++) {
		struct update_reply	*reply;
		struct update		*update;

		if (reply_buf->urb_lens[i] == 0)
			continue;

		reply = update_get_reply(reply_buf, i, NULL);
		if (reply == NULL)
			RETURN(-EPROTO);

		update = update_buf_get(buf, reply->ur_transno_idx,
					NULL);
		if (update == NULL)
			RETURN(-EPROTO);
		update->u_batchid = reply->ur_transno;
		update->u_xid = reply->ur_xid;
	}
	update_dump_buf(buf);

	RETURN(0);
}

int osp_trans_stop(const struct lu_env *env, struct dt_device *dt_dev,
		   struct thandle *th)
{
	struct thandle_update_dt *tud;
	struct ptlrpc_request *req;
	int rc = 0;
	ENTRY;

	tud = osp_find_update(th, dt_dev);
	if (tud == NULL)
		return rc;

	update_dump_buf(th->th_update_buf);
	LASSERT(tud->tud_count > 0);
	LASSERT(tud->tud_count <= UPDATE_PER_RPC_MAX);
	rc = osp_remote_sync(env, dt_dev, th->th_update_buf, &req, tud);
	if (rc == 0) {
		rc = osp_update_transno_xid(th->th_update_buf, req);
		ptlrpc_req_finished(req);
	}

	cfs_list_del(&tud->tud_list);
	OBD_FREE_PTR(tud);

	RETURN(rc);
}

/**
 * In DNE phase I, all remote updates will be packed into RPC (the format
 * description is in lustre_idl.h) during declare phase, all of updates
 * are attached to the transaction, one entry per OSP. Then in trans start,
 * LOD will walk through these entries and send these UPDATEs to the remote
 * MDT to be executed synchronously.
 */
int osp_trans_start(const struct lu_env *env, struct dt_device *dt_dev,
		    struct thandle *th)
{
	struct thandle_update_dt *tud;

	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);
#if 0
	/* In phase I, if the transaction includes remote updates, the local
	 * update should be synchronized, so it will set th_sync = 1 */
	LASSERT(tud->tud_count > 0);
	LASSERT(tud->tud_count <= UPDATE_PER_RPC_MAX);
	rc = osp_remote_sync(env, dt, th->th_update_buf, NULL, tud);
	th->th_sync = 1;
#endif
	RETURN(0);
}

static inline void osp_md_add_update_batchid(struct thandle *handle)
{
	handle->th_batchid++;
}

/**
 * Find one loc in th_dev for the thandle_update_dt,
 * Because only one thread can access this thandle, no need
 * lock now.
 */
static struct thandle_update_dt
*osp_find_create_update_loc(const struct lu_env *env, struct thandle *th,
			    struct dt_object *dt)
{
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct thandle_update_dt *tud;
	ENTRY;

	if (th->th_update_buf == NULL) {
		int rc;

		th->th_update_buf = update_buf_alloc();
		if (th->th_update_buf == NULL)
			RETURN(ERR_PTR(-ENOMEM));
		th->th_update_buf_size = UPDATE_BUFFER_SIZE;
		th->th_update = 1;
		rc = dt_trans_update_declare_llog_add(env, th);
		if (rc != 0)
			RETURN(ERR_PTR(rc));
	}

	tud = osp_find_update(th, dt_dev);
	if (tud != NULL)
		RETURN(tud);

	OBD_ALLOC_PTR(tud);
	if (tud == NULL)
		return ERR_PTR(-ENOMEM);

	CFS_INIT_LIST_HEAD(&tud->tud_list);
	tud->tud_dt = dt_dev;

	cfs_list_add_tail(&tud->tud_list, &th->th_remote_update_list);
	th->th_sync = 1;
	RETURN(tud);
}

static int osp_get_attr_from_req(const struct lu_env *env,
				 struct ptlrpc_request *req,
				 struct lu_attr *attr, int index)
{
	struct update_reply_buf	*reply_buf;
	struct obdo		*lobdo = &osp_env_info(env)->osi_obdo;
	struct obdo		*wobdo;
	int			size;

	LASSERT(attr != NULL);

	reply_buf = req_capsule_server_sized_get(&req->rq_pill,
						 &RMF_UPDATE_REPLY,
						 UPDATE_BUFFER_SIZE);
	if (reply_buf->urb_version != UPDATE_REPLY_V2)
		return -EPROTO;

	size = update_get_reply_data(req, reply_buf, (void **)&wobdo, index);
	if (size < 0)
		return size;
	else if (size != sizeof(struct obdo))
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
	struct thandle_update_dt *tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}
	
	return 0;
}

static int osp_md_object_create(const struct lu_env *env, struct dt_object *dt,
				struct lu_attr *attr,
				struct dt_allocation_hint *hint,
				struct dt_object_format *dof,
				struct thandle *th)
{
	struct osp_object	 *obj = dt2osp_obj(dt);
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct thandle_update_dt *tud;
	int			 rc;

	CDEBUG(D_INFO, "create obj "DFID"\n", PFID(lu_object_fid(&dt->do_lu)));

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

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
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)));

		rc = dt_trans_update_ref_del(env, dt, th);
		if (rc != 0)
			GOTO(out, rc);
		tud->tud_count++;

		if (S_ISDIR(lu_object_attr(&dt->do_lu))) {
			/* decrease for ".." */
			rc = dt_trans_update_ref_del(env, dt, th);
			if (rc != 0)
				GOTO(out, rc);
			tud->tud_count++;
		}

		rc = dt_trans_update_object_destroy(env, dt, th);
		if (rc != 0)
			GOTO(out, rc);
		tud->tud_count++;

		dt->do_lu.lo_header->loh_attr &= ~LOHA_EXISTS;
		/* Increase batchid to add this orphan object deletion
		 * to separate transaction */
		osp_md_add_update_batchid(th);
	}

	rc = dt_trans_update_create(env, dt, attr, hint, dof, th);
	if (rc != 0)
		GOTO(out, rc);
	tud->tud_count++;

	/* Because the create update RPC will be sent during declare phase,
	 * if creation reaches here, it means the object has been created
	 * successfully */
	dt->do_lu.lo_header->loh_attr |= LOHA_EXISTS | (attr->la_mode & S_IFMT);
	obj->opo_empty = 1;

out:
	if (rc)
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);

	return rc;
}

static int osp_md_declare_object_ref_del(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct thandle_update_dt	*tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		      (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}

	return 0;
}

static int osp_md_object_ref_del(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, "ref del "DFID"\n", PFID(lu_object_fid(&dt->do_lu)));
	rc = dt_trans_update_ref_del(env, dt, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;
	return 0;
}

static int osp_md_declare_ref_add(const struct lu_env *env,
				  struct dt_object *dt, struct thandle *th)
{
	struct thandle_update_dt *tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}

	return 0;
}

static int osp_md_object_ref_add(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, "ref add "DFID"\n", PFID(lu_object_fid(&dt->do_lu)));

	rc = dt_trans_update_ref_add(env, dt, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;

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
	struct thandle_update_dt *tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}
	return 0;
}

static int osp_md_attr_set(const struct lu_env *env, struct dt_object *dt,
			   const struct lu_attr *attr, struct thandle *th,
			   struct lustre_capa *capa)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, "attr set "DFID"\n", PFID(lu_object_fid(&dt->do_lu)));
	rc = dt_trans_update_attr_set(env, dt, attr, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;
	return 0;
}

static int osp_md_declare_xattr_set(const struct lu_env *env,
				    struct dt_object *dt,
				    const struct lu_buf *buf,
				    const char *name, int flag,
				    struct thandle *th)
{
	struct thandle_update_dt *tud;

	LASSERT(buf->lb_len > 0 && buf->lb_buf != NULL);
	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}

	return 0;
}

static int osp_md_xattr_set(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_buf *buf, const char *name, int fl,
			    struct thandle *th, struct lustre_capa *capa)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, "xattr %s set obj "DFID"\n", name,
	       PFID(lu_object_fid(&dt->do_lu)));

	rc = dt_trans_update_xattr_set(env, dt, buf, name, fl, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;
	return 0;
}

static int osp_md_xattr_get(const struct lu_env *env, struct dt_object *dt,
			    struct lu_buf *buf, const char *name,
			    struct lustre_capa *capa)
{
	struct dt_device	*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct ptlrpc_request	*req = NULL;
	int			rc;
	struct update_reply_buf	*reply_buf;
	void			*ea_buf;
	struct update_buf	*ubuf;
	int			size;
	ENTRY;

	ubuf = update_buf_alloc();
	if (ubuf == NULL)
		RETURN(-ENOMEM);

	LASSERT(name != NULL);
	rc = dt_update_xattr_get(env, ubuf, UPDATE_BUFFER_SIZE, dt,
				 (char *)name);
	if (rc != 0) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	rc = osp_remote_sync(env, dt_dev, ubuf, &req, NULL);
	if (rc != 0)
		GOTO(out, rc);

	reply_buf = req_capsule_server_sized_get(&req->rq_pill,
						 &RMF_UPDATE_REPLY,
						 UPDATE_BUFFER_SIZE);
	if (reply_buf->urb_version != UPDATE_REPLY_V2) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       reply_buf->urb_version, UPDATE_REPLY_V2, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	size = update_get_reply_data(req, reply_buf, &ea_buf, 0);
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

	if (ubuf != NULL)
		update_buf_free(ubuf);

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
	struct update_buf	*ubuf;
	struct ptlrpc_request	*req = NULL;
	int			size;
	int			rc;
	struct update_reply_buf	*reply_buf;
	struct lu_fid		*fid;

	ENTRY;

	ubuf = update_buf_alloc();
	if (ubuf == NULL)
		RETURN(-ENOMEM);

	rc = dt_update_index_lookup(env, ubuf, UPDATE_BUFFER_SIZE, dt, rec,
				    key);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = osp_remote_sync(env, dt_dev, ubuf, &req, NULL);
	if (rc < 0)
		GOTO(out, rc);

	reply_buf = req_capsule_server_sized_get(&req->rq_pill,
						 &RMF_UPDATE_REPLY,
						 UPDATE_BUFFER_SIZE);
	if (reply_buf->urb_version != UPDATE_REPLY_V2) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       reply_buf->urb_version, UPDATE_REPLY_V2, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	size = update_get_reply_data(req, reply_buf, (void **)&fid, 0);
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

	if (ubuf != NULL)
		update_buf_free(ubuf);

	RETURN(rc);
}

static int osp_md_declare_insert(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key, struct thandle *th)
{
	struct thandle_update_dt *tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}

	return 0;
}

static int osp_md_index_insert(const struct lu_env *env, struct dt_object *dt,
			       const struct dt_rec *rec,
			       const struct dt_key *key, struct thandle *th,
			       struct lustre_capa *capa, int ignore_quota)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, DFID"index insert %s: "DFID"\n",
	       PFID(lu_object_fid(&dt->do_lu)), (char *)key,
	       PFID((struct lu_fid *)rec));

	rc = dt_trans_update_index_insert(env, dt, rec, key, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;
	return 0;
}

static int osp_md_declare_delete(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_key *key, struct thandle *th)
{
	struct thandle_update_dt *tud;

	tud = osp_find_create_update_loc(env, th, dt);
	if (IS_ERR(tud)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(tud));
		return PTR_ERR(tud);
	}

	return 0;
}

static int osp_md_index_delete(const struct lu_env *env,
			       struct dt_object *dt,
			       const struct dt_key *key,
			       struct thandle *th,
			       struct lustre_capa *capa)
{
	struct thandle_update_dt *tud;
	struct dt_device	 *dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	int rc;

	LASSERT(th->th_update && th->th_update_buf != NULL);
	tud = osp_find_update(th, dt_dev);
	LASSERT(tud != NULL);

	CDEBUG(D_INFO, "index delete "DFID" %s\n",
	       PFID(&dt->do_lu.lo_header->loh_fid), (char *)key);

	rc = dt_trans_update_index_delete(env, dt, key, th);
	if (rc != 0)
		return rc;

	tud->tud_count++;
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
	struct update_buf     *ubuf;
	struct ptlrpc_request *req = NULL;
	int rc;
	ENTRY;

	ubuf = update_buf_alloc();
	if (ubuf == NULL)
		RETURN(-ENOMEM);

	rc = dt_update_attr_get(env, ubuf, UPDATE_BUFFER_SIZE, dt);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}
	dt_dev = lu2dt_dev(dt->do_lu.lo_dev);

	rc = osp_remote_sync(env, dt_dev, ubuf, &req, NULL);
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

	if (ubuf != NULL)
		update_buf_free(ubuf);

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
