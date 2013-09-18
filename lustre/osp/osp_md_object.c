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
			struct update_buf *ubuf, struct ptlrpc_request **reqp,
			int op)
{
	struct obd_import      *imp;
	struct ptlrpc_request  *req = NULL;
	struct update_buf      *tmp;
	int			rc;
	int			ubuf_len = update_buf_size(ubuf);
	ENTRY;

	imp = osp->opd_obd->u.cli.cl_import;
	LASSERT(imp);

	switch (op) {
	case UPDATE_LOG_CANCEL:
		req = ptlrpc_request_alloc(imp, &RQF_UPDATE_LOG_CANCEL);
		break;
	case UPDATE_OBJ:
		req = ptlrpc_request_alloc(imp, &RQF_UPDATE_OBJ);
		break;
	default:
		CERROR("%s: unknown op %d\n", osp->opd_obd->obd_name,
		       op);
		LBUG();
		break;
	}
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_UPDATE, RCL_CLIENT, ubuf_len);

	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, op);
	if (rc != 0) {
		ptlrpc_req_finished(req);
		RETURN(rc);
	}

	req_capsule_set_size(&req->rq_pill, &RMF_UPDATE_REPLY, RCL_SERVER,
			     UPDATE_BUFFER_SIZE);

	update_dump_buf(ubuf);
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

	rc = osp_prep_update_req(env, osp, ubuf, &req, UPDATE_OBJ);
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

static int osp_fld_lookup(const struct lu_env *env, struct osp_device *osp,
			  seqno_t seq, struct lu_seq_range *range)
{
	struct seq_server_site	*ss = osp_seq_site(osp);
	int			rc;

	if (fid_seq_is_idif(seq)) {
		fld_range_set_ost(range);
		range->lsr_index = idif_ost_idx(seq);
		return 0;
	}

	if (!fid_seq_in_fldb(seq)) {
		fld_range_set_mdt(range);
		if (ss != NULL)
			/* FIXME: If ss is NULL, it suppose not get lsr_index
			 * at all */
			range->lsr_index = ss->ss_node_id;
		return 0;
	}

	LASSERT(ss != NULL);
	fld_range_set_any(range);
	rc = fld_server_lookup(env, ss->ss_server_fld, seq, range);
	if (rc != 0) {
		CERROR("%s: cannot find FLD range for "LPX64": rc = %d\n",
		       osp->opd_obd->obd_name, seq, rc);
	}
	return rc;
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
*osp_find_update(struct thandle_update *tu, struct dt_device *dt_dev)
{
	struct thandle_update_dt   *tud;

	LASSERT(tu != NULL);
	cfs_list_for_each_entry(tud, &tu->tu_remote_update_list, tud_list) {
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
	struct thandle_update	 *tu = (struct thandle_update *)th;
	struct thandle_update_dt *tud;
	struct ptlrpc_request	 *req = NULL;
	struct osp_device	 *osp = dt2osp_dev(dt_dev);
	int			 rc = 0;
	ENTRY;

	LASSERT(tu != NULL);
	tud = osp_find_update(tu, dt_dev);
	if (tud == NULL)
		RETURN(rc);

	if (tu->tu_result != 0)
		GOTO(out, rc = tu->tu_result);

	LASSERT(tud->tud_count > 0);
	LASSERT(tud->tud_count <= UPDATE_PER_RPC_MAX);

	rc = osp_prep_update_req(env, osp, tu->tu_update_buf, &req, UPDATE_OBJ);
	if (rc)
		GOTO(out, rc);

	rc = ptlrpc_queue_wait(req);
	if (rc < 0)
		GOTO(out, rc);

	if (lustre_msg_get_last_committed(req->rq_repmsg) >
	    osp->opd_last_committed_transno) {
		/* If last committed transno changed, wakeup
		 * update log thread */
		osp->opd_last_committed_transno =
			lustre_msg_get_last_committed(req->rq_repmsg);
		osp->opd_new_committed = 1;
		wake_up(&osp->opd_update_waitq);

		CDEBUG(D_HA, "%s: trans "LPU64" wake up update log thread\n",
		       osp->opd_obd->obd_name, osp->opd_last_committed_transno);
	}
	rc = osp_update_transno_xid(tu->tu_update_buf, req);
out:
	cfs_list_del(&tud->tud_list);
	OBD_FREE_PTR(tud);
	if (req != NULL)
		ptlrpc_req_finished(req);
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

	tud = osp_find_update(th->th_update, dt_dev);
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
	handle->th_update->tu_batchid++;
}

static int osp_update_stop_txn_cb(const struct lu_env *env,
				  struct thandle *th, void *data)
{
	struct llog_cookie	*cookie = &osp_env_info(env)->osi_cookie;
	struct dt_device	*dt_dev = (struct dt_device *)data;
	struct lu_seq_range	*range = &osp_env_info(env)->osi_seq;
	struct osp_device	*osp = dt2osp_dev(dt_dev);
	struct update_buf	*ubuf;
	int i;
	int rc;
	ENTRY;

	LASSERT(dt_dev != NULL);
	LASSERT(th->th_update != NULL);
	ubuf = th->th_update->tu_update_buf;
	LASSERT(ubuf != NULL);

	rc = dt_trans_update_llog_add(env, osp->opd_storage, ubuf, cookie,
				      osp->opd_index, th);
	if (rc != 0) {
		CERROR("%s: update llog failed: rc = %d\n",
		       (dt2osp_dev(dt_dev))->opd_obd->obd_name, rc);
		RETURN(rc);
	}

	for (i = 0; i < ubuf->ub_count; i++) {
		struct update *update = update_buf_get(ubuf, i, NULL);

		rc = osp_fld_lookup(env, osp, fid_seq(&update->u_fid), range);
		if (rc != 0) {
			CDEBUG(D_ERROR, "%s:"DFID"seq lookup failed\n",
			       osp->opd_obd->obd_name,
			       PFID(&update->u_fid));
			continue;
		}

		if (osp->opd_group == range->lsr_index)
			update->u_cookie = *cookie;
	}

	RETURN(rc);
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
	struct dt_device		*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct osp_device		*osp = dt2osp_dev(dt_dev);
	struct thandle_update_dt	*tud = NULL;
	int				allocated = 0;
	int				rc;
	ENTRY;

	if (th->th_update == NULL) {
		OBD_ALLOC_PTR(th->th_update);
		if (th->th_update == NULL)
			RETURN(ERR_PTR(-ENOMEM));
		th->th_update->tu_update_buf = update_buf_alloc();
		if (th->th_update->tu_update_buf == NULL) {
			OBD_FREE_PTR(th->th_update);
			th->th_update = NULL;
			RETURN(ERR_PTR(-ENOMEM));
		}
		CFS_INIT_LIST_HEAD(&th->th_update->tu_remote_update_list);
		th->th_update->tu_update_buf_size = UPDATE_BUFFER_SIZE;
		th->th_record_update = 1;
		th->th_update->tu_master_index = osp->opd_group;
		allocated = 1;
	}

	tud = osp_find_update(th->th_update, dt_dev);
	if (tud != NULL)
		RETURN(tud);

	rc = dt_trans_update_declare_llog_add(env, osp->opd_storage, th,
					      osp->opd_index);
	if (rc != 0)
		GOTO(out, rc);

	OBD_ALLOC_PTR(tud);
	if (tud == NULL)
		GOTO(out, rc = -ENOMEM);

	CFS_INIT_LIST_HEAD(&tud->tud_list);
	tud->tud_dt = dt_dev;
	tud->tud_txn_stop_cb = osp_update_stop_txn_cb;
	tud->tud_cb_data = dt_dev;
	cfs_list_add_tail(&tud->tud_list,
			  &th->th_update->tu_remote_update_list);
	th->th_sync = 1;
out:
	if (rc != 0) {
		if (allocated == 1) {
			update_buf_free(th->th_update->tu_update_buf);
			OBD_FREE_PTR(th->th_update);
			th->th_update = NULL;
		}
		if (tud != NULL)
			OBD_FREE_PTR(tud);

		tud = ERR_PTR(rc);
	}
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
	if (reply_buf == NULL || reply_buf->urb_version != UPDATE_REPLY_V2)
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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
				 (char *)name, dt2osp_dev(dt_dev)->opd_group);
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
				    key, dt2osp_dev(dt_dev)->opd_group);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	LASSERT(th->th_update && th->th_update->tu_update_buf != NULL);
	tud = osp_find_update(th->th_update, dt_dev);
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

	rc = dt_update_attr_get(env, ubuf, UPDATE_BUFFER_SIZE, dt,
				dt2osp_dev(dt_dev)->opd_group);
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

static int osp_update_llog_init(const struct lu_env *env,
				struct osp_device *osp)
{
	struct osp_thread_info	*osi = osp_env_info(env);
	struct dt_device	*dt = osp->opd_storage;
	struct obd_llog_group	*olg;
	struct llog_ctxt	*ctxt;
	int			rc;
	ENTRY;

	CDEBUG(D_HA, "%s: init update log %d\n", osp->opd_obd->obd_name,
	       osp->opd_index);
	rc = dt_update_llog_init(env, dt, osp->opd_index,
				 &osp_mds_ost_orig_logops);
	if (rc != 0)
		RETURN(rc);

	/* add generation log, so llog_update_process thread
	 * can process records during initialization */
	osi->osi_gen.lgr_hdr.lrh_type = LLOG_GEN_REC;
	osi->osi_gen.lgr_hdr.lrh_len = sizeof(osi->osi_gen);

	olg = dt_update_find_olg(osp->opd_storage, osp->opd_index);
	if (olg == NULL) {
		CERROR("%s: can't get appropriate context: rc = %d\n",
		       osp->opd_obd->obd_name, -EINVAL);
		GOTO(out, rc = -EINVAL);
	}

	ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL) {
		CERROR("%s: can't get appropriate context: rc = %d\n",
		       osp->opd_obd->obd_name, -EINVAL);
		GOTO(out, rc = -EINVAL);
	}

	rc = llog_cat_add(env, ctxt->loc_handle, &osi->osi_gen.lgr_hdr,
			  NULL, NULL);
	llog_ctxt_put(ctxt);
out:
	if (rc != 0)
		dt_update_llog_fini(env, dt, osp->opd_index);
	RETURN(rc);
}

static void osp_update_llog_fini(const struct lu_env *env,
				 struct osp_device *osp)
{
	struct dt_device	*dt = osp->opd_storage;

	return dt_update_llog_fini(env, dt, osp->opd_index);
}

static inline int osp_update_running(struct osp_device *d)
{
	return !!(d->opd_update_thread.t_flags & SVC_RUNNING);
}

static inline int osp_update_stopped(struct osp_device *d)
{
	return !!(d->opd_update_thread.t_flags & SVC_STOPPED);
}

#define MAXIM_UPDATE_LOG_PER_RPC 2
static int osp_cancel_remote_log(const struct lu_env *env,
				 struct osp_device *osp,
				 struct update_buf *ubuf,
				 struct llog_cookie *rcookie)
{
	struct lu_fid *fid = &osp_env_info(env)->osi_fid;
	int rc;
	int size = sizeof(*rcookie);
	ENTRY;

	CDEBUG(D_HA, "%s: cancel remote cookie "DOSTID": %x\n",
	       osp->opd_obd->obd_name, POSTID(&rcookie->lgc_lgl.lgl_oi),
	       rcookie->lgc_lgl.lgl_ogen);

	logid_to_fid(&rcookie->lgc_lgl, fid);
	rc = update_insert(env, ubuf, UPDATE_BUFFER_SIZE, OBJ_LOG_CANCEL,
			   fid, 1, &size, (char **)&rcookie, 0, osp->opd_group);
	if (ubuf->ub_count >= MAXIM_UPDATE_LOG_PER_RPC) {
		struct ptlrpc_request	*req;
		int			ulen = update_buf_size(ubuf);
		struct l_wait_info	lwi = { 0 };

		do {
			rc = osp_prep_update_req(env, osp, ubuf, &req,
						 UPDATE_LOG_CANCEL);
			if (rc)
				GOTO(out, rc);

			rc = ptlrpc_queue_wait(req);

			if (rc != 0) {
				ptlrpc_req_finished(req);
				CDEBUG(D_HA, "%s: cancel "DOSTID
				       ":%x: rc = %d\n", osp->opd_obd->obd_name,
				       POSTID(&rcookie->lgc_lgl.lgl_oi),
				       rcookie->lgc_lgl.lgl_ogen, rc);
				l_wait_event(osp->opd_update_waitq,
					     !osp_update_running(osp) ||
					     osp->opd_imp_connected, &lwi);
			}
		} while (rc != 0);
		ptlrpc_req_finished(req);
		memset(ubuf, 0, ulen);
		update_buf_init(ubuf);
	}
out:
	RETURN(rc);
}

struct update_process_args {
	struct osp_device	*upa_osp;
	struct update_buf	*upa_ubuf;
};

static inline int osp_update_can_process_new(const struct lu_env *env,
					     struct osp_device *osp,
					     struct llog_handle *llh,
					     struct llog_rec_hdr *rec,
					     struct llog_cookie **cookie)
{
	struct llog_updatelog_rec *urec = (struct llog_updatelog_rec *)rec;
	struct update_buf	  *ubuf = &urec->urb;
	struct obd_import	  *imp = osp->opd_obd->u.cli.cl_import;
	struct lu_seq_range	  *range = &osp_env_info(env)->osi_seq;
	__u64			  transno = osp_last_local_committed(osp);
	__u64			  peer_transno;
	int			  committed = 1;
	int			  i;
	int			  rc;
	ENTRY;

	LASSERT(osp);
	if (!osp->opd_imp_connected)
		RETURN(0);

	if (llh->lgh_hdr->llh_count == 0)
		RETURN(0);

	if (unlikely(rec->lrh_type == LLOG_GEN_REC))
		RETURN(1);

	peer_transno = imp->imp_peer_committed_transno;
	update_buf_le_to_cpu(ubuf, ubuf);
	update_dump_buf(ubuf);
	CDEBUG(D_HA, "%s: local committed "LPU64" peer committed "LPU64"\n",
	       osp->opd_obd->obd_name, transno, peer_transno);
	for (i = 0; i < ubuf->ub_count; i++) {
		struct update *update = update_buf_get(ubuf, i, NULL);

		rc = osp_fld_lookup(env, osp, fid_seq(&update->u_fid),
				    range);
		if (rc != 0) {
			CDEBUG(D_HA, "%s:"DFID"seq lookup failed\n",
			       osp->opd_obd->obd_name,
			       PFID(&update->u_fid));
			committed = 0;
			break;
		}

		if (osp->opd_group == range->lsr_index) {
			if (update->u_batchid == 0) {
				committed = 0;
				break;
			}
			if (transno < update->u_batchid) {
				committed = 0;
				break;
			}
		}

		if (osp->opd_index == range->lsr_index) {
			if (update->u_batchid == 0) {
				committed = 0;
				break;
			}
			if (peer_transno < update->u_batchid) {
				committed = 0;
				break;
			}
			if (cookie != NULL)
				*cookie = &update->u_cookie;
		}
	}
	RETURN(committed);
}

/**
 * It will check whether all of updates for the operation have
 * been committed on all of MDTs, by the transno it collects from
 * all other MDT (by each osp reply).
 *
 * The update log on slave MDT log should include the transno of
 * operation on the master MDT. The slave MDT will destroy log rec
 * once it finds the update has been committed on Master MDT, then
 * it will send RPC to Master MDT to destroy the update log there.
 **/
static int osp_update_process_queues(const struct lu_env *env,
				     struct llog_handle *llh,
				     struct llog_rec_hdr *rec,
				     void *data)
{
	struct update_process_args *upa = (struct update_process_args *)data;
	struct osp_device	   *osp = upa->upa_osp;
	struct update_buf	   *cancel_ubuf = upa->upa_ubuf;
	struct llog_cookie	   *rcookie = NULL;
	int			   rc = 0;
	ENTRY;

	if (!osp_update_running(osp)) {
		CDEBUG(D_HA, "%s: stop llog processing\n",
		       osp->opd_obd->obd_name);
		RETURN(LLOG_PROC_BREAK);
	}

	if (osp_update_can_process_new(env, osp, llh, rec, &rcookie)) {
		struct llog_cookie	*lcookie;

		if (unlikely(rec->lrh_type == LLOG_GEN_REC)) {
			/* cancel any generation record */
			lcookie = &osp_env_info(env)->osi_cookie;
			lcookie->lgc_lgl = llh->lgh_id;
			lcookie->lgc_subsys = LLOG_UPDATE_ORIG_CTXT;
			lcookie->lgc_index = rec->lrh_index;
			CDEBUG(D_HA, "%s: cancel gen log "DOSTID": %x\n",
			       osp->opd_obd->obd_name,
			       POSTID(&lcookie->lgc_lgl.lgl_oi),
			       lcookie->lgc_lgl.lgl_ogen);
			rc = llog_cat_cancel_records(env,
					llh->u.phd.phd_cat_handle,
					1, lcookie);
		} else {
			LASSERT(rcookie != NULL);
			rc = osp_cancel_remote_log(env, osp, cancel_ubuf,
						   rcookie);
			if (rc != 0) {
				CDEBUG(D_HA, "%s: cancel "DOSTID": %x failed:"
				       " rc = %d\n", osp->opd_obd->obd_name,
				       POSTID(&rcookie->lgc_lgl.lgl_oi),
				       rcookie->lgc_lgl.lgl_ogen, rc);
			}

			/* cancel local record */
			lcookie = &osp_env_info(env)->osi_cookie;
			lcookie->lgc_lgl = llh->lgh_id;
			lcookie->lgc_subsys = LLOG_UPDATE_ORIG_CTXT;
			lcookie->lgc_index = rec->lrh_index;
			rc = llog_cat_cancel_records(env,
						llh->u.phd.phd_cat_handle,
						1, lcookie);
			if (rc != 0) {
				CDEBUG(D_HA, "%s: cancel "DOSTID": %x failed:"
				       " rc = %d\n", osp->opd_obd->obd_name,
				       POSTID(&lcookie->lgc_lgl.lgl_oi),
				       lcookie->lgc_lgl.lgl_ogen, rc);
			}
		}
	}

	RETURN(rc);
}

static int osp_update_thread(void *_arg)
{
	struct osp_device	*osp = _arg;
	struct l_wait_info	lwi = { 0 };
	struct ptlrpc_thread	*thread = &osp->opd_update_thread;
	struct llog_ctxt	*ctxt;
	struct llog_handle	*llh;
	struct obd_llog_group	*olg;
	struct update_process_args upa;
	struct lu_env		 env;
	int			 rc;

	ENTRY;

	rc = lu_env_init(&env, osp->opd_dt_dev.dd_lu_dev.ld_type->ldt_ctx_tags);
	if (rc) {
		CERROR("%s: init env error: rc = %d\n", osp->opd_obd->obd_name,
		       rc);
		RETURN(rc);
	}

	thread->t_flags = SVC_RUNNING;
	wake_up(&thread->t_ctl_waitq);

	olg = dt_update_find_olg(osp->opd_storage, osp->opd_index);
	if (olg == NULL) {
		CERROR("%s: can't get appropriate context: rc = %d\n",
		       osp->opd_obd->obd_name, -EINVAL);
		GOTO(out, rc = -EINVAL);
	}
	ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL) {
		CERROR("%s: can't get appropriate context: rc = %d\n",
		       osp->opd_obd->obd_name, -EINVAL);
		GOTO(out, rc = -EINVAL);
	}

	llh = ctxt->loc_handle;
	if (llh == NULL) {
		llog_ctxt_put(ctxt);
		GOTO(out, rc = -EINVAL);
	}

	upa.upa_osp = osp;
	upa.upa_ubuf = update_buf_alloc();

	do {
		rc = llog_cat_process(&env, llh, osp_update_process_queues,
				      &upa, 0, 0);
		osp->opd_new_committed = 0;
		l_wait_event(osp->opd_update_waitq,
			     !osp_update_running(osp) || osp->opd_new_committed,
			     &lwi);
		if (!osp_update_running(osp))
			break;
	} while (1);
	/* we don't expect llog_process_thread() to exit till umount */
	LASSERT(thread->t_flags != SVC_RUNNING);
	update_buf_free(upa.upa_ubuf);
	llog_ctxt_put(ctxt);
out:
	thread->t_flags = SVC_STOPPED;

	wake_up(&thread->t_ctl_waitq);

	lu_env_fini(&env);

	RETURN(0);
}

int osp_update_init(const struct lu_env *env, struct osp_device *osp)
{
	struct l_wait_info	lwi = { 0 };
	int			rc;
	ENTRY;

	rc = osp_update_llog_init(env, osp);
	if (rc != 0)
		RETURN(rc);

	/*
	 * Start synchronization thread
	 */
	init_waitqueue_head(&osp->opd_update_waitq);
	init_waitqueue_head(&osp->opd_update_thread.t_ctl_waitq);
	rc = PTR_ERR(kthread_run(osp_update_thread, osp,
				 "osp-update-%u-%u", osp->opd_index,
				 osp->opd_group));
	if (IS_ERR_VALUE(rc)) {
		CERROR("%s: can't start update thread: rc = %d\n",
		       osp->opd_obd->obd_name, rc);
		GOTO(err_llog, rc);
	}

	l_wait_event(osp->opd_update_thread.t_ctl_waitq,
		     osp_update_running(osp) || osp_update_stopped(osp), &lwi);

	RETURN(0);
err_llog:
	osp_update_llog_fini(env, osp);
	return rc;
}

int osp_update_fini(const struct lu_env *env, struct osp_device *osp)
{
	struct ptlrpc_thread *thread = &osp->opd_update_thread;

	ENTRY;

	thread->t_flags = SVC_STOPPING;
	wake_up(&osp->opd_update_waitq);
	wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_STOPPED);

	osp_update_llog_fini(env, osp);
	return 0;
}

