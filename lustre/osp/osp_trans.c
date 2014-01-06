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
 * lustre/osp/osp_trans.c
 *
 * Author: Di Wang <di.wang@intel.com>
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "osp_internal.h"

struct osp_async_update_args {
	void	*oaua_data;
};

struct osp_dummy_update_item {
	struct list_head		 odui_list;
	struct osp_object		*odui_obj;
	osp_dummy_update_interpterer_t	 odui_interpterer;
};

static struct osp_dummy_update_item *
osp_dummy_update_item_init(struct osp_object *obj,
			   osp_dummy_update_interpterer_t interpterer)
{
	struct osp_dummy_update_item *odui;

	OBD_ALLOC_PTR(odui);
	if (odui == NULL)
		return NULL;

	lu_object_get(osp2lu_obj(obj));
	INIT_LIST_HEAD(&odui->odui_list);
	odui->odui_obj = obj;
	odui->odui_interpterer = interpterer;

	return odui;
}

static void osp_dummy_update_item_fini(const struct lu_env *env,
				       struct osp_dummy_update_item * odui)
{
	LASSERT(list_empty(&odui->odui_list));

	lu_object_put(env, osp2lu_obj(odui->odui_obj));
	OBD_FREE_PTR(odui);
}

static int osp_dummy_update_interpret(const struct lu_env *env,
				      struct ptlrpc_request *req,
				      void *arg, int rc)
{
	struct update_reply		*reply	= NULL;
	struct osp_async_update_args	*oaua	= arg;
	struct update_request		*update = oaua->oaua_data;
	struct osp_dummy_update_item	*odui;
	struct osp_dummy_update_item	*next;
	int				 count	= 0;
	int				 index  = 0;
	int				 rc1	= 0;

	if (rc == 0 || req->rq_repmsg != NULL) {
		reply = req_capsule_server_sized_get(&req->rq_pill,
						     &RMF_UPDATE_REPLY,
						     UPDATE_BUFFER_SIZE);
		if (reply == NULL || reply->ur_version != UPDATE_REPLY_V1)
			rc1 = -EPROTO;
		else
			count = reply->ur_count;
	} else {
		rc1 = rc;
	}

	list_for_each_entry_safe(odui, next, &update->ur_callback_list,
				 odui_list) {
		list_del_init(&odui->odui_list);
		if (index < count && reply->ur_lens[index] > 0) {
			char *ptr = update_get_buf_internal(reply, index, NULL);

			LASSERT(ptr != NULL);

			rc1 = le32_to_cpu(*(int *)ptr);
		} else {
			rc1 = rc;
			if (unlikely(rc1 == 0))
				rc1 = -EINVAL;
		}

		odui->odui_interpterer(env, reply, odui->odui_obj, index, rc1);
		osp_dummy_update_item_fini(env, odui);
		index++;
	}

	out_destroy_update_req(update);

	return 0;
}

static int osp_update_interpret(const struct lu_env *env,
				struct ptlrpc_request *req,
				void *arg, int rc)
{
	struct osp_async_update_args	*oaua	= arg;
	struct update_request		*update = oaua->oaua_data;
	struct dt_txn_commit_cb 	*cb	= update->ur_cb;

	LASSERT(cb != NULL);
	LASSERT(cb->dcb_func != NULL);

	cb->dcb_func(NULL, NULL, cb, rc);
	out_destroy_update_req(update);

	return 0;
}

static int osp_sync_dummy_update(const struct lu_env *env,
				 struct osp_device *osp,
				 struct update_request *update)
{
	struct osp_async_update_args	*args;
	struct ptlrpc_request		*req = NULL;
	int				 rc;

	rc = out_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
				 update->ur_buf, UPDATE_BUFFER_SIZE, &req);
	if (rc != 0) {
		struct osp_dummy_update_item *odui;
		struct osp_dummy_update_item *next;

		list_for_each_entry_safe(odui, next,
					 &update->ur_callback_list,
					 odui_list) {
			list_del_init(&odui->odui_list);
			osp_dummy_update_item_fini(env, odui);
		}
		out_destroy_update_req(update);

		return rc;
	}

	list_del_init(&update->ur_list);
	args = ptlrpc_req_async_args(req);
	args->oaua_data = update;
	req->rq_interpret_reply = osp_dummy_update_interpret;
	ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);

	return 0;
}

struct update_request *
osp_find_or_create_dummy_update_req(struct osp_device *osp)
{
	struct thandle		*th	= &osp->opd_dummy_th;
	struct update_request	*update = th->th_current_request;

	if (update != NULL)
		return update;

	update = out_create_update_req(&osp->opd_dt_dev);
	if (!IS_ERR(update))
		th->th_current_request = update;

	return update;
}

int osp_insert_dummy_update(const struct lu_env *env,
			    struct update_request *update, int op,
			    struct osp_object *obj, int count,
			    int *lens, const char **bufs,
			    osp_dummy_update_interpterer_t interpterer)
{
	struct osp_dummy_update_item *odui;
	struct osp_device	     *osp = lu2osp_dev(osp2lu_obj(obj)->lo_dev);
	struct thandle		     *th  = &osp->opd_dummy_th;
	int			      rc  = 0;
	ENTRY;

	odui = osp_dummy_update_item_init(obj, interpterer);
	if (odui == NULL)
		RETURN(-ENOMEM);

again:
	rc = out_insert_update(env, update, op, lu_object_fid(osp2lu_obj(obj)),
			       count, lens, bufs);
	if (rc == -E2BIG) {
		th->th_current_request = NULL;
		mutex_unlock(&osp->opd_dummy_th_mutex);

		rc = osp_sync_dummy_update(env, osp, update);
		mutex_lock(&osp->opd_dummy_th_mutex);
		if (rc != 0)
			GOTO(out, rc);

		update = out_create_update_req(&osp->opd_dt_dev);
		if (IS_ERR(update))
			GOTO(out, rc = PTR_ERR(update));

		goto again;
	}

	if (rc == 0)
		list_add_tail(&odui->odui_list, &update->ur_callback_list);

	GOTO(out, rc);

out:
	if (rc != 0)
		osp_dummy_update_item_fini(env, odui);

	return rc;
}

static int osp_dummy_trans_start(const struct lu_env *env, struct dt_device *dt,
				 struct thandle *th)
{
	struct osp_device	*osp	= dt2osp_dev(dt);
	struct update_request	*update;
	int			 rc;

	mutex_lock(&osp->opd_dummy_th_mutex);
	if (th->th_current_request == NULL) {
		mutex_unlock(&osp->opd_dummy_th_mutex);

		return 0;
	}

	update = th->th_current_request;
	LASSERT(update->ur_buf != NULL);

	if (update->ur_buf->ub_count == 0) {
		mutex_unlock(&osp->opd_dummy_th_mutex);

		return 0;
	}

	th->th_current_request = NULL;
	mutex_unlock(&osp->opd_dummy_th_mutex);

	rc = osp_sync_dummy_update(env, osp, update);

	return rc;
}

struct thandle *osp_dummy_trans_create(const struct lu_env *env,
				       struct dt_device *dt)
{
	struct osp_device *d = dt2osp_dev(dt);

	return &d->opd_dummy_th;
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
	struct osp_device     *osp	= dt2osp_dev(dt);
	struct update_request *update;
	int rc = 0;

	if (th->th_dummy)
		return osp_dummy_trans_start(env, dt, th);

	/* In phase I, if the transaction includes remote updates, the local
	 * update should be synchronized, so it will set th_sync = 1 */
	update = th->th_current_request;
	LASSERT(update != NULL && update->ur_dt == dt);

	if (update->ur_buf->ub_count > 0) {
		if (update->ur_cb != NULL) {
			struct osp_async_update_args	*args;
			struct ptlrpc_request		*req;

			rc = out_prep_update_req(env,
						 osp->opd_obd->u.cli.cl_import,
						 update->ur_buf,
						 UPDATE_BUFFER_SIZE, &req);
			if (rc != 0)
				return rc;

			list_del_init(&update->ur_list);
			args = ptlrpc_req_async_args(req);
			args->oaua_data = update;
			req->rq_interpret_reply = osp_update_interpret;
			ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
		} else {
			th->th_sync = 1;
			rc = out_remote_sync(env, osp->opd_obd->u.cli.cl_import,
					     update, NULL);
		}
	}

	return rc;
}

int osp_trans_stop(const struct lu_env *env, struct thandle *th)
{
	struct update_request	*update = th->th_current_request;
	int			 rc	= 0;

	if (update != NULL) {
		if (!th->th_dummy)
			rc = update->ur_rc;

		th->th_current_request = NULL;
		out_destroy_update_req(update);
	}

	return rc;
}

int osp_trans_cb_add(struct thandle *th, struct dt_txn_commit_cb *dcb)
{
	struct update_request *update;

	update = out_find_update(th, dcb->dcb_dev);
	if (update != NULL)
		update->ur_cb = dcb;

	return 0;
}
