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
 *
 * 1. OSP transaction methods
 *
 * Implement OSP layer transaction related interfaces for the dt_device API
 * dt_device_operations.
 *
 *
 * 2. Handle asynchronous idempotent operations
 *
 * The OSP uses OUT RPC to talk with other server (MDT or OST) for kinds of
 * operations, such as create, unlink, insert, delete, lookup, set_(x)attr,
 * get_(x)attr, and so on. To safe RPCs, we allow multiple operations to be
 * packaged together in single OUT RPC.
 *
 * For the asynchronous idempotent operations, they will be inserted into a
 * shared asynchronous request queue - osp_device::opd_async_requests. Once
 * the queue is full, it will be purged via triggering the packaged OUT RPC,
 * and subsequent asynchronous idempotent requests will re-fill the queue.
 *
 * When the request is inserted into the shared queue, it will register an
 * interpreter. And when the packaged OUT RPC is replied (or fail), it will
 * call every registered interpreter to handle related operation result.
 *
 *
 * Author: Di Wang <di.wang@intel.com>
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "osp_internal.h"

struct osp_async_update_args {
	struct dt_update_request *oaua_update;
	bool			  oaua_flow_control;
};

struct osp_async_request {
	/* list in the dt_update_request::dur_cb_items */
	struct list_head		 oar_list;

	/* The target of the async update request. */
	struct osp_object		*oar_obj;

	/* The data used by oar_interpreter. */
	void				*oar_data;

	/* The interpreter function called after the async request handled. */
	osp_async_request_interpreter_t	 oar_interpreter;
};

/**
 * Allocate an asynchronous request and initialize it with the given parameters.
 *
 * \param[in] obj		pointer to the operation target
 * \param[in] data		pointer to the data used by the interpreter
 * \param[in] interpreter	pointer to the interpreter function
 *
 * \retval			pointer to the asychronous request
 * \retval			NULL if the allocation is failued
 */
static struct osp_async_request *
osp_async_request_init(struct osp_object *obj, void *data,
		       osp_async_request_interpreter_t interpreter)
{
	struct osp_async_request *oar;

	OBD_ALLOC_PTR(oar);
	if (oar == NULL)
		return NULL;

	lu_object_get(osp2lu_obj(obj));
	INIT_LIST_HEAD(&oar->oar_list);
	oar->oar_obj = obj;
	oar->oar_data = data;
	oar->oar_interpreter = interpreter;

	return oar;
}

/**
 * Destroy the asychronous request.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] oar	pointer to asychronous request
 */
static void osp_async_request_fini(const struct lu_env *env,
				   struct osp_async_request *oar)
{
	LASSERT(list_empty(&oar->oar_list));

	lu_object_put(env, osp2lu_obj(oar->oar_obj));
	OBD_FREE_PTR(oar);
}

/**
 * Interpret the packaged OUT RPC results.
 *
 * For every packaged sub-request, call its registered interpreter function.
 * Then destroy the sub-request.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] req	pointer to the RPC
 * \param[in] arg	pointer to data used by the interpreter
 * \param[in] rc	the RPC return value
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
static int osp_async_update_interpret(const struct lu_env *env,
				      struct ptlrpc_request *req,
				      void *arg, int rc)
{
	struct object_update_reply	*reply	= NULL;
	struct osp_async_update_args	*oaua	= arg;
	struct dt_update_request	*dt_update = oaua->oaua_update;
	struct osp_async_request	*oar;
	struct osp_async_request	*next;
	int				 count	= 0;
	int				 index  = 0;
	int				 rc1	= 0;

	if (oaua->oaua_flow_control)
		obd_put_request_slot(
				&dt2osp_dev(dt_update->dur_dt)->opd_obd->u.cli);

	if (rc == 0 || req->rq_repmsg != NULL) {
		reply = req_capsule_server_sized_get(&req->rq_pill,
						     &RMF_OUT_UPDATE_REPLY,
						     OUT_UPDATE_REPLY_SIZE);
		if (reply == NULL || reply->ourp_magic != UPDATE_REPLY_MAGIC)
			rc1 = -EPROTO;
		else
			count = reply->ourp_count;
	} else {
		rc1 = rc;
	}

	list_for_each_entry_safe(oar, next, &dt_update->dur_cb_items,
				 oar_list) {
		list_del_init(&oar->oar_list);
		if (index < count && reply->ourp_lens[index] > 0) {
			struct object_update_result *result;

			result = object_update_result_get(reply, index, NULL);
			if (result == NULL)
				rc1 = -EPROTO;
			else
				rc1 = result->our_rc;
		} else {
			rc1 = rc;
			if (unlikely(rc1 == 0))
				rc1 = -EINVAL;
		}

		oar->oar_interpreter(env, reply, req, oar->oar_obj,
				       oar->oar_data, index, rc1);
		osp_async_request_fini(env, oar);
		index++;
	}

	out_destroy_update_req(dt_update);

	return 0;
}

/**
 * Pack all the requests in the shared asynchronous idempotent request queue
 * in single OUT RPC, and give it to the background ptlrpcd daemon (to send).
 *
 * \param[in] env	pointer to the thread context
 * \param[in] osp	pointer to the OSP device
 * \param[in] update	pointer to the shared queue
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
int osp_unplug_async_request(const struct lu_env *env,
			     struct osp_device *osp,
			     struct dt_update_request *update)
{
	struct osp_async_update_args	*args;
	struct ptlrpc_request		*req = NULL;
	int				 rc;

	rc = out_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
				 update->dur_req, &req);
	if (rc != 0) {
		struct osp_async_request *oar;
		struct osp_async_request *next;

		list_for_each_entry_safe(oar, next,
					 &update->dur_cb_items, oar_list) {
			list_del_init(&oar->oar_list);
			oar->oar_interpreter(env, NULL, NULL, oar->oar_obj,
					       oar->oar_data, 0, rc);
			osp_async_request_fini(env, oar);
		}
		out_destroy_update_req(update);
	} else {
		LASSERT(list_empty(&update->dur_list));

		args = ptlrpc_req_async_args(req);
		args->oaua_update = update;
		req->rq_interpret_reply = osp_async_update_interpret;
		ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
	}

	return rc;
}

/**
 * Fine or create (if NOT exist or purged) the shared asynchronous idempotent
 * request queue.
 *
 * If the osp_device::opd_async_requests is not NULL, then return it directly;
 * otherwise create new dt_update_request and attach it to opd_async_requests.
 *
 * \param[in] osp	pointer to the OSP device
 *
 * \retval		pointer to the shared queue
 * \retval		negative error number on failure
 */
static struct dt_update_request *
osp_find_or_create_async_update_request(struct osp_device *osp)
{
	struct dt_update_request *update = osp->opd_async_requests;

	if (update != NULL)
		return update;

	update = out_create_update_req(&osp->opd_dt_dev);
	if (!IS_ERR(update))
		osp->opd_async_requests = update;

	return update;
}

/**
 * Insert an asynchronous idempotent sub-request to the shared request
 * queue which is attached to the osp_device.
 *
 * It will generate a new osp_async_request with the given parameters,
 * then try to insert it into the osp_device-based shared request queue.
 * If the queue is full, then triggers the packaged OUT RPC (purges the
 * shared queue).
 *
 * Hold the osp::opd_async_requests_mutex.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] op		operation type
 * \param[in] obj		pointer to the operation target
 * \param[in] count		how many buffers the request contains
 * \param[in] lens		the array for request buffers' lengths
 * \param[in] bufs		the array for request buffers
 * \param[in] data		pointer to the data used by the interpreter
 * \param[in] interpreter	pointer to the interpreter function
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
int osp_insert_async_request(const struct lu_env *env,
			     int op, struct osp_object *obj, int count,
			     int *lens, const char **bufs, void *data,
			     osp_async_request_interpreter_t interpreter)
{
	struct osp_async_request     *oar;
	struct osp_device	     *osp = lu2osp_dev(osp2lu_obj(obj)->lo_dev);
	struct dt_update_request     *update;
	int			      rc  = 0;
	ENTRY;

	oar = osp_async_request_init(obj, data, interpreter);
	if (oar == NULL)
		RETURN(-ENOMEM);

	update = osp_find_or_create_async_update_request(osp);
	if (IS_ERR(update))
		GOTO(out, rc = PTR_ERR(update));

again:
	rc = out_insert_update(env, update, op, lu_object_fid(osp2lu_obj(obj)),
			       count, lens, bufs);
	if (rc == -E2BIG) {
		osp->opd_async_requests = NULL;
		mutex_unlock(&osp->opd_async_requests_mutex);

		rc = osp_unplug_async_request(env, osp, update);
		mutex_lock(&osp->opd_async_requests_mutex);
		if (rc != 0)
			GOTO(out, rc);

		update = osp_find_or_create_async_update_request(osp);
		if (IS_ERR(update))
			GOTO(out, rc = PTR_ERR(update));

		goto again;
	}

	if (rc == 0)
		list_add_tail(&oar->oar_list, &update->dur_cb_items);

	GOTO(out, rc);

out:
	if (rc != 0)
		osp_async_request_fini(env, oar);

	return rc;
}

/**
 * The OSP layer dt_device_operations::dt_trans_create() interface.
 *
 * There are two kinds of transactions which will involve OSP:
 *
 * 1) If the transaction only contains update(s) on remote server
 *    (MDT or OST), then it is a remote transaction. For a remote
 *    transaction, the up layer caller will call dt_trans_create()
 *    on the OSP dt_device to use osp_trans_create() which creates
 *    the transaction handler and returns it to the caller.
 *
 * 2) If the transcation contains both local and remote updates,
 *    then the up layer caller will not trigger osp_trans_create(),
 *    instead, it will call dt_trans_create() on other dt_device,
 *    such as LOD dt_device which will generate the transaction
 *    handler. Such handler will be used by the whole transaction
 *    in the subsequent sub-operations.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] d		pointer to the OSP dt_device
 *
 * \retval		pointer to the transaction handler
 * \retval		negative error number on failure
 */
struct thandle *osp_trans_create(const struct lu_env *env, struct dt_device *d)
{
	struct thandle		*th = NULL;
	struct thandle_update	*tu = NULL;
	int			 rc = 0;

	OBD_ALLOC_PTR(th);
	if (unlikely(th == NULL))
		GOTO(out, rc = -ENOMEM);

	th->th_dev = d;
	th->th_tags = LCT_TX_HANDLE;
	atomic_set(&th->th_refc, 1);
	th->th_alloc_size = sizeof(*th);

	OBD_ALLOC_PTR(tu);
	if (tu == NULL)
		GOTO(out, rc = -ENOMEM);

	INIT_LIST_HEAD(&tu->tu_remote_update_list);
	tu->tu_only_remote_trans = 1;
	th->th_update = tu;

out:
	if (rc != 0) {
		if (tu != NULL)
			OBD_FREE_PTR(tu);
		if (th != NULL)
			OBD_FREE_PTR(th);
		th = ERR_PTR(rc);
	}

	return th;
}

/**
 * Trigger the request for remote updates.
 *
 * If it is a remote transaction, then related remote update(s) will be sent
 * asynchronously; otherwise, before fully supports asynchronous mode commit,
 * the cross MDT transaction has to be synchronized.
 *
 * Please refer to osp_trans_create() for transaction type.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] osp		pointer to the OSP device
 * \param[in] dt_update		pointer to the dt_update_request
 * \param[in] th		pointer to the transaction handler
 * \param[in] flow_control	whether need to control the flow
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
static int osp_trans_trigger(const struct lu_env *env, struct osp_device *osp,
			     struct dt_update_request *dt_update,
			     struct thandle *th, bool flow_control)
{
	struct thandle_update	*tu = th->th_update;
	int			 rc = 0;

	LASSERT(tu != NULL);

	if (is_only_remote_trans(th)) {
		struct osp_async_update_args	*args;
		struct ptlrpc_request		*req;

		list_del_init(&dt_update->dur_list);
		rc = out_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
					 dt_update->dur_req, &req);
		if (rc == 0) {
			args = ptlrpc_req_async_args(req);
			args->oaua_update = dt_update;
			args->oaua_flow_control = flow_control;
			req->rq_interpret_reply =
				osp_async_update_interpret;
			ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
		} else {
			out_destroy_update_req(dt_update);
		}
	} else {
		th->th_sync = 1;
		rc = out_remote_sync(env, osp->opd_obd->u.cli.cl_import,
				     dt_update, NULL);
	}

	return rc;
}

/**
 * The OSP layer dt_device_operations::dt_trans_start() interface.
 *
 * If it is a remote transaction, then related remote updates will
 * be triggered in the osp_trans_stop(); otherwise the transaction
 * contains both local and remote update(s), then when the OUT RPC
 * will be triggered depends on the operation, and is indicated by
 * the dt_device::tu_sent_after_local_trans, for example:
 *
 * 1) If it is remote create, it will send the remote req after local
 * transaction. i.e. create the object locally first, then insert the
 * remote name entry.
 *
 * 2) If it is remote unlink, it will send the remote req before the
 * local transaction, i.e. delete the name entry remotely first, then
 * destroy the local object.
 *
 * Please refer to osp_trans_create() for transaction type.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] dt		pointer to the OSP dt_device
 * \param[in] th		pointer to the transaction handler
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
int osp_trans_start(const struct lu_env *env, struct dt_device *dt,
		    struct thandle *th)
{
	struct thandle_update		*tu = th->th_update;
	struct dt_update_request	*dt_update;
	int				 rc = 0;

	if (tu == NULL)
		return rc;

	/* Check whether there are updates related with this OSP */
	dt_update = out_find_update(tu, dt);
	if (dt_update == NULL)
		return rc;

	if (!is_only_remote_trans(th) && !tu->tu_sent_after_local_trans)
		rc = osp_trans_trigger(env, dt2osp_dev(dt), dt_update, th,
				       false);

	return rc;
}

/**
 * The OSP layer dt_device_operations::dt_trans_stop() interface.
 *
 * If it is a remote transaction, or the update handler is marked
 * as tu_sent_after_local_trans, then related remote updates will
 * be triggered here via osp_trans_trigger().
 *
 * For synchronous mode update or any failed update, the request
 * will be destroyed explicitly when the osp_trans_stop().
 *
 * Please refer to osp_trans_create() for transaction type.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] dt		pointer to the OSP dt_device
 * \param[in] th		pointer to the transaction handler
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
int osp_trans_stop(const struct lu_env *env, struct dt_device *dt,
		   struct thandle *th)
{
	struct thandle_update		*tu = th->th_update;
	struct dt_update_request	*dt_update;
	int				 rc = 0;

	LASSERT(tu != NULL);
	LASSERT(tu != LP_POISON);

	/* Check whether there are updates related with this OSP */
	dt_update = out_find_update(tu, dt);
	if (dt_update == NULL) {
		if (!is_only_remote_trans(th))
			return rc;
		goto put;
	}

	if (dt_update->dur_req->ourq_count == 0) {
		out_destroy_update_req(dt_update);
		goto put;
	}

	if (is_only_remote_trans(th)) {
		if (th->th_result == 0) {
			struct osp_device *osp = dt2osp_dev(th->th_dev);
			struct client_obd *cli = &osp->opd_obd->u.cli;

			rc = obd_get_request_slot(cli);
			if (!osp->opd_imp_active || osp->opd_got_disconnected) {
				if (rc == 0)
					obd_put_request_slot(cli);

				rc = -ENOTCONN;
			}

			if (rc != 0) {
				out_destroy_update_req(dt_update);
				goto put;
			}

			rc = osp_trans_trigger(env, dt2osp_dev(dt),
					       dt_update, th, true);
			if (rc != 0)
				obd_put_request_slot(cli);
		} else {
			rc = th->th_result;
			out_destroy_update_req(dt_update);
		}
	} else {
		if (tu->tu_sent_after_local_trans)
			rc = osp_trans_trigger(env, dt2osp_dev(dt),
					       dt_update, th, false);
		rc = dt_update->dur_rc;
		out_destroy_update_req(dt_update);
	}

put:
	thandle_put(th);
	return rc;
}
