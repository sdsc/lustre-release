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
 * lustre/osp/osp_trans.c
 *
 *
 * 1. OSP (Object Storage Proxy) transaction methods
 *
 * Implement OSP layer transaction related interfaces for the dt_device API
 * dt_device_operations.
 *
 *
 * 2. Handle asynchronous idempotent operations
 *
 * The OSP uses OUT (Object Unified Target) RPC to talk with other server
 * (MDT or OST) for kinds of operations, such as create, unlink, insert,
 * delete, lookup, set_(x)attr, get_(x)attr, and etc. To reduce the number
 * of RPCs, we allow multiple operations to be packaged together in single
 * OUT RPC.
 *
 * For the asynchronous idempotent operations, such as get_(x)attr, related
 * RPCs will be inserted into a osp_device based shared asynchronous request
 * queue - osp_device::opd_async_requests. When the queue is full, all the
 * requests in the queue will be packaged into a single OUT RPC and given to
 * the ptlrpcd daemon (for sending), then the queue is purged and other new
 * requests can be inserted into it.
 *
 * When the asynchronous idempotent operation inserts the request into the
 * shared queue, it will register an interpreter. When the packaged OUT RPC
 * is replied (or failed to be sent out), all the registered interpreters
 * will be called one by one to handle each own result.
 *
 *
 * Author: Di Wang <di.wang@intel.com>
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "osp_internal.h"

/**
 * The argument for the interpreter callback of osp request.
 */
struct osp_update_args {
	struct dt_update_request *oaua_update;
	bool			  oaua_flow_control;
};

/**
 * Call back for each update request.
 */
struct osp_update_callback {
	/* list in the dt_update_request::dur_cb_items */
	struct list_head		 ouc_list;

	/* The target of the async update request. */
	struct osp_object		*ouc_obj;

	/* The data used by or_interpreter. */
	void				*ouc_data;

	/* The interpreter function called after the async request handled. */
	osp_update_interpreter_t	ouc_interpreter;
};

/**
 * Allocate an osp request and initialize it with the given parameters.
 *
 * \param[in] obj		pointer to the operation target
 * \param[in] data		pointer to the data used by the interpreter
 * \param[in] interpreter	pointer to the interpreter function
 *
 * \retval			pointer to the asychronous request
 * \retval			NULL if the allocation failed
 */
static struct osp_update_callback *
osp_update_callback_init(struct osp_object *obj, void *data,
			 osp_update_interpreter_t interpreter)
{
	struct osp_update_callback *ouc;

	OBD_ALLOC_PTR(ouc);
	if (ouc == NULL)
		return NULL;

	lu_object_get(osp2lu_obj(obj));
	INIT_LIST_HEAD(&ouc->ouc_list);
	ouc->ouc_obj = obj;
	ouc->ouc_data = data;
	ouc->ouc_interpreter = interpreter;

	return ouc;
}

/**
 * Destroy the osp_update_callback.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] ouc	pointer to osp_update_callback
 */
static void osp_update_callback_fini(const struct lu_env *env,
				     struct osp_update_callback *ouc)
{
	LASSERT(list_empty(&ouc->ouc_list));

	lu_object_put(env, osp2lu_obj(ouc->ouc_obj));
	OBD_FREE_PTR(ouc);
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
static int osp_update_interpret(const struct lu_env *env,
				struct ptlrpc_request *req, void *arg, int rc)
{
	struct object_update_reply	*reply	= NULL;
	struct osp_update_args		*oaua	= arg;
	struct dt_update_request	*dt_update = oaua->oaua_update;
	struct osp_update_callback	*ouc;
	struct osp_update_callback	*next;
	int				 count	= 0;
	int				 index  = 0;
	int				 rc1	= 0;

	if (oaua->oaua_flow_control)
		obd_put_request_slot(
				&dt2osp_dev(dt_update->dur_dt)->opd_obd->u.cli);

	/* Unpack the results from the reply message. */
	if (req->rq_repmsg != NULL) {
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

	list_for_each_entry_safe(ouc, next, &dt_update->dur_cb_items,
				 ouc_list) {
		list_del_init(&ouc->ouc_list);

		/* The peer may only have handled some requests (indicated
		 * by the 'count') in the packaged OUT RPC, we can only get
		 * results for the handled part. */
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

		if (ouc->ouc_interpreter != NULL)
			ouc->ouc_interpreter(env, reply, req, ouc->ouc_obj,
					     ouc->ouc_data, index, rc1);

		osp_update_callback_fini(env, ouc);
		index++;
	}

	dt_update_request_destroy(dt_update);

	return 0;
}

/**
 * Pack all the requests in the shared asynchronous idempotent request queue
 * into a single OUT RPC that will be given to the background ptlrpcd daemon.
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
	struct osp_update_args	*args;
	struct ptlrpc_request	*req = NULL;
	int			 rc;

	rc = out_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
				 update->dur_buf.ub_req, &req);
	if (rc != 0) {
		struct osp_update_callback *ouc;
		struct osp_update_callback *next;

		list_for_each_entry_safe(ouc, next,
					 &update->dur_cb_items, ouc_list) {
			list_del_init(&ouc->ouc_list);
			ouc->ouc_interpreter(env, NULL, NULL, ouc->ouc_obj,
					     ouc->ouc_data, 0, rc);
			osp_update_callback_fini(env, ouc);
		}
		dt_update_request_destroy(update);
	} else {
		args = ptlrpc_req_async_args(req);
		args->oaua_update = update;
		req->rq_interpret_reply = osp_update_interpret;
		ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
	}

	return rc;
}

/**
 * Find or create (if NOT exist or purged) the shared asynchronous idempotent
 * request queue - osp_device::opd_async_requests.
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

	update = dt_update_request_create(&osp->opd_dt_dev);
	if (!IS_ERR(update))
		osp->opd_async_requests = update;

	return update;
}

/**
 * Insert an osp_update_callback into the dt_update_request.
 *
 * Insert an osp_update_callback to the dt_update_request. Usually each update
 * in the dt_update_request will have one correspondent callback, and these
 * callbacks will be called in rq_interpret_reply.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] obj		pointer to the operation target object
 * \param[in] data		pointer to the data used by the interpreter
 * \param[in] interpreter	pointer to the interpreter function
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
int osp_insert_update_callback(const struct lu_env *env,
			       struct dt_update_request *update,
			       struct osp_object *obj, void *data,
			       osp_update_interpreter_t interpreter)
{
	struct osp_update_callback  *ouc;

	ouc = osp_update_callback_init(obj, data, interpreter);
	if (ouc == NULL)
		RETURN(-ENOMEM);

	list_add_tail(&ouc->ouc_list, &update->dur_cb_items);

	return 0;
}

/**
 * Insert an asynchronous idempotent request to the shared request queue that
 * is attached to the osp_device.
 *
 * This function generates a new osp_async_request with the given parameters,
 * then tries to insert the request into the osp_device-based shared request
 * queue. If the queue is full, then triggers the packaged OUT RPC to purge
 * the shared queue firstly, and then re-tries.
 *
 * NOTE: must hold the osp::opd_async_requests_mutex to serialize concurrent
 *	 osp_insert_async_request call from others.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] op		operation type, see 'enum update_type'
 * \param[in] obj		pointer to the operation target
 * \param[in] count		array size of the subsequent @lens and @bufs
 * \param[in] lens		buffer length array for the subsequent @bufs
 * \param[in] bufs		the buffers to compose the request
 * \param[in] data		pointer to the data used by the interpreter
 * \param[in] interpreter	pointer to the interpreter function
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
int osp_insert_async_request(const struct lu_env *env, enum update_type op,
			     struct osp_object *obj, int count,
			     __u16 *lens, const void **bufs, void *data,
			     osp_update_interpreter_t interpreter)
{
	struct osp_device	     *osp = lu2osp_dev(osp2lu_obj(obj)->lo_dev);
	struct dt_update_request	*update;
	struct object_update		*object_update;
	size_t				max_update_size;
	struct object_update_request	*ureq;
	int				rc = 0;
	ENTRY;

	update = osp_find_or_create_async_update_request(osp);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

again:
	ureq = update->dur_buf.ub_req;
	max_update_size = update->dur_buf.ub_req_size -
			    object_update_request_size(ureq);

	object_update = update_buffer_get_update(ureq, ureq->ourq_count);
	rc = out_update_pack(env, object_update, max_update_size, op,
			     lu_object_fid(osp2lu_obj(obj)), count, lens, bufs,
			     0);
	/* The queue is full. */
	if (rc == -E2BIG) {
		osp->opd_async_requests = NULL;
		mutex_unlock(&osp->opd_async_requests_mutex);

		rc = osp_unplug_async_request(env, osp, update);
		mutex_lock(&osp->opd_async_requests_mutex);
		if (rc != 0)
			RETURN(rc);

		update = osp_find_or_create_async_update_request(osp);
		if (IS_ERR(update))
			RETURN(PTR_ERR(update));

		goto again;
	}

	rc = osp_insert_update_callback(env, update, obj, data, interpreter);

	RETURN(rc);
}

int osp_trans_update_request_create(struct thandle *th)
{
	struct osp_thandle		*oth = thandle_to_osp_thandle(th);
	struct dt_update_request	*update;

	if (oth->ot_dur != NULL)
		return 0;

	update = dt_update_request_create(th->th_dev);
	if (IS_ERR(update))
		return PTR_ERR(update);

	oth->ot_dur = update;
	return 0;
}
/**
 * The OSP layer dt_device_operations::dt_trans_create() interface
 * to create a transaction.
 *
 * There are two kinds of transactions that will involve OSP:
 *
 * 1) If the transaction only contains the updates on remote server
 *    (MDT or OST), such as re-generating the lost OST-object for
 *    LFSCK, then it is a remote transaction. For remote transaction,
 *    the upper layer caller (such as the LFSCK engine) will call the
 *    dt_trans_create() (with the OSP dt_device as the parameter),
 *    then the call will be directed to the osp_trans_create() that
 *    creates the transaction handler and returns it to the caller.
 *
 * 2) If the transcation contains both local and remote updates,
 *    such as cross MDTs create under DNE mode, then the upper layer
 *    caller will not trigger osp_trans_create(). Instead, it will
 *    call dt_trans_create() on other dt_device, such as LOD that
 *    will generate the transaction handler. Such handler will be
 *    used by the whole transaction in subsequent sub-operations.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] d		pointer to the OSP dt_device
 *
 * \retval		pointer to the transaction handler
 * \retval		negative error number on failure
 */
struct thandle *osp_trans_create(const struct lu_env *env, struct dt_device *d)
{
	struct osp_thandle	*oth;
	struct thandle		*th = NULL;
	struct osp_device	*osp = dt2osp_dev(d);
	ENTRY;

	OBD_ALLOC_PTR(oth);
	if (unlikely(oth == NULL))
		RETURN(ERR_PTR(-ENOMEM));

	th = &oth->ot_super;
	th->th_dev = d;
	th->th_tags = LCT_TX_HANDLE;

	if (osp->opd_connect_mdt)
		th->th_remote_mdt = 1;

	RETURN(th);
}

/**
 * Trigger the request for remote updates.
 *
 * If the transaction is a remote transaction, then related remote updates
 * will be sent asynchronously; otherwise, the cross MDTs transaction will
 * be synchronized.
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
	struct osp_update_args	*args;
	struct ptlrpc_request	*req;
	int	rc = 0;
	ENTRY;

	rc = out_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
				 dt_update->dur_buf.ub_req, &req);
	if (rc != 0)
		RETURN(rc);

	args = ptlrpc_req_async_args(req);
	args->oaua_update = dt_update;
	args->oaua_flow_control = flow_control;
	req->rq_interpret_reply = osp_update_interpret;
	if (!th->th_remote_mdt) {
		ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
	} else {
		rc = ptlrpc_queue_wait(req);
		dt_update->dur_rc = rc;
		ptlrpc_req_finished(req);
	}

	RETURN(rc);
}

/**
 * The OSP layer dt_device_operations::dt_trans_start() interface
 * to start the transaction.
 *
 * If the transaction is a remote transaction, then related remote
 * updates will be triggered in the osp_trans_stop().
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
	struct osp_thandle	 *oth = thandle_to_osp_thandle(th);

	/* No update requests from this OSP */
	if (oth->ot_dur == NULL)
		return 0;

	return 0;
}

/**
 * The OSP layer dt_device_operations::dt_trans_stop() interface
 * to stop the transaction.
 *
 * If the transaction is a remote transaction, related remote
 * updates will be triggered here via osp_trans_trigger().
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
	struct osp_thandle	 *oth = thandle_to_osp_thandle(th);
	struct dt_update_request *dt_update;
	int			 rc = 0;
	ENTRY;

	dt_update = oth->ot_dur;
	if (dt_update == NULL)
		GOTO(out, rc);

	LASSERT(dt_update != LP_POISON);
	/* If there are no updates, destroy dt_update and thandle */
	if (dt_update->dur_buf.ub_req == NULL ||
	    dt_update->dur_buf.ub_req->ourq_count == 0) {
		dt_update_request_destroy(dt_update);
		GOTO(out, rc);
	}

	if (!th->th_remote_mdt) {
		struct osp_device *osp = dt2osp_dev(th->th_dev);
		struct client_obd *cli = &osp->opd_obd->u.cli;

		if (th->th_result != 0)
			GOTO(out, rc = th->th_result);

		rc = obd_get_request_slot(cli);
		if (!osp->opd_imp_active || osp->opd_got_disconnected) {
			if (rc == 0)
				obd_put_request_slot(cli);
			rc = -ENOTCONN;
		}
		if (rc != 0)
			GOTO(out, rc);

		rc = osp_trans_trigger(env, dt2osp_dev(dt),
				       dt_update, th, true);
		if (rc != 0)
			obd_put_request_slot(cli);
	} else {
		rc = osp_trans_trigger(env, dt2osp_dev(dt), dt_update,
				       th, false);
		rc = dt_update->dur_rc;
	}

out:
	/* If RPC is triggered successfully, dt_update will be freed in
	 * osp_update_interpreter() */
	if (rc != 0 && dt_update != NULL)
		dt_update_request_destroy(dt_update);

	OBD_FREE_PTR(oth);

	RETURN(rc);
}
