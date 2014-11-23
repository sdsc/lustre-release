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
	struct osp_thandle	 *oaua_oth;
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

static struct object_update_request *object_update_request_alloc(size_t size)
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
				       size_t ourq_size)
{
	if (ourq != NULL)
		OBD_FREE_LARGE(ourq, ourq_size);
}

/**
 * Allocate and initialize dt_update_request
 *
 * dt_update_request is being used to track updates being executed on
 * this dt_device(OSD or OSP). The update buffer will be 4k initially,
 * and increased if needed.
 *
 * \param [in] dt	dt device
 *
 * \retval		dt_update_request being allocated if succeed
 * \retval		ERR_PTR(errno) if failed
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

static void
object_update_request_dump(const struct object_update_request *ourq, __u32 umask)
{
	unsigned int i;
	size_t total_size = 0;

	for (i = 0; i < ourq->ourq_count; i++) {
		struct object_update	*update;
		size_t			size = 0;

		update = object_update_request_get(ourq, i, &size);
		CDEBUG(umask, "i: %u fid: "DFID" op: %s master: %u params %d"
		       "batchid: "LPU64" size %lu\n", i, PFID(&update->ou_fid),
		       update_op_str(update->ou_type),
		       update->ou_master_index, update->ou_params_count,
		       update->ou_batchid, (unsigned long)size);

		total_size += size;
	}

	CDEBUG(umask, "updates %p magic %x count %d size %lu\n", ourq,
	       ourq->ourq_magic, ourq->ourq_count, (unsigned long)total_size);
}

/**
 * Prepare update request.
 *
 * Prepare OUT update ptlrpc request, and the request usually includes
 * all of updates (stored in \param ureq) from one operation.
 *
 * \param[in] env	execution environment
 * \param[in] imp	import on which ptlrpc request will be sent
 * \param[in] ureq	hold all of updates which will be packed into the req
 * \param[in] reqp	request to be created
 *
 * \retval		0 if preparation succeeds.
 * \retval		negative errno if preparation fails.
 */
int osp_prep_update_req(const struct lu_env *env, struct obd_import *imp,
			const struct object_update_request *ureq,
			struct ptlrpc_request **reqp)
{
	struct ptlrpc_request		*req;
	struct object_update_request	*tmp;
	int				ureq_len;
	int				rc;
	ENTRY;

	object_update_request_dump(ureq, D_INFO);
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

/**
 * Send update RPC.
 *
 * Send update request to the remote MDT synchronously.
 *
 * \param[in] env	execution environment
 * \param[in] imp	import on which ptlrpc request will be sent
 * \param[in] dt_update	hold all of updates which will be packed into the req
 * \param[in] reqp	request to be created
 *
 * \retval		0 if RPC succeeds.
 * \retval		negative errno if RPC fails.
 */
int osp_remote_sync(const struct lu_env *env, struct obd_import *imp,
		    struct dt_update_request *dt_update,
		    struct ptlrpc_request **reqp)
{
	struct ptlrpc_request	*req = NULL;
	int			rc;
	ENTRY;

	rc = osp_prep_update_req(env, imp, dt_update->dur_buf.ub_req, &req);
	if (rc != 0)
		RETURN(rc);

	/* This will only be called with read-only update, and these updates
	 * might be used to retrieve update log during recovery process, so
	 * it will be allowed to send during recovery process */
	req->rq_allow_replay = 1;

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
	struct osp_thandle		*oth	= oaua->oaua_oth;
	struct dt_update_request	*dt_update = oaua->oaua_update;
	struct osp_update_callback	*ouc;
	struct osp_update_callback	*next;
	int				 count	= 0;
	int				 index  = 0;
	int				 rc1	= 0;

	ENTRY;

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

	if (oth != NULL) {
		/* oth and dt_update_requests will be destoryed in
		 * osp_thandle_put */
		sub_trans_stop_cb(&oth->ot_super, rc);
		osp_thandle_put(oth);
	} else {
		dt_update_request_destroy(dt_update);
	}

	RETURN(0);
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

	rc = osp_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
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
		args->oaua_oth = NULL;
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
			     lu_object_fid(osp2lu_obj(obj)), count, lens, bufs);
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
	if (IS_ERR(update)) {
		th->th_result = PTR_ERR(update);
		return PTR_ERR(update);
	}

	if (dt2osp_dev(th->th_dev)->opd_connect_mdt)
		update->dur_flags = UPDATE_FL_SYNC;

	oth->ot_dur = update;
	return 0;
}

void osp_thandle_destroy(struct osp_thandle *oth)
{
	if (oth->ot_dur != NULL)
		dt_update_request_destroy(oth->ot_dur);
	OBD_FREE_PTR(oth);
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
	struct osp_thandle		*oth;
	struct thandle			*th = NULL;
	ENTRY;

	if (!dt2osp_dev(d)->opd_connect_mdt)
		RETURN(NULL);

	OBD_ALLOC_PTR(oth);
	if (unlikely(oth == NULL))
		RETURN(ERR_PTR(-ENOMEM));

	th = &oth->ot_super;
	th->th_dev = d;
	th->th_tags = LCT_TX_HANDLE;

	INIT_LIST_HEAD(&oth->ot_list);
	atomic_set(&oth->ot_refcount, 1);

	RETURN(th);
}

static void osp_request_commit_cb(struct ptlrpc_request *req)
{
	struct thandle *th = req->rq_cb_data;
	struct osp_thandle *oth = thandle_to_osp_thandle(th);
	__u64		   last_committed_transno;
	ENTRY;

	/* Interesting, commit callback arrives before stop callback */
	if (th->th_transno == 0) {
		DEBUG_REQ(D_HA, req, "arrive before stop callback.");
		th->th_transno = req->rq_transno;
	}

	/* Sigh, when commit_cb is called, imp_peer_committed_transno
	 * is not updated by this req yet, so we tried to find out
	 * last committed transno from req ourselves */
	if (lustre_msg_get_last_committed(req->rq_repmsg))
		last_committed_transno =
			lustre_msg_get_last_committed(req->rq_repmsg);
	else
		last_committed_transno =
			req->rq_import->imp_peer_committed_transno;

	CDEBUG(D_HA, "trans no "LPU64" committed transno "LPU64"\n",
	       req->rq_transno, last_committed_transno);

	if (req->rq_transno != 0 &&
	    (req->rq_transno <= last_committed_transno))
		th->th_committed = 1;

	sub_trans_commit_cb(th);
	osp_thandle_put(oth);
	EXIT;
}

/**
 * Send the request for remote updates.
 *
 * Send updates to the remote MDT. Prepare the request by dt_update
 * and send them to remote MDT, for sync request, it will wait
 * until the reply return, otherwise hand it to ptlrpcd.
 *
 * Please refer to osp_trans_create() for transaction type.
 *
 * \param[in] env		pointer to the thread context
 * \param[in] osp		pointer to the OSP device
 * \param[in] dt_update		pointer to the dt_update_request
 * \param[in] th		pointer to the transaction handler
 * \param[out] sent		whether the RPC has been sent
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */

static int osp_send_update_req(const struct lu_env *env,
			       struct osp_device *osp,
			       struct osp_thandle *oth)
{
	struct osp_update_args	*args;
	struct ptlrpc_request	*req;
	struct lu_device *top_device;
	int	rc = 0;
	ENTRY;

	rc = osp_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
				 oth->ot_dur->dur_buf.ub_req, &req);
	if (rc != 0)
		RETURN(rc);

	args = ptlrpc_req_async_args(req);
	args->oaua_update = oth->ot_dur;
	args->oaua_oth = oth;
	args->oaua_flow_control = false;
	osp_thandle_get(oth); /* hold for update interpret */
	req->rq_interpret_reply = osp_update_interpret;
	osp_thandle_get(oth); /* hold for commit callback */
	req->rq_commit_cb = osp_request_commit_cb;
	req->rq_cb_data = &oth->ot_super;

	/* If the transaction is created during MDT recoverying
	 * process, it means this is an recovery update, we need
	 * to let OSP send it anyway without checking recoverying
	 * status, in case the other target is being recoveried
	 * at the same time, and if we wait here for the import
	 * to be recoveryed, it might cause deadlock */
	top_device = osp->opd_dt_dev.dd_lu_dev.ld_site->ls_top_dev;
	if (top_device->ld_obd->obd_recovering)
		req->rq_allow_replay = 1;

	rc = ptlrpc_queue_wait(req);

	/* balanced for osp_update_interpret */
	if (rc == -ENOMEM && req->rq_set == NULL) {
		sub_trans_stop_cb(&oth->ot_super, rc);
		osp_thandle_put(oth);
	}

	if (rc != 0 || req->rq_transno == 0)
		osp_thandle_put(oth);

	oth->ot_super.th_transno = req->rq_transno;
	ptlrpc_req_finished(req);

	RETURN(rc);
}

/**
 * Set version for the transaction
 *
 * Set the version for the transaction, then the osp RPC will be
 * sent in the order of version, i.e. the transaction with lower
 * version will be sent first.
 *
 * \param [in] oth	osp thandle to be set version.
 */
void osp_check_and_set_rpc_version(struct osp_thandle *oth)
{
	struct osp_device *osp = dt2osp_dev(oth->ot_super.th_dev);
	struct osp_update *ou = osp->opd_update;
	ENTRY;

	if (oth->ot_set_version)
		RETURN_EXIT;

	LASSERT(ou != NULL);

	spin_lock(&ou->ou_lock);
	oth->ot_version = ou->ou_version++;
	spin_unlock(&ou->ou_lock);
	oth->ot_set_version = 1;

	CDEBUG(D_INFO, "%s: version "LPU64" oth:version %p:"LPU64"\n",
	       osp->opd_obd->obd_name, ou->ou_version, oth, oth->ot_version);

	RETURN_EXIT;
}

/**
 * Get next OSP thandle in the sending list
 * Get next OSP thandle in the sending list by version number, next
 * transaction will be
 * 1. transaction which does not have a version number, usually llog
 * update RPC, cancel update records etc.
 * 2. transaction whose version == opd_rpc_version.
 *
 * \param [in] ou	osp update structure.
 * \param [out] othp	the pointer holding the next osp thandle.
 *
 * \retval		true if getting the next transaction.
 * \retval		false if not getting the next transaction.
 */
static bool
osp_get_next_trans(struct osp_update *ou, struct osp_thandle **othp)
{
	struct osp_thandle	*ot;
	struct osp_thandle	*tmp;
	bool			got_trans = false;

	spin_lock(&ou->ou_lock);
	list_for_each_entry_safe(ot, tmp, &ou->ou_list,
				 ot_list) {
		CDEBUG(D_INFO, "ot %p version "LPU64" rpc_version "LPU64"\n",
		       ot, ot->ot_version, ou->ou_rpc_version);
		if (!ot->ot_set_version) {
			list_del_init(&ot->ot_list);
			*othp = ot;
			got_trans = true;
			break;
		}

		/* Find next dt_update_request in the list */
		if (ot->ot_version == ou->ou_rpc_version) {
			list_del_init(&ot->ot_list);
			*othp = ot;
			got_trans = true;
			break;
		}
	}
	spin_unlock(&ou->ou_lock);

	return got_trans;
}

/**
 * Sending update thread
 *
 * Create thread to send update request to other MDTs, his thread will pull
 * out update request from the list in OSP by version number, i.e. it will
 * make sure the update request with lower version number will be sent first.
 *
 * \param[in] _arg	hold the OSP device.
 *
 * \retval		0 if the thread is created successfully.
 * \retal		negative error if the thread is not created
 *                      successfully.
 */
int osp_send_update_thread(void *_arg)
{
	struct lu_env		env;
	struct osp_device	*osp = _arg;
	struct l_wait_info	 lwi = { 0 };
	struct osp_update	*ou = osp->opd_update;
	struct ptlrpc_thread	*thread = &ou->ou_thread;
	struct osp_thandle	*oth = NULL;
	int			rc;
	ENTRY;

	LASSERT(ou != NULL);
	rc = lu_env_init(&env, osp->opd_dt_dev.dd_lu_dev.ld_type->ldt_ctx_tags);
	if (rc < 0) {
		CERROR("%s: init env error: rc = %d\n", osp->opd_obd->obd_name,
		       rc);
		RETURN(rc);
	}

	spin_lock(&ou->ou_lock);
	thread->t_flags = SVC_RUNNING;
	spin_unlock(&ou->ou_lock);
	wake_up(&thread->t_ctl_waitq);
	do {
		oth = NULL;
		l_wait_event(ou->ou_waitq,
			     !osp_send_update_thread_running(ou) ||
			     osp_get_next_trans(ou, &oth),
			     &lwi);

		if (!osp_send_update_thread_running(ou)) {
			if (oth != NULL) {
				sub_trans_stop_cb(&oth->ot_super, -EIO);
				osp_thandle_put(oth);
			}
			break;
		}

		if (oth->ot_super.th_result != 0) {
			sub_trans_stop_cb(&oth->ot_super, rc);
			osp_thandle_put(oth);
			continue;
		}

		rc = osp_send_update_req(&env, osp, oth);

		spin_lock(&ou->ou_lock);
		if (oth->ot_set_version)
			ou->ou_rpc_version = oth->ot_version + 1;
		spin_unlock(&ou->ou_lock);

		/* Balanced for thandle_get in osp_trans_trigger() */
		osp_thandle_put(oth);
	} while (1);

	thread->t_flags = SVC_STOPPED;
	lu_env_fini(&env);
	wake_up(&thread->t_ctl_waitq);

	RETURN(0);
}

/**
 * Trigger the request for remote updates.
 *
 * Add the request to the sending list, and wake up osp update
 * sending thread.
 *
 * \param[in] osp		pointer to the OSP device
 * \param[in] oth		pointer to the transaction handler
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
static int osp_trans_trigger(struct osp_device *osp, struct osp_thandle *oth)
{
	if (!osp_send_update_thread_running(osp->opd_update))
		RETURN(-EIO);

	CDEBUG(D_INFO, "%s: add oth %p with version "LPU64"\n",
	       osp->opd_obd->obd_name, oth, oth->ot_version);
	list_add_tail(&oth->ot_list, &osp->opd_update->ou_list);
	osp_thandle_get(oth);
	wake_up(&osp->opd_update->ou_waitq);
	RETURN(0);
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
	if (dt_update == NULL || th->th_committed) {
		rc = th->th_result;
		sub_trans_stop_cb(&oth->ot_super, rc);
		GOTO(out, rc);
	}

	LASSERT(dt_update != LP_POISON);
	/* If there are no updates, destroy dt_update and thandle */
	if (dt_update->dur_buf.ub_req == NULL ||
	    dt_update->dur_buf.ub_req->ourq_count == 0) {
		sub_trans_stop_cb(&oth->ot_super, th->th_result);
		GOTO(out, rc = th->th_result);
	}

	if (!is_only_remote_trans(th) || th->th_sync) {
		/* For normal update requests, we will add it to the OSP
		 * sending_list, then sending thread will pull out the
		 * item by version number. */
		rc = osp_trans_trigger(dt2osp_dev(dt), oth);
		GOTO(out, rc);
	} else {
		/* Mostly used by LFSCK case */
		struct osp_device *osp = dt2osp_dev(th->th_dev);
		struct client_obd *cli = &osp->opd_obd->u.cli;
		struct ptlrpc_request	*req;
		struct osp_update_args	*args;

		rc = osp_prep_update_req(env, osp->opd_obd->u.cli.cl_import,
					 dt_update->dur_buf.ub_req, &req);
		if (rc != 0)
			GOTO(out, rc);

		rc = obd_get_request_slot(cli);
		if (rc != 0)
			GOTO(out, rc);

		if (!osp->opd_imp_active || !osp->opd_imp_connected) {
			obd_put_request_slot(cli);
			GOTO(out, rc = -ENOTCONN);
		}

		args = ptlrpc_req_async_args(req);
		osp_thandle_get(oth);
		args->oaua_oth = oth;
		args->oaua_update = oth->ot_dur;
		args->oaua_flow_control = true;
		req->rq_interpret_reply = osp_update_interpret;
		ptlrpcd_add_req(req, PDL_POLICY_LOCAL, -1);
	}
out:
	osp_thandle_put(oth);

	RETURN(rc);
}
