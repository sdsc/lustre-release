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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * lustre/target/tgt_handler.c
 *
 * Lustre Unified Target request handler code
 *
 * Author: Brian Behlendorf <behlendorf1@llnl.gov>
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>
#include <obd_class.h>

#include "tgt_internal.h"

static int tgt_unpack_req_pack_rep(struct tgt_session_info *tsi, __u32 flags)
{
	struct req_capsule	*pill = tsi->tsi_pill;
	const struct mdt_body	*body = NULL;
	int			 rc = 0;

	ENTRY;

	if (req_capsule_has_field(pill, &RMF_MDT_BODY, RCL_CLIENT)) {
		body = req_capsule_client_get(pill, &RMF_MDT_BODY);
		if (body == NULL)
			RETURN(-EFAULT);
	}

	if (flags & HABEO_REFERO) {
		/* Pack reply */
		if (req_capsule_has_field(pill, &RMF_MDT_MD, RCL_SERVER))
			req_capsule_set_size(pill, &RMF_MDT_MD, RCL_SERVER,
					     body ? body->eadatasize : 0);
		if (req_capsule_has_field(pill, &RMF_LOGCOOKIES, RCL_SERVER))
			req_capsule_set_size(pill, &RMF_LOGCOOKIES,
					     RCL_SERVER, 0);

		rc = req_capsule_server_pack(pill);
	}
	RETURN(rc);
}

/*
 * Invoke handler for this request opc. Also do necessary preprocessing
 * (according to handler ->th_flags), and post-processing (setting of
 * ->last_{xid,committed}).
 */
static int tgt_handle_request0(struct tgt_session_info *tsi,
			       struct tgt_handler *h,
			       struct ptlrpc_request *req)
{
	int	 serious = 0;
	int	 rc;
	__u32	 flags;

	ENTRY;

	LASSERT(h->th_act != NULL);
	LASSERT(h->th_opc == lustre_msg_get_opc(req->rq_reqmsg));
	LASSERT(current->journal_info == NULL);

	rc = 0;
	flags = h->th_flags;
	LASSERT(ergo(flags & (HABEO_CORPUS | HABEO_REFERO),
		     h->th_fmt != NULL));
	if (h->th_fmt != NULL) {
		req_capsule_set(tsi->tsi_pill, h->th_fmt);
		rc = tgt_unpack_req_pack_rep(tsi, flags);
	}

	if (rc == 0 && flags & MUTABOR &&
	    tgt_conn_flags(tsi) & OBD_CONNECT_RDONLY)
		rc = -EROFS;

	if (rc == 0 && flags & HABEO_CLAVIS) {
		struct ldlm_request *dlm_req;

		LASSERT(h->th_fmt != NULL);

		dlm_req = req_capsule_client_get(tsi->tsi_pill, &RMF_DLM_REQ);
		if (dlm_req != NULL) {
			if (unlikely(dlm_req->lock_desc.l_resource.lr_type ==
				     LDLM_IBITS &&
				     dlm_req->lock_desc.l_policy_data.\
				     l_inodebits.bits == 0)) {
				/*
				 * Lock without inodebits makes no sense and
				 * will oops later in ldlm. If client miss to
				 * set such bits, do not trigger ASSERTION.
				 *
				 * For liblustre flock case, it maybe zero.
				 */
				rc = -EPROTO;
			} else {
				tsi->tsi_dlm_req = dlm_req;
			}
		} else {
			rc = -EFAULT;
		}
	}

	if (likely(rc == 0)) {
		/*
		 * Process request, there can be two types of rc:
		 * 1) errors with msg unpack/pack, other failures outside the
		 * operation itself. This is counted as serious errors;
		 * 2) errors during fs operation, should be placed in rq_status
		 * only
		 */
		rc = h->th_act(tsi);
		if (!is_serious(rc) &&
		    !req->rq_no_reply && req->rq_reply_state == NULL) {
			DEBUG_REQ(D_ERROR, req, "%s \"handler\" %s did not "
				  "pack reply and returned 0 error\n",
				  tgt_name(tsi), h->th_name);
			LBUG();
		}
		serious = is_serious(rc);
		rc = clear_serious(rc);
	} else {
		serious = 1;
	}

	req->rq_status = rc;

	/*
	 * ELDLM_* codes which > 0 should be in rq_status only as well as
	 * all non-serious errors.
	 */
	if (rc > 0 || !serious)
		rc = 0;

	LASSERT(current->journal_info == NULL);

	/*
	 * If we're DISCONNECTing, the export_data is already freed
	 *
	 * WAS if (likely(... && h->mh_opc != MDS_DISCONNECT))
	 */
	if (likely(rc == 0 && req->rq_export))
		target_committed_to_req(req);

	target_send_reply(req, rc, tsi->tsi_reply_fail_id);
	RETURN(0);
}

static int tgt_filter_recovery_request(struct ptlrpc_request *req,
				       struct obd_device *obd, int *process)
{
	switch (lustre_msg_get_opc(req->rq_reqmsg)) {
	case MDS_CONNECT: /* This will never get here, but for completeness. */
	case OST_CONNECT: /* This will never get here, but for completeness. */
	case MDS_DISCONNECT:
	case OST_DISCONNECT:
		*process = 1;
		RETURN(0);
	case MDS_CLOSE:
	case MDS_DONE_WRITING:
	case MDS_SYNC: /* used in unmounting */
	case OBD_PING:
	case MDS_REINT:
	case SEQ_QUERY:
	case FLD_QUERY:
	case LDLM_ENQUEUE:
		*process = target_queue_recovery_request(req, obd);
		RETURN(0);

	default:
		DEBUG_REQ(D_ERROR, req, "not permitted during recovery");
		*process = -EAGAIN;
		RETURN(0);
	}
}

/*
 * Handle recovery. Return:
 *        +1: continue request processing;
 *       -ve: abort immediately with the given error code;
 *         0: send reply with error code in req->rq_status;
 */
int tgt_handle_recovery(struct ptlrpc_request *req, int reply_fail_id)
{
	ENTRY;

	switch (lustre_msg_get_opc(req->rq_reqmsg)) {
	case MDS_CONNECT:
	case MGS_CONNECT:
	case SEC_CTX_INIT:
	case SEC_CTX_INIT_CONT:
	case SEC_CTX_FINI:
		RETURN(+1);
	}

	if (unlikely(!class_connected_export(req->rq_export))) {
		CERROR("operation %d on unconnected MDS from %s\n",
		       lustre_msg_get_opc(req->rq_reqmsg),
		       libcfs_id2str(req->rq_peer));
		req->rq_status = -ENOTCONN;
		target_send_reply(req, -ENOTCONN, reply_fail_id);
		RETURN(0);
	}

	if (!req->rq_export->exp_obd->obd_replayable)
		RETURN(+1);

	/* sanity check: if the xid matches, the request must be marked as a
	 * resent or replayed */
	if (req_xid_is_last(req)) {
		if (!(lustre_msg_get_flags(req->rq_reqmsg) &
		      (MSG_RESENT | MSG_REPLAY))) {
			DEBUG_REQ(D_WARNING, req, "rq_xid "LPU64" matches "
				  "last_xid, expected REPLAY or RESENT flag "
				  "(%x)", req->rq_xid,
				  lustre_msg_get_flags(req->rq_reqmsg));
			req->rq_status = -ENOTCONN;
			RETURN(-ENOTCONN);
		}
	}
	/* else: note the opposite is not always true; a RESENT req after a
	 * failover will usually not match the last_xid, since it was likely
	 * never committed. A REPLAYed request will almost never match the
	 * last xid, however it could for a committed, but still retained,
	 * open. */

	/* Check for aborted recovery... */
	if (unlikely(req->rq_export->exp_obd->obd_recovering)) {
		int rc;
		int should_process;

		DEBUG_REQ(D_INFO, req, "Got new replay");
		rc = tgt_filter_recovery_request(req, req->rq_export->exp_obd,
						 &should_process);
		if (rc != 0 || !should_process)
			RETURN(rc);
		else if (should_process < 0) {
			req->rq_status = should_process;
			rc = ptlrpc_error(req);
			RETURN(rc);
		}
	}
	RETURN(+1);
}

int tgt_msg_check_version(struct lustre_msg *msg)
{
	int rc;

	switch (lustre_msg_get_opc(msg)) {
	case MDS_CONNECT:
	case MDS_DISCONNECT:
	case MGS_CONNECT:
	case MGS_DISCONNECT:
	case OBD_PING:
	case SEC_CTX_INIT:
	case SEC_CTX_INIT_CONT:
	case SEC_CTX_FINI:
	case OBD_IDX_READ:
		rc = lustre_msg_check_version(msg, LUSTRE_OBD_VERSION);
		if (rc)
			CERROR("bad opc %u version %08x, expecting %08x\n",
			       lustre_msg_get_opc(msg),
			       lustre_msg_get_version(msg),
			       LUSTRE_OBD_VERSION);
		break;
	case MDS_GETSTATUS:
	case MDS_GETATTR:
	case MDS_GETATTR_NAME:
	case MDS_STATFS:
	case MDS_READPAGE:
	case MDS_WRITEPAGE:
	case MDS_IS_SUBDIR:
	case MDS_REINT:
	case MDS_CLOSE:
	case MDS_DONE_WRITING:
	case MDS_PIN:
	case MDS_SYNC:
	case MDS_GETXATTR:
	case MDS_SETXATTR:
	case MDS_SET_INFO:
	case MDS_GET_INFO:
	case MDS_QUOTACHECK:
	case MDS_QUOTACTL:
	case QUOTA_DQACQ:
	case QUOTA_DQREL:
	case SEQ_QUERY:
	case FLD_QUERY:
		rc = lustre_msg_check_version(msg, LUSTRE_MDS_VERSION);
		if (rc)
			CERROR("bad opc %u version %08x, expecting %08x\n",
			       lustre_msg_get_opc(msg),
			       lustre_msg_get_version(msg),
			       LUSTRE_MDS_VERSION);
		break;
	case MGS_EXCEPTION:
	case MGS_TARGET_REG:
	case MGS_TARGET_DEL:
	case MGS_SET_INFO:
	case MGS_CONFIG_READ:
		rc = lustre_msg_check_version(msg, LUSTRE_MGS_VERSION);
		if (rc)
			CERROR("bad opc %u version %08x, expecting %08x\n",
			       lustre_msg_get_opc(msg),
			       lustre_msg_get_version(msg),
			       LUSTRE_MGS_VERSION);
		break;
	case LDLM_ENQUEUE:
	case LDLM_CONVERT:
	case LDLM_BL_CALLBACK:
	case LDLM_CP_CALLBACK:
		rc = lustre_msg_check_version(msg, LUSTRE_DLM_VERSION);
		if (rc)
			CERROR("bad opc %u version %08x, expecting %08x\n",
			       lustre_msg_get_opc(msg),
			       lustre_msg_get_version(msg),
			       LUSTRE_DLM_VERSION);
		break;
	case OBD_LOG_CANCEL:
	case LLOG_ORIGIN_HANDLE_CREATE:
	case LLOG_ORIGIN_HANDLE_NEXT_BLOCK:
	case LLOG_ORIGIN_HANDLE_READ_HEADER:
	case LLOG_ORIGIN_HANDLE_CLOSE:
	case LLOG_ORIGIN_HANDLE_DESTROY:
	case LLOG_ORIGIN_HANDLE_PREV_BLOCK:
		rc = lustre_msg_check_version(msg, LUSTRE_LOG_VERSION);
		if (rc)
			CERROR("bad opc %u version %08x, expecting %08x\n",
			       lustre_msg_get_opc(msg),
			       lustre_msg_get_version(msg),
			       LUSTRE_LOG_VERSION);
		break;
	default:
		CWARN("Unknown opcode %d\n", lustre_msg_get_opc(msg));
		rc = -ENOTSUPP;
	}
	return rc;
}

void tgt_session_info_init(struct tgt_session_info *tsi,
			   struct ptlrpc_request *req,
			   int reply_fail_id)
{
	req_capsule_init(&req->rq_pill, req, RCL_SERVER);
	tsi->tsi_pill = &req->rq_pill;
	tsi->tsi_exp = req->rq_export;
	tsi->tsi_env = req->rq_svc_thread->t_env;
	tsi->tsi_reply_fail_id = reply_fail_id;
	tsi->tsi_dlm_req = NULL;
}

void tgt_session_info_fini(struct tgt_session_info *tsi)
{
	req_capsule_fini(tsi->tsi_pill);
}

int tgt_request_handle(struct ptlrpc_request *req)
{
	struct tgt_session_info	*tsi = tgt_ses_info(req->rq_svc_thread->t_env);
	struct lustre_msg	*msg = req->rq_reqmsg;
	struct tgt_handler	*h;
	struct tgt_opc_slice	*s;
	int			 request_fail_id = 0;
	int			 reply_fail_id = 0;
	__u32			 opc = lustre_msg_get_opc(msg);
	int			 rc;

	ENTRY;

	s = tgt_slice_find(req, &request_fail_id, &reply_fail_id);
	if (unlikely(s == NULL)) {
		CERROR("No handlers found for opc 0x%x, obd_type %s\n",
		       opc, req->rq_export != NULL ?
		       req->rq_export->exp_obd->obd_type->typ_name : "?");
		req->rq_status = -ENODEV;
		rc = ptlrpc_error(req);
		RETURN(rc);
	}

	if (CFS_FAIL_CHECK_ORSET(request_fail_id, CFS_FAIL_ONCE))
		RETURN(0);

	LASSERT(current->journal_info == NULL);

	rc = tgt_msg_check_version(msg);
	if (likely(rc == 0)) {
		rc = tgt_handle_recovery(req, reply_fail_id);
		if (likely(rc == +1)) {
			h = s->tos_hs + (opc - s->tos_opc_start);
			if (unlikely(h->th_opc == 0)) {
				CERROR("The unsupported opc: 0x%x\n", opc);
				req->rq_status = -ENOTSUPP;
				rc = ptlrpc_error(req);
			} else {
				LASSERTF(h->th_opc == opc,
					 "opcode mismatch %d != %d\n",
					 h->th_opc, opc);
				tgt_session_info_init(tsi, req, reply_fail_id);
				rc = tgt_handle_request0(tsi, h, req);
				tgt_session_info_fini(tsi);
			}
		}
	} else {
		CERROR("%s: drops mal-formed request\n",
		       req->rq_export != NULL ?
		       req->rq_export->exp_obd->obd_name : "?");
	}
	RETURN(rc);
}
EXPORT_SYMBOL(tgt_request_handle);

char *tgt_name(struct tgt_session_info *tsi)
{
	if (tsi->tsi_exp == NULL || tsi->tsi_exp->exp_obd == NULL)
		return "";

	return tsi->tsi_exp->exp_obd->obd_name;
}
EXPORT_SYMBOL(tgt_name);

void tgt_counter_incr(struct obd_export *exp, int opcode)
{
	lprocfs_counter_incr(exp->exp_obd->obd_stats, opcode);
	if (exp->exp_nid_stats && exp->exp_nid_stats->nid_stats != NULL)
		lprocfs_counter_incr(exp->exp_nid_stats->nid_stats, opcode);
}
EXPORT_SYMBOL(tgt_counter_incr);

/*
 * Unified target generic handlers.
 */
int tgt_connect(struct tgt_session_info *tsi)
{
	int rc;
	ENTRY;

	rc = target_handle_connect(tgt_ses_req(tsi));
	if (rc)
		RETURN(err_serious(rc));

	tsi->tsi_exp = tgt_ses_req(tsi)->rq_export;

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_connect);

int tgt_disconnect(struct tgt_session_info *tsi)
{
	int rc;
	ENTRY;

	rc = target_handle_disconnect(tgt_ses_req(tsi));
	if (rc)
		RETURN(err_serious(rc));

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_disconnect);

/*
 * Unified target OBD handlers
 */
int tgt_obd_ping(struct tgt_session_info *tsi)
{
	int rc;
	ENTRY;

	rc = target_handle_ping(tgt_ses_req(tsi));
	if (rc)
		RETURN(err_serious(rc));

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_obd_ping);

int tgt_obd_log_cancel(struct tgt_session_info *tsi)
{
	return err_serious(-EOPNOTSUPP);
}
EXPORT_SYMBOL(tgt_obd_log_cancel);

int tgt_obd_qc_callback(struct tgt_session_info *tsi)
{
	return err_serious(-EOPNOTSUPP);
}
EXPORT_SYMBOL(tgt_obd_qc_callback);

/*
 * Unified target DLM handlers.
 */
struct ldlm_callback_suite tgt_dlm_cbs = {
	.lcs_completion = ldlm_server_completion_ast,
	.lcs_blocking = ldlm_server_blocking_ast,
};

int tgt_enqueue(struct tgt_session_info *tsi)
{
	struct ptlrpc_request *req = tgt_ses_req(tsi);
	int rc;

	/*
	 * tsi->tsi_dlm_req was already swapped and (if necessary) converted,
	 * tsi->tsi_dlm_cbs was set by the *_req_handle() function.
	 */
	LASSERT(tsi->tsi_dlm_req != NULL);
	tsi->tsi_reply_fail_id = OBD_FAIL_LDLM_REPLY;

	rc = ldlm_handle_enqueue0(tsi->tsi_exp->exp_obd->obd_namespace, req,
				  tsi->tsi_dlm_req, &tgt_dlm_cbs);
	if (rc)
		RETURN(err_serious(rc));

	RETURN(req->rq_status);
}
EXPORT_SYMBOL(tgt_enqueue);

int tgt_convert(struct tgt_session_info *tsi)
{
	struct ptlrpc_request *req = tgt_ses_req(tsi);
	int rc;

	LASSERT(tsi->tsi_dlm_req);
	rc = ldlm_handle_convert0(req, tsi->tsi_dlm_req);
	if (rc)
		RETURN(err_serious(rc));

	RETURN(req->rq_status);
}
EXPORT_SYMBOL(tgt_convert);

int tgt_bl_callback(struct tgt_session_info *tsi)
{
	return err_serious(-EOPNOTSUPP);
}
EXPORT_SYMBOL(tgt_bl_callback);

int tgt_cp_callback(struct tgt_session_info *tsi)
{
	return err_serious(-EOPNOTSUPP);
}
EXPORT_SYMBOL(tgt_cp_callback);

/*
 * Unified target LLOG handlers.
 */
int tgt_llog_open(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_open(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_open);

int tgt_llog_close(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_close(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_close);


int tgt_llog_destroy(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_destroy(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_destroy);

int tgt_llog_read_header(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_read_header(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_read_header);

int tgt_llog_next_block(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_next_block(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_next_block);

int tgt_llog_prev_block(struct tgt_session_info *tsi)
{
	int rc;

	ENTRY;

	rc = llog_origin_handle_prev_block(tgt_ses_req(tsi));
	if (rc)
		RETURN(rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_llog_prev_block);
