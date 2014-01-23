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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _LUSTRE_LU_TARGET_H
#define _LUSTRE_LU_TARGET_H

#include <dt_object.h>
#include <lustre_export.h>
#include <lustre_update.h>
#include <lustre_disk.h>
#include <lustre_lfsck.h>

struct lu_target {
	struct obd_device	*lut_obd;
	struct dt_device	*lut_bottom;

	/* supported opcodes and handlers for this target */
	struct tgt_opc_slice	*lut_slice;
	__u32			 lut_reply_fail_id;
	__u32			 lut_request_fail_id;

	/* sptlrpc rules */
	rwlock_t		 lut_sptlrpc_lock;
	struct sptlrpc_rule_set	 lut_sptlrpc_rset;
	spinlock_t		 lut_flags_lock;
	int			 lut_sec_level;
	unsigned int		 lut_mds_capa:1,
				 lut_oss_capa:1,
				 lut_syncjournal:1,
				 lut_sync_lock_cancel:2,
				 /* e.g. OST node */
				 lut_no_reconstruct:1;
	/** last_rcvd file */
	struct dt_object	*lut_last_rcvd;
	/* transaction callbacks */
	struct dt_txn_callback	 lut_txn_cb;
	/** server data in last_rcvd file */
	struct lr_server_data	 lut_lsd;
	/** Server last transaction number */
	__u64			 lut_last_transno;
	/** Lock protecting last transaction number */
	spinlock_t		 lut_translock;
	/** Lock protecting client bitmap */
	spinlock_t		 lut_client_bitmap_lock;
	/** Bitmap of known clients */
	unsigned long		*lut_client_bitmap;
};

extern struct lu_context_key tgt_session_key;

struct tgt_session_info {
	/*
	 * The following members will be filled explicitly
	 * with specific data in tgt_ses_init().
	 */
	struct req_capsule	*tsi_pill;

	/*
	 * Lock request for "habeo clavis" operations.
	 */
	struct ldlm_request	*tsi_dlm_req;

	/* although we have export in req, there are cases when it is not
	 * available, e.g. closing files upon export destroy */
	struct obd_export	*tsi_exp;
	const struct lu_env	*tsi_env;
	struct lu_target	*tsi_tgt;

	const struct mdt_body	*tsi_mdt_body;
	struct ost_body		*tsi_ost_body;
	struct lu_object	*tsi_corpus;

	struct lu_fid		 tsi_fid;
	struct ldlm_res_id	 tsi_resid;

	/* object affected by VBR, for last_rcvd_update */
	struct dt_object	*tsi_vbr_obj;
	/* opdata for mdt_reint_open(), has the same value as
	 * ldlm_reply:lock_policy_res1.  The tgt_update_last_rcvd() stores
	 * this value onto disk for recovery when tgt_txn_stop_cb() is called.
	 */
	__u64			 tsi_opdata;

	/*
	 * Additional fail id that can be set by handler.
	 */
	int			 tsi_reply_fail_id;
	bool			 tsi_preprocessed;
	/* request JobID */
	char                    *tsi_jobid;
};

static inline struct tgt_session_info *tgt_ses_info(const struct lu_env *env)
{
	struct tgt_session_info *tsi;

	LASSERT(env->le_ses != NULL);
	tsi = lu_context_key_get(env->le_ses, &tgt_session_key);
	LASSERT(tsi);
	return tsi;
}

static inline void tgt_vbr_obj_set(const struct lu_env *env,
				   struct dt_object *obj)
{
	struct tgt_session_info	*tsi;

	if (env->le_ses != NULL) {
		tsi = tgt_ses_info(env);
		tsi->tsi_vbr_obj = obj;
	}
}

static inline void tgt_opdata_set(const struct lu_env *env, __u64 flags)
{
	struct tgt_session_info	*tsi;

	if (env->le_ses != NULL) {
		tsi = tgt_ses_info(env);
		tsi->tsi_opdata |= flags;
	}
}

static inline void tgt_opdata_clear(const struct lu_env *env, __u64 flags)
{
	struct tgt_session_info	*tsi;

	if (env->le_ses != NULL) {
		tsi = tgt_ses_info(env);
		tsi->tsi_opdata &= ~flags;
	}
}

/*
 * Generic unified target support.
 */
enum tgt_handler_flags {
	/*
	 * struct *_body is passed in the incoming message, and object
	 * identified by this fid exists on disk.
	 *                            *
	 * "habeo corpus" == "I have a body"
	 */
	HABEO_CORPUS = (1 << 0),
	/*
	 * struct ldlm_request is passed in the incoming message.
	 *
	 * "habeo clavis" == "I have a key"
	 *                                     */
	HABEO_CLAVIS = (1 << 1),
	/*
	 * this request has fixed reply format, so that reply message can be
	 * packed by generic code.
	 *
	 * "habeo refero" == "I have a reply"
	 */
	HABEO_REFERO = (1 << 2),
	/*
	 * this request will modify something, so check whether the file system
	 * is readonly or not, then return -EROFS to client asap if necessary.
	 *
	 * "mutabor" == "I shall modify"
	 */
	MUTABOR      = (1 << 3)
};

struct tgt_handler {
	/* The name of this handler. */
	const char		*th_name;
	/* Fail id, check at the beginning */
	int			 th_fail_id;
	/* Operation code */
	__u32			 th_opc;
	/* Flags in enum tgt_handler_flags */
	__u32			 th_flags;
	/* Request version for this opcode */
	int			 th_version;
	/* Handler function */
	int			(*th_act)(struct tgt_session_info *tsi);
	/* Handler function for high priority requests */
	void			(*th_hp)(struct tgt_session_info *tsi);
	/* Request format for this request */
	const struct req_format	*th_fmt;
};

struct tgt_opc_slice {
	__u32			 tos_opc_start; /* First op code */
	__u32			 tos_opc_end; /* Last op code */
	struct tgt_handler	*tos_hs; /* Registered handler */
};

static inline struct ptlrpc_request *tgt_ses_req(struct tgt_session_info *tsi)
{
	return tsi->tsi_pill ? tsi->tsi_pill->rc_req : NULL;
}

static inline __u64 tgt_conn_flags(struct tgt_session_info *tsi)
{
	LASSERT(tsi->tsi_exp);
	return exp_connect_flags(tsi->tsi_exp);
}

static inline int req_is_replay(struct ptlrpc_request *req)
{
	LASSERT(req->rq_reqmsg);
	return !!(lustre_msg_get_flags(req->rq_reqmsg) & MSG_REPLAY);
}

/* target/tgt_handler.c */
int tgt_request_handle(struct ptlrpc_request *req);
char *tgt_name(struct lu_target *tgt);
void tgt_counter_incr(struct obd_export *exp, int opcode);
int tgt_connect_check_sptlrpc(struct ptlrpc_request *req,
			      struct obd_export *exp);
int tgt_adapt_sptlrpc_conf(struct lu_target *tgt, int initial);
int tgt_connect(struct tgt_session_info *tsi);
int tgt_disconnect(struct tgt_session_info *uti);
int tgt_obd_ping(struct tgt_session_info *tsi);
int tgt_enqueue(struct tgt_session_info *tsi);
int tgt_convert(struct tgt_session_info *tsi);
int tgt_bl_callback(struct tgt_session_info *tsi);
int tgt_cp_callback(struct tgt_session_info *tsi);
int tgt_llog_open(struct tgt_session_info *tsi);
int tgt_llog_close(struct tgt_session_info *tsi);
int tgt_llog_destroy(struct tgt_session_info *tsi);
int tgt_llog_read_header(struct tgt_session_info *tsi);
int tgt_llog_next_block(struct tgt_session_info *tsi);
int tgt_llog_prev_block(struct tgt_session_info *tsi);
int tgt_sec_ctx_init(struct tgt_session_info *tsi);
int tgt_sec_ctx_init_cont(struct tgt_session_info *tsi);
int tgt_sec_ctx_fini(struct tgt_session_info *tsi);
int tgt_sendpage(struct tgt_session_info *tsi, struct lu_rdpg *rdpg, int nob);
int tgt_validate_obdo(struct tgt_session_info *tsi, struct obdo *oa);
int tgt_sync(const struct lu_env *env, struct lu_target *tgt,
	     struct dt_object *obj);

int tgt_io_thread_init(struct ptlrpc_thread *thread);
void tgt_io_thread_done(struct ptlrpc_thread *thread);

int tgt_extent_lock(struct ldlm_namespace *ns, struct ldlm_res_id *res_id,
		    __u64 start, __u64 end, struct lustre_handle *lh,
		    int mode, __u64 *flags);
void tgt_extent_unlock(struct lustre_handle *lh, ldlm_mode_t mode);
int tgt_brw_lock(struct ldlm_namespace *ns, struct ldlm_res_id *res_id,
		 struct obd_ioobj *obj, struct niobuf_remote *nb,
		 struct lustre_handle *lh, int mode);
void tgt_brw_unlock(struct obd_ioobj *obj, struct niobuf_remote *niob,
		    struct lustre_handle *lh, int mode);
int tgt_brw_read(struct tgt_session_info *tsi);
int tgt_brw_write(struct tgt_session_info *tsi);
int tgt_hpreq_handler(struct ptlrpc_request *req);
void tgt_register_lfsck_start(int (*start)(const struct lu_env *,
					   struct dt_device *,
					   struct lfsck_start_param *));
void tgt_register_lfsck_in_notify(int (*notify)(const struct lu_env *,
						struct dt_device *,
						struct lfsck_request *));
void tgt_register_lfsck_query(int (*query)(const struct lu_env *,
					   struct dt_device *,
					   struct lfsck_request *));

extern struct tgt_handler tgt_sec_ctx_handlers[];
extern struct tgt_handler tgt_lfsck_handlers[];
extern struct tgt_handler tgt_obd_handlers[];
extern struct tgt_handler tgt_dlm_handlers[];
extern struct tgt_handler tgt_llog_handlers[];
extern struct tgt_handler tgt_out_handlers[];
extern struct tgt_handler fld_handlers[];
extern struct tgt_handler seq_handlers[];

typedef void (*tgt_cb_t)(struct lu_target *lut, __u64 transno,
			 void *data, int err);
struct tgt_commit_cb {
	tgt_cb_t  tgt_cb_func;
	void     *tgt_cb_data;
};

int tgt_hpreq_handler(struct ptlrpc_request *req);

/* target/tgt_main.c */
void tgt_boot_epoch_update(struct lu_target *lut);
int tgt_last_commit_cb_add(struct thandle *th, struct lu_target *lut,
			   struct obd_export *exp, __u64 transno);
int tgt_new_client_cb_add(struct thandle *th, struct obd_export *exp);
int tgt_init(const struct lu_env *env, struct lu_target *lut,
	     struct obd_device *obd, struct dt_device *dt,
	     struct tgt_opc_slice *slice,
	     int request_fail_id, int reply_fail_id);
void tgt_fini(const struct lu_env *env, struct lu_target *lut);
int tgt_client_alloc(struct obd_export *exp);
void tgt_client_free(struct obd_export *exp);
int tgt_client_del(const struct lu_env *env, struct obd_export *exp);
int tgt_client_add(const struct lu_env *env, struct obd_export *exp, int);
int tgt_client_new(const struct lu_env *env, struct obd_export *exp);
int tgt_client_data_read(const struct lu_env *env, struct lu_target *tg,
			 struct lsd_client_data *lcd, loff_t *off, int index);
int tgt_client_data_write(const struct lu_env *env, struct lu_target *tg,
			  struct lsd_client_data *lcd, loff_t *off, struct thandle *th);
int tgt_server_data_read(const struct lu_env *env, struct lu_target *tg);
int tgt_server_data_write(const struct lu_env *env, struct lu_target *tg,
			  struct thandle *th);
int tgt_server_data_update(const struct lu_env *env, struct lu_target *tg,
			   int sync);
int tgt_truncate_last_rcvd(const struct lu_env *env, struct lu_target *tg,
			   loff_t off);

/* target/out_lib.c */
struct update_request *
out_find_update(struct thandle *th, struct dt_device *dt_dev);
void out_destroy_update_req(struct update_request *update);
struct update_request *out_create_update_req(struct dt_device *dt);
struct update_request *out_find_create_update_loc(struct thandle *th,
						  struct dt_object *dt);
int out_prep_update_req(const struct lu_env *env, struct obd_import *imp,
			const struct update_buf *ubuf, int ubuf_len,
			struct ptlrpc_request **reqp);
int out_remote_sync(const struct lu_env *env, struct obd_import *imp,
		    struct update_request *update,
		    struct ptlrpc_request **reqp);
int out_insert_update(const struct lu_env *env, struct update_request *update,
		      int op, const struct lu_fid *fid, int count,
		      int *lens, const char **bufs);

enum {
	ESERIOUS = 0x0001000
};

static inline int err_serious(int rc)
{
	LASSERT(rc < 0);
	return -(-rc | ESERIOUS);
}

static inline int clear_serious(int rc)
{
	if (rc < 0)
		rc = -(-rc & ~ESERIOUS);
	return rc;
}

static inline int is_serious(int rc)
{
	return (rc < 0 && -rc & ESERIOUS);
}

/**
 * Do not return server-side uid/gid to remote client
 */
static inline void tgt_drop_id(struct obd_export *exp, struct obdo *oa)
{
	if (unlikely(exp_connect_rmtclient(exp))) {
		oa->o_uid = -1;
		oa->o_gid = -1;
		oa->o_valid &= ~(OBD_MD_FLUID | OBD_MD_FLGID);
	}
}

/*
 * Unified target generic handers macros and generic functions.
 */
#define TGT_RPC_HANDLER_HP(base, flags, opc, fn, hp, fmt, version)	\
[opc - base] = {							\
	.th_name	= #opc,						\
	.th_fail_id	= OBD_FAIL_ ## opc ## _NET,			\
	.th_opc		= opc,						\
	.th_flags	= flags,					\
	.th_act		= fn,						\
	.th_fmt		= fmt,						\
	.th_version	= version,					\
	.th_hp		= hp,						\
}
#define TGT_RPC_HANDLER(base, flags, opc, fn, fmt, version)		\
	TGT_RPC_HANDLER_HP(base, flags, opc, fn, NULL, fmt, version)

/* MDT Request with a format known in advance */
#define TGT_MDT_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(MDS_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_MDS_VERSION)
/* Request with a format we do not yet know */
#define TGT_MDT_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(MDS_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_MDS_VERSION)

/* OST Request with a format known in advance */
#define TGT_OST_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(OST_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_OST_VERSION)
#define TGT_OST_HDL_HP(flags, name, fn, hp)				\
	TGT_RPC_HANDLER_HP(OST_FIRST_OPC, flags, name, fn, hp,		\
			   &RQF_ ## name, LUSTRE_OST_VERSION)

/* MGS request with a format known in advance */
#define TGT_MGS_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(MGS_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_MGS_VERSION)
#define TGT_MGS_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(MGS_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_MGS_VERSION)

/*
 * OBD handler macros and generic functions.
 */
#define TGT_OBD_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(OBD_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_OBD_VERSION)
#define TGT_OBD_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(OBD_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_OBD_VERSION)

/*
 * DLM handler macros and generic functions.
 */
#define TGT_DLM_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(LDLM_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_DLM_VERSION)
#define TGT_DLM_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(LDLM_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_DLM_VERSION)

/*
 * LLOG handler macros and generic functions.
 */
#define TGT_LLOG_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(LLOG_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_LOG_VERSION)
#define TGT_LLOG_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(LLOG_FIRST_OPC, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_LOG_VERSION)

/*
 * Sec context handler macros and generic functions.
 */
#define TGT_SEC_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(SEC_FIRST_OPC, flags, name, fn, NULL,		\
			LUSTRE_OBD_VERSION)

#define TGT_QUOTA_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(QUOTA_DQACQ, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_MDS_VERSION)

/* Sequence service handlers */
#define TGT_SEQ_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(SEQ_QUERY, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_MDS_VERSION)

/* FID Location Database handlers */
#define TGT_FLD_HDL_VAR(flags, name, fn)				\
	TGT_RPC_HANDLER(FLD_QUERY, flags, name, fn, NULL,		\
			LUSTRE_MDS_VERSION)

/* LFSCK handlers */
#define TGT_LFSCK_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(LFSCK_FIRST_OPC, flags, name, fn,		\
			&RQF_ ## name, LUSTRE_OBD_VERSION)

/* Request with a format known in advance */
#define TGT_UPDATE_HDL(flags, name, fn)					\
	TGT_RPC_HANDLER(UPDATE_OBJ, flags, name, fn, &RQF_ ## name,	\
			LUSTRE_MDS_VERSION)

#endif /* __LUSTRE_LU_TARGET_H */
