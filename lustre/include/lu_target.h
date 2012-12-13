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
#include <lustre_disk.h>

struct lu_target {
        struct obd_device       *lut_obd;
        struct dt_device        *lut_bottom;
        /** last_rcvd file */
        struct dt_object        *lut_last_rcvd;
        /* transaction callbacks */
        struct dt_txn_callback   lut_txn_cb;
        /** server data in last_rcvd file */
        struct lr_server_data    lut_lsd;
        /** Server last transaction number */
        __u64                    lut_last_transno;
        /** Lock protecting last transaction number */
	spinlock_t		 lut_translock;
	/** Lock protecting client bitmap */
	spinlock_t		 lut_client_bitmap_lock;
	/** Bitmap of known clients */
	unsigned long           *lut_client_bitmap;
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
	/*
	 * Additional fail id that can be set by handler.
	 */
	int			 tsi_reply_fail_id;
	int			 tsi_request_fail_id;
};

static inline struct tgt_session_info *tgt_ses_info(const struct lu_env *env)
{
	struct tgt_session_info *tsi;

	LASSERT(env->le_ses != NULL);
	tsi = lu_context_key_get(env->le_ses, &tgt_session_key);
	LASSERT(tsi);
	return tsi;
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
	/* Handler function */
	int			(*th_act)(struct tgt_session_info *tti);
	/* Request format for this request */
	const struct req_format	*th_fmt;
	/* Request versoin for this opcode */
	int			 th_version;
};

struct tgt_opc_slice {
	__u32			 tos_opc_start; /* First op code */
	int			 tos_opc_end; /* Last op code */
	struct tgt_handler	*tos_hs; /* Registered handler */
};

/* target/tgt_main.c */
int tgt_register_slice(struct tgt_opc_slice *slice, struct obd_type *obd_type,
		       int request_fail_id, int reply_fail_id);
void tgt_degister_slice(struct tgt_opc_slice *slice);

static inline struct ptlrpc_request *tgt_ses_req(struct tgt_session_info *tsi)
{
	return tsi->tsi_pill ? tsi->tsi_pill->rc_req : NULL;
}

static inline __u64 tgt_conn_flags(struct tgt_session_info *tsi)
{
	LASSERT(tsi->tsi_exp);
	return tsi->tsi_exp->exp_connect_flags;
}

/* target/tgt_handler.c */
int tgt_request_handle(struct ptlrpc_request *req);
char *tgt_name(struct tgt_session_info *tsi);
void tgt_counter_incr(struct obd_export *exp, int opcode);
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

typedef void (*tgt_cb_t)(struct lu_target *lut, __u64 transno,
			 void *data, int err);
struct tgt_commit_cb {
	tgt_cb_t  tgt_cb_func;
	void     *tgt_cb_data;
};

void tgt_boot_epoch_update(struct lu_target *lut);
int tgt_last_commit_cb_add(struct thandle *th, struct lu_target *lut,
			   struct obd_export *exp, __u64 transno);
int tgt_new_client_cb_add(struct thandle *th, struct obd_export *exp);
int tgt_init(const struct lu_env *env, struct lu_target *lut,
	     struct obd_device *obd, struct dt_device *dt);
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
int tgt_server_data_update(const struct lu_env *env, struct lu_target *tg, int sync);
int tgt_truncate_last_rcvd(const struct lu_env *env, struct lu_target *tg, loff_t off);

#endif /* __LUSTRE_LU_TARGET_H */
