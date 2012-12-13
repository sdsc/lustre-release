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
 * lustre/target/tgt_main.c
 *
 * Lustre Unified Target main initialization code
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>

#include "tgt_internal.h"

/* mutex to protect slices add/remove */
DEFINE_MUTEX(srv_slices_mutex);

#define SRV_SLICES_LIMIT	8
static struct srv_opcodes_slice srv_slices[SRV_SLICES_LIMIT];

int tgt_register_slice(struct tgt_opc_slice *slice, struct obd_type *obd_type,
		       int request_fail_id, int reply_fail_id)
{
	int i, rc;

	ENTRY;
	mutex_lock(&srv_slices_mutex);
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_obd_type == obd_type) {
			cfs_atomic_inc(&srv_slices[i].ts_ref);
			LASSERT(srv_slices[i].ts_slice == slice);
			GOTO(unlock, rc = 0);
		}
	}

	/* new slice, get first empty slot */
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == NULL) {
			srv_slices[i].ts_slice = slice;
			cfs_atomic_set(&srv_slices[i].ts_ref, 1);
			srv_slices[i].ts_obd_type = obd_type;
			srv_slices[i].ts_reply_fail_id = reply_fail_id;
			srv_slices[i].ts_request_fail_id = request_fail_id;
			GOTO(unlock, rc = 0);
		}
	}
	rc = -ENOMEM;
unlock:
	mutex_unlock(&srv_slices_mutex);
	return rc;
}
EXPORT_SYMBOL(tgt_register_slice);

void tgt_degister_slice(struct tgt_opc_slice *slice)
{
	int i;

	mutex_lock(&srv_slices_mutex);
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == slice) {
			if (cfs_atomic_dec_and_test(&srv_slices[i].ts_ref)) {
				srv_slices[i].ts_slice = NULL;
				srv_slices[i].ts_obd_type = NULL;
			}
			break;
		}
	}
	mutex_unlock(&srv_slices_mutex);
}
EXPORT_SYMBOL(tgt_degister_slice);

/* Find appropriate slice depending on obd_type and opcode */
struct tgt_opc_slice *tgt_slice_find(struct ptlrpc_request *req,
				     int *request_fail_id, int *reply_fail_id)
{
	struct tgt_opc_slice	*s;
	int			 i;
	__u32			 opc = lustre_msg_get_opc(req->rq_reqmsg);

	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == NULL)
			continue;
		/* if request has export then use only slice for that
		 * obd_type, otherwise looks for opcode in all slices */
		if (req->rq_export != NULL &&
		    req->rq_export->exp_obd->obd_type !=
		    srv_slices[i].ts_obd_type)
			continue;

		*request_fail_id = srv_slices[i].ts_request_fail_id;
		*reply_fail_id = srv_slices[i].ts_reply_fail_id;

		for (s = srv_slices[i].ts_slice; s->tos_hs != NULL; s++) {
			if (s->tos_opc_start <= opc && opc < s->tos_opc_end)
				return s;
		}
	}
	return NULL;
}

int tgt_init(const struct lu_env *env, struct lu_target *lut,
	     struct obd_device *obd, struct dt_device *dt)
{
	struct dt_object_format	dof;
	struct lu_attr		attr;
	struct lu_fid		fid;
	struct dt_object       *o;
	int			rc = 0;

	ENTRY;

	LASSERT(lut);
	LASSERT(obd);
	lut->lut_obd = obd;
	lut->lut_bottom = dt;
	lut->lut_last_rcvd = NULL;
	obd->u.obt.obt_lut = lut;
	obd->u.obt.obt_magic = OBT_MAGIC;

	spin_lock_init(&lut->lut_translock);

	OBD_ALLOC(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
	if (lut->lut_client_bitmap == NULL)
		RETURN(-ENOMEM);

	memset(&attr, 0, sizeof(attr));
	attr.la_valid = LA_MODE;
	attr.la_mode = S_IFREG | S_IRUGO | S_IWUSR;
	dof.dof_type = dt_mode_to_dft(S_IFREG);

	lu_local_obj_fid(&fid, MDT_LAST_RECV_OID);

	o = dt_find_or_create(env, lut->lut_bottom, &fid, &dof, &attr);
	if (!IS_ERR(o)) {
		lut->lut_last_rcvd = o;
	} else {
		OBD_FREE(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
		lut->lut_client_bitmap = NULL;
		rc = PTR_ERR(o);
		CERROR("cannot open %s: rc = %d\n", LAST_RCVD, rc);
	}

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_init);

void tgt_fini(const struct lu_env *env, struct lu_target *lut)
{
	ENTRY;

	if (lut->lut_client_bitmap) {
		OBD_FREE(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
		lut->lut_client_bitmap = NULL;
	}
	if (lut->lut_last_rcvd) {
		lu_object_put(env, &lut->lut_last_rcvd->do_lu);
		lut->lut_last_rcvd = NULL;
	}
	EXIT;
}
EXPORT_SYMBOL(tgt_fini);

/* context key constructor/destructor: tg_key_init, tg_key_fini */
LU_KEY_INIT_FINI(tgt, struct tgt_thread_info);

/* context key: tg_thread_key */
LU_CONTEXT_KEY_DEFINE(tgt, LCT_MD_THREAD | LCT_DT_THREAD);
EXPORT_SYMBOL(tgt_thread_key);

LU_KEY_INIT_GENERIC(tgt);

/* context key constructor/destructor: tgt_ses_key_init, tgt_ses_key_fini */
LU_KEY_INIT_FINI(tgt_ses, struct tgt_session_info);

/* context key: tgt_session_key */
struct lu_context_key tgt_session_key = {
	.lct_tags = LCT_SESSION,
	.lct_init = tgt_ses_key_init,
	.lct_fini = tgt_ses_key_fini,
};
EXPORT_SYMBOL(tgt_session_key);

LU_KEY_INIT_GENERIC(tgt_ses);

struct lprocfs_vars lprocfs_srv_module_vars[] = {
	{ 0 },
};

int tgt_mod_init(void)
{
	ENTRY;

	tgt_key_init_generic(&tgt_thread_key, NULL);
	lu_context_key_register_many(&tgt_thread_key, NULL);

	tgt_ses_key_init_generic(&tgt_session_key, NULL);
	lu_context_key_register_many(&tgt_session_key, NULL);

	RETURN(0);
}

void tgt_mod_exit(void)
{
	lu_context_key_degister(&tgt_thread_key);
	lu_context_key_degister(&tgt_session_key);
}

