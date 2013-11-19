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
 *
 * lustre/mdt/out_handler.c
 *
 * Object update handler between targets.
 *
 * Author: di.wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd_class.h>
#include <md_object.h>
#include "tgt_internal.h"
#include <lustre_log.h>
#include <lustre_update.h>

struct tx_arg *tx_add_exec(struct thandle_exec_args *ta, tx_exec_func_t func,
			   tx_exec_func_t undo, char *file, int line)
{
	int i;

	LASSERT(ta);
	LASSERT(func);

	i = ta->ta_argno;
	LASSERT(i < UPDATE_PER_RPC_MAX);

	ta->ta_argno++;

	ta->ta_args[i].exec_fn = func;
	ta->ta_args[i].undo_fn = undo;
	ta->ta_args[i].file    = file;
	ta->ta_args[i].line    = line;

	return &ta->ta_args[i];
}

static int out_tx_start(const struct lu_env *env, struct dt_device *dt,
			struct thandle_exec_args *ta, struct update_buf *ubuf)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int rc = 0;
	ENTRY;

	memset(ta, 0, sizeof(*ta));
	ta->ta_handle = dt_trans_create(env, dt);
	if (IS_ERR(ta->ta_handle)) {
		CERROR("%s: start handle error: rc = %ld\n",
		       dt_obd_name(dt), PTR_ERR(ta->ta_handle));
		rc = PTR_ERR(ta->ta_handle);
		ta->ta_handle = NULL;
		RETURN(rc);
	}
	ta->ta_dev = dt;
	/*For phase I, sync for cross-ref operation*/
	if (ubuf != NULL && tti->tti_u.update.tti_log_update)
		rc = dt_trans_update_declare_llog_add(env, dt, ta->ta_handle,
						update_buf_master_idx(ubuf));
	RETURN(rc);
}

static int out_trans_start(const struct lu_env *env,
			   struct thandle_exec_args *ta)
{
	/* Always do sync commit for Phase I */
	return dt_trans_start(env, ta->ta_dev, ta->ta_handle);
}

static void out_insert_reply_transno(const struct lu_env *env,
				     struct tx_arg *ta, __u64 transno)
{
	struct update_reply *reply;

	if (ta->reply == NULL)
		return;
	reply = update_get_reply(ta->reply, ta->index, NULL);
	LASSERT(reply != NULL);
	reply->ur_transno = transno;
	reply->ur_transno_idx = ta->uidx;
}

static int out_trans_stop(const struct lu_env *env,
			  struct thandle_exec_args *ta, int err)
{
	struct thandle_update *tu;
	int i;
	int rc;

	ta->ta_handle->th_result = err;

	tu = ta->ta_handle->th_update;
	rc = dt_trans_stop(env, ta->ta_dev, ta->ta_handle);
	for (i = 0; i < ta->ta_argno; i++) {
		/* insert trans no */
		LASSERT(tu != NULL);
		if (err == 0)
			out_insert_reply_transno(env, &ta->ta_args[i],
						 tu->tu_batchid);
		if (ta->ta_args[i].object != NULL) {
			lu_object_put(env, &ta->ta_args[i].object->do_lu);
			ta->ta_args[i].object = NULL;
		}
	}

	return rc;
}

static int out_txn_stop_cb(const struct lu_env *env, struct thandle *th,
			   void *data)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct thandle_update	*tu = th->th_update;
	int			master_index;
	int			rc = 0;

	ENTRY;

	if (tu == NULL)
	       RETURN(0);

	master_index = update_buf_master_idx(tu->tu_update_buf);

	if (tu->tu_update_buf != NULL && tti->tti_u.update.tti_log_update)
		rc = dt_trans_update_llog_add(env, th->th_dev,
					      tu->tu_update_buf, NULL,
					      master_index, th);
	RETURN(rc);
}

int out_tx_end(const struct lu_env *env, struct thandle_exec_args *ta,
	       struct update_buf *ubuf)
{
	struct tgt_session_info *tsi = tgt_ses_info(env);
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct thandle_update	*tu = &tti->tti_u.update.tti_update_handle;
	int i = 0, rc;
	ENTRY;

	LASSERT(ta->ta_dev);
	LASSERT(ta->ta_handle);

	if (ta->ta_err != 0 || ta->ta_argno == 0)
		GOTO(stop, rc = ta->ta_err);

	rc = out_trans_start(env, ta);
	if (unlikely(rc))
		GOTO(stop, rc);

	for (i = 0; i < ta->ta_argno; i++) {
		rc = ta->ta_args[i].exec_fn(env, ta->ta_handle,
					    &ta->ta_args[i]);
		if (unlikely(rc)) {
			CDEBUG(D_INFO, "error during execution of #%u from"
			       " %s:%d: rc = %d\n", i, ta->ta_args[i].file,
			       ta->ta_args[i].line, rc);
			while (--i >= 0) {
				LASSERTF(ta->ta_args[i].undo_fn != NULL,
				    "can't undo changes, hope for failover!\n");
				ta->ta_args[i].undo_fn(env, ta->ta_handle,
						       &ta->ta_args[i]);
			}
			break;
		}
	}

	memset(tu, 0, sizeof(*tu));
	CFS_INIT_LIST_HEAD(&tu->tu_remote_update_list);
	tu->tu_update_buf = ubuf;
	tu->tu_update_buf_size = update_buf_size(ubuf);
	tu->tu_txn_stop_cb = out_txn_stop_cb;
	ta->ta_handle->th_update = tu;
	/* Only fail for real update */
	tsi->tsi_reply_fail_id = OBD_FAIL_UPDATE_OBJ_NET_REP;
stop:
	CDEBUG(D_INFO, "%s: executed %u/%u: rc = %d\n",
	       dt_obd_name(ta->ta_dev), i, ta->ta_argno, rc);
	out_trans_stop(env, ta, rc);
	ta->ta_handle = NULL;
	ta->ta_argno = 0;
	ta->ta_err = 0;

	RETURN(rc);
}

static void out_reconstruct(const struct lu_env *env, struct dt_device *dt,
			    struct dt_object *obj,
			    struct update_reply_buf *reply_buf, int index)
{
	CDEBUG(D_INFO, "%s: fork reply %p index %d: rc = %d\n",
	       dt_obd_name(dt), reply_buf, index, 0);

	update_insert_reply(reply_buf, NULL, 0, index, 0);
	return;
}

typedef void (*out_reconstruct_t)(const struct lu_env *env,
				  struct dt_device *dt,
				  struct dt_object *obj,
				  struct update_reply_buf *reply_buf,
				  int index);

static inline int out_check_resent(const struct lu_env *env,
				   struct dt_device *dt,
				   struct dt_object *obj,
				   out_reconstruct_t reconstruct,
				   struct update_reply_buf *reply_buf,
				   int index)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);

	if (unlikely(tti->tti_u.update.tti_resend)) {
		LASSERT(reply_buf != NULL);
		reconstruct(env, dt, obj, reply_buf, index);
		return 1;
	}
	return 0;
}

static int out_obj_destroy(const struct lu_env *env, struct dt_object *dt_obj,
			   struct thandle *th)
{
	int rc;

	CDEBUG(D_INFO, "%s: destroy "DFID"\n", dt_obd_name(th->th_dev),
	       PFID(lu_object_fid(&dt_obj->do_lu)));

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_destroy(env, dt_obj, th);
	dt_write_unlock(env, dt_obj);

	return rc;
}

/**
 * All of the xxx_undo will be used once execution failed,
 * But because all of the required resource has been reserved in
 * declare phase, i.e. if declare succeed, it should make sure
 * the following executing phase succeed in anyway, so these undo
 * should be useless for most of the time in Phase I
 */
int out_tx_create_undo(const struct lu_env *env, struct thandle *th,
		       struct tx_arg *arg)
{
	int rc;

	rc = out_obj_destroy(env, arg->object, th);
	if (rc != 0)
		CERROR("%s: undo failure, we are doomed!: rc = %d\n",
		       dt_obd_name(th->th_dev), rc);
	return rc;
}

int out_tx_create_exec(const struct lu_env *env, struct thandle *th,
		       struct tx_arg *arg)
{
	struct dt_object	*dt_obj = arg->object;
	int			 rc;

	CDEBUG(D_OTHER, "%s: create "DFID": dof %u, mode %o\n",
	       dt_obd_name(th->th_dev),
	       PFID(lu_object_fid(&arg->object->do_lu)),
	       arg->u.create.dof.dof_type,
	       arg->u.create.attr.la_mode & S_IFMT);

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_create(env, dt_obj, &arg->u.create.attr,
		       &arg->u.create.hint, &arg->u.create.dof, th);

	dt_write_unlock(env, dt_obj);

	CDEBUG(D_INFO, "%s: insert create reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int __out_tx_create(const struct lu_env *env, struct dt_object *obj,
			   struct lu_attr *attr, struct lu_fid *parent_fid,
			   struct dt_object_format *dof,
			   struct thandle_exec_args *ta,
			   struct update_reply_buf *reply_buf,
			   int index, int uidx, char *file, int line)
{
	struct tx_arg *arg;

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_create(env, obj, attr, NULL, dof,
				       ta->ta_handle);
	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_create_exec, out_tx_create_undo, file,
			  line);
	LASSERT(arg);

	/* release the object in out_trans_stop */
	lu_object_get(&obj->do_lu);
	arg->object = obj;
	arg->u.create.attr = *attr;
	if (parent_fid != NULL)
		arg->u.create.fid = *parent_fid;
	memset(&arg->u.create.hint, 0, sizeof(arg->u.create.hint));
	arg->u.create.dof  = *dof;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;

	return 0;
}

static int out_create(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct dt_object        *obj = tti->tti_u.update.tti_dt_object;
	struct dt_object_format	*dof = &tti->tti_u.update.tti_update_dof;
	struct obdo		*lobdo = &tti->tti_u.update.tti_obdo;
	struct lu_attr		*attr = &tti->tti_attr;
	struct lu_fid		*fid = NULL;
	struct obdo		*wobdo;
	int			size;
	int			rc;

	ENTRY;

	wobdo = update_param_buf(update, 0, &size);
	if (wobdo == NULL || size != sizeof(*wobdo)) {
		CERROR("%s: obdo is NULL, invalid RPC: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	if (tti->tti_u.update.tti_swab)
		lustre_swab_obdo(wobdo);
	lustre_get_wire_obdo(NULL, lobdo, wobdo);
	la_from_obdo(attr, lobdo, lobdo->o_valid);

	dof->dof_type = dt_mode_to_dft(attr->la_mode);
	if (S_ISDIR(attr->la_mode) && update->u_lens[1] > 0) {
		int size;

		fid = update_param_buf(update, 1, &size);
		if (fid == NULL || size != sizeof(*fid)) {
			CERROR("%s: invalid fid: rc = %d\n",
			       tsi->tsi_tgt_name, -EPROTO);
			RETURN(err_serious(-EPROTO));
		}
		if (tti->tti_u.update.tti_swab)
			lustre_swab_lu_fid(fid);
		if (!fid_is_sane(fid)) {
			CERROR("%s: invalid fid "DFID": rc = %d\n",
			       tsi->tsi_tgt_name, PFID(fid), -EPROTO);
			RETURN(err_serious(-EPROTO));
		}
	}

	if (lu_object_exists(&obj->do_lu))
		RETURN(-EEXIST);

	rc = out_tx_create(tsi->tsi_env, obj, attr, fid, dof,
			   &tti->tti_tea,
			   tti->tti_u.update.tti_update_reply,
			   tti->tti_u.update.tti_update_reply_index,
			   tti->tti_u.update.tti_update_index);

	RETURN(rc);
}

static int out_tx_attr_set_undo(const struct lu_env *env,
				struct thandle *th, struct tx_arg *arg)
{
	CERROR("%s: attr set undo "DFID" unimplemented yet!: rc = %d\n",
	       dt_obd_name(th->th_dev),
	       PFID(lu_object_fid(&arg->object->do_lu)), -ENOTSUPP);

	return -ENOTSUPP;
}

static int out_tx_attr_set_exec(const struct lu_env *env, struct thandle *th,
				struct tx_arg *arg)
{
	struct dt_object	*dt_obj = arg->object;
	int			rc;

	CDEBUG(D_OTHER, "%s: attr set "DFID"\n", dt_obd_name(th->th_dev),
	       PFID(lu_object_fid(&dt_obj->do_lu)));

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_attr_set(env, dt_obj, &arg->u.attr_set.attr, th, NULL);
	dt_write_unlock(env, dt_obj);

	CDEBUG(D_INFO, "%s: insert attr_set reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int __out_tx_attr_set(const struct lu_env *env,
			     struct dt_object *dt_obj,
			     const struct lu_attr *attr,
			     struct thandle_exec_args *th,
			     struct update_reply_buf *reply_buf, int index,
			     int uidx, char *file, int line)
{
	struct tx_arg		*arg;

	LASSERT(th->ta_handle != NULL);
	th->ta_err = dt_declare_attr_set(env, dt_obj, attr, th->ta_handle);
	if (th->ta_err != 0)
		return th->ta_err;

	arg = tx_add_exec(th, out_tx_attr_set_exec, out_tx_attr_set_undo,
			  file, line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->u.attr_set.attr = *attr;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	return 0;
}

static int out_attr_set(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct lu_attr		*attr = &tti->tti_attr;
	struct dt_object        *obj = tti->tti_u.update.tti_dt_object;
	struct obdo		*lobdo = &tti->tti_u.update.tti_obdo;
	struct obdo		*wobdo;
	int			 size;
	int			 rc;

	ENTRY;

	wobdo = update_param_buf(update, 0, &size);
	if (wobdo == NULL || size != sizeof(*wobdo)) {
		CERROR("%s: empty obdo in the update: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	attr->la_valid = 0;
	attr->la_valid = 0;
	if (tti->tti_u.update.tti_swab)
		lustre_swab_obdo(wobdo);
	lustre_get_wire_obdo(NULL, lobdo, wobdo);
	la_from_obdo(attr, lobdo, lobdo->o_valid);

	rc = out_tx_attr_set(tsi->tsi_env, obj, attr, &tti->tti_tea,
			     tti->tti_u.update.tti_update_reply,
			     tti->tti_u.update.tti_update_reply_index,
			     tti->tti_u.update.tti_update_index);

	RETURN(rc);
}

static int out_attr_get(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct obdo		*obdo = &tti->tti_u.update.tti_obdo;
	struct lu_attr		*la = &tti->tti_attr;
	struct dt_object        *obj = tti->tti_u.update.tti_dt_object;
	int			rc;

	ENTRY;

	if (!lu_object_exists(&obj->do_lu))
		RETURN(-ENOENT);

	dt_read_lock(env, obj, MOR_TGT_CHILD);
	rc = dt_attr_get(env, obj, la, NULL);
	if (rc)
		GOTO(out_unlock, rc);
	/*
	 * If it is a directory, we will also check whether the
	 * directory is empty.
	 * la_flags = 0 : Empty.
	 *          = 1 : Not empty.
	 */
	la->la_flags = 0;
	if (S_ISDIR(la->la_mode)) {
		struct dt_it		*it;
		const struct dt_it_ops	*iops;

		if (!dt_try_as_dir(env, obj))
			GOTO(out_unlock, rc = -ENOTDIR);

		iops = &obj->do_index_ops->dio_it;
		it = iops->init(env, obj, LUDA_64BITHASH, BYPASS_CAPA);
		if (!IS_ERR(it)) {
			int  result;
			result = iops->get(env, it, (const void *)"");
			if (result > 0) {
				int i;
				for (result = 0, i = 0; result == 0 && i < 3;
				     ++i)
					result = iops->next(env, it);
				if (result == 0)
					la->la_flags = 1;
			} else if (result == 0)
				/*
				 * Huh? Index contains no zero key?
				 */
				rc = -EIO;

			iops->put(env, it);
			iops->fini(env, it);
		}
	}

	obdo->o_valid = 0;
	obdo_from_la(obdo, la, la->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

out_unlock:
	dt_read_unlock(env, obj);

	CDEBUG(D_INFO, "%s: insert attr get reply %p index %d: rc = %d\n",
	       tsi->tsi_tgt_name, tti->tti_u.update.tti_update_reply,
	       0, rc);

	update_insert_reply(tti->tti_u.update.tti_update_reply, obdo,
			    sizeof(*obdo), 0, rc);
	RETURN(rc);
}

static int out_xattr_get(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct lu_buf		*lbuf = &tti->tti_buf;
	struct update_reply_buf *reply_buf = tti->tti_u.update.tti_update_reply;
	struct update_reply	*reply;
	struct dt_object        *obj = tti->tti_u.update.tti_dt_object;
	char			*name;
	int			 rc;

	ENTRY;

	name = (char *)update_param_buf(update, 0, NULL);
	if (name == NULL) {
		CERROR("%s: empty name for xattr get: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	reply = update_get_reply(reply_buf, 0, NULL);
	LASSERT(reply != NULL);

	lbuf->lb_buf = reply->ur_data;
	lbuf->lb_len = UPDATE_BUFFER_SIZE -
		       cfs_size_round((unsigned long)reply->ur_data -
				      (unsigned long)reply_buf);
	dt_read_lock(env, obj, MOR_TGT_CHILD);
	rc = dt_xattr_get(env, obj, lbuf, name, NULL);
	dt_read_unlock(env, obj);
	if (rc < 0) {
		lbuf->lb_len = 0;
		GOTO(out, rc);
	}
	if (rc == 0) {
		lbuf->lb_len = 0;
		GOTO(out, rc = -ENOENT);
	}
	lbuf->lb_len = rc;
	rc = 0;

	CDEBUG(D_INFO, "%s: "DFID" get xattr %s len %d\n",
	       tsi->tsi_tgt_name, PFID(lu_object_fid(&obj->do_lu)),
	       name, (int)lbuf->lb_len);
out:
	update_insert_reply(reply_buf, lbuf->lb_buf, lbuf->lb_len, 0, rc);
	RETURN(rc);
}

static int out_index_lookup(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	char			*name;
	int			 rc;

	ENTRY;

	if (!lu_object_exists(&obj->do_lu))
		RETURN(-ENOENT);

	name = (char *)update_param_buf(update, 0, NULL);
	if (name == NULL) {
		CERROR("%s: empty name for lookup: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	dt_read_lock(env, obj, MOR_TGT_CHILD);
	if (!dt_try_as_dir(env, obj))
		GOTO(out_unlock, rc = -ENOTDIR);

	rc = dt_lookup(env, obj, (struct dt_rec *)&tti->tti_fid1,
		(struct dt_key *)name, NULL);

	if (rc < 0)
		GOTO(out_unlock, rc);

	if (rc == 0)
		rc += 1;

	CDEBUG(D_INFO, "lookup "DFID" %s get "DFID" rc %d\n",
	       PFID(lu_object_fid(&obj->do_lu)), name,
	       PFID(&tti->tti_fid1), rc);

out_unlock:
	dt_read_unlock(env, obj);

	CDEBUG(D_INFO, "%s: insert lookup reply %p index %d: rc = %d\n",
	       tsi->tsi_tgt_name, tti->tti_u.update.tti_update_reply,
	       0, rc);

	update_insert_reply(tti->tti_u.update.tti_update_reply,
			    &tti->tti_fid1, sizeof(tti->tti_fid1), 0, rc);
	RETURN(rc);
}

static int out_tx_xattr_set_exec(const struct lu_env *env,
				 struct thandle *th,
				 struct tx_arg *arg)
{
	struct dt_object *dt_obj = arg->object;
	struct lu_buf	 *buf = &arg->u.xattr_set.buf;
	int rc;

	CDEBUG(D_INFO, "%s: set xattr buf %p name %s flag %d\n",
	       dt_obd_name(th->th_dev), arg->u.xattr_set.buf.lb_buf,
	       arg->u.xattr_set.name, arg->u.xattr_set.flags);

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	if (buf->lb_buf == NULL && buf->lb_len == 0) {
		rc = dt_xattr_del(env, dt_obj, arg->u.xattr_set.name, th,
				  NULL);
	} else {
		rc = dt_xattr_set(env, dt_obj, &arg->u.xattr_set.buf,
				  arg->u.xattr_set.name, arg->u.xattr_set.flags,
				  th, NULL);
	}
	dt_write_unlock(env, dt_obj);
	/**
	 * Ignore errors if this is LINK EA
	 **/
	if (unlikely(rc && !strncmp(arg->u.xattr_set.name, XATTR_NAME_LINK,
				    strlen(XATTR_NAME_LINK))))
		rc = 0;

	CDEBUG(D_INFO, "%s: insert xattr set reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int __out_tx_xattr_set(const struct lu_env *env,
			      struct dt_object *dt_obj,
			      const struct lu_buf *buf,
			      const char *name, int flags,
			      struct thandle_exec_args *ta,
			      struct update_reply_buf *reply_buf,
			      int index, int uidx, char *file, int line)
{
	struct tx_arg		*arg;

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_xattr_set(env, dt_obj, buf, name,
					  flags, ta->ta_handle);
	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_xattr_set_exec, NULL, file, line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->u.xattr_set.name = name;
	arg->u.xattr_set.flags = flags;
	arg->u.xattr_set.buf = *buf;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	arg->u.xattr_set.csum = 0;
	return 0;
}

static int out_xattr_set(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	struct lu_buf		*lbuf = &tti->tti_buf;
	char			*name;
	char			*buf;
	char			*tmp;
	int			 buf_len = 0;
	int			 flag;
	int			 rc;
	ENTRY;

	name = update_param_buf(update, 0, NULL);
	if (name == NULL) {
		CERROR("%s: empty name for xattr set: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	buf = (char *)update_param_buf(update, 1, &buf_len);

	lbuf->lb_buf = buf;
	lbuf->lb_len = buf_len;

	tmp = (char *)update_param_buf(update, 2, NULL);
	if (tmp == NULL) {
		CERROR("%s: empty flag for xattr set: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	if (tti->tti_u.update.tti_swab)
		__swab32s((__u32 *)tmp);
	flag = *(int *)tmp;

	rc = out_tx_xattr_set(tsi->tsi_env, obj, lbuf, name, flag,
			      &tti->tti_tea,
			      tti->tti_u.update.tti_update_reply,
			      tti->tti_u.update.tti_update_reply_index,
			      tti->tti_u.update.tti_update_index);
	RETURN(rc);
}

static int out_obj_ref_add(const struct lu_env *env,
			   struct dt_object *dt_obj,
			   struct thandle *th)
{
	int rc;

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_ref_add(env, dt_obj, th);
	dt_write_unlock(env, dt_obj);

	return rc;
}

static int out_obj_ref_del(const struct lu_env *env,
			   struct dt_object *dt_obj,
			   struct thandle *th)
{
	int rc;

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_ref_del(env, dt_obj, th);
	dt_write_unlock(env, dt_obj);

	return rc;
}

static int out_tx_ref_add_exec(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	struct dt_object *dt_obj = arg->object;
	int rc;

	rc = out_obj_ref_add(env, dt_obj, th);

	CDEBUG(D_INFO, "%s: insert ref_add reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);
	return rc;
}

static int out_tx_ref_add_undo(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	return out_obj_ref_del(env, arg->object, th);
}

static int __out_tx_ref_add(const struct lu_env *env,
			    struct dt_object *dt_obj,
			    struct thandle_exec_args *ta,
			    struct update_reply_buf *reply_buf,
			    int index, int uidx, char *file, int line)
{
	struct tx_arg	*arg;

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_ref_add(env, dt_obj, ta->ta_handle);
	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_ref_add_exec, out_tx_ref_add_undo, file,
			  line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	return 0;
}

/**
 * increase ref of the object
 **/
static int out_ref_add(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	int			 rc;

	ENTRY;

	rc = out_tx_ref_add(tsi->tsi_env, obj, &tti->tti_tea,
			    tti->tti_u.update.tti_update_reply,
			    tti->tti_u.update.tti_update_reply_index,
			    tti->tti_u.update.tti_update_index);
	RETURN(rc);
}

static int out_tx_ref_del_exec(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	struct dt_object	*dt_obj = arg->object;
	int			 rc;

	rc = out_obj_ref_del(env, dt_obj, th);

	CDEBUG(D_INFO, "%s: insert ref_del reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, 0);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int out_tx_ref_del_undo(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	return out_obj_ref_add(env, arg->object, th);
}

static int __out_tx_ref_del(const struct lu_env *env,
			    struct dt_object *dt_obj,
			    struct thandle_exec_args *ta,
			    struct update_reply_buf *reply_buf,
			    int index, int uidx, char *file, int line)
{
	struct tx_arg	*arg;

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_ref_del(env, dt_obj, ta->ta_handle);
	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_ref_del_exec, out_tx_ref_del_undo, file,
			  line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	return 0;
}

static int out_ref_del(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	int			 rc;

	ENTRY;

	if (!lu_object_exists(&obj->do_lu))
		RETURN(-ENOENT);

	rc = out_tx_ref_del(tsi->tsi_env, obj, &tti->tti_tea,
			    tti->tti_u.update.tti_update_reply,
			    tti->tti_u.update.tti_update_reply_index,
			    tti->tti_u.update.tti_update_index);
	RETURN(rc);
}

static int out_obj_index_insert(const struct lu_env *env,
				struct dt_object *dt_obj,
				const struct dt_rec *rec,
				const struct dt_key *key,
				struct thandle *th)
{
	int rc;

	CDEBUG(D_INFO, "%s: index insert "DFID" name: %s fid "DFID"\n",
	       dt_obd_name(th->th_dev), PFID(lu_object_fid(&dt_obj->do_lu)),
	       (char *)key, PFID((struct lu_fid *)rec));

	if (dt_try_as_dir(env, dt_obj) == 0)
		return -ENOTDIR;

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_insert(env, dt_obj, rec, key, th, NULL, 0);
	dt_write_unlock(env, dt_obj);

	return rc;
}

static int out_obj_index_delete(const struct lu_env *env,
				struct dt_object *dt_obj,
				const struct dt_key *key,
				struct thandle *th)
{
	int rc;

	CDEBUG(D_INFO, "%s: index delete "DFID" name: %s\n",
	       dt_obd_name(th->th_dev), PFID(lu_object_fid(&dt_obj->do_lu)),
	       (char *)key);

	if (dt_try_as_dir(env, dt_obj) == 0)
		return -ENOTDIR;

	dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
	rc = dt_delete(env, dt_obj, key, th, NULL);
	dt_write_unlock(env, dt_obj);

	if (rc == -ENOENT)
		rc = 0;

	return rc;
}

static int out_tx_index_insert_exec(const struct lu_env *env,
				    struct thandle *th, struct tx_arg *arg)
{
	struct dt_object *dt_obj = arg->object;
	int rc;

	rc = out_obj_index_insert(env, dt_obj, arg->u.insert.rec,
				  arg->u.insert.key, th);

	CDEBUG(D_INFO, "%s: insert idx insert reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int out_tx_index_insert_undo(const struct lu_env *env,
				    struct thandle *th, struct tx_arg *arg)
{
	return out_obj_index_delete(env, arg->object, arg->u.insert.key, th);
}

static int __out_tx_index_insert(const struct lu_env *env,
				 struct dt_object *dt_obj,
				 char *name, struct lu_fid *fid,
				 struct thandle_exec_args *ta,
				 struct update_reply_buf *reply_buf,
				 int index, int uidx, char *file, int line)
{
	struct tx_arg *arg;

	LASSERT(ta->ta_handle != NULL);

	if (dt_try_as_dir(env, dt_obj) == 0) {
		ta->ta_err = -ENOTDIR;
		return ta->ta_err;
	}

	ta->ta_err = dt_declare_insert(env, dt_obj,
				       (struct dt_rec *)fid,
				       (struct dt_key *)name,
				       ta->ta_handle);

	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_index_insert_exec,
			  out_tx_index_insert_undo, file,
			  line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	arg->u.insert.rec = (struct dt_rec *)fid;
	arg->u.insert.key = (struct dt_key *)name;

	return 0;
}

static int out_index_insert(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update	  *update = tti->tti_u.update.tti_update;
	struct dt_object  *obj = tti->tti_u.update.tti_dt_object;
	struct lu_fid	  *fid;
	char		  *name;
	int		   rc = 0;
	int		   size;

	ENTRY;

	name = (char *)update_param_buf(update, 0, NULL);
	if (name == NULL) {
		CERROR("%s: empty name for index insert: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	fid = (struct lu_fid *)update_param_buf(update, 1, &size);
	if (fid == NULL || size != sizeof(*fid)) {
		CERROR("%s: invalid fid: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		       RETURN(err_serious(-EPROTO));
	}

	if (tti->tti_u.update.tti_swab)
		lustre_swab_lu_fid(fid);

	if (!fid_is_sane(fid)) {
		CERROR("%s: invalid FID "DFID": rc = %d\n",
		       tsi->tsi_tgt_name, PFID(fid), -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	rc = out_tx_index_insert(tsi->tsi_env, obj, name, fid,
				 &tti->tti_tea,
				 tti->tti_u.update.tti_update_reply,
				 tti->tti_u.update.tti_update_reply_index,
				 tti->tti_u.update.tti_update_index);
	RETURN(rc);
}

static int out_tx_index_delete_exec(const struct lu_env *env,
				    struct thandle *th,
				    struct tx_arg *arg)
{
	int rc;

	rc = out_obj_index_delete(env, arg->object, arg->u.insert.key, th);

	CDEBUG(D_INFO, "%s: insert idx insert reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	return rc;
}

static int out_tx_index_delete_undo(const struct lu_env *env,
				    struct thandle *th,
				    struct tx_arg *arg)
{
	CERROR("%s: Oops, can not rollback index_delete yet: rc = %d\n",
	       dt_obd_name(th->th_dev), -ENOTSUPP);
	return -ENOTSUPP;
}

static int __out_tx_index_delete(const struct lu_env *env,
				 struct dt_object *dt_obj, char *name,
				 struct thandle_exec_args *ta,
				 struct update_reply_buf *reply_buf,
				 int index, int uidx, char *file, int line)
{
	struct tx_arg *arg;

	if (dt_try_as_dir(env, dt_obj) == 0) {
		ta->ta_err = -ENOTDIR;
		return ta->ta_err;
	}

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_delete(env, dt_obj,
				       (struct dt_key *)name,
				       ta->ta_handle);
	if (ta->ta_err != 0)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_index_delete_exec,
			  out_tx_index_delete_undo, file,
			  line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	arg->u.insert.key = (struct dt_key *)name;
	return 0;
}

static int out_index_delete(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	char			*name;
	int			 rc = 0;

	if (!lu_object_exists(&obj->do_lu))
		RETURN(-ENOENT);

	name = (char *)update_param_buf(update, 0, NULL);
	if (name == NULL) {
		CERROR("%s: empty name for index delete: rc = %d\n",
		       tsi->tsi_tgt_name, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	rc = out_tx_index_delete(tsi->tsi_env, obj, name, &tti->tti_tea,
				 tti->tti_u.update.tti_update_reply,
				 tti->tti_u.update.tti_update_reply_index,
				 tti->tti_u.update.tti_update_index);
	RETURN(rc);
}

static int out_tx_destroy_exec(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	struct dt_object *dt_obj = arg->object;
	int rc;

	rc = out_obj_destroy(env, dt_obj, th);

	CDEBUG(D_INFO, "%s: insert destroy reply %p index %d: rc = %d\n",
	       dt_obd_name(th->th_dev), arg->reply, arg->index, rc);

	if (arg->reply != NULL)
		update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

	RETURN(rc);
}

static int out_tx_destroy_undo(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	CERROR("%s: not support destroy undo yet!: rc = %d\n",
	       dt_obd_name(th->th_dev), -ENOTSUPP);
	return -ENOTSUPP;
}

static int __out_tx_destroy(const struct lu_env *env, struct dt_object *dt_obj,
			    struct thandle_exec_args *ta,
			    struct update_reply_buf *reply_buf,
			    int index, int uidx, char *file, int line)
{
	struct tx_arg *arg;

	LASSERT(ta->ta_handle != NULL);
	ta->ta_err = dt_declare_destroy(env, dt_obj, ta->ta_handle);
	if (ta->ta_err)
		return ta->ta_err;

	arg = tx_add_exec(ta, out_tx_destroy_exec, out_tx_destroy_undo,
			  file, line);
	LASSERT(arg);
	lu_object_get(&dt_obj->do_lu);
	arg->object = dt_obj;
	arg->reply = reply_buf;
	arg->index = index;
	arg->uidx = uidx;
	return 0;
}

static int out_destroy(struct tgt_session_info *tsi)
{
	struct tgt_thread_info	*tti = tgt_th_info(tsi->tsi_env);
	struct update		*update = tti->tti_u.update.tti_update;
	struct dt_object	*obj = tti->tti_u.update.tti_dt_object;
	struct lu_fid		*fid;
	int			 rc;
	ENTRY;

	fid = &update->u_fid;
	if (!fid_is_sane(fid)) {
		CERROR("%s: invalid FID "DFID": rc = %d\n",
		       tsi->tsi_tgt_name, PFID(fid), -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	if (!lu_object_exists(&obj->do_lu))
		RETURN(-ENOENT);

	rc = out_tx_destroy(tsi->tsi_env, obj, &tti->tti_tea,
			    tti->tti_u.update.tti_update_reply,
			    tti->tti_u.update.tti_update_reply_index,
			    tti->tti_u.update.tti_update_index);

	RETURN(rc);
}

#define DEF_OUT_HNDL(opc, name, flags, fn)     \
[opc - OBJ_CREATE] = {					\
	.th_name    = name,				\
	.th_fail_id = 0,				\
	.th_opc     = opc,				\
	.th_flags   = flags,				\
	.th_act     = fn,				\
	.th_fmt     = NULL,				\
	.th_version = 0,				\
}

#define out_handler mdt_handler
static struct tgt_handler out_update_ops[] = {
	DEF_OUT_HNDL(OBJ_CREATE, "obj_create", MUTABOR | HABEO_REFERO,
		     out_create),
	DEF_OUT_HNDL(OBJ_DESTROY, "obj_create", MUTABOR | HABEO_REFERO,
		     out_destroy),
	DEF_OUT_HNDL(OBJ_REF_ADD, "obj_ref_add", MUTABOR | HABEO_REFERO,
		     out_ref_add),
	DEF_OUT_HNDL(OBJ_REF_DEL, "obj_ref_del", MUTABOR | HABEO_REFERO,
		     out_ref_del),
	DEF_OUT_HNDL(OBJ_ATTR_SET, "obj_attr_set",  MUTABOR | HABEO_REFERO,
		     out_attr_set),
	DEF_OUT_HNDL(OBJ_ATTR_GET, "obj_attr_get",  HABEO_REFERO,
		     out_attr_get),
	DEF_OUT_HNDL(OBJ_XATTR_SET, "obj_xattr_set", MUTABOR | HABEO_REFERO,
		     out_xattr_set),
	DEF_OUT_HNDL(OBJ_XATTR_GET, "obj_xattr_get", HABEO_REFERO,
		     out_xattr_get),
	DEF_OUT_HNDL(OBJ_INDEX_LOOKUP, "obj_index_lookup", HABEO_REFERO,
		     out_index_lookup),
	DEF_OUT_HNDL(OBJ_INDEX_INSERT, "obj_index_insert",
		     MUTABOR | HABEO_REFERO, out_index_insert),
	DEF_OUT_HNDL(OBJ_INDEX_DELETE, "obj_index_delete",
		     MUTABOR | HABEO_REFERO, out_index_delete),
};

struct tgt_handler *out_handler_find(__u32 opc)
{
	struct tgt_handler *h;

	h = NULL;
	if (OBJ_CREATE <= opc && opc < OBJ_LAST) {
		h = &out_update_ops[opc - OBJ_CREATE];
		LASSERTF(h->th_opc == opc, "opcode mismatch %d != %d\n",
			 h->th_opc, opc);
	} else {
		h = NULL; /* unsupported opc */
	}
	return h;
}

static int out_handle_updates(const struct lu_env *env, struct dt_device *dt,
			      struct update_buf *ubuf,
			      struct update_reply_buf *reply_buf)
{
	struct tgt_session_info		*tsi = tgt_ses_info(env);
	struct tgt_thread_info		*tti = tgt_th_info(env);
	struct thandle_exec_args	*ta = &tti->tti_tea;
	__u64				old_batchid = -1;
	int				count;
	int				i;
	int				rc = 0;
	int				rc1;
	int				index = 0;

	ENTRY;

	count = ubuf->ub_count;
	if (count <= 0)
		RETURN(err_serious(-EPROTO));

	update_dump_buf(ubuf, D_INFO);
	if (reply_buf != NULL)
		update_init_reply_buf(reply_buf, count);
	tti->tti_u.update.tti_update_reply = reply_buf;
	memset(ta, 0, sizeof(*ta));
	for (i = 0; i < count; i++) {
		struct tgt_handler	*h;
		struct dt_object	*dt_obj;
		struct update		*update;

		update = (struct update *)update_buf_get(ubuf, i, NULL);

		if (tti->tti_u.update.tti_swab)
			lustre_swab_update(update);

		if (!fid_is_sane(&update->u_fid)) {
			CERROR("Invalid FID "DFID": rc = %d\n",
			       PFID(&update->u_fid), -EPROTO);
			GOTO(out, rc = err_serious(-EPROTO));
		}

		/* skip the update for remote object */
		if (update->u_index !=
		    dt->dd_lu_dev.ld_site->ld_seq_site->ss_node_id) {
			CDEBUG(D_INFO, "%s: "DFID" skip %s\n",
			       tgt_name(tsi->tsi_tgt), PFID(&update->u_fid),
			       update_op_str(update->u_type));
			continue;
		}

		dt_obj = dt_locate(env, dt, &update->u_fid);
		if (IS_ERR(dt_obj))
			GOTO(out, rc = PTR_ERR(dt_obj));

		tti->tti_u.update.tti_dt_object = dt_obj;
		tti->tti_u.update.tti_update = update;
		tti->tti_u.update.tti_update_reply_index = index;
		tti->tti_u.update.tti_update_index = i;

		h = out_handler_find(update->u_type);
		if (unlikely(h == NULL)) {
			CERROR("Unsupported opc: 0x%x: rc = %d\n",
			       update->u_type, -ENOTSUPP);
			GOTO(next, rc = -ENOTSUPP);
		}

		CDEBUG(D_INFO, "%s: handler %s "DFID" old_batchid "
		       LPU64" u_batchid "LPU64"\n", tsi->tsi_tgt_name,
		       h->th_name, PFID(&update->u_fid), old_batchid,
		       update->u_batchid);
		/* For real modification RPC, start transaction and check if
		 * the update has been executed */
		if (h->th_flags & MUTABOR) {
			if (old_batchid == -1) {
				old_batchid = update->u_batchid;
				rc = out_tx_start(env, dt, ta, ubuf);
				if (rc < 0)
					GOTO(next, rc);	
			} else if (old_batchid != update->u_batchid) {
				/* Stop the current update transaction,
				 * create a new one */
				rc = out_tx_end(env, ta, ubuf);
				if (rc != 0)
					GOTO(next, rc);	

				rc = out_tx_start(env, dt, ta, ubuf);
				if (rc != 0)
					GOTO(next, rc);	
				old_batchid = update->u_batchid;
			}
			if (out_check_resent(env, dt, dt_obj, out_reconstruct,
					     reply_buf, index))
				GOTO(next, rc);
		}
		rc = h->th_act(tsi);
		index++;
next:
		lu_object_put(env, &dt_obj->do_lu);
		if (rc < 0)
			GOTO(out, rc);
	}
out:
	if (ta->ta_handle != NULL) {
		rc1 = out_tx_end(env, ta, ubuf);
		if (rc == 0)
			rc = rc1;
	}
	RETURN(rc);
}

int out_do_updates(const struct lu_env *env, struct dt_device *dt,
		   struct update_buf *ubuf)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct tgt_session_info	*tsi = tgt_ses_info(env);
	int rc;
	ENTRY;

	/* Initialize tgt session needed by out_handler */
	memset(tsi, 0, sizeof(*tsi));
	tsi->tsi_tgt_name = dt->dd_lu_dev.ld_obd->obd_name;
	tsi->tsi_env = env;
	tti->tti_u.update.tti_resend = 0;
	tti->tti_u.update.tti_log_update = 0;

	rc = out_handle_updates(env, dt, ubuf, NULL);

	RETURN(rc);

}
EXPORT_SYMBOL(out_do_updates);

/**
 * Check whether the update needs to be logged on disk.
 * return 1: log the update.
 * return 0: Do not need log update.
 **/
static int out_check_log_update(const struct lu_env *env, struct dt_device *dt,
				struct update_buf *ubuf)
{
	int i;

	/* If all updates in the ubuf will be executed on this target, it does 
	 * not need log the update */ 
	for (i = 0; i < ubuf->ub_count; i++) {
		struct update *update = update_buf_get(ubuf, i, NULL);
		if (update->u_index !=
		    dt->dd_lu_dev.ld_site->ld_seq_site->ss_node_id)
			return 1;
	}
	return 0;
}
/**
 * Object updates between Targets. Because all the updates has been
 * dis-assemblied into object updates at sender side, so OUT will
 * call OSD API directly to execute these updates.
 *
 * In DNE phase I all of the updates in the request need to be executed
 * in one transaction, and the transaction has to be synchronously.
 *
 * Please refer to lustre/include/lustre/lustre_idl.h for req/reply
 * format.
 */
int out_handle(struct tgt_session_info *tsi)
{
	const struct lu_env		*env = tsi->tsi_env;
	struct tgt_thread_info		*tti = tgt_th_info(env);
	struct req_capsule		*pill = tsi->tsi_pill;
	struct ptlrpc_request		*req = pill->rc_req;
	struct dt_device		*dt = tsi->tsi_tgt->lut_bottom;
	struct update_buf		*ubuf;
	struct update_reply_buf		*reply_buf;
	int				 bufsize;
	int				 rc = 0;

	ENTRY;

	req_capsule_set(pill, &RQF_UPDATE_OBJ);
	ubuf = req_capsule_client_get(pill, &RMF_UPDATE);
	if (ubuf == NULL) {
		CERROR("%s: No buf!: rc = %d\n", tsi->tsi_tgt_name,
		       -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	bufsize = req_capsule_get_size(pill, &RMF_UPDATE, RCL_CLIENT);
	if (bufsize != update_buf_size(ubuf)) {
		CERROR("%s: invalid bufsize %d: rc = %d\n",
		       tsi->tsi_tgt_name, bufsize, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	if (ubuf->ub_magic != UPDATE_REQUEST_MAGIC) {
		CERROR("%s: invalid update buffer magic %x expect %x: "
		       "rc = %d\n", tsi->tsi_tgt_name, ubuf->ub_magic,
		       UPDATE_REQUEST_MAGIC, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	req_capsule_set_size(pill, &RMF_UPDATE_REPLY, RCL_SERVER,
			     UPDATE_BUFFER_SIZE);
	rc = req_capsule_server_pack(pill);
	if (rc != 0) {
		CERROR("%s: Can't pack response: rc = %d\n",
		       tsi->tsi_tgt_name, rc);
		RETURN(rc);
	}

	/* Prepare the update reply buffer */
	reply_buf = req_capsule_server_get(pill, &RMF_UPDATE_REPLY);
	if (reply_buf == NULL)
		RETURN(err_serious(-EPROTO));

	tti->tti_u.update.tti_log_update = out_check_log_update(env, dt, ubuf);

	/* set swab flag */
	if (ptlrpc_req_need_swab(req))
		tti->tti_u.update.tti_swab = 1;
	else
		tti->tti_u.update.tti_swab = 0;

	/* set resend flag */
	if (unlikely(lustre_msg_get_flags(req->rq_reqmsg) & MSG_RESENT)) {
		if (req_xid_is_last(req)) {
			tti->tti_u.update.tti_resend = 1;
		} else {
			tti->tti_u.update.tti_resend = 0;
			DEBUG_REQ(D_HA, req, "no reply for RESENT req (have "
				  LPD64")",
			req->rq_export->exp_target_data.ted_lcd->lcd_last_xid);
		}
	} else {
		tti->tti_u.update.tti_resend = 0;
	}
	rc = out_handle_updates(env, dt, ubuf, reply_buf);
	RETURN(rc);
}

int out_log_handle(struct tgt_session_info *tsi)
{
	const struct lu_env		*env = tsi->tsi_env;
	struct tgt_thread_info		*tti = tgt_th_info(env);
	struct req_capsule		*pill = tsi->tsi_pill;
	struct dt_device		*dt = tsi->tsi_tgt->lut_bottom;
	struct update_buf		*ubuf;
	struct update_reply_buf		*reply_buf;
	struct llog_cookie		*cookies;
	int				 bufsize;
	int				 count;
	int				 i;
	int				 master_index;
	int				 rc = 0;

	ENTRY;

	req_capsule_set(pill, &RQF_UPDATE_LOG_CANCEL);
	ubuf = req_capsule_client_get(pill, &RMF_UPDATE);
	if (ubuf == NULL) {
		CERROR("%s: No buf!: rc = %d\n", tsi->tsi_tgt_name,
		       -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	bufsize = req_capsule_get_size(pill, &RMF_UPDATE, RCL_CLIENT);
	if (bufsize != update_buf_size(ubuf)) {
		CERROR("%s: invalid bufsize %d: rc = %d\n",
		       tsi->tsi_tgt_name, bufsize, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	if (ubuf->ub_magic != UPDATE_REQUEST_MAGIC) {
		CERROR("%s: invalid update buffer magic %x expect %x: "
		       "rc = %d\n", tsi->tsi_tgt_name, ubuf->ub_magic,
		       UPDATE_REQUEST_MAGIC, -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	count = ubuf->ub_count;
	if (count <= 0) {
		CERROR("%s: empty update: rc = %d\n", tsi->tsi_tgt_name,
		       -EPROTO);
		RETURN(err_serious(-EPROTO));
	}

	req_capsule_set_size(pill, &RMF_UPDATE_REPLY, RCL_SERVER,
			     UPDATE_BUFFER_SIZE);
	rc = req_capsule_server_pack(pill);
	if (rc != 0) {
		CERROR("%s: Can't pack response: rc = %d\n",
		       tsi->tsi_tgt_name, rc);
		RETURN(rc);
	}

	/* Prepare the update reply buffer */
	reply_buf = req_capsule_server_get(pill, &RMF_UPDATE_REPLY);
	if (reply_buf == NULL)
		RETURN(err_serious(-EPROTO));
	update_init_reply_buf(reply_buf, count);
	tti->tti_u.update.tti_update_reply = reply_buf;

	master_index = update_buf_master_idx(ubuf);

	OBD_ALLOC(cookies, sizeof(*cookies) * count);
	if (cookies == NULL)
		GOTO(trans_stop, rc = -ENOMEM);

	for (i = 0; i < count; i++) {
		struct update		*update;
		struct llog_cookie	*cookie;

		update = (struct update *)update_buf_get(ubuf, i, NULL);

		if (ptlrpc_req_need_swab(pill->rc_req))
			lustre_swab_update(update);

		cookie = update_param_buf(update, 0, NULL);
		if (cookie == NULL)
			GOTO(trans_stop, rc = -EINVAL);

		cookies[i] = *cookie;
	}

	rc = dt_update_llog_cancel(env, dt, cookies, count, master_index);
trans_stop:
	for (i = 0; i < count; i++)
		update_insert_reply(reply_buf, NULL, 0, i, rc);
	if (cookies != NULL)
		OBD_FREE(cookies, sizeof(*cookies) * count);
	RETURN(rc);
}

/* Only open is supported, no new llog can be created remotely */
int out_update_llog_open(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct req_capsule	*pill = tsi->tsi_pill;
	struct dt_device	*dt = tsi->tsi_tgt->lut_bottom;
	struct obd_llog_group	*olg;
	struct llog_handle	*loghandle;
	struct llogd_body	*body;
	struct llogd_body	*repbody;
	struct llog_ctxt	*ctxt;
	int			index;
	struct llog_catid	*cid = &tti->tti_u.update.tti_cid;
	struct lu_fid		*fid = &tti->tti_fid1;
	int			rc;
	ENTRY;

	body = req_capsule_client_get(pill, &RMF_LLOGD_BODY);
	if (body == NULL)
		RETURN(-EFAULT);

	rc = req_capsule_server_pack(pill);
	if (rc)
		RETURN(-ENOMEM);

	index = pill->rc_req->rq_export->exp_mdt_data.med_index;
	/* Only support catlist log right now */
	if (body->lgd_llh_flags & LLOG_F_IS_CATLIST) {
		logid_to_fid(&body->lgd_logid, fid);
		rc = llog_osd_get_cat_list(env, dt, index, 1, cid, fid);
		if (rc) {
			CERROR("%s: can't get id from catalogs: rc = %d\n",
			       tsi->tsi_tgt_name, rc);
			RETURN(rc);
		}

		if (logid_id(&cid->lci_logid) == 0)
			RETURN(-ENOENT);

		CDEBUG(D_HA, "%s: open llog "DFID" index %d\n", tsi->tsi_tgt_name,
		       PFID(fid), index);
	} else {
		cid->lci_logid = body->lgd_logid;
	}

	olg = dt_update_find_olg(dt, index);
	if (olg == NULL)
		RETURN(-ENOENT);

	ctxt = llog_group_get_ctxt(olg, body->lgd_ctxt_idx);
	if (ctxt == NULL) {
		CDEBUG(D_WARNING, "%s: no ctxt. group=%p idx=%d\n",
		       tsi->tsi_tgt_name, olg, body->lgd_ctxt_idx);
		RETURN(-ENODEV);
	}

	rc = llog_open(env, ctxt, &loghandle, &cid->lci_logid, NULL,
		       LLOG_OPEN_EXISTS);
	if (rc)
		GOTO(out_ctxt, rc);

	repbody = req_capsule_server_get(pill, &RMF_LLOGD_BODY);
	repbody->lgd_logid = loghandle->lgh_id;
	repbody->lgd_index = index;

	llog_origin_close(env, loghandle);
	EXIT;
out_ctxt:
	llog_ctxt_put(ctxt);
	return rc;
}

int out_update_llog_next_block(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct req_capsule	*pill = tsi->tsi_pill;
	struct dt_device	*dt = tsi->tsi_tgt->lut_bottom;
	struct obd_llog_group	*olg;
	struct llog_handle	*loghandle;
	struct llogd_body	*body;
	struct llogd_body	*repbody;
	struct llog_ctxt	*ctxt;
	__u32			 flags;
	void			*ptr;
	int			 index;
	int			 rc;

	ENTRY;

	body = req_capsule_client_get(pill, &RMF_LLOGD_BODY);
	if (body == NULL)
		RETURN(err_serious(-EFAULT));

	req_capsule_set_size(pill, &RMF_EADATA, RCL_SERVER, LLOG_CHUNK_SIZE);
	rc = req_capsule_server_pack(pill);
	if (rc)
		RETURN(err_serious(-ENOMEM));

	CDEBUG(D_HA, "%s: next block "DOSTID":%u flags %x\n",
	       tsi->tsi_tgt_name, POSTID(&body->lgd_logid.lgl_oi),
	       body->lgd_logid.lgl_ogen, body->lgd_llh_flags);

	index = pill->rc_req->rq_export->exp_mdt_data.med_index;
	olg = dt_update_find_olg(dt, index);
	if (olg == NULL)
		RETURN(-ENOENT);

	ctxt = llog_group_get_ctxt(olg, body->lgd_ctxt_idx);
	if (ctxt == NULL) {
		CDEBUG(D_WARNING, "%s: no ctxt. group=%p idx=%d\n",
		       tsi->tsi_tgt_name, olg, body->lgd_ctxt_idx);
		RETURN(-ENODEV);
	}

	rc = llog_open(env, ctxt, &loghandle,
		       &body->lgd_logid, NULL, LLOG_OPEN_EXISTS);
	if (rc)
		GOTO(out_ctxt, rc);

	flags = body->lgd_llh_flags;
	rc = llog_init_handle(env, loghandle, flags, NULL);
	if (rc)
		GOTO(out_close, rc);

	repbody = req_capsule_server_get(pill, &RMF_LLOGD_BODY);
	*repbody = *body;
	repbody->lgd_logid.lgl_ogen = index;
	ptr = req_capsule_server_get(pill, &RMF_EADATA);
	rc = llog_next_block(env, loghandle,
			     &repbody->lgd_saved_index, repbody->lgd_index,
			     &repbody->lgd_cur_offset, ptr, LLOG_CHUNK_SIZE);
	if (rc)
		GOTO(out_close, rc);
	EXIT;
out_close:
	llog_origin_close(env, loghandle);
out_ctxt:
	llog_ctxt_put(ctxt);
	return rc;
}

int out_update_llog_prev_block(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct req_capsule	*pill = tsi->tsi_pill;
	struct dt_device	*dt = tsi->tsi_tgt->lut_bottom;
	struct obd_llog_group	*olg;
	struct llog_handle	*loghandle;
	struct llogd_body	*body;
	struct llogd_body	*repbody;
	struct llog_ctxt	*ctxt;
	__u32			 flags;
	void			*ptr;
	int			 rc;
	int			index;
	ENTRY;

	body = req_capsule_client_get(pill, &RMF_LLOGD_BODY);
	if (body == NULL)
		RETURN(err_serious(-EFAULT));

	req_capsule_set_size(pill, &RMF_EADATA, RCL_SERVER, LLOG_CHUNK_SIZE);
	rc = req_capsule_server_pack(pill);
	if (rc)
		RETURN(err_serious(-ENOMEM));

	index = pill->rc_req->rq_export->exp_mdt_data.med_index;

	CDEBUG(D_HA, "%s: prev block "DOSTID":%u\n",
	       tsi->tsi_tgt_name, POSTID(&body->lgd_logid.lgl_oi),
	       body->lgd_logid.lgl_ogen);

	olg = dt_update_find_olg(dt, index);
	if (olg == NULL)
		RETURN(-ENOENT);

	ctxt = llog_group_get_ctxt(olg, body->lgd_ctxt_idx);
	if (ctxt == NULL) {
		CDEBUG(D_WARNING, "%s: no ctxt. group=%p idx=%d\n",
		       tsi->tsi_tgt_name, olg, body->lgd_ctxt_idx);
		RETURN(-ENODEV);
	}

	rc = llog_open(env, ctxt, &loghandle, &body->lgd_logid, NULL,
		       LLOG_OPEN_EXISTS);
	if (rc)
		GOTO(out_ctxt, rc);

	flags = body->lgd_llh_flags;
	rc = llog_init_handle(env, loghandle, flags, NULL);
	if (rc)
		GOTO(out_close, rc);

	repbody = req_capsule_server_get(pill, &RMF_LLOGD_BODY);
	*repbody = *body;
	repbody->lgd_logid.lgl_ogen = index;

	ptr = req_capsule_server_get(pill, &RMF_EADATA);
	rc = llog_prev_block(env, loghandle, body->lgd_index, ptr,
			     LLOG_CHUNK_SIZE);
	if (rc)
		GOTO(out_close, rc);

	EXIT;
out_close:
	llog_origin_close(env, loghandle);
out_ctxt:
	llog_ctxt_put(ctxt);
	return rc;
}

int out_update_llog_read_header(struct tgt_session_info *tsi)
{
	const struct lu_env	*env = tsi->tsi_env;
	struct req_capsule	*pill = tsi->tsi_pill;
	struct dt_device	*dt = tsi->tsi_tgt->lut_bottom;
	struct obd_llog_group	*olg;
	struct llog_handle	*loghandle;
	struct llogd_body	*body;
	struct llog_log_hdr	*hdr;
	struct llog_ctxt	*ctxt;
	__u32			 flags;
	int			 rc;
	int			 index;

	ENTRY;

	body = req_capsule_client_get(pill, &RMF_LLOGD_BODY);
	if (body == NULL)
		RETURN(-EFAULT);

	rc = req_capsule_server_pack(pill);
	if (rc)
		RETURN(-ENOMEM);

	CDEBUG(D_HA, "%s: read header "DOSTID":%u\n",
	       tsi->tsi_tgt_name, POSTID(&body->lgd_logid.lgl_oi),
	       body->lgd_logid.lgl_ogen);

	index = pill->rc_req->rq_export->exp_mdt_data.med_index;
	olg = dt_update_find_olg(dt, index);
	if (olg == NULL)
		RETURN(-ENOENT);

	ctxt = llog_group_get_ctxt(olg, body->lgd_ctxt_idx);
	if (ctxt == NULL) {
		CDEBUG(D_WARNING, "%s: no ctxt. group=%p idx=%d\n",
		       tsi->tsi_tgt_name, olg, body->lgd_ctxt_idx);
		RETURN(-ENODEV);
	}

	rc = llog_open(env, ctxt, &loghandle, &body->lgd_logid, NULL,
		       LLOG_OPEN_EXISTS);
	if (rc)
		GOTO(out_ctxt, rc);

	/*
	 * llog_init_handle() reads the llog header
	 */
	flags = body->lgd_llh_flags;
	rc = llog_init_handle(env, loghandle, flags, NULL);
	if (rc)
		GOTO(out_close, rc);

	hdr = req_capsule_server_get(pill, &RMF_LLOG_LOG_HDR);
	*hdr = *loghandle->lgh_hdr;
	EXIT;
out_close:
	llog_origin_close(env, loghandle);
out_ctxt:
	llog_ctxt_put(ctxt);
	return rc;
}

int out_update_llog_close(struct tgt_session_info *tsi)
{
	struct req_capsule	*pill = tsi->tsi_pill;
	int			rc;

	ENTRY;

	rc = req_capsule_server_pack(pill);
	if (rc != 0)
		RETURN(rc);
	RETURN(0);
}

struct tgt_handler tgt_out_handlers[] = {
TGT_UPDATE_HDL(MUTABOR,	UPDATE_OBJ,	out_handle),
TGT_UPDATE_HDL(MUTABOR,	UPDATE_LOG_CANCEL,	out_log_handle),
};
EXPORT_SYMBOL(tgt_out_handlers);

struct tgt_handler tgt_out_llog_handlers[] = {
TGT_LLOG_HDL    (0,	LLOG_ORIGIN_HANDLE_CREATE,	out_update_llog_open),
TGT_LLOG_HDL    (0,	LLOG_ORIGIN_HANDLE_NEXT_BLOCK,	out_update_llog_next_block),
TGT_LLOG_HDL    (0,	LLOG_ORIGIN_HANDLE_READ_HEADER,	out_update_llog_read_header),
TGT_LLOG_HDL_VAR(0,	LLOG_ORIGIN_HANDLE_CLOSE,	out_update_llog_close),
TGT_LLOG_HDL    (0,	LLOG_ORIGIN_HANDLE_PREV_BLOCK,	out_update_llog_prev_block),
};
EXPORT_SYMBOL(tgt_out_llog_handlers);

