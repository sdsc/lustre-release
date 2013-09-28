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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdt/out_handler.c
 *
 * Object update handler between targets.
 *
 * Author: di.wang <di.wang@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>
/*
 * struct OBD_{ALLOC,FREE}*()
 */
#include <obd_support.h>
/* struct ptlrpc_request */
#include <lustre_net.h>
/* struct obd_export */
#include <lustre_export.h>
/* struct obd_device */
#include <obd.h>
/* lu2dt_dev() */
#include <dt_object.h>
#include <lustre_mds.h>
#include <lustre_mdt.h>
#include "mdt_internal.h"
#ifdef HAVE_QUOTA_SUPPORT
# include <lustre_quota.h>
#endif
#include <lustre_acl.h>
#include <lustre_param.h>
#include <lustre_fsfilt.h>
#include <obd_cksum.h>

static const char dot[] = ".";
static const char dotdot[] = "..";

static void __out_tx_create(const struct lu_env *, struct lu_object *,
			    struct lu_attr *, struct lu_fid *,
			    struct dt_object_format *,
			    struct thandle_exec_args *, struct update_reply *,
			    int, char *, int);
#define out_tx_create(env, obj, attr, fid, dof, th, reply, idx) \
	__out_tx_create(env,obj,attr,fid,dof,th,reply,idx,__FILE__,__LINE__)

static void __out_tx_attr_set(const struct lu_env *, struct lu_object *,
			      const struct lu_attr *,struct thandle_exec_args *,
		       	      struct update_reply *, int, char *, int);
#define out_tx_attr_set(env,obj,attr,th, reply, idx) \
	__out_tx_attr_set(env,obj,attr,th,reply,idx,__FILE__,__LINE__)

static void __out_tx_xattr_set(const struct lu_env *, struct lu_object *,
			       const struct lu_buf *, const char *name, int flags,
			       struct thandle_exec_args *, struct update_reply *,
			       int, char *, int);
#define out_tx_xattr_set(env,obj,buf,name,fl,th,reply,idx) \
	__out_tx_xattr_set(env,obj,buf,name,fl,th,reply,idx,__FILE__,__LINE__)

static void __out_tx_ref_add(const struct lu_env *, struct lu_object *,
			     struct thandle_exec_args *, struct update_reply *,
			     int, char *, int);
#define out_tx_ref_add(env,obj,th,reply,idx) \
	__out_tx_ref_add(env,obj,th,reply,idx,__FILE__,__LINE__)

static void __out_tx_ref_del(const struct lu_env *, struct lu_object *,
			     struct thandle_exec_args *, struct update_reply *,
			     int, char *, int);
#define out_tx_ref_del(env,obj,th,reply,idx) \
	__out_tx_ref_del(env,obj,th,reply,idx,__FILE__,__LINE__)

static void __out_tx_index_insert(const struct lu_env *, struct lu_object *,
				  char *, struct lu_fid *,
				  struct thandle_exec_args *,
				  struct update_reply *, int, char *, int);

#define out_tx_index_insert(env,obj,th,name, fid, reply,idx) \
	__out_tx_index_insert(env,obj,th,name,fid,reply,idx,__FILE__,__LINE__)


static void __out_tx_index_delete(const struct lu_env *, struct lu_object *,
				  char *, struct thandle_exec_args *,
				  struct update_reply *, int, char *, int);
#define out_tx_index_delete(env, obj, th, name, reply,idx) \
	__out_tx_index_delete(env,obj,th,name,reply,idx, __FILE__, __LINE__)

static void __out_tx_object_destroy(const struct lu_env *, struct lu_object *,
				    struct ost_body *,
				    struct thandle_exec_args *,
				    struct update_reply *, int, char *, int);

#define out_tx_object_destroy(env, obj, th, body, reply,idx) \
	__out_tx_object_destroy(env, obj, th, body, reply, idx, __FILE__,__LINE__)

struct tx_arg *tx_add_exec(struct thandle_exec_args *ta, tx_exec_func_t func,
			   tx_exec_func_t undo, char *file, int line)
{
	int i;
 
	LASSERT(ta);
	LASSERT(func);

	i = ta->ta_argno;
	LASSERT(i < UPDATE_MAX_OPS);
 
	ta->ta_argno++;
 
	ta->ta_args[i].exec_fn = func;
	ta->ta_args[i].undo_fn = undo;
	ta->ta_args[i].file    = file;
	ta->ta_args[i].line    = line;

	return &ta->ta_args[i];
}

static void out_tx_start(const struct lu_env *env, struct mdt_device *mdt,
			 struct thandle_exec_args *th, int sync)
{
	struct dt_device *dt = mdt->mdt_bottom;

	LASSERT(th->ta_handle == NULL);
	th->ta_handle = dt_trans_create(env, dt);
	if (IS_ERR(th->ta_handle)) {
		CERROR("start handle error %ld \n", PTR_ERR(th->ta_handle));
		return;
	}
	th->ta_dev = dt;
	/*For phase I, we should sync for cross-ref operation*/
	th->ta_handle->th_sync = 1;
	return;
}

static int out_trans_start(const struct lu_env *env, struct dt_device *dt,
			   struct thandle *th)
{
	return dt_trans_start(env, dt, th);
}

static int out_trans_stop(const struct lu_env *env, struct dt_device *dt,
			  int err, struct thandle *th)
{
	th->th_result = err;
	return dt_trans_stop(env, dt, th);
}

int out_tx_end(struct mdt_thread_info *info, struct thandle_exec_args *th)
{
	struct thandle_exec_args *_th = &info->mti_handle;
	int i = 0, rc;

	LASSERT(th == _th);
	LASSERT(th->ta_dev);
	LASSERT(th->ta_handle);

	if ((rc = th->ta_err)) {
		CDEBUG(D_OTHER, "error during declaration: %d\n", rc);
		GOTO(stop, rc);
	}

	rc = out_trans_start(info->mti_env, th->ta_dev, th->ta_handle);
	if (unlikely(rc))
		GOTO(stop, rc);

	CDEBUG(D_OTHER, "start execution\n");
	for (i = 0; i < th->ta_argno; i++) {
		rc = th->ta_args[i].exec_fn(info->mti_env, th->ta_handle,
					    &th->ta_args[i]);
		if (unlikely(rc)) {
			CERROR("error during execution of #%u from %s:%d: %d\n",
			       i, th->ta_args[i].file, th->ta_args[i].line, rc);
			while (--i >= 0) {
				LASSERTF(th->ta_args[i].undo_fn,
				    "can't undo changes, hope for failover!\n");
				th->ta_args[i].undo_fn(info->mti_env,
						       th->ta_handle,
						       &th->ta_args[i]);
			}
			break;
		}
	}
stop:
	CDEBUG(D_OTHER, "executed %u/%u: %d\n", i, th->ta_argno, rc);
	out_trans_stop(info->mti_env, th->ta_dev, rc, th->ta_handle);
	th->ta_handle = NULL;
	th->ta_argno = 0;
	th->ta_err = 0;

	RETURN(rc);
}

static struct dt_object *out_get_dt_obj(struct lu_object *obj)
{
	struct mdt_device *mdt;
	struct dt_device *dt;
	struct lu_object *bottom_obj;

	mdt = lu2mdt_dev(obj->lo_dev);
	dt = mdt->mdt_bottom;

	bottom_obj = lu_object_locate(obj->lo_header,
				      dt->dd_lu_dev.ld_type);
	LASSERT(obj != NULL);

	return lu2dt_obj(bottom_obj);
}

/**
 * To match the current transaction and lock model,
 * it needs to do following
 *	      lock_object;
 *		 start_transaction;
 *		      ....
 *		 end_transaction;
 *	      unlock_object;
 * In phase I, updates from one request should
 * at most includes 2 objects, and they will be locked
 * before the transaction. So if it finds more
 * than 2 objects in these updates, it will just return -ENOTSUPP now
 **/
static int out_find_lock_object(struct mdt_thread_info *info,
				struct lu_fid *fid,
				struct lu_object **pobj,
				enum update_lock_mode mode)
{
	struct dt_object *dt_obj;
	struct lu_object *obj;
	int i;

	LASSERT(mode == UPDATE_READ_LOCK || mode == UPDATE_WRITE_LOCK);

	obj = lu_object_find(info->mti_env,
			     &info->mti_mdt->mdt_md_dev.md_lu_dev,
			     fid, NULL);
	if (IS_ERR(obj))
		return (PTR_ERR(obj));

	*pobj = obj;
	for (i = 0; i < UPDATE_OBJ_COUNT; i++) {
		if (info->mti_u.update.mti_update_obj[i].ul_obj == obj) {
			if (info->mti_u.update.mti_update_obj[i].ul_mode !=
								      mode)
				RETURN(-ENOTSUPP);
			/* The object is already hold on
			 * mti_u.update.mti_update_obj[i].ul_obj，
			 * and this put is for lu_object_find above */
			lu_object_put(info->mti_env, obj);
			RETURN(0);
		} else if (info->mti_u.update.mti_update_obj[i].ul_obj == NULL) {
			info->mti_u.update.mti_update_obj[i].ul_obj = obj;
			break;
		}
	}

	if (i == UPDATE_OBJ_COUNT) {
		int j;

		CERROR("Lookup "DFID", but there are already two objs!\n",
		       PFID(fid));
		for (j = 0; j < UPDATE_OBJ_COUNT; j++) {
			struct update_object *update =
					  &info->mti_u.update.mti_update_obj[j];
			CERROR("{"DFID":%d}\n", PFID(&obj->lo_header->loh_fid),
			       update->ul_mode);
		}
		return(-ENOTSUPP);
	}
 
	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL);

	if (mode == UPDATE_READ_LOCK)
		dt_read_lock(info->mti_env, dt_obj, 0);
	else if (mode == UPDATE_WRITE_LOCK)
		dt_write_lock(info->mti_env, dt_obj, 0);
	else
		LBUG();

	info->mti_u.update.mti_update_obj[i].ul_mode = mode;
	CDEBUG(D_INFO, "Set object %p "DFID", mode: %d i %d\n",
	       obj, PFID(&obj->lo_header->loh_fid), mode, i);
	RETURN(0);
}

static void out_unlock_objects(struct mdt_thread_info *info)
{
	struct lu_object *obj;
	int	       i;
	int	       mode;

	for(i = 0; i < UPDATE_OBJ_COUNT; i++) {
		struct dt_object *dt_obj;

		obj = info->mti_u.update.mti_update_obj[i].ul_obj;
		if (obj == NULL)
			continue;

		dt_obj = out_get_dt_obj(obj);

		mode = info->mti_u.update.mti_update_obj[i].ul_mode;
		LASSERT(mode != 0);
		if (mode == UPDATE_READ_LOCK)
			dt_read_unlock(info->mti_env, dt_obj);
		else if (mode == UPDATE_WRITE_LOCK)
			dt_write_unlock(info->mti_env, dt_obj);
		else
			LBUG();
		CDEBUG(D_INFO, "unlock the object %p "DFID" mode %d\n",
		       obj, PFID(&obj->lo_header->loh_fid), mode);
		lu_object_put(info->mti_env, obj);
		info->mti_u.update.mti_update_obj[i].ul_obj = NULL;
		info->mti_u.update.mti_update_obj[i].ul_mode = 0;
	}
}
 
static void out_common_reconstruct(struct mdt_thread_info *mti,
				   struct lu_object *obj,
				   struct update_reply *reply,
				   int index)
{
	update_insert_reply(reply, NULL, 0, index, 0);
	return;
}

typedef void (*out_reconstruct_t)(struct mdt_thread_info *mti,
				  struct lu_object *obj,
				  struct update_reply *reply,
				  int index);

static inline int out_common_check_resent(struct mdt_thread_info *info,
				   	  struct lu_object *obj, 
				   	  out_reconstruct_t reconstruct,
				   	  struct update_reply *reply,
				   	  int index)
{
	struct ptlrpc_request *req = mdt_info_req(info);
	ENTRY;
	
	if (lustre_msg_get_flags(req->rq_reqmsg) & MSG_RESENT) {
		/* FIXME: We should check XID, but for phase I, the
 		 * cross-MDT operation will be synchronously, and all
 		 * of the updates for a remote creation are in one RPC,
 		 * so we will know whether the operation has been
 		 * executed by checking whether the object exists */
		if (info->mti_u.update.mti_update_resend) {
			reconstruct(info, obj, reply, index);
			RETURN(1);
		}
	}
	RETURN(0);
}

static inline int out_create_check_resent(struct mdt_thread_info *info,
				   	  struct lu_object *obj, 
				   	  out_reconstruct_t reconstruct,
				   	  struct update_reply *reply,
				   	  int index)
{
	struct ptlrpc_request *req = mdt_info_req(info);
	
	ENTRY;
	
	if (lustre_msg_get_flags(req->rq_reqmsg) & MSG_RESENT) {
		/* FIXME: We should check XID, but for phase I, the
 		 * cross-MDT operation will be synchronously, and all
 		 * of the updates for a remote creation are in one RPC,
 		 * so we will know whether the operation has been
 		 * executed by checking whether the object exists */
		if (info->mti_u.update.mti_update_resend) {
			reconstruct(info, obj, reply, index);
			RETURN(1);
		}

		if (lu_object_exists(obj) > 0) { 
		    	info->mti_u.update.mti_update_resend = 1;
			reconstruct(info, obj, reply, index);
			RETURN(1);
		}
	}
	RETURN(0);
}

static inline int out_index_delete_check_resent(struct mdt_thread_info *info,
				   	  	struct lu_object *obj, 
				   	  	out_reconstruct_t reconstruct,
				   	  	struct update_reply *reply,
				   	  	int index, char *name)
{
	struct ptlrpc_request *req = mdt_info_req(info);

	ENTRY;

	if (lustre_msg_get_flags(req->rq_reqmsg) & MSG_RESENT) {
		const struct lu_env     *env = info->mti_env;
		struct dt_object	*dt_obj;
		int rc;

		if (info->mti_u.update.mti_update_resend) {
			reconstruct(info, obj, reply, index);
			RETURN(1);
		}

		if (name == NULL)
			RETURN(0);

		dt_obj = out_get_dt_obj(obj);
		LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));

		if (dt_try_as_dir(info->mti_env, dt_obj) == 0) {
			CERROR("%s: "DFID" is not directory\n",
				mdt2obd_dev(info->mti_mdt)->obd_name,
				PFID(lu_object_fid(obj)));
			return -ENOTDIR;
		}
		rc = dt_lookup(env, dt_obj, (struct dt_rec *)&info->mti_tmp_fid1,
			       (struct dt_key *)name, NULL);
		if (rc == -ENOENT) {
			reconstruct(info, obj, reply, index);
			info->mti_u.update.mti_update_resend = 1;
			RETURN(1);
		}
       	}
	RETURN(0);
}

static void out_create_reconstruct(struct mdt_thread_info *mti,
				   struct lu_object *obj,
				   struct update_reply *reply,
				   int index)
{
	struct dt_object *dt_obj = out_get_dt_obj(obj);
	struct lu_attr   *la = &mti->mti_attr.ma_attr;
	int		  rc;

	rc = dt_attr_get(mti->mti_env, dt_obj, la, NULL);
	update_insert_reply(reply, (void*)la, sizeof(*la), index, rc);
	return;
}

int out_tx_create_undo(const struct lu_env *env, struct thandle *th,
			struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	rc = dt_destroy(env, dt_obj, th);
 
	/* we don't like double failures */
	LASSERT(rc == 0);
 
	return rc;
}
 
int out_tx_create_exec(const struct lu_env *env, struct thandle *th,
		       struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;

	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));

	CDEBUG(D_OTHER, "create "DFID": dof %u, mode %o\n",
	       PFID(&arg->object->lo_header->loh_fid),
	       arg->u.create.dof.dof_type,
	       arg->u.create.attr.la_mode & S_IFMT);

	rc = dt_create(env, dt_obj,
		       (struct lu_attr *) &arg->u.create.attr,
		       &arg->u.create.hint, &arg->u.create.dof, th);
	if (rc != 0)
		GOTO(out, rc);
#if 0
	if (arg->u.create.dof.dof_type == DFT_DIR) {
		struct lu_fid *fid;

		if (unlikely(dt_try_as_dir(env, dt_obj) == 0))
			GOTO(out, rc = -ENOTDIR);

		fid = &dt_obj->do_lu.lo_header->loh_fid;
		rc = dt_insert(env, dt_obj, (const struct dt_rec *)fid,
			       (const struct dt_key *)dot, th, NULL, 1);
		if (rc != 0)
			GOTO(out, rc);

		rc = dt_insert(env, dt_obj,
			       (const struct dt_rec *)&arg->u.create.fid,
			       (const struct dt_key *)dotdot,
			       th, NULL, 1);
		if (rc != 0)
			GOTO(out, rc);
	}
#endif
out:
	CDEBUG(D_INFO, "insert create reply mode %o index %d \n",
	       arg->u.create.attr.la_mode, arg->index);
 	update_insert_reply(arg->reply, (void*)&arg->u.create.attr,
 			    sizeof(arg->u.create.attr), arg->index, rc);
	return rc;
}

static void __out_tx_create(const struct lu_env *env,
			    struct lu_object *obj,
			    struct lu_attr *attr,
			    struct lu_fid *parent_fid,
			    struct dt_object_format *dof,
			    struct thandle_exec_args *th,
			    struct update_reply *reply,
			    int index, char *file, int line)
{
	struct dt_object *dt_obj;
	int rc;
 
	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	/* don't proceed if any of previous declaration failed */
	if (th->ta_err)
		return;
 
	LASSERT(th->ta_handle != NULL);
	rc = dt_declare_create(env, dt_obj, attr, NULL, dof, th->ta_handle);
	th->ta_err = rc;

	if (likely(rc == 0)) {
		struct tx_arg *arg;

		arg = tx_add_exec(th, out_tx_create_exec,
				  out_tx_create_undo, file, line);
		LASSERT(arg);
		arg->object = obj;
		arg->u.create.attr = *attr;
		if (parent_fid)
			arg->u.create.fid = *parent_fid;
		memset(&arg->u.create.hint, 0, sizeof(arg->u.create.hint));
		arg->u.create.dof  = *dof;
 		arg->reply = reply;
 		arg->index = index;
	}
}

/**
 * Create the object.
 **/
static int out_create(struct mdt_thread_info *info)
{
        struct update             *update = info->mti_u.update.mti_update;
        struct lu_attr            *attr;
        struct lu_fid             *fid;
        struct lu_fid             *fid1 = NULL;
        struct dt_object_format   *dof = &info->mti_u.update.mti_update_dof;
        struct lu_object          *obj = NULL;
        int                        rc;

        attr = (struct lu_attr *)update_param_buf(update, 0, NULL);
        LASSERT(attr != NULL);
        lu_attr_le_to_cpu(attr, attr);

        fid = &update->u_fid;
        fid_le_to_cpu(fid, fid);
        LASSERT(fid_is_sane(fid));

        dof->dof_type = dt_mode_to_dft(attr->la_mode);

        if (S_ISDIR(attr->la_mode)) {
                int size;

                fid1 = (struct lu_fid *)update_param_buf(update, 1, &size);
                LASSERT(fid1 != NULL && size == sizeof(*fid1));
                fid_le_to_cpu(fid1, fid1);
                LASSERT(fid_is_sane(fid1));
        }

        rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
        if (rc)
                RETURN(rc);

        LASSERT(obj != NULL);

        if (out_create_check_resent(info, obj, out_create_reconstruct,
                             info->mti_u.update.mti_update_reply,
                             info->mti_u.update.mti_update_reply_index))
                return 0;

        if (info->mti_handle.ta_handle == NULL) {
                out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
                             1);
                if (IS_ERR(info->mti_handle.ta_handle))
                        return PTR_ERR(info->mti_handle.ta_handle);
        }

        out_tx_create(info->mti_env, obj, attr, fid1, dof, &info->mti_handle,
                      info->mti_u.update.mti_update_reply,
                      info->mti_u.update.mti_update_reply_index);

        return 0;
}

static int out_tx_attr_set_undo(const struct lu_env *env, struct thandle *th,
				struct tx_arg *arg)
{
	struct dt_object *dt_obj;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CERROR("attr set undo "DFID" unimplemented yet!\n",
	       PFID(&arg->object->lo_header->loh_fid));
 
	LBUG();
	return 0;
}
 
static int out_tx_attr_set_exec(const struct lu_env *env, struct thandle *th,
				struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CDEBUG(D_OTHER, "attr set "DFID"\n",
	       PFID(&arg->object->lo_header->loh_fid));
 
	rc = dt_attr_set(env, dt_obj,
			(const struct lu_attr *)&arg->u.attr_set.attr,
			 th, NULL);
 
	update_insert_reply(arg->reply, NULL, 0, arg->index, rc);
 
	return rc;
}

static void __out_tx_attr_set(const struct lu_env *env,
			      struct lu_object *obj,
			      const struct lu_attr *attr,
			      struct thandle_exec_args *th,
			      struct update_reply *reply, int index,
			      char *file, int line)
{
	struct dt_object *dt_obj;
	int rc;

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
	if (th->ta_err)
		return;

	LASSERT(th->ta_handle != NULL);
	rc = dt_declare_attr_set(env, dt_obj, attr, th->ta_handle);
	th->ta_err = rc;

	if (likely(rc == 0)) {
		struct tx_arg *arg;

		arg = tx_add_exec(th, out_tx_attr_set_exec,
				  out_tx_attr_set_undo, file, line);
		LASSERT(arg);
		arg->object = obj;
		arg->u.attr_set.attr = *attr;
		arg->reply = reply;
		arg->index = index;
	}
}

/**
 * Attr set
 **/
static int out_attr_set(struct mdt_thread_info *info)
{
	struct update           *update = info->mti_u.update.mti_update;
	struct lu_attr          *attr;
	struct lu_fid           *fid;
	struct lu_object        *obj = NULL;
	int                      size;
	int                      rc;
 
	attr = (struct lu_attr *)update_param_buf(update, 0, &size);
	LASSERT(attr != NULL && size == sizeof(*attr));
	lu_attr_le_to_cpu(attr, attr);
 
	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));
 
	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;
 
	LASSERT(obj != NULL);
 
	if (out_common_check_resent(info, obj, out_common_reconstruct,
      				    info->mti_u.update.mti_update_reply,
	     			    info->mti_u.update.mti_update_reply_index))
		return 0;

	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}
 
	out_tx_attr_set(info->mti_env, obj, (const struct lu_attr *)attr,
			&info->mti_handle, info->mti_u.update.mti_update_reply,
			info->mti_u.update.mti_update_reply_index);
	return 0;
}

/**
 * attr get
 **/
static int out_attr_get(struct mdt_thread_info *info)
{
	struct update 		*update = info->mti_u.update.mti_update;
	const struct lu_env     *env = info->mti_env;
	struct lu_attr          *la = &info->mti_attr.ma_attr;
	struct lu_fid           *fid;
	struct lu_object        *obj = NULL;
	struct dt_object        *dt_obj;
	int                      rc;
 
	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));
 
	rc = out_find_lock_object(info, fid, &obj, UPDATE_READ_LOCK);
	if (rc)
		GOTO(out, rc);

	LASSERT(obj != NULL);

	if (lu_object_exists(obj) <= 0)
		GOTO(out, rc = -ENOENT);

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));

	rc = dt_attr_get(env, dt_obj, la, NULL);
	if (rc)
		GOTO(out, rc);

	/**
	 * If it is a directory, we will also check whether the
	 * directory is empty.
	 * la_flags = 0 : Empty.
	 *          = 1 : Not empty.
	 **/
	la->la_flags = 0;
	if (S_ISDIR(la->la_mode)) {
		struct dt_it     *it;
		const struct dt_it_ops *iops;

		if (!dt_try_as_dir(env, dt_obj))
			GOTO(out, rc = -ENOTDIR);

		iops = &dt_obj->do_index_ops->dio_it;
		it = iops->init(env, dt_obj, LUDA_64BITHASH, BYPASS_CAPA);
		if (!IS_ERR(it)) {
			int  result;
			result = iops->get(env, it, (const void *)"");
			if (result > 0) {
				int i;
				for (result = 0, i = 0;
						 result == 0 && i < 3; ++i)
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
out:
	update_insert_reply(info->mti_u.update.mti_update_reply, la,
			    sizeof(*la), 0, rc);
	return rc;
}

static int out_xattr_get(struct mdt_thread_info *info)
{
	struct update 		*update = info->mti_u.update.mti_update;
	const struct lu_env     *env = info->mti_env;
	struct lu_buf       	*lbuf = &info->mti_buf;
        struct update_reply     *reply = info->mti_u.update.mti_update_reply;
	struct lu_fid           *fid;
	struct lu_object        *obj = NULL;
	struct dt_object        *dt_obj;
	char                	*name;
	void                	*ptr;
	int                  	 rc;

	name = (char *)update_param_buf(update, 0, NULL);
	LASSERT(name != NULL);

	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));

	ptr = update_get_buf_internal(reply, 0, NULL);
	LASSERT(ptr != NULL);
	
	rc = out_find_lock_object(info, fid, &obj, UPDATE_READ_LOCK);
	if (rc)
		GOTO(out, rc);

	LASSERT(obj != NULL);

	if (lu_object_exists(obj) <= 0)
		GOTO(out, rc = -ENOENT);

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));

	/* The first 4 bytes(int) are used to store the result */
	lbuf->lb_buf = (char *)ptr + sizeof(int);
	lbuf->lb_len = UPDATE_BUFFER_SIZE - sizeof(struct update_reply);
	rc = dt_xattr_get(env, dt_obj, lbuf, name, NULL);
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
		mdt_obd_name(info->mti_mdt), PFID(fid), name,
		(int)lbuf->lb_len);
out:
	*(int *)ptr = rc;
	reply->ur_lens[0] = lbuf->lb_len + sizeof(int);
	return rc;
}

static int out_index_lookup(struct mdt_thread_info *info)
{
	struct update 		*update = info->mti_u.update.mti_update;
	const struct lu_env     *env = info->mti_env;
	struct lu_fid		*fid;
	struct lu_object        *obj = NULL;
	struct dt_object        *dt_obj;
	char                	*name;
	int                  	 rc;

	name = (char *)update_param_buf(update, 0, NULL);
	LASSERT(name != NULL);

	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));

	rc = out_find_lock_object(info, fid, &obj, UPDATE_READ_LOCK);
	if (rc)
		GOTO(out, rc);

	LASSERT(obj != NULL);

	if (lu_object_exists(obj) <= 0)
		GOTO(out, rc = -ENOENT);

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));

	if (!dt_try_as_dir(env, dt_obj))
		GOTO(out, rc = -ENOTDIR);

	rc = dt_lookup(env, dt_obj, (struct dt_rec *)&info->mti_tmp_fid1,
		(struct dt_key *)name, NULL);

	if (rc < 0)
		GOTO(out, rc);

	if (rc == 0)
		rc += 1;

	CDEBUG(D_INFO, "lookup "DFID" %s get "DFID" rc %d\n", PFID(fid), name,
		PFID(&info->mti_tmp_fid1), rc);
	lustre_swab_lu_fid(&info->mti_tmp_fid1);
out:
	update_insert_reply(info->mti_u.update.mti_update_reply,
			    &info->mti_tmp_fid1, sizeof(info->mti_tmp_fid1),
			    0, rc);
	return rc;
}

static int out_tx_xattr_set_exec(const struct lu_env *env, struct thandle *th,
				 struct tx_arg *arg)
{
        struct dt_object *dt_obj;
        int rc;

        LASSERT(arg->object);
        dt_obj = out_get_dt_obj(arg->object);
        LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
        LASSERT(dt_write_locked(env, dt_obj));

        CDEBUG(D_OTHER, "attr set "DFID"\n",
               PFID(&arg->object->lo_header->loh_fid));

        rc = dt_xattr_set(env, dt_obj,
                          (const struct lu_buf *)&arg->u.xattr_set.buf,
                          arg->u.xattr_set.name, arg->u.xattr_set.flags,
                          th, NULL);
	/**
	 * Ignore errors if this is LINK EA
	 **/
        if (unlikely(rc && !strncmp(arg->u.xattr_set.name, XATTR_NAME_LINK,
                                    strlen(XATTR_NAME_LINK)))) {
                CERROR("ignore linkea error: %d\n", rc);
                rc = 0;
        }

        update_insert_reply(arg->reply, NULL, 0, arg->index, rc);

        CDEBUG(D_INFO, "set xattr buf %p name %s flag %d \n",
               arg->u.xattr_set.buf.lb_buf, arg->u.xattr_set.name,
               arg->u.xattr_set.flags);

        return rc;
}

static void __out_tx_xattr_set(const struct lu_env *env, struct lu_object *obj,
			       const struct lu_buf *buf, const char *name,
			       int flags, struct thandle_exec_args *th,
			       struct update_reply *reply, int index,
			       char *file, int line)
{
	struct dt_object *dt_obj;
	int rc;

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
	if (th->ta_err)
		return;

	LASSERT(th->ta_handle != NULL);
	rc = dt_declare_xattr_set(env, dt_obj, buf, name, flags, th->ta_handle);
	th->ta_err = rc;

	if (likely(rc == 0)) {
		struct tx_arg *arg;

		arg = tx_add_exec(th, out_tx_xattr_set_exec, NULL, file, line);
		LASSERT(arg);
		arg->object = obj;
		arg->u.xattr_set.name = name;
		arg->u.xattr_set.flags = flags;
		arg->u.xattr_set.buf = *buf;
		arg->reply = reply;
		arg->index = index;
		arg->u.xattr_set.csum = 0;
	}
}

/**
 * Create the object.
 **/
static int out_xattr_set(struct mdt_thread_info *info)
{
	struct update 	    *update = info->mti_u.update.mti_update;
	struct lu_fid       *fid;
	struct lu_object    *obj = NULL;
	struct lu_buf       *lbuf = &info->mti_buf;
	char                *name;
	char                *buf;
	char                *tmp;
	int                  buf_len = 0;
	int                  fl;
	int                  rc;
 
	name = (char *)update_param_buf(update, 0, NULL);
	LASSERT(name != NULL);
 
	buf = (char *)update_param_buf(update, 1, &buf_len);
	LASSERT(buf != NULL && buf_len != 0);
 
	lbuf->lb_buf = buf;
	lbuf->lb_len = buf_len;
 
	tmp = (char *)update_param_buf(update, 3, NULL);
	LASSERT(tmp != NULL);
	fl = le32_to_cpu(*(int*)tmp);

	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));

	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;

	LASSERT(obj != NULL);

	if (out_common_check_resent(info, obj, out_common_reconstruct,
		      	     info->mti_u.update.mti_update_reply,
			     info->mti_u.update.mti_update_reply_index))
		return 0;

	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}

	out_tx_xattr_set(info->mti_env, obj, lbuf, name, fl, &info->mti_handle,
			 info->mti_u.update.mti_update_reply,
			 info->mti_u.update.mti_update_reply_index);

	return 0;
}
 
static int out_tx_ref_add_exec(const struct lu_env *env, struct thandle *th,
				struct tx_arg *arg)
{
	struct dt_object *dt_obj;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CDEBUG(D_OTHER, "ref add "DFID"\n",
	       PFID(&arg->object->lo_header->loh_fid));
 
	dt_ref_add(env, dt_obj, th);
 
 	update_insert_reply(arg->reply, NULL, 0, arg->index, 0);
	return 0;
}
 
static int out_tx_ref_add_undo(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	struct dt_object *dt_obj;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CDEBUG(D_OTHER, "ref del "DFID"\n",
	       PFID(&arg->object->lo_header->loh_fid));
 
	dt_ref_del(env, dt_obj, th);
 
	return 0;
}

static void __out_tx_ref_add(const struct lu_env *env,
			     struct lu_object *obj,
			     struct thandle_exec_args *th,
			     struct update_reply *reply,
			     int index, char *file, int line)
{
	struct tx_arg *arg;
	struct dt_object *dt_obj;
 
	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	if (th->ta_err)
		return;
 
	LASSERT(th->ta_handle != NULL);
	dt_declare_ref_add(env, dt_obj, th->ta_handle);
 
	arg = tx_add_exec(th, out_tx_ref_add_exec, out_tx_ref_add_undo, file,
			  line);
	LASSERT(arg);
	arg->object = obj;
 	arg->reply = reply;
 	arg->index = index;
}

/**
 * increase ref of the object
 **/
static int out_ref_add(struct mdt_thread_info *info)
{
	struct update 	      *update = info->mti_u.update.mti_update;
	struct lu_object      *obj = NULL;
	struct lu_fid         *fid;
	int                    rc;
 
	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));
 
	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;
 
	LASSERT(obj != NULL);

	if (out_common_check_resent(info, obj, out_common_reconstruct,
				info->mti_u.update.mti_update_reply,
				info->mti_u.update.mti_update_reply_index))
		return 0;

	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}
 
	out_tx_ref_add(info->mti_env, obj, &info->mti_handle,
		       info->mti_u.update.mti_update_reply,
		       info->mti_u.update.mti_update_reply_index);
	return 0;
}
 
static int out_tx_ref_del_exec(const struct lu_env *env, struct thandle *th,
				struct tx_arg *arg)
{
	out_tx_ref_add_undo(env, th, arg);
 	update_insert_reply(arg->reply, NULL, 0, arg->index, 0);
	return 0;
}
 
static int out_tx_ref_del_undo(const struct lu_env *env, struct thandle *th,
			       struct tx_arg *arg)
{
	/* FIXME: what if the object has been deleted */
	return out_tx_ref_add_exec(env, th, arg);
}

static void __out_tx_ref_del(const struct lu_env *env,
			     struct lu_object *obj,
			     struct thandle_exec_args *th,
			     struct update_reply *reply,
			     int index, char *file, int line)
{
	struct tx_arg *arg;
	struct dt_object *dt_obj;
 
	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	if (th->ta_err)
		return;
 
	LASSERT(th->ta_handle != NULL);
	dt_declare_ref_del(env, dt_obj, th->ta_handle);
 
	arg = tx_add_exec(th, out_tx_ref_del_exec, out_tx_ref_del_undo, file,
			  line);
	LASSERT(arg);
	arg->object = obj;
 	arg->reply = reply;
 	arg->index = index;
}

/**
 * increase ref of the object
 **/
static int out_ref_del(struct mdt_thread_info *info)
{
	struct update 	    *update = info->mti_u.update.mti_update;
	struct lu_object    *obj;
	struct lu_fid       *fid;
	int                  rc;
 
	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));
 
	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;
 
	LASSERT(obj != NULL);
 
	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}
 
	out_tx_ref_del(info->mti_env, obj, &info->mti_handle,
		       info->mti_u.update.mti_update_reply,
		       info->mti_u.update.mti_update_reply_index);
	return 0;
}
 
static int out_tx_index_insert_exec(const struct lu_env *env,
				    struct thandle *th, struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CDEBUG(D_OTHER, "index insert "DFID" name: %s fid "DFID" \n",
	       PFID(&arg->object->lo_header->loh_fid),
	       (char *)arg->u.insert.key,
	       PFID((struct lu_fid *)arg->u.insert.rec));

	if (dt_try_as_dir(env, dt_obj) == 0) {
		CERROR(""DFID" is not directory\n",
		       PFID(lu_object_fid(arg->object)));
		return -ENOTDIR;
	}

	rc = dt_insert(env, dt_obj, arg->u.insert.rec, arg->u.insert.key,
		       th, NULL, 0);
 
	update_insert_reply(arg->reply, NULL, 0, arg->index, rc);
 
	return rc;
}
 
static int out_tx_index_insert_undo(const struct lu_env *env,
				    struct thandle *th, struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
 	CDEBUG(D_OTHER, "index delete "DFID" name: %s\n",
 	       PFID(&arg->object->lo_header->loh_fid),
 	       (char *)arg->u.insert.key);
 
	if (dt_try_as_dir(env, dt_obj) == 0) {
		CERROR(""DFID" is not directory\n",
		       PFID(lu_object_fid(arg->object)));
		return -ENOTDIR;
	}

	/* FIXME: no declare here */
	rc = dt_delete(env, dt_obj, arg->u.insert.key, th, NULL);
 
	update_insert_reply(arg->reply, NULL, 0, arg->index, rc);
 
	return rc;
}
 
static void __out_tx_index_insert(const struct lu_env *env,
				  struct lu_object *obj,
				  char *name, struct lu_fid *fid,
				  struct thandle_exec_args *th,
				  struct update_reply *reply,
				  int index, char *file, int line)
{
	struct tx_arg *arg;
	struct dt_object *dt_obj;
	int    rc = 0;
 
	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	if (th->ta_err)
		return;
 
	LASSERT(th->ta_handle != NULL);

	if (lu_object_exists(obj)) {
		/* FIXME: for creation, index declaration is done inside
		 * object declare, which might needs to be fixed. */
		if (dt_try_as_dir(env, dt_obj) == 0) {
			CERROR(""DFID" is not directory\n",
			       PFID(lu_object_fid(obj)));
			th->ta_err = -ENOTDIR;
			return;
		}
		rc = dt_declare_insert(env, dt_obj, (struct dt_rec *)fid,
					(struct dt_key *)name, th->ta_handle);
	}

	th->ta_err = rc;
	if (likely(rc == 0)) {
		arg = tx_add_exec(th, out_tx_index_insert_exec,
				  out_tx_index_insert_undo, file,
				  line);
		LASSERT(arg);
		arg->object = obj;
		arg->reply = reply;
		arg->index = index;
		arg->u.insert.rec = (struct dt_rec *)fid;
		arg->u.insert.key = (struct dt_key *)name;
	}
}

/**
 * index insert
 **/
static int out_index_insert(struct mdt_thread_info *info)
{
	struct update 	   *update = info->mti_u.update.mti_update;
	struct lu_fid      *fid;
	struct lu_fid      *fid1;
	char               *name;
	struct lu_object   *obj;
	int                 rc = 0;
	int                 size;
 
	name = (char *)update_param_buf(update, 0, NULL);
	LASSERT(name != NULL);
 
	fid1 = (struct lu_fid *)update_param_buf(update, 1, &size);
	LASSERT(fid1 != NULL && size == sizeof(*fid1));
	fid_le_to_cpu(fid1, fid1);
	LASSERT(fid_is_sane(fid1));
 
	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));

	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;

	LASSERT(obj != NULL);
	if (out_common_check_resent(info, obj, out_common_reconstruct,
		      	     info->mti_u.update.mti_update_reply,
			     info->mti_u.update.mti_update_reply_index))
		return 0;

	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}
 
	CDEBUG(D_INFO, ""DFID" insert entry %s:"DFID"\n", PFID(fid), name,
	       PFID(fid1));
	out_tx_index_insert(info->mti_env, obj, name, fid1, &info->mti_handle,
			    info->mti_u.update.mti_update_reply,
			    info->mti_u.update.mti_update_reply_index);
	return rc;
}

static int out_tx_index_delete_exec(const struct lu_env *env,
				    struct thandle *th,
				    struct tx_arg *arg)
{
	return out_tx_index_insert_undo(env, th, arg);
}

static int out_tx_index_delete_undo(const struct lu_env *env,
				    struct thandle *th,
				    struct tx_arg *arg)
{
	/* FIXME: we do not have FID for insert here */
	return out_tx_index_insert_exec(env, th, arg);
}

static void __out_tx_index_delete(const struct lu_env *env,
				  struct lu_object *obj, char *name,
				  struct thandle_exec_args *th,
				  struct update_reply *reply,
				  int index, char *file, int line)
{
	struct tx_arg *arg;
	struct dt_object *dt_obj;
	int    rc;

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));

	if (th->ta_err)
		return;

	LASSERT(th->ta_handle != NULL);
	if (dt_try_as_dir(env, dt_obj) == 0) {
		CERROR(""DFID" is not directory\n",
		       PFID(lu_object_fid(obj)));
		th->ta_err = -ENOTDIR;
		return;
	}
	rc = dt_declare_delete(env, dt_obj, (struct dt_key *)name,
			       th->ta_handle);
	th->ta_err = rc;
	if (likely(rc == 0)) {
		arg = tx_add_exec(th, out_tx_index_delete_exec,
				  out_tx_index_delete_undo, file,
				  line);
		LASSERT(arg);
		arg->object = obj;
		arg->reply = reply;
		arg->index = index;
		arg->u.insert.key = (struct dt_key *)name;
	}
}

/**
 * index insert
 **/
static int out_index_delete(struct mdt_thread_info *info)
{
	struct update		*update = info->mti_u.update.mti_update;
	struct lu_fid		*fid;
	char			*name;
	struct lu_object	*obj;
	int			rc = 0;

	name = (char *)update_param_buf(update, 0, NULL);
	LASSERT(name != NULL);

	fid = &update->u_fid;
	fid_le_to_cpu(fid, fid);
	LASSERT(fid_is_sane(fid));

	rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
	if (rc)
		return rc;

	LASSERT(obj != NULL);

	if (out_index_delete_check_resent(info, obj, out_common_reconstruct,
				info->mti_u.update.mti_update_reply,
				info->mti_u.update.mti_update_reply_index,
			     	name))
		return 0;
 

	if (info->mti_handle.ta_handle == NULL) {
		out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
			     1);
		if (IS_ERR(info->mti_handle.ta_handle))
			return PTR_ERR(info->mti_handle.ta_handle);
	}

	CDEBUG(D_INFO, ""DFID" delete entry %s\n", PFID(fid), name);

	out_tx_index_delete(info->mti_env, obj, name, &info->mti_handle,
			    info->mti_u.update.mti_update_reply,
			    info->mti_u.update.mti_update_reply_index);
	return rc;
}

static int out_tx_object_destroy_exec(const struct lu_env *env,
				      struct thandle *th,
				      struct tx_arg *arg)
{
	struct dt_object *dt_obj;
	int rc;
 
	LASSERT(arg->object);
	dt_obj = out_get_dt_obj(arg->object);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));
 
	CDEBUG(D_OTHER, "destroy object "DFID"\n",
	       PFID(&arg->object->lo_header->loh_fid));
 
	rc = dt_destroy(env, dt_obj, th);
 
	update_insert_reply(arg->reply, NULL, 0, arg->index, rc);
	return 0;
}

static int out_tx_object_destroy_undo(const struct lu_env *env,
				      struct thandle *th,
				      struct tx_arg *arg)
{
	return 0;
}

static void __out_tx_object_destroy(const struct lu_env *env, struct lu_object *obj,
				    struct ost_body *body,
				    struct thandle_exec_args *th,
				    struct update_reply *reply,
				    int index, char *file, int line)
{
	struct tx_arg *arg;
	struct dt_object *dt_obj;
	int    rc;

	dt_obj = out_get_dt_obj(obj);
	LASSERT(dt_obj != NULL && !IS_ERR(dt_obj));
	LASSERT(dt_write_locked(env, dt_obj));

	if (th->ta_err)
		return;

	LASSERT(th->ta_handle != NULL);

	rc = dt_declare_destroy(env, dt_obj, th->ta_handle);
	th->ta_err = rc;

	if (likely(rc == 0)) {
		arg = tx_add_exec(th, out_tx_object_destroy_exec,
				  out_tx_object_destroy_undo, file,
				  line);
		LASSERT(arg);
		arg->object = obj;
		arg->reply = reply;
		arg->index = index;
		arg->u.destroy.body = body;
	}

}

/**
 * object destroy
 **/
static int out_destroy(struct mdt_thread_info *info)
{
	struct update     *update = info->mti_u.update.mti_update;
        struct lu_fid     *fid;
        struct ost_body   *body;
        struct lu_object  *obj;
        int                rc = 0;
        int                size;

        body = (struct ost_body *)update_param_buf(update, 0, &size);
        LASSERT(body != NULL && size == sizeof(*body));

        fid = &update->u_fid;
        fid_le_to_cpu(fid, fid);
        LASSERT(fid_is_sane(fid));

        rc = out_find_lock_object(info, fid, &obj, UPDATE_WRITE_LOCK);
        if (rc)
                return rc;

        LASSERT(obj != NULL);

        if (info->mti_handle.ta_handle == NULL) {
                out_tx_start(info->mti_env, info->mti_mdt, &info->mti_handle,
                             0);
                if (IS_ERR(info->mti_handle.ta_handle))
                        return PTR_ERR(info->mti_handle.ta_handle);
        }

        CDEBUG(D_INFO, "object destroy:"DFID"\n", PFID(fid));

        out_tx_object_destroy(info->mti_env, obj, body, &info->mti_handle,
                            info->mti_u.update.mti_update_reply,
                            info->mti_u.update.mti_update_reply_index);

        return rc;
}

#define DEF_OUT_HNDL(opc, name, fail_id, flags, fn)     \
[opc - OBJ_CREATE] = {				  \
	.mh_name    = name,			     \
	.mh_fail_id = fail_id,			  \
	.mh_opc     = opc,			      \
	.mh_flags   = flags,			    \
	.mh_act     = fn,			       \
	.mh_fmt     = NULL			      \
}

#define out_handler mdt_handler
static struct out_handler out_update_ops[] = {
	DEF_OUT_HNDL(OBJ_CREATE, "obj_create", 0, MUTABOR | HABEO_REFERO,
							    out_create),
	DEF_OUT_HNDL(OBJ_DESTROY, "obj_destroy", 0, MUTABOR | HABEO_REFERO,
							    out_destroy),
	DEF_OUT_HNDL(OBJ_REF_ADD, "obj_ref_add", 0, MUTABOR | HABEO_REFERO,
							    out_ref_add),
	DEF_OUT_HNDL(OBJ_REF_DEL, "obj_ref_del", 0, MUTABOR | HABEO_REFERO,
							    out_ref_del),
	DEF_OUT_HNDL(OBJ_ATTR_SET, "obj_attr_set", 0,  MUTABOR | HABEO_REFERO,
							    out_attr_set),
	DEF_OUT_HNDL(OBJ_ATTR_GET, "obj_attr_get", 0,  HABEO_REFERO,
							    out_attr_get),
	DEF_OUT_HNDL(OBJ_XATTR_SET, "obj_xattr_set", 0, MUTABOR | HABEO_REFERO,
							   out_xattr_set),
	DEF_OUT_HNDL(OBJ_XATTR_GET, "obj_xattr_get", 0, HABEO_REFERO,
							   out_xattr_get),
	DEF_OUT_HNDL(OBJ_INDEX_LOOKUP, "obj_index_lookup", 0,
				   HABEO_REFERO, out_index_lookup),
	DEF_OUT_HNDL(OBJ_INDEX_INSERT, "obj_index_insert", 0,
				   MUTABOR | HABEO_REFERO, out_index_insert),
	DEF_OUT_HNDL(OBJ_INDEX_DELETE, "obj_index_delete", 0,
				   MUTABOR | HABEO_REFERO, out_index_delete),
};

#define out_opc_slice mdt_opc_slice
static struct out_opc_slice out_handlers[] = {
	{
		.mos_opc_start = OBJ_CREATE,
		.mos_opc_end   = OBJ_MAX,
		.mos_hs	= out_update_ops
	},
};

static void out_init_update_object(struct mdt_thread_info *info)
{
	int i;

	for(i = 0; i < UPDATE_OBJ_COUNT; i++) {
		info->mti_u.update.mti_update_obj[i].ul_obj = NULL;
		info->mti_u.update.mti_update_obj[i].ul_mode = 0;
	}
}

/**
 * Object updates between Targets
 */
int out_handle(struct mdt_thread_info *info)
{
	struct req_capsule        *pill = info->mti_pill;
	struct update_buf    	  *ubuf;
	struct update 		  *update;
	struct thandle_exec_args  *th = &info->mti_handle;
	char                      *buf;
	int                        bufsize;
	int                        count;
	unsigned                   off;
	int                        i;
	int                        rc = 0;
	ENTRY;

	req_capsule_set(pill, &RQF_MDS_OBJ_UPDATE);
	bufsize = req_capsule_get_size(pill, &RMF_EADATA, RCL_CLIENT);
	if (bufsize != UPDATE_BUFFER_SIZE) {
		CERROR("%s: invalid bufsize %d\n", mdt_obd_name(info->mti_mdt),
		       bufsize);
		RETURN(-EPROTO);
	}

	buf = req_capsule_client_get(pill, &RMF_EADATA);
	if (buf == NULL) {
		CERROR("%s: No buf!\n", mdt_obd_name(info->mti_mdt));
		RETURN(-EPROTO);
	}

	ubuf = (struct update_buf *)buf;
	if (le32_to_cpu(ubuf->ub_magic) != UPDATE_BUFFER_MAGIC) {
		CERROR("%s: invalid magic %x\n", mdt_obd_name(info->mti_mdt),
			le32_to_cpu(ubuf->ub_magic));
		RETURN(-EPROTO);
	}

	count = le32_to_cpu(ubuf->ub_count);
	if (count <= 0) {
		CERROR("%s: No update! \n", mdt_obd_name(info->mti_mdt));
		RETURN(-EPROTO);
	}

	req_capsule_set_size(pill, &RMF_EADATA, RCL_SERVER, UPDATE_BUFFER_SIZE);
	rc = req_capsule_server_pack(pill);
	if (rc != 0) {
		CERROR("Can't pack response, rc %d\n", rc);
		RETURN(rc);
	}
	info->mti_u.update.mti_update_reply =
	       (struct update_reply *)req_capsule_server_get(pill, &RMF_EADATA);

	update_init_reply_buf(info->mti_u.update.mti_update_reply, count);

	off = cfs_size_round(offsetof(struct update_buf, ub_bufs[0]));
	out_init_update_object(info);
	info->mti_u.update.mti_update_resend = 0;
	for (i = 0; i < count; i++) {
		struct out_handler *h;

		update = (struct update *)((char *)ubuf + off);
		info->mti_u.update.mti_update = update;
		info->mti_u.update.mti_update_reply_index = i;

		h = mdt_handler_find(update->u_type, out_handlers);
		if (likely(h != NULL)) {
			rc = h->mh_act(info);
		} else {
			CERROR("%s: The unsupported opc: 0x%x\n",
			       mdt_obd_name(info->mti_mdt), update->u_type);
			GOTO(out, rc = -ENOTSUPP);
		}
		if (rc < 0)
			GOTO(out, rc);
		off += cfs_size_round(update_size(update));
	}

	if (th->ta_handle != NULL)
		rc = out_tx_end(info, th);
out:
	out_unlock_objects(info);
        info->mti_fail_id = OBD_FAIL_MDS_OBJ_UPDATE_NET;
	RETURN(rc);
}

