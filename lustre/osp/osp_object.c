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
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osp/osp_object.c
 *
 * Lustre OST Proxy Device
 *
 * Author: Alex Zhuravlev <alexey.zhuravlev@intel.com>
 * Author: Mikhail Pershin <mike.tappro@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "osp_internal.h"

static void osp_object_assign_fid(const struct lu_env *env,
				 struct osp_device *d, struct osp_object *o)
{
	struct osp_thread_info *osi = osp_env_info(env);

	LASSERT(fid_is_zero(lu_object_fid(&o->opo_obj.do_lu)));
	LASSERT(o->opo_reserved);
	o->opo_reserved = 0;

	osp_precreate_get_fid(env, d, &osi->osi_fid);

	lu_object_assign_fid(env, &o->opo_obj.do_lu, &osi->osi_fid);
}

static void osp_attr_from_obdo(struct osp_object *o, struct obdo *obdo)
{
	struct lu_attr *attr = &o->opo_attr;

	attr->la_valid = 0;
	if (obdo->o_valid & OBD_MD_FLATIME) {
		attr->la_atime = obdo->o_atime;
		attr->la_valid |= LA_ATIME;
	}
	if (obdo->o_valid & OBD_MD_FLMTIME) {
		attr->la_mtime = obdo->o_mtime;
		attr->la_valid |= LA_MTIME;
	}
	if (obdo->o_valid & OBD_MD_FLCTIME) {
		attr->la_ctime = obdo->o_ctime;
		attr->la_valid |= LA_CTIME;
	}
	if (obdo->o_valid & OBD_MD_FLSIZE) {
		attr->la_size = obdo->o_size;
		attr->la_valid |= LA_SIZE;
	}
	if (obdo->o_valid & OBD_MD_FLMODE) {
		attr->la_mode = (obdo->o_mode & ~S_IFMT);
		attr->la_valid |= LA_MODE;
	}
	if (obdo->o_valid & OBD_MD_FLUID) {
		attr->la_uid = obdo->o_uid;
		attr->la_valid |= LA_UID;
	}
	if (obdo->o_valid & OBD_MD_FLGID) {
		attr->la_gid = obdo->o_gid;
		attr->la_valid |= LA_GID;
	}
	if (obdo->o_valid & OBD_MD_FLBLOCKS) {
		attr->la_blocks = obdo->o_blocks;
		attr->la_valid |= LA_BLOCKS;
	}
	if (obdo->o_valid & OBD_MD_FLTYPE) {
		attr->la_mode |= (obdo->o_mode & S_IFMT);
		attr->la_valid |= LA_MODE;
	}
	if (obdo->o_valid & OBD_MD_FLFLAGS) {
		attr->la_flags = obdo->o_flags;
		attr->la_valid |= LA_FLAGS;
	}
	if (obdo->o_valid & OBD_MD_FLNLINK) {
		attr->la_nlink = obdo->o_nlink;
		attr->la_valid |= LA_NLINK;
	}
	if (obdo->o_valid & OBD_MD_FLBLKSZ) {
		attr->la_blkbits = obdo->o_blksize;
		attr->la_valid |= LA_BLKSIZE;
	}
	if (obdo->o_valid & OBD_MD_FLFID) {
		o->opo_pfid.f_seq = obdo->o_parent_seq;
		o->opo_pfid.f_oid = obdo->o_parent_oid;
		o->opo_pfid.f_ver = obdo->o_stripe_idx;
		o->opo_pfid_ready = 1;
	}
}

static int osp_async_getattr_interpret(const struct lu_env *env,
				       struct ptlrpc_request *req,
				       void *arg, int rc)
{
	struct osp_object *o	=
			((union ptlrpc_async_args *)arg)->pointer_arg[0];
	struct ost_body   *body = NULL;

	if (rc == 0) {
		body = req_capsule_server_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			rc = -EPROTO;
	}
	spin_lock(&o->opo_async_getattr_lock);
	if (rc == 0) {
		osp_attr_from_obdo(o, &body->oa);
	} else {
		o->opo_attr.la_valid = 0;
		o->opo_pfid_ready = 0;
	}
	o->opo_async_getattr_wait = 0;
	spin_unlock(&o->opo_async_getattr_lock);
	wake_up_all(&o->opo_async_getattr_waitq);
	lu_object_put(env, osp2lu_obj(o));
	return 0;
}

static void osp_getattr_fill_oa(struct obdo *oa, struct osp_object *o)
{
	fid_to_ostid(lu_object_fid(&o->opo_obj.do_lu), &oa->o_oi);
	oa->o_valid = OBD_MD_FLID | OBD_MD_FLGROUP;
}

static int osp_declare_attr_get(const struct lu_env *env, struct dt_object *dt)
{
	struct osp_object	*o	  = dt2osp_obj(dt);
	struct osp_device	*d	  = lu2osp_dev(dt->do_lu.lo_dev);
	struct lu_attr		*la	  = &o->opo_attr;
	struct ptlrpc_request	*req;
	struct ost_body 	*body;
	int			 rc	  = 0;
	ENTRY;

	spin_lock(&o->opo_async_getattr_lock);
	if (o->opo_async_getattr_wait) {
		spin_unlock(&o->opo_async_getattr_lock);
		RETURN(0);
	}

	/* Re-fetch the attribute because OSP does not hold related
	 * ldlm lock to protect OST-object attributes. */
	la->la_valid = 0;
	o->opo_pfid_ready = 0;
	o->opo_async_getattr_wait = 1;
	spin_unlock(&o->opo_async_getattr_lock);

	req = ptlrpc_request_alloc(d->opd_obd->u.cli.cl_import,
				   &RQF_OST_GETATTR);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_CAPA1, RCL_CLIENT, 0);
	rc = ptlrpc_request_pack(req, LUSTRE_OST_VERSION, OST_GETATTR);
	if (rc != 0) {
		ptlrpc_request_free(req);
		GOTO(out, rc);
	}

	body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
	osp_getattr_fill_oa(&body->oa, o);
	ptlrpc_request_set_replen(req);
	req->rq_interpret_reply = osp_async_getattr_interpret;
	lu_object_get(osp2lu_obj(o));
	req->rq_async_args.pointer_arg[0] = o;
	ptlrpcd_add_req(req, PDL_POLICY_ROUND, -1);

	GOTO(out, rc = 0);

out:
	if (rc != 0) {
		spin_lock(&o->opo_async_getattr_lock);
		o->opo_async_getattr_wait = 0;
		spin_unlock(&o->opo_async_getattr_lock);
		wake_up_all(&o->opo_async_getattr_waitq);
	}
	return rc;
}

static inline int osp_async_attr_wakeup(struct osp_object *o)
{
	int rc = 0;
	spin_lock(&o->opo_async_getattr_lock);
	if (!o->opo_async_getattr_wait)
		rc = 1;
	spin_unlock(&o->opo_async_getattr_lock);
	return rc;
}

static int osp_attr_get(const struct lu_env *env, struct dt_object *dt,
			struct lu_attr *attr, struct lustre_capa *capa)
{
	struct osp_object	*o	= dt2osp_obj(dt);
	struct osp_device	*d	= lu2osp_dev(dt->do_lu.lo_dev);
	struct lu_attr		*la	= &o->opo_attr;
	struct ptlrpc_request	*req;
	struct ost_body 	*body;
	int			 rc	= 0;
	ENTRY;

	spin_lock(&o->opo_async_getattr_lock);
	if (la->la_valid) {
		memcpy(attr, la, sizeof(*attr));
		spin_unlock(&o->opo_async_getattr_lock);
		RETURN(0);
	}

	if (o->opo_async_getattr_wait) {
		struct l_wait_info lwi = { 0 };

		spin_unlock(&o->opo_async_getattr_lock);
		l_wait_event(o->opo_async_getattr_waitq,
			     osp_async_attr_wakeup(o),
			     &lwi);
		spin_lock(&o->opo_async_getattr_lock);
		if (la->la_valid) {
			memcpy(attr, la, sizeof(*attr));
			spin_unlock(&o->opo_async_getattr_lock);
			RETURN(0);
		}
	}
	spin_unlock(&o->opo_async_getattr_lock);

	req = ptlrpc_request_alloc(d->opd_obd->u.cli.cl_import,
				   &RQF_OST_GETATTR);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_CAPA1, RCL_CLIENT, 0);
	rc = ptlrpc_request_pack(req, LUSTRE_OST_VERSION, OST_GETATTR);
	if (rc != 0)
		GOTO(out, rc);

	body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
	osp_getattr_fill_oa(&body->oa, o);
	ptlrpc_request_set_replen(req);
	rc = ptlrpc_queue_wait(req);
	if (rc != 0)
		GOTO(out, rc);

	body = req_capsule_server_get(&req->rq_pill, &RMF_OST_BODY);
	if (body == NULL)
		GOTO(out, rc = -EPROTO);

	osp_attr_from_obdo(o, &body->oa);
	memcpy(attr, la, sizeof(*attr));

	GOTO(out, rc = 0);

out:
	ptlrpc_req_finished(req);
	return rc;
}

static int osp_declare_attr_set(const struct lu_env *env, struct dt_object *dt,
				const struct lu_attr *attr, struct thandle *th)
{
	struct osp_device	*d = lu2osp_dev(dt->do_lu.lo_dev);
	struct osp_object	*o = dt2osp_obj(dt);
	int			 rc = 0;

	ENTRY;

	/*
	 * Usually we don't allow server stack to manipulate size
	 * but there is a special case when striping is created
	 * late, after stripless file got truncated to non-zero.
	 *
	 * In this case we do the following:
	 *
	 * 1) grab id in declare - this can lead to leaked OST objects
	 *    but we don't currently have proper mechanism and the only
	 *    options we have are to do truncate RPC holding transaction
	 *    open (very bad) or to grab id in declare at cost of leaked
	 *    OST object in same very rare unfortunate case (just bad)
	 *    notice 1.6-2.0 do assignment outside of running transaction
	 *    all the time, meaning many more chances for leaked objects.
	 *
	 * 2) send synchronous truncate RPC with just assigned id
	 */

	/* there are few places in MDD code still passing NULL
	 * XXX: to be fixed soon */
	if (attr == NULL)
		RETURN(0);

	if (attr->la_valid & LA_SIZE && attr->la_size > 0 &&
	    fid_is_zero(lu_object_fid(&o->opo_obj.do_lu))) {
		LASSERT(!dt_object_exists(dt));
		osp_object_assign_fid(env, d, o);
		rc = osp_object_truncate(env, dt, attr->la_size);
		if (rc)
			RETURN(rc);
	}

	if (o->opo_new) {
		/* no need in logging for new objects being created */
		RETURN(0);
	}

	if (!(attr->la_valid & (LA_UID | LA_GID)))
		RETURN(0);

	/*
	 * track all UID/GID changes via llog
	 */
	rc = osp_sync_declare_add(env, o, MDS_SETATTR64_REC, th);

	RETURN(rc);
}

static int osp_attr_set(const struct lu_env *env, struct dt_object *dt,
			const struct lu_attr *attr, struct thandle *th,
			struct lustre_capa *capa)
{
	struct osp_object	*o = dt2osp_obj(dt);
	int			 rc = 0;

	ENTRY;

	/* we're interested in uid/gid changes only */
	if (!(attr->la_valid & (LA_UID | LA_GID)))
		RETURN(0);

	/* new object, the very first ->attr_set()
	 * initializing attributes needs no logging
	 * all subsequent one are subject to the
	 * logging and synchronization with OST */
	if (o->opo_new) {
		o->opo_new = 0;
		RETURN(0);
	}

	/*
	 * once transaction is committed put proper command on
	 * the queue going to our OST
	 */
	rc = osp_sync_add(env, o, MDS_SETATTR64_REC, th, attr);

	/* XXX: send new uid/gid to OST ASAP? */

	RETURN(rc);
}

static int osp_xattr_get(const struct lu_env *env, struct dt_object *dt,
			 struct lu_buf *buf, const char *name,
			 struct lustre_capa *capa)
{
	struct osp_object	*o	= dt2osp_obj(dt);
	struct osp_device	*d	= lu2osp_dev(dt->do_lu.lo_dev);
	struct ptlrpc_request	*req;
	struct ost_body 	*body;
	int			 rc	= 0;

	if (strcmp(name, XATTR_NAME_FID) != 0)
		return -EOPNOTSUPP;

	if (buf == NULL || buf->lb_len == 0)
		return sizeof(struct lu_fid);

	if (buf->lb_len < sizeof(struct lu_fid))
		return -ERANGE;

	spin_lock(&o->opo_async_getattr_lock);
	if (o->opo_pfid_ready) {
		memcpy(buf->lb_buf, &o->opo_pfid, sizeof(struct lu_fid));
		spin_unlock(&o->opo_async_getattr_lock);
		return sizeof(struct lu_fid);
	}

	if (o->opo_async_getattr_wait) {
		struct l_wait_info lwi = { 0 };

		spin_unlock(&o->opo_async_getattr_lock);
		l_wait_event(o->opo_async_getattr_waitq,
			     osp_async_attr_wakeup(o),
			     &lwi);
		spin_lock(&o->opo_async_getattr_lock);
		if (o->opo_pfid_ready) {
			memcpy(buf->lb_buf, &o->opo_pfid,
			       sizeof(struct lu_fid));
			spin_unlock(&o->opo_async_getattr_lock);
			return sizeof(struct lu_fid);
		}
	}
	spin_unlock(&o->opo_async_getattr_lock);

	req = ptlrpc_request_alloc(d->opd_obd->u.cli.cl_import,
				   &RQF_OST_GETATTR);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_CAPA1, RCL_CLIENT, 0);
	rc = ptlrpc_request_pack(req, LUSTRE_OST_VERSION, OST_GETATTR);
	if (rc != 0)
		GOTO(out, rc);

	body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
	osp_getattr_fill_oa(&body->oa, o);
	ptlrpc_request_set_replen(req);
	rc = ptlrpc_queue_wait(req);
	if (rc != 0)
		GOTO(out, rc);

	body = req_capsule_server_get(&req->rq_pill, &RMF_OST_BODY);
	if (body == NULL)
		GOTO(out, rc = -EPROTO);

	osp_attr_from_obdo(o, &body->oa);
	memcpy(buf->lb_buf, &o->opo_pfid, sizeof(struct lu_fid));

	GOTO(out, rc = sizeof(struct lu_fid));

out:
	ptlrpc_req_finished(req);
	return rc;
}

static int osp_declare_object_create(const struct lu_env *env,
				     struct dt_object *dt,
				     struct lu_attr *attr,
				     struct dt_allocation_hint *hint,
				     struct dt_object_format *dof,
				     struct thandle *th)
{
	struct osp_thread_info	*osi = osp_env_info(env);
	struct osp_device	*d = lu2osp_dev(dt->do_lu.lo_dev);
	struct osp_object	*o = dt2osp_obj(dt);
	const struct lu_fid	*fid;
	int			 rc = 0;

	ENTRY;

	/* should happen to non-0 OSP only so that at least one object
	 * has been already declared in the scenario and LOD should
	 * cleanup that */
	if (OBD_FAIL_CHECK(OBD_FAIL_MDS_OSC_CREATE_FAIL) && d->opd_index == 1)
		RETURN(-ENOSPC);

	LASSERT(d->opd_last_used_oid_file);
	fid = lu_object_fid(&dt->do_lu);

	/*
	 * There can be gaps in precreated ids and record to unlink llog
	 * XXX: we do not handle gaps yet, implemented before solution
	 *	was found to be racy, so we disabled that. there is no
	 *	point in making useless but expensive llog declaration.
	 */
	/* rc = osp_sync_declare_add(env, o, MDS_UNLINK64_REC, th); */

	if (unlikely(!fid_is_zero(fid))) {
		/* replay case: caller knows fid */
		osi->osi_off = sizeof(osi->osi_id) * d->opd_index;
		rc = dt_declare_record_write(env, d->opd_last_used_oid_file,
					     sizeof(osi->osi_id), osi->osi_off,
					     th);
		RETURN(rc);
	}

	/*
	 * in declaration we need to reserve object so that we don't block
	 * awaiting precreation RPC to complete
	 */
	rc = osp_precreate_reserve(env, d);
	/*
	 * we also need to declare update to local "last used id" file for
	 * recovery if object isn't used for a reason, we need to release
	 * reservation, this can be made in osd_object_release()
	 */
	if (rc == 0) {
		/* mark id is reserved: in create we don't want to talk
		 * to OST */
		LASSERT(o->opo_reserved == 0);
		o->opo_reserved = 1;

		/* common for all OSPs file hystorically */
		osi->osi_off = sizeof(osi->osi_id) * d->opd_index;
		rc = dt_declare_record_write(env, d->opd_last_used_oid_file,
					     sizeof(osi->osi_id), osi->osi_off,
					     th);
	} else {
		/* not needed in the cache anymore */
		set_bit(LU_OBJECT_HEARD_BANSHEE,
			    &dt->do_lu.lo_header->loh_flags);
	}
	RETURN(rc);
}

static int osp_object_create(const struct lu_env *env, struct dt_object *dt,
			     struct lu_attr *attr,
			     struct dt_allocation_hint *hint,
			     struct dt_object_format *dof, struct thandle *th)
{
	struct osp_thread_info	*osi = osp_env_info(env);
	struct osp_device	*d = lu2osp_dev(dt->do_lu.lo_dev);
	struct osp_object	*o = dt2osp_obj(dt);
	int			rc = 0;
	struct lu_fid		*fid = &osi->osi_fid;
	ENTRY;

	if (o->opo_reserved) {
		/* regular case, fid is assigned holding trunsaction open */
		 osp_object_assign_fid(env, d, o);
	}

	memcpy(fid, lu_object_fid(&dt->do_lu), sizeof(*fid));

	LASSERTF(fid_is_sane(fid), "fid for osp_object %p is insane"DFID"!\n",
		 o, PFID(fid));

	if (!o->opo_reserved) {
		/* special case, id was assigned outside of transaction
		 * see comments in osp_declare_attr_set */
		spin_lock(&d->opd_pre_lock);
		osp_update_last_fid(d, fid);
		spin_unlock(&d->opd_pre_lock);
	}

	CDEBUG(D_INODE, "fid for osp_object %p is "DFID"\n", o, PFID(fid));

	/* If the precreate ends, it means it will be ready to rollover to
	 * the new sequence soon, all the creation should be synchronized,
	 * otherwise during replay, the replay fid will be inconsistent with
	 * last_used/create fid */
	if (osp_precreate_end_seq(env, d) && osp_is_fid_client(d))
		th->th_sync = 1;

	/*
	 * it's OK if the import is inactive by this moment - id was created
	 * by OST earlier, we just need to maintain it consistently on the disk
	 * once import is reconnected, OSP will claim this and other objects
	 * used and OST either keep them, if they exist or recreate
	 */

	/* we might have lost precreated objects */
	if (unlikely(d->opd_gap_count) > 0) {
		spin_lock(&d->opd_pre_lock);
		if (d->opd_gap_count > 0) {
			int count = d->opd_gap_count;

			ostid_set_id(&osi->osi_oi,
				     fid_oid(&d->opd_gap_start_fid));
			d->opd_gap_count = 0;
			spin_unlock(&d->opd_pre_lock);

			CDEBUG(D_HA, "Writting gap "DFID"+%d in llog\n",
			       PFID(&d->opd_gap_start_fid), count);
			/* real gap handling is disabled intil ORI-692 will be
			 * fixed, now we only report gaps */
		} else {
			spin_unlock(&d->opd_pre_lock);
		}
	}

	/* new object, the very first ->attr_set()
	 * initializing attributes needs no logging */
	o->opo_new = 1;

	/* Only need update last_used oid file, seq file will only be update
	 * during seq rollover */
	osp_objid_buf_prep(&osi->osi_lb, &osi->osi_off,
			   &d->opd_last_used_fid.f_oid, d->opd_index);

	rc = dt_record_write(env, d->opd_last_used_oid_file, &osi->osi_lb,
			     &osi->osi_off, th);

	CDEBUG(D_HA, "%s: Wrote last used FID: "DFID", index %d: %d\n",
	       d->opd_obd->obd_name, PFID(fid), d->opd_index, rc);

	RETURN(rc);
}

static int osp_declare_object_destroy(const struct lu_env *env,
				      struct dt_object *dt, struct thandle *th)
{
	struct osp_object	*o = dt2osp_obj(dt);
	int			 rc = 0;

	ENTRY;

	/*
	 * track objects to be destroyed via llog
	 */
	rc = osp_sync_declare_add(env, o, MDS_UNLINK64_REC, th);

	RETURN(rc);
}

static int osp_object_destroy(const struct lu_env *env, struct dt_object *dt,
			      struct thandle *th)
{
	struct osp_object	*o = dt2osp_obj(dt);
	int			 rc = 0;

	ENTRY;

	/*
	 * once transaction is committed put proper command on
	 * the queue going to our OST
	 */
	rc = osp_sync_add(env, o, MDS_UNLINK64_REC, th, NULL);

	/* not needed in cache any more */
	set_bit(LU_OBJECT_HEARD_BANSHEE, &dt->do_lu.lo_header->loh_flags);

	RETURN(rc);
}

struct dt_object_operations osp_obj_ops = {
	.do_declare_attr_get	= osp_declare_attr_get,
	.do_attr_get		= osp_attr_get,
	.do_declare_attr_set	= osp_declare_attr_set,
	.do_attr_set		= osp_attr_set,
	.do_xattr_get		= osp_xattr_get,
	.do_declare_create	= osp_declare_object_create,
	.do_create		= osp_object_create,
	.do_declare_destroy	= osp_declare_object_destroy,
	.do_destroy		= osp_object_destroy,
};

static int is_ost_obj(struct lu_object *lo)
{
	struct osp_device  *osp  = lu2osp_dev(lo->lo_dev);

	return !osp->opd_connect_mdt;
}

static int osp_object_init(const struct lu_env *env, struct lu_object *o,
			   const struct lu_object_conf *conf)
{
	struct osp_object	*po = lu2osp_obj(o);
	int			rc = 0;
	ENTRY;

	init_rwsem(&po->opo_sem);
	spin_lock_init(&po->opo_async_getattr_lock);
	init_waitqueue_head(&po->opo_async_getattr_waitq);

	if (is_ost_obj(o)) {
		po->opo_obj.do_ops = &osp_obj_ops;
	} else {
		struct lu_attr		*la = &osp_env_info(env)->osi_attr;

		po->opo_obj.do_ops = &osp_md_obj_ops;
		o->lo_header->loh_attr |= LOHA_REMOTE;
		rc = po->opo_obj.do_ops->do_attr_get(env, lu2dt_obj(o),
						     la, NULL);
		if (rc == 0)
			o->lo_header->loh_attr |=
				LOHA_EXISTS | (la->la_mode & S_IFMT);
		if (rc == -ENOENT)
			rc = 0;
	}
	RETURN(rc);
}

static void osp_object_free(const struct lu_env *env, struct lu_object *o)
{
	struct osp_object	*obj = lu2osp_obj(o);
	struct lu_object_header	*h = o->lo_header;

	dt_object_fini(&obj->opo_obj);
	lu_object_header_fini(h);
	OBD_SLAB_FREE_PTR(obj, osp_object_kmem);
}

static void osp_object_release(const struct lu_env *env, struct lu_object *o)
{
	struct osp_object	*po = lu2osp_obj(o);
	struct osp_device	*d  = lu2osp_dev(o->lo_dev);

	ENTRY;

	/*
	 * release reservation if object was declared but not created
	 * this may require lu_object_put() in LOD
	 */
	if (unlikely(po->opo_reserved)) {
		LASSERT(d->opd_pre_reserved > 0);
		spin_lock(&d->opd_pre_lock);
		d->opd_pre_reserved--;
		spin_unlock(&d->opd_pre_lock);

		/* not needed in cache any more */
		set_bit(LU_OBJECT_HEARD_BANSHEE, &o->lo_header->loh_flags);
	}
	EXIT;
}

static int osp_object_print(const struct lu_env *env, void *cookie,
			    lu_printer_t p, const struct lu_object *l)
{
	const struct osp_object *o = lu2osp_obj((struct lu_object *)l);

	return (*p)(env, cookie, LUSTRE_OSP_NAME"-object@%p", o);
}

static int osp_object_invariant(const struct lu_object *o)
{
	LBUG();
}

struct lu_object_operations osp_lu_obj_ops = {
	.loo_object_init	= osp_object_init,
	.loo_object_free	= osp_object_free,
	.loo_object_release	= osp_object_release,
	.loo_object_print	= osp_object_print,
	.loo_object_invariant	= osp_object_invariant
};
