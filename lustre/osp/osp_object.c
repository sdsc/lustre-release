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

static inline int is_ost_obj(struct lu_object *lo)
{
	struct osp_device  *osp  = lu2osp_dev(lo->lo_dev);

	return !osp->opd_connect_mdt;
}

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

static int osp_get_attr_from_req(const struct lu_env *env,
				 struct update_reply *reply,
				 struct osp_object *obj, int index)
{
	struct obdo *lobdo = &osp_env_info(env)->osi_obdo;
	struct obdo *wobdo;
	int	     size;

	size = update_get_reply_buf(reply, (void **)&wobdo, index);
	if (size != sizeof(struct obdo))
		return -EPROTO;

	obdo_le_to_cpu(wobdo, wobdo);
	lustre_get_wire_obdo(NULL, lobdo, wobdo);
	spin_lock(&obj->opo_lock);
	la_from_obdo(&obj->opo_attr, lobdo, lobdo->o_valid);
	spin_unlock(&obj->opo_lock);
	return 0;
}

static int osp_attr_get_interpterer(const struct lu_env *env,
				    struct update_reply *reply,
				    struct osp_object *obj,
				    int index, int rc)
{
	struct lu_attr *attr = &obj->opo_attr;

	if (rc == 0) {
		osp2lu_obj(obj)->lo_header->loh_attr |= LOHA_EXISTS;
		return osp_get_attr_from_req(env, reply, obj, index);
	} else if (rc == -ENOENT) {
		osp2lu_obj(obj)->lo_header->loh_attr &= ~LOHA_EXISTS;
		spin_lock(&obj->opo_lock);
		attr->la_valid = 0;
		spin_unlock(&obj->opo_lock);
	} else {
		spin_lock(&obj->opo_lock);
		attr->la_valid = 0;
		spin_unlock(&obj->opo_lock);
	}

	return 0;
}

static int osp_declare_attr_get(const struct lu_env *env, struct dt_object *dt,
				struct lustre_capa *capa)
{
	struct osp_object	*obj	= dt2osp_obj(dt);
	struct osp_device	*osp	= lu2osp_dev(dt->do_lu.lo_dev);
	struct update_request	*update;
	int			 rc	= 0;

	mutex_lock(&osp->opd_dummy_th_mutex);
	update = osp_find_or_create_dummy_update_req(osp);
	if (IS_ERR(update))
		rc = PTR_ERR(update);
	else
		rc = osp_insert_dummy_update(env, update, OBJ_ATTR_GET, obj, 0,
					     NULL, NULL, osp_attr_get_interpterer);
	mutex_unlock(&osp->opd_dummy_th_mutex);
	return rc;
}

int osp_attr_get(const struct lu_env *env, struct dt_object *dt,
		 struct lu_attr *attr, struct lustre_capa *capa)
{
	struct osp_device	*osp	= lu2osp_dev(dt->do_lu.lo_dev);
	struct osp_object	*obj	= dt2osp_obj(dt);
	struct dt_device	*dev	= &osp->opd_dt_dev;
	struct update_request	*update;
	struct update_reply	*reply;
	struct ptlrpc_request	*req	= NULL;
	int			 rc	= 0;
	ENTRY;

	if (is_ost_obj(&dt->do_lu)) {
		/* XXX: For LFSCK purpose. The OST-object's attribute is
		 *	pre-fetched without ldlm lock protection. So some
		 *	attribute, such as size/blocks/mtime/ctime may be
		 *	not valid. But it is NOT important for the LFSCK. */
		spin_lock(&obj->opo_lock);
		if (obj->opo_attr.la_valid != 0) {
			memcpy(attr, &obj->opo_attr, sizeof(*attr));
			spin_unlock(&obj->opo_lock);
			RETURN(0);
		}
		spin_unlock(&obj->opo_lock);
	}

	update = out_create_update_req(dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = out_insert_update(env, update, OBJ_ATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       0, NULL, NULL);
	if (rc != 0) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = out_remote_sync(env, osp->opd_obd->u.cli.cl_import, update, &req);
	if (rc != 0) {
		if (rc == -ENOENT)
			osp2lu_obj(obj)->lo_header->loh_attr &= ~LOHA_EXISTS;
		else
			CERROR("%s:osp_attr_get update error: rc = %d\n",
			       dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	reply = req_capsule_server_sized_get(&req->rq_pill, &RMF_UPDATE_REPLY,
					     UPDATE_BUFFER_SIZE);
	if (reply == NULL || reply->ur_version != UPDATE_REPLY_V1)
		GOTO(out, rc = -EPROTO);

	osp2lu_obj(obj)->lo_header->loh_attr |= LOHA_EXISTS;
	rc = osp_get_attr_from_req(env, reply, obj, 0);
	if (rc == 0) {
		osp2lu_obj(obj)->lo_header->loh_attr |= LOHA_EXISTS;
		if (is_ost_obj(&dt->do_lu)) {
			spin_lock(&obj->opo_lock);
			memcpy(attr, &obj->opo_attr, sizeof(*attr));
			spin_unlock(&obj->opo_lock);
		} else {
			if (attr->la_flags == 1)
				obj->opo_empty = 0;
			else
				obj->opo_empty = 1;
		}
	}

	GOTO(out, rc);

out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	out_destroy_update_req(update);
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

	spin_lock(&o->opo_lock);
	/* NOT sure whether updating attr on the OST will fail or not.
	 * Force to re-fetch attr from the OST next time. */
	o->opo_attr.la_valid = 0;
	spin_unlock(&o->opo_lock);

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

/* Only support xattr_get callback for XATTR_FID_NAME. */
static int osp_xattr_get_interpterer(const struct lu_env *env,
				     struct update_reply *reply,
				     struct osp_object *obj,
				     int index, int rc)
{
	if (rc == 0) {
		void *ea_buf = NULL;
		int   len;

		len = update_get_reply_buf(reply, &ea_buf, index);
		if (len < 0)
			return len;

		LASSERT(ea_buf != NULL);

		if (len < sizeof(obj->opo_pfid))
			return -EINVAL;

		spin_lock(&obj->opo_lock);
		memcpy(&obj->opo_pfid, ea_buf, sizeof(obj->opo_pfid));
		obj->opo_pfid_ready = 1;
		spin_unlock(&obj->opo_lock);
	} else if (rc == -ENOENT || rc == -ENODATA) {
		spin_lock(&obj->opo_lock);
		memset(&obj->opo_pfid, 0, sizeof(obj->opo_pfid));
		obj->opo_pfid_ready = 1;
		spin_unlock(&obj->opo_lock);
	} else {
		spin_lock(&obj->opo_lock);
		obj->opo_pfid_ready = 0;
		spin_unlock(&obj->opo_lock);
	}

	return 0;
}

/* Only support xattr_get callback for XATTR_FID_NAME. */
static int osp_declare_xattr_get(const struct lu_env *env, struct dt_object *dt,
				 struct lu_buf *buf, const char *name,
				 struct lustre_capa *capa)
{
	struct osp_object	*obj	= dt2osp_obj(dt);
	struct osp_device	*osp	= lu2osp_dev(dt->do_lu.lo_dev);
	struct update_request	*update;
	int			 namelen;
	int			 rc	= 0;

	LASSERT(buf != NULL);
	LASSERT(name != NULL);

	if (strcmp(name, XATTR_NAME_FID) != 0)
		return -EOPNOTSUPP;

	namelen = strlen(name);
	mutex_lock(&osp->opd_dummy_th_mutex);
	update = osp_find_or_create_dummy_update_req(osp);
	if (IS_ERR(update))
		rc = PTR_ERR(update);
	else
		rc = osp_insert_dummy_update(env, update, OBJ_XATTR_GET, obj,
					     1, &namelen, (char **)&name,
					     osp_xattr_get_interpterer);
	mutex_unlock(&osp->opd_dummy_th_mutex);
	return rc;
}

int osp_xattr_get(const struct lu_env *env, struct dt_object *dt,
		  struct lu_buf *buf, const char *name,
		  struct lustre_capa *capa)
{
	struct osp_device	*osp	= lu2osp_dev(dt->do_lu.lo_dev);
	struct osp_object	*obj	= dt2osp_obj(dt);
	struct dt_device	*dev	= &osp->opd_dt_dev;
	struct update_request	*update;
	struct ptlrpc_request	*req	= NULL;
	struct update_reply	*reply;
	void			*ea_buf = NULL;
	int			 namelen;
	int			 rc	= 0;
	ENTRY;

	LASSERT(buf != NULL);
	LASSERT(name != NULL);

	if (strcmp(name, XATTR_NAME_FID) == 0) {
		if (buf->lb_buf == NULL)
			RETURN(sizeof(obj->opo_pfid));

		if (buf->lb_len < sizeof(obj->opo_pfid))
			RETURN(-ERANGE);

		spin_lock(&obj->opo_lock);
		if (obj->opo_pfid_ready) {
			memcpy(buf->lb_buf, &obj->opo_pfid,
			       sizeof(obj->opo_pfid));
			spin_unlock(&obj->opo_lock);
			RETURN(sizeof(obj->opo_pfid));
		}
		spin_unlock(&obj->opo_lock);
	}

	update = out_create_update_req(dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	namelen = strlen(name);
	rc = out_insert_update(env, update, OBJ_XATTR_GET,
			       (struct lu_fid *)lu_object_fid(&dt->do_lu),
			       1, &namelen, (char **)&name);
	if (rc != 0) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = out_remote_sync(env, osp->opd_obd->u.cli.cl_import, update, &req);
	if (rc != 0) {
		if (strcmp(name, XATTR_NAME_FID) == 0) {
			if (rc == -ENOENT || rc == -ENODATA) {
				spin_lock(&obj->opo_lock);
				memset(&obj->opo_pfid, 0, sizeof(obj->opo_pfid));
				obj->opo_pfid_ready = 1;
				spin_unlock(&obj->opo_lock);
			}
		}

		GOTO(out, rc);
	}

	reply = req_capsule_server_sized_get(&req->rq_pill, &RMF_UPDATE_REPLY,
					    UPDATE_BUFFER_SIZE);
	if (reply->ur_version != UPDATE_REPLY_V1) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dev->dd_lu_dev.ld_obd->obd_name,
		       reply->ur_version, UPDATE_REPLY_V1, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	rc = update_get_reply_buf(reply, &ea_buf, 0);
	if (rc < 0)
		GOTO(out, rc);

	LASSERT(rc > 0 && rc < PAGE_CACHE_SIZE);
	LASSERT(ea_buf != NULL);

	if (strcmp(name, XATTR_NAME_FID) == 0) {
		if (rc < sizeof(obj->opo_pfid))
			GOTO(out, rc = -EINVAL);

		spin_lock(&obj->opo_lock);
		memcpy(&obj->opo_pfid, ea_buf, sizeof(obj->opo_pfid));
		obj->opo_pfid_ready = 1;
		spin_unlock(&obj->opo_lock);
	}

	if (buf->lb_buf != NULL)
		memcpy(buf->lb_buf, ea_buf, rc);

	GOTO(out, rc);

out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	out_destroy_update_req(update);
	return rc;
}

int osp_declare_xattr_set(const struct lu_env *env, struct dt_object *dt,
			  const struct lu_buf *buf, const char *name,
			  int flag, struct thandle *th)
{
	struct update_request	*update;
	struct lu_fid		*fid;
	int			sizes[3] = {strlen(name), buf->lb_len,
					    sizeof(int)};
	char			*bufs[3] = {(char *)name, (char *)buf->lb_buf };
	int			rc;

	LASSERT(buf->lb_len > 0 && buf->lb_buf != NULL);

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	flag = cpu_to_le32(flag);
	bufs[2] = (char *)&flag;

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	rc = out_insert_update(env, update, OBJ_XATTR_SET, fid,
			       ARRAY_SIZE(sizes), sizes, bufs);

	return rc;
}

int osp_xattr_set(const struct lu_env *env, struct dt_object *dt,
		  const struct lu_buf *buf, const char *name, int fl,
		  struct thandle *th, struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "xattr %s set object "DFID"\n", name,
	       PFID(&dt->do_lu.lo_header->loh_fid));

	if (strcmp(name, XATTR_NAME_FID) == 0) {
		struct osp_object *obj = dt2osp_obj(dt);

		/* NOT sure whether updating xattr on the OST will fail or not.
		 * Force to re-fetch the xattr from the OST next time. */
		spin_lock(&obj->opo_lock);
		obj->opo_pfid_ready = 0;
		spin_unlock(&obj->opo_lock);
	}

	return 0;
}

/* XXX: In the future, both OST-side create and MDT-side create should share
 *	the same interface. */
static inline int osp_declare_object_recreate(const struct lu_env *env,
					      struct dt_object *dt,
					      struct lu_attr *attr,
					      struct dt_allocation_hint *hint,
					      struct dt_object_format *dof,
					      struct thandle *th)
{
	return osp_md_declare_object_create(env, dt, attr, hint, dof, th);
}

static inline int osp_object_recreate(const struct lu_env *env,
				      struct dt_object *dt,
				      struct lu_attr *attr,
				      struct dt_allocation_hint *hint,
				      struct dt_object_format *dof,
				      struct thandle *th)
{
	return osp_md_object_create(env, dt, attr, hint, dof, th);
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

	if (hint != NULL && unlikely(hint->dah_flags & DAHF_RECREATE)) {
		LASSERT(fid_is_sane(lu_object_fid(&dt->do_lu)));

		rc = osp_declare_object_recreate(env, dt, attr, hint, dof, th);
		if (rc != 0)
			RETURN(rc);

		/* If the the MDT object is destroyed during recreate the
		 * OST-object, then the recreated OST-object via llog. */
		rc = osp_sync_declare_add(env, o, MDS_UNLINK64_REC, th);
		RETURN(rc);
	}

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

	if (hint != NULL && unlikely(hint->dah_flags & DAHF_RECREATE)) {
		rc = osp_object_recreate(env, dt, attr, hint, dof, th);
		RETURN(rc);
	}

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

int osp_declare_object_destroy(const struct lu_env *env,
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

int osp_object_destroy(const struct lu_env *env, struct dt_object *dt,
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

struct osp_orphan_it {
	int			  ooi_idx;
	int			  ooi_pos0;
	int			  ooi_pos1;
	int			  ooi_pos2;
	int			  ooi_total_npages;
	int			  ooi_valid_npages;
	unsigned int		  ooi_swab:1,
				  ooi_over:1;
	struct lu_fid		  ooi_tgt;
	struct lu_fid		  ooi_next;
	struct dt_object	 *ooi_obj;
	struct lu_orphan_ent	 *ooi_ent;
	struct page		 *ooi_cur_page;
	struct lu_idxpage	 *ooi_cur_idxpage;
	struct page		**ooi_pages;
};

static int osp_orphan_index_lookup(const struct lu_env *env,
				   struct dt_object *dt,
				   struct dt_rec *rec,
				   const struct dt_key *key,
				   struct lustre_capa *capa)
{
	return -EOPNOTSUPP;
}

static int osp_orphan_index_declare_insert(const struct lu_env *env,
					   struct dt_object *dt,
					   const struct dt_rec *rec,
					   const struct dt_key *key,
					   struct thandle *handle)
{
	return -EOPNOTSUPP;
}

static int osp_orphan_index_insert(const struct lu_env *env,
				   struct dt_object *dt,
				   const struct dt_rec *rec,
				   const struct dt_key *key,
				   struct thandle *handle,
				   struct lustre_capa *capa,
				   int ignore_quota)
{
	return -EOPNOTSUPP;
}

static int osp_orphan_index_declare_delete(const struct lu_env *env,
					   struct dt_object *dt,
					   const struct dt_key *key,
					   struct thandle *handle)
{
	return -EOPNOTSUPP;
}

static int osp_orphan_index_delete(const struct lu_env *env,
				   struct dt_object *dt,
				   const struct dt_key *key,
				   struct thandle *handle,
				   struct lustre_capa *capa)
{
	return -EOPNOTSUPP;
}

static struct dt_it *osp_orphan_it_init(const struct lu_env *env,
					struct dt_object *dt,
					__u32 attr,
					struct lustre_capa *capa)
{
	struct osp_orphan_it *it;

	OBD_ALLOC_PTR(it);
	if (it == NULL)
		return ERR_PTR(-ENOMEM);

	it->ooi_idx = -1;
	it->ooi_pos2 = -1;
	it->ooi_obj = dt;
	return (struct dt_it *)it;
}

static void osp_orphan_it_fini(const struct lu_env *env,
			       struct dt_it *di)
{
	struct osp_orphan_it	 *it		= (struct osp_orphan_it *)di;
	struct page		**pages 	= it->ooi_pages;
	int			  npages	= it->ooi_total_npages;
	int			  i;

	if (pages != NULL) {
		for (i = 0; i < npages; i++) {
			if (pages[i] != NULL) {
				if (pages[i] == it->ooi_cur_page) {
					kunmap(pages[i]);
					it->ooi_cur_page = NULL;
				}
				__free_page(pages[i]);
			}
		}
		OBD_FREE(pages, npages * sizeof(*pages));
	}
	OBD_FREE_PTR(it);
}

static int osp_orphan_it_fetch(const struct lu_env *env,
			       struct osp_orphan_it *it)
{
	struct osp_device	 *osp	= lu2osp_dev(it->ooi_obj->do_lu.lo_dev);
	struct page		**pages;
	struct ptlrpc_request	 *req	= NULL;
	struct ptlrpc_bulk_desc  *desc;
	struct idx_info 	 *ii;
	int			  npages;
	int			  rc;
	int			  i;
	ENTRY;

	/* 1MB bulk */
	npages = min_t(unsigned int, OFD_MAX_BRW_SIZE, 1 << 20);
	npages /= PAGE_CACHE_SIZE;

	OBD_ALLOC(pages, npages * sizeof(*pages));
	if (pages == NULL)
		RETURN(-ENOMEM);

	it->ooi_pages = pages;
	it->ooi_total_npages = npages;
	for (i = 0; i < npages; i++) {
		pages[i] = alloc_page(GFP_IOFS);
		if (pages[i] == NULL)
			RETURN(-ENOMEM);
	}

	req = ptlrpc_request_alloc(osp->opd_obd->u.cli.cl_import,
				   &RQF_OBD_IDX_READ);
	if (req == NULL)
		RETURN(-ENOMEM);

	rc = ptlrpc_request_pack(req, LUSTRE_OBD_VERSION, OBD_IDX_READ);
	if (rc != 0) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	req->rq_request_portal = OST_IDX_PORTAL;
	ptlrpc_at_set_req_timeout(req);

	desc = ptlrpc_prep_bulk_imp(req, npages, 1, BULK_PUT_SINK,
				    MDS_BULK_PORTAL);
	if (desc == NULL) {
		ptlrpc_request_free(req);
		RETURN(-ENOMEM);
	}

	for (i = 0; i < npages; i++)
		ptlrpc_prep_bulk_page_pin(desc, pages[i], 0, PAGE_CACHE_SIZE);

	ii = req_capsule_client_get(&req->rq_pill, &RMF_IDX_INFO);
	memset(ii, 0, sizeof(*ii));
	ii->ii_fid = it->ooi_tgt;
	ii->ii_magic = IDX_INFO_MAGIC;
	ii->ii_flags = II_FL_NOHASH | IT_FL_VIRTUAL | IT_FL_BIGKEY;
	ii->ii_count = npages * LU_PAGE_COUNT;
	ii->ii_seq_start = it->ooi_next.f_seq;
	ii->ii_oid_start = it->ooi_next.f_oid;
	ii->ii_ver_start = it->ooi_next.f_ver;
	ii->ii_index = it->ooi_idx;

	ptlrpc_request_set_replen(req);
	rc = ptlrpc_queue_wait(req);
	if (rc != 0)
		GOTO(out, rc);

	rc = sptlrpc_cli_unwrap_bulk_read(req, req->rq_bulk,
					  req->rq_bulk->bd_nob_transferred);
	if (rc < 0)
		GOTO(out, rc);

	ii = req_capsule_server_get(&req->rq_pill, &RMF_IDX_INFO);
	if (ii->ii_magic != IDX_INFO_MAGIC)
		 GOTO(out, rc = -EPROTO);

	if (!(ii->ii_flags & IT_FL_BIGKEY))
		GOTO(out, rc = -EOPNOTSUPP);

	npages = (ii->ii_count + LU_PAGE_COUNT - 1) >>
		 (PAGE_CACHE_SHIFT - LU_PAGE_SHIFT);
	if (npages > it->ooi_total_npages) {
		CERROR("%s: returned more pages than expected, %u > %u\n",
		       osp->opd_obd->obd_name, npages, it->ooi_total_npages);
		GOTO(out, rc = -EINVAL);
	}

	it->ooi_valid_npages = npages;
	if (ptlrpc_rep_need_swab(req))
		it->ooi_swab = 1;

	/* Usually, the "oid == 0" is only used for LAST_ID file, but LAST_ID
	 * file cannot be orphan. So here reusing "oid == 0" to indicate that
	 * there are not more orphans. */
	if (unlikely(ii->ii_oid_end == 0))
		it->ooi_over = 1;

	it->ooi_next.f_seq = ii->ii_seq_end;
	it->ooi_next.f_oid = ii->ii_oid_end;
	it->ooi_next.f_ver = ii->ii_ver_end;

	GOTO(out, rc = 0);

out:
	ptlrpc_req_finished(req);
	return rc;
}

static int osp_orphan_it_next(const struct lu_env *env,
			      struct dt_it *di)
{
	struct osp_orphan_it	 *it		= (struct osp_orphan_it *)di;
	struct lu_idxpage	 *idxpage;
	struct page		**pages;
	int			  rc;
	int			  i;
	ENTRY;

again2:
	idxpage = it->ooi_cur_idxpage;
	if (idxpage != NULL) {
		if (idxpage->lip_nr == 0)
			RETURN(1);

		it->ooi_pos2++;
		if (it->ooi_pos2 < idxpage->lip_nr) {
			it->ooi_ent =
				(struct lu_orphan_ent *)idxpage->lip_entries +
				it->ooi_pos2;
			if (it->ooi_swab)
				lustre_swab_orphan_ent(it->ooi_ent);
			RETURN(0);
		}

		it->ooi_cur_idxpage = NULL;
		it->ooi_pos1++;

again1:
		if (it->ooi_pos1 < LU_PAGE_COUNT) {
			it->ooi_cur_idxpage = (void *)it->ooi_cur_page +
					      LU_PAGE_SIZE * it->ooi_pos1;
			if (it->ooi_swab)
				lustre_swab_lip_header(it->ooi_cur_idxpage);
			if (it->ooi_cur_idxpage->lip_magic != LIP_MAGIC) {
				struct osp_device *osp =
					lu2osp_dev(it->ooi_obj->do_lu.lo_dev);

				CERROR("%s: invalid magic (%x != %x) for page "
				       "%d/%d while read layout orphan index\n",
				       osp->opd_obd->obd_name,
				       it->ooi_cur_idxpage->lip_magic,
				       LIP_MAGIC, it->ooi_pos0, it->ooi_pos1);
				/* Skip this lu_page next time. */
				it->ooi_pos2 = idxpage->lip_nr - 1;
				RETURN(-EINVAL);
			}
			it->ooi_pos2 = -1;
			goto again2;
		}

		kunmap(it->ooi_cur_page);
		it->ooi_cur_page = NULL;
		it->ooi_pos0++;

again0:
		pages = it->ooi_pages;
		if (it->ooi_pos0 < it->ooi_valid_npages) {
			it->ooi_cur_page = kmap(pages[it->ooi_pos0]);
			it->ooi_pos1 = 0;
			goto again1;
		}

		if (fid_is_zero(&it->ooi_next))
			RETURN(1);

		for (i = 0; i < it->ooi_total_npages; i++) {
			if (pages[i] != NULL)
				__free_page(pages[i]);
		}
		OBD_FREE(pages, it->ooi_total_npages * sizeof(*pages));

		it->ooi_pos0 = 0;
		it->ooi_total_npages = 0;
		it->ooi_valid_npages = 0;
		it->ooi_swab = 0;
		it->ooi_ent = NULL;
		it->ooi_cur_page = NULL;
		it->ooi_cur_idxpage = NULL;
		it->ooi_pages = NULL;
	}

	if (it->ooi_over)
		RETURN(1);

	rc = osp_orphan_it_fetch(env, it);
	if (rc == 0)
		goto again0;

	RETURN(rc);
}

static int osp_orphan_it_get(const struct lu_env *env,
			     struct dt_it *di,
			     const struct dt_key *key)
{
	struct osp_orphan_it	*it	= (struct osp_orphan_it *)di;
	struct idx_info		*ii	= (struct idx_info *)key;
	int			 rc;

	/* Forbid to set iteration position after iteration started. */
	if (it->ooi_idx != -1)
		return -EPERM;

	LASSERT(ii->ii_index != -1);

	it->ooi_next.f_seq = ii->ii_seq_start;
	it->ooi_next.f_oid = ii->ii_oid_start;
	it->ooi_next.f_ver = ii->ii_ver_start;
	it->ooi_tgt = ii->ii_fid;
	it->ooi_idx = ii->ii_index;
	rc = osp_orphan_it_next(env, di);
	if (rc == 1)
		return 0;
	if (rc == 0)
		return 1;
	return rc;
}

static void osp_orphan_it_put(const struct lu_env *env,
			      struct dt_it *di)
{
}

static struct dt_key *osp_orphan_it_key(const struct lu_env *env,
					const struct dt_it *di)
{
	struct osp_orphan_it	*it  = (struct osp_orphan_it *)di;
	struct lu_orphan_ent	*ent = it->ooi_ent;

	return (struct dt_key *)(&ent->loe_key);
}

static int osp_orphan_it_key_size(const struct lu_env *env,
				  const struct dt_it *di)
{
	return sizeof(struct lu_fid);
}

static int osp_orphan_it_rec(const struct lu_env *env,
			     const struct dt_it *di,
			     struct dt_rec *rec,
			     __u32 attr)
{
	struct osp_orphan_it	*it  = (struct osp_orphan_it *)di;
	struct lu_orphan_ent	*ent = it->ooi_ent;

	*(struct lu_orphan_rec *)rec = ent->loe_rec;
	return 0;
}

static __u64 osp_orphan_it_store(const struct lu_env *env,
				 const struct dt_it *di)
{
	return -E2BIG;
}

static int osp_orphan_it_load(const struct lu_env *env,
			      const struct dt_it *di,
			      __u64 hash)
{
	return -E2BIG;
}

static int osp_orphan_it_key_rec(const struct lu_env *env,
				const struct dt_it *di,
				void *key_rec)
{
	return 0;
}

static const struct dt_index_operations osp_orphan_index_ops = {
	.dio_lookup		= osp_orphan_index_lookup,
	.dio_declare_insert	= osp_orphan_index_declare_insert,
	.dio_insert		= osp_orphan_index_insert,
	.dio_declare_delete	= osp_orphan_index_declare_delete,
	.dio_delete		= osp_orphan_index_delete,
	.dio_it = {
		.init		= osp_orphan_it_init,
		.fini		= osp_orphan_it_fini,
		.next		= osp_orphan_it_next,
		.get		= osp_orphan_it_get,
		.put		= osp_orphan_it_put,
		.key		= osp_orphan_it_key,
		.key_size	= osp_orphan_it_key_size,
		.rec		= osp_orphan_it_rec,
		.store		= osp_orphan_it_store,
		.load		= osp_orphan_it_load,
		.key_rec	= osp_orphan_it_key_rec,
	}
};

static int osp_index_try(const struct lu_env *env,
			 struct dt_object *dt,
			 const struct dt_index_features *feat)
{
	if (feat == &dt_lfsck_orphan_features) {
		LASSERT(fid_is_last_id(lu_object_fid(&dt->do_lu)));

		dt->do_index_ops = &osp_orphan_index_ops;
		return 0;
	}

	return -EINVAL;
}

struct dt_object_operations osp_obj_ops = {
	.do_declare_attr_get	= osp_declare_attr_get,
	.do_attr_get		= osp_attr_get,
	.do_declare_attr_set	= osp_declare_attr_set,
	.do_attr_set		= osp_attr_set,
	.do_declare_xattr_get	= osp_declare_xattr_get,
	.do_xattr_get		= osp_xattr_get,
	.do_declare_xattr_set	= osp_declare_xattr_set,
	.do_xattr_set		= osp_xattr_set,
	.do_declare_create	= osp_declare_object_create,
	.do_create		= osp_object_create,
	.do_declare_destroy	= osp_declare_object_destroy,
	.do_destroy		= osp_object_destroy,
	.do_index_try		= osp_index_try,
};

static int osp_object_init(const struct lu_env *env, struct lu_object *o,
			   const struct lu_object_conf *conf)
{
	struct osp_object	*po = lu2osp_obj(o);
	int			rc = 0;
	ENTRY;

	spin_lock_init(&po->opo_lock);
	o->lo_header->loh_attr |= LOHA_REMOTE;

	if (is_ost_obj(o)) {
		po->opo_obj.do_ops = &osp_obj_ops;
	} else {
		struct lu_attr		*la = &osp_env_info(env)->osi_attr;

		po->opo_obj.do_ops = &osp_md_obj_ops;
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
