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
 */
/*
 * lustre/osp/osp_md_object.c
 *
 * Metadata OSP methods
 *
 * This file implementss methods for remote MD object, which includes
 * dt_object_operations, dt_index_operations and dt_body_operations.
 *
 * For cross-MDT operation, clients send the RPC to the master MDT,
 * then the operation will be decomposed into object updates, and
 * dispatched to OSD and OSP, the local object updates go to local OSD,
 * and the remote objects go to OSP. In OSP, these remote object
 * updates will packed into update RPC and being sent to the remote MDT
 * (Object Update Target OUT) to be handled.
 *
 * In DNE phase I, because of missing complete recovery solution, updates
 * will be executed orderly and synchronously.
 *     1. Initially, the transaction will be created.
 *     2. Then in transaction declare, it collects and packs remote
 *        updates(in osp_md_declare_xxx).
 *     3. Then in transaction start, it send these remote updates
 *     to remote MDTs, which will execute these updates synchronously.
 *     4. Then in transaction execute phase, the local updates will be
 *     executed asynchronously.
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include <lustre_log.h>
#include "osp_internal.h"

static const char dot[] = ".";
static const char dotdot[] = "..";

/**
 * Declare the remote object update.
 *
 * This method will declare the creation of remote object, i.e.
 * insert remote object create update into RPC. Note: if the object
 * is already being created, it needs add object destroy updates
 * ahead of create updates, i.e. destory the object then re-create
 * the object.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to be created.
 * param[in] attr	attribute of the created object.
 * param[in] hint	creation hint of the creation.
 * param[in] dof	the format information of the created
 *                      object.
 * param[in] th		the thansaction handle of the creation.
 *
 * retval		= 0 creation(update insertion) succeed
 *                      < 0 creation(update insertion) failed.
 */
int osp_md_declare_object_create(const struct lu_env *env,
				 struct dt_object *dt,
				 struct lu_attr *attr,
				 struct dt_allocation_hint *hint,
				 struct dt_object_format *dof,
				 struct thandle *th)
{
	struct osp_thread_info		*osi = osp_env_info(env);
	struct dt_update_request	*update;
	struct lu_fid			*fid1;
	int				sizes[2] = {sizeof(struct obdo), 0};
	char				*bufs[2] = {NULL, NULL};
	int				buf_count;
	int				rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	osi->osi_obdo.o_valid = 0;
	obdo_from_la(&osi->osi_obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, &osi->osi_obdo, &osi->osi_obdo);

	bufs[0] = (char *)&osi->osi_obdo;
	buf_count = 1;
	fid1 = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	if (hint != NULL && hint->dah_parent) {
		struct lu_fid *fid2;

		fid2 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid2);
		bufs[1] = (char *)fid2;
		buf_count++;
	}

	if (lu_object_exists(&dt->do_lu)) {
		/* If the object already exists, we needs to destroy
		 * this orphan object first.
		 *
		 * The scenario might happen in this case
		 *
		 * 1. client send remote create to MDT0.
		 * 2. MDT0 send create update to MDT1.
		 * 3. MDT1 finished create synchronously.
		 * 4. MDT0 failed and reboot.
		 * 5. client resend remote create to MDT0.
		 * 6. MDT0 tries to resend create update to MDT1,
		 *    but find the object already exists
		 */
		CDEBUG(D_HA, "%s: object "DFID" exists, destroy this orphan\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, PFID(fid1));

		rc = out_insert_update(env, update, OUT_REF_DEL, fid1, 0,
				       NULL, NULL);
		if (rc != 0)
			GOTO(out, rc);

		if (S_ISDIR(lu_object_attr(&dt->do_lu))) {
			/* decrease for ".." */
			rc = out_insert_update(env, update, OUT_REF_DEL, fid1,
					       0, NULL, NULL);
			if (rc != 0)
				GOTO(out, rc);
		}

		rc = out_insert_update(env, update, OUT_DESTROY, fid1, 0, NULL,
				       NULL);
		if (rc != 0)
			GOTO(out, rc);

		dt->do_lu.lo_header->loh_attr &= ~LOHA_EXISTS;
		/* Increase batchid to add this orphan object deletion
		 * to separate transaction */
		update_inc_batchid(update);
	}

	rc = out_insert_update(env, update, OUT_CREATE, fid1, buf_count, sizes,
			       (const char **)bufs);
out:
	if (rc)
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);

	return rc;
}

/**
 * Create remote object update
 *
 * This method is supposed to create the remote object, but in
 * DNE phase I, remote updates are actually being executed in transaction
 * start, i.e. the object is already being created when calling this
 * method, so only mark the necessary flags here.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be created.
 * param[in] attr	attribute of the created object.
 * param[in] hint	creation hint of the creation.
 * param[in] dof	the format information of the created
 *                      object.
 * param[in] th		the thansaction handle of the creation.
 *
 * retval		= 0 creation succeed
 *                      < 0  creation failed.
 */
int osp_md_object_create(const struct lu_env *env, struct dt_object *dt,
			 struct lu_attr *attr, struct dt_allocation_hint *hint,
			 struct dt_object_format *dof, struct thandle *th)
{
	CDEBUG(D_INFO, "create object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	/* Because the create update RPC will be sent during declare phase,
	 * if creation reaches here, it means the object has been created
	 * successfully */
	dt->do_lu.lo_header->loh_attr |= LOHA_EXISTS | (attr->la_mode & S_IFMT);

	return 0;
}

/**
 * Declare the remote object ref count descrease.
 *
 * This method will declare the refcount decrease of the remote object, i.e.
 * insert remote object refcount descrease update into RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		object to decrease the refcount.
 * param[in] th		the thansaction handle of refcount decrease.
 *
 * retval		= 0 refcount decrease(update insertion) succeed
 *                      <0  refcount decrease(update insertion) failed.
 */
static int osp_md_declare_object_ref_del(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		      (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = out_insert_update(env, update, OUT_REF_DEL, fid, 0, NULL, NULL);

	return rc;
}

/**
 * remote object ref count descrease.
 *
 * This method is supposed to decrease the refcount of the remote object, but
 * in DNE phase I, remote updates are actually being executed in transaction
 * start, i.e. the object refcount is already being decreased when calling
 * this method. So do nothing in this method for now.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be decrease the refcount.
 * param[in] th		the thansaction handle of the creation.
 *
 * retval		= 0 refcount decrease succeed
 *                      <0  refcount decrease failed.
 */
static int osp_md_object_ref_del(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref del object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

/**
 * Declare the remote object refcount increase.
 *
 * This method will declare the refcount increase of the remote object, i.e.
 * insert remote object refcount increase update into RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to increase the refcount.
 * param[in] th		the thansaction handle of refcount add.
 *
 * retval		= 0 refcount decrease(update insertion) succeed
 *                      <0  refcount decrease(update insertion) failed.
 */
static int osp_md_declare_ref_add(const struct lu_env *env,
				  struct dt_object *dt, struct thandle *th)
{
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = out_insert_update(env, update, OUT_REF_ADD, fid, 0, NULL, NULL);

	return rc;
}

/**
 * Increase the refcount of the remote object.
 *
 * This method is supposed to increase the refcount of the remote object,  but
 * in DNE phase I, remote updates are actually being executed in transaction
 * start, i.e. the object refcount is already being increased when calling this
 * method. So do nothing in this method for now.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to increase the refcount.
 * param[in] th		the thansaction handle of refcount add.
 *
 * retval		= 0 refcount increase succeed
 *                      <0  refcount increase failed.
 */
static int osp_md_object_ref_add(const struct lu_env *env, struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref add object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

/**
 * Initialize the allocation hint for object creation, which is usually being
 * called before the creation, and these hints(parent and child mode) will be
 * be sent to the remote OUT and used in the object create process, same as
 * osd object create.
 *
 * param[in] env	execution environment.
 * param[in] ah		the hint to be initialized.
 * param[in] parent	the parent of the object.
 * param[in] child	the object to be created.
 * param[in] child_mode the mode of the created object.
 */
static void osp_md_ah_init(const struct lu_env *env,
			   struct dt_allocation_hint *ah,
			   struct dt_object *parent,
			   struct dt_object *child,
			   umode_t child_mode)
{
	LASSERT(ah);

	ah->dah_parent = parent;
	ah->dah_mode = child_mode;
}

/**
 * Declare the remote object attribute set.
 *
 * This method will declare setting attributes of the remote object, i.e.
 * insert remote object attr_set update into RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to set attributes.
 * param[in] attr	attributes to be set.
 * param[in] th		the thansaction handle of attr_set.
 *
 * retval		= 0 attributes set(update insertion) succeed
 *                      < 0 attributes set(update insertion) failed.
 */
int osp_md_declare_attr_set(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_attr *attr, struct thandle *th)
{
	struct osp_thread_info		*osi = osp_env_info(env);
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				size = sizeof(struct obdo);
	char				*buf;
	int				rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	osi->osi_obdo.o_valid = 0;
	obdo_from_la(&osi->osi_obdo, (struct lu_attr *)attr,
		     attr->la_valid);
	lustre_set_wire_obdo(NULL, &osi->osi_obdo, &osi->osi_obdo);

	buf = (char *)&osi->osi_obdo;
	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = out_insert_update(env, update, OUT_ATTR_SET, fid, 1, &size,
			       (const char **)&buf);

	return rc;
}

/**
 * attribute set of the remote object.
 *
 * This method is supposed to set attributes of the remote object, but
 * in DNE phase I, remote updates are actually being executed in transaction
 * start, i.e. object attributes are already being set when calling this method.
 * So do nothing in this method for now.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to set attributes.
 * param[in] attr	attributes to be set.
 * param[in] th		the thansaction handle of attr_set.
 * param[in] capa	capablity of setting attributes, and it is not being
 *                      implemented yet, only adding because of object API
 *                      interface.
 *
 * retval		= 0 attributes set(update insertion) succeed
 *                      < 0 attributes set(update insertion) failed.
 */
int osp_md_attr_set(const struct lu_env *env, struct dt_object *dt,
		    const struct lu_attr *attr, struct thandle *th,
		    struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "attr set object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	RETURN(0);
}

/**
 * lock the remote object in read mode.
 *
 * These methods(osp_md_object_xx_lock) will only lock the remote object
 * in local cache, which uses the semaphore(opo_sem) inside the osp_object
 * to lock the object. Note: it will not lock the object in the whole
 * cluster, which will rely on the ldlm lock.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be locked
 * param[in] role	lock role from MDD layer, see mdd_object_role.
 */
static void osp_md_object_read_lock(const struct lu_env *env,
				    struct dt_object *dt, unsigned role)
{
	struct osp_object  *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner != env);
	down_read_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
}

/**
 * lock the remote object in write mode.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be locked
 * param[in] role	lock role from MDD layer, see mdd_object_role.
 */
static void osp_md_object_write_lock(const struct lu_env *env,
				     struct dt_object *dt, unsigned role)
{
	struct osp_object *obj = dt2osp_obj(dt);

	down_write_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
	obj->opo_owner = env;
}

/**
 * unlock the readlock of remote object.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be unlocked
 */
static void osp_md_object_read_unlock(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	up_read(&obj->opo_sem);
}

/**
 * unlock the writelock of remote object.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be unlocked
 */
static void osp_md_object_write_unlock(const struct lu_env *env,
				       struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner == env);
	obj->opo_owner = NULL;
	up_write(&obj->opo_sem);
}

/**
 * Check whether the object is being locked in write mode.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be checked.
 */
static int osp_md_object_write_locked(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	return obj->opo_owner == env;
}

/**
 * lookup record(by key) under a remote index object.
 *
 * This method will pack object lookup update into RPC and send to
 * the remote OUT and wait the lookup result.
 *
 * param[in] env	execution environment
 * param[in] dt		index object to lookup.
 * param[in] rec	record to be lookup.
 * param[in] key	key of this lookup.
 * param[in] capa	capablity of this lookup, which is not being implemented
 *                      yet.
 * retval		= 1 lookup succeed, get the record.
 *                      other value lookup failed, did not get record.
 */
static int osp_md_index_lookup(const struct lu_env *env, struct dt_object *dt,
			       struct dt_rec *rec, const struct dt_key *key,
			       struct lustre_capa *capa)
{
	struct lu_buf		*lbuf	= &osp_env_info(env)->osi_lb2;
	struct osp_device	*osp	= lu2osp_dev(dt->do_lu.lo_dev);
	struct dt_device	*dt_dev	= &osp->opd_dt_dev;
	struct dt_update_request   *update;
	struct object_update_reply *reply;
	struct ptlrpc_request	   *req = NULL;
	int			   size = strlen((char *)key) + 1;
	struct lu_fid		   *fid;
	int			   rc;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	update = out_create_update_req(dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = out_insert_update(env, update, OUT_INDEX_LOOKUP,
			       lu_object_fid(&dt->do_lu),
			       1, &size, (const char **)&key);
	if (rc) {
		CERROR("%s: Insert update error: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = out_remote_sync(env, osp->opd_obd->u.cli.cl_import, update, &req);
	if (rc < 0)
		GOTO(out, rc);

	reply = req_capsule_server_sized_get(&req->rq_pill,
					     &RMF_OUT_UPDATE_REPLY,
					     OUT_UPDATE_REPLY_SIZE);
	if (reply->ourp_magic != UPDATE_REPLY_MAGIC) {
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       reply->ourp_magic, UPDATE_REPLY_MAGIC, -EPROTO);
		GOTO(out, rc = -EPROTO);
	}

	rc = object_update_result_data_get(reply, lbuf, 0);
	if (rc < 0)
		GOTO(out, rc);

	if (lbuf->lb_len != sizeof(*fid)) {
		CERROR("%s: lookup "DFID" %s wrong size %d\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key,
		       (int)lbuf->lb_len);
		GOTO(out, rc = -EINVAL);
	}

	fid = lbuf->lb_buf;
	if (ptlrpc_rep_need_swab(req))
		lustre_swab_lu_fid(fid);
	if (!fid_is_sane(fid)) {
		CERROR("%s: lookup "DFID" %s invalid fid "DFID"\n",
		       dt_dev->dd_lu_dev.ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)), (char *)key, PFID(fid));
		GOTO(out, rc = -EINVAL);
	}

	memcpy(rec, fid, sizeof(*fid));

	GOTO(out, rc = 1);

out:
	if (req != NULL)
		ptlrpc_req_finished(req);

	out_destroy_update_req(update);

	return rc;
}

/**
 * Declare the remote index object insert.
 *
 * This method will declare the index insert of the remote object, i.e.
 * insert index insert update into RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to index insert.
 * param[in] rec	record of insert index.
 * param[in] key	key of insert index.
 * param[in] th		the thansaction handle of refcount add.
 *
 * retval		= 0 index insert(update insertion) succeed
 *                      <0  index insert(update insertion) failed.
 */
static int osp_md_declare_insert(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct dt_update_request *update;
	struct lu_fid		 *fid;
	struct lu_fid		 *rec_fid = (struct lu_fid *)rec;
	int			 size[2] = {strlen((char *)key) + 1,
						  sizeof(*rec_fid)};
	const char		 *bufs[2] = {(char *)key, (char *)rec_fid};
	int			 rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	CDEBUG(D_INFO, "%s: insert index of "DFID" %s: "DFID"\n",
	       dt->do_lu.lo_dev->ld_obd->obd_name,
	       PFID(fid), (char *)key, PFID(rec_fid));

	fid_cpu_to_le(rec_fid, rec_fid);

	rc = out_insert_update(env, update, OUT_INDEX_INSERT, fid,
			       ARRAY_SIZE(size), size, bufs);
	return rc;
}

/**
 * The remote index object insert.
 *
 * This method is supposed to do the index insert of the remote
 * object, but in DNE phase I, remote updates are actually being
 * executed in transaction start, i.e. the index insert is already
 * being inserted when calling this method. So do nothing in this
 * method for now.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to index insert.
 * param[in] rec	record of insert index.
 * param[in] key	key of insert index.
 * param[in] th		the thansaction handle of index insert.
 * param[in] ignore_quota whether ignore quota for this operation.
 *
 * retval		= 0 index insert(update insertion) succeed
 *                      < 0 index insert(update insertion) failed.
 */
static int osp_md_index_insert(const struct lu_env *env,
			       struct dt_object *dt,
			       const struct dt_rec *rec,
			       const struct dt_key *key,
			       struct thandle *th,
			       struct lustre_capa *capa,
			       int ignore_quota)
{
	return 0;
}

/**
 * Declare the remote index object insert.
 *
 * This method will declare the index delete of the remote object, i.e.
 * insert index delete update into RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to index delete.
 * param[in] key	key of index delete.
 * param[in] th		the thansaction handle of index delete.
 *
 * retval		= 0 index insert(update insertion) succeed
 *                      < 0 index insert(update insertion) failed.
 */
static int osp_md_declare_delete(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct dt_update_request *update;
	struct lu_fid *fid;
	int size = strlen((char *)key) + 1;
	int rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = out_insert_update(env, update, OUT_INDEX_DELETE, fid, 1, &size,
			       (const char **)&key);

	return rc;
}

/**
 * The remote object index delete.
 *
 * This method is supposed to do the index delete of the remote
 * object, but in DNE phase I, remote updates are actually being
 * executed in transaction start, i.e. the index is already being
 * deleted when calling this method. So do nothing in this method
 * for now.
 *
 * param[in] env	execution environment
 * param[in] dt		Remote object to index delete.
 * param[in] key	key of index delete.
 * param[in] th		the thansaction handle of refcount add.
 *
 * retval		= 0 index delete succeed
 *                      <0  index delete failed.
 */
static int osp_md_index_delete(const struct lu_env *env,
			       struct dt_object *dt,
			       const struct dt_key *key,
			       struct thandle *th,
			       struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "index delete "DFID" %s\n",
	       PFID(&dt->do_lu.lo_header->loh_fid), (char *)key);

	return 0;
}

/**
 * The remote object index it_next.
 *
 * This method will locate the pointer of the iterator to the next entry,
 * it share the similar internal implementation with osp_orphan_it_next,
 * which is being used for remote orphan index object. This method will
 * be used for remote directory.
 *
 * param[in] env	execution environment
 * param[in] di		iterator of this iteration.
 *
 * retval		= 0 successfuly put the pointer to the next entry.
 *                      = 1 to the end of the index object.
 *                      other value failed to set the pointer.
 */
int osp_md_index_it_next(const struct lu_env *env, struct dt_it *di)
{
	struct osp_it		*it = (struct osp_it *)di;
	struct lu_idxpage	*idxpage;
	struct lu_dirent	*ent = (struct lu_dirent *)it->ooi_ent;
	int			rc;
	ENTRY;

again:
	idxpage = it->ooi_cur_idxpage;
	if (idxpage != NULL) {
		if (idxpage->lip_nr == 0)
			RETURN(1);

		it->ooi_pos_ent++;
		if (ent == NULL) {
			it->ooi_ent =
			      (struct lu_dirent *)idxpage->lip_entries;
			RETURN(0);
		} else if (le16_to_cpu(ent->lde_reclen) != 0 &&
			   it->ooi_pos_ent < idxpage->lip_nr) {
			ent = (struct lu_dirent *)(((char *)ent) +
					le16_to_cpu(ent->lde_reclen));
			it->ooi_ent = ent;
			RETURN(0);
		} else {
			it->ooi_ent = NULL;
		}
	}

	rc = osp_it_next_page(env, di);
	if (rc == 0)
		goto again;

	RETURN(rc);
}

/**
 * This method will be used to get record from current position of
 * the iterator.
 *
 * param[in] env	execution environment
 * param[in] di		iterator of this iteration.
 * param[out] rec	hold the record.
 * param[in] attr	attributes of the index object, so it knows
 * 			how to packed the entry.
 *
 * retval		= 0 successfuly get the record.
 *                      = other value if it is failed.
 */
static int osp_md_index_it_rec(const struct lu_env *env, const struct dt_it *di,
			       struct dt_rec *rec, __u32 attr)
{
	struct osp_it		*it = (struct osp_it *)di;
	struct lu_dirent	*ent = (struct lu_dirent *)it->ooi_ent;
	int			reclen;

	reclen = lu_dirent_calc_size(le16_to_cpu(ent->lde_namelen), attr);
	memcpy(rec, ent, reclen);
	return 0;
}

const struct dt_index_operations osp_md_index_ops = {
	.dio_lookup         = osp_md_index_lookup,
	.dio_declare_insert = osp_md_declare_insert,
	.dio_insert         = osp_md_index_insert,
	.dio_declare_delete = osp_md_declare_delete,
	.dio_delete         = osp_md_index_delete,
	.dio_it     = {
		.init     = osp_it_init,
		.fini     = osp_it_fini,
		.get      = osp_it_get,
		.put      = osp_it_put,
		.next     = osp_md_index_it_next,
		.rec      = osp_md_index_it_rec,
		.store    = osp_it_store,
		.key_rec  = osp_it_key_rec,
	}
};

/**
 * This method will try to initialize the index api pointer for the
 * given object, usually it is the entry point of the index api. i.e.
 * the index object should be initialized in index_try, then start
 * using index api.
 *
 * param[in] env	execution environment
 * param[in] dt		index object to be initialized.
 * param[in] feat	the index feature of the object.
 *
 * retval		= 0 sucessfully initialize the index object.
 * 			< 0 initialization failed.
 */
static int osp_md_index_try(const struct lu_env *env,
			    struct dt_object *dt,
			    const struct dt_index_features *feat)
{
	dt->do_index_ops = &osp_md_index_ops;
	return 0;
}

/**
 * This method will enqueue the lock(by ldlm_cli_enqueue) of remote
 * object on the remote MDT, which will lock the object in the global
 * namespace.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be locked.
 * param[out] lh	lock handle.
 * param[in] einfo	enqueue information.
 * param[in] policy	lock policy.
 *
 * retval		= ELDLM_OK enqueue the lock successfully.
 * 			= other value if it is failed.
 */
static int osp_md_object_lock(const struct lu_env *env,
			      struct dt_object *dt,
			      struct lustre_handle *lh,
			      struct ldlm_enqueue_info *einfo,
			      ldlm_policy_data_t *policy)
{
	struct ldlm_res_id	*res_id;
	struct dt_device	*dt_dev = lu2dt_dev(dt->do_lu.lo_dev);
	struct osp_device	*osp = dt2osp_dev(dt_dev);
	struct ptlrpc_request	*req;
	int			rc = 0;
	__u64			flags = 0;
	ldlm_mode_t		mode;

	res_id = einfo->ei_res_id;
	LASSERT(res_id != NULL);

	mode = ldlm_lock_match(osp->opd_obd->obd_namespace,
			       LDLM_FL_BLOCK_GRANTED, res_id,
			       einfo->ei_type, policy,
			       einfo->ei_mode, lh, 0);
	if (mode > 0)
		return ELDLM_OK;

	req = ldlm_enqueue_pack(osp->opd_exp, 0);
	if (IS_ERR(req))
		RETURN(PTR_ERR(req));

	rc = ldlm_cli_enqueue(osp->opd_exp, &req, einfo, res_id,
			      (const ldlm_policy_data_t *)policy,
			      &flags, NULL, 0, LVB_T_NONE, lh, 0);

	ptlrpc_req_finished(req);

	return rc == ELDLM_OK ? 0 : -EIO;
}

/**
 * This method will cancel the lock of remote object.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be locked.
 * param[in] einfo	enqueue information.
 * param[in] policy	lock policy.
 *
 * retval		= 0 cancel the lock successfully.
 * 			= other value if it is failed.
 */
static int osp_md_object_unlock(const struct lu_env *env,
				struct dt_object *dt,
				struct ldlm_enqueue_info *einfo,
				ldlm_policy_data_t *policy)
{
	struct lustre_handle	*lockh = einfo->ei_cbdata;

	/* unlock finally */
	ldlm_lock_decref(lockh, einfo->ei_mode);

	return 0;
}

struct dt_object_operations osp_md_obj_ops = {
	.do_read_lock         = osp_md_object_read_lock,
	.do_write_lock        = osp_md_object_write_lock,
	.do_read_unlock       = osp_md_object_read_unlock,
	.do_write_unlock      = osp_md_object_write_unlock,
	.do_write_locked      = osp_md_object_write_locked,
	.do_declare_create    = osp_md_declare_object_create,
	.do_create            = osp_md_object_create,
	.do_declare_ref_add   = osp_md_declare_ref_add,
	.do_ref_add           = osp_md_object_ref_add,
	.do_declare_ref_del   = osp_md_declare_object_ref_del,
	.do_ref_del           = osp_md_object_ref_del,
	.do_declare_destroy   = osp_declare_object_destroy,
	.do_destroy           = osp_object_destroy,
	.do_ah_init           = osp_md_ah_init,
	.do_attr_get	      = osp_attr_get,
	.do_declare_attr_set  = osp_md_declare_attr_set,
	.do_attr_set          = osp_md_attr_set,
	.do_xattr_get         = osp_xattr_get,
	.do_declare_xattr_set = osp_declare_xattr_set,
	.do_xattr_set         = osp_xattr_set,
	.do_declare_xattr_del = osp_declare_xattr_del,
	.do_xattr_del         = osp_xattr_del,
	.do_index_try         = osp_md_index_try,
	.do_object_lock       = osp_md_object_lock,
	.do_object_unlock     = osp_md_object_unlock,
};

/**
 * This method will declare the object write, in DNE phase I, it will pack
 * the write object update into the RPC.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be written.
 * param[in] buf	buffer to write.
 * param[in] pos	offset of write.
 * param[in] th		transaction handle of the write.
 *
 * retval		= 0 declare write successfully.
 * 			= other value if it is failed.
 */
static ssize_t osp_md_declare_write(const struct lu_env *env,
				    struct dt_object *dt,
				    const struct lu_buf *buf,
				    loff_t pos, struct thandle *th)
{
	struct dt_update_request  *update;
	struct lu_fid		  *fid;
	int			  sizes[2] = {buf->lb_len, sizeof(pos)};
	const char		  *bufs[2] = {(char *)buf->lb_buf,
					      (char *)&pos};
	ssize_t			  rc;

	update = out_find_create_update_loc(th, dt);
	if (IS_ERR(update)) {
		CERROR("%s: Get OSP update buf failed: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       (int)PTR_ERR(update));
		return PTR_ERR(update);
	}

	pos = cpu_to_le64(pos);
	bufs[1] = (char *)&pos;
	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	rc = out_insert_update(env, update, OUT_WRITE, fid,
			       ARRAY_SIZE(sizes), sizes, bufs);

	return rc;

}

/**
 * This method is supposed to write the buffer to the remote object,
 * but in DNE phase I, remote updates are actually being executed in
 * transaction start, the buffer is already being written when this
 * method is being called. So do nothing in this method for now.
 *
 * param[in] env	execution environment
 * param[in] dt		object to be written.
 * param[in] buf	buffer to write.
 * param[in] pos	offset of write.
 * param[in] th		transaction handle of the write.
 * param[in] capa	capablity of the write which is not being implemented
 * 			yet.
 * param[in] ignore_quota if ignore_quota for this write.
 *
 * retval		= length to be written, if write successfully.
 * 			< 0 if write is failed.
 */
static ssize_t osp_md_write(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_buf *buf, loff_t *pos,
			    struct thandle *handle,
			    struct lustre_capa *capa, int ignore_quota)
{
	return buf->lb_len;
}

/* These body operation will be used to write symlinks during migration etc */
struct dt_body_operations osp_md_body_ops = {
	.dbo_declare_write	= osp_md_declare_write,
	.dbo_write		= osp_md_write,
};
