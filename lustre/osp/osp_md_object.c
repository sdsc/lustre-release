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
 * Lustre MDT Proxy Device
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include <lustre_log.h>
#include "osp_internal.h"

static const char dot[] = ".";
static const char dotdot[] = "..";

int osp_md_declare_object_create(const struct lu_env *env,
				 struct dt_object *dt,
				 struct lu_attr *attr,
				 struct dt_allocation_hint *hint,
				 struct dt_object_format *dof,
				 struct thandle *th)
{
	struct dt_update_request	*update;
	int				rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

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
		       dt->do_lu.lo_dev->ld_obd->obd_name,
		       PFID(lu_object_fid(&dt->do_lu)));

		rc = out_ref_del_pack(env, &update->dur_buf,
				      lu_object_fid(&dt->do_lu),
				      update->dur_batchid);
		if (rc != 0)
			GOTO(out, rc);

		if (S_ISDIR(lu_object_attr(&dt->do_lu))) {
			/* decrease for ".." */
			rc = out_ref_del_pack(env, &update->dur_buf,
					      lu_object_fid(&dt->do_lu),
					      update->dur_batchid);
			if (rc != 0)
				GOTO(out, rc);
		}

		rc = out_object_destroy_pack(env, &update->dur_buf,
					     lu_object_fid(&dt->do_lu),
					     update->dur_batchid);
		if (rc != 0)
			GOTO(out, rc);

		dt->do_lu.lo_header->loh_attr &= ~LOHA_EXISTS;
		/* Increase batchid to add this orphan object deletion
		 * to separate transaction */
		update_inc_batchid(update);
	}

	rc = out_create_pack(env, &update->dur_buf,
			     lu_object_fid(&dt->do_lu), attr, hint, dof,
			     update->dur_batchid);
	if (rc != 0)
		GOTO(out, rc);
out:
	if (rc)
		CERROR("%s: Insert update error: rc = %d\n",
		       dt->do_lu.lo_dev->ld_obd->obd_name, rc);

	return rc;
}

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

static int osp_md_declare_object_ref_del(const struct lu_env *env,
					 struct dt_object *dt,
					 struct thandle *th)
{
	struct dt_update_request	*update;
	struct lu_fid			*fid;
	int				rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);

	rc = out_ref_del_pack(env, &update->dur_buf,
			      lu_object_fid(&dt->do_lu),
			      update->dur_batchid);
	return rc;
}

static int osp_md_object_ref_del(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref del object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

static int osp_md_declare_ref_add(const struct lu_env *env,
				  struct dt_object *dt, struct thandle *th)
{
	struct dt_update_request	*update;
	int				rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

	rc = out_ref_add_pack(env, &update->dur_buf,
			      lu_object_fid(&dt->do_lu),
			      update->dur_batchid);

	return rc;
}

static int osp_md_object_ref_add(const struct lu_env *env,
				 struct dt_object *dt,
				 struct thandle *th)
{
	CDEBUG(D_INFO, "ref add object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	return 0;
}

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

int osp_md_declare_attr_set(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_attr *attr, struct thandle *th)
{
	struct dt_update_request	*update;
	int				rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

	rc = out_attr_set_pack(env, &update->dur_buf,
			       lu_object_fid(&dt->do_lu), attr,
			       update->dur_batchid);

	return rc;
}

int osp_md_attr_set(const struct lu_env *env, struct dt_object *dt,
		    const struct lu_attr *attr, struct thandle *th,
		    struct lustre_capa *capa)
{
	CDEBUG(D_INFO, "attr set object "DFID"\n",
	       PFID(&dt->do_lu.lo_header->loh_fid));

	RETURN(0);
}

static void osp_md_object_read_lock(const struct lu_env *env,
				    struct dt_object *dt, unsigned role)
{
	struct osp_object  *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner != env);
	down_read_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
}

static void osp_md_object_write_lock(const struct lu_env *env,
				     struct dt_object *dt, unsigned role)
{
	struct osp_object *obj = dt2osp_obj(dt);

	down_write_nested(&obj->opo_sem, role);

	LASSERT(obj->opo_owner == NULL);
	obj->opo_owner = env;
}

static void osp_md_object_read_unlock(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	up_read(&obj->opo_sem);
}

static void osp_md_object_write_unlock(const struct lu_env *env,
				       struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	LASSERT(obj->opo_owner == env);
	obj->opo_owner = NULL;
	up_write(&obj->opo_sem);
}

static int osp_md_object_write_locked(const struct lu_env *env,
				      struct dt_object *dt)
{
	struct osp_object *obj = dt2osp_obj(dt);

	return obj->opo_owner == env;
}

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
	struct lu_fid		   *fid;
	int			   rc;
	ENTRY;

	/* Because it needs send the update buffer right away,
	 * just create an update buffer, instead of attaching the
	 * update_remote list of the thandle.
	 */
	update = dt_update_request_create(dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = out_index_lookup_pack(env, &update->dur_buf,
				   lu_object_fid(&dt->do_lu), rec, key);
	if (rc != 0) {
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

	dt_update_request_destroy(update);

	return rc;
}

static int osp_md_declare_insert(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct osp_thandle	 *oth = thandle_to_osp_thandle(th);
	struct dt_update_request *update = oth->ot_dur;
	int			 rc;


	rc = out_index_insert_pack(env, &update->dur_buf,
				   lu_object_fid(&dt->do_lu), rec, key,
				   update->dur_batchid);
	if (rc != 0)
		return rc;

	/* Before async update is allowed, if it will insert remote
	 * name entry, it should make sure the local object is created,
	 * i.e. the remote update RPC should be sent after local
	 * update(create object) */
	oth->ot_send_updates_after_local_trans = 1;

	return rc;
}

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

static int osp_md_declare_delete(const struct lu_env *env,
				 struct dt_object *dt,
				 const struct dt_key *key,
				 struct thandle *th)
{
	struct dt_update_request *update;
	int			 rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

	rc = out_index_delete_pack(env, &update->dur_buf,
				   lu_object_fid(&dt->do_lu), key,
				   update->dur_batchid);
	return rc;
}

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

static struct dt_key *osp_it_key(const struct lu_env *env,
				 const struct dt_it *di)
{
	LBUG();
	return NULL;
}

static int osp_it_key_size(const struct lu_env *env, const struct dt_it *di)
{
	LBUG();
	return 0;
}

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

static int osp_it_load(const struct lu_env *env, const struct dt_it *di,
		       __u64 hash)
{
	LBUG();
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
		.key      = osp_it_key,
		.key_size = osp_it_key_size,
		.rec      = osp_md_index_it_rec,
		.store    = osp_it_store,
		.load     = osp_it_load,
		.key_rec  = osp_it_key_rec,
	}
};

static int osp_md_index_try(const struct lu_env *env,
			    struct dt_object *dt,
			    const struct dt_index_features *feat)
{
	dt->do_index_ops = &osp_md_index_ops;
	return 0;
}

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

static ssize_t osp_md_declare_write(const struct lu_env *env,
				    struct dt_object *dt,
				    const struct lu_buf *buf,
				    loff_t pos, struct thandle *th)
{
	struct dt_update_request  *update;
	ssize_t			  rc;

	update = thandle_to_dt_update_request(th);
	LASSERT(update != NULL);

	rc = out_write_pack(env, &update->dur_buf, lu_object_fid(&dt->do_lu),
			    buf, pos, update->dur_batchid);

	return rc;

}

static ssize_t osp_md_write(const struct lu_env *env, struct dt_object *dt,
			    const struct lu_buf *buf, loff_t *pos,
			    struct thandle *handle,
			    struct lustre_capa *capa, int ignore_quota)
{
	*pos += buf->lb_len;
	return buf->lb_len;
}

/* These body operation will be used to write symlinks during migration etc */
struct dt_body_operations osp_md_body_ops = {
	.dbo_declare_write	= osp_md_declare_write,
	.dbo_write		= osp_md_write,
};
