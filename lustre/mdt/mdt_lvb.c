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
 * Copyright (c) 2012, 2015, Intel Corporation.
 * Use is subject to license terms.
 *
 * lustre/mdt/mdt_lvb.c
 *
 * Author: Jinshan Xiong <jinshan.xiong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_MDS
#include <lustre_swab.h>
#include "mdt_internal.h"

/* Called with res->lr_lvb_sem held */
static int mdt_lvbo_init(struct ldlm_resource *res)
{
	if (IS_LQUOTA_RES(res)) {
		struct mdt_device	*mdt;

		mdt = ldlm_res_to_ns(res)->ns_lvbp;
		if (mdt->mdt_qmt_dev == NULL)
			return 0;

		/* call lvbo init function of quota master */
		return qmt_hdls.qmth_lvbo_init(mdt->mdt_qmt_dev, res);
	}
	return 0;
}

static int mdt_dom_lvb_alloc(struct ldlm_resource *res)
{
	struct ost_lvb *lvb;

	if (res->lr_lvb_data != NULL)
		return 0;

	OBD_ALLOC_PTR(lvb);
	if (lvb == NULL)
		return -ENOMEM;

	res->lr_lvb_data = lvb;
	res->lr_lvb_len = sizeof(*lvb);

	/* Store error in LVB to inidicate it has no data yet.
	 */
	OST_LVB_SET_ERR(lvb->lvb_blocks, -ENODATA);
	return 0;
}

int mdt_dom_lvb_is_valid(struct ldlm_resource *res)
{
	struct ost_lvb *res_lvb = res->lr_lvb_data;

	return !(res_lvb == NULL || OST_LVB_IS_ERR(res_lvb->lvb_blocks));
}

static int mdt_dom_lvbo_update(struct ldlm_resource *res,
			       struct ldlm_lock *lock,
			       struct ptlrpc_request *req, int increase_only)
{
	struct obd_export *exp = lock ? lock->l_export : NULL;
	struct mdt_device *mdt;
	struct mdt_object *mo;
	struct mdt_thread_info *info;
	struct ost_lvb *lvb;
	struct lu_env env;
	struct lu_fid *fid;
	struct md_attr *ma;
	int rc = 0;

	ENTRY;

	/* Before going further let's check that OBD and export are healthy.
	 */
	if (exp != NULL &&
	    (exp->exp_disconnected || exp->exp_failed ||
	     exp->exp_obd->obd_stopping)) {
		CDEBUG(D_INFO, "Skip LVB update, export is %s, obd is %s\n",
		       exp->exp_failed ? "failed" : "disconnected",
		       exp->exp_obd->obd_stopping ? "stopping" : "OK");
		RETURN(0);
	}

	rc = mdt_dom_lvb_alloc(res);
	if (rc < 0)
		RETURN(rc);

	mdt = ldlm_res_to_ns(res)->ns_lvbp;
	if (mdt == NULL)
		RETURN(-ENOENT);

	rc = lu_env_init(&env, LCT_MD_THREAD);
	if (rc)
		RETURN(rc);

	info = lu_context_key_get(&env.le_ctx, &mdt_thread_key);
	if (info == NULL)
		GOTO(out_env, rc = -ENOMEM);

	memset(info, 0, sizeof *info);
	info->mti_env = &env;
	info->mti_exp = req ? req->rq_export : NULL;
	info->mti_mdt = mdt;

	fid = &info->mti_tmp_fid2;
	fid_extract_from_res_name(fid, &res->lr_name);

	lvb = res->lr_lvb_data;
	LASSERT(lvb);

	/* Update the LVB from the network message */
	if (req != NULL) {
		struct ost_lvb *rpc_lvb;

		rpc_lvb = req_capsule_server_swab_get(&req->rq_pill,
						      &RMF_DLM_LVB,
						      lustre_swab_ost_lvb);
		if (rpc_lvb == NULL)
			goto disk_update;

		lock_res(res);
		if (rpc_lvb->lvb_size > lvb->lvb_size || !increase_only) {
			CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb size: "
			       "%llu -> %llu\n", PFID(fid),
			       lvb->lvb_size, rpc_lvb->lvb_size);
			lvb->lvb_size = rpc_lvb->lvb_size;
		}
		if (rpc_lvb->lvb_mtime > lvb->lvb_mtime || !increase_only) {
			CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb mtime: "
			       "%llu -> %llu\n", PFID(fid),
			       lvb->lvb_mtime, rpc_lvb->lvb_mtime);
			lvb->lvb_mtime = rpc_lvb->lvb_mtime;
		}
		if (rpc_lvb->lvb_atime > lvb->lvb_atime || !increase_only) {
			CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb atime: "
			       "%llu -> %llu\n", PFID(fid),
			       lvb->lvb_atime, rpc_lvb->lvb_atime);
			lvb->lvb_atime = rpc_lvb->lvb_atime;
		}
		if (rpc_lvb->lvb_ctime > lvb->lvb_ctime || !increase_only) {
			CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb ctime: "
			       "%llu -> %llu\n", PFID(fid),
			       lvb->lvb_ctime, rpc_lvb->lvb_ctime);
			lvb->lvb_ctime = rpc_lvb->lvb_ctime;
		}
		if (rpc_lvb->lvb_blocks > lvb->lvb_blocks || !increase_only) {
			CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb blocks: "
			       "%llu -> %llu\n", PFID(fid),
			       lvb->lvb_blocks, rpc_lvb->lvb_blocks);
			lvb->lvb_blocks = rpc_lvb->lvb_blocks;
		}
		unlock_res(res);
	}

disk_update:
	/* Update the LVB from the disk inode */
	mo = mdt_object_find(&env, mdt, fid);
	if (IS_ERR(mo))
		GOTO(out_env, rc = PTR_ERR(mo));

	if (!mdt_object_exists(mo) || mdt_object_remote(mo))
		GOTO(out, rc = -ENOENT);

	ma = &info->mti_attr;
	ma->ma_valid = 0;
	ma->ma_need = MA_INODE;
	rc = mo_attr_get(&env, mdt_object_child(mo), ma);
	if (rc)
		GOTO(out, rc);

	lock_res(res);
	if (ma->ma_attr.la_size > lvb->lvb_size || !increase_only) {
		CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb size from disk: "
		       "%llu -> %llu\n", PFID(fid),
		       lvb->lvb_size, ma->ma_attr.la_size);
		lvb->lvb_size = ma->ma_attr.la_size;
	}

	if (ma->ma_attr.la_mtime > lvb->lvb_mtime || !increase_only) {
		CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb mtime from disk: "
		       "%llu -> %llu\n", PFID(fid),
		       lvb->lvb_mtime, ma->ma_attr.la_mtime);
		lvb->lvb_mtime = ma->ma_attr.la_mtime;
	}
	if (ma->ma_attr.la_atime > lvb->lvb_atime || !increase_only) {
		CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb atime from disk: "
		       "%llu -> %llu\n", PFID(fid),
		       lvb->lvb_atime, ma->ma_attr.la_atime);
		lvb->lvb_atime = ma->ma_attr.la_atime;
	}
	if (ma->ma_attr.la_ctime > lvb->lvb_ctime || !increase_only) {
		CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb ctime from disk: "
		       "%llu -> %llu\n", PFID(fid),
		       lvb->lvb_ctime, ma->ma_attr.la_ctime);
		lvb->lvb_ctime = ma->ma_attr.la_ctime;
	}
	if (ma->ma_attr.la_blocks > lvb->lvb_blocks || !increase_only) {
		CDEBUG(D_DLMTRACE, "res: "DFID" updating lvb blocks from disk: "
		       "%llu -> %llu\n", PFID(fid), lvb->lvb_blocks,
		       (unsigned long long)ma->ma_attr.la_blocks);
		lvb->lvb_blocks = ma->ma_attr.la_blocks;
	}
	unlock_res(res);

out:
	mdt_object_put(&env, mo);
out_env:
	lu_env_fini(&env);
	return rc;
}

static int mdt_lvbo_update(struct ldlm_resource *res, struct ldlm_lock *lock,
			   struct ptlrpc_request *req, int increase_only)
{
	ENTRY;

	if (IS_LQUOTA_RES(res)) {
		struct mdt_device	*mdt;

		mdt = ldlm_res_to_ns(res)->ns_lvbp;
		if (mdt->mdt_qmt_dev == NULL)
			return 0;

		/* call lvbo update function of quota master */
		return qmt_hdls.qmth_lvbo_update(mdt->mdt_qmt_dev, res, req,
						 increase_only);
	}

	/* Data-on-MDT lvbo update.
	 * Like a ldlm_lock_init() the lock can be skipped and that means
	 * it is DOM resource because lvbo_update() without lock is called
	 * by MDT for DOM objects only.
	 */
	if (lock == NULL || ldlm_has_dom(lock))
		return mdt_dom_lvbo_update(res, lock, req, increase_only);
	return 0;
}


static int mdt_lvbo_size(struct ldlm_lock *lock)
{
	struct mdt_device *mdt;

	/* resource on server side never changes. */
	mdt = ldlm_res_to_ns(lock->l_resource)->ns_lvbp;
	LASSERT(mdt != NULL);

	if (IS_LQUOTA_RES(lock->l_resource)) {
		if (mdt->mdt_qmt_dev == NULL)
			return 0;

		/* call lvbo size function of quota master */
		return qmt_hdls.qmth_lvbo_size(mdt->mdt_qmt_dev, lock);
	}

	if (ldlm_has_dom(lock))
		return sizeof(struct ost_lvb);

	if (ldlm_has_layout(lock))
		return mdt->mdt_max_mdsize;

	return 0;
}

static int mdt_lvbo_fill(struct ldlm_lock *lock, void *lvb, int lvblen)
{
	struct lu_env env;
	struct mdt_thread_info *info;
	struct mdt_device *mdt;
	struct lu_fid *fid;
	struct mdt_object *obj = NULL;
	struct md_object *child = NULL;
	int rc;
	ENTRY;

	mdt = ldlm_lock_to_ns(lock)->ns_lvbp;
	if (IS_LQUOTA_RES(lock->l_resource)) {
		if (mdt->mdt_qmt_dev == NULL)
			RETURN(0);

		/* call lvbo fill function of quota master */
		rc = qmt_hdls.qmth_lvbo_fill(mdt->mdt_qmt_dev, lock, lvb,
					     lvblen);
		RETURN(rc);
	}

	if (ldlm_has_dom(lock)) {
		struct ldlm_resource *res = lock->l_resource;
		int lvb_len = sizeof(struct ost_lvb);

		if (!mdt_dom_lvb_is_valid(res))
			mdt_dom_lvbo_update(lock->l_resource, lock, NULL, 0);

		if (lvb_len > lvblen)
			lvb_len = lvblen;

		lock_res(res);
		memcpy(lvb, res->lr_lvb_data, lvb_len);
		unlock_res(res);

		RETURN(lvb_len);
	}

	/* Only fill layout if layout lock is granted */
	if (!ldlm_has_layout(lock) || lock->l_granted_mode != lock->l_req_mode)
		RETURN(0);

	/* XXX create an env to talk to mdt stack. We should get this env from
	 * ptlrpc_thread->t_env. */
	rc = lu_env_init(&env, LCT_MD_THREAD);
	/* Likely ENOMEM */
	if (rc)
		RETURN(rc);

	info = lu_context_key_get(&env.le_ctx, &mdt_thread_key);
	/* Likely ENOMEM */
	if (info == NULL)
		GOTO(out, rc = -ENOMEM);

	memset(info, 0, sizeof *info);
	info->mti_env = &env;
	info->mti_exp = lock->l_export;
	info->mti_mdt = mdt;

	/* XXX get fid by resource id. why don't include fid in ldlm_resource */
	fid = &info->mti_tmp_fid2;
	fid_extract_from_res_name(fid, &lock->l_resource->lr_name);

	obj = mdt_object_find(&env, info->mti_mdt, fid);
	if (IS_ERR(obj))
		GOTO(out, rc = PTR_ERR(obj));

	if (!mdt_object_exists(obj) || mdt_object_remote(obj))
		GOTO(out, rc = -ENOENT);

	child = mdt_object_child(obj);

	/* get the length of lsm */
	rc = mo_xattr_get(&env, child, &LU_BUF_NULL, XATTR_NAME_LOV);
	if (rc < 0)
		GOTO(out, rc);
	if (rc > 0) {
		struct lu_buf *lmm = NULL;
		if (lvblen < rc) {
			CERROR("%s: expected %d actual %d.\n",
			       mdt_obd_name(mdt), rc, lvblen);
			GOTO(out, rc = -ERANGE);
		}
		lmm = &info->mti_buf;
		lmm->lb_buf = lvb;
		lmm->lb_len = rc;
		rc = mo_xattr_get(&env, child, lmm, XATTR_NAME_LOV);
		if (rc < 0)
			GOTO(out, rc);
	}

out:
	if (obj != NULL && !IS_ERR(obj))
		mdt_object_put(&env, obj);
	lu_env_fini(&env);
	RETURN(rc < 0 ? 0 : rc);
}

static int mdt_lvbo_free(struct ldlm_resource *res)
{
	if (IS_LQUOTA_RES(res)) {
		struct mdt_device	*mdt;

		mdt = ldlm_res_to_ns(res)->ns_lvbp;
		if (mdt->mdt_qmt_dev == NULL)
			return 0;

		/* call lvbo free function of quota master */
		return qmt_hdls.qmth_lvbo_free(mdt->mdt_qmt_dev, res);
	}

	/* Data-on-MDT lvbo free */
	if (res->lr_lvb_data != NULL)
		OBD_FREE(res->lr_lvb_data, res->lr_lvb_len);
	return 0;
}

struct ldlm_valblock_ops mdt_lvbo = {
	lvbo_init:	mdt_lvbo_init,
	lvbo_update:	mdt_lvbo_update,
	lvbo_size:	mdt_lvbo_size,
	lvbo_fill:	mdt_lvbo_fill,
	lvbo_free:	mdt_lvbo_free
};
