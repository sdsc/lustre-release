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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2014 Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdt/mdt_io.c
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_FILTER

#include <dt_object.h>
#include "mdt_internal.h"

#define VALID_FLAGS (LA_TYPE | LA_MODE | LA_SIZE | LA_BLOCKS | \
		     LA_BLKSIZE | LA_ATIME | LA_MTIME | LA_CTIME)

/* TODO: functions below are stubs for now, they will be implemented with
 * grant support on MDT */
static inline void mdt_io_counter_incr(struct obd_export *exp, int opcode,
				       char *jobid, long amount)
{
	return;
}

static int mdt_preprw_read(const struct lu_env *env, struct obd_export *exp,
			   struct mdt_device *mdt, struct lu_fid *fid,
			   struct lu_attr *la, int niocount,
			   struct niobuf_remote *rnb, int *nr_local,
			   struct niobuf_local *lnb, char *jobid)
{
	struct dt_device	*dt = mdt->mdt_bottom;
	struct dt_object	*mo;
	int			 i, j, rc, tot_bytes = 0;

	ENTRY;
	LASSERT(env != NULL);

	mo = dt_locate(env, dt, fid);
	if (IS_ERR(mo))
		RETURN(PTR_ERR(mo));
	LASSERT(mo != NULL);

	dt_read_lock(env, mo, 0);
	if (!dt_object_exists(mo))
		GOTO(unlock, rc = -ENOENT);

	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < niocount; i++) {
		rc = dt_bufs_get(env, mo, rnb + i, lnb + j, 0);
		if (unlikely(rc < 0))
			GOTO(buf_put, rc);
		LASSERT(rc <= MD_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		j += rc;
		*nr_local += rc;
		LASSERT(j <= MD_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}

	LASSERT(*nr_local > 0 && *nr_local <= MD_MAX_BRW_PAGES);
	rc = dt_attr_get(env, mo, la);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	rc = dt_read_prep(env, mo, lnb, *nr_local);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	mdt_io_counter_incr(exp, LPROC_MDT_IO_READ, jobid, tot_bytes);
	RETURN(0);

buf_put:
	dt_bufs_put(env, mo, lnb, *nr_local);
unlock:
	dt_read_unlock(env, mo);
	lu_object_put(env, &mo->do_lu);
	return rc;
}

static int mdt_preprw_write(const struct lu_env *env, struct obd_export *exp,
			    struct mdt_device *mdt, struct lu_fid *fid,
			    struct lu_attr *la, struct obdo *oa,
			    int objcount, struct obd_ioobj *obj,
			    struct niobuf_remote *rnb, int *nr_local,
			    struct niobuf_local *lnb, char *jobid)
{
	struct dt_device	*dt = mdt->mdt_bottom;
	struct dt_object	*mo;
	int			 i, j, k, rc = 0, tot_bytes = 0;

	ENTRY;
	LASSERT(env != NULL);
	LASSERT(objcount == 1);

	mo = dt_locate(env, dt, fid);
	if (IS_ERR(mo))
		GOTO(out, rc = PTR_ERR(mo));
	LASSERT(mo != NULL);

	dt_read_lock(env, mo, 0);
	if (!dt_object_exists(mo)) {
		CERROR("%s: BRW to missing obj "DFID"\n",
		       exp->exp_obd->obd_name, PFID(fid));
		dt_read_unlock(env, mo);
		lu_object_put(env, &mo->do_lu);
		GOTO(out, rc = -ENOENT);
	}

	/* Process incoming grant info, set OBD_BRW_GRANTED flag and grant some
	 * space back if possible */
	tgt_grant_prepare_write(env, exp, oa, rnb, obj->ioo_bufcnt);

	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < obj->ioo_bufcnt; i++) {
		rc = dt_bufs_get(env, mo, rnb + i, lnb + j, 1);
		if (unlikely(rc < 0))
			GOTO(err, rc);
		LASSERT(rc <= MD_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		for (k = 0; k < rc; k++) {
			lnb[j + k].lnb_flags = rnb[i].rnb_flags;
			if (!(rnb[i].rnb_flags & OBD_BRW_GRANTED))
				lnb[j + k].lnb_rc = -ENOSPC;
		}
		j += rc;
		*nr_local += rc;
		LASSERT(j <= MD_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}
	LASSERT(*nr_local > 0 && *nr_local <= MD_MAX_BRW_PAGES);

	rc = dt_write_prep(env, mo, lnb, *nr_local);
	if (unlikely(rc != 0))
		GOTO(err, rc);

	mdt_io_counter_incr(exp, LPROC_MDT_IO_WRITE, jobid, tot_bytes);
	RETURN(0);
err:
	dt_bufs_put(env, mo, lnb, *nr_local);
	dt_read_unlock(env, mo);
	lu_object_put(env, &mo->do_lu);
	/* tgt_grant_prepare_write() was called, so we must commit */
	tgt_grant_commit(exp, oa->o_grant_used, rc);
out:
	/* let's still process incoming grant information packed in the oa,
	 * but without enforcing grant since we won't proceed with the write.
	 * Just like a read request actually. */
	tgt_grant_prepare_read(env, exp, oa);
	return rc;
}

int mdt_obd_preprw(const struct lu_env *env, int cmd, struct obd_export *exp,
		   struct obdo *oa, int objcount, struct obd_ioobj *obj,
		   struct niobuf_remote *rnb, int *nr_local,
		   struct niobuf_local *lnb)
{
	struct tgt_session_info	*tsi = tgt_ses_info(env);
	struct mdt_thread_info	*info = tsi2mdt_info(tsi);
	struct lu_attr		*la = &info->mti_attr.ma_attr;
	struct mdt_device	*mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	char			*jobid;
	int			 rc = 0;

	/* The default value PTLRPC_MAX_BRW_PAGES is set in tgt_brw_write()
	 * but for MDT it is different, correct it here. */
	if (*nr_local > MD_MAX_BRW_PAGES)
		*nr_local = MD_MAX_BRW_PAGES;

	info = tsi2mdt_info(tsi);
	jobid = tsi->tsi_jobid;

	LASSERT(oa != NULL);
	LASSERT(objcount == 1);
	LASSERT(obj->ioo_bufcnt > 0);

	if (cmd == OBD_BRW_WRITE) {
		la_from_obdo(la, oa, OBD_MD_FLGETATTR);
		rc = mdt_preprw_write(env, exp, mdt, &tsi->tsi_fid, la, oa,
				      objcount, obj, rnb, nr_local, lnb,
				      jobid);
	} else if (cmd == OBD_BRW_READ) {
		tgt_grant_prepare_read(env, exp, oa);
		rc = mdt_preprw_read(env, exp, mdt, &tsi->tsi_fid, la,
				     obj->ioo_bufcnt, rnb, nr_local, lnb,
				     jobid);
		obdo_from_la(oa, la, LA_ATIME);
	} else {
		CERROR("%s: wrong cmd %d received!\n",
		       exp->exp_obd->obd_name, cmd);
		rc = -EPROTO;
	}
	RETURN(rc);
}

static int mdt_commitrw_read(const struct lu_env *env, struct mdt_device *mdt,
			     struct lu_fid *fid, int objcount, int niocount,
			     struct niobuf_local *lnb)
{
	struct dt_device	*dt = mdt->mdt_bottom;
	struct dt_object	*mo;

	ENTRY;

	LASSERT(niocount > 0);

	mo = dt_locate(env, dt, fid);
	if (IS_ERR(mo))
		RETURN(PTR_ERR(mo));
	LASSERT(mo != NULL);
	LASSERT(dt_object_exists(mo));
	dt_bufs_put(env, mo, lnb, niocount);

	dt_read_unlock(env, mo);
	lu_object_put(env, &mo->do_lu);
	/* second put is pair to object_get in ofd_preprw_read */
	lu_object_put(env, &mo->do_lu);

	RETURN(0);
}

static int mdt_commitrw_write(const struct lu_env *env, struct obd_export *exp,
			      struct mdt_device *mdt, struct lu_fid *fid,
			      struct lu_attr *la, int objcount, int niocount,
			      struct niobuf_local *lnb, unsigned long granted,
			      int old_rc)
{
	struct dt_device	*dt = mdt->mdt_bottom;
	struct dt_object	*mo;
	struct thandle		*th;
	int			 rc = 0;
	int			 retries = 0;
	int			 i;

	ENTRY;

	LASSERT(objcount == 1);

	mo = dt_locate(env, dt, fid);
	LASSERT(mo != NULL);
	LASSERT(dt_object_exists(mo));

	if (old_rc)
		GOTO(out, rc = old_rc);

	la->la_valid &= LA_ATIME | LA_MTIME | LA_CTIME;

retry:
	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	for (i = 0; i < niocount; i++) {
		if (!(lnb[i].lnb_flags & OBD_BRW_ASYNC)) {
			th->th_sync = 1;
			break;
		}
	}

	if (OBD_FAIL_CHECK(OBD_FAIL_OST_DQACQ_NET))
		GOTO(out_stop, rc = -EINPROGRESS);

	rc = dt_declare_write_commit(env, mo, lnb, niocount, th);
	if (rc)
		GOTO(out_stop, rc);

	if (la->la_valid) {
		/* update [mac]time if needed */
		rc = dt_declare_attr_set(env, mo, la, th);
		if (rc)
			GOTO(out_stop, rc);
	}

	rc = dt_trans_start(env, dt, th);
	if (rc)
		GOTO(out_stop, rc);

	rc = dt_write_commit(env, mo, lnb, niocount, th);
	if (rc)
		GOTO(out_stop, rc);

	if (la->la_valid) {
		rc = dt_attr_set(env, mo, la, th);
		if (rc)
			GOTO(out_stop, rc);
	}

	/* get attr to return */
	rc = dt_attr_get(env, mo, la);

out_stop:
	/* Force commit to make the just-deleted blocks
	 * reusable. LU-456 */
	if (rc == -ENOSPC)
		th->th_sync = 1;


	if (rc == 0 && granted > 0) {
		if (tgt_grant_commit_cb_add(th, exp, granted) == 0)
			granted = 0;
	}

	th->th_result = rc;
	dt_trans_stop(env, dt, th);
	if (rc == -ENOSPC && retries++ < 3) {
		CDEBUG(D_INODE, "retry after force commit, retries:%d\n",
		       retries);
		goto retry;
	}

out:
	dt_bufs_put(env, mo, lnb, niocount);
	dt_read_unlock(env, mo);
	lu_object_put(env, &mo->do_lu);
	/* second put is pair to object_get in ofd_preprw_write */
	lu_object_put(env, &mo->do_lu);
	if (granted > 0)
		tgt_grant_commit(exp, granted, old_rc);
	RETURN(rc);
}

int mdt_obd_commitrw(const struct lu_env *env, int cmd, struct obd_export *exp,
		     struct obdo *oa, int objcount, struct obd_ioobj *obj,
		     struct niobuf_remote *rnb, int npages,
		     struct niobuf_local *lnb, int old_rc)
{
	struct tgt_session_info	*tsi = tgt_ses_info(env);
	struct mdt_thread_info	*info = mdt_th_info(env);
	struct mdt_device	*mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	struct lu_attr		*la = &info->mti_attr.ma_attr;
	__u64			 valid;
	int			 rc = 0;

	LASSERT(npages > 0);

	if (cmd == OBD_BRW_WRITE) {
		/* Don't update timestamps if this write is older than a
		 * setattr which modifies the timestamps. b=10150 */

		/* XXX when we start having persistent reservations this needs
		 * to be changed to ofd_fmd_get() to create the fmd if it
		 * doesn't already exist so we can store the reservation handle
		 * there. */
		valid = OBD_MD_FLUID | OBD_MD_FLGID;
		valid |= OBD_MD_FLATIME | OBD_MD_FLMTIME | OBD_MD_FLCTIME;

		la_from_obdo(la, oa, valid);

		rc = mdt_commitrw_write(env, exp, mdt, &tsi->tsi_fid,
					la, objcount, npages,
					lnb, oa->o_grant_used, old_rc);
		if (rc == 0)
			obdo_from_la(oa, la, VALID_FLAGS | LA_GID | LA_UID);
		else
			obdo_from_la(oa, la, LA_GID | LA_UID);

		/* don't report overquota flag if we failed before reaching
		 * commit */
		if (old_rc == 0 && (rc == 0 || rc == -EDQUOT)) {
			/* return the overquota flags to client */
			if (lnb[0].lnb_flags & OBD_BRW_OVER_USRQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_USRQUOTA;
				else
					oa->o_flags = OBD_FL_NO_USRQUOTA;
			}

			if (lnb[0].lnb_flags & OBD_BRW_OVER_GRPQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_GRPQUOTA;
				else
					oa->o_flags = OBD_FL_NO_GRPQUOTA;
			}

			oa->o_valid |= OBD_MD_FLFLAGS | OBD_MD_FLUSRQUOTA |
				       OBD_MD_FLGRPQUOTA;
		}
	} else if (cmd == OBD_BRW_READ) {
		struct ldlm_namespace *ns = mdt->mdt_namespace;

		/* If oa != NULL then mdt_preprw_read updated the inode
		 * atime and we should update the lvb so that other glimpses
		 * will also get the updated value. bug 5972 */
		if (oa && ns && ns->ns_lvbo && ns->ns_lvbo->lvbo_update) {
			struct ldlm_resource *rs = NULL;

			rs = ldlm_resource_get(ns, NULL, &tsi->tsi_resid,
					       LDLM_IBITS, 0);
			if (!IS_ERR(rs)) {
				ldlm_res_lvbo_update(rs, NULL, 1);
				ldlm_resource_putref(rs);
			}
		}
		rc = mdt_commitrw_read(env, mdt, &tsi->tsi_fid, objcount,
				       npages, lnb);
		if (old_rc)
			rc = old_rc;
	} else {
		LBUG();
		rc = -EPROTO;
	}
	RETURN(rc);
}

int mdt_object_punch(const struct lu_env *env, struct dt_device *dt,
		     struct dt_object *dob, __u64 start, __u64 end,
		     struct lu_attr *la)
{
	struct thandle		*th;
	int			 rc;

	ENTRY;

	/* we support truncate, not punch yet */
	LASSERT(end == OBD_OBJECT_EOF);

	dt_write_lock(env, dob, 0);
	if (!dt_object_exists(dob))
		GOTO(unlock, rc = -ENOENT);

#if 0
	/* VBR: version recovery check */
	rc = mdt_version_get_check(info, dob, 0);
	if (rc)
		GOTO(unlock, rc);
#endif
	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		GOTO(unlock, rc = PTR_ERR(th));

	rc = dt_declare_attr_set(env, dob, la, th);
	if (rc)
		GOTO(stop, rc);

	rc = dt_declare_punch(env, dob, start, OBD_OBJECT_EOF, th);
	if (rc)
		GOTO(stop, rc);

	tgt_vbr_obj_set(env, dob);
	rc = dt_trans_start(env, dt, th);
	if (rc)
		GOTO(stop, rc);

	rc = dt_punch(env, dob, start, OBD_OBJECT_EOF, th);
	if (rc)
		GOTO(stop, rc);

	rc = dt_attr_set(env, dob, la, th);
	if (rc)
		GOTO(stop, rc);

stop:
	th->th_result = rc;
	dt_trans_stop(env, dt, th);
unlock:
	dt_write_unlock(env, dob);
	RETURN(rc);
}

int mdt_punch_hdl(struct tgt_session_info *tsi)
{
	const struct obdo	*oa = &tsi->tsi_ost_body->oa;
	struct ost_body		*repbody;
	struct mdt_thread_info	*info = tsi2mdt_info(tsi);
	struct lu_attr		*la = &info->mti_attr.ma_attr;
	struct ldlm_namespace	*ns = tsi->tsi_tgt->lut_obd->obd_namespace;
	struct obd_export	*exp = tsi->tsi_exp;
	struct mdt_device	*mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	struct dt_object	*dob;
	__u64			 flags = 0;
	struct lustre_handle	 lh = { 0, };
	int			 rc;
	__u64			 start, end;
	bool			 srvlock;

	ENTRY;

	/* check that we do support OBD_CONNECT_TRUNCLOCK. */
	CLASSERT(OST_CONNECT_SUPPORTED & OBD_CONNECT_TRUNCLOCK);

	if ((oa->o_valid & (OBD_MD_FLSIZE | OBD_MD_FLBLOCKS)) !=
	    (OBD_MD_FLSIZE | OBD_MD_FLBLOCKS))
		RETURN(err_serious(-EPROTO));

	repbody = req_capsule_server_get(tsi->tsi_pill, &RMF_OST_BODY);
	if (repbody == NULL)
		RETURN(err_serious(-ENOMEM));

	/* punch start,end are passed in o_size,o_blocks throught wire */
	start = oa->o_size;
	end = oa->o_blocks;

	if (end != OBD_OBJECT_EOF) /* Only truncate is supported */
		RETURN(-EPROTO);

	/* standard truncate optimization: if file body is completely
	 * destroyed, don't send data back to the server. */
	if (start == 0)
		flags |= LDLM_FL_AST_DISCARD_DATA;

	repbody->oa.o_oi = oa->o_oi;
	repbody->oa.o_valid = OBD_MD_FLID;

	srvlock = (exp_connect_flags(exp) & OBD_CONNECT_SRVLOCK) &&
		  oa->o_valid & OBD_MD_FLFLAGS &&
		  oa->o_flags & OBD_FL_SRVLOCK;

	if (srvlock) {
		rc = tgt_mdt_extent_lock(ns, &tsi->tsi_resid, start, end, &lh,
					 LCK_PW, &flags);
		if (rc != 0)
			RETURN(rc);
	}

	CDEBUG(D_INODE, "calling punch for object "DFID", valid = %#llx"
	       ", start = %lld, end = %lld\n", PFID(&tsi->tsi_fid),
	       oa->o_valid, start, end);

	dob = dt_locate(tsi->tsi_env, mdt->mdt_bottom, &tsi->tsi_fid);
	if (IS_ERR(dob))
		GOTO(out, rc = PTR_ERR(dob));

	if (!dt_object_exists(dob))
		GOTO(out_put, rc = -ENOENT);

	la_from_obdo(la, oa, OBD_MD_FLMTIME | OBD_MD_FLATIME | OBD_MD_FLCTIME);
	la->la_size = start;
	la->la_valid |= LA_SIZE;

	rc = mdt_object_punch(tsi->tsi_env, mdt->mdt_bottom, dob,
			      start, end, la);
	if (rc)
		GOTO(out_put, rc);

	mdt_io_counter_incr(tsi->tsi_exp, LPROC_MDT_IO_PUNCH,
			    tsi->tsi_jobid, 1);
	EXIT;
out_put:
	lu_object_put(tsi->tsi_env, &dob->do_lu);
out:
	if (srvlock)
		tgt_extent_unlock(&lh, LCK_PW);
	if (rc == 0) {
		struct ldlm_resource *res;

		/* we do not call this before to avoid lu_object_find() in
		 *  ->lvbo_update() holding another reference on the object.
		 * otherwise concurrent destroy can make the object unavailable
		 * for 2nd lu_object_find() waiting for the first reference
		 * to go... deadlock! */
		res = ldlm_resource_get(ns, NULL, &tsi->tsi_resid,
					LDLM_IBITS, 0);
		if (!IS_ERR(res)) {
			ldlm_res_lvbo_update(res, NULL, 0);
			ldlm_resource_putref(res);
		}
	}
	return rc;
}

/**
 * MDT glimpse for Data-on-MDT
 *
 * If there is write lock on client then function issues glimpse_ast to get
 * an actual size from that client.
 *
 */
int mdt_do_glimpse(const struct lu_env *env, struct ldlm_namespace *ns,
		   struct ldlm_resource *res)
{
	struct tgt_session_info *tsi = tgt_ses_info(env);
	union ldlm_policy_data policy;
	struct lustre_handle lockh;
	enum ldlm_mode mode;
	struct ldlm_lock *lock;
	struct ldlm_glimpse_work gl_work;
	struct list_head gl_list;
	int rc;

	ENTRY;

	/* There can be only one lock covering data, try to match it. */
	policy.l_inodebits.bits = MDS_INODELOCK_DOM;
	mode = ldlm_lock_match(ns, LDLM_FL_BLOCK_GRANTED | LDLM_FL_TEST_LOCK,
			       &res->lr_name, LDLM_IBITS, &policy,
			       LCK_PW | LCK_EX, &lockh, 0);

	/* There is no other PW lock on this object; finished. */
	if (mode == 0)
		RETURN(0);

	lock = ldlm_handle2lock(&lockh);
	if (lock == NULL)
		RETURN(0);

	/* do not glimpse for the same client, may happen due to local
	 * glimpse, see mdt_dom_object_size()
	 */
	if (lock->l_export->exp_handle.h_cookie ==
	    tsi->tsi_exp->exp_handle.h_cookie)
		GOTO(out, rc = -EINVAL);

	/*
	 * This check is for lock taken in mdt_reint_unlink() that does
	 * not have l_glimpse_ast set. So the logic is: if there is a lock
	 * with no l_glimpse_ast set, this object is being destroyed already.
	 * Hence, if you are grabbing DLM locks on the server, always set
	 * non-NULL glimpse_ast (e.g., ldlm_request.c::ldlm_glimpse_ast()).
	 */
	if (lock->l_glimpse_ast == NULL)
		GOTO(out, rc = -ENOENT);

	/* Populate the gl_work structure.
	 * Grab additional reference on the lock which will be released in
	 * ldlm_work_gl_ast_lock() */
	gl_work.gl_lock = LDLM_LOCK_GET(lock);
	/* The glimpse callback is sent to one single IO lock. As a result,
	 * the gl_work list is just composed of one element */
	INIT_LIST_HEAD(&gl_list);
	list_add_tail(&gl_work.gl_list, &gl_list);
	/* There is actually no need for a glimpse descriptor when glimpsing
	 * IO locks */
	gl_work.gl_desc = NULL;
	/* the ldlm_glimpse_work structure is allocated on the stack */
	gl_work.gl_flags = LDLM_GL_WORK_NOFREE;

	ldlm_glimpse_locks(res, &gl_list); /* this will update the LVB */

	if (!list_empty(&gl_list))
		LDLM_LOCK_RELEASE(lock);
	rc = 0;
	EXIT;
out:
	LDLM_LOCK_PUT(lock);
	return rc;
}

/**
 * MDT glimpse for Data-on-MDT
 *
 * This function is called when MDT get attributes for the DoM object.
 * If there is write lock on client then function issues glimpse_ast to get
 * an actual size from that client.
 */
int mdt_dom_object_size(const struct lu_env *env, struct mdt_device *mdt,
			const struct lu_fid *fid, struct mdt_body *mb,
			struct lustre_handle *lh)
{
	struct ldlm_res_id resid;
	struct ldlm_resource *res;
	struct ost_lvb *res_lvb;
	bool dom_lock = false;
	int rc = 0;

	ENTRY;

	if (lustre_handle_is_used(lh)) {
		struct ldlm_lock *lock;

		lock = ldlm_handle2lock(lh);
		if (lock != NULL) {
			dom_lock = ldlm_has_dom(lock);
			LDLM_LOCK_PUT(lock);
		}
	}

	fid_build_reg_res_name(fid, &resid);
	res = ldlm_resource_get(mdt->mdt_namespace, NULL, &resid,
				LDLM_IBITS, 1);
	if (IS_ERR(res))
		RETURN(-ENOENT);

	/* if there is no DOM bit in the lock then glimpse is needed
	 * to return valid size */
	if (!dom_lock) {
		rc = mdt_do_glimpse(env, mdt->mdt_namespace, res);
		if (rc < 0)
			GOTO(out, rc);
	}

	/* Update lvbo data if DoM lock returned or if LVB is not yet valid. */
	if (dom_lock || !mdt_dom_lvb_is_valid(res))
		ldlm_res_lvbo_update(res, NULL, 0);

	res_lvb = res->lr_lvb_data;
	LASSERT(res_lvb);

	lock_res(res);
	mb->mbo_size = res_lvb->lvb_size;
	mb->mbo_blocks = res_lvb->lvb_blocks;

	if (mb->mbo_mtime < res_lvb->lvb_mtime)
		mb->mbo_mtime = res_lvb->lvb_mtime;

	if (mb->mbo_ctime < res_lvb->lvb_ctime)
		mb->mbo_ctime = res_lvb->lvb_ctime;

	if (mb->mbo_atime < res_lvb->lvb_atime)
		mb->mbo_atime = res_lvb->lvb_atime;

	mb->mbo_valid |= OBD_MD_FLATIME | OBD_MD_FLCTIME | OBD_MD_FLMTIME |
			 OBD_MD_FLSIZE | OBD_MD_FLBLOCKS | OBD_MD_DOM_SIZE;
	unlock_res(res);
out:
	ldlm_resource_putref(res);
	RETURN(0);
}

/**
 * MDT DoM lock intent policy (glimpse)
 *
 * Intent policy is called when lock has an intent, for DoM file that
 * means glimpse lock and policy fills Lock Value Block (LVB).
 *
 * If already granted lock is found it will be placed in \a lockp and
 * returned back to caller function.
 *
 * \param[in] tsi	 session info
 * \param[in,out] lockp	 pointer to the lock
 * \param[in] flags	 LDLM flags
 *
 * \retval		ELDLM_LOCK_REPLACED if already granted lock was found
 *			and placed in \a lockp
 * \retval		ELDLM_LOCK_ABORTED in other cases except error
 * \retval		negative value on error
 */
int mdt_glimpse_enqueue(struct tgt_session_info *tsi, struct ldlm_namespace *ns,
			struct ldlm_lock **lockp, __u64 flags)
{
	struct ldlm_lock	*lock = *lockp;
	struct ldlm_resource	*res = lock->l_resource;
	ldlm_processing_policy	 policy;
	struct ost_lvb		*res_lvb, *reply_lvb;
	struct ldlm_reply	*rep;
	int rc;

	ENTRY;

	policy = ldlm_get_processing_policy(res);
	LASSERT(policy != NULL);

	lock->l_lvb_type = LVB_T_OST;
	req_capsule_set_size(tsi->tsi_pill, &RMF_DLM_LVB, RCL_SERVER,
			     sizeof(*reply_lvb));
	rc = req_capsule_server_pack(tsi->tsi_pill);
	if (rc)
		RETURN(err_serious(rc));

	rep = req_capsule_server_get(tsi->tsi_pill, &RMF_DLM_REP);
	if (rep == NULL)
		RETURN(-EPROTO);

	reply_lvb = req_capsule_server_get(tsi->tsi_pill, &RMF_DLM_LVB);
	if (reply_lvb == NULL)
		RETURN(-EPROTO);

	lock_res(res);
	/* Check if this is a resend case (MSG_RESENT is set on RPC) and a
	 * lock was found by ldlm_handle_enqueue(); if so no need to grant
	 * it again. */
	if (flags & LDLM_FL_RESENT) {
		rc = LDLM_ITER_CONTINUE;
	} else {
		__u64 tmpflags = 0;
		enum ldlm_error err;

		rc = policy(lock, &tmpflags, 0, &err, NULL);
		check_res_locked(res);
	}
	unlock_res(res);

	/* The lock met with no resistance; we're finished. */
	if (rc == LDLM_ITER_CONTINUE) {
		RETURN(ELDLM_LOCK_REPLACED);
	} else if (flags & LDLM_FL_BLOCK_NOWAIT) {
		/* LDLM_FL_BLOCK_NOWAIT means it is for AGL. Do not send glimpse
		 * callback for glimpse size. The real size user will trigger
		 * the glimpse callback when necessary. */
		RETURN(ELDLM_LOCK_ABORTED);
	}

	rc = mdt_do_glimpse(tsi->tsi_env, ns, res);
	if (rc == -ENOENT) {
		/* We are racing with unlink(); just return -ENOENT */
		rep->lock_policy_res1 = ptlrpc_status_hton(-ENOENT);
	} else if (rc == -EINVAL) {
		/* this is possible is client lock has been cancelled but
		 * still exists on server. If that lock was found on server
		 * as only conflicting lock then the client has already
		 * size authority and glimpse is not needed. */
		CDEBUG(D_DLMTRACE, "Glimpse from the client owning lock\n");
		RETURN(ELDLM_LOCK_REPLACED);
	}

	/* LVB can be without valid data in case of DOM */
	if (!mdt_dom_lvb_is_valid(res))
		ldlm_lvbo_update(res, lock, NULL, 0);

	res_lvb = res->lr_lvb_data;
	lock_res(res);
	*reply_lvb = *res_lvb;
	unlock_res(res);

	RETURN(ELDLM_LOCK_ABORTED);
}

int mdt_dom_discard_data(struct mdt_thread_info *info,
			 const struct lu_fid *fid)
{
	struct mdt_device *mdt = info->mti_mdt;
	union ldlm_policy_data *policy = &info->mti_policy;
	struct ldlm_res_id *res_id = &info->mti_res_id;
	struct lustre_handle dom_lh;
	__u64 flags = LDLM_FL_AST_DISCARD_DATA;
	int rc = 0;

	policy->l_inodebits.bits = MDS_INODELOCK_DOM;
	fid_build_reg_res_name(fid, res_id);

	/* Tell the clients that the object is gone now and that they should
	 * throw away any cached pages. */
	rc = ldlm_cli_enqueue_local(mdt->mdt_namespace, res_id, LDLM_IBITS,
				    policy, LCK_PW, &flags, ldlm_blocking_ast,
				    ldlm_completion_ast, NULL, NULL, 0,
				    LVB_T_NONE, NULL, &dom_lh);

	/* We only care about the side-effects, just drop the lock. */
	if (rc == ELDLM_OK)
		ldlm_lock_decref(&dom_lh, LCK_PW);
	return 0;
}

/* check if client has already DoM lock for given resource */
int mdt_dom_client_has_lock(struct mdt_thread_info *info,
			    const struct lu_fid *fid)
{
	struct mdt_device *mdt = info->mti_mdt;
	union ldlm_policy_data *policy = &info->mti_policy;
	struct ldlm_res_id *res_id = &info->mti_res_id;
	struct lustre_handle lockh;
	enum ldlm_mode mode;
	struct ldlm_lock *lock;
	int rc = 0;

	policy->l_inodebits.bits = MDS_INODELOCK_DOM;
	fid_build_reg_res_name(fid, res_id);

	mode = ldlm_lock_match(mdt->mdt_namespace, LDLM_FL_BLOCK_GRANTED |
			       LDLM_FL_TEST_LOCK, res_id, LDLM_IBITS, policy,
			       LCK_PW, &lockh, 0);

	/* There is no other PW lock on this object; finished. */
	if (mode == 0)
		return 0;

	lock = ldlm_handle2lock(&lockh);
	if (lock == 0)
		return 0;

	/* check if lock from the same client */
	if (lock->l_export->exp_handle.h_cookie ==
	    info->mti_exp->exp_handle.h_cookie)
		rc = 1;

	LDLM_LOCK_PUT(lock);
	return rc;
}

/* read file data to the buffer */
int mdt_dom_read_on_open(struct mdt_thread_info *mti, struct mdt_device *mdt,
			 struct lustre_handle *lh)
{
	const struct lu_env *env = mti->mti_env;
	struct tgt_session_info *tsi = tgt_ses_info(env);
	struct req_capsule *pill = tsi->tsi_pill;
	const struct lu_fid *fid;
	struct ptlrpc_request *req = tgt_ses_req(tsi);
	struct mdt_body *mbo;
	struct dt_device *dt = mdt->mdt_bottom;
	struct dt_object *mo;
	void *buf;
	struct niobuf_remote *rnb = NULL;
	struct niobuf_local *lnb;
	int rc;
	int max_reply_len;
	loff_t offset;
	unsigned int len, copied = 0;
	int lnbs, nr_local, i;

	bool dom_lock = false;

	ENTRY;

	rc = mdt_fix_reply(mti);
	if (rc < 0)
		RETURN(rc);

	if (!req_capsule_field_present(pill, &RMF_NIOBUF_INLINE, RCL_SERVER)) {
		/* There is no reply buffers for this field, this means that
		 * client has no support for data in reply.
		 */
		RETURN(0);
	}

	mbo = req_capsule_server_get(pill, &RMF_MDT_BODY);

	if (lustre_handle_is_used(lh)) {
		struct ldlm_lock *lock;

		lock = ldlm_handle2lock(lh);
		if (lock) {
			dom_lock = ldlm_has_dom(lock);
			LDLM_LOCK_PUT(lock);
		}
	}

	/* return data along with open only along with DoM lock */
	if (!dom_lock || !mdt->mdt_opts.mo_dom_read_open)
		RETURN(0);

	if (!(mbo->mbo_valid & OBD_MD_FLSIZE))
		RETURN(0);

	if (mbo->mbo_size == 0)
		RETURN(0);

	/* check the maximum size available in reply */
	max_reply_len = req->rq_rqbd->rqbd_svcpt->scp_service->srv_max_reply_size;

	CDEBUG(D_INFO, "File size %llu, reply size %d/%d/%d\n", mbo->mbo_size,
	       max_reply_len, req->rq_reqmsg->lm_repsize, req->rq_replen);
	len = req->rq_reqmsg->lm_repsize - req->rq_replen;
	max_reply_len -= req->rq_replen;

	/* NB: at this moment we have the following sizes:
	 * - req->rq_replen: used data in reply
	 * - req->rq_reqmsg->lm_repsize: total allocated reply buffer at client
	 * - max_reply_size: maximum reply size allowed by protocol
	 *
	 * Ideal case when file size fits in allocated reply buffer,
	 * that mean we can return whole data in reply. We can also fit more
	 * data up to max_reply_size in total reply size, but this will cause
	 * re-allocation on client and resend with larger buffer. This is still
	 * faster than separate READ IO.
	 * Third case if file is too big to fit even in maximum size, in that
	 * case we return just tail to optimize possible append.
	 *
	 * At the moment the following strategy is used:
	 * 1) try to fit into the buffer we have
	 * 2) TODO: introduce procfs value to allow buffer re-allocation to
	 *    fit more data with resend (always ON now).
	 * 3) return just file tail otherwise.
	 */
	if (mbo->mbo_size <= len) {
		/* can fit whole data */
		len = mbo->mbo_size;
		offset = 0;
	} else if (mbo->mbo_size <= max_reply_len) {
		/* TODO: make this tunable ON/OFF because this will cause
		 * buffer re-allocation and resend
		 */
		len = mbo->mbo_size;
		offset = 0;
	} else {
		int tail = mbo->mbo_size % PAGE_SIZE;

		/* no tail or tail can't fit in reply */
		if (tail == 0 || len < tail)
			RETURN(0);

		len = tail;
		offset = mbo->mbo_size - len;
	}
	LASSERT((offset % PAGE_SIZE) == 0);
	rc = req_capsule_server_grow(pill, &RMF_NIOBUF_INLINE,
				     sizeof(*rnb) + len);
	if (rc != 0) {
		/* failed to grow data buffer, just exit */
		GOTO(out, rc = -E2BIG);
	}

	/* re-take MDT_BODY buffer after the buffer growing above */
	mbo = req_capsule_server_get(pill, &RMF_MDT_BODY);
	fid = &mbo->mbo_fid1;
	if (!fid_is_sane(fid)) {
		CERROR("FID is not sane: "DFID"\n", PFID(fid));
		RETURN(0);
	}

	rnb = req_capsule_server_get(tsi->tsi_pill, &RMF_NIOBUF_INLINE);
	if (rnb == NULL)
		GOTO(out, rc = -EPROTO);
	buf = (char *)rnb + sizeof(*rnb);
	rnb->rnb_len = len;
	rnb->rnb_offset = offset;

	mo = dt_locate(env, dt, fid);
	if (IS_ERR(mo))
		GOTO(out, rc = PTR_ERR(mo));
	LASSERT(mo != NULL);

	dt_read_lock(env, mo, 0);
	if (!dt_object_exists(mo))
		GOTO(unlock, rc = -ENOENT);

	/* parse remote buffers to local buffers and prepare the latter */
	lnbs = (len >> PAGE_SHIFT) + 1;
	OBD_ALLOC(lnb, sizeof(*lnb) * lnbs);
	if (lnb == NULL)
		GOTO(unlock, rc = -ENOMEM);

	rc = dt_bufs_get(env, mo, rnb, lnb, 0);
	if (unlikely(rc < 0))
		GOTO(free, rc);
	LASSERT(rc <= lnbs);
	nr_local = rc;
	rc = dt_read_prep(env, mo, lnb, nr_local);
	if (unlikely(rc))
		GOTO(buf_put, rc);
	/* copy data finally to the buffer */
	for (i = 0; i < nr_local; i++) {
		char *p = kmap(lnb[i].lnb_page);
		long off;

		LASSERT(lnb[i].lnb_page_offset == 0);
		off = lnb[i].lnb_len & ~PAGE_MASK;
		if (off > 0)
			memset(p + off, 0, PAGE_SIZE - off);

		memcpy(buf + (i << PAGE_SHIFT), p, lnb[i].lnb_len);
		kunmap(lnb[i].lnb_page);
		copied += lnb[i].lnb_len;
		LASSERT(rc <= len);
	}
	CDEBUG(D_INFO, "Read %i (wanted %u) bytes from %llu\n", copied,
	       len, offset);
	if (copied < len)
		CWARN("Read less data than expected, is size wrong?\n");
	EXIT;
buf_put:
	dt_bufs_put(env, mo, lnb, nr_local);
free:
	OBD_FREE(lnb, sizeof(*lnb) * lnbs);
unlock:
	dt_read_unlock(env, mo);
	lu_object_put(env, &mo->do_lu);
out:
	if (rnb != NULL)
		rnb->rnb_len = copied;
	RETURN(0);
}

