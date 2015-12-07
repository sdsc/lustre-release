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

/* --------------- MDT grant code ---------------- */

long mdt_grant_connect(const struct lu_env *env,
		       struct obd_export *exp,
		       u64 want, bool conservative)
{
	struct mdt_device	*mdt = mdt_exp2dev(exp);
	u64			 left;
	long			 grant;

	ENTRY;

	dt_statfs(env, mdt->mdt_bottom, &mdt->mdt_osfs);

	left = (mdt->mdt_osfs.os_bavail * mdt->mdt_osfs.os_bsize) / 2;

	grant = left;

	CDEBUG(D_CACHE, "%s: cli %s/%p ocd_grant: %ld want: %llu left: %llu\n",
	       exp->exp_obd->obd_name, exp->exp_client_uuid.uuid,
	       exp, grant, want, left);

	return grant;
}

void mdt_grant_prepare_write(const struct lu_env *env,
			     struct obd_export *exp, struct obdo *oa,
			     struct niobuf_remote *rnb, int niocount)
{
	struct mdt_device	*mdt = mdt_exp2dev(exp);
	u64			 left;

	ENTRY;

	left = (mdt->mdt_osfs.os_bavail * mdt->mdt_osfs.os_bsize) / 2;

	/* grant more space back to the client if possible */
	oa->o_grant = left;
}
/* ---------------- end of MDT grant code ---------------- */

#define VALID_FLAGS (LA_TYPE | LA_MODE | LA_SIZE | LA_BLOCKS | \
		     LA_BLKSIZE | LA_ATIME | LA_MTIME | LA_CTIME)

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
		LASSERT(rc <= PTLRPC_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		j += rc;
		*nr_local += rc;
		LASSERT(j <= PTLRPC_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}

	LASSERT(*nr_local > 0 && *nr_local <= PTLRPC_MAX_BRW_PAGES);
	rc = dt_attr_get(env, mo, la);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	rc = dt_read_prep(env, mo, lnb, *nr_local);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	//mdt_counter_incr(exp, LPROC_OFD_STATS_READ, jobid, tot_bytes);
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
	mdt_grant_prepare_write(env, exp, oa, rnb, obj->ioo_bufcnt);
	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < obj->ioo_bufcnt; i++) {
		rc = dt_bufs_get(env, mo, rnb + i, lnb + j, 1);
		if (unlikely(rc < 0))
			GOTO(err, rc);
		LASSERT(rc <= PTLRPC_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		for (k = 0; k < rc; k++) {
			lnb[j+k].lnb_flags = rnb[i].rnb_flags;
#if 0
			if (!(rnb[i].rnb_flags & OBD_BRW_GRANTED))
				lnb[j+k].rc = -ENOSPC;
#endif
		}
		j += rc;
		*nr_local += rc;
		LASSERT(j <= PTLRPC_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}
	LASSERT(*nr_local > 0 && *nr_local <= PTLRPC_MAX_BRW_PAGES);

	rc = dt_write_prep(env, mo, lnb, *nr_local);
	if (unlikely(rc != 0))
		GOTO(err, rc);

	//ofd_counter_incr(exp, LPROC_OFD_STATS_WRITE, jobid, tot_bytes);
	RETURN(0);
err:
	dt_bufs_put(env, mo, lnb, *nr_local);
	dt_read_unlock(env, mo);
	/* ofd_grant_prepare_write() was called, so we must commit */
	//mdt_grant_commit(env, exp, rc);
out:
	/* let's still process incoming grant information packed in the oa,
	 * but without enforcing grant since we won't proceed with the write.
	 * Just like a read request actually. */
	//mdt_grant_prepare_read(env, exp, oa);
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

	if (*nr_local > PTLRPC_MAX_BRW_PAGES) {
		CERROR("%s: bulk has too many pages %d, which exceeds the"
		       "maximum pages per RPC of %d\n",
		       exp->exp_obd->obd_name, *nr_local, PTLRPC_MAX_BRW_PAGES);
		RETURN(-EPROTO);
	}

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
		/* no grants on MDT so far
		mdt_grant_prepare_read(env, exp, oa);
		*/
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
			      struct niobuf_local *lnb, int old_rc)
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
	//ofd_grant_commit(env, info->fti_exp, old_rc);
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
		//fmd = ofd_fmd_find(exp, &info->fti_fid);
		//if (!fmd || fmd->fmd_mactime_xid < info->fti_xid)
			valid |= OBD_MD_FLATIME | OBD_MD_FLMTIME |
				 OBD_MD_FLCTIME;
		//ofd_fmd_put(exp, fmd);

		la_from_obdo(la, oa, valid);

		rc = mdt_commitrw_write(env, exp, mdt, &tsi->tsi_fid,
					la, objcount, npages,
					lnb, old_rc);
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

	/* VBR: version recovery check */
	//rc = mdt_version_get_check(info, dob, 0);
	//if (rc)
	//	GOTO(unlock, rc);

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
	//CLASSERT(OST_CONNECT_SUPPORTED & OBD_CONNECT_TRUNCLOCK);

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
		rc = tgt_extent_lock(ns, &tsi->tsi_resid, start, end, &lh,
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

	//ofd_counter_incr(tsi->tsi_exp, LPROC_OFD_STATS_PUNCH,
	//		 tsi->tsi_jobid, 1);
	EXIT;
out_put:
	lu_object_put(tsi->tsi_env, &dob->do_lu);
out:
	if (srvlock)
		tgt_extent_unlock(&lh, LCK_PW);
	return rc;
}

