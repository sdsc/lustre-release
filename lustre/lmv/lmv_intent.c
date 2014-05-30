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
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LMV
#ifdef __KERNEL__
#include <linux/slab.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <linux/math64.h>
#include <linux/seq_file.h>
#include <linux/namei.h>
#include <linux/lustre_intent.h>
#else
#include <liblustre.h>
#endif

#include <obd_support.h>
#include <lustre/lustre_idl.h>
#include <lustre_lib.h>
#include <lustre_net.h>
#include <lustre_dlm.h>
#include <lustre_mdc.h>
#include <obd_class.h>
#include <lprocfs_status.h>
#include "lmv_internal.h"

static int lmv_intent_remote(struct obd_export *exp, struct lookup_intent *it,
			     const struct lu_fid *parent_fid,
			     struct ptlrpc_request **reqp,
			     ldlm_blocking_callback cb_blocking,
			     __u64 extra_lock_flags)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct ptlrpc_request	*req = NULL;
	struct lustre_handle	plock;
	struct md_op_data	*op_data;
	struct lmv_tgt_desc	*tgt;
	struct mdt_body		*body;
	int			pmode;
	int			rc = 0;
	ENTRY;

	body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	LASSERT((body->mbo_valid & OBD_MD_MDS));

	/*
	 * Unfortunately, we have to lie to MDC/MDS to retrieve
	 * attributes llite needs and provideproper locking.
	 */
	if (it->it_op & IT_LOOKUP)
		it->it_op = IT_GETATTR;

	/*
	 * We got LOOKUP lock, but we really need attrs.
	 */
	pmode = it->d.lustre.it_lock_mode;
	if (pmode) {
		plock.cookie = it->d.lustre.it_lock_handle;
		it->d.lustre.it_lock_mode = 0;
		it->d.lustre.it_data = NULL;
	}

	LASSERT(fid_is_sane(&body->mbo_fid1));

	tgt = lmv_find_target(lmv, &body->mbo_fid1);
	if (IS_ERR(tgt))
		GOTO(out, rc = PTR_ERR(tgt));

	OBD_ALLOC_PTR(op_data);
	if (op_data == NULL)
		GOTO(out, rc = -ENOMEM);

	op_data->op_fid1 = body->mbo_fid1;
	/* Sent the parent FID to the remote MDT */
	if (parent_fid != NULL) {
		/* The parent fid is only for remote open to
		 * check whether the open is from OBF,
		 * see mdt_cross_open */
		LASSERT(it->it_op & IT_OPEN);
		op_data->op_fid2 = *parent_fid;
		/* Add object FID to op_fid3, in case it needs to check stale
		 * (M_CHECK_STALE), see mdc_finish_intent_lock */
		op_data->op_fid3 = body->mbo_fid1;
	}

	op_data->op_bias = MDS_CROSS_REF;
	CDEBUG(D_INODE, "REMOTE_INTENT with fid="DFID" -> mds #%d\n",
	       PFID(&body->mbo_fid1), tgt->ltd_idx);

	rc = md_intent_lock(tgt->ltd_exp, op_data, it, &req, cb_blocking,
			    extra_lock_flags);
        if (rc)
                GOTO(out_free_op_data, rc);

	/*
	 * LLite needs LOOKUP lock to track dentry revocation in order to
	 * maintain dcache consistency. Thus drop UPDATE|PERM lock here
	 * and put LOOKUP in request.
	 */
	if (it->d.lustre.it_lock_mode != 0) {
		it->d.lustre.it_remote_lock_handle =
					it->d.lustre.it_lock_handle;
		it->d.lustre.it_remote_lock_mode = it->d.lustre.it_lock_mode;
	}

	if (pmode) {
		it->d.lustre.it_lock_handle = plock.cookie;
		it->d.lustre.it_lock_mode = pmode;
	}

	EXIT;
out_free_op_data:
	OBD_FREE_PTR(op_data);
out:
	if (rc && pmode)
		ldlm_lock_decref(&plock, pmode);

	ptlrpc_req_finished(*reqp);
	*reqp = req;
	return rc;
}

#ifdef __KERNEL__
int lmv_revalidate_slaves(struct obd_export *exp, struct mdt_body *mbody,
			  struct lmv_stripe_md *lsm,
			  ldlm_blocking_callback cb_blocking,
			  int extra_lock_flags)
{
	struct obd_device      *obd = exp->exp_obd;
	struct lmv_obd         *lmv = &obd->u.lmv;
	struct mdt_body		*body;
	struct md_op_data      *op_data;
	unsigned long           size = 0;
	unsigned long           nlink = 0;
	obd_time		atime = 0;
	obd_time		ctime = 0;
	obd_time		mtime = 0;
	int                     i;
	int                     rc = 0;

	ENTRY;

	/**
	 * revalidate slaves has some problems, temporarily return,
	 * we may not need that
	 */
	OBD_ALLOC_PTR(op_data);
	if (op_data == NULL)
		RETURN(-ENOMEM);

	/**
	 * Loop over the stripe information, check validity and update them
	 * from MDS if needed.
	 */
	for (i = 0; i < lsm->lsm_md_stripe_count; i++) {
		struct lu_fid		fid;
		struct lookup_intent	it = { .it_op = IT_GETATTR };
		struct ptlrpc_request	*req = NULL;
		struct lustre_handle	*lockh = NULL;
		struct lmv_tgt_desc	*tgt = NULL;
		struct inode		*inode;

		fid = lsm->lsm_md_oinfo[i].lmo_fid;
		inode = lsm->lsm_md_oinfo[i].lmo_root;

		/*
		 * Prepare op_data for revalidating. Note that @fid2 shluld be
		 * defined otherwise it will go to server and take new lock
		 * which is not needed here.
		 */
		memset(op_data, 0, sizeof(*op_data));
		op_data->op_fid1 = fid;
		op_data->op_fid2 = fid;

		tgt = lmv_locate_mds(lmv, op_data, &fid);
		if (IS_ERR(tgt))
			GOTO(cleanup, rc = PTR_ERR(tgt));

		CDEBUG(D_INODE, "Revalidate slave "DFID" -> mds #%d\n",
		       PFID(&fid), tgt->ltd_idx);

		rc = md_intent_lock(tgt->ltd_exp, op_data, &it, &req,
				    cb_blocking, extra_lock_flags);
		if (rc < 0)
			GOTO(cleanup, rc);

		lockh = (struct lustre_handle *)&it.d.lustre.it_lock_handle;
		if (rc > 0 && req == NULL) {
			/* slave inode is still valid */
			CDEBUG(D_INODE, "slave "DFID" is still valid.\n",
			       PFID(&fid));
			rc = 0;
		} else {
			/* refresh slave from server */
			body = req_capsule_server_get(&req->rq_pill,
						      &RMF_MDT_BODY);
			LASSERT(body != NULL);
			if (unlikely(body->mbo_nlink < 2)) {
				CERROR("%s: nlink %d < 2 corrupt stripe %d "DFID
				       ":" DFID"\n",
				       obd->obd_name, body->mbo_nlink, i,
				       PFID(&lsm->lsm_md_oinfo[i].lmo_fid),
				       PFID(&lsm->lsm_md_oinfo[0].lmo_fid));

				if (req != NULL)
					ptlrpc_req_finished(req);

				if (it.d.lustre.it_lock_mode && lockh) {
					ldlm_lock_decref(lockh,
						 it.d.lustre.it_lock_mode);
					it.d.lustre.it_lock_mode = 0;
				}

				GOTO(cleanup, rc = -EIO);
			}


			i_size_write(inode, body->mbo_size);
			set_nlink(inode, body->mbo_nlink);
			LTIME_S(inode->i_atime) = body->mbo_atime;
			LTIME_S(inode->i_ctime) = body->mbo_ctime;
			LTIME_S(inode->i_mtime) = body->mbo_mtime;

			if (req != NULL)
				ptlrpc_req_finished(req);
		}

		md_set_lock_data(tgt->ltd_exp, &lockh->cookie, inode, NULL);

		size += i_size_read(inode);

		if (i != 0)
			nlink += inode->i_nlink - 2;
		else
			nlink += inode->i_nlink;

		atime = LTIME_S(inode->i_atime) > atime ?
				LTIME_S(inode->i_atime) : atime;
		ctime = LTIME_S(inode->i_ctime) > ctime ?
				LTIME_S(inode->i_ctime) : ctime;
		mtime = LTIME_S(inode->i_mtime) > mtime ?
				LTIME_S(inode->i_mtime) : mtime;

		if (it.d.lustre.it_lock_mode != 0 && lockh != NULL) {
			ldlm_lock_decref(lockh, it.d.lustre.it_lock_mode);
			it.d.lustre.it_lock_mode = 0;
		}

		CDEBUG(D_INODE, "i %d "DFID" size %llu, nlink %u, atime "
		       "%lu, mtime %lu, ctime %lu.\n", i, PFID(&fid),
		       i_size_read(inode), inode->i_nlink,
		       LTIME_S(inode->i_atime), LTIME_S(inode->i_mtime),
		       LTIME_S(inode->i_ctime));
	}

	/*
	 * update attr of master request.
	 */
	CDEBUG(D_INODE, "Return refreshed attrs: size = %lu nlink %lu atime "
	       LPU64 "ctime "LPU64" mtime "LPU64" for " DFID"\n", size, nlink,
	       atime, ctime, mtime, PFID(&lsm->lsm_md_oinfo[0].lmo_fid));

	if (mbody != NULL) {
		mbody->mbo_atime = atime;
		mbody->mbo_ctime = ctime;
		mbody->mbo_mtime = mtime;
	}
cleanup:
	OBD_FREE_PTR(op_data);
	RETURN(rc);
}

#else

int lmv_revalidate_slaves(struct obd_export *exp, struct mdt_body *mbody,
			  struct lmv_stripe_md *lsm,
			  ldlm_blocking_callback cb_blocking,
			  int extra_lock_flags)
{
	return 0;
}

#endif

/*
 * IT_OPEN is intended to open (and create, possible) an object. Parent (pid)
 * may be split dir.
 */
int lmv_intent_open(struct obd_export *exp, struct md_op_data *op_data,
		    struct lookup_intent *it, struct ptlrpc_request **reqp,
		    ldlm_blocking_callback cb_blocking, __u64 extra_lock_flags)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt;
	struct mdt_body		*body;
	int			rc;
	ENTRY;

	/* Note: client might open with some random flags(sanity 33b), so we can
	 * not make sure op_fid2 is being initialized with BY_FID flag */
	if (it->it_flags & MDS_OPEN_BY_FID && fid_is_sane(&op_data->op_fid2)) {
		if (op_data->op_mea1 != NULL) {
			struct lmv_stripe_md	*lsm = op_data->op_mea1;
			const struct lmv_oinfo	*oinfo;

			oinfo = lsm_name_to_stripe_info(lsm, op_data->op_name,
							op_data->op_namelen);
			if (IS_ERR(oinfo))
				RETURN(PTR_ERR(oinfo));
			op_data->op_fid1 = oinfo->lmo_fid;
		}

		tgt = lmv_find_target(lmv, &op_data->op_fid2);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));

		op_data->op_mds = tgt->ltd_idx;
	} else {
		tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));
	}

	/* If it is ready to open the file by FID, do not need
	 * allocate FID at all, otherwise it will confuse MDT */
	if ((it->it_op & IT_CREAT) &&
	    !(it->it_flags & MDS_OPEN_BY_FID)) {
		/*
		 * For open with IT_CREATE and for IT_CREATE cases allocate new
		 * fid and setup FLD for it.
		 */
		op_data->op_fid3 = op_data->op_fid2;
		rc = lmv_fid_alloc(NULL, exp, &op_data->op_fid2, op_data);
		if (rc != 0)
			RETURN(rc);
	}

	CDEBUG(D_INODE, "OPEN_INTENT with fid1="DFID", fid2="DFID","
	       " name='%s' -> mds #%d\n", PFID(&op_data->op_fid1),
	       PFID(&op_data->op_fid2), op_data->op_name, tgt->ltd_idx);

	rc = md_intent_lock(tgt->ltd_exp, op_data, it, reqp, cb_blocking,
			    extra_lock_flags);
	if (rc != 0)
		RETURN(rc);
	/*
	 * Nothing is found, do not access body->fid1 as it is zero and thus
	 * pointless.
	 */
	if ((it->d.lustre.it_disposition & DISP_LOOKUP_NEG) &&
	    !(it->d.lustre.it_disposition & DISP_OPEN_CREATE) &&
	    !(it->d.lustre.it_disposition & DISP_OPEN_OPEN))
		RETURN(rc);

	body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	/* Not cross-ref case, just get out of here. */
	if (unlikely((body->mbo_valid & OBD_MD_MDS))) {
		rc = lmv_intent_remote(exp, it, &op_data->op_fid1, reqp,
				       cb_blocking, extra_lock_flags);
		if (rc != 0)
			RETURN(rc);

		body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
		if (body == NULL)
			RETURN(-EPROTO);
	}

	RETURN(rc);
}

/*
 * Handler for: getattr, lookup and revalidate cases.
 */
static int
lmv_intent_lookup(struct obd_export *exp, struct md_op_data *op_data,
		  struct lookup_intent *it, struct ptlrpc_request **reqp,
		  ldlm_blocking_callback cb_blocking,
		  __u64 extra_lock_flags)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt = NULL;
	struct mdt_body		*body;
	struct lmv_stripe_md	*lsm = op_data->op_mea1;
	int			rc = 0;
	ENTRY;

	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	if (!fid_is_sane(&op_data->op_fid2))
		fid_zero(&op_data->op_fid2);

	CDEBUG(D_INODE, "LOOKUP_INTENT with fid1="DFID", fid2="DFID
	       ", name='%s' -> mds #%d lsm=%p lsm_magic=%x\n",
	       PFID(&op_data->op_fid1), PFID(&op_data->op_fid2),
	       op_data->op_name ? op_data->op_name : "<NULL>",
	       tgt->ltd_idx, lsm, lsm == NULL ? -1 : lsm->lsm_md_magic);

	op_data->op_bias &= ~MDS_CROSS_REF;

	rc = md_intent_lock(tgt->ltd_exp, op_data, it, reqp, cb_blocking,
			    extra_lock_flags);
	if (rc < 0)
		RETURN(rc);

	if (*reqp == NULL) {
		/* If RPC happens, lsm information will be revalidated
		 * during update_inode process (see ll_update_lsm_md) */
		if (op_data->op_mea2 != NULL) {
			rc = lmv_revalidate_slaves(exp, NULL, op_data->op_mea2,
						   cb_blocking,
						   extra_lock_flags);
			if (rc != 0)
				RETURN(rc);
		}
		RETURN(rc);
	} else if (it_disposition(it, DISP_LOOKUP_NEG) && lsm != NULL &&
		   lsm->lsm_md_hash_type & LMV_HASH_FLAG_MIGRATION) {
		/* For migrating directory, if it can not find the child in
		 * the source directory(master stripe), try the targeting
		 * directory(stripe 1) */
		tgt = lmv_find_target(lmv, &lsm->lsm_md_oinfo[1].lmo_fid);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));

		ptlrpc_req_finished(*reqp);
		it->d.lustre.it_data = NULL;
		*reqp = NULL;

		CDEBUG(D_INODE, "For migrating dir, try target dir "DFID"\n",
		       PFID(&lsm->lsm_md_oinfo[1].lmo_fid));

		op_data->op_fid1 = lsm->lsm_md_oinfo[1].lmo_fid;
		it->d.lustre.it_disposition &= ~DISP_ENQ_COMPLETE;
		rc = md_intent_lock(tgt->ltd_exp, op_data, it, reqp,
				    cb_blocking, extra_lock_flags);
	}
	/*
	 * MDS has returned success. Probably name has been resolved in
	 * remote inode. Let's check this.
	 */
	body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	/* Not cross-ref case, just get out of here. */
	if (unlikely((body->mbo_valid & OBD_MD_MDS))) {
		rc = lmv_intent_remote(exp, it, NULL, reqp, cb_blocking,
				       extra_lock_flags);
		if (rc != 0)
			RETURN(rc);
		body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
		if (body == NULL)
			RETURN(-EPROTO);
	}

	RETURN(rc);
}

int lmv_intent_lock(struct obd_export *exp, struct md_op_data *op_data,
		    struct lookup_intent *it, struct ptlrpc_request **reqp,
		    ldlm_blocking_callback cb_blocking,
		    __u64 extra_lock_flags)
{
        struct obd_device *obd = exp->exp_obd;
        int                rc;
        ENTRY;

        LASSERT(it != NULL);
        LASSERT(fid_is_sane(&op_data->op_fid1));

        CDEBUG(D_INODE, "INTENT LOCK '%s' for '%*s' on "DFID"\n",
               LL_IT2STR(it), op_data->op_namelen, op_data->op_name,
               PFID(&op_data->op_fid1));

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

	if (it->it_op & (IT_LOOKUP | IT_GETATTR | IT_LAYOUT))
		rc = lmv_intent_lookup(exp, op_data, it, reqp, cb_blocking,
				       extra_lock_flags);
	else if (it->it_op & IT_OPEN)
		rc = lmv_intent_open(exp, op_data, it, reqp, cb_blocking,
				     extra_lock_flags);
	else
		LBUG();

	RETURN(rc);
}
