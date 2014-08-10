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
#include <asm/div64.h>
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

static int lmv_intent_remote(struct obd_export *exp, void *lmm,
			     int lmmsize, struct lookup_intent *it,
			     const struct lu_fid *parent_fid, int flags,
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

	LASSERT((body->valid & OBD_MD_MDS));

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

	LASSERT(fid_is_sane(&body->fid1));

	tgt = lmv_find_target(lmv, &body->fid1);
	if (IS_ERR(tgt))
		GOTO(out, rc = PTR_ERR(tgt));

	OBD_ALLOC_PTR(op_data);
	if (op_data == NULL)
		GOTO(out, rc = -ENOMEM);

	op_data->op_fid1 = body->fid1;
	/* Sent the parent FID to the remote MDT */
	if (parent_fid != NULL) {
		/* The parent fid is only for remote open to
		 * check whether the open is from OBF,
		 * see mdt_cross_open */
		LASSERT(it->it_op & IT_OPEN);
		op_data->op_fid2 = *parent_fid;
		/* Add object FID to op_fid3, in case it needs to check stale
		 * (M_CHECK_STALE), see mdc_finish_intent_lock */
		op_data->op_fid3 = body->fid1;
	}

	op_data->op_bias = MDS_CROSS_REF;
	CDEBUG(D_INODE, "REMOTE_INTENT with fid="DFID" -> mds #%d\n",
	       PFID(&body->fid1), tgt->ltd_idx);

        rc = md_intent_lock(tgt->ltd_exp, op_data, lmm, lmmsize, it,
                            flags, &req, cb_blocking, extra_lock_flags);
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

	it->d.lustre.it_lock_handle = plock.cookie;
	it->d.lustre.it_lock_mode = pmode;

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

	if (lsm->lsm_count <= 1)
		RETURN(0);

	OBD_ALLOC_PTR(op_data);
	if (op_data == NULL)
		RETURN(-ENOMEM);

	/**
	 * Loop over the stripe information, check validity and update them
	 * from MDS if needed.
	 */
	for (i = 0; i < lsm->lsm_count; i++) {
		struct lu_fid		fid;
		struct lookup_intent	it = { .it_op = IT_GETATTR };
		struct ptlrpc_request	*req = NULL;
		struct lustre_handle	*lockh = NULL;
		struct lmv_tgt_desc	*tgt;

		fid = lsm->lsm_oinfo[i].lmo_fid;
		if (i == 0) {
			if (mbody != NULL) {
				body = mbody;
				goto update;
			} else {
				goto release_lock;
			}
		}

		/*
		 * Prepare op_data for revalidating. Note that @fid2 shuld be
		 * defined otherwise it will go to server and take new lock
		 * which is what we reall not need here.
		 */
		memset(op_data, 0, sizeof(*op_data));
		op_data->op_bias = MDS_CROSS_REF;
		op_data->op_fid1 = fid;
		op_data->op_fid2 = fid;

		tgt = lmv_locate_mds(lmv, op_data, &fid);
		if (IS_ERR(tgt))
			GOTO(cleanup, rc = PTR_ERR(tgt));

		CDEBUG(D_INODE, "Revalidate slave "DFID" -> mds #%d\n",
		       PFID(&fid), tgt->ltd_idx);

		rc = md_intent_lock(tgt->ltd_exp, op_data, NULL, 0, &it, 0,
				    &req, cb_blocking, extra_lock_flags);

		lockh = (struct lustre_handle *)&it.d.lustre.it_lock_handle;
		if (rc > 0 && req == NULL) {
			/*
			 * Nice, this slave is valid.
			 */
			CDEBUG(D_INODE, "Cached slave "DFID"\n", PFID(&fid));
			rc = 0;
			goto release_lock;
		}

		if (rc < 0)
			GOTO(cleanup, rc);

		body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
		LASSERT(body != NULL);
update:
		if (unlikely(body->nlink < 2)) {
			CERROR("nlink %d < 2 corrupt stripe %d "DFID" of "
			       DFID"\n", body->nlink, i,
			       PFID(&lsm->lsm_oinfo[i].lmo_fid),
			       PFID(&lsm->lsm_oinfo[0].lmo_fid));
			if (req)
				ptlrpc_req_finished(req);
			GOTO(cleanup, rc = -EIO);
		}

		lsm->lsm_oinfo[i].lmo_size = body->size;

		/* subtract nlink of . and .. from non-master stripe */
		lsm->lsm_oinfo[i].lmo_nlink = body->nlink;

		lsm->lsm_oinfo[i].lmo_atime = body->atime;
		lsm->lsm_oinfo[i].lmo_ctime = body->ctime;
		lsm->lsm_oinfo[i].lmo_mtime = body->mtime;
		CDEBUG(D_INODE, "Fresh size %lu nlink %lu from "DFID" atime "
		       LPU64 "mtime "LPU64" ctime "LPU64"\n",
		       (unsigned long)body->size, (unsigned long)body->nlink,
		       PFID(&fid), body->atime, body->mtime, body->ctime);
		if (req)
			ptlrpc_req_finished(req);
release_lock:
		size += lsm->lsm_oinfo[i].lmo_size;

		if (i != 0)
			nlink += lsm->lsm_oinfo[i].lmo_nlink - 2;
		else
			nlink += lsm->lsm_oinfo[i].lmo_nlink;

		if (lsm->lsm_oinfo[i].lmo_atime > atime)
			atime = lsm->lsm_oinfo[i].lmo_atime;

		if (lsm->lsm_oinfo[i].lmo_ctime > ctime)
			ctime = lsm->lsm_oinfo[i].lmo_ctime;

		if (lsm->lsm_oinfo[i].lmo_mtime > mtime)
			mtime = lsm->lsm_oinfo[i].lmo_mtime;

		if (it.d.lustre.it_lock_mode && lockh) {
			ldlm_lock_decref(lockh, it.d.lustre.it_lock_mode);
			it.d.lustre.it_lock_mode = 0;
		}
	}

	/*
	 * update attr of master request.
	 */
	CDEBUG(D_INODE, "Return refreshed attrs: size = %lu nlink %lu atime "
	       LPU64 "ctime "LPU64" mtime "LPU64" for " DFID"\n", size, nlink,
	       atime, ctime, mtime, PFID(&lsm->lsm_oinfo[0].lmo_fid));

	if (mbody != NULL) {
		mbody->size = size;
		mbody->nlink = nlink;
		mbody->atime = atime;
		mbody->ctime = ctime;
		mbody->mtime = mtime;
	}
cleanup:
	OBD_FREE_PTR(op_data);
	RETURN(rc);
}

/*
 * IT_OPEN is intended to open (and create, possible) an object. Parent (pid)
 * may be split dir.
 */
int lmv_intent_open(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
                    ldlm_blocking_callback cb_blocking,
		    __u64 extra_lock_flags)
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
		if (op_data->op_mea1) {
			struct lmv_stripe_md *lsm = op_data->op_mea1;
			int index;

			index = raw_name2idx(lsm->lsm_hash_type, lsm->lsm_count,
					     op_data->op_name,
					     op_data->op_namelen);
			LASSERT(index < lsm->lsm_count);
			op_data->op_fid1 = lsm->lsm_oinfo[index].lmo_fid;
		}

		tgt = lmv_find_target(lmv, &op_data->op_fid2);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));

		op_data->op_mds = tgt->ltd_idx;
	} else {
		tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	}
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	/* If it is ready to open the file by FID, do not need
	 * allocate FID at all, otherwise it will confuse MDT */
	if ((it->it_op & IT_CREAT) &&
	    !(it->it_flags & MDS_OPEN_BY_FID)) {
		/*
		 * For open with IT_CREATE and for IT_CREATE cases allocate new
		 * fid and setup FLD for it.
		 */
		op_data->op_fid3 = op_data->op_fid2;
		rc = lmv_fid_alloc(exp, &op_data->op_fid2, op_data);
		if (rc != 0)
			RETURN(rc);
	}

	CDEBUG(D_INODE, "OPEN_INTENT with fid1="DFID", fid2="DFID","
	       " name='%s' -> mds #%d\n", PFID(&op_data->op_fid1),
	       PFID(&op_data->op_fid2), op_data->op_name, tgt->ltd_idx);

	rc = md_intent_lock(tgt->ltd_exp, op_data, lmm, lmmsize, it, flags,
			    reqp, cb_blocking, extra_lock_flags);
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
	if (unlikely((body->valid & OBD_MD_MDS))) {
		rc = lmv_intent_remote(exp, lmm, lmmsize, it, &op_data->op_fid1,
				       flags, reqp, cb_blocking,
				       extra_lock_flags);
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
int lmv_intent_lookup(struct obd_export *exp, struct md_op_data *op_data,
                      void *lmm, int lmmsize, struct lookup_intent *it,
                      int flags, struct ptlrpc_request **reqp,
                      ldlm_blocking_callback cb_blocking,
		      __u64 extra_lock_flags)
{
	struct obd_device      *obd = exp->exp_obd;
	struct lmv_obd         *lmv = &obd->u.lmv;
	struct lmv_tgt_desc    *tgt = NULL;
	struct mdt_body        *body;
	int                     rc = 0;
	struct lmv_stripe_md   *lsm = op_data->op_mea1;
	ENTRY;

	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	if (!fid_is_sane(&op_data->op_fid2))
		fid_zero(&op_data->op_fid2);

	CDEBUG(D_INODE, "LOOKUP_INTENT with fid1="DFID", fid2="DFID
	       ", name='%s' -> mds #%d lsm %p lsm_magic %x\n",
	       PFID(&op_data->op_fid1), PFID(&op_data->op_fid2),
	       op_data->op_name ? op_data->op_name : "<NULL>",
	       tgt->ltd_idx, lsm, lsm == NULL ? -1 : lsm->lsm_md_magic);

	op_data->op_bias &= ~MDS_CROSS_REF;

	rc = md_intent_lock(tgt->ltd_exp, op_data, lmm, lmmsize, it,
			     flags, reqp, cb_blocking, extra_lock_flags);
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
	} else if (it_disposition(it, DISP_LOOKUP_NEG) &&
		   lsm != NULL && lsm->lsm_md_magic == LMV_MAGIC_MIGRATE) {
		/* For migrating directory, if it can not find the child in
		 * the source directory(master stripe), try the targeting
		 * directory(stripe 1) */
		LASSERT(lsm->lsm_count == 2);
		tgt = lmv_find_target(lmv, &lsm->lsm_oinfo[1].lmo_fid);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));

		ptlrpc_req_finished(*reqp);
		CDEBUG(D_INODE, "For migrating dir, try target dir "DFID"\n",
		       PFID(&lsm->lsm_oinfo[1].lmo_fid));

		op_data->op_fid1 = lsm->lsm_oinfo[1].lmo_fid;
		it->d.lustre.it_disposition &= ~DISP_ENQ_COMPLETE;
		rc = md_intent_lock(tgt->ltd_exp, op_data, lmm, lmmsize, it,
			     flags, reqp, cb_blocking, extra_lock_flags);
		RETURN(rc);
	}
	/*
	 * MDS has returned success. Probably name has been resolved in
	 * remote inode. Let's check this.
	 */
	body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	/* Not cross-ref case, just get out of here. */
	if (unlikely((body->valid & OBD_MD_MDS))) {
		rc = lmv_intent_remote(exp, lmm, lmmsize, it, NULL, flags,
				       reqp, cb_blocking, extra_lock_flags);
		if (rc != 0)
			RETURN(rc);
		body = req_capsule_server_get(&(*reqp)->rq_pill, &RMF_MDT_BODY);
		if (body == NULL)
			RETURN(-EPROTO);
	}

	RETURN(rc);
}

int lmv_intent_lock(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
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
                rc = lmv_intent_lookup(exp, op_data, lmm, lmmsize, it,
                                       flags, reqp, cb_blocking,
                                       extra_lock_flags);
        else if (it->it_op & IT_OPEN)
                rc = lmv_intent_open(exp, op_data, lmm, lmmsize, it,
                                     flags, reqp, cb_blocking,
                                     extra_lock_flags);
        else
                LBUG();
        RETURN(rc);
}
