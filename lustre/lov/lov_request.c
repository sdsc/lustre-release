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
 * Copyright (c) 2005, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LOV

#ifdef __KERNEL__
#include <libcfs/libcfs.h>
#else
#include <liblustre.h>
#endif

#include <obd_class.h>
#include <obd_lov.h>
#include <obd_ost.h>
#include <lustre/lustre_idl.h>

#include "lov_internal.h"

static void lov_init_set(struct lov_request_set *set)
{
	set->set_count = 0;
	cfs_atomic_set(&set->set_completes, 0);
	cfs_atomic_set(&set->set_success, 0);
	cfs_atomic_set(&set->set_finish_checked, 0);
	set->set_cookies = 0;
	CFS_INIT_LIST_HEAD(&set->set_list);
	cfs_atomic_set(&set->set_refcount, 1);
	init_waitqueue_head(&set->set_waitq);
}

void lov_finish_set(struct lov_request_set *set)
{
	struct list_head *pos, *n;
	struct lov_request *req;
	ENTRY;

	LASSERT(set != NULL);
	list_for_each_safe(pos, n, &set->set_list) {
		req = list_entry(pos, struct lov_request, rq_link);
		list_del_init(&req->rq_link);

		if (req->rq_oi.oi_oa != NULL)
			OBDO_FREE(req->rq_oi.oi_oa);

		if (req->rq_oi.oi_osfs != NULL)
			OBD_FREE_PTR(req->rq_oi.oi_osfs);

		OBD_FREE_PTR(req);
	}

	OBD_FREE_PTR(set);
	EXIT;
}

int lov_set_finished(struct lov_request_set *set, int idempotent)
{
	int completes = cfs_atomic_read(&set->set_completes);

	CDEBUG(D_INFO, "check set %d/%d\n", completes, set->set_count);

	if (completes == set->set_count) {
		if (idempotent)
			return 1;
		if (cfs_atomic_inc_return(&set->set_finish_checked) == 1)
			return 1;
	}
	return 0;
}

void lov_update_set(struct lov_request_set *set,
		    struct lov_request *req, int rc)
{
	req->rq_complete = 1;
	req->rq_rc = rc;

	cfs_atomic_inc(&set->set_completes);
	if (rc == 0)
		cfs_atomic_inc(&set->set_success);

	wake_up(&set->set_waitq);
}

int lov_update_common_set(struct lov_request_set *set,
                          struct lov_request *req, int rc)
{
        struct lov_obd *lov = &set->set_exp->exp_obd->u.lov;
        ENTRY;

        lov_update_set(set, req, rc);

        /* grace error on inactive ost */
        if (rc && !(lov->lov_tgts[req->rq_idx] &&
                    lov->lov_tgts[req->rq_idx]->ltd_active))
                rc = 0;

        /* FIXME in raid1 regime, should return 0 */
        RETURN(rc);
}

void lov_set_add_req(struct lov_request *req, struct lov_request_set *set)
{
        cfs_list_add_tail(&req->rq_link, &set->set_list);
        set->set_count++;
        req->rq_rqset = set;
}

static int lov_check_set(struct lov_obd *lov, int idx)
{
	int rc = 0;
	mutex_lock(&lov->lov_lock);

	if (lov->lov_tgts[idx] == NULL ||
	    lov->lov_tgts[idx]->ltd_active ||
	    (lov->lov_tgts[idx]->ltd_exp != NULL &&
	     class_exp2cliimp(lov->lov_tgts[idx]->ltd_exp)->imp_connect_tried))
		rc = 1;

	mutex_unlock(&lov->lov_lock);
	return rc;
}

/* Check if the OSC connection exists and is active.
 * If the OSC has not yet had a chance to connect to the OST the first time,
 * wait once for it to connect instead of returning an error.
 */
int lov_check_and_wait_active(struct lov_obd *lov, int ost_idx)
{
	wait_queue_head_t waitq;
	struct l_wait_info lwi;
	struct lov_tgt_desc *tgt;
	int rc = 0;

	mutex_lock(&lov->lov_lock);

	tgt = lov->lov_tgts[ost_idx];

	if (unlikely(tgt == NULL))
		GOTO(out, rc = 0);

	if (likely(tgt->ltd_active))
		GOTO(out, rc = 1);

	if (tgt->ltd_exp && class_exp2cliimp(tgt->ltd_exp)->imp_connect_tried)
		GOTO(out, rc = 0);

	mutex_unlock(&lov->lov_lock);

	init_waitqueue_head(&waitq);
	lwi = LWI_TIMEOUT_INTERVAL(cfs_time_seconds(obd_timeout),
				   cfs_time_seconds(1), NULL, NULL);

	rc = l_wait_event(waitq, lov_check_set(lov, ost_idx), &lwi);
	if (tgt->ltd_active)
		return 1;

	return 0;

out:
	mutex_unlock(&lov->lov_lock);
	return rc;
}

static int common_attr_done(struct lov_request_set *set)
{
        cfs_list_t *pos;
        struct lov_request *req;
        struct obdo *tmp_oa;
        int rc = 0, attrset = 0;
        ENTRY;

        LASSERT(set->set_oi != NULL);

        if (set->set_oi->oi_oa == NULL)
                RETURN(0);

        if (!cfs_atomic_read(&set->set_success))
                RETURN(-EIO);

        OBDO_ALLOC(tmp_oa);
        if (tmp_oa == NULL)
                GOTO(out, rc = -ENOMEM);

        cfs_list_for_each (pos, &set->set_list) {
                req = cfs_list_entry(pos, struct lov_request, rq_link);

                if (!req->rq_complete || req->rq_rc)
                        continue;
                if (req->rq_oi.oi_oa->o_valid == 0)   /* inactive stripe */
                        continue;
                lov_merge_attrs(tmp_oa, req->rq_oi.oi_oa,
                                req->rq_oi.oi_oa->o_valid,
                                set->set_oi->oi_md, req->rq_stripe, &attrset);
        }
        if (!attrset) {
                CERROR("No stripes had valid attrs\n");
                rc = -EIO;
        }
        if ((set->set_oi->oi_oa->o_valid & OBD_MD_FLEPOCH) &&
            (set->set_oi->oi_md->lsm_stripe_count != attrset)) {
                /* When we take attributes of some epoch, we require all the
                 * ost to be active. */
                CERROR("Not all the stripes had valid attrs\n");
                GOTO(out, rc = -EIO);
        }

        tmp_oa->o_oi = set->set_oi->oi_oa->o_oi;
        memcpy(set->set_oi->oi_oa, tmp_oa, sizeof(*set->set_oi->oi_oa));
out:
        if (tmp_oa)
                OBDO_FREE(tmp_oa);
        RETURN(rc);

}

int lov_fini_getattr_set(struct lov_request_set *set)
{
        int rc = 0;
        ENTRY;

        if (set == NULL)
                RETURN(0);
        LASSERT(set->set_exp);
        if (cfs_atomic_read(&set->set_completes))
                rc = common_attr_done(set);

        lov_put_reqset(set);

        RETURN(rc);
}

/* The callback for osc_getattr_async that finilizes a request info when a
 * response is received. */
static int cb_getattr_update(void *cookie, int rc)
{
        struct obd_info *oinfo = cookie;
        struct lov_request *lovreq;
        lovreq = container_of(oinfo, struct lov_request, rq_oi);
        return lov_update_common_set(lovreq->rq_rqset, lovreq, rc);
}

int lov_prep_getattr_set(struct obd_export *exp, struct obd_info *oinfo,
                         struct lov_request_set **reqset)
{
        struct lov_request_set *set;
        struct lov_obd *lov = &exp->exp_obd->u.lov;
        int rc = 0, i;
        ENTRY;

        OBD_ALLOC(set, sizeof(*set));
        if (set == NULL)
                RETURN(-ENOMEM);
        lov_init_set(set);

        set->set_exp = exp;
        set->set_oi = oinfo;

        for (i = 0; i < oinfo->oi_md->lsm_stripe_count; i++) {
		struct lov_oinfo *loi;
		struct lov_request *req;

		loi = oinfo->oi_md->lsm_oinfo[i];
		if (!lov_check_and_wait_active(lov, loi->loi_ost_idx)) {
			CDEBUG(D_HA, "lov idx %d inactive\n", loi->loi_ost_idx);
			if (oinfo->oi_oa->o_valid & OBD_MD_FLEPOCH)
				/* SOM requires all the OSTs to be active. */
				GOTO(out_set, rc = -EIO);
			continue;
		}

                OBD_ALLOC(req, sizeof(*req));
                if (req == NULL)
                        GOTO(out_set, rc = -ENOMEM);

                req->rq_stripe = i;
                req->rq_idx = loi->loi_ost_idx;

                OBDO_ALLOC(req->rq_oi.oi_oa);
                if (req->rq_oi.oi_oa == NULL) {
                        OBD_FREE(req, sizeof(*req));
                        GOTO(out_set, rc = -ENOMEM);
                }
                memcpy(req->rq_oi.oi_oa, oinfo->oi_oa,
                       sizeof(*req->rq_oi.oi_oa));
                req->rq_oi.oi_oa->o_oi = loi->loi_oi;
                req->rq_oi.oi_cb_up = cb_getattr_update;
                req->rq_oi.oi_capa = oinfo->oi_capa;

                lov_set_add_req(req, set);
        }
        if (!set->set_count)
                GOTO(out_set, rc = -EIO);
        *reqset = set;
        RETURN(rc);
out_set:
        lov_fini_getattr_set(set);
        RETURN(rc);
}

int lov_fini_destroy_set(struct lov_request_set *set)
{
        ENTRY;

        if (set == NULL)
                RETURN(0);
        LASSERT(set->set_exp);
        if (cfs_atomic_read(&set->set_completes)) {
                /* FIXME update qos data here */
        }

        lov_put_reqset(set);

        RETURN(0);
}

int lov_prep_destroy_set(struct obd_export *exp, struct obd_info *oinfo,
                         struct obdo *src_oa, struct lov_stripe_md *lsm,
                         struct obd_trans_info *oti,
                         struct lov_request_set **reqset)
{
        struct lov_request_set *set;
        struct lov_obd *lov = &exp->exp_obd->u.lov;
        int rc = 0, i;
        ENTRY;

        OBD_ALLOC(set, sizeof(*set));
        if (set == NULL)
                RETURN(-ENOMEM);
        lov_init_set(set);

        set->set_exp = exp;
        set->set_oi = oinfo;
        set->set_oi->oi_md = lsm;
        set->set_oi->oi_oa = src_oa;
        if (oti != NULL && src_oa->o_valid & OBD_MD_FLCOOKIE)
                set->set_cookies = oti->oti_logcookies;

        for (i = 0; i < lsm->lsm_stripe_count; i++) {
		struct lov_oinfo *loi;
		struct lov_request *req;

		loi = lsm->lsm_oinfo[i];
		if (!lov_check_and_wait_active(lov, loi->loi_ost_idx)) {
			CDEBUG(D_HA, "lov idx %d inactive\n", loi->loi_ost_idx);
			continue;
		}

                OBD_ALLOC(req, sizeof(*req));
                if (req == NULL)
                        GOTO(out_set, rc = -ENOMEM);

                req->rq_stripe = i;
                req->rq_idx = loi->loi_ost_idx;

                OBDO_ALLOC(req->rq_oi.oi_oa);
                if (req->rq_oi.oi_oa == NULL) {
                        OBD_FREE(req, sizeof(*req));
                        GOTO(out_set, rc = -ENOMEM);
                }
                memcpy(req->rq_oi.oi_oa, src_oa, sizeof(*req->rq_oi.oi_oa));
		req->rq_oi.oi_oa->o_oi = loi->loi_oi;
                lov_set_add_req(req, set);
        }
        if (!set->set_count)
                GOTO(out_set, rc = -EIO);
        *reqset = set;
        RETURN(rc);
out_set:
        lov_fini_destroy_set(set);
        RETURN(rc);
}

int lov_fini_setattr_set(struct lov_request_set *set)
{
        int rc = 0;
        ENTRY;

        if (set == NULL)
                RETURN(0);
        LASSERT(set->set_exp);
        if (cfs_atomic_read(&set->set_completes)) {
                rc = common_attr_done(set);
                /* FIXME update qos data here */
        }

        lov_put_reqset(set);
        RETURN(rc);
}

int lov_update_setattr_set(struct lov_request_set *set,
                           struct lov_request *req, int rc)
{
        struct lov_obd *lov = &req->rq_rqset->set_exp->exp_obd->u.lov;
        struct lov_stripe_md *lsm = req->rq_rqset->set_oi->oi_md;
        ENTRY;

        lov_update_set(set, req, rc);

        /* grace error on inactive ost */
        if (rc && !(lov->lov_tgts[req->rq_idx] &&
                    lov->lov_tgts[req->rq_idx]->ltd_active))
                rc = 0;

        if (rc == 0) {
                if (req->rq_oi.oi_oa->o_valid & OBD_MD_FLCTIME)
                        lsm->lsm_oinfo[req->rq_stripe]->loi_lvb.lvb_ctime =
                                req->rq_oi.oi_oa->o_ctime;
                if (req->rq_oi.oi_oa->o_valid & OBD_MD_FLMTIME)
                        lsm->lsm_oinfo[req->rq_stripe]->loi_lvb.lvb_mtime =
                                req->rq_oi.oi_oa->o_mtime;
                if (req->rq_oi.oi_oa->o_valid & OBD_MD_FLATIME)
                        lsm->lsm_oinfo[req->rq_stripe]->loi_lvb.lvb_atime =
                                req->rq_oi.oi_oa->o_atime;
        }

        RETURN(rc);
}

/* The callback for osc_setattr_async that finilizes a request info when a
 * response is received. */
static int cb_setattr_update(void *cookie, int rc)
{
        struct obd_info *oinfo = cookie;
        struct lov_request *lovreq;
        lovreq = container_of(oinfo, struct lov_request, rq_oi);
        return lov_update_setattr_set(lovreq->rq_rqset, lovreq, rc);
}

int lov_prep_setattr_set(struct obd_export *exp, struct obd_info *oinfo,
                         struct obd_trans_info *oti,
                         struct lov_request_set **reqset)
{
        struct lov_request_set *set;
        struct lov_obd *lov = &exp->exp_obd->u.lov;
        int rc = 0, i;
        ENTRY;

        OBD_ALLOC(set, sizeof(*set));
        if (set == NULL)
                RETURN(-ENOMEM);
        lov_init_set(set);

        set->set_exp = exp;
        set->set_oi = oinfo;
        if (oti != NULL && oinfo->oi_oa->o_valid & OBD_MD_FLCOOKIE)
                set->set_cookies = oti->oti_logcookies;

        for (i = 0; i < oinfo->oi_md->lsm_stripe_count; i++) {
		struct lov_oinfo *loi = oinfo->oi_md->lsm_oinfo[i];
		struct lov_request *req;

		if (!lov_check_and_wait_active(lov, loi->loi_ost_idx)) {
			CDEBUG(D_HA, "lov idx %d inactive\n", loi->loi_ost_idx);
			continue;
		}

                OBD_ALLOC(req, sizeof(*req));
                if (req == NULL)
                        GOTO(out_set, rc = -ENOMEM);
                req->rq_stripe = i;
                req->rq_idx = loi->loi_ost_idx;

                OBDO_ALLOC(req->rq_oi.oi_oa);
                if (req->rq_oi.oi_oa == NULL) {
                        OBD_FREE(req, sizeof(*req));
                        GOTO(out_set, rc = -ENOMEM);
                }
		memcpy(req->rq_oi.oi_oa, oinfo->oi_oa,
		       sizeof(*req->rq_oi.oi_oa));
		req->rq_oi.oi_oa->o_oi = loi->loi_oi;
		req->rq_oi.oi_oa->o_stripe_idx = i;
		req->rq_oi.oi_cb_up = cb_setattr_update;
		req->rq_oi.oi_capa = oinfo->oi_capa;

                if (oinfo->oi_oa->o_valid & OBD_MD_FLSIZE) {
                        int off = lov_stripe_offset(oinfo->oi_md,
                                                    oinfo->oi_oa->o_size, i,
                                                    &req->rq_oi.oi_oa->o_size);

                        if (off < 0 && req->rq_oi.oi_oa->o_size)
                                req->rq_oi.oi_oa->o_size--;

                        CDEBUG(D_INODE, "stripe %d has size "LPU64"/"LPU64"\n",
                               i, req->rq_oi.oi_oa->o_size,
                               oinfo->oi_oa->o_size);
                }
                lov_set_add_req(req, set);
        }
        if (!set->set_count)
                GOTO(out_set, rc = -EIO);
        *reqset = set;
        RETURN(rc);
out_set:
        lov_fini_setattr_set(set);
        RETURN(rc);
}

#define LOV_U64_MAX ((__u64)~0ULL)
#define LOV_SUM_MAX(tot, add)                                           \
        do {                                                            \
                if ((tot) + (add) < (tot))                              \
                        (tot) = LOV_U64_MAX;                            \
                else                                                    \
                        (tot) += (add);                                 \
        } while(0)

int lov_fini_statfs(struct obd_device *obd, struct obd_statfs *osfs,int success)
{
        ENTRY;

        if (success) {
                __u32 expected_stripes = lov_get_stripecnt(&obd->u.lov,
                                                           LOV_MAGIC, 0);
		if (osfs->os_files != LOV_U64_MAX)
			lov_do_div64(osfs->os_files, expected_stripes);
		if (osfs->os_ffree != LOV_U64_MAX)
			lov_do_div64(osfs->os_ffree, expected_stripes);

		spin_lock(&obd->obd_osfs_lock);
		memcpy(&obd->obd_osfs, osfs, sizeof(*osfs));
		obd->obd_osfs_age = cfs_time_current_64();
		spin_unlock(&obd->obd_osfs_lock);
		RETURN(0);
	}

	RETURN(-EIO);
}

int lov_fini_statfs_set(struct lov_request_set *set)
{
        int rc = 0;
        ENTRY;

        if (set == NULL)
                RETURN(0);

        if (cfs_atomic_read(&set->set_completes)) {
                rc = lov_fini_statfs(set->set_obd, set->set_oi->oi_osfs,
                                     cfs_atomic_read(&set->set_success));
        }
        lov_put_reqset(set);
        RETURN(rc);
}

void lov_update_statfs(struct obd_statfs *osfs, struct obd_statfs *lov_sfs,
                       int success)
{
        int shift = 0, quit = 0;
        __u64 tmp;

        if (success == 0) {
                memcpy(osfs, lov_sfs, sizeof(*lov_sfs));
        } else {
                if (osfs->os_bsize != lov_sfs->os_bsize) {
                        /* assume all block sizes are always powers of 2 */
                        /* get the bits difference */
                        tmp = osfs->os_bsize | lov_sfs->os_bsize;
                        for (shift = 0; shift <= 64; ++shift) {
                                if (tmp & 1) {
                                        if (quit)
                                                break;
                                        else
                                                quit = 1;
                                        shift = 0;
                                }
                                tmp >>= 1;
                        }
                }

                if (osfs->os_bsize < lov_sfs->os_bsize) {
                        osfs->os_bsize = lov_sfs->os_bsize;

                        osfs->os_bfree  >>= shift;
                        osfs->os_bavail >>= shift;
                        osfs->os_blocks >>= shift;
                } else if (shift != 0) {
                        lov_sfs->os_bfree  >>= shift;
                        lov_sfs->os_bavail >>= shift;
                        lov_sfs->os_blocks >>= shift;
                }
#ifdef MIN_DF
                /* Sandia requested that df (and so, statfs) only
                   returned minimal available space on
                   a single OST, so people would be able to
                   write this much data guaranteed. */
                if (osfs->os_bavail > lov_sfs->os_bavail) {
                        /* Presumably if new bavail is smaller,
                           new bfree is bigger as well */
                        osfs->os_bfree = lov_sfs->os_bfree;
                        osfs->os_bavail = lov_sfs->os_bavail;
                }
#else
                osfs->os_bfree += lov_sfs->os_bfree;
                osfs->os_bavail += lov_sfs->os_bavail;
#endif
                osfs->os_blocks += lov_sfs->os_blocks;
                /* XXX not sure about this one - depends on policy.
                 *   - could be minimum if we always stripe on all OBDs
                 *     (but that would be wrong for any other policy,
                 *     if one of the OBDs has no more objects left)
                 *   - could be sum if we stripe whole objects
                 *   - could be average, just to give a nice number
                 *
                 * To give a "reasonable" (if not wholly accurate)
                 * number, we divide the total number of free objects
                 * by expected stripe count (watch out for overflow).
                 */
                LOV_SUM_MAX(osfs->os_files, lov_sfs->os_files);
                LOV_SUM_MAX(osfs->os_ffree, lov_sfs->os_ffree);
        }
}

/* The callback for osc_statfs_async that finilizes a request info when a
 * response is received. */
static int cb_statfs_update(void *cookie, int rc)
{
        struct obd_info *oinfo = cookie;
        struct lov_request *lovreq;
        struct lov_request_set *set;
        struct obd_statfs *osfs, *lov_sfs;
        struct lov_obd *lov;
        struct lov_tgt_desc *tgt;
        struct obd_device *lovobd, *tgtobd;
        int success;
        ENTRY;

        lovreq = container_of(oinfo, struct lov_request, rq_oi);
        set = lovreq->rq_rqset;
        lovobd = set->set_obd;
        lov = &lovobd->u.lov;
        osfs = set->set_oi->oi_osfs;
        lov_sfs = oinfo->oi_osfs;
        success = cfs_atomic_read(&set->set_success);
        /* XXX: the same is done in lov_update_common_set, however
           lovset->set_exp is not initialized. */
        lov_update_set(set, lovreq, rc);
        if (rc)
                GOTO(out, rc);

        obd_getref(lovobd);
        tgt = lov->lov_tgts[lovreq->rq_idx];
        if (!tgt || !tgt->ltd_active)
                GOTO(out_update, rc);

        tgtobd = class_exp2obd(tgt->ltd_exp);
	spin_lock(&tgtobd->obd_osfs_lock);
	memcpy(&tgtobd->obd_osfs, lov_sfs, sizeof(*lov_sfs));
	if ((oinfo->oi_flags & OBD_STATFS_FROM_CACHE) == 0)
		tgtobd->obd_osfs_age = cfs_time_current_64();
	spin_unlock(&tgtobd->obd_osfs_lock);

out_update:
        lov_update_statfs(osfs, lov_sfs, success);
        obd_putref(lovobd);

out:
	if (set->set_oi->oi_flags & OBD_STATFS_PTLRPCD &&
	    lov_set_finished(set, 0)) {
		lov_statfs_interpret(NULL, set, set->set_count !=
				     cfs_atomic_read(&set->set_success));
	}

	RETURN(0);
}

int lov_prep_statfs_set(struct obd_device *obd, struct obd_info *oinfo,
                        struct lov_request_set **reqset)
{
        struct lov_request_set *set;
        struct lov_obd *lov = &obd->u.lov;
        int rc = 0, i;
        ENTRY;

        OBD_ALLOC(set, sizeof(*set));
        if (set == NULL)
                RETURN(-ENOMEM);
        lov_init_set(set);

        set->set_obd = obd;
        set->set_oi = oinfo;

        /* We only get block data from the OBD */
        for (i = 0; i < lov->desc.ld_tgt_count; i++) {
		struct lov_request *req;

		if (lov->lov_tgts[i] == NULL ||
		    (oinfo->oi_flags & OBD_STATFS_NODELAY &&
		     !lov->lov_tgts[i]->ltd_active)) {
			CDEBUG(D_HA, "lov idx %d inactive\n", i);
			continue;
		}

		if (!lov->lov_tgts[i]->ltd_active)
			lov_check_and_wait_active(lov, i);

                /* skip targets that have been explicitely disabled by the
                 * administrator */
                if (!lov->lov_tgts[i]->ltd_exp) {
                        CDEBUG(D_HA, "lov idx %d administratively disabled\n", i);
                        continue;
                }

                OBD_ALLOC(req, sizeof(*req));
                if (req == NULL)
                        GOTO(out_set, rc = -ENOMEM);

                OBD_ALLOC(req->rq_oi.oi_osfs, sizeof(*req->rq_oi.oi_osfs));
                if (req->rq_oi.oi_osfs == NULL) {
                        OBD_FREE(req, sizeof(*req));
                        GOTO(out_set, rc = -ENOMEM);
                }

                req->rq_idx = i;
                req->rq_oi.oi_cb_up = cb_statfs_update;
                req->rq_oi.oi_flags = oinfo->oi_flags;

                lov_set_add_req(req, set);
        }
        if (!set->set_count)
                GOTO(out_set, rc = -EIO);
        *reqset = set;
        RETURN(rc);
out_set:
        lov_fini_statfs_set(set);
        RETURN(rc);
}
