/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * Copyright (c) 2011, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */
#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LQUOTA

#ifdef __KERNEL__
# include <linux/version.h>
# include <linux/module.h>
# include <linux/init.h>
# include <linux/fs.h>
# include <linux/jbd.h>
# include <linux/quota.h>
# include <linux/buffer_head.h>
# include <linux/workqueue.h>
# include <linux/mount.h>
#else /* __KERNEL__ */
# include <liblustre.h>
#endif

#include <obd_class.h>
#include <lustre_mds.h>
#include <lustre_dlm.h>
#include <lustre_cfg.h>
#include <obd_ost.h>
#include <lustre_fsfilt.h>
#include <lustre_quota.h>
#include "quota_internal.h"

#ifdef HAVE_QUOTA_SUPPORT
#ifdef __KERNEL__

int mds_quota_ctl(struct obd_device *obd, struct obd_export *unused,
                  struct obd_quotactl *oqctl)
{
        struct obd_device_target *obt = &obd->u.obt;
        struct lustre_quota_ctxt *qctxt = &obd->u.obt.obt_qctxt;
        struct timeval work_start;
        struct timeval work_end;
        long timediff;
        int rc = 0;
        ENTRY;

        cfs_gettimeofday(&work_start);
        switch (oqctl->qc_cmd) {
        case Q_QUOTAON:
                oqctl->qc_id = obt->obt_qfmt; /* override qfmt version */
                rc = mds_quota_on(obd, oqctl);
                break;
        case Q_QUOTAOFF:
                oqctl->qc_id = obt->obt_qfmt; /* override qfmt version */
                rc = mds_quota_off(obd, oqctl);
                break;
        case Q_SETINFO:
                rc = mds_set_dqinfo(obd, oqctl);
                break;
        case Q_GETINFO:
                rc = mds_get_dqinfo(obd, oqctl);
                break;
        case Q_SETQUOTA:
                rc = mds_set_dqblk(obd, oqctl);
                break;
        case Q_GETQUOTA:
                rc = mds_get_dqblk(obd, oqctl);
                break;
        case Q_GETOINFO:
        case Q_GETOQUOTA:
                rc = mds_get_obd_quota(obd, oqctl);
                break;
        case LUSTRE_Q_INVALIDATE:
                rc = mds_quota_invalidate(obd, oqctl);
                break;
        case LUSTRE_Q_FINVALIDATE:
                oqctl->qc_id = obt->obt_qfmt; /* override qfmt version */
                rc = mds_quota_finvalidate(obd, oqctl);
                break;
        default:
                CERROR("%s: unsupported mds_quotactl command: %d\n",
                       obd->obd_name, oqctl->qc_cmd);
                RETURN(-EFAULT);
        }

        if (rc)
                CDEBUG(D_INFO, "mds_quotactl admin quota command %d, id %u, "
                               "type %d, failed: rc = %d\n",
                       oqctl->qc_cmd, oqctl->qc_id, oqctl->qc_type, rc);
        cfs_gettimeofday(&work_end);
        timediff = cfs_timeval_sub(&work_end, &work_start, NULL);
        lprocfs_counter_add(qctxt->lqc_stats, LQUOTA_QUOTA_CTL, timediff);

        RETURN(rc);
}

int filter_quota_ctl(struct obd_device *unused, struct obd_export *exp,
                     struct obd_quotactl *oqctl)
{
        struct obd_device *obd = exp->exp_obd;
        struct obd_device_target *obt = &obd->u.obt;
        struct lvfs_run_ctxt saved;
        struct lustre_quota_ctxt *qctxt = &obt->obt_qctxt;
        struct lustre_qunit_size *lqs;
        void *handle = NULL;
        struct timeval work_start;
        struct timeval work_end;
        long timediff;
        int rc = 0;
        ENTRY;

        cfs_gettimeofday(&work_start);
        switch (oqctl->qc_cmd) {
        case Q_QUOTAON:
                oqctl->qc_id = obt->obt_qfmt;
                rc = generic_quota_on(obd, oqctl, 0);
                break;
        case Q_FINVALIDATE:
        case Q_QUOTAOFF:
                cfs_down(&obt->obt_quotachecking);
                if (oqctl->qc_cmd == Q_FINVALIDATE &&
                    (obt->obt_qctxt.lqc_flags & UGQUOTA2LQC(oqctl->qc_type))) {
                        CWARN("quota[%u] is on yet\n", oqctl->qc_type);
                        cfs_up(&obt->obt_quotachecking);
                        rc = -EBUSY;
                        break;
                }
                oqctl->qc_id = obt->obt_qfmt; /* override qfmt version */
        case Q_GETOINFO:
        case Q_GETOQUOTA:
        case Q_GETQUOTA:
                /* In recovery scenario, this pending dqacq/dqrel might have
                 * been processed by master successfully before it's dquot
                 * on master enter recovery mode. We must wait for this
                 * dqacq/dqrel done then return the correct limits to master */
                if (oqctl->qc_stat == QUOTA_RECOVERING)
                        handle = quota_barrier(&obd->u.obt.obt_qctxt, oqctl, 1);

                push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                rc = fsfilt_quotactl(obd, obt->obt_sb, oqctl);
                pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);

                if (oqctl->qc_stat == QUOTA_RECOVERING)
                        quota_unbarrier(handle);

                if (oqctl->qc_cmd == Q_QUOTAOFF ||
                    oqctl->qc_cmd == Q_FINVALIDATE) {
                        if (oqctl->qc_cmd == Q_QUOTAOFF) {
                                if (!rc)
                                        obt->obt_qctxt.lqc_flags &=
                                                ~UGQUOTA2LQC(oqctl->qc_type);
                                else if (quota_is_off(qctxt, oqctl))
                                                rc = -EALREADY;
                                CDEBUG(D_QUOTA, "%s: quotaoff type:flags:rc "
                                       "%u:%lu:%d\n", obd->obd_name,
                                       oqctl->qc_type, qctxt->lqc_flags, rc);
                        }
                        cfs_up(&obt->obt_quotachecking);
                }

                break;
        case Q_SETQUOTA:
                /* currently, it is only used for nullifying the quota */
                handle = quota_barrier(&obd->u.obt.obt_qctxt, oqctl, 1);

                push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                rc = fsfilt_quotactl(obd, obd->u.obt.obt_sb, oqctl);

                if (!rc) {
                        oqctl->qc_cmd = Q_SYNC;
                        fsfilt_quotactl(obd, obd->u.obt.obt_sb, oqctl);
                        oqctl->qc_cmd = Q_SETQUOTA;
                }
                pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                quota_unbarrier(handle);

                lqs = quota_search_lqs(LQS_KEY(oqctl->qc_type, oqctl->qc_id),
                                       qctxt, 0);
                if (lqs == NULL || IS_ERR(lqs)){
                        CERROR("fail to create lqs during setquota operation "
                               "for %sid %u\n", oqctl->qc_type ? "g" : "u",
                               oqctl->qc_id);
                } else {
                        lqs->lqs_flags &= ~QB_SET;
                        lqs_putref(lqs);
                }

                break;
        case Q_INITQUOTA:
                {
                unsigned int id[MAXQUOTAS] = { 0, 0 };

                /* Initialize quota limit to MIN_QLIMIT */
                LASSERT(oqctl->qc_dqblk.dqb_valid == QIF_BLIMITS);
                LASSERT(oqctl->qc_dqblk.dqb_bsoftlimit == 0);

                if (!oqctl->qc_dqblk.dqb_bhardlimit)
                        goto adjust;

               /* There might be a pending dqacq/dqrel (which is going to
                 * clear stale limits on slave). we should wait for it's
                 * completion then initialize limits */
                handle = quota_barrier(&obd->u.obt.obt_qctxt, oqctl, 1);
                LASSERT(oqctl->qc_dqblk.dqb_bhardlimit == MIN_QLIMIT);
                push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                rc = fsfilt_quotactl(obd, obd->u.obt.obt_sb, oqctl);

                /* Update on-disk quota, in case of lose the changed limits
                 * (MIN_QLIMIT) on crash, which cannot be recovered.*/
                if (!rc) {
                        oqctl->qc_cmd = Q_SYNC;
                        fsfilt_quotactl(obd, obd->u.obt.obt_sb, oqctl);
                        oqctl->qc_cmd = Q_INITQUOTA;
                }
                pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                quota_unbarrier(handle);

                if (rc)
                        RETURN(rc);
adjust:
                lqs = quota_search_lqs(LQS_KEY(oqctl->qc_type, oqctl->qc_id),
                                       qctxt, 1);
                if (lqs == NULL || IS_ERR(lqs)){
                        CERROR("fail to create lqs during setquota operation "
                               "for %sid %u\n", oqctl->qc_type ? "g" : "u",
                               oqctl->qc_id);
                        break;
                } else {
                        lqs->lqs_flags |= QB_SET;
                        lqs_putref(lqs);
                }

                /* Trigger qunit pre-acquire */
                if (oqctl->qc_type == USRQUOTA)
                        id[USRQUOTA] = oqctl->qc_id;
                else
                        id[GRPQUOTA] = oqctl->qc_id;

                rc = qctxt_adjust_qunit(obd, &obd->u.obt.obt_qctxt,
                                        id, 1, 0, NULL);
                if (rc == -EDQUOT || rc == -EBUSY) {
                        CDEBUG(D_QUOTA, "rc: %d.\n", rc);
                        rc = 0;
                }

                break;
                }
        default:
                CERROR("%s: unsupported filter_quotactl command: %d\n",
                       obd->obd_name, oqctl->qc_cmd);
                RETURN(-EFAULT);
        }
        cfs_gettimeofday(&work_end);
        timediff = cfs_timeval_sub(&work_end, &work_start, NULL);
        lprocfs_counter_add(qctxt->lqc_stats, LQUOTA_QUOTA_CTL, timediff);

        RETURN(rc);
}
#endif /* __KERNEL__ */
#endif

int client_quota_ctl(struct obd_device *unused, struct obd_export *exp,
                     struct obd_quotactl *oqctl)
{
        struct ptlrpc_request   *req;
        struct obd_quotactl     *oqc;
        const struct req_format *rf;
        int                      ver, opc, rc;
        ENTRY;

        if (!strcmp(exp->exp_obd->obd_type->typ_name, LUSTRE_MDC_NAME)) {
                rf  = &RQF_MDS_QUOTACTL;
                ver = LUSTRE_MDS_VERSION,
                opc = MDS_QUOTACTL;
        } else if (!strcmp(exp->exp_obd->obd_type->typ_name, LUSTRE_OSC_NAME)) {
                rf  = &RQF_OST_QUOTACTL;
                ver = LUSTRE_OST_VERSION,
                opc = OST_QUOTACTL;
        } else {
                RETURN(-EINVAL);
        }

        req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp), rf, ver, opc);
        if (req == NULL)
                RETURN(-ENOMEM);

        oqc = req_capsule_client_get(&req->rq_pill, &RMF_OBD_QUOTACTL);
        *oqc = *oqctl;

        ptlrpc_request_set_replen(req);
        ptlrpc_at_set_req_timeout(req);
        req->rq_no_resend = 1;

        rc = ptlrpc_queue_wait(req);
        if (rc)
                CERROR("ptlrpc_queue_wait failed, rc: %d\n", rc);

        if (req->rq_repmsg &&
            (oqc = req_capsule_server_get(&req->rq_pill, &RMF_OBD_QUOTACTL))) {
                *oqctl = *oqc;
        } else if (!rc) {
                CERROR ("Can't unpack obd_quotactl\n");
                rc = -EPROTO;
        }

        EXIT;

        ptlrpc_req_finished(req);

        return rc;
}

/**
 * For lmv, only need to send request to master MDT, and the master MDT will
 * process with other slave MDTs.
 */
int lmv_quota_ctl(struct obd_device *unused, struct obd_export *exp,
                  struct obd_quotactl *oqctl)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_tgt_desc *tgt = &lmv->tgts[0];
        int rc = 0, i;
        __u64 curspace;
        __u64 curinodes;
        ENTRY;

        if (!lmv->desc.ld_tgt_count || !tgt->ltd_active) {
                CERROR("master lmv inactive\n");
                RETURN(-EIO);
        }

        if (oqctl->qc_cmd != Q_GETOQUOTA) {
                rc = obd_quotactl(tgt->ltd_exp, oqctl);
                RETURN(rc);
        }

        curspace = curinodes = 0;
        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                int err;
                tgt = &lmv->tgts[i];

                if (tgt->ltd_exp == NULL)
                        continue;
                if (!tgt->ltd_active) {
                        CDEBUG(D_HA, "mdt %d is inactive.\n", i);
                        continue;
                }

                err = obd_quotactl(tgt->ltd_exp, oqctl);
                if (err) {
                        CERROR("getquota on mdt %d failed. %d\n", i, err);
                        if (!rc)
                                rc = err;
                } else {
                        curspace += oqctl->qc_dqblk.dqb_curspace;
                        curinodes += oqctl->qc_dqblk.dqb_curinodes;
                }
        }
        oqctl->qc_dqblk.dqb_curspace = curspace;
        oqctl->qc_dqblk.dqb_curinodes = curinodes;

        RETURN(rc);
}

int lov_quota_ctl(struct obd_device *unused, struct obd_export *exp,
                  struct obd_quotactl *oqctl)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lov_obd *lov = &obd->u.lov;
        struct lov_tgt_desc *tgt;
        __u64 curspace = 0;
        __u64 bhardlimit = 0;
        int i, rc = 0;
        ENTRY;

        if (oqctl->qc_cmd != LUSTRE_Q_QUOTAON &&
            oqctl->qc_cmd != LUSTRE_Q_QUOTAOFF &&
            oqctl->qc_cmd != Q_GETOQUOTA &&
            oqctl->qc_cmd != Q_INITQUOTA &&
            oqctl->qc_cmd != LUSTRE_Q_SETQUOTA &&
            oqctl->qc_cmd != Q_FINVALIDATE) {
                CERROR("bad quota opc %x for lov obd", oqctl->qc_cmd);
                RETURN(-EFAULT);
        }

        /* for lov tgt */
        obd_getref(obd);
        for (i = 0; i < lov->desc.ld_tgt_count; i++) {
                int err;

                tgt = lov->lov_tgts[i];

                if (!tgt)
                        continue;

                if (!tgt->ltd_active || tgt->ltd_reap) {
                        if (oqctl->qc_cmd == Q_GETOQUOTA &&
                            lov->lov_tgts[i]->ltd_activate) {
                                rc = -EREMOTEIO;
                                CERROR("ost %d is inactive\n", i);
                        } else {
                                CDEBUG(D_HA, "ost %d is inactive\n", i);
                        }
                        continue;
                }

                err = obd_quotactl(tgt->ltd_exp, oqctl);
                if (err) {
                        if (tgt->ltd_active && !rc)
                                rc = err;
                        continue;
                }

                if (oqctl->qc_cmd == Q_GETOQUOTA) {
                        curspace += oqctl->qc_dqblk.dqb_curspace;
                        bhardlimit += oqctl->qc_dqblk.dqb_bhardlimit;
                }
        }
        obd_putref(obd);

        if (oqctl->qc_cmd == Q_GETOQUOTA) {
                oqctl->qc_dqblk.dqb_curspace = curspace;
                oqctl->qc_dqblk.dqb_bhardlimit = bhardlimit;
        }
        RETURN(rc);
}
