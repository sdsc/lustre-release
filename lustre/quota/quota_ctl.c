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
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
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
# include <linux/smp_lock.h>
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

/* When quotaon, build a lqs for every uid/gid who has been set limitation
 * for quota. After quota_search_lqs, it will hold one ref for the lqs.
 * It will be released when qctxt_cleanup() is executed b=18574 */
void build_lqs(struct obd_device *obd)
{
        struct lustre_quota_ctxt *qctxt = &obd->u.obt.obt_qctxt;
        struct list_head id_list;
        int i, rc;

        INIT_LIST_HEAD(&id_list);
        for (i = 0; i < MAXQUOTAS; i++) {
                struct dquot_id *dqid, *tmp;

#ifndef KERNEL_SUPPORTS_QUOTA_READ
                rc = fsfilt_qids(obd, sb_dqopt(qctxt->lqc_sb)->files[i], NULL,
                                 i, &id_list);
#else
                rc = fsfilt_qids(obd, NULL, sb_dqopt(qctxt->lqc_sb)->files[i],
                                 i, &id_list);
#endif
                if (rc) {
                        CDEBUG(D_ERROR, "fail to get %s qids!\n",
                               i ? "group" : "user");
                        continue;
                }

                list_for_each_entry_safe(dqid, tmp, &id_list,
                                         di_link) {
                        struct lustre_qunit_size *lqs;

                        list_del_init(&dqid->di_link);
                        lqs = quota_search_lqs(LQS_KEY(i, dqid->di_id),
                                               qctxt, 1);
                        if (lqs && !IS_ERR(lqs)) {
                                lqs->lqs_flags |= dqid->di_flag;
                                lqs_putref(lqs);
                        } else {
                                CDEBUG(D_ERROR, "fail to create a lqs"
                                       "(%s id: %u)!\n", i ? "group" : "user",
                                       dqid->di_id);
                        }

                        OBD_FREE_PTR(dqid);
                }
        }
}

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

        do_gettimeofday(&work_start);
        switch (oqctl->qc_cmd) {
        case Q_QUOTAON:
                oqctl->qc_id = obt->obt_qfmt; /* override qfmt version */
                rc = mds_quota_on(obd, oqctl);
                /* when quotaon, create lqs for every quota uid/gid b=18574 */
                build_lqs(obd);
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
        do_gettimeofday(&work_end);
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
        struct lustre_quota_ctxt *qctxt = &obd->u.obt.obt_qctxt;
        struct lustre_qunit_size *lqs;
        void *handle = NULL;
        struct timeval work_start;
        struct timeval work_end;
        long timediff;
        int rc = 0;
        ENTRY;

        do_gettimeofday(&work_start);
        switch (oqctl->qc_cmd) {
        case Q_FINVALIDATE:
        case Q_QUOTAON:
        case Q_QUOTAOFF:
                down(&obt->obt_quotachecking);
                if (oqctl->qc_cmd == Q_FINVALIDATE &&
                    (obt->obt_qctxt.lqc_flags & UGQUOTA2LQC(oqctl->qc_type))) {
                        CWARN("quota[%u] is on yet\n", oqctl->qc_type);
                        up(&obt->obt_quotachecking);
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

                if (oqctl->qc_cmd == Q_QUOTAON || oqctl->qc_cmd == Q_QUOTAOFF ||
                    oqctl->qc_cmd == Q_FINVALIDATE) {
                        if (oqctl->qc_cmd == Q_QUOTAON) {
                                if (!rc)
                                        obt->obt_qctxt.lqc_flags |=
                                                UGQUOTA2LQC(oqctl->qc_type);
                                else if (rc == -EBUSY &&
                                         quota_is_on(qctxt, oqctl))
                                                rc = -EALREADY;
                        } else if (oqctl->qc_cmd == Q_QUOTAOFF) {
                                if (!rc)
                                        obt->obt_qctxt.lqc_flags &=
                                                ~UGQUOTA2LQC(oqctl->qc_type);
                                else if (quota_is_off(qctxt, oqctl))
                                                rc = -EALREADY;
                        }
                        up(&obt->obt_quotachecking);
                }

                /* when quotaon, create lqs for every quota uid/gid b=18574 */
                if (oqctl->qc_cmd == Q_QUOTAON)
                        build_lqs(obd);
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
                        CDEBUG(D_ERROR, "fail to create lqs when setquota\n");
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
                        CDEBUG(D_ERROR, "fail to create lqs when setquota\n");
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
        do_gettimeofday(&work_end);
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

        rc = ptlrpc_queue_wait(req);
        if (rc) {
                CERROR("ptlrpc_queue_wait failed, rc: %d\n", rc);
                GOTO(out, rc);
        }

        oqc = NULL;
        if (req->rq_repmsg)
                oqc = req_capsule_server_get(&req->rq_pill, &RMF_OBD_QUOTACTL);

        if (oqc == NULL) {
                CERROR ("Can't unpack obd_quotactl\n");
                GOTO(out, rc = -EPROTO);
        }

        *oqctl = *oqc;
        EXIT;
out:
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
        int rc;
        ENTRY;

        if (!lmv->desc.ld_tgt_count || !tgt->ltd_active) {
                CERROR("master lmv inactive\n");
                RETURN(-EIO);
        }

        rc = obd_quotactl(tgt->ltd_exp, oqctl);
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
                if (!tgt || !tgt->ltd_active || tgt->ltd_reap) {
                        if (oqctl->qc_cmd == Q_GETOQUOTA) {
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
