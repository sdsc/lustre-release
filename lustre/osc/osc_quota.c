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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2011 Whamcloud, Inc.
 *
 * Code originally extracted from quota directory
 */
#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <obd_ost.h>
#include "osc_internal.h"

#if !defined(__KERNEL__)
# define rcu_read_lock()   cfs_spin_lock(&cli->cl_quota_lock[type])
# define rcu_read_unlock() cfs_spin_unlock(&cli->cl_quota_lock[type])
#endif

static inline struct osc_quota_info *osc_oqi_alloc(obd_uid id)
{
        struct osc_quota_info *oqi;

        OBD_SLAB_ALLOC_PTR(oqi, osc_quota_kmem);
        if (oqi)
                oqi->oqi_id = id;
        return oqi;
}

static void osc_oqi_free_cb(cfs_rcu_head_t *rcu)
{
        struct osc_quota_info *oqi;

        oqi = container_of(rcu, struct osc_quota_info, oqi_rcu);
        OBD_SLAB_FREE_PTR(oqi, osc_quota_kmem);
}

static inline void osc_oqi_free(struct osc_quota_info *oqi)
{
        cfs_call_rcu(&oqi->oqi_rcu, osc_oqi_free_cb);
}

int osc_quota_chkdq(struct client_obd *cli, const unsigned int qid[])
{
        int type;
        ENTRY;

        for (type = 0; type < MAXQUOTAS; type++) {
                struct osc_quota_info *oqi;

                /* look-up the id in the per-type radix tree */
                rcu_read_lock();
                oqi = radix_tree_lookup(&cli->cl_quota_ids[type], qid[type]);
                if (oqi) {
                        obd_uid id = oqi->oqi_id;

                        rcu_read_unlock();
                        LASSERTF(id == qid[type],
                                 "The ids don't match %u != %u\n", qid[type],
                                 id);
                        /* the slot is busy, the user is about to run out of
                         * quota space on this OST */
                        CDEBUG(D_QUOTA, "chkdq found noquota for %s %d\n",
                               type == USRQUOTA ? "user" : "group", qid[type]);
                        RETURN(NO_QUOTA);
                }
                rcu_read_unlock();
        }
        RETURN(QUOTA_OK);
}

#define MD_QUOTA_FLAG(type) (type == USRQUOTA) ? OBD_MD_FLUSRQUOTA \
                                               : OBD_MD_FLGRPQUOTA
#define FL_QUOTA_FLAG(type) (type == USRQUOTA) ? OBD_FL_NO_USRQUOTA \
                                               : OBD_FL_NO_GRPQUOTA

int osc_quota_setdq(struct client_obd *cli, const unsigned int qid[],
                    obd_flag valid, obd_flag flags)
{
        int type;
        int rc = 0;
        ENTRY;

        if ((valid & (OBD_MD_FLUSRQUOTA | OBD_MD_FLGRPQUOTA)) == 0)
                RETURN(0);

        for (type = 0; type < MAXQUOTAS; type++) {
                struct osc_quota_info *oqi;
                obd_flag               noquota;

                if ((valid & MD_QUOTA_FLAG(type)) == 0)
                        continue;

                noquota = !!(flags & FL_QUOTA_FLAG(type));
                if (noquota) {
                        /* This id is getting close to its quota limit, let's
                         * switch to sync i/o */
                        oqi = osc_oqi_alloc(qid[type]);
                        if (oqi == NULL) {
                                rc = -ENOMEM;
                                break;
                        }
                        rc = cfs_radix_tree_preload(CFS_ALLOC_IO);
                        if (rc) {
                                osc_oqi_free(oqi);
                                break;
                        }

                        cfs_spin_lock(&cli->cl_quota_lock[type]);
                        /* might fail with -EEXIT, doesn't matter */
                        rc = radix_tree_insert(&cli->cl_quota_ids[type],
                                               qid[type], &oqi);
                        cfs_spin_unlock(&cli->cl_quota_lock[type]);

                        radix_tree_preload_end();
                        if (rc)
                                osc_oqi_free(oqi);

                        CDEBUG(D_QUOTA, "setdq to insert for %s %d (%d)\n",
                               type == USRQUOTA ? "user" : "group", qid[type],
                               rc);
                } else { /* !noquota */
                        /* This id is now off the hook, let's remove it from
                         * the radix tree */
                        cfs_spin_lock(&cli->cl_quota_lock[type]);
                        oqi = radix_tree_delete(&cli->cl_quota_ids[type],
                                                qid[type]);
                        cfs_spin_unlock(&cli->cl_quota_lock[type]);

                        if (oqi)
                                osc_oqi_free(oqi);

                        CDEBUG(D_QUOTA, "setdq to remove for %s %d\n",
                               type == USRQUOTA ? "user" : "group", qid[type]);
                }
        }
        RETURN(rc);
}

int osc_quota_setup(struct obd_device *obd)
{
        struct client_obd *cli = &obd->u.cli;
        int                type;
        ENTRY;

        for (type = 0; type < MAXQUOTAS; type++) {
                cfs_spin_lock_init(&cli->cl_quota_lock[type]);
                INIT_RADIX_TREE(&cli->cl_quota_ids[type], GFP_ATOMIC);
        }

        RETURN(0);
}

int osc_quota_cleanup(struct obd_device *obd)
{
        struct client_obd *cli = &obd->u.cli;
        int                type;
        ENTRY;

        for (type = 0; type < MAXQUOTAS; type++) {
                struct osc_quota_info *oqi;
                void                  *item; /* to make gcc happy */

                cfs_spin_lock(&cli->cl_quota_lock[type]);
                while (radix_tree_gang_lookup(&cli->cl_quota_ids[type],
                                              &item, 0, 1) > 0) {
                        oqi = (struct osc_quota_info *)item;
                        oqi = radix_tree_delete(&cli->cl_quota_ids[type],
                                                oqi->oqi_id);
                        if (oqi)
                                osc_oqi_free(oqi);
                }
                cfs_spin_unlock(&cli->cl_quota_lock[type]);
#ifdef __KERNEL__
                synchronize_rcu();
#endif
                LASSERT(cli->cl_quota_ids[type].rnode == NULL);
        }

        RETURN(0);
}

int osc_quotactl(struct obd_device *unused, struct obd_export *exp,
                 struct obd_quotactl *oqctl)
{
        struct ptlrpc_request *req;
        struct obd_quotactl   *oqc;
        int                    rc;
        ENTRY;

        req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
                                        &RQF_OST_QUOTACTL, LUSTRE_OST_VERSION,
                                        OST_QUOTACTL);
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
        ptlrpc_req_finished(req);

        RETURN(rc);
}

int osc_quotacheck(struct obd_device *unused, struct obd_export *exp,
                   struct obd_quotactl *oqctl)
{
        struct client_obd       *cli = &exp->exp_obd->u.cli;
        struct ptlrpc_request   *req;
        struct obd_quotactl     *body;
        int                      rc;
        ENTRY;

        req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
                                        &RQF_OST_QUOTACHECK, LUSTRE_OST_VERSION,
                                        OST_QUOTACHECK);
        if (req == NULL)
                RETURN(-ENOMEM);

        body = req_capsule_client_get(&req->rq_pill, &RMF_OBD_QUOTACTL);
        *body = *oqctl;

        ptlrpc_request_set_replen(req);

        /* the next poll will find -ENODATA, that means quotacheck is
         * going on */
        cli->cl_qchk_stat = -ENODATA;
        rc = ptlrpc_queue_wait(req);
        if (rc)
                cli->cl_qchk_stat = rc;
        ptlrpc_req_finished(req);
        RETURN(rc);
}

int osc_quota_poll_check(struct obd_export *exp, struct if_quotacheck *qchk)
{
        struct client_obd *cli = &exp->exp_obd->u.cli;
        int rc;
        ENTRY;

        qchk->obd_uuid = cli->cl_target_uuid;
        memcpy(qchk->obd_type, LUSTRE_OST_NAME, strlen(LUSTRE_OST_NAME));

        rc = cli->cl_qchk_stat;
        /* the client is not the previous one */
        if (rc == CL_NOT_QUOTACHECKED)
                rc = -EINTR;
        RETURN(rc);
}

int osc_quota_adjust_qunit(struct obd_export *exp,
                           struct quota_adjust_qunit *oqaq,
                           struct lustre_quota_ctxt *qctxt,
                           struct ptlrpc_request_set *rqset)
{
        struct ptlrpc_request     *req;
        struct quota_adjust_qunit *oqa;
        int                        rc = 0;
        ENTRY;

        /* client don't support this kind of operation, abort it */
        if (!(exp->exp_connect_flags & OBD_CONNECT_CHANGE_QS)) {
                CDEBUG(D_QUOTA, "osc: %s don't support change qunit size\n",
                       exp->exp_obd->obd_name);
                RETURN(rc);
        }
        if (strcmp(exp->exp_obd->obd_type->typ_name, LUSTRE_OSC_NAME))
                RETURN(-EINVAL);

        LASSERT(rqset);

        req = ptlrpc_request_alloc_pack(class_exp2cliimp(exp),
                                        &RQF_OST_QUOTA_ADJUST_QUNIT,
                                        LUSTRE_OST_VERSION,
                                        OST_QUOTA_ADJUST_QUNIT);
        if (req == NULL)
                RETURN(-ENOMEM);

        oqa = req_capsule_client_get(&req->rq_pill, &RMF_QUOTA_ADJUST_QUNIT);
        *oqa = *oqaq;

        ptlrpc_request_set_replen(req);

        ptlrpc_set_add_req(rqset, req);
        RETURN(rc);
}
