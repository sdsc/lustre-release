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
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _LMV_INTERNAL_H_
#define _LMV_INTERNAL_H_

#include <lustre/lustre_idl.h>
#include <obd.h>

#define LMV_MAX_TGT_COUNT 128

#define lmv_init_lock(lmv)   cfs_down(&lmv->init_sem);
#define lmv_init_unlock(lmv) cfs_up(&lmv->init_sem);

#define LL_IT2STR(it)				        \
	((it) ? ldlm_it2str((it)->it_op) : "0")

int lmv_check_connect(struct obd_device *obd);

int lmv_intent_lock(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
                    ldlm_blocking_callback cb_blocking,
                    int extra_lock_flags);

int lmv_intent_lookup(struct obd_export *exp, struct md_op_data *op_data,
                      void *lmm, int lmmsize, struct lookup_intent *it,
                      int flags, struct ptlrpc_request **reqp,
                      ldlm_blocking_callback cb_blocking,
                      int extra_lock_flags);

int lmv_intent_open(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
                    ldlm_blocking_callback cb_blocking,
                    int extra_lock_flags);

struct lmv_stripe_md *lmv_get_lsm_from_req(struct obd_export *exp,
                                           struct ptlrpc_request *req);

int lmv_allocate_slaves(struct obd_device *obd, struct lu_fid *pid,
                        struct md_op_data *op, struct lu_fid *fid);

int lmv_revalidate_slaves(struct obd_export *, struct ptlrpc_request **,
                          struct lmv_stripe_md *, struct lookup_intent *,
                          int, ldlm_blocking_callback cb_blocking,
                          int extra_lock_flags);

int lmv_handle_split(struct obd_export *, const struct lu_fid *);
int lmv_blocking_ast(struct ldlm_lock *, struct ldlm_lock_desc *,
		     void *, int);
int lmv_fld_lookup(struct lmv_obd *lmv, const struct lu_fid *fid,
                   mdsno_t *mds);
int __lmv_fid_alloc(struct lmv_obd *lmv, struct lu_fid *fid,
                    mdsno_t mds);
int lmv_fid_alloc(struct obd_export *exp, struct lu_fid *fid,
                  struct md_op_data *op_data);

static inline struct lmv_stripe_md *lmv_get_mea(struct ptlrpc_request *req)
{
        struct mdt_body         *body;
        struct lmv_stripe_md    *mea;

        LASSERT(req != NULL);

        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);

        if (!body || !S_ISDIR(body->mode) || !body->eadatasize)
                return NULL;

        mea = req_capsule_server_sized_get(&req->rq_pill, &RMF_MDT_MD,
                                           body->eadatasize);
        LASSERT(mea != NULL);

        if (mea->mea_count == 0)
                return NULL;
        if( mea->mea_magic != MEA_MAGIC_LAST_CHAR &&
                mea->mea_magic != MEA_MAGIC_ALL_CHARS &&
                mea->mea_magic != MEA_MAGIC_HASH_SEGMENT)
                return NULL;

        return mea;
}

static inline int lmv_get_easize(struct lmv_obd *lmv)
{
        return sizeof(struct lmv_stripe_md) +
                lmv->desc.ld_tgt_count *
                sizeof(struct lu_fid);
}

static inline struct lmv_tgt_desc *
lmv_get_target(struct lmv_obd *lmv, mdsno_t mds)
{
        return &lmv->tgts[mds];
}

static inline struct lmv_tgt_desc *
lmv_find_target(struct lmv_obd *lmv, const struct lu_fid *fid)
{
        mdsno_t mds = 0;
        int rc;

        if (lmv->desc.ld_tgt_count > 1) {
                rc = lmv_fld_lookup(lmv, fid, &mds);
                if (rc)
                        return ERR_PTR(rc);
        }

        return lmv_get_target(lmv, mds);
}

struct lmv_tgt_desc
*lmv_locate_mds(struct lmv_obd *lmv, struct md_op_data *op_data,
                struct lmv_stripe_md *lsm, struct lu_fid *fid);

struct lmv_stripe_md *lmv_get_lsm_from_req(struct obd_export *exp,
                                           struct ptlrpc_request *req);

int lmv_unpack_md(struct obd_export *exp, struct lmv_stripe_md **lsmp,
                  struct lmv_mds_md *lmm, int stripe_count);


/* lproc_lmv.c */
#ifdef LPROCFS
void lprocfs_lmv_init_vars(struct lprocfs_static_vars *lvars);
#else
static inline void lprocfs_lmv_init_vars(struct lprocfs_static_vars *lvars)
{
        memset(lvars, 0, sizeof(*lvars));
}
#endif
extern struct file_operations lmv_proc_target_fops;

#endif
