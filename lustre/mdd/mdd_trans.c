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
 * Copyright (c) 2011 Whamcloud, Inc.
 *
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdd/mdd_trans.c
 *
 * Lustre Metadata Server (mdd) routines
 *
 * Author: Wang Di <wangdi@clusterfs.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>
#ifdef HAVE_EXT4_LDISKFS
#include <ldiskfs/ldiskfs_jbd2.h>
#else
#include <linux/jbd.h>
#endif
#include <obd.h>
#include <obd_class.h>
#include <lustre_ver.h>
#include <obd_support.h>
#include <lprocfs_status.h>

#ifdef HAVE_EXT4_LDISKFS
#include <ldiskfs/ldiskfs.h>
#else
#include <linux/ldiskfs_fs.h>
#endif
#include <lustre_mds.h>
#include <lustre/lustre_idl.h>

#include "mdd_internal.h"

struct mdd_txn_scrub {
        struct dt_txn_commit_cb   mts_cb;
        struct lu_object         *mts_obj;
};

static void mdd_txn_scrub_callback(struct lu_env *env, struct thandle *th,
                                   struct dt_txn_commit_cb *cb, int err)
{
        struct mdd_txn_scrub *mts;

        mts = container_of0(cb, struct mdd_txn_scrub, mts_cb);

        /*
         * XXX: 'err != 0' means Scrub fail to update the OI mapping. We can do
         *      nothing for that, just make other waiting threads to go forward.
         */
        lu_object_signal_scrub(mts->mts_obj->lo_header);
        lu_object_put(env, mts->mts_obj);

        cfs_list_del(&mts->mts_cb.dcb_linkage);
        OBD_FREE_PTR(mts);
}

static int mdd_txn_scrub_add(struct thandle *th, struct lu_object *obj)
{
        struct mdd_txn_scrub *mts;
        int rc;

        OBD_ALLOC_PTR(mts);
        if (mts == NULL)
                return -ENOMEM;

        mts->mts_cb.dcb_func = mdd_txn_scrub_callback;
        CFS_INIT_LIST_HEAD(&mts->mts_cb.dcb_linkage);
        mts->mts_obj = obj;

        rc = dt_trans_cb_add(th, &mts->mts_cb);
        if (rc)
                OBD_FREE_PTR(mts);
        else
                lu_object_get(mts->mts_obj);
        return rc;
}

int mdd_txn_stop_cb(const struct lu_env *env, struct thandle *txn,
                    void *cookie)
{
        struct mdd_device *mdd = cookie;
        struct obd_device *obd;

        if (txn->th_scrub) {
                struct mdd_scrub_it *msi = mdd->mdd_scrub_it;
                int rc;

                if (txn->th_result != 0)
                        return 0;

                if (txn->th_sync == 0 && msi != NULL && msi->msi_obj != NULL) {
                        rc = mdd_txn_scrub_add(txn, msi->msi_obj);
                        if (rc != 0)
                                txn->th_sync = 1;
                        else
                                msi->msi_obj = NULL;
                }

                return 0;
        }

        obd = mdd2obd_dev(mdd);
        LASSERT(obd);

        return mds_lov_write_objids(obd);
}

struct thandle *mdd_trans_create(const struct lu_env *env,
                                 struct mdd_device *mdd)
{
        return mdd_child_ops(mdd)->dt_trans_create(env, mdd->mdd_child);
}

int mdd_trans_start(const struct lu_env *env, struct mdd_device *mdd,
                    struct thandle *th)
{
        return mdd_child_ops(mdd)->dt_trans_start(env, mdd->mdd_child, th);
}

int mdd_trans_stop(const struct lu_env *env, struct mdd_device *mdd,
                    int result, struct thandle *handle)
{
        handle->th_result = result;
        return mdd_child_ops(mdd)->dt_trans_stop(env, handle);
}
