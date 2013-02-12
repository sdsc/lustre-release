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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ptlrpc/llog_net.c
 *
 * OST<->MDS recovery logging infrastructure.
 *
 * Invariants in implementation:
 * - we do not share logs among different OST<->MDS connections, so that
 *   if an OST or MDS fails it need only look at log(s) relevant to itself
 *
 * Author: Andreas Dilger <adilger@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_LOG

#ifdef __KERNEL__
#include <libcfs/libcfs.h>
#else
#include <liblustre.h>
#endif

#include <obd_class.h>
#include <lustre_log.h>
#include <libcfs/list.h>
#include <lvfs.h>
#include <lustre_fsfilt.h>

#ifdef __KERNEL__
int llog_origin_connect(struct llog_ctxt *ctxt,
                        struct llog_logid *logid, struct llog_gen *gen,
                        struct obd_uuid *uuid)
{
        return -ENOTSUPP;
}
EXPORT_SYMBOL(llog_origin_connect);

int llog_handle_connect(struct ptlrpc_request *req)
{
        struct obd_device *obd = req->rq_export->exp_obd;
        struct llogd_conn_body *req_body;
        struct llog_ctxt *ctxt;
        int rc;
        ENTRY;

        req_body = req_capsule_client_get(&req->rq_pill, &RMF_LLOGD_CONN_BODY);

        ctxt = llog_get_context(obd, req_body->lgdc_ctxt_idx);
        rc = llog_connect(ctxt, &req_body->lgdc_logid,
                          &req_body->lgdc_gen, NULL);

        llog_ctxt_put(ctxt);
        if (rc != 0)
                CERROR("failed at llog_relp_connect\n");

        RETURN(rc);
}
EXPORT_SYMBOL(llog_handle_connect);

int llog_receptor_accept(struct llog_ctxt *ctxt, struct obd_import *imp)
{
        ENTRY;

        LASSERT(ctxt);
	mutex_lock(&ctxt->loc_mutex);
        if (ctxt->loc_imp != imp) {
                if (ctxt->loc_imp) {
                        CWARN("changing the import %p - %p\n",
                              ctxt->loc_imp, imp);
                        class_import_put(ctxt->loc_imp);
                }
                ctxt->loc_imp = class_import_get(imp);
        }
	mutex_unlock(&ctxt->loc_mutex);
        RETURN(0);
}
EXPORT_SYMBOL(llog_receptor_accept);

#else /* !__KERNEL__ */

int llog_origin_connect(struct llog_ctxt *ctxt,
                        struct llog_logid *logid, struct llog_gen *gen,
                        struct obd_uuid *uuid)
{
        return 0;
}
#endif

int llog_initiator_connect(struct llog_ctxt *ctxt)
{
        struct obd_import *new_imp;
        ENTRY;

        LASSERT(ctxt);
        new_imp = ctxt->loc_obd->u.cli.cl_import;
        LASSERTF(ctxt->loc_imp == NULL || ctxt->loc_imp == new_imp,
                 "%p - %p\n", ctxt->loc_imp, new_imp);
	mutex_lock(&ctxt->loc_mutex);
        if (ctxt->loc_imp != new_imp) {
                if (ctxt->loc_imp)
                        class_import_put(ctxt->loc_imp);
                ctxt->loc_imp = class_import_get(new_imp);
        }
	mutex_unlock(&ctxt->loc_mutex);
        RETURN(0);
}
EXPORT_SYMBOL(llog_initiator_connect);
