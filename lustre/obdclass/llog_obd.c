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
 * Copyright (c) 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LOG

#ifndef __KERNEL__
#include <liblustre.h>
#endif

#include <obd_class.h>
#include <lustre_log.h>
#include "llog_internal.h"

/* helper functions for calling the llog obd methods */
static struct llog_ctxt* llog_new_ctxt(struct obd_device *obd)
{
        struct llog_ctxt *ctxt;

        OBD_ALLOC_PTR(ctxt);
        if (!ctxt)
                return NULL;

        ctxt->loc_obd = obd;
        cfs_atomic_set(&ctxt->loc_refcount, 1);

        return ctxt;
}

static void llog_ctxt_destroy(struct llog_ctxt *ctxt)
{
        if (ctxt->loc_exp) {
                class_export_put(ctxt->loc_exp);
                ctxt->loc_exp = NULL;
        }
        if (ctxt->loc_imp) {
                class_import_put(ctxt->loc_imp);
                ctxt->loc_imp = NULL;
        }
        LASSERT(ctxt->loc_llcd == NULL);
        OBD_FREE_PTR(ctxt);
}

int __llog_ctxt_put(const struct lu_env *env, struct llog_ctxt *ctxt)
{
        struct obd_llog_group *olg = ctxt->loc_olg;
        struct obd_device *obd;
        int rc = 0;

	spin_lock(&olg->olg_lock);
	if (!cfs_atomic_dec_and_test(&ctxt->loc_refcount)) {
		spin_unlock(&olg->olg_lock);
		return rc;
	}
	olg->olg_ctxts[ctxt->loc_idx] = NULL;
	spin_unlock(&olg->olg_lock);

	if (ctxt->loc_lcm)
		lcm_put(ctxt->loc_lcm);

	obd = ctxt->loc_obd;
	spin_lock(&obd->obd_dev_lock);
	/* sync with llog ctxt user thread */
	spin_unlock(&obd->obd_dev_lock);

        /* obd->obd_starting is needed for the case of cleanup
         * in error case while obd is starting up. */
        LASSERTF(obd->obd_starting == 1 ||
                 obd->obd_stopping == 1 || obd->obd_set_up == 0,
                 "wrong obd state: %d/%d/%d\n", !!obd->obd_starting,
                 !!obd->obd_stopping, !!obd->obd_set_up);

        /* cleanup the llog ctxt here */
        if (CTXTP(ctxt, cleanup))
		rc = CTXTP(ctxt, cleanup)(env, ctxt);

	llog_ctxt_destroy(ctxt);
	cfs_waitq_signal(&olg->olg_waitq);
	return rc;
}
EXPORT_SYMBOL(__llog_ctxt_put);

int llog_cleanup(const struct lu_env *env, struct llog_ctxt *ctxt)
{
        struct l_wait_info lwi = LWI_INTR(LWI_ON_SIGNAL_NOOP, NULL);
        struct obd_llog_group *olg;
        int rc, idx;
        ENTRY;

        LASSERT(ctxt != NULL);
        LASSERT(ctxt != LP_POISON);

        olg = ctxt->loc_olg;
        LASSERT(olg != NULL);
        LASSERT(olg != LP_POISON);

        idx = ctxt->loc_idx;

	/*
         * Banlance the ctxt get when calling llog_cleanup()
         */
        LASSERT(cfs_atomic_read(&ctxt->loc_refcount) < LI_POISON);
        LASSERT(cfs_atomic_read(&ctxt->loc_refcount) > 1);
        llog_ctxt_put(ctxt);

	/*
	 * Try to free the ctxt.
	 */
	rc = __llog_ctxt_put(env, ctxt);
        if (rc)
                CERROR("Error %d while cleaning up ctxt %p\n",
                       rc, ctxt);

        l_wait_event(olg->olg_waitq,
                     llog_group_ctxt_null(olg, idx), &lwi);

        RETURN(rc);
}
EXPORT_SYMBOL(llog_cleanup);

int llog_setup(const struct lu_env *env, struct obd_device *obd,
	       struct obd_llog_group *olg, int index,
	       struct obd_device *disk_obd, struct llog_operations *op)
{
        struct llog_ctxt *ctxt;
        int rc = 0;
        ENTRY;

        if (index < 0 || index >= LLOG_MAX_CTXTS)
                RETURN(-EINVAL);

        LASSERT(olg != NULL);

        ctxt = llog_new_ctxt(obd);
        if (!ctxt)
                RETURN(-ENOMEM);

        ctxt->loc_obd = obd;
        ctxt->loc_olg = olg;
        ctxt->loc_idx = index;
        ctxt->loc_logops = op;
	mutex_init(&ctxt->loc_mutex);
        ctxt->loc_exp = class_export_get(disk_obd->obd_self_export);
        ctxt->loc_flags = LLOG_CTXT_FLAG_UNINITIALIZED;

        rc = llog_group_set_ctxt(olg, ctxt, index);
        if (rc) {
                llog_ctxt_destroy(ctxt);
                if (rc == -EEXIST) {
                        ctxt = llog_group_get_ctxt(olg, index);
                        if (ctxt) {
                                /*
                                 * mds_lov_update_desc() might call here multiple
                                 * times. So if the llog is already set up then
                                 * don't to do it again. 
                                 */
                                CDEBUG(D_CONFIG, "obd %s ctxt %d already set up\n",
                                       obd->obd_name, index);
                                LASSERT(ctxt->loc_olg == olg);
                                LASSERT(ctxt->loc_obd == obd);
                                LASSERT(ctxt->loc_exp == disk_obd->obd_self_export);
                                LASSERT(ctxt->loc_logops == op);
                                llog_ctxt_put(ctxt);
                        }
                        rc = 0;
                }
                RETURN(rc);
        }

	if (op->lop_setup) {
		if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LLOG_SETUP))
			rc = -EOPNOTSUPP;
		else
			rc = op->lop_setup(env, obd, olg, index, disk_obd);
	}

	if (rc) {
		CERROR("%s: ctxt %d lop_setup=%p failed: rc = %d\n",
		       obd->obd_name, index, op->lop_setup, rc);
		llog_group_clear_ctxt(olg, index);
		llog_ctxt_destroy(ctxt);
        } else {
                CDEBUG(D_CONFIG, "obd %s ctxt %d is initialized\n",
                       obd->obd_name, index);
                ctxt->loc_flags &= ~LLOG_CTXT_FLAG_UNINITIALIZED;
        }

        RETURN(rc);
}
EXPORT_SYMBOL(llog_setup);

int llog_sync(struct llog_ctxt *ctxt, struct obd_export *exp, int flags)
{
        int rc = 0;
        ENTRY;

        if (!ctxt)
                RETURN(0);

        if (CTXTP(ctxt, sync))
		rc = CTXTP(ctxt, sync)(ctxt, exp, flags);

        RETURN(rc);
}
EXPORT_SYMBOL(llog_sync);

int llog_obd_add(const struct lu_env *env, struct llog_ctxt *ctxt,
		 struct llog_rec_hdr *rec, struct lov_stripe_md *lsm,
		 struct llog_cookie *logcookies, int numcookies)
{
        int raised, rc;
        ENTRY;

        if (!ctxt) {
                CERROR("No ctxt\n");
                RETURN(-ENODEV);
        }

        if (ctxt->loc_flags & LLOG_CTXT_FLAG_UNINITIALIZED)
                RETURN(-ENXIO);

	CTXT_CHECK_OP(ctxt, obd_add, -EOPNOTSUPP);
        raised = cfs_cap_raised(CFS_CAP_SYS_RESOURCE);
        if (!raised)
                cfs_cap_raise(CFS_CAP_SYS_RESOURCE);
	rc = CTXTP(ctxt, obd_add)(env, ctxt, rec, lsm, logcookies,
				  numcookies);
        if (!raised)
                cfs_cap_lower(CFS_CAP_SYS_RESOURCE);
        RETURN(rc);
}
EXPORT_SYMBOL(llog_obd_add);

int llog_cancel(const struct lu_env *env, struct llog_ctxt *ctxt,
		struct lov_stripe_md *lsm, int count,
		struct llog_cookie *cookies, int flags)
{
        int rc;
        ENTRY;

        if (!ctxt) {
                CERROR("No ctxt\n");
                RETURN(-ENODEV);
        }

        CTXT_CHECK_OP(ctxt, cancel, -EOPNOTSUPP);
	rc = CTXTP(ctxt, cancel)(env, ctxt, lsm, count, cookies, flags);
        RETURN(rc);
}
EXPORT_SYMBOL(llog_cancel);

/* add for obdfilter/sz and mds/unlink */
int llog_obd_origin_add(const struct lu_env *env, struct llog_ctxt *ctxt,
			struct llog_rec_hdr *rec, struct lov_stripe_md *lsm,
			struct llog_cookie *logcookies, int numcookies)
{
        struct llog_handle *cathandle;
        int rc;
        ENTRY;

        cathandle = ctxt->loc_handle;
        LASSERT(cathandle != NULL);
	rc = llog_cat_add(env, cathandle, rec, logcookies, NULL);
        if (rc != 0 && rc != 1)
                CERROR("write one catalog record failed: %d\n", rc);
        RETURN(rc);
}
EXPORT_SYMBOL(llog_obd_origin_add);

int obd_llog_init(struct obd_device *obd, struct obd_llog_group *olg,
                  struct obd_device *disk_obd, int *index)
{
        int rc;
        ENTRY;
        OBD_CHECK_DT_OP(obd, llog_init, 0);
        OBD_COUNTER_INCREMENT(obd, llog_init);

        rc = OBP(obd, llog_init)(obd, olg, disk_obd, index);
        RETURN(rc);
}
EXPORT_SYMBOL(obd_llog_init);

int obd_llog_finish(struct obd_device *obd, int count)
{
        int rc;
        ENTRY;
        OBD_CHECK_DT_OP(obd, llog_finish, 0);
        OBD_COUNTER_INCREMENT(obd, llog_finish);

        rc = OBP(obd, llog_finish)(obd, count);
        RETURN(rc);
}
EXPORT_SYMBOL(obd_llog_finish);

/* context key constructor/destructor: llog_key_init, llog_key_fini */
LU_KEY_INIT_FINI(llog, struct llog_thread_info);
/* context key: llog_thread_key */
LU_CONTEXT_KEY_DEFINE(llog, LCT_MD_THREAD | LCT_MG_THREAD | LCT_LOCAL);
LU_KEY_INIT_GENERIC(llog);
EXPORT_SYMBOL(llog_thread_key);

int llog_info_init(void)
{
	llog_key_init_generic(&llog_thread_key, NULL);
	lu_context_key_register(&llog_thread_key);
	return 0;
}

void llog_info_fini(void)
{
	lu_context_key_degister(&llog_thread_key);
}
