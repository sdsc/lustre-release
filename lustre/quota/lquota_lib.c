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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011, 2012, Intel, Inc.
 * Use is subject to license terms.
 *
 * Author: Johann Lombardi <johann.lombardi@intel.com>
 * Author: Niu    Yawei    <yawei.niu@intel.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#define DEBUG_SUBSYSTEM S_LQUOTA

#include <linux/version.h>
#include <linux/module.h>
#include <linux/init.h>

#include "lquota_internal.h"

/* register lquota key */
LU_KEY_INIT_FINI(lquota, struct lquota_thread_info);
LU_CONTEXT_KEY_DEFINE(lquota, LCT_MD_THREAD | LCT_DT_THREAD | LCT_LOCAL);
LU_KEY_INIT_GENERIC(lquota);

/**
 * Look-up accounting object to collect space usage information for user
 * or group.
 *
 * \param env  - is the environment passed by the caller
 * \param dev  - is the dt_device storing the accounting object
 * \param type - is the quota type, either USRQUOTA or GRPQUOTA
 */
struct dt_object *acct_obj_lookup(const struct lu_env *env,
				  struct dt_device *dev, int type)
{
	struct lquota_thread_info	*qti = lquota_info(env);
	struct dt_object		*obj = NULL;
	ENTRY;

	lu_local_obj_fid(&qti->qti_fid,
			 type == USRQUOTA ? ACCT_USER_OID : ACCT_GROUP_OID);

	/* lookup the accounting object */
	obj = dt_locate(env, dev, &qti->qti_fid);
	if (IS_ERR(obj))
		RETURN(obj);

	if (!dt_object_exists(obj)) {
		lu_object_put(env, &obj->do_lu);
		RETURN(ERR_PTR(-ENOENT));
	}

	if (obj->do_index_ops == NULL) {
		int rc;

		/* set up indexing operations */
		rc = obj->do_ops->do_index_try(env, obj, &dt_acct_features);
		if (rc) {
			CERROR("%s: failed to set up indexing operations for %s"
			       " acct object rc:%d\n",
			       dev->dd_lu_dev.ld_obd->obd_name,
			       QTYPE_NAME(type), rc);
			lu_object_put(env, &obj->do_lu);
			RETURN(ERR_PTR(rc));
		}
	}
	RETURN(obj);
}

/**
 * Initialize slave index object to collect local quota limit for user or group.
 *
 * \param env - is the environment passed by the caller
 * \param dev - is the dt_device storing the slave index object
 * \param type - is the quota type, either USRQUOTA or GRPQUOTA
 */
static struct dt_object *quota_obj_lookup(const struct lu_env *env,
					  struct dt_device *dev, int type)
{
	struct lquota_thread_info	*qti = lquota_info(env);
	struct dt_object		*obj = NULL;
	ENTRY;

	qti->qti_fid.f_seq = FID_SEQ_QUOTA;
	qti->qti_fid.f_oid = type == USRQUOTA ? QUOTA_USR_OID : QUOTA_GRP_OID;
	qti->qti_fid.f_ver = 0;

	/* lookup the quota object */
	obj = dt_locate(env, dev, &qti->qti_fid);
	if (IS_ERR(obj))
		RETURN(obj);

	if (!dt_object_exists(obj)) {
		lu_object_put(env, &obj->do_lu);
		RETURN(ERR_PTR(-ENOENT));
	}

	if (obj->do_index_ops == NULL) {
		int rc;

		/* set up indexing operations */
		rc = obj->do_ops->do_index_try(env, obj,
					       &dt_quota_slv_features);
		if (rc) {
			CERROR("%s: failed to set up indexing operations for %s"
			       " slave index object rc:%d\n",
			       dev->dd_lu_dev.ld_obd->obd_name,
			       QTYPE_NAME(type), rc);
			lu_object_put(env, &obj->do_lu);
			RETURN(ERR_PTR(rc));
		}
	}
	RETURN(obj);
}

/*
 * Helper routine to retrieve slave information.
 * This function converts a quotactl request into quota/accounting object
 * operations. It is independant of the slave stack which is only accessible
 * from the OSD layer.
 *
 * \param env   - is the environment passed by the caller
 * \param dev   - is the dt_device this quotactl is executed on
 * \param oqctl - is the quotactl request
 */
int lquotactl_slv(const struct lu_env *env, struct dt_device *dev,
		  struct obd_quotactl *oqctl)
{
	struct lquota_thread_info	*qti = lquota_info(env);
	__u64				 key;
	struct dt_object		*obj;
	struct obd_dqblk		*dqblk = &oqctl->qc_dqblk;
	int				 rc;
	ENTRY;

	if (oqctl->qc_cmd != Q_GETOQUOTA) {
		/* as in many other places, dev->dd_lu_dev.ld_obd->obd_name
		 * point to an invalid obd_name, to be fixed in LU-1574 */
		CERROR("%s: Unsupported quotactl command: %x\n",
		       dev->dd_lu_dev.ld_obd->obd_name, oqctl->qc_cmd);
		RETURN(-EOPNOTSUPP);
	}

	if (oqctl->qc_type != USRQUOTA && oqctl->qc_type != GRPQUOTA)
		/* no support for directory quota yet */
		RETURN(-EOPNOTSUPP);

	/* qc_id is a 32-bit field while a key has 64 bits */
	key = oqctl->qc_id;

	/* Step 1: collect accounting information */

	obj = acct_obj_lookup(env, dev, oqctl->qc_type);
	if (IS_ERR(obj))
		RETURN(-EOPNOTSUPP);
	if (obj->do_index_ops == NULL)
		GOTO(out, rc = -EINVAL);

	/* lookup record storing space accounting information for this ID */
	rc = dt_lookup(env, obj, (struct dt_rec *)&qti->qti_acct_rec,
		       (struct dt_key *)&key, BYPASS_CAPA);
	if (rc < 0)
		GOTO(out, rc);

	memset(&oqctl->qc_dqblk, 0, sizeof(struct obd_dqblk));
	dqblk->dqb_curspace	= qti->qti_acct_rec.bspace;
	dqblk->dqb_curinodes	= qti->qti_acct_rec.ispace;
	dqblk->dqb_valid	= QIF_USAGE;

	lu_object_put(env, &obj->do_lu);

	/* Step 2: collect enforcement information */

	obj = quota_obj_lookup(env, dev, oqctl->qc_type);
	if (IS_ERR(obj))
		RETURN(0);
	if (obj->do_index_ops == NULL)
		GOTO(out, rc = 0);

	memset(&qti->qti_slv_rec, 0, sizeof(qti->qti_slv_rec));
	/* lookup record storing enforcement information for this ID */
	rc = dt_lookup(env, obj, (struct dt_rec *)&qti->qti_slv_rec,
		       (struct dt_key *)&key, BYPASS_CAPA);
	if (rc < 0 && rc != -ENOENT)
		GOTO(out, rc = 0);

	if (lu_device_is_md(dev->dd_lu_dev.ld_site->ls_top_dev)) {
		dqblk->dqb_ihardlimit = qti->qti_slv_rec.qsr_granted;
		dqblk->dqb_bhardlimit = 0;
	} else {
		dqblk->dqb_ihardlimit = 0;
		dqblk->dqb_bhardlimit = qti->qti_slv_rec.qsr_granted;
	}
	dqblk->dqb_valid |= QIF_LIMITS;

	rc = 0;
	EXIT;
out:
	lu_object_put(env, &obj->do_lu);
        return rc;
}
EXPORT_SYMBOL(lquotactl_slv);

static int __init init_lquota(void)
{
	int	rc;

	/* call old quota module init function */
	rc = init_lustre_quota();
	if (rc)
		return rc;

	/* new quota initialization */
	lquota_key_init_generic(&lquota_thread_key, NULL);
	lu_context_key_register(&lquota_thread_key);

	return 0;
}

static void exit_lquota(void)
{
	/* call old quota module exit function */
	exit_lustre_quota();

	lu_context_key_degister(&lquota_thread_key);
}

MODULE_AUTHOR("Intel, Inc. <http://www.intel.com/>");
MODULE_DESCRIPTION("Lustre Quota");
MODULE_LICENSE("GPL");

cfs_module(lquota, "2.4.0", init_lquota, exit_lquota);
