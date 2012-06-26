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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdt/mdt_lvb.c
 *
 * Author: Jinshan Xiong <jinshan.xiong@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "mdt_internal.h"

/* Called with res->lr_lvb_sem held */
static int mdt_lvbo_init(struct ldlm_resource *res)
{
	return 0;
}

static int mdt_lvbo_size(struct ldlm_lock *lock)
{
	struct mdt_device *mdt;
	int mdsize = 0;

	LASSERT(lock->l_resource->lr_type == LDLM_IBITS);

	if (!(lock->l_policy_data.l_inodebits.bits & MDS_INODELOCK_LAYOUT))
		return 0;

	/* resource on server side never changes. */
	mdt = ldlm_res_to_ns(lock->l_resource)->ns_lvbp;
	LASSERT(mdt != NULL);

	lock_res_and_lock(lock);
	if (lock->l_req_mode == lock->l_granted_mode)
		mdsize = mdt->mdt_max_mdsize;
	unlock_res_and_lock(lock);

	return mdsize;
}

static int mdt_lvbo_fill(struct ldlm_lock *lock, void *lvb, int lvblen)
{
	struct lu_env env;
	struct mdt_thread_info *info;
	struct ldlm_res_id *name = &lock->l_resource->lr_name;
	struct lu_fid *fid;
	struct mdt_object *obj = NULL;
	struct md_object *child = NULL;
	struct lu_buf *buf = NULL;
	int rc;
	ENTRY;

	if (!(lock->l_policy_data.l_inodebits.bits & MDS_INODELOCK_LAYOUT))
		RETURN(0);

	/* layout lock will be granted to client, fill in lvb with layout */

	/* XXX create an env to talk to mdt stack. We should get this env from
	 * ptlrpc_thread->t_env. */
	rc = lu_env_init(&env, LCT_MD_THREAD);
	LASSERT(rc == 0);

	info = lu_context_key_get(&env.le_ctx, &mdt_thread_key);
	LASSERT(info != NULL);
	memset(info, 0, sizeof *info);
	info->mti_env = &env;
	info->mti_exp = lock->l_export;
	info->mti_mdt = ldlm_lock_to_ns(lock)->ns_lvbp;

	/* XXX get fid by resource id. why don't include fid in ldlm_resource */
	fid = &info->mti_tmp_fid2;
	fid_build_from_res_name(fid, name);

	obj = mdt_object_find(&env, info->mti_mdt, fid);
	if (IS_ERR(obj))
		GOTO(out, rc = PTR_ERR(obj));

	if (mdt_object_exists(obj) <= 0)
		GOTO(out, rc = -ENOENT);

	child = mdt_object_child(obj);

	/* get the length of lsm */
	rc = mo_xattr_get(&env, child, &LU_BUF_NULL, XATTR_NAME_LOV);
	if (rc < 0)
		GOTO(out, rc);

	if (rc > 0) {
		if (lvblen < rc) {
			CERROR("%s: expected %d actual %d.\n",
				info->mti_exp->exp_obd->obd_name, rc, lvblen);
			GOTO(out, rc = -ERANGE);
		}

		buf = &info->mti_buf;
		buf->lb_buf = lvb;
		buf->lb_len = rc;

		rc = mo_xattr_get(&env, child, buf, XATTR_NAME_LOV);
		if (rc < 0)
			GOTO(out, rc);
	}

out:
	if (obj != NULL && !IS_ERR(obj))
		mdt_object_put(&env, obj);
	if (rc < 0)
		CERROR("%s: fill lvbo error: %d\n",
			info->mti_exp->exp_obd->obd_name, rc);
	lu_env_fini(&env);
	RETURN(rc < 0 ? 0 : rc);
}

struct ldlm_valblock_ops mdt_lvbo = {
	lvbo_init:	mdt_lvbo_init,
	lvbo_size: 	mdt_lvbo_size,
	lvbo_fill: 	mdt_lvbo_fill
};
