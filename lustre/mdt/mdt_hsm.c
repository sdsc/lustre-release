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
 * Copyright (c) 2012 Intel Corporation
 * Copyright (c) 2012 Commissariat a l'energie atomique et aux energies
 *                    alternatives
 * Use is subject to license terms.
 *
 * Contributor: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Contributor: JC Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "mdt_internal.h"

/**
 * Update on-disk HSM attributes.
 */
int mdt_hsm_attr_set(struct mdt_thread_info *info, struct mdt_object *obj,
		     struct md_hsm *mh)
{
	struct md_object	*next = mdt_object_child(obj);
	struct lu_buf		*buf = &info->mti_buf;
	struct hsm_attrs	*attrs;
	int			 rc;
	ENTRY;

	attrs = (struct hsm_attrs *)info->mti_xattr_buf;
	CLASSERT(sizeof(info->mti_xattr_buf) >= sizeof(*attrs));

	/* pack HSM attributes */
	lustre_hsm2buf(info->mti_xattr_buf, mh);

	/* update SOM attributes */
	buf->lb_buf = attrs;
	buf->lb_len = sizeof(*attrs);
	rc = mo_xattr_set(info->mti_env, next, buf, XATTR_NAME_HSM, 0);

	RETURN(rc);
}


/**
 * Retrieve the current HSM flags, archive id and undergoing HSM requests for
 * the fid provided in RPC body.
 *
 * Current requests are read from coordinator states.
 *
 * This is MDS_HSM_STATE_GET RPC handler.
 */
int mdt_hsm_state_get(struct mdt_thread_info *info)
{
	struct mdt_object	*obj = info->mti_object;
	struct md_attr		*ma  = &info->mti_attr;
	struct hsm_user_state	*hus;
	struct mdt_lock_handle	*lh;
	int			 rc;
	ENTRY;

	lh = &info->mti_lh[MDT_LH_CHILD];
	mdt_lock_reg_init(lh, LCK_PR);
	rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP,
			     MDT_LOCAL_LOCK);
	if (rc)
		RETURN(rc);

	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc)
		GOTO(out_unlock, rc = err_serious(rc));

	ma->ma_valid = 0;
	ma->ma_need = MA_HSM;
	rc = mdt_attr_get_complex(info, obj, ma);
	if (rc)
		GOTO(out_ucred, rc);

	if (req_capsule_get_size(info->mti_pill, &RMF_CAPA1, RCL_CLIENT))
		mdt_set_capainfo(info, 0, &info->mti_body->fid1,
			    req_capsule_client_get(info->mti_pill, &RMF_CAPA1));

	hus = req_capsule_server_get(info->mti_pill, &RMF_HSM_USER_STATE);
	LASSERT(hus);

	/* Current HSM flags */
	hus->hus_states = ma->ma_hsm.mh_flags;
	hus->hus_archive_num = ma->ma_hsm.mh_arch_num;

	EXIT;
out_ucred:
	mdt_exit_ucred(info);
out_unlock:
	mdt_object_unlock(info, obj, lh, 1);
	return rc;
}

/**
 * Change HSM state and archive number of a file.
 *
 * Archive number is changed iif the value is not 0.
 * The new flagset that will be computed should result in a coherent state.
 * This function checks that are flags are compatible.
 *
 * This is MDS_HSM_STATE_SET RPC handler.
 */
int mdt_hsm_state_set(struct mdt_thread_info *info)
{
	struct mdt_object	*obj = info->mti_object;
	struct md_attr          *ma = &info->mti_attr;
	struct hsm_state_set	*hss;
	struct mdt_lock_handle	*lh;
	int			 rc;
	__u64			 flags;
	ENTRY;

	lh = &info->mti_lh[MDT_LH_CHILD];
	mdt_lock_reg_init(lh, LCK_PW);
	rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP,
			     MDT_LOCAL_LOCK);
	if (rc)
		RETURN(rc);

	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc)
		GOTO(out_obj, rc = err_serious(rc));

	/* Read current HSM info */
	ma->ma_valid = 0;
	ma->ma_need = MA_HSM;
	rc = mdt_attr_get_complex(info, obj, ma);
	if (rc)
		GOTO(out_ucred, rc);

	hss = req_capsule_client_get(info->mti_pill, &RMF_HSM_STATE_SET);
	LASSERT(hss);

	if (req_capsule_get_size(info->mti_pill, &RMF_CAPA1, RCL_CLIENT))
		mdt_set_capainfo(info, 0, &info->mti_body->fid1,
			    req_capsule_client_get(info->mti_pill, &RMF_CAPA1));

	/* Change HSM flags depending on provided masks */
	if (hss->hss_valid & HSS_SETMASK)
		ma->ma_hsm.mh_flags |= hss->hss_setmask;
	if (hss->hss_valid & HSS_CLEARMASK)
		ma->ma_hsm.mh_flags &= ~hss->hss_clearmask;

	/* Change archive_num if provided. */
	if (hss->hss_valid & HSS_ARCHIVE_NUM) {
		if (!(ma->ma_hsm.mh_flags & HS_EXISTS)) {
			CDEBUG(D_HSM, "Could not set an archive number for "
			       DFID "if HSM EXISTS flag is not set.\n",
			       PFID(&info->mti_body->fid1));
			GOTO(out_ucred, rc);
		}
		ma->ma_hsm.mh_arch_num = hss->hss_archive_num;
	}

	/* Check for inconsistant HSM flagset.
	 * DIRTY without EXISTS: no dirty if no archive was created.
	 * DIRTY and RELEASED: a dirty file could not be released.
	 * RELEASED without ARCHIVED: do not release a non-archived file.
	 * LOST without ARCHIVED: cannot lost a non-archived file.
	 */
	flags = ma->ma_hsm.mh_flags;
	if (((flags & HS_DIRTY) && !(flags & HS_EXISTS)) ||
	    ((flags & HS_RELEASED) && (flags & HS_DIRTY)) ||
	    ((flags & HS_RELEASED) && !(flags & HS_ARCHIVED)) ||
	    ((flags & HS_LOST)     && !(flags & HS_ARCHIVED))) {
		CDEBUG(D_HSM, "Incompatible flag change on "DFID
			      "flags="LPX64"\n",
		       PFID(&info->mti_body->fid1), flags);
		GOTO(out_ucred, rc = -EINVAL);
	}

	/* Save the modified flags */
	rc = mdt_hsm_attr_set(info, obj, &ma->ma_hsm);
	if (rc)
		GOTO(out_ucred, rc);

	EXIT;

out_ucred:
	mdt_exit_ucred(info);
out_obj:
	mdt_object_unlock(info, obj, lh, 1);
	return rc;
}
