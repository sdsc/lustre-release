/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011, 2012 Commissariat a l'energie atomique et aux energies
 *                          alternatives
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2015, Intel Corporation.
 */
/*
 * lustre/mdt/mdt_hsm.c
 *
 * Lustre Metadata Target (mdt) request handler
 *
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Author: JC Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "mdt_internal.h"

/* Max allocation to satisfy single HSM RPC. */
#define MDT_HSM_ALLOC_MAX (1 << 20)

#define MDT_HSM_ALLOC(ptr, size)			\
	do {						\
		if ((size) <= MDT_HSM_ALLOC_MAX)	\
			OBD_ALLOC_LARGE((ptr), (size));	\
		else					\
			(ptr) = NULL;			\
	} while (0)

#define MDT_HSM_FREE(ptr, size) OBD_FREE_LARGE((ptr), (size))

/**
 * Update on-disk HSM attributes.
 */
int mdt_hsm_attr_set(struct mdt_thread_info *info, struct mdt_object *obj,
		     const struct md_hsm *mh)
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

	/* update HSM attributes */
	buf->lb_buf = attrs;
	buf->lb_len = sizeof(*attrs);
	rc = mo_xattr_set(info->mti_env, next, buf, XATTR_NAME_HSM, 0);

	RETURN(rc);
}

static inline bool mdt_hsm_is_admin(struct mdt_thread_info *info)
{
	bool is_admin;
	int rc;

	if (info->mti_body == NULL)
		return false;

	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc < 0)
		return false;

	is_admin = md_capable(mdt_ucred(info), CFS_CAP_SYS_ADMIN);

	mdt_exit_ucred(info);

	return is_admin;
}

/**
 * Extract information coming from a copytool and asks coordinator to update
 * a request status depending on the update content.
 *
 * Copytools could use this to report failure in their process.
 *
 * This is HSM_PROGRESS RPC handler.
 */
int mdt_hsm_progress(struct tgt_session_info *tsi)
{
	struct mdt_thread_info		*info;
	struct hsm_progress_kernel	*hpk;
	int				 rc;
	ENTRY;

	if (tsi->tsi_mdt_body == NULL)
		RETURN(-EPROTO);

	hpk = req_capsule_client_get(tsi->tsi_pill, &RMF_MDS_HSM_PROGRESS);
	if (hpk == NULL)
		RETURN(err_serious(-EPROTO));

	hpk->hpk_errval = lustre_errno_ntoh(hpk->hpk_errval);

	CDEBUG(D_HSM, "Progress on "DFID": len=%llu : rc = %d\n",
	       PFID(&hpk->hpk_fid), hpk->hpk_extent.length, hpk->hpk_errval);

	if (hpk->hpk_errval)
		CDEBUG(D_HSM, "Copytool progress on "DFID" failed : rc = %d; %s.\n",
		       PFID(&hpk->hpk_fid), hpk->hpk_errval,
		       hpk->hpk_flags & HP_FLAG_RETRY ? "will retry" : "fatal");

	if (hpk->hpk_flags & HP_FLAG_COMPLETED)
		CDEBUG(D_HSM, "Finished "DFID" : rc = %d; cancel cookie=%#llx\n",
		       PFID(&hpk->hpk_fid), hpk->hpk_errval, hpk->hpk_cookie);

	info = tsi2mdt_info(tsi);
	if (!mdt_hsm_is_admin(info))
		GOTO(out, rc = -EPERM);

	rc = mdt_hsm_coordinator_update(info, hpk);
out:
	mdt_thread_info_fini(info);
	RETURN(rc);
}

int mdt_hsm_ct_register(struct tgt_session_info *tsi)
{
	struct mdt_thread_info	*info;
	__u32			*archives;
	int			 rc;
	ENTRY;

	archives = req_capsule_client_get(tsi->tsi_pill, &RMF_MDS_HSM_ARCHIVE);
	if (archives == NULL)
		RETURN(err_serious(-EPROTO));

	info = tsi2mdt_info(tsi);
	if (!mdt_hsm_is_admin(info))
		GOTO(out, rc = -EPERM);

	/* XXX: directly include this function here? */
	rc = mdt_hsm_agent_register_mask(info, &tsi->tsi_exp->exp_client_uuid,
					 *archives);
out:
	mdt_thread_info_fini(info);
	RETURN(rc);
}

int mdt_hsm_ct_unregister(struct tgt_session_info *tsi)
{
	struct mdt_thread_info	*info;
	int			 rc;
	ENTRY;

	if (tsi->tsi_mdt_body == NULL)
		RETURN(-EPROTO);

	info = tsi2mdt_info(tsi);
	if (!mdt_hsm_is_admin(info))
		GOTO(out, rc = -EPERM);

	/* XXX: directly include this function here? */
	rc = mdt_hsm_agent_unregister(info, &tsi->tsi_exp->exp_client_uuid);
out:
	mdt_thread_info_fini(info);
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
int mdt_hsm_state_get(struct tgt_session_info *tsi)
{
	struct mdt_thread_info	*info = tsi2mdt_info(tsi);
	struct mdt_object	*obj = info->mti_object;
	struct md_attr		*ma  = &info->mti_attr;
	struct hsm_user_state	*hus;
	struct mdt_lock_handle	*lh;
	int			 rc;
	ENTRY;

	if (info->mti_body == NULL || obj == NULL)
		GOTO(out, rc = -EPROTO);

	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc < 0)
		GOTO(out, rc = err_serious(rc));

	lh = &info->mti_lh[MDT_LH_CHILD];
	mdt_lock_reg_init(lh, LCK_PR);
	rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP);
	if (rc < 0)
		GOTO(out_ucred, rc);

	ma->ma_valid = 0;
	ma->ma_need = MA_HSM;
	rc = mdt_attr_get_complex(info, obj, ma);
	if (rc)
		GOTO(out_unlock, rc);

	hus = req_capsule_server_get(tsi->tsi_pill, &RMF_HSM_USER_STATE);
	if (hus == NULL)
		GOTO(out_unlock, rc = -EPROTO);

	/* Current HSM flags */
	hus->hus_states = ma->ma_hsm.mh_flags;
	hus->hus_archive_id = ma->ma_hsm.mh_arch_id;

	EXIT;
out_unlock:
	mdt_object_unlock(info, obj, lh, 1);
out_ucred:
	mdt_exit_ucred(info);
out:
	mdt_thread_info_fini(info);
	return rc;
}

/**
 * Change HSM state and archive number of a file.
 *
 * Archive number is changed iif the value is not 0.
 * The new flagset that will be computed should result in a coherent state.
 * This function checks that flags are compatible.
 *
 * This is MDS_HSM_STATE_SET RPC handler.
 */
int mdt_hsm_state_set(struct tgt_session_info *tsi)
{
	struct mdt_thread_info	*info = tsi2mdt_info(tsi);
	struct mdt_object	*obj = info->mti_object;
	struct md_attr          *ma = &info->mti_attr;
	struct hsm_state_set	*hss;
	struct mdt_lock_handle	*lh;
	int			 rc;
	__u64			 flags;
	ENTRY;

	hss = req_capsule_client_get(info->mti_pill, &RMF_HSM_STATE_SET);

	if (info->mti_body == NULL || obj == NULL || hss == NULL)
		GOTO(out, rc = -EPROTO);

	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc < 0)
		GOTO(out, rc = err_serious(rc));

	lh = &info->mti_lh[MDT_LH_CHILD];
	mdt_lock_reg_init(lh, LCK_PW);
	rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP |
			     MDS_INODELOCK_XATTR);
	if (rc < 0)
		GOTO(out_ucred, rc);

	/* Detect out-of range masks */
	if ((hss->hss_setmask | hss->hss_clearmask) & ~HSM_FLAGS_MASK) {
		CDEBUG(D_HSM, "Incompatible masks provided (set %#llx"
		       ", clear %#llx) vs supported set (%#x).\n",
		       hss->hss_setmask, hss->hss_clearmask, HSM_FLAGS_MASK);
		GOTO(out_unlock, rc = -EINVAL);
	}

	/* Non-root users are forbidden to set or clear flags which are
	 * NOT defined in HSM_USER_MASK. */
	if (((hss->hss_setmask | hss->hss_clearmask) & ~HSM_USER_MASK) &&
	    !md_capable(mdt_ucred(info), CFS_CAP_SYS_ADMIN)) {
		CDEBUG(D_HSM, "Incompatible masks provided (set %#llx"
		       ", clear %#llx) vs unprivileged set (%#x).\n",
		       hss->hss_setmask, hss->hss_clearmask, HSM_USER_MASK);
		GOTO(out_unlock, rc = -EPERM);
	}

	/* Read current HSM info */
	ma->ma_valid = 0;
	ma->ma_need = MA_HSM;
	rc = mdt_attr_get_complex(info, obj, ma);
	if (rc)
		GOTO(out_unlock, rc);

	/* Change HSM flags depending on provided masks */
	if (hss->hss_valid & HSS_SETMASK)
		ma->ma_hsm.mh_flags |= hss->hss_setmask;
	if (hss->hss_valid & HSS_CLEARMASK)
		ma->ma_hsm.mh_flags &= ~hss->hss_clearmask;

	/* Change archive_id if provided. */
	if (hss->hss_valid & HSS_ARCHIVE_ID) {
		if (!(ma->ma_hsm.mh_flags & HS_EXISTS)) {
			CDEBUG(D_HSM, "Could not set an archive number for "
			       DFID "if HSM EXISTS flag is not set.\n",
			       PFID(&info->mti_body->mbo_fid1));
			GOTO(out_unlock, rc);
		}

		/* Detect out-of range archive id */
		if (hss->hss_archive_id > LL_HSM_MAX_ARCHIVE) {
			CDEBUG(D_HSM, "archive id %u exceeds maximum %zu.\n",
			       hss->hss_archive_id, LL_HSM_MAX_ARCHIVE);
			GOTO(out_unlock, rc = -EINVAL);
		}

		ma->ma_hsm.mh_arch_id = hss->hss_archive_id;
	}

	/* Check for inconsistant HSM flagset.
	 * DIRTY without EXISTS: no dirty if no archive was created.
	 * DIRTY and RELEASED: a dirty file could not be released.
	 * RELEASED without ARCHIVED: do not release a non-archived file.
	 * LOST without ARCHIVED: cannot lost a non-archived file.
	 */
	flags = ma->ma_hsm.mh_flags;
	if ((flags & HS_DIRTY    && !(flags & HS_EXISTS)) ||
	    (flags & HS_RELEASED && flags & HS_DIRTY) ||
	    (flags & HS_RELEASED && !(flags & HS_ARCHIVED)) ||
	    (flags & HS_LOST     && !(flags & HS_ARCHIVED))) {
		CDEBUG(D_HSM, "Incompatible flag change on "DFID
			      "flags=%#llx\n",
		       PFID(&info->mti_body->mbo_fid1), flags);
		GOTO(out_unlock, rc = -EINVAL);
	}

	/* Save the modified flags */
	rc = mdt_hsm_attr_set(info, obj, &ma->ma_hsm);
	if (rc)
		GOTO(out_unlock, rc);

	EXIT;

out_unlock:
	mdt_object_unlock(info, obj, lh, 1);
out_ucred:
	mdt_exit_ucred(info);
out:
	mdt_thread_info_fini(info);
	return rc;
}

/**
 * Retrieve undergoing HSM requests for the fid provided in RPC body.
 * Current requests are read from coordinator states.
 *
 * This is MDS_HSM_ACTION RPC handler.
 */
int mdt_hsm_action(struct tgt_session_info *tsi)
{
	struct mdt_thread_info		*info;
	struct hsm_current_action	*hca;
	struct hsm_action_list		*hal = NULL;
	struct hsm_action_item		*hai;
	int				 hal_size;
	int				 rc;
	ENTRY;

	hca = req_capsule_server_get(tsi->tsi_pill,
				     &RMF_MDS_HSM_CURRENT_ACTION);
	if (hca == NULL)
		RETURN(err_serious(-EPROTO));

	if (tsi->tsi_mdt_body == NULL)
		RETURN(-EPROTO);

	info = tsi2mdt_info(tsi);
	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc)
		GOTO(out, rc = err_serious(rc));

	/* Coordinator information */
	hal_size = sizeof(*hal) +
		   cfs_size_round(MTI_NAME_MAXLEN) /* fsname */ +
		   cfs_size_round(sizeof(*hai));

	MDT_HSM_ALLOC(hal, hal_size);
	if (hal == NULL)
		GOTO(out_ucred, rc = -ENOMEM);

	hal->hal_version = HAL_VERSION;
	hal->hal_archive_id = 0;
	hal->hal_flags = 0;
	obd_uuid2fsname(hal->hal_fsname, mdt_obd_name(info->mti_mdt),
			MTI_NAME_MAXLEN);
	hal->hal_count = 1;
	hai = hai_first(hal);
	hai->hai_action = HSMA_NONE;
	hai->hai_cookie = 0;
	hai->hai_gid = 0;
	hai->hai_fid = info->mti_body->mbo_fid1;
	hai->hai_len = sizeof(*hai);

	rc = mdt_hsm_get_actions(info, hal);
	if (rc)
		GOTO(out_free, rc);

	/* cookie is used to give back request status */
	if (hai->hai_cookie == 0)
		hca->hca_state = HPS_WAITING;
	else
		hca->hca_state = HPS_RUNNING;

	switch (hai->hai_action) {
	case HSMA_NONE:
		hca->hca_action = HUA_NONE;
		break;
	case HSMA_ARCHIVE:
		hca->hca_action = HUA_ARCHIVE;
		break;
	case HSMA_RESTORE:
		hca->hca_action = HUA_RESTORE;
		break;
	case HSMA_REMOVE:
		hca->hca_action = HUA_REMOVE;
		break;
	case HSMA_CANCEL:
		hca->hca_action = HUA_CANCEL;
		break;
	default:
		hca->hca_action = HUA_NONE;
		CERROR("%s: Unknown hsm action: %d on "DFID"\n",
		       mdt_obd_name(info->mti_mdt),
		       hai->hai_action, PFID(&hai->hai_fid));
		break;
	}

	hca->hca_location = hai->hai_extent;

	EXIT;
out_free:
	MDT_HSM_FREE(hal, hal_size);
out_ucred:
	mdt_exit_ucred(info);
out:
	mdt_thread_info_fini(info);
	return rc;
}

/* Return true if a FID is present in an action list. */
static bool is_fid_in_hal(struct hsm_action_list *hal, const lustre_fid *fid)
{
	struct hsm_action_item *hai;
	int i;

	for (hai = hai_first(hal), i = 0;
	     i < hal->hal_count;
	     i++, hai = hai_next(hai)) {
		if (lu_fid_eq(&hai->hai_fid, fid))
			return true;
	}

	return false;
}

/**
 * Process the HSM actions described in a struct hsm_user_request.
 *
 * The action described in hur will be send to coordinator to be saved and
 * processed later or either handled directly if hur.hur_action is HUA_RELEASE.
 *
 * This is MDS_HSM_REQUEST RPC handler.
 */
int mdt_hsm_request(struct tgt_session_info *tsi)
{
	struct mdt_thread_info		*info;
	struct req_capsule		*pill = tsi->tsi_pill;
	struct hsm_request		*hr;
	struct hsm_user_item		*hui;
	struct hsm_action_list		*hal;
	struct hsm_action_item		*hai;
	const void			*data;
	int				 hui_list_size;
	int				 data_size;
	enum hsm_copytool_action	 action = HSMA_NONE;
	int				 hal_size, i, rc;
	ENTRY;

	hr = req_capsule_client_get(pill, &RMF_MDS_HSM_REQUEST);
	hui = req_capsule_client_get(pill, &RMF_MDS_HSM_USER_ITEM);
	data = req_capsule_client_get(pill, &RMF_GENERIC_DATA);

	if (tsi->tsi_mdt_body == NULL || hr == NULL || hui == NULL || data == NULL)
		RETURN(-EPROTO);

	/* Sanity check. Nothing to do with an empty list */
	if (hr->hr_itemcount == 0)
		RETURN(0);

	hui_list_size = req_capsule_get_size(pill, &RMF_MDS_HSM_USER_ITEM,
					     RCL_CLIENT);
	if (hui_list_size < hr->hr_itemcount * sizeof(*hui))
		RETURN(-EPROTO);

	data_size = req_capsule_get_size(pill, &RMF_GENERIC_DATA, RCL_CLIENT);
	if (data_size != hr->hr_data_len)
		RETURN(-EPROTO);

	info = tsi2mdt_info(tsi);
	/* Only valid if client is remote */
	rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
	if (rc)
		GOTO(out, rc);

	switch (hr->hr_action) {
	/* code to be removed in hsm1_merge and final patch */
	case HUA_RELEASE:
		CERROR("Release action is not working in hsm1_coord\n");
		GOTO(out_ucred, rc = -EINVAL);
		break;
	/* end of code to be removed */
	case HUA_ARCHIVE:
		action = HSMA_ARCHIVE;
		break;
	case HUA_RESTORE:
		action = HSMA_RESTORE;
		break;
	case HUA_REMOVE:
		action = HSMA_REMOVE;
		break;
	case HUA_CANCEL:
		action = HSMA_CANCEL;
		break;
	default:
		CERROR("Unknown hsm action: %d\n", hr->hr_action);
		GOTO(out_ucred, rc = -EINVAL);
	}

	hal_size = sizeof(*hal) + cfs_size_round(MTI_NAME_MAXLEN) /* fsname */ +
		   (sizeof(*hai) + cfs_size_round(hr->hr_data_len)) *
		   hr->hr_itemcount;

	MDT_HSM_ALLOC(hal, hal_size);
	if (hal == NULL)
		GOTO(out_ucred, rc = -ENOMEM);

	hal->hal_version = HAL_VERSION;
	hal->hal_archive_id = hr->hr_archive_id;
	hal->hal_flags = hr->hr_flags;
	obd_uuid2fsname(hal->hal_fsname, mdt_obd_name(info->mti_mdt),
			MTI_NAME_MAXLEN);

	hal->hal_count = 0;
	hai = hai_first(hal);
	for (i = 0; i < hr->hr_itemcount; i++, hai = hai_next(hai)) {
		/* Get rid of duplicate entries. Otherwise we get
		 * duplicated work in the llog. */
		if (is_fid_in_hal(hal, &hui[i].hui_fid))
			continue;

		hai->hai_action = action;
		hai->hai_cookie = 0;
		hai->hai_gid = 0;
		hai->hai_fid = hui[i].hui_fid;
		hai->hai_extent = hui[i].hui_extent;
		memcpy(hai->hai_data, data, hr->hr_data_len);
		hai->hai_len = sizeof(*hai) + hr->hr_data_len;

		hal->hal_count++;
	}

	rc = mdt_hsm_add_actions(info, hal, false);

	MDT_HSM_FREE(hal, hal_size);

	GOTO(out_ucred, rc);

out_ucred:
	mdt_exit_ucred(info);
out:
	mdt_thread_info_fini(info);
	return rc;
}

int mdt_hsm_policy_rule_init(struct mdt_hsm_policy *policy,
			     const char *expression)
{
	int			 rc = 0;
	struct obd_policy_value	*value = NULL;
	struct obd_policy_value	*n;
	struct list_head	 list;
	struct list_head	 old_list;
	__u64			 valid = 0;
	ENTRY;

	INIT_LIST_HEAD(&old_list);
	INIT_LIST_HEAD(&list);

	rc = obd_policy_value_init(&list, &value, &valid,
				   expression);
	if (rc != 0)
		RETURN(rc);

	write_lock(&policy->mhp_lock);
	list_splice_init(&policy->mhp_values, &old_list);
	list_splice_init(&list, &policy->mhp_values);
	policy->mhp_rule = value;
	policy->mhp_valid = valid;
	write_unlock(&policy->mhp_lock);

	list_for_each_entry_safe(value, n, &old_list,
				 opv_linkage) {
		list_del_init(&value->opv_linkage);
		OBD_FREE_PTR(value);
	}
	RETURN(rc);
}

static int mdt_hsm_env_init(struct lu_env *env, struct mdt_device *mdt)
{
	int			 rc;
	struct mdt_thread_info	*info;

	rc = lu_env_init(env, LCT_MD_THREAD);
	if (rc)
		return rc;

	info = lu_context_key_get(&env->le_ctx, &mdt_thread_key);
	LASSERT(info != NULL);
	memset(info, 0, sizeof *info);
	info->mti_env = env;
	info->mti_mdt = mdt;

	return 0;
}

static void mdt_hsm_env_fini(struct lu_env *env)
{
	lu_env_fini(env);
}

int mdt_hsm_attr_get(struct mdt_device *mdt, const struct lu_fid *fid,
		     struct md_attr **ma)
{
	struct md_attr		*tmp_ma = NULL;
	struct lu_env		 env;
	struct mdt_object	*obj = NULL;
	struct md_object	*next = NULL;
	int			 rc = 0;
	ENTRY;

	if (!fid_is_sane(fid))
		RETURN(-EINVAL);

	rc = mdt_hsm_env_init(&env, mdt);
	if (rc != 0)
		RETURN(rc);

	obj = mdt_object_find(&env, mdt, fid);
	if (IS_ERR(obj))
		GOTO(out_env, rc = PTR_ERR(obj));

	if (!mdt_object_exists(obj))
		GOTO(out_obj, rc = -ENOENT);

	OBD_ALLOC_PTR(tmp_ma);
	if (tmp_ma == NULL)
		GOTO(out_obj, rc = -ENOMEM);

	next = mdt_object_child(obj);

	tmp_ma->ma_need = MA_INODE;
	tmp_ma->ma_valid = 0;
	rc = mo_attr_get(&env, next, tmp_ma);
	if (rc)
		GOTO(out_ma, rc);
	tmp_ma->ma_valid |= MA_INODE;
	*ma = tmp_ma;
	GOTO(out_obj, rc = 0);
out_ma:
	OBD_FREE_PTR(tmp_ma);
out_obj:
	mdt_object_put(&env, obj);
out_env:
	mdt_hsm_env_fini(&env);
	RETURN(rc);
}

static int mdt_hsm_policy_work_add(struct mdt_hsm_policy *policy,
				   const struct lu_fid *fid,
				   __u64 operations)
{
	struct mdt_hsm_policy_work	*work;
	struct mdt_hsm_policy_work	*tmp;
	int				 rc = 0;
	bool				 wakeup = false;
	bool				 redundant = false;
	ENTRY;

	OBD_ALLOC_PTR(work);
	if (work == NULL)
		GOTO(out, rc = -ENOMEM);

	work->mhpw_fid = *fid;
	work->mhpw_operations = operations;
	work->mhpw_archive_id = policy->mhp_archive_id;

	spin_lock(&policy->mhp_thread_lock);
	if (list_empty(&policy->mhp_work_list))
		wakeup = true;
	/* Find redundant work and append the opc if possible */
	list_for_each_entry(tmp, &policy->mhp_work_list, mhpw_linkage) {
		if (!lu_fid_eq(&tmp->mhpw_fid, fid))
			break;

		/* TODO: cancel if possible */
		if ((tmp->mhpw_operations | operations) ==
		    tmp->mhpw_operations)
			redundant = true;
	}
	if (!redundant)
		list_add_tail(&work->mhpw_linkage, &policy->mhp_work_list);
	spin_unlock(&policy->mhp_thread_lock);

	if (redundant) {
		OBD_FREE_PTR(work);
	} else {
		if (wakeup)
			wake_up_all(&policy->mhp_waitq);
	}

out:
	RETURN(rc);
}

void mdt_hsm_policy_trigger(struct mdt_device *mdt, struct mdt_object *obj,
			    struct md_attr *ma)
{
	int			 rc;
	struct mdt_hsm_policy	*policy = &mdt->mdt_hsm_policy;
	__u64			 result = 0;
	__u64			 operations;
	struct timeval		 sys_time;
	ENTRY;

	if (!policy->mhp_archive_id)
		RETURN_EXIT;

	read_lock(&policy->mhp_lock);

	if (policy->mhp_valid & OBD_POLICY_VALID_SYS_TIME)
		do_gettimeofday(&sys_time);

	rc = obd_policy_rule_postorder_traverse(policy->mhp_rule,
						obd_policy_rule_result_func,
						&result, true, &ma->ma_attr,
						&sys_time);
	if (rc) {
		CERROR("failed to evaluate HSM policy rule, rc = %d\n", rc);
		goto out;
	}

	operations = result & OPC_HSMA_BIT_MASK;
	if (operations == 0)
		goto out;

	/* TODO: pass object directly? */
	rc = mdt_hsm_policy_work_add(policy, mdt_object_fid(obj), operations);
	if (rc) {
		CERROR("failed to add HSM policy work, rc = %d\n", rc);
	}
	CERROR("XXX value of policy %llu\n", result);
out:
	read_unlock(&policy->mhp_lock);
	RETURN_EXIT;
}

static int mdt_hsm_policy_work_handler(struct ptlrpc_thread *thread,
				       struct lu_env *env,
				       struct mdt_hsm_policy *policy,
				       struct mdt_hsm_policy_work *work)
{
	struct mdt_thread_info	*info;
	struct hsm_action_list	*hal;
	struct hsm_action_item	*hai;
	int			 hal_size;
	int			 rc;
	ENTRY;

	info = lu_context_key_get(&env->le_ctx, &mdt_thread_key);
	LASSERT(info != NULL);

	/* Coordinator information */
	hal_size = sizeof(*hal) +
		cfs_size_round(MTI_NAME_MAXLEN) /* fsname */ +
		cfs_size_round(sizeof(*hai));
	MDT_HSM_ALLOC(hal, hal_size);
	if (hal == NULL)
		GOTO(out, rc = -ENOMEM);

	hal->hal_version = HAL_VERSION;
	hal->hal_archive_id = work->mhpw_archive_id;
	hal->hal_flags = 0;
	obd_uuid2fsname(hal->hal_fsname, mdt_obd_name(info->mti_mdt),
			MTI_NAME_MAXLEN);
	hal->hal_count = 1;
	hai = hai_first(hal);
	/* TODO: check the work->mhpw_operations */
	hai->hai_action = HSMA_ARCHIVE;
	hai->hai_cookie = 0;
	hai->hai_gid = 0;
	hai->hai_fid = work->mhpw_fid;
	hai->hai_extent.offset = 0;
	hai->hai_extent.length = -1;
	hai->hai_len = sizeof(*hai);
	rc = mdt_hsm_add_actions(info, hal, true);

	MDT_HSM_FREE(hal, hal_size);
	CERROR("XXX handling HSM work\n");
out:
	RETURN(rc);
}

static int mdt_hsm_policy_main(void *args)
{
	struct ptlrpc_thread		*thread = args;
	struct mdt_hsm_policy		*policy = thread->t_data;
	struct lu_env			 env;
	struct l_wait_info		 lwi    = { 0 };
	int				 rc;
	struct mdt_hsm_policy_work	*work;
	ENTRY;

	rc = mdt_hsm_env_init(&env, policy->mhp_mdt);
	spin_lock(&policy->mhp_thread_lock);
	thread_set_flags(thread, rc != 0 ? SVC_STOPPED : SVC_RUNNING);
	spin_unlock(&policy->mhp_thread_lock);
	wake_up_all(&thread->t_ctl_waitq);
	if (rc != 0)
		RETURN(rc);

	spin_lock(&policy->mhp_thread_lock);
	while (1) {
		if (unlikely(!thread_is_running(thread)))
			break;

		while (!list_empty(&policy->mhp_work_list)) {
			work = list_entry(policy->mhp_work_list.next,
					  struct mdt_hsm_policy_work,
					  mhpw_linkage);
			list_del_init(&work->mhpw_linkage);
			spin_unlock(&policy->mhp_thread_lock);

			rc = mdt_hsm_policy_work_handler(thread, &env, policy,
							 work);
			if (rc) {
				/* TODO: add the work back? */
				CERROR("failed to handle the HSM policy of "
				       "file "DFID", rc = %d\n",
				       PFID(&work->mhpw_fid), rc);
			} else {
				CDEBUG(D_HSM, "handled the HSM policy of "
				       "file "DFID"\n",
				       PFID(&work->mhpw_fid));
			}
			OBD_FREE_PTR(work);
			spin_lock(&policy->mhp_thread_lock);
		}

		spin_unlock(&policy->mhp_thread_lock);
		l_wait_event(policy->mhp_waitq,
			     !list_empty(&policy->mhp_work_list) ||
			     !thread_is_running(thread),
			     &lwi);
		spin_lock(&policy->mhp_thread_lock);
	}

	while (!list_empty(&policy->mhp_work_list)) {
		work = list_entry(policy->mhp_work_list.next,
				  struct mdt_hsm_policy_work,
				  mhpw_linkage);
		list_del_init(&work->mhpw_linkage);
		spin_unlock(&policy->mhp_thread_lock);
		OBD_FREE_PTR(work);
		spin_lock(&policy->mhp_thread_lock);
	}

	thread_set_flags(thread, SVC_STOPPED);
	spin_unlock(&policy->mhp_thread_lock);
	wake_up_all(&thread->t_ctl_waitq);
	mdt_hsm_env_fini(&env);
	RETURN(0);
}

static void mdt_hsm_policy_rule_fini(struct mdt_hsm_policy *policy)
{
	struct obd_policy_value	*value, *n;
	ENTRY;

	list_for_each_entry_safe(value, n, &policy->mhp_values,
				 opv_linkage) {
		list_del_init(&value->opv_linkage);
		OBD_FREE_PTR(value);
	}

	RETURN_EXIT;
}

static int mdt_hsm_policy_thread_stop(struct mdt_hsm_policy *policy,
				      struct ptlrpc_thread *thread)
{
	struct l_wait_info	 lwi	= { 0 };

	spin_lock(&policy->mhp_thread_lock);
	if (thread_is_init(thread) || thread_is_stopped(thread)) {
		spin_unlock(&policy->mhp_thread_lock);

		return -EALREADY;
	}

	thread_set_flags(thread, SVC_STOPPING);
	spin_unlock(&policy->mhp_thread_lock);
	wake_up_all(&policy->mhp_waitq);
	l_wait_event(thread->t_ctl_waitq,
		     thread_is_stopped(thread),
		     &lwi);

	return 0;
}

static int mdt_hsm_policy_thread_number = 8;
module_param(mdt_hsm_policy_thread_number, uint, 0644);
MODULE_PARM_DESC(mdt_hsm_policy_thread_number, "Thread number of HSM policy");

static void mdt_hsm_policy_threads_stop(struct mdt_hsm_policy *policy)
{
	struct ptlrpc_thread	*thread;
	int			 i;
	ENTRY;

	for (i = 0; i < mdt_hsm_policy_thread_number; i++) {
		thread = &policy->mhp_threads[i];
		mdt_hsm_policy_thread_stop(policy, thread);
	}

	OBD_FREE(policy->mhp_threads, sizeof(*policy->mhp_threads) *
		 mdt_hsm_policy_thread_number);
	EXIT;
}

static int mdt_hsm_policy_thread_start(struct mdt_hsm_policy *policy,
				       struct ptlrpc_thread *thread,
				       int index)
{
	struct l_wait_info	 lwi = { 0 };
	struct task_struct	*task;
	int			 rc;

	task = kthread_run(mdt_hsm_policy_main, thread, "mdt_hsm_policy_%d",
			   index);
	if (IS_ERR(task)) {
		rc = PTR_ERR(task);
		CERROR("cannot start cache thread: rc = %d\n", rc);
		GOTO(out, rc = -ENOMEM);
	} else {
		rc = 0;
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_running(thread) ||
			     thread_is_stopped(thread),
			     &lwi);
	}

out:
	return rc;
}

static int mdt_hsm_policy_threads_start(struct mdt_hsm_policy *policy)
{
	struct ptlrpc_thread	*thread;
	int			 i;
	int			 rc;
	ENTRY;

	OBD_ALLOC(policy->mhp_threads, sizeof(*policy->mhp_threads) *
		  mdt_hsm_policy_thread_number);
	if (policy->mhp_threads == NULL)
		RETURN(-ENOMEM);

	for (i = 0; i < mdt_hsm_policy_thread_number; i++) {
		thread = &policy->mhp_threads[i];
		thread->t_data = policy;
		/**
		 * Need to init flag here so that mdt_hsm_policy_threads_stop()
		 * can skip initial threads
		 */
		thread_set_flags(thread, 0);
		init_waitqueue_head(&thread->t_ctl_waitq);
	}

	for (i = 0; i < mdt_hsm_policy_thread_number; i++) {
		thread = &policy->mhp_threads[i];
		rc = mdt_hsm_policy_thread_start(policy, thread, i);
		if (rc != 0) {
			mdt_hsm_policy_threads_stop(policy);
			RETURN(rc);
		}
	}

	RETURN(0);
}

int mdt_hsm_policy_init(struct mdt_device *mdt)
{
	int			 rc;
	struct mdt_hsm_policy	*policy = &mdt->mdt_hsm_policy;
	ENTRY;

	policy->mhp_mdt = mdt;
	policy->mhp_fid.f_seq = 0x0;
	policy->mhp_fid.f_oid = 0x0;
	policy->mhp_fid.f_ver = 0x0;
	policy->mhp_valid = 0;
	policy->mhp_rule = NULL;
	policy->mhp_archive_id = 0;
	rwlock_init(&policy->mhp_lock);
	spin_lock_init(&policy->mhp_thread_lock);
	INIT_LIST_HEAD(&policy->mhp_work_list);
	INIT_LIST_HEAD(&policy->mhp_values);
	init_waitqueue_head(&policy->mhp_waitq);

	rc = mdt_hsm_policy_rule_init(policy, "0");
	if (rc) {
		CERROR("failed to init HSM policy rule, rc = %d\n", rc);
		GOTO(out, rc);
	}

	rc = mdt_hsm_policy_threads_start(policy);
	if (rc) {
		CERROR("failed to start threads of HSM policy, rc = %d\n", rc);
		GOTO(out_rule, rc);
	}
	RETURN(0);
out_rule:
	mdt_hsm_policy_rule_fini(policy);
out:
	RETURN(rc);
}

void mdt_hsm_policy_fini(struct mdt_device *mdt)
{
	struct mdt_hsm_policy	*policy = &mdt->mdt_hsm_policy;

	mdt_hsm_policy_threads_stop(policy);
	mdt_hsm_policy_rule_fini(policy);

	LASSERT(list_empty(&policy->mhp_work_list));
	LASSERT(list_empty(&policy->mhp_values));
}
