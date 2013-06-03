/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * lustre/mdt/mdt_hsm_cdt_client.c
 *
 * Lustre HSM Coordinator
 *
 * Author: Jacques-Charles Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <obd_support.h>
#include <lustre_net.h>
#include <lustre_export.h>
#include <obd.h>
#include <obd_lov.h>
#include <lprocfs_status.h>
#include <lustre_log.h>
#include "mdt_internal.h"

/* fake functions, will be removed with LU-3343 */
struct mdt_object *mdt_cdt_get_md_hsm(struct mdt_thread_info *mti,
				      struct lu_fid *fid, struct md_hsm *hsm,
				      struct mdt_lock_handle *lh)
{
	return ERR_PTR(-EINVAL);
}

int mdt_cdt_check_action_compat(struct hsm_action_item *hai, int hal_an,
				__u64 rq_flags, struct md_hsm *hsm)
{
	return 0;
}

int mdt_coordinator_wakeup(struct mdt_device *mdt)
{
	return 0;
}

/**
 * data passed to llog_cat_process() callback
 * to find compatible requests
 */
struct data_compat_cb {
	struct coordinator	*cdt;
	struct hsm_action_list	*hal;
};

/**
 * llog_cat_process() callback, used to find record
 * compatibles with a new hsm_action_list
 * \param env [IN] environment
 * \param llh [IN] llog handle
 * \param hdr [IN] llog record
 * \param data [IN] cb data = data_compat_cb
 * \retval 0 success
 * \retval -ve failure
 */
static int mdt_agent_find_compatible_cb(const struct lu_env *env,
					struct llog_handle *llh,
					struct llog_rec_hdr *hdr,
					void *data)
{
	struct llog_agent_req_rec	*larr;
	struct data_compat_cb		*dccb;
	struct hsm_action_item		*hai;
	int				 i;
	ENTRY;

	larr = (struct llog_agent_req_rec *)hdr;
	dccb = data;
	/* a compatible request must be WAITING or STARTED
	 * and not a cancel */
	if (((larr->arr_status != ARS_WAITING) &&
	    (larr->arr_status != ARS_STARTED)) ||
	    (larr->arr_hai.hai_action == HSMA_CANCEL))
		RETURN(0);

	hai = hai_zero(dccb->hal);
	for (i = 0 ; i < dccb->hal->hal_count ; i++, hai = hai_next(hai)) {
		/* if request is a CANCEL:
		 * if cookie set in the request, no need to find a compatible
		 *  one the cookie in the request is directly used.
		 * if cookie is not set, we use the fid to find the request
		 *  to cancel (the "compatible" one)
		 *  if the caller sets the cookie, we assume he also sets the
		 *  arr_archive_id
		 */
		if ((hai->hai_action == HSMA_CANCEL) && (hai->hai_cookie != 0))
			continue;

		if (!lu_fid_eq(&hai->hai_fid, &larr->arr_hai.hai_fid))
			continue;

		/* HSMA_NONE is used to find running request for some fid */
		if (hai->hai_action == HSMA_NONE) {
			dccb->hal->hal_archive_id = larr->arr_archive_id;
			dccb->hal->hal_flags = larr->arr_flags;
			*hai = larr->arr_hai;
			continue;
		}
		/* in V1 we do not manage partial transfert
		 * so extent is always whole file
		 */
		hai->hai_cookie = larr->arr_hai.hai_cookie;
		/* we read the archive number from the request we cancel */
		if ((hai->hai_action == HSMA_CANCEL) &&
		    (dccb->hal->hal_archive_id == 0))
			dccb->hal->hal_archive_id = larr->arr_archive_id;
	}
	RETURN(0);
}

/**
 * find compatible requests already recorded
 * \param env [IN] environment
 * \param mdt [IN] MDT device
 * \param hal [IN/OUT] new request
 *    cookie set to compatible found or to 0 if not found
 *    for cancel request, see callback mdt_agent_find_compatible_cb()
 * \retval 0 success
 * \retval -ve failure
 */
static int mdt_agent_find_compatible(const struct lu_env *env,
				     struct mdt_device *mdt,
				     struct hsm_action_list *hal)
{
	struct data_compat_cb	 dccb;
	struct hsm_action_item	*hai;
	int			 rc, i, ok_cnt;
	ENTRY;

	ok_cnt = 0;
	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++, hai = hai_next(hai)) {
		/* in a cancel request hai_cookie may be set by caller to
		 * show the request to be canceled
		 * if not we need to search by fid
		 */
		if ((hai->hai_action == HSMA_CANCEL) && (hai->hai_cookie != 0))
			ok_cnt++;
		else
			hai->hai_cookie = 0;
	}

	/* if all requests are cancel with cookie, no need to find compatible */
	if (ok_cnt == hal->hal_count)
		RETURN(0);

	dccb.cdt = &mdt->mdt_coordinator;
	dccb.hal = hal;

	rc = cdt_llog_process(env, mdt, mdt_agent_find_compatible_cb, &dccb);

	RETURN(rc);
}

/**
 * check if an action is really needed
 * \param hai [IN] request description
 * \param hal_an [IN] request archive number (not used)
 * \param rq_flags [IN] request flags
 * \param hsm [IN] file hsm metadata
 * \retval boolean
 */
static int mdt_check_action_needed(struct hsm_action_item *hai, int hal_an,
				   __u64 rq_flags, struct md_hsm *hsm)
{
	int	 rc = 0;
	int	 hsm_flags;
	ENTRY;

	if (rq_flags & HSM_FORCE_ACTION)
		RETURN(1);

	hsm_flags = hsm->mh_flags;
	switch (hai->hai_action) {
	case HSMA_ARCHIVE:
		if ((hsm_flags & HS_DIRTY) || !(hsm_flags & HS_ARCHIVED))
			rc = 1;
		break;
	case HSMA_RESTORE:
		/* if file is dirty we must return an error, this function
		 * cannot, so we ask for an action and
		 * mdt_cdt_check_action_compat() will return an error
		 */
		if ((hsm_flags & HS_RELEASED) || (hsm_flags & HS_DIRTY))
			rc = 1;
		break;
	case HSMA_REMOVE:
		if (hsm_flags & (HS_ARCHIVED | HS_EXISTS))
			rc = 1;
		break;
	case HSMA_CANCEL:
		rc = 1;
		break;
	}
	CDEBUG(D_HSM, "fid="DFID" action=%s rq_flags="LPX64
		      " extent="LPX64"-"LPX64" hsm_flags=%X %s\n",
		      PFID(&hai->hai_fid),
		      hsm_copytool_action2name(hai->hai_action), rq_flags,
		      hai->hai_extent.offset, hai->hai_extent.length,
		      hsm->mh_flags,
		      (rc ? "action needed" : "no action needed"));

	RETURN(rc);
}

/**
 * test sanity of an hal
 * fid must be valid
 * action must be known
 * \param hal [IN]
 * \retval boolean
 */
static int hal_is_sane(struct hsm_action_list *hal)
{
	int			 i;
	struct hsm_action_item	*hai;
	ENTRY;

	if (hal->hal_count == 0)
		RETURN(0);

	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++) {
		if (!fid_is_sane(&hai->hai_fid))
			RETURN(0);
		switch (hai->hai_action) {
		case HSMA_NONE:
		case HSMA_ARCHIVE:
		case HSMA_RESTORE:
		case HSMA_REMOVE:
		case HSMA_CANCEL:
			break;
		default:
			RETURN(0);
		}
	}
	RETURN(1);
}

/*
 * Coordinator external API
 */

/**
 * register a list of requests
 * \param mti [IN]
 * \param hal [IN] list of requests
 * \param compound_id [OUT] id of the compound request
 * \retval 0 success
 * \retval -ve failure
 * in case of restore, caller must hold layout lock
 */
int mdt_hsm_coordinator_actions(struct mdt_thread_info *mti,
				struct hsm_action_list *hal,
				__u64 *compound_id)
{
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct obd_export	*exp;
	struct hsm_action_item	*hai;
	int			 rc = 0, i;
	struct md_hsm		 mh;
	int			 restore_request = 0;
	struct mdt_object	*obj = NULL;
	ENTRY;

	/* no coordinator started, so we cannot serve requests */
	if (cdt->cdt_state == CDT_STOPPED)
		RETURN(-EAGAIN);

	if (!hal_is_sane(hal))
		RETURN(-EINVAL);

	exp = mti->mti_exp;

	*compound_id = cfs_atomic_inc_return(&cdt->cdt_compound_id);

	/* search for compatible request, if found hai_cookie is set
	 * to the request cookie
	 * it is also used to set the cookie for cancel request by fid
	 */
	rc = mdt_agent_find_compatible(mti->mti_env, mdt, hal);
	if (rc)
		GOTO(out, rc);

	hai = hai_zero(hal);
	for (i = 0; i < hal->hal_count; i++, hai = hai_next(hai)) {
		int archive_id;
		__u64 flags;

		/* default archive number is the one explicitly specified */
		archive_id = hal->hal_archive_id;
		flags = hal->hal_flags;

		rc = -1;

		/* by default, data fid is same as lustre fid */
		/* the volatile data fid will be created by copy tool and
		 * send from the agent through the progress call */
		memcpy(&hai->hai_dfid, &hai->hai_fid, sizeof(hai->hai_fid));

		/* done here to manage first and redundant requests cases */
		if (hai->hai_action == HSMA_RESTORE)
			restore_request = 1;

		/* test result of mdt_agent_find_compatible()
		 * if request redundant or cancel of nothing
		 * do not record
		 */
		/* redundant case */
		if ((hai->hai_action != HSMA_CANCEL) && (hai->hai_cookie != 0))
			continue;
		/* cancel nothing case */
		if ((hai->hai_action == HSMA_CANCEL) && (hai->hai_cookie == 0))
			continue;

		/* new request or cancel request
		 * we search for HSM status flags to check for compatibility
		 * if restore, we take the group lock
		 */

		/* if action is cancel, also no need to check */
		if (hai->hai_action == HSMA_CANCEL)
			goto record;

		/* get hsm attributes */
		obj = mdt_cdt_get_md_hsm(mti, &hai->hai_fid, &mh, NULL);
		if (IS_ERR(obj)) {
			/* in case of archive remove, lustre file
			 * is not mandatory */
			if (hai->hai_action == HSMA_REMOVE)
				goto record;
			GOTO(out, rc = PTR_ERR(obj));
		}

		/* Check if an action is needed, compare request
		 * and HSM flags status */
		if (!mdt_check_action_needed(hai, archive_id, flags, &mh))
			continue;

		/* Check if file request is compatible with HSM flags status
		 * and stop at first incompatible
		 */
		if (!mdt_cdt_check_action_compat(hai, archive_id, flags, &mh))
			GOTO(out, rc = -EPERM);

		/* for cancel archive number is taken from canceled request
		 * for other request, we take from lma if not specified,
		 * this works also for archive because the default value is 0
		 * /!\ there is a side effect: in case of restore on multiple
		 * files which are in different backend, the initial compound
		 * request will be split in multiple requests because we cannot
		 * warranty an agent can serve any combinaison of archive
		 * backend
		 */
		if ((hai->hai_action != HSMA_CANCEL) &&
		    (archive_id == 0))
			archive_id = mh.mh_arch_id;

		/* if restore, take an exclusive lock on layout */
		if (hai->hai_action == HSMA_RESTORE) {
			struct cdt_restore_handle	*crh;
			struct mdt_object		*child;

			OBD_ALLOC_PTR(crh);
			if (crh == NULL)
				GOTO(out, rc = -ENOMEM);

			crh->crh_fid = hai->hai_fid;
			/* in V1 only whole file is supported
			crh->extent.start = hai->hai_extent.offset;
			crh->extent.end = hai->hai_extent.offset +
					    hai->hai_extent.length;
			 */
			crh->crh_extent.start = 0;
			crh->crh_extent.end = OBD_OBJECT_EOF;

			mdt_lock_reg_init(&crh->crh_lh, LCK_EX);
			child = mdt_object_find_lock(mti, &crh->crh_fid,
						     &crh->crh_lh,
						     MDS_INODELOCK_LAYOUT);
			if (IS_ERR(child)) {
				rc = PTR_ERR(child);
				CERROR("%s: Could not take layout lock for "DFID
				       " rc=%d\n", mdt_obd_name(mdt),
				       PFID(&crh->crh_fid), rc);
				OBD_FREE_PTR(crh);
				GOTO(out, rc);
			}
			/* we choose to not keep a keep a reference
			 * on the object during the restore time which can be
			 * very long */
			mdt_object_put(mti->mti_env, child);

			down(&cdt->cdt_restore_lock);
			cfs_list_add_tail(&crh->crh_list,
					  &cdt->cdt_restore_hdl);
			up(&cdt->cdt_restore_lock);
		}
record:
		/* record request */
		rc = mdt_agent_record_add(mti->mti_env, mdt, *compound_id,
					  archive_id, flags, hai);
		if (rc)
			GOTO(out, rc);
	}
	if ((restore_request == 1) &&
	    (cdt->cdt_policy & CDT_NONBLOCKING_RESTORE))
		rc = -ENODATA;
	else
		rc = 0;
	EXIT;
out:
	/* if work has been added, wake up coordinator */
	if ((rc == 0) || (rc == -ENODATA))
		mdt_coordinator_wakeup(mdt);

	return rc;
}

/**
 * get running action on a fid list or from cookie
 * \param mti [IN]
 * \param hal [IN/OUT] requests
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_coordinator_get_running(struct mdt_thread_info *mti,
				    struct hsm_action_list *hal)
{
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct hsm_action_item	*hai;
	int			 i;
	ENTRY;

	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++, hai = hai_next(hai)) {
		struct cdt_agent_req *car;

		if (!fid_is_sane(&hai->hai_fid))
			RETURN(-EINVAL);

		car = mdt_cdt_find_request(cdt, 0, &hai->hai_fid);
		if (IS_ERR(car)) {
			hai->hai_cookie = 0;
			hai->hai_action = HSMA_NONE;
		} else {
			*hai = *car->car_hai;
			mdt_cdt_put_request(car);
		}
	}
	RETURN(0);
}

/**
 * get registered action on a fid list
 * \param mti [IN]
 * \param hal [IN/OUT] requests
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_coordinator_get_actions(struct mdt_thread_info *mti,
				    struct hsm_action_list *hal)
{
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct hsm_action_item	*hai;
	int			 i, rc;
	ENTRY;

	/* no coordinator started, so we cannot serve requests */
	if (cdt->cdt_state == CDT_STOPPED)
		RETURN(-EAGAIN);

	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++, hai = hai_next(hai)) {
		hai->hai_action = HSMA_NONE;
		if (!fid_is_sane(&hai->hai_fid))
			RETURN(-EINVAL);
	}

	/* 1st we search in recorded requests */
	rc = mdt_agent_find_compatible(mti->mti_env, mdt, hal);
	if (rc)
		RETURN(rc);

	/* 2nd we search if the request are running
	 * cookie is cleared to tell to caller, the request is
	 * waiting
	 * we could in place use the record status, but in the future
	 * we may want do give back dynamic informations on the
	 * running request
	 */
	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++, hai = hai_next(hai)) {
		struct cdt_agent_req *car;

		car = mdt_cdt_find_request(cdt, hai->hai_cookie, NULL);
		if (IS_ERR(car)) {
			hai->hai_cookie = 0;
		} else {
			__u64 data_moved;

			mdt_cdt_get_work_done(car, &data_moved);
			/* this is just to give the volume of data moved
			 * it means data_moved data have been moved from the
			 * original request but we do not know which one
			 */
			hai->hai_extent.length = data_moved;
			mdt_cdt_put_request(car);
		}
	}

	RETURN(0);
}

