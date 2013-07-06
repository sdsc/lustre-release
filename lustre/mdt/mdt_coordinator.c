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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 *  Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * lustre/mdt/mdt_coordinator.c
 *
 * Lustre HSM Coordinator
 *
 * Author: Jacques-Charles Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Author: Thomas Leibovici <thomas.leibovici@cea.fr>
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

#define CDT_LOV_EA_SZ lov_mds_md_size(LOV_MAX_STRIPE_COUNT, LOV_MAGIC_V3)

static struct lprocfs_vars lprocfs_mdt_hsm_vars[];

/**
 * get md_attr on a fid
 * \param mti [IN] context
 * \param fid [IN] object fid
 * \param ma [OUT] object attr
 * \param lh [IN] lock handle, if non NULL, it has to be initialized
 *  if NULL, we use MDT_LH_HSM as a temporary variable
 * \retval obj
 */
static struct mdt_object *hsm_get_md_attr(struct mdt_thread_info *mti,
					  const struct lu_fid *fid,
					  struct md_attr *ma,
					  struct mdt_lock_handle *lh)
{
	struct mdt_object	*obj;
	int			 rc, unlock = 0;
	ENTRY;

	if (lh == NULL) {
		lh = &mti->mti_lh[MDT_LH_HSM];
		unlock = 1;
		mdt_lock_handle_init(lh);
		mdt_lock_reg_init(lh, LCK_CR);
	}

	/* find object by fid and lock it to get/set HSM flags */
	obj = mdt_object_find_lock(mti, fid, lh, MDS_INODELOCK_UPDATE);
	if (IS_ERR(obj))
		RETURN(obj);

	if (mdt_object_exists(obj) != 1) {
		/* no more object */
		mdt_object_unlock_put(mti, obj, lh,
				      MDS_INODELOCK_UPDATE);
		RETURN(ERR_PTR(-ENOENT));
	}

	rc = mdt_attr_get_complex(mti, obj, ma);
	if (unlock || rc)
		mdt_object_unlock_put(mti, obj, lh, 1);

	if (rc)
		RETURN(ERR_PTR(rc));

	RETURN(obj);
}

/**
 * get hsm attributes on a fid
 * \param mti [IN] context
 * \param fid [IN] object fid
 * \param hsm [OUT] hsm meta data
 * \param lh [IN] lock handle, if non NULL it has to be initialized
 * \retval obj
 */
struct mdt_object *mdt_hsm_get_md_hsm(struct mdt_thread_info *mti,
				      const struct lu_fid *fid,
				      struct md_hsm *hsm,
				      struct mdt_lock_handle *lh)
{
	struct md_attr		*ma;
	struct mdt_object	*obj;
	ENTRY;

	ma = &mti->mti_attr;
	ma->ma_need = MA_HSM;
	ma->ma_valid = 0;

	obj = hsm_get_md_attr(mti, fid, ma, lh);
	if (!IS_ERR(obj)) {
		if (ma->ma_valid & MA_HSM)
			*hsm = ma->ma_hsm;
		else
			memset(hsm, 0, sizeof(*hsm));
	}
	ma->ma_valid = 0;
	RETURN(obj);
}

/**
 * set hsm attributes on a fid
 * \param mti [IN] context
 * \param obj [IN] locked object
 * \param mh [OUT] hsm metadata
 * \param lh [IN] lock handle on object (if not NULL we unlock on return)
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_set_md_hsm(struct mdt_thread_info *mti, struct mdt_object *obj,
		       const struct md_hsm *mh, struct mdt_lock_handle *lh)
{
	int		 rc;
	ENTRY;

	rc = mdt_hsm_attr_set(mti, obj, mh);

	if (lh != NULL)
		mdt_object_unlock_put(mti, obj, lh, MDS_INODELOCK_UPDATE);

	RETURN(rc);
}

void dump_hal(const char *prefix, struct hsm_action_list *hal)
{
	int			 i, sz;
	struct hsm_action_item	*hai;
	char			 buf[12];

	if (hal->hal_count == 0)
		CERROR("Empty hsm_action_list\n");

	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++) {
		sz = hai->hai_len - sizeof(*hai);
		CDEBUG(D_HSM, "%s %d: fid="DFID" dfid="DFID
		       " compound/cookie="LPX64"/"LPX64
		       " action=%s archive#=%d flags="LPX64
		       " extent="LPX64"-"LPX64" gid="LPX64
		       " datalen=%d data=[%s]\n",
		       prefix, i,
		       PFID(&hai->hai_fid), PFID(&hai->hai_dfid),
		       hal->hal_compound_id, hai->hai_cookie,
		       hsm_copytool_action2name(hai->hai_action),
		       hal->hal_archive_id,
		       hal->hal_flags,
		       hai->hai_extent.offset,
		       hai->hai_extent.length,
		       hai->hai_gid, sz,
		       hai_dump_data_field(hai, buf, sizeof(buf)));
		hai = hai_next(hai);
	}
}

/**
 * data passed to llog_cat_process() callback
 * to scan requests and take actions
 */
struct data_scan_cb {
	struct mdt_thread_info		*mti;
	char				 fs_name[MTI_NAME_MAXLEN+1];
	/* request to be send to agents */
	int				 request_sz;	/** allocated size */
	int				 max_request;	/** vector size */
	int				 request_cnt;	/** used count */
	struct {
		int			 hal_sz;
		int			 hal_used_sz;
		struct hsm_action_list	*hal;
	} *request;
	/* records to be canceled */
	int				 max_cookie;	/** vector size */
	int				 cookie_cnt;	/** used count */
	__u64				*cookies;
};

/**
 *  llog_cat_process() callback, used to:
 *  - find waiting request and start action
 *  - purge canceled and done requests
 * \param env [IN] environment
 * \param llh [IN] llog handle
 * \param hdr [IN] llog record
 * \param data [IN/OUT] cb data = struct data_scan_cb
 * \retval 0 success
 * \retval -ve failure
 */
static int mdt_coordinator_cb(const struct lu_env *env,
			      struct llog_handle *llh,
			      struct llog_rec_hdr *hdr,
			      void *data)
{
	const struct llog_agent_req_rec	*larr;
	struct data_scan_cb		*dscb;
	struct hsm_action_item		*hai;
	struct mdt_device		*mdt;
	struct coordinator		*cdt;
	int				 rc;
	ENTRY;

	dscb = data;
	mdt = dscb->mti->mti_mdt;
	cdt = &mdt->mdt_coordinator;

	larr = (struct llog_agent_req_rec *)hdr;
	dump_llog_agent_req_rec("mdt_coordinator_cb(): ", larr);
	switch (larr->arr_status) {
	case ARS_WAITING: {
		int i, free, found;

		down(&cdt->cdt_counter_lock);
		if (cdt->cdt_request_count == cdt->cdt_max_request) {
			/* agents are full */
			up(&cdt->cdt_counter_lock);
			break;
		}

		/* first search if the request if known and
		 * if where is room in the request vector */
		free = -1;
		found = -1;
		for (i = 0 ; i < dscb->max_request ; i++) {
			if (dscb->request[i].hal == NULL) {
				free = i;
				continue;
			}
			if (dscb->request[i].hal->hal_compound_id ==
				larr->arr_compound_id) {
				found = i;
				continue;
			}
		}
		if ((found == -1) && (free == -1)) {
			/* unknown request and no more room for new request,
			 * continue scan for to find other entries for
			 * already found request
			 */
			up(&cdt->cdt_counter_lock);
			RETURN(0);
		}
		if (found == -1) {
			struct hsm_action_list *hal;

			/* request is not already known */
			/* allocates hai vector size just needs to be large
			 * enough */
			dscb->request[free].hal_sz =
				     sizeof(*dscb->request[free].hal) +
				     cfs_size_round(MTI_NAME_MAXLEN+1) +
				     2 * cfs_size_round(larr->arr_hai.hai_len);
			OBD_ALLOC(hal, dscb->request[free].hal_sz);
			if (!hal) {
				CERROR("%s: Cannot allocate memory (%d o)"
				       "for compound "LPX64"\n",
				       mdt_obd_name(mdt),
				       dscb->request[i].hal_sz,
				       larr->arr_compound_id);
				up(&cdt->cdt_counter_lock);
				RETURN(-ENOMEM);
			}
			hal->hal_version = HAL_VERSION;
			strncpy(hal->hal_fsname, dscb->fs_name,
				MTI_NAME_MAXLEN);
			hal->hal_fsname[MTI_NAME_MAXLEN] = '\0';
			hal->hal_compound_id = larr->arr_compound_id;
			hal->hal_archive_id = larr->arr_archive_id;
			hal->hal_flags = larr->arr_flags;
			hal->hal_count = 0;
			dscb->request[free].hal_used_sz = hal_size(hal);
			dscb->request[free].hal = hal;
			dscb->request_cnt++;
			found = free;
			hai = hai_zero(hal);
		} else {
			/* request is known */
			/* we check if record archive num is the same as the
			 * known request, if not we will serve it in multiple
			 * time because we do not know if the agent can serve
			 * multiple backend
			 */
			if (larr->arr_archive_id !=
			    dscb->request[found].hal->hal_archive_id) {
				up(&cdt->cdt_counter_lock);
				RETURN(0);
			}

			if (dscb->request[found].hal_sz <
			    dscb->request[found].hal_used_sz +
			     cfs_size_round(larr->arr_hai.hai_len)) {
				/* Not enough room, need an extension */
				void *hal_buffer;
				int sz;

				sz = 2 * dscb->request[found].hal_sz;
				OBD_ALLOC(hal_buffer, sz);
				if (!hal_buffer) {
					CERROR("%s: Cannot allocate memory "
					       "(%d o) for compound "LPX64"\n",
					       mdt_obd_name(mdt), sz,
					       larr->arr_compound_id);
					up(&cdt->cdt_counter_lock);
					RETURN(-ENOMEM);
				}
				memcpy(hal_buffer, dscb->request[found].hal,
				       dscb->request[found].hal_used_sz);
				OBD_FREE(dscb->request[found].hal,
					 dscb->request[found].hal_sz);
				dscb->request[found].hal = hal_buffer;
				dscb->request[found].hal_sz = sz;
			}
			hai = hai_zero(dscb->request[found].hal);
			for (i = 0 ; i < dscb->request[found].hal->hal_count ;
			     i++)
				hai = hai_next(hai);
		}
		memcpy(hai, &larr->arr_hai, larr->arr_hai.hai_len);
		hai->hai_cookie = larr->arr_hai.hai_cookie;
		hai->hai_gid = larr->arr_hai.hai_gid;

		dscb->request[found].hal_used_sz +=
						   cfs_size_round(hai->hai_len);
		dscb->request[found].hal->hal_count++;
		up(&cdt->cdt_counter_lock);
		break;
	}
	case ARS_STARTED: {
		struct cdt_agent_req *car;
		cfs_time_t last;

		/* we search for a running request
		 * error may happen if coordinator crashes or stopped
		 * with running request
		 */
		car = mdt_cdt_find_request(cdt, larr->arr_hai.hai_cookie, NULL);
		if (IS_ERR(car)) {
			last = larr->arr_req_create;
		} else {
			last = car->car_req_update;
			mdt_cdt_put_request(car);
		}

		/* test if request too long, if yes cancel it
		 * the same way the copy tool acknowledge a cancel request */
		if ((last + cdt->cdt_timeout) < cfs_time_current_sec()) {
			struct hsm_progress_kernel pgs;

			dump_llog_agent_req_rec("mdt_coordinator_cb(): "
						"request timeouted, start "
						"cleaning", larr);
			/* a too old cancel request just needs to be removed
			 * this can happen, if copy tool does not support cancel
			 * for other requests, we have to remove the running
			 * request and notify the copytool
			 */
			pgs.hpk_fid = larr->arr_hai.hai_fid;
			pgs.hpk_cookie = larr->arr_hai.hai_cookie;
			pgs.hpk_extent = larr->arr_hai.hai_extent;
			pgs.hpk_flags = HP_FLAG_COMPLETED;
			pgs.hpk_errval = ENOSYS;
			pgs.hpk_data_version = 0;
			/* update request state, but do not record in llog, to
			 * avoid deadlock on cdt_llog_lock
			 */
			rc = mdt_hsm_update_request_state(dscb->mti, &pgs, 0);
			if (rc)
				CERROR("%s: Cannot cleanup timeouted request: "
				       DFID" for cookie "LPX64" action=%s\n",
				       mdt_obd_name(mdt),
				       PFID(&pgs.hpk_fid), pgs.hpk_cookie,
				       hsm_copytool_action2name(
						     larr->arr_hai.hai_action));

			/* add the cookie to the list of record to be
			 * canceled by caller */
			if (dscb->max_cookie == (dscb->cookie_cnt - 1)) {
				__u64 *ptr, *old_ptr;
				int old_sz, new_sz, new_cnt;

				/* need to increase vector size */
				old_sz = sizeof(__u64) * dscb->max_cookie;
				old_ptr = dscb->cookies;

				new_cnt = 2 * dscb->max_cookie;
				new_sz = sizeof(__u64) * new_cnt;

				OBD_ALLOC(ptr, new_sz);
				if (!ptr) {
					CERROR("%s: Cannot allocate memory "
					       "(%d o) for cookie vector\n",
					       mdt_obd_name(mdt), new_sz);
					RETURN(-ENOMEM);
				}
				memcpy(ptr, dscb->cookies, old_sz);
				dscb->cookies = ptr;
				dscb->max_cookie = new_cnt;
				OBD_FREE(old_ptr, old_sz);
			}
			dscb->cookies[dscb->cookie_cnt] =
						       larr->arr_hai.hai_cookie;
			dscb->cookie_cnt++;
		}
		break;
	}
	case ARS_FAILED:
	case ARS_CANCELED:
	case ARS_SUCCEED:
		if ((larr->arr_req_change + cdt->cdt_delay) <
		    cfs_time_current_sec())
			RETURN(LLOG_DEL_RECORD);
		break;
	}
	RETURN(0);
}

/**
 * create /proc entries for coordinator
 * \param mdt [IN]
 * \retval 0 success
 * \retval -ve failure
 */
static int hsm_cdt_procfs_init(struct mdt_device *mdt)
{
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	int			 rc = 0;
	ENTRY;

	/* init /proc entries, failure is not critical */
	cdt->cdt_proc_dir = lprocfs_register("hsm",
					     mdt2obd_dev(mdt)->obd_proc_entry,
					     lprocfs_mdt_hsm_vars, mdt);
	if (IS_ERR(cdt->cdt_proc_dir)) {
		rc = PTR_ERR(cdt->cdt_proc_dir);
		CERROR("%s: Cannot create hsm entry in mdt proc dir, rc=%d\n",
		       mdt_obd_name(mdt), rc);
		cdt->cdt_proc_dir = NULL;
		RETURN(rc);
	}

	RETURN(0);
}

/**
 * coordinator thread
 * \param data [IN] obd device
 * \retval 0 success
 * \retval -ve failure
 */
static int mdt_coordinator(void *data)
{
	struct mdt_thread_info	*mti = data;
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	char			 name[] = "hsm_cdtr";
	struct data_scan_cb	 dscb;
	int			 rc = 0;
	ENTRY;

	cfs_daemonize(name);

	cdt->cdt_thread->t_flags = SVC_RUNNING;
	cfs_waitq_signal(&cdt->cdt_thread->t_ctl_waitq);

	CDEBUG(D_HSM, "%s: coordinator thread starting, pid=%d\n",
	       name, cfs_curproc_pid());

	/*
	 * create /proc entries for coordinator
	 */
	hsm_cdt_procfs_init(mdt);
	/* timeouted cookie vector initialization */
	dscb.max_cookie = 0;
	dscb.cookie_cnt = 0;
	dscb.cookies = NULL;
	/* we use a copy of cdt_max_request in the cb, so if cdt_max_request
	 * increases due to a change from /proc we do not overflow the
	 * dscb.request[] vector
	 */
	dscb.max_request = cdt->cdt_max_request;
	dscb.request_sz = dscb.max_request * sizeof(*dscb.request);
	OBD_ALLOC(dscb.request, dscb.request_sz);
	if (!dscb.request)
		GOTO(out, rc = -ENOMEM);

	dscb.mti = mti;
	obd_uuid2fsname(dscb.fs_name, mdt_obd_name(mdt), MTI_NAME_MAXLEN);

	while (1) {
		struct l_wait_info lwi;
		int i;

		lwi = LWI_TIMEOUT(cfs_time_seconds(cdt->cdt_loop_period),
				  NULL, NULL);
		l_wait_event(cdt->cdt_thread->t_ctl_waitq,
			     (cdt->cdt_thread->t_flags &
			      (SVC_STOPPING|SVC_EVENT)),
			     &lwi);

		CDEBUG(D_HSM, "coordinator resumes\n");

		if ((cdt->cdt_thread->t_flags & SVC_STOPPING) ||
		    (cdt->cdt_state == CDT_STOPPING)) {
			cdt->cdt_thread->t_flags &= ~SVC_STOPPING;
			rc = 0;
			break;
		}

		/* wake up before timeout, new work arrives */
		if (cdt->cdt_thread->t_flags & SVC_EVENT)
			cdt->cdt_thread->t_flags &= ~SVC_EVENT;

		/* if coordinator is suspended continue to wait */
		if (cdt->cdt_state == CDT_DISABLE) {
			CDEBUG(D_HSM, "disable state, coordinator sleeps\n");
			continue;
		}

		CDEBUG(D_HSM, "coordinator starts reading llog\n");

		if (dscb.max_request != cdt->cdt_max_request) {
			/* cdt_max_request has changed,
			 * we need to allocate a new buffer
			 */
			OBD_FREE(dscb.request, dscb.request_sz);
			dscb.max_request = cdt->cdt_max_request;
			dscb.request_sz =
				   dscb.max_request * sizeof(*dscb.request);
			OBD_ALLOC(dscb.request, dscb.request_sz);
			if (!dscb.request) {
				rc = -ENOMEM;
				break;
			}
		}

		/* create canceled cookie vector for an arbitrary size
		 * if needed, vector will grow during llog scan
		 */
		dscb.max_cookie = 10;
		dscb.cookie_cnt = 0;
		OBD_ALLOC(dscb.cookies, dscb.max_cookie * sizeof(__u64));
		if (!dscb.cookies) {
			rc = -ENOMEM;
			goto clean_cb_alloc;
		}
		dscb.request_cnt = 0;

		rc = cdt_llog_process(mti->mti_env, mdt,
				      mdt_coordinator_cb, &dscb);
		if (rc < 0)
			goto clean_cb_alloc;

		CDEBUG(D_HSM, "Found %d requests to send and %d"
			      " requests to cancel\n",
		       dscb.request_cnt, dscb.cookie_cnt);
		/* first we cancel llog records of the timeouted requests */
		if (dscb.cookie_cnt > 0) {
			rc = mdt_agent_record_update(mti->mti_env, mdt,
						     dscb.cookies,
						     dscb.cookie_cnt,
						     ARS_CANCELED);
			if (rc)
				CERROR("%s: mdt_agent_record_update() failed, "
				       "rc=%d, cannot update status to %s "
				       "for %d cookies\n",
				       mdt_obd_name(mdt), rc,
				       agent_req_status2name(ARS_CANCELED),
				       dscb.cookie_cnt);
		}

		if (cfs_list_empty(&cdt->cdt_agents)) {
			CDEBUG(D_HSM, "no agent available, "
				      "coordinator sleeps\n");
			goto clean_cb_alloc;
		}

		/* here dscb contains a list of requests to be started */
		for (i = 0 ; i < dscb.max_request ; i++) {
			struct hsm_action_list	*hal;
			struct hsm_action_item	*hai;
			__u64			*cookies;
			int			 sz, j;
			enum agent_req_status	 status;

			/* still room for work ? */
			down(&cdt->cdt_counter_lock);
			if (cdt->cdt_request_count == cdt->cdt_max_request) {
				up(&cdt->cdt_counter_lock);
				break;
			}
			up(&cdt->cdt_counter_lock);

			if (dscb.request[i].hal == NULL)
				continue;

			/* found a request, we start it */
			/* kuc payload allocation so we avoid an additionnal
			 * allocation in mdt_hsm_agent_send()
			 */
			hal = kuc_alloc(dscb.request[i].hal_used_sz,
					KUC_TRANSPORT_HSM, HMT_ACTION_LIST);
			if (IS_ERR(hal)) {
				CERROR("%s: Cannot allocate memory (%d o) "
				       "for compound "LPX64"\n",
				       mdt_obd_name(mdt),
				       dscb.request[i].hal_used_sz,
				       dscb.request[i].hal->hal_compound_id);
				continue;
			}
			memcpy(hal, dscb.request[i].hal,
			       dscb.request[i].hal_used_sz);

			rc = mdt_hsm_agent_send(mti, hal, 0);
			/* if failure, we suppose it is temporary
			 * if the copy tool failed to do the request
			 * it has to use hsm_progress
			 */
			status = (rc ? ARS_WAITING : ARS_STARTED);

			/* set up cookie vector to set records status
			 * after copy tools start or failed
			 */
			sz = dscb.request[i].hal->hal_count * sizeof(__u64);
			OBD_ALLOC(cookies, sz);
			if (!cookies) {
				CERROR("%s: Cannot allocate memory (%d o) "
				       "for cookies vector "LPX64"\n",
				       mdt_obd_name(mdt), sz,
				       dscb.request[i].hal->hal_compound_id);
				kuc_free(hal, dscb.request[i].hal_used_sz);
				continue;
			}
			hai = hai_zero(hal);
			for (j = 0 ; j < dscb.request[i].hal->hal_count ; j++) {
				cookies[j] = hai->hai_cookie;
				hai = hai_next(hai);
			}

			rc = mdt_agent_record_update(mti->mti_env, mdt, cookies,
						dscb.request[i].hal->hal_count,
						status);
			if (rc)
				CERROR("%s: mdt_agent_record_update() failed, "
				       "rc=%d, cannot update status to %s "
				       "for %d cookies\n",
				       mdt_obd_name(mdt), rc,
				       agent_req_status2name(status),
				       dscb.request[i].hal->hal_count);

			OBD_FREE(cookies, sz);
			kuc_free(hal, dscb.request[i].hal_used_sz);
		}
clean_cb_alloc:
		/* free cookie vector allocated for/by callback */
		if (dscb.cookies) {
			OBD_FREE(dscb.cookies, dscb.max_cookie * sizeof(__u64));
			dscb.max_cookie = 0;
			dscb.cookie_cnt = 0;
			dscb.cookies = NULL;
		}

		/* free hal allocated by callback */
		for (i = 0 ; i < dscb.max_request ; i++) {
			if (dscb.request[i].hal) {
				OBD_FREE(dscb.request[i].hal,
					 dscb.request[i].hal_sz);
				dscb.request[i].hal_sz = 0;
				dscb.request[i].hal = NULL;
				dscb.request_cnt--;
			}
		}
		LASSERT(dscb.request_cnt == 0);

		/* reset callback data */
		memset(dscb.request, 0, dscb.request_sz);
	}
	rc = 0;
	EXIT;
out:
	if (dscb.request)
		OBD_FREE(dscb.request, dscb.request_sz);

	if (dscb.cookies)
		OBD_FREE(dscb.cookies, dscb.max_cookie * sizeof(__u64));

	if (cdt->cdt_state == CDT_STOPPING) {
		/* request comes from /proc path, so we need to clean cdt
		 * struct */
		 mdt_hsm_cdt_stop(mdt);
		 mdt->mdt_opts.mo_coordinator = 0;
	} else {
		/* request comes from a thread event, generated
		 * by mdt_stop_coordinator(), we have to ack
		 * and cdt cleaning will be done by event sender
		 */
		cdt->cdt_thread->t_flags = SVC_STOPPED;
		cfs_waitq_signal(&cdt->cdt_thread->t_ctl_waitq);
	}

	CDEBUG(D_HSM, "%s: coordinator thread exiting, process=%d, rc=%d\n",
	       name, cfs_curproc_pid(), rc);

	return rc;
}

/**
 * data passed to llog_cat_process() callback
 * to scan requests and take actions
 */
struct data_restore_cb {
	struct mdt_thread_info	*drcb_mti;
};

/**
 *  llog_cat_process() callback, used to:
 *  - find restore request and allocate the restore handle
 * \param env [IN] environment
 * \param llh [IN] llog handle
 * \param hdr [IN] llog record
 * \param data [IN/OUT] cb data = struct data_restore_cb
 * \retval 0 success
 * \retval -ve failure
 */
static int hsm_restore_cb(const struct lu_env *env,
			  struct llog_handle *llh,
			  struct llog_rec_hdr *hdr, void *data)
{
	struct llog_agent_req_rec	*larr;
	struct data_restore_cb		*drcb;
	struct cdt_restore_handle	*crh;
	struct hsm_action_item		*hai;
	struct mdt_thread_info		*mti;
	struct coordinator		*cdt;
	struct mdt_object		*child;
	int rc;
	ENTRY;

	drcb = data;
	mti = drcb->drcb_mti;
	cdt = &mti->mti_mdt->mdt_coordinator;

	larr = (struct llog_agent_req_rec *)hdr;
	hai = &larr->arr_hai;
	if ((hai->hai_action != HSMA_RESTORE) ||
	     agent_req_in_final_state(larr->arr_status))
		RETURN(0);

	/* restore request not in a final state */

	OBD_ALLOC_PTR(crh);
	if (crh == NULL)
		RETURN(-ENOMEM);

	crh->crh_fid = hai->hai_fid;
	/* in V1 all file is restored
	crh->extent.start = hai->hai_extent.offset;
	crh->extent.end = hai->hai_extent.offset + hai->hai_extent.length;
	*/
	crh->crh_extent.start = 0;
	crh->crh_extent.end = OBD_OBJECT_EOF;
	/* get the layout lock */
	mdt_lock_reg_init(&crh->crh_lh, LCK_EX);
	child = mdt_object_find_lock(mti, &crh->crh_fid, &crh->crh_lh,
				     MDS_INODELOCK_LAYOUT);
	if (IS_ERR(child))
		GOTO(out, rc = PTR_ERR(child));

	rc = 0;
	/* we choose to not keep a keep a reference
	 * on the object during the restore time which can be very long */
	mdt_object_put(mti->mti_env, child);

	down(&cdt->cdt_restore_lock);
	cfs_list_add_tail(&crh->crh_list, &cdt->cdt_restore_hdl);
	up(&cdt->cdt_restore_lock);

out:
	RETURN(rc);
}

/**
 * restore coordinator state at startup
 * the goal is to take a layout lock for each registered restore resquest
 * \param mti [IN] context
 */
static int mdt_hsm_pending_restore(struct mdt_thread_info *mti)
{
	struct data_restore_cb	 drcb;
	int			 rc;
	ENTRY;

	drcb.drcb_mti = mti;

	rc = cdt_llog_process(mti->mti_env, mti->mti_mdt,
			      hsm_restore_cb, &drcb);

	RETURN(rc);
}

static int hsm_init_ucred(struct lu_ucred *uc)
{
	ENTRY;

	uc->uc_valid = UCRED_OLD;
	uc->uc_o_uid = 0;
	uc->uc_o_gid = 0;
	uc->uc_o_fsuid = 0;
	uc->uc_o_fsgid = 0;
	uc->uc_uid = 0;
	uc->uc_gid = 0;
	uc->uc_fsuid = 0;
	uc->uc_fsgid = 0;
	uc->uc_suppgids[0] = -1;
	uc->uc_suppgids[1] = -1;
	uc->uc_cap = 0;
	uc->uc_umask = 0777;
	uc->uc_ginfo = NULL;
	uc->uc_identity = NULL;

	RETURN(0);
}

/**
 * wake up coordinator thread
 * \param mdt [IN] device
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_cdt_wakeup(struct mdt_device *mdt)
{
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	ENTRY;

	if (cdt->cdt_state == CDT_STOPPED)
		RETURN(-ESRCH);

	/* wake up coordinator */
	cdt->cdt_thread->t_flags = SVC_EVENT;
	cfs_waitq_signal(&cdt->cdt_thread->t_ctl_waitq);

	RETURN(0);
}

/**
 * initialize coordinator struct
 * \param mdt [IN] device
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_cdt_init(struct mdt_device *mdt)
{
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct mdt_thread_info	*cdt_mti = NULL;
	int			 rc;
	struct lu_ucred		*uc;
	ENTRY;

	cdt->cdt_state = CDT_STOPPED;

	rc = lu_env_init(&cdt->cdt_env, LCT_MD_THREAD|LCT_DT_THREAD);
	if (rc)
		RETURN(rc);

	OBD_ALLOC_PTR(cdt->cdt_env.le_ses);
	if (!cdt->cdt_env.le_ses)
		GOTO(free, rc = -ENOMEM);

	/* not sure of list of flags: LCT_SESSION solves ucred alloc */
	lu_context_init(cdt->cdt_env.le_ses,
			LCT_MD_THREAD|LCT_DT_THREAD|LCT_REMEMBER|
			LCT_SESSION);
	cdt_mti = lu_context_key_get(&cdt->cdt_env.le_ctx, &mdt_thread_key);
	LASSERT(cdt_mti != NULL);

	cdt_mti->mti_env = &cdt->cdt_env;
	cdt_mti->mti_mdt = mdt;
	OBD_ALLOC_PTR(cdt_mti->mti_exp);
	if (!cdt_mti->mti_exp)
		GOTO(free, rc = -ENOMEM);

	/* used when cdt thread needs to read a lov ea from disk
	 * and when we are not in a file access context from a client
	 * we use the biggest lov ea size
	 */
	cdt_mti->mti_attr.ma_lmm_size = CDT_LOV_EA_SZ;

	OBD_ALLOC(cdt_mti->mti_attr.ma_lmm, cdt_mti->mti_attr.ma_lmm_size);
	if (!cdt_mti->mti_attr.ma_lmm)
		GOTO(free, rc = -ENOMEM);

	uc = mdt_ucred(cdt_mti);
	hsm_init_ucred(uc);

	OBD_ALLOC_PTR(cdt->cdt_thread);
	if (!cdt->cdt_thread)
		GOTO(free, rc = -ENOMEM);
	cfs_waitq_init(&cdt->cdt_thread->t_ctl_waitq);

	RETURN(0);

free:
	if (cdt->cdt_env.le_ses)
		OBD_FREE_PTR(cdt->cdt_env.le_ses);
	if (cdt_mti && cdt_mti->mti_exp)
		OBD_FREE_PTR(cdt_mti->mti_exp);
	if (cdt_mti && cdt_mti->mti_attr.ma_lmm)
		OBD_FREE(cdt_mti->mti_attr.ma_lmm, CDT_LOV_EA_SZ);
	if (cdt->cdt_thread)
		OBD_FREE_PTR(cdt->cdt_thread);

	RETURN(rc);
}

/**
 * start a coordinator thread
 * \param mdt [IN] device
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_cdt_start(struct mdt_device *mdt)
{
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	int			 rc;
	void			*ptr;
	struct mdt_thread_info	*cdt_mti;
	ENTRY;

	/* functions defined but not yet used
	 * this avoid compilation warning
	 */
	ptr = dump_requests;

	if (cdt->cdt_state != CDT_STOPPED) {
		CERROR("%s: Coordinator already started\n",
		       mdt_obd_name(mdt));
		RETURN(-EALREADY);
	}

	cdt->cdt_policy = CDT_DEFAULT_POLICY;
	cdt->cdt_state = CDT_INIT;

	cfs_atomic_set(&cdt->cdt_compound_id, cfs_time_current_sec());
	/* just need to be larger than previous one */
	/* cdt_last_cookie is protected by cdt_llog_lock */
	cdt->cdt_last_cookie = cfs_time_current_sec();
	sema_init(&cdt->cdt_counter_lock, 1);
	sema_init(&cdt->cdt_llog_lock, 1);
	init_rwsem(&cdt->cdt_agent_lock);
	init_rwsem(&cdt->cdt_request_lock);
	sema_init(&cdt->cdt_restore_lock, 1);
	cdt->cdt_loop_period = 10;
	cdt->cdt_delay = 60;
	cdt->cdt_timeout = 3600;
	cdt->cdt_max_request = 3;
	cdt->cdt_request_count = 0;
	CFS_INIT_LIST_HEAD(&cdt->cdt_requests);
	CFS_INIT_LIST_HEAD(&cdt->cdt_agents);
	CFS_INIT_LIST_HEAD(&cdt->cdt_restore_hdl);

	/* to avoid deadlock when start is made through /proc
	 * /proc entries are created by the coordinator thread */

	/* set up list of started restore requests */
	cdt_mti = lu_context_key_get(&cdt->cdt_env.le_ctx, &mdt_thread_key);
	mdt_hsm_pending_restore(cdt_mti);

	rc = cfs_create_thread(mdt_coordinator, cdt_mti,
			       CLONE_VM | CLONE_FILES);
	if (rc < 0) {
		cdt->cdt_state = CDT_STOPPED;
		CERROR("%s: error starting ll_hsm_coordinator-mdt: %d\n",
		       mdt_obd_name(mdt), rc);
		RETURN(rc);
	} else {
		CDEBUG(D_HSM, "ll_hsm_coordinator-mdt started, thread=%d\n",
		       rc);
		rc = 0;
	}

	cfs_wait_event(cdt->cdt_thread->t_ctl_waitq,
		       (cdt->cdt_thread->t_flags & SVC_RUNNING));

	cdt->cdt_state = CDT_RUNNING;
	mdt->mdt_opts.mo_coordinator = 1;
	RETURN(0);
}

/**
 * stop a coordinator thread
 * \param mdt [IN] device
 */
int  mdt_hsm_cdt_stop(struct mdt_device *mdt)
{
	struct coordinator		*cdt = &mdt->mdt_coordinator;
	cfs_list_t			*pos, *tmp;
	struct cdt_agent_req		*car;
	struct hsm_agent		*ha;
	struct cdt_restore_handle	*crh;
	struct mdt_thread_info		*cdt_mti;
	ENTRY;

	if (cdt->cdt_state == CDT_STOPPED) {
		CERROR("%s: Coordinator already stopped\n",
		       mdt_obd_name(mdt));
		RETURN(-EALREADY);
	}

	/* remove proc entries */
	if (cdt->cdt_proc_dir != NULL)
		lprocfs_remove(&cdt->cdt_proc_dir);

	if (cdt->cdt_state != CDT_STOPPING) {
		/* stop coordinator thread before cleaning */
		cdt->cdt_thread->t_flags = SVC_STOPPING;
		cfs_waitq_signal(&cdt->cdt_thread->t_ctl_waitq);
		cfs_wait_event(cdt->cdt_thread->t_ctl_waitq,
			       cdt->cdt_thread->t_flags & SVC_STOPPED);
	}
	cdt->cdt_state = CDT_STOPPED;

	/* start cleaning */
	down_write(&cdt->cdt_request_lock);
	cfs_list_for_each_safe(pos, tmp, &cdt->cdt_requests) {
		car = cfs_list_entry(pos, struct cdt_agent_req,
				     car_request_list);
		cfs_list_del(&car->car_request_list);
		mdt_cdt_free_request(car);
	}
	up_write(&cdt->cdt_request_lock);

	down_write(&cdt->cdt_agent_lock);
	cfs_list_for_each_safe(pos, tmp, &cdt->cdt_agents) {
		ha = cfs_list_entry(pos, struct hsm_agent, ha_list);
		cfs_list_del(&ha->ha_list);
		OBD_FREE_PTR(ha);
	}
	up_write(&cdt->cdt_agent_lock);

	cdt_mti = lu_context_key_get(&cdt->cdt_env.le_ctx, &mdt_thread_key);
	down(&cdt->cdt_restore_lock);
	cfs_list_for_each_safe(pos, tmp, &cdt->cdt_restore_hdl) {
		struct mdt_object	*child;

		crh = cfs_list_entry(pos, struct cdt_restore_handle,
				     crh_list);

		/* give back layout lock */
		child = mdt_object_find(&cdt->cdt_env, mdt, &crh->crh_fid);
		if (!IS_ERR(child))
			mdt_object_unlock_put(cdt_mti, child, &crh->crh_lh, 1);

		cfs_list_del(&crh->crh_list);

		OBD_FREE_PTR(crh);
	}
	up(&cdt->cdt_restore_lock);

	mdt->mdt_opts.mo_coordinator = 0;

	RETURN(0);
}

/**
 * free a coordinator thread
 * \param mdt [IN] device
 */
int  mdt_hsm_cdt_fini(struct mdt_device *mdt)
{
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct mdt_thread_info	*cdt_mti;
	ENTRY;

	OBD_FREE_PTR(cdt->cdt_thread);

	cdt_mti = lu_context_key_get(&cdt->cdt_env.le_ctx, &mdt_thread_key);
	OBD_FREE(cdt_mti->mti_attr.ma_lmm, CDT_LOV_EA_SZ);
	OBD_FREE_PTR(cdt_mti->mti_exp);

	lu_context_fini(cdt->cdt_env.le_ses);
	OBD_FREE_PTR(cdt->cdt_env.le_ses);

	lu_env_fini(&cdt->cdt_env);

	RETURN(0);
}

/**
 * register all requests from an hal in the memory list
 * \param mti [IN] context
 * \param hal [IN] request
 * \param uuid [OUT] in case of CANCEL, the uuid of the agent
 *  which is running the CT
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_add_hal(struct mdt_thread_info *mti,
		    struct hsm_action_list *hal, struct obd_uuid *uuid)
{
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct hsm_action_item	*hai;
	int			 rc = 0, i;
	ENTRY;

	/* register request in memory list */
	hai = hai_zero(hal);
	for (i = 0 ; i < hal->hal_count ; i++, hai = hai_next(hai)) {
		struct cdt_agent_req *car;

		/* in case of a cancel request, we first mark the ondisk
		 * record of the request we want to stop as canceled
		 * this does not change the cancel record
		 * it will be done when updating the request status
		 */
		if (hai->hai_action == HSMA_CANCEL) {
			rc = mdt_agent_record_update(mti->mti_env, mti->mti_mdt,
						     &hai->hai_cookie,
						     1, ARS_CANCELED);
			if (rc) {
				CERROR("%s: mdt_agent_record_update() failed, "
				       "rc=%d, cannot update status to %s "
				       "for cookie "LPX64"\n",
				       mdt_obd_name(mdt), rc,
				       agent_req_status2name(ARS_CANCELED),
				       hai->hai_cookie);
				GOTO(out, rc);
			}

			/* find the running request to set it canceled */
			car = mdt_cdt_find_request(cdt, hai->hai_cookie, NULL);
			if (!IS_ERR(car)) {
				car->car_canceled = 1;
				/* uuid has to be changed to the one running the
				* request to cancel */
				*uuid = car->car_uuid;
				mdt_cdt_put_request(car);
			}
			/* no need to memorize cancel request
			 * this also avoid a deadlock when we receive
			 * a purge all requests command
			 */
			continue;
		}

		if (hai->hai_action == HSMA_ARCHIVE) {
			struct mdt_lock_handle *lh;
			struct mdt_object *obj;
			struct md_hsm hsm;

			lh = &mti->mti_lh[MDT_LH_HSM];
			mdt_lock_handle_init(lh);
			mdt_lock_reg_init(lh, LCK_PW);

			obj = mdt_hsm_get_md_hsm(mti, &hai->hai_fid, &hsm, lh);
			if (IS_ERR(obj) && (PTR_ERR(obj) == -ENOENT))
				continue;
			if (IS_ERR(obj))
				GOTO(out, rc = PTR_ERR(obj));

			hsm.mh_flags |= HS_EXISTS;
			hsm.mh_arch_id = hal->hal_archive_id;
			rc = mdt_hsm_set_md_hsm(mti, obj, &hsm, lh);
			if (rc)
				GOTO(out, rc);
		}

		car = mdt_cdt_alloc_request(hal->hal_compound_id,
					    hal->hal_archive_id, hal->hal_flags,
					    uuid, hai);
		if (IS_ERR(car))
			GOTO(out, rc = PTR_ERR(car));

		rc = mdt_cdt_add_request(cdt, car);
	}
out:
	RETURN(rc);
}

/**
 * swap layouts between 2 fids
 * \param mti [IN] context
 * \param fid1 [IN]
 * \param fid2 [IN]
 */
static int hsm_swap_layouts(struct mdt_thread_info *mti,
			    const lustre_fid *fid, const lustre_fid *dfid)
{
	struct mdt_device	*mdt = mti->mti_mdt;
	struct mdt_object	*child1, *child2;
	struct mdt_lock_handle	*lh2;
	int			 rc;
	ENTRY;

	child1 = mdt_object_find(mti->mti_env, mdt, fid);
	if (IS_ERR(child1))
		GOTO(out, rc = PTR_ERR(child1));

	/* we already have layout lock on fid so take only
	 * on dfid */
	lh2 = &mti->mti_lh[MDT_LH_OLD];
	mdt_lock_reg_init(lh2, LCK_EX);
	child2 = mdt_object_find_lock(mti, dfid, lh2, MDS_INODELOCK_LAYOUT);
	if (IS_ERR(child2))
		GOTO(out_child1, rc = PTR_ERR(child2));

	/* if copy tool closes the volatile before sending the final
	 * progress through llapi_hsm_copy_end(), all the objects
	 * are removed and mdd_swap_layout LBUG */
	if (mdt_object_exists(child2)) {
		rc = mo_swap_layouts(mti->mti_env, mdt_object_child(child1),
				     mdt_object_child(child2), 0);
	} else {
		CERROR("%s: Copytool has closed volatile file "DFID"\n",
		       mdt_obd_name(mti->mti_mdt), PFID(dfid));
		rc = -ENOENT;
	}

	mdt_object_unlock_put(mti, child2, lh2, 1);
out_child1:
	mdt_object_put(mti->mti_env, child1);
out:
	RETURN(rc);
}

/**
 * update status of a request
 * \param mti [IN] context
 * \param pgs [IN] progress of the copy tool
 * \param update_record [IN] update llog record
 * \retval 0 success
 * \retval -ve failure
 */
int mdt_hsm_update_request_state(struct mdt_thread_info *mti,
				 struct hsm_progress_kernel *pgs,
				 const int update_record)
{
	const struct lu_env	*env = mti->mti_env;
	struct mdt_device	*mdt = mti->mti_mdt;
	struct coordinator	*cdt = &mdt->mdt_coordinator;
	struct cdt_agent_req	*car;
	enum agent_req_status	 status = ARS_FAILED;
	struct mdt_lock_handle	*lh;
	struct mdt_object	*obj = NULL;
	int			 cl_flags = 0, rc = 0;
	struct md_attr		*ma = NULL;
	int			 lmm_size_alloc;
	ENTRY;

	/* no coordinator started, so we cannot serve requests */
	if (cdt->cdt_state == CDT_STOPPED)
		RETURN(-EAGAIN);

	/* first do sanity checks */
	car = mdt_cdt_update_request(cdt, pgs);
	if (IS_ERR(car)) {
		CERROR("%s: Cannot find running request for cookie "LPX64
		       " on fid="DFID"\n",
		       mdt_obd_name(mdt),
		       pgs->hpk_cookie, PFID(&pgs->hpk_fid));
		RETURN(PTR_ERR(car));
	}

	CDEBUG(D_HSM, "Progress received for fid="DFID" cookie="LPX64
		      " action=%s flags=%d err=%d fid="DFID" dfid="DFID"\n",
		      PFID(&pgs->hpk_fid), pgs->hpk_cookie,
		      hsm_copytool_action2name(car->car_hai->hai_action),
		      pgs->hpk_flags, pgs->hpk_errval,
		      PFID(&car->car_hai->hai_fid),
		      PFID(&car->car_hai->hai_dfid));

	/* progress is done on fid or data fid depending of the action and
	 * of the copy progress */
	/* for restore progress is used to send back the data fid to cdt */
	if ((car->car_hai->hai_action == HSMA_RESTORE) &&
	    (lu_fid_eq(&car->car_hai->hai_fid, &car->car_hai->hai_dfid)))
		memcpy(&car->car_hai->hai_dfid, &pgs->hpk_fid,
		       sizeof(pgs->hpk_fid));

	if (((car->car_hai->hai_action == HSMA_RESTORE) ||
	     (car->car_hai->hai_action == HSMA_ARCHIVE)) &&
	    (!lu_fid_eq(&pgs->hpk_fid, &car->car_hai->hai_dfid) &&
	     !lu_fid_eq(&pgs->hpk_fid, &car->car_hai->hai_fid))) {
		CERROR("%s: Progress fid "DFID" for cookie "LPX64
		       " does not match request fid "DFID" nor dfid "DFID"\n",
		       mdt_obd_name(mdt),
		       PFID(&pgs->hpk_fid), pgs->hpk_cookie,
		       PFID(&car->car_hai->hai_fid),
		       PFID(&car->car_hai->hai_dfid));
		RETURN(-EINVAL);
	}

	if (pgs->hpk_errval != 0 && !(pgs->hpk_flags & HP_FLAG_COMPLETED)) {
		CERROR("%s: Progress on fid "DFID" for cookie "LPX64" action=%s"
		       " is not coherent (err=%d and not completed"
		       " (flags=%d))\n",
		       mdt_obd_name(mdt),
		       PFID(&pgs->hpk_fid), pgs->hpk_cookie,
		       hsm_copytool_action2name(car->car_hai->hai_action),
		       pgs->hpk_errval, pgs->hpk_flags);
		RETURN(-EINVAL);
	}

	/* now progress is valid */

	/* we use a root like ucred */
	hsm_init_ucred(mdt_ucred(mti));

	if (pgs->hpk_flags & HP_FLAG_COMPLETED) {
		OBD_ALLOC_PTR(ma);
		if (!ma) {
			rc = -ENOMEM;
			goto free_ma;
		}
		lmm_size_alloc = CDT_LOV_EA_SZ;
		OBD_ALLOC(ma->ma_lmm, lmm_size_alloc);
		if (!ma->ma_lmm) {
			rc = -ENOMEM;
			goto free_ma;
		}

		ma->ma_lmm_size = lmm_size_alloc;
		/* MA_HSM for hsm attibutes */
		ma->ma_need = MA_HSM | MA_LOV;
		ma->ma_valid = 0;

		/* find object by fid and lock it to get/set HSM flags */
		/* we are in an RPC context so we can use the shared lh */
		lh = &mti->mti_lh[MDT_LH_CHILD];
		mdt_lock_handle_init(lh);
		mdt_lock_reg_init(lh, LCK_PW);

		obj = hsm_get_md_attr(mti, &car->car_hai->hai_fid, ma, lh);
		if (IS_ERR(obj)) {
			/* object removed */
			status = ARS_SUCCEED;
			ma->ma_valid = 0;
			goto unlock;
		}

		/* we need to keep MA_LOV for lock ? and restore */
		ma->ma_valid &= ~MA_HSM;
		/* no need to change ma->ma_archive_number
		 * mdt_cdt_get_md_attr()::mo_attr_get() got it from disk and
		 * it is still valid
		 */
		if (pgs->hpk_errval != 0) {
			switch (pgs->hpk_errval) {
			case ENOSYS:
				/* the copy tool does not support cancel
				 * so the cancel request is failed
				 * As we cannot distinguish a cancel progress
				 * from another action progress (they have the
				 * same cookie), we suppose here the CT returns
				 * ENOSYS only if does not support cancel
				 */
				/* this can also happen when cdt calls it to
				 * for a timeouted request */
				status = ARS_FAILED;
				/* to have a cancel event in changelog */
				pgs->hpk_errval = ECANCELED;
				break;
			case ECANCELED:
				/* the request record has already been set to
				 * ARS_CANCELED, this set the cancel request
				 * to ARS_SUCCEED */
				status = ARS_SUCCEED;
				break;
			default:
				status = (((cdt->cdt_policy &
					   CDT_NORETRY_ACTION) ||
					   !(pgs->hpk_flags & HP_FLAG_RETRY)) ?
					   ARS_FAILED : ARS_WAITING);
				break;
			}

			if (pgs->hpk_errval > CLF_HSM_MAXERROR) {
				CERROR("%s: Request "LPX64" on fid "DFID
				       " failed, error code %d too large\n",
				       mdt_obd_name(mdt),
				       pgs->hpk_cookie, PFID(&pgs->hpk_fid),
				       pgs->hpk_errval);
				hsm_set_cl_error(&cl_flags,
						 CLF_HSM_ERROVERFLOW);
				rc = -EINVAL;
			} else {
				hsm_set_cl_error(&cl_flags, pgs->hpk_errval);
			}

			switch (car->car_hai->hai_action) {
			case HSMA_ARCHIVE:
				hsm_set_cl_event(&cl_flags, HE_ARCHIVE);
				break;
			case HSMA_RESTORE:
				hsm_set_cl_event(&cl_flags, HE_RESTORE);
				break;
			case HSMA_REMOVE:
				hsm_set_cl_event(&cl_flags, HE_REMOVE);
				break;
			case HSMA_CANCEL:
				hsm_set_cl_event(&cl_flags, HE_CANCEL);
				CERROR("%s: Failed request "LPX64" on fid "DFID
				       " cannot be a CANCEL\n",
				       mdt_obd_name(mdt),
				       pgs->hpk_cookie,
				       PFID(&pgs->hpk_fid));
				break;
			default:
				CERROR("%s: Failed request "LPX64" on fid "DFID
				       " %d is an unknown action\n",
				       mdt_obd_name(mdt),
				       pgs->hpk_cookie, PFID(&pgs->hpk_fid),
				       car->car_hai->hai_action);
				rc = -EINVAL;
				break;
			}
		} else {
			status = ARS_SUCCEED;
			switch (car->car_hai->hai_action) {
			case HSMA_ARCHIVE:
				hsm_set_cl_event(&cl_flags, HE_ARCHIVE);
				/* set ARCHIVE keep EXIST and clear LOST and
				 * DIRTY */
				ma->ma_hsm.mh_arch_ver =
						  pgs->hpk_data_version;
				ma->ma_hsm.mh_flags |= HS_ARCHIVED;
				ma->ma_hsm.mh_flags &= ~(HS_LOST|HS_DIRTY);
				ma->ma_valid |= MA_HSM;
				break;
			case HSMA_RESTORE:
				hsm_set_cl_event(&cl_flags, HE_RESTORE);

				/* clear RELEASED and DIRTY */
				ma->ma_hsm.mh_flags &= ~(HS_RELEASED |
							 HS_DIRTY);
				ma->ma_valid |= MA_HSM;

				/* Restoring has changed the file version on
				 * disk. */
				ma->ma_hsm.mh_arch_ver =
							pgs->hpk_data_version;

				break;
			case HSMA_REMOVE:
				hsm_set_cl_event(&cl_flags, HE_REMOVE);
				/* clear ARCHIVED EXISTS and LOST */
				ma->ma_hsm.mh_flags &= ~(HS_ARCHIVED |
							 HS_EXISTS | HS_LOST);
				ma->ma_valid |= MA_HSM;
				break;
			case HSMA_CANCEL:
				hsm_set_cl_event(&cl_flags, HE_CANCEL);
				CERROR("%s: Successful request "LPX64
				       " on fid "DFID
				       " cannot be a CANCEL\n",
				       mdt_obd_name(mdt),
				       pgs->hpk_cookie,
				       PFID(&pgs->hpk_fid));
				break;
			default:
				CERROR("%s: Successful request "LPX64
				       " on fid "DFID
				       " %d is an unknown action\n",
				       mdt_obd_name(mdt),
				       pgs->hpk_cookie, PFID(&pgs->hpk_fid),
				       car->car_hai->hai_action);
				rc = -EINVAL;
				break;
			}
		}

		/* rc != 0 means error when analysing action, it may come from
		 * a crasy CT no need to manage DIRTY
		 */
		if (rc == 0)
			hsm_set_cl_flags(&cl_flags,
					 ((ma->ma_hsm.mh_flags & HS_DIRTY) ?
					  CLF_HSM_DIRTY : 0));

		/* unlock is done later, after layout lock management */
		if (ma->ma_valid & MA_HSM)
			rc = mdt_hsm_set_md_hsm(mti, obj, &ma->ma_hsm, NULL);

unlock:
		/* we give back layout lock only if restore was successful or
		 * if restore was canceled or if policy is to not retry
		 * in other cases we just unlock the object */
		if ((car->car_hai->hai_action == HSMA_RESTORE) &&
		    ((pgs->hpk_errval == 0) || (pgs->hpk_errval == ECANCELED) ||
		     (cdt->cdt_policy & CDT_NORETRY_ACTION))) {
			cfs_list_t			*pos, *tmp;
			struct cdt_restore_handle	*crh;

			/* restore in data fid done, we swap the layouts */
			rc = hsm_swap_layouts(mti, &car->car_hai->hai_fid,
					      &car->car_hai->hai_dfid);
			if (rc) {
				status = ARS_FAILED;
				pgs->hpk_errval = -rc;
				GOTO(free_ma, rc);
			}
			/* give back layout lock */
			down(&cdt->cdt_restore_lock);
			cfs_list_for_each_safe(pos, tmp,
					       &cdt->cdt_restore_hdl) {
				crh = cfs_list_entry(pos,
						  struct cdt_restore_handle,
						  crh_list);
				if (lu_fid_eq(&crh->crh_fid,
					      &car->car_hai->hai_fid)) {
					/* just give back layout lock, we keep
					 * the reference which is given back
					 * later with the lock for hsm flags */
					if (!IS_ERR(obj))
						mdt_object_unlock(mti, obj,
								  &crh->crh_lh,
								  1);
					cfs_list_del(&crh->crh_list);
					OBD_FREE_PTR(crh);

					if (rc) {
						up(&cdt->cdt_restore_lock);
						GOTO(free_ma, rc);
					}
					break;
				}
			}
			up(&cdt->cdt_restore_lock);
		}

		if (!IS_ERR(obj))
			mdt_object_unlock_put(mti, obj, lh, 1);
free_ma:
		if (ma && ma->ma_lmm)
			OBD_FREE(ma->ma_lmm, lmm_size_alloc);
		if (ma)
			OBD_FREE_PTR(ma);

		if ((obj != NULL) && !IS_ERR(obj))
			mo_changelog(env, CL_HSM, cl_flags,
				     mdt_object_child(obj));

		/* remove request from memory list */
		mdt_cdt_remove_request(cdt, pgs->hpk_cookie);

		CDEBUG(D_HSM, "Updating record: fid="DFID" cookie="LPX64
		       " action=%s status=%s\n", PFID(&pgs->hpk_fid),
			pgs->hpk_cookie,
			hsm_copytool_action2name(car->car_hai->hai_action),
			agent_req_status2name(status));

		if (update_record) {
			rc = mdt_agent_record_update(mti->mti_env, mdt,
						     &pgs->hpk_cookie, 1,
						     status);
			if (rc)
				CERROR("%s: mdt_agent_record_update() failed, "
				       "rc=%d, cannot update status to %s "
				       "for cookie "LPX64"\n",
				       mdt_obd_name(mdt), rc,
				       agent_req_status2name(status),
				       pgs->hpk_cookie);
		}

		/* ct has completed a request, so a slot is available, wakeup
		 * cdt to find new work */
		mdt_hsm_cdt_wakeup(mdt);
	} else {
		/* if copytool send a progress on a canceled request
		 * we inform it it should stop
		 */
		if (car->car_canceled == 1)
			rc = -ECANCELED;
	}

	/* remove ref got from mdt_cdt_update_request() */
	mdt_cdt_put_request(car);

	RETURN(rc);
}


/**
 * data passed to llog_cat_process() callback
 * to cancel requests
 */
struct data_cancel_all_cb {
	struct mdt_device	*mdt;
};

/**
 *  llog_cat_process() callback, used to:
 *  - purge all requests
 * \param env [IN] environment
 * \param llh [IN] llog handle
 * \param hdr [IN] llog record
 * \param data [IN] cb data = struct data_cancel_all_cb
 * \retval 0 success
 * \retval -ve failure
 */
static int mdt_cancel_all_cb(const struct lu_env *env,
			     struct llog_handle *llh,
			     struct llog_rec_hdr *hdr, void *data)
{
	struct llog_agent_req_rec	*larr;
	struct data_cancel_all_cb	*dcacb;
	int				 rc = 0;
	ENTRY;

	larr = (struct llog_agent_req_rec *)hdr;
	dcacb = data;
	if ((larr->arr_status == ARS_WAITING) ||
	    (larr->arr_status == ARS_STARTED)) {
		larr->arr_status = ARS_CANCELED;
		larr->arr_req_change = cfs_time_current_sec();
		rc = mdt_agent_llog_update_rec(env, dcacb->mdt, llh, larr);
		RETURN(LLOG_DEL_RECORD);
	}
	RETURN(rc);
}

/**
 * cancel all actions
 * \param obd [IN] MDT device
 */
static int hsm_cancel_all_actions(struct mdt_device *mdt)
{
	struct mdt_thread_info		*mti;
	struct coordinator		*cdt = &mdt->mdt_coordinator;
	cfs_list_t			*pos, *tmp;
	struct cdt_agent_req		*car;
	struct hsm_action_list		*hal = NULL;
	struct hsm_action_item		*hai;
	struct data_cancel_all_cb	 dcacb;
	int				 hal_sz = 0, hal_len, rc;
	enum cdt_states			 save_state;
	ENTRY;

	/* retrieve coordinator context */
	mti = lu_context_key_get(&cdt->cdt_env.le_ctx, &mdt_thread_key);

	/* disable coordinator */
	save_state = cdt->cdt_state;
	cdt->cdt_state = CDT_DISABLE;

	/* send cancel to all running requests */
	down_read(&cdt->cdt_request_lock);
	cfs_list_for_each_safe(pos, tmp, &cdt->cdt_requests) {
		car = cfs_list_entry(pos, struct cdt_agent_req,
				     car_request_list);
		mdt_cdt_get_request(car);
		/* request is not yet removed from list, it will be done
		 * when copytool will return progress
		 */

		if (car->car_hai->hai_action == HSMA_CANCEL) {
			mdt_cdt_put_request(car);
			continue;
		}

		/* needed size */
		hal_len = sizeof(*hal) + cfs_size_round(MTI_NAME_MAXLEN + 1) +
			  cfs_size_round(car->car_hai->hai_len);

		if ((hal_len > hal_sz) && (hal_sz > 0)) {
			/* not enough room, free old buffer */
			OBD_FREE(hal, hal_sz);
			hal = NULL;
		}

		/* empty buffer, allocate one */
		if (hal == NULL) {
			hal_sz = hal_len;
			OBD_ALLOC(hal, hal_sz);
			if (hal == NULL) {
				up_read(&cdt->cdt_request_lock);
				mdt_cdt_put_request(car);
				GOTO(out, rc = -ENOMEM);
			}
		}

		hal->hal_version = HAL_VERSION;
		obd_uuid2fsname(hal->hal_fsname, mdt_obd_name(mdt),
				MTI_NAME_MAXLEN);
		hal->hal_fsname[MTI_NAME_MAXLEN] = '\0';
		hal->hal_compound_id = car->car_compound_id;
		hal->hal_archive_id = car->car_archive_id;
		hal->hal_flags = car->car_flags;
		hal->hal_count = 0;

		hai = hai_zero(hal);
		memcpy(hai, car->car_hai, car->car_hai->hai_len);
		hai->hai_action = HSMA_CANCEL;
		hal->hal_count = 1;

		/* it is possible to safely call mdt_hsm_agent_send()
		 * (ie without a deadlock on cdt_request_lock), because the
		 * write lock is taken only if we are not in purge mode
		 * (mdt_hsm_agent_send() does not call mdt_cdt_add_request()
		 *   nor mdt_cdt_remove_request())
		 */
		/* no conflict with cdt thread because cdt is disable and we
		 * have the request lock */
		mdt_hsm_agent_send(mti, hal, 1);

		mdt_cdt_put_request(car);
	}
	up_read(&cdt->cdt_request_lock);

	if (hal != NULL)
		OBD_FREE(hal, hal_sz);

	/* cancel all on-disk records */
	dcacb.mdt = mdt;

	rc = cdt_llog_process(mti->mti_env, mti->mti_mdt,
			      mdt_cancel_all_cb, &dcacb);
out:
	/* enable coordinator */
	cdt->cdt_state = save_state;

	RETURN(rc);
}

/**
 * check if a request is comptaible with file status
 * \param hai [IN] request description
 * \param hal_an [IN] request archive number (not used)
 * \param rq_flags [IN] request flags
 * \param hsm [IN] file hsm metadata
 * \retval boolean
 */
int mdt_hsm_check_action_compat(const struct hsm_action_item *hai,
				const int hal_an, const __u64 rq_flags,
				const struct md_hsm *hsm)
{
	int	 rc = 0;
	int	 hsm_flags;
	ENTRY;

	hsm_flags = hsm->mh_flags;
	switch (hai->hai_action) {
	case HSMA_ARCHIVE:
		if (!(hsm_flags & HS_NOARCHIVE) &&
		    ((hsm_flags & HS_DIRTY) || !(hsm_flags & HS_ARCHIVED)))
			rc = 1;
		break;
	case HSMA_RESTORE:
		if (!(hsm_flags & HS_DIRTY) && (hsm_flags & HS_RELEASED) &&
		    (hsm_flags & HS_ARCHIVED) && !(hsm_flags & HS_LOST))
			rc = 1;
		break;
	case HSMA_REMOVE:
		if (!(hsm_flags & HS_RELEASED) &&
		    (hsm_flags & (HS_ARCHIVED | HS_EXISTS)))
			rc = 1;
		break;
	case HSMA_CANCEL:
		rc = 1;
		break;
	}
	CDEBUG(D_HSM, "fid="DFID" action=%s flags="LPX64
		      " extent="LPX64"-"LPX64" hsm_flags=%.8X %s\n",
		      PFID(&hai->hai_fid),
		      hsm_copytool_action2name(hai->hai_action), rq_flags,
		      hai->hai_extent.offset, hai->hai_extent.length,
		      hsm->mh_flags,
		      (rc ? "compatible" : "uncompatible"));

	RETURN(rc);
}

/*
 * /proc interface used to get/set hsm behaviour (cdt->cdt_policy)
 */
static struct {
	__u64		 bit;
	char		*name;
	char		*nickname;
} hsm_policy_names[] = {
	{ CDT_NONBLOCKING_RESTORE,	"non_blocking_restore",	"nbr"},
	{ CDT_NORETRY_ACTION,		"no_retry_action",	"nra"},
	{ 0 },
};

/**
 * convert a policy name to a bit
 * \param name [IN] policy name
 * \retval 0 unknown
 * \retval   policy bit
 */
static __u64 hsm_policy_str2bit(const char *name)
{
	int	 i;

	for (i = 0 ; hsm_policy_names[i].bit != 0 ; i++)
		if (strncmp(hsm_policy_names[i].nickname, name,
			    strlen(hsm_policy_names[i].nickname)) == 0)
			return hsm_policy_names[i].bit;
	return 0;
}

/**
 * convert a policy bit field to a string
 * \param mask [IN] policy bit field
 * \param buffer [OUT] string
 * \param count [IN] size of buffer
 * \retval size filled in buffer
 */
static int hsm_policy_bit2str(const __u64 mask, char *buffer, int count)
{
	int	 i, j, sz;
	char	*ptr;
	__u64	 bit;
	ENTRY;

	ptr = buffer;
	sz = snprintf(buffer, count, "("LPX64") ", mask);
	ptr += sz;
	count -= sz;
	for (i = 0 ; i < (sizeof(mask) * 8) ; i++) {
		bit = (1ULL << i);
		if (!(bit  & mask))
			continue;

		for (j = 0 ; hsm_policy_names[j].bit != 0 ; j++) {
			if (hsm_policy_names[j].bit == bit) {
				sz = snprintf(ptr, count, "%s(%s) ",
					      hsm_policy_names[j].name,
					      hsm_policy_names[j].nickname);
				ptr += sz;
				count -= sz;
				break;
			}
		}
	}
	RETURN(ptr - buffer);
}

/* methods to read/write hsm policy flags */
static int lprocfs_rd_hsm_policy(char *page, char **start, off_t off,
				 int count, int *eof, void *data)
{
	struct coordinator	*cdt = data;
	int			 sz;
	ENTRY;

	sz = hsm_policy_bit2str(cdt->cdt_policy, page, count);
	page[sz] = '\n';
	sz++;
	page[sz] = '\0';
	RETURN(sz);
}

static int lprocfs_wr_hsm_policy(struct file *file, const char *buffer,
				 unsigned long count, void *data)
{
	struct coordinator	*cdt = data;
	int			 sz;
	char			*start, *end;
	__u64			 policy;
	int			 set;
	char			*buf;
	ENTRY;

	if (strncmp(buffer, "help", 4) == 0) {
		sz = PAGE_SIZE;
		OBD_ALLOC(buf, sz);
		if (!buf)
			RETURN(-ENOMEM);

		hsm_policy_bit2str(CDT_POLICY_MASK, buf, sz);
		CWARN("Supported policies are: %s\n", buf);
		OBD_FREE(buf, sz);
		RETURN(count);
	}

	OBD_ALLOC(buf, count+1);
	if (!buf)
		RETURN(-ENOMEM);
	memcpy(buf, buffer, count);

	buf[count] = '\0';
	start = buf;

	policy = 0;
	do {
		end = strchr(start, ' ');
		if (end != NULL)
			*end = '\0';
		switch (*start) {
		case '-':
			start++;
			set = 0;
			break;
		case '+':
			start++;
			set = 1;
			break;
		default:
			set = 2;
			break;
		}
		policy = hsm_policy_str2bit(start);
		if (!policy)
			break;

		switch (set) {
		case 0:
			cdt->cdt_policy &= ~policy;
			break;
		case 1:
			cdt->cdt_policy |= policy;
			break;
		case 2:
			cdt->cdt_policy = policy;
			break;
		}

		start = end + 1;
	} while (end != NULL);
	OBD_FREE(buf, count+1);
	RETURN(count);
}

#define GENERATE_PROC_METHOD(VAR)					\
static int lprocfs_rd_hsm_##VAR(char *page, char **start, off_t off,	\
				int count, int *eof, void *data)	\
{									\
	struct mdt_device	*mdt = data;				\
	struct coordinator	*cdt = &mdt->mdt_coordinator;		\
	int			 sz;					\
	ENTRY;								\
									\
	sz = snprintf(page, count, LPU64"\n", (__u64)cdt->VAR);		\
	*eof = 1;							\
	RETURN(sz);							\
}									\
static int lprocfs_wr_hsm_##VAR(struct file *file, const char *buffer,	\
				unsigned long count, void *data)	\
									\
{									\
	struct mdt_device	*mdt = data;				\
	struct coordinator	*cdt = &mdt->mdt_coordinator;		\
	unsigned long		 val;					\
	int			 rc;					\
	ENTRY;								\
									\
	rc = kstrtoul(buffer, 0, &val);					\
	if (rc)								\
		RETURN(-EINVAL);					\
	if (val > 0) {							\
		cdt->VAR = val;						\
		RETURN(count);						\
	}								\
	RETURN(-EINVAL);						\
}

GENERATE_PROC_METHOD(cdt_loop_period)
GENERATE_PROC_METHOD(cdt_delay)
GENERATE_PROC_METHOD(cdt_timeout)
GENERATE_PROC_METHOD(cdt_max_request)

/*
 * procfs write method for MDT/hsm_control
 * proc entry is in mdt directory so data is mdt obd_device pointer
 */
#define CDT_ENABLE_CMD   "enabled"
#define CDT_STOP_CMD     "shutdown"
#define CDT_DISABLE_CMD  "disabled"
#define CDT_PURGE_CMD    "purge"
#define CDT_HELP_CMD     "help"

int lprocfs_wr_hsm_cdt_control(struct file *file, const char *buffer,
			       unsigned long count, void *data)
{
	struct obd_device	*obd = data;
	struct mdt_device	*mdt = mdt_dev(obd->obd_lu_dev);
	struct coordinator	*cdt = &(mdt->mdt_coordinator);
	int			 rc, usage = 0;
	ENTRY;

	rc = 0;
	if (strncmp(buffer, CDT_ENABLE_CMD, strlen(CDT_ENABLE_CMD)) == 0) {
		if (cdt->cdt_state == CDT_DISABLE) {
			cdt->cdt_state = CDT_RUNNING;
			mdt_hsm_cdt_wakeup(mdt);
		} else {
			rc = mdt_hsm_cdt_start(mdt);
		}
	} else if (strncmp(buffer, CDT_STOP_CMD, strlen(CDT_STOP_CMD)) == 0) {
		cdt->cdt_state = CDT_STOPPING;
	} else if (strncmp(buffer, CDT_DISABLE_CMD,
			   strlen(CDT_DISABLE_CMD)) == 0) {
		cdt->cdt_state = CDT_DISABLE;
	} else if (strncmp(buffer, CDT_PURGE_CMD, strlen(CDT_PURGE_CMD)) == 0) {
		rc = hsm_cancel_all_actions(mdt);
	} else if (strncmp(buffer, CDT_HELP_CMD, strlen(CDT_HELP_CMD)) == 0) {
		usage = 1;
	} else {
		usage = 1;
		rc = -EINVAL;
	}

	if (usage == 1)
		CERROR("%s: Valid coordinator control commands are: "
		       "%s %s %s %s %s\n", mdt_obd_name(mdt),
		       CDT_ENABLE_CMD, CDT_STOP_CMD, CDT_DISABLE_CMD,
		       CDT_PURGE_CMD, CDT_HELP_CMD);

	if (rc)
		RETURN(rc);

	RETURN(count);
}

int lprocfs_rd_hsm_cdt_control(char *page, char **start, off_t off,
			       int count, int *eof, void *data)
{
	struct obd_device	*obd = data;
	struct coordinator	*cdt;
	int			 sz;
	ENTRY;

	cdt = &(mdt_dev(obd->obd_lu_dev)->mdt_coordinator);
	*eof = 1;

	if (cdt->cdt_state == CDT_INIT)
		sz = snprintf(page, count, "init\n");
	else if (cdt->cdt_state == CDT_RUNNING)
		sz = snprintf(page, count, "enabled\n");
	else if (cdt->cdt_state == CDT_STOPPING)
		sz = snprintf(page, count, "stopping\n");
	else if (cdt->cdt_state == CDT_STOPPED)
		sz = snprintf(page, count, "stopped\n");
	else if (cdt->cdt_state == CDT_DISABLE)
		sz = snprintf(page, count, "disabled\n");
	else
		sz = snprintf(page, count, "unknown\n");

	RETURN(sz);
}

static struct lprocfs_vars lprocfs_mdt_hsm_vars[] = {
	{ "agents",		NULL, NULL, NULL, &mdt_hsm_agent_fops, 0 },
	{ "agent_actions",	NULL, NULL, NULL,
				&mdt_agent_actions_fops, 0444 },
	{ "grace_delay",	lprocfs_rd_hsm_cdt_delay,
				lprocfs_wr_hsm_cdt_delay,
				NULL, NULL, 0 },
	{ "loop_period",	lprocfs_rd_hsm_cdt_loop_period,
				lprocfs_wr_hsm_cdt_loop_period,
				NULL, NULL, 0 },
	{ "max_requests",	lprocfs_rd_hsm_cdt_max_request,
				lprocfs_wr_hsm_cdt_max_request,
				NULL, NULL, 0 },
	{ "policy",		lprocfs_rd_hsm_policy, lprocfs_wr_hsm_policy,
				NULL, NULL, 0 },
	{ "request_timeout",	lprocfs_rd_hsm_cdt_timeout,
				lprocfs_wr_hsm_cdt_timeout,
				NULL, NULL, 0 },
	{ "requests",		NULL, NULL, NULL, &mdt_hsm_request_fops, 0 },
	{ 0 }
};
