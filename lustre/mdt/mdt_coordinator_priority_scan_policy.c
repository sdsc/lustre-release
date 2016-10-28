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
 * Copyright (c) 2016, Commissariat a l'Energie Atomique et aux Energies
 *                     Alternatives.
 */

#include <lustre_log.h>

#include "mdt_coordinator_scan_policies.h"

#define PRIORITY_SCAN_POLICY_RATIO 80
#define PRIORITY_SCAN_POLICY_DEFAULT_MASK ((1 << HSMA_RESTORE) |	\
					   (1 << HSMA_REMOVE)  |	\
					   (1 << HSMA_CANCEL))

/**
 * data passed to llog_cat_process() callback to batch waiting requests in
 * a struct hsm_action_list
 */
struct hsm_scan_request {
	int			 hal_sz;
	int			 hal_used_sz;
	struct hsm_action_list	*hal;
};

struct spo_priority_data {
	struct mdt_thread_info	*mti;
	char			 fsname[MTI_NAME_MAXLEN+1];
	/* safe copy of cdt->max_requests */
	int			 max_requests;	/** vector size */
	struct kref		 kref;
	/* Used in spo_priority_batch_request */
	struct rw_semaphore	 rwsem;		/** to protect mutable params */
	unsigned int		 priority_mask;
	unsigned int		 ratio;
	unsigned int		 full_mask;
	/* request to be sent to agents */
	struct hsm_scan_request *request;
	size_t			 request_sz;	/** allocated size */
	size_t			 request_cnt;	/** used count */
};

/**
 * Add an hai to the hal of an hsm_scan_request
 *
 * \param request	the destination hsm_scan_request
 * \param hai		the request to batch
 *
 * \retval		< 0 if something goes wrong
 */
static int hsm_scan_request_add_hai(struct hsm_scan_request *request,
				    struct hsm_action_item *hai)
{
	size_t hai_len = cfs_size_round(hai->hai_len);

	if (request->hal_sz < request->hal_used_sz + hai_len) {
		/* Resize the request's hal */
		struct hsm_action_list *hal_buffer;
		size_t hal_sz = request->hal_sz * 2;

		while (hal_sz < request->hal_used_sz + hai_len)
			hal_sz *= 2;
		if (hal_sz > HAL_MAXSIZE) {
			hal_sz = HAL_MAXSIZE;
			if (hal_sz < request->hal_used_sz + hai_len)
				/* hal would be too big, do not allow that */
				return -ENOBUFS;
		}
		OBD_ALLOC(hal_buffer, hal_sz);
		if (hal_buffer == NULL)
			return -ENOMEM;

		memcpy(hal_buffer, request->hal, request->hal_sz);
		OBD_FREE(request->hal, request->hal_sz);
		request->hal = hal_buffer;
		request->hal_sz = hal_sz;
	}

	memcpy((char *) request->hal + request->hal_used_sz, hai, hai->hai_len);

	/* Update request's size members */
	request->hal_used_sz += hai_len;
	request->hal->hal_count++;
	return 0;
}

/**
 * Initialize a struct hsm_scan_request with a request from the cdt's llog
 *
 * \param request	the struct hsm_scan_request to initialize
 * \param fsname	the name of the lustre filesystem the coordinator is
 *			working for
 * \param larr		the entry of the llog that contains the request
 *
 * \retval		< 0 if something goes wrong
 */
#define __HAL_MINSIZE (1 << 7)
static int hsm_scan_request_init(struct hsm_scan_request *request, char *fsname,
				 struct llog_agent_req_rec *larr)
{
	size_t hal_sz = __HAL_MINSIZE;
	size_t min_sz = sizeof(*request->hal) +
			cfs_size_round(strlen(fsname) + 1) +
			cfs_size_round(larr->arr_hai.hai_len);

	/* Any hai should be able to be store in an hal
	 * otherwise one should consider raising LDLM_MAXREQSIZE
	 */
	LASSERT(min_sz <= HAL_MAXSIZE);

	/* Once in a while this part of the code should be reviewed to check
	 * that __HAL_MINSIZE corresponds to the size of the average hai
	 */
	if (hal_sz < min_sz) {
		hal_sz *= 2;
		while (hal_sz < min_sz)
			hal_sz *= 2;

		if (hal_sz > HAL_MAXSIZE)
			hal_sz = HAL_MAXSIZE;
		/* No need to check if it is big enough, it was done before */
	}

	OBD_ALLOC(request->hal, hal_sz);
	if (request->hal == NULL)
		return -ENOMEM;

	/* Initialize the hal */
	request->hal->hal_version = HAL_VERSION;
	strlcpy(request->hal->hal_fsname, fsname, strlen(fsname) + 1);
	request->hal->hal_compound_id = larr->arr_compound_id;
	request->hal->hal_archive_id = larr->arr_archive_id;
	request->hal->hal_flags = larr->arr_flags;
	request->hal->hal_count = 0;

	/* Set request's size members */
	request->hal_sz = hal_sz;
	request->hal_used_sz = hal_size(request->hal);
	return 0;
}

/**
 * Returns true if the given action_type has priority
 * (cf. spo_priority_batch_request)
 */
static inline bool has_priority(enum hsm_copytool_action action_type,
				struct spo_priority_data *spd)
{
	bool has_priority;

	down_read(&spd->rwsem);
	has_priority = (1 << action_type) & ~spd->priority_mask;
	up_read(&spd->rwsem);
	return has_priority;
}

/**
 * Batch a request from the llog in a buffer of the private data of the policy
 *
 * \param cdt	the coordinator in charge of processing the request
 * \param larr	the entry in the llog which contains the request
 *
 * \retval	< 0 if something goes wrong
 *
 * Idea of the policy:
 * Non-replaceable requests have priority over replaceable requests.
 * Replaceable requests are inserted to the right side of the array
 * while non-replaceable ones are added to the left.
 * The barrier represents a soft limit.
 * Each slot (hsm_scan_request) can only contain one kind of HSM request
 *
 * Example:
 *
 * RESTORE is more important than ARCHIVE:
 *	priority_mask |= (1 << HSMA_RESTORE)
 *	priority_mask &= ~(1 << HSMA_ARCHIVE)
 *
 * We set the "ratio" so that if possible the total number of requests
 * batched amounts for 2/3 of the total requests.
 * ratio = 66
 *
 * In this example it is considered a struct hsm_action_list can hold a
 * maximum of 2 requests (it can actually hold a lot more).
 * cdt_max_requests is considered to be 3 (default value)
 *
 * Key:
 *	AX	HSMA_ARCHIVE request, number X
 *	RY	HSMA_RESTORE request, number Y
 *	rZ	hsm_scan_request, number Z
 *
 * Requests to queue:
 *	R0, A0, A1, A2, A3, R1, A4, R2, R3, R4
 *
 * t = 0,
 * r0 ########    r1 ########    r2 ########
 * #    #    #    #    #    #	 #    #    #
 * ###########    ###########    ###########
 *
 * t = 1, R0 has priority => fill from the left
 * r0 ########    r1 ########    r2 ########
 * # R0 #    #    #    #    #	 #    #    #
 * ###########    ###########    ###########
 *
 * t = 2, A0 does not have priority => fill from the right
 * r0 ########    r1 ########    r2 ########
 * # R0 #    #    #    #    #	 # A0 #    #
 * ###########    ###########    ###########
 *
 * t = 3,
 * r0 ########    r1 ########    r2 ########
 * # R0 #    #    #    #    #	 # A0 # A1 #
 * ###########    ###########    ###########
 *
 * t = 4, no space left in r2 for A2, use r1
 * r0 ########    r1 ########    r2 ########
 * # R0 #    #    # A2 #    #    # A0 # A1 #
 * ###########    ###########    ###########
 *
 * ...
 *
 * t = 7, no space at all for A4, A4 does not have priority => skip
 * r0 ########    r1 ########    r2 ########
 * # R0 # R1 #    # A2 # A3 #    # A0 # A1 #
 * ###########    ###########    ###########
 *
 * t = 8, no space for R2, R2 has priority => replace r1
 * r0 ########    r1 ########    r2 ########
 * # R0 # R1 #    # R2 #    #    # A0 # A1 #
 * ###########    ###########    ###########
 *
 * t = 9,
 * r0 ########    r1 ########    r2 ########
 * # R0 # R1 #    # R2 # R3 #    # A0 # A1 #
 * ###########    ###########    ###########
 *
 * t = 10, no space for R4, R4 has priority, but the ratio is 2/3,
 * it thus allows at least 1/3 of other requests => skip
 * r0 ########    r1 ########    r2 ########
 * # R0 # R1 #    # R2 # R3 #    # A0 # A1 #
 * ###########    ###########    ###########
 */
static int spo_priority_batch_request(struct coordinator *cdt,
		struct llog_agent_req_rec *larr)
{
	struct hsm_action_item *hai = &larr->arr_hai;
	struct spo_priority_data *spd = cdt->cdt_scan_policy->csp_private_data;
	int start, end;
	int barrier;
	int rc;
	int i;

	/* Fast path check */
	if (hai->hai_action & spd->full_mask)
		/* No more space for that kind of request */
		return 0;

	if (has_priority(hai->hai_action, spd)) {
		start = spd->max_requests - 1;
		end = 0;
	} else {
		start = 0;
		end = spd->max_requests;
	}
	down_read(&spd->rwsem);
	barrier = (spd->max_requests - 1) * spd->ratio / 100;
	up_read(&spd->rwsem);

	/* To avoid starvation on one kind of request */
	if (barrier == 0)
		barrier++;
	else if (barrier == spd->max_requests)
		barrier--;

	for (i = start; (start <= end && i < end) || (start > end && i >= end);
	     (start <= end) ? i++ : i--) {

		struct hsm_scan_request *request = &spd->request[i];
		struct hsm_action_item *hal_item;

		if (request->hal == NULL) {
			rc = hsm_scan_request_init(request, spd->fsname, larr);
			if (rc != 0)
				return rc;
			spd->request_cnt++;
			return hsm_scan_request_add_hai(request, hai);
		}

		/* Take the first hai in hal to check its type */
		hal_item = hai_first(request->hal);
		if (hal_item->hai_action == hai->hai_action) {
			if (larr->arr_compound_id !=
			    request->hal->hal_compound_id)
				/* Keep the old way to group requests */
				continue;
			if (larr->arr_archive_id !=
			    request->hal->hal_archive_id)
				/* Copytool might not be able to handle
				 * multiple backends
				 */
				continue;

			rc = hsm_scan_request_add_hai(request, hai);
			if (rc == 0)
				return 0;
			else if (rc != -ENOBUFS)
				return rc;
			/* else: the hai was not added because there was not any
			 * space left in the hal, and it could not grow anymore.
			 */
		} else if (has_priority(hal_item->hai_action, spd) &&
			   !has_priority(hai->hai_action, spd)) {
			if (i > barrier) {
				/* No more space for that kind of request */
				spd->full_mask |= 1 << hai->hai_action;
				break;
			}

			/* Replace a request that does not have priority */
			OBD_FREE(request->hal, request->hal_sz);
			rc = hsm_scan_request_init(request, spd->fsname, larr);
			if (rc != 0)
				return rc;
			return hsm_scan_request_add_hai(request, hai);
		} else {
			/* No more space for that kind of request */
			spd->full_mask |= 1 << hai->hai_action;
		}
	}
	return 0;
}

/**
 * Is a request taking too long ?
 *
 * \param cdt	the coordinator in charge of the request
 * \param larr	the llog_entry associated with the request
 *
 * \retval	true if the request is taking too long, false otherwise
 */
static bool request_expired(struct coordinator *cdt,
			    struct llog_agent_req_rec *larr)
{
	struct cdt_agent_req *car;
	cfs_time_t now = cfs_time_current_sec();
	cfs_time_t last;

	/* Look for a running request...
	 * Error may occur if the coordinator crashes or stops
	 * while running a request
	 */
	car = mdt_cdt_find_request(cdt, larr->arr_hai.hai_cookie, NULL);
	if (car == NULL) {
		last = larr->arr_req_create;
	} else {
		last = car->car_req_update;
		mdt_cdt_put_request(car);
	}

	return now > last + cdt->cdt_active_req_timeout;
}

/**
 * Cancel a request that timed out
 *
 * \param cdt	the coordinator in charge of the request
 * \param larr	the llog entry matching the request
 * \param llh	llog_handle passed by the llog_cat_process function
 * \param hdr	llog_rec_hdr passed by the llog_cat_process function
 *
 * \retval	< 0 if something goes wrong.
 */
static int expire_request(struct coordinator *cdt,
			  struct llog_agent_req_rec *larr,
			  struct llog_handle *llh, struct llog_rec_hdr *hdr)
{
	struct spo_priority_data *spd = cdt->cdt_scan_policy->csp_private_data;
	struct mdt_thread_info *mti = spd->mti;
	struct mdt_device *mdt = spd->mti->mti_mdt;
	struct hsm_progress_kernel pgs;
	int rc;

	ENTRY;
	dump_llog_agent_req_rec("request timed out, start cleaning", larr);

	pgs.hpk_fid = larr->arr_hai.hai_fid;
	pgs.hpk_cookie = larr->arr_hai.hai_cookie;
	pgs.hpk_extent = larr->arr_hai.hai_extent;
	pgs.hpk_flags = HP_FLAG_COMPLETED;
	pgs.hpk_errval = ENOSYS;
	pgs.hpk_data_version = 0;

	/* Update request state, but do not record in llog to avoid a
	 * deadlock on cdt_llog_lock
	 */
	rc = mdt_hsm_update_request_state(mti, &pgs, 0);
	if (rc)
		CERROR("%s: cannot cleanup timed out request: "DFID" for cookie %#llx action=%s\n",
		       mdt_obd_name(mdt), PFID(&pgs.hpk_fid), pgs.hpk_cookie,
		       hsm_copytool_action2name(larr->arr_hai.hai_action));

	if (rc == -ENOENT) {
		/* The request no longer exists, forget about it.
		 * Do not send a cancel request to the client, for which
		 * an error will be sent back, leading to an endless
		 * cycle of cancellation.
		 */
		RETURN(LLOG_DEL_RECORD);
	}

	/* XXX A cancel request cannot be cancelled. */
	if (larr->arr_hai.hai_action == HSMA_CANCEL)
		RETURN(0);

	larr->arr_status = ARS_CANCELED;
	larr->arr_req_change = cfs_time_current_sec();
	rc = llog_write(mti->mti_env, llh, hdr, hdr->lrh_index);
	if (rc < 0)
		CERROR("%s: cannot update agent log: rc = %d\n",
		       mdt_obd_name(mdt), rc);
	RETURN(0);
}

/**
 *  llog_cat_process() callback, used to:
 *  - find waiting request and start action
 *  - purge canceled and done requests
 *
 * \param env [IN] environment
 * \param llh [IN] llog handle
 * \param hdr [IN] llog record
 * \param data [IN/OUT] cb data = struct spo_priority_data
 *
 * \retval 0 success
 * \retval -ve failure
 *
 */
static int spo_priority_cb(const struct lu_env *env, struct llog_handle *llh,
			   struct llog_rec_hdr *hdr, void *data)
{
	struct llog_agent_req_rec *larr = (struct llog_agent_req_rec *)hdr;
	struct spo_priority_data *spd = data;
	struct coordinator *cdt = &spd->mti->mti_mdt->mdt_coordinator;
	int rc = 0;

	ENTRY;
	dump_llog_agent_req_rec("spo_priority_cb(): ", larr);
	switch (larr->arr_status) {
	case ARS_WAITING:
		/* Are agents full? */
		if (atomic_read(&cdt->cdt_request_count) >=
		    cdt->cdt_max_requests)
			break;

		/* Look for an empty slot to store the hai or a suitable request
		 * to batch it with. The criteria to match two requests is their
		 * compound_id and their archive_id.
		 */
		rc = spo_priority_batch_request(cdt, larr);
		rc = rc == -ENOBUFS ? 0 : rc;
		break;
	case ARS_STARTED:
		if (request_expired(cdt, larr))
			rc = expire_request(cdt, larr, llh, hdr);

		break;
	case ARS_FAILED:
	case ARS_CANCELED:
	case ARS_SUCCEED:
		if ((larr->arr_req_change + cdt->cdt_grace_delay) <
		    cfs_time_current_sec())
			RETURN(LLOG_DEL_RECORD);
		break;
	}
	RETURN(rc);
}

/**
 * Try to send  a batch of waiting requests to an agent
 *
 * \param mti	the struct mdt_thread_info associated to the coordinator
 * \param hal	the hal of requests to send/start
 *
 * \retval	< 0 if something goes wrong
 *
 * The request may silently fail to be sent, this is not an issue in itself.
 * The coordinator will simply try again. The return code does not reflect
 * the fact that every request in the hal was sent.
 */
static int spo_priority_process_waiting_requests(struct mdt_thread_info *mti,
						 struct hsm_action_list *hal)
{
	struct hsm_action_item *hai;
	enum agent_req_status status;
	__u64 *cookies;
	int sz, j;
	int rc = 0;

	rc = mdt_hsm_agent_send(mti, hal, 0);
	/* A failure is supposed to be temporary.
	 * If the copy tool failed to do the request it has to use hsm_progress
	 */
	status = (rc ? ARS_WAITING : ARS_STARTED);

	/* Set up the cookie vector to set records status after copytools
	 * start the request (or fail it).
	 */
	sz = hal->hal_count * sizeof(__u64);
	OBD_ALLOC(cookies, sz);
	if (cookies == NULL)
		return -ENOMEM;

	hai = hai_first(hal);
	for (j = 0; j < hal->hal_count; j++, hai = hai_next(hai))
		cookies[j] = hai->hai_cookie;

	rc = mdt_agent_record_update(mti->mti_env, mti->mti_mdt, cookies,
				     hal->hal_count, status);
	if (rc)
		CERROR("%s: mdt_agent_record_update() failed, rc=%d, cannot update status to %s for %d cookies\n",
		       mdt_obd_name(mti->mti_mdt), rc,
		       agent_req_status2name(status), hal->hal_count);

	OBD_FREE(cookies, sz);
	return rc;
}

/**
 * Scan the coordinator's llog of requests
 *
 * \param cdt	the coordinator whose llog one wants to scan
 *
 * \retval	< 0 if something goes wrong
 *
 * This both performs housekeeping on the requests that have been started, those
 * which failed, succeeded or were canceled and launch new requests on agents.
 */
static int spo_priority_process_llog(struct coordinator *cdt)
{
	struct mdt_thread_info *mti = cdt->cdt_mti;
	struct mdt_device *mdt = mti->mti_mdt;
	struct spo_priority_data *spd = cdt->cdt_scan_policy->csp_private_data;
	int rc = 0;
	int i;

	ENTRY;
	spd->full_mask = 0;

	/* Check the maximum number of requests against the coordinator */
	if (spd->max_requests != cdt->cdt_max_requests) {
		/* cdt_max_requests has changed */
		OBD_FREE(spd->request, spd->request_sz);
		spd->max_requests = cdt->cdt_max_requests;
		spd->request_sz = spd->max_requests * sizeof(*spd->request);
		OBD_ALLOC(spd->request, spd->request_sz);
		if (spd->request == NULL)
			RETURN(-ENOMEM);
	}

	/* Process the llog and batch waiting requests in spd */
	rc = cdt_llog_process(mti->mti_env, mdt, spo_priority_cb, spd);
	if (rc < 0)
		goto out_free;

	CDEBUG(D_HSM, "found %zu requests to send\n", spd->request_cnt);

	/* Fast path check */
	if (list_empty(&cdt->cdt_agents)) {
		CDEBUG(D_HSM, "no agent available, coordinator sleeps\n");
		goto out_free;
	}

	/* Process the (waiting) requests in spd */
	for (i = 0; i < spd->max_requests; i++) {
		struct hsm_action_list *hal = spd->request[i].hal;

		/* Still room for work ? */
		if (atomic_read(&cdt->cdt_request_count) >=
		    cdt->cdt_max_requests)
			break;

		if (hal == NULL)
			continue;

		/* Try to start the requests in hal */
		spo_priority_process_waiting_requests(mti, hal);
	}

out_free:
	/* free the hal(s) allocated by the callback */
	for (i = 0; i < spd->max_requests; i++) {
		struct hsm_scan_request *request = &spd->request[i];

		if (request->hal != NULL) {
			OBD_FREE(request->hal, request->hal_sz);
			request->hal = NULL;
		}
	}
	spd->request_cnt = 0;
	RETURN(rc);
}

/**
 * Set up the private data for the policy
 *
 * \param cdt	the coordinator one wants to set up
 *
 * \retval	< 0 if something goes wrong
 */
static int spo_priority_init(struct coordinator *cdt)
{
	struct mdt_thread_info *mti = cdt->cdt_mti;
	struct mdt_device *mdt = mti->mti_mdt;
	struct spo_priority_data *spd;

	OBD_ALLOC_PTR(spd);
	if (spd == NULL)
		return -ENOMEM;

	/* Constants */
	spd->mti = mti;
	obd_uuid2fsname(spd->fsname, mdt_obd_name(mdt), MTI_NAME_MAXLEN);

	/* The lock to protect the mutable members */
	init_rwsem(&spd->rwsem);

	/* The reference counter to know when to release the ressources */
	kref_init(&spd->kref);

	/* Customizable */
	spd->ratio = PRIORITY_SCAN_POLICY_RATIO;
	spd->priority_mask = PRIORITY_SCAN_POLICY_DEFAULT_MASK;

	/* Coordinator dependent */
	spd->max_requests = cdt->cdt_max_requests;
	spd->request_sz = spd->max_requests * sizeof(*spd->request);
	OBD_ALLOC(spd->request, spd->request_sz);
	if (spd->request == NULL) {
		OBD_FREE_PTR(spd);
		return -ENOMEM;
	}

	/* Expose the private data */
	cdt->cdt_scan_policy->csp_private_data = spd;
	return 0;
}

static void spo_priority_release(struct kref *kref)
{
	struct spo_priority_data *spd = container_of(kref,
						     struct spo_priority_data,
						     kref);

	/* free the hal(s) allocated by the callback */
	if (spd->request != NULL) {
		int i;

		for (i = 0; i < spd->max_requests; i++) {
			struct hsm_scan_request *request = &spd->request[i];

			if (request->hal != NULL)
				OBD_FREE(request->hal, request->hal_sz);
		}
		OBD_FREE(spd->request, spd->request_sz);
	}
	OBD_FREE_PTR(spd);
}

/**
 * Tear down function for the policy
 *
 * \param cdt	the coordinator one wants to tear down
 */
static void spo_priority_exit(struct coordinator *cdt)
{
	struct spo_priority_data *spd = cdt->cdt_scan_policy->csp_private_data;

	kref_put(&spd->kref, spo_priority_release);
}

/**
 * Show the the priority_mask of the priority_policy
 */
static int spo_priority_mask_seq_show(struct seq_file *m, void *v)
{
	struct coordinator *cdt = m->private;
	struct spo_priority_data *spd;
	int mask;
	int i;

	ENTRY;
	down_read(&cdt->cdt_scan_policy_rwsem);

	if (cdt->cdt_scan_policy->csp_id != CSP_PRIORITY) {
		up_read(&cdt->cdt_scan_policy_rwsem);
		RETURN(-ENODEV);
	}

	spd = cdt->cdt_scan_policy->csp_private_data;
	kref_get(&spd->kref);

	up_read(&cdt->cdt_scan_policy_rwsem);

	down_read(&spd->rwsem);

	mask = spd->priority_mask;
	for (i = 0; i < 8 * sizeof(mask); i++) {
		if (mask & (1UL << i))
			seq_printf(m, "[%s]", hsm_copytool_action2name(i));
		else if (strcmp(hsm_copytool_action2name(i), "UNKNOWN") != 0)
			seq_printf(m, "%s", hsm_copytool_action2name(i));
		if (i + 1 < 8 * sizeof(mask))
			seq_putc(m, ' ');
	}
	seq_putc(m, '\n');

	up_read(&spd->rwsem);

	kref_put(&spd->kref, spo_priority_release);

	RETURN(0);
}

/**
 * Set the priority_mask of the priority_policy from a proc file
 */
static ssize_t spo_priority_mask_seq_write(struct file *file,
					   const char __user *buff,
					   size_t count, loff_t *off)
{
	struct seq_file *m = file->private_data;
	struct coordinator *cdt = m->private;
	struct spo_priority_data *spd;
	char *kbuff;
	ssize_t rc;

	ENTRY;
	down_read(&cdt->cdt_scan_policy_rwsem);

	/* Check wether or not the policy is active */
	if (cdt->cdt_scan_policy->csp_id != CSP_PRIORITY) {
		up_read(&cdt->cdt_scan_policy_rwsem);
		RETURN(-ENODEV);
	}

	spd = cdt->cdt_scan_policy->csp_private_data;
	kref_get(&spd->kref);

	up_read(&cdt->cdt_scan_policy_rwsem);

	if (count >= PROC_BUF_SIZE) {
		rc = -EINVAL;
		goto out_put_ref;
	}

	OBD_ALLOC(kbuff, count + 1);
	if (kbuff == NULL) {
		rc = -ENOMEM;
		goto out_put_ref;
	}

	if (copy_from_user(kbuff, buff, count)) {
		rc = -EFAULT;
		goto out_free;
	}

	kbuff[count] = '\0';

	down_write(&spd->rwsem);
	rc = cfs_str2mask(kbuff, hsm_copytool_action2name, &spd->priority_mask,
			  0, HSM_COPYTOOL_ACTION_MAX_MASK);
	up_write(&spd->rwsem);

out_free:
	OBD_FREE(kbuff, count + 1);
out_put_ref:
	kref_put(&spd->kref, spo_priority_release);

	RETURN(rc < 0 ? rc : count);
}

LPROC_SEQ_FOPS(spo_priority_mask);

/**
 * Show the ratio of the priority_policy
 */
static int spo_priority_ratio_seq_show(struct seq_file *m, void *v)
{
	struct coordinator *cdt = m->private;
	struct spo_priority_data *spd;
	unsigned int ratio;

	ENTRY;
	down_read(&cdt->cdt_scan_policy_rwsem);

	/* Check wether or not the policy is active */
	if (cdt->cdt_scan_policy->csp_id != CSP_PRIORITY) {
		up_read(&cdt->cdt_scan_policy_rwsem);
		RETURN(-ENODEV);
	}

	spd = cdt->cdt_scan_policy->csp_private_data;

	down_read(&spd->rwsem);
	ratio = spd->ratio;
	up_read(&spd->rwsem);

	up_read(&cdt->cdt_scan_policy_rwsem);

	seq_printf(m, "%i\n", ratio);

	RETURN(0);
}

/**
 * Set the ratio of the priority policy from a proc file
 */
static ssize_t spo_priority_ratio_seq_write(struct file *file,
					    const char __user *buff,
					    size_t count, loff_t *off)
{
	struct seq_file *m = file->private_data;
	struct coordinator *cdt = m->private;
	struct spo_priority_data *spd;
	int ratio;
	char *kbuff;
	ssize_t rc;

	ENTRY;
	down_read(&cdt->cdt_scan_policy_rwsem);

	/* Check wether or not the policy is active */
	if (cdt->cdt_scan_policy->csp_id != CSP_PRIORITY) {
		up_read(&cdt->cdt_scan_policy_rwsem);
		RETURN(-ENODEV);
	}

	spd = cdt->cdt_scan_policy->csp_private_data;
	kref_get(&spd->kref);

	up_read(&cdt->cdt_scan_policy_rwsem);

	if (count >= PROC_BUF_SIZE) {
		rc = -EINVAL;
		goto out_put_ref;
	}

	OBD_ALLOC(kbuff, count + 1);
	if (kbuff == NULL) {
		rc = -ENOMEM;
		goto out_put_ref;
	}

	if (copy_from_user(kbuff, buff, count)) {
		rc = -EFAULT;
		goto out_free;
	}

	kbuff[count] = '\0';

	rc = kstrtoint(kbuff, 0, &ratio);
	if (rc < 0)
		goto out_free;

	/* Not 100 to avoid complete starvation on a type of request
	 * and more than 50 for semantics (high > low)
	 */
	if (ratio >= 100 || ratio <= 50) {
		rc = -EINVAL;
		goto out_free;
	}

	down_write(&spd->rwsem);
	spd->ratio = ratio;
	up_write(&spd->rwsem);

	CDEBUG(D_HSM, "%s: set ratio on priority policy to %u/%u\n",
	       mdt_obd_name(cdt->cdt_mti->mti_mdt), ratio, 100 - ratio);

out_free:
	OBD_FREE(kbuff, count + 1);
out_put_ref:
	kref_put(&spd->kref, spo_priority_release);

	RETURN(rc < 0 ? rc : count);
}

LPROC_SEQ_FOPS(spo_priority_ratio);

/* Expose the priority policy's operations vector */
static struct scan_policy_operations spo_priority_policy = {
	.spo_init_policy	= spo_priority_init,
	.spo_exit_policy	= spo_priority_exit,
	.spo_process_requests	= spo_priority_process_llog,
};

static struct lprocfs_vars spo_lprocfs_vars[] = {
	{ .name = "csp_priority_mask",
	  .fops = &spo_priority_mask_fops,
	},
	{ .name = "csp_priority_ratio",
	  .fops = &spo_priority_ratio_fops,
	},
	{ 0 }
};

struct cdt_scan_policy csp_priority = {
	.csp_name		= "priority_policy",
	.csp_id			= CSP_PRIORITY,
	.csp_ops		= &spo_priority_policy,
	.csp_lprocfs_vars	= spo_lprocfs_vars,
};
