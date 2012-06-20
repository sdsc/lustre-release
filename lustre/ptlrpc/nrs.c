/* GPL HEADER START
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
 * Copyright (c) 2011, 2012, Intel, Inc.
 *
 * Copyright 2012 Xyratex Technology Limited
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ptlrpc/nrs.c
 *
 * Network Request Scheduler (NRS)
 *
 * Author: Liang Zhen <liang@whamcloud.com>
 * Author: Nikitas Angelinas <nikitas_angelinas@xyratex.com>
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lu_object.h>
#include <lustre/lustre_idl.h>
#include <lustre_req_layout.h>
#include <lprocfs_status.h>
#include <libcfs/libcfs.h>
#include "ptlrpc_internal.h"


extern cfs_spinlock_t ptlrpc_all_services_lock;
extern cfs_list_t ptlrpc_all_services;

/** List of all policies registred with NRS core */
static CFS_LIST_HEAD(nrs_pols_list);
static DEFINE_SPINLOCK(nrs_pols_lock);

static int
nrs_policy_init(struct ptlrpc_nrs_policy *policy, void *arg)
{
	return policy->pol_ops->op_policy_init ?
	       policy->pol_ops->op_policy_init(policy, arg) : 0;
}

static void
nrs_policy_fini(struct ptlrpc_nrs_policy *policy)
{
	LASSERT(policy->pol_ref == 0);
	LASSERT(policy->pol_req_queued == 0);

	if (policy->pol_ops->op_policy_fini)
		policy->pol_ops->op_policy_fini(policy);
}

static int
nrs_policy_ctl_locked(struct ptlrpc_nrs_policy *policy, unsigned opc, void *arg)
{
	return policy->pol_ops->op_policy_ctl ?
	       policy->pol_ops->op_policy_ctl(policy, opc, arg) : -ENOSYS;
}

static void
nrs_policy_stop_final(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs *nrs = policy->pol_nrs;
	ENTRY;

	LASSERT(cfs_list_empty(&policy->pol_list_queued));
	LASSERT(policy->pol_req_queued == 0 &&
		policy->pol_req_started == 0);

	if (policy->pol_ops->op_policy_stop != NULL) {
		cfs_spin_unlock(&nrs->nrs_lock);

		policy->pol_ops->op_policy_stop(policy);

		cfs_spin_lock(&nrs->nrs_lock);

		LASSERT(policy->pol_req_queued == 0 &&
			policy->pol_req_started == 0);
	}

	policy->pol_private = NULL;

	policy->pol_state = NRS_POL_STATE_STOPPED;
	EXIT;
}

static int
nrs_policy_stop_locked(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs *nrs = policy->pol_nrs;
	ENTRY;

	if (nrs->nrs_policy_fallback == policy && !nrs->nrs_stopping)
		RETURN(-EPERM);

	if (policy->pol_state == NRS_POL_STATE_STARTING)
		RETURN(-EAGAIN);

	/* In progress or already stopped */
	if (policy->pol_state != NRS_POL_STATE_STARTED)
		RETURN(0);

	policy->pol_state = NRS_POL_STATE_STOPPING;

	/* Immediately make it invisible */
	if (nrs->nrs_policy_primary == policy) {
		nrs->nrs_policy_primary = NULL;

	} else {
		LASSERT(nrs->nrs_policy_fallback == policy);
		nrs->nrs_policy_fallback = NULL;
	}

	/* I have the only refcount */
	if (policy->pol_ref == 1)
		nrs_policy_stop_final(policy);

	RETURN(0);
}

/**
 */
static void
nrs_policy_stop_primary_helper(struct ptlrpc_nrs *nrs)
{
	struct ptlrpc_nrs_policy *tmp = nrs->nrs_policy_primary;
	ENTRY;

	if (nrs->nrs_policy_primary == NULL) {
		/**
		 * XXX: This should really be RETURN_EXIT, but the latter does
		 * not currently print anything out, and possibly should be
		 * fixed to do so.
		 */
		EXIT;
		return;
	}

	nrs->nrs_policy_primary = NULL;

	LASSERT(tmp->pol_state == NRS_POL_STATE_STARTED);
	tmp->pol_state = NRS_POL_STATE_STOPPING;

	if (tmp->pol_ref == 0)
		nrs_policy_stop_final(tmp);
	EXIT;
}

static int
nrs_policy_start_locked(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs      *nrs = policy->pol_nrs;
	int			rc = 0;
	ENTRY;

	/**
	 * Don't allow multiple starting which is too complex, and has no real
	 * benefit.
	 */
	if (nrs->nrs_policy_starting)
		RETURN(-EAGAIN);

	LASSERT(policy->pol_state != NRS_POL_STATE_STARTING);

	if (policy->pol_state == NRS_POL_STATE_STOPPING ||
	    policy->pol_state == NRS_POL_STATE_UNAVAIL)
		RETURN(-EAGAIN);

	if (policy->pol_flags & PTLRPC_NRS_FL_FALLBACK) {
		/**
		 * This is for cases in which the user sets the policy to the
		 * fallback policy (currently fifo for all services); i.e. the
		 * user is resetting the policy to the default; so we stop the
		 * primary policy, if any.
		 */
		if (policy == nrs->nrs_policy_fallback) {
			nrs_policy_stop_primary_helper(nrs);

			RETURN(0);
		}

		/**
		 * If we reach here, we must be setting up the fallback policy
		 * at service startup time.
		 */
		LASSERT(nrs->nrs_policy_fallback == NULL);
	} else {
		/** Shouldn't start primary policy if w/o fallback policy. */
		if (nrs->nrs_policy_fallback == NULL)
			RETURN(-EPERM);
	}

	/**
	 * Check this here, because we first need to check if this is a command
	 * for stopping the primary policy; see note above.
	 */
	if (policy->pol_state == NRS_POL_STATE_STARTED)
		RETURN(0);

	/** Serialize policy starting */
	nrs->nrs_policy_starting = 1;

	policy->pol_state = NRS_POL_STATE_STARTING;

	if (policy->pol_ops->op_policy_start) {
		cfs_spin_unlock(&nrs->nrs_lock);

		rc = policy->pol_ops->op_policy_start(policy);

		cfs_spin_lock(&nrs->nrs_lock);
		if (rc != 0) {
			policy->pol_state = NRS_POL_STATE_STOPPED;
			GOTO(out, rc);
		}
	}

	policy->pol_state = NRS_POL_STATE_STARTED;

	if (policy->pol_flags & PTLRPC_NRS_FL_FALLBACK) {
		LASSERT(nrs->nrs_policy_fallback == NULL); /* No change */
		nrs->nrs_policy_fallback = policy;

	} else {
		/* Try to stop current primary policy if there is one */
		nrs_policy_stop_primary_helper(nrs);

		nrs->nrs_policy_primary = policy;
	}

 out:
	nrs->nrs_policy_starting = 0;

	RETURN(rc);
}

static void
nrs_policy_get_locked(struct ptlrpc_nrs_policy *policy)
{
	policy->pol_ref++;
}

/**
 *
 */
static struct ptlrpc_nrs_policy *
nrs_policy_find_locked(struct ptlrpc_nrs *nrs, char *name)
{
	struct ptlrpc_nrs_policy *tmp;

	cfs_list_for_each_entry(tmp, &(nrs)->nrs_policy_list, pol_list) {
		if (!strncmp(tmp->pol_name, name, NRS_POL_NAME_MAX)) {
			nrs_policy_get_locked(tmp);
			return tmp;
		}
	}
	return NULL;
}

static void
nrs_policy_put_locked(struct ptlrpc_nrs_policy *policy)
{
	LASSERT(policy->pol_ref > 0);

	policy->pol_ref--;
	if (policy->pol_ref == 0 &&
	    policy->pol_state == NRS_POL_STATE_STOPPING)
		nrs_policy_stop_final(policy);
}

static void
nrs_policy_put(struct ptlrpc_nrs_policy *policy)
{
	cfs_spin_lock(&policy->pol_nrs->nrs_lock);
	nrs_policy_put_locked(policy);
	cfs_spin_unlock(&policy->pol_nrs->nrs_lock);
}

static void
nrs_resource_put(struct ptlrpc_nrs_resource *res)
{
	struct ptlrpc_nrs_policy *policy = res->res_policy;

	/* Not always have op_res_put */
	if (policy->pol_ops->op_res_put != NULL) {
		struct ptlrpc_nrs_resource *parent;

		for (; res != NULL; res = parent) {
			parent = res->res_parent;
			policy->pol_ops->op_res_put(policy, res);
		}
	}
}

/**
 */
static struct ptlrpc_nrs_resource *
nrs_resource_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq, __u8 ltd)
{
	struct ptlrpc_nrs_resource *res = NULL;
	struct ptlrpc_nrs_resource *tmp = NULL;
	int			    rc;

	while (1) {
		rc = policy->pol_ops->op_res_get(policy, nrq, res, &tmp, ltd);
		if (rc < 0) {
			if (res != NULL)
				nrs_resource_put(res);
			return NULL;
		}

		LASSERT(tmp != NULL);
		tmp->res_parent = res;
		tmp->res_policy = policy;
		res = tmp;
		tmp = NULL;
		if (rc > 0)
			return res;
	}
}

/**
 * \a ltd is used for getting HP NRS head resourcesi when set; it
 * signifies that allocations to get resources should be atomic, and implies
 * that the ptlrpoc_nrs_request \a nrs has been already initialized.
 */
static void
nrs_resource_get_safe(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_request *nrq,
		      struct ptlrpc_nrs_resource **resp, __u8 ltd)
{
	struct ptlrpc_nrs_policy   *primary = NULL;
	struct ptlrpc_nrs_policy   *fallback = NULL;

	memset(resp, 0, sizeof(resp[0]) * NRS_RES_MAX);

	cfs_spin_lock(&nrs->nrs_lock);

	fallback = nrs->nrs_policy_fallback;
	nrs_policy_get_locked(fallback);

	primary = nrs->nrs_policy_primary;
	if (primary != NULL)
		nrs_policy_get_locked(primary);

	cfs_spin_unlock(&nrs->nrs_lock);

	resp[NRS_RES_FALLBACK] = nrs_resource_get(fallback, nrq, ltd);
	LASSERT(resp[NRS_RES_FALLBACK] != NULL);

	if (primary != NULL) {
		resp[NRS_RES_PRIMARY] = nrs_resource_get(primary, nrq, ltd);
		/**
		 * A primary policy may exist which may not wish to serve a
		 * particular request for different reasons; release the
		 * reference on the policy as it will not be used for this
		 * request.
		 */
		if (resp[NRS_RES_PRIMARY] == NULL)
			nrs_policy_put(primary);
	}
}

static void
nrs_resource_put_safe(struct ptlrpc_nrs_resource **resp)
{
	int				i;

	for (i = 0; i < NRS_RES_MAX; i++) {
		if (resp[i] != NULL) {
			nrs_resource_put(resp[i]);
			nrs_policy_put(resp[i]->res_policy);
			resp[i] = NULL;
		}
	}
}

static struct ptlrpc_nrs_request *
nrs_request_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct ptlrpc_nrs_request *nrq;

	/** XXX: May want to remove these assertions from here. */
	LASSERT(policy->pol_req_queued > 0);

	nrq = policy->pol_ops->op_req_poll(policy, arg);

	LASSERT(nrq != NULL);
	LASSERT(nrs_request_policy(nrq) == policy);

	return nrq;
}

static void
nrs_request_enqueue(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy;
	int			  rc;
	int			  i;

	/**
	 * Try in descending order, because primary policy is
	 * the preferred choice.
	 */
	for (i = NRS_RES_MAX - 1; i >= 0; i--) {
		if (nrq->nr_res_ptrs[i] == NULL)
			continue;

		nrq->nr_res_idx = i;

		policy = nrq->nr_res_ptrs[i]->res_policy;
		rc = policy->pol_ops->op_req_enqueue(policy, nrq);
		if (rc == 0) {
			policy->pol_nrs->nrs_req_queued++;
			policy->pol_req_queued++;
			return;
		}
	}
	/**
	 * Should never get here, as at least the primary policy should (always)
	 * succeed.
	 */
	LBUG();
}

static struct ptlrpc_nrs_policy *
nrs_request_dequeue(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy;

	policy = nrs_request_policy(nrq);

	policy->pol_ops->op_req_dequeue(policy, nrq);
	policy->pol_nrs->nrs_req_queued--;
	policy->pol_req_queued--;

	return policy;
}

static void
nrs_request_start(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy = nrs_request_policy(nrq);

	policy->pol_req_started++;
	policy->pol_nrs->nrs_req_started++;
	if (policy->pol_ops->op_req_start)
		policy->pol_ops->op_req_start(policy, nrq);
}

static void
nrs_request_stop(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy = nrs_request_policy(nrq);

	if (policy->pol_ops->op_req_stop)
		policy->pol_ops->op_req_stop(policy, nrq);

	policy->pol_nrs->nrs_req_started--;
	policy->pol_req_started--;
}

static int
nrs_policy_ctl(struct ptlrpc_nrs *nrs, char *name, enum ptlrpc_nrs_ctl opc,
	       void *arg)
{
	struct ptlrpc_nrs_policy       *policy;
	struct ptlrpc_nrs_pol_info     *info;
	int				rc = 0;

	cfs_spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, name);
	if (policy == NULL) {
		rc = -ENOENT;
		goto out;
	}

	switch (opc) {
	default:
		rc = nrs_policy_ctl_locked(policy, opc, arg);
		break;

	case PTLRPC_NRS_CTL_GET_INFO:
		if (arg == NULL) {
			rc = -EINVAL;
			break;
		}

		info = (struct ptlrpc_nrs_pol_info *)arg;

		memcpy(info->pi_name, policy->pol_name, NRS_POL_NAME_MAX + 1);
		info->pi_fallback    = !!(policy->pol_flags &
					  PTLRPC_NRS_FL_FALLBACK);
		info->pi_state	     = policy->pol_state;
		info->pi_req_queued  = policy->pol_req_queued;
		info->pi_req_started = policy->pol_req_started;

		break;

	case PTLRPC_NRS_CTL_START:
		rc = nrs_policy_start_locked(policy);
		break;

	case PTLRPC_NRS_CTL_SHRINK:
		/* XXX reserved */
		rc = -ENOSYS;
		break;
	}
 out:
	if (policy != NULL)
		nrs_policy_put_locked(policy);

	cfs_spin_unlock(&nrs->nrs_lock);

	return rc;
}

static int
nrs_policy_unregister(struct ptlrpc_nrs *nrs, char *name)
{
	struct ptlrpc_nrs_policy *policy = NULL;
	ENTRY;

	cfs_spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, name);
	if (policy == NULL) {
		cfs_spin_unlock(&nrs->nrs_lock);

		CERROR("Can't find NRS policy %s\n", name);
		RETURN(-ENOENT);
	}

	if (policy->pol_ref > 1) {
		CERROR("Policy %s is busy with %d references\n", name,
		       (int)policy->pol_ref);
		nrs_policy_put_locked(policy);

		cfs_spin_unlock(&nrs->nrs_lock);
		RETURN(-EBUSY);
	}

	LASSERT(policy->pol_req_queued == 0);
	LASSERT(policy->pol_req_started == 0);

	if (policy->pol_state != NRS_POL_STATE_STOPPED) {
		nrs_policy_stop_locked(policy);
		LASSERT(policy->pol_state == NRS_POL_STATE_STOPPED);
	}

	cfs_list_del(&policy->pol_list);
	nrs_policy_put_locked(policy);

	cfs_spin_unlock(&nrs->nrs_lock);

	nrs_policy_fini(policy);

	LASSERT(policy->pol_private == NULL);
	OBD_FREE_PTR(policy);

	RETURN(0);
}

static int
nrs_policy_register(struct ptlrpc_service_part *svcpt,
		    struct ptlrpc_nrs_pol_desc *desc, void *arg, __u8 hp)
{
	struct ptlrpc_nrs_policy *policy;
	struct ptlrpc_nrs_policy *tmp;
	struct ptlrpc_nrs	 *nrs = ptlrpc_svcpt2nrs(svcpt, hp);
	int			  rc;
	ENTRY;

	LASSERT(desc->pd_ops != NULL);
	LASSERT(desc->pd_ops->op_res_get != NULL);
	LASSERT(desc->pd_ops->op_req_poll != NULL);
	LASSERT(desc->pd_ops->op_req_enqueue != NULL);
	LASSERT(desc->pd_ops->op_req_dequeue != NULL);

	/* should have default policy */
	OBD_CPT_ALLOC_GFP(policy, svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt, sizeof(*policy), CFS_ALLOC_ATOMIC);
	if (policy == NULL)
		RETURN(-ENOMEM);

	policy->pol_nrs     = nrs;
	policy->pol_name    = desc->pd_name;
	policy->pol_ops     = desc->pd_ops;
	policy->pol_state   = desc->pd_flags & PTLRPC_NRS_FL_REG_EXTERN ?
			      NRS_POL_STATE_UNAVAIL : NRS_POL_STATE_STOPPED;
	policy->pol_flags   = desc->pd_flags & ~PTLRPC_NRS_FL_REG_EXTERN;

	CFS_INIT_LIST_HEAD(&policy->pol_list);
	CFS_INIT_LIST_HEAD(&policy->pol_list_queued);

	rc = nrs_policy_init(policy, arg);
	if (rc != 0) {
		OBD_FREE_PTR(policy);
		RETURN(rc);
	}

	cfs_spin_lock(&nrs->nrs_lock);

	tmp = nrs_policy_find_locked(nrs, policy->pol_name);
	if (tmp != NULL) {
		CERROR("NRS policy %s has been registered, can't register it for "
		       "%s\n", policy->pol_name, svcpt->scp_service->srv_name);
		nrs_policy_put_locked(tmp);

		cfs_spin_unlock(&nrs->nrs_lock);
		nrs_policy_fini(policy);
		OBD_FREE_PTR(policy);

		RETURN(-EEXIST);
	}

	cfs_list_add_tail(&policy->pol_list, &nrs->nrs_policy_list);

	if (policy->pol_flags & PTLRPC_NRS_FL_REG_START)
		rc = nrs_policy_start_locked(policy);

	cfs_spin_unlock(&nrs->nrs_lock);

	if (rc != 0)
		(void) nrs_policy_unregister(nrs, policy->pol_name);

	RETURN(rc);
}

static void
ptlrpc_nrs_req_add_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_nrs_policy       *policy;

	LASSERT(req->rq_nrq.nr_initialized);
	LASSERT(!req->rq_nrq.nr_enqueued);

	nrs_request_enqueue(&req->rq_nrq);
	req->rq_nrq.nr_enqueued = 1;

	policy = nrs_request_policy(&req->rq_nrq);
	if (cfs_list_empty(&policy->pol_list_queued)) {
		cfs_list_add_tail(&policy->pol_list_queued,
				  &policy->pol_nrs->nrs_policy_queued);
	}
}

/**
 * Enqueue a request on the high priority NRS head.
 */
static void
__ptlrpc_nrs_hpreq_add_nolock(struct ptlrpc_service_part *svcpt,
			      struct ptlrpc_request *req)
{
	int		opc = lustre_msg_get_opc(req->rq_reqmsg);
	ENTRY;

	LASSERT(svcpt != NULL);

	/* Add to the high priority queue. */
	req->rq_hp = 1;
	ptlrpc_nrs_req_add_nolock(req);
	if (opc != OBD_PING)
		DEBUG_REQ(D_NET, req, "high priority req");
	EXIT;
}

static void
ptlrpc_nrs_hpreq_add_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part	*svcpt = req->rq_rqbd->rqbd_svcpt;
	struct ptlrpc_nrs		*nrs = ptlrpc_svcpt2nrs(svcpt, 1);
	struct ptlrpc_nrs_request	*nrq = &req->rq_nrq;
	struct ptlrpc_nrs_resource	*res1[NRS_RES_MAX];
	struct ptlrpc_nrs_resource	*res2[NRS_RES_MAX];
	ENTRY;

	nrs_resource_get_safe(nrs, nrq, res1, 1);

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	__ptlrpc_nrs_hpreq_add_nolock(svcpt, req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));

	nrs_resource_put_safe(res1);
	EXIT;
}

static int
nrs_svcpt_policy_register(struct ptlrpc_service_part *svcpt,
			  struct ptlrpc_nrs_pol_desc *desc, void *arg)
{
	int rc;
	ENTRY;

	rc = nrs_policy_register(svcpt, desc, arg, 0);
	if (rc != 0 || !nrs_svcpt_has_hp(svcpt))
		RETURN(rc);

	rc = nrs_policy_register(svcpt, desc, arg, 1);

	RETURN(rc);
}

static struct ptlrpc_nrs_pol_desc ptlrpc_nrs_fifo_desc;

static struct ptlrpc_nrs_pol_desc *nrs_pol_std_type[] = {
	&ptlrpc_nrs_fifo_desc,
};

/**
 * Policy is compatible with all services.
 */
int
nrs_policy_compat_all(struct ptlrpc_service_part *svcpt,
		      struct ptlrpc_nrs_pol_desc *desc)
{
	return 1;
}
EXPORT_SYMBOL(nrs_policy_compat_all);

/**
 * Policy is compatible with only one service.
 */
int
nrs_policy_compat_one(struct ptlrpc_service_part *svcpt,
		      struct ptlrpc_nrs_pol_desc *desc)
{
	if (!strncmp(svcpt->scp_service->srv_name, desc->pd_svc_name,
		     NRS_POL_NAME_MAX))
		return 1;
	return 0;
}
EXPORT_SYMBOL(nrs_policy_compat_one);

static inline int
nrs_policy_compatible(struct ptlrpc_service_part *svcpt,
		      struct ptlrpc_nrs_pol_desc *desc)
{
	return desc->pd_compat(svcpt, desc);
}

/**
 * Register all compatible policies in nrs_pols_list, for this service partition
 */
static int
nrs_svcpt_register_policies(struct ptlrpc_service_part *svcpt)
{
	int				rc;
	struct ptlrpc_nrs_pol_desc     *desc;
	ENTRY;

	/**
	 * Cycle through nrs_pols_list and register supported policies with
	 * this service partition.
	 */
	cfs_spin_lock(&nrs_pols_lock);

	cfs_list_for_each_entry(desc, &nrs_pols_list, pd_list) {
		if (nrs_policy_compatible(svcpt, desc)) {
			rc = nrs_svcpt_policy_register(svcpt, desc, NULL);
			if (rc != 0) {
				CERROR("Failed to register NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, svcpt->scp_cpt,
				       svcpt->scp_service->srv_name, rc);
				/**
				 * Fail registration if any of the policies'
				 * registration fails.
				 */
				GOTO(failed, rc);
			}
		}
	}
failed:
	cfs_spin_unlock(&nrs_pols_lock);

	RETURN(rc);
}

static int
nrs_svcpt_policy_unregister(struct ptlrpc_service_part *svcpt,
			    enum ptlrpc_nrs_queue_type queue, char *name)
{
	int	rc = 0;
	ENTRY;

	switch (queue) {
	default:
		RETURN(-EINVAL);

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		rc = nrs_policy_unregister(ptlrpc_svcpt2nrs(svcpt, 0), name);
		if (queue == PTLRPC_NRS_QUEUE_REG || rc != 0)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_unregister(ptlrpc_svcpt2nrs(svcpt, 1), name);
		break;
	}

	RETURN(rc);
}

static int
ptlrpc_svcpt_nrs_setup(struct ptlrpc_service_part *svcpt,
		       struct ptlrpc_service_conf *conf)
{
	struct ptlrpc_nrs	       *nrs;
	int				rc;
	ENTRY;

	nrs = ptlrpc_svcpt2nrs(svcpt, 0);
	nrs->nrs_svcpt = svcpt;
	nrs->nrs_queue_type = PTLRPC_NRS_QUEUE_REG;
	cfs_spin_lock_init(&nrs->nrs_lock);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_list);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_queued);

	if (conf->psc_ops.so_hpreq_handler == NULL)
		goto no_hp;

	OBD_CPT_ALLOC_PTR(svcpt->scp_nrs_hp,
			  svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt);
	if (svcpt->scp_nrs_hp == NULL)
		RETURN(-ENOMEM);
	nrs = ptlrpc_svcpt2nrs(svcpt, 1);
	nrs->nrs_svcpt = svcpt;
	nrs->nrs_queue_type = PTLRPC_NRS_QUEUE_HP;
	cfs_spin_lock_init(&nrs->nrs_lock);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_list);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_queued);

no_hp:
	rc = nrs_svcpt_register_policies(svcpt);

	RETURN(rc);
}

static void
ptlrpc_svcpt_nrs_cleanup(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_nrs	       *nrs;
	struct ptlrpc_nrs_policy       *policy;
	struct ptlrpc_nrs_policy       *tmp;
	int				rc;
	ENTRY;

	/**
	 * Cleanup regular NRS head policies.
	 */
	nrs = ptlrpc_svcpt2nrs(svcpt, 0);
	nrs->nrs_stopping = 1;

	cfs_list_for_each_entry_safe(policy, tmp, &nrs->nrs_policy_list,
				     pol_list) {
		rc = nrs_svcpt_policy_unregister(svcpt, PTLRPC_NRS_QUEUE_REG,
						 policy->pol_name);
		LASSERT(rc == 0);
	}

	/**
	 * Cleanup HP NRS head policies, if there is one.
	 */
	if (!nrs_svcpt_has_hp(svcpt)) {
		EXIT;
		return;
	}

	nrs = ptlrpc_svcpt2nrs(svcpt, 1);
	nrs->nrs_stopping = 1;

	cfs_list_for_each_entry_safe(policy, tmp, &nrs->nrs_policy_list,
				     pol_list) {
		rc = nrs_svcpt_policy_unregister(svcpt, PTLRPC_NRS_QUEUE_HP,
						 policy->pol_name);
		LASSERT(rc == 0);
	}
	OBD_FREE_PTR(svcpt->scp_nrs_hp);
	EXIT;
}

/**
 * Add a policy descriptor to NRS core's global policy list, nrs_pol_list
 */
static int
nrs_policy_add_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs_pol_desc     *tmp;
	ENTRY;

	cfs_list_for_each_entry(tmp, &nrs_pols_list, pd_list) {
		if (!strncmp(tmp->pd_name, desc->pd_name, NRS_POL_NAME_MAX)) {
			CERROR("Failing to register NRS policy %s which has "
			       "already been registered with NRS core!\n",
			       desc->pd_name);
			RETURN(-EEXIST);
		}
	}

	cfs_list_add_tail(&desc->pd_list, &nrs_pols_list);

	RETURN(0);
}

/**
 * Remove a policy descriptor from NRS core's global policy list, nrs_pol_list
 */
static int
nrs_policy_rem_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs_pol_desc     *tmp;
	ENTRY;

	cfs_list_for_each_entry(tmp, &nrs_pols_list, pd_list) {
		if (!strncmp(tmp->pd_name, desc->pd_name, NRS_POL_NAME_MAX)) {
			cfs_list_del(&desc->pd_list);
			RETURN(0);
		}
	}

	CERROR("Unable to find policy %s during unregistration, in NRS core\n",
	       desc->pd_name);
	RETURN(-ENOENT);
}

/**
 * Remove the policy from all service partitions
 * of all supported services.
 *
 * Should only be called on a policy that is not handling any requests; a policy
 * that is in the NRS_POL_STATE_STOPPED state is guaranteed not to have any
 * requests pending
 */
/**
 */
static int
ptlrpc_nrs_policy_unregister_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_service	       *svc;
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc;
	ENTRY;

	cfs_list_for_each_entry(svc, &ptlrpc_all_services, srv_list) {
		ptlrpc_service_for_each_part(svcpt, i, svc) {
			/**
			 * If policy type is not supported, skip to the next
			 * service partition.
			 */
			if (!nrs_policy_compatible(svcpt, desc))
				continue;

			rc = nrs_svcpt_policy_unregister(svcpt,
						       nrs_svcpt_has_hp(svcpt) ?
							 PTLRPC_NRS_QUEUE_BOTH :
							 PTLRPC_NRS_QUEUE_REG,
							 desc->pd_name);
			/**
			 * Ignore -ENOENT as the policy may not have registered
			 * successfully on all service partitions.
			 */
			if (rc == -ENOENT) {
				rc = 0;
			} else if (rc != 0) {
				CERROR("Failed to unregister NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, svcpt->scp_cpt,
				       svcpt->scp_service->srv_name, rc);
				GOTO(fail, rc);
			}
		}
	}

	/**
	 * Only remove the policy when it has been removed from all service
	 * partitions of all services.
	 */
	cfs_spin_lock(&nrs_pols_lock);
	rc = nrs_policy_rem_locked(desc);
	cfs_spin_unlock(&nrs_pols_lock);
fail:
	RETURN(rc);
}

static void
nrs_svcpt_pol_make_avail(struct ptlrpc_nrs *nrs, char *name)
{
	struct ptlrpc_nrs_policy *pol;

	LASSERT(nrs);
	LASSERT(name);

	cfs_spin_lock(&nrs->nrs_lock);
	pol = nrs_policy_find_locked(nrs, name);
	if (pol) {
		LASSERT(pol->pol_state == NRS_POL_STATE_UNAVAIL);
		pol->pol_state = NRS_POL_STATE_STOPPED;
	}
	cfs_spin_unlock(&nrs->nrs_lock);
}

static void
nrs_pol_make_avail(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_service	       *svc;
	struct ptlrpc_service_part     *svcpt;
	int				i;
	ENTRY;

	 /**
	  * Cycle through all registered instances of the policy and place them
	  * at the STOPPED state.
	  */
	cfs_list_for_each_entry(svc, &ptlrpc_all_services, srv_list) {
		ptlrpc_service_for_each_part(svcpt, i, svc) {
			struct ptlrpc_nrs	 *nrs;

			if (!nrs_policy_compatible(svcpt, desc))
				/**
				 * If the policy is not supported, try the
				 * next service partition.
				 */
				continue;

			nrs = ptlrpc_svcpt2nrs(svcpt, 0);
			nrs_svcpt_pol_make_avail(nrs, desc->pd_name);

			if (!nrs_svcpt_has_hp(svcpt))
				continue;

			nrs = ptlrpc_svcpt2nrs(svcpt, 1);
			nrs_svcpt_pol_make_avail(nrs, desc->pd_name);
		}
	}
	EXIT;
}




/**
 * Exported API
 */


int
ptlrpc_nrs_policy_unregister(struct ptlrpc_nrs_pol_desc *desc)
{
	int		rc;
	ENTRY;

	cfs_spin_lock(&ptlrpc_all_services_lock);

	if (desc->pd_flags & PTLRPC_NRS_FL_FALLBACK) {
		CERROR("Unable to unregister a fallback policy, unless the "
		       "service is stopping.\n");
		GOTO(skip, rc = -EPERM);
	}
	rc = ptlrpc_nrs_policy_unregister_locked(desc);
	if (rc == -EBUSY) {
		desc->pd_name[NRS_POL_NAME_MAX] = '\0';
		CERROR("Please first stop policy %s on all service partitions "
		       "and then retry to unregister the policy.\n",
		       desc->pd_name);
	}

skip:
	cfs_spin_unlock(&ptlrpc_all_services_lock);

	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_nrs_policy_unregister);

/**
 * Register a new policy with NRS core.
 *
 * The function will only succeed if policy registration with all service
 * partitions is successful.
 */
int
ptlrpc_nrs_policy_register(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_service	       *svc;
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc;
	int				rc2;
	ENTRY;

	LASSERT(desc != NULL);
	LASSERT(desc->pd_name != NULL);
	LASSERT(desc->pd_ops != NULL);
	LASSERT(desc->pd_compat != NULL);

	desc->pd_name[NRS_POL_NAME_MAX] = '\0';

	if (desc->pd_flags & (PTLRPC_NRS_FL_FALLBACK |
			      PTLRPC_NRS_FL_REG_START)) {
		CERROR("Failing to register NRS policy %s; re-check policy "
		       "flags, externally-registered policies cannot act as "
		       "fallback policies or be started immediately without "
		       "interaction with lprocfs.\n", desc->pd_name);
		RETURN(-EINVAL);
	}

	desc->pd_flags |= PTLRPC_NRS_FL_REG_EXTERN;

	cfs_spin_lock(&nrs_pols_lock);
	rc = nrs_policy_add_locked(desc);
	cfs_spin_unlock(&nrs_pols_lock);

	if (rc != 0)
		RETURN(rc);

	/* Register the new policy on the NRS heads for all ptlrpc services */
        cfs_spin_lock(&ptlrpc_all_services_lock);

	cfs_list_for_each_entry(svc, &ptlrpc_all_services, srv_list) {
		ptlrpc_service_for_each_part(svcpt, i, svc) {
			if (!nrs_policy_compatible(svcpt, desc))
				/**
				 * If the policy is not supported, try the
				 * next service partition.
				 */
				continue;
			/**
			 * If policy type is supported, register the policy.
			 */
			rc = nrs_svcpt_policy_register(svcpt, desc, NULL);
			if (rc != 0) {
				CERROR("Failed to register NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, svcpt->scp_cpt,
				       svcpt->scp_service->srv_name, rc);

				rc2 = ptlrpc_nrs_policy_unregister_locked(desc);
				LASSERT(rc2 == 0);
				GOTO(fail, rc);
			}

		}
	}
	/**
	 */
	nrs_pol_make_avail(desc);
fail:
        cfs_spin_unlock(&ptlrpc_all_services_lock);

	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_nrs_policy_register);

int
ptlrpc_service_nrs_setup(struct ptlrpc_service *svc,
			 struct ptlrpc_service_conf *conf)
{
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc = 0;

	/* Initialize NRS heads on all service CPTs */
	ptlrpc_service_for_each_part(svcpt, i, svc) {
		rc = ptlrpc_svcpt_nrs_setup(svcpt, conf);
		if (rc != 0)
			break;
	}
	RETURN(rc);
}

void
ptlrpc_service_nrs_cleanup(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part     *svcpt;
	int				i;

	/* Clean up NRS heads on all service partitions */
	ptlrpc_service_for_each_part(svcpt, i, svc)
		ptlrpc_svcpt_nrs_cleanup(svcpt);
}

/**
 * Obtains resources for the request.
 */
void
ptlrpc_nrs_req_initialize(struct ptlrpc_service_part *svcpt,
			  struct ptlrpc_request *req)
{
	/**
	 * Obtain regular NRS head resources during initialization.
	 */
	struct ptlrpc_nrs	*nrs = ptlrpc_svcpt2nrs(svcpt, 0);

	memset(&req->rq_nrq, 0, sizeof(req->rq_nrq));
	nrs_resource_get_safe(nrs, &req->rq_nrq, req->rq_nrq.nr_res_ptrs, 0);

	/**
	 * No protection on bit nr_initialized because no contention
	 * at this early stage.
	 */
	req->rq_nrq.nr_initialized = 1;
}

void
ptlrpc_nrs_req_finalize(struct ptlrpc_request *req)
{
	if (req->rq_nrq.nr_initialized) {
		nrs_resource_put_safe(req->rq_nrq.nr_res_ptrs);
		/* no protection on bit nr_initialized because no
		 * contention at this late stage */
		req->rq_nrq.nr_finalized = 1;
	}
}

void
ptlrpc_nrs_req_start_nolock(struct ptlrpc_request *req)
{
	req->rq_nrq.nr_started = 1;
	nrs_request_start(&req->rq_nrq);
}

void
ptlrpc_nrs_req_stop_nolock(struct ptlrpc_request *req)
{
	if (req->rq_nrq.nr_started) {
		nrs_request_stop(&req->rq_nrq);
		req->rq_nrq.nr_stopped = 1;
	}
}

void
ptlrpc_nrs_req_add(struct ptlrpc_service_part *svcpt,
		   struct ptlrpc_request *req, __u8 hp)
{
	cfs_spin_lock(&svcpt->scp_req_lock);

	if (hp)
		ptlrpc_nrs_hpreq_add_nolock(req);
	else
		ptlrpc_nrs_req_add_nolock(req);

	cfs_spin_unlock(&svcpt->scp_req_lock);
}

struct ptlrpc_request *
ptlrpc_nrs_req_poll_nolock(struct ptlrpc_service_part *svcpt, __u8 hp)
{
	struct ptlrpc_nrs	  *nrs = ptlrpc_svcpt2nrs(svcpt, hp);
	struct ptlrpc_nrs_policy  *policy;
	struct ptlrpc_nrs_request *nrq;

	if (unlikely(nrs->nrs_req_queued == 0))
		return NULL;

	/**
	 * Always try to drain requests from all NRS polices even if they are
	 * inactive, because the user can change policy status at runtime.
	 */
	cfs_list_for_each_entry(policy, &(nrs)->nrs_policy_queued,
				pol_list_queued) {
		nrq = nrs_request_poll(policy, NULL);
		if (likely(nrq != NULL))
			return container_of(nrq, struct ptlrpc_request, rq_nrq);
	}

	return NULL;
}

void
ptlrpc_nrs_req_del_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_nrs_policy  *policy;

	LASSERT(req->rq_nrq.nr_enqueued);
	LASSERT(!req->rq_nrq.nr_dequeued);

	policy = nrs_request_policy(&req->rq_nrq);
	nrs_request_dequeue(&req->rq_nrq);
	req->rq_nrq.nr_dequeued = 1;

	/**
	 * If the policy has no more requests queued, remove it from
	 * nrs_policy_queued.
	 */
	if (policy->pol_req_queued == 0) {
		cfs_list_del_init(&policy->pol_list_queued);

	/**
	 * If there are other policies with queued requests, move the current
	 * policy to the end so we can round robin over all policies and drain
	 * the requests.
	 */
	} else if (policy->pol_req_queued != policy->pol_nrs->nrs_req_queued) {
		LASSERT(policy->pol_req_queued <
			policy->pol_nrs->nrs_req_queued);

		cfs_list_move_tail(&policy->pol_list_queued,
				   &policy->pol_nrs->nrs_policy_queued);
	}
}

/**
 * NB: Should be called while holding svcpt->scp_req_lock to get a reliable
 * result.
 */
int
ptlrpc_nrs_req_pending_nolock(struct ptlrpc_service_part *svcpt, __u8 hp)
{
	struct ptlrpc_nrs *nrs = ptlrpc_svcpt2nrs(svcpt, hp);

	return nrs->nrs_req_queued > 0;
};

void
ptlrpc_nrs_req_hp_move(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part	*svcpt = req->rq_rqbd->rqbd_svcpt;
	struct ptlrpc_nrs		*nrs = ptlrpc_svcpt2nrs(svcpt, 1);
	struct ptlrpc_nrs_request	*nrq = &req->rq_nrq;
	struct ptlrpc_nrs_resource	*res1[NRS_RES_MAX];
	struct ptlrpc_nrs_resource	*res2[NRS_RES_MAX];
	ENTRY;

	nrs_resource_get_safe(nrs, nrq, res1, 1);

	cfs_spin_lock(&svcpt->scp_req_lock);

	if (!ptlrpc_nrs_req_can_move(req))
		goto out;

	if (nrq->nr_enqueued) {
		ptlrpc_nrs_req_del_nolock(req);
		nrq->nr_enqueued = nrq->nr_dequeued = 0;
	}

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	__ptlrpc_nrs_hpreq_add_nolock(svcpt, req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));
 out:
	cfs_spin_unlock(&svcpt->scp_req_lock);

	nrs_resource_put_safe(res1);
	EXIT;
}

int
ptlrpc_nrs_policy_control(struct ptlrpc_service_part *svcpt,
			  enum ptlrpc_nrs_queue_type queue, char *name,
			  enum ptlrpc_nrs_ctl opc, void *arg)
{
	int	rc;

	switch (queue) {
	default:
		return -EINVAL;

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		rc = nrs_policy_ctl(ptlrpc_svcpt2nrs(svcpt, 0), name, opc,
				    arg);
		if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_ctl(ptlrpc_svcpt2nrs(svcpt, 1), name, opc,
				    arg);
		break;
	}

	return rc;
}

int
ptlrpc_nrs_init(void)
{
	int	rc;
	int	i;
	ENTRY;

	/** Add standard/built-in policies to nrs_pols list. */
	for (i = 0; i < ARRAY_SIZE(nrs_pol_std_type); i++) {
		/**
		 * No need to take nrs_pols_lock at this early stage.
		 */
		rc = nrs_policy_add_locked(nrs_pol_std_type[i]);
		/**
		 * This should not fail for in-tree policies.
		 */
		LASSERT(rc == 0);
	}

	return rc;
}

void
ptlrpc_nrs_fini(void)
{
}

/**
 * FIFO policy
 */

static int
nrs_fifo_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head;

	OBD_CPT_ALLOC_PTR(head, ptlrpc_nrspol2cptab(policy),
			  ptlrpc_nrspol2cptid(policy));
	if (head == NULL)
		return -ENOMEM;

	CFS_INIT_LIST_HEAD(&head->fh_list);
	policy->pol_private = head;
	return 0;
}

static void
nrs_fifo_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head = policy->pol_private;

	LASSERT(head != NULL);
	LASSERT(cfs_list_empty(&head->fh_list));

	OBD_FREE_PTR(head);
}

static int
nrs_fifo_res_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq,
		 struct ptlrpc_nrs_resource *parent,
		 struct ptlrpc_nrs_resource **resp, __u8 ltd)
{
	/* just use the object embedded in fifo request */
	*resp = &((struct nrs_fifo_head *)policy->pol_private)->fh_res;
	return 1; /* done */
}

static struct ptlrpc_nrs_request *
nrs_fifo_req_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct nrs_fifo_head *head = policy->pol_private;

	LASSERT(head != NULL);

	return cfs_list_empty(&head->fh_list) ? NULL :
	       cfs_list_entry(head->fh_list.next, struct ptlrpc_nrs_request,
			      nr_u.fifo.fr_list);
}

/**
 * The assumption behind this function is that is always succeeds, retuning 0.
 */
static int
nrs_fifo_req_add(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_fifo_head *head;

	head = container_of(nrs_request_resource(nrq), struct nrs_fifo_head,
			    fh_res);
	/* Only used for debugging */
	nrq->nr_u.fifo.fr_sequence = head->fh_sequence++;
	cfs_list_add_tail(&nrq->nr_u.fifo.fr_list, &head->fh_list);

	return 0;
}

static void
nrs_fifo_req_del(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	LASSERT(!cfs_list_empty(&nrq->nr_u.fifo.fr_list));
	cfs_list_del_init(&nrq->nr_u.fifo.fr_list);
}

static void
nrs_fifo_req_start(struct ptlrpc_nrs_policy *policy,
		   struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	CDEBUG(D_RPCTRACE, "NRS start %s request from %s, seq: "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, libcfs_id2str(req->rq_peer),
	       nrq->nr_u.fifo.fr_sequence);
}

static void
nrs_fifo_req_stop(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	CDEBUG(D_RPCTRACE, "NRS stop %s request from %s, seq: "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, libcfs_id2str(req->rq_peer),
	       nrq->nr_u.fifo.fr_sequence);
}

static struct ptlrpc_nrs_ops nrs_fifo_ops = {
	.op_policy_start	= nrs_fifo_start,
	.op_policy_stop		= nrs_fifo_stop,
	.op_res_get		= nrs_fifo_res_get,
	.op_req_poll		= nrs_fifo_req_poll,
	.op_req_enqueue		= nrs_fifo_req_add,
	.op_req_dequeue		= nrs_fifo_req_del,
	.op_req_start		= nrs_fifo_req_start,
	.op_req_stop		= nrs_fifo_req_stop,
};

static struct ptlrpc_nrs_pol_desc ptlrpc_nrs_fifo_desc = {
	.pd_name		= "fifo",
	.pd_ops			= &nrs_fifo_ops,
	.pd_compat		= nrs_policy_compat_all,
	.pd_flags		= PTLRPC_NRS_FL_FALLBACK |
				  PTLRPC_NRS_FL_REG_START
};
