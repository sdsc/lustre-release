/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011 Intel Corporation
 *
 * Copyright 2012 Xyratex Technology Limited
 */
/*
 * lustre/ptlrpc/nrs.c
 *
 * Network Request Scheduler (NRS)
 *
 * Allows to reorder the handling of RPCs at servers.
 *
 * Author: Liang Zhen <liang@whamcloud.com>
 * Author: Nikitas Angelinas <nikitas_angelinas@xyratex.com>
 */
/**
 * \addtogoup nrs
 * @{
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lprocfs_status.h>
#include <libcfs/libcfs.h>
#include "ptlrpc_internal.h"


/**
 * List of all NRS heads on all service partitions of all services; protected
 * by \e nrs_core_mutex.
 */
static CFS_LIST_HEAD(nrs_heads_list);

/**
 * List of all policies registered with NRS core; protected by \e nrs_core_mutex
 */
static CFS_LIST_HEAD(nrs_pols_list);

/**
 * Protects \e nrs_pols_list, and \e nrs_heads_list
 */
static DEFINE_MUTEX(nrs_core_mutex);

static int
nrs_policy_init(struct ptlrpc_nrs_policy *policy)
{
	return policy->pol_ops->op_policy_init ?
	       policy->pol_ops->op_policy_init(policy) : 0;
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
nrs_policy_ctl_locked(struct ptlrpc_nrs_policy *policy, enum ptlrpc_nrs_ctl opc,
		      void *arg)
{
	return policy->pol_ops->op_policy_ctl ?
	       policy->pol_ops->op_policy_ctl(policy, opc, arg) : -ENOSYS;
}

static void
nrs_policy_stop0(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs *nrs = policy->pol_nrs;
	ENTRY;

	LASSERT(cfs_list_empty(&policy->pol_list_queued));
	LASSERT(policy->pol_req_queued == 0 &&
		policy->pol_req_started == 0);

	if (policy->pol_ops->op_policy_stop != NULL) {
		spin_unlock(&nrs->nrs_lock);

		policy->pol_ops->op_policy_stop(policy);

		spin_lock(&nrs->nrs_lock);

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
		nrs_policy_stop0(policy);

	RETURN(0);
}

/**
 * Transitions the \a nrs NRS head's primary policy to
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPING and if the policy has no
 * pending usage references, to ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED.
 *
 * \param[in] nrs The NRS head to carry out this operation on
 */
static void
nrs_policy_stop_primary(struct ptlrpc_nrs *nrs)
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
		nrs_policy_stop0(tmp);
	EXIT;
}

/**
 * Transitions a policy across the ptlrpc_nrs_pol_state range of values, in
 * response to an lprocfs command to start a policy.
 *
 * If a primary policy different to the current one is specified, this function
 * will transition the new policy to the
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STARTING and then to
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STARTED, and will then transition
 * the old primary policy (if there is one) to
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPING, and if there are no outstanding
 * references on the policy to ptlrpc_nrs_pol_stae::NRS_POL_STATE_STOPPED.
 *
 * If the fallback policy is specified, this is taken to indicate an instruction
 * to stop the current primary policy, without substituting it with another
 * primary policy, so the primary policy (if any) is transitioned to
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPING, and if there are no outstanding
 * references on the policy to ptlrpc_nrs_pol_stae::NRS_POL_STATE_STOPPED. In
 * this case, the fallback policy is only left active in the NRS head.
 */
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
			nrs_policy_stop_primary(nrs);

			RETURN(0);
		}

		/**
		 * If we reach here, we must be setting up the fallback policy
		 * at service startup time, and only a single policy with the
		 * nrs_policy_flags::PTLRPC_NRS_FL_FALLBACK flag set can
		 * register with NRS core.
		 */
		LASSERT(nrs->nrs_policy_fallback == NULL);
	} else {
		/**
		 * Shouldn't start primary policy if w/o fallback policy.
		 */
		if (nrs->nrs_policy_fallback == NULL)
			RETURN(-EPERM);

		if (policy->pol_state == NRS_POL_STATE_STARTED)
			RETURN(0);
	}

	/**
	 * Serialize policy starting.across the NRS head
	 */
	nrs->nrs_policy_starting = 1;

	policy->pol_state = NRS_POL_STATE_STARTING;

	if (policy->pol_ops->op_policy_start) {
		spin_unlock(&nrs->nrs_lock);

		rc = policy->pol_ops->op_policy_start(policy);

		spin_lock(&nrs->nrs_lock);
		if (rc != 0) {
			policy->pol_state = NRS_POL_STATE_STOPPED;
			GOTO(out, rc);
		}
	}

	policy->pol_state = NRS_POL_STATE_STARTED;

	if (policy->pol_flags & PTLRPC_NRS_FL_FALLBACK) {
		/**
		 * This path is only used at PTLRPC service setup time.
		 */
		nrs->nrs_policy_fallback = policy;
	} else {
		/*
		 * Try to stop the current primary policy if there is one.
		 */
		nrs_policy_stop_primary(nrs);

		/**
		 * And set the newly-started policy as the primary one.
		 */
		nrs->nrs_policy_primary = policy;
	}

 out:
	nrs->nrs_policy_starting = 0;

	RETURN(rc);
}

/**
 * Increases the policy's usage reference count.
 */
static void
nrs_policy_get_locked(struct ptlrpc_nrs_policy *policy)
{
	policy->pol_ref++;
}

/**
 * Decreases the policy's usage reference count, and stops the policy in case it
 * was already stopping and have no more outstanding usage references (which
 * indicates it has no more queued or started requests, and can be safely
 * stopped).
 */
static void
nrs_policy_put_locked(struct ptlrpc_nrs_policy *policy)
{
	LASSERT(policy->pol_ref > 0);

	policy->pol_ref--;
	if (unlikely(policy->pol_ref == 0 &&
	    policy->pol_state == NRS_POL_STATE_STOPPING))
		nrs_policy_stop0(policy);
}

static void
nrs_policy_put(struct ptlrpc_nrs_policy *policy)
{
	spin_lock(&policy->pol_nrs->nrs_lock);
	nrs_policy_put_locked(policy);
	spin_unlock(&policy->pol_nrs->nrs_lock);
}

/**
 * Find and return a policy by name.
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

/**
 * Release references for the resource hierarchy moving upwards towards the
 * policy instance resource.
 */
static void
nrs_resource_put(struct ptlrpc_nrs_resource *res)
{
	struct ptlrpc_nrs_policy *policy = res->res_policy;

	if (policy->pol_ops->op_res_put != NULL) {
		struct ptlrpc_nrs_resource *parent;

		for (; res != NULL; res = parent) {
			parent = res->res_parent;
			policy->pol_ops->op_res_put(policy, res);
		}
	}
}

/**
 * Obtains references for each resource in the resource hierarchy for request
 * \a nrq if it is to be handled by \a policy.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request
 * \param[in] ltd    Signifies whether we are performing a resource request in
 *		     order to place or move \a nrq to the high-priority NRS
 *		     head; we should not sleep when set.
 *
 * \retval NULL Resource hierarchy references not obtained
 * \retval valid-pointer  The bottom level of the resource hierarchy
 *
 * \see ptlrpc_nrs_pol_ops::op_res_get()
 */
static struct ptlrpc_nrs_resource *
nrs_resource_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq, bool ltd)
{
	/**
	 * Set to NULL to traverse the resource hierarchy from the top.
	 */
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
		/**
		 * Return once we have obtained a reference to the bottom level
		 * of the resource hierarchy.
		 */
		if (rc == NRS_RES_LVL_MAX - 1)
			return res;
	}
}

/**
 * Obtains reources for the resource hierarchies amd policy references for
 * the fallback and current primary policy (if any), that will later be used
 * to handle request \a nrq.
 *
 * \param[in]  nrs  The NRS head instance that will be handling request \a nrq.
 * \param[in]  nrq  The request that is being handled.
 * \param[out] resp The array where references to the resource hierarchy are
 *		    stored.
 * \param[in]  ltd  Is used for getting HP NRS head resources when set; it
 *		    signifies that allocations to get resources should be
 *		    atomic, and implies that the ptlrpoc_nrs_request \a nrs has
 *		    been already initialized.
 */
static void
nrs_resource_get_safe(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_request *nrq,
		      struct ptlrpc_nrs_resource **resp, bool ltd)
{
	struct ptlrpc_nrs_policy   *primary = NULL;
	struct ptlrpc_nrs_policy   *fallback = NULL;

	memset(resp, 0, sizeof(resp[0]) * NRS_RES_MAX);

	/**
	 * Obtain policy references.
	 */
	spin_lock(&nrs->nrs_lock);

	fallback = nrs->nrs_policy_fallback;
	nrs_policy_get_locked(fallback);

	primary = nrs->nrs_policy_primary;
	if (primary != NULL)
		nrs_policy_get_locked(primary);

	spin_unlock(&nrs->nrs_lock);

	/**
	 * Obtain resource hierarchy references.
	 */
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

/**
 * Releases references to resource hierarchies and policies, because they are no
 * longer required; used when request handling has been completed, ot the
 * request is moving to the high priority NRS head.
 *
 * \param resp The resource hierarchy that is being released
 *
 * \see ptlrpcnrs_req_hp_move()
 * \see ptlrpc_nrs_req_finalize()
 */
static void
nrs_resource_put_safe(struct ptlrpc_nrs_resource **resp)
{
        struct ptlrpc_nrs_policy *pols[NRS_RES_MAX];
        struct ptlrpc_nrs        *nrs = NULL;
        int			  i;

        for (i = 0; i < NRS_RES_MAX; i++) {
                if (resp[i] != NULL) {
                        pols[i] = resp[i]->res_policy;
                        nrs_resource_put(resp[i]);
                        resp[i] = NULL;
                } else {
                        pols[i] = NULL;
                }
        }

        for (i = 0; i < NRS_RES_MAX; i++) {
                if (pols[i] == NULL)
                        continue;

                if (nrs == NULL) {
                        nrs = pols[i]->pol_nrs;
                        spin_lock(&nrs->nrs_lock);
                }
                nrs_policy_put_locked(pols[i]);
        }

        if (nrs != NULL)
                spin_unlock(&nrs->nrs_lock);
}

/**
 * Obtains an NRS request from \a policy for handling via polling.
 *
 * \param[in] policy	The policy being polled
 * \param[in,out] arg   Reserved parameter
 */
static struct ptlrpc_nrs_request *
nrs_request_poll(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs_request *nrq;

	/** XXX: May want to remove these assertions from here. */
	LASSERT(policy->pol_req_queued > 0);

	nrq = policy->pol_ops->op_req_poll(policy);

	LASSERT(nrq != NULL);
	LASSERT(nrs_request_policy(nrq) == policy);

	return nrq;
}

/**
 * Enqueues request \a nrq for later handling, via one one the policies for
 * which resources where earlier obtained via nrs_resource_get_safe(). The
 * function attempts to enqueue the request first on the primary policy
 * (if any), since this is the preferred choice.
 *
 * \param nrq The request being enqueued
 *
 * \see nrs_resource_get_safe()
 */
static void
nrs_request_enqueue(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy;
	int			  rc;
	int			  i;

	/**
	 * Try in descending order, because the primary policy (if any) is
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
	 * Should never get here, as at least the primary policy's
	 * ptlrpc_nrs_pol_ops::op_req_enqueue() implementation should always
	 * succeed.
	 */
	LBUG();
}

/**
 * Dequeues request \a nrq from the policy which was used for handling it
 *
 * \param nrq The request being dequeued
 *
 * \see ptlrpc_nrs_req_del_nolock()
 */
static void
nrs_request_dequeue(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy;

	policy = nrs_request_policy(nrq);

	policy->pol_ops->op_req_dequeue(policy, nrq);
	policy->pol_nrs->nrs_req_queued--;
	policy->pol_req_queued--;
}

/**
 * Is called when the request starts being handled, after it has been enqueued,
 * polled and dequeued.
 *
 * \param[in] nrs The NRS request that is starting to be handled; can be used
 *		  for job/resource control.
 *
 * \see ptlrpc_nrs_req_start_nolock()
 */
static void
nrs_request_start(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy = nrs_request_policy(nrq);

	policy->pol_req_started++;
	policy->pol_nrs->nrs_req_started++;
	if (policy->pol_ops->op_req_start)
		policy->pol_ops->op_req_start(policy, nrq);
}

/**
 * Called when a request has been handled
 *
 * \param[in] nrs The request that has been handled; can be used for
 *		  job/resource control.
 *
 * \see ptlrpc_nrs_req_stop_nolock()
 */
static void
nrs_request_stop(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy = nrs_request_policy(nrq);

	if (policy->pol_ops->op_req_stop)
		policy->pol_ops->op_req_stop(policy, nrq);

	policy->pol_nrs->nrs_req_started--;
	policy->pol_req_started--;
}

/**
 * Handler for operations that can be carried out on policies.
 *
 * Handles opcodes that are common to all policy types within NRS core, and
 * passes any unknown opcodes to the policy-specific control function.
 *
 * \param[in]	  nrs  The NRS head this policy belongs to.
 * \param[in]	  name The human-readable policy name; should be the same as
 *		       ptlrpc_nrs_pol_desc::pd_name.
 * \param[in]	  opc  The opcode of the operation being carried out.
 * \param[in,out] arg  Can be used to pass information in and out between when
 *		       carrying an operation; usually data that is private to
 *		       the policy at some level, or generic policy status
 *		       information.
 *
 * \retval -ve error condition
 * \retval   0 operation was carried out successfully
 */
static int
nrs_policy_ctl(struct ptlrpc_nrs *nrs, char *name, enum ptlrpc_nrs_ctl opc,
	       void *arg)
{
	struct ptlrpc_nrs_policy       *policy;
	struct ptlrpc_nrs_pol_info     *info;
	int				rc = 0;

	spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, name);
	if (policy == NULL)
		GOTO(out, rc = -ENODEV);

	switch (opc) {
		/**
		 * Unknown opcode, pass it down to the policy-specific control
		 * function for handling.
		 */
	default:
		rc = nrs_policy_ctl_locked(policy, opc, arg);
		break;

		/**
		 * Obtain status information on \e policy.
		 */
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

		/**
		 * Start \e policy
		 */
	case PTLRPC_NRS_CTL_START:
		rc = nrs_policy_start_locked(policy);
		break;

		/**
		 * TODO: This may need to be augmented for resource deallocation
		 * used by the policies.
		 */
	case PTLRPC_NRS_CTL_SHRINK:
		rc = -ENOSYS;
		break;
	}
 out:
	if (policy != NULL)
		nrs_policy_put_locked(policy);

	spin_unlock(&nrs->nrs_lock);

	return rc;
}

/**
 * Unregisters a policy by name.
 *
 * \param[in] nrs  The NRS head this policy belongs to.
 * \param[in] name The human-readable policy name; should be the same as
 *	           ptlrpc_nrs_pol_desc::pd_name
 *
 * \retval -ve error
 * \retval   0 success
 *
 * \see nrs_svcpt_policy_unregister()
 */
static int
nrs_policy_unregister(struct ptlrpc_nrs *nrs, char *name)
{
	struct ptlrpc_nrs_policy *policy = NULL;
	ENTRY;

	spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, name);
	if (policy == NULL) {
		spin_unlock(&nrs->nrs_lock);

		CERROR("Can't find NRS policy %s\n", name);
		RETURN(-ENODEV);
	}

	if (policy->pol_ref > 1) {
		CERROR("Policy %s is busy with %d references\n", name,
		       (int)policy->pol_ref);
		nrs_policy_put_locked(policy);

		spin_unlock(&nrs->nrs_lock);
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

	spin_unlock(&nrs->nrs_lock);

	nrs_policy_fini(policy);

	LASSERT(policy->pol_private == NULL);
	OBD_FREE_PTR(policy);

	RETURN(0);
}

/**
 * Register a policy from \policy descriptor \a desc.
 *
 * \param[in] svcpt The PLRPC service partition on which the policy will be
 *		    registered.
 * \param[in] desc  The policy descriptor from which the information will be
 *		    obtained to register the policy.
 * \param[in] hp    Is this policy to be registered with the normal or
 *		    high-priority NRS head of \a svcpt?
 *
 * \retval -ve error
 * \retval   0 success
 */
static int
nrs_policy_register(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs_policy       *policy;
	struct ptlrpc_nrs_policy       *tmp;
	struct ptlrpc_service_part     *svcpt = nrs->nrs_svcpt;
	int				rc;
	ENTRY;

	LASSERT(svcpt != NULL);
	LASSERT(desc->pd_ops != NULL);
	LASSERT(desc->pd_ops->op_res_get != NULL);
	LASSERT(desc->pd_ops->op_req_poll != NULL);
	LASSERT(desc->pd_ops->op_req_enqueue != NULL);
	LASSERT(desc->pd_ops->op_req_dequeue != NULL);
	LASSERT(desc->pd_compat != NULL);

	OBD_CPT_ALLOC_PTR(policy, svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt);
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

	rc = nrs_policy_init(policy);
	if (rc != 0) {
		OBD_FREE_PTR(policy);
		RETURN(rc);
	}

	spin_lock(&nrs->nrs_lock);

	tmp = nrs_policy_find_locked(nrs, policy->pol_name);
	if (tmp != NULL) {
		CERROR("NRS policy %s has been registered, can't register it for "
		       "%s\n", policy->pol_name, svcpt->scp_service->srv_name);
		nrs_policy_put_locked(tmp);

		spin_unlock(&nrs->nrs_lock);
		nrs_policy_fini(policy);
		OBD_FREE_PTR(policy);

		RETURN(-EEXIST);
	}

	cfs_list_add_tail(&policy->pol_list, &nrs->nrs_policy_list);

	if (policy->pol_flags & PTLRPC_NRS_FL_REG_START)
		rc = nrs_policy_start_locked(policy);

	spin_unlock(&nrs->nrs_lock);

	if (rc != 0)
		(void) nrs_policy_unregister(nrs, policy->pol_name);

	RETURN(rc);
}

/**
 * Enqueue request \a req using one of the policies its resources are referring
 * to.
 *
 * \param[in] req The request to enqueue.
 */
static void
ptlrpc_nrs_req_add_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_nrs_policy       *policy;

	LASSERT(req->rq_nrq.nr_initialized);
	LASSERT(!req->rq_nrq.nr_enqueued);

	nrs_request_enqueue(&req->rq_nrq);
	req->rq_nrq.nr_enqueued = 1;

	policy = nrs_request_policy(&req->rq_nrq);
	/**
	 * Add the policy to the NRS head's list of policies with enqueued
	 * requests, if it has not been added there.
	 */
	if (cfs_list_empty(&policy->pol_list_queued)) {
		cfs_list_add_tail(&policy->pol_list_queued,
				  &policy->pol_nrs->nrs_policy_queued);
	}
}

/**
 * Helper function for enqueueing a request on the high priority NRS head.
 *
 * \param req The request to enqueue.
 */
static void
ptlrpc_nrs_hpreq_add_nolock0(struct ptlrpc_request *req)
{
	int	opc = lustre_msg_get_opc(req->rq_reqmsg);
	ENTRY;

	/* Add to the high priority queue. */
	req->rq_hp = 1;
	ptlrpc_nrs_req_add_nolock(req);
	if (opc != OBD_PING)
		DEBUG_REQ(D_NET, req, "high priority req");
	EXIT;
}

/**
 * Enqueues request \a req on the high-priority NRS head.
 *
 * Obtains references to the resource hierarchy and policies for the
 * high-priority NRS head, and releases references to the resource hierarchy
 * and policies for the normal NRS head that were obtained during the request
 * initialization phase.
 *
 * \param[in] req The request to enqueue
 *
 * \see ptlrpc_nrs_req_initialize()
 */
static void
ptlrpc_nrs_hpreq_add_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part	*svcpt = req->rq_rqbd->rqbd_svcpt;
	struct ptlrpc_nrs		*nrs = nrs_svcpt2nrs(svcpt, true);
	struct ptlrpc_nrs_request	*nrq = &req->rq_nrq;
	struct ptlrpc_nrs_resource	*res1[NRS_RES_MAX];
	struct ptlrpc_nrs_resource	*res2[NRS_RES_MAX];
	ENTRY;

	nrs_resource_get_safe(nrs, nrq, res1, true);

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	ptlrpc_nrs_hpreq_add_nolock0(req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));

	nrs_resource_put_safe(res1);
	EXIT;
}

static struct ptlrpc_nrs_pol_desc ptlrpc_nrs_fifo_desc;

/**
 * Array of policies that ship alongside NRS core; t.e. ones that do not
 * register externally using ptlrpc_nrs_policy_register().
 */
static struct ptlrpc_nrs_pol_desc *nrs_pols_builtin[] = {
	&ptlrpc_nrs_fifo_desc,
};

/**
 * Returns a boolean predicate indicating whether the policy described by
 * \a desc is adequate for use with NRS head \a nrs.
 *
 * \param[in] nrs The NRS head
 * \param[in] desc  The policy descriptor
 *
 * \retval false The policy is not compatible with the service partition
 * \retval true	 The policy is compatible with the service partition
 */
static inline bool
nrs_policy_compatible(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_pol_desc *desc)
{
	return desc->pd_compat(nrs, desc);
}

/**
 * Registers all compatible policies in nrs_pols_list, for NRS head \a nrs.
 *
 * \param[in] nrs The NRS head
 *
 * \retval -ve error
 * \retval   0 success
 *
 * \see ptlrpc_service_nrs_setup()
 */
static int
nrs_register_policies_locked(struct ptlrpc_nrs *nrs)
{
	int				rc = -EINVAL;
	struct ptlrpc_nrs_pol_desc     *desc;
	ENTRY;

	cfs_list_for_each_entry(desc, &nrs_pols_list, pd_list) {
		if (nrs_policy_compatible(nrs, desc)) {
			rc = nrs_policy_register(nrs, desc);
			if (rc != 0) {
				CERROR("Failed to register NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, nrs->nrs_svcpt->scp_cpt,
				       nrs->nrs_svcpt->scp_service->srv_name,
				       rc);
				/**
				 * Fail registration if any of the policies'
				 * registration fails.
				 */
				GOTO(failed, rc);
			}
		}
	}
failed:

	RETURN(rc);
}

/**
 * Unregisters a policy by name on either the regular, high-priority (if any)
 * or both NRS heads of the service partition \a svcpt.
 *
 * \param[in] svcpt The PTLRPC service partition
 * \param[in] queue Whether to unregister the policy on the normal,
 *		    high-priority or both NRS heads of the service partition.
 * \param[in]name   The policy's human-redable name
 */
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
		rc = nrs_policy_unregister(nrs_svcpt2nrs(svcpt, false),
					   name);
		if (queue == PTLRPC_NRS_QUEUE_REG || rc != 0)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_unregister(nrs_svcpt2nrs(svcpt, true),
					   name);
		break;
	}

	RETURN(rc);
}
/**
 *
 */
static int
svcpt_nrs_setup0(struct ptlrpc_nrs *nrs, struct ptlrpc_service_part *svcpt)
{
	int				rc;
	enum ptlrpc_nrs_queue_type	queue;

	if (nrs == &svcpt->scp_nrs_reg)
		queue = PTLRPC_NRS_QUEUE_REG;
	else if (nrs == svcpt->scp_nrs_hp)
		queue = PTLRPC_NRS_QUEUE_HP;
	else
		LBUG();

	nrs->nrs_svcpt = svcpt;
	nrs->nrs_queue_type = queue;
	spin_lock_init(&nrs->nrs_lock);
	CFS_INIT_LIST_HEAD(&nrs->nrs_heads);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_list);
	CFS_INIT_LIST_HEAD(&nrs->nrs_policy_queued);

	mutex_lock(&nrs_core_mutex);

	cfs_list_add_tail(&nrs->nrs_heads, &nrs_heads_list);
	rc = nrs_register_policies_locked(nrs);

	mutex_unlock(&nrs_core_mutex);

	RETURN(rc);
}

/**
 * Allocates a regular and optionally a high-priority NRS head (if the service
 * handles high-priority RPCs), and then registers all available compatible
 * policies on those NRS heads.
 *
 * \param[n] svcpt The PTLRPC service partition to setup
 * \param[in] conf Used to detec t whether the service is handling high-priority
 *		   RPCs
 */
static int
ptlrpc_svcpt_nrs_setup(struct ptlrpc_service_part *svcpt,
		       struct ptlrpc_service_conf *conf)
{
	struct ptlrpc_nrs	       *nrs;
	int				rc;
	ENTRY;

	/**
	 * Initialize the regular NRS head.
	 */
	nrs = nrs_svcpt2nrs(svcpt, false);
	rc = svcpt_nrs_setup0(nrs, svcpt);
	if (rc)
		GOTO(done, rc);

	/**
	 * Optionally allcoate a high-priority NRS head.
	 */
	if (conf->psc_ops.so_hpreq_handler == NULL)
		goto done;

	OBD_CPT_ALLOC_PTR(svcpt->scp_nrs_hp,
			  svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt);
	if (svcpt->scp_nrs_hp == NULL)
		RETURN(-ENOMEM);

	nrs = nrs_svcpt2nrs(svcpt, true);
	rc = svcpt_nrs_setup0(nrs, svcpt);

done:
	RETURN(rc);
}

/**
 * Unregisters all policies on all available NRS heads in a service partition;
 * called at PTLRPC service unregistration time.
 *
 * \param[in] svcpt The PTLRPC service partition
 */
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
	nrs = nrs_svcpt2nrs(svcpt, false);
	nrs->nrs_stopping = 1;

	cfs_list_for_each_entry_safe(policy, tmp, &nrs->nrs_policy_list,
				     pol_list) {
		rc = nrs_svcpt_policy_unregister(svcpt, PTLRPC_NRS_QUEUE_REG,
						 policy->pol_name);
		LASSERT(rc == 0);
	}
	cfs_list_del(&svcpt->scp_nrs_reg.nrs_heads);

	/**
	 * Cleanup HP NRS head policies, if there is one.
	 */
	if (!nrs_svcpt_has_hp(svcpt)) {
		EXIT;
		return;
	}

	nrs = nrs_svcpt2nrs(svcpt, true);
	nrs->nrs_stopping = 1;

	cfs_list_for_each_entry_safe(policy, tmp, &nrs->nrs_policy_list,
				     pol_list) {
		rc = nrs_svcpt_policy_unregister(svcpt, PTLRPC_NRS_QUEUE_HP,
						 policy->pol_name);
		LASSERT(rc == 0);
	}
	cfs_list_del(&svcpt->scp_nrs_hp->nrs_heads);
	OBD_FREE_PTR(svcpt->scp_nrs_hp);
	EXIT;
}

/**
 * Checks whether the policy in \a desc has been added to NRS core's list of
 * policies, \e nrs_pols_list.
 *
 * \param[in] desc The policy desciptor
 *
 * \retval true The policy is present
 * \retval false The policy is not present
 */
static bool
nrs_policy_exists_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs_pol_desc     *tmp;
	ENTRY;

	cfs_list_for_each_entry(tmp, &nrs_pols_list, pd_list) {
		if (!strncmp(tmp->pd_name, desc->pd_name, NRS_POL_NAME_MAX))
			return true;
	}

	return false;
}

/**
 * Removes the policy from all supported NRS heads
 *
 * \param[in] desc The policy descriptor to unregister
 *
 * \retval -ve error
 * \retval  0  successfully unregistered policy on all supported NRS heads
 *
 * \pre mutex_is_locked(&nrs_core_mutex)
 */
static int
nrs_policy_unregister_all_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs      *nrs;
	int			rc = 0;
	ENTRY;

	cfs_list_for_each_entry(nrs, &nrs_heads_list, nrs_heads) {
		if (!nrs_policy_compatible(nrs, desc))
			/**
			 * The policy may only have registered on compatible
			 * NRS heads.
			 */
			continue;

		rc = nrs_policy_unregister(nrs, desc->pd_name);

		/**
		 * Ignore -ENODEV as the policy may not have registered
		 * successfully on all service partitions.
		 */
		if (rc == -ENODEV) {
			rc = 0;
		} else if (rc != 0) {
			CERROR("Failed to unregister NRS policy %s for "
			       "partition %d of service %s: %d\n",
			       desc->pd_name, nrs->nrs_svcpt->scp_cpt,
			       nrs->nrs_svcpt->scp_service->srv_name, rc);
			break;
		}
	}
	RETURN(rc);
}

/**
 * Transitions a policy from ptlrpc_nrs_pol_state::NRS_POL_STATE_UNAVAIL to
 * ptlrpc_nrs_pol_state::STOPPED; is used to prevent policies that are
 * registering externally using ptlrpc_nrs_policy_register from starting
 * before they have successfully registered on all compatible service
 * partitions.
 *
 * \param[in] nrs  The NRS head that the policy belongs to
 * \param[in] name The human-readable policy name
 */
static void
nrs_pol_make_available0(struct ptlrpc_nrs *nrs, char *name)
{
	struct ptlrpc_nrs_policy *pol;

	LASSERT(nrs);
	LASSERT(name);

	spin_lock(&nrs->nrs_lock);
	pol = nrs_policy_find_locked(nrs, name);
	if (pol) {
		LASSERT(pol->pol_state == NRS_POL_STATE_UNAVAIL);
		pol->pol_state = NRS_POL_STATE_STOPPED;
	}
	spin_unlock(&nrs->nrs_lock);
}

/**
 * Make the policy available on all compatible service partitions of all PTLRPC
 * services.
 *
 * \param[in] desc The descriptor for the policy that is to be made available
 *
 * \see nrs_pol_make_available0()
 */
static void
nrs_pol_make_available_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs	 *nrs;
	ENTRY;

	 /**
	  * Cycle through all registered instances of the policy and place them
	  * at the STOPPED state.
	  */
	cfs_list_for_each_entry(nrs, &nrs_heads_list, nrs_heads) {
		if (!nrs_policy_compatible(nrs, desc))
			continue;
		nrs_pol_make_available0(nrs, desc->pd_name);
	}
	EXIT;
}

/**
 * Registers a new policy with NRS core.
 *
 * Used for policies that register externally with NRS core, ie. ones that are
 * not part of \e nrs_pols_builtin[]. The function will only succeed if policy
 * registration with all compatible service partitions is successful.
 *
 * \param[in] desc The policy descriptor to register
 *
 * \retval -ve error
 * \retval   0 success
 */
int
ptlrpc_nrs_policy_register(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs	       *nrs;
	int				rc;
	int				rc2;
	ENTRY;

	LASSERT(desc != NULL);
	LASSERT(desc->pd_name != NULL);

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

	mutex_lock(&nrs_core_mutex);

	rc = nrs_policy_exists_locked(desc);
	if (rc) {
		CERROR("Failing to register NRS policy %s which has "
		       "already been registered with NRS core!\n",
		       desc->pd_name);
		GOTO(fail, rc = -EEXIST);
	}

	/**
	 * Register the new policy on all supported NRS heads
	 */
	cfs_list_for_each_entry(nrs, &nrs_heads_list, nrs_heads) {
		if (!nrs_policy_compatible(nrs, desc))
			/**
			 * Attempt to register the policy if it is
			 * compatible, otherwise try the next NRS head.
			 */
			continue;

		rc = nrs_policy_register(nrs, desc);
		if (rc != 0) {
			CERROR("Failed to register NRS policy %s for partition "
			       "%d of service %s: %d\n", desc->pd_name,
			       nrs->nrs_svcpt->scp_cpt,
			       nrs->nrs_svcpt->scp_service->srv_name, rc);

			rc2 = nrs_policy_unregister_all_locked(desc);
			/**
			 * Should not fail at this point
			 */
			LASSERT(rc2 == 0);
			GOTO(fail, rc);
		}

	}

	cfs_list_add_tail(&desc->pd_list, &nrs_pols_list);
	/**
	 * The policy has successfully registered with all service partitions,
	 * so mark the policy instances at the NRS heads as available.
	 */
	nrs_pol_make_available_locked(desc);
fail:
	mutex_unlock(&nrs_core_mutex);

	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_nrs_policy_register);

/**
 * Unregisters a previously registered policy with NRS core. All instances of
 * the policy on all NRS heads of all supported services are removed.
 *
 * \param[in] desc The descriptor of the policy to unregister
 *
 * \retval -ve error
 * \retval   0 success
 */
int
ptlrpc_nrs_policy_unregister(struct ptlrpc_nrs_pol_desc *desc)
{
	int		rc;
	ENTRY;

	LASSERT(desc != NULL);

	if (desc->pd_flags & PTLRPC_NRS_FL_FALLBACK) {
		CERROR("Unable to unregister a fallback policy, unless the "
		       "PTLRPC service is stopping.\n");
		RETURN(-EPERM);
	}

	desc->pd_name[NRS_POL_NAME_MAX] = '\0';

	mutex_lock(&nrs_core_mutex);

	rc = nrs_policy_exists_locked(desc);
	if (!rc) {
		CERROR("Failing to unregister NRS policy %s which has "
		       "not been registered with NRS core!\n",
		       desc->pd_name);
		GOTO(fail, rc = -EEXIST);
	}

	rc = nrs_policy_unregister_all_locked(desc);
	if (rc == -EBUSY) {
		CERROR("Please first stop policy %s on all service partitions "
		       "and then retry to unregister the policy.\n",
		       desc->pd_name);
		GOTO(fail, rc);
	}
	CDEBUG(D_INFO, "Unregistering policy %s from NRS core.\n",
	       desc->pd_name);

	cfs_list_del(&desc->pd_list);
fail:
	mutex_unlock(&nrs_core_mutex);

	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_nrs_policy_unregister);

/**
 * Setup NRS heads on all service partitions of service \a svc, and register
 * all compatible policies on those NRS heads.
 *
 * \param[in] svc  The service to setup
 * \param[in] conf Used to determine whether the service will need a
 *		   high-priority NRS head.
 *
 * \retval -ve error, the calling logic should eventually call
 *		      ptlrpc_service_nrs_cleanup() to undo any work performed
 *		      by this function.
 *
 * \see ptlrpc_register_service()
 * \see ptlrpc_service_nrs_cleanup()
 */
int
ptlrpc_service_nrs_setup(struct ptlrpc_service *svc,
			 struct ptlrpc_service_conf *conf)
{
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc = 0;

	/**
	 * Initialize NRS heads on all service CPTs.
	 */
	ptlrpc_service_for_each_part(svcpt, i, svc) {
		rc = ptlrpc_svcpt_nrs_setup(svcpt, conf);
		if (rc != 0)
			break;
	}
	RETURN(rc);
}

/**
 * Unregisters all policies on all service partitions of service \a svc.
 *
 * \param[in] svc The PTLRPC service to unregister
 */
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
 * Obtains resources on the regular NRS head of the service partition \a svcpt
 * for the request \a req.
 *
 * \param[in] svcpt The service partition
 * \param[in] req The request
 */
void
ptlrpc_nrs_req_initialize(struct ptlrpc_service_part *svcpt,
			  struct ptlrpc_request *req)
{
	struct ptlrpc_nrs	*nrs = nrs_svcpt2nrs(svcpt, false);

	memset(&req->rq_nrq, 0, sizeof(req->rq_nrq));
	nrs_resource_get_safe(nrs, &req->rq_nrq, req->rq_nrq.nr_res_ptrs,
			      false);

	/**
	 * It is fine to access \e nr_initialized without locking as there is
	 * no contention at this early stage.
	 */
	req->rq_nrq.nr_initialized = 1;
}

/**
 * Releases resources for a request; is called after the request has been
 * handled.
 *
 * \param[in] req The request
 *
 * \see ptlrpc_server_finish_request()
 */
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

/**
 * Enqueues request \a req on either the regular or high-priority NRS head
 * of service partition \a svcpt.
 *
 * \param[in] svcpt The service partition
 * \param[in] req   The reqeust to be enqueued
 * \param[in] hp    Whether to enqueue the request on the regular or
 *		    high-priority NRS head.
 */
void
ptlrpc_nrs_req_add(struct ptlrpc_service_part *svcpt,
		   struct ptlrpc_request *req, bool hp)
{
	spin_lock(&svcpt->scp_req_lock);

	if (hp)
		ptlrpc_nrs_hpreq_add_nolock(req);
	else
		ptlrpc_nrs_req_add_nolock(req);

	spin_unlock(&svcpt->scp_req_lock);
}

/**
 * Obtains a request for handling from an NRS head of service partition
 * \a svcpt.
 *
 * \param[in] svcpt The service partition
 * \param[in] hp    Whether to obtain a request from the regular or
 *		    high-priority NRS head.
 *
 * \retval the request to be handled
 * \retval NULL on failure
 */
struct ptlrpc_request *
ptlrpc_nrs_req_poll_nolock(struct ptlrpc_service_part *svcpt, bool hp)
{
	struct ptlrpc_nrs	  *nrs = nrs_svcpt2nrs(svcpt, hp);
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
		nrq = nrs_request_poll(policy);
		if (likely(nrq != NULL))
			return container_of(nrq, struct ptlrpc_request, rq_nrq);
	}

	return NULL;
}

/**
 * Dequeues a request that was previously obtained via ptlrpc_nrs_req_poll() and
 * is about to be handled.
 *
 * \param[in] req The request
 */
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
	 * ptlrpc_nrs::nrs_policy_queued.
	 */
	if (policy->pol_req_queued == 0) {
		cfs_list_del_init(&policy->pol_list_queued);

	/**
	 * If there are other policies with queued requests, move the current
	 * policy to the end so that we can round robin over all policies and
	 * drain the requests.
	 */
	} else if (policy->pol_req_queued != policy->pol_nrs->nrs_req_queued) {
		LASSERT(policy->pol_req_queued <
			policy->pol_nrs->nrs_req_queued);

		cfs_list_move_tail(&policy->pol_list_queued,
				   &policy->pol_nrs->nrs_policy_queued);
	}
}

/**
 * Returns whether there are any requests currently enqueued on any of the
 * policies of service partition's \a svcpt NRS head specified by \a hp. Should
 * be called while holding ptlrpc_service_part::scp_req_lock to get a reliable
 * result.
 *
 * \param[in] svcpt The service partition to enquire.
 * \param[in] hp    Whether the regular or high-priority NRS head is to be
 *		    enquired.
 *
 * \retval false The indicated NRS head has no enqueued requests.
 * \retval true	 The indicated NRS head has some enqueued requests.
 */
bool
ptlrpc_nrs_req_pending_nolock(struct ptlrpc_service_part *svcpt, bool hp)
{
	struct ptlrpc_nrs *nrs = nrs_svcpt2nrs(svcpt, hp);

	return nrs->nrs_req_queued > 0;
};

/**
 * Moves request \a req from the regular to the high-priority NRS head.
 *
 * \param[in] req The request to move
 */
void
ptlrpc_nrs_req_hp_move(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part	*svcpt = req->rq_rqbd->rqbd_svcpt;
	struct ptlrpc_nrs		*nrs = nrs_svcpt2nrs(svcpt, true);
	struct ptlrpc_nrs_request	*nrq = &req->rq_nrq;
	struct ptlrpc_nrs_resource	*res1[NRS_RES_MAX];
	struct ptlrpc_nrs_resource	*res2[NRS_RES_MAX];
	ENTRY;

	/**
	 * Obtain the high-priority NRS head resources.
	 */
	nrs_resource_get_safe(nrs, nrq, res1, true);

	spin_lock(&svcpt->scp_req_lock);

	if (!ptlrpc_nrs_req_can_move(req))
		goto out;

	ptlrpc_nrs_req_del_nolock(req);
	nrq->nr_enqueued = nrq->nr_dequeued = 0;

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	ptlrpc_nrs_hpreq_add_nolock0(req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));
 out:
	spin_unlock(&svcpt->scp_req_lock);

	/**
	 * Release either the regular NRS head resources if we moved the
	 * request, or the high-priority NRS head resources if we took a
	 * reference earlier in this function and ptlrpc_nrs_rq_can_move()
	 * returned false.
	 */
	nrs_resource_put_safe(res1);
	EXIT;
}

/**
 * Carries out control operation \a opc on the policy identified by the
 * human-readable \a name, on service partition \a svcpt.
 *
 * \param[in]	  svcpt The service partition the policy belongs to.
 * \param[in]	  queue Whether to carry out the command on the policy which
 *			belongs to the regular, high-priority, or both NRS heads
 *			of service partitions \a svcpt.
 * \param[in]	  name  The policy to act upon, by human-readable name
 * \param[in]	  opc	The opcode of the operation to carry out
 * \param[in,out] arg	Can be used as a generic in/out buffer between control
 *			operations and the user environment.
 *
 *\retval -ve error condition
 *\retval   0 operation was carried out successfully
 */
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
		rc = nrs_policy_ctl(nrs_svcpt2nrs(svcpt, false), name, opc,
				    arg);
		if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_ctl(nrs_svcpt2nrs(svcpt, true), name, opc,
				    arg);
		break;
	}

	return rc;
}

/**
 * Adds all policies that ship with NRS, i.e. those in the \e nrs_pols_builtin
 * array, to NRS core's list of policies \e nrs_pols_list.
 *
 * \retval 0 All policy descriptors in \e nrs_pols_builtin have been added
 *	     successfully to \e nrs_pols_list
 */
int
ptlrpc_nrs_init(void)
{
	int	rc = -EINVAL;
	int	i;
	ENTRY;

	for (i = 0; i < ARRAY_SIZE(nrs_pols_builtin); i++) {
		/**
		 * No need to take nrs_core_mutex as there is no contention at
		 * this early stage.
		 */
		rc = nrs_policy_exists_locked(nrs_pols_builtin[i]);
		/**
		 * This should not fail for in-tree policies.
		 */
		LASSERT(rc == false);
		cfs_list_add_tail(&nrs_pols_builtin[i]->pd_list,
				  &nrs_pols_list);
	}

	RETURN(rc);
}

/**
 * Stub finalization function
 */
void
ptlrpc_nrs_fini(void)
{
}

/**
 * \name fifo
 *
 * The FIFO policy is a logical wrapper around previous, non-NRS functionality.
 * It schedules RPCs in the same order as they are queued from LNet.
 *
 * @{
 */

/**
 * Is called before the policy transitions into
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STARTED; allocates and initializes a
 * policy-specific private data structure.
 *
 * \param[in] policy The policy to start
 *
 * \retval -ENOMEM OOM error
 * \retval  0	   success
 *
 * \see nrs_policy_register()
 * \see nrs_policy_ctl()
 */
static int
nrs_fifo_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head;

	OBD_CPT_ALLOC_PTR(head, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (head == NULL)
		return -ENOMEM;

	CFS_INIT_LIST_HEAD(&head->fh_list);
	policy->pol_private = head;
	return 0;
}

/**
 * Is called before the policy transitions into
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED; deallocates the policy-specific
 * private data structure.
 *
 * \param[in] policy The policy to stop
 *
 * \see nrs_policy_stop0()
 */
static void
nrs_fifo_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head = policy->pol_private;

	LASSERT(head != NULL);
	LASSERT(cfs_list_empty(&head->fh_list));

	OBD_FREE_PTR(head);
}

/**
 * Is called for obtaining a FIFO policy resource.
 *
 * \param[in]  policy The policy on which the request is being asked for
 * \param[in]  nrq    The request for which resources are being taken
 * \param[in]  parent Parent resource, unused in this policy
 * \param[out] resp   Resources references are placed in this array
 * \param[in]  ltd    Signifies limited caller context; unused in this policy
 *
 * \retval NRS_RES_LVL_CHILD1 The FIFO policy only has a one-level resource
 *			      hierarchy, as since it implements a simple
 *			      scheduling algorithm in which request priority is
 *			      determined on the request arrival order, it does
 *			      not need to maintain a set of resources that would
 *			      otherwise be used to calculate a request's
 *			      priority
 *
 * \see nrs_resource_get_safe()
 */
static enum nrs_resource_level
nrs_fifo_res_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq,
		 struct ptlrpc_nrs_resource *parent,
		 struct ptlrpc_nrs_resource **resp, bool ltd)
{
	/**
	 * Just return the resource embedded inside nrs_fifo_head, and end this
	 * resource hierarchy reference request.
	 */
	*resp = &((struct nrs_fifo_head *)policy->pol_private)->fh_res;
	return NRS_RES_LVL_CHILD1;
}

/**
 * Called when polling the fifo policy for a request.
 *
 * \param[in] poliy The policy being polled
 *
 * \retval The request to be handled; this is the next request in the FIFO
 *	   queue
 * \see ptlrpc_nrs_req_poll_nolock()
 */
static struct ptlrpc_nrs_request *
nrs_fifo_req_poll(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head = policy->pol_private;

	LASSERT(head != NULL);

	return cfs_list_empty(&head->fh_list) ? NULL :
	       cfs_list_entry(head->fh_list.next, struct ptlrpc_nrs_request,
			      nr_u.fifo.fr_list);
}

/**
 * Adds request \a nrq to \a policy's list of queued requests
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to add
 *
 * \retval 0 success; nrs_request_enqueue() assumes this function will always
 *		      succeed
 */
static int
nrs_fifo_req_add(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_fifo_head *head;

	head = container_of(nrs_request_resource(nrq), struct nrs_fifo_head,
			    fh_res);
	/**
	 * Only used for debugging
	 */
	nrq->nr_u.fifo.fr_sequence = head->fh_sequence++;
	cfs_list_add_tail(&nrq->nr_u.fifo.fr_list, &head->fh_list);

	return 0;
}

/**
 * Removes request \a nrq from \a policy's list of queued requests.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to remove
 */
static void
nrs_fifo_req_del(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	LASSERT(!cfs_list_empty(&nrq->nr_u.fifo.fr_list));
	cfs_list_del_init(&nrq->nr_u.fifo.fr_list);
}

/**
 * Prints a debug statement right before the request \a nrq starts being
 * handled.
 *
 * \param[in] policy The policy handling the request
 * \param[in] nrq    The request being handled
 */
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

/**
 * Prints a debug statement right before the request \a nrq stops being
 * handled.
 *
 * \param[in] policy The policy handling the request
 * \param[in] nrq    The request being handled
 *
 * \see ptlrpc_server_finish_request()
 * \see ptlrpc_nrs_req_stop_nolock()
 */
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

/**
 * FIFO policy operations
 */
static struct ptlrpc_nrs_pol_ops nrs_fifo_ops = {
	.op_policy_start	= nrs_fifo_start,
	.op_policy_stop		= nrs_fifo_stop,
	.op_res_get		= nrs_fifo_res_get,
	.op_req_poll		= nrs_fifo_req_poll,
	.op_req_enqueue		= nrs_fifo_req_add,
	.op_req_dequeue		= nrs_fifo_req_del,
	.op_req_start		= nrs_fifo_req_start,
	.op_req_stop		= nrs_fifo_req_stop,
};

/**
 * FIFO policy descriptor
 */
static struct ptlrpc_nrs_pol_desc ptlrpc_nrs_fifo_desc = {
	.pd_name		= "fifo",
	.pd_ops			= &nrs_fifo_ops,
	.pd_compat		= nrs_policy_compat_all,
	.pd_flags		= PTLRPC_NRS_FL_FALLBACK |
				  PTLRPC_NRS_FL_REG_START
};

/** @} fifo */

/** @} nrs */
