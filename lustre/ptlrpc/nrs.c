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

/* XXX: This is just for liblustre. Remove macro when "cfs_" prefix is dropped
 * from cfs_list_head. */
#if defined (__linux__) && defined(__KERNEL__)
extern struct list_head ptlrpc_all_services;
#else
extern struct cfs_list_head ptlrpc_all_services;
#endif
extern spinlock_t ptlrpc_all_services_lock;

/**
 * NRS core object.
 */
struct nrs_core nrs_core;

static int
nrs_policy_init(struct ptlrpc_nrs_policy *policy)
{
	return policy->pol_ops->op_policy_init != NULL ?
	       policy->pol_ops->op_policy_init(policy) : 0;
}

static void
nrs_policy_fini(struct ptlrpc_nrs_policy *policy)
{
	LASSERT(policy->pol_ref == 0);
	LASSERT(policy->pol_req_queued == 0);

	if (policy->pol_ops->op_policy_fini != NULL)
		policy->pol_ops->op_policy_fini(policy);
}

static int
nrs_policy_ctl_locked(struct ptlrpc_nrs_policy *policy, enum ptlrpc_nrs_ctl opc,
		      void *arg)
{
	return policy->pol_ops->op_policy_ctl != NULL ?
	       policy->pol_ops->op_policy_ctl(policy, opc, arg) : -ENOSYS;
}

static void
nrs_policy_stop0(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs *nrs = policy->pol_nrs;
	ENTRY;

	if (policy->pol_ops->op_policy_stop != NULL) {
		spin_unlock(&nrs->nrs_lock);

		policy->pol_ops->op_policy_stop(policy);

		spin_lock(&nrs->nrs_lock);
	}

	LASSERT(cfs_list_empty(&policy->pol_list_queued));
	LASSERT(policy->pol_req_queued == 0 &&
		policy->pol_req_started == 0);

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

	if (tmp == NULL) {
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
		if (strcmp(tmp->pol_name, name) == 0) {
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
 * \param[in] policy	  The policy
 * \param[in] nrq	  The request
 * \param[in] moving_req  Denotes whether this is a call to the function by
 *			  ldlm_lock_reorder_req(), in order to move \a nrq to
 *			  the high-priority NRS head; we should not sleep when
 *			  set.
 *
 * \retval NULL Resource hierarchy references not obtained
 * \retval valid-pointer  The bottom level of the resource hierarchy
 *
 * \see ptlrpc_nrs_pol_ops::op_res_get()
 */
static struct ptlrpc_nrs_resource *
nrs_resource_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq, bool moving_req)
{
	/**
	 * Set to NULL to traverse the resource hierarchy from the top.
	 */
	struct ptlrpc_nrs_resource *res = NULL;
	struct ptlrpc_nrs_resource *tmp = NULL;
	int			    rc;

	while (1) {
		rc = policy->pol_ops->op_res_get(policy, nrq, res, &tmp,
						 moving_req);
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
		if (rc > 0)
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
 * \param[in]  moving_req  Is set when obtaining resources while moving a
 *			   request from a policy on the regular NRS head to a
 *			   policy on the HP NRS head (via
 *			   ldlm_lock_reorder_req()). It signifies that
 *			   allocations to get resources should be atomic; for
 *			   a full explanation, see comment in
 *			   ptlrpc_nrs_pol_ops::op_res_get().
 */
static void
nrs_resource_get_safe(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_request *nrq,
		      struct ptlrpc_nrs_resource **resp, bool moving_req)
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
	resp[NRS_RES_FALLBACK] = nrs_resource_get(fallback, nrq, moving_req);
	LASSERT(resp[NRS_RES_FALLBACK] != NULL);

	if (primary != NULL) {
		resp[NRS_RES_PRIMARY] = nrs_resource_get(primary, nrq,
							 moving_req);
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

	LASSERT(policy->pol_nrs->nrs_req_queued > 0);
	LASSERT(policy->pol_req_queued > 0);

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

	LASSERT(policy->pol_nrs->nrs_req_started > 0);
	LASSERT(policy->pol_req_started > 0);

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
	int				rc = 0;

	spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, name);
	if (policy == NULL)
		GOTO(out, rc = -ENOENT);

	switch (opc) {
		/**
		 * Unknown opcode, pass it down to the policy-specific control
		 * function for handling.
		 */
	default:
		rc = nrs_policy_ctl_locked(policy, opc, arg);
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
		RETURN(-ENOENT);
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
	nrs->nrs_num_pols--;

	nrs_policy_put_locked(policy);

	spin_unlock(&nrs->nrs_lock);

	nrs_policy_fini(policy);

	LASSERT(policy->pol_private == NULL);
	OBD_FREE_PTR(policy);

	RETURN(0);
}

/**
 * Register a policy from \policy descriptor \a desc with NRS head \a nrs.
 *
 * \param[in] nrs   The NRS head on which the policy will be registered.
 * \param[in] desc  The policy descriptor from which the information will be
 *		    obtained to register the policy.
 * \param[in] ltd   Signifies that an external policy is registering, so this
 *		    function is called in atomic context.
 *
 * \retval -ve error
 * \retval   0 success
 */
static int
nrs_policy_register(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_pol_desc *desc,
		    bool ltd)
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

	OBD_CPT_ALLOC_GFP(policy, svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt, sizeof(*policy),
			  !ltd ? CFS_ALLOC_IO : CFS_ALLOC_ATOMIC);
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
		CERROR("NRS policy %s has been registered, can't register it "
		       "for %s\n",
		       policy->pol_name, svcpt->scp_service->srv_name);
		nrs_policy_put_locked(tmp);

		spin_unlock(&nrs->nrs_lock);
		nrs_policy_fini(policy);
		OBD_FREE_PTR(policy);

		RETURN(-EEXIST);
	}

	cfs_list_add_tail(&policy->pol_list, &nrs->nrs_policy_list);
	nrs->nrs_num_pols++;

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
	if (cfs_list_empty(&policy->pol_list_queued))
		cfs_list_add_tail(&policy->pol_list_queued,
				  &policy->pol_nrs->nrs_policy_queued);
}

/**
 * Enqueue a request on the high priority NRS head.
 *
 * \param req The request to enqueue.
 */
static void
ptlrpc_nrs_hpreq_add_nolock(struct ptlrpc_request *req)
{
	int	opc = lustre_msg_get_opc(req->rq_reqmsg);
	ENTRY;

	spin_lock(&req->rq_lock);
	req->rq_hp = 1;
	ptlrpc_nrs_req_add_nolock(req);
	if (opc != OBD_PING)
		DEBUG_REQ(D_NET, req, "high priority req");
	spin_unlock(&req->rq_lock);
	EXIT;
}

/* ptlrpc/nrs_fifo.c */
extern struct ptlrpc_nrs_pol_desc ptlrpc_nrs_fifo_desc;

/**
 * Array of policies that ship alongside NRS core; t.e. ones that do not
 * register externally using ptlrpc_nrs_policy_register().
 */
static struct ptlrpc_nrs_pol_desc *nrs_pols_builtin[] = {
	&ptlrpc_nrs_fifo_desc,
};

/**
 * Returns a boolean predicate indicating whether the policy described by
 * \a desc is adequate for use with service \a svc.
 *
 * \param[in] nrs    The service
 * \param[in] desc  The policy descriptor
 *
 * \retval false The policy is not compatible with the service partition
 * \retval true	 The policy is compatible with the service partition
 */
static inline bool
nrs_policy_compatible(struct ptlrpc_service *svc,
		      struct ptlrpc_nrs_pol_desc *desc)
{
	return desc->pd_compat(svc, desc);
}

/**
 * Registers all compatible policies in nrs_core.nrs_policies, for NRS head
 * \a nrs.
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
	/* For convenience */
	struct ptlrpc_service_part     *svcpt = nrs->nrs_svcpt;
	struct ptlrpc_service	       *svc = svcpt->scp_service;
	ENTRY;

	cfs_list_for_each_entry(desc, &nrs_core.nrs_policies, pd_list) {
		if (nrs_policy_compatible(svc, desc)) {
			rc = nrs_policy_register(nrs, desc, false);
			if (rc != 0) {
				CERROR("Failed to register NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, svcpt->scp_cpt,
				       svc->srv_name, rc);
				/**
				 * Fail registration if any of the policies'
				 * registration fails.
				 */
				break;
			}
		}
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

	cfs_list_add_tail(&nrs->nrs_heads, &nrs_core.nrs_heads);
	rc = nrs_register_policies_locked(nrs);

	RETURN(rc);
}

/**
 * Allocates a regular and optionally a high-priority NRS head (if the service
 * handles high-priority RPCs), and then registers all available compatible
 * policies on those NRS heads.
 *
 * \param[n] svcpt The PTLRPC service partition to setup
 */
static int
ptlrpc_svcpt_nrs_setup(struct ptlrpc_service_part *svcpt)
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
		GOTO(out, rc);

	/**
	 * Optionally allocate a high-priority NRS head.
	 */
	if (svcpt->scp_service->srv_ops.so_hpreq_handler == NULL)
		GOTO(out, rc);

	OBD_CPT_ALLOC_PTR(svcpt->scp_nrs_hp,
			  svcpt->scp_service->srv_cptable,
			  svcpt->scp_cpt);
	if (svcpt->scp_nrs_hp == NULL)
		GOTO(out, rc = -ENOMEM);

	nrs = nrs_svcpt2nrs(svcpt, true);
	rc = svcpt_nrs_setup0(nrs, svcpt);

out:
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
	bool				hp = false;
	ENTRY;

again:
	nrs = nrs_svcpt2nrs(svcpt, hp);
	nrs->nrs_stopping = 1;

	cfs_list_for_each_entry_safe(policy, tmp, &nrs->nrs_policy_list,
				     pol_list) {
		rc = nrs_policy_unregister(nrs, policy->pol_name);
		LASSERT(rc == 0);
	}
	cfs_list_del(&nrs->nrs_heads);

	/**
	 * If the service partition has an HP NRS head, clean that up as well.
	 */
	if (!hp && nrs_svcpt_has_hp(svcpt)) {
		hp = true;
		goto again;
	}

	if (hp)
		OBD_FREE_PTR(nrs);

	EXIT;
}

/**
 * Checks whether the policy in \a desc has been added to NRS core's list of
 * policies, \e nrs_core.nrs_policies.
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

	cfs_list_for_each_entry(tmp, &nrs_core.nrs_policies, pd_list) {
		if (strcmp(tmp->pd_name, desc->pd_name) == 0)
			return true;
	}
	return false;
}

/**
 * Removes the policy from all supported NRS heads.
 *
 * \param[in] desc The policy descriptor to unregister
 *
 * \retval -ve error
 * \retval  0  successfully unregistered policy on all supported NRS heads
 *
 * \pre mutex_is_locked(&nrs_core.nrs_mutex)
 */
static int
nrs_policy_unregister_locked(struct ptlrpc_nrs_pol_desc *desc)
{
	struct ptlrpc_nrs      *nrs;
	int			rc = 0;
	ENTRY;

	cfs_list_for_each_entry(nrs, &nrs_core.nrs_heads, nrs_heads) {
		if (!nrs_policy_compatible(nrs->nrs_svcpt->scp_service, desc)) {
			/**
			 * The policy may only have registered on compatible
			 * NRS heads.
			 */
			continue;
		}

		rc = nrs_policy_unregister(nrs, desc->pd_name);

		/**
		 * Ignore -ENOENT as the policy may not have registered
		 * successfully on all service partitions.
		 */
		if (rc == -ENOENT) {
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
	cfs_list_for_each_entry(nrs, &nrs_core.nrs_heads, nrs_heads) {
		if (!nrs_policy_compatible(nrs->nrs_svcpt->scp_service, desc))
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
        struct ptlrpc_service	       *svc;
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc;
	int				rc2;
	ENTRY;

	LASSERT(desc != NULL);
	LASSERT(desc->pd_name != NULL);

	desc->pd_name[NRS_POL_NAME_MAX - 1] = '\0';

	if (desc->pd_flags & (PTLRPC_NRS_FL_FALLBACK |
			      PTLRPC_NRS_FL_REG_START)) {
		CERROR("Failing to register NRS policy %s; re-check policy "
		       "flags, externally-registered policies cannot act as "
		       "fallback policies or be started immediately without "
		       "interaction with lprocfs.\n", desc->pd_name);
		RETURN(-EINVAL);
	}

	desc->pd_flags |= PTLRPC_NRS_FL_REG_EXTERN;

	mutex_lock(&nrs_core.nrs_mutex);

	rc = nrs_policy_exists_locked(desc);
	if (rc) {
		CERROR("Failing to register NRS policy %s which has "
		       "already been registered with NRS core!\n",
		       desc->pd_name);
		GOTO(fail, rc = -EEXIST);
	}

	/**
	 * Register the new policy on all compatible services
	 * XXX: Perhaps going through the ptlrpc_all_services list here is not
	 * ideal.
	 */
	spin_lock(&ptlrpc_all_services_lock);

	cfs_list_for_each_entry(svc, &ptlrpc_all_services, srv_list) {
		if (!nrs_policy_compatible(svc, desc)) {
			/**
			 * Attempt to register the policy if it is
			 * compatible, otherwise try the next service.
			 */
			continue;
		}
		ptlrpc_service_for_each_part(svcpt, i, svc) {
			bool hp = false;

again:
			nrs = nrs_svcpt2nrs(svcpt, hp);
			rc = nrs_policy_register(nrs, desc, true);
			if (rc != 0) {
				CERROR("Failed to register NRS policy %s for "
				       "partition %d of service %s: %d\n",
				       desc->pd_name, nrs->nrs_svcpt->scp_cpt,
				       nrs->nrs_svcpt->scp_service->srv_name,
				       rc);

				rc2 = nrs_policy_unregister_locked(desc);
				/**
				 * Should not fail at this point
				 */
				LASSERT(rc2 == 0);
				spin_unlock(&ptlrpc_all_services_lock);
				GOTO(fail, rc);
			}

			if (!hp && nrs_svc_has_hp(svc)) {
				hp = true;
				goto again;
			}
		}
		if (desc->pd_ops->op_lprocfs_init != NULL) {
			rc = desc->pd_ops->op_lprocfs_init(svc);
			if (rc != 0) {
				rc2 = nrs_policy_unregister_locked(desc);
				/**
				 * Should not fail at this point
				 */
				LASSERT(rc2 == 0);
				spin_unlock(&ptlrpc_all_services_lock);
				GOTO(fail, rc);
			}
		}
	}

	spin_unlock(&ptlrpc_all_services_lock);

	/**
	 * The policy has successfully registered with all service partitions,
	 * so mark the policy instances at the NRS heads as available.
	 */
	nrs_pol_make_available_locked(desc);

	cfs_list_add_tail(&desc->pd_list, &nrs_core.nrs_policies);
fail:
	mutex_unlock(&nrs_core.nrs_mutex);

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
	int		       rc;
	struct ptlrpc_service *svc;
	ENTRY;

	LASSERT(desc != NULL);

	if (desc->pd_flags & PTLRPC_NRS_FL_FALLBACK) {
		CERROR("Unable to unregister a fallback policy, unless the "
		       "PTLRPC service is stopping.\n");
		RETURN(-EPERM);
	}

	desc->pd_name[NRS_POL_NAME_MAX - 1] = '\0';

	mutex_lock(&nrs_core.nrs_mutex);

	rc = nrs_policy_exists_locked(desc);
	if (!rc) {
		CERROR("Failing to unregister NRS policy %s which has "
		       "not been registered with NRS core!\n",
		       desc->pd_name);
		GOTO(fail, rc = -ENOENT);
	}

	rc = nrs_policy_unregister_locked(desc);
	if (rc == -EBUSY) {
		CERROR("Please first stop policy %s on all service partitions "
		       "and then retry to unregister the policy.\n",
		       desc->pd_name);
		GOTO(fail, rc);
	}
	CDEBUG(D_INFO, "Unregistering policy %s from NRS core.\n",
	       desc->pd_name);

	cfs_list_del(&desc->pd_list);

	/**
	 * Unregister the policy's lprocfs interface from all compatible
	 * services.
	 * XXX: Perhaps going through the ptlrpc_all_services list here is not
	 * ideal.
	 */
	spin_lock(&ptlrpc_all_services_lock);

	cfs_list_for_each_entry(svc, &ptlrpc_all_services, srv_list) {
		if (!nrs_policy_compatible(svc, desc))
			continue;

		if (desc->pd_ops->op_lprocfs_fini != NULL)
			desc->pd_ops->op_lprocfs_fini(svc);
	}

	spin_unlock(&ptlrpc_all_services_lock);

fail:
	mutex_unlock(&nrs_core.nrs_mutex);

	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_nrs_policy_unregister);

/**
 * Setup NRS heads on all service partitions of service \a svc, and register
 * all compatible policies on those NRS heads.
 *
 * \param[in] svc  The service to setup
 *
 * \retval -ve error, the calling logic should eventually call
 *		      ptlrpc_service_nrs_cleanup() to undo any work performed
 *		      by this function.
 *
 * \see ptlrpc_register_service()
 * \see ptlrpc_service_nrs_cleanup()
 */
int
ptlrpc_service_nrs_setup(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part     *svcpt;
	struct ptlrpc_nrs_pol_desc     *desc;
	int				i;
	int				rc = 0;

	mutex_lock(&nrs_core.nrs_mutex);

	/**
	 * Initialize NRS heads on all service CPTs.
	 */
	ptlrpc_service_for_each_part(svcpt, i, svc) {
		rc = ptlrpc_svcpt_nrs_setup(svcpt);
		if (rc != 0)
			GOTO(failed, rc);
	}

	/*
	 * Set up lprocfs interfaces for all supported policies for the
	 * service.
	 */
	cfs_list_for_each_entry(desc, &nrs_core.nrs_policies, pd_list) {
		if (!nrs_policy_compatible(svc, desc))
			continue;

		if (desc->pd_ops->op_lprocfs_init != NULL) {
			rc = desc->pd_ops->op_lprocfs_init(svc);
			if (rc != 0)
				break;
		}
	}

failed:

	mutex_unlock(&nrs_core.nrs_mutex);

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
	struct ptlrpc_nrs_pol_desc     *desc;
	int				i;

	mutex_lock(&nrs_core.nrs_mutex);

	/* Clean up NRS heads on all service partitions */
	ptlrpc_service_for_each_part(svcpt, i, svc)
		ptlrpc_svcpt_nrs_cleanup(svcpt);

	/**
	 * Clean up lprocfs interfaces for all supported policies for the
	 * service.
	 */
	cfs_list_for_each_entry(desc, &nrs_core.nrs_policies, pd_list) {
		if (!nrs_policy_compatible(svc, desc))
			continue;

		if (desc->pd_ops->op_lprocfs_fini != NULL)
			desc->pd_ops->op_lprocfs_fini(svc);
	}

	mutex_unlock(&nrs_core.nrs_mutex);
}

/**
 * Obtains NRS head resources for request \a req.
 *
 * These could be either on the regular or HP NRS head of \a svcpt; resources
 * taken on the regular head can later be swapped for HP head resources by
 * ldlm_lock_reorder_req().
 *
 * \param[in] svcpt The service partition
 * \param[in] req   The request
 * \param[in] hp    Which NRS head of \a svcpt to use
 */
void
ptlrpc_nrs_req_initialize(struct ptlrpc_service_part *svcpt,
			  struct ptlrpc_request *req, bool hp)
{
	struct ptlrpc_nrs	*nrs = nrs_svcpt2nrs(svcpt, hp);

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
		 * If there are other policies with queued requests, move the
		 * current policy to the end so that we can round robin over
		 * all policies and drain the requests.
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
	 * XXX: Maybe want to remove nrs_resource[get|put]_safe() dance
	 * when request cannot actually move; move this further down?
	 */
	nrs_resource_get_safe(nrs, nrq, res1, true);

	spin_lock(&svcpt->scp_req_lock);

	if (!ptlrpc_nrs_req_can_move(req))
		goto out;

	ptlrpc_nrs_req_del_nolock(req);
	nrq->nr_enqueued = nrq->nr_dequeued = 0;

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	ptlrpc_nrs_hpreq_add_nolock(req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));
 out:
	spin_unlock(&svcpt->scp_req_lock);

	/**
	 * Release either the regular NRS head resources if we moved the
	 * request, or the high-priority NRS head resources if we took a
	 * reference earlier in this function and ptlrpc_nrs_req_can_move()
	 * returned false.
	 */
	nrs_resource_put_safe(res1);
	EXIT;
}

/**
 * Carries out a control operation \a opc on the policy identified by the
 * human-readable \a name, on all service partitions of service \a svc..
 *
 * \param[in]	  svc	 The service the policy belongs to.
 * \param[in]	  queue  Whether to carry out the command on the policy which
 *			 belongs to the regular, high-priority, or both NRS
 *			 heads of service partitions of \a svc.
 * \param[in]	  name   The policy to act upon, by human-readable name
 * \param[in]	  opc	 The opcode of the operation to carry out
 * \param[in]	  single When set, the operation will only be carried out on the
 *			 NRS heads of the first service partition of \a svc.
 *			 This is useful for some policies which e.g. share
 *			 identical values on the same parameters of different
 *			 service partitions; when reading these parameters via
 *			 lprocfs, these policies may just want to obtain and
 *			 print out the values from the first service partition.
 *			 Storing these values centrally elsewhere then could be
 *			 another solution for this, but it does not fit well
 *			 with the NRS locking model.
 * \param[in,out] arg	 Can be used as a generic in/out buffer between control
 *			 operations and the user environment.
 *
 *\retval -ve error condition
 *\retval   0 operation was carried out successfully
 */
int
ptlrpc_nrs_policy_control(struct ptlrpc_service *svc,
			  enum ptlrpc_nrs_queue_type queue, char *name,
			  enum ptlrpc_nrs_ctl opc, bool single, void *arg)
{
	struct ptlrpc_service_part     *svcpt;
	int				i;
	int				rc = 0;
	ENTRY;

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		switch (queue) {
		default:
			return -EINVAL;

		case PTLRPC_NRS_QUEUE_BOTH:
		case PTLRPC_NRS_QUEUE_REG:
			rc = nrs_policy_ctl(nrs_svcpt2nrs(svcpt, false), name,
					    opc, arg);
			if (rc != 0)
				GOTO(out, rc);

			if (queue == PTLRPC_NRS_QUEUE_REG)
				break;
			/* fallthrough */

		case PTLRPC_NRS_QUEUE_HP:
			/**
			 * XXX: We could optionally check for
			 * nrs_svc_has_hp(svc) here, and return an error if it
			 * is false. Right now we rely on the policies' lprocfs
			 * handlers that call the present function to make this
			 * check; if they fail to do so, they might hit the
			 * assertion inside nrs_svcpt2nrs() below.
			 */
			rc = nrs_policy_ctl(nrs_svcpt2nrs(svcpt, true), name,
					    opc, arg);
			if (rc != 0 || single)
				GOTO(out, rc);

			break;
		}
	}
out:
	RETURN(rc);
}

/**
 * Adds all policies that ship with NRS, i.e. those in the \e nrs_pols_builtin
 * array, to NRS core's list of policies \e nrs_core.nrs_policies.
 *
 * \retval 0 All policy descriptors in \e nrs_pols_builtin have been added
 *	     successfully to \e nrs_core.nrs_policies
 */
int
ptlrpc_nrs_init(void)
{
	int	rc = -EINVAL;
	int	i;
	ENTRY;

	/**
	 * Initialize the NRS core object.
	 */
	mutex_init(&nrs_core.nrs_mutex);
	CFS_INIT_LIST_HEAD(&nrs_core.nrs_heads);
	CFS_INIT_LIST_HEAD(&nrs_core.nrs_policies);

	for (i = 0; i < ARRAY_SIZE(nrs_pols_builtin); i++) {
		/**
		 * No need to take nrs_core.nrs_mutex as there is no contention at
		 * this early stage.
		 */
		rc = nrs_policy_exists_locked(nrs_pols_builtin[i]);
		/**
		 * This should not fail for in-tree policies.
		 */
		LASSERT(rc == false);
		cfs_list_add_tail(&nrs_pols_builtin[i]->pd_list,
				  &nrs_core.nrs_policies);
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

/** @} nrs */
