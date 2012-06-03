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
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
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
#include <lustre_disk.h>
#include <lprocfs_status.h>
#include "ptlrpc_internal.h"

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

	policy->pol_state = NRS_POLICY_STATE_STOPPED;
}

static int
nrs_policy_stop_locked(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs *nrs = policy->pol_nrs;

	if (nrs->nrs_policy_fallback == policy && !nrs->nrs_stopping)
		return -EPERM; /* not allowed */

	if (policy->pol_state == NRS_POLICY_STATE_STARTING)
		return -EAGAIN; /* retry later */

	if (policy->pol_state != NRS_POLICY_STATE_STARTED)
		return 0; /* in progress or already stopped */

	policy->pol_state = NRS_POLICY_STATE_STOPPING;

	/* immediately make it invisible */
	if (nrs->nrs_policy_primary == policy) {
		nrs->nrs_policy_primary = NULL;

	} else {
		LASSERT(nrs->nrs_policy_fallback == policy);
		nrs->nrs_policy_fallback = NULL;
	}

	if (policy->pol_ref == 1) /* I have the only refcount */
		nrs_policy_stop_final(policy);

	return 0;
}

static int
nrs_policy_start_locked(struct ptlrpc_nrs_policy *policy)
{
	struct ptlrpc_nrs      *nrs = policy->pol_nrs;
	int			rc = 0;

	/* don't allow multiple starting which is too complex */
	if (nrs->nrs_policy_starting)
		return -EAGAIN;

	LASSERT(policy->pol_state != NRS_POLICY_STATE_STARTING);

	if (policy->pol_state == NRS_POLICY_STATE_STARTED)
		return 0;

	if (policy->pol_state == NRS_POLICY_STATE_STOPPING)
		return -EAGAIN; /* retry later */

	if (policy->pol_flags & PTLRPC_NRS_FL_FALLBACK) {
		/* already have fallback policy? */
		if (nrs->nrs_policy_fallback != NULL)
			return -EPERM;
	} else {
		/* shouldn't start primary policy if w/o fallback policy */
		if (nrs->nrs_policy_fallback == NULL)
			return -EPERM;
	}

	/* serialize policy starting */
	nrs->nrs_policy_starting = 1;

	policy->pol_state = NRS_POLICY_STATE_STARTING;

	if (policy->pol_ops->op_policy_start) {
		cfs_spin_unlock(&nrs->nrs_lock);

		rc = policy->pol_ops->op_policy_start(policy);

		cfs_spin_lock(&nrs->nrs_lock);
		if (rc != 0) {
			policy->pol_state = NRS_POLICY_STATE_STOPPED;
			goto out;
		}
	}

	policy->pol_state = NRS_POLICY_STATE_STARTED;

	if (policy->pol_flags & PTLRPC_NRS_FL_FALLBACK) {
		LASSERT(nrs->nrs_policy_fallback == NULL); /* no change */
		nrs->nrs_policy_fallback = policy;

	} else {
		/* try to stop current primary if there is */
		if (nrs->nrs_policy_primary != NULL) {
			struct ptlrpc_nrs_policy *tmp = nrs->nrs_policy_primary;

			nrs->nrs_policy_primary = NULL;

			LASSERT(tmp->pol_state == NRS_POLICY_STATE_STARTED);
			tmp->pol_state = NRS_POLICY_STATE_STOPPING;

			if (tmp->pol_ref == 0)
				nrs_policy_stop_final(tmp);
		}

		nrs->nrs_policy_primary = policy;
	}

 out:
	nrs->nrs_policy_starting = 0;
	return rc;
}

static void
nrs_policy_get_locked(struct ptlrpc_nrs_policy *policy)
{
	policy->pol_ref++;
}

static struct ptlrpc_nrs_policy *
nrs_policy_find_locked(struct ptlrpc_nrs *nrs, int type)
{
	struct ptlrpc_nrs_policy *tmp;

	cfs_list_for_each_entry(tmp, &(nrs)->nrs_policy_list, pol_list) {
		if (type == tmp->pol_type) {
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
	    policy->pol_state == NRS_POLICY_STATE_STOPPING)
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

	/* not always have op_res_put */
	if (policy->pol_ops->op_res_put != NULL) {
		struct ptlrpc_nrs_resource *parent;

		for (; res != NULL; res = parent) {
			parent = res->res_parent;
			policy->pol_ops->op_res_put(policy, res);
		}
	}
}

static struct ptlrpc_nrs_resource *
nrs_resource_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_resource *res = NULL;
	struct ptlrpc_nrs_resource *tmp = NULL;
	int		       rc;

	while (1) {
		rc = policy->pol_ops->op_res_get(policy, nrq, res, &tmp);
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

static void
nrs_resource_get_safe(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_request *nrq,
		      struct ptlrpc_nrs_resource **resp)
{
	struct ptlrpc_nrs_policy   *primary	= NULL;
	struct ptlrpc_nrs_policy   *fallback = NULL;

	memset(resp, 0, sizeof(resp[0]) * NRS_RES_MAX);

	cfs_spin_lock(&nrs->nrs_lock);

	fallback = nrs->nrs_policy_fallback;
	nrs_policy_get_locked(fallback);

	primary = nrs->nrs_policy_primary;
	if (primary != NULL)
		nrs_policy_get_locked(primary);

	cfs_spin_unlock(&nrs->nrs_lock);

	resp[NRS_RES_FALLBACK] = nrs_resource_get(fallback, nrq);
	LASSERT(resp[NRS_RES_FALLBACK] != NULL);

	if (primary != NULL) {
		resp[NRS_RES_PRIMARY] = nrs_resource_get(primary, nrq);
		/* A primary policy may exist which may not wish to serve a
		 * particular request for different reasons; see
		 * nrs_orr_req_supported() for an example of this; release the
		 * reference on the policy as it will not be used for this
		 * request */
		if (resp[NRS_RES_PRIMARY] == NULL)
			nrs_policy_put(primary);
	}

	return;
}

static void
nrs_resource_put_safe(struct ptlrpc_nrs_resource **resp)
{
	struct ptlrpc_nrs_policy       *pols[NRS_RES_MAX];
	struct ptlrpc_nrs	       *nrs = NULL;
	int				i;

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
			cfs_spin_lock(&nrs->nrs_lock);
		}
		nrs_policy_put_locked(pols[i]);
	}

	if (nrs != NULL)
		cfs_spin_unlock(&nrs->nrs_lock);
}

static struct ptlrpc_nrs_resource *
nrs_request_resource(struct ptlrpc_nrs_request *nrq)
{
	LASSERT(nrq->nr_initialized);
	LASSERT(!nrq->nr_finalized);

	return nrq->nr_res_ptrs[nrq->nr_res_idx];
}

static struct ptlrpc_nrs_policy *
nrs_request_policy(struct ptlrpc_nrs_request *nrq)
{
	return nrs_request_resource(nrq)->res_policy;
}

static struct ptlrpc_nrs_request *
nrs_request_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct ptlrpc_nrs_request *nrq;

	if (policy->pol_req_queued == 0)
		return NULL;

	nrq = policy->pol_ops->op_req_poll(policy, arg);

	LASSERT(nrq != NULL);
	LASSERT(nrs_request_policy(nrq) == policy);

	return nrq;
}

static void
nrs_request_enqueue(struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_nrs_policy *policy;
	int rc = -1;
	int i;

	/* try in descending order, because primary policy is
	 * the preferred choice */
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
	LBUG(); /* should never be here */
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
nrs_policy_ctl(struct ptlrpc_nrs *nrs, enum ptlrpc_nrs_pol_type type,
	       enum ptlrpc_nrs_ctl opc, void *arg)
{
	struct ptlrpc_nrs_policy       *policy;
	struct ptlrpc_nrs_policy_info  *info;
	int				rc = 0;

	cfs_spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, type);
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

		info = (struct ptlrpc_nrs_policy_info *)arg;

		memcpy(info->pi_name, policy->pol_name, NRS_POL_NAME_MAX);
		info->pi_fallback    = !!(policy->pol_flags &
					  PTLRPC_NRS_FL_FALLBACK);
		info->pi_state	     = policy->pol_state;
		info->pi_req_queued  = policy->pol_req_queued;
		info->pi_req_started = policy->pol_req_started;

		break;

	case PTLRPC_NRS_CTL_STOP:
		rc = nrs_policy_stop_locked(policy);
		break;

	case PTLRPC_NRS_CTL_START:
		rc = nrs_policy_start_locked(policy);
		break;

	case PTLRPC_NRS_CTL_ORR_RD_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;

		cfs_read_lock(&orrd->od_lock);
		if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG)
			info->oi_quantum_reg = orrd->od_quantum;
		else if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_HP)
			info->oi_quantum_hp = orrd->od_quantum;
		else
			LBUG();
		cfs_read_unlock(&orrd->od_lock);
		}
		break;

	case PTLRPC_NRS_CTL_ORR_WR_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		cfs_write_lock(&orrd->od_lock);
		orrd->od_quantum = *(__u16 *)arg;
		cfs_write_unlock(&orrd->od_lock);
		}
		break;

	case PTLRPC_NRS_CTL_ORR_RD_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;

		cfs_read_lock(&orrd->od_lock);
		if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG)
			info->oi_physical_reg = orrd->od_physical;
		else if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_HP)
			info->oi_physical_hp = orrd->od_physical;
		cfs_read_unlock(&orrd->od_lock);
		}
		break;

	case PTLRPC_NRS_CTL_ORR_WR_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		char			*off_type = arg;

		cfs_write_lock(&orrd->od_lock);
		if (!strcmp(off_type, "physical"))
			orrd->od_physical = 1;
		else if (!strcmp(off_type, "logical"))
			orrd->od_physical = 0;
		else
			LBUG();
		cfs_write_unlock(&orrd->od_lock);
		}
		break;

	case PTLRPC_NRS_CTL_ORR_RD_SUPP_REQ: {
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;
		struct nrs_orr_data	*orrd = policy->pol_private;

		cfs_read_lock(&orrd->od_lock);
		if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG)
			info->oi_physical_reg = orrd->od_supp;
		else if (nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_HP)
			info->oi_physical_hp = orrd->od_supp;
		else
			LBUG();
		cfs_read_unlock(&orrd->od_lock);
		}
		break;

	case PTLRPC_NRS_CTL_ORR_WR_SUPP_REQ: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		char			*supp = arg;

		cfs_write_lock(&orrd->od_lock);
		if (!strcmp(supp, "read"))
			orrd->od_supp = NOS_OST_READ;
		else if (!strcmp(supp, "write"))
			orrd->od_supp = NOS_OST_WRITE;
		else if (!strcmp(supp, "readwrite"))
			orrd->od_supp = NOS_OST_RW;
		else
			rc = -EINVAL;
		cfs_write_unlock(&orrd->od_lock);
		}
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
nrs_policy_unregister(struct ptlrpc_nrs *nrs, int type)
{
	struct ptlrpc_nrs_policy *policy = NULL;

	cfs_spin_lock(&nrs->nrs_lock);

	policy = nrs_policy_find_locked(nrs, type);
	if (policy == NULL) {
		cfs_spin_unlock(&nrs->nrs_lock);

		CERROR("Can't find NRS type %d\n", type);
		return -ENOENT;
	}

	if (policy->pol_ref > 1) {
		cfs_spin_unlock(&nrs->nrs_lock);

		CERROR("Policy is busy %d\n", (int)policy->pol_ref);
		nrs_policy_put(policy);

		return -EBUSY;
	}

	LASSERT(policy->pol_req_queued == 0);
	LASSERT(policy->pol_req_started == 0);

	if (policy->pol_state != NRS_POLICY_STATE_STOPPED) {
		nrs_policy_stop_locked(policy);
		LASSERT(policy->pol_state == NRS_POLICY_STATE_STOPPED);
	}

	cfs_list_del(&policy->pol_list);
	nrs_policy_put_locked(policy);

	cfs_spin_unlock(&nrs->nrs_lock);

	nrs_policy_fini(policy);

	LASSERT(policy->pol_private == NULL);
	OBD_FREE_PTR(policy);

	return 0;
}

static int
nrs_policy_register(struct ptlrpc_nrs *nrs, struct ptlrpc_nrs_policy_desc *desc,
		    unsigned int flags, void *arg)
{
	struct ptlrpc_nrs_policy *policy;
	struct ptlrpc_nrs_policy *tmp;
	int			  rc;

	LASSERT(desc->pd_ops != NULL);
	LASSERT(desc->pd_ops->op_res_get != NULL);
	LASSERT(desc->pd_ops->op_req_poll != NULL);
	LASSERT(desc->pd_ops->op_req_enqueue != NULL);
	LASSERT(desc->pd_ops->op_req_dequeue != NULL);

	/* should have default policy */
	OBD_ALLOC_PTR(policy);
	if (policy == NULL)
		return -ENOMEM;

	policy->pol_nrs     = nrs;
	policy->pol_name    = desc->pd_name;
	policy->pol_type    = desc->pd_type;
	policy->pol_ops     = desc->pd_ops;
	policy->pol_state   = NRS_POLICY_STATE_STOPPED;
	policy->pol_flags   = flags;

	if (flags & PTLRPC_NRS_FL_FALLBACK)
		policy->pol_flags |= PTLRPC_NRS_FL_REG_START;

	CFS_INIT_LIST_HEAD(&policy->pol_list);
	CFS_INIT_LIST_HEAD(&policy->pol_list_queued);

	rc = nrs_policy_init(policy, arg);
	if (rc != 0) {
		OBD_FREE_PTR(policy);
		return rc;
	}

	cfs_spin_lock(&nrs->nrs_lock);

	tmp = nrs_policy_find_locked(nrs, policy->pol_type);
	if (tmp != NULL) {
		cfs_spin_unlock(&nrs->nrs_lock);

		CERROR("NRS type %d (%s) has been registered, can't "
		       "register it for %s\n",
		       tmp->pol_type, tmp->pol_name, policy->pol_name);

		nrs_policy_put(tmp);
		nrs_policy_fini(policy);
		OBD_FREE_PTR(policy);

		return -EEXIST;
	}

	cfs_list_add(&policy->pol_list, &nrs->nrs_policy_list);
	if (policy->pol_flags & PTLRPC_NRS_FL_REG_START)
		rc = nrs_policy_start_locked(policy);

	cfs_spin_unlock(&nrs->nrs_lock);

	if (rc != 0)
		nrs_policy_unregister(nrs, policy->pol_type);

	return rc;
}

static void
ptlrpc_nrs_req_add_nolock(struct ptlrpc_service *svc,
			  struct ptlrpc_request *req)
{
	struct ptlrpc_nrs_policy *policy;

	/* NB: must call with hold svc::srv_rq_lock */
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
 * Make the request a high priority one.
 *
 * All the high priority requests are queued in a separate FIFO
 * ptlrpc_service::srv_request_hpq list which is parallel to
 * ptlrpc_service::srv_request_queue list but has a higher priority
 * for handling.
 *
 * \see ptlrpc_server_handle_request().
 */
static void
ptlrpc_nrs_hpreq_add_nolock(struct ptlrpc_service *svc,
			    struct ptlrpc_request *req)
{
	ENTRY;
	LASSERT(svc != NULL);
	cfs_spin_lock(&req->rq_lock);
	if (req->rq_hp == 0) {
		int opc = lustre_msg_get_opc(req->rq_reqmsg);

		/* Add to the high priority queue. */
		req->rq_hp = 1;
		ptlrpc_nrs_req_add_nolock(svc, req);
		if (opc != OBD_PING)
			DEBUG_REQ(D_NET, req, "high priority req");
	}
	cfs_spin_unlock(&req->rq_lock);
	EXIT;
}

struct ptlrpc_request *
ptlrpc_nrs_req_poll_nolock(struct ptlrpc_service *svc, __u8 hp)
{
	struct ptlrpc_nrs	  *nrs = ptlrpc_server_nrs(svc, hp);
	struct ptlrpc_nrs_policy  *policy;
	struct ptlrpc_nrs_request *nrq;

	if (nrs->nrs_req_queued == 0)
		return NULL;

	/* NB: must call with hold svc::srv_rq_lock */
	/* always try to drain requests from all NRS polices even they are
	 * inactive, because user can change policy status at runtime */
	cfs_list_for_each_entry(policy, &(nrs)->nrs_policy_queued,
				pol_list_queued) {
		nrq = nrs_request_poll(policy, NULL);
		if (nrq != NULL)
			return container_of(nrq, struct ptlrpc_request, rq_nrq);
	}
	return NULL;
}

static int
ptlrpc_nrs_policy_register(struct ptlrpc_service *svc,
			   enum ptlrpc_nrs_queue_type queue,
			   struct ptlrpc_nrs_policy_desc *desc,
			   unsigned long flags, void *arg)
{
	int rc;

	switch (queue) {
	default:
		return -EINVAL;

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		rc = nrs_policy_register(ptlrpc_server_nrs(svc, 0),
					 desc, flags, arg);
		if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_register(ptlrpc_server_nrs(svc, 1),
					 desc, flags, arg);
		break;
	}

	return rc;
}

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_fifo_desc;
static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_crr_desc;
static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_crrn_desc;
static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_orr_desc;
static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_trr_desc;

/* TODO: Organize this better? */
static int
ptlrpc_nrs_register_policies(struct ptlrpc_service *svc,
			     enum ptlrpc_nrs_queue_type queue)
{
	int rc;

	if (svc->srv_nrs_supported & PTLRPC_NRS_TYPE_FIFO) {
		rc = ptlrpc_nrs_policy_register(svc, queue,
						&ptlrpc_nrs_fifo_desc,
						PTLRPC_NRS_FL_FALLBACK |
						PTLRPC_NRS_FL_REG_START,
						NULL);
		if (rc != 0)
			GOTO(failed, rc);
	}

	if (svc->srv_nrs_supported & PTLRPC_NRS_TYPE_CRR) {
		rc = ptlrpc_nrs_policy_register(svc, queue,
						&ptlrpc_nrs_crr_desc, 0, NULL);
		if (rc != 0)
			GOTO(failed, rc);
	}

	if (svc->srv_nrs_supported & PTLRPC_NRS_TYPE_CRRN) {
		rc = ptlrpc_nrs_policy_register(svc, queue,
						&ptlrpc_nrs_crrn_desc, 0, NULL);
		if (rc != 0)
			GOTO(failed, rc);
	}

	if (svc->srv_nrs_supported & PTLRPC_NRS_TYPE_ORR) {
		rc = ptlrpc_nrs_policy_register(svc, queue,
						&ptlrpc_nrs_orr_desc, 0, NULL);
		if (rc != 0)
			GOTO(failed, rc);
	}
	if (svc->srv_nrs_supported & PTLRPC_NRS_TYPE_TRR) {
		rc = ptlrpc_nrs_policy_register(svc, queue,
						&ptlrpc_nrs_trr_desc, 0, NULL);
		/* Redundant */
		if (rc != 0)
			GOTO(failed, rc);
	}
failed:
	return rc;
}

static int
ptlrpc_nrs_policy_unregister(struct ptlrpc_service *svc,
			     enum ptlrpc_nrs_queue_type queue, int type)
{
	int	rc = 0;

	switch (queue) {
	default:
		return -EINVAL;

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		rc = nrs_policy_unregister(ptlrpc_server_nrs(svc, 0), type);
		if (queue == PTLRPC_NRS_QUEUE_REG || rc != 0)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_unregister(ptlrpc_server_nrs(svc, 1), type);
		break;
	}

	return rc;
}

/** Exported API */

int
ptlrpc_server_nrs_setup(struct ptlrpc_service *svc,
			enum ptlrpc_nrs_queue_type queue)
{
	struct ptlrpc_nrs      *nrs;
	int			rc;

	switch (queue) {
	default:
		return -EINVAL;

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		nrs = ptlrpc_server_nrs(svc, 0);
		nrs->nrs_service = svc;
		nrs->nrs_queue_type = PTLRPC_NRS_QUEUE_REG;
		cfs_spin_lock_init(&nrs->nrs_lock);
		CFS_INIT_LIST_HEAD(&nrs->nrs_policy_list);
		CFS_INIT_LIST_HEAD(&nrs->nrs_policy_queued);
		/* Optionally fall-through */
		if (queue == PTLRPC_NRS_QUEUE_REG)
			break;
	case PTLRPC_NRS_QUEUE_HP:
		OBD_ALLOC_PTR(svc->srv_hpreq_nrs);
		if (svc->srv_hpreq_nrs == NULL)
			RETURN(-ENOMEM);
		nrs = ptlrpc_server_nrs(svc, 1);
		nrs->nrs_service = svc;
		nrs->nrs_queue_type = PTLRPC_NRS_QUEUE_HP;
		cfs_spin_lock_init(&nrs->nrs_lock);
		CFS_INIT_LIST_HEAD(&nrs->nrs_policy_list);
		CFS_INIT_LIST_HEAD(&nrs->nrs_policy_queued);
		/* Redundant */
		break;
	}
	/* Register NRS policies; this uses \a ptlrpc_service::srv_nrs_supported
	 * so should happen after it has been set */
	rc = ptlrpc_nrs_register_policies(svc, queue);
	RETURN(rc);
}

void
ptlrpc_server_nrs_cleanup(struct ptlrpc_service *svc)
{
	struct ptlrpc_nrs	 *nrs;
	struct ptlrpc_nrs_policy *policy;
	int			  rc;

	nrs = ptlrpc_server_nrs(svc, 0);
	nrs->nrs_stopping = 1;
	while (!cfs_list_empty(&nrs->nrs_policy_list)) {
		policy = cfs_list_entry(nrs->nrs_policy_list.next,
					struct ptlrpc_nrs_policy, pol_list);
		rc = ptlrpc_nrs_policy_unregister(svc, PTLRPC_NRS_QUEUE_REG,
						  policy->pol_type);
		LASSERT(rc == 0);
	}

	if (ptlrpc_nrs_svc_has_hp(svc)) {
		nrs = ptlrpc_server_nrs(svc, 1);
		nrs->nrs_stopping = 1;
		while (!cfs_list_empty(&nrs->nrs_policy_list)) {
			policy = cfs_list_entry(nrs->nrs_policy_list.next,
						struct ptlrpc_nrs_policy,
						pol_list);
			rc = ptlrpc_nrs_policy_unregister(svc,
							  PTLRPC_NRS_QUEUE_HP,
							  policy->pol_type);
			LASSERT(rc == 0);
		}
		OBD_FREE_PTR(svc->srv_hpreq_nrs);
	}
}

void
ptlrpc_nrs_req_initialize(struct ptlrpc_service *svc,
			  struct ptlrpc_request *req)
{
	memset(&req->rq_nrq, 0, sizeof(req->rq_nrq));
	nrs_resource_get_safe(ptlrpc_server_nrs(svc, req->rq_hp),
			      &req->rq_nrq, req->rq_nrq.nr_res_ptrs);
	/* no protection on bit nr_initialized because no contention
	 * at this early stage */
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

int
ptlrpc_nrs_req_pending_nolock(struct ptlrpc_service *svc, __u8 hp)
{
	struct ptlrpc_nrs *nrs = ptlrpc_server_nrs(svc, hp);

	/* NB: can be called w/o any lock */
	return nrs->nrs_req_queued > 0;
};

void
ptlrpc_nrs_req_del_nolock(struct ptlrpc_request *req)
{
	struct ptlrpc_nrs_policy  *policy;

	/* NB: must call with hold svc::srv_rq_lock */
	LASSERT(req->rq_nrq.nr_enqueued);

	policy = nrs_request_policy(&req->rq_nrq);
	nrs_request_dequeue(&req->rq_nrq);
	req->rq_nrq.nr_dequeued = 1;

	cfs_list_del_init(&policy->pol_list_queued);
	/* any pending request on other polices? */
	if (policy->pol_req_queued != 0) {
		/* move current policy to the end so we can round-robin
		 * over all polices and drain requests */
		cfs_list_add_tail(&policy->pol_list_queued,
				  &policy->pol_nrs->nrs_policy_queued);
	}
}

void
ptlrpc_nrs_req_add(struct ptlrpc_service *svc,
		   struct ptlrpc_request *req, __u8 hp)
{
	struct ptlrpc_nrs_request *nrq = &req->rq_nrq;

	/* TODO:This locking can occur a bit later probably inside
	 * ptlrpc_nrs_req_add_nolock(), not quite needed at this point */
	cfs_spin_lock(&svc->srv_rq_lock);
	/* Before inserting the request into the queue, check if it is not
	 * inserted yet, or even already handled -- it may happen due to
	 * a racing ldlm_server_blocking_ast(). */
	if (!nrq->nr_enqueued) {
		if (hp)
			ptlrpc_nrs_hpreq_add_nolock(svc, req);
		else
			ptlrpc_nrs_req_add_nolock(svc, req);
	}

	cfs_spin_unlock(&svc->srv_rq_lock);
}

void
ptlrpc_nrs_req_prioritize(struct ptlrpc_request *req)
{
	struct ptlrpc_service		*svc = req->rq_rqbd->rqbd_service;
	struct ptlrpc_nrs		*nrs = ptlrpc_server_nrs(svc, 1);
	struct ptlrpc_nrs_request	*nrq = &req->rq_nrq;
	struct ptlrpc_nrs_resource	*res1[NRS_RES_MAX];
	struct ptlrpc_nrs_resource	*res2[NRS_RES_MAX];

	if (nrq->nr_started)
		return;

	nrs_resource_get_safe(nrs, nrq, res1);

	cfs_spin_lock(&svc->srv_rq_lock);

	if (nrq->nr_started ||
	    (nrq->nr_enqueued && nrs_request_policy(nrq)->pol_nrs == nrs))
		goto out;

	if (nrq->nr_enqueued) {
		ptlrpc_nrs_req_del_nolock(req);
		nrq->nr_enqueued = nrq->nr_dequeued = 0;
	}

	memcpy(res2, nrq->nr_res_ptrs, NRS_RES_MAX * sizeof(res2[0]));
	memcpy(nrq->nr_res_ptrs, res1, NRS_RES_MAX * sizeof(res1[0]));

	ptlrpc_nrs_hpreq_add_nolock(svc, req);

	memcpy(res1, res2, NRS_RES_MAX * sizeof(res1[0]));
 out:
	cfs_spin_unlock(&svc->srv_rq_lock);

	nrs_resource_put_safe(res1);
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

int
ptlrpc_nrs_policy_control(struct ptlrpc_service *svc,
			  enum ptlrpc_nrs_queue_type queue,
			  enum ptlrpc_nrs_pol_type type,
			  enum ptlrpc_nrs_ctl opc, void *arg)
{
	int	rc;

	switch (queue) {
	default:
		return -EINVAL;

	case PTLRPC_NRS_QUEUE_BOTH:
	case PTLRPC_NRS_QUEUE_REG:
		rc = nrs_policy_ctl(ptlrpc_server_nrs(svc, 0), type, opc, arg);
		if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
			break;

	case PTLRPC_NRS_QUEUE_HP:
		rc = nrs_policy_ctl(ptlrpc_server_nrs(svc, 1), type, opc, arg);
		break;
	}

	return rc;
}

/** NRS policies */

/*
 * FIFO policy
 */

static int
nrs_fifo_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_fifo_head *head;

	OBD_ALLOC_PTR(head);
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
	policy->pol_private = NULL;
}

static int
nrs_fifo_res_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq,
		 struct ptlrpc_nrs_resource *parent,
		 struct ptlrpc_nrs_resource **resp)
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

static int
nrs_fifo_req_add(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_fifo_head *head;

	head = container_of(nrs_request_resource(nrq), struct nrs_fifo_head,
			    fh_res);
	/* they are for debug */
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
	.op_policy_init		= NULL,
	.op_policy_fini		= NULL,
	.op_policy_start	= nrs_fifo_start,
	.op_policy_stop		= nrs_fifo_stop,
	.op_policy_ctl		= NULL, /* not used */
	.op_res_get		= nrs_fifo_res_get,
	.op_res_put		= NULL, /* not used */
	.op_req_poll		= nrs_fifo_req_poll,
	.op_req_enqueue		= nrs_fifo_req_add,
	.op_req_dequeue		= nrs_fifo_req_del,
	.op_req_start		= nrs_fifo_req_start,
	.op_req_stop		= nrs_fifo_req_stop,
};

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_fifo_desc = {
	.pd_name		= "fifo",
	.pd_type		= PTLRPC_NRS_TYPE_FIFO,
	.pd_ops			= &nrs_fifo_ops,
};

/*
 * CRR, Client Round Robin over exports
 */

static int
crr_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct ptlrpc_nrs_request *nrq1;
	struct ptlrpc_nrs_request *nrq2;

	nrq1 = container_of(e1, struct ptlrpc_nrs_request, nr_u.crr.cr_node);
	nrq2 = container_of(e2, struct ptlrpc_nrs_request, nr_u.crr.cr_node);

	if (nrq1->nr_u.crr.cr_round < nrq2->nr_u.crr.cr_round)
		return 1;
	else if (nrq1->nr_u.crr.cr_round > nrq2->nr_u.crr.cr_round)
		return 0;
	/* equal */
	if (nrq1->nr_u.crr.cr_sequence < nrq2->nr_u.crr.cr_sequence)
		return 1;
	else
		return 0;
}

static cfs_binheap_ops_t nrs_crr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= crr_req_compare,
};

static int
nrs_crr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crr_data	*cdat;

	OBD_ALLOC_PTR(cdat);
	if (cdat == NULL)
		return -ENOMEM;

	/* XXX we might want to move this allocation to op_policy_start()
	 * in the future FIXME */
	cdat->cd_binheap = cfs_binheap_create(&nrs_crr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL);
	if (cdat->cd_binheap == NULL) {
		OBD_FREE_PTR(cdat);
		return -ENOMEM;
	}
	policy->pol_private = cdat;
	return 0;
}

static void
nrs_crr_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crr_data *cdat = policy->pol_private;

	LASSERT(cdat != NULL);
	LASSERT(cfs_binheap_is_empty(cdat->cd_binheap));

	cfs_binheap_destroy(cdat->cd_binheap);
	OBD_FREE_PTR(cdat);

	policy->pol_private = NULL;
}

static int
nrs_crr_res_get(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq,
		struct ptlrpc_nrs_resource *parent,
		struct ptlrpc_nrs_resource **resp)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	if (req->rq_export == NULL)
		return -1;

	if (parent == NULL) {
		*resp = &((struct nrs_crr_data *)policy->pol_private)->cd_res;
		return 0;
	} else {
		*resp = req->rq_hp ? &req->rq_export->exp_nrs_res_hp.ce_res :
				     &req->rq_export->exp_nrs_res.ce_res;
		return 1; /* done */
	}
}

static struct ptlrpc_nrs_request *
nrs_crr_req_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct nrs_crr_data	*cdat = policy->pol_private;
	cfs_binheap_node_t *node = cfs_binheap_root(cdat->cd_binheap);

	return node == NULL ? NULL :
	       container_of(node, struct ptlrpc_nrs_request, nr_u.crr.cr_node);
}

static int
nrs_crr_req_add(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crr_data    *cdat;
	struct nrs_crr_export  *cexp;
	int			rc;

	cexp = container_of(nrs_request_resource(nrq),
			    struct nrs_crr_export, ce_res);
	cdat = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_crr_data, cd_res);

	if (cexp->ce_round < cdat->cd_round)
		cexp->ce_round = cdat->cd_round;

	nrq->nr_u.crr.cr_round = cexp->ce_round;
	nrq->nr_u.crr.cr_sequence = cdat->cd_sequence;

	rc = cfs_binheap_insert(cdat->cd_binheap, &nrq->nr_u.crr.cr_node);
	if (rc == 0) {
		cdat->cd_sequence++;
		cexp->ce_round++;
	}

	return rc;
}

static void
nrs_crr_req_del(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crr_data	*cdat;
	struct nrs_crr_export	*cexp;
	cfs_binheap_node_t	*node;

	cexp = container_of(nrs_request_resource(nrq),
			    struct nrs_crr_export, ce_res);
	cdat = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_crr_data, cd_res);

	LASSERT(nrq->nr_u.crr.cr_round < cexp->ce_round);

	cfs_binheap_remove(cdat->cd_binheap, &nrq->nr_u.crr.cr_node);

	node = cfs_binheap_root(cdat->cd_binheap);
	if (node == NULL) { /* no more request */
		cdat->cd_round++;
	} else {
		nrq = container_of(node, struct ptlrpc_nrs_request,
				   nr_u.crr.cr_node);
		if (cdat->cd_round < nrq->nr_u.crr.cr_round)
			cdat->cd_round = nrq->nr_u.crr.cr_round;
	}
}

static void
nrs_crr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE,
	       "NRS start %s request from %s, round "LPU64", seq: "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, libcfs_id2str(req->rq_peer),
	       nrq->nr_u.crr.cr_round, nrq->nr_u.crr.cr_sequence);
}

static void
nrs_crr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	CDEBUG(D_RPCTRACE,
	       "NRS stop %s request from %s, round "LPU64", seq: "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, libcfs_id2str(req->rq_peer),
	       nrq->nr_u.crr.cr_round, nrq->nr_u.crr.cr_sequence);
}

static struct ptlrpc_nrs_ops nrs_crr_ops = {
	.op_policy_init		= NULL,
	.op_policy_fini		= NULL,
	.op_policy_start	= nrs_crr_start,
	.op_policy_stop		= nrs_crr_stop,
	.op_policy_ctl		= NULL, /* not used */
	.op_res_get		= nrs_crr_res_get,
	.op_res_put		= NULL, /* not used */
	.op_req_poll		= nrs_crr_req_poll,
	.op_req_enqueue		= nrs_crr_req_add,
	.op_req_dequeue		= nrs_crr_req_del,
	.op_req_start		= nrs_crr_req_start,
	.op_req_stop		= nrs_crr_req_stop,
};

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_crr_desc = {
	.pd_name		= "crr",
	.pd_type		= PTLRPC_NRS_TYPE_CRR,
	.pd_ops			= &nrs_crr_ops,
};

/*
 * CRR-N, Client Round Robin over NIDs
 */

static unsigned
nrs_crrn_hop_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(lnet_nid_t), mask);
}

static int
nrs_crrn_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	lnet_nid_t		*nid = (lnet_nid_t *)key;
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	return *nid == cli->cc_nid;
}

static void *
nrs_crrn_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	return &cli->cc_nid;
}

static void *
nrs_crrn_hop_obj(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_crrn_client, cc_hnode);
}

static void
nrs_crrn_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client *cli = cfs_hlist_entry(hnode,
						      struct nrs_crrn_client,
						      cc_hnode);
	cfs_atomic_inc(&cli->cc_ref);
}

static void
nrs_crrn_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	cfs_atomic_dec(&cli->cc_ref);
}

static void
nrs_crrn_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);

	LASSERTF(cfs_atomic_read(&cli->cc_ref) == 0,
		 "Busy CRR-N object %s with %d refs\n",
		 libcfs_nid2str(cli->cc_nid), cfs_atomic_read(&cli->cc_ref));
	OBD_FREE_PTR(cli);
}

static cfs_hash_ops_t nrs_crrn_hash_ops = {
	.hs_hash	= nrs_crrn_hop_hash,
	.hs_keycmp	= nrs_crrn_hop_keycmp,
	.hs_key		= nrs_crrn_hop_key,
	.hs_object	= nrs_crrn_hop_obj,
	.hs_get		= nrs_crrn_hop_get,
	.hs_put		= nrs_crrn_hop_put,
	.hs_put_locked	= nrs_crrn_hop_put,
	.hs_exit	= nrs_crrn_hop_exit,
};

#define NRS_NID_BBITS		8
#define NRS_NID_HBITS_LOW	10
#define NRS_NID_HBITS_HIGH	15

static int
nrs_crrn_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net;

	OBD_ALLOC_PTR(net);
	if (net == NULL)
		return -ENOMEM;

	/* XXX we might want to move these allocations to op_policy_start()
	 * in the future FIXME */
	net->cn_binheap = cfs_binheap_create(&nrs_crr_heap_ops,
					     CBH_FLAG_ATOMIC_GROW, 4096, NULL);
	if (net->cn_binheap == NULL)
		goto failed;

	net->cn_cli_hash = cfs_hash_create("nrs_nid_hash",
					   NRS_NID_HBITS_LOW,
					   NRS_NID_HBITS_HIGH,
					   NRS_NID_BBITS, 0,
					   CFS_HASH_MIN_THETA,
					   CFS_HASH_MAX_THETA,
					   &nrs_crrn_hash_ops,
					   CFS_HASH_DEFAULT);
	if (net->cn_cli_hash == NULL)
		goto failed;

	policy->pol_private = net;

	return 0;

failed:
	if (net->cn_binheap != NULL)
		cfs_binheap_destroy(net->cn_binheap);

	OBD_FREE_PTR(net);
	return -ENOMEM;
}

static void
nrs_crrn_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net = policy->pol_private;

	LASSERT(net != NULL);
	LASSERT(net->cn_binheap != NULL);
	LASSERT(net->cn_cli_hash != NULL);
	LASSERT(cfs_binheap_is_empty(net->cn_binheap));

	cfs_binheap_destroy(net->cn_binheap);
	cfs_hash_putref(net->cn_cli_hash);
	OBD_FREE_PTR(net);

	policy->pol_private = NULL;
}

static int
nrs_crrn_res_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq,
		 struct ptlrpc_nrs_resource *parent,
		 struct ptlrpc_nrs_resource **resp)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	struct nrs_crrn_client	*tmp;
	struct ptlrpc_request	*req;

	if (parent == NULL) {
		*resp = &((struct nrs_crrn_net *)policy->pol_private)->cn_res;
		return 0;
	}

	net = container_of(parent, struct nrs_crrn_net, cn_res);
	req = container_of(nrq, struct ptlrpc_request, rq_nrq);

	cli = cfs_hash_lookup(net->cn_cli_hash, &req->rq_peer.nid);
	if (cli != NULL)
		goto out;

	OBD_ALLOC_PTR(cli);
	if (cli == NULL)
		return -ENOMEM;

	cli->cc_nid = req->rq_peer.nid;
	cfs_atomic_set(&cli->cc_ref, 1); /* 1 for caller */
	tmp = cfs_hash_findadd_unique(net->cn_cli_hash,
				      &cli->cc_nid, &cli->cc_hnode);
	if (tmp != cli) {
		OBD_FREE_PTR(cli);
		cli = tmp;
	}
 out:
	*resp = &cli->cc_res;
	return 1;
}

static void
nrs_crrn_res_put(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_resource *res)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;

	if (res->res_parent == NULL)
		return;

	cli = container_of(res, struct nrs_crrn_client, cc_res);
	net = container_of(res->res_parent, struct nrs_crrn_net, cn_res);

	cfs_hash_put(net->cn_cli_hash, &cli->cc_hnode);
}

static struct ptlrpc_nrs_request *
nrs_crrn_req_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct nrs_crrn_net	*net = policy->pol_private;
	cfs_binheap_node_t *node = cfs_binheap_root(net->cn_binheap);

	return node == NULL ? NULL :
	       container_of(node, struct ptlrpc_nrs_request, nr_u.crr.cr_node);
}

static int
nrs_crrn_req_add(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	int		   rc;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_crrn_client, cc_res);
	net = container_of(nrs_request_resource(nrq)->res_parent,
			   struct nrs_crrn_net, cn_res);

	if (cli->cc_round < net->cn_round)
		cli->cc_round = net->cn_round;

	nrq->nr_u.crr.cr_round = cli->cc_round;
	nrq->nr_u.crr.cr_sequence = net->cn_sequence;

	rc = cfs_binheap_insert(net->cn_binheap, &nrq->nr_u.crr.cr_node);
	if (rc == 0) {
		net->cn_sequence++;
		cli->cc_round++;
	}
	return rc;
}

static void
nrs_crrn_req_del(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	cfs_binheap_node_t	*node;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_crrn_client, cc_res);
	net = container_of(nrs_request_resource(nrq)->res_parent,
			   struct nrs_crrn_net, cn_res);

	LASSERT(nrq->nr_u.crr.cr_round < cli->cc_round);

	cfs_binheap_remove(net->cn_binheap, &nrq->nr_u.crr.cr_node);

	node = cfs_binheap_root(net->cn_binheap);
	if (node == NULL) { /* no more request */
		net->cn_round++;
	} else {
		nrq = container_of(node, struct ptlrpc_nrs_request,
				   nr_u.crr.cr_node);
		if (net->cn_round < nrq->nr_u.crr.cr_round)
			net->cn_round = nrq->nr_u.crr.cr_round;
	}
}

static struct ptlrpc_nrs_ops nrs_crrn_ops = {
	.op_policy_init		= NULL,
	.op_policy_fini		= NULL,
	.op_policy_start	= nrs_crrn_start,
	.op_policy_stop		= nrs_crrn_stop,
	.op_policy_ctl		= NULL, /* not used */
	.op_res_get		= nrs_crrn_res_get,
	.op_res_put		= nrs_crrn_res_put,
	.op_req_poll		= nrs_crrn_req_poll,
	.op_req_enqueue		= nrs_crrn_req_add,
	.op_req_dequeue		= nrs_crrn_req_del,
	.op_req_start		= nrs_crr_req_start, /* reuse CRR */
	.op_req_stop		= nrs_crr_req_stop,  /* reuse CRR */
};

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_crrn_desc = {
	.pd_name		= "crrn",
	.pd_type		= PTLRPC_NRS_TYPE_CRRN,
	.pd_ops			= &nrs_crrn_ops,
};

/*
 * ORR, Object-based Round Robin policy
 */

/* Comparison function for the binary heap; determines the direction of
 * inequality between ORR requests, based on the correspoding physical or
 * logical block numbers */
static int
orr_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct ptlrpc_nrs_request *nrq1;
	struct ptlrpc_nrs_request *nrq2;

	nrq1 = container_of(e1, struct ptlrpc_nrs_request, nr_u.orr.or_node);
	nrq2 = container_of(e2, struct ptlrpc_nrs_request, nr_u.orr.or_node);

	if (nrq1->nr_u.orr.or_round < nrq2->nr_u.orr.or_round)
		return 1;
	if (nrq1->nr_u.orr.or_round > nrq2->nr_u.orr.or_round)
		return 0;

	/* If round numbers are equal, requests should be sorted by
	 * ascending offset */
	if (nrq1->nr_u.orr.or_range.or_start < nrq2->nr_u.orr.or_range.or_start)
		return 1;
	else if (nrq1->nr_u.orr.or_range.or_start >
		 nrq2->nr_u.orr.or_range.or_start)
		return 0;
	/* Requests start from the same offset */
	else
		/* Do the longer one first; maybe slightly more chances of
		 * hitting the disk drive cache later with the lengthiest
		 * request */
		if (nrq1->nr_u.orr.or_range.or_end >
		    nrq2->nr_u.orr.or_range.or_end)
			return 0;
		else
			return 1;
}

/* ORR binary heap operations */
static cfs_binheap_ops_t nrs_orr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= orr_req_compare,
};

/* TODO: Use a better way, or document this */
#define NRS_ORR_DFLT_OID	0x0ULL

/* Populate the ORR/TRR key fields for the RPC */
/* TODO: Check return values of this */
static int
nrs_orr_key_fill(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq,
		 enum ptlrpc_nrs_pol_type type, struct nrs_orr_key *key)
{
	struct ptlrpc_request  *req;
	struct ost_body        *body;
	__u32			opc;
	int			rc = 0;


	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	LASSERT(req != NULL);

	opc = lustre_msg_get_opc(req->rq_reqmsg);

	req_capsule_init(&req->rq_pill, req, RCL_SERVER);

	if (opc == OST_READ) {
		req_capsule_set(&req->rq_pill, &RQF_OST_BRW_READ);
		nrq->nr_u.orr.or_write = 0;
	} else if (opc == OST_WRITE) {
		req_capsule_set(&req->rq_pill, &RQF_OST_BRW_WRITE);
		nrq->nr_u.orr.or_write = 1;
	} else {
		/* Only OST_[READ|WRITE] supported by ORR/TRR */
		LBUG();
	}

	if (type == PTLRPC_NRS_TYPE_ORR) {
		/* request pill has been initialized in ptlrpc_hpreq_init() */
		body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			GOTO(out, rc = -EFAULT);

		/* XXX: This really needs a call to ost_validate_obdo(), to
		 * get a proper objid although it seems not to be essential at
		 * the moment, as long as we get something that is unique; maybe
		 * this will become more important with FID-on-OST later?
		 */
		key->ok_id = body->oa.o_id;
	} else if (type == PTLRPC_NRS_TYPE_TRR) {
		key->ok_id = NRS_ORR_DFLT_OID;
	} else {
		LBUG();
	}
#ifdef HAVE_SERVER_SUPPORT
	{
		/* lsd variable would go unused in #else if declared in function
		 * scope. */
		struct lr_server_data  *lsd;

		lsd = class_server_data(req->rq_export->exp_obd);
		/* XXX: Redundant check? */
		if (lsd == NULL)
			GOTO(out, rc = -EFAULT);

		key->ok_idx = lsd->lsd_ost_index;
	}
#else
	/* XXX: Can we do something better than this here? This is just to fix
	 * builds made with --disable-server. */
	key->ok_idx = 0;
#endif

out:
	return rc;
}

/* Checks if the RPC type handled by ORR or TRR */
static __u8
nrs_orr_req_supported(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request  *req;
	__u32			opc;
	enum nrs_orr_supp	supp;

	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	opc = lustre_msg_get_opc(req->rq_reqmsg);

	cfs_read_lock(&orrd->od_lock);
	supp = orrd->od_supp;
	cfs_read_unlock(&orrd->od_lock);

	switch (supp) {
	case NOS_OST_READ:
		if (opc == OST_READ)
			RETURN(1);
		break;
	case NOS_OST_WRITE:
		if (opc == OST_WRITE)
			RETURN(1);
		break;
	case NOS_OST_RW:
		if (opc == OST_READ || opc == OST_WRITE)
			RETURN(1);
		break;
	default:
		LBUG();
	}
	RETURN(0);
}

/* Populate the range values for the request with logical offsets */
static void
nrs_orr_logical(struct niobuf_remote *nb, int niocount,
		struct nrs_orr_req_range *range)
{
	/* Should we do this at page boundaries ? */
	range->or_start = nb[0].offset & CFS_PAGE_MASK;
	range->or_end = (nb[niocount - 1].offset +
			 nb[niocount - 1].len - 1) | ~CFS_PAGE_MASK;
}

static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys);

/* Set ORR range values in RPC */
static void
nrs_orr_set_range(struct ptlrpc_nrs_request *nrq,
		  struct nrs_orr_req_range *range)
{
	nrq->nr_u.orr.or_range.or_start = range->or_start;
	nrq->nr_u.orr.or_range.or_end = range->or_end;
}

static int
nrs_orr_get_range(struct ptlrpc_nrs_request *nrq, struct nrs_orr_data *orrd)
{
	struct ptlrpc_request  *req = container_of(nrq, struct ptlrpc_request,
						   rq_nrq);
	struct ptlrpc_service	*svc = req->rq_rqbd->rqbd_service;
	struct obd_ioobj	*ioo;
	struct niobuf_remote	*nb;
	struct ost_body		*body;
	struct nrs_orr_req_range range;
	int			niocount;
	int			objcount;
	int			rc = 0;
	int			i;
	__u8			phys;

	/* Obtain physical offsets if selected, and this is not an OST_WRITE
	 * RPC */
	cfs_read_lock(&orrd->od_lock);
	phys = orrd->od_physical && !nrq->nr_u.orr.or_write;
	cfs_read_unlock(&orrd->od_lock);

	objcount = req_capsule_get_size(&req->rq_pill, &RMF_OBD_IOOBJ,
					RCL_CLIENT) / sizeof(*ioo);
	/* Get and analyze fields; the following LASSERTs are also performed
	 * later */
	ioo = req_capsule_client_get(&req->rq_pill, &RMF_OBD_IOOBJ);
	if (ioo == NULL)
		GOTO(out, rc = -EFAULT);

	body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
	if (body == NULL)
		GOTO(out, rc = -EFAULT);

	/* Should this be only ioo.ioo_bufcnt? */
	for (niocount = i = 0; i < objcount; i++)
		niocount += ioo[i].ioo_bufcnt;

	nb = req_capsule_client_get(&req->rq_pill, &RMF_NIOBUF_REMOTE);
	if (nb == NULL)
		GOTO(out, rc = -EFAULT);

	/* Use logical information from niobuf_remote structures */
	nrs_orr_logical(nb, niocount, &range);

	if (phys) {
		/* Release the lock here temporarily (we don't really need it
		 * here), as some operations that need to be carried out as
		 * part of performing the fimeap call may need to sleep.
		 */
		cfs_spin_unlock(&svc->srv_rq_lock);

		/* Translate to physical block offsets from backend filesystem
		 * extents; range is in and out parameter */
		rc = nrs_orr_log2phys(req->rq_export, &body->oa, &range,
				      &range);

		cfs_spin_lock(&svc->srv_rq_lock);

		if (rc)
			GOTO(out, rc);
	}
	/* Assign retrieved range values to request */
	nrs_orr_set_range(nrq, &range);
out:
	return rc;
}

/* TODO: Re-check these */
#define NRS_ORR_HBITS_LOW	10
#define NRS_ORR_HBITS_HIGH	16
#define NRS_ORR_HBBITS		 8

static int nrs_orr_hop_keys_eq(struct nrs_orr_key *k1, struct nrs_orr_key *k2)
{
	return (k1->ok_idx == k2->ok_idx && k1->ok_id == k2->ok_id);
}

/* ORR hash operations */
static unsigned
nrs_orr_hop_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(struct nrs_orr_key), mask);
}

static void *
nrs_orr_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);

	return &orro->oo_key;
}

static int
nrs_orr_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);

	return nrs_orr_hop_keys_eq(&orro->oo_key, (struct nrs_orr_key *)key);
}

static void *
nrs_orr_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
}

static void
nrs_orr_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);
	cfs_atomic_inc(&orro->oo_ref);
}

static void
nrs_orr_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);
	cfs_atomic_dec(&orro->oo_ref);
}

static void
nrs_orr_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orro = cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
	orrd = container_of(orro->oo_res.res_parent, struct nrs_orr_data,
			    od_res);

	LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
		 "Busy NRS orr policy for object with objid "LPX64" at OST "
		 "with OST index %u, with %d refs\n", orro->oo_key.ok_id,
		 orro->oo_key.ok_idx, cfs_atomic_read(&orro->oo_ref));

	OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
}

static cfs_hash_ops_t nrs_orr_hash_ops = {
	.hs_hash	= nrs_orr_hop_hash,
	.hs_key		= nrs_orr_hop_key,
	.hs_keycpy	= NULL,
	.hs_keycmp	= nrs_orr_hop_keycmp,
	.hs_object	= nrs_orr_hop_object,
	.hs_get		= nrs_orr_hop_get,
	.hs_put		= nrs_orr_hop_put,
	.hs_put_locked	= nrs_orr_hop_put,
	.hs_exit	= nrs_orr_hop_exit
};

#define NRS_ORR_QUANTUM_DFLT 256
/* Offset ordering type, 0 for logical offsets, 1 for physical */
#define NRS_ORR_OFFSET_TYPE 1

/* TODO: Change these; temporary implementation */
#ifdef LPROCFS
int nrs_orr_lprocfs_init(struct ptlrpc_nrs_policy *policy);
void nrs_orr_lprocfs_fini(struct ptlrpc_nrs_policy *policy);
#else
int nrs_orr_lprocfs_init(struct ptlrpc_nrs_policy *policy) { return 0; }
void nrs_orr_lprocfs_fini(struct ptlrpc_nrs_policy *policy) { }
#endif

static int
nrs_orr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	*orrd;
	int			rc = 0;

	ENTRY;

	OBD_ALLOC_PTR(orrd);
	if (orrd == NULL)
		RETURN(-ENOMEM);

	/* Binary heap for sorted incoming requests */
	orrd->od_binheap = cfs_binheap_create(&nrs_orr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL);
	if (orrd->od_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Slab cache for NRS ORR objects */
	orrd->od_cache = cfs_mem_cache_create(policy->pol_nrs->nrs_queue_type ==
					      PTLRPC_NRS_QUEUE_REG ?
					      "nrs_orr_reg" : "nrs_orr_hp",
					      sizeof(struct nrs_orr_object),
					      0, 0);
	if (orrd->od_cache == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Hash for finding objects by struct nrs_orr_key */
	orrd->od_obj_hash = cfs_hash_create(policy->pol_nrs->nrs_queue_type ==
					    PTLRPC_NRS_QUEUE_REG ?
					    "nrs_orr_reg" :
					    "nrs_orr_hp",
					    NRS_ORR_HBITS_LOW,
					    NRS_ORR_HBITS_HIGH,
					    NRS_ORR_HBBITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_orr_hash_ops,
					    CFS_HASH_DEFAULT);

	if (orrd->od_obj_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	cfs_rwlock_init(&orrd->od_lock);
	orrd->od_quantum = NRS_ORR_QUANTUM_DFLT;
	orrd->od_supp = NOS_DFLT;
	orrd->od_physical = NRS_ORR_OFFSET_TYPE;

	rc = nrs_orr_lprocfs_init(policy);
	if (rc != 0)
		GOTO(failed, rc);

	policy->pol_private = orrd;

	RETURN(rc);

failed:
	if (orrd->od_obj_hash) {
		cfs_hash_putref(orrd->od_obj_hash);
		orrd->od_obj_hash = NULL;
	}
	if (orrd->od_cache) {
		rc = cfs_mem_cache_destroy(orrd->od_cache);
		LASSERTF(rc == 0, "Could not destroy od_cache slab\n");
	}
	if (orrd->od_binheap != NULL)
		cfs_binheap_destroy(orrd->od_binheap);

	OBD_FREE_PTR(orrd);

	RETURN(rc);
}

static void
nrs_orr_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data *orrd = policy->pol_private;

	ENTRY;

	LASSERT(orrd != NULL);
	LASSERT(cfs_binheap_is_empty(orrd->od_binheap));

	cfs_binheap_destroy(orrd->od_binheap);
	cfs_hash_putref(orrd->od_obj_hash);
	cfs_mem_cache_destroy(orrd->od_cache);

	nrs_orr_lprocfs_fini(policy);

	OBD_FREE_PTR(orrd);

	policy->pol_private = NULL;
}

/* TODO: Check return values of this */
static int
nrs_orr_res_get(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq,
		struct ptlrpc_nrs_resource *parent,
		struct ptlrpc_nrs_resource **resp)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	struct nrs_orr_object	*tmp;
	struct nrs_orr_key	key;
	int			rc = 0;

	ENTRY;

	/* struct nrs_orr_data is requested */
	if (parent == NULL) {
		*resp = &((struct nrs_orr_data *)policy->pol_private)->od_res;
		RETURN(0);
	}

	orrd = container_of(parent, struct nrs_orr_data, od_res);
	/* If the request type is not supported, fail the enqueuing; the RPC
	 * will be handled by the default NRS policy. */
	if (!nrs_orr_req_supported(orrd, nrq))
		RETURN(-1);

	/* struct nrs_orr_object is requested */
	rc = nrs_orr_key_fill(orrd, nrq, policy->pol_type, &key);
	if (rc != 0)
		RETURN(rc);

	orro = cfs_hash_lookup(orrd->od_obj_hash, &key);
	if (orro != NULL)
		GOTO(out, 1);

	OBD_SLAB_ALLOC_PTR_GFP(orro, orrd->od_cache, CFS_ALLOC_IO);
	if (orro == NULL)
		RETURN(-ENOMEM);

	orro->oo_key = key;
	/* XXX: This needs to be locked, really */
	orro->oo_quantum = orrd->od_quantum;

	cfs_atomic_set(&orro->oo_ref, 1);
	tmp = cfs_hash_findadd_unique(orrd->od_obj_hash, &orro->oo_key,
				      &orro->oo_hnode);
	if (tmp != orro) {
		OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
		orro = tmp;
	}
out:
	/* For debugging purposes */
	nrq->nr_u.orr.or_key = orro->oo_key;

	*resp = &orro->oo_res;

	RETURN(1);
}

static void
nrs_orr_res_put(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_resource *res)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	ENTRY;

	if (res->res_parent == NULL)
		return;

	orro = container_of(res, struct nrs_orr_object, oo_res);
	LASSERT(res->res_parent != NULL);
	orrd = container_of(res->res_parent, struct nrs_orr_data, od_res);

	cfs_hash_put(orrd->od_obj_hash, &orro->oo_hnode);
}

/* Picks up a request from the root of the binheap */
static struct ptlrpc_nrs_request *
nrs_orr_req_poll(struct ptlrpc_nrs_policy *policy, void *arg)
{
	struct nrs_orr_data	*orrd = policy->pol_private;
	cfs_binheap_node_t	*node = cfs_binheap_root(orrd->od_binheap);

	return node == NULL ? NULL :
		container_of(node, struct ptlrpc_nrs_request, nr_u.orr.or_node);
}

/* Sort-adds a request to the binary heap */
static int
nrs_orr_req_add(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	int			 rc;

	orro = container_of(nrs_request_resource(nrq),
			    struct nrs_orr_object, oo_res);
	orrd = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_orr_data, od_res);

	LASSERT(orro->oo_res.res_parent != NULL);
	LASSERT(orro->oo_res.res_parent ==
		nrs_request_resource(nrq)->res_parent);

	rc = nrs_orr_get_range(nrq, orrd);
	if (rc)
		RETURN(rc);

	if (orro->oo_started == 0 || orro->oo_active == 0) {
		orro->oo_round = orrd->od_round;
		orrd->od_round++;
		orro->oo_started = 1;
	}

	nrq->nr_u.orr.or_round = orro->oo_round;
	rc = cfs_binheap_insert(orrd->od_binheap, &nrq->nr_u.orr.or_node);
	if (rc == 0) {
		orro->oo_quantum--;
		orro->oo_active++;
		if (orro->oo_quantum == 0) {
			orro->oo_quantum = orrd->od_quantum;
			orro->oo_started = 0;
		}
	}
	RETURN(rc);
}

static void
nrs_orr_req_del(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orrd = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_orr_data, od_res);
	orro = container_of(nrs_request_resource(nrq),
			    struct nrs_orr_object, oo_res);

	LASSERT(nrq->nr_u.orr.or_round < orrd->od_round);
	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	cfs_binheap_remove(orrd->od_binheap, &nrq->nr_u.orr.or_node);
	orro->oo_active--;
}

static void
nrs_orr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS ORR start %s request for object with ID "LPX64""
	       " from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static void
nrs_orr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS ORR stop %s request for object with ID "LPX64" "
	       "from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static struct ptlrpc_nrs_ops nrs_orr_ops = {
	.op_policy_init		= NULL,
	.op_policy_fini		= NULL,
	.op_policy_start	= nrs_orr_start,
	.op_policy_stop		= nrs_orr_stop,
	.op_policy_ctl		= NULL,
	.op_res_get		= nrs_orr_res_get,
	.op_res_put		= nrs_orr_res_put,
	.op_req_poll		= nrs_orr_req_poll,
	.op_req_enqueue		= nrs_orr_req_add,
	.op_req_dequeue		= nrs_orr_req_del,
	.op_req_start		= nrs_orr_req_start,
	.op_req_stop		= nrs_orr_req_stop
};

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_orr_desc = {
	.pd_name		= "orr",
	.pd_type		= PTLRPC_NRS_TYPE_ORR,
	.pd_ops			= &nrs_orr_ops
};

#ifdef __KERNEL__

#define ORR_NUM_EXTENTS 1

/* This structure mirrors struct ll_user_fiemap, but uses a fixed length for the
 * array of ll_fiemap_extent structs; its prime intention is for statically
 * allocated variables.
 */
struct nrs_orr_fiemap {
	__u64 of_start;
	__u64 of_length;
	__u32 of_flags;
	__u32 of_mapped_extents;
	__u32 of_extent_count;
	__u32 of_reserved;
	struct ll_fiemap_extent of_extents[ORR_NUM_EXTENTS];
};

/* Get extents from the range of niobuf_remotes here by doing fiemap calls via
 * obd_get_info(), and then return a lower and higher physical block number for
 * TODO: Maybe this should be placed in the OST layer?
 */
static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys)
{
	/* Use ll_fiemap_info_key, as done in ost_get_info() */
	struct ll_fiemap_info_key  fm_key = { .name = KEY_FIEMAP };
	struct nrs_orr_fiemap	   of_fiemap;
	struct ll_user_fiemap	  *fiemap = (struct ll_user_fiemap *)&of_fiemap;
	int			   rc;
	int			   replylen;
	loff_t			   start = OBD_OBJECT_EOF;
	loff_t			   end = 0;
	__u64			   log_req_len = log->or_end - log->or_start;

	ENTRY;

	fiemap = (struct ll_user_fiemap *)&of_fiemap;

	fm_key.oa = *oa;
	fm_key.fiemap.fm_start = log->or_start;
	fm_key.fiemap.fm_length = log_req_len;
	fm_key.fiemap.fm_extent_count = ORR_NUM_EXTENTS;

	/* Here we perform a fiemap call in order to get extent descriptions;
	 * ideally, we could pass '0' in to fm_extent_count in order to obtain
	 * the number of extents required in order to map the whole file and
	 * allocate as much memory as we require, but here we adopt a faster
	 * route by performing only one fiemap call to get the extent
	 * information and assume the nrs_orr_fiemap struct that is allocated
	 * on the stack will suffice; i fit does not, we can either fail the
	 * operation, or resort to allocating memory on demand or from a pool.
	 */
	/* Do an obd_get_info to get the extent descriptions */
	rc = obd_get_info(NULL, exp, sizeof(fm_key), &fm_key, &replylen,
			  (struct ll_user_fiemap *)&of_fiemap, NULL);
	if (unlikely(rc)) {
		CERROR("obd_get_info failed: rc = %d\n", rc);
		goto out;
	}

	if (unlikely(fiemap->fm_mapped_extents == 0 ||
	    fiemap->fm_mapped_extents > ORR_NUM_EXTENTS)) {
		CERROR("fm_mapped_extents is %u.\n", fiemap->fm_mapped_extents);
		GOTO(out, rc = -EFAULT);
	}

	/* Optimize start and end calculation for the one fiemap case; this will
	 * have to be changed if we include more than one fiemap structs in the
	 * future.
	 */
	start = fiemap->fm_extents[0].fe_physical;
	start += log->or_start - fiemap->fm_extents[0].fe_logical;
	end = start + log_req_len;

	phys->or_start = start;
	phys->or_end = end;
out:

	return rc;
}
#else
/* Temporary physical offset stub for liblustre */
static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys)
{
	return 0;
}
#endif

/*
 * TRR, Target-based Round Robin policy
 *
 * TRR reuses much of the functions and data structures of ORR
 */

/* TRR binary heap operations */
static cfs_binheap_ops_t nrs_trr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= orr_req_compare,
};

static void
nrs_trr_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orro = cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
	orrd = container_of(orro->oo_res.res_parent, struct nrs_orr_data,
			    od_res);

	LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
		 "Busy NRS TRR policy at OST with index %u, with %d refs\n",
		 orro->oo_key.ok_idx, cfs_atomic_read(&orro->oo_ref));

	OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
}

static cfs_hash_ops_t nrs_trr_hash_ops = {
	.hs_hash	= nrs_orr_hop_hash,
	.hs_key		= nrs_orr_hop_key,
	.hs_keycpy	= NULL,
	.hs_keycmp	= nrs_orr_hop_keycmp,
	.hs_object	= nrs_orr_hop_object,
	.hs_get		= nrs_orr_hop_get,
	.hs_put		= nrs_orr_hop_put,
	.hs_put_locked	= nrs_orr_hop_put,
	.hs_exit	= nrs_trr_hop_exit
};

/* TODO: Re-check these */
#define NRS_TRR_HBITS_LOW	3
#define NRS_TRR_HBITS_HIGH	8
#define NRS_TRR_HBBITS		2


static int
nrs_trr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data    *orrd;
	int			rc = 0;

	ENTRY;

	OBD_ALLOC_PTR(orrd);
	if (orrd == NULL)
		RETURN(-ENOMEM);

	/* Binary heap for sorted incoming requests */
	orrd->od_binheap = cfs_binheap_create(&nrs_trr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL);
	if (orrd->od_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Slab cache for TRR targets */
	/* TODO: Unionize keys for ORR/TRR ? */
	orrd->od_cache = cfs_mem_cache_create(policy->pol_nrs->nrs_queue_type ==
					      PTLRPC_NRS_QUEUE_REG ?
					      "nrs_trr_reg" : "nrs_trr_hp",
					      sizeof(struct nrs_orr_object),
					      0, 0);
	if (orrd->od_cache == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Hash for finding objects by nrs_trr_key_t */
	orrd->od_obj_hash = cfs_hash_create(policy->pol_nrs->nrs_queue_type ==
					    PTLRPC_NRS_QUEUE_REG ?
					    "nrs_trr_reg" : "nrs_trr_hp",
					    NRS_TRR_HBITS_LOW,
					    NRS_TRR_HBITS_HIGH,
					    NRS_TRR_HBBITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_trr_hash_ops,
					    CFS_HASH_DEFAULT);

	if (orrd->od_obj_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	cfs_rwlock_init(&orrd->od_lock);
	orrd->od_quantum = NRS_ORR_QUANTUM_DFLT;
	orrd->od_supp = NOS_DFLT;
	orrd->od_physical = NRS_ORR_OFFSET_TYPE;

	rc = nrs_orr_lprocfs_init(policy);
	if (rc != 0)
		GOTO(failed, rc);

	policy->pol_private = orrd;

	RETURN(rc);

failed:
	if (orrd->od_obj_hash) {
		cfs_hash_putref(orrd->od_obj_hash);
		orrd->od_obj_hash = NULL;
	}
	if (orrd->od_cache) {
		rc = cfs_mem_cache_destroy(orrd->od_cache);
		LASSERTF(rc == 0, "Could not destroy od_cache slab\n");
	}
	if (orrd->od_binheap != NULL)
		cfs_binheap_destroy(orrd->od_binheap);

	OBD_FREE_PTR(orrd);

	RETURN(rc);
}

static void
nrs_trr_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data *orrd = policy->pol_private;

	ENTRY;

	LASSERT(orrd != NULL);
	LASSERT(cfs_binheap_is_empty(orrd->od_binheap));

	cfs_binheap_destroy(orrd->od_binheap);
	cfs_hash_putref(orrd->od_obj_hash);
	cfs_mem_cache_destroy(orrd->od_cache);

	nrs_orr_lprocfs_fini(policy);

	OBD_FREE_PTR(orrd);

	policy->pol_private = NULL;
}

static void
nrs_trr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS TRR start %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static void
nrs_trr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS TRR stop %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

/* Reuse much of the ORR functionality for TRR */
static struct ptlrpc_nrs_ops nrs_trr_ops = {
	.op_policy_init		= NULL,
	.op_policy_fini		= NULL,
	.op_policy_start	= nrs_trr_start,
	.op_policy_stop		= nrs_trr_stop,
	.op_policy_ctl		= NULL,
	.op_res_get		= nrs_orr_res_get,
	.op_res_put		= nrs_orr_res_put,
	.op_req_poll		= nrs_orr_req_poll,
	.op_req_enqueue		= nrs_orr_req_add,
	.op_req_dequeue		= nrs_orr_req_del,
	.op_req_start		= nrs_trr_req_start,
	.op_req_stop		= nrs_trr_req_stop
};

static struct ptlrpc_nrs_policy_desc ptlrpc_nrs_trr_desc = {
	.pd_name		= "trr",
	.pd_type		= PTLRPC_NRS_TYPE_TRR,
	.pd_ops			= &nrs_trr_ops
};
