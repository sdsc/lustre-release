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
 * lustre/ptlrpc/nrs_crr.c
 *
 * Network Request Scheduler (NRS) CRR-N policy
 *
 * Request ordering in a batched Round-Robin manner over client NIDs
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
#include "ptlrpc_internal.h"

/**
 * \name CRR-N policy
 *
 * Client Round-Robin scheduling over client NIDs
 *
 * @{
 *
 */

/**
 * Binary heap predicate.
 *
 * Uses ptlrpc_nrs_request::nr_u::crr::cr_round to compare two binheap nodes and
 * produce a binary predicate that shows their relative priority, so that the
 * binary heap can perform the necessary sorting operations.
 *
 * \param[in] e1 The first binheap node to compare
 * \param[in] e2 The second binheap node to compare
 *
 * \retval 0 e1 > e2
 * \retval 1 e1 <= e2
 */
static int crrn_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct ptlrpc_nrs_request *nrq1;
	struct ptlrpc_nrs_request *nrq2;

	nrq1 = container_of(e1, struct ptlrpc_nrs_request, nr_node);
	nrq2 = container_of(e2, struct ptlrpc_nrs_request, nr_node);

	if (nrq1->nr_u.crr.cr_round < nrq2->nr_u.crr.cr_round)
		return 1;
	else if (nrq1->nr_u.crr.cr_round > nrq2->nr_u.crr.cr_round)
		return 0;

	return nrq1->nr_u.crr.cr_sequence < nrq2->nr_u.crr.cr_sequence;
}

static cfs_binheap_ops_t nrs_crrn_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= crrn_req_compare,
};

/**
 * libcfs_hash operations for nrs_crrn_net::cn_cli_hash
 *
 * This uses ptlrpc_request::rq_peer.nid as its key, in order to hash
 * nrs_crrn_client objects.
 */
#define NRS_NID_BKT_BITS	8
#define NRS_NID_BITS		16

static unsigned nrs_crrn_hop_hash(cfs_hash_t *hs, const void *key,
				  unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(lnet_nid_t), mask);
}

static int nrs_crrn_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	lnet_nid_t		*nid = (lnet_nid_t *)key;
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	return *nid == cli->cc_nid;
}

static void * nrs_crrn_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	return &cli->cc_nid;
}

static void * nrs_crrn_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_crrn_client, cc_hnode);
}

static void nrs_crrn_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client *cli = cfs_hlist_entry(hnode,
						      struct nrs_crrn_client,
						      cc_hnode);
	cfs_atomic_inc(&cli->cc_ref);
}

static void nrs_crrn_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	cfs_atomic_dec(&cli->cc_ref);
}

static void nrs_crrn_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_crrn_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_crrn_client,
						       cc_hnode);
	LASSERTF(cfs_atomic_read(&cli->cc_ref) == 0,
		 "Busy CRR-N object from client with NID %s, with %d refs\n",
		 libcfs_nid2str(cli->cc_nid), cfs_atomic_read(&cli->cc_ref));

	OBD_FREE_PTR(cli);
}

static cfs_hash_ops_t nrs_crrn_hash_ops = {
	.hs_hash	= nrs_crrn_hop_hash,
	.hs_keycmp	= nrs_crrn_hop_keycmp,
	.hs_key		= nrs_crrn_hop_key,
	.hs_object	= nrs_crrn_hop_object,
	.hs_get		= nrs_crrn_hop_get,
	.hs_put		= nrs_crrn_hop_put,
	.hs_put_locked	= nrs_crrn_hop_put,
	.hs_exit	= nrs_crrn_hop_exit,
};

/**
 * Called when a CRR-N policy instance is started.
 *
 * \param[in] policy The policy
 *
 * \retval -ENOMEM OOM error
 * \retval 0	   success
 */
static int nrs_crrn_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net    *net;
	int			rc = 0;
	ENTRY;

	OBD_CPT_ALLOC_PTR(net, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (net == NULL)
		RETURN(-ENOMEM);

	net->cn_binheap = cfs_binheap_create(&nrs_crrn_heap_ops,
					     CBH_FLAG_ATOMIC_GROW, 4096, NULL,
					     nrs_pol2cptab(policy),
					     nrs_pol2cptid(policy));
	if (net->cn_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	net->cn_cli_hash = cfs_hash_create("nrs_crrn_nid_hash",
					   NRS_NID_BITS, NRS_NID_BITS,
					   NRS_NID_BKT_BITS, 0,
					   CFS_HASH_MIN_THETA,
					   CFS_HASH_MAX_THETA,
					   &nrs_crrn_hash_ops,
					   CFS_HASH_RW_BKTLOCK);
	if (net->cn_cli_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	/**
	 * Set default quantum value to max_rpcs_in_flight for non-MDS OSCs;
	 * there may be more RPCs pending from each struct nrs_crrn_client even
	 * with the default max_rpcs_in_flight value, as we are scheduling over
	 * NIDs, and there may be more than one mount point per client.
	 */
	net->cn_quantum = OSC_MAX_RIF_DEFAULT;
	/**
	 * Set to 1 so that the test inside nrs_crrn_req_add() can evaluate to
	 * true.
	 */
	net->cn_sequence = 1;

	policy->pol_private = net;

	RETURN(rc);

failed:
	if (net->cn_binheap != NULL)
		cfs_binheap_destroy(net->cn_binheap);

	OBD_FREE_PTR(net);

	RETURN(rc);
}

/**
 * Called when a CRR-N policy instance is stopped.
 *
 * Called when the policy has been instructed to transition to the
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED state and has no more pending
 * requests to serve.
 *
 * \param[in] policy The policy
 */
static void nrs_crrn_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net = policy->pol_private;
	ENTRY;

	LASSERT(net != NULL);
	LASSERT(net->cn_binheap != NULL);
	LASSERT(net->cn_cli_hash != NULL);
	LASSERT(cfs_binheap_is_empty(net->cn_binheap));

	cfs_binheap_destroy(net->cn_binheap);
	cfs_hash_putref(net->cn_cli_hash);

	OBD_FREE_PTR(net);
}

/**
 * Performs a policy-specific ctl function on CRR-N policy instances; similar
 * to ioctl.
 *
 * \param[in]	  policy The policy instance
 * \param[in]	  opc	 The opcode
 * \param[in,out] arg	 Used for passing parameters and information
 *
 * \pre spin_is_locked(&policy->pol_nrs->->nrs_lock)
 * \post spin_is_locked(&policy->pol_nrs->->nrs_lock)
 *
 * \retval 0   Operation carried out successfully
 * \retval -ve Error
 */
int nrs_crrn_ctl(struct ptlrpc_nrs_policy *policy, enum ptlrpc_nrs_ctl opc,
		 void *arg)
{
	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	/**
	 * The policy may be stopped, but the lprocfs files and
	 * ptlrpc_nrs_policy instances remain present until unregistration
	 * time; do not perform the ctl operation if this is the case.
	 */
	if (policy->pol_private == NULL)
		RETURN(-ENODEV);

	switch(opc) {
	default:
		RETURN(-EINVAL);

	/**
	 * Read Round Robin quantum size of a policy instance.
	 */
	case NRS_CTL_CRRN_RD_QUANTUM: {
		struct nrs_crrn_net	*net = policy->pol_private;

		*(__u16 *)arg = net->cn_quantum;
		}
		break;

	/**
	 * Write Round Robin quantum size of a policy instance.
	 */
	case NRS_CTL_CRRN_WR_QUANTUM: {
		struct nrs_crrn_net	*net = policy->pol_private;

		net->cn_quantum = *(__u16 *)arg;
		LASSERT(net->cn_quantum != 0);
		}
		break;
	}

	RETURN(0);
}

/**
 * Obtains resources from CRR-N policy instances. The top-level resource lives
 * inside \e nrs_crrn_net and the second-level resource inside
 * \e nrs_crrn_client object instances.
 *
 * \param[in]  policy	  The policy for which resources are being taken for
 *			  request \a nrq
 * \param[in]  nrq	  The request for which resources are being taken
 * \param[in]  parent	  Parent resource, embedded in nrs_crrn_net for the
 *			  CRR-N policy
 * \param[out] resp	  Resources references are placed in this array
 * \param[in]  moving_req Signifies limited caller context; used to perform
 *			  memory allocations in an atomic context in this
 *			  policy
 *
 * \retval 0   We are returning a top-level, parent resource, one that is
 *	       embedded in an nrs_crrn_net object
 * \retval 1   We are returning a bottom-level resource, one that is embedded
 *	       in an nrs_crrn_client object
 *
 * \see nrs_resource_get_safe()
 */
int nrs_crrn_res_get(struct ptlrpc_nrs_policy *policy,
		     struct ptlrpc_nrs_request *nrq,
		     const struct ptlrpc_nrs_resource *parent,
		     struct ptlrpc_nrs_resource **resp, bool moving_req)
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

	OBD_CPT_ALLOC_GFP(cli, nrs_pol2cptab(policy), nrs_pol2cptid(policy),
			  sizeof(*cli), moving_req ? CFS_ALLOC_ATOMIC :
			  CFS_ALLOC_IO);
	if (cli == NULL)
		return -ENOMEM;

	cli->cc_nid = req->rq_peer.nid;

	cfs_atomic_set(&cli->cc_ref, 1);
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

/**
 * Called when releasing references to the resource
 * hierachy obtained for a request for scheduling using the CRR-N policy
 *
 * \param[in] policy   The policy the resource belongs to
 * \param[in] res      The resource to be released
 */
static void nrs_crrn_res_put(struct ptlrpc_nrs_policy *policy,
			     const struct ptlrpc_nrs_resource *res)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;

	/**
	 * Do nothing for freeing parent, nrs_crrn_net resources
	 */
	if (res->res_parent == NULL)
		return;

	cli = container_of(res, struct nrs_crrn_client, cc_res);
	net = container_of(res->res_parent, struct nrs_crrn_net, cn_res);

	cfs_hash_put(net->cn_cli_hash, &cli->cc_hnode);
}

/**
 * Called when polling the CRR-N policy for a request so that it can be served
 *
 * \param[in] policy The policy being polled
 *
 * \retval The request to be handled
 *
 * \see ptlrpc_nrs_req_poll_nolock()
 */
static
struct ptlrpc_nrs_request * nrs_crrn_req_poll(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net = policy->pol_private;
	cfs_binheap_node_t	*node = cfs_binheap_root(net->cn_binheap);

	return node == NULL ? NULL :
	       container_of(node, struct ptlrpc_nrs_request, nr_node);
}

/**
 * Adds request \a nrq to a CRR-N \a policy instance's set of queued requests
 *
 * A scheduling round is a stream of requests that have been sorted in batches
 * according to the client that they originate from (as identified by its NID);
 * there can be only one batch for each client in each round. The batches are of
 * maximum size nrs_crrn_net:cn_quantum. When a new request arrives for
 * scheduling from a client that has exhausted its quantum in its current round,
 * it will start scheduling requests on the next scheduling round. Clients are
 * allowed to schedule requests against a round until all requests for the round
 * are serviced, so a client might miss a round if it is not generating requests
 * for a long enough period of time. Clients that miss a round will continue
 * with scheduling the next request that they generate, starting at the round
 * that requests are being dispatched for, at the time of arrival of this new
 * request.
 *
 * Requests are tagged with the round number and a sequence number; the sequence
 * number indicates the relative ordering amongst the batches of requests in a
 * round, and is identical for all requests in a batch, as is the round number.
 * The round and sequence numbers are used by crrn_req_compare() in order to
 * maintain an ordered set of rounds, with each round consisting of an ordered
 * set of batches of requests.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to add
 *
 * \retval 0 request successfully added
 * \retval != 0 error
 */
static int nrs_crrn_req_add(struct ptlrpc_nrs_policy *policy,
			    struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	int			 rc;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_crrn_client, cc_res);
	net = container_of(nrs_request_resource(nrq)->res_parent,
			   struct nrs_crrn_net, cn_res);

	if (cli->cc_quantum == 0 || cli->cc_round < net->cn_round ||
	    (cli->cc_active == 0 && cli->cc_quantum > 0)) {

		/**
		 * If the client has no pending requests, and still some of its
		 * quantum remaining unused, which implies it has not had a
		 * chance to schedule up to its maximum allowed batch size of
		 * requests in the previous round it participated, schedule this
		 * next request on a new round; this avoids fragmentation of
		 * request batches caused by client inactivity, at the expense
		 * of potentially slightly increased service time for the
		 * request batch this request will be a part of.
		 */
		if (cli->cc_active == 0 && cli->cc_quantum > 0)
			cli->cc_round++;

		/** A new scheduling round has commenced */
		if (cli->cc_round < net->cn_round)
			cli->cc_round = net->cn_round;

		/** I was not the last client through here */
		if (cli->cc_sequence < net->cn_sequence)
			cli->cc_sequence = ++net->cn_sequence;
		/**
		 * Reset the quantum if we have reached the maximum quantum
		 * size for this batch, or even if we have not managed to
		 * complete a batch size up to its maximum allowed size.
		 * XXX: Accessed unlocked
		 */
		cli->cc_quantum = net->cn_quantum;
	}

	nrq->nr_u.crr.cr_round = cli->cc_round;
	nrq->nr_u.crr.cr_sequence = cli->cc_sequence;

	rc = cfs_binheap_insert(net->cn_binheap, &nrq->nr_node);
	if (rc == 0) {
		cli->cc_active++;
		if (--cli->cc_quantum == 0)
			cli->cc_round++;
	}
	return rc;
}

/**
 * Removes request \a nrq from a CRR-N \a policy instance's set of queued
 * requests.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to remove
 */
static void nrs_crrn_req_del(struct ptlrpc_nrs_policy *policy,
			     struct ptlrpc_nrs_request *nrq)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	cfs_binheap_node_t	*node;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_crrn_client, cc_res);
	net = container_of(nrs_request_resource(nrq)->res_parent,
			   struct nrs_crrn_net, cn_res);

	LASSERT(nrq->nr_u.crr.cr_round <= cli->cc_round);

	cfs_binheap_remove(net->cn_binheap, &nrq->nr_node);
	cli->cc_active--;

	node = cfs_binheap_root(net->cn_binheap);
	/**
	 * No more requests
	 */
	if (node == NULL) {
		net->cn_round++;
	} else {
		nrq = container_of(node, struct ptlrpc_nrs_request, nr_node);

		if (net->cn_round < nrq->nr_u.crr.cr_round)
			net->cn_round = nrq->nr_u.crr.cr_round;
	}
}

/**
 * Called right before the request \a nrq starts being handled by CRR-N policy
 * instance \a policy.
 *
 * \param[in] policy The policy handling the request
 * \param[in] nrq    The request being handled
 */
static void nrs_crrn_req_start(struct ptlrpc_nrs_policy *policy,
			       struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	/* NB: resource control, credits etc can be added here */
	CDEBUG(D_RPCTRACE,
	       "NRS start %s request from %s, round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_desc->pd_name,
	       libcfs_id2str(req->rq_peer), nrq->nr_u.crr.cr_round);
}

/**
 * Called right after the request \a nrq finishes being handled by CRR-N policy
 * instance \a policy.
 *
 * \param[in] policy The policy that handled the request
 * \param[in] nrq    The request that was handled
 */
static void nrs_crrn_req_stop(struct ptlrpc_nrs_policy *policy,
			      struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	CDEBUG(D_RPCTRACE,
	       "NRS stop %s request from %s, round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_desc->pd_name,
	       libcfs_id2str(req->rq_peer), nrq->nr_u.crr.cr_round);
}

/**
 * CRR-N policy operations
 */
static struct ptlrpc_nrs_pol_ops nrs_crrn_ops = {
	.op_policy_start	= nrs_crrn_start,
	.op_policy_stop		= nrs_crrn_stop,
	.op_policy_ctl		= nrs_crrn_ctl,
	.op_res_get		= nrs_crrn_res_get,
	.op_res_put		= nrs_crrn_res_put,
	.op_req_poll		= nrs_crrn_req_poll,
	.op_req_enqueue		= nrs_crrn_req_add,
	.op_req_dequeue		= nrs_crrn_req_del,
	.op_req_start		= nrs_crrn_req_start,
	.op_req_stop		= nrs_crrn_req_stop,
	.op_lprocfs_init	= nrs_crrn_lprocfs_init,
	.op_lprocfs_fini	= nrs_crrn_lprocfs_fini,
};

/**
 * CRR-N policy descriptor
 */
struct ptlrpc_nrs_pol_desc ptlrpc_nrs_crrn_desc = {
	.pd_name		= NRS_POL_NAME_CRRN,
	.pd_ops			= &nrs_crrn_ops,
	.pd_compat		= nrs_policy_compat_all,
};

/** @} CRR-N policy */

/** @} nrs */
