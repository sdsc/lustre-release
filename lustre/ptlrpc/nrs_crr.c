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
 * Request ordering in a RR manner over client NIDs
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
#include <libcfs/libcfs.h>
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
 * Uses
 * ptlrpc_nrs_request::nr_u::crr::cr_round and
 * ptlrpc_nrs_request::nr_u::crr::cr_sequence to compare two binheap nodes and
 * produce a binary predicate that shows their relative priority, so that the
 * binary heap can perform the necessary sorting operations.
 *
 * \param[in] e1 The first binheap node to compare
 * \param[in] e2 The first binheap node to compare
 *
 * \retval 0 e1 > e2
 * \retval 1 e1 < e2
 */
static int
crr_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct ptlrpc_nrs_request *nrq1;
	struct ptlrpc_nrs_request *nrq2;

	nrq1 = container_of(e1, struct ptlrpc_nrs_request, nr_node);
	nrq2 = container_of(e2, struct ptlrpc_nrs_request, nr_node);

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

/**
 * A CRR-N policy instance is started.
 *
 * \param[in] policy The policy
 *
 * \retval -ENOMEM OOM error
 * \retval 0	   success
 */
static int
nrs_crrn_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net;

	OBD_CPT_ALLOC_PTR(net, nrs_pol2cptab(policy),
			  nrs_pol2cptid(policy));
	if (net == NULL)
		return -ENOMEM;

	net->cn_binheap = cfs_binheap_create(&nrs_crr_heap_ops,
					     CBH_FLAG_ATOMIC_GROW, 4096, NULL,
					     nrs_pol2cptab(policy),
					     nrs_pol2cptid(policy));
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

/**
 * A CRR-N policy instance is stopped.
 *
 * Called when the policy has been instructed to transition to the
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED state and has no more
 * pending requests to serve.
 *
 * \param[in] policy The policy
 *
 */

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
}

/**
 * Obtains resources from CRR-N policy instances. The top-level resource lives
 * inside \e nrs_crrn_net and the second-level resource inside
 * \e nrs_crrn_client.
 */
enum nrs_resource_level
nrs_crrn_res_get(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq,
		 struct ptlrpc_nrs_resource *parent,
		 struct ptlrpc_nrs_resource **resp, bool ltd)
{
	struct nrs_crrn_net	*net;
	struct nrs_crrn_client	*cli;
	struct nrs_crrn_client	*tmp;
	struct ptlrpc_request	*req;

	if (parent == NULL) {
		*resp = &((struct nrs_crrn_net *)policy->pol_private)->cn_res;
		return NRS_RES_LVL_PARENT;
	}

	net = container_of(parent, struct nrs_crrn_net, cn_res);
	req = container_of(nrq, struct ptlrpc_request, rq_nrq);

	cli = cfs_hash_lookup(net->cn_cli_hash, &req->rq_peer.nid);
	if (cli != NULL)
		goto out;

	OBD_CPT_ALLOC_GFP(cli, nrs_pol2cptab(policy),
			  nrs_pol2cptid(policy), sizeof(*cli),
			  !ltd ? CFS_ALLOC_IO : CFS_ALLOC_ATOMIC);
	if (cli == NULL)
		return -ENOMEM;

	cli->cc_nid = req->rq_peer.nid;
	/**
	 * 1 for caller
	 */
	cfs_atomic_set(&cli->cc_ref, 1);
	tmp = cfs_hash_findadd_unique(net->cn_cli_hash,
				      &cli->cc_nid, &cli->cc_hnode);
	if (tmp != cli) {
		OBD_FREE_PTR(cli);
		cli = tmp;
	}
out:
	*resp = &cli->cc_res;
	return NRS_RES_LVL_CHILD1;
}

/**
 *
 */
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

/**
 * Obtains an NRS request from a CRR-N policy instance so that it can be
 * served.
 */
static struct ptlrpc_nrs_request *
nrs_crrn_req_poll(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_crrn_net	*net = policy->pol_private;
	cfs_binheap_node_t *node = cfs_binheap_root(net->cn_binheap);

	return node == NULL ? NULL :
	       container_of(node, struct ptlrpc_nrs_request, nr_node);
}

/**
 * Adds an NRS request to a CRR-N policy instance.
 */
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

	rc = cfs_binheap_insert(net->cn_binheap, &nrq->nr_node);
	if (rc == 0) {
		net->cn_sequence++;
		cli->cc_round++;
	}
	return rc;
}

/**
 * Deletes a request from a CRR-N policy instance.
 */
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

	cfs_binheap_remove(net->cn_binheap, &nrq->nr_node);

	node = cfs_binheap_root(net->cn_binheap);
	/**
	 * No more requests.
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
 * Called right before a request is scheduled by CRR-N policy instance.
 */
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

/**
 * Called right after a request has been scheduled by a CRR-N policy instance.
 */
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

static struct ptlrpc_nrs_pol_ops nrs_crrn_ops = {
	.op_policy_start	= nrs_crrn_start,
	.op_policy_stop		= nrs_crrn_stop,
	.op_res_get		= nrs_crrn_res_get,
	.op_res_put		= nrs_crrn_res_put,
	.op_req_poll		= nrs_crrn_req_poll,
	.op_req_enqueue		= nrs_crrn_req_add,
	.op_req_dequeue		= nrs_crrn_req_del,
	.op_req_start		= nrs_crr_req_start,
	.op_req_stop		= nrs_crr_req_stop,
};

struct ptlrpc_nrs_pol_desc ptlrpc_nrs_crrn_desc = {
	.pd_name		= "crrn",
	.pd_ops			= &nrs_crrn_ops,
	.pd_compat		= nrs_policy_compat_all,
};

/** @} CRR-N policy */

/** @} nrs */
