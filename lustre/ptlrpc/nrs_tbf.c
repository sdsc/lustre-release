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
 * Copyright (C) 2012 DataDirect Networks, Inc.
 *
 */
/*
 * lustre/ptlrpc/nrs_tbf.c
 *
 * Network Request Scheduler (NRS) Token Bucket Filter(TBF) policy
 *
 */

#ifdef HAVE_SERVER_SUPPORT

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
#include <libcfs/libcfs.h>
#include "ptlrpc_internal.h"

/**
 * \name tbf
 *
 * Token Bucket Filter over client NIDs
 *
 * @{
 */

#define NRS_POL_NAME_TBF	"tbf"

int tbf_rate = 10000;
CFS_MODULE_PARM(tbf_rate, "i", int, 0644,
		"Default rate limit in RPCs/s");
int tbf_depth = 3;
CFS_MODULE_PARM(tbf_depth, "i", int, 0644,
		"How many tokens that a client can save up");

static enum hrtimer_restart nrs_tbf_timer_cb(struct hrtimer *timer)
{
	struct nrs_tbf_head *head = container_of(timer, struct nrs_tbf_head,
						 th_timer);
	struct ptlrpc_nrs   *nrs = head->th_res.res_policy->pol_nrs;
	struct ptlrpc_service_part *svcpt = nrs->nrs_svcpt;

	cfs_atomic_set(&nrs->nrs_throttling, 0);
	cfs_waitq_signal(&svcpt->scp_waitq);

	return HRTIMER_NORESTART;
}

#define NRS_TBF_DEFAULT_RULE "default"

static void nrs_tbf_rule_fini(struct nrs_tbf_rule *rule)
{
	LASSERT(rule->tr_ref == 0);
	LASSERT(cfs_list_empty(&rule->tr_cli_list));
	LASSERT(!cfs_list_empty(&rule->tr_linkage));
	cfs_list_del_init(&rule->tr_linkage);
	if (!cfs_list_empty(&rule->tr_nids))
		cfs_free_nidlist(&rule->tr_nids);
	LASSERT(rule->tr_nids_str);
	OBD_FREE(rule->tr_nids_str, strlen(rule->tr_nids_str) + 1);
	OBD_FREE_PTR(rule);
}

/**
 * Decreases the rule's usage reference count, and stops the rule in case it
 * was already stopping and have no more outstanding usage references (which
 * indicates it has no more queued or started requests, and can be safely
 * stopped).
 */
static void nrs_tbf_rule_put(struct nrs_tbf_rule *rule)
{
	LASSERT(rule->tr_ref > 0);

	rule->tr_ref--;
	if (unlikely(rule->tr_ref == 0 &&
	    rule->tr_state == NTRS_STOPPING))
		nrs_tbf_rule_fini(rule);
}

/**
 * Increases the rule's usage reference count.
 */
static inline void nrs_tbf_rule_get(struct nrs_tbf_rule *rule)
{
	rule->tr_ref++;
}

static void
nrs_tbf_cli_rule_put(struct nrs_tbf_client *cli)
{
	LASSERT(!cfs_list_empty(&cli->tc_linkage));
	LASSERT(cli->tc_rule);
	cfs_list_del_init(&cli->tc_linkage);
	nrs_tbf_rule_put(cli->tc_rule);
	cli->tc_rule = NULL;
}

static void
nrs_tbf_cli_fini(struct nrs_tbf_client *cli)
{
	LASSERT(cfs_list_empty(&cli->tc_list));
	LASSERT(!cli->tc_in_heap);
	LASSERT(cfs_atomic_read(&cli->tc_ref) == 0);
	nrs_tbf_cli_rule_put(cli);
	OBD_FREE_PTR(cli);
}

static void
nrs_tbf_cli_reset_value(struct nrs_tbf_head *head,
			struct nrs_tbf_rule *rule,
			struct nrs_tbf_client *cli)

{
	spin_lock(&head->th_binheap_lock);
	cli->tc_rpc_rate = rule->tr_rpc_rate;
	cli->tc_nsecs = rule->tr_nsecs;
	cli->tc_depth = rule->tr_depth;
	cli->tc_ntoken = rule->tr_depth;
	cli->tc_check_time = ktime_to_ns(ktime_get());
	cli->tc_rule_sequence = head->th_rule_sequence;
	cli->tc_rule_generation = rule->tr_generation;
	if (cli->tc_in_heap) {
		cfs_binheap_relocate(head->th_binheap,
				     &cli->tc_node);
	}
	spin_unlock(&head->th_binheap_lock);
}

static void
nrs_tbf_cli_reset(struct nrs_tbf_head *head,
		  struct nrs_tbf_rule *rule,
		  struct nrs_tbf_client *cli)
{
	if (!cfs_list_empty(&cli->tc_linkage)) {
		LASSERT(rule != cli->tc_rule);
		nrs_tbf_cli_rule_put(cli);
	}
	LASSERT(cli->tc_rule == NULL);
	LASSERT(cfs_list_empty(&cli->tc_linkage));
	/* Rule's ref is added before called */
	cli->tc_rule = rule;
	cfs_list_add_tail(&cli->tc_linkage, &rule->tr_cli_list);
	nrs_tbf_cli_reset_value(head, rule, cli);
}

static int
nrs_tbf_rule_dump(struct nrs_tbf_rule *rule, char *buff, int length)
{
	return snprintf(buff, length, "%s {%s} %llu, ref %ld, %s\n",
			rule->tr_name,
			rule->tr_nids_str,
			rule->tr_rpc_rate,
			rule->tr_ref - 1,
			(rule->tr_state & NTRS_STOPPING)
			 ? "stopping" : "active");
}

static int
nrs_tbf_rule_dump_all(struct nrs_tbf_head *head, char *buff, int length)
{
	struct nrs_tbf_rule *rule;
	cfs_list_t *pos;
	int rc = 0;

	LASSERT(head != NULL);
	/* List the rules from newest to oldest */
	cfs_list_for_each(pos, &head->th_list) {
		rule = cfs_list_entry(pos, struct nrs_tbf_rule, tr_linkage);
		rc += nrs_tbf_rule_dump(rule, buff + rc, length - rc);
	}

	return rc;
}

static struct nrs_tbf_rule *
nrs_tbf_rule_find(struct nrs_tbf_head *head,
		  const char *name)
{
	struct nrs_tbf_rule *rule;
	cfs_list_t *pos;

	LASSERT(head != NULL);
	cfs_list_for_each(pos, &head->th_list) {
		rule = cfs_list_entry(pos, struct nrs_tbf_rule, tr_linkage);
		if ((rule->tr_state & NTRS_STOPPING) == 0 &&
		    strlen(name) == strlen(rule->tr_name) &&
		    strcmp(rule->tr_name, name) == 0) {
			nrs_tbf_rule_get(rule);
			return rule;
		}
	}
	return NULL;
}

static struct nrs_tbf_rule  *
nrs_tbf_match_rule(struct nrs_tbf_head *head,
		   lnet_nid_t nid)
{
	struct nrs_tbf_rule *rule = NULL;
	cfs_list_t	    *tmp;

	/* Match the newest rule in the list */
	cfs_list_for_each(tmp, &head->th_list) {
		struct nrs_tbf_rule *tmp_rule = cfs_list_entry(tmp,
						struct nrs_tbf_rule,
						tr_linkage);
		if ((tmp_rule->tr_state & NTRS_STOPPING) == 0 &&
		    cfs_match_nid(nid, &tmp_rule->tr_nids)) {
			rule = tmp_rule;
			break;
		}
	}

	if (rule == NULL)
		rule = head->th_rule;

	nrs_tbf_rule_get(rule);
	return rule;
}

static struct nrs_tbf_rule *
nrs_tbf_rule_start(struct ptlrpc_nrs_policy *policy,
		   struct nrs_tbf_head *head,
		   struct nrs_tbf_cmd *start)
{
	struct nrs_tbf_rule *rule;
	int rc = 0;

	rule = nrs_tbf_rule_find(head, start->tc_name);
	if (rule) {
		nrs_tbf_rule_put(rule);
		GOTO(out, rc = -EEXIST);
	}

	OBD_CPT_ALLOC_PTR(rule, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (rule == NULL)
		GOTO(out, rc = -ENOMEM);

	memcpy(rule->tr_name, start->tc_name, strlen(start->tc_name));
	rule->tr_rpc_rate = start->tc_rpc_rate;
	rule->tr_nsecs = NSEC_PER_SEC / rule->tr_rpc_rate;
	rule->tr_depth = tbf_depth;
	CFS_INIT_LIST_HEAD(&rule->tr_cli_list);
	CFS_INIT_LIST_HEAD(&rule->tr_nids);

	LASSERT(start->tc_nids_str);
	OBD_CPT_ALLOC(rule->tr_nids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      strlen(start->tc_nids_str) + 1);
	if (rule->tr_nids_str == NULL)
		GOTO(out_free_rule, rc = -ENOMEM);
	memcpy(rule->tr_nids_str,
	       start->tc_nids_str,
	       strlen(start->tc_nids_str));

	if (!cfs_list_empty(&start->tc_nids)) {
		if (cfs_parse_nidlist(rule->tr_nids_str,
				      strlen(rule->tr_nids_str),
				      &rule->tr_nids) <= 0) {
			CERROR("nids {%s} illegal\n",
			       rule->tr_nids_str);
			LBUG();
			GOTO(out_free_nid, rc = -EINVAL);
		}
	}

	nrs_tbf_rule_get(rule);
	/* Add as the newest rule */
	cfs_list_add(&rule->tr_linkage, &head->th_list);
	head->th_rule_sequence++;
	goto out;
out_free_nid:
	OBD_FREE(rule->tr_nids_str,
		 strlen(start->tc_nids_str) + 1);
out_free_rule:
	OBD_FREE_PTR(rule);
out:
	if (rc)
		rule = ERR_PTR(rc);
	return rule;
}

static int
nrs_tbf_rule_change(struct ptlrpc_nrs_policy *policy,
		     struct nrs_tbf_head *head,
		     struct nrs_tbf_cmd *change)
{
	struct nrs_tbf_rule *rule;
	int rc = 0;

	rule = nrs_tbf_rule_find(head, change->tc_name);
	if (rule == NULL)
		GOTO(out, rc = -ENOENT);

	rule->tr_rpc_rate = change->tc_rpc_rate;
	rule->tr_nsecs = NSEC_PER_SEC / rule->tr_rpc_rate;
	rule->tr_generation++;
	nrs_tbf_rule_put(rule);
out:
	return rc;
}

static int
nrs_tbf_rule_stop(struct ptlrpc_nrs_policy *policy,
		  struct nrs_tbf_head *head,
		  struct nrs_tbf_cmd *stop)
{
	struct nrs_tbf_rule *rule;
	int rc = 0;

	if (strlen(stop->tc_name) == strlen(NRS_TBF_DEFAULT_RULE) &&
	    strcmp(stop->tc_name, NRS_TBF_DEFAULT_RULE) == 0)
		GOTO(out, rc = -EPERM);

	rule = nrs_tbf_rule_find(head, stop->tc_name);
	if (rule == NULL)
		GOTO(out, rc = -ENOENT);

	rule->tr_state |= NTRS_STOPPING;
	nrs_tbf_rule_put(rule);
	nrs_tbf_rule_put(rule);
out:
	return rc;
}

static int
nrs_tbf_command(struct ptlrpc_nrs_policy *policy,
		struct nrs_tbf_head *head,
		struct nrs_tbf_cmd *cmd)
{
	struct nrs_tbf_rule *rule;
	int rc = 0;

	if (cmd->tc_cmd == NRS_CTL_TBF_START_RULE) {
		rule = nrs_tbf_rule_start(policy, head, cmd);
		if (IS_ERR(rule))
			rc = PTR_ERR(rule);
	} else if (cmd->tc_cmd == NRS_CTL_TBF_CHANGE_RATE) {
		rc = nrs_tbf_rule_change(policy, head, cmd);
	} else if (cmd->tc_cmd == NRS_CTL_TBF_STOP_RULE) {
		rc = nrs_tbf_rule_stop(policy, head, cmd);
	} else {
		rc = -EFAULT;
	}

	return rc;
}

/**
 * Binary heap predicate.
 *
 * \param[in] e1 the first binheap node to compare
 * \param[in] e2 the second binheap node to compare
 *
 * \retval 0 e1 > e2
 * \retval 1 e1 < e2
 */
static int tbf_cli_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct nrs_tbf_client *cli1;
	struct nrs_tbf_client *cli2;

	cli1 = container_of(e1, struct nrs_tbf_client, tc_node);
	cli2 = container_of(e2, struct nrs_tbf_client, tc_node);

	if (cli1->tc_check_time + cli1->tc_nsecs <
	    cli2->tc_check_time + cli2->tc_nsecs)
		return 1;
	else if (cli1->tc_check_time + cli1->tc_nsecs >
		 cli2->tc_check_time + cli2->tc_nsecs)
		return 0;

	if (cli1->tc_check_time < cli2->tc_check_time)
		return 1;
	else if (cli1->tc_check_time > cli2->tc_check_time)
		return 0;

	/* Maybe need more comparasion, e.g. request number in the rules */
	return 1;
}

/**
 * TBF binary heap operations
 */
static cfs_binheap_ops_t nrs_tbf_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= tbf_cli_compare,
};


/**
 * libcfs_hash operations for nrs_tbf_net::cn_cli_hash
 *
 * This uses ptlrpc_request::rq_peer.nid as its key, in order to hash
 * nrs_tbf_client objects.
 */
#define NRS_TBF_NID_BKT_BITS    8
#define NRS_TBF_NID_BITS        16

static unsigned nrs_tbf_hop_hash(cfs_hash_t *hs, const void *key,
				  unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(lnet_nid_t), mask);
}

static int nrs_tbf_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	lnet_nid_t	      *nid = (lnet_nid_t *)key;
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);
	return *nid == cli->tc_nid;
}

static void *nrs_tbf_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);
	return &cli->tc_nid;
}

static void *nrs_tbf_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_tbf_client, tc_hnode);
}

static void nrs_tbf_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);
	cfs_atomic_inc(&cli->tc_ref);
}

static void nrs_tbf_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);
	cfs_atomic_dec(&cli->tc_ref);
}

static void nrs_tbf_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);
	LASSERTF(cfs_atomic_read(&cli->tc_ref) == 0,
		 "Busy TBF object from client with NID %s, with %d refs\n",
		 libcfs_nid2str(cli->tc_nid), cfs_atomic_read(&cli->tc_ref));

	nrs_tbf_cli_fini(cli);
}

static cfs_hash_ops_t nrs_tbf_hash_ops = {
	.hs_hash	= nrs_tbf_hop_hash,
	.hs_keycmp	= nrs_tbf_hop_keycmp,
	.hs_key		= nrs_tbf_hop_key,
	.hs_object	= nrs_tbf_hop_object,
	.hs_get		= nrs_tbf_hop_get,
	.hs_put		= nrs_tbf_hop_put,
	.hs_put_locked	= nrs_tbf_hop_put,
	.hs_exit	= nrs_tbf_hop_exit,
};

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
static int nrs_tbf_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_tbf_head  *head;
	struct nrs_tbf_rule *rule;
	struct nrs_tbf_cmd    start;
	int rc = 0;

	OBD_CPT_ALLOC_PTR(head, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (head == NULL)
		GOTO(out, rc = -ENOMEM);

	head->th_binheap = cfs_binheap_create(&nrs_tbf_heap_ops,
					      CBH_FLAG_ATOMIC_GROW, 4096, NULL,
					      nrs_pol2cptab(policy),
					      nrs_pol2cptid(policy));
	if (head->th_binheap == NULL)
		GOTO(out_free_head, rc = -ENOMEM);

	head->th_cli_hash = cfs_hash_create("nrs_tbf_nid_hash",
					      NRS_TBF_NID_BITS,
					      NRS_TBF_NID_BITS,
					      NRS_TBF_NID_BKT_BITS, 0,
					      CFS_HASH_MIN_THETA,
					      CFS_HASH_MAX_THETA,
					      &nrs_tbf_hash_ops,
					      CFS_HASH_RW_BKTLOCK);
	if (head->th_cli_hash == NULL)
		GOTO(out_free_heap, rc = -ENOMEM);

	spin_lock_init(&head->th_binheap_lock);
	CFS_INIT_LIST_HEAD(&head->th_list);
	hrtimer_init(&head->th_timer, CLOCK_MONOTONIC, HRTIMER_MODE_ABS);
	head->th_timer.function = nrs_tbf_timer_cb;

	OBD_CPT_ALLOC(start.tc_nids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      2);
	if (start.tc_nids_str == NULL)
		GOTO(out_free_hash, rc = -ENOMEM);
	start.tc_nids_str[0] = '*';

	start.tc_rpc_rate = tbf_rate;
	start.tc_name = NRS_TBF_DEFAULT_RULE;
	CFS_INIT_LIST_HEAD(&start.tc_nids);
	rule = nrs_tbf_rule_start(policy, head, &start);
	OBD_FREE(start.tc_nids_str, 2);
	if (IS_ERR(rule))
		GOTO(out_free_hash, rc = PTR_ERR(rule));

	head->th_rule = rule;
	policy->pol_private = head;
	goto out;
out_free_hash:
	cfs_hash_putref(head->th_cli_hash);
out_free_heap:
	cfs_binheap_destroy(head->th_binheap);
out_free_head:
	OBD_FREE_PTR(head);
out:
	return rc;
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
static void nrs_tbf_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_tbf_head *head = policy->pol_private;
	struct nrs_tbf_rule *rule;
	cfs_list_t *l, *tmp;

	LASSERT(head != NULL);
	LASSERT(head->th_cli_hash != NULL);
	hrtimer_cancel(&head->th_timer);
	/* Should cleanup hash first before free rules */
	cfs_hash_putref(head->th_cli_hash);
	cfs_list_for_each_safe(l, tmp, &head->th_list) {
		rule = cfs_list_entry(l, struct nrs_tbf_rule, tr_linkage);
		nrs_tbf_rule_put(rule);
	}
	LASSERT(head->th_binheap != NULL);
	LASSERT(cfs_binheap_is_empty(head->th_binheap));
	cfs_binheap_destroy(head->th_binheap);
	OBD_FREE_PTR(head);
	cfs_atomic_set(&policy->pol_nrs->nrs_throttling, 0);
}

/**
 * Performs a policy-specific ctl function on TBF policy instances; similar
 * to ioctl.
 *
 * \param[in]	  policy the policy instance
 * \param[in]	  opc	 the opcode
 * \param[in,out] arg	 used for passing parameters and information
 *
 * \pre spin_is_locked(&policy->pol_nrs->->nrs_lock)
 * \post spin_is_locked(&policy->pol_nrs->->nrs_lock)
 *
 * \retval 0   operation carried out successfully
 * \retval -ve error
 */
int nrs_tbf_ctl(struct ptlrpc_nrs_policy *policy, enum ptlrpc_nrs_ctl opc,
		void *arg)
{
	int rc = 0;
	ENTRY;

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	switch (opc) {
	default:
		RETURN(-EINVAL);

	/**
	 * Read RPC rate size of a policy instance.
	 */
	case NRS_CTL_TBF_RD_RULE: {
		struct nrs_tbf_head *head = policy->pol_private;
		struct ptlrpc_service_part *svcpt;
		struct nrs_tbf_dump *dump;
		int length;

		dump = (struct nrs_tbf_dump *)arg;

		svcpt = policy->pol_nrs->nrs_svcpt;
		length = snprintf(dump->td_buff, dump->td_size,
				  "CPT %d:\n",
				  svcpt->scp_cpt);
		dump->td_length += length;
		dump->td_buff += length;
		dump->td_size -= length;

		length = nrs_tbf_rule_dump_all(head,
					       dump->td_buff,
					       dump->td_size);
		dump->td_length += length;
		dump->td_buff += length;
		dump->td_size -= length;
		}
		break;

	/**
	 * Write RPC rate of a policy instance.
	 */
	case NRS_CTL_TBF_WR_RULE: {
		struct nrs_tbf_head *head = policy->pol_private;
		struct nrs_tbf_cmd *cmd;

		cmd = (struct nrs_tbf_cmd *)arg;
		rc = nrs_tbf_command(policy,
				     head,
				     cmd);
		}
		break;
	}

	RETURN(rc);
}

/**
 * Is called for obtaining a TBF policy resource.
 *
 * \param[in]  policy	  The policy on which the request is being asked for
 * \param[in]  nrq	  The request for which resources are being taken
 * \param[in]  parent	  Parent resource, unused in this policy
 * \param[out] resp	  Resources references are placed in this array
 * \param[in]  moving_req Signifies limited caller context; unused in this
 *			  policy
 *
 *
 * \see nrs_resource_get_safe()
 */
static int nrs_tbf_res_get(struct ptlrpc_nrs_policy *policy,
			   struct ptlrpc_nrs_request *nrq,
			   const struct ptlrpc_nrs_resource *parent,
			   struct ptlrpc_nrs_resource **resp,
			   bool moving_req)
{
	struct nrs_tbf_head   *head;
	struct nrs_tbf_client *cli;
	struct nrs_tbf_client *tmp;
	struct nrs_tbf_rule   *rule;
	struct ptlrpc_request *req;

	if (parent == NULL) {
		*resp = &((struct nrs_tbf_head *)policy->pol_private)->th_res;
		return 0;
	}

	head = container_of(parent, struct nrs_tbf_head, th_res);
	req = container_of(nrq, struct ptlrpc_request, rq_nrq);

	cli = cfs_hash_lookup(head->th_cli_hash, &req->rq_peer.nid);
	if (cli != NULL) {
		LASSERT(cli->tc_rule);
		if (cli->tc_rule_sequence != head->th_rule_sequence ||
		    cli->tc_rule->tr_state & NTRS_STOPPING) {
			rule = nrs_tbf_match_rule(head, cli->tc_nid);
			if (rule != cli->tc_rule) {
				nrs_tbf_cli_reset(head, rule, cli);
			} else {
				LASSERT(cli->tc_rule_sequence !=
					head->th_rule_sequence);
				nrs_tbf_rule_put(rule);
			}
		} else if (cli->tc_rule_generation !=
			   cli->tc_rule->tr_generation) {
			nrs_tbf_cli_reset_value(head, cli->tc_rule, cli);
		}
		goto out;
	}

	OBD_CPT_ALLOC_GFP(cli, nrs_pol2cptab(policy), nrs_pol2cptid(policy),
			  sizeof(*cli), moving_req ? CFS_ALLOC_ATOMIC :
			  CFS_ALLOC_IO);
	if (cli == NULL)
		return -ENOMEM;
	cli->tc_in_heap = false;
	cli->tc_nid = req->rq_peer.nid;
	CFS_INIT_LIST_HEAD(&cli->tc_list);
	CFS_INIT_LIST_HEAD(&cli->tc_linkage);
	cfs_atomic_set(&cli->tc_ref, 1);
	rule = nrs_tbf_match_rule(head, cli->tc_nid);
	nrs_tbf_cli_reset(head, rule, cli);
	tmp = cfs_hash_findadd_unique(head->th_cli_hash, &cli->tc_nid,
				      &cli->tc_hnode);
	if (tmp != cli) {
		nrs_tbf_cli_fini(cli);
		cli = tmp;
	}
out:
	*resp = &cli->tc_res;

	return 1;
}

/**
 * Called when releasing references to the resource hierachy obtained for a
 * request for scheduling using the TBF policy.
 *
 * \param[in] policy   the policy the resource belongs to
 * \param[in] res      the resource to be released
 */
static void nrs_tbf_res_put(struct ptlrpc_nrs_policy *policy,
			    const struct ptlrpc_nrs_resource *res)
{
	struct nrs_tbf_head   *head;
	struct nrs_tbf_client *cli;

	/**
	 * Do nothing for freeing parent, nrs_tbf_net resources
	 */
	if (res->res_parent == NULL)
		return;

	cli = container_of(res, struct nrs_tbf_client, tc_res);
	head = container_of(res->res_parent, struct nrs_tbf_head, th_res);

	cfs_hash_put(head->th_cli_hash, &cli->tc_hnode);
}

/**
 * Called when getting a request from the TBF policy for handling, or just
 * peeking; removes the request from the policy when it is to be handled.
 *
 * \param[in] policy The policy
 * \param[in] peek   When set, signifies that we just want to examine the
 *		     request, and not handle it, so the request is not removed
 *		     from the policy.
 * \param[in] force  Force the policy to return a request; unused in this
 *		     policy
 *
 * \retval The request to be handled; this is the next request in the TBF
 *	   rule
 *
 * \see ptlrpc_nrs_req_get_nolock()
 * \see nrs_request_get()
 */
static
struct ptlrpc_nrs_request *nrs_tbf_req_get(struct ptlrpc_nrs_policy *policy,
					   bool peek, bool force)
{
	struct nrs_tbf_head	  *head = policy->pol_private;
	struct ptlrpc_nrs_request *nrq = NULL;
	struct nrs_tbf_client     *cli;
	cfs_binheap_node_t	  *node;

	if (!peek && cfs_atomic_read(&policy->pol_nrs->nrs_throttling))
		return NULL;

	spin_lock(&head->th_binheap_lock);
	node = cfs_binheap_root(head->th_binheap);
	if (unlikely(node == NULL))
		return NULL;

	cli = container_of(node, struct nrs_tbf_client, tc_node);
	LASSERT(cli->tc_in_heap);
	if (peek) {
		nrq = cfs_list_entry(cli->tc_list.next,
				     struct ptlrpc_nrs_request,
				     nr_u.tbf.tr_list);
	} else {
		__u64 now = ktime_to_ns(ktime_get());
		__u64 passed;
		long  ntoken;
		__u64 deadline;

		deadline = cli->tc_check_time +
			  cli->tc_nsecs;
		LASSERT(now >= cli->tc_check_time);
		passed = now - cli->tc_check_time;
		ntoken = (passed * cli->tc_rpc_rate) / NSEC_PER_SEC;
		ntoken += cli->tc_ntoken;
		if (ntoken > cli->tc_depth)
			ntoken = cli->tc_depth;
		if (ntoken > 0) {
			struct ptlrpc_request *req;
			nrq = cfs_list_entry(cli->tc_list.next,
					     struct ptlrpc_nrs_request,
					     nr_u.tbf.tr_list);
			req = container_of(nrq,
					   struct ptlrpc_request,
					   rq_nrq);
			ntoken--;
			cli->tc_ntoken = ntoken;
			cli->tc_check_time = now;
			cfs_list_del_init(&nrq->nr_u.tbf.tr_list);
			if (cfs_list_empty(&cli->tc_list)) {
				cfs_binheap_remove(head->th_binheap,
						   &cli->tc_node);
				cli->tc_in_heap = false;
			} else {
				cfs_binheap_relocate(head->th_binheap,
						     &cli->tc_node);
			}
			CDEBUG(D_RPCTRACE,
			       "NRS start %s request from %s, "
			       "seq: "LPU64"\n",
			       policy->pol_desc->pd_name,
			       libcfs_id2str(req->rq_peer),
			       nrq->nr_u.tbf.tr_sequence);
		} else {
			ktime_t time;

			cfs_atomic_set(&policy->pol_nrs->nrs_throttling, 1);
			head->th_deadline = deadline;
			time = ktime_set(0, 0);
			time = ktime_add_ns(time, deadline);
			hrtimer_start(&head->th_timer, time, HRTIMER_MODE_ABS);
		}
	}
	spin_unlock(&head->th_binheap_lock);

	return nrq;
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
static int nrs_tbf_req_add(struct ptlrpc_nrs_policy *policy,
			   struct ptlrpc_nrs_request *nrq)
{
	struct nrs_tbf_head   *head;
	struct nrs_tbf_client *cli;
	int		       rc = 0;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_tbf_client, tc_res);
	head = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_tbf_head, th_res);
	if (cfs_list_empty(&cli->tc_list)) {
		LASSERT(!cli->tc_in_heap);
		spin_lock(&head->th_binheap_lock);
		rc = cfs_binheap_insert(head->th_binheap, &cli->tc_node);
		spin_unlock(&head->th_binheap_lock);
		if (rc == 0) {
			cli->tc_in_heap = true;
			nrq->nr_u.tbf.tr_sequence = head->th_sequence++;
			cfs_list_add_tail(&nrq->nr_u.tbf.tr_list,
					  &cli->tc_list);
			if (cfs_atomic_read(&policy->pol_nrs->nrs_throttling)) {
				__u64 deadline = cli->tc_check_time +
						 cli->tc_nsecs;
				if ((head->th_deadline > deadline) &&
				    (hrtimer_try_to_cancel(&head->th_timer)
				     >= 0)) {
					ktime_t time;
					head->th_deadline = deadline;
					time = ktime_set(0, 0);
					time = ktime_add_ns(time, deadline);
					hrtimer_start(&head->th_timer, time,
						      HRTIMER_MODE_ABS);
				}
			}
		}
	} else {
		LASSERT(cli->tc_in_heap);
		nrq->nr_u.tbf.tr_sequence = head->th_sequence++;
		cfs_list_add_tail(&nrq->nr_u.tbf.tr_list,
				  &cli->tc_list);
	}
	return rc;
}

/**
 * Removes request \a nrq from \a policy's list of queued requests.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to remove
 */
static void nrs_tbf_req_del(struct ptlrpc_nrs_policy *policy,
			     struct ptlrpc_nrs_request *nrq)
{
	struct nrs_tbf_head   *head;
	struct nrs_tbf_client *cli;

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_tbf_client, tc_res);
	head = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_tbf_head, th_res);

	LASSERT(!cfs_list_empty(&nrq->nr_u.tbf.tr_list));
	cfs_list_del_init(&nrq->nr_u.tbf.tr_list);
	if (cfs_list_empty(&cli->tc_list)) {
		spin_lock(&head->th_binheap_lock);
		cfs_binheap_remove(head->th_binheap,
				   &cli->tc_node);
		cli->tc_in_heap = false;
		spin_unlock(&head->th_binheap_lock);
	} else {
		spin_lock(&head->th_binheap_lock);
		cfs_binheap_relocate(head->th_binheap,
				     &cli->tc_node);
		spin_unlock(&head->th_binheap_lock);
	}
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
static void nrs_tbf_req_stop(struct ptlrpc_nrs_policy *policy,
			      struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
						  rq_nrq);

	CDEBUG(D_RPCTRACE, "NRS stop %s request from %s, seq: "LPU64"\n",
	       policy->pol_desc->pd_name, libcfs_id2str(req->rq_peer),
	       nrq->nr_u.tbf.tr_sequence);
}

#ifdef LPROCFS

/**
 * lprocfs interface
 */

/**
 * The maximum RPC rate.
 */
#define LPROCFS_NRS_RATE_MAX		65535

static int ptlrpc_lprocfs_rd_nrs_tbf_rule(char *page, char **start,
					   off_t off, int count, int *eof,
					   void *data)
{
	struct ptlrpc_service	    *svc = data;
	int			     rc;
	int			     rc2;
	struct nrs_tbf_dump	     dump;

	rc2 = snprintf(page, count, "regular_requests:\n");
	/**
	 * Perform two separate calls to this as only one of the NRS heads'
	 * policies may be in the ptlrpc_nrs_pol_state::NRS_POL_STATE_STARTED or
	 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPING state.
	 */
	dump.td_length = 0;
	dump.td_buff = page + rc2;
	dump.td_size = count - rc2;
	rc = ptlrpc_nrs_policy_control(svc, PTLRPC_NRS_QUEUE_REG,
				       NRS_POL_NAME_TBF,
				       NRS_CTL_TBF_RD_RULE,
				       false, &dump);
	if (rc == 0) {
		*eof = 1;
		rc2 += dump.td_length;
		/**
		 * Ignore -ENODEV as the regular NRS head's policy may be in the
		 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED state.
		 */
	} else if (rc != -ENODEV) {
		return rc;
	}

	if (!nrs_svc_has_hp(svc))
		goto no_hp;

	rc2 += snprintf(page + rc2, count - rc2, "high_priority_requests:\n");
	dump.td_length = 0;
	dump.td_buff = page + rc2;
	dump.td_size = count - rc2;
	rc = ptlrpc_nrs_policy_control(svc, PTLRPC_NRS_QUEUE_HP,
				       NRS_POL_NAME_TBF,
				       NRS_CTL_TBF_RD_RULE,
				       false, &dump);
	if (rc == 0) {
		*eof = 1;
		rc2 += dump.td_length;
		/**
		 * Ignore -ENODEV as the high priority NRS head's policy may be
		 * in the ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED state.
		 */
	} else if (rc != -ENODEV) {
		return rc;
	}

no_hp:

	return rc2 ? : rc;
}

static struct nrs_tbf_cmd *
nrs_tbf_parse_cmd(char *buffer, unsigned long count)
{
	static struct nrs_tbf_cmd *cmd;
	char			  *token;
	char			  *val;
	int			   i;
	int			   rc = 0;

	OBD_ALLOC_PTR(cmd);
	if (cmd == NULL)
		GOTO(out, rc = -ENOMEM);

	CFS_INIT_LIST_HEAD(&cmd->tc_nids);

	val = buffer;
	token = strsep(&val, " ");
	if (val == NULL || strlen(val) == 0)
		GOTO(out_free_cmd, rc = -EINVAL);

	/* Type of the command */
	if (strcmp(token, "start") == 0)
		cmd->tc_cmd = NRS_CTL_TBF_START_RULE;
	else if (strcmp(token, "stop") == 0)
		cmd->tc_cmd = NRS_CTL_TBF_STOP_RULE;
	else if (strcmp(token, "change") == 0)
		cmd->tc_cmd = NRS_CTL_TBF_CHANGE_RATE;
	else
		GOTO(out_free_cmd, rc = -EINVAL);

	/* Name of the rule */
	token = strsep(&val, " ");
	if (val == NULL) {
		/**
		 * Stop comand only need name argument,
		 * But other commands need NID or rate argument.
		 */
		if (cmd->tc_cmd != NRS_CTL_TBF_STOP_RULE)
			GOTO(out_free_cmd, rc = -EINVAL);
	}

	for (i = 0; i < strlen(token); i++) {
		if ((!isalnum(token[i])) &&
		    (token[i] != '_'))
			GOTO(out_free_cmd, rc = -EINVAL);
	}
	cmd->tc_name = token;

	if (cmd->tc_cmd == NRS_CTL_TBF_START_RULE) {
		/* List of NID */
		LASSERT(val);
		token = strsep(&val, "}");
		if (val == NULL)
			GOTO(out_free_cmd, rc = -EINVAL);

		if (strlen(token) <= 1 ||
		    token[0] != '{')
			GOTO(out_free_cmd, rc = -EINVAL);
		token++;
		OBD_ALLOC(cmd->tc_nids_str, strlen(token) + 1);
		if (cmd->tc_nids_str == NULL)
			GOTO(out_free_cmd, rc = -EINVAL);

		memcpy(cmd->tc_nids_str, token, strlen(token));

		/* parse NID list */
		if (cfs_parse_nidlist(token,
				      strlen(token),
				      &cmd->tc_nids) <= 0)
			GOTO(out_free_nid, rc = -EINVAL);

		if (val[0] == '\0')
			val = NULL;
		else if (val[0] == ' ')
			val++;
	}

	if (val != NULL) {
		if (cmd->tc_cmd == NRS_CTL_TBF_STOP_RULE ||
		    strlen(val) == 0 || !isdigit(val[0]))
			GOTO(out_free_nid, rc = -EINVAL);

		cmd->tc_rpc_rate = simple_strtoull(val, NULL, 10);
		if (cmd->tc_rpc_rate <= 0 ||
		    cmd->tc_rpc_rate >= LPROCFS_NRS_RATE_MAX)
			GOTO(out_free_nid, rc = -EINVAL);
	} else {
		if (cmd->tc_cmd == NRS_CTL_TBF_CHANGE_RATE)
			GOTO(out_free_nid, rc = -EINVAL);
		/* No RPC rate given */
		cmd->tc_rpc_rate = tbf_rate;
	}
	goto out;
out_free_nid:
	if (!cfs_list_empty(&cmd->tc_nids))
		cfs_free_nidlist(&cmd->tc_nids);
	if (cmd->tc_nids_str)
		OBD_FREE(cmd->tc_nids_str, strlen(cmd->tc_nids_str) + 1);
out_free_cmd:
	OBD_FREE_PTR(cmd);
out:
	if (rc)
		cmd = ERR_PTR(rc);
	return cmd;
}

extern struct nrs_core nrs_core;
#define LPROCFS_WR_NRS_TBF_MAX_CMD (4096)
static int ptlrpc_lprocfs_wr_nrs_tbf_rule(struct file *file,
					  const char *buffer,
					  unsigned long count, void *data)
{
	struct ptlrpc_service	  *svc = data;
	char			  *kernbuf;
	char			  *val;
	int			   rc;
	static struct nrs_tbf_cmd *cmd;
	enum ptlrpc_nrs_queue_type queue = PTLRPC_NRS_QUEUE_BOTH;
	unsigned long		   length;
	char			  *token;

	OBD_ALLOC(kernbuf, LPROCFS_WR_NRS_TBF_MAX_CMD);
	if (kernbuf == NULL)
		GOTO(out, rc = -ENOMEM);

	if (count > LPROCFS_WR_NRS_TBF_MAX_CMD - 1)
		GOTO(out_free_kernbuff, rc = -EINVAL);

	if (cfs_copy_from_user(kernbuf, buffer, count))
		GOTO(out_free_kernbuff, rc = -EFAULT);

	kernbuf[count] = '\0';

	val = kernbuf;
	token = strsep(&val, " ");
	if (val == NULL)
		GOTO(out_free_kernbuff, rc = -EINVAL);

	if (strcmp(token, "reg") == 0) {
		queue = PTLRPC_NRS_QUEUE_REG;
	} else if (strcmp(token, "hp") == 0) {
		queue = PTLRPC_NRS_QUEUE_HP;
	} else {
		kernbuf[strlen(token)] = ' ';
		val = kernbuf;
	}
	length = strlen(val);

	if (length == 0)
		GOTO(out_free_kernbuff, rc = -EINVAL);

	if (queue == PTLRPC_NRS_QUEUE_HP && !nrs_svc_has_hp(svc))
		GOTO(out_free_kernbuff, rc = -ENODEV);
	else if (queue == PTLRPC_NRS_QUEUE_BOTH && !nrs_svc_has_hp(svc))
		queue = PTLRPC_NRS_QUEUE_REG;

	cmd = nrs_tbf_parse_cmd(val, length);
	if (IS_ERR(cmd))
		GOTO(out_free_kernbuff, rc = PTR_ERR(cmd));

	/**
	 * Serialize NRS core lprocfs operations with policy registration/
	 * unregistration.
	 */
	mutex_lock(&nrs_core.nrs_mutex);
	rc = ptlrpc_nrs_policy_control(svc, queue,
				       NRS_POL_NAME_TBF,
				       NRS_CTL_TBF_WR_RULE,
				       false, cmd);
	mutex_unlock(&nrs_core.nrs_mutex);

	if (!cfs_list_empty(&cmd->tc_nids))
		cfs_free_nidlist(&cmd->tc_nids);
	if (cmd->tc_nids_str)
		OBD_FREE(cmd->tc_nids_str, strlen(cmd->tc_nids_str) + 1);
	OBD_FREE_PTR(cmd);
out_free_kernbuff:
	OBD_FREE(kernbuf, LPROCFS_WR_NRS_TBF_MAX_CMD);
out:
	return rc ? rc : count;
}


/**
 * Initializes a TBF policy's lprocfs interface for service \a svc
 *
 * \param[in] svc the service
 *
 * \retval 0	success
 * \retval != 0	error
 */
int nrs_tbf_lprocfs_init(struct ptlrpc_service *svc)
{
	int	rc;
	struct lprocfs_vars nrs_tbf_lprocfs_vars[] = {
		{ .name		= "nrs_tbf_rule",
		  .read_fptr	= ptlrpc_lprocfs_rd_nrs_tbf_rule,
		  .write_fptr	= ptlrpc_lprocfs_wr_nrs_tbf_rule,
		  .data = svc },
		{ NULL }
	};

	if (svc->srv_procroot == NULL)
		return 0;

	rc = lprocfs_add_vars(svc->srv_procroot, nrs_tbf_lprocfs_vars, NULL);

	return rc;
}

/**
 * Cleans up a TBF policy's lprocfs interface for service \a svc
 *
 * \param[in] svc the service
 */
void nrs_tbf_lprocfs_fini(struct ptlrpc_service *svc)
{
	if (svc->srv_procroot == NULL)
		return;

	lprocfs_remove_proc_entry("nrs_tbf_quantum", svc->srv_procroot);
}

#endif /* LPROCFS */

/**
 * TBF policy operations
 */
static const struct ptlrpc_nrs_pol_ops nrs_tbf_ops = {
	.op_policy_start	= nrs_tbf_start,
	.op_policy_stop		= nrs_tbf_stop,
	.op_policy_ctl		= nrs_tbf_ctl,
	.op_res_get		= nrs_tbf_res_get,
	.op_res_put		= nrs_tbf_res_put,
	.op_req_get		= nrs_tbf_req_get,
	.op_req_enqueue		= nrs_tbf_req_add,
	.op_req_dequeue		= nrs_tbf_req_del,
	.op_req_stop		= nrs_tbf_req_stop,
#ifdef LPROCFS
	.op_lprocfs_init	= nrs_tbf_lprocfs_init,
	.op_lprocfs_fini	= nrs_tbf_lprocfs_fini,
#endif
};

/**
 * TBF policy configuration
 */
struct ptlrpc_nrs_pol_conf nrs_conf_tbf = {
	.nc_name		= NRS_POL_NAME_TBF,
	.nc_ops			= &nrs_tbf_ops,
	.nc_compat		= nrs_policy_compat_all,
};

/** @} tbf */

/** @} nrs */

#endif /* HAVE_SERVER_SUPPORT */
