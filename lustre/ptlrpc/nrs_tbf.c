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
 * Copyright (C) 2013 DataDirect Networks, Inc.
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

	spin_lock(&nrs->nrs_lock);
	nrs->nrs_throttling = 0;
	spin_unlock(&nrs->nrs_lock);
	cfs_waitq_signal(&svcpt->scp_waitq);

	return HRTIMER_NORESTART;
}

#define NRS_TBF_DEFAULT_RULE "default"

static void nrs_tbf_rule_fini(struct nrs_tbf_rule *rule)
{
	LASSERT(cfs_atomic_read(&rule->tr_ref) == 0);
	LASSERT(cfs_list_empty(&rule->tr_cli_list));
	LASSERT(cfs_list_empty(&rule->tr_linkage));

	rule->tr_head->th_ops->o_rule_fini(rule);
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
	if (cfs_atomic_dec_return(&rule->tr_ref) > 0)
		return;

	nrs_tbf_rule_fini(rule);
}

/**
 * Increases the rule's usage reference count.
 */
static inline void nrs_tbf_rule_get(struct nrs_tbf_rule *rule)
{
	cfs_atomic_inc(&rule->tr_ref);
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
nrs_tbf_cli_reset_value(struct nrs_tbf_head *head,
			struct nrs_tbf_client *cli)

{
	struct nrs_tbf_rule *rule = cli->tc_rule;

	cli->tc_rpc_rate = rule->tr_rpc_rate;
	cli->tc_nsecs = rule->tr_nsecs;
	cli->tc_depth = rule->tr_depth;
	cli->tc_ntoken = rule->tr_depth;
	cli->tc_check_time = ktime_to_ns(ktime_get());
	cli->tc_rule_sequence = cfs_atomic_read(&head->th_rule_sequence);
	cli->tc_rule_generation = rule->tr_generation;
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
	nrs_tbf_cli_reset_value(head, cli);
}

static int
nrs_tbf_rule_dump(struct nrs_tbf_rule *rule, char *buff, int length)
{
	return rule->tr_head->th_ops->o_rule_dump(rule, buff, length);
}

static int
nrs_tbf_rule_dump_all(struct nrs_tbf_head *head, char *buff, int length)
{
	struct nrs_tbf_rule *rule;
	int rc = 0;

	LASSERT(head != NULL);
	spin_lock(&head->th_rule_lock);
	/* List the rules from newest to oldest */
	cfs_list_for_each_entry(rule, &head->th_list, tr_linkage) {
		LASSERT((rule->tr_flags & NTRS_STOPPING) == 0);
		rc += nrs_tbf_rule_dump(rule, buff + rc, length - rc);
	}
	spin_unlock(&head->th_rule_lock);

	return rc;
}

static struct nrs_tbf_rule *
nrs_tbf_rule_find(struct nrs_tbf_head *head,
		  const char *name)
{
	struct nrs_tbf_rule *rule = NULL;
	struct nrs_tbf_rule *tmp_rule;

	LASSERT(head != NULL);
	spin_lock(&head->th_rule_lock);
	cfs_list_for_each_entry(tmp_rule, &head->th_list, tr_linkage) {
		LASSERT((tmp_rule->tr_flags & NTRS_STOPPING) == 0);
		if (strcmp(tmp_rule->tr_name, name) == 0) {
			nrs_tbf_rule_get(tmp_rule);
			rule = tmp_rule;
			break;
		}
	}
	spin_unlock(&head->th_rule_lock);
	return rule;
}

static struct nrs_tbf_rule *
nrs_tbf_rule_match(struct nrs_tbf_head *head,
		   struct nrs_tbf_client *cli)
{
	struct nrs_tbf_rule *rule = NULL;
	struct nrs_tbf_rule *tmp_rule;

	spin_lock(&head->th_rule_lock);
	/* Match the newest rule in the list */
	cfs_list_for_each_entry(tmp_rule, &head->th_list, tr_linkage) {
		LASSERT((tmp_rule->tr_flags & NTRS_STOPPING) == 0);
		if (head->th_ops->o_rule_match(tmp_rule, cli)) {
			rule = tmp_rule;
			break;
		}
	}

	if (rule == NULL)
		rule = head->th_rule;

	nrs_tbf_rule_get(rule);
	spin_unlock(&head->th_rule_lock);
	return rule;
}

static void
nrs_tbf_cli_init(struct nrs_tbf_head *head,
		 struct nrs_tbf_client *cli,
		 struct ptlrpc_request *req)
{
	struct nrs_tbf_rule *rule;

	cli->tc_in_heap = false;
	head->th_ops->o_cli_init(cli, req);
	CFS_INIT_LIST_HEAD(&cli->tc_list);
	CFS_INIT_LIST_HEAD(&cli->tc_linkage);
	cfs_atomic_set(&cli->tc_ref, 1);
	rule = nrs_tbf_rule_match(head, cli);
	nrs_tbf_cli_reset(head, rule, cli);
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

static int
nrs_tbf_rule_start(struct ptlrpc_nrs_policy *policy,
		   struct nrs_tbf_head *head,
		   struct nrs_tbf_cmd *start)
{
	struct nrs_tbf_rule *rule;
	int rc;

	LASSERT(!spin_is_locked(&policy->pol_nrs->nrs_lock));

	rule = nrs_tbf_rule_find(head, start->tc_name);
	if (rule) {
		nrs_tbf_rule_put(rule);
		return -EEXIST;
	}

	OBD_CPT_ALLOC_PTR(rule, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (rule == NULL)
		return -ENOMEM;

	memcpy(rule->tr_name, start->tc_name, strlen(start->tc_name));
	rule->tr_rpc_rate = start->tc_rpc_rate;
	rule->tr_nsecs = NSEC_PER_SEC / rule->tr_rpc_rate;
	rule->tr_depth = tbf_depth;
	cfs_atomic_set(&rule->tr_ref, 1);
	CFS_INIT_LIST_HEAD(&rule->tr_cli_list);
	CFS_INIT_LIST_HEAD(&rule->tr_nids);

	rc = head->th_ops->o_rule_init(policy, rule, start);
	if (rc) {
		OBD_FREE_PTR(rule);
		return rc;
	}

	if (!(start->tc_rule_flags & NTRS_DEFAULT))
		spin_lock(&policy->pol_nrs->nrs_lock);
	/* Add as the newest rule */
	spin_lock(&head->th_rule_lock);
	cfs_list_add(&rule->tr_linkage, &head->th_list);
	rule->tr_head = head;
	spin_unlock(&head->th_rule_lock);
	cfs_atomic_inc(&head->th_rule_sequence);
	if (!(start->tc_rule_flags & NTRS_DEFAULT))
		spin_unlock(&policy->pol_nrs->nrs_lock);
	else {
		rule->tr_flags |= NTRS_DEFAULT;
		LASSERT(head->th_rule == NULL);
		head->th_rule = rule;
	}

	return 0;
}

static int
nrs_tbf_rule_change(struct ptlrpc_nrs_policy *policy,
		    struct nrs_tbf_head *head,
		    struct nrs_tbf_cmd *change)
{
	struct nrs_tbf_rule *rule;

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	rule = nrs_tbf_rule_find(head, change->tc_name);
	if (rule == NULL)
		return -ENOENT;

	rule->tr_rpc_rate = change->tc_rpc_rate;
	rule->tr_nsecs = NSEC_PER_SEC / rule->tr_rpc_rate;
	rule->tr_generation++;
	nrs_tbf_rule_put(rule);

	return 0;
}

static int
nrs_tbf_rule_stop(struct ptlrpc_nrs_policy *policy,
		  struct nrs_tbf_head *head,
		  struct nrs_tbf_cmd *stop)
{
	struct nrs_tbf_rule *rule;

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	if (strcmp(stop->tc_name, NRS_TBF_DEFAULT_RULE) == 0)
		return -EPERM;

	rule = nrs_tbf_rule_find(head, stop->tc_name);
	if (rule == NULL)
		return -ENOENT;

	cfs_list_del_init(&rule->tr_linkage);
	rule->tr_flags |= NTRS_STOPPING;
	nrs_tbf_rule_put(rule);
	nrs_tbf_rule_put(rule);

	return 0;
}

static int
nrs_tbf_command(struct ptlrpc_nrs_policy *policy,
		struct nrs_tbf_head *head,
		struct nrs_tbf_cmd *cmd)
{
	int rc;

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	switch (cmd->tc_cmd) {
	case NRS_CTL_TBF_START_RULE:
		if (!(cmd->tc_valid_types & head->th_type_flag))
			return -EINVAL;

		spin_unlock(&policy->pol_nrs->nrs_lock);
		rc = nrs_tbf_rule_start(policy, head, cmd);
		spin_lock(&policy->pol_nrs->nrs_lock);
		return rc;
	case NRS_CTL_TBF_CHANGE_RATE:
		rc = nrs_tbf_rule_change(policy, head, cmd);
		return rc;
	case NRS_CTL_TBF_STOP_RULE:
		rc = nrs_tbf_rule_stop(policy, head, cmd);
		/* Take it as a success, if not exists at all */
		if (rc == -ENOENT)
			rc = 0;
		return rc;
	default:
		return -EFAULT;
	}

	return -EFAULT;
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

static unsigned nrs_tbf_jobid_hop_hash(cfs_hash_t *hs, const void *key,
				  unsigned mask)
{
	return cfs_hash_djb2_hash(key, strlen(key), mask);
}

static int nrs_tbf_jobid_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	return (strcmp(cli->tc_jobid, key) == 0);
}

static void *nrs_tbf_jobid_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	return cli->tc_jobid;
}

static void *nrs_tbf_jobid_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_tbf_client, tc_hnode);
}

static void nrs_tbf_jobid_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	cfs_atomic_inc(&cli->tc_ref);
}

static void nrs_tbf_jobid_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	cfs_atomic_dec(&cli->tc_ref);
}

/**
 * Removes an nrs_tbf_client from the hash and frees its memory, if it has no
 * active users
 */
static void nrs_tbf_jobid_hop_put_free(cfs_hash_t *hs,
				       cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client	*cli = cfs_hlist_entry(hnode,
						       struct nrs_tbf_client,
						       tc_hnode);
	cfs_hash_bd_t		 bds[2];

	cfs_hash_lock(hs, 0);
	cfs_hash_dual_bd_get_and_lock(hs, cli->tc_jobid, bds, 1);

	if (cfs_atomic_dec_return(&cli->tc_ref) > 1) {
		cfs_hash_dual_bd_unlock(hs, bds, 1);
		cfs_hash_unlock(hs, 0);
		return;
	}

	if (bds[1].bd_bucket == NULL)
		cfs_hash_bd_del_locked(hs, &bds[0], hnode);
	else
		hnode = cfs_hash_dual_bd_finddel_locked(hs, bds, cli->tc_jobid,
							hnode);

	cfs_hash_dual_bd_unlock(hs, bds, 1);
	cfs_hash_unlock(hs, 0);

	LASSERT(cfs_atomic_read(&cli->tc_ref) == 1 ||
		cfs_atomic_read(&cli->tc_ref) == 0);
	if (cfs_atomic_read(&cli->tc_ref) == 1)
		cfs_atomic_dec(&cli->tc_ref);
	nrs_tbf_cli_fini(cli);
}

static cfs_hash_ops_t nrs_tbf_jobid_hash_ops = {
	.hs_hash	= nrs_tbf_jobid_hop_hash,
	.hs_keycmp	= nrs_tbf_jobid_hop_keycmp,
	.hs_key		= nrs_tbf_jobid_hop_key,
	.hs_object	= nrs_tbf_jobid_hop_object,
	.hs_get		= nrs_tbf_jobid_hop_get,
	.hs_put		= nrs_tbf_jobid_hop_put_free,
	.hs_put_locked	= nrs_tbf_jobid_hop_put,
};

#define NRS_TBF_JOBID_HASH_FLAGS (CFS_HASH_SPIN_BKTLOCK | \
				  CFS_HASH_COUNTER | CFS_HASH_REHASH)

static struct nrs_tbf_client *
nrs_tbf_jobid_cli_find(struct nrs_tbf_head *head,
		       struct ptlrpc_request *req)
{
	return cfs_hash_lookup(head->th_cli_hash,
			       lustre_msg_get_jobid(req->rq_reqmsg));
}

static struct nrs_tbf_client *
nrs_tbf_jobid_cli_findadd(struct nrs_tbf_head *head,
			  struct nrs_tbf_client *cli)
{
	return cfs_hash_findadd_unique(head->th_cli_hash, cli->tc_jobid,
				       &cli->tc_hnode);
}

static void
nrs_tbf_jobid_cli_init(struct nrs_tbf_client *cli,
			     struct ptlrpc_request *req)
{
	char *jobid = lustre_msg_get_jobid(req->rq_reqmsg);

	LASSERT(strlen(jobid) < JOBSTATS_JOBID_SIZE);
	memcpy(cli->tc_jobid, jobid, strlen(jobid));
}

static int
nrs_tbf_jobid_startup(struct ptlrpc_nrs_policy *policy,
		      struct nrs_tbf_head *head)
{
	struct nrs_tbf_cmd	 start;
	int rc;

	head->th_cli_hash = cfs_hash_create("nrs_tbf_hash",
					    HASH_JOB_STATS_CUR_BITS,
					    HASH_JOB_STATS_MAX_BITS,
					    HASH_JOB_STATS_BKT_BITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_tbf_jobid_hash_ops,
					    NRS_TBF_JOBID_HASH_FLAGS);
	if (head->th_cli_hash == NULL)
		return -ENOMEM;

	memset(&start, 0, sizeof(start));
	OBD_CPT_ALLOC(start.tc_jobids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      2);
	if (start.tc_jobids_str == NULL) {
		cfs_hash_putref(head->th_cli_hash);
		return -ENOMEM;
	}
	start.tc_jobids_str[0] = '*';

	start.tc_rpc_rate = tbf_rate;
	start.tc_rule_flags = NTRS_DEFAULT;
	start.tc_name = NRS_TBF_DEFAULT_RULE;
	CFS_INIT_LIST_HEAD(&start.tc_jobids);
	rc = nrs_tbf_rule_start(policy, head, &start);
	OBD_FREE(start.tc_jobids_str, 2);

	return rc;
}

/**
 * Frees jobid of \a list.
 *
 */
static void
nrs_tbf_jobid_list_free(cfs_list_t *jobid_list)
{
	struct nrs_tbf_jobid *jobid, *n;

	cfs_list_for_each_entry_safe(jobid, n, jobid_list, tj_linkage) {
		OBD_FREE(jobid->tj_id, strlen(jobid->tj_id) + 1);
		cfs_list_del(&jobid->tj_linkage);
		OBD_FREE(jobid, sizeof(struct nrs_tbf_jobid));
	}
}

static int
nrs_tbf_jobid_list_add(const struct cfs_lstr *id, cfs_list_t *jobid_list)
{
	struct nrs_tbf_jobid *jobid;

	OBD_ALLOC(jobid, sizeof(struct nrs_tbf_jobid));
	if (jobid == NULL)
		return -ENOMEM;

	OBD_ALLOC(jobid->tj_id, id->ls_len + 1);
	if (jobid->tj_id == NULL) {
		OBD_FREE(jobid, sizeof(struct nrs_tbf_jobid));
		return -ENOMEM;
	}

	memcpy(jobid->tj_id, id->ls_str, id->ls_len);
	cfs_list_add_tail(&jobid->tj_linkage, jobid_list);
	return 0;
}

static int
nrs_tbf_jobid_list_match(cfs_list_t *jobid_list, char *id)
{
	struct nrs_tbf_jobid *jobid;

	cfs_list_for_each_entry(jobid, jobid_list, tj_linkage) {
		if (strcmp(id, jobid->tj_id) == 0)
			return 1;
	}
	return 0;
}

static int
nrs_tbf_jobid_list_parse(char *str, int len, cfs_list_t *jobid_list)
{
	struct cfs_lstr src;
	struct cfs_lstr res;
	int rc;
	ENTRY;

	src.ls_str = str;
	src.ls_len = len;
	CFS_INIT_LIST_HEAD(jobid_list);
	while (src.ls_str) {
		rc = cfs_gettok(&src, ' ', &res);
		if (rc == 0) {
			nrs_tbf_jobid_list_free(jobid_list);
			RETURN(-EINVAL);
		}
		rc = nrs_tbf_jobid_list_add(&res, jobid_list);
		if (rc) {
			nrs_tbf_jobid_list_free(jobid_list);
			RETURN(rc);
		}
	}
	RETURN(0);
}

static void nrs_tbf_jobid_cmd_fini(struct nrs_tbf_cmd *cmd)
{
	if (!cfs_list_empty(&cmd->tc_jobids))
		nrs_tbf_jobid_list_free(&cmd->tc_jobids);
	if (cmd->tc_jobids_str)
		OBD_FREE(cmd->tc_jobids_str, strlen(cmd->tc_jobids_str) + 1);
}

static int nrs_tbf_jobid_parse(struct nrs_tbf_cmd *cmd, const char *id)
{
	int rc;

	OBD_ALLOC(cmd->tc_jobids_str, strlen(id) + 1);
	if (cmd->tc_jobids_str == NULL)
		return -ENOMEM;

	memcpy(cmd->tc_jobids_str, id, strlen(id));

	/* parse jobid list */
	rc = nrs_tbf_jobid_list_parse(cmd->tc_jobids_str,
				      strlen(cmd->tc_jobids_str),
				      &cmd->tc_jobids);
	if (rc)
		nrs_tbf_jobid_cmd_fini(cmd);

	return rc;
}

static int nrs_tbf_jobid_rule_init(struct ptlrpc_nrs_policy *policy,
				   struct nrs_tbf_rule *rule,
				   struct nrs_tbf_cmd *start)
{
	int rc = 0;

	LASSERT(start->tc_jobids_str);
	OBD_CPT_ALLOC(rule->tr_jobids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      strlen(start->tc_jobids_str) + 1);
	if (rule->tr_jobids_str == NULL)
		GOTO(out, rc = -ENOMEM);

	memcpy(rule->tr_jobids_str,
	       start->tc_jobids_str,
	       strlen(start->tc_jobids_str));

	CFS_INIT_LIST_HEAD(&rule->tr_jobids);
	if (!cfs_list_empty(&start->tc_jobids)) {
		rc = nrs_tbf_jobid_list_parse(rule->tr_jobids_str,
					      strlen(rule->tr_jobids_str),
					      &rule->tr_jobids);
		if (rc) {
			CERROR("jobids {%s} illegal\n",
			       rule->tr_jobids_str);
			GOTO(out_free_jobid, rc);
		}
	}
	return rc;
out_free_jobid:
	OBD_FREE(rule->tr_jobids_str,
		 strlen(start->tc_jobids_str) + 1);
out:
	return rc;
}

static int
nrs_tbf_jobid_rule_dump(struct nrs_tbf_rule *rule, char *buff, int length)
{
	return snprintf(buff, length, "%s {%s} %llu, ref %d\n",
			rule->tr_name,
			rule->tr_jobids_str,
			rule->tr_rpc_rate,
			cfs_atomic_read(&rule->tr_ref) - 1);
}

static int
nrs_tbf_jobid_rule_match(struct nrs_tbf_rule *rule,
			 struct nrs_tbf_client *cli)
{
	return nrs_tbf_jobid_list_match(&rule->tr_jobids, cli->tc_jobid);
}

static void nrs_tbf_jobid_rule_fini(struct nrs_tbf_rule *rule)
{
	if (!cfs_list_empty(&rule->tr_jobids))
		nrs_tbf_jobid_list_free(&rule->tr_jobids);
	LASSERT(rule->tr_jobids_str != NULL);
	OBD_FREE(rule->tr_jobids_str, strlen(rule->tr_jobids_str) + 1);
}

struct nrs_tbf_ops nrs_tbf_jobid_ops = {
	.o_name = NRS_TBF_TYPE_JOBID,
	.o_startup = nrs_tbf_jobid_startup,
	.o_cli_find = nrs_tbf_jobid_cli_find,
	.o_cli_findadd = nrs_tbf_jobid_cli_findadd,
	.o_cli_init = nrs_tbf_jobid_cli_init,
	.o_rule_init = nrs_tbf_jobid_rule_init,
	.o_rule_dump = nrs_tbf_jobid_rule_dump,
	.o_rule_match = nrs_tbf_jobid_rule_match,
	.o_rule_fini = nrs_tbf_jobid_rule_fini,
};

/**
 * libcfs_hash operations for nrs_tbf_net::cn_cli_hash
 *
 * This uses ptlrpc_request::rq_peer.nid as its key, in order to hash
 * nrs_tbf_client objects.
 */
#define NRS_TBF_NID_BKT_BITS    8
#define NRS_TBF_NID_BITS        16

static unsigned nrs_tbf_nid_hop_hash(cfs_hash_t *hs, const void *key,
				  unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(lnet_nid_t), mask);
}

static int nrs_tbf_nid_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	lnet_nid_t	      *nid = (lnet_nid_t *)key;
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	return *nid == cli->tc_nid;
}

static void *nrs_tbf_nid_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	return &cli->tc_nid;
}

static void *nrs_tbf_nid_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_tbf_client, tc_hnode);
}

static void nrs_tbf_nid_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	cfs_atomic_inc(&cli->tc_ref);
}

static void nrs_tbf_nid_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	cfs_atomic_dec(&cli->tc_ref);
}

static void nrs_tbf_nid_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_tbf_client *cli = cfs_hlist_entry(hnode,
						     struct nrs_tbf_client,
						     tc_hnode);

	LASSERTF(cfs_atomic_read(&cli->tc_ref) == 0,
		 "Busy TBF object from client with NID %s, with %d refs\n",
		 libcfs_nid2str(cli->tc_nid), cfs_atomic_read(&cli->tc_ref));

	nrs_tbf_cli_fini(cli);
}

static cfs_hash_ops_t nrs_tbf_nid_hash_ops = {
	.hs_hash	= nrs_tbf_nid_hop_hash,
	.hs_keycmp	= nrs_tbf_nid_hop_keycmp,
	.hs_key		= nrs_tbf_nid_hop_key,
	.hs_object	= nrs_tbf_nid_hop_object,
	.hs_get		= nrs_tbf_nid_hop_get,
	.hs_put		= nrs_tbf_nid_hop_put,
	.hs_put_locked	= nrs_tbf_nid_hop_put,
	.hs_exit	= nrs_tbf_nid_hop_exit,
};

static struct nrs_tbf_client *
nrs_tbf_nid_cli_find(struct nrs_tbf_head *head,
		     struct ptlrpc_request *req)
{
	return cfs_hash_lookup(head->th_cli_hash, &req->rq_peer.nid);
}

static struct nrs_tbf_client *
nrs_tbf_nid_cli_findadd(struct nrs_tbf_head *head,
			struct nrs_tbf_client *cli)
{
	return cfs_hash_findadd_unique(head->th_cli_hash, &cli->tc_nid,
				       &cli->tc_hnode);
}

static int
nrs_tbf_nid_startup(struct ptlrpc_nrs_policy *policy,
		    struct nrs_tbf_head *head)
{
	struct nrs_tbf_cmd	start;
	int rc;

	head->th_cli_hash = cfs_hash_create("nrs_tbf_hash",
					    NRS_TBF_NID_BITS,
					    NRS_TBF_NID_BITS,
					    NRS_TBF_NID_BKT_BITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_tbf_nid_hash_ops,
					    CFS_HASH_RW_BKTLOCK);
	if (head->th_cli_hash == NULL)
		return -ENOMEM;

	memset(&start, 0, sizeof(start));
	OBD_CPT_ALLOC(start.tc_nids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      2);
	if (start.tc_nids_str == NULL) {
		cfs_hash_putref(head->th_cli_hash);
		return -ENOMEM;
	}
	start.tc_nids_str[0] = '*';

	start.tc_rpc_rate = tbf_rate;
	start.tc_rule_flags = NTRS_DEFAULT;
	start.tc_name = NRS_TBF_DEFAULT_RULE;
	CFS_INIT_LIST_HEAD(&start.tc_nids);
	rc = nrs_tbf_rule_start(policy, head, &start);
	OBD_FREE(start.tc_nids_str, 2);

	return rc;
}

static void
nrs_tbf_nid_cli_init(struct nrs_tbf_client *cli,
			     struct ptlrpc_request *req)
{
	cli->tc_nid = req->rq_peer.nid;
}

static int nrs_tbf_nid_rule_init(struct ptlrpc_nrs_policy *policy,
				 struct nrs_tbf_rule *rule,
				 struct nrs_tbf_cmd *start)
{
	LASSERT(start->tc_nids_str);
	OBD_CPT_ALLOC(rule->tr_nids_str,
		      nrs_pol2cptab(policy),
		      nrs_pol2cptid(policy),
		      strlen(start->tc_nids_str) + 1);
	if (rule->tr_nids_str == NULL)
		return -ENOMEM;

	memcpy(rule->tr_nids_str,
	       start->tc_nids_str,
	       strlen(start->tc_nids_str));

	CFS_INIT_LIST_HEAD(&rule->tr_nids);
	if (!cfs_list_empty(&start->tc_nids)) {
		if (cfs_parse_nidlist(rule->tr_nids_str,
				      strlen(rule->tr_nids_str),
				      &rule->tr_nids) <= 0) {
			CERROR("nids {%s} illegal\n",
			       rule->tr_nids_str);
			OBD_FREE(rule->tr_nids_str,
				 strlen(start->tc_nids_str) + 1);
			return -EINVAL;
		}
	}
	return 0;
}

static int
nrs_tbf_nid_rule_dump(struct nrs_tbf_rule *rule, char *buff, int length)
{
	return snprintf(buff, length, "%s {%s} %llu, ref %d\n",
			rule->tr_name,
			rule->tr_nids_str,
			rule->tr_rpc_rate,
			cfs_atomic_read(&rule->tr_ref) - 1);
}

static int
nrs_tbf_nid_rule_match(struct nrs_tbf_rule *rule,
		       struct nrs_tbf_client *cli)
{
	return cfs_match_nid(cli->tc_nid, &rule->tr_nids);
}

static void nrs_tbf_nid_rule_fini(struct nrs_tbf_rule *rule)
{
	if (!cfs_list_empty(&rule->tr_nids))
		cfs_free_nidlist(&rule->tr_nids);
	LASSERT(rule->tr_nids_str != NULL);
	OBD_FREE(rule->tr_nids_str, strlen(rule->tr_nids_str) + 1);
}

static void nrs_tbf_nid_cmd_fini(struct nrs_tbf_cmd *cmd)
{
	if (!cfs_list_empty(&cmd->tc_nids))
		cfs_free_nidlist(&cmd->tc_nids);
	if (cmd->tc_nids_str)
		OBD_FREE(cmd->tc_nids_str, strlen(cmd->tc_nids_str) + 1);
}

static int nrs_tbf_nid_parse(struct nrs_tbf_cmd *cmd, const char *id)
{
	OBD_ALLOC(cmd->tc_nids_str, strlen(id) + 1);
	if (cmd->tc_nids_str == NULL)
		return -ENOMEM;

	memcpy(cmd->tc_nids_str, id, strlen(id));

	/* parse NID list */
	if (cfs_parse_nidlist(cmd->tc_nids_str,
			      strlen(cmd->tc_nids_str),
			      &cmd->tc_nids) <= 0) {
		nrs_tbf_nid_cmd_fini(cmd);
		return -EINVAL;
	}

	return 0;
}

struct nrs_tbf_ops nrs_tbf_nid_ops = {
	.o_name = NRS_TBF_TYPE_NID,
	.o_startup = nrs_tbf_nid_startup,
	.o_cli_find = nrs_tbf_nid_cli_find,
	.o_cli_findadd = nrs_tbf_nid_cli_findadd,
	.o_cli_init = nrs_tbf_nid_cli_init,
	.o_rule_init = nrs_tbf_nid_rule_init,
	.o_rule_dump = nrs_tbf_nid_rule_dump,
	.o_rule_match = nrs_tbf_nid_rule_match,
	.o_rule_fini = nrs_tbf_nid_rule_fini,
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
static int nrs_tbf_start(struct ptlrpc_nrs_policy *policy, char *arg)
{
	struct nrs_tbf_head	*head;
	struct nrs_tbf_ops	*ops;
	__u32			 type;
	int rc = 0;

	if (arg == NULL || strlen(arg) > NRS_TBF_TYPE_MAX_LEN)
		GOTO(out, rc = -EINVAL);

	if ((strlen(arg) == strlen(NRS_TBF_TYPE_NID)) &&
	    (strcmp(arg, NRS_TBF_TYPE_NID) == 0)) {
		ops = &nrs_tbf_nid_ops;
		type = NRS_TBF_FLAG_NID;
	} else if (strcmp(arg, NRS_TBF_TYPE_JOBID) == 0) {
		ops = &nrs_tbf_jobid_ops;
		type = NRS_TBF_FLAG_JOBID;
	} else
		GOTO(out, rc = -ENOTSUPP);

	OBD_CPT_ALLOC_PTR(head, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (head == NULL)
		GOTO(out, rc = -ENOMEM);

	memcpy(head->th_type, arg, strlen(arg));
	head->th_type[strlen(arg)] = '\0';
	head->th_ops = ops;
	head->th_type_flag = type;

	head->th_binheap = cfs_binheap_create(&nrs_tbf_heap_ops,
					      CBH_FLAG_ATOMIC_GROW, 4096, NULL,
					      nrs_pol2cptab(policy),
					      nrs_pol2cptid(policy));
	if (head->th_binheap == NULL)
		GOTO(out_free_head, rc = -ENOMEM);

	cfs_atomic_set(&head->th_rule_sequence, 0);
	spin_lock_init(&head->th_rule_lock);
	CFS_INIT_LIST_HEAD(&head->th_list);
	hrtimer_init(&head->th_timer, CLOCK_MONOTONIC, HRTIMER_MODE_ABS);
	head->th_timer.function = nrs_tbf_timer_cb;
	rc = head->th_ops->o_startup(policy, head);
	if (rc)
		GOTO(out_free_heap, rc);

	policy->pol_private = head;
	return 0;
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
	struct ptlrpc_nrs *nrs = policy->pol_nrs;
	struct nrs_tbf_rule *rule, *n;

	LASSERT(head != NULL);
	LASSERT(head->th_cli_hash != NULL);
	hrtimer_cancel(&head->th_timer);
	/* Should cleanup hash first before free rules */
	cfs_hash_putref(head->th_cli_hash);
	cfs_list_for_each_entry_safe(rule, n, &head->th_list, tr_linkage) {
		cfs_list_del_init(&rule->tr_linkage);
		nrs_tbf_rule_put(rule);
	}
	LASSERT(cfs_list_empty(&head->th_list));
	LASSERT(head->th_binheap != NULL);
	LASSERT(cfs_binheap_is_empty(head->th_binheap));
	cfs_binheap_destroy(head->th_binheap);
	OBD_FREE_PTR(head);
	/* TODO: Notify service thread */
	spin_lock(&nrs->nrs_lock);
	nrs->nrs_throttling = 0;
	spin_unlock(&nrs->nrs_lock);
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
	struct ptlrpc_request *req;

	if (parent == NULL) {
		*resp = &((struct nrs_tbf_head *)policy->pol_private)->th_res;
		return 0;
	}

	head = container_of(parent, struct nrs_tbf_head, th_res);
	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	cli = head->th_ops->o_cli_find(head, req);
	if (cli != NULL) {
		spin_lock(&policy->pol_nrs->nrs_svcpt->scp_req_lock);
		LASSERT(cli->tc_rule);
		if (cli->tc_rule_sequence !=
		    cfs_atomic_read(&head->th_rule_sequence) ||
		    cli->tc_rule->tr_flags & NTRS_STOPPING) {
			struct nrs_tbf_rule *rule;

			rule = nrs_tbf_rule_match(head, cli);
			if (rule != cli->tc_rule)
				nrs_tbf_cli_reset(head, rule, cli);
			else
				nrs_tbf_rule_put(rule);
		} else if (cli->tc_rule_generation !=
			   cli->tc_rule->tr_generation) {
			nrs_tbf_cli_reset_value(head, cli);
		}
		spin_unlock(&policy->pol_nrs->nrs_svcpt->scp_req_lock);
		goto out;
	}

	OBD_CPT_ALLOC_GFP(cli, nrs_pol2cptab(policy), nrs_pol2cptid(policy),
			  sizeof(*cli), moving_req ? GFP_ATOMIC : __GFP_IO);
	if (cli == NULL)
		return -ENOMEM;
	nrs_tbf_cli_init(head, cli, req);
	tmp = head->th_ops->o_cli_findadd(head, cli);
	if (tmp != cli) {
		cfs_atomic_dec(&cli->tc_ref);
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

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_svcpt->scp_req_lock));

	if (!peek && policy->pol_nrs->nrs_throttling)
		return NULL;

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

			spin_lock(&policy->pol_nrs->nrs_lock);
			policy->pol_nrs->nrs_throttling = 1;
			spin_unlock(&policy->pol_nrs->nrs_lock);
			head->th_deadline = deadline;
			time = ktime_set(0, 0);
			time = ktime_add_ns(time, deadline);
			hrtimer_start(&head->th_timer, time, HRTIMER_MODE_ABS);
		}
	}

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

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_svcpt->scp_req_lock));

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_tbf_client, tc_res);
	head = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_tbf_head, th_res);
	if (cfs_list_empty(&cli->tc_list)) {
		LASSERT(!cli->tc_in_heap);
		rc = cfs_binheap_insert(head->th_binheap, &cli->tc_node);
		if (rc == 0) {
			cli->tc_in_heap = true;
			nrq->nr_u.tbf.tr_sequence = head->th_sequence++;
			cfs_list_add_tail(&nrq->nr_u.tbf.tr_list,
					  &cli->tc_list);
			if (policy->pol_nrs->nrs_throttling) {
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

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_svcpt->scp_req_lock));

	cli = container_of(nrs_request_resource(nrq),
			   struct nrs_tbf_client, tc_res);
	head = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_tbf_head, th_res);

	LASSERT(!cfs_list_empty(&nrq->nr_u.tbf.tr_list));
	cfs_list_del_init(&nrq->nr_u.tbf.tr_list);
	if (cfs_list_empty(&cli->tc_list)) {
		cfs_binheap_remove(head->th_binheap,
				   &cli->tc_node);
		cli->tc_in_heap = false;
	} else {
		cfs_binheap_relocate(head->th_binheap,
				     &cli->tc_node);
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

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_svcpt->scp_req_lock));

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

static int nrs_tbf_id_parse(struct nrs_tbf_cmd *cmd, char **val)
{
	int rc;
	char *token;

	token = strsep(val, "}");
	if (*val == NULL)
		GOTO(out, rc = -EINVAL);

	if (strlen(token) <= 1 ||
	    token[0] != '{')
		GOTO(out, rc = -EINVAL);
	/* Skip '{' */
	token++;

	/* Should be followed by ' ' or nothing */
	if ((*val)[0] == '\0')
		*val = NULL;
	else if ((*val)[0] == ' ')
		(*val)++;
	else
		GOTO(out, rc = -EINVAL);

	rc = nrs_tbf_jobid_parse(cmd, token);
	if (!rc)
		cmd->tc_valid_types |= NRS_TBF_FLAG_JOBID;

	rc = nrs_tbf_nid_parse(cmd, token);
	if (!rc)
		cmd->tc_valid_types |= NRS_TBF_FLAG_NID;

	if (!cmd->tc_valid_types)
		rc = -EINVAL;
	else
		rc = 0;
out:
	return rc;
}


static void nrs_tbf_cmd_fini(struct nrs_tbf_cmd *cmd)
{
	if (cmd->tc_valid_types & NRS_TBF_FLAG_JOBID)
		nrs_tbf_jobid_cmd_fini(cmd);
	if (cmd->tc_valid_types & NRS_TBF_FLAG_NID)
		nrs_tbf_nid_cmd_fini(cmd);
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
		 * But other commands need ID or rate argument.
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
		/* List of ID */
		LASSERT(val);
		rc = nrs_tbf_id_parse(cmd, &val);
		if (rc)
			GOTO(out_free_cmd, rc);
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
	nrs_tbf_cmd_fini(cmd);
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

	if (copy_from_user(kernbuf, buffer, count))
		GOTO(out_free_kernbuff, rc = -EFAULT);

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

	nrs_tbf_cmd_fini(cmd);
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
