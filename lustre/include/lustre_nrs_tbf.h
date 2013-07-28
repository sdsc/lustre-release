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
 * Copyright (C) 2013 DataDirect Networks, Inc.
 *
 */
/*
 *
 * Network Request Scheduler (NRS) Token Bucket Filter(TBF) policy header
 *
 */



#ifndef _LUSTRE_NRS_TBF_H
#define _LUSTRE_NRS_TBF_H
#include <lustre_net.h>
/* \name tbf
 *
 * TBF policy
 *
 * @{
 */
struct nrs_tbf_client {
	/** Resource object for policy instance. */
	struct ptlrpc_nrs_resource	tc_res;
	/** Node in the hash table. */
	cfs_hlist_node_t		tc_hnode;
	/** NID of the client. */
	lnet_nid_t			tc_nid;
	/** Reference number of the client. */
	cfs_atomic_t			tc_ref;
	/** Likage to rule. */
	cfs_list_t		        tc_linkage;
	/** Pointer to rule. */
	struct nrs_tbf_rule	       *tc_rule;
	/** Generation of the rule matched. */
	__u64                           tc_rule_generation;
	/** Limit of RPC rate. */
	__u64                           tc_rpc_rate;
	/** Time to wait for next token. */
	__u64                           tc_nsecs;
	/** RPC token number. */
	__u64                           tc_ntoken;
	/** Token bucket depth. */
	__u64                           tc_depth;
	/** Time check-point. */
	__u64				tc_check_time;
	/** List of queued requests. */
	cfs_list_t			tc_list;
	/** Node in binary heap. */
	cfs_binheap_node_t		tc_node;
	/** Whether the client is in heap. */
	bool				tc_in_heap;
	/** Sequence of the newest rule. */
	__u64				tc_rule_sequence;
};

#define MAX_TBF_NAME (16)
enum nrs_tbf_rule_states {
	/** The rule is stopping */
	NTRS_STOPPING = 0x00000001,
};

struct nrs_tbf_rule {
	/** Name of the rule. */
	char				tr_name[MAX_TBF_NAME];
	/** Likage to head. */
	cfs_list_t			tr_linkage;
	/** Nid list of the rule. */
	cfs_list_t			tr_nids;
	/** Nid list string of the rule.*/
	char			       *tr_nids_str;
	/** RPC/s limit. */
	__u64				tr_rpc_rate;
	/** Time to wait for next token. */
	__u64				tr_nsecs;
	/** Token bucket depth. */
	__u64				tr_depth;
	/** List of client. */
	cfs_list_t			tr_cli_list;
	/** State of the rule. */
	__u32				tr_state;
	/** Usage Reference count taken on the rule. */
	long				tr_ref;
	/** Generation of the rule. */
	__u64                           tr_generation;
};

struct nrs_tbf_dump {
	char			       *td_buff;
	int				td_size;
	int				td_length;
};

/**
 * Private data structure for the TBF policy
 */
struct nrs_tbf_head {
	/**
	 * Resource object for policy instance.
	 */
	struct ptlrpc_nrs_resource	th_res;
	/**
	 * List of rules.
	 */
	cfs_list_t			th_list;
	/**
	 * Generation of rules.
	 */
	__u64				th_rule_sequence;
	/**
	 * Default queue.
	 */
	struct nrs_tbf_rule	       *th_rule;
	/**
	 * Timer for next token.
	 */
#if defined(__KERNEL__) && defined(__linux__)
	struct hrtimer			th_timer;
#endif
	/**
	 * Deadline of the timer.
	 */
	__u64				th_deadline;
	/**
	 * Sequence of requests in TBF.
	 */
	__u64				th_sequence;
	/**
	 * Heap of queues in TBF.
	 */
	cfs_binheap_t		       *th_binheap;
	/**
	 * Lock to protect the binheap.
	 */
	spinlock_t			th_binheap_lock;
	/**
	 * Heap of queues in TBF.
	 */
	cfs_hash_t		       *th_cli_hash;
};

enum nrs_tbf_cmd_type {
	NRS_CTL_TBF_START_RULE = 0,
	NRS_CTL_TBF_STOP_RULE,
	NRS_CTL_TBF_CHANGE_RATE,
};

struct nrs_tbf_cmd {
	enum nrs_tbf_cmd_type        tc_cmd;
	char                        *tc_name;
	__u64                        tc_rpc_rate;
	cfs_list_t                   tc_nids;
	char                        *tc_nids_str;
};

struct nrs_tbf_req {
	/**
	 * Linkage to queue.
	 */
	cfs_list_t		tr_list;
	/**
	 * Sequence of the request.
	 */
	__u64			tr_sequence;
};

/**
 * TBF policy operations.
 */
enum nrs_ctl_tbf {
	/**
	 * Read the the data of a TBF policy.
	 */
	NRS_CTL_TBF_RD_RULE = PTLRPC_NRS_CTL_1ST_POL_SPEC,
	/**
	 * Write the the data of a TBF policy.
	 */
	NRS_CTL_TBF_WR_RULE,
};

/** @} tbf */
#endif
