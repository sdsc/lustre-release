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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lnet/lnet/net_fault.c
 *
 * Unreliable network simulator
 *
 * Author: liang.zhen@intel.com
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

/**
 * Add a new drop rule to LNet
 * There is no check for duplicated drop rule, all rules will be checked for
 * incoming message.
 */
int
lnet_drop_rule_add(struct lnet_drop_rule_attr *attr)
{
	struct lnet_drop_rule *rule;

	CLASSERT(LNET_DROP_PUT_BIT == 1 << LNET_MSG_PUT);
	CLASSERT(LNET_DROP_ACK_BIT == 1 << LNET_MSG_ACK);
	CLASSERT(LNET_DROP_GET_BIT == 1 << LNET_MSG_GET);
	CLASSERT(LNET_DROP_REPLY_BIT == 1 << LNET_MSG_REPLY);

	if (attr->dra_rate < LNET_DROP_RATE_MIN) {
		CDEBUG(D_NET, "drop rate (1/%d) should be lower than 1/%d\n",
		       attr->dra_rate, LNET_DROP_RATE_MIN);
		return -EINVAL;
	}

	CFS_ALLOC_PTR(rule);
	if (rule == NULL)
		return -ENOMEM;

	spin_lock_init(&rule->dr_lock);

	/* NB: only PUT and GET can be filtered if portal mask has been set */
	if (attr->dra_ptl_mask != 0)
		attr->dra_msg_mask &= LNET_DROP_GET_BIT | LNET_DROP_PUT_BIT;

	rule->dr_attr = *attr;
	rule->dr_drop_at = cfs_rand() % attr->dra_rate;
	if (attr->dra_delay != 0)
		rule->dr_start = cfs_time_current_sec() + attr->dra_delay;

	lnet_net_lock(LNET_LOCK_EX);
	list_add(&rule->dr_link, &the_lnet.ln_drop_rules);
	lnet_net_unlock(LNET_LOCK_EX);

	CDEBUG(D_NET, "Added drop rule: src %s, dst %s, rate %d, delay %d\n",
	       libcfs_nid2str(attr->dra_src), libcfs_nid2str(attr->dra_src),
	       attr->dra_rate, attr->dra_delay);
	return 0;
}

/**
 * Remove matched drop rules from lnet, if \a all is true, all rules will
 * be removed, otherwise all matched rules will be removed.
 * If \a src is zero, then all rules have \a dst as destination will be remove
 * If \a dst is zero, then all rules have \a src as source will be removed
 * If both of them are zero, all rules will be removed
 */
int
lnet_drop_rule_del(lnet_nid_t src, lnet_nid_t dst)
{
	struct lnet_drop_rule *rule;
	struct lnet_drop_rule *tmp;
	struct list_head       zombies;
	int		       n = 0;

	INIT_LIST_HEAD(&zombies);

	lnet_net_lock(LNET_LOCK_EX);
	list_for_each_entry_safe(rule, tmp, &the_lnet.ln_drop_rules, dr_link) {
		if (rule->dr_attr.dra_src != src && src != 0)
			continue;

		if (rule->dr_attr.dra_dst != dst && dst != 0)
			continue;

		list_move(&rule->dr_link, &zombies);
	}
	lnet_net_unlock(LNET_LOCK_EX);

	list_for_each_entry_safe(rule, tmp, &zombies, dr_link) {
		CDEBUG(D_NET, "Remove drop rule: src %s->dst: %s (1/%d)\n",
		       libcfs_nid2str(rule->dr_attr.dra_src),
		       libcfs_nid2str(rule->dr_attr.dra_dst),
		       rule->dr_attr.dra_rate);

		list_del(&rule->dr_link);
		CFS_FREE_PTR(rule);
		n++;
	}
	return n;
}

/**
 * List drop rule at position of \a pos
 */
int
lnet_drop_rule_list(int pos, struct lnet_drop_rule_attr *attr,
		    struct lnet_drop_rule_stat *stat)
{
	struct lnet_drop_rule *rule;
	int		       cpt;
	int		       i = 0;
	int		       rc = -ENOENT;
	ENTRY;

	cpt = lnet_net_lock_current();
	list_for_each_entry(rule, &the_lnet.ln_drop_rules, dr_link) {
		if (i++ < pos)
			continue;

		spin_lock(&rule->dr_lock);
		*attr = rule->dr_attr;
		*stat = rule->dr_stat;
		spin_unlock(&rule->dr_lock);
		rc = 0;
		break;
	}
	lnet_net_unlock(cpt);
	RETURN(rc);
}

/**
 * reset counters of all drop rules
 */
void
lnet_drop_rule_reset(void)
{
	struct lnet_drop_rule *rule;
	int		       cpt;

	cpt = lnet_net_lock_current();

	list_for_each_entry(rule, &the_lnet.ln_drop_rules, dr_link) {
		spin_lock(&rule->dr_lock);

		memset(&rule->dr_stat, 0, sizeof(rule->dr_stat));
		rule->dr_drop_at = cfs_rand() % rule->dr_attr.dra_rate;

		spin_unlock(&rule->dr_lock);
	}
	lnet_net_unlock(cpt);
}

static bool
drop_nid_match(lnet_nid_t rule_nid, lnet_nid_t nid)
{
	if (rule_nid == nid || rule_nid == LNET_NID_ANY)
		return true;

	if (LNET_NIDNET(rule_nid) != LNET_NIDNET(nid))
		return false;

	/* 255.255.255.255@net is wildcard for all addresses in this network */
	return LNET_NIDADDR(rule_nid) == LNET_NIDADDR(LNET_NID_ANY);
}

/** source and destination NID can match rule or not */
static bool
drop_rule_nid_match(struct lnet_drop_rule *rule,
		    lnet_nid_t src, lnet_nid_t dst)
{
	return drop_nid_match(rule->dr_attr.dra_src, src) &&
	       drop_nid_match(rule->dr_attr.dra_dst, dst);
}

/**
 * check source/destination NID, portal, message type and drop rate,
 * decide whether should drop this message or not
 */
static bool
drop_rule_match(struct lnet_drop_rule *rule, lnet_nid_t src,
		lnet_nid_t dst, unsigned int type, unsigned int portal)
{
	bool	drop;

	if (rule->dr_start != 0 && rule->dr_start > cfs_time_current_sec())
		return false; /* inactive rule */

	if (!drop_rule_nid_match(rule, src, dst))
		return false;

	if (rule->dr_attr.dra_msg_mask != 0 &&
	    !(rule->dr_attr.dra_msg_mask & (1 << type)))
		return false;

	/* NB: ACK and REPLY have no portal, but they should have been
	 * rejected by message mask */
	if (rule->dr_attr.dra_ptl_mask != 0 && /* has portal filter */
	    !(rule->dr_attr.dra_ptl_mask & (1ULL << portal)))
		return false;

	/* match this rule, check drop rate now */
	spin_lock(&rule->dr_lock);
	if (unlikely(rule->dr_start != 0))
		rule->dr_start = 0; /* activate it */

	drop = rule->dr_stat.drs_count++ == rule->dr_drop_at;
	/* generate the next random rate sequence */
	if (rule->dr_stat.drs_count % rule->dr_attr.dra_rate == 0) {
		rule->dr_drop_at = rule->dr_stat.drs_count +
				   cfs_rand() % rule->dr_attr.dra_rate;

		CDEBUG(D_NET, "Drop Rule %s->%s: next drop: %lu\n",
		       libcfs_nid2str(rule->dr_attr.dra_src),
		       libcfs_nid2str(rule->dr_attr.dra_dst),
		       rule->dr_drop_at);
	}

	if (!drop) {
		spin_unlock(&rule->dr_lock);
		return false;
	}

	rule->dr_stat.drs_dropped++;
	switch (type) {
	case LNET_MSG_PUT:
		rule->dr_stat.drs_put++;
		break;
	case LNET_MSG_ACK:
		rule->dr_stat.drs_ack++;
		break;
	case LNET_MSG_GET:
		rule->dr_stat.drs_get++;
		break;
	case LNET_MSG_REPLY:
		rule->dr_stat.drs_reply++;
		break;
	}

	spin_unlock(&rule->dr_lock);
	return true;
}

/**
 * Check if message from \a src to \a dst can match any existed drop rule
 */
bool
lnet_drop_rule_check(lnet_hdr_t *hdr)
{
	struct lnet_drop_rule	*rule;
	lnet_nid_t		 src = le64_to_cpu(hdr->src_nid);
	lnet_nid_t		 dst = le64_to_cpu(hdr->dest_nid);
	unsigned int		 typ = le32_to_cpu(hdr->type);
	unsigned int		 ptl = -1;
	bool			 drop = false;
	int			 cpt;

	/* NB: if Portal is specified, then only PUT and GET can be
	 * filtered by drop rule */
	if (typ == LNET_MSG_PUT)
		ptl = le32_to_cpu(hdr->msg.put.ptl_index);
	else if (typ == LNET_MSG_GET)
		ptl = le32_to_cpu(hdr->msg.get.ptl_index);

	cpt = lnet_net_lock_current();
	list_for_each_entry(rule, &the_lnet.ln_drop_rules, dr_link) {
		drop = drop_rule_match(rule, src, dst, typ, ptl);
		if (drop)
			break;
	}

	lnet_net_unlock(cpt);
	return drop;
}
