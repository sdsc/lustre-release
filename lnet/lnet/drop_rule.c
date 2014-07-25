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
 * lnet/lnet/drop-rule.c
 *
 * Author: liang.zhen@intel.com
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

/**
 * Add a new drop rule to LNet
 * There is no check for duplicated drop rule, new rule will always override
 * old rules.
 */
int
lnet_drop_rule_add(lnet_nid_t src, lnet_nid_t dst,
		   unsigned int rate, unsigned int delay)
{
	struct lnet_drop_rule *rule;

	if (rate < LNET_DROP_RATE_MIN) {
		CDEBUG(D_ERROR, "drop rate is too small: %d/%d\n",
		       rate, LNET_DROP_RATE_MIN);
		return -EINVAL;
	}

	CFS_ALLOC_PTR(rule);
	if (rule == NULL)
		return -ENOMEM;

	CDEBUG(D_NET, "Add drop rule: src %s, dst %s, rate %d, delay %d\n",
	       libcfs_nid2str(src), libcfs_nid2str(dst), rate, delay);

	spin_lock_init(&rule->dr_lock);
	rule->dr_src	 = src;
	rule->dr_dst	 = dst;
	rule->dr_rate	 = rate;
	rule->dr_drop_at = cfs_rand() % rule->dr_rate;
	if (delay != 0)
		rule->dr_start = cfs_time_current_sec() + delay;

	/* always add at head of the list, only new rule is checked even if
	 * there is old rule for the same path */
	lnet_net_lock(LNET_LOCK_EX);
	list_add(&rule->dr_link, &the_lnet.ln_drop_rules);
	lnet_net_unlock(LNET_LOCK_EX);

	return 0;
}

/**
 * Remove matched drop rules from lnet, if \a all is true, all rules will
 * be removed, otherwise all matched rules will be removed.
 */
int
lnet_drop_rule_del(lnet_nid_t src, lnet_nid_t dst, bool all)
{
	struct lnet_drop_rule *rule;
	struct lnet_drop_rule *tmp;
	LIST_HEAD	      (zombies);
	int		       n = 0;

	lnet_net_lock(LNET_LOCK_EX);

	list_for_each_entry_safe(rule, tmp, &the_lnet.ln_drop_rules, dr_link) {
		if (all || (rule->dr_src == src && rule->dr_dst == dst)) {
			CDEBUG(D_NET, "Remove drop rule: src %s, dst: %s\n",
			       libcfs_nid2str(src), libcfs_nid2str(dst));
			list_move(&rule->dr_link, &zombies);
		}
	}
	lnet_net_unlock(LNET_LOCK_EX);

	list_for_each_entry_safe(rule, tmp, &zombies, dr_link) {
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
lnet_drop_rule_list(int pos, lnet_nid_t *src, lnet_nid_t *dst,
		    unsigned int *rate, unsigned int *dropped_put,
		    unsigned int *dropped_ack, unsigned int *dropped_get,
		    unsigned int *dropped_reply)
{
	struct lnet_drop_rule *rule;
	int		       rc = -ENOENT;
	int		       i = 0;

	lnet_net_lock(LNET_LOCK_EX);
	list_for_each_entry(rule, &the_lnet.ln_drop_rules, dr_link) {
		if (i++ < pos)
			continue;

		*src		= rule->dr_src;
		*dst		= rule->dr_dst;
		*rate		= rule->dr_rate;
		*dropped_put	= rule->dr_dropped_put;
		*dropped_ack	= rule->dr_dropped_ack;
		*dropped_get	= rule->dr_dropped_get;
		*dropped_reply	= rule->dr_dropped_reply;
		rc = 0;
		break;
	}
	lnet_net_unlock(LNET_LOCK_EX);

	return rc;
}

static bool
drop_nid_match(lnet_nid_t rule_nid, lnet_nid_t nid)
{
	if (rule_nid == LNET_NID_ANY)
		return true;

	if (LNET_NIDNET(rule_nid) != LNET_NIDNET(nid))
		return false;

	/* 255.255.255.255@net is wildcard for all addresses in this net */
	if (LNET_NIDADDR(rule_nid) == LNET_NIDADDR(LNET_NID_ANY) ||
	    rule_nid == nid)
		return true;

	return false;
}

/** source and destination NID can match rule or not */
static bool
drop_rule_nid_check(struct lnet_drop_rule *rule,
		    lnet_nid_t src, lnet_nid_t dst)
{
	return drop_nid_match(rule->dr_src, src) &&
	       drop_nid_match(rule->dr_dst, dst);
}

/** should drop current message or not, based on drop rate of this rule */
static bool
drop_rule_rate_check(struct lnet_drop_rule *rule)
{
	bool	drop;

	if (rule->dr_start != 0 && rule->dr_start > cfs_time_current_sec())
		return false;

	spin_lock(&rule->dr_lock);

	drop = rule->dr_count == rule->dr_drop_at;
	/* generate the next random rate sequence */
	if (++rule->dr_count % rule->dr_rate == 0)
		rule->dr_drop_at = rule->dr_count + cfs_rand() % rule->dr_rate;

	spin_unlock(&rule->dr_lock);

	return drop;
}

/**
 * Check if message from \a src to \a dst can match any existed drop rule
 */
bool
lnet_drop_rule_check(__u32 msg_type, lnet_nid_t src, lnet_nid_t dst)
{
	struct lnet_drop_rule	*rule;
	int			 cpt;
	bool			 drop = false;

	cpt = lnet_net_lock_current();
	list_for_each_entry(rule, &the_lnet.ln_drop_rules, dr_link) {
		if (drop_rule_nid_check(rule, src, dst)) {
			drop = drop_rule_rate_check(rule);
			break;
		}
	}

	if (likely(!drop)) {
		lnet_net_unlock(cpt);
		return false;
	}

	switch (msg_type) {
	case LNET_MSG_PUT:
		rule->dr_dropped_put++;
		break;
	case LNET_MSG_ACK:
		rule->dr_dropped_ack++;
		break;
	case LNET_MSG_GET:
		rule->dr_dropped_get++;
		break;
	case LNET_MSG_REPLY:
		rule->dr_dropped_reply++;
		break;
	}
	lnet_net_unlock(cpt);

	return true;
}
