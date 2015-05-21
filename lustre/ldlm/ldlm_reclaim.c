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
 * GPL HEADER END
 */
/*
 * Copyright (c) 2015, Intel Corporation.
 * Use is subject to license terms.
 *
 * Author: Niu    Yawei    <yawei.niu@intel.com>
 */

#define DEBUG_SUBSYSTEM S_LDLM

#include <linux/kthread.h>
#include <lustre_dlm.h>
#include "ldlm_internal.h"

#ifdef HAVE_SERVER_SUPPORT

/*
 * To avoid ldlm lock exhausting server memory, two global parameters:
 * ldlm_watermark_low & ldlm_watermark_high are used for reclaiming
 * granted locks and rejecting incoming enqueue requests defensively.
 *
 * ldlm_watermark_low: When the amount of granted locks reaching this
 * threshold, server start to revoke locks gradually.
 *
 * ldlm_watermark_high: When the amount of granted locks reaching this
 * threshold, server will return -EINPROGRESS to any incoming enqueue
 * request until the lock count is shrunk below the threshold again.
 *
 * ldlm_watermark_low & ldlm_watermark_high can be set to a ratio of
 * the total memory (1 ~ 99) or kbytes of memory (>= 100), when it's
 * set to zero, the feature is disabled.
 */

/*
 * FIXME:
 *
 * In current implementation, server identifies which locks should be
 * revoked by choosing locks from namespace/resource in roundrobin
 * manner, which isn't optimal. The ideal way should be server notifies
 * clients to cancel locks voluntarily, because only client knows exactly
 * when the lock is last used.
 *
 * However how to notify client immediately is a problem, one idea
 * is to leverage the glimplse callbacks on some artificial global
 * lock (like quota global lock does), but that requires protocol
 * changes, let's fix it in future long-term solution.
 */

static struct percpu_counter	ldlm_granted_total;
static atomic_t			ldlm_nr_reclaimer;

struct ldlm_reclaim_cb_data {
	struct list_head	rcd_rpc_list;
	int			rcd_added;
	int			rcd_total;
	int			rcd_skip;
};

static inline bool ldlm_lock_reclaimable(struct ldlm_lock *lock)
{
	struct ldlm_namespace *ns = ldlm_lock_to_ns(lock);

	/* FLOCK & PLAIN lock are not reclaimable. FLOCK is
	 * explicitly controlled by application, PLAIN lock
	 * is used by quota global lock and config lock.
	 */
	if (ns->ns_client == LDLM_NAMESPACE_SERVER &&
	    (lock->l_resource->lr_type == LDLM_IBITS ||
	     lock->l_resource->lr_type == LDLM_EXTENT))
		return true;
	return false;
}

#define LDLM_RECLAIM_AGE	300 /* seconds */
static int ldlm_reclaim_lock_cb(struct cfs_hash *hs, struct cfs_hash_bd *bd,
				struct hlist_node *hnode, void *arg)

{
	struct ldlm_resource		*res;
	struct ldlm_reclaim_cb_data	*data;
	struct ldlm_lock		*lock;
	cfs_duration_t			 age;
	int				 rc = 0;

	data = (struct ldlm_reclaim_cb_data *)arg;

	LASSERTF(data->rcd_added < data->rcd_total, "added:%d total:%d\n",
		 data->rcd_added, data->rcd_total);

	if (data->rcd_skip != 0) {
		data->rcd_skip--;
		return 0;
	}

	res = cfs_hash_object(hs, hnode);
	age = cfs_time_seconds(LDLM_RECLAIM_AGE);

	lock_res(res);
	list_for_each_entry(lock, &res->lr_granted, l_res_link) {
		if (!ldlm_lock_reclaimable(lock))
			continue;

		/* Don't reclaim the lock being used a short time ago,
		 * we regard LDLM_RECLAIM_AGE  as 'short time' here. */
		if (!OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_LOW) &&
		    cfs_time_before(cfs_time_current(),
				    cfs_time_add(lock->l_last_used, age)))
			continue;

		if (!ldlm_is_ast_sent(lock)) {
			ldlm_set_ast_sent(lock);
			LASSERT(list_empty(&lock->l_rk_ast));
			list_add(&lock->l_rk_ast, &data->rcd_rpc_list);
			LDLM_LOCK_GET(lock);
			if (++data->rcd_added == data->rcd_total) {
				rc = 1; /* stop the iteration */
				break;
			}
		}
	}
	unlock_res(res);

	ldlm_res_to_ns(res)->ns_reclaim_cursor++;
	return rc;
}

static void ldlm_reclaim_res(struct ldlm_namespace *ns, int *count)
{
	struct ldlm_reclaim_cb_data	 data;
	int				 nr_res = atomic_read(&ns->ns_bref);
	unsigned			 cursor;
	ENTRY;

	LASSERT(*count != 0);
	if (nr_res == 0) {
		EXIT;
		return;
	}

	INIT_LIST_HEAD(&data.rcd_rpc_list);
	data.rcd_added = 0;
	data.rcd_total = *count;
	cursor = ns->ns_reclaim_cursor % nr_res;
	data.rcd_skip = cursor;

again:
	cfs_hash_for_each_nolock(ns->ns_rs_hash, ldlm_reclaim_lock_cb, &data);
	if ((data.rcd_added < data.rcd_total) && cursor != 0) {
		cursor = data.rcd_skip = 0;
		goto again;
	}

	CDEBUG(D_DLMTRACE, "NS(%s): %d locks to be reclaimed, found %d/%d "
	       "locks.\n", ldlm_ns_name(ns), *count, data.rcd_added,
	       data.rcd_total);

	LASSERTF(*count >= data.rcd_added, "count:%d, added:%d\n", *count,
		 data.rcd_added);

	ldlm_run_ast_work(ns, &data.rcd_rpc_list, LDLM_WORK_REVOKE_AST);
	*count -= data.rcd_added;
	EXIT;
}

#define LDLM_RECLAIM_BATCH	512	
static void ldlm_reclaim_ns(void)
{
	struct ldlm_namespace	*ns;
	int			 count = LDLM_RECLAIM_BATCH;
	int			 ns_nr, nr_processed = 0;
	ldlm_side_t		 ns_cli = LDLM_NAMESPACE_SERVER;
	ENTRY;

	if (!atomic_add_unless(&ldlm_nr_reclaimer, 1, 1)) {
		EXIT;
		return;
	}

	ns_nr = ldlm_namespace_nr_read(ns_cli);
	while (count > 0) {
		mutex_lock(ldlm_namespace_lock(ns_cli));

		if (list_empty(ldlm_namespace_list(ns_cli))) {
			mutex_unlock(ldlm_namespace_lock(ns_cli));
			break;
		}

		ns = ldlm_namespace_first_locked(ns_cli);
		ldlm_namespace_get(ns);
		ldlm_namespace_move_to_active_locked(ns, ns_cli);
		mutex_unlock(ldlm_namespace_lock(ns_cli));

		ldlm_reclaim_res(ns, &count);
		ldlm_namespace_put(ns);
		nr_processed++;
		if (nr_processed > (ns_nr * 2))
			break;
	}

	atomic_add_unless(&ldlm_nr_reclaimer, -1, 0);
	EXIT;
}

void ldlm_reclaim_add(struct ldlm_lock *lock)
{
	if (!ldlm_lock_reclaimable(lock))
		return;
	percpu_counter_add(&ldlm_granted_total, 1);
	lock->l_last_used = cfs_time_current();
}

void ldlm_reclaim_del(struct ldlm_lock *lock)
{
	if (!ldlm_lock_reclaimable(lock))
		return;
	percpu_counter_sub(&ldlm_granted_total, 1);
}

bool ldlm_granted_full(void)
{
	__u64 high = ldlm_watermark_high;
	__u64 low = ldlm_watermark_low;

	if (low != 0 && OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_LOW))
		low = cfs_fail_val;

	if (low != 0 &&
	    percpu_counter_read_positive(&ldlm_granted_total) > low)
		ldlm_reclaim_ns();

	if (high != 0 && OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_HIGH))
		high = cfs_fail_val;

	if (high != 0 &&
	    percpu_counter_read_positive(&ldlm_granted_total) > high)
		return true;

	return false;
}

#define LDLM_WATERMARK_LOW_DEFAULT	20
#define LDLM_WATERMARK_HIGH_DEFAULT	30
int ldlm_reclaim_setup(void)
{
	atomic_set(&ldlm_nr_reclaimer, 0);
	ldlm_watermark_low = ldlm_wm2locknr(LDLM_WATERMARK_LOW_DEFAULT);
	ldlm_watermark_high = ldlm_wm2locknr(LDLM_WATERMARK_HIGH_DEFAULT);

	return percpu_counter_init(&ldlm_granted_total, 0);
}

void ldlm_reclaim_cleanup(void)
{
	percpu_counter_destroy(&ldlm_granted_total);
}

#else /* HAVE_SERVER_SUPPORT */

bool ldlm_granted_full(void)
{
	return false;
}

void ldlm_reclaim_add(struct ldlm_lock *lock)
{
}

void ldlm_reclaim_del(struct ldlm_lock *lock)
{
}

#endif /* HAVE_SERVER_SUPPORT */
