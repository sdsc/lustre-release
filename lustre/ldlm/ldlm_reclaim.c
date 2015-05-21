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
 * ldlm_watermark_low & ldlm_watermark_high is set to ratio of total
 * memory, so the valid numbers are 0 ~ 99. 0 means feature disabled.
 */

/*
 * FIXME:
 *
 * In current implementation, server identifies which locks should be
 * revoked (choose random namespace/resource and pick granted locks
 * by the granted time order), which isn't optimal. The ideal way
 * should be server notifying clients to cancel locks voluntarily,
 * because only client knows exactly when the lock is last used.
 *
 * However how to notify client immediately is a problem, one idea
 * is to leverage the glimplse callbacks on some artificial global
 * lock (like quota global lock does), but that requires protocol
 * changes, let's fix it in future long-term solution.
 */

static struct percpu_counter	ldlm_granted_total;
static struct ptlrpc_thread	ldlm_reclaim_thread;

struct ldlm_reclaim_cb_data {
	struct list_head	rcd_rpc_list;
	__u64			rcd_added;
	__u64			rcd_total;
	int			rcd_skip;
};

static inline __u64 ldlm_granted_threshold(bool high)
{
	__u64 max = NUM_CACHEPAGES;
	int wm = high ? ldlm_watermark_high : ldlm_watermark_low;

	if (wm == 0)
		return 0;

	if (((high && OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_HIGH)) ||
	     (!high && OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_LOW))) &&
	     cfs_fail_val > 0)
		return cfs_fail_val;

	LASSERT(wm < 100 && max > 0 && PAGE_CACHE_SHIFT > 10);
	/* Assume 1k mem can hold 2 ldlm locks */
	max = (max << (PAGE_CACHE_SHIFT - 10)) * 2 * wm;
	do_div(max, 100);
	return max == 0 ? 1 : max;
}

static inline bool ldlm_need_reclaim(bool slow)
{
	__u64 granted, max = ldlm_granted_threshold(false);

	if (max == 0)
		return false;

	granted = slow ? percpu_counter_sum(&ldlm_granted_total) :
			 percpu_counter_read_positive(&ldlm_granted_total);
	return granted > max ? true : false;
}

static inline int ldlm_lock_reclaimable(struct ldlm_lock *lock)
{
	struct ldlm_namespace *ns = ldlm_lock_to_ns(lock);

	/* FLOCK & PLAIN lock are not reclaimable. FLOCK is
	 * explicitly controlled by application, PLAIN lock
	 * is used by quota global lock and config lock.
	 */
	if (ns->ns_client == LDLM_NAMESPACE_SERVER &&
	    (lock->l_resource->lr_type == LDLM_IBITS ||
	     lock->l_resource->lr_type == LDLM_EXTENT))
		return 1;
	return 0;
}

#define LDLM_RECLAIM_AGE	300 /* seconds */
static int ldlm_reclaim_lock_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				struct hlist_node *hnode, void *arg)

{
	struct ldlm_resource		*res;
	struct ldlm_reclaim_cb_data	*data;
	struct ldlm_lock		*lock;
	cfs_duration_t			 age;
	int				 rc = 0;

	data = (struct ldlm_reclaim_cb_data *)arg;

	if (data->rcd_added >= data->rcd_total) {
		return 1;
	} else if (data->rcd_skip != 0) {
		data->rcd_skip--;
		return 0;
	}

	res = cfs_hash_object(hs, hnode);
	age = cfs_time_seconds(LDLM_RECLAIM_AGE);

	lock_res(res);
	list_for_each_entry(lock, &res->lr_granted_lru, l_lru) {
		LASSERT(ldlm_lock_reclaimable(lock));

		/* Don't reclaim the lock being granted a short time ago,
		 * we regard LDLM_RECLAIM_AGE  as 'short time' here. */
		if (!OBD_FAIL_CHECK(OBD_FAIL_LDLM_WATERMARK_LOW) &&
		    cfs_time_before(cfs_time_current(),
				    cfs_time_add(lock->l_grant_time, age)))
			break;

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
	return rc;
}

static void ldlm_reclaim_res(struct ldlm_namespace *ns, __u64 *count)
{
	struct ldlm_reclaim_cb_data	 data;
	int				 nr_res = atomic_read(&ns->ns_bref);
	int				 cursor;
	ENTRY;

	if (nr_res == 0) {
		EXIT;
		return;
	}

	INIT_LIST_HEAD(&data.rcd_rpc_list);
	data.rcd_added = 0;
	/* Try to reclaim half of granted locks from current namespace */
	data.rcd_total = atomic_read(&ns->ns_pool.pl_granted);
	data.rcd_total >>= 1;
	if (data.rcd_total > *count)
		data.rcd_total = *count;
	else if (data.rcd_total == 0)
		data.rcd_total = 1;
	/* Start with a random resource */
	cursor = cfs_rand() % nr_res;
	data.rcd_skip = cursor;

again:
	cfs_hash_for_each_nolock(ns->ns_rs_hash, ldlm_reclaim_lock_cb, &data);
	if ((data.rcd_added < data.rcd_total) && cursor) {
		cursor = data.rcd_skip = 0;
		goto again;
	}

	CDEBUG(D_DLMTRACE, "NS(%s): "LPU64" locks to be reclaimed, found "
	       ""LPU64"/"LPU64" locks.\n", ldlm_ns_name(ns), *count,
	       data.rcd_added, data.rcd_total);

	LASSERTF(*count >= data.rcd_added, "count:"LPU64", added:"LPU64"\n",
		 *count, data.rcd_added);

	ldlm_run_ast_work(ns, &data.rcd_rpc_list, LDLM_WORK_REVOKE_AST);
	*count -= data.rcd_added;
	EXIT;
}

#define LDLM_RECLAIM_BATCH_MAX	(1024 * 256)	/* 256k locks */
static void ldlm_reclaim_ns(void)
{
	struct ldlm_namespace	*ns;
	__u64			 count = ldlm_granted_threshold(false);
	int			 ns_nr, nr_processed = 0;
	ldlm_side_t		 ns_cli = LDLM_NAMESPACE_SERVER;
	ENTRY;

	if (count == 0) {
		EXIT;
		return;
	}

	/* Try to reclaim 10% of low_watermark locks each time */
	if (count > 10)
		do_div(count, 10);
	if (count > LDLM_RECLAIM_BATCH_MAX)
		count = LDLM_RECLAIM_BATCH_MAX;

	ns_nr = ldlm_namespace_nr_read(ns_cli);

	while (count > 0) {
		mutex_lock(ldlm_namespace_lock(ns_cli));

		if (list_empty(ldlm_namespace_list(ns_cli))) {
			mutex_unlock(ldlm_namespace_lock(ns_cli));
			CERROR("No server namespace is found!\n");
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
	EXIT;
}

#define LDLM_RECLAIM_INTERVAL	30	/* seconds */
static int ldlm_reclaim_main(void *arg)
{
	struct ptlrpc_thread	*thread = (struct ptlrpc_thread *)arg;
	struct l_wait_info	 lwi;
	cfs_duration_t		 delay;
	ENTRY;

	thread_set_flags(thread, SVC_RUNNING);
	wake_up(&thread->t_ctl_waitq);

	delay = cfs_time_seconds(LDLM_RECLAIM_INTERVAL);
	while (1) {
		if (thread_is_stopping(thread))
			break;

		thread_clear_flags(thread, SVC_EVENT);

		if (ldlm_need_reclaim(true)) {
			ldlm_reclaim_ns();

			lwi = LWI_TIMEOUT(delay, NULL, NULL);
			l_wait_event(thread->t_ctl_waitq,
				     thread_is_stopping(thread), &lwi);
		} else {
			lwi = LWI_INTR(LWI_ON_SIGNAL_NOOP, NULL);
			l_wait_event(thread->t_ctl_waitq,
				     thread_is_stopping(thread) ||
				     thread_is_event(thread), &lwi);
		}
	}

	thread_set_flags(thread, SVC_STOPPED);
	wake_up(&thread->t_ctl_waitq);
	RETURN(0);
}

static int ldlm_reclaim_thread_start(struct ptlrpc_thread *thread)
{
	struct task_struct	*task;
	struct l_wait_info	 lwi = { 0 };
	ENTRY;

	init_waitqueue_head(&thread->t_ctl_waitq);

	task = kthread_run(ldlm_reclaim_main, thread, "ldlm_reclaimd");
	if (IS_ERR(task)) {
		thread_set_flags(thread, SVC_STOPPED);
		RETURN(PTR_ERR(task));
	}
	l_wait_event(thread->t_ctl_waitq, thread_is_running(thread), &lwi);
	RETURN(0);
}

static void ldlm_reclaim_thread_stop(struct ptlrpc_thread *thread)
{
	struct l_wait_info	 lwi = { 0 };

	if (!thread_is_init(thread) && !thread_is_stopped(thread)) {
		thread_set_flags(thread, SVC_STOPPING);
		wake_up(&thread->t_ctl_waitq);

		l_wait_event(thread->t_ctl_waitq,
			     thread_is_stopped(thread), &lwi);
	}
}

void ldlm_reclaim_add(struct ldlm_lock *lock)
{
	if (!ldlm_lock_reclaimable(lock))
		return;
	percpu_counter_add(&ldlm_granted_total, 1);
	LASSERT(list_empty(&lock->l_lru));
	list_add_tail(&lock->l_lru, &lock->l_resource->lr_granted_lru);
	lock->l_grant_time = cfs_time_current();

	if (ldlm_need_reclaim(false)) {
		thread_add_flags(&ldlm_reclaim_thread, SVC_EVENT);
		wake_up(&ldlm_reclaim_thread.t_ctl_waitq);
	}
}

void ldlm_reclaim_del(struct ldlm_lock *lock)
{
	if (!ldlm_lock_reclaimable(lock))
		return;
	percpu_counter_add(&ldlm_granted_total, -1);
	LASSERT(!list_empty(&lock->l_lru));
	list_del_init(&lock->l_lru);
}

bool ldlm_granted_full(void)
{
	__u64 max = ldlm_granted_threshold(true);

	if (max == 0)
		return false;

	if (percpu_counter_read_positive(&ldlm_granted_total) < max)
		return false;

	thread_add_flags(&ldlm_reclaim_thread, SVC_EVENT);
	wake_up(&ldlm_reclaim_thread.t_ctl_waitq);
	return true;
}

int ldlm_reclaim_setup(void)
{
	int rc;
	rc = percpu_counter_init(&ldlm_granted_total, 0);
	if (rc)
		return rc;
	return ldlm_reclaim_thread_start(&ldlm_reclaim_thread);
}

void ldlm_reclaim_cleanup(void)
{
	ldlm_reclaim_thread_stop(&ldlm_reclaim_thread);
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

int ldlm_reclaim_setup(void)
{
	return 0;
}

void ldlm_reclaim_cleanup(void)
{
}

#endif /* HAVE_SERVER_SUPPORT */
