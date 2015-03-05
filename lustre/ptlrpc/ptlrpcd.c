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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ptlrpc/ptlrpcd.c
 */

/** \defgroup ptlrpcd PortalRPC daemon
 *
 * ptlrpcd is a special thread with its own set where other user might add
 * requests when they don't want to wait for their completion.
 * PtlRPCD will take care of sending such requests and then processing their
 * replies and calling completion callbacks as necessary.
 * The callbacks are called directly from ptlrpcd context.
 * It is important to never significantly block (esp. on RPCs!) within such
 * completion handler or a deadlock might occur where ptlrpcd enters some
 * callback that attempts to send another RPC and wait for it to return,
 * during which time ptlrpcd is completely blocked, so e.g. if import
 * fails, recovery cannot progress because connection requests are also
 * sent by ptlrpcd.
 *
 * @{
 */

#define DEBUG_SUBSYSTEM S_RPC

#include <libcfs/libcfs.h>
#include <lustre_net.h>
#include <lustre_lib.h>
#include <lustre_ha.h>
#include <obd_class.h>   /* for obd_zombie */
#include <obd_support.h> /* for OBD_FAIL_CHECK */
#include <cl_object.h> /* cl_env_{get,put}() */
#include <lprocfs_status.h>

#include "ptlrpc_internal.h"

/* One of these per CPT. */
struct ptlrpcd {
	int			pd_size;
	int			pd_index;
	int			pd_cpt;
	int			pd_cursor;
	int			pd_nthreads;
	struct ptlrpcd_ctl	pd_thread_rcv;
	struct ptlrpcd_ctl	pd_threads[0];
};

static int max_ptlrpcds;
CFS_MODULE_PARM(max_ptlrpcds, "i", int, 0644,
		"Max ptlrpcd thread count to be started per cpt.");

static int ptlrpcd_bind_policy = PDB_POLICY_PAIR;
CFS_MODULE_PARM(ptlrpcd_bind_policy, "i", int, 0644,
		"Ptlrpcd threads binding mode.");

/* Temp parameter to allow before/after testing w/o recompiling. */
static int ptlrpcd_use_cpts;
CFS_MODULE_PARM(ptlrpcd_use_cpts, "i", int, 0644,
		"Make ptlrpcd threads honour CPU partitions.");

static char *ptlrpcd_cpts;
CFS_MODULE_PARM(ptlrpcd_cpts, "s", charp, 0644,
		"CPU partitions ptlrpcd threads should run on");

static int		ptlrpcds_ncpts;
static __u32		*ptlrpcds_cpts;
static struct ptlrpcd	**ptlrpcds;

struct mutex ptlrpcd_mutex;
static int ptlrpcd_users = 0;

void ptlrpcd_wake(struct ptlrpc_request *req)
{
	struct ptlrpc_request_set *set = req->rq_set;

	LASSERT(set != NULL);
	wake_up(&set->set_waitq);
}
EXPORT_SYMBOL(ptlrpcd_wake);

static struct ptlrpcd_ctl *
ptlrpcd_select_pc(struct ptlrpc_request *req, pdl_policy_t policy, int index)
{
	struct ptlrpcd	*pd;
	int		cpt;
	int		idx;

	/*
	 * Use the struct ptlrpcd for the current CPT if there is one.
	 * Otherwise pick an arbitrary one.
	 */
	cpt = cfs_cpt_current(cfs_cpt_table, 1);
	if (ptlrcpd_use_cpts == 0) {
		idx = 0;
	} else if (ptlrpcds_cpts == NULL) {
		idx = cpt;
	} else {
		for (idx = 0; idx < ptlrpcds_ncpts; idx++)
			if (ptlrpcds_cpts[idx] == cpt)
				break;
		if (idx >= ptlrpcds_ncpts)
			idx = cpt % ptlrpcds_ncpts;
	}
	pd = ptlrpcds[idx];

	if (req != NULL && req->rq_send_state != LUSTRE_IMP_FULL)
		return &pd->pd_thread_rcv;

	switch (policy) {
	case PDL_POLICY_SAME:
		if (pd->pd_cpt == CFS_CPT_ANY) {
			idx = smp_processor_id() % pd->pd_nthreads;
			break;
		}
		for (idx = 0; idx < pd->pd_nthreads; idx++)
			if (pd->pd_threads[idx].pc_cpu == smp_processor_id())
				break;
		if (idx < pd->pd_nthreads)
			break;
		/* Fall through to PDL_POLICY_LOCAL */
	case PDL_POLICY_LOCAL:
		/* Fall through to PDL_POLICY_ROUND. */
		index = -1;
	case PDL_POLICY_PREFERRED:
		if (index >= 0 && index < num_online_cpus()) {
			if (pd->pd_cpt == CFS_CPT_ANY) {
				idx = index % pd->pd_nthreads;
				break;
			}
			for (idx = 0; idx < pd->pd_nthreads; idx++)
				if (pd->pd_threads[idx].pc_cpu == index)
					break;
			if (idx < pd->pd_nthreads)
				break;
		}
		/* Fall through to PDL_POLICY_ROUND for bad index. */
	default:
		/* Fall through to PDL_POLICY_ROUND for unknown policy. */
	case PDL_POLICY_ROUND:
		/* We do not care whether it is strict load balance. */
		idx = pd->pd_cursor;
		do {
			if (++idx == pd->pd_nthreads)
				idx = 0;
		} while (pd->pd_threads[idx].pc_cpu == smp_processor_id());
		pd->pd_cursor = idx;
		break;
	}

	return &pd->pd_threads[idx];
}

/**
 * Move all request from an existing request set to the ptlrpcd queue.
 * All requests from the set must be in phase RQ_PHASE_NEW.
 */
void ptlrpcd_add_rqset(struct ptlrpc_request_set *set)
{
	struct list_head *tmp, *pos;
        struct ptlrpcd_ctl *pc;
        struct ptlrpc_request_set *new;
        int count, i;

        pc = ptlrpcd_select_pc(NULL, PDL_POLICY_LOCAL, -1);
        new = pc->pc_set;

	list_for_each_safe(pos, tmp, &set->set_requests) {
		struct ptlrpc_request *req =
			list_entry(pos, struct ptlrpc_request,
				   rq_set_chain);

		LASSERT(req->rq_phase == RQ_PHASE_NEW);
		req->rq_set = new;
		req->rq_queued_time = cfs_time_current();
	}

	spin_lock(&new->set_new_req_lock);
	list_splice_init(&set->set_requests, &new->set_new_requests);
	i = atomic_read(&set->set_remaining);
	count = atomic_add_return(i, &new->set_new_count);
	atomic_set(&set->set_remaining, 0);
	spin_unlock(&new->set_new_req_lock);
	if (count == i) {
		wake_up(&new->set_waitq);

		/* XXX: It maybe unnecessary to wakeup all the partners. But to
		 *      guarantee the async RPC can be processed ASAP, we have
		 *      no other better choice. It maybe fixed in future. */
		for (i = 0; i < pc->pc_npartners; i++)
			wake_up(&pc->pc_partners[i]->pc_set->set_waitq);
	}
}

/**
 * Return transferred RPCs count.
 */
static int ptlrpcd_steal_rqset(struct ptlrpc_request_set *des,
                               struct ptlrpc_request_set *src)
{
	struct list_head *tmp, *pos;
	struct ptlrpc_request *req;
	int rc = 0;

	spin_lock(&src->set_new_req_lock);
	if (likely(!list_empty(&src->set_new_requests))) {
		list_for_each_safe(pos, tmp, &src->set_new_requests) {
			req = list_entry(pos, struct ptlrpc_request,
					 rq_set_chain);
			req->rq_set = des;
		}
		list_splice_init(&src->set_new_requests,
				 &des->set_requests);
		rc = atomic_read(&src->set_new_count);
		atomic_add(rc, &des->set_remaining);
		atomic_set(&src->set_new_count, 0);
	}
	spin_unlock(&src->set_new_req_lock);
	return rc;
}

/**
 * Requests that are added to the ptlrpcd queue are sent via
 * ptlrpcd_check->ptlrpc_check_set().
 */
void ptlrpcd_add_req(struct ptlrpc_request *req, pdl_policy_t policy, int idx)
{
	struct ptlrpcd_ctl *pc;

	if (req->rq_reqmsg)
		lustre_msg_set_jobid(req->rq_reqmsg, NULL);

	spin_lock(&req->rq_lock);
	if (req->rq_invalid_rqset) {
		struct l_wait_info lwi = LWI_TIMEOUT(cfs_time_seconds(5),
						     back_to_sleep, NULL);

		req->rq_invalid_rqset = 0;
		spin_unlock(&req->rq_lock);
		l_wait_event(req->rq_set_waitq, (req->rq_set == NULL), &lwi);
	} else if (req->rq_set) {
		/* If we have a vaid "rq_set", just reuse it to avoid double
		 * linked. */
		LASSERT(req->rq_phase == RQ_PHASE_NEW);
		LASSERT(req->rq_send_state == LUSTRE_IMP_REPLAY);

		/* ptlrpc_check_set will decrease the count */
		atomic_inc(&req->rq_set->set_remaining);
		spin_unlock(&req->rq_lock);
		wake_up(&req->rq_set->set_waitq);
		return;
	} else {
		spin_unlock(&req->rq_lock);
	}

	pc = ptlrpcd_select_pc(req, policy, idx);

	DEBUG_REQ(D_INFO, req, "add req [%p] to pc [%s:%d]",
		  req, pc->pc_name, pc->pc_index);

	ptlrpc_set_add_new_req(pc, req);
}
EXPORT_SYMBOL(ptlrpcd_add_req);

static inline void ptlrpc_reqset_get(struct ptlrpc_request_set *set)
{
	atomic_inc(&set->set_refcount);
}

/**
 * Check if there is more work to do on ptlrpcd set.
 * Returns 1 if yes.
 */
static int ptlrpcd_check(struct lu_env *env, struct ptlrpcd_ctl *pc)
{
	struct list_head *tmp, *pos;
        struct ptlrpc_request *req;
        struct ptlrpc_request_set *set = pc->pc_set;
        int rc = 0;
        int rc2;
        ENTRY;

	if (atomic_read(&set->set_new_count)) {
		spin_lock(&set->set_new_req_lock);
		if (likely(!list_empty(&set->set_new_requests))) {
			list_splice_init(&set->set_new_requests,
					     &set->set_requests);
			atomic_add(atomic_read(&set->set_new_count),
				   &set->set_remaining);
			atomic_set(&set->set_new_count, 0);
			/*
			 * Need to calculate its timeout.
			 */
			rc = 1;
		}
		spin_unlock(&set->set_new_req_lock);
	}

	/* We should call lu_env_refill() before handling new requests to make
	 * sure that env key the requests depending on really exists.
	 */
	rc2 = lu_env_refill(env);
	if (rc2 != 0) {
		/*
		 * XXX This is very awkward situation, because
		 * execution can neither continue (request
		 * interpreters assume that env is set up), nor repeat
		 * the loop (as this potentially results in a tight
		 * loop of -ENOMEM's).
		 *
		 * Fortunately, refill only ever does something when
		 * new modules are loaded, i.e., early during boot up.
		 */
		CERROR("Failure to refill session: %d\n", rc2);
		RETURN(rc);
	}

	if (atomic_read(&set->set_remaining))
		rc |= ptlrpc_check_set(env, set);

	/* NB: ptlrpc_check_set has already moved complted request at the
	 * head of seq::set_requests */
	list_for_each_safe(pos, tmp, &set->set_requests) {
		req = list_entry(pos, struct ptlrpc_request, rq_set_chain);
		if (req->rq_phase != RQ_PHASE_COMPLETE)
			break;

		list_del_init(&req->rq_set_chain);
		req->rq_set = NULL;
		ptlrpc_req_finished(req);
	}

	if (rc == 0) {
		/*
		 * If new requests have been added, make sure to wake up.
		 */
		rc = atomic_read(&set->set_new_count);

                /* If we have nothing to do, check whether we can take some
                 * work from our partner threads. */
                if (rc == 0 && pc->pc_npartners > 0) {
                        struct ptlrpcd_ctl *partner;
                        struct ptlrpc_request_set *ps;
                        int first = pc->pc_cursor;

                        do {
                                partner = pc->pc_partners[pc->pc_cursor++];
                                if (pc->pc_cursor >= pc->pc_npartners)
                                        pc->pc_cursor = 0;
                                if (partner == NULL)
                                        continue;

				spin_lock(&partner->pc_lock);
				ps = partner->pc_set;
				if (ps == NULL) {
					spin_unlock(&partner->pc_lock);
					continue;
				}

				ptlrpc_reqset_get(ps);
				spin_unlock(&partner->pc_lock);

				if (atomic_read(&ps->set_new_count)) {
					rc = ptlrpcd_steal_rqset(set, ps);
					if (rc > 0)
						CDEBUG(D_RPCTRACE, "transfer %d"
						       " async RPCs [%d->%d]\n",
						       rc, partner->pc_index,
						       pc->pc_index);
				}
				ptlrpc_reqset_put(ps);
			} while (rc == 0 && pc->pc_cursor != first);
		}
	}

	RETURN(rc);
}

/**
 * Main ptlrpcd thread.
 * ptlrpc's code paths like to execute in process context, so we have this
 * thread which spins on a set which contains the rpcs and sends them.
 *
 */
static int ptlrpcd(void *arg)
{
	struct ptlrpcd_ctl		*pc = arg;
	struct ptlrpc_request_set	*set = pc->pc_set;
	struct ptlrpcd			*pd;
	struct lu_context		ses = { 0 };
	struct lu_env			env = { .le_ses = &ses };
	int				rc;
	int				exit = 0;
	ENTRY;

	unshare_fs_struct();

	pd = (void *)pc - offsetof(struct ptlrpcd, pd_threads[pc->pc_index]);

	/* If a CPT is defined, always bind to that CPT. */
	if (pd->pd_cpt != CFS_CPT_ANY) {
		if (cfs_cpt_bind(cfs_cpt_table, pd->pd_cpt) != 0) {
			CWARN("Failed to bind %s on CPT %d\n",
			      pc->pc_name, pd->pd_cpt);
		}
#if defined(CONFIG_SMP)
		if (test_bit(LIOD_BIND, &pc->pc_flags)) {
			cpumask_t	*mask;
			int		index;

			mask = cfs_cpt_cpumask(cfs_cpt_table, pd->pd_cpt);
			pc->pc_cpu = cpumask_first(mask);
			for (index = pc->pc_index; index > 0; index--)
				pc->pc_cpu = cpumask_next(pc->pc_cpu, mask);
			set_cpus_allowed_ptr(current, cpumask_of(pc->pc_cpu));
		}
	} else if (test_bit(LIOD_BIND, &pc->pc_flags)) {
		int	index = pc->pc_index;

		if (index >= 0 && index < num_possible_cpus()) {
			while (!cpu_online(index)) {
				if (++index >= num_possible_cpus())
					index = 0;
			}
			set_cpus_allowed_ptr(current,
				     cpumask_of_node(cpu_to_node(index)));
		}
#endif
	}

	/* Both client and server (MDT/OST) may use the environment. */
	rc = lu_context_init(&env.le_ctx, LCT_MD_THREAD | LCT_DT_THREAD |
					  LCT_CL_THREAD | LCT_REMEMBER |
					  LCT_NOREF);
	if (rc == 0) {
		rc = lu_context_init(env.le_ses,
				     LCT_SESSION|LCT_REMEMBER|LCT_NOREF);
		if (rc != 0)
			lu_context_fini(&env.le_ctx);
	}
	complete(&pc->pc_starting);

        if (rc != 0)
                RETURN(rc);

        /*
         * This mainloop strongly resembles ptlrpc_set_wait() except that our
         * set never completes.  ptlrpcd_check() calls ptlrpc_check_set() when
         * there are requests in the set. New requests come in on the set's
         * new_req_list and ptlrpcd_check() moves them into the set.
         */
        do {
                struct l_wait_info lwi;
                int timeout;

                timeout = ptlrpc_set_next_timeout(set);
                lwi = LWI_TIMEOUT(cfs_time_seconds(timeout ? timeout : 1),
                                  ptlrpc_expired_set, set);

		lu_context_enter(&env.le_ctx);
		lu_context_enter(env.le_ses);
		l_wait_event(set->set_waitq, ptlrpcd_check(&env, pc), &lwi);
		lu_context_exit(&env.le_ctx);
		lu_context_exit(env.le_ses);

		/*
		 * Abort inflight rpcs for forced stop case.
		 */
		if (test_bit(LIOD_STOP, &pc->pc_flags)) {
			if (test_bit(LIOD_FORCE, &pc->pc_flags))
                                ptlrpc_abort_set(set);
                        exit++;
                }

                /*
                 * Let's make one more loop to make sure that ptlrpcd_check()
                 * copied all raced new rpcs into the set so we can kill them.
                 */
        } while (exit < 2);

        /*
         * Wait for inflight requests to drain.
         */
	if (!list_empty(&set->set_requests))
                ptlrpc_set_wait(set);
	lu_context_fini(&env.le_ctx);
	lu_context_fini(env.le_ses);

	complete(&pc->pc_finishing);

	return 0;
}

/* XXX: We want multiple CPU cores to share the async RPC load. So we start many
 *      ptlrpcd threads. We also want to reduce the ptlrpcd overhead caused by
 *      data transfer cross-CPU cores. So we bind ptlrpcd thread to specified
 *      CPU core. But binding all ptlrpcd threads maybe cause response delay
 *      because of some CPU core(s) busy with other loads.
 *
 *      For example: "ls -l", some async RPCs for statahead are assigned to
 *      ptlrpcd_0, and ptlrpcd_0 is bound to CPU_0, but CPU_0 may be quite busy
 *      with other non-ptlrpcd, like "ls -l" itself (we want to the "ls -l"
 *      thread, statahead thread, and ptlrpcd thread can run in parallel), under
 *      such case, the statahead async RPCs can not be processed in time, it is
 *      unexpected. If ptlrpcd_0 can be re-scheduled on other CPU core, it may
 *      be better. But it breaks former data transfer policy.
 *
 *      So we shouldn't be blind for avoiding the data transfer. We make some
 *      compromise: divide the ptlrpcd threds pool into two parts. One part is
 *      for bound mode, each ptlrpcd thread in this part is bound to some CPU
 *      core. The other part is for free mode, all the ptlrpcd threads in the
 *      part can be scheduled on any CPU core. We specify some partnership
 *      between bound mode ptlrpcd thread(s) and free mode ptlrpcd thread(s),
 *      and the async RPC load within the partners are shared.
 *
 *      It can partly avoid data transfer cross-CPU (if the bound mode ptlrpcd
 *      thread can be scheduled in time), and try to guarantee the async RPC
 *      processed ASAP (as long as the free mode ptlrpcd thread can be scheduled
 *      on any CPU core).
 *
 *      As for how to specify the partnership between bound mode ptlrpcd
 *      thread(s) and free mode ptlrpcd thread(s), the simplest way is to use
 *      <free bound> pair. In future, we can specify some more complex
 *      partnership based on the patches for CPU partition. But before such
 *      patches are available, we prefer to use the simplest one.
 */
static int ptlrpcd_partners(struct ptlrpcd *pd, int index)
{
	struct ptlrpcd_ctl	*pc;
#if defined(CONFIG_NUMA)
	struct ptlrpcd_ctl	*ppc;
	int			i;
#endif
	int			rc = 0;
	ENTRY;

	LASSERT(index >= 0 && index < pd->pd_nthreads);
	pc = &pd->pd_threads[index];
	switch (ptlrpcd_bind_policy) {
	case PDB_POLICY_NONE:
		pc->pc_npartners = -1;
		break;
	case PDB_POLICY_FULL:
		pc->pc_npartners = 0;
		set_bit(LIOD_BIND, &pc->pc_flags);
		break;
	case PDB_POLICY_PAIR:
		LASSERT(pd->pd_nthreads % 2 == 0);
		pc->pc_npartners = 1;
		break;
	case PDB_POLICY_NEIGHBOR:
#if defined(CONFIG_NUMA)
		pc->pc_npartners = pd->pd_nthreads - 1;
		set_bit(LIOD_BIND, &pc->pc_flags);
#else
		LASSERT(pd->pd_nthreads >= 3);
		pc->pc_npartners = 2;
#endif
		break;
	default:
		CERROR("unknown ptlrpcd bind policy %d\n", ptlrpcd_bind_policy);
		rc = -EINVAL;
	}

	if (rc != 0)
		GOTO(out, rc);
	if (pc->pc_npartners <= 0)
		GOTO(out, rc);

	OBD_CPT_ALLOC(pc->pc_partners, cfs_cpt_table, pd->pd_cpt,
		      sizeof(struct ptlrpcd_ctl *) * pc->pc_npartners);
	if (pc->pc_partners == NULL) {
		pc->pc_npartners = 0;
		GOTO(out, rc = -ENOMEM);
	}

	switch (ptlrpcd_bind_policy) {
	case PDB_POLICY_PAIR:
		if (index & 0x1) {
			set_bit(LIOD_BIND, &pc->pc_flags);
			pc->pc_partners[0] = &pd->pd_threads[index - 1];
			pd->pd_threads[index - 1].pc_partners[0] = pc;
		}
		break;
	case PDB_POLICY_NEIGHBOR:
#if defined(CONFIG_NUMA)
		/*
		 * partners are cores in the same cpu partition. setup
		 * partnership only with ptlrpcd threads that are
		 * already initialized.
		 */
		for (i = 0; i < index; i++) {
			ppc = &pd->pd_threads[i];
			pc->pc_partners[i] = ppc;
			ppc->pc_partners[index - 1] = pc;
		}
#else
		if (index & 0x1)
			set_bit(LIOD_BIND, &pc->pc_flags);
		if (index > 0) {
			pc->pc_partners[0] = &pd->pd_threads[index - 1];
			pd->pd_threads[index - 1].pc_partners[1] = pc;
			if (index == pd->pd_nthreads - 1) {
				pc->pc_partners[1] = &pd->pd_threads[0];
				pd->pd_threads[0].pc_partners[0] = pc;
			}
		}
#endif
		break;
	}
out:
	EXIT;
	RETURN(rc);
}

int ptlrpcd_start(struct ptlrpcd *pd, int index, const char *name)
{
	struct ptlrpcd_ctl	*pc;
	struct task_struct	*task;
	int			rc = 0;
        ENTRY;

	LASSERT(index >= -1 && index < pd->pd_nthreads);
	if (index < 0)
		pc = &pd->pd_thread_rcv;
	else
		pc = &pd->pd_threads[index];

        /*
         * Do not allow start second thread for one pc.
         */
	if (test_and_set_bit(LIOD_START, &pc->pc_flags)) {
		CWARN("Starting second thread (%s) for same pc %p\n",
		      name, pc);
		RETURN(0);
	}

	pc->pc_cpu = -1;
	pc->pc_index = index;
	init_completion(&pc->pc_starting);
	init_completion(&pc->pc_finishing);
	spin_lock_init(&pc->pc_lock);
	strlcpy(pc->pc_name, name, sizeof(pc->pc_name));
        pc->pc_set = ptlrpc_prep_set();
        if (pc->pc_set == NULL)
                GOTO(out, rc = -ENOMEM);

        /*
         * So far only "client" ptlrpcd uses an environment. In the future,
         * ptlrpcd thread (or a thread-set) has to be given an argument,
         * describing its "scope".
         */
        rc = lu_context_init(&pc->pc_env.le_ctx, LCT_CL_THREAD|LCT_REMEMBER);
        if (rc != 0)
		GOTO(out_set, rc);

	if (index >= 0) {
		rc = ptlrpcd_partners(pd, index);
		if (rc < 0)
			GOTO(out_env, rc);
	}

	task = kthread_run(ptlrpcd, pc, pc->pc_name);
	if (IS_ERR(task))
		GOTO(out_env, rc = PTR_ERR(task));

	wait_for_completion(&pc->pc_starting);

	RETURN(0);

out_env:
	lu_context_fini(&pc->pc_env.le_ctx);

out_set:
	if (pc->pc_set != NULL) {
		struct ptlrpc_request_set *set = pc->pc_set;

		spin_lock(&pc->pc_lock);
		pc->pc_set = NULL;
		spin_unlock(&pc->pc_lock);
		ptlrpc_set_destroy(set);
	}
	clear_bit(LIOD_BIND, &pc->pc_flags);
out:
	clear_bit(LIOD_START, &pc->pc_flags);
	RETURN(rc);
}

void ptlrpcd_stop(struct ptlrpcd_ctl *pc, int force)
{
	ENTRY;

	if (!test_bit(LIOD_START, &pc->pc_flags)) {
		CWARN("Thread for pc %p was not started\n", pc);
		goto out;
	}

	set_bit(LIOD_STOP, &pc->pc_flags);
	if (force)
		set_bit(LIOD_FORCE, &pc->pc_flags);
	wake_up(&pc->pc_set->set_waitq);

out:
	EXIT;
}

void ptlrpcd_free(struct ptlrpcd_ctl *pc)
{
	struct ptlrpc_request_set *set = pc->pc_set;
	ENTRY;

	if (!test_bit(LIOD_START, &pc->pc_flags)) {
		CWARN("Thread for pc %p was not started\n", pc);
		goto out;
	}

	wait_for_completion(&pc->pc_finishing);
	lu_context_fini(&pc->pc_env.le_ctx);

	spin_lock(&pc->pc_lock);
	pc->pc_set = NULL;
	spin_unlock(&pc->pc_lock);
	ptlrpc_set_destroy(set);

	clear_bit(LIOD_START, &pc->pc_flags);
	clear_bit(LIOD_STOP, &pc->pc_flags);
	clear_bit(LIOD_FORCE, &pc->pc_flags);
	clear_bit(LIOD_BIND, &pc->pc_flags);

out:
        if (pc->pc_npartners > 0) {
                LASSERT(pc->pc_partners != NULL);

                OBD_FREE(pc->pc_partners,
                         sizeof(struct ptlrpcd_ctl *) * pc->pc_npartners);
                pc->pc_partners = NULL;
        }
        pc->pc_npartners = 0;
        EXIT;
}

static void ptlrpcd_fini(void)
{
	int	i;
	int	j;
	ENTRY;

	if (ptlrpcds != NULL) {
		for (i = 0; i < ptlrpcds_ncpts; i++) {
			if (ptlrpcds[i] == NULL)
				continue;
			for (j = 0; j < ptlrpcds[i]->pd_nthreads; j++)
				ptlrpcd_stop(&ptlrpcds[i]->pd_threads[j], 0);
			for (j = 0; j < ptlrpcds[i]->pd_nthreads; j++)
				ptlrpcd_free(&ptlrpcds[i]->pd_threads[j]);
			ptlrpcd_stop(&ptlrpcds[i]->pd_thread_rcv, 0);
			ptlrpcd_free(&ptlrpcds[i]->pd_thread_rcv);
			OBD_FREE(ptlrpcds[i], ptlrpcds[i]->pd_size);
			ptlrpcds[i] = NULL;
		}
		OBD_FREE(ptlrpcds, sizeof(struct ptlrpcd *) * ptlrpcds_ncpts);
	}

	if (ptlrpcds_cpts != NULL) {
		cfs_expr_list_values_free(ptlrpcds_cpts, ptlrpcds_ncpts);
		ptlrpcds_cpts = NULL;
	}
	ptlrpcds_ncpts = 0;

	EXIT;
}

static int ptlrpcd_init(void)
{
	int			nthreads;
	char			name[16];
	int			size;
	int			i;
	int			j;
	int			rc = 0;
	struct cfs_cpt_table	*cptable;
	__u32			*cpts = NULL;
	int			ncpts;
	int			cpt;
	struct ptlrpcd		*pd;
	ENTRY;

	cptable = cfs_cpt_table;
	if (ptlrpcd_use_cpts == 0) {
		ncpts = 1;
	} else {
		ncpts = cfs_cpt_number(cptable);
		if (ptlrpcd_cpts != NULL) {
			struct cfs_expr_list	*el;

			rc = cfs_expr_list_parse(ptlrpcd_cpts,
						 strlen(ptlrpcd_cpts),
						 0, ncpts - 1, &el);
			if (rc != 0) {
				CERROR("%s: invalid CPT pattern string: %s",
				       "ptlrpcd_cpts", ptlrpcd_cpts);
				GOTO(out, rc = -EINVAL);
			}

			rc = cfs_expr_list_values(el, ncpts, &cpts);
			cfs_expr_list_free(el);
			if (rc <= 0) {
				CERROR("%s: failed to parse CPT array %s: %d\n",
				       "ptlrpcd_cpts", ptlrpcd_cpts, rc);
				if (rc == 0)
					rc = -EINVAL;
				GOTO(out, rc);
			}
			ncpts = rc;
		}
	}
	ptlrpcds_ncpts = ncpts;
	ptlrpcds_cpts = cpts;

	size = ncpts * sizeof(struct ptlrpcd *);
	OBD_ALLOC(ptlrpcds, size);
	if (ptlrpcds == NULL)
		GOTO(out, rc = -ENOMEM);
	memset(ptlrpcds, 0, size);

	for (i = 0; i < ncpts; i++) {
		if (ptlrpcd_use_cpts == 0)
			cpt = CFS_CPT_ANY;
		else
			cpt = (cpts != NULL ? cpts[i] : i);

		nthreads = cfs_cpt_weight(cptable, cpt);
		if (max_ptlrpcds > 0 && max_ptlrpcds < nthreads)
			nthreads = max_ptlrpcds;
		if (nthreads < 2)
			nthreads = 2;
		if (nthreads < 3 && ptlrpcd_bind_policy == PDB_POLICY_NEIGHBOR)
			ptlrpcd_bind_policy = PDB_POLICY_PAIR;
		if (nthreads % 2 != 0 && ptlrpcd_bind_policy == PDB_POLICY_PAIR)
			nthreads &= ~1; /* make sure it is even */

		size = offsetof(struct ptlrpcd, pd_threads[nthreads]);
		OBD_CPT_ALLOC(pd, cptable, cpt, size);
		if (!pd)
			GOTO(out, rc = -ENOMEM);
		memset(pd, 0, size);
		pd->pd_size = size;
		pd->pd_index = i;
		pd->pd_cpt = cpt;
		pd->pd_cursor = 0;
		pd->pd_nthreads = nthreads;
		ptlrpcds[i] = pd;

		if (cpt == CFS_CPT_ANY)
			snprintf(name, sizeof(name), "ptlrpcd_rcv");
		else
			snprintf(name, sizeof(name), "ptlrpcd_%02d_rcv", cpt);
		set_bit(LIOD_RECOVERY, &pd->pd_thread_rcv.pc_flags);
		rc = ptlrpcd_start(pd, -1, name);
		if (rc < 0)
			GOTO(out, rc);

		/* XXX: We start nthreads ptlrpc daemons on this cpt.
		 *      Each of them can process any non-recovery
		 *      async RPC to improve overall async RPC
		 *      efficiency.
		 *
		 *      But there are some issues with async I/O RPCs
		 *      and async non-I/O RPCs processed in the same
		 *      set under some cases. The ptlrpcd may be
		 *      blocked by some async I/O RPC(s), then will
		 *      cause other async non-I/O RPC(s) can not be
		 *      processed in time.
		 *
		 *      Maybe we should distinguish blocked async RPCs
		 *      from non-blocked async RPCs, and process them
		 *      in different ptlrpcd sets to avoid unnecessary
		 *      dependency. But how to distribute async RPCs
		 *      load among all the ptlrpc daemons becomes
		 *      another trouble.
		 */
		for (j = 0; j < nthreads; j++) {
			if (cpt == CFS_CPT_ANY) {
				snprintf(name, sizeof(name),
					"ptlrpcd_%02d", j);
			} else {
				snprintf(name, sizeof(name),
					"ptlrpcd_%02d_%02d", cpt, j);
			}
			rc = ptlrpcd_start(pd, j, name);
			if (rc < 0)
				GOTO(out, rc);
		}
	}
out:
	if (rc != 0)
		ptlrpcd_fini();

	RETURN(rc);
}

int ptlrpcd_addref(void)
{
        int rc = 0;
        ENTRY;

	mutex_lock(&ptlrpcd_mutex);
        if (++ptlrpcd_users == 1) {
		rc = ptlrpcd_init();
		if (rc < 0)
			ptlrpcd_users--;
	}
	mutex_unlock(&ptlrpcd_mutex);
        RETURN(rc);
}
EXPORT_SYMBOL(ptlrpcd_addref);

void ptlrpcd_decref(void)
{
	mutex_lock(&ptlrpcd_mutex);
        if (--ptlrpcd_users == 0)
                ptlrpcd_fini();
	mutex_unlock(&ptlrpcd_mutex);
}
EXPORT_SYMBOL(ptlrpcd_decref);
/** @} ptlrpcd */
