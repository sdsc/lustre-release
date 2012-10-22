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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lu_object.h>
#include <lnet/types.h>
#include "ptlrpc_internal.h"

/* The following are visible and mutable through /sys/module/ptlrpc */
int test_req_buffer_pressure = 0;
CFS_MODULE_PARM(test_req_buffer_pressure, "i", int, 0444,
                "set non-zero to put pressure on request buffer pools");
CFS_MODULE_PARM(at_min, "i", int, 0644,
                "Adaptive timeout minimum (sec)");
CFS_MODULE_PARM(at_max, "i", int, 0644,
                "Adaptive timeout maximum (sec)");
CFS_MODULE_PARM(at_history, "i", int, 0644,
                "Adaptive timeouts remember the slowest event that took place "
                "within this period (sec)");
CFS_MODULE_PARM(at_early_margin, "i", int, 0644,
                "How soon before an RPC deadline to send an early reply");
CFS_MODULE_PARM(at_extra, "i", int, 0644,
                "How much extra time to give with each early reply");


/* forward ref */
static int ptlrpc_server_post_idle_rqbds(struct ptlrpc_service_part *svcpt);
static void ptlrpc_server_hpreq_fini(struct ptlrpc_request *req);
static void ptlrpc_at_remove_timed(struct ptlrpc_request *req);

static CFS_LIST_HEAD(ptlrpc_all_services);
cfs_spinlock_t ptlrpc_all_services_lock;

struct ptlrpc_request_buffer_desc *
ptlrpc_alloc_rqbd(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_service		  *svc = svcpt->scp_service;
	struct ptlrpc_request_buffer_desc *rqbd;

	OBD_CPT_ALLOC_PTR(rqbd, svc->srv_cptable, svcpt->scp_cpt);
	if (rqbd == NULL)
		return NULL;

	rqbd->rqbd_svcpt = svcpt;
	rqbd->rqbd_refcount = 0;
	rqbd->rqbd_cbid.cbid_fn = request_in_callback;
	rqbd->rqbd_cbid.cbid_arg = rqbd;
	CFS_INIT_LIST_HEAD(&rqbd->rqbd_reqs);
	OBD_CPT_ALLOC_LARGE(rqbd->rqbd_buffer, svc->srv_cptable,
			    svcpt->scp_cpt, svc->srv_buf_size);
	if (rqbd->rqbd_buffer == NULL) {
		OBD_FREE_PTR(rqbd);
		return NULL;
	}

	cfs_spin_lock(&svcpt->scp_lock);
	cfs_list_add(&rqbd->rqbd_list, &svcpt->scp_rqbd_idle);
	svcpt->scp_nrqbds_total++;
	cfs_spin_unlock(&svcpt->scp_lock);

	return rqbd;
}

void
ptlrpc_free_rqbd(struct ptlrpc_request_buffer_desc *rqbd)
{
	struct ptlrpc_service_part *svcpt = rqbd->rqbd_svcpt;

	LASSERT(rqbd->rqbd_refcount == 0);
	LASSERT(cfs_list_empty(&rqbd->rqbd_reqs));

	cfs_spin_lock(&svcpt->scp_lock);
	cfs_list_del(&rqbd->rqbd_list);
	svcpt->scp_nrqbds_total--;
	cfs_spin_unlock(&svcpt->scp_lock);

	OBD_FREE_LARGE(rqbd->rqbd_buffer, svcpt->scp_service->srv_buf_size);
	OBD_FREE_PTR(rqbd);
}

int
ptlrpc_grow_req_bufs(struct ptlrpc_service_part *svcpt, int post)
{
	struct ptlrpc_service		  *svc = svcpt->scp_service;
        struct ptlrpc_request_buffer_desc *rqbd;
        int                                rc = 0;
        int                                i;

        for (i = 0; i < svc->srv_nbuf_per_group; i++) {
                /* NB: another thread might be doing this as well, we need to
                 * make sure that it wouldn't over-allocate, see LU-1212. */
		if (svcpt->scp_nrqbds_posted >= svc->srv_nbuf_per_group)
			break;

		rqbd = ptlrpc_alloc_rqbd(svcpt);

                if (rqbd == NULL) {
                        CERROR("%s: Can't allocate request buffer\n",
                               svc->srv_name);
                        rc = -ENOMEM;
                        break;
                }
	}

	CDEBUG(D_RPCTRACE,
	       "%s: allocate %d new %d-byte reqbufs (%d/%d left), rc = %d\n",
	       svc->srv_name, i, svc->srv_buf_size, svcpt->scp_nrqbds_posted,
	       svcpt->scp_nrqbds_total, rc);

	if (post && rc == 0)
		rc = ptlrpc_server_post_idle_rqbds(svcpt);

	return rc;
}

/**
 * Part of Rep-Ack logic.
 * Puts a lock and its mode into reply state assotiated to request reply.
 */
void
ptlrpc_save_lock(struct ptlrpc_request *req,
                 struct lustre_handle *lock, int mode, int no_ack)
{
        struct ptlrpc_reply_state *rs = req->rq_reply_state;
        int                        idx;

        LASSERT(rs != NULL);
        LASSERT(rs->rs_nlocks < RS_MAX_LOCKS);

        if (req->rq_export->exp_disconnected) {
                ldlm_lock_decref(lock, mode);
        } else {
                idx = rs->rs_nlocks++;
                rs->rs_locks[idx] = *lock;
                rs->rs_modes[idx] = mode;
                rs->rs_difficult = 1;
                rs->rs_no_ack = !!no_ack;
        }
}
EXPORT_SYMBOL(ptlrpc_save_lock);

#ifdef __KERNEL__

struct ptlrpc_hr_partition;

struct ptlrpc_hr_thread {
	int				hrt_id;		/* thread ID */
	cfs_spinlock_t			hrt_lock;
	cfs_waitq_t			hrt_waitq;
	cfs_list_t			hrt_queue;	/* RS queue */
	struct ptlrpc_hr_partition	*hrt_partition;
};

struct ptlrpc_hr_partition {
	/* # of started threads */
	cfs_atomic_t			hrp_nstarted;
	/* # of stopped threads */
	cfs_atomic_t			hrp_nstopped;
	/* cpu partition id */
	int				hrp_cpt;
	/* round-robin rotor for choosing thread */
	int				hrp_rotor;
	/* total number of threads on this partition */
	int				hrp_nthrs;
	/* threads table */
	struct ptlrpc_hr_thread		*hrp_thrs;
};

#define HRT_RUNNING 0
#define HRT_STOPPING 1

struct ptlrpc_hr_service {
	/* CPU partition table, it's just cfs_cpt_table for now */
	struct cfs_cpt_table		*hr_cpt_table;
	/** controller sleep waitq */
	cfs_waitq_t			hr_waitq;
        unsigned int			hr_stopping;
	/** roundrobin rotor for non-affinity service */
	unsigned int			hr_rotor;
	/* partition data */
	struct ptlrpc_hr_partition	**hr_partitions;
};

struct rs_batch {
	cfs_list_t			rsb_replies;
	unsigned int			rsb_n_replies;
	struct ptlrpc_service_part	*rsb_svcpt;
};

/** reply handling service. */
static struct ptlrpc_hr_service		ptlrpc_hr;

/**
 * maximum mumber of replies scheduled in one batch
 */
#define MAX_SCHEDULED 256

/**
 * Initialize a reply batch.
 *
 * \param b batch
 */
static void rs_batch_init(struct rs_batch *b)
{
        memset(b, 0, sizeof *b);
        CFS_INIT_LIST_HEAD(&b->rsb_replies);
}

/**
 * Choose an hr thread to dispatch requests to.
 */
static struct ptlrpc_hr_thread *
ptlrpc_hr_select(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_hr_partition	*hrp;
	unsigned int			rotor;

	if (svcpt->scp_cpt >= 0 &&
	    svcpt->scp_service->srv_cptable == ptlrpc_hr.hr_cpt_table) {
		/* directly match partition */
		hrp = ptlrpc_hr.hr_partitions[svcpt->scp_cpt];

	} else {
		rotor = ptlrpc_hr.hr_rotor++;
		rotor %= cfs_cpt_number(ptlrpc_hr.hr_cpt_table);

		hrp = ptlrpc_hr.hr_partitions[rotor];
	}

	rotor = hrp->hrp_rotor++;
	return &hrp->hrp_thrs[rotor % hrp->hrp_nthrs];
}

/**
 * Dispatch all replies accumulated in the batch to one from
 * dedicated reply handling threads.
 *
 * \param b batch
 */
static void rs_batch_dispatch(struct rs_batch *b)
{
	if (b->rsb_n_replies != 0) {
		struct ptlrpc_hr_thread	*hrt;

		hrt = ptlrpc_hr_select(b->rsb_svcpt);

		cfs_spin_lock(&hrt->hrt_lock);
		cfs_list_splice_init(&b->rsb_replies, &hrt->hrt_queue);
		cfs_spin_unlock(&hrt->hrt_lock);

		cfs_waitq_signal(&hrt->hrt_waitq);
		b->rsb_n_replies = 0;
	}
}

/**
 * Add a reply to a batch.
 * Add one reply object to a batch, schedule batched replies if overload.
 *
 * \param b batch
 * \param rs reply
 */
static void rs_batch_add(struct rs_batch *b, struct ptlrpc_reply_state *rs)
{
	struct ptlrpc_service_part *svcpt = rs->rs_svcpt;

	if (svcpt != b->rsb_svcpt || b->rsb_n_replies >= MAX_SCHEDULED) {
		if (b->rsb_svcpt != NULL) {
			rs_batch_dispatch(b);
			cfs_spin_unlock(&b->rsb_svcpt->scp_rep_lock);
		}
		cfs_spin_lock(&svcpt->scp_rep_lock);
		b->rsb_svcpt = svcpt;
        }
        cfs_spin_lock(&rs->rs_lock);
        rs->rs_scheduled_ever = 1;
        if (rs->rs_scheduled == 0) {
                cfs_list_move(&rs->rs_list, &b->rsb_replies);
                rs->rs_scheduled = 1;
                b->rsb_n_replies++;
        }
        rs->rs_committed = 1;
        cfs_spin_unlock(&rs->rs_lock);
}

/**
 * Reply batch finalization.
 * Dispatch remaining replies from the batch
 * and release remaining spinlock.
 *
 * \param b batch
 */
static void rs_batch_fini(struct rs_batch *b)
{
	if (b->rsb_svcpt != NULL) {
		rs_batch_dispatch(b);
		cfs_spin_unlock(&b->rsb_svcpt->scp_rep_lock);
	}
}

#define DECLARE_RS_BATCH(b)     struct rs_batch b

#else /* __KERNEL__ */

#define rs_batch_init(b)        do{}while(0)
#define rs_batch_fini(b)        do{}while(0)
#define rs_batch_add(b, r)      ptlrpc_schedule_difficult_reply(r)
#define DECLARE_RS_BATCH(b)

#endif /* __KERNEL__ */

/**
 * Put reply state into a queue for processing because we received
 * ACK from the client
 */
void ptlrpc_dispatch_difficult_reply(struct ptlrpc_reply_state *rs)
{
#ifdef __KERNEL__
	struct ptlrpc_hr_thread *hrt;
	ENTRY;

	LASSERT(cfs_list_empty(&rs->rs_list));

	hrt = ptlrpc_hr_select(rs->rs_svcpt);

	cfs_spin_lock(&hrt->hrt_lock);
	cfs_list_add_tail(&rs->rs_list, &hrt->hrt_queue);
	cfs_spin_unlock(&hrt->hrt_lock);

	cfs_waitq_signal(&hrt->hrt_waitq);
	EXIT;
#else
	cfs_list_add_tail(&rs->rs_list, &rs->rs_svcpt->scp_rep_queue);
#endif
}

void
ptlrpc_schedule_difficult_reply(struct ptlrpc_reply_state *rs)
{
	ENTRY;

	LASSERT_SPIN_LOCKED(&rs->rs_svcpt->scp_rep_lock);
        LASSERT_SPIN_LOCKED(&rs->rs_lock);
        LASSERT (rs->rs_difficult);
        rs->rs_scheduled_ever = 1;  /* flag any notification attempt */

        if (rs->rs_scheduled) {     /* being set up or already notified */
                EXIT;
                return;
        }

        rs->rs_scheduled = 1;
        cfs_list_del_init(&rs->rs_list);
        ptlrpc_dispatch_difficult_reply(rs);
        EXIT;
}
EXPORT_SYMBOL(ptlrpc_schedule_difficult_reply);

void ptlrpc_commit_replies(struct obd_export *exp)
{
        struct ptlrpc_reply_state *rs, *nxt;
        DECLARE_RS_BATCH(batch);
        ENTRY;

        rs_batch_init(&batch);
        /* Find any replies that have been committed and get their service
         * to attend to complete them. */

        /* CAVEAT EMPTOR: spinlock ordering!!! */
        cfs_spin_lock(&exp->exp_uncommitted_replies_lock);
        cfs_list_for_each_entry_safe(rs, nxt, &exp->exp_uncommitted_replies,
                                     rs_obd_list) {
                LASSERT (rs->rs_difficult);
                /* VBR: per-export last_committed */
                LASSERT(rs->rs_export);
                if (rs->rs_transno <= exp->exp_last_committed) {
                        cfs_list_del_init(&rs->rs_obd_list);
                        rs_batch_add(&batch, rs);
                }
        }
        cfs_spin_unlock(&exp->exp_uncommitted_replies_lock);
        rs_batch_fini(&batch);
        EXIT;
}
EXPORT_SYMBOL(ptlrpc_commit_replies);

static int
ptlrpc_server_post_idle_rqbds(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_request_buffer_desc *rqbd;
	int				  rc;
	int				  posted = 0;

	for (;;) {
		cfs_spin_lock(&svcpt->scp_lock);

		if (cfs_list_empty(&svcpt->scp_rqbd_idle)) {
			cfs_spin_unlock(&svcpt->scp_lock);
			return posted;
		}

		rqbd = cfs_list_entry(svcpt->scp_rqbd_idle.next,
				      struct ptlrpc_request_buffer_desc,
				      rqbd_list);
		cfs_list_del(&rqbd->rqbd_list);

		/* assume we will post successfully */
		svcpt->scp_nrqbds_posted++;
		cfs_list_add(&rqbd->rqbd_list, &svcpt->scp_rqbd_posted);

		cfs_spin_unlock(&svcpt->scp_lock);

		rc = ptlrpc_register_rqbd(rqbd);
		if (rc != 0)
			break;

		posted = 1;
	}

	cfs_spin_lock(&svcpt->scp_lock);

	svcpt->scp_nrqbds_posted--;
	cfs_list_del(&rqbd->rqbd_list);
	cfs_list_add_tail(&rqbd->rqbd_list, &svcpt->scp_rqbd_idle);

	/* Don't complain if no request buffers are posted right now; LNET
	 * won't drop requests because we set the portal lazy! */

	cfs_spin_unlock(&svcpt->scp_lock);

	return -1;
}

static void ptlrpc_at_timer(unsigned long castmeharder)
{
	struct ptlrpc_service_part *svcpt;

	svcpt = (struct ptlrpc_service_part *)castmeharder;

	svcpt->scp_at_check = 1;
	svcpt->scp_at_checktime = cfs_time_current();
	cfs_waitq_signal(&svcpt->scp_waitq);
}

static void
ptlrpc_server_nthreads_check(struct ptlrpc_service *svc,
			     struct ptlrpc_service_conf *conf)
{
#ifdef __KERNEL__
	struct ptlrpc_service_thr_conf	*tc = &conf->psc_thr;
	unsigned			init;
	unsigned			total;
	unsigned			nthrs;
	int				weight;

	/*
	 * Common code for estimating & validating threads number.
	 * CPT affinity service could have percpt thread-pool instead
	 * of a global thread-pool, which means user might not always
	 * get the threads number they give it in conf::tc_nthrs_user
	 * even they did set. It's because we need to validate threads
	 * number for each CPT to guarantee each pool will have enough
	 * threads to keep the service healthy.
	 */
	init = PTLRPC_NTHRS_INIT + (svc->srv_ops.so_hpreq_handler != NULL);
	init = max_t(int, init, tc->tc_nthrs_init);

	/* NB: please see comments in lustre_lnet.h for definition
	 * details of these members */
	LASSERT(tc->tc_nthrs_max != 0);

	if (tc->tc_nthrs_user != 0) {
		/* In case there is a reason to test a service with many
		 * threads, we give a less strict check here, it can
		 * be up to 8 * nthrs_max */
		total = min(tc->tc_nthrs_max * 8, tc->tc_nthrs_user);
		nthrs = total / svc->srv_ncpts;
		init  = max(init, nthrs);
		goto out;
	}

	total = tc->tc_nthrs_max;
	if (tc->tc_nthrs_base == 0) {
		/* don't care about base threads number per partition,
		 * this is most for non-affinity service */
		nthrs = total / svc->srv_ncpts;
		goto out;
	}

	nthrs = tc->tc_nthrs_base;
	if (svc->srv_ncpts == 1) {
		int	i;

		/* NB: Increase the base number if it's single partition
		 * and total number of cores/HTs is larger or equal to 4.
		 * result will always < 2 * nthrs_base */
		weight = cfs_cpt_weight(svc->srv_cptable, CFS_CPT_ANY);
		for (i = 1; (weight >> (i + 1)) != 0 && /* >= 4 cores/HTs */
			    (tc->tc_nthrs_base >> i) != 0; i++)
			nthrs += tc->tc_nthrs_base >> i;
	}

	if (tc->tc_thr_factor != 0) {
		int	  factor = tc->tc_thr_factor;
		const int fade = 4;

		/*
		 * User wants to increase number of threads with for
		 * each CPU core/HT, most likely the factor is larger then
		 * one thread/core because service threads are supposed to
		 * be blocked by lock or wait for IO.
		 */
		/*
		 * Amdahl's law says that adding processors wouldn't give
		 * a linear increasing of parallelism, so it's nonsense to
		 * have too many threads no matter how many cores/HTs
		 * there are.
		 */
		if (cfs_cpu_ht_nsiblings(0) > 1) { /* weight is # of HTs */
			/* depress thread factor for hyper-thread */
			factor = factor - (factor >> 1) + (factor >> 3);
		}

		weight = cfs_cpt_weight(svc->srv_cptable, 0);
		LASSERT(weight > 0);

		for (; factor > 0 && weight > 0; factor--, weight -= fade)
			nthrs += min(weight, fade) * factor;
	}

	if (nthrs * svc->srv_ncpts > tc->tc_nthrs_max) {
		nthrs = max(tc->tc_nthrs_base,
			    tc->tc_nthrs_max / svc->srv_ncpts);
	}
 out:
	nthrs = max(nthrs, tc->tc_nthrs_init);
	svc->srv_nthrs_cpt_limit = nthrs;
	svc->srv_nthrs_cpt_init = init;

	if (nthrs * svc->srv_ncpts > tc->tc_nthrs_max) {
		LCONSOLE_WARN("%s: This service may have more threads (%d) "
			      "than the given soft limit (%d)\n",
			      svc->srv_name, nthrs * svc->srv_ncpts,
			      tc->tc_nthrs_max);
	}
#endif
}

/**
 * Initialize percpt data for a service
 */
static int
ptlrpc_service_part_init(struct ptlrpc_service *svc,
			 struct ptlrpc_service_part *svcpt, int cpt)
{
	struct ptlrpc_at_array	*array;
	int			size;
	int			index;
	int			rc;

	svcpt->scp_cpt = cpt;
	CFS_INIT_LIST_HEAD(&svcpt->scp_threads);

	/* rqbd and incoming request queue */
	cfs_spin_lock_init(&svcpt->scp_lock);
	CFS_INIT_LIST_HEAD(&svcpt->scp_rqbd_idle);
	CFS_INIT_LIST_HEAD(&svcpt->scp_rqbd_posted);
	CFS_INIT_LIST_HEAD(&svcpt->scp_req_incoming);
	cfs_waitq_init(&svcpt->scp_waitq);
	/* history request & rqbd list */
	CFS_INIT_LIST_HEAD(&svcpt->scp_hist_reqs);
	CFS_INIT_LIST_HEAD(&svcpt->scp_hist_rqbds);

	/* acitve requests and hp requests */
	cfs_spin_lock_init(&svcpt->scp_req_lock);
	CFS_INIT_LIST_HEAD(&svcpt->scp_req_pending);
	CFS_INIT_LIST_HEAD(&svcpt->scp_hreq_pending);

	/* reply states */
	cfs_spin_lock_init(&svcpt->scp_rep_lock);
	CFS_INIT_LIST_HEAD(&svcpt->scp_rep_active);
#ifndef __KERNEL__
	CFS_INIT_LIST_HEAD(&svcpt->scp_rep_queue);
#endif
	CFS_INIT_LIST_HEAD(&svcpt->scp_rep_idle);
	cfs_waitq_init(&svcpt->scp_rep_waitq);
	cfs_atomic_set(&svcpt->scp_nreps_difficult, 0);

	/* adaptive timeout */
	cfs_spin_lock_init(&svcpt->scp_at_lock);
	array = &svcpt->scp_at_array;

	size = at_est2timeout(at_max);
	array->paa_size     = size;
	array->paa_count    = 0;
	array->paa_deadline = -1;

	/* allocate memory for scp_at_array (ptlrpc_at_array) */
	OBD_CPT_ALLOC(array->paa_reqs_array,
		      svc->srv_cptable, cpt, sizeof(cfs_list_t) * size);
	if (array->paa_reqs_array == NULL)
		return -ENOMEM;

	for (index = 0; index < size; index++)
		CFS_INIT_LIST_HEAD(&array->paa_reqs_array[index]);

	OBD_CPT_ALLOC(array->paa_reqs_count,
		      svc->srv_cptable, cpt, sizeof(__u32) * size);
	if (array->paa_reqs_count == NULL)
		goto failed;

	cfs_timer_init(&svcpt->scp_at_timer, ptlrpc_at_timer, svcpt);
	/* At SOW, service time should be quick; 10s seems generous. If client
	 * timeout is less than this, we'll be sending an early reply. */
	at_init(&svcpt->scp_at_estimate, 10, 0);

	/* assign this before call ptlrpc_grow_req_bufs */
	svcpt->scp_service = svc;
	/* Now allocate the request buffers, but don't post them now */
	rc = ptlrpc_grow_req_bufs(svcpt, 0);
	/* We shouldn't be under memory pressure at startup, so
	 * fail if we can't allocate all our buffers at this time. */
	if (rc != 0)
		goto failed;

	return 0;

 failed:
	if (array->paa_reqs_count != NULL) {
		OBD_FREE(array->paa_reqs_count, sizeof(__u32) * size);
		array->paa_reqs_count = NULL;
	}

	if (array->paa_reqs_array != NULL) {
		OBD_FREE(array->paa_reqs_array,
			 sizeof(cfs_list_t) * array->paa_size);
		array->paa_reqs_array = NULL;
	}

	return -ENOMEM;
}

/**
 * Initialize service on a given portal.
 * This includes starting serving threads , allocating and posting rqbds and
 * so on.
 */
struct ptlrpc_service *
ptlrpc_register_service(struct ptlrpc_service_conf *conf,
			cfs_proc_dir_entry_t *proc_entry)
{
	struct ptlrpc_service_cpt_conf	*cconf = &conf->psc_cpt;
	struct ptlrpc_service		*service;
	struct ptlrpc_service_part	*svcpt;
	struct cfs_cpt_table		*cptable;
	__u32				*cpts = NULL;
	int				ncpts;
	int				cpt;
	int				rc;
	int				i;
	ENTRY;

	LASSERT(conf->psc_buf.bc_nbufs > 0);
	LASSERT(conf->psc_buf.bc_buf_size >=
		conf->psc_buf.bc_req_max_size + SPTLRPC_MAX_PAYLOAD);
	LASSERT(conf->psc_thr.tc_ctx_tags != 0);

	cptable = cconf->cc_cptable;
	if (cptable == NULL)
		cptable = cfs_cpt_table;

	if (!conf->psc_thr.tc_cpu_affinity) {
		ncpts = 1;
	} else {
		ncpts = cfs_cpt_number(cptable);
		if (cconf->cc_pattern != NULL) {
			struct cfs_expr_list	*el;

			rc = cfs_expr_list_parse(cconf->cc_pattern,
						 strlen(cconf->cc_pattern),
						 0, ncpts - 1, &el);
			if (rc != 0) {
				CERROR("%s: invalid CPT pattern string: %s",
				       conf->psc_name, cconf->cc_pattern);
				RETURN(ERR_PTR(-EINVAL));
			}

			rc = cfs_expr_list_values(el, ncpts, &cpts);
			cfs_expr_list_free(el);
			if (rc <= 0) {
				CERROR("%s: failed to parse CPT array %s: %d\n",
				       conf->psc_name, cconf->cc_pattern, rc);
				if (cpts != NULL)
					OBD_FREE(cpts, sizeof(*cpts) * ncpts);
				RETURN(ERR_PTR(rc < 0 ? rc : -EINVAL));
			}
			ncpts = rc;
		}
	}

	OBD_ALLOC(service, offsetof(struct ptlrpc_service, srv_parts[ncpts]));
	if (service == NULL) {
		if (cpts != NULL)
			OBD_FREE(cpts, sizeof(*cpts) * ncpts);
		RETURN(ERR_PTR(-ENOMEM));
	}

	service->srv_cptable		= cptable;
	service->srv_cpts		= cpts;
	service->srv_ncpts		= ncpts;

	service->srv_cpt_bits = 0; /* it's zero already, easy to read... */
	while ((1 << service->srv_cpt_bits) < cfs_cpt_number(cptable))
		service->srv_cpt_bits++;

	/* public members */
	cfs_spin_lock_init(&service->srv_lock);
	service->srv_name		= conf->psc_name;
	service->srv_watchdog_factor	= conf->psc_watchdog_factor;
	CFS_INIT_LIST_HEAD(&service->srv_list); /* for safty of cleanup */

	/* buffer configuration */
	service->srv_nbuf_per_group	= test_req_buffer_pressure ?  1 :
					  max(conf->psc_buf.bc_nbufs /
					      service->srv_ncpts, 1U);
	service->srv_max_req_size	= conf->psc_buf.bc_req_max_size +
					  SPTLRPC_MAX_PAYLOAD;
	service->srv_buf_size		= conf->psc_buf.bc_buf_size;
	service->srv_rep_portal		= conf->psc_buf.bc_rep_portal;
	service->srv_req_portal		= conf->psc_buf.bc_req_portal;

	/* Increase max reply size to next power of two */
	service->srv_max_reply_size = 1;
	while (service->srv_max_reply_size <
	       conf->psc_buf.bc_rep_max_size + SPTLRPC_MAX_PAYLOAD)
		service->srv_max_reply_size <<= 1;

	service->srv_thread_name	= conf->psc_thr.tc_thr_name;
	service->srv_ctx_tags		= conf->psc_thr.tc_ctx_tags;
	service->srv_hpreq_ratio	= PTLRPC_SVC_HP_RATIO;
	service->srv_ops		= conf->psc_ops;

	for (i = 0; i < ncpts; i++) {
		if (!conf->psc_thr.tc_cpu_affinity)
			cpt = CFS_CPT_ANY;
		else
			cpt = cpts != NULL ? cpts[i] : i;

		OBD_CPT_ALLOC(svcpt, cptable, cpt, sizeof(*svcpt));
		if (svcpt == NULL)
			GOTO(failed, rc = -ENOMEM);

		service->srv_parts[i] = svcpt;
		rc = ptlrpc_service_part_init(service, svcpt, cpt);
		if (rc != 0)
			GOTO(failed, rc);
	}

	ptlrpc_server_nthreads_check(service, conf);

	rc = LNetSetLazyPortal(service->srv_req_portal);
	LASSERT(rc == 0);

        cfs_spin_lock (&ptlrpc_all_services_lock);
        cfs_list_add (&service->srv_list, &ptlrpc_all_services);
        cfs_spin_unlock (&ptlrpc_all_services_lock);

        if (proc_entry != NULL)
                ptlrpc_lprocfs_register_service(proc_entry, service);

        CDEBUG(D_NET, "%s: Started, listening on portal %d\n",
               service->srv_name, service->srv_req_portal);

#ifdef __KERNEL__
	rc = ptlrpc_start_threads(service);
	if (rc != 0) {
		CERROR("Failed to start threads for service %s: %d\n",
		       service->srv_name, rc);
		GOTO(failed, rc);
	}
#endif

	RETURN(service);
failed:
	ptlrpc_unregister_service(service);
	RETURN(ERR_PTR(rc));
}
EXPORT_SYMBOL(ptlrpc_register_service);

/**
 * to actually free the request, must be called without holding svc_lock.
 * note it's caller's responsibility to unlink req->rq_list.
 */
static void ptlrpc_server_free_request(struct ptlrpc_request *req)
{
        LASSERT(cfs_atomic_read(&req->rq_refcount) == 0);
        LASSERT(cfs_list_empty(&req->rq_timed_list));

         /* DEBUG_REQ() assumes the reply state of a request with a valid
          * ref will not be destroyed until that reference is dropped. */
        ptlrpc_req_drop_rs(req);

        sptlrpc_svc_ctx_decref(req);

        if (req != &req->rq_rqbd->rqbd_req) {
                /* NB request buffers use an embedded
                 * req if the incoming req unlinked the
                 * MD; this isn't one of them! */
                OBD_FREE(req, sizeof(*req));
        }
}

/**
 * drop a reference count of the request. if it reaches 0, we either
 * put it into history list, or free it immediately.
 */
void ptlrpc_server_drop_request(struct ptlrpc_request *req)
{
	struct ptlrpc_request_buffer_desc *rqbd = req->rq_rqbd;
	struct ptlrpc_service_part	  *svcpt = rqbd->rqbd_svcpt;
	struct ptlrpc_service		  *svc = svcpt->scp_service;
        int                                refcount;
        cfs_list_t                        *tmp;
        cfs_list_t                        *nxt;

        if (!cfs_atomic_dec_and_test(&req->rq_refcount))
                return;

	if (req->rq_at_linked) {
		cfs_spin_lock(&svcpt->scp_at_lock);
		/* recheck with lock, in case it's unlinked by
		 * ptlrpc_at_check_timed() */
		if (likely(req->rq_at_linked))
			ptlrpc_at_remove_timed(req);
		cfs_spin_unlock(&svcpt->scp_at_lock);
	}

	LASSERT(cfs_list_empty(&req->rq_timed_list));

        /* finalize request */
        if (req->rq_export) {
                class_export_put(req->rq_export);
                req->rq_export = NULL;
        }

	cfs_spin_lock(&svcpt->scp_lock);

        cfs_list_add(&req->rq_list, &rqbd->rqbd_reqs);

        refcount = --(rqbd->rqbd_refcount);
        if (refcount == 0) {
                /* request buffer is now idle: add to history */
                cfs_list_del(&rqbd->rqbd_list);

		cfs_list_add_tail(&rqbd->rqbd_list, &svcpt->scp_hist_rqbds);
		svcpt->scp_hist_nrqbds++;

		/* cull some history?
		 * I expect only about 1 or 2 rqbds need to be recycled here */
		while (svcpt->scp_hist_nrqbds > svc->srv_hist_nrqbds_cpt_max) {
			rqbd = cfs_list_entry(svcpt->scp_hist_rqbds.next,
					      struct ptlrpc_request_buffer_desc,
					      rqbd_list);

			cfs_list_del(&rqbd->rqbd_list);
			svcpt->scp_hist_nrqbds--;

                        /* remove rqbd's reqs from svc's req history while
                         * I've got the service lock */
                        cfs_list_for_each(tmp, &rqbd->rqbd_reqs) {
                                req = cfs_list_entry(tmp, struct ptlrpc_request,
                                                     rq_list);
                                /* Track the highest culled req seq */
				if (req->rq_history_seq >
				    svcpt->scp_hist_seq_culled) {
					svcpt->scp_hist_seq_culled =
						req->rq_history_seq;
				}
				cfs_list_del(&req->rq_history_list);
			}

			cfs_spin_unlock(&svcpt->scp_lock);

                        cfs_list_for_each_safe(tmp, nxt, &rqbd->rqbd_reqs) {
                                req = cfs_list_entry(rqbd->rqbd_reqs.next,
                                                     struct ptlrpc_request,
                                                     rq_list);
                                cfs_list_del(&req->rq_list);
                                ptlrpc_server_free_request(req);
                        }

			cfs_spin_lock(&svcpt->scp_lock);
			/*
			 * now all reqs including the embedded req has been
			 * disposed, schedule request buffer for re-use.
			 */
			LASSERT(cfs_atomic_read(&rqbd->rqbd_req.rq_refcount) ==
				0);
			cfs_list_add_tail(&rqbd->rqbd_list,
					  &svcpt->scp_rqbd_idle);
		}

		cfs_spin_unlock(&svcpt->scp_lock);
	} else if (req->rq_reply_state && req->rq_reply_state->rs_prealloc) {
		/* If we are low on memory, we are not interested in history */
		cfs_list_del(&req->rq_list);
		cfs_list_del_init(&req->rq_history_list);

		/* Track the highest culled req seq */
		if (req->rq_history_seq > svcpt->scp_hist_seq_culled)
			svcpt->scp_hist_seq_culled = req->rq_history_seq;

		cfs_spin_unlock(&svcpt->scp_lock);

		ptlrpc_server_free_request(req);
	} else {
		cfs_spin_unlock(&svcpt->scp_lock);
	}
}

/**
 * to finish a request: stop sending more early replies, and release
 * the request. should be called after we finished handling the request.
 */
static void ptlrpc_server_finish_request(struct ptlrpc_service_part *svcpt,
					 struct ptlrpc_request *req)
{
	ptlrpc_server_hpreq_fini(req);

	cfs_spin_lock(&svcpt->scp_req_lock);
	svcpt->scp_nreqs_active--;
	if (req->rq_hp)
		svcpt->scp_nhreqs_active--;
	cfs_spin_unlock(&svcpt->scp_req_lock);

	ptlrpc_server_drop_request(req);
}

/**
 * This function makes sure dead exports are evicted in a timely manner.
 * This function is only called when some export receives a message (i.e.,
 * the network is up.)
 */
static void ptlrpc_update_export_timer(struct obd_export *exp, long extra_delay)
{
        struct obd_export *oldest_exp;
        time_t oldest_time, new_time;

        ENTRY;

        LASSERT(exp);

        /* Compensate for slow machines, etc, by faking our request time
           into the future.  Although this can break the strict time-ordering
           of the list, we can be really lazy here - we don't have to evict
           at the exact right moment.  Eventually, all silent exports
           will make it to the top of the list. */

        /* Do not pay attention on 1sec or smaller renewals. */
        new_time = cfs_time_current_sec() + extra_delay;
        if (exp->exp_last_request_time + 1 /*second */ >= new_time)
                RETURN_EXIT;

        exp->exp_last_request_time = new_time;
        CDEBUG(D_HA, "updating export %s at "CFS_TIME_T" exp %p\n",
               exp->exp_client_uuid.uuid,
               exp->exp_last_request_time, exp);

        /* exports may get disconnected from the chain even though the
           export has references, so we must keep the spin lock while
           manipulating the lists */
        cfs_spin_lock(&exp->exp_obd->obd_dev_lock);

        if (cfs_list_empty(&exp->exp_obd_chain_timed)) {
                /* this one is not timed */
                cfs_spin_unlock(&exp->exp_obd->obd_dev_lock);
                RETURN_EXIT;
        }

        cfs_list_move_tail(&exp->exp_obd_chain_timed,
                           &exp->exp_obd->obd_exports_timed);

        oldest_exp = cfs_list_entry(exp->exp_obd->obd_exports_timed.next,
                                    struct obd_export, exp_obd_chain_timed);
        oldest_time = oldest_exp->exp_last_request_time;
        cfs_spin_unlock(&exp->exp_obd->obd_dev_lock);

        if (exp->exp_obd->obd_recovering) {
                /* be nice to everyone during recovery */
                EXIT;
                return;
        }

        /* Note - racing to start/reset the obd_eviction timer is safe */
        if (exp->exp_obd->obd_eviction_timer == 0) {
                /* Check if the oldest entry is expired. */
                if (cfs_time_current_sec() > (oldest_time + PING_EVICT_TIMEOUT +
                                              extra_delay)) {
                        /* We need a second timer, in case the net was down and
                         * it just came back. Since the pinger may skip every
                         * other PING_INTERVAL (see note in ptlrpc_pinger_main),
                         * we better wait for 3. */
                        exp->exp_obd->obd_eviction_timer =
                                cfs_time_current_sec() + 3 * PING_INTERVAL;
                        CDEBUG(D_HA, "%s: Think about evicting %s from "CFS_TIME_T"\n",
                               exp->exp_obd->obd_name, 
                               obd_export_nid2str(oldest_exp), oldest_time);
                }
        } else {
                if (cfs_time_current_sec() >
                    (exp->exp_obd->obd_eviction_timer + extra_delay)) {
                        /* The evictor won't evict anyone who we've heard from
                         * recently, so we don't have to check before we start
                         * it. */
                        if (!ping_evictor_wake(exp))
                                exp->exp_obd->obd_eviction_timer = 0;
                }
        }

        EXIT;
}

/**
 * Sanity check request \a req.
 * Return 0 if all is ok, error code otherwise.
 */
static int ptlrpc_check_req(struct ptlrpc_request *req)
{
        int rc = 0;

        if (unlikely(lustre_msg_get_conn_cnt(req->rq_reqmsg) <
                     req->rq_export->exp_conn_cnt)) {
                DEBUG_REQ(D_ERROR, req,
                          "DROPPING req from old connection %d < %d",
                          lustre_msg_get_conn_cnt(req->rq_reqmsg),
                          req->rq_export->exp_conn_cnt);
                return -EEXIST;
        }
        if (unlikely(req->rq_export->exp_obd &&
                     req->rq_export->exp_obd->obd_fail)) {
             /* Failing over, don't handle any more reqs, send
                error response instead. */
                CDEBUG(D_RPCTRACE, "Dropping req %p for failed obd %s\n",
                       req, req->rq_export->exp_obd->obd_name);
                rc = -ENODEV;
        } else if (lustre_msg_get_flags(req->rq_reqmsg) &
                   (MSG_REPLAY | MSG_REQ_REPLAY_DONE) &&
                   !(req->rq_export->exp_obd->obd_recovering)) {
                        DEBUG_REQ(D_ERROR, req,
                                  "Invalid replay without recovery");
                        class_fail_export(req->rq_export);
                        rc = -ENODEV;
        } else if (lustre_msg_get_transno(req->rq_reqmsg) != 0 &&
                   !(req->rq_export->exp_obd->obd_recovering)) {
                        DEBUG_REQ(D_ERROR, req, "Invalid req with transno "
                                  LPU64" without recovery",
                                  lustre_msg_get_transno(req->rq_reqmsg));
                        class_fail_export(req->rq_export);
                        rc = -ENODEV;
        }

        if (unlikely(rc < 0)) {
                req->rq_status = rc;
                ptlrpc_error(req);
        }
        return rc;
}

static void ptlrpc_at_set_timer(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_at_array *array = &svcpt->scp_at_array;
	__s32 next;

	if (array->paa_count == 0) {
		cfs_timer_disarm(&svcpt->scp_at_timer);
		return;
	}

	/* Set timer for closest deadline */
	next = (__s32)(array->paa_deadline - cfs_time_current_sec() -
		       at_early_margin);
	if (next <= 0) {
		ptlrpc_at_timer((unsigned long)svcpt);
	} else {
		cfs_timer_arm(&svcpt->scp_at_timer, cfs_time_shift(next));
		CDEBUG(D_INFO, "armed %s at %+ds\n",
		       svcpt->scp_service->srv_name, next);
	}
}

/* Add rpc to early reply check list */
static int ptlrpc_at_add_timed(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part *svcpt = req->rq_rqbd->rqbd_svcpt;
	struct ptlrpc_at_array *array = &svcpt->scp_at_array;
        struct ptlrpc_request *rq = NULL;
        __u32 index;

        if (AT_OFF)
                return(0);

        if (req->rq_no_reply)
                return 0;

        if ((lustre_msghdr_get_flags(req->rq_reqmsg) & MSGHDR_AT_SUPPORT) == 0)
                return(-ENOSYS);

	cfs_spin_lock(&svcpt->scp_at_lock);
        LASSERT(cfs_list_empty(&req->rq_timed_list));

        index = (unsigned long)req->rq_deadline % array->paa_size;
        if (array->paa_reqs_count[index] > 0) {
                /* latest rpcs will have the latest deadlines in the list,
                 * so search backward. */
                cfs_list_for_each_entry_reverse(rq,
                                                &array->paa_reqs_array[index],
                                                rq_timed_list) {
                        if (req->rq_deadline >= rq->rq_deadline) {
                                cfs_list_add(&req->rq_timed_list,
                                             &rq->rq_timed_list);
                                break;
                        }
                }
        }

        /* Add the request at the head of the list */
        if (cfs_list_empty(&req->rq_timed_list))
                cfs_list_add(&req->rq_timed_list,
                             &array->paa_reqs_array[index]);

        cfs_spin_lock(&req->rq_lock);
        req->rq_at_linked = 1;
        cfs_spin_unlock(&req->rq_lock);
        req->rq_at_index = index;
        array->paa_reqs_count[index]++;
        array->paa_count++;
        if (array->paa_count == 1 || array->paa_deadline > req->rq_deadline) {
                array->paa_deadline = req->rq_deadline;
		ptlrpc_at_set_timer(svcpt);
	}
	cfs_spin_unlock(&svcpt->scp_at_lock);

	return 0;
}

static void
ptlrpc_at_remove_timed(struct ptlrpc_request *req)
{
	struct ptlrpc_at_array *array;

	array = &req->rq_rqbd->rqbd_svcpt->scp_at_array;

	/* NB: must call with hold svcpt::scp_at_lock */
	LASSERT(!cfs_list_empty(&req->rq_timed_list));
	cfs_list_del_init(&req->rq_timed_list);

	cfs_spin_lock(&req->rq_lock);
	req->rq_at_linked = 0;
	cfs_spin_unlock(&req->rq_lock);

	array->paa_reqs_count[req->rq_at_index]--;
	array->paa_count--;
}

static int ptlrpc_at_send_early_reply(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part *svcpt = req->rq_rqbd->rqbd_svcpt;
        struct ptlrpc_request *reqcopy;
        struct lustre_msg *reqmsg;
        cfs_duration_t olddl = req->rq_deadline - cfs_time_current_sec();
        time_t newdl;
        int rc;
        ENTRY;

        /* deadline is when the client expects us to reply, margin is the
           difference between clients' and servers' expectations */
        DEBUG_REQ(D_ADAPTTO, req,
                  "%ssending early reply (deadline %+lds, margin %+lds) for "
                  "%d+%d", AT_OFF ? "AT off - not " : "",
		  olddl, olddl - at_get(&svcpt->scp_at_estimate),
		  at_get(&svcpt->scp_at_estimate), at_extra);

        if (AT_OFF)
                RETURN(0);

        if (olddl < 0) {
                DEBUG_REQ(D_WARNING, req, "Already past deadline (%+lds), "
                          "not sending early reply. Consider increasing "
                          "at_early_margin (%d)?", olddl, at_early_margin);

                /* Return an error so we're not re-added to the timed list. */
                RETURN(-ETIMEDOUT);
        }

        if ((lustre_msghdr_get_flags(req->rq_reqmsg) & MSGHDR_AT_SUPPORT) == 0){
                DEBUG_REQ(D_INFO, req, "Wanted to ask client for more time, "
                          "but no AT support");
                RETURN(-ENOSYS);
        }

        if (req->rq_export &&
            lustre_msg_get_flags(req->rq_reqmsg) &
            (MSG_REPLAY | MSG_REQ_REPLAY_DONE | MSG_LOCK_REPLAY_DONE)) {
                /* During recovery, we don't want to send too many early
                 * replies, but on the other hand we want to make sure the
                 * client has enough time to resend if the rpc is lost. So
                 * during the recovery period send at least 4 early replies,
                 * spacing them every at_extra if we can. at_estimate should
                 * always equal this fixed value during recovery. */
		at_measured(&svcpt->scp_at_estimate, min(at_extra,
			    req->rq_export->exp_obd->obd_recovery_timeout / 4));
	} else {
		/* Fake our processing time into the future to ask the clients
		 * for some extra amount of time */
		at_measured(&svcpt->scp_at_estimate, at_extra +
			    cfs_time_current_sec() -
			    req->rq_arrival_time.tv_sec);

		/* Check to see if we've actually increased the deadline -
		 * we may be past adaptive_max */
		if (req->rq_deadline >= req->rq_arrival_time.tv_sec +
		    at_get(&svcpt->scp_at_estimate)) {
			DEBUG_REQ(D_WARNING, req, "Couldn't add any time "
				  "(%ld/%ld), not sending early reply\n",
				  olddl, req->rq_arrival_time.tv_sec +
				  at_get(&svcpt->scp_at_estimate) -
				  cfs_time_current_sec());
			RETURN(-ETIMEDOUT);
		}
	}
	newdl = cfs_time_current_sec() + at_get(&svcpt->scp_at_estimate);

        OBD_ALLOC(reqcopy, sizeof *reqcopy);
        if (reqcopy == NULL)
                RETURN(-ENOMEM);
        OBD_ALLOC_LARGE(reqmsg, req->rq_reqlen);
        if (!reqmsg) {
                OBD_FREE(reqcopy, sizeof *reqcopy);
                RETURN(-ENOMEM);
        }

        *reqcopy = *req;
        reqcopy->rq_reply_state = NULL;
        reqcopy->rq_rep_swab_mask = 0;
        reqcopy->rq_pack_bulk = 0;
        reqcopy->rq_pack_udesc = 0;
        reqcopy->rq_packed_final = 0;
        sptlrpc_svc_ctx_addref(reqcopy);
        /* We only need the reqmsg for the magic */
        reqcopy->rq_reqmsg = reqmsg;
        memcpy(reqmsg, req->rq_reqmsg, req->rq_reqlen);

        LASSERT(cfs_atomic_read(&req->rq_refcount));
        /** if it is last refcount then early reply isn't needed */
        if (cfs_atomic_read(&req->rq_refcount) == 1) {
                DEBUG_REQ(D_ADAPTTO, reqcopy, "Normal reply already sent out, "
                          "abort sending early reply\n");
                GOTO(out, rc = -EINVAL);
        }

        /* Connection ref */
        reqcopy->rq_export = class_conn2export(
                                     lustre_msg_get_handle(reqcopy->rq_reqmsg));
        if (reqcopy->rq_export == NULL)
                GOTO(out, rc = -ENODEV);

        /* RPC ref */
        class_export_rpc_get(reqcopy->rq_export);
        if (reqcopy->rq_export->exp_obd &&
            reqcopy->rq_export->exp_obd->obd_fail)
                GOTO(out_put, rc = -ENODEV);

        rc = lustre_pack_reply_flags(reqcopy, 1, NULL, NULL, LPRFL_EARLY_REPLY);
        if (rc)
                GOTO(out_put, rc);

        rc = ptlrpc_send_reply(reqcopy, PTLRPC_REPLY_EARLY);

        if (!rc) {
                /* Adjust our own deadline to what we told the client */
                req->rq_deadline = newdl;
                req->rq_early_count++; /* number sent, server side */
        } else {
                DEBUG_REQ(D_ERROR, req, "Early reply send failed %d", rc);
        }

        /* Free the (early) reply state from lustre_pack_reply.
           (ptlrpc_send_reply takes it's own rs ref, so this is safe here) */
        ptlrpc_req_drop_rs(reqcopy);

out_put:
        class_export_rpc_put(reqcopy->rq_export);
        class_export_put(reqcopy->rq_export);
out:
        sptlrpc_svc_ctx_decref(reqcopy);
        OBD_FREE_LARGE(reqmsg, req->rq_reqlen);
        OBD_FREE(reqcopy, sizeof *reqcopy);
        RETURN(rc);
}

/* Send early replies to everybody expiring within at_early_margin
   asking for at_extra time */
static int ptlrpc_at_check_timed(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_at_array *array = &svcpt->scp_at_array;
        struct ptlrpc_request *rq, *n;
        cfs_list_t work_list;
        __u32  index, count;
        time_t deadline;
        time_t now = cfs_time_current_sec();
        cfs_duration_t delay;
        int first, counter = 0;
        ENTRY;

	cfs_spin_lock(&svcpt->scp_at_lock);
	if (svcpt->scp_at_check == 0) {
		cfs_spin_unlock(&svcpt->scp_at_lock);
		RETURN(0);
	}
	delay = cfs_time_sub(cfs_time_current(), svcpt->scp_at_checktime);
	svcpt->scp_at_check = 0;

	if (array->paa_count == 0) {
		cfs_spin_unlock(&svcpt->scp_at_lock);
		RETURN(0);
	}

	/* The timer went off, but maybe the nearest rpc already completed. */
	first = array->paa_deadline - now;
	if (first > at_early_margin) {
		/* We've still got plenty of time.  Reset the timer. */
		ptlrpc_at_set_timer(svcpt);
		cfs_spin_unlock(&svcpt->scp_at_lock);
		RETURN(0);
	}

        /* We're close to a timeout, and we don't know how much longer the
           server will take. Send early replies to everyone expiring soon. */
        CFS_INIT_LIST_HEAD(&work_list);
        deadline = -1;
        index = (unsigned long)array->paa_deadline % array->paa_size;
        count = array->paa_count;
        while (count > 0) {
                count -= array->paa_reqs_count[index];
                cfs_list_for_each_entry_safe(rq, n,
                                             &array->paa_reqs_array[index],
                                             rq_timed_list) {
			if (rq->rq_deadline > now + at_early_margin) {
				/* update the earliest deadline */
				if (deadline == -1 ||
				    rq->rq_deadline < deadline)
					deadline = rq->rq_deadline;
				break;
			}

			ptlrpc_at_remove_timed(rq);
			/**
			 * ptlrpc_server_drop_request() may drop
			 * refcount to 0 already. Let's check this and
			 * don't add entry to work_list
			 */
			if (likely(cfs_atomic_inc_not_zero(&rq->rq_refcount)))
				cfs_list_add(&rq->rq_timed_list, &work_list);
			counter++;
                }

                if (++index >= array->paa_size)
                        index = 0;
        }
        array->paa_deadline = deadline;
	/* we have a new earliest deadline, restart the timer */
	ptlrpc_at_set_timer(svcpt);

	cfs_spin_unlock(&svcpt->scp_at_lock);

        CDEBUG(D_ADAPTTO, "timeout in %+ds, asking for %d secs on %d early "
               "replies\n", first, at_extra, counter);
        if (first < 0) {
                /* We're already past request deadlines before we even get a
                   chance to send early replies */
                LCONSOLE_WARN("%s: This server is not able to keep up with "
			      "request traffic (cpu-bound).\n",
			      svcpt->scp_service->srv_name);
		CWARN("earlyQ=%d reqQ=%d recA=%d, svcEst=%d, "
		      "delay="CFS_DURATION_T"(jiff)\n",
		      counter, svcpt->scp_nreqs_incoming,
		      svcpt->scp_nreqs_active,
		      at_get(&svcpt->scp_at_estimate), delay);
        }

        /* we took additional refcount so entries can't be deleted from list, no
         * locking is needed */
        while (!cfs_list_empty(&work_list)) {
                rq = cfs_list_entry(work_list.next, struct ptlrpc_request,
                                    rq_timed_list);
                cfs_list_del_init(&rq->rq_timed_list);

                if (ptlrpc_at_send_early_reply(rq) == 0)
                        ptlrpc_at_add_timed(rq);

                ptlrpc_server_drop_request(rq);
        }

	RETURN(1); /* return "did_something" for liblustre */
}

/**
 * Put the request to the export list if the request may become
 * a high priority one.
 */
static int ptlrpc_server_hpreq_init(struct ptlrpc_service *svc,
				    struct ptlrpc_request *req)
{
        int rc = 0;
        ENTRY;

	if (svc->srv_ops.so_hpreq_handler) {
		rc = svc->srv_ops.so_hpreq_handler(req);
                if (rc)
                        RETURN(rc);
        }
        if (req->rq_export && req->rq_ops) {
                /* Perform request specific check. We should do this check
                 * before the request is added into exp_hp_rpcs list otherwise
                 * it may hit swab race at LU-1044. */
                if (req->rq_ops->hpreq_check)
                        rc = req->rq_ops->hpreq_check(req);

                cfs_spin_lock_bh(&req->rq_export->exp_rpc_lock);
                cfs_list_add(&req->rq_exp_list,
                             &req->rq_export->exp_hp_rpcs);
                cfs_spin_unlock_bh(&req->rq_export->exp_rpc_lock);
        }

        RETURN(rc);
}

/** Remove the request from the export list. */
static void ptlrpc_server_hpreq_fini(struct ptlrpc_request *req)
{
        ENTRY;
        if (req->rq_export && req->rq_ops) {
                /* refresh lock timeout again so that client has more
                 * room to send lock cancel RPC. */
                if (req->rq_ops->hpreq_fini)
                        req->rq_ops->hpreq_fini(req);

                cfs_spin_lock_bh(&req->rq_export->exp_rpc_lock);
                cfs_list_del_init(&req->rq_exp_list);
                cfs_spin_unlock_bh(&req->rq_export->exp_rpc_lock);
        }
        EXIT;
}

static int ptlrpc_hpreq_check(struct ptlrpc_request *req)
{
	return 1;
}

static struct ptlrpc_hpreq_ops ptlrpc_hpreq_common = {
	.hpreq_check       = ptlrpc_hpreq_check,
};

/* Hi-Priority RPC check by RPC operation code. */
int ptlrpc_hpreq_handler(struct ptlrpc_request *req)
{
	int opc = lustre_msg_get_opc(req->rq_reqmsg);

	/* Check for export to let only reconnects for not yet evicted
	 * export to become a HP rpc. */
	if ((req->rq_export != NULL) &&
	    (opc == OBD_PING || opc == MDS_CONNECT || opc == OST_CONNECT))
		req->rq_ops = &ptlrpc_hpreq_common;

	return 0;
}
EXPORT_SYMBOL(ptlrpc_hpreq_handler);

/**
 * Make the request a high priority one.
 *
 * All the high priority requests are queued in a separate FIFO
 * ptlrpc_service_part::scp_hpreq_pending list which is parallel to
 * ptlrpc_service_part::scp_req_pending list but has a higher priority
 * for handling.
 *
 * \see ptlrpc_server_handle_request().
 */
static void ptlrpc_hpreq_reorder_nolock(struct ptlrpc_service_part *svcpt,
                                        struct ptlrpc_request *req)
{
        ENTRY;

        cfs_spin_lock(&req->rq_lock);
        if (req->rq_hp == 0) {
                int opc = lustre_msg_get_opc(req->rq_reqmsg);

                /* Add to the high priority queue. */
		cfs_list_move_tail(&req->rq_list, &svcpt->scp_hreq_pending);
                req->rq_hp = 1;
                if (opc != OBD_PING)
                        DEBUG_REQ(D_RPCTRACE, req, "high priority req");
        }
        cfs_spin_unlock(&req->rq_lock);
        EXIT;
}

/**
 * \see ptlrpc_hpreq_reorder_nolock
 */
void ptlrpc_hpreq_reorder(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part *svcpt = req->rq_rqbd->rqbd_svcpt;
	ENTRY;

	cfs_spin_lock(&svcpt->scp_req_lock);
	/* It may happen that the request is already taken for the processing
	 * but still in the export list, or the request is not in the request
	 * queue but in the export list already, do not add it into the
	 * HP list. */
	if (!cfs_list_empty(&req->rq_list))
		ptlrpc_hpreq_reorder_nolock(svcpt, req);
	cfs_spin_unlock(&svcpt->scp_req_lock);
	EXIT;
}
EXPORT_SYMBOL(ptlrpc_hpreq_reorder);

/**
 * Add a request to the regular or HP queue; optionally perform HP request
 * initialization.
 */
static int ptlrpc_server_request_add(struct ptlrpc_service_part *svcpt,
				     struct ptlrpc_request *req)
{
	int	rc;
	ENTRY;

	rc = ptlrpc_server_hpreq_init(svcpt->scp_service, req);
	if (rc < 0)
		RETURN(rc);

	cfs_spin_lock(&svcpt->scp_req_lock);

	if (rc)
		ptlrpc_hpreq_reorder_nolock(svcpt, req);
	else
		cfs_list_add_tail(&req->rq_list, &svcpt->scp_req_pending);

	cfs_spin_unlock(&svcpt->scp_req_lock);

	RETURN(0);
}

/**
 * Allow to handle high priority request
 * User can call it w/o any lock but need to hold
 * ptlrpc_service_part::scp_req_lock to get reliable result
 */
static int ptlrpc_server_allow_high(struct ptlrpc_service_part *svcpt,
				    int force)
{
	if (force)
		return 1;

	if (svcpt->scp_nreqs_active >= svcpt->scp_nthrs_running - 1)
		return 0;

	if (svcpt->scp_nhreqs_active == 0)
		return 1;

	return cfs_list_empty(&svcpt->scp_req_pending) ||
	       svcpt->scp_hreq_count < svcpt->scp_service->srv_hpreq_ratio;
}

static int ptlrpc_server_high_pending(struct ptlrpc_service_part *svcpt,
				      int force)
{
	return ptlrpc_server_allow_high(svcpt, force) &&
	       !cfs_list_empty(&svcpt->scp_hreq_pending);
}

/**
 * Only allow normal priority requests on a service that has a high-priority
 * queue if forced (i.e. cleanup), if there are other high priority requests
 * already being processed (i.e. those threads can service more high-priority
 * requests), or if there are enough idle threads that a later thread can do
 * a high priority request.
 * User can call it w/o any lock but need to hold
 * ptlrpc_service_part::scp_req_lock to get reliable result
 */
static int ptlrpc_server_allow_normal(struct ptlrpc_service_part *svcpt,
				      int force)
{
#ifndef __KERNEL__
	if (1) /* always allow to handle normal request for liblustre */
		return 1;
#endif
	if (force ||
	    svcpt->scp_nreqs_active < svcpt->scp_nthrs_running - 2)
		return 1;

	if (svcpt->scp_nreqs_active >= svcpt->scp_nthrs_running - 1)
		return 0;

	return svcpt->scp_nhreqs_active > 0 ||
	       svcpt->scp_service->srv_ops.so_hpreq_handler == NULL;
}

static int ptlrpc_server_normal_pending(struct ptlrpc_service_part *svcpt,
					int force)
{
	return ptlrpc_server_allow_normal(svcpt, force) &&
	       !cfs_list_empty(&svcpt->scp_req_pending);
}

/**
 * Returns true if there are requests available in incoming
 * request queue for processing and it is allowed to fetch them.
 * User can call it w/o any lock but need to hold ptlrpc_service::scp_req_lock
 * to get reliable result
 * \see ptlrpc_server_allow_normal
 * \see ptlrpc_server_allow high
 */
static inline int
ptlrpc_server_request_pending(struct ptlrpc_service_part *svcpt, int force)
{
	return ptlrpc_server_high_pending(svcpt, force) ||
	       ptlrpc_server_normal_pending(svcpt, force);
}

/**
 * Fetch a request for processing from queue of unprocessed requests.
 * Favors high-priority requests.
 * Returns a pointer to fetched request.
 */
static struct ptlrpc_request *
ptlrpc_server_request_get(struct ptlrpc_service_part *svcpt, int force)
{
	struct ptlrpc_request *req;
	ENTRY;

	if (ptlrpc_server_high_pending(svcpt, force)) {
		req = cfs_list_entry(svcpt->scp_hreq_pending.next,
				     struct ptlrpc_request, rq_list);
		svcpt->scp_hreq_count++;
		RETURN(req);
	}

	if (ptlrpc_server_normal_pending(svcpt, force)) {
		req = cfs_list_entry(svcpt->scp_req_pending.next,
				     struct ptlrpc_request, rq_list);
		svcpt->scp_hreq_count = 0;
		RETURN(req);
	}
	RETURN(NULL);
}

/**
 * Handle freshly incoming reqs, add to timed early reply list,
 * pass on to regular request queue.
 * All incoming requests pass through here before getting into
 * ptlrpc_server_handle_req later on.
 */
static int
ptlrpc_server_handle_req_in(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_service	*svc = svcpt->scp_service;
	struct ptlrpc_request	*req;
	__u32			deadline;
	int			rc;
	ENTRY;

	cfs_spin_lock(&svcpt->scp_lock);
	if (cfs_list_empty(&svcpt->scp_req_incoming)) {
		cfs_spin_unlock(&svcpt->scp_lock);
		RETURN(0);
	}

	req = cfs_list_entry(svcpt->scp_req_incoming.next,
			     struct ptlrpc_request, rq_list);
	cfs_list_del_init(&req->rq_list);
	svcpt->scp_nreqs_incoming--;
	/* Consider this still a "queued" request as far as stats are
	 * concerned */
	cfs_spin_unlock(&svcpt->scp_lock);

        /* go through security check/transform */
        rc = sptlrpc_svc_unwrap_request(req);
        switch (rc) {
        case SECSVC_OK:
                break;
        case SECSVC_COMPLETE:
                target_send_reply(req, 0, OBD_FAIL_MDS_ALL_REPLY_NET);
                goto err_req;
        case SECSVC_DROP:
                goto err_req;
        default:
                LBUG();
        }

        /*
         * for null-flavored rpc, msg has been unpacked by sptlrpc, although
         * redo it wouldn't be harmful.
         */
        if (SPTLRPC_FLVR_POLICY(req->rq_flvr.sf_rpc) != SPTLRPC_POLICY_NULL) {
                rc = ptlrpc_unpack_req_msg(req, req->rq_reqlen);
                if (rc != 0) {
                        CERROR("error unpacking request: ptl %d from %s "
                               "x"LPU64"\n", svc->srv_req_portal,
                               libcfs_id2str(req->rq_peer), req->rq_xid);
                        goto err_req;
                }
        }

        rc = lustre_unpack_req_ptlrpc_body(req, MSG_PTLRPC_BODY_OFF);
        if (rc) {
                CERROR ("error unpacking ptlrpc body: ptl %d from %s x"
                        LPU64"\n", svc->srv_req_portal,
                        libcfs_id2str(req->rq_peer), req->rq_xid);
                goto err_req;
        }

        if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_DROP_REQ_OPC) &&
            lustre_msg_get_opc(req->rq_reqmsg) == cfs_fail_val) {
                CERROR("drop incoming rpc opc %u, x"LPU64"\n",
                       cfs_fail_val, req->rq_xid);
                goto err_req;
        }

        rc = -EINVAL;
        if (lustre_msg_get_type(req->rq_reqmsg) != PTL_RPC_MSG_REQUEST) {
                CERROR("wrong packet type received (type=%u) from %s\n",
                       lustre_msg_get_type(req->rq_reqmsg),
                       libcfs_id2str(req->rq_peer));
                goto err_req;
        }

        switch(lustre_msg_get_opc(req->rq_reqmsg)) {
        case MDS_WRITEPAGE:
        case OST_WRITE:
                req->rq_bulk_write = 1;
                break;
        case MDS_READPAGE:
        case OST_READ:
        case MGS_CONFIG_READ:
                req->rq_bulk_read = 1;
                break;
        }

        CDEBUG(D_RPCTRACE, "got req x"LPU64"\n", req->rq_xid);

        req->rq_export = class_conn2export(
                lustre_msg_get_handle(req->rq_reqmsg));
        if (req->rq_export) {
		class_export_rpc_get(req->rq_export);
                rc = ptlrpc_check_req(req);
                if (rc == 0) {
                        rc = sptlrpc_target_export_check(req->rq_export, req);
                        if (rc)
                                DEBUG_REQ(D_ERROR, req, "DROPPING req with "
                                          "illegal security flavor,");
                }

                if (rc)
                        goto err_req;
                ptlrpc_update_export_timer(req->rq_export, 0);
        }

        /* req_in handling should/must be fast */
        if (cfs_time_current_sec() - req->rq_arrival_time.tv_sec > 5)
                DEBUG_REQ(D_WARNING, req, "Slow req_in handling "CFS_DURATION_T"s",
                          cfs_time_sub(cfs_time_current_sec(),
                                       req->rq_arrival_time.tv_sec));

        /* Set rpc server deadline and add it to the timed list */
        deadline = (lustre_msghdr_get_flags(req->rq_reqmsg) &
                    MSGHDR_AT_SUPPORT) ?
                   /* The max time the client expects us to take */
                   lustre_msg_get_timeout(req->rq_reqmsg) : obd_timeout;
        req->rq_deadline = req->rq_arrival_time.tv_sec + deadline;
        if (unlikely(deadline == 0)) {
                DEBUG_REQ(D_ERROR, req, "Dropping request with 0 timeout");
                goto err_req;
        }

        ptlrpc_at_add_timed(req);

        /* Move it over to the request processing queue */
	rc = ptlrpc_server_request_add(svcpt, req);
	if (rc) {
		ptlrpc_server_hpreq_fini(req);
		GOTO(err_req, rc);
	}
	cfs_waitq_signal(&svcpt->scp_waitq);
	RETURN(1);

err_req:
	if (req->rq_export)
		class_export_rpc_put(req->rq_export);
	cfs_spin_lock(&svcpt->scp_req_lock);
	svcpt->scp_nreqs_active++;
	cfs_spin_unlock(&svcpt->scp_req_lock);
	ptlrpc_server_finish_request(svcpt, req);

	RETURN(1);
}

/**
 * Main incoming request handling logic.
 * Calls handler function from service to do actual processing.
 */
static int
ptlrpc_server_handle_request(struct ptlrpc_service_part *svcpt,
			     struct ptlrpc_thread *thread)
{
	struct ptlrpc_service *svc = svcpt->scp_service;
        struct obd_export     *export = NULL;
        struct ptlrpc_request *request;
        struct timeval         work_start;
        struct timeval         work_end;
        long                   timediff;
        int                    rc;
        int                    fail_opc = 0;
        ENTRY;

	cfs_spin_lock(&svcpt->scp_req_lock);
#ifndef __KERNEL__
	/* !@%$# liblustre only has 1 thread */
	if (cfs_atomic_read(&svcpt->scp_nreps_difficult) != 0) {
		cfs_spin_unlock(&svcpt->scp_req_lock);
		RETURN(0);
	}
#endif
	request = ptlrpc_server_request_get(svcpt, 0);
	if  (request == NULL) {
		cfs_spin_unlock(&svcpt->scp_req_lock);
                RETURN(0);
        }

        if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_NOTIMEOUT))
                fail_opc = OBD_FAIL_PTLRPC_HPREQ_NOTIMEOUT;
        else if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_TIMEOUT))
                fail_opc = OBD_FAIL_PTLRPC_HPREQ_TIMEOUT;

        if (unlikely(fail_opc)) {
                if (request->rq_export && request->rq_ops) {
			cfs_spin_unlock(&svcpt->scp_req_lock);

			OBD_FAIL_TIMEOUT(fail_opc, 4);

			cfs_spin_lock(&svcpt->scp_req_lock);
			request = ptlrpc_server_request_get(svcpt, 0);
			if  (request == NULL) {
				cfs_spin_unlock(&svcpt->scp_req_lock);
				RETURN(0);
			}
		}
	}

	cfs_list_del_init(&request->rq_list);
	svcpt->scp_nreqs_active++;
	if (request->rq_hp)
		svcpt->scp_nhreqs_active++;

	cfs_spin_unlock(&svcpt->scp_req_lock);

        ptlrpc_rqphase_move(request, RQ_PHASE_INTERPRET);

        if(OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_DUMP_LOG))
                libcfs_debug_dumplog();

        cfs_gettimeofday(&work_start);
        timediff = cfs_timeval_sub(&work_start, &request->rq_arrival_time,NULL);
        if (likely(svc->srv_stats != NULL)) {
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQWAIT_CNTR,
                                    timediff);
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQQDEPTH_CNTR,
				    svcpt->scp_nreqs_incoming);
		lprocfs_counter_add(svc->srv_stats, PTLRPC_REQACTIVE_CNTR,
				    svcpt->scp_nreqs_active);
		lprocfs_counter_add(svc->srv_stats, PTLRPC_TIMEOUT,
				    at_get(&svcpt->scp_at_estimate));
        }

	export = request->rq_export;
	rc = lu_context_init(&request->rq_session, LCT_SESSION | LCT_NOREF);
        if (rc) {
                CERROR("Failure to initialize session: %d\n", rc);
                goto out_req;
        }
        request->rq_session.lc_thread = thread;
        request->rq_session.lc_cookie = 0x5;
        lu_context_enter(&request->rq_session);

        CDEBUG(D_NET, "got req "LPU64"\n", request->rq_xid);

        request->rq_svc_thread = thread;
        if (thread)
                request->rq_svc_thread->t_env->le_ses = &request->rq_session;

        if (likely(request->rq_export)) {
		if (unlikely(ptlrpc_check_req(request)))
			goto put_conn;
                ptlrpc_update_export_timer(request->rq_export, timediff >> 19);
        }

        /* Discard requests queued for longer than the deadline.
           The deadline is increased if we send an early reply. */
        if (cfs_time_current_sec() > request->rq_deadline) {
                DEBUG_REQ(D_ERROR, request, "Dropping timed-out request from %s"
                          ": deadline "CFS_DURATION_T":"CFS_DURATION_T"s ago\n",
                          libcfs_id2str(request->rq_peer),
                          cfs_time_sub(request->rq_deadline,
                          request->rq_arrival_time.tv_sec),
                          cfs_time_sub(cfs_time_current_sec(),
                          request->rq_deadline));
                goto put_conn;
        }

        CDEBUG(D_RPCTRACE, "Handling RPC pname:cluuid+ref:pid:xid:nid:opc "
               "%s:%s+%d:%d:x"LPU64":%s:%d\n", cfs_curproc_comm(),
               (request->rq_export ?
                (char *)request->rq_export->exp_client_uuid.uuid : "0"),
               (request->rq_export ?
                cfs_atomic_read(&request->rq_export->exp_refcount) : -99),
               lustre_msg_get_status(request->rq_reqmsg), request->rq_xid,
               libcfs_id2str(request->rq_peer),
               lustre_msg_get_opc(request->rq_reqmsg));

        if (lustre_msg_get_opc(request->rq_reqmsg) != OBD_PING)
                CFS_FAIL_TIMEOUT_MS(OBD_FAIL_PTLRPC_PAUSE_REQ, cfs_fail_val);

	rc = svc->srv_ops.so_req_handler(request);

        ptlrpc_rqphase_move(request, RQ_PHASE_COMPLETE);

put_conn:
        lu_context_exit(&request->rq_session);
        lu_context_fini(&request->rq_session);

        if (unlikely(cfs_time_current_sec() > request->rq_deadline)) {
                DEBUG_REQ(D_WARNING, request, "Request x"LPU64" took longer "
                          "than estimated ("CFS_DURATION_T":"CFS_DURATION_T"s);"
                          " client may timeout.",
                          request->rq_xid, cfs_time_sub(request->rq_deadline,
                          request->rq_arrival_time.tv_sec),
                          cfs_time_sub(cfs_time_current_sec(),
                          request->rq_deadline));
        }

        cfs_gettimeofday(&work_end);
        timediff = cfs_timeval_sub(&work_end, &work_start, NULL);
        CDEBUG(D_RPCTRACE, "Handled RPC pname:cluuid+ref:pid:xid:nid:opc "
               "%s:%s+%d:%d:x"LPU64":%s:%d Request procesed in "
               "%ldus (%ldus total) trans "LPU64" rc %d/%d\n",
                cfs_curproc_comm(),
                (request->rq_export ?
                 (char *)request->rq_export->exp_client_uuid.uuid : "0"),
                (request->rq_export ?
                 cfs_atomic_read(&request->rq_export->exp_refcount) : -99),
                lustre_msg_get_status(request->rq_reqmsg),
                request->rq_xid,
                libcfs_id2str(request->rq_peer),
                lustre_msg_get_opc(request->rq_reqmsg),
                timediff,
                cfs_timeval_sub(&work_end, &request->rq_arrival_time, NULL),
                (request->rq_repmsg ?
                 lustre_msg_get_transno(request->rq_repmsg) :
                 request->rq_transno),
                request->rq_status,
                (request->rq_repmsg ?
                 lustre_msg_get_status(request->rq_repmsg) : -999));
        if (likely(svc->srv_stats != NULL && request->rq_reqmsg != NULL)) {
                __u32 op = lustre_msg_get_opc(request->rq_reqmsg);
                int opc = opcode_offset(op);
                if (opc > 0 && !(op == LDLM_ENQUEUE || op == MDS_REINT)) {
                        LASSERT(opc < LUSTRE_MAX_OPCODES);
                        lprocfs_counter_add(svc->srv_stats,
                                            opc + EXTRA_MAX_OPCODES,
                                            timediff);
                }
        }
        if (unlikely(request->rq_early_count)) {
                DEBUG_REQ(D_ADAPTTO, request,
                          "sent %d early replies before finishing in "
                          CFS_DURATION_T"s",
                          request->rq_early_count,
                          cfs_time_sub(work_end.tv_sec,
                          request->rq_arrival_time.tv_sec));
        }

out_req:
	if (export != NULL)
		class_export_rpc_put(export);
	ptlrpc_server_finish_request(svcpt, request);

	RETURN(1);
}

/**
 * An internal function to process a single reply state object.
 */
static int
ptlrpc_handle_rs(struct ptlrpc_reply_state *rs)
{
	struct ptlrpc_service_part *svcpt = rs->rs_svcpt;
	struct ptlrpc_service     *svc = svcpt->scp_service;
        struct obd_export         *exp;
        int                        nlocks;
        int                        been_handled;
        ENTRY;

        exp = rs->rs_export;

        LASSERT (rs->rs_difficult);
        LASSERT (rs->rs_scheduled);
        LASSERT (cfs_list_empty(&rs->rs_list));

        cfs_spin_lock (&exp->exp_lock);
        /* Noop if removed already */
        cfs_list_del_init (&rs->rs_exp_list);
        cfs_spin_unlock (&exp->exp_lock);

        /* The disk commit callback holds exp_uncommitted_replies_lock while it
         * iterates over newly committed replies, removing them from
         * exp_uncommitted_replies.  It then drops this lock and schedules the
         * replies it found for handling here.
         *
         * We can avoid contention for exp_uncommitted_replies_lock between the
         * HRT threads and further commit callbacks by checking rs_committed
         * which is set in the commit callback while it holds both
         * rs_lock and exp_uncommitted_reples.
         *
         * If we see rs_committed clear, the commit callback _may_ not have
         * handled this reply yet and we race with it to grab
         * exp_uncommitted_replies_lock before removing the reply from
         * exp_uncommitted_replies.  Note that if we lose the race and the
         * reply has already been removed, list_del_init() is a noop.
         *
         * If we see rs_committed set, we know the commit callback is handling,
         * or has handled this reply since store reordering might allow us to
         * see rs_committed set out of sequence.  But since this is done
         * holding rs_lock, we can be sure it has all completed once we hold
         * rs_lock, which we do right next.
         */
        if (!rs->rs_committed) {
                cfs_spin_lock(&exp->exp_uncommitted_replies_lock);
                cfs_list_del_init(&rs->rs_obd_list);
                cfs_spin_unlock(&exp->exp_uncommitted_replies_lock);
        }

        cfs_spin_lock(&rs->rs_lock);

        been_handled = rs->rs_handled;
        rs->rs_handled = 1;

        nlocks = rs->rs_nlocks;                 /* atomic "steal", but */
        rs->rs_nlocks = 0;                      /* locks still on rs_locks! */

        if (nlocks == 0 && !been_handled) {
                /* If we see this, we should already have seen the warning
                 * in mds_steal_ack_locks()  */
		CDEBUG(D_HA, "All locks stolen from rs %p x"LPD64".t"LPD64
		       " o%d NID %s\n",
		       rs,
		       rs->rs_xid, rs->rs_transno, rs->rs_opc,
		       libcfs_nid2str(exp->exp_connection->c_peer.nid));
        }

        if ((!been_handled && rs->rs_on_net) || nlocks > 0) {
                cfs_spin_unlock(&rs->rs_lock);

                if (!been_handled && rs->rs_on_net) {
                        LNetMDUnlink(rs->rs_md_h);
                        /* Ignore return code; we're racing with
                         * completion... */
                }

                while (nlocks-- > 0)
                        ldlm_lock_decref(&rs->rs_locks[nlocks],
                                         rs->rs_modes[nlocks]);

                cfs_spin_lock(&rs->rs_lock);
        }

        rs->rs_scheduled = 0;

        if (!rs->rs_on_net) {
                /* Off the net */
                cfs_spin_unlock(&rs->rs_lock);

                class_export_put (exp);
                rs->rs_export = NULL;
                ptlrpc_rs_decref (rs);
		if (cfs_atomic_dec_and_test(&svcpt->scp_nreps_difficult) &&
		    svc->srv_is_stopping)
			cfs_waitq_broadcast(&svcpt->scp_waitq);
		RETURN(1);
	}

	/* still on the net; callback will schedule */
	cfs_spin_unlock(&rs->rs_lock);
	RETURN(1);
}

#ifndef __KERNEL__

/**
 * Check whether given service has a reply available for processing
 * and process it.
 *
 * \param svc a ptlrpc service
 * \retval 0 no replies processed
 * \retval 1 one reply processed
 */
static int
ptlrpc_server_handle_reply(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_reply_state *rs = NULL;
	ENTRY;

	cfs_spin_lock(&svcpt->scp_rep_lock);
	if (!cfs_list_empty(&svcpt->scp_rep_queue)) {
		rs = cfs_list_entry(svcpt->scp_rep_queue.prev,
				    struct ptlrpc_reply_state,
				    rs_list);
		cfs_list_del_init(&rs->rs_list);
	}
	cfs_spin_unlock(&svcpt->scp_rep_lock);
	if (rs != NULL)
		ptlrpc_handle_rs(rs);
	RETURN(rs != NULL);
}

/* FIXME make use of timeout later */
int
liblustre_check_services (void *arg)
{
        int  did_something = 0;
        int  rc;
        cfs_list_t *tmp, *nxt;
        ENTRY;

        /* I'm relying on being single threaded, not to have to lock
         * ptlrpc_all_services etc */
        cfs_list_for_each_safe (tmp, nxt, &ptlrpc_all_services) {
                struct ptlrpc_service *svc =
                        cfs_list_entry (tmp, struct ptlrpc_service, srv_list);
		struct ptlrpc_service_part *svcpt;

		LASSERT(svc->srv_ncpts == 1);
		svcpt = svc->srv_parts[0];

		if (svcpt->scp_nthrs_running != 0)     /* I've recursed */
			continue;

		/* service threads can block for bulk, so this limits us
		 * (arbitrarily) to recursing 1 stack frame per service.
		 * Note that the problem with recursion is that we have to
		 * unwind completely before our caller can resume. */

		svcpt->scp_nthrs_running++;

		do {
			rc = ptlrpc_server_handle_req_in(svcpt);
			rc |= ptlrpc_server_handle_reply(svcpt);
			rc |= ptlrpc_at_check_timed(svcpt);
			rc |= ptlrpc_server_handle_request(svcpt, NULL);
			rc |= (ptlrpc_server_post_idle_rqbds(svcpt) > 0);
			did_something |= rc;
		} while (rc);

		svcpt->scp_nthrs_running--;
	}

	RETURN(did_something);
}
#define ptlrpc_stop_all_threads(s) do {} while (0)

#else /* __KERNEL__ */

static void
ptlrpc_check_rqbd_pool(struct ptlrpc_service_part *svcpt)
{
	int avail = svcpt->scp_nrqbds_posted;
	int low_water = test_req_buffer_pressure ? 0 :
			svcpt->scp_service->srv_nbuf_per_group / 2;

        /* NB I'm not locking; just looking. */

        /* CAVEAT EMPTOR: We might be allocating buffers here because we've
         * allowed the request history to grow out of control.  We could put a
         * sanity check on that here and cull some history if we need the
         * space. */

        if (avail <= low_water)
		ptlrpc_grow_req_bufs(svcpt, 1);

	if (svcpt->scp_service->srv_stats) {
		lprocfs_counter_add(svcpt->scp_service->srv_stats,
				    PTLRPC_REQBUF_AVAIL_CNTR, avail);
	}
}

static int
ptlrpc_retry_rqbds(void *arg)
{
	struct ptlrpc_service_part *svcpt = (struct ptlrpc_service_part *)arg;

	svcpt->scp_rqbd_timeout = 0;
	return -ETIMEDOUT;
}

static inline int
ptlrpc_threads_enough(struct ptlrpc_service_part *svcpt)
{
	return svcpt->scp_nreqs_active <
	       svcpt->scp_nthrs_running - 1 -
	       (svcpt->scp_service->srv_ops.so_hpreq_handler != NULL);
}

/**
 * allowed to create more threads
 * user can call it w/o any lock but need to hold
 * ptlrpc_service_part::scp_lock to get reliable result
 */
static inline int
ptlrpc_threads_increasable(struct ptlrpc_service_part *svcpt)
{
	return svcpt->scp_nthrs_running +
	       svcpt->scp_nthrs_starting <
	       svcpt->scp_service->srv_nthrs_cpt_limit;
}

/**
 * too many requests and allowed to create more threads
 */
static inline int
ptlrpc_threads_need_create(struct ptlrpc_service_part *svcpt)
{
	return !ptlrpc_threads_enough(svcpt) &&
		ptlrpc_threads_increasable(svcpt);
}

static inline int
ptlrpc_thread_stopping(struct ptlrpc_thread *thread)
{
	return thread_is_stopping(thread) ||
	       thread->t_svcpt->scp_service->srv_is_stopping;
}

static inline int
ptlrpc_rqbd_pending(struct ptlrpc_service_part *svcpt)
{
	return !cfs_list_empty(&svcpt->scp_rqbd_idle) &&
	       svcpt->scp_rqbd_timeout == 0;
}

static inline int
ptlrpc_at_check(struct ptlrpc_service_part *svcpt)
{
	return svcpt->scp_at_check;
}

/**
 * requests wait on preprocessing
 * user can call it w/o any lock but need to hold
 * ptlrpc_service_part::scp_lock to get reliable result
 */
static inline int
ptlrpc_server_request_incoming(struct ptlrpc_service_part *svcpt)
{
	return !cfs_list_empty(&svcpt->scp_req_incoming);
}

static __attribute__((__noinline__)) int
ptlrpc_wait_event(struct ptlrpc_service_part *svcpt,
		  struct ptlrpc_thread *thread)
{
	/* Don't exit while there are replies to be handled */
	struct l_wait_info lwi = LWI_TIMEOUT(svcpt->scp_rqbd_timeout,
					     ptlrpc_retry_rqbds, svcpt);

	lc_watchdog_disable(thread->t_watchdog);

	cfs_cond_resched();

	l_wait_event_exclusive_head(svcpt->scp_waitq,
				ptlrpc_thread_stopping(thread) ||
				ptlrpc_server_request_incoming(svcpt) ||
				ptlrpc_server_request_pending(svcpt, 0) ||
				ptlrpc_rqbd_pending(svcpt) ||
				ptlrpc_at_check(svcpt), &lwi);

	if (ptlrpc_thread_stopping(thread))
		return -EINTR;

	lc_watchdog_touch(thread->t_watchdog,
			  ptlrpc_server_get_timeout(svcpt));
	return 0;
}

/**
 * Main thread body for service threads.
 * Waits in a loop waiting for new requests to process to appear.
 * Every time an incoming requests is added to its queue, a waitq
 * is woken up and one of the threads will handle it.
 */
static int ptlrpc_main(void *arg)
{
	struct ptlrpc_thread		*thread = (struct ptlrpc_thread *)arg;
	struct ptlrpc_service_part	*svcpt = thread->t_svcpt;
	struct ptlrpc_service		*svc = svcpt->scp_service;
	struct ptlrpc_reply_state	*rs;
#ifdef WITH_GROUP_INFO
        cfs_group_info_t *ginfo = NULL;
#endif
        struct lu_env *env;
        int counter = 0, rc = 0;
        ENTRY;

        thread->t_pid = cfs_curproc_pid();
        cfs_daemonize_ctxt(thread->t_name);

	/* NB: we will call cfs_cpt_bind() for all threads, because we
	 * might want to run lustre server only on a subset of system CPUs,
	 * in that case ->scp_cpt is CFS_CPT_ANY */
	rc = cfs_cpt_bind(svc->srv_cptable, svcpt->scp_cpt);
	if (rc != 0) {
		CWARN("%s: failed to bind %s on CPT %d\n",
		      svc->srv_name, thread->t_name, svcpt->scp_cpt);
	}

#ifdef WITH_GROUP_INFO
        ginfo = cfs_groups_alloc(0);
        if (!ginfo) {
                rc = -ENOMEM;
                goto out;
        }

        cfs_set_current_groups(ginfo);
        cfs_put_group_info(ginfo);
#endif

	if (svc->srv_ops.so_thr_init != NULL) {
		rc = svc->srv_ops.so_thr_init(thread);
                if (rc)
                        goto out;
        }

        OBD_ALLOC_PTR(env);
        if (env == NULL) {
                rc = -ENOMEM;
                goto out_srv_fini;
        }

        rc = lu_context_init(&env->le_ctx,
                             svc->srv_ctx_tags|LCT_REMEMBER|LCT_NOREF);
        if (rc)
                goto out_srv_fini;

        thread->t_env = env;
        env->le_ctx.lc_thread = thread;
        env->le_ctx.lc_cookie = 0x6;

	while (!cfs_list_empty(&svcpt->scp_rqbd_idle)) {
		rc = ptlrpc_server_post_idle_rqbds(svcpt);
		if (rc >= 0)
			continue;

		CERROR("Failed to post rqbd for %s on CPT %d: %d\n",
			svc->srv_name, svcpt->scp_cpt, rc);
		goto out_srv_fini;
	}

        /* Alloc reply state structure for this one */
        OBD_ALLOC_LARGE(rs, svc->srv_max_reply_size);
        if (!rs) {
                rc = -ENOMEM;
                goto out_srv_fini;
        }

	cfs_spin_lock(&svcpt->scp_lock);

	LASSERT(thread_is_starting(thread));
	thread_clear_flags(thread, SVC_STARTING);

	LASSERT(svcpt->scp_nthrs_starting == 1);
	svcpt->scp_nthrs_starting--;

	/* SVC_STOPPING may already be set here if someone else is trying
	 * to stop the service while this new thread has been dynamically
	 * forked. We still set SVC_RUNNING to let our creator know that
	 * we are now running, however we will exit as soon as possible */
	thread_add_flags(thread, SVC_RUNNING);
	svcpt->scp_nthrs_running++;
	cfs_spin_unlock(&svcpt->scp_lock);

	/* wake up our creator in case he's still waiting. */
	cfs_waitq_signal(&thread->t_ctl_waitq);

	thread->t_watchdog = lc_watchdog_add(ptlrpc_server_get_timeout(svcpt),
					     NULL, NULL);

	cfs_spin_lock(&svcpt->scp_rep_lock);
	cfs_list_add(&rs->rs_list, &svcpt->scp_rep_idle);
	cfs_waitq_signal(&svcpt->scp_rep_waitq);
	cfs_spin_unlock(&svcpt->scp_rep_lock);

	CDEBUG(D_NET, "service thread %d (#%d) started\n", thread->t_id,
	       svcpt->scp_nthrs_running);

	/* XXX maintain a list of all managed devices: insert here */
	while (!ptlrpc_thread_stopping(thread)) {
		if (ptlrpc_wait_event(svcpt, thread))
			break;

		ptlrpc_check_rqbd_pool(svcpt);

		if (ptlrpc_threads_need_create(svcpt)) {
			/* Ignore return code - we tried... */
			ptlrpc_start_thread(svcpt, 0);
                }

		/* Process all incoming reqs before handling any */
		if (ptlrpc_server_request_incoming(svcpt)) {
			ptlrpc_server_handle_req_in(svcpt);
			/* but limit ourselves in case of flood */
			if (counter++ < 100)
				continue;
			counter = 0;
		}

		if (ptlrpc_at_check(svcpt))
			ptlrpc_at_check_timed(svcpt);

		if (ptlrpc_server_request_pending(svcpt, 0)) {
			lu_context_enter(&env->le_ctx);
			ptlrpc_server_handle_request(svcpt, thread);
			lu_context_exit(&env->le_ctx);
                }

		if (ptlrpc_rqbd_pending(svcpt) &&
		    ptlrpc_server_post_idle_rqbds(svcpt) < 0) {
			/* I just failed to repost request buffers.
			 * Wait for a timeout (unless something else
			 * happens) before I try again */
			svcpt->scp_rqbd_timeout = cfs_time_seconds(1) / 10;
			CDEBUG(D_RPCTRACE, "Posted buffers: %d\n",
			       svcpt->scp_nrqbds_posted);
                }
        }

        lc_watchdog_delete(thread->t_watchdog);
        thread->t_watchdog = NULL;

out_srv_fini:
        /*
         * deconstruct service specific state created by ptlrpc_start_thread()
         */
	if (svc->srv_ops.so_thr_done != NULL)
		svc->srv_ops.so_thr_done(thread);

        if (env != NULL) {
                lu_context_fini(&env->le_ctx);
                OBD_FREE_PTR(env);
        }
out:
        CDEBUG(D_RPCTRACE, "service thread [ %p : %u ] %d exiting: rc %d\n",
               thread, thread->t_pid, thread->t_id, rc);

	cfs_spin_lock(&svcpt->scp_lock);
	if (thread_test_and_clear_flags(thread, SVC_STARTING))
		svcpt->scp_nthrs_starting--;

	if (thread_test_and_clear_flags(thread, SVC_RUNNING)) {
		/* must know immediately */
		svcpt->scp_nthrs_running--;
	}

	thread->t_id = rc;
	thread_add_flags(thread, SVC_STOPPED);

	cfs_waitq_signal(&thread->t_ctl_waitq);
	cfs_spin_unlock(&svcpt->scp_lock);

	return rc;
}

static int hrt_dont_sleep(struct ptlrpc_hr_thread *hrt,
			  cfs_list_t *replies)
{
	int result;

	cfs_spin_lock(&hrt->hrt_lock);

	cfs_list_splice_init(&hrt->hrt_queue, replies);
	result = ptlrpc_hr.hr_stopping || !cfs_list_empty(replies);

	cfs_spin_unlock(&hrt->hrt_lock);
	return result;
}

/**
 * Main body of "handle reply" function.
 * It processes acked reply states
 */
static int ptlrpc_hr_main(void *arg)
{
	struct ptlrpc_hr_thread		*hrt = (struct ptlrpc_hr_thread *)arg;
	struct ptlrpc_hr_partition	*hrp = hrt->hrt_partition;
	CFS_LIST_HEAD			(replies);
	char				threadname[20];
	int				rc;

	snprintf(threadname, sizeof(threadname), "ptlrpc_hr%02d_%03d",
		 hrp->hrp_cpt, hrt->hrt_id);
	cfs_daemonize_ctxt(threadname);

	rc = cfs_cpt_bind(ptlrpc_hr.hr_cpt_table, hrp->hrp_cpt);
	if (rc != 0) {
		CWARN("Failed to bind %s on CPT %d of CPT table %p: rc = %d\n",
		      threadname, hrp->hrp_cpt, ptlrpc_hr.hr_cpt_table, rc);
	}

	cfs_atomic_inc(&hrp->hrp_nstarted);
	cfs_waitq_signal(&ptlrpc_hr.hr_waitq);

	while (!ptlrpc_hr.hr_stopping) {
		l_wait_condition(hrt->hrt_waitq, hrt_dont_sleep(hrt, &replies));

                while (!cfs_list_empty(&replies)) {
                        struct ptlrpc_reply_state *rs;

                        rs = cfs_list_entry(replies.prev,
                                            struct ptlrpc_reply_state,
                                            rs_list);
                        cfs_list_del_init(&rs->rs_list);
                        ptlrpc_handle_rs(rs);
                }
        }

	cfs_atomic_inc(&hrp->hrp_nstopped);
	cfs_waitq_signal(&ptlrpc_hr.hr_waitq);

	return 0;
}

static void ptlrpc_stop_hr_threads(void)
{
	struct ptlrpc_hr_partition	*hrp;
	int				i;
	int				j;

	ptlrpc_hr.hr_stopping = 1;

	cfs_percpt_for_each(hrp, i, ptlrpc_hr.hr_partitions) {
		if (hrp->hrp_thrs == NULL)
			continue; /* uninitialized */
		for (j = 0; j < hrp->hrp_nthrs; j++)
			cfs_waitq_broadcast(&hrp->hrp_thrs[j].hrt_waitq);
	}

	cfs_percpt_for_each(hrp, i, ptlrpc_hr.hr_partitions) {
		if (hrp->hrp_thrs == NULL)
			continue; /* uninitialized */
		cfs_wait_event(ptlrpc_hr.hr_waitq,
			       cfs_atomic_read(&hrp->hrp_nstopped) ==
			       cfs_atomic_read(&hrp->hrp_nstarted));
	}
}

static int ptlrpc_start_hr_threads(void)
{
	struct ptlrpc_hr_partition	*hrp;
	int				i;
	int				j;
	ENTRY;

	cfs_percpt_for_each(hrp, i, ptlrpc_hr.hr_partitions) {
		int	rc = 0;

		for (j = 0; j < hrp->hrp_nthrs; j++) {
			rc = cfs_create_thread(ptlrpc_hr_main,
					       &hrp->hrp_thrs[j],
					       CLONE_VM | CLONE_FILES);
			if (rc < 0)
				break;
		}
		cfs_wait_event(ptlrpc_hr.hr_waitq,
			       cfs_atomic_read(&hrp->hrp_nstarted) == j);
		if (rc >= 0)
			continue;

		CERROR("Reply handling thread %d:%d Failed on starting: "
		       "rc = %d\n", i, j, rc);
		ptlrpc_stop_hr_threads();
		RETURN(rc);
	}
	RETURN(0);
}

static void ptlrpc_svcpt_stop_threads(struct ptlrpc_service_part *svcpt)
{
	struct l_wait_info	lwi = { 0 };
	struct ptlrpc_thread	*thread;
	CFS_LIST_HEAD		(zombie);

	ENTRY;

	CDEBUG(D_INFO, "Stopping threads for service %s\n",
	       svcpt->scp_service->srv_name);

	cfs_spin_lock(&svcpt->scp_lock);
	/* let the thread know that we would like it to stop asap */
	list_for_each_entry(thread, &svcpt->scp_threads, t_link) {
		CDEBUG(D_INFO, "Stopping thread %s #%u\n",
		       svcpt->scp_service->srv_thread_name, thread->t_id);
		thread_add_flags(thread, SVC_STOPPING);
	}

	cfs_waitq_broadcast(&svcpt->scp_waitq);

	while (!cfs_list_empty(&svcpt->scp_threads)) {
		thread = cfs_list_entry(svcpt->scp_threads.next,
					struct ptlrpc_thread, t_link);
		if (thread_is_stopped(thread)) {
			cfs_list_del(&thread->t_link);
			cfs_list_add(&thread->t_link, &zombie);
			continue;
		}
		cfs_spin_unlock(&svcpt->scp_lock);

		CDEBUG(D_INFO, "waiting for stopping-thread %s #%u\n",
		       svcpt->scp_service->srv_thread_name, thread->t_id);
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_stopped(thread), &lwi);

		cfs_spin_lock(&svcpt->scp_lock);
	}

	cfs_spin_unlock(&svcpt->scp_lock);

	while (!cfs_list_empty(&zombie)) {
		thread = cfs_list_entry(zombie.next,
					struct ptlrpc_thread, t_link);
		cfs_list_del(&thread->t_link);
		OBD_FREE_PTR(thread);
	}
	EXIT;
}

/**
 * Stops all threads of a particular service \a svc
 */
void ptlrpc_stop_all_threads(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part *svcpt;
	int			   i;
	ENTRY;

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service != NULL)
			ptlrpc_svcpt_stop_threads(svcpt);
	}

	EXIT;
}
EXPORT_SYMBOL(ptlrpc_stop_all_threads);

int ptlrpc_start_threads(struct ptlrpc_service *svc)
{
	int	rc = 0;
	int	i;
	int	j;
	ENTRY;

	/* We require 2 threads min, see note in ptlrpc_server_handle_request */
	LASSERT(svc->srv_nthrs_cpt_init >= PTLRPC_NTHRS_INIT);

	for (i = 0; i < svc->srv_ncpts; i++) {
		for (j = 0; j < svc->srv_nthrs_cpt_init; j++) {
			rc = ptlrpc_start_thread(svc->srv_parts[i], 1);
			if (rc == 0)
				continue;

			if (rc != -EMFILE)
				goto failed;
			/* We have enough threads, don't start more. b=15759 */
			break;
		}
	}

	RETURN(0);
 failed:
	CERROR("cannot start %s thread #%d_%d: rc %d\n",
	       svc->srv_thread_name, i, j, rc);
	ptlrpc_stop_all_threads(svc);
	RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_start_threads);

int ptlrpc_start_thread(struct ptlrpc_service_part *svcpt, int wait)
{
	struct l_wait_info	lwi = { 0 };
	struct ptlrpc_thread	*thread;
	struct ptlrpc_service	*svc = svcpt->scp_service;
	int			rc;
	ENTRY;

	LASSERT(svcpt != NULL);

	CDEBUG(D_RPCTRACE, "%s[%d] started %d min %d max %d\n",
	       svc->srv_name, svcpt->scp_cpt, svcpt->scp_nthrs_running,
	       svc->srv_nthrs_cpt_init, svc->srv_nthrs_cpt_limit);

 again:
	if (unlikely(svc->srv_is_stopping))
		RETURN(-ESRCH);

	if (!ptlrpc_threads_increasable(svcpt) ||
	    (OBD_FAIL_CHECK(OBD_FAIL_TGT_TOOMANY_THREADS) &&
	     svcpt->scp_nthrs_running == svc->srv_nthrs_cpt_init - 1))
		RETURN(-EMFILE);

	OBD_CPT_ALLOC_PTR(thread, svc->srv_cptable, svcpt->scp_cpt);
	if (thread == NULL)
		RETURN(-ENOMEM);
	cfs_waitq_init(&thread->t_ctl_waitq);

	cfs_spin_lock(&svcpt->scp_lock);
	if (!ptlrpc_threads_increasable(svcpt)) {
		cfs_spin_unlock(&svcpt->scp_lock);
		OBD_FREE_PTR(thread);
		RETURN(-EMFILE);
	}

	if (svcpt->scp_nthrs_starting != 0) {
		/* serialize starting because some modules (obdfilter)
		 * might require unique and contiguous t_id */
		LASSERT(svcpt->scp_nthrs_starting == 1);
		cfs_spin_unlock(&svcpt->scp_lock);
		OBD_FREE_PTR(thread);
		if (wait) {
			CDEBUG(D_INFO, "Waiting for creating thread %s #%d\n",
			       svc->srv_thread_name, svcpt->scp_thr_nextid);
			cfs_schedule();
			goto again;
		}

		CDEBUG(D_INFO, "Creating thread %s #%d race, retry later\n",
		       svc->srv_thread_name, svcpt->scp_thr_nextid);
		RETURN(-EAGAIN);
	}

	svcpt->scp_nthrs_starting++;
	thread->t_id = svcpt->scp_thr_nextid++;
	thread_add_flags(thread, SVC_STARTING);
	thread->t_svcpt = svcpt;

	cfs_list_add(&thread->t_link, &svcpt->scp_threads);
	cfs_spin_unlock(&svcpt->scp_lock);

	if (svcpt->scp_cpt >= 0) {
		snprintf(thread->t_name, PTLRPC_THR_NAME_LEN, "%s%02d_%03d",
			 svc->srv_thread_name, svcpt->scp_cpt, thread->t_id);
	} else {
		snprintf(thread->t_name, PTLRPC_THR_NAME_LEN, "%s_%04d",
			 svc->srv_thread_name, thread->t_id);
	}

	CDEBUG(D_RPCTRACE, "starting thread '%s'\n", thread->t_name);
	/*
	 * CLONE_VM and CLONE_FILES just avoid a needless copy, because we
	 * just drop the VM and FILES in cfs_daemonize_ctxt() right away.
	 */
	rc = cfs_create_thread(ptlrpc_main, thread, CFS_DAEMON_FLAGS);
	if (rc < 0) {
		CERROR("cannot start thread '%s': rc %d\n",
		       thread->t_name, rc);
		cfs_spin_lock(&svcpt->scp_lock);
		cfs_list_del(&thread->t_link);
		--svcpt->scp_nthrs_starting;
		cfs_spin_unlock(&svcpt->scp_lock);

                OBD_FREE(thread, sizeof(*thread));
                RETURN(rc);
        }

	if (!wait)
		RETURN(0);

        l_wait_event(thread->t_ctl_waitq,
                     thread_is_running(thread) || thread_is_stopped(thread),
                     &lwi);

        rc = thread_is_stopped(thread) ? thread->t_id : 0;
        RETURN(rc);
}

int ptlrpc_hr_init(void)
{
	struct ptlrpc_hr_partition	*hrp;
	struct ptlrpc_hr_thread		*hrt;
	int				rc;
	int				i;
	int				j;
	ENTRY;

	memset(&ptlrpc_hr, 0, sizeof(ptlrpc_hr));
	ptlrpc_hr.hr_cpt_table = cfs_cpt_table;

	ptlrpc_hr.hr_partitions = cfs_percpt_alloc(ptlrpc_hr.hr_cpt_table,
						   sizeof(*hrp));
	if (ptlrpc_hr.hr_partitions == NULL)
		RETURN(-ENOMEM);

	cfs_waitq_init(&ptlrpc_hr.hr_waitq);

	cfs_percpt_for_each(hrp, i, ptlrpc_hr.hr_partitions) {
		hrp->hrp_cpt = i;

		cfs_atomic_set(&hrp->hrp_nstarted, 0);
		cfs_atomic_set(&hrp->hrp_nstopped, 0);

		hrp->hrp_nthrs = cfs_cpt_weight(ptlrpc_hr.hr_cpt_table, i);
		hrp->hrp_nthrs /= cfs_cpu_ht_nsiblings(0);

		LASSERT(hrp->hrp_nthrs > 0);
		OBD_CPT_ALLOC(hrp->hrp_thrs, ptlrpc_hr.hr_cpt_table, i,
			      hrp->hrp_nthrs * sizeof(*hrt));
		if (hrp->hrp_thrs == NULL)
			GOTO(out, rc = -ENOMEM);

		for (j = 0; j < hrp->hrp_nthrs; j++) {
			hrt = &hrp->hrp_thrs[j];

			hrt->hrt_id = j;
			hrt->hrt_partition = hrp;
			cfs_waitq_init(&hrt->hrt_waitq);
			cfs_spin_lock_init(&hrt->hrt_lock);
			CFS_INIT_LIST_HEAD(&hrt->hrt_queue);
		}
	}

	rc = ptlrpc_start_hr_threads();
out:
	if (rc != 0)
		ptlrpc_hr_fini();
	RETURN(rc);
}

void ptlrpc_hr_fini(void)
{
	struct ptlrpc_hr_partition	*hrp;
	int				i;

	if (ptlrpc_hr.hr_partitions == NULL)
		return;

	ptlrpc_stop_hr_threads();

	cfs_percpt_for_each(hrp, i, ptlrpc_hr.hr_partitions) {
		if (hrp->hrp_thrs != NULL) {
			OBD_FREE(hrp->hrp_thrs,
				 hrp->hrp_nthrs * sizeof(hrp->hrp_thrs[0]));
		}
	}

	cfs_percpt_free(ptlrpc_hr.hr_partitions);
	ptlrpc_hr.hr_partitions = NULL;
}

#endif /* __KERNEL__ */

/**
 * Wait until all already scheduled replies are processed.
 */
static void ptlrpc_wait_replies(struct ptlrpc_service_part *svcpt)
{
	while (1) {
		int rc;
		struct l_wait_info lwi = LWI_TIMEOUT(cfs_time_seconds(10),
						     NULL, NULL);

		rc = l_wait_event(svcpt->scp_waitq,
		     cfs_atomic_read(&svcpt->scp_nreps_difficult) == 0, &lwi);
		if (rc == 0)
			break;
		CWARN("Unexpectedly long timeout %s %p\n",
		      svcpt->scp_service->srv_name, svcpt->scp_service);
	}
}

static void
ptlrpc_service_del_atimer(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part	*svcpt;
	int				i;

	/* early disarm AT timer... */
	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service != NULL)
			cfs_timer_disarm(&svcpt->scp_at_timer);
	}
}

static void
ptlrpc_service_unlink_rqbd(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part	  *svcpt;
	struct ptlrpc_request_buffer_desc *rqbd;
	struct l_wait_info		  lwi;
	int				  rc;
	int				  i;

	/* All history will be culled when the next request buffer is
	 * freed in ptlrpc_service_purge_all() */
	svc->srv_hist_nrqbds_cpt_max = 0;

	rc = LNetClearLazyPortal(svc->srv_req_portal);
	LASSERT(rc == 0);

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service == NULL)
			break;

		/* Unlink all the request buffers.  This forces a 'final'
		 * event with its 'unlink' flag set for each posted rqbd */
		cfs_list_for_each_entry(rqbd, &svcpt->scp_rqbd_posted,
					rqbd_list) {
			rc = LNetMDUnlink(rqbd->rqbd_md_h);
			LASSERT(rc == 0 || rc == -ENOENT);
		}
	}

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service == NULL)
			break;

		/* Wait for the network to release any buffers
		 * it's currently filling */
		cfs_spin_lock(&svcpt->scp_lock);
		while (svcpt->scp_nrqbds_posted != 0) {
			cfs_spin_unlock(&svcpt->scp_lock);
			/* Network access will complete in finite time but
			 * the HUGE timeout lets us CWARN for visibility
			 * of sluggish NALs */
			lwi = LWI_TIMEOUT_INTERVAL(
					cfs_time_seconds(LONG_UNLINK),
					cfs_time_seconds(1), NULL, NULL);
			rc = l_wait_event(svcpt->scp_waitq,
					  svcpt->scp_nrqbds_posted == 0, &lwi);
			if (rc == -ETIMEDOUT) {
				CWARN("Service %s waiting for "
				      "request buffers\n",
				      svcpt->scp_service->srv_name);
			}
			cfs_spin_lock(&svcpt->scp_lock);
		}
		cfs_spin_unlock(&svcpt->scp_lock);
	}
}

static void
ptlrpc_service_purge_all(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part		*svcpt;
	struct ptlrpc_request_buffer_desc	*rqbd;
	struct ptlrpc_request			*req;
	struct ptlrpc_reply_state		*rs;
	int					i;

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service == NULL)
			break;

		cfs_spin_lock(&svcpt->scp_rep_lock);
		while (!cfs_list_empty(&svcpt->scp_rep_active)) {
			rs = cfs_list_entry(svcpt->scp_rep_active.next,
					    struct ptlrpc_reply_state, rs_list);
			cfs_spin_lock(&rs->rs_lock);
			ptlrpc_schedule_difficult_reply(rs);
			cfs_spin_unlock(&rs->rs_lock);
		}
		cfs_spin_unlock(&svcpt->scp_rep_lock);

		/* purge the request queue.  NB No new replies (rqbds
		 * all unlinked) and no service threads, so I'm the only
		 * thread noodling the request queue now */
		while (!cfs_list_empty(&svcpt->scp_req_incoming)) {
			req = cfs_list_entry(svcpt->scp_req_incoming.next,
					     struct ptlrpc_request, rq_list);

			cfs_list_del(&req->rq_list);
			svcpt->scp_nreqs_incoming--;
			svcpt->scp_nreqs_active++;
			ptlrpc_server_finish_request(svcpt, req);
		}

		while (ptlrpc_server_request_pending(svcpt, 1)) {
			req = ptlrpc_server_request_get(svcpt, 1);
			cfs_list_del(&req->rq_list);
			svcpt->scp_nreqs_active++;
			ptlrpc_server_hpreq_fini(req);

			if (req->rq_export != NULL)
				class_export_rpc_put(req->rq_export);
			ptlrpc_server_finish_request(svcpt, req);
		}

		LASSERT(cfs_list_empty(&svcpt->scp_rqbd_posted));
		LASSERT(svcpt->scp_nreqs_incoming == 0);
		LASSERT(svcpt->scp_nreqs_active == 0);
		/* history should have been culled by
		 * ptlrpc_server_finish_request */
		LASSERT(svcpt->scp_hist_nrqbds == 0);

		/* Now free all the request buffers since nothing
		 * references them any more... */

		while (!cfs_list_empty(&svcpt->scp_rqbd_idle)) {
			rqbd = cfs_list_entry(svcpt->scp_rqbd_idle.next,
					      struct ptlrpc_request_buffer_desc,
					      rqbd_list);
			ptlrpc_free_rqbd(rqbd);
		}
		ptlrpc_wait_replies(svcpt);

		while (!cfs_list_empty(&svcpt->scp_rep_idle)) {
			rs = cfs_list_entry(svcpt->scp_rep_idle.next,
					    struct ptlrpc_reply_state,
					    rs_list);
			cfs_list_del(&rs->rs_list);
			OBD_FREE_LARGE(rs, svc->srv_max_reply_size);
		}
	}
}

static void
ptlrpc_service_free(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part	*svcpt;
	struct ptlrpc_at_array		*array;
	int				i;

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		if (svcpt->scp_service == NULL)
			break;

		/* In case somebody rearmed this in the meantime */
		cfs_timer_disarm(&svcpt->scp_at_timer);
		array = &svcpt->scp_at_array;

		if (array->paa_reqs_array != NULL) {
			OBD_FREE(array->paa_reqs_array,
				 sizeof(cfs_list_t) * array->paa_size);
			array->paa_reqs_array = NULL;
		}

		if (array->paa_reqs_count != NULL) {
			OBD_FREE(array->paa_reqs_count,
				 sizeof(__u32) * array->paa_size);
			array->paa_reqs_count = NULL;
		}
	}

	ptlrpc_service_for_each_part(svcpt, i, svc)
		OBD_FREE_PTR(svcpt);

	if (svc->srv_cpts != NULL)
		cfs_expr_list_values_free(svc->srv_cpts, svc->srv_ncpts);

	OBD_FREE(svc, offsetof(struct ptlrpc_service,
			       srv_parts[svc->srv_ncpts]));
}

int ptlrpc_unregister_service(struct ptlrpc_service *service)
{
	ENTRY;

	CDEBUG(D_NET, "%s: tearing down\n", service->srv_name);

	service->srv_is_stopping = 1;

	cfs_spin_lock(&ptlrpc_all_services_lock);
	cfs_list_del_init(&service->srv_list);
	cfs_spin_unlock(&ptlrpc_all_services_lock);

	ptlrpc_lprocfs_unregister_service(service);

	ptlrpc_service_del_atimer(service);
	ptlrpc_stop_all_threads(service);

	ptlrpc_service_unlink_rqbd(service);
	ptlrpc_service_purge_all(service);
	ptlrpc_service_free(service);

	RETURN(0);
}
EXPORT_SYMBOL(ptlrpc_unregister_service);

/**
 * Returns 0 if the service is healthy.
 *
 * Right now, it just checks to make sure that requests aren't languishing
 * in the queue.  We'll use this health check to govern whether a node needs
 * to be shot, so it's intentionally non-aggressive. */
int ptlrpc_svcpt_health_check(struct ptlrpc_service_part *svcpt)
{
	struct ptlrpc_request		*request;
	struct timeval			right_now;
	long				timediff;

	cfs_gettimeofday(&right_now);

	cfs_spin_lock(&svcpt->scp_req_lock);
	if (!ptlrpc_server_request_pending(svcpt, 1)) {
		cfs_spin_unlock(&svcpt->scp_req_lock);
		return 0;
	}

	/* How long has the next entry been waiting? */
	if (cfs_list_empty(&svcpt->scp_req_pending)) {
		request = cfs_list_entry(svcpt->scp_hreq_pending.next,
					 struct ptlrpc_request, rq_list);
	} else {
		request = cfs_list_entry(svcpt->scp_req_pending.next,
					 struct ptlrpc_request, rq_list);
	}

	timediff = cfs_timeval_sub(&right_now, &request->rq_arrival_time, NULL);
	cfs_spin_unlock(&svcpt->scp_req_lock);

	if ((timediff / ONE_MILLION) >
	    (AT_OFF ? obd_timeout * 3 / 2 : at_max)) {
		CERROR("%s: unhealthy - request has been waiting %lds\n",
		       svcpt->scp_service->srv_name, timediff / ONE_MILLION);
		return -1;
	}

	return 0;
}

int
ptlrpc_service_health_check(struct ptlrpc_service *svc)
{
	struct ptlrpc_service_part	*svcpt;
	int				i;

	if (svc == NULL || svc->srv_parts == NULL)
		return 0;

	ptlrpc_service_for_each_part(svcpt, i, svc) {
		int rc = ptlrpc_svcpt_health_check(svcpt);

		if (rc != 0)
			return rc;
	}
	return 0;
}
EXPORT_SYMBOL(ptlrpc_service_health_check);
