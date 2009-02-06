/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
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
unsigned int at_min = 0;
CFS_MODULE_PARM(at_min, "i", int, 0644,
                "Adaptive timeout minimum (sec)");
unsigned int at_max = 600;
EXPORT_SYMBOL(at_max);
CFS_MODULE_PARM(at_max, "i", int, 0644,
                "Adaptive timeout maximum (sec)");
unsigned int at_history = 600;
CFS_MODULE_PARM(at_history, "i", int, 0644,
                "Adaptive timeouts remember the slowest event that took place "
                "within this period (sec)");
static int at_early_margin = 5;
CFS_MODULE_PARM(at_early_margin, "i", int, 0644,
                "How soon before an RPC deadline to send an early reply");
static int at_extra = 30;
CFS_MODULE_PARM(at_extra, "i", int, 0644,
                "How much extra time to give with each early reply");


/* forward ref */
static int ptlrpc_server_post_idle_rqbds (struct ptlrpc_service *svc);

static CFS_LIST_HEAD(ptlrpc_all_services);
spinlock_t ptlrpc_all_services_lock;

static char *
ptlrpc_alloc_request_buffer (int size)
{
        char *ptr;

        if (size > SVC_BUF_VMALLOC_THRESHOLD)
                OBD_VMALLOC(ptr, size);
        else
                OBD_ALLOC(ptr, size);

        return (ptr);
}

static void
ptlrpc_free_request_buffer (char *ptr, int size)
{
        if (size > SVC_BUF_VMALLOC_THRESHOLD)
                OBD_VFREE(ptr, size);
        else
                OBD_FREE(ptr, size);
}

struct ptlrpc_request_buffer_desc *
ptlrpc_alloc_rqbd (struct ptlrpc_service *svc)
{
        struct ptlrpc_request_buffer_desc *rqbd;

        OBD_ALLOC_PTR(rqbd);
        if (rqbd == NULL)
                return (NULL);

        rqbd->rqbd_service = svc;
        rqbd->rqbd_refcount = 0;
        rqbd->rqbd_cbid.cbid_fn = request_in_callback;
        rqbd->rqbd_cbid.cbid_arg = rqbd;
        CFS_INIT_LIST_HEAD(&rqbd->rqbd_reqs);
        rqbd->rqbd_buffer = ptlrpc_alloc_request_buffer(svc->srv_buf_size);

        if (rqbd->rqbd_buffer == NULL) {
                OBD_FREE_PTR(rqbd);
                return (NULL);
        }

        spin_lock(&svc->srv_lock);
        list_add(&rqbd->rqbd_list, &svc->srv_idle_rqbds);
        svc->srv_nbufs++;
        spin_unlock(&svc->srv_lock);

        return (rqbd);
}

void
ptlrpc_free_rqbd (struct ptlrpc_request_buffer_desc *rqbd)
{
        struct ptlrpc_service *svc = rqbd->rqbd_service;

        LASSERT (rqbd->rqbd_refcount == 0);
        LASSERT (list_empty(&rqbd->rqbd_reqs));

        spin_lock(&svc->srv_lock);
        list_del(&rqbd->rqbd_list);
        svc->srv_nbufs--;
        spin_unlock(&svc->srv_lock);

        ptlrpc_free_request_buffer (rqbd->rqbd_buffer, svc->srv_buf_size);
        OBD_FREE_PTR(rqbd);
}

int
ptlrpc_grow_req_bufs(struct ptlrpc_service *svc)
{
        struct ptlrpc_request_buffer_desc *rqbd;
        int                                i;

        CDEBUG(D_RPCTRACE, "%s: allocate %d new %d-byte reqbufs (%d/%d left)\n",
               svc->srv_name, svc->srv_nbuf_per_group, svc->srv_buf_size,
               svc->srv_nrqbd_receiving, svc->srv_nbufs);
        for (i = 0; i < svc->srv_nbuf_per_group; i++) {
                rqbd = ptlrpc_alloc_rqbd(svc);

                if (rqbd == NULL) {
                        CERROR ("%s: Can't allocate request buffer\n",
                                svc->srv_name);
                        return (-ENOMEM);
                }

                if (ptlrpc_server_post_idle_rqbds(svc) < 0)
                        return (-EAGAIN);
        }

        return (0);
}

void
ptlrpc_save_lock (struct ptlrpc_request *req,
                  struct lustre_handle *lock, int mode, int no_ack)
{
        struct ptlrpc_reply_state *rs = req->rq_reply_state;
        int                        idx;

        LASSERT(rs != NULL);
        LASSERT(rs->rs_nlocks < RS_MAX_LOCKS);

        idx = rs->rs_nlocks++;
        rs->rs_locks[idx] = *lock;
        rs->rs_modes[idx] = mode;
        rs->rs_difficult = 1;
        rs->rs_no_ack = !!no_ack;
}

#ifdef __KERNEL__

#define HRT_RUNNING 0
#define HRT_STOPPING 1

struct ptlrpc_hr_thread {
        spinlock_t        hrt_lock;
        unsigned long     hrt_flags;
        cfs_waitq_t       hrt_wait;
        struct list_head  hrt_queue;
        struct completion hrt_completion;
};

struct ptlrpc_hr_service {
        int                     hr_index;
        int                     hr_n_threads;
        int                     hr_size;
        struct ptlrpc_hr_thread hr_threads[0];
};

struct rs_batch {
        struct list_head        rsb_replies;
        struct ptlrpc_service  *rsb_svc;
        unsigned int            rsb_n_replies;
};

/**
 *  A pointer to per-node reply handling service.
 */
static struct ptlrpc_hr_service *ptlrpc_hr = NULL;

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
 * Dispatch all replies accumulated in the batch to one from
 * dedicated reply handing threads.
 *
 * \param b batch
 */
static void rs_batch_dispatch(struct rs_batch *b)
{
        if (b->rsb_n_replies != 0) {
                struct ptlrpc_hr_service *hr = ptlrpc_hr;
                int idx;

                idx = hr->hr_index++;
                if (hr->hr_index >= hr->hr_n_threads)
                        hr->hr_index = 0;

                spin_lock(&hr->hr_threads[idx].hrt_lock);
                list_splice_init(&b->rsb_replies,
                                 &hr->hr_threads[idx].hrt_queue);
                spin_unlock(&hr->hr_threads[idx].hrt_lock);
                cfs_waitq_signal(&hr->hr_threads[idx].hrt_wait);
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
        struct ptlrpc_service *svc = rs->rs_service;

        if (svc != b->rsb_svc || b->rsb_n_replies >= MAX_SCHEDULED) {
                if (b->rsb_svc != NULL) {
                        rs_batch_dispatch(b);
                        spin_unlock(&b->rsb_svc->srv_lock);
                }
                spin_lock(&svc->srv_lock);
                b->rsb_svc = svc;
        }
        spin_lock(&rs->rs_lock);
        rs->rs_scheduled_ever = 1;
        if (rs->rs_scheduled == 0) {
                list_move(&rs->rs_list, &b->rsb_replies);
                rs->rs_scheduled = 1;
                b->rsb_n_replies++;
        }
        spin_unlock(&rs->rs_lock);
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
        if (b->rsb_svc != 0) {
                rs_batch_dispatch(b);
                spin_unlock(&b->rsb_svc->srv_lock);
        }
}

#define DECLARE_RS_BATCH(b)     struct rs_batch b

#else /* __KERNEL__ */

#define rs_batch_init(b)        do{}while(0)
#define rs_batch_fini(b)        do{}while(0)
#define rs_batch_add(b, r)      ptlrpc_schedule_difficult_reply(r)
#define DECLARE_RS_BATCH(b)

#endif /* __KERNEL__ */

void ptlrpc_dispatch_difficult_reply(struct ptlrpc_reply_state *rs)
{
#ifdef __KERNEL__
        struct ptlrpc_hr_service *hr = ptlrpc_hr;
        int idx;
        ENTRY;

        LASSERT(list_empty(&rs->rs_list));

        idx = hr->hr_index++;
        if (hr->hr_index >= hr->hr_n_threads)
                hr->hr_index = 0;
        spin_lock(&hr->hr_threads[idx].hrt_lock);
        list_add_tail(&rs->rs_list, &hr->hr_threads[idx].hrt_queue);
        spin_unlock(&hr->hr_threads[idx].hrt_lock);
        cfs_waitq_signal(&hr->hr_threads[idx].hrt_wait);
        EXIT;
#else
        list_add_tail(&rs->rs_list, &rs->rs_service->srv_reply_queue);
#endif
}

void
ptlrpc_schedule_difficult_reply (struct ptlrpc_reply_state *rs)
{
        ENTRY;

        LASSERT_SPIN_LOCKED(&rs->rs_service->srv_lock);
        LASSERT_SPIN_LOCKED(&rs->rs_lock);
        LASSERT (rs->rs_difficult);
        rs->rs_scheduled_ever = 1;              /* flag any notification attempt */

        if (rs->rs_scheduled) {                  /* being set up or already notified */
                EXIT;
                return;
        }

        rs->rs_scheduled = 1;
        list_del_init(&rs->rs_list);
        ptlrpc_dispatch_difficult_reply(rs);
        EXIT;
}

void
ptlrpc_commit_replies (struct obd_device *obd)
{
        struct list_head   *tmp;
        struct list_head   *nxt;
        DECLARE_RS_BATCH(batch);
        ENTRY;

        rs_batch_init(&batch);
        /* Find any replies that have been committed and get their service
         * to attend to complete them. */

        /* CAVEAT EMPTOR: spinlock ordering!!! */
        spin_lock(&obd->obd_uncommitted_replies_lock);
        list_for_each_safe (tmp, nxt, &obd->obd_uncommitted_replies) {
                struct ptlrpc_reply_state *rs =
                        list_entry(tmp, struct ptlrpc_reply_state, rs_obd_list);

                LASSERT (rs->rs_difficult);

                if (rs->rs_transno <= obd->obd_last_committed) {
                        list_del_init(&rs->rs_obd_list);
                        rs_batch_add(&batch, rs);
                }
        }
        spin_unlock(&obd->obd_uncommitted_replies_lock);
        rs_batch_fini(&batch);
        EXIT;
}

static int
ptlrpc_server_post_idle_rqbds (struct ptlrpc_service *svc)
{
        struct ptlrpc_request_buffer_desc *rqbd;
        int                                rc;
        int                                posted = 0;

        for (;;) {
                spin_lock(&svc->srv_lock);

                if (list_empty (&svc->srv_idle_rqbds)) {
                        spin_unlock(&svc->srv_lock);
                        return (posted);
                }

                rqbd = list_entry(svc->srv_idle_rqbds.next,
                                  struct ptlrpc_request_buffer_desc,
                                  rqbd_list);
                list_del (&rqbd->rqbd_list);

                /* assume we will post successfully */
                svc->srv_nrqbd_receiving++;
                list_add (&rqbd->rqbd_list, &svc->srv_active_rqbds);

                spin_unlock(&svc->srv_lock);

                rc = ptlrpc_register_rqbd(rqbd);
                if (rc != 0)
                        break;

                posted = 1;
        }

        spin_lock(&svc->srv_lock);

        svc->srv_nrqbd_receiving--;
        list_del(&rqbd->rqbd_list);
        list_add_tail(&rqbd->rqbd_list, &svc->srv_idle_rqbds);

        /* Don't complain if no request buffers are posted right now; LNET
         * won't drop requests because we set the portal lazy! */

        spin_unlock(&svc->srv_lock);

        return (-1);
}

struct ptlrpc_service *ptlrpc_init_svc_conf(struct ptlrpc_service_conf *c,
                                            svc_handler_t h, char *name,
                                            struct proc_dir_entry *proc_entry,
                                            svcreq_printfn_t prntfn,
                                            char *threadname)
{
        return ptlrpc_init_svc(c->psc_nbufs, c->psc_bufsize,
                               c->psc_max_req_size, c->psc_max_reply_size,
                               c->psc_req_portal, c->psc_rep_portal,
                               c->psc_watchdog_factor,
                               h, name, proc_entry,
                               prntfn, c->psc_min_threads, c->psc_max_threads,
                               threadname, c->psc_ctx_tags, NULL);
}
EXPORT_SYMBOL(ptlrpc_init_svc_conf);

static void ptlrpc_at_timer(unsigned long castmeharder)
{
        struct ptlrpc_service *svc = (struct ptlrpc_service *)castmeharder;
        svc->srv_at_check = 1;
        svc->srv_at_checktime = cfs_time_current();
        cfs_waitq_signal(&svc->srv_waitq);
}

/* @threadname should be 11 characters or less - 3 will be added on */
struct ptlrpc_service *
ptlrpc_init_svc(int nbufs, int bufsize, int max_req_size, int max_reply_size,
                int req_portal, int rep_portal, int watchdog_factor,
                svc_handler_t handler, char *name,
                cfs_proc_dir_entry_t *proc_entry,
                svcreq_printfn_t svcreq_printfn,
                int min_threads, int max_threads,
                char *threadname, __u32 ctx_tags,
                svc_hpreq_handler_t hp_handler)
{
        int                    rc;
        struct ptlrpc_service *service;
        ENTRY;

        LASSERT (nbufs > 0);
        LASSERT (bufsize >= max_req_size + SPTLRPC_MAX_PAYLOAD);
        LASSERT (ctx_tags != 0);

        OBD_ALLOC_PTR(service);
        if (service == NULL)
                RETURN(NULL);

        /* First initialise enough for early teardown */

        service->srv_name = name;
        spin_lock_init(&service->srv_lock);
        CFS_INIT_LIST_HEAD(&service->srv_threads);
        cfs_waitq_init(&service->srv_waitq);

        service->srv_nbuf_per_group = test_req_buffer_pressure ? 1 : nbufs;
        service->srv_max_req_size = max_req_size + SPTLRPC_MAX_PAYLOAD;
        service->srv_buf_size = bufsize;
        service->srv_rep_portal = rep_portal;
        service->srv_req_portal = req_portal;
        service->srv_watchdog_factor = watchdog_factor;
        service->srv_handler = handler;
        service->srv_request_history_print_fn = svcreq_printfn;
        service->srv_request_seq = 1;           /* valid seq #s start at 1 */
        service->srv_request_max_cull_seq = 0;
        service->srv_threads_min = min_threads;
        service->srv_threads_max = max_threads;
        service->srv_thread_name = threadname;
        service->srv_ctx_tags = ctx_tags;
        service->srv_hpreq_handler = hp_handler;
        service->srv_hpreq_ratio = PTLRPC_SVC_HP_RATIO;
        service->srv_hpreq_count = 0;
        service->srv_n_hpreq = 0;

        rc = LNetSetLazyPortal(service->srv_req_portal);
        LASSERT (rc == 0);

        CFS_INIT_LIST_HEAD(&service->srv_request_queue);
        CFS_INIT_LIST_HEAD(&service->srv_request_hpq);
        CFS_INIT_LIST_HEAD(&service->srv_idle_rqbds);
        CFS_INIT_LIST_HEAD(&service->srv_active_rqbds);
        CFS_INIT_LIST_HEAD(&service->srv_history_rqbds);
        CFS_INIT_LIST_HEAD(&service->srv_request_history);
        CFS_INIT_LIST_HEAD(&service->srv_active_replies);
#ifndef __KERNEL__
        CFS_INIT_LIST_HEAD(&service->srv_reply_queue);
#endif
        CFS_INIT_LIST_HEAD(&service->srv_free_rs_list);
        cfs_waitq_init(&service->srv_free_rs_waitq);
        atomic_set(&service->srv_n_difficult_replies, 0);

        spin_lock_init(&service->srv_at_lock);
        CFS_INIT_LIST_HEAD(&service->srv_req_in_queue);
        CFS_INIT_LIST_HEAD(&service->srv_at_list);
        cfs_timer_init(&service->srv_at_timer, ptlrpc_at_timer, service);
        /* At SOW, service time should be quick; 10s seems generous. If client
           timeout is less than this, we'll be sending an early reply. */
        at_init(&service->srv_at_estimate, 10, 0);

        spin_lock (&ptlrpc_all_services_lock);
        list_add (&service->srv_list, &ptlrpc_all_services);
        spin_unlock (&ptlrpc_all_services_lock);

        /* Now allocate the request buffers */
        rc = ptlrpc_grow_req_bufs(service);
        /* We shouldn't be under memory pressure at startup, so
         * fail if we can't post all our buffers at this time. */
        if (rc != 0)
                GOTO(failed, NULL);

        /* Now allocate pool of reply buffers */
        /* Increase max reply size to next power of two */
        service->srv_max_reply_size = 1;
        while (service->srv_max_reply_size <
               max_reply_size + SPTLRPC_MAX_PAYLOAD)
                service->srv_max_reply_size <<= 1;

        if (proc_entry != NULL)
                ptlrpc_lprocfs_register_service(proc_entry, service);

        CDEBUG(D_NET, "%s: Started, listening on portal %d\n",
               service->srv_name, service->srv_req_portal);

        RETURN(service);
failed:
        ptlrpc_unregister_service(service);
        return NULL;
}

/**
 * to actually free the request, must be called without holding svc_lock.
 * note it's caller's responsibility to unlink req->rq_list.
 */
static void ptlrpc_server_free_request(struct ptlrpc_request *req)
{
        LASSERT(atomic_read(&req->rq_refcount) == 0);
        LASSERT(list_empty(&req->rq_timed_list));

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
static void ptlrpc_server_drop_request(struct ptlrpc_request *req)
{
        struct ptlrpc_request_buffer_desc *rqbd = req->rq_rqbd;
        struct ptlrpc_service             *svc = rqbd->rqbd_service;
        int                                refcount;
        struct list_head                  *tmp;
        struct list_head                  *nxt;

        if (!atomic_dec_and_test(&req->rq_refcount))
                return;

        spin_lock(&svc->srv_lock);

        svc->srv_n_active_reqs--;
        list_add(&req->rq_list, &rqbd->rqbd_reqs);

        refcount = --(rqbd->rqbd_refcount);
        if (refcount == 0) {
                /* request buffer is now idle: add to history */
                list_del(&rqbd->rqbd_list);
                list_add_tail(&rqbd->rqbd_list, &svc->srv_history_rqbds);
                svc->srv_n_history_rqbds++;

                /* cull some history?
                 * I expect only about 1 or 2 rqbds need to be recycled here */
                while (svc->srv_n_history_rqbds > svc->srv_max_history_rqbds) {
                        rqbd = list_entry(svc->srv_history_rqbds.next,
                                          struct ptlrpc_request_buffer_desc,
                                          rqbd_list);

                        list_del(&rqbd->rqbd_list);
                        svc->srv_n_history_rqbds--;

                        /* remove rqbd's reqs from svc's req history while
                         * I've got the service lock */
                        list_for_each(tmp, &rqbd->rqbd_reqs) {
                                req = list_entry(tmp, struct ptlrpc_request,
                                                 rq_list);
                                /* Track the highest culled req seq */
                                if (req->rq_history_seq >
                                    svc->srv_request_max_cull_seq)
                                        svc->srv_request_max_cull_seq =
                                                req->rq_history_seq;
                                list_del(&req->rq_history_list);
                        }

                        spin_unlock(&svc->srv_lock);

                        list_for_each_safe(tmp, nxt, &rqbd->rqbd_reqs) {
                                req = list_entry(rqbd->rqbd_reqs.next,
                                                 struct ptlrpc_request,
                                                 rq_list);
                                list_del(&req->rq_list);
                                ptlrpc_server_free_request(req);
                        }

                        spin_lock(&svc->srv_lock);
                        /*
                         * now all reqs including the embedded req has been
                         * disposed, schedule request buffer for re-use.
                         */
                        LASSERT(atomic_read(&rqbd->rqbd_req.rq_refcount) == 0);
                        list_add_tail(&rqbd->rqbd_list, &svc->srv_idle_rqbds);
                }

                spin_unlock(&svc->srv_lock);
        } else if (req->rq_reply_state && req->rq_reply_state->rs_prealloc) {
                /* If we are low on memory, we are not interested in history */
                list_del(&req->rq_list);
                list_del_init(&req->rq_history_list);
                spin_unlock(&svc->srv_lock);

                ptlrpc_server_free_request(req);
        } else {
                spin_unlock(&svc->srv_lock);
        }
}

/**
 * to finish a request: stop sending more early replies, and release
 * the request. should be called after we finished handling the request.
 */
static void ptlrpc_server_finish_request(struct ptlrpc_request *req)
{
        struct ptlrpc_service  *svc = req->rq_rqbd->rqbd_service;

        if (req->rq_export) {
                class_export_put(req->rq_export);
                req->rq_export = NULL;
        }

        if (req->rq_phase != RQ_PHASE_NEW) /* incorrect message magic */
                DEBUG_REQ(D_INFO, req, "free req");

        spin_lock(&svc->srv_at_lock);
        req->rq_sent_final = 1;
        list_del_init(&req->rq_timed_list);
        spin_unlock(&svc->srv_at_lock);

        ptlrpc_server_drop_request(req);
}

/* This function makes sure dead exports are evicted in a timely manner.
   This function is only called when some export receives a message (i.e.,
   the network is up.) */
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
        spin_lock(&exp->exp_obd->obd_dev_lock);

        if (list_empty(&exp->exp_obd_chain_timed)) {
                /* this one is not timed */
                spin_unlock(&exp->exp_obd->obd_dev_lock);
                RETURN_EXIT;
        }

        list_move_tail(&exp->exp_obd_chain_timed,
                       &exp->exp_obd->obd_exports_timed);

        oldest_exp = list_entry(exp->exp_obd->obd_exports_timed.next,
                                struct obd_export, exp_obd_chain_timed);
        oldest_time = oldest_exp->exp_last_request_time;
        spin_unlock(&exp->exp_obd->obd_dev_lock);

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
                               exp->exp_obd->obd_name, obd_export_nid2str(exp),
                               oldest_time);
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

static int ptlrpc_check_req(struct ptlrpc_request *req)
{
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
                req->rq_status = -ENODEV;
                ptlrpc_error(req);
                return -ENODEV;
        }

        return 0;
}

static void ptlrpc_at_set_timer(struct ptlrpc_service *svc)
{
        struct ptlrpc_request *rq;
        __s32 next;

        spin_lock(&svc->srv_at_lock);
        if (list_empty(&svc->srv_at_list)) {
                cfs_timer_disarm(&svc->srv_at_timer);
                spin_unlock(&svc->srv_at_lock);
                return;
        }

        /* Set timer for closest deadline */
        rq = list_entry(svc->srv_at_list.next, struct ptlrpc_request,
                        rq_timed_list);
        next = (__s32)(rq->rq_deadline - cfs_time_current_sec() -
                       at_early_margin);
        if (next <= 0)
                ptlrpc_at_timer((unsigned long)svc);
        else
                cfs_timer_arm(&svc->srv_at_timer, cfs_time_shift(next));
        spin_unlock(&svc->srv_at_lock);
        CDEBUG(D_INFO, "armed %s at %+ds\n", svc->srv_name, next);
}

/* Add rpc to early reply check list */
static int ptlrpc_at_add_timed(struct ptlrpc_request *req)
{
        struct ptlrpc_service *svc = req->rq_rqbd->rqbd_service;
        struct ptlrpc_request *rq;
        int found = 0;

        if (AT_OFF)
                return(0);

        if (req->rq_no_reply)
                return 0;

        if ((lustre_msghdr_get_flags(req->rq_reqmsg) & MSGHDR_AT_SUPPORT) == 0)
                return(-ENOSYS);

        spin_lock(&svc->srv_at_lock);

        if (unlikely(req->rq_sent_final)) {
                spin_unlock(&svc->srv_at_lock);
                return 0;
        }

        LASSERT(list_empty(&req->rq_timed_list));
        /* Add to sorted list.  Presumably latest rpcs will have the latest
           deadlines, so search backward. */
        list_for_each_entry_reverse(rq, &svc->srv_at_list, rq_timed_list) {
                if (req->rq_deadline >= rq->rq_deadline) {
                        list_add(&req->rq_timed_list, &rq->rq_timed_list);
                        found++;
                        break;
                }
        }
        if (!found)
                /* Add to front if shortest deadline or list empty */
                list_add(&req->rq_timed_list, &svc->srv_at_list);

        /* Check if we're the head of the list */
        found = (svc->srv_at_list.next == &req->rq_timed_list);

        spin_unlock(&svc->srv_at_lock);

        if (found)
                ptlrpc_at_set_timer(svc);

        return 0;
}

static int ptlrpc_at_send_early_reply(struct ptlrpc_request *req,
                                      int extra_time)
{
        struct ptlrpc_service *svc = req->rq_rqbd->rqbd_service;
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
                  olddl, olddl - at_get(&svc->srv_at_estimate),
                  at_get(&svc->srv_at_estimate), extra_time);

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

        if (req->rq_export && req->rq_export->exp_in_recovery) {
                /* don't increase server estimates during recovery, and give
                   clients the full recovery time. */
                newdl = cfs_time_current_sec() +
                        req->rq_export->exp_obd->obd_recovery_timeout;
        } else {
                if (extra_time) {
                        /* Fake our processing time into the future to ask the
                           clients for some extra amount of time */
                        extra_time += cfs_time_current_sec() -
                                req->rq_arrival_time.tv_sec;
                        at_add(&svc->srv_at_estimate, extra_time);
                }
                newdl = req->rq_arrival_time.tv_sec +
                        at_get(&svc->srv_at_estimate);
        }
        if (req->rq_deadline >= newdl) {
                /* We're not adding any time, no need to send an early reply
                   (e.g. maybe at adaptive_max) */
                DEBUG_REQ(D_WARNING, req, "Couldn't add any time ("
                          CFS_DURATION_T"/"CFS_DURATION_T"), "
                          "not sending early reply\n", olddl,
                          cfs_time_sub(newdl, cfs_time_current_sec()));
                RETURN(-ETIMEDOUT);
        }

        OBD_ALLOC(reqcopy, sizeof *reqcopy);
        if (reqcopy == NULL)
                RETURN(-ENOMEM);
        OBD_ALLOC(reqmsg, req->rq_reqlen);
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

        if (req->rq_sent_final) {
                DEBUG_REQ(D_ADAPTTO, reqcopy, "Normal reply already sent out, "
                          "abort sending early reply\n");
                GOTO(out, rc = 0);
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
        OBD_FREE(reqmsg, req->rq_reqlen);
        OBD_FREE(reqcopy, sizeof *reqcopy);
        RETURN(rc);
}

/* Send early replies to everybody expiring within at_early_margin
   asking for at_extra time */
static int ptlrpc_at_check_timed(struct ptlrpc_service *svc)
{
        struct ptlrpc_request *rq, *n;
        struct list_head work_list;
        time_t now = cfs_time_current_sec();
        cfs_duration_t delay;
        int first, counter = 0;
        ENTRY;

        spin_lock(&svc->srv_at_lock);
        if (svc->srv_at_check == 0) {
                spin_unlock(&svc->srv_at_lock);
                RETURN(0);
        }
        delay = cfs_time_sub(cfs_time_current(), svc->srv_at_checktime);
        svc->srv_at_check = 0;

        if (list_empty(&svc->srv_at_list)) {
                spin_unlock(&svc->srv_at_lock);
                RETURN(0);
        }

        /* The timer went off, but maybe the nearest rpc already completed. */
        rq = list_entry(svc->srv_at_list.next, struct ptlrpc_request,
                        rq_timed_list);
        first = (int)(rq->rq_deadline - now);
        if (first > at_early_margin) {
                /* We've still got plenty of time.  Reset the timer. */
                spin_unlock(&svc->srv_at_lock);
                ptlrpc_at_set_timer(svc);
                RETURN(0);
        }

        /* We're close to a timeout, and we don't know how much longer the
           server will take. Send early replies to everyone expiring soon. */
        CFS_INIT_LIST_HEAD(&work_list);
        list_for_each_entry_safe(rq, n, &svc->srv_at_list, rq_timed_list) {
                if (rq->rq_deadline <= now + at_early_margin) {
                        list_move_tail(&rq->rq_timed_list, &work_list);
                        counter++;
                } else {
                        break;
                }
        }

        spin_unlock(&svc->srv_at_lock);

        /* we have a new earliest deadline, restart the timer */
        ptlrpc_at_set_timer(svc);

        CDEBUG(D_ADAPTTO, "timeout in %+ds, asking for %d secs on %d early "
               "replies\n", first, at_extra, counter);
        if (first < 0) {
                /* We're already past request deadlines before we even get a
                   chance to send early replies */
                LCONSOLE_WARN("%s: This server is not able to keep up with "
                              "request traffic (cpu-bound).\n", svc->srv_name);
                CWARN("earlyQ=%d reqQ=%d recA=%d, svcEst=%d, "
                      "delay="CFS_DURATION_T"(jiff)\n",
                      counter, svc->srv_n_queued_reqs, svc->srv_n_active_reqs,
                      at_get(&svc->srv_at_estimate), delay);
        }

        /* ptlrpc_server_finish_request may delete an entry out of
         * the work list */
        spin_lock(&svc->srv_at_lock);
        while (!list_empty(&work_list)) {
                rq = list_entry(work_list.next, struct ptlrpc_request,
                                rq_timed_list);
                list_del_init(&rq->rq_timed_list);
                /* if the entry is still in the worklist, it hasn't been
                   deleted, and is safe to take a ref to keep the req around */
                atomic_inc(&rq->rq_refcount);
                spin_unlock(&svc->srv_at_lock);

                if (ptlrpc_at_send_early_reply(rq, at_extra) == 0)
                        ptlrpc_at_add_timed(rq);

                ptlrpc_server_drop_request(rq);
                spin_lock(&svc->srv_at_lock);
        }
        spin_unlock(&svc->srv_at_lock);

        RETURN(0);
}

/**
 * Put the request to the export list if the request may become
 * a high priority one.
 */
static int ptlrpc_hpreq_init(struct ptlrpc_service *svc,
                             struct ptlrpc_request *req)
{
        int rc;
        ENTRY;

        if (svc->srv_hpreq_handler) {
                rc = svc->srv_hpreq_handler(req);
                if (rc)
                        RETURN(rc);
        }
        if (req->rq_export && req->rq_ops) {
                spin_lock(&req->rq_export->exp_lock);
                list_add(&req->rq_exp_list, &req->rq_export->exp_queued_rpc);
                spin_unlock(&req->rq_export->exp_lock);
        }

        RETURN(0);
}

/** Remove the request from the export list. */
static void ptlrpc_hpreq_fini(struct ptlrpc_request *req)
{
        ENTRY;
        if (req->rq_export && req->rq_ops) {
                spin_lock(&req->rq_export->exp_lock);
                list_del_init(&req->rq_exp_list);
                spin_unlock(&req->rq_export->exp_lock);
        }
        EXIT;
}

/**
 * Make the request a high priority one.
 *
 * All the high priority requests are queued in a separate FIFO
 * ptlrpc_service::srv_request_hpq list which is parallel to
 * ptlrpc_service::srv_request_queue list but has a higher priority
 * for handling.
 *
 * \see ptlrpc_server_handle_request().
 */
static void ptlrpc_hpreq_reorder_nolock(struct ptlrpc_service *svc,
                                        struct ptlrpc_request *req)
{
        ENTRY;
        LASSERT(svc != NULL);
        spin_lock(&req->rq_lock);
        if (req->rq_hp == 0) {
                int opc = lustre_msg_get_opc(req->rq_reqmsg);

                /* Add to the high priority queue. */
                list_move_tail(&req->rq_list, &svc->srv_request_hpq);
                req->rq_hp = 1;
                if (opc != OBD_PING)
                        DEBUG_REQ(D_NET, req, "high priority req");
        }
        spin_unlock(&req->rq_lock);
        EXIT;
}

void ptlrpc_hpreq_reorder(struct ptlrpc_request *req)
{
        struct ptlrpc_service *svc = req->rq_rqbd->rqbd_service;
        ENTRY;

        spin_lock(&svc->srv_lock);
        /* It may happen that the request is already taken for the processing
         * but still in the export list, do not re-add it into the HP list. */
        if (req->rq_phase == RQ_PHASE_NEW)
                ptlrpc_hpreq_reorder_nolock(svc, req);
        spin_unlock(&svc->srv_lock);
        EXIT;
}

/** Check if the request if a high priority one. */
static int ptlrpc_server_hpreq_check(struct ptlrpc_request *req)
{
        int opc, rc = 0;
        ENTRY;

        /* Check by request opc. */
        opc = lustre_msg_get_opc(req->rq_reqmsg);
        if (opc == OBD_PING)
                RETURN(1);

        /* Perform request specific check. */
        if (req->rq_ops && req->rq_ops->hpreq_check)
                rc = req->rq_ops->hpreq_check(req);
        RETURN(rc);
}

/** Check if a request is a high priority one. */
static int ptlrpc_server_request_add(struct ptlrpc_service *svc,
                                     struct ptlrpc_request *req)
{
        int rc;
        ENTRY;

        rc = ptlrpc_server_hpreq_check(req);
        if (rc < 0)
                RETURN(rc);

        spin_lock(&svc->srv_lock);
        /* Before inserting the request into the queue, check if it is not
         * inserted yet, or even already handled -- it may happen due to
         * a racing ldlm_server_blocking_ast(). */
        if (req->rq_phase == RQ_PHASE_NEW && list_empty(&req->rq_list)) {
                if (rc)
                        ptlrpc_hpreq_reorder_nolock(svc, req);
                else
                        list_add_tail(&req->rq_list, &svc->srv_request_queue);
        }
        spin_unlock(&svc->srv_lock);

        RETURN(0);
}

/* Only allow normal priority requests on a service that has a high-priority
 * queue if forced (i.e. cleanup), if there are other high priority requests
 * already being processed (i.e. those threads can service more high-priority
 * requests), or if there are enough idle threads that a later thread can do
 * a high priority request. */
static int ptlrpc_server_allow_normal(struct ptlrpc_service *svc, int force)
{
        return force || !svc->srv_hpreq_handler || svc->srv_n_hpreq > 0 ||
               svc->srv_n_active_reqs < svc->srv_threads_running - 2;
}

static struct ptlrpc_request *
ptlrpc_server_request_get(struct ptlrpc_service *svc, int force)
{
        struct ptlrpc_request *req = NULL;
        ENTRY;

        if (ptlrpc_server_allow_normal(svc, force) &&
            !list_empty(&svc->srv_request_queue) &&
            (list_empty(&svc->srv_request_hpq) ||
             svc->srv_hpreq_count >= svc->srv_hpreq_ratio)) {
                req = list_entry(svc->srv_request_queue.next,
                                 struct ptlrpc_request, rq_list);
                svc->srv_hpreq_count = 0;
        } else if (!list_empty(&svc->srv_request_hpq)) {
                req = list_entry(svc->srv_request_hpq.next,
                                 struct ptlrpc_request, rq_list);
                svc->srv_hpreq_count++;
        }
        RETURN(req);
}

static int ptlrpc_server_request_pending(struct ptlrpc_service *svc, int force)
{
        return ((ptlrpc_server_allow_normal(svc, force) &&
                 !list_empty(&svc->srv_request_queue)) ||
                !list_empty(&svc->srv_request_hpq));
}

/* Handle freshly incoming reqs, add to timed early reply list,
   pass on to regular request queue */
static int
ptlrpc_server_handle_req_in(struct ptlrpc_service *svc)
{
        struct ptlrpc_request *req;
        __u32                  deadline;
        int                    rc;
        ENTRY;

        LASSERT(svc);

        spin_lock(&svc->srv_lock);
        if (list_empty(&svc->srv_req_in_queue)) {
                spin_unlock(&svc->srv_lock);
                RETURN(0);
        }

        req = list_entry(svc->srv_req_in_queue.next,
                         struct ptlrpc_request, rq_list);
        list_del_init (&req->rq_list);
        /* Consider this still a "queued" request as far as stats are
           concerned */
        spin_unlock(&svc->srv_lock);

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

        /* Clear request swab mask; this is a new request */
        req->rq_req_swab_mask = 0;

        rc = lustre_unpack_msg(req->rq_reqmsg, req->rq_reqlen);
        if (rc != 0) {
                CERROR("error unpacking request: ptl %d from %s x"LPU64"\n",
                       svc->srv_req_portal, libcfs_id2str(req->rq_peer),
                       req->rq_xid);
                goto err_req;
        }

        rc = lustre_unpack_req_ptlrpc_body(req, MSG_PTLRPC_BODY_OFF);
        if (rc) {
                CERROR ("error unpacking ptlrpc body: ptl %d from %s x"
                        LPU64"\n", svc->srv_req_portal,
                        libcfs_id2str(req->rq_peer), req->rq_xid);
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
                req->rq_bulk_read = 1;
                break;
        }

        CDEBUG(D_NET, "got req "LPD64"\n", req->rq_xid);

        req->rq_export = class_conn2export(
                lustre_msg_get_handle(req->rq_reqmsg));
        if (req->rq_export) {
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
        rc = ptlrpc_hpreq_init(svc, req);
        if (rc)
                GOTO(err_req, rc);

        /* Move it over to the request processing queue */
        rc = ptlrpc_server_request_add(svc, req);
        if (rc)
                GOTO(err_req, rc);
        cfs_waitq_signal(&svc->srv_waitq);
        RETURN(1);

err_req:
        spin_lock(&svc->srv_lock);
        svc->srv_n_queued_reqs--;
        svc->srv_n_active_reqs++;
        spin_unlock(&svc->srv_lock);
        ptlrpc_server_finish_request(req);

        RETURN(1);
}

static int
ptlrpc_server_handle_request(struct ptlrpc_service *svc,
                             struct ptlrpc_thread *thread)
{
        struct obd_export     *export = NULL;
        struct ptlrpc_request *request;
        struct timeval         work_start;
        struct timeval         work_end;
        long                   timediff;
        int                    opc, rc;
        int                    fail_opc = 0;
        ENTRY;

        LASSERT(svc);

        spin_lock(&svc->srv_lock);
        if (unlikely(!ptlrpc_server_request_pending(svc, 0) ||
            (
#ifndef __KERNEL__
             /* !@%$# liblustre only has 1 thread */
             atomic_read(&svc->srv_n_difficult_replies) != 0 &&
#endif
             svc->srv_n_active_reqs >= (svc->srv_threads_running - 1)))) {
                 /* Don't handle regular requests in the last thread, in order               * re
                  * to handle difficult replies (which might block other threads)
                  * as well as handle any incoming reqs, early replies, etc.
                  * That means we always need at least 2 service threads. */
                spin_unlock(&svc->srv_lock);
                RETURN(0);
             }

        request = ptlrpc_server_request_get(svc, 0);
        if  (request == NULL) {
                spin_unlock(&svc->srv_lock);
                RETURN(0);
        }

        opc = lustre_msg_get_opc(request->rq_reqmsg);
        if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_NOTIMEOUT))
                fail_opc = OBD_FAIL_PTLRPC_HPREQ_NOTIMEOUT;
        else if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_TIMEOUT))
                fail_opc = OBD_FAIL_PTLRPC_HPREQ_TIMEOUT;

        if (unlikely(fail_opc)) {
                if (request->rq_export && request->rq_ops) {
                        spin_unlock(&svc->srv_lock);
                        OBD_FAIL_TIMEOUT(fail_opc, 4);
                        spin_lock(&svc->srv_lock);
                        request = ptlrpc_server_request_get(svc, 0);
                        if  (request == NULL) {
                                spin_unlock(&svc->srv_lock);
                                RETURN(0);
                        }
                        LASSERT(ptlrpc_server_request_pending(svc, 0));
                }
        }

        list_del_init(&request->rq_list);
        svc->srv_n_queued_reqs--;
        svc->srv_n_active_reqs++;
        if (request->rq_hp)
                svc->srv_n_hpreq++;

        /* The phase is changed under the lock here because we need to know
         * the request is under processing (see ptlrpc_hpreq_reorder()). */
        ptlrpc_rqphase_move(request, RQ_PHASE_INTERPRET);
        spin_unlock(&svc->srv_lock);

        ptlrpc_hpreq_fini(request);

        if(OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_DUMP_LOG))
                libcfs_debug_dumplog();

        do_gettimeofday(&work_start);
        timediff = cfs_timeval_sub(&work_start, &request->rq_arrival_time,NULL);
        if (likely(svc->srv_stats != NULL)) {
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQWAIT_CNTR,
                                    timediff);
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQQDEPTH_CNTR,
                                    svc->srv_n_queued_reqs);
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQACTIVE_CNTR,
                                    svc->srv_n_active_reqs);
                lprocfs_counter_add(svc->srv_stats, PTLRPC_TIMEOUT,
                                    at_get(&svc->srv_at_estimate));
        }

        rc = lu_context_init(&request->rq_session,
                             LCT_SESSION|LCT_REMEMBER|LCT_NOREF);
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
                export = class_export_rpc_get(request->rq_export);
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
                goto put_rpc_export;
        }

        CDEBUG(D_RPCTRACE, "Handling RPC pname:cluuid+ref:pid:xid:nid:opc "
               "%s:%s+%d:%d:x"LPU64":%s:%d\n", cfs_curproc_comm(),
               (request->rq_export ?
                (char *)request->rq_export->exp_client_uuid.uuid : "0"),
               (request->rq_export ?
                atomic_read(&request->rq_export->exp_refcount) : -99),
               lustre_msg_get_status(request->rq_reqmsg), request->rq_xid,
               libcfs_id2str(request->rq_peer),
               lustre_msg_get_opc(request->rq_reqmsg));

        OBD_FAIL_TIMEOUT_MS(OBD_FAIL_PTLRPC_PAUSE_REQ, obd_fail_val);

        rc = svc->srv_handler(request);

        ptlrpc_rqphase_move(request, RQ_PHASE_COMPLETE);

        CDEBUG(D_RPCTRACE, "Handled RPC pname:cluuid+ref:pid:xid:nid:opc "
               "%s:%s+%d:%d:x"LPU64":%s:%d\n", cfs_curproc_comm(),
               (request->rq_export ?
                (char *)request->rq_export->exp_client_uuid.uuid : "0"),
               (request->rq_export ?
                atomic_read(&request->rq_export->exp_refcount) : -99),
               lustre_msg_get_status(request->rq_reqmsg), request->rq_xid,
               libcfs_id2str(request->rq_peer),
               lustre_msg_get_opc(request->rq_reqmsg));

put_rpc_export:
        if (export != NULL)
                class_export_rpc_put(export);
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

        do_gettimeofday(&work_end);
        timediff = cfs_timeval_sub(&work_end, &work_start, NULL);
        CDEBUG(D_RPCTRACE, "request x"LPU64" opc %u from %s processed in "
               "%ldus (%ldus total) trans "LPU64" rc %d/%d\n",
               request->rq_xid, lustre_msg_get_opc(request->rq_reqmsg),
               libcfs_id2str(request->rq_peer), timediff,
               cfs_timeval_sub(&work_end, &request->rq_arrival_time, NULL),
               request->rq_repmsg ? lustre_msg_get_transno(request->rq_repmsg) :
               request->rq_transno, request->rq_status,
               request->rq_repmsg ? lustre_msg_get_status(request->rq_repmsg):
               -999);
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
        spin_lock(&svc->srv_lock);
        if (request->rq_hp)
                svc->srv_n_hpreq--;
        spin_unlock(&svc->srv_lock);
        ptlrpc_server_finish_request(request);

        RETURN(1);
}

/**
 * An internal function to process a single reply state object.
 */
static int
ptlrpc_handle_rs (struct ptlrpc_reply_state *rs)
{
        struct ptlrpc_service     *svc = rs->rs_service;
        struct obd_export         *exp;
        struct obd_device         *obd;
        int                        nlocks;
        int                        been_handled;
        ENTRY;

        exp = rs->rs_export;
        obd = exp->exp_obd;

        LASSERT (rs->rs_difficult);
        LASSERT (rs->rs_scheduled);
        LASSERT (list_empty(&rs->rs_list));

        spin_lock (&exp->exp_lock);
        /* Noop if removed already */
        list_del_init (&rs->rs_exp_list);
        spin_unlock (&exp->exp_lock);

        /* Avoid obd_uncommitted_replies_lock contention if we 100% sure that
         * rs has been removed from the list already */
        if (!list_empty_careful(&rs->rs_obd_list)) {
                spin_lock(&obd->obd_uncommitted_replies_lock);
                list_del_init(&rs->rs_obd_list);
                spin_unlock(&obd->obd_uncommitted_replies_lock);
        }

        spin_lock(&rs->rs_lock);

        been_handled = rs->rs_handled;
        rs->rs_handled = 1;

        nlocks = rs->rs_nlocks;                 /* atomic "steal", but */
        rs->rs_nlocks = 0;                      /* locks still on rs_locks! */

        if (nlocks == 0 && !been_handled) {
                /* If we see this, we should already have seen the warning
                 * in mds_steal_ack_locks()  */
                CWARN("All locks stolen from rs %p x"LPD64".t"LPD64
                      " o%d NID %s\n",
                      rs,
                      rs->rs_xid, rs->rs_transno,
                      lustre_msg_get_opc(rs->rs_msg),
                      libcfs_nid2str(exp->exp_connection->c_peer.nid));
        }

        if ((!been_handled && rs->rs_on_net) || nlocks > 0) {
                spin_unlock(&rs->rs_lock);

                if (!been_handled && rs->rs_on_net) {
                        LNetMDUnlink(rs->rs_md_h);
                        /* Ignore return code; we're racing with
                         * completion... */
                }

                while (nlocks-- > 0)
                        ldlm_lock_decref(&rs->rs_locks[nlocks],
                                         rs->rs_modes[nlocks]);

                spin_lock(&rs->rs_lock);
        }

        rs->rs_scheduled = 0;

        if (!rs->rs_on_net) {
                /* Off the net */
                spin_unlock(&rs->rs_lock);

                class_export_put (exp);
                rs->rs_export = NULL;
                ptlrpc_rs_decref (rs);
                atomic_dec (&svc->srv_outstanding_replies);
                if (atomic_dec_and_test(&svc->srv_n_difficult_replies) &&
                    svc->srv_is_stopping)
                        cfs_waitq_broadcast(&svc->srv_waitq);
                RETURN(1);
        }

        /* still on the net; callback will schedule */
        spin_unlock(&rs->rs_lock);
        RETURN(1);
}

#ifndef __KERNEL__

/**
 * Check whether given service has a reply available for processing
 * and process it.
 *
 * \param svc a ptlrpc service
 * \retval 0 no replies processes
 * \retval 1 one reply processed
 */
static int
ptlrpc_server_handle_reply(struct ptlrpc_service *svc)
{
        struct ptlrpc_reply_state *rs = NULL;
        ENTRY;

        spin_lock(&svc->srv_lock);
        if (!list_empty(&svc->srv_reply_queue)) {
                rs = list_entry(svc->srv_reply_queue.prev,
                                struct ptlrpc_reply_state,
                                rs_list);
                list_del_init(&rs->rs_list);
        }
        spin_unlock(&svc->srv_lock);
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
        struct list_head *tmp, *nxt;
        ENTRY;

        /* I'm relying on being single threaded, not to have to lock
         * ptlrpc_all_services etc */
        list_for_each_safe (tmp, nxt, &ptlrpc_all_services) {
                struct ptlrpc_service *svc =
                        list_entry (tmp, struct ptlrpc_service, srv_list);

                if (svc->srv_threads_running != 0)     /* I've recursed */
                        continue;

                /* service threads can block for bulk, so this limits us
                 * (arbitrarily) to recursing 1 stack frame per service.
                 * Note that the problem with recursion is that we have to
                 * unwind completely before our caller can resume. */

                svc->srv_threads_running++;

                do {
                        rc = ptlrpc_server_handle_req_in(svc);
                        rc |= ptlrpc_server_handle_reply(svc);
                        rc |= ptlrpc_at_check_timed(svc);
                        rc |= ptlrpc_server_handle_request(svc, NULL);
                        rc |= (ptlrpc_server_post_idle_rqbds(svc) > 0);
                        did_something |= rc;
                } while (rc);

                svc->srv_threads_running--;
        }

        RETURN(did_something);
}
#define ptlrpc_stop_all_threads(s) do {} while (0)

#else /* __KERNEL__ */

/* Don't use daemonize, it removes fs struct from new thread (bug 418) */
void ptlrpc_daemonize(char *name)
{
        struct fs_struct *fs = current->fs;

        atomic_inc(&fs->count);
        cfs_daemonize(name);
        exit_fs(cfs_current());
        current->fs = fs;
        ll_set_fs_pwd(current->fs, init_task.fs->pwdmnt, init_task.fs->pwd);
}

static void
ptlrpc_check_rqbd_pool(struct ptlrpc_service *svc)
{
        int avail = svc->srv_nrqbd_receiving;
        int low_water = test_req_buffer_pressure ? 0 :
                        svc->srv_nbuf_per_group/2;

        /* NB I'm not locking; just looking. */

        /* CAVEAT EMPTOR: We might be allocating buffers here because we've
         * allowed the request history to grow out of control.  We could put a
         * sanity check on that here and cull some history if we need the
         * space. */

        if (avail <= low_water)
                ptlrpc_grow_req_bufs(svc);

        if (svc->srv_stats)
                lprocfs_counter_add(svc->srv_stats, PTLRPC_REQBUF_AVAIL_CNTR,
                                    avail);
}

static int
ptlrpc_retry_rqbds(void *arg)
{
        struct ptlrpc_service *svc = (struct ptlrpc_service *)arg;

        svc->srv_rqbd_timeout = 0;
        return (-ETIMEDOUT);
}

static int ptlrpc_main(void *arg)
{
        struct ptlrpc_svc_data *data = (struct ptlrpc_svc_data *)arg;
        struct ptlrpc_service  *svc = data->svc;
        struct ptlrpc_thread   *thread = data->thread;
        struct obd_device      *dev = data->dev;
        struct ptlrpc_reply_state *rs;
#ifdef WITH_GROUP_INFO
        struct group_info *ginfo = NULL;
#endif
        struct lu_env env;
        int counter = 0, rc = 0;
        ENTRY;

        ptlrpc_daemonize(data->name);

#if defined(HAVE_NODE_TO_CPUMASK) && defined(CONFIG_NUMA)
        /* we need to do this before any per-thread allocation is done so that
         * we get the per-thread allocations on local node.  bug 7342 */
        if (svc->srv_cpu_affinity) {
                int cpu, num_cpu;

                for (cpu = 0, num_cpu = 0; cpu < num_possible_cpus(); cpu++) {
                        if (!cpu_online(cpu))
                                continue;
                        if (num_cpu == thread->t_id % num_online_cpus())
                                break;
                        num_cpu++;
                }
                set_cpus_allowed(cfs_current(), node_to_cpumask(cpu_to_node(cpu)));
        }
#endif

#ifdef WITH_GROUP_INFO
        ginfo = groups_alloc(0);
        if (!ginfo) {
                rc = -ENOMEM;
                goto out;
        }

        set_current_groups(ginfo);
        put_group_info(ginfo);
#endif

        if (svc->srv_init != NULL) {
                rc = svc->srv_init(thread);
                if (rc)
                        goto out;
        }

        rc = lu_context_init(&env.le_ctx,
                             svc->srv_ctx_tags|LCT_REMEMBER|LCT_NOREF);
        if (rc)
                goto out_srv_fini;

        thread->t_env = &env;
        env.le_ctx.lc_thread = thread;
        env.le_ctx.lc_cookie = 0x6;

        /* Alloc reply state structure for this one */
        OBD_ALLOC_GFP(rs, svc->srv_max_reply_size, CFS_ALLOC_STD);
        if (!rs) {
                rc = -ENOMEM;
                goto out_srv_fini;
        }

        /* Record that the thread is running */
        thread->t_flags = SVC_RUNNING;
        /*
         * wake up our creator. Note: @data is invalid after this point,
         * because it's allocated on ptlrpc_start_thread() stack.
         */
        cfs_waitq_signal(&thread->t_ctl_waitq);

        thread->t_watchdog = lc_watchdog_add(max_t(int, obd_timeout, AT_OFF ? 0 :
                                                   at_get(&svc->srv_at_estimate))
                                             *  svc->srv_watchdog_factor,
                                             NULL, NULL);

        spin_lock(&svc->srv_lock);
        svc->srv_threads_running++;
        list_add(&rs->rs_list, &svc->srv_free_rs_list);
        spin_unlock(&svc->srv_lock);
        cfs_waitq_signal(&svc->srv_free_rs_waitq);

        CDEBUG(D_NET, "service thread %d (#%d) started\n", thread->t_id,
               svc->srv_threads_running);

        /* XXX maintain a list of all managed devices: insert here */

        while ((thread->t_flags & SVC_STOPPING) == 0) {
                /* Don't exit while there are replies to be handled */
                struct l_wait_info lwi = LWI_TIMEOUT(svc->srv_rqbd_timeout,
                                                     ptlrpc_retry_rqbds, svc);

                lc_watchdog_disable(thread->t_watchdog);

                cond_resched();

                l_wait_event_exclusive (svc->srv_waitq,
                              ((thread->t_flags & SVC_STOPPING) != 0) ||
                              (!list_empty(&svc->srv_idle_rqbds) &&
                               svc->srv_rqbd_timeout == 0) ||
                              !list_empty(&svc->srv_req_in_queue) ||
                              (ptlrpc_server_request_pending(svc, 0) &&
                               (svc->srv_n_active_reqs <
                                (svc->srv_threads_running - 1))) ||
                              svc->srv_at_check,
                              &lwi);

                lc_watchdog_touch_ms(thread->t_watchdog, max_t(int, obd_timeout,
                                     AT_OFF ? 0 :
                                     at_get(&svc->srv_at_estimate)) *
                                     svc->srv_watchdog_factor);

                ptlrpc_check_rqbd_pool(svc);

                if ((svc->srv_threads_started < svc->srv_threads_max) &&
                    (svc->srv_n_active_reqs >= (svc->srv_threads_started - 1))){
                        /* Ignore return code - we tried... */
                        ptlrpc_start_thread(dev, svc);
                }

                if (!list_empty(&svc->srv_req_in_queue)) {
                        /* Process all incoming reqs before handling any */
                        ptlrpc_server_handle_req_in(svc);
                        /* but limit ourselves in case of flood */
                        if (counter++ < 1000)
                                continue;
                        counter = 0;
                }

                if (svc->srv_at_check)
                        ptlrpc_at_check_timed(svc);

                /* don't handle requests in the last thread */
                if (ptlrpc_server_request_pending(svc, 0) &&
                    (svc->srv_n_active_reqs < (svc->srv_threads_running - 1))) {
                        lu_context_enter(&env.le_ctx);
                        ptlrpc_server_handle_request(svc, thread);
                        lu_context_exit(&env.le_ctx);
                }

                if (!list_empty(&svc->srv_idle_rqbds) &&
                    ptlrpc_server_post_idle_rqbds(svc) < 0) {
                        /* I just failed to repost request buffers.  Wait
                         * for a timeout (unless something else happens)
                         * before I try again */
                        svc->srv_rqbd_timeout = cfs_time_seconds(1)/10;
                        CDEBUG(D_RPCTRACE,"Posted buffers: %d\n",
                               svc->srv_nrqbd_receiving);
                }
        }

        lc_watchdog_delete(thread->t_watchdog);
        thread->t_watchdog = NULL;

out_srv_fini:
        /*
         * deconstruct service specific state created by ptlrpc_start_thread()
         */
        if (svc->srv_done != NULL)
                svc->srv_done(thread);

        lu_context_fini(&env.le_ctx);
out:
        CDEBUG(D_NET, "service thread %d exiting: rc %d\n", thread->t_id, rc);

        spin_lock(&svc->srv_lock);
        svc->srv_threads_running--; /* must know immediately */
        thread->t_id = rc;
        thread->t_flags = SVC_STOPPED;

        cfs_waitq_signal(&thread->t_ctl_waitq);
        spin_unlock(&svc->srv_lock);

        return rc;
}

struct ptlrpc_hr_args {
        int                       thread_index;
        int                       cpu_index;
        struct ptlrpc_hr_service *hrs;
};

static int hrt_dont_sleep(struct ptlrpc_hr_thread *t,
                          struct list_head *replies)
{
        int result;

        spin_lock(&t->hrt_lock);
        list_splice_init(&t->hrt_queue, replies);
        result = test_bit(HRT_STOPPING, &t->hrt_flags) ||
                !list_empty(replies);
        spin_unlock(&t->hrt_lock);
        return result;
}

static int ptlrpc_hr_main(void *arg)
{
        struct ptlrpc_hr_args * hr_args = arg;
        struct ptlrpc_hr_service *hr = hr_args->hrs;
        struct ptlrpc_hr_thread *t = &hr->hr_threads[hr_args->thread_index];
        char threadname[20];
        CFS_LIST_HEAD(replies);

        snprintf(threadname, sizeof(threadname),
                 "ptlrpc_hr_%d", hr_args->thread_index);

        ptlrpc_daemonize(threadname);
#if defined(HAVE_NODE_TO_CPUMASK)
        set_cpus_allowed(cfs_current(),
                         node_to_cpumask(cpu_to_node(hr_args->cpu_index)));
#endif
        set_bit(HRT_RUNNING, &t->hrt_flags);
        cfs_waitq_signal(&t->hrt_wait);

        while (!test_bit(HRT_STOPPING, &t->hrt_flags)) {

                cfs_wait_event(t->hrt_wait, hrt_dont_sleep(t, &replies));
                while (!list_empty(&replies)) {
                        struct ptlrpc_reply_state *rs;

                        rs = list_entry(replies.prev,
                                        struct ptlrpc_reply_state,
                                        rs_list);
                        list_del_init(&rs->rs_list);
                        ptlrpc_handle_rs(rs);
                }
        }

        clear_bit(HRT_RUNNING, &t->hrt_flags);
        complete(&t->hrt_completion);

        return 0;
}

static int ptlrpc_start_hr_thread(struct ptlrpc_hr_service *hr, int n, int cpu)
{
        struct ptlrpc_hr_thread *t = &hr->hr_threads[n];
        struct ptlrpc_hr_args args;
        int rc;
        ENTRY;

        args.thread_index = n;
        args.cpu_index = cpu;
        args.hrs = hr;

        rc = cfs_kernel_thread(ptlrpc_hr_main, (void*)&args,
                               CLONE_VM|CLONE_FILES);
        if (rc < 0) {
                complete(&t->hrt_completion);
                GOTO(out, rc);
        }
        cfs_wait_event(t->hrt_wait, test_bit(HRT_RUNNING, &t->hrt_flags));
        RETURN(0);
 out:
        return rc;
}

static void ptlrpc_stop_hr_thread(struct ptlrpc_hr_thread *t)
{
        ENTRY;

        set_bit(HRT_STOPPING, &t->hrt_flags);
        cfs_waitq_signal(&t->hrt_wait);
        wait_for_completion(&t->hrt_completion);

        EXIT;
}

static void ptlrpc_stop_hr_threads(struct ptlrpc_hr_service *hrs)
{
        int n;
        ENTRY;

        for (n = 0; n < hrs->hr_n_threads; n++)
                ptlrpc_stop_hr_thread(&hrs->hr_threads[n]);

        EXIT;
}

static int ptlrpc_start_hr_threads(struct ptlrpc_hr_service *hr)
{
        int rc = -ENOMEM;
        int n, cpu, threads_started = 0;
        ENTRY;

        LASSERT(hr != NULL);
        LASSERT(hr->hr_n_threads > 0);

        for (n = 0, cpu = 0; n < hr->hr_n_threads; n++) {
#if defined(HAVE_NODE_TO_CPUMASK)
                while(!cpu_online(cpu)) {
                        cpu++;
                        if (cpu >= num_possible_cpus())
                                cpu = 0;
                }
#endif
                rc = ptlrpc_start_hr_thread(hr, n, cpu);
                if (rc != 0)
                        break;
                threads_started++;
                cpu++;
        }
        if (threads_started == 0) {
                CERROR("No reply handling threads started\n");
                RETURN(-ESRCH);
        }
        if (threads_started < hr->hr_n_threads) {
                CWARN("Started only %d reply handling threads from %d\n",
                      threads_started, hr->hr_n_threads);
                hr->hr_n_threads = threads_started;
        }
        RETURN(0);
}

static void ptlrpc_stop_thread(struct ptlrpc_service *svc,
                               struct ptlrpc_thread *thread)
{
        struct l_wait_info lwi = { 0 };
        ENTRY;

        CDEBUG(D_RPCTRACE, "Stopping thread %p\n", thread);
        spin_lock(&svc->srv_lock);
        thread->t_flags = SVC_STOPPING;
        spin_unlock(&svc->srv_lock);

        cfs_waitq_broadcast(&svc->srv_waitq);
        l_wait_event(thread->t_ctl_waitq, (thread->t_flags & SVC_STOPPED),
                     &lwi);

        spin_lock(&svc->srv_lock);
        list_del(&thread->t_link);
        spin_unlock(&svc->srv_lock);

        OBD_FREE_PTR(thread);
        EXIT;
}

void ptlrpc_stop_all_threads(struct ptlrpc_service *svc)
{
        struct ptlrpc_thread *thread;
        ENTRY;

        spin_lock(&svc->srv_lock);
        while (!list_empty(&svc->srv_threads)) {
                thread = list_entry(svc->srv_threads.next,
                                    struct ptlrpc_thread, t_link);

                spin_unlock(&svc->srv_lock);
                ptlrpc_stop_thread(svc, thread);
                spin_lock(&svc->srv_lock);
        }

        spin_unlock(&svc->srv_lock);
        EXIT;
}

int ptlrpc_start_threads(struct obd_device *dev, struct ptlrpc_service *svc)
{
        int i, rc = 0;
        ENTRY;

        /* We require 2 threads min - see note in
           ptlrpc_server_handle_request */
        LASSERT(svc->srv_threads_min >= 2);
        for (i = 0; i < svc->srv_threads_min; i++) {
                rc = ptlrpc_start_thread(dev, svc);
                /* We have enough threads, don't start more.  b=15759 */
                if (rc == -EMFILE)
                        break;
                if (rc) {
                        CERROR("cannot start %s thread #%d: rc %d\n",
                               svc->srv_thread_name, i, rc);
                        ptlrpc_stop_all_threads(svc);
                }
        }
        RETURN(rc);
}

int ptlrpc_start_thread(struct obd_device *dev, struct ptlrpc_service *svc)
{
        struct l_wait_info lwi = { 0 };
        struct ptlrpc_svc_data d;
        struct ptlrpc_thread *thread;
        char name[32];
        int id, rc;
        ENTRY;

        CDEBUG(D_RPCTRACE, "%s started %d min %d max %d running %d\n",
               svc->srv_name, svc->srv_threads_started, svc->srv_threads_min,
               svc->srv_threads_max, svc->srv_threads_running);
        if (unlikely(svc->srv_threads_started >= svc->srv_threads_max) ||
            (OBD_FAIL_CHECK(OBD_FAIL_TGT_TOOMANY_THREADS) &&
             svc->srv_threads_started == svc->srv_threads_min - 1))
                RETURN(-EMFILE);

        OBD_ALLOC_PTR(thread);
        if (thread == NULL)
                RETURN(-ENOMEM);
        cfs_waitq_init(&thread->t_ctl_waitq);

        spin_lock(&svc->srv_lock);
        if (svc->srv_threads_started >= svc->srv_threads_max) {
                spin_unlock(&svc->srv_lock);
                OBD_FREE_PTR(thread);
                RETURN(-EMFILE);
        }
        list_add(&thread->t_link, &svc->srv_threads);
        id = svc->srv_threads_started++;
        spin_unlock(&svc->srv_lock);

        thread->t_id = id;
        sprintf(name, "%s_%02d", svc->srv_thread_name, id);
        d.dev = dev;
        d.svc = svc;
        d.name = name;
        d.thread = thread;

        CDEBUG(D_RPCTRACE, "starting thread '%s'\n", name);

          /* CLONE_VM and CLONE_FILES just avoid a needless copy, because we
         * just drop the VM and FILES in ptlrpc_daemonize() right away.
         */
        rc = cfs_kernel_thread(ptlrpc_main, &d, CLONE_VM | CLONE_FILES);
        if (rc < 0) {
                CERROR("cannot start thread '%s': rc %d\n", name, rc);

                spin_lock(&svc->srv_lock);
                list_del(&thread->t_link);
                --svc->srv_threads_started;
                spin_unlock(&svc->srv_lock);

                OBD_FREE(thread, sizeof(*thread));
                RETURN(rc);
        }
        l_wait_event(thread->t_ctl_waitq,
                     thread->t_flags & (SVC_RUNNING | SVC_STOPPED), &lwi);

        rc = (thread->t_flags & SVC_STOPPED) ? thread->t_id : 0;
        RETURN(rc);
}


int ptlrpc_hr_init(void)
{
        int i;
        int n_cpus = num_online_cpus();
        struct ptlrpc_hr_service *hr;
        int size;
        ENTRY;

        LASSERT(ptlrpc_hr == NULL);

        size = offsetof(struct ptlrpc_hr_service, hr_threads[n_cpus]);
        OBD_ALLOC(hr, size);
        if (hr == NULL)
                RETURN(-ENOMEM);
        for (i = 0; i < n_cpus; i++) {
                struct ptlrpc_hr_thread *t = &hr->hr_threads[i];

                spin_lock_init(&t->hrt_lock);
                cfs_waitq_init(&t->hrt_wait);
                CFS_INIT_LIST_HEAD(&t->hrt_queue);
                init_completion(&t->hrt_completion);
        }
        hr->hr_n_threads = n_cpus;
        hr->hr_size = size;
        ptlrpc_hr = hr;

        RETURN(ptlrpc_start_hr_threads(hr));
}

void ptlrpc_hr_fini(void)
{
        if (ptlrpc_hr != NULL) {
                ptlrpc_stop_hr_threads(ptlrpc_hr);
                OBD_FREE(ptlrpc_hr, ptlrpc_hr->hr_size);
                ptlrpc_hr = NULL;
        }
}

#endif /* __KERNEL__ */

/**
 * Wait until all already scheduled replies are processed.
 */
static void ptlrpc_wait_replies(struct ptlrpc_service *svc)
{
        while (1) {
                int rc;
                struct l_wait_info lwi = LWI_TIMEOUT(cfs_time_seconds(10),
                                                     NULL, NULL);
                rc = l_wait_event(svc->srv_waitq,
                                  atomic_read(&svc->srv_n_difficult_replies) == 0,
                                  &lwi);
                if (rc == 0)
                        break;
                CWARN("Unexpectedly long timeout %p\n", svc);
        }
}

int ptlrpc_unregister_service(struct ptlrpc_service *service)
{
        int                   rc;
        struct l_wait_info    lwi;
        struct list_head     *tmp;
        struct ptlrpc_reply_state *rs, *t;
        ENTRY;

        service->srv_is_stopping = 1;
        cfs_timer_disarm(&service->srv_at_timer);

        ptlrpc_stop_all_threads(service);
        LASSERT(list_empty(&service->srv_threads));

        spin_lock (&ptlrpc_all_services_lock);
        list_del_init (&service->srv_list);
        spin_unlock (&ptlrpc_all_services_lock);

        ptlrpc_lprocfs_unregister_service(service);

        /* All history will be culled when the next request buffer is
         * freed */
        service->srv_max_history_rqbds = 0;

        CDEBUG(D_NET, "%s: tearing down\n", service->srv_name);

        rc = LNetClearLazyPortal(service->srv_req_portal);
        LASSERT (rc == 0);

        /* Unlink all the request buffers.  This forces a 'final' event with
         * its 'unlink' flag set for each posted rqbd */
        list_for_each(tmp, &service->srv_active_rqbds) {
                struct ptlrpc_request_buffer_desc *rqbd =
                        list_entry(tmp, struct ptlrpc_request_buffer_desc,
                                   rqbd_list);

                rc = LNetMDUnlink(rqbd->rqbd_md_h);
                LASSERT (rc == 0 || rc == -ENOENT);
        }

        /* Wait for the network to release any buffers it's currently
         * filling */
        for (;;) {
                spin_lock(&service->srv_lock);
                rc = service->srv_nrqbd_receiving;
                spin_unlock(&service->srv_lock);

                if (rc == 0)
                        break;

                /* Network access will complete in finite time but the HUGE
                 * timeout lets us CWARN for visibility of sluggish NALs */
                lwi = LWI_TIMEOUT_INTERVAL(cfs_time_seconds(LONG_UNLINK),
                                           cfs_time_seconds(1), NULL, NULL);
                rc = l_wait_event(service->srv_waitq,
                                  service->srv_nrqbd_receiving == 0,
                                  &lwi);
                if (rc == -ETIMEDOUT)
                        CWARN("Service %s waiting for request buffers\n",
                              service->srv_name);
        }

        /* schedule all outstanding replies to terminate them */
        spin_lock(&service->srv_lock);
        while (!list_empty(&service->srv_active_replies)) {
                struct ptlrpc_reply_state *rs =
                        list_entry(service->srv_active_replies.next,
                                   struct ptlrpc_reply_state, rs_list);
                spin_lock(&rs->rs_lock);
                ptlrpc_schedule_difficult_reply(rs);
                spin_unlock(&rs->rs_lock);
        }
        spin_unlock(&service->srv_lock);

        /* purge the request queue.  NB No new replies (rqbds all unlinked)
         * and no service threads, so I'm the only thread noodling the
         * request queue now */
        while (!list_empty(&service->srv_req_in_queue)) {
                struct ptlrpc_request *req =
                        list_entry(service->srv_req_in_queue.next,
                                   struct ptlrpc_request,
                                   rq_list);

                list_del(&req->rq_list);
                service->srv_n_queued_reqs--;
                service->srv_n_active_reqs++;
                ptlrpc_server_finish_request(req);
        }
        while (ptlrpc_server_request_pending(service, 1)) {
                struct ptlrpc_request *req;

                req = ptlrpc_server_request_get(service, 1);
                list_del(&req->rq_list);
                service->srv_n_queued_reqs--;
                service->srv_n_active_reqs++;
                ptlrpc_hpreq_fini(req);
                ptlrpc_server_finish_request(req);
        }
        LASSERT(service->srv_n_queued_reqs == 0);
        LASSERT(service->srv_n_active_reqs == 0);
        LASSERT(service->srv_n_history_rqbds == 0);
        LASSERT(list_empty(&service->srv_active_rqbds));

        /* Now free all the request buffers since nothing references them
         * any more... */
        while (!list_empty(&service->srv_idle_rqbds)) {
                struct ptlrpc_request_buffer_desc *rqbd =
                        list_entry(service->srv_idle_rqbds.next,
                                   struct ptlrpc_request_buffer_desc,
                                   rqbd_list);

                ptlrpc_free_rqbd(rqbd);
        }

        ptlrpc_wait_replies(service);

        list_for_each_entry_safe(rs, t, &service->srv_free_rs_list, rs_list) {
                list_del(&rs->rs_list);
                OBD_FREE(rs, service->srv_max_reply_size);
        }

        /* In case somebody rearmed this in the meantime */
        cfs_timer_disarm(&service->srv_at_timer);

        OBD_FREE_PTR(service);
        RETURN(0);
}

/* Returns 0 if the service is healthy.
 *
 * Right now, it just checks to make sure that requests aren't languishing
 * in the queue.  We'll use this health check to govern whether a node needs
 * to be shot, so it's intentionally non-aggressive. */
int ptlrpc_service_health_check(struct ptlrpc_service *svc)
{
        struct ptlrpc_request *request;
        struct timeval         right_now;
        long                   timediff;

        if (svc == NULL)
                return 0;

        do_gettimeofday(&right_now);

        spin_lock(&svc->srv_lock);
        if (!ptlrpc_server_request_pending(svc, 1)) {
                spin_unlock(&svc->srv_lock);
                return 0;
        }

        /* How long has the next entry been waiting? */
        if (list_empty(&svc->srv_request_queue))
                request = list_entry(svc->srv_request_hpq.next,
                                     struct ptlrpc_request, rq_list);
        else
                request = list_entry(svc->srv_request_queue.next,
                                     struct ptlrpc_request, rq_list);
        timediff = cfs_timeval_sub(&right_now, &request->rq_arrival_time, NULL);
        spin_unlock(&svc->srv_lock);

        if ((timediff / ONE_MILLION) > (AT_OFF ? obd_timeout * 3/2 :
                                        at_max)) {
                CERROR("%s: unhealthy - request has been waiting %lds\n",
                       svc->srv_name, timediff / ONE_MILLION);
                return (-1);
        }

        return 0;
}
