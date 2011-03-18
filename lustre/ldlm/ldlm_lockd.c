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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ldlm/ldlm_lockd.c
 *
 * Author: Peter Braam <braam@clusterfs.com>
 * Author: Phil Schwan <phil@clusterfs.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LDLM

#ifdef __KERNEL__
# include <libcfs/libcfs.h>
#else
# include <liblustre.h>
#endif

#include <lustre_dlm.h>
#include <obd_class.h>
#include <libcfs/list.h>
#include "ldlm_internal.h"

#ifdef __KERNEL__
static int ldlm_num_threads;
CFS_MODULE_PARM(ldlm_num_threads, "i", int, 0444,
                "number of DLM service threads to start");
#endif

extern cfs_mem_cache_t *ldlm_resource_slab;
extern cfs_mem_cache_t *ldlm_lock_slab;
static cfs_semaphore_t  ldlm_ref_sem;
static int ldlm_refcount;

/* LDLM state */

static struct ldlm_state *ldlm_state;

static void ldlm_exp_stats_inc(struct obd_export *exp, ldlm_cmd_t ldlm_cmd)
{
        if (exp == NULL ||
            exp->exp_nid_stats == NULL ||
            exp->exp_nid_stats->nid_ldlm_stats == NULL)
                return;

        lprocfs_counter_incr(exp->exp_nid_stats->nid_ldlm_stats,
                             ldlm_cmd - LDLM_FIRST_OPC);
}

inline cfs_time_t round_timeout(cfs_time_t timeout)
{
        return cfs_time_seconds((int)cfs_duration_sec(cfs_time_sub(timeout, 0)) + 1);
}

/* timeout for initial callback (AST) reply (bz10399) */
static inline unsigned int ldlm_get_rq_timeout(void)
{
        /* Non-AT value */
        unsigned int timeout = min(ldlm_timeout, obd_timeout / 3);

        return timeout < 1 ? 1 : timeout;
}

#ifdef __KERNEL__

#define LDLM_WTIMER_HASH_BITS           7
#define LDLM_WTIMER_HASH_SIZE          (1U << LDLM_WTIMER_HASH_BITS)
#define LDLM_WTIMER_HASH_MASK          (LDLM_WTIMER_HASH_SIZE - 1U)

typedef struct ldlm_wait_timer {
        cfs_spinlock_t            lwt_lock;
        cfs_timer_t               lwt_timer;
        struct list_head          lwt_waiting_locks;
} ldlm_wait_timer_t;

static struct ldlm_wait_data {
        /** serialize */
        cfs_spinlock_t            lwd_lock;
        /** where expired lock hanlder waits on */
        cfs_waitq_t               lwd_waitq;
        /** list of expired locks */
        cfs_list_t                lwd_expired_locks;
        /** expired lock handler status */
        int                       lwd_thread_state;
        int                       lwd_dump;
        ldlm_wait_timer_t       **lwd_wtimer;
} ldlm_waitd;

static ldlm_wait_timer_t *
ldlm_lock_to_wtimer(struct ldlm_lock *lock)
{
        /* on server-side resource of lock doesn't change */
        struct ldlm_res_id *name = &lock->l_resource->lr_name;
        __u32               hash = 0;
        int                 i;

        for (i = 0; i < RES_NAME_SIZE; i++)
                hash += name->name[i];

        return ldlm_waitd.lwd_wtimer[hash & LDLM_WTIMER_HASH_MASK];
}

#endif /* __KERNEL__ */

#define ELT_STOPPED   0
#define ELT_READY     1
#define ELT_TERMINATE 2

struct ldlm_bl_pool {
        cfs_spinlock_t          blp_lock;

        /*
         * blp_prio_list is used for callbacks that should be handled
         * as a priority. It is used for LDLM_FL_DISCARD_DATA requests.
         * see bug 13843
         */
        cfs_list_t              blp_prio_list;

        /*
         * blp_list is used for all other callbacks which are likely
         * to take longer to process.
         */
        cfs_list_t              blp_list;

        cfs_waitq_t             blp_waitq;
        cfs_completion_t        blp_comp;
        cfs_atomic_t            blp_num_threads;
        cfs_atomic_t            blp_busy_threads;
        int                     blp_min_threads;
        int                     blp_max_threads;
};

struct ldlm_bl_work_item {
        cfs_list_t              blwi_entry;
        struct ldlm_namespace  *blwi_ns;
        struct ldlm_lock_desc   blwi_ld;
        struct ldlm_lock       *blwi_lock;
        cfs_list_t              blwi_head;
        int                     blwi_count;
        cfs_completion_t        blwi_comp;
        int                     blwi_mode;
        int                     blwi_mem_pressure;
};

#ifdef __KERNEL__

static inline int have_expired_locks(void)
{
        int need_to_run;

        ENTRY;
        cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
        need_to_run = !cfs_list_empty(&ldlm_waitd.lwd_expired_locks);
        cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);

        RETURN(need_to_run);
}

static int expired_lock_main(void *arg)
{
        cfs_list_t *expired = &ldlm_waitd.lwd_expired_locks;
        struct l_wait_info lwi = { 0 };
        int do_dump;

        ENTRY;
        cfs_daemonize("ldlm_elt");

        ldlm_waitd.lwd_thread_state = ELT_READY;
        cfs_waitq_signal(&ldlm_waitd.lwd_waitq);

        while (1) {
                l_wait_event(ldlm_waitd.lwd_waitq,
                             have_expired_locks() ||
                             ldlm_waitd.lwd_thread_state == ELT_TERMINATE,
                             &lwi);

                cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
                if (ldlm_waitd.lwd_dump) {
                        cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);

                        /* from waiting_locks_callback, but not in timer */
                        libcfs_debug_dumplog();
                        libcfs_run_lbug_upcall(__FILE__,
                                                "waiting_locks_callback",
                                                ldlm_waitd.lwd_dump);

                        cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
                        ldlm_waitd.lwd_dump = 0;
                }

                do_dump = 0;

                while (!cfs_list_empty(expired)) {
                        struct obd_export *export;
                        struct ldlm_lock *lock;

                        lock = cfs_list_entry(expired->next, struct ldlm_lock,
                                          l_pending_chain);
                        if ((void *)lock < LP_POISON + CFS_PAGE_SIZE &&
                            (void *)lock >= LP_POISON) {
                                cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);
                                CERROR("free lock on elt list %p\n", lock);
                                LBUG();
                        }
                        cfs_list_del_init(&lock->l_pending_chain);
                        if ((void *)lock->l_export < LP_POISON + CFS_PAGE_SIZE &&
                            (void *)lock->l_export >= LP_POISON) {
                                CERROR("lock with free export on elt list %p\n",
                                       lock->l_export);
                                lock->l_export = NULL;
                                LDLM_ERROR(lock, "free export");
                                /* release extra ref grabbed by
                                 * ldlm_add_waiting_lock() or
                                 * ldlm_failed_ast() */
                                LDLM_LOCK_RELEASE(lock);
                                continue;
                        }
                        export = class_export_lock_get(lock->l_export, lock);
                        cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);

                        do_dump++;
                        class_fail_export(export);
                        class_export_lock_put(export, lock);

                        /* release extra ref grabbed by ldlm_add_waiting_lock()
                         * or ldlm_failed_ast() */
                        LDLM_LOCK_RELEASE(lock);

                        cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
                }
                cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);

                if (do_dump && obd_dump_on_eviction) {
                        CERROR("dump the log upon eviction\n");
                        libcfs_debug_dumplog();
                }

                if (ldlm_waitd.lwd_thread_state == ELT_TERMINATE)
                        break;
        }

        ldlm_waitd.lwd_thread_state = ELT_STOPPED;
        cfs_waitq_signal(&ldlm_waitd.lwd_waitq);
        RETURN(0);
}

static int __ldlm_add_waiting_lock(ldlm_wait_timer_t *wtimer,
                                   struct ldlm_lock *lock, int seconds);
static int __ldlm_refresh_waiting_lock(ldlm_wait_timer_t *wtimer,
                                       struct ldlm_lock *lock, int timeout);
/**
 * Check if there is a request in the export request list
 * which prevents the lock canceling.
 */
static int ldlm_lock_busy(struct ldlm_lock *lock)
{
        struct ptlrpc_request *req;
        int match = 0;
        ENTRY;

        if (lock->l_export == NULL)
                return 0;

        cfs_spin_lock_bh(&lock->l_export->exp_rpc_lock);
        cfs_list_for_each_entry(req, &lock->l_export->exp_queued_rpc,
                                rq_exp_list) {
                if (req->rq_ops->hpreq_lock_match) {
                        match = req->rq_ops->hpreq_lock_match(req, lock);
                        if (match)
                                break;
                }
        }
        cfs_spin_unlock_bh(&lock->l_export->exp_rpc_lock);
        RETURN(match);
}

/* This is called from within a timer interrupt and cannot schedule */
static void waiting_locks_callback(unsigned long data)
{
        ldlm_wait_timer_t *wtimer = (ldlm_wait_timer_t *)data;
        struct obd_export *exp;
        struct ldlm_lock  *lock;
        int expired = 0;

        cfs_spin_lock_bh(&wtimer->lwt_lock);
        while (!cfs_list_empty(&wtimer->lwt_waiting_locks)) {
                lock = cfs_list_entry(wtimer->lwt_waiting_locks.next,
                                      struct ldlm_lock, l_pending_chain);
                if (cfs_time_after(lock->l_callback_timeout,
                                   cfs_time_current()) ||
                    (lock->l_req_mode == LCK_GROUP))
                        break;

                exp = lock->l_export;
                if (ptlrpc_check_suspend()) {
                        /* there is a case when we talk to one mds, holding
                         * lock from another mds. this way we easily can get
                         * here, if second mds is being recovered. so, we
                         * suspend timeouts. bug 6019 */

                        LDLM_ERROR(lock, "recharge timeout: %s@%s nid %s ",
                                   exp->exp_client_uuid.uuid,
                                   exp->exp_connection->c_remote_uuid.uuid,
                                   libcfs_nid2str(export_to_nid(exp)));

                        cfs_list_del_init(&lock->l_pending_chain);
                        __ldlm_add_waiting_lock(wtimer, lock,
                                                ldlm_get_enq_timeout(lock));
                        continue;
                }

                /* if timeout overlaps the activation time of suspended timeouts
                 * then extend it to give a chance for client to reconnect */
                if (cfs_time_before(cfs_time_sub(lock->l_callback_timeout,
                                                 cfs_time_seconds(obd_timeout)/2),
                                    ptlrpc_suspend_wakeup_time())) {
                        LDLM_ERROR(lock, "extend timeout due to recovery: %s@%s nid %s ",
                                   exp->exp_client_uuid.uuid,
                                   exp->exp_connection->c_remote_uuid.uuid,
                                   libcfs_nid2str(export_to_nid(exp)));

                        cfs_list_del_init(&lock->l_pending_chain);
                        __ldlm_add_waiting_lock(wtimer, lock,
                                                ldlm_get_enq_timeout(lock));
                        continue;
                }

                /* Check if we need to prolong timeout */
                if (!OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_TIMEOUT) &&
                    ldlm_lock_busy(lock)) {
                        int cont = lock->l_pending_chain.next !=
                                   &wtimer->lwt_waiting_locks;

                        __ldlm_refresh_waiting_lock(wtimer, lock,
                                                    ldlm_get_enq_timeout(lock));
                        if (!cont)
                                break;
                        continue;
                }
                ldlm_lock_to_ns(lock)->ns_timeouts++;
                LDLM_ERROR(lock, "lock callback timer expired after %lds: "
                           "evicting client at %s ",
                           cfs_time_current_sec()- lock->l_last_activity,
                           libcfs_nid2str(export_to_nid(exp)));

                /* no needs to take an extra ref on the lock since it was in
                 * the waiting_locks_list and ldlm_add_waiting_lock()
                 * already grabbed a ref */
                if (!expired) {
                        cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
                        expired = 1;
                }
                cfs_list_move_tail(&lock->l_pending_chain,
                                   &ldlm_waitd.lwd_expired_locks);
        }

        if (expired) {
                if (obd_dump_on_timeout)
                        ldlm_waitd.lwd_dump = __LINE__;
                cfs_waitq_signal(&ldlm_waitd.lwd_waitq);
                cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);
        }

        /*
         * Make sure the timer will fire again if we have any locks
         * left.
         */
        if (!cfs_list_empty(&wtimer->lwt_waiting_locks)) {
                cfs_time_t timeout_rounded;

                lock = cfs_list_entry(wtimer->lwt_waiting_locks.next,
                                      struct ldlm_lock, l_pending_chain);
                timeout_rounded =
                        (cfs_time_t)round_timeout(lock->l_callback_timeout);
                cfs_timer_arm(&wtimer->lwt_timer, timeout_rounded);
        }
        cfs_spin_unlock_bh(&wtimer->lwt_lock);
}

/*
 * Indicate that we're waiting for a client to call us back cancelling a given
 * lock.  We add it to the pending-callback chain, and schedule the lock-timeout
 * timer to fire appropriately.  (We round up to the next second, to avoid
 * floods of timer firings during periods of high lock contention and traffic).
 * As done by ldlm_add_waiting_lock(), the caller must grab a lock reference
 * if it has been added to the waiting list (1 is returned).
 *
 * Called with the namespace lock held.
 */
static int __ldlm_add_waiting_lock(ldlm_wait_timer_t *wtimer,
                                   struct ldlm_lock *lock, int seconds)
{
        cfs_time_t timeout;
        cfs_time_t timeout_rounded;

        if (!cfs_list_empty(&lock->l_pending_chain))
                return 0;

        if (OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_NOTIMEOUT) ||
            OBD_FAIL_CHECK(OBD_FAIL_PTLRPC_HPREQ_TIMEOUT))
                seconds = 1;

        timeout = cfs_time_shift(seconds);
        if (likely(cfs_time_after(timeout, lock->l_callback_timeout)))
                lock->l_callback_timeout = timeout;

        timeout_rounded = round_timeout(lock->l_callback_timeout);

        if (cfs_time_before(timeout_rounded,
                            cfs_timer_deadline(&wtimer->lwt_timer)) ||
            !cfs_timer_is_armed(&wtimer->lwt_timer)) {
                cfs_timer_arm(&wtimer->lwt_timer, timeout_rounded);
        }
        /* if the new lock has a shorter timeout than something earlier on
           the list, we'll wait the longer amount of time; no big deal. */
        /* FIFO */
        cfs_list_add_tail(&lock->l_pending_chain, &wtimer->lwt_waiting_locks);
        return 1;
}

static int ldlm_add_waiting_lock(struct ldlm_lock *lock)
{
        ldlm_wait_timer_t *wtimer;
        int timeout = ldlm_get_enq_timeout(lock);
        int ret;

        LASSERT(!(lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK));

        wtimer = ldlm_lock_to_wtimer(lock);
        cfs_spin_lock_bh(&wtimer->lwt_lock);
        if (lock->l_destroyed) {
                static cfs_time_t next;
                cfs_spin_unlock_bh(&wtimer->lwt_lock);
                LDLM_ERROR(lock, "not waiting on destroyed lock (bug 5653)");
                if (cfs_time_after(cfs_time_current(), next)) {
                        next = cfs_time_shift(14400);
                        libcfs_debug_dumpstack(NULL);
                }
                return 0;
        }

        ret = __ldlm_add_waiting_lock(wtimer, lock, timeout);
        if (ret) {
                /* grab ref on the lock if it has been added to the
                 * waiting list */
                LDLM_LOCK_GET(lock);
        }
        cfs_spin_unlock_bh(&wtimer->lwt_lock);

        LDLM_DEBUG(lock, "%sadding to wait list(timeout: %d, AT: %s)",
                   ret == 0 ? "not re-" : "", timeout,
                   AT_OFF ? "off" : "on");
        return ret;
}

/*
 * Remove a lock from the pending list, likely because it had its cancellation
 * callback arrive without incident.  This adjusts the lock-timeout timer if
 * needed.  Returns 0 if the lock wasn't pending after all, 1 if it was.
 * As done by ldlm_del_waiting_lock(), the caller must release the lock
 * reference when the lock is removed from any list (1 is returned).
 *
 * Called with namespace lock held.
 */
static int __ldlm_del_waiting_lock(ldlm_wait_timer_t *wtimer,
                                   struct ldlm_lock *lock)
{
        cfs_list_t *list_next;

        if (cfs_list_empty(&lock->l_pending_chain))
                return 0;

        list_next = lock->l_pending_chain.next;
        if (lock->l_pending_chain.prev == &wtimer->lwt_waiting_locks) {
                /* Removing the head of the list, adjust timer. */
                if (list_next == &wtimer->lwt_waiting_locks) {
                        /* No more, just cancel. */
                        cfs_timer_disarm(&wtimer->lwt_timer);
                } else {
                        struct ldlm_lock *next;
                        next = cfs_list_entry(list_next, struct ldlm_lock,
                                              l_pending_chain);
                        cfs_timer_arm(&wtimer->lwt_timer,
                                      round_timeout(next->l_callback_timeout));
                }
        }
        cfs_list_del_init(&lock->l_pending_chain);

        return 1;
}

int ldlm_del_waiting_lock(struct ldlm_lock *lock)
{
        ldlm_wait_timer_t *wtimer;
        int ret;

        if (lock->l_export == NULL) {
                /* We don't have a "waiting locks list" on clients. */
                CDEBUG(D_DLMTRACE, "Client lock %p : no-op\n", lock);
                return 0;
        }

        wtimer = ldlm_lock_to_wtimer(lock);
        cfs_spin_lock_bh(&wtimer->lwt_lock);
        ret = __ldlm_del_waiting_lock(wtimer, lock);
        cfs_spin_unlock_bh(&wtimer->lwt_lock);
        if (ret)
                /* release lock ref if it has indeed been removed
                 * from a list */
                LDLM_LOCK_RELEASE(lock);

        LDLM_DEBUG(lock, "%s", ret == 0 ? "wasn't waiting" : "removed");
        return ret;
}

static int __ldlm_refresh_waiting_lock(ldlm_wait_timer_t *wtimer,
                                       struct ldlm_lock *lock, int timeout)
{
        if (cfs_list_empty(&lock->l_pending_chain))
                return 0;

        /* we remove/add the lock to the waiting list, so no needs to
         * release/take a lock reference */
        __ldlm_del_waiting_lock(wtimer, lock);
        __ldlm_add_waiting_lock(wtimer, lock, timeout);
        return 1;
}
/*
 * Prolong the lock
 *
 * Called with namespace lock held.
 */
int ldlm_refresh_waiting_lock(struct ldlm_lock *lock, int timeout)
{
        ldlm_wait_timer_t *wtimer;
        int     rc;

        if (lock->l_export == NULL) {
                /* We don't have a "waiting locks list" on clients. */
                LDLM_DEBUG(lock, "client lock: no-op");
                return 0;
        }

        wtimer = ldlm_lock_to_wtimer(lock);
        cfs_spin_lock_bh(&wtimer->lwt_lock);
        rc = __ldlm_refresh_waiting_lock(wtimer, lock, timeout);
        cfs_spin_unlock_bh(&wtimer->lwt_lock);
        if (rc)
                LDLM_DEBUG(lock, "refreshed");
        else
                LDLM_DEBUG(lock, "wasn't waiting");
        return rc;
}
#else /* !__KERNEL__ */

static int ldlm_add_waiting_lock(struct ldlm_lock *lock)
{
        LASSERT(!(lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK));
        RETURN(1);
}

int ldlm_del_waiting_lock(struct ldlm_lock *lock)
{
        RETURN(0);
}

int ldlm_refresh_waiting_lock(struct ldlm_lock *lock, int timeout)
{
        RETURN(0);
}
#endif /* __KERNEL__ */

static void ldlm_failed_ast(struct ldlm_lock *lock, int rc,
                            const char *ast_type)
{
        LCONSOLE_ERROR_MSG(0x138, "%s: A client on nid %s was evicted due "
                           "to a lock %s callback time out: rc %d\n",
                           lock->l_export->exp_obd->obd_name,
                           obd_export_nid2str(lock->l_export), ast_type, rc);

        if (obd_dump_on_timeout)
                libcfs_debug_dumplog();
#ifdef __KERNEL__
        {
        ldlm_wait_timer_t *wtimer;

        wtimer = ldlm_lock_to_wtimer(lock);

        cfs_spin_lock_bh(&wtimer->lwt_lock);
        if (__ldlm_del_waiting_lock(wtimer, lock) == 0) {
                /* the lock was not in any list, grab an extra ref before adding
                 * the lock to the expired list */
                LDLM_LOCK_GET(lock);
        }

        cfs_spin_lock_bh(&ldlm_waitd.lwd_lock);
        cfs_list_add(&lock->l_pending_chain,
                     &ldlm_waitd.lwd_expired_locks);
        cfs_waitq_signal(&ldlm_waitd.lwd_waitq);
        cfs_spin_unlock_bh(&ldlm_waitd.lwd_lock);

        cfs_spin_unlock_bh(&wtimer->lwt_lock);
        }
#else
        class_fail_export(lock->l_export);
#endif
}

static int ldlm_handle_ast_error(struct ldlm_lock *lock,
                                 struct ptlrpc_request *req, int rc,
                                 const char *ast_type)
{
        lnet_process_id_t peer = req->rq_import->imp_connection->c_peer;

        if (rc == -ETIMEDOUT || rc == -EINTR || rc == -ENOTCONN) {
                LASSERT(lock->l_export);
                if (lock->l_export->exp_libclient) {
                        LDLM_DEBUG(lock, "%s AST to liblustre client (nid %s)"
                                   " timeout, just cancelling lock", ast_type,
                                   libcfs_nid2str(peer.nid));
                        ldlm_lock_cancel(lock);
                        rc = -ERESTART;
                } else if (lock->l_flags & LDLM_FL_CANCEL) {
                        LDLM_DEBUG(lock, "%s AST timeout from nid %s, but "
                                   "cancel was received (AST reply lost?)",
                                   ast_type, libcfs_nid2str(peer.nid));
                        ldlm_lock_cancel(lock);
                        rc = -ERESTART;
                } else {
                        ldlm_del_waiting_lock(lock);
                        ldlm_failed_ast(lock, rc, ast_type);
                }
        } else if (rc) {
                if (rc == -EINVAL) {
                        struct ldlm_resource *res = lock->l_resource;
                        LDLM_DEBUG(lock, "client (nid %s) returned %d"
                               " from %s AST - normal race",
                               libcfs_nid2str(peer.nid),
                               req->rq_repmsg ?
                               lustre_msg_get_status(req->rq_repmsg) : -1,
                               ast_type);
                        if (res) {
                                /* update lvbo to return proper attributes.
                                 * see bug 23174 */
                                ldlm_resource_getref(res);
                                ldlm_res_lvbo_update(res, NULL, 1);
                                ldlm_resource_putref(res);
                        }

                } else {
                        LDLM_ERROR(lock, "client (nid %s) returned %d "
                                   "from %s AST", libcfs_nid2str(peer.nid),
                                   (req->rq_repmsg != NULL) ?
                                   lustre_msg_get_status(req->rq_repmsg) : 0,
                                   ast_type);
                }
                ldlm_lock_cancel(lock);
                /* Server-side AST functions are called from ldlm_reprocess_all,
                 * which needs to be told to please restart its reprocessing. */
                rc = -ERESTART;
        }

        return rc;
}

static int ldlm_cb_interpret(const struct lu_env *env,
                             struct ptlrpc_request *req, void *data, int rc)
{
        struct ldlm_cb_set_arg *arg;
        struct ldlm_lock *lock;
        ENTRY;

        LASSERT(data != NULL);

        arg = req->rq_async_args.pointer_arg[0];
        lock = req->rq_async_args.pointer_arg[1];
        LASSERT(lock != NULL);
        if (rc != 0) {
                rc = ldlm_handle_ast_error(lock, req, rc,
                                           arg->type == LDLM_BL_CALLBACK
                                           ? "blocking" : "completion");
        }

        LDLM_LOCK_RELEASE(lock);

        if (rc == -ERESTART)
                cfs_atomic_set(&arg->restart, 1);

        RETURN(0);
}

static inline int ldlm_bl_and_cp_ast_fini(struct ptlrpc_request *req,
                                          struct ldlm_cb_set_arg *arg,
                                          struct ldlm_lock *lock,
                                          int instant_cancel)
{
        int rc = 0;
        ENTRY;

        if (unlikely(instant_cancel)) {
                rc = ptl_send_rpc(req, 1);
                ptlrpc_req_finished(req);
                if (rc == 0)
                        /* If we cancelled the lock, we need to restart
                         * ldlm_reprocess_queue */
                        cfs_atomic_set(&arg->restart, 1);
        } else {
                LDLM_LOCK_GET(lock);
                ptlrpc_set_add_req(arg->set, req);
        }

        RETURN(rc);
}

/**
 * Check if there are requests in the export request list which prevent
 * the lock canceling and make these requests high priority ones.
 */
static void ldlm_lock_reorder_req(struct ldlm_lock *lock)
{
        struct ptlrpc_request *req;
        ENTRY;

        if (lock->l_export == NULL) {
                LDLM_DEBUG(lock, "client lock: no-op");
                RETURN_EXIT;
        }

        cfs_spin_lock_bh(&lock->l_export->exp_rpc_lock);
        cfs_list_for_each_entry(req, &lock->l_export->exp_queued_rpc,
                                rq_exp_list) {
                if (!req->rq_hp && req->rq_ops->hpreq_lock_match &&
                    req->rq_ops->hpreq_lock_match(req, lock))
                        ptlrpc_hpreq_reorder(req);
        }
        cfs_spin_unlock_bh(&lock->l_export->exp_rpc_lock);
        EXIT;
}

/*
 * ->l_blocking_ast() method for server-side locks. This is invoked when newly
 * enqueued server lock conflicts with given one.
 *
 * Sends blocking ast rpc to the client owning that lock; arms timeout timer
 * to wait for client response.
 */
int ldlm_server_blocking_ast(struct ldlm_lock *lock,
                             struct ldlm_lock_desc *desc,
                             void *data, int flag)
{
        struct ldlm_cb_set_arg *arg = data;
        struct ldlm_request    *body;
        struct ptlrpc_request  *req;
        int                     instant_cancel = 0;
        int                     rc = 0;
        ENTRY;

        if (flag == LDLM_CB_CANCELING)
                /* Don't need to do anything here. */
                RETURN(0);

        LASSERT(lock);
        LASSERT(data != NULL);
        if (lock->l_export->exp_obd->obd_recovering != 0) {
                LDLM_ERROR(lock, "BUG 6063: lock collide during recovery");
                ldlm_lock_dump(D_ERROR, lock, 0);
        }

        ldlm_lock_reorder_req(lock);

        req = ptlrpc_request_alloc_pack(lock->l_export->exp_imp_reverse,
                                        &RQF_LDLM_BL_CALLBACK,
                                        LUSTRE_DLM_VERSION, LDLM_BL_CALLBACK);
        if (req == NULL)
                RETURN(-ENOMEM);

        req->rq_async_args.pointer_arg[0] = arg;
        req->rq_async_args.pointer_arg[1] = lock;
        req->rq_interpret_reply = ldlm_cb_interpret;
        req->rq_no_resend = 1;

        lock_res(lock->l_resource);
        if (lock->l_granted_mode != lock->l_req_mode) {
                /* this blocking AST will be communicated as part of the
                 * completion AST instead */
                unlock_res(lock->l_resource);
                ptlrpc_req_finished(req);
                LDLM_DEBUG(lock, "lock not granted, not sending blocking AST");
                RETURN(0);
        }

        if (lock->l_destroyed) {
                /* What's the point? */
                unlock_res(lock->l_resource);
                ptlrpc_req_finished(req);
                RETURN(0);
        }

        if (lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK)
                instant_cancel = 1;

        body = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        body->lock_handle[0] = lock->l_remote_handle;
        body->lock_desc = *desc;
        body->lock_flags |= (lock->l_flags & LDLM_AST_FLAGS);

        LDLM_DEBUG(lock, "server preparing blocking AST");

        ptlrpc_request_set_replen(req);
        if (instant_cancel) {
                unlock_res(lock->l_resource);
                ldlm_lock_cancel(lock);
        } else {
                LASSERT(lock->l_granted_mode == lock->l_req_mode);
                ldlm_add_waiting_lock(lock);
                unlock_res(lock->l_resource);
        }

        req->rq_send_state = LUSTRE_IMP_FULL;
        /* ptlrpc_request_alloc_pack already set timeout */
        if (AT_OFF)
                req->rq_timeout = ldlm_get_rq_timeout();

        ldlm_exp_stats_inc(lock->l_export, LDLM_BL_CALLBACK);

        rc = ldlm_bl_and_cp_ast_fini(req, arg, lock, instant_cancel);

        RETURN(rc);
}

int ldlm_server_completion_ast(struct ldlm_lock *lock, int flags, void *data)
{
        struct ldlm_cb_set_arg *arg = data;
        struct ldlm_request    *body;
        struct ptlrpc_request  *req;
        long                    total_enqueue_wait;
        int                     instant_cancel = 0;
        int                     rc = 0;
        ENTRY;

        LASSERT(lock != NULL);
        LASSERT(data != NULL);

        total_enqueue_wait = cfs_time_sub(cfs_time_current_sec(),
                                          lock->l_last_activity);

        req = ptlrpc_request_alloc(lock->l_export->exp_imp_reverse,
                                    &RQF_LDLM_CP_CALLBACK);
        if (req == NULL)
                RETURN(-ENOMEM);

        /* server namespace, doesn't need lock */
        if (lock->l_resource->lr_lvb_len) {
                 req_capsule_set_size(&req->rq_pill, &RMF_DLM_LVB, RCL_CLIENT,
                                      lock->l_resource->lr_lvb_len);
        }

        rc = ptlrpc_request_pack(req, LUSTRE_DLM_VERSION, LDLM_CP_CALLBACK);
        if (rc) {
                ptlrpc_request_free(req);
                RETURN(rc);
        }

        req->rq_async_args.pointer_arg[0] = arg;
        req->rq_async_args.pointer_arg[1] = lock;
        req->rq_interpret_reply = ldlm_cb_interpret;
        req->rq_no_resend = 1;
        body = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);

        body->lock_handle[0] = lock->l_remote_handle;
        body->lock_flags = flags;
        ldlm_lock2desc(lock, &body->lock_desc);
        if (lock->l_resource->lr_lvb_len) {
                void *lvb = req_capsule_client_get(&req->rq_pill, &RMF_DLM_LVB);

                cfs_down(&lock->l_resource->lr_lvb_sem);
                memcpy(lvb, lock->l_resource->lr_lvb_data,
                       lock->l_resource->lr_lvb_len);
                cfs_up(&lock->l_resource->lr_lvb_sem);
        }

        LDLM_DEBUG(lock, "server preparing completion AST (after %lds wait)",
                   total_enqueue_wait);

        /* Server-side enqueue wait time estimate, used in
            __ldlm_add_waiting_lock to set future enqueue timers */
        if (total_enqueue_wait < ldlm_get_enq_timeout(lock))
                at_measured(ldlm_lock_to_ns_at(lock),
                            total_enqueue_wait);
        else
                /* bz18618. Don't add lock enqueue time we spend waiting for a
                   previous callback to fail. Locks waiting legitimately will
                   get extended by ldlm_refresh_waiting_lock regardless of the
                   estimate, so it's okay to underestimate here. */
                LDLM_DEBUG(lock, "lock completed after %lus; estimate was %ds. "
                       "It is likely that a previous callback timed out.",
                       total_enqueue_wait,
                       at_get(ldlm_lock_to_ns_at(lock)));

        ptlrpc_request_set_replen(req);

        req->rq_send_state = LUSTRE_IMP_FULL;
        /* ptlrpc_request_pack already set timeout */
        if (AT_OFF)
                req->rq_timeout = ldlm_get_rq_timeout();

        /* We only send real blocking ASTs after the lock is granted */
        lock_res_and_lock(lock);
        if (lock->l_flags & LDLM_FL_AST_SENT) {
                body->lock_flags |= LDLM_FL_AST_SENT;
                /* copy ast flags like LDLM_FL_DISCARD_DATA */
                body->lock_flags |= (lock->l_flags & LDLM_AST_FLAGS);

                /* We might get here prior to ldlm_handle_enqueue setting
                 * LDLM_FL_CANCEL_ON_BLOCK flag. Then we will put this lock
                 * into waiting list, but this is safe and similar code in
                 * ldlm_handle_enqueue will call ldlm_lock_cancel() still,
                 * that would not only cancel the lock, but will also remove
                 * it from waiting list */
                instant_cancel = !!(lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK);
                if (!instant_cancel) /* start the lock-timeout clock */
                        ldlm_add_waiting_lock(lock);
        }
        unlock_res_and_lock(lock);
        if (instant_cancel)
                ldlm_lock_cancel(lock);

        ldlm_exp_stats_inc(lock->l_export, LDLM_CP_CALLBACK);

        rc = ldlm_bl_and_cp_ast_fini(req, arg, lock, instant_cancel);

        RETURN(rc);
}

int ldlm_server_glimpse_ast(struct ldlm_lock *lock, void *data)
{
        struct ldlm_resource  *res = lock->l_resource;
        struct ldlm_request   *body;
        struct ptlrpc_request *req;
        int                    rc;
        ENTRY;

        LASSERT(lock != NULL);

        req = ptlrpc_request_alloc_pack(lock->l_export->exp_imp_reverse,
                                        &RQF_LDLM_GL_CALLBACK,
                                        LUSTRE_DLM_VERSION, LDLM_GL_CALLBACK);

        if (req == NULL)
                RETURN(-ENOMEM);

        body = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        body->lock_handle[0] = lock->l_remote_handle;
        ldlm_lock2desc(lock, &body->lock_desc);

        /* server namespace, doesn't need lock */
        req_capsule_set_size(&req->rq_pill, &RMF_DLM_LVB, RCL_SERVER,
                             lock->l_resource->lr_lvb_len);
        res = lock->l_resource;
        ptlrpc_request_set_replen(req);


        req->rq_send_state = LUSTRE_IMP_FULL;
        /* ptlrpc_request_alloc_pack already set timeout */
        if (AT_OFF)
                req->rq_timeout = ldlm_get_rq_timeout();

        ldlm_exp_stats_inc(lock->l_export, LDLM_GL_CALLBACK);

        rc = ptlrpc_queue_wait(req);
        if (rc == -ELDLM_NO_LOCK_DATA)
                LDLM_DEBUG(lock, "lost race - client has a lock but no inode");
        else if (rc != 0)
                rc = ldlm_handle_ast_error(lock, req, rc, "glimpse");
        else
                rc = ldlm_res_lvbo_update(res, req, 1);

        ptlrpc_req_finished(req);
        if (rc == -ERESTART)
                ldlm_reprocess_all(res);

        RETURN(rc);
}

#ifdef __KERNEL__
extern unsigned long long lu_time_stamp_get(void);
#else
#define lu_time_stamp_get() time(NULL)
#endif

static void ldlm_svc_get_eopc(const struct ldlm_request *dlm_req,
                              struct lprocfs_stats *srv_stats)
{
        int lock_type = 0, op = 0;

        lock_type = dlm_req->lock_desc.l_resource.lr_type;

        switch (lock_type) {
        case LDLM_PLAIN:
                op = PTLRPC_LAST_CNTR + LDLM_PLAIN_ENQUEUE;
                break;
        case LDLM_EXTENT:
                if (dlm_req->lock_flags & LDLM_FL_HAS_INTENT)
                        op = PTLRPC_LAST_CNTR + LDLM_GLIMPSE_ENQUEUE;
                else
                        op = PTLRPC_LAST_CNTR + LDLM_EXTENT_ENQUEUE;
                break;
        case LDLM_FLOCK:
                op = PTLRPC_LAST_CNTR + LDLM_FLOCK_ENQUEUE;
                break;
        case LDLM_IBITS:
                op = PTLRPC_LAST_CNTR + LDLM_IBITS_ENQUEUE;
                break;
        default:
                op = 0;
                break;
        }

        if (op)
                lprocfs_counter_incr(srv_stats, op);

        return;
}

static int ldlm_lock_enqueue_prep(struct ldlm_namespace *ns,
                                  struct ptlrpc_request *req,
                                  const struct ldlm_request *dlm_req,
                                  const struct ldlm_callback_suite *cbs,
                                  struct ldlm_lock **lockp)
{
        const struct ldlm_lock_desc *desc = &dlm_req->lock_desc;
        struct ldlm_lock *lock = NULL;
        ldlm_it_status_t  it_status;
        int   rc = 0;

        ENTRY;

        it_status = ldlm_lock_intent_check(ns, req, dlm_req->lock_flags);
        if (!(it_status & LDLM_ENQ_PREP_LOCK)) {
                /* don't want to prepare lock, because ns_policy will
                 * always create and enqueue another lock */
                RETURN(0);
        }

        /* Find an existing lock in the per-export lock hash */
        if (unlikely((dlm_req->lock_flags & LDLM_FL_REPLAY))) {
                lock = cfs_hash_lookup(req->rq_export->exp_lock_hash,
                                       (void *)&dlm_req->lock_handle[0]);
        }

        if (lock != NULL) {
                DEBUG_REQ(D_DLMTRACE, req, "found existing lock cookie "
                          LPX64, lock->l_handle.h_cookie);

        } else {
                /* NB: The lock's callback data might be set in the
                 * policy function */
                lock = ldlm_lock_create(ns, &desc->l_resource.lr_name,
                                        desc->l_resource.lr_type,
                                        desc->l_req_mode,
                                        cbs, NULL, 0);
                if (lock == NULL)
                        RETURN(-ENOMEM);

                lock->l_remote_handle = dlm_req->lock_handle[0];
                LDLM_DEBUG(lock,
                           "server-side enqueue handler, new lock created");

                OBD_FAIL_TIMEOUT(OBD_FAIL_LDLM_ENQUEUE_BLOCKED,
                                 obd_timeout * 2);
                /* Don't enqueue a lock onto a desconnected export
                 * due to eviction (bug 3822) or server umount (bug 24324).
                 * Cancel it now instead. */
                if (req->rq_export->exp_disconnected) {
                        LDLM_ERROR(lock, "lock on disconnected export %p",
                                   req->rq_export);
                        LDLM_LOCK_RELEASE(lock);
                        RETURN(-ENOTCONN);
                }

                lock->l_export = class_export_lock_get(req->rq_export, lock);
                if (lock->l_export->exp_lock_hash) {
                        cfs_hash_add(lock->l_export->exp_lock_hash,
                                     &lock->l_remote_handle,
                                     &lock->l_exp_hash);
                }
        }

        /* pack reply for non-policy path */
        if (!(it_status & LDLM_ENQ_IT_POLICY)) {
                if (lock->l_resource->lr_lvb_len > 0) {
                        req_capsule_set_size(&req->rq_pill,
                                             &RMF_DLM_LVB, RCL_SERVER,
                                             lock->l_resource->lr_lvb_len);
                }

                if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_ENQUEUE_EXTENT_ERR)) {
                        LDLM_LOCK_RELEASE(lock);
                        RETURN(-ENOMEM);
                }

                rc = req_capsule_server_pack(&req->rq_pill);
                if (rc) {
                        LDLM_LOCK_RELEASE(lock);
                        RETURN(rc);
                }
        }

        lock->l_last_activity = cfs_time_current_sec();
        if (desc->l_resource.lr_type != LDLM_PLAIN)
                lock->l_policy_data = desc->l_policy_data;
        if (desc->l_resource.lr_type == LDLM_EXTENT)
                lock->l_req_extent = lock->l_policy_data.l_extent;

        *lockp = lock;
        RETURN(rc);
}

static int ldlm_lock_enqueue_fini(struct ptlrpc_request *req,
                                  const struct ldlm_request *dlm_req,
                                  struct ldlm_lock *lock, __u32 flags)
{
        struct ldlm_reply *dlm_rep;
        int    instant_cancel = 0;
        int    rc = 0;

        ENTRY;
        if (lock == NULL) /* it's possible for intent_only */
                RETURN(0);

        dlm_rep = req_capsule_server_get(&req->rq_pill, &RMF_DLM_REP);
        dlm_rep->lock_flags = flags;

        if (lock->l_resource->lr_lvb_len > 0) {
                void *lvb = req_capsule_server_get(&req->rq_pill, &RMF_DLM_LVB);

                LASSERTF(lvb != NULL, "req %p, lock %p\n", req, lock);
                cfs_down(&lock->l_resource->lr_lvb_sem);
                memcpy(lvb, lock->l_resource->lr_lvb_data,
                       lock->l_resource->lr_lvb_len);
                cfs_up(&lock->l_resource->lr_lvb_sem);
        }

        ldlm_lock2desc(lock, &dlm_rep->lock_desc);
        ldlm_lock2handle(lock, &dlm_rep->lock_handle);

        /* We never send a blocking AST until the lock is granted, but
         * we can tell it right now */
        lock_res_and_lock(lock);

        /* Now take into account flags to be inherited from original lock
         * request both in reply to client and in our own lock flags. */
        dlm_rep->lock_flags |= dlm_req->lock_flags & LDLM_INHERIT_FLAGS;
        lock->l_flags |= dlm_req->lock_flags & LDLM_INHERIT_FLAGS;

        /* Don't move a pending lock onto the export if it has already been
         * disconnected due to eviction (bug 5683) or server umount (bug 24324).
         * Cancel it now instead. */
        if (unlikely(req->rq_export->exp_disconnected ||
                     OBD_FAIL_CHECK(OBD_FAIL_LDLM_ENQUEUE_OLD_EXPORT))) {
                LDLM_ERROR(lock, "lock on destroyed export %p", req->rq_export);
                /* NB: cancel it now, otherwise ldlm_cancel_locks_for_export()
                 * may have no chance to cancel this lock if it's already
                 * destroyed by ldlm_handle_enqueue0 */
                instant_cancel = 1;
                rc = -ENOTCONN;

        } else if (lock->l_flags & LDLM_FL_AST_SENT) {
                dlm_rep->lock_flags |= LDLM_FL_AST_SENT;
                if (lock->l_granted_mode == lock->l_req_mode) {
                        /*
                         * Only cancel lock if it was granted, because it would
                         * be destroyed immediately and would never be granted
                         * in the future, causing timeouts on client.  Not
                         * granted lock will be cancelled immediately after
                         * sending completion AST.
                         */
                        instant_cancel = !!(dlm_rep->lock_flags &
                                            LDLM_FL_CANCEL_ON_BLOCK);
                        if (!instant_cancel)
                                ldlm_add_waiting_lock(lock);
                }
        }
        unlock_res_and_lock(lock);

        if (instant_cancel)
                ldlm_lock_cancel(lock);

        /* NB: LOCK_REPLACED code in ldlm_lock_intent and canceled
         * lock depend on this */
        if (instant_cancel ||
            dlm_req->lock_desc.l_resource.lr_type != LDLM_FLOCK)
                ldlm_reprocess_all(lock->l_resource);
        RETURN(rc);
}

/*
 * Main server-side entry point into LDLM. This is called by ptlrpc service
 * threads to carry out client lock enqueueing requests.
 */
int ldlm_handle_enqueue0(struct ldlm_namespace *ns,
                         struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req,
                         const struct ldlm_callback_suite *cbs)
{
        const struct ldlm_lock_desc *desc = &dlm_req->lock_desc;
        struct ldlm_lock *lock = NULL;
        int err = ELDLM_OK;
        int rc = 0;
        __u32 flags;
        ENTRY;

        LDLM_DEBUG_NOLOCK("server-side enqueue handler START");

        ldlm_request_cancel(req, dlm_req, LDLM_ENQUEUE_CANCEL_OFF);

        LASSERT(req->rq_export);

        if (req->rq_svcd->scd_service->srv_stats) {
                ldlm_svc_get_eopc(dlm_req,
                                  req->rq_svcd->scd_service->srv_stats);
        }

        ldlm_exp_stats_inc(req->rq_export, LDLM_ENQUEUE);

        if (unlikely(desc->l_resource.lr_type < LDLM_MIN_TYPE ||
                     desc->l_resource.lr_type >= LDLM_MAX_TYPE)) {
                DEBUG_REQ(D_ERROR, req, "invalid lock request type %d",
                          desc->l_resource.lr_type);
                GOTO(out, rc = -EFAULT);
        }

        if (unlikely(desc->l_req_mode <= LCK_MINMODE ||
                     desc->l_req_mode >= LCK_MAXMODE ||
                     desc->l_req_mode & (desc->l_req_mode-1))) {
                DEBUG_REQ(D_ERROR, req, "invalid lock request mode %d",
                          desc->l_req_mode);
                GOTO(out, rc = -EFAULT);
        }

        if (req->rq_export->exp_connect_flags & OBD_CONNECT_IBITS) {
                if (unlikely(desc->l_resource.lr_type == LDLM_PLAIN)) {
                        DEBUG_REQ(D_ERROR, req,
                                  "PLAIN lock request from IBITS client?");
                        GOTO(out, rc = -EPROTO);
                }
        } else if (unlikely(desc->l_resource.lr_type == LDLM_IBITS)) {
                DEBUG_REQ(D_ERROR, req,
                          "IBITS lock request from unaware client?");
                GOTO(out, rc = -EPROTO);
        }

        /* Make sure we never ever grant usual metadata locks to
         * liblustre clients */
        if (req->rq_export->exp_libclient &&
            (desc->l_resource.lr_type == LDLM_PLAIN ||
             desc->l_resource.lr_type == LDLM_IBITS) &&
            !(dlm_req->lock_flags & LDLM_FL_CANCEL_ON_BLOCK)){
                DEBUG_REQ(D_ERROR, req, "sync lock request from libclient %d",
                          dlm_req->lock_flags);
                GOTO(out, rc = -EPROTO);
        }

#if 0
        /* FIXME this makes it impossible to use LDLM_PLAIN locks -- check
           against server's _CONNECT_SUPPORTED flags? (I don't want to use
           ibits for mgc/mgs) */

        /* INODEBITS_INTEROP: Perform conversion from plain lock to
         * inodebits lock if client does not support them. */
        if (!(req->rq_export->exp_connect_flags & OBD_CONNECT_IBITS) &&
            (dlm_req->lock_desc.l_resource.lr_type == LDLM_PLAIN)) {
                dlm_req->lock_desc.l_resource.lr_type = LDLM_IBITS;
                dlm_req->lock_desc.l_policy_data.l_inodebits.bits =
                        MDS_INODELOCK_LOOKUP | MDS_INODELOCK_UPDATE;
                if (dlm_req->lock_desc.l_req_mode == LCK_PR)
                        dlm_req->lock_desc.l_req_mode = LCK_CR;
        }
#endif

        rc = ldlm_lock_enqueue_prep(ns, req, dlm_req, cbs, &lock);
        if (rc != 0)
                GOTO(out, rc);

        flags = dlm_req->lock_flags;
        if (ldlm_lock_intent_check(ns, req, flags) & LDLM_ENQ_IT_POLICY) {
                /* In this case, the reply buffer is allocated deep in
                 * ldlm_lock_intent by the policy function. */
                err = ldlm_lock_intent(ns, &lock, (void *)req,
                                       desc->l_req_mode, (int *)&flags);

        } else { /* replay or no intent */
                LASSERT(lock != NULL);
                LASSERT(req->rq_packed_final);
                err = ldlm_lock_enqueue(ns, lock, (int *)&flags);
        }

        if (err == ELDLM_OK)
                rc = ldlm_lock_enqueue_fini(req, dlm_req, lock, flags);
        else if (err < 0) /* a real failure */
                rc = err;

 out:
        req->rq_status = rc ?: err; /* return either error - bug 11190 */

        if (!req->rq_packed_final) {
                /* NB: we can be here only if we failed to enqueue lock
                 * or execute intent policy, because ldlm_lock_enqueue_prep
                 * or ns::ns_policy will pack reply */
                LASSERT(rc != 0);
                lustre_pack_reply(req, 1, NULL, NULL);
        }

        if (lock != NULL) {
                LDLM_DEBUG(lock, "server-side enqueue handler END, "
                           "packing reply (err=%d, rc=%d)", err, rc);
                if (rc != 0) {
                        lock_res_and_lock(lock);
                        ldlm_resource_unlink_lock(lock);
                        ldlm_lock_destroy_nolock(lock);
                        unlock_res_and_lock(lock);
                }
                LDLM_LOCK_RELEASE(lock);
        }

        LDLM_DEBUG_NOLOCK("server-side enqueue handler END (lock %p, rc %d)",
                          lock, rc);
        RETURN(rc);
}

int ldlm_handle_enqueue(struct ptlrpc_request *req,
                        ldlm_completion_callback completion_callback,
                        ldlm_blocking_callback blocking_callback,
                        ldlm_glimpse_callback glimpse_callback)
{
        struct ldlm_request *dlm_req;
        struct ldlm_callback_suite cbs = {
                .lcs_completion = completion_callback,
                .lcs_blocking   = blocking_callback,
                .lcs_glimpse    = glimpse_callback
        };

        dlm_req = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        if (dlm_req == NULL)
                return -EFAULT;

        return ldlm_handle_enqueue0(req->rq_export->exp_obd->obd_namespace,
                                    req, dlm_req, &cbs);
}

int ldlm_handle_convert0(struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req)
{
        struct ldlm_reply *dlm_rep;
        struct ldlm_lock *lock;
        int rc;
        ENTRY;

        ldlm_exp_stats_inc(req->rq_export, LDLM_CONVERT);

        rc = req_capsule_server_pack(&req->rq_pill);
        if (rc)
                RETURN(rc);

        dlm_rep = req_capsule_server_get(&req->rq_pill, &RMF_DLM_REP);
        dlm_rep->lock_flags = dlm_req->lock_flags;

        lock = ldlm_handle2lock(&dlm_req->lock_handle[0]);
        if (!lock) {
                req->rq_status = EINVAL;
        } else {
                void *res = NULL;

                LDLM_DEBUG(lock, "server-side convert handler START");

                lock->l_last_activity = cfs_time_current_sec();
                res = ldlm_lock_convert(lock, dlm_req->lock_desc.l_req_mode,
                                        &dlm_rep->lock_flags);
                if (res) {
                        if (ldlm_del_waiting_lock(lock))
                                LDLM_DEBUG(lock, "converted waiting lock");
                        req->rq_status = 0;
                } else {
                        req->rq_status = EDEADLOCK;
                }
        }

        if (lock) {
                if (!req->rq_status)
                        ldlm_reprocess_all(lock->l_resource);
                LDLM_DEBUG(lock, "server-side convert handler END");
                LDLM_LOCK_PUT(lock);
        } else
                LDLM_DEBUG_NOLOCK("server-side convert handler END");

        RETURN(0);
}

int ldlm_handle_convert(struct ptlrpc_request *req)
{
        int rc;
        struct ldlm_request *dlm_req;

        dlm_req = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        if (dlm_req != NULL) {
                rc = ldlm_handle_convert0(req, dlm_req);
        } else {
                CERROR ("Can't unpack dlm_req\n");
                rc = -EFAULT;
        }
        return rc;
}

/* Cancel all the locks whos handles are packed into ldlm_request */
int ldlm_request_cancel(struct ptlrpc_request *req,
                        const struct ldlm_request *dlm_req, int first)
{
        struct ldlm_resource *res, *pres = NULL;
        struct ldlm_lock *lock;
        int i, count, done = 0;
        ENTRY;

        count = dlm_req->lock_count ? dlm_req->lock_count : 1;
        if (first >= count)
                RETURN(0);

        /* There is no lock on the server at the replay time,
         * skip lock cancelling to make replay tests to pass. */
        if (lustre_msg_get_flags(req->rq_reqmsg) & MSG_REPLAY)
                RETURN(0);

        LDLM_DEBUG_NOLOCK("server-side cancel handler START: %d locks, "
                          "starting at %d", count, first);

        for (i = first; i < count; i++) {
                lock = ldlm_handle2lock(&dlm_req->lock_handle[i]);
                if (!lock) {
                        LDLM_DEBUG_NOLOCK("server-side cancel handler stale "
                                          "lock (cookie "LPU64")",
                                          dlm_req->lock_handle[i].cookie);
                        continue;
                }

                res = lock->l_resource;
                done++;

                if (res != pres) {
                        if (pres != NULL) {
                                ldlm_reprocess_all(pres);
                                LDLM_RESOURCE_DELREF(pres);
                                ldlm_resource_putref(pres);
                        }
                        if (res != NULL) {
                                ldlm_resource_getref(res);
                                LDLM_RESOURCE_ADDREF(res);
                                ldlm_res_lvbo_update(res, NULL, 1);
                        }
                        pres = res;
                }
                ldlm_lock_cancel(lock);
                LDLM_LOCK_PUT(lock);
        }
        if (pres != NULL) {
                ldlm_reprocess_all(pres);
                LDLM_RESOURCE_DELREF(pres);
                ldlm_resource_putref(pres);
        }
        LDLM_DEBUG_NOLOCK("server-side cancel handler END");
        RETURN(done);
}

int ldlm_handle_cancel(struct ptlrpc_request *req)
{
        struct ldlm_request *dlm_req;
        int rc;
        ENTRY;

        dlm_req = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        if (dlm_req == NULL) {
                CDEBUG(D_INFO, "bad request buffer for cancel\n");
                RETURN(-EFAULT);
        }

        ldlm_exp_stats_inc(req->rq_export, LDLM_CANCEL);

        rc = req_capsule_server_pack(&req->rq_pill);
        if (rc)
                RETURN(rc);

        if (!ldlm_request_cancel(req, dlm_req, 0))
                req->rq_status = ESTALE;

        RETURN(ptlrpc_reply(req));
}

void ldlm_handle_bl_callback(struct ldlm_namespace *ns,
                             struct ldlm_lock_desc *ld, struct ldlm_lock *lock)
{
        int do_ast;
        ENTRY;

        LDLM_DEBUG(lock, "client blocking AST callback handler");

        lock_res_and_lock(lock);
        lock->l_flags |= LDLM_FL_CBPENDING;

        if (lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK)
                lock->l_flags |= LDLM_FL_CANCEL;

        do_ast = (!lock->l_readers && !lock->l_writers);
        unlock_res_and_lock(lock);

        if (do_ast) {
                CDEBUG(D_DLMTRACE, "Lock %p already unused, calling callback (%p)\n",
                       lock, lock->l_blocking_ast);
                if (lock->l_blocking_ast != NULL)
                        lock->l_blocking_ast(lock, ld, lock->l_ast_data,
                                             LDLM_CB_BLOCKING);
        } else {
                CDEBUG(D_DLMTRACE, "Lock %p is referenced, will be cancelled later\n",
                       lock);
        }

        LDLM_DEBUG(lock, "client blocking callback handler END");
        LDLM_LOCK_RELEASE(lock);
        EXIT;
}

static void ldlm_handle_cp_callback(struct ptlrpc_request *req,
                                    struct ldlm_namespace *ns,
                                    struct ldlm_request *dlm_req,
                                    struct ldlm_lock *lock)
{
        CFS_LIST_HEAD(ast_list);
        ENTRY;

        LDLM_DEBUG(lock, "client completion callback handler START");

        if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_CANCEL_BL_CB_RACE)) {
                int to = cfs_time_seconds(1);
                while (to > 0) {
                        cfs_schedule_timeout_and_set_state(
                                CFS_TASK_INTERRUPTIBLE, to);
                        if (lock->l_granted_mode == lock->l_req_mode ||
                            lock->l_destroyed)
                                break;
                }
        }

        lock_res_and_lock(lock);
        if (lock->l_destroyed ||
            lock->l_granted_mode == lock->l_req_mode) {
                /* bug 11300: the lock has already been granted */
                unlock_res_and_lock(lock);
                LDLM_DEBUG(lock, "Double grant race happened");
                LDLM_LOCK_RELEASE(lock);
                EXIT;
                return;
        }

        /* If we receive the completion AST before the actual enqueue returned,
         * then we might need to switch lock modes, resources, or extents. */
        if (dlm_req->lock_desc.l_granted_mode != lock->l_req_mode) {
                lock->l_req_mode = dlm_req->lock_desc.l_granted_mode;
                LDLM_DEBUG(lock, "completion AST, new lock mode");
        }

        if (lock->l_resource->lr_type != LDLM_PLAIN) {
                lock->l_policy_data = dlm_req->lock_desc.l_policy_data;
                LDLM_DEBUG(lock, "completion AST, new policy data");
        }

        ldlm_resource_unlink_lock(lock);
        if (memcmp(&dlm_req->lock_desc.l_resource.lr_name,
                   &lock->l_resource->lr_name,
                   sizeof(lock->l_resource->lr_name)) != 0) {
                unlock_res_and_lock(lock);
                if (ldlm_lock_change_resource(ns, lock,
                                &dlm_req->lock_desc.l_resource.lr_name) != 0) {
                        LDLM_ERROR(lock, "Failed to allocate resource");
                        LDLM_LOCK_RELEASE(lock);
                        EXIT;
                        return;
                }
                LDLM_DEBUG(lock, "completion AST, new resource");
                CERROR("change resource!\n");
                lock_res_and_lock(lock);
        }

        if (dlm_req->lock_flags & LDLM_FL_AST_SENT) {
                /* BL_AST locks are not needed in lru.
                 * let ldlm_cancel_lru() be fast. */
                ldlm_lock_remove_from_lru(lock);
                lock->l_flags |= LDLM_FL_CBPENDING | LDLM_FL_BL_AST;
                LDLM_DEBUG(lock, "completion AST includes blocking AST");
        }

        if (lock->l_lvb_len) {
                if (req_capsule_get_size(&req->rq_pill, &RMF_DLM_LVB,
                                         RCL_CLIENT) < lock->l_lvb_len) {
                        LDLM_ERROR(lock, "completion AST did not contain "
                                   "expected LVB!");
                } else {
                        void *lvb = req_capsule_client_get(&req->rq_pill,
                                                           &RMF_DLM_LVB);
                        memcpy(lock->l_lvb_data, lvb, lock->l_lvb_len);
                }
        }

        ldlm_grant_lock(lock, &ast_list);
        unlock_res_and_lock(lock);

        LDLM_DEBUG(lock, "callback handler finished, about to run_ast_work");

        /* Let Enqueue to call osc_lock_upcall() and initialize
         * l_ast_data */
        OBD_FAIL_TIMEOUT(OBD_FAIL_OSC_CP_ENQ_RACE, 2);

        ldlm_run_ast_work(&ast_list, LDLM_WORK_CP_AST);

        LDLM_DEBUG_NOLOCK("client completion callback handler END (lock %p)",
                          lock);
        LDLM_LOCK_RELEASE(lock);
        EXIT;
}

static void ldlm_handle_gl_callback(struct ptlrpc_request *req,
                                    struct ldlm_namespace *ns,
                                    struct ldlm_request *dlm_req,
                                    struct ldlm_lock *lock)
{
        int rc = -ENOSYS;
        ENTRY;

        LDLM_DEBUG(lock, "client glimpse AST callback handler");

        if (lock->l_glimpse_ast != NULL)
                rc = lock->l_glimpse_ast(lock, req);

        if (req->rq_repmsg != NULL) {
                ptlrpc_reply(req);
        } else {
                req->rq_status = rc;
                ptlrpc_error(req);
        }

        lock_res_and_lock(lock);
        if (lock->l_granted_mode == LCK_PW &&
            !lock->l_readers && !lock->l_writers &&
            cfs_time_after(cfs_time_current(),
                           cfs_time_add(lock->l_last_used,
                                        cfs_time_seconds(10)))) {
                unlock_res_and_lock(lock);
                if (ldlm_bl_to_thread_lock(ns, NULL, lock))
                        ldlm_handle_bl_callback(ns, NULL, lock);

                EXIT;
                return;
        }
        unlock_res_and_lock(lock);
        LDLM_LOCK_RELEASE(lock);
        EXIT;
}

static int ldlm_callback_reply(struct ptlrpc_request *req, int rc)
{
        if (req->rq_no_reply)
                return 0;

        req->rq_status = rc;
        if (!req->rq_packed_final) {
                rc = lustre_pack_reply(req, 1, NULL, NULL);
                if (rc)
                        return rc;
        }
        return ptlrpc_reply(req);
}

#ifdef __KERNEL__
static int __ldlm_bl_to_thread(struct ldlm_bl_work_item *blwi, int mode)
{
        struct ldlm_bl_pool *blp = ldlm_state->ldlm_bl_pool;
        ENTRY;

        cfs_spin_lock(&blp->blp_lock);
        if (blwi->blwi_lock && blwi->blwi_lock->l_flags & LDLM_FL_DISCARD_DATA) {
                /* add LDLM_FL_DISCARD_DATA requests to the priority list */
                cfs_list_add_tail(&blwi->blwi_entry, &blp->blp_prio_list);
        } else {
                /* other blocking callbacks are added to the regular list */
                cfs_list_add_tail(&blwi->blwi_entry, &blp->blp_list);
        }
        cfs_spin_unlock(&blp->blp_lock);

        cfs_waitq_signal(&blp->blp_waitq);

        /* can not use blwi->blwi_mode as blwi could be already freed in
           LDLM_ASYNC mode */
        if (mode == LDLM_SYNC)
                cfs_wait_for_completion(&blwi->blwi_comp);

        RETURN(0);
}

static inline void init_blwi(struct ldlm_bl_work_item *blwi,
                             struct ldlm_namespace *ns,
                             struct ldlm_lock_desc *ld,
                             cfs_list_t *cancels, int count,
                             struct ldlm_lock *lock,
                             int mode)
{
        cfs_init_completion(&blwi->blwi_comp);
        CFS_INIT_LIST_HEAD(&blwi->blwi_head);

        if (cfs_memory_pressure_get())
                blwi->blwi_mem_pressure = 1;

        blwi->blwi_ns = ns;
        blwi->blwi_mode = mode;
        if (ld != NULL)
                blwi->blwi_ld = *ld;
        if (count) {
                cfs_list_add(&blwi->blwi_head, cancels);
                cfs_list_del_init(cancels);
                blwi->blwi_count = count;
        } else {
                blwi->blwi_lock = lock;
        }
}

static int ldlm_bl_to_thread(struct ldlm_namespace *ns,
                             struct ldlm_lock_desc *ld, struct ldlm_lock *lock,
                             cfs_list_t *cancels, int count, int mode)
{
        ENTRY;

        if (cancels && count == 0)
                RETURN(0);

        if (mode == LDLM_SYNC) {
                /* if it is synchronous call do minimum mem alloc, as it could
                 * be triggered from kernel shrinker
                 */
                struct ldlm_bl_work_item blwi;
                memset(&blwi, 0, sizeof(blwi));
                init_blwi(&blwi, ns, ld, cancels, count, lock, LDLM_SYNC);
                RETURN(__ldlm_bl_to_thread(&blwi, LDLM_SYNC));
        } else {
                struct ldlm_bl_work_item *blwi;
                OBD_ALLOC(blwi, sizeof(*blwi));
                if (blwi == NULL)
                        RETURN(-ENOMEM);
                init_blwi(blwi, ns, ld, cancels, count, lock, LDLM_ASYNC);

                RETURN(__ldlm_bl_to_thread(blwi, LDLM_ASYNC));
        }
}

#endif

int ldlm_bl_to_thread_lock(struct ldlm_namespace *ns, struct ldlm_lock_desc *ld,
                           struct ldlm_lock *lock)
{
#ifdef __KERNEL__
        RETURN(ldlm_bl_to_thread(ns, ld, lock, NULL, 0, LDLM_ASYNC));
#else
        RETURN(-ENOSYS);
#endif
}

int ldlm_bl_to_thread_list(struct ldlm_namespace *ns, struct ldlm_lock_desc *ld,
                           cfs_list_t *cancels, int count, int mode)
{
#ifdef __KERNEL__
        RETURN(ldlm_bl_to_thread(ns, ld, NULL, cancels, count, mode));
#else
        RETURN(-ENOSYS);
#endif
}

/* Setinfo coming from Server (eg MDT) to Client (eg MDC)! */
static int ldlm_handle_setinfo(struct ptlrpc_request *req)
{
        struct obd_device *obd = req->rq_export->exp_obd;
        char *key;
        void *val;
        int keylen, vallen;
        int rc = -ENOSYS;
        ENTRY;

        DEBUG_REQ(D_HSM, req, "%s: handle setinfo\n", obd->obd_name);

        req_capsule_set(&req->rq_pill, &RQF_OBD_SET_INFO);

        key = req_capsule_client_get(&req->rq_pill, &RMF_SETINFO_KEY);
        if (key == NULL) {
                DEBUG_REQ(D_IOCTL, req, "no set_info key");
                RETURN(-EFAULT);
        }
        keylen = req_capsule_get_size(&req->rq_pill, &RMF_SETINFO_KEY,
                                      RCL_CLIENT);
        val = req_capsule_client_get(&req->rq_pill, &RMF_SETINFO_VAL);
        if (val == NULL) {
                DEBUG_REQ(D_IOCTL, req, "no set_info val");
                RETURN(-EFAULT);
        }
        vallen = req_capsule_get_size(&req->rq_pill, &RMF_SETINFO_VAL,
                                      RCL_CLIENT);

        /* We are responsible for swabbing contents of val */

        if (KEY_IS(KEY_HSM_COPYTOOL_SEND))
                /* Pass it on to mdc (the "export" in this case) */
                rc = obd_set_info_async(req->rq_export,
                                        sizeof(KEY_HSM_COPYTOOL_SEND),
                                        KEY_HSM_COPYTOOL_SEND,
                                        vallen, val, NULL);
        else
                DEBUG_REQ(D_WARNING, req, "ignoring unknown key %s", key);

        return rc;
}

static inline void ldlm_callback_errmsg(struct ptlrpc_request *req,
                                        const char *msg, int rc,
                                        struct lustre_handle *handle)
{
        DEBUG_REQ((req->rq_no_reply || rc) ? D_WARNING : D_DLMTRACE, req,
                  "%s: [nid %s] [rc %d] [lock "LPX64"]",
                  msg, libcfs_id2str(req->rq_peer), rc,
                  handle ? handle->cookie : 0);
        if (req->rq_no_reply)
                CWARN("No reply was sent, maybe cause bug 21636.\n");
        else if (rc)
                CWARN("Send reply failed, maybe cause bug 21636.\n");
}

/* TODO: handle requests in a similar way as MDT: see mdt_handle_common() */
static int ldlm_callback_handler(struct ptlrpc_request *req)
{
        struct ldlm_namespace *ns;
        struct ldlm_request *dlm_req;
        struct ldlm_lock *lock;
        int rc;
        ENTRY;

        /* Requests arrive in sender's byte order.  The ptlrpc service
         * handler has already checked and, if necessary, byte-swapped the
         * incoming request message body, but I am responsible for the
         * message buffers. */

        /* do nothing for sec context finalize */
        if (lustre_msg_get_opc(req->rq_reqmsg) == SEC_CTX_FINI)
                RETURN(0);

        req_capsule_init(&req->rq_pill, req, RCL_SERVER);

        if (req->rq_export == NULL) {
                rc = ldlm_callback_reply(req, -ENOTCONN);
                ldlm_callback_errmsg(req, "Operate on unconnected server",
                                     rc, NULL);
                RETURN(0);
        }

        LASSERT(req->rq_export != NULL);
        LASSERT(req->rq_export->exp_obd != NULL);

        switch (lustre_msg_get_opc(req->rq_reqmsg)) {
        case LDLM_BL_CALLBACK:
                if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_BL_CALLBACK))
                        RETURN(0);
                break;
        case LDLM_CP_CALLBACK:
                if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_CP_CALLBACK))
                        RETURN(0);
                break;
        case LDLM_GL_CALLBACK:
                if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_GL_CALLBACK))
                        RETURN(0);
                break;
        case LDLM_SET_INFO:
                rc = ldlm_handle_setinfo(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case OBD_LOG_CANCEL: /* remove this eventually - for 1.4.0 compat */
                CERROR("shouldn't be handling OBD_LOG_CANCEL on DLM thread\n");
                req_capsule_set(&req->rq_pill, &RQF_LOG_CANCEL);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOG_CANCEL_NET))
                        RETURN(0);
                rc = llog_origin_handle_cancel(req);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOG_CANCEL_REP))
                        RETURN(0);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case OBD_QC_CALLBACK:
                req_capsule_set(&req->rq_pill, &RQF_QC_CALLBACK);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_QC_CALLBACK_NET))
                        RETURN(0);
                rc = target_handle_qc_callback(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case QUOTA_DQACQ:
        case QUOTA_DQREL:
                /* reply in handler */
                req_capsule_set(&req->rq_pill, &RQF_MDS_QUOTA_DQACQ);
                rc = target_handle_dqacq_callback(req);
                RETURN(0);
        case LLOG_ORIGIN_HANDLE_CREATE:
                req_capsule_set(&req->rq_pill, &RQF_LLOG_ORIGIN_HANDLE_CREATE);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOGD_NET))
                        RETURN(0);
                rc = llog_origin_handle_create(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case LLOG_ORIGIN_HANDLE_NEXT_BLOCK:
                req_capsule_set(&req->rq_pill,
                                &RQF_LLOG_ORIGIN_HANDLE_NEXT_BLOCK);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOGD_NET))
                        RETURN(0);
                rc = llog_origin_handle_next_block(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case LLOG_ORIGIN_HANDLE_READ_HEADER:
                req_capsule_set(&req->rq_pill,
                                &RQF_LLOG_ORIGIN_HANDLE_READ_HEADER);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOGD_NET))
                        RETURN(0);
                rc = llog_origin_handle_read_header(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        case LLOG_ORIGIN_HANDLE_CLOSE:
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOGD_NET))
                        RETURN(0);
                rc = llog_origin_handle_close(req);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        default:
                CERROR("unknown opcode %u\n",
                       lustre_msg_get_opc(req->rq_reqmsg));
                ldlm_callback_reply(req, -EPROTO);
                RETURN(0);
        }

        ns = req->rq_export->exp_obd->obd_namespace;
        LASSERT(ns != NULL);

        req_capsule_set(&req->rq_pill, &RQF_LDLM_CALLBACK);

        dlm_req = req_capsule_client_get(&req->rq_pill, &RMF_DLM_REQ);
        if (dlm_req == NULL) {
                rc = ldlm_callback_reply(req, -EPROTO);
                ldlm_callback_errmsg(req, "Operate without parameter", rc,
                                     NULL);
                RETURN(0);
        }

        /* Force a known safe race, send a cancel to the server for a lock
         * which the server has already started a blocking callback on. */
        if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_CANCEL_BL_CB_RACE) &&
            lustre_msg_get_opc(req->rq_reqmsg) == LDLM_BL_CALLBACK) {
                rc = ldlm_cli_cancel(&dlm_req->lock_handle[0]);
                if (rc < 0)
                        CERROR("ldlm_cli_cancel: %d\n", rc);
        }

        lock = ldlm_handle2lock_long(&dlm_req->lock_handle[0], 0);
        if (!lock) {
                CDEBUG(D_DLMTRACE, "callback on lock "LPX64" - lock "
                       "disappeared\n", dlm_req->lock_handle[0].cookie);
                rc = ldlm_callback_reply(req, -EINVAL);
                ldlm_callback_errmsg(req, "Operate with invalid parameter", rc,
                                     &dlm_req->lock_handle[0]);
                RETURN(0);
        }

        if ((lock->l_flags & LDLM_FL_FAIL_LOC) &&
            lustre_msg_get_opc(req->rq_reqmsg) == LDLM_BL_CALLBACK)
                OBD_RACE(OBD_FAIL_LDLM_CP_BL_RACE);

        /* Copy hints/flags (e.g. LDLM_FL_DISCARD_DATA) from AST. */
        lock_res_and_lock(lock);
        lock->l_flags |= (dlm_req->lock_flags & LDLM_AST_FLAGS);
        if (lustre_msg_get_opc(req->rq_reqmsg) == LDLM_BL_CALLBACK) {
                /* If somebody cancels lock and cache is already dropped,
                 * or lock is failed before cp_ast received on client,
                 * we can tell the server we have no lock. Otherwise, we
                 * should send cancel after dropping the cache. */
                if (((lock->l_flags & LDLM_FL_CANCELING) &&
                    (lock->l_flags & LDLM_FL_BL_DONE)) ||
                    (lock->l_flags & LDLM_FL_FAILED)) {
                        LDLM_DEBUG(lock, "callback on lock "
                                   LPX64" - lock disappeared\n",
                                   dlm_req->lock_handle[0].cookie);
                        unlock_res_and_lock(lock);
                        LDLM_LOCK_RELEASE(lock);
                        rc = ldlm_callback_reply(req, -EINVAL);
                        ldlm_callback_errmsg(req, "Operate on stale lock", rc,
                                             &dlm_req->lock_handle[0]);
                        RETURN(0);
                }
                /* BL_AST locks are not needed in lru.
                 * let ldlm_cancel_lru() be fast. */
                ldlm_lock_remove_from_lru(lock);
                lock->l_flags |= LDLM_FL_BL_AST;
        }
        unlock_res_and_lock(lock);

        /* We want the ost thread to get this reply so that it can respond
         * to ost requests (write cache writeback) that might be triggered
         * in the callback.
         *
         * But we'd also like to be able to indicate in the reply that we're
         * cancelling right now, because it's unused, or have an intent result
         * in the reply, so we might have to push the responsibility for sending
         * the reply down into the AST handlers, alas. */

        switch (lustre_msg_get_opc(req->rq_reqmsg)) {
        case LDLM_BL_CALLBACK:
                CDEBUG(D_INODE, "blocking ast\n");
                req_capsule_extend(&req->rq_pill, &RQF_LDLM_BL_CALLBACK);
                if (!(lock->l_flags & LDLM_FL_CANCEL_ON_BLOCK)) {
                        rc = ldlm_callback_reply(req, 0);
                        if (req->rq_no_reply || rc)
                                ldlm_callback_errmsg(req, "Normal process", rc,
                                                     &dlm_req->lock_handle[0]);
                }
                if (ldlm_bl_to_thread_lock(ns, &dlm_req->lock_desc, lock))
                        ldlm_handle_bl_callback(ns, &dlm_req->lock_desc, lock);
                break;
        case LDLM_CP_CALLBACK:
                CDEBUG(D_INODE, "completion ast\n");
                req_capsule_extend(&req->rq_pill, &RQF_LDLM_CP_CALLBACK);
                ldlm_callback_reply(req, 0);
                ldlm_handle_cp_callback(req, ns, dlm_req, lock);
                break;
        case LDLM_GL_CALLBACK:
                CDEBUG(D_INODE, "glimpse ast\n");
                req_capsule_extend(&req->rq_pill, &RQF_LDLM_GL_CALLBACK);
                ldlm_handle_gl_callback(req, ns, dlm_req, lock);
                break;
        default:
                LBUG();                         /* checked above */
        }

        RETURN(0);
}

static int ldlm_cancel_handler(struct ptlrpc_request *req)
{
        int rc;
        ENTRY;

        /* Requests arrive in sender's byte order.  The ptlrpc service
         * handler has already checked and, if necessary, byte-swapped the
         * incoming request message body, but I am responsible for the
         * message buffers. */

        req_capsule_init(&req->rq_pill, req, RCL_SERVER);

        if (req->rq_export == NULL) {
                struct ldlm_request *dlm_req;

                CERROR("%s from %s arrived at %lu with bad export cookie "
                       LPU64"\n",
                       ll_opcode2str(lustre_msg_get_opc(req->rq_reqmsg)),
                       libcfs_nid2str(req->rq_peer.nid),
                       req->rq_arrival_time.tv_sec,
                       lustre_msg_get_handle(req->rq_reqmsg)->cookie);

                if (lustre_msg_get_opc(req->rq_reqmsg) == LDLM_CANCEL) {
                        req_capsule_set(&req->rq_pill, &RQF_LDLM_CALLBACK);
                        dlm_req = req_capsule_client_get(&req->rq_pill,
                                                         &RMF_DLM_REQ);
                        if (dlm_req != NULL)
                                ldlm_lock_dump_handle(D_ERROR,
                                                      &dlm_req->lock_handle[0]);
                }
                ldlm_callback_reply(req, -ENOTCONN);
                RETURN(0);
        }

        switch (lustre_msg_get_opc(req->rq_reqmsg)) {

        /* XXX FIXME move this back to mds/handler.c, bug 249 */
        case LDLM_CANCEL:
                req_capsule_set(&req->rq_pill, &RQF_LDLM_CANCEL);
                CDEBUG(D_INODE, "cancel\n");
                if (OBD_FAIL_CHECK(OBD_FAIL_LDLM_CANCEL))
                        RETURN(0);
                rc = ldlm_handle_cancel(req);
                if (rc)
                        break;
                RETURN(0);
        case OBD_LOG_CANCEL:
                req_capsule_set(&req->rq_pill, &RQF_LOG_CANCEL);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOG_CANCEL_NET))
                        RETURN(0);
                rc = llog_origin_handle_cancel(req);
                if (OBD_FAIL_CHECK(OBD_FAIL_OBD_LOG_CANCEL_REP))
                        RETURN(0);
                ldlm_callback_reply(req, rc);
                RETURN(0);
        default:
                CERROR("invalid opcode %d\n",
                       lustre_msg_get_opc(req->rq_reqmsg));
                req_capsule_set(&req->rq_pill, &RQF_LDLM_CALLBACK);
                ldlm_callback_reply(req, -EINVAL);
        }

        RETURN(0);
}

int ldlm_revoke_lock_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
                        cfs_hlist_node_t *hnode, void *data)

{
        cfs_list_t         *rpc_list = data;
        struct ldlm_lock   *lock = cfs_hash_object(hs, hnode);

        lock_res_and_lock(lock);

        if (lock->l_req_mode != lock->l_granted_mode) {
                unlock_res_and_lock(lock);
                return 0;
        }

        LASSERT(lock->l_resource);
        if (lock->l_resource->lr_type != LDLM_IBITS &&
            lock->l_resource->lr_type != LDLM_PLAIN) {
                unlock_res_and_lock(lock);
                return 0;
        }

        if (lock->l_flags & LDLM_FL_AST_SENT) {
                unlock_res_and_lock(lock);
                return 0;
        }

        LASSERT(lock->l_blocking_ast);
        LASSERT(!lock->l_blocking_lock);

        lock->l_flags |= LDLM_FL_AST_SENT;
        if (lock->l_export && lock->l_export->exp_lock_hash &&
            !cfs_hlist_unhashed(&lock->l_exp_hash))
                cfs_hash_del(lock->l_export->exp_lock_hash,
                             &lock->l_remote_handle, &lock->l_exp_hash);
        cfs_list_add_tail(&lock->l_rk_ast, rpc_list);
        LDLM_LOCK_GET(lock);

        unlock_res_and_lock(lock);
        return 0;
}

void ldlm_revoke_export_locks(struct obd_export *exp)
{
        cfs_list_t  rpc_list;
        ENTRY;

        CFS_INIT_LIST_HEAD(&rpc_list);
        cfs_hash_for_each_empty(exp->exp_lock_hash,
                                ldlm_revoke_lock_cb, &rpc_list);
        ldlm_run_ast_work(&rpc_list, LDLM_WORK_REVOKE_AST);

        EXIT;
}

#ifdef __KERNEL__
static struct ldlm_bl_work_item *ldlm_bl_get_work(struct ldlm_bl_pool *blp)
{
        struct ldlm_bl_work_item *blwi = NULL;
        static unsigned int num_bl = 0;

        cfs_spin_lock(&blp->blp_lock);
        /* process a request from the blp_list at least every blp_num_threads */
        if (!cfs_list_empty(&blp->blp_list) &&
            (cfs_list_empty(&blp->blp_prio_list) || num_bl == 0))
                blwi = cfs_list_entry(blp->blp_list.next,
                                      struct ldlm_bl_work_item, blwi_entry);
        else
                if (!cfs_list_empty(&blp->blp_prio_list))
                        blwi = cfs_list_entry(blp->blp_prio_list.next,
                                              struct ldlm_bl_work_item,
                                              blwi_entry);

        if (blwi) {
                if (++num_bl >= cfs_atomic_read(&blp->blp_num_threads))
                        num_bl = 0;
                cfs_list_del(&blwi->blwi_entry);
        }
        cfs_spin_unlock(&blp->blp_lock);

        return blwi;
}

/* This only contains temporary data until the thread starts */
struct ldlm_bl_thread_data {
        char                    bltd_name[CFS_CURPROC_COMM_MAX];
        struct ldlm_bl_pool     *bltd_blp;
        cfs_completion_t        bltd_comp;
        int                     bltd_num;
};

static int ldlm_bl_thread_main(void *arg);

static int ldlm_bl_thread_start(struct ldlm_bl_pool *blp)
{
        struct ldlm_bl_thread_data bltd = { .bltd_blp = blp };
        int rc;

        cfs_init_completion(&bltd.bltd_comp);
        rc = cfs_kernel_thread(ldlm_bl_thread_main, &bltd, 0);
        if (rc < 0) {
                CERROR("cannot start LDLM thread ldlm_bl_%02d: rc %d\n",
                       cfs_atomic_read(&blp->blp_num_threads), rc);
                return rc;
        }
        cfs_wait_for_completion(&bltd.bltd_comp);

        return 0;
}

static int ldlm_bl_thread_main(void *arg)
{
        struct ldlm_bl_pool *blp;
        ENTRY;

        {
                struct ldlm_bl_thread_data *bltd = arg;

                blp = bltd->bltd_blp;

                bltd->bltd_num =
                        cfs_atomic_inc_return(&blp->blp_num_threads) - 1;
                cfs_atomic_inc(&blp->blp_busy_threads);

                snprintf(bltd->bltd_name, sizeof(bltd->bltd_name) - 1,
                        "ldlm_bl_%02d", bltd->bltd_num);
                cfs_daemonize(bltd->bltd_name);

                cfs_complete(&bltd->bltd_comp);
                /* cannot use bltd after this, it is only on caller's stack */
        }

        while (1) {
                struct l_wait_info lwi = { 0 };
                struct ldlm_bl_work_item *blwi = NULL;

                blwi = ldlm_bl_get_work(blp);

                if (blwi == NULL) {
                        int busy;

                        cfs_atomic_dec(&blp->blp_busy_threads);
                        l_wait_event_exclusive(blp->blp_waitq,
                                         (blwi = ldlm_bl_get_work(blp)) != NULL,
                                         &lwi);
                        busy = cfs_atomic_inc_return(&blp->blp_busy_threads);

                        if (blwi->blwi_ns == NULL)
                                /* added by ldlm_cleanup() */
                                break;

                        /* Not fatal if racy and have a few too many threads */
                        if (unlikely(busy < blp->blp_max_threads &&
                            busy >= cfs_atomic_read(&blp->blp_num_threads) &&
                            !blwi->blwi_mem_pressure))
                                /* discard the return value, we tried */
                                ldlm_bl_thread_start(blp);
                } else {
                        if (blwi->blwi_ns == NULL)
                                /* added by ldlm_cleanup() */
                                break;
                }
                if (blwi->blwi_mem_pressure)
                        cfs_memory_pressure_set();

                if (blwi->blwi_count) {
                        int count;
                        /* The special case when we cancel locks in lru
                         * asynchronously, we pass the list of locks here.
                         * Thus locks are marked LDLM_FL_CANCELING, but NOT
                         * canceled locally yet. */
                        count = ldlm_cli_cancel_list_local(&blwi->blwi_head,
                                                           blwi->blwi_count,
                                                           LCF_BL_AST);
                        ldlm_cli_cancel_list(&blwi->blwi_head, count, NULL, 0);
                } else {
                        ldlm_handle_bl_callback(blwi->blwi_ns, &blwi->blwi_ld,
                                                blwi->blwi_lock);
                }
                if (blwi->blwi_mem_pressure)
                        cfs_memory_pressure_clr();

                if (blwi->blwi_mode == LDLM_ASYNC)
                        OBD_FREE(blwi, sizeof(*blwi));
                else
                        cfs_complete(&blwi->blwi_comp);
        }

        cfs_atomic_dec(&blp->blp_busy_threads);
        cfs_atomic_dec(&blp->blp_num_threads);
        cfs_complete(&blp->blp_comp);
        RETURN(0);
}

#endif

static int ldlm_setup(void);
static int ldlm_cleanup(void);

int ldlm_get_ref(void)
{
        int rc = 0;
        ENTRY;
        cfs_mutex_down(&ldlm_ref_sem);
        if (++ldlm_refcount == 1) {
                rc = ldlm_setup();
                if (rc)
                        ldlm_refcount--;
        }
        cfs_mutex_up(&ldlm_ref_sem);

        RETURN(rc);
}

void ldlm_put_ref(void)
{
        ENTRY;
        cfs_mutex_down(&ldlm_ref_sem);
        if (ldlm_refcount == 1) {
                int rc = ldlm_cleanup();
                if (rc)
                        CERROR("ldlm_cleanup failed: %d\n", rc);
                else
                        ldlm_refcount--;
        } else {
                ldlm_refcount--;
        }
        cfs_mutex_up(&ldlm_ref_sem);

        EXIT;
}

/*
 * Export handle<->lock hash operations.
 */
static unsigned
ldlm_export_lock_hash(cfs_hash_t *hs, void *key, unsigned mask)
{
        return cfs_hash_u64_hash(((struct lustre_handle *)key)->cookie, mask);
}

static void *
ldlm_export_lock_key(cfs_hlist_node_t *hnode)
{
        struct ldlm_lock *lock;

        lock = cfs_hlist_entry(hnode, struct ldlm_lock, l_exp_hash);
        return &lock->l_remote_handle;
}

static void
ldlm_export_lock_keycpy(cfs_hlist_node_t *hnode, void *key)
{
        struct ldlm_lock     *lock;

        lock = cfs_hlist_entry(hnode, struct ldlm_lock, l_exp_hash);
        lock->l_remote_handle = *(struct lustre_handle *)key;
}

static int
ldlm_export_lock_keycmp(void *key, cfs_hlist_node_t *hnode)
{
        return lustre_handle_equal(ldlm_export_lock_key(hnode), key);
}

static void *
ldlm_export_lock_object(cfs_hlist_node_t *hnode)
{
        return cfs_hlist_entry(hnode, struct ldlm_lock, l_exp_hash);
}

static void
ldlm_export_lock_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        struct ldlm_lock *lock;

        lock = cfs_hlist_entry(hnode, struct ldlm_lock, l_exp_hash);
        LDLM_LOCK_GET(lock);
}

static void
ldlm_export_lock_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        struct ldlm_lock *lock;

        lock = cfs_hlist_entry(hnode, struct ldlm_lock, l_exp_hash);
        LDLM_LOCK_RELEASE(lock);
}

static cfs_hash_ops_t ldlm_export_lock_ops = {
        .hs_hash        = ldlm_export_lock_hash,
        .hs_key         = ldlm_export_lock_key,
        .hs_keycmp      = ldlm_export_lock_keycmp,
        .hs_keycpy      = ldlm_export_lock_keycpy,
        .hs_object      = ldlm_export_lock_object,
        .hs_get         = ldlm_export_lock_get,
        .hs_put         = ldlm_export_lock_put,
        .hs_put_locked  = ldlm_export_lock_put,
};

int ldlm_init_export(struct obd_export *exp)
{
        ENTRY;

        exp->exp_lock_hash =
                cfs_hash_create(obd_uuid2str(&exp->exp_client_uuid),
                                HASH_EXP_LOCK_CUR_BITS,
                                HASH_EXP_LOCK_MAX_BITS,
                                HASH_EXP_LOCK_BKT_BITS, 0,
                                CFS_HASH_MIN_THETA, CFS_HASH_MAX_THETA,
                                &ldlm_export_lock_ops,
                                CFS_HASH_DEFAULT | CFS_HASH_REHASH_KEY |
                                CFS_HASH_NBLK_CHANGE);

        if (!exp->exp_lock_hash)
                RETURN(-ENOMEM);

        RETURN(0);
}
EXPORT_SYMBOL(ldlm_init_export);

void ldlm_destroy_export(struct obd_export *exp)
{
        ENTRY;
        cfs_hash_putref(exp->exp_lock_hash);
        exp->exp_lock_hash = NULL;
        EXIT;
}
EXPORT_SYMBOL(ldlm_destroy_export);

static ptlrpc_svc_ops_t ldlm_cbd_svc_ops = {
        .sop_thread_init        = NULL,
        .sop_thread_done        = NULL,
        .sop_req_dispatcher     = NULL,
        .sop_req_handler        = ldlm_callback_handler,
        .sop_hpreq_handler      = NULL,
        .sop_req_printer        = NULL,
};

static ptlrpc_svc_ops_t ldlm_canceld_svc_ops = {
        .sop_thread_init        = NULL,
        .sop_thread_done        = NULL,
        .sop_req_dispatcher     = NULL,
        .sop_req_handler        = ldlm_cancel_handler,
        .sop_hpreq_handler      = NULL,
        .sop_req_printer        = NULL,
};

static int ldlm_setup(void)
{
        struct ldlm_bl_pool *blp;
        int rc = 0;
        int ldlm_min_threads = LDLM_THREADS_MIN;
        int ldlm_max_threads = LDLM_THREADS_MAX;
        int ldlm_cpu_min_threads = LDLM_CPU_THREADS_MIN;
        int ldlm_cpu_max_threads = LDLM_CPU_THREADS_MAX(LDLM_THREADS_MAX);
#ifdef __KERNEL__
        int i;
#endif
        ENTRY;

        if (ldlm_state != NULL)
                RETURN(-EALREADY);

        OBD_ALLOC(ldlm_state, sizeof(*ldlm_state));
        if (ldlm_state == NULL)
                RETURN(-ENOMEM);

#ifdef LPROCFS
        rc = ldlm_proc_setup();
        if (rc != 0)
                GOTO(out_free, rc);
#endif

#ifdef __KERNEL__
        if (ldlm_num_threads) {
                /* If ldlm_num_threads is set, it is the min and the max. */
                if (ldlm_num_threads > PTLRPC_THREADS_MAX)
                        ldlm_num_threads = PTLRPC_THREADS_MAX;
                if (ldlm_num_threads < PTLRPC_THREADS_MIN(0))
                        ldlm_num_threads = PTLRPC_THREADS_MIN(0);
                ldlm_min_threads =
                ldlm_max_threads = ldlm_num_threads;
                ldlm_cpu_min_threads =
                ldlm_cpu_max_threads = LDLM_CPU_THREADS_MAX(ldlm_num_threads);
        }
#endif

        ldlm_state->ldlm_cb_service =
                ptlrpc_init_svc("ldlm_cbd", "ldlm_cb",
                                &ldlm_cbd_svc_ops, 0, 0,
                                ldlm_min_threads, ldlm_max_threads,
                                LDLM_CB_REQUEST_PORTAL, LDLM_CB_REPLY_PORTAL,
                                LDLM_NBUFS, LDLM_BUFSIZE,
                                LDLM_MAXREQSIZE, LDLM_MAXREPSIZE,
                                2, ldlm_svc_proc_dir,
                                LCT_MD_THREAD|LCT_DT_THREAD);

        if (!ldlm_state->ldlm_cb_service) {
                CERROR("failed to start service\n");
                GOTO(out_proc, rc = -ENOMEM);
        }

        ldlm_state->ldlm_cancel_service =
                ptlrpc_init_svc("ldlm_canceld", "ldlm_cn",
                                &ldlm_canceld_svc_ops, 1, 0,
                                ldlm_cpu_min_threads, ldlm_cpu_max_threads,
                                LDLM_CANCEL_REQUEST_PORTAL,
                                LDLM_CANCEL_REPLY_PORTAL,
                                LDLM_CPU_NBUFS, LDLM_BUFSIZE,
                                LDLM_MAXREQSIZE, LDLM_MAXREPSIZE,
                                6, ldlm_svc_proc_dir,
                                LCT_MD_THREAD|LCT_DT_THREAD|LCT_CL_THREAD);

        if (!ldlm_state->ldlm_cancel_service) {
                CERROR("failed to start service\n");
                GOTO(out_proc, rc = -ENOMEM);
        }

        OBD_ALLOC(blp, sizeof(*blp));
        if (blp == NULL)
                GOTO(out_proc, rc = -ENOMEM);
        ldlm_state->ldlm_bl_pool = blp;

        cfs_spin_lock_init(&blp->blp_lock);
        CFS_INIT_LIST_HEAD(&blp->blp_list);
        CFS_INIT_LIST_HEAD(&blp->blp_prio_list);
        cfs_waitq_init(&blp->blp_waitq);
        cfs_atomic_set(&blp->blp_num_threads, 0);
        cfs_atomic_set(&blp->blp_busy_threads, 0);
        blp->blp_min_threads = ldlm_min_threads;
        blp->blp_max_threads = ldlm_max_threads;

#ifdef __KERNEL__
        for (i = 0; i < blp->blp_min_threads; i++) {
                rc = ldlm_bl_thread_start(blp);
                if (rc < 0)
                        GOTO(out_thread, rc);
        }

        rc = ptlrpc_start_threads(ldlm_state->ldlm_cancel_service);
        if (rc)
                GOTO(out_thread, rc);

        rc = ptlrpc_start_threads(ldlm_state->ldlm_cb_service);
        if (rc)
                GOTO(out_thread, rc);

        CFS_INIT_LIST_HEAD(&ldlm_waitd.lwd_expired_locks);
        ldlm_waitd.lwd_thread_state = ELT_STOPPED;
        cfs_waitq_init(&ldlm_waitd.lwd_waitq);
        cfs_spin_lock_init(&ldlm_waitd.lwd_lock);

        ldlm_waitd.lwd_wtimer = cfs_array_alloc(LDLM_WTIMER_HASH_SIZE,
                                                sizeof(ldlm_wait_timer_t), 1);
        if (ldlm_waitd.lwd_wtimer == NULL) {
                CERROR("Failed to allocate lock waiting timers\n");
                GOTO(out_thread, rc);
        }

        for (i = 0; i < LDLM_WTIMER_HASH_SIZE; i++) {
                ldlm_wait_timer_t *wtimer = ldlm_waitd.lwd_wtimer[i];

                cfs_spin_lock_init(&wtimer->lwt_lock);
                CFS_INIT_LIST_HEAD(&wtimer->lwt_waiting_locks);
                cfs_timer_init(&wtimer->lwt_timer,
                               waiting_locks_callback, wtimer);
        }

        rc = cfs_kernel_thread(expired_lock_main, NULL, CLONE_VM | CLONE_FILES);
        if (rc < 0) {
                CERROR("Cannot start ldlm expired-lock thread: %d\n", rc);
                GOTO(out_thread, rc);
        }

        cfs_wait_event(ldlm_waitd.lwd_waitq,
                       ldlm_waitd.lwd_thread_state == ELT_READY);
#endif

#ifdef __KERNEL__
        rc = ldlm_pools_init();
        if (rc)
                GOTO(out_thread, rc);
#endif
        RETURN(0);

#ifdef __KERNEL__
 out_thread:
        ptlrpc_unregister_service(ldlm_state->ldlm_cancel_service);
        ptlrpc_unregister_service(ldlm_state->ldlm_cb_service);
        if (ldlm_waitd.lwd_wtimer != NULL) {
                cfs_array_free(ldlm_waitd.lwd_wtimer);
                ldlm_waitd.lwd_wtimer = NULL;
        }
#endif

 out_proc:
#ifdef LPROCFS
        ldlm_proc_cleanup();
 out_free:
#endif
        OBD_FREE(ldlm_state, sizeof(*ldlm_state));
        ldlm_state = NULL;
        return rc;
}

static int ldlm_cleanup(void)
{
#ifdef __KERNEL__
        struct ldlm_bl_pool *blp = ldlm_state->ldlm_bl_pool;
#endif
        ENTRY;

        if (!cfs_list_empty(ldlm_namespace_list(LDLM_NAMESPACE_SERVER)) ||
            !cfs_list_empty(ldlm_namespace_list(LDLM_NAMESPACE_CLIENT))) {
                CERROR("ldlm still has namespaces; clean these up first.\n");
                ldlm_dump_all_namespaces(LDLM_NAMESPACE_SERVER, D_DLMTRACE);
                ldlm_dump_all_namespaces(LDLM_NAMESPACE_CLIENT, D_DLMTRACE);
                RETURN(-EBUSY);
        }

#ifdef __KERNEL__
        ldlm_pools_fini();
#endif

#ifdef __KERNEL__
        while (cfs_atomic_read(&blp->blp_num_threads) > 0) {
                struct ldlm_bl_work_item blwi = { .blwi_ns = NULL };

                cfs_init_completion(&blp->blp_comp);

                cfs_spin_lock(&blp->blp_lock);
                cfs_list_add_tail(&blwi.blwi_entry, &blp->blp_list);
                cfs_waitq_signal(&blp->blp_waitq);
                cfs_spin_unlock(&blp->blp_lock);

                cfs_wait_for_completion(&blp->blp_comp);
        }
        OBD_FREE(blp, sizeof(*blp));

        ptlrpc_unregister_service(ldlm_state->ldlm_cb_service);
        ptlrpc_unregister_service(ldlm_state->ldlm_cancel_service);
        ldlm_proc_cleanup();

        ldlm_waitd.lwd_thread_state = ELT_TERMINATE;
        cfs_waitq_signal(&ldlm_waitd.lwd_waitq);
        cfs_wait_event(ldlm_waitd.lwd_waitq,
                       ldlm_waitd.lwd_thread_state == ELT_STOPPED);
        cfs_array_free(ldlm_waitd.lwd_wtimer);
#else
        ptlrpc_unregister_service(ldlm_state->ldlm_cb_service);
        ptlrpc_unregister_service(ldlm_state->ldlm_cancel_service);
#endif

        OBD_FREE(ldlm_state, sizeof(*ldlm_state));
        ldlm_state = NULL;

        RETURN(0);
}

int __init ldlm_init(void)
{
        cfs_init_mutex(&ldlm_ref_sem);
        cfs_init_mutex(ldlm_namespace_lock(LDLM_NAMESPACE_SERVER));
        cfs_init_mutex(ldlm_namespace_lock(LDLM_NAMESPACE_CLIENT));
        ldlm_resource_slab = cfs_mem_cache_create("ldlm_resources",
                                               sizeof(struct ldlm_resource), 0,
                                               CFS_SLAB_HWCACHE_ALIGN);
        if (ldlm_resource_slab == NULL)
                return -ENOMEM;

        ldlm_lock_slab = cfs_mem_cache_create("ldlm_locks",
                              sizeof(struct ldlm_lock), 0,
                              CFS_SLAB_HWCACHE_ALIGN | CFS_SLAB_DESTROY_BY_RCU);
        if (ldlm_lock_slab == NULL) {
                cfs_mem_cache_destroy(ldlm_resource_slab);
                return -ENOMEM;
        }

        ldlm_interval_slab = cfs_mem_cache_create("interval_node",
                                        sizeof(struct ldlm_interval),
                                        0, CFS_SLAB_HWCACHE_ALIGN);
        if (ldlm_interval_slab == NULL) {
                cfs_mem_cache_destroy(ldlm_resource_slab);
                cfs_mem_cache_destroy(ldlm_lock_slab);
                return -ENOMEM;
        }
#if LUSTRE_TRACKS_LOCK_EXP_REFS
        class_export_dump_hook = ldlm_dump_export_locks;
#endif
        return 0;
}

void __exit ldlm_exit(void)
{
        int rc;
        if (ldlm_refcount)
                CERROR("ldlm_refcount is %d in ldlm_exit!\n", ldlm_refcount);
        rc = cfs_mem_cache_destroy(ldlm_resource_slab);
        LASSERTF(rc == 0, "couldn't free ldlm resource slab\n");
#ifdef __KERNEL__
        /* ldlm_lock_put() use RCU to call ldlm_lock_free, so need call
         * synchronize_rcu() to wait a grace period elapsed, so that
         * ldlm_lock_free() get a chance to be called. */
        synchronize_rcu();
#endif
        rc = cfs_mem_cache_destroy(ldlm_lock_slab);
        LASSERTF(rc == 0, "couldn't free ldlm lock slab\n");
        rc = cfs_mem_cache_destroy(ldlm_interval_slab);
        LASSERTF(rc == 0, "couldn't free interval node slab\n");
}

/* ldlm_extent.c */
EXPORT_SYMBOL(ldlm_extent_shift_kms);

/* ldlm_lock.c */
EXPORT_SYMBOL(ldlm_get_processing_policy);
EXPORT_SYMBOL(ldlm_lock2desc);
EXPORT_SYMBOL(ldlm_register_intent);
EXPORT_SYMBOL(ldlm_lockname);
EXPORT_SYMBOL(ldlm_typename);
EXPORT_SYMBOL(ldlm_lock2handle);
EXPORT_SYMBOL(__ldlm_handle2lock);
EXPORT_SYMBOL(ldlm_lock_get);
EXPORT_SYMBOL(ldlm_lock_put);
EXPORT_SYMBOL(ldlm_lock_match);
EXPORT_SYMBOL(ldlm_lock_cancel);
EXPORT_SYMBOL(ldlm_lock_addref);
EXPORT_SYMBOL(ldlm_lock_addref_try);
EXPORT_SYMBOL(ldlm_lock_decref);
EXPORT_SYMBOL(ldlm_lock_decref_and_cancel);
EXPORT_SYMBOL(ldlm_lock_change_resource);
EXPORT_SYMBOL(ldlm_it2str);
EXPORT_SYMBOL(ldlm_lock_dump);
EXPORT_SYMBOL(ldlm_lock_dump_handle);
EXPORT_SYMBOL(ldlm_reprocess_all_ns);
EXPORT_SYMBOL(ldlm_lock_allow_match_locked);
EXPORT_SYMBOL(ldlm_lock_allow_match);
EXPORT_SYMBOL(ldlm_lock_downgrade);
EXPORT_SYMBOL(ldlm_lock_convert);

/* ldlm_request.c */
EXPORT_SYMBOL(ldlm_completion_ast_async);
EXPORT_SYMBOL(ldlm_blocking_ast_nocheck);
EXPORT_SYMBOL(ldlm_completion_ast);
EXPORT_SYMBOL(ldlm_blocking_ast);
EXPORT_SYMBOL(ldlm_glimpse_ast);
EXPORT_SYMBOL(ldlm_expired_completion_wait);
EXPORT_SYMBOL(ldlm_prep_enqueue_req);
EXPORT_SYMBOL(ldlm_prep_elc_req);
EXPORT_SYMBOL(ldlm_cli_convert);
EXPORT_SYMBOL(ldlm_cli_enqueue);
EXPORT_SYMBOL(ldlm_cli_enqueue_fini);
EXPORT_SYMBOL(ldlm_cli_enqueue_local);
EXPORT_SYMBOL(ldlm_cli_cancel);
EXPORT_SYMBOL(ldlm_cli_cancel_unused);
EXPORT_SYMBOL(ldlm_cli_cancel_unused_resource);
EXPORT_SYMBOL(ldlm_cli_cancel_req);
EXPORT_SYMBOL(ldlm_replay_locks);
EXPORT_SYMBOL(ldlm_resource_foreach);
EXPORT_SYMBOL(ldlm_namespace_foreach);
EXPORT_SYMBOL(ldlm_resource_iterate);
EXPORT_SYMBOL(ldlm_cancel_resource_local);
EXPORT_SYMBOL(ldlm_cli_cancel_list_local);
EXPORT_SYMBOL(ldlm_cli_cancel_list);

/* ldlm_lockd.c */
EXPORT_SYMBOL(ldlm_server_blocking_ast);
EXPORT_SYMBOL(ldlm_server_completion_ast);
EXPORT_SYMBOL(ldlm_server_glimpse_ast);
EXPORT_SYMBOL(ldlm_handle_enqueue);
EXPORT_SYMBOL(ldlm_handle_enqueue0);
EXPORT_SYMBOL(ldlm_handle_cancel);
EXPORT_SYMBOL(ldlm_request_cancel);
EXPORT_SYMBOL(ldlm_handle_convert);
EXPORT_SYMBOL(ldlm_handle_convert0);
EXPORT_SYMBOL(ldlm_del_waiting_lock);
EXPORT_SYMBOL(ldlm_get_ref);
EXPORT_SYMBOL(ldlm_put_ref);
EXPORT_SYMBOL(ldlm_refresh_waiting_lock);
EXPORT_SYMBOL(ldlm_revoke_export_locks);

/* ldlm_resource.c */
EXPORT_SYMBOL(ldlm_namespace_new);
EXPORT_SYMBOL(ldlm_namespace_cleanup);
EXPORT_SYMBOL(ldlm_namespace_free);
EXPORT_SYMBOL(ldlm_namespace_dump);
EXPORT_SYMBOL(ldlm_dump_all_namespaces);
EXPORT_SYMBOL(ldlm_resource_get);
EXPORT_SYMBOL(ldlm_resource_putref);
EXPORT_SYMBOL(ldlm_resource_unlink_lock);

/* ldlm_lib.c */
EXPORT_SYMBOL(client_import_add_conn);
EXPORT_SYMBOL(client_import_del_conn);
EXPORT_SYMBOL(client_obd_setup);
EXPORT_SYMBOL(client_obd_cleanup);
EXPORT_SYMBOL(client_connect_import);
EXPORT_SYMBOL(client_disconnect_export);
EXPORT_SYMBOL(server_disconnect_export);
EXPORT_SYMBOL(target_stop_recovery_thread);
EXPORT_SYMBOL(target_handle_connect);
EXPORT_SYMBOL(target_cleanup_recovery);
EXPORT_SYMBOL(target_destroy_export);
EXPORT_SYMBOL(target_cancel_recovery_timer);
EXPORT_SYMBOL(target_send_reply);
EXPORT_SYMBOL(target_queue_recovery_request);
EXPORT_SYMBOL(target_handle_ping);
EXPORT_SYMBOL(target_pack_pool_reply);
EXPORT_SYMBOL(target_handle_disconnect);

/* l_lock.c */
EXPORT_SYMBOL(lock_res_and_lock);
EXPORT_SYMBOL(unlock_res_and_lock);
