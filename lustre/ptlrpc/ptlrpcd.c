/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2001-2003 Cluster File Systems, Inc.
 *   Author Peter Braam <braam@clusterfs.com>
 *
 *   This file is part of Lustre, http://www.lustre.org.
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#define DEBUG_SUBSYSTEM S_RPC

#ifdef __KERNEL__
# include <linux/version.h>
# include <linux/module.h>
# include <linux/mm.h>
# include <linux/highmem.h>
# include <linux/lustre_dlm.h>
# if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
#  include <linux/workqueue.h>
#  include <linux/smp_lock.h>
# else
#  include <linux/locks.h>
# endif
# include <linux/ctype.h>
# include <linux/init.h>
#else /* __KERNEL__ */
# include <liblustre.h>
# include <ctype.h>
#endif

#include <libcfs/kp30.h>
#include <linux/lustre_net.h>

#include <linux/lustre_ha.h>
#include <linux/obd_support.h> /* for OBD_FAIL_CHECK */
#include <linux/lprocfs_status.h>

#define LIOD_STOP 0
struct ptlrpcd_ctl {
        unsigned long             pc_flags;
        spinlock_t                pc_lock;
        struct completion         pc_starting;
        struct completion         pc_finishing;
        struct list_head          pc_req_list;
        wait_queue_head_t         pc_waitq;
        struct ptlrpc_request_set *pc_set;
        char                      pc_name[16];
#ifndef __KERNEL__
        int                       pc_recurred;
        void                     *pc_callback;
#endif
};

static struct ptlrpcd_ctl ptlrpcd_pc;
static struct ptlrpcd_ctl ptlrpcd_recovery_pc;

static DECLARE_MUTEX(ptlrpcd_sem);
static int ptlrpcd_users = 0;

void ptlrpcd_wake(struct ptlrpc_request *req)
{
        struct ptlrpcd_ctl *pc = req->rq_ptlrpcd_data;

        LASSERT(pc != NULL);

        wake_up(&pc->pc_waitq);
}

void ptlrpcd_add_req(struct ptlrpc_request *req)
{
        struct ptlrpcd_ctl *pc;

        if (req->rq_send_state == LUSTRE_IMP_FULL)
                pc = &ptlrpcd_pc;
        else 
                pc = &ptlrpcd_recovery_pc;

        do_gettimeofday(&req->rq_rpcd_start);
        ptlrpc_set_add_new_req(pc->pc_set, req);
        req->rq_ptlrpcd_data = pc;
                
        ptlrpcd_wake(req);
}

static int ptlrpcd_check(struct ptlrpcd_ctl *pc)
{
        struct list_head *tmp, *pos;
        struct ptlrpc_request *req;
        unsigned long flags;
        int rc = 0;
        ENTRY;

        if (test_bit(LIOD_STOP, &pc->pc_flags))
                RETURN(1);

        spin_lock_irqsave(&pc->pc_set->set_new_req_lock, flags);
        list_for_each_safe(pos, tmp, &pc->pc_set->set_new_requests) {
                req = list_entry(pos, struct ptlrpc_request, rq_set_chain);
                list_del_init(&req->rq_set_chain);
                ptlrpc_set_add_req(pc->pc_set, req);
                rc = 1; /* need to calculate its timeout */
        }
        spin_unlock_irqrestore(&pc->pc_set->set_new_req_lock, flags);

        if (pc->pc_set->set_remaining) {
                rc = rc | ptlrpc_check_set(pc->pc_set);

                /* XXX our set never completes, so we prune the completed
                 * reqs after each iteration. boy could this be smarter. */
                list_for_each_safe(pos, tmp, &pc->pc_set->set_requests) {
                        req = list_entry(pos, struct ptlrpc_request,
                                         rq_set_chain);
                        if (req->rq_phase != RQ_PHASE_COMPLETE)
                                continue;

                        list_del_init(&req->rq_set_chain);
                        req->rq_set = NULL;
                        ptlrpc_req_finished (req);
                }
        }
        if (rc == 0) {
                /* If new requests have been added, make sure to wake up */
                spin_lock_irqsave(&pc->pc_set->set_new_req_lock, flags);
                rc = !list_empty(&pc->pc_set->set_new_requests);
                spin_unlock_irqrestore(&pc->pc_set->set_new_req_lock, flags);
        }
        RETURN(rc);
}

#ifdef __KERNEL__
/* ptlrpc's code paths like to execute in process context, so we have this
 * thread which spins on a set which contains the io rpcs.  llite specifies
 * ptlrpcd's set when it pushes pages down into the oscs */
static int ptlrpcd(void *arg)
{
        struct ptlrpcd_ctl *pc = arg;
        unsigned long flags;
        ENTRY;

        kportal_daemonize(pc->pc_name);

        SIGNAL_MASK_LOCK(current, flags);
        sigfillset(&current->blocked);
        RECALC_SIGPENDING;
        SIGNAL_MASK_UNLOCK(current, flags);

        complete(&pc->pc_starting);

        /* this mainloop strongly resembles ptlrpc_set_wait except
         * that our set never completes.  ptlrpcd_check calls ptlrpc_check_set
         * when there are requests in the set.  new requests come in
         * on the set's new_req_list and ptlrpcd_check moves them into
         * the set. */
        while (1) {
                wait_queue_t set_wait;
                struct l_wait_info lwi;
                int timeout;

                timeout = ptlrpc_set_next_timeout(pc->pc_set) * HZ;
                lwi = LWI_TIMEOUT(timeout, ptlrpc_expired_set, pc->pc_set);

                /* ala the pinger, wait on pc's waitqueue and the set's */
                init_waitqueue_entry(&set_wait, current);
                add_wait_queue(&pc->pc_set->set_waitq, &set_wait);
                l_wait_event(pc->pc_waitq, ptlrpcd_check(pc), &lwi);
                remove_wait_queue(&pc->pc_set->set_waitq, &set_wait);

                if (test_bit(LIOD_STOP, &pc->pc_flags))
                        break;
        }
        /* wait for inflight requests to drain */
        if (!list_empty(&pc->pc_set->set_requests))
                ptlrpc_set_wait(pc->pc_set);
        complete(&pc->pc_finishing);
        return 0;
}
#else

int ptlrpcd_check_async_rpcs(void *arg)
{
        struct ptlrpcd_ctl *pc = arg;
        int                  rc = 0;

        /* single threaded!! */
        pc->pc_recurred++;

        if (pc->pc_recurred == 1) {
                rc = ptlrpcd_check(pc);
                if (!rc)
                        ptlrpc_expired_set(pc->pc_set);
        }

        pc->pc_recurred--;
        return rc;
}
#endif

static int ptlrpcd_start(char *name, struct ptlrpcd_ctl *pc)
{
        int rc = 0;

        memset(pc, 0, sizeof(*pc));
        init_completion(&pc->pc_starting);
        init_completion(&pc->pc_finishing);
        init_waitqueue_head(&pc->pc_waitq);
        pc->pc_flags = 0;
        spin_lock_init(&pc->pc_lock);
        INIT_LIST_HEAD(&pc->pc_req_list);
        snprintf (pc->pc_name, sizeof (pc->pc_name), name);

        pc->pc_set = ptlrpc_prep_set();
        if (pc->pc_set == NULL)
                GOTO(out, rc = -ENOMEM);

#ifdef __KERNEL__
        if (kernel_thread(ptlrpcd, pc, 0) < 0)  {
                ptlrpc_set_destroy(pc->pc_set);
                GOTO(out, rc = -ECHILD);
        }

        wait_for_completion(&pc->pc_starting);
#else
        pc->pc_callback =
                liblustre_register_wait_callback(&ptlrpcd_check_async_rpcs, pc);
#endif
out:
        RETURN(rc);
}

static void ptlrpcd_stop(struct ptlrpcd_ctl *pc)
{
        set_bit(LIOD_STOP, &pc->pc_flags);
        wake_up(&pc->pc_waitq);
#ifdef __KERNEL__
        wait_for_completion(&pc->pc_finishing);
#else
        liblustre_deregister_wait_callback(pc->pc_callback);
#endif
        ptlrpc_set_destroy(pc->pc_set);
}

int ptlrpcd_addref(void)
{
        int rc = 0;
        ENTRY;

        down(&ptlrpcd_sem);
        if (++ptlrpcd_users != 1)
                GOTO(out, rc);

        rc = ptlrpcd_start("ptlrpcd", &ptlrpcd_pc);
        if (rc) {
                --ptlrpcd_users;
                GOTO(out, rc);
        }

        rc = ptlrpcd_start("ptlrpcd-recov", &ptlrpcd_recovery_pc);
        if (rc) {
                ptlrpcd_stop(&ptlrpcd_pc);
                --ptlrpcd_users;
                GOTO(out, rc);
        }
out:
        up(&ptlrpcd_sem);
        RETURN(rc);
}

void ptlrpcd_decref(void)
{
        down(&ptlrpcd_sem);
        if (--ptlrpcd_users == 0) {
                ptlrpcd_stop(&ptlrpcd_pc);
                ptlrpcd_stop(&ptlrpcd_recovery_pc);
        }
        up(&ptlrpcd_sem);
}
