/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2003 Cluster File Systems, Inc.
 *   Author: Andreas Dilger <adilger@clusterfs.com>
 *
 *   This file is part of the Lustre file system, http://www.lustre.org
 *   Lustre is a trademark of Cluster File Systems, Inc.
 *
 *   You may have signed or agreed to another license before downloading
 *   this software.  If so, you are bound by the terms and conditions
 *   of that agreement, and the following does not apply to you.  See the
 *   LICENSE file included with this distribution for more information.
 *
 *   If you did not agree to a different license, then this copy of Lustre
 *   is open source software; you can redistribute it and/or modify it
 *   under the terms of version 2 of the GNU General Public License as
 *   published by the Free Software Foundation.
 *
 *   In either case, Lustre is distributed in the hope that it will be
 *   useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 *   of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   license text for more details.
 *
 * OST<->MDS recovery logging thread.
 *
 * Invariants in implementation:
 * - we do not share logs among different OST<->MDS connections, so that
 *   if an OST or MDS fails it need only look at log(s) relevant to itself
 */

#define DEBUG_SUBSYSTEM S_LOG

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#ifdef __KERNEL__
# include <libcfs/libcfs.h>
#else
# include <libcfs/list.h>
# include <liblustre.h>
#endif

#include <libcfs/kp30.h>
#include <obd_class.h>
#include <lustre_commit_confd.h>
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lnet/types.h>
#include <libcfs/list.h>
#include <lustre_log.h>
#include "ptlrpc_internal.h"

#ifdef __KERNEL__

static struct llog_commit_master lustre_lcm;
static struct llog_commit_master *lcm = &lustre_lcm;

/* Allocate new commit structs in case we do not have enough.
 * Make the llcd size small enough that it fits into a single page when we
 * are sending/receiving it. */
static int llcd_alloc(void)
{
        struct llog_canceld_ctxt *llcd;
        int llcd_size;

        /* payload of lustre_msg V2 is bigger */
        llcd_size = 4096 - lustre_msg_size(LUSTRE_MSG_MAGIC_V2, 1, NULL);
        OBD_ALLOC(llcd,
                  llcd_size + offsetof(struct llog_canceld_ctxt, llcd_cookies));
        if (llcd == NULL)
                return -ENOMEM;

        llcd->llcd_size = llcd_size;
        llcd->llcd_lcm = lcm;

        spin_lock(&lcm->lcm_llcd_lock);
        list_add(&llcd->llcd_list, &lcm->lcm_llcd_free);
        atomic_inc(&lcm->lcm_llcd_numfree);
        spin_unlock(&lcm->lcm_llcd_lock);

        return 0;
}

/* Get a free cookie struct from the list */
struct llog_canceld_ctxt *llcd_grab(void)
{
        struct llog_canceld_ctxt *llcd;

repeat:
        spin_lock(&lcm->lcm_llcd_lock);
        if (list_empty(&lcm->lcm_llcd_free)) {
                spin_unlock(&lcm->lcm_llcd_lock);
                if (llcd_alloc() < 0) {
                        CERROR("unable to allocate log commit data!\n");
                        return NULL;
                }
                /* check new llcd wasn't grabbed while lock dropped, b=7407 */
                goto repeat;
        }

        llcd = list_entry(lcm->lcm_llcd_free.next, typeof(*llcd), llcd_list);
        list_del(&llcd->llcd_list);
        atomic_dec(&lcm->lcm_llcd_numfree);
        spin_unlock(&lcm->lcm_llcd_lock);

        llcd->llcd_cookiebytes = 0;

        return llcd;
}
EXPORT_SYMBOL(llcd_grab);

static void llcd_put(struct llog_canceld_ctxt *llcd)
{
        if (atomic_read(&lcm->lcm_llcd_numfree) >= lcm->lcm_llcd_maxfree) {
                int llcd_size = llcd->llcd_size +
                         offsetof(struct llog_canceld_ctxt, llcd_cookies);
                OBD_FREE(llcd, llcd_size);
        } else {
                spin_lock(&lcm->lcm_llcd_lock);
                list_add(&llcd->llcd_list, &lcm->lcm_llcd_free);
                atomic_inc(&lcm->lcm_llcd_numfree);
                spin_unlock(&lcm->lcm_llcd_lock);
        }
}

/* Send some cookies to the appropriate target */
void llcd_send(struct llog_canceld_ctxt *llcd)
{
        spin_lock(&llcd->llcd_lcm->lcm_llcd_lock);
        list_add_tail(&llcd->llcd_list, &llcd->llcd_lcm->lcm_llcd_pending);
        spin_unlock(&llcd->llcd_lcm->lcm_llcd_lock);

        cfs_waitq_signal_nr(&llcd->llcd_lcm->lcm_waitq, 1);
}
EXPORT_SYMBOL(llcd_send);

/* deleted objects have a commit callback that cancels the MDS
 * log record for the deletion.  The commit callback calls this
 * function
 */
int llog_obd_repl_cancel(struct llog_ctxt *ctxt,
                         struct lov_stripe_md *lsm, int count,
                         struct llog_cookie *cookies, int flags)
{
        struct llog_canceld_ctxt *llcd;
        int rc = 0;
        ENTRY;

        LASSERT(ctxt);

        mutex_down(&ctxt->loc_sem);
        if (ctxt->loc_imp == NULL) {
                CDEBUG(D_HA, "no import for ctxt %p\n", ctxt);
                GOTO(out, rc = 0);
        }

        llcd = ctxt->loc_llcd;

        if (count > 0 && cookies != NULL) {
                if (llcd == NULL) {
                        llcd = llcd_grab();
                        if (llcd == NULL) {
                                CERROR("couldn't get an llcd - dropped "LPX64
                                       ":%x+%u\n",
                                       cookies->lgc_lgl.lgl_oid,
                                       cookies->lgc_lgl.lgl_ogen,
                                       cookies->lgc_index);
                                GOTO(out, rc = -ENOMEM);
                        }
                        llcd->llcd_ctxt = ctxt;
                        ctxt->loc_llcd = llcd;
                }

                memcpy((char *)llcd->llcd_cookies + llcd->llcd_cookiebytes,
                       cookies, sizeof(*cookies));
                llcd->llcd_cookiebytes += sizeof(*cookies);
        } else {
                if (llcd == NULL || !(flags & OBD_LLOG_FL_SENDNOW))
                        GOTO(out, rc);
        }

        if ((llcd->llcd_size - llcd->llcd_cookiebytes) < sizeof(*cookies) ||
            (flags & OBD_LLOG_FL_SENDNOW)) {
                CDEBUG(D_HA, "send llcd %p:%p\n", llcd, llcd->llcd_ctxt);
                ctxt->loc_llcd = NULL;
                llcd_send(llcd);
        }
out:
        mutex_up(&ctxt->loc_sem);
        return rc;
}
EXPORT_SYMBOL(llog_obd_repl_cancel);

int llog_obd_repl_sync(struct llog_ctxt *ctxt, struct obd_export *exp)
{
        int rc = 0;
        ENTRY;

        if (exp && (ctxt->loc_imp == exp->exp_imp_reverse)) {
                CDEBUG(D_HA, "reverse import disconnected, put llcd %p:%p\n",
                       ctxt->loc_llcd, ctxt);
                mutex_down(&ctxt->loc_sem);
                if (ctxt->loc_llcd != NULL) {
                        llcd_put(ctxt->loc_llcd);
                        ctxt->loc_llcd = NULL;
                }
                ctxt->loc_imp = NULL;
                mutex_up(&ctxt->loc_sem);
        } else {
                rc = llog_cancel(ctxt, NULL, 0, NULL, OBD_LLOG_FL_SENDNOW);
        }

        RETURN(rc);
}
EXPORT_SYMBOL(llog_obd_repl_sync);

static int log_commit_thread(void *arg)
{
        struct llog_commit_master *lcm = arg;
        struct llog_commit_daemon *lcd;
        struct llog_canceld_ctxt *llcd, *n;
        ENTRY;

        OBD_ALLOC(lcd, sizeof(*lcd));
        if (lcd == NULL)
                RETURN(-ENOMEM);

        spin_lock(&lcm->lcm_thread_lock);
        THREAD_NAME(cfs_curproc_comm(), CFS_CURPROC_COMM_MAX - 1,
                    "ll_log_comt_%02d", atomic_read(&lcm->lcm_thread_total));
        atomic_inc(&lcm->lcm_thread_total);
        spin_unlock(&lcm->lcm_thread_lock);

        ptlrpc_daemonize(cfs_curproc_comm()); /* thread never needs to do IO */

        CFS_INIT_LIST_HEAD(&lcd->lcd_lcm_list);
        CFS_INIT_LIST_HEAD(&lcd->lcd_llcd_list);
        lcd->lcd_lcm = lcm;

        CDEBUG(D_HA, "%s started\n", cfs_curproc_comm());
        do {
                struct ptlrpc_request *request;
                struct obd_import *import = NULL;
                struct list_head *sending_list;
                int rc = 0;

                /* If we do not have enough pages available, allocate some */
                while (atomic_read(&lcm->lcm_llcd_numfree) <
                       lcm->lcm_llcd_minfree) {
                        if (llcd_alloc() < 0)
                                break;
                }

                spin_lock(&lcm->lcm_thread_lock);
                atomic_inc(&lcm->lcm_thread_numidle);
                list_move(&lcd->lcd_lcm_list, &lcm->lcm_thread_idle);
                spin_unlock(&lcm->lcm_thread_lock);

                wait_event_interruptible(lcm->lcm_waitq,
                                         !list_empty(&lcm->lcm_llcd_pending) ||
                                         lcm->lcm_flags & LLOG_LCM_FL_EXIT);

                /* If we are the last available thread, start a new one in case
                 * we get blocked on an RPC (nobody else will start a new one)*/
                spin_lock(&lcm->lcm_thread_lock);
                atomic_dec(&lcm->lcm_thread_numidle);
                list_move(&lcd->lcd_lcm_list, &lcm->lcm_thread_busy);
                spin_unlock(&lcm->lcm_thread_lock);

                sending_list = &lcm->lcm_llcd_pending;
        resend:
                import = NULL;
                if (lcm->lcm_flags & LLOG_LCM_FL_EXIT) {
                        lcm->lcm_llcd_maxfree = 0;
                        lcm->lcm_llcd_minfree = 0;
                        lcm->lcm_thread_max = 0;

                        if (list_empty(&lcm->lcm_llcd_pending) ||
                            lcm->lcm_flags & LLOG_LCM_FL_EXIT_FORCE)
                                break;
                }

                if (atomic_read(&lcm->lcm_thread_numidle) <= 1 &&
                    atomic_read(&lcm->lcm_thread_total) < lcm->lcm_thread_max) {
                        rc = llog_start_commit_thread();
                        if (rc < 0)
                                CERROR("error starting thread: rc %d\n", rc);
                }

                /* Move all of the pending cancels from the same OST off of
                 * the list, so we don't get multiple threads blocked and/or
                 * doing upcalls on the same OST in case of failure. */
                spin_lock(&lcm->lcm_llcd_lock);
                if (!list_empty(sending_list)) {
                        list_move_tail(sending_list->next,
                                       &lcd->lcd_llcd_list);
                        llcd = list_entry(lcd->lcd_llcd_list.next,
                                          typeof(*llcd), llcd_list);
                        LASSERT(llcd->llcd_lcm == lcm);
                        import = llcd->llcd_ctxt->loc_imp;
                }
                list_for_each_entry_safe(llcd, n, sending_list, llcd_list) {
                        LASSERT(llcd->llcd_lcm == lcm);
                        if (import == llcd->llcd_ctxt->loc_imp)
                                list_move_tail(&llcd->llcd_list,
                                               &lcd->lcd_llcd_list);
                }
                if (sending_list != &lcm->lcm_llcd_resend) {
                        list_for_each_entry_safe(llcd, n, &lcm->lcm_llcd_resend,
                                                 llcd_list) {
                                LASSERT(llcd->llcd_lcm == lcm);
                                if (import == llcd->llcd_ctxt->loc_imp)
                                        list_move_tail(&llcd->llcd_list,
                                                       &lcd->lcd_llcd_list);
                        }
                }
                spin_unlock(&lcm->lcm_llcd_lock);

                /* We are the only one manipulating our local list - no lock */
                list_for_each_entry_safe(llcd,n, &lcd->lcd_llcd_list,llcd_list){
                        int size[2] = { sizeof(struct ptlrpc_body),
                                        llcd->llcd_cookiebytes };
                        char *bufs[2] = { NULL, (char *)llcd->llcd_cookies };

                        list_del(&llcd->llcd_list);
                        if (llcd->llcd_cookiebytes == 0) {
                                CDEBUG(D_HA, "put empty llcd %p:%p\n",
                                       llcd, llcd->llcd_ctxt);
                                llcd_put(llcd);
                                continue;
                        }

                        mutex_down(&llcd->llcd_ctxt->loc_sem);
                        if (llcd->llcd_ctxt->loc_imp == NULL) {
                                mutex_up(&llcd->llcd_ctxt->loc_sem);
                                CWARN("import will be destroyed, put "
                                      "llcd %p:%p\n", llcd, llcd->llcd_ctxt);
                                llcd_put(llcd);
                                continue;
                        }
                        mutex_up(&llcd->llcd_ctxt->loc_sem);

                        if (!import || (import == LP_POISON) ||
                            (import->imp_client == LP_POISON)) {
                                CERROR("No import %p (llcd=%p, ctxt=%p)\n",
                                       import, llcd, llcd->llcd_ctxt);
                                llcd_put(llcd);
                                continue;
                        }

                        request = ptlrpc_prep_req(import, LUSTRE_LOG_VERSION,
                                                  OBD_LOG_CANCEL, 2, size,bufs);
                        if (request == NULL) {
                                rc = -ENOMEM;
                                CERROR("error preparing commit: rc %d\n", rc);

                                spin_lock(&lcm->lcm_llcd_lock);
                                list_splice(&lcd->lcd_llcd_list,
                                            &lcm->lcm_llcd_resend);
                                CFS_INIT_LIST_HEAD(&lcd->lcd_llcd_list);
                                spin_unlock(&lcm->lcm_llcd_lock);
                                break;
                        }

                        /* XXX FIXME bug 249, 5515 */
                        request->rq_request_portal = LDLM_CANCEL_REQUEST_PORTAL;
                        request->rq_reply_portal = LDLM_CANCEL_REPLY_PORTAL;

                        ptlrpc_req_set_repsize(request, 1, NULL);
                        mutex_down(&llcd->llcd_ctxt->loc_sem);
                        if (llcd->llcd_ctxt->loc_imp == NULL) {
                                mutex_up(&llcd->llcd_ctxt->loc_sem);
                                CWARN("import will be destroyed, put "
                                      "llcd %p:%p\n", llcd, llcd->llcd_ctxt);
                                llcd_put(llcd);
                                ptlrpc_req_finished(request);
                                continue;
                        }
                        mutex_up(&llcd->llcd_ctxt->loc_sem);
                        rc = ptlrpc_queue_wait(request);
                        ptlrpc_req_finished(request);

                        /* If the RPC failed, we put this and the remaining
                         * messages onto the resend list for another time. */
                        if (rc == 0) {
                                llcd_put(llcd);
                                continue;
                        }

                        CERROR("commit %p:%p drop %d cookies: rc %d\n",
                               llcd, llcd->llcd_ctxt,
                               (int)(llcd->llcd_cookiebytes /
                                     sizeof(*llcd->llcd_cookies)), rc);
                        llcd_put(llcd);
                }

                if (rc == 0) {
                        sending_list = &lcm->lcm_llcd_resend;
                        if (!list_empty(sending_list))
                                goto resend;
                }
        } while(1);

        /* If we are force exiting, just drop all of the cookies. */
        if (lcm->lcm_flags & LLOG_LCM_FL_EXIT_FORCE) {
                spin_lock(&lcm->lcm_llcd_lock);
                list_splice(&lcm->lcm_llcd_pending, &lcd->lcd_llcd_list);
                list_splice(&lcm->lcm_llcd_resend, &lcd->lcd_llcd_list);
                list_splice(&lcm->lcm_llcd_free, &lcd->lcd_llcd_list);
                spin_unlock(&lcm->lcm_llcd_lock);

                list_for_each_entry_safe(llcd, n, &lcd->lcd_llcd_list,llcd_list)
                        llcd_put(llcd);
        }

        spin_lock(&lcm->lcm_thread_lock);
        list_del(&lcd->lcd_lcm_list);
        spin_unlock(&lcm->lcm_thread_lock);
        OBD_FREE(lcd, sizeof(*lcd));

        CDEBUG(D_HA, "%s exiting\n", cfs_curproc_comm());

        spin_lock(&lcm->lcm_thread_lock);
        atomic_dec(&lcm->lcm_thread_total);
        spin_unlock(&lcm->lcm_thread_lock);
        cfs_waitq_signal(&lcm->lcm_waitq);

        return 0;
}

int llog_start_commit_thread(void)
{
        int rc;
        ENTRY;

        if (atomic_read(&lcm->lcm_thread_total) >= lcm->lcm_thread_max)
                RETURN(0);

        rc = cfs_kernel_thread(log_commit_thread, lcm, CLONE_VM | CLONE_FILES);
        if (rc < 0) {
                CERROR("error starting thread #%d: %d\n",
                       atomic_read(&lcm->lcm_thread_total), rc);
                RETURN(rc);
        }

        RETURN(0);
}
EXPORT_SYMBOL(llog_start_commit_thread);

static struct llog_process_args {
        struct semaphore         llpa_sem;
        struct llog_ctxt        *llpa_ctxt;
        void                    *llpa_cb;
        void                    *llpa_arg;
} llpa;

int llog_init_commit_master(void)
{
        CFS_INIT_LIST_HEAD(&lcm->lcm_thread_busy);
        CFS_INIT_LIST_HEAD(&lcm->lcm_thread_idle);
        spin_lock_init(&lcm->lcm_thread_lock);
        atomic_set(&lcm->lcm_thread_numidle, 0);
        cfs_waitq_init(&lcm->lcm_waitq);
        CFS_INIT_LIST_HEAD(&lcm->lcm_llcd_pending);
        CFS_INIT_LIST_HEAD(&lcm->lcm_llcd_resend);
        CFS_INIT_LIST_HEAD(&lcm->lcm_llcd_free);
        spin_lock_init(&lcm->lcm_llcd_lock);
        atomic_set(&lcm->lcm_llcd_numfree, 0);
        lcm->lcm_llcd_minfree = 0;
        lcm->lcm_thread_max = 5;
        /* FIXME initialize semaphore for llog_process_args */
        sema_init(&llpa.llpa_sem, 1);
        return 0;
}

int llog_cleanup_commit_master(int force)
{
        lcm->lcm_flags |= LLOG_LCM_FL_EXIT;
        if (force)
                lcm->lcm_flags |= LLOG_LCM_FL_EXIT_FORCE;
        cfs_waitq_signal(&lcm->lcm_waitq);

        wait_event_interruptible(lcm->lcm_waitq,
                                 atomic_read(&lcm->lcm_thread_total) == 0);
        return 0;
}

static int log_process_thread(void *args)
{
        struct llog_process_args *data = args;
        struct llog_ctxt *ctxt = data->llpa_ctxt;
        void   *cb = data->llpa_cb;
        struct llog_logid logid = *(struct llog_logid *)(data->llpa_arg);
        struct llog_handle *llh = NULL;
        int rc;
        ENTRY;

        mutex_up(&data->llpa_sem);
        ptlrpc_daemonize("llog_process");     /* thread does IO to log files */

        rc = llog_create(ctxt, &llh, &logid, NULL);
        if (rc) {
                CERROR("llog_create failed %d\n", rc);
                RETURN(rc);
        }
        rc = llog_init_handle(llh, LLOG_F_IS_CAT, NULL);
        if (rc) {
                CERROR("llog_init_handle failed %d\n", rc);
                GOTO(out, rc);
        }

        if (cb) {
                rc = llog_cat_process(llh, (llog_cb_t)cb, NULL);
                if (rc != LLOG_PROC_BREAK)
                        CERROR("llog_cat_process failed %d\n", rc);
        } else {
                CWARN("no callback function for recovery\n");
        }

        CDEBUG(D_HA, "send llcd %p:%p forcibly after recovery\n",
               ctxt->loc_llcd, ctxt);
        llog_sync(ctxt, NULL);
out:
        rc = llog_cat_put(llh);
        if (rc)
                CERROR("llog_cat_put failed %d\n", rc);

        RETURN(rc);
}

static int llog_recovery_generic(struct llog_ctxt *ctxt, void *handle,void *arg)
{
        int rc;
        ENTRY;

        mutex_down(&llpa.llpa_sem);
        llpa.llpa_ctxt = ctxt;
        llpa.llpa_cb = handle;
        llpa.llpa_arg = arg;

        rc = cfs_kernel_thread(log_process_thread, &llpa, CLONE_VM | CLONE_FILES);
        if (rc < 0)
                CERROR("error starting log_process_thread: %d\n", rc);
        else {
                CDEBUG(D_HA, "log_process_thread: %d\n", rc);
                rc = 0;
        }

        RETURN(rc);
}

int llog_repl_connect(struct llog_ctxt *ctxt, int count,
                      struct llog_logid *logid, struct llog_gen *gen,
                      struct obd_uuid *uuid)
{
        struct llog_canceld_ctxt *llcd;
        int rc;
        ENTRY;

        /* send back llcd before recovery from llog */
        if (ctxt->loc_llcd != NULL) {
                CWARN("llcd %p:%p not empty\n", ctxt->loc_llcd, ctxt);
                llog_sync(ctxt, NULL);
        }

        mutex_down(&ctxt->loc_sem);
        ctxt->loc_gen = *gen;
        llcd = llcd_grab();
        if (llcd == NULL) {
                CERROR("couldn't get an llcd\n");
                mutex_up(&ctxt->loc_sem);
                RETURN(-ENOMEM);
        }
        llcd->llcd_ctxt = ctxt;
        ctxt->loc_llcd = llcd;
        mutex_up(&ctxt->loc_sem);

        rc = llog_recovery_generic(ctxt, ctxt->llog_proc_cb, logid);
        if (rc != 0)
                CERROR("error recovery process: %d\n", rc);

        RETURN(rc);
}
EXPORT_SYMBOL(llog_repl_connect);

#else /* !__KERNEL__ */

int llog_obd_repl_cancel(struct llog_ctxt *ctxt,
                         struct lov_stripe_md *lsm, int count,
                         struct llog_cookie *cookies, int flags)
{
        return 0;
}
#endif
