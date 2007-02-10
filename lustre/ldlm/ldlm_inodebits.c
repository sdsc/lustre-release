/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (c) 2002, 2003, 2004 Cluster File Systems, Inc.
 *   Author: Peter Braam <braam@clusterfs.com>
 *   Author: Phil Schwan <phil@clusterfs.com>
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
 */

#define DEBUG_SUBSYSTEM S_LDLM
#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <lustre_dlm.h>
#include <obd_support.h>
#include <lustre_lib.h>

#include "ldlm_internal.h"

/* Determine if the lock is compatible with all locks on the queue. */
static int
ldlm_inodebits_compat_queue(struct list_head *queue, struct ldlm_lock *req,
                            struct list_head *work_list)
{
        struct list_head *tmp, *tmp_tail;
        struct ldlm_lock *lock;
        ldlm_mode_t req_mode = req->l_req_mode;
        __u64 req_bits = req->l_policy_data.l_inodebits.bits;
        int compat = 1;
        ENTRY;

        LASSERT(req_bits); /* There is no sense in lock with no bits set,
                              I think. Also such a lock would be compatible
                               with any other bit lock */
        list_for_each(tmp, queue) {
                lock = list_entry(tmp, struct ldlm_lock, l_res_link);

                if (req == lock)
                        RETURN(compat);

                /* locks are compatible, bits don't matter */
                if (lockmode_compat(lock->l_req_mode, req_mode)) {
                        /* jump to next mode group */
                        if (LDLM_SL_HEAD(&lock->l_sl_mode))
                                tmp = &list_entry(lock->l_sl_mode.next, 
                                                  struct ldlm_lock,
                                                  l_sl_mode)->l_res_link;
                        continue;
                }

                tmp_tail = tmp;
                if (LDLM_SL_HEAD(&lock->l_sl_mode))
                        tmp_tail = &list_entry(lock->l_sl_mode.next,
                                               struct ldlm_lock,
                                               l_sl_mode)->l_res_link;
                for (;;) {
                        /* locks with bits overlapped are conflicting locks */
                        if (lock->l_policy_data.l_inodebits.bits & req_bits) {
                                /* conflicting policy */
                                if (!work_list)
                                        RETURN(0);
                               
                                compat = 0;
                                if (lock->l_blocking_ast)
                                        ldlm_add_ast_work_item(lock, req, 
                                                               work_list);
                                /* add all members of the policy group */
                                if (LDLM_SL_HEAD(&lock->l_sl_policy)) {
                                        do {
                                                tmp = lock->l_res_link.next;
                                                lock = list_entry(tmp,
                                                            struct ldlm_lock,
                                                            l_res_link);
                                                if (lock->l_blocking_ast)
                                                        ldlm_add_ast_work_item(
                                                                     lock,
                                                                     req,
                                                                     work_list);
                                        } while (!LDLM_SL_TAIL(&lock->l_sl_policy));
                                }
                        } else if (LDLM_SL_HEAD(&lock->l_sl_policy)) {
                                /* jump to next policy group */
                                tmp = &list_entry(lock->l_sl_policy.next,
                                                  struct ldlm_lock,
                                                  l_sl_policy)->l_res_link;
                        }
                        if (tmp == tmp_tail)
                                break;
                        else
                                tmp = tmp->next;
                        lock = list_entry(tmp, struct ldlm_lock, l_res_link);
                }       /* for locks in a mode group */
        }       /* for each lock in the queue */

        RETURN(compat);
}

/* If first_enq is 0 (ie, called from ldlm_reprocess_queue):
  *   - blocking ASTs have already been sent
  *   - the caller has already initialized req->lr_tmp
  *   - must call this function with the ns lock held
  *
  * If first_enq is 1 (ie, called from ldlm_lock_enqueue):
  *   - blocking ASTs have not been sent
  *   - the caller has NOT initialized req->lr_tmp, so we must
  *   - must call this function with the ns lock held once */
int ldlm_process_inodebits_lock(struct ldlm_lock *lock, int *flags,
                                int first_enq, ldlm_error_t *err,
                                struct list_head *work_list)
{
        struct ldlm_resource *res = lock->l_resource;
        struct list_head rpc_list = CFS_LIST_HEAD_INIT(rpc_list);
        int rc;
        ENTRY;

        LASSERT(list_empty(&res->lr_converting));
        check_res_locked(res);

        if (!first_enq) {
                LASSERT(work_list != NULL);
                rc = ldlm_inodebits_compat_queue(&res->lr_granted, lock, NULL);
                if (!rc)
                        RETURN(LDLM_ITER_STOP);
                rc = ldlm_inodebits_compat_queue(&res->lr_waiting, lock, NULL);
                if (!rc)
                        RETURN(LDLM_ITER_STOP);

                ldlm_resource_unlink_lock(lock);
                ldlm_grant_lock(lock, work_list);
                RETURN(LDLM_ITER_CONTINUE);
        }

 restart:
        rc = ldlm_inodebits_compat_queue(&res->lr_granted, lock, &rpc_list);
        rc += ldlm_inodebits_compat_queue(&res->lr_waiting, lock, &rpc_list);

        if (rc != 2) {
                /* If either of the compat_queue()s returned 0, then we
                 * have ASTs to send and must go onto the waiting list.
                 *
                 * bug 2322: we used to unlink and re-add here, which was a
                 * terrible folly -- if we goto restart, we could get
                 * re-ordered!  Causes deadlock, because ASTs aren't sent! */
                if (list_empty(&lock->l_res_link))
                        ldlm_resource_add_lock(res, &res->lr_waiting, lock);
                unlock_res(res);
                rc = ldlm_run_bl_ast_work(&rpc_list);
                lock_res(res);
                if (rc == -ERESTART)
                        GOTO(restart, -ERESTART);
                *flags |= LDLM_FL_BLOCK_GRANTED;
        } else {
                ldlm_resource_unlink_lock(lock);
                ldlm_grant_lock(lock, NULL);
        }
        RETURN(0);
}
