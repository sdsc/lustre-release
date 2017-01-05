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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2016, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ldlm/ldlm_plain.c
 *
 * Author: Peter Braam <braam@clusterfs.com>
 * Author: Phil Schwan <phil@clusterfs.com>
 */

/**
 * This file contains implementation of PLAIN lock type.
 *
 * PLAIN locks are the simplest form of LDLM locking, and are used when
 * there only needs to be a single lock on a resource. This avoids some
 * of the complexity of EXTENT and IBITS lock types, but doesn't allow
 * different "parts" of a resource to be locked concurrently.  Example
 * use cases for PLAIN locks include locking of MGS configuration logs
 * and (as of Lustre 2.4) quota records.
 */

#define DEBUG_SUBSYSTEM S_LDLM

#include <lustre_dlm.h>
#include <obd_support.h>
#include <lustre_lib.h>

#include "ldlm_internal.h"

#ifdef HAVE_SERVER_SUPPORT
/**
 * Determine if the lock is compatible with all locks on the queue.
 *
 * If \a work_list is provided, conflicting locks are linked there.
 * If \a work_list is not provided, we exit this function on first conflict.
 *
 * \retval 0 if there are conflicting locks in the \a queue
 * \retval 1 if the lock is compatible to all locks in \a queue
 */
static inline int
ldlm_plain_compat_queue(struct list_head *queue, struct ldlm_lock *req,
			struct list_head *work_list)
{
	enum ldlm_mode req_mode = req->l_req_mode;
	struct ldlm_lock *lock, *next_lock;
	int compat = 1;
	ENTRY;

	lockmode_verify(req_mode);

	list_for_each_entry_safe(lock, next_lock, queue, l_res_link) {

		/* We stop walking the queue if we hit ourselves so we don't
		 * take conflicting locks enqueued after us into account,
		 * or we'd wait forever. */
		if (req == lock)
			RETURN(compat);

		/* Advance loop cursor to last lock of mode group. */
		next_lock = list_entry(list_entry(lock->l_sl_mode.prev,
						  struct ldlm_lock,
						  l_sl_mode)->l_res_link.next,
				       struct ldlm_lock, l_res_link);

		if (lockmode_compat(lock->l_req_mode, req_mode))
                        continue;

                if (!work_list)
                        RETURN(0);

                compat = 0;

		/* Add locks of the mode group to \a work_list as
		 * blocking locks for \a req. */
                if (lock->l_blocking_ast)
                        ldlm_add_ast_work_item(lock, req, work_list);

                {
			struct list_head *head;

                        head = &lock->l_sl_mode;
			list_for_each_entry(lock, head, l_sl_mode)
                                if (lock->l_blocking_ast)
                                        ldlm_add_ast_work_item(lock, req,
                                                               work_list);
                }
        }

        RETURN(compat);
}

/**
 * Process a granting attempt for plain lock.
 * Must be called with ns lock held.
 *
 * This function looks for any conflicts for \a lock in the granted or
 * waiting queues. The lock is granted if no conflicts are found in
 * either queue.
 *
 * If \a first_enq is 0 (ie, called from ldlm_reprocess_queue):
 *   - blocking ASTs have already been sent
 *
 * If \a first_enq is 1 (ie, called from ldlm_lock_enqueue):
 *   - blocking ASTs have not been sent yet, so list of conflicting locks
 *     would be collected and ASTs sent.
 *
 * If \a first_enq is 2 (ie, called from ldlm_reprocess_queue):
 *   - once recovery done, we need to scan the whole waiting lock list to
 *     send blocking ASTs in case the client didn't receive it.
 */
int ldlm_process_plain_lock(struct ldlm_lock *lock, __u64 *flags,
			    int first_enq, enum ldlm_error *err,
			    struct list_head *work_list)
{
	struct ldlm_resource *res = lock->l_resource;
	struct list_head rpc_list;
	int rc;
	ENTRY;

	LASSERT(lock->l_granted_mode != lock->l_req_mode);
	check_res_locked(res);
	LASSERT(list_empty(&res->lr_converting));
	INIT_LIST_HEAD(&rpc_list);

        if (!first_enq) {
                LASSERT(work_list != NULL);
                rc = ldlm_plain_compat_queue(&res->lr_granted, lock, NULL);
                if (!rc)
                        RETURN(LDLM_ITER_STOP);
                rc = ldlm_plain_compat_queue(&res->lr_waiting, lock, NULL);
                if (!rc)
                        RETURN(LDLM_ITER_STOP);

                ldlm_resource_unlink_lock(lock);
                ldlm_grant_lock(lock, work_list);
                RETURN(LDLM_ITER_CONTINUE);
        }

 restart:
        rc = ldlm_plain_compat_queue(&res->lr_granted, lock, &rpc_list);
        rc += ldlm_plain_compat_queue(&res->lr_waiting, lock, &rpc_list);

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
                rc = ldlm_run_ast_work(ldlm_res_to_ns(res), &rpc_list,
                                       LDLM_WORK_BL_AST);
                lock_res(res);
		if (rc == -ERESTART && first_enq == 1) {
			/* We were granted while waiting, nothing left to do */
			if (lock->l_granted_mode == lock->l_req_mode)
				GOTO(out, rc = 0);
			/* Lock was destroyed while we were waiting, abort */
			if (ldlm_is_destroyed(lock))
				GOTO(out, rc = -EAGAIN);

			/* Otherwise try again */
			GOTO(restart, rc);
		}
                *flags |= LDLM_FL_BLOCK_GRANTED;
	} else if (first_enq == 1) {
		ldlm_resource_unlink_lock(lock);
		ldlm_grant_lock(lock, NULL);
	}

	rc = 0;
out:
	*err = rc;
	LASSERT(list_empty(&rpc_list));

	RETURN(first_enq == 1 ? rc : LDLM_ITER_CONTINUE);
}
#endif /* HAVE_SERVER_SUPPORT */

void ldlm_plain_policy_wire_to_local(const union ldlm_wire_policy_data *wpolicy,
				     union ldlm_policy_data *lpolicy)
{
	/* No policy for plain locks */
}

void ldlm_plain_policy_local_to_wire(const union ldlm_policy_data *lpolicy,
				     union ldlm_wire_policy_data *wpolicy)
{
	/* No policy for plain locks */
}
