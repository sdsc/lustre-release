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
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ldlm/ldlm_inodebits.c
 *
 * Author: Peter Braam <braam@clusterfs.com>
 * Author: Phil Schwan <phil@clusterfs.com>
 */

/**
 * This file contains implementation of IBITS lock type
 *
 * IBITS lock type contains a bit mask determining various properties of an
 * object. The meanings of specific bits are specific to the caller and are
 * opaque to LDLM code.
 *
 * Locks with intersecting bitmasks and conflicting lock modes (e.g.  LCK_PW)
 * are considered conflicting.  See the lock mode compatibility matrix
 * in lustre_dlm.h.
 */

#define DEBUG_SUBSYSTEM S_LDLM

#include <lustre_dlm.h>
#include <obd_support.h>
#include <lustre_lib.h>
#include <obd_class.h>

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
 *
 * IBITS locks in granted queue are organized in bunches of
 * same-mode/same-bits locks called "skip lists". The First lock in the
 * bunch contains a pointer to the end of the bunch.  This allows us to
 * skip an entire bunch when iterating the list in search for conflicting
 * locks if first lock of the bunch is not conflicting with us.
 */
static int
ldlm_inodebits_compat_queue(struct list_head *queue, struct ldlm_lock *req,
			    struct list_head *work_list)
{
	struct list_head *tmp;
	struct ldlm_lock *lock;
	__u64 req_bits = req->l_policy_data.l_inodebits.bits;
	int compat = 1;
	ENTRY;

	/* There is no sense in lock with no bits set, I think.
	 * Also, such a lock would be compatible with any other bit lock */
	LASSERT(req_bits != 0);

	list_for_each(tmp, queue) {
		struct list_head *mode_tail;

		lock = list_entry(tmp, struct ldlm_lock, l_res_link);

		/* We stop walking the queue if we hit ourselves so we don't
		 * take conflicting locks enqueued after us into account,
		 * or we'd wait forever. */
		if (req == lock)
			RETURN(compat);

		/* last lock in mode group */
		LASSERT(lock->l_sl_mode.prev != NULL);
		mode_tail = &list_entry(lock->l_sl_mode.prev,
					struct ldlm_lock,
					l_sl_mode)->l_res_link;

		/* if reqest lock is not COS_INCOMPAT and COS is disabled,
		 * they are compatible, IOW this request is from a local
		 * transaction on a DNE system. */
		if (lock->l_req_mode == LCK_COS && !ldlm_is_cos_incompat(req) &&
		    !ldlm_is_cos_enabled(req)) {
			/* jump to last lock in mode group */
			tmp = mode_tail;
			continue;
		}

		/* locks' mode are compatible, bits don't matter */
		if (lockmode_compat(lock->l_req_mode, req->l_req_mode)) {
			/* jump to last lock in mode group */
			tmp = mode_tail;
			continue;
		}

		for (;;) {
			struct list_head *head;

			/* Advance loop cursor to last lock in policy group. */
			tmp = &list_entry(lock->l_sl_policy.prev,
					      struct ldlm_lock,
					      l_sl_policy)->l_res_link;

			/* Locks with overlapping bits conflict. */
			if (lock->l_policy_data.l_inodebits.bits & req_bits) {
				/* COS lock mode has a special compatibility
				 * requirement: it is only compatible with
				 * locks from the same client. */
				if (lock->l_req_mode == LCK_COS &&
				    !ldlm_is_cos_incompat(req) &&
				    ldlm_is_cos_enabled(req) &&
				    lock->l_client_cookie == req->l_client_cookie)
					goto not_conflicting;
				/* Found a conflicting policy group. */
				if (!work_list)
					RETURN(0);

				compat = 0;

				/* Add locks of the policy group to @work_list
				 * as blocking locks for @req */
                                if (lock->l_blocking_ast)
                                        ldlm_add_ast_work_item(lock, req,
                                                               work_list);
                                head = &lock->l_sl_policy;
				list_for_each_entry(lock, head, l_sl_policy)
                                        if (lock->l_blocking_ast)
                                                ldlm_add_ast_work_item(lock, req,
                                                                       work_list);
                        }
                not_conflicting:
                        if (tmp == mode_tail)
                                break;

                        tmp = tmp->next;
			lock = list_entry(tmp, struct ldlm_lock,
                                              l_res_link);
		} /* Loop over policy groups within one mode group. */
	} /* Loop over mode groups within @queue. */

	RETURN(compat);
}

static inline int ldlm_run_bl_ast_and_check(struct ldlm_lock *lock,
					    struct list_head *list,
					    __u64 *flags)
{
	struct ldlm_resource *res = lock->l_resource;
	int rc;

	check_res_locked(res);
	unlock_res(res);
	rc = ldlm_run_ast_work(ldlm_res_to_ns(res), list,
			       LDLM_WORK_BL_AST);
	lock_res(res);
	if (rc == -ERESTART) {
		/* We were granted while waiting, nothing left to do */
		if (lock->l_granted_mode == lock->l_req_mode)
			return 0;
		/* Lock was destroyed while we were waiting, abort */
		if (ldlm_is_destroyed(lock))
			return -EAGAIN;
		return rc;
	}
	*flags |= LDLM_FL_BLOCK_GRANTED;
	return 0;
}

/**
 * Process a granting attempt for IBITS lock.
 * Must be called with ns lock held
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
 */
int ldlm_process_inodebits_lock(struct ldlm_lock *lock, __u64 *flags,
				int first_enq, enum ldlm_error *err,
				struct list_head *work_list)
{
	struct ldlm_resource *res = lock->l_resource;
	struct list_head rpc_list;
	int rc;
	ENTRY;

	LASSERT(lock->l_granted_mode != lock->l_req_mode);
	LASSERT(list_empty(&res->lr_converting));
	INIT_LIST_HEAD(&rpc_list);
	check_res_locked(res);

	/* The LDLM_FL_BLOCK_NOWAIT flag is used by MDT in several cases:
	 * - to get layout lock in advance during open and getattr
	 * - for COS locks to resolve DNE remote objects conflicts
	 */
	if (!first_enq || (*flags & LDLM_FL_BLOCK_NOWAIT)) {
		struct list_head *ast_list;

		if (*flags & LDLM_FL_BLOCK_NOWAIT) {
			ast_list = NULL;
			*err = ELDLM_LOCK_WOULDBLOCK;
		} else {
			ast_list = &rpc_list;
			*err = ELDLM_LOCK_ABORTED;
		}
restart2:
		rc = ldlm_inodebits_compat_queue(&res->lr_granted, lock,
						 ast_list);
		if (rc == 0) {
			if (!ast_list || list_empty(ast_list))
				RETURN(LDLM_ITER_STOP);

			/* It is possible that lock was converted and is kept
			 * in granted queue but there was another new lock that
			 * conflicts too, so blocking AST should be sent again:
			 * 1) lock1 conflicts with lock2
			 * 2) bl_ast was sent for lock2
			 * 3) lock3 comes and conflicts with lock2 too
			 * 4) no bl_ast sent because lock2->l_bl_ast_sent is 1
			 * 5) lock2 was converted for lock1 but not for lock3
			 * 6) lock1 granted, lock3 still is waiting for lock2,
			 *    but there will never be another bl_ast for that
			 *
			 * To avoid this scenario the rpc_list with items from
			 * the granted list is prepared during every reprocess
			 * and bl_ast is sent again.
			 */
			rc = ldlm_run_bl_ast_and_check(lock, &rpc_list, flags);
			if (rc == -ERESTART)
				GOTO(restart2, rc);
			GOTO(out, rc);
		}
		rc = ldlm_inodebits_compat_queue(&res->lr_waiting, lock, NULL);
		if (!rc)
			RETURN(LDLM_ITER_STOP);

		ldlm_resource_unlink_lock(lock);
		ldlm_grant_lock(lock, work_list);

		*err = ELDLM_OK;
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
		 * re-ordered!  Causes deadlock, because ASTs aren't sent!
		 */
		if (list_empty(&lock->l_res_link))
			ldlm_resource_add_lock(res, &res->lr_waiting, lock);
		rc = ldlm_run_bl_ast_and_check(lock, &rpc_list, flags);
		if (rc == -ERESTART)
			GOTO(restart, rc);
		GOTO(out, rc);
	} else {
		ldlm_resource_unlink_lock(lock);
		ldlm_grant_lock(lock, NULL);
	}

	rc = 0;
out:
	*err = rc;
	LASSERT(list_empty(&rpc_list));

	RETURN(rc);
}
#endif /* HAVE_SERVER_SUPPORT */

void ldlm_ibits_policy_wire_to_local(const union ldlm_wire_policy_data *wpolicy,
				     union ldlm_policy_data *lpolicy)
{
	lpolicy->l_inodebits.bits = wpolicy->l_inodebits.bits;
}

void ldlm_ibits_policy_local_to_wire(const union ldlm_policy_data *lpolicy,
				     union ldlm_wire_policy_data *wpolicy)
{
	memset(wpolicy, 0, sizeof(*wpolicy));
	wpolicy->l_inodebits.bits = lpolicy->l_inodebits.bits;
}

/**
 * Attempt to convert already granted IBITS lock with several bits set to
 * a lock with less bits (downgrade).
 *
 * Such lock conversion is used to keep lock with non-blocking bits instead of
 * cancelling it, introduced for better support of DoM files.
 */
int ldlm_inodebits_downgrade(struct ldlm_lock *lock,  __u64 wanted)
{
	ENTRY;

	check_res_locked(lock->l_resource);
	/* Just return if there are no conflicting bits */
	if ((lock->l_policy_data.l_inodebits.bits & wanted) == 0) {
		LDLM_WARN(lock, "try to downgrade with no conflicts %#llx"
			  "/%#llx\n", lock->l_policy_data.l_inodebits.bits,
			  wanted);
		RETURN(-EINVAL);
	}
	LASSERT(lock->l_resource->lr_type == LDLM_IBITS);

	/* remove lock from a skiplist and put in the new place
	 * according with new inodebits */
	ldlm_resource_unlink_lock(lock);
	lock->l_policy_data.l_inodebits.bits &= ~wanted;
	ldlm_grant_lock_with_skiplist(lock);

	RETURN(0);
}

int ldlm_cli_inodebits_downgrade(struct ldlm_lock *lock,  __u64 wanted)
{
	struct lustre_handle lockh;
	__u32 flags = 0;
	int rc;

	ENTRY;

	ldlm_lock2handle(lock, &lockh);

	lock_res_and_lock(lock);
	/* check if there is race with cancel */
	if (ldlm_is_canceling(lock) || ldlm_is_cancel(lock)) {
		unlock_res_and_lock(lock);
		RETURN(-EINVAL);
	}
	ldlm_lock_addref_internal_nolock(lock, lock->l_granted_mode);
	ldlm_set_converting(lock);

	rc = ldlm_inodebits_downgrade(lock, wanted);
	unlock_res_and_lock(lock);
	if (rc != 0) {
		LBUG();
		RETURN(rc);
	}
	/* now send convert RPC to the server */
	rc = ldlm_cli_convert(&lockh, lock->l_granted_mode, &flags);
	lock_res_and_lock(lock);
	ldlm_clear_converting(lock);
	if (rc == 0) {
		ldlm_clear_cbpending(lock);
		ldlm_clear_bl_ast(lock);
	}
	ldlm_lock_decref_internal_nolock(lock, lock->l_granted_mode);
	unlock_res_and_lock(lock);

	RETURN(rc);
}
EXPORT_SYMBOL(ldlm_cli_inodebits_downgrade);
