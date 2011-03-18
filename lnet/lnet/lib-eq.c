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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lnet/lnet/lib-eq.c
 *
 * Library level Event queue management routines
 */

#define DEBUG_SUBSYSTEM S_LNET
#include <lnet/lib-lnet.h>

/**
 * Create an event queue that has room for \a count number of events.
 *
 * The event queue is circular and older events will be overwritten by new
 * ones if they are not removed in time by the user using the functions
 * LNetEQGet(), LNetEQWait(), or LNetEQPoll(). It is up to the user to
 * determine the appropriate size of the event queue to prevent this loss
 * of events. Note that when EQ handler is specified in \a callback, no
 * event loss can happen, since the handler is run for each event deposited
 * into the EQ.
 *
 * \param count The number of events to be stored in the event queue. It
 * will be rounded up to the next power of two.
 * \param callback A handler function that runs when an event is deposited
 * into the EQ. The constant value LNET_EQ_HANDLER_NONE can be used to
 * indicate that no event handler is desired.
 * \param handle On successful return, this location will hold a handle for
 * the newly created EQ.
 *
 * \retval 0       On success.
 * \retval -EINVAL If an parameter is not valid.
 * \retval -ENOMEM If memory for the EQ can't be allocated.
 *
 * \see lnet_eq_handler_t for the discussion on EQ handler semantics.
 */
int
LNetEQAlloc(unsigned int count, lnet_eq_handler_t callback,
            lnet_handle_eq_t *handle)
{
        lnet_eq_cpud_t  *cpud;
        lnet_eq_t       *eq;
        int              i;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        /* We need count to be a power of 2 so that when eq_{enq,deq}_seq
         * overflow, they don't skip entries, so the queue has the same
         * apparent capacity at all times */

        count = cfs_power2_roundup(count);
        if (count < 0)
                return -EINVAL;

        /* XXX remove me in the future */
        if (callback != LNET_EQ_HANDLER_NONE)
                count = 0;

        /* count can be 0 if only need callback, we can eliminate
         * overhead of enqueue event */
        if (count == 0 && callback == LNET_EQ_HANDLER_NONE)
                return -EINVAL;

        eq = lnet_eq_alloc();
        if (eq == NULL)
                return -ENOMEM;

        memset(eq, 0, sizeof(lnet_eq_t));

        eq->eq_deq_seq  = 1;
        eq->eq_enq_seq  = 1;
        eq->eq_size     = count;
        eq->eq_callback = callback;

        if (count != 0){
                LIBCFS_ALLOC(eq->eq_events, count * sizeof(lnet_event_t));
                if (eq->eq_events == NULL)
                        goto error;
        }

        eq->eq_cpuds = cfs_percpu_alloc(sizeof(lnet_eq_cpud_t));
        if (eq->eq_cpuds == NULL)
                goto error;

        cfs_percpu_for_each(cpud, i, eq->eq_cpuds)
                cpud->ec_count = 0; /* don't have to but more readable */

        /* MUST lock all OBS for changing EQ hash & active list */
        LNET_OBJ_LOCK_ALL();

        cfs_list_add(&eq->eq_list, &lnet_objcd_default()->loc_active_eqs);
        lnet_initialise_handle(lnet_objcd_default(),
                               &eq->eq_lh, LNET_COOKIE_TYPE_EQ);
        lnet_eq2handle(handle, eq);

        LNET_OBJ_UNLOCK_ALL();

        return 0;

 error:
        if (eq->eq_cpuds != NULL)
                cfs_percpu_free(eq->eq_cpuds);
        if (eq->eq_events != NULL)
                LIBCFS_FREE(eq->eq_events, count * sizeof(lnet_event_t));

        LNET_OBJ_LOCK_ALL();
        lnet_eq_free(eq);
        LNET_OBJ_UNLOCK_ALL();

        return -ENOMEM;
}

/**
 * Release the resources associated with an event queue if it's idle;
 * otherwise do nothing and it's up to the user to try again.
 *
 * \param eqh A handle for the event queue to be released.
 *
 * \retval 0 If the EQ is not in use and freed.
 * \retval -ENOENT If \a eqh does not point to a valid EQ.
 * \retval -EBUSY  If the EQ is still in use by some MDs.
 */
int
LNetEQFree(lnet_handle_eq_t eqh)
{
        lnet_event_t      *events = NULL;
        lnet_eq_cpud_t   **cpuds  = NULL;
        lnet_eq_cpud_t    *cpud;
        lnet_eq_t         *eq;
        int                size = 0;
        int                i;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        LNET_OBJ_LOCK_ALL();
        eq = lnet_handle2eq(lnet_objcd_default(), &eqh);
        if (eq == NULL) {
                LNET_OBJ_UNLOCK_ALL();
                return -ENOENT;
        }

        cpuds = eq->eq_cpuds;
        cfs_percpu_for_each(cpud, i, cpuds) {
                if (cpud->ec_count != 0) {
                        LNET_OBJ_UNLOCK_ALL();
                        return -EBUSY;
                }
        }

        events = eq->eq_events;
        size   = eq->eq_size;
        /* ensure all future handle lookups fail */
        lnet_invalidate_handle(&eq->eq_lh);
        cfs_list_del(&eq->eq_list);
        lnet_eq_free(eq);

        LNET_OBJ_UNLOCK_ALL();

        if (events != NULL)
                LIBCFS_FREE(events, size * sizeof (lnet_event_t));
        if (cpuds != NULL)
                cfs_percpu_free(cpuds);

        return 0;
}

int
lib_get_event (lnet_obj_cpud_t *objcd, lnet_eq_t *eq, lnet_event_t *ev)
{
        int           new_index = eq->eq_deq_seq & (eq->eq_size - 1);
        lnet_event_t *new_event = &eq->eq_events[new_index];
        int           rc = 1;

        ENTRY;
        /* no new event? */
        if (LNET_SEQ_GT(eq->eq_deq_seq, new_event->sequence))
                RETURN(0);

        *ev = *new_event;

        CDEBUG(D_INFO, "event: %p, sequence: %lu, eq->size: %u\n",
               new_event, eq->eq_deq_seq, eq->eq_size);

        /* ...but did it overwrite an event we've not seen yet? */
        if (eq->eq_deq_seq != ev->sequence) {
                /* don't complain with CERROR: some EQs are sized small
                 * anyway; if it's important, the caller should complain */
                CDEBUG(D_NET, "Event Queue Overflow: eq seq %lu ev seq %lu\n",
                       eq->eq_deq_seq, ev->sequence);
                rc = -EOVERFLOW;
        }

        eq->eq_deq_seq = ev->sequence + 1;

        RETURN(rc);
}

/**
 * A nonblocking function that can be used to get the next event in an EQ.
 * If an event handler is associated with the EQ, the handler will run before
 * this function returns successfully. The event is removed from the queue.
 *
 * \param eventq A handle for the event queue.
 * \param event On successful return (1 or -EOVERFLOW), this location will
 * hold the next event in the EQ.
 *
 * \retval 0          No pending event in the EQ.
 * \retval 1          Indicates success.
 * \retval -ENOENT    If \a eventq does not point to a valid EQ.
 * \retval -EOVERFLOW Indicates success (i.e., an event is returned) and that
 * at least one event between this event and the last event obtained from the
 * EQ has been dropped due to limited space in the EQ.
 */
int
LNetEQGet (lnet_handle_eq_t eventq, lnet_event_t *event)
{
        int which;

        return LNetEQPoll(&eventq, 1, 0, event, &which);
}

/**
 * Block the calling process until there is an event in the EQ.
 * If an event handler is associated with the EQ, the handler will run before
 * this function returns successfully. This function returns the next event
 * in the EQ and removes it from the EQ.
 *
 * \param eventq A handle for the event queue.
 * \param event On successful return (1 or -EOVERFLOW), this location will
 * hold the next event in the EQ.
 *
 * \retval 1          Indicates success.
 * \retval -ENOENT    If \a eventq does not point to a valid EQ.
 * \retval -EOVERFLOW Indicates success (i.e., an event is returned) and that
 * at least one event between this event and the last event obtained from the
 * EQ has been dropped due to limited space in the EQ.
 */
int
LNetEQWait (lnet_handle_eq_t eventq, lnet_event_t *event)
{
        int which;

        return LNetEQPoll(&eventq, 1, LNET_TIME_FOREVER, event, &which);
}

enum {
        LNET_EV_CHECK_NONE,
        LNET_EV_CHECK_ONCE,
        LNET_EV_CHECK_MORE,
};

#ifdef __KERNEL__

static int
lnet_eq_wait_locked(lnet_obj_cpud_t *objcd, int *timeout_ms)
{
        int              tms = *timeout_ms;
        int              rc;
        cfs_waitlink_t   wl;
        cfs_time_t       now;

        if (tms == 0) /* don't want to wait */
                return LNET_EV_CHECK_NONE;

        rc = LNET_EV_CHECK_MORE;

        cfs_waitlink_init(&wl);
        cfs_set_current_state(CFS_TASK_INTERRUPTIBLE);
        cfs_waitq_add(&the_lnet.ln_waitq, &wl);

        LNET_OBJ_UNLOCK(objcd);

        if (tms < 0) {
                cfs_waitq_wait(&wl, CFS_TASK_INTERRUPTIBLE);
        } else {
                struct timeval tv;

                now = cfs_time_current();
                cfs_waitq_timedwait(&wl, CFS_TASK_INTERRUPTIBLE,
                                    cfs_time_seconds(tms)/1000);
                cfs_duration_usec(cfs_time_sub(cfs_time_current(), now), &tv);
                tms -= (int)(tv.tv_sec * 1000 + tv.tv_usec / 1000);
                if (tms < 0) { /* no more wait but may have new event */
                        rc = LNET_EV_CHECK_ONCE;
                        tms = 0;
                }
        }

        *timeout_ms = tms;
        LNET_OBJ_LOCK(objcd);
        cfs_waitq_del(&the_lnet.ln_waitq, &wl);

        return rc;
}

#else /* !__KERNEL__ */

static int
lnet_eq_wait_locked(lnet_obj_cpud_t *objcd, int *timeout_ms)
{
        lnet_net_cpud_t *netcd = lnet_netcd_default();
        lnet_ni_t       *eqwaitni = the_lnet.ln_eqwaitni;
        int              tms = *timeout_ms;
        int              rc  = LNET_EV_CHECK_MORE;
        struct timeval   then;
        struct timeval   now;

        if (eqwaitni == NULL) {
# ifndef HAVE_LIBPTHREAD
                /* If I'm single-threaded, LNET fails at startup if it can't
                 * set the_lnet.ln_eqwaitni correctly.  */
                LBUG();
# else /* !HAVE_LIBPTHREAD */
                cfs_private_lock_t *pl;
                pthread_mutex_t    *lock;
                struct timespec     ts;

                /* XXX: it's not right here, because cfs_spinlock_t is not
                 * defined as pthread_mutex_t even with HAVE_LIBPTHREAD,
                 * we need to fix it after cfs_spinlock_t is defined
                 * correctly for pthread */
                pl = (cfs_private_lock_t *)((void *)objcd - sizeof(*pl));
                lock = (pthread_mutex_t *)(&pl->pl_lock);
                LBUG();

                if (tms < 0) {
                        pthread_cond_wait(&the_lnet.ln_cond, lock);
                        goto out;
                }

                gettimeofday(&then, NULL);

                ts.tv_sec = then.tv_sec + tms / 1000;
                ts.tv_nsec = then.tv_usec * 1000 + (tms % 1000) * 1000000;
                if (ts.tv_nsec >= 1000000000) {
                        ts.tv_sec++;
                        ts.tv_nsec -= 1000000000;
                }

                pthread_cond_timedwait(&the_lnet.ln_cond, lock, &ts);

                gettimeofday(&now, NULL);
                tms -= (now.tv_sec - then.tv_sec) * 1000 +
                       (now.tv_usec - then.tv_usec) / 1000;
                if (tms < 0) { /* no more wait but may have new event */
                        rc = LNET_EV_CHECK_ONCE;
                        tms = 0;
                }
                goto out;
# endif /* HAVE_LIBPTHREAD */
        }

        /* I have a single NI that I have to call into, to get
         * events queued, or to block. */
        LNET_OBJ_UNLOCK(objcd);

        LNET_NET_LOCK(netcd);
        if (eqwaitni != the_lnet.ln_eqwaitni) {
                LNET_NET_UNLOCK(netcd);
                rc = LNET_EV_CHECK_NONE;
                goto out;
        }

        lnet_ni_addref_locked(netcd, eqwaitni, netcd->lnc_cpuid);
        LNET_NET_UNLOCK(netcd);

        if (tms <= 0) {
                (eqwaitni->ni_lnd->lnd_wait)(eqwaitni, tms);
                goto out;
        }

        gettimeofday(&then, NULL);

        (eqwaitni->ni_lnd->lnd_wait)(eqwaitni, tms);

        gettimeofday(&now, NULL);
        tms -= (now.tv_sec - then.tv_sec) * 1000 +
               (now.tv_usec - then.tv_usec) / 1000;

        if (tms < 0)
                tms = 0;
 out:
        if (tms == 0) /* timedout, no more wait */
                rc = LNET_EV_CHECK_ONCE;

        /* NB: lnet_ni_decref() always decref for lnet_netcd_default() */
        lnet_ni_decref(eqwaitni);

        *timeout_ms = tms;
        LNET_OBJ_LOCK(objcd);

        return rc;
}

#endif /* __KERNEL__ */

/**
 * Block the calling process until there's an event from a set of EQs or
 * timeout happens.
 *
 * If an event handler is associated with the EQ, the handler will run before
 * this function returns successfully, in which case the corresponding event
 * is consumed.
 *
 * LNetEQPoll() provides a timeout to allow applications to poll, block for a
 * fixed period, or block indefinitely.
 *
 * \param eventqs,neq An array of EQ handles, and size of the array.
 * \param timeout_ms Time in milliseconds to wait for an event to occur on
 * one of the EQs. The constant LNET_TIME_FOREVER can be used to indicate an
 * infinite timeout.
 * \param event,which On successful return (1 or -EOVERFLOW), \a event will
 * hold the next event in the EQs, and \a which will contain the index of the
 * EQ from which the event was taken.
 *
 * \retval 0          No pending event in the EQs after timeout.
 * \retval 1          Indicates success.
 * \retval -EOVERFLOW Indicates success (i.e., an event is returned) and that
 * at least one event between this event and the last event obtained from the
 * EQ indicated by \a which has been dropped due to limited space in the EQ.
 * \retval -ENOENT    If there's an invalid handle in \a eventqs.
 */
int
LNetEQPoll (lnet_handle_eq_t *eventqs, int neq, int timeout_ms,
            lnet_event_t *event, int *which)
{
        lnet_obj_cpud_t *objcd = lnet_objcd_default();
        int              check_op = LNET_EV_CHECK_MORE;
        int              i;
        int              rc;
        ENTRY;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        if (neq < 1)
                RETURN(-ENOENT);

        LNET_OBJ_LOCK(objcd);

        for (;;) {
#ifndef __KERNEL__
                LNET_OBJ_UNLOCK(objcd);

                /* Recursion breaker */
                if (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING &&
                    !LNetHandleIsEqual(eventqs[0], the_lnet.ln_rc_eqh))
                        lnet_router_checker();

                LNET_OBJ_LOCK(objcd);
#endif
                for (i = 0; i < neq; i++) {
                        lnet_eq_t *eq = lnet_handle2eq(objcd, &eventqs[i]);

                        if (eq == NULL || eq->eq_size == 0) {
                                LNET_OBJ_UNLOCK(objcd);
                                RETURN(-ENOENT);
                        }

                        rc = lib_get_event(objcd, eq, event);
                        if (rc != 0) {
                                LNET_OBJ_UNLOCK(objcd);
                                *which = i;
                                RETURN(rc);
                        }
                }

                if (check_op == LNET_EV_CHECK_ONCE)
                        break;

                check_op = lnet_eq_wait_locked(objcd, &timeout_ms);
                if (check_op == LNET_EV_CHECK_NONE)
                        break;
        }
        LNET_OBJ_UNLOCK(objcd);
        RETURN(0);
}
