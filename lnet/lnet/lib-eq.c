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
        lnet_eq_t     *eq;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        /* We need count to be a power of 2 so that when eq_{enq,deq}_seq
         * overflow, they don't skip entries, so the queue has the same
         * apparent capacity at all times */

        if (count != LOWEST_BIT_SET(count)) {   /* not a power of 2 already */
                do {                    /* knock off all but the top bit... */
                        count &= ~LOWEST_BIT_SET (count);
                } while (count != LOWEST_BIT_SET(count));

                count <<= 1;                             /* ...and round up */
        }

        if (count == 0)        /* catch bad parameter / overflow on roundup */
                return (-EINVAL);

        eq = lnet_eq_alloc();
        if (eq == NULL)
                return (-ENOMEM);

        LIBCFS_ALLOC(eq->eq_events, count * sizeof(lnet_event_t));
        if (eq->eq_events == NULL) {
		lnet_eq_free(eq);

                return -ENOMEM;
        }

        /* NB this resets all event sequence numbers to 0, to be earlier
         * than eq_deq_seq */
        memset(eq->eq_events, 0, count * sizeof(lnet_event_t));

        eq->eq_deq_seq = 1;
        eq->eq_enq_seq = 1;
        eq->eq_size = count;
        eq->eq_refcount = 0;
        eq->eq_callback = callback;

        LNET_LOCK();

	lnet_res_lh_initialize(&the_lnet.ln_eq_container, &eq->eq_lh);
	cfs_list_add(&eq->eq_list, &the_lnet.ln_eq_container.rec_active);

        LNET_UNLOCK();

        lnet_eq2handle(handle, eq);
        return (0);
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
        lnet_eq_t     *eq;
        int            size;
        lnet_event_t  *events;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        LNET_LOCK();

        eq = lnet_handle2eq(&eqh);
        if (eq == NULL) {
                LNET_UNLOCK();
                return (-ENOENT);
        }

        if (eq->eq_refcount != 0) {
                CDEBUG(D_NET, "Event queue (%d) busy on destroy.\n",
                       eq->eq_refcount);
                LNET_UNLOCK();
                return (-EBUSY);
        }

        /* stash for free after lock dropped */
        events  = eq->eq_events;
        size    = eq->eq_size;

	lnet_res_lh_invalidate(&eq->eq_lh);
        cfs_list_del (&eq->eq_list);
	lnet_eq_free_locked(eq);

        LNET_UNLOCK();

        LIBCFS_FREE(events, size * sizeof (lnet_event_t));

        return 0;
}

void
lnet_eq_enqueue_event(lnet_eq_t *eq, lnet_event_t *ev)
{
	lnet_event_t	*eq_slot;

	/* Allocate the next queue slot */
	ev->sequence = eq->eq_enq_seq++;

	/* size must be a power of 2 to handle sequence # overflow */
	LASSERT(eq->eq_size != 0 &&
		eq->eq_size == LOWEST_BIT_SET(eq->eq_size));
	eq_slot = eq->eq_events + (ev->sequence & (eq->eq_size - 1));

	/* There is no race since both event consumers and event producers
	 * take the LNET_LOCK, so we don't screw around with memory
	 * barriers, setting the sequence number last or weird structure
	 * layout assertions. */
	*eq_slot = *ev;

	/* Call the callback handler (if any) */
	if (eq->eq_callback != NULL)
		eq->eq_callback(eq_slot);

#ifdef __KERNEL__
	/* Wake anyone waiting in LNetEQPoll() */
	if (cfs_waitq_active(&the_lnet.ln_eq_waitq))
		cfs_waitq_broadcast(&the_lnet.ln_eq_waitq);
#else
# ifndef HAVE_LIBPTHREAD
	/* LNetEQPoll() calls into _the_ LND to wait for action */
# else
	/* Wake anyone waiting in LNetEQPoll() */
	pthread_cond_broadcast(&the_lnet.ln_eq_cond);
# endif
#endif
}

int
lnet_eq_dequeue_event(lnet_eq_t *eq, lnet_event_t *ev)
{
        int           new_index = eq->eq_deq_seq & (eq->eq_size - 1);
        lnet_event_t *new_event = &eq->eq_events[new_index];
        int           rc;
        ENTRY;

        if (LNET_SEQ_GT (eq->eq_deq_seq, new_event->sequence)) {
                RETURN(0);
        }

        /* We've got a new event... */
        *ev = *new_event;

	CDEBUG(D_INFO, "event: %p, sequence: %lu, eq->size: %u\n",
	       new_event, eq->eq_deq_seq, eq->eq_size);

        /* ...but did it overwrite an event we've not seen yet? */
        if (eq->eq_deq_seq == new_event->sequence) {
                rc = 1;
        } else {
                /* don't complain with CERROR: some EQs are sized small
                 * anyway; if it's important, the caller should complain */
                CDEBUG(D_NET, "Event Queue Overflow: eq seq %lu ev seq %lu\n",
                       eq->eq_deq_seq, new_event->sequence);
                rc = -EOVERFLOW;
        }

        eq->eq_deq_seq = new_event->sequence + 1;
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

        return LNetEQPoll(&eventq, 1, 0,
                         event, &which);
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

        return LNetEQPoll(&eventq, 1, LNET_TIME_FOREVER,
                         event, &which);
}

#ifdef __KERNEL__

static int
lnet_eq_wait_locked(int *timeout_ms)
{
	int              tms = *timeout_ms;
	int              wait;
	cfs_waitlink_t   wl;
	cfs_time_t       now;

	if (tms == 0)
		return -1; /* don't want to wait and no new event */

	cfs_waitlink_init(&wl);
	cfs_set_current_state(CFS_TASK_INTERRUPTIBLE);
	cfs_waitq_add(&the_lnet.ln_eq_waitq, &wl);

	LNET_UNLOCK();

	if (tms < 0) {
		cfs_waitq_wait(&wl, CFS_TASK_INTERRUPTIBLE);

	} else {
		struct timeval tv;

		now = cfs_time_current();
		cfs_waitq_timedwait(&wl, CFS_TASK_INTERRUPTIBLE,
				    cfs_time_seconds(tms) / 1000);
		cfs_duration_usec(cfs_time_sub(cfs_time_current(), now), &tv);
		tms -= (int)(tv.tv_sec * 1000 + tv.tv_usec / 1000);
		if (tms < 0) /* no more wait but may have new event */
			tms = 0;
	}

	wait = tms != 0; /* might need to call here again */
	*timeout_ms = tms;

	LNET_LOCK();
	cfs_waitq_del(&the_lnet.ln_eq_waitq, &wl);

	return wait;
}

#else /* !__KERNEL__ */

# ifdef HAVE_LIBPTHREAD
static void
lnet_eq_cond_wait(struct timespec *ts)
{
	if (ts == NULL) {
		pthread_cond_wait(&the_lnet.ln_eq_cond, &the_lnet.ln_lock);
	} else {
		pthread_cond_timedwait(&the_lnet.ln_eq_cond,
				       &the_lnet.ln_lock, ts);
	}
}
# endif

static int
lnet_eq_wait_locked(int *timeout_ms)
{
	lnet_ni_t         *eq_waitni = NULL;
	int                tms = *timeout_ms;
	int                wait;
	struct timeval     then;
	struct timeval     now;

	if (the_lnet.ln_eq_waitni != NULL) {
		/* I have a single NI that I have to call into, to get
		 * events queued, or to block. */
		eq_waitni = the_lnet.ln_eq_waitni;
		lnet_ni_addref_locked(eq_waitni);

		LNET_UNLOCK();

		if (tms <= 0) { /* even for tms == 0 */
			(eq_waitni->ni_lnd->lnd_wait)(eq_waitni, tms);

		} else {
			gettimeofday(&then, NULL);

			(eq_waitni->ni_lnd->lnd_wait)(eq_waitni, tms);

			gettimeofday(&now, NULL);
			tms -= (now.tv_sec - then.tv_sec) * 1000 +
			       (now.tv_usec - then.tv_usec) / 1000;
			if (tms < 0)
				tms = 0;
		}

		LNET_LOCK();
		lnet_ni_decref_locked(eq_waitni);
	} else { /* w/o eq_waitni */
# ifndef HAVE_LIBPTHREAD
		/* If I'm single-threaded, LNET fails at startup if it can't
		 * set the_lnet.ln_eqwaitni correctly.  */
		LBUG();
# else /* HAVE_LIBPTHREAD */
		struct timespec  ts;

		if (tms == 0) /* don't want to wait and new event */
			return -1;

		if (tms < 0) {
			lnet_eq_cond_wait(NULL);

		} else {

			gettimeofday(&then, NULL);

			ts.tv_sec = then.tv_sec + tms / 1000;
			ts.tv_nsec = then.tv_usec * 1000 +
				     (tms % 1000) * 1000000;
			if (ts.tv_nsec >= 1000000000) {
				ts.tv_sec++;
				ts.tv_nsec -= 1000000000;
			}

			lnet_eq_cond_wait(&ts);

			gettimeofday(&now, NULL);
			tms -= (now.tv_sec - then.tv_sec) * 1000 +
			       (now.tv_usec - then.tv_usec) / 1000;
			if (tms < 0)
				tms = 0;
		}
# endif /* HAVE_LIBPTHREAD */
	}

	wait = tms != 0;
	*timeout_ms = tms;

	return wait;
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
	int	wait = 1;
	int	rc;
	int	i;
        ENTRY;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        if (neq < 1)
                RETURN(-ENOENT);

        LNET_LOCK();

        for (;;) {
#ifndef __KERNEL__
                LNET_UNLOCK();

                /* Recursion breaker */
                if (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING &&
                    !LNetHandleIsEqual(eventqs[0], the_lnet.ln_rc_eqh))
                        lnet_router_checker();

                LNET_LOCK();
#endif
                for (i = 0; i < neq; i++) {
                        lnet_eq_t *eq = lnet_handle2eq(&eventqs[i]);

                        if (eq == NULL) {
                                LNET_UNLOCK();
                                RETURN(-ENOENT);
                        }

			rc = lnet_eq_dequeue_event(eq, event);
                        if (rc != 0) {
                                LNET_UNLOCK();
                                *which = i;
                                RETURN(rc);
                        }
                }

		if (wait == 0)
			break;

		/*
		 * return value of lnet_eq_wait_locked:
		 * -1 : did nothing and it's sure no new event
		 *  1 : sleep inside and wait until new event
		 *  0 : don't want to wait anymore, but might have new event
		 *      so need to call dequeue again
		 */
		wait = lnet_eq_wait_locked(&timeout_ms);
		if (wait < 0) /* no new event */
			break;
	}

	LNET_UNLOCK();
	RETURN(0);
}
