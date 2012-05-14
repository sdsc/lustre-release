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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/libcfs/workitem.c
 *
 * Author: Isaac Huang <isaac@clusterfs.com>
 *         Liang Zhen  <zhen.liang@sun.com>
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <libcfs/libcfs.h>

typedef struct cfs_wi_sched {
#ifdef __KERNEL__
        /** serialised workitems */
        cfs_spinlock_t  ws_lock;
        /** where schedulers sleep */
        cfs_waitq_t     ws_waitq;
#endif
        /** concurrent workitems */
        cfs_list_t      ws_runq;
        /** rescheduled running-workitems */
        cfs_list_t      ws_rerunq;
	/** scheduler identifier */
	unsigned short	ws_id;
	/** initialized */
	__u8		ws_init:1;
	/** shutting down, protected by cfs_wi_data::wi_glock */
	__u8		ws_stopping:1;
	/** serialize starting thread, protected by cfs_wi_sched::ws_lock */
	__u8		ws_starting;
	/** started scheduler thread, protected by cfs_wi_sched::ws_lock */
	unsigned int	ws_nthreads;
} cfs_wi_sched_t;

struct cfs_workitem_data {
	/** WI module is initialized */
	int			wi_init;
	/** shutting down the whole WI module */
	int			wi_stopping;
	/** serialize */
	cfs_spinlock_t		wi_glock;
	/** non-affinity schedulers */
	struct cfs_wi_sched	*wi_scheds[CFS_WI_SCHED_ID_MAX];
	/** CPT affinity schedulers */
	struct cfs_wi_sched	**wi_scheds_cpt;
} cfs_wi_data;

static struct cfs_wi_sched **
cfs_wi_sched_address(unsigned short sched_id)
{
	unsigned short	fl = (sched_id & CFS_WI_SCHED_FL_MASK);
	unsigned short	id = (sched_id & CFS_WI_SCHED_ID_MASK);

	if (fl == CFS_WI_SCHED_FL_REG) {
		LASSERT(id  < CFS_WI_SCHED_ID_MAX);
		return &cfs_wi_data.wi_scheds[id];

	} else if (fl == CFS_WI_SCHED_FL_CPT) {
		LASSERT(id < cfs_cpt_number(cfs_cpt_table));
		return &cfs_wi_data.wi_scheds_cpt[id];
	} else {
		LBUG();
		return NULL;
	}
}

static inline cfs_wi_sched_t *
cfs_wi_to_sched(cfs_workitem_t *wi)
{
	struct cfs_wi_sched *sched = *cfs_wi_sched_address(wi->wi_sched_id);

	LASSERT(!sched->ws_stopping);
	return sched;
}

#ifdef __KERNEL__
static inline void
cfs_wi_sched_lock(cfs_wi_sched_t *sched)
{
        cfs_spin_lock(&sched->ws_lock);
}

static inline void
cfs_wi_sched_unlock(cfs_wi_sched_t *sched)
{
        cfs_spin_unlock(&sched->ws_lock);
}

static inline int
cfs_wi_sched_cansleep(cfs_wi_sched_t *sched)
{
	cfs_wi_sched_lock(sched);
	if (sched->ws_stopping) {
                cfs_wi_sched_unlock(sched);
                return 0;
        }

        if (!cfs_list_empty(&sched->ws_runq)) {
                cfs_wi_sched_unlock(sched);
                return 0;
        }
        cfs_wi_sched_unlock(sched);
        return 1;
}

#else /* !__KERNEL__ */

static inline void
cfs_wi_sched_lock(cfs_wi_sched_t *sched)
{
        cfs_spin_lock(&cfs_wi_data.wi_glock);
}

static inline void
cfs_wi_sched_unlock(cfs_wi_sched_t *sched)
{
        cfs_spin_unlock(&cfs_wi_data.wi_glock);
}

#endif /* __KERNEL__ */

/* XXX:
 * 0. it only works when called from wi->wi_action.
 * 1. when it returns no one shall try to schedule the workitem.
 */
void
cfs_wi_exit(cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_to_sched(wi);

	LASSERT(!cfs_in_interrupt()); /* because we use plain spinlock */
	LASSERT(!sched->ws_stopping);
	LASSERT(sched->ws_init);

        cfs_wi_sched_lock(sched);

#ifdef __KERNEL__
        LASSERT (wi->wi_running);
#endif
        if (wi->wi_scheduled) { /* cancel pending schedules */
                LASSERT (!cfs_list_empty(&wi->wi_list));
                cfs_list_del_init(&wi->wi_list);
        }

        LASSERT (cfs_list_empty(&wi->wi_list));
        wi->wi_scheduled = 1; /* LBUG future schedule attempts */

        cfs_wi_sched_unlock(sched);
        return;
}
EXPORT_SYMBOL(cfs_wi_exit);

/**
 * cancel a workitem:
 */
int
cfs_wi_cancel(cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_to_sched(wi);
        int             rc;

	LASSERT(!cfs_in_interrupt()); /* because we use plain spinlock */
	LASSERT(!sched->ws_stopping);
	LASSERT(sched->ws_init);

        cfs_wi_sched_lock(sched);
        /*
         * return 0 if it's running already, otherwise return 1, which
         * means the workitem will not be scheduled and will not have
         * any race with wi_action.
         */
        rc = !(wi->wi_running);

        if (wi->wi_scheduled) { /* cancel pending schedules */
                LASSERT (!cfs_list_empty(&wi->wi_list));
                cfs_list_del_init(&wi->wi_list);
                wi->wi_scheduled = 0;
        }

        LASSERT (cfs_list_empty(&wi->wi_list));

        cfs_wi_sched_unlock(sched);
        return rc;
}
EXPORT_SYMBOL(cfs_wi_cancel);

/*
 * Workitem scheduled with (serial == 1) is strictly serialised not only with
 * itself, but also with others scheduled this way.
 *
 * Now there's only one static serialised queue, but in the future more might
 * be added, and even dynamic creation of serialised queues might be supported.
 */
void
cfs_wi_schedule(cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_to_sched(wi);

	LASSERT(!cfs_in_interrupt()); /* because we use plain spinlock */
	LASSERT(!sched->ws_stopping);
	LASSERT(sched->ws_init);

        cfs_wi_sched_lock(sched);

        if (!wi->wi_scheduled) {
                LASSERT (cfs_list_empty(&wi->wi_list));

                wi->wi_scheduled = 1;
                if (!wi->wi_running) {
                        cfs_list_add_tail(&wi->wi_list, &sched->ws_runq);
#ifdef __KERNEL__
                        cfs_waitq_signal(&sched->ws_waitq);
#endif
                } else {
                        cfs_list_add(&wi->wi_list, &sched->ws_rerunq);
                }
        }

        LASSERT (!cfs_list_empty(&wi->wi_list));
        cfs_wi_sched_unlock(sched);
        return;
}
EXPORT_SYMBOL(cfs_wi_schedule);

#ifdef __KERNEL__

static int
cfs_wi_scheduler (void *arg)
{
	struct cfs_wi_sched	*sched = (cfs_wi_sched_t *)arg;
	char			name[16];
	unsigned short		id;
	unsigned short		fl;

	fl = (sched->ws_id & CFS_WI_SCHED_FL_MASK);
	id = (sched->ws_id & CFS_WI_SCHED_ID_MASK);

	if (fl == CFS_WI_SCHED_FL_REG) {
		snprintf(name, sizeof(name), "cfs_wi_r_%02d_%02d",
			 id, sched->ws_nthreads);

	} else if (fl == CFS_WI_SCHED_FL_CPT) {
		snprintf(name, sizeof(name), "cfs_wi_c_%02d_%02d",
			 id, sched->ws_nthreads);
	} else {
		snprintf(name, sizeof(name), "cfs_wi_%02d_%02d",
			 id, sched->ws_nthreads);
	}

	cfs_daemonize(name);
	cfs_block_allsigs();

	/* CPT affinity scheduler? */
	if ((sched->ws_id & CFS_WI_SCHED_FL_MASK) == CFS_WI_SCHED_FL_CPT) {
		int id = sched->ws_id & CFS_WI_SCHED_ID_MASK;

		LASSERT(id < cfs_cpt_number(cfs_cpt_table));
		cfs_cpt_bind(cfs_cpt_table, id);
	}

	cfs_wi_sched_lock(sched);

	LASSERT(sched->ws_starting == 1);
	sched->ws_starting--;
	sched->ws_nthreads++;

	while (!sched->ws_stopping) {
                int             nloops = 0;
                int             rc;
                cfs_workitem_t *wi;

                while (!cfs_list_empty(&sched->ws_runq) &&
                       nloops < CFS_WI_RESCHED) {
                        wi = cfs_list_entry(sched->ws_runq.next,
                                            cfs_workitem_t, wi_list);
                        LASSERT (wi->wi_scheduled && !wi->wi_running);

                        cfs_list_del_init(&wi->wi_list);

                        wi->wi_running   = 1;
                        wi->wi_scheduled = 0;
                        cfs_wi_sched_unlock(sched);
                        nloops++;

                        rc = (*wi->wi_action) (wi);

                        cfs_wi_sched_lock(sched);
                        if (rc != 0) /* WI should be dead, even be freed! */
                                continue;

                        wi->wi_running = 0;
                        if (cfs_list_empty(&wi->wi_list))
                                continue;

                        LASSERT (wi->wi_scheduled);
                        /* wi is rescheduled, should be on rerunq now, we
                         * move it to runq so it can run action now */
                        cfs_list_move_tail(&wi->wi_list, &sched->ws_runq);
                }

                if (!cfs_list_empty(&sched->ws_runq)) {
                        cfs_wi_sched_unlock(sched);
                        /* don't sleep because some workitems still
                         * expect me to come back soon */
                        cfs_cond_resched();
                        cfs_wi_sched_lock(sched);
                        continue;
                }

                cfs_wi_sched_unlock(sched);
                cfs_wait_event_interruptible_exclusive(sched->ws_waitq,
                                !cfs_wi_sched_cansleep(sched), rc);
                cfs_wi_sched_lock(sched);
        }

	sched->ws_nthreads--;
        cfs_wi_sched_unlock(sched);

        return 0;
}

#else /* __KERNEL__ */

int
cfs_wi_check_events (void)
{
        int               n = 0;
	int		  i;
        cfs_workitem_t   *wi;

        cfs_spin_lock(&cfs_wi_data.wi_glock);

        for (;;) {
		struct cfs_wi_sched	*sched;
		cfs_list_t		*q = NULL;

                /** rerunq is always empty for userspace */
		for (i = 0; q == NULL && i < CFS_WI_SCHED_ID_MAX; i++) {
			sched = cfs_wi_data.wi_scheds[i];
			if (sched != NULL && !cfs_list_empty(&sched->ws_runq))
				q = &sched->ws_runq;
		}

		for (i = 0; q == NULL &&
			    i < cfs_cpt_number(cfs_cpt_table); i++) {
			sched = cfs_wi_data.wi_scheds_cpt[i];
			if (sched != NULL && !cfs_list_empty(&sched->ws_runq))
				q = &sched->ws_runq;
		}

		if (q == NULL)
			break;

                wi = cfs_list_entry(q->next, cfs_workitem_t, wi_list);
                cfs_list_del_init(&wi->wi_list);

                LASSERT (wi->wi_scheduled);
                wi->wi_scheduled = 0;
                cfs_spin_unlock(&cfs_wi_data.wi_glock);

                n++;
                (*wi->wi_action) (wi);

                cfs_spin_lock(&cfs_wi_data.wi_glock);
        }

        cfs_spin_unlock(&cfs_wi_data.wi_glock);
        return n;
}

#endif

static void
cfs_wi_sched_init(struct cfs_wi_sched *sched, int sched_id)
{
	sched->ws_id = sched_id;
	sched->ws_nthreads  = 0;
	sched->ws_stopping = 0;
	sched->ws_starting = 0;
#ifdef __KERNEL__
        cfs_spin_lock_init(&sched->ws_lock);
        cfs_waitq_init(&sched->ws_waitq);
#endif
        CFS_INIT_LIST_HEAD(&sched->ws_runq);
        CFS_INIT_LIST_HEAD(&sched->ws_rerunq);
}

int
cfs_wi_sched_stop(unsigned short sched_id)
{
	struct cfs_wi_sched	**tmp;
	struct cfs_wi_sched	*sched;
	unsigned short		fl;
	unsigned short		id;
	int			rc = 0;

	LASSERT(cfs_wi_data.wi_init);

	id = sched_id & CFS_WI_SCHED_ID_MASK;
	fl = sched_id & CFS_WI_SCHED_FL_MASK;

	if ((fl != CFS_WI_SCHED_FL_REG && fl != CFS_WI_SCHED_FL_CPT) ||
	    (fl == CFS_WI_SCHED_FL_REG && id >= CFS_WI_SCHED_ID_MAX) ||
	    (fl == CFS_WI_SCHED_FL_CPT &&
	     id >= cfs_cpt_number(cfs_cpt_table))) {
		CDEBUG(D_INFO, "Invalid WI scheduler id: %u\n", sched_id);
		return -EINVAL;
	}

	cfs_spin_lock(&cfs_wi_data.wi_glock);

	tmp = cfs_wi_sched_address(sched_id);

	sched = *tmp;
	if (sched == NULL || sched->ws_stopping) {
		CDEBUG(D_INFO, "can't find WI scheduler %d or "
			       "it's in progress of stopping\n", sched_id);
		rc = sched == NULL ? -ENOENT : -EAGAIN;

	} else {
		LASSERT(sched->ws_init);
		sched->ws_stopping = 1;
	}

	cfs_spin_unlock(&cfs_wi_data.wi_glock);

	if (rc != 0)
		return rc;

#ifdef __KERNEL__
	cfs_wi_sched_lock(sched);

	cfs_waitq_broadcast(&sched->ws_waitq);
	rc = 2;
	while (sched->ws_nthreads > 0) {
		CDEBUG(IS_PO2(++rc) ? D_WARNING : D_NET,
		       "waiting for %d threads of WI sched[%d] to terminate\n",
		       sched->ws_nthreads, sched_id);

		cfs_wi_sched_unlock(sched);
		cfs_pause(cfs_time_seconds(1) / 20);
		cfs_wi_sched_lock(sched);
	}

	cfs_wi_sched_unlock(sched);
#endif

	cfs_spin_lock(&cfs_wi_data.wi_glock);

	LASSERT(*tmp == sched);
	*tmp = NULL;

	cfs_spin_unlock(&cfs_wi_data.wi_glock);

	LIBCFS_FREE(sched, sizeof(*sched));
	return 0;
}
EXPORT_SYMBOL(cfs_wi_sched_stop);

int
cfs_wi_sched_start(unsigned short sched_id, int nthrs)
{
	struct cfs_wi_sched	**tmp;
	struct cfs_wi_sched	*sched;
	unsigned short		fl;
	unsigned short		id;
	int			rc;

	LASSERT(cfs_wi_data.wi_init);
	LASSERT(!cfs_wi_data.wi_stopping);

	id = sched_id & CFS_WI_SCHED_ID_MASK;
	fl = sched_id & CFS_WI_SCHED_FL_MASK;

	if ((fl != CFS_WI_SCHED_FL_REG && fl != CFS_WI_SCHED_FL_CPT) ||
	    (fl == CFS_WI_SCHED_FL_REG && id >= CFS_WI_SCHED_ID_MAX) ||
	    (fl == CFS_WI_SCHED_FL_CPT &&
	     id >= cfs_cpt_number(cfs_cpt_table))) {
		CDEBUG(D_INFO, "Invalid WI scheduler id: %u\n", sched_id);
		return -EINVAL;
	}

	tmp = cfs_wi_sched_address(sched_id);
	if (*tmp != NULL) {
		CDEBUG(D_INFO, "WI scheduler %u existed\n", sched_id);
		return -EEXIST;
	}

	LIBCFS_ALLOC(sched, sizeof(*sched));
	if (sched == NULL)
		return -ENOMEM;

	cfs_wi_sched_init(sched, sched_id);

	cfs_spin_lock(&cfs_wi_data.wi_glock);
	if (*tmp != NULL) { /* recheck with lock */
		cfs_spin_unlock(&cfs_wi_data.wi_glock);

		CDEBUG(D_INFO, "WI scheduler %u existed\n", sched_id);
		LIBCFS_FREE(sched, sizeof(*sched));
		return -EEXIST;
	}

	*tmp = sched;
	cfs_spin_unlock(&cfs_wi_data.wi_glock);

	rc = 0;
#ifdef __KERNEL__
	while (nthrs > 0)  {
		cfs_wi_sched_lock(sched);
		while (sched->ws_starting > 0) {
			cfs_wi_sched_unlock(sched);
			cfs_schedule();
			cfs_wi_sched_lock(sched);
		}

		sched->ws_starting++;
		cfs_wi_sched_unlock(sched);

		rc = cfs_create_thread(cfs_wi_scheduler, sched, 0);
		if (rc >= 0) {
			nthrs--;
			continue;
		}

		cfs_wi_sched_lock(sched);
		sched->ws_starting--;
		cfs_wi_sched_unlock(sched);

		sched->ws_init = 1; /* don't LBUG cfs_wi_sched_stop */
		cfs_wi_sched_stop(sched_id);
		return rc;
	}
#endif

	sched->ws_init = 1;
	return 0;
}
EXPORT_SYMBOL(cfs_wi_sched_start);

int
cfs_wi_startup(void)
{
	if (cfs_cpt_number(cfs_cpt_table) > (1 << CFS_WI_SCHED_BITS)) {
		CERROR("Too many CPTs %d, workitem only reserved %d bits for "
		       "CPT id, please decrease CPT numbers for libcfs.\n",
		       cfs_cpt_number(cfs_cpt_table), CFS_WI_SCHED_BITS);
		return -EPERM;
	}

	memset(&cfs_wi_data, 0, sizeof(cfs_wi_data));

	cfs_spin_lock_init(&cfs_wi_data.wi_glock);

	LIBCFS_ALLOC(cfs_wi_data.wi_scheds_cpt,
		     sizeof(struct cfs_wi_sched *) *
			    cfs_cpt_number(cfs_cpt_table));

	if (cfs_wi_data.wi_scheds_cpt == NULL)
		return -ENOMEM;

	cfs_wi_data.wi_init = 1;
        return 0;
}

void
cfs_wi_shutdown (void)
{
	int	i;

	cfs_wi_data.wi_stopping = 1;

	for (i = 0; i < CFS_WI_SCHED_ID_MAX; i++) {
		if (cfs_wi_data.wi_scheds[i] != NULL)
			cfs_wi_sched_stop(i | CFS_WI_SCHED_FL_REG);
	}


	if (cfs_wi_data.wi_scheds_cpt != NULL) {
		for (i = 0; i < cfs_cpt_number(cfs_cpt_table); i++) {
			if (cfs_wi_data.wi_scheds_cpt[i] != NULL)
				cfs_wi_sched_stop(i | CFS_WI_SCHED_FL_CPT);
		}

		LIBCFS_FREE(cfs_wi_data.wi_scheds_cpt,
			    sizeof(struct cfs_wi_sched *) *
				   cfs_cpt_number(cfs_cpt_table));
        }

	cfs_wi_data.wi_stopping = 0;
	cfs_wi_data.wi_init = 0;
}
