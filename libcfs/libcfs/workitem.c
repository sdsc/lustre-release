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
        int             ws_id;
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
        /** serialize starting */
        int             ws_starting;
        /** started scheduler thread */
        int             ws_running;
        /** shutting down */
        int             ws_stopping;
} cfs_wi_sched_t;

struct cfs_workitem_data {
        /** serialize */
        cfs_spinlock_t    wi_glock;
        /** scheduler for serial workitem */
        cfs_wi_sched_t    wi_sched_serial;
        /** scheduler for non-affinity workitem */
        cfs_wi_sched_t    wi_sched_any;
        /** default scheduler */
        cfs_wi_sched_t  **wi_scheds;
} cfs_wi_data;

static cfs_wi_sched_t *
cfs_wi_sched_get(int sched_id)
{
        if (sched_id == CFS_WI_SCHED_SERIAL) {
                return &cfs_wi_data.wi_sched_serial;

        } else if (sched_id == CFS_WI_SCHED_ANY) {
                return &cfs_wi_data.wi_sched_any;

        } else {
                LASSERT(sched_id >= 0 && sched_id < cfs_cpu_node_num());
                return cfs_wi_data.wi_scheds != NULL ?
                       cfs_wi_data.wi_scheds[sched_id] : NULL;
        }
}

static inline cfs_wi_sched_t *
cfs_wi_to_sched(cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_sched_get(wi->wi_sched_id);

        return sched != NULL ? sched : &cfs_wi_data.wi_sched_any;
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

#else

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

#endif

/* XXX:
 * 0. it only works when called from wi->wi_action.
 * 1. when it returns no one shall try to schedule the workitem.
 */
void
cfs_wi_exit(cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_to_sched(wi);

        LASSERT (!cfs_in_interrupt()); /* because we use plain spinlock */
        LASSERT (!sched->ws_stopping);

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
CFS_EXPORT_SYMBOL(cfs_wi_exit);

/**
 * cancel a workitem:
 */
int
cfs_wi_cancel (cfs_workitem_t *wi)
{
        cfs_wi_sched_t *sched = cfs_wi_to_sched(wi);
        int             rc;

        LASSERT (!cfs_in_interrupt()); /* because we use plain spinlock */
        LASSERT (!sched->ws_stopping);

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

CFS_EXPORT_SYMBOL(cfs_wi_cancel);

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

        LASSERT (!cfs_in_interrupt()); /* because we use plain spinlock */
        LASSERT (!sched->ws_stopping);

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

CFS_EXPORT_SYMBOL(cfs_wi_schedule);

#ifdef __KERNEL__

static int
cfs_wi_scheduler (void *arg)
{
        cfs_wi_sched_t *sched = (cfs_wi_sched_t *)arg;
        char            name[24];

        if (sched->ws_id == CFS_WI_SCHED_SERIAL) {
                snprintf(name, sizeof(name), "cfs_wi_serial");
        } else if (sched->ws_id == CFS_WI_SCHED_ANY) {
                snprintf(name, sizeof(name),
                         "cfs_wi_x_%03d", sched->ws_running);
        } else {
                /* will be sched = &cfs_wi_data.wi_scheds[id] in the future */
                snprintf(name, sizeof(name),
                         "cfs_wi_%03d_%02d", sched->ws_id, sched->ws_running);
        }
        cfs_daemonize(name);

        cfs_block_allsigs();

        if (sched->ws_id >= 0)
                cfs_cpu_bind(sched->ws_id);

        cfs_wi_sched_lock(sched);

        LASSERT(sched->ws_starting == 1);
        sched->ws_starting--;
        sched->ws_running++;

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

        sched->ws_running--;
        cfs_wi_sched_unlock(sched);

        return 0;
}

#else /* __KERNEL__ */

int
cfs_wi_check_events (void)
{
        int               n = 0;
        cfs_workitem_t   *wi;
        cfs_list_t       *q;

        LASSERT(cfs_wi_data.wi_scheds == NULL);

        cfs_spin_lock(&cfs_wi_data.wi_glock);

        for (;;) {
                /** rerunq is always empty for userspace */
                if (!cfs_list_empty(&cfs_wi_data.wi_sched_serial.ws_runq))
                        q = &cfs_wi_data.wi_sched_serial.ws_runq;
                else if (!cfs_list_empty(&cfs_wi_data.wi_sched_any.ws_runq))
                        q = &cfs_wi_data.wi_sched_any.ws_runq;
                else
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
cfs_wi_sched_init(int sched_id)
{
        cfs_wi_sched_t *sched = cfs_wi_sched_get(sched_id);

        if (sched == NULL)
                return;

        sched->ws_id = sched_id;
        sched->ws_running  = 0;
        sched->ws_stopping = 0;
        sched->ws_starting = 0;
#ifdef __KERNEL__
        cfs_spin_lock_init(&sched->ws_lock);
        cfs_waitq_init(&sched->ws_waitq);
#endif
        CFS_INIT_LIST_HEAD(&sched->ws_runq);
        CFS_INIT_LIST_HEAD(&sched->ws_rerunq);
}

#ifdef __KERNEL__
void
cfs_wi_sched_stop(int sched_id)
{
        cfs_wi_sched_t *sched = cfs_wi_sched_get(sched_id);

        if (sched == NULL)
                return;

        cfs_wi_sched_lock(sched);
        LASSERT(sched->ws_starting == 0);

        sched->ws_stopping = 1;
        cfs_waitq_broadcast(&sched->ws_waitq);

        while (sched->ws_running > 0) {
                cfs_wi_sched_unlock(sched);
                cfs_pause(cfs_time_seconds(1) / 50);
                cfs_wi_sched_lock(sched);
        }

        sched->ws_stopping = 0;
        cfs_wi_sched_unlock(sched);
}

CFS_EXPORT_SYMBOL(cfs_wi_sched_stop);

int
cfs_wi_sched_start(int sched_id)
{
        cfs_wi_sched_t *sched = cfs_wi_sched_get(sched_id);
        int  nthrs;
        int  rc;
        int  i;

        if (sched == NULL)
                return 0;

        if (sched_id == CFS_WI_SCHED_SERIAL) {
                nthrs = 1;
        } else if (sched_id == CFS_WI_SCHED_ANY) {
                nthrs = max(cfs_cpu_node_weight(0), cfs_cpu_node_num()) >= 8 ?
                        4 : 2;
        } else {
                nthrs = min(cfs_cpu_node_weight(sched_id), 8);
        }

        LASSERT(!sched->ws_stopping);
        LASSERT(sched->ws_running == 0);

        for (rc = i = 0; i < nthrs; i++)  {
                cfs_wi_sched_lock(sched);
                while (sched->ws_starting > 0) {
                        cfs_wi_sched_unlock(sched);
                        cfs_schedule();
                        cfs_wi_sched_lock(sched);
                }

                sched->ws_starting++;
                cfs_wi_sched_unlock(sched);

                rc = cfs_kernel_thread(cfs_wi_scheduler, sched, 0);
                if (rc >= 0)
                        continue;

                cfs_wi_sched_lock(sched);
                sched->ws_starting--;
                cfs_wi_sched_unlock(sched);
                break;
        }

        if (rc >= 0)
                return 0;

        cfs_wi_sched_stop(sched_id);
        return rc;
}

CFS_EXPORT_SYMBOL(cfs_wi_sched_start);

#else

void
cfs_wi_sched_stop(int sched_id)
{
}

int
cfs_wi_sched_start(int sched_id)
{
        return 0;
}

#endif

int
cfs_wi_startup (void)
{
        int rc = 0;
        int i;

        cfs_spin_lock_init(&cfs_wi_data.wi_glock);

        cfs_wi_sched_init(CFS_WI_SCHED_ANY);
        cfs_wi_sched_init(CFS_WI_SCHED_SERIAL);

        if (cfs_cpu_node_num() == 1) {
                cfs_wi_data.wi_scheds = NULL;

        } else {
                cfs_wi_data.wi_scheds =
                            cfs_percpu_alloc(sizeof(cfs_wi_sched_t));
                if (cfs_wi_data.wi_scheds == NULL)
                        return -ENOMEM;

                cfs_cpus_for_each(i)
                        cfs_wi_sched_init(i);
        }

#ifdef __KERNEL__
        /* we only start CFS_WI_SCHED_ANY at here, other scheds are
         * for lnet_selftest */
        rc = cfs_wi_sched_start(CFS_WI_SCHED_ANY);
        if (rc != 0) {
                CERROR ("Can't spawn workitem scheduler: %d\n", rc);
                cfs_wi_shutdown();
        }
#else
        i = rc = 0;
#endif

        return rc;
}

void
cfs_wi_shutdown (void)
{
#ifdef __KERNEL__
        int i;

        cfs_wi_sched_stop(CFS_WI_SCHED_ANY);
        cfs_wi_sched_stop(CFS_WI_SCHED_SERIAL);

        if (cfs_wi_data.wi_scheds != NULL) {
                cfs_cpus_for_each(i)
                        cfs_wi_sched_stop(i);
                cfs_percpu_free(cfs_wi_data.wi_scheds);
        }
#endif
}
