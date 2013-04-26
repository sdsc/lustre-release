/**
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/**
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/**
 * Copyright (c) 2013, Intel Corporation.
 */
/**
 * This file is part of lustre/DAOS
 *
 * lustre/tests/daos/daos_test.h
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#ifndef __DAOS_TEST_H__
#define __DAOS_TEST_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <libcfs/libcfs.h>
#include <daos/daos.h>

#define DT_PRINT(fmt, ...)				\
do {							\
	fprintf(stdout, fmt, ## __VA_ARGS__);		\
	fflush(stdout);					\
} while (0)

#define DT_ERROR(fmt, ...)					\
do {								\
	fprintf(stderr, "%s:%d:%s() " fmt, __FILE__, __LINE__,	\
		__FUNCTION__, ## __VA_ARGS__);			\
	fflush(stderr);						\
} while (0)

#define DAOS_TEST_ENTER(fmt, ...)				\
	DT_PRINT("DAOS %s: " fmt, __FUNCTION__,  ## __VA_ARGS__)

#define DAOS_TEST_RETURN(rc)					\
do {								\
	if ((rc) == 0)						\
		DT_PRINT("DAOS %s: PASS\n", __FUNCTION__);	\
	else							\
		DT_PRINT("DAOS %s: FAILED\n", __FUNCTION__);	\
	fflush(stdout);						\
	return rc;						\
} while (0)

static inline void
dt_timeval_diff(struct timeval *tv1,
		struct timeval *tv2, struct timeval *df)
{
	struct timeval	tmp;

	if (tv2 == NULL) {
		gettimeofday(&tmp, NULL);
		tv2 = &tmp;
	}

	if (tv2->tv_usec >= tv1->tv_usec) {
		df->tv_sec  = tv2->tv_sec - tv1->tv_sec;
		df->tv_usec = tv2->tv_usec - tv1->tv_usec;
	} else {
		df->tv_sec  = tv2->tv_sec - 1 - tv1->tv_sec;
		df->tv_usec = tv2->tv_usec + 1000000 - tv1->tv_usec;
	}
}

static inline float
dt_timeval_diff_float(struct timeval *tv1, struct timeval *tv2)
{
	struct timeval	diff;

	dt_timeval_diff(tv1, tv2, &diff);
	return (float)diff.tv_sec + ((float)diff.tv_usec / 1000000);
}

static inline int
dt_cpus_online(void)
{
	int	cpus = 1;

#ifdef _SC_NPROCESSORS_ONLN
	cpus = sysconf(_SC_NPROCESSORS_ONLN);
#endif
	return cpus >= 1 ? cpus : 1;
}

/* task group lock */
typedef struct {
	pthread_mutex_t tl_mutex;
	pthread_cond_t  tl_cond;
	char            tl_data[0];
} dt_tg_lock_t;

dt_tg_lock_t *dt_tg_lock_create(int size);
void dt_tg_lock_destroy(dt_tg_lock_t *tg_lock);
void *dt_tg_lock_data(dt_tg_lock_t *tg_lock);
void dt_tg_lock(dt_tg_lock_t *tg_lock);
void dt_tg_unlock(dt_tg_lock_t *tg_lock);
void dt_tg_wait(dt_tg_lock_t *tg_lock);
void dt_tg_signal(dt_tg_lock_t *tg_lock);
void dt_tg_broadcast(dt_tg_lock_t *tg_lock);

struct dt_task_group;
typedef void *(*dt_tg_func_t)(int rank, struct dt_task_group *tg);

typedef struct dt_task_group {
	dt_tg_lock_t	*tg_lock;
	pthread_t	tg_tid;
	int		tg_pid;
	int		tg_error;
	int		tg_total;
	int		tg_starting;
	int		tg_running;
	int		tg_stopped;
	int		tg_barrier;
	int		tg_index;
	dt_tg_func_t	tg_run;
} dt_task_group_t;;

dt_task_group_t *dt_task_group_alloc(void);
void dt_task_group_free(dt_task_group_t *tg);
int dt_task_group_thread(dt_task_group_t *tg, int total, dt_tg_func_t run);
int dt_task_group_fork(dt_task_group_t *tg, int total, int *rank);
void dt_task_group_fail(dt_task_group_t *tg, int error);
int dt_task_group_barrier(dt_task_group_t *tg);
int dt_task_group_exit(dt_task_group_t *tg);

#define DT_TASK_GROUP_BARRIER(tg, rc, out)		\
do {							\
	rc = dt_task_group_barrier(tg);			\
	if (rc != 0)					\
		goto out;				\
} while (0)

#define DT_TASK_GROUP_CHECK(tg, rc, out, fmt, ...)	\
do {							\
	if (rc == 0)					\
		break;					\
	dt_task_group_fail(tg, rc);			\
	DT_ERROR("Error(rc:%d): " fmt,			\
		 rc, ## __VA_ARGS__);			\
	goto out;					\
} while (0)

void dt_events_free(daos_event_t *evs, int eventn);
daos_event_t *dt_events_alloc(int eventn, daos_event_t *parent,
			      daos_handle_t eqh);

extern int daos_noop(daos_event_t *ev, int latency);

#endif
