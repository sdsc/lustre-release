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
 * lustre/tests/daos/daos_test.c
 *
 * helper functions for DAOS tests
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#include "daos_test.h"

/**
 * create a task group lock, it is pthread_mutex created in shared memory
 * and can be used as synchronization primitives for different processes
 *
 * \param size		extra bytes in shared memory
 */
dt_tg_lock_t *
dt_tg_lock_create(int size)
{
	dt_tg_lock_t		*tg_lock;
	pthread_mutexattr_t	mattr;
	pthread_condattr_t	cattr;
	int			shmid;
	int			rc;

	/* Create new segment */
	shmid = shmget(IPC_PRIVATE,
		       offsetof(dt_tg_lock_t, tl_data[size]), 0600);
	if (shmid == -1) {
		DT_ERROR("Can't create tg_lock: %s\n", strerror(errno));
		return NULL;
	}

	/* Attatch to new segment */
	tg_lock = (dt_tg_lock_t *)shmat(shmid, NULL, 0);

	if (tg_lock == (dt_tg_lock_t *)(-1)) {
		DT_ERROR("Can't attach tg_lock: %s\n", strerror(errno));
		return NULL;
	}

	/*
	 * Mark segment as destroyed, so it will disappear when we exit.
	 * Forks will inherit attached segments, so we should be OK.
	 */
	if (shmctl(shmid, IPC_RMID, NULL) == -1) {
                DT_ERROR("Can't destroy tg_lock: %s\n", strerror(errno));
		return NULL;
	}

	pthread_mutexattr_init(&mattr);
	pthread_condattr_init(&cattr);

	rc = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
	if (rc != 0) {
		DT_ERROR("Can't set shared mutex attr\n");
		return NULL;
	}

	rc = pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
	if (rc != 0) {
		DT_ERROR("Can't set shared cond attr\n");
		return NULL;
	}

	pthread_mutex_init(&tg_lock->tl_mutex, &mattr);
	pthread_cond_init(&tg_lock->tl_cond, &cattr);

	pthread_mutexattr_destroy(&mattr);
	pthread_condattr_destroy(&cattr);

	return tg_lock;
}

/** destroy a task group lock */
void
dt_tg_lock_destroy(dt_tg_lock_t *tg_lock)
{
	pthread_mutex_destroy(&tg_lock->tl_mutex);
	pthread_cond_destroy(&tg_lock->tl_cond);
	shmdt((void *)tg_lock);
}

/** return private data of task group lock */
void *
dt_tg_lock_data(dt_tg_lock_t *tg_lock)
{
	return &tg_lock->tl_data[0];
}

/** lock task group lock */
void
dt_tg_lock(dt_tg_lock_t *tg_lock)
{
        pthread_mutex_lock(&tg_lock->tl_mutex);
}

/** unlock task group lock */
void
dt_tg_unlock(dt_tg_lock_t *tg_lock)
{
        pthread_mutex_unlock(&tg_lock->tl_mutex);
}

/** wait on condition, it needs to be called with hold of tg lock */
void
dt_tg_wait(dt_tg_lock_t *tg_lock)
{
        pthread_cond_wait(&tg_lock->tl_cond, &tg_lock->tl_mutex);
}

/** wake up a process waiting on tg_lock */
void
dt_tg_signal(dt_tg_lock_t *tg_lock)
{
        pthread_cond_signal(&tg_lock->tl_cond);
}

/** wake up all processes waiting on tg_lock */
void
dt_tg_broadcast(dt_tg_lock_t *tg_lock)
{
        pthread_cond_broadcast(&tg_lock->tl_cond);
}

/** allocate a task group, see dt_task_group_fork() for details */
dt_task_group_t *
dt_task_group_alloc(void)
{
	dt_tg_lock_t	*tg_lock;
	dt_task_group_t *tg;

	tg_lock = dt_tg_lock_create(sizeof(*tg));
	if (tg_lock == NULL)
		return NULL;

	tg = dt_tg_lock_data(tg_lock);

	memset(tg, 0, sizeof(*tg));
	tg->tg_lock = tg_lock;

	return tg;
}

/** free a task group */
void
dt_task_group_free(dt_task_group_t *tg)
{
	LASSERT(tg->tg_lock != NULL);
	dt_tg_lock_destroy(tg->tg_lock);
}

static int
dt_task_group_enter(dt_task_group_t *tg, int error)
{
	int	rc;

	LASSERT(error >= 0);

	dt_tg_lock(tg->tg_lock);
	if ((tg->tg_pid >= 0 && tg->tg_pid == getpid()) ||
	    (tg->tg_pid < 0 && pthread_equal(pthread_self(), tg->tg_tid)))
		rc = 0;
	else
		rc = tg->tg_running++;

	if (error != 0 && tg->tg_error == 0)
		tg->tg_error = error;

	if (tg->tg_error != 0)
		rc = -tg->tg_error;

	if (tg->tg_running == tg->tg_total ||
	    (tg->tg_error != 0 && tg->tg_running == tg->tg_starting))
		dt_tg_broadcast(tg->tg_lock);
	else
		dt_tg_wait(tg->tg_lock);

	dt_tg_unlock(tg->tg_lock);
	return rc;
}

void *
dt_thread_main(void *arg)
{
	dt_task_group_t	*tg = (dt_task_group_t *)arg;
	int		rc;

	rc = dt_task_group_enter(tg, 0);
	if (rc < 0)
		dt_task_group_exit(tg);

	tg->tg_run(rc, tg);
	return NULL;
}

/**
 * Create \a total pthreads for a task group, it will return 0 if all threads
 * are created, or error number if any creation failed. All created threads
 * will exit if any creation failed.
 */
int
dt_task_group_thread(dt_task_group_t *tg, int total, dt_tg_func_t func)
{
	pthread_t       thread;
	int		rc = 0;
	int		i;

	LASSERT(tg->tg_total == 0);
	tg->tg_total	= total + 1; /* including myself */
	tg->tg_tid	= pthread_self();
	tg->tg_run	= func;
	tg->tg_pid	= -1;
	tg->tg_running	= 1; /* myself */

	for (i = 1; i < tg->tg_total; i++) {
		dt_tg_lock(tg->tg_lock);
		tg->tg_starting++;
		dt_tg_unlock(tg->tg_lock);

		rc = pthread_create(&thread, NULL, dt_thread_main, tg);
		if (rc == 0)
			continue;

		DT_ERROR("Failed to create pthread %d: %d\n", i, errno);
		rc = errno;
		break;
	}

	rc = dt_task_group_enter(tg, rc);
	if (rc < 0)
		dt_task_group_exit(tg);

	return rc;
}

/**
 * Fork \a total child processes for a task group, it will return 0 if all
 * chidlren are created, or error number if any creation failed. All child
 * processes will exit if any fork() failed.
 */
int
dt_task_group_fork(dt_task_group_t *tg, int total, int *rank)
{
	int	rc = 0;
	int	pid;
	int	i;

	LASSERT(tg->tg_total == 0);
	tg->tg_total	= total + 1; /* including myself */
	tg->tg_pid	= getpid();
	tg->tg_running	= 1; /* myself */

	for (i = 1; i < tg->tg_total; i++) {
		dt_tg_lock(tg->tg_lock);
		tg->tg_starting++;
		dt_tg_unlock(tg->tg_lock);

		pid = fork();
		if (pid == 0) /* child */
			break;
		else if (pid > 0) /* parent */
			continue;

		DT_ERROR("Failed to create child process: %d\n", errno);
		rc = errno;
		break;
	}

	rc = dt_task_group_enter(tg, rc);
	if (rc < 0) {
		dt_task_group_exit(tg);
		return rc;
	}

	if (rank != NULL)
		*rank = rc;
	return 0;
}

/**
 * Blocks until all processes/threads in this task group have reached this
 * routine or found there is an error.
 */
int
dt_task_group_barrier(dt_task_group_t *tg)
{
	int	rc;

	dt_tg_lock(tg->tg_lock);
	if (tg->tg_error != 0) {
		rc = tg->tg_error;
		dt_tg_unlock(tg->tg_lock);
		return rc;
	}

	tg->tg_barrier++;
	if (tg->tg_barrier < tg->tg_running) {
		dt_tg_wait(tg->tg_lock);
	} else {
		dt_tg_broadcast(tg->tg_lock);
		tg->tg_barrier = 0;
		tg->tg_index++;
		/* DT_DEBUG("Barrier ID: %d\n", tg->tg_index); */
	}
	/* NB: tp_error could have been changed when I was sleeping */
	rc = tg->tg_error;
	dt_tg_unlock(tg->tg_lock);
	return rc;
}

/** Set error for a task group */
void
dt_task_group_fail(dt_task_group_t *tg, int error)
{
	if (error == 0)
		return;

	dt_tg_lock(tg->tg_lock);
	if (tg->tg_error == 0)
		tg->tg_error = error;

	dt_tg_broadcast(tg->tg_lock);
	dt_tg_unlock(tg->tg_lock);
}

/**
 * exit from a task group until all threads/processes in this group have
 * reach this routine.
 */
int
dt_task_group_exit(dt_task_group_t *tg)
{
	int		error = 0;
	int		owner;
	int		pthread;

	dt_tg_lock(tg->tg_lock);

	pthread = tg->tg_pid < 0;
	owner = pthread ? pthread_equal(pthread_self(), tg->tg_tid) :
		tg->tg_pid == getpid();

	tg->tg_stopped++;
	/* NB: could be waken up by dt_task_group_fail */
	while (owner && tg->tg_stopped < tg->tg_running)
		dt_tg_wait(tg->tg_lock);

	dt_tg_broadcast(tg->tg_lock);
	error = tg->tg_error;

	dt_tg_unlock(tg->tg_lock);
	if (!owner) {
		if (pthread)
			pthread_exit(NULL);
		else
			exit(error);
	}
	return error;
}

void
dt_events_free(daos_event_t *evs, int eventn)
{
	int	i;

	if (evs == NULL)
		return;

	for (i = 0; i < eventn; i++)
		daos_event_fini(&evs[i]);

	free(evs);
}

daos_event_t *
dt_events_alloc(int eventn, daos_event_t *parent, daos_handle_t eqh)
{
	daos_event_t	*evs;
	int		rc;
	int		i;

	evs = calloc(eventn, sizeof(*evs));
	if (evs == NULL)
		return NULL;

	for (i = 0; i < eventn; i++) {
		rc = daos_event_init(&evs[i], eqh, parent);
		if (rc != 0) {
			DT_ERROR("Failed to initialize event %d: %d\n", i, rc);
			goto failed;
		}
	}
	return evs;
 failed:
	dt_events_free(evs, eventn);
	return NULL;
}
