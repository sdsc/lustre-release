#include "daos_test.h"

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

void
dt_tg_lock_destroy(dt_tg_lock_t *tg_lock)
{
	pthread_mutex_destroy(&tg_lock->tl_mutex);
	pthread_cond_destroy(&tg_lock->tl_cond);
}

void *
dt_tg_lock_data(dt_tg_lock_t *tg_lock)
{
	return &tg_lock->tl_data[0];
}

void
dt_tg_lock(dt_tg_lock_t *tg_lock)
{
        pthread_mutex_lock(&tg_lock->tl_mutex);
}

void
dt_tg_unlock(dt_tg_lock_t *tg_lock)
{
        pthread_mutex_unlock(&tg_lock->tl_mutex);
}

void
dt_tg_wait(dt_tg_lock_t *tg_lock)
{
        pthread_cond_wait(&tg_lock->tl_cond, &tg_lock->tl_mutex);
}

void
dt_tg_signal(dt_tg_lock_t *tg_lock)
{
        pthread_cond_signal(&tg_lock->tl_cond);
}

void
dt_tg_broadcast(dt_tg_lock_t *tg_lock)
{
        pthread_cond_broadcast(&tg_lock->tl_cond);
}

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
	if (rc < 0) {
		dt_task_group_stop(tg);
		pthread_exit(NULL);
	}

	tg->tg_run(arg);
	return NULL;
}

int
dt_task_group_thread(dt_task_group_t *tg, int total, void *(*run)(void *arg))
{
	pthread_t       thread;
	int		rc = 0;
	int		i;

	LASSERT(tg->tg_total == 0);
	tg->tg_total	= total + 1; /* including myself */
	tg->tg_tid	= pthread_self();
	tg->tg_pid	= -1;
	tg->tg_running	= 1; /* myself */

	for (i = 0; i < tg->tg_total; i++) {
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
	return rc;
}

/**
 * NB: tg_lock and tg must be created in shared memory
 */
int
dt_task_group_fork(dt_task_group_t *tg, int total)
{
	int	rc = 0;
	int	pid;
	int	i;

	LASSERT(tg->tg_total == 0);
	tg->tg_total	= total + 1; /* including myself */
	tg->tg_pid	= getpid();
	tg->tg_running	= 1; /* myself */

	for (i = 0; i < tg->tg_total; i++) {
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
	if (rc >= 0)
		return rc;

	if (tg->tg_pid != getpid())
		exit(-1);

	return rc;
}

int
dt_proc_barrier(dt_task_group_t *tg)
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

void
dt_proc_fail(dt_task_group_t *tg, int error)
{
	if (error == 0)
		return;

	dt_tg_lock(tg->tg_lock);
	if (tg->tg_error == 0)
		tg->tg_error = error;

	dt_tg_broadcast(tg->tg_lock);
	dt_tg_unlock(tg->tg_lock);
}

int
dt_task_group_stop(dt_task_group_t *tg)
{
	dt_tg_lock_t	*tg_lock;
	int		error = 0;

	dt_tg_lock(tg->tg_lock);
	tg->tg_stopped++;
	/* NB: could be waken up by dt_proc_set_error */
	while (tg->tg_stopped < tg->tg_running)
		dt_tg_wait(tg->tg_lock);

	dt_tg_broadcast(tg->tg_lock);
	error = tg->tg_error;

	dt_tg_unlock(tg->tg_lock);

	tg_lock = tg->tg_lock;
	memset(tg, 0, sizeof(*tg));
	tg->tg_lock = tg_lock;

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
