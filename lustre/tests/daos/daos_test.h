#ifndef __DAOS_TEST_H__
#define __DAOS_TEST_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <libcfs/libcfs.h>
#include <daos/daos.h>

#define DT_PRINT(fmt, ...)				\
do {							\
	fprintf(stdout, fmt, ## __VA_ARGS__);		\
	fflush(stdout);					\
} while (0)

#define DT_ERROR(fmt, ...)				\
do {							\
	fprintf(stderr, fmt, ## __VA_ARGS__);		\
	fflush(stderr);					\
} while (0)

#define DAOS_TEST_ENTER(test_name)			\
	DT_PRINT("==== DAOS TEST: %s\n", test_name)

#define DAOS_TEST_RETURN(rc)				\
do {							\
	if ((rc) == 0)					\
		DT_PRINT("==== DAOS TEST: PASS\n");	\
	else						\
		DT_PRINT("==== DAOS TEST: FAILED\n");	\
	fflush(stdout);					\
	return rc;					\
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

/* task group lock */
typedef struct {
	pthread_mutex_t tl_mutex;
	pthread_cond_t  tl_cond;
	char            tl_data[0];
} dt_tg_lock_t;

typedef void *(*dt_tg_func_t)(void *arg);

dt_tg_lock_t *dt_tg_lock_create(int size);
void dt_tg_lock_destroy(dt_tg_lock_t *tg_lock);
void *dt_tg_lock_data(dt_tg_lock_t *tg_lock);
void dt_tg_lock(dt_tg_lock_t *tg_lock);
void dt_tg_unlock(dt_tg_lock_t *tg_lock);
void dt_tg_wait(dt_tg_lock_t *tg_lock);
void dt_tg_signal(dt_tg_lock_t *tg_lock);
void dt_tg_broadcast(dt_tg_lock_t *tg_lock);

typedef struct {
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
int dt_task_group_fork(dt_task_group_t *tg, int total);
void dt_task_group_fail(dt_task_group_t *tg, int error);
int dt_task_group_barrier(dt_task_group_t *tg);
int dt_task_group_stop(dt_task_group_t *tg);

#define DT_TASK_GROUP_BARRIER(tg, rc, out)		\
do {							\
	rc = dt_task_group_barrier(tg);			\
	if (rc != 0)					\
		goto out;				\
} while (0)

#define DT_TASK_GROUP_FAIL(tg, rc, out, fmt, ...)	\
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
