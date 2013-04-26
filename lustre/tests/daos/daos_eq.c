#include "daos_test.h"

#define MY_EQ_COUNT	100

static int
dt_eq_create_destroy(void)
{
	daos_handle_t	eqhs[MY_EQ_COUNT];
	int		rc;
	int		i;

	DAOS_TEST_ENTER("EQ create/destroy");

	DT_PRINT("\tCreate %d events\n", MY_EQ_COUNT);
	for (i = 0; i < MY_EQ_COUNT; i++) {
		rc = daos_eq_create(&eqhs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to create EQ: %d\n", rc);
			goto out;
		}
	}

	DT_PRINT("\tDestroy %d events\n", MY_EQ_COUNT);
	for (i = 0; i < MY_EQ_COUNT; i++) {
		rc = daos_eq_destroy(eqhs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to destroy EQ: %d\n", rc);
			goto out;
		}
	}
out:
	DAOS_TEST_RETURN(rc);
}

static int
dt_event_init_fini(void)
{
	const int	ev_count = 10000;
	const int	ev_intv = 2;
	daos_event_t	*evs = NULL;
	daos_event_t	*tmp;
	daos_event_t	parent;
	daos_handle_t	eqh;
	int		rc;
	int		rc2;
	int		i;

	DAOS_TEST_ENTER("event init/fini");
	rc = daos_eq_create(&eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evs = calloc(ev_count, sizeof(*evs));
	if (evs == NULL) {
		DT_ERROR("Failed to allocate %d events\n", ev_count);
		goto out;
	}

	DT_PRINT("\tInitialize %d events\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], eqh, NULL);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("\tFinalize %d events\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_fini(&evs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to finalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	rc = daos_event_init(&parent, eqh, NULL);
	if (rc != 0) {
		DT_ERROR("Failed to initialize parent event: %d\n", rc);
		goto out;
	}

	DT_PRINT("\tInitialize %d events with parent\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], eqh, &parent);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d "
				 "with parent: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("\tIterate events via parent\n");
	for (tmp = NULL, i = 0;
	     (tmp = daos_event_next(&parent, tmp)) != NULL; i++) {
		if (tmp != &evs[i]) {
			DT_ERROR("%p is not expected event %p\n", tmp, &evs[i]);
			rc = -1;
			goto out;
		}
	}

	DT_PRINT("\tFinalize event with parent\n");
	rc = daos_event_fini(&evs[0]);
	if (rc != 0) {
		DT_ERROR("Failed to finalize with event with parent: %d\n", rc);
		goto out;
	}

	DT_PRINT("\tFinalize event with children\n");
	rc = daos_event_fini(&parent);
	if (rc != 0) {
		DT_ERROR("Failed to finalized parent event: %d\n", rc);
		goto out;
	}

	DT_PRINT("\tChild events should be finalized by parent\n");
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_fini(&evs[i]);
		if (rc != -ENOENT) {
			DT_ERROR("Should return ENOENT while finalizing "
				 "dead event %d: %d\n", i, rc);
			goto out;
		}
	}

	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], eqh, NULL);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("\tFinalize inflight/completed event\n");
	for (i = 0; i < 2; i++) {
		rc = daos_noop(&evs[i], i == 0 ? 0 : ev_intv);
		if (rc != 0) {
			DT_ERROR("Failed to submit event : %d\n", rc);
			goto out;
		}

		sleep(1);
		rc = daos_event_fini(&evs[i]);
		if (i == 0) {
			if (rc != 0) {
				DT_ERROR("Failed to finalize "
					 "completed event: %d\n", rc);
				goto out;
			}
			DT_PRINT("\tFinalizing completed event returned 0\n");
			continue;
		}

		if (rc != -EBUSY) {
			DT_ERROR("Should get EBUSY for finalizing "
				 "inflight event: %d\n", rc);
			goto out;
		}
		DT_PRINT("\tFinalizing inflight event returned EBUSY\n");
	}

	DT_PRINT("\tDestroy EQ with inflight events should return EBUSY\n");
	rc = daos_eq_destroy(eqh);
	if (rc != -EBUSY) {
		DT_PRINT("Should get EBUSY for finalizing EQ "
			 "with inflight event: %d\n", rc);
		goto out;
	}

	while (1) {
		rc = daos_eq_query(eqh, DAOS_EVQ_COMPLETED, 1, &tmp);
		if (rc < 0) {
			DT_ERROR("Failed to query EQ: %d\n", rc);
			goto out;
		}
		if (rc == 1) {
			LASSERT(tmp == &evs[1]);
			break;
		}
		sleep(1);
	}
	DT_PRINT("\tDestroy EQ with completed & idle events\n");
	rc = 0;
out:
	rc2 = daos_eq_destroy(eqh);
	if (rc2 != 0) {
		DT_ERROR("Failed to destroy EQ: %d\n", rc2);
		rc = rc2;
	}

	if (evs != NULL)
		free(evs);

	DAOS_TEST_RETURN(rc);
}

static int
dt_event_limit(void)
{
	/* XXX should get this value via sysctl */
	const int	ev_count = (1 << 20) + 1;
	daos_event_t	*evs = NULL;
	daos_handle_t	eqh;
	int		rc;
	int		rc2;
	int		i;

	DAOS_TEST_ENTER("event init/fini");
	rc = daos_eq_create(&eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evs = calloc(ev_count, sizeof(*evs));
	if (evs == NULL) {
		DT_ERROR("Failed to allocate %d events\n", ev_count);
		goto out;
	}

	DT_PRINT("\tInitialize %d events which exceeds upper limit "
		 "of events per EQ\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], eqh, NULL);
		if (rc == 0)
			continue;

		if (i != ev_count - 1) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}

		if (rc != -EMFILE) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
		DT_PRINT("\tGot EMFILE while exceeding limit\n");
		rc = 0;
	}
out:
	rc2 = daos_eq_destroy(eqh);
	if (rc2 != 0) {
		DT_ERROR("Failed to destroy EQ: %d\n", rc2);
		rc = rc2;
	}

	if (evs != NULL)
		free(evs);

	DAOS_TEST_RETURN(rc);
}

static int
dt_event_poll_query(void)
{
	const int	ev_count = 100;
	const int	ev_intv  = 2;
	daos_event_t	**evps   = NULL;
	daos_event_t	*evs     = NULL;
	daos_handle_t	eqh;
	struct timeval	then;
	int		diff;
	int		rc;
	int		i;

	DAOS_TEST_ENTER("event poll/query");
	rc = daos_eq_create(&eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evps = calloc(ev_count, sizeof(*evps));
	if (evps == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	evs = dt_events_alloc(ev_count, NULL, eqh);
	if (evs == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	DT_PRINT("\tQuery empty EQ\n");
	rc = daos_eq_query(eqh, DAOS_EVQ_ALL, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to query event: %d\n", rc);
		goto out;
	}

	DT_PRINT("\tPoll on empty EQ with timeout(%d)\n", ev_intv);
	/* poll() always wait with timeout */
	gettimeofday(&then, NULL);
	rc = daos_eq_poll(eqh, 0, ev_intv * 1000000, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		goto out;
	}

	diff = lrintf(dt_timeval_diff_float(&then, NULL));
	if (diff < ev_intv) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, diff);
		rc = -1;
		goto out;
	}

	DT_PRINT("\tPoll and wait only if there is inflight event\n");
	/* poll() should wait if there is inflight event */
	gettimeofday(&then, NULL);
	rc = daos_eq_poll(eqh, 1, ev_intv * 1000000, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		goto out;
	}

	diff = lrintf(dt_timeval_diff_float(&then, NULL));
	if (diff != 0) {
		DT_ERROR("Should not wait if there isn't inflight event\n");
		rc = -1;
		goto out;
	}

	/* submit an event after @ev_intv seconds */
	rc = daos_noop(&evs[0], ev_intv);
	if (rc != 0) {
		DT_ERROR("Failed to submit event : %d\n", rc);
		goto out;
	}

	rc = daos_eq_query(eqh, DAOS_EVQ_ALL, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to query event: %d\n", rc);
		rc = -1;
		goto out;
	}

	gettimeofday(&then, NULL);
	rc = daos_eq_poll(eqh, 1, -1, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		rc = -1;
		goto out;
	}

	diff = lrintf(dt_timeval_diff_float(&then, NULL));
	if (diff < ev_intv) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, diff);
		rc = -1;
		goto out;
	}

	LASSERT(ev_count >= 2 && ev_count % 2 == 0);
	for (i = 0; i < ev_count; i++) {
		/* submit the first half events without latency, and the
		 * second half with latency */
		rc = daos_noop(&evs[i], (i < ev_count / 2) ? 0 : ev_intv);
		if (rc != 0) {
			DT_ERROR("Failed to submit event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("\tQuery inflight event\n");
	rc = daos_eq_query(eqh, DAOS_EVQ_INFLIGHT, ev_count, evps);
	if (rc < ev_count / 2) {
		DT_ERROR("Failed to query inflight event: %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("\tQuery completed event\n");
	while (1) {
		rc = daos_eq_query(eqh, DAOS_EVQ_COMPLETED, ev_count, evps);
		if (rc > 0)
			break;

		if (rc < 0) {
			DT_ERROR("Failed to query event: %d\n", rc);
			goto out;
		}
	}

	DT_PRINT("\tPoll completed events\n");
	for (i = 0; i < ev_count / 2; ) {
		rc = daos_eq_poll(eqh, 1, -1, ev_count, evps);
		if (rc < 0) {
			DT_PRINT("Failed to poll event: %d\n", rc);
			goto out;
		}
		i += rc;
	}

	if (i != ev_count / 2) {
		DT_PRINT("Failed, more completion events then expected %d/%d\n",
			 i, ev_count / 2);
		goto out;
	}

	/* poll and wait for the second half events (with latency) */
	DT_PRINT("\tPoll and drain EQ\n");
	gettimeofday(&then, NULL);
	for (i = 0; i < ev_count / 2; ) {
		rc = daos_eq_poll(eqh, 1, -1, ev_count, evps);
		if (rc < 0) {
			DT_PRINT("Failed to poll event: %d\n", rc);
			goto out;
		}
		i += rc;
	}

	if (i != ev_count / 2) {
		DT_PRINT("Failed, more completion events then expected %d/%d\n",
			 i, ev_count / 2);
		goto out;
	}

	diff = lrintf(dt_timeval_diff_float(&then, NULL));
	if (diff < ev_intv) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, diff);
		goto out;
	}
	rc = 0;
out:
	if (evs != NULL)
		dt_events_free(evs, ev_count);

	if (evps != NULL)
		free(evps);

	daos_eq_destroy(eqh);
	DAOS_TEST_RETURN(rc);
}

static int
dt_event_poll_query_parent(void)
{
	const int	ev_count = 100;
	const int	ev_intv = 4;
	daos_event_t	*evs = NULL;
	daos_event_t	*evps[2];
	daos_event_t	parent;
	daos_handle_t	eqh;
	int		rc;
	int		i;

	DAOS_TEST_ENTER("parent event poll/query");
	rc = daos_eq_create(&eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	rc = daos_event_init(&parent, eqh, NULL);
	if (rc != 0) {
		DT_ERROR("Failed to initialize parent event\n");
		goto out;
	}

	evs = dt_events_alloc(ev_count, &parent, eqh);
	if (evs == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	for (i = 0; i < ev_count; i++) {
		rc = daos_noop(&evs[i], ev_intv);
		if (rc != 0) {
			DT_ERROR("Failed to submit event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("\tQuery inflight parent EQ\n");
	rc = daos_eq_query(eqh, DAOS_EVQ_INFLIGHT, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to query inflight parent event: %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("\tFinalize parent event with inflight children "
		 "should return EBUSY\n");
	rc = daos_event_fini(&parent);
	if (rc != -EBUSY) {
		DT_ERROR("Should return EBUSY not %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("\tQuery completed parent EQ\n");
	while (1) {
		rc = daos_eq_query(eqh, DAOS_EVQ_COMPLETED, ev_count, evps);
		if (rc == 1)
			break;

		if (rc == 0) {
			sleep(1);
			continue;
		}

		DT_ERROR("Failed to query inflight parent event: %d\n", rc);
		rc = -1;
		goto out;
	}

	rc = daos_eq_poll(eqh, 0, -1, 2, evps);
	if (rc != 1) {
		DT_ERROR("Failed to poll parent event: %d\n", rc);
		rc = -1;
		goto out;
	}

	rc = 0;
out:
	if (evs != NULL)
		dt_events_free(evs, ev_count);

	daos_event_fini(&parent);

	daos_eq_destroy(eqh);
	DAOS_TEST_RETURN(rc);
}

static int
dt_event_poll_perf(int procn, int eventn)
{
	dt_task_group_t *tg;
	daos_event_t	*evs = NULL;
	daos_handle_t	eqh;
	struct timeval	then;
	int		rank;
	int		rc2;
	int		rc;
	int		i;
	int		j;

	DAOS_TEST_ENTER("event performance");
	tg = dt_task_group_alloc();
	if (tg == NULL) {
		DT_ERROR("Failed to create task group\n");
		DAOS_TEST_RETURN(-1);
	}

	LASSERT(procn >= 1);
	rc = dt_task_group_fork(tg, procn - 1, &rank);
	if (rc != 0) {
		DT_ERROR("Failed to create tasks: %d\n", rc);
		dt_task_group_free(tg);
		DAOS_TEST_RETURN(-1);
	}

	rc = daos_eq_create(&eqh);
	DT_TASK_GROUP_CHECK(tg, rc, out, "Failed to create EQ: %d", rc);

	evs = calloc(eventn, sizeof(*evs));
	rc = evs == NULL ? -ENOMEM : 0;
	DT_TASK_GROUP_CHECK(tg, rc, out,
			    "Failed to allocate %d events\n", eventn);

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	gettimeofday(&then, NULL);

	for (i = 0; i < eventn; i++) {
		rc = daos_event_init(&evs[i], eqh, NULL);
		DT_TASK_GROUP_CHECK(tg, rc, out,
				    "Failed to initalize event %d: %d\n",
				    i, rc);
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	if (rank == 0) {
		DT_PRINT("\t%d process[es] initialized %d events "
			 "in %f seconds\n", procn, eventn * procn,
			 dt_timeval_diff_float(&then, NULL));
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	gettimeofday(&then, NULL);

	for (i = 0; i < eventn; i++) {
		rc = daos_noop(&evs[i], 0);
		DT_TASK_GROUP_CHECK(tg, rc, out,
				    "Failed to submit event %d: %d\n", i, rc);
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	if (rank == 0) {
		DT_PRINT("\t%d process[es] submitted %d events "
			 "in %f seconds\n", procn, eventn * procn,
			 dt_timeval_diff_float(&then, NULL));
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	gettimeofday(&then, NULL);

	for (i = 0; i < eventn; i++) {
		daos_event_t	*tmp;

		rc2 = daos_eq_poll(eqh, 0, -1, 1, &tmp);
		rc = rc2 < 0 ? rc2 : 0;
		DT_TASK_GROUP_CHECK(tg, rc, out,
				    "Failed to poll event %d: %d\n", i, rc);
		LASSERT(rc2 == 1);
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	if (rank == 0) {
		DT_PRINT("\t%d process[es] polled %d events in %f seconds\n",
			 procn, eventn * procn,
			 dt_timeval_diff_float(&then, NULL));
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	gettimeofday(&then, NULL);

	for (i = j = 0; j < eventn;) {
		daos_event_t	*tmps[8];

		if (i < eventn) {
			rc = daos_noop(&evs[i], 0);
			DT_TASK_GROUP_CHECK(tg, rc, out,
					    "Failed to submit event %d: %d\n",
					    i, rc);
			i++;
		}

		rc2 = daos_eq_poll(eqh, 0, 0, 8, tmps);
		rc = rc2 < 0 ? rc2 : 0;
		DT_TASK_GROUP_CHECK(tg, rc, out,
				    "Failed to poll event %d: %d\n", i, rc);
		LASSERT(rc2 <= 8);
		j += rc2;
	}
	LASSERT(j == eventn);

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	if (rank == 0) {
		DT_PRINT("\t%d process[es] submitted and polled %d events "
			 "in %f seconds\n", procn, eventn * procn,
			 dt_timeval_diff_float(&then, NULL));
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	gettimeofday(&then, NULL);

	for (i = 0; i < eventn; i++) {
		rc = daos_event_fini(&evs[i]);
		DT_TASK_GROUP_CHECK(tg, rc, out,
				    "Failed to finalize event %d: %d\n", i, rc);
	}

	DT_TASK_GROUP_BARRIER(tg, rc, out);
	if (rank == 0) {
		DT_PRINT("\t%d process[es] inalized %d events in %f seconds\n",
			 procn, eventn * procn,
			 dt_timeval_diff_float(&then, NULL));
	}
 out:
	if (evs != NULL)
		dt_events_free(evs, eventn);

	daos_eq_destroy(eqh);

	rc = dt_task_group_exit(tg);
	dt_task_group_free(tg);

	DAOS_TEST_RETURN(rc);
}

int
main(int argc, char **argv)
{
	int	rc;

	rc = dt_eq_create_destroy();
	if (rc != 0)
		goto failed;

	rc = dt_event_init_fini();
	if (rc != 0)
		goto failed;

	rc = dt_event_limit();
	if (rc != 0)
		goto failed;

	rc = dt_event_poll_query();
	if (rc != 0)
		goto failed;

	rc = dt_event_poll_query_parent();
	if (rc != 0)
		goto failed;

	rc = dt_event_poll_perf(1, 1000 * 1000);
	if (rc != 0)
		goto failed;

	if (dt_cpus_online() > 1) {
		rc = dt_event_poll_perf(dt_cpus_online(), 500 * 1000);
		if (rc != 0)
			goto failed;
	}
	return 0;
failed:
	return -1;
}
