#include "daos_test.h"

#define MY_EQ_COUNT	100
#define MY_EV_COUNT	(1000 * 1000)

static daos_handle_t	my_eqh;

static int
dt_eq_create_destroy(void)
{
	daos_handle_t	eqhs[MY_EQ_COUNT];
	int		rc;
	int		i;

	DAOS_TEST_ENTER("EQ create/destroy");

	for (i = 0; i < MY_EQ_COUNT; i++) {
		rc = daos_eq_create(&eqhs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to create EQ: %d\n", rc);
			goto out;
		}
	}

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
	daos_event_t	*evs;
	daos_event_t	*tmp;
	daos_event_t	parent;
	int		rc;
	int		rc2;
	int		i;

	DAOS_TEST_ENTER("event init/fini");
	rc = daos_eq_create(&my_eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evs = calloc(ev_count, sizeof(*evs));
	if (evs == NULL) {
		DT_ERROR("Failed to allocate %d events\n", ev_count);
		goto out;
	}

	DT_PRINT("Initialize %d events\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], my_eqh, NULL);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("Finalize %d events\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_fini(&evs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to finalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	rc = daos_event_init(&parent, my_eqh, NULL);
	if (rc != 0) {
		DT_ERROR("Failed to initialize parent event: %d\n", rc);
		goto out;
	}

	DT_PRINT("Initialize %d events with parent\n", ev_count);
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], my_eqh, &parent);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d "
				 "with parent: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("Iterate events via parent\n");
	for (tmp = NULL, i = 0;
	     (tmp = daos_event_next(&parent, tmp)) != NULL; i++) {
		if (tmp != &evs[i]) {
			DT_ERROR("%p is not expected event %p\n", tmp, &evs[i]);
			rc = -1;
			goto out;
		}
	}

	DT_PRINT("Finalize event with parent\n");
	rc = daos_event_fini(&evs[0]);
	if (rc != 0) {
		DT_ERROR("Failed to finalize with event with parent: %d\n", rc);
		goto out;
	}

	DT_PRINT("Finalize event with children\n");
	rc = daos_event_fini(&parent);
	if (rc != 0) {
		DT_ERROR("Failed to finalized parent event: %d\n", rc);
		goto out;
	}

	DT_PRINT("Child events should be finalized by parent\n");
	for (i = 0; i < ev_count; i++) {
		rc = daos_event_fini(&evs[i]);
		if (rc != -ENOENT) {
			DT_ERROR("Should return ENOENT while finalizing "
				 "dead event %d: %d\n", i, rc);
			goto out;
		}
	}

	for (i = 0; i < ev_count; i++) {
		rc = daos_event_init(&evs[i], my_eqh, NULL);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("Finalize inflight/completed event\n");
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
			DT_PRINT("Got 0 while finalizing completed event\n");
		} else {
			if (rc != -EBUSY) {
				DT_ERROR("Should get EBUSY for finalizing "
					 "inflight event: %d\n", rc);
				goto out;
			}
			DT_PRINT("Got EBUSY while finalizing inflight event\n");
		}
	}

	DT_PRINT("Destroy EQ with inflight events should return EBUSY\n");
	rc = daos_eq_destroy(my_eqh);
	if (rc != -EBUSY) {
		DT_PRINT("Should get EBUSY for finalizing EQ "
			 "with inflight event: %d\n", rc);
		goto out;
	}

	while (1) {
		rc = daos_eq_query(my_eqh, DAOS_EVQ_COMPLETED, 1, &tmp);
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
	DT_PRINT("Destroy EQ with completed & idle events\n");
	rc = 0;
out:
	rc2 = daos_eq_destroy(my_eqh);
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
	const int	ev_intv  = 4;
	daos_event_t	**evps   = NULL;
	daos_event_t	*evs     = NULL;
	struct timeval	then;
	struct timeval	diff;
	int		rc;
	int		i;

	DAOS_TEST_ENTER("event poll/query");
	rc = daos_eq_create(&my_eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evps = calloc(ev_count, sizeof(*evps));
	if (evps == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	evs = dt_events_alloc(ev_count, NULL, my_eqh);
	if (evs == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	DT_PRINT("Query empty EQ\n");
	rc = daos_eq_query(my_eqh, DAOS_EVQ_ALL, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to query event: %d\n", rc);
		goto out;
	}

	DT_PRINT("Poll on empty EQ with timeout(%d)\n", ev_intv);
	gettimeofday(&then, NULL);
	rc = daos_eq_poll(my_eqh, 0, ev_intv * 1000000, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		goto out;
	}
	dt_timeval_diff(&then, NULL, &diff);
	if (diff.tv_sec < ev_intv - 1) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, (int)diff.tv_sec);
		rc = -1;
		goto out;
	}

	DT_PRINT("Poll and wait only if there is inflight event\n");
	gettimeofday(&then, NULL);
	rc = daos_eq_poll(my_eqh, 1, ev_intv * 1000000, ev_count, evps);
	if (rc != 0) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		goto out;
	}
	dt_timeval_diff(&then, NULL, &diff);
	if (diff.tv_sec != 0) {
		DT_ERROR("Should not wait if there isn't inflight event\n");
		rc = -1;
		goto out;
	}

	rc = daos_noop(&evs[0], ev_intv);
	if (rc != 0) {
		DT_ERROR("Failed to submit event : %d\n", rc);
		goto out;
	}

	rc = daos_eq_query(my_eqh, DAOS_EVQ_ALL, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to query event: %d\n", rc);
		rc = -1;
		goto out;
	}

	gettimeofday(&then, NULL);
	rc = daos_eq_poll(my_eqh, 1, -1, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to poll event: %d\n", rc);
		rc = -1;
		goto out;
	}
	dt_timeval_diff(&then, NULL, &diff);
	if (diff.tv_sec < ev_intv - 1) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, (int)diff.tv_sec);
		rc = -1;
		goto out;
	}

	LASSERT(ev_count >= 2 && ev_count % 2 == 0);
	for (i = 0; i < ev_count; i++) {
		rc = daos_noop(&evs[i], i < ev_count / 2 ? 0 : ev_intv);
		if (rc != 0) {
			DT_ERROR("Failed to submit event %d: %d\n", i, rc);
			goto out;
		}
	}

	DT_PRINT("Query inflight event\n");
	rc = daos_eq_query(my_eqh, DAOS_EVQ_INFLIGHT, ev_count, evps);
	if (rc < ev_count / 2) {
		DT_ERROR("Failed to query inflight event: %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("Query completed event\n");
	while (1) {
		rc = daos_eq_query(my_eqh, DAOS_EVQ_COMPLETED, ev_count, evps);
		if (rc > 0)
			break;

		if (rc < 0) {
			DT_ERROR("Failed to query event: %d\n", rc);
			goto out;
		}
	}

	DT_PRINT("Poll completed events\n");
	for (i = 0; i < ev_count / 2; ) {
		rc = daos_eq_poll(my_eqh, 1, -1, ev_count, evps);
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

	DT_PRINT("Poll and drain EQ\n");
	gettimeofday(&then, NULL);
	for (i = 0; i < ev_count / 2; ) {
		rc = daos_eq_poll(my_eqh, 1, -1, ev_count, evps);
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

	dt_timeval_diff(&then, NULL, &diff);
	if (diff.tv_sec < ev_intv - 1) {
		DT_ERROR("Should have slept %d seconds, not %d\n",
			 ev_intv, (int)diff.tv_sec);
		goto out;
	}
	rc = 0;
out:
	if (evs != NULL)
		dt_events_free(evs, ev_count);

	if (evps != NULL)
		free(evps);

	daos_eq_destroy(my_eqh);
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
	int		rc;
	int		i;

	DAOS_TEST_ENTER("parent event poll/query");
	rc = daos_eq_create(&my_eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	rc = daos_event_init(&parent, my_eqh, NULL);
	if (rc != 0) {
		DT_ERROR("Failed to initialize parent event\n");
		goto out;
	}

	evs = dt_events_alloc(ev_count, &parent, my_eqh);
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

	DT_PRINT("Query inflight parent EQ\n");
	rc = daos_eq_query(my_eqh, DAOS_EVQ_INFLIGHT, ev_count, evps);
	if (rc != 1) {
		DT_ERROR("Failed to query inflight parent event: %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("Finalize parent event with inflight children "
		 "should return EBUSY\n");
	rc = daos_event_fini(&parent);
	if (rc != -EBUSY) {
		DT_ERROR("Should return EBUSY not %d\n", rc);
		rc = -1;
		goto out;
	}

	DT_PRINT("Query completed parent EQ\n");
	while (1) {
		rc = daos_eq_query(my_eqh, DAOS_EVQ_COMPLETED, ev_count, evps);
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

	rc = daos_eq_poll(my_eqh, 0, -1, 2, evps);
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

	daos_eq_destroy(my_eqh);
	DAOS_TEST_RETURN(rc);
}

static int
dt_event_poll_perf(void)
{
	daos_event_t	*evs = NULL;
	struct timeval	then;
	struct timeval	diff;
	int		rc;
	int		i;
	int		j;

	DAOS_TEST_ENTER("event performance");
	rc = daos_eq_create(&my_eqh);
	if (rc != 0) {
		DT_ERROR("Failed to create EQ: %d\n", rc);
		goto out;
	}

	evs = calloc(MY_EV_COUNT, sizeof(*evs));
	if (evs == NULL) {
		DT_ERROR("Failed to allocate %d events\n", MY_EV_COUNT);
		goto out;
	}

	gettimeofday(&then, NULL);
	for (i = 0; i < MY_EV_COUNT; i++) {
		rc = daos_event_init(&evs[i], my_eqh, NULL);
		if (rc != 0) {
			DT_ERROR("Failed to initalize event %d: %d\n", i, rc);
			goto out;
		}
	}
	dt_timeval_diff(&then, NULL, &diff);
	DT_PRINT("Initialized %d events in %f seconds\n", MY_EV_COUNT,
		 (float)diff.tv_sec + ((float)diff.tv_usec / 1000000));

	gettimeofday(&then, NULL);
	for (i = 0; i < MY_EV_COUNT; i++) {
		rc = daos_noop(&evs[i], 0);
		if (rc != 0) {
			DT_ERROR("Failed to submit event %d: %d\n", i, rc);
			goto out;
		}
	}
	dt_timeval_diff(&then, NULL, &diff);
	DT_PRINT("Submitted %d events in %f seconds\n", MY_EV_COUNT,
		 (float)diff.tv_sec + ((float)diff.tv_usec / 1000000));

	gettimeofday(&then, NULL);
	for (i = 0; i < MY_EV_COUNT; i++) {
		daos_event_t	*tmp;

		rc = daos_eq_poll(my_eqh, 0, -1, 1, &tmp);
		if (rc < 0) {
			DT_ERROR("Failed to poll event %d: %d\n", i, rc);
			goto out;
		}
		LASSERT(rc == 1);
	}
	dt_timeval_diff(&then, NULL, &diff);
	DT_PRINT("Polled %d events in %f seconds\n", MY_EV_COUNT,
		 (float)diff.tv_sec + ((float)diff.tv_usec / 1000000));

	gettimeofday(&then, NULL);
	for (i = j = 0; j < MY_EV_COUNT;) {
		daos_event_t	*tmps[8];

		if (i < MY_EV_COUNT) {
			rc = daos_noop(&evs[i], 0);
			if (rc != 0) {
				DT_ERROR("Failed to submit event %d: %d\n",
					  i, rc);
				goto out;
			}
			i++;
		}

		rc = daos_eq_poll(my_eqh, 0, 0, 8, tmps);
		if (rc < 0) {
			DT_ERROR("Failed to poll event %d: %d\n", i, rc);
			goto out;
		}
		LASSERT(rc <= 8);
		j += rc;
		LASSERT(j <= MY_EV_COUNT);
	}
	dt_timeval_diff(&then, NULL, &diff);
	DT_PRINT("Submitted and polled %d events in %f seconds\n", MY_EV_COUNT,
		 (float)diff.tv_sec + ((float)diff.tv_usec / 1000000));

	gettimeofday(&then, NULL);
	for (i = 0; i < MY_EV_COUNT; i++) {
		rc = daos_event_fini(&evs[i]);
		if (rc != 0) {
			DT_ERROR("Failed to finalize event %d: %d\n", i, rc);
			goto out;
		}
	}
	dt_timeval_diff(&then, NULL, &diff);
	DT_PRINT("Finalized %d events in %f seconds\n", MY_EV_COUNT,
		 (float)diff.tv_sec + ((float)diff.tv_usec / 1000000));

 out:
	if (evs != NULL)
		dt_events_free(evs, MY_EV_COUNT);

	daos_eq_destroy(my_eqh);

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

	rc = dt_event_poll_query();
	if (rc != 0)
		goto failed;

	rc = dt_event_poll_query_parent();
	if (rc != 0)
		goto failed;

	rc = dt_event_poll_perf();
	if (rc != 0)
		goto failed;

	return 0;
failed:
	return -1;
}
