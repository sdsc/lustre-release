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
 * This file is part of Lustre/DAOS
 *
 * lustre/daos/daos_eq.c
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#include "daos_internal.h"

static void daos_event_link_locked(struct daos_eq *eq, struct daos_kevent *ev);
static void daos_event_unlink_locked(struct daos_eq *eq, struct daos_kevent *ev,
				     int unlink_children);
static void daos_event_detach_locked(struct daos_eq *eq,
				    struct daos_kevent *ev);
static void daos_event_abort_locked(struct daos_eq *eq,
				    struct daos_kevent *ev, int unlink);
static int daos_event_complete_cb(struct daos_kevent *ev, int unlinked);
static int daos_event_poll_wait_locked(struct daos_eq *eq,
				       struct daos_kevent *ev);
static void daos_event_free(struct daos_kevent *ev);
static void
daos_eq_free(struct daos_hlink *hlink)
{
	struct daos_eq *eq = container_of(hlink, struct daos_eq, eq_hlink);

	LASSERT(list_empty(&eq->eq_idle));
	LASSERT(list_empty(&eq->eq_disp));
	LASSERT(list_empty(&eq->eq_comp));
	LASSERT(list_empty(&eq->eq_poll));
	LASSERT(eq->eq_pollwait == 0);
	LASSERTF(eq->eq_compn == 0 && eq->eq_dispn == 0 && eq->eq_total == 0,
		 "completed: %d, inflight: %d, total: %d\n",
		 eq->eq_compn, eq->eq_dispn, eq->eq_total);

	if (eq->eq_hash != NULL)
		cfs_hash_putref(eq->eq_hash);

	OBD_FREE_PTR(eq);
}

static struct daos_hlink_ops eq_h_ops = {
	.hop_free	= daos_eq_free,
};

struct daos_eq *
daos_eq_lookup(daos_handle_t eqh)
{
	struct daos_hlink *hlink;

	if ((eqh.cookie & DAOS_HTYPES_MASK) != DAOS_HTYPE_EQ) {
		CDEBUG(D_DAOS, "Cookie type is not EQ: "LPU64"\n",
		       (eqh.cookie & DAOS_HTYPES_MASK));
		return NULL;
	}

	hlink = daos_hlink_lookup(eqh.cookie);
	return hlink == NULL ?
	       NULL : container_of(hlink, struct daos_eq, eq_hlink);
}

void
daos_eq_putref(struct daos_eq *eq)
{
	daos_hlink_putref(&eq->eq_hlink);
}

void
daos_eq_delete(struct daos_eq *eq)
{
	daos_hlink_delete(&eq->eq_hlink);
}

void
daos_eq_insert(struct daos_eq *eq)
{
	daos_hlink_insert(DAOS_HTYPE_EQ, &eq->eq_hlink, &eq_h_ops);
}

void
daos_eq_handle(struct daos_eq *eq, daos_handle_t *h)
{
	daos_hlink_key(&eq->eq_hlink, &h->cookie);
}

#define DAOS_EQ_HASH_BITS	8
#define DAOS_EQ_HASH_BITS_MAX	18

extern cfs_hash_ops_t		daos_event_hops;

int
daos_eq_create(struct daos_dev_env *denv, struct daos_usr_eq_create *eqc)
{
	struct daos_eq		  *eq;
	daos_handle_t		   eqh;
	int			   rc;

	ENTRY;
	OBD_ALLOC_PTR(eq);
	if (eq == NULL)
		RETURN(-ENOMEM);

	eq->eq_cookie = 1; /* zero is reserved for invalid handle cookie */
	INIT_LIST_HEAD(&eq->eq_disp);
	INIT_LIST_HEAD(&eq->eq_idle);
	INIT_LIST_HEAD(&eq->eq_comp);
	INIT_LIST_HEAD(&eq->eq_poll);

	spin_lock_init(&eq->eq_lock);
	INIT_LIST_HEAD(&eq->eq_flink);
	daos_hlink_init(&eq->eq_hlink);
	init_waitqueue_head(&eq->eq_waitq);
	/* create event hash table */
	eq->eq_hash = cfs_hash_create("event_hash",
				      DAOS_EQ_HASH_BITS,
				      DAOS_EQ_HASH_BITS_MAX,
				      DAOS_EQ_HASH_BITS, 0,
				      CFS_HASH_MIN_THETA,
				      CFS_HASH_MAX_THETA,
				      &daos_event_hops,
				      CFS_HASH_REHASH |
				      CFS_HASH_NO_BKTLOCK |
				      CFS_HASH_NO_ITEMREF |
				      CFS_HASH_ASSERT_EMPTY);
	if (eq->eq_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Initialize EQ handle and add it into handle hash */
	spin_lock(&denv->den_lock);
	list_add_tail(&eq->eq_flink, &denv->den_eqs);
	spin_unlock(&denv->den_lock);

	daos_eq_insert(eq);
	daos_eq_handle(eq, &eqh);

	rc = copy_to_user(eqc->eqc_handle, &eqh, sizeof(eqh));
	if (rc != 0) {
		struct daos_usr_eq_destroy eqd;

		CERROR("Failed to return EQ handle\n");
		eqd.eqd_handle = eqh;
		eqd.eqd_force = 1;
		daos_eq_destroy(denv, &eqd);
		rc = -EFAULT;
	}
failed:
	daos_eq_putref(eq);
	RETURN(rc);
}

int
daos_eq_destroy(struct daos_dev_env *denv, struct daos_usr_eq_destroy *eqd)
{
	struct daos_eq		   *eq;
	struct daos_kevent	   *ev;
	struct list_head	    zombies;
	int			    rc = 0;

	ENTRY;
	eq = daos_eq_lookup(eqd->eqd_handle);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Can't find EQ by "LPX64"\n",
		       eqd->eqd_handle.cookie);
		RETURN(-ENOENT);
	}

	INIT_LIST_HEAD(&zombies);
	spin_lock(&eq->eq_lock);
	if (eq->eq_finalizing) {
		spin_unlock(&eq->eq_lock);
		CDEBUG(D_DAOS, "EQ is in progress of destroy\n");
		GOTO(out, rc = -ENOENT);
	}

	if (!list_empty(&eq->eq_disp) &&
	    !*the_daos_params.dmp_can_abort && !eqd->eqd_force) {
		spin_unlock(&eq->eq_lock);
		CDEBUG(D_DAOS, "Can't destroy EQ with inflight event\n");
		GOTO(out, rc = -EBUSY);
	}

	eq->eq_finalizing = 1;
	wake_up_all(&eq->eq_waitq); /* wakeup all polling threads */
	spin_unlock(&eq->eq_lock);

	daos_eq_delete(eq); /* remove it from handle table */

	spin_lock(&denv->den_lock);
	if (!list_empty(&eq->eq_flink))
		list_del_init(&eq->eq_flink);
	spin_unlock(&denv->den_lock);

	spin_lock(&eq->eq_lock);
	/* abort all inflight events */
	while (!list_empty(&eq->eq_disp)) {
		ev = list_entry(eq->eq_disp.prev, struct daos_kevent, ev_link);

		LASSERT(ev->ev_parent == NULL);
		/* NB: aborted event is always on the head of eq::eq_disp,
		 * which means if the last event is aborted, all events
		 * have been aborted */
		if (ev->ev_status == DAOS_EVS_ABORT)
			break;

		if (*the_daos_params.dmp_can_abort) {
			/* abort and unlink */
			daos_event_abort_locked(eq, ev, 1);
		} else {
			/* unlink only */
			daos_event_unlink_locked(eq, ev, 1);
		}

		if (need_resched()) { /* can be a very long list... */
			spin_unlock(&eq->eq_lock);
			cond_resched();
			spin_lock(&eq->eq_lock);
		}
	}

	/* wait until polling list is emtpy, otherwise it is too complex
	 * to handle race because polling thread has no lock on event */
	while (!list_empty(&eq->eq_poll)) {
		ev = list_entry(eq->eq_poll.next, struct daos_kevent, ev_link);
		daos_event_poll_wait_locked(eq, ev);
	}

	/* unlink all completed & unlink events */
	while (!list_empty(&eq->eq_comp) || !list_empty(&eq->eq_idle)) {
		ev = list_empty(&eq->eq_comp) ?
		     list_entry(eq->eq_idle.next, struct daos_kevent, ev_link) :
		     list_entry(eq->eq_comp.next, struct daos_kevent, ev_link);

		daos_event_unlink_locked(eq, ev, 1);
		daos_event_detach_locked(eq, ev);
		list_add(&ev->ev_link, &zombies);

		if (need_resched()) { /* can be a very long list... */
			spin_unlock(&eq->eq_lock);
			cond_resched();
			spin_lock(&eq->eq_lock);
		}
	}

	spin_unlock(&eq->eq_lock);

	while (!list_empty(&zombies)) {
		ev = list_entry(zombies.next, struct daos_kevent, ev_link);

		LASSERT(ev->ev_parent == NULL);
		list_del_init(&ev->ev_link);
		/* callback op_complete for those completed events, e.g.
		 * release all references taken by this event */
		if (ev->ev_status == DAOS_EVS_COMPLETE)
			daos_event_complete_cb(ev, 1);
		daos_event_free(ev);
	}
 out:
	daos_eq_putref(eq);
	RETURN(rc);
}

static int
daos_event_copy_out(struct daos_kevent *ev, daos_event_t __user **uevent_pp)
{
	daos_event_t	 *uevent = ev->ev_uevent;

	if (put_user(ev->ev_error, &uevent->ev_status) != 0) {
		CERROR("Failed to copy event status to userspace\n");
		return -EFAULT;
	}

	if (copy_to_user(uevent_pp, &uevent, sizeof(*uevent_pp)) != 0) {
		CERROR("Failed to copy event address to userspace\n");
		return -EFAULT;
	}

	return 0;
}

int
daos_eq_poll(struct daos_dev_env *denv, struct daos_usr_eq_poll *eqp)
{
	struct daos_eq		*eq;
	wait_queue_t		 wait;
	int			 rc = 0;
	int			 count = 0;

	ENTRY;
	if (eqp->eqp_ueventn == 0 || eqp->eqp_uevents == NULL) {
		CDEBUG(D_DAOS, "Invalid parameter, eventn: %d, event_pp: %p\n",
		       eqp->eqp_ueventn, eqp->eqp_uevents);
		RETURN(-EINVAL);
	}

	eq = daos_eq_lookup(eqp->eqp_handle);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Invalid EQ handle "LPU64"\n",
		       eqp->eqp_handle.cookie);
		RETURN(-ENOENT);
	}

	init_waitqueue_entry(&wait, current);

	spin_lock(&eq->eq_lock);
	if (unlikely(eq->eq_error != 0)) {
		CDEBUG(D_DAOS, "Failed to poll from EQ: %d\n", eq->eq_error);
		GOTO(out, rc = eq->eq_error);
	}
 again:
	while (!list_empty(&eq->eq_comp) && !eq->eq_finalizing) {
		struct daos_kevent *ev;

		ev = list_entry(eq->eq_comp.next, struct daos_kevent, ev_link);
		LASSERT(ev->ev_status == DAOS_EVS_COMPLETE);
		/* child event should never be here */
		LASSERT(ev->ev_parent == NULL);

		LASSERT(eq->eq_compn > 0);
		eq->eq_compn--;

		ev->ev_ref++; /* +1 for polling */
		/* move it to eq::eq_poll so another thread can not
		 * see this event while polling */
		list_move(&ev->ev_link, &eq->eq_poll);
		spin_unlock(&eq->eq_lock);

		/* NB: must release EQ lock because these functions need to
		 * copy data to userspace */
		rc = daos_event_complete_cb(ev, 0);
		if (rc == 0)
			rc = daos_event_copy_out(ev, &eqp->eqp_uevents[count]);

		spin_lock(&eq->eq_lock);

		/* nobody can do anything on this event while I'm polling */
		LASSERT(ev->ev_status == DAOS_EVS_COMPLETE);
		ev->ev_status = DAOS_EVS_NONE;

		LASSERT(ev->ev_ref == 2);
		ev->ev_ref--;

		ev->ev_error = 0;
		ev->ev_childn_comp = 0;

		list_move_tail(&ev->ev_link, &eq->eq_idle);
		/* someone is waiting for poll completion, e.g. event_fini */
		if (unlikely(eq->eq_pollwait != 0))
			wake_up_all(&eq->eq_waitq);

		if (rc != 0) {
			eq->eq_error = rc;
			GOTO(out, rc);
		}

		if (++count == eqp->eqp_ueventn)
			break;
	}

	if (eq->eq_finalizing) { /* EQ is unlinked */
		CDEBUG(D_DAOS, "EQ is finalizing\n");
		GOTO(out, rc = -ENOENT);
	}

	if (count > 0) /* have got completion event */
		GOTO(out, rc = count);

	/* is there any inflight event? */
	if (eqp->eqp_wait_inf && list_empty(&eq->eq_disp))
		GOTO(out, rc);

	if (eqp->eqp_timeout == 0) /* caller does not want to wait at all */
		GOTO(out, rc);

	set_current_state(TASK_INTERRUPTIBLE);
	add_wait_queue(&eq->eq_waitq, &wait);
	spin_unlock(&eq->eq_lock);

	if (eqp->eqp_timeout < 0) { /* wait forever */
		schedule();
	} else { /* timed wait */
		struct timespec ts = ns_to_timespec(eqp->eqp_timeout * 1000);
		unsigned long   to;

		to = schedule_timeout(timespec_to_jiffies(&ts));
		if (to == 0)
			eqp->eqp_timeout = 0;
		else
			eqp->eqp_timeout = jiffies_to_usecs(to);
	}

	spin_lock(&eq->eq_lock);
	set_current_state(TASK_RUNNING);
	remove_wait_queue(&eq->eq_waitq, &wait);

	if (!signal_pending(current))
		goto again; /* recheck */

	rc = -EINTR;
 out:
	spin_unlock(&eq->eq_lock);
	daos_eq_putref(eq);
	RETURN(rc);
}

/* EQ query iterator */
struct daos_eqq_it {
	daos_event_t		__user *it_uevent;
};

/* EQ query buffer */
struct daos_eqq_buf {
	struct list_head	eb_list;
	struct daos_eqq_it	eb_its[0];
};

/* EQ query buffer cursor */
struct daos_eqq_buf_cur {
	struct daos_eqq_buf	*ec_buf;
	int			ec_cur;
};

/* allocate buffer for EQ query */
static int
daos_eq_query_buf_alloc(struct list_head *head, int count)
{
	struct daos_eqq_buf	*buf;
	int			num;
	int			i;

	num = (CFS_PAGE_SIZE - sizeof(*buf)) / sizeof(struct daos_eqq_it);
	for (i = 0; count > 0; i++) {
		OBD_ALLOC(buf, CFS_PAGE_SIZE);
		if (buf == NULL)
			break;

		count -= num;
		list_add(&buf->eb_list, head);
	}
	return i * num;
}

/* free buffer for EQ query */
static void
daos_eq_query_buf_free(struct list_head *head)
{
	struct daos_eqq_buf	*buf;

	while (!list_empty(head)) {
		buf = list_entry(head->next, struct daos_eqq_buf, eb_list);
		list_del(&buf->eb_list);
		OBD_FREE(buf, CFS_PAGE_SIZE);
	}
}

/* move buffer cursor ahead */
static struct daos_eqq_it *
daos_eq_query_buf_next(struct list_head *head, struct daos_eqq_buf_cur *cur)
{
	LASSERT(!list_empty(head));

	if (cur->ec_buf == NULL) {
		cur->ec_cur = 0;
		cur->ec_buf = list_entry(head->next, struct daos_eqq_buf,
					 eb_list);
	}

	if ((void *)&cur->ec_buf->eb_its[cur->ec_cur + 1] -
	    (void *)(cur->ec_buf) > CFS_PAGE_SIZE) {
		if (cur->ec_buf->eb_list.next == head)
			return NULL;

		cur->ec_cur = 0;
		cur->ec_buf = list_entry(cur->ec_buf->eb_list.next,
					 struct daos_eqq_buf, eb_list);
	}
	return &cur->ec_buf->eb_its[cur->ec_cur++];
}

int
daos_eq_query(struct daos_dev_env *denv, struct daos_usr_eq_query *eqq)
{
	struct daos_eq		 *eq;
	struct daos_kevent	 *ev;
	struct daos_eqq_it	 *it;
	struct list_head	  head;
	struct daos_eqq_buf_cur   cur;
	int			  rc = 0;

	ENTRY;
	INIT_LIST_HEAD(&head);
	/* in case user passed in a crazy number... */
	eqq->eqq_ueventn = min_t(int, DAOS_EQ_EVN_MAX, eqq->eqq_ueventn);

	eq = daos_eq_lookup(eqq->eqq_handle);
	if (eq == NULL)
		RETURN(-ENOENT);

	spin_lock(&eq->eq_lock);
	if (eq->eq_finalizing || eq->eq_error) {
		rc = eq->eq_error != 0 ? -eq->eq_error : -ENOENT;
		spin_unlock(&eq->eq_lock);
		GOTO(out, rc);
	}

	if ((eqq->eqq_query & DAOS_EVQ_COMPLETED) != 0)
		rc += eq->eq_compn;

	if ((eqq->eqq_query & DAOS_EVQ_INFLIGHT) != 0)
		rc += eq->eq_dispn;

	spin_unlock(&eq->eq_lock);

	if (eqq->eqq_ueventn == 0 || eqq->eqq_uevents == NULL)
		GOTO(out, rc);

	/* give 256 extra slots in case there are more events coming */
	rc = min_t(int, rc + 256, eqq->eqq_ueventn);
	if (daos_eq_query_buf_alloc(&head, rc) == 0)
		GOTO(out, rc = -ENOMEM);

	rc = 0;
	memset(&cur, 0, sizeof(cur));

	spin_lock(&eq->eq_lock);
	if (eq->eq_finalizing || eq->eq_error) {
		rc = eq->eq_error != 0 ? -eq->eq_error : -EINTR;
		spin_unlock(&eq->eq_lock);
		GOTO(out, rc);
	}

	if ((eqq->eqq_query & DAOS_EVQ_COMPLETED) != 0) {
		list_for_each_entry(ev, &eq->eq_comp, ev_link) {
			LASSERT(ev->ev_parent == NULL);
			it = daos_eq_query_buf_next(&head, &cur);
			if (it == NULL)
				GOTO(do_copy, rc);

			it->it_uevent = ev->ev_uevent;
			if (++rc == eqq->eqq_ueventn)
				GOTO(do_copy, rc);
		}
	}

	if ((eqq->eqq_query & DAOS_EVQ_INFLIGHT) != 0) {
		list_for_each_entry(ev, &eq->eq_disp, ev_link) {
			LASSERT(ev->ev_parent == NULL);
			it = daos_eq_query_buf_next(&head, &cur);
			if (it == NULL)
				GOTO(do_copy, rc);

			it->it_uevent = ev->ev_uevent;
			if (++rc == eqq->eqq_ueventn)
				GOTO(do_copy, rc);
		}
	}
 do_copy:
	spin_unlock(&eq->eq_lock);

	memset(&cur, 0, sizeof(cur)); /* reset cursor */
	for (rc = 0; rc < eqq->eqq_ueventn; rc++) {
		it = daos_eq_query_buf_next(&head, &cur);
		if (it == NULL || it->it_uevent == NULL)
			break;

		if (copy_to_user(&eqq->eqq_uevents[rc],
				 &it->it_uevent, sizeof(it->it_uevent)) != 0) {
			CERROR("Failed to copy event address to userspace\n");
			GOTO(out, rc = -EFAULT);
		}
	}
 out:
	daos_eq_query_buf_free(&head);
	daos_eq_putref(eq);
	RETURN(rc);
}

static unsigned int
daos_event_hop_hash(cfs_hash_t *hs, const void *key, unsigned int mask)
{
	return (*(__u64 *)key) & mask;
}

static void *
daos_event_hop_obj(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct daos_kevent, ev_hnode);
}

static void *
daos_event_hop_key(cfs_hlist_node_t *hnode)
{
	struct daos_kevent *ev = daos_event_hop_obj(hnode);

	return &ev->ev_cookie;
}

static int
daos_event_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct daos_kevent *ev = daos_event_hop_obj(hnode);

	return *(__u64 *)key == ev->ev_cookie;
}

static void
daos_event_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	/* NB: noop, event refcount is managed outside hashtable */
}

static void
daos_event_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	/* NB: noop, event refcount is managed outside hashtable */
}

static void
daos_event_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct daos_kevent *ev = daos_event_hop_obj(hnode);

	OBD_FREE_PTR(ev);
}

cfs_hash_ops_t daos_event_hops = {
	.hs_hash        = daos_event_hop_hash,
	.hs_key         = daos_event_hop_key,
	.hs_keycmp      = daos_event_hop_keycmp,
	.hs_keycpy      = NULL,
	.hs_object      = daos_event_hop_obj,
	.hs_get         = daos_event_hop_get,
	.hs_put_locked  = daos_event_hop_put,
	.hs_put         = daos_event_hop_put,
	.hs_exit        = daos_event_hop_exit,
};

/** add an event to hash of EQ, setup cookie etc */
static void
daos_event_link_locked(struct daos_eq *eq, struct daos_kevent *ev)
{
	ev->ev_cookie = eq->eq_cookie++;
	if (unlikely(eq->eq_cookie == 0))
		eq->eq_cookie = 1; /* zero is reserved for NUL */

	ev->ev_ref = 1;
	cfs_hash_add(eq->eq_hash, &ev->ev_cookie, &ev->ev_hnode);
}

/**
 * remove an event from hash of EQ so nobody can find it via ID,
 * user can also indicate to unlink all his children.
 * A unlinked event is still attached on EQ if it's inflight, it will
 * be detached from EQ after completion.
 */
static void
daos_event_unlink_locked(struct daos_eq *eq,
			 struct daos_kevent *ev, int unlink_children)
{
	struct daos_kevent *tmp;

	if (ev->ev_cookie == 0) /* already unlinked */
		return;

	cfs_hash_del(eq->eq_hash, &ev->ev_cookie, &ev->ev_hnode);
	LASSERT(ev->ev_ref > 0);
	ev->ev_ref--;
	ev->ev_cookie = 0; /* mark it as unlinked */

	if (!unlink_children)
		return;

	list_for_each_entry(tmp, &ev->ev_children, ev_link) {
		if (tmp->ev_cookie == 0)
			continue;

		cfs_hash_del(eq->eq_hash, &tmp->ev_cookie, &tmp->ev_hnode);
		LASSERT(tmp->ev_ref > 0);
		tmp->ev_ref--;
		tmp->ev_cookie = 0;
	}
}

/** attach an event to EQ or its parent and increase counters */
static void
daos_event_attach_locked(struct daos_eq *eq,
			 struct daos_kevent *ev, struct daos_kevent *parent)
{
	eq->eq_total++;
	if (parent == NULL) {
		list_add_tail(&ev->ev_link, &eq->eq_idle);
		return;
	}

	parent->ev_childn++;
	ev->ev_parent = parent;
	list_add_tail(&ev->ev_link, &parent->ev_children);
}

/** dettach an event from EQ or its parent and decrease counters */
static void
daos_event_detach_locked(struct daos_eq *eq, struct daos_kevent *ev)
{
	LASSERT(ev->ev_ref <= 1);
	LASSERT(ev->ev_status == DAOS_EVS_COMPLETE ||
		ev->ev_status == DAOS_EVS_NONE);

	if (list_empty(&ev->ev_link))
		return;

	list_del_init(&ev->ev_link);
	if (ev->ev_parent == NULL) {
		if (ev->ev_status == DAOS_EVS_COMPLETE) {
			LASSERT(eq->eq_compn > 0);
			eq->eq_compn--;
		}
	} else { /* has parent event */
		LASSERT(ev->ev_parent->ev_childn > 0);
		ev->ev_parent->ev_childn--;
		if (ev->ev_status == DAOS_EVS_COMPLETE) {
			LASSERT(ev->ev_parent->ev_childn_comp > 0);
			ev->ev_parent->ev_childn_comp--;
		}
		ev->ev_parent = NULL;
	}

	LASSERT(eq->eq_total > 0);
	eq->eq_total--;
}

/**
 * This function is called for checking/waiting poll before unlink event.
 * EQ poll can not have any lock while copying data to userspace, which means
 * it's very complex to handle race between poll and unlink for parent/child
 * event, so we just wait for polling which should not be very long.
 */
static int
daos_event_poll_wait_locked(struct daos_eq *eq, struct daos_kevent *ev)
{
	struct daos_kevent	*master;
	wait_queue_t		wait;

	master = ev->ev_parent == NULL ? ev : ev->ev_parent;
	if (master->ev_status != DAOS_EVS_COMPLETE)
		return 0;

	if (master->ev_ref == 1) /* not in poll */
		return 0;

	/* a thread is polling this event */
	LASSERT(master->ev_ref == 2);

	init_waitqueue_entry(&wait, current);
	set_current_state(TASK_UNINTERRUPTIBLE);
	add_wait_queue(&eq->eq_waitq, &wait);

	eq->eq_pollwait++;
	spin_unlock(&eq->eq_lock);

	schedule();

	spin_lock(&eq->eq_lock);
	eq->eq_pollwait--;

	set_current_state(TASK_RUNNING);
	remove_wait_queue(&eq->eq_waitq, &wait);
	return 1; /* waited */
}

static int
daos_parent_event_completed(struct daos_kevent *ev)
{
	LASSERT(ev->ev_childn_comp <= ev->ev_childn);

	if (ev->ev_status == DAOS_EVS_COMPLETE)
		return 2;

	if (ev->ev_status == DAOS_EVS_NONE)
		return 0;

	if (ev->ev_childn_comp == ev->ev_childn)
		return 1;

	if (ev->ev_status == DAOS_EVS_ABORT &&
	    ((ev->ev_cookie != 0 && ev->ev_ref == 1) ||
	     (ev->ev_cookie == 0 && ev->ev_ref == 0)))
		return 1;

	return 0;
}

static void
daos_event_free(struct daos_kevent *ev)
{
	struct daos_kevent *tmp;
	struct daos_eq	   *eq;
	struct list_head    zombies;

	LASSERT(ev->ev_ref == 0);
	LASSERT(ev->ev_status == DAOS_EVS_NONE);
	LASSERT(list_empty(&ev->ev_link));

	INIT_LIST_HEAD(&zombies);
	list_add(&ev->ev_link, &zombies);

	eq = ev->ev_eq;
	LASSERT(eq != NULL);

	spin_lock(&eq->eq_lock);
	/* detach all children from me */
	while (!list_empty(&ev->ev_children)) {
		tmp = container_of(ev->ev_children.next,
				   struct daos_kevent, ev_link);
		LASSERT(tmp->ev_status == DAOS_EVS_NONE);
		LASSERT(tmp->ev_parent == ev);
		LASSERT(tmp->ev_ref == 0);

		daos_event_detach_locked(eq, tmp);
		list_add(&tmp->ev_link, &zombies);
	}

	LASSERT(ev->ev_childn == 0);
	spin_unlock(&eq->eq_lock);

	while (!list_empty(&zombies)) {
		tmp = container_of(zombies.next, struct daos_kevent, ev_link);
		list_del(&tmp->ev_link);
		if (tmp->ev_eq != NULL)
			daos_eq_putref(tmp->ev_eq);
		OBD_FREE_PTR(tmp);
	}
}

static struct daos_kevent *
daos_event_alloc(void)
{
	struct daos_kevent	*ev;

	OBD_ALLOC_PTR(ev);
	if (ev == NULL)
		return NULL;

	ev->ev_status = DAOS_EVS_NONE;
	INIT_HLIST_NODE(&ev->ev_hnode);
	INIT_LIST_HEAD(&ev->ev_link);
	INIT_LIST_HEAD(&ev->ev_children);

	return ev;
}

int
daos_event_init(struct daos_dev_env *denv, struct daos_usr_ev_init *evi)
{
	struct daos_kevent	*parent = NULL;
	struct daos_kevent	*ev;
	struct daos_eq		*eq;
	__u64			cookie;
	int			rc;

	ENTRY;
	if (evi->evi_uevent == NULL || evi->evi_upriv == NULL) {
		CDEBUG(D_DAOS, "Userspace event is NULL\n");
		RETURN(-EINVAL);
	}

	eq = daos_eq_lookup(evi->evi_eqh);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Invalid eq handle "LPU64"\n",
		       evi->evi_eqh.cookie);
		RETURN(-ENOENT);
	}

	ev = daos_event_alloc();
	if (ev == NULL) {
		daos_eq_putref(eq);
		RETURN(-ENOMEM);
	}

	ev->ev_eq = eq; /* take over refcount */
	ev->ev_uevent = evi->evi_uevent;

	spin_lock(&eq->eq_lock);
	if (eq->eq_finalizing) {
		CDEBUG(D_DAOS, "EQ is finalizing\n");
		GOTO(failed, rc = -ENOENT);
	}

	if (eq->eq_total >= *the_daos_params.dmp_eq_eventn) {
		CWARN("Too many events for this EQ, total/max: %d/%d\n",
		      eq->eq_total, DAOS_EQ_EVN_MAX);
		GOTO(failed, rc = -ENOMEM);
	}

	if (evi->evi_parent != 0) {
		parent = cfs_hash_lookup(eq->eq_hash, &evi->evi_parent);
		if (parent == NULL) {
			CDEBUG(D_DAOS, "Cannot find parent event\n");
			GOTO(failed, rc = -ENOENT);
		}

		if (parent->ev_parent != NULL) {
			CDEBUG(D_DAOS, "Nested parent event\n");
			GOTO(failed, rc = -EPERM);
		}

		if (parent->ev_status != DAOS_EVS_NONE) {
			CDEBUG(D_DAOS, "Parent event is not idle\n");
			GOTO(failed, rc = -EPERM);
		}
	}

	/* link it to event hash-table of EQ */
	daos_event_link_locked(eq, ev);
	/* attach event to idle list or parent event */
	daos_event_attach_locked(eq, ev, parent);
	cookie = ev->ev_cookie;

	spin_unlock(&eq->eq_lock);

	/* event and EQ can be gone anytime after I released the EQ lock */

	rc = put_user(cookie, &evi->evi_upriv->ev_cookie);
	if (rc != 0) {
		struct daos_usr_ev_fini evf;

		CERROR("Failed to copy event id to userspace\n");
		evf.evf_id.eqh	  = evi->evi_eqh;
		evf.evf_id.cookie = cookie;
		evf.evf_force     = 1;
		daos_event_fini(denv, &evf);
		rc = -EFAULT;
	}
	RETURN(rc);
failed:
	spin_unlock(&eq->eq_lock);
	/* NB: EQ reference will be freed by daos_event_free */
	daos_event_free(ev);
	RETURN(rc);
}

static int
daos_event_complete_cb(struct daos_kevent *ev, int unlinked)
{
	struct daos_kevent *tmp;
	int		    rc = 0;
	int		    rc2;

	/* NB: called w/o EQ lock */
	LASSERT(ev->ev_status == DAOS_EVS_COMPLETE);
	/* NB: children list cannot be changed unless parent status
	 * is DAOS_EVS_NONE */
	list_for_each_entry(tmp, &ev->ev_children, ev_link) {
		if (tmp->ev_status == DAOS_EVS_NONE) /* parent is aborted */
			continue;

		LASSERT(tmp->ev_status == DAOS_EVS_COMPLETE);
		if (tmp->ev_ops != NULL && tmp->ev_ops->op_complete != NULL) {
			rc2 = tmp->ev_ops->op_complete(tmp->ev_kparam,
						       tmp->ev_uparam,
						       unlinked);
			if (rc2 != 0 && rc == 0)
				rc = rc2;
		}
		/* NB: normally ev::ev_status should be protected by
		 * eq::eq_lock, but nobody can race with me on child
		 * events at this point, and we don't want to traverse
		 * a very long list again, with lock in hot path. */
		tmp->ev_status = DAOS_EVS_NONE;
	}

	if (ev->ev_ops != NULL && ev->ev_ops->op_complete!= NULL) {
		rc2 = ev->ev_ops->op_complete(ev->ev_kparam,
					      ev->ev_uparam, unlinked);
		if (rc2 != 0 && rc == 0)
			rc = rc2;
	}
	/* NB: if event has been unlinked, it is safe to change status
	 * at here because nobody can race with me, otherwise event status
	 * will be changed outside this function and under lock protection */
	if (unlinked) {
		LASSERT(ev->ev_ref == 0);
		ev->ev_status = DAOS_EVS_NONE;
	}
	return rc;
}

int
daos_event_fini(struct daos_dev_env *denv, struct daos_usr_ev_fini *evf)
{
	struct daos_kevent	*parent;
	struct daos_kevent	*ev;
	struct daos_eq		*eq;
	int			rc = 0;

	ENTRY;
	eq = daos_eq_lookup(evf->evf_id.eqh);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Unknown EQ cookie "LPU64"\n",
		       evf->evf_id.eqh.cookie);
		RETURN(-ENOENT);
	}

	spin_lock(&eq->eq_lock);
 again:
	ev = cfs_hash_lookup(eq->eq_hash, &evf->evf_id.cookie);
	if (ev == NULL) {
		spin_unlock(&eq->eq_lock);
		CDEBUG(D_DAOS, "Unknown event "LPU64"\n", evf->evf_id.cookie);
		GOTO(out, rc = -ENOENT);
	}
	parent = ev->ev_parent;

	switch (ev->ev_status) {
	default:
		LBUG();
	case DAOS_EVS_DISPATCH:
		if (!*the_daos_params.dmp_can_abort && !evf->evf_force) {
			spin_unlock(&eq->eq_lock);
			CDEBUG(D_DAOS, "Can't finalize inflight event\n");
			GOTO(out, rc = -EBUSY);
		}

		if (*the_daos_params.dmp_can_abort)
			daos_event_abort_locked(eq, ev, 1);
		else
			daos_event_unlink_locked(eq, ev, 1);

		spin_unlock(&eq->eq_lock);
		GOTO(out, rc = 0);

		/* fall through */
	case DAOS_EVS_ABORT:
		daos_event_unlink_locked(eq, ev, 1);
		spin_unlock(&eq->eq_lock);
		GOTO(out, rc = 0);

	case DAOS_EVS_COMPLETE:
		if (daos_event_poll_wait_locked(eq, ev))
			goto again;
		/* fall through */
	case DAOS_EVS_NONE:
		/* unlink from event hashtable of EQ, including all children */
		daos_event_unlink_locked(eq, ev, 1);
		/* detach from idle list */
		daos_event_detach_locked(eq, ev);
		if (ev->ev_status == DAOS_EVS_COMPLETE)
			break;

		if (parent == NULL)
			break;
		/* parent can become "completed" because child is unlinked */
		if (daos_parent_event_completed(parent) == 1) {
			parent->ev_status = DAOS_EVS_COMPLETE;
			LASSERT(eq->eq_dispn > 0);
			eq->eq_dispn--;
			eq->eq_compn++;

			list_move_tail(&parent->ev_link, &eq->eq_comp);
			if (waitqueue_active(&eq->eq_waitq))
				wake_up(&eq->eq_waitq);
		}
		break;
	}
	spin_unlock(&eq->eq_lock);

	if (ev->ev_status == DAOS_EVS_COMPLETE)
		daos_event_complete_cb(ev, 1);

	daos_event_free(ev);
out:
	daos_eq_putref(eq);
	RETURN(rc);
}

static void
daos_event_abort_one(struct daos_kevent *ev, int unlinked)
{
	if (ev->ev_status != DAOS_EVS_DISPATCH)
		return;

	/* NB: ev::ev_error will be set by daos_event_complete(),
	 * so user will not see an error if operation has already
	 * finished at this point */
	/* NB: always set ev_status to DAOS_EVS_ABORT even w/o callback,
	 * so aborted parent event can be marked as COMPLETE right after
	 * completion all dispatched events (instead of all children).
	 * See daos_parent_event_completed for details. */
	ev->ev_status = DAOS_EVS_ABORT;
	if (ev->ev_ops != NULL && ev->ev_ops->op_abort != NULL)
		ev->ev_ops->op_abort(ev->ev_kparam, unlinked);
}

static void
daos_event_abort_locked(struct daos_eq *eq, struct daos_kevent *ev, int unlink)
{
	struct daos_kevent	*tmp;

	LASSERT(ev->ev_status == DAOS_EVS_DISPATCH);

	if (unlink)
		daos_event_unlink_locked(eq, ev, 0);
	daos_event_abort_one(ev, unlink);

	/* abort all children if he has */
	list_for_each_entry(tmp, &ev->ev_children, ev_link) {
		if (unlink)
			daos_event_unlink_locked(eq, ev, 0);
		daos_event_abort_one(tmp, unlink);
	}

	/* if aborted event is not a child event, move it to the
	 * head of dispatched list */
	if (ev->ev_parent == NULL) {
		list_del(&ev->ev_link);
		list_add(&ev->ev_link, &eq->eq_disp);
	}
}

int
daos_event_abort(struct daos_dev_env *denv, struct daos_usr_ev_abort *eva)
{
	struct daos_kevent	 *ev;
	struct daos_eq		 *eq;

	ENTRY;
	if (!*the_daos_params.dmp_can_abort) {
		CDEBUG(D_DAOS, "Event abort is disabled\n");
		RETURN(-EPERM);
	}

	eq = daos_eq_lookup(eva->eva_id.eqh);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Invalid EQ handle "LPX64"\n",
		       eva->eva_id.eqh.cookie);
		RETURN(-ENOENT);
	}

	spin_lock(&eq->eq_lock);
	ev = cfs_hash_lookup(eq->eq_hash, &eva->eva_id.cookie);
	if (ev != NULL && ev->ev_status == DAOS_EVS_DISPATCH)
		daos_event_abort_locked(eq, ev, 0);

	spin_unlock(&eq->eq_lock);

	daos_eq_putref(eq);
	RETURN(0);
}

int
daos_event_next(struct daos_dev_env *denv, struct daos_usr_ev_next *evn)
{
	struct daos_kevent	*parent;
	struct daos_kevent	*cur;
	struct daos_eq		*eq;
	daos_event_t		*ucur = NULL;
	int			rc = 0;

	ENTRY;
	if (evn->evn_next == NULL)
		RETURN(-EINVAL);

	eq = daos_eq_lookup(evn->evn_parent.eqh);
	if (eq == NULL)
		RETURN(-ENOENT);

	spin_lock(&eq->eq_lock);
	parent = cfs_hash_lookup(eq->eq_hash, &evn->evn_parent.cookie);
	if (parent == NULL)
		GOTO(out, rc = -ENOENT);

	if (evn->evn_current == 0) {
		if (list_empty(&parent->ev_children))
			GOTO(out, rc = 0);

		cur = list_entry(parent->ev_children.next,
				 struct daos_kevent, ev_link);
		ucur = cur->ev_uevent;
		GOTO(out, rc = 0);
	}

	cur = cfs_hash_lookup(eq->eq_hash, &evn->evn_current);
	if (cur == NULL) {
		CDEBUG(D_DAOS, "Can't find cursor event\n");
		GOTO(out, rc = -ENOENT);
	}

	if (cur->ev_parent != parent) {
		CDEBUG(D_DAOS, "Cursor event is not child of given parent\n");
		GOTO(out, rc = -EINVAL);
	}

	if (cur->ev_link.next == &parent->ev_children)
		GOTO(out, rc = 0);

	cur = list_entry(cur->ev_link.next, struct daos_kevent, ev_link);
	ucur = cur->ev_uevent;
	LASSERT(ucur != NULL);
out:
	spin_unlock(&eq->eq_lock);
	if (rc == 0 && copy_to_user(evn->evn_next, &ucur, sizeof(ucur))) {
		CERROR("Failed to copy event address to userspace\n");
		rc = -EFAULT;
	}

	daos_eq_putref(eq);
	RETURN(rc);
}

static int
daos_event_dispatch_locked(struct daos_eq *eq, struct daos_kevent *ev)
{
	struct daos_kevent	*parent = ev->ev_parent;

	LASSERT(!list_empty(&ev->ev_link));

	if (!list_empty(&ev->ev_children)) {
		CDEBUG(D_DAOS, "Can't dispatch parent event\n");
		return -EPERM;
	}

	if (ev->ev_status != DAOS_EVS_NONE) {
		CDEBUG(D_DAOS, "Event is not idle: %d\n", ev->ev_status);
		return -EPERM;
	}

	if (parent != NULL &&
	    parent->ev_status != DAOS_EVS_NONE &&
	    parent->ev_status != DAOS_EVS_DISPATCH) {
		CDEBUG(D_DAOS, "Parent is not idle: %d\n", ev->ev_status);
		return -EPERM;
	}

	ev->ev_status = DAOS_EVS_DISPATCH;
	LASSERT(ev->ev_ref == 1);
	ev->ev_ref++;

	if (parent != NULL) {
		LASSERT(parent->ev_status == DAOS_EVS_NONE ||
			parent->ev_status == DAOS_EVS_DISPATCH);
		parent->ev_ref++;
		if (parent->ev_status == DAOS_EVS_DISPATCH)
			return 0;

		/* convert parent status to "DISPATCH" if it's the first
		 * dispatched child event */
		parent->ev_status = DAOS_EVS_DISPATCH;
		ev = parent;
	}

	list_move_tail(&ev->ev_link, &eq->eq_disp);
	eq->eq_dispn++;
	return 0;
}

struct daos_kevent *
daos_event_dispatch(daos_event_id_t eid, void *uparam,
		    void *kparam, daos_kevent_ops_t *ops)
{
	struct daos_kevent	*ev = NULL;
	struct daos_eq		*eq;
	int			rc;

	eq = daos_eq_lookup(eid.eqh);
	if (eq == NULL) {
		CDEBUG(D_DAOS, "Can't find EQ from cookie "LPU64"\n",
		       eid.eqh.cookie);
		return NULL;
	}

	spin_lock(&eq->eq_lock);
	if (eq->eq_finalizing || eq->eq_error) {
		CDEBUG(D_DAOS, "Event queue is in progress of finalizing\n");
		goto out;
	}

	ev = cfs_hash_lookup(eq->eq_hash, &eid.cookie);
	if (ev == NULL) {
		CDEBUG(D_DAOS, "Can't find event "LPU64" from EQ\n",
		       eid.cookie);
		goto out;
	}

	rc = daos_event_dispatch_locked(eq, ev);
	if (rc != 0) {
		ev = NULL;
		goto out;
	}

	ev->ev_uparam = uparam;
	ev->ev_kparam = kparam;
	ev->ev_ops = ops;
 out:
	spin_unlock(&eq->eq_lock);
	daos_eq_putref(eq);
	return ev;
}
EXPORT_SYMBOL(daos_event_dispatch);

void
daos_event_complete(struct daos_kevent *ev, int error)
{
	struct daos_eq	   *eq = ev->ev_eq;
	struct daos_kevent *parent;
	struct list_head    zombies;
	int		    rc;

	INIT_LIST_HEAD(&zombies);

	LASSERT(eq != NULL);
	spin_lock(&eq->eq_lock);

	LASSERT(ev->ev_status == DAOS_EVS_DISPATCH ||
		ev->ev_status == DAOS_EVS_ABORT);

	LASSERT(!list_empty(&eq->eq_disp));
	LASSERT(!list_empty(&ev->ev_link));
	LASSERT(list_empty(&ev->ev_children));

	ev->ev_error = error;
	ev->ev_status = DAOS_EVS_COMPLETE;

	parent = ev->ev_parent;
	if (parent == NULL) {
		LASSERT(eq->eq_dispn > 0);
		eq->eq_dispn--;
		eq->eq_compn++;
	} else {
		parent->ev_childn_comp++;
	}

	LASSERT(ev->ev_ref >= 1);
	ev->ev_ref--;

	if (ev->ev_ref == 0) {
		/* unlinked already (by daos_event_fini or daos_eq_destroy) */
		daos_event_detach_locked(eq, ev);
		list_add(&ev->ev_link, &zombies);
		ev = NULL;
	}

	if (parent != NULL) {
		LASSERT(parent->ev_status == DAOS_EVS_DISPATCH ||
			parent->ev_status == DAOS_EVS_ABORT);

		if (parent->ev_error == 0 && error != 0)
			parent->ev_error = error;

		LASSERT(parent->ev_ref >= 1);
		parent->ev_ref--;
		rc = daos_parent_event_completed(parent);
		if (rc == 0)
			goto out;

		LASSERT(rc == 1);
		/* all children completed */
		parent->ev_status = DAOS_EVS_COMPLETE;
		LASSERT(eq->eq_dispn > 0);
		eq->eq_dispn--;
		eq->eq_compn++;

		if (parent->ev_ref == 0) {
			daos_event_detach_locked(eq, parent);
			list_add(&parent->ev_link, &zombies);
		} else {
			ev = parent;
		}
	}

	if (ev == NULL) /* unlinked, no event needs to be polled */
		goto out;

	list_move_tail(&ev->ev_link, &eq->eq_comp);
	if (waitqueue_active(&eq->eq_waitq))
		wake_up(&eq->eq_waitq);
out:
	spin_unlock(&eq->eq_lock);

	while (!list_empty(&zombies)) {
		ev = list_entry(zombies.next, struct daos_kevent, ev_link);
		list_del_init(&ev->ev_link);
		daos_event_complete_cb(ev, 1);
		daos_event_free(ev);
	}
}
EXPORT_SYMBOL(daos_event_complete);

long
daos_eq_ioctl(struct daos_dev_env *denv, unsigned int cmd,
	      struct daos_usr_param *uparam)
{
	void	*body = daos_usr_param_buf(uparam, 0);

	LASSERT(_IOC_TYPE(cmd) == DAOS_IOC_EQ);
	switch (cmd) {
	default:
		CERROR("DAOS: Unknown EQ opcode: %x\n", cmd);
		return -EINVAL;

	case DAOS_IOC_EQ_CREATE:
		return daos_eq_create(denv, body);

	case DAOS_IOC_EQ_DESTROY:
		return daos_eq_destroy(denv, body);

	case DAOS_IOC_EQ_POLL:
		return daos_eq_poll(denv, body);

	case DAOS_IOC_EQ_QUERY:
		return daos_eq_query(denv, body);

	case DAOS_IOC_EV_INIT:
		return daos_event_init(denv, body);

	case DAOS_IOC_EV_FINI:
		return daos_event_fini(denv, body);

	case DAOS_IOC_EV_ABORT:
		return daos_event_abort(denv, body);

	case DAOS_IOC_EV_NEXT:
		return daos_event_next(denv, body);
	}
}
