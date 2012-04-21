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
 * lnet/lnet/lib-msg.c
 *
 * Message decoding, parsing and finalizing routines
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

/* must be called with lnet_res_lock held */
void
lnet_build_unlink_event (lnet_libmd_t *md, lnet_event_t *ev)
{
        ENTRY;

        memset(ev, 0, sizeof(*ev));

        ev->status   = 0;
        ev->unlinked = 1;
        ev->type     = LNET_EVENT_UNLINK;
        lnet_md_deconstruct(md, &ev->md);
        lnet_md2handle(&ev->md_handle, md);
        EXIT;
}

/*
 * Don't need any lock.
 * must be called after lnet_msg_attach_md and BEFORE lnet_msg_commit
 */
void
lnet_build_msg_event(lnet_msg_t *msg, lnet_event_kind_t ev_type)
{
	lnet_hdr_t   *hdr = &msg->msg_hdr;
	lnet_event_t *ev  = &msg->msg_ev;

	LASSERT(!msg->msg_routing);

	ev->type = ev_type;

	if (ev_type == LNET_EVENT_SEND) {
		/* event for active message */
		ev->target.nid    = le64_to_cpu(hdr->dest_nid);
		ev->target.pid    = le32_to_cpu(hdr->dest_pid);
		ev->initiator.nid = LNET_NID_ANY;
		ev->initiator.pid = the_lnet.ln_pid;
		ev->sender        = LNET_NID_ANY;

	} else {
		/* event for passive message */
		ev->target.pid    = hdr->dest_pid;
		ev->target.nid    = hdr->dest_nid;
		ev->initiator.pid = hdr->src_pid;
		ev->initiator.nid = hdr->src_nid;
		ev->rlength       = hdr->payload_length;
		ev->sender        = msg->msg_from;
		ev->mlength	  = msg->msg_wanted;
		ev->offset	  = msg->msg_offset;
	}

	switch (ev_type) {
	default:
		LBUG();

	case LNET_EVENT_PUT: /* passive PUT */
		ev->pt_index   = hdr->msg.put.ptl_index;
		ev->match_bits = hdr->msg.put.match_bits;
		ev->hdr_data   = hdr->msg.put.hdr_data;
		return;

	case LNET_EVENT_GET: /* passive GET */
		ev->pt_index   = hdr->msg.get.ptl_index;
		ev->match_bits = hdr->msg.get.match_bits;
		ev->hdr_data   = 0;
		return;

	case LNET_EVENT_ACK: /* ACK */
		ev->match_bits = hdr->msg.ack.match_bits;
		ev->mlength    = hdr->msg.ack.mlength;
		return;

	case LNET_EVENT_REPLY: /* REPLY */
		return;

	case LNET_EVENT_SEND: /* active message */
		if (msg->msg_type == LNET_MSG_PUT) {
			ev->pt_index   = le32_to_cpu(hdr->msg.put.ptl_index);
			ev->match_bits = le64_to_cpu(hdr->msg.put.match_bits);
			ev->offset     = le32_to_cpu(hdr->msg.put.offset);
			ev->mlength    =
			ev->rlength    = le32_to_cpu(hdr->payload_length);
			ev->hdr_data   = le64_to_cpu(hdr->msg.put.hdr_data);

		} else {
			LASSERT(msg->msg_type == LNET_MSG_GET);
			ev->pt_index   = le32_to_cpu(hdr->msg.get.ptl_index);
			ev->match_bits = le64_to_cpu(hdr->msg.get.match_bits);
			ev->mlength    =
			ev->rlength    = le32_to_cpu(hdr->msg.get.sink_length);
			ev->offset     = le32_to_cpu(hdr->msg.get.src_offset);
			ev->hdr_data   = 0;
		}
		return;
	}
}

void
lnet_msg_commit(lnet_msg_t *msg, int msg_type, int cpt)
{
	lnet_counters_t	*counters = the_lnet.ln_counters[cpt];
	lnet_event_t	*ev	  = &msg->msg_ev;

	/* always called holding the lnet_net_lock, and called after
	 * lnet_build_msg_event() */

	/* routed message can be committed for both receiving and sending */
	LASSERT(!msg->msg_tx_committed);

	if (msg->msg_rx_committed) { /* routed message, or reply for GET */
		LASSERT(msg->msg_onactivelist);

		msg->msg_tx_committed = 1;
		msg->msg_tx_cpt = cpt;
		return;
	}

	LASSERT(!msg->msg_onactivelist);
	msg->msg_onactivelist = 1;
	cfs_list_add(&msg->msg_activelist,
		     &the_lnet.ln_msg_containers[cpt]->msc_active);

	counters->msgs_alloc++;
	if (counters->msgs_alloc > counters->msgs_max)
		counters->msgs_max = counters->msgs_alloc;

	switch (ev->type) {
	default: /* no event for routing message */
		LASSERT(msg->msg_routing);

		msg->msg_rx_cpt = cpt;
		msg->msg_rx_committed = 1;
		counters->route_count++;
		counters->route_length += msg->msg_len;
		return;

	case LNET_EVENT_SEND: /* active sending */
		LASSERT(msg_type == LNET_MSG_PUT || \
			msg_type == LNET_MSG_GET || msg_type == LNET_MSG_ACK);

		msg->msg_tx_cpt = cpt;
		msg->msg_tx_committed = 1;
		counters->send_count++;
		if (msg_type == LNET_MSG_PUT)
			counters->send_length += msg->msg_ev.mlength;
		return;

	case LNET_EVENT_PUT:
	case LNET_EVENT_GET:
	case LNET_EVENT_ACK:
	case LNET_EVENT_REPLY:
		LASSERT(!msg->msg_rx_committed);

		/* passive message */
		msg->msg_rx_cpt = cpt;
		msg->msg_rx_committed = 1;
		counters->recv_count++;

		if (msg_type == LNET_MSG_GET)
			counters->send_length += msg->msg_ev.mlength;
		else if (msg_type == LNET_MSG_PUT || msg_type == LNET_MSG_REPLY)
			counters->recv_length += msg->msg_ev.mlength;
		return;
	}
}

void
lnet_msg_decommit(lnet_msg_t *msg, int cpt)
{
	LASSERT(msg->msg_tx_committed || msg->msg_rx_committed);
	LASSERT(msg->msg_onactivelist);

	if (msg->msg_tx_committed) { /* always decommit for sending first */
		LASSERT(cpt == msg->msg_tx_cpt);

		lnet_return_tx_credits_locked(msg);
		msg->msg_tx_committed = 0;
	}

	if (!msg->msg_rx_committed) {
		the_lnet.ln_counters[cpt]->msgs_alloc--;
		cfs_list_del(&msg->msg_activelist);
		msg->msg_onactivelist = 0;
		return;
	}

	if (cpt != msg->msg_rx_cpt) {
		lnet_net_unlock(cpt);
		lnet_net_lock(msg->msg_rx_cpt);
	}

	lnet_return_rx_credits_locked(msg);

	cfs_list_del(&msg->msg_activelist);
	msg->msg_onactivelist = 0;
	msg->msg_rx_committed = 0;

	the_lnet.ln_counters[msg->msg_rx_cpt]->msgs_alloc--;

	if (cpt != msg->msg_rx_cpt) {
		lnet_net_unlock(msg->msg_rx_cpt);
		lnet_net_lock(cpt);
	}
}

void
lnet_msg_attach_md(lnet_msg_t *msg, lnet_libmd_t *md,
		   unsigned int offset, unsigned int mlen)
{
	/* always called holding the lnet_res_lock */

	/* Here, we attach the MD on lnet_msg and mark it busy and
	 * decrementing its threshold. Come what may, the lnet_msg "owns"
	 * the MD until a call to lnet_msg_detach_md or lnet_finalize()
	 * signals completion. */
	LASSERT(!msg->msg_routing);

	msg->msg_md = md;
	if (msg->msg_receiving) { /* commited for receiving */
		msg->msg_offset = offset;
		msg->msg_wanted = mlen;
	}

	md->md_refcount++;
	if (md->md_threshold != LNET_MD_THRESH_INF) {
		LASSERT(md->md_threshold > 0);
		md->md_threshold--;
	}

	/* build umd in event */
	lnet_md2handle(&msg->msg_ev.md_handle, md);
	lnet_md_deconstruct(md, &msg->msg_ev.md);
}

void
lnet_msg_detach_md(lnet_msg_t *msg, int status)
{
	lnet_libmd_t	*md = msg->msg_md;
	int		unlink;
	int		cpt;

	/* NB: called w/o either lnet_res_lock or lnet_net_lock */
	if (md == NULL)
		return;

	/* message already has attached with MD */
	cpt = lnet_cpt_of_cookie(md->md_lh.lh_cookie);

	lnet_res_lock(cpt);
	/* Now it's safe to drop my caller's ref */
	md->md_refcount--;
	LASSERT(md->md_refcount >= 0);

	unlink = lnet_md_unlinkable(md);
	if (md->md_eq != NULL) {
		msg->msg_ev.status   = status;
		msg->msg_ev.unlinked = unlink;
		lnet_eq_enqueue_event(md->md_eq, &msg->msg_ev);
	}

	if (unlink)
		lnet_md_unlink(md, cpt);

	lnet_res_unlock(cpt);

	msg->msg_md = NULL;
}

static inline int
lnet_msg_need_ack(lnet_msg_t *msg)
{
	return msg->msg_ev.status == 0 && msg->msg_ack;
}

static inline int
lnet_msg_need_forward(lnet_msg_t *msg)
{
	return msg->msg_ev.status == 0 &&
	       msg->msg_routing && !msg->msg_sending;
}

static void
lnet_msg_complete_locked(lnet_msg_t *msg, int cpt)
{
        lnet_handle_wire_t ack_wmd;
        int                rc;

	if (lnet_msg_need_ack(msg)) {
                /* Only send an ACK if the PUT completed successfully */

		lnet_msg_decommit(msg, cpt);

                msg->msg_ack = 0;
		lnet_net_unlock(cpt);

                LASSERT(msg->msg_ev.type == LNET_EVENT_PUT);
                LASSERT(!msg->msg_routing);

                ack_wmd = msg->msg_hdr.msg.put.ack_wmd;

		/* although this message will not generate any event but
		 * lnet_msg_commit replys on even type */
		msg->msg_ev.type = LNET_EVENT_SEND;
                lnet_prep_send(msg, LNET_MSG_ACK, msg->msg_ev.initiator, 0, 0);

                msg->msg_hdr.msg.ack.dst_wmd = ack_wmd;
                msg->msg_hdr.msg.ack.match_bits = msg->msg_ev.match_bits;
                msg->msg_hdr.msg.ack.mlength = cpu_to_le32(msg->msg_ev.mlength);

		rc = lnet_send(msg->msg_ev.target.nid, msg, LNET_NID_ANY);

		lnet_net_lock(cpt);

                if (rc == 0)
                        return;

	} else if (lnet_msg_need_forward(msg)) { /* not forwarded */
                LASSERT (!msg->msg_receiving);  /* called back recv already */

		lnet_net_unlock(cpt);

		rc = lnet_send(LNET_NID_ANY, msg, LNET_NID_ANY);

		lnet_net_lock(cpt);

                if (rc == 0)
                        return;
        }

	lnet_msg_decommit(msg, cpt);

	lnet_msg_free_locked(msg);
}

void
lnet_finalize (lnet_ni_t *ni, lnet_msg_t *msg, int status)
{
	struct lnet_msg_container *container;
	int	my_slot;
	int	cpt;
	int	i;

        LASSERT (!cfs_in_interrupt ());

        if (msg == NULL)
                return;
#if 0
	CDEBUG(D_WARNING,
	       "%s msg->%s Flags:%s%s%s%s%s%s%s%s%s%s%s txp %s rxp %s\n",
               lnet_msgtyp2str(msg->msg_type), libcfs_id2str(msg->msg_target),
               msg->msg_target_is_router ? "t" : "",
               msg->msg_routing ? "X" : "",
               msg->msg_ack ? "A" : "",
               msg->msg_sending ? "S" : "",
               msg->msg_receiving ? "R" : "",
               msg->msg_delayed ? "d" : "",
               msg->msg_txcredit ? "C" : "",
               msg->msg_peertxcredit ? "c" : "",
               msg->msg_rtrcredit ? "F" : "",
               msg->msg_peerrtrcredit ? "f" : "",
	       msg->msg_committed ? "!" : "",
	       msg->msg_txpeer == NULL ?
	       "<none>" : libcfs_nid2str(msg->msg_txpeer->lp_nid),
	       msg->msg_rxpeer == NULL ?
	       "<none>" : libcfs_nid2str(msg->msg_rxpeer->lp_nid));
#endif
	if (msg->msg_md != NULL)
		lnet_msg_detach_md(msg, status);

	if (!msg->msg_rx_committed && !msg->msg_tx_committed) {
		/* not commited to network yet */
		LASSERT(!msg->msg_onactivelist);
		lnet_msg_free(msg);
		return;
        }

	/*
	 * NB: routed message can be commited for both receiving and sending,
	 * we should finalize in LIFO order and keep counters correct.
	 * (finalize sending first then finalize receiving)
	 */
	cpt = msg->msg_tx_committed ? msg->msg_tx_cpt : msg->msg_rx_cpt;
	lnet_net_lock(cpt);

	container = the_lnet.ln_msg_containers[cpt];
	cfs_list_add_tail(&msg->msg_list, &container->msc_finalizing);

	/* Recursion breaker.  Don't complete the message here if I am
	 * already completing messages */

#ifdef __KERNEL__
        my_slot = -1;
	for (i = 0; i < container->msc_nfinalizers; i++) {
		if (container->msc_finalizers[i] == cfs_current())
                        goto out;

		if (my_slot < 0 && container->msc_finalizers[i] == NULL)
                        my_slot = i;
        }

        if (my_slot < 0)
                goto out;

	container->msc_finalizers[my_slot] = cfs_current();
#else
	LASSERT(container->msc_nfinalizers == 1);
	if (container->msc_finalizers[0] != NULL)
		goto out;

	my_slot = i = 0;
	container->msc_finalizers[0] = (struct lnet_msg_container *)1;
#endif

	while (!cfs_list_empty(&container->msc_finalizing)) {
		msg = cfs_list_entry(container->msc_finalizing.next,
                                     lnet_msg_t, msg_list);

                cfs_list_del(&msg->msg_list);

                /* NB drops and regains the lnet lock if it actually does
                 * anything, so my finalizing friends can chomp along too */
		lnet_msg_complete_locked(msg, cpt);
        }

	container->msc_finalizers[my_slot] = NULL;
 out:
	lnet_net_unlock(cpt);
}

void
lnet_msg_container_cleanup(struct lnet_msg_container *container)
{
	int     count = 0;

	if (container->msc_init == 0)
		return;

	while (!cfs_list_empty(&container->msc_active)) {
		lnet_msg_t *msg = cfs_list_entry(container->msc_active.next,
						 lnet_msg_t, msg_activelist);

		LASSERT(msg->msg_onactivelist);
		msg->msg_onactivelist = 0;
		cfs_list_del(&msg->msg_activelist);
		lnet_msg_free(msg);
		count++;
	}

	if (count > 0)
		CERROR("%d active msg on exit\n", count);

	if (container->msc_finalizers != NULL) {
		LIBCFS_FREE(container->msc_finalizers,
			    container->msc_nfinalizers *
			    sizeof(*container->msc_finalizers));
		container->msc_finalizers = NULL;
	}
#ifdef LNET_USE_LIB_FREELIST
	lnet_freelist_fini(&container->msc_freelist);
#endif
	container->msc_init = 0;
}

int
lnet_msg_container_setup(struct lnet_msg_container *container, int cpt)
{
	container->msc_init = 1;

	CFS_INIT_LIST_HEAD(&container->msc_active);
	CFS_INIT_LIST_HEAD(&container->msc_finalizing);

#ifdef LNET_USE_LIB_FREELIST
	memset(&container->msc_freelist, 0, sizeof(lnet_freelist_t));

	if (lnet_freelist_init(&container->msc_freelist,
			       MAX_MSGS, sizeof(lnet_msg_t)) != 0) {
		CERROR("Failed to init freelist for message container\n");
		lnet_msg_container_cleanup(container);
		return -ENOMEM;
	}
#endif

	container->msc_nfinalizers = cfs_cpt_weight(lnet_cpt_table(), cpt);

	LIBCFS_CPT_ALLOC(container->msc_finalizers,
			 lnet_cpt_table(), cpt,
			 container->msc_nfinalizers *
			 sizeof(*container->msc_finalizers));

	if (container->msc_finalizers == NULL) {
		CERROR("Failed to allocate message finalizers\n");
		lnet_msg_container_cleanup(container);
		return -ENOMEM;
	}

	return 0;
}

void
lnet_msg_containers_destroy(void)
{
	struct lnet_msg_container *container;
	int	i;

	if (the_lnet.ln_msg_containers == NULL)
		return;

	cfs_percpt_for_each(container, i, the_lnet.ln_msg_containers)
		lnet_msg_container_cleanup(container);

	cfs_percpt_free(the_lnet.ln_msg_containers);
	the_lnet.ln_msg_containers = NULL;
}

int
lnet_msg_containers_create(void)
{
	struct lnet_msg_container *container;
	int	rc;
	int	i;

	the_lnet.ln_msg_containers = cfs_percpt_alloc(lnet_cpt_table(),
						      sizeof(*container));
	if (the_lnet.ln_msg_containers == NULL) {
		CERROR("Failed to allocate cpu-partition data for network\n");
		return -ENOMEM;
	}

	cfs_percpt_for_each(container, i, the_lnet.ln_msg_containers) {
		rc = lnet_msg_container_setup(container, i);
		if (rc != 0) {
			lnet_msg_containers_destroy();
			return rc;
		}
	}
	return 0;
}
