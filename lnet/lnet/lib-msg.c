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

void
lnet_enq_event_locked(lnet_obj_cpud_t *objcd,
                      lnet_eq_t *eq, lnet_event_t *ev)
{
        /* MUST called with LNET_OBJ_LOCK hold */
        int             index;

        if (eq->eq_size == 0) {
                LASSERT (eq->eq_callback != LNET_EQ_HANDLER_NONE);
                eq->eq_callback(ev);
                return;
        }

        /* enqueue event, always serialize by LNET_OBJ_LOCK_DEFAULT,
         * so we don't suggest to use EQPoll because contention */
        LNET_OBJ_LOCK_MORE(objcd, lnet_objcd_default());

        ev->sequence = eq->eq_enq_seq++;
        index = ev->sequence & (eq->eq_size - 1);

        eq->eq_events[index] = *ev;

        if (eq->eq_callback != LNET_EQ_HANDLER_NONE)
                eq->eq_callback(ev);

#ifdef __KERNEL__
        if (cfs_waitq_active(&the_lnet.ln_waitq))
                cfs_waitq_broadcast(&the_lnet.ln_waitq);
#else
# ifndef HAVE_LIBPTHREAD
        /* LNetEQPoll() calls into _the_ LND to wait for action */
# else
        /* Wake anyone waiting in LNetEQPoll() */
        pthread_cond_broadcast(&the_lnet.ln_cond);
# endif
#endif
        LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
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
lnet_complete_msg_locked(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        lnet_handle_wire_t ack_wmd;
        int                rc;

        if (lnet_msg_need_ack(msg)) {
                /* Only send an ACK if the PUT completed successfully */

                lnet_return_credits_locked(netcd, msg);

                msg->msg_ack = 0;
                LNET_NET_UNLOCK(netcd);

                LASSERT(msg->msg_ev.type == LNET_EVENT_PUT);
                LASSERT(!msg->msg_routing);

                ack_wmd = msg->msg_hdr.msg.put.ack_wmd;

                lnet_prep_send(msg, LNET_MSG_ACK, msg->msg_ev.initiator, 0, 0);

                msg->msg_hdr.msg.ack.dst_wmd = ack_wmd;
                msg->msg_hdr.msg.ack.match_bits = msg->msg_ev.match_bits;
                msg->msg_hdr.msg.ack.mlength = cpu_to_le32(msg->msg_ev.mlength);

                rc = lnet_send(msg->msg_ev.target.nid, msg);

                LNET_NET_LOCK(netcd);

                if (rc == 0)
                        return;

        } else if (lnet_msg_need_forward(msg)) { /* not forwarded */
                LASSERT (!msg->msg_receiving);  /* called back recv already */

                LNET_NET_UNLOCK(netcd);

                rc = lnet_send(LNET_NID_ANY, msg);

                LNET_NET_LOCK(netcd);

                if (rc == 0)
                        return;
        }

        lnet_return_credits_locked(netcd, msg);
        lnet_msg_decommit(netcd, msg);
}

void
lnet_finalize (lnet_ni_t *ni, lnet_msg_t *msg, int status)
{
        lnet_net_cpud_t *netcd;

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
               msg->msg_onactivelist ? "!" : "",
               msg->msg_txpeer == NULL ?
               "<none>" : libcfs_nid2str(msg->msg_txpeer->lp_nid),
               msg->msg_rxpeer == NULL ?
               "<none>" : libcfs_nid2str(msg->msg_rxpeer->lp_nid));
#endif
        if (msg->msg_md != NULL) {
                lnet_libmd_t    *md = msg->msg_md;
                lnet_obj_cpud_t *objcd;
                int              unlink;

                objcd = lnet_objcd_from_cookie(md->md_lh.lh_cookie);

                LNET_OBJ_LOCK(objcd);
                /* Now it's safe to drop my caller's ref */
                md->md_refcount--;
                LASSERT(md->md_refcount >= 0);

                unlink = lnet_md_unlinkable(md);
                if (md->md_eq != NULL) {
                        msg->msg_ev.status   = status;
                        msg->msg_ev.unlinked = unlink;
                        lnet_enq_event_locked(objcd, md->md_eq,
                                              &msg->msg_ev);
                }

                if (unlink)
                        lnet_md_unlink(objcd, md);
                LNET_OBJ_UNLOCK(objcd);
                msg->msg_md = NULL;
        }

        netcd = msg->msg_netcd;
        if (netcd == NULL) {
                /* never commit to network, take LNET_NET_LOCK only
                 * to keep consistency with userspace */
                LASSERT(msg->msg_rxpeer == NULL && msg->msg_txpeer == NULL);

                LNET_NET_LOCK(lnet_netcd_default());
                lnet_msg_free(msg);
                LNET_NET_UNLOCK(lnet_netcd_default());
                return;
        }

        LNET_NET_LOCK(netcd);

        cfs_list_add_tail (&msg->msg_list, &netcd->lnc_finalizeq);

        /* Recursion breaker.  Don't complete the message here if I am
         * already completing messages */
        if (netcd->lnc_finalizing) {
                LNET_NET_UNLOCK(netcd);
                return;
        }

        netcd->lnc_finalizing = 1;

        while (!cfs_list_empty(&netcd->lnc_finalizeq)) {
                msg = cfs_list_entry(netcd->lnc_finalizeq.next,
                                     lnet_msg_t, msg_list);

                cfs_list_del(&msg->msg_list);

                /* NB drops and regains the lnet lock if it actually does
                 * anything, so my finalizing friends can chomp along too */
                lnet_complete_msg_locked(netcd, msg);
        }

        netcd->lnc_finalizing = 0;
        LNET_NET_UNLOCK(netcd);
}
