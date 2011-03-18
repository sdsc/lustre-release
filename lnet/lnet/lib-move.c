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
 * lnet/lnet/lib-move.c
 *
 * Data movement routines
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

static int local_nid_dist_zero = 1;
CFS_MODULE_PARM(local_nid_dist_zero, "i", int, 0444,
                "Reserved");

#define LNET_MATCHMD_NONE     0   /* Didn't match */
#define LNET_MATCHMD_OK       1   /* Matched OK */
#define LNET_MATCHMD_DROP     2   /* Must be discarded */

static void
lnet_md_commit(lnet_libmd_t *md, lnet_msg_t *msg)
{
        /* always called holding the LNET_OBJ_LOCK */
        /* Here, we commit the MD to a network OP by marking it busy and
         * decrementing its threshold.  Come what may, the network "owns"
         * the MD until a call to lnet_finalize() signals completion. */
        LASSERT(!msg->msg_routing);

        msg->msg_md = md;

        md->md_refcount++;
        if (md->md_threshold != LNET_MD_THRESH_INF) {
                LASSERT (md->md_threshold > 0);
                md->md_threshold--;
        }
        msg->msg_md_threshold = md->md_threshold;
}

static void
lnet_build_msg_event(lnet_event_kind_t ev_type, lnet_msg_t *msg)
{
        lnet_hdr_t   *hdr = &msg->msg_hdr;
        lnet_event_t *ev  = &msg->msg_ev;

        /* Don't need any lock hold, but MUST called after
         * lnet_md_commit() and before lnet_msg_commit() */
        LASSERT(msg->msg_md != NULL);

        lnet_md2handle(&msg->msg_ev.md_handle, msg->msg_md);
        lnet_md_deconstruct(msg->msg_md, &msg->msg_ev.md);
        msg->msg_ev.md.threshold = msg->msg_md_threshold;

        ev->type = ev_type;

        if (ev_type != LNET_EVENT_SEND) {
                /* event for incoming event */
                ev->target.pid    = hdr->dest_pid;
                ev->target.nid    = hdr->dest_nid;
                ev->initiator.pid = hdr->src_pid;
                ev->initiator.nid = hdr->src_nid;
                ev->rlength       = hdr->payload_length;

                if (ev_type == LNET_EVENT_REPLY &&
                    msg->msg_type == LNET_MSG_GET) { /* optimized GETs */
                        ev->sender = hdr->src_nid;
                } else {
                        LASSERT(msg->msg_rxpeer != NULL);
                        ev->sender = msg->msg_rxpeer->lp_nid;
                }

        } else { /* event for outgoing message */
                ev->target.nid    = le64_to_cpu(hdr->dest_nid);
                ev->target.pid    = le32_to_cpu(hdr->dest_pid);
                ev->initiator.nid = LNET_NID_ANY;
                ev->initiator.pid = the_lnet.ln_pid;
                ev->sender        = LNET_NID_ANY;
        }

        switch (ev_type) {
        default:
                LBUG();
        case LNET_EVENT_PUT: /* incoming PUT */
                ev->pt_index   = hdr->msg.put.ptl_index;
                ev->match_bits = hdr->msg.put.match_bits;
                ev->mlength    = msg->msg_wanted;
                ev->offset     = msg->msg_offset;
                ev->hdr_data   = hdr->msg.put.hdr_data;
                return;

        case LNET_EVENT_GET: /* incoming GET */
                ev->pt_index   = hdr->msg.get.ptl_index;
                ev->match_bits = hdr->msg.get.match_bits;
                ev->mlength    = msg->msg_wanted;
                ev->offset     = msg->msg_offset;
                ev->hdr_data   = 0;
                return;

        case LNET_EVENT_ACK: /* incoming ACK */
                ev->match_bits = hdr->msg.ack.match_bits;
                ev->mlength    = hdr->msg.ack.mlength;
                ev->offset     = 0;
                return;

        case LNET_EVENT_REPLY: /* incoming REPLY */
                ev->mlength    = msg->msg_wanted;
                ev->offset     = 0;
                return;

        case LNET_EVENT_SEND: /* outgoing */
                if (msg->msg_type == LNET_MSG_PUT) {
                        ev->pt_index   = le32_to_cpu(hdr->msg.put.ptl_index);
                        ev->match_bits = le64_to_cpu(hdr->msg.put.match_bits);
                        ev->offset     = le32_to_cpu(hdr->msg.put.offset);
                        ev->mlength    = ev->rlength =
                                         le32_to_cpu(hdr->payload_length);
                        ev->hdr_data   = hdr->msg.put.hdr_data;
                        return;
                }

                LASSERT (msg->msg_type == LNET_MSG_GET);
                ev->pt_index   = le32_to_cpu(hdr->msg.get.ptl_index);
                ev->match_bits = le64_to_cpu(hdr->msg.get.match_bits);
                ev->mlength    = ev->rlength =
                                 le32_to_cpu(hdr->msg.get.sink_length);
                ev->offset     = le32_to_cpu(hdr->msg.get.src_offset);
                ev->hdr_data   = 0;
                return;
        }
}

static inline int
lnet_msg_is_commited(lnet_msg_t *msg)
{
        return msg->msg_netcd != NULL;
}

void
lnet_msg_decommit(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        LASSERT (msg->msg_netcd != NULL);

        msg->msg_netcd = NULL;
        cfs_list_del(&msg->msg_activelist);
        netcd->lnc_counters.msgs_alloc--;
        lnet_msg_free(msg);
}

static void
lnet_msg_commit(lnet_net_cpud_t *netcd, lnet_msg_t *msg, int msg_type)
{
        lnet_event_t *ev  = &msg->msg_ev;

        /* always called holding the LNET_NET_LOCK */
        LASSERT(msg->msg_netcd == NULL);

        msg->msg_netcd = netcd;
        cfs_list_add(&msg->msg_activelist, &netcd->lnc_active_msgs);

        if (++netcd->lnc_counters.msgs_alloc > netcd->lnc_counters.msgs_max)
                netcd->lnc_counters.msgs_max = netcd->lnc_counters.msgs_alloc;

        if (msg->msg_routing) {
                netcd->lnc_counters.route_count++;
                netcd->lnc_counters.route_length += msg->msg_len;
                return;
        }

        switch (ev->type) {
        default:
                LBUG();

        case LNET_EVENT_SEND:
                LASSERT (msg_type == LNET_MSG_PUT ||
                         msg_type == LNET_MSG_GET);

                netcd->lnc_counters.send_count++;
                if (msg_type == LNET_MSG_PUT)
                        netcd->lnc_counters.send_length += msg->msg_ev.mlength;
                return;

        case LNET_EVENT_PUT:
        case LNET_EVENT_GET:
        case LNET_EVENT_ACK:
        case LNET_EVENT_REPLY:
                /* incoming message */
                netcd->lnc_counters.recv_count++;

                if (msg_type == LNET_MSG_GET)
                        netcd->lnc_counters.send_length += msg->msg_ev.mlength;
                else if (msg_type == LNET_MSG_PUT ||
                         msg_type == LNET_MSG_REPLY)
                        netcd->lnc_counters.recv_length += msg->msg_ev.mlength;
                return;
        }
}

static int
lnet_try_match_md(lnet_obj_cpud_t *objcd, int index, int op_mask,
                  lnet_process_id_t src, unsigned int rlength,
                  unsigned int roffset, __u64 match_bits,
                  lnet_libmd_t *md, lnet_msg_t *msg)
{
        /* ALWAYS called holding the LNET_OBJ_LOCK, and can't LNET_OBJ_UNLOCK;
         * lnet_match_blocked_msg() relies on this to avoid races */
        unsigned int  offset;
        unsigned int  mlength;
        lnet_me_t    *me = md->md_me;

        /* mismatched MD op */
        if ((md->md_options & op_mask) == 0)
                return LNET_MATCHMD_NONE;

        /* MD exhausted */
        if (lnet_md_exhausted(md))
                return LNET_MATCHMD_NONE;

        /* mismatched ME nid/pid? */
        if (me->me_match_id.nid != LNET_NID_ANY &&
            me->me_match_id.nid != src.nid)
                return LNET_MATCHMD_NONE;

        if (me->me_match_id.pid != LNET_PID_ANY &&
            me->me_match_id.pid != src.pid)
                return LNET_MATCHMD_NONE;

        /* mismatched ME matchbits? */
        if (((me->me_match_bits ^ match_bits) & ~me->me_ignore_bits) != 0)
                return LNET_MATCHMD_NONE;

        /* Hurrah! This _is_ a match; check it out... */

        if ((md->md_options & LNET_MD_MANAGE_REMOTE) == 0)
                offset = md->md_offset;
        else
                offset = roffset;

        if ((md->md_options & LNET_MD_MAX_SIZE) != 0) {
                mlength = md->md_max_size;
                LASSERT (md->md_offset + mlength <= md->md_length);
        } else {
                mlength = md->md_length - offset;
        }

        if (rlength <= mlength) {        /* fits in allowed space */
                mlength = rlength;
        } else if ((md->md_options & LNET_MD_TRUNCATE) == 0) {
                /* this packet _really_ is too big */
                CERROR("Matching packet from %s, match "LPU64
                       " length %d too big: %d left, %d allowed\n",
                       libcfs_id2str(src), match_bits, rlength,
                       md->md_length - offset, mlength);

                return LNET_MATCHMD_DROP;
        }

        /* Commit to this ME/MD */
        CDEBUG(D_NET, "Incoming %s index %x from %s of "
               "length %d/%d into md "LPX64" [%d] + %d\n",
               (op_mask == LNET_MD_OP_PUT) ? "put" : "get",
               index, libcfs_id2str(src), mlength, rlength,
               md->md_lh.lh_cookie, md->md_niov, offset);

        lnet_md_commit(md, msg);
        /* NB: we should make md::md_offset to be cacheline aligned
         * in the future, so less cacheline conflicts on requests
         * for upper layer */
        md->md_offset = offset + mlength;

        msg->msg_offset = offset;
        msg->msg_wanted = mlength;

        /* Auto-unlink NOW, so the ME gets unlinked if required.
         * We bumped md->md_refcount above so the MD just gets flagged
         * for unlink when it is finalized. */
        if ((md->md_flags & LNET_MD_FLAG_AUTO_UNLINK) != 0 &&
            lnet_md_exhausted(md)) {
                lnet_md_unlink(objcd, md);
        }

        return LNET_MATCHMD_OK;
}

static int
lnet_do_match_md(lnet_obj_cpud_t *objcd, int index, int op_mask,
                 lnet_process_id_t src, unsigned int rlength,
                 unsigned int roffset, __u64 match_bits, lnet_msg_t *msg)
{
        cfs_list_t       *head;
        lnet_me_t        *tmp;
        lnet_me_t        *me;
        int               rc;

        /* called with LNET_OBJ_LOCK */
        head = lnet_portal_me_head(objcd, index, src, match_bits);
        if (head == NULL)
                return LNET_MATCHMD_NONE;

        cfs_list_for_each_entry_safe_typed(me, tmp, head,
                                           lnet_me_t, me_list) {
                lnet_libmd_t *md = me->me_md;

                /* ME attached but MD not attached yet */
                if (md == NULL)
                        continue;

                LASSERT (me == md->md_me);

                rc = lnet_try_match_md(objcd, index, op_mask, src, rlength,
                                       roffset, match_bits, md, msg);
                switch (rc) {
                default:
                        LBUG();

                case LNET_MATCHMD_NONE:
                        continue;

                case LNET_MATCHMD_OK:
                case LNET_MATCHMD_DROP:
                        return rc;
                }
                /* not reached */
        }

        return LNET_MATCHMD_NONE;
}

static inline int
lnet_op_should_drop(int op_mask, lnet_portal_t *ptl)
{
        return (op_mask == LNET_MD_OP_GET ||
               !lnet_portal_is_lazy(ptl)) ?
               LNET_MATCHMD_DROP : LNET_MATCHMD_NONE;
}

static int
lnet_match_md(lnet_obj_cpud_t *objcd, int index, int op_mask,
              lnet_process_id_t src, unsigned int rlength,
              unsigned int roffset, __u64 match_bits, lnet_msg_t *msg)
{
        lnet_portal_t    *ptl = the_lnet.ln_portals[index];
        lnet_obj_cpud_t   *pacer = NULL;
        int               first = 0;
        int               cpuid;
        int               rc;

        CDEBUG (D_NET, "Request from %s of length %d into portal %d "
                "MB="LPX64"\n", libcfs_id2str(src),
                rlength, index, match_bits);

        if (index < 0 || index >= MAX_PORTALS) {
                CERROR("Invalid portal %d not in [0-%d]\n",
                       index, MAX_PORTALS);
                return LNET_MATCHMD_DROP;
        }

        CFS_INIT_LIST_HEAD(&msg->msg_list);

        rc = lnet_do_match_md(objcd, index, op_mask, src,
                              rlength, roffset, match_bits, msg);
        if (rc == LNET_MATCHMD_OK || rc == LNET_MATCHMD_DROP)
                return rc;

        /* rc is LNET_MATCHMD_NONE */
        if (LNET_CONCURRENCY == 1 ||
            lnet_portal_is_unique(ptl) || ptl->ptl_ent_nactive == 0) {
                /* no buffer stealing */
                return lnet_op_should_drop(op_mask, ptl);
        }

        /* it's a wildcard portal, steal buffer from other CPU.
         * NB: stealing is slow performance, we always expect caller
         * post enough MD for all CPUs */
        LNET_OBJ_LOCK_MORE(objcd, lnet_objcd_default());

        if (objcd->loc_ptl_ents[index].pte_deadline != 0 &&
            cfs_time_aftereq(cfs_time_current(),
                             objcd->loc_ptl_ents[index].pte_deadline)) {
                /* Nobody post anything on this CPU for a while */
                objcd->loc_ptl_ents[index].pte_deadline = 0;
                lnet_ptl_ent_deactivate(ptl, objcd->loc_cpuid);
        }

        cfs_list_add_tail(&msg->msg_list, &ptl->ptl_msg_stealing);
        if (ptl->ptl_ent_stealing >= 0) {
                cpuid = ptl->ptl_ent_stealing;
                LASSERT(cpuid < LNET_CONCURRENCY);
        } else {
                cpuid = (objcd->loc_cpuid == LNET_CONCURRENCY - 1) ?
                        0 : objcd->loc_cpuid + 1;
        }
        first = cpuid;
        LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
        LNET_OBJ_UNLOCK(objcd);

        while (1) {
                pacer = lnet_objcd_from_cpuid(cpuid);

                LNET_OBJ_LOCK(pacer);
                LNET_OBJ_LOCK_MORE(pacer, lnet_objcd_default());

                if (cfs_list_empty(&msg->msg_list)) {
                        /* matched by another thread */
                        rc = msg->msg_md == NULL ?
                             LNET_MATCHMD_DROP : LNET_MATCHMD_OK;
                        break;
                }

                /* Haven't been matched by other thread */
                rc = lnet_do_match_md(pacer, index, op_mask, src,
                                      rlength, roffset, match_bits, msg);
                if (rc != LNET_MATCHMD_NONE) {
                        if (rc == LNET_MATCHMD_DROP)
                                break;

                        if (ptl->ptl_ent_stealing < 0 ||
                            ptl->ptl_ent_stealing != pacer->loc_cpuid)
                                ptl->ptl_ent_stealing = cpuid;
                        break;
                }

                cpuid = (cpuid == LNET_CONCURRENCY - 1) ? 0 : cpuid + 1;
                if (cpuid == first) { /* done all search */
                        rc = lnet_op_should_drop(op_mask, ptl);
                        break;
                }

                LNET_OBJ_UNLOCK_MORE(pacer, lnet_objcd_default());
                LNET_OBJ_UNLOCK(pacer);
        }

        if (rc != LNET_MATCHMD_NONE) {
                /* just remove from stealing list if it can't be delayed,
                 * otherwise we leave it on stealing list, it will be
                 * checked again in lnet_delay_put_locked() */
                if (!cfs_list_empty(&msg->msg_list))
                        cfs_list_del_init(&msg->msg_list);
        }

        LNET_OBJ_UNLOCK_MORE(pacer, lnet_objcd_default());
        LNET_OBJ_UNLOCK(pacer);

        LNET_OBJ_LOCK(objcd);
        return rc;
}

static void
lnet_fail_nid_locked(lnet_net_cpud_t *netcd,
                     lnet_nid_t nid, cfs_list_t *head)
{
        lnet_test_peer_t  *tp;
        cfs_list_t        *el;
        cfs_list_t        *next;

        cfs_list_for_each_safe(el, next, &netcd->lnc_test_peers) {
                tp = cfs_list_entry(el, lnet_test_peer_t, tp_list);

                if (tp->tp_threshold == 0 ||    /* needs culling anyway */
                    nid == LNET_NID_ANY ||      /* removing all entries */
                    nid == tp->tp_nid) {        /* matched this one */
                        cfs_list_del(&tp->tp_list);
                        cfs_list_add(&tp->tp_list, head);
                }
        }
}

int
lnet_fail_nid (lnet_nid_t nid, unsigned int threshold)
{
        lnet_net_cpud_t   *netcd;
        lnet_test_peer_t  *tp;
        cfs_list_t         cull;

        LASSERT (the_lnet.ln_init);

        if (threshold != 0) {
                /* Adding a new entry */
                if (nid == LNET_NID_ANY)
                        return -EINVAL;

                LIBCFS_ALLOC(tp, sizeof(*tp));
                if (tp == NULL)
                        return -ENOMEM;

                tp->tp_nid = nid;
                tp->tp_threshold = threshold;

                netcd = lnet_netcd_from_nid(nid);
                LNET_NET_LOCK(netcd);
                cfs_list_add_tail(&tp->tp_list, &netcd->lnc_test_peers);
                LNET_NET_UNLOCK(netcd);
                return 0;
        }

        /* removing entries */
        CFS_INIT_LIST_HEAD (&cull);

        netcd = nid == LNET_NID_ANY ? LNET_NET_ALL : lnet_netcd_from_nid(nid);

        LNET_NET_LOCK(netcd);

        if (netcd != LNET_NET_ALL) {
                lnet_fail_nid_locked(netcd, nid, &cull);
        } else {
                int     i;
                /* nid == LNET_NID_ANY, fail all */
                lnet_netcd_for_each(i) {
                        lnet_fail_nid_locked(lnet_netcd_from_cpuid(i),
                                             nid, &cull);
                }
        }

        LNET_NET_UNLOCK(netcd);

        while (!cfs_list_empty (&cull)) {
                tp = cfs_list_entry (cull.next, lnet_test_peer_t, tp_list);

                cfs_list_del (&tp->tp_list);
                LIBCFS_FREE(tp, sizeof (*tp));
        }
        return 0;
}

static int
lnet_fail_peer_locked(lnet_net_cpud_t *netcd, lnet_nid_t nid,
                      int outgoing, cfs_list_t *cull)
{
        lnet_test_peer_t *tp;
        cfs_list_t       *el;
        cfs_list_t       *next;
        int               fail = 0;

        cfs_list_for_each_safe (el, next, &netcd->lnc_test_peers) {
                tp = cfs_list_entry (el, lnet_test_peer_t, tp_list);

                if (tp->tp_threshold == 0) {
                        /* zombie entry */
                        if (outgoing) {
                                /* only cull zombies on outgoing tests,
                                 * since we may be at interrupt priority on
                                 * incoming messages. */
                                cfs_list_del (&tp->tp_list);
                                cfs_list_add (&tp->tp_list, cull);
                        }
                        continue;
                }

                if (tp->tp_nid == LNET_NID_ANY || /* fail every peer */
                    nid == tp->tp_nid) {        /* fail this peer */
                        fail = 1;

                        if (tp->tp_threshold != LNET_MD_THRESH_INF) {
                                tp->tp_threshold--;
                                if (outgoing &&
                                    tp->tp_threshold == 0) {
                                        /* see above */
                                        cfs_list_del (&tp->tp_list);
                                        cfs_list_add (&tp->tp_list, cull);
                                }
                        }
                        break;
                }
        }

        return fail;
}

static int
lnet_fail_peer(lnet_nid_t nid, int outgoing)
{
        lnet_net_cpud_t   *netcd;
        lnet_test_peer_t *tp;
        cfs_list_t        cull;
        int               fail = 0;

        netcd = nid == LNET_NID_ANY ? LNET_NET_ALL : lnet_netcd_from_nid(nid);
        CFS_INIT_LIST_HEAD(&cull);

        LNET_NET_LOCK(netcd);
        if (netcd != LNET_NET_ALL) {
                fail = lnet_fail_peer_locked(netcd, nid, outgoing, &cull);
        } else {
                int     i;
                int     rc = 0;

                lnet_netcd_for_each(i) {
                        rc = lnet_fail_peer_locked(lnet_netcd_from_cpuid(i),
                                                   nid, outgoing, &cull);
                        if (rc != 0 && fail == 0)
                                fail = rc;
                }
        }

        LNET_NET_UNLOCK(netcd);

        while (!cfs_list_empty (&cull)) {
                tp = cfs_list_entry (cull.next, lnet_test_peer_t, tp_list);
                cfs_list_del (&tp->tp_list);

                LIBCFS_FREE(tp, sizeof (*tp));
        }

        return fail;
}

unsigned int
lnet_iov_nob (unsigned int niov, struct iovec *iov)
{
        unsigned int nob = 0;

        while (niov-- > 0)
                nob += (iov++)->iov_len;

        return (nob);
}

void
lnet_copy_iov2iov (unsigned int ndiov, struct iovec *diov, unsigned int doffset,
                   unsigned int nsiov, struct iovec *siov, unsigned int soffset,
                   unsigned int nob)
{
        /* NB diov, siov are READ-ONLY */
        unsigned int  this_nob;

        if (nob == 0)
                return;

        /* skip complete frags before 'doffset' */
        LASSERT (ndiov > 0);
        while (doffset >= diov->iov_len) {
                doffset -= diov->iov_len;
                diov++;
                ndiov--;
                LASSERT (ndiov > 0);
        }

        /* skip complete frags before 'soffset' */
        LASSERT (nsiov > 0);
        while (soffset >= siov->iov_len) {
                soffset -= siov->iov_len;
                siov++;
                nsiov--;
                LASSERT (nsiov > 0);
        }

        do {
                LASSERT (ndiov > 0);
                LASSERT (nsiov > 0);
                this_nob = MIN(diov->iov_len - doffset,
                               siov->iov_len - soffset);
                this_nob = MIN(this_nob, nob);

                memcpy ((char *)diov->iov_base + doffset,
                        (char *)siov->iov_base + soffset, this_nob);
                nob -= this_nob;

                if (diov->iov_len > doffset + this_nob) {
                        doffset += this_nob;
                } else {
                        diov++;
                        ndiov--;
                        doffset = 0;
                }

                if (siov->iov_len > soffset + this_nob) {
                        soffset += this_nob;
                } else {
                        siov++;
                        nsiov--;
                        soffset = 0;
                }
        } while (nob > 0);
}

int
lnet_extract_iov (int dst_niov, struct iovec *dst,
                  int src_niov, struct iovec *src,
                  unsigned int offset, unsigned int len)
{
        /* Initialise 'dst' to the subset of 'src' starting at 'offset',
         * for exactly 'len' bytes, and return the number of entries.
         * NB not destructive to 'src' */
        unsigned int    frag_len;
        unsigned int    niov;

        if (len == 0)                           /* no data => */
                return (0);                     /* no frags */

        LASSERT (src_niov > 0);
        while (offset >= src->iov_len) {      /* skip initial frags */
                offset -= src->iov_len;
                src_niov--;
                src++;
                LASSERT (src_niov > 0);
        }

        niov = 1;
        for (;;) {
                LASSERT (src_niov > 0);
                LASSERT ((int)niov <= dst_niov);

                frag_len = src->iov_len - offset;
                dst->iov_base = ((char *)src->iov_base) + offset;

                if (len <= frag_len) {
                        dst->iov_len = len;
                        return (niov);
                }

                dst->iov_len = frag_len;

                len -= frag_len;
                dst++;
                src++;
                niov++;
                src_niov--;
                offset = 0;
        }
}

#ifndef __KERNEL__
unsigned int
lnet_kiov_nob (unsigned int niov, lnet_kiov_t *kiov)
{
        LASSERT (0);
        return (0);
}

void
lnet_copy_kiov2kiov (unsigned int ndkiov, lnet_kiov_t *dkiov, unsigned int doffset,
                     unsigned int nskiov, lnet_kiov_t *skiov, unsigned int soffset,
                     unsigned int nob)
{
        LASSERT (0);
}

void
lnet_copy_kiov2iov (unsigned int niov, struct iovec *iov, unsigned int iovoffset,
                    unsigned int nkiov, lnet_kiov_t *kiov, unsigned int kiovoffset,
                    unsigned int nob)
{
        LASSERT (0);
}

void
lnet_copy_iov2kiov (unsigned int nkiov, lnet_kiov_t *kiov, unsigned int kiovoffset,
                    unsigned int niov, struct iovec *iov, unsigned int iovoffset,
                    unsigned int nob)
{
        LASSERT (0);
}

int
lnet_extract_kiov (int dst_niov, lnet_kiov_t *dst,
                   int src_niov, lnet_kiov_t *src,
                   unsigned int offset, unsigned int len)
{
        LASSERT (0);
}

#else /* __KERNEL__ */

unsigned int
lnet_kiov_nob (unsigned int niov, lnet_kiov_t *kiov)
{
        unsigned int  nob = 0;

        while (niov-- > 0)
                nob += (kiov++)->kiov_len;

        return (nob);
}

void
lnet_copy_kiov2kiov (unsigned int ndiov, lnet_kiov_t *diov, unsigned int doffset,
                     unsigned int nsiov, lnet_kiov_t *siov, unsigned int soffset,
                     unsigned int nob)
{
        /* NB diov, siov are READ-ONLY */
        unsigned int    this_nob;
        char           *daddr = NULL;
        char           *saddr = NULL;

        if (nob == 0)
                return;

        LASSERT (!cfs_in_interrupt ());

        LASSERT (ndiov > 0);
        while (doffset >= diov->kiov_len) {
                doffset -= diov->kiov_len;
                diov++;
                ndiov--;
                LASSERT (ndiov > 0);
        }

        LASSERT (nsiov > 0);
        while (soffset >= siov->kiov_len) {
                soffset -= siov->kiov_len;
                siov++;
                nsiov--;
                LASSERT (nsiov > 0);
        }

        do {
                LASSERT (ndiov > 0);
                LASSERT (nsiov > 0);
                this_nob = MIN(diov->kiov_len - doffset,
                               siov->kiov_len - soffset);
                this_nob = MIN(this_nob, nob);

                if (daddr == NULL)
                        daddr = ((char *)cfs_kmap(diov->kiov_page)) + 
                                diov->kiov_offset + doffset;
                if (saddr == NULL)
                        saddr = ((char *)cfs_kmap(siov->kiov_page)) + 
                                siov->kiov_offset + soffset;

                /* Vanishing risk of kmap deadlock when mapping 2 pages.
                 * However in practice at least one of the kiovs will be mapped
                 * kernel pages and the map/unmap will be NOOPs */

                memcpy (daddr, saddr, this_nob);
                nob -= this_nob;

                if (diov->kiov_len > doffset + this_nob) {
                        daddr += this_nob;
                        doffset += this_nob;
                } else {
                        cfs_kunmap(diov->kiov_page);
                        daddr = NULL;
                        diov++;
                        ndiov--;
                        doffset = 0;
                }

                if (siov->kiov_len > soffset + this_nob) {
                        saddr += this_nob;
                        soffset += this_nob;
                } else {
                        cfs_kunmap(siov->kiov_page);
                        saddr = NULL;
                        siov++;
                        nsiov--;
                        soffset = 0;
                }
        } while (nob > 0);

        if (daddr != NULL)
                cfs_kunmap(diov->kiov_page);
        if (saddr != NULL)
                cfs_kunmap(siov->kiov_page);
}

void
lnet_copy_kiov2iov (unsigned int niov, struct iovec *iov, unsigned int iovoffset,
                    unsigned int nkiov, lnet_kiov_t *kiov, unsigned int kiovoffset,
                    unsigned int nob)
{
        /* NB iov, kiov are READ-ONLY */
        unsigned int    this_nob;
        char           *addr = NULL;

        if (nob == 0)
                return;

        LASSERT (!cfs_in_interrupt ());

        LASSERT (niov > 0);
        while (iovoffset >= iov->iov_len) {
                iovoffset -= iov->iov_len;
                iov++;
                niov--;
                LASSERT (niov > 0);
        }

        LASSERT (nkiov > 0);
        while (kiovoffset >= kiov->kiov_len) {
                kiovoffset -= kiov->kiov_len;
                kiov++;
                nkiov--;
                LASSERT (nkiov > 0);
        }

        do {
                LASSERT (niov > 0);
                LASSERT (nkiov > 0);
                this_nob = MIN(iov->iov_len - iovoffset,
                               kiov->kiov_len - kiovoffset);
                this_nob = MIN(this_nob, nob);

                if (addr == NULL)
                        addr = ((char *)cfs_kmap(kiov->kiov_page)) + 
                                kiov->kiov_offset + kiovoffset;

                memcpy ((char *)iov->iov_base + iovoffset, addr, this_nob);
                nob -= this_nob;

                if (iov->iov_len > iovoffset + this_nob) {
                        iovoffset += this_nob;
                } else {
                        iov++;
                        niov--;
                        iovoffset = 0;
                }

                if (kiov->kiov_len > kiovoffset + this_nob) {
                        addr += this_nob;
                        kiovoffset += this_nob;
                } else {
                        cfs_kunmap(kiov->kiov_page);
                        addr = NULL;
                        kiov++;
                        nkiov--;
                        kiovoffset = 0;
                }

        } while (nob > 0);

        if (addr != NULL)
                cfs_kunmap(kiov->kiov_page);
}

void
lnet_copy_iov2kiov (unsigned int nkiov, lnet_kiov_t *kiov, unsigned int kiovoffset,
                    unsigned int niov, struct iovec *iov, unsigned int iovoffset,
                    unsigned int nob)
{
        /* NB kiov, iov are READ-ONLY */
        unsigned int    this_nob;
        char           *addr = NULL;

        if (nob == 0)
                return;

        LASSERT (!cfs_in_interrupt ());

        LASSERT (nkiov > 0);
        while (kiovoffset >= kiov->kiov_len) {
                kiovoffset -= kiov->kiov_len;
                kiov++;
                nkiov--;
                LASSERT (nkiov > 0);
        }

        LASSERT (niov > 0);
        while (iovoffset >= iov->iov_len) {
                iovoffset -= iov->iov_len;
                iov++;
                niov--;
                LASSERT (niov > 0);
        }

        do {
                LASSERT (nkiov > 0);
                LASSERT (niov > 0);
                this_nob = MIN(kiov->kiov_len - kiovoffset,
                               iov->iov_len - iovoffset);
                this_nob = MIN(this_nob, nob);

                if (addr == NULL)
                        addr = ((char *)cfs_kmap(kiov->kiov_page)) + 
                                kiov->kiov_offset + kiovoffset;

                memcpy (addr, (char *)iov->iov_base + iovoffset, this_nob);
                nob -= this_nob;

                if (kiov->kiov_len > kiovoffset + this_nob) {
                        addr += this_nob;
                        kiovoffset += this_nob;
                } else {
                        cfs_kunmap(kiov->kiov_page);
                        addr = NULL;
                        kiov++;
                        nkiov--;
                        kiovoffset = 0;
                }

                if (iov->iov_len > iovoffset + this_nob) {
                        iovoffset += this_nob;
                } else {
                        iov++;
                        niov--;
                        iovoffset = 0;
                }
        } while (nob > 0);

        if (addr != NULL)
                cfs_kunmap(kiov->kiov_page);
}

int
lnet_extract_kiov (int dst_niov, lnet_kiov_t *dst,
                   int src_niov, lnet_kiov_t *src,
                   unsigned int offset, unsigned int len)
{
        /* Initialise 'dst' to the subset of 'src' starting at 'offset',
         * for exactly 'len' bytes, and return the number of entries.
         * NB not destructive to 'src' */
        unsigned int    frag_len;
        unsigned int    niov;

        if (len == 0)                           /* no data => */
                return (0);                     /* no frags */

        LASSERT (src_niov > 0);
        while (offset >= src->kiov_len) {      /* skip initial frags */
                offset -= src->kiov_len;
                src_niov--;
                src++;
                LASSERT (src_niov > 0);
        }

        niov = 1;
        for (;;) {
                LASSERT (src_niov > 0);
                LASSERT ((int)niov <= dst_niov);

                frag_len = src->kiov_len - offset;
                dst->kiov_page = src->kiov_page;
                dst->kiov_offset = src->kiov_offset + offset;

                if (len <= frag_len) {
                        dst->kiov_len = len;
                        LASSERT (dst->kiov_offset + dst->kiov_len <= CFS_PAGE_SIZE);
                        return (niov);
                }

                dst->kiov_len = frag_len;
                LASSERT (dst->kiov_offset + dst->kiov_len <= CFS_PAGE_SIZE);

                len -= frag_len;
                dst++;
                src++;
                niov++;
                src_niov--;
                offset = 0;
        }
}
#endif

static void
lnet_ni_recv(lnet_ni_t *ni,
             void *private, lnet_msg_t *msg, int delayed,
             unsigned int offset, unsigned int mlen, unsigned int rlen)
{
        unsigned int  niov = 0;
        struct iovec *iov = NULL;
        lnet_kiov_t  *kiov = NULL;
        int           rc;

        LASSERT (!cfs_in_interrupt ());
        LASSERT (mlen == 0 || msg != NULL);

        if (msg != NULL) {
                LASSERT(msg->msg_receiving);
                LASSERT(!msg->msg_sending);
                LASSERT(rlen == msg->msg_len);
                LASSERT(mlen == msg->msg_wanted);
                LASSERT(offset == msg->msg_offset);

                msg->msg_receiving = 0;

                if (mlen != 0) {
                        niov = msg->msg_niov;
                        iov  = msg->msg_iov;
                        kiov = msg->msg_kiov;

                        LASSERT (niov > 0);
                        LASSERT ((iov == NULL) != (kiov == NULL));
                }
        }

        rc = (ni->ni_lnd->lnd_recv)(ni, private, msg, delayed,
                                    niov, iov, kiov, offset, mlen, rlen);
        if (rc < 0)
                lnet_finalize(ni, msg, rc);
}

int
lnet_compare_routes(lnet_route_t *r1, lnet_route_t *r2)
{
        /* race here, but harmless */
        lnet_peer_t *p1 = r1->lr_gateway;
        lnet_peer_t *p2 = r2->lr_gateway;

        if (r1->lr_hops < r2->lr_hops)
                return 1;

        if (r1->lr_hops > r2->lr_hops)
                return -1;

        if (p1->lp_txqnob < p2->lp_txqnob)
                return 1;

        if (p1->lp_txqnob > p2->lp_txqnob)
                return -1;

        if (p1->lp_txcredits > p2->lp_txcredits)
                return 1;

        if (p1->lp_txcredits < p2->lp_txcredits)
                return -1;

        return 0;
}

static void
lnet_setpayloadbuffer(lnet_msg_t *msg)
{
        lnet_libmd_t *md = msg->msg_md;

        LASSERT (msg->msg_len > 0);
        LASSERT (!msg->msg_routing);
        LASSERT (md != NULL);
        LASSERT (msg->msg_niov == 0);
        LASSERT (msg->msg_iov == NULL);
        LASSERT (msg->msg_kiov == NULL);

        msg->msg_niov = md->md_niov;
        if ((md->md_options & LNET_MD_KIOV) != 0)
                msg->msg_kiov = md->md_iov.kiov;
        else
                msg->msg_iov = md->md_iov.iov;
}

void
lnet_prep_send(lnet_msg_t *msg, int type, lnet_process_id_t target,
               unsigned int offset, unsigned int len)
{
        msg->msg_type = type;
        msg->msg_target = target;
        msg->msg_len = len;
        msg->msg_offset = offset;

        if (len != 0)
                lnet_setpayloadbuffer(msg);

        memset (&msg->msg_hdr, 0, sizeof (msg->msg_hdr));
        msg->msg_hdr.type           = cpu_to_le32(type);
        msg->msg_hdr.dest_nid       = cpu_to_le64(target.nid);
        msg->msg_hdr.dest_pid       = cpu_to_le32(target.pid);
        /* src_nid will be set later */
        msg->msg_hdr.src_pid        = cpu_to_le32(the_lnet.ln_pid);
        msg->msg_hdr.payload_length = cpu_to_le32(len);
}

static void
lnet_ni_send(lnet_ni_t *ni, lnet_msg_t *msg)
{
        void   *priv = msg->msg_private;
        int     rc;

        LASSERT (!cfs_in_interrupt ());
        LASSERT (LNET_NETTYP(LNET_NIDNET(ni->ni_nid)) == LOLND ||
                 (msg->msg_txcredit && msg->msg_peertxcredit));

        rc = (ni->ni_lnd->lnd_send)(ni, priv, msg);
        if (rc < 0)
                lnet_finalize(ni, msg, rc);
}

static int
lnet_eager_recv(lnet_msg_t *msg)
{
        lnet_peer_t *peer;
        lnet_ni_t   *ni;
        int          rc = 0;

        LASSERT (msg->msg_receiving);
        LASSERT (!msg->msg_sending);

        peer = msg->msg_rxpeer;
        ni   = peer->lp_ni;

        rc = (ni->ni_lnd->lnd_eager_recv)(ni, msg->msg_private,
                                          msg, &msg->msg_private);
        if (rc == 0)
                return 0;

        LASSERT (rc < 0); /* required by my callers */

        CERROR("recv from %s / send to %s aborted: "
               "eager_recv failed %d\n",
               libcfs_nid2str(peer->lp_nid),
               libcfs_id2str(msg->msg_target), rc);

        return rc;
}

/* NB: caller shall hold a ref on 'lp' as I'd drop LNET_NET_LOCK */
void
lnet_ni_peer_alive(lnet_net_cpud_t *netcd, lnet_peer_t *lp)
{
        cfs_time_t  last_alive = 0;
        lnet_ni_t  *ni = lp->lp_ni;

        LASSERT (lnet_peer_aliveness_enabled(lp));
        LASSERT (ni->ni_lnd->lnd_query != NULL);

        LNET_NET_UNLOCK(netcd);
        (ni->ni_lnd->lnd_query)(ni, lp->lp_nid, &last_alive);
        LNET_NET_LOCK(netcd);

        lp->lp_last_query = cfs_time_current();

        if (last_alive != 0) /* NI has updated timestamp */
                lp->lp_last_alive = last_alive;
        return;
}

/* NB: always called with LNET_NET_LOCK held */
static inline int
lnet_peer_is_alive (lnet_peer_t *lp, cfs_time_t now)
{
        int        alive;
        cfs_time_t deadline;

        LASSERT (lnet_peer_aliveness_enabled(lp));

        /* Trust lnet_notify() if it has more recent aliveness news, but
         * ignore the initial assumed death (see lnet_peers_start_down()).
         */
        if (!lp->lp_alive && lp->lp_alive_count > 0 &&
            cfs_time_aftereq(lp->lp_timestamp, lp->lp_last_alive))
                return 0;

        deadline = cfs_time_add(lp->lp_last_alive,
                                cfs_time_seconds(lp->lp_ni->ni_peertimeout));
        alive = cfs_time_after(deadline, now);

        /* Update obsolete lp_alive except for routers assumed to be dead
         * initially, because router checker would update aliveness in this
         * case, and moreover lp_last_alive at peer creation is assumed.
         */
        if (alive && !lp->lp_alive &&
            !(lnet_isrouter(lp) && lp->lp_alive_count == 0))
                lnet_notify_locked(lp, 0, 1, lp->lp_last_alive);

        return alive;
}


/* NB: returns 1 when alive, 0 when dead, negative when error;
 *     may drop the LNET_NET_LOCK */
int
lnet_peer_alive_locked(lnet_net_cpud_t *netcd, lnet_peer_t *lp)
{
        cfs_time_t now = cfs_time_current();

        if (!lnet_peer_aliveness_enabled(lp))
                return -ENODEV;

        if (lnet_peer_is_alive(lp, now))
                return 1;

        /* Peer appears dead, but we should avoid frequent NI queries (at
         * most once per lnet_queryinterval seconds). */
        if (lp->lp_last_query != 0) {
                static const int lnet_queryinterval = 1;

                cfs_time_t next_query =
                           cfs_time_add(lp->lp_last_query,
                                        cfs_time_seconds(lnet_queryinterval));

                if (cfs_time_before(now, next_query)) {
                        if (lp->lp_alive)
                                CWARN("Unexpected aliveness of peer %s: "
                                      "%d < %d (%d/%d)\n",
                                      libcfs_nid2str(lp->lp_nid),
                                      (int)now, (int)next_query,
                                      lnet_queryinterval,
                                      lp->lp_ni->ni_peertimeout);
                        return 0;
                }
        }

        /* query NI for latest aliveness news */
        lnet_ni_peer_alive(netcd, lp);

        if (lnet_peer_is_alive(lp, now))
                return 1;

        lnet_notify_locked(lp, 0, 0, lp->lp_last_alive);
        return 0;
}

int
lnet_post_send_locked(lnet_net_cpud_t *netcd, lnet_msg_t *msg, int do_send)
{
        /* lnet_send is going to LNET_NET_UNLOCK immediately after this,
         * so it sets do_send FALSE and I don't do the unlock/send/lock bit.
         * I return EAGAIN if msg blocked, EHOSTUNREACH if msg_txpeer
         * appears dead, and 0 if sent or OK to send */
        lnet_peer_t     *lp = msg->msg_txpeer;
        lnet_ni_t       *ni = lp->lp_ni;
        lnet_ni_cpud_t  *nic = ni->ni_cpuds[netcd->lnc_cpuid];

        /* non-lnet_send() callers have checked before */
        LASSERT (!do_send || msg->msg_delayed);
        LASSERT (!msg->msg_receiving);

        /* NB 'lp' is always the next hop */
        if ((msg->msg_target.pid & LNET_PID_USERFLAG) == 0 &&
            lnet_peer_alive_locked(netcd, lp) == 0) {
                netcd->lnc_counters.drop_count++;
                netcd->lnc_counters.drop_count += msg->msg_len;
                LNET_NET_UNLOCK(netcd);

                CNETERR("Dropping message for %s: peer not alive\n",
                        libcfs_id2str(msg->msg_target));
                if (do_send)
                        lnet_finalize(ni, msg, -EHOSTUNREACH);

                LNET_NET_LOCK(netcd);
                return EHOSTUNREACH;
        }

        if (!msg->msg_peertxcredit) {
                LASSERT ((lp->lp_txcredits < 0) ==
                         !cfs_list_empty(&lp->lp_txq));

                msg->msg_peertxcredit = 1;
                lp->lp_txqnob += msg->msg_len + sizeof(lnet_hdr_t);
                lp->lp_txcredits--;

                if (lp->lp_txcredits < lp->lp_mintxcredits)
                        lp->lp_mintxcredits = lp->lp_txcredits;

                if (lp->lp_txcredits < 0) {
                        msg->msg_delayed = 1;
                        cfs_list_add_tail(&msg->msg_list, &lp->lp_txq);
                        return EAGAIN;
                }
        }

        if (!msg->msg_txcredit) {
                LASSERT ((nic->nc_txcredits < 0) ==
                         !cfs_list_empty(&nic->nc_txq));

                msg->msg_txcredit = 1;
                nic->nc_txcredits--;

                if (nic->nc_txcredits < nic->nc_mintxcredits)
                        nic->nc_mintxcredits = nic->nc_txcredits;

                if (nic->nc_txcredits < 0) {
                        msg->msg_delayed = 1;
                        cfs_list_add_tail(&msg->msg_list, &nic->nc_txq);
                        return EAGAIN;
                }
        }

        if (do_send) {
                LNET_NET_UNLOCK(netcd);
                lnet_ni_send(ni, msg);
                LNET_NET_LOCK(netcd);
        }
        return 0;
}

#ifdef __KERNEL__

static lnet_rtrbufpool_t *
lnet_msg2bufpool(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        lnet_rtrbufpool_t *rbp = &netcd->lnc_rtrpools[0];

        LASSERT (msg->msg_len <= LNET_MTU);
        while (msg->msg_len > (unsigned int)rbp->rbp_npages * CFS_PAGE_SIZE) {
                rbp++;
                LASSERT (rbp < &netcd->lnc_rtrpools[LNET_NRBPOOLS]);
        }

        return rbp;
}

int
lnet_post_routed_recv_locked(lnet_net_cpud_t *netcd,
                             lnet_msg_t *msg, int do_recv)
{
        /* lnet_parse is going to LNET_NET_UNLOCK immediately after this, so it
         * sets do_recv FALSE and I don't do the unlock/send/lock bit.  I
         * return EAGAIN if msg blocked and 0 if received or OK to receive */
        lnet_peer_t         *lp = msg->msg_rxpeer;
        lnet_rtrbufpool_t   *rbp;
        lnet_rtrbuf_t       *rb;

        LASSERT (msg->msg_iov == NULL);
        LASSERT (msg->msg_kiov == NULL);
        LASSERT (msg->msg_niov == 0);
        LASSERT (msg->msg_routing);
        LASSERT (msg->msg_receiving);
        LASSERT (!msg->msg_sending);

        /* non-lnet_parse callers only send delayed messages */
        LASSERT (!do_recv || msg->msg_delayed);

        if (!msg->msg_peerrtrcredit) {
                LASSERT ((lp->lp_rtrcredits < 0) ==
                         !cfs_list_empty(&lp->lp_rtrq));

                msg->msg_peerrtrcredit = 1;
                lp->lp_rtrcredits--;
                if (lp->lp_rtrcredits < lp->lp_minrtrcredits)
                        lp->lp_minrtrcredits = lp->lp_rtrcredits;

                if (lp->lp_rtrcredits < 0) {
                        /* must have checked eager_recv before here */
                        LASSERT (msg->msg_delayed);
                        cfs_list_add_tail(&msg->msg_list, &lp->lp_rtrq);
                        return EAGAIN;
                }
        }

        rbp = lnet_msg2bufpool(netcd, msg);

        if (!msg->msg_rtrcredit) {
                LASSERT ((rbp->rbp_credits < 0) ==
                         !cfs_list_empty(&rbp->rbp_msgs));

                msg->msg_rtrcredit = 1;
                rbp->rbp_credits--;
                if (rbp->rbp_credits < rbp->rbp_mincredits)
                        rbp->rbp_mincredits = rbp->rbp_credits;

                if (rbp->rbp_credits < 0) {
                        /* must have checked eager_recv before here */
                        LASSERT (msg->msg_delayed);
                        cfs_list_add_tail(&msg->msg_list, &rbp->rbp_msgs);
                        return EAGAIN;
                }
        }

        LASSERT (!cfs_list_empty(&rbp->rbp_bufs));
        rb = cfs_list_entry(rbp->rbp_bufs.next, lnet_rtrbuf_t, rb_list);
        cfs_list_del(&rb->rb_list);

        msg->msg_niov = rbp->rbp_npages;
        msg->msg_kiov = &rb->rb_kiov[0];

        if (do_recv) {
                LNET_NET_UNLOCK(netcd);
                lnet_ni_recv(lp->lp_ni, msg->msg_private, msg, 1,
                             0, msg->msg_len, msg->msg_len);
                LNET_NET_LOCK(netcd);
        }
        return 0;
}
#endif

static void
lnet_return_tx_credits_locked(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        lnet_peer_t       *txpeer = msg->msg_txpeer;
        lnet_net_cpud_t   *netcd_tx;
        lnet_msg_t        *msg2;
        lnet_ni_t         *ni;

        /* NB: it's possible to have netcd_tx != netcd for routed msg */
        netcd_tx = txpeer == NULL ? netcd :
                   lnet_netcd_from_nid_locked(txpeer->lp_nid);

        LNET_NET_LOCK_SWITCH(netcd, netcd_tx);

        if (msg->msg_txcredit) {
                lnet_ni_cpud_t  *nic;
                /* give back NI txcredits */
                msg->msg_txcredit = 0;
                ni = txpeer->lp_ni;
                nic = ni->ni_cpuds[netcd_tx->lnc_cpuid];

                LASSERT((nic->nc_txcredits < 0) ==
                        !cfs_list_empty(&nic->nc_txq));

                nic->nc_txcredits++;
                if (nic->nc_txcredits <= 0) {
                        msg2 = cfs_list_entry(nic->nc_txq.next,
                                              lnet_msg_t, msg_list);
                        cfs_list_del(&msg2->msg_list);

                        LASSERT(msg2->msg_txpeer->lp_ni == ni);
                        LASSERT(msg2->msg_delayed);

                        (void) lnet_post_send_locked(netcd_tx, msg2, 1);
                }
        }

        if (msg->msg_peertxcredit) {
                /* give back peer txcredits */
                msg->msg_peertxcredit = 0;

                LASSERT((txpeer->lp_txcredits < 0) ==
                        !cfs_list_empty(&txpeer->lp_txq));

                txpeer->lp_txqnob -= msg->msg_len + sizeof(lnet_hdr_t);
                LASSERT (txpeer->lp_txqnob >= 0);

                txpeer->lp_txcredits++;
                if (txpeer->lp_txcredits <= 0) {
                        msg2 = cfs_list_entry(txpeer->lp_txq.next,
                                              lnet_msg_t, msg_list);
                        cfs_list_del(&msg2->msg_list);

                        LASSERT (msg2->msg_txpeer == txpeer);
                        LASSERT (msg2->msg_delayed);

                        (void) lnet_post_send_locked(netcd_tx, msg2, 1);
                }
        }

        if (txpeer != NULL) {
                msg->msg_txpeer = NULL;
                lnet_peer_decref_locked(netcd_tx, txpeer);
        }

        LNET_NET_LOCK_SWITCH(netcd_tx, netcd);
}

static void
lnet_return_rx_credits_locked(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        lnet_peer_t       *rxpeer = msg->msg_rxpeer;
#ifdef __KERNEL__
        lnet_msg_t        *msg2;

        if (msg->msg_rtrcredit) {
                /* give back global router credits */
                lnet_rtrbuf_t     *rb;
                lnet_rtrbufpool_t *rbp;

                /* NB If a msg ever blocks for a buffer in rbp_msgs, it stays
                 * there until it gets one allocated, or aborts the wait
                 * itself */
                LASSERT (msg->msg_kiov != NULL);

                rb = cfs_list_entry(msg->msg_kiov, lnet_rtrbuf_t, rb_kiov[0]);
                rbp = rb->rb_pool;
                LASSERT (rbp == lnet_msg2bufpool(netcd, msg));

                msg->msg_kiov = NULL;
                msg->msg_rtrcredit = 0;

                LASSERT((rbp->rbp_credits < 0) ==
                        !cfs_list_empty(&rbp->rbp_msgs));
                LASSERT((rbp->rbp_credits > 0) ==
                        !cfs_list_empty(&rbp->rbp_bufs));

                cfs_list_add(&rb->rb_list, &rbp->rbp_bufs);
                rbp->rbp_credits++;
                if (rbp->rbp_credits <= 0) {
                        msg2 = cfs_list_entry(rbp->rbp_msgs.next,
                                              lnet_msg_t, msg_list);
                        cfs_list_del(&msg2->msg_list);

                        (void) lnet_post_routed_recv_locked(netcd, msg2, 1);
                }
        }

        if (msg->msg_peerrtrcredit) {
                /* give back peer router credits */
                msg->msg_peerrtrcredit = 0;

                LASSERT((rxpeer->lp_rtrcredits < 0) ==
                        !cfs_list_empty(&rxpeer->lp_rtrq));

                rxpeer->lp_rtrcredits++;
                if (rxpeer->lp_rtrcredits <= 0) {
                        msg2 = cfs_list_entry(rxpeer->lp_rtrq.next,
                                              lnet_msg_t, msg_list);
                        cfs_list_del(&msg2->msg_list);

                        (void) lnet_post_routed_recv_locked(netcd, msg2, 1);
                }
        }
#else
        LASSERT (!msg->msg_rtrcredit);
        LASSERT (!msg->msg_peerrtrcredit);
#endif
        if (rxpeer != NULL) {
                msg->msg_rxpeer = NULL;
                lnet_peer_decref_locked(netcd, rxpeer);
        }
}

void
lnet_return_credits_locked(lnet_net_cpud_t *netcd, lnet_msg_t *msg)
{
        lnet_return_rx_credits_locked(netcd, msg);
        lnet_return_tx_credits_locked(netcd, msg);
}

static lnet_peer_t *
lnet_find_gateway(lnet_ni_t *ni, lnet_nid_t nid)
{
        lnet_remotenet_t *rnet;
        lnet_route_t     *rtr;
        lnet_route_t     *rtr_best;
        lnet_peer_t      *lp;
        lnet_peer_t      *lp2;
        int               footprint;
        int               rc;

        rnet = lnet_find_net_locked(LNET_NIDNET(nid));
        if (rnet == NULL)
                return NULL;

        /* Find the best gateway I can use */
        lp = NULL;
        rtr_best = NULL;
        footprint = rnet->lrn_footprint++; /* race but no matter */
        footprint = rnet->lrn_nroutes - (footprint % rnet->lrn_nroutes);
        cfs_list_for_each_entry_reverse(rtr, &rnet->lrn_routes, lr_list) {
                lp2 = rtr->lr_gateway;

                footprint--;
                if (!lp2->lp_alive)
                        continue;

                if (lnet_router_down_ni(lp2, rnet->lrn_net) > 0)
                        continue;

                if (ni != NULL && lp2->lp_ni != ni)
                        continue;

                if (lp == NULL) {
                        rtr_best = rtr;
                        lp = lp2;
                        continue;
                }

                rc = lnet_compare_routes(rtr, rtr_best);
                if (rc < 0)
                        continue;

                if (rc > 0 || footprint >= 0) {
                        rtr_best = rtr;
                        lp = lp2;
                }
        }

        return lp;
}

int
lnet_send(lnet_nid_t src_nid, lnet_msg_t *msg)
{
        lnet_nid_t            dst_nid = msg->msg_target.nid;
        lnet_nid_t            gw_nid  = LNET_NID_ANY;
        lnet_net_cpud_t      *netcd;
        lnet_ni_t            *local_ni;
        lnet_ni_t            *src_ni;
        lnet_peer_t          *lp;
        __u64                 version;
        int                   rc;

        LASSERT (msg->msg_txpeer == NULL);
        LASSERT (!msg->msg_sending);
        LASSERT (!msg->msg_target_is_router);
        LASSERT (!msg->msg_receiving);

        if (dst_nid == LNET_NID_ANY) {
                CERROR("Target can't be LNET_NID_ANY\n");
                return -EINVAL;
        }

        msg->msg_sending = 1;

        version = 0;
 again:
        if (gw_nid == LNET_NID_ANY)
                netcd = lnet_netcd_from_nid(dst_nid);
        else
                netcd = lnet_netcd_from_nid(gw_nid);

        /* NB! ni != NULL == interface pre-determined (ACK/REPLY) */
        LNET_NET_LOCK(netcd);

        if (unlikely(the_lnet.ln_shutdown)) {
                LNET_NET_UNLOCK(netcd);
                return -ESHUTDOWN;
        }

        if (unlikely(gw_nid != LNET_NID_ANY &&
                     version != the_lnet.ln_remote_nets_version)) {
                /* already know preferred router, but router-table changed */
                LNET_NET_UNLOCK(netcd);
                gw_nid  = LNET_NID_ANY;
                version = 0;
                goto again;
        }

        if (src_nid == LNET_NID_ANY) {
                src_ni = NULL;
        } else {
                src_ni = lnet_nid2ni_locked(netcd, src_nid, netcd->lnc_cpuid);
                if (src_ni == NULL) {
                        LNET_NET_UNLOCK(netcd);
                        LCONSOLE_WARN("Can't send to %s: src %s is not a "
                                      "local nid\n", libcfs_nid2str(dst_nid),
                                      libcfs_nid2str(src_nid));
                        return -EINVAL;
                }
                LASSERT (!msg->msg_routing);
        }

        /* Is this for someone on a local network? */
        local_ni = lnet_net2ni_locked(netcd, LNET_NIDNET(dst_nid),
                                      netcd->lnc_cpuid);

        if (local_ni != NULL) {
                if (unlikely(gw_nid != LNET_NID_ANY)) {
                        gw_nid = LNET_NID_ANY;
                        version = 0;
                        goto again;
                }

                if (src_ni == NULL) {
                        src_ni = local_ni;
                        src_nid = src_ni->ni_nid;
                } else if (src_ni == local_ni) {
                        lnet_ni_decref_locked(netcd, local_ni,
                                              netcd->lnc_cpuid);
                } else {
                        lnet_ni_decref_locked(netcd, local_ni,
                                              netcd->lnc_cpuid);
                        lnet_ni_decref_locked(netcd, src_ni,
                                              netcd->lnc_cpuid);
                        LNET_NET_UNLOCK(netcd);
                        LCONSOLE_WARN("No route to %s via from %s\n",
                                      libcfs_nid2str(dst_nid),
                                      libcfs_nid2str(src_nid));
                        return -EINVAL;
                }

                LASSERT (src_nid != LNET_NID_ANY);

                if (!msg->msg_routing)
                        msg->msg_hdr.src_nid = cpu_to_le64(src_nid);

                if (src_ni == the_lnet.ln_loni) {
                        /* No send credit hassles with LOLND */
                        if (!lnet_msg_is_commited(msg)) {
                                LASSERT(msg->msg_type == LNET_MSG_PUT ||
                                        msg->msg_type == LNET_MSG_GET);
                                lnet_msg_commit(netcd, msg, msg->msg_type);
                        }
                        LNET_NET_UNLOCK(netcd);
                        lnet_ni_send(src_ni, msg);
                        LNET_NET_LOCK(netcd);
                        lnet_ni_decref_locked(netcd, src_ni,
                                              netcd->lnc_cpuid);
                        LNET_NET_UNLOCK(netcd);
                        return 0;
                }

                rc = lnet_nid2peer_locked(netcd, &lp, dst_nid);
                /* lp has ref on src_ni; lose mine */
                lnet_ni_decref_locked(netcd, src_ni,
                                      netcd->lnc_cpuid);
                if (rc != 0) {
                        LNET_NET_UNLOCK(netcd);
                        LCONSOLE_WARN("Error %d finding peer %s\n", rc,
                                      libcfs_nid2str(dst_nid));
                        /* ENOMEM or shutting down */
                        return rc;
                }
                LASSERT (lp->lp_ni == src_ni);
        } else {
#ifndef __KERNEL__
                LNET_NET_UNLOCK(netcd);

                /* NB
                 * - once application finishes computation, check here to update
                 *   router states before it waits for pending IO in LNetEQPoll
                 * - recursion breaker: router checker sends no message
                 *   to remote networks */
                if (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING)
                        lnet_router_checker();

                LNET_NET_LOCK(netcd);
#endif
                /* sending to a remote network */
                if (gw_nid != LNET_NID_ANY) {
                        lp = lnet_find_peer_locked(netcd, gw_nid);
                        LASSERT(lp != NULL); /* no change on remote net */
                        if (unlikely(!lp->lp_alive)) {
                                LNET_NET_UNLOCK(netcd);
                                gw_nid = LNET_NID_ANY;
                                version = 0;
                                goto again;
                        }

                } else {
                        /* no preferred router yet */
                        lp = lnet_find_gateway(src_ni, dst_nid);
                        if (lp == NULL) {
                                if (src_ni != NULL) {
                                        lnet_ni_decref_locked(netcd, src_ni,
                                                              netcd->lnc_cpuid);
                                }
                                LNET_NET_UNLOCK(netcd);
                                LCONSOLE_WARN("No route to %s via %s "
                                              "(all routers down)\n",
                                              libcfs_id2str(msg->msg_target),
                                              libcfs_nid2str(src_nid));
                                return -EHOSTUNREACH;
                        }

                        gw_nid = lp->lp_nid;
                        if (netcd != lnet_netcd_from_nid_locked(gw_nid)) {
                                version = the_lnet.ln_remote_nets_version;
                                if (src_ni != NULL) {
                                        lnet_ni_decref_locked(netcd, src_ni,
                                                              netcd->lnc_cpuid);
                                }
                                LNET_NET_UNLOCK(netcd);
                                goto again;
                        }
                }

                if (src_ni == NULL) {
                        src_ni = lp->lp_ni;
                        src_nid = src_ni->ni_nid;
                } else {
                        LASSERT (src_ni == lp->lp_ni);
                        lnet_ni_decref_locked(netcd, src_ni,
                                              netcd->lnc_cpuid);
                }

                lnet_peer_addref_locked(lp);

                LASSERT (src_nid != LNET_NID_ANY);

                if (!msg->msg_routing) {
                        /* I'm the source and now I know which NI to send on */
                        msg->msg_hdr.src_nid = cpu_to_le64(src_nid);
                }

                msg->msg_target_is_router = 1;
                msg->msg_target.nid = lp->lp_nid;
                msg->msg_target.pid = LUSTRE_SRV_LNET_PID;
        }

        /* 'lp' is our best choice of peer */

        LASSERT (!msg->msg_peertxcredit);
        LASSERT (!msg->msg_txcredit);
        LASSERT (msg->msg_txpeer == NULL);

        msg->msg_txpeer = lp;                   /* msg takes my ref on lp */

        if (!lnet_msg_is_commited(msg)) /* routed, or REPLY or ACK */
                lnet_msg_commit(netcd, msg, msg->msg_type);

        rc = lnet_post_send_locked(netcd, msg, 0);
        LNET_NET_UNLOCK(netcd);

        if (rc == EHOSTUNREACH)
                return -EHOSTUNREACH;

        if (rc == 0)
                lnet_ni_send(src_ni, msg);

        return 0;
}

static void
lnet_drop_message(lnet_net_cpud_t *netcd, lnet_ni_t *ni,
                  void *private, unsigned int nob)
{
        LNET_NET_LOCK(netcd);
        netcd->lnc_counters.drop_count++;
        netcd->lnc_counters.drop_length += nob;
        LNET_NET_UNLOCK(netcd);

        lnet_ni_recv(ni, private, NULL, 0, 0, 0, nob);
}

static void
lnet_drop_delayed_put(lnet_msg_t *msg, char *reason)
{
        lnet_net_cpud_t   *netcd;
        lnet_process_id_t  id = {0};

        id.nid = msg->msg_hdr.src_nid;
        id.pid = msg->msg_hdr.src_pid;

        LASSERT (msg->msg_md == NULL);
        LASSERT (msg->msg_delayed);
        LASSERT (msg->msg_rxpeer != NULL);
        LASSERT (msg->msg_hdr.type == LNET_MSG_PUT);

        CWARN("Dropping delayed PUT from %s portal %d match "LPU64
              " offset %d length %d: %s\n", 
              libcfs_id2str(id),
              msg->msg_hdr.msg.put.ptl_index,
              msg->msg_hdr.msg.put.match_bits,
              msg->msg_hdr.msg.put.offset,
              msg->msg_hdr.payload_length,
              reason);

        /* NB I can't drop msg's ref on msg_rxpeer until after I've
         * called lnet_drop_message(), so I just hang onto msg as well
         * until that's done */

        netcd = lnet_netcd_from_nid(msg->msg_rxpeer->lp_nid);

        lnet_drop_message(netcd, msg->msg_rxpeer->lp_ni,
                          msg->msg_private, msg->msg_len);
        LNET_NET_LOCK(netcd);

        lnet_peer_decref_locked(netcd, msg->msg_rxpeer);
        msg->msg_rxpeer = NULL;

        lnet_msg_free(msg);

        LNET_NET_UNLOCK(netcd);
}

/**
 * Turn on the lazy portal attribute. Use with caution!
 *
 * This portal attribute only affects incoming PUT requests to the portal,
 * and is off by default. By default, if there's no matching MD for an
 * incoming PUT request, it is simply dropped. With the lazy attribute on,
 * such requests are queued indefinitely until either a matching MD is
 * posted to the portal or the lazy attribute is turned off.
 *
 * It would prevent dropped requests, however it should be regarded as the
 * last line of defense - i.e. users must keep a close watch on active
 * buffers on a lazy portal and once it becomes too low post more buffers as
 * soon as possible. This is because delayed requests usually have detrimental
 * effects on underlying network connections. A few delayed requests often
 * suffice to bring an underlying connection to a complete halt, due to flow
 * control mechanisms.
 *
 * There's also a DOS attack risk. If users don't post match-all MDs on a
 * lazy portal, a malicious peer can easily stop a service by sending some
 * PUT requests with match bits that won't match any MD. A routed server is
 * especially vulnerable since the connections to its neighbor routers are
 * shared among all clients.
 *
 * \param portal Index of the portal to enable the lazy attribute on.
 *
 * \retval 0       On success.
 * \retval -EINVAL If \a portal is not a valid index.
 */
int
LNetSetLazyPortal(int portal)
{
        lnet_portal_t  *ptl;

        if (portal < 0 || portal >= MAX_PORTALS)
                return -EINVAL;

        CDEBUG(D_NET, "Setting portal %d lazy\n", portal);

        ptl = the_lnet.ln_portals[portal];

        LNET_OBJ_LOCK_DEFAULT();
        lnet_portal_setopt(ptl, LNET_PTL_LAZY);
        LNET_OBJ_UNLOCK_DEFAULT();

        return 0;
}

/**
 * Turn off the lazy portal attribute. Delayed requests on the portal,
 * if any, will be all dropped when this function returns.
 *
 * \param portal Index of the portal to disable the lazy attribute on.
 *
 * \retval 0       On success.
 * \retval -EINVAL If \a portal is not a valid index.
 */
int
LNetClearLazyPortal(int portal)
{
        CFS_LIST_HEAD    (zombies);
        lnet_portal_t    *ptl;
        lnet_msg_t       *msg;

        if (portal < 0 || portal >= MAX_PORTALS)
                return -EINVAL;

        ptl = the_lnet.ln_portals[portal];

        LNET_OBJ_LOCK_DEFAULT();

        if (!lnet_portal_is_lazy(ptl)) {
                LNET_OBJ_UNLOCK_DEFAULT();
                return 0;
        }

        if (the_lnet.ln_shutdown)
                CWARN ("Active lazy portal %d on exit\n", portal);
        else
                CDEBUG (D_NET, "clearing portal %d lazy\n", portal);

        lnet_portal_unsetopt(ptl, LNET_PTL_LAZY);
        cfs_list_splice_init(&ptl->ptl_msg_delayed, &zombies);

        LNET_OBJ_UNLOCK_DEFAULT();

        while (!cfs_list_empty(&zombies)) {
                msg = cfs_list_entry(zombies.next, lnet_msg_t, msg_list);
                cfs_list_del(&msg->msg_list);

                lnet_drop_delayed_put(msg, "Clearing lazy portal attr");
        }

        return 0;
}

static void
lnet_recv_put(lnet_msg_t *msg, int delayed)
{
        lnet_hdr_t       *hdr = &msg->msg_hdr;
        lnet_net_cpud_t  *netcd = lnet_netcd_from_nid(msg->msg_rxpeer->lp_nid);

        lnet_build_msg_event(LNET_EVENT_PUT, msg);

        LNET_NET_LOCK(netcd);

        lnet_msg_commit(netcd, msg, msg->msg_type);

        LNET_NET_UNLOCK(netcd);

        if (msg->msg_wanted != 0)
                lnet_setpayloadbuffer(msg);

        /* Must I ACK?  If so I'll grab the ack_wmd out of the header and put
         * it back into the ACK during lnet_finalize() */
        msg->msg_ack = (!lnet_is_wire_handle_none(&hdr->msg.put.ack_wmd) &&
                        (msg->msg_md->md_options & LNET_MD_ACK_DISABLE) == 0);

        lnet_ni_recv(msg->msg_rxpeer->lp_ni,
                     msg->msg_private, msg,
                     delayed, msg->msg_offset,
                     msg->msg_wanted, msg->msg_len);
}

/* called with LNET_OBJ_LOCK held */
static int
lnet_match_msg_list(lnet_obj_cpud_t *objcd, lnet_libmd_t *md,
                    cfs_list_t *msgq, cfs_list_t *matches, cfs_list_t *drops)
{
        lnet_msg_t     *msg;
        lnet_msg_t     *tmp;
        int             exhausted = 0;

        cfs_list_for_each_entry_safe_typed(msg, tmp, msgq,
                                           lnet_msg_t, msg_list) {
                lnet_hdr_t       *hdr;
                lnet_process_id_t src;
                int               index;
                int               rc;

                hdr   = &msg->msg_hdr;
                index = hdr->msg.put.ptl_index;

                src.nid = hdr->src_nid;
                src.pid = hdr->src_pid;

                rc = lnet_try_match_md(objcd, index, LNET_MD_OP_PUT, src,
                                       hdr->payload_length,
                                       hdr->msg.put.offset,
                                       hdr->msg.put.match_bits, md, msg);

                if (rc == LNET_MATCHMD_NONE)
                        continue;

                /* Hurrah! This _is_ a match */
                cfs_list_del_init(&msg->msg_list);
                exhausted = lnet_md_exhausted(md);

                if (rc == LNET_MATCHMD_DROP) {
                        if (drops != NULL)
                                cfs_list_add_tail(&msg->msg_list, drops);
                        break;
                }

                LASSERT(rc == LNET_MATCHMD_OK);
                if (matches == NULL)
                        break;

                cfs_list_add_tail(&msg->msg_list, matches);
                CDEBUG(D_NET, "Resuming delayed PUT from %s portal %d "
                       "match "LPU64" offset %d length %d.\n",
                       libcfs_id2str(src), hdr->msg.put.ptl_index,
                       hdr->msg.put.match_bits, hdr->msg.put.offset,
                       hdr->payload_length);
                break;
        }
        return exhausted;
}

void
lnet_match_blocked_msg(lnet_obj_cpud_t *objcd, lnet_libmd_t *md)
{
        CFS_LIST_HEAD    (drops);
        CFS_LIST_HEAD    (matches);
        lnet_msg_t       *msg;
        lnet_me_t        *me  = md->md_me;
        lnet_portal_t    *ptl = the_lnet.ln_portals[me->me_portal];
        int               exhausted = 0;

        LASSERT (me->me_portal < MAX_PORTALS);
        LASSERT (md->md_refcount == 0); /* a brand new MD */

        /* racing but not a big issue */
        if (cfs_list_empty(&ptl->ptl_msg_stealing) && /* nobody steal */
            cfs_list_empty(&ptl->ptl_msg_delayed))    /* no blocked message */
                return;

        /* portal operations are serialized by locking lnet_objcd_default() */
        LNET_OBJ_LOCK_MORE(objcd, lnet_objcd_default());

        /* satisfy stealing list firstly, because the stealer
         * is still looping for buffer */
        if (!cfs_list_empty(&ptl->ptl_msg_stealing)) {
                exhausted = lnet_match_msg_list(objcd, md,
                                                &ptl->ptl_msg_stealing,
                                                NULL, NULL);
        }

        if (!exhausted && !cfs_list_empty(&ptl->ptl_msg_delayed)) {
                LASSERT (lnet_portal_is_lazy(ptl));
                exhausted = lnet_match_msg_list(objcd, md,
                                                &ptl->ptl_msg_delayed,
                                                &matches, &drops);
        }

        if (ptl->ptl_ent_stealing < 0 ||
            ptl->ptl_ent_stealing != objcd->loc_cpuid)
                ptl->ptl_ent_stealing = objcd->loc_cpuid;

        LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
        LNET_OBJ_UNLOCK(objcd);

        while (!cfs_list_empty(&drops)) {
                msg = cfs_list_entry(drops.next, lnet_msg_t, msg_list);

                cfs_list_del(&msg->msg_list);
                lnet_drop_delayed_put(msg, "Bad match");
        }

        while (!cfs_list_empty(&matches)) {
                msg = cfs_list_entry(matches.next, lnet_msg_t, msg_list);

                cfs_list_del(&msg->msg_list);
                /* md won't disappear under me, since each msg
                 * holds a ref on it */
                lnet_recv_put(msg, 1);
        }

        LNET_OBJ_LOCK(objcd);
}

static int
lnet_delay_put_locked(lnet_ni_t *ni, lnet_obj_cpud_t *objcd,
                      lnet_portal_t *ptl, lnet_msg_t *msg)
{
        int     rc  = LNET_MATCHMD_NONE;
        int     rc2 = 0;

        if (ni->ni_lnd->lnd_eager_recv != NULL) {
                LNET_OBJ_UNLOCK(objcd);
                if ((rc2 = lnet_eager_recv(msg)) != 0)
                        CERROR("Eager receive faild: %d\n", rc2);

                LNET_OBJ_LOCK(objcd);
        }

        LASSERT(!msg->msg_delayed);

        LNET_OBJ_LOCK_MORE(objcd, lnet_objcd_default());
        if (!cfs_list_empty(&msg->msg_list)) {
                /* still on stealing list */
                cfs_list_del_init(&msg->msg_list);
                if (rc2 != 0)
                        rc = LNET_MATCHMD_DROP;
        } else { /* removed from stealing list */
                rc = msg->msg_md != NULL ?
                     LNET_MATCHMD_OK : LNET_MATCHMD_DROP;
        }

        if (rc != LNET_MATCHMD_NONE) {
                LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
                return rc;
        }

        if (the_lnet.ln_shutdown || !lnet_portal_is_lazy(ptl)) {
                LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
                return LNET_MATCHMD_DROP;
        }

        msg->msg_delayed = 1;
        cfs_list_add_tail(&msg->msg_list, &ptl->ptl_msg_delayed);
        LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());

        /* msg delayed */
        return LNET_MATCHMD_NONE;
}

static int
lnet_parse_put(lnet_ni_t *ni, lnet_msg_t *msg)
{
        int                rc;
        int                index;
        lnet_obj_cpud_t   *objcd;
        lnet_hdr_t        *hdr = &msg->msg_hdr;
        unsigned int       rlength = hdr->payload_length;
        lnet_process_id_t  src= {0};

        src.nid = hdr->src_nid;
        src.pid = hdr->src_pid;

        /* Convert put fields to host byte order */
        hdr->msg.put.match_bits = le64_to_cpu(hdr->msg.put.match_bits);
        hdr->msg.put.ptl_index  = le32_to_cpu(hdr->msg.put.ptl_index);
        hdr->msg.put.offset     = le32_to_cpu(hdr->msg.put.offset);

        index = hdr->msg.put.ptl_index;

        objcd = lnet_objcd_from_match(index, LNET_INS_NONE,
                                      src, hdr->msg.put.match_bits);
        LNET_OBJ_LOCK(objcd);
        rc = lnet_match_md(objcd, index, LNET_MD_OP_PUT, src,
                           rlength, hdr->msg.put.offset,
                           hdr->msg.put.match_bits, msg);

 again:
        switch (rc) {
        default:
                LBUG();

        case LNET_MATCHMD_OK:
                LNET_OBJ_UNLOCK(objcd);
                lnet_recv_put(msg, 0);
                return 0;

        case LNET_MATCHMD_NONE:
                rc = lnet_delay_put_locked(ni, objcd,
                                           the_lnet.ln_portals[index], msg);
                if (rc != LNET_MATCHMD_NONE)
                        goto again;

                /* delayed put */
                LNET_OBJ_UNLOCK(objcd);
                CDEBUG(D_NET, "Delaying PUT from %s portal %d match "
                       LPU64" offset %d length %d: no match \n",
                       libcfs_id2str(src), index,
                       hdr->msg.put.match_bits,
                       hdr->msg.put.offset, rlength);

                return 0;

        case LNET_MATCHMD_DROP:
                LNET_OBJ_UNLOCK(objcd);
                /* fall through */
        }

        CNETERR("Dropping PUT from %s portal %d match "LPU64
                " offset %d length %d: %d\n",
                libcfs_id2str(src), index,
                hdr->msg.put.match_bits,
                hdr->msg.put.offset, rlength, rc);

        return ENOENT;          /* +ve: OK but no match */
}

static int
lnet_parse_get(lnet_ni_t *ni, lnet_msg_t *msg, int rdma_get)
{
        lnet_hdr_t        *hdr = &msg->msg_hdr;
        lnet_get_t        *get = &hdr->msg.get;
        lnet_obj_cpud_t   *objcd;
        lnet_net_cpud_t   *netcd;
        lnet_process_id_t  src = {0};
        lnet_handle_wire_t reply_wmd;
        int                rc;

        src.nid = hdr->src_nid;
        src.pid = hdr->src_pid;

        /* Convert get fields to host byte order */
        get->match_bits  = le64_to_cpu(get->match_bits);
        get->ptl_index   = le32_to_cpu(get->ptl_index);
        get->sink_length = le32_to_cpu(get->sink_length);
        get->src_offset  = le32_to_cpu(get->src_offset);

        objcd = lnet_objcd_from_match(get->ptl_index,
                                      LNET_INS_NONE,
                                      src, get->match_bits);
        LNET_OBJ_LOCK(objcd);

        rc = lnet_match_md(objcd, get->ptl_index, LNET_MD_OP_GET, src,
                           get->sink_length, get->src_offset,
                           get->match_bits, msg);
        LNET_OBJ_UNLOCK(objcd);

        if (rc == LNET_MATCHMD_DROP) {
                CNETERR("Dropping GET from %s portal %d match "LPU64
                        " offset %d length %d\n",
                        libcfs_id2str(src),
                        hdr->msg.get.ptl_index,
                        hdr->msg.get.match_bits,
                        hdr->msg.get.src_offset,
                        hdr->msg.get.sink_length);
                return ENOENT;
        }

        LASSERT (rc == LNET_MATCHMD_OK);

        lnet_build_msg_event(LNET_EVENT_GET, msg);

        netcd = lnet_netcd_from_nid(msg->msg_rxpeer->lp_nid);
        LNET_NET_LOCK(netcd);

        lnet_msg_commit(netcd, msg, msg->msg_type);

        LNET_NET_UNLOCK(netcd);

        reply_wmd = hdr->msg.get.return_wmd;

        lnet_prep_send(msg, LNET_MSG_REPLY, src,
                       msg->msg_offset, msg->msg_wanted);

        msg->msg_hdr.msg.reply.dst_wmd = reply_wmd;

        if (rdma_get) {
                /* The LND completes the REPLY from her recv procedure */
                lnet_ni_recv(ni, msg->msg_private, msg, 0,
                             msg->msg_offset, msg->msg_wanted, msg->msg_len);
                return 0;
        }

        lnet_ni_recv(ni, msg->msg_private, NULL, 0, 0, 0, 0);
        msg->msg_receiving = 0;

        rc = lnet_send(ni->ni_nid, msg);

        if (rc < 0) {
                /* didn't get as far as lnet_ni_send() */
                CERROR("%s: Unable to send REPLY for GET from %s: %d\n",
                       libcfs_nid2str(ni->ni_nid), libcfs_id2str(src), rc);

                lnet_finalize(ni, msg, rc);
        }

        return 0;
}

static int
lnet_parse_reply(lnet_ni_t *ni, lnet_msg_t *msg)
{
        lnet_net_cpud_t   *netcd;
        void             *private = msg->msg_private;
        lnet_hdr_t       *hdr = &msg->msg_hdr;
        lnet_obj_cpud_t   *objcd;
        lnet_process_id_t src = {0};
        lnet_libmd_t     *md  = NULL;

        src.nid = hdr->src_nid;
        src.pid = hdr->src_pid;

        objcd = lnet_objcd_from_cookie(hdr->msg.reply.dst_wmd.wh_object_cookie);
        LNET_OBJ_LOCK(objcd);

        /* NB handles only looked up by creator (no flips) */
        md = lnet_wire_handle2md(objcd, &hdr->msg.reply.dst_wmd);

        if (md == NULL || md->md_threshold == 0 || md->md_me != NULL) {
                CNETERR("%s: Dropping REPLY from %s for %s "
                        "MD "LPX64"."LPX64"\n",
                        libcfs_nid2str(ni->ni_nid), libcfs_id2str(src),
                        (md == NULL) ? "invalid" : "inactive",
                        hdr->msg.reply.dst_wmd.wh_interface_cookie,
                        hdr->msg.reply.dst_wmd.wh_object_cookie);
                if (md != NULL && md->md_me != NULL)
                        CERROR("REPLY MD also attached to portal %d\n",
                               md->md_me->me_portal);

                LNET_OBJ_UNLOCK(objcd);
                return ENOENT;                  /* +ve: OK but no match */
        }

        LASSERT (md->md_offset == 0);

        msg->msg_wanted = MIN(msg->msg_len, (int)md->md_length);

        if (msg->msg_wanted < msg->msg_len &&
            (md->md_options & LNET_MD_TRUNCATE) == 0) {
                LNET_OBJ_UNLOCK(objcd);

                CNETERR("%s: Dropping REPLY from %s length %d "
                        "for MD "LPX64" would overflow (%d)\n",
                        libcfs_nid2str(ni->ni_nid), libcfs_id2str(src),
                        msg->msg_len, hdr->msg.reply.dst_wmd.wh_object_cookie,
                        msg->msg_wanted);

                return ENOENT;          /* +ve: OK but no match */
        }

        lnet_md_commit(md, msg);

        LNET_OBJ_UNLOCK(objcd);

        CDEBUG(D_NET, "%s: Reply from %s of length %d/%d into md "LPX64"\n",
               libcfs_nid2str(ni->ni_nid), libcfs_id2str(src),
               msg->msg_wanted, msg->msg_len,
               hdr->msg.reply.dst_wmd.wh_object_cookie);

        if (msg->msg_wanted != 0)
                lnet_setpayloadbuffer(msg);

        lnet_build_msg_event(LNET_EVENT_REPLY, msg);

        netcd = lnet_netcd_from_nid(msg->msg_rxpeer->lp_nid);
        LNET_NET_LOCK(netcd);

        lnet_msg_commit(netcd, msg, msg->msg_type);

        LNET_NET_UNLOCK(netcd);

        lnet_ni_recv(ni, private, msg, 0, 0, msg->msg_wanted, msg->msg_len);

        return 0;
}

static int
lnet_parse_ack(lnet_ni_t *ni, lnet_msg_t *msg)
{
        lnet_hdr_t       *hdr = &msg->msg_hdr;
        lnet_process_id_t src = {0};
        lnet_libmd_t     *md  = NULL;
        lnet_obj_cpud_t  *objcd;
        lnet_net_cpud_t  *netcd;

        src.nid = hdr->src_nid;
        src.pid = hdr->src_pid;

        /* Convert ack fields to host byte order */
        hdr->msg.ack.match_bits = le64_to_cpu(hdr->msg.ack.match_bits);
        hdr->msg.ack.mlength = le32_to_cpu(hdr->msg.ack.mlength);

        objcd = lnet_objcd_from_cookie(hdr->msg.ack.dst_wmd.wh_object_cookie);
        LNET_OBJ_LOCK(objcd);

        /* NB handles only looked up by creator (no flips) */
        md = lnet_wire_handle2md(objcd, &hdr->msg.ack.dst_wmd);

        if (md == NULL || md->md_threshold == 0 || md->md_me != NULL) {
                /* Don't moan; this is expected */
                CDEBUG(D_NET,
                       "%s: Dropping ACK from %s to %s MD "LPX64"."LPX64"\n",
                       libcfs_nid2str(ni->ni_nid), libcfs_id2str(src),
                       (md == NULL) ? "invalid" : "inactive",
                       hdr->msg.ack.dst_wmd.wh_interface_cookie,
                       hdr->msg.ack.dst_wmd.wh_object_cookie);
                if (md != NULL && md->md_me != NULL)
                        CERROR("Source MD also attached to portal %d\n",
                               md->md_me->me_portal);
                LNET_OBJ_UNLOCK(objcd);
                return ENOENT;                  /* +ve! */
        }

        lnet_md_commit(md, msg);
        msg->msg_wanted = 0;

        LNET_OBJ_UNLOCK(objcd);

        CDEBUG(D_NET, "%s: ACK from %s into md "LPX64"\n",
               libcfs_nid2str(ni->ni_nid), libcfs_id2str(src),
               hdr->msg.ack.dst_wmd.wh_object_cookie);

        lnet_build_msg_event(LNET_EVENT_ACK, msg);

        netcd = lnet_netcd_from_nid(msg->msg_rxpeer->lp_nid);
        LNET_NET_LOCK(netcd);

        lnet_msg_commit(netcd, msg, msg->msg_type);

        LNET_NET_UNLOCK(netcd);

        lnet_ni_recv(ni, msg->msg_private, msg, 0, 0, 0, msg->msg_len);
        return 0;
}

char *
lnet_msgtyp2str (int type)
{
        switch (type) {
        case LNET_MSG_ACK:
                return ("ACK");
        case LNET_MSG_PUT:
                return ("PUT");
        case LNET_MSG_GET:
                return ("GET");
        case LNET_MSG_REPLY:
                return ("REPLY");
        case LNET_MSG_HELLO:
                return ("HELLO");
        default:
                return ("<UNKNOWN>");
        }
}

void
lnet_print_hdr(lnet_hdr_t * hdr)
{
        lnet_process_id_t src = {0};
        lnet_process_id_t dst = {0};
        char *type_str = lnet_msgtyp2str (hdr->type);

        src.nid = hdr->src_nid;
        src.pid = hdr->src_pid;

        dst.nid = hdr->dest_nid;
        dst.pid = hdr->dest_pid;

        CWARN("P3 Header at %p of type %s\n", hdr, type_str);
        CWARN("    From %s\n", libcfs_id2str(src));
        CWARN("    To   %s\n", libcfs_id2str(dst));

        switch (hdr->type) {
        default:
                break;

        case LNET_MSG_PUT:
                CWARN("    Ptl index %d, ack md "LPX64"."LPX64", "
                      "match bits "LPU64"\n",
                      hdr->msg.put.ptl_index,
                      hdr->msg.put.ack_wmd.wh_interface_cookie,
                      hdr->msg.put.ack_wmd.wh_object_cookie,
                      hdr->msg.put.match_bits);
                CWARN("    Length %d, offset %d, hdr data "LPX64"\n",
                      hdr->payload_length, hdr->msg.put.offset,
                      hdr->msg.put.hdr_data);
                break;

        case LNET_MSG_GET:
                CWARN("    Ptl index %d, return md "LPX64"."LPX64", "
                      "match bits "LPU64"\n", hdr->msg.get.ptl_index,
                      hdr->msg.get.return_wmd.wh_interface_cookie,
                      hdr->msg.get.return_wmd.wh_object_cookie,
                      hdr->msg.get.match_bits);
                CWARN("    Length %d, src offset %d\n",
                      hdr->msg.get.sink_length,
                      hdr->msg.get.src_offset);
                break;

        case LNET_MSG_ACK:
                CWARN("    dst md "LPX64"."LPX64", "
                      "manipulated length %d\n",
                      hdr->msg.ack.dst_wmd.wh_interface_cookie,
                      hdr->msg.ack.dst_wmd.wh_object_cookie,
                      hdr->msg.ack.mlength);
                break;

        case LNET_MSG_REPLY:
                CWARN("    dst md "LPX64"."LPX64", "
                      "length %d\n",
                      hdr->msg.reply.dst_wmd.wh_interface_cookie,
                      hdr->msg.reply.dst_wmd.wh_object_cookie,
                      hdr->payload_length);
        }

}

int
lnet_parse(lnet_ni_t *ni, lnet_hdr_t *hdr, lnet_nid_t from_nid,
           void *private, int rdma_req)
{
        lnet_net_cpud_t *netcd;
        lnet_msg_t      *msg;
        int              rc = 0;
        int              for_me;
        lnet_pid_t       dest_pid;
        lnet_nid_t       dest_nid;
        lnet_nid_t       src_nid;
        cfs_time_t       now;
        __u32            payload_length;
        __u32            type;

        LASSERT (!cfs_in_interrupt ());

        type = le32_to_cpu(hdr->type);
        src_nid = le64_to_cpu(hdr->src_nid);
        dest_nid = le64_to_cpu(hdr->dest_nid);
        dest_pid = le32_to_cpu(hdr->dest_pid);
        payload_length = le32_to_cpu(hdr->payload_length);

        for_me = (ni->ni_nid == dest_nid);

        switch (type) {
        case LNET_MSG_ACK:
        case LNET_MSG_GET:
                if (payload_length > 0) {
                        CERROR("%s, src %s: bad %s payload %d (0 expected)\n",
                               libcfs_nid2str(from_nid),
                               libcfs_nid2str(src_nid),
                               lnet_msgtyp2str(type), payload_length);
                        return -EPROTO;
                }
                break;

        case LNET_MSG_PUT:
        case LNET_MSG_REPLY:
                if (payload_length > (__u32)(for_me ? LNET_MAX_PAYLOAD : LNET_MTU)) {
                        CERROR("%s, src %s: bad %s payload %d "
                               "(%d max expected)\n",
                               libcfs_nid2str(from_nid),
                               libcfs_nid2str(src_nid),
                               lnet_msgtyp2str(type),
                               payload_length,
                               for_me ? LNET_MAX_PAYLOAD : LNET_MTU);
                        return -EPROTO;
                }
                break;

        default:
                CERROR("%s, src %s: Bad message type 0x%x\n",
                       libcfs_nid2str(from_nid),
                       libcfs_nid2str(src_nid), type);
                return -EPROTO;
        }

        now = cfs_time_current_sec();
        if (the_lnet.ln_routing &&
            cfs_time_after(now, ni->ni_last_alive)) {
                /* NB: ni_last_alive and ns_status are always serialized
                 * by LNET_NET_LOCK_DEFAULT() see lnet_update_ni_status() */
                LNET_NET_LOCK_DEFAULT();

                ni->ni_last_alive = now;
                if (ni->ni_status != NULL &&
                    ni->ni_status->ns_status == LNET_NI_STATUS_DOWN)
                        ni->ni_status->ns_status = LNET_NI_STATUS_UP;

                LNET_NET_UNLOCK_DEFAULT();
        }

        /* Regard a bad destination NID as a protocol error.  Senders should
         * know what they're doing; if they don't they're misconfigured, buggy
         * or malicious so we chop them off at the knees :) */
        netcd = lnet_netcd_from_nid(src_nid);

        if (!for_me) {
                if (LNET_NIDNET(dest_nid) == LNET_NIDNET(ni->ni_nid)) {
                        /* should have gone direct */
                        CERROR ("%s, src %s: Bad dest nid %s "
                                "(should have been sent direct)\n",
                                libcfs_nid2str(from_nid),
                                libcfs_nid2str(src_nid),
                                libcfs_nid2str(dest_nid));
                        return -EPROTO;
                }

                if (lnet_islocalnid(dest_nid)) {
                        /* dest is another local NI; sender should have used
                         * this node's NID on its own network */
                        CERROR ("%s, src %s: Bad dest nid %s "
                                "(it's my nid but on a different network)\n",
                                libcfs_nid2str(from_nid),
                                libcfs_nid2str(src_nid),
                                libcfs_nid2str(dest_nid));
                        return -EPROTO;
                }

                if (rdma_req && type == LNET_MSG_GET) {
                        CERROR ("%s, src %s: Bad optimized GET for %s "
                                "(final destination must be me)\n",
                                libcfs_nid2str(from_nid),
                                libcfs_nid2str(src_nid),
                                libcfs_nid2str(dest_nid));
                        return -EPROTO;
                }

                if (!the_lnet.ln_routing) {
                        CERROR ("%s, src %s: Dropping message for %s "
                                "(routing not enabled)\n",
                                libcfs_nid2str(from_nid),
                                libcfs_nid2str(src_nid),
                                libcfs_nid2str(dest_nid));
                        goto drop;
                }
        }

        /* Message looks OK; we're not going to return an error, so we MUST
         * call back lnd_recv() come what may... */

        if (!cfs_list_empty(&netcd->lnc_test_peers) &&  /* normally we don't */
            lnet_fail_peer(src_nid, 0))        /* shall we now? */
        {
                CERROR("%s, src %s: Dropping %s to simulate failure\n",
                       libcfs_nid2str(from_nid), libcfs_nid2str(src_nid),
                       lnet_msgtyp2str(type));
                goto drop;
        }

        msg = lnet_msg_alloc();
        if (msg == NULL) {
                CERROR("%s, src %s: Dropping %s (out of memory)\n",
                       libcfs_nid2str(from_nid), libcfs_nid2str(src_nid), 
                       lnet_msgtyp2str(type));
                goto drop;
        }

        /* msg zeroed in lnet_msg_alloc; i.e. flags all clear, pointers NULL etc */

        msg->msg_type = type;
        msg->msg_private = private;
        msg->msg_receiving = 1;
        msg->msg_len = msg->msg_wanted = payload_length;
        msg->msg_offset = 0;
        msg->msg_hdr = *hdr;

        if (for_me) {
                /* convert common msg->hdr fields to host byteorder */
                msg->msg_hdr.type     = type;
                msg->msg_hdr.src_nid  = src_nid;
                msg->msg_hdr.src_pid  = le32_to_cpu(msg->msg_hdr.src_pid);
                msg->msg_hdr.dest_nid = dest_nid;
                msg->msg_hdr.dest_pid = le32_to_cpu(msg->msg_hdr.dest_pid);
                msg->msg_hdr.payload_length = payload_length;

        } else { /* remote network */
                msg->msg_target.pid   = le32_to_cpu(hdr->dest_pid);
                msg->msg_target.nid   = dest_nid;
                msg->msg_routing      = 1;
        }

        netcd = lnet_netcd_from_nid(from_nid);

        LNET_NET_LOCK(netcd);
        rc = lnet_nid2peer_locked(netcd, &msg->msg_rxpeer, from_nid);
        if (rc != 0) {
                LNET_NET_UNLOCK(netcd);
                CERROR("%s, src %s: Dropping %s "
                       "(error %d looking up sender)\n",
                       libcfs_nid2str(from_nid), libcfs_nid2str(src_nid),
                       lnet_msgtyp2str(type), rc);
                goto free_drop;
        }

#ifndef __KERNEL__
        LNET_NET_UNLOCK(netcd);

        LASSERT (for_me);
#else
        if (!for_me) {
                if (msg->msg_rxpeer->lp_rtrcredits <= 0 ||
                    lnet_msg2bufpool(netcd, msg)->rbp_credits <= 0) {
                        LASSERT (!msg->msg_delayed);

                        rc = 0;
                        msg->msg_delayed = 1;
                        if (ni->ni_lnd->lnd_eager_recv != NULL) {
                                LNET_NET_UNLOCK(netcd);
                                rc = lnet_eager_recv(msg);
                                LNET_NET_LOCK(netcd);
                        }

                        if (rc != 0) {
                                LNET_NET_UNLOCK(netcd);
                                goto free_drop;
                        }
                }
                lnet_msg_commit(netcd, msg, msg->msg_type);
                rc = lnet_post_routed_recv_locked(netcd, msg, 0);
                LNET_NET_UNLOCK(netcd);

                if (rc == 0)
                        lnet_ni_recv(ni, msg->msg_private, msg, 0,
                                     0, payload_length, payload_length);
                return 0;
        }

        LNET_NET_UNLOCK(netcd);
#endif

        switch (type) {
        case LNET_MSG_ACK:
                rc = lnet_parse_ack(ni, msg);
                break;
        case LNET_MSG_PUT:
                rc = lnet_parse_put(ni, msg);
                break;
        case LNET_MSG_GET:
                rc = lnet_parse_get(ni, msg, rdma_req);
                break;
        case LNET_MSG_REPLY:
                rc = lnet_parse_reply(ni, msg);
                break;
        default:
                LASSERT(0);
                goto free_drop;  /* prevent an unused label if !kernel */
        }

        if (rc == 0)
                return 0;

        LASSERT (rc == ENOENT);

 free_drop:
        LASSERT (msg->msg_md == NULL);

        LNET_NET_LOCK(netcd);
        if (msg->msg_rxpeer != NULL) {
                lnet_peer_decref_locked(netcd, msg->msg_rxpeer);
                msg->msg_rxpeer = NULL;
        }
        lnet_msg_free(msg);   /* expects LNET_NET_LOCK held */

        LNET_NET_UNLOCK(netcd);

 drop:
        lnet_drop_message(netcd, ni, private, payload_length);
        return 0;
}

/**
 * Initiate an asynchronous PUT operation.
 *
 * There are several events associated with a PUT: completion of the send on
 * the initiator node (LNET_EVENT_SEND), and when the send completes
 * successfully, the receipt of an acknowledgment (LNET_EVENT_ACK) indicating
 * that the operation was accepted by the target. The event LNET_EVENT_PUT is
 * used at the target node to indicate the completion of incoming data
 * delivery.
 *
 * The local events will be logged in the EQ associated with the MD pointed to
 * by \a mdh handle. Using a MD without an associated EQ results in these
 * events being discarded. In this case, the caller must have another
 * mechanism (e.g., a higher level protocol) for determining when it is safe
 * to modify the memory region associated with the MD.
 *
 * Note that LNet does not guarantee the order of LNET_EVENT_SEND and
 * LNET_EVENT_ACK, though intuitively ACK should happen after SEND.
 *
 * \param self Indicates the NID of a local interface through which to send
 * the PUT request. Use LNET_NID_ANY to let LNet choose one by itself.
 * \param mdh A handle for the MD that describes the memory to be sent. The MD
 * must be "free floating" (See LNetMDBind()).
 * \param ack Controls whether an acknowledgment is requested.
 * Acknowledgments are only sent when they are requested by the initiating
 * process and the target MD enables them.
 * \param target A process identifier for the target process.
 * \param portal The index in the \a target's portal table.
 * \param match_bits The match bits to use for MD selection at the target
 * process.
 * \param offset The offset into the target MD (only used when the target
 * MD has the LNET_MD_MANAGE_REMOTE option set).
 * \param hdr_data 64 bits of user data that can be included in the message
 * header. This data is written to an event queue entry at the target if an
 * EQ is present on the matching MD.
 *
 * \retval  0      Success, and only in this case events will be generated
 * and logged to EQ (if it exists).
 * \retval -EIO    Simulated failure.
 * \retval -ENOMEM Memory allocation failure.
 * \retval -ENOENT Invalid MD object.
 *
 * \see lnet_event_t::hdr_data and lnet_event_kind_t.
 */
int
LNetPut(lnet_nid_t self, lnet_handle_md_t mdh, lnet_ack_req_t ack,
        lnet_process_id_t target, unsigned int portal,
        __u64 match_bits, unsigned int offset,
        __u64 hdr_data)
{
        lnet_net_cpud_t  *netcd = lnet_netcd_from_nid(target.nid);
        lnet_obj_cpud_t  *objcd;
        lnet_msg_t       *msg;
        lnet_libmd_t     *md;
        int               rc;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        if (!cfs_list_empty (&netcd->lnc_test_peers) && /* normally we don't */
            lnet_fail_peer (target.nid, 1))          /* shall we now? */
        {
                CERROR("Dropping PUT to %s: simulated failure\n",
                       libcfs_id2str(target));
                return -EIO;
        }

        msg = lnet_msg_alloc();
        if (msg == NULL) {
                CERROR("Dropping PUT to %s: ENOMEM on lnet_msg_t\n",
                       libcfs_id2str(target));
                return -ENOMEM;
        }

        msg->msg_vmflush = !!cfs_memory_pressure_get();

        objcd = lnet_objcd_from_cookie(mdh.cookie);
        LNET_OBJ_LOCK(objcd);

        md = lnet_handle2md(objcd, &mdh);
        if (md == NULL || md->md_threshold == 0 || md->md_me != NULL) {
                CERROR("Dropping PUT ("LPU64":%d:%s): MD (%d) invalid\n",
                       match_bits, portal, libcfs_id2str(target),
                       md == NULL ? -1 : md->md_threshold);
                if (md != NULL && md->md_me != NULL) {
                        CERROR("Source MD also attached to portal %d\n",
                               md->md_me->me_portal);
                }

                LNET_OBJ_UNLOCK(objcd);

                LNET_NET_LOCK(netcd);
                lnet_msg_free(msg);

                LNET_NET_UNLOCK(netcd);
                return -ENOENT;
        }

        lnet_md_commit(md, msg);

        LNET_OBJ_UNLOCK(objcd);

        CDEBUG(D_NET, "LNetPut -> %s\n", libcfs_id2str(target));

        lnet_prep_send(msg, LNET_MSG_PUT, target, 0, md->md_length);

        msg->msg_hdr.msg.put.match_bits = cpu_to_le64(match_bits);
        msg->msg_hdr.msg.put.ptl_index  = cpu_to_le32(portal);
        msg->msg_hdr.msg.put.offset     = cpu_to_le32(offset);
        msg->msg_hdr.msg.put.hdr_data   = hdr_data;

        /* NB handles only looked up by creator (no flips) */
        if (ack == LNET_ACK_REQ) {
                msg->msg_hdr.msg.put.ack_wmd.wh_interface_cookie =
                        the_lnet.ln_interface_cookie;
                msg->msg_hdr.msg.put.ack_wmd.wh_object_cookie =
                        md->md_lh.lh_cookie;
        } else {
                msg->msg_hdr.msg.put.ack_wmd.wh_interface_cookie =
                        LNET_WIRE_HANDLE_COOKIE_NONE;
                msg->msg_hdr.msg.put.ack_wmd.wh_object_cookie =
                        LNET_WIRE_HANDLE_COOKIE_NONE;
        }

        lnet_build_msg_event(LNET_EVENT_SEND, msg);

        rc = lnet_send(self, msg);

        if (rc != 0) {
                CNETERR("Error sending PUT to %s: %d\n",
                        libcfs_id2str(target), rc);
                lnet_finalize (NULL, msg, rc);
        }

        /* completion will be signalled by an event */
        return 0;
}

lnet_msg_t *
lnet_create_reply_msg (lnet_ni_t *ni, lnet_msg_t *getmsg)
{
        /* The LND can DMA direct to the GET md (i.e. no REPLY msg).  This
         * returns a msg for the LND to pass to lnet_finalize() when the sink
         * data has been received.
         *
         * CAVEAT EMPTOR: 'getmsg' is the original GET, which is freed when
         * lnet_finalize() is called on it, so the LND must call this first */
        lnet_msg_t        *msg = NULL;
        lnet_libmd_t      *getmd = getmsg->msg_md;
        lnet_process_id_t  peer_id = getmsg->msg_target;
        lnet_net_cpud_t   *netcd = lnet_netcd_from_nid(peer_id.nid);
        lnet_obj_cpud_t   *objcd;

        LASSERT (!getmsg->msg_target_is_router);
        LASSERT (!getmsg->msg_routing);

        msg = lnet_msg_alloc();
        if (msg == NULL) {
                CERROR ("%s: Dropping REPLY from %s: can't allocate msg\n",
                        libcfs_nid2str(ni->ni_nid), libcfs_id2str(peer_id));
                goto drop;
        }

        objcd = lnet_objcd_from_cookie(getmd->md_lh.lh_cookie);
        LNET_OBJ_LOCK(objcd);

        LASSERT (getmd->md_refcount > 0);

        if (getmd->md_threshold == 0) {
                CERROR ("%s: Dropping REPLY from %s for inactive MD %p\n",
                        libcfs_nid2str(ni->ni_nid), libcfs_id2str(peer_id), 
                        getmd);
                LNET_OBJ_UNLOCK(objcd);
                goto drop;
        }

        LASSERT (getmd->md_offset == 0);

        lnet_md_commit(getmd, msg);

        LNET_OBJ_UNLOCK(objcd);

        CDEBUG(D_NET, "%s: Reply from %s md %p\n", 
               libcfs_nid2str(ni->ni_nid), libcfs_id2str(peer_id), getmd);

        msg->msg_type = LNET_MSG_GET; /* flag this msg as an "optimized" GET */
        msg->msg_wanted = getmd->md_length;

        msg->msg_hdr.src_nid        = peer_id.nid;
        msg->msg_hdr.payload_length = getmd->md_length;

        lnet_build_msg_event(LNET_EVENT_REPLY, msg);

        LNET_NET_LOCK(netcd);

        lnet_msg_commit(netcd, msg, LNET_MSG_REPLY);

        LNET_NET_UNLOCK(netcd);

        return msg;


 drop:
        LNET_NET_LOCK(netcd);
        if (msg != NULL)
                lnet_msg_free(msg);

        netcd->lnc_counters.drop_count++;
        netcd->lnc_counters.drop_length += getmd->md_length;

        LNET_NET_UNLOCK(netcd);

        return NULL;
}

void
lnet_set_reply_msg_len(lnet_ni_t *ni, lnet_msg_t *reply, unsigned int len)
{
        /* Set the REPLY length, now the RDMA that elides the REPLY message has
         * completed and I know it. */
        LASSERT (reply != NULL);
        LASSERT (reply->msg_type == LNET_MSG_GET);
        LASSERT (reply->msg_ev.type == LNET_EVENT_REPLY);

        /* NB I trusted my peer to RDMA.  If she tells me she's written beyond
         * the end of my buffer, I might as well be dead. */
        LASSERT (len <= reply->msg_ev.mlength);

        reply->msg_ev.mlength = len;
}

/**
 * Initiate an asynchronous GET operation.
 *
 * On the initiator node, an LNET_EVENT_SEND is logged when the GET request
 * is sent, and an LNET_EVENT_REPLY is logged when the data returned from
 * the target node in the REPLY has been written to local MD.
 *
 * On the target node, an LNET_EVENT_GET is logged when the GET request
 * arrives and is accepted into a MD.
 *
 * \param self,target,portal,match_bits,offset See the discussion in LNetPut().
 * \param mdh A handle for the MD that describes the memory into which the
 * requested data will be received. The MD must be "free floating" (See LNetMDBind()).
 *
 * \retval  0      Success, and only in this case events will be generated
 * and logged to EQ (if it exists) of the MD.
 * \retval -EIO    Simulated failure.
 * \retval -ENOMEM Memory allocation failure.
 * \retval -ENOENT Invalid MD object.
 */
int
LNetGet(lnet_nid_t self, lnet_handle_md_t mdh,
        lnet_process_id_t target, unsigned int portal,
        __u64 match_bits, unsigned int offset)
{
        lnet_msg_t       *msg;
        lnet_libmd_t     *md;
        lnet_net_cpud_t   *netcd;
        lnet_obj_cpud_t   *objcd;
        int               rc;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        netcd = lnet_netcd_from_nid(target.nid);
        if (!cfs_list_empty(&netcd->lnc_test_peers) && /* normally we don't */
            lnet_fail_peer(target.nid, 1))           /* shall we now? */
        {
                CERROR("Dropping GET to %s: simulated failure\n",
                       libcfs_id2str(target));
                return -EIO;
        }

        msg = lnet_msg_alloc();
        if (msg == NULL) {
                CERROR("Dropping GET to %s: ENOMEM on lnet_msg_t\n",
                       libcfs_id2str(target));
                return -ENOMEM;
        }

        objcd = lnet_objcd_from_cookie(mdh.cookie);
        LNET_OBJ_LOCK(objcd);

        md = lnet_handle2md(objcd, &mdh);
        if (md == NULL || md->md_threshold == 0 || md->md_me != NULL) {
                CERROR("Dropping GET ("LPU64":%d:%s): MD (%d) invalid\n",
                       match_bits, portal, libcfs_id2str(target),
                       md == NULL ? -1 : md->md_threshold);

                if (md != NULL && md->md_me != NULL) {
                        CERROR("REPLY MD also attached to portal %d\n",
                               md->md_me->me_portal);
                }

                LNET_OBJ_UNLOCK(objcd);

                LNET_NET_LOCK(netcd);
                lnet_msg_free(msg);

                LNET_NET_UNLOCK(netcd);

                return -ENOENT;
        }

        lnet_md_commit(md, msg);

        LNET_OBJ_UNLOCK(objcd);

        CDEBUG(D_NET, "LNetGet -> %s\n", libcfs_id2str(target));

        lnet_prep_send(msg, LNET_MSG_GET, target, 0, 0);

        msg->msg_hdr.msg.get.match_bits  = cpu_to_le64(match_bits);
        msg->msg_hdr.msg.get.ptl_index   = cpu_to_le32(portal);
        msg->msg_hdr.msg.get.src_offset  = cpu_to_le32(offset);
        msg->msg_hdr.msg.get.sink_length = cpu_to_le32(md->md_length);

        /* NB handles only looked up by creator (no flips) */
        msg->msg_hdr.msg.get.return_wmd.wh_interface_cookie =
                the_lnet.ln_interface_cookie;
        msg->msg_hdr.msg.get.return_wmd.wh_object_cookie =
                md->md_lh.lh_cookie;

        lnet_build_msg_event(LNET_EVENT_SEND, msg);

        rc = lnet_send(self, msg);

        if (rc != 0) {
                CNETERR("error sending GET to %s: %d\n",
                        libcfs_id2str(target), rc);
                lnet_finalize (NULL, msg, rc);
        }

        /* completion will be signalled by an event */
        return 0;
}

/**
 * Calculate distance to node at \a dstnid.
 *
 * \param dstnid Target NID.
 * \param srcnidp If not NULL, NID of the local interface to reach \a dstnid
 * is saved here.
 * \param orderp If not NULL, order of the route to reach \a dstnid is saved
 * here.
 *
 * \retval 0 If \a dstnid belongs to a local interface, and reserved option
 * local_nid_dist_zero is set, which is the default.
 * \retval positives Distance to target NID, i.e. number of hops plus one.
 * \retval -EHOSTUNREACH If \a dstnid is not reachable.
 */
int
LNetDist (lnet_nid_t dstnid, lnet_nid_t *srcnidp, __u32 *orderp)
{
        cfs_list_t       *e;
        lnet_ni_t        *ni;
        lnet_remotenet_t *rnet;
        lnet_net_cpud_t  *netcd  = lnet_netcd_current();
        __u32             dstnet = LNET_NIDNET(dstnid);
        int               hops;
        __u32             order = 2;

        /* if !local_nid_dist_zero, I don't return a distance of 0 ever
         * (when lustre sees a distance of 0, it substitutes 0@lo), so I
         * keep order 0 free for 0@lo and order 1 free for a local NID
         * match */

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        LNET_NET_LOCK(netcd);

        cfs_list_for_each (e, &the_lnet.ln_nis) {
                ni = cfs_list_entry(e, lnet_ni_t, ni_list);

                if (ni->ni_nid == dstnid) {
                        if (srcnidp != NULL)
                                *srcnidp = dstnid;
                        if (orderp != NULL) {
                                if (LNET_NETTYP(LNET_NIDNET(dstnid)) == LOLND)
                                        *orderp = 0;
                                else
                                        *orderp = 1;
                        }
                        LNET_NET_UNLOCK(netcd);

                        return local_nid_dist_zero ? 0 : 1;
                }

                if (LNET_NIDNET(ni->ni_nid) == dstnet) {
                        if (srcnidp != NULL)
                                *srcnidp = ni->ni_nid;
                        if (orderp != NULL)
                                *orderp = order;
                        LNET_NET_UNLOCK(netcd);
                        return 1;
                }

                order++;
        }

        cfs_list_for_each (e, &the_lnet.ln_remote_nets) {
                rnet = cfs_list_entry(e, lnet_remotenet_t, lrn_list);

                if (rnet->lrn_net == dstnet) {
                        lnet_route_t *route;
                        lnet_route_t *shortest = NULL;

                        LASSERT (!cfs_list_empty(&rnet->lrn_routes));

                        cfs_list_for_each_entry(route, &rnet->lrn_routes,
                                                lr_list) {
                                if (shortest == NULL ||
                                    route->lr_hops < shortest->lr_hops)
                                        shortest = route;
                        }

                        LASSERT (shortest != NULL);
                        hops = shortest->lr_hops;
                        if (srcnidp != NULL)
                                *srcnidp = shortest->lr_gateway->lp_ni->ni_nid;
                        if (orderp != NULL)
                                *orderp = order;
                        LNET_NET_UNLOCK(netcd);
                        return hops + 1;
                }
                order++;
        }

        LNET_NET_UNLOCK(netcd);
        return -EHOSTUNREACH;
}

/**
 * Set the number of asynchronous messages expected from a target process.
 *
 * This function is only meaningful for userspace callers. It's a no-op when
 * called from kernel.
 *
 * Asynchronous messages are those that can come from a target when the
 * userspace process is not waiting for IO to complete; e.g., AST callbacks
 * from Lustre servers. Specifying the expected number of such messages
 * allows them to be eagerly received when user process is not running in
 * LNet; otherwise network errors may occur.
 *
 * \param id Process ID of the target process.
 * \param nasync Number of asynchronous messages expected from the target.
 *
 * \return 0 on success, and an error code otherwise.
 */
int
LNetSetAsync(lnet_process_id_t id, int nasync)
{
#ifdef __KERNEL__
        return 0;
#else
        lnet_ni_t        *ni;
        lnet_net_cpud_t  *netcd;
        lnet_remotenet_t *rnet;
        cfs_list_t       *tmp;
        lnet_route_t     *route;
        lnet_nid_t       *nids;
        int               nnids;
        int               maxnids = 256;
        int               rc = 0;
        int               rc2;

        /* Target on a local network? */

        ni = lnet_net2ni(LNET_NIDNET(id.nid));
        if (ni != NULL) {
                if (ni->ni_lnd->lnd_setasync != NULL)
                        rc = (ni->ni_lnd->lnd_setasync)(ni, id, nasync);
                lnet_ni_decref(ni);
                return rc;
        }

        /* Target on a remote network: apply to routers */
 again:
        LIBCFS_ALLOC(nids, maxnids * sizeof(*nids));
        if (nids == NULL)
                return -ENOMEM;
        nnids = 0;

        /* Snapshot all the router NIDs */
        netcd = lnet_netcd_current();
        LNET_NET_LOCK(netcd);
        rnet = lnet_find_net_locked(LNET_NIDNET(id.nid));
        if (rnet != NULL) {
                cfs_list_for_each(tmp, &rnet->lrn_routes) {
                        if (nnids == maxnids) {
                                LNET_NET_UNLOCK(netcd);
                                LIBCFS_FREE(nids, maxnids * sizeof(*nids));
                                maxnids *= 2;
                                goto again;
                        }

                        route = cfs_list_entry(tmp, lnet_route_t, lr_list);
                        nids[nnids++] = route->lr_gateway->lp_nid;
                }
        }
        LNET_NET_UNLOCK(netcd);

        /* set async on all the routers */
        while (nnids-- > 0) {
                id.pid = LUSTRE_SRV_LNET_PID;
                id.nid = nids[nnids];

                ni = lnet_net2ni(LNET_NIDNET(id.nid));
                if (ni == NULL)
                        continue;

                if (ni->ni_lnd->lnd_setasync != NULL) {
                        rc2 = (ni->ni_lnd->lnd_setasync)(ni, id, nasync);
                        if (rc2 != 0)
                                rc = rc2;
                }
                lnet_ni_decref(ni);
        }

        LIBCFS_FREE(nids, maxnids * sizeof(*nids));
        return rc;
#endif
}
