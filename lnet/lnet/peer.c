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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lnet/lnet/peer.c
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

void
lnet_clear_peer_table(void)
{
        lnet_net_cpud_t *netcd;
        int             i;
        int             j;

        LASSERT (the_lnet.ln_shutdown);         /* i.e. no new peers */

        lnet_netcd_for_each(i) {
                netcd = lnet_netcd_from_cpuid(i);
                LNET_NET_LOCK(netcd);
                for (j = 0; j < LNET_PEER_HASH_SIZE; j++) {
                        cfs_list_t *peers = &netcd->lnc_peer_hash[j];

                        while (!cfs_list_empty(peers)) {
                                lnet_peer_t *lp = cfs_list_entry(peers->next,
                                                                 lnet_peer_t,
                                                                 lp_hashlist);
                                cfs_list_del(&lp->lp_hashlist);
                                /* lose hash table's ref */
                                lnet_peer_decref_locked(netcd, lp);
                        }
                }
                LNET_NET_UNLOCK(netcd);
        }

        lnet_netcd_for_each(i) {
                netcd = lnet_netcd_from_cpuid(i);
                LNET_NET_LOCK(netcd);
                for (j = 3; netcd->lnc_npeers != 0;j++) {
                        LNET_NET_UNLOCK(netcd);

                        if ((j & (j - 1)) == 0) {
                                CDEBUG(D_WARNING,
                                       "Waiting for %d peers on CPU %d\n",
                                netcd->lnc_npeers, i);
                        }
                        cfs_pause(cfs_time_seconds(1));

                        LNET_NET_LOCK(netcd);
                }
                LNET_NET_UNLOCK(netcd);
        }
}

void
lnet_destroy_peer_locked(lnet_net_cpud_t *netcd, lnet_peer_t *lp)
{
        lnet_net_cpud_t *netcd2 = netcd != LNET_NET_ALL ? netcd :
                                  lnet_netcd_from_nid_locked(lp->lp_nid);

        lnet_ni_decref_locked(netcd, lp->lp_ni, netcd2->lnc_cpuid);
        LNET_NET_UNLOCK(netcd);

        LASSERT (lp->lp_refcount == 0);
        LASSERT (lp->lp_rtr_refcount == 0);
	LASSERT (cfs_list_empty(&lp->lp_txq));
        LASSERT (lp->lp_txqnob == 0);
        LASSERT (lp->lp_rcd == NULL);

	LIBCFS_FREE(lp, sizeof(*lp));

        LNET_NET_LOCK(netcd);

        LASSERT(netcd2->lnc_npeers > 0);
        netcd2->lnc_npeers--;
}

lnet_peer_t *
lnet_find_peer_locked(lnet_net_cpud_t *netcd, lnet_nid_t nid)
{
        cfs_list_t       *peers;
        cfs_list_t       *tmp;
        lnet_peer_t      *lp;

        if (the_lnet.ln_shutdown)
                return NULL;

        if (unlikely(netcd == LNET_NET_ALL))
                netcd = lnet_netcd_from_nid_locked(nid);

        peers = lnet_nid2peerhash(netcd, nid);
        cfs_list_for_each (tmp, peers) {
                lp = cfs_list_entry(tmp, lnet_peer_t, lp_hashlist);

                if (lp->lp_nid == nid) {
                        lnet_peer_addref_locked(lp);
                        return lp;
                }
        }

        return NULL;
}

int
lnet_nid2peer_locked(lnet_net_cpud_t *netcd, lnet_peer_t **lpp, lnet_nid_t nid)
{
	lnet_peer_t     *lp;
	lnet_peer_t     *lp2;
        lnet_net_cpud_t *netcd2;

        lp = lnet_find_peer_locked(netcd, nid);
        if (lp != NULL) {
                *lpp = lp;
                return 0;
        }

        LNET_NET_UNLOCK(netcd);

	LIBCFS_ALLOC(lp, sizeof(*lp));
	if (lp == NULL) {
                *lpp = NULL;
                LNET_NET_LOCK(netcd);
                return -ENOMEM;
        }

	CFS_INIT_LIST_HEAD(&lp->lp_txq);
        CFS_INIT_LIST_HEAD(&lp->lp_rtrq);

        lp->lp_notify = 0;
        lp->lp_notifylnd = 0;
        lp->lp_notifying = 0;
        lp->lp_alive_count = 0;
        lp->lp_timestamp = 0;
        lp->lp_alive = !lnet_peers_start_down(); /* 1 bit!! */
        lp->lp_last_alive = cfs_time_current(); /* assumes alive */
        lp->lp_last_query = 0; /* haven't asked NI yet */
        lp->lp_ping_timestamp = 0;
        lp->lp_nid = nid;
        lp->lp_refcount = 2;                    /* 1 for caller; 1 for hash */
        lp->lp_rtr_refcount = 0;

        LNET_NET_LOCK(netcd);

        lp2 = lnet_find_peer_locked(netcd, nid);
        if (lp2 != NULL) {
                LNET_NET_UNLOCK(netcd);
                LIBCFS_FREE(lp, sizeof(*lp));
                LNET_NET_LOCK(netcd);

                if (the_lnet.ln_shutdown) {
                        lnet_peer_decref_locked(netcd, lp2);
                        *lpp = NULL;
                        return -ESHUTDOWN;
                }

                *lpp = lp2;
                return 0;
        }

        netcd2 = netcd != LNET_NET_ALL ? netcd :
                 lnet_netcd_from_nid_locked(nid);

        lp->lp_ni = lnet_net2ni_locked(netcd, LNET_NIDNET(nid),
                                       netcd2->lnc_cpuid);
        if (lp->lp_ni == NULL) {
                LNET_NET_UNLOCK(netcd);
                LIBCFS_FREE(lp, sizeof(*lp));
                LNET_NET_LOCK(netcd);

                *lpp = NULL;
                return the_lnet.ln_shutdown ? -ESHUTDOWN : -EHOSTUNREACH;
        }

        lp->lp_txcredits    =
        lp->lp_mintxcredits = lp->lp_ni->ni_peertxcredits;
        lp->lp_rtrcredits    =
        lp->lp_minrtrcredits = lnet_peer_buffer_credits(lp->lp_ni);

        /* can't add peers after shutdown starts */
        LASSERT (!the_lnet.ln_shutdown);

        cfs_list_add_tail(&lp->lp_hashlist, lnet_nid2peerhash(netcd2, nid));
        netcd2->lnc_peertable_version++;
        netcd2->lnc_npeers++;
        *lpp = lp;
        return 0;
}

void
lnet_debug_peer(lnet_nid_t nid)
{
        lnet_net_cpud_t *netcd = lnet_netcd_from_nid(nid);
        char            *aliveness = "NA";
        int              rc;
        lnet_peer_t     *lp;

        LNET_NET_LOCK(netcd);

        rc = lnet_nid2peer_locked(netcd, &lp, nid);
        if (rc != 0) {
                LNET_NET_UNLOCK(netcd);
                CDEBUG(D_WARNING, "No peer %s\n", libcfs_nid2str(nid));
                return;
        }

        if (lnet_isrouter(lp) || lnet_peer_aliveness_enabled(lp))
                aliveness = lp->lp_alive ? "up" : "down";

        CDEBUG(D_WARNING, "%-24s %4d %5s %5d %5d %5d %5d %5d %ld\n",
               libcfs_nid2str(lp->lp_nid), lp->lp_refcount,
               aliveness, lp->lp_ni->ni_peertxcredits,
               lp->lp_rtrcredits, lp->lp_minrtrcredits,
               lp->lp_txcredits, lp->lp_mintxcredits, lp->lp_txqnob);

        lnet_peer_decref_locked(netcd, lp);

        LNET_NET_UNLOCK(netcd);
}
