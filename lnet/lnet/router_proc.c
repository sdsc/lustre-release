/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 *   This file is part of Portals
 *   http://sourceforge.net/projects/sandiaportals/
 *
 *   Portals is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Portals is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Portals; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#define DEBUG_SUBSYSTEM S_LNET
#include <libcfs/libcfs.h>
#include <lnet/lib-lnet.h>

#if defined(__KERNEL__) && defined(LNET_ROUTER)

/* This is really lnet_proc.c. You might need to update sanity test 215
 * if any file format is changed. */

static cfs_sysctl_table_header_t *lnet_table_header = NULL;

#ifndef HAVE_SYSCTL_UNNUMBERED
#define CTL_LNET         (0x100)
enum {
        PSDEV_LNET_STATS = 100,
        PSDEV_LNET_ROUTES,
        PSDEV_LNET_ROUTERS,
        PSDEV_LNET_PEERS,
        PSDEV_LNET_BUFFERS,
        PSDEV_LNET_NIS,
};
#else
#define CTL_LNET           CTL_UNNUMBERED
#define PSDEV_LNET_STATS   CTL_UNNUMBERED
#define PSDEV_LNET_ROUTES  CTL_UNNUMBERED
#define PSDEV_LNET_ROUTERS CTL_UNNUMBERED
#define PSDEV_LNET_PEERS   CTL_UNNUMBERED
#define PSDEV_LNET_BUFFERS CTL_UNNUMBERED
#define PSDEV_LNET_NIS     CTL_UNNUMBERED
#endif

#define LNET_LOFFT_BITS        (sizeof(loff_t) * 8)
/** 16 (or 4) bits for CPU id */
#define LNET_CPU_IDX_BITS     ((LNET_LOFFT_BITS <= 32) ? 4 : 16)
/** change version, 16 bits or 8 bits (unlikely) */
#define LNET_PHASH_VER_BITS     MAX(((MIN(LNET_LOFFT_BITS, 64)) / 4), 8)

#define LNET_PHASH_IDX_BITS     LNET_PEER_HASH_BITS
/** bits for peer hash offset */
#define LNET_PHASH_NUM_BITS    (LNET_LOFFT_BITS -       \
                                LNET_CPU_IDX_BITS -     \
                                LNET_PHASH_VER_BITS -   \
                                LNET_PHASH_IDX_BITS - 1)
/** bits for peer hash table */
#define LNET_PHASH_TBL_BITS    (LNET_PHASH_IDX_BITS + LNET_PHASH_NUM_BITS)
/** bits for peer hash table + hash version */
#define LNET_PHASH_ALL_BITS    (LNET_PHASH_TBL_BITS + LNET_PHASH_VER_BITS)

#define LNET_CPU_IDX_BITMASK   ((1ULL << LNET_CPU_IDX_BITS) - 1)
#define LNET_PHASH_VER_BITMASK ((1ULL << LNET_PHASH_VER_BITS) - 1)
#define LNET_PHASH_IDX_BITMASK ((1ULL << LNET_PHASH_IDX_BITS) - 1)
#define LNET_PHASH_NUM_BITMASK ((1ULL << LNET_PHASH_NUM_BITS) - 1)

#define LNET_CPU_IDX_MASK      (LNET_CPU_IDX_BITMASK << LNET_PHASH_ALL_BITS)
#define LNET_PHASH_VER_MASK    (LNET_PHASH_VER_BITMASK << LNET_PHASH_TBL_BITS)
#define LNET_PHASH_IDX_MASK    (LNET_PHASH_IDX_BITMASK << LNET_PHASH_NUM_BITS)
#define LNET_PHASH_NUM_MASK    (LNET_PHASH_NUM_BITMASK)

#define LNET_CPU_IDX_GET(pos)   (int)(((pos) & LNET_CPU_IDX_MASK) >>    \
                                      LNET_PHASH_ALL_BITS)
#define LNET_VERSION_GET(pos)   (int)(((pos) & LNET_PHASH_VER_MASK) >>  \
                                      LNET_PHASH_TBL_BITS)
#define LNET_PHASH_IDX_GET(pos) (int)(((pos) & LNET_PHASH_IDX_MASK) >>  \
                                      LNET_PHASH_NUM_BITS)
#define LNET_PHASH_NUM_GET(pos) (int)((pos) & LNET_PHASH_NUM_MASK)

#define LNET_PHASH_VER_VALID(v) (unsigned int)((v) & LNET_PHASH_VER_BITMASK)

#define LNET_PHASH_POS_MAKE(cpuid, ver, idx, num)                              \
        (((((loff_t)(cpuid)) & LNET_CPU_IDX_BITMASK) << LNET_PHASH_ALL_BITS) | \
         ((((loff_t)(ver)) & LNET_PHASH_VER_BITMASK) << LNET_PHASH_TBL_BITS) | \
         ((((loff_t)(idx)) & LNET_PHASH_IDX_BITMASK) << LNET_PHASH_NUM_BITS) | \
         ((num) & LNET_PHASH_NUM_BITMASK))

static int __proc_lnet_stats(void *data, int write,
                             loff_t pos, void *buffer, int nob)
{
        int              rc;
        lnet_counters_t *ctrs;
        int              len;
        char            *tmpstr;
        const int        tmpsiz = 256; /* 7 %u and 4 LPU64 */

        if (write) {
                lnet_reset_counters();
                return 0;
        }

        /* read */

        LIBCFS_ALLOC(ctrs, sizeof(*ctrs));
        if (ctrs == NULL)
                return -ENOMEM;

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL) {
                LIBCFS_FREE(ctrs, sizeof(*ctrs));
                return -ENOMEM;
        }

        lnet_get_counters(ctrs);

        len = snprintf(tmpstr, tmpsiz,
                       "%u %u %u %u %u %u %u "LPU64" "LPU64" "
                       LPU64" "LPU64,
                       ctrs->msgs_alloc, ctrs->msgs_max,
                       ctrs->errors,
                       ctrs->send_count, ctrs->recv_count,
                       ctrs->route_count, ctrs->drop_count,
                       ctrs->send_length, ctrs->recv_length,
                       ctrs->route_length, ctrs->drop_length);

        if (pos >= min_t(int, len, strlen(tmpstr)))
                rc = 0;
        else
                rc = cfs_trace_copyout_string(buffer, nob,
                                              tmpstr + pos, "\n");

        LIBCFS_FREE(tmpstr, tmpsiz);
        LIBCFS_FREE(ctrs, sizeof(*ctrs));
        return rc;
}

DECLARE_PROC_HANDLER(proc_lnet_stats);

int LL_PROC_PROTO(proc_lnet_routes)
{
        int        rc     = 0;
        char      *tmpstr;
        char      *s;
        const int  tmpsiz = 256;
        int        len;
        int        ver;
        int        num;

        DECLARE_LL_PROC_PPOS_DECL;

        CLASSERT(sizeof(loff_t) >= 4);
        CLASSERT(LNET_PHASH_NUM_BITS > 0);

        num = LNET_PHASH_NUM_GET(*ppos);
        ver = LNET_VERSION_GET(*ppos);

        LASSERT (!write);

        if (*lenp == 0)
                return 0;

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        if (*ppos == 0) {
                s += snprintf(s, tmpstr + tmpsiz - s, "Routing %s\n",
                              the_lnet.ln_routing ? "enabled" : "disabled");
                LASSERT (tmpstr + tmpsiz - s > 0);

                s += snprintf(s, tmpstr + tmpsiz - s, "%-8s %4s %7s %s\n",
                              "net", "hops", "state", "router");
                LASSERT (tmpstr + tmpsiz - s > 0);

                LNET_NET_LOCK_DEFAULT();
                ver = (unsigned int)the_lnet.ln_remote_nets_version;
                LNET_NET_UNLOCK_DEFAULT();
                *ppos = LNET_PHASH_POS_MAKE(0, ver, 0, num);
        } else {
                cfs_list_t        *n;
                cfs_list_t        *r;
                lnet_route_t      *route = NULL;
                lnet_remotenet_t  *rnet  = NULL;
                int                skip  = num - 1;

                LNET_NET_LOCK_DEFAULT();

                if (ver !=
                    LNET_PHASH_VER_VALID(the_lnet.ln_remote_nets_version)) {
                        LNET_NET_UNLOCK_DEFAULT();
                        LIBCFS_FREE(tmpstr, tmpsiz);
                        return -ESTALE;
                }

                n = the_lnet.ln_remote_nets.next;

                while (n != &the_lnet.ln_remote_nets && route == NULL) {
                        rnet = cfs_list_entry(n, lnet_remotenet_t, lrn_list);

                        r = rnet->lrn_routes.next;

                        while (r != &rnet->lrn_routes) {
                                lnet_route_t *re =
                                        cfs_list_entry(r, lnet_route_t,
                                                       lr_list);
                                if (skip == 0) {
                                        route = re;
                                        break;
                                }

                                skip--;
                                r = r->next;
                        }

                        n = n->next;
                }

                if (route != NULL) {
                        __u32        net   = rnet->lrn_net;
                        unsigned int hops  = route->lr_hops;
                        lnet_nid_t   nid   = route->lr_gateway->lp_nid;
                        int          alive = route->lr_gateway->lp_alive;

                        s += snprintf(s, tmpstr + tmpsiz - s,
                                      "%-8s %4u %7s %s\n",
                                      libcfs_net2str(net), hops,
                                      alive ? "up" : "down",
                                      libcfs_nid2str(nid));
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

                LNET_NET_UNLOCK_DEFAULT();
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else {
                        num += 1;
                        *ppos = LNET_PHASH_POS_MAKE(0, ver, 0, num);
                }
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

int LL_PROC_PROTO(proc_lnet_routers)
{
        int        rc = 0;
        char      *tmpstr;
        char      *s;
        const int  tmpsiz = 256;
        int        len;
        int        ver;
        int        num;

        DECLARE_LL_PROC_PPOS_DECL;

        num = LNET_PHASH_NUM_GET(*ppos);
        ver = LNET_VERSION_GET(*ppos);

        LASSERT (!write);

        if (*lenp == 0)
                return 0;

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        if (*ppos == 0) {
                s += snprintf(s, tmpstr + tmpsiz - s,
                              "%-4s %7s %9s %6s %12s %9s %8s %7s %s\n",
                              "ref", "rtr_ref", "alive_cnt", "state",
                              "last_ping", "ping_sent", "deadline",
                              "down_ni", "router");
                LASSERT (tmpstr + tmpsiz - s > 0);

                LNET_NET_LOCK_DEFAULT();
                ver = (unsigned int)the_lnet.ln_routers_version;
                LNET_NET_UNLOCK_DEFAULT();
                *ppos = LNET_PHASH_POS_MAKE(0, ver, 0, num);
        } else {
                cfs_list_t        *r;
                lnet_peer_t       *peer = NULL;
                int                skip = num - 1;

                LNET_NET_LOCK_DEFAULT();

                if (ver != LNET_PHASH_VER_VALID(the_lnet.ln_routers_version)) {
                        LNET_NET_UNLOCK_DEFAULT();
                        LIBCFS_FREE(tmpstr, tmpsiz);
                        return -ESTALE;
                }

                r = the_lnet.ln_routers.next;

                while (r != &the_lnet.ln_routers) {
                        lnet_peer_t *lp = cfs_list_entry(r, lnet_peer_t,
                                                         lp_rtr_list);

                        if (skip == 0) {
                                peer = lp;
                                break;
                        }

                        skip--;
                        r = r->next;
                }

                if (peer != NULL) {
                        lnet_nid_t nid = peer->lp_nid;
                        cfs_time_t now = cfs_time_current();
                        cfs_time_t deadline = peer->lp_ping_deadline;
                        int nrefs     = peer->lp_refcount;
                        int nrtrrefs  = peer->lp_rtr_refcount;
                        int alive_cnt = peer->lp_alive_count;
                        int alive     = peer->lp_alive;
                        int pingsent  = !peer->lp_ping_notsent;
                        int last_ping = cfs_duration_sec(cfs_time_sub(now,
                                                     peer->lp_ping_timestamp));
                        int down_ni   = lnet_router_down_ni(peer,
                                                    LNET_NIDNET(LNET_NID_ANY));

                        if (deadline == 0)
                                s += snprintf(s, tmpstr + tmpsiz - s,
                                              "%-4d %7d %9d %6s %12d %9d %8s %7d %s\n",
                                              nrefs, nrtrrefs, alive_cnt,
                                              alive ? "up" : "down", last_ping,
                                              pingsent, "NA", down_ni,
                                              libcfs_nid2str(nid));
                        else
                                s += snprintf(s, tmpstr + tmpsiz - s,
                                              "%-4d %7d %9d %6s %12d %9d %8lu %7d %s\n",
                                              nrefs, nrtrrefs, alive_cnt,
                                              alive ? "up" : "down", last_ping,
                                              pingsent,
                                              cfs_duration_sec(cfs_time_sub(deadline, now)),
                                              down_ni, libcfs_nid2str(nid));
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

                LNET_NET_UNLOCK_DEFAULT();
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else {
                        num += 1;
                        *ppos = LNET_PHASH_POS_MAKE(0, ver, 0, num);
                }
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

int LL_PROC_PROTO(proc_lnet_peers)
{
        lnet_net_cpud_t *netcd;
        int        rc = 0;
        char      *tmpstr;
        char      *s;
        const int  tmpsiz      = 256;
        int        len;
        int        ver;
        int        idx;
        int        num;
        int        cpuid;

        DECLARE_LL_PROC_PPOS_DECL;

        cpuid = LNET_CPU_IDX_GET(*ppos);
        ver = LNET_VERSION_GET(*ppos);
        idx = LNET_PHASH_IDX_GET(*ppos);
        num = LNET_PHASH_NUM_GET(*ppos);

        CLASSERT ((1ULL << LNET_PHASH_TBL_BITS) >= LNET_PEER_HASH_SIZE);

        LASSERT (!write);

        if (*lenp == 0)
                return 0;

        if (cpuid >= LNET_CONCURRENCY)
                return 0;

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        if (*ppos == 0) {
                s += snprintf(s, tmpstr + tmpsiz - s,
                              "%-24s %4s %5s %5s %5s %5s %5s %5s %5s %s\n",
                              "nid", "refs", "state", "last", "max",
                              "rtr", "min", "tx", "min", "queue");
                LASSERT (tmpstr + tmpsiz - s > 0);
                num++;
        } else {
                cfs_list_t        *p    = NULL;
                lnet_peer_t       *peer = NULL;
                int                skip = num - 1;

        again:
                netcd = lnet_netcd_from_cpuid(cpuid);
                LNET_NET_LOCK(netcd);

                if (idx == 0 && num == 1) {
                        ver = LNET_PHASH_VER_VALID(
                              netcd->lnc_peertable_version);
                }

                if (ver != LNET_PHASH_VER_VALID(netcd->lnc_peertable_version)) {
                        LNET_NET_UNLOCK(netcd);
                        LIBCFS_FREE(tmpstr, tmpsiz);
                        return -ESTALE;
                }

                while (idx < LNET_PEER_HASH_SIZE) {
                        if (p == NULL)
                                p = netcd->lnc_peer_hash[idx].next;

                        while (p != &netcd->lnc_peer_hash[idx]) {
                                lnet_peer_t *lp = cfs_list_entry(p, lnet_peer_t,
                                                                 lp_hashlist);
                                if (skip == 0) {
                                        peer = lp;

                                        /* minor optimization: start from idx+1
                                         * on next iteration if we've just
                                         * drained lp_hashlist */
                                        if (lp->lp_hashlist.next ==
                                            &netcd->lnc_peer_hash[idx]) {
                                                num = 1;
                                                idx++;
                                        } else {
                                                num++;
                                        }

                                        break;
                                }

                                skip--;
                                p = lp->lp_hashlist.next;
                        }

                        if (peer != NULL)
                                break;

                        p = NULL;
                        num = 1;
                        idx++;
                }

                if (peer != NULL) {
                        lnet_nid_t nid       = peer->lp_nid;
                        int        nrefs     = peer->lp_refcount;
                        int        lastalive = -1;
                        char      *aliveness = "NA";
                        int        maxcr     = peer->lp_ni->ni_peertxcredits;
                        int        txcr      = peer->lp_txcredits;
                        int        mintxcr   = peer->lp_mintxcredits;
                        int        rtrcr     = peer->lp_rtrcredits;
                        int        minrtrcr  = peer->lp_minrtrcredits;
                        int        txqnob    = peer->lp_txqnob;

                        if (lnet_isrouter(peer) ||
                            lnet_peer_aliveness_enabled(peer))
                                aliveness = peer->lp_alive ? "up" : "down";
                        LNET_NET_UNLOCK(netcd);

                        if (lnet_peer_aliveness_enabled(peer)) {
                                cfs_time_t     now = cfs_time_current();
                                cfs_duration_t delta;

                                delta = cfs_time_sub(now, peer->lp_last_alive);
                                lastalive = cfs_duration_sec(delta);

                                /* No need to mess up peers contents with
                                 * arbitrarily long integers - it suffices to
                                 * know that lastalive is more than 10000s old
                                 */
                                if (lastalive >= 10000)
                                        lastalive = 9999;
                        }

                        s += snprintf(s, tmpstr + tmpsiz - s,
                                      "%-24s %4d %5s %5d %5d %5d %5d %5d %5d %d\n",
                                      libcfs_nid2str(nid), nrefs, aliveness,
                                      lastalive, maxcr, rtrcr, minrtrcr, txcr,
                                      mintxcr, txqnob);
                        LASSERT (tmpstr + tmpsiz - s > 0);

                } else { /* peer is NULL */
                        LNET_NET_UNLOCK(netcd);

                        if (idx == LNET_PEER_HASH_SIZE &&
                            cpuid < min((unsigned)LNET_CPU_IDX_BITMASK,
                                        LNET_CONCURRENCY) - 1) {
                                cpuid++;
                                idx = 0;
                                num = 1;
                                goto again;
                        }
                }
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else
                        *ppos = LNET_PHASH_POS_MAKE(cpuid, ver, idx, num);
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

static int __proc_lnet_buffers(void *data, int write,
                               loff_t pos, void *buffer, int nob)
{
        int              i;
        int              rc;
        int              len;
        int              idx;
        char            *s;
        char            *tmpstr;
        lnet_net_cpud_t *netcd;
        const int        tmpsiz = LNET_CONCURRENCY * 64 *
                                  (LNET_NRBPOOLS + 1); /* (4 %d) * 4 */

        LASSERT (!write);

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        s += snprintf(s, tmpstr + tmpsiz - s,
                      "%5s %5s %7s %7s\n",
                      "pages", "count", "credits", "min");
        LASSERT (tmpstr + tmpsiz - s > 0);

        lnet_netcd_for_each(i) {
                netcd = lnet_netcd_from_cpuid(i);

                LNET_NET_LOCK(netcd);
                for (idx = 0; idx < LNET_NRBPOOLS; idx++) {
                        lnet_rtrbufpool_t *rbp = &netcd->lnc_rtrpools[idx];

                        int npages = rbp->rbp_npages;
                        int nbuf   = rbp->rbp_nbuffers;
                        int cr     = rbp->rbp_credits;
                        int mincr  = rbp->rbp_mincredits;

                        s += snprintf(s, tmpstr + tmpsiz - s,
                                      "%5d %5d %7d %7d\n",
                                      npages, nbuf, cr, mincr);
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

                LNET_NET_UNLOCK(netcd);
        }

        len = s - tmpstr;

        if (pos >= min_t(int, len, strlen(tmpstr)))
                rc = 0;
        else
                rc = cfs_trace_copyout_string(buffer, nob,
                                              tmpstr + pos, NULL);

        LIBCFS_FREE(tmpstr, tmpsiz);
        return rc;
}

DECLARE_PROC_HANDLER(proc_lnet_buffers);

int LL_PROC_PROTO(proc_lnet_nis)
{
        int        rc = 0;
        char      *tmpstr;
        char      *s;
        const int  tmpsiz = 256;
        int        len;

        DECLARE_LL_PROC_PPOS_DECL;

        LASSERT (!write);

        if (*lenp == 0)
                return 0;

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        if (*ppos == 0) {
                s += snprintf(s, tmpstr + tmpsiz - s,
                              "%-24s %6s %5s %4s %4s %4s %5s %5s %5s\n",
                              "nid", "status", "alive", "refs", "peer",
                              "rtr", "max", "tx", "min");
                LASSERT (tmpstr + tmpsiz - s > 0);
        } else {
                lnet_ni_t         *ni = NULL;
                lnet_ni_cpud_t    *nc;
                cfs_list_t        *n;
                int                skip = *ppos - 1;

                LNET_NET_LOCK_DEFAULT();

                n = the_lnet.ln_nis.next;

                while (n != &the_lnet.ln_nis) {
                        lnet_ni_t *a_ni = cfs_list_entry(n, lnet_ni_t, ni_list);

                        if (skip == 0) {
                                ni = a_ni;
                                break;
                        }

                        skip--;
                        n = n->next;
                }

                if (ni != NULL) {
                        cfs_time_t now = cfs_time_current_sec();
                        int        last_alive = -1;
                        int        maxtxcr = ni->ni_maxtxcredits;
                        int        txcr = ni->ni_txcredits;
                        int        mintxcr = ni->ni_mintxcredits;
                        int        npeertxcr = ni->ni_peertxcredits;
                        int        npeerrtrcr = ni->ni_peerrtrcredits;
                        lnet_nid_t nid = ni->ni_nid;
                        int        nref = 0;
                        int        i;
                        char      *stat;

                        if (the_lnet.ln_routing)
                                last_alive = cfs_time_sub(now, ni->ni_last_alive);
                        /* @lo forever alive */
                        if (ni->ni_lnd->lnd_type == LOLND)
                                last_alive = 0;

                        LASSERT (ni->ni_status != NULL);
                        stat = (ni->ni_status->ns_status == LNET_NI_STATUS_UP) ?
                                                                  "up" : "down";

                        /* not atomic, but OK */
                        cfs_percpu_for_each(nc, i, ni->ni_cpuds)
                                nref += nc->nc_refcount;

                        s += snprintf(s, tmpstr + tmpsiz - s,
                                      "%-24s %6s %5d %4d %4d %4d %5d %5d %5d\n",
                                      libcfs_nid2str(nid), stat, last_alive,
                                      nref, npeertxcr, npeerrtrcr, maxtxcr,
                                      txcr, mintxcr);
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

                LNET_NET_UNLOCK_DEFAULT();
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else
                        *ppos += 1;
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

static cfs_sysctl_table_t lnet_table[] = {
        /*
         * NB No .strategy entries have been provided since sysctl(8) prefers
         * to go via /proc for portability.
         */
        {
                .ctl_name = PSDEV_LNET_STATS,
                .procname = "stats",
                .mode     = 0644,
                .proc_handler = &proc_lnet_stats,
        },
        {
                .ctl_name = PSDEV_LNET_ROUTES,
                .procname = "routes",
                .mode     = 0444,
                .proc_handler = &proc_lnet_routes,
        },
        {
                .ctl_name = PSDEV_LNET_ROUTERS,
                .procname = "routers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_routers,
        },
        {
                .ctl_name = PSDEV_LNET_PEERS,
                .procname = "peers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_peers,
        },
        {
                .ctl_name = PSDEV_LNET_PEERS,
                .procname = "buffers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_buffers,
        },
        {
                .ctl_name = PSDEV_LNET_NIS,
                .procname = "nis",
                .mode     = 0444,
                .proc_handler = &proc_lnet_nis,
        },
        {0}
};

static cfs_sysctl_table_t top_table[] = {
        {
                .ctl_name = CTL_LNET,
                .procname = "lnet",
                .mode     = 0555,
                .data     = NULL,
                .maxlen   = 0,
                .child    = lnet_table,
        },
        {
                .ctl_name = 0
        }
};

void
lnet_proc_init(void)
{
#ifdef CONFIG_SYSCTL
        if (lnet_table_header == NULL)
                lnet_table_header = cfs_register_sysctl_table(top_table, 0);
#endif
}

void
lnet_proc_fini(void)
{
#ifdef CONFIG_SYSCTL
        if (lnet_table_header != NULL)
                cfs_unregister_sysctl_table(lnet_table_header);

        lnet_table_header = NULL;
#endif
}

#else

void
lnet_proc_init(void)
{
}

void
lnet_proc_fini(void)
{
}

#endif
