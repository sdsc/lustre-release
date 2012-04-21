/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 * Copyright (c) 2011, Whamcloud, Inc.
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

extern unsigned int lnet_redir_put;

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
	PSDEV_LNET_REDIR_PUT,
};
#else
#define CTL_LNET           CTL_UNNUMBERED
#define PSDEV_LNET_STATS   CTL_UNNUMBERED
#define PSDEV_LNET_ROUTES  CTL_UNNUMBERED
#define PSDEV_LNET_ROUTERS CTL_UNNUMBERED
#define PSDEV_LNET_PEERS   CTL_UNNUMBERED
#define PSDEV_LNET_BUFFERS CTL_UNNUMBERED
#define PSDEV_LNET_NIS     CTL_UNNUMBERED
#define PSDEV_LNET_REDIR_PUT	CTL_UNNUMBERED
#endif

#define LNET_LOFFT_BITS        (sizeof(loff_t) * 8)

/*
 * NB: max allowed LNET_CPT_BITS is 8 on 64-bit system and 2 on 32-bit system
 */
#define LNET_PROC_CPT_BITS      LNET_CPT_BITS
/* change version, 16 bits or 8 bits (unlikely) */
#define LNET_PROC_VER_BITS      MAX(((MIN(LNET_LOFFT_BITS, 64)) / 4), 8)

#define LNET_PROC_HASH_BITS     LNET_PEER_HASH_BITS
/*
 * bits for peer hash offset
 * NB: we don't use the highest bit of *ppos because it's signed
 */
#define LNET_PROC_HOFF_BITS    (LNET_LOFFT_BITS -       \
				LNET_PROC_CPT_BITS -    \
				LNET_PROC_VER_BITS -    \
				LNET_PROC_HASH_BITS - 1)
/* bits for hash index + position */
#define LNET_PROC_HPOS_BITS    (LNET_PROC_HASH_BITS + LNET_PROC_HOFF_BITS)
/* bits for peer hash table + hash version */
#define LNET_PROC_VPOS_BITS    (LNET_PROC_HPOS_BITS + LNET_PROC_VER_BITS)

#define LNET_PROC_CPT_MASK    ((1ULL << LNET_PROC_CPT_BITS) - 1)
#define LNET_PROC_VER_MASK    ((1ULL << LNET_PROC_VER_BITS) - 1)
#define LNET_PROC_HASH_MASK   ((1ULL << LNET_PROC_HASH_BITS) - 1)
#define LNET_PROC_HOFF_MASK   ((1ULL << LNET_PROC_HOFF_BITS) - 1)

#define LNET_PROC_CPT_GET(pos)                          \
	(int)(((pos) >> LNET_PROC_VPOS_BITS) & LNET_PROC_CPT_MASK)

#define LNET_PROC_VER_GET(pos)                          \
	(int)(((pos) >> LNET_PROC_HPOS_BITS) & LNET_PROC_VER_MASK)

#define LNET_PROC_HASH_GET(pos)                         \
	(int)(((pos) >> LNET_PROC_HOFF_BITS) & LNET_PROC_HASH_MASK)

#define LNET_PROC_HOFF_GET(pos)                          \
	(int)((pos) & LNET_PROC_HOFF_MASK)

#define LNET_PROC_POS_MAKE(cpt, ver, hash, off)                              \
	(((((loff_t)(cpt)) & LNET_PROC_CPT_MASK) << LNET_PROC_VPOS_BITS) |   \
	 ((((loff_t)(ver)) & LNET_PROC_VER_MASK) << LNET_PROC_HPOS_BITS) |   \
	 ((((loff_t)(hash)) & LNET_PROC_HASH_MASK) << LNET_PROC_HOFF_BITS) | \
	 ((off) & LNET_PROC_HOFF_MASK))

#define LNET_PROC_VERSION(v) (unsigned int)((v) & LNET_PROC_VER_MASK)

static int __proc_lnet_stats(void *data, int write,
                             loff_t pos, void *buffer, int nob)
{
        int              rc;
        lnet_counters_t *ctrs;
        int              len;
        char            *tmpstr;
        const int        tmpsiz = 256; /* 7 %u and 4 LPU64 */

        if (write) {
		lnet_counters_reset();
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

	lnet_counters_get(ctrs);

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
	int        off;

        DECLARE_LL_PROC_PPOS_DECL;

	CLASSERT(sizeof(loff_t) >= 4);

	off = LNET_PROC_HOFF_GET(*ppos);
	ver = LNET_PROC_VER_GET(*ppos);

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

		lnet_net_lock(0);
                ver = (unsigned int)the_lnet.ln_remote_nets_version;
		lnet_net_unlock(0);
		*ppos = LNET_PROC_POS_MAKE(0, ver, 0, off);
        } else {
                cfs_list_t        *n;
                cfs_list_t        *r;
                lnet_route_t      *route = NULL;
                lnet_remotenet_t  *rnet  = NULL;
		int                skip  = off - 1;

		lnet_net_lock(0);

		if (ver != LNET_PROC_VERSION(the_lnet.ln_remote_nets_version)) {
			lnet_net_unlock(0);
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

		lnet_net_unlock(0);
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else {
			off += 1;
			*ppos = LNET_PROC_POS_MAKE(0, ver, 0, off);
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
	int        off;

        DECLARE_LL_PROC_PPOS_DECL;

	off = LNET_PROC_HOFF_GET(*ppos);
	ver = LNET_PROC_VER_GET(*ppos);

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

		lnet_net_lock(0);
                ver = (unsigned int)the_lnet.ln_routers_version;
		lnet_net_unlock(0);
		*ppos = LNET_PROC_POS_MAKE(0, ver, 0, off);
        } else {
                cfs_list_t        *r;
                lnet_peer_t       *peer = NULL;
		int                skip = off - 1;

		lnet_net_lock(0);

		if (ver != LNET_PROC_VERSION(the_lnet.ln_routers_version)) {
			lnet_net_unlock(0);
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
			int down_nis  = 0;
			lnet_route_t *rtr;

			if (peer->lp_ping_version == LNET_PROTO_PING_VERSION) {
				cfs_list_for_each_entry(rtr, &peer->lp_routes,
							lr_gwlist)
					down_nis += rtr->lr_downis;
			}

                        if (deadline == 0)
                                s += snprintf(s, tmpstr + tmpsiz - s,
                                              "%-4d %7d %9d %6s %12d %9d %8s %7d %s\n",
                                              nrefs, nrtrrefs, alive_cnt,
                                              alive ? "up" : "down", last_ping,
					      pingsent, "NA", down_nis,
                                              libcfs_nid2str(nid));
                        else
                                s += snprintf(s, tmpstr + tmpsiz - s,
                                              "%-4d %7d %9d %6s %12d %9d %8lu %7d %s\n",
                                              nrefs, nrtrrefs, alive_cnt,
                                              alive ? "up" : "down", last_ping,
                                              pingsent,
                                              cfs_duration_sec(cfs_time_sub(deadline, now)),
					      down_nis, libcfs_nid2str(nid));
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

		lnet_net_unlock(0);
        }

        len = s - tmpstr;     /* how many bytes was written */

        if (len > *lenp) {    /* linux-supplied buffer is too small */
                rc = -EINVAL;
        } else if (len > 0) { /* wrote something */
                if (cfs_copy_to_user(buffer, tmpstr, len))
                        rc = -EFAULT;
                else {
			off += 1;
			*ppos = LNET_PROC_POS_MAKE(0, ver, 0, off);
                }
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

int LL_PROC_PROTO(proc_lnet_peers)
{
        int        rc = 0;
        char      *tmpstr;
        char      *s;
        const int  tmpsiz      = 256;
        int        len;
        int        ver;
	int        hash;
	int        hoff;
	int        cpt;

        DECLARE_LL_PROC_PPOS_DECL;

	cpt = LNET_PROC_CPT_GET(*ppos);
	ver = LNET_PROC_VER_GET(*ppos);
	hash = LNET_PROC_HASH_GET(*ppos);
	hoff = LNET_PROC_HOFF_GET(*ppos);

	CLASSERT((1ULL << LNET_PROC_HASH_BITS) >= LNET_PEER_HASH_BITS);

        LASSERT (!write);

        if (*lenp == 0)
                return 0;

	if (cpt >= LNET_CPT_NUMBER)
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
		hoff++;
        } else {
		struct lnet_peer_table *ptab;
                cfs_list_t        *p    = NULL;
                lnet_peer_t       *peer = NULL;
		int                skip = hoff - 1;

	again:
		lnet_net_lock(cpt);

		ptab = the_lnet.ln_peer_tables[cpt];

		if (hoff == 1)
			ver = LNET_PROC_VERSION(ptab->pt_version);

		if (ver != LNET_PROC_VERSION(ptab->pt_version)) {
			lnet_net_unlock(cpt);
                        LIBCFS_FREE(tmpstr, tmpsiz);
                        return -ESTALE;
                }

		while (hash < LNET_PEER_HASH_SIZE) {
                        if (p == NULL)
				p = ptab->pt_hash[hash].next;

			while (p != &ptab->pt_hash[hash]) {
                                lnet_peer_t *lp = cfs_list_entry(p, lnet_peer_t,
                                                                 lp_hashlist);
                                if (skip == 0) {
                                        peer = lp;

					/* minor optimization: start from hash+1
                                         * on next iteration if we've just
                                         * drained lp_hashlist */
                                        if (lp->lp_hashlist.next ==
					    &ptab->pt_hash[hash]) {
						hoff = 1;
						hash++;
                                        } else {
						hoff++;
                                        }

                                        break;
                                }

                                skip--;
                                p = lp->lp_hashlist.next;
                        }

                        if (peer != NULL)
                                break;

                        p = NULL;
			hoff = 1;
			hash++;
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
			lnet_net_unlock(cpt);

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
			lnet_net_unlock(cpt);

			if (hash == LNET_PEER_HASH_SIZE &&
			    cpt < LNET_CPT_NUMBER - 1) {
				cpt++;
				hash = 0;
				hoff = 1;
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
			*ppos = LNET_PROC_POS_MAKE(cpt, ver, hash, hoff);
        }

        LIBCFS_FREE(tmpstr, tmpsiz);

        if (rc == 0)
                *lenp = len;

        return rc;
}

static int __proc_lnet_buffers(void *data, int write,
                               loff_t pos, void *buffer, int nob)
{
	lnet_rtrbufpool_t *rbpp;
	int              i;
        int              rc;
        int              len;
	int              idx;
        char            *s;
        char            *tmpstr;
        const int        tmpsiz = 64 * (LNET_NRBPOOLS + 1); /* (4 %d) * 4 */

        LASSERT (!write);

        LIBCFS_ALLOC(tmpstr, tmpsiz);
        if (tmpstr == NULL)
                return -ENOMEM;

        s = tmpstr; /* points to current position in tmpstr[] */

        s += snprintf(s, tmpstr + tmpsiz - s,
                      "%5s %5s %7s %7s\n",
		      "pages", "count", "credits", "min");
        LASSERT (tmpstr + tmpsiz - s > 0);

	if (the_lnet.ln_rtrpools == NULL)
		goto out;

        for (idx = 0; idx < LNET_NRBPOOLS; idx++) {
		int npages = the_lnet.ln_rtrpools[0][idx].rbp_npages;
		int nbuf   = the_lnet.ln_rtrpools[0][idx].rbp_nbuffers;
		int cr     = nbuf;
		int mincr  = nbuf;

		lnet_net_lock(LNET_LOCK_EX);

		cfs_percpt_for_each(rbpp, i, the_lnet.ln_rtrpools) {
			lnet_rtrbufpool_t *rbp = &rbpp[idx];

			cr = min(cr, rbp->rbp_credits);
			mincr = min(cr, rbp->rbp_mincredits);
		}

		lnet_net_unlock(LNET_LOCK_EX);

                s += snprintf(s, tmpstr + tmpsiz - s,
			      "%5d %5d %7d %7d\n", npages, nbuf, cr, mincr);
                LASSERT (tmpstr + tmpsiz - s > 0);
        }

 out:
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

static int __proc_lnet_buffers_details(void *data, int write,
				       loff_t pos, void *buffer, int nob)
{
	lnet_rtrbufpool_t *rbpp;
	int              i;
	int              rc;
	int              len;
	int              idx;
	char            *s;
	char            *tmpstr;
	const int        tmpsiz = LNET_CPT_NUMBER * 64 *
				  (LNET_NRBPOOLS + 1); /* (4 %d) * 4 */

	LASSERT(!write);

	LIBCFS_ALLOC(tmpstr, tmpsiz);
	if (tmpstr == NULL)
		return -ENOMEM;

	s = tmpstr; /* points to current position in tmpstr[] */

	s += snprintf(s, tmpstr + tmpsiz - s,
		      "%3s %5s %5s %7s %7s\n",
		      "cpt", "pages", "count", "credits", "min");
	LASSERT(tmpstr + tmpsiz - s > 0);

	if (the_lnet.ln_rtrpools == NULL)
		goto out;

	cfs_percpt_for_each(rbpp, i, the_lnet.ln_rtrpools) {
		lnet_net_lock(i);
		for (idx = 0; idx < LNET_NRBPOOLS; idx++) {
			lnet_rtrbufpool_t *rbp = &rbpp[idx];

			int npages = rbp->rbp_npages;
			int nbuf   = rbp->rbp_nbuffers;
			int cr     = rbp->rbp_credits;
			int mincr  = rbp->rbp_mincredits;

			s += snprintf(s, tmpstr + tmpsiz - s,
				      "%3d %5d %5d %7d %7d\n",
				      i, npages, nbuf, cr, mincr);
			LASSERT(tmpstr + tmpsiz - s > 0);
		}
		lnet_net_unlock(i);
	}
 out:
	len = s - tmpstr;

	if (pos >= min_t(int, len, strlen(tmpstr)))
		rc = 0;
	else
		rc = cfs_trace_copyout_string(buffer, nob,
					      tmpstr + pos, NULL);

	LIBCFS_FREE(tmpstr, tmpsiz);
	return rc;
}

DECLARE_PROC_HANDLER(proc_lnet_buffers_details);


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
                cfs_list_t        *n;
                lnet_ni_t         *ni   = NULL;
                int                skip = *ppos - 1;

		lnet_net_lock(0);

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
			struct lnet_tx_queue *tq;
			cfs_time_t now = cfs_time_current_sec();
                        int        last_alive = -1;
			int        maxtxcr = ni->ni_credits_cpt;
                        int        npeertxcr = ni->ni_peertxcredits;
                        int        npeerrtrcr = ni->ni_peerrtrcredits;
                        lnet_nid_t nid = ni->ni_nid;
			int        mintxcr = maxtxcr;
			int        txcr = 0;
			int        refs = 0;
			int        i;
			int	   j;
			int       *pref;
                        char      *stat;

                        if (the_lnet.ln_routing) {
				last_alive = cfs_time_sub(now,
							  ni->ni_last_alive);
			}
			/* @lo forever alive */
			if (ni->ni_lnd->lnd_type == LOLND)
                                last_alive = 0;

                        LASSERT (ni->ni_status != NULL);
                        stat = (ni->ni_status->ns_status == LNET_NI_STATUS_UP) ?
                                                                  "up" : "down";

			/* not atomic, but OK */
			cfs_percpt_for_each(pref, i, ni->ni_refs)
				refs += *pref;

			/* NB: only minimum credit is reported so far,
			 * we probably should report credits info on
			 * all cpu-partitions */
			cfs_percpt_for_each(tq, i, ni->ni_tx_queues) {
				for (j = 0; ni->ni_cpts != NULL &&
					    j < ni->ni_ncpts; j++) {
					if (i == ni->ni_cpts[j])
						break;
				}

				if (ni->ni_ncpts == j)
					continue;

				txcr = min(txcr, tq->tq_credits);
				mintxcr = min(mintxcr,  tq->tq_credits_min);
			}

                        s += snprintf(s, tmpstr + tmpsiz - s,
                                      "%-24s %6s %5d %4d %4d %4d %5d %5d %5d\n",
				      libcfs_nid2str(nid), stat, last_alive,
				      refs, npeertxcr, npeerrtrcr, maxtxcr,
                                      txcr, mintxcr);
                        LASSERT (tmpstr + tmpsiz - s > 0);
                }

		lnet_net_unlock(0);
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

#define LNET_REDIR_STR_LEN	16

struct lnet_redir_put_table {
	int		re_value;
	const char	*re_name;
};

static struct lnet_redir_put_table redir_put_table[] = {
	{
		.re_value = LNET_REDIR_OFF,
		.re_name  = "OFF",
	},
	{
		.re_value = LNET_REDIR_ON,
		.re_name  = "ON",
	},
	{
		.re_value = LNET_REDIR_AUTO,
		.re_name  = "AUTO",
	},
	{
		.re_value = LNET_REDIR_ROUTED,
		.re_name  = "ROUTED",
	},
	{
		.re_value = -1,
		.re_name  = NULL,
	},
};

static int __proc_lnet_redir_put(void *data, int write,
				 loff_t pos, void *buffer, int nob)
{
	char	str[LNET_REDIR_STR_LEN];
	char	*tmp;
	int	rc;
	int	i;

	if (!write) {
		lnet_res_lock(0);

		for (i = 0; redir_put_table[i].re_value >= 0; i++) {
			if (redir_put_table[i].re_value == lnet_redir_put)
				break;
		}

		LASSERT(redir_put_table[i].re_value == lnet_redir_put);
		lnet_res_unlock(0);

		rc = snprintf(str, LNET_REDIR_STR_LEN, "%s",
			      redir_put_table[i].re_name);

		if (pos >= min_t(int, rc, strlen(str))) {
			rc = 0;
		} else {
			rc = cfs_trace_copyout_string(buffer, nob,
						      str + pos, "\n");
		}

		return rc;
	}

	rc = cfs_trace_copyin_string(str, LNET_REDIR_STR_LEN, buffer, nob);
	if (rc < 0)
		return rc;

	tmp = cfs_trimwhite(str);

	lnet_res_lock(0);
	for (i = 0; redir_put_table[i].re_name != NULL; i++) {
		if (cfs_strncasecmp(redir_put_table[i].re_name, tmp,
				    strlen(redir_put_table[i].re_name) == 0)) {
			lnet_redir_put = redir_put_table[i].re_value;
			lnet_res_unlock(0);
			return 0;
		}
	}

	lnet_res_unlock(0);
	return -EINVAL;
}

DECLARE_PROC_HANDLER(proc_lnet_redir_put);

static cfs_sysctl_table_t lnet_table[] = {
        /*
         * NB No .strategy entries have been provided since sysctl(8) prefers
         * to go via /proc for portability.
         */
        {
                INIT_CTL_NAME(PSDEV_LNET_STATS)
                .procname = "stats",
                .mode     = 0644,
                .proc_handler = &proc_lnet_stats,
        },
        {
                INIT_CTL_NAME(PSDEV_LNET_ROUTES)
                .procname = "routes",
                .mode     = 0444,
                .proc_handler = &proc_lnet_routes,
        },
        {
                INIT_CTL_NAME(PSDEV_LNET_ROUTERS)
                .procname = "routers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_routers,
        },
        {
                INIT_CTL_NAME(PSDEV_LNET_PEERS)
                .procname = "peers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_peers,
        },
        {
                INIT_CTL_NAME(PSDEV_LNET_PEERS)
                .procname = "buffers",
                .mode     = 0444,
                .proc_handler = &proc_lnet_buffers,
        },
        {
		INIT_CTL_NAME(PSDEV_LNET_PEERS)
		.procname = "buffers_details",
		.mode     = 0444,
		.proc_handler = &proc_lnet_buffers_details,
	},
	{
                INIT_CTL_NAME(PSDEV_LNET_NIS)
                .procname = "nis",
                .mode     = 0444,
                .proc_handler = &proc_lnet_nis,
        },
        {
		INIT_CTL_NAME(PSDEV_LNET_REDIR_PUT)
		.procname = "redirect_put",
		.mode     = 0644,
		.proc_handler = &proc_lnet_redir_put,
	},
	{
                INIT_CTL_NAME(0)
        }
};

static cfs_sysctl_table_t top_table[] = {
        {
                INIT_CTL_NAME(CTL_LNET)
                .procname = "lnet",
                .mode     = 0555,
                .data     = NULL,
                .maxlen   = 0,
                .child    = lnet_table,
        },
        {
                INIT_CTL_NAME(0)
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
