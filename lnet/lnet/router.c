/*
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
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
#include <lnet/lib-lnet.h>

#if defined(__KERNEL__) && defined(LNET_ROUTER)

#define LNET_NRB_TINY_MIN	512	/* min value for each CPT */
#define LNET_NRB_TINY		(LNET_NRB_TINY_MIN * 4)
#define LNET_NRB_SMALL_MIN	4096	/* min value for each CPT */
#define LNET_NRB_SMALL		(LNET_NRB_SMALL_MIN * 4)
#define LNET_NRB_SMALL_PAGES	1
#define LNET_NRB_LARGE_MIN	256	/* min value for each CPT */
#define LNET_NRB_LARGE		(LNET_NRB_LARGE_MIN * 4)
#define LNET_NRB_LARGE_PAGES	((LNET_MTU + PAGE_CACHE_SIZE - 1) \
				 >> PAGE_CACHE_SHIFT)

static char *forwarding = "";
CFS_MODULE_PARM(forwarding, "s", charp, 0444,
                "Explicitly enable/disable forwarding between networks");

static int tiny_router_buffers_hold;
static int tiny_router_buffers;
CFS_MODULE_PARM(tiny_router_buffers, "i", int, 0444,
		"# of 0 payload messages to buffer in the router");
static int small_router_buffers_hold;
static int small_router_buffers;
CFS_MODULE_PARM(small_router_buffers, "i", int, 0444,
		"# of small (1 page) messages to buffer in the router");
static int large_router_buffers_hold;
static int large_router_buffers;
CFS_MODULE_PARM(large_router_buffers, "i", int, 0444,
		"# of large messages to buffer in the router");
static int peer_buffer_credits = 0;
CFS_MODULE_PARM(peer_buffer_credits, "i", int, 0444,
                "# router buffer credits per peer");

static int auto_down = 1;
CFS_MODULE_PARM(auto_down, "i", int, 0444,
                "Automatically mark peers down on comms error");

int
lnet_peer_buffer_credits(lnet_ni_t *ni)
{
        /* NI option overrides LNet default */
        if (ni->ni_peerrtrcredits > 0)
                return ni->ni_peerrtrcredits;
        if (peer_buffer_credits > 0)
                return peer_buffer_credits;

        /* As an approximation, allow this peer the same number of router
         * buffers as it is allowed outstanding sends */
        return ni->ni_peertxcredits;
}

/* forward ref's */
static int lnet_router_checker(void *);
#else

int
lnet_peer_buffer_credits(lnet_ni_t *ni)
{
        return 0;
}

#endif

static int check_routers_before_use = 0;
CFS_MODULE_PARM(check_routers_before_use, "i", int, 0444,
		"Assume routers are down and ping them before use");

static int avoid_asym_router_failure = 1;
CFS_MODULE_PARM(avoid_asym_router_failure, "i", int, 0644,
		"Avoid asymmetrical router failures (0 to disable)");

static int dead_router_check_interval = 60;
CFS_MODULE_PARM(dead_router_check_interval, "i", int, 0644,
		"Seconds between dead router health checks (<= 0 to disable)");

static int live_router_check_interval = 60;
CFS_MODULE_PARM(live_router_check_interval, "i", int, 0644,
		"Seconds between live router health checks (<= 0 to disable)");

static int router_ping_timeout = 50;
CFS_MODULE_PARM(router_ping_timeout, "i", int, 0644,
		"Seconds to wait for the reply to a router health query");

int
lnet_peers_start_down(void)
{
        return check_routers_before_use;
}

void
lnet_notify_locked(lnet_peer_t *lp, int notifylnd, int alive, cfs_time_t when)
{
        if (cfs_time_before(when, lp->lp_timestamp)) { /* out of date information */
                CDEBUG(D_NET, "Out of date\n");
                return;
        }

        lp->lp_timestamp = when;                /* update timestamp */
        lp->lp_ping_deadline = 0;               /* disable ping timeout */

        if (lp->lp_alive_count != 0 &&          /* got old news */
            (!lp->lp_alive) == (!alive)) {      /* new date for old news */
                CDEBUG(D_NET, "Old news\n");
                return;
        }

        /* Flag that notification is outstanding */

        lp->lp_alive_count++;
        lp->lp_alive = !(!alive);               /* 1 bit! */
        lp->lp_notify = 1;
        lp->lp_notifylnd |= notifylnd;
	if (lp->lp_alive)
		lp->lp_ping_feats = LNET_PING_FEAT_INVAL; /* reset */

	CDEBUG(D_NET, "set %s %d\n", libcfs_nid2str(lp->lp_nid), alive);
}

void
lnet_ni_notify_locked(lnet_ni_t *ni, lnet_peer_t *lp)
{
        int        alive;
        int        notifylnd;

        /* Notify only in 1 thread at any time to ensure ordered notification.
         * NB individual events can be missed; the only guarantee is that you
         * always get the most recent news */

	if (lp->lp_notifying)
                return;

        lp->lp_notifying = 1;

        while (lp->lp_notify) {
                alive     = lp->lp_alive;
                notifylnd = lp->lp_notifylnd;

                lp->lp_notifylnd = 0;
                lp->lp_notify    = 0;

                if (notifylnd && ni->ni_lnd->lnd_notify != NULL) {
			lnet_net_unlock(lp->lp_cpt);

			/* A new notification could happen now; I'll handle it
			 * when control returns to me */

			(ni->ni_lnd->lnd_notify)(ni, lp->lp_nid, alive);

			lnet_net_lock(lp->lp_cpt);
		}
	}

	lp->lp_notifying = 0;
}


static void
lnet_rtr_addref_locked(lnet_peer_t *lp)
{
	LASSERT(lp->lp_refcount > 0);
	LASSERT(lp->lp_rtr_refcount >= 0);

	/* lnet_net_lock must be exclusively locked */
        lp->lp_rtr_refcount++;
        if (lp->lp_rtr_refcount == 1) {
                cfs_list_t *pos;

                /* a simple insertion sort */
                cfs_list_for_each_prev(pos, &the_lnet.ln_routers) {
                        lnet_peer_t *rtr = cfs_list_entry(pos, lnet_peer_t,
                                                          lp_rtr_list);

                        if (rtr->lp_nid < lp->lp_nid)
                                break;
                }

                cfs_list_add(&lp->lp_rtr_list, pos);
                /* addref for the_lnet.ln_routers */
                lnet_peer_addref_locked(lp);
                the_lnet.ln_routers_version++;
        }
}

static void
lnet_rtr_decref_locked(lnet_peer_t *lp)
{
	LASSERT(lp->lp_refcount > 0);
	LASSERT(lp->lp_rtr_refcount > 0);

	/* lnet_net_lock must be exclusively locked */
	lp->lp_rtr_refcount--;
	if (lp->lp_rtr_refcount == 0) {
		LASSERT(cfs_list_empty(&lp->lp_routes));

		if (lp->lp_rcd != NULL) {
			cfs_list_add(&lp->lp_rcd->rcd_list,
				     &the_lnet.ln_rcd_deathrow);
                        lp->lp_rcd = NULL;
                }

                cfs_list_del(&lp->lp_rtr_list);
                /* decref for the_lnet.ln_routers */
                lnet_peer_decref_locked(lp);
                the_lnet.ln_routers_version++;
        }
}

lnet_remotenet_t *
lnet_find_net_locked (__u32 net)
{
	lnet_remotenet_t	*rnet;
	cfs_list_t		*tmp;
	cfs_list_t		*rn_list;

	LASSERT(!the_lnet.ln_shutdown);

	rn_list = lnet_net2rnethash(net);
	cfs_list_for_each(tmp, rn_list) {
		rnet = cfs_list_entry(tmp, lnet_remotenet_t, lrn_list);

		if (rnet->lrn_net == net)
			return rnet;
	}
	return NULL;
}

static void lnet_shuffle_seed(void)
{
        static int seeded = 0;
        int lnd_type, seed[2];
        struct timeval tv;
        lnet_ni_t *ni;
        cfs_list_t *tmp;

        if (seeded)
                return;

        cfs_get_random_bytes(seed, sizeof(seed));

        /* Nodes with small feet have little entropy
         * the NID for this node gives the most entropy in the low bits */
        cfs_list_for_each(tmp, &the_lnet.ln_nis) {
                ni = cfs_list_entry(tmp, lnet_ni_t, ni_list);
                lnd_type = LNET_NETTYP(LNET_NIDNET(ni->ni_nid));

                if (lnd_type != LOLND)
                        seed[0] ^= (LNET_NIDADDR(ni->ni_nid) | lnd_type);
        }

	do_gettimeofday(&tv);
	cfs_srand(tv.tv_sec ^ seed[0], tv.tv_usec ^ seed[1]);
	seeded = 1;
	return;
}

/* NB expects LNET_LOCK held */
void
lnet_add_route_to_rnet (lnet_remotenet_t *rnet, lnet_route_t *route)
{
        unsigned int      len = 0;
        unsigned int      offset = 0;
        cfs_list_t       *e;

        lnet_shuffle_seed();

        cfs_list_for_each (e, &rnet->lrn_routes) {
                len++;
        }

        /* len+1 positions to add a new entry, also prevents division by 0 */
        offset = cfs_rand() % (len + 1);
        cfs_list_for_each (e, &rnet->lrn_routes) {
                if (offset == 0)
                        break;
                offset--;
        }
        cfs_list_add(&route->lr_list, e);
	cfs_list_add(&route->lr_gwlist, &route->lr_gateway->lp_routes);

	the_lnet.ln_remote_nets_version++;
	lnet_rtr_addref_locked(route->lr_gateway);
}

int
lnet_add_route(__u32 net, unsigned int hops, lnet_nid_t gateway,
	       unsigned int priority)
{
	cfs_list_t          *e;
	lnet_remotenet_t    *rnet;
	lnet_remotenet_t    *rnet2;
	lnet_route_t        *route;
	lnet_ni_t           *ni;
	int                  add_route;
	int                  rc;

	CDEBUG(D_NET, "Add route: net %s hops %u priority %u gw %s\n",
	       libcfs_net2str(net), hops, priority, libcfs_nid2str(gateway));

	if (gateway == LNET_NID_ANY ||
	    LNET_NETTYP(LNET_NIDNET(gateway)) == LOLND ||
	    net == LNET_NIDNET(LNET_NID_ANY) ||
	    LNET_NETTYP(net) == LOLND ||
	    LNET_NIDNET(gateway) == net ||
	    hops < 1 || hops > 255)
		return -EINVAL;

	if (lnet_islocalnet(net))	/* it's a local network */
		return 0;		/* ignore the route entry */

	/* Assume net, route, all new */
	LIBCFS_ALLOC(route, sizeof(*route));
	LIBCFS_ALLOC(rnet, sizeof(*rnet));
	if (route == NULL || rnet == NULL) {
		CERROR("Out of memory creating route %s %d %s\n",
		       libcfs_net2str(net), hops, libcfs_nid2str(gateway));
		if (route != NULL)
			LIBCFS_FREE(route, sizeof(*route));
		if (rnet != NULL)
			LIBCFS_FREE(rnet, sizeof(*rnet));
		return -ENOMEM;
	}

	CFS_INIT_LIST_HEAD(&rnet->lrn_routes);
	rnet->lrn_net = net;
	route->lr_hops = hops;
	route->lr_net = net;
	route->lr_priority = priority;

	lnet_net_lock(LNET_LOCK_EX);

	rc = lnet_nid2peer_locked(&route->lr_gateway, gateway, LNET_LOCK_EX);
	if (rc != 0) {
		lnet_net_unlock(LNET_LOCK_EX);

		LIBCFS_FREE(route, sizeof(*route));
		LIBCFS_FREE(rnet, sizeof(*rnet));

		if (rc == -EHOSTUNREACH) /* gateway is not on a local net. */
			return 0;	 /* ignore the route entry */
		else
			CERROR("Error %d creating route %s %d %s\n", rc,
			       libcfs_net2str(net), hops,
			       libcfs_nid2str(gateway));
		return rc;
	}

	LASSERT(!the_lnet.ln_shutdown);

	rnet2 = lnet_find_net_locked(net);
	if (rnet2 == NULL) {
		/* new network */
		cfs_list_add_tail(&rnet->lrn_list, lnet_net2rnethash(net));
		rnet2 = rnet;
	}

	/* Search for a duplicate route (it's a NOOP if it is) */
	add_route = 1;
	cfs_list_for_each(e, &rnet2->lrn_routes) {
		lnet_route_t *route2 = cfs_list_entry(e, lnet_route_t, lr_list);

		if (route2->lr_gateway == route->lr_gateway) {
			add_route = 0;
			break;
		}

		/* our lookups must be true */
		LASSERT(route2->lr_gateway->lp_nid != gateway);
	}

	if (add_route) {
		lnet_peer_addref_locked(route->lr_gateway); /* +1 for notify */
		lnet_add_route_to_rnet(rnet2, route);

		ni = route->lr_gateway->lp_ni;
		lnet_net_unlock(LNET_LOCK_EX);

		/* XXX Assume alive */
		if (ni->ni_lnd->lnd_notify != NULL)
			(ni->ni_lnd->lnd_notify)(ni, gateway, 1);

		lnet_net_lock(LNET_LOCK_EX);
	}

	/* -1 for notify or !add_route */
	lnet_peer_decref_locked(route->lr_gateway);
	lnet_net_unlock(LNET_LOCK_EX);

	if (!add_route)
		LIBCFS_FREE(route, sizeof(*route));

	if (rnet != rnet2)
		LIBCFS_FREE(rnet, sizeof(*rnet));

	return 0;
}

int
lnet_check_routes(void)
{
	lnet_remotenet_t	*rnet;
	lnet_route_t		*route;
	lnet_route_t		*route2;
	cfs_list_t		*e1;
	cfs_list_t		*e2;
	int			cpt;
	cfs_list_t		*rn_list;
	int			i;

	cpt = lnet_net_lock_current();

	for (i = 0; i < LNET_REMOTE_NETS_HASH_SIZE; i++) {
		rn_list = &the_lnet.ln_remote_nets_hash[i];
		cfs_list_for_each(e1, rn_list) {
			rnet = cfs_list_entry(e1, lnet_remotenet_t, lrn_list);

			route2 = NULL;
			cfs_list_for_each(e2, &rnet->lrn_routes) {
				lnet_nid_t	nid1;
				lnet_nid_t	nid2;
				int		net;

				route = cfs_list_entry(e2, lnet_route_t,
						       lr_list);

				if (route2 == NULL) {
					route2 = route;
					continue;
				}

				if (route->lr_gateway->lp_ni ==
				    route2->lr_gateway->lp_ni)
					continue;

				nid1 = route->lr_gateway->lp_nid;
				nid2 = route2->lr_gateway->lp_nid;
				net = rnet->lrn_net;

				lnet_net_unlock(cpt);

				CERROR("Routes to %s via %s and %s not "
				       "supported\n",
				       libcfs_net2str(net),
				       libcfs_nid2str(nid1),
				       libcfs_nid2str(nid2));
				return -EINVAL;
			}
		}
	}

	lnet_net_unlock(cpt);
	return 0;
}

int
lnet_del_route(__u32 net, lnet_nid_t gw_nid)
{
	struct lnet_peer	*gateway;
	lnet_remotenet_t	*rnet;
	lnet_route_t		*route;
	cfs_list_t		*e1;
	cfs_list_t		*e2;
	int			rc = -ENOENT;
	cfs_list_t		*rn_list;
	int			idx = 0;

	CDEBUG(D_NET, "Del route: net %s : gw %s\n",
	       libcfs_net2str(net), libcfs_nid2str(gw_nid));

	/* NB Caller may specify either all routes via the given gateway
	 * or a specific route entry actual NIDs) */

	lnet_net_lock(LNET_LOCK_EX);
	if (net == LNET_NIDNET(LNET_NID_ANY))
		rn_list = &the_lnet.ln_remote_nets_hash[0];
	else
		rn_list = lnet_net2rnethash(net);

 again:
	cfs_list_for_each(e1, rn_list) {
		rnet = cfs_list_entry(e1, lnet_remotenet_t, lrn_list);

		if (!(net == LNET_NIDNET(LNET_NID_ANY) ||
			net == rnet->lrn_net))
			continue;

		cfs_list_for_each(e2, &rnet->lrn_routes) {
			route = cfs_list_entry(e2, lnet_route_t, lr_list);

			gateway = route->lr_gateway;
			if (!(gw_nid == LNET_NID_ANY ||
			      gw_nid == gateway->lp_nid))
				continue;

			cfs_list_del(&route->lr_list);
			cfs_list_del(&route->lr_gwlist);
			the_lnet.ln_remote_nets_version++;

			if (cfs_list_empty(&rnet->lrn_routes))
				cfs_list_del(&rnet->lrn_list);
			else
				rnet = NULL;

			lnet_rtr_decref_locked(gateway);
			lnet_peer_decref_locked(gateway);

			lnet_net_unlock(LNET_LOCK_EX);

			LIBCFS_FREE(route, sizeof(*route));

			if (rnet != NULL)
				LIBCFS_FREE(rnet, sizeof(*rnet));

			rc = 0;
			lnet_net_lock(LNET_LOCK_EX);
			goto again;
		}
	}

	if (net == LNET_NIDNET(LNET_NID_ANY) &&
	    ++idx < LNET_REMOTE_NETS_HASH_SIZE) {
		rn_list = &the_lnet.ln_remote_nets_hash[idx];
		goto again;
	}
	lnet_net_unlock(LNET_LOCK_EX);

	return rc;
}

void
lnet_destroy_routes (void)
{
        lnet_del_route(LNET_NIDNET(LNET_NID_ANY), LNET_NID_ANY);
}

static void
lnet_fill_ni_info(struct lnet_ni *ni, __u32 *cpt_count, __u64 *nid,
		  int *peer_timeout, int *peer_tx_credits,
		  int *peer_rtr_credits, int *max_tx_credits,
		  struct libcfs_ioctl_net_config *net_config)
{
	int i;

	if (ni == NULL)
		return;

	if (net_config == NULL)
		return;

	if (ni->ni_interfaces[0] != NULL) {
		for (i = 0; i < LNET_MAX_INTERFACES; i++) {
			if (ni->ni_interfaces[i] != NULL) {
				strncpy(net_config->ni_interfaces[i],
					ni->ni_interfaces[i],
					LIBCFS_MAX_STR_LEN);
			}
		}
	}

	*nid = ni->ni_nid;
	*peer_timeout = ni->ni_peertimeout;
	*peer_tx_credits = ni->ni_peertxcredits;
	*peer_rtr_credits = ni->ni_peerrtrcredits;
	*max_tx_credits = ni->ni_maxtxcredits;

	LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
	net_config->ni_status = ni->ni_status->ns_status;
	LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);

	for (i = 0;
	     ni->ni_cpts != NULL && i < ni->ni_ncpts &&
	     i < LIBCFS_MAX_SHOW_NUM_CPT;
	     i++)
		net_config->cpts[i] = ni->ni_cpts[i];

	*cpt_count = ni->ni_ncpts;
}

int
lnet_get_net_config(int idx, __u32 *cpt_count, __u64 *nid, int *peer_timeout,
		    int *peer_tx_credits, int *peer_rtr_credits,
		    int *max_tx_credits,
		    struct libcfs_ioctl_net_config *net_config)
{
	struct lnet_ni	*ni;
	cfs_list_t	*tmp;
	int		cpt;
	int		rc = -ENOENT;

	cpt = lnet_net_lock_current();

	list_for_each(tmp, &the_lnet.ln_nis) {
		ni = list_entry(tmp, lnet_ni_t, ni_list);
		if (idx-- == 0) {
			rc = 0;
			lnet_fill_ni_info(ni, cpt_count, nid, peer_timeout,
					  peer_tx_credits, peer_rtr_credits,
					  max_tx_credits, net_config);
			break;
		}
	}

	lnet_net_unlock(cpt);
	return rc;
}

int lnet_get_rtrpools(int idx, struct libcfs_ioctl_pool_cfg *pool_cfg)
{
	int i, rc = -ENOENT, lidx, j;

	if (the_lnet.ln_rtrpools == NULL)
		return rc;

	for (i = 0; i < LNET_NRBPOOLS; i++) {
		lnet_rtrbufpool_t *rbp;

		lnet_net_lock(LNET_LOCK_EX);
		lidx = idx;
		cfs_percpt_for_each(rbp, j, the_lnet.ln_rtrpools) {
			if (lidx-- == 0) {
				rc = 0;
				pool_cfg->pools[i].npages = rbp[i].rbp_npages;
				pool_cfg->pools[i].nbuffers =
					rbp[i].rbp_nbuffers;
				pool_cfg->pools[i].credits = rbp[i].rbp_credits;
				pool_cfg->pools[i].mincredits =
					rbp[i].rbp_mincredits;
				break;
			}
		}
		lnet_net_unlock(LNET_LOCK_EX);
	}

	lnet_net_lock(LNET_LOCK_EX);
	pool_cfg->routing = the_lnet.ln_routing;
	lnet_net_unlock(LNET_LOCK_EX);

	return rc;
}

int
lnet_get_route(int idx, __u32 *net, __u32 *hops,
	       lnet_nid_t *gateway, __u32 *alive, __u32 *priority)
{
	cfs_list_t		*e1;
	cfs_list_t		*e2;
	lnet_remotenet_t	*rnet;
	lnet_route_t		*route;
	int			cpt;
	int			i;
	cfs_list_t		*rn_list;

	cpt = lnet_net_lock_current();

	for (i = 0; i < LNET_REMOTE_NETS_HASH_SIZE; i++) {
		rn_list = &the_lnet.ln_remote_nets_hash[i];
		cfs_list_for_each(e1, rn_list) {
			rnet = cfs_list_entry(e1, lnet_remotenet_t, lrn_list);

			cfs_list_for_each(e2, &rnet->lrn_routes) {
				route = cfs_list_entry(e2, lnet_route_t,
						       lr_list);

				if (idx-- == 0) {
					*net	  = rnet->lrn_net;
					*hops	  = route->lr_hops;
					*priority = route->lr_priority;
					*gateway  = route->lr_gateway->lp_nid;
					*alive	  =
						route->lr_gateway->lp_alive &&
							!route->lr_downis;
					lnet_net_unlock(cpt);
					return 0;
				}
			}
		}
	}

	lnet_net_unlock(cpt);
	return -ENOENT;
}

void
lnet_swap_pinginfo(lnet_ping_info_t *info)
{
	int               i;
	lnet_ni_status_t *stat;

	__swab32s(&info->pi_magic);
	__swab32s(&info->pi_features);
	__swab32s(&info->pi_pid);
	__swab32s(&info->pi_nnis);
	for (i = 0; i < info->pi_nnis && i < LNET_MAX_RTR_NIS; i++) {
		stat = &info->pi_ni[i];
		__swab64s(&stat->ns_nid);
		__swab32s(&stat->ns_status);
	}
	return;
}

/**
 * parse router-checker pinginfo, record number of down NIs for remote
 * networks on that router.
 */
static void
lnet_parse_rc_info(lnet_rc_data_t *rcd)
{
	lnet_ping_info_t	*info = rcd->rcd_pinginfo;
	struct lnet_peer	*gw   = rcd->rcd_gateway;
	lnet_route_t		*rtr;

	if (!gw->lp_alive)
		return;

	if (info->pi_magic == __swab32(LNET_PROTO_PING_MAGIC))
		lnet_swap_pinginfo(info);

	/* NB always racing with network! */
	if (info->pi_magic != LNET_PROTO_PING_MAGIC) {
		CDEBUG(D_NET, "%s: Unexpected magic %08x\n",
		       libcfs_nid2str(gw->lp_nid), info->pi_magic);
		gw->lp_ping_feats = LNET_PING_FEAT_INVAL;
		return;
	}

	gw->lp_ping_feats = info->pi_features;
	if ((gw->lp_ping_feats & LNET_PING_FEAT_MASK) == 0) {
		CDEBUG(D_NET, "%s: Unexpected features 0x%x\n",
		       libcfs_nid2str(gw->lp_nid), gw->lp_ping_feats);
		return; /* nothing I can understand */
	}

	if ((gw->lp_ping_feats & LNET_PING_FEAT_NI_STATUS) == 0)
		return; /* can't carry NI status info */

	cfs_list_for_each_entry(rtr, &gw->lp_routes, lr_gwlist) {
		int	ptl_status = LNET_NI_STATUS_INVALID;
		int	down = 0;
		int	up = 0;
		int	i;

		if ((gw->lp_ping_feats & LNET_PING_FEAT_ROUTING) == 0) {
			rtr->lr_downis = 1;
			continue;
		}

		for (i = 0; i < info->pi_nnis && i < LNET_MAX_RTR_NIS; i++) {
			lnet_ni_status_t *stat = &info->pi_ni[i];
			lnet_nid_t	 nid = stat->ns_nid;

			if (nid == LNET_NID_ANY) {
				CDEBUG(D_NET, "%s: unexpected LNET_NID_ANY\n",
				       libcfs_nid2str(gw->lp_nid));
				gw->lp_ping_feats = LNET_PING_FEAT_INVAL;
				return;
			}

			if (LNET_NETTYP(LNET_NIDNET(nid)) == LOLND)
				continue;

			if (stat->ns_status == LNET_NI_STATUS_DOWN) {
				if (LNET_NETTYP(LNET_NIDNET(nid)) != PTLLND)
					down++;
				else if (ptl_status != LNET_NI_STATUS_UP)
					ptl_status = LNET_NI_STATUS_DOWN;
				continue;
			}

			if (stat->ns_status == LNET_NI_STATUS_UP) {
				if (LNET_NIDNET(nid) == rtr->lr_net) {
					up = 1;
					break;
				}
				/* ptl NIs are considered down only when
				 * they're all down */
				if (LNET_NETTYP(LNET_NIDNET(nid)) == PTLLND)
					ptl_status = LNET_NI_STATUS_UP;
				continue;
			}

			CDEBUG(D_NET, "%s: Unexpected status 0x%x\n",
			       libcfs_nid2str(gw->lp_nid), stat->ns_status);
			gw->lp_ping_feats = LNET_PING_FEAT_INVAL;
			return;
		}

		if (up) { /* ignore downed NIs if NI for dest network is up */
			rtr->lr_downis = 0;
			continue;
		}
		rtr->lr_downis = down + (ptl_status == LNET_NI_STATUS_DOWN);
	}
}

static void
lnet_router_checker_event(lnet_event_t *event)
{
	lnet_rc_data_t		*rcd = event->md.user_ptr;
	struct lnet_peer	*lp;

	LASSERT(rcd != NULL);

	if (event->unlinked) {
		LNetInvalidateHandle(&rcd->rcd_mdh);
		return;
	}

	LASSERT(event->type == LNET_EVENT_SEND ||
		event->type == LNET_EVENT_REPLY);

	lp = rcd->rcd_gateway;
	LASSERT(lp != NULL);

	 /* NB: it's called with holding lnet_res_lock, we have a few
	  * places need to hold both locks at the same time, please take
	  * care of lock ordering */
	lnet_net_lock(lp->lp_cpt);
	if (!lnet_isrouter(lp) || lp->lp_rcd != rcd)
		/* ignore if no longer a router or rcd is replaced */
		goto out;

	if (event->type == LNET_EVENT_SEND) {
		lp->lp_ping_notsent = 0;
		if (event->status == 0)
			goto out;
	}

	/* LNET_EVENT_REPLY */
	/* A successful REPLY means the router is up.  If _any_ comms
	 * to the router fail I assume it's down (this will happen if
	 * we ping alive routers to try to detect router death before
	 * apps get burned). */

	lnet_notify_locked(lp, 1, (event->status == 0), cfs_time_current());
	/* The router checker will wake up very shortly and do the
	 * actual notification.
	 * XXX If 'lp' stops being a router before then, it will still
	 * have the notification pending!!! */

	if (avoid_asym_router_failure && event->status == 0)
		lnet_parse_rc_info(rcd);

 out:
	lnet_net_unlock(lp->lp_cpt);
}

void
lnet_wait_known_routerstate(void)
{
        lnet_peer_t         *rtr;
        cfs_list_t          *entry;
        int                  all_known;

        LASSERT (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING);

        for (;;) {
		int	cpt = lnet_net_lock_current();

                all_known = 1;
                cfs_list_for_each (entry, &the_lnet.ln_routers) {
                        rtr = cfs_list_entry(entry, lnet_peer_t, lp_rtr_list);

                        if (rtr->lp_alive_count == 0) {
                                all_known = 0;
                                break;
                        }
                }

		lnet_net_unlock(cpt);

                if (all_known)
                        return;

#ifndef __KERNEL__
                lnet_router_checker();
#endif
                cfs_pause(cfs_time_seconds(1));
        }
}

void
lnet_update_ni_status_locked(void)
{
	lnet_ni_t	*ni;
	long		now;
	int		timeout;

	LASSERT(the_lnet.ln_routing);

	timeout = router_ping_timeout +
		  MAX(live_router_check_interval, dead_router_check_interval);

	now = cfs_time_current_sec();
	LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
	cfs_list_for_each_entry(ni, &the_lnet.ln_nis, ni_list) {
		if (ni->ni_lnd->lnd_type == LOLND)
			continue;

		if (now < ni->ni_last_alive + timeout)
			continue;

		lnet_ni_lock(ni);
		/* re-check with lock */
		if (now < ni->ni_last_alive + timeout) {
			lnet_ni_unlock(ni);
			continue;
		}

		LASSERT(ni->ni_status != NULL);

		if (ni->ni_status->ns_status != LNET_NI_STATUS_DOWN) {
			CDEBUG(D_NET, "NI(%s:%d) status changed to down\n",
			       libcfs_nid2str(ni->ni_nid), timeout);
			/* NB: so far, this is the only place to set
			 * NI status to "down" */
			ni->ni_status->ns_status = LNET_NI_STATUS_DOWN;
		}
		lnet_ni_unlock(ni);
	}
	LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);
}

void
lnet_destroy_rc_data(lnet_rc_data_t *rcd)
{
	LASSERT(cfs_list_empty(&rcd->rcd_list));
	/* detached from network */
	LASSERT(LNetHandleIsInvalid(rcd->rcd_mdh));

	if (rcd->rcd_gateway != NULL) {
		int cpt = rcd->rcd_gateway->lp_cpt;

		lnet_net_lock(cpt);
		lnet_peer_decref_locked(rcd->rcd_gateway);
		lnet_net_unlock(cpt);
	}

	if (rcd->rcd_pinginfo != NULL)
		LIBCFS_FREE(rcd->rcd_pinginfo, LNET_PINGINFO_SIZE);

	LIBCFS_FREE(rcd, sizeof(*rcd));
}

lnet_rc_data_t *
lnet_create_rc_data_locked(lnet_peer_t *gateway)
{
	lnet_rc_data_t		*rcd = NULL;
	lnet_ping_info_t	*pi;
	int			rc;
	int			i;

	lnet_net_unlock(gateway->lp_cpt);

	LIBCFS_ALLOC(rcd, sizeof(*rcd));
	if (rcd == NULL)
		goto out;

	LNetInvalidateHandle(&rcd->rcd_mdh);
	CFS_INIT_LIST_HEAD(&rcd->rcd_list);

	LIBCFS_ALLOC(pi, LNET_PINGINFO_SIZE);
	if (pi == NULL)
		goto out;

        memset(pi, 0, LNET_PINGINFO_SIZE);
        for (i = 0; i < LNET_MAX_RTR_NIS; i++) {
                pi->pi_ni[i].ns_nid = LNET_NID_ANY;
                pi->pi_ni[i].ns_status = LNET_NI_STATUS_INVALID;
        }
        rcd->rcd_pinginfo = pi;

        LASSERT (!LNetHandleIsInvalid(the_lnet.ln_rc_eqh));
        rc = LNetMDBind((lnet_md_t){.start     = pi,
                                    .user_ptr  = rcd,
                                    .length    = LNET_PINGINFO_SIZE,
                                    .threshold = LNET_MD_THRESH_INF,
                                    .options   = LNET_MD_TRUNCATE,
                                    .eq_handle = the_lnet.ln_rc_eqh},
                        LNET_UNLINK,
                        &rcd->rcd_mdh);
        if (rc < 0) {
                CERROR("Can't bind MD: %d\n", rc);
		goto out;
	}
	LASSERT(rc == 0);

	lnet_net_lock(gateway->lp_cpt);
	/* router table changed or someone has created rcd for this gateway */
	if (!lnet_isrouter(gateway) || gateway->lp_rcd != NULL) {
		lnet_net_unlock(gateway->lp_cpt);
		goto out;
	}

	lnet_peer_addref_locked(gateway);
	rcd->rcd_gateway = gateway;
	gateway->lp_rcd = rcd;
	gateway->lp_ping_notsent = 0;

	return rcd;

 out:
	if (rcd != NULL) {
		if (!LNetHandleIsInvalid(rcd->rcd_mdh)) {
			rc = LNetMDUnlink(rcd->rcd_mdh);
			LASSERT(rc == 0);
		}
		lnet_destroy_rc_data(rcd);
	}

	lnet_net_lock(gateway->lp_cpt);
	return gateway->lp_rcd;
}

static int
lnet_router_check_interval (lnet_peer_t *rtr)
{
        int secs;

        secs = rtr->lp_alive ? live_router_check_interval :
                               dead_router_check_interval;
        if (secs < 0)
                secs = 0;

        return secs;
}

static void
lnet_ping_router_locked (lnet_peer_t *rtr)
{
        lnet_rc_data_t *rcd = NULL;
        cfs_time_t      now = cfs_time_current();
        int             secs;

        lnet_peer_addref_locked(rtr);

        if (rtr->lp_ping_deadline != 0 && /* ping timed out? */
            cfs_time_after(now, rtr->lp_ping_deadline))
                lnet_notify_locked(rtr, 1, 0, now);

	/* Run any outstanding notifications */
	lnet_ni_notify_locked(rtr->lp_ni, rtr);

	if (!lnet_isrouter(rtr) ||
	    the_lnet.ln_rc_state != LNET_RC_STATE_RUNNING) {
		/* router table changed or router checker is shutting down */
		lnet_peer_decref_locked(rtr);
		return;
	}

	rcd = rtr->lp_rcd != NULL ?
	      rtr->lp_rcd : lnet_create_rc_data_locked(rtr);

	if (rcd == NULL)
		return;

        secs = lnet_router_check_interval(rtr);

        CDEBUG(D_NET,
               "rtr %s %d: deadline %lu ping_notsent %d alive %d "
               "alive_count %d lp_ping_timestamp %lu\n",
               libcfs_nid2str(rtr->lp_nid), secs,
               rtr->lp_ping_deadline, rtr->lp_ping_notsent,
               rtr->lp_alive, rtr->lp_alive_count, rtr->lp_ping_timestamp);

        if (secs != 0 && !rtr->lp_ping_notsent &&
            cfs_time_after(now, cfs_time_add(rtr->lp_ping_timestamp,
                                             cfs_time_seconds(secs)))) {
                int               rc;
                lnet_process_id_t id;
                lnet_handle_md_t  mdh;

                id.nid = rtr->lp_nid;
                id.pid = LUSTRE_SRV_LNET_PID;
                CDEBUG(D_NET, "Check: %s\n", libcfs_id2str(id));

                rtr->lp_ping_notsent   = 1;
                rtr->lp_ping_timestamp = now;

		mdh = rcd->rcd_mdh;

		if (rtr->lp_ping_deadline == 0) {
			rtr->lp_ping_deadline =
				cfs_time_shift(router_ping_timeout);
		}

		lnet_net_unlock(rtr->lp_cpt);

		rc = LNetGet(LNET_NID_ANY, mdh, id, LNET_RESERVED_PORTAL,
			     LNET_PROTO_PING_MATCHBITS, 0);

		lnet_net_lock(rtr->lp_cpt);
                if (rc != 0)
                        rtr->lp_ping_notsent = 0; /* no event pending */
        }

        lnet_peer_decref_locked(rtr);
        return;
}

int
lnet_router_checker_start(void)
{
        int          rc;
        int          eqsz;
#ifndef __KERNEL__
        lnet_peer_t *rtr;
        __u64        version;
        int          nrtr = 0;
        int          router_checker_max_eqsize = 10240;

        LASSERT (check_routers_before_use);
        LASSERT (dead_router_check_interval > 0);

	lnet_net_lock(0);

        /* As an approximation, allow each router the same number of
         * outstanding events as it is allowed outstanding sends */
        eqsz = 0;
        version = the_lnet.ln_routers_version;
        cfs_list_for_each_entry(rtr, &the_lnet.ln_routers, lp_rtr_list) {
                lnet_ni_t         *ni = rtr->lp_ni;
                lnet_process_id_t  id;

                nrtr++;
                eqsz += ni->ni_peertxcredits;

                /* one async ping reply per router */
                id.nid = rtr->lp_nid;
                id.pid = LUSTRE_SRV_LNET_PID;

		lnet_net_unlock(0);

                rc = LNetSetAsync(id, 1);
                if (rc != 0) {
                        CWARN("LNetSetAsync %s failed: %d\n",
                              libcfs_id2str(id), rc);
                        return rc;
                }

		lnet_net_lock(0);
		/* NB router list doesn't change in userspace */
		LASSERT(version == the_lnet.ln_routers_version);
	}

	lnet_net_unlock(0);

        if (nrtr == 0) {
                CDEBUG(D_NET,
                       "No router found, not starting router checker\n");
                return 0;
        }

        /* at least allow a SENT and a REPLY per router */
        if (router_checker_max_eqsize < 2 * nrtr)
                router_checker_max_eqsize = 2 * nrtr;

        LASSERT (eqsz > 0);
        if (eqsz > router_checker_max_eqsize)
                eqsz = router_checker_max_eqsize;
#endif

        LASSERT (the_lnet.ln_rc_state == LNET_RC_STATE_SHUTDOWN);

        if (check_routers_before_use &&
            dead_router_check_interval <= 0) {
                LCONSOLE_ERROR_MSG(0x10a, "'dead_router_check_interval' must be"
                                   " set if 'check_routers_before_use' is set"
                                   "\n");
                return -EINVAL;
        }

        if (!the_lnet.ln_routing &&
            live_router_check_interval <= 0 &&
            dead_router_check_interval <= 0)
                return 0;

#ifdef __KERNEL__
	sema_init(&the_lnet.ln_rc_signal, 0);
        /* EQ size doesn't matter; the callback is guaranteed to get every
         * event */
	eqsz = 0;
        rc = LNetEQAlloc(eqsz, lnet_router_checker_event,
                         &the_lnet.ln_rc_eqh);
#else
        rc = LNetEQAlloc(eqsz, LNET_EQ_HANDLER_NONE,
                         &the_lnet.ln_rc_eqh);
#endif
        if (rc != 0) {
                CERROR("Can't allocate EQ(%d): %d\n", eqsz, rc);
                return -ENOMEM;
        }

        the_lnet.ln_rc_state = LNET_RC_STATE_RUNNING;
#ifdef __KERNEL__
	rc = PTR_ERR(kthread_run(lnet_router_checker,
				 NULL, "router_checker"));
	if (IS_ERR_VALUE(rc)) {
		CERROR("Can't start router checker thread: %d\n", rc);
		/* block until event callback signals exit */
		down(&the_lnet.ln_rc_signal);
		rc = LNetEQFree(the_lnet.ln_rc_eqh);
		LASSERT(rc == 0);
		the_lnet.ln_rc_state = LNET_RC_STATE_SHUTDOWN;
		return -ENOMEM;
	}
#endif

        if (check_routers_before_use) {
                /* Note that a helpful side-effect of pinging all known routers
                 * at startup is that it makes them drop stale connections they
                 * may have to a previous instance of me. */
                lnet_wait_known_routerstate();
        }

        return 0;
}

void
lnet_router_checker_stop (void)
{
        int rc;

        if (the_lnet.ln_rc_state == LNET_RC_STATE_SHUTDOWN)
                return;

        LASSERT (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING);
	the_lnet.ln_rc_state = LNET_RC_STATE_STOPPING;

#ifdef __KERNEL__
	/* block until event callback signals exit */
	down(&the_lnet.ln_rc_signal);
#else
	lnet_router_checker();
#endif
	LASSERT(the_lnet.ln_rc_state == LNET_RC_STATE_SHUTDOWN);

        rc = LNetEQFree(the_lnet.ln_rc_eqh);
        LASSERT (rc == 0);
        return;
}

static void
lnet_prune_rc_data(int wait_unlink)
{
	lnet_rc_data_t		*rcd;
	lnet_rc_data_t		*tmp;
	lnet_peer_t		*lp;
	cfs_list_t		head;
	int			i = 2;

	if (likely(the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING &&
		   cfs_list_empty(&the_lnet.ln_rcd_deathrow) &&
		   cfs_list_empty(&the_lnet.ln_rcd_zombie)))
		return;

	CFS_INIT_LIST_HEAD(&head);

	lnet_net_lock(LNET_LOCK_EX);

	if (the_lnet.ln_rc_state != LNET_RC_STATE_RUNNING) {
		/* router checker is stopping, prune all */
		cfs_list_for_each_entry(lp, &the_lnet.ln_routers,
					lp_rtr_list) {
			if (lp->lp_rcd == NULL)
				continue;

			LASSERT(cfs_list_empty(&lp->lp_rcd->rcd_list));
			cfs_list_add(&lp->lp_rcd->rcd_list,
				     &the_lnet.ln_rcd_deathrow);
			lp->lp_rcd = NULL;
		}
	}

	/* unlink all RCDs on deathrow list */
	cfs_list_splice_init(&the_lnet.ln_rcd_deathrow, &head);

	if (!cfs_list_empty(&head)) {
		lnet_net_unlock(LNET_LOCK_EX);

		cfs_list_for_each_entry(rcd, &head, rcd_list)
			LNetMDUnlink(rcd->rcd_mdh);

		lnet_net_lock(LNET_LOCK_EX);
	}

	cfs_list_splice_init(&head, &the_lnet.ln_rcd_zombie);

	/* release all zombie RCDs */
	while (!cfs_list_empty(&the_lnet.ln_rcd_zombie)) {
		cfs_list_for_each_entry_safe(rcd, tmp, &the_lnet.ln_rcd_zombie,
					     rcd_list) {
			if (LNetHandleIsInvalid(rcd->rcd_mdh))
				cfs_list_move(&rcd->rcd_list, &head);
		}

		wait_unlink = wait_unlink &&
			      !cfs_list_empty(&the_lnet.ln_rcd_zombie);

		lnet_net_unlock(LNET_LOCK_EX);

		while (!cfs_list_empty(&head)) {
			rcd = cfs_list_entry(head.next,
					     lnet_rc_data_t, rcd_list);
			cfs_list_del_init(&rcd->rcd_list);
			lnet_destroy_rc_data(rcd);
		}

		if (!wait_unlink)
			return;

		i++;
		CDEBUG(((i & (-i)) == i) ? D_WARNING : D_NET,
		       "Waiting for rc buffers to unlink\n");
		cfs_pause(cfs_time_seconds(1) / 4);

		lnet_net_lock(LNET_LOCK_EX);
	}

	lnet_net_unlock(LNET_LOCK_EX);
}


#if defined(__KERNEL__) && defined(LNET_ROUTER)

static int
lnet_router_checker(void *arg)
{
        lnet_peer_t       *rtr;
        cfs_list_t        *entry;

        cfs_block_allsigs();

        LASSERT (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING);

        while (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING) {
		__u64	version;
		int	cpt;
		int	cpt2;

		cpt = lnet_net_lock_current();
rescan:
		version = the_lnet.ln_routers_version;

		cfs_list_for_each(entry, &the_lnet.ln_routers) {
			rtr = cfs_list_entry(entry, lnet_peer_t, lp_rtr_list);

			cpt2 = lnet_cpt_of_nid_locked(rtr->lp_nid);
			if (cpt != cpt2) {
				lnet_net_unlock(cpt);
				cpt = cpt2;
				lnet_net_lock(cpt);
				/* the routers list has changed */
				if (version != the_lnet.ln_routers_version)
					goto rescan;
			}

                        lnet_ping_router_locked(rtr);

                        /* NB dropped lock */
                        if (version != the_lnet.ln_routers_version) {
                                /* the routers list has changed */
                                goto rescan;
                        }
                }

		if (the_lnet.ln_routing)
			lnet_update_ni_status_locked();

		lnet_net_unlock(cpt);

		lnet_prune_rc_data(0); /* don't wait for UNLINK */

		/* Call cfs_pause() here always adds 1 to load average
		 * because kernel counts # active tasks as nr_running
		 * + nr_uninterruptible. */
		schedule_timeout_and_set_state(TASK_INTERRUPTIBLE,
						   cfs_time_seconds(1));
	}

	LASSERT(the_lnet.ln_rc_state == LNET_RC_STATE_STOPPING);

	lnet_prune_rc_data(1); /* wait for UNLINK */

	the_lnet.ln_rc_state = LNET_RC_STATE_SHUTDOWN;
	up(&the_lnet.ln_rc_signal);
	/* The unlink event callback will signal final completion */
	return 0;
}

void
lnet_destroy_rtrbuf(lnet_rtrbuf_t *rb, int npages)
{
        int sz = offsetof(lnet_rtrbuf_t, rb_kiov[npages]);

        while (--npages >= 0)
		__free_page(rb->rb_kiov[npages].kiov_page);

        LIBCFS_FREE(rb, sz);
}

lnet_rtrbuf_t *
lnet_new_rtrbuf(lnet_rtrbufpool_t *rbp, int cpt)
{
	int            npages = rbp->rbp_npages;
	int            sz = offsetof(lnet_rtrbuf_t, rb_kiov[npages]);
	struct page   *page;
	lnet_rtrbuf_t *rb;
	int            i;

	LIBCFS_CPT_ALLOC(rb, lnet_cpt_table(), cpt, sz);
	if (rb == NULL)
		return NULL;

	rb->rb_pool = rbp;

	for (i = 0; i < npages; i++) {
		page = cfs_page_cpt_alloc(lnet_cpt_table(), cpt,
					  __GFP_ZERO | GFP_IOFS);
                if (page == NULL) {
                        while (--i >= 0)
				__free_page(rb->rb_kiov[i].kiov_page);

                        LIBCFS_FREE(rb, sz);
                        return NULL;
                }

		rb->rb_kiov[i].kiov_len = PAGE_CACHE_SIZE;
                rb->rb_kiov[i].kiov_offset = 0;
                rb->rb_kiov[i].kiov_page = page;
        }

        return rb;
}

void
lnet_rtrpool_free_bufs(lnet_rtrbufpool_t *rbp)
{
	int		npages = rbp->rbp_npages;
	lnet_rtrbuf_t	*rb;
	lnet_msg_t	*msg;

	if (rbp->rbp_nbuffers == 0) /* not initialized or already freed */
		return;

	/* Free buffered messages (if any). */
	while (!cfs_list_empty(&rbp->rbp_msgs)) {
		msg = cfs_list_entry(rbp->rbp_msgs.next,
				     lnet_msg_t, msg_list);
		cfs_list_del(&msg->msg_list);
		lnet_msg_free_locked(msg);
	}

	/* Free buffers on the free list. */
	while (!cfs_list_empty(&rbp->rbp_bufs)) {
		rb = cfs_list_entry(rbp->rbp_bufs.next,
				    lnet_rtrbuf_t, rb_list);
		cfs_list_del(&rb->rb_list);
		lnet_destroy_rtrbuf(rb, npages);
	}

	rbp->rbp_nbuffers = rbp->rbp_credits = 0;
	rbp->rbp_mincredits = 0;
}

static int
lnet_rtrpool_adjust_bufs(lnet_rtrbufpool_t *rbp, int nbufs, int cpt)
{
	/* If we are called for less buffers than already in the pool, we
	 * just lower the nbuffers number and excess buffers will be
	 * thrown away as they are returned to the free list.  Credits
	 * then get adjusted as well. */
	if (nbufs < rbp->rbp_nbuffers) {
		rbp->rbp_nbuffers = nbufs;
		return 0;
	}

	/* Add any extra buffers needed. */
	while (rbp->rbp_nbuffers < nbufs) {
		lnet_rtrbuf_t *rb;

		rb = lnet_new_rtrbuf(rbp, cpt);
		if (rb == NULL) {
			CERROR("Failed to allocate %d route bufs of %d pages\n",
			       nbufs - rbp->rbp_nbuffers, rbp->rbp_npages);
			return -ENOMEM;
		}

		lnet_net_lock(cpt);

		cfs_list_add(&rb->rb_list, &rbp->rbp_bufs);
		rbp->rbp_nbuffers++;
		rbp->rbp_credits++;
		if (rbp->rbp_credits <= 0) {
			/* We need to schedule blocked msg using the newly
			   added buffer. */
			lnet_schedule_blocked_locked(rbp);
		}
		lnet_net_unlock(cpt);
	}

	return 0;
}

void
lnet_rtrpool_init(lnet_rtrbufpool_t *rbp, int npages)
{
        CFS_INIT_LIST_HEAD(&rbp->rbp_msgs);
        CFS_INIT_LIST_HEAD(&rbp->rbp_bufs);

        rbp->rbp_npages = npages;
        rbp->rbp_credits = 0;
        rbp->rbp_mincredits = 0;
}

void
lnet_rtrpools_free(int keep_pools)
{
	lnet_rtrbufpool_t *rtrp;
	int		  i;

	if (the_lnet.ln_rtrpools == NULL) /* uninitialized or freed */
		return;

	cfs_percpt_for_each(rtrp, i, the_lnet.ln_rtrpools) {
		lnet_rtrpool_free_bufs(&rtrp[LNET_TINY_BUF_IDX]);
		lnet_rtrpool_free_bufs(&rtrp[LNET_SMALL_BUF_IDX]);
		lnet_rtrpool_free_bufs(&rtrp[LNET_LARGE_BUF_IDX]);
	}

	if (!keep_pools) {
		cfs_percpt_free(the_lnet.ln_rtrpools);
		the_lnet.ln_rtrpools = NULL;
	}
}

static int
lnet_nrb_tiny_calculate(void)
{
	int	nrbs = LNET_NRB_TINY;

	if (tiny_router_buffers < 0) {
		LCONSOLE_ERROR_MSG(0x10c,
				   "tiny_router_buffers=%d invalid when "
				   "routing enabled\n", tiny_router_buffers);
		return -1;
	}

	if (tiny_router_buffers > 0)
		nrbs = tiny_router_buffers;

	nrbs /= LNET_CPT_NUMBER;
	return max(nrbs, LNET_NRB_TINY_MIN);
}

static int
lnet_nrb_small_calculate(void)
{
	int	nrbs = LNET_NRB_SMALL;

	if (small_router_buffers < 0) {
		LCONSOLE_ERROR_MSG(0x10c,
				   "small_router_buffers=%d invalid when "
				   "routing enabled\n", small_router_buffers);
		return -1;
	}

	if (small_router_buffers > 0)
		nrbs = small_router_buffers;

	nrbs /= LNET_CPT_NUMBER;
	return max(nrbs, LNET_NRB_SMALL_MIN);
}

static int
lnet_nrb_large_calculate(void)
{
	int	nrbs = LNET_NRB_LARGE;

	if (large_router_buffers < 0) {
		LCONSOLE_ERROR_MSG(0x10c,
				   "large_router_buffers=%d invalid when "
				   "routing enabled\n", large_router_buffers);
		return -1;
	}

	if (large_router_buffers > 0)
		nrbs = large_router_buffers;

	nrbs /= LNET_CPT_NUMBER;
	return max(nrbs, LNET_NRB_LARGE_MIN);
}

int
lnet_rtrpools_alloc(int im_a_router)
{
	lnet_rtrbufpool_t *rtrp;
	int	large_pages = LNET_NRB_LARGE_PAGES;
	int	small_pages = LNET_NRB_SMALL_PAGES;
	int	nrb_tiny;
	int	nrb_small;
	int	nrb_large;
	int	rc;
	int	i;

	if (!strcmp(forwarding, "")) {
		/* not set either way */
		if (!im_a_router)
			return 0;
	} else if (!strcmp(forwarding, "disabled")) {
		/* explicitly disabled */
		return 0;
	} else if (!strcmp(forwarding, "enabled")) {
		/* explicitly enabled */
	} else {
		LCONSOLE_ERROR_MSG(0x10b, "'forwarding' not set to either "
				   "'enabled' or 'disabled'\n");
		return -EINVAL;
	}

	nrb_tiny = lnet_nrb_tiny_calculate();
	if (nrb_tiny < 0)
		return -EINVAL;

	nrb_small = lnet_nrb_small_calculate();
	if (nrb_small < 0)
		return -EINVAL;

	nrb_large = lnet_nrb_large_calculate();
	if (nrb_large < 0)
		return -EINVAL;

	the_lnet.ln_rtrpools = cfs_percpt_alloc(lnet_cpt_table(),
						LNET_NRBPOOLS *
						sizeof(lnet_rtrbufpool_t));
	if (the_lnet.ln_rtrpools == NULL) {
		LCONSOLE_ERROR_MSG(0x10c,
				   "Failed to initialize router buffe pool\n");
		return -ENOMEM;
	}

	cfs_percpt_for_each(rtrp, i, the_lnet.ln_rtrpools) {
		lnet_rtrpool_init(&rtrp[LNET_TINY_BUF_IDX], 0);
		rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_TINY_BUF_IDX],
					      nrb_tiny, i);
		if (rc != 0)
			goto failed;

		lnet_rtrpool_init(&rtrp[LNET_SMALL_BUF_IDX], small_pages);
		rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_SMALL_BUF_IDX],
					      nrb_small, i);
		if (rc != 0)
			goto failed;

		lnet_rtrpool_init(&rtrp[LNET_LARGE_BUF_IDX], large_pages);
		rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_LARGE_BUF_IDX],
					      nrb_large, i);
		if (rc != 0)
			goto failed;
	}

	tiny_router_buffers = nrb_tiny;
	small_router_buffers = nrb_small;
	large_router_buffers = nrb_large;

	lnet_net_lock(LNET_LOCK_EX);
	the_lnet.ln_routing = 1;
	lnet_net_unlock(LNET_LOCK_EX);

	LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
	if (the_lnet.ln_ping_info)
		the_lnet.ln_ping_info->pi_features |=
		  LNET_PING_FEAT_ROUTING;
	LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);
	return 0;

 failed:
	lnet_rtrpools_free(0);
	return rc;
}

static int
lnet_adjust_rtrpools_helper(int tiny, int small, int large)
{
	int nrb_tiny = 0, nrb_small = 0, nrb_large = 0;
	int rc = 0;
	int i;
	lnet_rtrbufpool_t *rtrp;

	/* If the provided values for each buffer pool are different than the
	 * configured values, we need to take action. */
	if (tiny != tiny_router_buffers &&
	    tiny >= 0) {
		tiny_router_buffers = tiny;
		nrb_tiny = lnet_nrb_tiny_calculate();
		if (nrb_tiny < 0)
			return -EINVAL;
		cfs_percpt_for_each(rtrp, i, the_lnet.ln_rtrpools) {
			rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_TINY_BUF_IDX],
						      nrb_tiny, i);
			if (rc != 0)
				goto failed;
		}
		tiny_router_buffers = nrb_tiny;
	}
	if (small != small_router_buffers &&
	    small >= 0) {
		small_router_buffers = small;
		nrb_small = lnet_nrb_small_calculate();
		if (nrb_small < 0)
			return -EINVAL;
		cfs_percpt_for_each(rtrp, i, the_lnet.ln_rtrpools) {
			rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_SMALL_BUF_IDX],
						      nrb_small, i);
			if (rc != 0)
				goto failed;
		}
		small_router_buffers = nrb_small;
	}
	if (large != large_router_buffers &&
	    large >= 0) {
		large_router_buffers = large;
		nrb_large = lnet_nrb_large_calculate();
		if (nrb_large < 0)
			return -EINVAL;
		cfs_percpt_for_each(rtrp, i, the_lnet.ln_rtrpools) {
			rc = lnet_rtrpool_adjust_bufs(&rtrp[LNET_LARGE_BUF_IDX],
						      nrb_large, i);
			if (rc != 0)
				goto failed;
		}
		large_router_buffers = nrb_large;
	}

	return 0;

failed:
	lnet_rtrpools_free(1);
	return rc;
}

int lnet_enable_rtrpools(void)
{

	if (!the_lnet.ln_routing) {
		if (the_lnet.ln_rtrpools == NULL) {
			/* If routing is turned off, and we have never
			 * initialized the pools before, just call the
			 * standard buffer pool allocation routine as
			 * if we are just configuring this for the first
			 * time. */
			return lnet_rtrpools_alloc(1);
		} else {
			int rc;

			rc = lnet_adjust_rtrpools_helper
				(tiny_router_buffers_hold,
				 small_router_buffers_hold,
				 large_router_buffers_hold);
			if (rc != 0)
				return rc;
			lnet_net_lock(LNET_LOCK_EX);
			the_lnet.ln_routing = 1;
			lnet_net_unlock(LNET_LOCK_EX);
			LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
			if (the_lnet.ln_ping_info)
				the_lnet.ln_ping_info->pi_features |=
				  LNET_PING_FEAT_ROUTING;
			LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);
		}
	}

	return 0;
}

int lnet_disable_rtrpools(void)
{
	if (the_lnet.ln_routing) {
		/*
		 * Turn off routing if it's enabled, and keep the values
		 * of tiny_router_buffers, small_router_buffers and
		 * large_router_buffers to the previously set values in
		 * order to use them the next time routing is enabled.
		 */
		lnet_net_lock(LNET_LOCK_EX);
		the_lnet.ln_routing = 0;
		tiny_router_buffers_hold = tiny_router_buffers;
		tiny_router_buffers = 0;
		small_router_buffers_hold = small_router_buffers;
		small_router_buffers = 0;
		large_router_buffers_hold = large_router_buffers;
		large_router_buffers = 0;
		lnet_rtrpools_free(1);
		lnet_net_unlock(LNET_LOCK_EX);
		LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
		if (the_lnet.ln_ping_info)
			the_lnet.ln_ping_info->pi_features &=
			  ~LNET_PING_FEAT_ROUTING;
		LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);
	}

	return 0;
}

int
lnet_adjust_rtrpools(int tiny, int small, int large)
{
	/* if routing is turned off, then just set the buffer values to be
	 * used when routing is turned on.  If the value passed in is <0
	 * then leave the buffer size as it previously was */
	if (!the_lnet.ln_routing) {
		if (tiny >= 0)
			tiny_router_buffers_hold = tiny;
		if (small >= 0)
			small_router_buffers_hold = small;
		if (large >= 0)
			large_router_buffers_hold = large;
		return 0;
	}


	return lnet_adjust_rtrpools_helper(tiny, small, large);
}

int
lnet_notify(lnet_ni_t *ni, lnet_nid_t nid, int alive, cfs_time_t when)
{
	struct lnet_peer	*lp = NULL;
	cfs_time_t		now = cfs_time_current();
	int			cpt = lnet_cpt_of_nid(nid);

	LASSERT (!in_interrupt ());

	CDEBUG (D_NET, "%s notifying %s: %s\n",
		(ni == NULL) ? "userspace" : libcfs_nid2str(ni->ni_nid),
		libcfs_nid2str(nid),
		alive ? "up" : "down");

        if (ni != NULL &&
            LNET_NIDNET(ni->ni_nid) != LNET_NIDNET(nid)) {
                CWARN ("Ignoring notification of %s %s by %s (different net)\n",
                        libcfs_nid2str(nid), alive ? "birth" : "death",
                        libcfs_nid2str(ni->ni_nid));
                return -EINVAL;
        }

        /* can't do predictions... */
        if (cfs_time_after(when, now)) {
                CWARN ("Ignoring prediction from %s of %s %s "
                       "%ld seconds in the future\n",
                       (ni == NULL) ? "userspace" : libcfs_nid2str(ni->ni_nid),
                       libcfs_nid2str(nid), alive ? "up" : "down",
                       cfs_duration_sec(cfs_time_sub(when, now)));
                return -EINVAL;
        }

        if (ni != NULL && !alive &&             /* LND telling me she's down */
            !auto_down) {                       /* auto-down disabled */
                CDEBUG(D_NET, "Auto-down disabled\n");
                return 0;
        }

	lnet_net_lock(cpt);

	if (the_lnet.ln_shutdown) {
		lnet_net_unlock(cpt);
		return -ESHUTDOWN;
	}

	lp = lnet_find_peer_locked(the_lnet.ln_peer_tables[cpt], nid);
	if (lp == NULL) {
		/* nid not found */
		lnet_net_unlock(cpt);
                CDEBUG(D_NET, "%s not found\n", libcfs_nid2str(nid));
                return 0;
        }

        /* We can't fully trust LND on reporting exact peer last_alive
         * if he notifies us about dead peer. For example ksocklnd can
         * call us with when == _time_when_the_node_was_booted_ if
         * no connections were successfully established */
        if (ni != NULL && !alive && when < lp->lp_last_alive)
                when = lp->lp_last_alive;

        lnet_notify_locked(lp, ni == NULL, alive, when);

	lnet_ni_notify_locked(ni, lp);

	lnet_peer_decref_locked(lp);

	lnet_net_unlock(cpt);
	return 0;
}
EXPORT_SYMBOL(lnet_notify);

void
lnet_get_tunables (void)
{
        return;
}

#else

int
lnet_notify (lnet_ni_t *ni, lnet_nid_t nid, int alive, cfs_time_t when)
{
        return -EOPNOTSUPP;
}

void
lnet_router_checker (void)
{
        static time_t last = 0;
        static int    running = 0;

        time_t            now = cfs_time_current_sec();
        int               interval = now - last;
        int               rc;
        __u64             version;
        lnet_peer_t      *rtr;

        /* It's no use to call me again within a sec - all intervals and
         * timeouts are measured in seconds */
        if (last != 0 && interval < 2)
                return;

        if (last != 0 &&
            interval > MAX(live_router_check_interval,
                           dead_router_check_interval))
                CNETERR("Checker(%d/%d) not called for %d seconds\n",
                        live_router_check_interval, dead_router_check_interval,
                        interval);

	LASSERT(LNET_CPT_NUMBER == 1);

	lnet_net_lock(0);
	LASSERT(!running); /* recursion check */
	running = 1;
	lnet_net_unlock(0);

	last = now;

	if (the_lnet.ln_rc_state == LNET_RC_STATE_STOPPING)
		lnet_prune_rc_data(0); /* unlink all rcd and nowait */

        /* consume all pending events */
        while (1) {
                int          i;
                lnet_event_t ev;

                /* NB ln_rc_eqh must be the 1st in 'eventqs' otherwise the
                 * recursion breaker in LNetEQPoll would fail */
                rc = LNetEQPoll(&the_lnet.ln_rc_eqh, 1, 0, &ev, &i);
                if (rc == 0)   /* no event pending */
                        break;

                /* NB a lost SENT prevents me from pinging a router again */
                if (rc == -EOVERFLOW) {
                        CERROR("Dropped an event!!!\n");
                        abort();
                }

                LASSERT (rc == 1);

                lnet_router_checker_event(&ev);
        }

	if (the_lnet.ln_rc_state == LNET_RC_STATE_STOPPING) {
		lnet_prune_rc_data(1); /* release rcd */
		the_lnet.ln_rc_state = LNET_RC_STATE_SHUTDOWN;
                running = 0;
                return;
        }

        LASSERT (the_lnet.ln_rc_state == LNET_RC_STATE_RUNNING);

	lnet_net_lock(0);

        version = the_lnet.ln_routers_version;
        cfs_list_for_each_entry (rtr, &the_lnet.ln_routers, lp_rtr_list) {
                lnet_ping_router_locked(rtr);
                LASSERT (version == the_lnet.ln_routers_version);
        }

	lnet_net_unlock(0);

	running = 0; /* lock only needed for the recursion check */
	return;
}

/* NB lnet_peers_start_down depends on me,
 * so must be called before any peer creation */
void
lnet_get_tunables (void)
{
        char *s;

        s = getenv("LNET_ROUTER_PING_TIMEOUT");
        if (s != NULL) router_ping_timeout = atoi(s);

        s = getenv("LNET_LIVE_ROUTER_CHECK_INTERVAL");
        if (s != NULL) live_router_check_interval = atoi(s);

        s = getenv("LNET_DEAD_ROUTER_CHECK_INTERVAL");
        if (s != NULL) dead_router_check_interval = atoi(s);

        /* This replaces old lnd_notify mechanism */
        check_routers_before_use = 1;
        if (dead_router_check_interval <= 0)
                dead_router_check_interval = 30;
}

void
lnet_rtrpools_free(int keep_pools)
{
}

int
lnet_rtrpools_alloc(int im_a_arouter)
{
        return 0;
}

#endif
