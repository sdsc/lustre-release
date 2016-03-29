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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lnet/lnet/peer.c
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>
#include <lnet/lib-dlc.h>

int
lnet_peer_tables_create(void)
{
	struct lnet_peer_table	*ptable;
	struct list_head	*hash;
	int			i;
	int			j;

	the_lnet.ln_peer_tables = cfs_percpt_alloc(lnet_cpt_table(),
						   sizeof(*ptable));
	if (the_lnet.ln_peer_tables == NULL) {
		CERROR("Failed to allocate cpu-partition peer tables\n");
		return -ENOMEM;
	}

	cfs_percpt_for_each(ptable, i, the_lnet.ln_peer_tables) {
		INIT_LIST_HEAD(&ptable->pt_deathrow);

		LIBCFS_CPT_ALLOC(hash, lnet_cpt_table(), i,
				 LNET_PEER_HASH_SIZE * sizeof(*hash));
		if (hash == NULL) {
			CERROR("Failed to create peer hash table\n");
			lnet_peer_tables_destroy();
			return -ENOMEM;
		}

		for (j = 0; j < LNET_PEER_HASH_SIZE; j++)
			INIT_LIST_HEAD(&hash[j]);
		ptable->pt_hash = hash; /* sign of initialization */
	}

	return 0;
}

void
lnet_peer_tables_destroy(void)
{
	struct lnet_peer_table	*ptable;
	struct list_head	*hash;
	int			i;
	int			j;

	if (the_lnet.ln_peer_tables == NULL)
		return;

	cfs_percpt_for_each(ptable, i, the_lnet.ln_peer_tables) {
		hash = ptable->pt_hash;
		if (hash == NULL) /* not intialized */
			break;

		LASSERT(list_empty(&ptable->pt_deathrow));

		ptable->pt_hash = NULL;
		for (j = 0; j < LNET_PEER_HASH_SIZE; j++)
			LASSERT(list_empty(&hash[j]));

		LIBCFS_FREE(hash, LNET_PEER_HASH_SIZE * sizeof(*hash));
	}

	cfs_percpt_free(the_lnet.ln_peer_tables);
	the_lnet.ln_peer_tables = NULL;
}

static void
lnet_peer_table_cleanup_locked(lnet_ni_t *ni, struct lnet_peer_table *ptable)
{
	int			 i;
	struct lnet_peer_ni	*lp;
	struct lnet_peer_ni	*tmp;

	for (i = 0; i < LNET_PEER_HASH_SIZE; i++) {
		list_for_each_entry_safe(lp, tmp, &ptable->pt_hash[i],
					 lpni_hashlist) {
			if (ni != NULL && ni->ni_net != lp->lpni_net)
				continue;
			list_del_init(&lp->lpni_hashlist);
			/* Lose hash table's ref */
			ptable->pt_zombies++;
			lnet_peer_decref_locked(lp);
		}
	}
}

static void
lnet_peer_table_deathrow_wait_locked(struct lnet_peer_table *ptable,
				     int cpt_locked)
{
	int	i;

	for (i = 3; ptable->pt_zombies != 0; i++) {
		lnet_net_unlock(cpt_locked);

		if (IS_PO2(i)) {
			CDEBUG(D_WARNING,
			       "Waiting for %d zombies on peer table\n",
			       ptable->pt_zombies);
		}
		set_current_state(TASK_UNINTERRUPTIBLE);
		schedule_timeout(cfs_time_seconds(1) >> 1);
		lnet_net_lock(cpt_locked);
	}
}

static void
lnet_peer_table_del_rtrs_locked(lnet_ni_t *ni, struct lnet_peer_table *ptable,
				int cpt_locked)
{
	struct lnet_peer_ni	*lp;
	struct lnet_peer_ni	*tmp;
	lnet_nid_t		lpni_nid;
	int			i;

	for (i = 0; i < LNET_PEER_HASH_SIZE; i++) {
		list_for_each_entry_safe(lp, tmp, &ptable->pt_hash[i],
					 lpni_hashlist) {
			if (ni->ni_net != lp->lpni_net)
				continue;

			if (lp->lpni_rtr_refcount == 0)
				continue;

			lpni_nid = lp->lpni_nid;

			lnet_net_unlock(cpt_locked);
			lnet_del_route(LNET_NIDNET(LNET_NID_ANY), lpni_nid);
			lnet_net_lock(cpt_locked);
		}
	}
}

void
lnet_peer_tables_cleanup(lnet_ni_t *ni)
{
	int				i;
	struct lnet_peer_table		*ptable;
	struct lnet_peer_ni		*lp;
	struct list_head		deathrow;

	INIT_LIST_HEAD(&deathrow);

	LASSERT(the_lnet.ln_shutdown || ni != NULL);
	/* If just deleting the peers for a NI, get rid of any routes these
	 * peers are gateways for. */
	cfs_percpt_for_each(ptable, i, the_lnet.ln_peer_tables) {
		lnet_net_lock(i);
		lnet_peer_table_del_rtrs_locked(ni, ptable, i);
		lnet_net_unlock(i);
	}

	/* Start the process of moving the applicable peers to
	 * deathrow. */
	cfs_percpt_for_each(ptable, i, the_lnet.ln_peer_tables) {
		lnet_net_lock(i);
		lnet_peer_table_cleanup_locked(ni, ptable);
		lnet_net_unlock(i);
	}

	/* Cleanup all entries on deathrow. */
	cfs_percpt_for_each(ptable, i, the_lnet.ln_peer_tables) {
		lnet_net_lock(i);
		lnet_peer_table_deathrow_wait_locked(ptable, i);
		list_splice_init(&ptable->pt_deathrow, &deathrow);
		lnet_net_unlock(i);
	}

	while (!list_empty(&deathrow)) {
		lp = list_entry(deathrow.next, struct lnet_peer_ni,
				lpni_hashlist);
		list_del(&lp->lpni_hashlist);
		LIBCFS_FREE(lp, sizeof(*lp));
	}
}

static struct lnet_peer_ni *
lnet_get_peer_ni_locked(struct lnet_peer_table *ptable, lnet_nid_t nid)
{
	struct list_head	*peers;
	struct lnet_peer_ni	*lp;

	LASSERT(!the_lnet.ln_shutdown);

	peers = &ptable->pt_hash[lnet_nid2peerhash(nid)];
	list_for_each_entry(lp, peers, lpni_hashlist) {
		if (lp->lpni_nid == nid) {
			lnet_peer_addref_locked(lp);
			return lp;
		}
	}

	return NULL;
}

struct lnet_peer_ni *
lnet_find_peer_ni_locked(lnet_nid_t nid, int cpt)
{
	struct lnet_peer_ni *lpni;
	struct lnet_peer_table *ptable;

	ptable = the_lnet.ln_peer_tables[cpt];
	lpni = lnet_get_peer_ni_locked(ptable, nid);

	return lpni;
}

int
lnet_find_or_create_peer_locked(lnet_nid_t dst_nid, int cpt, struct lnet_peer **peer)
{
	struct lnet_peer_ni *lpni;

	lpni = lnet_find_peer_ni_locked(dst_nid, cpt);
	if (lpni == NULL) {
		int rc;
		rc = lnet_nid2peerni_locked(&lpni, dst_nid, cpt);
		if (rc != 0)
			return rc;
	}

	*peer = lpni->lpni_peer_net->lpn_peer;
	lnet_peer_decref_locked(lpni);

	return 0;
}

struct lnet_peer_ni *
lnet_get_next_peer_ni_locked(struct lnet_peer *peer,
			     struct lnet_peer_net *peer_net,
			     struct lnet_peer_ni *prev)
{
	struct lnet_peer_ni *lpni;
	struct lnet_peer_net *net = peer_net;

	if (prev == NULL) {
		if (net == NULL)
			net = list_entry(peer->lp_peer_nets.next,
					 struct lnet_peer_net,
					 lpn_on_peer_list);
		lpni = list_entry(net->lpn_peer_nis.next, struct lnet_peer_ni,
				  lpni_on_peer_net_list);

		return lpni;
	}

	if (prev->lpni_on_peer_net_list.next ==
	    &prev->lpni_peer_net->lpn_peer_nis) {
		/*
		 * if you reached the end of the peer ni list and the peer
		 * net is specified then thre are no more peer nis in that
		 * net.
		 */
		if (net != NULL)
			return NULL;

		/*
		 * we reached the end of this net ni list. move to the
		 * next net
		 */
		if (prev->lpni_peer_net->lpn_on_peer_list.next ==
		    &peer->lp_peer_nets)
			/* no more nets and no more NIs. */
			return NULL;

		/* get the next net */
		net = list_entry(prev->lpni_peer_net->lpn_on_peer_list.next,
				 struct lnet_peer_net,
				 lpn_on_peer_list);
		/* get the ni on it */
		lpni = list_entry(net->lpn_peer_nis.next, struct lnet_peer_ni,
				  lpni_on_peer_net_list);

		return lpni;
	}

	/* there are more nis left */
	lpni = list_entry(prev->lpni_on_peer_net_list.next,
			  struct lnet_peer_ni, lpni_on_peer_net_list);

	return lpni;
}

bool
lnet_peer_is_ni_pref_locked(struct lnet_peer_ni *lpni, struct lnet_ni *ni)
{
	int i;

	for (i = 0; i < lpni->lpni_pref_nnids; i++) {
		if (lpni->lpni_pref_nids[i] == ni->ni_nid)
			return true;
	}
	return false;
}

static void
lnet_try_destroy_peer_hierarchy_locked(struct lnet_peer_ni *lpni)
{
	struct lnet_peer_net *peer_net;
	struct lnet_peer *peer;

	/* TODO: could the below situation happen? accessing an already
	 * destroyed peer? */
	if (lpni->lpni_peer_net == NULL ||
	    lpni->lpni_peer_net->lpn_peer == NULL)
		return;

	peer_net = lpni->lpni_peer_net;
	peer = lpni->lpni_peer_net->lpn_peer;

	list_del_init(&lpni->lpni_on_peer_net_list);
	lpni->lpni_peer_net = NULL;

	/* if peer_net is empty, then remove it from the peer */
	if (list_empty(&peer_net->lpn_peer_nis)) {
		list_del_init(&peer_net->lpn_on_peer_list);
		peer_net->lpn_peer = NULL;
		LIBCFS_FREE(peer_net, sizeof(*peer_net));

		/* if the peer is empty then remove it from the
		 * the_lnet.ln_peers */
		if (list_empty(&peer->lp_peer_nets)) {
			list_del_init(&peer->lp_on_lnet_peer_list);
			LIBCFS_FREE(peer, sizeof(*peer));
		}
	}
}

static int
lnet_build_peer_hierarchy_locked(struct lnet_peer_ni *lpni)
{
	struct lnet_peer *peer;
	struct lnet_peer_net *peer_net;
	__u32 lpni_net = LNET_NIDNET(lpni->lpni_nid);

	peer = NULL;
	peer_net = NULL;

	LIBCFS_ALLOC(peer, sizeof(*peer));
	if (peer == NULL)
		return -ENOMEM;

	LIBCFS_ALLOC(peer_net, sizeof(*peer_net));
	if (peer_net == NULL) {
		LIBCFS_FREE(peer, sizeof(*peer));
		return -ENOMEM;
	}

	INIT_LIST_HEAD(&peer->lp_on_lnet_peer_list);
	INIT_LIST_HEAD(&peer->lp_peer_nets);
	INIT_LIST_HEAD(&peer_net->lpn_on_peer_list);
	INIT_LIST_HEAD(&peer_net->lpn_peer_nis);

	/* build the hierarchy */
	peer_net->lpn_net_id = lpni_net;
	peer_net->lpn_peer = peer;
	lpni->lpni_peer_net = peer_net;
	peer->lp_primary_nid = lpni->lpni_nid;
	list_add_tail(&peer_net->lpn_on_peer_list, &peer->lp_peer_nets);
	list_add_tail(&lpni->lpni_on_peer_net_list, &peer_net->lpn_peer_nis);
	list_add_tail(&peer->lp_on_lnet_peer_list, &the_lnet.ln_peers);

	return 0;
}

struct lnet_peer_net *
lnet_peer_get_net_locked(struct lnet_peer *peer, __u32 net_id)
{
	struct lnet_peer_net *peer_net;
	list_for_each_entry(peer_net, &peer->lp_peer_nets, lpn_on_peer_list) {
		if (peer_net->lpn_net_id == net_id)
			return peer_net;
	}
	return NULL;
}

/* given the key nid find the peer to add the new peer NID to. If the key
 * nid is NULL, then create a new peer, but first make sure that the NID
 * is unique */
int
lnet_add_peer_ni_to_peer(lnet_nid_t key_nid, lnet_nid_t nid)
{
	struct lnet_peer_ni *lpni, *lpni2;
	struct lnet_peer *peer;
	struct lnet_peer_net *peer_net;
	int cpt, cpt2, rc;
	struct lnet_peer_table *ptable = NULL;
	__u32 net_id = LNET_NIDNET(nid);

	if (nid == 0)
		return -EINVAL;

	/* check that nid is unique */
	cpt = lnet_cpt_of_nid_locked(nid, NULL);
	lpni = lnet_find_peer_ni_locked(nid, cpt);
	if (lpni != NULL) {
		lnet_peer_decref_locked(lpni);
		return -EEXIST;
	}

	if (key_nid != 0) {
		cpt2 = lnet_cpt_of_nid_locked(key_nid, NULL);
		lpni = lnet_find_peer_ni_locked(key_nid, cpt2);
		if (lpni == NULL)
			/* key_nid refers to a non-existant peer_ni.*/
			return -EINVAL;
		peer = lpni->lpni_peer_net->lpn_peer;
		peer->lp_multi_rail = true;
		lnet_peer_decref_locked(lpni);
	} else {
		rc = lnet_nid2peerni_locked(&lpni, nid, LNET_LOCK_EX);
		if (rc == 0) {
			lpni->lpni_peer_net->lpn_peer->lp_multi_rail = true;
			lnet_peer_decref_locked(lpni);
		}
		lnet_net_unlock(cpt);
		return rc;
	}

	lpni = NULL;

	LIBCFS_CPT_ALLOC(lpni, lnet_cpt_table(), cpt, sizeof(*lpni));
	if (lpni == NULL)
		return -ENOMEM;

	INIT_LIST_HEAD(&lpni->lpni_txq);
	INIT_LIST_HEAD(&lpni->lpni_rtrq);
	INIT_LIST_HEAD(&lpni->lpni_routes);

	lpni->lpni_alive = !lnet_peers_start_down(); /* 1 bit!! */
	lpni->lpni_last_alive = cfs_time_current(); /* assumes alive */
	lpni->lpni_ping_feats = LNET_PING_FEAT_INVAL;
	lpni->lpni_nid = nid;
	lpni->lpni_cpt = cpt2;

	lnet_net_lock(cpt);

	lpni2 = lnet_find_peer_ni_locked(cpt, nid);
	if (lpni2 != NULL) {
		lnet_peer_decref_locked(lpni2);
		/* sanity check that lpni2's peer is what we expect */
		if (lpni2->lpni_peer_net->lpn_peer != peer) {
			lnet_net_unlock(cpt);
			return -EEXIST;
		}
		/* another thread has already added it */
		lnet_net_unlock(cpt);
		return 0;
	}

	lpni->lpni_txcredits    =
	lpni->lpni_mintxcredits = lpni->lpni_net->net_peertxcredits;
	lpni->lpni_rtrcredits    =
	lpni->lpni_minrtrcredits = lnet_peer_buffer_credits(lpni->lpni_net);

	lnet_set_peer_ni_health_locked(lpni, true);
	lnet_peer_addref_locked(lpni);

	/* add the lpni to a net */
	list_for_each_entry(peer_net, &peer->lp_peer_nets, lpn_on_peer_list) {
		if (peer_net->lpn_net_id == net_id) {
			list_add_tail(&lpni->lpni_on_peer_net_list,
				      &peer_net->lpn_peer_nis);
			lnet_net_unlock(cpt);
			return 0;
		}
	}

	/* if we get here then there is no peer_net so add one */
	peer_net = NULL;
	LIBCFS_ALLOC(peer_net, sizeof(*peer_net));
	if (peer_net == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	INIT_LIST_HEAD(&peer_net->lpn_on_peer_list);
	INIT_LIST_HEAD(&peer_net->lpn_peer_nis);

	/* build the hierarchy */
	peer_net->lpn_net_id = net_id;
	peer_net->lpn_peer = peer;
	lpni->lpni_peer_net = peer_net;
	list_add_tail(&lpni->lpni_on_peer_net_list, &peer_net->lpn_peer_nis);
	list_add_tail(&peer_net->lpn_on_peer_list, &peer->lp_peer_nets);

	ptable = the_lnet.ln_peer_tables[cpt];
	list_add_tail(&lpni->lpni_hashlist,
		      &ptable->pt_hash[lnet_nid2peerhash(nid)]);
	ptable->pt_version++;

	lnet_net_unlock(cpt);
	return 0;
out:
	lnet_net_unlock(cpt);
	if (lpni != NULL)
		list_add(&lpni->lpni_hashlist, &ptable->pt_deathrow);

	return 0;
}

void
lnet_destroy_peer_ni_locked(struct lnet_peer_ni *lpni)
{
	struct lnet_peer_table *ptable;

	LASSERT(lpni->lpni_refcount == 0);
	LASSERT(lpni->lpni_rtr_refcount == 0);
	LASSERT(list_empty(&lpni->lpni_txq));
	LASSERT(list_empty(&lpni->lpni_hashlist));
	LASSERT(lpni->lpni_txqnob == 0);
	LASSERT(lpni->lpni_peer_net != NULL);
	LASSERT(lpni->lpni_peer_net->lpn_peer != NULL);

	ptable = the_lnet.ln_peer_tables[lpni->lpni_cpt];
	LASSERT(ptable->pt_number > 0);
	ptable->pt_number--;

	lpni->lpni_net = NULL;

	lnet_try_destroy_peer_hierarchy_locked(lpni);

	list_add(&lpni->lpni_hashlist, &ptable->pt_deathrow);
	LASSERT(ptable->pt_zombies > 0);
	ptable->pt_zombies--;
}

int
lnet_nid2peerni_locked(struct lnet_peer_ni **lpnip, lnet_nid_t nid, int cpt)
{
	struct lnet_peer_table	*ptable;
	struct lnet_peer_ni	*lpni = NULL;
	struct lnet_peer_ni	*lpni2;
	int			cpt2;
	int			rc = 0;

	*lpnip = NULL;
	if (the_lnet.ln_shutdown) /* it's shutting down */
		return -ESHUTDOWN;

	/* cpt can be LNET_LOCK_EX if it's called from router functions */
	cpt2 = cpt != LNET_LOCK_EX ? cpt : lnet_cpt_of_nid_locked(nid, NULL);

	ptable = the_lnet.ln_peer_tables[cpt2];
	lpni = lnet_get_peer_ni_locked(ptable, nid);
	if (lpni != NULL) {
		*lpnip = lpni;
		return 0;
	}

	if (!list_empty(&ptable->pt_deathrow)) {
		lpni = list_entry(ptable->pt_deathrow.next,
				  struct lnet_peer_ni, lpni_hashlist);
		list_del(&lpni->lpni_hashlist);
	}

	/*
	 * take extra refcount in case another thread has shutdown LNet
	 * and destroyed locks and peer-table before I finish the allocation
	 */
	ptable->pt_number++;
	lnet_net_unlock(cpt);

	if (lpni != NULL)
		memset(lpni, 0, sizeof(*lpni));
	else
		LIBCFS_CPT_ALLOC(lpni, lnet_cpt_table(), cpt2, sizeof(*lpni));

	if (lpni == NULL) {
		rc = -ENOMEM;
		lnet_net_lock(cpt);
		goto out;
	}

	INIT_LIST_HEAD(&lpni->lpni_txq);
	INIT_LIST_HEAD(&lpni->lpni_rtrq);
	INIT_LIST_HEAD(&lpni->lpni_routes);

	lpni->lpni_alive = !lnet_peers_start_down(); /* 1 bit!! */
	lpni->lpni_last_alive = cfs_time_current(); /* assumes alive */
	lpni->lpni_ping_feats = LNET_PING_FEAT_INVAL;
	lpni->lpni_nid = nid;
	lpni->lpni_cpt = cpt2;
	lpni->lpni_refcount = 2;	/* 1 for caller; 1 for hash */

	lnet_net_lock(cpt);

	if (the_lnet.ln_shutdown) {
		rc = -ESHUTDOWN;
		goto out;
	}

	lpni2 = lnet_get_peer_ni_locked(ptable, nid);
	if (lpni2 != NULL) {
		*lpnip = lpni2;
		goto out;
	}

	lpni->lpni_net = lnet_get_net_locked(LNET_NIDNET(lpni->lpni_nid));
	if (lpni->lpni_net != NULL) {
		lpni->lpni_txcredits    =
		lpni->lpni_mintxcredits = lpni->lpni_net->net_peertxcredits;
		lpni->lpni_rtrcredits    =
		lpni->lpni_minrtrcredits =
			lnet_peer_buffer_credits(lpni->lpni_net);
	} else {
		CDEBUG(D_NET, "peer_ni %s is not directly connected\n",
		       libcfs_nid2str(nid));
	}

	lnet_set_peer_ni_health_locked(lpni, true);

	rc = lnet_build_peer_hierarchy_locked(lpni);
	if (rc != 0)
		goto out;

	list_add_tail(&lpni->lpni_hashlist,
			&ptable->pt_hash[lnet_nid2peerhash(nid)]);
	ptable->pt_version++;
	*lpnip = lpni;

	return 0;
out:
	if (lpni != NULL)
		list_add(&lpni->lpni_hashlist, &ptable->pt_deathrow);
	ptable->pt_number--;
	return rc;
}

void
lnet_debug_peer(lnet_nid_t nid)
{
	char			*aliveness = "NA";
	struct lnet_peer_ni	*lp;
	int			rc;
	int			cpt;

	cpt = lnet_cpt_of_nid(nid, NULL);
	lnet_net_lock(cpt);

	rc = lnet_nid2peerni_locked(&lp, nid, cpt);
	if (rc != 0) {
		lnet_net_unlock(cpt);
		CDEBUG(D_WARNING, "No peer %s\n", libcfs_nid2str(nid));
		return;
	}

	if (lnet_isrouter(lp) || lnet_peer_aliveness_enabled(lp))
		aliveness = lp->lpni_alive ? "up" : "down";

	CDEBUG(D_WARNING, "%-24s %4d %5s %5d %5d %5d %5d %5d %ld\n",
	       libcfs_nid2str(lp->lpni_nid), lp->lpni_refcount,
	       aliveness, lp->lpni_net->net_peertxcredits,
	       lp->lpni_rtrcredits, lp->lpni_minrtrcredits,
	       lp->lpni_txcredits, lp->lpni_mintxcredits, lp->lpni_txqnob);

	lnet_peer_decref_locked(lp);

	lnet_net_unlock(cpt);
}

int lnet_get_peer_info(__u32 peer_index, __u64 *nid,
		       char aliveness[LNET_MAX_STR_LEN],
		       __u32 *cpt_iter, __u32 *refcount,
		       __u32 *ni_peer_tx_credits, __u32 *peer_tx_credits,
		       __u32 *peer_rtr_credits, __u32 *peer_min_rtr_credits,
		       __u32 *peer_tx_qnob)
{
	struct lnet_peer_table		*peer_table;
	struct lnet_peer_ni		*lp;
	int				j;
	int				lncpt;
	bool				found = false;

	/* get the number of CPTs */
	lncpt = cfs_percpt_number(the_lnet.ln_peer_tables);

	/* if the cpt number to be examined is >= the number of cpts in
	 * the system then indicate that there are no more cpts to examin
	 */
	if (*cpt_iter > lncpt)
		return -ENOENT;

	/* get the current table */
	peer_table = the_lnet.ln_peer_tables[*cpt_iter];
	/* if the ptable is NULL then there are no more cpts to examine */
	if (peer_table == NULL)
		return -ENOENT;

	lnet_net_lock(*cpt_iter);

	for (j = 0; j < LNET_PEER_HASH_SIZE && !found; j++) {
		struct list_head *peers = &peer_table->pt_hash[j];

		list_for_each_entry(lp, peers, lpni_hashlist) {
			if (peer_index-- > 0)
				continue;

			snprintf(aliveness, LNET_MAX_STR_LEN, "NA");
			if (lnet_isrouter(lp) ||
				lnet_peer_aliveness_enabled(lp))
				snprintf(aliveness, LNET_MAX_STR_LEN,
					 lp->lpni_alive ? "up" : "down");

			*nid = lp->lpni_nid;
			*refcount = lp->lpni_refcount;
			*ni_peer_tx_credits = lp->lpni_net->net_peertxcredits;
			*peer_tx_credits = lp->lpni_txcredits;
			*peer_rtr_credits = lp->lpni_rtrcredits;
			*peer_min_rtr_credits = lp->lpni_mintxcredits;
			*peer_tx_qnob = lp->lpni_txqnob;

			found = true;
		}

	}
	lnet_net_unlock(*cpt_iter);

	*cpt_iter = lncpt;

	return found ? 0 : -ENOENT;
}
