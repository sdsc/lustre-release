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
 * lnet/include/lnet/lib-lnet.h
 *
 * Top level include for library side routines
 */

#ifndef __LNET_LIB_LNET_H__
#define __LNET_LIB_LNET_H__

#if defined(__linux__)
#include <lnet/linux/lib-lnet.h>
#elif defined(__APPLE__)
#include <lnet/darwin/lib-lnet.h>
#elif defined(__WINNT__)
#include <lnet/winnt/lib-lnet.h>
#else
#error Unsupported Operating System
#endif

#include <libcfs/libcfs.h>
#include <lnet/types.h>
#include <lnet/lnet.h>
#include <lnet/lib-types.h>

extern lnet_t  the_lnet;                        /* THE network */

/** exclusive lock */
#define LNET_LOCK_EX		CFS_PERCPT_LOCK_EX

#define LNET_CPT_NUMBER		(the_lnet.ln_cpt_number)
#define LNET_CPT_BITS		(the_lnet.ln_cpt_bits)
#define LNET_CPT_MASK		((1UL << LNET_CPT_BITS) - 1)

#if !defined(__KERNEL__) || defined(LNET_USE_LIB_FREELIST)
/* 1 CPT, simplify implementation... */
# define LNET_CPT_MAX_BITS	0
#else
# if (BITS_PER_LONG == 32)
/* 4 CPTs, allowing more CPTs might put us under memory pressure */
#  define LNET_CPT_MAX_BITS	2
# else /* 64-bit system */
/*
 * 256 CPTs for thousands of CPUs, allowing more CPTs might put us
 * under risk of consuming lh_cooke
 */
#  define LNET_CPT_MAX_BITS	8
# endif /* BITS_PER_LONG == 32 */
#endif

/* max allowed CPT number */
#define LNET_CPT_MAX		(1 << LNET_CPT_MAX_BITS)

static inline int lnet_is_wire_handle_none (lnet_handle_wire_t *wh)
{
        return (wh->wh_interface_cookie == LNET_WIRE_HANDLE_COOKIE_NONE &&
                wh->wh_object_cookie == LNET_WIRE_HANDLE_COOKIE_NONE);
}

static inline int lnet_md_exhausted (lnet_libmd_t *md)
{
        return (md->md_threshold == 0 ||
                ((md->md_options & LNET_MD_MAX_SIZE) != 0 &&
                 md->md_offset + md->md_max_size > md->md_length));
}

static inline int lnet_md_unlinkable (lnet_libmd_t *md)
{
        /* Should unlink md when its refcount is 0 and either:
	 *  - md has been flagged for deletion (by auto unlink or
	 *    LNetM[DE]Unlink, in the latter case md may not be exhausted).
         *  - auto unlink is on and md is exhausted.
         */
        if (md->md_refcount != 0)
                return 0;

        if ((md->md_flags & LNET_MD_FLAG_ZOMBIE) != 0)
                return 1;

        return ((md->md_flags & LNET_MD_FLAG_AUTO_UNLINK) != 0 &&
                lnet_md_exhausted(md));
}

static inline struct cfs_cpt_table *
lnet_cpt_table(void)
{
	return the_lnet.ln_cpt_table;
}

static inline int
lnet_cpt_current(void)
{
	return cfs_cpt_current(the_lnet.ln_cpt_table, 1);
}

static inline int
lnet_cpt_of_cookie(__u64 cookie)
{
	unsigned int cpt = (cookie >> LNET_COOKIE_TYPE_BITS) & LNET_CPT_MASK;

	/* unlikely but LNET_CPT_NUMBER doesn't have to be power2 */
	return cpt < LNET_CPT_NUMBER ? cpt : cpt % LNET_CPT_NUMBER;
}

/* network lock */
static inline void
lnet_net_lock(int cpt)
{
	cfs_percpt_lock(the_lnet.ln_net_lock, cpt);
}

static inline void
lnet_net_unlock(int cpt)
{
	cfs_percpt_unlock(the_lnet.ln_net_lock, cpt);
}

static inline int
lnet_net_lock_current(void)
{
	int cpt = lnet_cpt_current();

	lnet_net_lock(cpt);
	return cpt;
}

/* resource lock */
/*
 * NB: there are a couple of cases that need to hold both lnet_res_lock and
 * lnet_net_lock, the locking order should be locking lnet_res_lock first,
 * and lnet_net_lock later.
 */
static inline void
lnet_res_lock(int cpt)
{
	cfs_percpt_lock(the_lnet.ln_res_lock, cpt);
}

static inline void
lnet_res_unlock(int cpt)
{
	cfs_percpt_unlock(the_lnet.ln_res_lock, cpt);
}

static inline int
lnet_res_lock_current(void)
{
	int cpt = lnet_cpt_current();

	lnet_res_lock(cpt);
	return cpt;
}

static inline void
lnet_ni_lock(lnet_ni_t *ni, int net_locked)
{
	if (LNET_CPT_NUMBER > 1) {
		cfs_spin_lock(&ni->ni_lock);
	} else { /* reuse lnet_lnet_lock to reduce locking dance overhead */
		if (!net_locked)
			lnet_net_lock(0);
	}
}

static inline void
lnet_ni_unlock(lnet_ni_t *ni, int net_locked)
{
	if (LNET_CPT_NUMBER > 1) {
		cfs_spin_unlock(&ni->ni_lock);
	} else { /* reuse lnet_lnet_lock to reduce locking dance overhead */
		if (!net_locked)
			lnet_net_unlock(0);
	}
}

#ifdef __KERNEL__

#define LNET_MUTEX_LOCK(m)   cfs_mutex_lock(m)
#define LNET_MUTEX_UNLOCK(m) cfs_mutex_unlock(m)

#else /* !__KERNEL__ */

# ifndef HAVE_LIBPTHREAD
#define LNET_SINGLE_THREADED_LOCK(l)            \
do {                                            \
        LASSERT ((l) == 0);                     \
        (l) = 1;                                \
} while (0)

#define LNET_SINGLE_THREADED_UNLOCK(l)          \
do {                                            \
        LASSERT ((l) == 1);                     \
        (l) = 0;                                \
} while (0)

#define LNET_MUTEX_LOCK(m)     LNET_SINGLE_THREADED_LOCK(*(m))
#define LNET_MUTEX_UNLOCK(m)   LNET_SINGLE_THREADED_UNLOCK(*(m))
# else
#define LNET_MUTEX_LOCK(m)     pthread_mutex_lock(m)
#define LNET_MUTEX_UNLOCK(m)   pthread_mutex_unlock(m)
# endif
#endif /* __KERNEL__ */

#define MAX_MES         2048
#define MAX_MDS         2048
#define MAX_MSGS        2048    /* Outstanding messages */
#define MAX_EQS         512

#ifdef LNET_USE_LIB_FREELIST

int lnet_freelist_init(lnet_freelist_t *fl, int n, int size);
void lnet_freelist_fini(lnet_freelist_t *fl);

static inline void *
lnet_freelist_alloc (lnet_freelist_t *fl)
{
	/* ALWAYS called with lnet_res_lock held */
        lnet_freeobj_t *o;

        if (cfs_list_empty (&fl->fl_list))
                return (NULL);

        o = cfs_list_entry (fl->fl_list.next, lnet_freeobj_t, fo_list);
        cfs_list_del (&o->fo_list);
	memset(o, 0, fl->fl_objsize);

        return ((void *)&o->fo_contents);
}

static inline void
lnet_freelist_free (lnet_freelist_t *fl, void *obj)
{
	/* ALWAYS called with lnet_res_lock held */
        lnet_freeobj_t *o = cfs_list_entry (obj, lnet_freeobj_t, fo_contents);

        cfs_list_add (&o->fo_list, &fl->fl_list);
}

static inline lnet_eq_t *
lnet_eq_alloc (void)
{
	/* NEVER called with lnet_res_lock held */
	struct lnet_res_container *rec = &the_lnet.ln_eq_container;
	lnet_eq_t		  *eq;

	LASSERT(LNET_CPT_NUMBER == 1);

	lnet_res_lock(0);
	eq = (lnet_eq_t *)lnet_freelist_alloc(&rec->rec_freelist);
	lnet_res_unlock(0);

	return eq;
}

static inline void
lnet_eq_free_locked(lnet_eq_t *eq)
{
	LASSERT(LNET_CPT_NUMBER == 1);
	/* ALWAYS called with lnet_res_lock held */
	lnet_freelist_free(&the_lnet.ln_eq_container.rec_freelist, eq);
}

static inline void
lnet_eq_free(lnet_eq_t *eq)
{
	lnet_res_lock(0);
	lnet_eq_free_locked(eq);
	lnet_res_unlock(0);
}

static inline lnet_libmd_t *
lnet_md_alloc (lnet_md_t *umd)
{
	/* NEVER called with lnet_res_lock held */
	struct lnet_res_container *rec = the_lnet.ln_md_containers[0];
	lnet_libmd_t		  *md;

	LASSERT(LNET_CPT_NUMBER == 1);

	lnet_res_lock(0);
	md = (lnet_libmd_t *)lnet_freelist_alloc(&rec->rec_freelist);
	lnet_res_unlock(0);

	if (md != NULL)
		CFS_INIT_LIST_HEAD(&md->md_list);

	return md;
}

static inline void
lnet_md_free_locked(lnet_libmd_t *md)
{
	LASSERT(LNET_CPT_NUMBER == 1);
	/* ALWAYS called with lnet_res_lock held */
	lnet_freelist_free(&the_lnet.ln_md_containers[0]->rec_freelist, md);
}

static inline void
lnet_md_free(lnet_libmd_t *md)
{
	lnet_res_lock(0);
	lnet_md_free_locked(md);
	lnet_res_unlock(0);
}

static inline lnet_me_t *
lnet_me_alloc (void)
{
        /* NEVER called with liblock held */
	struct lnet_res_container *rec = the_lnet.ln_me_containers[0];
	lnet_me_t		  *me;

	LASSERT(LNET_CPT_NUMBER == 1);

	lnet_res_lock(0);
	me = (lnet_me_t *)lnet_freelist_alloc(&rec->rec_freelist);
	lnet_res_unlock(0);

        return (me);
}

static inline void
lnet_me_free_locked(lnet_me_t *me)
{
	LASSERT(LNET_CPT_NUMBER == 1);
	/* ALWAYS called with lnet_res_lock held */
	lnet_freelist_free(&the_lnet.ln_me_containers[0]->rec_freelist, me);
}

static inline void
lnet_me_free(lnet_me_t *me)
{
	lnet_res_lock(0);
	lnet_me_free_locked(me);
	lnet_res_unlock(0);
}

static inline lnet_msg_t *
lnet_msg_alloc (void)
{
	/* NEVER called with lnet_net_lock held */
	struct lnet_msg_container *msc = the_lnet.ln_msg_containers[0];
	lnet_msg_t		  *msg;

	LASSERT(LNET_CPT_NUMBER == 1);

	lnet_net_lock(0);
	msg = (lnet_msg_t *)lnet_freelist_alloc(&msc->msc_freelist);
	lnet_net_unlock(0);

        if (msg != NULL) {
                /* NULL pointers, clear flags etc */
#ifdef CRAY_XT3
                msg->msg_ev.uid = LNET_UID_ANY;
#endif
        }
        return(msg);
}

static inline void
lnet_msg_free_locked(lnet_msg_t *msg)
{
	/* ALWAYS called with lnet_net_lock held */
	LASSERT(LNET_CPT_NUMBER == 1);
	LASSERT(!msg->msg_onactivelist);

	lnet_freelist_free(&the_lnet.ln_msg_containers[0]->msc_freelist, msg);
}

static inline void
lnet_msg_free(lnet_msg_t *msg)
{
	lnet_net_lock(0);
	lnet_msg_free_locked(msg);
	lnet_net_unlock(0);
}

#else /* LNET_USE_LIB_FREELIST */

static inline lnet_eq_t *
lnet_eq_alloc (void)
{
	/* NEVER called with lnet_res_lock held */
        lnet_eq_t *eq;

        LIBCFS_ALLOC(eq, sizeof(*eq));
        return (eq);
}

static inline void
lnet_eq_free (lnet_eq_t *eq)
{
	/* ALWAYS called with lnet_res_lock held */
        LIBCFS_FREE(eq, sizeof(*eq));
}

static inline lnet_libmd_t *
lnet_md_alloc (lnet_md_t *umd)
{
	/* NEVER called with lnet_res_lock held */
        lnet_libmd_t *md;
        unsigned int  size;
        unsigned int  niov;

        if ((umd->options & LNET_MD_KIOV) != 0) {
                niov = umd->length;
                size = offsetof(lnet_libmd_t, md_iov.kiov[niov]);
        } else {
                niov = ((umd->options & LNET_MD_IOVEC) != 0) ?
                       umd->length : 1;
                size = offsetof(lnet_libmd_t, md_iov.iov[niov]);
        }

        LIBCFS_ALLOC(md, size);

        if (md != NULL) {
                /* Set here in case of early free */
                md->md_options = umd->options;
                md->md_niov = niov;
                CFS_INIT_LIST_HEAD(&md->md_list);
        }

        return (md);
}

static inline void
lnet_md_free(lnet_libmd_t *md)
{
	/* ALWAYS called with lnet_res_lock held */
        unsigned int  size;

        if ((md->md_options & LNET_MD_KIOV) != 0)
                size = offsetof(lnet_libmd_t, md_iov.kiov[md->md_niov]);
        else
                size = offsetof(lnet_libmd_t, md_iov.iov[md->md_niov]);

        LIBCFS_FREE(md, size);
}

static inline lnet_me_t *
lnet_me_alloc (void)
{
	/* NEVER called with lnet_net_lock held */
        lnet_me_t *me;

        LIBCFS_ALLOC(me, sizeof(*me));
        return (me);
}

static inline void
lnet_me_free(lnet_me_t *me)
{
        LIBCFS_FREE(me, sizeof(*me));
}

static inline lnet_msg_t *
lnet_msg_alloc(void)
{
	/* NEVER called with lnet_net_lock held */
        lnet_msg_t *msg;

        LIBCFS_ALLOC(msg, sizeof(*msg));
        /* no need to zero, LIBCFS_ALLOC does for us */
#ifdef CRAY_XT3
        if (msg != NULL) {
                msg->msg_ev.uid = LNET_UID_ANY;
        }
#endif
        return (msg);
}

static inline void
lnet_msg_free(lnet_msg_t *msg)
{
        LASSERT (!msg->msg_onactivelist);
        LIBCFS_FREE(msg, sizeof(*msg));
}

#define lnet_eq_free_locked(eq)		lnet_eq_free(eq)
#define lnet_md_free_locked(md)		lnet_md_free(md)
#define lnet_me_free_locked(me)		lnet_me_free(me)
#define lnet_msg_free_locked(msg)	lnet_msg_free(msg)

#endif /* LNET_USE_LIB_FREELIST */

lnet_libhandle_t *lnet_res_lh_lookup(struct lnet_res_container *rec,
				     __u64 cookie);
void lnet_res_lh_initialize(struct lnet_res_container *rec,
			    lnet_libhandle_t *lh);

static inline void
lnet_res_lh_invalidate(lnet_libhandle_t *lh)
{
	/* ALWAYS called with lnet_res_lock held */
	cfs_list_del(&lh->lh_hash_chain);
}

static inline void
lnet_eq2handle (lnet_handle_eq_t *handle, lnet_eq_t *eq)
{
        if (eq == NULL) {
                LNetInvalidateHandle(handle);
                return;
        }

        handle->cookie = eq->eq_lh.lh_cookie;
}

static inline lnet_eq_t *
lnet_handle2eq (lnet_handle_eq_t *handle)
{
	/* ALWAYS called with lnet_res_lock held */
	lnet_libhandle_t *lh;

	lh = lnet_res_lh_lookup(&the_lnet.ln_eq_container, handle->cookie);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_eq_t, eq_lh));
}

static inline void
lnet_md2handle (lnet_handle_md_t *handle, lnet_libmd_t *md)
{
        handle->cookie = md->md_lh.lh_cookie;
}

static inline lnet_libmd_t *
lnet_handle2md (lnet_handle_md_t *handle)
{
	/* ALWAYS called with lnet_res_lock held */
	lnet_libhandle_t *lh;
	int		  cpt;

	cpt = lnet_cpt_of_cookie(handle->cookie);
	lh = lnet_res_lh_lookup(the_lnet.ln_md_containers[cpt],
				handle->cookie);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_libmd_t, md_lh));
}

static inline lnet_libmd_t *
lnet_wire_handle2md (lnet_handle_wire_t *wh)
{
	/* ALWAYS called with lnet_res_lock held */
	lnet_libhandle_t     *lh;
	int		      cpt;

        if (wh->wh_interface_cookie != the_lnet.ln_interface_cookie)
                return (NULL);

	cpt = lnet_cpt_of_cookie(wh->wh_object_cookie);
	lh = lnet_res_lh_lookup(the_lnet.ln_md_containers[cpt],
				wh->wh_object_cookie);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_libmd_t, md_lh));
}

static inline void
lnet_me2handle (lnet_handle_me_t *handle, lnet_me_t *me)
{
        handle->cookie = me->me_lh.lh_cookie;
}

static inline lnet_me_t *
lnet_handle2me(lnet_handle_me_t *handle)
{
	/* ALWAYS called with lnet_res_lock held */
	struct lnet_res_container *rec;
	lnet_libhandle_t	  *lh;

	rec = the_lnet.ln_me_containers[lnet_cpt_of_cookie(handle->cookie)];
	lh = lnet_res_lh_lookup(rec, handle->cookie);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_me_t, me_lh));
}

static inline int
lnet_ptl_is_lazy(struct lnet_portal *ptl)
{
	return !!(ptl->ptl_options & LNET_PTL_LAZY);
}

static inline int
lnet_ptl_is_unique(struct lnet_portal *ptl)
{
	return !!(ptl->ptl_options & LNET_PTL_MATCH_UNIQUE);
}

static inline int
lnet_ptl_is_wildcard(struct lnet_portal *ptl)
{
	return !!(ptl->ptl_options & LNET_PTL_MATCH_WILDCARD);
}

static inline void
lnet_ptl_setopt(struct lnet_portal *ptl, int opt)
{
        ptl->ptl_options |= opt;
}

static inline void
lnet_ptl_unsetopt(struct lnet_portal *ptl, int opt)
{
        ptl->ptl_options &= ~opt;
}

static inline void
lnet_peer_addref_locked(lnet_peer_t *lp)
{
        LASSERT (lp->lp_refcount > 0);
        lp->lp_refcount++;
}

extern void lnet_destroy_peer_locked(lnet_peer_t *lp);

static inline void
lnet_peer_decref_locked(lnet_peer_t *lp)
{
        LASSERT (lp->lp_refcount > 0);
        lp->lp_refcount--;
        if (lp->lp_refcount == 0)
                lnet_destroy_peer_locked(lp);
}

static inline int
lnet_isrouter(lnet_peer_t *lp)
{
        return lp->lp_rtr_refcount != 0;
}

static inline void
lnet_ni_addref_locked(lnet_ni_t *ni, int cpt)
{
	LASSERT(cpt >= 0 && cpt < LNET_CPT_NUMBER);
	LASSERT(*ni->ni_refs[cpt] >= 0);

	(*ni->ni_refs[cpt])++;
}

static inline void
lnet_ni_addref(lnet_ni_t *ni)
{
	/* NB: always take refcount on cpt == 0 */
	lnet_net_lock(0);
	lnet_ni_addref_locked(ni, 0);
	lnet_net_unlock(0);
}

static inline void
lnet_ni_decref_locked(lnet_ni_t *ni, int cpt)
{
	LASSERT(*ni->ni_refs[cpt] > 0);
	(*ni->ni_refs[cpt])--;
}

static inline void
lnet_ni_decref(lnet_ni_t *ni)
{
	/* NB: always release refcount on cpt == 0 */
	lnet_net_lock(0);
	lnet_ni_decref_locked(ni, 0);
	lnet_net_unlock(0);
}

void lnet_ni_free(lnet_ni_t *ni);

static inline int
lnet_nid2peerhash(lnet_nid_t nid)
{
	return cfs_hash_long(nid, LNET_PEER_HASH_BITS);
}

extern lnd_t the_lolnd;

#ifndef __KERNEL__
/* unconditional registration */
#define LNET_REGISTER_ULND(lnd)                 \
do {                                            \
        extern lnd_t lnd;                       \
                                                \
        lnet_register_lnd(&(lnd));              \
} while (0)

/* conditional registration */
#define LNET_REGISTER_ULND_IF_PRESENT(lnd)                              \
do {                                                                    \
        extern lnd_t lnd __attribute__ ((weak, alias("the_lolnd")));    \
                                                                        \
        if (&(lnd) != &the_lolnd)                                       \
                lnet_register_lnd(&(lnd));                              \
} while (0)
#endif

#ifdef CRAY_XT3
inline static void
lnet_set_msg_uid(lnet_ni_t *ni, lnet_msg_t *msg, lnet_uid_t uid)
{
        LASSERT (msg->msg_ev.uid == LNET_UID_ANY);
        msg->msg_ev.uid = uid;
}
#endif

extern int lnet_cpt_of_nid_locked(lnet_nid_t nid);
extern int lnet_cpt_of_nid(lnet_nid_t nid);
extern lnet_ni_t *lnet_nid2ni_locked(lnet_nid_t nid, int cpt);
extern lnet_ni_t *lnet_net2ni_locked(__u32 net, int cpt);
static inline lnet_ni_t *
lnet_net2ni (__u32 net)
{
        lnet_ni_t *ni;

	lnet_net_lock(0);
	ni = lnet_net2ni_locked(net, 0);
	lnet_net_unlock(0);

        return ni;
}

int lnet_notify(lnet_ni_t *ni, lnet_nid_t peer, int alive, cfs_time_t when);
void lnet_notify_locked(lnet_peer_t *lp, int notifylnd, int alive, cfs_time_t when);
int lnet_add_route(__u32 net, unsigned int hops, lnet_nid_t gateway_nid);
int lnet_check_routes(void);
int lnet_del_route(__u32 net, lnet_nid_t gw_nid);
void lnet_destroy_routes(void);
int lnet_get_route(int idx, __u32 *net, __u32 *hops,
                   lnet_nid_t *gateway, __u32 *alive);
void lnet_proc_init(void);
void lnet_proc_fini(void);
int  lnet_alloc_rtrpools(int im_a_router);
void lnet_free_rtrpools(void);
lnet_remotenet_t *lnet_find_net_locked (__u32 net);

int lnet_islocalnid(lnet_nid_t nid);
int lnet_islocalnet(__u32 net);

void lnet_msg_attach_md(lnet_msg_t *msg, lnet_libmd_t *md,
			unsigned int offset, unsigned int mlen);
void lnet_msg_detach_md(lnet_msg_t *msg, int status);
void lnet_build_unlink_event(lnet_libmd_t *md, lnet_event_t *ev);
void lnet_build_msg_event(lnet_msg_t *msg, lnet_event_kind_t ev_type);
void lnet_eq_enqueue_event(lnet_eq_t *eq, lnet_event_t *ev);
void lnet_prep_send(lnet_msg_t *msg, int type, lnet_process_id_t target,
                    unsigned int offset, unsigned int len);
int lnet_send(lnet_nid_t nid, lnet_msg_t *msg, lnet_nid_t rtr_nid);
void lnet_return_tx_credits_locked(lnet_msg_t *msg);
void lnet_return_rx_credits_locked(lnet_msg_t *msg);

int lnet_portals_create(void);
void lnet_portals_destroy(void);
int lnet_ptl_type_match(struct lnet_portal *ptl, lnet_process_id_t id,
			__u64 mbits, __u64 ignore_bits);
void lnet_ptl_match_blocked_msg(struct lnet_portal *ptl, lnet_libmd_t *md,
				cfs_list_t *matches, cfs_list_t *drops);
int lnet_mt_match_msg(struct lnet_match_table *mtable,
		      int op_mask, lnet_process_id_t src,
		      unsigned int rlength, unsigned int roffset,
		      __u64 mbits, lnet_msg_t *msg);
int lnet_ptl_match_msg(struct lnet_portal *ptl,
		       int op_mask, lnet_process_id_t src,
		       unsigned int rlength, unsigned int roffset,
		       __u64 mbits, lnet_msg_t *msg);
cfs_list_t *lnet_mt_list_head(struct lnet_match_table *mtable,
			      lnet_process_id_t id, __u64 mbits);

int lnet_parse (lnet_ni_t *ni, lnet_hdr_t *hdr,
                lnet_nid_t fromnid, void *private, int rdma_req);
void lnet_recv(lnet_ni_t *ni, void *private, lnet_msg_t *msg, int delayed,
               unsigned int offset, unsigned int mlen, unsigned int rlen);
lnet_msg_t *lnet_create_reply_msg (lnet_ni_t *ni, lnet_msg_t *get_msg);
void lnet_set_reply_msg_len(lnet_ni_t *ni, lnet_msg_t *msg, unsigned int len);
void lnet_msg_commit(lnet_msg_t *msg, int msg_type, int cpt);
void lnet_msg_decommit(lnet_msg_t *msg, int cpt);
void lnet_finalize(lnet_ni_t *ni, lnet_msg_t *msg, int rc);
void lnet_drop_delayed_msg_list(cfs_list_t *head, char *reason);
void lnet_recv_delayed_msg_list(cfs_list_t *head);
int lnet_msg_containers_create(void);
void lnet_msg_containers_destroy(void);

char *lnet_msgtyp2str (int type);
void lnet_print_hdr (lnet_hdr_t * hdr);
int lnet_fail_nid(lnet_nid_t nid, unsigned int threshold);

void lnet_counters_get(lnet_counters_t *counters);
void lnet_counters_reset(void);

unsigned int lnet_iov_nob (unsigned int niov, struct iovec *iov);
int lnet_extract_iov (int dst_niov, struct iovec *dst,
                      int src_niov, struct iovec *src,
                      unsigned int offset, unsigned int len);

unsigned int lnet_kiov_nob (unsigned int niov, lnet_kiov_t *iov);
int lnet_extract_kiov (int dst_niov, lnet_kiov_t *dst,
                      int src_niov, lnet_kiov_t *src,
                      unsigned int offset, unsigned int len);

void lnet_copy_iov2iov (unsigned int ndiov, struct iovec *diov,
                        unsigned int doffset,
                        unsigned int nsiov, struct iovec *siov,
                        unsigned int soffset, unsigned int nob);
void lnet_copy_kiov2iov (unsigned int niov, struct iovec *iov,
                         unsigned int iovoffset,
                         unsigned int nkiov, lnet_kiov_t *kiov,
                         unsigned int kiovoffset, unsigned int nob);
void lnet_copy_iov2kiov (unsigned int nkiov, lnet_kiov_t *kiov,
                         unsigned int kiovoffset,
                         unsigned int niov, struct iovec *iov,
                         unsigned int iovoffset, unsigned int nob);
void lnet_copy_kiov2kiov (unsigned int ndkiov, lnet_kiov_t *dkiov,
                          unsigned int doffset,
                          unsigned int nskiov, lnet_kiov_t *skiov,
                          unsigned int soffset, unsigned int nob);

static inline void
lnet_copy_iov2flat(int dlen, void *dest, unsigned int doffset,
                   unsigned int nsiov, struct iovec *siov, unsigned int soffset,
                   unsigned int nob)
{
        struct iovec diov = {/*.iov_base = */ dest, /*.iov_len = */ dlen};

        lnet_copy_iov2iov(1, &diov, doffset,
                          nsiov, siov, soffset, nob);
}

static inline void
lnet_copy_kiov2flat(int dlen, void *dest, unsigned int doffset,
                    unsigned int nsiov, lnet_kiov_t *skiov, unsigned int soffset,
                    unsigned int nob)
{
        struct iovec diov = {/* .iov_base = */ dest, /* .iov_len = */ dlen};

        lnet_copy_kiov2iov(1, &diov, doffset,
                           nsiov, skiov, soffset, nob);
}

static inline void
lnet_copy_flat2iov(unsigned int ndiov, struct iovec *diov, unsigned int doffset,
                   int slen, void *src, unsigned int soffset, unsigned int nob)
{
        struct iovec siov = {/*.iov_base = */ src, /*.iov_len = */slen};
        lnet_copy_iov2iov(ndiov, diov, doffset,
                          1, &siov, soffset, nob);
}

static inline void
lnet_copy_flat2kiov(unsigned int ndiov, lnet_kiov_t *dkiov, unsigned int doffset,
                    int slen, void *src, unsigned int soffset, unsigned int nob)
{
        struct iovec siov = {/* .iov_base = */ src, /* .iov_len = */ slen};
        lnet_copy_iov2kiov(ndiov, dkiov, doffset,
                           1, &siov, soffset, nob);
}

struct lnet_match_table *lnet_mt_of_attach(struct lnet_portal *ptl,
					   lnet_process_id_t id, __u64 mbits,
					   lnet_ins_pos_t pos);
struct lnet_match_table *lnet_mt_of_match(struct lnet_portal *ptl,
					  lnet_process_id_t id, __u64 mbits,
					  unsigned int intent);
void lnet_ptl_enable_mt(struct lnet_portal *ptl, int cpt);
void lnet_ptl_disable_mt(struct lnet_portal *ptl, int cpt);

void lnet_me_unlink(lnet_me_t *me, int cpt);

void lnet_md_unlink(lnet_libmd_t *md, int cpt);
void lnet_md_deconstruct(lnet_libmd_t *lmd, lnet_md_t *umd);

void lnet_register_lnd(lnd_t *lnd);
void lnet_unregister_lnd(lnd_t *lnd);
int lnet_set_ip_niaddr (lnet_ni_t *ni);

#ifdef __KERNEL__
int lnet_connect(cfs_socket_t **sockp, lnet_nid_t peer_nid,
                 __u32 local_ip, __u32 peer_ip, int peer_port);
void lnet_connect_console_error(int rc, lnet_nid_t peer_nid,
                                __u32 peer_ip, int port);
int lnet_count_acceptor_nis(void);
int lnet_acceptor_timeout(void);
int lnet_acceptor_port(void);
#else
void lnet_router_checker(void);
#endif

#ifdef HAVE_LIBPTHREAD
int lnet_count_acceptor_nis(void);
int lnet_acceptor_port(void);
#endif

int lnet_acceptor_start(void);
void lnet_acceptor_stop(void);

void lnet_get_tunables(void);
int lnet_peers_start_down(void);
int lnet_peer_buffer_credits(lnet_ni_t *ni);

int lnet_router_checker_start(void);
void lnet_router_checker_stop(void);
void lnet_swap_pinginfo(lnet_ping_info_t *info);
int lnet_router_down_ni(lnet_peer_t *rtr, __u32 net);

int lnet_ping_target_init(void);
void lnet_ping_target_fini(void);
int lnet_ping(lnet_process_id_t id, int timeout_ms,
              lnet_process_id_t *ids, int n_ids);

int lnet_parse_ip2nets (char **networksp, char *ip2nets);
int lnet_parse_routes (char *route_str, int *im_a_router);
int lnet_parse_networks (cfs_list_t *nilist, char *networks);

int lnet_nid2peer_locked(lnet_peer_t **lpp, lnet_nid_t nid, int cpt);
lnet_peer_t *lnet_find_peer_locked(struct lnet_peer_table *ptable,
				   lnet_nid_t nid);
int lnet_peer_tables_create(void);
void lnet_peer_tables_destroy(void);
void lnet_peer_tables_cleanup(void);
void lnet_debug_peer(lnet_nid_t nid);

#ifndef __KERNEL__
static inline int
lnet_parse_int_tunable(int *value, char *name)
{
        char    *env = getenv(name);
        char    *end;

        if (env == NULL)
                return 0;

        *value = strtoull(env, &end, 0);
        if (*end == 0)
                return 0;

        CERROR("Can't parse tunable %s=%s\n", name, env);
        return -EINVAL;
}
#endif

#endif
