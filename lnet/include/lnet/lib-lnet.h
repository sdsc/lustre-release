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

static inline unsigned
lnet_match_to_hash(lnet_process_id_t id, __u64 mbits)
{
        mbits += id.nid + id.pid;
        return cfs_hash_long((unsigned long)mbits, LNET_PORTAL_HASH_BITS);
}

static inline unsigned
lnet_nid_to_hash(lnet_nid_t nid)
{
        return cfs_hash_long((__u64)nid, LNET_PEER_HASH_BITS);
}

static inline lnet_obj_cpud_t *
lnet_objcd_from_cpuid(int cpuid)
{
        return cfs_sclock_index_data(the_lnet.ln_obj_lock, cpuid);
}

static inline lnet_obj_cpud_t *
lnet_objcd_current(void)
{
        return cfs_sclock_cur_data(the_lnet.ln_obj_lock);
}

static inline lnet_obj_cpud_t *
lnet_objcd_default(void)
{
        /* the last one, for lock ordering */
        return lnet_objcd_from_cpuid(LNET_CONCURRENCY - 1);
}

static inline lnet_obj_cpud_t *
lnet_objcd_from_cookie(__u64 cookie)
{
        unsigned int cpuid = (cookie >> LNET_COOKIE_BITS) & LNET_CONCUR_MASK;

        LASSERT(cpuid < LNET_CONCURRENCY);
        return lnet_objcd_from_cpuid(cpuid);
}

int lnet_net_to_cpuid_locked(__u32 net);
int lnet_net_to_cpuid(__u32 net);

static inline unsigned
lnet_nid_to_cpuid_locked(lnet_nid_t nid)
{
        int cpuid = lnet_net_to_cpuid_locked(LNET_NIDNET(nid));

        return cpuid >= 0 ? cpuid :
               cfs_sclock_key_index(the_lnet.ln_net_lock, nid);
}

static inline unsigned
lnet_nid_to_cpuid(lnet_nid_t nid)
{
        int cpuid = lnet_net_to_cpuid(LNET_NIDNET(nid));

        return cpuid >= 0 ? cpuid :
               cfs_sclock_key_index(the_lnet.ln_net_lock, nid);
}

static inline lnet_net_cpud_t *
lnet_netcd_from_nid_locked(lnet_nid_t nid)
{
        return cfs_sclock_index_data(the_lnet.ln_net_lock,
                                     lnet_nid_to_cpuid_locked(nid));
}

static inline lnet_net_cpud_t *
lnet_netcd_from_nid(lnet_nid_t nid)
{
        return cfs_sclock_index_data(the_lnet.ln_net_lock,
                                     lnet_nid_to_cpuid(nid));
}

static inline lnet_net_cpud_t *
lnet_netcd_from_cpuid(int cpuid)
{
        return cfs_sclock_index_data(the_lnet.ln_net_lock, cpuid);
}

static inline lnet_net_cpud_t *
lnet_netcd_current(void)
{
        return cfs_sclock_cur_data(the_lnet.ln_net_lock);
}

static inline lnet_net_cpud_t *
lnet_netcd_default(void)
{
        /* the last one for lock ordering */
        return lnet_netcd_from_cpuid(LNET_CONCURRENCY - 1);
}

lnet_obj_cpud_t *lnet_objcd_from_match(unsigned index, lnet_ins_pos_t pos,
                                       lnet_process_id_t id, __u64 mbits);

#define LNET_NET_ALL          ((lnet_net_cpud_t *)-1)
#define LNET_OBJ_ALL          ((lnet_obj_cpud_t *)-1)

#define lnet_netcd_for_each(i)                                  \
        cfs_sclock_for_each(i, the_lnet.ln_net_lock)

#define lnet_objcd_for_each(i)                                  \
        cfs_sclock_for_each(i, the_lnet.ln_obj_lock)

#define LNET_NET_LOCK(n)                                        \
do {                                                            \
        cfs_sclock_lock(the_lnet.ln_net_lock,                   \
                        (n) != LNET_NET_ALL ?                   \
                        (n)->lnc_cpuid : CFS_SCLOCK_EXCL);      \
} while (0)

#define LNET_NET_UNLOCK(n)                                      \
do {                                                            \
        cfs_sclock_unlock(the_lnet.ln_net_lock,                 \
                          (n) != LNET_NET_ALL ?                 \
                          (n)->lnc_cpuid : CFS_SCLOCK_EXCL);    \
} while (0)

#define LNET_NET_LOCK_MORE(n1, n2)                              \
do {                                                            \
        LASSERT((n2) != LNET_NET_ALL);                          \
        if ((n1) == LNET_NET_ALL || (n1) == (n2))               \
                break;                                          \
        LASSERT((n1)->lnc_cpuid < (n2)->lnc_cpuid);             \
        LNET_NET_LOCK(n2);                                      \
} while (0)

#define LNET_NET_UNLOCK_MORE(n1, n2)                            \
do {                                                            \
        LASSERT((n2) != LNET_NET_ALL);                          \
        if ((n1) == LNET_NET_ALL || (n1) == (n2))               \
                break;                                          \
        LNET_NET_UNLOCK(n2);                                    \
} while (0)

#define LNET_NET_LOCK_SWITCH(n1, n2)                            \
do {                                                            \
        if ((n1) == (n2))                                       \
                break;                                          \
        LNET_NET_UNLOCK(n1);                                    \
        LNET_NET_LOCK(n2);                                      \
} while (0)

#define LNET_NET_LOCK_DEFAULT()                                 \
        LNET_NET_LOCK(lnet_netcd_default())

#define LNET_NET_UNLOCK_DEFAULT()                               \
        LNET_NET_UNLOCK(lnet_netcd_default())

#define LNET_NET_LOCK_ALL()                                     \
        LNET_NET_LOCK(LNET_NET_ALL)

#define LNET_NET_UNLOCK_ALL()                                   \
        LNET_NET_UNLOCK(LNET_NET_ALL)


#define LNET_OBJ_LOCK(o)                                        \
do {                                                            \
        cfs_sclock_lock(the_lnet.ln_obj_lock,                   \
                        (o) != LNET_OBJ_ALL ?                   \
                        (o)->loc_cpuid : CFS_SCLOCK_EXCL);      \
} while (0)

#define LNET_OBJ_UNLOCK(o)                                      \
do {                                                            \
        cfs_sclock_unlock(the_lnet.ln_obj_lock,                 \
                          (o) != LNET_OBJ_ALL ?                 \
                          (o)->loc_cpuid : CFS_SCLOCK_EXCL);    \
} while (0)

#define LNET_OBJ_LOCK_MORE(o1, o2)                              \
do {                                                            \
        LASSERT((o2) != LNET_OBJ_ALL);                          \
        if ((o1) == LNET_OBJ_ALL || (o1) == (o2))               \
                break;                                          \
        LASSERT((o1)->loc_cpuid < (o2)->loc_cpuid);             \
        LNET_OBJ_LOCK(o2);                                      \
} while (0)

#define LNET_OBJ_UNLOCK_MORE(o1, o2)                            \
do {                                                            \
        LASSERT((o2) != LNET_OBJ_ALL);                          \
        if ((o1) == LNET_OBJ_ALL || (o1) == (o2))               \
                break;                                          \
        LNET_OBJ_UNLOCK(o2);                                    \
} while (0)

#define LNET_OBJ_LOCK_SWITCH(o1, o2)                            \
do {                                                            \
        if ((o1) == (o2))                                       \
                break;                                          \
        LNET_OBJ_UNLOCK(o1);                                    \
        LNET_OBJ_LOCK(o2);                                      \
} while (0)

#define LNET_OBJ_LOCK_DEFAULT()                                 \
        LNET_OBJ_LOCK(lnet_objcd_default())

#define LNET_OBJ_UNLOCK_DEFAULT()                               \
        LNET_OBJ_UNLOCK(lnet_objcd_default())

#define LNET_OBJ_LOCK_ALL()                                     \
        LNET_OBJ_LOCK(LNET_OBJ_ALL)

#define LNET_OBJ_UNLOCK_ALL()                                   \
        LNET_OBJ_UNLOCK(LNET_OBJ_ALL)

#ifdef __KERNEL__
#define LNET_MUTEX_DOWN(m)      cfs_mutex_down(m)
#define LNET_MUTEX_UP(m)        cfs_mutex_up(m)
#else
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

#define LNET_MUTEX_DOWN(m) LNET_SINGLE_THREADED_LOCK(*(m))
#define LNET_MUTEX_UP(m)   LNET_SINGLE_THREADED_UNLOCK(*(m))
# else
#define LNET_MUTEX_DOWN(m) pthread_mutex_lock(m)
#define LNET_MUTEX_UP(m)   pthread_mutex_unlock(m)
# endif
#endif

#ifdef LNET_USE_LIB_FREELIST

static inline void *
lnet_freelist_alloc (lnet_freelist_t *fl)
{
        /* ALWAYS called with liblock held */
        lnet_freeobj_t *o;

        if (cfs_list_empty (&fl->fl_list))
                return (NULL);

        o = cfs_list_entry (fl->fl_list.next, lnet_freeobj_t, fo_list);
        cfs_list_del (&o->fo_list);
        return ((void *)&o->fo_contents);
}

static inline void
lnet_freelist_free (lnet_freelist_t *fl, void *obj)
{
        /* ALWAYS called with liblock held */
        lnet_freeobj_t *o = cfs_list_entry (obj, lnet_freeobj_t, fo_contents);

        cfs_list_add (&o->fo_list, &fl->fl_list);
}


static inline lnet_eq_t *
lnet_eq_alloc (void)
{
        /* NEVER called with any held */
        lnet_obj_cpud_t *objcd = lnet_objcd_default();
        lnet_eq_t       *eq;

        LNET_OBJ_LOCK(objcd);
        eq = (lnet_eq_t *)lnet_freelist_alloc(&objcd->loc_free_eqs);
        LNET_OBJ_UNLOCK(objcd);

        return (eq);
}

static inline void
lnet_eq_free (lnet_eq_t *eq)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
        lnet_freelist_free(&lnet_objcd_default()->loc_free_eqs, eq);
}

static inline lnet_libmd_t *
lnet_md_alloc (lnet_md_t *umd)
{
        /* NEVER called with liblock held */
        lnet_obj_cpud_t *objcd = lnet_objcd_default();
        lnet_libmd_t    *md;

        LNET_OBJ_LOCK(objcd);
        md = (lnet_libmd_t *)lnet_freelist_alloc(&objcd->loc_free_mds);
        LNET_OBJ_UNLOCK(objcd);

        if (md != NULL)
                CFS_INIT_LIST_HEAD(&md->md_list);

        return (md);
}

static inline void
lnet_md_free (lnet_libmd_t *md)
{
        /* ALWAYS called with liblock held */
        lnet_freelist_free (&lnet_objcd_default()->loc_free_mds, md);
}

static inline lnet_me_t *
lnet_me_alloc (void)
{
        /* NEVER called with liblock held */
        lnet_obj_cpud_t *objcd = lnet_objcd_default();
        lnet_me_t       *me;

        LNET_OBJ_LOCK(objcd);
        me = (lnet_me_t *)lnet_freelist_alloc(&objcd->loc_free_mes);
        LNET_OBJ_UNLOCK(objcd);

        return (me);
}

static inline void
lnet_me_free (lnet_me_t *me)
{
        /* ALWAYS called with liblock held */
        lnet_freelist_free (&lnet_objcd_default()->loc_free_mes, me);
}

static inline lnet_msg_t *
lnet_msg_alloc (void)
{
        /* NEVER called with any lock held */
        lnet_net_cpud_t *netcd = lnet_netcd_default();
        lnet_msg_t      *msg;

        LNET_NET_LOCK(netcd);
        msg = (lnet_msg_t *)lnet_freelist_alloc(&netcd->lnc_free_msgs);
        LNET_NET_UNLOCK(netcd);

        if (msg != NULL) {
                /* NULL pointers, clear flags etc */
                memset (msg, 0, sizeof (*msg));
#ifdef CRAY_XT3
                msg->msg_ev.uid = LNET_UID_ANY;
#endif
        }
        return(msg);
}

static inline void
lnet_msg_free (lnet_msg_t *msg)
{
        /* ALWAYS called with LNET_NET_LOCK held */
        LASSERT(msg->msg_netcd == NULL);
        lnet_freelist_free(&lnet_netcd_default()->lnc_free_msgs, msg);
}

#else

static inline lnet_eq_t *
lnet_eq_alloc (void)
{
        /* NEVER called with any lock held */
        lnet_eq_t *eq;

        LIBCFS_ALLOC(eq, sizeof(*eq));
        return (eq);
}

static inline void
lnet_eq_free (lnet_eq_t *eq)
{
        /* ALWAYS called with LNET_OSB_LOCK held */
        LIBCFS_FREE(eq, sizeof(*eq));
}

static inline lnet_libmd_t *
lnet_md_alloc (lnet_md_t *umd)
{
        /* NEVER called with any lock held */
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
lnet_md_free (lnet_libmd_t *md)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
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
        /* NEVER called with any lock held */
        lnet_me_t *me;

        LIBCFS_ALLOC(me, sizeof(*me));
        return (me);
}

static inline void
lnet_me_free(lnet_me_t *me)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
        LIBCFS_FREE(me, sizeof(*me));
}

static inline lnet_msg_t *
lnet_msg_alloc(void)
{
        /* NEVER called with any lock held */
        lnet_msg_t *msg;

        LIBCFS_ALLOC(msg, sizeof(*msg));
        /* no need to zero, LIBCFS_ALLOC does for us */
#ifdef CRAY_XT3
        if (msg != NULL)
                msg->msg_ev.uid = LNET_UID_ANY;
#endif
        return (msg);
}

static inline void
lnet_msg_free(lnet_msg_t *msg)
{
        /* ALWAYS called with LNET_NET_LOCK held */
        LASSERT (msg->msg_netcd == NULL);
        LIBCFS_FREE(msg, sizeof(*msg));
}
#endif /* !LNET_USE_LIB_FREELIST */

extern lnet_libhandle_t *lnet_lookup_cookie(lnet_obj_cpud_t *objcd,
                                            __u64 cookie, int type);
extern void lnet_initialise_handle(lnet_obj_cpud_t *objcd,
                                   lnet_libhandle_t *lh, int type);
static inline void lnet_invalidate_handle(lnet_libhandle_t *lh)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
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
lnet_handle2eq(lnet_obj_cpud_t *objcd, lnet_handle_eq_t *handle)
{
        /* ALWAYS called with liblock held */
        lnet_libhandle_t *lh = lnet_lookup_cookie(lnet_objcd_default(),
                                                  handle->cookie,
                                                  LNET_COOKIE_TYPE_EQ);
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
lnet_handle2md(lnet_obj_cpud_t *objcd, lnet_handle_md_t *handle)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
        lnet_libhandle_t *lh = lnet_lookup_cookie(objcd, handle->cookie,
                                                  LNET_COOKIE_TYPE_MD);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_libmd_t, md_lh));
}

static inline lnet_libmd_t *
lnet_wire_handle2md(lnet_obj_cpud_t *objcd, lnet_handle_wire_t *wh)
{
        /* ALWAYS called with LNET_OBJ_LOCK held */
        lnet_libhandle_t *lh;

        if (wh->wh_interface_cookie != the_lnet.ln_interface_cookie)
                return (NULL);

        lh = lnet_lookup_cookie(objcd, wh->wh_object_cookie,
                                LNET_COOKIE_TYPE_MD);
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
lnet_handle2me(lnet_obj_cpud_t *objcd, lnet_handle_me_t *handle)
{
        /* ALWAYS called with liblock held */
        lnet_libhandle_t *lh = lnet_lookup_cookie(objcd, handle->cookie,
                                                  LNET_COOKIE_TYPE_ME);
        if (lh == NULL)
                return (NULL);

        return (lh_entry (lh, lnet_me_t, me_lh));
}

static inline int
lnet_portal_is_lazy(lnet_portal_t *ptl)
{
        return !!(ptl->ptl_options & LNET_PTL_LAZY);
}

static inline int
lnet_portal_is_unique(lnet_portal_t *ptl)
{
        return !!(ptl->ptl_options & LNET_PTL_MATCH_UNIQUE);
}

static inline int
lnet_portal_is_wildcard(lnet_portal_t *ptl)
{
        return !!(ptl->ptl_options & LNET_PTL_MATCH_WILDCARD);
}

static inline void
lnet_portal_setopt(lnet_portal_t *ptl, int opt)
{
        ptl->ptl_options |= opt;
}

static inline void
lnet_portal_unsetopt(lnet_portal_t *ptl, int opt)
{
        ptl->ptl_options &= ~opt;
}

static inline int
lnet_match_is_unique(lnet_process_id_t match_id,
                     __u64 match_bits, __u64 ignore_bits)
{
        return ignore_bits == 0 &&
               match_id.nid != LNET_NID_ANY &&
               match_id.pid != LNET_PID_ANY;
}

static inline cfs_list_t *
lnet_portal_me_head(lnet_obj_cpud_t *objcd, int index,
                    lnet_process_id_t id, __u64 mbits)
{
        lnet_portal_t       *ptl  = the_lnet.ln_portals[index];
        lnet_portal_entry_t *ptes = &objcd->loc_ptl_ents[index];

        if (lnet_portal_is_wildcard(ptl))
                return &ptes->pte_mlist;
        else if (lnet_portal_is_unique(ptl))
                return &ptes->pte_mhash[lnet_match_to_hash(id, mbits)];
        else
                return NULL;
}

extern void lnet_ptl_ent_activate(lnet_portal_t *ptl, int cpuid);
extern void lnet_ptl_ent_deactivate(lnet_portal_t *ptl, int cpuid);

extern void lnet_destroy_peer_locked(lnet_net_cpud_t *netcd, lnet_peer_t *lp);

static inline void
lnet_peer_addref_locked(lnet_peer_t *lp)
{
        LASSERT (lp->lp_refcount > 0);
        lp->lp_refcount++;
}

static inline void
lnet_peer_decref_locked(lnet_net_cpud_t *netcd, lnet_peer_t *lp)
{
        LASSERT (lp->lp_refcount > 0);
        lp->lp_refcount--;
        if (lp->lp_refcount == 0)
                lnet_destroy_peer_locked(netcd, lp);
}

static inline int
lnet_isrouter(lnet_peer_t *lp)
{
        return lp->lp_rtr_refcount != 0;
}

void lnet_ni_addref_locked(lnet_net_cpud_t *netcd,
                           lnet_ni_t *ni, int cpuid);
void lnet_ni_decref_locked(lnet_net_cpud_t *netcd,
                           lnet_ni_t *ni, int cpuid);
void lnet_ni_addref(lnet_ni_t *ni);
void lnet_ni_decref(lnet_ni_t *ni);

static inline cfs_list_t *
lnet_nid2peerhash(lnet_net_cpud_t *netcd, lnet_nid_t nid)
{
        unsigned int idx = lnet_nid_to_hash(nid);

        return &netcd->lnc_peer_hash[idx];
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

extern lnet_ni_t *lnet_nid2ni_locked(lnet_net_cpud_t *netcd,
                                     lnet_nid_t nid, int cpuid);
extern lnet_ni_t *lnet_net2ni_locked(lnet_net_cpud_t *netcd,
                                     __u32 net, int cpuid);
extern lnet_ni_t *lnet_net2ni(__u32 net);

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
void lnet_netcd_free_rtrpools(lnet_net_cpud_t *netcd);
void lnet_netcd_init_rtrpools(lnet_net_cpud_t *netcd);
int  lnet_alloc_rtrpools(int im_a_router);
lnet_remotenet_t *lnet_find_net_locked (__u32 net);

int lnet_islocalnid(lnet_nid_t nid);
int lnet_islocalnet(__u32 net);

void lnet_msg_decommit(lnet_net_cpud_t *netcd, lnet_msg_t *msg);
void lnet_build_unlink_event(lnet_libmd_t *md, lnet_event_t *ev);
void lnet_enq_event_locked(lnet_obj_cpud_t *objcd,
                           lnet_eq_t *eq, lnet_event_t *ev);
void lnet_prep_send(lnet_msg_t *msg, int type, lnet_process_id_t target,
                    unsigned int offset, unsigned int len);
int lnet_send(lnet_nid_t nid, lnet_msg_t *msg);
void lnet_return_credits_locked (lnet_net_cpud_t *netcd, lnet_msg_t *msg);
void lnet_match_blocked_msg(lnet_obj_cpud_t *objcd, lnet_libmd_t *md);
int lnet_parse (lnet_ni_t *ni, lnet_hdr_t *hdr,
                lnet_nid_t fromnid, void *private, int rdma_req);
void lnet_recv(lnet_ni_t *ni, void *private, lnet_msg_t *msg, int delayed,
               unsigned int offset, unsigned int mlen, unsigned int rlen);
lnet_msg_t *lnet_create_reply_msg (lnet_ni_t *ni, lnet_msg_t *get_msg);
void lnet_set_reply_msg_len(lnet_ni_t *ni, lnet_msg_t *msg, unsigned int len);
void lnet_finalize(lnet_ni_t *ni, lnet_msg_t *msg, int rc);
void lnet_decommit_msg(lnet_net_cpud_t *netcd, lnet_msg_t *msg);

char *lnet_msgtyp2str (int type);
void lnet_print_hdr (lnet_hdr_t * hdr);
int lnet_fail_nid(lnet_nid_t nid, unsigned int threshold);

void lnet_get_counters(lnet_counters_t *counters);
void lnet_reset_counters(void);

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

void lnet_me_unlink(lnet_obj_cpud_t *objcd, lnet_me_t *me);

void lnet_md_unlink(lnet_obj_cpud_t *objcd, lnet_libmd_t *md);
void lnet_md_deconstruct(lnet_libmd_t *lmd, lnet_md_t *umd);

void lnet_me_activate_pte(lnet_portal_t *ptl, int id);
void lnet_me_deactivate_pte(lnet_portal_t *ptl, int id);

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

int lnet_nid2peer_locked(lnet_net_cpud_t *netcd,
                         lnet_peer_t **lpp, lnet_nid_t nid);
lnet_peer_t *lnet_find_peer_locked(lnet_net_cpud_t *netcd, lnet_nid_t nid);
void lnet_clear_peer_table(void);
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
