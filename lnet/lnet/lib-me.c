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
 * lnet/lnet/lib-me.c
 *
 * Match Entry management routines
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

static int
lnet_me_match_portal(lnet_portal_t *ptl, lnet_process_id_t id,
                     __u64 match_bits, __u64 ignore_bits)
{
        int unique = lnet_match_is_unique(id, match_bits, ignore_bits);;

        LASSERT(!lnet_portal_is_unique(ptl) ||
                !lnet_portal_is_wildcard(ptl));

        /* prefer to check w/o any lock */
        if (likely(lnet_portal_is_unique(ptl) ||
                   lnet_portal_is_wildcard(ptl)))
                goto match;

        /* unset, new portal */
        LNET_OBJ_LOCK(lnet_objcd_default());

        if (unlikely(lnet_portal_is_unique(ptl) ||
                     lnet_portal_is_wildcard(ptl))) {
                LNET_OBJ_UNLOCK(lnet_objcd_default());
                goto match;
        }

        /* still not set */
        if (unique)
                lnet_portal_setopt(ptl, LNET_PTL_MATCH_UNIQUE);
        else
                lnet_portal_setopt(ptl, LNET_PTL_MATCH_WILDCARD);
        LNET_OBJ_UNLOCK(lnet_objcd_default());
        return 1;
 match:
        if (lnet_portal_is_unique(ptl) && !unique)
                return 0;
        else if (lnet_portal_is_wildcard(ptl) && unique)
                return 0;
        else
                return 1;
}

/**
 * Create and attach a match entry to the match list of \a portal. The new
 * ME is empty, i.e. not associated with a memory descriptor. LNetMDAttach()
 * can be used to attach a MD to an empty ME.
 *
 * \param portal The portal table index where the ME should be attached.
 * \param match_id Specifies the match criteria for the process ID of
 * the requester. The constants LNET_PID_ANY and LNET_NID_ANY can be
 * used to wildcard either of the identifiers in the lnet_process_id_t
 * structure.
 * \param match_bits,ignore_bits Specify the match criteria to apply
 * to the match bits in the incoming request. The ignore bits are used
 * to mask out insignificant bits in the incoming match bits. The resulting
 * bits are then compared to the ME's match bits to determine if the
 * incoming request meets the match criteria.
 * \param unlink Indicates whether the ME should be unlinked when the memory
 * descriptor associated with it is unlinked (Note that the check for
 * unlinking a ME only occurs when the memory descriptor is unlinked.).
 * Valid values are LNET_RETAIN and LNET_UNLINK.
 * \param pos Indicates whether the new ME should be prepended or
 * appended to the match list. Allowed constants: LNET_INS_BEFORE,
 * LNET_INS_AFTER.
 * \param handle On successful returns, a handle to the newly created ME
 * object is saved here. This handle can be used later in LNetMEInsert(),
 * LNetMEUnlink(), or LNetMDAttach() functions.
 *
 * \retval 0       On success.
 * \retval -EINVAL If \a portal is invalid.
 * \retval -ENOMEM If new ME object cannot be allocated.
 */
int
LNetMEAttach(unsigned int portal,
             lnet_process_id_t match_id,
             __u64 match_bits, __u64 ignore_bits,
             lnet_unlink_t unlink, lnet_ins_pos_t pos,
             lnet_handle_me_t *handle)
{
        lnet_obj_cpud_t  *objcd;
        lnet_me_t        *me;
        lnet_portal_t    *ptl;
        cfs_list_t       *head;
        int               rc;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        if ((int)portal >= MAX_PORTALS)
                return -EINVAL;

        ptl = the_lnet.ln_portals[portal];
        rc = lnet_me_match_portal(ptl, match_id, match_bits, ignore_bits);
        if (!rc)
                return -EPERM;

        me = lnet_me_alloc();
        if (me == NULL)
                return -ENOMEM;

        me->me_portal      = portal;
        me->me_match_id    = match_id;
        me->me_match_bits  = match_bits;
        me->me_ignore_bits = ignore_bits;
        me->me_unlink      = unlink;
        me->me_md          = NULL;

        objcd = lnet_objcd_from_match(portal, pos, match_id, match_bits);
        LNET_OBJ_LOCK(objcd);

        if (unlikely(objcd->loc_ptl_ents[portal].pte_deadline == 0) &&
            lnet_portal_is_wildcard(ptl)) {
                LNET_OBJ_LOCK_MORE(objcd, lnet_objcd_default());
                lnet_ptl_ent_activate(the_lnet.ln_portals[portal],
                                      objcd->loc_cpuid);
                LNET_OBJ_UNLOCK_MORE(objcd, lnet_objcd_default());
        }

        objcd->loc_ptl_ents[portal].pte_deadline =
                cfs_time_shift(LNET_PTL_ENT_DEADLINE);

        lnet_initialise_handle(objcd, &me->me_lh, LNET_COOKIE_TYPE_ME);
        lnet_me2handle(handle, me);

        head = lnet_portal_me_head(objcd, portal, match_id, match_bits);
        LASSERT (head != NULL);

        if (pos == LNET_INS_AFTER || pos == LNET_INS_AFTER_CPU)
                cfs_list_add_tail(&me->me_list, head);
        else
                cfs_list_add(&me->me_list, head);

        LNET_OBJ_UNLOCK(objcd);

        return 0;
}

/**
 * Create and a match entry and insert it before or after the ME pointed to by
 * \a current_meh. The new ME is empty, i.e. not associated with a memory
 * descriptor. LNetMDAttach() can be used to attach a MD to an empty ME.
 *
 * This function is identical to LNetMEAttach() except for the position
 * where the new ME is inserted.
 *
 * \param current_meh A handle for a ME. The new ME will be inserted
 * immediately before or immediately after this ME.
 * \param match_id,match_bits,ignore_bits,unlink,pos,handle See the discussion
 * for LNetMEAttach().
 *
 * \retval 0       On success.
 * \retval -ENOMEM If new ME object cannot be allocated.
 * \retval -ENOENT If \a current_meh does not point to a valid match entry.
 */
int
LNetMEInsert(lnet_handle_me_t current_meh,
             lnet_process_id_t match_id,
             __u64 match_bits, __u64 ignore_bits,
             lnet_unlink_t unlink, lnet_ins_pos_t pos,
             lnet_handle_me_t *handle)
{
        lnet_me_t        *current_me;
        lnet_me_t        *new_me;
        lnet_obj_cpud_t  *objcd;
        lnet_portal_t    *ptl;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        new_me = lnet_me_alloc();
        if (new_me == NULL)
                return -ENOMEM;

        objcd = lnet_objcd_from_cookie(current_meh.cookie);
        LNET_OBJ_LOCK(objcd);

        current_me = lnet_handle2me(objcd, &current_meh);
        if (current_me == NULL) {
                lnet_me_free(new_me);
                LNET_OBJ_UNLOCK(objcd);
                return -ENOENT;
        }

        LASSERT (current_me->me_portal < MAX_PORTALS);

        ptl = the_lnet.ln_portals[current_me->me_portal];
        if (lnet_portal_is_unique(ptl)) {
                /* nosense to insertion on unique portal */
                lnet_me_free (new_me);
                LNET_OBJ_UNLOCK(objcd);
                return -EPERM;
        }

        new_me->me_portal      = current_me->me_portal;
        new_me->me_match_id    = match_id;
        new_me->me_match_bits  = match_bits;
        new_me->me_ignore_bits = ignore_bits;
        new_me->me_unlink      = unlink;
        new_me->me_md          = NULL;

        lnet_initialise_handle(objcd, &new_me->me_lh, LNET_COOKIE_TYPE_ME);
        lnet_me2handle(handle, new_me);

        if (pos == LNET_INS_AFTER)
                cfs_list_add(&new_me->me_list, &current_me->me_list);
        else
                cfs_list_add_tail(&new_me->me_list, &current_me->me_list);

        LNET_OBJ_UNLOCK(objcd);
        return 0;
}

/**
 * Unlink a match entry from its match list.
 *
 * This operation also releases any resources associated with the ME. If a
 * memory descriptor is attached to the ME, then it will be unlinked as well
 * and an unlink event will be generated. It is an error to use the ME handle
 * after calling LNetMEUnlink().
 *
 * \param meh A handle for the ME to be unlinked.
 *
 * \retval 0       On success.
 * \retval -ENOENT If \a meh does not point to a valid ME.
 * \see LNetMDUnlink() for the discussion on delivering unlink event.
 */
int
LNetMEUnlink(lnet_handle_me_t meh)
{
        lnet_obj_cpud_t   *objcd;
        lnet_me_t         *me;
        lnet_libmd_t      *md;
        lnet_event_t       ev;

        LASSERT (the_lnet.ln_init);
        LASSERT (the_lnet.ln_refcount > 0);

        objcd = lnet_objcd_from_cookie(meh.cookie);
        LNET_OBJ_LOCK(objcd);

        me = lnet_handle2me(objcd, &meh);
        if (me == NULL) {
                LNET_OBJ_UNLOCK(objcd);
                return -ENOENT;
        }

        md = me->me_md;
        if (md != NULL &&
            md->md_eq != NULL &&
            md->md_refcount == 0) {
                lnet_build_unlink_event(md, &ev);
                lnet_enq_event_locked(objcd, md->md_eq, &ev);
        }

        lnet_me_unlink(objcd, me);

        LNET_OBJ_UNLOCK(objcd);
        return 0;
}

/* call with LNET_OBJ_LOCK please */
void
lnet_me_unlink(lnet_obj_cpud_t *objcd, lnet_me_t *me)
{
        cfs_list_del (&me->me_list);

        if (me->me_md != NULL) {
                me->me_md->md_me = NULL;
                lnet_md_unlink(objcd, me->me_md);
        }

        lnet_invalidate_handle (&me->me_lh);
        lnet_me_free(me);
}


#if 0
static void
lib_me_dump(lnet_me_t *me)
{
        CWARN("Match Entry %p ("LPX64")\n", me,
              me->me_lh.lh_cookie);

        CWARN("\tMatch/Ignore\t= %016lx / %016lx\n",
              me->me_match_bits, me->me_ignore_bits);

        CWARN("\tMD\t= %p\n", me->md);
        CWARN("\tprev\t= %p\n",
              cfs_list_entry(me->me_list.prev, lnet_me_t, me_list));
        CWARN("\tnext\t= %p\n",
              cfs_list_entry(me->me_list.next, lnet_me_t, me_list));
}
#endif
