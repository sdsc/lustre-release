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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lnet/lnet/lib-ptl.c
 *
 * portal & match routines
 *
 * Author: liang@whamcloud.com
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <lnet/lib-lnet.h>

static void
lnet_ptl_lock(struct lnet_portal *ptl, int res_locked)
{
        if (LNET_CPT_NUMBER > 1) {
                cfs_spin_lock(&ptl->ptl_lock);
        } else {
                /* just use the lnet_res_lock which is just a spinlock when
                 * CPT number is one */
                if (!res_locked)
                        lnet_res_lock(0);
        }
}

static void
lnet_ptl_unlock(struct lnet_portal *ptl, int res_locked)
{
        if (LNET_CPT_NUMBER > 1) {
                cfs_spin_unlock(&ptl->ptl_lock);
        } else {
                /* just use the global lock which is just a spinlock when
                 * CPT number is one */
                if (!res_locked)
                        lnet_res_unlock(0);
        }
}

int
lnet_ptl_type_match(struct lnet_portal *ptl, lnet_process_id_t match_id,
                    __u64 mbits, __u64 ignore_bits)
{
        int unique;

        unique = ignore_bits == 0 &&
                 match_id.nid != LNET_NID_ANY &&
                 match_id.pid != LNET_PID_ANY;

        LASSERT(!lnet_ptl_is_unique(ptl) || !lnet_ptl_is_wildcard(ptl));

        /* prefer to check w/o any lock */
        if (likely(lnet_ptl_is_unique(ptl) || lnet_ptl_is_wildcard(ptl)))
                goto match;

        /* unset, new portal */
        lnet_ptl_lock(ptl, 0);
        /* check again with lock */
        if (unlikely(lnet_ptl_is_unique(ptl) || lnet_ptl_is_wildcard(ptl))) {
                lnet_ptl_unlock(ptl, 0);
                goto match;
        }

        /* still not set */
        if (unique)
                lnet_ptl_setopt(ptl, LNET_PTL_MATCH_UNIQUE);
        else
                lnet_ptl_setopt(ptl, LNET_PTL_MATCH_WILDCARD);

        lnet_ptl_unlock(ptl, 0);

        return 1;

 match:
        if ((lnet_ptl_is_unique(ptl) && !unique) ||
            (lnet_ptl_is_wildcard(ptl) && unique))
                return 0;
        return 1;
}

void
lnet_ptl_enable_mt(struct lnet_portal *ptl, int cpt)
{
        struct lnet_match_table *mtab = ptl->ptl_mtables[cpt];
        int	i;

        /* with hold of lnet_res_lock(cpt) */
        LASSERT(lnet_ptl_is_wildcard(ptl));

        if (LNET_CPT_NUMBER == 1)
                return; /* the only match table is always enabled */

        if (likely(mtab->mt_lastpost != 0)) {
                /* match table is already enabled, just update time stamp */
                mtab->mt_lastpost = cfs_time_current_sec();
                return;
        }

        mtab->mt_lastpost = cfs_time_current_sec();

        lnet_ptl_lock(ptl, 1);

        /* NB: we want to keep them ordered so inactive match-table
         * can always been shadowed to the same active match-table */
        LASSERT(ptl->ptl_mt_nmaps < LNET_CPT_NUMBER);

        ptl->ptl_mt_maps[ptl->ptl_mt_nmaps] = cpt;
        for (i = ptl->ptl_mt_nmaps - 1; i >= 0; i--) {
                LASSERT(ptl->ptl_mt_maps[i] != cpt);
                if (ptl->ptl_mt_maps[i] < cpt)
                        break;

                /* swap to order */
                ptl->ptl_mt_maps[i + 1] = ptl->ptl_mt_maps[i];
                ptl->ptl_mt_maps[i] = cpt;
        }

        ptl->ptl_mt_nmaps++;

        lnet_ptl_unlock(ptl, 1);
}

void
lnet_ptl_disable_mt(struct lnet_portal *ptl, int cpt)
{
        struct lnet_match_table *mtab = ptl->ptl_mtables[cpt];
	int	i;

        /* with hold of lnet_res_lock(cpt) */
        LASSERT(lnet_ptl_is_wildcard(ptl));

        if (LNET_CPT_NUMBER == 1)
                return; /* always enable the only match table */

        if (mtab->mt_lastpost == 0 || /* already disabled */
            (cfs_time_current_sec() - mtab->mt_lastpost < LNET_MT_DEADLINE))
                return;

        mtab->mt_lastpost = 0;

        lnet_ptl_lock(ptl, 1);

        LASSERT(ptl->ptl_mt_nmaps > 0 &&
                ptl->ptl_mt_nmaps <= LNET_CPT_NUMBER);

        ptl->ptl_mt_nmaps--;

        for (i = 0; i < ptl->ptl_mt_nmaps; i++) {
                if (ptl->ptl_mt_maps[i] >= cpt) /* overwrite it */
                        ptl->ptl_mt_maps[i] = ptl->ptl_mt_maps[i + 1];
        }

        lnet_ptl_unlock(ptl, 1);
}

static struct lnet_match_table *
lnet_ptl_umatch2mt(struct lnet_portal *ptl, lnet_process_id_t id, __u64 mbits)
{
        /* if it's a unique portal, return match-table hashed by NID */
	/* NB: lnet_cpt_of_nid() might take lnet_net_lock */
        return lnet_ptl_is_unique(ptl) ?
               ptl->ptl_mtables[lnet_cpt_of_nid(id.nid)] : NULL;
}

struct lnet_match_table *
lnet_mt_of_attach(struct lnet_portal *ptl, lnet_process_id_t id,
                  __u64 mbits, lnet_ins_pos_t pos)
{
        struct lnet_match_table *mtable;

        if (LNET_CPT_NUMBER == 1)
                return ptl->ptl_mtables[0]; /* the only one */

        mtable = lnet_ptl_umatch2mt(ptl, id, mbits);
        if (mtable != NULL) /* unique portal */
                return mtable;

        /* it's a wildcard portal */
        switch (pos) {
        default:
                LBUG();

        case LNET_INS_BEFORE:
        case LNET_INS_AFTER:
                /* posted by no affinity thread, always hash to specific
                 * match-table to avoid buffer stealing which is heavy */
                return ptl->ptl_mtables[ptl->ptl_index % LNET_CPT_NUMBER];

        case LNET_INS_LOCAL:
                /* posted by cpu-affinity thread */
                return ptl->ptl_mtables[lnet_cpt_current()];
        }
}

struct lnet_match_table *
lnet_mt_of_match(struct lnet_portal *ptl, lnet_process_id_t id,
                 __u64 mbits, unsigned int intent)
{
	struct lnet_match_table *mtable;
	int	nmaps;
	int	cpt;

        if (LNET_CPT_NUMBER == 1)
                return ptl->ptl_mtables[0]; /* the only one */

        mtable = lnet_ptl_umatch2mt(ptl, id, mbits);
        if (mtable != NULL)
                return mtable;

        /* wildcard portal */
        if (intent == 0)
                cpt = lnet_cpt_current();
        else
                cpt = (intent & LNET_PUT_IT_MASK) % LNET_CPT_NUMBER;

        /* portal entry on current CPU is active? */
        if (ptl->ptl_mtables[cpt]->mt_lastpost == 0) {
                /* is there any active entry for this portal? */
                nmaps = ptl->ptl_mt_nmaps;
                /* map to an active mtable to avoid heavy "stealing" */
                if (nmaps != 0) {
			/* NB: there is possibility that ptl_mt_maps is
			 * changed because we are not under protection of
			 * lnet_ptl_lock, but it shouldn't hurt anything */
                        cpt = ptl->ptl_rotor++;
                        cpt = ptl->ptl_mt_maps[cpt % nmaps];
                }
        }

        return ptl->ptl_mtables[cpt];
}

cfs_list_t *
lnet_mt_list_head(struct lnet_match_table *mtable,
                  lnet_process_id_t id, __u64 mbits)
{
        struct lnet_portal *ptl = the_lnet.ln_portals[mtable->mt_portal];

        if (lnet_ptl_is_wildcard(ptl)) {
                return &mtable->mt_mlist;

        } else if (lnet_ptl_is_unique(ptl)) {
                unsigned long hash = mbits + id.nid + id.pid;

                hash = cfs_hash_long(hash, LNET_MT_HASH_BITS);
                return &mtable->mt_mhash[hash];
        }
        return NULL;
}

static int
lnet_md_match_msg(int index, int op_mask, lnet_process_id_t src,
                  unsigned int rlength, unsigned int roffset,
                  __u64 match_bits, lnet_libmd_t *md, lnet_msg_t *msg)
{
        /* ALWAYS called holding the lnet_res_lock, and can't lnet_res_unlock;
         * lnet_ptl_match_blocked_msg() relies on this to avoid races */
        unsigned int  offset;
        unsigned int  mlength;
        lnet_me_t    *me = md->md_me;

        /* MD exhausted */
        if (lnet_md_exhausted(md))
                return LNET_MATCHMD_NONE | LNET_MATCHMD_EXHAUSTED;

        /* mismatched MD op */
        if ((md->md_options & op_mask) == 0)
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
                LASSERT(md->md_offset + mlength <= md->md_length);
        } else {
                mlength = md->md_length - offset;
        }

        if (rlength <= mlength) { /* fits in allowed space */
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

        lnet_msg_attach_md(msg, md, offset, mlength);
        md->md_offset = offset + mlength;

        if (!lnet_md_exhausted(md))
                return LNET_MATCHMD_OK;

        /* Auto-unlink NOW, so the ME gets unlinked if required.
         * We bumped md->md_refcount above so the MD just gets flagged
         * for unlink when it is finalized. */
        if ((md->md_flags & LNET_MD_FLAG_AUTO_UNLINK) != 0)
                lnet_md_unlink(md, lnet_cpt_of_cookie(md->md_lh.lh_cookie));

        return LNET_MATCHMD_OK | LNET_MATCHMD_EXHAUSTED;
}

int
lnet_mt_match_msg(struct lnet_match_table *mtable,
                  int op_mask, lnet_process_id_t src,
                  unsigned int rlength, unsigned int roffset,
                  __u64 mbits, lnet_msg_t *msg)
{
        cfs_list_t	*head = lnet_mt_list_head(mtable, src, mbits);
	lnet_me_t	*tmp;
	lnet_me_t	*me;
        int		exhausted = LNET_MATCHMD_EXHAUSTED;
        int		rc;

        CDEBUG (D_NET, "Request from %s of length %d into portal %d "
                "MB="LPX64"\n", libcfs_id2str(src),
                rlength, mtable->mt_portal, mbits);

        if (head == NULL || cfs_list_empty(head))
                return LNET_MATCHMD_NONE | exhausted;

        cfs_list_for_each_entry_safe(me, tmp, head, me_list) {
                lnet_libmd_t *md = me->me_md;

                /* ME attached but MD not attached yet */
                if (md == NULL)
                        continue;

                LASSERT(me == md->md_me);

                rc = lnet_md_match_msg(mtable->mt_portal, op_mask, src,
                                       rlength, roffset, mbits, md,  msg);
                if ((rc & LNET_MATCHMD_EXHAUSTED) == 0)
                        exhausted = 0; /* matchlist is not empty */

                if ((rc & LNET_MATCHMD_FINISH) != 0) {
                        /* don't return EXHAUSTED bits because we don't know
                         * whether the list is empty or not */
                        return rc & ~LNET_MATCHMD_EXHAUSTED;
                }
        }

        /* LNET_MATCHMD_NONE */
        if (op_mask == LNET_MD_OP_GET ||
            !lnet_ptl_is_lazy(the_lnet.ln_portals[mtable->mt_portal]))
                return LNET_MATCHMD_DROP | exhausted;
        else
                return LNET_MATCHMD_NONE | exhausted;
}

int
lnet_ptl_match_msg(struct lnet_portal *ptl, int op_mask,
		   lnet_process_id_t src, unsigned int rlength,
		   unsigned int roffset, __u64 mbits, lnet_msg_t *msg)
{
        int rc = LNET_MATCHMD_NONE;
        int stealing = 0;
        int ncpts;
        int cpt;
        int i;

	/* match buffers on all CPTs of a portal, if there isn't any matched
	 * buffer then put the message on delayed list.
	 * It has more locking overhead and it should be rare */
        LASSERT(op_mask == LNET_MD_OP_PUT); /* PUT only */

        if (lnet_ptl_is_unique(ptl)) {
                cpt = lnet_mt_of_match(ptl, src, mbits, 0)->mt_cpt;
                ncpts = 1;

        } else {
                int nmaps = ptl->ptl_mt_nmaps;

                cpt = lnet_cpt_current();
                /* map to an active match-table w/o ptl_lock */
                if (nmaps != 0)
                        cpt = ptl->ptl_mt_maps[cpt % nmaps];
                ncpts = LNET_CPT_NUMBER;
        }

        CFS_INIT_LIST_HEAD(&msg->msg_list);

        for (i = 0; i < ncpts; i++) {
                struct lnet_match_table *mtab;
                int tmp;

                tmp = (cpt + i) % LNET_CPT_NUMBER;
		mtab = ptl->ptl_mtables[tmp];
                if (mtab->mt_lastpost == 0 && i != ncpts - 1)
                        continue;

                lnet_res_lock(tmp);
                lnet_ptl_lock(ptl, 1);

                if (!stealing) {
			/* attach the message on stealing list, so other
			 * threads can see it and match it while posting
			 * new buffer */
                        stealing = 1;
                        cfs_list_add_tail(&msg->msg_list,
                                          &ptl->ptl_msg_stealing);
                }

                if (!cfs_list_empty(&msg->msg_list)) { /* on stealing list */
                        rc = lnet_mt_match_msg(mtab, op_mask, src, rlength,
                                               roffset, mbits, msg);
                        if ((rc & LNET_MATCHMD_FINISH) != 0)
                                cfs_list_del_init(&msg->msg_list);

                } else { /* another thread has matched the message for me */
                        rc = msg->msg_md == NULL ?
                             LNET_MATCHMD_DROP : LNET_MATCHMD_OK;
                }

                if (cfs_list_empty(&msg->msg_list)) { /* match or drop */
                        lnet_ptl_unlock(ptl, 1);
                        lnet_res_unlock(tmp);
                        return rc;
                }
		/* no match */

                if (i == ncpts - 1) { /* the last match-table */
                        rc &= LNET_MATCHMD_EXHAUSTED; /* clear other flags */
                        if (lnet_ptl_is_lazy(ptl)) {
                                cfs_list_move_tail(&msg->msg_list,
                                                   &ptl->ptl_msg_delayed);
                                rc |= LNET_MATCHMD_NONE;
                        } else {
				/* failed to steal buffer, drop it */
                                cfs_list_del_init(&msg->msg_list);
                                rc |= LNET_MATCHMD_DROP;
                        }
                }

                lnet_ptl_unlock(ptl, 1);

                if (lnet_ptl_is_wildcard(ptl) &&
                    (rc & LNET_MATCHMD_EXHAUSTED) != 0)
                        lnet_ptl_disable_mt(ptl, tmp);

                lnet_res_unlock(tmp);
        }

        return rc;
}

static int
lnet_md_match_msg_list(lnet_libmd_t *md, cfs_list_t *msg_head,
                       cfs_list_t *matches, cfs_list_t *drops)
{
        lnet_msg_t     *msg;
        lnet_msg_t     *tmp;
        int             rc;
        int             exhausted = 0;

        /* called with lnet_res_lock held */
        cfs_list_for_each_entry_safe(msg, tmp, msg_head, msg_list) {
                lnet_hdr_t        *hdr = &msg->msg_hdr;
                lnet_process_id_t  src = {0};

                LASSERT(hdr->type == LNET_MSG_PUT);

                src.nid = hdr->src_nid;
                src.pid = hdr->src_pid;

                rc = lnet_md_match_msg(hdr->msg.put.ptl_index,
                                       LNET_MD_OP_PUT, src,
                                       hdr->payload_length,
                                       hdr->msg.put.offset,
                                       hdr->msg.put.match_bits, md, msg);

                exhausted = (rc & LNET_MATCHMD_EXHAUSTED) != 0;
                if ((rc & LNET_MATCHMD_NONE) != 0 && !exhausted)
                        continue;

                /* Hurrah! This _is_ a match */
                cfs_list_del_init(&msg->msg_list);

                if (((rc & LNET_MATCHMD_DROP) != 0) && drops != NULL)
                        cfs_list_add_tail(&msg->msg_list, drops);
                else if (((rc & LNET_MATCHMD_OK) != 0) && matches != NULL)
                        cfs_list_add_tail(&msg->msg_list, matches);

                if (exhausted)
                        break;
        }

        return exhausted;
}

void
lnet_ptl_match_blocked_msg(struct lnet_portal *ptl, lnet_libmd_t *md,
                           cfs_list_t *matches, cfs_list_t *drops)
{
        lnet_me_t        *me = md->md_me;
        int               exhausted = 0;

        LASSERT(me->me_portal < the_lnet.ln_nportals);
        LASSERT(md->md_refcount == 0); /* a brand new MD */

        /* no protection peek */
        if (cfs_list_empty(&ptl->ptl_msg_stealing) && /* nobody steal */
            cfs_list_empty(&ptl->ptl_msg_delayed))    /* no blocked message */
                return;

        /* portal operations are serialized by locking lnet_ptl_lock */
        lnet_ptl_lock(ptl, 1);

        /* satisfy stealing list firstly, because the stealer
         * is still looping for buffer */
        if (!cfs_list_empty(&ptl->ptl_msg_stealing)) {
                /* NB: I wouldn't return messages, because the stealer
		 * will take them, see details in lnet_ptl_match_msg */
                exhausted = lnet_md_match_msg_list(md, &ptl->ptl_msg_stealing,
                                                   NULL, NULL);
        }

        if (!exhausted && !cfs_list_empty(&ptl->ptl_msg_delayed)) {
		LASSERT(lnet_ptl_is_lazy(ptl));
		lnet_md_match_msg_list(md, &ptl->ptl_msg_delayed,
				       matches, drops);
	}

        lnet_ptl_unlock(ptl, 1);
}

void
lnet_ptl_cleanup(struct lnet_portal *ptl)
{
	struct lnet_match_table *mtable;
        lnet_me_t		*me;
        int			i;
        int			j;

        LASSERT(cfs_list_empty(&ptl->ptl_msg_stealing));
        LASSERT(cfs_list_empty(&ptl->ptl_msg_delayed));

        /* cleanup ME */
        if (ptl->ptl_mtables == NULL)
                return;

        cfs_percpt_for_each(mtable, i, ptl->ptl_mtables) {
                if (mtable->mt_mhash == NULL)
                        continue;

                while (!cfs_list_empty(&mtable->mt_mlist)) {
                        me = cfs_list_entry(mtable->mt_mlist.next,
                                            lnet_me_t, me_list);
                        CERROR("Active wildcard ME %p on exit\n", me);
                        cfs_list_del(&me->me_list);
                        lnet_me_free(me);
                }

                for (j = 0; j < LNET_MT_HASH_SIZE; j++) {
                        while (!cfs_list_empty(&mtable->mt_mhash[j])) {
                                me = cfs_list_entry(mtable->mt_mhash[j].next,
                                               lnet_me_t, me_list);
                                CERROR("Active unique ME %p on exit\n", me);
                                cfs_list_del(&me->me_list);
                                lnet_me_free(me);
                        }
                }

                LIBCFS_FREE(mtable->mt_mhash,
                            LNET_MT_HASH_SIZE * sizeof(mtable->mt_mhash[0]));
                mtable->mt_mhash = NULL;
        }

        cfs_percpt_free(ptl->ptl_mtables);
        ptl->ptl_mtables = NULL;
}

int
lnet_ptl_setup(struct lnet_portal *ptl, int index)
{
        struct lnet_match_table *mtable;
        int	i;

        ptl->ptl_index = index;
        cfs_spin_lock_init(&ptl->ptl_lock);
        CFS_INIT_LIST_HEAD(&ptl->ptl_msg_stealing);
        CFS_INIT_LIST_HEAD(&ptl->ptl_msg_delayed);

        ptl->ptl_mtables = cfs_percpt_alloc(lnet_cpt_table(),
                                            sizeof(struct lnet_match_table));
        if (ptl->ptl_mtables == NULL) {
                CERROR("Failed to create match table for portal %d\n", index);
                return -ENOMEM;
        }

        cfs_percpt_for_each(mtable, i, ptl->ptl_mtables) {
                cfs_list_t *mhash;
                int         j;

                LIBCFS_CPT_ALLOC(mhash, lnet_cpt_table(), i,
                                 sizeof(mhash[0]) * LNET_MT_HASH_SIZE);
                if (mhash == NULL)
                        goto failed;

                for (j = 0; j < LNET_MT_HASH_SIZE; j++)
                        CFS_INIT_LIST_HEAD(&mhash[j]);

                CFS_INIT_LIST_HEAD(&mtable->mt_mlist);
                mtable->mt_portal = index;
                mtable->mt_mhash = mhash;
                mtable->mt_cpt = i;
        }

        return 0;
 failed:
        lnet_ptl_cleanup(ptl);
        return -ENOMEM;
}

void
lnet_portals_destroy(void)
{
        int	i;

        if (the_lnet.ln_portals == NULL)
                return;

        for(i = 0 ; i < the_lnet.ln_nportals; i++)
                lnet_ptl_cleanup(the_lnet.ln_portals[i]);

        cfs_array_free(the_lnet.ln_portals);
        the_lnet.ln_portals = NULL;
}

int
lnet_portals_create(void)
{
        int	size;
        int	i;

        size = offsetof(struct lnet_portal, ptl_mt_maps[LNET_CPT_NUMBER]);

        the_lnet.ln_nportals = MAX_PORTALS;
        the_lnet.ln_portals = cfs_array_alloc(the_lnet.ln_nportals, size);
        if (the_lnet.ln_portals == NULL) {
                CERROR("Failed to allocate portals table\n");
                return -ENOMEM;
        }

        for(i = 0 ; i < the_lnet.ln_nportals; i++){
                if (lnet_ptl_setup(the_lnet.ln_portals[i], i)) {
                        lnet_portals_destroy();
                        return -ENOMEM;
                }
        }

        return 0;
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
        struct lnet_portal  *ptl;

        if (portal < 0 || portal >= the_lnet.ln_nportals)
                return -EINVAL;

        CDEBUG(D_NET, "Setting portal %d lazy\n", portal);

        ptl = the_lnet.ln_portals[portal];

        lnet_ptl_lock(ptl, 0);
        lnet_ptl_setopt(ptl, LNET_PTL_LAZY);
        lnet_ptl_unlock(ptl, 0);

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
        CFS_LIST_HEAD		(zombies);
        struct lnet_portal	*ptl;

        if (portal < 0 || portal >= the_lnet.ln_nportals)
                return -EINVAL;

        ptl = the_lnet.ln_portals[portal];

        lnet_ptl_lock(ptl, 0);

        if (!lnet_ptl_is_lazy(ptl)) {
                lnet_ptl_unlock(ptl, 0);
                return 0;
        }

        lnet_ptl_unsetopt(ptl, LNET_PTL_LAZY);
        cfs_list_splice_init(&ptl->ptl_msg_delayed, &zombies);

        lnet_ptl_unlock(ptl, 0);

        lnet_drop_delayed_msg_list(&zombies, "Clearing lazy portal attr");

        if (the_lnet.ln_shutdown)
                CWARN ("Active lazy portal %d on exit\n", portal);
        else
                CDEBUG (D_NET, "clearing portal %d lazy\n", portal);

        return 0;
}
