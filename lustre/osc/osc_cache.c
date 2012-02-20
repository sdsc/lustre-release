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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011 Whamcloud, Inc.
 *
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * osc cache management.
 *
 * Author: Jinshan Xiong <jinshan.xiong@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_OSC

#include "osc_cl_internal.h"
#include "osc_internal.h"

static void osc_update_pending(struct osc_object *obj, int cmd, int delta);
static int osc_extent_wait(const struct lu_env *env, struct osc_extent *ext);
static void osc_ap_completion(const struct lu_env *env, struct client_obd *cli,
                              struct osc_async_page *oap, int sent, int rc);
static int osc_make_ready(const struct lu_env *env, struct osc_async_page *oap,
                          int cmd);
static int osc_refresh_count(const struct lu_env *env,
                             struct osc_async_page *oap, int cmd);
static void osc_extent_tree_dump0(int level, struct osc_object *obj,
                                  const char *func, int line);
static int osc_io_unplug_async(const struct lu_env *env,
                               struct client_obd *cli, struct osc_object *osc);
#define osc_extent_tree_dump(lvl, obj) \
        osc_extent_tree_dump0(lvl, obj, __FUNCTION__, __LINE__)

/** \addtogroup osc
 *  @{
 */

/* ------------------ osc extent ------------------ */
static inline char *ext_flags(struct osc_extent *ext, char *flags)
{
        char *buf = flags;
        *buf++ = ext->oe_rw ? 'r' : 'w';
        if (ext->oe_intree)
                *buf++ = 'i';
        if (ext->oe_srvlock)
                *buf++ = 's';
        if (ext->oe_hp)
                *buf++ = 'h';
        if (ext->oe_urgent)
                *buf++ = 'u';
        if (ext->oe_memalloc)
                *buf++ = 'm';
        *buf = 0;
        return flags;
}

static inline char list_empty_marker(cfs_list_t *list)
{
        return cfs_list_empty(list) ? '-' : '+';
}

#define EXTSTR       "[%lu -> %lu/%lu]"
#define EXTPARA(ext) (ext)->oe_start, (ext)->oe_end, (ext)->oe_max_end

#define OSC_EXTENT_DUMP(lvl, extent, fmt, ...) do {                           \
        struct osc_extent *__ext = (extent);                                  \
        const char *__str[] = OES_STRINGS;                                    \
        char __buf[16];                                                       \
                                                                              \
        CDEBUG(lvl,                                                           \
                "extent %p@{" EXTSTR ", "                                     \
                "[%d|%d|%c|%s|%s|%p], [%d|%d|%c|%c|%p|%p]} " fmt,             \
                /* ----- extent part 0 ----- */                               \
                __ext, EXTPARA(__ext),                                        \
                /* ----- part 1 ----- */                                      \
                cfs_atomic_read(&__ext->oe_refc),                             \
                cfs_atomic_read(&__ext->oe_users),                            \
                list_empty_marker(&__ext->oe_link),                           \
                __str[__ext->oe_state], ext_flags(__ext, __buf),              \
                __ext->oe_obj,                                                \
                /* ----- part 2 ----- */                                      \
                __ext->oe_grants, __ext->oe_nr_pages,                         \
                list_empty_marker(&__ext->oe_pages),                          \
                cfs_waitq_active(&__ext->oe_waitq) ? '+' : '-',               \
                __ext->oe_osclock, __ext->oe_owner,                           \
                /* ----- part 4 ----- */                                      \
                ## __VA_ARGS__);                                              \
} while (0)

#undef EASSERTF
#define EASSERTF(expr, ext, fmt, args...) do {                                \
        if (!(expr)) {                                                        \
                OSC_EXTENT_DUMP(D_ERROR, (ext), fmt, ##args);                 \
                osc_extent_tree_dump(D_ERROR, (ext)->oe_obj);                 \
                LASSERT(expr);                                                \
        }                                                                     \
} while (0)

#undef EASSERT
#define EASSERT(expr, ext) EASSERTF(expr, ext, "")

static inline struct osc_extent *rb_extent(struct rb_node *n)
{
        if (n == NULL)
                return NULL;

        return container_of(n, struct osc_extent, oe_node);
}

static inline struct osc_extent *next_extent(struct osc_extent *ext)
{
        if (ext == NULL)
                return NULL;

        LASSERT(ext->oe_intree);
        return rb_extent(rb_next(&ext->oe_node));
}

static inline struct osc_extent *prev_extent(struct osc_extent *ext)
{
        if (ext == NULL)
                return NULL;

        LASSERT(ext->oe_intree);
        return rb_extent(rb_prev(&ext->oe_node));
}

static inline struct osc_extent *first_extent(struct osc_object *obj)
{
        return rb_extent(rb_first(&obj->oo_root));
}

static int osc_extent_sanity_check0(struct osc_extent *ext, int locked,
                                    const char *func, const int line)
{
        struct osc_object *obj = ext->oe_obj;
        struct osc_async_page *oap;
        int page_count;
        int rc = 0;

        if (!locked)
                osc_object_lock(obj);
        else if (!osc_object_is_locked(obj))
                GOTO(out, rc = 9);

        if (ext->oe_state >= OES_STATE_MAX)
                GOTO(out, rc = 10);

        if (cfs_atomic_read(&ext->oe_refc) <= 0)
                GOTO(out, rc = 20);

        if (cfs_atomic_read(&ext->oe_refc) < cfs_atomic_read(&ext->oe_users))
                GOTO(out, rc = 30);

        switch (ext->oe_state) {
        case OES_INV:
                if (ext->oe_nr_pages > 0 || !cfs_list_empty(&ext->oe_pages))
                        GOTO(out, rc = 35);
                GOTO(out, rc = 0);
                break;
        case OES_ACTIVE:
                if (cfs_atomic_read(&ext->oe_users) == 0)
                        GOTO(out, rc = 40);
                if (ext->oe_hp)
                        GOTO(out, rc = 50);
                break;
        case OES_CACHE:
                if (ext->oe_grants == 0)
                        GOTO(out, rc = 60);
        default:
                if (cfs_atomic_read(&ext->oe_users) > 0)
                        GOTO(out, rc = 70);
        }

        if (ext->oe_max_end < ext->oe_end || ext->oe_end < ext->oe_start)
                GOTO(out, rc = 80);

        if (ext->oe_osclock == NULL && ext->oe_grants > 0)
                GOTO(out, rc = 90);

        if (ext->oe_osclock) {
                struct cl_lock_descr *descr;
                descr = &ext->oe_osclock->cll_descr;
                if (!(descr->cld_start <= ext->oe_start &&
                      descr->cld_end >= ext->oe_max_end))
                        GOTO(out, rc = 100);
        }

        /* Do not verify page list if extent is in RPC. This is because an
         * in-RPC extent is supposed to be exclusively accessible w/o lock. */
        if (ext->oe_state > OES_CACHE)
                GOTO(out, rc = 0);

        page_count = 0;
        cfs_list_for_each_entry(oap, &ext->oe_pages, oap_pending_item) {
                pgoff_t index = oap2cl_page(oap)->cp_index;
                ++page_count;
                if (index > ext->oe_end || index < ext->oe_start)
                        GOTO(out, rc = 110);
        }
        if (page_count != ext->oe_nr_pages)
                GOTO(out, rc = 120);

out:
        if (!locked)
                osc_object_unlock(obj);
        if (rc != 0)
                OSC_EXTENT_DUMP(D_ERROR, ext,
                                "%s:%d sanity check %p failed with rc = %d\n",
                                func, line, ext, rc);
        return rc;
}

#define sanity_check_nolock(ext) \
        osc_extent_sanity_check0(ext, 1, __FUNCTION__, __LINE__)
#define sanity_check(ext) \
        osc_extent_sanity_check0(ext, 0, __FUNCTION__, __LINE__)

static void osc_extent_state_set(struct osc_extent *ext, int state)
{
        LASSERT(osc_object_is_locked(ext->oe_obj));
        LASSERT(state >= OES_INV && state < OES_STATE_MAX);

        /* Never try to sanity check a state changing extent :-) */
        /* LASSERT(sanity_check_nolock(ext) == 0); */

        /* TODO: validate the state machine */
        ext->oe_state = state;
}

static struct osc_extent *osc_extent_alloc(struct osc_object *obj)
{
        struct osc_extent *ext;

        OBD_SLAB_ALLOC_PTR_GFP(ext, osc_extent_kmem, CFS_ALLOC_STD);
        if (ext == NULL)
                return NULL;

        RB_CLEAR_NODE(&ext->oe_node);
        ext->oe_obj = obj;
        cfs_atomic_set(&ext->oe_refc, 1);
        cfs_atomic_set(&ext->oe_users, 0);
        CFS_INIT_LIST_HEAD(&ext->oe_link);
        ext->oe_state = OES_INV;
        CFS_INIT_LIST_HEAD(&ext->oe_pages);
        cfs_waitq_init(&ext->oe_waitq);
        ext->oe_osclock = NULL;

        return ext;
}

static void osc_extent_free(struct osc_extent *ext)
{
        OBD_SLAB_FREE_PTR(ext, osc_extent_kmem);
}

static struct osc_extent *osc_extent_get(struct osc_extent *ext)
{
        LASSERT(cfs_atomic_read(&ext->oe_refc) >= 0);
        cfs_atomic_inc(&ext->oe_refc);
        return ext;
}

static void osc_extent_put(const struct lu_env *env, struct osc_extent *ext)
{
        LASSERT(cfs_atomic_read(&ext->oe_refc) > 0);
        /* LASSERT(sanity_check(ext) == 0); */
        if (cfs_atomic_dec_and_test(&ext->oe_refc)) {
                LASSERT(cfs_list_empty(&ext->oe_link));
                LASSERT(cfs_atomic_read(&ext->oe_users) == 0);
                LASSERT(ext->oe_state == OES_INV);
                LASSERT(!ext->oe_intree);

                if (ext->oe_osclock) {
                        cl_lock_put(env, ext->oe_osclock);
                        ext->oe_osclock = NULL;
                }
                osc_extent_free(ext);
        }
}

static void osc_extent_put_trust(struct osc_extent *ext)
{
        LASSERT(cfs_atomic_read(&ext->oe_refc) > 1);
        LASSERT(osc_object_is_locked(ext->oe_obj));
        cfs_atomic_dec(&ext->oe_refc);
}

static int osc_extent_is_overlapped(struct osc_object *obj,
                                    struct osc_extent *ext)
{
        struct osc_extent *tmp;

        LASSERT(osc_object_is_locked(obj));
        for (tmp = first_extent(obj); tmp != NULL; tmp = next_extent(tmp)) {
                if (tmp == ext)
                        continue;
                if (tmp->oe_end >= ext->oe_start &&
                    tmp->oe_start <= ext->oe_end)
                        return 1;
        }
        return 0;
}

/**
 * Return the extent which includes pgoff @index, or return the greatest
 * previous extent in the tree.
 */
static struct osc_extent *osc_extent_search(struct osc_object *obj,
                                            pgoff_t index)
{
        struct rb_node    *n = obj->oo_root.rb_node;
        struct osc_extent *tmp, *p = NULL;

        LASSERT(osc_object_is_locked(obj));
        while (n != NULL) {
                tmp = rb_extent(n);
                if (index < tmp->oe_start) {
                        n = n->rb_left;
                } else if (index > tmp->oe_end) {
                        p = rb_extent(n);
                        n = n->rb_right;
                } else {
                        return tmp;
                }
        }
        return p;
}

/*
 * Return the extent covering @index, otherwise return NULL
 */
static struct osc_extent *osc_extent_lookup(struct osc_object *obj,
                                            pgoff_t index)
{
        struct osc_extent *ext;

        ext = osc_extent_search(obj, index);
        if (ext != NULL && ext->oe_start <= index && index <= ext->oe_end)
                return osc_extent_get(ext);
        return NULL;
}

static void osc_extent_insert(struct osc_object *obj, struct osc_extent *ext)
{
        struct rb_node   **n      = &obj->oo_root.rb_node;
        struct rb_node    *parent = NULL;
        struct osc_extent *tmp;

        LASSERT(ext->oe_intree == 0);
        LASSERT(ext->oe_obj == obj);
        LASSERT(osc_object_is_locked(obj));
        while (*n != NULL) {
                tmp = rb_extent(*n);
                parent = *n;

                if (ext->oe_end < tmp->oe_start)
                        n = &(*n)->rb_left;
                else if (ext->oe_start > tmp->oe_end)
                        n = &(*n)->rb_right;
                else
                        EASSERTF(0, tmp, EXTSTR, EXTPARA(ext));
        }
        rb_link_node(&ext->oe_node, parent, n);
        rb_insert_color(&ext->oe_node, &obj->oo_root);
        osc_extent_get(ext);
        ext->oe_intree = 1;
}

static void osc_extent_erase(struct osc_extent *ext)
{
        struct osc_object *obj = ext->oe_obj;
        if (ext->oe_intree) {
                rb_erase(&ext->oe_node, &obj->oo_root);
                ext->oe_intree = 0;
                /* rbtree held a refcount */
                osc_extent_put_trust(ext);
        }
}

static struct osc_extent *osc_extent_hold(struct osc_extent *ext)
{
        struct osc_object *obj = ext->oe_obj;

        LASSERT(osc_object_is_locked(obj));
        LASSERT(ext->oe_state == OES_ACTIVE || ext->oe_state == OES_CACHE);
        if (ext->oe_state == OES_CACHE) {
                osc_extent_state_set(ext, OES_ACTIVE);
                osc_update_pending(obj, OBD_BRW_WRITE, -ext->oe_nr_pages);
        }
        cfs_atomic_inc(&ext->oe_users);
        cfs_list_del_init(&ext->oe_link);
        return osc_extent_get(ext);
}

static void __osc_extent_destroy(struct osc_extent *ext)
{
        LASSERT(osc_object_is_locked(ext->oe_obj));
        LASSERT(cfs_list_empty(&ext->oe_pages));
        osc_extent_erase(ext);
        cfs_list_del_init(&ext->oe_link);
        osc_extent_state_set(ext, OES_INV);
        cfs_waitq_broadcast(&ext->oe_waitq);
        OSC_EXTENT_DUMP(D_CACHE, ext, "destroyed.\n");
}

static void osc_extent_destroy(struct osc_extent *ext)
{
        struct osc_object *obj = ext->oe_obj;

        osc_object_lock(obj);
        __osc_extent_destroy(ext);
        osc_object_unlock(obj);
}

static int osc_extent_merge(const struct lu_env *env, struct osc_extent *cur,
                            struct osc_extent *victim)
{
        struct osc_object *obj = cur->oe_obj;
        pgoff_t block_start;
        pgoff_t block_end;
        int ppb_bits;

        LASSERT(cur->oe_state == OES_CACHE);
        LASSERT(osc_object_is_locked(obj));
        if (victim == NULL)
                return -EINVAL;

        if (victim->oe_state != OES_CACHE)
                return -EBUSY;

        if (cur->oe_max_end != victim->oe_max_end)
                return -ERANGE;

        LASSERT(cur->oe_osclock == victim->oe_osclock);
        ppb_bits = osc_cli(obj)->cl_blockbits - CFS_PAGE_SHIFT;
        block_start = cur->oe_start >> ppb_bits;
        block_end   = cur->oe_end   >> ppb_bits;
        if (block_start   != (victim->oe_end >> ppb_bits) + 1 &&
            block_end + 1 != victim->oe_start >> ppb_bits)
                return -ERANGE;

        OSC_EXTENT_DUMP(D_CACHE, victim, "will be merged by %p.\n", cur);

        cur->oe_start     = min(cur->oe_start, victim->oe_start);
        cur->oe_end       = max(cur->oe_end,   victim->oe_end);
        cur->oe_grants   += victim->oe_grants;
        cur->oe_nr_pages += victim->oe_nr_pages;
        /* only the following bits are needed to merge */
        cur->oe_urgent   |= victim->oe_urgent;
        cur->oe_memalloc |= victim->oe_memalloc;
        cfs_list_splice_init(&victim->oe_pages, &cur->oe_pages);
        cfs_list_del_init(&victim->oe_link);
        victim->oe_nr_pages = 0;

        osc_extent_get(victim);
        __osc_extent_destroy(victim);
        osc_extent_put(env, victim);

        OSC_EXTENT_DUMP(D_CACHE, cur, "after merging %p.\n", victim);
        return 0;
}

/**
 * Drop user count of osc_extent, and issue RPC if there is no grant reserved
 * for this extent.
 */
int osc_extent_release(const struct lu_env *env, struct osc_extent *ext)
{
        struct osc_object *obj = ext->oe_obj;
        int rc = 0;
        ENTRY;

        LASSERT(cfs_atomic_read(&ext->oe_users) > 0);
        LASSERT(sanity_check(ext) == 0);
        LASSERT(ext->oe_grants > 0);

        if (cfs_atomic_dec_and_lock(&ext->oe_users, &obj->oo_lock)) {
                LASSERT(ext->oe_state == OES_ACTIVE);
                osc_extent_state_set(ext, OES_CACHE);
                osc_update_pending(obj, OBD_BRW_WRITE, ext->oe_nr_pages);

                osc_extent_merge(env, ext, prev_extent(ext));
                osc_extent_merge(env, ext, next_extent(ext));

                if (ext->oe_urgent)
                        cfs_list_move_tail(&ext->oe_link, &obj->oo_urgent_exts);
                osc_object_unlock(obj);

                osc_io_unplug_async(env, osc_cli(obj), obj);
        }
        osc_extent_put(env, ext);
        RETURN(rc);
}

/**
 ***************************
 * POLICY TO MERGE EXTENTS *
 ***************************
 *
 * Assuming CUR is the extent going to add, and PREV is the greatest previous
 * neighbour of CUR in tree. Here are the policy to merge them:
 * 1. if CUR is not contiguous and overlapped to PREV, then create a new
 *    extent; Otherwise
 * 2. if PREV is in IO:
 *    2.1 if PREV is overlapped with CUR, we have to push PREV to be written
 *        out, and wait for IO to be finished;
 *    2.2 if PREV is contiguous to CUR, create a new extent;
 * 3. PREV is not in IO
 *    try to allocate grants for CUR and merge CUR and next. If failed to
 *    allocate grants:
 *    3.1 if they are overlapped, we should write out PREV and try again after
 *        IO is finished;
 *    3.2 allocate a no-grant extent.
 *
 * The same policy will be applied to next extent also.
 */

static inline int overlapped(struct osc_extent *ex1, struct osc_extent *ex2)
{
        return !(ex1->oe_end < ex2->oe_start || ex2->oe_end < ex1->oe_start);
}

/**
 * Find or create an extent which includes @index.
 */
struct osc_extent *osc_extent_find(const struct lu_env *env,
                                   struct osc_object *obj, pgoff_t index,
                                   int *grants)

{
        struct client_obd *cli = osc_cli(obj);
        struct cl_lock    *lock;
        struct osc_extent *cur;
        struct osc_extent *ext;
        struct osc_extent *conflict = NULL;
        struct osc_extent *found = NULL;
        pgoff_t            block;
        pgoff_t            max_end;
        int                blocksize;
        int                ppb_bits; /* pages per block bits */
        int                block_mask;
        int                rpc_mask;
        int                rc;
        ENTRY;

        cur = osc_extent_alloc(obj);
        if (cur == NULL)
                RETURN(ERR_PTR(-ENOMEM));

        lock = cl_lock_at_pgoff(env, osc2cl(obj), index, NULL, 0, 0);
        LASSERT(lock != NULL);
        LASSERT(lock->cll_descr.cld_mode >= CLM_WRITE);

        LASSERT(cli->cl_blockbits >= CFS_PAGE_SHIFT);
        ppb_bits   = cli->cl_blockbits - CFS_PAGE_SHIFT;
        block_mask = ~((1 << ppb_bits) - 1);
        blocksize  = 1 << cli->cl_blockbits;
        block      = index >> ppb_bits;

        /* align end to rpc edge */
        rpc_mask = ~(cli->cl_max_pages_per_rpc - 1);
        max_end = ((index + ~rpc_mask + 1) & rpc_mask) - 1;
        max_end = min_t(pgoff_t, max_end, lock->cll_descr.cld_end);

        /* initialize new extent by parameters so far */
        cur->oe_max_end = max_end;
        cur->oe_start   = index & block_mask;
        cur->oe_end     = ((index + ~block_mask + 1) & block_mask) - 1;
        if (cur->oe_start < lock->cll_descr.cld_start)
                cur->oe_start = lock->cll_descr.cld_start;
        if (cur->oe_end > max_end)
                cur->oe_end = max_end;
        cur->oe_osclock = lock;
        cur->oe_grants  = 0;

        /* grants has been allocated by caller */
        LASSERTF(*grants >= blocksize + cli->cl_extent_tax,
                 "%u/%u/%u.\n", *grants, blocksize, cli->cl_extent_tax);
        LASSERTF((max_end - cur->oe_start) <= ~rpc_mask, EXTSTR, EXTPARA(cur));

restart:
        osc_object_lock(obj);
        ext = osc_extent_search(obj, cur->oe_start);
        if (ext == NULL)
                ext = first_extent(obj);
        while (ext != NULL) {
                LASSERT(sanity_check_nolock(ext) == 0);
                /* We always extend extent's max_end to rpc and dlm lock
                 * boundary, so if max_end doesn't match, it means they
                 * can't be merged due to belonging to different RPC slots
                 * or being covered by different locks. */
                if (ext->oe_max_end < max_end) {
                        ext = next_extent(ext);
                        continue;
                } else if (ext->oe_max_end > max_end) {
                        EASSERTF(ext->oe_start > max_end, ext,
                                 EXTSTR, EXTPARA(cur));
                        break;
                }

                LASSERT(ext->oe_osclock == lock);
                LASSERT(ext->oe_grants > 0);

                /* ok, from now on, ext and cur have these attrs:
                 * 1. covered by the same lock
                 * 2. belong to the same RPC slot
                 * Try to merge them if they are contiguous at block level */

                if (overlapped(ext, cur)) {
                        /* cur is the minimum unit, so overlapping means
                         * full contain. */
                        EASSERTF((ext->oe_start <= cur->oe_start &&
                                  ext->oe_end >= cur->oe_end),
                                 ext, EXTSTR, EXTPARA(cur));

                        if (ext->oe_state > OES_CACHE) {
                                /* for simplicity, we wait for this extent to
                                 * finish before going forward. */
                                conflict = osc_extent_get(ext);
                                break;
                        }

                        found = osc_extent_hold(ext);
                        break;
                } else if (ext->oe_state != OES_CACHE) {
                        /* we can't do anything for a non OES_CACHE extent */
                        ext = next_extent(ext);
                        continue;
                }

                /* it's required that an extent must be contiguous at block
                 * level so that we know the whole extent is covered by grant
                 * (the pages in the extent are NOT required to be contiguous).
                 * Otherwise, it will be too much difficult to know which
                 * blocks have grants allocated. */

                /* try to do front merge - extend ext's start */
                if (block + 1 == ext->oe_start >> ppb_bits) {
                        /* ext must be block size aligned */
                        EASSERT((ext->oe_start & ~block_mask) == 0, ext);

                        /* pull ext's start back to cover cur */
                        ext->oe_start   = cur->oe_start;
                        ext->oe_grants += blocksize;
                        *grants        -= blocksize;

                        found = osc_extent_hold(ext);
                } else if (block == (ext->oe_end >> ppb_bits) + 1) {
                        /* rear merge */
                        ext->oe_end     = cur->oe_end;
                        ext->oe_grants += blocksize;
                        *grants        -= blocksize;

                        /* try to merge with the next one because we just fill
                         * in a gap */
                        if (osc_extent_merge(env, ext, next_extent(ext)) == 0)
                                /* we can save extent tax from next extent */
                                *grants += cli->cl_extent_tax;

                        found = osc_extent_hold(ext);
                }
                if (found != NULL)
                        break;

                ext = next_extent(ext);
        }

        osc_extent_tree_dump(D_CACHE, obj);
        if (found != NULL) {
                LASSERT(conflict == NULL);
                if (!IS_ERR(found)) {
                        LASSERT(found->oe_osclock == cur->oe_osclock);
                        OSC_EXTENT_DUMP(D_CACHE, found,
                                        "found caching ext for %lu.\n", index);
                }
        } else if (conflict == NULL) {
                /* create a new extent */
                EASSERT(osc_extent_is_overlapped(obj, cur) == 0, cur);
                cur->oe_grants = blocksize + cli->cl_extent_tax;
                *grants -= cur->oe_grants;
                LASSERT(*grants >= 0);

                cur->oe_state = OES_CACHE;
                found = osc_extent_hold(cur);
                osc_extent_insert(obj, cur);
                OSC_EXTENT_DUMP(D_CACHE, cur, "add into tree %lu/%lu.\n",
                                index, lock->cll_descr.cld_end);
        }
        osc_object_unlock(obj);

        if (conflict != NULL) {
                LASSERT(found == NULL);

                rc = osc_extent_wait(env, conflict);
                osc_extent_put(env, conflict);
                conflict = NULL;
                if (rc < 0)
                        GOTO(out, found = ERR_PTR(rc));

                goto restart;
        }
        EXIT;

out:
        osc_extent_put(env, cur);
        LASSERT(*grants >= 0);
        return found;
}

/**
 * Called when IO is finished to an extent.
 */
int osc_extent_finish(const struct lu_env *env, struct osc_extent *ext,
                      int sent, int rc)
{
        struct client_obd *cli = osc_cli(ext->oe_obj);
        struct osc_async_page *oap;
        struct osc_async_page *tmp;
        struct osc_async_page *last = NULL;
        int nr_pages = ext->oe_nr_pages;
        int lost_grant = 0;
        ENTRY;

        OSC_EXTENT_DUMP(D_CACHE, ext, "extent finished.\n");

        EASSERT(ergo(rc == 0, ext->oe_state == OES_RPC), ext);
        cfs_list_for_each_entry_safe(oap, tmp, &ext->oe_pages,
                                     oap_pending_item) {
                cfs_list_del_init(&oap->oap_rpc_item);
                cfs_list_del_init(&oap->oap_pending_item);
                if (last == NULL || last->oap_obj_off < oap->oap_obj_off)
                        last = oap;

                --ext->oe_nr_pages;
                osc_ap_completion(env, cli, oap, sent, rc);
        }
        EASSERT(ext->oe_nr_pages == 0, ext);

        if (!sent) {
                lost_grant = ext->oe_grants;
        } else if (cli->cl_bsize < CFS_PAGE_SIZE &&
                   last->oap_count != CFS_PAGE_SIZE) {
                /* For short writes we shouldn't count parts of pages that
                 * span a whole block on the OST side, or our accounting goes
                 * wrong.  Should match the code in filter_grant_check. */
                int blocksize = cli->cl_bsize;
                int offset = oap->oap_page_off & ~CFS_PAGE_MASK;
                int count = oap->oap_count + (offset & (blocksize - 1));
                int end = (offset + oap->oap_count) & (blocksize - 1);
                if (end)
                        count += blocksize - end;

                lost_grant = CFS_PAGE_SIZE - count;
        }
        if (ext->oe_grants > 0)
                osc_free_grant(cli, nr_pages, lost_grant);

        osc_extent_destroy(ext);
        /* put the refcount for RPC */
        osc_extent_put(env, ext);
        RETURN(0);
}

/**
 * Wait for the extent to be written out.
 */
static int osc_extent_wait(const struct lu_env *env, struct osc_extent *ext)
{
        struct osc_object *obj = ext->oe_obj;
        struct l_wait_info lwi = LWI_INTR(LWI_ON_SIGNAL_NOOP, NULL);
        int rc = 0;
        ENTRY;

        osc_object_lock(obj);
        LASSERT(sanity_check_nolock(ext) == 0);
        if (!ext->oe_urgent && !ext->oe_hp) {
                if (ext->oe_state == OES_ACTIVE) {
                        ext->oe_urgent = 1;
                } else if (ext->oe_state == OES_CACHE) {
                        ext->oe_urgent = 1;
                        osc_extent_hold(ext);
                        rc = 1;
                }
        }
        osc_object_unlock(obj);
        if (rc == 1)
                osc_extent_release(env, ext);

        /* wait for the extent until its state goes to OES_INV */
        rc = l_wait_event(ext->oe_waitq, ext->oe_state == OES_INV, &lwi);
        RETURN(rc);
}

/**
 * Discard pages behind @size. If @ext is overlapped with @size, then
 * partial truncate happens.
 */
static int osc_extent_truncate(struct osc_extent *ext, pgoff_t trunc_index)
{
        struct cl_env_nest     nest;
        struct lu_env         *env;
        struct cl_io          *io;
        struct osc_object     *obj = ext->oe_obj;
        struct client_obd     *cli = osc_cli(obj);
        struct osc_async_page *oap;
        struct osc_async_page *tmp;
        int                    pages_in_block = 0;
        int                    ppb_bits    = cli->cl_blockbits - CFS_PAGE_SHIFT;
        __u64                  trunc_block = trunc_index >> ppb_bits;
        int                    grants   = 0;
        int                    nr_pages = 0;
        int                    rc       = 0;
        ENTRY;

        LASSERT(sanity_check(ext) == 0);
        LASSERT(ext->oe_state == OES_TRUNC);
        LASSERT(!ext->oe_urgent);

        /* Request new lu_env.
         * We can't use that env from osc_cache_truncate_start() because
         * it's from lov_io_sub and not fully initialized. */
        env        = cl_env_nested_get(&nest);
        io         = &osc_env_info(env)->oti_io;
        io->ci_obj = cl_object_top(osc2cl(obj));
        rc = cl_io_init(env, io, CIT_MISC, io->ci_obj);
        if (rc < 0)
                GOTO(out, rc);

        /* discard all pages with index greater then trunc_index */
        cfs_list_for_each_entry_safe(oap, tmp, &ext->oe_pages,
                                     oap_pending_item) {
                struct cl_page  *sub  = oap2cl_page(oap);
                struct cl_page  *page = cl_page_top(sub);

                LASSERT(cfs_list_empty(&oap->oap_rpc_item));

                if (sub->cp_index < trunc_index) {
                        if (sub->cp_index >> ppb_bits == trunc_block)
                                ++pages_in_block;
                        continue;
                }

                cfs_list_del_init(&oap->oap_pending_item);

                cl_page_get(page);
                lu_ref_add(&page->cp_reference, "truncate", cfs_current());

                if (cl_page_own(env, io, page) == 0) {
                        cl_page_unmap(env, io, page);
                        cl_page_discard(env, io, page);
                        cl_page_disown(env, io, page);
                } else {
                        LASSERT(page->cp_state == CPS_FREEING);
                        LASSERT(0);
                }

                lu_ref_del(&page->cp_reference, "truncate", cfs_current());
                cl_page_put(env, page);

                --ext->oe_nr_pages;
                ++nr_pages;
        }
        EASSERTF(ergo(ext->oe_start >= trunc_index, ext->oe_nr_pages == 0),
                 ext, "trunc_index %lu\n", trunc_index);

        osc_object_lock(obj);
        if (ext->oe_nr_pages == 0) {
                LASSERT(pages_in_block == 0);
                grants = ext->oe_grants;
                ext->oe_grants = 0;
        } else { /* calculate how many grants we can free */
                int     blocks = (ext->oe_end >> ppb_bits) - trunc_block;
                pgoff_t last_index;


                /* if there is no pages in this block, we can also free grants
                 * for the last block */
                if (pages_in_block == 0) {
                        /* if this is the 1st block and no pages in this block,
                         * ext->oe_nr_pages must be zero, so we should be in
                         * the other if-clause. */
                        LASSERT(trunc_block > 0);
                        --trunc_block;
                        ++blocks;
                }

                /* this is what we can free from this extent */
                grants          = blocks << cli->cl_blockbits;
                ext->oe_grants -= grants;
                last_index      = ((trunc_block + 1) << ppb_bits) - 1;
                ext->oe_end     = min(last_index, ext->oe_max_end);
                LASSERT(ext->oe_end >= ext->oe_start);
                LASSERT(ext->oe_grants > 0);
        }
        osc_object_unlock(obj);

        if (grants > 0 || nr_pages > 0)
                osc_free_grant(cli, nr_pages, grants);

out:
        cl_io_fini(env, io);
        cl_env_nested_put(&nest, env);
        RETURN(rc);
}

static int osc_extent_make_ready(const struct lu_env *env,
                                 struct osc_extent *ext)
{
        struct osc_async_page *oap;
        struct osc_async_page *last = NULL;
        struct osc_object *obj = ext->oe_obj;
        int page_count = 0;
        int rc;
        ENTRY;

        /* we're going to grab page lock, so object lock must not be taken. */
        LASSERT(sanity_check(ext) == 0);
        /* in locking state, any process should not touch this extent. */
        EASSERT(ext->oe_state == OES_LOCKING, ext);
        EASSERT(ext->oe_owner != NULL, ext);

        OSC_EXTENT_DUMP(D_CACHE, ext, "make ready\n");

        cfs_list_for_each_entry(oap, &ext->oe_pages, oap_pending_item) {
                ++page_count;
                if (last == NULL || last->oap_obj_off < oap->oap_obj_off)
                        last = oap;

                /* checking ASYNC_READY is race safe */
                if ((oap->oap_async_flags & ASYNC_READY) != 0)
                        continue;

                rc = osc_make_ready(env, oap, OBD_BRW_WRITE);
                switch (rc) {
                case -ENOENT:
                        /* skip freeing pages */
                        LBUG();
                        break;
                case 0:
                        cfs_spin_lock(&oap->oap_lock);
                        oap->oap_async_flags |= ASYNC_READY;
                        cfs_spin_unlock(&oap->oap_lock);
                        break;
                case -EALREADY:
                        LASSERT((oap->oap_async_flags & ASYNC_READY) != 0);
                        break;
                default:
                        LASSERTF(0, "unknown return code: %d\n", rc);
                }
        }

        LASSERT(page_count == ext->oe_nr_pages);
        LASSERT(last != NULL);
        /* the last page is the only one we need to refresh its count by
         * the size of file. */
        if (!(last->oap_async_flags & ASYNC_COUNT_STABLE)) {
                last->oap_count = osc_refresh_count(env, last, OBD_BRW_WRITE);
                LASSERT(last->oap_count > 0);
                LASSERT(last->oap_page_off + last->oap_count <= CFS_PAGE_SIZE);
                last->oap_async_flags |= ASYNC_COUNT_STABLE;
        }

        /* for the rest of pages, we don't need to call osf_refresh_count()
         * because it's known they are not the last page */
        cfs_list_for_each_entry(oap, &ext->oe_pages, oap_pending_item) {
                if (!(oap->oap_async_flags & ASYNC_COUNT_STABLE)) {
                        oap->oap_count = CFS_PAGE_SIZE - oap->oap_page_off;
                        oap->oap_async_flags |= ASYNC_COUNT_STABLE;
                }
        }

        osc_object_lock(obj);
        osc_extent_state_set(ext, OES_RPC);
        osc_object_unlock(obj);
        /* get a refcount for RPC. */
        osc_extent_get(ext);

        RETURN(0);
}

static int osc_extent_expand(struct osc_extent *ext, pgoff_t index, int *grants)
{
        struct osc_object *obj = ext->oe_obj;
        struct client_obd *cli = osc_cli(obj);
        struct osc_extent *next;
        int ppb_bits = cli->cl_blockbits - CFS_PAGE_SHIFT;
        pgoff_t block = index >> ppb_bits;
        pgoff_t end_block;
        pgoff_t end_index;
        int blocksize = 1 << cli->cl_blockbits;
        int rc = 0;
        ENTRY;

        LASSERT(ext->oe_max_end >= index && ext->oe_start <= index);
        osc_object_lock(obj);
        LASSERT(sanity_check_nolock(ext) == 0);
        end_block = ext->oe_end >> ppb_bits;
        if (block > end_block + 1)
                GOTO(out, rc = -ERANGE);

        if (end_block >= block)
                GOTO(out, rc = 0);

        LASSERT(end_block + 1 == block);
        /* try to expand this extent to cover @index */
        end_index = min(ext->oe_max_end, ((block + 1) << ppb_bits) - 1);

        next = next_extent(ext);
        if (next != NULL && next->oe_start <= end_index)
                /* complex mode, it will be handled by osc_extent_find() */
                GOTO(out, rc = -EAGAIN);

        ext->oe_end = end_index;
        ext->oe_grants += blocksize;
        *grants -= blocksize;
        LASSERT(*grants >= 0);
        EASSERTF(osc_extent_is_overlapped(obj, ext) == 0, ext,
                 "overlapped after expanding for %lu.\n", index);
        EXIT;

out:
        osc_object_unlock(obj);
        RETURN(rc);
}

static void osc_extent_tree_dump0(int level, struct osc_object *obj,
                                  const char *func, int line)
{
        struct osc_extent *ext;
        int                cnt;

        CDEBUG(level, "Dump object %p extents at %s:%d.\n", obj, func, line);

        /* osc_object_lock(obj); */
        cnt = 1;
        for (ext = first_extent(obj); ext != NULL; ext = next_extent(ext))
                OSC_EXTENT_DUMP(level, ext, "in tree %d.\n", cnt++);

        cnt = 1;
        cfs_list_for_each_entry(ext, &obj->oo_hp_exts, oe_link)
                OSC_EXTENT_DUMP(level, ext, "hp %d.\n", cnt++);

        cnt = 1;
        cfs_list_for_each_entry(ext, &obj->oo_urgent_exts, oe_link)
                OSC_EXTENT_DUMP(level, ext, "urgent %d.\n", cnt++);

        cnt = 1;
        cfs_list_for_each_entry(ext, &obj->oo_reading_exts, oe_link)
                OSC_EXTENT_DUMP(level, ext, "reading %d.\n", cnt++);
        /* osc_object_unlock(obj); */
}

/* ------------------ osc extent end ------------------ */

static inline int osc_is_ready(struct osc_object *osc)
{
        return (!cfs_list_empty(&osc->oo_ready_item) ||
                !cfs_list_empty(&osc->oo_hp_ready_item));
}

#define OSC_IO_DEBUG(OSC, STR, args...)                                       \
        CDEBUG(D_CACHE, "obj %p ready %d|%c|%c wr %d|%c|%c rd %d|%c " STR,    \
               (OSC), osc_is_ready(OSC),                                      \
               list_empty_marker(&(OSC)->oo_hp_ready_item),                   \
               list_empty_marker(&(OSC)->oo_ready_item),                      \
               cfs_atomic_read(&(OSC)->oo_nr_writes),                         \
               list_empty_marker(&(OSC)->oo_hp_exts),                         \
               list_empty_marker(&(OSC)->oo_urgent_exts),                     \
               cfs_atomic_read(&(OSC)->oo_nr_reads),                          \
               list_empty_marker(&(OSC)->oo_reading_exts),                    \
               ##args)

static int osc_make_ready(const struct lu_env *env, struct osc_async_page *oap,
                          int cmd)
{
        struct osc_page *opg  = oap2osc_page(oap);
        struct cl_page  *page = cl_page_top(oap2cl_page(oap));
        int result;

        LASSERT(cmd == OBD_BRW_WRITE); /* no cached reads */

        ENTRY;
        result = cl_page_make_ready(env, page, CRT_WRITE);
        if (result == 0)
                opg->ops_submit_time = cfs_time_current();
        RETURN(result);
}

static int osc_refresh_count(const struct lu_env *env,
                             struct osc_async_page *oap, int cmd)
{
        struct osc_page  *opg = oap2osc_page(oap);
        struct cl_page   *page = oap2cl_page(oap);
        struct cl_object *obj;
        struct cl_attr   *attr = &osc_env_info(env)->oti_attr;

        int result;
        loff_t kms;

        /* readpage queues with _COUNT_STABLE, shouldn't get here. */
        LASSERT(!(cmd & OBD_BRW_READ));
        LASSERT(opg != NULL);
        obj = opg->ops_cl.cpl_obj;

        cl_object_attr_lock(obj);
        result = cl_object_attr_get(env, obj, attr);
        cl_object_attr_unlock(obj);
        if (result < 0)
                return result;
        kms = attr->cat_kms;
        if (cl_offset(obj, page->cp_index) >= kms)
                /* catch race with truncate */
                return 0;
        else if (cl_offset(obj, page->cp_index + 1) > kms)
                /* catch sub-page write at end of file */
                return kms % CFS_PAGE_SIZE;
        else
                return CFS_PAGE_SIZE;
}

static int osc_completion(const struct lu_env *env, struct osc_async_page *oap,
                          int cmd, int rc)
{
        struct osc_page   *opg  = oap2osc_page(oap);
        struct cl_page    *page = cl_page_top(oap2cl_page(oap));
        struct osc_object *obj  = cl2osc(opg->ops_cl.cpl_obj);
        enum cl_req_type   crt;
        int srvlock;

        ENTRY;

        cmd &= ~OBD_BRW_NOQUOTA;
        LASSERT(equi(page->cp_state == CPS_PAGEIN,  cmd == OBD_BRW_READ));
        LASSERT(equi(page->cp_state == CPS_PAGEOUT, cmd == OBD_BRW_WRITE));
        LASSERT(opg->ops_transfer_pinned);

        /*
         * page->cp_req can be NULL if io submission failed before
         * cl_req was allocated.
         */
        if (page->cp_req != NULL)
                cl_req_page_done(env, page);
        LASSERT(page->cp_req == NULL);

        crt = cmd == OBD_BRW_READ ? CRT_READ : CRT_WRITE;
        /* Clear opg->ops_transfer_pinned before VM lock is released. */
        opg->ops_transfer_pinned = 0;

        cfs_spin_lock(&obj->oo_seatbelt);
        LASSERT(opg->ops_submitter != NULL);
        LASSERT(!cfs_list_empty(&opg->ops_inflight));
        cfs_list_del_init(&opg->ops_inflight);
        opg->ops_submitter = NULL;
        cfs_spin_unlock(&obj->oo_seatbelt);

        opg->ops_submit_time = 0;
        srvlock = oap->oap_brw_flags & OBD_BRW_SRVLOCK;

        cl_page_completion(env, page, crt, rc);

        /* statistic */
        if (rc == 0 && srvlock) {
                struct lu_device *ld    = opg->ops_cl.cpl_obj->co_lu.lo_dev;
                struct osc_stats *stats = &lu2osc_dev(ld)->od_stats;
                int bytes = oap->oap_count;

                if (crt == CRT_READ)
                        stats->os_lockless_reads += bytes;
                else
                        stats->os_lockless_writes += bytes;
        }

        /*
         * This has to be the last operation with the page, as locks are
         * released in cl_page_completion() and nothing except for the
         * reference counter protects page from concurrent reclaim.
         */
        lu_ref_del(&page->cp_reference, "transfer", page);
        /*
         * As page->cp_obj is pinned by a reference from page->cp_req, it is
         * safe to call cl_page_put() without risking object destruction in a
         * non-blocking context.
         */
        cl_page_put(env, page);
        RETURN(0);
}

static int osc_max_rpc_in_flight(struct client_obd *cli, struct osc_object *osc)
{
        int hprpc = !!cfs_list_empty(&osc->oo_hp_exts);
        return rpcs_in_flight(cli) >= cli->cl_max_rpcs_in_flight + hprpc;
}

/* This maintains the lists of pending pages to read/write for a given object
 * (lop).  This is used by osc_check_rpcs->osc_next_obj() and osc_list_maint()
 * to quickly find objects that are ready to send an RPC. */
static int osc_makes_rpc(struct client_obd *cli, struct osc_object *osc,
                         int cmd)
{
        int invalid_import = 0;
        ENTRY;

        /* if we have an invalid import we want to drain the queued pages
         * by forcing them through rpcs that immediately fail and complete
         * the pages.  recovery relies on this to empty the queued pages
         * before canceling the locks and evicting down the llite pages */
        if ((cli->cl_import == NULL || cli->cl_import->imp_invalid))
                invalid_import = 1;

        if (cmd & OBD_BRW_WRITE) {
                if (cfs_atomic_read(&osc->oo_nr_writes) == 0)
                        RETURN(0);
                if (invalid_import) {
                        CDEBUG(D_CACHE, "invalid import forcing RPC\n");
                        RETURN(1);
                }
                if (!cfs_list_empty(&osc->oo_hp_exts)) {
                        CDEBUG(D_CACHE, "high prio request forcing RPC\n");
                        RETURN(1);
                }
                if (!cfs_list_empty(&osc->oo_urgent_exts)) {
                        CDEBUG(D_CACHE, "urgent request forcing RPC\n");
                        RETURN(1);
                }
                /* trigger a write rpc stream as long as there are dirtiers
                 * waiting for space.  as they're waiting, they're not going to
                 * create more pages to coalesce with what's waiting.. */
                if (!cfs_list_empty(&cli->cl_cache_waiters)) {
                        CDEBUG(D_CACHE, "cache waiters forcing RPC\n");
                        RETURN(1);
                }
                if (cfs_atomic_read(&osc->oo_nr_writes) >=
                    cli->cl_max_pages_per_rpc)
                        RETURN(1);
        } else {
                if (cfs_atomic_read(&osc->oo_nr_reads) == 0)
                        RETURN(0);
                if (invalid_import) {
                        CDEBUG(D_CACHE, "invalid import forcing RPC\n");
                        RETURN(1);
                }
                /* all read are urgent. */
                if (!cfs_list_empty(&osc->oo_reading_exts))
                        RETURN(1);
        }

        RETURN(0);
}

static void osc_update_pending(struct osc_object *obj, int cmd, int delta)
{
        struct client_obd *cli = osc_cli(obj);
        if (cmd & OBD_BRW_WRITE) {
                cfs_atomic_add(delta, &obj->oo_nr_writes);
                cfs_atomic_add(delta, &cli->cl_pending_w_pages);
                LASSERT(cfs_atomic_read(&obj->oo_nr_writes) >= 0);
        } else {
                cfs_atomic_add(delta, &obj->oo_nr_reads);
                cfs_atomic_add(delta, &cli->cl_pending_r_pages);
                LASSERT(cfs_atomic_read(&obj->oo_nr_reads) >= 0);
        }
        OSC_IO_DEBUG(obj, "update pending cmd %d delta %d.\n", cmd, delta);
}

static int osc_makes_hprpc(struct osc_object *obj)
{
        return !cfs_list_empty(&obj->oo_hp_exts);
}

static void on_list(cfs_list_t *item, cfs_list_t *list, int should_be_on)
{
        if (cfs_list_empty(item) && should_be_on)
                cfs_list_add_tail(item, list);
        else if (!cfs_list_empty(item) && !should_be_on)
                cfs_list_del_init(item);
}

/* maintain the osc's cli list membership invariants so that osc_send_oap_rpc
 * can find pages to build into rpcs quickly */
static int __osc_list_maint(struct client_obd *cli, struct osc_object *osc)
{
        if (osc_makes_hprpc(osc)) {
                /* HP rpc */
                on_list(&osc->oo_ready_item, &cli->cl_loi_ready_list, 0);
                on_list(&osc->oo_hp_ready_item, &cli->cl_loi_hp_ready_list, 1);
        } else {
                on_list(&osc->oo_hp_ready_item, &cli->cl_loi_hp_ready_list, 0);
                on_list(&osc->oo_ready_item, &cli->cl_loi_ready_list,
                        osc_makes_rpc(cli, osc, OBD_BRW_WRITE) ||
                        osc_makes_rpc(cli, osc, OBD_BRW_READ));
        }

        on_list(&osc->oo_write_item, &cli->cl_loi_write_list,
                cfs_atomic_read(&osc->oo_nr_writes) > 0);

        on_list(&osc->oo_read_item, &cli->cl_loi_read_list,
                cfs_atomic_read(&osc->oo_nr_reads) > 0);

        return osc_is_ready(osc);
}

static int osc_list_maint(struct client_obd *cli, struct osc_object *osc)
{
        int is_ready;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        is_ready = __osc_list_maint(cli, osc);
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        return is_ready;
}

/* this is trying to propogate async writeback errors back up to the
 * application.  As an async write fails we record the error code for later if
 * the app does an fsync.  As long as errors persist we force future rpcs to be
 * sync so that the app can get a sync error and break the cycle of queueing
 * pages for which writeback will fail. */
static void osc_process_ar(struct osc_async_rc *ar, __u64 xid,
                           int rc)
{
        if (rc) {
                if (!ar->ar_rc)
                        ar->ar_rc = rc;

                ar->ar_force_sync = 1;
                ar->ar_min_xid = ptlrpc_sample_next_xid();
                return;

        }

        if (ar->ar_force_sync && (xid >= ar->ar_min_xid))
                ar->ar_force_sync = 0;
}

/* this must be called holding the loi list lock to give coverage to exit_cache,
 * async_flag maintenance, and oap_request */
static void osc_ap_completion(const struct lu_env *env, struct client_obd *cli,
                              struct osc_async_page *oap, int sent, int rc)
{
        struct osc_object *osc = oap->oap_obj;
        struct lov_oinfo  *loi = osc->oo_oinfo;
        __u64 xid = 0;

        ENTRY;
        if (oap->oap_request != NULL) {
                xid = ptlrpc_req_xid(oap->oap_request);
                ptlrpc_req_finished(oap->oap_request);
                oap->oap_request = NULL;
        }

        /* As the transfer for this page is being done, clear the flags */
        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags = 0;
        cfs_spin_unlock(&oap->oap_lock);
        oap->oap_interrupted = 0;

        if (oap->oap_cmd & OBD_BRW_WRITE && xid > 0) {
                client_obd_list_lock(&cli->cl_loi_list_lock);
                osc_process_ar(&cli->cl_ar, xid, rc);
                osc_process_ar(&loi->loi_ar, xid, rc);
                client_obd_list_unlock(&cli->cl_loi_list_lock);
        }

        rc = osc_completion(env, oap, oap->oap_cmd, rc);
        if (rc)
                CERROR("completion on oap %p obj %p returns %d.\n",
                       oap, osc, rc);

        EXIT;
}

/* merge extent for IO. */
static int try_to_merge_extent(struct client_obd *cli, struct osc_extent *ext,
                               cfs_list_t *rpclist, int *pc)
{
        struct osc_extent *tmp;
        ENTRY;

        EASSERT((ext->oe_state == OES_CACHE || ext->oe_state == OES_LOCK_DONE),
                ext);
        EASSERT(ext->oe_nr_pages <= cli->cl_max_pages_per_rpc, ext);

        if (*pc + ext->oe_nr_pages > cli->cl_max_pages_per_rpc)
                RETURN(0);

        cfs_list_for_each_entry(tmp, rpclist, oe_link) {
                EASSERT(tmp->oe_owner == cfs_current(), tmp);
#if 0
                if (overlapped(tmp, ext)) {
                        OSC_EXTENT_DUMP(D_ERROR, tmp, "overlapped %p.\n", ext);
                        EASSERT(0, ext);
                }
#endif

                if (tmp->oe_srvlock != ext->oe_srvlock ||
                    !tmp->oe_grants != !ext->oe_grants)
                        RETURN(0);

                /* remove break for strict check */
                break;
        }

        *pc += ext->oe_nr_pages;
        cfs_list_move_tail(&ext->oe_link, rpclist);
        ext->oe_owner = cfs_current();
        RETURN(1);
}

/**
 * In order to prevent multiple ptlrpcd from breaking contiguous extents,
 * get_write_extent() takes all appropriate extents in atomic.
 */
static int get_write_extents(struct osc_object *obj, cfs_list_t *rpclist)
{
        struct client_obd *cli = osc_cli(obj);
        struct osc_extent *ext;
        struct osc_extent *next;
        int page_count = 0;

        LASSERT(osc_object_is_locked(obj));
        while (!cfs_list_empty(&obj->oo_hp_exts)) {
                ext = cfs_list_entry(obj->oo_hp_exts.next, struct osc_extent,
                                     oe_link);
                LASSERT(ext->oe_state == OES_CACHE);
                if (!try_to_merge_extent(cli, ext, rpclist, &page_count))
                        return page_count;
        }
        if (page_count == cli->cl_max_pages_per_rpc)
                return page_count;

        cfs_list_for_each_entry_safe(ext, next, &obj->oo_urgent_exts, oe_link) {
                if (!try_to_merge_extent(cli, ext, rpclist, &page_count))
                        return page_count;
        }
        if (page_count == cli->cl_max_pages_per_rpc)
                return page_count;

        ext = NULL;
        if (!cfs_list_empty(rpclist)) {
                ext = cfs_list_entry(rpclist->prev, struct osc_extent, oe_link);
                if (!ext->oe_intree)
                        return page_count;
                ext = next_extent(ext);
        }
        if (ext == NULL)
                ext = first_extent(obj);
        while (ext != NULL) {
                if (ext->oe_state != OES_CACHE) {
                        ext = next_extent(ext);
                        continue;
                }

                /* this extent may be already in current rpclist */
                if (!cfs_list_empty(&ext->oe_link) && ext->oe_owner != NULL) {
                        ext = next_extent(ext);
                        continue;
                }

                if (!try_to_merge_extent(cli, ext, rpclist, &page_count))
                        return page_count;

                ext = next_extent(ext);
        }
        return page_count;
}

static int
osc_send_write_rpc(const struct lu_env *env, struct client_obd *cli,
                   struct osc_object *osc, pdl_policy_t pol)
{
        CFS_LIST_HEAD(rpclist);
        struct osc_extent *ext;
        struct osc_extent *tmp;
        struct osc_extent *first = NULL;
        obd_count page_count = 0;
        int srvlock = 0;
        int rc = 0;
        ENTRY;

        LASSERT(osc_object_is_locked(osc));

        page_count = get_write_extents(osc, &rpclist);
        LASSERT(equi(page_count == 0, cfs_list_empty(&rpclist)));

        if (cfs_list_empty(&rpclist))
                RETURN(0);

        osc_update_pending(osc, OBD_BRW_WRITE, -page_count);

        cfs_list_for_each_entry(ext, &rpclist, oe_link) {
                LASSERT(ext->oe_state == OES_CACHE ||
                        ext->oe_state == OES_LOCK_DONE);
                if (ext->oe_state == OES_CACHE)
                        osc_extent_state_set(ext, OES_LOCKING);
                else
                        osc_extent_state_set(ext, OES_RPC);
        }

        /* we're going to grab page lock, so release object lock because
         * lock order is page lock -> object lock. */
        osc_object_unlock(osc);

        cfs_list_for_each_entry_safe(ext, tmp, &rpclist, oe_link) {
                if (ext->oe_state == OES_LOCKING) {
                        rc = osc_extent_make_ready(env, ext);
                        if (unlikely(rc < 0)) {
                                cfs_list_del_init(&ext->oe_link);
                                osc_extent_finish(env, ext, 0, rc);
                                continue;
                        }
                }
                if (first == NULL) {
                        first = ext;
                        srvlock = ext->oe_srvlock;
                } else {
                        LASSERT(srvlock == ext->oe_srvlock);
                }
        }

        if (!cfs_list_empty(&rpclist)) {
                LASSERT(page_count > 0);
                rc = osc_build_rpc(env, cli, &rpclist, OBD_BRW_WRITE, pol);
                LASSERT(cfs_list_empty(&rpclist));
        }

        osc_object_lock(osc);
        RETURN(rc);
}

/**
 * prepare pages for ASYNC io and put pages in send queue.
 *
 * \param cmd OBD_BRW_* macroses
 * \param lop pending pages
 *
 * \return zero if no page added to send queue.
 * \return 1 if pages successfully added to send queue.
 * \return negative on errors.
 */
static int
osc_send_read_rpc(const struct lu_env *env, struct client_obd *cli,
                  struct osc_object *osc, pdl_policy_t pol)
{
        struct osc_extent *ext;
        struct osc_extent *next;
        CFS_LIST_HEAD(rpclist);
        int page_count = 0;
        int rc = 0;
        ENTRY;

        LASSERT(osc_object_is_locked(osc));
        cfs_list_for_each_entry_safe(ext, next, &osc->oo_reading_exts, oe_link){
                EASSERT(ext->oe_state == OES_LOCK_DONE, ext);
                if (!try_to_merge_extent(cli, ext, &rpclist, &page_count))
                        break;
                osc_extent_state_set(ext, OES_RPC);
        }
        LASSERT(page_count <= cli->cl_max_pages_per_rpc);

        osc_update_pending(osc, OBD_BRW_READ, -page_count);

        if (!cfs_list_empty(&rpclist)) {
                osc_object_unlock(osc);

                LASSERT(page_count > 0);
                rc = osc_build_rpc(env, cli, &rpclist, OBD_BRW_READ, pol);
                LASSERT(cfs_list_empty(&rpclist));

                osc_object_lock(osc);
        }
        RETURN(rc);
}

#define list_to_obj(list, item) ({                                      \
        cfs_list_t *__tmp = (list)->next;                               \
        cfs_list_del_init(__tmp);                                       \
        cfs_list_entry(__tmp, struct osc_object, oo_##item);            \
})

/* This is called by osc_check_rpcs() to find which objects have pages that
 * we could be sending.  These lists are maintained by osc_makes_rpc(). */
static struct osc_object *osc_next_obj(struct client_obd *cli)
{
        ENTRY;

        /* First return objects that have blocked locks so that they
         * will be flushed quickly and other clients can get the lock,
         * then objects which have pages ready to be stuffed into RPCs */
        if (!cfs_list_empty(&cli->cl_loi_hp_ready_list))
                RETURN(list_to_obj(&cli->cl_loi_hp_ready_list, hp_ready_item));
        if (!cfs_list_empty(&cli->cl_loi_ready_list))
                RETURN(list_to_obj(&cli->cl_loi_ready_list, ready_item));

        /* then if we have cache waiters, return all objects with queued
         * writes.  This is especially important when many small files
         * have filled up the cache and not been fired into rpcs because
         * they don't pass the nr_pending/object threshhold */
        if (!cfs_list_empty(&cli->cl_cache_waiters) &&
            !cfs_list_empty(&cli->cl_loi_write_list))
                RETURN(list_to_obj(&cli->cl_loi_write_list, write_item));

        /* then return all queued objects when we have an invalid import
         * so that they get flushed */
        if (cli->cl_import == NULL || cli->cl_import->imp_invalid) {
                if (!cfs_list_empty(&cli->cl_loi_write_list))
                        RETURN(list_to_obj(&cli->cl_loi_write_list,
                                           write_item));
                if (!cfs_list_empty(&cli->cl_loi_read_list))
                        RETURN(list_to_obj(&cli->cl_loi_read_list,
                                           read_item));
        }
        RETURN(NULL);
}

/* called with the loi list lock held */
static void osc_check_rpcs(const struct lu_env *env, struct client_obd *cli,
                           pdl_policy_t pol)
{
        struct osc_object *osc;
        int rc = 0;
        ENTRY;

        while ((osc = osc_next_obj(cli)) != NULL) {
                struct cl_object *obj = osc2cl(osc);
                struct lu_ref_link *link;

                OSC_IO_DEBUG(osc, "%lu in flight\n", rpcs_in_flight(cli));

                if (osc_max_rpc_in_flight(cli, osc)) {
                        __osc_list_maint(cli, osc);
                        break;
                }

                cl_object_get(obj);
                client_obd_list_unlock(&cli->cl_loi_list_lock);
                link = lu_object_ref_add(&obj->co_lu, "check", cfs_current());

                /* attempt some read/write balancing by alternating between
                 * reads and writes in an object.  The makes_rpc checks here
                 * would be redundant if we were getting read/write work items
                 * instead of objects.  we don't want send_oap_rpc to drain a
                 * partial read pending queue when we're given this object to
                 * do io on writes while there are cache waiters */
                osc_object_lock(osc);
                if (osc_makes_rpc(cli, osc, OBD_BRW_WRITE)) {
                        rc = osc_send_write_rpc(env, cli, osc, pol);
                        if (rc < 0) {
                                CERROR("Write request failed with %d\n", rc);

                                /* osc_send_write_rpc failed, mostly because of
                                 * memory pressure.
                                 *
                                 * It can't break here, because if:
                                 *  - a page was submitted by osc_io_submit, so
                                 *    page locked;
                                 *  - no request in flight
                                 *  - no subsequent request
                                 * The system will be in live-lock state,
                                 * because there is no chance to call
                                 * osc_io_unplug() and osc_check_rpcs() any
                                 * more. pdflush can't help in this case,
                                 * because it might be blocked at grabbing
                                 * the page lock as we mentioned.
                                 *
                                 * Anyway, continue to drain pages. */
                                /* break; */
                        }
                }
                if (osc_makes_rpc(cli, osc, OBD_BRW_READ)) {
                        rc = osc_send_read_rpc(env, cli, osc, pol);
                        if (rc < 0)
                                CERROR("Read request failed with %d\n", rc);
                }
                osc_object_unlock(osc);

                osc_list_maint(cli, osc);
                lu_object_ref_del_at(&obj->co_lu, link, "check", cfs_current());
                cl_object_put(env, obj);

                client_obd_list_lock(&cli->cl_loi_list_lock);
        }
}

static int osc_io_unplug0(const struct lu_env *env, struct client_obd *cli,
                          struct osc_object *osc, pdl_policy_t pol, int async)
{
        int has_rpcs = 1;
        int rc = 0;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        if (osc != NULL)
                has_rpcs = __osc_list_maint(cli, osc);
        if (has_rpcs) {
                if (!async) {
                        osc_check_rpcs(env, cli, pol);
                } else {
                        CDEBUG(D_CACHE, "Queue writeback work for client %p.\n",
                               cli);
                        LASSERT(cli->cl_writeback_work != NULL);
                        rc = ptlrpcd_queue_work(cli->cl_writeback_work);
                }
        }
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        return rc;
}

static int osc_io_unplug_async(const struct lu_env *env,
                                struct client_obd *cli, struct osc_object *osc)
{
        /* XXX: policy is no use actually. */
        return osc_io_unplug0(env, cli, osc, PDL_POLICY_ROUND, 1);
}

void osc_io_unplug(const struct lu_env *env, struct client_obd *cli,
                   struct osc_object *osc, pdl_policy_t pol)
{
        (void)osc_io_unplug0(env, cli, osc, pol, 0);
}

int osc_prep_async_page(struct osc_object *osc, struct osc_page *ops,
                        cfs_page_t *page, loff_t offset)
{
        struct obd_export     *exp = osc_export(osc);
        struct osc_async_page *oap = &ops->ops_oap;
        ENTRY;

        if (!page)
                return cfs_size_round(sizeof(*oap));

        oap->oap_magic = OAP_MAGIC;
        oap->oap_cli = &exp->exp_obd->u.cli;
        oap->oap_obj = osc;

        oap->oap_page = page;
        oap->oap_obj_off = offset;
        LASSERT(!(offset & ~CFS_PAGE_MASK));

        if (!client_is_remote(exp) && cfs_capable(CFS_CAP_SYS_RESOURCE))
                oap->oap_brw_flags = OBD_BRW_NOQUOTA;

        CFS_INIT_LIST_HEAD(&oap->oap_pending_item);
        CFS_INIT_LIST_HEAD(&oap->oap_rpc_item);

        cfs_spin_lock_init(&oap->oap_lock);
        CDEBUG(D_INFO, "oap %p page %p obj off "LPU64"\n",
               oap, page, oap->oap_obj_off);
        RETURN(0);
}

int osc_queue_async_io(const struct lu_env *env, struct cl_io *io,
                       struct osc_page *ops)
{
        struct osc_io         *oio = osc_env_io(env);
        struct osc_extent     *ext = NULL;
        struct osc_async_page *oap = &ops->ops_oap;
        struct client_obd     *cli = oap->oap_cli;
        struct osc_object     *osc = oap->oap_obj;
        pgoff_t                index;
        int                    grants = 0;
        int                    brw_flags = OBD_BRW_ASYNC;
        int                    cmd = OBD_BRW_WRITE;
        int                    need_release = 0;
        int                    rc = 0;
        ENTRY;

        if (oap->oap_magic != OAP_MAGIC)
                RETURN(-EINVAL);

        if (cli->cl_import == NULL || cli->cl_import->imp_invalid)
                RETURN(-EIO);

        if (!cfs_list_empty(&oap->oap_pending_item) ||
            !cfs_list_empty(&oap->oap_rpc_item))
                RETURN(-EBUSY);

        /* Set the OBD_BRW_SRVLOCK before the page is queued. */
        brw_flags |= ops->ops_srvlock ? OBD_BRW_SRVLOCK : 0;
        if (!client_is_remote(osc_export(osc)) &&
            cfs_capable(CFS_CAP_SYS_RESOURCE)) {
                brw_flags |= OBD_BRW_NOQUOTA;
                cmd |= OBD_BRW_NOQUOTA;
        }

        /* check if the file's owner/group is over quota */
        if (!(cmd & OBD_BRW_NOQUOTA)) {
                struct cl_object *obj;
                struct cl_attr   *attr;
                unsigned int qid[MAXQUOTAS];

                obj = cl_object_top(&osc->oo_cl);
                attr = &osc_env_info(env)->oti_attr;

                cl_object_attr_lock(obj);
                rc = cl_object_attr_get(env, obj, attr);
                cl_object_attr_unlock(obj);

                qid[USRQUOTA] = attr->cat_uid;
                qid[GRPQUOTA] = attr->cat_gid;
                if (rc == 0 && osc_quota_chkdq(cli, qid) == NO_QUOTA)
                        rc = -EDQUOT;
                if (rc)
                        RETURN(rc);
        }

        oap->oap_cmd = cmd;
        oap->oap_page_off = ops->ops_from;
        oap->oap_count = ops->ops_to - ops->ops_from;
        oap->oap_async_flags = 0;
        oap->oap_brw_flags = brw_flags;

        OSC_IO_DEBUG(osc, "oap %p page %p added for cmd %d\n",
                     oap, oap->oap_page, oap->oap_cmd & OBD_BRW_RWMASK);

        index = oap2cl_page(oap)->cp_index;

        /* Add this page into extent, the theory is:
         * TODO: */

        ext = oio->oi_active;
        if (ext != NULL && ext->oe_start <= index && ext->oe_max_end >= index) {
                /* one block plus extent overhead must be enough to write this
                 * page */
                grants = (1 << cli->cl_blockbits) + cli->cl_extent_tax;
                if (ext->oe_end >= index)
                        grants = 0;

                /* it doesn't need any grant to dirty this page */
                client_obd_list_lock(&cli->cl_loi_list_lock);
                rc = osc_enter_cache_try(cli, oap, grants, 0);
                client_obd_list_unlock(&cli->cl_loi_list_lock);
                if (rc == 0) { /* try failed */
                        grants = 0;
                        need_release = 1;
                } else if (ext->oe_end < index) {
                        int tmp = grants;
                        /* try to expand this extent */
                        rc = osc_extent_expand(ext, index, &tmp);
                        if (rc < 0) {
                                need_release = 1;
                                /* don't free reserved grant */
                        } else {
                                OSC_EXTENT_DUMP(D_CACHE, ext,
                                                "expanded for %lu.\n", index);
                                osc_unreserve_grant(cli, grants, tmp);
                                grants = 0;
                        }
                }
                rc = 0;
        } else if (ext != NULL) {
                /* index is located outside of active extent */
                need_release = 1;
        }
        if (need_release) {
                osc_extent_release(env, ext);
                oio->oi_active = NULL;
                ext = NULL;
        }

        if (ext == NULL) {
                int tmp = (1 << cli->cl_blockbits) + cli->cl_extent_tax;

                /* try to find new extent to cover this page */
                LASSERT(oio->oi_active == NULL);
                /* we may have allocated grant for this page if we failed
                 * to expand the previous active extent. */
                LASSERT(ergo(grants > 0, grants >= tmp));

                rc = 0;
                if (grants == 0) {
                        /* we haven't allocated grant for this page. */
                        rc = osc_enter_cache(env, cli, oap, tmp);
                        if (rc == 0)
                                grants = tmp;
                }

                tmp = grants;
                if (rc == 0) {
                        ext = osc_extent_find(env, osc, index, &tmp);
                        if (IS_ERR(ext)) {
                                LASSERT(tmp == grants);
                                osc_exit_cache(cli, oap);
                                rc = PTR_ERR(ext);
                                ext = NULL;
                        } else {
                                oio->oi_active = ext;
                        }
                }
                if (grants > 0)
                        osc_unreserve_grant(cli, grants, tmp);
        }

        if (ext != NULL) {
                EASSERTF(ext->oe_end >= index && ext->oe_start <= index,
                         ext, "index = %lu.\n", index);
                LASSERT((oap->oap_brw_flags & OBD_BRW_FROM_GRANT) != 0);

                osc_object_lock(osc);
                if (ext->oe_nr_pages == 0)
                        ext->oe_srvlock = ops->ops_srvlock;
                else
                        LASSERT(ext->oe_srvlock == ops->ops_srvlock);
                ++ext->oe_nr_pages;
                cfs_list_add_tail(&oap->oap_pending_item, &ext->oe_pages);
                osc_object_unlock(osc);
        }
        if (cl_io_is_sync_write(io) && oio->oi_active != NULL) {
                /* release extent if this is sync write. kernel will
                 * wait for this page to be flushed before osc_io_end()
                 * is called. */
                osc_extent_release(env, oio->oi_active);
                oio->oi_active = NULL;
        }
        RETURN(rc);
}

int osc_teardown_async_page(const struct lu_env *env,
                            struct osc_object *obj, struct osc_page *ops)
{
        struct osc_async_page *oap = &ops->ops_oap;
        struct osc_extent     *ext = NULL;
        int rc = 0;
        ENTRY;

        LASSERT(oap->oap_magic == OAP_MAGIC);

        CDEBUG(D_INFO, "teardown oap %p page %p at index %lu.\n",
               oap, ops, oap2cl_page(oap)->cp_index);

        osc_object_lock(obj);
        if (!cfs_list_empty(&oap->oap_rpc_item)) {
                CDEBUG(D_CACHE, "oap %p is not in cache.\n", oap);
                rc = -EBUSY;
        } else if (!cfs_list_empty(&oap->oap_pending_item)) {
                ext = osc_extent_lookup(obj, oap2cl_page(oap)->cp_index);
                /* only truncated pages are allowed to be taken out.
                 * See osc_extent_truncate() for details. */
                if (ext != NULL && ext->oe_state != OES_TRUNC) {
                        OSC_EXTENT_DUMP(D_ERROR, ext, "trunc at %lu.\n",
                                        oap2cl_page(oap)->cp_index);
                        rc = -EBUSY;
                }
        }
        osc_object_unlock(obj);
        if (ext != NULL)
                osc_extent_put(env, ext);
        RETURN(rc);
}

/**
 * This is called when a page is picked up by kernel to write out.
 *
 * We should find out the corresponding extent and add the whole extent
 * into urgent list. The extent may be being truncated or used, handle it
 * carefully.
 */
int osc_flush_async_page(const struct lu_env *env, struct cl_io *io,
                         struct osc_page *ops)
{
        struct osc_extent *ext   = NULL;
        struct osc_object *obj   = cl2osc(ops->ops_cl.cpl_obj);
        struct cl_page    *cp    = ops->ops_cl.cpl_page;
        pgoff_t            index = cp->cp_index;
        struct osc_async_page *oap = &ops->ops_oap;
        int unplug = 0;
        int rc = 0;
        ENTRY;

        osc_object_lock(obj);
        ext = osc_extent_lookup(obj, index);
        if (ext == NULL) {
                osc_extent_tree_dump(D_ERROR, obj);
                LASSERTF(0, "page index %lu is NOT covered.\n", index);
        }

        switch (ext->oe_state) {
        case OES_RPC:
        case OES_LOCK_DONE:
                CL_PAGE_DEBUG(D_ERROR, env, cl_page_top(cp),
                              "flush an in-rpc page?\n");
                LASSERT(0);
                break;
        case OES_TRUNC:
                /* race with truncate, page will be redirtied */
                GOTO(out, rc = -EAGAIN);
                break;
        default:
                break;
        }

        rc = cl_page_prep(env, io, cl_page_top(cp), CRT_WRITE);
        if (rc < 0)
                GOTO(out, rc);

        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags |= ASYNC_READY|ASYNC_URGENT;
        cfs_spin_unlock(&oap->oap_lock);

        if (cfs_memory_pressure_get())
                ext->oe_memalloc = 1;

        ext->oe_urgent = 1;
        if (ext->oe_state == OES_CACHE && cfs_list_empty(&ext->oe_link)) {
                OSC_EXTENT_DUMP(D_CACHE, ext,
                                "flush page %p make it urgent.\n", oap);
                cfs_list_add_tail(&ext->oe_link, &obj->oo_urgent_exts);
                unplug = 1;
        }
        rc = 0;
        EXIT;

out:
        osc_object_unlock(obj);
        osc_extent_put(env, ext);
        if (unplug)
                osc_io_unplug_async(env, osc_cli(obj), obj);
        return rc;
}

/**
 * this is called when a sync waiter receives an interruption.  Its job is to
 * get the caller woken as soon as possible.  If its page hasn't been put in an
 * rpc yet it can dequeue immediately.  Otherwise it has to mark the rpc as
 * desiring interruption which will forcefully complete the rpc once the rpc
 * has timed out.
 */
int osc_cancel_async_page(const struct lu_env *env, struct osc_page *ops)
{
        struct osc_async_page *oap = &ops->ops_oap;
        struct osc_object     *obj = oap->oap_obj;
        struct client_obd     *cli = osc_cli(obj);
        struct osc_extent     *ext;
        struct osc_extent     *found = NULL;
        cfs_list_t            *plist;
        pgoff_t index = oap2cl_page(oap)->cp_index;
        int     rc = -EBUSY;
        int     cmd;
        ENTRY;

        LASSERT(!oap->oap_interrupted);
        oap->oap_interrupted = 1;

        /* Find out the caching extent */
        osc_object_lock(obj);
        if (oap->oap_cmd & OBD_BRW_WRITE) {
                plist = &obj->oo_urgent_exts;
                cmd   = OBD_BRW_WRITE;
        } else {
                plist = &obj->oo_reading_exts;
                cmd   = OBD_BRW_READ;
        }
        cfs_list_for_each_entry(ext, plist, oe_link) {
                if (ext->oe_start <= index && ext->oe_end >= index) {
                        LASSERT(ext->oe_state == OES_LOCK_DONE);
                        /* For OES_LOCK_DONE state extent, it has already held
                         * a refcount for RPC. */
                        found = osc_extent_get(ext);
                        break;
                }
        }
        if (found != NULL) {
                cfs_list_del_init(&found->oe_link);
                osc_update_pending(obj, cmd, -found->oe_nr_pages);
                osc_object_unlock(obj);

                osc_extent_finish(env, found, 0, -EINTR);
                osc_extent_put(env, found);
                rc = 0;
        } else {
                osc_object_unlock(obj);
                /* ok, it's been put in an rpc. only one oap gets a request
                 * reference */
                if (oap->oap_request != NULL) {
                        ptlrpc_mark_interrupted(oap->oap_request);
                        ptlrpcd_wake(oap->oap_request);
                        ptlrpc_req_finished(oap->oap_request);
                        oap->oap_request = NULL;
                }
        }

        osc_list_maint(cli, obj);
        RETURN(rc);
}

int osc_queue_sync_pages(const struct lu_env *env, struct osc_object *obj,
                         cfs_list_t *list, int cmd, int brw_flags)
{
        struct client_obd     *cli = osc_cli(obj);
        struct osc_extent     *ext;
        struct osc_async_page *oap;
        int     page_count = 0;
        pgoff_t start      = CL_PAGE_EOF;
        pgoff_t end        = 0;
        ENTRY;

        cfs_list_for_each_entry(oap, list, oap_pending_item) {
                struct cl_page *cp = oap2cl_page(oap);
                if (cp->cp_index > end)
                        end = cp->cp_index;
                if (cp->cp_index < start)
                        start = cp->cp_index;
                ++page_count;
        }
        LASSERT(page_count <= cli->cl_max_pages_per_rpc);

        ext = osc_extent_alloc(obj);
        if (ext == NULL) {
                cfs_list_for_each_entry(oap, list, oap_pending_item) {
                        cfs_list_del_init(&oap->oap_pending_item);
                        osc_ap_completion(env, cli, oap, 0, -ENOMEM);
                }
                RETURN(-ENOMEM);
        }

        ext->oe_rw = !!(cmd & OBD_BRW_READ);
        ext->oe_urgent = 1;
        ext->oe_start = start;
        ext->oe_end = ext->oe_max_end = end;
        ext->oe_obj = obj;
        ext->oe_srvlock = !!(brw_flags & OBD_BRW_SRVLOCK);
        ext->oe_nr_pages = page_count;
        cfs_list_splice_init(list, &ext->oe_pages);

        osc_object_lock(obj);
        /* Reuse the initial refcount for RPC, don't drop it */
        osc_extent_state_set(ext, OES_LOCK_DONE);
        if (cmd & OBD_BRW_WRITE) {
                cfs_list_add_tail(&ext->oe_link, &obj->oo_urgent_exts);
                osc_update_pending(obj, OBD_BRW_WRITE, page_count);
        } else {
                cfs_list_add_tail(&ext->oe_link, &obj->oo_reading_exts);
                osc_update_pending(obj, OBD_BRW_READ, page_count);
        }
        osc_object_unlock(obj);

        osc_io_unplug(env, cli, obj, PDL_POLICY_ROUND);
        RETURN(0);
}

/**
 * Called by osc_io_setattr_start() to freeze and destroy covering extents.
 */
int osc_cache_truncate_start(const struct lu_env *env, struct osc_io *oio,
                             struct osc_object *obj, __u64 size)
{
        struct client_obd *cli = osc_cli(obj);
        struct osc_extent *ext;
        struct osc_extent *waiting = NULL;
        pgoff_t index;
        CFS_LIST_HEAD(list);
        int result = 0;
        ENTRY;

        /* pages with index greater or equal to index will be truncated. */
        index = cl_index(osc2cl(obj), size + CFS_PAGE_SIZE - 1);

again:
        osc_object_lock(obj);
        ext = osc_extent_search(obj, index);
        if (ext == NULL)
                ext = first_extent(obj);
        else if (ext->oe_end < index)
                ext = next_extent(ext);
        while (ext != NULL) {
                EASSERT(ext->oe_state != OES_ACTIVE, ext);
                EASSERT(ext->oe_state != OES_TRUNC, ext);
                if (ext->oe_state > OES_CACHE || ext->oe_urgent) {
                        /* if ext is in urgent state, it means there must exist
                         * a page already having been flushed by write_page().
                         * We have to wait for this extent because we can't
                         * truncate that page. */
                        LASSERT(!ext->oe_hp);
                        OSC_EXTENT_DUMP(D_CACHE, ext,
                                        "waiting for busy extent\n");
                        waiting = osc_extent_get(ext);
                        break;
                }

                osc_extent_get(ext);
                EASSERT(ext->oe_state == OES_CACHE, ext);
                osc_extent_state_set(ext, OES_TRUNC);
                EASSERT(cfs_list_empty(&ext->oe_link), ext);
                cfs_list_add_tail(&ext->oe_link, &list);
                osc_update_pending(obj, OBD_BRW_WRITE, -ext->oe_nr_pages);

                ext = next_extent(ext);
        }
        osc_object_unlock(obj);

        osc_list_maint(cli, obj);

        while (!cfs_list_empty(&list)) {
                int rc;

                ext = cfs_list_entry(list.next, struct osc_extent, oe_link);
                cfs_list_del_init(&ext->oe_link);

                rc = osc_extent_truncate(ext, index);
                if (rc < 0) {
                        if (result == 0)
                                result = rc;

                        OSC_EXTENT_DUMP(D_ERROR, ext,
                                        "truncate error %d\n", rc);
                } else if (ext->oe_nr_pages == 0) {
                        osc_extent_destroy(ext);
                } else {
                        /* this must be an overlapped extent which means only
                         * part of pages in this extent have been truncated.
                         */
                        EASSERTF(ext->oe_start < index, ext,
                                 "trunc index = %lu.\n", index);
                        /* fix index to skip this partially truncated extent */
                        index = ext->oe_end + 1;

                        /* we need to hold this extent in OES_TRUNC state so
                         * that no writeback will happen. This is to avoid
                         * BUG 17397. */
                        LASSERT(oio->oi_trunc == NULL);
                        oio->oi_trunc = osc_extent_get(ext);
                        OSC_EXTENT_DUMP(D_CACHE, ext,
                                        "trunc at "LPU64"\n", size);
                }
                osc_extent_put(env, ext);
        }
        if (waiting != NULL) {
                if (result == 0)
                        result = osc_extent_wait(env, waiting);

                osc_extent_put(env, waiting);
                waiting = NULL;
                if (result == 0)
                        goto again;
        }
        RETURN(result);
}

/**
 * Called after osc_io_setattr_end to add oio->oi_trunc back to cache.
 */
void osc_cache_truncate_end(const struct lu_env *env, struct osc_io *oio,
                            struct osc_object *obj)
{
        struct osc_extent *ext = oio->oi_trunc;

        oio->oi_trunc = NULL;
        if (ext != NULL) {
                EASSERT(ext->oe_nr_pages > 0, ext);
                EASSERT(ext->oe_state == OES_TRUNC, ext);
                EASSERT(!ext->oe_urgent, ext);

                OSC_EXTENT_DUMP(D_CACHE, ext, "trunc -> cache.\n");
                osc_object_lock(obj);
                osc_extent_state_set(ext, OES_CACHE);
                osc_update_pending(obj, OBD_BRW_WRITE, ext->oe_nr_pages);
                osc_object_unlock(obj);
                osc_extent_put(env, ext);

                osc_list_maint(osc_cli(obj), obj);
        }
}

/**
 * Called when a write osc_lock is being canceled.
 */
int osc_cache_pageout(const struct lu_env *env, struct osc_lock *ols,
                      int discard)
{
        struct osc_object    *obj;
        struct osc_extent    *ext;
        struct osc_extent    *waiting = NULL;
        struct cl_lock_descr *descr = &ols->ols_cl.cls_lock->cll_descr;
        pgoff_t               start = descr->cld_start;
        pgoff_t               end   = descr->cld_end;
        int                   do_wait = 0;
        int                   unplug  = 0;
        int                   result  = 0;
        CFS_LIST_HEAD        (list);
        ENTRY;

        if (descr->cld_mode < CLM_WRITE)
                RETURN(0);

        obj = cl2osc(ols->ols_cl.cls_obj);
again:
        osc_object_lock(obj);
        ext = osc_extent_search(obj, start);
        if (ext == NULL)
                ext = first_extent(obj);
        else if (ext->oe_end < start)
                ext = next_extent(ext);
        while (ext != NULL) {
                if (ext->oe_start > end)
                        break;

                /* lock must contain region of extent */
                EASSERT(ext->oe_start >= start && ext->oe_max_end <= end, ext);
                if (do_wait) {
                        waiting = osc_extent_get(ext);
                        break;
                }

                switch (ext->oe_state) {
                case OES_CACHE:
                        EASSERT(!ext->oe_hp, ext);
                        if (!discard) {
                                ext->oe_hp = 1;
                                cfs_list_move_tail(&ext->oe_link,
                                                   &obj->oo_hp_exts);
                                unplug = 1;
                        } else {
                                osc_extent_state_set(ext, OES_LOCKING);
                                ext->oe_owner = cfs_current();
                                cfs_list_move_tail(&ext->oe_link, &list);
                                osc_update_pending(obj, OBD_BRW_WRITE,
                                                   -ext->oe_nr_pages);
                        }
                        break;
                case OES_INV:
                case OES_TRUNC:
                case OES_ACTIVE:
                        EASSERTF(0, ext, "impossible state.\n");
                default:
                        /* the extent is already in IO(OES_RPC) */
                        break;
                }
                ext = next_extent(ext);
        }
        osc_object_unlock(obj);

        LASSERT(ergo(!discard, cfs_list_empty(&list)));
        if (!cfs_list_empty(&list)) {
                struct osc_extent *tmp;
                int rc;

                osc_list_maint(osc_cli(obj), obj);
                cfs_list_for_each_entry_safe(ext, tmp, &list, oe_link) {
                        cfs_list_del_init(&ext->oe_link);
                        EASSERT(ext->oe_state == OES_LOCKING, ext);

                        /* Discard caching pages. We don't actually write this
                         * extent out but we complete it as if we did. */
                        rc = osc_extent_make_ready(env, ext);
                        if (unlikely(rc < 0)) {
                                OSC_EXTENT_DUMP(D_ERROR, ext,
                                                "make_ready returned %d\n", rc);
                                if (result == 0)
                                        result = rc;
                        }

                        /* finish the extent as if the pages were sent */
                        osc_extent_finish(env, ext, 0, 0);
                }
        }

        if (unplug)
                osc_io_unplug(env, osc_cli(obj), obj, PDL_POLICY_ROUND);

        if (!do_wait) {
                LASSERT(waiting == NULL);
                do_wait = 1;
                goto again;
        } else if (waiting != NULL) {
                osc_extent_wait(env, waiting);
                osc_extent_put(env, waiting);
                waiting = NULL;
                goto again;
        }
        OSC_IO_DEBUG(obj, "cache page out.\n");
        RETURN(result);
}

/** @} osc */
