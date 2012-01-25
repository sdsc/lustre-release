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
 * Implementation of cl_page for OSC layer.
 *
 *   Author: Nikita Danilov <nikita.danilov@sun.com>
 */

#define DEBUG_SUBSYSTEM S_OSC

#include "osc_cl_internal.h"

static int osc_prep_async_page(struct osc_object *osc, struct osc_page *ops,
                               cfs_page_t *page, loff_t offset);
static int osc_queue_async_io(const struct lu_env *env, struct osc_page *ops);
static int osc_teardown_async_page(struct osc_object *obj,
                                   struct osc_page *ops);
static int osc_oap_interrupted(const struct lu_env *env,
                               struct osc_async_page *oap);
static void osc_oap_to_pending(struct osc_async_page *oap);
static void osc_list_maint(struct client_obd *cli, struct osc_object *osc);

/** \addtogroup osc 
 *  @{ 
 */

/* 
 * Comment out osc_page_protected because it may sleep inside the
 * the client_obd_list_lock.
 * client_obd_list_lock -> osc_ap_completion -> osc_completion ->
 *   -> osc_page_protected -> osc_page_is_dlocked -> osc_match_base
 *   -> ldlm_lock_match -> sptlrpc_import_check_ctx -> sleep.
 */
#if 0
static int osc_page_is_dlocked(const struct lu_env *env,
                               const struct osc_page *opg,
                               enum cl_lock_mode mode, int pending, int unref)
{
        struct cl_page         *page;
        struct osc_object      *obj;
        struct osc_thread_info *info;
        struct ldlm_res_id     *resname;
        struct lustre_handle   *lockh;
        ldlm_policy_data_t     *policy;
        ldlm_mode_t             dlmmode;
        int                     flags;

        cfs_might_sleep();

        info = osc_env_info(env);
        resname = &info->oti_resname;
        policy = &info->oti_policy;
        lockh = &info->oti_handle;
        page = opg->ops_cl.cpl_page;
        obj = cl2osc(opg->ops_cl.cpl_obj);

        flags = LDLM_FL_TEST_LOCK | LDLM_FL_BLOCK_GRANTED;
        if (pending)
                flags |= LDLM_FL_CBPENDING;

        dlmmode = osc_cl_lock2ldlm(mode) | LCK_PW;
        osc_lock_build_res(env, obj, resname);
        osc_index2policy(policy, page->cp_obj, page->cp_index, page->cp_index);
        return osc_match_base(osc_export(obj), resname, LDLM_EXTENT, policy,
                              dlmmode, &flags, NULL, lockh, unref);
}

/**
 * Checks an invariant that a page in the cache is covered by a lock, as
 * needed.
 */
static int osc_page_protected(const struct lu_env *env,
                              const struct osc_page *opg,
                              enum cl_lock_mode mode, int unref)
{
        struct cl_object_header *hdr;
        struct cl_lock          *scan;
        struct cl_page          *page;
        struct cl_lock_descr    *descr;
        int result;

        LINVRNT(!opg->ops_temp);

        page = opg->ops_cl.cpl_page;
        if (page->cp_owner != NULL &&
            cl_io_top(page->cp_owner)->ci_lockreq == CILR_NEVER)
                /*
                 * If IO is done without locks (liblustre, or lloop), lock is
                 * not required.
                 */
                result = 1;
        else
                /* otherwise check for a DLM lock */
        result = osc_page_is_dlocked(env, opg, mode, 1, unref);
        if (result == 0) {
                /* maybe this page is a part of a lockless io? */
                hdr = cl_object_header(opg->ops_cl.cpl_obj);
                descr = &osc_env_info(env)->oti_descr;
                descr->cld_mode = mode;
                descr->cld_start = page->cp_index;
                descr->cld_end   = page->cp_index;
                cfs_spin_lock(&hdr->coh_lock_guard);
                cfs_list_for_each_entry(scan, &hdr->coh_locks, cll_linkage) {
                        /*
                         * Lock-less sub-lock has to be either in HELD state
                         * (when io is actively going on), or in CACHED state,
                         * when top-lock is being unlocked:
                         * cl_io_unlock()->cl_unuse()->...->lov_lock_unuse().
                         */
                        if ((scan->cll_state == CLS_HELD ||
                             scan->cll_state == CLS_CACHED) &&
                            cl_lock_ext_match(&scan->cll_descr, descr)) {
                                struct osc_lock *olck;

                                olck = osc_lock_at(scan);
                                result = osc_lock_is_lockless(olck);
                                break;
                        }
                }
                cfs_spin_unlock(&hdr->coh_lock_guard);
        }
        return result;
}
#else
static int osc_page_protected(const struct lu_env *env,
                              const struct osc_page *opg,
                              enum cl_lock_mode mode, int unref)
{
        return 1;
}
#endif

/*****************************************************************************
 *
 * Page operations.
 *
 */
static void osc_page_fini(const struct lu_env *env,
                          struct cl_page_slice *slice)
{
        struct osc_page *opg = cl2osc_page(slice);
        CDEBUG(D_TRACE, "%p\n", opg);
        LASSERT(opg->ops_lock == NULL);
        OBD_SLAB_FREE_PTR(opg, osc_page_kmem);
}

static void osc_page_transfer_get(struct osc_page *opg, const char *label)
{
        struct cl_page *page = cl_page_top(opg->ops_cl.cpl_page);

        LASSERT(!opg->ops_transfer_pinned);
        cl_page_get(page);
        lu_ref_add_atomic(&page->cp_reference, label, page);
        opg->ops_transfer_pinned = 1;
}

static void osc_page_transfer_put(const struct lu_env *env,
                                  struct osc_page *opg)
{
        struct cl_page *page = cl_page_top(opg->ops_cl.cpl_page);

        if (opg->ops_transfer_pinned) {
                lu_ref_del(&page->cp_reference, "transfer", page);
                opg->ops_transfer_pinned = 0;
                cl_page_put(env, page);
        }
}

/**
 * This is called once for every page when it is submitted for a transfer
 * either opportunistic (osc_page_cache_add()), or immediate
 * (osc_page_submit()).
 */
static void osc_page_transfer_add(const struct lu_env *env,
                                  struct osc_page *opg, enum cl_req_type crt)
{
        struct osc_object *obj;

        obj = cl2osc(opg->ops_cl.cpl_obj);
        cfs_spin_lock(&obj->oo_seatbelt);
        cfs_list_add(&opg->ops_inflight, &obj->oo_inflight[crt]);
        opg->ops_submitter = cfs_current();
        cfs_spin_unlock(&obj->oo_seatbelt);
}

static int osc_page_cache_add(const struct lu_env *env,
                              const struct cl_page_slice *slice,
                              struct cl_io *unused)
{
        struct osc_page *opg = cl2osc_page(slice);
        int result;
        ENTRY;

        LINVRNT(osc_page_protected(env, opg, CLM_WRITE, 0));

        osc_page_transfer_get(opg, "transfer\0cache");
        result = osc_queue_async_io(env, opg);
        if (result != 0)
                osc_page_transfer_put(env, opg);
        else
                osc_page_transfer_add(env, opg, CRT_WRITE);
        RETURN(result);
}

void osc_index2policy(ldlm_policy_data_t *policy, const struct cl_object *obj,
                      pgoff_t start, pgoff_t end)
{
        memset(policy, 0, sizeof *policy);
        policy->l_extent.start = cl_offset(obj, start);
        policy->l_extent.end   = cl_offset(obj, end + 1) - 1;
}

static int osc_page_addref_lock(const struct lu_env *env,
                                struct osc_page *opg,
                                struct cl_lock *lock)
{
        struct osc_lock *olock;
        int              rc;

        LASSERT(opg->ops_lock == NULL);

        olock = osc_lock_at(lock);
        if (cfs_atomic_inc_return(&olock->ols_pageref) <= 0) {
                cfs_atomic_dec(&olock->ols_pageref);
                cl_lock_put(env, lock);
                rc = 1;
        } else {
                opg->ops_lock = lock;
                rc = 0;
        }
        return rc;
}

static void osc_page_putref_lock(const struct lu_env *env,
                                 struct osc_page *opg)
{
        struct cl_lock  *lock = opg->ops_lock;
        struct osc_lock *olock;

        LASSERT(lock != NULL);
        olock = osc_lock_at(lock);

        cfs_atomic_dec(&olock->ols_pageref);
        opg->ops_lock = NULL;

        /*
         * Note: usually this won't be the last reference of the lock, but if
         * it is, then all the lock_put do is at most just freeing some memory,
         * so it would be OK that caller is holding spinlocks.
         */
        LASSERT(cfs_atomic_read(&lock->cll_ref) > 1 || olock->ols_hold == 0);
        cl_lock_put(env, lock);
}

static int osc_page_is_under_lock(const struct lu_env *env,
                                  const struct cl_page_slice *slice,
                                  struct cl_io *unused)
{
        struct cl_lock *lock;
        int             result;

        ENTRY;
        lock = cl_lock_at_page(env, slice->cpl_obj, slice->cpl_page,
                               NULL, 1, 0);
        if (lock != NULL &&
            osc_page_addref_lock(env, cl2osc_page(slice), lock) == 0)
                result = -EBUSY;
        else
                result = -ENODATA;
        RETURN(result);
}

static void osc_page_disown(const struct lu_env *env,
                            const struct cl_page_slice *slice,
                            struct cl_io *io)
{
        struct osc_page *opg = cl2osc_page(slice);

        if (unlikely(opg->ops_lock))
                osc_page_putref_lock(env, opg);
}

static void osc_page_completion_read(const struct lu_env *env,
                                     const struct cl_page_slice *slice,
                                     int ioret)
{
        struct osc_page *opg = cl2osc_page(slice);

        if (likely(opg->ops_lock))
                osc_page_putref_lock(env, opg);
}

static int osc_page_fail(const struct lu_env *env,
                         const struct cl_page_slice *slice,
                         struct cl_io *unused)
{
        /*
         * Cached read?
         */
        LBUG();
        return 0;
}


static const char *osc_list(cfs_list_t *head)
{
        return cfs_list_empty(head) ? "-" : "+";
}

static inline cfs_time_t osc_submit_duration(struct osc_page *opg)
{
        if (opg->ops_submit_time == 0)
                return 0;

        return (cfs_time_current() - opg->ops_submit_time);
}

static int osc_page_print(const struct lu_env *env,
                          const struct cl_page_slice *slice,
                          void *cookie, lu_printer_t printer)
{
        struct osc_page       *opg = cl2osc_page(slice);
        struct osc_async_page *oap = &opg->ops_oap;
        struct osc_object     *obj = cl2osc(slice->cpl_obj);
        struct client_obd     *cli = &osc_export(obj)->exp_obd->u.cli;

        return (*printer)(env, cookie, LUSTRE_OSC_NAME"-page@%p: "
                          "1< %#x %d %u %s %s %s > "
                          "2< "LPU64" %u %u %#x %#x | %p %p %p > "
                          "3< %s %p %d %lu %d > "
                          "4< %d %d %d %lu %s | %s %s %s %s > "
                          "5< %s %s %s %s | %d %s %s | %d %s %s>\n",
                          opg,
                          /* 1 */
                          oap->oap_magic, oap->oap_cmd,
                          oap->oap_interrupted,
                          osc_list(&oap->oap_pending_item),
                          osc_list(&oap->oap_urgent_item),
                          osc_list(&oap->oap_rpc_item),
                          /* 2 */
                          oap->oap_obj_off, oap->oap_page_off, oap->oap_count,
                          oap->oap_async_flags, oap->oap_brw_flags,
                          oap->oap_request, oap->oap_cli, obj,
                          /* 3 */
                          osc_list(&opg->ops_inflight),
                          opg->ops_submitter, opg->ops_transfer_pinned,
                          osc_submit_duration(opg), opg->ops_srvlock,
                          /* 4 */
                          cli->cl_r_in_flight, cli->cl_w_in_flight,
                          cli->cl_max_rpcs_in_flight,
                          cli->cl_avail_grant,
                          osc_list(&cli->cl_cache_waiters),
                          osc_list(&cli->cl_loi_ready_list),
                          osc_list(&cli->cl_loi_hp_ready_list),
                          osc_list(&cli->cl_loi_write_list),
                          osc_list(&cli->cl_loi_read_list),
                          /* 5 */
                          osc_list(&obj->oo_ready_item),
                          osc_list(&obj->oo_hp_ready_item),
                          osc_list(&obj->oo_write_item),
                          osc_list(&obj->oo_read_item),
                          obj->oo_read_pages.oop_num_pending,
                          osc_list(&obj->oo_read_pages.oop_pending),
                          osc_list(&obj->oo_read_pages.oop_urgent),
                          obj->oo_write_pages.oop_num_pending,
                          osc_list(&obj->oo_write_pages.oop_pending),
                          osc_list(&obj->oo_write_pages.oop_urgent));
}

static void osc_page_delete(const struct lu_env *env,
                            const struct cl_page_slice *slice)
{
        struct osc_page   *opg = cl2osc_page(slice);
        struct osc_object *obj = cl2osc(opg->ops_cl.cpl_obj);
        int rc;

        LINVRNT(opg->ops_temp || osc_page_protected(env, opg, CLM_READ, 1));

        ENTRY;
        CDEBUG(D_TRACE, "%p\n", opg);
        osc_page_transfer_put(env, opg);
        rc = osc_teardown_async_page(obj, opg);
        if (rc) {
                CL_PAGE_DEBUG(D_ERROR, env, cl_page_top(slice->cpl_page),
                              "Trying to teardown failed: %d\n", rc);
                LASSERT(0);
        }
        cfs_spin_lock(&obj->oo_seatbelt);
        cfs_list_del_init(&opg->ops_inflight);
        cfs_spin_unlock(&obj->oo_seatbelt);
        EXIT;
}

void osc_page_clip(const struct lu_env *env, const struct cl_page_slice *slice,
                   int from, int to)
{
        struct osc_page       *opg = cl2osc_page(slice);
        struct osc_async_page *oap = &opg->ops_oap;

        LINVRNT(osc_page_protected(env, opg, CLM_READ, 0));

        opg->ops_from = from;
        opg->ops_to   = to;
        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags |= ASYNC_COUNT_STABLE;
        cfs_spin_unlock(&oap->oap_lock);
}

static int osc_page_cancel(const struct lu_env *env,
                           const struct cl_page_slice *slice)
{
        struct osc_page *opg       = cl2osc_page(slice);
        struct osc_async_page *oap = &opg->ops_oap;
        int rc = 0;

        LINVRNT(osc_page_protected(env, opg, CLM_READ, 0));

        client_obd_list_lock(&oap->oap_cli->cl_loi_list_lock);
        /* Check if the transferring against this page
         * is completed, or not even queued. */
        if (opg->ops_transfer_pinned)
                /* FIXME: may not be interrupted.. */
                rc = osc_oap_interrupted(env, oap);
        LASSERT(ergo(rc == 0, opg->ops_transfer_pinned == 0));
        client_obd_list_unlock(&oap->oap_cli->cl_loi_list_lock);
        return rc;
}

static const struct cl_page_operations osc_page_ops = {
        .cpo_fini          = osc_page_fini,
        .cpo_print         = osc_page_print,
        .cpo_delete        = osc_page_delete,
        .cpo_is_under_lock = osc_page_is_under_lock,
        .cpo_disown        = osc_page_disown,
        .io = {
                [CRT_READ] = {
                        .cpo_cache_add  = osc_page_fail,
                        .cpo_completion = osc_page_completion_read
                },
                [CRT_WRITE] = {
                        .cpo_cache_add  = osc_page_cache_add
                }
        },
        .cpo_clip           = osc_page_clip,
        .cpo_cancel         = osc_page_cancel
};

struct osc_page *oap2osc_page(struct osc_async_page *oap)
{
        return (struct osc_page *)container_of(oap, struct osc_page, ops_oap);
}

static int osc_make_ready(const struct lu_env *env, struct osc_async_page *oap,
                          int cmd)
{
        struct osc_page *opg  = oap2osc_page(oap);
        struct cl_page  *page = cl_page_top(opg->ops_cl.cpl_page);
        int result;

        LASSERT(cmd == OBD_BRW_WRITE); /* no cached reads */
        LINVRNT(osc_page_protected(env, opg, CLM_WRITE, 1));

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
        struct cl_page   *page;
        struct cl_object *obj;
        struct cl_attr   *attr = &osc_env_info(env)->oti_attr;

        int result;
        loff_t kms;

        LINVRNT(osc_page_protected(env, opg, CLM_READ, 1));

        /* readpage queues with _COUNT_STABLE, shouldn't get here. */
        LASSERT(!(cmd & OBD_BRW_READ));
        LASSERT(opg != NULL);
        page = opg->ops_cl.cpl_page;
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
                          int cmd, struct obdo *oa, int rc)
{
        struct osc_page       *opg  = oap2osc_page(oap);
        struct cl_page        *page = cl_page_top(opg->ops_cl.cpl_page);
        struct osc_object     *obj  = cl2osc(opg->ops_cl.cpl_obj);
        enum cl_req_type crt;
        int srvlock;

        LINVRNT(osc_page_protected(env, opg, CLM_READ, 1));

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

        /* As the transfer for this page is being done, clear the flags */
        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags = 0;
        cfs_spin_unlock(&oap->oap_lock);

        crt = cmd == OBD_BRW_READ ? CRT_READ : CRT_WRITE;
        /* Clear opg->ops_transfer_pinned before VM lock is released. */
        opg->ops_transfer_pinned = 0;

        cfs_spin_lock(&obj->oo_seatbelt);
        LASSERT(opg->ops_submitter != NULL);
        LASSERT(!cfs_list_empty(&opg->ops_inflight));
        cfs_list_del_init(&opg->ops_inflight);
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

struct cl_page *osc_page_init(const struct lu_env *env,
                              struct cl_object *obj,
                              struct cl_page *page, cfs_page_t *vmpage)
{
        struct osc_object *osc = cl2osc(obj);
        struct osc_page   *opg;
        int result;

        OBD_SLAB_ALLOC_PTR_GFP(opg, osc_page_kmem, CFS_ALLOC_IO);
        if (opg != NULL) {
                opg->ops_from = 0;
                opg->ops_to   = CFS_PAGE_SIZE;

                result = osc_prep_async_page(osc, opg, vmpage,
                                             cl_offset(obj, page->cp_index));
                if (result == 0) {
                        struct osc_io *oio = osc_env_io(env);
                        opg->ops_srvlock = osc_io_srvlock(oio);
                        cl_page_slice_add(page, &opg->ops_cl, obj,
                                          &osc_page_ops);
                }
                /*
                 * Cannot assert osc_page_protected() here as read-ahead
                 * creates temporary pages outside of a lock.
                 */
#ifdef INVARIANT_CHECK
                opg->ops_temp = !osc_page_protected(env, opg, CLM_READ, 1);
#endif
                CFS_INIT_LIST_HEAD(&opg->ops_inflight);
        } else
                result = -ENOMEM;
        return ERR_PTR(result);
}

/**
 * Helper function called by osc_io_submit() for every page in an immediate
 * transfer (i.e., transferred synchronously).
 */
void osc_io_submit_page(const struct lu_env *env,
                        struct osc_io *oio, struct osc_page *opg,
                        enum cl_req_type crt)
{
        struct osc_async_page *oap = &opg->ops_oap;
        struct client_obd     *cli = oap->oap_cli;
        int flags = 0;

        LINVRNT(osc_page_protected(env, opg,
                                   crt == CRT_WRITE ? CLM_WRITE : CLM_READ, 1));

        oap->oap_page_off   = opg->ops_from;
        oap->oap_count      = opg->ops_to - opg->ops_from;
        /* Give a hint to OST that requests are coming from kswapd - bug19529 */
        if (cfs_memory_pressure_get())
                oap->oap_brw_flags |= OBD_BRW_MEMALLOC;
        oap->oap_brw_flags |= OBD_BRW_SYNC;
        if (osc_io_srvlock(oio))
                oap->oap_brw_flags |= OBD_BRW_SRVLOCK;

        oap->oap_cmd = crt == CRT_WRITE ? OBD_BRW_WRITE : OBD_BRW_READ;
        if (!client_is_remote(osc_export(cl2osc(opg->ops_cl.cpl_obj))) &&
            cfs_capable(CFS_CAP_SYS_RESOURCE)) {
                oap->oap_brw_flags |= OBD_BRW_NOQUOTA;
                oap->oap_cmd |= OBD_BRW_NOQUOTA;
        }

        if (oap->oap_cmd & OBD_BRW_READ)
                flags = ASYNC_COUNT_STABLE;
        else if (!(oap->oap_brw_page.flag & OBD_BRW_FROM_GRANT))
                osc_enter_cache_try(env, cli, oap, 1);

        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags |= OSC_FLAGS | flags;
        cfs_spin_unlock(&oap->oap_lock);

        osc_oap_to_pending(oap);
        osc_page_transfer_get(opg, "transfer\0imm");
        osc_page_transfer_add(env, opg, crt);
}

/** @} osc */

/** \addtogroup osc cache
 *  @{
 */
#define OSC_IO_DEBUG(OSC, STR, args...)                                  \
        CDEBUG(D_INODE, "loi ready %d wr %d:%d rd %d:%d " STR,           \
               !cfs_list_empty(&(OSC)->oo_ready_item) ||                 \
               !cfs_list_empty(&(OSC)->oo_hp_ready_item),                \
               (OSC)->oo_write_pages.oop_num_pending,                    \
               !cfs_list_empty(&(OSC)->oo_write_pages.oop_urgent),       \
               (OSC)->oo_read_pages.oop_num_pending,                     \
               !cfs_list_empty(&(OSC)->oo_read_pages.oop_urgent),        \
               args)

static int osc_max_rpc_in_flight(struct client_obd *cli, struct osc_object *osc)
{
        struct osc_async_page *oap;
        int hprpc = 0;

        if (!cfs_list_empty(&osc->oo_write_pages.oop_urgent)) {
                oap = cfs_list_entry(osc->oo_write_pages.oop_urgent.next,
                                     struct osc_async_page, oap_urgent_item);
                hprpc = !!(oap->oap_async_flags & ASYNC_HP);
        }

        if (!hprpc && !cfs_list_empty(&osc->oo_read_pages.oop_urgent)) {
                oap = cfs_list_entry(osc->oo_read_pages.oop_urgent.next,
                                     struct osc_async_page, oap_urgent_item);
                hprpc = !!(oap->oap_async_flags & ASYNC_HP);
        }

        return rpcs_in_flight(cli) >= cli->cl_max_rpcs_in_flight + hprpc;
}

/* This maintains the lists of pending pages to read/write for a given object
 * (lop).  This is used by osc_check_rpcs->osc_next_obj() and osc_list_maint()
 * to quickly find objects that are ready to send an RPC. */
static int osc_makes_rpc(struct client_obd *cli, struct osc_object *osc,
                         int cmd)
{
        struct osc_oap_pages *lop;
        ENTRY;

        if (cmd & OBD_BRW_WRITE) {
                lop = &osc->oo_write_pages;
        } else {
                lop = &osc->oo_read_pages;
        }

        if (lop->oop_num_pending == 0)
                RETURN(0);

        /* if we have an invalid import we want to drain the queued pages
         * by forcing them through rpcs that immediately fail and complete
         * the pages.  recovery relies on this to empty the queued pages
         * before canceling the locks and evicting down the llite pages */
        if (cli->cl_import == NULL || cli->cl_import->imp_invalid)
                RETURN(1);

        /* stream rpcs in queue order as long as as there is an urgent page
         * queued.  this is our cheap solution for good batching in the case
         * where writepage marks some random page in the middle of the file
         * as urgent because of, say, memory pressure */
        if (!cfs_list_empty(&lop->oop_urgent)) {
                CDEBUG(D_CACHE, "urgent request forcing RPC\n");
                RETURN(1);
        }

        if (cmd & OBD_BRW_WRITE) {
                /* trigger a write rpc stream as long as there are dirtiers
                 * waiting for space.  as they're waiting, they're not going to
                 * create more pages to coalesce with what's waiting.. */
                if (!cfs_list_empty(&cli->cl_cache_waiters)) {
                        CDEBUG(D_CACHE, "cache waiters forcing RPC\n");
                        RETURN(1);
                }
        }
        if (lop->oop_num_pending >= cli->cl_max_pages_per_rpc)
                RETURN(1);

        RETURN(0);
}

static void lop_update_pending(struct client_obd *cli,
                               struct osc_oap_pages *lop, int cmd, int delta)
{
        lop->oop_num_pending += delta;
        if (cmd & OBD_BRW_WRITE)
                cli->cl_pending_w_pages += delta;
        else
                cli->cl_pending_r_pages += delta;
}

static int osc_prep_async_page(struct osc_object *osc, struct osc_page *ops,
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
        CFS_INIT_LIST_HEAD(&oap->oap_urgent_item);
        CFS_INIT_LIST_HEAD(&oap->oap_rpc_item);

        cfs_spin_lock_init(&oap->oap_lock);
        CDEBUG(D_CACHE, "oap %p page %p obj off "LPU64"\n",
               oap, page, oap->oap_obj_off);
        RETURN(0);
}

static int osc_queue_async_io(const struct lu_env *env, struct osc_page *ops)
{
        struct osc_async_page *oap = &ops->ops_oap;
        struct client_obd     *cli = oap->oap_cli;
        struct osc_object     *osc = oap->oap_obj;
        struct obd_export     *exp = osc_export(osc);
        int brw_flags = OBD_BRW_ASYNC;
        int cmd = OBD_BRW_WRITE;
        int rc = 0;
        ENTRY;

        if (oap->oap_magic != OAP_MAGIC)
                RETURN(-EINVAL);

        if (cli->cl_import == NULL || cli->cl_import->imp_invalid)
                RETURN(-EIO);

        if (!cfs_list_empty(&oap->oap_pending_item) ||
            !cfs_list_empty(&oap->oap_urgent_item) ||
            !cfs_list_empty(&oap->oap_rpc_item))
                RETURN(-EBUSY);

        /* Set the OBD_BRW_SRVLOCK before the page is queued. */
        brw_flags |= ops->ops_srvlock ? OBD_BRW_SRVLOCK : 0;
        if (!client_is_remote(exp) && cfs_capable(CFS_CAP_SYS_RESOURCE)) {
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
                if (rc == 0 &&
                    osc_quota_chkdq(cli, qid) == NO_QUOTA)
                        rc = -EDQUOT;
                if (rc)
                        RETURN(rc);
        }

        client_obd_list_lock(&cli->cl_loi_list_lock);

        oap->oap_cmd = cmd;
        oap->oap_page_off = ops->ops_from;
        oap->oap_count = ops->ops_to - ops->ops_from;
        oap->oap_async_flags = 0;
        oap->oap_brw_flags = brw_flags;
        /* Give a hint to OST that requests are coming from kswapd - bug19529 */
        if (cfs_memory_pressure_get())
                oap->oap_brw_flags |= OBD_BRW_MEMALLOC;

        rc = osc_enter_cache(env, cli, oap);
        if (rc) {
                client_obd_list_unlock(&cli->cl_loi_list_lock);
                RETURN(rc);
        }

        OSC_IO_DEBUG(osc, "oap %p page %p added for cmd %d\n",
                     oap, oap->oap_page, cmd);

        osc_oap_to_pending(oap);
        osc_list_maint(cli, osc);
        if (!osc_max_rpc_in_flight(cli, osc) &&
            osc_makes_rpc(cli, osc, OBD_BRW_WRITE)) {
                LASSERT(cli->cl_writeback_work != NULL);
                rc = ptlrpcd_queue_work(cli->cl_writeback_work);

                CDEBUG(D_CACHE, "Queued writeback work for client obd %p/%d.\n",
                       cli, rc);
        }
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        RETURN(0);
}

static int osc_teardown_async_page(struct osc_object *obj, struct osc_page *ops)
{
        struct osc_async_page *oap = &ops->ops_oap;
        struct client_obd     *cli = oap->oap_cli;
        struct osc_oap_pages  *lop;
        int rc = 0;
        ENTRY;

        if (oap->oap_magic != OAP_MAGIC)
                RETURN(-EINVAL);

        if (oap->oap_cmd & OBD_BRW_WRITE) {
                lop = &obj->oo_write_pages;
        } else {
                lop = &obj->oo_read_pages;
        }

        client_obd_list_lock(&cli->cl_loi_list_lock);

        if (!cfs_list_empty(&oap->oap_rpc_item))
                GOTO(out, rc = -EBUSY);

        osc_exit_cache(cli, oap, 0);
        osc_wake_cache_waiters(cli);

        if (!cfs_list_empty(&oap->oap_urgent_item)) {
                cfs_list_del_init(&oap->oap_urgent_item);
                cfs_spin_lock(&oap->oap_lock);
                oap->oap_async_flags &= ~(ASYNC_URGENT | ASYNC_HP);
                cfs_spin_unlock(&oap->oap_lock);
        }
        if (!cfs_list_empty(&oap->oap_pending_item)) {
                cfs_list_del_init(&oap->oap_pending_item);
                lop_update_pending(cli, lop, oap->oap_cmd, -1);
        }
        osc_list_maint(cli, obj);
        OSC_IO_DEBUG(obj, "oap %p page %p torn down\n", oap, oap->oap_page);
out:
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        RETURN(rc);
}

/* aka (~was & now & flag), but this is more clear :) */
#define SETTING(was, now, flag) (!(was & flag) && (now & flag))

int osc_set_async_flags(struct client_obd *cli, struct osc_object *obj,
                        struct osc_async_page *oap, obd_flag async_flags)
{
        struct osc_oap_pages *lop;
        int flags = 0;
        ENTRY;

        LASSERT(!cfs_list_empty(&oap->oap_pending_item));

        if (oap->oap_cmd & OBD_BRW_WRITE) {
                lop = &obj->oo_write_pages;
        } else {
                lop = &obj->oo_read_pages;
        }

        if ((oap->oap_async_flags & async_flags) == async_flags)
                RETURN(0);

        if (SETTING(oap->oap_async_flags, async_flags, ASYNC_READY))
                flags |= ASYNC_READY;

        if (SETTING(oap->oap_async_flags, async_flags, ASYNC_URGENT) &&
            cfs_list_empty(&oap->oap_rpc_item)) {
                if (oap->oap_async_flags & ASYNC_HP)
                        cfs_list_add(&oap->oap_urgent_item, &lop->oop_urgent);
                else
                        cfs_list_add_tail(&oap->oap_urgent_item,
                                          &lop->oop_urgent);
                flags |= ASYNC_URGENT;
                osc_list_maint(cli, obj);
        }
        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags |= flags;
        cfs_spin_unlock(&oap->oap_lock);

        OSC_IO_DEBUG(obj, "oap %p page %p has flags %x\n", oap,
                     oap->oap_page, oap->oap_async_flags);
        RETURN(0);
}

static int osc_makes_hprpc(struct osc_oap_pages *lop)
{
        struct osc_async_page *oap;
        ENTRY;

        if (cfs_list_empty(&lop->oop_urgent))
                RETURN(0);

        oap = cfs_list_entry(lop->oop_urgent.next,
                         struct osc_async_page, oap_urgent_item);

        if (oap->oap_async_flags & ASYNC_HP) {
                CDEBUG(D_CACHE, "hp request forcing RPC\n");
                RETURN(1);
        }

        RETURN(0);
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
static void osc_list_maint(struct client_obd *cli, struct osc_object *osc)
{
        if (osc_makes_hprpc(&osc->oo_write_pages) ||
            osc_makes_hprpc(&osc->oo_read_pages)) {
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
                osc->oo_write_pages.oop_num_pending);

        on_list(&osc->oo_read_item, &cli->cl_loi_read_list,
                osc->oo_read_pages.oop_num_pending);
}

/**
 * this is called when a sync waiter receives an interruption.  Its job is to
 * get the caller woken as soon as possible.  If its page hasn't been put in an
 * rpc yet it can dequeue immediately.  Otherwise it has to mark the rpc as
 * desiring interruption which will forcefully complete the rpc once the rpc
 * has timed out.
 */
static int osc_oap_interrupted(const struct lu_env *env,
                               struct osc_async_page *oap)
{
        int rc = -EBUSY;
        ENTRY;

        LASSERT(!oap->oap_interrupted);
        oap->oap_interrupted = 1;

        /* ok, it's been put in an rpc. only one oap gets a request reference */
        if (oap->oap_request != NULL) {
                ptlrpc_mark_interrupted(oap->oap_request);
                ptlrpcd_wake(oap->oap_request);
                ptlrpc_req_finished(oap->oap_request);
                oap->oap_request = NULL;
        }

        /*
         * page completion may be called only if ->cpo_prep() method was
         * executed by osc_io_submit(), that also adds page the to pending list
         */
        if (!cfs_list_empty(&oap->oap_pending_item)) {
                struct osc_oap_pages *lop;
                struct osc_object *osc = oap->oap_obj;

                cfs_list_del_init(&oap->oap_pending_item);
                cfs_list_del_init(&oap->oap_urgent_item);

                lop = (oap->oap_cmd & OBD_BRW_WRITE) ?
                        &osc->oo_write_pages : &osc->oo_read_pages;
                lop_update_pending(oap->oap_cli, lop, oap->oap_cmd, -1);
                osc_list_maint(oap->oap_cli, osc);
                rc = osc_completion(env, oap, oap->oap_cmd, NULL, -EINTR);
        }

        RETURN(rc);
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

static void osc_oap_to_pending(struct osc_async_page *oap)
{
        struct osc_object    *osc = oap->oap_obj;
        struct osc_oap_pages *lop;

        if (oap->oap_cmd & OBD_BRW_WRITE)
                lop = &osc->oo_write_pages;
        else
                lop = &osc->oo_read_pages;

        if (oap->oap_async_flags & ASYNC_HP)
                cfs_list_add(&oap->oap_urgent_item, &lop->oop_urgent);
        else if (oap->oap_async_flags & ASYNC_URGENT)
                cfs_list_add_tail(&oap->oap_urgent_item, &lop->oop_urgent);
        cfs_list_add_tail(&oap->oap_pending_item, &lop->oop_pending);
        lop_update_pending(oap->oap_cli, lop, oap->oap_cmd, 1);
}

/* this must be called holding the loi list lock to give coverage to exit_cache,
 * async_flag maintenance, and oap_request */
void osc_ap_completion(const struct lu_env *env, struct client_obd *cli,
                       struct obdo *oa, struct osc_async_page *oap,
                       int sent, int rc)
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

        cfs_spin_lock(&oap->oap_lock);
        oap->oap_async_flags = 0;
        cfs_spin_unlock(&oap->oap_lock);
        oap->oap_interrupted = 0;

        if (oap->oap_cmd & OBD_BRW_WRITE) {
                osc_process_ar(&cli->cl_ar, xid, rc);
                osc_process_ar(&loi->loi_ar, xid, rc);
        }

        if (rc == 0 && oa != NULL) {
                if (oa->o_valid & OBD_MD_FLBLOCKS)
                        loi->loi_lvb.lvb_blocks = oa->o_blocks;
                if (oa->o_valid & OBD_MD_FLMTIME)
                        loi->loi_lvb.lvb_mtime = oa->o_mtime;
                if (oa->o_valid & OBD_MD_FLATIME)
                        loi->loi_lvb.lvb_atime = oa->o_atime;
                if (oa->o_valid & OBD_MD_FLCTIME)
                        loi->loi_lvb.lvb_ctime = oa->o_ctime;
        }

        rc = osc_completion(env, oap, oap->oap_cmd, oa, rc);

        /* cl_page_completion() drops PG_locked. so, a new I/O on the page could
         * start, but OSC calls it under lock and thus we can add oap back to
         * pending safely */
        if (rc)
                /* upper layer wants to leave the page on pending queue */
                osc_oap_to_pending(oap);
        else
                osc_exit_cache(cli, oap, sent);
        EXIT;
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
osc_send_oap_rpc(const struct lu_env *env, struct client_obd *cli,
                 struct osc_object *osc, int cmd,
                 struct osc_oap_pages *lop, pdl_policy_t pol)
{
        struct ptlrpc_request *req;
        obd_count page_count = 0;
        struct osc_async_page *oap = NULL, *tmp;
        CFS_LIST_HEAD(rpc_list);
        int srvlock = 0, mem_tight = 0;
        obd_off starting_offset = OBD_OBJECT_EOF;
        unsigned int ending_offset;
        int starting_page_off = 0;
        ENTRY;

        /* ASYNC_HP pages first. At present, when the lock the pages is
         * to be canceled, the pages covered by the lock will be sent out
         * with ASYNC_HP. We have to send out them as soon as possible. */
        cfs_list_for_each_entry_safe(oap, tmp, &lop->oop_urgent, oap_urgent_item) {
                if (oap->oap_async_flags & ASYNC_HP)
                        cfs_list_move(&oap->oap_pending_item, &lop->oop_pending);
                if (++page_count >= cli->cl_max_pages_per_rpc)
                        break;
        }
        page_count = 0;

        /* first we find the pages we're allowed to work with */
        cfs_list_for_each_entry_safe(oap, tmp, &lop->oop_pending,
                                     oap_pending_item) {
                LASSERTF(oap->oap_magic == OAP_MAGIC, "Bad oap magic: oap %p, "
                         "magic 0x%x\n", oap, oap->oap_magic);

                if (page_count != 0 &&
                    srvlock != !!(oap->oap_brw_flags & OBD_BRW_SRVLOCK)) {
                        CDEBUG(D_PAGE, "SRVLOCK flag mismatch,"
                               " oap %p, page %p, srvlock %u\n",
                               oap, oap->oap_brw_page.pg, (unsigned)!srvlock);
                        break;
                }

                /* If there is a gap at the start of this page, it can't merge
                 * with any previous page, so we'll hand the network a
                 * "fragmented" page array that it can't transfer in 1 RDMA */
                if (oap->oap_obj_off < starting_offset) {
                        if (starting_page_off != 0)
                                break;

                        starting_page_off = oap->oap_page_off;
                        starting_offset = oap->oap_obj_off + starting_page_off;
                } else if (oap->oap_page_off != 0)
                        break;

                /* in llite being 'ready' equates to the page being locked
                 * until completion unlocks it.  commit_write submits a page
                 * as not ready because its unlock will happen unconditionally
                 * as the call returns.  if we race with commit_write giving
                 * us that page we don't want to create a hole in the page
                 * stream, so we stop and leave the rpc to be fired by
                 * another dirtier or kupdated interval (the not ready page
                 * will still be on the dirty list).  we could call in
                 * at the end of ll_file_write to process the queue again. */
                if (!(oap->oap_async_flags & ASYNC_READY)) {
                        int rc = osc_make_ready(env, oap, cmd);
                        if (rc < 0)
                                CDEBUG(D_INODE, "oap %p page %p returned %d "
                                                "instead of ready\n", oap,
                                                oap->oap_page, rc);
                        switch (rc) {
                        case -EAGAIN:
                                /* llite is telling us that the page is still
                                 * in commit_write and that we should try
                                 * and put it in an rpc again later.  we
                                 * break out of the loop so we don't create
                                 * a hole in the sequence of pages in the rpc
                                 * stream.*/
                                oap = NULL;
                                break;
                        case -EINTR:
                                /* the io isn't needed.. tell the checks
                                 * below to complete the rpc with EINTR */
                                cfs_spin_lock(&oap->oap_lock);
                                oap->oap_async_flags |= ASYNC_COUNT_STABLE;
                                cfs_spin_unlock(&oap->oap_lock);
                                oap->oap_count = -EINTR;
                                break;
                        case 0:
                                cfs_spin_lock(&oap->oap_lock);
                                oap->oap_async_flags |= ASYNC_READY;
                                cfs_spin_unlock(&oap->oap_lock);
                                break;
                        default:
                                LASSERTF(0, "oap %p page %p returned %d "
                                            "from make_ready\n", oap,
                                            oap->oap_page, rc);
                                break;
                        }
                }
                if (oap == NULL)
                        break;

                /* take the page out of our book-keeping */
                cfs_list_del_init(&oap->oap_pending_item);
                lop_update_pending(cli, lop, cmd, -1);
                cfs_list_del_init(&oap->oap_urgent_item);

                /* ask the caller for the size of the io as the rpc leaves. */
                if (!(oap->oap_async_flags & ASYNC_COUNT_STABLE)) {
                        oap->oap_count = osc_refresh_count(env, oap, cmd);
                        LASSERT(oap->oap_page_off + oap->oap_count <= CFS_PAGE_SIZE);
                }
                if (oap->oap_count <= 0) {
                        CDEBUG(D_CACHE, "oap %p count %d, completing\n", oap,
                               oap->oap_count);
                        osc_ap_completion(env, cli, NULL,
                                          oap, 0, oap->oap_count);
                        continue;
                }

                /* now put the page back in our accounting */
                cfs_list_add_tail(&oap->oap_rpc_item, &rpc_list);
                if (page_count++ == 0)
                        srvlock = !!(oap->oap_brw_flags & OBD_BRW_SRVLOCK);

                if (oap->oap_brw_flags & OBD_BRW_MEMALLOC)
                        mem_tight = 1;

                /* End on a PTLRPC_MAX_BRW_SIZE boundary.  We want full-sized
                 * RPCs aligned on PTLRPC_MAX_BRW_SIZE boundaries to help reads
                 * have the same alignment as the initial writes that allocated
                 * extents on the server. */
                ending_offset = oap->oap_obj_off + oap->oap_page_off +
                                oap->oap_count;
                if (!(ending_offset & (PTLRPC_MAX_BRW_SIZE - 1)))
                        break;

                if (page_count >= cli->cl_max_pages_per_rpc)
                        break;

                /* If there is a gap at the end of this page, it can't merge
                 * with any subsequent pages, so we'll hand the network a
                 * "fragmented" page array that it can't transfer in 1 RDMA */
                if (oap->oap_page_off + oap->oap_count < CFS_PAGE_SIZE)
                        break;
        }

        osc_wake_cache_waiters(cli);

        osc_list_maint(cli, osc);

        client_obd_list_unlock(&cli->cl_loi_list_lock);

        if (page_count == 0) {
                client_obd_list_lock(&cli->cl_loi_list_lock);
                RETURN(0);
        }

        req = osc_build_brw_req(env, cli, &rpc_list, page_count,
                                mem_tight ? (cmd | OBD_BRW_MEMALLOC) : cmd);
        if (IS_ERR(req)) {
                LASSERT(cfs_list_empty(&rpc_list));
                osc_list_maint(cli, osc);
                RETURN(PTR_ERR(req));
        }

        starting_offset &= PTLRPC_MAX_BRW_SIZE - 1;
        if (cmd == OBD_BRW_READ) {
                lprocfs_oh_tally_log2(&cli->cl_read_page_hist, page_count);
                lprocfs_oh_tally(&cli->cl_read_rpc_hist, cli->cl_r_in_flight);
                lprocfs_oh_tally_log2(&cli->cl_read_offset_hist,
                                      (starting_offset >> CFS_PAGE_SHIFT) + 1);
        } else {
                lprocfs_oh_tally_log2(&cli->cl_write_page_hist, page_count);
                lprocfs_oh_tally(&cli->cl_write_rpc_hist,
                                 cli->cl_w_in_flight);
                lprocfs_oh_tally_log2(&cli->cl_write_offset_hist,
                                      (starting_offset >> CFS_PAGE_SHIFT) + 1);
        }

        if (cmd == OBD_BRW_READ)
                cli->cl_r_in_flight++;
        else
                cli->cl_w_in_flight++;

        /* XXX: Maybe the caller can check the RPC bulk descriptor to see which
         *      CPU/NUMA node the majority of pages were allocated on, and try
         *      to assign the async RPC to the CPU core (PDL_POLICY_PREFERRED)
         *      to reduce cross-CPU memory traffic.
         *
         *      But on the other hand, we expect that multiple ptlrpcd threads
         *      and the initial write sponsor can run in parallel, especially
         *      when data checksum is enabled, which is CPU-bound operation and
         *      single ptlrpcd thread cannot process in time. So more ptlrpcd
         *      threads sharing BRW load (with PDL_POLICY_ROUND) seems better.
         */
        ptlrpcd_add_req(req, pol, -1);
        RETURN(1);
}

#define list_to_obj(list, item) \
        cfs_list_entry((list)->next, struct osc_object, oo_##item)

/* This is called by osc_check_rpcs() to find which objects have pages that
 * we could be sending.  These lists are maintained by osc_makes_rpc(). */
struct osc_object *osc_next_obj(struct client_obd *cli)
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
        int rc = 0, race_counter = 0;
        ENTRY;

        while ((osc = osc_next_obj(cli)) != NULL) {
                OSC_IO_DEBUG(osc, "%lu in flight\n", rpcs_in_flight(cli));

                if (osc_max_rpc_in_flight(cli, osc))
                        break;

                /* attempt some read/write balancing by alternating between
                 * reads and writes in an object.  The makes_rpc checks here
                 * would be redundant if we were getting read/write work items
                 * instead of objects.  we don't want send_oap_rpc to drain a
                 * partial read pending queue when we're given this object to
                 * do io on writes while there are cache waiters */
                if (osc_makes_rpc(cli, osc, OBD_BRW_WRITE)) {
                        rc = osc_send_oap_rpc(env, cli, osc, OBD_BRW_WRITE,
                                              &osc->oo_write_pages, pol);
                        if (rc < 0) {
                                CERROR("Write request failed with %d\n", rc);

                                /* osc_send_oap_rpc failed, mostly because of
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

                        if (rc > 0)
                                race_counter = 0;
                        else if (rc == 0)
                                race_counter++;
                }
                if (osc_makes_rpc(cli, osc, OBD_BRW_READ)) {
                        rc = osc_send_oap_rpc(env, cli, osc, OBD_BRW_READ,
                                              &osc->oo_read_pages, pol);
                        if (rc < 0)
                                CERROR("Read request failed with %d\n", rc);

                        if (rc > 0)
                                race_counter = 0;
                        else if (rc == 0)
                                race_counter++;
                }

                /* attempt some inter-object balancing by issuing rpcs
                 * for each object in turn */
                if (!cfs_list_empty(&osc->oo_hp_ready_item))
                        cfs_list_del_init(&osc->oo_hp_ready_item);
                if (!cfs_list_empty(&osc->oo_ready_item))
                        cfs_list_del_init(&osc->oo_ready_item);
                if (!cfs_list_empty(&osc->oo_write_item))
                        cfs_list_del_init(&osc->oo_write_item);
                if (!cfs_list_empty(&osc->oo_read_item))
                        cfs_list_del_init(&osc->oo_read_item);

                osc_list_maint(cli, osc);

                /* send_oap_rpc fails with 0 when make_ready tells it to
                 * back off.  llite's make_ready does this when it tries
                 * to lock a page queued for write that is already locked.
                 * we want to try sending rpcs from many objects, but we
                 * don't want to spin failing with 0.  */
                if (race_counter == 10)
                        break;
        }
}

void osc_io_unplug(const struct lu_env *env, struct client_obd *cli,
                   struct osc_object *osc, pdl_policy_t pol)
{
        if (osc)
                osc_list_maint(cli, osc);
        osc_check_rpcs(env, cli, pol);
}

/** @} osc cache */
