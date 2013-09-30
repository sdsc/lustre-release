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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Implementation of cl_page for OSC layer.
 *
 *   Author: Nikita Danilov <nikita.danilov@sun.com>
 *   Author: Jinshan Xiong <jinshan.xiong@intel.com>
 */

#define DEBUG_SUBSYSTEM S_OSC

#include "osc_cl_internal.h"

static void osc_lru_del(struct client_obd *cli, struct osc_page *opg);
static void osc_lru_use(struct client_obd *cli, struct osc_page *opg);
static int osc_lru_reserve(const struct lu_env *env, struct osc_object *obj,
			   struct osc_page *opg);

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
	__u64                   flags;

	might_sleep();

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
		spin_lock(&hdr->coh_lock_guard);
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
		spin_unlock(&hdr->coh_lock_guard);
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
}

void osc_page_transfer_get(struct osc_page *opg, const char *label)
{
        struct cl_page *page = cl_page_top(opg->ops_cl.cpl_page);

        LASSERT(!opg->ops_transfer_pinned);
        cl_page_get(page);
        lu_ref_add_atomic(&page->cp_reference, label, page);
        opg->ops_transfer_pinned = 1;
}

void osc_page_transfer_put(const struct lu_env *env, struct osc_page *opg)
{
	struct cl_page *page = cl_page_top(opg->ops_cl.cpl_page);

	if (opg->ops_transfer_pinned) {
		opg->ops_transfer_pinned = 0;
		lu_ref_del(&page->cp_reference, "transfer", page);
		cl_page_put(env, page);
	}
}

/**
 * This is called once for every page when it is submitted for a transfer
 * either opportunistic (osc_page_cache_add()), or immediate
 * (osc_page_submit()).
 */
void osc_page_transfer_add(const struct lu_env *env, struct osc_page *opg,
			   enum cl_req_type crt)
{
	struct osc_object *obj = cl2osc(opg->ops_cl.cpl_obj);

	/* ops_lru and ops_inflight share the same field, so take it from LRU
	 * first and then use it as inflight. */
	osc_lru_use(osc_cli(obj), opg);

	spin_lock(&obj->oo_seatbelt);
	cfs_list_add(&opg->ops_inflight, &obj->oo_inflight[crt]);
	opg->ops_submitter = current;
	spin_unlock(&obj->oo_seatbelt);
}

int osc_page_cache_add(const struct lu_env *env,
			const struct cl_page_slice *slice, struct cl_io *io)
{
	struct osc_page *opg = cl2osc_page(slice);
	int result;
	ENTRY;

	LINVRNT(osc_page_protected(env, opg, CLM_WRITE, 0));

	osc_page_transfer_get(opg, "transfer\0cache");
	result = osc_queue_async_io(env, io, opg);
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
                rc = -ENODATA;
        } else {
		cl_lock_get(lock);
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

        cl_lock_put(env, lock);
}

static int osc_page_is_under_lock(const struct lu_env *env,
                                  const struct cl_page_slice *slice,
                                  struct cl_io *unused)
{
        struct cl_lock *lock;
        int             result = -ENODATA;

        ENTRY;
        lock = cl_lock_at_page(env, slice->cpl_obj, slice->cpl_page,
                               NULL, 1, 0);
        if (lock != NULL) {
		if (osc_page_addref_lock(env, cl2osc_page(slice), lock) == 0)
			result = -EBUSY;
		cl_lock_put(env, lock);
	}
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
	struct osc_page   *opg = cl2osc_page(slice);

	if (likely(opg->ops_lock))
		osc_page_putref_lock(env, opg);
}

static void osc_page_completion_write(const struct lu_env *env,
				      const struct cl_page_slice *slice,
				      int ioret)
{
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
			  "1< %#x %d %u %s %s > "
			  "2< "LPU64" %u %u %#x %#x | %p %p %p > "
			  "3< %s %p %d %lu %d > "
			  "4< %d %d %d %lu %s | %s %s %s %s > "
			  "5< %s %s %s %s | %d %s | %d %s %s>\n",
                          opg,
                          /* 1 */
                          oap->oap_magic, oap->oap_cmd,
                          oap->oap_interrupted,
                          osc_list(&oap->oap_pending_item),
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
			  cfs_atomic_read(&obj->oo_nr_reads),
			  osc_list(&obj->oo_reading_exts),
			  cfs_atomic_read(&obj->oo_nr_writes),
			  osc_list(&obj->oo_hp_exts),
			  osc_list(&obj->oo_urgent_exts));
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
	rc = osc_teardown_async_page(env, obj, opg);
        if (rc) {
                CL_PAGE_DEBUG(D_ERROR, env, cl_page_top(slice->cpl_page),
                              "Trying to teardown failed: %d\n", rc);
                LASSERT(0);
        }

	spin_lock(&obj->oo_seatbelt);
	if (opg->ops_submitter != NULL) {
		LASSERT(!cfs_list_empty(&opg->ops_inflight));
		cfs_list_del_init(&opg->ops_inflight);
		opg->ops_submitter = NULL;
	}
	spin_unlock(&obj->oo_seatbelt);

	osc_lru_del(osc_cli(obj), opg);

	if (slice->cpl_page->cp_type == CPT_CACHEABLE) {
		void *value;

		spin_lock(&obj->oo_tree_lock);
		value = radix_tree_delete(&obj->oo_tree, osc_index(opg));
		if (value != NULL)
			--obj->oo_npages;
		spin_unlock(&obj->oo_tree_lock);

		LASSERT(ergo(value != NULL, value == opg));
	}

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
	spin_lock(&oap->oap_lock);
	oap->oap_async_flags |= ASYNC_COUNT_STABLE;
	spin_unlock(&oap->oap_lock);
}

static int osc_page_cancel(const struct lu_env *env,
                           const struct cl_page_slice *slice)
{
	struct osc_page *opg = cl2osc_page(slice);
        int rc = 0;

        LINVRNT(osc_page_protected(env, opg, CLM_READ, 0));

        /* Check if the transferring against this page
         * is completed, or not even queued. */
        if (opg->ops_transfer_pinned)
                /* FIXME: may not be interrupted.. */
		rc = osc_cancel_async_page(env, opg);
        LASSERT(ergo(rc == 0, opg->ops_transfer_pinned == 0));
        return rc;
}

static int osc_page_flush(const struct lu_env *env,
			  const struct cl_page_slice *slice,
			  struct cl_io *io)
{
	struct osc_page *opg = cl2osc_page(slice);
	int rc = 0;
	ENTRY;
	rc = osc_flush_async_page(env, io, opg);
	RETURN(rc);
}

static const struct cl_page_operations osc_page_ops = {
        .cpo_fini          = osc_page_fini,
        .cpo_print         = osc_page_print,
        .cpo_delete        = osc_page_delete,
        .cpo_is_under_lock = osc_page_is_under_lock,
        .cpo_disown        = osc_page_disown,
        .io = {
                [CRT_READ] = {
                        .cpo_completion = osc_page_completion_read
                },
                [CRT_WRITE] = {
			.cpo_completion = osc_page_completion_write
		}
	},
	.cpo_clip           = osc_page_clip,
	.cpo_cancel         = osc_page_cancel,
	.cpo_flush          = osc_page_flush
};

int osc_page_init(const struct lu_env *env, struct cl_object *obj,
		  struct cl_page *page, struct page *vmpage)
{
	struct osc_object *osc = cl2osc(obj);
	struct osc_page   *opg = cl_object_page_slice(obj, page);
	int result;

	opg->ops_from = 0;
	opg->ops_to   = PAGE_CACHE_SIZE;

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
#ifdef CONFIG_LUSTRE_DEBUG_EXPENSIVE_CHECK
	opg->ops_temp = !osc_page_protected(env, opg, CLM_READ, 1);
#endif
	/* ops_inflight and ops_lru are the same field, but it doesn't
	 * hurt to initialize it twice :-) */
	CFS_INIT_LIST_HEAD(&opg->ops_inflight);
	CFS_INIT_LIST_HEAD(&opg->ops_lru);

	/* reserve an LRU space for this page */
	if (page->cp_type == CPT_CACHEABLE && result == 0) {
		result = osc_lru_reserve(env, osc, opg);
		if (result == 0) {
			spin_lock(&osc->oo_tree_lock);
			result = radix_tree_insert(&osc->oo_tree,
						   page->cp_index, opg);
			if (result == 0)
				++osc->oo_npages;
			spin_unlock(&osc->oo_tree_lock);
			LASSERT(result == 0);
		}
	}

	return result;
}

/**
 * Helper function called by osc_io_submit() for every page in an immediate
 * transfer (i.e., transferred synchronously).
 */
void osc_page_submit(const struct lu_env *env, struct osc_page *opg,
		     enum cl_req_type crt, int brw_flags)
{
	struct osc_async_page *oap = &opg->ops_oap;
	struct osc_object     *obj = oap->oap_obj;

	LINVRNT(osc_page_protected(env, opg,
				   crt == CRT_WRITE ? CLM_WRITE : CLM_READ, 1));

	LASSERTF(oap->oap_magic == OAP_MAGIC, "Bad oap magic: oap %p, "
		 "magic 0x%x\n", oap, oap->oap_magic);
	LASSERT(oap->oap_async_flags & ASYNC_READY);
	LASSERT(oap->oap_async_flags & ASYNC_COUNT_STABLE);

	oap->oap_cmd       = crt == CRT_WRITE ? OBD_BRW_WRITE : OBD_BRW_READ;
	oap->oap_page_off  = opg->ops_from;
	oap->oap_count     = opg->ops_to - opg->ops_from;
	oap->oap_brw_flags = OBD_BRW_SYNC | brw_flags;

	if (!client_is_remote(osc_export(obj)) &&
			cfs_capable(CFS_CAP_SYS_RESOURCE)) {
		oap->oap_brw_flags |= OBD_BRW_NOQUOTA;
		oap->oap_cmd |= OBD_BRW_NOQUOTA;
	}

	opg->ops_submit_time = cfs_time_current();
	osc_page_transfer_get(opg, "transfer\0imm");
	osc_page_transfer_add(env, opg, crt);
}

/* --------------- LRU page management ------------------ */

/* OSC is a natural place to manage LRU pages as applications are specialized
 * to write OSC by OSC. Ideally, if one OSC is used more frequently it should
 * occupy more LRU slots. On the other hand, we should avoid using up all LRU
 * slots (client_obd::cl_lru_left) otherwise process has to be put into sleep
 * for free LRU slots - this will be very bad so the algorithm requires each
 * OSC to free slots voluntarily to maintain a reasonable number of free slots
 * at any time.
 */

static CFS_DECL_WAITQ(osc_lru_waitq);
static cfs_atomic_t osc_lru_waiters = CFS_ATOMIC_INIT(0);
/* LRU pages are freed in batch mode. OSC should at least free this
 * number of pages to avoid running out of LRU budget, and.. */
static const int lru_shrink_min = 2 << (20 - PAGE_CACHE_SHIFT); /* 2M */
/* free this number at most otherwise it will take too long time to finsih. */
static const int lru_shrink_max = 8 << (20 - PAGE_CACHE_SHIFT); /* 8M */

/* Check if we can free LRU slots from this OSC. If there exists LRU waiters,
 * we should free slots aggressively. In this way, slots are freed in a steady
 * step to maintain fairness among OSCs.
 *
 * Return how many LRU pages should be freed. */
static int osc_cache_too_much(struct client_obd *cli)
{
	struct cl_client_cache *cache = cli->cl_cache;
	int pages = cfs_atomic_read(&cli->cl_lru_in_list);
	unsigned long budget;

	budget = cache->ccc_lru_max / cfs_atomic_read(&cache->ccc_users);

	/* if it's going to run out LRU slots, we should free some, but not
	 * too much to maintain faireness among OSCs. */
	if (cfs_atomic_read(cli->cl_lru_left) < cache->ccc_lru_max >> 4) {
		if (pages >= budget)
			return lru_shrink_max;
		else if (pages >= budget / 2)
			return lru_shrink_min;
	} else if (pages >= budget * 2)
		return lru_shrink_min;
	return 0;
}

int lru_queue_work(const struct lu_env *env, void *data)
{
	struct client_obd *cli = data;

	CDEBUG(D_CACHE, "Run LRU work for client obd %p.\n", cli);

	while (osc_cache_too_much(cli))
		osc_lru_shrink(env, cli, lru_shrink_max, true);

	RETURN(0);
}

void osc_lru_add_batch(struct client_obd *cli, cfs_list_t *plist)
{
	CFS_LIST_HEAD(lru);
	struct osc_async_page *oap;
	int npages = 0;

	cfs_list_for_each_entry(oap, plist, oap_pending_item) {
		struct osc_page *opg = oap2osc_page(oap);

		if (!opg->ops_in_lru)
			continue;

		++npages;
		LASSERT(cfs_list_empty(&opg->ops_lru));
		cfs_list_add(&opg->ops_lru, &lru);
	}

	if (npages > 0) {
		client_obd_list_lock(&cli->cl_lru_list_lock);
		cfs_list_splice_tail(&lru, &cli->cl_lru_list);
		cfs_atomic_sub(npages, &cli->cl_lru_busy);
		cfs_atomic_add(npages, &cli->cl_lru_in_list);
		client_obd_list_unlock(&cli->cl_lru_list_lock);

		/* XXX: May set force to be true for better performance */
		if (osc_cache_too_much(cli))
			(void)ptlrpcd_queue_work(cli->cl_lru_work);
	}
}

static void __osc_lru_del(struct client_obd *cli, struct osc_page *opg)
{
	LASSERT(cfs_atomic_read(&cli->cl_lru_in_list) > 0);
	cfs_list_del_init(&opg->ops_lru);
	cfs_atomic_dec(&cli->cl_lru_in_list);
}

/**
 * Page is being destroyed. The page may be not in LRU list, if the transfer
 * has never finished(error occurred).
 */
static void osc_lru_del(struct client_obd *cli, struct osc_page *opg)
{
	if (opg->ops_in_lru) {
		client_obd_list_lock(&cli->cl_lru_list_lock);
		if (!cfs_list_empty(&opg->ops_lru)) {
			__osc_lru_del(cli, opg);
		} else {
			LASSERT(cfs_atomic_read(&cli->cl_lru_busy) > 0);
			cfs_atomic_dec(&cli->cl_lru_busy);
		}
		client_obd_list_unlock(&cli->cl_lru_list_lock);

		cfs_atomic_inc(cli->cl_lru_left);
		/* this is a great place to release more LRU pages if
		 * this osc occupies too many LRU pages and kernel is
		 * stealing one of them. */
		if (!memory_pressure_get())
			(void)ptlrpcd_queue_work(cli->cl_lru_work);
		wake_up(&osc_lru_waitq);
	} else {
		LASSERT(cfs_list_empty(&opg->ops_lru));
	}
}

/**
 * Delete page from LRUlist for redirty.
 */
static void osc_lru_use(struct client_obd *cli, struct osc_page *opg)
{
	/* If page is being transfered for the first time,
	 * ops_lru should be empty */
	if (opg->ops_in_lru && !cfs_list_empty(&opg->ops_lru)) {
		client_obd_list_lock(&cli->cl_lru_list_lock);
		__osc_lru_del(cli, opg);
		client_obd_list_unlock(&cli->cl_lru_list_lock);
		cfs_atomic_inc(&cli->cl_lru_busy);
	}
}

static void discard_pagevec(const struct lu_env *env, struct cl_io *io,
				struct cl_page **pvec, int max_index)
{
        int i;

        for (i = 0; i < max_index; i++) {
                struct cl_page *page = pvec[i];

		LASSERT(cl_page_is_owned(page, io));
		cl_page_discard(env, io, page);
		cl_page_disown(env, io, page);
                cl_page_put(env, page);

                pvec[i] = NULL;
        }
}

/**
 * Drop @target of pages from LRU at most.
 */
int osc_lru_shrink(const struct lu_env *env, struct client_obd *cli,
		   int target, bool force)
{
	struct cl_io *io;
	struct cl_object *clobj = NULL;
	struct cl_page **pvec;
	struct osc_page *opg;
	int maxscan = 0;
	int count = 0;
	int index = 0;
	int rc = 0;
	ENTRY;

	LASSERT(cfs_atomic_read(&cli->cl_lru_in_list) >= 0);
	if (cfs_atomic_read(&cli->cl_lru_in_list) == 0 || target <= 0)
		RETURN(0);

	if (!force) {
		if (cfs_atomic_read(&cli->cl_lru_shrinkers) > 0)
			RETURN(-EBUSY);

		if (cfs_atomic_inc_return(&cli->cl_lru_shrinkers) > 1) {
			cfs_atomic_dec(&cli->cl_lru_shrinkers);
			RETURN(-EBUSY);
		}
	} else {
		cfs_atomic_inc(&cli->cl_lru_shrinkers);
	}

	pvec = (struct cl_page **)osc_env_info(env)->oti_pvec;
	io = &osc_env_info(env)->oti_io;

	client_obd_list_lock(&cli->cl_lru_list_lock);
	maxscan = min(target << 1, cfs_atomic_read(&cli->cl_lru_in_list));
	while (!cfs_list_empty(&cli->cl_lru_list)) {
		struct cl_page *page;
		bool will_free = false;

		if (--maxscan < 0)
			break;

		opg = cfs_list_entry(cli->cl_lru_list.next, struct osc_page,
				     ops_lru);
		page = cl_page_top(opg->ops_cl.cpl_page);
		if (cl_page_in_use_noref(page)) {
			cfs_list_move_tail(&opg->ops_lru, &cli->cl_lru_list);
			continue;
		}

		LASSERT(page->cp_obj != NULL);
		if (clobj != page->cp_obj) {
			struct cl_object *tmp = page->cp_obj;

			cl_object_get(tmp);
			client_obd_list_unlock(&cli->cl_lru_list_lock);

			if (clobj != NULL) {
				discard_pagevec(env, io, pvec, index);
				index = 0;

				cl_io_fini(env, io);
				cl_object_put(env, clobj);
				clobj = NULL;
			}

			clobj = tmp;
			io->ci_obj = clobj;
			io->ci_ignore_layout = 1;
			rc = cl_io_init(env, io, CIT_MISC, clobj);

			client_obd_list_lock(&cli->cl_lru_list_lock);

			if (rc != 0)
				break;

			++maxscan;
			continue;
		}

		if (cl_page_own_try(env, io, page) == 0) {
			if (!cl_page_in_use_noref(page)) {
				/* remove it from lru list earlier to avoid
				 * lock contention */
				__osc_lru_del(cli, opg);
				opg->ops_in_lru = 0; /* will be discarded */

				cl_page_get(page);
				will_free = true;
			} else {
				cl_page_disown(env, io, page);
			}
		}

		if (!will_free) {
			cfs_list_move_tail(&opg->ops_lru, &cli->cl_lru_list);
			continue;
		}

		/* Don't discard and free the page with cl_lru_list held */
		pvec[index++] = page;
		if (unlikely(index == OTI_PVEC_SIZE)) {
			client_obd_list_unlock(&cli->cl_lru_list_lock);
			discard_pagevec(env, io, pvec, index);
			index = 0;

			client_obd_list_lock(&cli->cl_lru_list_lock);
		}

		if (++count >= target)
			break;
	}
	client_obd_list_unlock(&cli->cl_lru_list_lock);

	if (clobj != NULL) {
		discard_pagevec(env, io, pvec, index);

		cl_io_fini(env, io);
		cl_object_put(env, clobj);
	}

	cfs_atomic_dec(&cli->cl_lru_shrinkers);
	if (count > 0) {
		cfs_atomic_add(count, cli->cl_lru_left);
		wake_up_all(&osc_lru_waitq);
	}
	RETURN(count > 0 ? count : rc);
}

static inline int max_to_shrink(struct client_obd *cli)
{
	return min(cfs_atomic_read(&cli->cl_lru_in_list) >> 1, lru_shrink_max);
}

int osc_lru_reclaim(struct client_obd *cli)
{
	struct cl_env_nest nest;
	struct lu_env *env;
	struct cl_client_cache *cache = cli->cl_cache;
	int max_scans;
	int rc = 0;
	ENTRY;

	LASSERT(cache != NULL);
	LASSERT(!cfs_list_empty(&cache->ccc_lru));

	env = cl_env_nested_get(&nest);
	if (IS_ERR(env))
		RETURN(rc);

	rc = osc_lru_shrink(env, cli, osc_cache_too_much(cli), false);
	if (rc != 0) {
		if (rc == -EBUSY)
			rc = 0;

		CDEBUG(D_CACHE, "%s: Free %d pages from own LRU: %p.\n",
			cli->cl_import->imp_obd->obd_name, rc, cli);
		GOTO(out, rc);
	}

	CDEBUG(D_CACHE, "%s: cli %p no free slots, pages: %d, busy: %d.\n",
		cli->cl_import->imp_obd->obd_name, cli,
		cfs_atomic_read(&cli->cl_lru_in_list),
		cfs_atomic_read(&cli->cl_lru_busy));

	/* Reclaim LRU slots from other client_obd as it can't free enough
	 * from its own. This should rarely happen. */
	spin_lock(&cache->ccc_lru_lock);
	cache->ccc_lru_shrinkers++;
	cfs_list_move_tail(&cli->cl_lru_osc, &cache->ccc_lru);

	max_scans = cfs_atomic_read(&cache->ccc_users);
	while (--max_scans > 0 && !cfs_list_empty(&cache->ccc_lru)) {
		cli = cfs_list_entry(cache->ccc_lru.next, struct client_obd,
					cl_lru_osc);

		CDEBUG(D_CACHE, "%s: cli %p LRU pages: %d, busy: %d.\n",
			cli->cl_import->imp_obd->obd_name, cli,
			cfs_atomic_read(&cli->cl_lru_in_list),
			cfs_atomic_read(&cli->cl_lru_busy));

		cfs_list_move_tail(&cli->cl_lru_osc, &cache->ccc_lru);
		if (osc_cache_too_much(cli) > 0) {
			spin_unlock(&cache->ccc_lru_lock);

			rc = osc_lru_shrink(env, cli, osc_cache_too_much(cli),
					    true);
			spin_lock(&cache->ccc_lru_lock);
			if (rc != 0)
				break;
		}
	}
	spin_unlock(&cache->ccc_lru_lock);

out:
	cl_env_nested_put(&nest, env);
	CDEBUG(D_CACHE, "%s: cli %p freed %d pages.\n",
		cli->cl_import->imp_obd->obd_name, cli, rc);
	return rc;
}

static int osc_lru_reserve(const struct lu_env *env, struct osc_object *obj,
			   struct osc_page *opg)
{
	struct l_wait_info lwi = LWI_INTR(LWI_ON_SIGNAL_NOOP, NULL);
	struct osc_io *oio = osc_env_io(env);
	struct client_obd *cli = osc_cli(obj);
	int rc = 0;
	ENTRY;

	if (cli->cl_cache == NULL) /* shall not be in LRU */
		RETURN(0);

	if (oio->oi_lru_reserved > 0) {
		--oio->oi_lru_reserved;
		goto out;
	}

	LASSERT(cfs_atomic_read(cli->cl_lru_left) >= 0);
	while (!cfs_atomic_add_unless(cli->cl_lru_left, -1, 0)) {

		/* run out of LRU spaces, try to drop some by itself */
		rc = osc_lru_reclaim(cli);
		if (rc < 0)
			break;
		if (rc > 0)
			continue;

		cond_resched();

		/* slowest case, all of caching pages are busy, notifying
		 * other OSCs that we're lack of LRU slots. */
		cfs_atomic_inc(&osc_lru_waiters);

		rc = l_wait_event(osc_lru_waitq,
				cfs_atomic_read(cli->cl_lru_left) > 0,
				&lwi);

		cfs_atomic_dec(&osc_lru_waiters);
		if (rc < 0)
			break;
	}

out:
	if (rc >= 0) {
		cfs_atomic_inc(&cli->cl_lru_busy);
		opg->ops_in_lru = 1;
		rc = 0;
	}

	RETURN(rc);
}

/** @} osc */
