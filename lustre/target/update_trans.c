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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2014, Intel Corporation.
 */
/*
 * lustre/target/update_trans.c
 *
 * This file implements the update distribute transaction API.
 *
 * To manage the cross-MDT operation (distribute operation) transaction,
 * the transaction will also be separated two layers on MD stack, top
 * transaction and sub transaction.
 *
 * During the distribute operation, top transaction is created in the LOD
 * layer, and represent the operation. Sub transaction is created by
 * each OSD or OSP. Top transaction start/stop will trigger all of its sub
 * transaction start/stop. Top transaction (the whole operation) is committed
 * only all of its sub transaction are committed.
 *
 * there are three kinds of transactions
 * 1. local transaction: All updates are in a single local OSD.
 * 2. Remote transaction: All Updates are only in the remote OSD,
 *    i.e. locally all updates are in OSP.
 * 3. Mixed transaction: Updates are both in local OSD and remote
 *    OSD.
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <lu_target.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>

/**
 * Create the top transaction.
 *
 * Create the top transaction on the master device. It will create a top
 * thandle and a sub thandle on the master device.
 *
 * \param[in] env		execution environment
 * \param[in] master_dev	master_dev the top thandle will be created
 *
 * \retval			pointer to the created thandle.
 * \retval			PTR_ERR(errno) if creation failed.
 */
struct thandle *
top_trans_create(const struct lu_env *env, struct dt_device *master_dev)
{
	struct top_thandle	*top_th;
	struct thandle		*child_th;
	struct thandle		*parent_th;
	ENTRY;

	OBD_ALLOC_GFP(top_th, sizeof(*top_th), __GFP_IO);
	if (top_th == NULL)
		return ERR_PTR(-ENOMEM);

	child_th = dt_trans_create(env, master_dev);
	if (IS_ERR(child_th)) {
		OBD_FREE_PTR(top_th);
		return child_th;
	}

	atomic_set(&top_th->tt_refcount, 1);
	top_th->tt_magic = TOP_THANDLE_MAGIC;
	child_th->th_storage_th = child_th;
	top_th->tt_child = child_th;
	child_th->th_top = &top_th->tt_super;
	top_th->tt_update_records = NULL;
	INIT_LIST_HEAD(&top_th->tt_sub_trans_list);

	parent_th = &top_th->tt_super;

	parent_th->th_storage_th = child_th;
	parent_th->th_top = parent_th;

	return parent_th;
}
EXPORT_SYMBOL(top_trans_create);

/**
 * start the top transaction.
 *
 * Start all of its sub transactions, then start master sub transaction.
 *
 * \param[in] env		execution environment
 * \param[in] master_dev	master_dev the top thandle will be created
 * \param[in] th		top thandle
 *
 * \retval			0 if transaction start succeeds.
 * \retval			negative errno if start fails.
 */
int top_trans_start(const struct lu_env *env, struct dt_device *master_dev,
		    struct thandle *th)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct sub_thandle	*lst;
	int			rc;

	rc = check_and_prepare_update_record(env, th);
	if (rc < 0)
		return rc;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	list_for_each_entry(lst, &top_th->tt_sub_trans_list, st_list) {
		lst->st_sub_th->th_sync = th->th_sync;
		lst->st_sub_th->th_local = th->th_local;
		rc = dt_trans_start(env, lst->st_sub_th->th_dev,
				    lst->st_sub_th);
		if (rc != 0)
			return rc;
	}

	top_th->tt_child->th_local = th->th_local;
	top_th->tt_child->th_sync = th->th_sync;

	return dt_trans_start(env, master_dev, top_th->tt_child);
}
EXPORT_SYMBOL(top_trans_start);

/**
 * Stop the top transaction.
 *
 * Stop the transaction on the master device first, then stop transactions
 * on other sub devices.
 *
 * \param[in] env		execution environment
 * \param[in] master_dev	master_dev the top thandle will be created
 * \param[in] th		top thandle
 *
 * \retval			0 if stop transaction succeeds.
 * \retval			negative errno if creation fails.
 */
int top_trans_stop(const struct lu_env *env, struct dt_device *master_dev,
		   struct thandle *th)
{
	struct sub_thandle	*lst;
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	int			rc2 = 0;
	int			rc;
	ENTRY;

	/* Note: we need walk through all of sub_transaction and do transaction
	 * stop to release the resource here */
	if (top_th->tt_update_records != NULL) {
		rc = merge_params_updates_buf(env, th);
		if (rc == 0)
			update_records_dump(
				top_th->tt_update_records->tur_update_records,
				D_HA);
	}

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);

	top_th->tt_child->th_local = th->th_local;
	top_th->tt_child->th_sync = th->th_sync;

	rc = dt_trans_stop(env, master_dev, top_th->tt_child);

	list_for_each_entry(lst, &top_th->tt_sub_trans_list, st_list) {
		if (rc != 0)
			lst->st_sub_th->th_result = rc;
		else
			lst->st_sub_th->th_result = rc2;

		lst->st_sub_th->th_sync = th->th_sync;
		lst->st_sub_th->th_local = th->th_local;
		rc2 = dt_trans_stop(env, lst->st_sub_th->th_dev,
				    lst->st_sub_th);
		if (unlikely(rc2 < 0 && rc == 0))
			rc = rc2;
	}

	top_thandle_put(top_th);

	RETURN(rc);
}
EXPORT_SYMBOL(top_trans_stop);

/**
 * Get sub thandle.
 *
 * Get sub thandle from the top thandle according to the sub dt_device.
 *
 * \param[in] env	execution environment
 * \param[in] th	thandle on the top layer.
 * \param[in] sub_dt	sub dt_device used to get sub transaction
 *
 * \retval		thandle of sub transaction if succeed
 * \retval		PTR_ERR(errno) if failed
 */
struct thandle *get_sub_thandle_by_dt(const struct lu_env *env,
				      struct thandle *th,
				      struct dt_device *sub_dt)
{
	struct sub_thandle	*lst;
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct thandle		*sub_th;
	ENTRY;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	LASSERT(top_th->tt_child != NULL);
	if (likely(sub_dt == top_th->tt_child->th_dev))
		RETURN(top_th->tt_child);

	/* Find or create the transaction in tt_trans_list, since there is
	 * always only one thread access the list, so no need lock here */
	list_for_each_entry(lst, &top_th->tt_sub_trans_list, st_list) {
		if (lst->st_sub_th->th_dev == sub_dt)
			RETURN(lst->st_sub_th);
	}

	sub_th = dt_trans_create(env, sub_dt);
	if (IS_ERR(sub_th))
		RETURN(sub_th);

	/* XXX all of mixed transaction (see struct th_handle) will
	 * be synchronized until async update is done */
	th->th_sync = 1;

	sub_th->th_top = th;
	OBD_ALLOC_PTR(lst);
	if (lst == NULL) {
		dt_trans_stop(env, sub_dt, sub_th);
		RETURN(ERR_PTR(-ENOMEM));
	}

	INIT_LIST_HEAD(&lst->st_list);
	lst->st_sub_th = sub_th;
	list_add(&lst->st_list, &top_th->tt_sub_trans_list);
	lst->st_record_update = 1;

	sub_th->th_storage_th = top_th->tt_child;

	RETURN(sub_th);
}
EXPORT_SYMBOL(get_sub_thandle_by_dt);

/**
 * Top thandle destroy
 *
 * Destroy the top thandle and all of its sub thandle.
 *
 * \param[in] top_th	top thandle to be destroyed.
 */
void top_thandle_destroy(struct top_thandle *top_th)
{
	struct sub_thandle *st;
	struct sub_thandle *tmp;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	list_for_each_entry_safe(st, tmp, &top_th->tt_sub_trans_list, st_list) {
		list_del(&st->st_list);
		OBD_FREE_PTR(st);
	}
	OBD_FREE_PTR(top_th);
}
EXPORT_SYMBOL(top_thandle_destroy);
