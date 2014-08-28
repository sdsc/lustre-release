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
#include <lustre_log.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>

/**
 * Declare write update to sub device
 *
 * Declare Write updates llog records to the sub device during distribute
 * transaction.
 *
 * \param[in] env	execution environment
 * \param[in] records	update records being written
 * \param[in] st	sub transaction handle
 *
 * \retval		0 if writing succeeds
 * \retval		negative errno if writing fails
 */
static int sub_declare_updates_write(const struct lu_env *env,
				     struct update_records *records,
				     struct thandle *sub_th)
{
	struct llog_ctxt	*ctxt;
	struct dt_device	*dt = sub_th->th_dev;
	int rc;

	/* If ctxt is NULL, it means not need to write update,
	 * for example if the the OSP is used to connect to OST */
	ctxt = llog_get_context(dt->dd_lu_dev.ld_obd,
				LLOG_UPDATELOG_ORIG_CTXT);
	LASSERT(ctxt != NULL);

	/* Not ready to record updates yet. */
	if (ctxt->loc_handle == NULL) {
		llog_ctxt_put(ctxt);
		return 0;
	}

	records->ur_hdr.lrh_len = LLOG_CHUNK_SIZE;
	rc = llog_declare_add(env, ctxt->loc_handle, &records->ur_hdr,
			      sub_th);

	llog_ctxt_put(ctxt);

	return rc;
}

/**
 * write update to sub device
 *
 * Write updates llog records to the sub device during distribute
 * transaction.
 *
 * \param[in] env	execution environment
 * \param[in] records	update records being written
 * \param[in] st	sub transaction handle
 *
 * \retval		0 if writing succeeds
 * \retval		negative errno if writing fails
 */
int sub_updates_write(const struct lu_env *env,
		      struct update_records *records,
		      struct sub_thandle *st)
{
	struct sub_thandle_update *stu;
	struct llog_ctxt	*ctxt;
	struct dt_device	*dt = st->st_dt;
	int			rc;
	ENTRY;

	LASSERT(st->st_update != NULL);
	stu = st->st_update;

	ctxt = llog_get_context(dt->dd_lu_dev.ld_obd,
				LLOG_UPDATELOG_ORIG_CTXT);
	LASSERT(ctxt != NULL);

	/* Not ready to record updates yet, usually happens
	 * in error handler path */
	if (ctxt->loc_handle == NULL) {
		llog_ctxt_put(ctxt);
		RETURN(0);
	}

	rc = llog_add(env, ctxt->loc_handle, &records->ur_hdr,
		      &stu->stu_cookie, st->st_sub_th);
	llog_ctxt_put(ctxt);

	RETURN(rc);
}

/**
 * Prepare the update records.
 *
 * Merge params and ops into the update records, then initializing
 * the update buffer.
 *
 * During transaction execution phase, parameters and update ops
 * are collected in two different buffers (see lod_updates_pack()),
 * during transaction stop, it needs to be merged in one buffer,
 * so it will be written in the update log.
 *
 * \param[in] env	execution environment
 * \param[in] lur	lod_update_records to be merged
 *
 * \retval		0 if merging succeeds.
 * \retval		negaitive errno if merging fails.
 */
static int prepare_writing_updates(const struct lu_env *env,
				   struct thandle_update_records *tur)
{
	struct update_params *params;
	size_t params_size;
	size_t ops_size;

	if (tur->tur_update_records == NULL ||
	    tur->tur_update_params == NULL)
		return 0;

	/* Extends the update records buffer if needed */
	params_size = update_params_size(tur->tur_update_params);
	ops_size = update_ops_size(&tur->tur_update_records->ur_ops);
	if (sizeof(struct update_records) + ops_size + params_size >=
	    tur->tur_update_records_size) {
		int rc;

		rc = tur_update_records_extend(tur,
					sizeof(struct update_records) +
					ops_size + params_size);
		if (rc != 0)
			return rc;
	}

	params = update_records_get_params(tur->tur_update_records);
	memcpy(params, tur->tur_update_params, params_size);

	/* Init update record header */
	tur->tur_update_records->ur_hdr.lrh_len =
		cfs_size_round(update_records_size(tur->tur_update_records));
	tur->tur_update_records->ur_hdr.lrh_type = UPDATE_REC;

	/* Dump updates for debugging purpose */
	update_records_dump(tur->tur_update_records, D_HA);

	return 0;
}

/**
 * write update transaction
 *
 * Check if there are updates being recorded in this transaction,
 * it will write the record into the disk.
 *
 * \param[in] env	execution environment
 * \param[in] top_th	top transaction handle
 *
 * \retval		0 if writing succeeds
 * \retval		negative errno if writing fails
 */
static int updates_write(const struct lu_env *env, struct thandle *th)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct thandle_update_records	*tur = top_th->tt_update_records;
	struct sub_thandle	*st;
	struct sub_thandle	*tmp;
	int			rc;

	/* merge the parameters and updates into one buffer */
	rc = prepare_writing_updates(env, tur);
	if (rc < 0)
		RETURN(rc);

	list_for_each_entry_safe(st, tmp, &top_th->tt_sub_trans_list, st_list) {
		if (st->st_update == NULL || st->st_committed)
			continue;

		rc = sub_updates_write(env, tur->tur_update_records, st);
		if (rc != 0)
			break;
	}

	return rc;
}

static struct sub_thandle
*create_sub_thandle(const struct lu_env *env, struct top_thandle *top_th,
		    struct thandle *sub_th)
{
	struct sub_thandle *st;
	ENTRY;

	OBD_ALLOC_PTR(st);
	if (st == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	INIT_LIST_HEAD(&st->st_list);
	st->st_sub_th = sub_th;
	st->st_dt = sub_th->th_dev;
	list_add(&st->st_list, &top_th->tt_sub_trans_list);

	if (sub_th->th_remote_mdt) {
		/* If it is for remote MDT operation, then allocate the
		 * update structure for cross-MDT operation. */
		OBD_ALLOC_PTR(st->st_update);
		if (st->st_update == NULL) {
			OBD_FREE_PTR(st);
			RETURN(ERR_PTR(-ENOMEM));
		}
	}

	RETURN(st);
}

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

	child_th->th_storage_th = child_th;
	top_th->tt_child = child_th;
	child_th->th_top = &top_th->tt_super;
	top_th->tt_update_records = NULL;
	INIT_LIST_HEAD(&top_th->tt_sub_trans_list);
	INIT_LIST_HEAD(&top_th->tt_commit_list);

	parent_th = &top_th->tt_super;

	parent_th->th_storage_th = child_th;
	parent_th->th_top = parent_th;

	return parent_th;
}
EXPORT_SYMBOL(top_trans_create);

/**
 * write update transaction
 *
 * Check if there are updates being recorded in this transaction,
 * it will write the record into the disk.
 *
 * \param[in] env	execution environment
 * \param[in] top_th	top transaction handle
 *
 * \retval		0 if writing succeeds
 * \retval		negative errno if writing fails
 */
static int declare_updates_write(const struct lu_env *env,
				 struct top_thandle *top_th)
{
	struct update_records *records;
	struct sub_thandle *st;
	int rc;

	LASSERT(top_th->tt_update_records != NULL);
	records = top_th->tt_update_records->tur_update_records;

	/* Declare update write for all other target */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {

		if (st->st_update == NULL)
			continue;

		rc = sub_declare_updates_write(env, records, st->st_sub_th);
		if (rc < 0)
			break;
	}

	return rc;
}

/**
 * Prepare cross-MDT operation.
 *
 * Create the update record buffer to record updates for cross-MDT operation,
 * add master sub transaction to tt_sub_trans_list, and declare the update
 * writes.
 *
 * During updates packing, all of parameters will be packed in
 * tur_update_params, and updates will be packed in tur_update_records.
 * Then in transaction stop, parameters and updates will be merged
 * into one updates buffer.
 *
 * And also master thandle will be added to the sub_th list, so it will be
 * easy to track the commit status.
 *
 * \param[in] env	execution environment
 * \param[in] th	top transaction handle
 *
 * \retval		0 if preparation succeeds.
 * \retval		negative errno if preparation fails.
 */
static int prepare_mulitple_node_trans(const struct lu_env *env,
				       struct thandle *th)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct thandle_update_records	*tur;
	struct sub_thandle		*master_st;
	struct lu_target		*lut;
	int				rc;
	ENTRY;

	/* Prepare the update buffer for recording updates */
	if (top_th->tt_update_records != NULL)
		RETURN(0);

	tur = &update_env_info(env)->uti_tur;
	rc = check_and_prepare_update_record(env, tur);
	top_th->tt_update_records = tur;

	/* Get distribution ID for this distributed operation */
	lut = th->th_dev->dd_lu_dev.ld_site->ls_target;
	spin_lock(&lut->lut_distribution_id_lock);
	tur->tur_update_records->ur_cookie = lut->lut_distribution_id++;
	spin_unlock(&lut->lut_distribution_id_lock);

	/* we need to add the master sub transaction to the start
	 * of the list, so it will be executed first during trans start
	 * and trans stop */
	master_st = create_sub_thandle(env, top_th, top_th->tt_child);
	if (IS_ERR(master_st))
		RETURN(PTR_ERR(master_st));

	rc = declare_updates_write(env, top_th);

	RETURN(rc);
}

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
	struct sub_thandle	*st;
	int			rc;

	/* Walk through all of sub transaction to see if it needs to
	 * record updates for this transaction */
	if (top_th->tt_multiple_node) {
		rc = prepare_mulitple_node_trans(env, th);
		if (rc < 0)
			RETURN(rc);
	}

	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		st->st_sub_th->th_sync = th->th_sync;
		st->st_sub_th->th_local = th->th_local;
		rc = dt_trans_start(env, st->st_sub_th->th_dev,
				    st->st_sub_th);
		if (rc != 0)
			return rc;
	}

	top_th->tt_child->th_local = th->th_local;
	top_th->tt_child->th_sync = th->th_sync;

	return dt_trans_start(env, master_dev, top_th->tt_child);
}
EXPORT_SYMBOL(top_trans_start);

/**
 * Destroy top thandle
 *
 * Destory all of sub_thandle and top thandle.
 *
 * \param [in] top_th	top thandle to be destoryed.
 */
void top_thandle_destroy(struct top_thandle *top_th)
{
	struct sub_thandle *st;
	struct sub_thandle *tmp;

	list_for_each_entry_safe(st, tmp, &top_th->tt_sub_trans_list,
				 st_list) {
		list_del(&st->st_list);
		if (st->st_update != NULL)
			OBD_FREE_PTR(st->st_update);
		OBD_FREE_PTR(st);
	}

	OBD_FREE_PTR(top_th);
	return;
}
EXPORT_SYMBOL(top_thandle_destroy);

/**
 * Stop the transaction for mulitnode transaction.
 *
 * Walk through all of sub transactions and stop all of them. Note:
 * during the recovery phase, some of sub transactions might been
 * committed, and only call commit callback for these sub transactions.
 *
 * \param[in] env	execution environment
 * \param[in] th	LOD transaction handle
 *
 * \retval		0 if transaction stop succeeds.
 * \retval		negative errno if transaction stop fails.
 */
static int multiple_nodes_trans_stop(const struct lu_env *env,
				     struct thandle *th)
{
	struct top_thandle	*top_th;
	struct sub_thandle	*st;
	int			rc = 0;

	top_th = container_of0(th, struct top_thandle, tt_super);

	LASSERT(!list_empty(&top_th->tt_sub_trans_list));
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		struct dt_device *dt_dev = st->st_dt;
		int rc1;

		/* If this transaction has been committed, just call
		 * commit callback, this usually happens during updates
		 * recovery */
		if (st->st_committed) {
			dt_txn_hook_commit(st->st_sub_th);
		} else {
			st->st_sub_th->th_result = th->th_result;
			rc1 = dt_trans_stop(env, dt_dev, st->st_sub_th);
			if (rc1 < 0) {
				CERROR("%s: trans stop failed: rc = %d\n",
				       dt_dev->dd_lu_dev.ld_obd->obd_name, rc1);
				if (rc == 0)
					rc = rc1;
			}
		}
	}

	return rc;
}

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
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct sub_thandle	*st;
	int			rc2 = 0;
	int			rc;
	ENTRY;

	/* Note: we need walk through all of sub_transaction and do transaction
	 * stop to release the resource here */
	if (top_th->tt_update_records != NULL) {
		rc = updates_write(env, th);
		if (rc != 0) {
			CDEBUG(D_HA, "%s: write updates failed: rc = %d\n",
			       master_dev->dd_lu_dev.ld_obd->obd_name, rc);
			/* Still need call dt_trans_stop to release resources
			 * holding by the transaction */
		}
	}

	if (top_th->tt_multiple_node) {
		rc = multiple_nodes_trans_stop(env, th);
		RETURN(rc);
	}

	top_th->tt_child->th_result = th->th_result;
	top_th->tt_child->th_local = th->th_local;
	top_th->tt_child->th_sync = th->th_sync;
	rc = dt_trans_stop(env, master_dev, top_th->tt_child);

	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		if (rc != 0)
			st->st_sub_th->th_result = rc;
		else
			st->st_sub_th->th_result = rc2;

		st->st_sub_th->th_sync = th->th_sync;
		st->st_sub_th->th_local = th->th_local;
		rc2 = dt_trans_stop(env, st->st_sub_th->th_dev,
				    st->st_sub_th);
		if (unlikely(rc2 != 0 && rc == 0)) {
			rc = rc2;
			CERROR("%s: trans stop failed: rc = %d\n",
			    st->st_sub_th->th_dev->dd_lu_dev.ld_obd->obd_name,
			    rc);
			break;
		}
	}

	top_thandle_destroy(top_th);
	RETURN(rc);
}
EXPORT_SYMBOL(top_trans_stop);

struct sub_thandle *lookup_sub_thandle(const struct thandle *th,
				       const struct dt_device *sub_dt)
{
	struct sub_thandle *st;
	struct top_thandle *top_th;

	top_th = container_of0(th, struct top_thandle, tt_super);
	/* Find or create the transaction in tt_trans_list, since there is
	 * always only one thread access the list, so no need lock here */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		if (st->st_sub_th->th_dev == sub_dt)
			RETURN(st);
	}

	RETURN(NULL);
}
EXPORT_SYMBOL(lookup_sub_thandle);

/**
 * Get sub thandle.
 *
 * Get sub thandle from the top thandle according to the sub object. This is
 * usually happened for distribute transaction (see top_thandle/sub_thandle).
 *
 * \param[in] env	execution environment
 * \param[in] th	thandle on the top layer.
 * \param[in] child_obj child object used to get sub transaction
 *
 * \retval		thandle of sub transaction if succeed
 * \retval		PTR_ERR(errno) if failed
 */
struct thandle *get_sub_thandle(const struct lu_env *env,
				struct thandle *th,
				const struct dt_object *sub_obj)
{
	struct dt_device	*sub_dt = lu2dt_dev(sub_obj->do_lu.lo_dev);
	struct sub_thandle	*lst;
	struct top_thandle	*top_th;
	struct thandle		*sub_th;
	ENTRY;

	top_th = container_of0(th, struct top_thandle, tt_super);
	LASSERT(top_th->tt_child != NULL);
	if (likely(sub_dt == top_th->tt_child->th_dev))
		RETURN(top_th->tt_child);

	lst = lookup_sub_thandle(th, sub_dt);
	if (lst != NULL)
		RETURN(lst->st_sub_th);

	/* Find or create the transaction in tt_trans_list, since there is
	 * always only one thread access the list, so no need lock here */
	list_for_each_entry(lst, &top_th->tt_sub_trans_list, st_list) {
		if (lst->st_sub_th->th_dev == sub_dt)
			RETURN(lst->st_sub_th);
	}

	sub_th = dt_trans_create(env, sub_dt);
	if (IS_ERR(sub_th))
		RETURN(sub_th);

	/* If the child does not need to create the transaction like
	 * OSP connected to OST, just use current thandle */
	if (sub_th == NULL)
		RETURN(th);

	lst = create_sub_thandle(env, top_th, sub_th);
	if (IS_ERR(lst)) {
		dt_trans_stop(env, sub_dt, sub_th);
		RETURN(ERR_CAST(lst));
	}

	if (sub_th->th_remote_mdt)
		top_th->tt_multiple_node = 1;

	sub_th->th_storage_th = top_th->tt_child;

	RETURN(sub_th);
}
EXPORT_SYMBOL(get_sub_thandle);

