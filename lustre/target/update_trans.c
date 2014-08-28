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
 * Dump top thandle
 *
 * Dump top thandle and all of its sub thandle to the debug log.
 *
 * \param[in]mask	debug mask
 * \param[in]top_th	top_thandle to be dumped
 */
void top_thandle_dump(unsigned int mask, struct top_thandle *top_th)
{
	struct sub_thandle	  *st;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	CDEBUG(mask, "top_handle %p ref %d child %p update_records %p"
	       "master_committed %d multiple_node %d master dt %s"
	       " cookie "DOSTID"\n",
	       top_th, atomic_read(&top_th->tt_refcount), top_th->tt_child,
	       top_th->tt_update_records, top_th->tt_child_committed,
	       top_th->tt_multiple_node,
	       top_th->tt_super.th_dev->dd_lu_dev.ld_obd->obd_name,
	       POSTID(&top_th->tt_master_cookie.lgc_lgl.lgl_oi));

	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		CDEBUG(mask, "st %p obd %s committed %d sub_th %p\n", st,
		       st->st_dt->dd_lu_dev.ld_obd->obd_name, st->st_committed,
		       st->st_sub_th);
		if (st->st_update != NULL) {
			struct sub_thandle_update *stu = st->st_update;

			CDEBUG(mask, "st_update %p cookie "DOSTID"\n", stu,
			       POSTID(&stu->stu_cookie.lgc_lgl.lgl_oi));
		}
	}
}
EXPORT_SYMBOL(top_thandle_dump);

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
 * \retval		1 if writing succeeds
 * \retval		negative errno if writing fails
 */
static int sub_updates_write(const struct lu_env *env,
			     struct update_records *records,
			     struct thandle *sub_th,
			     struct llog_cookie *cookie)
{
	struct dt_device	*dt = sub_th->th_dev;
	struct llog_ctxt	*ctxt;
	int			rc;
	ENTRY;

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
		      cookie, sub_th);
	llog_ctxt_put(ctxt);

	CDEBUG(D_HA, "%s: Add update log "DOSTID":%u.\n",
	       dt->dd_lu_dev.ld_obd->obd_name,
	       POSTID(&cookie->lgc_lgl.lgl_oi), cookie->lgc_index);

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

	if (tur == NULL || tur->tur_update_records == NULL ||
	    tur->tur_update_params == NULL)
		return 0;

	/* Extends the update records buffer if needed */
	params_size = update_params_size(tur->tur_update_params);
	ops_size = update_ops_size(&tur->tur_update_records->ur_ops);
	if (sizeof(struct update_records) + ops_size + params_size >
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

	atomic_set(&top_th->tt_refcount, 1);
	top_th->tt_magic = TOP_THANDLE_MAGIC;
	child_th->th_storage_th = child_th;
	top_th->tt_child = child_th;
	child_th->th_top = &top_th->tt_super;
	top_th->tt_update_records = NULL;
	INIT_LIST_HEAD(&top_th->tt_sub_trans_list);
	INIT_LIST_HEAD(&top_th->tt_commit_list);

	parent_th = &top_th->tt_super;

	parent_th->th_storage_th = child_th;
	parent_th->th_top = parent_th;
	parent_th->th_tags |= child_th->th_tags;
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
	if (!top_th->tt_child->th_committed) {
		rc = sub_declare_updates_write(env, records, top_th->tt_child);
		if (rc < 0)
			return rc;
	}

	/* Declare update write for all other target */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		if (st->st_update == NULL || st->st_sub_th->th_committed)
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
	struct lu_target		*lut;
	int				rc;
	ENTRY;

	/* During replay, tt_update_records had been gotten from update log */
	if (top_th->tt_update_records == NULL) {
		tur = &update_env_info(env)->uti_tur;
		rc = check_and_prepare_update_record(env, tur);
		if (rc != 0)
			RETURN(rc);

		top_th->tt_update_records = tur;

		/* Get distribution ID for this distributed operation */
		lut = th->th_dev->dd_lu_dev.ld_site->ls_target;
		spin_lock(&lut->lut_distribution_id_lock);
		tur->tur_update_records->ur_cookie = lut->lut_distribution_id++;
		spin_unlock(&lut->lut_distribution_id_lock);
	}

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
	int			rc = 0;
	ENTRY;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	top_thandle_dump(D_HA, top_th);
	/* Walk through all of sub transaction to see if it needs to
	 * record updates for this transaction */
	if (top_th->tt_multiple_node) {
		rc = prepare_mulitple_node_trans(env, th);
		if (rc < 0)
			GOTO(out, rc);
		/* Mark remote MDT flag on master MDT thandle, so the commit
		 * callback can recorganize the distribute transaction */
		top_th->tt_child->th_remote_mdt = 1;
	}

	/* Start transactions on other MDT */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		if (st->st_sub_th->th_committed)
			continue;
		st->st_sub_th->th_sync = th->th_sync;
		st->st_sub_th->th_local = th->th_local;
		st->st_sub_th->th_tags = th->th_tags;
		rc = dt_trans_start(env, st->st_sub_th->th_dev,
				    st->st_sub_th);
		if (rc != 0)
			GOTO(out, rc);
	}

	/* Start transaction on master MDT */
	if (!top_th->tt_child->th_committed) {
		top_th->tt_child->th_local = th->th_local;
		top_th->tt_child->th_sync = th->th_sync;
		top_th->tt_child->th_tags = th->th_tags;
		rc = dt_trans_start(env, master_dev, top_th->tt_child);
	}
out:
	th->th_result = rc;
	RETURN(rc);
}
EXPORT_SYMBOL(top_trans_start);

/**
 * Top thandle commit callback
 *
 * This callback will be called when all of sub transactions are committed.
 *
 * \param[in] th	top thandle to be committed.
 */
static void top_trans_committed_cb(struct thandle *th)
{
	struct top_thandle *top_th = container_of(th, struct top_thandle,
						  tt_super);

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	LASSERT(top_th->tt_commit_callback != NULL);
	LASSERT(atomic_read(&top_th->tt_refcount) > 0);
	th->th_committed = 1;
	if (th->th_result == 0)
		top_th->tt_commit_callback(th, top_th->tt_commit_callback_arg);
	else
		top_thandle_put(top_th);
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
	struct thandle_update_records *tur;
	int			rc = 0;
	bool			all_committed = true;
	ENTRY;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	tur = top_th->tt_update_records;

	if (tur != NULL && tur->tur_update_records != NULL &&
	    !top_th->tt_child->th_committed && th->th_result == 0) {
		/* Merge the parameters and updates into one buffer */
		rc = prepare_writing_updates(env, tur);
		if (rc < 0) {
			CERROR("%s: prepare updates failed: rc = %d\n",
			       master_dev->dd_lu_dev.ld_obd->obd_name, rc);
			th->th_result = rc;
			GOTO(stop_master_trans, rc);
		}

		update_records_dump(tur->tur_update_records, D_HA);
		/* Write updates to the master MDT */
		rc = sub_updates_write(env, tur->tur_update_records,
				       top_th->tt_child,
				       &top_th->tt_master_cookie);
		if (rc < 0) {
			CERROR("%s: write updates failed: rc = %d\n",
			       master_dev->dd_lu_dev.ld_obd->obd_name, rc);
			th->th_result = rc;
		}
	}

stop_master_trans:
	/* Hold the top transaction handle for mulitple node transaction until
	 * all of sub thandle are committed */
	if (top_th->tt_multiple_node)
		top_thandle_get(top_th);

	/* Step 2: Stop the transaction on the master MDT, the stop callback
	 * will fill the master transno in the update logs to other MDT. */
	top_th->tt_child->th_local = th->th_local;
	top_th->tt_child->th_sync = th->th_sync;
	top_th->tt_child->th_tags = th->th_tags;
	top_th->tt_child->th_result = th->th_result;
	if (likely(!top_th->tt_child->th_committed)) {
		if (top_th->tt_multiple_node)
			top_thandle_get(top_th);
		all_committed = false;
		rc = dt_trans_stop(env, master_dev, top_th->tt_child);
		if (rc < 0 && top_th->tt_multiple_node)
			top_thandle_put(top_th);
	} else {
		/* If the transaction has been committed, we still
		 * call dt_trans_stop to free the thandle */
		rc = dt_trans_stop(env, master_dev, top_th->tt_child);
		top_th->tt_child = NULL;
	}

	if (rc < 0) {
		th->th_result = rc;
		GOTO(stop_other_trans, rc);
	}

	/* Step 3: write updates to other MDTs */
	if (tur != NULL && tur->tur_update_records != NULL &&
	    th->th_result == 0) {
		/* Stop callback will add more updates, so merge the parameters
		 * and updates into one buffer */
		rc = prepare_writing_updates(env, tur);
		if (rc < 0) {
			CERROR("%s: prepare updates failed: rc = %d\n",
			       master_dev->dd_lu_dev.ld_obd->obd_name, rc);
			th->th_result = rc;
			GOTO(stop_other_trans, rc);
		}

		update_records_dump(tur->tur_update_records, D_HA);
		list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
			if (st->st_update == NULL ||
			    st->st_sub_th->th_committed)
				continue;

			rc = sub_updates_write(env, tur->tur_update_records,
					       st->st_sub_th,
					       &st->st_update->stu_cookie);
			if (rc < 0) {
				th->th_result = rc;
				break;
			}
		}
		if (rc > 0)
			rc = 0;
	}

stop_other_trans:
	/* Step 4: Stop the transaction on other MDTs */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		st->st_sub_th->th_sync = th->th_sync;
		st->st_sub_th->th_local = th->th_local;
		st->st_sub_th->th_tags = th->th_tags;
		st->st_sub_th->th_result = th->th_result;
		if (likely(!st->st_sub_th->th_committed)) {
			if (st->st_update == NULL) {
				/* Only track the committed status if there are
				 * update logs attached to it, i.e. we do not
				 * care the sub transaction for OST, and only
				 * release the sub transaction for now. */
				rc = dt_trans_stop(env, st->st_sub_th->th_dev,
						   st->st_sub_th);
				continue;
			}

			/* hold the top_thandle until this sub thandle is
			 * committed. */
			LASSERT(top_th->tt_multiple_node);
			top_thandle_get(top_th);
			all_committed = false;

			rc = dt_trans_stop(env, st->st_sub_th->th_dev,
					   st->st_sub_th);

			if (rc < 0) {
				top_thandle_put(top_th);
				st->st_sub_th = NULL;
			}
		} else {
			/* If the transaction has been committed, we still
			 * call dt_trans_stop to free the thandle */
			rc = dt_trans_stop(env, st->st_sub_th->th_dev,
					   st->st_sub_th);
			st->st_sub_th = NULL;
		}

		if (unlikely(rc < 0 && th->th_result == 0))
			th->th_result = rc;
	}

	/* If all sub thandle have been committed, then call
	 * top_trans_committed_cb directly, which might happen
	 * during recovery. */
	if (top_th->tt_multiple_node && all_committed)
		top_trans_committed_cb(&top_th->tt_super);

	/* Balance for the refcount in top_trans_create, Note: if it is NOT
	 * multiple node transaction, the top transaction will be destroyed. */
	top_thandle_put(top_th);

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
	struct sub_thandle	*st;
	struct top_thandle	*top_th;
	struct thandle		*sub_th;
	ENTRY;

	/* If th_top == NULL, it means the transaction is created
	 * in sub layer(OSD/OSP), just return itself, see
	 * out_tx_end()->osd_trans_start() -> tgt_txn_start_cb() */
	if (th->th_top == NULL)
		RETURN(th);

	top_th = container_of(th, struct top_thandle, tt_super);
	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	LASSERT(top_th->tt_child != NULL);
	if (likely(sub_dt == top_th->tt_child->th_dev))
		RETURN(top_th->tt_child);

	st = lookup_sub_thandle(th, sub_dt);
	if (st != NULL)
		RETURN(st->st_sub_th);

	sub_th = dt_trans_create(env, sub_dt);
	if (IS_ERR(sub_th))
		RETURN(sub_th);

	/* XXX all of mixed transaction (see struct th_handle) will
	 * be synchronized until async update is done */
	if (sub_th->th_remote_mdt) {
		th->th_sync = 1;
		top_th->tt_multiple_node = 1;
	}

	st = create_sub_thandle(env, top_th, sub_th);
	if (IS_ERR(st)) {
		dt_trans_stop(env, sub_dt, sub_th);
		RETURN(ERR_CAST(st));
	}

	sub_th->th_storage_th = top_th->tt_child;
	sub_th->th_top = th;

	RETURN(sub_th);
}
EXPORT_SYMBOL(get_sub_thandle);

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
		if (st->st_update != NULL)
			OBD_FREE_PTR(st->st_update);
		OBD_FREE_PTR(st);
	}
	OBD_FREE_PTR(top_th);
}
EXPORT_SYMBOL(top_thandle_destroy);

/**
 * sub thandle commit callback
 *
 * Mark the sub thandle to be committed and if all sub thandle are committed
 * notify the top thandle.
 *
 * \param[in] sub_th	sub thandle being committed.
 */
void sub_trans_commit_cb(struct thandle *sub_th)
{
	struct top_thandle	*top_th;
	struct dt_device	*sub_dt = sub_th->th_dev;
	struct sub_thandle	*st;
	bool			all_committed = true;
	bool			all_freed = true;
	bool			th_abort = false;
	ENTRY;

	if (sub_th->th_top == NULL || !sub_th->th_remote_mdt)
		RETURN_EXIT;

	top_th = container_of(sub_th->th_top, struct top_thandle, tt_super);
	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	LASSERT(top_th->tt_multiple_node);
	top_thandle_dump(D_HA, top_th);

	/* Mark the correspondent sub th to be committed, Note: the sub_th
	 * will be destroyed after this func, so it need remember the commit
	 * status in top_th */
	if (top_th->tt_child != NULL &&
	    top_th->tt_child->th_dev == sub_dt) {
		/* Check whether the master OSD is being umounted */
		if (sub_dt != NULL && sub_dt->dd_lu_dev.ld_obd->obd_stopping)
			th_abort = true;
		if (top_th->tt_child->th_committed)
			top_th->tt_child_committed = 1;
		top_th->tt_child = NULL;
	} else {
		/* Check if all sub thandles are committed */
		list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
			if (st->st_sub_th == NULL)
				continue;
			if (st->st_sub_th == sub_th) {
				if (st->st_sub_th->th_committed)
					st->st_committed = 1;
				st->st_sub_th = NULL;
				top_thandle_put(top_th);
				break;
			}
		}
	}


	/* If master OSD is being umounted, other sub thandles should
	 * already being put, so this put will release the top_th.*/
	if (th_abort) {
		LASSERT(top_th->tt_child == NULL);
		list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list)
			LASSERT(st->st_sub_th == NULL);
		top_thandle_put(top_th);
		RETURN_EXIT;
	}

	/* Check whether all of sub thandles committed and freed. Note:
	 * sometimes even though the commit callback is called for some
	 * sub thandle, it does not mean it has been committed. especially
	 * during umount process, see osp_request_commit_cb().
	 * For the top thandle,
	 *  If all of sub thandles have been committed, then call top thandle
	 *  commit callback.
	 *  If all of sub thandles are freed, but not all committed, we only
	 *  all top_thandle_put() to release the top thandle */
	if (top_th->tt_child != NULL)
		all_freed = false;

	if (!top_th->tt_child_committed)
		all_committed = false;

	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		/* Note: we only care the sub transacion if there are updates
		 * attached to it, i.e. we do not care sub transaction to OST
		 * for now */
		if (st->st_sub_th != NULL && st->st_update != NULL)
 			all_freed = false;
		if (!st->st_committed && st->st_update != NULL)
 			all_committed = false;
 	}

	if (all_committed)
		top_trans_committed_cb(&top_th->tt_super);
	else if (all_freed)
		top_thandle_put(top_th);

	sub_th->th_top = NULL;
	RETURN_EXIT;
}
EXPORT_SYMBOL(sub_trans_commit_cb);
