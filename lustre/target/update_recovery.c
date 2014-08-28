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
 * lustre/target/update_recovery.c
 *
 * This file implement the methods to handle the update recovery.
 *
 * During DNE recovery, the recovery thread will redo the operation according
 * to the transaction no, and these replay are either from client replay req
 * or update replay records(for distribute transaction) in the update log.
 * For distribute transaction replay, the replay thread will call
 * distribute_txn_replay_handle() to handle the updates.
 *
 * After the Master MDT restarts, it will retrieve the update records from all
 * of MDTs, for each distributed operation, it will check updates on all MDTs,
 * if some updates records are missing on some MDTs, the replay thread will redo
 * updates on these MDTs.
 *
 * Author: Di Wang <di.wang@intel.com>
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <lu_target.h>
#include <md_object.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>
#include "tgt_internal.h"

/**
 * Lookup distribute_txn_replay req
 *
 * Lookup distribute_txn_replay in the replay list by batchid.
 * It is assumed the list has been locked before calling this function.
 *
 * \param[in] tdtd	distribute_txn_data, which holds the replay
 *                      list.
 * \param[in] batchid	batchid used by lookup.
 *
 * \retval		pointer of the replay if succeeds.
 * \retval		NULL if can not find it.
 */
static struct distribute_txn_replay_req *
dtrq_lookup(struct target_distribute_txn_data *tdtd, __u64 batchid)
{
	struct distribute_txn_replay_req	*tmp;
	struct distribute_txn_replay_req	*dtrq = NULL;

	list_for_each_entry(tmp, &tdtd->tdtd_replay_list, dtrq_list) {
		if (tmp->dtrq_updates->ur_batchid == batchid) {
			dtrq = tmp;
			break;
		}
	}
	return dtrq;
}

/**
 * insert distribute txn replay req
 *
 * Insert distribute txn replay to the replay list, and it assumes the
 * list has been looked. Note: the replay list is a sorted list, which
 * is sorted by master transno.
 *
 * \param[in] tdtd	target distribute txn data where replay list is
 * \param[in] new	distribute txn replay to be inserted
 */
static void dtrq_insert(struct target_distribute_txn_data *tdtd,
			struct distribute_txn_replay_req *new)
{
	struct distribute_txn_replay_req *iter;

	list_for_each_entry_reverse(iter, &tdtd->tdtd_replay_list, dtrq_list) {
		if (iter->dtrq_updates->ur_master_transno >
		    new->dtrq_updates->ur_master_transno)
			continue;

		list_add(&new->dtrq_list, &iter->dtrq_list);
		break;
	}
	if (list_empty(&new->dtrq_list))
		list_add(&new->dtrq_list, &tdtd->tdtd_replay_list);
}

/**
 * create distribute txn replayi req
 *
 * Allocate distribute txn replay req according to the update records.
 *
 * \param[in] tdtd	target distribute txn data where replay list is.
 * \param[in] record    update records from the update log.
 *
 * \retval		the pointer of distribute txn replay req if
 *                      the creation succeeds.
 * \retval		NULL if the creation fails.
 */
static struct distribute_txn_replay_req *
dtrq_create(struct update_records *record)
{
	struct distribute_txn_replay_req *new;

	OBD_ALLOC_PTR(new);
	if (new == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	new->dtrq_updates_size = update_records_size(record);
	OBD_ALLOC(new->dtrq_updates, new->dtrq_updates_size);
	if (new->dtrq_updates == NULL) {
		OBD_FREE_PTR(new);
		RETURN(ERR_PTR(-ENOMEM));
	}

	memcpy(new->dtrq_updates, record, new->dtrq_updates_size);

	spin_lock_init(&new->dtrq_sub_list_lock);
	INIT_LIST_HEAD(&new->dtrq_sub_list);
	INIT_LIST_HEAD(&new->dtrq_list);

	RETURN(new);
}

/**
 * Lookup distribute sub replay
 *
 * Lookup distribute sub replay in the sub list of distribute_txn_replay by
 * mdt_index.
 *
 * \param[in] distribute_txn_replay_req	the distribute txn replay req to lookup
 * \param[in] mdt_index			the mdt_index as the key of lookup
 *
 * \retval		the pointer of sub replay if it can be found.
 * \retval		NULL if it can not find.
 */
static struct distribute_txn_replay_req_sub *
dtrq_sub_lookup(struct distribute_txn_replay_req *dtrq, __u32 mdt_index)
{
	struct distribute_txn_replay_req_sub *dtrqs = NULL;
	struct distribute_txn_replay_req_sub *tmp;

	list_for_each_entry(tmp, &dtrq->dtrq_sub_list, dtrqs_list) {
		if (tmp->dtrqs_mdt_index == mdt_index) {
			dtrqs = tmp;
			break;
		}
	}
	return dtrqs;
}

/**
 * Insert distribute txn sub req replay
 *
 * Allocate sub replay req and insert distribute txn replay list.
 *
 * \param[in] dtrq	d to be added
 * \param[in] records   the update record
 * \param[in] mdt_index	the mdt_index of the update records
 *
 * \retval		0 if the adding succeeds.
 * \retval		negative errno if the adding fails.
 */
static int
dtrq_sub_create_and_insert(struct distribute_txn_replay_req *dtrq,
			   struct update_records *records,
			   struct llog_cookie *cookie,
			   __u32 mdt_index)
{
	struct distribute_txn_replay_req_sub *dtrqs = NULL;
	struct distribute_txn_replay_req_sub *new;
	ENTRY;

	spin_lock(&dtrq->dtrq_sub_list_lock);
	dtrqs = dtrq_sub_lookup(dtrq, mdt_index);
	spin_unlock(&dtrq->dtrq_sub_list_lock);
	if (dtrqs != NULL)
		RETURN(0);

	OBD_ALLOC_PTR(new);
	if (new == NULL)
		RETURN(-ENOMEM);

	INIT_LIST_HEAD(&new->dtrqs_list);
	new->dtrqs_mdt_index = mdt_index;
	new->dtrqs_llog_cookie = *cookie;
	spin_lock(&dtrq->dtrq_sub_list_lock);
	dtrqs = dtrq_sub_lookup(dtrq, mdt_index);
	if (dtrqs == NULL)
		list_add(&new->dtrqs_list, &dtrq->dtrq_sub_list);
	else
		OBD_FREE_PTR(new);
	spin_unlock(&dtrq->dtrq_sub_list_lock);

	RETURN(0);
}

/**
 * Insert update records to the replay list.
 *
 * Allocate distribute txn replay req and insert it into the replay
 * list, then insert the update records into the replay req.
 *
 * \param[in] tdtd	distribute txn replay data where the replay list
 *                      is.
 * \param[in] record    the update record
 * \param[in] cookie    cookie of the record
 * \param[in] index	mdt index of the record
 *
 * \retval		0 if the adding succeeds.
 * \retval		negative errno if the adding fails.
 */
int insert_update_records_to_replay_list(struct target_distribute_txn_data *tdtd,
					 struct update_records *record,
					 struct llog_cookie *cookie,
					 __u32 mdt_index)
{
	struct distribute_txn_replay_req *dtrq;
	int rc;
	ENTRY;

	spin_lock(&tdtd->tdtd_replay_list_lock);
	dtrq = dtrq_lookup(tdtd, record->ur_batchid);
	if (dtrq == NULL) {
		/* If the transno in the update record is 0, it means the
		 * update are from master MDT, and we will use the master
		 * last committed transno as its batchid. Note: if it got
		 * the records from the slave later, it needs to update
		 * the batchid by the transno in slave update log (see below) */
		dtrq = dtrq_create(record);
		if (IS_ERR(dtrq)) {
			spin_unlock(&tdtd->tdtd_replay_list_lock);
			RETURN(PTR_ERR(dtrq));
		}

		if (record->ur_master_transno == 0)
			dtrq->dtrq_updates->ur_master_transno =
				tdtd->tdtd_lut->lut_last_transno;
		dtrq_insert(tdtd, dtrq);
	} else if (record->ur_master_transno != 0 &&
		   dtrq->dtrq_updates->ur_master_transno !=
		   record->ur_master_transno) {
		/* If the master transno in update header is not matched with
		 * the one in the record, then it means the dtrq is originally
		 * created by master record, and we need update master transno
		 * and reposition the dtrq(by master transno). */
		dtrq->dtrq_updates->ur_master_transno =
						record->ur_master_transno;
		list_del_init(&dtrq->dtrq_list);
		dtrq_insert(tdtd, dtrq);
	}

	spin_unlock(&tdtd->tdtd_replay_list_lock);

	update_records_dump(dtrq->dtrq_updates, D_HA);

	rc = dtrq_sub_create_and_insert(dtrq, record, cookie, mdt_index);

	RETURN(rc);
}
EXPORT_SYMBOL(insert_update_records_to_replay_list);

/**
 * Dump updates of distribute txns.
 *
 * Output all of recovery updates in the distribute txn list to the
 * debug log.
 *
 * \param[in] tdtd	distribute txn data where all of distribute txn
 *                      are listed.
 * \param[in] mask	debug mask
 */
void dtrq_list_dump(struct target_distribute_txn_data *tdtd, unsigned int mask)
{
	struct distribute_txn_replay_req *dtrq;

	spin_lock(&tdtd->tdtd_replay_list_lock);
	list_for_each_entry(dtrq, &tdtd->tdtd_replay_list, dtrq_list)
		update_records_dump(dtrq->dtrq_updates, mask);
	spin_unlock(&tdtd->tdtd_replay_list_lock);
}
EXPORT_SYMBOL(dtrq_list_dump);

/**
 * Destroy distribute txn replay req
 *
 * Destroy distribute txn replay req and all of subs.
 *
 * \param[in] dtrq	distribute txn replqy req to be destroyed.
 */
void dtrq_destory(struct distribute_txn_replay_req *dtrq)
{
	struct distribute_txn_replay_req_sub	*dtrqs;
	struct distribute_txn_replay_req_sub	*tmp;

	LASSERT(list_empty(&dtrq->dtrq_list));
	spin_lock(&dtrq->dtrq_sub_list_lock);
	list_for_each_entry_safe(dtrqs, tmp, &dtrq->dtrq_sub_list, dtrqs_list) {
		list_del(&dtrqs->dtrqs_list);
		OBD_FREE_PTR(dtrqs);
	}
	spin_unlock(&dtrq->dtrq_sub_list_lock);

	if (dtrq->dtrq_updates != NULL)
		OBD_FREE(dtrq->dtrq_updates, dtrq->dtrq_updates_size);

	OBD_FREE_PTR(dtrq);
}
EXPORT_SYMBOL(dtrq_destory);

/**
 * Destroy all of replay req.
 *
 * Destroy all of replay req in the replay list.
 *
 * \param[in] tdtd	target distribute txn data where the replay list is.
 */
void dtrq_list_destroy(struct target_distribute_txn_data *tdtd)
{
	struct distribute_txn_replay_req *dtrq;
	struct distribute_txn_replay_req *tmp;

	spin_lock(&tdtd->tdtd_replay_list_lock);
	list_for_each_entry_safe(dtrq, tmp, &tdtd->tdtd_replay_list,
				 dtrq_list) {
		list_del_init(&dtrq->dtrq_list);
		dtrq_destory(dtrq);
	}
	spin_unlock(&tdtd->tdtd_replay_list_lock);
}
EXPORT_SYMBOL(dtrq_list_destroy);

/**
 * Get next req in the replay list
 *
 * Get next req needs to be replayed, since it is a sorted list
 * (by master MDT transno)
 *
 * \param[in] tdtd	distribute txn data where the replay list is
 *
 * \retval		the pointer of update recovery header
 */
struct distribute_txn_replay_req *
distribute_txn_get_next_req(struct target_distribute_txn_data *tdtd)
{
	struct distribute_txn_replay_req *dtrq = NULL;

	spin_lock(&tdtd->tdtd_replay_list_lock);
	if (!list_empty(&tdtd->tdtd_replay_list)) {
		dtrq = list_entry(tdtd->tdtd_replay_list.next,
				 struct distribute_txn_replay_req, dtrq_list);
		list_del_init(&dtrq->dtrq_list);
	}
	spin_unlock(&tdtd->tdtd_replay_list_lock);

	return dtrq;
}
EXPORT_SYMBOL(distribute_txn_get_next_req);

/**
 * Get next transno in the replay list, because this is the sorted
 * list, so it will return the transno of next req in the list.
 *
 * \param[in] tdtd	distribute txn data where the replay list is
 *
 * \retval		the transno of next update in the list
 */
__u64 distribute_txn_get_next_transno(struct target_distribute_txn_data *tdtd)
{
	struct distribute_txn_replay_req	*dtrq = NULL;
	__u64					transno = 0;

	spin_lock(&tdtd->tdtd_replay_list_lock);
	if (!list_empty(&tdtd->tdtd_replay_list)) {
		dtrq = list_entry(tdtd->tdtd_replay_list.next,
				 struct distribute_txn_replay_req, dtrq_list);
		transno = dtrq->dtrq_updates->ur_master_transno;
	}
	spin_unlock(&tdtd->tdtd_replay_list_lock);

	CDEBUG(D_HA, "%s: Next update transno "LPU64"\n",
	       tdtd->tdtd_lut->lut_obd->obd_name, transno);
	return transno;
}
EXPORT_SYMBOL(distribute_txn_get_next_transno);

/**
 * Check if the update of one object is committed
 *
 * Check whether the update for the object is committed by checking whether
 * the correspondent sub exists in the replay req. If it is committed, mark
 * the committed flag in correspondent the sub thandle.
 *
 * \param[in] env	execution environment
 * \param[in] dtrq	replay request
 * \param[in] dt_obj	object for the update
 * \param[in] top_th	top thandle
 * \param[in] sub_th	sub thandle which the update belongs to
 *
 * \retval		1 if the update is not committed.
 * \retval		0 if the update is committed.
 * \retval		negative errno if some other failures happen.
 */
static int update_is_committed(const struct lu_env *env,
			       struct distribute_txn_replay_req *dtrq,
			       struct dt_object *dt_obj,
			       struct top_thandle *top_th,
			       struct sub_thandle *st)
{
	struct seq_server_site	*seq_site;
	const struct lu_fid	*fid = lu_object_fid(&dt_obj->do_lu);
	struct distribute_txn_replay_req_sub	*dtrqs;
	__u32			mdt_index;
	ENTRY;

	if (st->st_sub_th != NULL)
		RETURN(1);

	if (st->st_committed)
		RETURN(0);

	seq_site = lu_site2seq(dt_obj->do_lu.lo_dev->ld_site);
	if (fid_is_update_log(fid)) {
		mdt_index = fid_oid(fid);
	} else if (!fid_seq_in_fldb(fid_seq(fid))) {
		mdt_index = seq_site->ss_node_id;
	} else {
		struct lu_server_fld *fld;
		struct lu_seq_range range = {0};
		int rc;

		fld = seq_site->ss_server_fld;
		fld_range_set_type(&range, LU_SEQ_RANGE_MDT);
		LASSERT(fld->lsf_seq_lookup != NULL);
		rc = fld->lsf_seq_lookup(env, fld, fid_seq(fid),
					 &range);
		if (rc < 0)
			RETURN(rc);
		mdt_index = range.lsr_index;
	}

	dtrqs = dtrq_sub_lookup(dtrq, mdt_index);
	if (dtrqs != NULL || top_th->tt_multiple_thandle->tmt_committed) {
		st->st_committed = 1;
		if (dtrqs != NULL)
			st->st_cookie = dtrqs->dtrqs_llog_cookie;
		RETURN(0);
	}
	RETURN(1);
}

/**
 * Implementation of different update methods for update recovery.
 *
 * These following functions update_recovery_$(update_name) implement
 * different updates recovery methods. They will extract the parameters
 * from the common parameters area and call correspondent dt API to redo
 * the update.
 *
 * \param[in] env	execution environment
 * \param[in] op	update operation to be replayed
 * \param[in] params	common update parameters which holds all parameters
 *                      of the operation
 * \param[in] th	transaction handle
 * \param[in] declare	indicate it will do declare or real execution, true
 *                      means declare, false means real execution
 *
 * \retval		0 if it succeeds.
 * \retval		negative errno if it fails.
 */
static int update_recovery_create(const struct lu_env *env,
				  struct dt_object *dt_obj,
				  const struct update_op *op,
				  const struct update_params *params,
				  struct thandle *th, bool declare)
{
	struct update_thread_info *uti = update_env_info(env);
	struct lu_attr		*attr;
	struct dt_object_format	dof;
	__u16			size;
	int rc;
	ENTRY;

	if (dt_object_exists(dt_obj))
		RETURN(-EEXIST);

	attr = update_params_get_param_buf(params,
					   op->uop_params_off[0],
					   &size);
	if (attr == NULL)
		RETURN(-EIO);
	if (size != sizeof(*attr))
		RETURN(-EIO);

	lu_attr_le_to_cpu(&uti->uti_attr, attr);

	dof.dof_type = dt_mode_to_dft(attr->la_mode);
	if (declare) {
		rc = dt_declare_create(env, dt_obj, &uti->uti_attr, NULL,
				       &dof, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_create(env, dt_obj, &uti->uti_attr, NULL, &dof, th);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_destroy(const struct lu_env *env,
				   struct dt_object *dt_obj,
				   const struct update_op *op,
				   const struct update_params *params,
				   struct thandle *th, bool declare)
{
	int	rc;
	ENTRY;

	if (declare) {
		rc = dt_declare_destroy(env, dt_obj, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_destroy(env, dt_obj, th);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_ref_add(const struct lu_env *env,
				   struct dt_object *dt_obj,
				   const struct update_op *op,
				   const struct update_params *params,
				   struct thandle *th, bool declare)
{
	int	rc;
	ENTRY;

	if (declare) {
		rc = dt_declare_ref_add(env, dt_obj, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_ref_add(env, dt_obj, th);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_ref_del(const struct lu_env *env,
				   struct dt_object *dt_obj,
				   const struct update_op *op,
				   const struct update_params *params,
				   struct thandle *th, bool declare)
{
	int	rc;
	ENTRY;

	if (declare) {
		rc = dt_declare_ref_del(env, dt_obj, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_ref_del(env, dt_obj, th);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_attr_set(const struct lu_env *env,
				    struct dt_object *dt_obj,
				    const struct update_op *op,
				    const struct update_params *params,
				    struct thandle *th, bool declare)
{
	struct update_thread_info *uti = update_env_info(env);
	struct lu_attr		  *attr;
	__u16			  size;
	int			  rc;
	ENTRY;

	attr = update_params_get_param_buf(params, op->uop_params_off[0],
					   &size);
	if (attr == NULL)
		RETURN(-EIO);
	if (size != sizeof(*attr))
		RETURN(-EIO);

	lu_attr_le_to_cpu(&uti->uti_attr, attr);

	if (declare) {
		rc = dt_declare_attr_set(env, dt_obj, &uti->uti_attr, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_attr_set(env, dt_obj, &uti->uti_attr, th,
				 BYPASS_CAPA);
		dt_write_unlock(env, dt_obj);
	}

	RETURN(rc);
}

static int update_recovery_xattr_set(const struct lu_env *env,
				     struct dt_object *dt_obj,
				     const struct update_op *op,
				     const struct update_params *params,
				     struct thandle *th, bool declare)
{
	struct update_thread_info *uti = update_env_info(env);
	char		*buf;
	char		*name;
	int		fl;
	__u16		size;
	int		rc;
	ENTRY;

	name = update_params_get_param_buf(params,
					   op->uop_params_off[0], &size);
	if (name == NULL)
		RETURN(-EIO);

	buf = update_params_get_param_buf(params,
					  op->uop_params_off[1], &size);
	if (buf == NULL)
		RETURN(-EIO);

	uti->uti_buf.lb_buf = buf;
	uti->uti_buf.lb_len = (size_t)size;

	buf = update_params_get_param_buf(params, op->uop_params_off[2],
					  &size);
	if (buf == NULL)
		RETURN(-EIO);
	if (size != sizeof(fl))
		RETURN(-EIO);

	fl = le32_to_cpu(*(int *)buf);

	if (declare) {
		rc = dt_declare_xattr_set(env, dt_obj, &uti->uti_buf, name,
					  fl, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_xattr_set(env, dt_obj, &uti->uti_buf, name, fl, th,
				  BYPASS_CAPA);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_index_insert(const struct lu_env *env,
					struct dt_object *dt_obj,
					const struct update_op *op,
					const struct update_params *params,
					struct thandle *th, bool declare)
{
	struct update_thread_info *uti = update_env_info(env);
	struct lu_fid		*fid;
	char			*name;
	__u32			*ptype;
	__u32			type;
	__u16			size;
	int rc;
	ENTRY;

	name = update_params_get_param_buf(params, op->uop_params_off[0],
					   &size);
	if (name == NULL)
		RETURN(-EIO);

	fid = update_params_get_param_buf(params, op->uop_params_off[1],
					  &size);
	if (fid == NULL)
		RETURN(-EIO);
	if (size != sizeof(*fid))
		RETURN(-EIO);

	fid_le_to_cpu(&uti->uti_fid, fid);

	ptype = update_params_get_param_buf(params, op->uop_params_off[2],
					   &size);
	if (ptype == NULL)
		RETURN(-EIO);
	if (size != sizeof(*ptype))
		RETURN(-EIO);
	type = le32_to_cpu(*ptype);

	if (dt_try_as_dir(env, dt_obj) == 0)
		RETURN(-ENOTDIR);

	uti->uti_rec.rec_fid = &uti->uti_fid;
	uti->uti_rec.rec_type = type;

	if (declare) {
		rc = dt_declare_insert(env, dt_obj,
				       (const struct dt_rec *)&uti->uti_rec,
				       (const struct dt_key *)name, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_insert(env, dt_obj,
			       (const struct dt_rec *)&uti->uti_rec,
			       (const struct dt_key *)name, th, BYPASS_CAPA,
				0);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_index_delete(const struct lu_env *env,
					struct dt_object *dt_obj,
					const struct update_op *op,
					const struct update_params *params,
					struct thandle *th, bool declare)
{
	char	*name;
	__u16	size;
	int	rc;
	ENTRY;

	name = update_params_get_param_buf(params,
					   op->uop_params_off[0], &size);
	if (name == NULL)
		RETURN(-EIO);

	if (dt_try_as_dir(env, dt_obj) == 0)
		RETURN(-ENOTDIR);

	if (declare) {
		rc = dt_declare_delete(env, dt_obj,
				       (const struct dt_key *)name, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_delete(env, dt_obj,
			       (const struct dt_key *)name,  th, BYPASS_CAPA);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_write(const struct lu_env *env,
				 struct dt_object *dt_obj,
				 const struct update_op *op,
				 const struct update_params *params,
				 struct thandle *th, bool declare)
{
	struct update_thread_info *uti = update_env_info(env);
	char		*buf;
	__u64		pos;
	__u16		size;
	int rc;
	ENTRY;

	buf = update_params_get_param_buf(params,
					  op->uop_params_off[0], &size);
	if (buf == NULL)
		RETURN(-EIO);

	uti->uti_buf.lb_buf = buf;
	uti->uti_buf.lb_len = size;

	buf = update_params_get_param_buf(params,
					  op->uop_params_off[1], &size);
	if (buf == NULL)
		RETURN(-EIO);

	pos = le64_to_cpu(*(__u64 *)buf);

	if (declare) {
		rc = dt_declare_write(env, dt_obj, &uti->uti_buf, pos, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_write(env, dt_obj, &uti->uti_buf, &pos, th,
			      BYPASS_CAPA, 1);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

static int update_recovery_xattr_del(const struct lu_env *env,
				     struct dt_object *dt_obj,
				     const struct update_op *op,
				     const struct update_params *params,
				     struct thandle *th, bool declare)
{
	char	*name;
	__u16	size;
	int	rc;
	ENTRY;

	name = update_params_get_param_buf(params,
					   op->uop_params_off[0], &size);
	if (name == NULL)
		RETURN(-EIO);

	if (declare) {
		rc = dt_declare_xattr_del(env, dt_obj, name, th);
	} else {
		dt_write_lock(env, dt_obj, MOR_TGT_CHILD);
		rc = dt_xattr_del(env, dt_obj, name, th, BYPASS_CAPA);
		dt_write_unlock(env, dt_obj);
	}
	RETURN(rc);
}

#define update_recovery_declare(env, dt_obj, name, op, params, th)	\
	update_recovery_##name(env, dt_obj, op, params, th, true)

#define update_recovery_execute(env, dt_obj, name, op, params, th)	\
	update_recovery_##name(env, dt_obj, op, params, th, false)

/**
 * Declare distribute txn replay
 *
 * Declare distribute txn replay by update records. It will check if , it still
 * needs to call declare API to create sub thandle (see struct sub_thandle)
 * attached it to the lod_transaction and track the whole operation as the
 * normal distributed operation.
 *
 * \param[in] env	execution environment
 * \param[in] tdtd	distribute txn replay data which hold all of replay
 *                      reqs and all replay parameters.
 * \param[in] dtrq	distribute transaction replay req.
 * \param[in] th	transaction handle.
 *
 * \retval		0 if declare succeeds.
 * \retval		negative errno if declare fails.
 */
static int update_recovery_handle_declare(const struct lu_env *env,
					struct target_distribute_txn_data *tdtd,
					struct distribute_txn_replay_req *dtrq,
					struct thandle *th)
{
	struct update_records	*records = dtrq->dtrq_updates;
	struct update_ops	*ops = &records->ur_ops;
	struct update_params	*params = update_records_get_params(records);
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct top_multiple_thandle *tmt = top_th->tt_multiple_thandle;
	struct update_op	*op;
	unsigned int		i;
	int			rc = 0;
	ENTRY;

	/* These records have been swabbed in llog_cat_process() */
	for (i = 0, op = &ops->uops_op[0]; i < ops->uops_count;
	     i++, op = update_op_next_op(op)) {
		struct lu_fid		*fid = &op->uop_fid;
		struct dt_object	*dt_obj;
		struct dt_object	*sub_dt_obj;
		struct dt_device	*sub_dt;
		struct sub_thandle	*st;

		dt_obj = dt_locate(env, tdtd->tdtd_dt, fid);
		if (IS_ERR(dt_obj)) {
			rc = PTR_ERR(dt_obj);
			break;
		}
		sub_dt_obj = dt_object_child(dt_obj);

		/* Create sub thandle if not */
		sub_dt = lu2dt_dev(sub_dt_obj->do_lu.lo_dev);
		st = lookup_sub_thandle(tmt, sub_dt);
		if (st == NULL) {
			st = create_sub_thandle(tmt, sub_dt);
			if (IS_ERR(st))
				GOTO(next, rc = PTR_ERR(st));
		}

		/* check if updates on the OSD/OSP are committed */
		rc = update_is_committed(env, dtrq, dt_obj, top_th, st);
		if (rc == 0)
			/* If this is committed, goto next */
			goto next;

		if (rc < 0)
			GOTO(next, rc);

		/* Create thandle for sub thandle if needed */
		if (st->st_sub_th == NULL) {
			rc = sub_thandle_trans_create(env, top_th, st);
			if (rc != 0)
				GOTO(next, rc);
		}

		switch (op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_declare(env, sub_dt_obj, create,
						     op, params, st->st_sub_th);
			break;
		case OUT_DESTROY:
			rc = update_recovery_declare(env, sub_dt_obj, destroy,
						     op, params, st->st_sub_th);
			break;
		case OUT_REF_ADD:
			rc = update_recovery_declare(env, sub_dt_obj, ref_add,
						     op, params, st->st_sub_th);
			break;
		case OUT_REF_DEL:
			rc = update_recovery_declare(env, sub_dt_obj, ref_del,
						     op, params, st->st_sub_th);
			break;
		case OUT_ATTR_SET:
			rc = update_recovery_declare(env, sub_dt_obj, attr_set,
						     op, params, st->st_sub_th);
			break;
		case OUT_XATTR_SET:
			rc = update_recovery_declare(env, sub_dt_obj, xattr_set,
						     op, params, st->st_sub_th);
			break;
		case OUT_INDEX_INSERT:
			rc = update_recovery_declare(env, sub_dt_obj,
						     index_insert, op, params,
						     st->st_sub_th);
			break;
		case OUT_INDEX_DELETE:
			rc = update_recovery_declare(env, sub_dt_obj,
						     index_delete, op, params,
						     st->st_sub_th);
			break;
		case OUT_WRITE:
			rc = update_recovery_declare(env, sub_dt_obj, write,
						     op, params, st->st_sub_th);
			break;
		case OUT_XATTR_DEL:
			rc = update_recovery_declare(env, sub_dt_obj, xattr_del,
						     op, params, st->st_sub_th);
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}
next:
		lu_object_put(env, &dt_obj->do_lu);
		if (rc < 0)
			break;
	}

	th->th_result = rc;
	RETURN(rc);
}

/**
 * redo updates on MDT if needed.
 *
 * During DNE recovery, the recovery thread (target_recovery_thread) will call
 * this function to replay distribute txn updates on all MDTs. It only replay
 * updates on the MDT where the update record is missing.
 *
 * If the update already exists on the MDT, then it does not need replay the
 * updates on that MDT, and only mark the sub transaction has been committed
 * there.
 *
 * \param[in] env	execution environment
 * \param[in] tdtd	target distribute txn data, which holds the replay list
 *                      and all parameters needed by replay process.
 * \param[in] dtrq	distribute txn replay req.
 *
 * \retval		0 if replay succeeds.
 * \retval		negative errno if replay failes.
 */
int distribute_txn_replay_handle(struct lu_env *env,
				 struct target_distribute_txn_data *tdtd,
				 struct distribute_txn_replay_req *dtrq)
{
	struct update_records	*records = dtrq->dtrq_updates;
	struct update_ops	*ops = &records->ur_ops;
	struct update_params	*params = update_records_get_params(records);
	struct lu_context	session_env;
	struct update_op	*op;
	struct thandle		*th = NULL;
	struct top_thandle	*top_th;
	struct top_multiple_thandle *tmt;
	struct thandle_update_records *tur = NULL;
	unsigned int		i;
	int			rc = 0;
	ENTRY;

	/* initialize session, it is needed for the handler of target */
	rc = lu_context_init(&session_env, LCT_SERVER_SESSION |
					       LCT_NOREF);
	if (rc) {
		CERROR("%s: failure to initialize session: rc = %d\n",
		       tdtd->tdtd_lut->lut_obd->obd_name, rc);
		RETURN(rc);
	}
	lu_context_enter(&session_env);
	env->le_ses = &session_env;
	lu_env_refill(env);
	update_records_dump(records, D_HA);
	th = top_trans_create(env, NULL);
	if (IS_ERR(th))
		GOTO(exit_session, rc = PTR_ERR(th));

	/* Create distribute transaction structure for this top thandle */
	top_th = container_of(th, struct top_thandle, tt_super);
	rc = top_trans_create_tmt(env, top_th);
	if (rc < 0)
		GOTO(stop_trans, rc);

	/* check if the distribute transaction has been committed */
	tmt = top_th->tt_multiple_thandle;
	tmt->tmt_batchid = records->ur_batchid;
	if (tmt->tmt_batchid < tdtd->tdtd_committed_batchid)
		tmt->tmt_committed = 1;

	tmt->tmt_master_sub_dt = tdtd->tdtd_lut->lut_bottom;
	rc = update_recovery_handle_declare(env, tdtd, dtrq, th);
	if (rc < 0)
		GOTO(stop_trans, rc);

	tur = &update_env_info(env)->uti_tur;
	tur->tur_update_records = records;
	tur->tur_update_records_size = update_records_size(records);
	tur->tur_update_params = params;
	tmt->tmt_update_records = tur;

	rc = top_trans_start(env, NULL, th);
	if (rc < 0)
		GOTO(stop_trans, rc);

	op = &ops->uops_op[0];
	for (i = 0, op = &ops->uops_op[0]; i < ops->uops_count;
	     i++, op = update_op_next_op(op)) {
		struct lu_fid	 *fid = &op->uop_fid;
		struct dt_object *dt_obj;
		struct dt_object *sub_dt_obj;
		struct dt_device *sub_dt;
		struct sub_thandle *st;

		dt_obj = dt_locate(env, tdtd->tdtd_dt, fid);
		if (IS_ERR(dt_obj)) {
			rc = PTR_ERR(dt_obj);
			break;
		}

		/* Since we do not need pack the updates, so replay
		 * the update on the bottom layer(OSP/OSD) directly */
		sub_dt_obj = dt_object_child(dt_obj);

		sub_dt = lu2dt_dev(sub_dt_obj->do_lu.lo_dev);
		st = lookup_sub_thandle(tmt, sub_dt);
		LASSERT(st != NULL);
		if (st->st_committed) {
			CDEBUG(D_HA, "Skip %s "DFID" already committed on %s\n",
			       update_op_str(op->uop_type), PFID(fid),
			       sub_dt->dd_lu_dev.ld_obd->obd_name);
			LASSERT(st->st_sub_th == NULL);
			goto next;
		}

		LASSERT(st->st_sub_th != NULL);
		CDEBUG(D_HA, "Replay update %s "DFID" on %s\n",
		       update_op_str(op->uop_type), PFID(fid),
		       sub_dt->dd_lu_dev.ld_obd->obd_name);

		switch (op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_execute(env, sub_dt_obj, create,
						     op, params, st->st_sub_th);
			break;
		case OUT_DESTROY:
			rc = update_recovery_execute(env, sub_dt_obj, destroy,
						     op, params, st->st_sub_th);
			break;
		case OUT_REF_ADD:
			rc = update_recovery_execute(env, sub_dt_obj, ref_add,
						     op, params, st->st_sub_th);
			break;
		case OUT_REF_DEL:
			rc = update_recovery_execute(env, sub_dt_obj, ref_del,
						     op, params, st->st_sub_th);
			break;
		case OUT_ATTR_SET:
			rc = update_recovery_execute(env, sub_dt_obj, attr_set,
						     op, params, st->st_sub_th);
			break;
		case OUT_XATTR_SET:
			rc = update_recovery_execute(env, sub_dt_obj, xattr_set,
						     op, params, st->st_sub_th);
			break;
		case OUT_INDEX_INSERT:
			rc = update_recovery_execute(env, sub_dt_obj,
						     index_insert, op, params,
						     st->st_sub_th);
			break;
		case OUT_INDEX_DELETE:
			rc = update_recovery_execute(env, sub_dt_obj,
						     index_delete, op, params,
						     st->st_sub_th);
			break;
		case OUT_WRITE:
			rc = update_recovery_execute(env, sub_dt_obj,
						     write, op, params,
						     st->st_sub_th);
			break;
		case OUT_XATTR_DEL:
			rc = update_recovery_execute(env, sub_dt_obj,
						     xattr_del, op, params,
						     st->st_sub_th);
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}
next:
		lu_object_put(env, &dt_obj->do_lu);
		if (rc < 0)
			break;
	}

stop_trans:
	if (rc < 0)
		th->th_result = rc;
	rc = top_trans_stop(env, tdtd->tdtd_dt, th);
	if (tur != NULL) {
		tur->tur_update_records = NULL;
		tur->tur_update_params = NULL;
	}
exit_session:
	lu_context_exit(&session_env);
	lu_context_fini(&session_env);
	RETURN(rc);
}
EXPORT_SYMBOL(distribute_txn_replay_handle);
