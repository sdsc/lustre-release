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
 * or update replay records in the update log. For update replay, the recovery
 * thread will call API in this file to redo the update.
 *
 * After the Master MDT restarts, it will retrieve the update logs from all
 * of MDTs, for each distributed operation, it will check updates on all MDTs,
 * if some updates log records are missing on some MDTs, the recovery thread
 * will redo updates on these MDTs.
 *
 * All of the update records will be linked to the update_recovery_data as the
 * update_recovery_header, which stands for one operation. The recovery thread
 * will pick replay update from this list.
 *
 * For each operation (struct each update_recovery_header), if updates on one
 * MDT have been committed to disk, the update_recovery will be added to the
 * update_recovery_header (urh_update_list), and the recovery thread will know
 * what updates are missing for the operation.
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
 * Lookup update_recovery_header
 *
 * Lookup update_recovery_header under the replay list by cookie, which is
 * created by master MDT during normal execution (see lod_trans_start).
 *
 * It is assumed the list has been locked (urd_list_lock) before calling
 * this function.
 *
 * \param[in] urd	update_recovery_data, which holds the update replay
 *                      list.
 * \param[in] cookie	cookie which is used as the key.
 *
 * \retval		pointer of the header if succeeds.
 * \retval		NULL if can not find it.
 */
static struct update_recovery_header *
update_recovery_header_lookup(struct update_recovery_data *urd, __u64 cookie)
{
	struct update_recovery_header	*tmp;
	struct update_recovery_header	*urh = NULL;

	list_for_each_entry(tmp, &urd->urd_list, urh_list) {
		if (tmp->urh_updates->ur_cookie == cookie) {
			urh = tmp;
			break;
		}
	}
	return urh;
}

/**
 * insert recovery update header
 *
 * Insert recovery update header to the update recovery list. Note: the
 * recovery list is a sorted list, which is sorted by transno of master MDT.
 *
 * \param[in] urd	update_recovery_data where update recovery list is
 * \param[in] new	update record hearder to be inserted
 */
static void
update_recovery_header_insert(struct update_recovery_data *urd,
			      struct update_recovery_header *new)
{
	struct update_recovery_header *iter;

	list_for_each_entry_reverse(iter, &urd->urd_list, urh_list) {
		if (iter->urh_updates->ur_batchid >
		    new->urh_updates->ur_batchid)
			continue;

		list_add(&new->urh_list, &iter->urh_list);
		break;
	}
	if (list_empty(&new->urh_list))
		list_add(&new->urh_list, &urd->urd_list);
}

/**
 * create recovery update header
 *
 * Create recovery update header.
 *
 * \param[in] urd	update_recovery_data where update recovery list is.
 * \param[in] record    update records got from update log, on which
 *                      update_recovery_header creation will be based on.
 *
 * \retval		the pointer of recovery update header if
 *                      the creation succeeds.
 * \retval		NULL if the creation fails.
 */
static struct update_recovery_header *
update_recovery_header_create(struct update_records *record)
{
	struct update_recovery_header *new;

	OBD_ALLOC_PTR(new);
	if (new == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	new->urh_updates_size = update_records_size(record);
	OBD_ALLOC(new->urh_updates, new->urh_updates_size);
	if (new->urh_updates == NULL) {
		OBD_FREE_PTR(new);
		RETURN(ERR_PTR(-ENOMEM));
	}

	memcpy(new->urh_updates, record, new->urh_updates_size);

	spin_lock_init(&new->urh_update_list_lock);
	INIT_LIST_HEAD(&new->urh_update_list);
	INIT_LIST_HEAD(&new->urh_list);

	RETURN(new);
}

/**
 * Lookup recovery update record
 *
 * Lookup recovery update in the update recovery header list by mdt_index.
 *
 * \param[in] lruh	the header of recovery update where lookup is
 * \param[in] mdt_index the mdt_index which is the key of lookup
 *
 * \retval		the pointer of recovery update if it can be found.
 * \retval		NULL if it can not find.
 */
static struct update_recovery *
update_recovery_lookup(struct update_recovery_header *urh,
		       __u32 mdt_index)
{
	struct update_recovery *ru = NULL;
	struct update_recovery *tmp;

	list_for_each_entry(tmp, &urh->urh_update_list, ur_list) {
		if (tmp->ur_mdt_index == mdt_index) {
			ru = tmp;
			break;
		}
	}
	return ru;
}

/**
 * Add recovery update record
 *
 * Allocate recovery update and insert update recovery header list.
 *
 * \param[in] urh	the header of recovery update to be added
 * \param[in] records   the update record
 * \param[in] mdt_index	the mdt_index of the update records
 *
 * \retval		0 if the adding succeeds.
 * \retval		negative errno if the adding fails.
 */
static int
update_recovery_create_insert(struct update_recovery_header *urh,
			      struct update_records *records,
			      struct llog_cookie *cookie,
			      __u32 mdt_index)
{
	struct update_recovery *ur = NULL;
	struct update_recovery *new;
	ENTRY;

	spin_lock(&urh->urh_update_list_lock);
	ur = update_recovery_lookup(urh, mdt_index);
	spin_unlock(&urh->urh_update_list_lock);
	if (ur != NULL)
		RETURN(0);

	OBD_ALLOC_PTR(new);
	if (new == NULL)
		RETURN(-ENOMEM);

	INIT_LIST_HEAD(&new->ur_list);
	new->ur_mdt_index = mdt_index;
	new->ur_llog_cookie = *cookie;
	spin_lock(&urh->urh_update_list_lock);
	ur = update_recovery_lookup(urh, mdt_index);
	if (ur == NULL)
		list_add(&new->ur_list, &urh->urh_update_list);
	spin_unlock(&urh->urh_update_list_lock);

	RETURN(0);
}

/**
 * Insert recovery update to the update recovery list.
 *
 * Try to find recovery header of the record in the recovery_list.
 * \param[in] lruh	the header of recovery update to be added
 * \param[in] lrd	the update recovery data arguments, where the
 *                      recovery update list is
 * \param[in] records   the update record
 *
 * \retval		0 if the adding succeeds.
 * \retval		negative errno if the adding fails.
 */
int insert_update_records_to_recovery_list(struct lu_target *lut,
					   struct update_records *record,
					   struct llog_cookie *cookie,
					   __u32 index)
{
	struct update_recovery_data	*urd = lut->lut_update_recovery_data;
	struct update_recovery_header	*urh;
	int rc;
	ENTRY;

	LASSERT(urd != NULL);
	spin_lock(&urd->urd_list_lock);
	urh = update_recovery_header_lookup(urd, record->ur_cookie);
	if (urh == NULL) {
		/* If the transno in the update record is 0, it means the
		 * update are from master MDT, and we will use the master
		 * last committed transno as its batchid. Note: if it got
		 * the records from the slave later, it needs to update
		 * the batchid by the transno in slave update log (see below) */
		urh = update_recovery_header_create(record);
		if (IS_ERR(urh)) {
			spin_unlock(&urd->urd_list_lock);
			RETURN(PTR_ERR(urh));
		}

		if (urh->urh_updates->ur_batchid == 0)
			urh->urh_updates->ur_batchid = lut->lut_last_transno;

		update_recovery_header_insert(urd, urh);
	} else if (record->ur_batchid != 0 &&
		   urh->urh_updates->ur_batchid != record->ur_batchid) {
		/* If the batchid in update header is not matched with the one
		 * in the record, then it means the urh is originally created
		 * by the update log in the master MDT, and we need update
		 * the batchid and reposition the urh(by real batchid) inside
		 * the list. */
		urh->urh_updates->ur_batchid = record->ur_batchid;
		list_del_init(&urh->urh_list);
		update_recovery_header_insert(urd, urh);
	}
	spin_unlock(&urd->urd_list_lock);

	update_records_dump(urh->urh_updates, D_HA);

	rc = update_recovery_create_insert(urh, record, cookie, index);

	RETURN(rc);
}
EXPORT_SYMBOL(insert_update_records_to_recovery_list);

/**
 * Dump recovery updates to the update recovery list.
 *
 * Output all of recovery updates in the recovery list to the debug log.
 *
 * \param[in] urd	update recovery data where all of recovery updates
 *                      are listed.
 * \param[in] mask	debug mask
 */
void dump_updates_in_recovery_list(struct update_recovery_data *urd,
				   unsigned int mask)
{
	struct update_recovery_header *urh;

	spin_lock(&urd->urd_list_lock);
	list_for_each_entry(urh, &urd->urd_list, urh_list)
		update_records_dump(urh->urh_updates, mask);
	spin_unlock(&urd->urd_list_lock);
}
EXPORT_SYMBOL(dump_updates_in_recovery_list);

/**
 * Destroy update recovery hearder
 *
 * Destroy update recovery header and all of update recovery under it.
 *
 * \param[in] urh	update recovery header to be destroyed.
 */
void update_recovery_header_destory(struct update_recovery_header *urh)
{
	struct update_recovery	*ur;
	struct update_recovery	*tmp;

	LASSERT(list_empty(&urh->urh_list));
	spin_lock(&urh->urh_update_list_lock);
	list_for_each_entry_safe(ur, tmp, &urh->urh_update_list, ur_list) {
		list_del(&ur->ur_list);
		OBD_FREE_PTR(ur);
	}
	spin_unlock(&urh->urh_update_list_lock);

	if (urh->urh_updates != NULL)
		OBD_FREE(urh->urh_updates, urh->urh_updates_size);

	OBD_FREE_PTR(urh);
}
EXPORT_SYMBOL(update_recovery_header_destory);

/**
 * Destroy all of updates.
 *
 * Destroy all of updates in the recovery list.
 *
 * \param[in] urh	update recovery header to be destroyed.
 */
void destroy_updates_in_recovery_list(struct update_recovery_data *urd)
{
	struct update_recovery_header *urh;
	struct update_recovery_header *tmp;

	spin_lock(&urd->urd_list_lock);
	list_for_each_entry_safe(urh, tmp, &urd->urd_list, urh_list) {
		list_del_init(&urh->urh_list);
		update_recovery_header_destory(urh);
	}
	spin_unlock(&urd->urd_list_lock);
}
EXPORT_SYMBOL(destroy_updates_in_recovery_list);

/**
 * Get next update in the recovery list, because this is the sorted
 * list, so it also means get next update needs to be replayed.
 *
 * \param[in] urd	update recovery data where the recovery list is
 *
 * \retval		the pointer of update recovery header
 */
struct update_recovery_header *
update_recovery_get_next_update(struct update_recovery_data *urd)
{
	struct update_recovery_header	*urh = NULL;

	spin_lock(&urd->urd_list_lock);
	if (!list_empty(&urd->urd_list)) {
		urh = list_entry(urd->urd_list.next,
				 struct update_recovery_header, urh_list);
		list_del_init(&urh->urh_list);
	}
	spin_unlock(&urd->urd_list_lock);

	return urh;
}
EXPORT_SYMBOL(update_recovery_get_next_update);

/**
 * Get next update transno in the recovery list, because this is the sorted
 * list, so it will return the transno of next update in the list.
 *
 * \param[in] urd	update recovery data where the recovery list is
 *
 * \retval		the transno of next update in the list
 */
__u64 update_recovery_get_next_transno(struct update_recovery_data *urd)
{
	struct update_recovery_header	*urh = NULL;
	__u64				transno = 0;

	spin_lock(&urd->urd_list_lock);
	if (!list_empty(&urd->urd_list)) {
		urh = list_entry(urd->urd_list.next,
				 struct update_recovery_header, urh_list);
		transno = urh->urh_updates->ur_batchid;
	}
	spin_unlock(&urd->urd_list_lock);

	CDEBUG(D_HA, "Next update transno "LPU64"\n", transno);
	return transno;
}
EXPORT_SYMBOL(update_recovery_get_next_transno);

/* Check if the sub thandle is committed
 *
 * Check if the sub thandle is committed under top thandle.
 *
 * \param[in] top_th	top_thandle
 * \param[in] sub_th	sub thandle
 *
 * \retval		true if the sub_thandle is committed.
 * \retval		false if the sub thandle is not committed yet.
 */
static bool sub_thandle_is_committed(struct top_thandle *top_th,
				     struct thandle *sub_th)
{
	struct sub_thandle *st;

	if (top_th->tt_child == sub_th) {
		if (top_th->tt_child->th_committed)
			RETURN(true);
		RETURN(false);
	}

	st = lookup_sub_thandle(&top_th->tt_super, sub_th->th_dev);
	LASSERT(st != NULL);
	if (st->st_sub_th->th_committed)
		RETURN(true);

	RETURN(false);
}

/**
 * Check if the update is committed
 *
 * Check whether the updates is committed, and mark the committed flag in the
 * sub thandle. It will know this by checking whether the update recovery,
 * collecting in update log retrieve process, (see lod_sub_recovery_thread())
 * exists in the list of update_recovery_header.
 *
 * \param[in] env	execution environment
 * \param[in] dt	the dt device of the recovery, usually it is LOD.
 * \param[in] urh	the update recovery header which holds the
 *                      updates.
 * \param[in] op	update to be checked
 * \param[in] top_th	top thandle
 * \param[in] sub_th	sub thandle which the update belongs to
 *
 * \retval		1 if updates needs to be replayed.
 * \retval		0 if updates does not need to be replayed.
 * \retval		negative errno if some other failures happen.
 */
static int update_recovery_is_committed(const struct lu_env *env,
					struct dt_device *dt,
					struct update_recovery_header *urh,
					struct dt_object *dt_obj,
					struct top_thandle *top_th,
					struct thandle *sub_th)
{
	struct seq_server_site	*seq_site = lu_site2seq(dt2lu_dev(dt)->ld_site);
	const struct lu_fid	*fid = lu_object_fid(&dt_obj->do_lu);
	struct sub_thandle	*st;
	struct update_recovery	*ur;
	__u32			mdt_index;
	__u32			master_index;
	ENTRY;

	if (sub_thandle_is_committed(top_th, sub_th))
		RETURN(0);

	/* Check whether update log in master MDT exists.
	 * If it does not get the update log from master MDT, it needs
	 * to compare the transno in the update log with the last
	 * committed transno on the master MDT
	 *
	 * 1. if transno <= last_committed transno, then it means the
	 * update log has been destroyed on master MDT and also all of
	 * updates of this transaction have been committed on all of MDTs.
	 *
	 * 2. otherwise the updates on master MDT has not been committed yet. */
	if (!top_th->tt_super.th_committed) {
		master_index = seq_site->ss_node_id;
		ur = update_recovery_lookup(urh, master_index);
		if (ur == NULL) {
			struct lu_target *lut;

			lut = dt2lu_dev(dt)->ld_site->ls_target;
			if (urh->urh_updates->ur_batchid != 0 &&
			    urh->urh_updates->ur_batchid <=
				lut->lut_obd->obd_last_committed) {
				CDEBUG(D_HA, "master sub_th %p under top_th %p"
				      " is committed ur_batchid "LPU64
				      " last_committed "LPU64"\n", sub_th,
				      top_th, urh->urh_updates->ur_batchid,
				      lut->lut_obd->obd_last_committed);
				top_th->tt_super.th_committed = 1;
			}
		}
	}

	/* Then check whether the update log exists, if it exists skip
	 * the update, otherwise redo the update */
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

	ur = update_recovery_lookup(urh, mdt_index);
	if (ur != NULL || top_th->tt_super.th_committed) {
		if (top_th->tt_child == sub_th) {
			top_th->tt_child->th_committed = 1;
			top_th->tt_child_committed = 1;
			if (ur != NULL)
				top_th->tt_master_cookie =
					ur->ur_llog_cookie;
		} else {
			st = lookup_sub_thandle(&top_th->tt_super,
						sub_th->th_dev);
			LASSERT(st != NULL);
			LASSERT(st->st_update != NULL);
			st->st_sub_th->th_committed = 1;
			st->st_committed = 1;
			if (ur != NULL)
				st->st_update->stu_cookie =
						ur->ur_llog_cookie;

		}
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

struct update_recovery_lock_handle {
	struct lustre_handle	urlh_lh;
	struct list_head	urlh_list;
	ldlm_mode_t		urlh_mode;
};

/**
 * Enqueue ldlm lock of the object.
 *
 * Extract lock mode and policy from the update operation, and
 * enqueue the lock of the object. And it also matain a lock
 * list, so it will check whether the object has been locked
 * before enqueue the lock, and cancel all of locks when all
 * of updates of the operation have been finshed.
 *
 * \param[in] env	execution environment
 * \param[in] urd	update recovery data which hold different recovery
 *                      parameters.
 * \param[in] dt_obj	dt_object to be locked
 * \param[in] op	update op to be replayed
 * \param[in|out] lock_list lock_list the new lock will be linked
 *
 * \retval		0 if lock succeeds.
 * \reval		negative errno if lock fails.
 */
static int update_recovery_lock_obj(const struct lu_env *env,
				    struct update_recovery_data *urd,
				    struct dt_object *dt_obj,
				    struct update_op *op,
				    struct list_head *lock_list)
{
	struct update_thread_info	*uti = update_env_info(env);
	struct ldlm_res_id		*res_id = &uti->uti_resid;
	ldlm_policy_data_t		*policy = &uti->uti_policy;
	struct ldlm_enqueue_info	*einfo = &uti->uti_einfo;
	ldlm_mode_t			mode;
	struct lustre_handle		lockh;
	struct update_recovery_lock_handle *urlh = NULL;
	__u64				  ibits = 0;
	int				  rc = 0;
	ENTRY;

	/* Only lock the object with Normal FID, root or IGIF */
	if (!fid_is_norm(lu_object_fid(&dt_obj->do_lu)) &&
	    !fid_is_root(lu_object_fid(&dt_obj->do_lu)) &&
	    !fid_is_igif(lu_object_fid(&dt_obj->do_lu)))
		RETURN(0);

	switch (op->uop_type) {
	case OUT_CREATE:
		mode = LCK_MINMODE;
		break;
	case OUT_DESTROY:
	case OUT_REF_ADD:
	case OUT_REF_DEL:
	case OUT_ATTR_SET:
		/* In case there will be permission change, so hold both
		 *UPDATE and LOOKUP lock */
		mode = LCK_EX;
		ibits = MDS_INODELOCK_UPDATE | MDS_INODELOCK_LOOKUP;
		break;
	case OUT_INDEX_INSERT:
	case OUT_INDEX_DELETE:
	case OUT_XATTR_SET:
	case OUT_WRITE:
	case OUT_XATTR_DEL:
		mode = LCK_EX;
		ibits = MDS_INODELOCK_UPDATE;
		break;
	default:
		CERROR("Unknown update type %u\n", (__u32)op->uop_type);
		rc = -EINVAL;
		break;
	}

	/* No need lock for this update */
	if (rc != 0 || mode == LCK_MINMODE)
		RETURN(rc);

	fid_build_reg_res_name(lu_object_fid(&dt_obj->do_lu), res_id);

	/* Check whether the lock has been gotten, Only single thread will
	 * access the list, so no need lock */
	list_for_each_entry(urlh, lock_list, urlh_list) {
		struct ldlm_lock *lock = ldlm_handle2lock(&urlh->urlh_lh);

		if (memcmp(&lock->l_resource->lr_name, res_id,
			   sizeof(*res_id)) != 0) {
			LDLM_LOCK_PUT(lock);
			continue;
		}

		ibits &= ~lock->l_policy_data.l_inodebits.bits;
		if (ibits == 0) {
			LDLM_LOCK_PUT(lock);
			break;
		}
		LDLM_LOCK_PUT(lock);
	}
	/* Do not need do anything if the lock has been hold */
	if (ibits == 0)
		RETURN(0);

	OBD_ALLOC_PTR(urlh);
	if (urlh == NULL)
		RETURN(-ENOMEM);

	policy->l_inodebits.bits = ibits;

	if (dt_object_remote(dt_obj)) {
		einfo->ei_res_id = res_id;
		einfo->ei_type = LDLM_IBITS;
		einfo->ei_mode = mode;
		einfo->ei_cb_bl = urd->urd_remote_cb_bl;
		einfo->ei_cb_cp = urd->urd_completion_cb;
		rc = dt_object_lock(env, dt_obj, &lockh, einfo,
				    policy);
		if (rc != 0)
			GOTO(out, rc);
	} else {
		__u64 flags = LDLM_FL_LOCAL_ONLY | LDLM_FL_ATOMIC_CB;

		rc = ldlm_cli_enqueue_local(urd->urd_ns, res_id, LDLM_IBITS,
					    policy, mode, &flags,
					    urd->urd_local_cb_bl,
					    urd->urd_completion_cb, NULL,
					    NULL, 0, LVB_T_NONE, NULL, &lockh);
		if (rc != ELDLM_OK)
			GOTO(out, rc = -EIO);
	}

	urlh->urlh_lh = lockh;
	urlh->urlh_mode = mode;
	INIT_LIST_HEAD(&urlh->urlh_list);
	list_add(&urlh->urlh_list, lock_list);
out:
	if (rc != 0 && urlh != NULL)
		OBD_FREE_PTR(urlh);

	RETURN(rc);
}

static void update_recovery_unlock_list(const struct lu_env *env,
					struct update_recovery_data *urd,
					struct list_head *lock_list)
{
	struct update_recovery_lock_handle *urlh;
	struct update_recovery_lock_handle *tmp;

	list_for_each_entry_safe(urlh, tmp, lock_list, urlh_list) {
		ldlm_lock_decref_and_cancel(&urlh->urlh_lh, urlh->urlh_mode);
		list_del(&urlh->urlh_list);
		OBD_FREE_PTR(urlh);
	}

	return;
}

#define update_recovery_declare(env, dt_obj, name, op, params, th)	\
	update_recovery_##name(env, dt_obj, op, params, sub_th, true)

#define update_recovery_execute(env, dt_obj, name, op, params, th)	\
	update_recovery_##name(env, dt_obj, op, params, th, false)

/**
 * Declare updates recovery
 *
 * Declare updates recovery for the following replay. Note: it declares
 * the updates on all involve MDTs no matter if it is it needs to replay
 * or not, because even the update have been committed on the MDT, it still
 * needs to call declare API to create sub thandle (see struct sub_thandle)
 * attached it to the lod_transaction and track the whole operation as the
 * normal distributed operation.
 *
 * It will also get the ldlm lock of the object to be replayed, which will
 * be released when the replay is finished.
 *
 * \param[in] env	execution environment
 * \param[in] urd	update recovery data which hold different recovery
 *                      parameters.
 * \param[in] urh	updates header to be replayed.
 * \param[in] lock_list list of the locks gotten for the this replay.
 * \param[in] th	transaction handle.
 *
 * \retval		0 if declare succeeds.
 * \retval		negative errno if declare fails.
 */
static int update_recovery_handle_declare(const struct lu_env *env,
					  struct update_recovery_data *urd,
					  struct update_recovery_header *urh,
					  struct list_head *lock_list,
					  struct thandle *th)
{
	struct update_records	*records = urh->urh_updates;
	struct update_ops	*ops = &records->ur_ops;
	struct update_params	*params = update_records_get_params(records);
	struct update_op	*op;
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	unsigned int		i;
	int			rc = 0;
	ENTRY;

	op = &ops->uops_op[0];
	/* These records have been swabbed in llog_cat_process() */
	for (i = 0; i < ops->uops_count; i++, op = update_op_next_op(op)) {
		struct lu_fid		*fid = &op->uop_fid;
		struct dt_object	*dt_obj;
		struct dt_object	*sub_dt_obj;
		struct thandle		*sub_th;

		dt_obj = dt_locate(env, urd->urd_dt, fid);
		if (IS_ERR(dt_obj))
			GOTO(out, rc = PTR_ERR(dt_obj));

		sub_dt_obj = dt_object_child(dt_obj);

		/* Find or create sub thandle, Note: even the update may
		 * already been committed, we still need to create the sub
		 * thandle for the purpose of tracking the whole operation
		 * commit status */
		sub_th = get_sub_thandle(env, &top_th->tt_super, sub_dt_obj);
		if (IS_ERR(sub_th)) {
			lu_object_put(env, &dt_obj->do_lu);
			GOTO(out, rc = PTR_ERR(dt_obj));
		}

		rc = update_recovery_is_committed(env, urd->urd_dt, urh, dt_obj,
						  top_th, sub_th);
		if (rc <= 0) {
			lu_object_put(env, &dt_obj->do_lu);
			if (rc < 0)
				GOTO(out, rc);
			else
				/* If this is committed, goto next */
				continue;
		}

		rc = update_recovery_lock_obj(env, urd, dt_obj, op, lock_list);
		if (rc != 0) {
			lu_object_put(env, &dt_obj->do_lu);
			GOTO(out, rc);
		}

		switch (op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_declare(env, sub_dt_obj, create,
						     op, params, sub_th);
			break;
		case OUT_DESTROY:
			rc = update_recovery_declare(env, sub_dt_obj, destroy,
						     op, params, sub_th);
			break;
		case OUT_REF_ADD:
			rc = update_recovery_declare(env, sub_dt_obj, ref_add,
						     op, params, sub_th);
			break;
		case OUT_REF_DEL:
			rc = update_recovery_declare(env, sub_dt_obj, ref_del,
						     op, params, sub_th);
			break;
		case OUT_ATTR_SET:
			rc = update_recovery_declare(env, sub_dt_obj, attr_set,
						     op, params, sub_th);
			break;
		case OUT_XATTR_SET:
			rc = update_recovery_declare(env, sub_dt_obj, xattr_set,
						     op, params, sub_th);
			break;
		case OUT_INDEX_INSERT:
			rc = update_recovery_declare(env, sub_dt_obj,
						     index_insert, op, params,
						     sub_th);
			break;
		case OUT_INDEX_DELETE:
			rc = update_recovery_declare(env, sub_dt_obj,
						     index_delete, op, params,
						     sub_th);
			break;
		case OUT_WRITE:
			rc = update_recovery_declare(env, sub_dt_obj, write,
						     op, params, sub_th);
			break;
		case OUT_XATTR_DEL:
			rc = update_recovery_declare(env, sub_dt_obj, xattr_del,
						     op, params, sub_th);
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}
		lu_object_put(env, &dt_obj->do_lu);
		if (rc < 0) {
			th->th_result = rc;
			break;
		}
	}
out:
	RETURN(rc);
}

/**
 * redo updates on MDT if needed.
 *
 * During DNE recovery, the recovery thread (target_recovery_thread) will call
 * this function to replay updates on all MDTs. It only replay updates on the
 * MDT where the update log is missing. The whole recovery process will be based
 * LOD layer, and similar as normal transaction (see lod_trans_start/stop).
 * But there are a few notes:
 *
 * 1. If the update already exists on the MDT, then it does not need replay the
 * updates on that MDT, and only mark the sub transaction has been committed
 * there.
 *
 * 2. Because updates are already there, it does not needs to packed updates as
 * normal distribute transaction, and only write the update log on those MDTs
 * where updates needs to be replayed. So during replay it will call the bottom
 * layer (OSD/OSP) directly, instead of LOD.
 *
 * 3. It needs to lock the object by itself, so it will lock the object during
 * declare phase and unlock the objects after all updates were replayed.
 *
 * \param[in] env	execution environment
 * \param[in] urd	update recovery data, which holds the the all parameters
 *                      needed by recovery process.
 * \param[in] urh	updates header to be replayed.
 *
 * \retval		0 if replay succeeds.
 * \retval		negative errno if replay failes.
 */
int update_recovery_handle(const struct lu_env *env,
			   struct update_recovery_data *urd,
			   struct update_recovery_header *urh)
{
	struct update_records	*records = urh->urh_updates;
	struct update_ops	*ops = &records->ur_ops;
	struct update_params	*params = update_records_get_params(records);
	struct update_op	*op;
	struct list_head	lock_list;
	struct thandle		*th = NULL;
	struct top_thandle	*top_th;
	struct thandle_update_records *tur = NULL;
	unsigned int		i;
	int			rc = 0;
	ENTRY;

	update_records_dump(urh->urh_updates, D_HA);

	th = dt_trans_create(env, urd->urd_dt);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	/* The lock_list will hold all the locks for the transaction. */
	INIT_LIST_HEAD(&lock_list);
	rc = update_recovery_handle_declare(env, urd, urh, &lock_list, th);
	if (rc < 0)
		GOTO(stop_trans, rc);

	/* Set update records for this update replay */
	top_th = container_of(th, struct top_thandle, tt_super);
	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	tur = &update_env_info(env)->uti_tur;
	tur->tur_update_records = records;
	tur->tur_update_records_size = update_records_size(records);
	tur->tur_update_params = params;
	top_th->tt_update_records = tur;

	rc = dt_trans_start(env, urd->urd_dt, th);
	if (rc < 0)
		GOTO(stop_trans, rc);

	op = &ops->uops_op[0];
	for (i = 0; i < ops->uops_count; i++, op = update_op_next_op(op)) {
		struct lu_fid	 *fid = &op->uop_fid;
		struct dt_object *dt_obj;
		struct dt_object *sub_dt_obj;
		struct thandle	 *sub_th;

		dt_obj = dt_locate(env, urd->urd_dt, fid);
		if (IS_ERR(dt_obj))
			GOTO(stop_trans, rc = PTR_ERR(dt_obj));

		/* Since we do not need pack the updates, so replay
		 * the update on the bottom layer(OSP/OSD) directly */
		sub_dt_obj = dt_object_child(dt_obj);
		sub_th = get_sub_thandle(env, th, sub_dt_obj);

		if (sub_thandle_is_committed(top_th, sub_th)) {
			CDEBUG(D_HA, "Skip %s "DFID" already committed on %s\n",
			       update_op_str(op->uop_type), PFID(fid),
			       sub_th->th_dev->dd_lu_dev.ld_obd->obd_name);
			lu_object_put(env, &dt_obj->do_lu);
			continue;
		}

		CDEBUG(D_HA, "Replay update %s "DFID" on %s\n",
		       update_op_str(op->uop_type), PFID(fid),
		       sub_th->th_dev->dd_lu_dev.ld_obd->obd_name);

		switch (op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_execute(env, sub_dt_obj, create,
						     op, params, sub_th);
			break;
		case OUT_DESTROY:
			rc = update_recovery_execute(env, sub_dt_obj, destroy,
						     op, params, sub_th);
			break;
		case OUT_REF_ADD:
			rc = update_recovery_execute(env, sub_dt_obj, ref_add,
						     op, params, sub_th);
			break;
		case OUT_REF_DEL:
			rc = update_recovery_execute(env, sub_dt_obj, ref_del,
						     op, params, sub_th);
			break;
		case OUT_ATTR_SET:
			rc = update_recovery_execute(env, sub_dt_obj, attr_set,
						     op, params, sub_th);
			break;
		case OUT_XATTR_SET:
			rc = update_recovery_execute(env, sub_dt_obj, xattr_set,
						     op, params, sub_th);
			break;
		case OUT_INDEX_INSERT:
			rc = update_recovery_execute(env, sub_dt_obj,
						     index_insert, op, params,
						     sub_th);
			break;
		case OUT_INDEX_DELETE:
			rc = update_recovery_execute(env, sub_dt_obj,
						     index_delete, op, params,
						     sub_th);
			break;
		case OUT_WRITE:
			rc = update_recovery_execute(env, sub_dt_obj,
						     write, op, params, sub_th);
			break;
		case OUT_XATTR_DEL:
			rc = update_recovery_execute(env, sub_dt_obj,
						     xattr_del, op, params,
						     sub_th);
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}
		lu_object_put(env, &dt_obj->do_lu);
		if (rc < 0) {
			th->th_result = rc;
			break;
		}
	}

stop_trans:
	update_recovery_unlock_list(env, urd, &lock_list);
	rc = dt_trans_stop(env, urd->urd_dt, th);
	if (tur != NULL) {
		/* These buffers will be freed in llog process thread */
		tur->tur_update_records = NULL;
		tur->tur_update_params = NULL;
	}
	RETURN(rc);
}
EXPORT_SYMBOL(update_recovery_handle);
