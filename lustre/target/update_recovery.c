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
 * Author: Di Wang <di.wang@intel.com>
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <lu_target.h>
#include <md_object.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>
#include "tgt_internal.h"

static struct update_recovery_header*
update_recovery_header_lookup(struct update_recovery_data *urd, __u64 cookie)
{
	struct update_recovery_header	*tmp;
	struct update_recovery_header	*urh = NULL;

	list_for_each_entry(tmp, &urd->urd_list, urh_list) {
		if (tmp->urh_cookie == cookie) {
			urh = tmp;
			break;
		}
	}
	return urh;
}

static void
update_recovery_header_insert(struct update_recovery_data *urd,
			      struct update_recovery_header *new)
{
	struct update_recovery_header *urh;
	struct update_recovery_header *tmp;

	spin_lock(&urd->urd_list_lock);
	if (new->urh_transno == 0) {
		list_add(&new->urh_list, &urd->urd_list);
	} else {
		bool inserted = false;

		list_for_each_entry_safe(urh, tmp, &urd->urd_list, urh_list) {
			if (new->urh_transno > urh->urh_transno) {
				list_add(&new->urh_list, &urh->urh_list);
				inserted = true;
			}
		}
		if (!inserted)
			list_add_tail(&new->urh_list, &urd->urd_list);
	}
	spin_unlock(&urd->urd_list_lock);
}

static struct update_recovery_header*
update_recovery_header_create(struct update_recovery_data *urd,
			      struct update_records *record)
{
	struct update_recovery_header	*new;
	struct update_recovery_header	*urh = NULL;
	ENTRY;

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

	spin_lock(&urd->urd_list_lock);
	urh = update_recovery_header_lookup(urd, record->ur_cookie);
	if (urh == NULL) {
		list_add(&new->urh_list, &urd->urd_list);
		spin_unlock(&urd->urd_list_lock);
		urh = new;
	} else {
		spin_unlock(&urd->urd_list_lock);
		if (new->urh_updates != NULL)
			OBD_FREE(new->urh_updates, new->urh_updates_size);
		OBD_FREE_PTR(new);
	}

	RETURN(urh);
}

static struct update_recovery*
update_recovery_lookup(struct update_recovery_header *urh,
		       __u32 mdt_index)
{
	struct update_recovery *ru = NULL;
	struct update_recovery *tmp;

	list_for_each_entry(tmp, &urh->urh_list, ur_list) {
		if (tmp->ur_mdt_index == mdt_index) {
			ru = tmp;
			break;
		}
	}
	return ru;
}

static int
update_recovery_create_insert(struct update_recovery_header *urh,
			      struct update_records *records,
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

	spin_lock(&urh->urh_update_list_lock);
	ur = update_recovery_lookup(urh, mdt_index);
	if (ur == NULL)
		list_add(&new->ur_list, &urh->urh_update_list);
	spin_unlock(&urh->urh_update_list_lock);

	RETURN(0);
}

int insert_update_records_to_recovery_list(struct update_recovery_data *urd,
					   struct update_records *record,
					   __u32 index)
{
	struct update_recovery_header *urh;
	int rc;
	ENTRY;

	spin_lock(&urd->urd_list_lock);
	urh = update_recovery_header_lookup(urd, record->ur_cookie);
	if (urh->urh_transno == 0 && record->ur_batchid != 0) {
		list_del_init(&urh->urh_list);
		update_recovery_header_insert(urd, urh);
	}
	spin_unlock(&urd->urd_list_lock);
	if (urh == NULL) {
		urh = update_recovery_header_create(urd, record);
		if (IS_ERR(urh))
			RETURN(PTR_ERR(urh));
	}

	rc = update_recovery_create_insert(urh, record, index);

	return rc;
}
EXPORT_SYMBOL(insert_update_records_to_recovery_list);

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

void update_recovery_header_destory(struct update_recovery_header *urh)
{
	struct update_recovery	*ur;
	struct update_recovery	*tmp;

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

void destroy_updates_in_recovery_list(struct update_recovery_data *urd)
{
	struct update_recovery_header *urh;
	struct update_recovery_header *tmp;

	spin_lock(&urd->urd_list_lock);
	list_for_each_entry_safe(urh, tmp, &urd->urd_list, urh_list) {
		list_del(&urh->urh_list);
		update_recovery_header_destory(urh);
	}
	spin_unlock(&urd->urd_list_lock);
}
EXPORT_SYMBOL(destroy_updates_in_recovery_list);

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

__u64 update_recovery_get_next_transno(struct update_recovery_data *urd)
{
	struct update_recovery_header	*urh = NULL;
	__u64				transno = 0;

	spin_lock(&urd->urd_list_lock);
	if (!list_empty(&urd->urd_list)) {
		urh = list_entry(urd->urd_list.next,
				 struct update_recovery_header, urh_list);
		transno = urh->urh_cookie;
	}
	spin_unlock(&urd->urd_list_lock);

	return transno;
}
EXPORT_SYMBOL(update_recovery_get_next_transno);

static int update_recovery_is_needed(const struct lu_env *env,
				     struct dt_device *dt,
				     struct update_recovery_header *urh,
				     const struct lu_fid *fid)
{
	struct seq_server_site	*seq_site;
	__u32			mdt_index;
	struct update_recovery	*ur;

	seq_site = lu_site2seq(dt2lu_dev(dt)->ld_site);
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
		rc = fld_server_lookup(env, fld, fid_seq(fid),
				       &range);
		if (rc < 0)
			RETURN(rc);

		mdt_index = range.lsr_index;
	}

	ur = update_recovery_lookup(urh, mdt_index);
	if (ur == NULL)
		return 1;

	return 0;
}

static void dof_le_to_cpu(struct dt_object_format *ddof,
			  struct dt_object_format *sdof)
{
	return;
}

static int update_recovery_create_internal(const struct lu_env *env,
					  struct update_recovery_data *urd,
					  const struct update_op *op,
					  const struct update_params *params,
					  struct thandle *th,
					  bool declare)
{
	struct dt_object	*dt_obj;
	struct lu_attr		*attr;
	struct dt_object_format	*dof;
	unsigned int		index = 0;
	int rc;
	ENTRY;

	dt_obj = dt_locate(env, urd->urd_dt, &op->uop_fid);
	if (IS_ERR(dt_obj))
		RETURN(PTR_ERR(dt_obj));

	if (dt_object_exists(dt_obj))
		RETURN(-EEXIST);

	attr = update_params_get_param_buf_by_size(params,
					op->uop_params_off[index++],
					sizeof(*attr));
	if (IS_ERR(attr))
		RETURN(PTR_ERR(attr));
	lu_attr_le_to_cpu(attr, attr);

	if (op->uop_params_count == 3)
		index ++;

	dof = update_params_get_param_buf_by_size(params,
				      op->uop_params_off[index],
				      sizeof(*dof));
	if (IS_ERR(dof))
		RETURN(PTR_ERR(dof));
	dof_le_to_cpu(dof, dof);

	if (declare)
		rc = dt_declare_create(env, dt_obj, attr, NULL, dof, th);
	else
		rc = dt_create(env, dt_obj, attr, NULL, dof, th);

	RETURN(rc);
}

static int update_recovery_create(const struct lu_env *env,
				  struct update_recovery_data *urd,
				  const struct update_op *op,
				  const struct update_params *params,
				  struct thandle *th)
{
	return update_recovery_create_internal(env, urd, op, params, th, false);
}

static int update_recovery_declare_create(const struct lu_env *env,
					  struct update_recovery_data *urd,
					  const struct update_op *op,
					  const struct update_params *params,
					  struct thandle *th)
{
	return update_recovery_create_internal(env, urd, op, params, th, true);
}

static int update_recovery_handle_declare(const struct lu_env *env,
					  struct update_recovery_data *urd,
					  struct update_recovery_header *urh,
					  struct thandle *th)
{
	struct update_records	*records = urh->urh_updates;
	struct update_ops	*ops = &records->ur_ops;
	unsigned int		i;
	int			rc = 0;

	/* These records have been swabbed in llog_cat_process() */
	for (i = 0; i < ops->uops_count; i++) {
		struct update_op *op = &ops->uops_op[i];
		struct lu_fid	 *fid = &op->uop_fid;

		/* Check whether it needs to recovery */
		rc = update_recovery_is_needed(env, urd->urd_dt, urh, fid);
		if (rc < 0)
			GOTO(out, rc);

		/* Do not need recovery */
		if (rc == 0)
			continue;

		switch(op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_declare_create(env, urd, op,
							    &records->ur_params,
							    th);
			break;
		case OUT_DESTROY:
		case OUT_REF_ADD:
		case OUT_REF_DEL:
		case OUT_ATTR_SET:
		case OUT_XATTR_SET:
		case OUT_INDEX_INSERT:
		case OUT_INDEX_DELETE:
		case OUT_WRITE:
		case OUT_XATTR_DEL:
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}
		/* Cancel the replay record */
	}
out:
	return rc;
}

int update_recovery_handle(const struct lu_env *env,
			   struct update_recovery_data *urd,
			   struct update_recovery_header *urh)
{
	struct update_records	*records = urh->urh_updates;
	struct update_ops	*ops = &records->ur_ops;
	struct thandle		*th = NULL;
	unsigned int		i;
	int			rc = 0;

	th = dt_trans_create(env, urd->urd_dt);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	rc = update_recovery_handle_declare(env, urd, urh, th);
	if (rc != 0)
		GOTO(stop_trans, rc);

	for (i = 0; i < ops->uops_count; i++) {
		struct update_op *op = &ops->uops_op[i];
		struct lu_fid	 *fid = &op->uop_fid;

		/* Check whether it needs to recovery */
		rc = update_recovery_is_needed(env, urd->urd_dt, urh, fid);
		if (rc < 0)
			GOTO(stop_trans, rc);

		/* Do not need recovery */
		if (rc == 0)
			continue;

		switch(op->uop_type) {
		case OUT_CREATE:
			rc = update_recovery_create(env, urd, op,
						    &records->ur_params, th);
			break;
		case OUT_DESTROY:
		case OUT_REF_ADD:
		case OUT_REF_DEL:
		case OUT_ATTR_SET:
		case OUT_XATTR_SET:
		case OUT_INDEX_INSERT:
		case OUT_INDEX_DELETE:
		case OUT_WRITE:
		case OUT_XATTR_DEL:
			break;
		default:
			CERROR("Unknown update type %u\n", (__u32)op->uop_type);
			rc = -EINVAL;
			break;
		}

		/* Cancel the replay record */
	}

stop_trans:
	dt_trans_stop(env, urd->urd_dt, th);
	RETURN(rc);
}
EXPORT_SYMBOL(update_recovery_handle);
