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
 * lustre/target/update_records.c
 *
 * This file implement the methods to pack updates as update records, which
 * will be written to the disk as llog record, and might be used during
 * recovery.
 *
 * For cross-MDT operation, all of updates of the operation needs to be
 * recorded in the disk, then during recovery phase, the recovery thread
 * will retrieve and redo these updates if it needed.
 *
 * See comments above struct update_records for the format of update_records.
 *
 * Author: Di Wang <di.wang@intel.com>
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <lu_target.h>
#include <lustre_update.h>
#include <obd.h>
#include <obd_class.h>
#include "tgt_internal.h"

/**
 * Dump update record.
 *
 * Dump all of updates in the update_records, mostly for debugging purpose.
 *
 * \param[in] records	update records to be dumpped
 * \param[in] mask	debug level mask
 *
 */
void update_records_dump(struct update_records *records, unsigned int mask)
{
	struct update_ops	*ops;
	struct update_op	*op = NULL;
	struct update_params	*params;
	unsigned int		i;

	ops = &records->ur_ops;
	params = update_records_get_params(records);

	CDEBUG(mask, "ops %d params %d\n", ops->uops_count,
	       params->up_params_count);

	if (ops->uops_count == 0)
		return;

	op = &ops->uops_op[0];
	for (i = 1; i < ops->uops_count; i++) {
		unsigned int j;

		CDEBUG(mask, "%d: "DFID" %s: \n", i, PFID(&op->uop_fid),
		       update_op_str(op->uop_type));

		for (j = 0;  j < op->uop_params_count; j++) {
			CDEBUG(mask, "param %u offset %d\n",
			       j, (int)op->uop_params_off[j]);
		}

		op = update_op_next_op(op);
	}

	return;
}
EXPORT_SYMBOL(update_records_dump);

/**
 * Pack parameters to update records
 *
 * Find and insert parameter to update records, if the parameter
 * already exists in \a params, then just return the offset of this
 * parameter, otherwise insert the parameter and return its offset
 *
 * \param[in] params	update params in which to insert parameter
 * \param[in] new_param parameters to be inserted.
 * \param[in] new_param_size the size of \a new_param
 *
 * \retval		index inside \a params if parameter insertion
 *                      succeeds.
 * \retval		negative errno if the parameter packing fails.
 */
static int update_records_param_pack(struct update_params *params,
				     const void *new_param,
				     size_t new_param_size)
{
	struct object_update_param	*param;
	int				params_count = params->up_params_count;
	unsigned int			i;

	for (i = 0; i < params_count; i++) {
		struct object_update_param *param;

		param = update_params_get_param(params, i);
		if (param->oup_len == new_param_size &&
		   memcmp(param->oup_buf, new_param, new_param_size) == 0)
			break;
	}

	/* Found the parameter and return its index */
	if (i < params_count)
		return i;

	param = (struct object_update_param *)((char *)params +
				update_params_size(params));

	param->oup_len = new_param_size;
	memcpy(param->oup_buf, new_param, new_param_size);

	params->up_params_count++;

	return params_count;
}

/**
 * Pack update to update records
 *
 * Pack the update and its parameters to the update records. First it will
 * insert parameters, get the offset of these parameter, then fill the
 * update with these offset. If insertion exceed the maximum size of
 * current update records, it will return -E2BIG here, and the caller might
 * extend the update_record size \see lod_updates_pack.
 *
 * \param[in] env	execution environment
 * \param[in] fid	FID of the update.
 * \param[in] op_type	operation type of the update
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update.
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter.
 * \param[in] params_count the count of the parameters for the update
 * \param[in] params_bufs buffers of parameters
 * \param[in] params_size sizes of parameters
 *
 * \retval		0 if packing succeeds
 * \retval		negative errno if packing fails
 */
int update_records_update_pack(const struct lu_env *env,
			       const struct lu_fid *fid, int op_type,
			       struct update_ops *ops,
			       size_t *max_op_size,
			       struct update_params *params,
			       size_t *max_param_size, int params_count,
			       const void **params_bufs,
			       size_t *params_size)
{
	struct update_op	*op;
	size_t			total_params_size = 0;
	int			index;
	unsigned int		i;

	/* Check whether the packing exceeding the maxima update length */
	if (unlikely(*max_op_size < sizeof(struct update_op))) {
		CDEBUG(D_INFO, "max_op_size %d update_op %d\n",
		      (int)*max_op_size, (int)sizeof(struct update_op));
		*max_op_size = sizeof(struct update_op);
		return -E2BIG;
	}

	for (i = 0; i < params_count; i++)
		total_params_size +=
			cfs_size_round(sizeof(struct object_update_param) +
				       params_size[i]);

	/* Check whether the packing exceeding the maxima parameters length */
	if (unlikely(*max_param_size < total_params_size)) {
		CDEBUG(D_INFO, "max_param_size %d params size %d\n",
		       (int)*max_param_size, (int)total_params_size);
		*max_param_size = total_params_size;
		return -E2BIG;
	}

	op = update_ops_get_op(ops, ops->uops_count);
	op->uop_fid = *fid;
	op->uop_type = (__u16)op_type;
	op->uop_params_count = params_count;
	for (i = 0; i < params_count; i++) {
		LASSERT(params_bufs[i] != NULL);
		index = update_records_param_pack(params, params_bufs[i],
						  params_size[i]);
		if (index < 0)
			return index;

		CDEBUG(D_INFO, "pack %s params %d:%u\n",
		       update_op_str((__u16)op_type), index, i);

		op->uop_params_off[i] = index;
	}

	ops->uops_size += update_op_size(op);
	ops->uops_count++;

	return 0;
}

/**
 * Pack create update
 *
 * Pack create update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to be created
 * \param[in] attr	attribute of the object to be created
 * \param[in] hint	creation hint
 * \param[in] dof	creation format information
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_create_pack(const struct lu_env *env,
			       struct update_ops *ops,
			       size_t *max_ops_size,
			       struct update_params *params,
			       size_t *max_param_size,
			       const struct lu_fid *fid,
			       const struct lu_attr *attr,
			       const struct dt_allocation_hint *hint,
			       struct dt_object_format *dof)
{
	size_t			sizes[2];
	const void		*bufs[2];
	int			buf_count = 0;
	struct lu_fid		*fid1 = NULL;
	struct lu_fid		tmp_fid;
	int			rc;

	if (attr != NULL) {
		bufs[0] = attr;
		sizes[0] = sizeof(*attr);
		buf_count++;
	}

	if (hint != NULL && hint->dah_parent) {
		fid1 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		fid_cpu_to_le(&tmp_fid, fid1);
		bufs[1] = &tmp_fid;
		sizes[1] = sizeof(tmp_fid);
		buf_count++;
	}

	rc = update_records_update_pack(env, fid, OUT_CREATE, ops, max_ops_size,
					params, max_param_size, buf_count, bufs,
					sizes);
	return rc;
}
EXPORT_SYMBOL(update_records_create_pack);

/**
 * Pack attr set update
 *
 * Pack attr_set update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to set attr
 * \param[in] attr	attribute of attr set
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_attr_set_pack(const struct lu_env *env,
				 struct update_ops *ops, size_t *max_ops_size,
				 struct update_params *params,
				 size_t *max_param_size,
				 const struct lu_fid *fid,
				 const struct lu_attr *attr)
{
	size_t size = sizeof(*attr);

	return update_records_update_pack(env, fid, OUT_ATTR_SET, ops,
					  max_ops_size, params, max_param_size,
					  1, (const void **)&attr, &size);
}
EXPORT_SYMBOL(update_records_attr_set_pack);

/**
 * Pack ref add update
 *
 * Pack ref add update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to add reference
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_ref_add_pack(const struct lu_env *env,
				struct update_ops *ops, size_t *max_ops_size,
				struct update_params *params,
				size_t *max_param_size,
				const struct lu_fid *fid)
{
	return update_records_update_pack(env, fid, OUT_REF_ADD, ops,
					  max_ops_size, params, max_param_size,
					  0, NULL, NULL);
}
EXPORT_SYMBOL(update_records_ref_add_pack);

/**
 * Pack ref del update
 *
 * Pack ref del update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to delete reference
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_ref_del_pack(const struct lu_env *env,
				struct update_ops *ops, size_t *max_ops_size,
				struct update_params *params,
				size_t *max_param_size,
				const struct lu_fid *fid)
{
	return update_records_update_pack(env, fid, OUT_REF_DEL, ops,
					  max_ops_size, params, max_param_size,
					  0, NULL, NULL);
}
EXPORT_SYMBOL(update_records_ref_del_pack);

/**
 * Pack object destroy update
 *
 * Pack object destroy update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to delete reference
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_object_destroy_pack(const struct lu_env *env,
				       struct update_ops *ops,
				       size_t *max_ops_size,
				       struct update_params *params,
				       size_t *max_param_size,
				       const struct lu_fid *fid)
{
	return update_records_update_pack(env, fid, OUT_DESTROY, ops,
					  max_ops_size, params, max_param_size,
					  0, NULL, NULL);
}
EXPORT_SYMBOL(update_records_object_destroy_pack);

/**
 * Pack index insert update
 *
 * Pack index insert update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to insert index
 * \param[in] rec	record of insertion
 * \param[in] key	key of insertion
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_index_insert_pack(const struct lu_env *env,
				     struct update_ops *ops,
				     size_t *max_ops_size,
				     struct update_params *params,
				     size_t *max_param_size,
				     const struct lu_fid *fid,
				     const struct dt_rec *rec,
				     const struct dt_key *key)
{
	struct dt_insert_rec	   *rec1 = (struct dt_insert_rec *)rec;
	struct lu_fid		   rec_fid;
	__u32			   type = cpu_to_le32(rec1->rec_type);
	size_t			   sizes[3] = { strlen((char *)key) + 1,
						sizeof(rec_fid),
						sizeof(type) };
	const void		   *bufs[3] = { key,
						&rec_fid,
						&type };

	fid_cpu_to_le(&rec_fid, rec1->rec_fid);

	return update_records_update_pack(env, fid, OUT_INDEX_INSERT, ops,
					  max_ops_size, params, max_param_size,
					  3, bufs, sizes);
}
EXPORT_SYMBOL(update_records_index_insert_pack);

/**
 * Pack index delete update
 *
 * Pack index delete update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to delete index
 * \param[in] key	key of deletion
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_index_delete_pack(const struct lu_env *env,
				     struct update_ops *ops,
				     size_t *max_ops_size,
				     struct update_params *params,
				     size_t *max_param_size,
				     const struct lu_fid *fid,
				     const struct dt_key *key)
{
	size_t size = strlen((char *)key) + 1;

	return update_records_update_pack(env, fid, OUT_INDEX_DELETE, ops,
					  max_ops_size, params, max_param_size,
					  1, (const void **)&key, &size);
}
EXPORT_SYMBOL(update_records_index_delete_pack);

/**
 * Pack xattr set update
 *
 * Pack xattr set update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to set xattr
 * \param[in] buf	xattr to be set
 * \param[in] name	name of the xattr
 * \param[in] flag	flag for setting xattr
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_xattr_set_pack(const struct lu_env *env,
				  struct update_ops *ops, size_t *max_ops_size,
				  struct update_params *params,
				  size_t *max_param_size,
				  const struct lu_fid *fid,
				  const struct lu_buf *buf, const char *name,
				  int flag)
{
	size_t	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(int)};
	const void *bufs[3] = {name, buf->lb_buf, &flag};

	flag = cpu_to_le32(flag);

	return update_records_update_pack(env, fid, OUT_XATTR_SET, ops,
					  max_ops_size, params, max_param_size,
					  3, bufs, sizes);
}
EXPORT_SYMBOL(update_records_xattr_set_pack);

/**
 * Pack xattr delete update
 *
 * Pack xattr delete update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to delete xattr
 * \param[in] name	name of the xattr
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_xattr_del_pack(const struct lu_env *env,
				  struct update_ops *ops, size_t *max_ops_size,
				  struct update_params *params,
				  size_t *max_param_size,
				  const struct lu_fid *fid,
				  const char *name)
{
	size_t	size = strlen(name) + 1;

	return update_records_update_pack(env, fid, OUT_XATTR_DEL, ops,
					  max_ops_size, params, max_param_size,
					  1, (const void **)&name, &size);
}
EXPORT_SYMBOL(update_records_xattr_del_pack);

/**
 * Pack write update
 *
 * Pack write update into update records.
 *
 * \param[in] env	execution environment
 * \param[in] ops	ur_ops in update records
 * \param[in|out] max_op_size maximum size of the update
 * \param[in] params	ur_params in update records
 * \param[in|out] max_param_size maximum size of the parameter
 * \param[in] fid	FID of the object to write into
 * \param[in] buf	buffer to write which includes an embedded size field
 * \param[in] pos	offet in the object to start writing at
 *
 * \retval		0 if packing succeeds.
 * \retval		negative errno if packing fails.
 */
int update_records_write_pack(const struct lu_env *env,
			      struct update_ops *ops, size_t *max_ops_size,
			      struct update_params *params,
			      size_t *max_param_size,
			      const struct lu_fid *fid,
			      const struct lu_buf *buf,
			      loff_t pos)
{
	size_t		sizes[2] = {buf->lb_len, sizeof(pos)};
	const void	*bufs[2] = {buf->lb_buf, &pos};

	pos = cpu_to_le64(pos);

	return update_records_update_pack(env, fid, OUT_XATTR_DEL, ops,
					  max_ops_size, params, max_param_size,
					  2, bufs, sizes);
}
EXPORT_SYMBOL(update_records_write_pack);
