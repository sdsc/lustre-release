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
 * http://www.gnu.org/licenses/gpl-2.0.htm
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2013, 2014, Intel Corporation.
 */
/*
 * lustre/include/lustre_update.h
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#ifndef _LUSTRE_UPDATE_H
#define _LUSTRE_UPDATE_H
#include <lustre_net.h>
#include <dt_object.h>

#define OUT_UPDATE_INIT_BUFFER_SIZE	4096
/* 16KB, the current biggest size is llog header(8KB) */
#define OUT_UPDATE_REPLY_SIZE		16384

struct dt_key;
struct dt_rec;
struct object_update_param;

struct update_buffer {
	struct object_update_request	*ub_req;
	size_t				ub_req_size;
};

/**
 * Tracking the updates being executed on this dt_device.
 */
struct dt_update_request {
	struct dt_device		*dur_dt;
	/* attached itself to thandle */
	int				dur_flags;
	/* update request result */
	int				dur_rc;
	/* Current batch(transaction) id */
	__u64				dur_batchid;
	/* Holding object updates */
	struct update_buffer		dur_buf;
	struct list_head		dur_cb_items;
};

struct update_params {
	__u32				up_params_count;
	struct object_update_param	up_params[0];
};

static inline int update_params_size(const struct update_params *params)
{
	struct object_update_param	*param;
	size_t total_size = sizeof(*params);
	unsigned int i;

	param = (struct object_update_param *)&params->up_params[0];
	for (i = 0; i < params->up_params_count; i++) {
		size_t size = object_update_param_size(param);

		param = (struct object_update_param *)((char *)param + size);
		total_size += size;
	}

	return total_size;
}

static inline struct object_update_param *
update_params_get_param(const struct update_params *params, unsigned int index)
{
	struct object_update_param *param;
	unsigned int		i;

	if (index > params->up_params_count)
		return NULL;

	param = (struct object_update_param *)&params->up_params[0];
	for (i = 0; i < index; i++)
		param = (struct object_update_param *)((char *)param +
			object_update_param_size(param));

	return param;
}

static inline void*
update_params_get_param_buf(const struct update_params *params, __u16 index,
			    __u16 *size)
{
	struct object_update_param *param;

	param = update_params_get_param(params, (unsigned int)index);
	if (param == NULL)
		return NULL;

	if (size != NULL)
		*size = param->oup_len;

	return &param->oup_buf[0];
}

struct update_op {
	struct lu_fid uop_fid;
	__u16	uop_type;
	__u16	uop_params_count;
	__u16	uop_params_off[0];
};

static inline size_t
update_op_size(struct update_op *uop)
{
	return sizeof(*uop) + uop->uop_params_count *
			      sizeof(uop->uop_params_off[0]);
}

static inline struct update_op*
update_op_next_op(struct update_op *uop)
{
	return (struct update_op *)((char *)uop + update_op_size(uop));
}

/* All of updates in the mulitple_update_record */
struct update_ops {
	__u32			uops_count;
	__u32			uops_padding;
	struct update_op	uops_op[0];
};

static inline size_t update_ops_size(const struct update_ops *ops)
{
	struct update_op *op;
	size_t total_size = sizeof(*ops);
	unsigned int i;

	op = (struct update_op *)&ops->uops_op[0];
	for (i = 0; i < ops->uops_count; i++, op = update_op_next_op(op))
		total_size += update_op_size(op);

	return total_size;
}

/*
 * This is the update record format used to store the updates in
 * disk. All updates of the operation will be stored in ur_ops.
 * All of parameters for updates of the operation will be stored
 * in ur_params.
 * To save the space of the record, parameters in ur_ops will only
 * remember their offset in ur_params, so to avoid storing duplicate
 * parameters in ur_params, which can help us save a lot space for
 * operation like creating striped directory.
 */
struct update_records {
	struct llog_rec_hdr	ur_hdr;
	__u64			ur_master_transno;
	__u64			ur_batchid;
	__u32			ur_flags;
	struct update_ops	ur_ops;
	struct update_params	ur_params;
	struct llog_rec_tail	ur_tail;
};

/**
 * Attached in the thandle to record the updates for distribute
 * distribution.
 */
struct thandle_update_records {
	/* All of updates for the cross-MDT operation. */
	struct update_records	*tur_update_records;
	size_t			tur_update_records_size;

	/* All of parameters for the cross-MDT operation */
	struct update_params    *tur_update_params;
	size_t			tur_update_params_size;
};

#define TOP_THANDLE_MAGIC	0x20140917
struct top_multiple_thandle {
	struct dt_device	*tmt_master_sub_dt;
	atomic_t		tmt_refcount;
	/* Other sub transactions will be listed here. */
	struct list_head	tmt_sub_trans_list;

	struct list_head	tmt_commit_list;
	/* All of update records will packed here */
	struct thandle_update_records *tmt_update_records;

	__u64			tmt_batchid;
	int			tmt_result;
	__u32			tmt_magic;
	__u32			tmt_committed:1;
};

/* Top/sub_thandle are used to manage the distribute transaction, which
 * includes updates on several nodes. top_handle is used to represent the
 * whole operation, and sub_thandle is used to represent the update on
 * each node. */
struct top_thandle {
	struct thandle		tt_super;
	/* The master sub transaction. */
	struct thandle		*tt_child;

	struct top_multiple_thandle *tt_multiple_thandle;
};

/* Sub thandle is used to track multiple sub thandles under one parent
 * thandle */
struct sub_thandle {
	struct thandle		*st_sub_th;
	struct dt_device	*st_dt;
	struct list_head	st_list;
	struct llog_cookie	st_cookie;
	struct dt_txn_commit_cb	st_commit_dcb;
	int			st_result;
	unsigned int		 st_committed:1;
};

static inline struct update_params *
update_records_get_params(const struct update_records *records)
{
	return (struct update_params *)((char *)records +
		offsetof(struct update_records, ur_ops) +
		update_ops_size(&records->ur_ops));
}

static inline int
update_records_size(const struct update_records *records)
{
	struct update_params *params;

	params = update_records_get_params(records);

	return cfs_size_round(sizeof(*records) +
	       update_params_size(params) +
	       update_ops_size(&records->ur_ops));
}

static inline struct update_op *
update_ops_get_op(const struct update_ops *ops, __u32 index)
{
	struct update_op *op;
	unsigned int i;

	if (index > ops->uops_count)
		return NULL;

	op = (struct update_op *)&ops->uops_op[0];
	for (i = 0; i < index; i++)
		op = update_op_next_op(op);

	return op;
}

static inline void
*object_update_param_get(const struct object_update *update, size_t index,
			 size_t *size)
{
	const struct	object_update_param *param;
	size_t		i;

	if (index >= update->ou_params_count)
		return NULL;

	param = &update->ou_params[0];
	for (i = 0; i < index; i++)
		param = (struct object_update_param *)((char *)param +
			object_update_param_size(param));

	if (size != NULL)
		*size = param->oup_len;

	if (param->oup_len == 0)
		return NULL;

	return (void *)&param->oup_buf[0];
}

static inline unsigned long
object_update_request_size(const struct object_update_request *our)
{
	unsigned long	size;
	size_t		i = 0;

	size = offsetof(struct object_update_request, ourq_updates[0]);
	for (i = 0; i < our->ourq_count; i++) {
		struct object_update *update;

		update = (struct object_update *)((char *)our + size);
		size += object_update_size(update);
	}
	return size;
}

static inline void
object_update_reply_init(struct object_update_reply *reply, size_t count)
{
	reply->ourp_magic = UPDATE_REPLY_MAGIC;
	reply->ourp_count = count;
}

static inline void
object_update_result_insert(struct object_update_reply *reply,
			    void *data, size_t data_len, size_t index,
			    int rc)
{
	struct object_update_result *update_result;
	char *ptr;

	update_result = object_update_result_get(reply, index, NULL);
	LASSERT(update_result != NULL);

	update_result->our_rc = ptlrpc_status_hton(rc);
	if (data_len > 0) {
		LASSERT(data != NULL);
		ptr = (char *)update_result +
			cfs_size_round(sizeof(struct object_update_reply));
		update_result->our_datalen = data_len;
		memcpy(ptr, data, data_len);
	}

	reply->ourp_lens[index] = cfs_size_round(data_len +
					sizeof(struct object_update_result));
}

static inline int
object_update_result_data_get(const struct object_update_reply *reply,
			      struct lu_buf *lbuf, size_t index)
{
	struct object_update_result *update_result;
	size_t size = 0;
	int    result;

	LASSERT(lbuf != NULL);
	update_result = object_update_result_get(reply, index, &size);
	if (update_result == NULL ||
	    size < cfs_size_round(sizeof(struct object_update_reply)) ||
	    update_result->our_datalen > size)
		RETURN(-EFAULT);

	result = ptlrpc_status_ntoh(update_result->our_rc);
	if (result < 0)
		return result;

	lbuf->lb_buf = update_result->our_data;
	lbuf->lb_len = update_result->our_datalen;

	return 0;
}

/* target/out_lib.c */
int out_update_pack(const struct lu_env *env, struct object_update *update,
		    size_t max_update_size, enum update_type op,
		    const struct lu_fid *fid, int params_count,
		    __u16 *param_sizes, const void **bufs);
int out_create_pack(const struct lu_env *env, struct object_update *update,
		    size_t max_update_size, const struct lu_fid *fid,
		    const struct lu_attr *attr, struct dt_allocation_hint *hint,
		    struct dt_object_format *dof);
int out_striped_create_pack(const struct lu_env *env,
			    struct object_update *update,
			    size_t max_update_size,
			    const struct lu_fid *fid, const void *buffer,
			    int buffer_length, struct lu_attr *attr);
int out_object_destroy_pack(const struct lu_env *env,
			    struct object_update *update,
			    size_t max_update_size,
			    const struct lu_fid *fid, __u16 cookie_size,
			    const void *cookie);
int out_index_delete_pack(const struct lu_env *env,
			  struct object_update *update, size_t max_update_size,
			  const struct lu_fid *fid, const struct dt_key *key);
int out_index_insert_pack(const struct lu_env *env,
			  struct object_update *update, size_t max_update_size,
			  const struct lu_fid *fid, const struct dt_rec *rec,
			  const struct dt_key *key);
int out_xattr_set_pack(const struct lu_env *env,
		       struct object_update *update, size_t max_update_size,
		       const struct lu_fid *fid, const struct lu_buf *buf,
		       const char *name, int flag);
int out_xattr_del_pack(const struct lu_env *env,
		       struct object_update *update, size_t max_update_size,
		       const struct lu_fid *fid, const char *name);
int out_attr_set_pack(const struct lu_env *env,
		      struct object_update *update, size_t max_update_size,
		      const struct lu_fid *fid, const struct lu_attr *attr);
int out_ref_add_pack(const struct lu_env *env,
		     struct object_update *update, size_t max_update_size,
		     const struct lu_fid *fid);
int out_ref_del_pack(const struct lu_env *env,
		     struct object_update *update, size_t max_update_size,
		     const struct lu_fid *fid);
int out_write_pack(const struct lu_env *env,
		   struct object_update *update, size_t max_update_size,
		   const struct lu_fid *fid, const struct lu_buf *buf,
		   loff_t pos);
int out_attr_get_pack(const struct lu_env *env,
		      struct object_update *update, size_t max_update_size,
		      const struct lu_fid *fid);
int out_index_lookup_pack(const struct lu_env *env,
			  struct object_update *update, size_t max_update_size,
			  const struct lu_fid *fid, struct dt_rec *rec,
			  const struct dt_key *key);
int out_xattr_get_pack(const struct lu_env *env,
		       struct object_update *update, size_t max_update_size,
		       const struct lu_fid *fid, const char *name);
int out_read_pack(const struct lu_env *env, struct object_update *update,
		  int max_update_length, const struct lu_fid *fid,
		  ssize_t size, loff_t pos);

const char *update_op_str(__u16 opcode);

/* target/update_trans.c */
struct thandle *get_sub_thandle(const struct lu_env *env, struct thandle *th,
				const struct dt_object *sub_obj);
struct thandle *
top_trans_create(const struct lu_env *env, struct dt_device *master_dev);
int top_trans_start(const struct lu_env *env, struct dt_device *master_dev,
		    struct thandle *th);
int top_trans_stop(const struct lu_env *env, struct dt_device *master_dev,
		   struct thandle *th);
void top_multiple_thandle_destroy(struct top_multiple_thandle *tmt);

static inline void top_multiple_thandle_get(struct top_multiple_thandle *tmt)
{
	atomic_inc(&tmt->tmt_refcount);
}

static inline void top_multiple_thandle_put(struct top_multiple_thandle *tmt)
{
	if (atomic_dec_and_test(&tmt->tmt_refcount))
		top_multiple_thandle_destroy(tmt);
}

struct sub_thandle *lookup_sub_thandle(struct top_multiple_thandle *tmt,
				       struct dt_device *dt_dev);
int sub_thandle_trans_create(const struct lu_env *env,
			     struct top_thandle *top_th,
			     struct sub_thandle *st);

/* update_records.c */
void update_records_dump(struct update_records *records, unsigned int mask);
int update_records_create_pack(const struct lu_env *env,
			       struct update_ops *ops,
			       size_t *max_ops_size,
			       struct update_params *params,
			       size_t *max_param_size,
			       const struct lu_fid *fid,
			       const struct lu_attr *attr,
			       const struct dt_allocation_hint *hint,
			       struct dt_object_format *dof);
int update_records_attr_set_pack(const struct lu_env *env,
				 struct update_ops *ops, size_t *max_ops_size,
				 struct update_params *params,
				 size_t *max_param_size,
				 const struct lu_fid *fid,
				 const struct lu_attr *attr);
int update_records_ref_add_pack(const struct lu_env *env,
				struct update_ops *ops, size_t *max_ops_size,
				struct update_params *params,
				size_t *max_param_size,
				const struct lu_fid *fid);
int update_records_ref_del_pack(const struct lu_env *env,
				struct update_ops *ops, size_t *max_ops_size,
				struct update_params *params,
				size_t *max_param_size,
				const struct lu_fid *fid);
int update_records_object_destroy_pack(const struct lu_env *env,
				       struct update_ops *ops,
				       size_t *max_ops_size,
				       struct update_params *params,
				       size_t *max_param_size,
				       const struct lu_fid *fid);
int update_records_index_insert_pack(const struct lu_env *env,
				     struct update_ops *ops,
				     size_t *max_ops_size,
				     struct update_params *params,
				     size_t *max_param_size,
				     const struct lu_fid *fid,
				     const struct dt_rec *rec,
				     const struct dt_key *key);
int update_records_index_delete_pack(const struct lu_env *env,
				     struct update_ops *ops,
				     size_t *max_ops_size,
				     struct update_params *params,
				     size_t *max_param_size,
				     const struct lu_fid *fid,
				     const struct dt_key *key);
int update_records_xattr_set_pack(const struct lu_env *env,
				  struct update_ops *ops, size_t *max_ops_size,
				  struct update_params *params,
				  size_t *max_param_size,
				  const struct lu_fid *fid,
				  const struct lu_buf *buf, const char *name,
				  int flag);
int update_records_xattr_del_pack(const struct lu_env *env,
				  struct update_ops *ops, size_t *max_ops_size,
				  struct update_params *params,
				  size_t *max_param_size,
				  const struct lu_fid *fid,
				  const char *name);
int update_records_write_pack(const struct lu_env *env,
			      struct update_ops *ops, size_t *max_ops_size,
			      struct update_params *params,
			      size_t *max_param_size,
			      const struct lu_fid *fid,
			      const struct lu_buf *buf,
			      loff_t pos);
int update_records_punch_pack(const struct lu_env *env,
			      struct update_ops *ops, size_t *max_ops_size,
			      struct update_params *params,
			      size_t *max_param_size,
			      const struct lu_fid *fid,
			      __u64 start, __u64 end);

int tur_update_records_extend(struct thandle_update_records *tur,
			      size_t new_size);
int tur_update_params_extend(struct thandle_update_records *tur,
			     size_t new_size);
int merge_params_updates_buf(const struct lu_env *env, struct thandle *th);

struct update_thread_info {
	struct lu_attr			uti_attr;
	struct lu_fid			uti_fid;
	struct lu_buf			uti_buf;
	struct ldlm_res_id		uti_resid;
	ldlm_policy_data_t		uti_policy;
	struct ldlm_enqueue_info	uti_einfo;
	struct thandle_update_records	uti_tur;
	struct dt_insert_rec		uti_rec;
};

extern struct lu_context_key update_thread_key;

static inline struct update_thread_info *
update_env_info(const struct lu_env *env)
{
	struct update_thread_info *uti;

	uti = lu_context_key_get(&env->le_ctx, &update_thread_key);
	LASSERT(uti);
	return uti;
}

#define UPDATE_RECORDS_BUFFER_SIZE	8192
#define UPDATE_PARAMS_BUFFER_SIZE	8192
#define update_record_pack(name, rc, th, ...)				\
do {                                                                    \
	struct top_thandle	*tth;					\
	struct top_multiple_thandle *tmt;				\
	struct thandle_update_records *tur;				\
	struct update_params	*params;				\
	struct update_records	*records;				\
	size_t			params_size;				\
	size_t			ops_size;				\
	size_t			max_op_size;				\
	size_t			max_param_size;				\
									\
	tth = container_of0(th, struct top_thandle, tt_super);		\
	tmt = tth->tt_multiple_thandle;					\
	LASSERT(tmt != NULL);						\
	LASSERT(tmt->tmt_update_records != NULL);			\
	tur = tmt->tmt_update_records;					\
	params = tur->tur_update_params;				\
	params_size = update_params_size(params);			\
	max_param_size = tur->tur_update_params_size - params_size;	\
									\
	records = tur->tur_update_records;				\
	ops_size = update_ops_size(&records->ur_ops);			\
	max_op_size = tur->tur_update_records_size -			\
			  ops_size - sizeof(*records);			\
									\
	rc = update_records_##name##_pack(env, &records->ur_ops,	\
					  &max_op_size, params,		\
					  &max_param_size,		\
					  __VA_ARGS__);			\
	if (rc == -E2BIG) {						\
		int rc1;						\
									\
		/* extend update records buffer */			\
		if (max_op_size >= (tur->tur_update_records_size -	\
				    ops_size - sizeof(*records))) {	\
			rc1 = tur_update_records_extend(tur,		\
					tur->tur_update_records_size +	\
					UPDATE_RECORDS_BUFFER_SIZE);	\
			if (rc1 != 0) {					\
				rc = rc1;				\
				break;					\
			}						\
		}							\
									\
		/* extend parameters buffer */				\
		if (max_param_size >= (tur->tur_update_params_size -	\
				       params_size)) {			\
			rc1 = tur_update_params_extend(tur,		\
					tur->tur_update_records_size +	\
					UPDATE_PARAMS_BUFFER_SIZE);	\
			if (rc1 != 0) {					\
				rc = rc1;				\
				break;					\
			}						\
		}							\
		continue;						\
	} else {							\
		break;							\
	}								\
} while (1)
#endif
