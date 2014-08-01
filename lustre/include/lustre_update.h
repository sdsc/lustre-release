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
 * Copyright (c) 2013, Intel Corporation.
 */
/*
 * lustre/include/lustre_update.h
 *
 * Author: Di Wang <di.wang@intel.com>
 */

#ifndef _LUSTRE_UPDATE_H
#define _LUSTRE_UPDATE_H

#define UPDATE_BUFFER_SIZE	8192
static inline void update_buf_init(struct update_buf *ubuf)
{
	ubuf->ub_magic = UPDATE_REQUEST_MAGIC;
	ubuf->ub_count = 0;
}

static inline struct update_buf *update_buf_alloc(void)
{
	struct update_buf *buf;

	OBD_ALLOC_LARGE(buf, UPDATE_BUFFER_SIZE);
	if (buf == NULL)
		return NULL;

	update_buf_init(buf);
	return buf;
}

static inline void update_buf_free(struct update_buf *ubuf)
{
	if (ubuf == NULL)
		return;
	LASSERT(ubuf->ub_magic == UPDATE_REQUEST_MAGIC);
	OBD_FREE_LARGE(ubuf, UPDATE_BUFFER_SIZE);
	return;
}

static inline unsigned long update_size(struct update *update)
{
	unsigned long size;
	int	   i;

	size = cfs_size_round(offsetof(struct update, u_bufs[0]));
	for (i = 0; i < UPDATE_PARAM_COUNT; i++)
		size += cfs_size_round(update->u_lens[i]);

	return size;
}

static inline void *update_param_buf(struct update *update, int index,
				     int *size)
{
	int	i;
	void	*ptr;

	if (index >= UPDATE_PARAM_COUNT)
		return NULL;

	ptr = (char *)update + cfs_size_round(offsetof(struct update,
						       u_bufs[0]));
	for (i = 0; i < index; i++) {
		LASSERT(update->u_lens[i] > 0);
		ptr += cfs_size_round(update->u_lens[i]);
	}

	if (size != NULL)
		*size = update->u_lens[index];

	return ptr;
}

static inline unsigned long update_buf_size(struct update_buf *buf)
{
	unsigned long size;
	int	   i = 0;

	size = cfs_size_round(offsetof(struct update_buf, ub_bufs[0]));
	for (i = 0; i < buf->ub_count; i++) {
		struct update *update;

		update = (struct update *)((char *)buf + size);
		size += update_size(update);
	}
	LASSERT(size <= UPDATE_BUFFER_SIZE);
	return size;
}

static inline void *update_buf_get(struct update_buf *buf, int index, int *size)
{
	int	count = buf->ub_count;
	void	*ptr;
	int	i = 0;

	if (index >= count)
		return NULL;

	ptr = (char *)buf + cfs_size_round(offsetof(struct update_buf,
						    ub_bufs[0]));
	for (i = 0; i < index; i++)
		ptr += update_size((struct update *)ptr);

	if (size != NULL)
		*size = update_size((struct update *)ptr);

	return ptr;
}

static inline int update_buf_master_idx(struct update_buf *ubuf)
{
	struct update	*update;
	int		count = ubuf->ub_count;

	LASSERT(count > 0);
	/* XXX currently the master index of one update buf should
	 * be same, so we only need return the master index of first
	 * update */
	update = update_buf_get(ubuf, 0, NULL);

	return update->u_master_index;
}

static inline void update_init_reply_buf(struct update_reply_buf *reply_buf,
					 int count)
{
	reply_buf->urb_version = UPDATE_REPLY_V2;
	reply_buf->urb_count = count;
}

static inline struct update_reply
*update_get_reply(struct update_reply_buf *reply_buf, int index, int *size)
{
	char *ptr;
	int count = reply_buf->urb_count;
	int i;

	if (index >= count)
		return NULL;

	ptr = (char *)reply_buf +
	       cfs_size_round(offsetof(struct update_reply_buf,
				       urb_lens[count]));
	for (i = 0; i < index; i++) {
		LASSERT(reply_buf->urb_lens[i] >=
			cfs_size_round(sizeof(struct update_reply)));
		ptr += cfs_size_round(reply_buf->urb_lens[i]);
	}

	if (size != NULL)
		*size = reply_buf->urb_lens[index];

	return (struct update_reply *)ptr;
}

static inline void update_insert_reply(struct update_reply_buf *reply_buf,
				       void *data, int data_len, int index,
				       int rc)
{
	struct update_reply *reply;
	char *ptr;

	reply = update_get_reply(reply_buf, index, NULL);
	LASSERT(reply != NULL);

	reply->ur_rc = ptlrpc_status_hton(rc);
	ptr = (char *)reply + cfs_size_round(sizeof(struct update_reply));
	if (data_len > 0) {
		LASSERT(data != NULL);
		reply->ur_datalen = data_len;
		memcpy(ptr, data, data_len);
	}

	reply_buf->urb_lens[index] = cfs_size_round(data_len) +
				 cfs_size_round(sizeof(struct update_reply));
}

static inline int update_get_reply_data(struct ptlrpc_request *req,
				        struct update_reply_buf *reply_buf,
				        void **buf, int index)
{
	struct update_reply *reply;
	int  size = 0;
	int  result;

	reply = update_get_reply(reply_buf, index, &size);
	LASSERT(reply != NULL);
	if (ptlrpc_rep_need_swab(req))
		lustre_swab_update_reply(reply);
	result = ptlrpc_status_ntoh(reply->ur_rc);
	if (result < 0)
		return result;

	LASSERT((reply != NULL &&
		 size >= cfs_size_round(sizeof(struct update_reply))));
	*buf = (char *)reply->ur_data;

	return reply->ur_datalen;
}

static inline void update_cpu_to_le(struct update *dst, struct update *src)
{
	int i;

	dst->u_type = cpu_to_le16(src->u_type);
	dst->u_master_index = cpu_to_le16(src->u_master_index);
	dst->u_flags = cpu_to_le32(src->u_flags);
	dst->u_batchid = cpu_to_le64(src->u_batchid);
	dst->u_xid = cpu_to_le64(src->u_xid);
	fid_cpu_to_le(&dst->u_fid, &src->u_fid);
	llog_cookie_cpu_to_le(&dst->u_cookie, &src->u_cookie);
	for (i = 0; i < UPDATE_PARAM_COUNT; i++)
		dst->u_lens[i] = cpu_to_le32(src->u_lens[i]);
	return;
}

static inline void update_le_to_cpu(struct update *dst, struct update *src)
{
	int i;

	dst->u_type = le16_to_cpu(src->u_type);
	dst->u_master_index = le16_to_cpu(src->u_master_index);
	dst->u_flags = le32_to_cpu(src->u_flags);
	dst->u_batchid = le64_to_cpu(src->u_batchid);
	dst->u_xid = le64_to_cpu(src->u_xid);
	fid_le_to_cpu(&dst->u_fid, &src->u_fid);
	llog_cookie_le_to_cpu(&dst->u_cookie, &src->u_cookie);
	for (i = 0; i < UPDATE_PARAM_COUNT; i++)
		dst->u_lens[i] = le32_to_cpu(src->u_lens[i]);
	return;
}

static inline void update_buf_cpu_to_le(struct update_buf *dst_ubuf,
					struct update_buf *src_ubuf)
{
	int i;

	dst_ubuf->ub_magic = cpu_to_le32(src_ubuf->ub_magic);
	dst_ubuf->ub_count = cpu_to_le32(src_ubuf->ub_count);

	for (i = 0; i < src_ubuf->ub_count; i++) {
		struct update *src = update_buf_get(src_ubuf, i, NULL);
		struct update *dst = update_buf_get(dst_ubuf, i, NULL);

		update_cpu_to_le(dst, src);
	}

	return;
}

static inline void update_buf_le_to_cpu(struct update_buf *dst_ubuf,
					struct update_buf *src_ubuf)
{
	int i;

	dst_ubuf->ub_magic = le32_to_cpu(src_ubuf->ub_magic);
	dst_ubuf->ub_count = le32_to_cpu(src_ubuf->ub_count);
	for (i = 0; i < dst_ubuf->ub_count; i++) {
		struct update *src = update_buf_get(src_ubuf, i, NULL);
		struct update *dst = update_buf_get(dst_ubuf, i, NULL);

		update_le_to_cpu(dst, src);
	}

	return;
}


const char *update_op_str(__u16 opcode);
/* For debugging purpose */
static inline void update_dump_buf(struct update_buf *ubuf)
{
	int i;

	CDEBUG(D_INFO, "ubuf %p magic %x count %d\n", ubuf, ubuf->ub_magic,
	       ubuf->ub_count);

	for (i = 0; i < ubuf->ub_count; i++) {
		struct update *update;
		update = (struct update *)update_buf_get(ubuf, i, NULL);
		CDEBUG(D_INFO, "i: %d fid: "DFID" op: %s master: %d idx %d"
		       "batchid: "LPU64" xid: "LPU64" cookie %u "DOSTID":%u\n",
		       i, PFID(&update->u_fid), update_op_str(update->u_type),
		       (int)update->u_master_index, update->u_index,
		       update->u_batchid, update->u_xid,
		       update->u_cookie.lgc_index,
		       POSTID(&update->u_cookie.lgc_lgl.lgl_oi),
		       update->u_cookie.lgc_lgl.lgl_ogen);
	}
}

struct update *update_pack(const struct lu_env *env,
			   struct update_buf *ubuf, int buf_len, int op,
			   const struct lu_fid *fid, int count, int *lens,
			   __u64 batchid, int index, int master_index);

int update_insert(const struct lu_env *env, struct update_buf *ubuf,
		  int buf_len, int op, const struct lu_fid *fid,
		  int count, int *lens, char **bufs, __u64 batchid,
		  int index, int master_index);
void dt_update_xid(struct update_buf *ubuf, int index, __u64 xid);

struct obd_llog_group *dt_update_find_olg(struct dt_device *dt, int index);

#endif
