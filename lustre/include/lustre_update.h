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
struct update_request {
	struct dt_device	*ur_dt;
	struct list_head	ur_list;    /* attached itself to thandle */
	int			ur_flags;
	int			ur_rc;	    /* request result */
	int			ur_batchid; /* Current batch(trans) id */
	struct update_buf	*ur_buf;   /* Holding the update req */
};

/* use high bit to determine if the request/reply buffer is already swabbed */
#define UPDATE_FL_SWABBED 0x80000000

static inline unsigned long update_size(struct update *update)
{
	unsigned long size;
	int	   i;

	size = cfs_size_round(offsetof(struct update, u_bufs[0]));
	for (i = 0; i < UPDATE_BUF_COUNT; i++)
		size += cfs_size_round(update->u_lens[i] & ~UPDATE_FL_SWABBED);

	return size;
}

static inline void *update_param_buf(struct update *update, int index,
				     int *size)
{
	int	i;
	void	*ptr;

	if (index >= UPDATE_BUF_COUNT)
		return NULL;

	if (unlikely(update->u_lens[i] & ~UPDATE_FL_SWABBED) == 0)
		return NULL;

	ptr = (char *)update + cfs_size_round(offsetof(struct update,
						       u_bufs[0]));
	for (i = 0; i < index; i++) {
		LASSERT((update->u_lens[i] & ~UPDATE_FL_SWABBED) > 0);
		ptr += cfs_size_round(update->u_lens[i] & ~UPDATE_FL_SWABBED);
	}

	if (size != NULL)
		*size = update->u_lens[index] & ~UPDATE_FL_SWABBED;

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
	void	*ptr;
	int	i;

	if (index >= buf->ub_count)
		return NULL;

	ptr = (char *)buf + cfs_size_round(offsetof(struct update_buf,
						    ub_bufs[0]));
	for (i = 0; i < index; i++)
		ptr += update_size((struct update *)ptr);

	if (size != NULL)
		*size = update_size((struct update *)ptr);

	return ptr;
}

static inline void update_init_reply_buf(struct update_reply *reply, int count)
{
	reply->ur_version = UPDATE_REPLY_V1;
	reply->ur_count = count;
}

/**
 * Mark an update_reply buffer as swabbed.  This should be set after all of
 * the fields in the buffer have been swabbed, OR before returning an error
 * to the caller.  If it is set too early, then later parts of the swabber
 * may think it has already been processed, and if not set before an error
 * is returned then the buffer will be re-swabbed if accessed again. */
static inline void update_reply_set_swabbed(struct update_reply *reply,
					    int index)
{
	reply->ur_lens[index] |= UPDATE_FL_SWABBED;
}

/**
 * Check if an update_reply buffer is already swabbed, and do not swab again.
 * The UPDATE_FL_SWABBED flag should not be passed over the network. */
static inline bool update_reply_need_swab(struct update_reply *reply, int index)
{
	return (reply->ur_lens[index] & UPDATE_FL_SWABBED) == 0;
}

static inline void *update_get_buf_internal(struct update_reply *reply,
					    int index, int *size)
{
	char *ptr;
	int count = reply->ur_count;
	int i;

	if (index >= count)
		return NULL;

	ptr = (char *)reply + cfs_size_round(offsetof(struct update_reply,
					     ur_lens[count]));
	for (i = 0; i < index; i++) {
		LASSERT((reply->ur_lens[i] & ~UPDATE_FL_SWABBED) > 0);
		ptr += cfs_size_round(reply->ur_lens[i] & ~UPDATE_FL_SWABBED);
	}

	if (size != NULL)
		*size = reply->ur_lens[index] & ~UPDATE_FL_SWABBED;

	return ptr;
}

static inline void update_insert_reply(struct update_reply *reply, void *data,
				       int data_len, int index, int rc)
{
	char *ptr;

	ptr = update_get_buf_internal(reply, index, NULL);
	LASSERT(ptr != NULL);

	*(int *)ptr = ptlrpc_status_hton(rc);
	ptr += sizeof(int);
	if (data_len > 0) {
		LASSERT(data != NULL);
		memcpy(ptr, data, data_len);
	}
	reply->ur_lens[index] = data_len + sizeof(int);
}

static inline int update_get_reply_buf(struct ptlrpc_request *req,
				       struct update_reply *reply, void **buf,
				       int index)
{
	char *ptr;
	int  size = 0;
	int  result;

	if (reply == NULL)
		RETURN(-EPROTO);

	/* header was already swabbed by lustre_swab_update_reply_buf() */
	if (reply->ur_version != UPDATE_REPLY_V1) {
		result = -EPROTO;
		CERROR("%s: Wrong version %x expected %x: rc = %d\n",
		       req->rq_import->imp_obd->obd_name,
		       reply->ur_version, UPDATE_REPLY_V1, result);
		RETURN(result);
	}

	ptr = update_get_buf_internal(reply, index, &size);
	if (ptr == NULL)
		RETURN(-EPROTO);

	if (ptlrpc_rep_need_swab(req) && update_reply_need_swab(reply, index))
		__swab32s((__u32 *)ptr);
	result = ptlrpc_status_ntoh(*(int *)ptr);
	if (result < 0) {
		/* If returning an error, mark update_reply swabbed so this
		 * isn't done again on the next access.  Later calls should
		 * also hit the same error for this update buffer, so no
		 * further processing should be done with it anyway. */
		update_reply_set_swabbed(reply, index);
		RETURN(result);
	}

	LASSERT(size >= sizeof(int));
	*buf = ptr + sizeof(int);
	return size - sizeof(int);
}

#endif
