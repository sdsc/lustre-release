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

#define OBJECT_UPDATE_INIT_BUFFER_SIZE	8192
#define OBJECT_UPDATE_REPLY_SIZE	8192
struct dt_update_request {
	struct dt_device		*dur_dt;
	/* attached itself to thandle */
	cfs_list_t			dur_list;
	int				dur_flags;
	/* update request result */
	int				dur_rc;
	/* Current batch(transaction) id */
	int				dur_batchid;
	/* Holding the update req */
	struct object_update_request	*dur_req;
	int				dur_req_len;
};

static inline unsigned long object_update_size(struct object_update *update)
{
	unsigned long size;
	int	   i;

	size = cfs_size_round(offsetof(struct object_update, ou_bufs[0]));
	for (i = 0; i < OBJECT_UPDATE_PARAMS_MAX; i++)
		size += cfs_size_round(update->ou_lens[i]);

	return size;
}

static inline void
*object_update_param_get(struct object_update *update, int index, int *size)
{
	void	*ptr;

	if (index >= OBJECT_UPDATE_PARAMS_MAX)
		return NULL;

	if (unlikely(update->ou_lens[index] == 0)) {
		ptr = NULL;
	} else {
		int	i;
		ptr = (char *)update +
		      cfs_size_round(offsetof(struct object_update,
					      ou_bufs[0]));
		for (i = 0; i < index; i++)
			ptr += cfs_size_round(update->ou_lens[i]);
	}

	if (size != NULL)
		*size = update->ou_lens[index];

	return ptr;
}

static inline unsigned long
object_update_request_size(struct object_update_request *our)
{
	unsigned long size;
	int	   i = 0;

	size = cfs_size_round(offsetof(struct object_update_request,
				       ourq_bufs[0]));
	for (i = 0; i < our->ourq_count; i++) {
		struct object_update *update;

		update = (struct object_update *)((char *)our + size);
		size += object_update_size(update);
	}
	return size;
}

static inline struct object_update
*object_update_request_get(struct object_update_request *our, int index,
			   int *size)
{
	int	count = our->ourq_count;
	void	*ptr;
	int	i = 0;

	if (index >= count)
		return NULL;

	ptr = (char *)our +
	      cfs_size_round(offsetof(struct object_update_request,
				      ourq_bufs[0]));
	for (i = 0; i < index; i++)
		ptr += object_update_size((struct object_update *)ptr);

	if (size != NULL)
		*size = object_update_size((struct object_update *)ptr);

	return ptr;
}

static inline void
object_update_reply_init(struct object_update_reply *reply, int count)
{
	reply->ourp_magic = UPDATE_REPLY_MAGIC;
	reply->ourp_count = count;
}

static inline struct object_update_result
*object_update_result_get(struct object_update_reply *reply, int index,
			 int *size)
{
	char *ptr;
	int count = reply->ourp_count;
	int i;

	if (index >= count)
		return NULL;

	ptr = (char *)reply +
	      cfs_size_round(offsetof(struct object_update_reply,
				      ourp_lens[count]));
	for (i = 0; i < index; i++) {
		LASSERT(reply->ourp_lens[i] > 0);
		ptr += cfs_size_round(reply->ourp_lens[i]);
	}

	if (size != NULL)
		*size = reply->ourp_lens[index];

	return (struct object_update_result *)ptr;
}

static inline void
object_update_result_insert(struct object_update_reply *reply,
			    void *data, int data_len, int index,
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
object_update_result_data_get(struct object_update_reply *reply,
			      void **buf, int index)
{
	struct object_update_result *update_result;
	int  size = 0;
	int  result;

	update_result = object_update_result_get(reply, index, &size);
	if (update_result == NULL ||
	    size < cfs_size_round(sizeof(struct object_update_reply)) ||
	    update_result->our_datalen > size)
		RETURN(-EFAULT);

	result = ptlrpc_status_ntoh(update_result->our_rc);
	if (result < 0)
		return result;

	*buf = (char *)update_result->our_data;
	return update_result->our_datalen;
}

#endif
