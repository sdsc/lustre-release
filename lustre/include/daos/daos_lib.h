/**
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/**
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/**
 * This file is part of lustre/DAOS
 *
 * lustre/include/daos/daos_lib.h
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#ifndef __DAOS_LIB_H__
#define __DAOS_LIB_H__

#include <libcfs/libcfs.h>
#include <daos/daos_api.h>

typedef struct {
	daos_handle_t		eqh;
	__u64			cookie;
} daos_event_id_t;

struct daos_event_priv {
	daos_handle_t		ev_eqh;
	__u64			ev_cookie;
	__u64			ev_parent;
};

typedef struct {
	/** called with eq_lock */
	int (*op_abort)(void *kparam, int unlinked);
	/** called w/o eq_lock */
	int (*op_complete)(void *kparam, void *uparam, int unlinked);
} daos_kevent_ops_t;

struct daos_kevent;

void daos_event_complete(struct daos_kevent *ev, int error);
struct daos_kevent *daos_event_dispatch(daos_event_id_t eid, void *uparam,
					void *kparam, daos_kevent_ops_t *ops);

enum {
	DAOS_IOC_OTHR		= 1,
	DAOS_IOC_EQ,
	DAOS_IOC_SYS,
	DAOS_IOC_EPOCH,
	DAOS_IOC_CO,
	DAOS_IOC_SHARD,
	DAOS_IOC_OBJ,
};

#define DAOS_IOC_VERSION	0x00010002

#define DAOS_IOC_BUF_LEN	(1U << 13)
#define DAOS_IOC_INBUF_LEN	(1U << 30)
#define DAOS_IOC_INBUF_NUM	4

struct daos_ioc_hdr {
	__u32			ih_version;
	__u32			ih_len;
};

#define DAOS_IOC_HDR		struct daos_ioc_hdr

#define DAOS_IOC_EQ_CREATE	_IOWR(DAOS_IOC_EQ, 1, DAOS_IOC_HDR)
#define DAOS_IOC_EQ_DESTROY	_IOWR(DAOS_IOC_EQ, 2, DAOS_IOC_HDR)
#define DAOS_IOC_EQ_POLL	_IOWR(DAOS_IOC_EQ, 3, DAOS_IOC_HDR)
#define DAOS_IOC_EQ_QUERY	_IOWR(DAOS_IOC_EQ, 4, DAOS_IOC_HDR)
#define DAOS_IOC_EV_INIT	_IOWR(DAOS_IOC_EQ, 5, DAOS_IOC_HDR)
#define DAOS_IOC_EV_FINI	_IOWR(DAOS_IOC_EQ, 6, DAOS_IOC_HDR)
#define DAOS_IOC_EV_ABORT	_IOWR(DAOS_IOC_EQ, 7, DAOS_IOC_HDR)
#define DAOS_IOC_EV_NEXT	_IOWR(DAOS_IOC_EQ, 8, DAOS_IOC_HDR)

/** start/stop cfs_wi_sched, it's for debug and test */
#define DAOS_IOC_NOOP		_IOWR(DAOS_IOC_OTHR, 1, DAOS_IOC_HDR)

struct daos_usr_data {
	struct daos_ioc_hdr	ud_hdr;
	__u32			ud_inbuf_lens[DAOS_IOC_INBUF_NUM];
	char			*ud_inbufs[DAOS_IOC_INBUF_NUM];
	char			ud_body[0];
};

struct daos_usr_eq_create {
	daos_handle_t		__user *eqc_handle;
};

struct daos_usr_eq_destroy {
	daos_handle_t		eqd_handle;
	/** allow to destroy EQ even there's inflight event */
	__u32			eqd_force;
};

struct daos_usr_eq_poll {
	daos_handle_t		eqp_handle;
	__s64			eqp_timeout;
	__u32			eqp_wait_inf;
	__u32			eqp_ueventn;
	daos_event_t		__user **eqp_uevents;
};

struct daos_usr_eq_query {
	daos_handle_t		eqq_handle;
	__u32			eqq_query;
	__u32			eqq_ueventn;
	daos_event_t		__user **eqq_uevents;
};

struct daos_usr_ev_init {
	daos_handle_t		evi_eqh;
	__u64			evi_parent;
	daos_event_t		__user *evi_uevent;
	struct daos_event_priv	__user *evi_upriv;
};

struct daos_usr_ev_fini {
	daos_event_id_t		evf_id;
	__u32			evf_force;
};

struct daos_usr_ev_abort {
	daos_event_id_t		eva_id;
};

struct daos_usr_ev_next {
	daos_event_id_t		evn_parent;
	__u64			evn_current;
	daos_event_t		__user **evn_next;
};

struct daos_usr_noop {
	daos_event_id_t		unp_ev;
	/** execution latency of NOOP (second) */
	__u32			unp_latency;
	__u32			unp_reserved;
};

static inline int
daos_ioc_data_parse(struct daos_usr_data *data)
{
	unsigned int	len;
	int		i;

	for (i = len = 0; i < DAOS_IOC_INBUF_NUM; i++) {
		if (data->ud_inbuf_lens[i] > DAOS_IOC_INBUF_LEN) {
			CERROR("DAOS ioctl: inbuf[%d] len is too large: %d\n",
			       i, data->ud_inbuf_lens[i]);
			return -EINVAL;
		}

		if ((data->ud_inbuf_lens[i] != 0 &&
		     data->ud_inbufs[i] == NULL) ||
		    (data->ud_inbuf_lens[i] == 0 &&
		     data->ud_inbufs[i] != NULL)) {
			CERROR("DAOS ioctl: inbuf[%d] address %p buf size %d\n",
			       i, (void *)data->ud_inbufs[i],
			       data->ud_inbuf_lens[i]);
			return -EINVAL;
		}

		if (data->ud_inbuf_lens[i] != 0) {
			data->ud_inbufs[i] = (char *)(&data->ud_body[len]);
			len += cfs_size_round(data->ud_inbuf_lens[i]);
			if (len + sizeof(*data) > data->ud_hdr.ih_len)
				return -EINVAL;
		}
	}
	return 0;
}

#endif /* __DAOS_LIB_H__ */
