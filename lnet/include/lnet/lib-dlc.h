/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * Copyright (c) 2014, Intel Corporation.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 *
 * LGPL HEADER END
 *
 * Contributers:
 *   Amir Shehata
 */

#ifndef LNET_DLC_H
#define LNET_DLC_H

#include <libcfs/libcfs_ioctl.h>
#include <lnet/lib-types.h>

#define MAX_NUM_SHOW_ENTRIES	32
#define LNET_MAX_STR_LEN	128
#define LNET_MAX_SHOW_NUM_CPT	128

struct lnet_ioctl_net_config {
	char ni_interfaces[LNET_MAX_INTERFACES][LNET_MAX_STR_LEN];
	__u32 ni_status;
	__u32 ni_cpts[LNET_MAX_SHOW_NUM_CPT];
};

struct lnet_ioctl_pool_cfg {
	struct {
		__u32 pl_npages;
		__u32 pl_nbuffers;
		__u32 pl_credits;
		__u32 pl_mincredits;
	} pl_pools[LNET_NRBPOOLS];
	__u32 pl_routing;
};

struct lnet_ioctl_config_data {
	struct libcfs_ioctl_hdr cfg_hdr;

	__u32 cfg_net;
	__u32 cfg_count;
	__u64 cfg_nid;
	__u32 cfg_ncpts;

	union {
		struct {
			__u32 rtr_hop;
			__u32 rtr_priority;
			__u32 rtr_flags;
		} cfg_route;
		struct {
			char net_intf[LNET_MAX_STR_LEN];
			__s32 net_peer_timeout;
			__s32 net_peer_tx_credits;
			__s32 net_peer_rtr_credits;
			__s32 net_max_tx_credits;
			__u32 net_cksum_algo;
			__u32 net_pad;
		} cfg_net;
		struct {
			__u32 buf_enable;
			__s32 buf_tiny;
			__s32 buf_small;
			__s32 buf_large;
		} cfg_buffers;
	} cfg_config_u;

	char cfg_bulk[0];
};

struct lnet_ioctl_peer {
	struct libcfs_ioctl_hdr pr_hdr;
	__u32 pr_count;
	__u32 pr_detail;
	__u64 pr_nid;

	union {
		struct {
			char cr_aliveness[LNET_MAX_STR_LEN];
			__u32 cr_refcount;
			__u32 cr_ni_peer_tx_credits;
			__u32 cr_peer_tx_credits;
			__u32 cr_peer_rtr_credits;
			__u32 cr_peer_min_rtr_credits;
			__u32 cr_peer_tx_qnob;
			__u32 cr_ncpt;
		} pr_peer_credits;
		struct {
			__u32 peer_ref_count;
			__u32 connecting;
			__u32 accepting;
			__u32 active_conn;
			__u32 waiting_conn;
			__u32 cpt;

			union {
				struct {
					__u32 local_ip;
					__u32 peer_ip;
					__u32 peer_port;
					__u32 conn_count;
					__u32 shared_count;
					__u32 pid;
				} socklnd;
				struct {
					__u64 peer_stamp;
					__u32 dev_id;
					__u32 peer_status;
					__u32 fmaq_len;
					__u32 nfma;
					__u32 tx_seq;
					__u32 rx_seq;
					__u32 nrdma;
					__u32 pad;
				} gnilnd;
			} pr_lnd;
		} pr_peer_details;
	} pr_lnd_u;
};

struct ioctl_tx_queue {
	int tx_sending;
	int tx_queued;
	int tx_waiting;
	int tx_status;
	unsigned long tx_deadline;
	__u64 tx_cookie;
	__u8 tx_msg_type;
	__u8 tx_msg_credits;
};

enum tx_conn_queue_type {
	TX_QUEUE_NOOPS = 0,
	TX_QUEUE_CR,
	TX_QUEUE_NCR,
	TX_QUEUE_RSRVD,
	TX_QUEUE_ACTIVE,
	TX_QUEUE_MAX,
};

struct lnet_ioctl_conn {
	struct libcfs_ioctl_hdr conn_hdr;
	__u64 conn_nid;
	__u32 conn_count;
	__u32 conn_state;
	__u32 conn_vers;
	__u32 conn_pad;

	union {
		struct {
			__u32 tx_buf_size;
			__u32 nagle;
			__u32 peer_ip;
			__u32 peer_port;
			__u32 local_ip;
			__u32 type;
			__u32 cpt;
			__u32 rx_buf_size;
			__u32 pid;
			__u32 pad;
		} socklnd_conn;
		struct {
			__u32 gnd_id;
			__u32 pad;
		} gnilnd_conn;
		struct {
			__u32 path_mtu;
			__u32 queue_type;
			__u32 num_entries;
			__u32 pad;
			struct ioctl_tx_queue tx_q[MAX_NUM_SHOW_ENTRIES];
		} o2iblnd_conn;
	} conn_lnd_u;
};

struct lnet_ioctl_lnet_stats {
	struct libcfs_ioctl_hdr st_hdr;
	struct lnet_counters st_cntrs;
};

#endif /* LNET_DLC_H */
