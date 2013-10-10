/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * Copyright (c) 2013, Intel Corporation, All rights reserved.
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

#ifndef LIBCFS_DLC_H
#define LIBCFS_DLC_H

#include <libcfs/libcfs_ioctl.h>

#define MAX_NUM_SHOW_ENTRIES	32
#define LIBCFS_IOCTL_VERSION2	0x0001000b
#define LIBCFS_MAX_INTERFACES	16
#define LIBCFS_MAX_STR_LEN	128
#define LIBCFS_MAX_SHOW_NUM_CPT	128
#define LIBCFS_NRBPOOLS		3

#define LIBCFS_IOC_INIT_V2(data)			\
do {							\
	memset(&(data), 0, sizeof(data));			\
	(data).hdr.ioc_version = LIBCFS_IOCTL_VERSION2;	\
	(data).hdr.ioc_len = sizeof(data);		\
} while (0)

struct libcfs_ioctl_net_config {
	char ni_interfaces[LIBCFS_MAX_INTERFACES][LIBCFS_MAX_STR_LEN];
	__u32 ni_status;
	__u32 cpts[LIBCFS_MAX_SHOW_NUM_CPT];
};

struct libcfs_ioctl_pool_cfg {
	struct {
		__u32 npages;
		__u32 nbuffers;
		__u32 credits;
		__u32 mincredits;
	} pools[LIBCFS_NRBPOOLS];
	__u32 routing;
};

struct libcfs_ioctl_config_data {
	struct libcfs_ioctl_hdr hdr;

	__u32 ioc_net;
	__u32 ioc_count;
	__u64 ioc_nid;
	__u32 ioc_ncpts;

	union {
		struct {
			__u32 hop;
			__u32 priority;
			__u32 flags;
		} route;
		struct {
			char intf[LIBCFS_MAX_STR_LEN];
			__s32 peer_to;
			__s32 peer_cr;
			__s32 peer_buf_cr;
			__s32 credits;
		} net;
		struct {
			__u32 enable;
			__s32 tiny;
			__s32 small;
			__s32 large;
		} buffers;
	} ioc_config_u;

	char ioc_bulk[0];
};

struct libcfs_ioctl_peer {
	struct libcfs_ioctl_hdr hdr;
	__u32 ioc_count;
	__u32 ioc_pad;
	__u64 ioc_nid;

	union {
		struct {
			char aliveness[LIBCFS_MAX_STR_LEN];
			__u32 refcount;
			__u32 ni_peer_tx_credits;
			__u32 peer_tx_credits;
			__u32 peer_rtr_credits;
			__u32 peer_min_rtr_credtis;
			__u32 peer_tx_qnob;
			__u32 ncpt;
		} peer_credits;
	} lnd_u;
};

struct lnet_counters {
	__u32 msgs_alloc;
	__u32 msgs_max;
	__u32 errors;
	__u32 send_count;
	__u32 recv_count;
	__u32 route_count;
	__u32 drop_count;
	__u64 send_length;
	__u64 recv_length;
	__u64 route_length;
	__u64 drop_length;
} __attribute__((packed));

struct libcfs_ioctl_lnet_stats {
	struct libcfs_ioctl_hdr hdr;
	struct lnet_counters cntrs;
};


#endif /* LIBCFS_DLC_H */
