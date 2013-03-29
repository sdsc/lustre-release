/*
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012, 2013 Intel Corporation
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * This file is part of Lustre/DAOS
 *
 * lustre/include/daos/daos_types.h
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#ifndef __DAOS_TYPES_H__
#define __DAOS_TYPES_H__

typedef uint64_t	daos_off_t;
typedef uint64_t	daos_size_t;

/**
 * generic handle, which can refer to any local data structure (container,
 * object, eq, epoch scope...)
 */
typedef struct {
	uint64_t	cookie;
} daos_handle_t;

typedef struct {
	/** epoch sequence number */
	uint64_t	ep_seq;
} daos_epoch_id_t;

#define DAOS_EPOCH_HCE	{-1}

typedef struct {
	/** epoch scope of current I/O request */
	daos_handle_t	ep_scope;
	/** epoch ID of current I/O request */
	daos_epoch_id_t	ep_eid;
} daos_epoch_t;

/** Event type */
typedef enum {
	/** reserved for parent event */
	DAOS_EV_NONE,
	DAOS_EV_EQ_DESTROY,
	DAOS_EV_SYS_OPEN,
	DAOS_EV_SYS_CLOSE,
	DAOS_EV_SYS_QUERY,
	DAOS_EV_SYS_QUERY_TGT,
	DAOS_EV_CO_OPEN,
	DAOS_EV_CO_CLOSE,
	DAOS_EV_CO_UNLINK,
	DAOS_EV_CO_SNAPSHOT,
	DAOS_EV_G2L,
	DAOS_EV_CO_QUERY,
	DAOS_EV_SHARD_ADD,
	DAOS_EV_SHARD_DISABLE,
	DAOS_EV_SHARD_QUERY,
	DAOS_EV_SHARD_LS_OBJ,
	DAOS_EV_SHARD_FLUSH,
	DAOS_EV_OBJ_OPEN,
	DAOS_EV_OBJ_CLOSE,
	DAOS_EV_OBJ_READ,
	DAOS_EV_OBJ_WRITE,
	DAOS_EV_OBJ_PUNCH,
	DAOS_EV_OBJ_FLUSH,
	DAOS_EV_EPC_OPEN,
	DAOS_EV_EPC_CLOSE,
	DAOS_EV_EP_SLIP,
	DAOS_EV_EP_CATCHUP,
	DAOS_EV_EP_COMMIT,
	DAOS_EV_EP_END,
} daos_ev_type_t;

/**
 * Userspace event structure
 */
typedef struct {
	/** event type */
        daos_ev_type_t		ev_type;
	/**
	 * event status, 0 or error code.
	 */
	int			ev_status;
	/** reserved space for DAOS usage */
	struct {
		uint64_t	space[7];
	}			ev_private;
} daos_event_t;

/** wait for completion event forever */
#define DAOS_EQ_WAIT		-1
/** always return immediately */
#define DAOS_EQ_NOWAIT		0

typedef enum {
	/** query outstanding completed event */
	DAOS_EVQ_COMPLETED	= (1),
	/** query # inflight event */
	DAOS_EVQ_INFLIGHT	= (1 << 1),
	/** query # inflight + completed events in EQ */
	DAOS_EVQ_ALL		= (DAOS_EVQ_COMPLETED | DAOS_EVQ_INFLIGHT),
} daos_ev_query_t;

enum {
	/** target is unknown */
	DAOS_TARGET_ST_UNKNOWN		= -2,
	/** target is disabled */
	DAOS_TARGET_ST_DISABLED		= -1,
	/** 0 - 100 are health levels */
};

typedef struct {
	/** storage type of the target, i.e: SSD... */
	int			ts_type;
	/**
	 * health status of target, i.e:
	 * -2	: unknown
	 * -1	: disabled
	 *  0 - 100 : health levels
	 */
	int			ts_status;
} daos_target_status_t;

typedef struct {
	unsigned int		tev_target;
	daos_target_status_t	tev_status;
} daos_target_event_t;

typedef struct {
	/** shard ID */
	unsigned int		sev_shard;
	/** target ID of this shard */
	unsigned int		sev_target;
	/** status of this target */
	daos_target_status_t	sev_status;
} daos_shard_event_t;

/**
 * DAOS storage tree structure has four layers: cage, rack, node and target
 */
typedef enum {
	DAOS_LOC_TYP_UNKNOWN	= 0,
	DAOS_LOC_TYP_CAGE,
	DAOS_LOC_TYP_RACK,
	DAOS_LOC_TYP_NODE,
	DAOS_LOC_TYP_TARGET,
} daos_loc_type_t;

/**
 * target placement information
 */
#define DAOS_LOC_UNKNOWN	-1

/**
 * location ID of cage/rack/node
 */
typedef struct {
	/** type of this ID: DAOS_LOC_CAGE/RACK/NODE/TARGET */
	daos_loc_type_t		lk_type;
	/** logic ID of cage/rack/node/target */
	unsigned int		lk_id;
} daos_loc_key_t;

/**
 * placement information of DAOS storage tree
 */
typedef struct {
	/** cage number */
	int			lc_cage;
	/** rack number */
	int			lc_rack;
	/** node number */
	int			lc_node;
} daos_location_t;

/**
 * performance metrics for target
 */
typedef struct {
	unsigned int		tp_rdbw; /* MB */
	unsigned int		tp_rdbw_bs;
	unsigned int		tp_wrbw; /* MB */
	unsigned int		tp_wrbw_bs;
	unsigned int		tp_iops;
	unsigned int		tp_iops_bs;
} daos_target_perf_t;

typedef struct {
	/** reserved for target CPU affinity */
	int			ta_cpu_aff;
	/** latency from caller to target (micro-second) */
	uint64_t		ta_latency;
} daos_target_aff_t;

typedef struct {
	/** capacity of the target */
	daos_size_t		ts_size;
	/** free space of target */
	daos_size_t		ts_free;
	/** number of shards in this target */
	unsigned int		ts_nshard;
} daos_target_space_t;

#define DAOS_TARGET_MAX_FAILOVER	4

typedef struct {
	/** location offset of current node in \a ti_failovers */
	unsigned int		tl_current;
	/** number of failover nodes of this target */
	unsigned int		tl_nlocs;
	/** location of failover nodes */
	daos_location_t		tl_failovers[DAOS_TARGET_MAX_FAILOVER];
} daos_target_loc_t;

/**
 * detail information of a target
 */
typedef struct {
	daos_target_loc_t	ti_loc;
	daos_target_status_t	ti_status;
	/** bandwidth information of target */
	daos_target_perf_t	ti_perf;
	daos_target_space_t	ti_space;
	daos_target_aff_t	ti_affinity;
} daos_target_info_t;

/** container open modes */
/** read-only */
#define	DAOS_COO_RO			(1)
/** read-write */
#define DAOS_COO_RW			(1 << 1)
/** create container if it's not existed */
#define DAOS_COO_CREATE			(1 << 2)
/** tell DAOS not to try to hide temporary target failures */
#define DAOS_COO_NOREPLAY		(1 << 3)

/**
 * container information
 */
typedef struct {
	/** user-id of owner */
	uid_t			ci_uid;
	/** group-id of owner */
	gid_t			ci_gid;
	/** the Highest Committed Epoch (HCE) */
	daos_epoch_id_t		ci_hce;
	/** number of shards */
	unsigned int		ci_nshard;
	/** number of disabled shards */
	unsigned int		ci_nshard_disabled;
	/** array to store disabled shards */
	unsigned int		ci_shards_disabled[0];
} daos_container_info_t;

typedef struct {
	/** number of non-empty object */
	daos_size_t		sai_nobjs;
	/** space used */
	daos_size_t		sai_used;
} daos_shard_space_t;

typedef struct {
	unsigned int		sai_target;
	daos_target_status_t	sai_status;
	/** shard location */
	daos_target_loc_t	sai_location;
	/** space infomation of target */
	daos_shard_space_t	sai_space;
	/** TODO: add members */
} daos_shard_info_t;

#define DAOS_LIST_END		0xffffffffffffffffULL

/** object ID */
typedef struct {
	/** baseline DAOS API, it's shard ID (20 bits for DAOS targets) */
	uint64_t	o_id_hi;
	/** baseline DAOS API, it's object ID within shard */
	uint64_t	o_id_lo;
} daos_obj_id_t;

enum {
	/** shared read */
	DAOS_OBJ_RO		= (1 << 1),
	/** shared read & write, no cache for write */
	DAOS_OBJ_RW		= (1 << 2),
	/** exclusive write, data can be cached */
	DAOS_OBJ_EXCL		= (1 << 3),
	/** random I/O */
	DAOS_OBJ_IO_RAND	= (1 << 4),
	/** sequential I/O */
	DAOS_OBJ_IO_SEQ		= (1 << 5),
};

/**
 * DAOS memroy buffer fragment
 */
typedef struct {
	void		*mf_addr;
	daos_size_t	mf_nob;
} daos_mm_frag_t;

/**
 * DAOS memory descriptor, it's an array of daos_iovec_t and it's source
 * of write or target of read
 */
typedef struct {
	unsigned long	mmd_nfrag;
	daos_mm_frag_t	mmd_frag[0];
} daos_mmd_t;

/**
 * IO fragment of a DAOS object
 */
typedef struct {
	daos_off_t	if_offset;
	daos_size_t	if_nob;
} daos_io_frag_t;

/**
 * IO desriptor of a DAOS object, it's an array of daos_io_frag_t and
 * it's target of write or source of read
 */
typedef struct {
	unsigned long	iod_nfrag;
	daos_io_frag_t	iod_frag[0];
} daos_iod_t;

#endif /* __DAOS_TYPES_H__ */
