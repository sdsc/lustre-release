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
 * This file is part of DAOS/lustre
 *
 * lustre/daos/daos_internal.h
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#ifndef __DAOS_INTERNAL_H__
#define __DAOS_INTERNAL_H__

#include <asm/uaccess.h>
#include <libcfs/libcfs.h>
#include <obd_support.h>
#include <daos/daos_types.h>
#include <daos/daos_lib.h>

#define DAOS_TEST		1

#define DAOS_HTYPE_BITS		3
#define DAOS_HTYPES_MASK	((1UL << DAOS_HTYPE_BITS) - 1)

typedef enum {
	DAOS_HTYPE_EQ,
	DAOS_HTYPE_EP,
	DAOS_HTYPE_CO,
	DAOS_HTYPE_OBJ,
	DAOS_HTYPE_MAX,
} daos_hlink_type_t;

struct daos_hlink;

struct daos_hlink_ops {
	void			(*hop_free)(struct daos_hlink *hlink);
};

struct daos_hlink {
	cfs_hlist_node_t	hl_link;
	__u64			hl_key;
	unsigned int		hl_ref;
	struct daos_hlink_ops	*hl_ops;
};

int  daos_hhash_initalize(void);
void daos_hhash_finalize(void);
void daos_hlink_init(struct daos_hlink *hlink);
void daos_hlink_insert(daos_hlink_type_t type,
		       struct daos_hlink *hlink, struct daos_hlink_ops *ops);
int  daos_hlink_insert_key(uint64_t key, struct daos_hlink *hlink,
			   struct daos_hlink_ops *ops);
struct daos_hlink *daos_hlink_lookup(uint64_t key);
void daos_hlink_putref_locked(struct daos_hlink *hlink);
void daos_hlink_putref(struct daos_hlink *hlink);
void daos_hlink_delete(struct daos_hlink *hlink);
int  daos_hlink_empty(struct daos_hlink *hlink);
void daos_hlink_key(struct daos_hlink *hlink, uint64_t *key);

int  daos_hhash_initialize(void);
void daos_hhash_finalize(void);

/**
 * DAOS event status
 */
typedef enum {
	/** intial status, it's also status of event after it's been polled */
	DAOS_EVS_NONE,
	/** event status after called daos_event_dispatch */
	DAOS_EVS_DISPATCH,
	/** abort inflight operation */
	DAOS_EVS_ABORT,
	/** event status after called daos_event_complete/abort */
	DAOS_EVS_COMPLETE,
} daos_ev_status_t;

/**
 * one million events per EQ in case application forgot to finalize
 * event and consume all memory
 */
#define DAOS_EQ_EVN_DEF		(1 << 20)
#define DAOS_EQ_EVN_MAX		(DAOS_EQ_EVN_DEF << 2)

/** kernel space event queue */
struct daos_eq {
	spinlock_t		eq_lock;
	/** link chain in global handle table */
	struct daos_hlink	eq_hlink;
	struct list_head	eq_flink;
	wait_queue_head_t	eq_waitq;
	unsigned int		eq_error;
	/** # events in this EQ */
	unsigned int		eq_total;
	/** # completed events in this EQ */
	unsigned int		eq_compn;
	/**
	 * # dispatched events in this EQ, it's decreased when status of
	 * event is changed from DAOS_EVS_DISPATCH to DAOS_EVS_COMPLETE
	 */
	unsigned int		eq_dispn;
	/** is there any process waiting for ev_ops::op_complete() */
	unsigned int		eq_pollwait:30;
	unsigned int		eq_finalizing:1;
	__u64			eq_cookie;
	struct list_head	eq_idle;
	/**
	 * inflight & aborted event list, aborted events are always
	 * on the tail of this list
	 */
	struct list_head	eq_disp;
	struct list_head	eq_comp;
	/**
	 * events in progress of polling, giving them a dedicated list because
	 * multiple threads can poll on a same EQ and we don't want them to
	 * see the same event, so events in polling will be moved from
	 * eq::eq_disp to eq::eq_comp.
	 */
	struct list_head	eq_poll;
	cfs_hash_t		*eq_hash;
};

/** kernel space event */
struct daos_kevent {
	/** unique event ID within EQ domain */
	__u64			ev_cookie;
	/** hash chain on EQ */
	cfs_hlist_node_t	ev_hnode;
	/** status of event */
	daos_ev_status_t	ev_status;
	/** error code of this event, +ve */
	int			ev_error;
	/** refcount, +1 for link to EQ hash, dispatch or poll */
	int			ev_ref;
	/** # of child events */
	int			ev_childn;
	/** # of completed child events */
	int			ev_childn_comp;
	/** list head of child events */
	struct list_head	ev_children;
	/** link chain on EQ or parent */
	struct list_head	ev_link;
	/** callbacks for completion and abort */
	daos_kevent_ops_t	*ev_ops;
	/** pointer to event queue */
	struct daos_eq		*ev_eq;
	/** pointer to parent event */
	struct daos_kevent	*ev_parent;
	/** user space  parameter for completion callback */
	void			*ev_uparam;
	/** kernel space parameter for completion callback */
	void			*ev_kparam;
	/** userspace event */
	daos_event_t		__user *ev_uevent;
};

struct daos_dev_env;

int daos_eq_create(struct daos_dev_env *denv, struct daos_usr_eq_create *eqc);
int daos_eq_destroy(struct daos_dev_env *denv, struct daos_usr_eq_destroy *eqd);
void daos_eq_handle(struct daos_eq *eq, daos_handle_t *h);
struct daos_eq *daos_eq_lookup(daos_handle_t eqh);
void daos_eq_putref(struct daos_eq *eq);
int daos_event_init(struct daos_dev_env *denv, struct daos_usr_ev_init *evi);
int daos_event_fini(struct daos_dev_env *denv, struct daos_usr_ev_fini *evf);

/**
 * DAOS API parameters, it has refcount because DAOS APIs are asynchronous
 * and parameters can be freed by a different thread from different module
 */
struct daos_usr_param {
	atomic_t		up_refcount;
	unsigned int		up_len;
	char			up_data[0];
};

static inline void
daos_usr_param_get(struct daos_usr_param *uparam)
{
	atomic_inc(&uparam->up_refcount);
}

static inline void
daos_usr_param_put(struct daos_usr_param *uparam)
{
	if (atomic_dec_and_test(&uparam->up_refcount))
		OBD_FREE(uparam, uparam->up_len);
}

static inline void *
daos_usr_param_buf(struct daos_usr_param *uparam, int idx)
{
	return ((struct daos_usr_data *)&uparam->up_data[0])->ud_inbufs[idx];
}

long daos_eq_ioctl(struct daos_dev_env *denv, unsigned int cmd,
		   struct daos_usr_param *uparam);

struct daos_module_params {
	/** # events per EQ */
	int			*dmp_eq_eventn;
	/** allow event abort */
	int			*dmp_can_abort;
};

struct daos_dev_env {
	spinlock_t		den_lock;
	/** EQ list */
	struct list_head	den_eqs;
};

struct daos_module {
	spinlock_t		dm_lock;
	int			dm_dev_registered:1;
	cfs_hash_t		*dm_hhash;
#if DAOS_TEST
	int			dm_schedn;
	struct cfs_wi_sched	**dm_scheds;
#endif
};

extern struct daos_module		the_daos;
extern struct daos_module_params	the_daos_params;

#endif /* __DAOS_INTERNAL_H__ */
