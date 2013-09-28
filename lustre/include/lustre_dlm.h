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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2010, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _LUSTRE_DLM_H__
#define _LUSTRE_DLM_H__

/** \defgroup ldlm ldlm
 *
 * @{
 */

#if defined(__linux__)
#include <linux/lustre_dlm.h>
#elif defined(__APPLE__)
#include <darwin/lustre_dlm.h>
#elif defined(__WINNT__)
#include <winnt/lustre_dlm.h>
#else
#error Unsupported operating system.
#endif

#include <lustre_lib.h>
#include <lustre_net.h>
#include <lustre_import.h>
#include <lustre_handles.h>
#include <interval_tree.h> /* for interval_node{}, ldlm_extent */
#include <lu_ref.h>

struct obd_ops;
struct obd_device;

#define OBD_LDLM_DEVICENAME  "ldlm"

#ifdef HAVE_BGL_SUPPORT
/* 1.5 times the maximum 128 tasks available in VN mode */
#define LDLM_DEFAULT_LRU_SIZE 196
#else
#define LDLM_DEFAULT_LRU_SIZE (100 * cfs_num_online_cpus())
#endif
#define LDLM_DEFAULT_MAX_ALIVE (cfs_time_seconds(36000))
#define LDLM_CTIME_AGE_LIMIT (10)
#define LDLM_DEFAULT_PARALLEL_AST_LIMIT 1024

typedef enum {
        ELDLM_OK = 0,

        ELDLM_LOCK_CHANGED = 300,
        ELDLM_LOCK_ABORTED = 301,
        ELDLM_LOCK_REPLACED = 302,
        ELDLM_NO_LOCK_DATA = 303,

        ELDLM_NAMESPACE_EXISTS = 400,
        ELDLM_BAD_NAMESPACE    = 401
} ldlm_error_t;

typedef enum {
        LDLM_NAMESPACE_SERVER = 1 << 0,
        LDLM_NAMESPACE_CLIENT = 1 << 1
} ldlm_side_t;

/**
 * Declaration of flags sent through the wire.
 **/
#define LDLM_FL_LOCK_CHANGED   0x000001 /* extent, mode, or resource changed */

/* If the server returns one of these flags, then the lock was put on that list.
 * If the client sends one of these flags (during recovery ONLY!), it wants the
 * lock added to the specified list, no questions asked. -p */
#define LDLM_FL_BLOCK_GRANTED  0x000002
#define LDLM_FL_BLOCK_CONV     0x000004
#define LDLM_FL_BLOCK_WAIT     0x000008

/* Used to be LDLM_FL_CBPENDING 0x000010 moved to non-wire flags */

#define LDLM_FL_AST_SENT       0x000020 /* blocking or cancel packet was
                                         * queued for sending. */
/* Used to be LDLM_FL_WAIT_NOREPROC 0x000040   moved to non-wire flags */
/* Used to be LDLM_FL_CANCEL        0x000080   moved to non-wire flags */

/* Lock is being replayed.  This could probably be implied by the fact that one
 * of BLOCK_{GRANTED,CONV,WAIT} is set, but that is pretty dangerous. */
#define LDLM_FL_REPLAY         0x000100

#define LDLM_FL_INTENT_ONLY    0x000200 /* don't grant lock, just do intent */

/* Used to be LDLM_FL_LOCAL_ONLY 0x000400  moved to non-wire flags */
/* Used to be LDLM_FL_FAILED     0x000800  moved to non-wire flags */

#define LDLM_FL_HAS_INTENT     0x001000 /* lock request has intent */

/* Used to be LDLM_FL_CANCELING  0x002000  moved to non-wire flags */
/* Used to be LDLM_FL_LOCAL      0x004000  moved to non-wire flags */

#define LDLM_FL_DISCARD_DATA   0x010000 /* discard (no writeback) on cancel */

#define LDLM_FL_NO_TIMEOUT     0x020000 /* Blocked by group lock - wait
                                         * indefinitely */

/* file & record locking */
#define LDLM_FL_BLOCK_NOWAIT   0x040000 /* server told not to wait if blocked.
                                         * For AGL, OST will not send glimpse
                                         * callback. */
#define LDLM_FL_TEST_LOCK      0x080000 // return blocking lock

/* Used to be LDLM_FL_LVB_READY  0x100000 moved to non-wire flags */
/* Used to be LDLM_FL_KMS_IGNORE 0x200000 moved to non-wire flags */
/* Used to be LDLM_FL_NO_LRU     0x400000 moved to non-wire flags */

/* Immediatelly cancel such locks when they block some other locks. Send
 * cancel notification to original lock holder, but expect no reply. This is
 * for clients (like liblustre) that cannot be expected to reliably response
 * to blocking ast. */
#define LDLM_FL_CANCEL_ON_BLOCK 0x800000

/* Flags flags inherited from parent lock when doing intents. */
#define LDLM_INHERIT_FLAGS     (LDLM_FL_CANCEL_ON_BLOCK)

/* Used to be LDLM_FL_CP_REQD        0x1000000 moved to non-wire flags */
/* Used to be LDLM_FL_CLEANED        0x2000000 moved to non-wire flags */
/* Used to be LDLM_FL_ATOMIC_CB      0x4000000 moved to non-wire flags */
/* Used to be LDLM_FL_BL_AST         0x10000000 moved to non-wire flags */
/* Used to be LDLM_FL_BL_DONE        0x20000000 moved to non-wire flags */

/* measure lock contention and return -EUSERS if locking contention is high */
#define LDLM_FL_DENY_ON_CONTENTION 0x40000000

/* These are flags that are mapped into the flags and ASTs of blocking locks */
#define LDLM_AST_DISCARD_DATA  0x80000000 /* Add FL_DISCARD to blocking ASTs */

/* Flags sent in AST lock_flags to be mapped into the receiving lock. */
#define LDLM_AST_FLAGS         (LDLM_FL_DISCARD_DATA)

/*
 * --------------------------------------------------------------------------
 * NOTE! Starting from this point, that is, LDLM_FL_* flags with values above
 * 0x80000000 will not be sent over the wire.
 * --------------------------------------------------------------------------
 */

/**
 * Declaration of flags not sent through the wire.
 **/
/* Used for marking lock as an target for -EINTR while cp_ast sleep
 * emulation + race with upcoming bl_ast.  */
#define LDLM_FL_FAIL_LOC       0x100000000ULL
/* Used while processing the unused list to know that we have already
 * handled this lock and decided to skip it */
#define LDLM_FL_SKIPPED        0x200000000ULL
/* this lock is being destroyed */
#define LDLM_FL_CBPENDING      0x400000000ULL
/* not a real flag, not saved in lock */
#define LDLM_FL_WAIT_NOREPROC  0x800000000ULL
/* cancellation callback already run */
#define LDLM_FL_CANCEL         0x1000000000ULL
#define LDLM_FL_LOCAL_ONLY     0x2000000000ULL
/* don't run the cancel callback under ldlm_cli_cancel_unused */
#define LDLM_FL_FAILED         0x4000000000ULL
/* lock cancel has already been sent */
#define LDLM_FL_CANCELING      0x8000000000ULL
/* local lock (ie, no srv/cli split) */
#define LDLM_FL_LOCAL          0x10000000000ULL
/* XXX FIXME: This is being added to b_size as a low-risk fix to the fact that
 * the LVB filling happens _after_ the lock has been granted, so another thread
 * can match`t before the LVB has been updated.  As a dirty hack, we set
 * LDLM_FL_LVB_READY only after we've done the LVB poop.
 * this is only needed on lov/osc now, where lvb is actually used and callers
 * must set it in input flags.
 *
 * The proper fix is to do the granting inside of the completion AST, which can
 * be replaced with a LVB-aware wrapping function for OSC locks.  That change is
 * pretty high-risk, though, and would need a lot more testing. */
#define LDLM_FL_LVB_READY      0x20000000000ULL
/* A lock contributes to the kms calculation until it has finished the part
 * of it's cancelation that performs write back on its dirty pages.  It
 * can remain on the granted list during this whole time.  Threads racing
 * to update the kms after performing their writeback need to know to
 * exclude each others locks from the calculation as they walk the granted
 * list. */
#define LDLM_FL_KMS_IGNORE     0x40000000000ULL
/* completion ast to be executed */
#define LDLM_FL_CP_REQD        0x80000000000ULL
/* cleanup_resource has already handled the lock */
#define LDLM_FL_CLEANED        0x100000000000ULL
/* optimization hint: LDLM can run blocking callback from current context
 * w/o involving separate thread. in order to decrease cs rate */
#define LDLM_FL_ATOMIC_CB      0x200000000000ULL
/* It may happen that a client initiate 2 operations, e.g. unlink and mkdir,
 * such that server send blocking ast for conflict locks to this client for
 * the 1st operation, whereas the 2nd operation has canceled this lock and
 * is waiting for rpc_lock which is taken by the 1st operation.
 * LDLM_FL_BL_AST is to be set by ldlm_callback_handler() to the lock not allow
 * ELC code to cancel it.
 * LDLM_FL_BL_DONE is to be set by ldlm_cancel_callback() when lock cache is
 * droped to let ldlm_callback_handler() return EINVAL to the server. It is
 * used when ELC rpc is already prepared and is waiting for rpc_lock, too late
 * to send a separate CANCEL rpc. */
#define LDLM_FL_BL_AST          0x400000000000ULL
#define LDLM_FL_BL_DONE         0x800000000000ULL
/* Don't put lock into the LRU list, so that it is not canceled due to aging.
 * Used by MGC locks, they are cancelled only at unmount or by callback. */
#define LDLM_FL_NO_LRU		0x1000000000000ULL


/* The blocking callback is overloaded to perform two functions.  These flags
 * indicate which operation should be performed. */
#define LDLM_CB_BLOCKING    1
#define LDLM_CB_CANCELING   2

/* compatibility matrix */
#define LCK_COMPAT_EX  LCK_NL
#define LCK_COMPAT_PW  (LCK_COMPAT_EX | LCK_CR)
#define LCK_COMPAT_PR  (LCK_COMPAT_PW | LCK_PR)
#define LCK_COMPAT_CW  (LCK_COMPAT_PW | LCK_CW)
#define LCK_COMPAT_CR  (LCK_COMPAT_CW | LCK_PR | LCK_PW)
#define LCK_COMPAT_NL  (LCK_COMPAT_CR | LCK_EX | LCK_GROUP)
#define LCK_COMPAT_GROUP  (LCK_GROUP | LCK_NL)
#define LCK_COMPAT_COS (LCK_COS)

extern ldlm_mode_t lck_compat_array[];

static inline void lockmode_verify(ldlm_mode_t mode)
{
       LASSERT(mode > LCK_MINMODE && mode < LCK_MAXMODE);
}

static inline int lockmode_compat(ldlm_mode_t exist_mode, ldlm_mode_t new_mode)
{
       return (lck_compat_array[exist_mode] & new_mode);
}

/*
 *
 * cluster name spaces
 *
 */

#define DLM_OST_NAMESPACE 1
#define DLM_MDS_NAMESPACE 2

/* XXX
   - do we just separate this by security domains and use a prefix for
     multiple namespaces in the same domain?
   -
*/

/*
 * Locking rules:
 *
 * lr_lock
 *
 * lr_lock
 *     waiting_locks_spinlock
 *
 * lr_lock
 *     led_lock
 *
 * lr_lock
 *     ns_lock
 *
 * lr_lvb_mutex
 *     lr_lock
 *
 */

struct ldlm_pool;
struct ldlm_lock;
struct ldlm_resource;
struct ldlm_namespace;

struct ldlm_pool_ops {
        int (*po_recalc)(struct ldlm_pool *pl);
        int (*po_shrink)(struct ldlm_pool *pl, int nr,
                         unsigned int gfp_mask);
        int (*po_setup)(struct ldlm_pool *pl, int limit);
};

/**
 * One second for pools thread check interval. Each pool has own period.
 */
#define LDLM_POOLS_THREAD_PERIOD (1)

/**
 * ~6% margin for modest pools. See ldlm_pool.c for details.
 */
#define LDLM_POOLS_MODEST_MARGIN_SHIFT (4)

/**
 * Default recalc period for server side pools in sec.
 */
#define LDLM_POOL_SRV_DEF_RECALC_PERIOD (1)

/**
 * Default recalc period for client side pools in sec.
 */
#define LDLM_POOL_CLI_DEF_RECALC_PERIOD (10)

struct ldlm_pool {
        /**
         * Pool proc directory.
         */
        cfs_proc_dir_entry_t  *pl_proc_dir;
        /**
         * Pool name, should be long enough to contain compound proc entry name.
         */
        char                   pl_name[100];
        /**
         * Lock for protecting slv/clv updates.
         */
	spinlock_t		pl_lock;
        /**
         * Number of allowed locks in in pool, both, client and server side.
         */
        cfs_atomic_t           pl_limit;
        /**
         * Number of granted locks in
         */
        cfs_atomic_t           pl_granted;
        /**
         * Grant rate per T.
         */
        cfs_atomic_t           pl_grant_rate;
        /**
         * Cancel rate per T.
         */
        cfs_atomic_t           pl_cancel_rate;
        /**
         * Server lock volume. Protected by pl_lock.
         */
        __u64                  pl_server_lock_volume;
        /**
         * Current biggest client lock volume. Protected by pl_lock.
         */
        __u64                  pl_client_lock_volume;
        /**
         * Lock volume factor. SLV on client is calculated as following:
         * server_slv * lock_volume_factor.
         */
        cfs_atomic_t           pl_lock_volume_factor;
        /**
         * Time when last slv from server was obtained.
         */
        time_t                 pl_recalc_time;
        /**
          * Recalc period for pool.
          */
        time_t                 pl_recalc_period;
        /**
         * Recalc and shrink ops.
         */
        struct ldlm_pool_ops  *pl_ops;
        /**
         * Number of planned locks for next period.
         */
        int                    pl_grant_plan;
        /**
         * Pool statistics.
         */
        struct lprocfs_stats  *pl_stats;
};

typedef int (*ldlm_res_policy)(struct ldlm_namespace *, struct ldlm_lock **,
			       void *req_cookie, ldlm_mode_t mode, __u64 flags,
			       void *data);

typedef int (*ldlm_cancel_for_recovery)(struct ldlm_lock *lock);

struct ldlm_valblock_ops {
        int (*lvbo_init)(struct ldlm_resource *res);
        int (*lvbo_update)(struct ldlm_resource *res,
                           struct ptlrpc_request *r,
                           int increase);
        int (*lvbo_free)(struct ldlm_resource *res);
	/* Return size of lvb data appropriate RPC size can be reserved */
	int (*lvbo_size)(struct ldlm_lock *lock);
	/* Called to fill in lvb data to RPC buffer @buf */
	int (*lvbo_fill)(struct ldlm_lock *lock, void *buf, int buflen);
};

typedef enum {
        LDLM_NAMESPACE_GREEDY = 1 << 0,
        LDLM_NAMESPACE_MODEST = 1 << 1
} ldlm_appetite_t;

/*
 * Default values for the "max_nolock_size", "contention_time" and
 * "contended_locks" namespace tunables.
 */
#define NS_DEFAULT_MAX_NOLOCK_BYTES 0
#define NS_DEFAULT_CONTENTION_SECONDS 2
#define NS_DEFAULT_CONTENDED_LOCKS 32

struct ldlm_ns_bucket {
        /** refer back */
        struct ldlm_namespace      *nsb_namespace;
        /** estimated lock callback time */
        struct adaptive_timeout     nsb_at_estimate;
};

enum {
        /** ldlm namespace lock stats */
        LDLM_NSS_LOCKS          = 0,
        LDLM_NSS_LAST
};

typedef enum {
        /** invalide type */
        LDLM_NS_TYPE_UNKNOWN    = 0,
        /** mdc namespace */
        LDLM_NS_TYPE_MDC,
        /** mds namespace */
        LDLM_NS_TYPE_MDT,
        /** osc namespace */
        LDLM_NS_TYPE_OSC,
        /** ost namespace */
        LDLM_NS_TYPE_OST,
        /** mgc namespace */
        LDLM_NS_TYPE_MGC,
        /** mgs namespace */
        LDLM_NS_TYPE_MGT,
} ldlm_ns_type_t;

struct ldlm_namespace {
        /**
         * Backward link to obd, required for ldlm pool to store new SLV.
         */
        struct obd_device     *ns_obd;

        /**
         * Is this a client-side lock tree?
         */
        ldlm_side_t            ns_client;

        /**
         * resource hash
         */
        cfs_hash_t            *ns_rs_hash;

        /**
         * serialize
         */
	spinlock_t		ns_lock;

        /**
         * big refcount (by bucket)
         */
        cfs_atomic_t           ns_bref;

        /**
         * Namespce connect flags supported by server (may be changed via proc,
         * lru resize may be disabled/enabled).
         */
        __u64                  ns_connect_flags;

         /**
          * Client side orig connect flags supported by server.
          */
        __u64                  ns_orig_connect_flags;

        /**
         * Position in global namespace list.
         */
        cfs_list_t             ns_list_chain;

        /**
         * All root resources in namespace.
         */
        cfs_list_t             ns_unused_list;
        int                    ns_nr_unused;

        unsigned int           ns_max_unused;
        unsigned int           ns_max_age;
        unsigned int           ns_timeouts;
         /**
          * Seconds.
          */
        unsigned int           ns_ctime_age_limit;

        /**
         * Next debug dump, jiffies.
         */
        cfs_time_t             ns_next_dump;

        ldlm_res_policy        ns_policy;
        struct ldlm_valblock_ops *ns_lvbo;
        void                  *ns_lvbp;
        cfs_waitq_t            ns_waitq;
        struct ldlm_pool       ns_pool;
        ldlm_appetite_t        ns_appetite;

        /**
         * If more than \a ns_contended_locks found, the resource is considered
         * to be contended.
         */
        unsigned               ns_contended_locks;

        /**
         * The resource remembers contended state during \a ns_contention_time,
         * in seconds.
         */
        unsigned               ns_contention_time;

        /**
         * Limit size of nolock requests, in bytes.
         */
        unsigned               ns_max_nolock_size;

        /**
         * Limit of parallel AST RPC count.
         */
        unsigned               ns_max_parallel_ast;

        /* callback to cancel locks before replaying it during recovery */
        ldlm_cancel_for_recovery ns_cancel_for_recovery;
        /**
         * ldlm lock stats
         */
        struct lprocfs_stats  *ns_stats;

        unsigned               ns_stopping:1;   /* namespace cleanup */
};

static inline int ns_is_client(struct ldlm_namespace *ns)
{
        LASSERT(ns != NULL);
        LASSERT(!(ns->ns_client & ~(LDLM_NAMESPACE_CLIENT |
                                    LDLM_NAMESPACE_SERVER)));
        LASSERT(ns->ns_client == LDLM_NAMESPACE_CLIENT ||
                ns->ns_client == LDLM_NAMESPACE_SERVER);
        return ns->ns_client == LDLM_NAMESPACE_CLIENT;
}

static inline int ns_is_server(struct ldlm_namespace *ns)
{
        LASSERT(ns != NULL);
        LASSERT(!(ns->ns_client & ~(LDLM_NAMESPACE_CLIENT |
                                    LDLM_NAMESPACE_SERVER)));
        LASSERT(ns->ns_client == LDLM_NAMESPACE_CLIENT ||
                ns->ns_client == LDLM_NAMESPACE_SERVER);
        return ns->ns_client == LDLM_NAMESPACE_SERVER;
}

static inline int ns_connect_cancelset(struct ldlm_namespace *ns)
{
	LASSERT(ns != NULL);
	return !!(ns->ns_connect_flags & OBD_CONNECT_CANCELSET);
}

static inline int ns_connect_lru_resize(struct ldlm_namespace *ns)
{
        LASSERT(ns != NULL);
        return !!(ns->ns_connect_flags & OBD_CONNECT_LRU_RESIZE);
}

static inline void ns_register_cancel(struct ldlm_namespace *ns,
                                      ldlm_cancel_for_recovery arg)
{
        LASSERT(ns != NULL);
        ns->ns_cancel_for_recovery = arg;
}

struct ldlm_lock;

typedef int (*ldlm_blocking_callback)(struct ldlm_lock *lock,
                                      struct ldlm_lock_desc *new, void *data,
                                      int flag);
typedef int (*ldlm_completion_callback)(struct ldlm_lock *lock, __u64 flags,
					void *data);
typedef int (*ldlm_glimpse_callback)(struct ldlm_lock *lock, void *data);
typedef unsigned long (*ldlm_weigh_callback)(struct ldlm_lock *lock);

struct ldlm_glimpse_work {
	struct ldlm_lock	*gl_lock; /* lock to glimpse */
	cfs_list_t		 gl_list; /* linkage to other gl work structs */
	__u32			 gl_flags;/* see LDLM_GL_WORK_* below */
	union ldlm_gl_desc	*gl_desc; /* glimpse descriptor to be packed in
					   * glimpse callback request */
};

/* the ldlm_glimpse_work is allocated on the stack and should not be freed */
#define LDLM_GL_WORK_NOFREE 0x1

/* Interval node data for each LDLM_EXTENT lock */
struct ldlm_interval {
        struct interval_node li_node;   /* node for tree mgmt */
        cfs_list_t           li_group;  /* the locks which have the same
                                         * policy - group of the policy */
};
#define to_ldlm_interval(n) container_of(n, struct ldlm_interval, li_node)

/* the interval tree must be accessed inside the resource lock. */
struct ldlm_interval_tree {
        /* tree size, this variable is used to count
         * granted PW locks in ldlm_extent_policy()*/
        int                   lit_size;
        ldlm_mode_t           lit_mode; /* lock mode */
        struct interval_node *lit_root; /* actually ldlm_interval */
};

#define LUSTRE_TRACKS_LOCK_EXP_REFS (0)

/* Cancel flag. */
typedef enum {
        LCF_ASYNC      = 0x1, /* Cancel locks asynchronously. */
        LCF_LOCAL      = 0x2, /* Cancel locks locally, not notifing server */
        LCF_BL_AST     = 0x4, /* Cancel locks marked as LDLM_FL_BL_AST
                               * in the same RPC */
} ldlm_cancel_flags_t;

struct ldlm_flock {
        __u64 start;
        __u64 end;
        __u64 owner;
        __u64 blocking_owner;
        struct obd_export *blocking_export;
	/* Protected by the hash lock */
	__u32 blocking_refs;
        __u32 pid;
};

typedef union {
        struct ldlm_extent l_extent;
        struct ldlm_flock l_flock;
        struct ldlm_inodebits l_inodebits;
} ldlm_policy_data_t;

void ldlm_convert_policy_to_wire(ldlm_type_t type,
                                 const ldlm_policy_data_t *lpolicy,
                                 ldlm_wire_policy_data_t *wpolicy);
void ldlm_convert_policy_to_local(struct obd_export *exp, ldlm_type_t type,
                                  const ldlm_wire_policy_data_t *wpolicy,
                                  ldlm_policy_data_t *lpolicy);

enum lvb_type {
	LVB_T_NONE	= 0,
	LVB_T_OST	= 1,
	LVB_T_LQUOTA	= 2,
	LVB_T_LAYOUT	= 3,
};

struct ldlm_lock {
        /**
         * Must be first in the structure.
         */
        struct portals_handle    l_handle;
        /**
         * Lock reference count.
         */
        cfs_atomic_t             l_refc;
        /**
         * Internal spinlock protects l_resource.  we should hold this lock
         * first before grabbing res_lock.
         */
	spinlock_t		l_lock;
        /**
         * ldlm_lock_change_resource() can change this.
         */
        struct ldlm_resource    *l_resource;
        /**
         * Protected by ns_hash_lock. List item for client side lru list.
         */
        cfs_list_t               l_lru;
        /**
         * Protected by lr_lock, linkage to resource's lock queues.
         */
        cfs_list_t               l_res_link;
        /**
         * Tree node for ldlm_extent.
         */
        struct ldlm_interval    *l_tree_node;
        /**
         * Protected by per-bucket exp->exp_lock_hash locks. Per export hash
         * of locks.
         */
        cfs_hlist_node_t         l_exp_hash;
        /**
         * Protected by lr_lock. Requested mode.
         */
	/**
	 * Protected by per-bucket exp->exp_flock_hash locks. Per export hash
	 * of locks.
	 */
	cfs_hlist_node_t         l_exp_flock_hash;

        ldlm_mode_t              l_req_mode;
        /**
         * Granted mode, also protected by lr_lock.
         */
        ldlm_mode_t              l_granted_mode;
        /**
         * Lock enqueue completion handler.
         */
        ldlm_completion_callback l_completion_ast;
        /**
         * Lock blocking ast handler.
         */
        ldlm_blocking_callback   l_blocking_ast;
        /**
         * Lock glimpse handler.
         */
        ldlm_glimpse_callback    l_glimpse_ast;
        ldlm_weigh_callback      l_weigh_ast;

        /**
         * Lock export.
         */
        struct obd_export       *l_export;
        /**
         * Lock connection export.
         */
        struct obd_export       *l_conn_export;

        /**
         * Remote lock handle.
         */
        struct lustre_handle     l_remote_handle;

        ldlm_policy_data_t       l_policy_data;

        /*
         * Protected by lr_lock. Various counters: readers, writers, etc.
         */
        __u64                 l_flags;
        __u32                 l_readers;
        __u32                 l_writers;
        /**
         * If the lock is granted, a process sleeps on this waitq to learn when
         * it's no longer in use.  If the lock is not granted, a process sleeps
         * on this waitq to learn when it becomes granted.
         */
        cfs_waitq_t           l_waitq;

        /** 
         * Seconds. it will be updated if there is any activity related to 
         * the lock, e.g. enqueue the lock or send block AST.
         */
        cfs_time_t            l_last_activity;

        /**
         * Jiffies. Should be converted to time if needed.
         */
        cfs_time_t            l_last_used;

        struct ldlm_extent    l_req_extent;

        unsigned int          l_failed:1,
        /*
         * Set for locks that were removed from class hash table and will be
         * destroyed when last reference to them is released. Set by
         * ldlm_lock_destroy_internal().
         *
         * Protected by lock and resource locks.
         */
                              l_destroyed:1,
	/*
	 * it's set in lock_res_and_lock() and unset in unlock_res_and_lock().
	 *
	 * NB: compare with check_res_locked(), check this bit is cheaper,
	 * also, spin_is_locked() is deprecated for kernel code, one reason is
	 * because it works only for SMP so user needs add extra macros like
	 * LASSERT_SPIN_LOCKED for uniprocessor kernels.
	 */
			      l_res_locked:1,
	/*
	 * it's set once we call ldlm_add_waiting_lock_res_locked()
	 * to start the lock-timeout timer and it will never be reset.
	 *
	 * Protected by lock_res_and_lock().
	 */
			      l_waited:1,
        /**
         * flag whether this is a server namespace lock.
         */
                              l_ns_srv:1;

        /*
         * Client-side-only members.
         */

	enum lvb_type	      l_lvb_type;
        /**
         * Temporary storage for an LVB received during an enqueue operation.
         */
        __u32                 l_lvb_len;
        void                 *l_lvb_data;

        void                 *l_ast_data;

        /*
         * Server-side-only members.
         */

        /** connection cookie for the client originated the operation. */
        __u64                 l_client_cookie;

        /**
         * Protected by elt_lock. Callbacks pending.
         */
        cfs_list_t            l_pending_chain;

        cfs_time_t            l_callback_timeout;

        /**
         * Pid which created this lock.
         */
        __u32                 l_pid;

        int                   l_bl_ast_run;
        /**
         * For ldlm_add_ast_work_item().
         */
        cfs_list_t            l_bl_ast;
        /**
         * For ldlm_add_ast_work_item().
         */
        cfs_list_t            l_cp_ast;
        /**
         * For ldlm_add_ast_work_item().
         */
        cfs_list_t            l_rk_ast;

        struct ldlm_lock     *l_blocking_lock;

        /**
         * Protected by lr_lock, linkages to "skip lists".
         */
        cfs_list_t            l_sl_mode;
        cfs_list_t            l_sl_policy;
        struct lu_ref         l_reference;
#if LUSTRE_TRACKS_LOCK_EXP_REFS
        /* Debugging stuff for bug 20498, for tracking export
           references. */
        /** number of export references taken */
        int                   l_exp_refs_nr;
        /** link all locks referencing one export */
        cfs_list_t            l_exp_refs_link;
        /** referenced export object */
        struct obd_export    *l_exp_refs_target;
#endif
        /** export blocking dlm lock list, protected by
         * l_export->exp_bl_list_lock.
         * Lock order of waiting_lists_spinlock, exp_bl_list_lock and res lock
         * is: res lock -> exp_bl_list_lock -> wanting_lists_spinlock. */
        cfs_list_t            l_exp_list;
};

struct ldlm_resource {
	struct ldlm_ns_bucket	*lr_ns_bucket;

	/* protected by ns_hash_lock */
	cfs_hlist_node_t	lr_hash;
	spinlock_t		lr_lock;

        /* protected by lr_lock */
        cfs_list_t             lr_granted;
        cfs_list_t             lr_converting;
        cfs_list_t             lr_waiting;
        ldlm_mode_t            lr_most_restr;
        ldlm_type_t            lr_type; /* LDLM_{PLAIN,EXTENT,FLOCK} */
        struct ldlm_res_id     lr_name;
        cfs_atomic_t           lr_refcount;

        struct ldlm_interval_tree lr_itree[LCK_MODE_NUM];  /* interval trees*/

        /* Server-side-only lock value block elements */
        /** to serialize lvbo_init */
	struct mutex		lr_lvb_mutex;
        __u32                  lr_lvb_len;
        /** protect by lr_lock */
        void                  *lr_lvb_data;

        /* when the resource was considered as contended */
        cfs_time_t             lr_contention_time;
        /**
         * List of references to this resource. For debugging.
         */
        struct lu_ref          lr_reference;

        struct inode          *lr_lvb_inode;
};

static inline char *
ldlm_ns_name(struct ldlm_namespace *ns)
{
        return ns->ns_rs_hash->hs_name;
}

static inline struct ldlm_namespace *
ldlm_res_to_ns(struct ldlm_resource *res)
{
        return res->lr_ns_bucket->nsb_namespace;
}

static inline struct ldlm_namespace *
ldlm_lock_to_ns(struct ldlm_lock *lock)
{
        return ldlm_res_to_ns(lock->l_resource);
}

static inline char *
ldlm_lock_to_ns_name(struct ldlm_lock *lock)
{
        return ldlm_ns_name(ldlm_lock_to_ns(lock));
}

static inline struct adaptive_timeout *
ldlm_lock_to_ns_at(struct ldlm_lock *lock)
{
        return &lock->l_resource->lr_ns_bucket->nsb_at_estimate;
}

static inline int ldlm_lvbo_init(struct ldlm_resource *res)
{
	struct ldlm_namespace *ns = ldlm_res_to_ns(res);

	if (ns->ns_lvbo != NULL && ns->ns_lvbo->lvbo_init != NULL)
		return ns->ns_lvbo->lvbo_init(res);

	return 0;
}

static inline int ldlm_lvbo_size(struct ldlm_lock *lock)
{
	struct ldlm_namespace *ns = ldlm_lock_to_ns(lock);

	if (ns->ns_lvbo != NULL && ns->ns_lvbo->lvbo_size != NULL)
		return ns->ns_lvbo->lvbo_size(lock);

	return 0;
}

static inline int ldlm_lvbo_fill(struct ldlm_lock *lock, void *buf, int len)
{
	struct ldlm_namespace *ns = ldlm_lock_to_ns(lock);

	if (ns->ns_lvbo != NULL) {
		LASSERT(ns->ns_lvbo->lvbo_fill != NULL);
		return ns->ns_lvbo->lvbo_fill(lock, buf, len);
	}
	return 0;
}

struct ldlm_ast_work {
        struct ldlm_lock      *w_lock;
        int                    w_blocking;
        struct ldlm_lock_desc  w_desc;
        cfs_list_t             w_list;
        int                    w_flags;
        void                  *w_data;
        int                    w_datalen;
};

/* ldlm_enqueue parameters common */
struct ldlm_enqueue_info {
        __u32 ei_type;   /* Type of the lock being enqueued. */
        __u32 ei_mode;   /* Mode of the lock being enqueued. */
        void *ei_cb_bl;  /* blocking lock callback */
        void *ei_cb_cp;  /* lock completion callback */
        void *ei_cb_gl;  /* lock glimpse callback */
        void *ei_cb_wg;  /* lock weigh callback */
        void *ei_cbdata; /* Data to be passed into callbacks. */
};

extern struct obd_ops ldlm_obd_ops;

extern char *ldlm_lockname[];
extern char *ldlm_typename[];
extern char *ldlm_it2str(int it);

#define LDLM_DEBUG_NOLOCK(format, a...)                 \
        CDEBUG(D_DLMTRACE, "### " format "\n" , ##a)

#ifdef LIBCFS_DEBUG
#define ldlm_lock_debug(msgdata, mask, cdls, lock, fmt, a...) do {      \
        CFS_CHECK_STACK(msgdata, mask, cdls);                           \
                                                                        \
        if (((mask) & D_CANTMASK) != 0 ||                               \
            ((libcfs_debug & (mask)) != 0 &&                            \
             (libcfs_subsystem_debug & DEBUG_SUBSYSTEM) != 0))          \
                _ldlm_lock_debug(lock, msgdata, fmt, ##a);              \
} while(0)

void _ldlm_lock_debug(struct ldlm_lock *lock,
                      struct libcfs_debug_msg_data *data,
                      const char *fmt, ...)
        __attribute__ ((format (printf, 3, 4)));

#define LDLM_DEBUG_LIMIT(mask, lock, fmt, a...) do {                         \
        static cfs_debug_limit_state_t _ldlm_cdls;                           \
        LIBCFS_DEBUG_MSG_DATA_DECL(msgdata, mask, &_ldlm_cdls);              \
        ldlm_lock_debug(&msgdata, mask, &_ldlm_cdls, lock, "### " fmt , ##a);\
} while (0)

#define LDLM_ERROR(lock, fmt, a...) LDLM_DEBUG_LIMIT(D_ERROR, lock, fmt, ## a)
#define LDLM_WARN(lock, fmt, a...)  LDLM_DEBUG_LIMIT(D_WARNING, lock, fmt, ## a)

#define LDLM_DEBUG(lock, fmt, a...)   do {                                  \
	if (likely(lock != NULL)) {					    \
		LIBCFS_DEBUG_MSG_DATA_DECL(msgdata, D_DLMTRACE, NULL);      \
		ldlm_lock_debug(&msgdata, D_DLMTRACE, NULL, lock, 	    \
				"### " fmt , ##a);			    \
	} else {							    \
		LDLM_DEBUG_NOLOCK("no dlm lock: " fmt, ##a);		    \
	}								    \
} while (0)
#else /* !LIBCFS_DEBUG */
# define LDLM_DEBUG_LIMIT(mask, lock, fmt, a...) ((void)0)
# define LDLM_DEBUG(lock, fmt, a...) ((void)0)
# define LDLM_ERROR(lock, fmt, a...) ((void)0)
#endif

typedef int (*ldlm_processing_policy)(struct ldlm_lock *lock, __u64 *flags,
                                      int first_enq, ldlm_error_t *err,
                                      cfs_list_t *work_list);

/*
 * Iterators.
 */

#define LDLM_ITER_CONTINUE 1 /* keep iterating */
#define LDLM_ITER_STOP     2 /* stop iterating */

typedef int (*ldlm_iterator_t)(struct ldlm_lock *, void *);
typedef int (*ldlm_res_iterator_t)(struct ldlm_resource *, void *);

int ldlm_resource_foreach(struct ldlm_resource *res, ldlm_iterator_t iter,
                          void *closure);
void ldlm_namespace_foreach(struct ldlm_namespace *ns, ldlm_iterator_t iter,
                            void *closure);

int ldlm_replay_locks(struct obd_import *imp);
int ldlm_resource_iterate(struct ldlm_namespace *, const struct ldlm_res_id *,
                           ldlm_iterator_t iter, void *data);

/* ldlm_flock.c */
int ldlm_flock_completion_ast(struct ldlm_lock *lock, __u64 flags, void *data);

/* ldlm_extent.c */
__u64 ldlm_extent_shift_kms(struct ldlm_lock *lock, __u64 old_kms);

struct ldlm_callback_suite {
        ldlm_completion_callback lcs_completion;
        ldlm_blocking_callback   lcs_blocking;
        ldlm_glimpse_callback    lcs_glimpse;
        ldlm_weigh_callback      lcs_weigh;
};

/* ldlm_lockd.c */
#ifdef HAVE_SERVER_SUPPORT
int ldlm_server_blocking_ast(struct ldlm_lock *, struct ldlm_lock_desc *,
                             void *data, int flag);
int ldlm_server_completion_ast(struct ldlm_lock *lock, __u64 flags, void *data);
int ldlm_server_glimpse_ast(struct ldlm_lock *lock, void *data);
int ldlm_glimpse_locks(struct ldlm_resource *res, cfs_list_t *gl_work_list);
int ldlm_handle_enqueue(struct ptlrpc_request *req, ldlm_completion_callback,
                        ldlm_blocking_callback, ldlm_glimpse_callback);
int ldlm_handle_enqueue0(struct ldlm_namespace *ns, struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req,
                         const struct ldlm_callback_suite *cbs);
int ldlm_handle_convert(struct ptlrpc_request *req);
int ldlm_handle_convert0(struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req);
int ldlm_handle_cancel(struct ptlrpc_request *req);
int ldlm_request_cancel(struct ptlrpc_request *req,
                        const struct ldlm_request *dlm_req, int first);
void ldlm_revoke_export_locks(struct obd_export *exp);
#endif
int ldlm_del_waiting_lock(struct ldlm_lock *lock);
int ldlm_refresh_waiting_lock(struct ldlm_lock *lock, int timeout);
int ldlm_get_ref(void);
void ldlm_put_ref(void);
int ldlm_init_export(struct obd_export *exp);
void ldlm_destroy_export(struct obd_export *exp);
struct ldlm_lock *ldlm_request_lock(struct ptlrpc_request *req);

/* ldlm_lock.c */
#ifdef HAVE_SERVER_SUPPORT
ldlm_processing_policy ldlm_get_processing_policy(struct ldlm_resource *res);
#endif
void ldlm_register_intent(struct ldlm_namespace *ns, ldlm_res_policy arg);
void ldlm_lock2handle(const struct ldlm_lock *lock,
                      struct lustre_handle *lockh);
struct ldlm_lock *__ldlm_handle2lock(const struct lustre_handle *, __u64 flags);
void ldlm_cancel_callback(struct ldlm_lock *);
int ldlm_lock_remove_from_lru(struct ldlm_lock *);
int ldlm_lock_set_data(struct lustre_handle *, void *);

static inline struct ldlm_lock *ldlm_handle2lock(const struct lustre_handle *h)
{
        return __ldlm_handle2lock(h, 0);
}

#define LDLM_LOCK_REF_DEL(lock) \
        lu_ref_del(&lock->l_reference, "handle", cfs_current())

static inline struct ldlm_lock *
ldlm_handle2lock_long(const struct lustre_handle *h, __u64 flags)
{
        struct ldlm_lock *lock;

        lock = __ldlm_handle2lock(h, flags);
        if (lock != NULL)
                LDLM_LOCK_REF_DEL(lock);
        return lock;
}

static inline int ldlm_res_lvbo_update(struct ldlm_resource *res,
                                       struct ptlrpc_request *r, int increase)
{
        if (ldlm_res_to_ns(res)->ns_lvbo &&
            ldlm_res_to_ns(res)->ns_lvbo->lvbo_update) {
                return ldlm_res_to_ns(res)->ns_lvbo->lvbo_update(res, r,
                                                                 increase);
        }
        return 0;
}

int ldlm_error2errno(ldlm_error_t error);
ldlm_error_t ldlm_errno2error(int err_no); /* don't call it `errno': this
                                            * confuses user-space. */
#if LUSTRE_TRACKS_LOCK_EXP_REFS
void ldlm_dump_export_locks(struct obd_export *exp);
#endif

/**
 * Release a temporary lock reference obtained by ldlm_handle2lock() or
 * __ldlm_handle2lock().
 */
#define LDLM_LOCK_PUT(lock)                     \
do {                                            \
        LDLM_LOCK_REF_DEL(lock);                \
        /*LDLM_DEBUG((lock), "put");*/          \
        ldlm_lock_put(lock);                    \
} while (0)

/**
 * Release a lock reference obtained by some other means (see
 * LDLM_LOCK_PUT()).
 */
#define LDLM_LOCK_RELEASE(lock)                 \
do {                                            \
        /*LDLM_DEBUG((lock), "put");*/          \
        ldlm_lock_put(lock);                    \
} while (0)

#define LDLM_LOCK_GET(lock)                     \
({                                              \
        ldlm_lock_get(lock);                    \
        /*LDLM_DEBUG((lock), "get");*/          \
        lock;                                   \
})

#define ldlm_lock_list_put(head, member, count)                     \
({                                                                  \
        struct ldlm_lock *_lock, *_next;                            \
        int c = count;                                              \
        cfs_list_for_each_entry_safe(_lock, _next, head, member) {  \
                if (c-- == 0)                                       \
                        break;                                      \
                cfs_list_del_init(&_lock->member);                  \
                LDLM_LOCK_RELEASE(_lock);                           \
        }                                                           \
        LASSERT(c <= 0);                                            \
})

struct ldlm_lock *ldlm_lock_get(struct ldlm_lock *lock);
void ldlm_lock_put(struct ldlm_lock *lock);
void ldlm_lock_destroy(struct ldlm_lock *lock);
void ldlm_lock2desc(struct ldlm_lock *lock, struct ldlm_lock_desc *desc);
void ldlm_lock_addref(struct lustre_handle *lockh, __u32 mode);
int  ldlm_lock_addref_try(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_decref(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_decref_and_cancel(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_fail_match_locked(struct ldlm_lock *lock);
void ldlm_lock_fail_match(struct ldlm_lock *lock);
void ldlm_lock_allow_match(struct ldlm_lock *lock);
void ldlm_lock_allow_match_locked(struct ldlm_lock *lock);
ldlm_mode_t ldlm_lock_match(struct ldlm_namespace *ns, __u64 flags,
                            const struct ldlm_res_id *, ldlm_type_t type,
                            ldlm_policy_data_t *, ldlm_mode_t mode,
                            struct lustre_handle *, int unref);
ldlm_mode_t ldlm_revalidate_lock_handle(struct lustre_handle *lockh,
                                        __u64 *bits);
struct ldlm_resource *ldlm_lock_convert(struct ldlm_lock *lock, int new_mode,
                                        __u32 *flags);
void ldlm_lock_downgrade(struct ldlm_lock *lock, int new_mode);
void ldlm_lock_cancel(struct ldlm_lock *lock);
void ldlm_reprocess_all(struct ldlm_resource *res);
void ldlm_reprocess_all_ns(struct ldlm_namespace *ns);
void ldlm_lock_dump_handle(int level, struct lustre_handle *);
void ldlm_unlink_lock_skiplist(struct ldlm_lock *req);

/* resource.c */
struct ldlm_namespace *
ldlm_namespace_new(struct obd_device *obd, char *name,
                   ldlm_side_t client, ldlm_appetite_t apt,
                   ldlm_ns_type_t ns_type);
int ldlm_namespace_cleanup(struct ldlm_namespace *ns, __u64 flags);
void ldlm_namespace_free(struct ldlm_namespace *ns,
                         struct obd_import *imp, int force);
void ldlm_namespace_register(struct ldlm_namespace *ns, ldlm_side_t client);
void ldlm_namespace_unregister(struct ldlm_namespace *ns, ldlm_side_t client);
void ldlm_namespace_move_locked(struct ldlm_namespace *ns, ldlm_side_t client);
struct ldlm_namespace *ldlm_namespace_first_locked(ldlm_side_t client);
void ldlm_namespace_get(struct ldlm_namespace *ns);
void ldlm_namespace_put(struct ldlm_namespace *ns);
int ldlm_proc_setup(void);
#ifdef LPROCFS
void ldlm_proc_cleanup(void);
#else
static inline void ldlm_proc_cleanup(void) {}
#endif

/* resource.c - internal */
struct ldlm_resource *ldlm_resource_get(struct ldlm_namespace *ns,
                                        struct ldlm_resource *parent,
                                        const struct ldlm_res_id *,
                                        ldlm_type_t type, int create);
struct ldlm_resource *ldlm_resource_getref(struct ldlm_resource *res);
int ldlm_resource_putref(struct ldlm_resource *res);
void ldlm_resource_add_lock(struct ldlm_resource *res,
                            cfs_list_t *head,
                            struct ldlm_lock *lock);
void ldlm_resource_unlink_lock(struct ldlm_lock *lock);
void ldlm_res2desc(struct ldlm_resource *res, struct ldlm_resource_desc *desc);
void ldlm_dump_all_namespaces(ldlm_side_t client, int level);
void ldlm_namespace_dump(int level, struct ldlm_namespace *);
void ldlm_resource_dump(int level, struct ldlm_resource *);
int ldlm_lock_change_resource(struct ldlm_namespace *, struct ldlm_lock *,
                              const struct ldlm_res_id *);

#define LDLM_RESOURCE_ADDREF(res) do {                                  \
        lu_ref_add_atomic(&(res)->lr_reference, __FUNCTION__, cfs_current());  \
} while (0)

#define LDLM_RESOURCE_DELREF(res) do {                                  \
        lu_ref_del(&(res)->lr_reference, __FUNCTION__, cfs_current());  \
} while (0)

/* ldlm_request.c */
int ldlm_expired_completion_wait(void *data);
int ldlm_blocking_ast_nocheck(struct ldlm_lock *lock);
int ldlm_blocking_ast(struct ldlm_lock *lock, struct ldlm_lock_desc *desc,
                      void *data, int flag);
int ldlm_glimpse_ast(struct ldlm_lock *lock, void *reqp);
int ldlm_completion_ast_async(struct ldlm_lock *lock, __u64 flags, void *data);
int ldlm_completion_ast(struct ldlm_lock *lock, __u64 flags, void *data);
int ldlm_cli_enqueue(struct obd_export *exp, struct ptlrpc_request **reqp,
                     struct ldlm_enqueue_info *einfo,
                     const struct ldlm_res_id *res_id,
		     ldlm_policy_data_t const *policy, __u64 *flags,
		     void *lvb, __u32 lvb_len, enum lvb_type lvb_type,
		     struct lustre_handle *lockh, int async);
int ldlm_prep_enqueue_req(struct obd_export *exp,
                          struct ptlrpc_request *req,
                          cfs_list_t *cancels,
                          int count);
int ldlm_prep_elc_req(struct obd_export *exp,
                      struct ptlrpc_request *req,
                      int version, int opc, int canceloff,
                      cfs_list_t *cancels, int count);

struct ptlrpc_request *ldlm_enqueue_pack(struct obd_export *exp, int lvb_len);
int ldlm_handle_enqueue0(struct ldlm_namespace *ns, struct ptlrpc_request *req,
			 const struct ldlm_request *dlm_req,
			 const struct ldlm_callback_suite *cbs);
int ldlm_cli_enqueue_fini(struct obd_export *exp, struct ptlrpc_request *req,
                          ldlm_type_t type, __u8 with_policy, ldlm_mode_t mode,
			  __u64 *flags, void *lvb, __u32 lvb_len,
                          struct lustre_handle *lockh, int rc);
int ldlm_cli_enqueue_local(struct ldlm_namespace *ns,
                           const struct ldlm_res_id *res_id,
                           ldlm_type_t type, ldlm_policy_data_t *policy,
			   ldlm_mode_t mode, __u64 *flags,
                           ldlm_blocking_callback blocking,
                           ldlm_completion_callback completion,
                           ldlm_glimpse_callback glimpse,
			   void *data, __u32 lvb_len, enum lvb_type lvb_type,
                           const __u64 *client_cookie,
                           struct lustre_handle *lockh);
int ldlm_server_ast(struct lustre_handle *lockh, struct ldlm_lock_desc *new,
                    void *data, __u32 data_len);
int ldlm_cli_convert(struct lustre_handle *, int new_mode, __u32 *flags);
int ldlm_cli_update_pool(struct ptlrpc_request *req);
int ldlm_cli_cancel(struct lustre_handle *lockh);
int ldlm_cli_cancel_unused(struct ldlm_namespace *, const struct ldlm_res_id *,
                           ldlm_cancel_flags_t flags, void *opaque);
int ldlm_cli_cancel_unused_resource(struct ldlm_namespace *ns,
                                    const struct ldlm_res_id *res_id,
                                    ldlm_policy_data_t *policy,
                                    ldlm_mode_t mode,
                                    ldlm_cancel_flags_t flags,
                                    void *opaque);
int ldlm_cli_cancel_req(struct obd_export *exp, cfs_list_t *head,
                        int count, ldlm_cancel_flags_t flags);
int ldlm_cancel_resource_local(struct ldlm_resource *res,
                               cfs_list_t *cancels,
                               ldlm_policy_data_t *policy,
                               ldlm_mode_t mode, int lock_flags,
                               ldlm_cancel_flags_t cancel_flags, void *opaque);
int ldlm_cli_cancel_list_local(cfs_list_t *cancels, int count,
                               ldlm_cancel_flags_t flags);
int ldlm_cli_cancel_list(cfs_list_t *head, int count,
                         struct ptlrpc_request *req, ldlm_cancel_flags_t flags);

/* mds/handler.c */
/* This has to be here because recursive inclusion sucks. */
int intent_disposition(struct ldlm_reply *rep, int flag);
void intent_set_disposition(struct ldlm_reply *rep, int flag);


/* ioctls for trying requests */
#define IOC_LDLM_TYPE                   'f'
#define IOC_LDLM_MIN_NR                 40

#define IOC_LDLM_TEST                   _IOWR('f', 40, long)
#define IOC_LDLM_DUMP                   _IOWR('f', 41, long)
#define IOC_LDLM_REGRESS_START          _IOWR('f', 42, long)
#define IOC_LDLM_REGRESS_STOP           _IOWR('f', 43, long)
#define IOC_LDLM_MAX_NR                 43

/**
 * "Modes" of acquiring lock_res, necessary to tell lockdep that taking more
 * than one lock_res is dead-lock safe.
 */
enum lock_res_type {
        LRT_NORMAL,
        LRT_NEW
};

static inline void lock_res(struct ldlm_resource *res)
{
	spin_lock(&res->lr_lock);
}

static inline void lock_res_nested(struct ldlm_resource *res,
                                   enum lock_res_type mode)
{
	spin_lock_nested(&res->lr_lock, mode);
}

static inline void unlock_res(struct ldlm_resource *res)
{
	spin_unlock(&res->lr_lock);
}

static inline void check_res_locked(struct ldlm_resource *res)
{
        LASSERT_SPIN_LOCKED(&res->lr_lock);
}

struct ldlm_resource * lock_res_and_lock(struct ldlm_lock *lock);
void unlock_res_and_lock(struct ldlm_lock *lock);

/* ldlm_pool.c */
void ldlm_pools_recalc(ldlm_side_t client);
int ldlm_pools_init(void);
void ldlm_pools_fini(void);

int ldlm_pool_init(struct ldlm_pool *pl, struct ldlm_namespace *ns,
                   int idx, ldlm_side_t client);
int ldlm_pool_shrink(struct ldlm_pool *pl, int nr,
                     unsigned int gfp_mask);
void ldlm_pool_fini(struct ldlm_pool *pl);
int ldlm_pool_setup(struct ldlm_pool *pl, int limit);
int ldlm_pool_recalc(struct ldlm_pool *pl);
__u32 ldlm_pool_get_lvf(struct ldlm_pool *pl);
__u64 ldlm_pool_get_slv(struct ldlm_pool *pl);
__u64 ldlm_pool_get_clv(struct ldlm_pool *pl);
__u32 ldlm_pool_get_limit(struct ldlm_pool *pl);
void ldlm_pool_set_slv(struct ldlm_pool *pl, __u64 slv);
void ldlm_pool_set_clv(struct ldlm_pool *pl, __u64 clv);
void ldlm_pool_set_limit(struct ldlm_pool *pl, __u32 limit);
void ldlm_pool_add(struct ldlm_pool *pl, struct ldlm_lock *lock);
void ldlm_pool_del(struct ldlm_pool *pl, struct ldlm_lock *lock);

/** @} ldlm */

#endif
