/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * (visit-tags-table FILE)
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */

#ifndef _LUSTRE_DLM_H__
#define _LUSTRE_DLM_H__

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
#include <lustre_export.h> /* for obd_export, for LDLM_DEBUG */

struct obd_ops;
struct obd_device;

#define OBD_LDLM_DEVICENAME  "ldlm"

#define LDLM_DEFAULT_LRU_SIZE (100 * smp_num_cpus)
#define LDLM_DEFAULT_MAX_ALIVE (cfs_time_seconds(36000))

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
        LDLM_NAMESPACE_SERVER = 0,
        LDLM_NAMESPACE_CLIENT = 1
} ldlm_side_t;

#define LDLM_FL_LOCK_CHANGED   0x000001 /* extent, mode, or resource changed */

/* If the server returns one of these flags, then the lock was put on that list.
 * If the client sends one of these flags (during recovery ONLY!), it wants the
 * lock added to the specified list, no questions asked. -p */
#define LDLM_FL_BLOCK_GRANTED  0x000002
#define LDLM_FL_BLOCK_CONV     0x000004
#define LDLM_FL_BLOCK_WAIT     0x000008

#define LDLM_FL_CBPENDING      0x000010 /* this lock is being destroyed */
#define LDLM_FL_AST_SENT       0x000020 /* blocking or cancel packet was sent */
#define LDLM_FL_WAIT_NOREPROC  0x000040 /* not a real flag, not saved in lock */
#define LDLM_FL_CANCEL         0x000080 /* cancellation callback already run */

/* Lock is being replayed.  This could probably be implied by the fact that one
 * of BLOCK_{GRANTED,CONV,WAIT} is set, but that is pretty dangerous. */
#define LDLM_FL_REPLAY         0x000100

#define LDLM_FL_INTENT_ONLY    0x000200 /* don't grant lock, just do intent */
#define LDLM_FL_LOCAL_ONLY     0x000400 /* see ldlm_cli_cancel_unused */

/* don't run the cancel callback under ldlm_cli_cancel_unused */
#define LDLM_FL_FAILED         0x000800

#define LDLM_FL_HAS_INTENT     0x001000 /* lock request has intent */
#define LDLM_FL_CANCELING      0x002000 /* lock cancel has already been sent */
#define LDLM_FL_LOCAL          0x004000 /* local lock (ie, no srv/cli split) */
#define LDLM_FL_WARN           0x008000 /* see ldlm_cli_cancel_unused */
#define LDLM_FL_DISCARD_DATA   0x010000 /* discard (no writeback) on cancel */

#define LDLM_FL_NO_TIMEOUT     0x020000 /* Blocked by group lock - wait
                                         * indefinitely */

/* file & record locking */
#define LDLM_FL_BLOCK_NOWAIT   0x040000 // server told not to wait if blocked
#define LDLM_FL_TEST_LOCK      0x080000 // return blocking lock

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

#define LDLM_FL_LVB_READY      0x100000

/* A lock contributes to the kms calculation until it has finished the part
 * of it's cancelation that performs write back on its dirty pages.  It
 * can remain on the granted list during this whole time.  Threads racing
 * to update the kms after performing their writeback need to know to
 * exclude each others locks from the calculation as they walk the granted
 * list. */
#define LDLM_FL_KMS_IGNORE     0x200000

/* Don't drop lock covering mmapped file in LRU */
#define LDLM_FL_NO_LRU         0x400000

/* Immediatelly cancel such locks when they block some other locks. Send
   cancel notification to original lock holder, but expect no reply. */
#define LDLM_FL_CANCEL_ON_BLOCK 0x800000

/* Flags flags inherited from parent lock when doing intents. */
#define LDLM_INHERIT_FLAGS     (LDLM_FL_CANCEL_ON_BLOCK)

/* These are flags that are mapped into the flags and ASTs of blocking locks */
#define LDLM_AST_DISCARD_DATA  0x80000000 /* Add FL_DISCARD to blocking ASTs */
/* Flags sent in AST lock_flags to be mapped into the receiving lock. */
#define LDLM_AST_FLAGS         (LDLM_FL_DISCARD_DATA)

/* completion ast to be executed */
#define LDLM_FL_CP_REQD        0x1000000

/* cleanup_resource has already handled the lock */
#define LDLM_FL_CLEANED        0x2000000

/* optimization hint: LDLM can run blocking callback from current context
 * w/o involving separate thread. in order to decrease cs rate */
#define LDLM_FL_ATOMIC_CB      0x4000000

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
#define LDLM_FL_BL_AST          0x10000000
#define LDLM_FL_BL_DONE         0x20000000

/* Cancel lock asynchronously. See ldlm_cli_cancel_unused_resource. */
#define LDLM_FL_ASYNC           0x40000000

/* The blocking callback is overloaded to perform two functions.  These flags
 * indicate which operation should be performed. */
#define LDLM_CB_BLOCKING    1
#define LDLM_CB_CANCELING   2

/* position flag of skip list pointers */
#define LDLM_SL_HEAD(skip_list)   ((skip_list)->next != NULL)
#define LDLM_SL_TAIL(skip_list)   ((skip_list)->prev != NULL)
#define LDLM_SL_EMPTY(skip_list)  ((skip_list)->next == NULL && \
                                   (skip_list)->prev == NULL)

/* compatibility matrix */
#define LCK_COMPAT_EX  LCK_NL
#define LCK_COMPAT_PW  (LCK_COMPAT_EX | LCK_CR)
#define LCK_COMPAT_PR  (LCK_COMPAT_PW | LCK_PR)
#define LCK_COMPAT_CW  (LCK_COMPAT_PW | LCK_CW)
#define LCK_COMPAT_CR  (LCK_COMPAT_CW | LCK_PR | LCK_PW)
#define LCK_COMPAT_NL  (LCK_COMPAT_CR | LCK_EX | LCK_GROUP)
#define LCK_COMPAT_GROUP  (LCK_GROUP | LCK_NL)

extern ldlm_mode_t lck_compat_array[];

static inline void lockmode_verify(ldlm_mode_t mode)
{
       LASSERT(mode > LCK_MINMODE && mode < LCK_MAXMODE);
}

static inline int lockmode_compat(ldlm_mode_t exist, ldlm_mode_t new)
{
       return (lck_compat_array[exist] & new);
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
 *     ns_unused_lock
 *
 * lr_lvb_sem
 *     lr_lock
 *
 */

struct ldlm_pool;
struct ldlm_lock;
struct ldlm_resource;
struct ldlm_namespace;

typedef int (*ldlm_pool_recalc_t)(struct ldlm_pool *pl);
                                  
typedef int (*ldlm_pool_shrink_t)(struct ldlm_pool *pl,
                                  int nr, unsigned int gfp_mask);

enum {
        LDLM_POOL_CTL_RECALC = 1 << 0, /* Pool recalc is enabled */
        LDLM_POOL_CTL_SHRINK = 1 << 1, /* Pool shrink is enabled */
        LDLM_POOL_CTL_FULL   = (LDLM_POOL_CTL_RECALC | LDLM_POOL_CTL_SHRINK)
};

/* One second for pools thread check interval. */
#define LDLM_POOLS_THREAD_PERIOD (1)

/* 5% margin for modest pools. See ldlm_pool.c for details. */
#define LDLM_POOLS_MODEST_MARGIN (5)

/* A change to SLV in % after which we want to wake up pools thread asap. */
#define LDLM_POOLS_FAST_SLV_CHANGE (5)

struct ldlm_pool {
        /* Common pool fields */
        cfs_proc_dir_entry_t  *pl_proc_dir;           /* Pool proc directory. */
        char                   pl_name[100];          /* Pool name, should be long 
                                                       * enough to contain complex 
                                                       * proc entry name. */
        spinlock_t             pl_lock;               /* Lock for protecting slv/clv 
                                                       * updates. */
        atomic_t               pl_limit;              /* Number of allowed locks in
                                                       * in pool, both, client and 
                                                       * server side. */
        atomic_t               pl_granted;            /* Number of granted locks. */
        atomic_t               pl_grant_rate;         /* Grant rate per T. */
        atomic_t               pl_cancel_rate;        /* Cancel rate per T. */
        atomic_t               pl_grant_speed;        /* Grant speed (GR - CR) per T. */
        __u64                  pl_server_lock_volume; /* Server lock volume. Protected 
                                                       * by pl_lock. */
        cfs_time_t             pl_update_time;        /* Time when last slv from server 
                                                       * was obtained. */
        ldlm_pool_recalc_t     pl_recalc;             /* Recalc callback func pointer. */
        ldlm_pool_shrink_t     pl_shrink;             /* Shrink callback func pointer. */
        int                    pl_control;            /* Pool features mask */
        
        /* Server side pool fields */
        atomic_t               pl_grant_plan;         /* Planned number of granted 
                                                       * locks for next T. */
        atomic_t               pl_grant_step;         /* Grant plan step for next T. */

        /* Client side pool related fields */
        atomic_t               pl_lock_volume_factor; /* Lock volume factor. */
        struct lprocfs_stats  *pl_stats;              /* Pool statistics. */
};

static inline int pool_recalc_enabled(struct ldlm_pool *pl)
{
        return pl->pl_control & LDLM_POOL_CTL_RECALC;
}

static inline int pool_shrink_enabled(struct ldlm_pool *pl)
{
        return pl->pl_control & LDLM_POOL_CTL_SHRINK;
}

typedef int (*ldlm_res_policy)(struct ldlm_namespace *, struct ldlm_lock **,
                               void *req_cookie, ldlm_mode_t mode, int flags,
                               void *data);

struct ldlm_valblock_ops {
        int (*lvbo_init)(struct ldlm_resource *res);
        int (*lvbo_update)(struct ldlm_resource *res, struct lustre_msg *m,
                           int buf_idx, int increase);
};

typedef enum {
        LDLM_NAMESPACE_GREEDY = 1 << 0,
        LDLM_NAMESPACE_MODEST = 1 << 1
} ldlm_appetite_t;

struct ldlm_namespace {
        char                  *ns_name;
        ldlm_side_t            ns_client; /* is this a client-side lock tree? */
        __u64                  ns_connect_flags; /* client side connect flags 
                                                  * supported by server */
        struct list_head      *ns_hash; /* hash table for ns */
        spinlock_t             ns_hash_lock;
        __u32                  ns_refcount; /* count of resources in the hash */
        struct list_head       ns_root_list; /* all root resources in ns */
        struct list_head       ns_list_chain; /* position in global NS list */

        struct list_head       ns_unused_list; /* all root resources in ns */
        int                    ns_nr_unused;
        spinlock_t             ns_unused_lock;

        unsigned int           ns_max_unused;
        unsigned int           ns_max_age;
        cfs_time_t             ns_next_dump;   /* next debug dump, jiffies */

        atomic_t               ns_locks;
        __u64                  ns_resources;
        ldlm_res_policy        ns_policy;
        struct ldlm_valblock_ops *ns_lvbo;
        void                  *ns_lvbp;
        cfs_waitq_t            ns_waitq;
        struct ldlm_pool       ns_pool;
        ldlm_appetite_t        ns_appetite;
};

static inline int ns_connect_lru_resize(struct ldlm_namespace *ns)
{
        LASSERT(ns != NULL);
        return ns->ns_connect_flags & OBD_CONNECT_LRU_RESIZE;
}

/*
 *
 * Resource hash table
 *
 */

#define RES_HASH_BITS 10
#define RES_HASH_SIZE (1UL << RES_HASH_BITS)
#define RES_HASH_MASK (RES_HASH_SIZE - 1)

struct ldlm_lock;

typedef int (*ldlm_blocking_callback)(struct ldlm_lock *lock,
                                      struct ldlm_lock_desc *new, void *data,
                                      int flag);
typedef int (*ldlm_completion_callback)(struct ldlm_lock *lock, int flags,
                                        void *data);
typedef int (*ldlm_glimpse_callback)(struct ldlm_lock *lock, void *data);

struct ldlm_lock {
        struct portals_handle l_handle; // must be first in the structure
        atomic_t              l_refc;

        /* internal spinlock protects l_resource.  we should hold this lock 
         * first before grabbing res_lock.*/
        spinlock_t            l_lock;

        /* ldlm_lock_change_resource() can change this */
        struct ldlm_resource *l_resource;

        /* protected by ns_hash_lock. FIXME */
        struct list_head      l_lru;

        /* protected by lr_lock */
        struct list_head      l_res_link; // position in one of three res lists

        struct list_head      l_sl_mode;        // skip pointer for request mode
        struct list_head      l_sl_policy;      // skip pointer for inodebits

        /* protected by led_lock */
        struct list_head      l_export_chain; // per-export chain of locks

        /* protected by lr_lock */
        ldlm_mode_t           l_req_mode;
        ldlm_mode_t           l_granted_mode;

        ldlm_completion_callback l_completion_ast;
        ldlm_blocking_callback   l_blocking_ast;
        ldlm_glimpse_callback    l_glimpse_ast;

        struct obd_export    *l_export;
        struct obd_export    *l_conn_export;

        struct lustre_handle  l_remote_handle;
        ldlm_policy_data_t    l_policy_data;

        /* protected by lr_lock */
        __u32                 l_flags;
        __u32                 l_readers;
        __u32                 l_writers;
        __u8                  l_destroyed;

        /* If the lock is granted, a process sleeps on this waitq to learn when
         * it's no longer in use.  If the lock is not granted, a process sleeps
         * on this waitq to learn when it becomes granted. */
        cfs_waitq_t           l_waitq;
        struct timeval        l_enqueued_time;

        cfs_time_t            l_last_used;      /* jiffies */
        struct ldlm_extent    l_req_extent;

        /* Client-side-only members */
        __u32                 l_lvb_len;        /* temporary storage for */
        void                 *l_lvb_data;       /* an LVB received during */
        void                 *l_lvb_swabber;    /* an enqueue */
        void                 *l_ast_data;

        /* Server-side-only members */

        /* protected by elt_lock */
        struct list_head      l_pending_chain;  /* callbacks pending */
        cfs_time_t            l_callback_timeout; /* jiffies */

        __u32                 l_pid;            /* pid which created this lock */

        /* for ldlm_add_ast_work_item() */
        struct list_head      l_bl_ast;
        struct list_head      l_cp_ast;
        struct ldlm_lock     *l_blocking_lock;
        int                   l_bl_ast_run;
};

struct ldlm_resource {
        struct ldlm_namespace *lr_namespace;

        /* protected by ns_hash_lock */
        struct list_head       lr_hash;
        struct ldlm_resource  *lr_parent;   /* 0 for a root resource */
        struct list_head       lr_children; /* list head for child resources */
        struct list_head       lr_childof;  /* part of ns_root_list if root res,
                                             * part of lr_children if child */
        spinlock_t             lr_lock;

        /* protected by lr_lock */
        struct list_head       lr_granted;
        struct list_head       lr_converting;
        struct list_head       lr_waiting;
        ldlm_mode_t            lr_most_restr;
        ldlm_type_t            lr_type; /* LDLM_{PLAIN,EXTENT,FLOCK} */
        struct ldlm_res_id     lr_name;
        atomic_t               lr_refcount;

        /* Server-side-only lock value block elements */
        struct semaphore       lr_lvb_sem;
        __u32                  lr_lvb_len;
        void                  *lr_lvb_data;
};

struct ldlm_ast_work {
        struct ldlm_lock *w_lock;
        int               w_blocking;
        struct ldlm_lock_desc w_desc;
        struct list_head   w_list;
        int w_flags;
        void *w_data;
        int w_datalen;
};

/* ldlm_enqueue parameters common */
struct ldlm_enqueue_info {
        __u32 ei_type;   /* Type of the lock being enqueued. */
        __u32 ei_mode;   /* Mode of the lock being enqueued. */
        void *ei_cb_bl;  /* Different callbacks for lock handling (blocking, */
        void *ei_cb_cp;  /* completion, glimpse) */
        void *ei_cb_gl;
        void *ei_cbdata; /* Data to be passed into callbacks. */
};

extern struct obd_ops ldlm_obd_ops;

extern char *ldlm_lockname[];
extern char *ldlm_typename[];
extern char *ldlm_it2str(int it);
#ifdef LIBCFS_DEBUG
#define ldlm_lock_debug(cdls, level, lock, file, func, line, fmt, a...) do { \
        CHECK_STACK();                                                  \
                                                                        \
        if (((level) & D_CANTMASK) != 0 ||                              \
            ((libcfs_debug & (level)) != 0 &&                           \
             (libcfs_subsystem_debug & DEBUG_SUBSYSTEM) != 0)) {        \
                static struct libcfs_debug_msg_data _ldlm_dbg_data =    \
                DEBUG_MSG_DATA_INIT(cdls, DEBUG_SUBSYSTEM,              \
                                    file, func, line);                  \
                _ldlm_lock_debug(lock, level, &_ldlm_dbg_data, fmt,     \
                                 ##a );                                 \
        }                                                               \
} while(0)

void _ldlm_lock_debug(struct ldlm_lock *lock, __u32 mask,
                      struct libcfs_debug_msg_data *data, const char *fmt,
                      ...)
        __attribute__ ((format (printf, 4, 5)));

#define LDLM_ERROR(lock, fmt, a...) do {                                \
        static cfs_debug_limit_state_t _ldlm_cdls;                      \
        ldlm_lock_debug(&_ldlm_cdls, D_ERROR, lock,                     \
                        __FILE__, __FUNCTION__, __LINE__,               \
                        "### " fmt , ##a);                              \
} while (0)

#define LDLM_DEBUG(lock, fmt, a...)   do {                              \
        ldlm_lock_debug(NULL, D_DLMTRACE, lock,                         \
                        __FILE__, __FUNCTION__, __LINE__,               \
                         "### " fmt , ##a);                             \
} while (0)
#else
#define LDLM_DEBUG(lock, fmt, a...) ((void)0)
#define LDLM_ERROR(lock, fmt, a...) ((void)0)
#endif

#define LDLM_DEBUG_NOLOCK(format, a...)                 \
        CDEBUG(D_DLMTRACE, "### " format "\n" , ##a)

typedef int (*ldlm_processing_policy)(struct ldlm_lock *lock, int *flags,
                                      int first_enq, ldlm_error_t *err,
                                      struct list_head *work_list);

/*
 * Iterators.
 */

#define LDLM_ITER_CONTINUE 1 /* keep iterating */
#define LDLM_ITER_STOP     2 /* stop iterating */

typedef int (*ldlm_iterator_t)(struct ldlm_lock *, void *);
typedef int (*ldlm_res_iterator_t)(struct ldlm_resource *, void *);

int ldlm_resource_foreach(struct ldlm_resource *res, ldlm_iterator_t iter,
                          void *closure);
int ldlm_namespace_foreach(struct ldlm_namespace *ns, ldlm_iterator_t iter,
                           void *closure);
int ldlm_namespace_foreach_res(struct ldlm_namespace *ns,
                               ldlm_res_iterator_t iter, void *closure);

int ldlm_replay_locks(struct obd_import *imp);
void ldlm_resource_iterate(struct ldlm_namespace *, const struct ldlm_res_id *,
                           ldlm_iterator_t iter, void *data);

/* ldlm_flock.c */
int ldlm_flock_completion_ast(struct ldlm_lock *lock, int flags, void *data);

/* ldlm_extent.c */
__u64 ldlm_extent_shift_kms(struct ldlm_lock *lock, __u64 old_kms);


/* ldlm_lockd.c */
int ldlm_server_blocking_ast(struct ldlm_lock *, struct ldlm_lock_desc *,
                             void *data, int flag);
int ldlm_server_completion_ast(struct ldlm_lock *lock, int flags, void *data);
int ldlm_server_glimpse_ast(struct ldlm_lock *lock, void *data);
int ldlm_handle_enqueue(struct ptlrpc_request *req, ldlm_completion_callback,
                        ldlm_blocking_callback, ldlm_glimpse_callback);
int ldlm_handle_convert(struct ptlrpc_request *req);
int ldlm_handle_cancel(struct ptlrpc_request *req);
int ldlm_request_cancel(struct ptlrpc_request *req,
                        const struct ldlm_request *dlm_req, int first);
int ldlm_del_waiting_lock(struct ldlm_lock *lock);
int ldlm_refresh_waiting_lock(struct ldlm_lock *lock);
void ldlm_revoke_export_locks(struct obd_export *exp);
int ldlm_get_ref(ldlm_side_t client);
void ldlm_put_ref(ldlm_side_t client, int force);

/* ldlm_lock.c */
ldlm_processing_policy ldlm_get_processing_policy(struct ldlm_resource *res);
void ldlm_register_intent(struct ldlm_namespace *ns, ldlm_res_policy arg);
void ldlm_lock2handle(const struct ldlm_lock *lock,
                      struct lustre_handle *lockh);
struct ldlm_lock *__ldlm_handle2lock(const struct lustre_handle *, int flags);
void ldlm_cancel_callback(struct ldlm_lock *);
int ldlm_lock_set_data(struct lustre_handle *, void *data);
int ldlm_lock_remove_from_lru(struct ldlm_lock *);
struct ldlm_lock *ldlm_handle2lock_ns(struct ldlm_namespace *,
                                      const struct lustre_handle *);

static inline struct ldlm_lock *ldlm_handle2lock(const struct lustre_handle *h)
{
        return __ldlm_handle2lock(h, 0);
}

static inline int ldlm_res_lvbo_update(struct ldlm_resource *res,
                                       struct lustre_msg *m, int buf_idx,
                                       int increase)
{
        if (res->lr_namespace->ns_lvbo &&
            res->lr_namespace->ns_lvbo->lvbo_update) {
                return res->lr_namespace->ns_lvbo->lvbo_update(res, m, buf_idx,
                                                               increase);
        }
        return 0;
}

#define LDLM_LOCK_PUT(lock)                     \
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

#define ldlm_lock_list_put(head, member, count)                 \
({                                                              \
        struct ldlm_lock *_lock, *_next;                        \
        int c = count;                                          \
        list_for_each_entry_safe(_lock, _next, head, member) {  \
                list_del_init(&_lock->member);                  \
                LDLM_LOCK_PUT(_lock);                           \
                if (--c == 0)                                   \
                        break;                                  \
        }                                                       \
})

struct ldlm_lock *ldlm_lock_get(struct ldlm_lock *lock);
void ldlm_lock_put(struct ldlm_lock *lock);
void ldlm_lock_destroy(struct ldlm_lock *lock);
void ldlm_lock2desc(struct ldlm_lock *lock, struct ldlm_lock_desc *desc);
void ldlm_lock_addref(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_decref(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_decref_and_cancel(struct lustre_handle *lockh, __u32 mode);
void ldlm_lock_allow_match(struct ldlm_lock *lock);
int ldlm_lock_match(struct ldlm_namespace *ns, int flags,
                    const struct ldlm_res_id *,
                    ldlm_type_t type, ldlm_policy_data_t *, ldlm_mode_t mode,
                    struct lustre_handle *);
struct ldlm_resource *ldlm_lock_convert(struct ldlm_lock *lock, int new_mode,
                                        int *flags);
void ldlm_lock_cancel(struct ldlm_lock *lock);
void ldlm_cancel_locks_for_export(struct obd_export *export);
void ldlm_reprocess_all(struct ldlm_resource *res);
void ldlm_reprocess_all_ns(struct ldlm_namespace *ns);
void ldlm_lock_dump(int level, struct ldlm_lock *lock, int pos);
void ldlm_lock_dump_handle(int level, struct lustre_handle *);
void ldlm_unlink_lock_skiplist(struct ldlm_lock *req);

/* resource.c */
struct ldlm_namespace *ldlm_namespace_new(char *name, ldlm_side_t client, 
                                          ldlm_appetite_t apt);
int ldlm_namespace_cleanup(struct ldlm_namespace *ns, int flags);
int ldlm_namespace_free(struct ldlm_namespace *ns, int force);
void ldlm_namespace_move(struct ldlm_namespace *ns, ldlm_side_t client);
struct ldlm_namespace *ldlm_namespace_first(ldlm_side_t client);
void ldlm_namespace_get(struct ldlm_namespace *ns);
void ldlm_namespace_put(struct ldlm_namespace *ns, int wakeup);
void ldlm_namespace_get_nolock(struct ldlm_namespace *ns);
void ldlm_namespace_put_nolock(struct ldlm_namespace *ns, int wakeup);
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
void ldlm_resource_add_lock(struct ldlm_resource *res, struct list_head *head,
                            struct ldlm_lock *lock);
void ldlm_resource_unlink_lock(struct ldlm_lock *lock);
void ldlm_res2desc(struct ldlm_resource *res, struct ldlm_resource_desc *desc);
void ldlm_dump_all_namespaces(ldlm_side_t client, int level);
void ldlm_namespace_dump(int level, struct ldlm_namespace *);
void ldlm_resource_dump(int level, struct ldlm_resource *);
int ldlm_lock_change_resource(struct ldlm_namespace *, struct ldlm_lock *,
                              const struct ldlm_res_id *);

struct ldlm_callback_suite {
        ldlm_completion_callback lcs_completion;
        ldlm_blocking_callback   lcs_blocking;
        ldlm_glimpse_callback    lcs_glimpse;
};

/* ldlm_request.c */
int ldlm_expired_completion_wait(void *data);
int ldlm_blocking_ast(struct ldlm_lock *lock, struct ldlm_lock_desc *desc,
                      void *data, int flag);
int ldlm_glimpse_ast(struct ldlm_lock *lock, void *reqp);
int ldlm_completion_ast(struct ldlm_lock *lock, int flags, void *data);
int ldlm_cli_enqueue(struct obd_export *exp, struct ptlrpc_request **reqp,
                     struct ldlm_enqueue_info *einfo,
                     const struct ldlm_res_id *res_id,
                     ldlm_policy_data_t *policy, int *flags,
                     void *lvb, __u32 lvb_len, void *lvb_swabber,
                     struct lustre_handle *lockh, int async);
struct ptlrpc_request *ldlm_prep_enqueue_req(struct obd_export *exp,
                                             int bufcount, int *size,
                                             struct list_head *head, int count);
int ldlm_handle_enqueue0(struct ldlm_namespace *ns, struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req,
                         const struct ldlm_callback_suite *cbs);
int ldlm_cli_enqueue_fini(struct obd_export *exp, struct ptlrpc_request *req,
                          ldlm_type_t type, __u8 with_policy, ldlm_mode_t mode,
                          int *flags, void *lvb, __u32 lvb_len,
                          void *lvb_swabber, struct lustre_handle *lockh,
                          int rc);
int ldlm_cli_enqueue_local(struct ldlm_namespace *ns,
                           const struct ldlm_res_id *res_id,
                           ldlm_type_t type, ldlm_policy_data_t *policy,
                           ldlm_mode_t mode, int *flags,
                           ldlm_blocking_callback blocking,
                           ldlm_completion_callback completion,
                           ldlm_glimpse_callback glimpse,
                           void *data, __u32 lvb_len, void *lvb_swabber,
                           struct lustre_handle *lockh);
int ldlm_server_ast(struct lustre_handle *lockh, struct ldlm_lock_desc *new,
                    void *data, __u32 data_len);
int ldlm_cli_convert(struct lustre_handle *, int new_mode, int *flags);
int ldlm_cli_update_pool(struct ptlrpc_request *req);
int ldlm_handle_convert0(struct ptlrpc_request *req,
                         const struct ldlm_request *dlm_req);
int ldlm_cli_cancel(struct lustre_handle *lockh);
int ldlm_cli_cancel_unused(struct ldlm_namespace *, const struct ldlm_res_id *,
                           int flags, void *opaque);
int ldlm_cli_cancel_unused_resource(struct ldlm_namespace *ns,
                                    const struct ldlm_res_id *res_id,
                                    ldlm_policy_data_t *policy,
                                    ldlm_mode_t mode, int flags, void *opaque);
int ldlm_cli_cancel_req(struct obd_export *exp, struct list_head *head,
                        int count, int flags);
int ldlm_cli_join_lru(struct ldlm_namespace *,
                      const struct ldlm_res_id *, int join);
int ldlm_cancel_resource_local(struct ldlm_resource *res,
                               struct list_head *cancels,
                               ldlm_policy_data_t *policy,
                               ldlm_mode_t mode, int lock_flags,
                               int flags, void *opaque);
int ldlm_cli_cancel_list(struct list_head *head, int count,
                         struct ptlrpc_request *req, int off, int flags);
 
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

static inline void lock_res(struct ldlm_resource *res)
{
        spin_lock(&res->lr_lock);
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
int ldlm_pools_init(ldlm_side_t client);
void ldlm_pools_recalc(ldlm_side_t client);
void ldlm_pools_fini(void);
void ldlm_pools_wakeup(void);

int ldlm_pool_init(struct ldlm_pool *pl, struct ldlm_namespace *ns, 
                   int idx, ldlm_side_t client);
int ldlm_pool_shrink(struct ldlm_pool *pl, int nr, 
                     unsigned int gfp_mask);
void ldlm_pool_fini(struct ldlm_pool *pl);
int ldlm_pool_setup(struct ldlm_pool *pl, __u32 limit);
int ldlm_pool_recalc(struct ldlm_pool *pl);
__u64 ldlm_pool_get_slv(struct ldlm_pool *pl);
__u32 ldlm_pool_get_limit(struct ldlm_pool *pl);
void ldlm_pool_set_slv(struct ldlm_pool *pl, __u64 slv);
void ldlm_pool_set_limit(struct ldlm_pool *pl, __u32 limit);
void ldlm_pool_add(struct ldlm_pool *pl, struct ldlm_lock *lock);
void ldlm_pool_del(struct ldlm_pool *pl, struct ldlm_lock *lock);
#endif

