/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */

#define MAX_STRING_SIZE 128

extern atomic_t ldlm_srv_namespace_nr;
extern atomic_t ldlm_cli_namespace_nr;
extern struct semaphore ldlm_srv_namespace_lock;
extern struct list_head ldlm_srv_namespace_list;
extern struct semaphore ldlm_cli_namespace_lock;
extern struct list_head ldlm_cli_namespace_list;

static inline atomic_t *ldlm_namespace_nr(ldlm_side_t client)
{
        return client == LDLM_NAMESPACE_SERVER ? 
                &ldlm_srv_namespace_nr : &ldlm_cli_namespace_nr;
}

static inline struct list_head *ldlm_namespace_list(ldlm_side_t client)
{
        return client == LDLM_NAMESPACE_SERVER ? 
                &ldlm_srv_namespace_list : &ldlm_cli_namespace_list;
}

static inline struct semaphore *ldlm_namespace_lock(ldlm_side_t client)
{
        return client == LDLM_NAMESPACE_SERVER ? 
                &ldlm_srv_namespace_lock : &ldlm_cli_namespace_lock;
}

/* ldlm_request.c */
typedef enum {
        LDLM_ASYNC,
        LDLM_SYNC,
} ldlm_sync_t;

/* Cancel lru flag, it indicates we cancel aged locks. */
#define LDLM_CANCEL_AGED 0x00000001

int ldlm_cancel_lru(struct ldlm_namespace *ns, int nr, ldlm_sync_t sync);
int ldlm_cancel_lru_local(struct ldlm_namespace *ns, struct list_head *cancels,
                          int count, int max, int flags);

/* ldlm_resource.c */
int ldlm_resource_putref_locked(struct ldlm_resource *res);
void ldlm_resource_insert_lock_after(struct ldlm_lock *original,
                                     struct ldlm_lock *new);

/* ldlm_lock.c */
void ldlm_grant_lock(struct ldlm_lock *lock, struct list_head *work_list);
struct ldlm_lock *
ldlm_lock_create(struct ldlm_namespace *ns, const struct ldlm_res_id *,
                 ldlm_type_t type, ldlm_mode_t, ldlm_blocking_callback,
                 ldlm_completion_callback, ldlm_glimpse_callback, void *data,
                 __u32 lvb_len);
ldlm_error_t ldlm_lock_enqueue(struct ldlm_namespace *, struct ldlm_lock **,
                               void *cookie, int *flags);
void ldlm_lock_addref_internal(struct ldlm_lock *, __u32 mode);
void ldlm_lock_decref_internal(struct ldlm_lock *, __u32 mode);
void ldlm_add_ast_work_item(struct ldlm_lock *lock, struct ldlm_lock *new,
                                struct list_head *work_list);
int ldlm_reprocess_queue(struct ldlm_resource *res, struct list_head *queue,
                         struct list_head *work_list);
int ldlm_run_bl_ast_work(struct list_head *rpc_list);
int ldlm_run_cp_ast_work(struct list_head *rpc_list);
int ldlm_lock_remove_from_lru(struct ldlm_lock *lock);
int ldlm_lock_remove_from_lru_nolock(struct ldlm_lock *lock);
void ldlm_lock_add_to_lru_nolock(struct ldlm_lock *lock);
void ldlm_lock_add_to_lru(struct ldlm_lock *lock);
void ldlm_lock_touch_in_lru(struct ldlm_lock *lock);
void ldlm_lock_destroy_nolock(struct ldlm_lock *lock);

/* ldlm_lockd.c */
int ldlm_bl_to_thread_lock(struct ldlm_namespace *ns, struct ldlm_lock_desc *ld,
                           struct ldlm_lock *lock);
int ldlm_bl_to_thread_list(struct ldlm_namespace *ns, struct ldlm_lock_desc *ld,
                           struct list_head *cancels, int count);

void ldlm_handle_bl_callback(struct ldlm_namespace *ns,
                             struct ldlm_lock_desc *ld, struct ldlm_lock *lock);

/* ldlm_plain.c */
int ldlm_process_plain_lock(struct ldlm_lock *lock, int *flags, int first_enq,
                            ldlm_error_t *err, struct list_head *work_list);

/* ldlm_extent.c */
int ldlm_process_extent_lock(struct ldlm_lock *lock, int *flags, int first_enq,
                             ldlm_error_t *err, struct list_head *work_list);

/* ldlm_flock.c */
int ldlm_process_flock_lock(struct ldlm_lock *req, int *flags, int first_enq,
                            ldlm_error_t *err, struct list_head *work_list);

/* ldlm_inodebits.c */
int ldlm_process_inodebits_lock(struct ldlm_lock *lock, int *flags,
                                int first_enq, ldlm_error_t *err,
                                struct list_head *work_list);

/* l_lock.c */
void l_check_ns_lock(struct ldlm_namespace *ns);
void l_check_no_ns_lock(struct ldlm_namespace *ns);

extern cfs_proc_dir_entry_t *ldlm_svc_proc_dir;
extern cfs_proc_dir_entry_t *ldlm_type_proc_dir;

struct ldlm_state {
        struct ptlrpc_service *ldlm_cb_service;
        struct ptlrpc_service *ldlm_cancel_service;
        struct ptlrpc_client *ldlm_client;
        struct ptlrpc_connection *ldlm_server_conn;
        struct ldlm_bl_pool *ldlm_bl_pool;
};

int ldlm_init(void);
void ldlm_exit(void);

