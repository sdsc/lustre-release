/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *   This file is part of Lustre, http://www.lustre.org
 *
 * MDC internal definitions.
 */

#include <lustre_mds.h>
void mdc_pack_req_body(struct ptlrpc_request *req, int offset,
                       __u64 valid, struct ll_fid *fid, int ea_size, int flags);
void mdc_pack_rep_body(struct ptlrpc_request *);
void mdc_readdir_pack(struct ptlrpc_request *req, int offset, __u64 pg_off,
		      __u32 size, struct ll_fid *mdc_fid);
void mdc_getattr_pack(struct ptlrpc_request *req, int offset, int valid,
                      int flags, struct mdc_op_data *data);
void mdc_setattr_pack(struct ptlrpc_request *req, int offset,
                      struct mdc_op_data *data,
                      struct iattr *iattr, void *ea, int ealen,
                      void *ea2, int ea2len);
void mdc_create_pack(struct ptlrpc_request *req, int offset,
                     struct mdc_op_data *op_data, const void *data, int datalen,
                     __u32 mode, __u32 uid, __u32 gid, __u32 cap_effective,
                     __u64 rdev);
void mdc_open_pack(struct ptlrpc_request *req, int offset,
                   struct mdc_op_data *op_data, __u32 mode, __u64 rdev,
                   __u32 flags, const void *data, int datalen);
void mdc_join_pack(struct ptlrpc_request *req, int offset,
                   struct mdc_op_data *op_data, __u64 head_size);
void mdc_unlink_pack(struct ptlrpc_request *req, int offset,
                     struct mdc_op_data *data);
void mdc_link_pack(struct ptlrpc_request *req, int offset,
                   struct mdc_op_data *data);
void mdc_rename_pack(struct ptlrpc_request *req, int offset,
                     struct mdc_op_data *data,
                     const char *old, int oldlen, const char *new, int newlen);
void mdc_close_pack(struct ptlrpc_request *req, int offset, struct obdo *oa,
                    int valid, struct obd_client_handle *och);
void mdc_exit_request(struct client_obd *cli);
void mdc_enter_request(struct client_obd *cli);

struct mdc_open_data {
        struct obd_client_handle *mod_och;
        struct ptlrpc_request    *mod_open_req;
        struct ptlrpc_request    *mod_close_req;
};

struct mdc_rpc_lock {
        struct semaphore rpcl_sem;
        struct lookup_intent *rpcl_it;
};

static inline void mdc_init_rpc_lock(struct mdc_rpc_lock *lck)
{
        sema_init(&lck->rpcl_sem, 1);
        lck->rpcl_it = NULL;
}

static inline void mdc_get_rpc_lock(struct mdc_rpc_lock *lck,
                                    struct lookup_intent *it)
{
        ENTRY;
        if (!it || (it->it_op != IT_GETATTR && it->it_op != IT_LOOKUP)) {
                down(&lck->rpcl_sem);
                LASSERT(lck->rpcl_it == NULL);
                lck->rpcl_it = it;
        }
}

static inline void mdc_put_rpc_lock(struct mdc_rpc_lock *lck,
                                    struct lookup_intent *it)
{
        if (!it || (it->it_op != IT_GETATTR && it->it_op != IT_LOOKUP)) {
                LASSERT(it == lck->rpcl_it);
                lck->rpcl_it = NULL;
                up(&lck->rpcl_sem);
        }
        EXIT;
}
