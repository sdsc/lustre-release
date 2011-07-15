/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Liang Zhen   <liang@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lu_object.h>
#include "ptlrpc_internal.h"

/* NRS */
static ptlrpc_nrs_policy_t *
nrs_policy_find_nolock(ptlrpc_nrs_head_t *nrs_hd, int type)
{
        ptlrpc_nrs_policy_t *tmp;

        nrs_for_each_policy(tmp, nrs_hd) {
                if (type == tmp->nrs_type)
                        return tmp;
        }
        return NULL;
}

static int
nrs_policy_init(ptlrpc_nrs_policy_t *policy, void *arg)
{
        int     rc = 0;

        if (policy->nrs_ops->op_policy_init)
                rc = policy->nrs_ops->op_policy_init(policy, arg);

        return rc;
}

static void
nrs_policy_fini(ptlrpc_nrs_policy_t *policy)
{
        if (policy->nrs_ops->op_policy_fini)
                policy->nrs_ops->op_policy_fini(policy);
}

static int
nrs_policy_ctl(ptlrpc_nrs_head_t *nrs_hd,
               int type, unsigned int cmd, void *arg)
{
        ptlrpc_nrs_policy_t *policy;
        int                 *ptr = (int *)arg;
        int                  rc = 0;

        cfs_spin_lock(&nrs_hd->nh_service->srv_rq_lock);

        policy = nrs_policy_find_nolock(nrs_hd, type);
        if (policy == NULL) {
                rc = -ENOENT;
                goto out;
        }

        switch (cmd) {
        default:
                cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);

                rc = policy->nrs_ops->op_policy_ctl != NULL ?
                     policy->nrs_ops->op_policy_ctl(policy, cmd, arg) : -ENOSYS;

                cfs_spin_lock(&nrs_hd->nh_service->srv_rq_lock);
                break;

        case PTLRPC_NRS_CTL_GET_STATE:
                if (ptr == NULL) {
                        rc = -EINVAL;
                        break;
                }

                *ptr = policy->nrs_default ? 2 : policy->nrs_active;
                break;

        case PTLRPC_NRS_CTL_SET_STATE:
                if (ptr == NULL) {
                        rc = -EINVAL;
                        break;
                }

                if (policy->nrs_default) {
                        rc = -EPERM;
                        break;
                }

                if (policy->nrs_active == !!(*ptr)) /* noop */
                        break;

                policy->nrs_active = !!(*ptr);

                nrs_hd = policy->nrs_head;
                if (!policy->nrs_active) {
                        LASSERT(nrs_hd->nh_policy_active == policy);
                        nrs_hd->nh_policy_active = NULL;

                } else { /* activate */
                        LASSERT(nrs_hd->nh_policy_active != policy);

                        if (nrs_hd->nh_policy_active != NULL)
                                nrs_hd->nh_policy_active->nrs_active = 0;
                        nrs_hd->nh_policy_active = policy;
                }
                break;

        case PTLRPC_NRS_CTL_SHRINK:
        case PTLRPC_NRS_CTL_KILL:
                /* XXX reserved */
                rc = -ENOSYS;
                break;
        }
 out:
        cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);
        return rc;
}

int
nrs_policy_register(ptlrpc_nrs_head_t *nrs_hd, int type, char *name,
                    ptlrpc_nrs_ops_t *ops, unsigned long flags, void *arg)
{
        ptlrpc_nrs_policy_t *policy;
        ptlrpc_nrs_policy_t *tmp;
        int                  rc;

        LASSERT(ops != NULL);
        LASSERT(ops->op_obj_get != NULL);
        LASSERT(ops->op_req_poll != NULL);
        LASSERT(ops->op_req_add != NULL);
        LASSERT(ops->op_req_del != NULL);

        /* should have default policy */
        OBD_ALLOC_PTR(policy);
        if (policy == NULL)
                return -ENOMEM;

        strncpy(policy->nrs_name, name, NRS_NAME_LEN);
        policy->nrs_head    = nrs_hd;
        policy->nrs_type    = type;
        policy->nrs_ops     = ops;
        policy->nrs_flags   = flags;

        rc = nrs_policy_init(policy, arg);
        if (rc != 0) {
                OBD_FREE_PTR(policy);
                return rc;
        }

        cfs_spin_lock(&nrs_hd->nh_service->srv_rq_lock);

        tmp = nrs_policy_find_nolock(nrs_hd, type);
        if (tmp != NULL) {
                cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);

                /* XXX it's not safe to refer @tmp w/o lock FIXME */
                CERROR("NRS type %d (%s) has been registered, can't"
                       "register it for %s\n", type, tmp->nrs_name, name);
                nrs_policy_fini(policy);
                OBD_FREE_PTR(policy);

                return -EEXIST;
        }

        /* the first registered policy must be the default */
        if (nrs_hd->nh_policy_default == NULL) {
                nrs_hd->nh_policy_default = policy;
                policy->nrs_default = 1;
        }
        cfs_list_add(&policy->nrs_list, &nrs_hd->nh_policy_list);

        cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);

        return 0;
}

void
nrs_policy_unregister(ptlrpc_nrs_head_t *nrs_hd, int type)
{
        ptlrpc_nrs_policy_t *policy = NULL;

        cfs_spin_lock(&nrs_hd->nh_service->srv_rq_lock);

        policy = nrs_policy_find_nolock(nrs_hd, type);
        if (policy == NULL) {
                cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);
                CERROR("Can't find NRS type %d\n", type);
                return;
        }

        LASSERT(nrs_request_poll(policy, NULL) == NULL);
        LASSERT(nrs_hd->nh_service->srv_is_stopping || policy->nrs_zombie);

        cfs_list_del(&policy->nrs_list);
        if (policy->nrs_default)
                nrs_hd->nh_policy_default = NULL;

        if (policy->nrs_active)
                nrs_hd->nh_policy_active = NULL;

        cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);

        nrs_policy_fini(policy);

        LASSERT(policy->nrs_private == NULL);
        OBD_FREE_PTR(policy);
}

static ptlrpc_nrs_object_t *
nrs_object_get(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        ptlrpc_nrs_object_t *obj;

        obj = policy->nrs_ops->op_obj_get(policy, nrq);
        if (obj != NULL)
                obj->ob_policy = policy;

        return obj;
}

static void
nrs_object_put(ptlrpc_nrs_object_t *obj)
{
        ptlrpc_nrs_policy_t *policy = obj->ob_policy;

        LASSERT(policy != NULL);
        /* not always need op_obj_put */
        if (policy->nrs_ops->op_obj_put)
                policy->nrs_ops->op_obj_put(policy, obj);
}

void
nrs_objects_get_safe(ptlrpc_nrs_head_t *nrs_hd,
                     ptlrpc_nrs_request_t *nrq, ptlrpc_nrs_object_t **objs)
{
        ptlrpc_nrs_policy_t *policy = nrs_hd->nh_policy_default;

        objs[NRS_OBJ_DEFAULT] = nrs_object_get(policy, nrq);
        /* nrs_object_get of default policy shouldn't fail */
        LASSERT(objs[NRS_OBJ_DEFAULT] != NULL);

        objs[NRS_OBJ_ACTIVE] = NULL;

        if (nrs_hd->nh_policy_active == NULL)
                return;

        /* NB: this lock is unnecessary because pointer assignment is
         * atomic in linux-kernel */
        cfs_spin_lock(&nrs_hd->nh_service->srv_rq_lock);
        if (nrs_hd->nh_policy_active == NULL) {
                cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);
                return;
        }

        LASSERT(policy != nrs_hd->nh_policy_active);

        policy = nrs_hd->nh_policy_active;
        cfs_spin_unlock(&nrs_hd->nh_service->srv_rq_lock);

        objs[NRS_OBJ_ACTIVE] = nrs_object_get(policy, nrq);
        return;
}

void
nrs_objects_put_safe(ptlrpc_nrs_object_t **objs)
{
        int i;

        for (i = NRS_OBJ_MAX - 1; i > NRS_OBJ_NULL; i--) {
                if (objs[i] != NULL) {
                        nrs_object_put(objs[i]);
                        objs[i] = NULL;
                }
        }
}

void
nrs_request_init(ptlrpc_nrs_head_t *nrs_hd, ptlrpc_nrs_request_t *nrq)
{
        nrq->nr_obj = NULL; /* not enqueued yet */
        nrs_objects_get_safe(nrs_hd, nrq, nrq->nr_objs);
}

void
nrs_request_fini(ptlrpc_nrs_request_t *nrq)
{
        nrs_objects_put_safe(nrq->nr_objs);
        nrq->nr_obj = NULL;
}

ptlrpc_nrs_request_t *
nrs_request_poll(ptlrpc_nrs_policy_t *policy, void *arg)
{
        ptlrpc_nrs_request_t *nrq;

        if (policy->nrs_req_count == 0)
                return NULL;

        nrq =  policy->nrs_ops->op_req_poll(policy, arg);

        LASSERT(nrq != NULL);
        LASSERT(nrq->nr_obj != NULL);
        LASSERT(nrq->nr_obj->ob_policy == policy);

        return nrq;
}

ptlrpc_nrs_policy_t *
nrs_request_add(ptlrpc_nrs_request_t *nrq)
{
        ptlrpc_nrs_policy_t *policy;
        int rc = -1;
        int i;

        LASSERT(nrq->nr_obj == NULL);

        for (i = NRS_OBJ_MAX - 1; i > NRS_OBJ_NULL; i--) {
                if (nrq->nr_objs[i] == NULL)
                        continue;

                policy = nrq->nr_objs[i]->ob_policy;
                LASSERT(policy != NULL);

                nrq->nr_obj = nrq->nr_objs[i];
                rc = policy->nrs_ops->op_req_add(policy, nrq);
                if (rc == 0) {
                        policy->nrs_head->nh_req_count++;
                        policy->nrs_req_count++;
                        return policy;
                }
                nrq->nr_obj = NULL;
        }

        LBUG(); /* shouldn never be here */
        return NULL;
}

ptlrpc_nrs_policy_t *
nrs_request_del(ptlrpc_nrs_request_t *nrq)
{
        ptlrpc_nrs_policy_t *policy;

        LASSERT(nrq->nr_obj != NULL);
        LASSERT(nrq->nr_obj->ob_policy != NULL);

        policy = nrq->nr_obj->ob_policy;

        policy->nrs_ops->op_req_del(policy, nrq);
        policy->nrs_head->nh_req_count--;
        policy->nrs_req_count--;

        return policy;
}

void
ptlrpc_server_req_add_nolock(struct ptlrpc_service *svc,
                             struct ptlrpc_request *req)
{
        /* NB: must call with hold svc::srv_rq_lock */
        nrs_request_add(&req->rq_nrq);
}

int
ptlrpc_server_req_pending_nolock(struct ptlrpc_service *svc, int hp)
{
        ptlrpc_nrs_head_t *nrs_hd = nrs_policy_head(svc, hp);

        /* NB: can be called w/o any lock */
        return nrs_hd->nh_req_count > 0;
};

struct ptlrpc_request *
ptlrpc_server_req_poll_nolock(struct ptlrpc_service *svc, int hp)
{
        ptlrpc_nrs_head_t     *nrs_hd = nrs_policy_head(svc, hp);
        ptlrpc_nrs_policy_t   *policy;
        ptlrpc_nrs_request_t  *nrq;

        if (nrs_hd->nh_req_count == 0)
                return NULL;

        /* NB: must call with hold svc::srv_rq_lock */
        /* always try to drain requests from all NRS polices even they are
         * inactive, because user can change policy status at runtime */
        nrs_for_each_policy(policy, nrs_hd) {
                nrq = nrs_request_poll(policy, NULL);
                if (nrq != NULL)
                        return container_of(nrq, struct ptlrpc_request, rq_nrq);
        }
        return NULL;
}

void
ptlrpc_server_req_del_nolock(struct ptlrpc_service *svc,
                             struct ptlrpc_request *req)
{
        ptlrpc_nrs_policy_t  *policy;

        /* NB: must call with hold svc::srv_rq_lock */
        policy = nrs_request_del(&req->rq_nrq);

        /* any pending request on other polices? */
        if (policy->nrs_head->nh_req_count > policy->nrs_req_count) {
                /* move current policy to the end so we can round-robin
                 * over all polices and drain requests */
                cfs_list_del(&policy->nrs_list);
                cfs_list_add_tail(&policy->nrs_list,
                                  &policy->nrs_head->nh_policy_list);
        }
}

int
ptlrpc_server_nrs_ctl(struct ptlrpc_service *svc,
                      ptlrpc_nrs_queue_type_t queue,
                      int type, unsigned cmd, void *arg)
{
        int     rc;

        switch (queue) {
        default:
                return -EINVAL;

        case PTLRPC_NRS_QUEUE_BOTH:
        case PTLRPC_NRS_QUEUE_REG:
                rc = nrs_policy_ctl(nrs_policy_head(svc, 0), type, cmd, arg);
                if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
                        break;

        case PTLRPC_NRS_QUEUE_HP:
                rc = nrs_policy_ctl(nrs_policy_head(svc, 1), type, cmd, arg);
                break;
        }

        return rc;
}
CFS_EXPORT_SYMBOL(ptlrpc_server_nrs_ctl);

int
ptlrpc_server_nrs_register(struct ptlrpc_service *svc,
                           ptlrpc_nrs_queue_type_t queue, int type,
                           char *name, ptlrpc_nrs_ops_t *ops,
                           unsigned long flags, void *arg)
{
        int rc;

        switch (queue) {
        default:
                return -EINVAL;

        case PTLRPC_NRS_QUEUE_BOTH:
        case PTLRPC_NRS_QUEUE_REG:
                rc = nrs_policy_register(nrs_policy_head(svc, 0),
                                         type, name, ops, flags, arg);
                if (rc != 0 || queue == PTLRPC_NRS_QUEUE_REG)
                        break;

        case PTLRPC_NRS_QUEUE_HP:
                rc = nrs_policy_register(nrs_policy_head(svc, 1),
                                         type, name, ops, flags, arg);
                break;
        }

        return rc;
}
CFS_EXPORT_SYMBOL(ptlrpc_server_nrs_register);

void
ptlrpc_server_nrs_unregister(struct ptlrpc_service *svc,
                             ptlrpc_nrs_queue_type_t queue, int type)
{
        switch (queue) {
        default:
                return;

        case PTLRPC_NRS_QUEUE_BOTH:
        case PTLRPC_NRS_QUEUE_REG:
                nrs_policy_unregister(nrs_policy_head(svc, 0), type);
                if (queue == PTLRPC_NRS_QUEUE_REG)
                        break;

        case PTLRPC_NRS_QUEUE_HP:
                nrs_policy_unregister(nrs_policy_head(svc, 1), type);
                break;
        }
}
CFS_EXPORT_SYMBOL(ptlrpc_server_nrs_unregister);

void
ptlrpc_server_nrs_setup(struct ptlrpc_service *svc)
{
        ptlrpc_nrs_head_t *nrs_hd;

        nrs_hd = nrs_policy_head(svc, 0);
        nrs_hd->nh_service = svc;
        CFS_INIT_LIST_HEAD(&nrs_hd->nh_policy_list);

        nrs_hd = nrs_policy_head(svc, 1);
        nrs_hd->nh_service = svc;
        CFS_INIT_LIST_HEAD(&nrs_hd->nh_policy_list);
}

void
ptlrpc_server_nrs_cleanup(struct ptlrpc_service *svc)
{
        ptlrpc_nrs_head_t   *nrs_hd;
        ptlrpc_nrs_policy_t *policy;

        nrs_hd = nrs_policy_head(svc, 0);
        while (!cfs_list_empty(&nrs_hd->nh_policy_list)) {
                policy = cfs_list_entry(nrs_hd->nh_policy_list.next,
                                        ptlrpc_nrs_policy_t, nrs_list);
                ptlrpc_server_nrs_unregister(svc, PTLRPC_NRS_QUEUE_REG,
                                             policy->nrs_type);
        }

        nrs_hd = nrs_policy_head(svc, 1);
        while (!cfs_list_empty(&nrs_hd->nh_policy_list)) {
                policy = cfs_list_entry(nrs_hd->nh_policy_list.next,
                                        ptlrpc_nrs_policy_t, nrs_list);
                ptlrpc_server_nrs_unregister(svc, PTLRPC_NRS_QUEUE_HP,
                                             policy->nrs_type);
        }
}

/** NRS polices */
static int
nrs_fifo_init(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_fifo_target_t *target;

        OBD_ALLOC_PTR(target);
        if (target == NULL)
                return -ENOMEM;

        CFS_INIT_LIST_HEAD(&target->ft_list);
        policy->nrs_private = target;
        return 0;
}

static void
nrs_fifo_fini(ptlrpc_nrs_policy_t *policy)
{
        nrs_fifo_target_t *target = policy->nrs_private;

        LASSERT(target != NULL);
        LASSERT(cfs_list_empty(&target->ft_list));

        OBD_FREE_PTR(target);
        policy->nrs_private = NULL;
}

static ptlrpc_nrs_object_t *
nrs_fifo_obj_get(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        /* just use the object embedded in fifo request */
        return &nrq->nr_u.fifo.fr_object.fo_obj;
}

static ptlrpc_nrs_request_t *
nrs_fifo_req_poll(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_fifo_target_t *tgt = policy->nrs_private;

        LASSERT(tgt != NULL);
        return cfs_list_empty(&tgt->ft_list) ? NULL :
               cfs_list_entry(tgt->ft_list.next, ptlrpc_nrs_request_t,
                              nr_u.fifo.fr_list);
}

static int
nrs_fifo_req_add(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_fifo_target_t    *tgt = policy->nrs_private;

        LASSERT(tgt != NULL);
        /* they are for debug */
        nrq->nr_key_major = 0;
        nrq->nr_key_minor = tgt->ft_sequence++;
        cfs_list_add_tail(&nrq->nr_u.fifo.fr_list, &tgt->ft_list);
        return 0;
}

static void
nrs_fifo_req_del(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        LASSERT(!cfs_list_empty(&nrq->nr_u.fifo.fr_list));
        cfs_list_del_init(&nrq->nr_u.fifo.fr_list);
}

ptlrpc_nrs_ops_t ptlrpc_nrs_fifo_ops = {
        .op_policy_init         = nrs_fifo_init,
        .op_policy_fini         = nrs_fifo_fini,
        .op_policy_ctl          = NULL, /* not used */
        .op_target_get          = NULL, /* reserved */
        .op_target_put          = NULL, /* reserved */
        .op_obj_get             = nrs_fifo_obj_get,
        .op_obj_put             = NULL, /* not used */
        .op_req_poll            = nrs_fifo_req_poll,
        .op_req_add             = nrs_fifo_req_add,
        .op_req_del             = nrs_fifo_req_del,
};
CFS_EXPORT_SYMBOL(ptlrpc_nrs_fifo_ops);

static int
crr_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
        ptlrpc_nrs_request_t *nrq1;
        ptlrpc_nrs_request_t *nrq2;

        nrq1 = container_of(e1, ptlrpc_nrs_request_t, nr_u.crr.cr_node);
        nrq2 = container_of(e2, ptlrpc_nrs_request_t, nr_u.crr.cr_node);

        if (nrq1->nr_key_major < nrq2->nr_key_major)
                return 1;
        else if (nrq1->nr_key_major > nrq2->nr_key_major)
                return 0;
        /* equal */
        if (nrq1->nr_key_minor < nrq2->nr_key_minor)
                return 1;
        else
                return 0;
}

cfs_binheap_ops_t nrs_crr_heap_ops = {
        .hop_enter      = NULL,
        .hop_exit       = NULL,
        .hop_compare    = crr_req_compare,
};

static int
nrs_crr_init(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_crr_target_t *target;

        OBD_ALLOC_PTR(target);
        if (target == NULL)
                return -ENOMEM;

        /* XXX we might want to move this allocation to op_poilicy_start()
         * in the future FIXME */
        target->ct_binheap = cfs_binheap_create(&nrs_crr_heap_ops,
                                                CBH_FLAG_ATOMIC_GROW,
                                                4096, NULL);
        if (target->ct_binheap == NULL) {
                OBD_FREE_PTR(target);
                return -ENOMEM;
        }
        policy->nrs_private = target;
        return 0;
}

static void
nrs_crr_fini(ptlrpc_nrs_policy_t *policy)
{
        nrs_crr_target_t *target = policy->nrs_private;

        LASSERT(target != NULL);
        LASSERT(cfs_binheap_is_empty(target->ct_binheap));

        cfs_binheap_destroy(target->ct_binheap);
        OBD_FREE_PTR(target);

        policy->nrs_private = NULL;
}

static ptlrpc_nrs_object_t *
nrs_crr_obj_get(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        struct ptlrpc_request *req = container_of(nrq, struct ptlrpc_request,
                                                  rq_nrq);

        if (req->rq_export == NULL)
                return NULL;

        return req->rq_hp ? &req->rq_export->exp_nrs_obj_hp.co_obj :
                            &req->rq_export->exp_nrs_obj.co_obj;
}

static ptlrpc_nrs_request_t *
nrs_crr_req_poll(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_crr_target_t   *target = policy->nrs_private;
        cfs_binheap_node_t *bhn = cfs_binheap_root(target->ct_binheap);

        return bhn == NULL ? NULL :
               container_of(bhn, ptlrpc_nrs_request_t, nr_u.crr.cr_node);
}

static int
nrs_crr_req_add(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_crr_target_t    *target = policy->nrs_private;
        nrs_crr_obj_t       *cob;
        int rc;

        cob = container_of(nrq->nr_obj, nrs_crr_obj_t, co_obj);

        if (cob->co_round < target->ct_round)
                cob->co_round = target->ct_round;

        nrq->nr_key_major = cob->co_round;
        nrq->nr_key_minor = target->ct_sequence;

        rc = cfs_binheap_insert(target->ct_binheap, &nrq->nr_u.crr.cr_node);
        if (rc == 0) {
                target->ct_sequence++;
                cob->co_round++;
        }

        return rc;
}

static void
nrs_crr_req_del(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_crr_target_t    *target = policy->nrs_private;
        nrs_crr_obj_t       *cob;
        cfs_binheap_node_t  *bhn;

        cob = container_of(nrq->nr_obj, nrs_crr_obj_t, co_obj);

        LASSERT(nrq->nr_key_major < cob->co_round);

        cfs_binheap_remove(target->ct_binheap, &nrq->nr_u.crr.cr_node);

        bhn = cfs_binheap_root(target->ct_binheap);
        if (bhn == NULL) { /* no more request */
                target->ct_round++;
        } else {
                nrq = container_of(bhn, ptlrpc_nrs_request_t, nr_u.crr.cr_node);
                if (target->ct_round < nrq->nr_key_major)
                        target->ct_round = nrq->nr_key_major;
        }
}

ptlrpc_nrs_ops_t ptlrpc_nrs_crr_ops = {
        .op_policy_init         = nrs_crr_init,
        .op_policy_fini         = nrs_crr_fini,
        .op_policy_ctl          = NULL, /* not used */
        .op_target_get          = NULL, /* reserved */
        .op_target_put          = NULL, /* reserved */
        .op_obj_get             = nrs_crr_obj_get,
        .op_obj_put             = NULL, /* not used */
        .op_req_poll            = nrs_crr_req_poll,
        .op_req_add             = nrs_crr_req_add,
        .op_req_del             = nrs_crr_req_del,
};
CFS_EXPORT_SYMBOL(ptlrpc_nrs_crr_ops);

static unsigned
nrs_crr2_hop_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
        return cfs_hash_djb2_hash(key, sizeof(lnet_nid_t), mask);
}

static int
nrs_crr2_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
        nrs_crr2_obj_t *cob = cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);
        lnet_nid_t     *nid = (lnet_nid_t *)key;

        return *nid == cob->co_nid;
}

static void *
nrs_crr2_hop_key(cfs_hlist_node_t *hnode)
{
        nrs_crr2_obj_t *cob = cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);

        return &cob->co_nid;
}

static void *
nrs_crr2_hop_obj(cfs_hlist_node_t *hnode)
{
        return cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);
}

static void
nrs_crr2_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        nrs_crr2_obj_t *cob = cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);

        cfs_atomic_inc(&cob->co_ref);
}

static void
nrs_crr2_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        nrs_crr2_obj_t *cob = cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);

        cfs_atomic_dec(&cob->co_ref);
}

static void
nrs_crr2_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        nrs_crr2_obj_t *cob = cfs_hlist_entry(hnode, nrs_crr2_obj_t, co_hnode);

        LASSERTF(cfs_atomic_read(&cob->co_ref) == 0,
                 "Busy crr2 object %s with %d refs\n",
                 libcfs_nid2str(cob->co_nid), cfs_atomic_read(&cob->co_ref));
        OBD_FREE_PTR(cob);
}

static cfs_hash_ops_t nrs_crr2_hash_ops = {
        .hs_hash        = nrs_crr2_hop_hash,
        .hs_keycmp      = nrs_crr2_hop_keycmp,
        .hs_key         = nrs_crr2_hop_key,
        .hs_object      = nrs_crr2_hop_obj,
        .hs_get         = nrs_crr2_hop_get,
        .hs_put         = nrs_crr2_hop_put,
        .hs_put_locked  = nrs_crr2_hop_put,
        .hs_exit        = nrs_crr2_hop_exit,
};

#define NRS_NID_BBITS           8
#define NRS_NID_HBITS_LOW       10
#define NRS_NID_HBITS_HIGH      15

static int
nrs_crr2_init(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_crr2_target_t *target;

        OBD_ALLOC_PTR(target);
        if (target == NULL)
                return -ENOMEM;

        /* XXX we might want to move these allocations to op_poilicy_start()
         * in the future FIXME */
        target->ct_binheap = cfs_binheap_create(&nrs_crr_heap_ops,
                                                CBH_FLAG_ATOMIC_GROW,
                                                4096, NULL);
        if (target->ct_binheap == NULL)
                goto failed;

        target->ct_cli_hash = cfs_hash_create("nrs_nid_hash",
                                              NRS_NID_HBITS_LOW,
                                              NRS_NID_HBITS_HIGH,
                                              NRS_NID_BBITS, 0,
                                              CFS_HASH_MIN_THETA,
                                              CFS_HASH_MAX_THETA,
                                              &nrs_crr2_hash_ops,
                                              CFS_HASH_DEFAULT);
        if (target->ct_cli_hash == NULL)
                goto failed;

        policy->nrs_private = target;

        return 0;

failed:
        if (target->ct_binheap != NULL)
                cfs_binheap_destroy(target->ct_binheap);

        OBD_FREE_PTR(target);
        return -ENOMEM;
}

static void
nrs_crr2_fini(ptlrpc_nrs_policy_t *policy)
{
        nrs_crr2_target_t *target = policy->nrs_private;

        LASSERT(target != NULL);
        LASSERT(target->ct_binheap != NULL);
        LASSERT(target->ct_cli_hash != NULL);
        LASSERT(cfs_binheap_is_empty(target->ct_binheap));

        cfs_binheap_destroy(target->ct_binheap);
        cfs_hash_putref(target->ct_cli_hash);
        OBD_FREE_PTR(target);

        policy->nrs_private = NULL;
}

static ptlrpc_nrs_object_t *
nrs_crr2_obj_get(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_crr2_target_t     *target = policy->nrs_private;
        nrs_crr2_obj_t        *cob;
        nrs_crr2_obj_t        *tmp;
        struct ptlrpc_request *req;

        req = container_of(nrq, struct ptlrpc_request, rq_nrq);

        cob = cfs_hash_lookup(target->ct_cli_hash, &req->rq_peer.nid);
        if (cob != NULL)
                return &cob->co_obj;

        OBD_ALLOC_PTR(cob);
        if (cob == NULL)
                return NULL;

        cob->co_nid = req->rq_peer.nid;
        cfs_atomic_set(&cob->co_ref, 1); /* 1 for caller */
        tmp = cfs_hash_findadd_unique(target->ct_cli_hash,
                                      &cob->co_nid, &cob->co_hnode);
        if (tmp != cob) {
                OBD_FREE_PTR(cob);
                cob = tmp;
        }
        return &cob->co_obj;
}

static void
nrs_crr2_obj_put(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_object_t *obj)
{
        nrs_crr2_target_t *target = policy->nrs_private;
        nrs_crr2_obj_t    *cob = container_of(obj, nrs_crr2_obj_t, co_obj);

        cfs_hash_put(target->ct_cli_hash, &cob->co_hnode);
}

static ptlrpc_nrs_request_t *
nrs_crr2_req_poll(ptlrpc_nrs_policy_t *policy, void *arg)
{
        nrs_crr2_target_t  *target = policy->nrs_private;
        cfs_binheap_node_t *bhn = cfs_binheap_root(target->ct_binheap);

        return bhn == NULL ? NULL :
               container_of(bhn, ptlrpc_nrs_request_t, nr_u.crr2.cr_node);
}

static int
nrs_crr2_req_add(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_crr2_target_t   *target = policy->nrs_private;
        nrs_crr2_obj_t      *cob;
        int                  rc;

        cob = container_of(nrq->nr_obj, nrs_crr2_obj_t, co_obj);

        if (cob->co_round < target->ct_round)
                cob->co_round = target->ct_round;

        nrq->nr_key_major = cob->co_round;
        nrq->nr_key_minor = target->ct_sequence;

        rc = cfs_binheap_insert(target->ct_binheap, &nrq->nr_u.crr2.cr_node);
        if (rc == 0) {
                target->ct_sequence++;
                cob->co_round++;
        }
        return rc;
}

static void
nrs_crr2_req_del(ptlrpc_nrs_policy_t *policy, ptlrpc_nrs_request_t *nrq)
{
        nrs_crr2_target_t   *target = policy->nrs_private;
        nrs_crr2_obj_t      *cob;
        cfs_binheap_node_t  *bhn;

        cob = container_of(nrq->nr_obj, nrs_crr2_obj_t, co_obj);

        LASSERT(nrq->nr_key_major < cob->co_round);

        cfs_binheap_remove(target->ct_binheap, &nrq->nr_u.crr2.cr_node);

        bhn = cfs_binheap_root(target->ct_binheap);
        if (bhn == NULL) { /* no more request */
                target->ct_round++;
        } else {
                nrq = container_of(bhn, ptlrpc_nrs_request_t,
                                   nr_u.crr2.cr_node);
                if (target->ct_round < nrq->nr_key_major)
                        target->ct_round = nrq->nr_key_major;
        }
}

ptlrpc_nrs_ops_t ptlrpc_nrs_crr2_ops = {
        .op_policy_init         = nrs_crr2_init,
        .op_policy_fini         = nrs_crr2_fini,
        .op_policy_ctl          = NULL, /* not used */
        .op_target_get          = NULL, /* reserved */
        .op_target_put          = NULL, /* reserved */
        .op_obj_get             = nrs_crr2_obj_get,
        .op_obj_put             = nrs_crr2_obj_put,
        .op_req_poll            = nrs_crr2_req_poll,
        .op_req_add             = nrs_crr2_req_add,
        .op_req_del             = nrs_crr2_req_del,
};
CFS_EXPORT_SYMBOL(ptlrpc_nrs_crr2_ops);
