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
 * Copyright (c) 2012 Whamcloud, Inc.
 */
/*
 * lustre/mdd/mdd_scrub.c
 *
 * Top-level entry points into mdd module
 *
 * LFSCK controller, which scans the whole device through low layer
 * iteration APIs, drives all scrub compeonents, controls the speed.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <lustre/lustre_idl.h>
#include <lustre_scrub.h>

#include "mdd_internal.h"

static inline scrub_prep_policy mdd_scrub_prep_policy_table(__u16 type)
{
        switch (type) {
        default:
                return (scrub_prep_policy)NULL;
        }
}

static inline scrub_exec_policy mdd_scrub_exec_policy_table(__u16 type)
{
        switch (type) {
        default:
                return (scrub_exec_policy)NULL;
        }
}

static inline scrub_post_policy mdd_scrub_post_policy_table(__u16 type)
{
        switch (type) {
        default:
                return (scrub_post_policy)NULL;
        }
}

static inline int is_recognized_type(__u16 type)
{
        return 0;
}

static int
mdd_scrub_register(const struct lu_env *env, struct scrub_exec_head *seh,
                   __u16 type, __u8 flags, int reset)
{
        struct scrub_exec_unit *seu;
        struct scrub_unit_desc *sud;
        int                     idx;
        int                     rc;
        ENTRY;

        seu = scrub_unit_new(mdd_scrub_prep_policy_table(type),
                             mdd_scrub_exec_policy_table(type),
                             mdd_scrub_post_policy_table(type));
        if (unlikely(seu == NULL))
                RETURN(-ENOMEM);

        sud = scrub_desc_find(&seh->seh_head, type, &idx);
        if (sud != NULL) {
                rc = scrub_unit_load(env, seh, sud->sud_offset, seu);
                if (rc != 0)
                        GOTO(out, rc);

                if (reset != 0)
                        scrub_unit_reset(seu);
                flags |= (sud->sud_flags & ~SUF_DRYRUN);
        } else {
                scrub_unit_init(seu, type);
        }

        rc = scrub_unit_register(seh, seu, SS_SCANNING, flags);

        GOTO(out, rc);

out:
        if (rc != 0)
                scrub_unit_free(seu);
        return rc;
}

static struct md_scrub_it *msi_init(struct md_device *md)
{
        struct md_scrub_it *msi;
        int                  rc;

        OBD_ALLOC_PTR(msi);
        if (unlikely(msi == NULL))
                return ERR_PTR(-ENOMEM);

        rc = lu_env_init(&msi->msi_env, LCT_MD_THREAD | LCT_DT_THREAD);
        if (rc != 0) {
                OBD_FREE_PTR(msi);
                return ERR_PTR(rc);
        }

        msi->msi_dev = md;
        cfs_waitq_init(&msi->msi_thread.t_ctl_waitq);
        cfs_atomic_set(&msi->msi_refcount, 1);
        return msi;
}

static inline struct md_scrub_it *msi_get(struct md_scrub_it *msi)
{
        cfs_atomic_inc(&msi->msi_refcount);
        return msi;
}

static inline void msi_put(struct md_scrub_it *msi)
{
        if (cfs_atomic_dec_and_test(&msi->msi_refcount)) {
                LASSERT(msi->msi_di == NULL);

                lu_env_fini(&msi->msi_env);
                OBD_FREE_PTR(msi);
        }
}

static struct dt_it *msi_start_di(const struct lu_env *env,
                                  struct scrub_exec_head *seh, __u32 args)
{
        struct dt_object *dto = seh->seh_obj;
        struct dt_it *di;
        int rc;

        rc = dto->do_ops->do_index_try(env, dto, &dt_scrub_features);
        if (rc != 0)
                return ERR_PTR(rc);

        di = dto->do_index_ops->dio_it.init(env, dto, args, BYPASS_CAPA);
        return di;
}

static void msi_stop_di(const struct lu_env *env,
                        struct scrub_exec_head *seh, struct dt_it *di)
{
        LASSERT(di != NULL);

        seh->seh_obj->do_index_ops->dio_it.fini(env, di);
}

static int mdd_scrub_main(void *args)
{
        struct md_scrub_it      *msi    = msi_get((struct md_scrub_it *)args);
        struct lu_env           *env    = &msi->msi_env;
        struct ptlrpc_thread    *thread = &msi->msi_thread;
        struct scrub_exec_head  *seh    =
                                        md2mdd_dev(msi->msi_dev)->mdd_scrub_seh;
        struct scrub_head       *sh     = &seh->seh_head;
        const struct dt_it_ops *iops    = &seh->seh_obj->do_index_ops->dio_it;
        struct dt_it            *di     = msi->msi_di;
        struct lu_fid           *fid    = &mdd_env_info(env)->mti_fid;
        struct md_object        *obj;
        int                      reset  = 0;
        int                      rc     = 0;
        int                      wait   = 0;
        ENTRY;

        cfs_daemonize("md_scrub");

        CDEBUG(D_SCRUB, "MD scrub: flags = 0x%x, pid = %d\n",
               (__u32)sh->sh_param_flags, cfs_curproc_pid());

        /* XXX: Prepare before wakeup the sponsor.
         *      Each scrub component should call iops->get() API with
         *      every bookmark, then low layer module can decide the
         *      start point for current iteration. */
        cfs_down_write(&seh->seh_rwsem);
        rc = scrub_prep(env, seh);

        cfs_spin_lock(&seh->seh_lock);
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        if (rc != 0) {
                reset = 1;
                GOTO(out, rc);
        }

        cfs_up_write(&seh->seh_rwsem);
        /* XXX: Call iops->load() to finish the choosing start point. */
        rc = iops->load(env, di, 0);
        if (rc != 0)
                GOTO(post, rc);

        CDEBUG(D_SCRUB, "MD scrub iteration start: pos = %s\n",
               (char *)iops->key(env, di));

        do {
                rc = iops->rec(env, di, (struct dt_rec *)fid, 0);
                if (rc == 0 && seh->seh_active != 0) {
                        obj = md_object_find_slice(env, msi->msi_dev, fid);
                        if (IS_ERR(obj))
                                rc = PTR_ERR(obj);
                        else
                                msi->msi_obj = obj;
                }

exec:
                cfs_down_read(&seh->seh_rwsem);
                rc = scrub_exec(env, seh, rc);
                cfs_up_read(&seh->seh_rwsem);

                if (msi->msi_obj != NULL) {
                        lu_object_put(env, &msi->msi_obj->mo_lu);
                        msi->msi_obj = NULL;
                }

                if (rc != 0)
                        GOTO(post, rc);

                if (cfs_time_beforeq(seh->seh_time_next_checkpoint,
                                     cfs_time_current())) {
                        memcpy(seh->seh_pos_checkpoint,
                               iops->key(env, di),
                               iops->key_size(env, di));
                        cfs_down_write(&seh->seh_rwsem);
                        rc = scrub_checkpoint(env, seh);
                        cfs_up_write(&seh->seh_rwsem);
                        if (rc != 0) {
                                CERROR("Fail to checkpoint: pos = %s,rc = %d\n",
                                       (char *)iops->key(env, di), rc);
                                GOTO(post, rc);
                        }
                }

                /* Rate control. */
                scrub_check_speed(seh, thread);
                if (unlikely(!thread_is_running(thread)))
                        GOTO(post, rc = 0);

                rc = iops->next(env, di);
                if (rc < 0)
                        goto exec;
        } while (rc == DSRV_CONTINUE);

        GOTO(post, rc);

post:
        switch (rc) {
        case DSRV_WAIT:
                wait = DSRV_WAIT;
        case DSRV_COMPLETED:
                rc = 1;
                break;
        case DSRV_PAUSED:
                rc = 0;
        default:
                break;
        }

        memcpy(seh->seh_pos_checkpoint, iops->key(env, di),
               iops->key_size(env, di));
        cfs_down_write(&seh->seh_rwsem);
        scrub_post(env, seh, rc);

        while (wait == DSRV_WAIT && thread_is_running(thread)) {
                struct l_wait_info lwi = LWI_TIMEOUT_INTR(cfs_time_seconds(1),
                                                          NULL, NULL, NULL);

                wait = iops->next(env, di);
                l_wait_event(thread->t_ctl_waitq,
                             wait != DSRV_WAIT ||
                             !thread_is_running(thread),
                             &lwi);
        }

        CDEBUG(D_SCRUB, "MD scrub iteration stop: pos = %s, rc = %d\n",
               (char *)iops->key(env, di), rc);
out:
        if (reset != 0)
                scrub_head_reset(env, seh);

        cfs_spin_lock(&seh->seh_lock);
        thread_set_flags(thread, SVC_STOPPED);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);

        msi->msi_di = NULL;
        iops->fini(env, di);
        seh->seh_private = NULL;
        cfs_up_write(&seh->seh_rwsem);
        msi_put(msi);
        return rc;
}

int mdd_scrub_start(const struct lu_env *env, struct mdd_device *mdd,
                    struct scrub_start *start)
{
        struct scrub_exec_head *seh     = mdd->mdd_scrub_seh;
        struct scrub_head      *sh      = &seh->seh_head;
        struct scrub_unit_desc *sud;
        struct md_scrub_it     *msi;
        struct l_wait_info      lwi     = { 0 };
        struct dt_it           *di;
        int                     append  = 0;
        int                     rwsem   = 1;
        int                     rc      = 0;
        int                     i;
        __u16                   valid   = 0;
        __u16                   flags   = 0;
        ENTRY;

        if (!(start->ss_flags & SPF_FORCE) && mdd->mdd_noauto_scrub)
                RETURN(-EPERM);

        cfs_down(&seh->seh_sem);
        cfs_down_write(&seh->seh_rwsem);
        msi = (struct md_scrub_it *)seh->seh_private;
        if (msi != NULL) {
                /* Do not allow to trigger scrubs during other LFSCK
                 * processing in this layer, but it may be supported
                 * by low layer scrub component(s). */
                append = 1;
                goto init;
        }

        if (start->ss_valid & SSV_SPEED_LIMIT &&
            sh->sh_param_speed_limit != start->ss_speed_limit) {
                sh->sh_param_speed_limit = start->ss_speed_limit;
                seh->seh_dirty = 1;
        }

        if (start->ss_valid & SSV_METHOD) {
                if (start->ss_method != SM_OTABLE)
                        GOTO(out, rc = -EOPNOTSUPP);

                if (sh->sh_param_method != start->ss_method) {
                        sh->sh_param_method = start->ss_method;
                        seh->seh_dirty = 1;
                }
        }

        if (start->ss_valid & SSV_ERROR_HANDLE) {
                if ((start->ss_flags & SPF_FAILOUT) &&
                   !(sh->sh_param_flags & SPF_FAILOUT)) {
                        sh->sh_param_flags |= SPF_FAILOUT;
                        seh->seh_dirty = 1;
                        valid |= DSV_ERROR_HANDLE;
                        flags |= DSF_FAILOUT;
                } else if ((sh->sh_param_flags & SPF_FAILOUT) &&
                          !(start->ss_flags & SPF_FAILOUT)) {
                        sh->sh_param_flags &= ~SPF_FAILOUT;
                        seh->seh_dirty = 1;
                        valid |= DSV_ERROR_HANDLE;
                }
        }

        if (start->ss_valid & SSV_DRYRUN) {
                if ((start->ss_flags & SPF_DRYRUN) &&
                   !(sh->sh_param_flags & SPF_DRYRUN)) {
                        sh->sh_param_flags |= SPF_DRYRUN;
                        seh->seh_dirty = 1;
                        valid |= DSV_DRYRUN;
                        flags |= DSF_DRYRUN;
                } else if ((sh->sh_param_flags & SPF_DRYRUN) &&
                          !(start->ss_flags & SPF_DRYRUN)) {
                        sh->sh_param_flags &= ~SPF_DRYRUN;
                        seh->seh_dirty = 1;
                        valid |= DSV_DRYRUN;
                }
        }

        if (start->ss_active == SCRUB_TYPES_DEF) {
                for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                        sud = &sh->sh_desc[i];
                        if (!is_recognized_type(sud->sud_type) ||
                            ((sud->sud_status != SS_CRASHED) &&
                            !(sud->sud_flags & SUF_INCONSISTENT)))
                                continue;

                        rc = mdd_scrub_register(env, seh, sud->sud_type,
                                                sud->sud_flags,
                                                start->ss_flags & SPF_RESET);
                        if (rc != 0)
                                GOTO(out, rc);
                }
        } else {
                for (i = 0; i < SCRUB_DESC_COUNT &&
                            start->ss_active != 0; i++) {
                        __u16 type = 1 << i;

                        if (!(type & start->ss_active) ||
                            !is_recognized_type(type))
                                continue;

                        rc = mdd_scrub_register(env, seh, type, 0,
                                                start->ss_flags & SPF_RESET);
                        if (rc != 0)
                                GOTO(out, rc);

                        start->ss_active &= ~type;
                }
        }

        scrub_set_speed(seh);
        msi = msi_init(&mdd->mdd_md_dev);
        if (unlikely(IS_ERR(msi)))
                GOTO(out, rc = PTR_ERR(msi));

init:
       if (start->ss_active != SCRUB_TYPES_DEF) {
                valid |= DSV_OI_SCRUB;
                if (start->ss_active & ST_OI_SCRUB) {
                        flags |= DSF_OI_SCRUB;
                        start->ss_active &= ~ST_OI_SCRUB;
                }

                if (!(start->ss_flags & SPF_FORCE))
                        flags |= DSF_TBR;
        }

        if (start->ss_flags & SPF_RESET)
                flags |= DSF_RESET;

        msi->msi_args = (flags << DT_SCRUB_FLAGS_SHIFT) | valid;
        di = msi_start_di(env, seh, msi->msi_args);
        if (IS_ERR(di)) {
                if (append == 0)
                        msi_put(msi);
                GOTO(out, rc = PTR_ERR(di));
        }

        if (append != 0) {
                LASSERT(seh->seh_private == msi);
                LASSERT(msi->msi_di == di);
                LASSERT(thread_is_running(&msi->msi_thread));
                GOTO(out, rc = 0);
        }

        LASSERT(msi->msi_di == NULL);

        msi->msi_di = di;
        seh->seh_private = msi;

        rc = cfs_create_thread(mdd_scrub_main, msi, 0);
        if (rc < 0) {
                CERROR("Cannot start md scrub thread: rc %d\n", rc);
                msi->msi_di = NULL;
                msi_stop_di(env, seh, di);
                seh->seh_private = NULL;
        } else {
                cfs_up_write(&seh->seh_rwsem);
                l_wait_event(msi->msi_thread.t_ctl_waitq,
                             thread_is_running(&msi->msi_thread) ||
                             thread_is_stopped(&msi->msi_thread),
                             &lwi);
                rwsem = 0;
                rc = 0;
        }

        msi_put(msi);
        GOTO(out, rc);

out:
        if (rc != 0 && append == 0)
                scrub_head_reset(env, seh);
        if (rwsem != 0)
                cfs_up_write(&seh->seh_rwsem);
        cfs_up(&seh->seh_sem);
        return rc;
}

int mdd_scrub_stop(const struct lu_env *env, struct mdd_device *mdd)
{
        struct scrub_exec_head *seh    = mdd->mdd_scrub_seh;
        struct l_wait_info      lwi    = { 0 };
        struct md_scrub_it     *msi;
        struct ptlrpc_thread   *thread;
        ENTRY;

        cfs_down(&seh->seh_sem);
        cfs_spin_lock(&seh->seh_lock);
        if (seh->seh_private == NULL) {
                cfs_spin_unlock(&seh->seh_lock);
                cfs_up(&seh->seh_sem);
                RETURN(-EALREADY);
        }

        msi = msi_get((struct md_scrub_it *)seh->seh_private);
        thread = &msi->msi_thread;
        LASSERT(!thread_is_stopped(thread));

        thread_set_flags(thread, SVC_STOPPING);
        cfs_spin_unlock(&seh->seh_lock);
        cfs_waitq_broadcast(&thread->t_ctl_waitq);
        l_wait_event(thread->t_ctl_waitq,
                     thread_is_stopped(thread),
                     &lwi);
        cfs_up(&seh->seh_sem);
        msi_put(msi);

        RETURN(0);
}

static struct scrub_exec_head *
mdd_scrub_init_seh(const struct lu_env *env, struct dt_object *obj)
{
        struct scrub_exec_head *seh;
        struct scrub_head      *sh;
        struct scrub_unit_desc *sud;
        int                     i;
        int                     rc;
        ENTRY;

        seh = scrub_head_new(obj);
        if (unlikely(seh == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        cfs_down_write(&seh->seh_rwsem);
        rc = scrub_head_load(env, seh);
        if (rc == -ENOENT) {
                scrub_head_init(seh);
                rc = 0;
        } else if (rc != 0) {
                GOTO(out, rc);
        }

        sh = &seh->seh_head;
        for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                sud = &sh->sh_desc[i];
                if (sud->sud_status == SS_SCANNING) {
                        sud->sud_status = SS_CRASHED;
                        seh->seh_dirty = 1;
                }
        }

        rc = scrub_head_store(env, seh);

        GOTO(out, rc);

out:
        if (rc != 0) {
                scrub_head_free(env, seh);
                seh = ERR_PTR(rc);
        }
        cfs_up_write(&seh->seh_rwsem);
        return seh;
}

int mdd_scrub_setup(const struct lu_env *env, struct mdd_device *mdd)
{
        struct dt_object       *obj;
        struct scrub_exec_head *seh;
        struct scrub_start     *start;
        int                     rc;
        ENTRY;

        if (mdd->mdd_dt_conf.ddp_mntopts & MNTOPT_NOAUTO_SCRUB)
                mdd->mdd_noauto_scrub = 1;

        obj = dt_store_open(env, mdd->mdd_child, "", mdd_scrub,
                            &mdd_env_info(env)->mti_fid);
        if (IS_ERR(obj))
                RETURN(PTR_ERR(obj));

        seh = mdd_scrub_init_seh(env, obj);
        lu_object_put(env, &obj->do_lu);
        if (IS_ERR(seh))
                RETURN(PTR_ERR(seh));

        mdd->mdd_scrub_seh = seh;
        if (mdd->mdd_noauto_scrub)
                RETURN(0);

        OBD_ALLOC_PTR(start);
        if (start == NULL) {
                CWARN("Not enough memory to start scrub when mounts up.\n");
                RETURN(0);
        }

        start->ss_version = SCRUB_VERSION_V1;
        start->ss_active = SCRUB_TYPES_DEF;
        CDEBUG(D_SCRUB,
               "Try to trigger Lustre scrub automatically when mounts up.\n");
        rc = mdd_scrub_start(env, mdd, start);
        if (rc != 0)
                CWARN("Fail to start Lustre scrub when mounts up: rc = %d\n",
                      rc);
        else
                CDEBUG(D_SCRUB, "Triggered Lustre Scrub when mounts up: %x\n",
                       start->ss_active);
        OBD_FREE_PTR(start);

        RETURN(0);
}

void mdd_scrub_cleanup(const struct lu_env *env, struct mdd_device *mdd)
{
        struct scrub_exec_head *seh = mdd->mdd_scrub_seh;

        if (seh != NULL) {
                if (seh->seh_private != NULL)
                        mdd_scrub_stop(env, mdd);

                cfs_down_write(&seh->seh_rwsem);
                scrub_head_free(env, seh);
                cfs_up_write(&seh->seh_rwsem);
                mdd->mdd_scrub_seh = NULL;
        }
}
