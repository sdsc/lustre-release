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
 * lustre/mdd/mdd_scrub.c
 *
 * Lustre Metadata Server (mdd) routines
 *
 * OI Scrub is used to rebuild Object Index files when restore MDT from
 * file-level backup. And also can be part of consistency routine check.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

/*
 * MDD layer iterator for Lustre online fsck. The kernel thread "mdd_scrub"
 * scans the whole system by some method(s) to perform specified checking(s)
 * and reparation(s).
 *
 * The method includes:
 * SM_ITABLE:    Inode table based iteration. In fact, "inode" is backend
 *               filesystem sutff, depends on local filesystem types. For
 *               osd-ldiskfs, it is inode, for osd-zfs, it is znode.
 *               MDD neither know nor care about that. It just uses lower
 *               layer iteration methods (API) to check/repair the system.
 * SM_NAMESPACE: Namespace based scanning. NOT support yet.
 *
 * The "mdd_scrub" can perform many routine check/repair, currently, its main
 * task is to rebuild the Object Index files for MDT restored from file-level
 * backup. The main loop of the iterator calls lower layer dt_index iteration
 * APIs for OI Scrub as following:
 * ->next():   Localize the iteration cursor to next object to be processed.
 * ->rec():    Read the record corresponding to the iterator current cursor.
 * ->insert(): Update the FID-to-lid mapping in the Object Index file.
 *
 * The local file "scrub" is used for recording the iterator parameters, status,
 * and other information. With these information, we can trace the iteration and
 * resume the iteration from last checkpoint.
 *
 * On the other hand, the file "scrub" is also used for detecting MDT file-level
 * backup/restore automatically when MDT mounts up: when the "scrub" is created,
 * its fid is recorded as part of its name entry under root directory. Normally,
 * MDT file-level backup/restore cannot reserve original fid information in name
 * entry. When MDT mounts up, it will detect such fid in name entry for the file
 * "scrub". If it is missed, means that MDT is restored from file-level backup.
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <lustre_fid.h>
#include "mdd_internal.h"

#define HALF_SEC        (CFS_HZ >> 1)

enum mdd_scrub_ecode {
        MSE_NULL                = 0,
        MSE_READ_RECORD         = 1,
        MSE_INSERT_RECORD       = 2,
        MSE_FIND_OBJECT         = 3,
        MSE_CREATE_TRANS        = 4,
        MSE_DECLARE_TRANS       = 5,
        MSE_START_TRANS         = 6,
        MSE_STOP_TRANS          = 7,
        MSE_CHECKPOINT          = 8,
};

static char *mdd_scrub_estr[] = {
        [MSE_NULL]              = "null",
        [MSE_READ_RECORD]       = "read record",
        [MSE_INSERT_RECORD]     = "insert record",
        [MSE_FIND_OBJECT]       = "find object",
        [MSE_CREATE_TRANS]      = "create trans",
        [MSE_DECLARE_TRANS]     = "declare trans",
        [MSE_START_TRANS]       = "start trans",
        [MSE_STOP_TRANS]        = "stop trans",
        [MSE_CHECKPOINT]        = "checkpoint",
};

static union scrub_key zero_key = { .sk_u64[0] = 0,
                                    .sk_u64[1] = 0 };

static inline void mdd_scrub_position_to_lid(union lu_local_id *lid,
                                             union scrub_key *key)
{
        lid->lli_u64[0] = key->sk_u64[0];
        lid->lli_u64[1] = key->sk_u64[1];
}

static inline void mdd_scrub_lid_to_position(union scrub_key *key,
                                             union lu_local_id *lid)
{
        key->sk_u64[0] = lid->lli_u64[0];
        key->sk_u64[1] = lid->lli_u64[1];
}

static inline int key_is_zero(union scrub_key *sk)
{
        return sk->sk_u64[0] == 0 &&
               sk->sk_u64[1] == 0;
}

static inline void key_inc(union scrub_key *sk, int keysize)
{
        switch(keysize) {
        case 16:
                sk->sk_u64[0]++;
                if (unlikely(sk->sk_u64[0] == 0))
                        sk->sk_u64[1]++;
                break;
        case 8:
                sk->sk_u64[0]++;
                break;
        case 4:
                sk->sk_u32[0]++;
                break;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
        }
}

static inline int key_cmp(union scrub_key *key1, union scrub_key *key2,
                          int keysize)
{
        switch (keysize) {
        case 16:
                if (key1->sk_u64[1] > key2->sk_u64[1]) {
                        return 1;
                } else if (key1->sk_u64[1] == key2->sk_u64[1]) {
                        if (key1->sk_u64[0] > key2->sk_u64[0])
                                return 1;
                        else if (key1->sk_u64[0] == key2->sk_u64[0])
                                return 0;
                        else
                                return -1;
                } else {
                        return -1;
                }
        case 8:
                if (key1->sk_u64[0] > key2->sk_u64[0])
                        return 1;
                else if (key1->sk_u64[0] == key2->sk_u64[0])
                        return 0;
                else
                        return -1;
        case 4:
                if (key1->sk_u32[0] > key2->sk_u32[0])
                        return 1;
                else if (key1->sk_u32[0] == key2->sk_u32[0])
                        return 0;
                else
                        return -1;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
                return 0;
        }
}

static inline void mdd_scrub_print(__u32 level, struct dt_key *key,
                                   int keysize, int rc, int ecode)
{
        switch (keysize) {
        case 16:
                CDEBUG(level, "MDD scrub failed to %s at: "LPU64"/"LPU64"\n",
                       mdd_scrub_estr[ecode], *(__u64 *)key,
                       *((__u64 *)key + 1));
                break;
        case 8:
                CDEBUG(level, "MDD scrub failed to %s at: "LPU64"\n",
                       mdd_scrub_estr[ecode], *(__u64 *)key);
                break;
        case 4:
                CDEBUG(level, "MDD scrub failed to %s at: %u\n",
                       mdd_scrub_estr[ecode], *(__u32 *)key);
                break;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
        }
}

static inline void mdd_scrub_convert_key(union scrub_key *sk,
                                         struct dt_key *dk, int keysize)
{
        switch (keysize) {
        case 16:
                sk->sk_u64[1] = *((__u64 *)dk + 1);
        case 8:
                sk->sk_u64[0] = *(__u64 *)dk;
                break;
        case 4:
                sk->sk_u32[0] = *(__u32 *)dk;
                break;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
        }
}

static void mdd_scrub_key_to_be(union scrub_key *des, union scrub_key *src,
                                int keysize)
{
        switch (keysize) {
        case 16:
                des->sk_u64[1] = cpu_to_be64(src->sk_u64[1]);
        case 8:
                des->sk_u64[0] = cpu_to_be64(src->sk_u64[0]);
                break;
        case 4:
                des->sk_u32[0] = cpu_to_be32(src->sk_u32[0]);
                break;
        case 0:
                memcpy(des, src, sizeof(*des));
                break;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
        }
}

static void mdd_scrub_key_to_cpu(union scrub_key *des, union scrub_key *src,
                                 int keysize)
{
        switch (keysize) {
        case 16:
                des->sk_u64[1] = be64_to_cpu(src->sk_u64[1]);
        case 8:
                des->sk_u64[0] = be64_to_cpu(src->sk_u64[0]);
                break;
        case 4:
                des->sk_u32[0] = be32_to_cpu(src->sk_u32[0]);
                break;
        case 0:
                memcpy(des, src, sizeof(*des));
                break;
        default:
                CERROR("Do not support other keysize %d yet\n", keysize);
                LBUG();
        }
}

void mdd_scrub_param_to_be(struct scrub_param *des,
                           struct scrub_param *src)
{
        des->sp_method              = cpu_to_be16(src->sp_method);
        des->sp_checkpoint_interval = cpu_to_be16(src->sp_checkpoint_interval);
        des->sp_speed_limit         = cpu_to_be32(src->sp_speed_limit);
        des->sp_pipeline_window     = cpu_to_be32(src->sp_pipeline_window);
        des->sp_flags               = cpu_to_be32(src->sp_flags);
}

static void mdd_scrub_param_to_cpu(struct scrub_param *des,
                                   struct scrub_param *src)
{
        des->sp_method              = be16_to_cpu(src->sp_method);
        des->sp_checkpoint_interval = be16_to_cpu(src->sp_checkpoint_interval);
        des->sp_speed_limit         = be32_to_cpu(src->sp_speed_limit);
        des->sp_pipeline_window     = be32_to_cpu(src->sp_pipeline_window);
        des->sp_flags               = be32_to_cpu(src->sp_flags);
}

static void mdd_scrub_ninfo_to_be(struct scrub_info_normal *des,
                                  struct scrub_info_normal *src, int keysize)
{
        des->sin_time_last_checkpoint =
                            cpu_to_be64(src->sin_time_last_checkpoint);
        mdd_scrub_key_to_be(&des->sin_position_last_checkpoint,
                            &src->sin_position_last_checkpoint, keysize);
        des->sin_items_scanned        = cpu_to_be64(src->sin_items_scanned);
        des->sin_items_updated        = cpu_to_be64(src->sin_items_updated);
        des->sin_items_failed         = cpu_to_be64(src->sin_items_failed);
        des->sin_time                 = cpu_to_be32(src->sin_time);
        des->sin_speed                = cpu_to_be32(src->sin_speed);
}

static void mdd_scrub_ninfo_to_cpu(struct scrub_info_normal *des,
                                   struct scrub_info_normal *src, int keysize)
{
        des->sin_time_last_checkpoint =
                             be64_to_cpu(src->sin_time_last_checkpoint);
        mdd_scrub_key_to_cpu(&des->sin_position_last_checkpoint,
                             &src->sin_position_last_checkpoint, keysize);
        des->sin_items_scanned        = be64_to_cpu(src->sin_items_scanned);
        des->sin_items_updated        = be64_to_cpu(src->sin_items_updated);
        des->sin_items_failed         = be64_to_cpu(src->sin_items_failed);
        des->sin_time                 = be32_to_cpu(src->sin_time);
        des->sin_speed                = be32_to_cpu(src->sin_speed);
}

static void mdd_scrub_dinfo_to_be(struct scrub_info_dryrun *des,
                                  struct scrub_info_dryrun *src, int keysize)
{
        des->sid_time_last_checkpoint =
                            cpu_to_be64(src->sid_time_last_checkpoint);
        mdd_scrub_key_to_be(&des->sid_position_last_checkpoint,
                            &src->sid_position_last_checkpoint, keysize);
        mdd_scrub_key_to_be(&des->sid_position_first_unmatched,
                            &src->sid_position_first_unmatched, keysize);
        des->sid_items_scanned        = cpu_to_be64(src->sid_items_scanned);
        des->sid_items_unmatched      = cpu_to_be64(src->sid_items_unmatched);
        des->sid_items_failed         = cpu_to_be64(src->sid_items_failed);
        des->sid_items_updated_prior  =
                             cpu_to_be64(src->sid_items_updated_prior);
        des->sid_time                 = cpu_to_be32(src->sid_time);
        des->sid_speed                = cpu_to_be32(src->sid_speed);
}

static void mdd_scrub_dinfo_to_cpu(struct scrub_info_dryrun *des,
                                   struct scrub_info_dryrun *src, int keysize)
{
        des->sid_time_last_checkpoint =
                             be64_to_cpu(src->sid_time_last_checkpoint);
        mdd_scrub_key_to_cpu(&des->sid_position_last_checkpoint,
                             &src->sid_position_last_checkpoint, keysize);
        mdd_scrub_key_to_cpu(&des->sid_position_first_unmatched,
                             &src->sid_position_first_unmatched, keysize);
        des->sid_items_scanned        = be64_to_cpu(src->sid_items_scanned);
        des->sid_items_unmatched      = be64_to_cpu(src->sid_items_unmatched);
        des->sid_items_failed         = be64_to_cpu(src->sid_items_failed);
        des->sid_items_updated_prior  =
                             be64_to_cpu(src->sid_items_updated_prior);
        des->sid_time                 = be32_to_cpu(src->sid_time);
        des->sid_speed                = be32_to_cpu(src->sid_speed);
}

void mdd_scrub_header_to_be(struct scrub_header *des,
                            struct scrub_header *src)
{
        __u32 magic = cpu_to_be32(src->sh_magic);

        if (magic == src->sh_magic) {
                memcpy(des, src, sizeof(*des));
        } else {
                des->sh_magic              = magic;
                des->sh_version            = cpu_to_be16(src->sh_version);
                des->sh_keysize            = cpu_to_be16(src->sh_keysize);
                des->sh_success_count      = cpu_to_be32(src->sh_success_count);
                des->sh_current_status     =
                                    cpu_to_be32(src->sh_current_status);
                des->sh_time_last_complete =
                                    cpu_to_be64(src->sh_time_last_complete);
                des->sh_time_initial_start =
                                    cpu_to_be64(src->sh_time_initial_start);
                des->sh_time_latest_start  =
                                    cpu_to_be64(src->sh_time_latest_start);
                mdd_scrub_key_to_be(&des->sh_position_latest_start,
                                    &src->sh_position_latest_start,
                                     src->sh_keysize);
                mdd_scrub_param_to_be(&des->sh_param, &src->sh_param);
                mdd_scrub_ninfo_to_be(&des->sh_normal, &src->sh_normal,
                                      src->sh_keysize);
                mdd_scrub_dinfo_to_be(&des->sh_dryrun, &src->sh_dryrun,
                                      src->sh_keysize);
        }
}

static void mdd_scrub_header_to_cpu(struct scrub_header *des,
                                    struct scrub_header *src)
{
        __u32 magic = be32_to_cpu(src->sh_magic);

        if (magic == src->sh_magic) {
                memcpy(des, src, sizeof(*des));
        } else {
                des->sh_magic              = magic;
                des->sh_version            = be16_to_cpu(src->sh_version);
                des->sh_keysize            = be16_to_cpu(src->sh_keysize);
                des->sh_success_count      = be32_to_cpu(src->sh_success_count);
                des->sh_current_status     =
                                     be32_to_cpu(src->sh_current_status);
                des->sh_time_last_complete =
                                     be64_to_cpu(src->sh_time_last_complete);
                des->sh_time_initial_start =
                                     be64_to_cpu(src->sh_time_initial_start);
                des->sh_time_latest_start  =
                                     be64_to_cpu(src->sh_time_latest_start);
                mdd_scrub_key_to_cpu(&des->sh_position_latest_start,
                                     &src->sh_position_latest_start,
                                     des->sh_keysize);
                mdd_scrub_param_to_cpu(&des->sh_param, &src->sh_param);
                mdd_scrub_ninfo_to_cpu(&des->sh_normal, &src->sh_normal,
                                       des->sh_keysize);
                mdd_scrub_dinfo_to_cpu(&des->sh_dryrun, &src->sh_dryrun,
                                       des->sh_keysize);
        }
}

static void mdd_init_scrub_param(struct scrub_param *param)
{
        param->sp_method              = SM_ITABLE;
        param->sp_checkpoint_interval = SPD_CHECKPOING_INTERVAL;
        param->sp_speed_limit         = SPD_SPEED_LIMIT;
        param->sp_pipeline_window     = SPD_PIPELINE_WINDOW;
        param->sp_flags               = SPF_OI_REBUILD;
}

static inline void mdd_init_scrub_ninfo(struct scrub_info_normal *ninfo)
{
        memset(ninfo, 0, sizeof(*ninfo));
}

static inline void mdd_init_scrub_dinfo(struct scrub_info_dryrun *dinfo)
{
        memset(dinfo, 0, sizeof(*dinfo));
}

static void mdd_init_scrub_header(struct scrub_header *header,
                                  enum scrub_status status)
{
        header->sh_magic                 = SCRUB_MAGIC;
        header->sh_version               = SCRUB_VERSION_V1;
        header->sh_keysize               = 0;
        header->sh_success_count         = 0;
        header->sh_current_status        = status;
        header->sh_time_last_complete    = 0;
        header->sh_time_initial_start    = 0;
        header->sh_time_latest_start     = 0;
        header->sh_position_latest_start = zero_key;
        mdd_init_scrub_param(&header->sh_param);
        mdd_init_scrub_ninfo(&header->sh_normal);
        mdd_init_scrub_dinfo(&header->sh_dryrun);
}

static int mdd_declare_scrub_store_header(const struct lu_env *env,
                                          struct mdd_device *mdd, int size,
                                          struct thandle *handle)
{
        int rc;

        rc = dt_declare_record_write(env, mdd->mdd_scrub_obj, size, 0, handle);
        if (rc == 0)
                handle->th_sync = 1;
        return rc;
}

int do_scrub_store_header(const struct lu_env *env, struct mdd_device *mdd,
                          struct scrub_header *header_disk)
{
        struct dt_object *dto = mdd->mdd_scrub_obj;
        struct thandle *handle;
        struct lu_buf *buf;
        loff_t pos = 0;
        int rc;
        ENTRY;

        handle = mdd_trans_create(env, mdd);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for scrub_store_header: "
                       "rc = %d\n", rc);
                RETURN(rc);
        }

        rc = mdd_declare_scrub_store_header(env, mdd, sizeof(*header_disk),
                                            handle);
        if (rc != 0) {
                CERROR("Fail to declare trans for scrub_store_header: "
                       "rc = %d\n", rc);
                GOTO(stop, rc);
        }

        rc = mdd_trans_start(env, mdd, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for scrub_store_header: "
                       "rc = %d\n", rc);
                GOTO(stop, rc);
        }

        buf = mdd_buf_get(env, header_disk, sizeof(*header_disk));
        rc = dto->do_body_ops->dbo_write(env, dto, buf, &pos, handle,
                                         BYPASS_CAPA, 1);
        if (rc == buf->lb_len)
                rc = 0;
        else if (rc >= 0)
                rc = -ENOSPC;

        EXIT;

stop:
        mdd_trans_stop(env, mdd, rc, handle);
        return rc;
}

static struct dt_it *
msi_start_it(const struct lu_env *env, struct mdd_device *mdd,
             enum scrub_method method, struct dt_scrub_param *args)
{
        struct dt_object *dto = mdd->mdd_scrub_obj;
        struct dt_it *it;
        int rc;

        if (method != SM_ITABLE)
                return ERR_PTR(-ENOTSUPP);

        rc = dto->do_ops->do_index_try(env, dto, &dt_scrub_features);
        if (rc != 0)
                return ERR_PTR(rc);

        it = dto->do_index_ops->dio_it.init(env, dto, args, BYPASS_CAPA);
        return it;
}

static void msi_stop_it(const struct lu_env *env, struct dt_object *dto,
                        struct dt_it *it)
{
        LASSERT(it != NULL);

        dto->do_index_ops->dio_it.fini(env, it);
}

static struct mdd_scrub_it *msi_init(struct mdd_device *mdd)
{
        struct mdd_scrub_it *msi;

        OBD_ALLOC_PTR(msi);
        if (unlikely(msi == NULL))
                return NULL;

        cfs_waitq_init(&msi->msi_thread.t_ctl_waitq);
        cfs_atomic_set(&msi->msi_refcount, 1);

        return msi;
}

static void msi_fini(struct mdd_scrub_it *msi)
{
        LASSERT(!thread_is_running(&msi->msi_thread));
        LASSERT(msi->msi_it == NULL);
        LASSERT(cfs_atomic_read(&msi->msi_refcount) == 0);

        OBD_FREE_PTR(msi);
}

static inline struct mdd_scrub_it *msi_get(struct mdd_scrub_it *msi)
{
        cfs_atomic_inc(&msi->msi_refcount);
        return msi;
}

static inline void msi_put(struct mdd_scrub_it *msi)
{
        if (cfs_atomic_dec_and_test(&msi->msi_refcount))
                msi_fini(msi);
}

static void msi_prior_notify(void *data)
{
        struct mdd_scrub_it *msi = data;

        msi->msi_has_prior = 1;
        cfs_waitq_signal(&msi->msi_thread.t_ctl_waitq);
}

static int mdd_declare_scrub_update_rec(const struct lu_env *env,
                                        struct mdd_device *mdd,
                                        const struct dt_rec *rec,
                                        const struct dt_key *key,
                                        struct thandle *handle)
{
        int rc;

        rc = dt_declare_insert(env, mdd->mdd_scrub_obj, rec, key, handle);
        return rc;
}

static int mdd_scrub_oi_rebuild(const struct lu_env *env,
                                struct mdd_device *mdd,
                                struct dt_scrub_rec *dsr,
                                int *ecode, __u64 *updated)
{
        struct scrub_header      *header      = &mdd->mdd_scrub_header_mem;
        struct scrub_param       *param       = &header->sh_param;
        struct dt_object         *dto         = mdd->mdd_scrub_obj;
        const struct dt_index_operations *dio = dto->do_index_ops;
        struct thandle           *handle;
        int rc, rc1;
        ENTRY;

        if ((param->sp_flags & SPF_DRYRUN) && !(dsr->dsr_valid & DSV_PRIOR)) {
                struct scrub_info_dryrun *dinfo = &header->sh_dryrun;

                dinfo->sid_items_unmatched++;
                if (unlikely(key_is_zero(&dinfo->sid_position_first_unmatched)))
                        mdd_scrub_lid_to_position(
                                        &dinfo->sid_position_first_unmatched,
                                        &dsr->dsr_lid);
                GOTO(out, rc);
        }

        handle = mdd_trans_create(env, mdd);
        if (IS_ERR(handle)) {
                *ecode = MSE_CREATE_TRANS;
                GOTO(out, rc = PTR_ERR(handle));
        }

        rc = mdd_declare_scrub_update_rec(env, mdd,
                                          (const struct dt_rec *)&dsr->dsr_lid,
                                          (const struct dt_key *)&dsr->dsr_fid,
                                          handle);
        if (rc != 0) {
                mdd_trans_stop(env, mdd, rc, handle);
                *ecode = MSE_DECLARE_TRANS;
                GOTO(out, rc);
        }

        rc = mdd_trans_start(env, mdd, handle);
        if (rc != 0) {
                mdd_trans_stop(env, mdd, rc, handle);
                *ecode = MSE_START_TRANS;
                GOTO(out, rc);
        }

        rc = dio->dio_insert(env, dto,
                             (const struct dt_rec *)&dsr->dsr_lid,
                             (const struct dt_key *)&dsr->dsr_fid,
                             handle, BYPASS_CAPA, 1);
        rc1 = mdd_trans_stop(env, mdd, rc, handle);
        if (rc < 0) {
                *ecode = MSE_INSERT_RECORD;
        } else if (rc1 < 0) {
                *ecode = MSE_STOP_TRANS;
               rc = rc1;
        }

        if (rc == 0)
                (*updated)++;
        EXIT;

out:
        return rc;
}

static int mdd_scrub_main(void *arg)
{
        struct lu_env             env;
        struct dt_scrub_rec       dsr;
        struct lu_object_hint     hint;
        struct mdd_device        *mdd         = arg;
        struct scrub_header      *header_mem  = &mdd->mdd_scrub_header_mem;
        struct scrub_header      *header_disk = &mdd->mdd_scrub_header_disk;
        struct scrub_param       *param       = &header_mem->sh_param;
        struct scrub_info_normal *ninfo       = &header_mem->sh_normal;
        struct scrub_info_dryrun *dinfo       = &header_mem->sh_dryrun;
        struct mdd_scrub_it      *msi         = msi_get(mdd->mdd_scrub_it);
        struct ptlrpc_thread     *thread      = &msi->msi_thread;
        struct dt_it             *it          = msi->msi_it;
        const struct dt_it_ops   *iops;
        __u64                    *scanned;
        __u64                    *updated;
        __u64                    *failed;
        __u32                    *rtime;
        __u32                    *speed;
        __u64                    *checkpoint_time;
        union scrub_key          *checkpoint_position;
        __u64                     cur_time;
        union scrub_key           start_position;
        cfs_duration_t            duration;
        struct dt_scrub_set_param dssp;
        struct lu_object         *obj;
        struct dt_key            *key;
        int                       key_size;
        int                       ecode       = MSE_NULL;
        int                       env_rc;
        int                       rc;
        __u32                     count       = 0;
        ENTRY;

        cfs_daemonize("mdd_scrub");
        env_rc = lu_env_init(&env, LCT_MD_THREAD);
        if (env_rc != 0) {
                CERROR("MDD scrub failed to init env: rc = %d\n", env_rc);
                GOTO(out, rc = env_rc);
        }

        iops = &mdd->mdd_scrub_obj->do_index_ops->dio_it;
        key = iops->key(&env, it);
        key_size = iops->key_size(&env, it);

        cfs_down(&mdd->mdd_scrub_header_sem);
        cur_time = cfs_time_current_sec();
        mdd_scrub_convert_key(&start_position, key, key_size);

        header_mem->sh_keysize = key_size;
        if (key_is_zero(&msi->msi_position))
                /* for reset or restored cases */
                header_mem->sh_time_initial_start = cur_time;
        header_mem->sh_time_latest_start = cur_time;
        header_mem->sh_position_latest_start = start_position;
        if (param->sp_flags & SPF_DRYRUN) {
                header_mem->sh_current_status = SS_DRURUN_SACNNING;
                scanned = &dinfo->sid_items_scanned;
                updated = &dinfo->sid_items_updated_prior;
                failed = &dinfo->sid_items_failed;
                rtime = &dinfo->sid_time;
                speed = &dinfo->sid_speed;
                checkpoint_time = &dinfo->sid_time_last_checkpoint;
                checkpoint_position= &dinfo->sid_position_last_checkpoint;
        } else {
                header_mem->sh_current_status = SS_REBUILDING;
                scanned = &ninfo->sin_items_scanned;
                updated = &ninfo->sin_items_updated;
                failed = &ninfo->sin_items_failed;
                rtime = &ninfo->sin_time;
                speed = &ninfo->sin_speed;
                checkpoint_time = &ninfo->sin_time_last_checkpoint;
                checkpoint_position= &ninfo->sin_position_last_checkpoint;
        }

        rc = mdd_scrub_store_header(&env, mdd, header_mem, header_disk);
        msi->msi_position = start_position;
        msi->msi_checkpoint_last = cfs_time_current();
        msi->msi_checkpoint_next = msi->msi_checkpoint_last +
                                cfs_time_seconds(param->sp_checkpoint_interval);
        cfs_up(&mdd->mdd_scrub_header_sem);
        if (rc != 0) {
                CERROR("MDD scrub failed to initially store header: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        CDEBUG(D_SCRUB, "MDD start scrub: flags = %u, pid = %d\n",
               param->sp_flags, cfs_curproc_pid());

        cfs_spin_lock(&mdd->mdd_scrub_lock);
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&mdd->mdd_scrub_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);

        while (1) {
                rc = iops->next(&env, it);
                if (rc != 0)
                        /* rc > 0: Completed.
                         * rc < 0: Lower layer iterator exit for failure. */
                        break;

                count++;
                msi->msi_new_scanned++;
                dsr.dsr_valid = 0;
                rc = iops->rec(&env, it, (struct dt_rec *)&dsr, 0);
                if (dsr.dsr_valid & DSV_PRIOR_MORE)
                        msi->msi_has_prior = 1;
                if (rc != 0) {
                        key = iops->key(&env, it);
                        mdd_scrub_convert_key(&msi->msi_position, key,key_size);
                        if (rc < 0)
                                ecode = MSE_READ_RECORD;
                        goto check;
                }

                LASSERT(dsr.dsr_valid & DSV_LOCAL_ID);
                mdd_scrub_lid_to_position(&msi->msi_position, &dsr.dsr_lid);

                /* XXX: Only process normal fid now. */
                if (!(dsr.dsr_valid & DSV_FID_NOR))
                        goto check;

                hint.loh_lid = dsr.dsr_lid;
                hint.loh_flags = LOH_F_LID | LOH_F_SCRUB;
                obj = lu_object_find(&env, &mdd->mdd_md_dev.md_lu_dev,
                                     &dsr.dsr_fid, NULL, &hint);
                if (IS_ERR(obj)) {
                        rc = PTR_ERR(obj);
                        ecode = MSE_FIND_OBJECT;
                        goto check;
                }

                /* XXX: process SPF_OI_REBUILD. */
                /* Do not update the mapping if:
                 * 1) new created entry.
                 * 2) updated already, maybe not committed yet, do not care. */
                if (hint.loh_flags & LOH_F_UNMATCHED &&
                    param->sp_flags & SPF_OI_REBUILD)
                        rc = mdd_scrub_oi_rebuild(&env, mdd, &dsr, &ecode,
                                                  updated);

                /* XXX: LINKEA, QUOTA and others can be processed later. */

                lu_object_put(&env, obj);

check:
                if (rc < 0) {
                        LASSERT(ecode != MSE_NULL);

                        (*failed)++;
                        if (param->sp_flags & SPF_FAILOUT)
                                mdd_scrub_print(D_ERROR, key, key_size, rc,
                                                ecode);
                        else
                                mdd_scrub_print(D_SCRUB, key, key_size, rc,
                                                ecode);
                }

                /* XXX: Different RPCs maybe trigger the same unmatched object
                 *      updating. */
                if (dsr.dsr_valid & DSV_PRIOR) {
                        dssp.dssp_flags = DSSF_WAKEUP_PRIOR;
                        iops->set(&env, it, &dssp);
                }

                if ((rc < 0) && (param->sp_flags & SPF_FAILOUT))
                        break;

                if (cfs_time_beforeq(msi->msi_checkpoint_next,
                                     cfs_time_current())) {
                        duration = cfs_time_current() -
                                                  msi->msi_checkpoint_last;

                        cfs_down(&mdd->mdd_scrub_header_sem);
                        *scanned += msi->msi_new_scanned;
                        *rtime += cfs_duration_sec(duration + HALF_SEC);
                        *speed = msi->msi_new_scanned * CFS_HZ / duration;
                        *checkpoint_time = cfs_time_current_sec();
                        *checkpoint_position = msi->msi_position;

                        rc = mdd_scrub_store_header(&env, mdd, header_mem,
                                                    header_disk);
                        msi->msi_new_scanned = 0;
                        msi->msi_checkpoint_last = cfs_time_current();
                        msi->msi_checkpoint_next = msi->msi_checkpoint_last +
                                cfs_time_seconds(param->sp_checkpoint_interval);
                        cfs_up(&mdd->mdd_scrub_header_sem);
                        if (rc != 0) {
                                mdd_scrub_print(D_ERROR,
                                        (struct dt_key *)&(msi->msi_position),
                                        key_size, rc, MSE_CHECKPOINT);
                                GOTO(out, rc);
                        }
                }

                if (msi->msi_sleep_jif > 0 && count >= msi->msi_sleep_rate) {
                        struct l_wait_info lwi;

                        lwi = LWI_TIMEOUT_INTR(msi->msi_sleep_jif, NULL,
                                               LWI_ON_SIGNAL_NOOP, NULL);
                        l_wait_event(thread->t_ctl_waitq,
                                     msi->msi_has_prior ||
                                     !thread_is_running(thread),
                                     &lwi);
                        count = 0;
                }

                if (unlikely(!thread_is_running(thread))) {
                        rc = 0;
                        break;
                }

                if (msi->msi_has_prior)
                        msi->msi_has_prior = 0;
        }

        duration = cfs_time_current() - msi->msi_checkpoint_last;
        cfs_down(&mdd->mdd_scrub_header_sem);
        cur_time = cfs_time_current_sec();
        if (msi->msi_new_scanned > 0) {
                *scanned += msi->msi_new_scanned;
                *rtime += cfs_duration_sec(duration + HALF_SEC);
                if (duration == 0)
                        *speed = msi->msi_new_scanned * CFS_HZ;
                else
                        *speed = msi->msi_new_scanned * CFS_HZ / duration;
                *checkpoint_time = cur_time;
                *checkpoint_position = msi->msi_position;
        }
        if (rc > 0) {
                if (param->sp_flags & SPF_DRYRUN) {
                        if (key_is_zero(&dinfo->sid_position_first_unmatched))
                                dinfo->sid_position_first_unmatched =
                                                        *checkpoint_position;
                        header_mem->sh_current_status = SS_DRURUN_SACNNED;
                } else {
                        header_mem->sh_success_count++;
                        header_mem->sh_time_last_complete = cur_time;
                        header_mem->sh_current_status = SS_COMPLETED;
                }
        } else if (rc == 0) {
                header_mem->sh_current_status = SS_PAUSED;
        } else {
                header_mem->sh_current_status = SS_FAILED;
        }

        rc = mdd_scrub_store_header(&env, mdd, header_mem, header_disk);
        cfs_up(&mdd->mdd_scrub_header_sem);
        if (rc != 0) {
                CERROR("MDD scrub failed to finally store header: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        EXIT;

out:
        CDEBUG(D_SCRUB, "MDD stop scrub: pid = %d, rc = %d\n",
               cfs_curproc_pid(), rc);

        msi_stop_it(&env, mdd->mdd_scrub_obj, msi->msi_it);
        cfs_spin_lock(&mdd->mdd_scrub_lock);
        mdd->mdd_scrub_it = NULL;
        msi->msi_it = NULL;
        thread_set_flags(thread, SVC_STOPPED);
        cfs_spin_unlock(&mdd->mdd_scrub_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        msi_put(msi);

        if (env_rc == 0)
                lu_env_fini(&env);

        return rc;
}

static int mdd_declare_scrub_store_fid(const struct lu_env *env,
                                       struct dt_object *root,
                                       const struct dt_rec *rec,
                                       const struct dt_key *key,
                                       struct thandle *handle)
{
        int rc;

        rc = dt_declare_delete(env, root, key, handle);
        if (rc != 0)
                return rc;

        rc = dt_declare_insert(env, root, rec, key, handle);
        if (rc == 0)
                handle->th_sync = 1;
        return rc;
}

static int mdd_scrub_store_fid(const struct lu_env *env, struct mdd_device *mdd)
{
        struct lu_fid *fid = &mdd_env_info(env)->mti_fid;
        struct dt_object *root = NULL;
        struct thandle *handle = NULL;
        const struct dt_index_operations *dio;
        struct dt_key *key = (struct dt_key *)mdd_scrub_name;
        int rc;
        ENTRY;

        root = dt_store_resolve(env, mdd->mdd_child, "", fid);
        if (IS_ERR(root)) {
                rc = PTR_ERR(root);
                CERROR("Fail to parse root for scrub_store_fid: rc = %d\n", rc);
                GOTO(out, rc);
        }

        rc = dt_try_as_dir(env, root);
        if (rc == 0)
                GOTO(out, rc = -ENOTDIR);

        dio = root->do_index_ops;
        fid = (struct lu_fid *)lu_object_fid(&mdd->mdd_scrub_obj->do_lu);

        handle = mdd_trans_create(env, mdd);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        rc = mdd_declare_scrub_store_fid(env, root, (struct dt_rec *)fid, key,
                                         handle);
        if (rc != 0) {
                CERROR("Fail to declare trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        rc = mdd_trans_start(env, mdd, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for scrub_store_fid: rc = %d\n",
                       rc);
                GOTO(out, rc);
        }

        /* Delete the old name entry, which does not contain fid. */
        rc = dio->dio_delete(env, root, key, handle, BYPASS_CAPA);
        if (rc != 0) {
                CERROR("Fail to delete old name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                GOTO(out, rc);
        }

        /* Insert the name entry with the same name, then the fid will be stored
         * when new entry inserted. */
        rc = dio->dio_insert(env, root, (const struct dt_rec *)fid, key, handle,
                             BYPASS_CAPA, 1);
        if (rc != 0) {
                CERROR("Fail to insert new name entry for scrub_store_fid: "
                       "rc = %d\n", rc);
                GOTO(out, rc);
        }

        EXIT;

out:
        if (handle != NULL && !IS_ERR(handle))
                mdd_trans_stop(env, mdd, rc, handle);
        if (root != NULL && !IS_ERR(root))
                lu_object_put(env, &root->do_lu);
        return rc;
}

int mdd_scrub_init(const struct lu_env *env, struct mdd_device *m,
                   int restored)
{
        struct dt_object *dto = m->mdd_scrub_obj;
        struct scrub_header *header_mem = &m->mdd_scrub_header_mem;
        struct scrub_header *header_disk = &m->mdd_scrub_header_disk;
        struct lu_buf *buf;
        loff_t pos = 0;
        int store_header = 0;
        int trigger_scrub = 0;
        int rc;
        ENTRY;

        buf = mdd_buf_get(env, header_disk, sizeof(*header_disk));
        rc = dto->do_body_ops->dbo_read(env, dto, buf, &pos, BYPASS_CAPA);
        if (rc == 0) {
                mdd_init_scrub_header(header_mem,
                                      restored ? SS_RESTORED : SS_INIT);
                store_header = 1;
        } else if (rc != buf->lb_len) {
                CERROR("Fail to read header for scrub: rc = %d, "
                       "expected = %d.\n", rc, (int)buf->lb_len);
                RETURN(rc < 0 ? rc : -EFAULT);
        } else {
                mdd_scrub_header_to_cpu(header_mem, header_disk);
                if (header_mem->sh_magic != SCRUB_MAGIC) {
                        CERROR("Invalid scrub header: "
                               "magic = 0x%x, expected = 0x%x.\n",
                               header_mem->sh_magic, SCRUB_MAGIC);
                        RETURN(-EINVAL);
                }

                switch (header_mem->sh_current_status) {
                case SS_INIT:
                case SS_COMPLETED:
                case SS_DRURUN_SACNNED:
                case SS_PAUSED:
                        if (restored != 0) {
                                header_mem->sh_current_status = SS_RESTORED;
                                store_header = 1;
                                trigger_scrub = 1;
                        }
                        break;
                case SS_RESTORED:
                        trigger_scrub = 1;
                        break;
                case SS_REBUILDING:
                case SS_DRURUN_SACNNING:
                        if (restored != 0)
                                header_mem->sh_current_status = SS_RESTORED;
                        else
                                /* System failed to change scrub status before
                                 * exit last time. */
                                header_mem->sh_current_status = SS_FAILED;
                        store_header = 1;
                        trigger_scrub = 1;
                        break;
                case SS_FAILED:
                        if (restored != 0) {
                                header_mem->sh_current_status = SS_RESTORED;
                                store_header = 1;
                        }
                        trigger_scrub = 1;
                        break;
                default:
                        CERROR("Unknown scrub status %d\n",
                               header_mem->sh_current_status);
                        RETURN(-EINVAL);
                }
        }

        if (store_header != 0) {
                rc = mdd_scrub_store_header(env, m, header_mem, header_disk);
                if (rc != 0) {
                        CERROR("Fail to store scrub header: rc = %d\n", rc);
                        RETURN(rc);
                }
        }

        if (restored != 0) {
                rc = mdd_scrub_store_fid(env, m);
                if (rc != 0)
                        RETURN(rc);
        }

        if (trigger_scrub != 0) {
                rc = mdd_scrub_start(env, m, NULL, 0);
                if (rc != 0)
                        /* Allow MDT to mount up even if OI Scrub auto trigger
                         * failure. */
                        CWARN("Fail to start scrub: rc = %d\n", rc);
        }

        RETURN(0);
}

int mdd_scrub_start(const struct lu_env *env, struct mdd_device *mdd,
                    struct start_scrub_param *param, int reset)
{
        struct dt_scrub_param    *dsp    = &mdd_env_info(env)->mti_scrub_param;
        struct scrub_header      *header = &mdd->mdd_scrub_header_mem;
        struct scrub_param       *sp     = &header->sh_param;
        struct scrub_info_normal *ninfo  = &header->sh_normal;
        struct scrub_info_dryrun *dinfo  = &header->sh_dryrun;
        struct l_wait_info        lwi    = { 0 };
        struct mdd_scrub_it      *msi    = NULL;
        struct dt_it             *it;
        int                       method;
        int                       rc;
        ENTRY;

        cfs_down(&mdd->mdd_scrub_ctl_sem);
        if (mdd->mdd_scrub_it != NULL)
                GOTO(out, rc = -EALREADY);

        msi = msi_init(mdd);
        if (unlikely(msi == NULL))
                GOTO(out, rc = -ENOMEM);

        /* XXX: check "param->ssp_version != header->sh_version" in future. */

        if (header->sh_current_status == SS_RESTORED)
                mdd_init_scrub_param(sp);

        if (param == NULL) {
                /* For restored case, the auto triggered OI Scrub should not
                 * dryun.
                 *
                 * 'reset != 0' means it is triggered by parallel PRC.
                 * Under such case, OI Scrub should not dryrun. */
                if (reset != 0)
                        sp->sp_flags &= ~SPF_DRYRUN;
        } else {
                /* It is from user space call. */
                cfs_down(&mdd->mdd_scrub_header_sem);
                if (param->ssp_valid & SSP_METHOD)
                        sp->sp_method = param->ssp_param.sp_method;
                if (param->ssp_valid & SSP_CHECKPOING_INTERVAL) {
                        if (param->ssp_param.sp_checkpoint_interval >
                                                        SPM_CHECKPOING_INTERVAL)
                                sp->sp_checkpoint_interval =
                                                        SPM_CHECKPOING_INTERVAL;
                        else if (param->ssp_param.sp_checkpoint_interval < 1)
                                sp->sp_checkpoint_interval = 1;
                        else
                                sp->sp_checkpoint_interval =
                                        param->ssp_param.sp_checkpoint_interval;
                }
                if (param->ssp_valid & SSP_SPEED_LIMIT)
                        sp->sp_speed_limit = param->ssp_param.sp_speed_limit;
                if (param->ssp_valid & SSP_ITERATOR_WINDOW)
                        sp->sp_pipeline_window =
                                        param->ssp_param.sp_pipeline_window;

                if (param->ssp_valid & SSP_ERROR_HANDLE) {
                        if (param->ssp_param.sp_flags & SPF_FAILOUT)
                                sp->sp_flags |= SPF_FAILOUT;
                        else
                                sp->sp_flags &= ~SPF_FAILOUT;
                }
                if (param->ssp_valid & SSP_DRYRUN) {
                        if (param->ssp_param.sp_flags & SPF_DRYRUN)
                                sp->sp_flags |= SPF_DRYRUN;
                        else
                                sp->sp_flags &= ~SPF_DRYRUN;
                }

                if (param->ssp_valid & SSP_OI_REBUILD) {
                        if (param->ssp_param.sp_flags & SPF_OI_REBUILD)
                                sp->sp_flags |= SPF_OI_REBUILD;
                        else
                                sp->sp_flags &= ~SPF_OI_REBUILD;
                }
                if (param->ssp_valid & SSP_IGIF) {
                        if (param->ssp_param.sp_flags & SPF_IGIF)
                                sp->sp_flags |= SPF_IGIF;
                        else
                                sp->sp_flags &= ~SPF_IGIF;
                }
                cfs_up(&mdd->mdd_scrub_header_sem);
        }

        /* Initial case. */
        if (header->sh_keysize == 0) {
                LASSERT(key_is_zero(&ninfo->sin_position_last_checkpoint));
                LASSERT(key_is_zero(&dinfo->sid_position_last_checkpoint));
                LASSERT(key_is_zero(&dinfo->sid_position_first_unmatched));
        }

        /* 'reset != 0' means it is triggered by parallel PRC. Under such case,
         * if the status is 'SS_COMPLETED', we need to restart iteration from
         * from the beginning. */
        if ((header->sh_current_status == SS_RESTORED) ||
            (param != NULL && param->ssp_valid & SSP_RESET) ||
            (reset != 0 && (header->sh_current_status == SS_COMPLETED ||
                            header->sh_current_status == SS_DRURUN_SACNNED))) {
                msi->msi_position = zero_key;
                mdd_init_scrub_ninfo(ninfo);
                mdd_init_scrub_dinfo(dinfo);
        } else {
                if (sp->sp_flags & SPF_DRYRUN) {
                        if (key_cmp(&ninfo->sin_position_last_checkpoint,
                                    &dinfo->sid_position_last_checkpoint,
                                    header->sh_keysize) > 0)
                                msi->msi_position =
                                        ninfo->sin_position_last_checkpoint;
                        else
                                msi->msi_position =
                                        dinfo->sid_position_last_checkpoint;
                        if (key_cmp(&ninfo->sin_position_last_checkpoint,
                                    &dinfo->sid_position_first_unmatched,
                                    header->sh_keysize) >= 0)
                                dinfo->sid_position_first_unmatched = zero_key;
                } else {
                        if (key_cmp(&dinfo->sid_position_first_unmatched,
                                    &ninfo->sin_position_last_checkpoint,
                                    header->sh_keysize) > 0)
                                msi->msi_position =
                                        dinfo->sid_position_first_unmatched;
                        else
                                msi->msi_position =
                                        ninfo->sin_position_last_checkpoint;
                }
        }

        mdd_scrub_msi_set_speed(sp, msi);

        method = sp->sp_method;
        dsp->dsp_window = sp->sp_pipeline_window;
        dsp->dsp_flags = 0;
        if (sp->sp_flags & SPF_OI_REBUILD)
                dsp->dsp_flags |= DSF_OI_REBUILD;
        if (sp->sp_flags & SPF_IGIF)
                dsp->dsp_flags |= DSF_IGIF;
        if (!key_is_zero(&msi->msi_position)) {
                LASSERT(header->sh_keysize != 0);

                key_inc(&msi->msi_position, header->sh_keysize);
        }
        mdd_scrub_position_to_lid(&dsp->dsp_lid, &msi->msi_position);
        dsp->dsp_notify = msi_prior_notify;
        dsp->dsp_data = msi;

        it = msi_start_it(env, mdd, method, dsp);
        if (IS_ERR(it)) {
                rc = PTR_ERR(it);
                CERROR("Fail at msi_start_it with: method = %d, rc = %d\n",
                       method, rc);
                if (param != NULL) {
                        cfs_down(&mdd->mdd_scrub_header_sem);
                        mdd_scrub_param_to_cpu(sp,
                                &mdd->mdd_scrub_header_disk.sh_param);
                        cfs_up(&mdd->mdd_scrub_header_sem);
                }
                GOTO(out, rc);
        }

        cfs_spin_lock(&mdd->mdd_scrub_lock);
        msi->msi_it = it;
        mdd->mdd_scrub_it = msi;
        cfs_spin_unlock(&mdd->mdd_scrub_lock);

        /* Save start position before mdd_scrub_main started. */
        if (param != NULL)
                param->ssp_start_point = msi->msi_position;

        rc = cfs_create_thread(mdd_scrub_main, mdd, 0);
        if (rc < 0) {
                CERROR("Cannot start mdd scrub thread: rc %d\n", rc);
                msi_stop_it(env, mdd->mdd_scrub_obj, it);
                cfs_spin_lock(&mdd->mdd_scrub_lock);
                mdd->mdd_scrub_it = NULL;
                msi->msi_it = NULL;
                cfs_spin_unlock(&mdd->mdd_scrub_lock);
                GOTO(out, rc);
        }

        l_wait_event(msi->msi_thread.t_ctl_waitq,
                     thread_is_running(&msi->msi_thread) ||
                     thread_is_stopped(&msi->msi_thread),
                     &lwi);

        if (param != NULL) {
                param->ssp_param = *sp;
                param->ssp_version = header->sh_version;
                param->ssp_keysize = header->sh_keysize;
        }

        GOTO(out, rc = 0);

out:
        cfs_up(&mdd->mdd_scrub_ctl_sem);
        if (msi != NULL)
                msi_put(msi);
        return rc;
}

int mdd_scrub_stop(const struct lu_env *env, struct mdd_device *mdd)
{
        struct l_wait_info    lwi    = { 0 };
        struct mdd_scrub_it  *msi;
        struct ptlrpc_thread *thread;
        ENTRY;

        cfs_down(&mdd->mdd_scrub_ctl_sem);
        cfs_spin_lock(&mdd->mdd_scrub_lock);
        if (unlikely(mdd->mdd_scrub_it == NULL)) {
                cfs_spin_unlock(&mdd->mdd_scrub_lock);
                cfs_up(&mdd->mdd_scrub_ctl_sem);
                RETURN(-EALREADY);
        }

        msi = msi_get(mdd->mdd_scrub_it);
        thread = &msi->msi_thread;
        LASSERT(!thread_is_stopped(thread));

        thread_set_flags(thread, SVC_STOPPING);
        cfs_spin_unlock(&mdd->mdd_scrub_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        l_wait_event(thread->t_ctl_waitq, thread_is_stopped(thread), &lwi);
        LASSERT(mdd->mdd_scrub_it == NULL);

        msi_put(msi);
        cfs_up(&mdd->mdd_scrub_ctl_sem);

        RETURN(0);
}

int mdd_scrub_show(const struct lu_env *env, struct mdd_device *mdd,
                   struct scrub_show *show)
{
        struct scrub_header *iheader = &mdd->mdd_scrub_header_mem;
        struct scrub_header *oheader;
        ENTRY;

        if (show == NULL)
                RETURN(0);
        else
                oheader = &show->ss_header;

        cfs_down(&mdd->mdd_scrub_header_sem);
        *oheader = *iheader;
        cfs_spin_lock(&mdd->mdd_scrub_lock);
        if (mdd->mdd_scrub_it != NULL) {
                struct mdd_scrub_it *msi = mdd->mdd_scrub_it;

                show->ss_current_position = msi->msi_position;
                if (iheader->sh_current_status == SS_REBUILDING) {
                        struct scrub_info_normal *ninfo = &oheader->sh_normal;
                        cfs_duration_t duration = cfs_time_current() -
                                                  msi->msi_checkpoint_last;

                        if (likely(duration > 0)) {
                                ninfo->sin_items_scanned +=msi->msi_new_scanned;
                                ninfo->sin_time = ninfo->sin_time +
                                          cfs_duration_sec(duration + HALF_SEC);
                                ninfo->sin_speed =
                                       msi->msi_new_scanned * CFS_HZ / duration;
                        }
                } else if (iheader->sh_current_status == SS_DRURUN_SACNNING) {
                        struct scrub_info_dryrun *dinfo = &oheader->sh_dryrun;
                        cfs_duration_t duration = cfs_time_current() -
                                                  msi->msi_checkpoint_last;

                        if (likely(duration > 0)) {
                                dinfo->sid_items_scanned +=msi->msi_new_scanned;
                                dinfo->sid_time = dinfo->sid_time +
                                          cfs_duration_sec(duration + HALF_SEC);
                                dinfo->sid_speed =
                                       msi->msi_new_scanned * CFS_HZ / duration;
                        }
                }
        }
        cfs_spin_unlock(&mdd->mdd_scrub_lock);
        cfs_up(&mdd->mdd_scrub_header_sem);

        RETURN(0);
}
