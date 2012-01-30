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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdd/mdd_lproc.c
 *
 * Lustre Metadata Server (mdd) routines
 *
 * Author: Wang Di <wangdi@clusterfs.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>
#include <linux/poll.h>
#include <obd.h>
#include <obd_class.h>
#include <lustre_ver.h>
#include <obd_support.h>
#include <lprocfs_status.h>
#include <lu_time.h>
#include <lustre_log.h>
#include <lustre/lustre_idl.h>
#include <libcfs/libcfs_string.h>

#include "mdd_internal.h"

static const char *mdd_counter_names[LPROC_MDD_NR] = {
};

int mdd_procfs_init(struct mdd_device *mdd, const char *name)
{
        struct lprocfs_static_vars lvars;
        struct lu_device    *ld = &mdd->mdd_md_dev.md_lu_dev;
        struct obd_type     *type;
        int                  rc;
        ENTRY;

        type = ld->ld_type->ldt_obd_type;

        LASSERT(name != NULL);
        LASSERT(type != NULL);

        /* Find the type procroot and add the proc entry for this device */
        lprocfs_mdd_init_vars(&lvars);
        mdd->mdd_proc_entry = lprocfs_register(name, type->typ_procroot,
                                               lvars.obd_vars, mdd);
        if (IS_ERR(mdd->mdd_proc_entry)) {
                rc = PTR_ERR(mdd->mdd_proc_entry);
                CERROR("Error %d setting up lprocfs for %s\n",
                       rc, name);
                mdd->mdd_proc_entry = NULL;
                GOTO(out, rc);
        }

        rc = lu_time_init(&mdd->mdd_stats,
                          mdd->mdd_proc_entry,
                          mdd_counter_names, ARRAY_SIZE(mdd_counter_names));

        EXIT;
out:
        if (rc)
               mdd_procfs_fini(mdd);
        return rc;
}

int mdd_procfs_fini(struct mdd_device *mdd)
{
        if (mdd->mdd_stats)
                lu_time_fini(&mdd->mdd_stats);

        if (mdd->mdd_proc_entry) {
                 lprocfs_remove(&mdd->mdd_proc_entry);
                 mdd->mdd_proc_entry = NULL;
        }
        RETURN(0);
}

void mdd_lprocfs_time_start(const struct lu_env *env)
{
        lu_lprocfs_time_start(env);
}

void mdd_lprocfs_time_end(const struct lu_env *env, struct mdd_device *mdd,
                          int idx)
{
        lu_lprocfs_time_end(env, mdd->mdd_stats, idx);
}

static int lprocfs_wr_atime_diff(struct file *file, const char *buffer,
                                 unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        char kernbuf[20], *end;
        unsigned long diff = 0;

        if (count > (sizeof(kernbuf) - 1))
                return -EINVAL;

        if (cfs_copy_from_user(kernbuf, buffer, count))
                return -EFAULT;

        kernbuf[count] = '\0';

        diff = simple_strtoul(kernbuf, &end, 0);
        if (kernbuf == end)
                return -EINVAL;

        mdd->mdd_atime_diff = diff;
        return count;
}

static int lprocfs_rd_atime_diff(char *page, char **start, off_t off,
                                 int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;

        *eof = 1;
        return snprintf(page, count, "%lu\n", mdd->mdd_atime_diff);
}


/**** changelogs ****/
static int lprocfs_rd_changelog_mask(char *page, char **start, off_t off,
                                     int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;
        int i = 0, rc = 0;

        *eof = 1;
        while (i < CL_LAST) {
                if (mdd->mdd_cl.mc_mask & (1 << i))
                        rc += snprintf(page + rc, count - rc, "%s ",
                                       changelog_type2str(i));
                i++;
        }
        return rc;
}

static int lprocfs_wr_changelog_mask(struct file *file, const char *buffer,
                                     unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        char *kernbuf;
        int rc;
        ENTRY;

        if (count >= CFS_PAGE_SIZE)
                RETURN(-EINVAL);
        OBD_ALLOC(kernbuf, CFS_PAGE_SIZE);
        if (kernbuf == NULL)
                RETURN(-ENOMEM);
        if (cfs_copy_from_user(kernbuf, buffer, count))
                GOTO(out, rc = -EFAULT);
        kernbuf[count] = 0;

        rc = cfs_str2mask(kernbuf, changelog_type2str, &mdd->mdd_cl.mc_mask,
                          CHANGELOG_MINMASK, CHANGELOG_ALLMASK);
        if (rc == 0)
                rc = count;
out:
        OBD_FREE(kernbuf, CFS_PAGE_SIZE);
        return rc;
}

struct cucb_data {
        char *page;
        int count;
        int idx;
};

static int lprocfs_changelog_users_cb(struct llog_handle *llh,
                                      struct llog_rec_hdr *hdr, void *data)
{
        struct llog_changelog_user_rec *rec;
        struct cucb_data *cucb = (struct cucb_data *)data;

        LASSERT(llh->lgh_hdr->llh_flags & LLOG_F_IS_PLAIN);

        rec = (struct llog_changelog_user_rec *)hdr;

        cucb->idx += snprintf(cucb->page + cucb->idx, cucb->count - cucb->idx,
                              CHANGELOG_USER_PREFIX"%-3d "LPU64"\n",
                              rec->cur_id, rec->cur_endrec);
        if (cucb->idx >= cucb->count)
                return -ENOSPC;

        return 0;
}

static int lprocfs_rd_changelog_users(char *page, char **start, off_t off,
                                      int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;
        struct llog_ctxt *ctxt;
        struct cucb_data cucb;
        __u64 cur;

        *eof = 1;

        ctxt = llog_get_context(mdd2obd_dev(mdd),LLOG_CHANGELOG_USER_ORIG_CTXT);
        if (ctxt == NULL)
                return -ENXIO;
        LASSERT(ctxt->loc_handle->lgh_hdr->llh_flags & LLOG_F_IS_CAT);

        cfs_spin_lock(&mdd->mdd_cl.mc_lock);
        cur = mdd->mdd_cl.mc_index;
        cfs_spin_unlock(&mdd->mdd_cl.mc_lock);

        cucb.count = count;
        cucb.page = page;
        cucb.idx = 0;

        cucb.idx += snprintf(cucb.page + cucb.idx, cucb.count - cucb.idx,
                              "current index: "LPU64"\n", cur);

        cucb.idx += snprintf(cucb.page + cucb.idx, cucb.count - cucb.idx,
                              "%-5s %s\n", "ID", "index");

        llog_cat_process(ctxt->loc_handle, lprocfs_changelog_users_cb,
                         &cucb, 0, 0);

        llog_ctxt_put(ctxt);
        return cucb.idx;
}

#ifdef HAVE_QUOTA_SUPPORT
static int mdd_lprocfs_quota_rd_type(char *page, char **start, off_t off,
                                     int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;
        return lprocfs_quota_rd_type(page, start, off, count, eof,
                                     mdd->mdd_obd_dev);
}

static int mdd_lprocfs_quota_wr_type(struct file *file, const char *buffer,
                                     unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        return lprocfs_quota_wr_type(file, buffer, count, mdd->mdd_obd_dev);
}
#endif

static int lprocfs_rd_sync_perm(char *page, char **start, off_t off,
                                int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;

        LASSERT(mdd != NULL);
        return snprintf(page, count, "%d\n", mdd->mdd_sync_permission);
}

static int lprocfs_wr_sync_perm(struct file *file, const char *buffer,
                                unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        int val, rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        mdd->mdd_sync_permission = !!val;
        return count;
}

static int lprocfs_rd_noscrub(char *page, char **start, off_t off,
                                int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;

        LASSERT(mdd != NULL);
        return snprintf(page, count, "%d\n", mdd->mdd_noscrub);
}

static int lprocfs_wr_noscrub(struct file *file, const char *buffer,
                              unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        int val, rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        cfs_down(&mdd->mdd_scrub_header_sem);
        if (!!val != mdd->mdd_noscrub) {
                mdd->mdd_noscrub = !!val;
                /* Notify MDT layer */
                rc = md_do_upcall(NULL, &mdd->mdd_md_dev, MD_NOSCRUB, &val);
                if (rc != 0)
                        mdd->mdd_noscrub = !val;
        }
        cfs_up(&mdd->mdd_scrub_header_sem);
        return rc != 0 ? rc : count;
}

static int lprocfs_rd_scrub_speed_limit(char *page, char **start, off_t off,
                                        int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;

        LASSERT(mdd != NULL);
        return snprintf(page, count, "%u\n",
                        mdd->mdd_scrub_header_mem.sh_param.sp_speed_limit);
}

static int lprocfs_wr_scrub_speed_limit(struct file *file, const char *buffer,
                                        unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        struct scrub_param *sp_mem = &mdd->mdd_scrub_header_mem.sh_param;
        struct scrub_param *sp_disk = &mdd->mdd_scrub_header_disk.sh_param;
        __u32 val, old;
        int rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        cfs_down(&mdd->mdd_scrub_header_sem);
        if (val != sp_mem->sp_speed_limit) {
                old = sp_mem->sp_speed_limit;
                sp_mem->sp_speed_limit = val;
                rc = mdd_scrub_store_param(mdd->mdd_scrub_env, mdd,
                                           &mdd->mdd_scrub_header_mem,
                                           &mdd->mdd_scrub_header_disk);
                if (rc != 0) {
                        sp_mem->sp_speed_limit = old;
                        sp_disk->sp_speed_limit = cpu_to_be32(old);
                } else {
                        cfs_spin_lock(&mdd->mdd_scrub_lock);
                        if (mdd->mdd_scrub_it != NULL)
                                mdd_scrub_msi_set_speed(sp_mem,
                                                        mdd->mdd_scrub_it);
                        cfs_spin_unlock(&mdd->mdd_scrub_lock);
                }
        }
        cfs_up(&mdd->mdd_scrub_header_sem);
        return rc != 0 ? rc : count;
}

static int lprocfs_rd_scrub_error_handle(char *page, char **start, off_t off,
                                         int count, int *eof, void *data)
{
        struct mdd_device *mdd = data;
        char *str;

        LASSERT(mdd != NULL);
        if (mdd->mdd_scrub_header_mem.sh_param.sp_flags & SPF_FAILOUT)
                str = "abort";
        else
                str = "continue";
        return snprintf(page, count, "%s\n", str);
}

static int lprocfs_wr_scrub_error_handle(struct file *file, const char *buffer,
                                         unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        struct scrub_param *sp_mem = &mdd->mdd_scrub_header_mem.sh_param;
        struct scrub_param *sp_disk = &mdd->mdd_scrub_header_disk.sh_param;
        char dummy[12];
        int val, old, rc = 0;

        LASSERT(mdd != NULL);
        if (cfs_copy_from_user(dummy, buffer, 8))
                return -EFAULT;

        if (strcmp(dummy, "abort") == 0) {
                val = 1;
        } else if (strcmp(dummy, "continue") == 0) {
                val = 0;
        } else {
                CWARN("Invalid value for scrub error handle.\n"
                      "The valid value should be: abort|continue\n");
                return -EINVAL;
        }

        cfs_down(&mdd->mdd_scrub_header_sem);
        if (sp_mem->sp_flags & SPF_FAILOUT)
                old = 1;
        else
                old = 0;

        if (val != old) {
                if (val != 0)
                        sp_mem->sp_flags |= SPF_FAILOUT;
                else
                        sp_mem->sp_flags &= ~SPF_FAILOUT;
                rc = mdd_scrub_store_param(mdd->mdd_scrub_env, mdd,
                                           &mdd->mdd_scrub_header_mem,
                                           &mdd->mdd_scrub_header_disk);
                if (rc != 0) {
                        if (old != 0)
                                sp_mem->sp_flags |= SPF_FAILOUT;
                        else
                                sp_mem->sp_flags &= ~SPF_FAILOUT;
                        sp_disk->sp_flags = cpu_to_be16(sp_mem->sp_flags);
                }
        }
        cfs_up(&mdd->mdd_scrub_header_sem);
        return rc != 0 ? rc : count;
}

static int lprocfs_rd_scrub_pipeline_window(char *page, char **start,
                                            off_t off, int count,
                                            int *eof, void *data)
{
        struct mdd_device *mdd = data;

        LASSERT(mdd != NULL);
        return snprintf(page, count, "%u\n",
                        mdd->mdd_scrub_header_mem.sh_param.sp_pipeline_window);
}

static int lprocfs_wr_scrub_pipeline_window(struct file *file,
                                            const char *buffer,
                                            unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        struct scrub_param *sp_mem = &mdd->mdd_scrub_header_mem.sh_param;
        struct scrub_param *sp_disk = &mdd->mdd_scrub_header_disk.sh_param;
        struct dt_scrub_set_param dssp;
        __u32 val, old;
        int rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        cfs_down(&mdd->mdd_scrub_header_sem);
        if (val != sp_mem->sp_pipeline_window) {
                old = sp_mem->sp_pipeline_window;
                sp_mem->sp_pipeline_window = val;
                rc = mdd_scrub_store_param(mdd->mdd_scrub_env, mdd,
                                           &mdd->mdd_scrub_header_mem,
                                           &mdd->mdd_scrub_header_disk);
                if (rc != 0) {
                        sp_mem->sp_pipeline_window = old;
                        sp_disk->sp_pipeline_window = cpu_to_be32(old);
                } else {
                        dssp.dssp_flags = DSSF_ADJUST_WINDOW;
                        dssp.dssp_window = sp_mem->sp_pipeline_window;
                        cfs_spin_lock(&mdd->mdd_scrub_lock);
                        if (mdd->mdd_scrub_it != NULL) {
                                struct mdd_scrub_it *msi = mdd->mdd_scrub_it;

                                if (msi->msi_it != NULL)
                                        mdd->mdd_scrub_obj->do_index_ops->
                                        dio_it.set(NULL, msi->msi_it, &dssp);
                        }
                        cfs_spin_unlock(&mdd->mdd_scrub_lock);
                }

        }
        cfs_up(&mdd->mdd_scrub_header_sem);
        return rc != 0 ? rc : count;
}

static int lprocfs_rd_scrub_checkpoint_interval(char *page, char **start,
                                                off_t off, int count,
                                                int *eof, void *data)
{
        struct mdd_device *mdd = data;

        LASSERT(mdd != NULL);
        return snprintf(page, count, "%u\n",
                        mdd->mdd_scrub_header_mem.
                        sh_param.sp_checkpoint_interval);
}

static int lprocfs_wr_scrub_checkpoint_interval(struct file *file,
                                                const char *buffer,
                                                unsigned long count, void *data)
{
        struct mdd_device *mdd = data;
        struct scrub_param *sp_mem = &mdd->mdd_scrub_header_mem.sh_param;
        struct scrub_param *sp_disk = &mdd->mdd_scrub_header_disk.sh_param;
        __u32 val, old;
        int rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        if (val < 1 || val > SPM_CHECKPOING_INTERVAL) {
                CWARN("Invalid value for scrub checkpoint interval: %d\n"
                      "The valid value should be [1 - %d]\n",
                      val, SPM_CHECKPOING_INTERVAL);
                return -EINVAL;
        }

        cfs_down(&mdd->mdd_scrub_header_sem);
        if (val != sp_mem->sp_checkpoint_interval) {
                old = sp_mem->sp_checkpoint_interval;
                sp_mem->sp_checkpoint_interval = val;
                rc = mdd_scrub_store_param(mdd->mdd_scrub_env, mdd,
                                           &mdd->mdd_scrub_header_mem,
                                           &mdd->mdd_scrub_header_disk);
                if (rc != 0) {
                        sp_mem->sp_checkpoint_interval = old;
                        sp_disk->sp_checkpoint_interval = cpu_to_be32(old);
                } else {
                        cfs_spin_lock(&mdd->mdd_scrub_lock);
                        if (mdd->mdd_scrub_it != NULL)
                                mdd->mdd_scrub_it->msi_checkpoint_next -=
                                                cfs_time_seconds(old -
                                                sp_mem->sp_checkpoint_interval);
                        cfs_spin_unlock(&mdd->mdd_scrub_lock);
                }
        }
        cfs_up(&mdd->mdd_scrub_header_sem);
        return rc != 0 ? rc : count;
}

static struct lprocfs_vars lprocfs_mdd_obd_vars[] = {
        { "atime_diff",      lprocfs_rd_atime_diff, lprocfs_wr_atime_diff, 0 },
        { "changelog_mask",  lprocfs_rd_changelog_mask,
                             lprocfs_wr_changelog_mask, 0 },
        { "changelog_users", lprocfs_rd_changelog_users, 0, 0},
#ifdef HAVE_QUOTA_SUPPORT
        { "quota_type",      mdd_lprocfs_quota_rd_type,
                             mdd_lprocfs_quota_wr_type, 0 },
#endif
        { "sync_permission", lprocfs_rd_sync_perm, lprocfs_wr_sync_perm, 0 },
        { "noscrub",         lprocfs_rd_noscrub, lprocfs_wr_noscrub, 0 },
        { "scrub_speed_limit", lprocfs_rd_scrub_speed_limit,
                               lprocfs_wr_scrub_speed_limit, 0 },
        { "scrub_error_handle", lprocfs_rd_scrub_error_handle,
                                lprocfs_wr_scrub_error_handle, 0 },
        { "scrub_pipeline_window", lprocfs_rd_scrub_pipeline_window,
                                   lprocfs_wr_scrub_pipeline_window, 0 },
        { "scrub_checkpoint_interval", lprocfs_rd_scrub_checkpoint_interval,
                                       lprocfs_wr_scrub_checkpoint_interval, 0},
        { 0 }
};

static struct lprocfs_vars lprocfs_mdd_module_vars[] = {
        { "num_refs",   lprocfs_rd_numrefs, 0, 0 },
        { 0 }
};

void lprocfs_mdd_init_vars(struct lprocfs_static_vars *lvars)
{
        lvars->module_vars  = lprocfs_mdd_module_vars;
        lvars->obd_vars     = lprocfs_mdd_obd_vars;
}

