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
 * lustre/mgs/mgs_nids.c
 *
 * NID table management for lustre.
 *
 * Author: Jinshan Xiong <jinshan.xiong@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MGS
#define D_MGS D_CONFIG

#ifdef __KERNEL__
#include <linux/module.h>
#include <linux/pagemap.h>
#include <linux/fs.h>
#endif

#include <obd.h>
#include <obd_lov.h>
#include <obd_class.h>
#include <lustre_log.h>
#include <obd_ost.h>
#include <libcfs/list.h>
#include <linux/lvfs.h>
#include <lustre_fsfilt.h>
#include <lustre_disk.h>
#include <lustre_param.h>
#include "mgs_internal.h"

static unsigned int ir_timeout;

static int nidtbl_is_sane(struct mgs_nidtbl *tbl)
{
        struct mgs_nidtbl_target *tgt;
        int version = 0;

        LASSERT(cfs_mutex_is_locked(&tbl->mn_lock));
        cfs_list_for_each_entry(tgt, &tbl->mn_targets, mnt_list) {
                if (!tgt->mnt_version)
                        continue;

                if (version >= tgt->mnt_version)
                        return 0;

                version = tgt->mnt_version;
        }
        return 1;
}

/**
 * Fetch nidtbl entries whose version are not less than @version
 */
static int mgs_nidtbl_read(struct obd_device *unused, struct mgs_nidtbl *tbl,
                           struct mgs_nidtbl_vers *vers, int is_client,
                           void *buf, int buflen)
{
        struct mgs_nidtbl_target *tgt;
        struct mgs_nidtbl_entry  *entry;
        struct mgs_target_info   *mti;
        u64 version = vers->mnv_cur;
        bool nobuf = false;
        int rc = 0;
        ENTRY;

        cfs_mutex_lock(&tbl->mn_lock);
        LASSERT(nidtbl_is_sane(tbl));
        vers->mnv_latest = tbl->mn_version;
        /* no more entries ? */
        if (version > tbl->mn_version) {
                version = tbl->mn_version;
                goto out;
        }

        /* iterate over all targets to compose a bitmap by the type of llog.
         * If the llog is for mdts, llog entries for osts will be returned;
         * otherwise, it's for clients, then llog entries for both osts and
         * mdts will be returned.
         */
        cfs_list_for_each_entry(tgt, &tbl->mn_targets, mnt_list) {
                int entry_len = sizeof(*entry);

                if (tgt->mnt_version < version)
                        continue;

                /* mdt only cares about ost. */
                if (!is_client && !tgt->mnt_is_ost)
                        continue;

                /* write target recover information */
                mti  = &tgt->mnt_mti;
                LASSERT(mti->mti_nid_count < MTI_NIDS_MAX);
                entry_len += mti->mti_nid_count * sizeof(lnet_nid_t);
                if (buflen < entry_len) {
                        nobuf = true;
                        break;
                }

                entry = (struct mgs_nidtbl_entry *)buf;
                entry->mne_version   = tgt->mnt_version;
                entry->mne_instance  = mti->mti_instance;
                entry->mne_index     = mti->mti_stripe_index;
                entry->mne_type      = LDD_F_SV_TYPE_OST;
                if (tgt->mnt_is_ost == 0)
                        entry->mne_type = LDD_F_SV_TYPE_MDT;
                entry->mne_pad       = 0;
                entry->mne_nid_type  = 0;
                entry->mne_nid_count = mti->mti_nid_count;
                memcpy(entry->mne_nids, mti->mti_nids,
                       mti->mti_nid_count * sizeof(lnet_nid_t));
                lustre_swab_mgs_nidtbl_entry(entry);

                version = tgt->mnt_version;
                buflen -= entry_len;
                rc     += entry_len;
                buf    += entry_len;
        }
out:
        LASSERT(version <= vers->mnv_latest);
        vers->mnv_cur = nobuf ? version : vers->mnv_latest;
        cfs_mutex_unlock(&tbl->mn_lock);
        LASSERT(ergo(version == 1, rc == 0)); /* get the log first time */

        CDEBUG(D_INFO, "IR: read_logs return with %d, version %llu\n",
               rc, version);
        RETURN(rc);
}

#define NIDTBL_OPEN_VERSION_PRE(obd, tbl)                               \
        struct lvfs_run_ctxt saved;                                     \
        struct file         *file = NULL;                               \
        char                 filename[sizeof(MGS_NIDTBL_DIR) + 8];      \
        u64                  version;                                   \
        loff_t               off = 0;                                   \
        int                  rc;                                        \
        ENTRY;                                                          \
                                                                        \
        LASSERT(cfs_mutex_is_locked(&tbl->mn_lock));                    \
        LASSERT(sizeof(filename) < 32);                                 \
                                                                        \
        sprintf(filename, "%s/%s",                                      \
                MGS_NIDTBL_DIR, tbl->mn_fsdb->fsdb_name);               \
                                                                        \
        push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);

#define NIDTBL_OPEN_VERSION_POST(obd, tbl)                              \
        pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);                    \
        EXIT;                                                           \
        return rc;

static int nidtbl_update_version(struct obd_device *obd, struct mgs_nidtbl *tbl)
{
        NIDTBL_OPEN_VERSION_PRE(obd, tbl);
        file = l_filp_open(filename, O_RDWR|O_CREAT, 0660);
        if (!IS_ERR(file)) {
                version = cpu_to_le64(tbl->mn_version);
                rc = lustre_fwrite(file, &version, sizeof(version), &off);
                if (rc == sizeof(version))
                        rc = 0;
                filp_close(file, 0);
                fsfilt_sync(obd, obd->u.mgs.mgs_sb);
        } else {
                rc = PTR_ERR(file);
        }
        NIDTBL_OPEN_VERSION_POST(obd, tbl);
}

#define MGS_NIDTBL_VERSION_INIT 2

static int nidtbl_read_version(struct obd_device *obd, struct mgs_nidtbl *tbl)
{
        NIDTBL_OPEN_VERSION_PRE(obd, tbl);
        file = l_filp_open(filename, O_RDONLY, 0);
        if (!IS_ERR(file)) {
                rc = lustre_fread(file, &version, sizeof(version), &off);
                if (rc == sizeof(version))
                        rc = cpu_to_le64(version);
                else if (rc == 0)
                        rc = MGS_NIDTBL_VERSION_INIT;
                else
                        CERROR("read version file %s error %d\n", filename, rc);
                filp_close(file, 0);
        } else {
                rc = PTR_ERR(file);
                if (rc == -ENOENT)
                        rc = MGS_NIDTBL_VERSION_INIT;
        }
        NIDTBL_OPEN_VERSION_POST(obd, tbl);
}

static int mgs_nidtbl_write(struct fs_db *fsdb, struct mgs_target_info *mti)
{
        struct mgs_nidtbl        *tbl;
        struct mgs_nidtbl_target *tgt;
        bool found = false;
        int is_ost = !!(mti->mti_flags & LDD_F_SV_TYPE_OST);
        int rc     = 0;
        ENTRY;

        tbl = &fsdb->fsdb_nidtbl;
        cfs_mutex_lock(&tbl->mn_lock);
        cfs_list_for_each_entry(tgt, &tbl->mn_targets, mnt_list) {
                struct mgs_target_info *info = &tgt->mnt_mti;
                if (is_ost == tgt->mnt_is_ost &&
                    mti->mti_stripe_index == info->mti_stripe_index) {
                        found = true;
                        break;
                }
        }
        if (!found) {
                OBD_ALLOC_PTR(tgt);
                if (tgt == NULL)
                        GOTO(out, rc = -ENOMEM);

                CFS_INIT_LIST_HEAD(&tgt->mnt_list);
                tgt->mnt_fs      = tbl;
                tgt->mnt_version = 0;       /* 0 means invalid */
                tgt->mnt_is_ost  = is_ost;

                ++tbl->mn_nr_targets;
        }

        tgt->mnt_version = ++tbl->mn_version;
        tgt->mnt_mti     = *mti;
        cfs_list_move_tail(&tgt->mnt_list, &tbl->mn_targets);

        rc = nidtbl_update_version(fsdb->fsdb_obd, tbl);
        EXIT;

out:
        cfs_mutex_unlock(&tbl->mn_lock);
        if (rc)
                CERROR("Write NID table version for file system %s error %d\n",
                       fsdb->fsdb_name, rc);
        return rc;
}

static void mgs_nidtbl_fini_fs(struct fs_db *fsdb)
{
        struct mgs_nidtbl *tbl = &fsdb->fsdb_nidtbl;
        CFS_LIST_HEAD(head);

        cfs_mutex_lock(&tbl->mn_lock);
        tbl->mn_nr_targets = 0;
        cfs_list_splice_init(&tbl->mn_targets, &head);
        cfs_mutex_unlock(&tbl->mn_lock);

        while (!cfs_list_empty(&head)) {
                struct mgs_nidtbl_target *tgt;
                tgt = list_entry(head.next, struct mgs_nidtbl_target, mnt_list);
                cfs_list_del(&tgt->mnt_list);
                OBD_FREE_PTR(tgt);
        }
}

static int mgs_nidtbl_init_fs(struct fs_db *fsdb)
{
        struct mgs_nidtbl *tbl = &fsdb->fsdb_nidtbl;

        CFS_INIT_LIST_HEAD(&tbl->mn_targets);
        cfs_mutex_init(&tbl->mn_lock);
        tbl->mn_nr_targets = 0;
        tbl->mn_fsdb = fsdb;
        cfs_mutex_lock(&tbl->mn_lock);
        tbl->mn_version = nidtbl_read_version(fsdb->fsdb_obd, tbl);
        cfs_mutex_unlock(&tbl->mn_lock);
        CDEBUG(D_INFO, "IR: current version is %llu\n", tbl->mn_version);

        return 0;
}

/* --------- Imperative Recovery relies on nidtbl stuff ------- */
void mgs_ir_notify_complete(struct fs_db *fsdb)
{
        struct timeval tv;
        cfs_duration_t delta;

        cfs_atomic_set(&fsdb->fsdb_notify_phase, 0);

        /* do statistic */
        fsdb->fsdb_notify_count++;
        delta = cfs_time_sub(cfs_time_current(), fsdb->fsdb_notify_start);
        fsdb->fsdb_notify_total += delta;
        if (delta > fsdb->fsdb_notify_max)
                fsdb->fsdb_notify_max = delta;

        cfs_duration_usec(delta, &tv);
        CDEBUG(D_MGS, "Revoke recover lock of %s completed after %ld.%06lds\n",
               fsdb->fsdb_name, tv.tv_sec, tv.tv_usec);
}

static int mgs_ir_notify(void *arg)
{
        struct fs_db      *fsdb   = arg;
        struct ldlm_res_id resid;

        char name[sizeof(fsdb->fsdb_name) + 20];

        LASSERTF(sizeof(name) < 32, "name is too large to be in stack.\n");
        sprintf(name, "mgs_%s_notify", fsdb->fsdb_name);
        cfs_daemonize(name);

        cfs_complete(&fsdb->fsdb_notify_comp);

        set_user_nice(current, -2);

        mgc_fsname2resid(fsdb->fsdb_name, &resid, CONFIG_T_RECOVER);
        while(1) {
                struct l_wait_info   lwi = { 0 };

                l_wait_event(fsdb->fsdb_notify_waitq,
                             fsdb->fsdb_notify_stop ||
                             cfs_atomic_read(&fsdb->fsdb_notify_phase),
                             &lwi);
                if (fsdb->fsdb_notify_stop)
                        break;

                CDEBUG(D_MGS, "%s woken up, phase is %d\n",
                       name, cfs_atomic_read(&fsdb->fsdb_notify_phase));

                fsdb->fsdb_notify_start = cfs_time_current();
                mgs_revoke_lock(fsdb->fsdb_obd, fsdb, CONFIG_T_RECOVER);
        }

        cfs_complete(&fsdb->fsdb_notify_comp);
        return 0;
}

int mgs_ir_init_fs(struct obd_device *obd, struct fs_db *fsdb)
{
        struct mgs_obd *mgs = &obd->u.mgs;
        int rc;

        if (!ir_timeout)
                ir_timeout = OBD_IR_MGS_TIMEOUT;

        fsdb->fsdb_ir_state = IR_FULL;
        if (cfs_time_before(cfs_time_current_sec(),
                            mgs->mgs_start_time + ir_timeout))
                fsdb->fsdb_ir_state = IR_STARTUP;
        fsdb->fsdb_nonir_clients = 0;
        CFS_INIT_LIST_HEAD(&fsdb->fsdb_clients);

        /* start notify thread */
        fsdb->fsdb_obd = obd;
        cfs_atomic_set(&fsdb->fsdb_notify_phase, 0);
        cfs_waitq_init(&fsdb->fsdb_notify_waitq);
        cfs_init_completion(&fsdb->fsdb_notify_comp);
        rc = cfs_create_thread(mgs_ir_notify, fsdb, CFS_DAEMON_FLAGS);
        if (rc > 0) {
                cfs_wait_for_completion(&fsdb->fsdb_notify_comp);
        } else {
                CERROR("Start notify thread error %d\n", rc);
        }

        mgs_nidtbl_init_fs(fsdb);
        return 0;
}

void mgs_ir_fini_fs(struct obd_device *obd, struct fs_db *fsdb)
{
        if (cfs_test_bit(FSDB_MGS_SELF, &fsdb->fsdb_flags))
                return;

        mgs_fsc_cleanup_by_fsdb(fsdb);

        mgs_nidtbl_fini_fs(fsdb);

        LASSERT(cfs_list_empty(&fsdb->fsdb_clients));

        fsdb->fsdb_notify_stop = 1;
        cfs_waitq_signal(&fsdb->fsdb_notify_waitq);
        cfs_wait_for_completion(&fsdb->fsdb_notify_comp);
}

/* caller must have held fsdb_sem */
static inline void ir_state_graduate(struct fs_db *fsdb)
{
        struct mgs_obd *mgs = &fsdb->fsdb_obd->u.mgs;

        if (fsdb->fsdb_ir_state == IR_STARTUP) {
                if (cfs_time_before(mgs->mgs_start_time + ir_timeout,
                                    cfs_time_current_sec())) {
                        fsdb->fsdb_ir_state = IR_FULL;
                        if (fsdb->fsdb_nonir_clients)
                                fsdb->fsdb_ir_state = IR_PARTIAL;
                }
        }
}

int mgs_ir_update(struct obd_device *obd, struct mgs_target_info *mti)
{
        struct fs_db *fsdb;
        bool notify = true;
        int rc;

        rc = mgs_find_or_make_fsdb(obd, mti->mti_fsname, &fsdb);
        if (rc)
                return rc;

        rc = mgs_nidtbl_write(fsdb, mti);
        if (rc)
                return rc;

        /* check ir state */
        cfs_down(&fsdb->fsdb_sem);
        ir_state_graduate(fsdb);
        switch(fsdb->fsdb_ir_state) {
        case IR_FULL:
                mti->mti_flags |= LDD_F_IR_CAPABLE;
                break;
        case IR_DISABLED:
                notify = false;
        case IR_STARTUP:
        case IR_PARTIAL:
                break;
        default:
                LBUG();
        }
        cfs_up(&fsdb->fsdb_sem);

        LASSERT(ergo(mti->mti_flags & LDD_F_IR_CAPABLE, notify));
        if (notify) {
                CDEBUG(D_MGS, "Try to revoke recover lock of %s\n",
                       fsdb->fsdb_name);
                cfs_atomic_inc(&fsdb->fsdb_notify_phase);
                cfs_waitq_signal(&fsdb->fsdb_notify_waitq);
        }
        return 0;
}

/* NID table can be cached by two entities: Clients and MDTs */
enum {
        IR_CLIENT  = 1,
        IR_MDT     = 2
};

static int delogname(char *logname, char *fsname, int *typ)
{
        char *ptr;
        int   type;
        int   len;

        ptr = strrchr(logname, '-');
        if (ptr == NULL)
                return -EINVAL;

        /* decouple file system name. The llog name may be:
         * - "prefix-fsname", prefix is "cliir" or "mdtir"
         */
        if (strncmp(ptr, "-mdtir", 6) == 0)
                type = IR_MDT;
        else if (strncmp(ptr, "-cliir", 6) == 0)
                type = IR_CLIENT;
        else
                return -EINVAL;

        len = (int)(ptr - logname);
        if (len == 0)
                return -EINVAL;

        memcpy(fsname, logname, len);
        fsname[len] = 0;
        if (typ)
                *typ = type;
        return 0;
}

int mgs_get_ir_logs(struct ptlrpc_request *req)
{
        struct obd_device *obd = req->rq_export->exp_obd;
        struct fs_db      *fsdb;
        struct mgs_nidtbl_vers *vers;
        char              *logname;
        void              *buf = NULL;
        char               fsname[16];
        int                type;
        int                rc = 0;
        ENTRY;

        logname = req_capsule_client_get(&req->rq_pill, &RMF_NAME);
        vers    = req_capsule_client_get(&req->rq_pill, &RMF_MGS_NIDTBL_VERS);
        if (vers == NULL || logname == NULL)
                RETURN(-EINVAL);
        if (strlen(logname) > sizeof(fsname) - 1)
                RETURN(-EINVAL);

        rc = delogname(logname, fsname, &type);
        if (rc)
                RETURN(rc);

        OBD_ALLOC(buf, LLOG_CHUNK_SIZE);
        if (buf == NULL)
                RETURN(-ENOMEM);

        rc = mgs_find_or_make_fsdb(obd, fsname, &fsdb);
        if (rc)
                GOTO(out, rc);

        rc = mgs_nidtbl_read(obd, &fsdb->fsdb_nidtbl, vers,
                             type == IR_CLIENT,
                             buf, LLOG_CHUNK_SIZE);

        if (rc >= 0) {
                struct mgs_nidtbl_vers *tmp;
                int buflen = rc;

                req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_VAL,
                                     RCL_SERVER, buflen);
                rc = req_capsule_server_pack(&req->rq_pill);
                if (rc)
                        GOTO(out, rc);

                tmp = req_capsule_server_get(&req->rq_pill,
                                             &RMF_MGS_NIDTBL_VERS);
                *(struct mgs_nidtbl_vers *)tmp = *vers;

                if (buflen > 0) {
                        void *eatbl;
                        eatbl = req_capsule_server_get(&req->rq_pill,
                                                        &RMF_GETINFO_VAL);
                        memcpy(eatbl, buf, buflen);
                }
                req_capsule_shrink(&req->rq_pill, &RMF_GETINFO_VAL, buflen,
                                   RCL_SERVER);
        }

out:
        OBD_FREE(buf, LLOG_CHUNK_SIZE);
        return rc;
}

static int lprocfs_ir_set_status(struct fs_db *fsdb, const char *buf)
{
        const char *strings[] = IR_STRINGS;
        int         state = -1;
        int         i;

        for (i = 0; i < ARRAY_SIZE(strings); i++) {
                if (strcmp(strings[i], buf) == 0) {
                        state = i;
                        break;
                }
        }
        if (state < 0)
                return -EINVAL;

        CDEBUG(D_MGS, "change fsr state of %s from %s to %s\n",
               fsdb->fsdb_name, strings[fsdb->fsdb_ir_state], strings[state]);
        cfs_down(&fsdb->fsdb_sem);
        if (state == IR_FULL && fsdb->fsdb_nonir_clients)
                state = IR_PARTIAL;
        fsdb->fsdb_ir_state = state;
        cfs_up(&fsdb->fsdb_sem);

        return 0;
}

static int lprocfs_ir_set_timeout(struct fs_db *fsdb, const char *buf)
{
        return -EINVAL;
}

static int lprocfs_ir_clear_stats(struct fs_db *fsdb, const char *buf)
{
        if (*buf)
                return -EINVAL;

        fsdb->fsdb_notify_total = 0;
        fsdb->fsdb_notify_max   = 0;
        fsdb->fsdb_notify_count = 0;
        return 0;
}

static struct lproc_ir_cmd {
        char *name;
        int   namelen;
        int (*handler)(struct fs_db *, const char *);
} ir_cmds[] = {
        { "status=",  7, lprocfs_ir_set_status },
        { "timeout=", 8, lprocfs_ir_set_timeout },
        { "0",        1, lprocfs_ir_clear_stats }
};

int lprocfs_wr_ir_status(struct file *file, const char *buffer,
                         unsigned long count, void *data)
{
        struct fs_db *fsdb = data;
        char *kbuf;
        char *ptr;
        int rc = 0;

        if (count > CFS_PAGE_SIZE)
                return -EINVAL;

        OBD_ALLOC(kbuf, count + 1);
        if (kbuf == NULL)
                return -ENOMEM;

        if (copy_from_user(kbuf, buffer, count)) {
                OBD_FREE(kbuf, count);
                return -EFAULT;
        }

        kbuf[count] = 0; /* buffer is supposed to end with 0 */
        if (kbuf[count - 1] == '\n')
                kbuf[count - 1] = 0;
        ptr = kbuf;

        /* fsname=<file system name> must be the 1st entry */
        while (ptr != NULL) {
                char *tmpptr;
                int i;

                tmpptr = strchr(ptr, ';');
                if (tmpptr)
                        *tmpptr++ = 0;

                rc = -EINVAL;
                for (i = 0; i < ARRAY_SIZE(ir_cmds); i++) {
                        struct lproc_ir_cmd *cmd;
                        int cmdlen;

                        cmd    = &ir_cmds[i];
                        cmdlen = cmd->namelen;
                        if (strncmp(cmd->name, ptr, cmdlen) == 0) {
                                ptr += cmdlen;
                                rc = cmd->handler(fsdb, ptr);
                                break;
                        }
                }
                if (rc)
                        break;

                ptr = tmpptr;
        }
        if (rc)
                CERROR("Unable to process command: %s(%d)\n", ptr, rc);
        OBD_FREE(kbuf, count + 1);
        return rc ?: count;
}

int lprocfs_rd_ir_status(struct seq_file *seq, void *data)
{
        struct fs_db      *fsdb = data;
        struct mgs_nidtbl *tbl  = &fsdb->fsdb_nidtbl;
        const char        *ir_strings[] = IR_STRINGS;

        /* mgs_live_seq_show() already holds fsdb_sem. */
        ir_state_graduate(fsdb);

        seq_printf(seq,
                   "\tstate: %s, nonir clients: %d\n"
                   "\tnidtbl version: %lld\n",
                   ir_strings[fsdb->fsdb_ir_state], fsdb->fsdb_nonir_clients,
                   tbl->mn_version);
        seq_printf(seq, "\tnotify total/max/count: %u/%u/%u\n",
                   fsdb->fsdb_notify_total, fsdb->fsdb_notify_max,
                   fsdb->fsdb_notify_count);
        return 0;
}

int lprocfs_rd_ir_timeout(char *page, char **start, off_t off, int count,
                          int *eof, void *data)
{
        *eof = 1;
        return snprintf(page, count, "%d\n", ir_timeout);
}

int lprocfs_wr_ir_timeout(struct file *file, const char *buffer,
                          unsigned long count, void *data)
{
        return lprocfs_wr_uint(file, buffer, count, &ir_timeout);
}

/* --------------- Handle non IR support clients --------------- */
/* attach a lustre file system to an export */
int mgs_fsc_attach(struct obd_export *exp, char *fsname)
{
        struct mgs_export_data *data = &exp->u.eu_mgs_data;
        struct obd_device *obd       = exp->exp_obd;
        struct fs_db      *fsdb;
        struct mgs_fsc    *fsc     = NULL;
        struct mgs_fsc    *new_fsc = NULL;
        bool               found   = false;
        int                rc;
        ENTRY;

        rc = mgs_find_or_make_fsdb(obd, fsname, &fsdb);
        if (rc)
                RETURN(rc);

        /* allocate a new fsc in case we need it in spinlock. */
        OBD_ALLOC_PTR(new_fsc);
        if (new_fsc == NULL)
                RETURN(-ENOMEM);

        CFS_INIT_LIST_HEAD(&new_fsc->mfc_export_list);
        CFS_INIT_LIST_HEAD(&new_fsc->mfc_fsdb_list);
        new_fsc->mfc_fsdb       = fsdb;
        new_fsc->mfc_export     = class_export_get(exp);
        new_fsc->mfc_ir_capable =
                        !!(exp->exp_connect_flags & OBD_CONNECT_IMP_RECOV);

        rc = -EEXIST;
        cfs_down(&fsdb->fsdb_sem);

        /* tend to find it in export list because this list is shorter. */
        cfs_spin_lock(&data->med_lock);
        cfs_list_for_each_entry(fsc, &data->med_clients, mfc_export_list) {
                if (strcmp(fsname, fsc->mfc_fsdb->fsdb_name) == 0) {
                        found = true;
                        break;
                }
        }
        if (!found) {
                fsc = new_fsc;
                new_fsc = NULL;

                /* add it into export list. */
                cfs_list_add(&fsc->mfc_export_list, &data->med_clients);

                /* add into fsdb list. */
                cfs_list_add(&fsc->mfc_fsdb_list, &fsdb->fsdb_clients);
                if (!fsc->mfc_ir_capable) {
                        ++fsdb->fsdb_nonir_clients;
                        if (fsdb->fsdb_ir_state == IR_FULL)
                                fsdb->fsdb_ir_state = IR_PARTIAL;
                }
                rc = 0;
        }
        cfs_spin_unlock(&data->med_lock);
        cfs_up(&fsdb->fsdb_sem);

        if (new_fsc) {
                class_export_put(new_fsc->mfc_export);
                OBD_FREE_PTR(new_fsc);
        }
        RETURN(rc);
}

void mgs_fsc_cleanup(struct obd_export *exp)
{
        struct mgs_export_data *data = &exp->u.eu_mgs_data;
        struct mgs_fsc *fsc, *tmp;
        CFS_LIST_HEAD(head);

        cfs_spin_lock(&data->med_lock);
        cfs_list_splice_init(&data->med_clients, &head);
        cfs_spin_unlock(&data->med_lock);

        cfs_list_for_each_entry_safe(fsc, tmp, &head, mfc_export_list) {
                struct fs_db *fsdb = fsc->mfc_fsdb;

                LASSERT(fsc->mfc_export == exp);

                cfs_down(&fsdb->fsdb_sem);
                cfs_list_del_init(&fsc->mfc_fsdb_list);
                if (fsc->mfc_ir_capable == 0) {
                        --fsdb->fsdb_nonir_clients;
                        LASSERT(fsdb->fsdb_ir_state != IR_FULL);
                        if (fsdb->fsdb_nonir_clients == 0 &&
                            fsdb->fsdb_ir_state == IR_PARTIAL)
                                fsdb->fsdb_ir_state = IR_FULL;
                }
                cfs_up(&fsdb->fsdb_sem);
                cfs_list_del_init(&fsc->mfc_export_list);
                class_export_put(fsc->mfc_export);
                OBD_FREE_PTR(fsc);
        }
}

/* must be called with fsdb->fsdb_sem held */
void mgs_fsc_cleanup_by_fsdb(struct fs_db *fsdb)
{
        struct mgs_fsc *fsc, *tmp;

        cfs_list_for_each_entry_safe(fsc, tmp, &fsdb->fsdb_clients,
                                     mfc_fsdb_list) {
                struct mgs_export_data *data = &fsc->mfc_export->u.eu_mgs_data;

                LASSERT(fsdb == fsc->mfc_fsdb);
                cfs_list_del_init(&fsc->mfc_fsdb_list);

                cfs_spin_lock(&data->med_lock);
                cfs_list_del_init(&fsc->mfc_export_list);
                cfs_spin_unlock(&data->med_lock);
                class_export_put(fsc->mfc_export);
                OBD_FREE_PTR(fsc);
        }

        fsdb->fsdb_nonir_clients = 0;
        if (fsdb->fsdb_ir_state == IR_PARTIAL)
                fsdb->fsdb_ir_state = IR_FULL;
}
