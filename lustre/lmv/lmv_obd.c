/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) 2002, 2003, 2004, 2005, 2006 Cluster File Systems, Inc.
 *
 *   This file is part of Lustre, http://www.lustre.org.
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LMV
#ifdef __KERNEL__
#include <linux/slab.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <asm/div64.h>
#include <linux/seq_file.h>
#include <linux/namei.h>
#else
#include <liblustre.h>
#endif
#include <linux/ext2_fs.h>

#include <lustre/lustre_idl.h>
#include <lustre_log.h>
#include <obd_support.h>
#include <lustre_lib.h>
#include <lustre_net.h>
#include <obd_class.h>
#include <lprocfs_status.h>
#include <lustre_lite.h>
#include "lmv_internal.h"

/* not defined for liblustre building */
#if !defined(ATOMIC_INIT)
#define ATOMIC_INIT(val) { (val) }
#endif

/* object cache. */
kmem_cache_t *obj_cache;
atomic_t obj_cache_count = ATOMIC_INIT(0);

static void lmv_activate_target(struct lmv_obd *lmv,
                                struct lmv_tgt_desc *tgt,
                                int activate)
{
        if (tgt->active == activate)
                return;

        tgt->active = activate;
        lmv->desc.ld_active_tgt_count += (activate ? 1 : -1);
}

/* Error codes:
 *
 *  -EINVAL  : UUID can't be found in the LMV's target list
 *  -ENOTCONN: The UUID is found, but the target connection is bad (!)
 *  -EBADF   : The UUID is found, but the OBD of the wrong type (!)
 */
static int lmv_set_mdc_active(struct lmv_obd *lmv, struct obd_uuid *uuid,
                              int activate)
{
        struct lmv_tgt_desc *tgt;
        struct obd_device *obd;
        int i, rc = 0;
        ENTRY;

        CDEBUG(D_INFO, "Searching in lmv %p for uuid %s (activate=%d)\n",
               lmv, uuid->uuid, activate);

        spin_lock(&lmv->lmv_lock);
        for (i = 0, tgt = lmv->tgts; i < lmv->desc.ld_tgt_count; i++, tgt++) {
                if (tgt->ltd_exp == NULL)
                        continue;

                CDEBUG(D_INFO, "lmv idx %d is %s conn "LPX64"\n",
                       i, tgt->uuid.uuid, tgt->ltd_exp->exp_handle.h_cookie);

                if (obd_uuid_equals(uuid, &tgt->uuid))
                        break;
        }

        if (i == lmv->desc.ld_tgt_count)
                GOTO(out_lmv_lock, rc = -EINVAL);

        obd = class_exp2obd(tgt->ltd_exp);
        if (obd == NULL)
                GOTO(out_lmv_lock, rc = -ENOTCONN);

        CDEBUG(D_INFO, "Found OBD %s=%s device %d (%p) type %s at LMV idx %d\n",
               obd->obd_name, obd->obd_uuid.uuid, obd->obd_minor, obd,
               obd->obd_type->typ_name, i);
        LASSERT(strcmp(obd->obd_type->typ_name, LUSTRE_MDC_NAME) == 0);

        if (tgt->active == activate) {
                CDEBUG(D_INFO, "OBD %p already %sactive!\n", obd,
                       activate ? "" : "in");
                GOTO(out_lmv_lock, rc);
        }

        CDEBUG(D_INFO, "Marking OBD %p %sactive\n",
               obd, activate ? "" : "in");

        lmv_activate_target(lmv, tgt, activate);

        EXIT;

 out_lmv_lock:
        spin_unlock(&lmv->lmv_lock);
        return rc;
}

static int lmv_notify(struct obd_device *obd, struct obd_device *watched,
                      enum obd_notify_event ev, void *data)
{
        struct obd_uuid *uuid;
        int rc;
        ENTRY;

        if (strcmp(watched->obd_type->typ_name, LUSTRE_MDC_NAME)) {
                CERROR("unexpected notification of %s %s!\n",
                       watched->obd_type->typ_name,
                       watched->obd_name);
                RETURN(-EINVAL);
        }
        uuid = &watched->u.cli.cl_target_uuid;

        /* Set MDC as active before notifying the observer, so the observer can
         * use the MDC normally. */
        rc = lmv_set_mdc_active(&obd->u.lmv, uuid,
                                ev == OBD_NOTIFY_ACTIVE);
        if (rc) {
                CERROR("%sactivation of %s failed: %d\n",
                       ev == OBD_NOTIFY_ACTIVE ? "" : "de",
                       uuid->uuid, rc);
                RETURN(rc);
        }

        if (obd->obd_observer)
                /* pass the notification up the chain. */
                rc = obd_notify(obd->obd_observer, watched, ev, data);

        RETURN(rc);
}

/* this is fake connect function. Its purpose is to initialize lmv and say
 * caller that everything is okay. Real connection will be performed later. */
static int lmv_connect(const struct lu_context *ctx,
                       struct lustre_handle *conn, struct obd_device *obd,
                       struct obd_uuid *cluuid, struct obd_connect_data *data)
{
#ifdef __KERNEL__
        struct proc_dir_entry *lmv_proc_dir;
#endif
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *exp;
        int rc = 0;
        ENTRY;

        rc = class_connect(conn, obd, cluuid);
        if (rc) {
                CERROR("class_connection() returned %d\n", rc);
                RETURN(rc);
        }

        exp = class_conn2export(conn);

        /* we don't want to actually do the underlying connections more than
         * once, so keep track. */
        lmv->refcount++;
        if (lmv->refcount > 1) {
                class_export_put(exp);
                RETURN(0);
        }

        lmv->exp = exp;
        lmv->connected = 0;
        lmv->cluuid = *cluuid;

        if (data)
                lmv->conn_data = *data;

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_register("target_obds", obd->obd_proc_entry,
                                        NULL, NULL);
        if (IS_ERR(lmv_proc_dir)) {
                CERROR("could not register /proc/fs/lustre/%s/%s/target_obds.",
                       obd->obd_type->typ_name, obd->obd_name);
                lmv_proc_dir = NULL;
        }
#endif

        /* all real clients should perform actual connection right away, because
         * it is possible, that LMV will not have opportunity to connect targets
         * and MDC stuff will be called directly, for instance while reading
         * ../mdc/../kbytesfree procfs file, etc. */
        if (data->ocd_connect_flags & OBD_CONNECT_REAL)
                rc = lmv_check_connect(obd);

#ifdef __KERNEL__
        if (rc) {
                if (lmv_proc_dir)
                        lprocfs_remove(lmv_proc_dir);
        }
#endif

        RETURN(rc);
}

static void lmv_set_timeouts(struct obd_device *obd)
{
        struct lmv_tgt_desc *tgts;
        struct lmv_obd *lmv;
        int i;

        lmv = &obd->u.lmv;
        if (lmv->server_timeout == 0)
                return;

        if (lmv->connected == 0)
                return;

        for (i = 0, tgts = lmv->tgts; i < lmv->desc.ld_tgt_count; i++, tgts++) {
                if (tgts->ltd_exp == NULL)
                        continue;

                obd_set_info_async(tgts->ltd_exp, strlen("inter_mds"),
                                   "inter_mds", 0, NULL, NULL);
        }
}

static int lmv_init_ea_size(struct obd_export *exp, int easize,
                            int def_easize, int cookiesize)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int i, rc = 0, change = 0;
        ENTRY;

        if (lmv->max_easize < easize) {
                lmv->max_easize = easize;
                change = 1;
        }
        if (lmv->max_def_easize < def_easize) {
                lmv->max_def_easize = def_easize;
                change = 1;
        }
        if (lmv->max_cookiesize < cookiesize) {
                lmv->max_cookiesize = cookiesize;
                change = 1;
        }
        if (change == 0)
                RETURN(0);

        if (lmv->connected == 0)
                RETURN(0);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                if (lmv->tgts[i].ltd_exp == NULL) {
                        CWARN("%s: NULL export for %d\n", obd->obd_name, i);
                        continue;
                }

                rc = md_init_ea_size(lmv->tgts[i].ltd_exp, easize, def_easize,
                                     cookiesize);
                if (rc) {
                        CERROR("obd_init_ea_size() failed on MDT target %d, "
                               "error %d.\n", i, rc);
                        break;
                }
        }
        RETURN(rc);
}

#define MAX_STRING_SIZE 128

int lmv_connect_mdc(struct obd_device *obd, struct lmv_tgt_desc *tgt)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_uuid *cluuid = &lmv->cluuid;
        struct obd_connect_data *mdc_data = NULL;
        struct obd_uuid lmv_mdc_uuid = { "LMV_MDC_UUID" };
        struct lustre_handle conn = {0, };
        struct obd_device *mdc_obd;
        struct obd_export *mdc_exp;
        int rc;
#ifdef __KERNEL__
        struct proc_dir_entry *lmv_proc_dir;
#endif
        ENTRY;

        /* for MDS: don't connect to yourself */
        if (obd_uuid_equals(&tgt->uuid, cluuid)) {
                CDEBUG(D_CONFIG, "don't connect back to %s\n", cluuid->uuid);
                /* XXX - the old code didn't increment active tgt count.
                 *       should we ? */
                RETURN(0);
        }

        mdc_obd = class_find_client_obd(&tgt->uuid, LUSTRE_MDC_NAME,
                                        &obd->obd_uuid);
        if (!mdc_obd) {
                CERROR("target %s not attached\n", tgt->uuid.uuid);
                RETURN(-EINVAL);
        }

        CDEBUG(D_CONFIG, "connect to %s(%s) - %s, %s FOR %s\n",
                mdc_obd->obd_name, mdc_obd->obd_uuid.uuid,
                tgt->uuid.uuid, obd->obd_uuid.uuid,
                cluuid->uuid);

        if (!mdc_obd->obd_set_up) {
                CERROR("target %s not set up\n", tgt->uuid.uuid);
                RETURN(-EINVAL);
        }

        rc = obd_connect(NULL, &conn, mdc_obd, &lmv_mdc_uuid, &lmv->conn_data);
        if (rc) {
                CERROR("target %s connect error %d\n", tgt->uuid.uuid, rc);
                RETURN(rc);
        }

        mdc_exp = class_conn2export(&conn);
        fld_client_add_target(&lmv->lmv_fld, mdc_exp);

        mdc_data = &class_exp2cliimp(mdc_exp)->imp_connect_data;

        rc = obd_register_observer(mdc_obd, obd);
        if (rc) {
                obd_disconnect(mdc_exp);
                CERROR("target %s register_observer error %d\n",
                       tgt->uuid.uuid, rc);
                RETURN(rc);
        }

        if (obd->obd_observer) {
                /* tell the mds_lmv about the new target */
                rc = obd_notify(obd->obd_observer, mdc_exp->exp_obd,
                                OBD_NOTIFY_ACTIVE, (void *)(tgt - lmv->tgts));
                if (rc) {
                        obd_disconnect(mdc_exp);
                        RETURN(rc);
                }
        }

        tgt->active = 1;
        tgt->ltd_exp = mdc_exp;
        lmv->desc.ld_active_tgt_count++;

        /* copy connect data, it may be used later */
        lmv->datas[tgt->idx] = *mdc_data;

        md_init_ea_size(tgt->ltd_exp, lmv->max_easize,
                        lmv->max_def_easize, lmv->max_cookiesize);

        CDEBUG(D_CONFIG, "connected to %s(%s) successfully (%d)\n",
                mdc_obd->obd_name, mdc_obd->obd_uuid.uuid,
                atomic_read(&obd->obd_refcount));

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_srch(obd->obd_proc_entry, "target_obds");
        if (lmv_proc_dir) {
                struct proc_dir_entry *mdc_symlink;
                char name[MAX_STRING_SIZE + 1];

                LASSERT(mdc_obd->obd_type != NULL);
                LASSERT(mdc_obd->obd_type->typ_name != NULL);
                name[MAX_STRING_SIZE] = '\0';
                snprintf(name, MAX_STRING_SIZE, "../../../%s/%s",
                         mdc_obd->obd_type->typ_name,
                         mdc_obd->obd_name);
                mdc_symlink = proc_symlink(mdc_obd->obd_name,
                                           lmv_proc_dir, name);
                if (mdc_symlink == NULL) {
                        CERROR("could not register LMV target "
                               "/proc/fs/lustre/%s/%s/target_obds/%s.",
                               obd->obd_type->typ_name, obd->obd_name,
                               mdc_obd->obd_name);
                        lprocfs_remove(lmv_proc_dir);
                        lmv_proc_dir = NULL;
                }
        }
#endif
        RETURN(0);
}

int lmv_add_target(struct obd_device *obd, struct obd_uuid *tgt_uuid)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_tgt_desc *tgt;
        int rc = 0;
        ENTRY;

        CDEBUG(D_CONFIG, "tgt_uuid: %s.\n", tgt_uuid->uuid);

        lmv_init_lock(lmv);

        if (lmv->desc.ld_active_tgt_count >= LMV_MAX_TGT_COUNT) {
                lmv_init_unlock(lmv);
                CERROR("can't add %s, LMV module compiled for %d MDCs. "
                       "That many MDCs already configured.\n",
                       tgt_uuid->uuid, LMV_MAX_TGT_COUNT);
                RETURN(-EINVAL);
        }
        if (lmv->desc.ld_tgt_count == 0) {
                struct obd_device *mdc_obd;

                mdc_obd = class_find_client_obd(tgt_uuid, LUSTRE_MDC_NAME,
                                                &obd->obd_uuid);
                if (!mdc_obd) {
                        lmv_init_unlock(lmv);
                        CERROR("Target %s not attached\n", tgt_uuid->uuid);
                        RETURN(-EINVAL);
                }

                rc = obd_llog_init(obd, mdc_obd, 0, NULL);
                if (rc) {
                        lmv_init_unlock(lmv);
                        CERROR("lmv failed to setup llogging subsystems\n");
                }
        }
        spin_lock(&lmv->lmv_lock);
        tgt = lmv->tgts + lmv->desc.ld_tgt_count++;
        tgt->uuid = *tgt_uuid;
        spin_unlock(&lmv->lmv_lock);

        if (lmv->connected) {
                rc = lmv_connect_mdc(obd, tgt);
                if (rc) {
                        spin_lock(&lmv->lmv_lock);
                        lmv->desc.ld_tgt_count--;
                        memset(tgt, 0, sizeof(*tgt));
                        spin_unlock(&lmv->lmv_lock);
                } else {
                        int easize = sizeof(struct lmv_stripe_md) +
                                     lmv->desc.ld_tgt_count *
                                     sizeof(struct lu_fid);
                        lmv_init_ea_size(obd->obd_self_export, easize, 0, 0);
                }
        }

        lmv_init_unlock(lmv);
        RETURN(rc);
}

/* performs a check if passed obd is connected. If no - connect it. */
int lmv_check_connect(struct obd_device *obd)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_tgt_desc *tgt;
        int i, rc, easize;
        ENTRY;

        if (lmv->connected)
                RETURN(0);

        lmv_init_lock(lmv);
        if (lmv->connected) {
                lmv_init_unlock(lmv);
                RETURN(0);
        }

        if (lmv->desc.ld_tgt_count == 0) {
                CERROR("%s: no targets configured.\n", obd->obd_name);
                RETURN(-EINVAL);
        }

        CDEBUG(D_CONFIG, "time to connect %s to %s\n",
               lmv->cluuid.uuid, obd->obd_name);

        LASSERT(lmv->tgts != NULL);

        for (i = 0, tgt = lmv->tgts; i < lmv->desc.ld_tgt_count; i++, tgt++) {
                rc = lmv_connect_mdc(obd, tgt);
                if (rc)
                        GOTO(out_disc, rc);
        }

        lmv_set_timeouts(obd);
        class_export_put(lmv->exp);
        lmv->connected = 1;
        easize = lmv_get_easize(lmv);
        lmv_init_ea_size(obd->obd_self_export, easize, 0, 0);
        lmv_init_unlock(lmv);
        RETURN(0);

 out_disc:
        while (i-- > 0) {
                int rc2;
                --tgt;
                tgt->active = 0;
                if (tgt->ltd_exp) {
                        --lmv->desc.ld_active_tgt_count;
                        rc2 = obd_disconnect(tgt->ltd_exp);
                        if (rc2) {
                                CERROR("error: LMV target %s disconnect on "
                                       "MDC idx %d: error %d\n",
                                       tgt->uuid.uuid, i, rc2);
                        }
                }
        }
        class_disconnect(lmv->exp);
        lmv_init_unlock(lmv);
        RETURN(rc);
}

static int lmv_disconnect(struct obd_export *exp)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;

#ifdef __KERNEL__
        struct proc_dir_entry *lmv_proc_dir;
#endif
        int rc, i;
        ENTRY;

        if (!lmv->tgts)
                goto out_local;

        /* Only disconnect the underlying layers on the final disconnect. */
        lmv->refcount--;
        if (lmv->refcount != 0)
                goto out_local;

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_srch(obd->obd_proc_entry, "target_obds");
#endif

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                struct obd_device *mdc_obd;

                if (lmv->tgts[i].ltd_exp == NULL)
                        continue;

                mdc_obd = class_exp2obd(lmv->tgts[i].ltd_exp);

                if (mdc_obd)
                        mdc_obd->obd_no_recov = obd->obd_no_recov;

#ifdef __KERNEL__
                if (lmv_proc_dir) {
                        struct proc_dir_entry *mdc_symlink;

                        mdc_symlink = lprocfs_srch(lmv_proc_dir, mdc_obd->obd_name);
                        if (mdc_symlink) {
                                lprocfs_remove(mdc_symlink);
                        } else {
                                CERROR("/proc/fs/lustre/%s/%s/target_obds/%s missing\n",
                                       obd->obd_type->typ_name, obd->obd_name,
                                       mdc_obd->obd_name);
                        }
                }
#endif
                CDEBUG(D_OTHER, "disconnected from %s(%s) successfully\n",
                        lmv->tgts[i].ltd_exp->exp_obd->obd_name,
                        lmv->tgts[i].ltd_exp->exp_obd->obd_uuid.uuid);

                obd_register_observer(lmv->tgts[i].ltd_exp->exp_obd, NULL);
                rc = obd_disconnect(lmv->tgts[i].ltd_exp);
                if (rc) {
                        if (lmv->tgts[i].active) {
                                CERROR("Target %s disconnect error %d\n",
                                       lmv->tgts[i].uuid.uuid, rc);
                        }
                        rc = 0;
                }

                lmv_activate_target(lmv, &lmv->tgts[i], 0);
                lmv->tgts[i].ltd_exp = NULL;
        }

#ifdef __KERNEL__
        if (lmv_proc_dir) {
                lprocfs_remove(lmv_proc_dir);
        } else {
                CERROR("/proc/fs/lustre/%s/%s/target_obds missing\n",
                       obd->obd_type->typ_name, obd->obd_name);
        }
#endif

out_local:
        /* this is the case when no real connection is established by
         * lmv_check_connect(). */
        if (!lmv->connected)
                class_export_put(exp);
        rc = class_disconnect(exp);
        if (lmv->refcount == 0)
                lmv->connected = 0;
        RETURN(rc);
}

static int lmv_iocontrol(unsigned int cmd, struct obd_export *exp,
                         int len, void *karg, void *uarg)
{
        struct obd_device *obddev = class_exp2obd(exp);
        struct lmv_obd *lmv = &obddev->u.lmv;
        int i, rc = 0, set = 0;
        ENTRY;

        if (lmv->desc.ld_tgt_count == 0)
                RETURN(-ENOTTY);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                int err;

                if (lmv->tgts[i].ltd_exp == NULL)
                        continue;

                err = obd_iocontrol(cmd, lmv->tgts[i].ltd_exp, len, karg, uarg);
                if (err) {
                        if (lmv->tgts[i].active) {
                                CERROR("error: iocontrol MDC %s on MDT"
                                       "idx %d: err = %d\n",
                                       lmv->tgts[i].uuid.uuid, i, err);
                                if (!rc)
                                        rc = err;
                        }
                } else
                        set = 1;
        }
        if (!set && !rc)
                rc = -EIO;

        RETURN(rc);
}

/* assume all is balanced for now */
static int lmv_fids_balanced(struct obd_device *obd)
{
        ENTRY;
        RETURN(1);
}

static int lmv_all_chars_policy(int count, struct qstr *name)
{
        unsigned int c = 0;
        unsigned int len = name->len;

        while (len > 0)
                c += name->name[-- len];
        c = c % count;
        return c;
}

static int lmv_placement_policy(struct obd_device *obd,
                                struct lu_placement_hint *hint,
                                mdsno_t *mds)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        int rc;
        ENTRY;

        LASSERT(mds != NULL);

        /* here are some policies to allocate new fid */
        if (lmv_fids_balanced(obd)) {
                /* allocate new fid basing on its name in the case fids are
                 * balanced, that is all sequences have more or less equal
                 * number of objects created. */
                if (hint->ph_cname && (hint->ph_opc == LUSTRE_OPC_MKDIR)) {
#if 0
                        *mds = lmv_all_chars_policy(lmv->desc.ld_tgt_count,
                                                    hint->ph_cname);
                        rc = 0;
#else
                        /* stress policy for tests - to use non-parent MDS */
                        LASSERT(fid_is_sane(hint->ph_pfid));
                        rc = lmv_fld_lookup(lmv, hint->ph_pfid, mds);
                        *mds = (int)(*mds + 1) % lmv->desc.ld_tgt_count;

#endif
                } else {
                        /* default policy is to use parent MDS */
                        LASSERT(fid_is_sane(hint->ph_pfid));
                        rc = lmv_fld_lookup(lmv, hint->ph_pfid, mds);
                }
        } else {
                /* sequences among all tgts are not well balanced, allocate new
                 * fid taking this into account to balance them. Not implemented
                 * yet! */
                *mds = 0;
                rc = -EINVAL;
        }

        if (rc) {
                CERROR("cannot choose MDS, err = %d\n", rc);
        } else {
                LASSERT(*mds < lmv->desc.ld_tgt_count);
        }

        RETURN(rc);
}

static int lmv_fid_init(struct obd_export *exp)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;
        int i, rc = 0;
        ENTRY;

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                if (lmv->tgts[i].ltd_exp == NULL)
                        continue;

                rc = obd_fid_init(lmv->tgts[i].ltd_exp);
                if (rc)
                        RETURN(rc);
        }
        RETURN(rc);
}

static int lmv_fid_fini(struct obd_export *exp)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;
        int i, rc = 0;
        ENTRY;

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                if (lmv->tgts[i].ltd_exp == NULL)
                        continue;

                rc = obd_fid_fini(lmv->tgts[i].ltd_exp);
                if (rc)
                        break;
        }
        RETURN(rc);
}

static int lmv_fid_alloc(struct obd_export *exp, struct lu_fid *fid,
                         struct lu_placement_hint *hint)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;
        mdsno_t mds;
        int rc;
        ENTRY;

        LASSERT(fid != NULL);
        LASSERT(hint != NULL);

        rc = lmv_placement_policy(obd, hint, &mds);
        if (rc) {
                CERROR("can't get target for allocating fid, "
                       "rc %d\n", rc);
                RETURN(rc);
        }

        /* asking underlaying tgt layer to allocate new fid */
        rc = obd_fid_alloc(lmv->tgts[mds].ltd_exp, fid, hint);

        /* client switches to new sequence, setup fld */
        if (rc > 0) {
                LASSERT(fid_is_sane(fid));
                
                rc = fld_client_create(&lmv->lmv_fld, fid_seq(fid),
                                       mds);
                if (rc) {
                        CERROR("can't create fld entry, rc %d\n", rc);
                        RETURN(rc);
                }
        }

        RETURN(rc);
}

static int lmv_fid_delete(struct obd_export *exp, const struct lu_fid *fid)
{
        ENTRY;

        LASSERT(exp && fid);
        if (lmv_obj_delete(exp, fid)) {
                CDEBUG(D_OTHER, "lmv object "DFID" is destroyed.\n",
                       PFID(fid));
        }
        RETURN(0);
}

static int lmv_setup(struct obd_device *obd, struct lustre_cfg *lcfg)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lprocfs_static_vars lvars;
        struct lmv_desc *desc;
        int rc, i = 0;
        ENTRY;

        if (LUSTRE_CFG_BUFLEN(lcfg, 1) < 1) {
                CERROR("LMV setup requires a descriptor\n");
                RETURN(-EINVAL);
        }

        desc = (struct lmv_desc *)lustre_cfg_buf(lcfg, 1);
        if (sizeof(*desc) > LUSTRE_CFG_BUFLEN(lcfg, 1)) {
                CERROR("descriptor size wrong: %d > %d\n",
                       (int)sizeof(*desc), LUSTRE_CFG_BUFLEN(lcfg, 1));
                RETURN(-EINVAL);
        }

        lmv->tgts_size = LMV_MAX_TGT_COUNT * sizeof(struct lmv_tgt_desc);

        OBD_ALLOC(lmv->tgts, lmv->tgts_size);
        if (lmv->tgts == NULL)
                RETURN(-ENOMEM);

        for (i = 0; i < LMV_MAX_TGT_COUNT; i++)
                lmv->tgts[i].idx = i;

        lmv->datas_size = LMV_MAX_TGT_COUNT * sizeof(struct obd_connect_data);

        OBD_ALLOC(lmv->datas, lmv->datas_size);
        if (lmv->datas == NULL)
                GOTO(out_free_tgts, rc = -ENOMEM);

        obd_str2uuid(&lmv->desc.ld_uuid, desc->ld_uuid.uuid);
        lmv->desc.ld_tgt_count = 0;
        lmv->desc.ld_active_tgt_count = 0;
        lmv->max_cookiesize = 0;
        lmv->max_def_easize = 0;
        lmv->max_easize = 0;

        spin_lock_init(&lmv->lmv_lock);
        sema_init(&lmv->init_sem, 1);

        rc = lmv_mgr_setup(obd);
        if (rc) {
                CERROR("Can't setup LMV object manager, "
                       "error %d.\n", rc);
                GOTO(out_free_datas, rc);
        }

        lprocfs_init_vars(lmv, &lvars);
        lprocfs_obd_setup(obd, lvars.obd_vars);
#ifdef LPROCFS
        {
                struct proc_dir_entry *entry;

                entry = create_proc_entry("target_obd_status", 0444,
                                          obd->obd_proc_entry);
                if (entry != NULL) {
                        entry->proc_fops = &lmv_proc_target_fops;
                        entry->data = obd;
                }
       }
#endif
        rc = fld_client_init(&lmv->lmv_fld,
                             "LMV_UUID", LUSTRE_CLI_FLD_HASH_RRB);
        if (rc) {
                CERROR("can't init FLD, err %d\n",
                       rc);
                GOTO(out_free_datas, rc);
        }

        RETURN(0);

out_free_datas:
        OBD_FREE(lmv->datas, lmv->datas_size);
        lmv->datas = NULL;
out_free_tgts:
        OBD_FREE(lmv->tgts, lmv->tgts_size);
        lmv->tgts = NULL;
        return rc;
}

static int lmv_cleanup(struct obd_device *obd)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        ENTRY;

        fld_client_fini(&lmv->lmv_fld);
        lprocfs_obd_cleanup(obd);
        lmv_mgr_cleanup(obd);
        OBD_FREE(lmv->datas, lmv->datas_size);
        OBD_FREE(lmv->tgts, lmv->tgts_size);

        RETURN(0);
}

static int lmv_process_config(struct obd_device *obd, obd_count len, void *buf)
{
        struct lustre_cfg *lcfg = buf;
        struct obd_uuid tgt_uuid;
        int rc;
        ENTRY;

        switch(lcfg->lcfg_command) {
        case LCFG_ADD_MDC:
                if (LUSTRE_CFG_BUFLEN(lcfg, 1) > sizeof(tgt_uuid.uuid))
                        GOTO(out, rc = -EINVAL);

                obd_str2uuid(&tgt_uuid, lustre_cfg_string(lcfg, 1));
		rc = lmv_add_target(obd, &tgt_uuid);
                GOTO(out, rc);
        default: {
                CERROR("Unknown command: %d\n", lcfg->lcfg_command);
                GOTO(out, rc = -EINVAL);
        }
        }
out:
        RETURN(rc);
}

static int lmv_statfs(struct obd_device *obd, struct obd_statfs *osfs,
                      __u64 max_age)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_statfs *temp;
        int rc = 0, i;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        OBD_ALLOC(temp, sizeof(*temp));
        if (temp == NULL)
                RETURN(-ENOMEM);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                if (lmv->tgts[i].ltd_exp == NULL)
                        continue;

                rc = obd_statfs(lmv->tgts[i].ltd_exp->exp_obd, temp, max_age);
                if (rc) {
                        CERROR("can't stat MDS #%d (%s), error %d\n", i,
                               lmv->tgts[i].ltd_exp->exp_obd->obd_name,
                               rc);
                        GOTO(out_free_temp, rc);
                }
                if (i == 0) {
                        *osfs = *temp;
                } else {
                        osfs->os_bavail += temp->os_bavail;
                        osfs->os_blocks += temp->os_blocks;
                        osfs->os_ffree += temp->os_ffree;
                        osfs->os_files += temp->os_files;
                }
        }

        EXIT;
out_free_temp:
        OBD_FREE(temp, sizeof(*temp));
        return rc;
}

static int lmv_getstatus(struct obd_export *exp,
                         struct lu_fid *fid)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        rc = md_getstatus(lmv->tgts[0].ltd_exp, fid);

        RETURN(rc);
}

static int lmv_getxattr(struct obd_export *exp, const struct lu_fid *fid,
                        obd_valid valid, const char *name, const char *input,
                        int input_size, int output_size, int flags,
                        struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt_exp = lmv_get_export(lmv, fid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_getxattr(tgt_exp, fid, valid, name, input, input_size,
                         output_size, flags, request);

        RETURN(rc);
}

static int lmv_setxattr(struct obd_export *exp, const struct lu_fid *fid,
                        obd_valid valid, const char *name, const char *input,
                        int input_size, int output_size, int flags,
                        struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt_exp = lmv_get_export(lmv, fid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_setxattr(tgt_exp, fid, valid, name,
                         input, input_size, output_size, flags, request);
        
        RETURN(rc);
}

static int lmv_getattr(struct obd_export *exp, const struct lu_fid *fid,
                       obd_valid valid, int ea_size,
                       struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        struct lmv_obj *obj;
        int rc, i;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt_exp = lmv_get_export(lmv, fid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_getattr(tgt_exp, fid, valid, ea_size, request);
        if (rc)
                RETURN(rc);

        obj = lmv_obj_grab(obd, fid);

        CDEBUG(D_OTHER, "GETATTR for "DFID" %s\n",
               PFID(fid), obj ? "(split)" : "");

        /* if object is split, then we loop over all the slaves and gather size
         * attribute. In ideal world we would have to gather also mds field from
         * all slaves, as object is spread over the cluster and this is
         * definitely interesting information and it is not good to loss it,
         * but... */
        if (obj) {
                struct mdt_body *body;

                if (*request == NULL) {
                        lmv_obj_put(obj);
                        RETURN(rc);
                }

                body = lustre_msg_buf((*request)->rq_repmsg, REQ_REC_OFF,
                                      sizeof(*body));
                LASSERT(body != NULL);

                lmv_obj_lock(obj);

                for (i = 0; i < obj->lo_objcount; i++) {
                        if (lmv->tgts[i].ltd_exp == NULL) {
                                CWARN("%s: NULL export for %d\n",
                                      obd->obd_name, i);
                                continue;
                        }

                        /* skip master obj. */
                        if (lu_fid_eq(&obj->lo_fid, &obj->lo_inodes[i].li_fid))
                                continue;

                        body->size += obj->lo_inodes[i].li_size;
                }

                lmv_obj_unlock(obj);
                lmv_obj_put(obj);
        }

        RETURN(rc);
}

static int lmv_change_cbdata(struct obd_export *exp,
                             const struct lu_fid *fid,
                             ldlm_iterator_t it,
                             void *data)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int i, rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        CDEBUG(D_OTHER, "CBDATA for "DFID"\n", PFID(fid));

        /* with CMD every object can have two locks in different namespaces:
         * lookup lock in space of mds storing direntry and update/open lock in
         * space of mds storing inode */
        for (i = 0; i < lmv->desc.ld_tgt_count; i++)
                md_change_cbdata(lmv->tgts[i].ltd_exp, fid, it, data);

        RETURN(0);
}

static int lmv_close(struct obd_export *exp,
                     struct md_op_data *op_data,
                     struct obd_client_handle *och,
                     struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        CDEBUG(D_OTHER, "CLOSE "DFID"\n", PFID(&op_data->fid1));
        rc = md_close(tgt_exp, op_data, och, request);
        RETURN(rc);
}

/* called in the case MDS returns -ERESTART on create on open, what means that
 * directory is split and its LMV presentation object has to be updated. */
int lmv_handle_split(struct obd_export *exp, const struct lu_fid *fid)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct ptlrpc_request *req = NULL;
        struct obd_export *tgt_exp;
        struct lmv_obj *obj;
        struct lustre_md md;
        int mealen, rc;
        __u64 valid;
        ENTRY;

        md.mea = NULL;
        mealen = lmv_get_easize(lmv);

        valid = OBD_MD_FLEASIZE | OBD_MD_FLDIREA | OBD_MD_MEA;

        tgt_exp = lmv_get_export(lmv, fid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        /* time to update mea of parent fid */
        rc = md_getattr(tgt_exp, fid, valid, mealen, &req);
        if (rc) {
                CERROR("md_getattr() failed, error %d\n", rc);
                GOTO(cleanup, rc);
        }

        rc = md_get_lustre_md(tgt_exp, req, 0, NULL, &md);
        if (rc) {
                CERROR("mdc_get_lustre_md() failed, error %d\n", rc);
                GOTO(cleanup, rc);
        }

        if (md.mea == NULL)
                GOTO(cleanup, rc = -ENODATA);

        obj = lmv_obj_create(exp, fid, md.mea);
        if (IS_ERR(obj))
                rc = PTR_ERR(obj);
        else
                lmv_obj_put(obj);

        obd_free_memmd(exp, (struct lov_stripe_md **)&md.mea);

        EXIT;
cleanup:
        if (req)
                ptlrpc_req_finished(req);
        return rc;
}

int lmv_create(struct obd_export *exp, struct md_op_data *op_data,
               const void *data, int datalen, int mode, __u32 uid,
               __u32 gid, __u32 cap_effective,  __u64 rdev,
               struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        struct mdt_body *body;
        struct lmv_obj *obj;
        int rc, loop = 0;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        if (!lmv->desc.ld_active_tgt_count)
                RETURN(-EIO);
repeat:
        LASSERT(++loop <= 2);
        obj = lmv_obj_grab(obd, &op_data->fid1);
        if (obj) {
                mdsno_t mds;
                
                mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                   op_data->name, op_data->namelen);
                op_data->fid1 = obj->lo_inodes[mds].li_fid;
                lmv_obj_put(obj);
        }

        CDEBUG(D_OTHER, "CREATE '%*s' on "DFID"\n", op_data->namelen,
               op_data->name, PFID(&op_data->fid1));

        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_create(tgt_exp, op_data, data, datalen, mode, uid, gid,
                       cap_effective, rdev, request);
        if (rc == 0) {
                if (*request == NULL)
                        RETURN(rc);

                body = lustre_msg_buf((*request)->rq_repmsg, REQ_REC_OFF,
                                      sizeof(*body));
                if (body == NULL)
                        RETURN(-ENOMEM);

                CDEBUG(D_OTHER, "created. "DFID"\n", PFID(&op_data->fid1));
        } else if (rc == -ERESTART) {
                /* directory got split. time to update local object and repeat
                 * the request with proper MDS. */
                rc = lmv_handle_split(exp, &op_data->fid1);
                if (rc == 0) {
                        ptlrpc_req_finished(*request);
                        goto repeat;
                }
        }
        RETURN(rc);
}

static int lmv_done_writing(struct obd_export *exp,
                            struct md_op_data *op_data)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_done_writing(tgt_exp, op_data);
        RETURN(rc);
}

static int
lmv_enqueue_slaves(struct obd_export *exp, int locktype,
                   struct lookup_intent *it, int lockmode,
                   struct md_op_data *op_data, struct lustre_handle *lockh,
                   void *lmm, int lmmsize, ldlm_completion_callback cb_compl,
                   ldlm_blocking_callback cb_blocking, void *cb_data)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_stripe_md *mea = op_data->mea1;
        struct md_op_data *op_data2;
        struct obd_export *tgt_exp;
        int i, rc = 0;
        ENTRY;

        OBD_ALLOC_PTR(op_data2);
        if (op_data2 == NULL)
                RETURN(-ENOMEM);

        LASSERT(mea != NULL);
        for (i = 0; i < mea->mea_count; i++) {
                memset(op_data2, 0, sizeof(*op_data2));
                op_data2->fid1 = mea->mea_ids[i];

                tgt_exp = lmv_get_export(lmv, &op_data2->fid1);
                if (IS_ERR(tgt_exp))
                        GOTO(cleanup, rc = PTR_ERR(tgt_exp));

                if (tgt_exp == NULL)
                        continue;

                rc = md_enqueue(tgt_exp, locktype, it, lockmode, op_data2,
                                lockh + i, lmm, lmmsize, cb_compl, cb_blocking,
                                cb_data, 0);

                CDEBUG(D_OTHER, "take lock on slave "DFID" -> %d/%d\n",
                       PFID(&mea->mea_ids[i]), rc, it->d.lustre.it_status);

                if (rc)
                        GOTO(cleanup, rc);
                
                if (it->d.lustre.it_data) {
                        struct ptlrpc_request *req;
                        req = (struct ptlrpc_request *)it->d.lustre.it_data;
                        ptlrpc_req_finished(req);
                }

                if (it->d.lustre.it_status)
                        GOTO(cleanup, rc = it->d.lustre.it_status);
        }

        EXIT;
cleanup:
        OBD_FREE_PTR(op_data2);

        if (rc != 0) {
                /* drop all taken locks */
                while (--i >= 0) {
                        if (lockh[i].cookie)
                                ldlm_lock_decref(lockh + i, lockmode);
                        lockh[i].cookie = 0;
                }
        }
        return rc;
}

static int
lmv_enqueue_remote(struct obd_export *exp, int lock_type,
                   struct lookup_intent *it, int lock_mode,
                   struct md_op_data *op_data, struct lustre_handle *lockh,
                   void *lmm, int lmmsize, ldlm_completion_callback cb_compl,
                   ldlm_blocking_callback cb_blocking, void *cb_data,
                   int extra_lock_flags)
{
        struct ptlrpc_request *req = it->d.lustre.it_data;
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct mdt_body *body = NULL;
        struct lustre_handle plock;
        struct obd_export *tgt_exp;
        struct md_op_data *rdata;
        int rc = 0, pmode;
        ENTRY;

        body = lustre_msg_buf(req->rq_repmsg, DLM_REPLY_REC_OFF, sizeof(*body));
        LASSERT(body != NULL);

        if (!(body->valid & OBD_MD_MDS))
                RETURN(0);

        CDEBUG(D_OTHER, "ENQUEUE '%s' on "DFID" -> "DFID"\n",
               LL_IT2STR(it), PFID(&op_data->fid1), PFID(&body->fid1));

        /* we got LOOKUP lock, but we really need attrs */
        pmode = it->d.lustre.it_lock_mode;
        LASSERT(pmode != 0);
        memcpy(&plock, lockh, sizeof(plock));
        it->d.lustre.it_lock_mode = 0;
        it->d.lustre.it_data = NULL;

        OBD_ALLOC_PTR(rdata);
        if (rdata == NULL)
                RETURN(-ENOMEM);
        rdata->fid1 = body->fid1;
        rdata->name = NULL;
        rdata->namelen = 0;

        it->d.lustre.it_disposition &= ~DISP_ENQ_COMPLETE;
        ptlrpc_req_finished(req);

        tgt_exp = lmv_get_export(lmv, &rdata->fid1);
        if (IS_ERR(tgt_exp))
                GOTO(out_free_rdata, rc = PTR_ERR(tgt_exp));

        rc = md_enqueue(tgt_exp, lock_type, it, lock_mode, rdata,
                        lockh, lmm, lmmsize, cb_compl, cb_blocking,
                        cb_data, extra_lock_flags);
        ldlm_lock_decref(&plock, pmode);

        EXIT;
out_free_rdata:
        OBD_FREE_PTR(rdata);
        return rc;
}

static int
lmv_enqueue(struct obd_export *exp, int lock_type,
            struct lookup_intent *it, int lock_mode,
            struct md_op_data *op_data, struct lustre_handle *lockh,
            void *lmm, int lmmsize, ldlm_completion_callback cb_compl,
            ldlm_blocking_callback cb_blocking, void *cb_data,
            int extra_lock_flags)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        struct lmv_obj *obj;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        if (op_data->mea1 && it->it_op == IT_UNLINK) {
                rc = lmv_enqueue_slaves(exp, lock_type, it, lock_mode,
                                        op_data, lockh, lmm, lmmsize,
                                        cb_compl, cb_blocking, cb_data);
                RETURN(rc);
        }

        if (op_data->namelen) {
                obj = lmv_obj_grab(obd, &op_data->fid1);
                if (obj) {
                        mdsno_t mds;
                        
                        /* directory is split. look for right mds for this
                         * name */
                        mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                           (char *)op_data->name, op_data->namelen);
                        op_data->fid1 = obj->lo_inodes[mds].li_fid;
                        lmv_obj_put(obj);
                }
        }
        CDEBUG(D_OTHER, "ENQUEUE '%s' on "DFID"\n", LL_IT2STR(it),
               PFID(&op_data->fid1));

        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_enqueue(tgt_exp, lock_type, it, lock_mode, op_data, lockh,
                        lmm, lmmsize, cb_compl, cb_blocking, cb_data,
                        extra_lock_flags);
        
        if (rc == 0 && it->it_op == IT_OPEN)
                rc = lmv_enqueue_remote(exp, lock_type, it, lock_mode,
                                        op_data, lockh, lmm, lmmsize,
                                        cb_compl, cb_blocking, cb_data,
                                        extra_lock_flags);
        RETURN(rc);
}

static int
lmv_getattr_name(struct obd_export *exp, const struct lu_fid *fid,
                 const char *filename, int namelen, obd_valid valid,
                 int ea_size, struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        struct lu_fid rid = *fid;
        struct mdt_body *body;
        struct lmv_obj *obj;
        int rc, loop = 0;
        mdsno_t mds;
        ENTRY;

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

repeat:
        LASSERT(++loop <= 2);
        obj = lmv_obj_grab(obd, fid);
        if (obj) {
                /* directory is split. look for right mds for this name */
                mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                   filename, namelen - 1);
                rid = obj->lo_inodes[mds].li_fid;
                lmv_obj_put(obj);
        }

        CDEBUG(D_OTHER, "getattr_name for %*s on "DFID" -> "DFID"\n",
               namelen, filename, PFID(fid), PFID(&rid));

        tgt_exp = lmv_get_export(lmv, &rid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_getattr_name(tgt_exp, &rid, filename, namelen, valid,
                             ea_size, request);
        if (rc == 0) {
                body = lustre_msg_buf((*request)->rq_repmsg,
                                      REQ_REC_OFF, sizeof(*body));
                LASSERT(body != NULL);

                if (body->valid & OBD_MD_MDS) {
                        struct ptlrpc_request *req = NULL;

                        rid = body->fid1;
                        CDEBUG(D_OTHER, "request attrs for "DFID"\n",
                               PFID(&rid));

                        tgt_exp = lmv_get_export(lmv, &rid);
                        if (IS_ERR(tgt_exp)) {
                                ptlrpc_req_finished(*request);
                                RETURN(PTR_ERR(tgt_exp));
                        }
                        
                        rc = md_getattr_name(tgt_exp, &rid, NULL, 1, valid,
                                             ea_size, &req);
                        ptlrpc_req_finished(*request);
                        *request = req;
                }
        } else if (rc == -ERESTART) {
                /* directory got split. time to update local object and repeat
                 * the request with proper MDS */
                rc = lmv_handle_split(exp, &rid);
                if (rc == 0) {
                        ptlrpc_req_finished(*request);
                        goto repeat;
                }
        }
        RETURN(rc);
}

/*
 * llite passes fid of an target inode in op_data->fid1 and id of directory in
 * op_data->fid2
 */
static int lmv_link(struct obd_export *exp, struct md_op_data *op_data,
                    struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_obj *obj;
        mdsno_t mds;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        if (op_data->namelen != 0) {
                /* usual link request */
                obj = lmv_obj_grab(obd, &op_data->fid2);
                if (obj) {
                        rc = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                          op_data->name, op_data->namelen);
                        op_data->fid2 = obj->lo_inodes[rc].li_fid;
                        lmv_obj_put(obj);
                }

                rc = lmv_fld_lookup(lmv, &op_data->fid2, &mds);
                if (rc)
                        RETURN(rc);

                CDEBUG(D_OTHER,"link "DFID":%*s to "DFID"\n",
                       PFID(&op_data->fid2), op_data->namelen,
                       op_data->name, PFID(&op_data->fid1));
        } else {
                rc = lmv_fld_lookup(lmv, &op_data->fid1, &mds);
                if (rc)
                        RETURN(rc);

                /* request from MDS to acquire i_links for inode by fid1 */
                CDEBUG(D_OTHER, "inc i_nlinks for "DFID"\n",
                       PFID(&op_data->fid1));
        }

        CDEBUG(D_OTHER, "forward to MDS #"LPU64" ("DFID")\n",
               mds, PFID(&op_data->fid1));
        
        rc = md_link(lmv->tgts[mds].ltd_exp, op_data, request);

        RETURN(rc);
}

static int lmv_rename(struct obd_export *exp, struct md_op_data *op_data,
                      const char *old, int oldlen, const char *new, int newlen,
                      struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_obj *obj;
        mdsno_t mds, mds2;
        int rc;
        ENTRY;

        CDEBUG(D_OTHER, "rename %*s in "DFID" to %*s in "DFID"\n",
               oldlen, old, PFID(&op_data->fid1), newlen, new,
               PFID(&op_data->fid2));

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        if (oldlen == 0) {
                /*
                 * MDS with old dir entry is asking another MDS to create name
                 * there.
                 */
                CDEBUG(D_OTHER,
                       "create %*s(%d/%d) in "DFID" pointing "
                       "to "DFID"\n", newlen, new, oldlen, newlen,
                       PFID(&op_data->fid2), PFID(&op_data->fid1));

                rc = lmv_fld_lookup(lmv, &op_data->fid2, &mds);
                if (rc)
                        RETURN(rc);

                /*
                 * target directory can be split, sowe should forward request to
                 * the right MDS.
                 */
                obj = lmv_obj_grab(obd, &op_data->fid2);
                if (obj) {
                        mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                           (char *)new, newlen);
                        op_data->fid2 = obj->lo_inodes[mds].li_fid;
                        CDEBUG(D_OTHER, "forward to MDS #"LPU64" ("DFID")\n", mds,
                               PFID(&op_data->fid2));
                        lmv_obj_put(obj);
                }
                goto request;
        }

        obj = lmv_obj_grab(obd, &op_data->fid1);
        if (obj) {
                /*
                 * directory is already split, so we have to forward request to
                 * the right MDS.
                 */
                mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                   (char *)old, oldlen);
                op_data->fid1 = obj->lo_inodes[mds].li_fid;
                CDEBUG(D_OTHER, "forward to MDS #"LPU64" ("DFID")\n", mds,
                       PFID(&op_data->fid1));
                lmv_obj_put(obj);
        }

        obj = lmv_obj_grab(obd, &op_data->fid2);
        if (obj) {
                /*
                 * directory is already split, so we have to forward request to
                 * the right MDS.
                 */
                mds = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                   (char *)new, newlen);

                op_data->fid2 = obj->lo_inodes[mds].li_fid;
                CDEBUG(D_OTHER, "forward to MDS #"LPU64" ("DFID")\n", mds,
                       PFID(&op_data->fid2));
                lmv_obj_put(obj);
        }

        rc = lmv_fld_lookup(lmv, &op_data->fid1, &mds);
        if (rc)
                RETURN(rc);

request:
        rc = lmv_fld_lookup(lmv, &op_data->fid2, &mds2);
        if (rc)
                RETURN(rc);

        if (mds != mds2) {
                CDEBUG(D_OTHER,"cross-node rename "DFID"/%*s to "DFID"/%*s\n",
                       PFID(&op_data->fid1), oldlen, old, PFID(&op_data->fid2),
                       newlen, new);
        }

        rc = md_rename(lmv->tgts[mds].ltd_exp, op_data, old, oldlen,
                       new, newlen, request);
        RETURN(rc);
}

static int lmv_setattr(struct obd_export *exp, struct md_op_data *op_data,
                       struct iattr *iattr, void *ea, int ealen, void *ea2,
                       int ea2len, struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct ptlrpc_request *req;
        struct obd_export *tgt_exp;
        struct mdt_body *body;
        struct lmv_obj *obj;
        int rc = 0, i;
        ENTRY;

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        obj = lmv_obj_grab(obd, &op_data->fid1);

        CDEBUG(D_OTHER, "SETATTR for "DFID", valid 0x%x%s\n",
               PFID(&op_data->fid1), iattr->ia_valid, obj ? ", split" : "");

        if (obj) {
                for (i = 0; i < obj->lo_objcount; i++) {
                        op_data->fid1 = obj->lo_inodes[i].li_fid;

                        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
                        if (IS_ERR(tgt_exp)) {
                                rc = PTR_ERR(tgt_exp);
                                break;
                        }

                        rc = md_setattr(tgt_exp, op_data, iattr, ea, ealen,
                                        ea2, ea2len, &req);

                        if (lu_fid_eq(&obj->lo_fid, &obj->lo_inodes[i].li_fid)) {
                                /*
                                 * this is master object and this request should
                                 * be returned back to llite.
                                 */
                                *request = req;
                        } else {
                                ptlrpc_req_finished(req);
                        }

                        if (rc)
                                break;
                }
                lmv_obj_put(obj);
        } else {
                tgt_exp = lmv_get_export(lmv, &op_data->fid1);
                if (IS_ERR(tgt_exp))
                        RETURN(PTR_ERR(tgt_exp));

                rc = md_setattr(tgt_exp, op_data, iattr, ea, ealen, ea2,
                                ea2len, request);
                if (rc == 0) {
                        body = lustre_msg_buf((*request)->rq_repmsg, REQ_REC_OFF,
                                              sizeof(*body));
                        LASSERT(body != NULL);
                }
        }
        RETURN(rc);
}

static int lmv_sync(struct obd_export *exp, const struct lu_fid *fid,
                    struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc;
        ENTRY;

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        tgt_exp = lmv_get_export(lmv, fid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_sync(tgt_exp, fid, request);
        RETURN(rc);
}

/* main purpose of LMV blocking ast is to remove split directory LMV
 * presentation object (struct lmv_obj) attached to the lock being revoked. */
int lmv_blocking_ast(struct ldlm_lock *lock,
                     struct ldlm_lock_desc *desc,
                     void *data, int flag)
{
        struct lustre_handle lockh;
        struct lmv_obj *obj;
        int rc;
        ENTRY;

        switch (flag) {
        case LDLM_CB_BLOCKING:
                ldlm_lock2handle(lock, &lockh);
                rc = ldlm_cli_cancel(&lockh);
                if (rc < 0) {
                        CDEBUG(D_INODE, "ldlm_cli_cancel: %d\n", rc);
                        RETURN(rc);
                }
                break;
        case LDLM_CB_CANCELING:
                /* time to drop cached attrs for dirobj */
                obj = lock->l_ast_data;
                if (obj) {
                        CDEBUG(D_OTHER, "cancel %s on "LPU64"/"LPU64
                               ", master "DFID"\n",
                               lock->l_resource->lr_name.name[3] == 1 ?
                               "LOOKUP" : "UPDATE",
                               lock->l_resource->lr_name.name[0],
                               lock->l_resource->lr_name.name[1],
                               PFID(&obj->lo_fid));
                        lmv_obj_put(obj);
                }
                break;
        default:
                LBUG();
        }
        RETURN(0);
}

static int lmv_readpage(struct obd_export *exp,
                        const struct lu_fid *fid,
                        __u64 offset, struct page *page,
                        struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        struct lu_fid rid = *fid;
        struct lmv_obj *obj;
        int i, rc;
        ENTRY;

        rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        CDEBUG(D_OTHER, "READPAGE at %llu from "DFID"\n",
               offset, PFID(&rid));

        obj = lmv_obj_grab(obd, fid);
        if (obj) {
                lmv_obj_lock(obj);

                /* find dirobj containing page with requested offset. */
                for (i = 0; i < obj->lo_objcount; i++) {
                        if (offset < obj->lo_inodes[i].li_size)
                                break;
                        offset -= obj->lo_inodes[i].li_size;
                }
                rid = obj->lo_inodes[i].li_fid;

                lmv_obj_unlock(obj);
                lmv_obj_put(obj);

                CDEBUG(D_OTHER, "forward to "DFID" with offset %lu\n",
                       PFID(&rid), (unsigned long)offset);
        }
        
        tgt_exp = lmv_get_export(lmv, &rid);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));

        rc = md_readpage(tgt_exp, &rid, offset, page, request);

        /*
         * Here we could remove "." and ".." from all pages which at not from
         * master. But MDS has only "." and ".." for master dir.
         */
        RETURN(rc);
}

static int lmv_unlink_slaves(struct obd_export *exp,
                             struct md_op_data *op_data,
                             struct ptlrpc_request **req)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_stripe_md *mea = op_data->mea1;
        struct md_op_data *op_data2;
        struct obd_export *tgt_exp;
        int i, rc = 0;
        ENTRY;

        OBD_ALLOC_PTR(op_data2);
        if (op_data2 == NULL)
                RETURN(-ENOMEM);

        LASSERT(mea != NULL);
        for (i = 0; i < mea->mea_count; i++) {
                memset(op_data2, 0, sizeof(*op_data2));
                op_data2->fid1 = mea->mea_ids[i];
                op_data2->create_mode = MDS_MODE_DONT_LOCK | S_IFDIR;

                tgt_exp = lmv_get_export(lmv, &op_data2->fid1);
                if (IS_ERR(tgt_exp))
                        GOTO(out_free_op_data2, rc = PTR_ERR(tgt_exp));

                if (tgt_exp == NULL)
                        continue;

                rc = md_unlink(tgt_exp, op_data2, req);

                CDEBUG(D_OTHER, "unlink slave "DFID" -> %d\n",
                       PFID(&mea->mea_ids[i]), rc);

                if (*req) {
                        ptlrpc_req_finished(*req);
                        *req = NULL;
                }
                if (rc)
                        GOTO(out_free_op_data2, rc);
        }

        EXIT;
out_free_op_data2:
        OBD_FREE_PTR(op_data2);
        return rc;
}

static int lmv_unlink(struct obd_export *exp, struct md_op_data *op_data,
                      struct ptlrpc_request **request)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        struct obd_export *tgt_exp;
        int rc, i;
        ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

        if (op_data->namelen == 0 && op_data->mea1 != NULL) {
                /* mds asks to remove slave objects */
                rc = lmv_unlink_slaves(exp, op_data, request);
                RETURN(rc);
        }

        if (op_data->namelen != 0) {
                struct lmv_obj *obj;

                obj = lmv_obj_grab(obd, &op_data->fid1);
                if (obj) {
                        i = raw_name2idx(obj->lo_hashtype, obj->lo_objcount,
                                         op_data->name, op_data->namelen);
                        op_data->fid1 = obj->lo_inodes[i].li_fid;
                        lmv_obj_put(obj);
                        CDEBUG(D_OTHER, "unlink '%*s' in "DFID" -> %u\n",
                               op_data->namelen, op_data->name,
                               PFID(&op_data->fid1), i);
                }
        } else {
                CDEBUG(D_OTHER, "drop i_nlink on "DFID"\n",
                       PFID(&op_data->fid1));
        }
        tgt_exp = lmv_get_export(lmv, &op_data->fid1);
        if (IS_ERR(tgt_exp))
                RETURN(PTR_ERR(tgt_exp));
        
        rc = md_unlink(tgt_exp, op_data, request);
        RETURN(rc);
}

static int lmv_llog_init(struct obd_device *obd, struct obd_device *tgt,
                         int count, struct llog_catid *logid)
{
        struct llog_ctxt *ctxt;
        int rc;
        ENTRY;

        rc = llog_setup(obd, LLOG_CONFIG_REPL_CTXT, tgt, 0, NULL,
                        &llog_client_ops);
        if (rc == 0) {
                ctxt = llog_get_context(obd, LLOG_CONFIG_REPL_CTXT);
                ctxt->loc_imp = tgt->u.cli.cl_import;
        }

        RETURN(rc);
}

static int lmv_llog_finish(struct obd_device *obd, int count)
{
        int rc;
        ENTRY;

        rc = llog_cleanup(llog_get_context(obd, LLOG_CONFIG_REPL_CTXT));
        RETURN(rc);
}

static int lmv_precleanup(struct obd_device *obd, enum obd_cleanup_stage stage)
{
        int rc = 0;

        switch (stage) {
        case OBD_CLEANUP_EARLY:
                /* XXX: here should be calling obd_precleanup() down to
                 * stack. */
                break;
        case OBD_CLEANUP_SELF_EXP:
                rc = obd_llog_finish(obd, 0);
                if (rc != 0)
                        CERROR("failed to cleanup llogging subsystems\n");
                break;
        default:
                break;
        }
        RETURN(rc);
}

static int lmv_get_info(struct obd_export *exp, __u32 keylen,
                        void *key, __u32 *vallen, void *val)
{
        struct obd_device *obd;
        struct lmv_obd *lmv;
        int rc = 0;
        ENTRY;

        obd = class_exp2obd(exp);
        if (obd == NULL) {
                CDEBUG(D_IOCTL, "invalid client cookie "LPX64"\n",
                       exp->exp_handle.h_cookie);
                RETURN(-EINVAL);
        }

        lmv = &obd->u.lmv;
        if (keylen >= strlen("remote_flag") && !strcmp(key, "remote_flag")) {
                struct lmv_tgt_desc *tgts;
                int i;

                rc = lmv_check_connect(obd);
                if (rc)
                        RETURN(rc);

                LASSERT(*vallen == sizeof(__u32));
                for (i = 0, tgts = lmv->tgts; i < lmv->desc.ld_tgt_count;
                     i++, tgts++) {

                        /* all tgts should be connected when this get called. */
                        if (!tgts || !tgts->ltd_exp) {
                                CERROR("target not setup?\n");
                                continue;
                        }

                        if (!obd_get_info(tgts->ltd_exp, keylen, key,
                                          vallen, val))
                                RETURN(0);
                }
                RETURN(-EINVAL);
        } else if (keylen >= strlen("max_easize") && !strcmp(key, "max_easize")) {
                
                rc = lmv_check_connect(obd);
                if (rc)
                        RETURN(rc);

                /* forwarding this request to first MDS, it should know LOV
                 * desc. */
                rc = obd_get_info(lmv->tgts[0].ltd_exp, keylen, key,
                                  vallen, val);
                RETURN(rc);
        }

        CDEBUG(D_IOCTL, "invalid key\n");
        RETURN(-EINVAL);
}

int lmv_set_info_async(struct obd_export *exp, obd_count keylen,
                       void *key, obd_count vallen, void *val,
                       struct ptlrpc_request_set *set)
{
        struct lmv_tgt_desc    *tgt;
        struct obd_device      *obd;
        struct lmv_obd         *lmv;
        int rc = 0;
        ENTRY;

        obd = class_exp2obd(exp);
        if (obd == NULL) {
                CDEBUG(D_IOCTL, "invalid client cookie "LPX64"\n",
                       exp->exp_handle.h_cookie);
                RETURN(-EINVAL);
        }
        lmv = &obd->u.lmv;

        /* maybe this could be default */
        if ((keylen == strlen("sec") && strcmp(key, "sec") == 0) ||
            (keylen == strlen("sec_flags") && strcmp(key, "sec_flags") == 0) ||
            (keylen == strlen("nllu") && strcmp(key, "nllu") == 0)) {
                struct obd_export *exp;
                int err, i;

                spin_lock(&lmv->lmv_lock);
                for (i = 0, tgt = lmv->tgts; i < lmv->desc.ld_tgt_count;
                     i++, tgt++) {
                        exp = tgt->ltd_exp;
                        /* during setup time the connections to mdc might
                         * haven't been established.
                         */
                        if (exp == NULL) {
                                struct obd_device *tgt_obd;

                                tgt_obd = class_find_client_obd(&tgt->uuid,
                                                                LUSTRE_MDC_NAME,
                                                                &obd->obd_uuid);
                                if (!tgt_obd) {
                                        CERROR("can't set info %s, "
                                               "device %s not attached?\n",
                                                (char *) key, tgt->uuid.uuid);
                                        rc = -EINVAL;
                                        continue;
                                }
                                exp = tgt_obd->obd_self_export;
                        }

                        err = obd_set_info_async(exp, keylen, key, vallen, val, set);
                        if (!rc)
                                rc = err;
                }
                spin_unlock(&lmv->lmv_lock);

                RETURN(rc);
        }
        if (((keylen == strlen("flush_cred") &&
             strcmp(key, "flush_cred") == 0)) ||
             ((keylen == strlen("crypto_type") &&
             strcmp(key, "crypto_type") == 0))) {
                int i;

                for (i = 0, tgt = lmv->tgts; i < lmv->desc.ld_tgt_count;
                     i++, tgt++) {
                        if (!tgt->ltd_exp)
                                continue;
                        rc = obd_set_info_async(tgt->ltd_exp,
                                                keylen, key, vallen,
                                                val, set);
                        if (rc)
                                RETURN(rc);
                }

                RETURN(0);
        }

        RETURN(-EINVAL);
}

int lmv_packmd(struct obd_export *exp, struct lov_mds_md **lmmp,
               struct lov_stripe_md *lsm)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_obd *lmv = &obd->u.lmv;
        struct lmv_stripe_md *meap, *lsmp;
        int mea_size, i;
        ENTRY;

        mea_size = lmv_get_easize(lmv);
        if (!lmmp)
                RETURN(mea_size);

        if (*lmmp && !lsm) {
                OBD_FREE(*lmmp, mea_size);
                *lmmp = NULL;
                RETURN(0);
        }

        if (*lmmp == NULL) {
                OBD_ALLOC(*lmmp, mea_size);
                if (*lmmp == NULL)
                        RETURN(-ENOMEM);
        }

        if (!lsm)
                RETURN(mea_size);

        lsmp = (struct lmv_stripe_md *)lsm;
        meap = (struct lmv_stripe_md *)*lmmp;

        if (lsmp->mea_magic != MEA_MAGIC_LAST_CHAR &&
            lsmp->mea_magic != MEA_MAGIC_ALL_CHARS)
                RETURN(-EINVAL);

        meap->mea_magic = cpu_to_le32(lsmp->mea_magic);
        meap->mea_count = cpu_to_le32(lsmp->mea_count);
        meap->mea_master = cpu_to_le32(lsmp->mea_master);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                meap->mea_ids[i] = meap->mea_ids[i];
                fid_cpu_to_le(&meap->mea_ids[i]);
        }

        RETURN(mea_size);
}

int lmv_unpackmd(struct obd_export *exp, struct lov_stripe_md **lsmp,
                 struct lov_mds_md *lmm, int lmm_size)
{
        struct obd_device *obd = class_exp2obd(exp);
        struct lmv_stripe_md **tmea = (struct lmv_stripe_md **)lsmp;
        struct lmv_stripe_md *mea = (struct lmv_stripe_md *)lmm;
        struct lmv_obd *lmv = &obd->u.lmv;
        int mea_size, i;
        __u32 magic;
        ENTRY;

        mea_size = lmv_get_easize(lmv);
        if (lsmp == NULL)
                return mea_size;

        if (*lsmp != NULL && lmm == NULL) {
                OBD_FREE(*tmea, mea_size);
                RETURN(0);
        }

        LASSERT(mea_size == lmm_size);

        OBD_ALLOC(*tmea, mea_size);
        if (*tmea == NULL)
                RETURN(-ENOMEM);

        if (!lmm)
                RETURN(mea_size);

        if (mea->mea_magic == MEA_MAGIC_LAST_CHAR ||
            mea->mea_magic == MEA_MAGIC_ALL_CHARS)
        {
                magic = le32_to_cpu(mea->mea_magic);
        } else {
                /* old mea isnot handled here */
                LBUG();
        }

        (*tmea)->mea_magic = magic;
        (*tmea)->mea_count = le32_to_cpu(mea->mea_count);
        (*tmea)->mea_master = le32_to_cpu(mea->mea_master);

        for (i = 0; i < (*tmea)->mea_count; i++) {
                (*tmea)->mea_ids[i] = mea->mea_ids[i];
                fid_le_to_cpu(&(*tmea)->mea_ids[i]);
        }
        RETURN(mea_size);
}

static int lmv_cancel_unused(struct obd_export *exp,
                             const struct lu_fid *fid,
                             int flags, void *opaque)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int rc = 0, err, i;
        ENTRY;

        LASSERT(fid != NULL);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                if (!lmv->tgts[i].ltd_exp || !lmv->tgts[i].active)
                        continue;

                err = md_cancel_unused(lmv->tgts[i].ltd_exp,
                                       fid, flags, opaque);
                if (!rc)
                        rc = err;
        }
        RETURN(rc);
}

int lmv_set_lock_data(struct obd_export *exp, __u64 *lockh, void *data)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;

        ENTRY;
        RETURN(md_set_lock_data(lmv->tgts[0].ltd_exp, lockh, data));
}

int lmv_lock_match(struct obd_export *exp, int flags,
                   const struct lu_fid *fid, ldlm_type_t type,
                   ldlm_policy_data_t *policy, ldlm_mode_t mode,
                   struct lustre_handle *lockh)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int i, rc = 0;
        ENTRY;

        CDEBUG(D_OTHER, "lock match for "DFID"\n", PFID(fid));

        /* with CMD every object can have two locks in different namespaces:
         * lookup lock in space of mds storing direntry and update/open lock in
         * space of mds storing inode. Thus we check all targets, not only that
         * one fid was created in. */
        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
                rc = md_lock_match(lmv->tgts[i].ltd_exp, flags, fid,
                                   type, policy, mode, lockh);
                if (rc)
                        RETURN(1);
        }

        RETURN(rc);
}

int lmv_get_lustre_md(struct obd_export *exp, struct ptlrpc_request *req,
                      int offset, struct obd_export *dt_exp, struct lustre_md *md)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;
        int rc;

        ENTRY;
        rc = md_get_lustre_md(lmv->tgts[0].ltd_exp, req, offset, dt_exp, md);
        RETURN(rc);
}

int lmv_free_lustre_md(struct obd_export *exp, struct lustre_md *md)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;

        ENTRY;
        RETURN(md_free_lustre_md(lmv->tgts[0].ltd_exp, md));
}

int lmv_set_open_replay_data(struct obd_export *exp,
                             struct obd_client_handle *och,
                             struct ptlrpc_request *open_req)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;

        ENTRY;
        RETURN(md_set_open_replay_data(lmv->tgts[0].ltd_exp,
                                       och, open_req));
}

int lmv_clear_open_replay_data(struct obd_export *exp,
                               struct obd_client_handle *och)
{
        struct obd_device *obd = exp->exp_obd;
        struct lmv_obd *lmv = &obd->u.lmv;

        ENTRY;
        RETURN(md_clear_open_replay_data(lmv->tgts[0].ltd_exp, och));
}

struct obd_ops lmv_obd_ops = {
        .o_owner                = THIS_MODULE,
        .o_setup                = lmv_setup,
        .o_cleanup              = lmv_cleanup,
        .o_precleanup           = lmv_precleanup,
        .o_process_config       = lmv_process_config,
        .o_connect              = lmv_connect,
        .o_disconnect           = lmv_disconnect,
        .o_statfs               = lmv_statfs,
        .o_llog_init            = lmv_llog_init,
        .o_llog_finish          = lmv_llog_finish,
        .o_get_info             = lmv_get_info,
        .o_set_info_async       = lmv_set_info_async,
        .o_packmd               = lmv_packmd,
        .o_unpackmd             = lmv_unpackmd,
        .o_notify               = lmv_notify,
        .o_fid_init             = lmv_fid_init,
        .o_fid_fini             = lmv_fid_fini,
        .o_fid_alloc            = lmv_fid_alloc,
        .o_fid_delete           = lmv_fid_delete,
        .o_iocontrol            = lmv_iocontrol
};

struct md_ops lmv_md_ops = {
        .m_getstatus            = lmv_getstatus,
        .m_change_cbdata        = lmv_change_cbdata,
        .m_close                = lmv_close,
        .m_create               = lmv_create,
        .m_done_writing         = lmv_done_writing,
        .m_enqueue              = lmv_enqueue,
        .m_getattr              = lmv_getattr,
        .m_getxattr             = lmv_getxattr,
        .m_getattr_name         = lmv_getattr_name,
        .m_intent_lock          = lmv_intent_lock,
        .m_link                 = lmv_link,
        .m_rename               = lmv_rename,
        .m_setattr              = lmv_setattr,
        .m_setxattr             = lmv_setxattr,
        .m_sync                 = lmv_sync,
        .m_readpage             = lmv_readpage,
        .m_unlink               = lmv_unlink,
        .m_init_ea_size         = lmv_init_ea_size,
        .m_cancel_unused        = lmv_cancel_unused,
        .m_set_lock_data        = lmv_set_lock_data,
        .m_lock_match           = lmv_lock_match,
        .m_get_lustre_md        = lmv_get_lustre_md,
        .m_free_lustre_md       = lmv_free_lustre_md,
        .m_set_open_replay_data = lmv_set_open_replay_data,
        .m_clear_open_replay_data = lmv_clear_open_replay_data
};

int __init lmv_init(void)
{
        struct lprocfs_static_vars lvars;
        int rc;

        obj_cache = kmem_cache_create("lmv_objects",
                                      sizeof(struct lmv_obj),
                                      0, 0, NULL, NULL);
        if (!obj_cache) {
                CERROR("error allocating lmv objects cache\n");
                return -ENOMEM;
        }

        lprocfs_init_vars(lmv, &lvars);
        rc = class_register_type(&lmv_obd_ops, &lmv_md_ops,
                                 lvars.module_vars, LUSTRE_LMV_NAME, NULL);
        if (rc)
                kmem_cache_destroy(obj_cache);

        return rc;
}

#ifdef __KERNEL__
static void lmv_exit(void)
{
        class_unregister_type(LUSTRE_LMV_NAME);

        LASSERTF(kmem_cache_destroy(obj_cache) == 0,
                 "can't free lmv objects cache, %d object(s)"
                 "still in use\n", atomic_read(&obj_cache_count));
}

MODULE_AUTHOR("Cluster File Systems, Inc. <info@clusterfs.com>");
MODULE_DESCRIPTION("Lustre Logical Metadata Volume OBD driver");
MODULE_LICENSE("GPL");

module_init(lmv_init);
module_exit(lmv_exit);
#endif
