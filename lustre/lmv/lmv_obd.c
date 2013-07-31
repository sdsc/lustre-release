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
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LMV
#ifdef __KERNEL__
#include <linux/slab.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/slab.h>
#include <linux/pagemap.h>
#include <linux/mm.h>
#include <asm/div64.h>
#include <linux/seq_file.h>
#include <linux/namei.h>
#else
#include <liblustre.h>
#endif

#include <lustre/lustre_idl.h>
#include <obd_support.h>
#include <lustre_lib.h>
#include <lustre_net.h>
#include <obd_class.h>
#include <lprocfs_status.h>
#include <cl_object.h>
#include <lclient.h>
#include <lustre_lite.h>
#include <lustre_fid.h>
#include "lmv_internal.h"

int raw_name2idx(int hashtype, int count, const char *name, int namelen)
{
	unsigned int	c = 0;
	int		idx;

	LASSERT(namelen > 0);

	if (filename_is_volatile(name, namelen, &idx)) {
		if ((idx >= 0) && (idx < count))
			return idx;
		goto hashchoice;
	}

	if (count <= 1)
		return 0;

hashchoice:
	switch (hashtype) {
	case MEA_MAGIC_LAST_CHAR:
		c = mea_last_char_hash(count, (char *)name, namelen);
		break;
	case MEA_MAGIC_ALL_CHARS:
		c = mea_all_chars_hash(count, (char *)name, namelen);
		break;
	case MEA_MAGIC_HASH_SEGMENT:
		CERROR("Unsupported hash type MEA_MAGIC_HASH_SEGMENT\n");
		break;
	default:
		CERROR("Unknown hash type 0x%x\n", hashtype);
	}

	LASSERT(c < count);
	return c;
}

static void lmv_activate_target(struct lmv_obd *lmv,
                                struct lmv_tgt_desc *tgt,
                                int activate)
{
        if (tgt->ltd_active == activate)
                return;

        tgt->ltd_active = activate;
        lmv->desc.ld_active_tgt_count += (activate ? 1 : -1);
}

/**
 * Error codes:
 *
 *  -EINVAL  : UUID can't be found in the LMV's target list
 *  -ENOTCONN: The UUID is found, but the target connection is bad (!)
 *  -EBADF   : The UUID is found, but the OBD of the wrong type (!)
 */
static int lmv_set_mdc_active(struct lmv_obd *lmv,
			      const struct obd_uuid *uuid,
			      int activate)
{
	struct lmv_tgt_desc	*tgt = NULL;
	struct obd_device	*obd;
	__u32			 i;
	int			 rc = 0;
	ENTRY;

	CDEBUG(D_INFO, "Searching in lmv %p for uuid %s (activate=%d)\n",
			lmv, uuid->uuid, activate);

	spin_lock(&lmv->lmv_lock);
	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		tgt = lmv->tgts[i];
		if (tgt == NULL || tgt->ltd_exp == NULL)
			continue;

		CDEBUG(D_INFO, "Target idx %d is %s conn "LPX64"\n", i,
		       tgt->ltd_uuid.uuid, tgt->ltd_exp->exp_handle.h_cookie);

		if (obd_uuid_equals(uuid, &tgt->ltd_uuid))
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

        if (tgt->ltd_active == activate) {
                CDEBUG(D_INFO, "OBD %p already %sactive!\n", obd,
                       activate ? "" : "in");
                GOTO(out_lmv_lock, rc);
        }

        CDEBUG(D_INFO, "Marking OBD %p %sactive\n", obd,
               activate ? "" : "in");
        lmv_activate_target(lmv, tgt, activate);
        EXIT;

 out_lmv_lock:
	spin_unlock(&lmv->lmv_lock);
	return rc;
}

struct obd_uuid *lmv_get_uuid(struct obd_export *exp)
{
	struct lmv_obd		*lmv = &exp->exp_obd->u.lmv;
	struct lmv_tgt_desc	*tgt = lmv->tgts[0];

	return (tgt == NULL) ? NULL : obd_get_uuid(tgt->ltd_exp);
}

static int lmv_notify(struct obd_device *obd, struct obd_device *watched,
                      enum obd_notify_event ev, void *data)
{
        struct obd_connect_data *conn_data;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct obd_uuid         *uuid;
        int                      rc = 0;
        ENTRY;

        if (strcmp(watched->obd_type->typ_name, LUSTRE_MDC_NAME)) {
                CERROR("unexpected notification of %s %s!\n",
                       watched->obd_type->typ_name,
                       watched->obd_name);
                RETURN(-EINVAL);
        }

        uuid = &watched->u.cli.cl_target_uuid;
        if (ev == OBD_NOTIFY_ACTIVE || ev == OBD_NOTIFY_INACTIVE) {
                /*
                 * Set MDC as active before notifying the observer, so the
                 * observer can use the MDC normally.
                 */
                rc = lmv_set_mdc_active(lmv, uuid,
                                        ev == OBD_NOTIFY_ACTIVE);
                if (rc) {
                        CERROR("%sactivation of %s failed: %d\n",
                               ev == OBD_NOTIFY_ACTIVE ? "" : "de",
                               uuid->uuid, rc);
                        RETURN(rc);
                }
	} else if (ev == OBD_NOTIFY_OCD) {
		conn_data = &watched->u.cli.cl_import->imp_connect_data;
		/*
		 * XXX: Make sure that ocd_connect_flags from all targets are
		 * the same. Otherwise one of MDTs runs wrong version or
		 * something like this.  --umka
		 */
		obd->obd_self_export->exp_connect_data = *conn_data;
	}
#if 0
        else if (ev == OBD_NOTIFY_DISCON) {
                /*
                 * For disconnect event, flush fld cache for failout MDS case.
                 */
                fld_client_flush(&lmv->lmv_fld);
        }
#endif
        /*
         * Pass the notification up the chain.
         */
        if (obd->obd_observer)
                rc = obd_notify(obd->obd_observer, watched, ev, data);

        RETURN(rc);
}

/**
 * This is fake connect function. Its purpose is to initialize lmv and say
 * caller that everything is okay. Real connection will be performed later.
 */
static int lmv_connect(const struct lu_env *env,
                       struct obd_export **exp, struct obd_device *obd,
                       struct obd_uuid *cluuid, struct obd_connect_data *data,
                       void *localdata)
{
#ifdef __KERNEL__
        struct proc_dir_entry *lmv_proc_dir;
#endif
        struct lmv_obd        *lmv = &obd->u.lmv;
        struct lustre_handle  conn = { 0 };
        int                    rc = 0;
        ENTRY;

        /*
         * We don't want to actually do the underlying connections more than
         * once, so keep track.
         */
        lmv->refcount++;
        if (lmv->refcount > 1) {
                *exp = NULL;
                RETURN(0);
        }

        rc = class_connect(&conn, obd, cluuid);
        if (rc) {
                CERROR("class_connection() returned %d\n", rc);
                RETURN(rc);
        }

        *exp = class_conn2export(&conn);
        class_export_get(*exp);

        lmv->exp = *exp;
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

        /*
         * All real clients should perform actual connection right away, because
         * it is possible, that LMV will not have opportunity to connect targets
         * and MDC stuff will be called directly, for instance while reading
         * ../mdc/../kbytesfree procfs file, etc.
         */
	if (data != NULL && (data->ocd_connect_flags & OBD_CONNECT_REAL))
                rc = lmv_check_connect(obd);

#ifdef __KERNEL__
        if (rc) {
                if (lmv_proc_dir)
                        lprocfs_remove(&lmv_proc_dir);
        }
#endif

        RETURN(rc);
}

static void lmv_set_timeouts(struct obd_device *obd)
{
	struct lmv_obd		*lmv;
	__u32			 i;

        lmv = &obd->u.lmv;
        if (lmv->server_timeout == 0)
                return;

        if (lmv->connected == 0)
                return;

	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active)
			continue;

		obd_set_info_async(NULL, tgt->ltd_exp, sizeof(KEY_INTERMDS),
				   KEY_INTERMDS, 0, NULL, NULL);
	}
}

static int lmv_init_ea_size(struct obd_export *exp, int easize,
                            int def_easize, int cookiesize)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	__u32			 i;
	int			 rc = 0;
	int			 change = 0;
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
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active) {
			CWARN("%s: NULL export for %d\n", obd->obd_name, i);
			continue;
		}

		rc = md_init_ea_size(tgt->ltd_exp, easize, def_easize,
				     cookiesize);
		if (rc) {
			CERROR("%s: obd_init_ea_size() failed on MDT target %d:"
			       " rc = %d.\n", obd->obd_name, i, rc);
			break;
		}
	}
	RETURN(rc);
}

#define MAX_STRING_SIZE 128

int lmv_connect_mdc(struct obd_device *obd, struct lmv_tgt_desc *tgt)
{
#ifdef __KERNEL__
        struct proc_dir_entry   *lmv_proc_dir;
#endif
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct obd_uuid         *cluuid = &lmv->cluuid;
        struct obd_uuid          lmv_mdc_uuid = { "LMV_MDC_UUID" };
        struct obd_device       *mdc_obd;
        struct obd_export       *mdc_exp;
        struct lu_fld_target     target;
        int                      rc;
        ENTRY;

        mdc_obd = class_find_client_obd(&tgt->ltd_uuid, LUSTRE_MDC_NAME,
                                        &obd->obd_uuid);
        if (!mdc_obd) {
                CERROR("target %s not attached\n", tgt->ltd_uuid.uuid);
                RETURN(-EINVAL);
        }

        CDEBUG(D_CONFIG, "connect to %s(%s) - %s, %s FOR %s\n",
                mdc_obd->obd_name, mdc_obd->obd_uuid.uuid,
                tgt->ltd_uuid.uuid, obd->obd_uuid.uuid,
                cluuid->uuid);

        if (!mdc_obd->obd_set_up) {
                CERROR("target %s is not set up\n", tgt->ltd_uuid.uuid);
                RETURN(-EINVAL);
        }

        rc = obd_connect(NULL, &mdc_exp, mdc_obd, &lmv_mdc_uuid,
                         &lmv->conn_data, NULL);
        if (rc) {
                CERROR("target %s connect error %d\n", tgt->ltd_uuid.uuid, rc);
                RETURN(rc);
        }

	/*
	 * Init fid sequence client for this mdc and add new fld target.
	 */
	rc = obd_fid_init(mdc_obd, mdc_exp, LUSTRE_SEQ_METADATA);
	if (rc)
		RETURN(rc);

        target.ft_srv = NULL;
        target.ft_exp = mdc_exp;
        target.ft_idx = tgt->ltd_idx;

        fld_client_add_target(&lmv->lmv_fld, &target);

        rc = obd_register_observer(mdc_obd, obd);
        if (rc) {
                obd_disconnect(mdc_exp);
                CERROR("target %s register_observer error %d\n",
                       tgt->ltd_uuid.uuid, rc);
                RETURN(rc);
        }

        if (obd->obd_observer) {
                /*
                 * Tell the observer about the new target.
                 */
		rc = obd_notify(obd->obd_observer, mdc_exp->exp_obd,
				OBD_NOTIFY_ACTIVE,
				(void *)(tgt - lmv->tgts[0]));
		if (rc) {
			obd_disconnect(mdc_exp);
			RETURN(rc);
		}
        }

        tgt->ltd_active = 1;
        tgt->ltd_exp = mdc_exp;
        lmv->desc.ld_active_tgt_count++;

        md_init_ea_size(tgt->ltd_exp, lmv->max_easize,
                        lmv->max_def_easize, lmv->max_cookiesize);

        CDEBUG(D_CONFIG, "Connected to %s(%s) successfully (%d)\n",
                mdc_obd->obd_name, mdc_obd->obd_uuid.uuid,
                cfs_atomic_read(&obd->obd_refcount));

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_srch(obd->obd_proc_entry, "target_obds");
        if (lmv_proc_dir) {
                struct proc_dir_entry *mdc_symlink;

                LASSERT(mdc_obd->obd_type != NULL);
                LASSERT(mdc_obd->obd_type->typ_name != NULL);
                mdc_symlink = lprocfs_add_symlink(mdc_obd->obd_name,
                                                  lmv_proc_dir,
                                                  "../../../%s/%s",
                                                  mdc_obd->obd_type->typ_name,
                                                  mdc_obd->obd_name);
                if (mdc_symlink == NULL) {
                        CERROR("Could not register LMV target "
                               "/proc/fs/lustre/%s/%s/target_obds/%s.",
                               obd->obd_type->typ_name, obd->obd_name,
                               mdc_obd->obd_name);
                        lprocfs_remove(&lmv_proc_dir);
                        lmv_proc_dir = NULL;
                }
        }
#endif
        RETURN(0);
}

static void lmv_del_target(struct lmv_obd *lmv, int index)
{
	if (lmv->tgts[index] == NULL)
		return;

	OBD_FREE_PTR(lmv->tgts[index]);
	lmv->tgts[index] = NULL;
	return;
}

static int lmv_add_target(struct obd_device *obd, struct obd_uuid *uuidp,
			   __u32 index, int gen)
{
        struct lmv_obd      *lmv = &obd->u.lmv;
        struct lmv_tgt_desc *tgt;
        int                  rc = 0;
        ENTRY;

	CDEBUG(D_CONFIG, "Target uuid: %s. index %d\n", uuidp->uuid, index);

        lmv_init_lock(lmv);

	if (lmv->desc.ld_tgt_count == 0) {
		struct obd_device *mdc_obd;

		mdc_obd = class_find_client_obd(uuidp, LUSTRE_MDC_NAME,
						&obd->obd_uuid);
		if (!mdc_obd) {
			lmv_init_unlock(lmv);
			CERROR("%s: Target %s not attached: rc = %d\n",
			       obd->obd_name, uuidp->uuid, -EINVAL);
			RETURN(-EINVAL);
		}
	}

	if ((index < lmv->tgts_size) && (lmv->tgts[index] != NULL)) {
		tgt = lmv->tgts[index];
		CERROR("%s: UUID %s already assigned at LOV target index %d:"
		       " rc = %d\n", obd->obd_name,
		       obd_uuid2str(&tgt->ltd_uuid), index, -EEXIST);
		lmv_init_unlock(lmv);
		RETURN(-EEXIST);
	}

	if (index >= lmv->tgts_size) {
		/* We need to reallocate the lmv target array. */
		struct lmv_tgt_desc **newtgts, **old = NULL;
		__u32 newsize = 1;
		__u32 oldsize = 0;

		while (newsize < index + 1)
			newsize = newsize << 1;
		OBD_ALLOC(newtgts, sizeof(*newtgts) * newsize);
		if (newtgts == NULL) {
			lmv_init_unlock(lmv);
			RETURN(-ENOMEM);
		}

		if (lmv->tgts_size) {
			memcpy(newtgts, lmv->tgts,
			       sizeof(*newtgts) * lmv->tgts_size);
			old = lmv->tgts;
			oldsize = lmv->tgts_size;
		}

		lmv->tgts = newtgts;
		lmv->tgts_size = newsize;
		smp_rmb();
		if (old)
			OBD_FREE(old, sizeof(*old) * oldsize);

		CDEBUG(D_CONFIG, "tgts: %p size: %d\n", lmv->tgts,
		       lmv->tgts_size);
	}

	OBD_ALLOC_PTR(tgt);
	if (!tgt) {
		lmv_init_unlock(lmv);
		RETURN(-ENOMEM);
	}

	mutex_init(&tgt->ltd_fid_mutex);
	tgt->ltd_idx = index;
	tgt->ltd_uuid = *uuidp;
	tgt->ltd_active = 0;
	lmv->tgts[index] = tgt;
	if (index >= lmv->desc.ld_tgt_count)
		lmv->desc.ld_tgt_count = index + 1;

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

int lmv_check_connect(struct obd_device *obd)
{
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt;
	__u32			 i;
	int			 rc;
	int			 easize;
	ENTRY;

        if (lmv->connected)
                RETURN(0);

        lmv_init_lock(lmv);
        if (lmv->connected) {
                lmv_init_unlock(lmv);
                RETURN(0);
        }

        if (lmv->desc.ld_tgt_count == 0) {
                lmv_init_unlock(lmv);
                CERROR("%s: no targets configured.\n", obd->obd_name);
                RETURN(-EINVAL);
        }

	LASSERT(lmv->tgts != NULL);

	if (lmv->tgts[0] == NULL) {
		lmv_init_unlock(lmv);
		CERROR("%s: no target configured for index 0.\n",
		       obd->obd_name);
		RETURN(-EINVAL);
	}

	CDEBUG(D_CONFIG, "Time to connect %s to %s\n",
	       lmv->cluuid.uuid, obd->obd_name);

	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		tgt = lmv->tgts[i];
		if (tgt == NULL)
			continue;
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
		tgt = lmv->tgts[i];
		if (tgt == NULL)
			continue;
                tgt->ltd_active = 0;
                if (tgt->ltd_exp) {
                        --lmv->desc.ld_active_tgt_count;
                        rc2 = obd_disconnect(tgt->ltd_exp);
                        if (rc2) {
                                CERROR("LMV target %s disconnect on "
                                       "MDC idx %d: error %d\n",
                                       tgt->ltd_uuid.uuid, i, rc2);
                        }
                }
        }
        class_disconnect(lmv->exp);
        lmv_init_unlock(lmv);
        RETURN(rc);
}

static int lmv_disconnect_mdc(struct obd_device *obd, struct lmv_tgt_desc *tgt)
{
#ifdef __KERNEL__
        struct proc_dir_entry  *lmv_proc_dir;
#endif
        struct lmv_obd         *lmv = &obd->u.lmv;
        struct obd_device      *mdc_obd;
        int                     rc;
        ENTRY;

        LASSERT(tgt != NULL);
        LASSERT(obd != NULL);

        mdc_obd = class_exp2obd(tgt->ltd_exp);

        if (mdc_obd) {
                mdc_obd->obd_force = obd->obd_force;
                mdc_obd->obd_fail = obd->obd_fail;
                mdc_obd->obd_no_recov = obd->obd_no_recov;
        }

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_srch(obd->obd_proc_entry, "target_obds");
        if (lmv_proc_dir) {
                struct proc_dir_entry *mdc_symlink;

                mdc_symlink = lprocfs_srch(lmv_proc_dir, mdc_obd->obd_name);
                if (mdc_symlink) {
                        lprocfs_remove(&mdc_symlink);
                } else {
                        CERROR("/proc/fs/lustre/%s/%s/target_obds/%s missing\n",
                               obd->obd_type->typ_name, obd->obd_name,
                               mdc_obd->obd_name);
                }
        }
#endif
	rc = obd_fid_fini(tgt->ltd_exp->exp_obd);
	if (rc)
		CERROR("Can't finanize fids factory\n");

        CDEBUG(D_INFO, "Disconnected from %s(%s) successfully\n",
               tgt->ltd_exp->exp_obd->obd_name,
               tgt->ltd_exp->exp_obd->obd_uuid.uuid);

        obd_register_observer(tgt->ltd_exp->exp_obd, NULL);
        rc = obd_disconnect(tgt->ltd_exp);
        if (rc) {
                if (tgt->ltd_active) {
                        CERROR("Target %s disconnect error %d\n",
                               tgt->ltd_uuid.uuid, rc);
                }
        }

        lmv_activate_target(lmv, tgt, 0);
        tgt->ltd_exp = NULL;
        RETURN(0);
}

static int lmv_disconnect(struct obd_export *exp)
{
	struct obd_device	*obd = class_exp2obd(exp);
#ifdef __KERNEL__
	struct proc_dir_entry	*lmv_proc_dir;
#endif
	struct lmv_obd		*lmv = &obd->u.lmv;
	int			 rc;
	__u32			 i;
	ENTRY;

        if (!lmv->tgts)
                goto out_local;

        /*
         * Only disconnect the underlying layers on the final disconnect.
         */
        lmv->refcount--;
        if (lmv->refcount != 0)
                goto out_local;

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		if (lmv->tgts[i] == NULL || lmv->tgts[i]->ltd_exp == NULL)
                        continue;

		lmv_disconnect_mdc(obd, lmv->tgts[i]);
        }

#ifdef __KERNEL__
        lmv_proc_dir = lprocfs_srch(obd->obd_proc_entry, "target_obds");
        if (lmv_proc_dir) {
                lprocfs_remove(&lmv_proc_dir);
        } else {
                CERROR("/proc/fs/lustre/%s/%s/target_obds missing\n",
                       obd->obd_type->typ_name, obd->obd_name);
        }
#endif

out_local:
        /*
         * This is the case when no real connection is established by
         * lmv_check_connect().
         */
        if (!lmv->connected)
                class_export_put(exp);
        rc = class_disconnect(exp);
        if (lmv->refcount == 0)
                lmv->connected = 0;
        RETURN(rc);
}

static int lmv_fid2path(struct obd_export *exp, int len, void *karg, void *uarg)
{
	struct obd_device	*obddev = class_exp2obd(exp);
	struct lmv_obd		*lmv = &obddev->u.lmv;
	struct getinfo_fid2path *gf;
	struct lmv_tgt_desc     *tgt;
	struct getinfo_fid2path *remote_gf = NULL;
	int			remote_gf_size = 0;
	int			rc;

	gf = (struct getinfo_fid2path *)karg;
	tgt = lmv_find_target(lmv, &gf->gf_fid);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

repeat_fid2path:
	rc = obd_iocontrol(OBD_IOC_FID2PATH, tgt->ltd_exp, len, gf, uarg);
	if (rc != 0 && rc != -EREMOTE)
		GOTO(out_fid2path, rc);

	/* If remote_gf != NULL, it means just building the
	 * path on the remote MDT, copy this path segement to gf */
	if (remote_gf != NULL) {
		struct getinfo_fid2path *ori_gf;
		char *ptr;

		ori_gf = (struct getinfo_fid2path *)karg;
		if (strlen(ori_gf->gf_path) +
		    strlen(gf->gf_path) > ori_gf->gf_pathlen)
			GOTO(out_fid2path, rc = -EOVERFLOW);

		ptr = ori_gf->gf_path;

		memmove(ptr + strlen(gf->gf_path) + 1, ptr,
			strlen(ori_gf->gf_path));

		strncpy(ptr, gf->gf_path, strlen(gf->gf_path));
		ptr += strlen(gf->gf_path);
		*ptr = '/';
	}

	CDEBUG(D_INFO, "%s: get path %s "DFID" rec: "LPU64" ln: %u\n",
	       tgt->ltd_exp->exp_obd->obd_name,
	       gf->gf_path, PFID(&gf->gf_fid), gf->gf_recno,
	       gf->gf_linkno);

	if (rc == 0)
		GOTO(out_fid2path, rc);

	/* sigh, has to go to another MDT to do path building further */
	if (remote_gf == NULL) {
		remote_gf_size = sizeof(*remote_gf) + PATH_MAX;
		OBD_ALLOC(remote_gf, remote_gf_size);
		if (remote_gf == NULL)
			GOTO(out_fid2path, rc = -ENOMEM);
		remote_gf->gf_pathlen = PATH_MAX;
	}

	if (!fid_is_sane(&gf->gf_fid)) {
		CERROR("%s: invalid FID "DFID": rc = %d\n",
		       tgt->ltd_exp->exp_obd->obd_name,
		       PFID(&gf->gf_fid), -EINVAL);
		GOTO(out_fid2path, rc = -EINVAL);
	}

	tgt = lmv_find_target(lmv, &gf->gf_fid);
	if (IS_ERR(tgt))
		GOTO(out_fid2path, rc = -EINVAL);

	remote_gf->gf_fid = gf->gf_fid;
	remote_gf->gf_recno = -1;
	remote_gf->gf_linkno = -1;
	memset(remote_gf->gf_path, 0, remote_gf->gf_pathlen);
	gf = remote_gf;
	goto repeat_fid2path;

out_fid2path:
	if (remote_gf != NULL)
		OBD_FREE(remote_gf, remote_gf_size);
	RETURN(rc);
}

static int lmv_hsm_req_count(struct lmv_obd *lmv,
			     const struct hsm_user_request *hur,
			     const struct lmv_tgt_desc *tgt_mds)
{
	__u32			 i;
	int			 nr = 0;
	struct lmv_tgt_desc	*curr_tgt;

	/* count how many requests must be sent to the given target */
	for (i = 0; i < hur->hur_request.hr_itemcount; i++) {
		curr_tgt = lmv_find_target(lmv, &hur->hur_user_item[i].hui_fid);
		if (obd_uuid_equals(&curr_tgt->ltd_uuid, &tgt_mds->ltd_uuid))
			nr++;
	}
	return nr;
}

static void lmv_hsm_req_build(struct lmv_obd *lmv,
			      struct hsm_user_request *hur_in,
			      const struct lmv_tgt_desc *tgt_mds,
			      struct hsm_user_request *hur_out)
{
	__u32			 i, nr_out;
	struct lmv_tgt_desc	*curr_tgt;

	/* build the hsm_user_request for the given target */
	hur_out->hur_request = hur_in->hur_request;
	nr_out = 0;
	for (i = 0; i < hur_in->hur_request.hr_itemcount; i++) {
		curr_tgt = lmv_find_target(lmv,
					   &hur_in->hur_user_item[i].hui_fid);
		if (obd_uuid_equals(&curr_tgt->ltd_uuid, &tgt_mds->ltd_uuid)) {
			hur_out->hur_user_item[nr_out] =
						hur_in->hur_user_item[i];
			nr_out++;
		}
	}
	hur_out->hur_request.hr_itemcount = nr_out;
	memcpy(hur_data(hur_out), hur_data(hur_in),
	       hur_in->hur_request.hr_data_len);
}

static int lmv_hsm_ct_unregister(struct lmv_obd *lmv, unsigned int cmd, int len,
				 struct lustre_kernelcomm *lk, void *uarg)
{
	__u32			 i;
	int			 rc;
	struct kkuc_ct_data	*kcd = NULL;
	ENTRY;

	/* unregister request (call from llapi_hsm_copytool_fini) */
	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL)
			continue;
		/* best effort: try to clean as much as possible
		 * (continue on error) */
		obd_iocontrol(cmd, tgt->ltd_exp, len, lk, uarg);
	}

	/* Whatever the result, remove copytool from kuc groups.
	 * Unreached coordinators will get EPIPE on next requests
	 * and will unregister automatically.
	 */
	rc = libcfs_kkuc_group_rem(lk->lk_uid, lk->lk_group, (void **)&kcd);
	if (kcd != NULL)
		OBD_FREE_PTR(kcd);

	RETURN(rc);
}

static int lmv_hsm_ct_register(struct lmv_obd *lmv, unsigned int cmd, int len,
			       struct lustre_kernelcomm *lk, void *uarg)
{
	struct file		*filp;
	__u32			 i, j;
	int			 err, rc;
	bool			 any_set = false;
	struct kkuc_ct_data	*kcd;
	ENTRY;

	/* All or nothing: try to register to all MDS.
	 * In case of failure, unregister from previous MDS,
	 * except if it because of inactive target. */
	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL)
			continue;
		err = obd_iocontrol(cmd, tgt->ltd_exp, len, lk, uarg);
		if (err) {
			if (tgt->ltd_active) {
				/* permanent error */
				CERROR("%s: iocontrol MDC %s on MDT"
				       " idx %d cmd %x: err = %d\n",
				       class_exp2obd(lmv->exp)->obd_name,
				       tgt->ltd_uuid.uuid, i, cmd, err);
				rc = err;
				lk->lk_flags |= LK_FLG_STOP;
				/* unregister from previous MDS */
				for (j = 0; j < i; j++) {
					tgt = lmv->tgts[j];
					if (tgt == NULL || tgt->ltd_exp == NULL)
						continue;
					obd_iocontrol(cmd, tgt->ltd_exp, len,
						      lk, uarg);
				}
				RETURN(rc);
			}
			/* else: transient error.
			 * kuc will register to the missing MDT
			 * when it is back */
		} else {
			any_set = true;
		}
	}

	if (!any_set)
		/* no registration done: return error */
		RETURN(-ENOTCONN);

	/* at least one registration done, with no failure */
	filp = fget(lk->lk_wfd);
	if (filp == NULL)
		RETURN(-EBADF);

	OBD_ALLOC_PTR(kcd);
	if (kcd == NULL) {
		fput(filp);
		RETURN(-ENOMEM);
	}
	kcd->kcd_magic = KKUC_CT_DATA_MAGIC;
	kcd->kcd_uuid = lmv->cluuid;
	kcd->kcd_archive = lk->lk_data;

	rc = libcfs_kkuc_group_add(filp, lk->lk_uid, lk->lk_group, kcd);
	if (rc != 0) {
		if (filp != NULL)
			fput(filp);
		OBD_FREE_PTR(kcd);
	}

	RETURN(rc);
}




static int lmv_iocontrol(unsigned int cmd, struct obd_export *exp,
                         int len, void *karg, void *uarg)
{
	struct obd_device	*obddev = class_exp2obd(exp);
	struct lmv_obd		*lmv = &obddev->u.lmv;
	struct lmv_tgt_desc	*tgt = NULL;
	__u32			 i = 0;
	int			 rc = 0;
	int			 set = 0;
	__u32			 count = lmv->desc.ld_tgt_count;
	ENTRY;

        if (count == 0)
                RETURN(-ENOTTY);

        switch (cmd) {
        case IOC_OBD_STATFS: {
                struct obd_ioctl_data *data = karg;
                struct obd_device *mdc_obd;
                struct obd_statfs stat_buf = {0};
                __u32 index;

                memcpy(&index, data->ioc_inlbuf2, sizeof(__u32));
                if ((index >= count))
                        RETURN(-ENODEV);

		tgt = lmv->tgts[index];
		if (tgt == NULL || !tgt->ltd_active)
			RETURN(-ENODATA);

		mdc_obd = class_exp2obd(tgt->ltd_exp);
		if (!mdc_obd)
			RETURN(-EINVAL);

		/* copy UUID */
		if (copy_to_user(data->ioc_pbuf2, obd2cli_tgt(mdc_obd),
				 min((int) data->ioc_plen2,
				     (int) sizeof(struct obd_uuid))))
			RETURN(-EFAULT);

		rc = obd_statfs(NULL, tgt->ltd_exp, &stat_buf,
				cfs_time_shift_64(-OBD_STATFS_CACHE_SECONDS),
				0);
		if (rc)
			RETURN(rc);
		if (copy_to_user(data->ioc_pbuf1, &stat_buf,
				 min((int) data->ioc_plen1,
				     (int) sizeof(stat_buf))))
			RETURN(-EFAULT);
		break;
        }
        case OBD_IOC_QUOTACTL: {
                struct if_quotactl *qctl = karg;
                struct obd_quotactl *oqctl;

		if (qctl->qc_valid == QC_MDTIDX) {
			if (count <= qctl->qc_idx)
				RETURN(-EINVAL);

			tgt = lmv->tgts[qctl->qc_idx];
			if (tgt == NULL || tgt->ltd_exp == NULL)
				RETURN(-EINVAL);
		} else if (qctl->qc_valid == QC_UUID) {
			for (i = 0; i < count; i++) {
				tgt = lmv->tgts[i];
				if (tgt == NULL)
					continue;
				if (!obd_uuid_equals(&tgt->ltd_uuid,
						     &qctl->obd_uuid))
					continue;

                                if (tgt->ltd_exp == NULL)
                                        RETURN(-EINVAL);

                                break;
                        }
                } else {
                        RETURN(-EINVAL);
                }

                if (i >= count)
                        RETURN(-EAGAIN);

                LASSERT(tgt != NULL && tgt->ltd_exp != NULL);
                OBD_ALLOC_PTR(oqctl);
                if (!oqctl)
                        RETURN(-ENOMEM);

                QCTL_COPY(oqctl, qctl);
                rc = obd_quotactl(tgt->ltd_exp, oqctl);
                if (rc == 0) {
                        QCTL_COPY(qctl, oqctl);
                        qctl->qc_valid = QC_MDTIDX;
                        qctl->obd_uuid = tgt->ltd_uuid;
                }
                OBD_FREE_PTR(oqctl);
                break;
        }
        case OBD_IOC_CHANGELOG_SEND:
        case OBD_IOC_CHANGELOG_CLEAR: {
                struct ioc_changelog *icc = karg;

                if (icc->icc_mdtindex >= count)
                        RETURN(-ENODEV);

		tgt = lmv->tgts[icc->icc_mdtindex];
		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active)
			RETURN(-ENODEV);
		rc = obd_iocontrol(cmd, tgt->ltd_exp, sizeof(*icc), icc, NULL);
		break;
	}
	case LL_IOC_GET_CONNECT_FLAGS: {
		tgt = lmv->tgts[0];
		if (tgt == NULL || tgt->ltd_exp == NULL)
			RETURN(-ENODATA);
		rc = obd_iocontrol(cmd, tgt->ltd_exp, len, karg, uarg);
		break;
	}
	case OBD_IOC_FID2PATH: {
		rc = lmv_fid2path(exp, len, karg, uarg);
		break;
	}
	case LL_IOC_HSM_STATE_GET:
	case LL_IOC_HSM_STATE_SET:
	case LL_IOC_HSM_ACTION: {
		struct md_op_data	*op_data = karg;

		tgt = lmv_find_target(lmv, &op_data->op_fid1);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));

		if (tgt->ltd_exp == NULL)
			RETURN(-EINVAL);

		rc = obd_iocontrol(cmd, tgt->ltd_exp, len, karg, uarg);
		break;
	}
	case LL_IOC_HSM_PROGRESS: {
		const struct hsm_progress_kernel *hpk = karg;

		tgt = lmv_find_target(lmv, &hpk->hpk_fid);
		if (IS_ERR(tgt))
			RETURN(PTR_ERR(tgt));
		rc = obd_iocontrol(cmd, tgt->ltd_exp, len, karg, uarg);
		break;
	}
	case LL_IOC_HSM_REQUEST: {
		struct hsm_user_request *hur = karg;
		unsigned int reqcount = hur->hur_request.hr_itemcount;

		if (reqcount == 0)
			RETURN(0);

		/* if the request is about a single fid
		 * or if there is a single MDS, no need to split
		 * the request. */
		if (reqcount == 1 || count == 1) {
			tgt = lmv_find_target(lmv,
					      &hur->hur_user_item[0].hui_fid);
			if (IS_ERR(tgt))
				RETURN(PTR_ERR(tgt));
			rc = obd_iocontrol(cmd, tgt->ltd_exp, len, karg, uarg);
		} else {
			/* split fid list to their respective MDS */
			for (i = 0; i < count; i++) {
				unsigned int		nr, reqlen;
				int			rc1;
				struct hsm_user_request *req;

				tgt = lmv->tgts[i];
				if (tgt == NULL || tgt->ltd_exp == NULL)
					continue;

				nr = lmv_hsm_req_count(lmv, hur, tgt);
				if (nr == 0) /* nothing for this MDS */
					continue;

				/* build a request with fids for this MDS */
				reqlen = offsetof(typeof(*hur),
						  hur_user_item[nr])
						+ hur->hur_request.hr_data_len;
				OBD_ALLOC_LARGE(req, reqlen);
				if (req == NULL)
					RETURN(-ENOMEM);

				lmv_hsm_req_build(lmv, hur, tgt, req);

				rc1 = obd_iocontrol(cmd, tgt->ltd_exp, reqlen,
						    req, uarg);
				if (rc1 != 0 && rc == 0)
					rc = rc1;
				OBD_FREE_LARGE(req, reqlen);
			}
		}
		break;
	}
	case LL_IOC_LOV_SWAP_LAYOUTS: {
		struct md_op_data	*op_data = karg;
		struct lmv_tgt_desc	*tgt1, *tgt2;

		tgt1 = lmv_find_target(lmv, &op_data->op_fid1);
		if (IS_ERR(tgt1))
			RETURN(PTR_ERR(tgt1));

		tgt2 = lmv_find_target(lmv, &op_data->op_fid2);
		if (IS_ERR(tgt2))
			RETURN(PTR_ERR(tgt2));

		if ((tgt1->ltd_exp == NULL) || (tgt2->ltd_exp == NULL))
			RETURN(-EINVAL);

		/* only files on same MDT can have their layouts swapped */
		if (tgt1->ltd_idx != tgt2->ltd_idx)
			RETURN(-EPERM);

		rc = obd_iocontrol(cmd, tgt1->ltd_exp, len, karg, uarg);
		break;
	}
	case LL_IOC_HSM_CT_START: {
		struct lustre_kernelcomm *lk = karg;
		if (lk->lk_flags & LK_FLG_STOP)
			rc = lmv_hsm_ct_unregister(lmv, cmd, len, lk, uarg);
		else
			rc = lmv_hsm_ct_register(lmv, cmd, len, lk, uarg);
		break;
	}
	default:
		for (i = 0; i < count; i++) {
			struct obd_device *mdc_obd;
			int err;

			tgt = lmv->tgts[i];
			if (tgt == NULL || tgt->ltd_exp == NULL)
				continue;
			/* ll_umount_begin() sets force flag but for lmv, not
			 * mdc. Let's pass it through */
			mdc_obd = class_exp2obd(tgt->ltd_exp);
			mdc_obd->obd_force = obddev->obd_force;
			err = obd_iocontrol(cmd, tgt->ltd_exp, len, karg, uarg);
			if (err == -ENODATA && cmd == OBD_IOC_POLL_QUOTACHECK) {
				RETURN(err);
			} else if (err) {
				if (tgt->ltd_active) {
					CERROR("error: iocontrol MDC %s on MDT"
					       " idx %d cmd %x: err = %d\n",
					       tgt->ltd_uuid.uuid, i, cmd, err);
					if (!rc)
						rc = err;
				}
			} else
				set = 1;
                }
                if (!set && !rc)
                        rc = -EIO;
        }
        RETURN(rc);
}

#if 0
static int lmv_all_chars_policy(int count, const char *name,
                                int len)
{
        unsigned int c = 0;

        while (len > 0)
                c += name[--len];
        c = c % count;
        return c;
}

static int lmv_nid_policy(struct lmv_obd *lmv)
{
        struct obd_import *imp;
        __u32              id;

        /*
         * XXX: To get nid we assume that underlying obd device is mdc.
         */
        imp = class_exp2cliimp(lmv->tgts[0].ltd_exp);
        id = imp->imp_connection->c_self ^ (imp->imp_connection->c_self >> 32);
        return id % lmv->desc.ld_tgt_count;
}

static int lmv_choose_mds(struct lmv_obd *lmv, struct md_op_data *op_data,
                          placement_policy_t placement)
{
        switch (placement) {
        case PLACEMENT_CHAR_POLICY:
                return lmv_all_chars_policy(lmv->desc.ld_tgt_count,
                                            op_data->op_name,
                                            op_data->op_namelen);
        case PLACEMENT_NID_POLICY:
                return lmv_nid_policy(lmv);

        default:
                break;
        }

        CERROR("Unsupported placement policy %x\n", placement);
        return -EINVAL;
}
#endif

/**
 * This is _inode_ placement policy function (not name).
 */
static int lmv_placement_policy(struct obd_device *obd,
                                struct md_op_data *op_data,
                                mdsno_t *mds)
{
	struct lmv_obd          *lmv = &obd->u.lmv;
	ENTRY;

	LASSERT(mds != NULL);

	if (lmv->desc.ld_tgt_count == 1) {
		*mds = 0;
		RETURN(0);
	}

	/**
	 * If stripe_offset is provided during setdirstripe
	 * (setdirstripe -i xx), xx MDS will be choosen.
	 */
	if (op_data->op_cli_flags & CLI_SET_MEA) {
		struct lmv_user_md *lum;

		lum = (struct lmv_user_md *)op_data->op_data;
		if (lum->lum_type == LMV_STRIPE_TYPE &&
		    lum->lum_stripe_offset != -1) {
			if (lum->lum_stripe_offset >= lmv->desc.ld_tgt_count) {
				CERROR("%s: Stripe_offset %d > MDT count %d:"
				       " rc = %d\n", obd->obd_name,
				       lum->lum_stripe_offset,
				       lmv->desc.ld_tgt_count, -ERANGE);
				RETURN(-ERANGE);
			}
			*mds = lum->lum_stripe_offset;
			RETURN(0);
		}
	}

	/* Allocate new fid on target according to operation type and parent
	 * home mds. */
	*mds = op_data->op_mds;
	RETURN(0);
}

int __lmv_fid_alloc(struct lmv_obd *lmv, struct lu_fid *fid,
		    mdsno_t mds)
{
	struct lmv_tgt_desc	*tgt;
	int			 rc;
	ENTRY;

	tgt = lmv_get_target(lmv, mds);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	/*
	 * New seq alloc and FLD setup should be atomic. Otherwise we may find
	 * on server that seq in new allocated fid is not yet known.
	 */
	mutex_lock(&tgt->ltd_fid_mutex);

	if (tgt->ltd_active == 0 || tgt->ltd_exp == NULL)
		GOTO(out, rc = -ENODEV);

        /*
         * Asking underlaying tgt layer to allocate new fid.
         */
        rc = obd_fid_alloc(tgt->ltd_exp, fid, NULL);
        if (rc > 0) {
                LASSERT(fid_is_sane(fid));
                rc = 0;
        }

        EXIT;
out:
	mutex_unlock(&tgt->ltd_fid_mutex);
        return rc;
}

int lmv_fid_alloc(struct obd_export *exp, struct lu_fid *fid,
                  struct md_op_data *op_data)
{
        struct obd_device     *obd = class_exp2obd(exp);
        struct lmv_obd        *lmv = &obd->u.lmv;
        mdsno_t                mds = 0;
        int                    rc;
        ENTRY;

        LASSERT(op_data != NULL);
        LASSERT(fid != NULL);

        rc = lmv_placement_policy(obd, op_data, &mds);
        if (rc) {
                CERROR("Can't get target for allocating fid, "
                       "rc %d\n", rc);
                RETURN(rc);
        }

        rc = __lmv_fid_alloc(lmv, fid, mds);
        if (rc) {
                CERROR("Can't alloc new fid, rc %d\n", rc);
                RETURN(rc);
        }

        RETURN(rc);
}

static int lmv_setup(struct obd_device *obd, struct lustre_cfg *lcfg)
{
        struct lmv_obd             *lmv = &obd->u.lmv;
        struct lprocfs_static_vars  lvars;
        struct lmv_desc            *desc;
        int                         rc;
        ENTRY;

        if (LUSTRE_CFG_BUFLEN(lcfg, 1) < 1) {
                CERROR("LMV setup requires a descriptor\n");
                RETURN(-EINVAL);
        }

        desc = (struct lmv_desc *)lustre_cfg_buf(lcfg, 1);
        if (sizeof(*desc) > LUSTRE_CFG_BUFLEN(lcfg, 1)) {
                CERROR("Lmv descriptor size wrong: %d > %d\n",
                       (int)sizeof(*desc), LUSTRE_CFG_BUFLEN(lcfg, 1));
                RETURN(-EINVAL);
        }

	OBD_ALLOC(lmv->tgts, sizeof(*lmv->tgts) * 32);
	if (lmv->tgts == NULL)
		RETURN(-ENOMEM);
	lmv->tgts_size = 32;

	obd_str2uuid(&lmv->desc.ld_uuid, desc->ld_uuid.uuid);
	lmv->desc.ld_tgt_count = 0;
	lmv->desc.ld_active_tgt_count = 0;
	lmv->max_cookiesize = 0;
	lmv->max_def_easize = 0;
	lmv->max_easize = 0;
	lmv->lmv_placement = PLACEMENT_CHAR_POLICY;

	spin_lock_init(&lmv->lmv_lock);
	mutex_init(&lmv->init_mutex);

	lprocfs_lmv_init_vars(&lvars);

	lprocfs_obd_setup(obd, lvars.obd_vars);
	lprocfs_alloc_md_stats(obd, 0);
#ifdef LPROCFS
	{
		rc = lprocfs_seq_create(obd->obd_proc_entry, "target_obd",
					0444, &lmv_proc_target_fops, obd);
		if (rc)
			CWARN("%s: error adding LMV target_obd file: rc = %d\n",
			      obd->obd_name, rc);
	}
#endif
	rc = fld_client_init(&lmv->lmv_fld, obd->obd_name,
			     LUSTRE_CLI_FLD_HASH_DHT);
	if (rc) {
		CERROR("Can't init FLD, err %d\n", rc);
		GOTO(out, rc);
	}

        RETURN(0);

out:
        return rc;
}

static int lmv_cleanup(struct obd_device *obd)
{
	struct lmv_obd   *lmv = &obd->u.lmv;
	ENTRY;

	fld_client_fini(&lmv->lmv_fld);
	if (lmv->tgts != NULL) {
		int i;
		for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
			if (lmv->tgts[i] == NULL)
				continue;
			lmv_del_target(lmv, i);
		}
		OBD_FREE(lmv->tgts, sizeof(*lmv->tgts) * lmv->tgts_size);
		lmv->tgts_size = 0;
	}
	RETURN(0);
}

static int lmv_process_config(struct obd_device *obd, obd_count len, void *buf)
{
	struct lustre_cfg	*lcfg = buf;
	struct obd_uuid		obd_uuid;
	int			gen;
	__u32			index;
	int			rc;
	ENTRY;

	switch (lcfg->lcfg_command) {
	case LCFG_ADD_MDC:
		/* modify_mdc_tgts add 0:lustre-clilmv  1:lustre-MDT0000_UUID
		 * 2:0  3:1  4:lustre-MDT0000-mdc_UUID */
		if (LUSTRE_CFG_BUFLEN(lcfg, 1) > sizeof(obd_uuid.uuid))
			GOTO(out, rc = -EINVAL);

		obd_str2uuid(&obd_uuid,  lustre_cfg_buf(lcfg, 1));

		if (sscanf(lustre_cfg_buf(lcfg, 2), "%d", &index) != 1)
			GOTO(out, rc = -EINVAL);
		if (sscanf(lustre_cfg_buf(lcfg, 3), "%d", &gen) != 1)
			GOTO(out, rc = -EINVAL);
		rc = lmv_add_target(obd, &obd_uuid, index, gen);
		GOTO(out, rc);
	default:
		CERROR("Unknown command: %d\n", lcfg->lcfg_command);
		GOTO(out, rc = -EINVAL);
	}
out:
	RETURN(rc);
}

static int lmv_statfs(const struct lu_env *env, struct obd_export *exp,
                      struct obd_statfs *osfs, __u64 max_age, __u32 flags)
{
	struct obd_device	*obd = class_exp2obd(exp);
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct obd_statfs	*temp;
	int			 rc = 0;
	__u32			 i;
	ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        OBD_ALLOC(temp, sizeof(*temp));
        if (temp == NULL)
                RETURN(-ENOMEM);

        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		if (lmv->tgts[i] == NULL || lmv->tgts[i]->ltd_exp == NULL)
			continue;

		rc = obd_statfs(env, lmv->tgts[i]->ltd_exp, temp,
				max_age, flags);
		if (rc) {
			CERROR("can't stat MDS #%d (%s), error %d\n", i,
			       lmv->tgts[i]->ltd_exp->exp_obd->obd_name,
			       rc);
			GOTO(out_free_temp, rc);
		}

		if (i == 0) {
			*osfs = *temp;
			/* If the statfs is from mount, it will needs
			 * retrieve necessary information from MDT0.
			 * i.e. mount does not need the merged osfs
			 * from all of MDT.
			 * And also clients can be mounted as long as
			 * MDT0 is in service*/
			if (flags & OBD_STATFS_FOR_MDT0)
				GOTO(out_free_temp, rc);
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
                         struct lu_fid *fid,
                         struct obd_capa **pc)
{
        struct obd_device    *obd = exp->exp_obd;
        struct lmv_obd       *lmv = &obd->u.lmv;
        int                   rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

	rc = md_getstatus(lmv->tgts[0]->ltd_exp, fid, pc);
	RETURN(rc);
}

static int lmv_getxattr(struct obd_export *exp, const struct lu_fid *fid,
                        struct obd_capa *oc, obd_valid valid, const char *name,
                        const char *input, int input_size, int output_size,
                        int flags, struct ptlrpc_request **request)
{
        struct obd_device      *obd = exp->exp_obd;
        struct lmv_obd         *lmv = &obd->u.lmv;
        struct lmv_tgt_desc    *tgt;
        int                     rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_getxattr(tgt->ltd_exp, fid, oc, valid, name, input,
                         input_size, output_size, flags, request);

        RETURN(rc);
}

static int lmv_setxattr(struct obd_export *exp, const struct lu_fid *fid,
                        struct obd_capa *oc, obd_valid valid, const char *name,
                        const char *input, int input_size, int output_size,
                        int flags, __u32 suppgid,
                        struct ptlrpc_request **request)
{
        struct obd_device      *obd = exp->exp_obd;
        struct lmv_obd         *lmv = &obd->u.lmv;
        struct lmv_tgt_desc    *tgt;
        int                     rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_setxattr(tgt->ltd_exp, fid, oc, valid, name, input,
                         input_size, output_size, flags, suppgid,
                         request);

        RETURN(rc);
}

static int lmv_getattr(struct obd_export *exp, struct md_op_data *op_data,
                       struct ptlrpc_request **request)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct lmv_tgt_desc     *tgt;
        int                      rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, &op_data->op_fid1);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

	if (op_data->op_flags & MF_GET_MDT_IDX) {
		op_data->op_mds = tgt->ltd_idx;
		RETURN(0);
	}

        rc = md_getattr(tgt->ltd_exp, op_data, request);

        RETURN(rc);
}

static int lmv_null_inode(struct obd_export *exp, const struct lu_fid *fid)
{
        struct obd_device   *obd = exp->exp_obd;
        struct lmv_obd      *lmv = &obd->u.lmv;
	__u32                i;
        int                  rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        CDEBUG(D_INODE, "CBDATA for "DFID"\n", PFID(fid));

	/*
	 * With DNE every object can have two locks in different namespaces:
	 * lookup lock in space of MDT storing direntry and update/open lock in
	 * space of MDT storing inode.
	 */
	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		if (lmv->tgts[i] == NULL || lmv->tgts[i]->ltd_exp == NULL)
			continue;
		md_null_inode(lmv->tgts[i]->ltd_exp, fid);
	}

	RETURN(0);
}

static int lmv_find_cbdata(struct obd_export *exp, const struct lu_fid *fid,
                           ldlm_iterator_t it, void *data)
{
        struct obd_device   *obd = exp->exp_obd;
        struct lmv_obd      *lmv = &obd->u.lmv;
	__u32                i;
        int                  rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        CDEBUG(D_INODE, "CBDATA for "DFID"\n", PFID(fid));

	/*
	 * With DNE every object can have two locks in different namespaces:
	 * lookup lock in space of MDT storing direntry and update/open lock in
	 * space of MDT storing inode.
	 */
	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		if (lmv->tgts[i] == NULL || lmv->tgts[i]->ltd_exp == NULL)
			continue;
		rc = md_find_cbdata(lmv->tgts[i]->ltd_exp, fid, it, data);
		if (rc)
			RETURN(rc);
	}

	RETURN(rc);
}


static int lmv_close(struct obd_export *exp, struct md_op_data *op_data,
                     struct md_open_data *mod, struct ptlrpc_request **request)
{
        struct obd_device     *obd = exp->exp_obd;
        struct lmv_obd        *lmv = &obd->u.lmv;
        struct lmv_tgt_desc   *tgt;
        int                    rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, &op_data->op_fid1);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        CDEBUG(D_INODE, "CLOSE "DFID"\n", PFID(&op_data->op_fid1));
        rc = md_close(tgt->ltd_exp, op_data, mod, request);
        RETURN(rc);
}

struct lmv_tgt_desc
*lmv_locate_mds(struct lmv_obd *lmv, struct md_op_data *op_data,
                struct lu_fid *fid)
{
	struct lmv_stripe_md	*lsm = op_data->op_mea1;
        struct lmv_tgt_desc	*tgt;
        int			sidx;

        if (!lsm || lsm->lsm_count <= 1 || op_data->op_namelen == 0) {
                tgt = lmv_find_target(lmv, fid);
		if (IS_ERR(tgt))
			return tgt;

                op_data->op_mds = tgt->ltd_idx;
                return tgt;
        }

	sidx = raw_name2idx(lsm->lsm_hash_type, lsm->lsm_count,
			    op_data->op_name, op_data->op_namelen);

	LASSERT(sidx < lsm->lsm_count);
	*fid = lsm->lsm_oinfo[sidx].lmo_fid;
	op_data->op_mds = lsm->lsm_oinfo[sidx].lmo_mds;
	tgt = lmv_get_target(lmv, lsm->lsm_oinfo[sidx].lmo_mds);

	CDEBUG(D_INFO, "locate on idx %d mds %u \n", sidx,
	       op_data->op_mds);
	return tgt;
}

int lmv_create(struct obd_export *exp, struct md_op_data *op_data,
               const void *data, int datalen, int mode, __u32 uid,
               __u32 gid, cfs_cap_t cap_effective, __u64 rdev,
               struct ptlrpc_request **request)
{
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt;
	int                      rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	if (!lmv->desc.ld_active_tgt_count)
		RETURN(-EIO);

	/* This is for choosing the MDT for name entry, and for
	 * striped directory, it will reset op_fid1 with the FID
	 * of the stripe where name is created */
	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	CDEBUG(D_INODE, "CREATE name '%*s' on "DFID" -> mds #%x\n",
	       op_data->op_namelen, op_data->op_name, PFID(&op_data->op_fid1),
	       op_data->op_mds);

	rc = lmv_fid_alloc(exp, &op_data->op_fid2, op_data);
	if (rc)
		RETURN(rc);

	/* Send the create request to the MDT where the object
	 * is located */
	tgt = lmv_find_target(lmv, &op_data->op_fid2);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));
	op_data->op_mds = tgt->ltd_idx;

	CDEBUG(D_INODE, "CREATE obj "DFID" -> mds #%x\n",
	       PFID(&op_data->op_fid2), op_data->op_mds);

	op_data->op_flags |= MF_MDC_CANCEL_FID1;
	rc = md_create(tgt->ltd_exp, op_data, data, datalen, mode, uid, gid,
		       cap_effective, rdev, request);
	if (rc == 0) {
		if (*request == NULL)
			RETURN(rc);
		CDEBUG(D_INODE, "Created - "DFID"\n", PFID(&op_data->op_fid2));
	}
	RETURN(rc);
}

static int lmv_done_writing(struct obd_export *exp,
                            struct md_op_data *op_data,
                            struct md_open_data *mod)
{
        struct obd_device     *obd = exp->exp_obd;
        struct lmv_obd        *lmv = &obd->u.lmv;
        struct lmv_tgt_desc   *tgt;
        int                    rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, &op_data->op_fid1);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_done_writing(tgt->ltd_exp, op_data, mod);
        RETURN(rc);
}

static int
lmv_enqueue_remote(struct obd_export *exp, struct ldlm_enqueue_info *einfo,
		   struct lookup_intent *it, struct md_op_data *op_data,
		   struct lustre_handle *lockh, void *lmm, int lmmsize,
		   __u64 extra_lock_flags)
{
        struct ptlrpc_request      *req = it->d.lustre.it_data;
        struct obd_device          *obd = exp->exp_obd;
        struct lmv_obd             *lmv = &obd->u.lmv;
        struct lustre_handle        plock;
        struct lmv_tgt_desc        *tgt;
        struct md_op_data          *rdata;
        struct lu_fid               fid1;
        struct mdt_body            *body;
        int                         rc = 0;
        int                         pmode;
        ENTRY;

        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
        LASSERT(body != NULL);

        if (!(body->valid & OBD_MD_MDS))
                RETURN(0);

        CDEBUG(D_INODE, "REMOTE_ENQUEUE '%s' on "DFID" -> "DFID"\n",
               LL_IT2STR(it), PFID(&op_data->op_fid1), PFID(&body->fid1));

        /*
         * We got LOOKUP lock, but we really need attrs.
         */
        pmode = it->d.lustre.it_lock_mode;
        LASSERT(pmode != 0);
        memcpy(&plock, lockh, sizeof(plock));
        it->d.lustre.it_lock_mode = 0;
        it->d.lustre.it_data = NULL;
        fid1 = body->fid1;

        ptlrpc_req_finished(req);

        tgt = lmv_find_target(lmv, &fid1);
        if (IS_ERR(tgt))
                GOTO(out, rc = PTR_ERR(tgt));

        OBD_ALLOC_PTR(rdata);
        if (rdata == NULL)
                GOTO(out, rc = -ENOMEM);

        rdata->op_fid1 = fid1;
        rdata->op_bias = MDS_CROSS_REF;

        rc = md_enqueue(tgt->ltd_exp, einfo, it, rdata, lockh,
                        lmm, lmmsize, NULL, extra_lock_flags);
        OBD_FREE_PTR(rdata);
        EXIT;
out:
        ldlm_lock_decref(&plock, pmode);
        return rc;
}

static int
lmv_enqueue(struct obd_export *exp, struct ldlm_enqueue_info *einfo,
            struct lookup_intent *it, struct md_op_data *op_data,
            struct lustre_handle *lockh, void *lmm, int lmmsize,
	    struct ptlrpc_request **req, __u64 extra_lock_flags)
{
	struct obd_device        *obd = exp->exp_obd;
	struct lmv_obd           *lmv = &obd->u.lmv;
	struct lmv_tgt_desc      *tgt;
	int                       rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	CDEBUG(D_INODE, "ENQUEUE '%s' on "DFID"\n",
	       LL_IT2STR(it), PFID(&op_data->op_fid1));

	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	CDEBUG(D_INODE, "ENQUEUE '%s' on "DFID" -> mds #%d\n",
	       LL_IT2STR(it), PFID(&op_data->op_fid1), tgt->ltd_idx);

	rc = md_enqueue(tgt->ltd_exp, einfo, it, op_data, lockh,
			lmm, lmmsize, req, extra_lock_flags);

	if (rc == 0 && it && it->it_op == IT_OPEN) {
		rc = lmv_enqueue_remote(exp, einfo, it, op_data, lockh,
					lmm, lmmsize, extra_lock_flags);
	}
	RETURN(rc);
}

static int
lmv_getattr_name(struct obd_export *exp,struct md_op_data *op_data,
                 struct ptlrpc_request **request)
{
	struct ptlrpc_request   *req = NULL;
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt;
	struct mdt_body         *body;
	int                      rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	CDEBUG(D_INODE, "GETATTR_NAME for %*s on "DFID" -> mds #%d\n",
	       op_data->op_namelen, op_data->op_name, PFID(&op_data->op_fid1),
	       tgt->ltd_idx);

	rc = md_getattr_name(tgt->ltd_exp, op_data, request);
	if (rc != 0)
		RETURN(rc);

	body = req_capsule_server_get(&(*request)->rq_pill,
				      &RMF_MDT_BODY);
	LASSERT(body != NULL);

	if (body->valid & OBD_MD_MDS) {
		struct lu_fid rid = body->fid1;
		CDEBUG(D_INODE, "Request attrs for "DFID"\n",
		       PFID(&rid));

		tgt = lmv_find_target(lmv, &rid);
		if (IS_ERR(tgt)) {
			ptlrpc_req_finished(*request);
			RETURN(PTR_ERR(tgt));
		}

		op_data->op_fid1 = rid;
		op_data->op_valid |= OBD_MD_FLCROSSREF;
		op_data->op_namelen = 0;
		op_data->op_name = NULL;
		rc = md_getattr_name(tgt->ltd_exp, op_data, &req);
		ptlrpc_req_finished(*request);
		*request = req;
	}

	RETURN(rc);
}

#define md_op_data_fid(op_data, fl)                     \
        (fl == MF_MDC_CANCEL_FID1 ? &op_data->op_fid1 : \
         fl == MF_MDC_CANCEL_FID2 ? &op_data->op_fid2 : \
         fl == MF_MDC_CANCEL_FID3 ? &op_data->op_fid3 : \
         fl == MF_MDC_CANCEL_FID4 ? &op_data->op_fid4 : \
         NULL)

static int lmv_early_cancel(struct obd_export *exp, struct md_op_data *op_data,
                            int op_tgt, ldlm_mode_t mode, int bits, int flag)
{
        struct lu_fid          *fid = md_op_data_fid(op_data, flag);
        struct obd_device      *obd = exp->exp_obd;
        struct lmv_obd         *lmv = &obd->u.lmv;
        struct lmv_tgt_desc    *tgt;
        ldlm_policy_data_t      policy = {{0}};
        int                     rc = 0;
        ENTRY;

        if (!fid_is_sane(fid))
                RETURN(0);

	tgt = lmv_find_target(lmv, fid);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	if (tgt->ltd_idx != op_tgt) {
		CDEBUG(D_INODE, "EARLY_CANCEL on "DFID"\n", PFID(fid));
		policy.l_inodebits.bits = bits;
		rc = md_cancel_unused(tgt->ltd_exp, fid, &policy,
				      mode, LCF_ASYNC, NULL);
	} else {
		CDEBUG(D_INODE,
		       "EARLY_CANCEL skip operation target %d on "DFID"\n",
		       op_tgt, PFID(fid));
		op_data->op_flags |= flag;
		rc = 0;
	}

	RETURN(rc);
}

/*
 * llite passes fid of an target inode in op_data->op_fid1 and id of directory in
 * op_data->op_fid2
 */
static int lmv_link(struct obd_export *exp, struct md_op_data *op_data,
                    struct ptlrpc_request **request)
{
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt;
	int                      rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	LASSERT(op_data->op_namelen != 0);

	CDEBUG(D_INODE, "LINK "DFID":%*s to "DFID"\n",
	       PFID(&op_data->op_fid2), op_data->op_namelen,
	       op_data->op_name, PFID(&op_data->op_fid1));

	op_data->op_fsuid = current_fsuid();
	op_data->op_fsgid = current_fsgid();
	op_data->op_cap = cfs_curproc_cap_pack();
	if (op_data->op_mea2) {
		struct lmv_stripe_md *lsm = op_data->op_mea2;
		int index;

		index = raw_name2idx(lsm->lsm_hash_type, lsm->lsm_count,
				     op_data->op_name, op_data->op_namelen);
		LASSERT(index < lsm->lsm_count);
		op_data->op_fid2 = lsm->lsm_oinfo[index].lmo_fid;
	}

	tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid2);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	/*
	 * Cancel UPDATE lock on child (fid1).
	 */
	op_data->op_flags |= MF_MDC_CANCEL_FID2;
	rc = lmv_early_cancel(exp, op_data, tgt->ltd_idx, LCK_EX,
			      MDS_INODELOCK_UPDATE, MF_MDC_CANCEL_FID1);
	if (rc != 0)
		RETURN(rc);

	rc = md_link(tgt->ltd_exp, op_data, request);

	RETURN(rc);
}

static int lmv_rename(struct obd_export *exp, struct md_op_data *op_data,
                      const char *old, int oldlen, const char *new, int newlen,
                      struct ptlrpc_request **request)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *src_tgt;
	int			rc;
	ENTRY;

        LASSERT(oldlen != 0);

        CDEBUG(D_INODE, "RENAME %*s in "DFID":%d to %*s in "DFID":%d\n",
               oldlen, old, PFID(&op_data->op_fid1),
	       op_data->op_mea1 ? op_data->op_mea1->lsm_count : 0,
               newlen, new, PFID(&op_data->op_fid2),
	       op_data->op_mea2 ? op_data->op_mea2->lsm_count : 0);

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

	op_data->op_fsuid = current_fsuid();
	op_data->op_fsgid = current_fsgid();
	op_data->op_cap = cfs_curproc_cap_pack();

	src_tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(src_tgt))
		RETURN(PTR_ERR(src_tgt));

	if (op_data->op_mea1) {
		struct lmv_stripe_md *lsm = op_data->op_mea1;
		int index;

		index = raw_name2idx(lsm->lsm_hash_type, lsm->lsm_count,
				     old, oldlen);
		LASSERT(index < lsm->lsm_count);
		op_data->op_fid1 = lsm->lsm_oinfo[index].lmo_fid;
	}

	if (op_data->op_mea2) {
		struct lmv_stripe_md *lsm = op_data->op_mea2;
		int index;

		index = raw_name2idx(lsm->lsm_hash_type, lsm->lsm_count,
				     new, newlen);
		LASSERT(index < lsm->lsm_count);
		op_data->op_fid2 = lsm->lsm_oinfo[index].lmo_fid;
	}

	/*
	 * LOOKUP lock on src child (fid3) should also be cancelled for
	 * src_tgt in mdc_rename.
	 */
	op_data->op_flags |= MF_MDC_CANCEL_FID1 | MF_MDC_CANCEL_FID3;

	/*
	 * Cancel UPDATE locks on tgt parent (fid2), tgt_tgt is its
	 * own target.
	 */
	rc = lmv_early_cancel(exp, op_data, src_tgt->ltd_idx,
			      LCK_EX, MDS_INODELOCK_UPDATE,
			      MF_MDC_CANCEL_FID2);

	/*
	 * Cancel LOOKUP locks on tgt child (fid4) for parent tgt_tgt.
	 */
	if (rc == 0) {
		rc = lmv_early_cancel(exp, op_data, src_tgt->ltd_idx,
				      LCK_EX, MDS_INODELOCK_LOOKUP,
				      MF_MDC_CANCEL_FID4);
	}

	/*
	 * Cancel all the locks on tgt child (fid4).
	 */
	if (rc == 0)
		rc = lmv_early_cancel(exp, op_data, src_tgt->ltd_idx,
				      LCK_EX, MDS_INODELOCK_FULL,
				      MF_MDC_CANCEL_FID4);

	if (rc == 0)
		rc = md_rename(src_tgt->ltd_exp, op_data, old, oldlen,
			       new, newlen, request);
	RETURN(rc);
}

static int lmv_setattr(struct obd_export *exp, struct md_op_data *op_data,
                       void *ea, int ealen, void *ea2, int ea2len,
                       struct ptlrpc_request **request,
                       struct md_open_data **mod)
{
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt;
	int                      rc = 0;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	CDEBUG(D_INODE, "SETATTR for "DFID", valid 0x%x\n",
	       PFID(&op_data->op_fid1), op_data->op_attr.ia_valid);

	op_data->op_flags |= MF_MDC_CANCEL_FID1;
	tgt = lmv_find_target(lmv, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	rc = md_setattr(tgt->ltd_exp, op_data, ea, ealen, ea2,
			ea2len, request, mod);

	RETURN(rc);
}

static int lmv_fsync(struct obd_export *exp, const struct lu_fid *fid,
		     struct obd_capa *oc, struct ptlrpc_request **request)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt;
	int			 rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc != 0)
		RETURN(rc);

	tgt = lmv_find_target(lmv, fid);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	rc = md_fsync(tgt->ltd_exp, fid, oc, request);
	RETURN(rc);
}

/*
 * Adjust a set of pages, each page containing an array of lu_dirpages,
 * so that each page can be used as a single logical lu_dirpage.
 *
 * A lu_dirpage is laid out as follows, where s = ldp_hash_start,
 * e = ldp_hash_end, f = ldp_flags, p = padding, and each "ent" is a
 * struct lu_dirent.  It has size up to LU_PAGE_SIZE. The ldp_hash_end
 * value is used as a cookie to request the next lu_dirpage in a
 * directory listing that spans multiple pages (two in this example):
 *   ________
 *  |        |
 * .|--------v-------   -----.
 * |s|e|f|p|ent|ent| ... |ent|
 * '--|--------------   -----'   Each CFS_PAGE contains a single
 *    '------.                   lu_dirpage.
 * .---------v-------   -----.
 * |s|e|f|p|ent| 0 | ... | 0 |
 * '-----------------   -----'
 *
 * However, on hosts where the native VM page size (PAGE_CACHE_SIZE) is
 * larger than LU_PAGE_SIZE, a single host page may contain multiple
 * lu_dirpages. After reading the lu_dirpages from the MDS, the
 * ldp_hash_end of the first lu_dirpage refers to the one immediately
 * after it in the same CFS_PAGE (arrows simplified for brevity, but
 * in general e0==s1, e1==s2, etc.):
 *
 * .--------------------   -----.
 * |s0|e0|f0|p|ent|ent| ... |ent|
 * |---v----------------   -----|
 * |s1|e1|f1|p|ent|ent| ... |ent|
 * |---v----------------   -----|  Here, each CFS_PAGE contains
 *             ...                 multiple lu_dirpages.
 * |---v----------------   -----|
 * |s'|e'|f'|p|ent|ent| ... |ent|
 * '---|----------------   -----'
 *     v
 * .----------------------------.
 * |        next CFS_PAGE       |
 *
 * This structure is transformed into a single logical lu_dirpage as follows:
 *
 * - Replace e0 with e' so the request for the next lu_dirpage gets the page
 *   labeled 'next CFS_PAGE'.
 *
 * - Copy the LDF_COLLIDE flag from f' to f0 to correctly reflect whether
 *   a hash collision with the next page exists.
 *
 * - Adjust the lde_reclen of the ending entry of each lu_dirpage to span
 *   to the first entry of the next lu_dirpage.
 */
#if PAGE_CACHE_SIZE > LU_PAGE_SIZE
static void lmv_adjust_dirpages(struct page **pages, int ncfspgs, int nlupgs)
{
	int i;

	for (i = 0; i < ncfspgs; i++) {
		struct lu_dirpage	*dp = kmap(pages[i]);
		struct lu_dirpage	*first = dp;
		struct lu_dirent	*end_dirent = NULL;
		struct lu_dirent	*ent;
		__u64			hash_end = dp->ldp_hash_end;
		__u32			flags = dp->ldp_flags;

		while (--nlupgs > 0) {
			ent = lu_dirent_start(dp);
			for (end_dirent = ent; ent != NULL;
			     end_dirent = ent, ent = lu_dirent_next(ent));

			/* Advance dp to next lu_dirpage. */
			dp = (struct lu_dirpage *)((char *)dp + LU_PAGE_SIZE);

			/* Check if we've reached the end of the CFS_PAGE. */
			if (!((unsigned long)dp & ~CFS_PAGE_MASK))
				break;

			/* Save the hash and flags of this lu_dirpage. */
			hash_end = dp->ldp_hash_end;
			flags = dp->ldp_flags;

			/* Check if lu_dirpage contains no entries. */
			if (!end_dirent)
				break;

			/* Enlarge the end entry lde_reclen from 0 to
			 * first entry of next lu_dirpage. */
			LASSERT(le16_to_cpu(end_dirent->lde_reclen) == 0);
			end_dirent->lde_reclen =
				cpu_to_le16((char *)(dp->ldp_entries) -
					    (char *)end_dirent);
		}

		first->ldp_hash_end = hash_end;
		first->ldp_flags &= ~cpu_to_le32(LDF_COLLIDE);
		first->ldp_flags |= flags & cpu_to_le32(LDF_COLLIDE);

		kunmap(pages[i]);
	}
	LASSERTF(nlupgs == 0, "left = %d", nlupgs);
}
#else
#define lmv_adjust_dirpages(pages, ncfspgs, nlupgs) do {} while (0)
#endif	/* PAGE_CACHE_SIZE > LU_PAGE_SIZE */

#define NORMAL_MAX_STRIPES 4
int lmv_read_entry(struct obd_export *exp, struct md_op_data *op_data,
		   struct md_callback *cb_op, struct lu_dirent **ldp)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_stripe_md	*lsm = op_data->op_mea1;
	struct lu_dirent	*tmp_ents[NORMAL_MAX_STRIPES];
	struct lu_dirent	**ents = NULL;
	int			stripe_count = lsm == NULL ? 1 : lsm->lsm_count;
	__u64			min_hash;
	int			min_idx = 0;
	int			i;
	int			rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	if (stripe_count > NORMAL_MAX_STRIPES) {
		OBD_ALLOC(ents, sizeof(struct lu_dirent *) * stripe_count);
		if (ents == NULL)
			GOTO(out, rc = -ENOMEM);
	} else {
		ents = tmp_ents;
		memset(ents, 0, sizeof(struct lu_dirent *) * stripe_count);
	}

	min_hash = MDS_DIR_END_OFF;
	for (i = 0; i < stripe_count; i++) {
		struct lmv_tgt_desc *tgt;
		if (likely(lsm == NULL)) {
			tgt = lmv_find_target(lmv, &op_data->op_fid1);
			if (IS_ERR(tgt))
				GOTO(out, rc = PTR_ERR(tgt));
			LASSERT(op_data->op_data != NULL);
		} else {
			tgt = lmv_get_target(lmv, lsm->lsm_oinfo[i].lmo_mds);
			if (IS_ERR(tgt))
				GOTO(out, rc = PTR_ERR(tgt));
			op_data->op_fid1 = lsm->lsm_oinfo[i].lmo_fid;
			op_data->op_fid2 = lsm->lsm_oinfo[i].lmo_fid;
			op_data->op_stripe_offset = i;
		}

		rc = md_read_entry(tgt->ltd_exp, op_data, cb_op, &ents[i]);
		if (rc != 0)
			GOTO(out, rc);

		if (ents[i] != NULL &&
		    le64_to_cpu(ents[i]->lde_hash) <= min_hash) {
			min_hash = le64_to_cpu(ents[i]->lde_hash);
			min_idx = i;
		}
	}

	if (min_hash != MDS_DIR_END_OFF)
		*ldp = ents[min_idx];
	else
		*ldp = NULL;
out:
	if (stripe_count > NORMAL_MAX_STRIPES && ents != NULL)
		OBD_FREE(ents, sizeof(struct lu_dirent *) * stripe_count);

	RETURN(rc);
}

static int lmv_unlink(struct obd_export *exp, struct md_op_data *op_data,
                      struct ptlrpc_request **request)
{
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt = NULL;
	struct mdt_body		*body;
	int                     rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);
retry:
	/* Send unlink requests to the MDT where the child is located */
	if (likely(!fid_is_zero(&op_data->op_fid2)))
		tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid2);
	else
		tgt = lmv_locate_mds(lmv, op_data, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	op_data->op_fsuid = current_fsuid();
	op_data->op_fsgid = current_fsgid();
	op_data->op_cap = cfs_curproc_cap_pack();

	/*
	 * If child's fid is given, cancel unused locks for it if it is from
	 * another export than parent.
	 *
	 * LOOKUP lock for child (fid3) should also be cancelled on parent
	 * tgt_tgt in mdc_unlink().
	 */
	op_data->op_flags |= MF_MDC_CANCEL_FID1 | MF_MDC_CANCEL_FID3;

	/*
	 * Cancel FULL locks on child (fid3).
	 */
	rc = lmv_early_cancel(exp, op_data, tgt->ltd_idx, LCK_EX,
			      MDS_INODELOCK_FULL, MF_MDC_CANCEL_FID3);

	if (rc != 0)
		RETURN(rc);

	CDEBUG(D_INODE, "unlink with fid="DFID"/"DFID" -> mds #%d\n",
	       PFID(&op_data->op_fid1), PFID(&op_data->op_fid2), tgt->ltd_idx);

	rc = md_unlink(tgt->ltd_exp, op_data, request);
	if (rc != 0 && rc != -EREMOTE)
		RETURN(rc);

	body = req_capsule_server_get(&(*request)->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		RETURN(-EPROTO);

	/* Not cross-ref case, just get out of here. */
	if (likely(!(body->valid & OBD_MD_MDS)))
		RETURN(0);

	CDEBUG(D_INODE, "%s: try unlink to another MDT for "DFID"\n",
	       exp->exp_obd->obd_name, PFID(&body->fid1));

	/* This is a remote object, try remote MDT, Note: it may
	 * try more than 1 time here, Considering following case
	 * /mnt/lustre is root on MDT0, remote1 is on MDT1
	 * 1. Initially A does not know where remote1 is, it send
	 *    unlink RPC to MDT0, MDT0 return -EREMOTE, it will
	 *    resend unlink RPC to MDT1 (retry 1st time).
	 *
	 * 2. During the unlink RPC in flight,
	 *    client B mv /mnt/lustre/remote1 /mnt/lustre/remote2
	 *    and create new remote1, but on MDT0
	 *
	 * 3. MDT1 get unlink RPC(from A), then do remote lock on
	 *    /mnt/lustre, then lookup get fid of remote1, and find
	 *    it is remote dir again, and replay -EREMOTE again.
	 *
	 * 4. Then A will resend unlink RPC to MDT0. (retry 2nd times).
	 *
	 * In theory, it might try unlimited time here, but it should
	 * be very rare case.  */
	op_data->op_fid2 = body->fid1;
	ptlrpc_req_finished(*request);
	*request = NULL;

	goto retry;
}

static int lmv_precleanup(struct obd_device *obd, enum obd_cleanup_stage stage)
{
        struct lmv_obd *lmv = &obd->u.lmv;
        int rc = 0;

        switch (stage) {
        case OBD_CLEANUP_EARLY:
                /* XXX: here should be calling obd_precleanup() down to
                 * stack. */
                break;
        case OBD_CLEANUP_EXPORTS:
                fld_client_proc_fini(&lmv->lmv_fld);
                lprocfs_obd_cleanup(obd);
		lprocfs_free_md_stats(obd);
                break;
        default:
                break;
        }
        RETURN(rc);
}

static int lmv_get_info(const struct lu_env *env, struct obd_export *exp,
                        __u32 keylen, void *key, __u32 *vallen, void *val,
                        struct lov_stripe_md *lsm)
{
        struct obd_device       *obd;
        struct lmv_obd          *lmv;
        int                      rc = 0;
        ENTRY;

        obd = class_exp2obd(exp);
        if (obd == NULL) {
                CDEBUG(D_IOCTL, "Invalid client cookie "LPX64"\n",
                       exp->exp_handle.h_cookie);
                RETURN(-EINVAL);
        }

        lmv = &obd->u.lmv;
        if (keylen >= strlen("remote_flag") && !strcmp(key, "remote_flag")) {
                int i;

                rc = lmv_check_connect(obd);
                if (rc)
                        RETURN(rc);

                LASSERT(*vallen == sizeof(__u32));
		for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
			struct lmv_tgt_desc *tgt = lmv->tgts[i];
			/*
			 * All tgts should be connected when this gets called.
			 */
			if (tgt == NULL || tgt->ltd_exp == NULL)
				continue;

			if (!obd_get_info(env, tgt->ltd_exp, keylen, key,
					  vallen, val, NULL))
				RETURN(0);
                }
                RETURN(-EINVAL);
        } else if (KEY_IS(KEY_MAX_EASIZE) || KEY_IS(KEY_CONN_DATA)) {
                rc = lmv_check_connect(obd);
                if (rc)
                        RETURN(rc);

		/*
		 * Forwarding this request to first MDS, it should know LOV
		 * desc.
		 */
		rc = obd_get_info(env, lmv->tgts[0]->ltd_exp, keylen, key,
				  vallen, val, NULL);
		if (!rc && KEY_IS(KEY_CONN_DATA))
			exp->exp_connect_data = *(struct obd_connect_data *)val;
                RETURN(rc);
        } else if (KEY_IS(KEY_TGT_COUNT)) {
                *((int *)val) = lmv->desc.ld_tgt_count;
                RETURN(0);
        }

        CDEBUG(D_IOCTL, "Invalid key\n");
        RETURN(-EINVAL);
}

int lmv_set_info_async(const struct lu_env *env, struct obd_export *exp,
                       obd_count keylen, void *key, obd_count vallen,
                       void *val, struct ptlrpc_request_set *set)
{
        struct lmv_tgt_desc    *tgt;
        struct obd_device      *obd;
        struct lmv_obd         *lmv;
        int rc = 0;
        ENTRY;

        obd = class_exp2obd(exp);
        if (obd == NULL) {
                CDEBUG(D_IOCTL, "Invalid client cookie "LPX64"\n",
                       exp->exp_handle.h_cookie);
                RETURN(-EINVAL);
        }
        lmv = &obd->u.lmv;

        if (KEY_IS(KEY_READ_ONLY) || KEY_IS(KEY_FLUSH_CTX)) {
                int i, err = 0;

		for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
			tgt = lmv->tgts[i];

			if (tgt == NULL || tgt->ltd_exp == NULL)
				continue;

                        err = obd_set_info_async(env, tgt->ltd_exp,
                                                 keylen, key, vallen, val, set);
                        if (err && rc == 0)
                                rc = err;
                }

                RETURN(rc);
        }

        RETURN(-EINVAL);
}

int lmv_pack_md(struct lmv_mds_md **lmmp, struct lmv_stripe_md *lsm,
		int stripe_count)
{
	int	lsm_size = 0;
	int	cplen;
	int	i;
	ENTRY;

	LASSERT(lmmp != NULL);

	/* Free lmm */
	if (*lmmp != NULL && lsm == NULL) {
		lsm_size = lmv_mds_md_size((*lmmp)->lmv_count,
					   (*lmmp)->lmv_magic);
		OBD_FREE(*lmmp, lsm_size);
		*lmmp = NULL;
		RETURN(0);
	}

	/* Alloc lmm */
	if (*lmmp == NULL && lsm == NULL) {
		lsm_size = lmv_mds_md_size(stripe_count, LMV_MAGIC_V1);
		OBD_ALLOC(*lmmp, lsm_size);
		(*lmmp)->lmv_count = stripe_count;
		(*lmmp)->lmv_magic = cpu_to_le32(LMV_MAGIC_V1);
		if (*lmmp == NULL)
			RETURN(-ENOMEM);
		RETURN(lsm_size);
	}
	/* Unpack lmm */
	LASSERT(lsm != NULL);
	lsm_size = lmv_mds_md_size(lsm->lsm_count, lsm->lsm_md_magic);
	if (*lmmp == NULL) {
		OBD_ALLOC(*lmmp, lsm_size);
		if (*lmmp == NULL)
			RETURN(-ENOMEM);
	}

	(*lmmp)->lmv_magic = cpu_to_le32(lsm->lsm_md_magic);
	(*lmmp)->lmv_count = cpu_to_le32(lsm->lsm_count);
	(*lmmp)->lmv_master = cpu_to_le32(lsm->lsm_master);
	(*lmmp)->lmv_hash_type = cpu_to_le32(lsm->lsm_hash_type);

	cplen = strlcpy((*lmmp)->lmv_pool_name, lsm->lsm_md_pool_name,
			sizeof((*lmmp)->lmv_pool_name));

	if (cplen >= sizeof((*lmmp)->lmv_pool_name))
		RETURN(-E2BIG);

	for (i = 0; i < lsm->lsm_count; i++) {
		fid_cpu_to_le(&(*lmmp)->lmv_data[i],
			      &lsm->lsm_oinfo[i].lmo_fid);
	}

	RETURN(lsm_size);
}
EXPORT_SYMBOL(lmv_pack_md);

int lmv_unpack_md(struct obd_export *exp, struct lmv_stripe_md **lsmp,
		  struct lmv_mds_md *lmm, int stripe_count)
{
	struct lmv_stripe_md *lsm;
	int     lsm_size;
	int     cplen;
	int     i;
	ENTRY;

	LASSERT(lsmp != NULL);

	lsm = *lsmp;
	/* Free memmd */
	if (lsm != NULL && lmm == NULL) {
#ifdef __KERNEL__
		for (i = 1; i < lsm->lsm_count; i++) {
			if (lsm->lsm_oinfo[i].lmo_root != NULL)
				iput(lsm->lsm_oinfo[i].lmo_root);
		}
#endif
		lsm_size = lmv_stripe_md_size(lsm->lsm_count);
		OBD_FREE(lsm, lsm_size);
		*lsmp = NULL;
		RETURN(0);
	}

	/* Alloc memmd */
	if (lsm == NULL && lmm == NULL) {
		lsm_size = lmv_stripe_md_size(stripe_count);
		OBD_ALLOC(lsm, lsm_size);
		if (lsm == NULL)
			RETURN(-ENOMEM);
		lsm->lsm_count = stripe_count;
		*lsmp = lsm;
		RETURN(0);
	}

	/* Unpack memmd */
	LASSERT(lmm != NULL);

	LASSERTF(le32_to_cpu(lmm->lmv_magic) == le32_to_cpu(LMV_MAGIC_V1) ||
		 le32_to_cpu(lmm->lmv_magic) == le32_to_cpu(LMV_USER_MAGIC),
		 "lmv magic is %x\n", le32_to_cpu(lmm->lmv_magic));

	if (le32_to_cpu(lmm->lmv_magic) == le32_to_cpu(LMV_MAGIC_V1))
		lsm_size = lmv_stripe_md_size(lmm->lmv_count);
	else
		/**
		 * Unpack default dirstripe(lmv_user_md) to lmv_stripe_md,
		 * stripecount should be 0 then.
		 */
		lsm_size = lmv_stripe_md_size(0);

	if (lsm == NULL) {
		OBD_ALLOC(lsm, lsm_size);
		if (lsm == NULL)
			RETURN(-ENOMEM);
		*lsmp = lsm;
	}

	if (le32_to_cpu(lmm->lmv_magic) == le32_to_cpu(LMV_USER_MAGIC)) {
		lsm->lsm_md_magic = le32_to_cpu(LMV_MAGIC_V1);
		RETURN(lsm_size);
	}

	lsm->lsm_md_magic = le32_to_cpu(lmm->lmv_magic);
	lsm->lsm_count = le32_to_cpu(lmm->lmv_count);
	lsm->lsm_master = le32_to_cpu(lmm->lmv_master);
	lsm->lsm_hash_type = le32_to_cpu(lmm->lmv_hash_type);
	lsm->lsm_layout_version = le32_to_cpu(lmm->lmv_layout_version);

	cplen = strlcpy(lsm->lsm_md_pool_name, lmm->lmv_pool_name,
			sizeof(lsm->lsm_md_pool_name));

	if (cplen >= sizeof(lsm->lsm_md_pool_name))
		RETURN(-E2BIG);

	CDEBUG(D_INFO, "unpack lsm count %d, master %d hash_type %d"
	       "layout_version %d\n", lsm->lsm_count,
	       lsm->lsm_master, lsm->lsm_hash_type,
	       lsm->lsm_layout_version);

	for (i = 0; i < le32_to_cpu(lmm->lmv_count); i++) {
		struct lmv_obd *lmv;
		int rc;

		LASSERT(exp != NULL);
		lmv = &exp->exp_obd->u.lmv;
		fid_le_to_cpu(&lsm->lsm_oinfo[i].lmo_fid,
			      &lmm->lmv_data[i]);
		rc = lmv_fld_lookup(lmv, &lsm->lsm_oinfo[i].lmo_fid,
				    &lsm->lsm_oinfo[i].lmo_mds);
		if (rc != 0)
			RETURN(rc);
		CDEBUG(D_INFO, "unpack fid #%d "DFID"\n", i,
		       PFID(&lsm->lsm_oinfo[i].lmo_fid));
	}
	RETURN(lsm_size);
}

int lmv_alloc_memmd(struct lmv_stripe_md **lsmp, int stripes)
{
        return lmv_unpack_md(NULL, lsmp, NULL, stripes);
}
EXPORT_SYMBOL(lmv_alloc_memmd);

void lmv_free_memmd(struct lmv_stripe_md *lsm)
{
        lmv_unpack_md(NULL, &lsm, NULL, 0);
}
EXPORT_SYMBOL(lmv_free_memmd);

int lmv_unpackmd(struct obd_export *exp, struct lov_stripe_md **lsmp,
                 struct lov_mds_md *lsm, int disk_len)
{
	return lmv_unpack_md(exp, (struct lmv_stripe_md **)lsmp,
			     (struct lmv_mds_md *)lsm, disk_len);
}

int lmv_packmd(struct obd_export *exp, struct lov_mds_md **lmmp,
               struct lov_stripe_md *lsm)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv_obd = &obd->u.lmv;
        struct lmv_stripe_md    *lmv = (struct lmv_stripe_md *)lsm;
        int stripe_count;

        if (lmmp == NULL) {
                if (lsm)
                        stripe_count = lmv->lsm_count;
                else
                        stripe_count = lmv_obd->desc.ld_tgt_count;

                return lmv_mds_md_size(stripe_count, LMV_MAGIC_V1);
        }

        return lmv_pack_md((struct lmv_mds_md **)lmmp,
                           (struct lmv_stripe_md *)lsm, 0);
}

static int lmv_cancel_unused(struct obd_export *exp, const struct lu_fid *fid,
                             ldlm_policy_data_t *policy, ldlm_mode_t mode,
                             ldlm_cancel_flags_t flags, void *opaque)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        int                      rc = 0;
        int                      err;
	__u32                    i;
        ENTRY;

        LASSERT(fid != NULL);

	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active)
			continue;

		err = md_cancel_unused(tgt->ltd_exp, fid, policy, mode, flags,
				       opaque);
		if (!rc)
			rc = err;
	}
	RETURN(rc);
}

int lmv_set_lock_data(struct obd_export *exp, __u64 *lockh, void *data,
                      __u64 *bits)
{
	struct lmv_obd		*lmv = &exp->exp_obd->u.lmv;
	struct lmv_tgt_desc	*tgt = lmv->tgts[0];
	int			 rc;
	ENTRY;

	if (tgt == NULL || tgt->ltd_exp == NULL)
		RETURN(-EINVAL);
	rc =  md_set_lock_data(tgt->ltd_exp, lockh, data, bits);
	RETURN(rc);
}

ldlm_mode_t lmv_lock_match(struct obd_export *exp, __u64 flags,
                           const struct lu_fid *fid, ldlm_type_t type,
                           ldlm_policy_data_t *policy, ldlm_mode_t mode,
                           struct lustre_handle *lockh)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        ldlm_mode_t              rc;
	__u32                    i;
        ENTRY;

        CDEBUG(D_INODE, "Lock match for "DFID"\n", PFID(fid));

        /*
         * With CMD every object can have two locks in different namespaces:
         * lookup lock in space of mds storing direntry and update/open lock in
         * space of mds storing inode. Thus we check all targets, not only that
         * one fid was created in.
         */
        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		struct lmv_tgt_desc *tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active)
			continue;

		rc = md_lock_match(tgt->ltd_exp, flags, fid, type, policy, mode,
				   lockh);
                if (rc)
                        RETURN(rc);
        }

        RETURN(0);
}

int lmv_get_lustre_md(struct obd_export *exp, struct ptlrpc_request *req,
		      struct obd_export *dt_exp, struct obd_export *md_exp,
		      struct lustre_md *md)
{
	struct lmv_obd          *lmv = &exp->exp_obd->u.lmv;
	struct lmv_tgt_desc	*tgt = lmv->tgts[0];

	if (tgt == NULL || tgt->ltd_exp == NULL)
		RETURN(-EINVAL);

	return md_get_lustre_md(lmv->tgts[0]->ltd_exp, req, dt_exp, md_exp, md);
}

int lmv_free_lustre_md(struct obd_export *exp, struct lustre_md *md)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt = lmv->tgts[0];
	ENTRY;

	if (md->lsm_md != NULL)
		lmv_free_memmd(md->lsm_md);
	if (tgt == NULL || tgt->ltd_exp == NULL)
		RETURN(-EINVAL);
	RETURN(md_free_lustre_md(lmv->tgts[0]->ltd_exp, md));
}

int lmv_set_open_replay_data(struct obd_export *exp,
			     struct obd_client_handle *och,
			     struct lookup_intent *it)
{
	struct obd_device	*obd = exp->exp_obd;
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt;
	ENTRY;

	tgt = lmv_find_target(lmv, &och->och_fid);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	RETURN(md_set_open_replay_data(tgt->ltd_exp, och, it));
}

int lmv_clear_open_replay_data(struct obd_export *exp,
                               struct obd_client_handle *och)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct lmv_tgt_desc     *tgt;
        ENTRY;

        tgt = lmv_find_target(lmv, &och->och_fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        RETURN(md_clear_open_replay_data(tgt->ltd_exp, och));
}

static int lmv_get_remote_perm(struct obd_export *exp,
                               const struct lu_fid *fid,
                               struct obd_capa *oc, __u32 suppgid,
                               struct ptlrpc_request **request)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct lmv_tgt_desc     *tgt;
        int                      rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_get_remote_perm(tgt->ltd_exp, fid, oc, suppgid, request);
        RETURN(rc);
}

static int lmv_renew_capa(struct obd_export *exp, struct obd_capa *oc,
                          renew_capa_cb_t cb)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct lmv_tgt_desc     *tgt;
        int                      rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, &oc->c_capa.lc_fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_renew_capa(tgt->ltd_exp, oc, cb);
        RETURN(rc);
}

int lmv_unpack_capa(struct obd_export *exp, struct ptlrpc_request *req,
		    const struct req_msg_field *field, struct obd_capa **oc)
{
	struct lmv_obd		*lmv = &exp->exp_obd->u.lmv;
	struct lmv_tgt_desc	*tgt = lmv->tgts[0];

	if (tgt == NULL || tgt->ltd_exp == NULL)
		RETURN(-EINVAL);
	return md_unpack_capa(tgt->ltd_exp, req, field, oc);
}

int lmv_intent_getattr_async(struct obd_export *exp,
                             struct md_enqueue_info *minfo,
                             struct ldlm_enqueue_info *einfo)
{
	struct md_op_data       *op_data = &minfo->mi_data;
	struct obd_device       *obd = exp->exp_obd;
	struct lmv_obd          *lmv = &obd->u.lmv;
	struct lmv_tgt_desc     *tgt = NULL;
	int                      rc;
	ENTRY;

	rc = lmv_check_connect(obd);
	if (rc)
		RETURN(rc);

	tgt = lmv_find_target(lmv, &op_data->op_fid1);
	if (IS_ERR(tgt))
		RETURN(PTR_ERR(tgt));

	rc = md_intent_getattr_async(tgt->ltd_exp, minfo, einfo);
	RETURN(rc);
}

int lmv_revalidate_lock(struct obd_export *exp, struct lookup_intent *it,
                        struct lu_fid *fid, __u64 *bits)
{
        struct obd_device       *obd = exp->exp_obd;
        struct lmv_obd          *lmv = &obd->u.lmv;
        struct lmv_tgt_desc     *tgt;
        int                      rc;
        ENTRY;

        rc = lmv_check_connect(obd);
        if (rc)
                RETURN(rc);

        tgt = lmv_find_target(lmv, fid);
        if (IS_ERR(tgt))
                RETURN(PTR_ERR(tgt));

        rc = md_revalidate_lock(tgt->ltd_exp, it, fid, bits);
        RETURN(rc);
}

/**
 * For lmv, only need to send request to master MDT, and the master MDT will
 * process with other slave MDTs. The only exception is Q_GETOQUOTA for which
 * we directly fetch data from the slave MDTs.
 */
int lmv_quotactl(struct obd_device *unused, struct obd_export *exp,
		 struct obd_quotactl *oqctl)
{
	struct obd_device   *obd = class_exp2obd(exp);
	struct lmv_obd      *lmv = &obd->u.lmv;
	struct lmv_tgt_desc *tgt = lmv->tgts[0];
	int                  rc = 0;
	__u32                i;
	__u64                curspace, curinodes;
	ENTRY;

	if (tgt == NULL ||
	    tgt->ltd_exp == NULL ||
	    !tgt->ltd_active ||
	    lmv->desc.ld_tgt_count == 0) {
		CERROR("master lmv inactive\n");
		RETURN(-EIO);
	}

        if (oqctl->qc_cmd != Q_GETOQUOTA) {
                rc = obd_quotactl(tgt->ltd_exp, oqctl);
                RETURN(rc);
        }

        curspace = curinodes = 0;
        for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		int err;
		tgt = lmv->tgts[i];

		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active)
			continue;

                err = obd_quotactl(tgt->ltd_exp, oqctl);
                if (err) {
                        CERROR("getquota on mdt %d failed. %d\n", i, err);
                        if (!rc)
                                rc = err;
                } else {
                        curspace += oqctl->qc_dqblk.dqb_curspace;
                        curinodes += oqctl->qc_dqblk.dqb_curinodes;
                }
        }
        oqctl->qc_dqblk.dqb_curspace = curspace;
        oqctl->qc_dqblk.dqb_curinodes = curinodes;

        RETURN(rc);
}

int lmv_quotacheck(struct obd_device *unused, struct obd_export *exp,
                   struct obd_quotactl *oqctl)
{
	struct obd_device	*obd = class_exp2obd(exp);
	struct lmv_obd		*lmv = &obd->u.lmv;
	struct lmv_tgt_desc	*tgt;
	__u32			 i;
	int			 rc = 0;
	ENTRY;

	for (i = 0; i < lmv->desc.ld_tgt_count; i++) {
		int err;
		tgt = lmv->tgts[i];
		if (tgt == NULL || tgt->ltd_exp == NULL || !tgt->ltd_active) {
			CERROR("lmv idx %d inactive\n", i);
			RETURN(-EIO);
		}

                err = obd_quotacheck(tgt->ltd_exp, oqctl);
                if (err && !rc)
                        rc = err;
        }

        RETURN(rc);
}

int lmv_update_lsm_md(struct obd_export *exp, struct lmv_stripe_md *lsm,
		      struct mdt_body *body, ldlm_blocking_callback cb_blocking)
{
	if (lsm->lsm_count <= 1)
		return 0;

	return lmv_revalidate_slaves(exp, body, lsm, cb_blocking, 0);
}

int lmv_merge_attr(struct obd_export *exp, struct lmv_stripe_md *lsm,
		   struct cl_attr *attr)
{
	int i;

	for (i = 0; i < lsm->lsm_count; i++) {
		CDEBUG(D_INFO, ""DFID" size %llu nlink %u atime "LPU64
		       "ctime "LPU64" mtime "LPU64"\n",
		       PFID(&lsm->lsm_oinfo[i].lmo_fid),
		       lsm->lsm_oinfo[i].lmo_size,
		       lsm->lsm_oinfo[i].lmo_nlink,
		       lsm->lsm_oinfo[i].lmo_atime,
		       lsm->lsm_oinfo[i].lmo_ctime,
		       lsm->lsm_oinfo[i].lmo_mtime);
		attr->cat_size += lsm->lsm_oinfo[i].lmo_size;
		if (i != 0)
			attr->cat_nlink += lsm->lsm_oinfo[i].lmo_nlink - 2;
		else
			attr->cat_nlink = lsm->lsm_oinfo[i].lmo_nlink;

		if (attr->cat_atime < lsm->lsm_oinfo[i].lmo_atime)
			attr->cat_atime = lsm->lsm_oinfo[i].lmo_atime;
		if (attr->cat_ctime < lsm->lsm_oinfo[i].lmo_ctime)
			attr->cat_ctime = lsm->lsm_oinfo[i].lmo_ctime;
		if (attr->cat_mtime < lsm->lsm_oinfo[i].lmo_mtime)
			attr->cat_mtime = lsm->lsm_oinfo[i].lmo_mtime;
	}
	return 0;
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
        .o_get_info             = lmv_get_info,
        .o_set_info_async       = lmv_set_info_async,
        .o_packmd               = lmv_packmd,
        .o_unpackmd             = lmv_unpackmd,
        .o_notify               = lmv_notify,
        .o_get_uuid             = lmv_get_uuid,
        .o_iocontrol            = lmv_iocontrol,
        .o_quotacheck           = lmv_quotacheck,
        .o_quotactl             = lmv_quotactl
};

struct md_ops lmv_md_ops = {
        .m_getstatus            = lmv_getstatus,
        .m_null_inode		= lmv_null_inode,
        .m_find_cbdata          = lmv_find_cbdata,
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
	.m_fsync		= lmv_fsync,
	.m_read_entry           = lmv_read_entry,
        .m_unlink               = lmv_unlink,
        .m_init_ea_size         = lmv_init_ea_size,
        .m_cancel_unused        = lmv_cancel_unused,
        .m_set_lock_data        = lmv_set_lock_data,
        .m_lock_match           = lmv_lock_match,
        .m_get_lustre_md        = lmv_get_lustre_md,
        .m_free_lustre_md       = lmv_free_lustre_md,
        .m_update_lsm_md	= lmv_update_lsm_md,
        .m_merge_attr		= lmv_merge_attr,
        .m_set_open_replay_data = lmv_set_open_replay_data,
        .m_clear_open_replay_data = lmv_clear_open_replay_data,
        .m_renew_capa           = lmv_renew_capa,
        .m_unpack_capa          = lmv_unpack_capa,
        .m_get_remote_perm      = lmv_get_remote_perm,
        .m_intent_getattr_async = lmv_intent_getattr_async,
        .m_revalidate_lock      = lmv_revalidate_lock
};

int __init lmv_init(void)
{
        struct lprocfs_static_vars lvars;
        int                        rc;

        lprocfs_lmv_init_vars(&lvars);

        rc = class_register_type(&lmv_obd_ops, &lmv_md_ops,
                                 lvars.module_vars, LUSTRE_LMV_NAME, NULL);
        return rc;
}

#ifdef __KERNEL__
static void lmv_exit(void)
{
        class_unregister_type(LUSTRE_LMV_NAME);
}

MODULE_AUTHOR("Sun Microsystems, Inc. <http://www.lustre.org/>");
MODULE_DESCRIPTION("Lustre Logical Metadata Volume OBD driver");
MODULE_LICENSE("GPL");

module_init(lmv_init);
module_exit(lmv_exit);
#endif
