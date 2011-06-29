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
 * lustre/mdt/mdt_hsm.c
 *
 * Lustre Metadata Target (mdt) request handler
 *
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Author: Jacques-Charles Lafoucriere <jc.lafoucriere@cea.fr>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include "mdt_internal.h"

/* coordinator service interface */
/* fake functions, real one are in coordinator patch
 * static declaration is used to make compilation
 * error if code is not removed in final patch
 */
static int mdt_hsm_coordinator_get_actions(struct mdt_thread_info *mti,
                                           struct hsm_action_list *hal)
{ return 0; }
/* end of fake functions */

/**
 * Retrieve the current HSM flags, archive id and undergoing HSM requests for
 * the fid provided in RPC body.
 *
 * Current requests are read from coordinator states.
 *
 * This is MDS_HSM_STATE_GET RPC handler.
 */
int mdt_hsm_state_get(struct mdt_thread_info *info)
{
        struct mdt_object      *obj = info->mti_object;
        struct md_attr         *ma  = &info->mti_attr;
        struct hsm_user_state  *hus;
        struct mdt_lock_handle *lh;
        int rc;
        ENTRY;

        lh = &info->mti_lh[MDT_LH_CHILD];
        mdt_lock_reg_init(lh, LCK_PR);
        rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP,
                             MDT_LOCAL_LOCK);
        if (rc)
                RETURN(rc);

        /* Only valid if client is remote */
        rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
        if (rc)
                GOTO(out_unlock, rc = err_serious(rc));

        ma->ma_need |= MA_HSM;
        rc = mo_attr_get(info->mti_env, mdt_object_child(obj), ma);
        if (rc) {
                CERROR("Could not get attr\n");
                GOTO(out_ucred, rc);
        }

        if (req_capsule_get_size(info->mti_pill, &RMF_CAPA1, RCL_CLIENT))
                mdt_set_capainfo(info, 0, &info->mti_body->fid1,
                            req_capsule_client_get(info->mti_pill, &RMF_CAPA1));

        hus = req_capsule_server_get(info->mti_pill, &RMF_HSM_USER_STATE);
        LASSERT(hus);

        /* Current HSM flags */
        hus->hus_states = ma->ma_hsm.mh_flags;
        hus->hus_archive_num = ma->ma_hsm.mh_archive_number;

        EXIT;
out_ucred:
        mdt_exit_ucred(info);
out_unlock:
        mdt_object_unlock(info, obj, lh, 1);
        return(rc);
}

/**
 * Retrieve undergoing HSM requests for the fid provided in RPC body.
 * Current requests are read from coordinator states.
 *
 * This is MDS_HSM_ACTION RPC handler.
 */
int mdt_hsm_action(struct mdt_thread_info *info)
{
        struct hsm_current_action  *hca;
        struct hsm_action_list *hal = NULL;
        struct hsm_action_item *hai;
        int rc, len;
        ENTRY;

        /* Only valid if client is remote */
        rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
        if (rc)
                RETURN(rc = err_serious(rc));

        if (req_capsule_get_size(info->mti_pill, &RMF_CAPA1, RCL_CLIENT))
                mdt_set_capainfo(info, 0, &info->mti_body->fid1,
                            req_capsule_client_get(info->mti_pill, &RMF_CAPA1));

        hca = req_capsule_server_get(info->mti_pill, &RMF_HSM_CURRENT_ACTION);
        LASSERT(hca);

        /* Coordinator information */
        len = sizeof(*hal) + MTI_NAME_MAXLEN /* fsname */ +
              cfs_size_round(sizeof(*hai));

        OBD_ALLOC(hal, len);
        if (hal == NULL)
                GOTO(out_ucred, -ENOMEM);

        hal->hal_version = HAL_VERSION;
        hal->hal_archive_num = 0;
        obd_uuid2fsname(hal->hal_fsname, mdt2obd_dev(info->mti_mdt)->obd_name,
                        MTI_NAME_MAXLEN);
        hal->hal_count = 1;
        hai = hai_zero(hal);
        hai->hai_action = HSMA_NONE;
        hai->hai_cookie = 0;
        hai->hai_gid = 0;
        hai->hai_fid = info->mti_body->fid1;
        hai->hai_len = sizeof(*hai);

        rc = mdt_hsm_coordinator_get_actions(info, hal);
        if (rc)
                GOTO(out_free, rc);

        /* cookie is used to give back request status */
        if (hai->hai_cookie == 0)
                hca->hca_state = HPS_WAITING;
        else
                hca->hca_state = HPS_RUNNING;

        switch (hai->hai_action) {
                case HSMA_NONE:
                        hca->hca_action = HUA_NONE;
                        break;
                case HSMA_ARCHIVE:
                        hca->hca_action = HUA_ARCHIVE;
                        break;
                case HSMA_RESTORE:
                        hca->hca_action = HUA_RESTORE;
                        break;
                case HSMA_REMOVE:
                        hca->hca_action = HUA_REMOVE;
                        break;
                case HSMA_CANCEL:
                        hca->hca_action = HUA_CANCEL;
                        break;
                default:
                        hca->hca_action = HUA_NONE;
                        CERROR("Unknown hsm action: %d on "DFID"\n",
                               hai->hai_action, PFID(&hai->hai_fid));
                        break;
        }

        hca->hca_location = hai->hai_extent;

        EXIT;
out_free:
        OBD_FREE(hal, len);
out_ucred:
        mdt_exit_ucred(info);
        return(rc);
}


/**
 * Change HSM state and archive number of a file.
 *
 * Archive number is changed iif the value is not 0.
 * The new flagset that will be computed should result in a coherent state.
 * This function checks that are flags are compatible.
 *
 * This is MDS_HSM_STATE_SET RPC handler.
 */
int mdt_hsm_state_set(struct mdt_thread_info *info)
{
        struct mdt_object      *obj = info->mti_object;
        struct md_attr         *ma  = &info->mti_attr;
        struct hsm_state_set   *hss;
        struct mdt_lock_handle *lh;
        int rc;
        int flags;
        ENTRY;

        lh = &info->mti_lh[MDT_LH_CHILD];
        mdt_lock_reg_init(lh, LCK_PW);
        rc = mdt_object_lock(info, obj, lh, MDS_INODELOCK_LOOKUP,
                             MDT_LOCAL_LOCK);
        if (rc)
                RETURN(rc);

        /* Only valid if client is remote */
        rc = mdt_init_ucred(info, (struct mdt_body *)info->mti_body);
        if (rc)
                GOTO(out_obj, rc = err_serious(rc));

        ma->ma_need |= MA_HSM;
        rc = mo_attr_get(info->mti_env, mdt_object_child(obj), ma);
        if (rc) {
                CERROR("Could not get attr\n");
                GOTO(out_ucred, rc);
        }

        hss = req_capsule_client_get(info->mti_pill, &RMF_HSM_STATE_SET);
        LASSERT(hss);

        if (req_capsule_get_size(info->mti_pill, &RMF_CAPA1, RCL_CLIENT))
                mdt_set_capainfo(info, 0, &info->mti_body->fid1,
                            req_capsule_client_get(info->mti_pill, &RMF_CAPA1));

        /* Change HSM flags depending on provided masks */
        if (hss->hss_valid & HSS_SETMASK)
                ma->ma_hsm.mh_flags |= hss->hss_setmask;
        if (hss->hss_valid & HSS_CLEARMASK)
                ma->ma_hsm.mh_flags &= ~ hss->hss_clearmask;

        /* Change archive_num if provided. */
        if (hss->hss_valid & HSS_ARCHIVE_NUM) {
                if (!(ma->ma_hsm.mh_flags & HS_EXISTS)) {
                        CDEBUG(D_HSM, "Could not set an archive number if "
                               "HSM EXISTS flag is not set.");
                        GOTO(out_ucred, rc);
                }
                ma->ma_hsm.mh_archive_number = hss->hss_archive_num;
        }

        /* Check for inconsistant HSM flagset.
         * DIRTY without EXISTS: no dirty if no archive was created.
         * DIRTY and RELEASED: a dirty file could not be released.
         * RELEASED without ARCHIVED: do not release a non-archived file.
         * LOST without ARCHIVED: cannot lost a non-archived file.
         */
        flags = ma->ma_hsm.mh_flags;
        if (((flags & HS_DIRTY) && !(flags & HS_EXISTS)) ||
            ((flags & HS_RELEASED) && (flags & HS_DIRTY)) ||
            ((flags & HS_RELEASED) && !(flags & HS_ARCHIVED)) ||
            ((flags & HS_LOST)     && !(flags & HS_ARCHIVED)) ) {
                CDEBUG(D_HSM, "Incompatible flag change.\n");
                rc = -EINVAL;
                GOTO(out_ucred, rc);
        }

        /* Save the modified flags */
        rc = mo_attr_set(info->mti_env, mdt_object_child(obj), ma);
        if (rc)
                GOTO(out_ucred, rc);

        EXIT;

out_ucred:
        mdt_exit_ucred(info);
out_obj:
        mdt_object_unlock(info, obj, lh, 1);
        return(rc);
}
