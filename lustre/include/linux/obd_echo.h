/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) 2001  Cluster File Systems, Inc.
 *
 * This code is issued under the GNU General Public License.
 * See the file COPYING in this distribution
 */

#ifndef _OBD_ECHO_H
#define _OBD_ECHO_H

#define OBD_ECHO_DEVICENAME "obdecho"
#define OBD_ECHO_CLIENT_DEVICENAME "echo_client"

struct ec_object {
        struct list_head       eco_obj_chain;
        struct obd_device     *eco_device;
        int                    eco_refcount;
        int                    eco_deleted;
        obd_id                 eco_id;
        struct lov_stripe_md  *eco_lsm;
};

struct ec_lock {
        struct list_head       ecl_exp_chain;
        struct ec_object      *ecl_object;
        __u64                  ecl_cookie;
        struct lustre_handle   ecl_lock_handle;
        struct ldlm_extent     ecl_extent;
        __u32                  ecl_mode;
};

#endif
