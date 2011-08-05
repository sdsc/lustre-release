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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_RPC

#ifndef __KERNEL__
# include <liblustre.h>
#endif
#include <obd.h>
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_lib.h>
#include <lustre_ha.h>
#include <lustre_net.h>
#include <lprocfs_status.h>

#define NIDS_MAX        32

struct uuid_nid_data {
        cfs_list_t       un_list;
        char             un_uuid[UUID_MAX + 1];
        int              un_nid_count;
        lnet_nid_t       un_nids[NIDS_MAX];
};

/* FIXME: This should probably become more elegant than a global linked list */
static cfs_list_t           g_uuid_list;
static cfs_spinlock_t       g_uuid_lock;

void class_init_uuidlist(void)
{
        CFS_INIT_LIST_HEAD(&g_uuid_list);
        cfs_spin_lock_init(&g_uuid_lock);
}

void class_exit_uuidlist(void)
{
        /* delete all */
        class_del_uuid(NULL);
}

int lustre_uuid_to_peer(const char *uuid, lnet_nid_t *peer_nid, int index)
{
        struct uuid_nid_data *data;
        int rc = -ENOENT;

        cfs_spin_lock(&g_uuid_lock);
        cfs_list_for_each_entry(data, &g_uuid_list, un_list) {
                if (strcmp(data->un_uuid, uuid) == 0) {
                        if (index >= data->un_nid_count)
                                break;

                        rc = 0;
                        *peer_nid = data->un_nids[index];
                        break;
                }
        }
        cfs_spin_unlock(&g_uuid_lock);
        return rc;
}

/* Add a nid to a niduuid.  Multiple nids can be added to a single uuid;
   LNET will choose the best one. */
int class_add_uuid(const char *uuid, __u64 nid)
{
        struct uuid_nid_data *data, *entry;
        int found = 0;
        int rc = 0;

        LASSERT(nid != 0);  /* valid newconfig NID is never zero */

        if (strlen(uuid) > UUID_MAX)
                return -ENOSPC;

        OBD_ALLOC(data, sizeof(*data));
        if (data == NULL)
                return -ENOMEM;

        strcpy(data->un_uuid, uuid);
        data->un_nids[0] = nid;
        data->un_nid_count = 1;

        cfs_spin_lock (&g_uuid_lock);
        cfs_list_for_each_entry(entry, &g_uuid_list, un_list) {
                if (strcmp(entry->un_uuid, uuid) == 0) {
                        int i;
                        found = 1;
                        for (i = 0; i < data->un_nid_count; i++) {
                                if (nid == entry->un_nids[i]) {
                                        rc = -EEXIST;
                                        break;
                                }
                        }
                        if (rc == 0)
                                data->un_nids[++data->un_nid_count] = nid;
                }
        }
        if (!found)
                cfs_list_add(&data->un_list, &g_uuid_list);
        cfs_spin_unlock (&g_uuid_lock);

        if (found) {
                CDEBUG(D_INFO, "found uuid %s %s cnt=%d\n", uuid,
                       libcfs_nid2str(nid), entry->un_nid_count);
                OBD_FREE(data, sizeof(*data));
        } else {
                CDEBUG(D_INFO, "add uuid %s %s\n", uuid, libcfs_nid2str(nid));
        }
        return 0;
}

/* Delete the nids for one uuid if specified, otherwise delete all */
int class_del_uuid(const char *uuid)
{
        CFS_LIST_HEAD(deathrow);
        struct uuid_nid_data *data;
        int found = 0;

        cfs_spin_lock (&g_uuid_lock);
        if (uuid == NULL) {
                cfs_list_splice_init(&g_uuid_list, &deathrow);
                found = 1;
        } else {
                cfs_list_for_each_entry(data, &g_uuid_list, un_list) {
                        if (strcmp(data->un_uuid, uuid))
                                continue;
                        cfs_list_move(&data->un_list, &deathrow);
                        found = 1;
                        break;
                }
        }
        cfs_spin_unlock (&g_uuid_lock);

        if (!found) {
                if (uuid)
                        CERROR("Try to delete a non-existent uuid %s\n", uuid);
                return -EINVAL;
        }

        while (!cfs_list_empty(&deathrow)) {
                data = cfs_list_entry(deathrow.next, struct uuid_nid_data,
                                      un_list);
                cfs_list_del(&data->un_list);

                CDEBUG(D_INFO, "del uuid %s %s/%d\n", data->un_uuid,
                       libcfs_nid2str(data->un_nids[0]),
                       data->un_nid_count);

                OBD_FREE(data, sizeof(*data));
        }

        return 0;
}

/* find @uuid by @nid */
int class_find_uuid(__u64 nid, struct obd_uuid *uuid, int skip_count)
{
        struct uuid_nid_data *entry;
        int found = 0;

        LASSERT(skip_count >= 0);

        cfs_spin_lock (&g_uuid_lock);
        cfs_list_for_each_entry(entry, &g_uuid_list, un_list) {
                int i;

                for(i = 0; i < entry->un_nid_count; i++) {
                        if (entry->un_nids[i] == nid) {
                                found = 1;
                                break;
                        }
                }

                if (found) {
                        if (skip_count--) {
                                found = 0;
                                continue;
                        }
                        obd_str2uuid(uuid, entry->un_uuid);
                        break;
                }
        }
        cfs_spin_unlock (&g_uuid_lock);
        return found ? 0 : -ENOENT;
}
EXPORT_SYMBOL(class_find_uuid);
