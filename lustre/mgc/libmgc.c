/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  lustre/mgc/mgc_request.c
 *  Lustre Management Client
 *
 *  Copyright (C) 2006 Cluster File Systems, Inc.
 *   Author: Nathan Rutman <nathan@clusterfs.com>
 *
 *   This file is part of Lustre, http://www.lustre.org
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
 *
 */
/* Minimal MGC for liblustre: only used to read the config log from the MGS
   at setup time, no updates. */
 
#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MGC

#include <liblustre.h>

#include <obd_class.h>
#include <lustre_dlm.h>
#include <lustre_log.h>
#include <lustre_fsfilt.h>
#include <lustre_disk.h>

static int mgc_setup(struct obd_device *obd, obd_count len, void *buf)
{
        int rc;
        ENTRY;

        ptlrpcd_addref();

        rc = client_obd_setup(obd, len, buf);
        if (rc)
                GOTO(err_decref, rc);

        rc = obd_llog_init(obd, obd, 0, NULL, NULL);
        if (rc) {
                CERROR("failed to setup llogging subsystems\n");
                GOTO(err_cleanup, rc);
        }

        RETURN(rc);

err_cleanup:
        client_obd_cleanup(obd);
err_decref:
        ptlrpcd_decref();
        RETURN(rc);
}

static int mgc_precleanup(struct obd_device *obd, enum obd_cleanup_stage stage)
{
        int rc = 0;
        ENTRY;

        switch (stage) {
        case OBD_CLEANUP_EARLY: 
        case OBD_CLEANUP_EXPORTS:
                break;
        case OBD_CLEANUP_SELF_EXP:
                rc = obd_llog_finish(obd, 0);
                if (rc != 0)
                        CERROR("failed to cleanup llogging subsystems\n");
                break;
        case OBD_CLEANUP_OBD:
                break;
        }
        RETURN(rc);
}

static int mgc_cleanup(struct obd_device *obd)
{
        struct client_obd *cli = &obd->u.cli;
        int rc;
        ENTRY;

        LASSERT(cli->cl_mgc_vfsmnt == NULL);
        
        ptlrpcd_decref();

        rc = client_obd_cleanup(obd);
        RETURN(rc);
}

static int mgc_llog_init(struct obd_device *obd, struct obd_device *tgt,
                         int count, struct llog_catid *logid, 
                         struct obd_uuid *uuid)
{
        struct llog_ctxt *ctxt;
        int rc;
        ENTRY;

        rc = llog_setup(obd, LLOG_CONFIG_REPL_CTXT, tgt, 0, NULL,
                        &llog_client_ops);
        if (rc == 0) {
                ctxt = llog_get_context(obd, LLOG_CONFIG_REPL_CTXT);
                ctxt->loc_imp = obd->u.cli.cl_import;
        }

        RETURN(rc);
}

static int mgc_llog_finish(struct obd_device *obd, int count)
{
        int rc;
        ENTRY;

        rc = llog_cleanup(llog_get_context(obd, LLOG_CONFIG_REPL_CTXT));

        RETURN(rc);
}

struct obd_ops mgc_obd_ops = {
        .o_owner        = THIS_MODULE,
        .o_setup        = mgc_setup,
        .o_precleanup   = mgc_precleanup,
        .o_cleanup      = mgc_cleanup,
        .o_add_conn     = client_import_add_conn,
        .o_del_conn     = client_import_del_conn,
        .o_connect      = client_connect_import,
        .o_disconnect   = client_disconnect_export,
        .o_llog_init    = mgc_llog_init,
        .o_llog_finish  = mgc_llog_finish,
};

int __init mgc_init(void)
{
        return class_register_type(&mgc_obd_ops, NULL, LUSTRE_MGC_NAME);
}

