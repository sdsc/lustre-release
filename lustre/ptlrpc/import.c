/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (c) 2002, 2003 Cluster File Systems, Inc.
 *   Author: Mike Shaver <shaver@clusterfs.com>
 *
 *   This file is part of the Lustre file system, http://www.lustre.org
 *   Lustre is a trademark of Cluster File Systems, Inc.
 *
 *   You may have signed or agreed to another license before downloading
 *   this software.  If so, you are bound by the terms and conditions
 *   of that agreement, and the following does not apply to you.  See the
 *   LICENSE file included with this distribution for more information.
 *
 *   If you did not agree to a different license, then this copy of Lustre
 *   is open source software; you can redistribute it and/or modify it
 *   under the terms of version 2 of the GNU General Public License as
 *   published by the Free Software Foundation.
 *
 *   In either case, Lustre is distributed in the hope that it will be
 *   useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 *   of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   license text for more details.
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <obd_support.h>
#include <lustre_ha.h>
#include <lustre_net.h>
#include <lustre_import.h>
#include <lustre_export.h>
#include <obd.h>
#include <obd_class.h>

#include "ptlrpc_internal.h"

struct ptlrpc_connect_async_args {
         __u64 pcaa_peer_committed;
        int pcaa_initial_connect;
};

/* A CLOSED import should remain so. */
#define IMPORT_SET_STATE_NOLOCK(imp, state)                                    \
do {                                                                           \
        if (imp->imp_state != LUSTRE_IMP_CLOSED) {                             \
               CDEBUG(D_HA, "%p %s: changing import state from %s to %s\n",    \
                      imp, obd2cli_tgt(imp->imp_obd),                          \
                      ptlrpc_import_state_name(imp->imp_state),                \
                      ptlrpc_import_state_name(state));                        \
               imp->imp_state = state;                                         \
        }                                                                      \
} while(0)

#define IMPORT_SET_STATE(imp, state)            \
do {                                            \
        spin_lock(&imp->imp_lock);              \
        IMPORT_SET_STATE_NOLOCK(imp, state);    \
        spin_unlock(&imp->imp_lock);            \
} while(0)


static int ptlrpc_connect_interpret(struct ptlrpc_request *request,
                                    void * data, int rc);
int ptlrpc_import_recovery_state_machine(struct obd_import *imp);

/* Only this function is allowed to change the import state when it is
 * CLOSED. I would rather refcount the import and free it after
 * disconnection like we do with exports. To do that, the client_obd
 * will need to save the peer info somewhere other than in the import,
 * though. */
int ptlrpc_init_import(struct obd_import *imp)
{
        spin_lock(&imp->imp_lock);

        imp->imp_generation++;
        imp->imp_state =  LUSTRE_IMP_NEW;

        spin_unlock(&imp->imp_lock);

        return 0;
}
EXPORT_SYMBOL(ptlrpc_init_import);

#define UUID_STR "_UUID"
static void deuuidify(char *uuid, const char *prefix, char **uuid_start,
                      int *uuid_len)
{
        *uuid_start = !prefix || strncmp(uuid, prefix, strlen(prefix))
                ? uuid : uuid + strlen(prefix);

        *uuid_len = strlen(*uuid_start);

        if (*uuid_len < strlen(UUID_STR))
                return;

        if (!strncmp(*uuid_start + *uuid_len - strlen(UUID_STR),
                    UUID_STR, strlen(UUID_STR)))
                *uuid_len -= strlen(UUID_STR);
}

/* Returns true if import was FULL, false if import was already not
 * connected.
 * @imp - import to be disconnected
 * @conn_cnt - connection count (epoch) of the request that timed out
 *             and caused the disconnection.  In some cases, multiple
 *             inflight requests can fail to a single target (e.g. OST
 *             bulk requests) and if one has already caused a reconnection
 *             (increasing the import->conn_cnt) the older failure should
 *             not also cause a reconnection.  If zero it forces a reconnect.
 */
int ptlrpc_set_import_discon(struct obd_import *imp, __u32 conn_cnt)
{
        int rc = 0;

        spin_lock(&imp->imp_lock);

        if (imp->imp_state == LUSTRE_IMP_FULL &&
            (conn_cnt == 0 || conn_cnt == imp->imp_conn_cnt)) {
                char *target_start;
                int   target_len;

                deuuidify(obd2cli_tgt(imp->imp_obd), NULL,
                          &target_start, &target_len);

                if (imp->imp_replayable) {
                        LCONSOLE_WARN("%s: Connection to service %.*s via nid "
                               "%s was lost; in progress operations using this "
                               "service will wait for recovery to complete.\n",
                               imp->imp_obd->obd_name, target_len, target_start,
                               libcfs_nid2str(imp->imp_connection->c_peer.nid));
                } else {
                        LCONSOLE_ERROR_MSG(0x166, "%s: Connection to service "
                                           "%.*s via nid %s was lost; in progress"
                                           "operations using this service will"
                                           "fail.\n",
                                           imp->imp_obd->obd_name,
                                           target_len, target_start,
                                 libcfs_nid2str(imp->imp_connection->c_peer.nid));
                }
                ptlrpc_deactivate_timeouts(imp);
                IMPORT_SET_STATE_NOLOCK(imp, LUSTRE_IMP_DISCON);
                spin_unlock(&imp->imp_lock);
    
                if (obd_dump_on_timeout)
                        libcfs_debug_dumplog();

                obd_import_event(imp->imp_obd, imp, IMP_EVENT_DISCON);
                rc = 1;
        } else {
                spin_unlock(&imp->imp_lock);
                CDEBUG(D_HA, "%s: import %p already %s (conn %u, was %u): %s\n",
                       imp->imp_client->cli_name, imp,
                       (imp->imp_state == LUSTRE_IMP_FULL &&
                        imp->imp_conn_cnt > conn_cnt) ?
                       "reconnected" : "not connected", imp->imp_conn_cnt,
                       conn_cnt, ptlrpc_import_state_name(imp->imp_state));
        }

        return rc;
}

/*
 * This acts as a barrier; all existing requests are rejected, and
 * no new requests will be accepted until the import is valid again.
 */
void ptlrpc_deactivate_import(struct obd_import *imp)
{
        ENTRY;

        spin_lock(&imp->imp_lock);
        if (imp->imp_invalid) {
                spin_unlock(&imp->imp_lock);
                EXIT;
                return;
        }

        CDEBUG(D_HA, "setting import %s INVALID\n", obd2cli_tgt(imp->imp_obd));
        imp->imp_invalid = 1;
        imp->imp_generation++;
        spin_unlock(&imp->imp_lock);

        ptlrpc_abort_inflight(imp);
        obd_import_event(imp->imp_obd, imp, IMP_EVENT_INACTIVE);

        EXIT;
}

/*
 * This function will invalidate the import, if necessary, then block
 * for all the RPC completions, and finally notify the obd to
 * invalidate its state (ie cancel locks, clear pending requests,
 * etc).
 */
void ptlrpc_invalidate_import(struct obd_import *imp)
{
        struct l_wait_info lwi;
        int rc;

        atomic_inc(&imp->imp_inval_count);

        ptlrpc_deactivate_import(imp);

        LASSERT(imp->imp_invalid);

        /* wait for all requests to error out and call completion callbacks */
        lwi = LWI_TIMEOUT_INTERVAL(cfs_timeout_cap(cfs_time_seconds(obd_timeout)), 
                                   HZ, NULL, NULL);
        rc = l_wait_event(imp->imp_recovery_waitq,
                          (atomic_read(&imp->imp_inflight) == 0), &lwi);

        if (rc)
                CDEBUG(D_HA, "%s: rc = %d waiting for callback (%d != 0)\n",
                       obd2cli_tgt(imp->imp_obd), rc,
                       atomic_read(&imp->imp_inflight));

        obd_import_event(imp->imp_obd, imp, IMP_EVENT_INVALIDATE);
        sptlrpc_import_flush_all_ctx(imp);

        atomic_dec(&imp->imp_inval_count);
        cfs_waitq_signal(&imp->imp_recovery_waitq);
}

/* unset imp_invalid */
void ptlrpc_activate_import(struct obd_import *imp)
{
        struct obd_device *obd = imp->imp_obd;

        spin_lock(&imp->imp_lock);
        imp->imp_invalid = 0;
        ptlrpc_activate_timeouts(imp);
        spin_unlock(&imp->imp_lock);
        obd_import_event(obd, imp, IMP_EVENT_ACTIVE);
}

void ptlrpc_fail_import(struct obd_import *imp, __u32 conn_cnt)
{
        ENTRY;

        LASSERT(!imp->imp_dlm_fake);

        if (ptlrpc_set_import_discon(imp, conn_cnt)) {
                if (!imp->imp_replayable) {
                        CDEBUG(D_HA, "import %s@%s for %s not replayable, "
                               "auto-deactivating\n",
                               obd2cli_tgt(imp->imp_obd),
                               imp->imp_connection->c_remote_uuid.uuid,
                               imp->imp_obd->obd_name);
                        ptlrpc_deactivate_import(imp);
                }

                CDEBUG(D_HA, "%s: waking up pinger\n",
                       obd2cli_tgt(imp->imp_obd));

                spin_lock(&imp->imp_lock);
                imp->imp_force_verify = 1;
                spin_unlock(&imp->imp_lock);

                ptlrpc_pinger_wake_up();
        }
        EXIT;
}

static int import_select_connection(struct obd_import *imp)
{
        struct obd_import_conn *imp_conn = NULL, *conn;
        struct obd_export *dlmexp;
        ENTRY;

        spin_lock(&imp->imp_lock);

        if (list_empty(&imp->imp_conn_list)) {
                CERROR("%s: no connections available\n",
                        imp->imp_obd->obd_name);
                spin_unlock(&imp->imp_lock);
                RETURN(-EINVAL);
        }

        list_for_each_entry(conn, &imp->imp_conn_list, oic_item) {
                CDEBUG(D_HA, "%s: connect to NID %s last attempt "LPU64"\n",
                       imp->imp_obd->obd_name,
                       libcfs_nid2str(conn->oic_conn->c_peer.nid),
                       conn->oic_last_attempt);
                /* Throttle the reconnect rate to once per RECONNECT_INTERVAL */
                if (cfs_time_before_64(conn->oic_last_attempt + 
                                       RECONNECT_INTERVAL * HZ,
                                       cfs_time_current_64())) {
                        /* If we have never tried this connection since the
                           the last successful attempt, go with this one */
                        if (cfs_time_beforeq_64(conn->oic_last_attempt,
                                               imp->imp_last_success_conn)) {
                                imp_conn = conn;
                                break;
                        }

                        /* Both of these connections have already been tried
                           since the last successful connection; just choose the
                           least recently used */
                        if (!imp_conn)
                                imp_conn = conn;
                        else if (cfs_time_before_64(conn->oic_last_attempt,
                                                    imp_conn->oic_last_attempt))
                                imp_conn = conn;
                }
        }

        /* if not found, simply choose the current one */
        if (!imp_conn) {
                LASSERT(imp->imp_conn_current);
                imp_conn = imp->imp_conn_current;
        }
        LASSERT(imp_conn->oic_conn);

        imp_conn->oic_last_attempt = cfs_time_current_64();

        /* switch connection, don't mind if it's same as the current one */
        if (imp->imp_connection)
                ptlrpc_put_connection(imp->imp_connection);
        imp->imp_connection = ptlrpc_connection_addref(imp_conn->oic_conn);

        dlmexp =  class_conn2export(&imp->imp_dlm_handle);
        LASSERT(dlmexp != NULL);
        if (dlmexp->exp_connection)
                ptlrpc_put_connection(dlmexp->exp_connection);
        dlmexp->exp_connection = ptlrpc_connection_addref(imp_conn->oic_conn);
        class_export_put(dlmexp);

        if (imp->imp_conn_current != imp_conn) {
                if (imp->imp_conn_current)
                        LCONSOLE_INFO("Changing connection for %s to %s/%s\n",
                                      imp->imp_obd->obd_name,
                                      imp_conn->oic_uuid.uuid,
                                      libcfs_nid2str(imp_conn->oic_conn->c_peer.nid));
                imp->imp_conn_current = imp_conn;
        }

        CDEBUG(D_HA, "%s: import %p using connection %s/%s\n",
               imp->imp_obd->obd_name, imp, imp_conn->oic_uuid.uuid,
               libcfs_nid2str(imp_conn->oic_conn->c_peer.nid));

        spin_unlock(&imp->imp_lock);

        RETURN(0);
}

/*
 * must be called under imp_lock
 */
int ptlrpc_first_transno(struct obd_import *imp, __u64 *transno)
{
        struct ptlrpc_request *req;
        struct list_head *tmp;
        
        if (list_empty(&imp->imp_replay_list))
                return 0;
        tmp = imp->imp_replay_list.next;
        req = list_entry(tmp, struct ptlrpc_request, rq_replay_list);
        *transno = req->rq_transno;
        return 1;
}

int ptlrpc_connect_import(struct obd_import *imp, char *new_uuid)
{
        struct obd_device *obd = imp->imp_obd;
        int initial_connect = 0;
        int set_transno = 0;
        int rc;
        __u64 committed_before_reconnect = 0;
        struct ptlrpc_request *request;
        int size[] = { sizeof(struct ptlrpc_body),
                       sizeof(imp->imp_obd->u.cli.cl_target_uuid),
                       sizeof(obd->obd_uuid),
                       sizeof(imp->imp_dlm_handle),
                       sizeof(imp->imp_connect_data) };
        char *tmp[] = { NULL,
                        obd2cli_tgt(imp->imp_obd),
                        obd->obd_uuid.uuid,
                        (char *)&imp->imp_dlm_handle,
                        (char *)&imp->imp_connect_data };
        struct ptlrpc_connect_async_args *aa;
        ENTRY;

        spin_lock(&imp->imp_lock);
        if (imp->imp_state == LUSTRE_IMP_CLOSED) {
                spin_unlock(&imp->imp_lock);
                CERROR("can't connect to a closed import\n");
                RETURN(-EINVAL);
        } else if (imp->imp_state == LUSTRE_IMP_FULL) {
                spin_unlock(&imp->imp_lock);
                CERROR("already connected\n");
                RETURN(0);
        } else if (imp->imp_state == LUSTRE_IMP_CONNECTING) {
                spin_unlock(&imp->imp_lock);
                CERROR("already connecting\n");
                RETURN(-EALREADY);
        }

        IMPORT_SET_STATE_NOLOCK(imp, LUSTRE_IMP_CONNECTING);

        imp->imp_conn_cnt++;
        imp->imp_resend_replay = 0;

        if (!lustre_handle_is_used(&imp->imp_remote_handle))
                initial_connect = 1;
        else
                committed_before_reconnect = imp->imp_peer_committed_transno;

        set_transno = ptlrpc_first_transno(imp, &imp->imp_connect_data.ocd_transno);
        spin_unlock(&imp->imp_lock);

        if (new_uuid) {
                struct obd_uuid uuid;

                obd_str2uuid(&uuid, new_uuid);
                rc = import_set_conn_priority(imp, &uuid);
                if (rc)
                        GOTO(out, rc);
        }

        rc = import_select_connection(imp);
        if (rc)
                GOTO(out, rc);

        /* last in connection list */
        if (imp->imp_conn_current->oic_item.next == &imp->imp_conn_list) {
                if (imp->imp_initial_recov_bk && initial_connect) {
                        CDEBUG(D_HA, "Last connection attempt (%d) for %s\n",
                               imp->imp_conn_cnt, obd2cli_tgt(imp->imp_obd));
                        /* Don't retry if connect fails */
                        rc = 0;
                        obd_set_info_async(obd->obd_self_export,
                                           strlen(KEY_INIT_RECOV),
                                           KEY_INIT_RECOV,
                                           sizeof(rc), &rc, NULL);
                }
                if (imp->imp_recon_bk) {
                        CDEBUG(D_HA, "Last reconnection attempt (%d) for %s\n",
                               imp->imp_conn_cnt, obd2cli_tgt(imp->imp_obd));
                        spin_lock(&imp->imp_lock);
                        imp->imp_last_recon = 1;
                        spin_unlock(&imp->imp_lock);
                }
        }

        /* Reset connect flags to the originally requested flags, in case
         * the server is updated on-the-fly we will get the new features. */
        imp->imp_connect_data.ocd_connect_flags = imp->imp_connect_flags_orig;
        rc = obd_reconnect(imp->imp_obd->obd_self_export, obd,
                           &obd->obd_uuid, &imp->imp_connect_data);
        if (rc)
                GOTO(out, rc);

        request = ptlrpc_prep_req(imp, LUSTRE_OBD_VERSION, imp->imp_connect_op,
                                  5, size, tmp);
        if (!request)
                GOTO(out, rc = -ENOMEM);

#ifndef __KERNEL__
        lustre_msg_add_op_flags(request->rq_reqmsg, MSG_CONNECT_LIBCLIENT);
#endif
        lustre_msg_add_op_flags(request->rq_reqmsg, MSG_CONNECT_NEXT_VER);

        request->rq_send_state = LUSTRE_IMP_CONNECTING;
        /* Allow a slightly larger reply for future growth compatibility */
        size[REPLY_REC_OFF] = sizeof(struct obd_connect_data) +
                              16 * sizeof(__u64);
        ptlrpc_req_set_repsize(request, 2, size);
        request->rq_interpret_reply = ptlrpc_connect_interpret;

        CLASSERT(sizeof (*aa) <= sizeof (request->rq_async_args));
        aa = (struct ptlrpc_connect_async_args *)&request->rq_async_args;
        memset(aa, 0, sizeof *aa);

        aa->pcaa_peer_committed = committed_before_reconnect;
        aa->pcaa_initial_connect = initial_connect;

        if (aa->pcaa_initial_connect) {
                spin_lock(&imp->imp_lock);
                imp->imp_replayable = 1;
                spin_unlock(&imp->imp_lock);
                /* On an initial connect, we don't know which one of a
                   failover server pair is up.  Don't wait long. */
#ifdef CRAY_XT3
                request->rq_timeout = max((int)(obd_timeout / 2), 5);
#else
                request->rq_timeout = max((int)(obd_timeout / 20), 5);
#endif
                lustre_msg_add_op_flags(request->rq_reqmsg, 
                                        MSG_CONNECT_INITIAL);
        }

        if (set_transno)
                lustre_msg_add_op_flags(request->rq_reqmsg, 
                                        MSG_CONNECT_TRANSNO);

        DEBUG_REQ(D_RPCTRACE, request, "(re)connect request");
        ptlrpcd_add_req(request);
        rc = 0;
out:
        if (rc != 0) {
                IMPORT_SET_STATE(imp, LUSTRE_IMP_DISCON);
        }

        RETURN(rc);
}
EXPORT_SYMBOL(ptlrpc_connect_import);

static void ptlrpc_maybe_ping_import_soon(struct obd_import *imp)
{
#ifdef __KERNEL__
        struct obd_import_conn *imp_conn;
#endif
        int wake_pinger = 0;

        ENTRY;

        spin_lock(&imp->imp_lock);
        if (list_empty(&imp->imp_conn_list))
                GOTO(unlock, 0);

#ifdef __KERNEL__
        imp_conn = list_entry(imp->imp_conn_list.prev,
                              struct obd_import_conn,
                              oic_item);

        if (imp->imp_conn_current != imp_conn) {
                ptlrpc_ping_import_soon(imp);
                wake_pinger = 1;
        }
#else
        /* liblustre has no pinger thead, so we wakup pinger anyway */
        wake_pinger = 1;
#endif 

 unlock:
        spin_unlock(&imp->imp_lock);

        if (wake_pinger)
                ptlrpc_pinger_wake_up();

        EXIT;
}

static int ptlrpc_connect_interpret(struct ptlrpc_request *request,
                                    void * data, int rc)
{
        struct ptlrpc_connect_async_args *aa = data;
        struct obd_import *imp = request->rq_import;
        struct client_obd *cli = &imp->imp_obd->u.cli;
        struct lustre_handle old_hdl;
        int msg_flags;
        ENTRY;

        spin_lock(&imp->imp_lock);
        if (imp->imp_state == LUSTRE_IMP_CLOSED) {
                spin_unlock(&imp->imp_lock);
                RETURN(0);
        }
        spin_unlock(&imp->imp_lock);

        if (rc)
                GOTO(out, rc);

        LASSERT(imp->imp_conn_current);

        msg_flags = lustre_msg_get_op_flags(request->rq_repmsg);

        /* All imports are pingable */
        spin_lock(&imp->imp_lock);
        imp->imp_pingable = 1;

        if (aa->pcaa_initial_connect) {
                if (msg_flags & MSG_CONNECT_REPLAYABLE) {
                        imp->imp_replayable = 1;
                        spin_unlock(&imp->imp_lock);
                        CDEBUG(D_HA, "connected to replayable target: %s\n",
                               obd2cli_tgt(imp->imp_obd));
                } else {
                        imp->imp_replayable = 0;
                        spin_unlock(&imp->imp_lock);
                }

                if (msg_flags & MSG_CONNECT_NEXT_VER) {
                        imp->imp_msg_magic = LUSTRE_MSG_MAGIC_V2;
                        CDEBUG(D_RPCTRACE, "connect to %s with lustre_msg_v2\n",
                               obd2cli_tgt(imp->imp_obd));
                } else {
                        CDEBUG(D_RPCTRACE, "connect to %s with lustre_msg_v1\n",
                               obd2cli_tgt(imp->imp_obd));
                }

                imp->imp_remote_handle =
                                *lustre_msg_get_handle(request->rq_repmsg);

                IMPORT_SET_STATE(imp, LUSTRE_IMP_FULL);
                GOTO(finish, rc = 0);
        } else {
                spin_unlock(&imp->imp_lock);
        }

        /* Determine what recovery state to move the import to. */
        if (MSG_CONNECT_RECONNECT & msg_flags) {
                memset(&old_hdl, 0, sizeof(old_hdl));
                if (!memcmp(&old_hdl, lustre_msg_get_handle(request->rq_repmsg),
                            sizeof (old_hdl))) {
                        CERROR("%s@%s didn't like our handle "LPX64
                               ", failed\n", obd2cli_tgt(imp->imp_obd),
                               imp->imp_connection->c_remote_uuid.uuid,
                               imp->imp_dlm_handle.cookie);
                        GOTO(out, rc = -ENOTCONN);
                }

                if (memcmp(&imp->imp_remote_handle,
                           lustre_msg_get_handle(request->rq_repmsg),
                           sizeof(imp->imp_remote_handle))) {
                        int level = D_ERROR;
                        /* Old MGC can reconnect to a restarted MGS */
                        if (strcmp(imp->imp_obd->obd_type->typ_name,
                                   LUSTRE_MGC_NAME) == 0) {
                                level = D_CONFIG;
                        }
                        CDEBUG(level, 
                               "%s@%s changed handle from "LPX64" to "LPX64
                               "; copying, but this may foreshadow disaster\n",
                               obd2cli_tgt(imp->imp_obd),
                               imp->imp_connection->c_remote_uuid.uuid,
                               imp->imp_remote_handle.cookie,
                               lustre_msg_get_handle(request->rq_repmsg)->
                                        cookie);
                        imp->imp_remote_handle =
                                     *lustre_msg_get_handle(request->rq_repmsg);
                } else {
                        CDEBUG(D_HA, "reconnected to %s@%s after partition\n",
                               obd2cli_tgt(imp->imp_obd),
                               imp->imp_connection->c_remote_uuid.uuid);
                }

                if (imp->imp_invalid) {
                        IMPORT_SET_STATE(imp, LUSTRE_IMP_EVICTED);
                } else if (MSG_CONNECT_RECOVERING & msg_flags) {
                        CDEBUG(D_HA, "%s: reconnected to %s during replay\n",
                               imp->imp_obd->obd_name,
                               obd2cli_tgt(imp->imp_obd));

                        spin_lock(&imp->imp_lock);
                        imp->imp_resend_replay = 1;
                        spin_unlock(&imp->imp_lock);

                        IMPORT_SET_STATE(imp, LUSTRE_IMP_REPLAY);
                } else {
                        IMPORT_SET_STATE(imp, LUSTRE_IMP_RECOVER);
                }
        } else if ((MSG_CONNECT_RECOVERING & msg_flags) && !imp->imp_invalid) {
                LASSERT(imp->imp_replayable);
                imp->imp_remote_handle =
                                *lustre_msg_get_handle(request->rq_repmsg);
                imp->imp_last_replay_transno = 0;
                IMPORT_SET_STATE(imp, LUSTRE_IMP_REPLAY);
        } else {
                imp->imp_remote_handle =
                                *lustre_msg_get_handle(request->rq_repmsg);
                IMPORT_SET_STATE(imp, LUSTRE_IMP_EVICTED);
        }

        /* Sanity checks for a reconnected import. */
        if (!(imp->imp_replayable) != !(msg_flags & MSG_CONNECT_REPLAYABLE)) {
                CERROR("imp_replayable flag does not match server "
                       "after reconnect. We should LBUG right here.\n");
        }

        if (lustre_msg_get_last_committed(request->rq_repmsg) <
            aa->pcaa_peer_committed) {
                CERROR("%s went back in time (transno "LPD64
                       " was previously committed, server now claims "LPD64
                       ")!  See https://bugzilla.clusterfs.com/"
                       "long_list.cgi?buglist=9646\n",
                       obd2cli_tgt(imp->imp_obd), aa->pcaa_peer_committed,
                       lustre_msg_get_last_committed(request->rq_repmsg));
        }

finish:
        rc = ptlrpc_import_recovery_state_machine(imp);
        if (rc != 0) {
                if (rc == -ENOTCONN) {
                        CDEBUG(D_HA, "evicted/aborted by %s@%s during recovery;"
                               "invalidating and reconnecting\n",
                               obd2cli_tgt(imp->imp_obd),
                               imp->imp_connection->c_remote_uuid.uuid);
                        ptlrpc_connect_import(imp, NULL);
                        RETURN(0);
                }
        } else {
                struct obd_connect_data *ocd;
                struct obd_export *exp;

                ocd = lustre_swab_repbuf(request, REPLY_REC_OFF, sizeof(*ocd),
                                         lustre_swab_connect);

                spin_lock(&imp->imp_lock);
                list_del(&imp->imp_conn_current->oic_item);
                list_add(&imp->imp_conn_current->oic_item, &imp->imp_conn_list);
                imp->imp_last_success_conn =
                        imp->imp_conn_current->oic_last_attempt;

                if (ocd == NULL) {
                        spin_unlock(&imp->imp_lock);
                        CERROR("Wrong connect data from server\n");
                        rc = -EPROTO;
                        GOTO(out, rc);
                }

                imp->imp_connect_data = *ocd;

                exp = class_conn2export(&imp->imp_dlm_handle);
                spin_unlock(&imp->imp_lock);

                /* check that server granted subset of flags we asked for. */
                LASSERTF((ocd->ocd_connect_flags &
                          imp->imp_connect_flags_orig) ==
                         ocd->ocd_connect_flags, LPX64" != "LPX64,
                         imp->imp_connect_flags_orig, ocd->ocd_connect_flags);

                if (!exp) {
                        /* This could happen if export is cleaned during the 
                           connect attempt */
                        CERROR("Missing export for %s\n", 
                               imp->imp_obd->obd_name);
                        GOTO(out, rc = -ENODEV);
                }
                exp->exp_connect_flags = ocd->ocd_connect_flags;
                imp->imp_obd->obd_self_export->exp_connect_flags = ocd->ocd_connect_flags;
                class_export_put(exp);

                obd_import_event(imp->imp_obd, imp, IMP_EVENT_OCD);

                if (!ocd->ocd_ibits_known &&
                    ocd->ocd_connect_flags & OBD_CONNECT_IBITS)
                        CERROR("Inodebits aware server returned zero compatible"
                               " bits?\n");

                if ((ocd->ocd_connect_flags & OBD_CONNECT_VERSION) &&
                    (ocd->ocd_version > LUSTRE_VERSION_CODE +
                                        LUSTRE_VERSION_OFFSET_WARN ||
                     ocd->ocd_version < LUSTRE_VERSION_CODE -
                                        LUSTRE_VERSION_OFFSET_WARN)) {
                        /* Sigh, some compilers do not like #ifdef in the middle
                           of macro arguments */
#ifdef __KERNEL__
                        const char *older =
                                "older. Consider upgrading this client";
#else
                        const char *older =
                                "older. Consider recompiling this application";
#endif
                        const char *newer = "newer than client version";

                        LCONSOLE_WARN("Server %s version (%d.%d.%d.%d) "
                                      "is much %s (%s)\n",
                                      obd2cli_tgt(imp->imp_obd),
                                      OBD_OCD_VERSION_MAJOR(ocd->ocd_version),
                                      OBD_OCD_VERSION_MINOR(ocd->ocd_version),
                                      OBD_OCD_VERSION_PATCH(ocd->ocd_version),
                                      OBD_OCD_VERSION_FIX(ocd->ocd_version),
                                      ocd->ocd_version > LUSTRE_VERSION_CODE ?
                                      newer : older, LUSTRE_VERSION_STRING);
                }

                if (ocd->ocd_connect_flags & OBD_CONNECT_BRW_SIZE) {
                        cli->cl_max_pages_per_rpc = 
                                ocd->ocd_brw_size >> CFS_PAGE_SHIFT;
                }

                imp->imp_obd->obd_namespace->ns_connect_flags = ocd->ocd_connect_flags;

                LASSERT((cli->cl_max_pages_per_rpc <= PTLRPC_MAX_BRW_PAGES) &&
                        (cli->cl_max_pages_per_rpc > 0));
        }

out:
        if (rc != 0) {
                IMPORT_SET_STATE(imp, LUSTRE_IMP_DISCON);
                if (aa->pcaa_initial_connect && !imp->imp_initial_recov)
                        ptlrpc_deactivate_import(imp);

                if ((imp->imp_recon_bk && imp->imp_last_recon) ||
                    (rc == -EACCES)) {
                        /*
                         * Give up trying to reconnect
                         * EACCES means client has no permission for connection
                         */
                        imp->imp_obd->obd_no_recov = 1;
                        ptlrpc_deactivate_import(imp);
                }

                if (rc == -EPROTO) {
                        struct obd_connect_data *ocd;

                        /* reply message might not be ready */
                        if (request->rq_repmsg != NULL)
                                RETURN(-EPROTO);

                        ocd = lustre_swab_repbuf(request, REPLY_REC_OFF,
                                                 sizeof *ocd,
                                                 lustre_swab_connect);
                        if (ocd &&
                            (ocd->ocd_connect_flags & OBD_CONNECT_VERSION) &&
                            (ocd->ocd_version != LUSTRE_VERSION_CODE)) {
                           /* Actually servers are only supposed to refuse
                              connection from liblustre clients, so we should
                              never see this from VFS context */
                                LCONSOLE_ERROR_MSG(0x16a, "Server %s version "
                                        "(%d.%d.%d.%d)"
                                        " refused connection from this client "
                                        "with an incompatible version (%s).  "
                                        "Client must be recompiled\n",
                                        obd2cli_tgt(imp->imp_obd),
                                        OBD_OCD_VERSION_MAJOR(ocd->ocd_version),
                                        OBD_OCD_VERSION_MINOR(ocd->ocd_version),
                                        OBD_OCD_VERSION_PATCH(ocd->ocd_version),
                                        OBD_OCD_VERSION_FIX(ocd->ocd_version),
                                        LUSTRE_VERSION_STRING);
                                ptlrpc_deactivate_import(imp);
                                IMPORT_SET_STATE(imp, LUSTRE_IMP_CLOSED);
                        }
                        RETURN(-EPROTO);
                }

                ptlrpc_maybe_ping_import_soon(imp);

                CDEBUG(D_HA, "recovery of %s on %s failed (%d)\n",
                       obd2cli_tgt(imp->imp_obd),
                       (char *)imp->imp_connection->c_remote_uuid.uuid, rc);
        }
        
        spin_lock(&imp->imp_lock);
        imp->imp_last_recon = 0;
        spin_unlock(&imp->imp_lock);

        cfs_waitq_signal(&imp->imp_recovery_waitq);
        RETURN(rc);
}

static int completed_replay_interpret(struct ptlrpc_request *req,
                                    void * data, int rc)
{
        ENTRY;
        atomic_dec(&req->rq_import->imp_replay_inflight);
        if (req->rq_status == 0) {
                ptlrpc_import_recovery_state_machine(req->rq_import);
        } else {
                CDEBUG(D_HA, "%s: LAST_REPLAY message error: %d, "
                       "reconnecting\n",
                       req->rq_import->imp_obd->obd_name, req->rq_status);
                ptlrpc_connect_import(req->rq_import, NULL);
        }

        RETURN(0);
}

static int signal_completed_replay(struct obd_import *imp)
{
        struct ptlrpc_request *req;
        ENTRY;

        LASSERT(atomic_read(&imp->imp_replay_inflight) == 0);
        atomic_inc(&imp->imp_replay_inflight);

        req = ptlrpc_prep_req(imp, LUSTRE_OBD_VERSION, OBD_PING, 1, NULL, NULL);
        if (!req) {
                atomic_dec(&imp->imp_replay_inflight);
                RETURN(-ENOMEM);
        }

        ptlrpc_req_set_repsize(req, 1, NULL);
        req->rq_send_state = LUSTRE_IMP_REPLAY_WAIT;
        lustre_msg_add_flags(req->rq_reqmsg, 
                             MSG_LOCK_REPLAY_DONE | MSG_REQ_REPLAY_DONE);
        req->rq_timeout *= 3;
        req->rq_interpret_reply = completed_replay_interpret;

        ptlrpcd_add_req(req);
        RETURN(0);
}

#ifdef __KERNEL__
static int ptlrpc_invalidate_import_thread(void *data)
{
        struct obd_import *imp = data;

        ENTRY;

        ptlrpc_daemonize("ll_imp_inval");
        
        CDEBUG(D_HA, "thread invalidate import %s to %s@%s\n",
               imp->imp_obd->obd_name, obd2cli_tgt(imp->imp_obd),
               imp->imp_connection->c_remote_uuid.uuid);

        ptlrpc_invalidate_import(imp);

        if (obd_dump_on_eviction) {
                CERROR("dump the log upon eviction\n");
                libcfs_debug_dumplog();
        }

        IMPORT_SET_STATE(imp, LUSTRE_IMP_RECOVER);
        ptlrpc_import_recovery_state_machine(imp);

        RETURN(0);
}
#endif

int ptlrpc_import_recovery_state_machine(struct obd_import *imp)
{
        int rc = 0;
        int inflight;
        char *target_start;
        int target_len;

        ENTRY;
        if (imp->imp_state == LUSTRE_IMP_EVICTED) {
                deuuidify(obd2cli_tgt(imp->imp_obd), NULL,
                          &target_start, &target_len);
                /* Don't care about MGC eviction */
                if (strcmp(imp->imp_obd->obd_type->typ_name,
                           LUSTRE_MGC_NAME) != 0) {
                        LCONSOLE_ERROR_MSG(0x167, "This client was evicted by "
                                           "%.*s; in progress operations using "
                                           "this service will fail.\n",
                                           target_len, target_start);
                }
                CDEBUG(D_HA, "evicted from %s@%s; invalidating\n",
                       obd2cli_tgt(imp->imp_obd),
                       imp->imp_connection->c_remote_uuid.uuid);

#ifdef __KERNEL__
                rc = cfs_kernel_thread(ptlrpc_invalidate_import_thread, imp,
                                       CLONE_VM | CLONE_FILES);
                if (rc < 0)
                        CERROR("error starting invalidate thread: %d\n", rc);
                else
                        rc = 0;
                RETURN(rc);
#else
                ptlrpc_invalidate_import(imp);

                IMPORT_SET_STATE(imp, LUSTRE_IMP_RECOVER);
#endif
        }

        if (imp->imp_state == LUSTRE_IMP_REPLAY) {
                CDEBUG(D_HA, "replay requested by %s\n",
                       obd2cli_tgt(imp->imp_obd));
                rc = ptlrpc_replay_next(imp, &inflight);
                if (inflight == 0 &&
                    atomic_read(&imp->imp_replay_inflight) == 0) {
                        IMPORT_SET_STATE(imp, LUSTRE_IMP_REPLAY_LOCKS);
                        rc = ldlm_replay_locks(imp);
                        if (rc)
                                GOTO(out, rc);
                }
                rc = 0;
        }

        if (imp->imp_state == LUSTRE_IMP_REPLAY_LOCKS) {
                if (atomic_read(&imp->imp_replay_inflight) == 0) {
                        IMPORT_SET_STATE(imp, LUSTRE_IMP_REPLAY_WAIT);
                        rc = signal_completed_replay(imp);
                        if (rc)
                                GOTO(out, rc);
                }

        }

        if (imp->imp_state == LUSTRE_IMP_REPLAY_WAIT) {
                if (atomic_read(&imp->imp_replay_inflight) == 0) {
                        IMPORT_SET_STATE(imp, LUSTRE_IMP_RECOVER);
                }
        }

        if (imp->imp_state == LUSTRE_IMP_RECOVER) {
                CDEBUG(D_HA, "reconnected to %s@%s\n",
                       obd2cli_tgt(imp->imp_obd),
                       imp->imp_connection->c_remote_uuid.uuid);

                rc = ptlrpc_resend(imp);
                if (rc)
                        GOTO(out, rc);
                IMPORT_SET_STATE(imp, LUSTRE_IMP_FULL);
                ptlrpc_activate_import(imp);

                deuuidify(obd2cli_tgt(imp->imp_obd), NULL,
                          &target_start, &target_len);
                LCONSOLE_INFO("%s: Connection restored to service %.*s "
                              "using nid %s.\n", imp->imp_obd->obd_name,
                              target_len, target_start,
                              libcfs_nid2str(imp->imp_connection->c_peer.nid));
        }

        if (imp->imp_state == LUSTRE_IMP_FULL) {
                cfs_waitq_signal(&imp->imp_recovery_waitq);
                ptlrpc_wake_delayed(imp);
        }

out:
        RETURN(rc);
}

static int back_to_sleep(void *unused)
{
        return 0;
}

int ptlrpc_disconnect_import(struct obd_import *imp, int noclose)
{
        struct ptlrpc_request *req;
        int rq_opc, rc = 0;
        ENTRY;

        switch (imp->imp_connect_op) {
        case OST_CONNECT: rq_opc = OST_DISCONNECT; break;
        case MDS_CONNECT: rq_opc = MDS_DISCONNECT; break;
        case MGS_CONNECT: rq_opc = MGS_DISCONNECT; break;
        default:
                CERROR("don't know how to disconnect from %s (connect_op %d)\n",
                       obd2cli_tgt(imp->imp_obd), imp->imp_connect_op);
                RETURN(-EINVAL);
        }

        if (ptlrpc_import_in_recovery(imp)) {
                struct l_wait_info lwi;
                cfs_duration_t timeout;
                if (imp->imp_server_timeout)
                        timeout = cfs_time_seconds(obd_timeout / 2);
                else
                        timeout = cfs_time_seconds(obd_timeout);
                
                timeout = MAX(timeout * HZ, 1);
                
                lwi = LWI_TIMEOUT_INTR(cfs_timeout_cap(timeout), 
                                       back_to_sleep, LWI_ON_SIGNAL_NOOP, NULL);
                rc = l_wait_event(imp->imp_recovery_waitq,
                                  !ptlrpc_import_in_recovery(imp), &lwi);

        }

        spin_lock(&imp->imp_lock);
        if (imp->imp_state != LUSTRE_IMP_FULL)
                GOTO(out, 0);

        spin_unlock(&imp->imp_lock);

        req = ptlrpc_prep_req(imp, LUSTRE_OBD_VERSION, rq_opc, 1, NULL, NULL);
        if (req) {
                /* We are disconnecting, do not retry a failed DISCONNECT rpc if
                 * it fails.  We can get through the above with a down server
                 * if the client doesn't know the server is gone yet. */
                req->rq_no_resend = 1;
#ifdef CRAY_XT3
                req->rq_timeout = obd_timeout / 3;
#else
                req->rq_timeout = 5;
#endif
                IMPORT_SET_STATE(imp, LUSTRE_IMP_CONNECTING);
                req->rq_send_state =  LUSTRE_IMP_CONNECTING;
                ptlrpc_req_set_repsize(req, 1, NULL);
                rc = ptlrpc_queue_wait(req);
                ptlrpc_req_finished(req);
        }

        spin_lock(&imp->imp_lock);
out:
        if (noclose) 
                IMPORT_SET_STATE_NOLOCK(imp, LUSTRE_IMP_DISCON);
        else
                IMPORT_SET_STATE_NOLOCK(imp, LUSTRE_IMP_CLOSED);
        memset(&imp->imp_remote_handle, 0, sizeof(imp->imp_remote_handle));
        imp->imp_conn_cnt = 0;
        imp->imp_last_recon = 0;
        spin_unlock(&imp->imp_lock);

        RETURN(rc);
}

