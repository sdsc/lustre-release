/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2001-2004 Cluster File Systems, Inc.
 *   Author: Andreas Dilger <adilger@clusterfs.com>
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
 *
 *  remote api for llog - client side
 *
 */

#define DEBUG_SUBSYSTEM S_LOG

#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif

#ifdef __KERNEL__
#include <libcfs/libcfs.h>
#else
#include <liblustre.h>
#endif

#include <obd_class.h>
#include <lustre_log.h>
#include <lustre_net.h>
#include <libcfs/list.h>

/* This is a callback from the llog_* functions.
 * Assumes caller has already pushed us into the kernel context. */
static int llog_client_create(struct llog_ctxt *ctxt, struct llog_handle **res,
                              struct llog_logid *logid, char *name)
{
        struct obd_import *imp;
        struct llogd_body req_body;
        struct llogd_body *body;
        struct llog_handle *handle;
        struct ptlrpc_request *req = NULL;
        int size[3] = { sizeof(struct ptlrpc_body), sizeof(req_body) };
        char *bufs[3] = { NULL, (char*)&req_body };
        int bufcount = 2;
        int rc;
        ENTRY;

        if (ctxt->loc_imp == NULL) {
                /* This used to be an assert; bug 6200 */
                CERROR("ctxt->loc_imp == NULL for context idx %d.  Unable to "
                       "complete MDS/OSS recovery, but I'll try again next "
                       "time.  Not fatal.\n", ctxt->loc_idx);
                RETURN(-EINVAL);
        }
        imp = ctxt->loc_imp;

        handle = llog_alloc_handle();
        if (handle == NULL)
                RETURN(-ENOMEM);
        *res = handle;

        memset(&req_body, 0, sizeof(req_body));
        if (logid)
                req_body.lgd_logid = *logid;
        req_body.lgd_ctxt_idx = ctxt->loc_idx - 1;

        if (name) {
                size[bufcount] = strlen(name) + 1;
                bufs[bufcount] = name;
                bufcount++;
        }

        req = ptlrpc_prep_req(imp, LUSTRE_LOG_VERSION,
                              LLOG_ORIGIN_HANDLE_CREATE, bufcount, size, bufs);
        if (!req)
                GOTO(err_free, rc = -ENOMEM);

        ptlrpc_req_set_repsize(req, 2, size);
        rc = ptlrpc_queue_wait(req);
        if (rc)
                GOTO(err_free, rc);

        body = lustre_swab_repbuf(req, REPLY_REC_OFF, sizeof(*body),
                                 lustre_swab_llogd_body);
        if (body == NULL) {
                CERROR ("Can't unpack llogd_body\n");
                GOTO(err_free, rc =-EFAULT);
        }

        handle->lgh_id = body->lgd_logid;
        handle->lgh_ctxt = ctxt;

out:
        if (req)
                ptlrpc_req_finished(req);
        RETURN(rc);

err_free:
        llog_free_handle(handle);
        goto out;
}

static int llog_client_destroy(struct llog_handle *loghandle)
{
        struct obd_import *imp = loghandle->lgh_ctxt->loc_imp;
        struct ptlrpc_request *req = NULL;
        struct llogd_body *body;
        int size[] = { sizeof(struct ptlrpc_body), sizeof(*body) };
        int rc;
        ENTRY;

        req = ptlrpc_prep_req(imp, LUSTRE_LOG_VERSION, 
                              LLOG_ORIGIN_HANDLE_DESTROY, 2, size, NULL);
        if (!req)
                RETURN(-ENOMEM);

        body = lustre_msg_buf(req->rq_reqmsg, REQ_REC_OFF, sizeof(*body));
        body->lgd_logid = loghandle->lgh_id;
        body->lgd_llh_flags = loghandle->lgh_hdr->llh_flags;

        ptlrpc_req_set_repsize(req, 2, size);
        rc = ptlrpc_queue_wait(req);
        
        ptlrpc_req_finished(req);
        RETURN(rc);
}


static int llog_client_next_block(struct llog_handle *loghandle,
                                  int *cur_idx, int next_idx,
                                  __u64 *cur_offset, void *buf, int len)
{
        struct obd_import *imp = loghandle->lgh_ctxt->loc_imp;
        struct ptlrpc_request *req = NULL;
        struct llogd_body *body;
        void * ptr;
        int size[3] = { sizeof(struct ptlrpc_body), sizeof(*body) };
        int rc;
        ENTRY;

        req = ptlrpc_prep_req(imp, LUSTRE_LOG_VERSION,
                              LLOG_ORIGIN_HANDLE_NEXT_BLOCK, 2, size, NULL);
        if (!req)
                GOTO(out, rc = -ENOMEM);

        body = lustre_msg_buf(req->rq_reqmsg, REQ_REC_OFF, sizeof(*body));
        body->lgd_logid = loghandle->lgh_id;
        body->lgd_ctxt_idx = loghandle->lgh_ctxt->loc_idx - 1;
        body->lgd_llh_flags = loghandle->lgh_hdr->llh_flags;
        body->lgd_index = next_idx;
        body->lgd_saved_index = *cur_idx;
        body->lgd_len = len;
        body->lgd_cur_offset = *cur_offset;

        size[REPLY_REC_OFF + 1] = len;
        ptlrpc_req_set_repsize(req, 3, size);
        rc = ptlrpc_queue_wait(req);
        if (rc)
                GOTO(out, rc);

        body = lustre_swab_repbuf(req, REPLY_REC_OFF, sizeof(*body),
                                 lustre_swab_llogd_body);
        if (body == NULL) {
                CERROR ("Can't unpack llogd_body\n");
                GOTO(out, rc =-EFAULT);
        }

        /* The log records are swabbed as they are processed */
        ptr = lustre_msg_buf(req->rq_repmsg, REPLY_REC_OFF + 1, len);
        if (ptr == NULL) {
                CERROR ("Can't unpack bitmap\n");
                GOTO(out, rc =-EFAULT);
        }

        *cur_idx = body->lgd_saved_index;
        *cur_offset = body->lgd_cur_offset;

        memcpy(buf, ptr, len);

out:
        if (req)
                ptlrpc_req_finished(req);
        RETURN(rc);
}

static int llog_client_prev_block(struct llog_handle *loghandle,
                                  int prev_idx, void *buf, int len)
{
        struct obd_import *imp = loghandle->lgh_ctxt->loc_imp;
        struct ptlrpc_request *req = NULL;
        struct llogd_body *body;
        void * ptr;
        int size[3] = { sizeof(struct ptlrpc_body), sizeof(*body) };
        int rc;
        ENTRY;

        req = ptlrpc_prep_req(imp, LUSTRE_LOG_VERSION,
                              LLOG_ORIGIN_HANDLE_PREV_BLOCK, 2, size, NULL);
        if (!req)
                GOTO(out, rc = -ENOMEM);

        body = lustre_msg_buf(req->rq_reqmsg, REQ_REC_OFF, sizeof(*body));
        body->lgd_logid = loghandle->lgh_id;
        body->lgd_ctxt_idx = loghandle->lgh_ctxt->loc_idx - 1;
        body->lgd_llh_flags = loghandle->lgh_hdr->llh_flags;
        body->lgd_index = prev_idx;
        body->lgd_len = len;

        size[REPLY_REC_OFF + 1] = len;
        ptlrpc_req_set_repsize(req, 3, size);
        rc = ptlrpc_queue_wait(req);
        if (rc)
                GOTO(out, rc);

        body = lustre_swab_repbuf(req, REPLY_REC_OFF, sizeof(*body),
                                 lustre_swab_llogd_body);
        if (body == NULL) {
                CERROR ("Can't unpack llogd_body\n");
                GOTO(out, rc =-EFAULT);
        }

        ptr = lustre_msg_buf(req->rq_repmsg, REPLY_REC_OFF + 1, len);
        if (ptr == NULL) {
                CERROR ("Can't unpack bitmap\n");
                GOTO(out, rc =-EFAULT);
        }

        memcpy(buf, ptr, len);

out:
        if (req)
                ptlrpc_req_finished(req);
        RETURN(rc);
}

static int llog_client_read_header(struct llog_handle *handle)
{
        struct obd_import *imp = handle->lgh_ctxt->loc_imp;
        struct ptlrpc_request *req = NULL;
        struct llogd_body *body;
        struct llog_log_hdr *hdr;
        struct llog_rec_hdr *llh_hdr;
        int size[2] = { sizeof(struct ptlrpc_body), sizeof(*body) };
        int repsize[2] = { sizeof(struct ptlrpc_body), sizeof(*hdr) };
        int rc;
        ENTRY;

        req = ptlrpc_prep_req(imp, LUSTRE_LOG_VERSION,
                              LLOG_ORIGIN_HANDLE_READ_HEADER, 2, size, NULL);
        if (!req)
                GOTO(out, rc = -ENOMEM);

        body = lustre_msg_buf(req->rq_reqmsg, REQ_REC_OFF, sizeof(*body));
        body->lgd_logid = handle->lgh_id;
        body->lgd_ctxt_idx = handle->lgh_ctxt->loc_idx - 1;
        body->lgd_llh_flags = handle->lgh_hdr->llh_flags;

        ptlrpc_req_set_repsize(req, 2, repsize);
        rc = ptlrpc_queue_wait(req);
        if (rc)
                GOTO(out, rc);

        hdr = lustre_swab_repbuf(req, REPLY_REC_OFF, sizeof(*hdr),
                                 lustre_swab_llog_hdr);
        if (hdr == NULL) {
                CERROR ("Can't unpack llog_hdr\n");
                GOTO(out, rc =-EFAULT);
        }

        memcpy(handle->lgh_hdr, hdr, sizeof (*hdr));
        handle->lgh_last_idx = handle->lgh_hdr->llh_tail.lrt_index;

        /* sanity checks */
        llh_hdr = &handle->lgh_hdr->llh_hdr;
        if (llh_hdr->lrh_type != LLOG_HDR_MAGIC) {
                CERROR("bad log header magic: %#x (expecting %#x)\n",
                       llh_hdr->lrh_type, LLOG_HDR_MAGIC);
                rc = -EIO;
        } else if (llh_hdr->lrh_len != LLOG_CHUNK_SIZE) {
                CERROR("incorrectly sized log header: %#x "
                       "(expecting %#x)\n",
                       llh_hdr->lrh_len, LLOG_CHUNK_SIZE);
                CERROR("you may need to re-run lconf --write_conf.\n");
                rc = -EIO;
        }

out:
        if (req)
                ptlrpc_req_finished(req);
        RETURN(rc);
}

static int llog_client_close(struct llog_handle *handle)
{
        /* this doesn't call LLOG_ORIGIN_HANDLE_CLOSE because
           the servers all close the file at the end of every
           other LLOG_ RPC. */
        return(0);
}


struct llog_operations llog_client_ops = {
        lop_next_block:  llog_client_next_block,
        lop_prev_block:  llog_client_prev_block,
        lop_read_header: llog_client_read_header,
        lop_create:      llog_client_create,
        lop_destroy:     llog_client_destroy,
        lop_close:       llog_client_close,
};
