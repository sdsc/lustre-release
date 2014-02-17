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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdt/mdt_recovery.c
 *
 * Lustre Metadata Target (mdt) recovery-related methods
 *
 * Author: Huang Hua <huanghua@clusterfs.com>
 * Author: Pershin Mike <tappro@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include "mdt_internal.h"

struct lu_buf *mdt_buf(const struct lu_env *env, void *area, ssize_t len)
{
        struct lu_buf *buf;
        struct mdt_thread_info *mti;

        mti = lu_context_key_get(&env->le_ctx, &mdt_thread_key);
        buf = &mti->mti_buf;
        buf->lb_buf = area;
        buf->lb_len = len;
        return buf;
}

const struct lu_buf *mdt_buf_const(const struct lu_env *env,
                                   const void *area, ssize_t len)
{
        struct lu_buf *buf;
        struct mdt_thread_info *mti;

        mti = lu_context_key_get(&env->le_ctx, &mdt_thread_key);
        buf = &mti->mti_buf;

        buf->lb_buf = (void *)area;
        buf->lb_len = len;
        return buf;
}

void mdt_trans_stop(const struct lu_env *env,
                    struct mdt_device *mdt, struct thandle *th)
{
        dt_trans_stop(env, mdt->mdt_bottom, th);
}

/*
 * last_rcvd & last_committed update callbacks
 */
extern struct lu_context_key mdt_thread_key;

/* This callback notifies MDT that transaction was done. This is needed by
 * mdt_save_lock() only. It is similar to new target code and will be removed
 * as mdt_save_lock() will be converted to use target structures */
static int mdt_txn_stop_cb(const struct lu_env *env,
                           struct thandle *txn, void *cookie)
{
	struct mdt_thread_info	*mti;

	mti = lu_context_key_get(&env->le_ctx, &mdt_thread_key);
	LASSERT(mti);

	if (mti->mti_has_trans)
		CDEBUG(D_INFO, "More than one transaction\n");
	else
		mti->mti_has_trans = 1;
	return 0;
}

int mdt_fs_setup(const struct lu_env *env, struct mdt_device *mdt,
		 struct obd_device *obd, struct lustre_sb_info *lsi)
{
	int rc = 0;

	ENTRY;

	if (OBD_FAIL_CHECK(OBD_FAIL_MDS_FS_SETUP))
		RETURN(-ENOENT);

	/* prepare transactions callbacks */
	mdt->mdt_txn_cb.dtc_txn_start = NULL;
	mdt->mdt_txn_cb.dtc_txn_stop = mdt_txn_stop_cb;
	mdt->mdt_txn_cb.dtc_txn_commit = NULL;
	mdt->mdt_txn_cb.dtc_cookie = NULL;
	mdt->mdt_txn_cb.dtc_tag = LCT_MD_THREAD;
	CFS_INIT_LIST_HEAD(&mdt->mdt_txn_cb.dtc_linkage);

	dt_txn_callback_add(mdt->mdt_bottom, &mdt->mdt_txn_cb);

	RETURN(rc);
}

void mdt_fs_cleanup(const struct lu_env *env, struct mdt_device *mdt)
{
        ENTRY;

        /* Remove transaction callback */
        dt_txn_callback_del(mdt->mdt_bottom, &mdt->mdt_txn_cb);
        if (mdt->mdt_ck_obj)
                lu_object_put(env, &mdt->mdt_ck_obj->do_lu);
        mdt->mdt_ck_obj = NULL;
        EXIT;
}

/* reconstruction code */
static void mdt_steal_ack_locks(struct ptlrpc_request *req)
{
	struct ptlrpc_service_part *svcpt;
        struct obd_export         *exp = req->rq_export;
        cfs_list_t                *tmp;
        struct ptlrpc_reply_state *oldrep;
        int                        i;

        /* CAVEAT EMPTOR: spinlock order */
	spin_lock(&exp->exp_lock);
        cfs_list_for_each (tmp, &exp->exp_outstanding_replies) {
                oldrep = cfs_list_entry(tmp, struct ptlrpc_reply_state,
                                        rs_exp_list);

                if (oldrep->rs_xid != req->rq_xid)
                        continue;

                if (oldrep->rs_opc != lustre_msg_get_opc(req->rq_reqmsg))
                        CERROR ("Resent req xid "LPU64" has mismatched opc: "
                                "new %d old %d\n", req->rq_xid,
                                lustre_msg_get_opc(req->rq_reqmsg),
                                oldrep->rs_opc);

		svcpt = oldrep->rs_svcpt;
		spin_lock(&svcpt->scp_rep_lock);

                cfs_list_del_init (&oldrep->rs_exp_list);

		CDEBUG(D_HA, "Stealing %d locks from rs %p x"LPD64".t"LPD64
		       " o%d NID %s\n",
		       oldrep->rs_nlocks, oldrep,
		       oldrep->rs_xid, oldrep->rs_transno, oldrep->rs_opc,
		       libcfs_nid2str(exp->exp_connection->c_peer.nid));

                for (i = 0; i < oldrep->rs_nlocks; i++)
                        ptlrpc_save_lock(req, &oldrep->rs_locks[i],
                                         oldrep->rs_modes[i], 0);
                oldrep->rs_nlocks = 0;

                DEBUG_REQ(D_HA, req, "stole locks for");
		spin_lock(&oldrep->rs_lock);
		ptlrpc_schedule_difficult_reply(oldrep);
		spin_unlock(&oldrep->rs_lock);

		spin_unlock(&svcpt->scp_rep_lock);
		break;
	}
	spin_unlock(&exp->exp_lock);
}

/**
 * VBR: restore versions
 */
void mdt_vbr_reconstruct(struct ptlrpc_request *req,
                         struct lsd_client_data *lcd)
{
        __u64 pre_versions[4] = {0};
        pre_versions[0] = lcd->lcd_pre_versions[0];
        pre_versions[1] = lcd->lcd_pre_versions[1];
        pre_versions[2] = lcd->lcd_pre_versions[2];
        pre_versions[3] = lcd->lcd_pre_versions[3];
        lustre_msg_set_versions(req->rq_repmsg, pre_versions);
}

void mdt_req_from_lcd(struct ptlrpc_request *req,
                      struct lsd_client_data *lcd)
{
        DEBUG_REQ(D_HA, req, "restoring transno "LPD64"/status %d",
                  lcd->lcd_last_transno, lcd->lcd_last_result);

        if (lustre_msg_get_opc(req->rq_reqmsg) == MDS_CLOSE ||
            lustre_msg_get_opc(req->rq_repmsg) == MDS_DONE_WRITING) {
                req->rq_transno = lcd->lcd_last_close_transno;
                req->rq_status = lcd->lcd_last_close_result;
        } else {
                req->rq_transno = lcd->lcd_last_transno;
                req->rq_status = lcd->lcd_last_result;
                mdt_vbr_reconstruct(req, lcd);
        }
        if (req->rq_status != 0)
                req->rq_transno = 0;
        lustre_msg_set_transno(req->rq_repmsg, req->rq_transno);
        lustre_msg_set_status(req->rq_repmsg, req->rq_status);
        DEBUG_REQ(D_RPCTRACE, req, "restoring transno "LPD64"/status %d",
                  req->rq_transno, req->rq_status);

        mdt_steal_ack_locks(req);
}

void mdt_reconstruct_generic(struct mdt_thread_info *mti,
                             struct mdt_lock_handle *lhc)
{
        struct ptlrpc_request *req = mdt_info_req(mti);
        struct tg_export_data *ted = &req->rq_export->exp_target_data;

        return mdt_req_from_lcd(req, ted->ted_lcd);
}

static void mdt_reconstruct_create(struct mdt_thread_info *mti,
                                   struct mdt_lock_handle *lhc)
{
        struct ptlrpc_request  *req = mdt_info_req(mti);
        struct obd_export *exp = req->rq_export;
        struct tg_export_data *ted = &exp->exp_target_data;
        struct mdt_device *mdt = mti->mti_mdt;
        struct mdt_object *child;
        struct mdt_body *body;
        int rc;

        mdt_req_from_lcd(req, ted->ted_lcd);
        if (req->rq_status)
                return;

	/* if no error, so child was created with requested fid */
	child = mdt_object_find(mti->mti_env, mdt, mti->mti_rr.rr_fid2);
	if (IS_ERR(child)) {
		rc = PTR_ERR(child);
		LCONSOLE_WARN("cannot lookup child "DFID": rc = %d; "
			      "evicting client %s with export %s\n",
			      PFID(mti->mti_rr.rr_fid2), rc,
			      obd_uuid2str(&exp->exp_client_uuid),
			      obd_export_nid2str(exp));
		mdt_export_evict(exp);
		RETURN_EXIT;
	}

        body = req_capsule_server_get(mti->mti_pill, &RMF_MDT_BODY);
        mti->mti_attr.ma_need = MA_INODE;
        mti->mti_attr.ma_valid = 0;
	rc = mdt_attr_get_complex(mti, child, &mti->mti_attr);
	if (rc == -EREMOTE) {
		/* object was created on remote server */
		if (!mdt_is_dne_client(exp))
			/* Return -EIO for old client */
			rc = -EIO;

		req->rq_status = rc;
		body->valid |= OBD_MD_MDS;
	}
	mdt_pack_attr2body(mti, body, &mti->mti_attr.ma_attr,
			   mdt_object_fid(child));
	mdt_object_put(mti->mti_env, child);
}

static void mdt_reconstruct_setattr(struct mdt_thread_info *mti,
                                    struct mdt_lock_handle *lhc)
{
        struct ptlrpc_request  *req = mdt_info_req(mti);
        struct obd_export *exp = req->rq_export;
        struct mdt_export_data *med = &exp->exp_mdt_data;
        struct mdt_device *mdt = mti->mti_mdt;
        struct mdt_object *obj;
        struct mdt_body *body;
	int rc;

        mdt_req_from_lcd(req, med->med_ted.ted_lcd);
        if (req->rq_status)
                return;

        body = req_capsule_server_get(mti->mti_pill, &RMF_MDT_BODY);
        obj = mdt_object_find(mti->mti_env, mdt, mti->mti_rr.rr_fid1);
	if (IS_ERR(obj)) {
		rc = PTR_ERR(obj);
		LCONSOLE_WARN("cannot lookup "DFID": rc = %d; "
			      "evicting client %s with export %s\n",
			      PFID(mti->mti_rr.rr_fid1), rc,
			      obd_uuid2str(&exp->exp_client_uuid),
			      obd_export_nid2str(exp));
		mdt_export_evict(exp);
		RETURN_EXIT;
	}

        mti->mti_attr.ma_need = MA_INODE;
        mti->mti_attr.ma_valid = 0;
	mdt_attr_get_complex(mti, obj, &mti->mti_attr);
        mdt_pack_attr2body(mti, body, &mti->mti_attr.ma_attr,
                           mdt_object_fid(obj));
        if (mti->mti_ioepoch && (mti->mti_ioepoch->flags & MF_EPOCH_OPEN)) {
                struct mdt_file_data *mfd;
                struct mdt_body *repbody;

                repbody = req_capsule_server_get(mti->mti_pill, &RMF_MDT_BODY);
                repbody->ioepoch = obj->mot_ioepoch;
		spin_lock(&med->med_open_lock);
		cfs_list_for_each_entry(mfd, &med->med_open_head, mfd_list) {
			if (mfd->mfd_xid == req->rq_xid)
				break;
		}
		LASSERT(&mfd->mfd_list != &med->med_open_head);
		spin_unlock(&med->med_open_lock);
		repbody->handle.cookie = mfd->mfd_handle.h_cookie;
	}

	mdt_object_put(mti->mti_env, obj);
}

typedef void (*mdt_reconstructor)(struct mdt_thread_info *mti,
                                  struct mdt_lock_handle *lhc);

static mdt_reconstructor reconstructors[REINT_MAX] = {
        [REINT_SETATTR]  = mdt_reconstruct_setattr,
        [REINT_CREATE]   = mdt_reconstruct_create,
        [REINT_LINK]     = mdt_reconstruct_generic,
        [REINT_UNLINK]   = mdt_reconstruct_generic,
        [REINT_RENAME]   = mdt_reconstruct_generic,
        [REINT_OPEN]     = mdt_reconstruct_open,
        [REINT_SETXATTR] = mdt_reconstruct_generic
};

void mdt_reconstruct(struct mdt_thread_info *mti,
                     struct mdt_lock_handle *lhc)
{
        ENTRY;
        reconstructors[mti->mti_rr.rr_opcode](mti, lhc);
        EXIT;
}
