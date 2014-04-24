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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Lustre Unified Target
 * These are common function to work with last_received file
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */
#include <obd.h>
#include <obd_class.h>
#include <lustre_fid.h>

#include "tgt_internal.h"

struct tg_reply_data {
	struct list_head	trd_list;
	struct lsd_reply_data	trd_reply;
	int			trd_index;
};

static inline struct lu_buf *tti_buf_lsd(struct tgt_thread_info *tti)
{
	tti->tti_buf.lb_buf = &tti->tti_lsd;
	tti->tti_buf.lb_len = sizeof(tti->tti_lsd);
	return &tti->tti_buf;
}

static inline struct lu_buf *tti_buf_lcd(struct tgt_thread_info *tti)
{
	tti->tti_buf.lb_buf = &tti->tti_lcd;
	tti->tti_buf.lb_len = sizeof(tti->tti_lcd);
	return &tti->tti_buf;
}

static inline void tgt_bitmap_alloc(struct lu_target *lut, int i)
{
	unsigned long *bmp;

	if (lut->lut_reply_bitmap[i] != NULL)
		return;

	OBD_ALLOC(bmp, LUT_BITMAP_SIZE);
	if (unlikely(bmp == NULL))
		return;

	spin_lock(&lut->lut_client_bitmap_lock);
	if (lut->lut_reply_bitmap[i] == NULL) {
		lut->lut_reply_bitmap[i] = bmp;
	} else {
		OBD_FREE(bmp, LUT_BITMAP_SIZE);
	}
	spin_unlock(&lut->lut_client_bitmap_lock);
}

static int tgt_bitmap_set(struct lu_target *lut, int idx)
{
	unsigned long *bmp;
	int i, j;

	i = idx / LUT_SLOTS_PER_BITMAP;	/* bitmap # */
	LASSERT(i < LUT_MAX_BITMAPS);
	j = idx % LUT_SLOTS_PER_BITMAP;	/* bit in the bitmap */

	bmp = lut->lut_reply_bitmap[i];
	if (bmp == NULL) {
		tgt_bitmap_alloc(lut, i);
		bmp = lut->lut_reply_bitmap[i];
		if (bmp == NULL)
			return -ENOMEM;
	}

	return test_and_set_bit(j, bmp);
}

static int tgt_bitmap_find_free(struct lu_target *lut)
{
	unsigned long *bmp;
	int	       i, j;

	/*
	 * XXX: there are few ways to improve this:
	 *	- do not scan from the beginning, instead continue from
	 *	  the latest used/freed bit
	 *	- make it more SMP-friendly, probably partition this by
	 *	  cacheline % #cpu or something like that
	 */
	for (i = 0; i < LUT_MAX_BITMAPS; i++) {
		bmp = lut->lut_reply_bitmap[i];
		if (unlikely(bmp == NULL)) {
			tgt_bitmap_alloc(lut, i);
			bmp = lut->lut_reply_bitmap[i];
			if (unlikely(bmp == NULL))
				return -ENOMEM;
		}

		bmp = lut->lut_reply_bitmap[i];
		do {
			j = find_first_zero_bit(bmp, LUT_SLOTS_PER_BITMAP);
			if (j >= LUT_SLOTS_PER_BITMAP)
				break; /* continue with the next bitmap */

			if (test_and_set_bit(j, bmp) == 0)
				return (i * LUT_SLOTS_PER_BITMAP) + j;
		} while (1);
	}

	return -ENOSPC;
}

static void tgt_bitmap_clear(struct lu_target *lut, int idx)
{
	int i, j;

	i = idx / LUT_SLOTS_PER_BITMAP;	/* bitmap # */
	LASSERT(i < LUT_MAX_BITMAPS);
	j = idx % LUT_SLOTS_PER_BITMAP;	/* bit in the bitmap */

	LASSERT(lut->lut_reply_bitmap[i] != NULL);

	if (!test_and_clear_bit(j, lut->lut_reply_bitmap[i]))
		LBUG();
}

static void tgt_free_reply_data(struct lu_target *lut,
				struct tg_export_data *ted,
				struct tg_reply_data *trd)
{
	CDEBUG(D_OTHER, "drop rd %p: xid %llu, transno %llu\n", trd,
	       trd->trd_reply.lrd_xid, trd->trd_reply.lrd_transno);

	list_del(&trd->trd_list);
	ted->ted_slots--;
	tgt_bitmap_clear(lut, trd->trd_index);
	OBD_FREE_PTR(trd);
}

static void tgt_release_reply_data(struct lu_target *lut,
				   struct tg_export_data *ted,
				   struct tg_reply_data *trd)
{
	/* this is the last slot for this client, do not
	 * re-use it until there is another slot for this
	 * client, otherwise we risk to lose last_committed */
	if (trd->trd_reply.lrd_transno == ted->ted_transno) {
		LASSERT(ted->ted_last_reply == NULL);
		list_del_init(&trd->trd_list);
		ted->ted_last_reply = trd;
	} else {
		tgt_free_reply_data(lut, ted, trd);
	}
}

/**
 * Allocate in-memory data for client slot related to export.
 */
int tgt_client_alloc(struct obd_export *exp)
{
	ENTRY;
	LASSERT(exp != exp->exp_obd->obd_self_export);

	/* Mark that slot is not yet valid, 0 doesn't work here */
	exp->exp_target_data.ted_lr_idx = -1;
	INIT_LIST_HEAD(&exp->exp_target_data.ted_reply_list);

	RETURN(0);
}
EXPORT_SYMBOL(tgt_client_alloc);

/**
 * Free in-memory data for client slot related to export.
 */
void tgt_client_free(struct obd_export *exp)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*lut = class_exp2tgt(exp);
	struct tg_reply_data	*trd, *tmp;

	LASSERT(exp != exp->exp_obd->obd_self_export);

	ted->ted_uuid[0] = '\0';

	if (ted->ted_slots_reused > 0 || ted->ted_slots_acked > 0)
		LCONSOLE_INFO("%s: %u acked, %u reused\n", tgt_name(lut),
			      ted->ted_slots_acked, ted->ted_slots_reused);
	list_for_each_entry_safe(trd, tmp, &ted->ted_reply_list, trd_list) {
		tgt_free_reply_data(lut, ted, trd);
	}
	if (ted->ted_last_reply != NULL) {
		tgt_free_reply_data(lut, ted, ted->ted_last_reply);
		ted->ted_last_reply = NULL;
	}

	/* Slot may be not yet assigned */
	if (ted->ted_lr_idx < 0)
		return;
	/* Clear bit when lcd is freed */
	LASSERT(lut->lut_client_bitmap);
	if (!test_and_clear_bit(ted->ted_lr_idx, lut->lut_client_bitmap)) {
		CERROR("%s: client %u bit already clear in bitmap\n",
		       exp->exp_obd->obd_name, ted->ted_lr_idx);
		LBUG();
	}
}
EXPORT_SYMBOL(tgt_client_free);

static int tgt_client_data_read(const struct lu_env *env,
				struct lu_target *tgt,
				struct lsd_client_data *lcd,
				loff_t *off, int index)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int			 rc;

	tti_buf_lcd(tti);
	rc = dt_record_read(env, tgt->lut_last_rcvd, &tti->tti_buf, off);
	if (rc == 0) {
		check_lcd(tgt->lut_obd->obd_name, index, &tti->tti_lcd);
		lcd_le_to_cpu(&tti->tti_lcd, lcd);
		lcd->lcd_last_result = ptlrpc_status_ntoh(lcd->lcd_last_result);
		lcd->lcd_last_close_result =
			ptlrpc_status_ntoh(lcd->lcd_last_close_result);
	}

	CDEBUG(D_INFO, "%s: read lcd @%lld uuid = %s, last_transno = "LPU64
	       ", last_xid = "LPU64", last_result = %u, last_data = %u, "
	       "last_close_transno = "LPU64", last_close_xid = "LPU64", "
	       "last_close_result = %u, rc = %d\n", tgt->lut_obd->obd_name,
	       *off, lcd->lcd_uuid, lcd->lcd_last_transno, lcd->lcd_last_xid,
	       lcd->lcd_last_result, lcd->lcd_last_data,
	       lcd->lcd_last_close_transno, lcd->lcd_last_close_xid,
	       lcd->lcd_last_close_result, rc);
	return rc;
}

static int tgt_client_data_write(const struct lu_env *env,
				 struct lu_target *tgt,
				 struct tg_export_data *ted,
				 struct thandle *th)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct lsd_client_data	*lcd = &tti->tti_lcd;

	memset(lcd, 0, sizeof(*lcd));
	memcpy(lcd->lcd_uuid, ted->ted_uuid, sizeof(ted->ted_uuid));
	lcd->lcd_last_epoch = cpu_to_le32(ted->ted_last_epoch);
	lcd->lcd_last_transno = cpu_to_le64(ted->ted_transno);

	tti->tti_buf.lb_buf = lcd;
	tti->tti_buf.lb_len = sizeof(*lcd);
	tti->tti_off = ted->ted_lr_off;

	return dt_record_write(env, tgt->lut_last_rcvd,
			       &tti->tti_buf, &tti->tti_off, th);
}

static int tgt_reply_data_write(const struct lu_env *env, struct lu_target *tgt,
				struct lsd_reply_data *lrd, loff_t off,
				struct thandle *th)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct lsd_reply_data	*buf = &tti->tti_lrd;

	lrd->lrd_result = ptlrpc_status_hton(lrd->lrd_result);

	buf->lrd_pre_versions[0] = cpu_to_le64(lrd->lrd_pre_versions[0]);
	buf->lrd_pre_versions[1] = cpu_to_le64(lrd->lrd_pre_versions[1]);
	buf->lrd_pre_versions[2] = cpu_to_le64(lrd->lrd_pre_versions[2]);
	buf->lrd_pre_versions[3] = cpu_to_le64(lrd->lrd_pre_versions[3]);
	buf->lrd_transno	 = cpu_to_le64(lrd->lrd_transno);
	buf->lrd_xid		 = cpu_to_le64(lrd->lrd_xid);
	buf->lrd_data		 = cpu_to_le32(lrd->lrd_data);
	buf->lrd_result		 = cpu_to_le32(lrd->lrd_result);
	buf->lrd_client_idx	 = cpu_to_le32(lrd->lrd_client_idx);
	buf->lrd_tag		 = cpu_to_le32(lrd->lrd_tag);

	lrd->lrd_result = ptlrpc_status_ntoh(lrd->lrd_result);

	tti->tti_off = off;
	tti->tti_buf.lb_buf = buf;
	tti->tti_buf.lb_len = sizeof(*buf);
	return dt_record_write(env, tgt->lut_reply_log, &tti->tti_buf,
			       &tti->tti_off, th);
}

static int tgt_reply_data_read(const struct lu_env *env, struct lu_target *tgt,
			       struct lsd_reply_data *lrd, loff_t off)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int			 rc;

	tti->tti_off = off;
	tti->tti_buf.lb_buf = lrd;
	tti->tti_buf.lb_len = sizeof(*lrd);
	rc = dt_record_read(env, tgt->lut_reply_log, &tti->tti_buf,
			    &tti->tti_off);
	if (unlikely(rc != 0))
		return rc;

	lrd->lrd_pre_versions[0] = le64_to_cpu(lrd->lrd_pre_versions[0]);
	lrd->lrd_pre_versions[1] = le64_to_cpu(lrd->lrd_pre_versions[1]);
	lrd->lrd_pre_versions[2] = le64_to_cpu(lrd->lrd_pre_versions[2]);
	lrd->lrd_pre_versions[3] = le64_to_cpu(lrd->lrd_pre_versions[3]);
	lrd->lrd_transno	 = le64_to_cpu(lrd->lrd_transno);
	lrd->lrd_xid		 = le64_to_cpu(lrd->lrd_xid);
	lrd->lrd_data		 = le32_to_cpu(lrd->lrd_data);
	lrd->lrd_result		 = le32_to_cpu(lrd->lrd_result);
	lrd->lrd_client_idx	 = le32_to_cpu(lrd->lrd_client_idx);
	lrd->lrd_tag		 = le32_to_cpu(lrd->lrd_tag);

	lrd->lrd_result = ptlrpc_status_ntoh(lrd->lrd_result);

	return 0;
}

/**
 * Update client data in last_rcvd
 */
int tgt_client_data_update(const struct lu_env *env, struct obd_export *exp)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*tgt = class_exp2tgt(exp);
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct thandle		*th;
	int			 rc = 0;

	ENTRY;

	th = dt_trans_create(env, tgt->lut_bottom);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	tti_buf_lcd(tti);
	rc = dt_declare_record_write(env, tgt->lut_last_rcvd,
				     &tti->tti_buf,
				     ted->ted_lr_off, th);
	if (rc)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, tgt->lut_bottom, th);
	if (rc)
		GOTO(out, rc);
	/*
	 * Until this operations will be committed the sync is needed
	 * for this export. This should be done _after_ starting the
	 * transaction so that many connecting clients will not bring
	 * server down with lots of sync writes.
	 */
	rc = tgt_new_client_cb_add(th, exp);
	if (rc) {
		/* can't add callback, do sync now */
		th->th_sync = 1;
	} else {
		spin_lock(&exp->exp_lock);
		exp->exp_need_sync = 1;
		spin_unlock(&exp->exp_lock);
	}

	rc = tgt_client_data_write(env, tgt, ted, th);
	EXIT;
out:
	dt_trans_stop(env, tgt->lut_bottom, th);
	CDEBUG(D_INFO, "%s: update last_rcvd client data for UUID = %s, "
	       "last_transno = "LPU64": rc = %d\n", tgt->lut_obd->obd_name,
	       tgt->lut_lsd.lsd_uuid, tgt->lut_lsd.lsd_last_transno, rc);

	return rc;
}

int tgt_server_data_read(const struct lu_env *env, struct lu_target *tgt)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int			 rc;

	tti->tti_off = 0;
	tti_buf_lsd(tti);
	rc = dt_record_read(env, tgt->lut_last_rcvd, &tti->tti_buf,
			    &tti->tti_off);
	if (rc == 0)
		lsd_le_to_cpu(&tti->tti_lsd, &tgt->lut_lsd);

	CDEBUG(D_INFO, "%s: read last_rcvd server data for UUID = %s, "
	       "last_transno = "LPU64": rc = %d\n", tgt->lut_obd->obd_name,
	       tgt->lut_lsd.lsd_uuid, tgt->lut_lsd.lsd_last_transno, rc);
        return rc;
}
EXPORT_SYMBOL(tgt_server_data_read);

int tgt_server_data_write(const struct lu_env *env, struct lu_target *tgt,
			  struct thandle *th)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int			 rc;

	ENTRY;

	tti->tti_off = 0;
	tti_buf_lsd(tti);
	lsd_cpu_to_le(&tgt->lut_lsd, &tti->tti_lsd);

	rc = dt_record_write(env, tgt->lut_last_rcvd, &tti->tti_buf,
			     &tti->tti_off, th);

	CDEBUG(D_INFO, "%s: write last_rcvd server data for UUID = %s, "
	       "last_transno = "LPU64": rc = %d\n", tgt->lut_obd->obd_name,
	       tgt->lut_lsd.lsd_uuid, tgt->lut_lsd.lsd_last_transno, rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_server_data_write);

/**
 * Update server data in last_rcvd
 */
int tgt_server_data_update(const struct lu_env *env, struct lu_target *tgt,
			   int sync)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct thandle		*th;
	int			 rc = 0;

	ENTRY;

	CDEBUG(D_SUPER,
	       "%s: mount_count is "LPU64", last_transno is "LPU64"\n",
	       tgt->lut_lsd.lsd_uuid, tgt->lut_obd->u.obt.obt_mount_count,
	       tgt->lut_last_transno);

	/* Always save latest transno to keep it fresh */
	spin_lock(&tgt->lut_translock);
	tgt->lut_lsd.lsd_last_transno = tgt->lut_last_transno;
	spin_unlock(&tgt->lut_translock);

	th = dt_trans_create(env, tgt->lut_bottom);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	th->th_sync = sync;

	tti_buf_lsd(tti);
	rc = dt_declare_record_write(env, tgt->lut_last_rcvd,
				     &tti->tti_buf, tti->tti_off, th);
	if (rc)
		GOTO(out, rc);

	rc = dt_trans_start(env, tgt->lut_bottom, th);
	if (rc)
		GOTO(out, rc);

	rc = tgt_server_data_write(env, tgt, th);
out:
	dt_trans_stop(env, tgt->lut_bottom, th);

	CDEBUG(D_INFO, "%s: update last_rcvd server data for UUID = %s, "
	       "last_transno = "LPU64": rc = %d\n", tgt->lut_obd->obd_name,
	       tgt->lut_lsd.lsd_uuid, tgt->lut_lsd.lsd_last_transno, rc);
	RETURN(rc);
}
EXPORT_SYMBOL(tgt_server_data_update);

int tgt_truncate_last_rcvd(const struct lu_env *env, struct lu_target *tgt,
			   loff_t size)
{
	struct dt_object *dt = tgt->lut_last_rcvd;
	struct thandle	 *th;
	struct lu_attr	  attr;
	int		  rc;

	ENTRY;

	attr.la_size = size;
	attr.la_valid = LA_SIZE;

	th = dt_trans_create(env, tgt->lut_bottom);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));
	rc = dt_declare_punch(env, dt, size, OBD_OBJECT_EOF, th);
	if (rc)
		GOTO(cleanup, rc);
	rc = dt_declare_attr_set(env, dt, &attr, th);
	if (rc)
		GOTO(cleanup, rc);
	rc = dt_trans_start_local(env, tgt->lut_bottom, th);
	if (rc)
		GOTO(cleanup, rc);

	rc = dt_punch(env, dt, size, OBD_OBJECT_EOF, th, BYPASS_CAPA);
	if (rc == 0)
		rc = dt_attr_set(env, dt, &attr, th, BYPASS_CAPA);

cleanup:
	dt_trans_stop(env, tgt->lut_bottom, th);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_truncate_last_rcvd);

void tgt_client_epoch_update(const struct lu_env *env, struct obd_export *exp)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*tgt = class_exp2tgt(exp);

	LASSERT(tgt->lut_bottom);
	/** VBR: set client last_epoch to current epoch */
	if (ted->ted_last_epoch >= tgt->lut_lsd.lsd_start_epoch)
		return;
	ted->ted_last_epoch = tgt->lut_lsd.lsd_start_epoch;
	tgt_client_data_update(env, exp);
}

/**
 * Update boot epoch when recovery ends
 */
void tgt_boot_epoch_update(struct lu_target *tgt)
{
	struct lu_env		 env;
	struct ptlrpc_request	*req;
	__u32			 start_epoch;
	cfs_list_t		 client_list;
	int			 rc;

	if (tgt->lut_obd->obd_stopping)
		return;

	rc = lu_env_init(&env, LCT_DT_THREAD);
	if (rc) {
		CERROR("%s: can't initialize environment: rc = %d\n",
		        tgt->lut_obd->obd_name, rc);
		return;
	}

	spin_lock(&tgt->lut_translock);
	start_epoch = lr_epoch(tgt->lut_last_transno) + 1;
	tgt->lut_last_transno = (__u64)start_epoch << LR_EPOCH_BITS;
	tgt->lut_lsd.lsd_start_epoch = start_epoch;
	spin_unlock(&tgt->lut_translock);

	CFS_INIT_LIST_HEAD(&client_list);
	/**
	 * The recovery is not yet finished and final queue can still be updated
	 * with resend requests. Move final list to separate one for processing
	 */
	spin_lock(&tgt->lut_obd->obd_recovery_task_lock);
	cfs_list_splice_init(&tgt->lut_obd->obd_final_req_queue, &client_list);
	spin_unlock(&tgt->lut_obd->obd_recovery_task_lock);

	/**
	 * go through list of exports participated in recovery and
	 * set new epoch for them
	 */
	cfs_list_for_each_entry(req, &client_list, rq_list) {
		LASSERT(!req->rq_export->exp_delayed);
		if (!req->rq_export->exp_vbr_failed)
			tgt_client_epoch_update(&env, req->rq_export);
	}
	/** return list back at once */
	spin_lock(&tgt->lut_obd->obd_recovery_task_lock);
	cfs_list_splice_init(&client_list, &tgt->lut_obd->obd_final_req_queue);
	spin_unlock(&tgt->lut_obd->obd_recovery_task_lock);
	/** update server epoch */
	tgt_server_data_update(&env, tgt, 1);
	lu_env_fini(&env);
}
EXPORT_SYMBOL(tgt_boot_epoch_update);

/**
 * commit callback, need to update last_commited value
 */
struct tgt_last_committed_callback {
	struct dt_txn_commit_cb	 llcc_cb;
	struct lu_target	*llcc_tgt;
	struct obd_export	*llcc_exp;
	__u64			 llcc_transno;
};

void tgt_cb_last_committed(struct lu_env *env, struct thandle *th,
			   struct dt_txn_commit_cb *cb, int err)
{
	struct tgt_last_committed_callback *ccb;

	ccb = container_of0(cb, struct tgt_last_committed_callback, llcc_cb);

	LASSERT(ccb->llcc_tgt != NULL);
	LASSERT(ccb->llcc_exp->exp_obd == ccb->llcc_tgt->lut_obd);

	spin_lock(&ccb->llcc_tgt->lut_translock);
	if (ccb->llcc_transno > ccb->llcc_tgt->lut_obd->obd_last_committed)
		ccb->llcc_tgt->lut_obd->obd_last_committed = ccb->llcc_transno;

	LASSERT(ccb->llcc_exp);
	if (ccb->llcc_transno > ccb->llcc_exp->exp_last_committed) {
		ccb->llcc_exp->exp_last_committed = ccb->llcc_transno;
		spin_unlock(&ccb->llcc_tgt->lut_translock);
		ptlrpc_commit_replies(ccb->llcc_exp);
	} else {
		spin_unlock(&ccb->llcc_tgt->lut_translock);
	}
	class_export_cb_put(ccb->llcc_exp);
	if (ccb->llcc_transno)
		CDEBUG(D_HA, "%s: transno "LPD64" is committed\n",
		       ccb->llcc_tgt->lut_obd->obd_name, ccb->llcc_transno);
	OBD_FREE_PTR(ccb);
}

int tgt_last_commit_cb_add(struct thandle *th, struct lu_target *tgt,
			   struct obd_export *exp, __u64 transno)
{
	struct tgt_last_committed_callback	*ccb;
	struct dt_txn_commit_cb			*dcb;
	int					 rc;

	OBD_ALLOC_PTR(ccb);
	if (ccb == NULL)
		return -ENOMEM;

	ccb->llcc_tgt = tgt;
	ccb->llcc_exp = class_export_cb_get(exp);
	ccb->llcc_transno = transno;

	dcb = &ccb->llcc_cb;
	dcb->dcb_func = tgt_cb_last_committed;
	CFS_INIT_LIST_HEAD(&dcb->dcb_linkage);
	strncpy(dcb->dcb_name, "tgt_cb_last_committed", MAX_COMMIT_CB_STR_LEN);
	dcb->dcb_name[MAX_COMMIT_CB_STR_LEN - 1] = '\0';

	rc = dt_trans_cb_add(th, dcb);
	if (rc) {
		class_export_cb_put(exp);
		OBD_FREE_PTR(ccb);
	}

	if (exp_connect_flags(exp) & OBD_CONNECT_LIGHTWEIGHT)
		/* report failure to force synchronous operation */
		return -EPERM;

	return rc;
}
EXPORT_SYMBOL(tgt_last_commit_cb_add);

struct tgt_new_client_callback {
	struct dt_txn_commit_cb	 lncc_cb;
	struct obd_export	*lncc_exp;
};

void tgt_cb_new_client(struct lu_env *env, struct thandle *th,
		       struct dt_txn_commit_cb *cb, int err)
{
	struct tgt_new_client_callback *ccb;

	ccb = container_of0(cb, struct tgt_new_client_callback, lncc_cb);

	LASSERT(ccb->lncc_exp->exp_obd);

	CDEBUG(D_RPCTRACE, "%s: committing for initial connect of %s\n",
	       ccb->lncc_exp->exp_obd->obd_name,
	       ccb->lncc_exp->exp_client_uuid.uuid);

	spin_lock(&ccb->lncc_exp->exp_lock);
	/* XXX: Currently, we use per-export based sync/async policy for
	 *	the update via OUT RPC, it is coarse-grained policy, and
	 *	will be changed as per-request based by DNE II patches. */
	if (!ccb->lncc_exp->exp_keep_sync)
		ccb->lncc_exp->exp_need_sync = 0;

	spin_unlock(&ccb->lncc_exp->exp_lock);
	class_export_cb_put(ccb->lncc_exp);

	OBD_FREE_PTR(ccb);
}

int tgt_new_client_cb_add(struct thandle *th, struct obd_export *exp)
{
	struct tgt_new_client_callback	*ccb;
	struct dt_txn_commit_cb		*dcb;
	int				 rc;

	OBD_ALLOC_PTR(ccb);
	if (ccb == NULL)
		return -ENOMEM;

	ccb->lncc_exp = class_export_cb_get(exp);

	dcb = &ccb->lncc_cb;
	dcb->dcb_func = tgt_cb_new_client;
	CFS_INIT_LIST_HEAD(&dcb->dcb_linkage);
	strncpy(dcb->dcb_name, "tgt_cb_new_client", MAX_COMMIT_CB_STR_LEN);
	dcb->dcb_name[MAX_COMMIT_CB_STR_LEN - 1] = '\0';

	rc = dt_trans_cb_add(th, dcb);
	if (rc) {
		class_export_cb_put(exp);
		OBD_FREE_PTR(ccb);
	}
	return rc;
}

/**
 * Add new client to the last_rcvd upon new connection.
 *
 * We use a bitmap to locate a free space in the last_rcvd file and initialize
 * tg_export_data.
 */
int tgt_client_new(const struct lu_env *env, struct obd_export *exp)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*tgt = class_exp2tgt(exp);
	int			 rc = 0, idx;

	ENTRY;

	LASSERT(tgt->lut_client_bitmap != NULL);
	if (!strcmp(ted->ted_uuid, tgt->lut_obd->obd_uuid.uuid))
		RETURN(0);

	mutex_init(&ted->ted_lcd_lock);

	if (exp_connect_flags(exp) & OBD_CONNECT_LIGHTWEIGHT)
		RETURN(0);

	/* the bitmap operations can handle cl_idx > sizeof(long) * 8, so
	 * there's no need for extra complication here
	 */
	idx = find_first_zero_bit(tgt->lut_client_bitmap, LR_MAX_CLIENTS);
repeat:
	if (idx >= LR_MAX_CLIENTS ||
	    OBD_FAIL_CHECK(OBD_FAIL_MDS_CLIENT_ADD)) {
		CERROR("%s: no room for %u clients - fix LR_MAX_CLIENTS\n",
		       tgt->lut_obd->obd_name,  idx);
		RETURN(-EOVERFLOW);
	}
	if (test_and_set_bit(idx, tgt->lut_client_bitmap)) {
		idx = find_next_zero_bit(tgt->lut_client_bitmap,
					     LR_MAX_CLIENTS, idx);
		goto repeat;
	}

	CDEBUG(D_INFO, "%s: client at idx %d with UUID '%s' added\n",
	       tgt->lut_obd->obd_name, idx, ted->ted_uuid);

	ted->ted_lr_idx = idx;
	ted->ted_lr_off = tgt->lut_lsd.lsd_client_start +
			  idx * tgt->lut_lsd.lsd_client_size;

	LASSERTF(ted->ted_lr_off > 0, "ted_lr_off = %llu\n", ted->ted_lr_off);

	CDEBUG(D_INFO, "%s: new client at index %d (%llu) with UUID '%s'\n",
	       tgt->lut_obd->obd_name, ted->ted_lr_idx, ted->ted_lr_off,
	       ted->ted_uuid);

	if (OBD_FAIL_CHECK(OBD_FAIL_TGT_CLIENT_ADD))
		RETURN(-ENOSPC);

	rc = tgt_client_data_update(env, exp);
	if (rc)
		CERROR("%s: Failed to write client lcd at idx %d, rc %d\n",
		       tgt->lut_obd->obd_name, idx, rc);

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_client_new);

/* Add client data to the MDS.  We use a bitmap to locate a free space
 * in the last_rcvd file if cl_off is -1 (i.e. a new client).
 * Otherwise, we just have to read the data from the last_rcvd file and
 * we know its offset.
 *
 * It should not be possible to fail adding an existing client - otherwise
 * mdt_init_server_data() callsite needs to be fixed.
 */
int tgt_client_add(const struct lu_env *env,  struct obd_export *exp, int idx)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*tgt = class_exp2tgt(exp);

	ENTRY;

	LASSERT(tgt->lut_client_bitmap != NULL);
	LASSERTF(idx >= 0, "%d\n", idx);

	if (!strcmp(ted->ted_uuid, tgt->lut_obd->obd_uuid.uuid) ||
	    exp_connect_flags(exp) & OBD_CONNECT_LIGHTWEIGHT)
		RETURN(0);

	if (test_and_set_bit(idx, tgt->lut_client_bitmap)) {
		CERROR("%s: client %d: bit already set in bitmap!!\n",
		       tgt->lut_obd->obd_name,  idx);
		LBUG();
	}

	CDEBUG(D_INFO, "%s: client at idx %d with UUID '%s' added\n",
	       tgt->lut_obd->obd_name, idx, ted->ted_uuid);

	ted->ted_lr_idx = idx;
	ted->ted_lr_off = tgt->lut_lsd.lsd_client_start +
			  idx * tgt->lut_lsd.lsd_client_size;

	mutex_init(&ted->ted_lcd_lock);

	LASSERTF(ted->ted_lr_off > 0, "ted_lr_off = %llu\n", ted->ted_lr_off);

	RETURN(0);
}
EXPORT_SYMBOL(tgt_client_add);

int tgt_client_del(const struct lu_env *env, struct obd_export *exp)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*tgt = class_exp2tgt(exp);
	int			 rc;

	ENTRY;

	/* XXX if lcd_uuid were a real obd_uuid, I could use obd_uuid_equals */
	if (!strcmp((char *)ted->ted_uuid,
		    (char *)tgt->lut_obd->obd_uuid.uuid) ||
	    exp_connect_flags(exp) & OBD_CONNECT_LIGHTWEIGHT)
		RETURN(0);

	CDEBUG(D_INFO, "%s: del client at idx %u, off %lld, UUID '%s'\n",
	       tgt->lut_obd->obd_name, ted->ted_lr_idx, ted->ted_lr_off,
	       ted->ted_uuid);

	/* Clear the bit _after_ zeroing out the client so we don't
	   race with filter_client_add and zero out new clients.*/
	if (!test_bit(ted->ted_lr_idx, tgt->lut_client_bitmap)) {
		CERROR("%s: client %u: bit already clear in bitmap!!\n",
		       tgt->lut_obd->obd_name, ted->ted_lr_idx);
		LBUG();
	}

	/* Do not erase record for recoverable client. */
	if (exp->exp_flags & OBD_OPT_FAILOVER)
		RETURN(0);

	/* Make sure the server's last_transno is up to date.
	 * This should be done before zeroing client slot so last_transno will
	 * be in server data or in client data in case of failure */
	rc = tgt_server_data_update(env, tgt, 0);
	if (rc != 0) {
		CERROR("%s: failed to update server data, skip client %s "
		       "zeroing, rc %d\n", tgt->lut_obd->obd_name,
		       ted->ted_uuid, rc);
		RETURN(rc);
	}

	rc = tgt_reply_log_clear(env, tgt, ted);
	if (rc != 0) {
		CERROR("%s: failed to clear reply log, skip client %s "
		       "zeroing, rc %d\n", tgt->lut_obd->obd_name,
		       ted->ted_uuid, rc);
		RETURN(rc);
	}

	mutex_lock(&ted->ted_lcd_lock);
	memset(ted->ted_uuid, 0, sizeof ted->ted_uuid);
	rc = tgt_client_data_update(env, exp);
	mutex_unlock(&ted->ted_lcd_lock);

	CDEBUG(rc == 0 ? D_INFO : D_ERROR,
	       "%s: zeroing out client %s at idx %u (%llu), rc %d\n",
	       tgt->lut_obd->obd_name, ted->ted_uuid,
	       ted->ted_lr_idx, ted->ted_lr_off, rc);
	RETURN(rc);
}
EXPORT_SYMBOL(tgt_client_del);

/*
 * last_rcvd & last_committed update callbacks
 */
int tgt_last_rcvd_update(const struct lu_env *env, struct lu_target *tgt,
			 struct dt_object *obj, __u64 opdata,
			 struct thandle *th, struct ptlrpc_request *req)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct tg_export_data	*ted;
	struct tg_reply_data	*trd;
	struct lsd_reply_data	*lrd;
	int			 i, rc = 0;
	loff_t			 off;
	bool			 lw_client, update = false;
	__u64			*pre_versions;

	ENTRY;

	ted = &req->rq_export->exp_target_data;

	lw_client = exp_connect_flags(req->rq_export) & OBD_CONNECT_LIGHTWEIGHT;
	if (ted->ted_lr_idx < 0 && !lw_client)
		/* ofd connect may cause transaction before export has
		 * last_rcvd slot */
		RETURN(0);

	tti->tti_transno = lustre_msg_get_transno(req->rq_reqmsg);

	spin_lock(&tgt->lut_translock);
	if (th->th_result != 0) {
		if (tti->tti_transno != 0) {
			CERROR("%s: replay transno "LPU64" failed: rc = %d\n",
			       tgt_name(tgt), tti->tti_transno, th->th_result);
		}
	} else if (tti->tti_transno == 0) {
		tti->tti_transno = ++tgt->lut_last_transno;
	} else {
		/* should be replay */
		if (tti->tti_transno > tgt->lut_last_transno)
			tgt->lut_last_transno = tti->tti_transno;
	}
	spin_unlock(&tgt->lut_translock);

	/** VBR: set new versions */
	if (th->th_result == 0 && obj != NULL)
		dt_version_set(env, obj, tti->tti_transno, th);

	/* filling reply data */
	CDEBUG(D_INODE, "transno = "LPU64", last_committed = "LPU64"\n",
	       tti->tti_transno, tgt->lut_obd->obd_last_committed);

	req->rq_transno = tti->tti_transno;
	lustre_msg_set_transno(req->rq_repmsg, tti->tti_transno);

	/* if can't add callback, do sync write */
	th->th_sync |= !!tgt_last_commit_cb_add(th, tgt, req->rq_export,
						tti->tti_transno);

	if (lw_client) {
		/* All operations performed by LW clients are synchronous and
		 * we store the committed transno in the last_rcvd header */
		spin_lock(&tgt->lut_translock);
		if (tti->tti_transno > tgt->lut_lsd.lsd_last_transno) {
			tgt->lut_lsd.lsd_last_transno = tti->tti_transno;
			update = true;
		}
		spin_unlock(&tgt->lut_translock);
		/* Although lightweight (LW) connections have no slot in
		 * last_rcvd, we still want to maintain the in-memory
		 * lsd_client_data structure in order to properly handle reply
		 * reconstruction. */
	} else if (ted->ted_lr_off == 0) {
		CERROR("%s: client idx %d has offset %lld\n",
		       tgt_name(tgt), ted->ted_lr_idx, ted->ted_lr_off);
		RETURN(-EINVAL);
	}

	/* if the export has already been disconnected, we have no last_rcvd
	 * slot, update server data with latest transno then */
	if (unlikely(ted->ted_uuid[0] == '\0')) {
		CWARN("commit transaction for disconnected client %s: rc %d\n",
		      req->rq_export->exp_client_uuid.uuid, rc);
		GOTO(srv_update, rc = 0);
	}

	if (lw_client)
		GOTO(srv_update, rc = 0);

	if (tgt->lut_no_reconstruct) {
		mutex_lock(&ted->ted_lcd_lock);
		if (tti->tti_transno > ted->ted_transno) {
			ted->ted_transno = tti->tti_transno;
			rc = tgt_client_data_write(env, tgt, ted, th);
		}
		mutex_unlock(&ted->ted_lcd_lock);
		GOTO(srv_update, rc);
	}

	/* sanity check we aren't going to rewrite pending slot */
	lrd = tgt_lookup_reply(req);
	LASSERTF(lrd == NULL, "found lrd %p: xid %llu, transno %llu\n",
		 lrd, lrd->lrd_xid, lrd->lrd_transno);

	OBD_ALLOC_PTR(trd);
	if (unlikely(trd == NULL))
		GOTO(srv_update, rc = -ENOMEM);
	lrd = &trd->trd_reply;

	mutex_lock(&ted->ted_lcd_lock);
	LASSERT(ergo(tti->tti_transno == 0, th->th_result != 0));

	if (tti->tti_transno > ted->ted_transno)
		ted->ted_transno = tti->tti_transno;

	/* VBR: save versions in last_rcvd for reconstruct. */
	pre_versions = lustre_msg_get_versions(req->rq_repmsg);
	if (pre_versions) {
		lrd->lrd_pre_versions[0] = pre_versions[0];
		lrd->lrd_pre_versions[1] = pre_versions[1];
		lrd->lrd_pre_versions[2] = pre_versions[2];
		lrd->lrd_pre_versions[3] = pre_versions[3];
	}

	/* XXX: lcd_last_data is __u32 but intent_dispostion is __u64,
	 * see struct ldlm_reply->lock_policy_res1; */
	lrd->lrd_data = opdata;
	lrd->lrd_xid = req->rq_xid;
	LASSERT(lrd->lrd_xid != 0);
	lrd->lrd_result = th->th_result;
	/* transno can be 0 */
	lrd->lrd_transno = tti->tti_transno;
	lrd->lrd_client_idx = ted->ted_lr_off;
	/* the tag is supposed to be verified in tgt_handle_tag() */
	lrd->lrd_tag = lustre_msg_get_tag(req->rq_reqmsg);

	/* find an empty slot */
	i = tgt_bitmap_find_free(tgt);
	if (unlikely(i < 0)) {
		CERROR("%s: couldn't find a slot: rc = %d\n", tgt_name(tgt), i);
		GOTO(unlock, rc = i);
	}
	trd->trd_index = i;

	off = sizeof(*lrd) * i;
	rc = tgt_reply_data_write(env, tgt, lrd, off, th);
	if (unlikely(rc != 0)) {
		CERROR("%s: can't update reply log: rc = %d\n",
		       tgt_name(tgt), rc);
		/* proceed, hopefully we won't need it to reconstruct */
		rc = 0;
	}

	list_add(&trd->trd_list, &ted->ted_reply_list);
	ted->ted_slots++;

	CDEBUG(D_OTHER, "add rd %p: tag %u, xid %llu, transno %llu\n",
		trd, lrd->lrd_tag, lrd->lrd_xid, lrd->lrd_transno);

	/* if there is a retained slot to preserve last committed transno,
	 * we can release it now - this new slot will be representing
	 * new last committed transno */
	if (ted->ted_last_reply != NULL) {
		tgt_free_reply_data(tgt, ted, ted->ted_last_reply);
		ted->ted_last_reply = NULL;
	}

unlock:
	mutex_unlock(&ted->ted_lcd_lock);
	EXIT;
srv_update:
	if (update)
		rc = tgt_server_data_write(env, tgt, th);
	return rc;
}

#if 1
int tgt_last_rcvd_update_echo(const struct lu_env *env, struct lu_target *tgt,
			      struct dt_object *obj, struct thandle *th,
			      struct obd_export *exp)
{
	LBUG();
	return 0;
}
#else

/*
 * last_rcvd update for echo client simulation.
 * It updates last_rcvd client slot and version of object in
 * simple way but with all locks to simulate all drawbacks
 */
int tgt_last_rcvd_update_echo(const struct lu_env *env, struct lu_target *tgt,
			      struct dt_object *obj, struct thandle *th,
			      struct obd_export *exp)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct tg_export_data	*ted = &exp->exp_target_data;
	int			 i, rc = 0;

	ENTRY;

	tti->tti_transno = 0;

	spin_lock(&tgt->lut_translock);
	if (th->th_result == 0)
		tti->tti_transno = ++tgt->lut_last_transno;
	spin_unlock(&tgt->lut_translock);

	/** VBR: set new versions */
	if (th->th_result == 0 && obj != NULL)
		dt_version_set(env, obj, tti->tti_transno, th);

	/* if can't add callback, do sync write */
	th->th_sync |= !!tgt_last_commit_cb_add(th, tgt, exp,
						tti->tti_transno);

	LASSERT(ted->ted_lr_off > 0);

	mutex_lock(&ted->ted_lcd_lock);
	LASSERT(ergo(tti->tti_transno == 0, th->th_result != 0));
	ted->ted_lcd->lcd_last_transno = tti->tti_transno;
	ted->ted_lcd->lcd_last_result = th->th_result;

	tti->tti_off = ted->ted_lr_off;
	rc = tgt_client_data_write(env, tgt, ted->ted_lcd, &tti->tti_off, th);
	mutex_unlock(&ted->ted_lcd_lock);
	RETURN(rc);
}
#endif

int tgt_clients_data_init(const struct lu_env *env, struct lu_target *tgt,
			  unsigned long last_size)
{
	struct obd_device	*obd = tgt->lut_obd;
	struct lr_server_data	*lsd = &tgt->lut_lsd;
	struct lsd_client_data	*lcd = NULL;
	struct tg_export_data	*ted;
	int			 cl_idx;
	int			 rc = 0;
	loff_t			 off = lsd->lsd_client_start;

	ENTRY;

	CLASSERT(offsetof(struct lsd_client_data, lcd_padding) +
		 sizeof(lcd->lcd_padding) == LR_CLIENT_SIZE);

	OBD_ALLOC_PTR(lcd);
	if (lcd == NULL)
		RETURN(-ENOMEM);

	for (cl_idx = 0; off < last_size; cl_idx++) {
		struct obd_export	*exp;
		__u64			 last_transno;

		/* Don't assume off is incremented properly by
		 * read_record(), in case sizeof(*lcd)
		 * isn't the same as fsd->lsd_client_size.  */
		off = lsd->lsd_client_start + cl_idx * lsd->lsd_client_size;
		rc = tgt_client_data_read(env, tgt, lcd, &off, cl_idx);
		if (rc) {
			CERROR("%s: error reading last_rcvd %s idx %d off "
			       "%llu: rc = %d\n", tgt_name(tgt), LAST_RCVD,
			       cl_idx, off, rc);
			rc = 0;
			break; /* read error shouldn't cause startup to fail */
		}

		if (lcd->lcd_uuid[0] == '\0') {
			CDEBUG(D_INFO, "skipping zeroed client at offset %d\n",
			       cl_idx);
			continue;
		}

		last_transno = lcd_last_transno(lcd);

		/* These exports are cleaned up by disconnect, so they
		 * need to be set up like real exports as connect does.
		 */
		CDEBUG(D_HA, "RCVRNG CLIENT uuid: %s idx: %d lr: "LPU64
		       " srv lr: "LPU64" lx: "LPU64"\n", lcd->lcd_uuid, cl_idx,
		       last_transno, lsd->lsd_last_transno, lcd_last_xid(lcd));

		exp = class_new_export(obd, (struct obd_uuid *)lcd->lcd_uuid);
		if (IS_ERR(exp)) {
			if (PTR_ERR(exp) == -EALREADY) {
				/* export already exists, zero out this one */
				CERROR("%s: Duplicate export %s!\n",
				       tgt_name(tgt), lcd->lcd_uuid);
				continue;
			}
			GOTO(err_out, rc = PTR_ERR(exp));
		}

		/* XXX: generate reply data from last_rcvd - compatibility */

		ted = &exp->exp_target_data;
		memcpy(ted->ted_uuid, lcd->lcd_uuid, sizeof(ted->ted_uuid));

		rc = tgt_client_add(env, exp, cl_idx);
		LASSERTF(rc == 0, "rc = %d\n", rc); /* can't fail existing */
		/* VBR: set export last committed version */
		exp->exp_last_committed = last_transno;
		spin_lock(&exp->exp_lock);
		exp->exp_connecting = 0;
		exp->exp_in_recovery = 0;
		spin_unlock(&exp->exp_lock);
		obd->obd_max_recoverable_clients++;
		class_export_put(exp);

		/* Need to check last_rcvd even for duplicated exports. */
		CDEBUG(D_OTHER, "client at idx %d has last_transno = "LPU64"\n",
		       cl_idx, last_transno);

		spin_lock(&tgt->lut_translock);
		tgt->lut_last_transno = max(last_transno,
					    tgt->lut_last_transno);
		spin_unlock(&tgt->lut_translock);
	}

err_out:
	OBD_FREE_PTR(lcd);
	RETURN(rc);
}

int tgt_reply_log_init(const struct lu_env *env, struct lu_target *tgt)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct lu_attr		*attr = &tti->tti_attr;
	struct obd_device	*obd = tgt->lut_obd;
	struct lr_server_data	*lsd = &tgt->lut_lsd;
	struct lsd_reply_data	*lrd = &tti->tti_lrd;
	struct lsd_client_data	*lcd = NULL;
	struct tg_export_data	*ted;
	cfs_hash_t		*hash = NULL;
	int			 i, rc = 0;
	loff_t			 off = lsd->lsd_client_start;

	ENTRY;

	rc = dt_attr_get(env, tgt->lut_reply_log, attr, BYPASS_CAPA);
	if (rc)
		RETURN(rc);

	OBD_ALLOC_PTR(lcd);
	if (lcd == NULL)
		RETURN(-ENOMEM);

	hash = cfs_hash_getref(obd->obd_uuid_hash);
	if (hash == NULL)
		GOTO(err_out, rc = -ENODEV);

	for (i = 0; i < attr->la_size / sizeof(*lrd); i++) {
		struct tg_reply_data	*trd;
		struct obd_export	*exp;

		off = i * sizeof(*lrd);
		rc = tgt_reply_data_read(env, tgt, lrd, off);
		if (rc) {
			CERROR("%s: error reading reply log off %llu: "
			       "rc = %d\n", tgt_name(tgt), off, rc);
			break;
		}

		LASSERT(lrd->lrd_client_idx >= 0);

		/* found corresponded client in last_rcvd */
		off = lrd->lrd_client_idx;
		if (off == 0) {
			/* XXX: actually we should not assert .. */
			LASSERT(lrd->lrd_xid == 0);
			LASSERT(lrd->lrd_transno == 0);
			continue;
		}

		rc = tgt_client_data_read(env, tgt, lcd, &off, 0);
		if (rc) {
			CERROR("%s: can't read last_rcvd@%llu: rc = %d\n",
			       tgt_name(tgt), off, rc);
			break;
		}

		if (lcd->lcd_uuid[0] == '\0')
			continue;

		exp = cfs_hash_lookup(hash, &lcd->lcd_uuid);
		if (exp == NULL) {
			CERROR("referring non-existing client %s?!\n",
			       lcd->lcd_uuid);
			rc = -EINVAL;
			break;
		}
		ted = &exp->exp_target_data;

		/* These exports are cleaned up by disconnect, so they
		 * need to be set up like real exports as connect does.
		 */
		LCONSOLE_INFO("%s: REPLY xid "LPU64" transno "LPU64"\n",
			      lcd->lcd_uuid, lrd->lrd_xid, lrd->lrd_transno);

		OBD_ALLOC_PTR(trd);
		if (unlikely(trd == NULL))
			GOTO(err_out, rc = -ENOMEM);

		/* XXX: fix endian */
		memcpy(&trd->trd_reply, lrd, sizeof(*lrd));
		trd->trd_index = i;

		if (tgt_bitmap_set(tgt, i) != 0) {
			/* XXX: return an error? */
			LBUG();
		}

		list_add(&trd->trd_list, &ted->ted_reply_list);
		ted->ted_slots++;

		exp->exp_last_committed = max(exp->exp_last_committed,
					      lrd->lrd_transno);
		class_export_put(exp);

		spin_lock(&tgt->lut_translock);
		tgt->lut_last_transno = max(lrd->lrd_transno,
					    tgt->lut_last_transno);
		spin_unlock(&tgt->lut_translock);
	}

err_out:
	if (lcd != NULL)
		OBD_FREE_PTR(lcd);
	if (hash != NULL)
		cfs_hash_putref(hash);

	RETURN(rc);
}

struct server_compat_data {
	__u32 rocompat;
	__u32 incompat;
	__u32 rocinit;
	__u32 incinit;
};

static struct server_compat_data tgt_scd[] = {
	[LDD_F_SV_TYPE_MDT] = {
		.rocompat = OBD_ROCOMPAT_LOVOBJID,
		.incompat = OBD_INCOMPAT_MDT | OBD_INCOMPAT_COMMON_LR |
			    OBD_INCOMPAT_FID | OBD_INCOMPAT_IAM_DIR |
			    OBD_INCOMPAT_LMM_VER | OBD_INCOMPAT_MULTI_OI,
		.rocinit = OBD_ROCOMPAT_LOVOBJID,
		.incinit = OBD_INCOMPAT_MDT | OBD_INCOMPAT_COMMON_LR |
			   OBD_INCOMPAT_MULTI_OI,
	},
	[LDD_F_SV_TYPE_OST] = {
		.rocompat = 0,
		.incompat = OBD_INCOMPAT_OST | OBD_INCOMPAT_COMMON_LR |
			    OBD_INCOMPAT_FID,
		.rocinit = 0,
		.incinit = OBD_INCOMPAT_OST | OBD_INCOMPAT_COMMON_LR,
	}
};

int tgt_server_data_init(const struct lu_env *env, struct lu_target *tgt)
{
	struct tgt_thread_info		*tti = tgt_th_info(env);
	struct lr_server_data		*lsd = &tgt->lut_lsd;
	unsigned long			 last_rcvd_size;
	__u32				 index;
	int				 rc, type;

	rc = dt_attr_get(env, tgt->lut_last_rcvd, &tti->tti_attr, BYPASS_CAPA);
	if (rc)
		RETURN(rc);

	last_rcvd_size = (unsigned long)tti->tti_attr.la_size;

	/* ensure padding in the struct is the correct size */
	CLASSERT(offsetof(struct lr_server_data, lsd_padding) +
		 sizeof(lsd->lsd_padding) == LR_SERVER_SIZE);

	rc = server_name2index(tgt_name(tgt), &index, NULL);
	if (rc < 0) {
		CERROR("%s: Can not get index from name: rc = %d\n",
		       tgt_name(tgt), rc);
		RETURN(rc);
	}
	/* server_name2index() returns type */
	type = rc;
	if (type != LDD_F_SV_TYPE_MDT && type != LDD_F_SV_TYPE_OST) {
		CERROR("%s: unknown target type %x\n", tgt_name(tgt), type);
		RETURN(-EINVAL);
	}

	/* last_rcvd on OST doesn't provide reconstruct support because there
	 * may be up to 8 in-flight write requests per single slot in
	 * last_rcvd client data
	 */
	tgt->lut_no_reconstruct = (type == LDD_F_SV_TYPE_OST);

	if (last_rcvd_size == 0) {
		LCONSOLE_WARN("%s: new disk, initializing\n", tgt_name(tgt));

		memcpy(lsd->lsd_uuid, tgt->lut_obd->obd_uuid.uuid,
		       sizeof(lsd->lsd_uuid));
		lsd->lsd_last_transno = 0;
		lsd->lsd_mount_count = 0;
		lsd->lsd_server_size = LR_SERVER_SIZE;
		lsd->lsd_client_start = LR_CLIENT_START;
		lsd->lsd_client_size = LR_CLIENT_SIZE;
		lsd->lsd_subdir_count = OBJ_SUBDIR_COUNT;
		lsd->lsd_osd_index = index;
		lsd->lsd_feature_rocompat = tgt_scd[type].rocinit;
		lsd->lsd_feature_incompat = tgt_scd[type].incinit;
	} else {
		rc = tgt_server_data_read(env, tgt);
		if (rc) {
			CERROR("%s: error reading LAST_RCVD: rc= %d\n",
			       tgt_name(tgt), rc);
			RETURN(rc);
		}
		if (strcmp(lsd->lsd_uuid, tgt->lut_obd->obd_uuid.uuid)) {
			LCONSOLE_ERROR_MSG(0x157, "Trying to start OBD %s "
					   "using the wrong disk %s. Were the"
					   " /dev/ assignments rearranged?\n",
					   tgt->lut_obd->obd_uuid.uuid,
					   lsd->lsd_uuid);
			RETURN(-EINVAL);
		}

		if (lsd->lsd_osd_index != index) {
			LCONSOLE_ERROR_MSG(0x157, "%s: index %d in last rcvd "
					   "is different with the index %d in"
					   "config log, It might be disk"
					   "corruption!\n", tgt_name(tgt),
					   lsd->lsd_osd_index, index);
			RETURN(-EINVAL);
		}
	}

	if (lsd->lsd_feature_incompat & ~tgt_scd[type].incompat) {
		CERROR("%s: unsupported incompat filesystem feature(s) %x\n",
		       tgt_name(tgt),
		       lsd->lsd_feature_incompat & ~tgt_scd[type].incompat);
		RETURN(-EINVAL);
	}

	if (type == LDD_F_SV_TYPE_MDT)
		lsd->lsd_feature_incompat |= OBD_INCOMPAT_FID;

	if (lsd->lsd_feature_rocompat & ~tgt_scd[type].rocompat) {
		CERROR("%s: unsupported read-only filesystem feature(s) %x\n",
		       tgt_name(tgt),
		       lsd->lsd_feature_rocompat & ~tgt_scd[type].rocompat);
		RETURN(-EINVAL);
	}
	/** Interop: evict all clients at first boot with 1.8 last_rcvd */
	if (type == LDD_F_SV_TYPE_MDT &&
	    !(lsd->lsd_feature_compat & OBD_COMPAT_20)) {
		if (last_rcvd_size > lsd->lsd_client_start) {
			LCONSOLE_WARN("%s: mounting at first time on 1.8 FS, "
				      "remove all clients for interop needs\n",
				      tgt_name(tgt));
			rc = tgt_truncate_last_rcvd(env, tgt,
						    lsd->lsd_client_start);
			if (rc)
				RETURN(rc);
			last_rcvd_size = lsd->lsd_client_start;
		}
		/** set 2.0 flag to upgrade/downgrade between 1.8 and 2.0 */
		lsd->lsd_feature_compat |= OBD_COMPAT_20;
	}

	spin_lock(&tgt->lut_translock);
	tgt->lut_last_transno = lsd->lsd_last_transno;
	spin_unlock(&tgt->lut_translock);

	lsd->lsd_mount_count++;

	CDEBUG(D_INODE, "=======,=BEGIN DUMPING LAST_RCVD========\n");
	CDEBUG(D_INODE, "%s: server last_transno: "LPU64"\n",
	       tgt_name(tgt), tgt->lut_last_transno);
	CDEBUG(D_INODE, "%s: server mount_count: "LPU64"\n",
	       tgt_name(tgt), lsd->lsd_mount_count);
	CDEBUG(D_INODE, "%s: server data size: %u\n",
	       tgt_name(tgt), lsd->lsd_server_size);
	CDEBUG(D_INODE, "%s: per-client data start: %u\n",
	       tgt_name(tgt), lsd->lsd_client_start);
	CDEBUG(D_INODE, "%s: per-client data size: %u\n",
	       tgt_name(tgt), lsd->lsd_client_size);
	CDEBUG(D_INODE, "%s: last_rcvd size: %lu\n",
	       tgt_name(tgt), last_rcvd_size);
	CDEBUG(D_INODE, "%s: server subdir_count: %u\n",
	       tgt_name(tgt), lsd->lsd_subdir_count);
	CDEBUG(D_INODE, "%s: last_rcvd clients: %lu\n", tgt_name(tgt),
	       last_rcvd_size <= lsd->lsd_client_start ? 0 :
	       (last_rcvd_size - lsd->lsd_client_start) /
		lsd->lsd_client_size);
	CDEBUG(D_INODE, "========END DUMPING LAST_RCVD========\n");

	if (lsd->lsd_server_size == 0 || lsd->lsd_client_start == 0 ||
	    lsd->lsd_client_size == 0) {
		CERROR("%s: bad last_rcvd contents!\n", tgt_name(tgt));
		RETURN(-EINVAL);
	}

	if (!tgt->lut_obd->obd_replayable)
		CWARN("%s: recovery support OFF\n", tgt_name(tgt));

	rc = tgt_clients_data_init(env, tgt, last_rcvd_size);
	if (rc < 0)
		GOTO(err_client, rc);

	spin_lock(&tgt->lut_translock);
	/* obd_last_committed is used for compatibility
	 * with other lustre recovery code */
	tgt->lut_obd->obd_last_committed = tgt->lut_last_transno;
	spin_unlock(&tgt->lut_translock);

	tgt->lut_obd->u.obt.obt_mount_count = lsd->lsd_mount_count;
	tgt->lut_obd->u.obt.obt_instance = (__u32)lsd->lsd_mount_count;

	/* save it, so mount count and last_transno is current */
	rc = tgt_server_data_update(env, tgt, 0);
	if (rc < 0)
		GOTO(err_client, rc);

	RETURN(0);

err_client:
	class_disconnect_exports(tgt->lut_obd);
	return rc;
}

/* add credits for last_rcvd update */
int tgt_txn_start_cb(const struct lu_env *env, struct thandle *th,
		     void *cookie)
{
	struct lu_target	*tgt = cookie;
	struct tgt_session_info	*tsi;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	int			 rc;

	/* if there is no session, then this transaction is not result of
	 * request processing but some local operation */
	if (env->le_ses == NULL)
		return 0;

	LASSERT(tgt->lut_last_rcvd);
	tsi = tgt_ses_info(env);
	/* OFD may start transaction without export assigned */
	if (tsi->tsi_exp == NULL)
		return 0;

	tti_buf_lcd(tti);
	rc = dt_declare_record_write(env, tgt->lut_last_rcvd,
				     &tti->tti_buf,
				     tsi->tsi_exp->exp_target_data.ted_lr_off,
				     th);
	if (rc)
		return rc;

	tti_buf_lsd(tti);
	rc = dt_declare_record_write(env, tgt->lut_last_rcvd,
				     &tti->tti_buf, 0, th);
	if (rc)
		return rc;

	if (tsi->tsi_vbr_obj != NULL &&
	    !lu_object_remote(&tsi->tsi_vbr_obj->do_lu))
		rc = dt_declare_version_set(env, tsi->tsi_vbr_obj, th);

	return rc;
}

/* Update last_rcvd records with latests transaction data */
int tgt_txn_stop_cb(const struct lu_env *env, struct thandle *th,
		    void *cookie)
{
	struct lu_target	*tgt = cookie;
	struct tgt_session_info	*tsi;
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct dt_object	*obj = NULL;
	int			 rc;
	bool			 echo_client;

	if (env->le_ses == NULL)
		return 0;

	tsi = tgt_ses_info(env);
	/* OFD may start transaction without export assigned */
	if (tsi->tsi_exp == NULL)
		return 0;

	echo_client = (tgt_ses_req(tsi) == NULL);

	if (tti->tti_has_trans && !echo_client) {
		if (tti->tti_mult_trans == 0) {
			CDEBUG(D_HA, "More than one transaction "LPU64"\n",
			       tti->tti_transno);
			RETURN(0);
		}
		/* we need another transno to be assigned */
		tti->tti_transno = 0;
	} else if (th->th_result == 0) {
		tti->tti_has_trans = 1;
	}

	if (tsi->tsi_vbr_obj != NULL &&
	    !lu_object_remote(&tsi->tsi_vbr_obj->do_lu)) {
		obj = tsi->tsi_vbr_obj;
	}

	if (unlikely(echo_client)) /* echo client special case */
		rc = tgt_last_rcvd_update_echo(env, tgt, obj, th,
					       tsi->tsi_exp);
	else
		rc = tgt_last_rcvd_update(env, tgt, obj, tsi->tsi_opdata, th,
					  tgt_ses_req(tsi));
	return rc;
}

int tgt_handle_repack(struct obd_export *exp, __u64 xid)
{
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*lut = class_exp2tgt(exp);
	struct tg_reply_data	*trd, *tmp;

	LASSERT(xid != 0);
	if (ted->ted_slots == 0)
		return 0;

	mutex_lock(&ted->ted_lcd_lock);
	list_for_each_entry_safe(trd, tmp, &ted->ted_reply_list, trd_list) {
		if (trd->trd_reply.lrd_xid != xid)
			continue;

		ted->ted_slots_acked++;
		tgt_release_reply_data(lut, ted, trd);
		break;
	}
	mutex_unlock(&ted->ted_lcd_lock);

	return 0;
}
EXPORT_SYMBOL(tgt_handle_repack);

struct lsd_reply_data *tgt_lookup_reply(struct ptlrpc_request *req)
{
	struct tg_export_data	*ted = &req->rq_export->exp_target_data;
	struct tg_reply_data	*trd, *tmp;
	struct lsd_reply_data	*lrd = NULL;
	int			 tag;

	tag = lustre_msg_get_tag(req->rq_reqmsg);

	mutex_lock(&ted->ted_lcd_lock);
	list_for_each_entry_safe(trd, tmp, &ted->ted_reply_list, trd_list) {
		if (trd->trd_reply.lrd_xid == req->rq_xid) {
			lrd = &trd->trd_reply;
			/* XXX: assert? can be an on-disk corruption? */
			LASSERT(lrd->lrd_tag == tag);
			break;
		}
	}
	mutex_unlock(&ted->ted_lcd_lock);

	return lrd;
}
EXPORT_SYMBOL(tgt_lookup_reply);

int tgt_reply_log_clear(const struct lu_env *env, struct lu_target *tgt,
			struct tg_export_data *ted)
{
	struct tgt_thread_info	*tti = tgt_th_info(env);
	struct tg_reply_data	*trd, *tmp;
	struct lu_buf		*buf = &tti->tti_buf;
	struct thandle		*th;
	loff_t			 off;
	struct list_head	list;
	int			 rc, rc2 = 0;
	ENTRY;

	INIT_LIST_HEAD(&list);
	mutex_lock(&ted->ted_lcd_lock);
	list_splice_init(&ted->ted_reply_list, &list);
	mutex_unlock(&ted->ted_lcd_lock);

	/* XXX: another option is to mark every record with unique id
	 *	(stored in last_rcvd as well), so we can match them on
	 *	boot and discard stale records */
	list_for_each_entry_safe(trd, tmp, &list, trd_list) {

		CDEBUG(D_OTHER, "drop xid %llu t%llu\n",
			trd->trd_reply.lrd_xid, trd->trd_reply.lrd_transno);
		memset(&trd->trd_reply, 0, sizeof(trd->trd_reply));
		buf->lb_buf = &trd->trd_reply;
		buf->lb_len = sizeof(struct lsd_reply_data);
		off = trd->trd_index * sizeof(struct lsd_reply_data);

		th = dt_trans_create(env, tgt->lut_bottom);
		if (IS_ERR(th))
			RETURN(PTR_ERR(th));

		rc = dt_declare_record_write(env, tgt->lut_reply_log,
					     buf, off, th);

		rc = dt_trans_start_local(env, tgt->lut_bottom, th);
		if (rc == 0) {
			rc = tgt_reply_data_write(env, tgt, &trd->trd_reply,
						  off, th);
			dt_trans_stop(env, tgt->lut_bottom, th);
		}
		if (rc2 == 0)
			rc2 = rc;

		tgt_free_reply_data(tgt, ted, trd);
	}

	RETURN(rc2);
}

int tgt_handle_tag(struct ptlrpc_request *req)
{
	struct obd_export	*exp = req->rq_export;
	struct tg_export_data	*ted = &exp->exp_target_data;
	struct lu_target	*lut = class_exp2tgt(exp);
	struct tg_reply_data	*trd, *tmp;
	int			 tag;

	LASSERT(req->rq_reqmsg);

	/* resent to specific slot shouldn't discard the slot's content */
	if (unlikely(lustre_msg_get_flags(req->rq_reqmsg) & MSG_RESENT))
		return 0;
	tag = lustre_msg_get_tag(req->rq_reqmsg);
	if (tag == 0)
		return 0;
	if (unlikely(tag > req->rq_export->exp_connect_data.ocd_maxslots))
		return -EINVAL;

	mutex_lock(&ted->ted_lcd_lock);
	list_for_each_entry_safe(trd, tmp, &ted->ted_reply_list, trd_list) {
		if (trd->trd_reply.lrd_tag != tag)
			continue;

		ted->ted_slots_reused++;
		tgt_release_reply_data(lut, ted, trd);
		break;
	}
	mutex_unlock(&ted->ted_lcd_lock);

	return 0;
}
