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
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ofd/ofd_io.c
 *
 * Author: Alex Tomas <bzzz@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_FILTER

#include <lu_target.h>
#include "ofd_internal.h"

struct ofd_inconsistency_item {
	struct list_head	 oii_list;
	struct work_struct	 oii_work;
	const struct lu_env	*oii_env;
	struct ofd_device	*oii_dev;
	struct ofd_object	*oii_obj;
};

static int ofd_pfid_set(const struct lu_env *env,
			struct ofd_object *fo,
			struct filter_fid *ff)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct ofd_device	*ofd  = ofd_obj2dev(fo);
	struct thandle		*th;
	int			 rc;
	ENTRY;

	ofd_write_lock(env, fo);
	if (!ofd_object_exists(fo))
		GOTO(unlock, rc = -ENOENT);

	th = ofd_trans_create(env, ofd);
	if (IS_ERR(th))
		GOTO(unlock, rc = PTR_ERR(th));

	info->fti_buf.lb_buf = ff;
	info->fti_buf.lb_len = sizeof(*ff);
	rc = dt_declare_xattr_set(env, ofd_object_child(fo), &info->fti_buf,
				  XATTR_NAME_FID, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = ofd_trans_start(env, ofd, NULL, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_xattr_set(env, ofd_object_child(fo), &info->fti_buf,
			  XATTR_NAME_FID, 0, th, BYPASS_CAPA);

	GOTO(stop, rc);

stop:
	ofd_trans_stop(env, ofd, th, rc);
unlock:
	ofd_write_unlock(env, fo);

	return rc;
}

static void ofd_inconsistency_self_cure(struct work_struct *work)
{
	struct ofd_inconsistency_item	*oii	=
		container_of(work, struct ofd_inconsistency_item, oii_work);
	const struct lu_env		*env	= oii->oii_env;
	struct ofd_device		*ofd	= oii->oii_dev;
	struct ofd_object		*fo	= oii->oii_obj;
	struct filter_fid		*ff	= &ofd_info(env)->fti_mds_fid;
	int				 rc;

	LASSERT(fo->ofo_pfid_inconsistent);

	fid_cpu_to_le(&ff->ff_parent, &fo->ofo_pfid);
	rc = ofd_pfid_set(env, fo, ff);
	if (rc == 0)
		atomic_inc(&ofd->ofd_inconsistency_self_repaired);
	else
		CDEBUG(D_LFSCK, "%s: fail to self cure inconsistency for "
		       DFID" to "DFID", rc = %d\n", ofd_obd(ofd)->obd_name,
		       PFID(lu_object_fid(&fo->ofo_obj.do_lu)),
		       PFID(&fo->ofo_pfid), rc);

	spin_lock(&fo->ofo_lock);
	/* Clear the ofo_pfid_inconsistent even if failed to fix the parent
	 * FID. Otherwise, others can not fix it neither next time. */
	fo->ofo_pfid_inconsistent = 0;
	spin_unlock(&fo->ofo_lock);

	lu_object_put(env, &fo->ofo_obj.do_lu);
	OBD_FREE_PTR(oii);
}

static void ofd_add_inconsistency_item(const struct lu_env *env,
				       struct ofd_object *fo, struct obdo *oa)
{
	struct ofd_device		*ofd	= ofd_obj2dev(fo);
	struct lu_fid			*pfid	= &fo->ofo_pfid;
	struct ofd_inconsistency_item	*oii;

	spin_lock(&fo->ofo_lock);
	pfid->f_seq = oa->o_parent_seq;
	pfid->f_oid = oa->o_parent_oid;
	/* XXX: In fact, the ff_parent::f_ver is not the real parent
	 *	FID::f_ver, instead, it is the OST-object index in
	 *	its parent MDT-object layout EA. */
	pfid->f_ver = oa->o_stripe_idx;

	if (fo->ofo_pfid_inconsistent) {
		spin_unlock(&fo->ofo_lock);

		return;
	}

	fo->ofo_pfid_inconsistent = 1;
	spin_unlock(&fo->ofo_lock);

	OBD_ALLOC_PTR(oii);
	if (unlikely(oii == NULL)) {
		CERROR("%s: cannot alloc memory to repair OST "
		       "inconsistency for "DFID"\n",
		       ofd_obd(ofd)->obd_name, PFID(&fo->ofo_pfid));
		spin_lock(&fo->ofo_lock);
		fo->ofo_pfid_inconsistent = 0;
		spin_unlock(&fo->ofo_lock);

		return;
	}

	INIT_LIST_HEAD(&oii->oii_list);
	oii->oii_env = env;
	oii->oii_dev = ofd;
	lu_object_get(&fo->ofo_obj.do_lu);
	oii->oii_obj = fo;
	INIT_WORK(&oii->oii_work, ofd_inconsistency_self_cure);

	atomic_inc(&ofd->ofd_inconsistency_self_detected);
	queue_work(ofd->ofd_inconsistency_wq, &oii->oii_work);

	/* XXX: When the found inconsistency exceeds some threshold,
	 *	we can trigger the LFSCK to scan part of the system
	 *	or the whole system, which depends on how to define
	 *	the threshold, a simple way maybe like that: define
	 *	the absolute value of how many inconsisteny allowed
	 *	to be repaired via self detect/repair mechanism, if
	 *	exceeded, then trigger the LFSCK to scan the layout
	 *	inconsistency within the whole system. */
}

static int ofd_fld_lookup(const struct lu_env *env, struct ofd_device *ofd,
			  obd_seq seq, struct lu_seq_range *range)
{
	int rc;

	if (fid_seq_is_igif(seq)) {
		fld_range_set_mdt(range);
		range->lsr_index = 0;

		return 0;
	}

	if (fid_seq_is_idif(seq)) {
		fld_range_set_ost(range);
		range->lsr_index = idif_ost_idx(seq);

		return 0;
	}

	fld_range_set_any(range);
	rc = fld_server_lookup(env, ofd->ofd_seq_site.ss_server_fld, seq,
			       range);

	return rc;
}

/*
 * \ret > 0: unrecognized stripe
 * \ret ==0: recognized stripe
 * \ret < 0: other failures
 */
static int ofd_lwp_check_stripe(const struct lu_env *env,
				struct obd_export *exp, struct ofd_device *ofd,
				struct obdo *oa)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct lu_buf		*lbuf = &info->fti_buf;
	struct lu_fid		*fid  = &info->fti_fid;
	struct ost_id		*oi   = &info->fti_ostid;
	struct update_request	*update;
	struct ptlrpc_request	*req  = NULL;
	struct update_reply	*reply;
	struct lov_mds_md_v1	*lmm  = NULL;
	struct lov_ost_data_v1	*objs;
	const char		*name = XATTR_NAME_LOV;
	__u32			 magic;
	__u32			 patten;
	int			 size = strlen(name);
	int			 rc;
	int			 i;
	__u16			 count;
	ENTRY;

	fid->f_seq = oa->o_parent_seq;
	fid->f_oid = oa->o_parent_oid;
	fid->f_ver = oa->o_parent_ver;

	update = out_create_update_req(&ofd->ofd_dt_dev);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	rc = out_insert_update(env, update, OBJ_XATTR_GET, fid, 1,
			       &size, &name);
	if (rc != 0)
		GOTO(out, rc);

	rc = out_remote_sync(env, class_exp2cliimp(exp), update, &req);
	if (rc != 0)
		GOTO(out, rc);

	reply = req_capsule_server_sized_get(&req->rq_pill, &RMF_UPDATE_REPLY,
					     UPDATE_BUFFER_SIZE);
	if (reply == NULL || reply->ur_version != UPDATE_REPLY_V1)
		GOTO(out, rc = -EPROTO);

	rc = update_get_reply_buf(reply, lbuf, 0);
	if (rc < 0)
		GOTO(out, rc = (rc == -ENODATA ? 1 : rc));

	lmm = lbuf->lb_buf;
	if (lmm == NULL)
		GOTO(out, rc = -EFAULT);

	magic = le32_to_cpu(lmm->lmm_magic);
	if (magic != LOV_MAGIC_V1 && magic != LOV_MAGIC_V3)
		GOTO(out, rc = -EINVAL);

	patten = le32_to_cpu(lmm->lmm_pattern);
	/* XXX: currently, we only support LOV_PATTERN_RAID0. */
	if (patten != LOV_PATTERN_RAID0)
		GOTO(out, rc = -EOPNOTSUPP);

	if (magic == LOV_MAGIC_V1)
		objs = &(lmm->lmm_objects[0]);
	else
		objs = &((struct lov_mds_md_v3 *)lmm)->lmm_objects[0];

	count = le16_to_cpu(lmm->lmm_stripe_count);
	fid_to_ostid(&oa->o_oi.oi_fid, oi);
	for (i = 0; i < count; i++, objs++) {
		struct ost_id *oi2 = &info->fti_ostid2;

		ostid_le_to_cpu(&objs->l_ost_oi, oi2);
		if (memcmp(oi, oi2, sizeof(*oi)) == 0) {
			if (i == oa->o_stripe_idx)
				GOTO(out, rc = 0);
			else
				GOTO(out, rc = 1);
		}
	}

	GOTO(out, rc = 1);

out:
	ptlrpc_req_finished(req);
	out_destroy_update_req(update);

	return rc;
}

static int ofd_verify_ff(const struct lu_env *env, struct ofd_object *fo,
			 struct obdo *oa, bool write)
{
	struct lu_fid		*pfid	= &fo->ofo_pfid;
	struct ofd_device	*ofd	= ofd_obj2dev(fo);
	struct lu_seq_range	*range	= &ofd_info(env)->fti_range;
	struct obd_export	*exp;
	int			 rc	= 0;
	ENTRY;

	if (fid_is_sane(pfid)) {
		if (likely(oa->o_parent_seq == pfid->f_seq &&
			   oa->o_parent_oid == pfid->f_oid &&
			   oa->o_stripe_idx == pfid->f_ver))
			RETURN(0);
	}

	/* Do not overwrite the MDS-side parent FID with local xattr. */
	if (!fo->ofo_pfid_inconsistent) {
		rc = ofd_object_ff_check(env, fo, true);
		if (rc == -ENODATA)
			RETURN(0);

		if (rc < 0)
			RETURN(rc);

		if (likely(oa->o_parent_seq == pfid->f_seq &&
			   oa->o_parent_oid == pfid->f_oid &&
			   oa->o_stripe_idx == pfid->f_ver))
			RETURN(0);
	}

	rc = ofd_fld_lookup(env, ofd, oa->o_parent_seq, range);
	if (rc != 0)
		RETURN(rc);

	if (unlikely(!fld_range_is_mdt(range)))
		RETURN(-EPERM);

	exp = lustre_find_lwp_by_index(ofd_obd(ofd)->obd_name,
				       range->lsr_index);
	if (unlikely(exp == NULL))
		RETURN(-EPERM);

	rc = ofd_lwp_check_stripe(env, exp, ofd, oa);
	class_export_put(exp);
	if (rc > 0)
		RETURN(-EPERM);

	if (rc < 0)
		RETURN(rc);

	/* The client given parent FID is correct, need to fix
	 * server-side inconsistency via a dedicated thread. */
	ofd_add_inconsistency_item(env, fo, oa);

	RETURN(0);
}

static int ofd_preprw_read(const struct lu_env *env, struct obd_export *exp,
			   struct ofd_device *ofd, struct lu_fid *fid,
			   struct lu_attr *la, struct obdo *oa, int niocount,
			   struct niobuf_remote *rnb, int *nr_local,
			   struct niobuf_local *lnb, char *jobid)
{
	struct ofd_object	*fo;
	int			 i, j, rc, tot_bytes = 0;

	ENTRY;
	LASSERT(env != NULL);

	fo = ofd_object_find(env, ofd, fid);
	if (IS_ERR(fo))
		RETURN(PTR_ERR(fo));
	LASSERT(fo != NULL);

	ofd_read_lock(env, fo);
	if (!ofd_object_exists(fo))
		GOTO(unlock, rc = -ENOENT);

	if (ofd->ofd_fail_on_inconsistency && oa->o_valid & OBD_MD_FLFID) {
		rc = ofd_verify_ff(env, fo, oa, false);
		if (rc != 0)
			GOTO(unlock, rc = -EPERM);
	}

	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < niocount; i++) {
		rc = dt_bufs_get(env, ofd_object_child(fo), rnb + i,
				 lnb + j, 0, ofd_object_capa(env, fo));
		if (unlikely(rc < 0))
			GOTO(buf_put, rc);
		LASSERT(rc <= PTLRPC_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		j += rc;
		*nr_local += rc;
		LASSERT(j <= PTLRPC_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}

	LASSERT(*nr_local > 0 && *nr_local <= PTLRPC_MAX_BRW_PAGES);
	rc = dt_attr_get(env, ofd_object_child(fo), la,
			 ofd_object_capa(env, fo));
	if (unlikely(rc))
		GOTO(buf_put, rc);

	rc = dt_read_prep(env, ofd_object_child(fo), lnb, *nr_local);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	ofd_counter_incr(exp, LPROC_OFD_STATS_READ, jobid, tot_bytes);
	RETURN(0);

buf_put:
	dt_bufs_put(env, ofd_object_child(fo), lnb, *nr_local);
unlock:
	ofd_read_unlock(env, fo);
	ofd_object_put(env, fo);
	return rc;
}

static int ofd_preprw_write(const struct lu_env *env, struct obd_export *exp,
			    struct ofd_device *ofd, struct lu_fid *fid,
			    struct lu_attr *la, struct obdo *oa,
			    int objcount, struct obd_ioobj *obj,
			    struct niobuf_remote *rnb, int *nr_local,
			    struct niobuf_local *lnb, char *jobid)
{
	struct ofd_object	*fo;
	int			 i, j, k, rc = 0, tot_bytes = 0;

	ENTRY;
	LASSERT(env != NULL);
	LASSERT(objcount == 1);

	if (unlikely(exp->exp_obd->obd_recovering)) {
		struct ofd_thread_info *info = ofd_info(env);

		/* copied from ofd_precreate_object */
		/* XXX this should be consolidated to use the same code
		 *     instead of a copy, due to the ongoing risk of bugs. */
		memset(&info->fti_attr, 0, sizeof(info->fti_attr));
		info->fti_attr.la_valid = LA_TYPE | LA_MODE;
		info->fti_attr.la_mode = S_IFREG | S_ISUID | S_ISGID | 0666;
		info->fti_attr.la_valid |= LA_ATIME | LA_MTIME | LA_CTIME;
		/* Initialize a/c/m time so any client timestamp will always
		 * be newer and update the inode. ctime = 0 is also handled
		 * specially in osd_inode_setattr().  See LU-221, LU-1042 */
		info->fti_attr.la_atime = 0;
		info->fti_attr.la_mtime = 0;
		info->fti_attr.la_ctime = 0;

		fo = ofd_object_find_or_create(env, ofd, fid, &info->fti_attr);
	} else {
		fo = ofd_object_find(env, ofd, fid);
	}

	if (IS_ERR(fo))
		GOTO(out, rc = PTR_ERR(fo));
	LASSERT(fo != NULL);

	ofd_read_lock(env, fo);
	if (!ofd_object_exists(fo)) {
		CERROR("%s: BRW to missing obj "DOSTID"\n",
		       exp->exp_obd->obd_name, POSTID(&obj->ioo_oid));
		ofd_read_unlock(env, fo);
		ofd_object_put(env, fo);
		GOTO(out, rc = -ENOENT);
	}

	if (ofd->ofd_fail_on_inconsistency && oa->o_valid & OBD_MD_FLFID) {
		rc = ofd_verify_ff(env, fo, oa, true);
		if (rc != 0) {
			ofd_read_unlock(env, fo);
			ofd_object_put(env, fo);
			GOTO(out, rc = -EPERM);
		}
	}

	/* Process incoming grant info, set OBD_BRW_GRANTED flag and grant some
	 * space back if possible */
	ofd_grant_prepare_write(env, exp, oa, rnb, obj->ioo_bufcnt);

	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < obj->ioo_bufcnt; i++) {
		rc = dt_bufs_get(env, ofd_object_child(fo),
				 rnb + i, lnb + j, 1,
				 ofd_object_capa(env, fo));
		if (unlikely(rc < 0))
			GOTO(err, rc);
		LASSERT(rc <= PTLRPC_MAX_BRW_PAGES);
		/* correct index for local buffers to continue with */
		for (k = 0; k < rc; k++) {
			lnb[j+k].lnb_flags = rnb[i].rnb_flags;
			if (!(rnb[i].rnb_flags & OBD_BRW_GRANTED))
				lnb[j+k].lnb_rc = -ENOSPC;

			/* remote client can't break through quota */
			if (exp_connect_rmtclient(exp))
				lnb[j+k].lnb_flags &= ~OBD_BRW_NOQUOTA;
		}
		j += rc;
		*nr_local += rc;
		LASSERT(j <= PTLRPC_MAX_BRW_PAGES);
		tot_bytes += rnb[i].rnb_len;
	}
	LASSERT(*nr_local > 0 && *nr_local <= PTLRPC_MAX_BRW_PAGES);

	rc = dt_write_prep(env, ofd_object_child(fo), lnb, *nr_local);
	if (unlikely(rc != 0))
		GOTO(err, rc);

	ofd_counter_incr(exp, LPROC_OFD_STATS_WRITE, jobid, tot_bytes);
	RETURN(0);
err:
	dt_bufs_put(env, ofd_object_child(fo), lnb, *nr_local);
	ofd_read_unlock(env, fo);
	/* ofd_grant_prepare_write() was called, so we must commit */
	ofd_grant_commit(env, exp, rc);
out:
	/* let's still process incoming grant information packed in the oa,
	 * but without enforcing grant since we won't proceed with the write.
	 * Just like a read request actually. */
	ofd_grant_prepare_read(env, exp, oa);
	return rc;
}

int ofd_preprw(const struct lu_env *env, int cmd, struct obd_export *exp,
	       struct obdo *oa, int objcount, struct obd_ioobj *obj,
	       struct niobuf_remote *rnb, int *nr_local,
	       struct niobuf_local *lnb, struct obd_trans_info *oti,
	       struct lustre_capa *capa)
{
	struct tgt_session_info	*tsi = tgt_ses_info(env);
	struct ofd_device	*ofd = ofd_exp(exp);
	struct ofd_thread_info	*info;
	char			*jobid;
	int			 rc = 0;

	if (*nr_local > PTLRPC_MAX_BRW_PAGES) {
		CERROR("%s: bulk has too many pages %d, which exceeds the"
		       "maximum pages per RPC of %d\n",
		       exp->exp_obd->obd_name, *nr_local, PTLRPC_MAX_BRW_PAGES);
		RETURN(-EPROTO);
	}

	if (tgt_ses_req(tsi) == NULL) { /* echo client case */
		LASSERT(oti != NULL);
		lu_env_refill((struct lu_env *)env);
		info = ofd_info_init(env, exp);
		ofd_oti2info(info, oti);
		jobid = oti->oti_jobid;
	} else {
		info = tsi2ofd_info(tsi);
		jobid = tsi->tsi_jobid;
	}

	LASSERT(oa != NULL);

	if (OBD_FAIL_CHECK(OBD_FAIL_OST_ENOENT)) {
		struct ofd_seq		*oseq;

		oseq = ofd_seq_load(env, ofd, ostid_seq(&oa->o_oi));
		if (IS_ERR(oseq)) {
			CERROR("%s: Can not find seq for "DOSTID
			       ": rc = %ld\n", ofd_name(ofd), POSTID(&oa->o_oi),
			       PTR_ERR(oseq));
			RETURN(-EINVAL);
		}

		if (oseq->os_destroys_in_progress == 0) {
			/* don't fail lookups for orphan recovery, it causes
			 * later LBUGs when objects still exist during
			 * precreate */
			ofd_seq_put(env, oseq);
			RETURN(-ENOENT);
		}
		ofd_seq_put(env, oseq);
	}

	LASSERT(objcount == 1);
	LASSERT(obj->ioo_bufcnt > 0);

	info->fti_fid = oa->o_oi.oi_fid;
	if (cmd == OBD_BRW_WRITE) {
		rc = ofd_auth_capa(exp, &info->fti_fid, ostid_seq(&oa->o_oi),
				   capa, CAPA_OPC_OSS_WRITE);
		if (rc == 0) {
			la_from_obdo(&info->fti_attr, oa, OBD_MD_FLGETATTR);
			rc = ofd_preprw_write(env, exp, ofd, &info->fti_fid,
					      &info->fti_attr, oa, objcount,
					      obj, rnb, nr_local, lnb, jobid);
		}
	} else if (cmd == OBD_BRW_READ) {
		rc = ofd_auth_capa(exp, &info->fti_fid, ostid_seq(&oa->o_oi),
				   capa, CAPA_OPC_OSS_READ);
		if (rc == 0) {
			ofd_grant_prepare_read(env, exp, oa);
			rc = ofd_preprw_read(env, exp, ofd, &info->fti_fid,
					     &info->fti_attr, oa,
					     obj->ioo_bufcnt, rnb, nr_local,
					     lnb, jobid);
			obdo_from_la(oa, &info->fti_attr, LA_ATIME);
		}
	} else {
		CERROR("%s: wrong cmd %d received!\n",
		       exp->exp_obd->obd_name, cmd);
		rc = -EPROTO;
	}
	RETURN(rc);
}

static int
ofd_commitrw_read(const struct lu_env *env, struct ofd_device *ofd,
		  struct lu_fid *fid, int objcount, int niocount,
		  struct niobuf_local *lnb)
{
	struct ofd_object *fo;

	ENTRY;

	LASSERT(niocount > 0);

	fo = ofd_object_find(env, ofd, fid);
	if (IS_ERR(fo))
		RETURN(PTR_ERR(fo));
	LASSERT(fo != NULL);
	LASSERT(ofd_object_exists(fo));
	dt_bufs_put(env, ofd_object_child(fo), lnb, niocount);

	ofd_read_unlock(env, fo);
	ofd_object_put(env, fo);
	/* second put is pair to object_get in ofd_preprw_read */
	ofd_object_put(env, fo);

	RETURN(0);
}

static int
ofd_write_attr_set(const struct lu_env *env, struct ofd_device *ofd,
		   struct ofd_object *ofd_obj, struct lu_attr *la,
		   struct filter_fid *ff)
{
	struct ofd_thread_info	*info = ofd_info(env);
	__u64			 valid = la->la_valid;
	int			 rc;
	struct thandle		*th;
	struct dt_object	*dt_obj;
	int			 ff_needed = 0;

	ENTRY;

	LASSERT(la);

	dt_obj = ofd_object_child(ofd_obj);
	LASSERT(dt_obj != NULL);

	la->la_valid &= LA_UID | LA_GID;

	rc = ofd_attr_handle_ugid(env, ofd_obj, la, 0 /* !is_setattr */);
	if (rc != 0)
		GOTO(out, rc);

	if (ff != NULL) {
		rc = ofd_object_ff_check(env, ofd_obj, false);
		if (rc == -ENODATA)
			ff_needed = 1;
		else if (rc < 0)
			GOTO(out, rc);
	}

	if (!la->la_valid && !ff_needed)
		/* no attributes to set */
		GOTO(out, rc = 0);

	th = ofd_trans_create(env, ofd);
	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	if (la->la_valid) {
		rc = dt_declare_attr_set(env, dt_obj, la, th);
		if (rc)
			GOTO(out_tx, rc);
	}

	if (ff_needed) {
		if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_UNMATCHED_PAIR1))
			ff->ff_parent.f_oid = cpu_to_le32(1UL << 31);
		if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_UNMATCHED_PAIR2))
			ff->ff_parent.f_oid =
			cpu_to_le32(le32_to_cpu(ff->ff_parent.f_oid) - 1);

		info->fti_buf.lb_buf = ff;
		info->fti_buf.lb_len = sizeof(*ff);
		rc = dt_declare_xattr_set(env, dt_obj, &info->fti_buf,
					  XATTR_NAME_FID, 0, th);
		if (rc)
			GOTO(out_tx, rc);
	}

	/* We don't need a transno for this operation which will be re-executed
	 * anyway when the OST_WRITE (with a transno assigned) is replayed */
	rc = dt_trans_start_local(env, ofd->ofd_osd , th);
	if (rc)
		GOTO(out_tx, rc);

	/* set uid/gid */
	if (la->la_valid) {
		rc = dt_attr_set(env, dt_obj, la, th,
				 ofd_object_capa(env, ofd_obj));
		if (rc)
			GOTO(out_tx, rc);
	}

	/* set filter fid EA */
	if (ff_needed) {
		rc = dt_xattr_set(env, dt_obj, &info->fti_buf, XATTR_NAME_FID,
				  0, th, BYPASS_CAPA);
		if (rc == 0) {
			ofd_obj->ofo_pfid.f_seq = le64_to_cpu(ff->ff_parent.f_seq);
			ofd_obj->ofo_pfid.f_oid = le32_to_cpu(ff->ff_parent.f_oid);
			ofd_obj->ofo_pfid.f_ver = le32_to_cpu(ff->ff_parent.f_ver);
		}
	}

	GOTO(out_tx, rc);

out_tx:
	dt_trans_stop(env, ofd->ofd_osd, th);
out:
	la->la_valid = valid;
	return rc;
}

struct ofd_soft_sync_callback {
	struct dt_txn_commit_cb	 ossc_cb;
	struct obd_export	*ossc_exp;
};

static void ofd_cb_soft_sync(struct lu_env *env, struct thandle *th,
			     struct dt_txn_commit_cb *cb, int err)
{
	struct ofd_soft_sync_callback	*ossc;

	ossc = container_of(cb, struct ofd_soft_sync_callback, ossc_cb);

	CDEBUG(D_INODE, "export %p soft sync count is reset\n", ossc->ossc_exp);
	atomic_set(&ossc->ossc_exp->exp_filter_data.fed_soft_sync_count, 0);

	class_export_cb_put(ossc->ossc_exp);
	OBD_FREE_PTR(ossc);
}

static int ofd_soft_sync_cb_add(struct thandle *th, struct obd_export *exp)
{
	struct ofd_soft_sync_callback		*ossc;
	struct dt_txn_commit_cb			*dcb;
	int					 rc;

	OBD_ALLOC_PTR(ossc);
	if (ossc == NULL)
		return -ENOMEM;

	ossc->ossc_exp = class_export_cb_get(exp);

	dcb = &ossc->ossc_cb;
	dcb->dcb_func = ofd_cb_soft_sync;
	CFS_INIT_LIST_HEAD(&dcb->dcb_linkage);
	strncpy(dcb->dcb_name, "ofd_cb_soft_sync", MAX_COMMIT_CB_STR_LEN);
	dcb->dcb_name[MAX_COMMIT_CB_STR_LEN - 1] = '\0';

	rc = dt_trans_cb_add(th, dcb);
	if (rc) {
		class_export_cb_put(exp);
		OBD_FREE_PTR(ossc);
	}

	return rc;
}

static int
ofd_commitrw_write(const struct lu_env *env, struct obd_export *exp,
		   struct ofd_device *ofd, struct lu_fid *fid,
		   struct lu_attr *la, struct filter_fid *ff, int objcount,
		   int niocount, struct niobuf_local *lnb, int old_rc)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct ofd_object	*fo;
	struct dt_object	*o;
	struct thandle		*th;
	int			 rc = 0;
	int			 retries = 0;
	int			 i;
	struct filter_export_data *fed = &exp->exp_filter_data;
	bool			 soft_sync = false;
	bool			 cb_registered = false;

	ENTRY;

	LASSERT(objcount == 1);

	fo = ofd_object_find(env, ofd, fid);
	LASSERT(fo != NULL);
	LASSERT(ofd_object_exists(fo));

	o = ofd_object_child(fo);
	LASSERT(o != NULL);

	if (old_rc)
		GOTO(out, rc = old_rc);

	/*
	 * The first write to each object must set some attributes.  It is
	 * important to set the uid/gid before calling
	 * dt_declare_write_commit() since quota enforcement is now handled in
	 * declare phases.
	 */
	rc = ofd_write_attr_set(env, ofd, fo, la, ff);
	if (rc)
		GOTO(out, rc);

	la->la_valid &= LA_ATIME | LA_MTIME | LA_CTIME;

retry:
	th = ofd_trans_create(env, ofd);
	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	th->th_sync |= ofd->ofd_syncjournal;
	if (th->th_sync == 0) {
		for (i = 0; i < niocount; i++) {
			if (!(lnb[i].lnb_flags & OBD_BRW_ASYNC)) {
				th->th_sync = 1;
				break;
			}
			if (lnb[i].lnb_flags & OBD_BRW_SOFT_SYNC)
				soft_sync = true;
		}
	}

	if (OBD_FAIL_CHECK(OBD_FAIL_OST_DQACQ_NET))
		GOTO(out_stop, rc = -EINPROGRESS);

	rc = dt_declare_write_commit(env, o, lnb, niocount, th);
	if (rc)
		GOTO(out_stop, rc);

	if (la->la_valid) {
		/* update [mac]time if needed */
		rc = dt_declare_attr_set(env, o, la, th);
		if (rc)
			GOTO(out_stop, rc);
	}

	rc = ofd_trans_start(env, ofd, fo, th);
	if (rc)
		GOTO(out_stop, rc);

	rc = dt_write_commit(env, o, lnb, niocount, th);
	if (rc)
		GOTO(out_stop, rc);

	if (la->la_valid) {
		rc = dt_attr_set(env, o, la, th, ofd_object_capa(env, fo));
		if (rc)
			GOTO(out_stop, rc);
	}

	/* get attr to return */
	rc = dt_attr_get(env, o, la, ofd_object_capa(env, fo));

out_stop:
	/* Force commit to make the just-deleted blocks
	 * reusable. LU-456 */
	if (rc == -ENOSPC)
		th->th_sync = 1;

	/* do this before trans stop in case commit has finished */
	if (!th->th_sync && soft_sync && !cb_registered) {
		ofd_soft_sync_cb_add(th, exp);
		cb_registered = true;
	}

	ofd_trans_stop(env, ofd, th, rc);
	if (rc == -ENOSPC && retries++ < 3) {
		CDEBUG(D_INODE, "retry after force commit, retries:%d\n",
		       retries);
		goto retry;
	}

	if (!soft_sync)
		/* reset fed_soft_sync_count upon non-SOFT_SYNC RPC */
		atomic_set(&fed->fed_soft_sync_count, 0);
	else if (atomic_inc_return(&fed->fed_soft_sync_count) ==
		 ofd->ofd_soft_sync_limit)
		dt_commit_async(env, ofd->ofd_osd);

out:
	dt_bufs_put(env, o, lnb, niocount);
	ofd_read_unlock(env, fo);
	ofd_object_put(env, fo);
	/* second put is pair to object_get in ofd_preprw_write */
	ofd_object_put(env, fo);
	ofd_grant_commit(env, info->fti_exp, old_rc);
	RETURN(rc);
}

int ofd_commitrw(const struct lu_env *env, int cmd, struct obd_export *exp,
		 struct obdo *oa, int objcount, struct obd_ioobj *obj,
		 struct niobuf_remote *rnb, int npages,
		 struct niobuf_local *lnb, struct obd_trans_info *oti,
		 int old_rc)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct ofd_mod_data	*fmd;
	__u64			 valid;
	struct ofd_device	*ofd = ofd_exp(exp);
	struct filter_fid	*ff = NULL;
	int			 rc = 0;

	LASSERT(npages > 0);

	info->fti_fid = oa->o_oi.oi_fid;
	if (cmd == OBD_BRW_WRITE) {
		/* Don't update timestamps if this write is older than a
		 * setattr which modifies the timestamps. b=10150 */

		/* XXX when we start having persistent reservations this needs
		 * to be changed to ofd_fmd_get() to create the fmd if it
		 * doesn't already exist so we can store the reservation handle
		 * there. */
		valid = OBD_MD_FLUID | OBD_MD_FLGID;
		fmd = ofd_fmd_find(exp, &info->fti_fid);
		if (!fmd || fmd->fmd_mactime_xid < info->fti_xid)
			valid |= OBD_MD_FLATIME | OBD_MD_FLMTIME |
				 OBD_MD_FLCTIME;
		ofd_fmd_put(exp, fmd);
		la_from_obdo(&info->fti_attr, oa, valid);

		if (oa->o_valid & OBD_MD_FLFID) {
			ff = &info->fti_mds_fid;
			ofd_prepare_fidea(ff, oa);
		}

		rc = ofd_commitrw_write(env, exp, ofd, &info->fti_fid,
					&info->fti_attr, ff, objcount, npages,
					lnb, old_rc);
		if (rc == 0)
			obdo_from_la(oa, &info->fti_attr,
				     OFD_VALID_FLAGS | LA_GID | LA_UID);
		else
			obdo_from_la(oa, &info->fti_attr, LA_GID | LA_UID);

		/* don't report overquota flag if we failed before reaching
		 * commit */
		if (old_rc == 0 && (rc == 0 || rc == -EDQUOT)) {
			/* return the overquota flags to client */
			if (lnb[0].lnb_flags & OBD_BRW_OVER_USRQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_USRQUOTA;
				else
					oa->o_flags = OBD_FL_NO_USRQUOTA;
			}

			if (lnb[0].lnb_flags & OBD_BRW_OVER_GRPQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_GRPQUOTA;
				else
					oa->o_flags = OBD_FL_NO_GRPQUOTA;
			}

			oa->o_valid |= OBD_MD_FLFLAGS;
			oa->o_valid |= OBD_MD_FLUSRQUOTA | OBD_MD_FLGRPQUOTA;
		}
	} else if (cmd == OBD_BRW_READ) {
		struct ldlm_namespace *ns = ofd->ofd_namespace;

		/* If oa != NULL then ofd_preprw_read updated the inode
		 * atime and we should update the lvb so that other glimpses
		 * will also get the updated value. bug 5972 */
		if (oa && ns && ns->ns_lvbo && ns->ns_lvbo->lvbo_update) {
			 struct ldlm_resource *rs = NULL;

			ost_fid_build_resid(&info->fti_fid, &info->fti_resid);
			rs = ldlm_resource_get(ns, NULL, &info->fti_resid,
					       LDLM_EXTENT, 0);
			if (rs != NULL) {
				ns->ns_lvbo->lvbo_update(rs, NULL, 1);
				ldlm_resource_putref(rs);
			}
		}
		rc = ofd_commitrw_read(env, ofd, &info->fti_fid, objcount,
				       npages, lnb);
		if (old_rc)
			rc = old_rc;
	} else {
		LBUG();
		rc = -EPROTO;
	}

	if (oti != NULL)
		ofd_info2oti(info, oti);
	RETURN(rc);
}
