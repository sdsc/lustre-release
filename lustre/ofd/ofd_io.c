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

#include "ofd_internal.h"

static CFS_LIST_HEAD(ofd_inconsistency_list);
static DEFINE_SPINLOCK(ofd_inconsistency_lock);

struct ofd_inconsistency_item {
	cfs_list_t		 oii_list;
	struct ofd_object	*oii_obj;
};

static int ofd_inconsistency_self_repair_main(void *args)
{
	struct lu_env		       env;
	struct ofd_thread_info	      *info;
	struct filter_fid	      *ff;
	struct lu_attr		      *la;
	struct ofd_device	      *ofd    = args;
	struct ptlrpc_thread	      *thread = &ofd->ofd_inconsistency_thread;
	struct ofd_inconsistency_item *oii;
	struct l_wait_info	       lwi    = { 0 };
	int			       rc;
	ENTRY;

	rc = lu_env_init(&env, LCT_DT_THREAD);
	spin_lock(&ofd_inconsistency_lock);
	thread_set_flags(thread, rc != 0 ? SVC_STOPPED : SVC_RUNNING);
	wake_up_all(&thread->t_ctl_waitq);
	if (rc != 0) {
		spin_unlock(&ofd_inconsistency_lock);
		RETURN(rc);
	}

	info = ofd_info_init(&env, NULL);
	ff = &info->fti_mds_fid;
	la = &info->fti_attr;
	memset(la, 0, sizeof(*la));

	while (1) {
		if (unlikely(!thread_is_running(thread)))
			break;

		while (!cfs_list_empty(&ofd_inconsistency_list)) {
			struct ofd_object *fo;

			oii = cfs_list_entry(ofd_inconsistency_list.next,
					     struct ofd_inconsistency_item,
					     oii_list);
			cfs_list_del_init(&oii->oii_list);
			spin_unlock(&ofd_inconsistency_lock);

			fo = oii->oii_obj;
			LASSERT(fo->ofo_pfid_exists);
			LASSERT(fo->ofo_inconsistent);

			fid_cpu_to_le(&ff->ff_parent, &fo->ofo_pfid);
			rc = ofd_attr_set(&env, fo, la, ff, true);
			if (rc == 0) {
				fo->ofo_inconsistent = 0;
				ofd->ofd_inconsistency_self_repaired++;
			} else {
				CDEBUG(D_LFSCK, "%s: fail to self_repair for "
				       DFID" to "DFID", rc = %d\n",
				       ofd_obd(ofd)->obd_name,
				       PFID(lu_object_fid(&fo->ofo_obj.do_lu)),
				       PFID(&fo->ofo_pfid), rc);
			}
			lu_object_put(&env, &fo->ofo_obj.do_lu);
			OBD_FREE_PTR(oii);
			spin_lock(&ofd_inconsistency_lock);
		}
		spin_unlock(&ofd_inconsistency_lock);
		l_wait_event(thread->t_ctl_waitq,
			     !cfs_list_empty(&ofd_inconsistency_list) ||
			     !thread_is_running(thread),
			     &lwi);
		spin_lock(&ofd_inconsistency_lock);
	}

	while (!cfs_list_empty(&ofd_inconsistency_list)) {
		oii = cfs_list_entry(ofd_inconsistency_list.next,
				     struct ofd_inconsistency_item,
				     oii_list);
		cfs_list_del_init(&oii->oii_list);
		spin_unlock(&ofd_inconsistency_lock);
		lu_object_put(&env, &oii->oii_obj->ofo_obj.do_lu);
		OBD_FREE_PTR(oii);
		spin_lock(&ofd_inconsistency_lock);
	}

	thread_set_flags(thread, SVC_STOPPED);
	wake_up_all(&thread->t_ctl_waitq);
	spin_unlock(&ofd_inconsistency_lock);
	lu_env_fini(&env);

	RETURN(0);
}

int ofd_start_inconsistency_self_repair_thread(struct ofd_device *ofd)
{
	struct ptlrpc_thread	*thread = &ofd->ofd_inconsistency_thread;
	struct l_wait_info	 lwi	= { 0 };
	long			 rc;

	spin_lock(&ofd_inconsistency_lock);
	if (unlikely(thread_is_running(thread))) {
		spin_unlock(&ofd_inconsistency_lock);
		return -EALREADY;
	}

	thread_set_flags(thread, 0);
	spin_unlock(&ofd_inconsistency_lock);
	rc = PTR_ERR(kthread_run(ofd_inconsistency_self_repair_main, ofd,
				 "self_repair"));
	if (IS_ERR_VALUE(rc)) {
		CERROR("%s: cannot start self_repair thread: rc = %ld\n",
		       ofd_obd(ofd)->obd_name, rc);
	} else {
		rc = 0;
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_running(thread) ||
			     thread_is_stopped(thread),
			     &lwi);
	}

	return rc;
}

int ofd_stop_inconsistency_self_repair_thread(struct ofd_device *ofd)
{
	struct ptlrpc_thread	*thread = &ofd->ofd_inconsistency_thread;
	struct l_wait_info	 lwi	= { 0 };

	spin_lock(&ofd_inconsistency_lock);
	if (thread_is_init(thread) || thread_is_stopped(thread)) {
		spin_unlock(&ofd_inconsistency_lock);
		return -EALREADY;
	}

	thread_set_flags(thread, SVC_STOPPING);
	spin_unlock(&ofd_inconsistency_lock);
	wake_up_all(&thread->t_ctl_waitq);
	l_wait_event(thread->t_ctl_waitq,
		     thread_is_stopped(thread),
		     &lwi);

	return 0;
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

static int ofd_lwp_get_layout(struct obd_export *exp, struct ptlrpc_request **req,
			      struct lu_fid *fid, int size)
{
	struct ptlrpc_request	*treq;
	struct mdt_body		*body;
	char			*tmp;
	int			 namelen = sizeof(XATTR_NAME_LOV);
	int			 rc;
	ENTRY;

	treq = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_GETXATTR);
	if (treq == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&treq->rq_pill, &RMF_CAPA1, RCL_CLIENT, 0);
	req_capsule_set_size(&treq->rq_pill, &RMF_NAME, RCL_CLIENT, namelen);
	req_capsule_set_size(&treq->rq_pill, &RMF_EADATA, RCL_CLIENT, 0);
	rc = ptlrpc_request_pack(treq, LUSTRE_MDS_VERSION, MDS_GETXATTR);
	if (rc != 0) {
		ptlrpc_request_free(treq);
		RETURN(rc);
	}

	body = req_capsule_client_get(&treq->rq_pill, &RMF_MDT_BODY);
	body->uid = 0;
	body->gid = 0;
	body->fsuid = 0;
	body->fsgid = 0;
	body->suppgid = 0;
	body->capability = cfs_curproc_cap_pack();
	body->flags = 0;
	body->eadatasize = size;
	body->fid1 = *fid;
	body->valid = OBD_MD_FLXATTR | OBD_MD_FLID;

	tmp = req_capsule_client_get(&treq->rq_pill, &RMF_NAME);
	memcpy(tmp, XATTR_NAME_LOV, namelen);

	req_capsule_set_size(&treq->rq_pill, &RMF_EADATA, RCL_SERVER, size);
	ptlrpc_request_set_replen(treq);

	rc = ptlrpc_queue_wait(treq);
	if (rc != 0)
		ptlrpc_req_finished(treq);
	else
		*req = treq;
	RETURN(rc);
}

/*
 * \ret > 0: unrecognized stripe
 * \ret ==0: recognized stripe
 * \ret < 0: other failures
 */
static int ofd_lwp_check_stripe(const struct lu_env *env,
				struct obd_export *exp, struct obdo *oa)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct lu_fid		*fid  = &info->fti_fid;
	struct obd_device	*obd  = exp->exp_obd;
	struct ptlrpc_request	*req  = NULL;
	struct mdt_body		*body;
	struct lov_mds_md_v1	*lmm;
	struct lov_ost_data_v1	*objs;
	__u32			 magic;
	__u32			 patten;
	int			 size = obd->u.cli.cl_max_mds_easize;
	int			 rc;
	int			 i;
	__u16			 count;
	ENTRY;

	fid->f_seq = oa->o_parent_seq;
	fid->f_oid = oa->o_parent_oid;
	fid->f_ver = oa->o_parent_ver;

again:
	rc = ofd_lwp_get_layout(exp, &req, fid, size);
	if (unlikely(rc == -ERANGE)) {
		LASSERT(size != 0);

		size = 0;
		goto again;
	}

	if (rc != 0)
		RETURN(rc);

	LASSERT(req != NULL);

	body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
	if (body == NULL)
		GOTO(out, rc = -EPROTO);

	/* Someone changed layout. */
	if (body->eadatasize == 0)
		GOTO(out, rc = 1);

	if (size == 0) {
		spin_lock(&obd->obd_dev_lock);
		if (body->eadatasize > obd->u.cli.cl_max_mds_easize)
			obd->u.cli.cl_max_mds_easize = body->eadatasize;
		spin_unlock(&obd->obd_dev_lock);

		ptlrpc_req_finished(req);
		size = obd->u.cli.cl_max_mds_easize;
		goto again;
	}

	lmm = req_capsule_server_sized_get(&req->rq_pill, &RMF_EADATA,
					   body->eadatasize);
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
	for (i = 0; i < count; i++, objs++) {
		struct ost_id *oi = &info->fti_ostid;

		ostid_le_to_cpu(&objs->l_ost_oi, oi);
		if (memcmp(oi, &oa->o_oi, sizeof(*oi)) == 0) {
			if (i == oa->o_stripe_idx)
				GOTO(out, rc = 0);
			else
				GOTO(out, rc = 1);
		}
	}

	GOTO(out, rc = 1);

out:
	ptlrpc_req_finished(req);
	return rc;
}

static void ofd_add_inconsistency_item(struct ofd_object *fo, struct obdo *oa)
{
	struct ofd_device		*ofd	= ofd_obj2dev(fo);
	struct lu_fid			*pfid	= &fo->ofo_pfid;
	struct ofd_inconsistency_item	*oii;
	bool				 wakeup = false;

	OBD_ALLOC_PTR(oii);
	if (oii == NULL) {
		CERROR("%s: cannot alloc memory to repair OST "
		       "inconsistency for "DFID"\n",
		       ofd_obd(ofd)->obd_name, PFID(&fo->ofo_pfid));
		return;
	}

	spin_lock(&fo->ofo_lock);
	if (fo->ofo_inconsistent) {
		spin_unlock(&fo->ofo_lock);
		OBD_FREE_PTR(oii);
		return;
	}

	pfid->f_seq = oa->o_parent_seq;
	pfid->f_oid = oa->o_parent_oid;
	/* XXX: In fact, the ff_parent::f_ver is not the real parent
	 *	FID::f_ver, instead, it is the OST-object index in
	 *	its parent MDT-object layout EA. */
	pfid->f_ver = oa->o_stripe_idx;
	fo->ofo_pfid_loaded = 1;
	fo->ofo_pfid_exists = 1;
	fo->ofo_inconsistent = 1;
	spin_unlock(&fo->ofo_lock);

	lu_object_get(&fo->ofo_obj.do_lu);
	oii->oii_obj = fo;
	CFS_INIT_LIST_HEAD(&oii->oii_list);
	spin_lock(&ofd_inconsistency_lock);
	if (cfs_list_empty(&ofd_inconsistency_list))
		wakeup = true;
	cfs_list_add_tail(&oii->oii_list, &ofd_inconsistency_list);
	ofd->ofd_inconsistency_self_detected++;
	spin_unlock(&ofd_inconsistency_lock);
	if (wakeup)
		wake_up_all(&ofd->ofd_inconsistency_thread.t_ctl_waitq);

	/* XXX: When the found inconsistency exceeds some threshold,
	 *	we can trigger the LFSCK to scan part of the system
	 *	or the whole system, which depends on how to define
	 *	the threshold, a simple way maybe like that: define
	 *	the absolute value of how many inconsisteny allowed
	 *	to be repaired via self detect/repair mechanism, if
	 *	exceeded, then trigger the LFSCK to scan the layout
	 *	inconsistency within the whole system. */
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

	rc = ofd_object_ff_check(env, fo);
	if (rc == -ENODATA)
		RETURN(0);

	if (rc < 0)
		RETURN(rc);

	spin_lock(&fo->ofo_lock);
	if (likely(oa->o_parent_seq == pfid->f_seq &&
		   oa->o_parent_oid == pfid->f_oid &&
		   oa->o_stripe_idx == pfid->f_ver)) {
		spin_unlock(&fo->ofo_lock);
		RETURN(0);
	}

	/* Someone has already fetched valid parent FID from the MDT,
	 * trust the ofd_object::ofo_pfid, no need to further check. */
	if (fo->ofo_inconsistent) {
		spin_unlock(&fo->ofo_lock);
		RETURN(-EPERM);
	}

	spin_unlock(&fo->ofo_lock);
	rc = ofd_fld_lookup(env, ofd, oa->o_parent_seq, range);
	if (rc != 0)
		RETURN(rc);

	if (unlikely(!fld_range_is_mdt(range)))
		RETURN(-EPERM);

	exp = lustre_find_lwp_by_index(ofd_obd(ofd)->obd_name,
				       range->lsr_index);
	if (unlikely(exp == NULL))
		RETURN(-EPERM);

	rc = ofd_lwp_check_stripe(env, exp, oa);
	class_export_put(exp);
	if (rc > 0)
		RETURN(-EPERM);

	if (rc < 0)
		RETURN(rc);

	/* The client given parent FID is correct, need to fix
	 * server-side inconsistency via a dedicated thread. */
	ofd_add_inconsistency_item(fo, oa);

	RETURN(0);
}

static int ofd_preprw_read(const struct lu_env *env, struct obd_export *exp,
			   struct ofd_device *ofd, struct lu_fid *fid,
			   struct lu_attr *la, struct obdo *oa, int niocount,
			   struct niobuf_remote *rnb, int *nr_local,
			   struct niobuf_local *lnb,
			   struct obd_trans_info *oti)
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
	lprocfs_counter_add(ofd_obd(ofd)->obd_stats,
			    LPROC_OFD_READ_BYTES, tot_bytes);
	ofd_counter_incr(exp, LPROC_OFD_STATS_READ,
			 oti->oti_jobid, tot_bytes);
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
			    struct niobuf_local *lnb,
			    struct obd_trans_info *oti)
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

	/* Always sync if syncjournal parameter is set */
	oti->oti_sync_write = ofd->ofd_syncjournal;

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
			if (!(rnb[i].rnb_flags & OBD_BRW_ASYNC))
				oti->oti_sync_write = 1;
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

	lprocfs_counter_add(ofd_obd(ofd)->obd_stats,
			    LPROC_OFD_WRITE_BYTES, tot_bytes);
	ofd_counter_incr(exp, LPROC_OFD_STATS_WRITE,
			 oti->oti_jobid, tot_bytes);
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

int ofd_preprw(const struct lu_env* env, int cmd, struct obd_export *exp,
	       struct obdo *oa, int objcount, struct obd_ioobj *obj,
	       struct niobuf_remote *rnb, int *nr_local,
	       struct niobuf_local *lnb, struct obd_trans_info *oti,
	       struct lustre_capa *capa)
{
	struct ofd_device	*ofd = ofd_exp(exp);
	struct ofd_thread_info	*info;
	int			 rc = 0;

	if (*nr_local > PTLRPC_MAX_BRW_PAGES) {
		CERROR("%s: bulk has too many pages %d, which exceeds the"
		       "maximum pages per RPC of %d\n",
		       exp->exp_obd->obd_name, *nr_local, PTLRPC_MAX_BRW_PAGES);
		RETURN(-EPROTO);
	}

	rc = lu_env_refill((struct lu_env *)env);
	LASSERT(rc == 0);
	info = ofd_info_init(env, exp);

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

	rc = ostid_to_fid(&info->fti_fid, &oa->o_oi, 0);
	if (unlikely(rc != 0))
		RETURN(rc);

	if (cmd == OBD_BRW_WRITE) {
		rc = ofd_auth_capa(exp, &info->fti_fid, ostid_seq(&oa->o_oi),
				   capa, CAPA_OPC_OSS_WRITE);
		if (rc == 0) {
			la_from_obdo(&info->fti_attr, oa, OBD_MD_FLGETATTR);
			rc = ofd_preprw_write(env, exp, ofd, &info->fti_fid,
					      &info->fti_attr, oa, objcount,
					      obj, rnb, nr_local, lnb, oti);
		}
	} else if (cmd == OBD_BRW_READ) {
		rc = ofd_auth_capa(exp, &info->fti_fid, ostid_seq(&oa->o_oi),
				   capa, CAPA_OPC_OSS_READ);
		if (rc == 0) {
			ofd_grant_prepare_read(env, exp, oa);
			rc = ofd_preprw_read(env, exp, ofd, &info->fti_fid,
					     &info->fti_attr, oa,
					     obj->ioo_bufcnt, rnb, nr_local,
					     lnb, oti);
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
		rc = ofd_object_ff_check(env, ofd_obj);
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
			ofd_obj->ofo_pfid_loaded = 1;
			ofd_obj->ofo_pfid_exists = 1;
		}
	}

	GOTO(out_tx, rc);

out_tx:
	dt_trans_stop(env, ofd->ofd_osd, th);
out:
	la->la_valid = valid;
	return rc;
}

static int
ofd_commitrw_write(const struct lu_env *env, struct ofd_device *ofd,
		   struct lu_fid *fid, struct lu_attr *la,
		   struct filter_fid *ff, int objcount,
		   int niocount, struct niobuf_local *lnb,
		   struct obd_trans_info *oti, int old_rc)
{
	struct ofd_thread_info	*info = ofd_info(env);
	struct ofd_object	*fo;
	struct dt_object	*o;
	struct thandle		*th;
	int			 rc = 0;
	int			 retries = 0;

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

	th->th_sync |= oti->oti_sync_write;

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

	ofd_trans_stop(env, ofd, th, rc);
	if (rc == -ENOSPC && retries++ < 3) {
		CDEBUG(D_INODE, "retry after force commit, retries:%d\n",
		       retries);
		goto retry;
	}

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
	struct ofd_thread_info	*info;
	struct ofd_mod_data	*fmd;
	__u64			 valid;
	struct ofd_device	*ofd = ofd_exp(exp);
	struct filter_fid	*ff = NULL;
	int			 rc = 0;

	info = ofd_info(env);
	ofd_oti2info(info, oti);

	LASSERT(npages > 0);

	rc = ostid_to_fid(&info->fti_fid, &oa->o_oi, 0);
	if (unlikely(rc != 0))
		RETURN(rc);
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

		rc = ofd_commitrw_write(env, ofd, &info->fti_fid,
					&info->fti_attr, ff, objcount, npages,
					lnb, oti, old_rc);
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

	ofd_info2oti(info, oti);
	RETURN(rc);
}
