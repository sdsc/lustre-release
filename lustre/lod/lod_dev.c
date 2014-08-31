/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright  2009 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 *
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/lod/lod_dev.c
 *
 * Lustre Logical Object Device
 *
 * Author: Alex Zhuravlev <alexey.zhuravlev@intel.com>
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */
/**
 * The Logical Object Device (LOD) layer manages access to striped
 * objects (both regular files and directories). It implements the DT
 * device and object APIs and is responsible for creating, storing,
 * and loading striping information as an extended attribute of the
 * underlying OSD object. LOD is the server side analog of the LOV and
 * LMV layers on the client side.
 *
 * Metadata LU object stack (layers of the same compound LU object,
 * all have the same FID):
 *
 *        MDT
 *         |      MD API
 *        MDD
 *         |      DT API
 *        LOD
 *       /   \    DT API
 *     OSD   OSP
 *
 * During LOD object initialization the localness or remoteness of the
 * object FID dictates the choice between OSD and OSP.
 *
 * An LOD object (file or directory) with N stripes (each has a
 * different FID):
 *
 *          LOD
 *           |
 *   +---+---+---+...+
 *   |   |   |   |   |
 *   S0  S1  S2  S3  S(N-1)  OS[DP] objects, seen as DT objects by LOD
 *
 * When upper layers must access an object's stripes (which are
 * themselves OST or MDT LU objects) LOD finds these objects by their
 * FIDs and stores them as an array of DT object pointers on the
 * object. Declarations and operations on LOD objects are received by
 * LOD (as DT object operations) and performed on the underlying
 * OS[DP] object and (as needed) on the stripes. From the perspective
 * of LOD, a stripe-less file (created by mknod() or open with
 * O_LOV_DELAY_CREATE) is an object which does not yet have stripes,
 * while a non-striped directory (created by mkdir()) is an object
 * which will never have stripes.
 *
 * The LOD layer also implements a small subset of the OBD device API
 * to support MDT stack initialization and finalization (an MDD device
 * connects and disconnects itself to and from the underlying LOD
 * device), and pool management. In turn LOD uses the OBD device API
 * to connect it self to the underlying OSD, and to connect itself to
 * OSP devices representing the MDTs and OSTs that bear the stripes of
 * its objects.
 */

#define DEBUG_SUBSYSTEM S_MDS

#include <obd_class.h>
#include <md_object.h>
#include <lustre_fid.h>
#include <lustre_param.h>
#include <lustre_update.h>
#include <lustre_log.h>

#include "lod_internal.h"

/**
 * Lookup MDT/OST index \a tgt by FID \a fid.
 *
 * \param lod LOD to be lookup at.
 * \param fid FID of object to find MDT/OST.
 * \param tgt MDT/OST index to return.
 * \param type indidcate the FID is on MDS or OST.
 **/
int lod_fld_lookup(const struct lu_env *env, struct lod_device *lod,
		   const struct lu_fid *fid, __u32 *tgt, int *type)
{
	struct lu_seq_range	range = { 0 };
	struct lu_server_fld	*server_fld;
	int rc = 0;
	ENTRY;

	LASSERTF(fid_is_sane(fid), "Invalid FID "DFID"\n", PFID(fid));

	if (fid_is_idif(fid)) {
		*tgt = fid_idif_ost_idx(fid);
		*type = LU_SEQ_RANGE_OST;
		RETURN(rc);
	}

	if (fid_is_update_log(fid)) {
		*tgt = fid_oid(fid);
		*type = LU_SEQ_RANGE_MDT;
		RETURN(rc);
	}

	if (!lod->lod_initialized || (!fid_seq_in_fldb(fid_seq(fid)))) {
		LASSERT(lu_site2seq(lod2lu_dev(lod)->ld_site) != NULL);

		*tgt = lu_site2seq(lod2lu_dev(lod)->ld_site)->ss_node_id;
		*type = LU_SEQ_RANGE_MDT;
		RETURN(rc);
	}

	server_fld = lu_site2seq(lod2lu_dev(lod)->ld_site)->ss_server_fld;
	fld_range_set_type(&range, *type);
	rc = fld_server_lookup(env, server_fld, fid_seq(fid), &range);
	if (rc)
		RETURN(rc);

	*tgt = range.lsr_index;
	*type = range.lsr_flags;

	CDEBUG(D_INFO, "LOD: got tgt %x for sequence: "
	       LPX64"\n", *tgt, fid_seq(fid));

	RETURN(rc);
}

extern struct lu_object_operations lod_lu_obj_ops;
extern struct dt_object_operations lod_obj_ops;

/* Slab for OSD object allocation */
struct kmem_cache *lod_object_kmem;

static struct lu_kmem_descr lod_caches[] = {
	{
		.ckd_cache = &lod_object_kmem,
		.ckd_name  = "lod_obj",
		.ckd_size  = sizeof(struct lod_object)
	},
	{
		.ckd_cache = NULL
	}
};

static struct lu_device *lod_device_fini(const struct lu_env *env,
					 struct lu_device *d);

struct lu_object *lod_object_alloc(const struct lu_env *env,
				   const struct lu_object_header *hdr,
				   struct lu_device *dev)
{
	struct lod_object	*lod_obj;
	struct lu_object	*lu_obj;
	ENTRY;

	OBD_SLAB_ALLOC_PTR_GFP(lod_obj, lod_object_kmem, GFP_NOFS);
	if (lod_obj == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	lu_obj = lod2lu_obj(lod_obj);
	dt_object_init(&lod_obj->ldo_obj, NULL, dev);
	lod_obj->ldo_obj.do_ops = &lod_obj_ops;
	lu_obj->lo_ops = &lod_lu_obj_ops;

	RETURN(lu_obj);
}

static int lod_sub_process_config(const struct lu_env *env,
				 struct lod_device *lod,
				 struct lod_tgt_descs *ltd,
				 struct lustre_cfg *lcfg)
{
	struct lu_device  *next;
	int rc = 0;
	unsigned int i;

	lod_getref(ltd);
	if (ltd->ltd_tgts_size <= 0) {
		lod_putref(lod, ltd);
		return 0;
	}
	cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {
		struct lod_tgt_desc *tgt;
		int rc1;

		tgt = LTD_TGT(ltd, i);
		LASSERT(tgt && tgt->ltd_tgt);
		next = &tgt->ltd_tgt->dd_lu_dev;
		rc1 = next->ld_ops->ldo_process_config(env, next, lcfg);
		if (rc1) {
			CERROR("%s: error cleaning up LOD index %u: cmd %#x"
			       ": rc = %d\n", lod2obd(lod)->obd_name, i,
			       lcfg->lcfg_command, rc1);
			rc = rc1;
		}
	}
	lod_putref(lod, ltd);
	return rc;
}

struct lod_recovery_data {
	struct lod_device	*lrd_lod;
	struct lod_tgt_desc	*lrd_ltd;
	struct ptlrpc_thread	*lrd_thread;
	struct update_recovery_data	*lrd_recovery_data;
};

static int lod_process_recovery_updates(const struct lu_env *env,
					struct llog_handle *llh,
					struct llog_rec_hdr *rec,
					void *data)
{
	struct lod_recovery_data	*lrd = data;
	struct update_recovery_data	*urd = lrd->lrd_recovery_data;

	return insert_update_records_to_recovery_list(urd,
					(struct update_records *)rec,
					lrd->lrd_ltd->ltd_index);
}

static int lod_sub_recovery_thread(void *arg)
{
	struct lod_recovery_data	*lrd = arg;
	struct lod_device		*lod = lrd->lrd_lod;
	struct dt_device		*dt = lrd->lrd_ltd->ltd_tgt;
	struct ptlrpc_thread		*thread = lrd->lrd_thread;
	struct obd_device		*obd;
	struct llog_ctxt		*ctxt;
	struct lu_env			env;
	int				rc;
	ENTRY;

	thread->t_flags = SVC_RUNNING;
	wake_up(&thread->t_ctl_waitq);

	rc = lu_env_init(&env, LCT_LOCAL | LCT_MD_THREAD);
	if (rc != 0) {
		OBD_FREE_PTR(lrd);
		CERROR("%s: can't initialize env: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		RETURN(rc);
	}

	rc = lod_sub_prep_llog(&env, lod, dt);
	if (rc != 0)
		GOTO(out, rc);

	/* Process the recovery record */
	obd = dt->dd_lu_dev.ld_obd;
	ctxt = llog_get_context(dt->dd_lu_dev.ld_obd, LLOG_UPDATELOG_ORIG_CTXT);
	LASSERT(ctxt != NULL);
	LASSERT(ctxt->loc_handle != NULL);

	rc = llog_cat_process(&env, ctxt->loc_handle,
			      lod_process_recovery_updates, lrd, 0, 0);
	if (rc < 0)
		GOTO(out, rc);

	lrd->lrd_ltd->ltd_got_update_log = 1;
	lrd->lrd_recovery_data->urd_got_recovery_updates++;

out:
	OBD_FREE_PTR(lrd);
	thread->t_flags = SVC_STOPPED;
	lu_env_fini(&env);
	RETURN(rc);
}

static struct llog_operations updatelog_orig_logops;
int lod_sub_init_llog(const struct lu_env *env, struct lod_device *lod,
		      struct dt_device *dt)
{
	struct obd_device		*obd;
	struct lod_recovery_data	*lrd = NULL;
	struct ptlrpc_thread		*thread;
	struct task_struct		*task;
	struct l_wait_info		lwi = { 0 };
	struct lod_tgt_desc		*sub_ltd = NULL;
	__u32				index;
	int				rc;
	ENTRY;

	OBD_ALLOC_PTR(lrd);
	if (lrd == NULL)
		RETURN(-ENOMEM);

	if (lod->lod_child == dt) {
		thread = &lod->lod_child_recovery_thread;
		rc = lodname2mdt_index(lod2obd(lod)->obd_name, &index);
		if (rc != 0)
			RETURN(rc);
	} else {
		struct lod_tgt_descs	*ltd = &lod->lod_mdt_descs;
		struct lod_tgt_desc	*tgt = NULL;
		int			i;

		mutex_lock(&ltd->ltd_mutex);
		cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {

			tgt = LTD_TGT(ltd, i);
			if (tgt->ltd_tgt == dt) {
				index = tgt->ltd_index;
				sub_ltd = tgt;
				break;
			}
		}
		mutex_unlock(&ltd->ltd_mutex);
		OBD_ALLOC_PTR(tgt->ltd_recovery_thread);
		if (tgt->ltd_recovery_thread == NULL) {
			OBD_FREE_PTR(lrd);
			RETURN(-ENOMEM);
		}
		thread = tgt->ltd_recovery_thread;
	}

	lrd->lrd_lod = lod;
	lrd->lrd_ltd = sub_ltd;
	lrd->lrd_thread = thread;
	init_waitqueue_head(&thread->t_ctl_waitq);
	lrd->lrd_recovery_data =
		lod2lu_dev(lod)->ld_site->ls_target->lut_update_recovery_data;
	LASSERT(lrd->lrd_recovery_data != NULL);

	obd = dt->dd_lu_dev.ld_obd;
	obd->obd_lvfs_ctxt.dt = dt;
	rc = llog_setup(env, obd, &obd->obd_olg, LLOG_UPDATELOG_ORIG_CTXT,
			NULL, &updatelog_orig_logops);
	if (rc < 0) {
		CERROR("%s: updatelog llog setup failed: rc = %d\n",
		       obd->obd_name, rc);
		OBD_FREE_PTR(lrd);
		RETURN(rc);
	}

	/* Start the recovery thread */
	task = kthread_run(lod_sub_recovery_thread, lrd, "lsub_rec-%u",
			   index);
	if (IS_ERR(task)) {
		rc = PTR_ERR(task);
		OBD_FREE_PTR(lrd);
		CERROR("%s: cannot start recovery thread: rc = %d\n",
		       obd->obd_name, rc);
		GOTO(out_llog, rc = PTR_ERR(task));
	}

	l_wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_RUNNING ||
					  thread->t_flags & SVC_STOPPED, &lwi);
out_llog:
	if (rc != 0)
		lod_sub_fini_llog(env, lod->lod_child, thread);

	RETURN(rc);
}

void lod_sub_fini_llog(const struct lu_env *env,
		       struct dt_device *dt, struct ptlrpc_thread *thread)
{
	struct obd_device	*obd;
	struct llog_ctxt	*ctxt;

	/* Stop recovery thread first */
	if (thread != NULL && thread->t_flags & SVC_RUNNING) {
		thread->t_flags = SVC_STOPPING;
		wake_up(&thread->t_ctl_waitq);
		wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_STOPPED);
	}

	obd = dt->dd_lu_dev.ld_obd;
	ctxt = llog_get_context(obd, LLOG_UPDATELOG_ORIG_CTXT);
	if (ctxt == NULL)
		return;

	if (ctxt->loc_handle != NULL)
		llog_cat_close(env, ctxt->loc_handle);

	llog_cleanup(env, ctxt);

	return;
}

void lod_sub_fini_all_llogs(const struct lu_env *env, struct lod_device *lod)
{
	struct lod_tgt_descs *ltd = &lod->lod_mdt_descs;
	int i;

	mutex_lock(&ltd->ltd_mutex);
	cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {
		struct lod_tgt_desc	*tgt;

		tgt = LTD_TGT(ltd, i);
		lod_sub_fini_llog(env, tgt->ltd_tgt, tgt->ltd_recovery_thread);
	}
	mutex_unlock(&ltd->ltd_mutex);
}

static void lod_thandle_destroy(struct lod_thandle *lth)
{
	struct lod_sub_thandle *lst;
	struct lod_sub_thandle *tmp;

	list_for_each_entry_safe(lst, tmp, &lth->lt_sub_trans_list, lst_list) {
		list_del(&lst->lst_list);
		if (lst->lst_update != NULL)
			OBD_FREE_PTR(lst->lst_update);
		OBD_FREE_PTR(lst);
	}

	OBD_FREE_PTR(lth);
	return;
}

static void lod_cleanup_commit_trans_track(const struct lu_env *env,
					   struct lod_device *lod)
{
	struct lod_thandle *lth;
	struct lod_thandle *tmp;

	spin_lock(&lod->lod_commit_lock);
	list_for_each_entry_safe(lth, tmp, &lod->lod_commit_list,
				 lt_commit_list) {
		list_del(&lth->lt_commit_list);
		lod_thandle_destroy(lth);
	}
	spin_unlock(&lod->lod_commit_lock);

	return;
}

int lodname2mdt_index(char *lodname, __u32 *mdt_index)
{
	unsigned long index;
	char *ptr, *tmp;

	/* The lodname suppose to be fsname-MDTxxxx-mdtlov */
	ptr = strrchr(lodname, '-');
	if (ptr == NULL) {
		CERROR("invalid MDT index in '%s'\n", lodname);
		return -EINVAL;
	}

	if (strncmp(ptr, "-mdtlov", 7) != 0) {
		CERROR("invalid MDT index in '%s'\n", lodname);
		return -EINVAL;
	}

	if ((unsigned long)ptr - (unsigned long)lodname <= 8) {
		CERROR("invalid MDT index in '%s'\n", lodname);
		return -EINVAL;
	}

	if (strncmp(ptr - 8, "-MDT", 4) != 0) {
		CERROR("invalid MDT index in '%s'\n", lodname);
		return -EINVAL;
	}

	index = simple_strtol(ptr - 4, &tmp, 16);
	if (*tmp != '-' || index > INT_MAX || index < 0) {
		CERROR("invalid MDT index in '%s'\n", lodname);
		return -EINVAL;
	}
	*mdt_index = index;
	return 0;
}

/**
 * Procss config log on LOD
 * \param env environment info
 * \param dev lod device
 * \param lcfg config log
 *
 * Add osc config log,
 * marker  20 (flags=0x01, v2.2.49.56) lustre-OST0001  'add osc'
 * add_uuid  nid=192.168.122.162@tcp(0x20000c0a87aa2)  0:  1:nidxxx
 * attach    0:lustre-OST0001-osc-MDT0001  1:osc  2:lustre-MDT0001-mdtlov_UUID
 * setup     0:lustre-OST0001-osc-MDT0001  1:lustre-OST0001_UUID  2:nid
 * lov_modify_tgts add 0:lustre-MDT0001-mdtlov  1:lustre-OST0001_UUID  2:1  3:1
 * marker  20 (flags=0x02, v2.2.49.56) lustre-OST0001  'add osc'
 *
 * Add mdc config log
 * marker  10 (flags=0x01, v2.2.49.56) lustre-MDT0000  'add osp'
 * add_uuid  nid=192.168.122.162@tcp(0x20000c0a87aa2)  0:  1:nid
 * attach 0:lustre-MDT0000-osp-MDT0001  1:osp  2:lustre-MDT0001-mdtlov_UUID
 * setup     0:lustre-MDT0000-osp-MDT0001  1:lustre-MDT0000_UUID  2:nid
 * modify_mdc_tgts add 0:lustre-MDT0001  1:lustre-MDT0000_UUID  2:0  3:1
 * marker  10 (flags=0x02, v2.2.49.56) lustre-MDT0000_UUID  'add osp'
 **/
static int lod_process_config(const struct lu_env *env,
			      struct lu_device *dev,
			      struct lustre_cfg *lcfg)
{
	struct lod_device *lod = lu2lod_dev(dev);
	struct lu_device  *next = &lod->lod_child->dd_lu_dev;
	char		  *arg1;
	int		   rc = 0;
	ENTRY;

	switch(lcfg->lcfg_command) {
	case LCFG_LOV_DEL_OBD:
	case LCFG_LOV_ADD_INA:
	case LCFG_LOV_ADD_OBD:
	case LCFG_ADD_MDC: {
		__u32 index;
		__u32 mdt_index;
		int gen;
		/* lov_modify_tgts add  0:lov_mdsA  1:osp  2:0  3:1
		 * modify_mdc_tgts add  0:lustre-MDT0001
		 *		      1:lustre-MDT0001-mdc0002
		 *		      2:2  3:1*/
		arg1 = lustre_cfg_string(lcfg, 1);

		if (sscanf(lustre_cfg_buf(lcfg, 2), "%d", &index) != 1)
			GOTO(out, rc = -EINVAL);
		if (sscanf(lustre_cfg_buf(lcfg, 3), "%d", &gen) != 1)
			GOTO(out, rc = -EINVAL);

		if (lcfg->lcfg_command == LCFG_LOV_ADD_OBD) {
			char *mdt;
			mdt = strstr(lustre_cfg_string(lcfg, 0), "-MDT");
			/* 1.8 configs don't have "-MDT0000" at the end */
			if (mdt == NULL) {
				mdt_index = 0;
			} else {
				rc = lodname2mdt_index(
					lustre_cfg_string(lcfg, 0),
					&mdt_index);
				if (rc != 0)
					GOTO(out, rc);
			}
			rc = lod_add_device(env, lod, arg1, index, gen,
					    mdt_index, LUSTRE_OSC_NAME, 1);
		} else if (lcfg->lcfg_command == LCFG_ADD_MDC) {
			mdt_index = index;
			rc = lod_add_device(env, lod, arg1, index, gen,
					    mdt_index, LUSTRE_MDC_NAME, 1);
		} else if (lcfg->lcfg_command == LCFG_LOV_ADD_INA) {
			/*FIXME: Add mdt_index for LCFG_LOV_ADD_INA*/
			mdt_index = 0;
			rc = lod_add_device(env, lod, arg1, index, gen,
					    mdt_index, LUSTRE_OSC_NAME, 0);
		} else {
			rc = lod_del_device(env, lod,
					    &lod->lod_ost_descs,
					    arg1, index, gen, true);
		}

		break;
	}

	case LCFG_PARAM: {
		struct obd_device *obd = lod2obd(lod);

		rc = class_process_proc_seq_param(PARAM_LOV, obd->obd_vars,
						  lcfg, obd);
		if (rc > 0)
			rc = 0;
		GOTO(out, rc);
	}
	case LCFG_PRE_CLEANUP: {
		lod_sub_process_config(env, lod, &lod->lod_mdt_descs, lcfg);
		lod_sub_process_config(env, lod, &lod->lod_ost_descs, lcfg);
		next = &lod->lod_child->dd_lu_dev;
		rc = next->ld_ops->ldo_process_config(env, next, lcfg);
		if (rc != 0)
			CDEBUG(D_HA, "%s: can't process %u: %d\n",
			       lod2obd(lod)->obd_name, lcfg->lcfg_command, rc);
		break;
	}
	case LCFG_CLEANUP: {
		/*
		 * do cleanup on underlying storage only when
		 * all OSPs are cleaned up, as they use that OSD as well
		 */
		lod_sub_fini_all_llogs(env, lod);
		lu_dev_del_linkage(dev->ld_site, dev);
		lod_sub_process_config(env, lod, &lod->lod_mdt_descs, lcfg);
		lod_sub_process_config(env, lod, &lod->lod_ost_descs, lcfg);
		next = &lod->lod_child->dd_lu_dev;
		rc = next->ld_ops->ldo_process_config(env, next, lcfg);
		if (rc)
			CERROR("%s: can't process %u: %d\n",
			       lod2obd(lod)->obd_name, lcfg->lcfg_command, rc);

		rc = obd_disconnect(lod->lod_child_exp);
		if (rc)
			CERROR("error in disconnect from storage: %d\n", rc);
		break;
	}
	default:
	       CERROR("%s: unknown command %u\n", lod2obd(lod)->obd_name,
		      lcfg->lcfg_command);
	       rc = -EINVAL;
	       break;
	}

out:
	RETURN(rc);
}

static int lod_recovery_complete(const struct lu_env *env,
				 struct lu_device *dev)
{
	struct lod_device   *lod = lu2lod_dev(dev);
	struct lu_device    *next = &lod->lod_child->dd_lu_dev;
	unsigned int	     i;
	int		     rc;
	ENTRY;

	LASSERT(lod->lod_recovery_completed == 0);
	lod->lod_recovery_completed = 1;

	rc = next->ld_ops->ldo_recovery_complete(env, next);

	lod_getref(&lod->lod_ost_descs);
	if (lod->lod_osts_size > 0) {
		cfs_foreach_bit(lod->lod_ost_bitmap, i) {
			struct lod_tgt_desc *tgt;
			tgt = OST_TGT(lod, i);
			LASSERT(tgt && tgt->ltd_tgt);
			next = &tgt->ltd_ost->dd_lu_dev;
			rc = next->ld_ops->ldo_recovery_complete(env, next);
			if (rc)
				CERROR("%s: can't complete recovery on #%d:"
					"%d\n", lod2obd(lod)->obd_name, i, rc);
		}
	}
	lod_putref(lod, &lod->lod_ost_descs);
	RETURN(rc);
}

static inline int lod_commit_track_running(struct lod_device *lod)
{
	return lod->lod_commit_track_thread.t_flags & SVC_RUNNING;
}

static inline int lod_commit_track_stopped(struct lod_device *lod)
{
	return lod->lod_commit_track_thread.t_flags & SVC_STOPPED;
}

static int lod_cancel_update_log(const struct lu_env *env,
				 struct lod_thandle *lth)
{
	struct lod_sub_thandle *lst;

	/* The first lst in the list is master sub transaction,
	 * cancel it first, after cancellation is committed,
	 * cancel other cookies */
	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		struct llog_ctxt	*ctxt;
		struct obd_device	*obd;

		obd = lst->lst_dt->dd_lu_dev.ld_obd;
		ctxt = llog_get_context(obd, LLOG_UPDATELOG_ORIG_CTXT);
		LASSERT(ctxt);

		LASSERT(lst->lst_update != NULL);
		llog_cat_cancel_records(env, ctxt->loc_handle,
					1, &lst->lst_update->lstu_cookie);
		llog_ctxt_put(ctxt);
	}

	return 0;
}

static int lod_commit_track_cancel_cookie(const struct lu_env *env,
					  struct lod_device *lod)
{
	int	count = 0;
	int	rc = 0;
	ENTRY;

	do {
		struct lod_thandle *lth;
		struct lod_thandle *tmp;

		spin_lock(&lod->lod_commit_lock);
		if (list_empty(&lod->lod_commit_list)) {
			spin_unlock(&lod->lod_commit_lock);
			break;
		}
		list_for_each_entry_safe(lth, tmp, &lod->lod_commit_list,
					 lt_commit_list) {
			list_del(&lth->lt_commit_list);
			break;
		}
		spin_unlock(&lod->lod_commit_lock);

		rc = lod_cancel_update_log(env, lth);
		if (rc < 0) {
			/* Add it back to the commit list */
			spin_lock(&lod->lod_commit_lock);
			list_add_tail(&lth->lt_commit_list,
				      &lod->lod_commit_list);
			spin_unlock(&lod->lod_commit_lock);
			break;
		}
		lod_thandle_destroy(lth);

		if (++count > 5)
			break;
	} while (1);

	RETURN(rc);
}

static bool lod_ready_for_cancel_log(struct lod_device *lod)
{
	return !list_empty(&lod->lod_commit_list);
}

static int lod_llog_cancel_thread(void *_arg)
{
	struct lod_device	*lod = _arg;
	struct ptlrpc_thread	*thread = &lod->lod_commit_track_thread;
	struct l_wait_info	 lwi = { 0 };
	struct lu_env		 env;
	int			 rc;

	ENTRY;

	rc = lu_env_init(&env, LCT_LOCAL | LCT_MD_THREAD);
	if (rc) {
		CERROR("%s: can't initialize env: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		RETURN(rc);
	}

	spin_lock(&lod->lod_commit_lock);
	thread->t_flags = SVC_RUNNING;
	spin_unlock(&lod->lod_commit_lock);
	wake_up(&thread->t_ctl_waitq);

	do {
		l_wait_event(lod->lod_commit_track_waitq,
			     !lod_commit_track_running(lod) ||
			     lod_ready_for_cancel_log(lod), &lwi);

		lod_commit_track_cancel_cookie(&env, lod);

		if (!lod_commit_track_running(lod))
			break;
	} while (1);

	thread->t_flags = SVC_STOPPED;

	wake_up(&thread->t_ctl_waitq);

	lu_env_fini(&env);

	RETURN(0);
}

static int lod_llog_daemon_init(const struct lu_env *env,
				struct lod_device *lod)
{
	struct l_wait_info	lwi = { 0 };
	struct task_struct	*task;
	__u32			index;
	int			rc;
	ENTRY;

	rc = lodname2mdt_index(lod2obd(lod)->obd_name, &index);
	if (rc < 0)
		RETURN(rc);

	/* Init the llog in its own stack */
	rc = lod_sub_init_llog(env, lod, lod->lod_child);
	if (rc != 0)
		RETURN(rc);

	spin_lock_init(&lod->lod_commit_lock);
	INIT_LIST_HEAD(&lod->lod_commit_list);

	init_waitqueue_head(&lod->lod_commit_track_waitq);
	init_waitqueue_head(&lod->lod_commit_track_thread.t_ctl_waitq);
	task = kthread_run(lod_llog_cancel_thread, lod, "lod_cancel-%u",
			   index);
	if (IS_ERR(task)) {
		rc = PTR_ERR(task);
		CERROR("%s: cannot start commit check thread: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		GOTO(out_llog, rc = PTR_ERR(task));
	}

	l_wait_event(lod->lod_commit_track_thread.t_ctl_waitq,
		     lod_commit_track_running(lod) ||
		     lod_commit_track_stopped(lod), &lwi);
out_llog:
	if (rc != 0)
		lod_sub_fini_llog(env, lod->lod_child,
				  &lod->lod_child_recovery_thread);
	RETURN(rc);
}

static int lod_llog_daemon_fini(const struct lu_env *env,
				 struct lod_device *lod)
{
	struct ptlrpc_thread	*thread = &lod->lod_commit_track_thread;

	thread->t_flags = SVC_STOPPING;
	wake_up(&lod->lod_commit_track_waitq);
	wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_STOPPED);

	lod_cleanup_commit_trans_track(env, lod);
	return 0;
}

static const char lod_updates_log_name[] = "update_log";
static int lod_prepare(const struct lu_env *env, struct lu_device *pdev,
		       struct lu_device *cdev)
{
	struct lod_device	*lod = lu2lod_dev(cdev);
	struct lu_device	*next = &lod->lod_child->dd_lu_dev;
	struct lu_fid		*fid = &lod_env_info(env)->lti_fid;
	int			rc;
	struct dt_object	*root;
	struct dt_object	*dto;
	__u32			index;
	ENTRY;

	rc = next->ld_ops->ldo_prepare(env, pdev, next);
	if (rc != 0) {
		CERROR("%s: prepare bottom error: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		RETURN(rc);
	}

	rc = dt_root_get(env, lod->lod_child, fid);
	if (rc < 0)
		RETURN(rc);

	root = dt_locate(env, lod->lod_child, fid);
	if (IS_ERR(root))
		RETURN(PTR_ERR(root));

	index = lu_site2seq(lod2lu_dev(lod)->ld_site)->ss_node_id;
	lu_update_log_fid(fid, index);

	/* Create update log object */
	dto = local_file_find_or_create_with_fid(env, lod->lod_child,
						 fid, root,
						 lod_updates_log_name,
				 S_IFREG | S_IRUGO | S_IWUSR | S_IXUGO);
	if (IS_ERR(dto))
		GOTO(out_put, rc = PTR_ERR(dto));

	rc = lod_llog_daemon_init(env, lod);
	if (rc != 0)
		GOTO(out_put, rc);

	/* since stack is not fully set up the local_storage uses own stack
	 * and we should drop its object from cache */
	lu_object_put_nocache(env, &dto->do_lu);

	lod->lod_initialized = 1;

out_put:
	lu_object_put_nocache(env, &root->do_lu);

	RETURN(rc);
}

const struct lu_device_operations lod_lu_ops = {
	.ldo_object_alloc	= lod_object_alloc,
	.ldo_process_config	= lod_process_config,
	.ldo_recovery_complete	= lod_recovery_complete,
	.ldo_prepare		= lod_prepare,
};

static int lod_root_get(const struct lu_env *env,
			struct dt_device *dev, struct lu_fid *f)
{
	return dt_root_get(env, dt2lod_dev(dev)->lod_child, f);
}

static int lod_statfs(const struct lu_env *env,
		      struct dt_device *dev, struct obd_statfs *sfs)
{
	return dt_statfs(env, dt2lod_dev(dev)->lod_child, sfs);
}

static struct thandle *lod_trans_create(const struct lu_env *env,
					struct dt_device *dev)
{
	struct lod_thandle	*lod_th;
	struct thandle		*child_th;
	struct thandle		*parent_th;
	ENTRY;

	OBD_ALLOC_GFP(lod_th, sizeof *lod_th, __GFP_IO);
	if (lod_th == NULL)
		return ERR_PTR(-ENOMEM);

	child_th = dt_trans_create(env, dt2lod_dev(dev)->lod_child);
	if (IS_ERR(child_th))
		return child_th;

	child_th->th_storage_th = child_th;
	lod_th->lt_child = child_th;
	lod_th->lt_update_records = NULL;
	lod_th->lt_magic = LOD_THANDLE_MAGIC;

	INIT_LIST_HEAD(&lod_th->lt_sub_trans_list);
	INIT_LIST_HEAD(&lod_th->lt_commit_list);

	parent_th = &lod_th->lt_super;

	parent_th->th_dev = dev;
	parent_th->th_storage_th = child_th;

	return parent_th;
}

/**
 * The transaction are separated in different layers, i.e. MDD LOD, OSD and
 * OSP has its own thandle.
 *
 * If the operation in one transaction involve several MDTs, LOD transaction
 * be attached by several sub transactions(OSD or OSP). This function gets one
 * sub transaction by sub object.
 *
 * \param[in] env	execution environment
 * \param[in] th	thandle on LOD layer
 * \param[in] child_obj child object used to get sub transaction
 *
 * \retval		thandle of sub transaction if succeed
 *                      PTR_ERR(errno) if failed
 */
struct thandle *lod_get_sub_trans(const struct lu_env *env,
				  struct thandle *th,
				  const struct dt_object *child_obj)
{
	struct dt_device	*child_dt = lu2dt_dev(child_obj->do_lu.lo_dev);
	struct lod_thandle	*lth;
	struct lod_sub_thandle	*lst;
	struct thandle		*child_th;
	int			rc = 0;
	ENTRY;

	lth = container_of0(th, struct lod_thandle, lt_super);
	LASSERT(lth->lt_child != NULL);
	if (likely(child_dt == lth->lt_child->th_dev))
		RETURN(lth->lt_child);

	/* Because there is always only one thread access the list, no
	 * need lock here. */
	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		if (lst->lst_dt == child_dt)
			RETURN(lst->lst_child);
	}

	child_th = dt_trans_create(env, child_dt);
	if (IS_ERR(child_th))
		RETURN(child_th);

	if (child_th->th_remote_mdt)
		lth->lt_multiple_node = 1;

	lst = lod_sub_create_trans(env, lth, child_th);
	if (IS_ERR(lst)) {
		child_th->th_result = rc;
		GOTO(out_stop, rc = -ENOMEM);
	}

	list_add(&lst->lst_list, &lth->lt_sub_trans_list);

out_stop:
	if (rc != 0) {
		dt_trans_stop(env, child_dt, child_th);
		child_th = ERR_PTR(rc);
	}

	RETURN(child_th);
}

static int lod_declare_updates_write(const struct lu_env *env,
				     struct lod_device *lod,
				     struct lod_thandle *lth)
{
	struct lod_sub_thandle *lst;
	struct update_records *records;
	int rc;

	LASSERT(lth->lt_update_records != NULL);
	records = lth->lt_update_records->lur_update_records;

	/* Declare update write for all other target */
	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		struct update_records *records;

		if (lst->lst_update == NULL)
			continue;

		records = lth->lt_update_records->lur_update_records;
		rc = lod_sub_declare_updates_write(env, lod, records,
						   lst->lst_child);
		if (rc != 0)
			break;
	}

	return rc;
}

static int lod_create_update_records(struct lod_update_records *lur)
{
	if (lur->lur_update_records != NULL)
		return 0;

	OBD_ALLOC_LARGE(lur->lur_update_records,
			UPDATE_RECORDS_BUFFER_SIZE);

	if (lur->lur_update_records == NULL)
		return -ENOMEM;

	lur->lur_update_records_size = UPDATE_RECORDS_BUFFER_SIZE;

	return 0;
}

int lod_extend_update_records(struct lod_update_records *lur,
			      size_t new_size)
{
	struct update_records	*records;

	OBD_ALLOC_LARGE(records, new_size);
	if (records == NULL)
		return -ENOMEM;

	if (lur->lur_update_records != NULL) {
		memcpy(records, lur->lur_update_records,
		       lur->lur_update_records_size);
		OBD_FREE_LARGE(lur->lur_update_records,
			       lur->lur_update_records_size);
	}

	lur->lur_update_records = records;
	lur->lur_update_records_size = new_size;

	return 0;
}

static int lod_create_update_params(struct lod_update_records *lur)
{
	if (lur->lur_update_params != NULL)
		return 0;

	OBD_ALLOC_LARGE(lur->lur_update_params, UPDATE_PARAMS_BUFFER_SIZE);
	if (lur->lur_update_params == NULL)
		return -ENOMEM;

	lur->lur_update_params_size = UPDATE_PARAMS_BUFFER_SIZE;
	return 0;
}

int lod_extend_update_params(struct lod_update_records *lur,
			     size_t new_size)
{
	struct update_params	*params;

	OBD_ALLOC_LARGE(params, new_size);
	if (params == NULL)
		return -ENOMEM;

	if (lur->lur_update_params != NULL) {
		memcpy(params, lur->lur_update_params,
		       lur->lur_update_params_size);
		OBD_FREE_LARGE(lur->lur_update_params,
			       lur->lur_update_params_size);
	}

	lur->lur_update_params = params;
	lur->lur_update_params_size = new_size;

	return 0;
}

/**
 * Prepare the update records.
 *
 * Merge params and ops into the update records, then initializing
 * the update buffer.
 *
 * During transaction execution phase, parameters and update ops
 * are collected in two different buffers (see lod_updates_pack()),
 * during transaction stop, it needs to be merged in one buffer,
 * so it will be written in the update log.
 *
 * \param[in] env	execution environment
 * \param[in] lur	lod_update_records to be merged
 *
 * \retval		0 if merging succeeds.
 * \retval		negaitive errno if merging fails.
 */
static int lod_prepare_writing_updates(const struct lu_env *env,
				       struct lod_update_records *lur)
{
	struct update_params *params;
	size_t params_size;
	size_t ops_size;

	if (lur->lur_update_records == NULL ||
	    lur->lur_update_params == NULL)
		return 0;

	/* Extends the update records buffer if needed */
	params_size = update_params_size(lur->lur_update_params);
	ops_size = update_ops_size(&lur->lur_update_records->ur_ops);
	if (sizeof(struct update_records) + ops_size + params_size >=
	    lur->lur_update_records_size) {
		int rc;

		rc = lod_extend_update_records(lur,
					sizeof(struct update_records) +
					ops_size + params_size);
		if (rc != 0)
			return rc;
	}

	params = update_records_get_params(lur->lur_update_records);
	memcpy(params, lur->lur_update_params, params_size);

	/* Init update record header */
	lur->lur_update_records->ur_hdr.lrh_len =
		cfs_size_round(update_records_size(lur->lur_update_records));
	lur->lur_update_records->ur_hdr.lrh_type = UPDATE_REC;

	/* Dump updates for debugging purpose */
	update_records_dump(lur->lur_update_records, D_HA);

	return 0;
}


/**
 * Prepare cross-MDT operation.
 *
 * Create the update record buffer to record updates for cross-MDT operation,
 * add master sub transaction to lt_sub_trans_list, and declare the update
 * writes.
 *
 * During updates packing, all of parameters will be packed in
 * lur_update_params, and updates will be packed in lur_update_records.
 * Then in transaction stop, parameters and updates will be merged
 * into one updates buffer. (\see lod_prepare_updates_buf() ).
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device
 * \param[in] lth	lod transaction handle
 *
 * \retval		0 if preparation succeeds.
 * \retval		negative errno if preparation fails.
 */
static int lod_prepare_mulitple_node_trans(const struct lu_env *env,
					   struct lod_device *lod,
					   struct lod_thandle *lth)
{
	struct lod_update_records	*lur;
	struct lod_sub_thandle		*master_lst;
	int				rc;
	ENTRY;

	/* step 1 Prepare the update buffer for recording updates */
	if (lth->lt_update_records != NULL)
		RETURN(0);

	lur = &lod_env_info(env)->lti_lur;
	if (lur->lur_update_records == NULL) {
		rc = lod_create_update_records(lur);
		if (rc < 0)
			RETURN(rc);
	}

	if (lur->lur_update_params == NULL) {
		rc = lod_create_update_params(lur);
		if (rc < 0)
			RETURN(rc);
	}

	lur->lur_update_records->ur_ops.uops_count = 0;
	lur->lur_update_params->up_params_count = 0;
	lur->lur_update_records->ur_cookie = lth->lt_super.th_cookie;
	lth->lt_update_records = lur;

	/* step 2: Add master sub transaction to the lt_sub_trans_list */
	/* Note: we need to add the master sub transaction to the start
	 * of the list, so it will be executed first during trans start
	 * and trans stop */
	master_lst = lod_sub_create_trans(env, lth, lth->lt_child);
	if (IS_ERR(master_lst))
		RETURN(PTR_ERR(master_lst));
	list_add(&master_lst->lst_list, &lth->lt_sub_trans_list);

	/* step 3: declare updates write */
	rc = lod_declare_updates_write(env, lod, lth);

	RETURN(rc);
}

static int lod_trans_start(const struct lu_env *env, struct dt_device *dev,
			   struct thandle *th)
{
	struct lod_thandle	*lth;
	struct lod_sub_thandle	*lst;
	struct lod_device	*lod = dt2lod_dev((struct dt_device *) dev);
	int			rc = 0;
	ENTRY;

	/* Walk through all of sub transaction to see if it needs to
	 * record updates for this transaction */
	lth = container_of0(th, struct lod_thandle, lt_super);
	if (lth->lt_multiple_node) {
		rc = lod_prepare_mulitple_node_trans(env, lod, lth);
		if (rc < 0)
			RETURN(rc);
	}

	/* Usually it does not need to record update */
	rc = dt_trans_start(env, lod->lod_child, lth->lt_child);
	if (rc != 0)
		RETURN(rc);

	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		if (lth->lt_child->th_dev == lst->lst_dt)
			continue;

		rc = dt_trans_start(env, lst->lst_dt, lst->lst_child);
		if (rc < 0)
			break;
	}

	RETURN(rc);
}

/**
 * write update transaction
 *
 * Check if there are updates being recorded in this transaction,
 * it will write the record into the disk.
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device
 * \param[in] lth	lod transaction handle
 *
 * \retval		0 if writing succeeds
 * \retval		negative errno if writing fails
 */
static int lod_updates_write(const struct lu_env *env, struct lod_device *lod,
			     struct lod_thandle *lth)
{
	struct lod_update_records *lur = lth->lt_update_records;
	struct lod_sub_thandle	*lst;
	int			rc;
	ENTRY;

	/* merge the parameters and updates into one buffer */
	rc = lod_prepare_writing_updates(env, lur);
	if (rc < 0)
		RETURN(rc);

	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		if (lst->lst_update == NULL)
			continue;

		rc = lod_sub_updates_write(env, lur->lur_update_records,
					   lth, lst);
		if (rc != 0)
			break;
	}

	RETURN(rc);
}

static int lod_trans_stop(const struct lu_env *env, struct dt_device *dt,
			  struct thandle *th)
{
	struct lod_sub_thandle	*lst;
	struct lod_thandle	*lth;
	struct lod_device	*lod;
	int			rc;
	ENTRY;

	lod = dt2lod_dev(dt);
	lth = container_of0(th, struct lod_thandle, lt_super);
	if (lth->lt_update_records != NULL && th->th_result == 0) {
		rc = lod_updates_write(env, lod, lth);
		if (rc != 0) {
			CERROR("%s: write updates failed: rc = %d\n",
			       lod2obd(lod)->obd_name, rc);
			/* Still need call dt_trans_stop to release resources
			 * holding by the transaction */
		}
		lth->lt_update_records = NULL;
	}

	/* Stop master sub thandle first */
	lth->lt_child->th_result = th->th_result;
	rc = dt_trans_stop(env, lod->lod_child, lth->lt_child);

	list_for_each_entry(lst, &lth->lt_sub_trans_list, lst_list) {
		struct dt_device *dt_dev = lst->lst_dt;

		if (lth->lt_child->th_dev == lst->lst_dt)
			continue;

		lst->lst_child->th_result = th->th_result;
		rc = dt_trans_stop(env, dt_dev, lst->lst_child);
		if (rc < 0) {
			CERROR("%s: trans stop failed: rc = %d\n",
			       dt_dev->dd_lu_dev.ld_obd->obd_name, rc);
			break;
		}
	}

	if (rc != 0 || !lth->lt_multiple_node)
		/* If it is failed, destroy the thandle */
		lod_thandle_destroy(lth);

	RETURN(rc);
}

static void lod_conf_get(const struct lu_env *env,
			 const struct dt_device *dev,
			 struct dt_device_param *param)
{
	dt_conf_get(env, dt2lod_dev((struct dt_device *)dev)->lod_child, param);
}

static int lod_sync(const struct lu_env *env, struct dt_device *dev)
{
	struct lod_device   *lod = dt2lod_dev(dev);
	struct lod_ost_desc *ost;
	unsigned int         i;
	int                  rc = 0;
	ENTRY;

	lod_getref(&lod->lod_ost_descs);
	lod_foreach_ost(lod, i) {
		ost = OST_TGT(lod, i);
		LASSERT(ost && ost->ltd_ost);
		rc = dt_sync(env, ost->ltd_ost);
		if (rc) {
			CERROR("%s: can't sync %u: %d\n",
			       lod2obd(lod)->obd_name, i, rc);
			break;
		}
	}
	lod_putref(lod, &lod->lod_ost_descs);
	if (rc == 0)
		rc = dt_sync(env, lod->lod_child);

	RETURN(rc);
}

static int lod_ro(const struct lu_env *env, struct dt_device *dev)
{
	return dt_ro(env, dt2lod_dev(dev)->lod_child);
}

static int lod_commit_async(const struct lu_env *env, struct dt_device *dev)
{
	return dt_commit_async(env, dt2lod_dev(dev)->lod_child);
}

static int lod_init_capa_ctxt(const struct lu_env *env, struct dt_device *dev,
			      int mode, unsigned long timeout,
			      __u32 alg, struct lustre_capa_key *keys)
{
	struct dt_device *next = dt2lod_dev(dev)->lod_child;
	return dt_init_capa_ctxt(env, next, mode, timeout, alg, keys);
}

static const struct dt_device_operations lod_dt_ops = {
	.dt_root_get         = lod_root_get,
	.dt_statfs           = lod_statfs,
	.dt_trans_create     = lod_trans_create,
	.dt_trans_start      = lod_trans_start,
	.dt_trans_stop       = lod_trans_stop,
	.dt_conf_get         = lod_conf_get,
	.dt_sync             = lod_sync,
	.dt_ro               = lod_ro,
	.dt_commit_async     = lod_commit_async,
	.dt_init_capa_ctxt   = lod_init_capa_ctxt,
};

static int lod_connect_to_osd(const struct lu_env *env, struct lod_device *lod,
			      struct lustre_cfg *cfg)
{
	struct obd_connect_data *data = NULL;
	struct obd_device	*obd;
	char			*nextdev = NULL, *p, *s;
	int			 rc, len = 0;
	ENTRY;

	LASSERT(cfg);
	LASSERT(lod->lod_child_exp == NULL);

	/* compatibility hack: we still use old config logs
	 * which specify LOV, but we need to learn underlying
	 * OSD device, which is supposed to be:
	 *  <fsname>-MDTxxxx-osd
	 *
	 * 2.x MGS generates lines like the following:
	 *   #03 (176)lov_setup 0:lustre-MDT0000-mdtlov  1:(struct lov_desc)
	 * 1.8 MGS generates lines like the following:
	 *   #03 (168)lov_setup 0:lustre-mdtlov  1:(struct lov_desc)
	 *
	 * we use "-MDT" to differentiate 2.x from 1.8 */

	if ((p = lustre_cfg_string(cfg, 0)) && strstr(p, "-mdtlov")) {
		len = strlen(p) + 1;
		OBD_ALLOC(nextdev, len);
		if (nextdev == NULL)
			GOTO(out, rc = -ENOMEM);

		strcpy(nextdev, p);
		s = strstr(nextdev, "-mdtlov");
		if (unlikely(s == NULL)) {
			CERROR("unable to parse device name %s\n",
			       lustre_cfg_string(cfg, 0));
			GOTO(out, rc = -EINVAL);
		}

		if (strstr(nextdev, "-MDT")) {
			/* 2.x config */
			strcpy(s, "-osd");
		} else {
			/* 1.8 config */
			strcpy(s, "-MDT0000-osd");
		}
	} else {
		CERROR("unable to parse device name %s\n",
		       lustre_cfg_string(cfg, 0));
		GOTO(out, rc = -EINVAL);
	}

	OBD_ALLOC_PTR(data);
	if (data == NULL)
		GOTO(out, rc = -ENOMEM);

	obd = class_name2obd(nextdev);
	if (obd == NULL) {
		CERROR("can not locate next device: %s\n", nextdev);
		GOTO(out, rc = -ENOTCONN);
	}

	data->ocd_connect_flags = OBD_CONNECT_VERSION;
	data->ocd_version = LUSTRE_VERSION_CODE;

	rc = obd_connect(env, &lod->lod_child_exp, obd, &obd->obd_uuid,
			 data, NULL);
	if (rc) {
		CERROR("cannot connect to next dev %s (%d)\n", nextdev, rc);
		GOTO(out, rc);
	}

	lod->lod_dt_dev.dd_lu_dev.ld_site =
		lod->lod_child_exp->exp_obd->obd_lu_dev->ld_site;
	LASSERT(lod->lod_dt_dev.dd_lu_dev.ld_site);
	lod->lod_child = lu2dt_dev(lod->lod_child_exp->exp_obd->obd_lu_dev);

out:
	if (data)
		OBD_FREE_PTR(data);
	if (nextdev)
		OBD_FREE(nextdev, len);
	RETURN(rc);
}

static int lod_tgt_desc_init(struct lod_tgt_descs *ltd)
{
	mutex_init(&ltd->ltd_mutex);
	init_rwsem(&ltd->ltd_rw_sem);

	/* the OST array and bitmap are allocated/grown dynamically as OSTs are
	 * added to the LOD, see lod_add_device() */
	ltd->ltd_tgt_bitmap = CFS_ALLOCATE_BITMAP(32);
	if (ltd->ltd_tgt_bitmap == NULL)
		RETURN(-ENOMEM);

	ltd->ltd_tgts_size  = 32;
	ltd->ltd_tgtnr      = 0;

	ltd->ltd_death_row = 0;
	ltd->ltd_refcount  = 0;
	return 0;
}

static int lod_init0(const struct lu_env *env, struct lod_device *lod,
		     struct lu_device_type *ldt, struct lustre_cfg *cfg)
{
	struct dt_device_param ddp;
	struct obd_device     *obd;
	int		       rc;
	ENTRY;

	obd = class_name2obd(lustre_cfg_string(cfg, 0));
	if (obd == NULL) {
		CERROR("Cannot find obd with name %s\n",
		       lustre_cfg_string(cfg, 0));
		RETURN(-ENODEV);
	}

	obd->obd_lu_dev = &lod->lod_dt_dev.dd_lu_dev;
	lod->lod_dt_dev.dd_lu_dev.ld_obd = obd;
	lod->lod_dt_dev.dd_lu_dev.ld_ops = &lod_lu_ops;
	lod->lod_dt_dev.dd_ops = &lod_dt_ops;

	rc = lod_connect_to_osd(env, lod, cfg);
	if (rc)
		RETURN(rc);

	dt_conf_get(env, &lod->lod_dt_dev, &ddp);
	lod->lod_osd_max_easize = ddp.ddp_max_ea_size;

	/* setup obd to be used with old lov code */
	rc = lod_pools_init(lod, cfg);
	if (rc)
		GOTO(out_disconnect, rc);

	rc = lod_procfs_init(lod);
	if (rc)
		GOTO(out_pools, rc);

	spin_lock_init(&lod->lod_desc_lock);
	spin_lock_init(&lod->lod_connects_lock);
	lod_tgt_desc_init(&lod->lod_mdt_descs);
	lod_tgt_desc_init(&lod->lod_ost_descs);

	RETURN(0);

out_pools:
	lod_pools_fini(lod);
out_disconnect:
	obd_disconnect(lod->lod_child_exp);
	RETURN(rc);
}

static struct lu_device *lod_device_free(const struct lu_env *env,
					 struct lu_device *lu)
{
	struct lod_device *lod = lu2lod_dev(lu);
	struct lu_device  *next = &lod->lod_child->dd_lu_dev;
	ENTRY;

	LASSERTF(atomic_read(&lu->ld_ref) == 0, "lu is %p\n", lu);
	dt_device_fini(&lod->lod_dt_dev);
	OBD_FREE_PTR(lod);
	RETURN(next);
}

static struct lu_device *lod_device_alloc(const struct lu_env *env,
					  struct lu_device_type *type,
					  struct lustre_cfg *lcfg)
{
	struct lod_device *lod;
	struct lu_device  *lu_dev;

	OBD_ALLOC_PTR(lod);
	if (lod == NULL) {
		lu_dev = ERR_PTR(-ENOMEM);
	} else {
		int rc;

		lu_dev = lod2lu_dev(lod);
		dt_device_init(&lod->lod_dt_dev, type);
		rc = lod_init0(env, lod, type, lcfg);
		if (rc != 0) {
			lod_device_free(env, lu_dev);
			lu_dev = ERR_PTR(rc);
		}
	}

	return lu_dev;
}

static struct lu_device *lod_device_fini(const struct lu_env *env,
					 struct lu_device *d)
{
	struct lod_device *lod = lu2lod_dev(d);
	int		   rc;
	ENTRY;

	lod_pools_fini(lod);

	lod_procfs_fini(lod);

	lod_llog_daemon_fini(env, lod);

	lod_sub_fini_llog(env, lod->lod_child, &lod->lod_child_recovery_thread);

	rc = lod_fini_tgt(env, lod, &lod->lod_ost_descs, true);
	if (rc)
		CERROR("%s:can not fini ost descs %d\n",
			lod2obd(lod)->obd_name, rc);

	rc = lod_fini_tgt(env, lod, &lod->lod_mdt_descs, false);
	if (rc)
		CERROR("%s:can not fini mdt descs %d\n",
			lod2obd(lod)->obd_name, rc);

	RETURN(NULL);
}

/*
 * we use exports to track all LOD users
 */
static int lod_obd_connect(const struct lu_env *env, struct obd_export **exp,
			   struct obd_device *obd, struct obd_uuid *cluuid,
			   struct obd_connect_data *data, void *localdata)
{
	struct lod_device    *lod = lu2lod_dev(obd->obd_lu_dev);
	struct lustre_handle  conn;
	int                   rc;
	ENTRY;

	CDEBUG(D_CONFIG, "connect #%d\n", lod->lod_connects);

	rc = class_connect(&conn, obd, cluuid);
	if (rc)
		RETURN(rc);

	*exp = class_conn2export(&conn);

	spin_lock(&lod->lod_connects_lock);
	lod->lod_connects++;
	/* at the moment we expect the only user */
	LASSERT(lod->lod_connects == 1);
	spin_unlock(&lod->lod_connects_lock);

	RETURN(0);
}

/*
 * once last export (we don't count self-export) disappeared
 * lod can be released
 */
static int lod_obd_disconnect(struct obd_export *exp)
{
	struct obd_device *obd = exp->exp_obd;
	struct lod_device *lod = lu2lod_dev(obd->obd_lu_dev);
	int                rc, release = 0;
	ENTRY;

	/* Only disconnect the underlying layers on the final disconnect. */
	spin_lock(&lod->lod_connects_lock);
	lod->lod_connects--;
	if (lod->lod_connects != 0) {
		/* why should there be more than 1 connect? */
		spin_unlock(&lod->lod_connects_lock);
		CERROR("%s: disconnect #%d\n", exp->exp_obd->obd_name,
		       lod->lod_connects);
		goto out;
	}
	spin_unlock(&lod->lod_connects_lock);

	/* the last user of lod has gone, let's release the device */
	release = 1;

out:
	rc = class_disconnect(exp); /* bz 9811 */

	if (rc == 0 && release)
		class_manual_cleanup(obd);
	RETURN(rc);
}

LU_KEY_INIT(lod, struct lod_thread_info);

static void lod_key_fini(const struct lu_context *ctx,
		struct lu_context_key *key, void *data)
{
	struct lod_thread_info *info = data;
	/* allocated in lod_get_lov_ea
	 * XXX: this is overload, a tread may have such store but used only
	 * once. Probably better would be pool of such stores per LOD.
	 */
	if (info->lti_ea_store) {
		OBD_FREE_LARGE(info->lti_ea_store, info->lti_ea_store_size);
		info->lti_ea_store = NULL;
		info->lti_ea_store_size = 0;
	}
	lu_buf_free(&info->lti_linkea_buf);

	if (info->lti_lur.lur_update_records != NULL)
		OBD_FREE_LARGE(info->lti_lur.lur_update_records,
			       info->lti_lur.lur_update_records_size);
	if (info->lti_lur.lur_update_params != NULL)
		OBD_FREE_LARGE(info->lti_lur.lur_update_params,
			       info->lti_lur.lur_update_params_size);

	OBD_FREE_PTR(info);
}

/* context key: lod_thread_key */
LU_CONTEXT_KEY_DEFINE(lod, LCT_MD_THREAD);

LU_TYPE_INIT_FINI(lod, &lod_thread_key);

static struct lu_device_type_operations lod_device_type_ops = {
	.ldto_init           = lod_type_init,
	.ldto_fini           = lod_type_fini,

	.ldto_start          = lod_type_start,
	.ldto_stop           = lod_type_stop,

	.ldto_device_alloc   = lod_device_alloc,
	.ldto_device_free    = lod_device_free,

	.ldto_device_fini    = lod_device_fini
};

static struct lu_device_type lod_device_type = {
	.ldt_tags     = LU_DEVICE_DT,
	.ldt_name     = LUSTRE_LOD_NAME,
	.ldt_ops      = &lod_device_type_ops,
	.ldt_ctx_tags = LCT_MD_THREAD,
};

static int lod_obd_get_info(const struct lu_env *env, struct obd_export *exp,
			    __u32 keylen, void *key, __u32 *vallen, void *val,
			    struct lov_stripe_md *lsm)
{
	int rc = -EINVAL;

	if (KEY_IS(KEY_OSP_CONNECTED)) {
		struct obd_device	*obd = exp->exp_obd;
		struct lod_device	*d;
		struct lod_ost_desc	*ost;
		unsigned int		i;
		int			rc = 1;

		if (!obd->obd_set_up || obd->obd_stopping)
			RETURN(-EAGAIN);

		d = lu2lod_dev(obd->obd_lu_dev);
		lod_getref(&d->lod_ost_descs);
		lod_foreach_ost(d, i) {
			ost = OST_TGT(d, i);
			LASSERT(ost && ost->ltd_ost);

			rc = obd_get_info(env, ost->ltd_exp, keylen, key,
					  vallen, val, lsm);
			/* one healthy device is enough */
			if (rc == 0)
				break;
		}
		lod_putref(d, &d->lod_ost_descs);
		RETURN(rc);
	}

	RETURN(rc);
}

static struct obd_ops lod_obd_device_ops = {
	.o_owner        = THIS_MODULE,
	.o_connect      = lod_obd_connect,
	.o_disconnect   = lod_obd_disconnect,
	.o_get_info     = lod_obd_get_info,
	.o_pool_new     = lod_pool_new,
	.o_pool_rem     = lod_pool_remove,
	.o_pool_add     = lod_pool_add,
	.o_pool_del     = lod_pool_del,
};

static int __init lod_mod_init(void)
{
	struct obd_type	*type;
	int rc;

	rc = lu_kmem_init(lod_caches);
	if (rc)
		return rc;

	rc = class_register_type(&lod_obd_device_ops, NULL, true, NULL,
#ifndef HAVE_ONLY_PROCFS_SEQ
				 NULL,
#endif
				 LUSTRE_LOD_NAME, &lod_device_type);
	if (rc) {
		lu_kmem_fini(lod_caches);
		return rc;
	}

	updatelog_orig_logops = llog_osd_ops;
	updatelog_orig_logops.lop_add = llog_cat_add_rec;
	updatelog_orig_logops.lop_declare_add = llog_cat_declare_add_rec;

	/* create "lov" entry in procfs for compatibility purposes */
	type = class_search_type(LUSTRE_LOV_NAME);
	if (type != NULL && type->typ_procroot != NULL)
		return rc;

	type = class_search_type(LUSTRE_LOD_NAME);
	type->typ_procsym = lprocfs_seq_register("lov", proc_lustre_root,
						 NULL, NULL);
	if (IS_ERR(type->typ_procsym)) {
		CERROR("lod: can't create compat entry \"lov\": %d\n",
		       (int)PTR_ERR(type->typ_procsym));
		type->typ_procsym = NULL;
	}
	return rc;
}

static void __exit lod_mod_exit(void)
{
	class_unregister_type(LUSTRE_LOD_NAME);
	lu_kmem_fini(lod_caches);
}

MODULE_AUTHOR("Whamcloud, Inc. <http://www.whamcloud.com/>");
MODULE_DESCRIPTION("Lustre Logical Object Device ("LUSTRE_LOD_NAME")");
MODULE_LICENSE("GPL");

module_init(lod_mod_init);
module_exit(lod_mod_exit);

