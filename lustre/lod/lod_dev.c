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

#define LOD_CANCEL_MASTER_THREAD_NAME	"master"
#define LOD_CANCEL_SLAVE_THREAD_NAME	"slave"

/*
 * Lookup target by FID.
 *
 * Lookup MDT/OST target index by FID. Type of the target can be
 * specific or any.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[in] lod		lod device
 * \param[in] fid		FID
 * \param[out] tgt		result target index
 * \param[in] type		expected type of the target:
 *				LU_SEQ_RANGE_{MDT,OST,ANY}
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
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
	if (server_fld == NULL)
		RETURN(-EIO);

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

/* Slab for dt_txn_callback */
struct kmem_cache *lod_txn_callback_kmem;
static struct lu_kmem_descr lod_caches[] = {
	{
		.ckd_cache = &lod_object_kmem,
		.ckd_name  = "lod_obj",
		.ckd_size  = sizeof(struct lod_object)
	},
	{
		.ckd_cache = &lod_txn_callback_kmem,
		.ckd_name  = "lod_txn_callback",
		.ckd_size  = sizeof(struct dt_txn_callback)
	},
	{
		.ckd_cache = NULL
	}
};

static struct lu_device *lod_device_fini(const struct lu_env *env,
					 struct lu_device *d);

/**
 * Implementation of lu_device_operations::ldo_object_alloc() for LOD
 *
 * Allocates and initializes LOD's slice in the given object.
 *
 * see include/lu_object.h for the details.
 */
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

/**
 * Process the config log for all sub device.
 *
 * The function goes through all the targets in the given table
 * and apply given configuration command on to the targets.
 * Used to cleanup the targets at unmount.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[in] lod		lod device
 * \param[in] ltd		target's table to go through
 * \param[in] lcfg		configuration command to apply
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

static struct llog_operations updatelog_orig_logops;
struct lod_recovery_data {
	struct lod_device	*lrd_lod;
	struct lod_tgt_desc	*lrd_ltd;
	struct ptlrpc_thread	*lrd_thread;
	__u32			lrd_idx;
};

/**
 * recovery thread for update log
 *
 * Start recovery thread and prepare the sub llog, then it will retrieve
 * the update records from the correpondent MDT and do recovery.
 *
 * \param[in]arg	update recovery thread argument see lod_recovery_data
 *
 * \retval		0 if recovery succeeds
 * \retval		negative errno if recovery failed.
 */
static int lod_sub_recovery_thread(void *arg)
{
	struct lod_recovery_data	*lrd = arg;
	struct lod_device		*lod = lrd->lrd_lod;
	struct dt_device		*dt;
	struct ptlrpc_thread		*thread = lrd->lrd_thread;
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

	if (lrd->lrd_ltd == NULL)
		dt = lod->lod_child;
	else
		dt = lrd->lrd_ltd->ltd_tgt;

	rc = lod_sub_prep_llog(&env, lod, dt, lrd->lrd_idx);
	if (rc != 0)
		GOTO(out, rc);

	/* XXX do recovery in the following patches */

out:
	OBD_FREE_PTR(lrd);
	thread->t_flags = SVC_STOPPED;
	wake_up(&thread->t_ctl_waitq);
	lu_env_fini(&env);
	RETURN(rc);
}

/**
 * finish sub llog context
 *
 * Stop update recovery thread for the sub device, then cleanup the
 * correspondent llog ctxt.
 *
 * \param[in] env      execution environment
 * \param[in] lod      lod device to do update recovery
 * \param[in] thread   recovery thread on this sub device
 */
void lod_sub_fini_llog(const struct lu_env *env,
                      struct dt_device *dt, struct ptlrpc_thread *thread)
{
	struct obd_device       *obd;
	struct llog_ctxt        *ctxt;
	ENTRY;

	obd = dt->dd_lu_dev.ld_obd;
	CDEBUG(D_INFO, "%s: finish llog\n", obd->obd_name);
	/* Stop recovery thread first */
	if (thread != NULL && thread->t_flags & SVC_RUNNING) {
	       thread->t_flags = SVC_STOPPING;
	       wake_up(&thread->t_ctl_waitq);
	       wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_STOPPED);
	}

	ctxt = llog_get_context(obd, LLOG_UPDATELOG_ORIG_CTXT);
	if (ctxt == NULL)
	       RETURN_EXIT;

	if (ctxt->loc_handle != NULL)
	       llog_cat_close(env, ctxt->loc_handle);

	llog_cleanup(env, ctxt);

	RETURN_EXIT;
}

/**
 * Extract MDT target index from a device name.
 *
 * a helper function to extract index from the given device name
 * like "fsname-MDTxxxx-mdtlov"
 *
 * \param[in] lodname		device name
 * \param[out] mdt_index	extracted index
 *
 * \retval 0		on success
 * \retval -EINVAL	if the name is invalid
 */
int lodname2mdt_index(char *lodname, __u32 *mdt_index)
{
	unsigned long index;
	char *ptr, *tmp;

	/* 1.8 configs don't have "-MDT0000" at the end */
	ptr = strstr(lodname, "-MDT");
	if (ptr == NULL) {
		*mdt_index = 0;
		return 0;
	}

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

static struct llog_operations updatelog_orig_logops;

/**
 * Init sub llog context
 *
 * Setup update llog ctxt for update recovery threads, then start the
 * recovery thread (lod_sub_recovery_thread) to read update llog from
 * the correspondent MDT to do update recovery.
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device to do update recovery
 * \param[in] dt	sub dt device for which the recovery thread is
 *
 * \retval		0 if initialization succeeds.
 * \retval		negative errno if initialization fails.
 */
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
			GOTO(free_lrd, rc);
	} else {
		struct lod_tgt_descs	*ltd = &lod->lod_mdt_descs;
		struct lod_tgt_desc	*tgt = NULL;
		int			i;

		cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {

			tgt = LTD_TGT(ltd, i);
			if (tgt->ltd_tgt == dt) {
				index = tgt->ltd_index;
				sub_ltd = tgt;
				break;
			}
		}
		LASSERT(sub_ltd != NULL);
		OBD_ALLOC_PTR(sub_ltd->ltd_recovery_thread);
		if (sub_ltd->ltd_recovery_thread == NULL)
			GOTO(free_lrd, rc = -ENOMEM);

		thread = sub_ltd->ltd_recovery_thread;
	}

	lrd->lrd_lod = lod;
	lrd->lrd_ltd = sub_ltd;
	lrd->lrd_thread = thread;
	lrd->lrd_idx = index;
	init_waitqueue_head(&thread->t_ctl_waitq);

	obd = dt->dd_lu_dev.ld_obd;
	obd->obd_lvfs_ctxt.dt = dt;
	rc = llog_setup(env, obd, &obd->obd_olg, LLOG_UPDATELOG_ORIG_CTXT,
			NULL, &updatelog_orig_logops);
	if (rc < 0) {
		CERROR("%s: updatelog llog setup failed: rc = %d\n",
		       obd->obd_name, rc);
		GOTO(free_thread, rc);
	}

	/* Start the recovery thread */
	task = kthread_run(lod_sub_recovery_thread, lrd, "lsub_rec-%u",
			   index);
	if (IS_ERR(task)) {
		rc = PTR_ERR(task);
		CERROR("%s: cannot start recovery thread: rc = %d\n",
		       obd->obd_name, rc);
		GOTO(out_llog, rc = PTR_ERR(task));
	}

	l_wait_event(thread->t_ctl_waitq, thread->t_flags & SVC_RUNNING ||
					  thread->t_flags & SVC_STOPPED, &lwi);

	RETURN(0);
out_llog:
	lod_sub_fini_llog(env, dt, thread);
free_thread:
	if (lod->lod_child != dt) {
		OBD_FREE_PTR(sub_ltd->ltd_recovery_thread);
		sub_ltd->ltd_recovery_thread = NULL;
	}
free_lrd:
	OBD_FREE_PTR(lrd);
	RETURN(rc);
}

static inline int lclt_cancel_thread_running(struct lod_cancel_log_thread *lclt)
{
	return lclt->lclt_thread.t_flags & SVC_RUNNING;
}

static inline int lclt_cancel_thread_stopped(struct lod_cancel_log_thread *lclt)
{
	return lclt->lclt_thread.t_flags & SVC_STOPPED;
}

/**
 * Stop llog cancel thread
 *
 * Stop llog cancel(master/slave) thread on LOD and also destory
 * all of transaction in the list.
 *
 * \param[in]lclt	cancel log thread to be stopped.
 */
static void lod_stop_llog_cancel_thread(struct lod_cancel_log_thread *lclt)
{

	struct top_thandle	*top_th;
	struct top_thandle	*tmp;

	/* Stop cancel thread */
	if (!lclt_cancel_thread_running(lclt))
		return;

	lclt->lclt_thread.t_flags = SVC_STOPPING;
	wake_up(&lclt->lclt_waitq);
	wait_event(lclt->lclt_thread.t_ctl_waitq,
		   lclt->lclt_thread.t_flags & SVC_STOPPED);

	/* Destroy transaction in the list */
	spin_lock(&lclt->lclt_lock);
	list_for_each_entry_safe(top_th, tmp, &lclt->lclt_list,
				 tt_commit_list) {
		list_del_init(&top_th->tt_commit_list);
		top_thandle_put(top_th);
	}
	spin_unlock(&lclt->lclt_lock);
}

/**
 * Finish the update llog
 *
 * Stop both master and slave llog cancel thread and release all the
 * thandles in their list and also finish the update llog in its child OSD.
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device
 */
static void lod_fini_update_llog(const struct lu_env *env,
				 struct lod_device *lod)
{
	lod_stop_llog_cancel_thread(&lod->lod_cancel_master_thread);
	lod_stop_llog_cancel_thread(&lod->lod_cancel_slave_thread);

	lod_sub_fini_llog(env, lod->lod_child,
			  &lod->lod_child_recovery_thread);
}


/**
 * finish all sub llog
 *
 * cleanup all of sub llog ctxt on the LOD.
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device to do update recovery
 */
static void lod_sub_fini_all_llogs(const struct lu_env *env,
				   struct lod_device *lod)
{
	struct lod_tgt_descs *ltd = &lod->lod_mdt_descs;
	int i;

	/* Stop the update log commit cancel threads and finish master
	 * llog ctxt */
	lod_fini_update_llog(env, lod);

	lod_getref(ltd);
	cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {
		struct lod_tgt_desc	*tgt;

		tgt = LTD_TGT(ltd, i);
		if (tgt->ltd_recovery_thread != NULL) {
			lod_sub_fini_llog(env, tgt->ltd_tgt,
					  tgt->ltd_recovery_thread);
			OBD_FREE_PTR(tgt->ltd_recovery_thread);
			tgt->ltd_recovery_thread = NULL;
		}
	}

	lod_putref(lod, ltd);
}


/**
 * Implementation of lu_device_operations::ldo_process_config() for LOD
 *
 * The method is called by the configuration subsystem during setup,
 * cleanup and when the configuration changes. The method processes
 * few specific commands like adding/removing the targets, changing
 * the runtime parameters.

 * \param[in] env		LU environment provided by the caller
 * \param[in] dev		lod device
 * \param[in] lcfg		configuration command to apply
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 *
 * The examples are below.
 *
 * Add osc config log:
 * marker  20 (flags=0x01, v2.2.49.56) lustre-OST0001  'add osc'
 * add_uuid  nid=192.168.122.162@tcp(0x20000c0a87aa2)  0:  1:nidxxx
 * attach    0:lustre-OST0001-osc-MDT0001  1:osc  2:lustre-MDT0001-mdtlov_UUID
 * setup     0:lustre-OST0001-osc-MDT0001  1:lustre-OST0001_UUID  2:nid
 * lov_modify_tgts add 0:lustre-MDT0001-mdtlov  1:lustre-OST0001_UUID  2:1  3:1
 * marker  20 (flags=0x02, v2.2.49.56) lustre-OST0001  'add osc'
 *
 * Add mdc config log:
 * marker  10 (flags=0x01, v2.2.49.56) lustre-MDT0000  'add osp'
 * add_uuid  nid=192.168.122.162@tcp(0x20000c0a87aa2)  0:  1:nid
 * attach 0:lustre-MDT0000-osp-MDT0001  1:osp  2:lustre-MDT0001-mdtlov_UUID
 * setup     0:lustre-MDT0000-osp-MDT0001  1:lustre-MDT0000_UUID  2:nid
 * modify_mdc_tgts add 0:lustre-MDT0001  1:lustre-MDT0000_UUID  2:0  3:1
 * marker  10 (flags=0x02, v2.2.49.56) lustre-MDT0000_UUID  'add osp'
 */
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
			__u32 mdt_index;

			rc = lodname2mdt_index(lustre_cfg_string(lcfg, 0),
					       &mdt_index);
			if (rc != 0)
				GOTO(out, rc);

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

		rc = class_process_proc_param(PARAM_LOV, obd->obd_vars,
					      lcfg, obd);
		if (rc > 0)
			rc = 0;
		GOTO(out, rc);
	}
	case LCFG_PRE_CLEANUP: {
		lod_sub_process_config(env, lod, &lod->lod_mdt_descs, lcfg);
		lod_sub_process_config(env, lod, &lod->lod_ost_descs, lcfg);
		lod_sub_fini_all_llogs(env, lod);
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

/**
 * Implementation of lu_device_operations::ldo_recovery_complete() for LOD
 *
 * The method is called once the recovery is complete. This implementation
 * distributes the notification to all the known targets.
 *
 * see include/lu_object.h for the details
 */
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

/**
 * Cancel the update log on slave MDTs
 *
 * Cancel the update log on slave MDTs then destroy the thandle.
 *
 * \param[in] env	execution environment
 * \param[in] top_th	the top thandle whose updates log on slave
 *                      MDTs will be canceled.
 *
 * \retval		0 if cancellation succeeds.
 * \retval		negative errno if cancellation fails.
 */
static int lod_cancel_slave_log(const struct lu_env *env,
				struct top_thandle *top_th)
{
	struct sub_thandle *st;
	int rc = 0;
	ENTRY;

	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);
	top_thandle_dump(D_HA, top_th);

	/* Cancel update logs on other MDTs */
	list_for_each_entry(st, &top_th->tt_sub_trans_list, st_list) {
		struct llog_ctxt	*ctxt;
		struct obd_device	*obd;
		struct llog_cookie	*cookie;
		struct thandle		*th;
		int			rc1;

		if (st->st_update == NULL)
			continue;

		cookie = &st->st_update->stu_cookie;
		obd = st->st_dt->dd_lu_dev.ld_obd;
		ctxt = llog_get_context(obd, LLOG_UPDATELOG_ORIG_CTXT);
		LASSERT(ctxt);

		th = dt_trans_create(env, st->st_dt);
		if (IS_ERR(th))
			GOTO(out_put, rc1 = PTR_ERR(th));

		th->th_wait_submit = 1;
		rc1 = llog_cat_declare_cancel_records(env, ctxt->loc_handle, 1,
						      cookie, th);
		if (rc1 < 0)
			GOTO(stop_trans, rc1);

		rc1 = dt_trans_start(env, st->st_dt, th);
		if (rc1 < 0)
			GOTO(stop_trans, rc1);

		rc1 = llog_cat_cancel_records(env, ctxt->loc_handle, 1,
					     cookie, th);
		if (rc1 < 0)
			GOTO(stop_trans, rc1);
stop_trans:
		dt_trans_stop(env, st->st_dt, th);
out_put:
		llog_ctxt_put(ctxt);
		if (rc1 != 0 && rc1 != -ENOENT)
			rc = rc1;

		CDEBUG(D_HA, "%s: cancel update log "DOSTID": rc = %d\n",
		       obd->obd_name,
		       POSTID(&st->st_update->stu_cookie.lgc_lgl.lgl_oi), rc1);
	}

	if (rc == 0)
		top_thandle_put(top_th);

	RETURN(rc);
}

/**
 * Add thandle to the cancel thread list
 *
 * Add thandle to one the cancel thread list and wakeup the thread to process
 * it.
 *
 * \param[in] lclt	lod_cancel_log_thread to cancel update log.
 * \param[in] top_th	top_thandle to add to the list.
 */
void lod_add_thandle_to_cancel_list(struct lod_cancel_log_thread *lclt,
				    struct top_thandle *top_th)
{
	top_thandle_dump(D_HA, top_th);
	spin_lock(&lclt->lclt_lock);
	if (lclt_cancel_thread_running(lclt))
		list_add_tail(&top_th->tt_commit_list, &lclt->lclt_list);
	else
		top_thandle_put(top_th);
	spin_unlock(&lclt->lclt_lock);
	wake_up(&lclt->lclt_waitq);
}

/**
 * commit callback of master log cancellation
 *
 * Move the transaction from master list to the slave list after master
 * log cancellation is committed.
 *
 * \param[in]th		thandle of master log cancellation
 * \param[in]cookie	cookie of the callback, which is LOD device.
 */
static void lod_cancel_master_log_commit(struct lu_env *env, struct thandle *th,
					 struct dt_txn_commit_cb *cb, int err)
{
	struct top_thandle *master_th;
	struct lod_device *lod;
	ENTRY;

	LASSERT(cb->dcb_data != NULL);
	master_th = (struct top_thandle *)cb->dcb_data;
	OBD_FREE_PTR(cb);
	if (th->th_dev->dd_lu_dev.ld_obd->obd_stopping) {
		top_thandle_put(master_th);
		RETURN_EXIT;
	}


	top_thandle_dump(D_HA, master_th);
	lod = dt2lod_dev(master_th->tt_super.th_dev);

	/* Move this transaction to the slave cancel list */
	lod_add_thandle_to_cancel_list(&lod->lod_cancel_slave_thread,
				       master_th);

	RETURN_EXIT;
}

/**
 * Cancel the master log
 *
 * Cancel the update log on the master MDT, after the cancellation
 * is committed, the transaction will be moved to the slave list.
 *
 * \param[in] env	execution environment
 * \param[in] top_th	the thandle whose master update log will be
 *                      cancelled
 *
 * \retval		0 if cancellation succeeds.
 * \retval		negative errno if cancellation fails.
 */
static int lod_cancel_master_log(const struct lu_env *env,
				 struct top_thandle *committed_th)
{
	struct thandle		*th;
	struct obd_device	*master_obd;
	struct llog_ctxt	*master_ctxt;
	struct dt_txn_commit_cb	*master_dcb;
	struct lod_device	*lod;
	int			rc;
	ENTRY;

	/* The first sub thandle will always be master sub thandle */
	LASSERT(committed_th->tt_magic == TOP_THANDLE_MAGIC);
	top_thandle_dump(D_HA, committed_th);

	lod = dt2lod_dev(committed_th->tt_super.th_dev);
	master_obd = lod->lod_child->dd_lu_dev.ld_obd;

	master_ctxt = llog_get_context(master_obd,
				LLOG_UPDATELOG_ORIG_CTXT);
	LASSERT(master_ctxt != NULL);

	OBD_ALLOC_PTR(master_dcb);
	if (master_dcb == NULL)
		GOTO(out_ctxt, rc = -ENOMEM);

	th = dt_trans_create(env, lod->lod_child);
	if (IS_ERR(th))
		GOTO(out_ctxt, rc = PTR_ERR(th));

	rc = llog_cat_declare_cancel_records(env, master_ctxt->loc_handle, 1,
					     &committed_th->tt_master_cookie,
					     th);
	if (rc < 0)
		GOTO(out_trans, rc);

	rc = dt_trans_start_local(env, lod->lod_child, th);
	if (rc < 0)
		GOTO(out_trans, rc);

	rc = llog_cat_cancel_records(env, master_ctxt->loc_handle, 1,
				     &committed_th->tt_master_cookie, th);
	if (rc < 0)
		GOTO(out_trans, rc);

	master_dcb->dcb_func = lod_cancel_master_log_commit;
	master_dcb->dcb_magic = TRANS_COMMIT_CB_MAGIC;
	master_dcb->dcb_data = committed_th;
	INIT_LIST_HEAD(&master_dcb->dcb_linkage);
	strlcpy(master_dcb->dcb_name, "master_log_commit_cb",
		sizeof(master_dcb->dcb_name));

	rc = dt_trans_cb_add(th, master_dcb);
	if (rc != 0)
		GOTO(out_trans, rc);

out_trans:
	dt_trans_stop(env, lod->lod_child, th);
out_ctxt:
	llog_ctxt_put(master_ctxt);
	if (rc != 0 && master_dcb != NULL)
		OBD_FREE_PTR(master_dcb);

	RETURN(rc);
}

/**
 * Cancel the update log in the master/slave list
 *
 * Walk through the master/slave list and cancel the update log for those
 * transaction, which are already committed on all of MDTs.
 *
 * \param[in] env	execution environment
 * \param[in] lclt	lod cancel log thread where the list is
 *
 * \retval		only return 0 for now.
 */
static int lod_cancel_update_logs(const struct lu_env *env,
				  struct lod_cancel_log_thread *lclt)
{
	int	rc = 0;
	ENTRY;

	do {
		struct top_thandle *top_th;
		struct top_thandle *tmp;

		spin_lock(&lclt->lclt_lock);
		if (list_empty(&lclt->lclt_list)) {
			spin_unlock(&lclt->lclt_lock);
			break;
		}
		list_for_each_entry_safe(top_th, tmp, &lclt->lclt_list,
					 tt_commit_list) {
			list_del(&top_th->tt_commit_list);
			break;
		}
		spin_unlock(&lclt->lclt_lock);

		rc = lclt->lclt_cancel(env, top_th);
		if (rc < 0) {
			/* Add it back to the commit list */
			spin_lock(&lclt->lclt_lock);
			list_add_tail(&top_th->tt_commit_list,
				      &lclt->lclt_list);
			spin_unlock(&lclt->lclt_lock);
			break;
		}

		if (!lclt_cancel_thread_running(lclt))
			break;
	} while (1);

	RETURN(0);
}

/**
 * Check whether the cancel thread ready for processing
 *
 * Check whether there is thandle to be listed in lclt_list to
 * know whether cancel thread needs to be wakeup.
 *
 * \param[in] lod	lod device where cancel thread is
 *
 * \retval		true if it is ready
 * \retval		false if it is not ready
 */
static bool lod_ready_for_cancel_log(struct lod_cancel_log_thread *lclt)
{
	return !list_empty(&lclt->lclt_list);
}

/**
 * llog record cancel thread
 *
 * Cancel the update llog records in the commit list. After the distributed
 * transaction is committed, which is checked by lod_sub_update_txn_commit(),
 * it will be added to the commit list, then this thread will be wakeup,
 * and cancel the update logs for the transaction.
 *
 * \param[in] _arg	argument for cancel thread
 *
 * \retval		0 if thread is running successfully
 * \retval		negative errno if the thread can not be run.
 */
static int lod_llog_cancel_thread(void *_arg)
{
	struct lod_cancel_log_thread	*lclt = _arg;
	struct ptlrpc_thread	*thread = &lclt->lclt_thread;
	struct l_wait_info	 lwi = { 0 };
	struct lu_env		 env;
	int			 rc;

	ENTRY;

	rc = lu_env_init(&env, LCT_LOCAL | LCT_MD_THREAD);
	if (rc != 0)
		RETURN(rc);

	spin_lock(&lclt->lclt_lock);
	thread->t_flags = SVC_RUNNING;
	spin_unlock(&lclt->lclt_lock);
	wake_up(&thread->t_ctl_waitq);

	do {
		l_wait_event(lclt->lclt_waitq,
			     !lclt_cancel_thread_running(lclt) ||
			     lod_ready_for_cancel_log(lclt), &lwi);

		lod_cancel_update_logs(&env, lclt);

		if (!lclt_cancel_thread_running(lclt))
			break;
	} while (1);

	thread->t_flags = SVC_STOPPED;
	wake_up(&thread->t_ctl_waitq);

	lu_env_fini(&env);

	RETURN(0);
}

/**
 * Init update logs on all sub device
 *
 * LOD initialize update logs on all of sub devices. Because the initialization
 * process might need FLD lookup, see llog_osd_open()->dt_locate()->...->
 * lod_object_init(), this API has to be called after LOD is initialized.
 * \param[in] env	execution environment
 * \param[in] lod	lod device
 *
 * \retval		0 if update log is initialized successfully.
 * \retval		negative errno if initialization fails.
 */
static int lod_sub_init_llogs(const struct lu_env *env, struct lod_device *lod)
{
	struct lod_tgt_descs	*ltd = &lod->lod_mdt_descs;
	int			rc;
	int			i;
	ENTRY;

	/* llog must be setup after LOD is initialized, because llog
	 * initialization include FLD lookup */
	LASSERT(lod->lod_initialized);

	/* Init the llog in its own stack */
	rc = lod_sub_init_llog(env, lod, lod->lod_child);
	if (rc < 0)
		RETURN(rc);

	cfs_foreach_bit(ltd->ltd_tgt_bitmap, i) {
		struct lod_tgt_desc	*tgt;

		tgt = LTD_TGT(ltd, i);
		rc = lod_sub_init_llog(env, lod, tgt->ltd_tgt);
		if (rc != 0)
			break;
	}

	RETURN(rc);
}

/**
 * Start llog cancel thread
 *
 * Start llog cancel(master/slave) thread on LOD
 *
 * \param[in]lclt	cancel log thread to be started.
 *
 * \retval		0 if the thread is started successfully.
 * \retval		negative errno if the thread is not being
 *                      started.
 */
static int lod_start_llog_cancel_thread(struct lod_cancel_log_thread *lclt,
					const char *name, __u32 index)
{
	struct task_struct	*task;
	struct l_wait_info	 lwi = { 0 };
	ENTRY;

	spin_lock_init(&lclt->lclt_lock);
	INIT_LIST_HEAD(&lclt->lclt_list);

	init_waitqueue_head(&lclt->lclt_waitq);
	init_waitqueue_head(&lclt->lclt_thread.t_ctl_waitq);
	task = kthread_run(lod_llog_cancel_thread, lclt, "%s-%u", name, index);
	if (IS_ERR(task))
		RETURN(PTR_ERR(task));

	l_wait_event(lclt->lclt_thread.t_ctl_waitq,
		     lclt_cancel_thread_running(lclt) ||
		     lclt_cancel_thread_stopped(lclt), &lwi);
	RETURN(0);
}

/**
 * Init the update llog on LOD
 *
 * Initialize the update llog for local OSD, then start the llog cancel
 * thread which will cancel the update log records for those committed
 * distributed transaction.
 *
 * For distributed transaction, after it is committed,
 * 1. The transaction is first being put to the master_list, and master
 * cancel thread will cancel the master (local) log.
 * 2. After the cancellation is committed to disk, it will be move to the
 * the slave_list.
 * 3. slave cancel thread will then cancel the update log on slave MDTs
 * and destory the transaction.
 *
 * \param[in] env	execution environment
 * \param[in] lod	lod device
 *
 * \retval		0 if initialization succeeds.
 * \retval		negative errno if initialization fails.
 */
static int lod_init_update_llog(const struct lu_env *env,
				struct lod_device *lod)
{
	__u32	index;
	int	rc;
	ENTRY;

	rc = lodname2mdt_index(lod2obd(lod)->obd_name, &index);
	if (rc < 0)
		RETURN(rc);

	/* Start cancel master log thread */
	lod->lod_cancel_master_thread.lclt_cancel = lod_cancel_master_log;
	rc = lod_start_llog_cancel_thread(&lod->lod_cancel_master_thread,
					  LOD_CANCEL_MASTER_THREAD_NAME, index);
	if (rc < 0) {
		CERROR("%s: cannot start commit check thread: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		RETURN(rc);
	}

	/* Start cancel slave log thread */
	lod->lod_cancel_slave_thread.lclt_cancel = lod_cancel_slave_log;
	rc = lod_start_llog_cancel_thread(&lod->lod_cancel_slave_thread,
					  LOD_CANCEL_SLAVE_THREAD_NAME, index);
	if (rc < 0) {
		CERROR("%s: cannot start commit check thread: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		GOTO(stop_master, rc);
	}

	/* Init the llog in its own stack */
	rc = lod_sub_init_llogs(env, lod);
	if (rc < 0) {
		CERROR("%s: cannot init llog: rc = %d\n",
		       lod2obd(lod)->obd_name, rc);
		GOTO(stop_master, rc);
	}

stop_master:
	if (rc != 0)
		lod_stop_llog_cancel_thread(&lod->lod_cancel_master_thread);

	RETURN(rc);
}

/**
 * Implementation of lu_device_operations::ldo_prepare() for LOD
 *
 * see include/lu_object.h for the details.
 */
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

	lod->lod_initialized = 1;

	rc = dt_root_get(env, lod->lod_child, fid);
	if (rc < 0)
		RETURN(rc);

	root = dt_locate(env, lod->lod_child, fid);
	if (IS_ERR(root))
		RETURN(PTR_ERR(root));

	/* Create update log object */
	index = lu_site2seq(lod2lu_dev(lod)->ld_site)->ss_node_id;
	lu_update_log_fid(fid, index);

	dto = local_file_find_or_create_with_fid(env, lod->lod_child,
						 fid, root,
						 lod_updates_log_name,
				 		 S_IFREG | S_IRUGO | S_IWUSR);
	if (IS_ERR(dto))
		GOTO(out_put, rc = PTR_ERR(dto));

	/* since stack is not fully set up the local_storage uses own stack
	 * and we should drop its object from cache */
	lu_object_put_nocache(env, &dto->do_lu);

	rc = lod_init_update_llog(env, lod);
	if (rc != 0)
		GOTO(out_put, rc);

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

/**
 * Implementation of dt_device_operations::dt_root_get() for LOD
 *
 * see include/dt_object.h for the details.
 */
static int lod_root_get(const struct lu_env *env,
			struct dt_device *dev, struct lu_fid *f)
{
	return dt_root_get(env, dt2lod_dev(dev)->lod_child, f);
}

/**
 * Implementation of dt_device_operations::dt_statfs() for LOD
 *
 * see include/dt_object.h for the details.
 */
static int lod_statfs(const struct lu_env *env,
		      struct dt_device *dev, struct obd_statfs *sfs)
{
	return dt_statfs(env, dt2lod_dev(dev)->lod_child, sfs);
}

/**
 * Implementation of dt_device_operations::dt_trans_create() for LOD
 *
 * Creates a transaction using local (to this node) OSD.
 *
 * see include/dt_object.h for the details.
 */
static struct thandle *lod_trans_create(const struct lu_env *env,
					struct dt_device *dt)
{
	struct thandle *th;

	th = top_trans_create(env, dt2lod_dev(dt)->lod_child);
	if (IS_ERR(th))
		return th;

	th->th_dev = dt;

	return th;
}

/**
 * Implementation of dt_device_operations::dt_trans_start() for LOD
 *
 * Starts the set of local transactions using the targets involved
 * in declare phase. Initial support for the distributed transactions.
 *
 * see include/dt_object.h for the details.
 */
static int lod_trans_start(const struct lu_env *env, struct dt_device *dt,
			   struct thandle *th)
{
	return top_trans_start(env, dt2lod_dev(dt)->lod_child, th);
}

static int lod_trans_cb_add(struct thandle *th,
			    struct dt_txn_commit_cb *dcb)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	dt_trans_cb_add(top_th->tt_child, dcb);
	return 0;
}

static void lod_trans_commit_callback(struct thandle *th, void *arg)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);
	struct lod_cancel_log_thread *lclt = arg;

	/* Add thandle to the cancel list */
	lod_add_thandle_to_cancel_list(lclt, top_th);
}

/**
 * Implementation of dt_device_operations::dt_trans_stop() for LOD
 *
 * Stops the set of local transactions using the targets involved
 * in declare phase. Initial support for the distributed transactions.
 *
 * see include/dt_object.h for the details.
 */
static int lod_trans_stop(const struct lu_env *env, struct dt_device *dt,
			  struct thandle *th)
{
	struct top_thandle	*top_th = container_of(th, struct top_thandle,
						       tt_super);

	if (top_th->tt_multiple_node) {
		LASSERT(top_th->tt_commit_callback == NULL);
		top_th->tt_commit_callback = lod_trans_commit_callback;
		top_th->tt_commit_callback_arg =
				&dt2lod_dev(dt)->lod_cancel_master_thread;
	}
	return top_trans_stop(env, dt2lod_dev(dt)->lod_child, th);
}

/**
 * Implementation of dt_device_operations::dt_conf_get() for LOD
 *
 * Currently returns the configuration provided by the local OSD.
 *
 * see include/dt_object.h for the details.
 */
static void lod_conf_get(const struct lu_env *env,
			 const struct dt_device *dev,
			 struct dt_device_param *param)
{
	dt_conf_get(env, dt2lod_dev((struct dt_device *)dev)->lod_child, param);
}

/**
 * Implementation of dt_device_operations::dt_sync() for LOD
 *
 * Syncs all known OST targets. Very very expensive and used
 * rarely by LFSCK now. Should not be used in general.
 *
 * see include/dt_object.h for the details.
 */
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

/**
 * Implementation of dt_device_operations::dt_ro() for LOD
 *
 * Turns local OSD read-only, used for the testing only.
 *
 * see include/dt_object.h for the details.
 */
static int lod_ro(const struct lu_env *env, struct dt_device *dev)
{
	return dt_ro(env, dt2lod_dev(dev)->lod_child);
}

/**
 * Implementation of dt_device_operations::dt_commit_async() for LOD
 *
 * Asks local OSD to commit sooner.
 *
 * see include/dt_object.h for the details.
 */
static int lod_commit_async(const struct lu_env *env, struct dt_device *dev)
{
	return dt_commit_async(env, dt2lod_dev(dev)->lod_child);
}

/**
 * Not used
 */
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
	.dt_trans_cb_add     = lod_trans_cb_add,
};

/**
 * Connect to a local OSD.
 *
 * Used to connect to the local OSD at mount. OSD name is taken from the
 * configuration command passed. This connection is used to identify LU
 * site and pin the OSD from early removal.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[in] lod		lod device
 * \param[in] cfg		configuration command to apply
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

/**
 * Allocate and initialize target table.
 *
 * A helper function to initialize the target table and allocate
 * a bitmap of the available targets.
 *
 * \param[in] ltd		target's table to initialize
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

static int lod_txn_stop_cb(const struct lu_env *env, struct thandle *th,
			   void *cookie)
{
	struct top_thandle	*top_th;

	if (th->th_top == NULL)
		return 0;

	top_th = container_of(th, struct top_thandle, tt_super);
	LASSERT(top_th->tt_magic == TOP_THANDLE_MAGIC);

	if (top_th->tt_update_records == NULL)
		return 0;

	top_th->tt_update_records->tur_update_records->ur_batchid =
							th->th_transno;

	return 0;
}

/**
 * Initialize LOD device at setup.
 *
 * Initializes the given LOD device using the original configuration command.
 * The function initiates a connection to the local OSD and initializes few
 * internal structures like pools, target tables, etc.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[in] lod		lod device
 * \param[in] ldt		not used
 * \param[in] cfg		configuration command
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

	/* prepare transactions callbacks */
	lod->lod_txn_cb.dtc_txn_start = NULL;
	lod->lod_txn_cb.dtc_txn_stop = lod_txn_stop_cb;
	lod->lod_txn_cb.dtc_txn_commit = NULL;
	lod->lod_txn_cb.dtc_cookie = lod;
	lod->lod_txn_cb.dtc_tag = LCT_MD_THREAD;
	INIT_LIST_HEAD(&lod->lod_txn_cb.dtc_linkage);

	dt_txn_callback_add(lod->lod_child, &lod->lod_txn_cb);

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

/**
 * Implementation of lu_device_type_operations::ldto_device_free() for LOD
 *
 * Releases the memory allocated for LOD device.
 *
 * see include/lu_object.h for the details.
 */
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

/**
 * Implementation of lu_device_type_operations::ldto_device_alloc() for LOD
 *
 * Allocates LOD device and calls the helpers to initialize it.
 *
 * see include/lu_object.h for the details.
 */
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

/**
 * Implementation of lu_device_type_operations::ldto_device_fini() for LOD
 *
 * Releases the internal resources used by LOD device.
 *
 * see include/lu_object.h for the details.
 */
static struct lu_device *lod_device_fini(const struct lu_env *env,
					 struct lu_device *d)
{
	struct lod_device *lod = lu2lod_dev(d);
	int		   rc;
	ENTRY;

	lod_pools_fini(lod);

	lod_procfs_fini(lod);

	dt_txn_callback_del(lod->lod_child, &lod->lod_txn_cb);

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

/**
 * Implementation of obd_ops::o_connect() for LOD
 *
 * Used to track all the users of this specific LOD device,
 * so the device stays up until the last user disconnected.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[out] exp		export the caller will be using to access LOD
 * \param[in] obd		OBD device representing LOD device
 * \param[in] cluuid		unique identifier of the caller
 * \param[in] data		not used
 * \param[in] localdata		not used
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

/**
 *
 * Implementation of obd_ops::o_disconnect() for LOD
 *
 * When the caller doesn't need to use this LOD instance, it calls
 * obd_disconnect() and LOD releases corresponding export/reference count.
 * Once all the users gone, LOD device is released.
 *
 * \param[in] exp		export provided to the caller in obd_connect()
 *
 * \retval 0			on success
 * \retval negative		negated errno on error
 **/
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

/**
 * Implementation of obd_ops::o_get_info() for LOD
 *
 * Currently, there is only one supported key: KEY_OSP_CONNECTED , to provide
 * the caller binary status whether LOD has seen connection to any OST target.
 *
 * \param[in] env		LU environment provided by the caller
 * \param[in] exp		export of the caller
 * \param[in] keylen		len of the key
 * \param[in] key		the key
 * \param[in] vallen		not used
 * \param[in] val		not used
 * \param[in] lsm		not used
 *
 * \retval			0 if a connection was seen
 * \retval			-EAGAIN if LOD isn't running yet or no
 *				connection has been seen yet
 * \retval			-EINVAL if not supported key is requested
 **/
static int lod_obd_get_info(const struct lu_env *env, struct obd_export *exp,
			    __u32 keylen, void *key, __u32 *vallen, void *val,
			    struct lov_stripe_md *lsm)
{
	int rc = -EINVAL;

	if (KEY_IS(KEY_OSP_CONNECTED)) {
		struct obd_device	*obd = exp->exp_obd;
		struct lod_device	*d;
		struct lod_tgt_desc	*tgt;
		unsigned int		i;
		int			rc = 1;

		if (!obd->obd_set_up || obd->obd_stopping)
			RETURN(-EAGAIN);

		d = lu2lod_dev(obd->obd_lu_dev);
		lod_getref(&d->lod_ost_descs);
		lod_foreach_ost(d, i) {
			tgt = OST_TGT(d, i);
			LASSERT(tgt && tgt->ltd_tgt);
			rc = obd_get_info(env, tgt->ltd_exp, keylen, key,
					  vallen, val, lsm);
			/* one healthy device is enough */
			if (rc == 0)
				break;
		}
		lod_putref(d, &d->lod_ost_descs);

		lod_getref(&d->lod_mdt_descs);
		lod_foreach_mdt(d, i) {
			struct llog_ctxt *ctxt;

			tgt = MDT_TGT(d, i);
			LASSERT(tgt && tgt->ltd_tgt);
			ctxt = llog_get_context(tgt->ltd_tgt->dd_lu_dev.ld_obd,
						LLOG_UPDATELOG_ORIG_CTXT);
			if (ctxt == NULL) {
				rc = -EAGAIN;
				break;
			}
			if (ctxt->loc_handle == NULL) {
				rc = -EAGAIN;
				llog_ctxt_put(ctxt);
				break;
			}
			llog_ctxt_put(ctxt);
		}
		lod_putref(d, &d->lod_mdt_descs);

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

