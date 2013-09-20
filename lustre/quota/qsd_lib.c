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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2012, 2013, Intel Corporation.
 * Use is subject to license terms.
 *
 * Author: Johann Lombardi <johann.lombardi@intel.com>
 * Author: Niu    Yawei    <yawei.niu@intel.com>
 */

/*
 * Quota Slave Driver (QSD) management.
 *
 * The quota slave feature is implemented under the form of a library called
 * QSD. Each OSD device should create a QSD instance via qsd_init() which will
 * be used to manage quota enforcement for this device. This implies:
 * - completing the reintegration procedure with the quota master (aka QMT, see
 *   qmt_dev.c) to retrieve the latest quota settings and space distribution.
 * - managing quota locks in order to be notified of configuration changes.
 * - acquiring space from the QMT when quota space for a given user/group is
 *   close to exhaustion.
 * - allocating quota space to service threads for local request processing.
 *
 * Once the QSD instance created, the OSD device should invoke qsd_start()
 * when recovery is completed. This notifies the QSD that we are about to
 * process new requests on which quota should be strictly enforced.
 * Then, qsd_op_begin/end can be used to reserve/release/pre-acquire quota space
 * for/after each operation until shutdown where the QSD instance should be
 * freed via qsd_fini().
 */

#define DEBUG_SUBSYSTEM S_LQUOTA

#include <obd_class.h>
#include "qsd_internal.h"

struct kmem_cache *upd_kmem;

struct lu_kmem_descr qsd_caches[] = {
	{
		.ckd_cache = &upd_kmem,
		.ckd_name  = "upd_kmem",
		.ckd_size  = sizeof(struct qsd_upd_rec)
	},
	{
		.ckd_cache = NULL
	}
};

/* define qsd thread key */
LU_KEY_INIT_FINI(qsd, struct qsd_thread_info);
LU_CONTEXT_KEY_DEFINE(qsd, LCT_MD_THREAD | LCT_DT_THREAD | LCT_LOCAL);
LU_KEY_INIT_GENERIC(qsd);

/* some procfs helpers */
static int lprocfs_qsd_rd_state(char *page, char **start, off_t off,
				int count, int *eof, void *data)
{
	struct qsd_instance	*qsd = (struct qsd_instance *)data;
	char			 enabled[5];
	int			 rc;

	LASSERT(qsd != NULL);

	memset(enabled, 0, sizeof(enabled));
	if (qsd_type_enabled(qsd, USRQUOTA))
		strcat(enabled, "u");
	if (qsd_type_enabled(qsd, GRPQUOTA))
		strcat(enabled, "g");
	if (strlen(enabled) == 0)
		strcat(enabled, "none");

	rc = snprintf(page, count,
		      "target name:    %s\n"
		      "pool ID:        %d\n"
		      "type:           %s\n"
		      "quota enabled:  %s\n"
		      "conn to master: %s\n",
		      qsd->qsd_svname, qsd->qsd_pool_id,
		      qsd->qsd_is_md ? "md" : "dt", enabled,
		      qsd->qsd_exp_valid ? "setup" : "not setup yet");

	if (qsd->qsd_prepared) {
		memset(enabled, 0, sizeof(enabled));
		if (qsd->qsd_type_array[USRQUOTA]->qqi_acct_obj != NULL)
			strcat(enabled, "u");
		if (qsd->qsd_type_array[GRPQUOTA]->qqi_acct_obj != NULL)
			strcat(enabled, "g");
		if (strlen(enabled) == 0)
			strcat(enabled, "none");
		rc +=  snprintf(page + rc, count - rc,
				"space acct:     %s\n"
				"user uptodate:  glb[%d],slv[%d],reint[%d]\n"
				"group uptodate: glb[%d],slv[%d],reint[%d]\n",
				enabled,
				qsd->qsd_type_array[USRQUOTA]->qqi_glb_uptodate,
				qsd->qsd_type_array[USRQUOTA]->qqi_slv_uptodate,
				qsd->qsd_type_array[USRQUOTA]->qqi_reint,
				qsd->qsd_type_array[GRPQUOTA]->qqi_glb_uptodate,
				qsd->qsd_type_array[GRPQUOTA]->qqi_slv_uptodate,
				qsd->qsd_type_array[GRPQUOTA]->qqi_reint);
	}
	return rc;
}

static int lprocfs_qsd_rd_enabled(char *page, char **start, off_t off,
				  int count, int *eof, void *data)
{
	struct qsd_instance	*qsd = (struct qsd_instance *)data;
	char			 enabled[5];

	LASSERT(qsd != NULL);

	memset(enabled, 0, sizeof(enabled));
	if (qsd_type_enabled(qsd, USRQUOTA))
		strcat(enabled, "u");
	if (qsd_type_enabled(qsd, GRPQUOTA))
		strcat(enabled, "g");
	if (strlen(enabled) == 0)
		strcat(enabled, "none");

	return snprintf(page, count, "%s\n", enabled);
}

/* force reintegration procedure to be executed.
 * Used for test/debugging purpose */
static int lprocfs_qsd_wr_force_reint(struct file *file, const char *buffer,
				      unsigned long count, void *data)
{
	struct qsd_instance	*qsd = (struct qsd_instance *)data;
	int			 rc = 0, qtype;

	LASSERT(qsd != NULL);

	write_lock(&qsd->qsd_lock);
	if (qsd->qsd_stopping) {
		/* don't mess up with shutdown procedure, it is already
		 * complicated enough */
		rc = -ESHUTDOWN;
	} else if (!qsd->qsd_prepared) {
		rc = -EAGAIN;
	} else {
		/* mark all indexes as stale */
		for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++) {
			qsd->qsd_type_array[qtype]->qqi_glb_uptodate = false;
			qsd->qsd_type_array[qtype]->qqi_slv_uptodate = false;
		}
	}
	write_unlock(&qsd->qsd_lock);

	if (rc)
		return rc;

	/* kick off reintegration */
	for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++) {
		rc = qsd_start_reint_thread(qsd->qsd_type_array[qtype]);
		if (rc)
			break;
	}
	return rc == 0 ? count : rc;
}

static int lprocfs_qsd_rd_timeout(char *page, char **start, off_t off,
				  int count, int *eof, void *data)
{
	struct qsd_instance	*qsd = (struct qsd_instance *)data;
	LASSERT(qsd != NULL);

	return snprintf(page, count, "%d\n", qsd_wait_timeout(qsd));
}

static int lprocfs_qsd_wr_timeout(struct file *file, const char *buffer,
				  unsigned long count, void *data)
{
	struct qsd_instance	*qsd = (struct qsd_instance *)data;
	int			 timeout, rc;
	LASSERT(qsd != NULL);

	rc = lprocfs_write_helper(buffer, count, &timeout);
	if (rc)
		return rc;
	if (timeout < 0)
		return -EINVAL;

	qsd->qsd_timeout = timeout;
	return count;
}

static struct lprocfs_vars lprocfs_quota_qsd_vars[] = {
	{ "info", lprocfs_qsd_rd_state, 0, 0},
	{ "enabled", lprocfs_qsd_rd_enabled, 0, 0},
	{ "force_reint", 0, lprocfs_qsd_wr_force_reint, 0},
	{ "timeout", lprocfs_qsd_rd_timeout, lprocfs_qsd_wr_timeout, 0},
	{ NULL }
};

/*
 * Callback function invoked by the OSP layer when the connection to the master
 * has been set up.
 *
 * \param data - is a pointer to the qsd_instance
 *
 * \retval - 0 on success, appropriate error on failure
 */
static int qsd_conn_callback(void *data)
{
	struct qsd_instance *qsd = (struct qsd_instance *)data;
	int                  type;
	ENTRY;

	/* qsd_exp should now be valid */
	LASSERT(qsd->qsd_exp);

	qsd->qsd_ns = class_exp2obd(qsd->qsd_exp)->obd_namespace;

	write_lock(&qsd->qsd_lock);
	/* notify that qsd_exp is now valid */
	qsd->qsd_exp_valid = true;
	write_unlock(&qsd->qsd_lock);

	/* Now that the connection to master is setup, we can initiate the
	 * reintegration procedure for quota types which are enabled.
	 * It is worth noting that, if the qsd_instance hasn't been started
	 * already, then we can only complete the first two steps of the
	 * reintegration procedure (i.e. global lock enqueue and slave
	 * index transfer) since the space usage reconciliation (i.e.
	 * step 3) will have to wait for qsd_start() to be called */
	for (type = USRQUOTA; type < MAXQUOTAS; type++) {
		struct qsd_qtype_info *qqi = qsd->qsd_type_array[type];
		wake_up(&qqi->qqi_reint_thread.t_ctl_waitq);
	}

	RETURN(0);
}

/*
 * Release qsd_qtype_info structure which contains data associated with a
 * given quota type. This releases the accounting objects.
 * It's called on OSD cleanup when the qsd instance is released.
 *
 * \param env - is the environment passed by the caller
 * \param qsd - is the qsd instance managing the qsd_qtype_info structure
 *              to be released
 * \param qtype - is the quota type to be shutdown
 */
static void qsd_qtype_fini(const struct lu_env *env, struct qsd_instance *qsd,
			   int qtype)
{
	struct qsd_qtype_info	*qqi;
	ENTRY;

	if (qsd->qsd_type_array[qtype] == NULL)
		RETURN_EXIT;
	qqi = qsd->qsd_type_array[qtype];
	qsd->qsd_type_array[qtype] = NULL;

	/* all deferred work lists should be empty */
	LASSERT(cfs_list_empty(&qqi->qqi_deferred_glb));
	LASSERT(cfs_list_empty(&qqi->qqi_deferred_slv));

	/* shutdown lquota site */
	if (qqi->qqi_site != NULL && !IS_ERR(qqi->qqi_site)) {
		lquota_site_free(env, qqi->qqi_site);
		qqi->qqi_site = NULL;
	}

	/* by now, all qqi users should have gone away */
	LASSERT(cfs_atomic_read(&qqi->qqi_ref) == 1);
	lu_ref_fini(&qqi->qqi_reference);

	/* release accounting object */
	if (qqi->qqi_acct_obj != NULL && !IS_ERR(qqi->qqi_acct_obj)) {
		lu_object_put(env, &qqi->qqi_acct_obj->do_lu);
		qqi->qqi_acct_obj = NULL;
	}

	/* release slv index */
	if (qqi->qqi_slv_obj != NULL && !IS_ERR(qqi->qqi_slv_obj)) {
		lu_object_put(env, &qqi->qqi_slv_obj->do_lu);
		qqi->qqi_slv_obj = NULL;
		qqi->qqi_slv_ver = 0;
	}

	/* release global index */
	if (qqi->qqi_glb_obj != NULL && !IS_ERR(qqi->qqi_glb_obj)) {
		lu_object_put(env, &qqi->qqi_glb_obj->do_lu);
		qqi->qqi_glb_obj = NULL;
		qqi->qqi_glb_ver = 0;
	}

	OBD_FREE_PTR(qqi);
	EXIT;
}

/*
 * Allocate and initialize a qsd_qtype_info structure for quota type \qtype.
 * This opens the accounting object and initializes the proc file.
 * It's called on OSD start when the qsd_prepare() is invoked on the qsd
 * instance.
 *
 * \param env  - the environment passed by the caller
 * \param qsd  - is the qsd instance which will be in charge of the new
 *               qsd_qtype_info instance.
 * \param qtype - is quota type to set up
 *
 * \retval - 0 on success and qsd->qsd_type_array[qtype] is allocated,
 *           appropriate error on failure
 */
static int qsd_qtype_init(const struct lu_env *env, struct qsd_instance *qsd,
			  int qtype)
{
	struct qsd_qtype_info	*qqi;
	int			 rc;
	struct obd_uuid		 uuid;
	ENTRY;

	LASSERT(qsd->qsd_type_array[qtype] == NULL);

	/* allocate structure for this quota type */
	OBD_ALLOC_PTR(qqi);
	if (qqi == NULL)
		RETURN(-ENOMEM);
	qsd->qsd_type_array[qtype] = qqi;
	cfs_atomic_set(&qqi->qqi_ref, 1); /* referenced from qsd */

	/* set backpointer and other parameters */
	qqi->qqi_qsd   = qsd;
	qqi->qqi_qtype = qtype;
	lu_ref_init(&qqi->qqi_reference);
	lquota_generate_fid(&qqi->qqi_fid, qsd->qsd_pool_id, QSD_RES_TYPE(qsd),
			    qtype);
	qqi->qqi_glb_uptodate = false;
	qqi->qqi_slv_uptodate = false;
	qqi->qqi_reint        = false;
	init_waitqueue_head(&qqi->qqi_reint_thread.t_ctl_waitq);
	thread_set_flags(&qqi->qqi_reint_thread, SVC_STOPPED);
	CFS_INIT_LIST_HEAD(&qqi->qqi_deferred_glb);
	CFS_INIT_LIST_HEAD(&qqi->qqi_deferred_slv);

	/* open accounting object */
	LASSERT(qqi->qqi_acct_obj == NULL);
	qqi->qqi_acct_obj = acct_obj_lookup(env, qsd->qsd_dev, qtype);
	if (IS_ERR(qqi->qqi_acct_obj)) {
		CDEBUG(D_QUOTA, "%s: no %s space accounting support rc:%ld\n",
		       qsd->qsd_svname, QTYPE_NAME(qtype),
		       PTR_ERR(qqi->qqi_acct_obj));
		qqi->qqi_acct_obj = NULL;
		qsd->qsd_acct_failed = true;
	}

	/* open global index copy */
	LASSERT(qqi->qqi_glb_obj == NULL);
	qqi->qqi_glb_obj = lquota_disk_glb_find_create(env, qsd->qsd_dev,
						       qsd->qsd_root,
						       &qqi->qqi_fid, true);
	if (IS_ERR(qqi->qqi_glb_obj)) {
		CERROR("%s: can't open global index copy "DFID" %ld\n",
		       qsd->qsd_svname, PFID(&qqi->qqi_fid),
		       PTR_ERR(qqi->qqi_glb_obj));
		GOTO(out, rc = PTR_ERR(qqi->qqi_glb_obj));
	}
	qqi->qqi_glb_ver = dt_version_get(env, qqi->qqi_glb_obj);

	/* open slave index copy */
	LASSERT(qqi->qqi_slv_obj == NULL);
	obd_str2uuid(&uuid, qsd->qsd_svname);
	qqi->qqi_slv_obj = lquota_disk_slv_find_create(env, qsd->qsd_dev,
						       qsd->qsd_root,
						       &qqi->qqi_fid, &uuid,
						       true);
	if (IS_ERR(qqi->qqi_slv_obj)) {
		CERROR("%s: can't open slave index copy "DFID" %ld\n",
		       qsd->qsd_svname, PFID(&qqi->qqi_fid),
		       PTR_ERR(qqi->qqi_slv_obj));
		GOTO(out, rc = PTR_ERR(qqi->qqi_slv_obj));
	}
	qqi->qqi_slv_ver = dt_version_get(env, qqi->qqi_slv_obj);

	/* allocate site */
	qqi->qqi_site = lquota_site_alloc(env, qqi, false, qtype, &qsd_lqe_ops);
	if (IS_ERR(qqi->qqi_site)) {
		CERROR("%s: can't allocate site "DFID" %ld\n", qsd->qsd_svname,
		       PFID(&qqi->qqi_fid), PTR_ERR(qqi->qqi_site));
		GOTO(out, rc = PTR_ERR(qqi->qqi_site));
	}

	/* register proc entry for accounting & global index copy objects */
	rc = lprocfs_seq_create(qsd->qsd_proc,
				qtype == USRQUOTA ? "acct_user" : "acct_group",
				0444, &lprocfs_quota_seq_fops,
				qqi->qqi_acct_obj);
	if (rc) {
		CERROR("%s: can't add procfs entry for accounting file %d\n",
		       qsd->qsd_svname, rc);
		GOTO(out, rc);
	}

	rc = lprocfs_seq_create(qsd->qsd_proc,
				qtype == USRQUOTA ? "limit_user" : "limit_group",
				0444, &lprocfs_quota_seq_fops,
				qqi->qqi_glb_obj);
	if (rc) {
		CERROR("%s: can't add procfs entry for global index copy %d\n",
		       qsd->qsd_svname, rc);
		GOTO(out, rc);
	}
	EXIT;
out:
	if (rc)
		qsd_qtype_fini(env, qsd, qtype);
	return rc;
}

/*
 * Release a qsd_instance. Companion of qsd_init(). This releases all data
 * structures associated with the quota slave (on-disk objects, lquota entry
 * tables, ...).
 * This function should be called when the OSD is shutting down.
 *
 * \param env - is the environment passed by the caller
 * \param qsd - is the qsd instance to shutdown
 */
void qsd_fini(const struct lu_env *env, struct qsd_instance *qsd)
{
	int	qtype;
	ENTRY;

	if (unlikely(qsd == NULL))
		RETURN_EXIT;

	CDEBUG(D_QUOTA, "%s: initiating QSD shutdown\n", qsd->qsd_svname);
	write_lock(&qsd->qsd_lock);
	qsd->qsd_stopping = true;
	write_unlock(&qsd->qsd_lock);

	/* remove qsd proc entry */
	if (qsd->qsd_proc != NULL) {
		lprocfs_remove(&qsd->qsd_proc);
		qsd->qsd_proc = NULL;
	}

	/* stop the writeback thread */
	qsd_stop_upd_thread(qsd);

	/* shutdown the reintegration threads */
	for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++) {
		if (qsd->qsd_type_array[qtype] == NULL)
			continue;
		qsd_stop_reint_thread(qsd->qsd_type_array[qtype]);
	}

	if (qsd->qsd_ns != NULL) {
		qsd->qsd_ns = NULL;
	}

	/* free per-quota type data */
	for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++)
		qsd_qtype_fini(env, qsd, qtype);

	/* deregister connection to the quota master */
	qsd->qsd_exp_valid = false;
	lustre_deregister_lwp_item(&qsd->qsd_exp);

	/* release per-filesystem information */
	if (qsd->qsd_fsinfo != NULL) {
		down(&qsd->qsd_fsinfo->qfs_sem);
		/* remove from the list of fsinfo */
		cfs_list_del_init(&qsd->qsd_link);
		up(&qsd->qsd_fsinfo->qfs_sem);
		qsd_put_fsinfo(qsd->qsd_fsinfo);
		qsd->qsd_fsinfo = NULL;
	}

	/* release quota root directory */
	if (qsd->qsd_root != NULL) {
		lu_object_put(env, &qsd->qsd_root->do_lu);
		qsd->qsd_root = NULL;
	}

	/* release reference on dt_device */
	if (qsd->qsd_dev != NULL) {
		lu_ref_del(&qsd->qsd_dev->dd_lu_dev.ld_reference, "qsd", qsd);
		lu_device_put(&qsd->qsd_dev->dd_lu_dev);
		qsd->qsd_dev = NULL;
	}

	CDEBUG(D_QUOTA, "%s: QSD shutdown completed\n", qsd->qsd_svname);
	OBD_FREE_PTR(qsd);
	EXIT;
}
EXPORT_SYMBOL(qsd_fini);

/*
 * Create a new qsd_instance to be associated with backend osd device
 * identified by \dev.
 *
 * \param env    - the environment passed by the caller
 * \param svname - is the service name of the OSD device creating this instance
 * \param dev    - is the dt_device where to store quota index files
 * \param osd_proc - is the procfs parent directory where to create procfs file
 *                   related to this new qsd instance
 *
 * \retval - pointer to new qsd_instance associated with dev \dev on success,
 *           appropriate error on failure
 */
struct qsd_instance *qsd_init(const struct lu_env *env, char *svname,
			      struct dt_device *dev,
			      cfs_proc_dir_entry_t *osd_proc,
			      char *pool_name,
			      int pool_id)
{
	struct qsd_thread_info	*qti = qsd_info(env);
	struct qsd_instance	*qsd;
	int			 rc, type, idx;
	char			*proc_name;
	int			 length;
	ENTRY;

	/* only configure qsd for MDT & OST */
	type = server_name2index(svname, &idx, NULL);
	if (type != LDD_F_SV_TYPE_MDT && type != LDD_F_SV_TYPE_OST)
		RETURN(ERR_PTR(-EINVAL));

	if (!equi(pool_name == NULL, pool_id == 0))
		RETURN(ERR_PTR(-EINVAL));

	/* allocate qsd instance */
	OBD_ALLOC_PTR(qsd);
	if (qsd == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	length = strlen(QSD_DIR) + LOV_MAXPOOLNAME + 2;
	OBD_ALLOC(proc_name, length);
	if (proc_name == NULL) {
		OBD_FREE_PTR(qsd);
		RETURN(ERR_PTR(-ENOMEM));
	}

	if (pool_name != NULL)
		snprintf(proc_name, length - 1, "%s_%s", QSD_DIR, pool_name);
	else
		snprintf(proc_name, length - 1, "%s", QSD_DIR);
	if (pool_name)
		strncpy(qsd->qsd_pool_name, pool_name,
			sizeof(qsd->qsd_pool_name) - 1);
	/* generic initializations */
	rwlock_init(&qsd->qsd_lock);
	CFS_INIT_LIST_HEAD(&qsd->qsd_link);
	thread_set_flags(&qsd->qsd_upd_thread, SVC_STOPPED);
	init_waitqueue_head(&qsd->qsd_upd_thread.t_ctl_waitq);
	CFS_INIT_LIST_HEAD(&qsd->qsd_upd_list);
	spin_lock_init(&qsd->qsd_adjust_lock);
	CFS_INIT_LIST_HEAD(&qsd->qsd_adjust_list);
	qsd->qsd_prepared = false;
	qsd->qsd_started = false;
	/* initialize refcount to 1, hash table will then grab an additional
	 * reference */
	atomic_set(&qsd->qsd_ref, 1);

	/* copy service name */
	if (strlcpy(qsd->qsd_svname, svname, sizeof(qsd->qsd_svname))
	    >= sizeof(qsd->qsd_svname))
		GOTO(out, rc = -E2BIG);

	/* grab reference on osd device */
	lu_device_get(&dev->dd_lu_dev);
	lu_ref_add(&dev->dd_lu_dev.ld_reference, "qsd", qsd);
	qsd->qsd_dev = dev;

	/* we only support pool ID 0 (default data or metadata pool) for the
	 * time being. A different pool ID could be assigned to this target via
	 * the configuration log in the future */
	qsd->qsd_pool_id  = pool_id;

	/* get fsname from svname */
	rc = server_name2fsname(svname, qti->qti_buf, NULL);
	if (rc) {
		CERROR("%s: fail to extract filesystem name\n", svname);
		GOTO(out, rc);
	}

	/* look up quota setting for the filesystem the target belongs to */
	qsd->qsd_fsinfo = qsd_get_fsinfo(qti->qti_buf, 1);
	if (qsd->qsd_fsinfo == NULL) {
		CERROR("%s: failed to locate filesystem information\n", svname);
		GOTO(out, rc = -EINVAL);
	}

	/* add in the list of lquota_fsinfo */
	down(&qsd->qsd_fsinfo->qfs_sem);
	list_add_tail(&qsd->qsd_link, &qsd->qsd_fsinfo->qfs_qsd_list);
	up(&qsd->qsd_fsinfo->qfs_sem);

	/* register procfs directory */
	qsd->qsd_proc = lprocfs_register(proc_name, osd_proc,
					 lprocfs_quota_qsd_vars, qsd);
	if (IS_ERR(qsd->qsd_proc)) {
		rc = PTR_ERR(qsd->qsd_proc);
		qsd->qsd_proc = NULL;
		CERROR("%s: fail to create quota slave proc entry (%d)\n",
		       svname, rc);
		GOTO(out, rc);
        }
	EXIT;
out:
	OBD_FREE(proc_name, length);
	if (rc) {
		qsd_fini(env, qsd);
		return ERR_PTR(rc);
	}
	RETURN(qsd);
}
EXPORT_SYMBOL(qsd_init);

/*
 * Initialize on-disk structures in order to manage quota enforcement for
 * the target associated with the qsd instance \qsd and starts the reintegration
 * procedure for each quota type as soon as possible.
 * The last step of the reintegration will be completed once qsd_start() is
 * called, at which points the space reconciliation with the master will be
 * executed.
 * This function must be called when the server stack is fully configured,
 * typically when ->ldo_prepare is called across the stack.
 *
 * \param env - the environment passed by the caller
 * \param qsd - is qsd_instance to prepare
 *
 * \retval - 0 on success, appropriate error on failure
 */
int qsd_prepare(const struct lu_env *env, struct qsd_instance *qsd,
		struct dt_object *root)
{
	struct qsd_thread_info	*qti = qsd_info(env);
	int			 qtype, rc = 0;
	ENTRY;

	if (unlikely(qsd == NULL))
		RETURN(0);

	read_lock(&qsd->qsd_lock);
	if (qsd->qsd_prepared) {
		CERROR("%s: qsd instance already prepared\n", qsd->qsd_svname);
		rc = -EALREADY;
	}
	read_unlock(&qsd->qsd_lock);
	if (rc)
		RETURN(rc);

	/* Record whether this qsd instance is managing quota enforcement for a
	 * MDT (i.e. inode quota) or OST (block quota) */
	if (lu_device_is_md(qsd->qsd_dev->dd_lu_dev.ld_site->ls_top_dev)) {
		qsd->qsd_is_md = true;
		qsd->qsd_sync_threshold = LQUOTA_LEAST_QUNIT(LQUOTA_RES_MD);
	} else {
		qsd->qsd_sync_threshold = LQUOTA_LEAST_QUNIT(LQUOTA_RES_DT);
	}

	/* look-up on-disk directory for the quota slave */
	sprintf(qti->qti_buf, "%s_0x%x", QSD_DIR, qsd->qsd_pool_id);
	qsd->qsd_root = lquota_disk_dir_find_create(env, qsd->qsd_dev, root,
						    qti->qti_buf);
	if (IS_ERR(qsd->qsd_root)) {
		rc = PTR_ERR(qsd->qsd_root);
		qsd->qsd_root = NULL;
		CERROR("%s: failed to create quota slave root dir (%d)\n",
		       qsd->qsd_svname, rc);
		RETURN(rc);
	}

	/* initialize per-quota type data */
	for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++) {
		rc = qsd_qtype_init(env, qsd, qtype);
		if (rc)
			RETURN(rc);
	}

	/* pools successfully setup, mark the qsd as prepared */
	write_lock(&qsd->qsd_lock);
	qsd->qsd_prepared = true;
	write_unlock(&qsd->qsd_lock);

	/* start reintegration thread for each type, if required */
	for (qtype = USRQUOTA; qtype < MAXQUOTAS; qtype++) {
		struct qsd_qtype_info	*qqi = qsd->qsd_type_array[qtype];

		if (qsd_type_enabled(qsd, qtype) && qsd->qsd_acct_failed) {
			LCONSOLE_ERROR("%s: can't enable quota enforcement "
				       "since space accounting isn't functional"
				       ". Please run tunefs.lustre --quota on "
				       "an unmounted filesystem if not done "
				       "already\n", qsd->qsd_svname);
			break;
		}

		rc = qsd_start_reint_thread(qqi);
		if (rc) {
			CERROR("%s: failed to start reint thread for type %s "
			       "(%d)\n", qsd->qsd_svname, QTYPE_NAME(qtype),
			       rc);
			RETURN(rc);
		}
	}

	/* start writeback thread */
	rc = qsd_start_upd_thread(qsd);
	if (rc) {
		CERROR("%s: failed to start writeback thread (%d)\n",
		       qsd->qsd_svname, rc);
		RETURN(rc);
	}

	/* generate osp name */
	rc = tgt_name2lwpname((char *)qsd->qsd_svname, qti->qti_buf);
	if (rc) {
		CERROR("%s: failed to generate ospname (%d)\n",
		       qsd->qsd_svname, rc);
		RETURN(rc);
	}

	/* the connection callback will start the reintegration
	 * procedure if quota is enabled */
	rc = lustre_register_lwp_item(qti->qti_buf, &qsd->qsd_exp,
				      qsd_conn_callback, (void *)qsd);
	if (rc) {
		CERROR("%s: fail to get connection to master (%d)\n",
		       qsd->qsd_svname, rc);
		RETURN(rc);
	}

	RETURN(0);
}
EXPORT_SYMBOL(qsd_prepare);

/*
 * Start a qsd instance. This will complete the last step of the reintegration
 * procedure as soon as possible (provided that the master is reachable).
 * This should be called when recovery has been completed and quota should now
 * be enforced on every operations.
 *
 * \param env - the environment passed by the caller
 * \param qsd - is the qsd instance associated with the osd device to start
 */
int qsd_start(const struct lu_env *env, struct qsd_instance *qsd)
{
	int	type, rc = 0;
	ENTRY;

	if (unlikely(qsd == NULL))
		RETURN(0);

	write_lock(&qsd->qsd_lock);
	if (!qsd->qsd_prepared) {
		CERROR("%s: can't start qsd instance since it wasn't properly "
		       "initialized\n", qsd->qsd_svname);
		rc = -EFAULT;
	} else if (qsd->qsd_started) {
		CERROR("%s: qsd instance already started\n", qsd->qsd_svname);
		rc = -EALREADY;
	} else {
		/* notify that the qsd_instance is now started */
		qsd->qsd_started = true;
	}
	write_unlock(&qsd->qsd_lock);

	if (rc)
		RETURN(rc);

	/* Trigger the 3rd step of reintegration: If usage > granted, acquire
	 * up to usage; If usage < granted, release down to usage.  */
	for (type = USRQUOTA; type < MAXQUOTAS; type++) {
		struct qsd_qtype_info	*qqi = qsd->qsd_type_array[type];
		wake_up(&qqi->qqi_reint_thread.t_ctl_waitq);
	}

	RETURN(rc);
}
EXPORT_SYMBOL(qsd_start);

void lustre_register_quota_process_config(int (*qpc)(struct lustre_cfg *lcfg));

/*
 * Global initialization performed at module load time
 */
int qsd_glb_init(void)
{
	int	rc;

	rc = lu_kmem_init(qsd_caches);
	if (rc)
		return rc;

	qsd_key_init_generic(&qsd_thread_key, NULL);
	lu_context_key_register(&qsd_thread_key);
	lustre_register_quota_process_config(qsd_process_config);

	return 0;
}

/*
 * Companion of qsd_glb_init() called at module unload time
 */
void qsd_glb_fini(void)
{
	lustre_register_quota_process_config(NULL);
	lu_kmem_fini(qsd_caches);
	lu_context_key_degister(&qsd_thread_key);
}

static inline void qsd_getref(struct qsd_instance *qsd)
{
	cfs_atomic_inc(&qsd->qsd_ref);
}

void qsd_putref(const struct lu_env *env,
		struct qsd_instance *qsd)
{
	LASSERT(atomic_read(&qsd->qsd_ref) > 0);
	if (cfs_atomic_dec_and_test(&qsd->qsd_ref))
		qsd_fini(env, qsd);
}
EXPORT_SYMBOL(qsd_putref);

static inline void qsd_putref_locked(struct qsd_instance *qsd)
{
	LASSERT(cfs_atomic_read(&qsd->qsd_ref) > 1);
	cfs_atomic_dec(&qsd->qsd_ref);
}

static unsigned qsd_hash_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
	return cfs_hash_u32_hash(*((__u32 *)key), mask);
}

static void *qsd_hash_key(cfs_hlist_node_t *hnode)
{
	struct qsd_instance *qsd;
	qsd = cfs_hlist_entry(hnode, struct qsd_instance, qsd_osd_hash);
	return &qsd->qsd_pool_id;
}

static int qsd_hash_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct qsd_instance *qsd;
	qsd = cfs_hlist_entry(hnode, struct qsd_instance, qsd_osd_hash);
	return qsd->qsd_pool_id == *((__u32 *)key);
}

static void *qsd_hash_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct qsd_instance, qsd_osd_hash);
}

static void qsd_hash_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct qsd_instance *qsd;
	qsd = cfs_hlist_entry(hnode, struct qsd_instance, qsd_osd_hash);
	qsd_getref(qsd);
}

static void qsd_hash_put_locked(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct qsd_instance *qsd;
	qsd = cfs_hlist_entry(hnode, struct qsd_instance, qsd_osd_hash);
	qsd_putref_locked(qsd);
}

static void qsd_hash_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	CERROR("Should not have any item left!\n");
}

cfs_hash_ops_t qsd_hash_ops = {
	.hs_hash	= qsd_hash_hash,
	.hs_key		= qsd_hash_key,
	.hs_keycmp	= qsd_hash_keycmp,
	.hs_object	= qsd_hash_object,
	.hs_get		= qsd_hash_get,
	.hs_put_locked	= qsd_hash_put_locked,
	.hs_exit	= qsd_hash_exit
};
EXPORT_SYMBOL(qsd_hash_ops);

int qsd_set_init(const struct lu_env *env, struct quota_set *set)
{
	set->qs_hash = cfs_hash_create("POOL_HASH",
				       HASH_POOLS_CUR_BITS,
				       HASH_POOLS_MAX_BITS,
				       HASH_POOLS_BKT_BITS, 0,
				       CFS_HASH_MIN_THETA,
				       CFS_HASH_MAX_THETA,
				       &qsd_hash_ops,
				       CFS_HASH_DEFAULT);
	if (!set->qs_hash)
		return -ENOMEM;

	CFS_INIT_LIST_HEAD(&set->qs_list);
	init_rwsem(&set->qs_sem);

	set->qs_inited = true;
	set->qs_started = false;
	return 0;
}
EXPORT_SYMBOL(qsd_set_init);

int qsd_set_prepare(const struct lu_env *env, struct quota_set *set,
		    struct dt_object *root)
{
	cfs_list_t *l;
	struct qsd_instance *qsd;
	int rc = 0;

	LASSERT(set->qs_inited);
	down_read(&set->qs_sem);
	cfs_list_for_each(l, &set->qs_list) {
		qsd = cfs_list_entry(l, struct qsd_instance, qsd_osd_link);
		CERROR("prepare pool %d\n", qsd->qsd_pool_id);
		rc = qsd_prepare(env, qsd, root);
		if (rc)
			break;
	}
	up_read(&set->qs_sem);

	return rc;
}
EXPORT_SYMBOL(qsd_set_prepare);

int qsd_set_start(const struct lu_env *env, struct quota_set *set)
{
	cfs_list_t *l;
	struct qsd_instance *qsd;
	int rc = 0;

	LASSERT(set->qs_inited);
	down_read(&set->qs_sem);
	cfs_list_for_each(l, &set->qs_list) {
		qsd = cfs_list_entry(l, struct qsd_instance, qsd_osd_link);
		rc = qsd_start(env, qsd);
		if (rc)
			break;
	}
	up_write(&set->qs_sem);
	set->qs_started = true;

	return rc;
}
EXPORT_SYMBOL(qsd_set_start);

void qsd_set_fini(const struct lu_env *env, struct quota_set *set)
{
	cfs_list_t *l, *tmp;
	struct qsd_instance *qsd;

	LASSERT(set->qs_inited);
	down_write(&set->qs_sem);
	cfs_list_for_each_safe(l, tmp, &set->qs_list) {
		qsd = cfs_list_entry(l, struct qsd_instance, qsd_osd_link);
		cfs_hash_del(set->qs_hash, &qsd->qsd_pool_id,
			     &qsd->qsd_osd_hash);
		/* remove from list */
		cfs_list_del_init(&qsd->qsd_osd_link);
		/* release extra reference taken in qsd_init */
		qsd_putref(env, qsd);
	}
	LASSERT(cfs_list_empty(&set->qs_list));
	up_write(&set->qs_sem);
	cfs_hash_putref(set->qs_hash);
	set->qs_inited = false;
	set->qs_started = false;
}
EXPORT_SYMBOL(qsd_set_fini);

int qsd_set_add(struct quota_set *set, struct qsd_instance *qsd)
{
	int rc;

	LASSERT(set->qs_inited);
	rc = cfs_hash_add_unique(set->qs_hash, &qsd->qsd_pool_id,
				   &qsd->qsd_osd_hash);
	if (rc)
		return rc;

	down_write(&set->qs_sem);
	cfs_list_add(&qsd->qsd_osd_link, &set->qs_list);
	up_write(&set->qs_sem);

	return rc;
}
EXPORT_SYMBOL(qsd_set_add);

struct qsd_instance *qsd_set_del(struct quota_set *set, int pool_id)
{
	struct qsd_instance *qsd;

	LASSERT(set->qs_inited);
	qsd = cfs_hash_del_key(set->qs_hash, (void *)&pool_id);
	if (qsd == NULL)
		return qsd;

	down_write(&set->qs_sem);
	cfs_list_del_init(&qsd->qsd_osd_link);
	up_write(&set->qs_sem);

	return qsd;
}
EXPORT_SYMBOL(qsd_set_del);

struct qsd_instance *qsd_set_lookup(struct quota_set *set, int pool_id)
{
	struct qsd_instance *qsd;

	LASSERT(set->qs_inited);
	qsd = cfs_hash_lookup(set->qs_hash, (void *)&pool_id);
	return qsd;
}
EXPORT_SYMBOL(qsd_set_lookup);
