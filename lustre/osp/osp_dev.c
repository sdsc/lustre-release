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
 * Copyright (c) 2011, 2012, Intel, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osp/osp_dev.c
 *
 * Lustre OST Proxy Device
 *
 * Author: Alex Zhuravlev <alexey.zhuravlev@intel.com>
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <obd_class.h>
#include <lustre_param.h>

#include "osp_internal.h"

/* Slab for OSP object allocation */
cfs_mem_cache_t *osp_object_kmem;

static struct lu_kmem_descr osp_caches[] = {
	{
		.ckd_cache = &osp_object_kmem,
		.ckd_name  = "osp_obj",
		.ckd_size  = sizeof(struct osp_object)
	},
	{
		.ckd_cache = NULL
	}
};

struct lu_object *osp_object_alloc(const struct lu_env *env,
				   const struct lu_object_header *hdr,
				   struct lu_device *d)
{
	struct lu_object_header	*h;
	struct osp_object	*o;
	struct lu_object	*l;

	LASSERT(hdr == NULL);

	OBD_SLAB_ALLOC_PTR_GFP(o, osp_object_kmem, CFS_ALLOC_IO);
	if (o != NULL) {
		l = &o->opo_obj.do_lu;
		h = &o->opo_header;

		lu_object_header_init(h);
		dt_object_init(&o->opo_obj, h, d);
		lu_object_add_top(h, l);

		l->lo_ops = &osp_lu_obj_ops;

		return l;
	} else {
		return NULL;
	}
}

static int osp_shutdown(const struct lu_env *env, struct osp_device *d)
{
	struct obd_import	*imp;
	int			 rc = 0;

	ENTRY;

	imp = d->opd_obd->u.cli.cl_import;

	/* Mark import deactivated now, so we don't try to reconnect if any
	 * of the cleanup RPCs fails (e.g. ldlm cancel, etc).  We don't
	 * fully deactivate the import, or that would drop all requests. */
	cfs_spin_lock(&imp->imp_lock);
	imp->imp_deactive = 1;
	cfs_spin_unlock(&imp->imp_lock);

	ptlrpc_deactivate_import(imp);

	/* Some non-replayable imports (MDS's OSCs) are pinged, so just
	 * delete it regardless.  (It's safe to delete an import that was
	 * never added.) */
	(void) ptlrpc_pinger_del_import(imp);

	rc = ptlrpc_disconnect_import(imp, 0);
	if (rc)
		CERROR("can't disconnect: %d\n", rc);

	ptlrpc_invalidate_import(imp);

	RETURN(rc);
}

static int osp_process_config(const struct lu_env *env,
			      struct lu_device *dev, struct lustre_cfg *lcfg)
{
	struct osp_device		*d = lu2osp_dev(dev);
	struct lprocfs_static_vars	 lvars = { 0 };
	int				 rc;

	ENTRY;

	switch(lcfg->lcfg_command) {
	case LCFG_CLEANUP:
		lu_dev_del_linkage(dev->ld_site, dev);
		rc = osp_shutdown(env, d);
		break;
	case LCFG_PARAM:
		lprocfs_osp_init_vars(&lvars);

		LASSERT(d->opd_obd);
		rc = class_process_proc_param(PARAM_OSP, lvars.obd_vars,
					      lcfg, d->opd_obd);
		if (rc > 0)
			rc = 0;
		if (rc == -ENOSYS) {
			/* class_process_proc_param() haven't found matching
			 * parameter and returned ENOSYS so that layer(s)
			 * below could use that. But OSP is the bottom, so
			 * just ignore it */
			CERROR("%s: unknown param %s\n",
			       (char *)lustre_cfg_string(lcfg, 0),
			       (char *)lustre_cfg_string(lcfg, 1));
			rc = 0;
		}
		break;
	default:
		CERROR("%s: unknown command %u\n",
		       (char *)lustre_cfg_string(lcfg, 0), lcfg->lcfg_command);
		rc = 0;
		break;
	}

	RETURN(rc);
}

static int osp_recovery_complete(const struct lu_env *env,
				 struct lu_device *dev)
{
	struct osp_device	*osp = lu2osp_dev(dev);
	int			 rc = 0;

	ENTRY;
	osp->opd_recovery_completed = 1;
	RETURN(rc);
}

const struct lu_device_operations osp_lu_ops = {
	.ldo_object_alloc	= osp_object_alloc,
	.ldo_process_config	= osp_process_config,
	.ldo_recovery_complete	= osp_recovery_complete,
};

static int osp_sync(const struct lu_env *env, struct dt_device *dev)
{
	ENTRY;

	/*
	 * XXX: wake up sync thread, command it to start flushing asap?
	 */

	RETURN(0);
}

static const struct dt_device_operations osp_dt_ops = {
	.dt_sync	= osp_sync,
};

static int osp_connect_to_osd(const struct lu_env *env, struct osp_device *m,
			      const char *nextdev)
{
	struct obd_connect_data	*data = NULL;
	struct obd_device	*obd;
	int			 rc;

	ENTRY;

	LASSERT(m->opd_storage_exp == NULL);

	OBD_ALLOC_PTR(data);
	if (data == NULL)
		RETURN(-ENOMEM);

	obd = class_name2obd(nextdev);
	if (obd == NULL) {
		CERROR("can't locate next device: %s\n", nextdev);
		GOTO(out, rc = -ENOTCONN);
	}

	rc = obd_connect(env, &m->opd_storage_exp, obd, &obd->obd_uuid, data,
			 NULL);
	if (rc) {
		CERROR("cannot connect to next dev %s (%d)\n", nextdev, rc);
		GOTO(out, rc);
	}

	m->opd_dt_dev.dd_lu_dev.ld_site =
		m->opd_storage_exp->exp_obd->obd_lu_dev->ld_site;
	LASSERT(m->opd_dt_dev.dd_lu_dev.ld_site);
	m->opd_storage = lu2dt_dev(m->opd_storage_exp->exp_obd->obd_lu_dev);

out:
	OBD_FREE_PTR(data);
	RETURN(rc);
}

static int osp_init0(const struct lu_env *env, struct osp_device *m,
		     struct lu_device_type *ldt, struct lustre_cfg *cfg)
{
	struct lprocfs_static_vars	 lvars = { 0 };
	struct proc_dir_entry		*osc_proc_dir;
	struct obd_import		*imp;
	class_uuid_t			 uuid;
	char				*src, *ost, *mdt, *osdname = NULL;
	int				 rc, idx;

	ENTRY;

	m->opd_obd = class_name2obd(lustre_cfg_string(cfg, 0));
	if (m->opd_obd == NULL) {
		CERROR("Cannot find obd with name %s\n",
		       lustre_cfg_string(cfg, 0));
		RETURN(-ENODEV);
	}

	/* There is no record in the MDT configuration for the local disk
	 * device, so we have to extract this from elsewhere in the profile.
	 * The only information we get at setup is from the OSC records:
	 * setup 0:{fsname}-OSTxxxx-osc[-MDTxxxx] 1:lustre-OST0000_UUID 2:NID
	 * Note that 1.8 generated configs are missing the -MDTxxxx part.
	 * We need to reconstruct the name of the underlying OSD from this:
	 * {fsname}-{svname}-osd, for example "lustre-MDT0000-osd".  We
	 * also need to determine the OST index from this - will be used
	 * to calculate the offset in shared lov_objids file later */

	src = lustre_cfg_string(cfg, 0);
	if (src == NULL)
		RETURN(-EINVAL);

	ost = strstr(src, "-OST");
	if (ost == NULL)
		RETURN(-EINVAL);

	idx = simple_strtol(ost + 4, &mdt, 16);
	if (mdt[0] != '-' || idx > INT_MAX || idx < 0) {
		CERROR("%s: invalid OST index in '%s'\n",
		       m->opd_obd->obd_name, src);
		GOTO(out_fini, rc = -EINVAL);
	}
	m->opd_index = idx;

	idx = ost - src;
	/* check the fsname length, and after this everything else will fit */
	if (idx > MTI_NAME_MAXLEN) {
		CERROR("%s: fsname too long in '%s'\n",
		       m->opd_obd->obd_name, src);
		GOTO(out_fini, rc = -EINVAL);
	}

	OBD_ALLOC(osdname, MAX_OBD_NAME);
	if (osdname == NULL)
		GOTO(out_fini, rc = -ENOMEM);

	memcpy(osdname, src, idx); /* copy just the fsname part */
	osdname[idx] = '\0';

	mdt = strstr(mdt, "-MDT");
	if (mdt == NULL) /* 1.8 configs don't have "-MDT0000" at the end */
		strcat(osdname, "-MDT0000");
	else
		strcat(osdname, mdt);
	strcat(osdname, "-osd");
	CDEBUG(D_HA, "%s: connect to %s (%s)\n",
	       m->opd_obd->obd_name, osdname, src);

	m->opd_dt_dev.dd_lu_dev.ld_ops = &osp_lu_ops;
	m->opd_dt_dev.dd_ops = &osp_dt_ops;
	m->opd_obd->obd_lu_dev = &m->opd_dt_dev.dd_lu_dev;

	rc = osp_connect_to_osd(env, m, osdname);
	if (rc)
		GOTO(out_fini, rc);

	rc = ptlrpcd_addref();
	if (rc)
		GOTO(out_disconnect, rc);

	rc = client_obd_setup(m->opd_obd, cfg);
	if (rc) {
		CERROR("%s: can't setup obd: %d\n", m->opd_obd->obd_name, rc);
		GOTO(out_ref, rc);
	}

	lprocfs_osp_init_vars(&lvars);
	if (lprocfs_obd_setup(m->opd_obd, lvars.obd_vars) == 0)
		ptlrpc_lprocfs_register_obd(m->opd_obd);

	/* for compatibility we link old procfs's OSC entries to osp ones */
	osc_proc_dir = lprocfs_srch(proc_lustre_root, "osc");
	if (osc_proc_dir) {
		cfs_proc_dir_entry_t	*symlink = NULL;
		char			*name;

		OBD_ALLOC(name, strlen(m->opd_obd->obd_name) + 1);
		if (name == NULL)
			GOTO(out, rc = -ENOMEM);

		strcpy(name, m->opd_obd->obd_name);
		if (strstr(name, "osc"))
			symlink = lprocfs_add_symlink(name, osc_proc_dir,
						      "../osp/%s",
						      m->opd_obd->obd_name);
		OBD_FREE(name, strlen(m->opd_obd->obd_name) + 1);
		m->opd_symlink = symlink;
	}

	/*
	 * Initiate connect to OST
	 */
	ll_generate_random_uuid(uuid);
	class_uuid_unparse(uuid, &m->opd_cluuid);

	imp = m->opd_obd->u.cli.cl_import;

	rc = ptlrpc_init_import(imp);
	if (rc)
		GOTO(out, rc);
	if (osdname)
		OBD_FREE(osdname, MAX_OBD_NAME);
	RETURN(0);

out:
	ptlrpc_lprocfs_unregister_obd(m->opd_obd);
	lprocfs_obd_cleanup(m->opd_obd);
	class_destroy_import(m->opd_obd->u.cli.cl_import);
	client_obd_cleanup(m->opd_obd);
out_ref:
	ptlrpcd_decref();
out_disconnect:
	obd_disconnect(m->opd_storage_exp);
out_fini:
	if (osdname)
		OBD_FREE(osdname, MAX_OBD_NAME);
	RETURN(rc);
}

static struct lu_device *osp_device_free(const struct lu_env *env,
					 struct lu_device *lu)
{
	struct osp_device *m = lu2osp_dev(lu);

	ENTRY;

	dt_device_fini(&m->opd_dt_dev);
	OBD_FREE_PTR(m);
	RETURN(NULL);
}

static struct lu_device *osp_device_alloc(const struct lu_env *env,
					  struct lu_device_type *t,
					  struct lustre_cfg *lcfg)
{
	struct osp_device *m;
	struct lu_device  *l;

	OBD_ALLOC_PTR(m);
	if (m == NULL) {
		l = ERR_PTR(-ENOMEM);
	} else {
		int rc;

		l = osp2lu_dev(m);
		dt_device_init(&m->opd_dt_dev, t);
		rc = osp_init0(env, m, t, lcfg);
		if (rc != 0) {
			osp_device_free(env, l);
			l = ERR_PTR(rc);
		}
	}
	return l;
}

static struct lu_device *osp_device_fini(const struct lu_env *env,
					 struct lu_device *d)
{
	struct osp_device *m = lu2osp_dev(d);
	struct obd_import *imp;
	int                rc;

	ENTRY;

	LASSERT(m->opd_storage_exp);
	obd_disconnect(m->opd_storage_exp);

	imp = m->opd_obd->u.cli.cl_import;

	if (imp->imp_rq_pool) {
		ptlrpc_free_rq_pool(imp->imp_rq_pool);
		imp->imp_rq_pool = NULL;
	}

	obd_cleanup_client_import(m->opd_obd);

	if (m->opd_symlink)
		lprocfs_remove(&m->opd_symlink);

	LASSERT(m->opd_obd);
	ptlrpc_lprocfs_unregister_obd(m->opd_obd);
	lprocfs_obd_cleanup(m->opd_obd);

	rc = client_obd_cleanup(m->opd_obd);
	LASSERTF(rc == 0, "error %d\n", rc);

	ptlrpcd_decref();

	RETURN(NULL);
}

static int osp_reconnect(const struct lu_env *env,
			 struct obd_export *exp, struct obd_device *obd,
			 struct obd_uuid *cluuid,
			 struct obd_connect_data *data,
			 void *localdata)
{
	return 0;
}

/*
 * we use exports to track all LOD users
 */
static int osp_obd_connect(const struct lu_env *env, struct obd_export **exp,
			   struct obd_device *obd, struct obd_uuid *cluuid,
			   struct obd_connect_data *data, void *localdata)
{
	struct osp_device       *osp = lu2osp_dev(obd->obd_lu_dev);
	struct obd_connect_data *ocd;
	struct obd_import       *imp;
	struct lustre_handle     conn;
	int                      rc;

	ENTRY;

	CDEBUG(D_CONFIG, "connect #%d\n", osp->opd_connects);

	rc = class_connect(&conn, obd, cluuid);
	if (rc)
		RETURN(rc);

	*exp = class_conn2export(&conn);

	/* Why should there ever be more than 1 connect? */
	osp->opd_connects++;
	LASSERT(osp->opd_connects == 1);

	imp = osp->opd_obd->u.cli.cl_import;
	imp->imp_dlm_handle = conn;

	ocd = &imp->imp_connect_data;
	ocd->ocd_connect_flags = OBD_CONNECT_AT |
				 OBD_CONNECT_FULL20 |
				 OBD_CONNECT_INDEX |
#ifdef HAVE_LRU_RESIZE_SUPPORT
				 OBD_CONNECT_LRU_RESIZE |
#endif
				 OBD_CONNECT_MDS |
				 OBD_CONNECT_OSS_CAPA |
				 OBD_CONNECT_REQPORTAL |
				 OBD_CONNECT_SKIP_ORPHAN |
				 OBD_CONNECT_VERSION;
	ocd->ocd_version = LUSTRE_VERSION_CODE;
	LASSERT(data->ocd_connect_flags & OBD_CONNECT_INDEX);
	ocd->ocd_index = data->ocd_index;
	imp->imp_connect_flags_orig = ocd->ocd_connect_flags;

	rc = ptlrpc_connect_import(imp);
	if (rc) {
		CERROR("can't connect obd: %d\n", rc);
		GOTO(out, rc);
	}

	ptlrpc_pinger_add_import(imp);

out:
	RETURN(rc);
}

/*
 * once last export (we don't count self-export) disappeared
 * osp can be released
 */
static int osp_obd_disconnect(struct obd_export *exp)
{
	struct obd_device *obd = exp->exp_obd;
	struct osp_device *osp = lu2osp_dev(obd->obd_lu_dev);
	int                rc;

	ENTRY;

	/* Only disconnect the underlying layers on the final disconnect. */
	LASSERT(osp->opd_connects == 1);
	osp->opd_connects--;

	rc = class_disconnect(exp);

	/* destroy the device */
	if (rc == 0)
		class_manual_cleanup(obd);

	RETURN(rc);
}

/* context key constructor/destructor: mdt_key_init, mdt_key_fini */
LU_KEY_INIT_FINI(osp, struct osp_thread_info);
static void osp_key_exit(const struct lu_context *ctx,
			 struct lu_context_key *key, void *data)
{
	struct osp_thread_info *info = data;

	info->osi_attr.la_valid = 0;
}

struct lu_context_key osp_thread_key = {
	.lct_tags = LCT_MD_THREAD,
	.lct_init = osp_key_init,
	.lct_fini = osp_key_fini,
	.lct_exit = osp_key_exit
};

/* context key constructor/destructor: mdt_txn_key_init, mdt_txn_key_fini */
LU_KEY_INIT_FINI(osp_txn, struct osp_txn_info);

struct lu_context_key osp_txn_key = {
	.lct_tags = LCT_OSP_THREAD,
	.lct_init = osp_txn_key_init,
	.lct_fini = osp_txn_key_fini
};
LU_TYPE_INIT_FINI(osp, &osp_thread_key, &osp_txn_key);

static struct lu_device_type_operations osp_device_type_ops = {
	.ldto_init           = osp_type_init,
	.ldto_fini           = osp_type_fini,

	.ldto_start          = osp_type_start,
	.ldto_stop           = osp_type_stop,

	.ldto_device_alloc   = osp_device_alloc,
	.ldto_device_free    = osp_device_free,

	.ldto_device_fini    = osp_device_fini
};

static struct lu_device_type osp_device_type = {
	.ldt_tags     = LU_DEVICE_DT,
	.ldt_name     = LUSTRE_OSP_NAME,
	.ldt_ops      = &osp_device_type_ops,
	.ldt_ctx_tags = LCT_MD_THREAD
};

static struct obd_ops osp_obd_device_ops = {
	.o_owner	= THIS_MODULE,
	.o_add_conn	= client_import_add_conn,
	.o_del_conn	= client_import_del_conn,
	.o_reconnect	= osp_reconnect,
	.o_connect	= osp_obd_connect,
	.o_disconnect	= osp_obd_disconnect,
};

static int __init osp_mod_init(void)
{
	struct lprocfs_static_vars	 lvars;
	cfs_proc_dir_entry_t		*osc_proc_dir;
	int				 rc;

	rc = lu_kmem_init(osp_caches);
	if (rc)
		return rc;

	lprocfs_osp_init_vars(&lvars);

	rc = class_register_type(&osp_obd_device_ops, NULL, lvars.module_vars,
				 LUSTRE_OSP_NAME, &osp_device_type);

	/* create "osc" entry in procfs for compatibility purposes */
	if (rc != 0) {
		lu_kmem_fini(osp_caches);
		return rc;
	}

	osc_proc_dir = lprocfs_srch(proc_lustre_root, "osc");
	if (osc_proc_dir == NULL) {
		osc_proc_dir = lprocfs_register("osc", proc_lustre_root, NULL,
						NULL);
		if (IS_ERR(osc_proc_dir))
			CERROR("osp: can't create compat entry \"osc\": %d\n",
			       (int) PTR_ERR(osc_proc_dir));
	}
	return rc;
}

static void __exit osp_mod_exit(void)
{
	lprocfs_try_remove_proc_entry("osc", proc_lustre_root);

	class_unregister_type(LUSTRE_OSP_NAME);
	lu_kmem_fini(osp_caches);
}

MODULE_AUTHOR("Intel, Inc. <http://www.intel.com/>");
MODULE_DESCRIPTION("Lustre OST Proxy Device ("LUSTRE_OSP_NAME")");
MODULE_LICENSE("GPL");

cfs_module(osp, "2.4.0", osp_mod_init, osp_mod_exit);

