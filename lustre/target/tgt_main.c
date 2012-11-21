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
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * lustre/target/tgt_main.c
 *
 * Lustre Unified Target main initialization code
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>

#include "tgt_internal.h"

/* ptlrpc services on server */
static struct ptlrpc_service *mdt_service;
static struct ptlrpc_service *mdt_readpage_service;
static struct ptlrpc_service *seq_controller_service;
static struct ptlrpc_service *seq_service;
static struct ptlrpc_service *fld_service;

/* ptlrpc handle for MDS->client connections (for lock ASTs). */
struct ptlrpc_client srv_ldlm_client;
EXPORT_SYMBOL(srv_ldlm_client);

/* mutex to protect services setup/cleanup against health check */
cfs_mutex_t srv_health_mutex;

/* mutex to protect slices add/remove */
CFS_DEFINE_MUTEX(srv_slices_mutex);

static cfs_proc_dir_entry_t *srv_proc_entry;

#define SRV_SLICES_LIMIT	8
static struct srv_opcodes_slice srv_slices[SRV_SLICES_LIMIT];

#define MDT_SERVICE_WATCHDOG_FACTOR	(2)

static unsigned long mds_num_threads;
CFS_MODULE_PARM(mds_num_threads, "ul", ulong, 0444,
		"number of MDS service threads to start");
static char *mds_num_cpts;
CFS_MODULE_PARM(mds_num_cpts, "c", charp, 0444,
		"CPU partitions MDS threads should run on");
static unsigned long mds_rdpg_num_threads;
CFS_MODULE_PARM(mds_rdpg_num_threads, "ul", ulong, 0444,
		"number of MDS readpage service threads to start");
static char *mds_rdpg_num_cpts;
CFS_MODULE_PARM(mds_rdpg_num_cpts, "c", charp, 0444,
		"CPU partitions MDS readpage threads should run on");

int tgt_register_slice(struct tgt_opc_slice *slice, tgt_handler_t handler)
{
	int i, rc;

	ENTRY;
	cfs_mutex_lock(&srv_slices_mutex);
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == slice) {
			cfs_atomic_inc(&srv_slices[i].ts_ref);
			LASSERT(srv_slices[i].ts_handler == handler);
			GOTO(unlock, rc = 0);
		}
	}

	/* new slice, get first empty slot */
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == NULL) {
			srv_slices[i].ts_slice = slice;
			cfs_atomic_set(&srv_slices[i].ts_ref, 1);
			srv_slices[i].ts_handler = handler;
			GOTO(unlock, rc = 0);
		}
	}
	rc = -ENOMEM;
unlock:
	cfs_mutex_unlock(&srv_slices_mutex);
	return rc;
}
EXPORT_SYMBOL(tgt_register_slice);

void tgt_degister_slice(struct tgt_opc_slice *slice)
{
	int i;

	cfs_mutex_lock(&srv_slices_mutex);
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == slice) {
			if (cfs_atomic_dec_and_test(&srv_slices[i].ts_ref))
				srv_slices[i].ts_slice = NULL;
			break;
		}
	}
	cfs_mutex_unlock(&srv_slices_mutex);
}
EXPORT_SYMBOL(tgt_degister_slice);

static tgt_handler_t tgt_handler_find(__u32 opc)
{
	struct tgt_opc_slice	*s;
	tgt_handler_t		 h;
	int			 i;

	h = NULL;
	for (i = 0; i < SRV_SLICES_LIMIT; i++) {
		if (srv_slices[i].ts_slice == NULL)
			continue;
		for (s = srv_slices[i].ts_slice; s->tos_hs != NULL; s++) {
			if (s->tos_opc_start <= opc && opc < s->tos_opc_end) {
				h = srv_slices[i].ts_handler;
				break;
			}
		}
	}
	return h;
}

static int tgt_request_handle(struct ptlrpc_request *req)
{
	tgt_handler_t	 h;
	int		 rc;

	h = tgt_handler_find(lustre_msg_get_opc(req->rq_reqmsg));
	if (likely(h != NULL)) {
		rc = h(req);
	} else {
		CERROR("The unsupported opc: 0x%x\n",
		       lustre_msg_get_opc(req->rq_reqmsg));
		       req->rq_status = -ENOTSUPP;
		       rc = ptlrpc_error(req);
	}
	return rc;
}

static void srv_stop_ptlrpc_services(void)
{
	ENTRY;

	ping_evictor_stop();

	cfs_mutex_lock(&srv_health_mutex);
	if (mdt_service != NULL) {
		ptlrpc_unregister_service(mdt_service);
		mdt_service = NULL;
	}
	if (mdt_readpage_service != NULL) {
		ptlrpc_unregister_service(mdt_readpage_service);
		mdt_readpage_service = NULL;
	}
	if (seq_controller_service != NULL) {
		ptlrpc_unregister_service(seq_controller_service);
		seq_controller_service = NULL;
	}
	if (seq_service != NULL) {
		ptlrpc_unregister_service(seq_service);
		seq_service = NULL;
	}
	if (fld_service != NULL) {
		ptlrpc_unregister_service(fld_service);
		fld_service = NULL;
	}
	cfs_mutex_unlock(&srv_health_mutex);

	EXIT;
}

static int srv_start_ptlrpc_services(void)
{
	struct ptlrpc_service_conf	 conf;
	int				 rc = 0;

	ENTRY;

	cfs_mutex_init(&srv_health_mutex);

	ptlrpc_init_client(LDLM_CB_REQUEST_PORTAL, LDLM_CB_REPLY_PORTAL,
			   "srv_ldlm_client", &srv_ldlm_client);

	conf = (typeof(conf)) {
		.psc_name		= LUSTRE_MDS_NAME,
		.psc_watchdog_factor	= MDT_SERVICE_WATCHDOG_FACTOR,
		.psc_buf		= {
			.bc_nbufs		= MDS_NBUFS,
			.bc_buf_size		= MDS_BUFSIZE,
			.bc_req_max_size	= MDS_MAXREQSIZE,
			.bc_rep_max_size	= MDS_MAXREPSIZE,
			.bc_req_portal		= MDS_REQUEST_PORTAL,
			.bc_rep_portal		= MDC_REPLY_PORTAL,
		},
		/*
		 * We'd like to have a mechanism to set this on a per-device
		 * basis, but alas...
		 */
		.psc_thr		= {
			.tc_thr_name		= LUSTRE_MDT_NAME,
			.tc_thr_factor		= MDT_THR_FACTOR,
			.tc_nthrs_init		= MDT_NTHRS_INIT,
			.tc_nthrs_base		= MDT_NTHRS_BASE,
			.tc_nthrs_max		= MDT_NTHRS_MAX,
			.tc_nthrs_user		= mds_num_threads,
			.tc_cpu_affinity	= 1,
			.tc_ctx_tags		= LCT_MD_THREAD,
		},
		.psc_cpt		= {
			.cc_pattern		= mds_num_cpts,
		},
		.psc_ops		= {
			.so_req_handler		= tgt_request_handle,
			.so_req_printer		= target_print_req,
			.so_hpreq_handler	= ptlrpc_hpreq_handler,
		},
	};

	mdt_service = ptlrpc_register_service(&conf, srv_proc_entry);
	if (IS_ERR(mdt_service)) {
		rc = PTR_ERR(mdt_service);
		CERROR("failed to start regular mdt service: %d\n", rc);
		mdt_service = NULL;
		RETURN(rc);
	}

	/*
	 * readpage service configuration. Parameters have to be adjusted,
	 * ideally.
	 */
	memset(&conf, 0, sizeof(conf));
	conf = (typeof(conf)) {
		.psc_name		= LUSTRE_MDS_NAME "_readpage",
		.psc_watchdog_factor	= MDT_SERVICE_WATCHDOG_FACTOR,
		.psc_buf		= {
			.bc_nbufs		= MDS_NBUFS,
			.bc_buf_size		= MDS_BUFSIZE,
			.bc_req_max_size	= MDS_MAXREQSIZE,
			.bc_rep_max_size	= MDS_MAXREPSIZE,
			.bc_req_portal		= MDS_READPAGE_PORTAL,
			.bc_rep_portal		= MDC_REPLY_PORTAL,
		},
		.psc_thr		= {
			.tc_thr_name		= "mdt_rdpg",
			.tc_thr_factor		= MDT_RDPG_THR_FACTOR,
			.tc_nthrs_init		= MDT_RDPG_NTHRS_INIT,
			.tc_nthrs_base		= MDT_RDPG_NTHRS_BASE,
			.tc_nthrs_max		= MDT_RDPG_NTHRS_MAX,
			.tc_nthrs_user		= mds_rdpg_num_threads,
			.tc_cpu_affinity	= 1,
			.tc_ctx_tags		= LCT_MD_THREAD,
		},
		.psc_cpt		= {
			.cc_pattern		= mds_rdpg_num_cpts,
		},
		.psc_ops		= {
			.so_req_handler		= tgt_request_handle,
			.so_req_printer		= target_print_req,
		},
	};

	mdt_readpage_service = ptlrpc_register_service(&conf, srv_proc_entry);
	if (IS_ERR(mdt_readpage_service)) {
		rc = PTR_ERR(mdt_readpage_service);
		CERROR("failed to start readpage service: %d\n", rc);
		mdt_readpage_service = NULL;
		GOTO(err_svc, rc);
	}

	/*
	 * sequence controller service configuration
	 */
	memset(&conf, 0, sizeof(conf));
	conf = (typeof(conf)) {
		.psc_name		= LUSTRE_MDS_NAME "_mdsc",
		.psc_watchdog_factor	= MDT_SERVICE_WATCHDOG_FACTOR,
		.psc_buf		= {
			.bc_nbufs		= MDS_NBUFS,
			.bc_buf_size		= MDS_BUFSIZE,
			.bc_req_max_size	= SEQ_MAXREQSIZE,
			.bc_rep_max_size	= SEQ_MAXREPSIZE,
			.bc_req_portal		= SEQ_CONTROLLER_PORTAL,
			.bc_rep_portal		= MDC_REPLY_PORTAL,
		},
		.psc_thr		= {
			.tc_thr_name		= "mdt_mdsc",
			.tc_nthrs_init		= MDT_OTHR_NTHRS_INIT,
			.tc_nthrs_max		= MDT_OTHR_NTHRS_MAX,
			.tc_ctx_tags		= LCT_MD_THREAD,
		},
		.psc_ops		= {
			.so_req_handler		= tgt_request_handle,
			.so_req_printer		= target_print_req,
		},
	};

	seq_controller_service = ptlrpc_register_service(&conf, srv_proc_entry);
	if (IS_ERR(seq_controller_service)) {
		rc = PTR_ERR(seq_controller_service);
		CERROR("failed to start seq controller service: %d\n", rc);
		seq_controller_service = NULL;
		GOTO(err_svc, rc);
	}

	/*
	 * metadata sequence server service configuration
	 */
	memset(&conf, 0, sizeof(conf));
	conf = (typeof(conf)) {
		.psc_name		= LUSTRE_MDS_NAME "_mdss",
		.psc_watchdog_factor	= MDT_SERVICE_WATCHDOG_FACTOR,
		.psc_buf		= {
			.bc_nbufs		= MDS_NBUFS,
			.bc_buf_size		= MDS_BUFSIZE,
			.bc_req_max_size	= SEQ_MAXREQSIZE,
			.bc_rep_max_size	= SEQ_MAXREPSIZE,
			.bc_req_portal		= SEQ_METADATA_PORTAL,
			.bc_rep_portal		= MDC_REPLY_PORTAL,
		},
		.psc_thr		= {
			.tc_thr_name		= "mdt_mdss",
			.tc_nthrs_init		= MDT_OTHR_NTHRS_INIT,
			.tc_nthrs_max		= MDT_OTHR_NTHRS_MAX,
			.tc_ctx_tags		= LCT_MD_THREAD | LCT_DT_THREAD
		},
		.psc_ops		= {
			.so_req_handler		= tgt_request_handle,
			.so_req_printer		= target_print_req,
		},
	};

	seq_service = ptlrpc_register_service(&conf, srv_proc_entry);
	if (IS_ERR(seq_service)) {
		rc = PTR_ERR(seq_service);
		CERROR("failed to start metadata seq server service: %d\n", rc);
		seq_service = NULL;
		GOTO(err_svc, rc);
	}

	/* FLD service start */
	memset(&conf, 0, sizeof(conf));
	conf = (typeof(conf)) {
		.psc_name	     = LUSTRE_MDS_NAME "_fld",
		.psc_watchdog_factor = MDT_SERVICE_WATCHDOG_FACTOR,
		.psc_buf		= {
			.bc_nbufs		= MDS_NBUFS,
			.bc_buf_size		= MDS_BUFSIZE,
			.bc_req_max_size	= FLD_MAXREQSIZE,
			.bc_rep_max_size	= FLD_MAXREPSIZE,
			.bc_req_portal		= FLD_REQUEST_PORTAL,
			.bc_rep_portal		= MDC_REPLY_PORTAL,
		},
		.psc_thr		= {
			.tc_thr_name		= "mdt_fld",
			.tc_nthrs_init		= MDT_OTHR_NTHRS_INIT,
			.tc_nthrs_max		= MDT_OTHR_NTHRS_MAX,
			.tc_ctx_tags		= LCT_DT_THREAD | LCT_MD_THREAD
		},
		.psc_ops		= {
			.so_req_handler		= tgt_request_handle,
			.so_req_printer		= target_print_req,
		},
	};

	fld_service = ptlrpc_register_service(&conf, srv_proc_entry);
	if (IS_ERR(fld_service)) {
		rc = PTR_ERR(fld_service);
		CERROR("failed to start fld service: %d\n", rc);
		fld_service = NULL;
		GOTO(err_svc, rc);
	}

	ping_evictor_start();

	EXIT;
err_svc:
	if (rc)
		srv_stop_ptlrpc_services();
	return rc;
}

int tgt_init(const struct lu_env *env, struct lu_target *lut,
	     struct obd_device *obd, struct dt_device *dt)
{
	struct dt_object_format	dof;
	struct lu_attr		attr;
	struct lu_fid		fid;
	struct dt_object       *o;
	int			rc = 0;

	ENTRY;

	LASSERT(lut);
	LASSERT(obd);
	lut->lut_obd = obd;
	lut->lut_bottom = dt;
	lut->lut_last_rcvd = NULL;
	obd->u.obt.obt_lut = lut;
	obd->u.obt.obt_magic = OBT_MAGIC;

	cfs_spin_lock_init(&lut->lut_translock);

	OBD_ALLOC(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
	if (lut->lut_client_bitmap == NULL)
		RETURN(-ENOMEM);

	memset(&attr, 0, sizeof(attr));
	attr.la_valid = LA_MODE;
	attr.la_mode = S_IFREG | S_IRUGO | S_IWUSR;
	dof.dof_type = dt_mode_to_dft(S_IFREG);

	lu_local_obj_fid(&fid, MDT_LAST_RECV_OID);

	o = dt_find_or_create(env, lut->lut_bottom, &fid, &dof, &attr);
	if (!IS_ERR(o)) {
		lut->lut_last_rcvd = o;
	} else {
		OBD_FREE(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
		lut->lut_client_bitmap = NULL;
		rc = PTR_ERR(o);
		CERROR("cannot open %s: rc = %d\n", LAST_RCVD, rc);
	}

	RETURN(rc);
}
EXPORT_SYMBOL(tgt_init);

void tgt_fini(const struct lu_env *env, struct lu_target *lut)
{
	ENTRY;

	if (lut->lut_client_bitmap) {
		OBD_FREE(lut->lut_client_bitmap, LR_MAX_CLIENTS >> 3);
		lut->lut_client_bitmap = NULL;
	}
	if (lut->lut_last_rcvd) {
		lu_object_put(env, &lut->lut_last_rcvd->do_lu);
		lut->lut_last_rcvd = NULL;
	}
	EXIT;
}
EXPORT_SYMBOL(tgt_fini);

/* context key constructor/destructor: tg_key_init, tg_key_fini */
LU_KEY_INIT_FINI(tgt, struct tgt_thread_info);

/* context key: tg_thread_key */
LU_CONTEXT_KEY_DEFINE(tgt, LCT_MD_THREAD | LCT_DT_THREAD);
EXPORT_SYMBOL(tgt_thread_key);

LU_KEY_INIT_GENERIC(tg);

struct lprocfs_vars lprocfs_srv_module_vars[] = {
	{ 0 },
};

int tgt_mod_init(void)
{
	int rc;

	ENTRY;

	tg_key_init_generic(&tgt_thread_key, NULL);
	lu_context_key_register_many(&tgt_thread_key, NULL);

	srv_proc_entry = lprocfs_register("server", proc_lustre_root,
					  lprocfs_srv_module_vars, NULL);
	if (IS_ERR(srv_proc_entry)) {
		rc = PTR_ERR(srv_proc_entry);
		srv_proc_entry = NULL;
		GOTO(out_key, rc);
	}

	rc = srv_start_ptlrpc_services();
	if (rc < 0)
		GOTO(out_proc, rc);
	RETURN(0);
out_proc:
	lprocfs_remove(&srv_proc_entry);
out_key:
	lu_context_key_degister(&tgt_thread_key);
	return rc;
}

void tgt_mod_exit(void)
{
	srv_stop_ptlrpc_services();
	lprocfs_remove(&srv_proc_entry);
	lu_context_key_degister(&tgt_thread_key);
}

