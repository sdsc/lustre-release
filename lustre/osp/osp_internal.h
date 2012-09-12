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
 * lustre/osp/osp_internal.h
 *
 * Author: Alex Zhuravlev <alexey.zhuravlev@intel.com>
 */

#ifndef _OSP_INTERNAL_H
#define _OSP_INTERNAL_H

#include <obd.h>
#include <dt_object.h>
#include <lustre_fid.h>

struct osp_device {
	struct dt_device		 opd_dt_dev;
	/* corresponded OST index */
	int				 opd_index;
	/* device used to store persistent state (llogs, last ids) */
	struct obd_export		*opd_storage_exp;
	struct dt_device		*opd_storage;
	struct dt_object		*opd_last_used_file;
	/* protected by opd_pre_lock */
	volatile obd_id			 opd_last_used_id;
	obd_id				 opd_gap_start;
	int				 opd_gap_count;
	/* connection to OST */
	struct obd_device		*opd_obd;
	struct obd_export		*opd_exp;
	struct obd_uuid			 opd_cluuid;
	struct obd_connect_data		*opd_connect_data;
	int				 opd_connects;
	cfs_proc_dir_entry_t		*opd_proc_entry;
	struct lprocfs_stats		*opd_stats;
	/* connection status. */
	int				 opd_new_connection;
	int				 opd_got_disconnected;
	int				 opd_imp_connected;
	int				 opd_imp_active;
	int				 opd_imp_seen_connected:1;

	/* whether local recovery is completed:
	 * reported via ->ldo_recovery_complete() */
	int				 opd_recovery_completed;

	cfs_proc_dir_entry_t		*opd_symlink;
};

extern cfs_mem_cache_t *osp_object_kmem;

/* this is a top object */
struct osp_object {
	struct lu_object_header	 opo_header;
	struct dt_object	 opo_obj;
	int			 opo_reserved;
};

extern struct lu_object_operations osp_lu_obj_ops;

struct osp_thread_info {
	struct lu_buf		 osi_lb;
	struct lu_fid		 osi_fid;
	struct lu_attr		 osi_attr;
	obd_id			 osi_id;
	loff_t			 osi_off;
};

static inline void osp_objid_buf_prep(struct osp_thread_info *osi,
				      struct osp_device *d, int index)
{
	osi->osi_lb.lb_buf = (void *)&d->opd_last_used_id;
	osi->osi_lb.lb_len = sizeof(d->opd_last_used_id);
	osi->osi_off = sizeof(d->opd_last_used_id) * index;
}

extern struct lu_context_key osp_thread_key;

static inline struct osp_thread_info *osp_env_info(const struct lu_env *env)
{
	struct osp_thread_info *info;

	info = lu_context_key_get(&env->le_ctx, &osp_thread_key);
	if (info == NULL) {
		lu_env_refill((struct lu_env *)env);
		info = lu_context_key_get(&env->le_ctx, &osp_thread_key);
	}
	LASSERT(info);
	return info;
}

struct osp_txn_info {
	__u32   oti_current_id;
};

extern struct lu_context_key osp_txn_key;

static inline struct osp_txn_info *osp_txn_info(struct lu_context *ctx)
{
	struct osp_txn_info *info;

	info = lu_context_key_get(ctx, &osp_txn_key);
	return info;
}

extern const struct lu_device_operations osp_lu_ops;

static inline int lu_device_is_osp(struct lu_device *d)
{
	return ergo(d != NULL && d->ld_ops != NULL, d->ld_ops == &osp_lu_ops);
}

static inline struct osp_device *lu2osp_dev(struct lu_device *d)
{
	LASSERT(lu_device_is_osp(d));
	return container_of0(d, struct osp_device, opd_dt_dev.dd_lu_dev);
}

static inline struct lu_device *osp2lu_dev(struct osp_device *d)
{
	return &d->opd_dt_dev.dd_lu_dev;
}

static inline struct osp_device *dt2osp_dev(struct dt_device *d)
{
	LASSERT(lu_device_is_osp(&d->dd_lu_dev));
	return container_of0(d, struct osp_device, opd_dt_dev);
}

static inline struct osp_object *lu2osp_obj(struct lu_object *o)
{
	LASSERT(ergo(o != NULL, lu_device_is_osp(o->lo_dev)));
	return container_of0(o, struct osp_object, opo_obj.do_lu);
}

static inline struct lu_object *osp2lu_obj(struct osp_object *obj)
{
	return &obj->opo_obj.do_lu;
}

static inline struct osp_object *osp_obj(const struct lu_object *o)
{
	LASSERT(lu_device_is_osp(o->lo_dev));
	return container_of0(o, struct osp_object, opo_obj.do_lu);
}

static inline struct osp_object *dt2osp_obj(const struct dt_object *d)
{
	return osp_obj(&d->do_lu);
}

static inline struct dt_object *osp_object_child(struct osp_object *o)
{
	return container_of0(lu_object_next(osp2lu_obj(o)),
                             struct dt_object, do_lu);
}

/* osp_dev.c */
void osp_update_last_id(struct osp_device *d, obd_id objid);

/* lproc_osp.c */
void lprocfs_osp_init_vars(struct lprocfs_static_vars *lvars);

#endif
