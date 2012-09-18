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
 * lustre/lod/lod_internal.h
 *
 * Author: Alexey Zhuravlev <alexey.zhuravlev@intel.com>
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#ifndef _LOD_INTERNAL_H
#define _LOD_INTERNAL_H

#include <libcfs/libcfs.h>
#include <obd.h>
#include <dt_object.h>

#define LOV_USES_ASSIGNED_STRIPE        0
#define LOV_USES_DEFAULT_STRIPE         1

struct lod_ost_desc {
	struct dt_device	*ltd_ost;
	struct list_head	 ltd_kill;
	struct obd_export	*ltd_exp;
	struct obd_uuid		 ltd_uuid;
	__u32			 ltd_gen;
	__u32			 ltd_index;
	struct ltd_qos		 ltd_qos; /* qos info per target */
	struct obd_statfs	 ltd_statfs;
				 /* is this target up for requests */
	unsigned long		 ltd_active:1,
				 /* should  target be activated */
				 ltd_activate:1,
				 /* should this target be deleted */
				 ltd_reap:1;
};

#define OST_PTRS		256     /* number of pointers at 1st level */
#define OST_PTRS_PER_BLOCK	256     /* number of pointers at 2nd level */

struct lod_ost_desc_idx {
	struct lod_ost_desc *ldi_ost[OST_PTRS_PER_BLOCK];
};

#define OST_TGT(dev, index) \
((dev)->lod_ost_idx[(index) / OST_PTRS_PER_BLOCK]->ldi_ost[(index) % \
							   OST_PTRS_PER_BLOCK])

struct lod_device {
	struct dt_device	 lod_dt_dev;
	struct obd_export	*lod_child_exp;
	struct dt_device	*lod_child;
	cfs_proc_dir_entry_t	*lod_proc_entry;
	struct lprocfs_stats	*lod_stats;
	int			 lod_connects;
	int			 lod_recovery_completed;

	/* lov settings descriptor storing static information */
	struct lov_desc		 lod_desc;

	/* use to protect ld_active_tgt_count and all ltd_active */
	cfs_spinlock_t		 lod_desc_lock;

	/* list of known OSTs */
	struct lod_ost_desc_idx	*lod_ost_idx[OST_PTRS];

	/* Size of the lod_osts array, granted to be a power of 2 */
	__u32			 lod_osts_size;
	/* number of registered OSTs */
	int			 lod_ostnr;
	/* OSTs scheduled to be deleted */
	__u32			 lod_death_row;
	/* bitmap of OSTs available */
	cfs_bitmap_t		*lod_ost_bitmap;

	/* maximum EA size underlied OSD may have */
	unsigned int		 lod_osd_max_easize;

	/* Table refcount used for delayed deletion */
	int			 lod_refcount;
	/* mutex to serialize concurrent updates to the ost table */
	cfs_mutex_t		 lod_mutex;
	/* read/write semaphore used for array relocation */
	cfs_rw_semaphore_t	 lod_rw_sem;

	enum lustre_sec_part	 lod_sp_me;

	cfs_proc_dir_entry_t	*lod_symlink;
};

extern const struct lu_device_operations lod_lu_ops;
extern struct dt_object_operations lod_obj_ops;

struct lod_object {
	struct dt_object	 mbo_obj;

	/* if object is striped, then the next fields describe stripes */
	__u16			 mbo_stripenr;
	__u16			 mbo_layout_gen;
	__u32			 mbo_stripe_size;
	char			*mbo_pool;
	struct dt_object	**mbo_stripe;
	/* to know how much memory to free, mbo_stripenr can be less */
	int			 mbo_stripes_allocated;
	/* default striping for directory represented by this object
	 * is cached in stripenr/stripe_size */
	int			 mbo_striping_cached:1;
	int			 mbo_def_striping_set:1;
	__u32			 mbo_def_stripe_size;
	__u16			 mbo_def_stripenr;
	__u16			 mbo_def_stripe_offset;
};

extern cfs_mem_cache_t *lod_object_kmem;
extern struct lu_object_operations lod_lu_obj_ops;

struct lod_thread_info {
	/* per-thread buffer for LOV EA */
	void			*lti_ea_store;
	int			 lti_ea_store_size;
	struct lu_buf		 lti_buf;
	struct ost_id		 lti_ostid;
	struct lu_fid		 lti_fid;
	struct lu_attr		 lti_attr;
};

static inline int lu_device_is_lod(struct lu_device *d)
{
	return ergo(d != NULL && d->ld_ops != NULL, d->ld_ops == &lod_lu_ops);
}

static inline struct lod_device *lu2lod_dev(struct lu_device *d)
{
	LASSERT(lu_device_is_lod(d));
	return container_of0(d, struct lod_device, lod_dt_dev.dd_lu_dev);
}

static inline struct lu_device *lod2lu_dev(struct lod_device *d)
{
	return &d->lod_dt_dev.dd_lu_dev;
}

static inline struct obd_device *lod2obd(struct lod_device *d)
{
	return d->lod_dt_dev.dd_lu_dev.ld_obd;
}

static inline struct lod_device *dt2lod_dev(struct dt_device *d)
{
	LASSERT(lu_device_is_lod(&d->dd_lu_dev));
	return container_of0(d, struct lod_device, lod_dt_dev);
}

static inline struct lod_object *lu2lod_obj(struct lu_object *o)
{
	LASSERT(ergo(o != NULL, lu_device_is_lod(o->lo_dev)));
	return container_of0(o, struct lod_object, mbo_obj.do_lu);
}

static inline struct lu_object *lod2lu_obj(struct lod_object *obj)
{
	return &obj->mbo_obj.do_lu;
}

static inline struct lod_object *lod_obj(const struct lu_object *o)
{
	LASSERT(lu_device_is_lod(o->lo_dev));
	return container_of0(o, struct lod_object, mbo_obj.do_lu);
}

static inline struct lod_object *lod_dt_obj(const struct dt_object *d)
{
	return lod_obj(&d->do_lu);
}

static inline struct dt_object *lod_object_child(struct lod_object *o)
{
	return container_of0(lu_object_next(lod2lu_obj(o)),
			     struct dt_object, do_lu);
}

static inline struct dt_object *lu2dt_obj(struct lu_object *o)
{
	LASSERT(ergo(o != NULL, lu_device_is_dt(o->lo_dev)));
	return container_of0(o, struct dt_object, do_lu);
}

static inline struct dt_object *dt_object_child(struct dt_object *o)
{
	return container_of0(lu_object_next(&(o)->do_lu),
			     struct dt_object, do_lu);
}

extern struct lu_context_key lod_thread_key;

static inline struct lod_thread_info *lod_env_info(const struct lu_env *env)
{
	struct lod_thread_info *info;

	info = lu_context_key_get(&env->le_ctx, &lod_thread_key);
	LASSERT(info);
	return info;
}

/* lod_lov.c */
void lod_getref(struct lod_device *lod);
void lod_putref(struct lod_device *lod);
int lod_add_device(const struct lu_env *env, struct lod_device *m,
		   char *osp, unsigned index, unsigned gen, int active);
int lod_del_device(const struct lu_env *env, struct lod_device *m,
		   char *osp, unsigned index, unsigned gen);
int lod_generate_and_set_lovea(const struct lu_env *env,
			       struct lod_object *mo, struct thandle *th);
int lod_load_striping(const struct lu_env *env, struct lod_object *mo);
int lod_get_lov_ea(const struct lu_env *env, struct lod_object *mo);
void lod_fix_desc(struct lov_desc *desc);
void lod_fix_desc_qos_maxage(__u32 *val);
void lod_fix_desc_pattern(__u32 *val);
void lod_fix_desc_stripe_count(__u32 *val);
void lod_fix_desc_stripe_size(__u64 *val);
int lod_lov_init(struct lod_device *m, struct lustre_cfg *cfg);
int lod_lov_fini(struct lod_device *m);
int lod_parse_striping(const struct lu_env *env, struct lod_object *mo,
		       const struct lu_buf *buf);
int lod_initialize_objects(const struct lu_env *env, struct lod_object *mo,
			   struct lov_ost_data_v1 *objs);
int lod_store_def_striping(const struct lu_env *env, struct dt_object *dt,
			   struct thandle *th);

/* lod_object.c */
int lod_object_set_pool(struct lod_object *o, char *pool);
int lod_declare_striped_object(const struct lu_env *env, struct dt_object *dt,
			       struct lu_attr *attr, const struct lu_buf *buf,
			       struct thandle *th);
int lod_striping_create(const struct lu_env *env, struct dt_object *dt,
			struct lu_attr *attr, struct dt_object_format *dof,
			struct thandle *th);
void lod_object_free_striping(const struct lu_env *env, struct lod_object *o);

/* lproc_lod.c */
void lprocfs_lod_init_vars(struct lprocfs_static_vars *lvars);

#endif

