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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_ECHO

#include <linux/user_namespace.h>
#ifdef HAVE_UIDGID_HEADER
# include <linux/uidgid.h>
#endif
#include <libcfs/libcfs.h>

#include <obd.h>
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_debug.h>
#include <lprocfs_status.h>
#include <cl_object.h>
#include <lustre_fid.h>
#include <lustre_acl.h>
#include <lustre_ioctl.h>
#include <lustre_net.h>
#include <lu_object.h>

/* a trivial device we create to use OSD API */
struct obe_device {
	struct dt_device	 obe_top_dev;
	/* how many handle's reference this local storage */
	atomic_t		 obe_refcount;
	/* underlaying OSD device */
	struct dt_device	*obe_osd;
	struct obd_export	*obe_exp;
};

struct obe_object {
	struct lu_object_header	 obo_header;
	struct dt_object	 obo_obj;
};

struct obe_thread_info {
	struct lu_attr		 oti_la;
	struct dt_index_features oti_dif;
};
struct lu_context_key obe_thread_key;

struct obe_update {
	struct list_head	 obu_list;
	struct object_update	*obu_update;
	struct dt_object	*obu_object;
	int			 obu_index; /* index in req/reply */
};

struct obe_tx {
	struct list_head		 obt_updates_list;
	struct thandle			*obt_th;
	struct dt_device		*obt_dt;
	__u64				 obt_batchid;
	struct object_update_reply	*obt_reply;

};


static struct dt_object *obe_locate(const struct lu_env *env,
				    struct obe_device *obe,
				    const struct lu_fid *fid)
{
	struct lu_object  *lo;

	lo = lu_object_find_at(env, &obe->obe_top_dev.dd_lu_dev, fid, NULL);
	if (IS_ERR(lo))
		return ERR_PTR(PTR_ERR(lo));

	return container_of0(lu_object_next(lo), struct dt_object, do_lu);
}

static struct dt_object *obe_locate_new(const struct lu_env *env,
				    struct obe_device *obe,
				    const struct lu_fid *fid)
{
	struct lu_object_conf conf = { LOC_F_NEW };
	struct lu_object  *lo;

	lo = lu_object_find_at(env, &obe->obe_top_dev.dd_lu_dev, fid, &conf);
	if (IS_ERR(lo))
		return ERR_PTR(PTR_ERR(lo));

	return container_of0(lu_object_next(lo), struct dt_object, do_lu);
}

LU_KEY_INIT_FINI(obe, struct obe_thread_info);

LU_TYPE_INIT_FINI(obe, &obe_thread_key);

LU_CONTEXT_KEY_DEFINE(obe, LCT_DT_THREAD);

static struct lu_device_type_operations obe_device_type_ops = {
	.ldto_init		= obe_type_init,
	.ldto_fini		= obe_type_fini,

	.ldto_start		= obe_type_start,
	.ldto_stop		= obe_type_stop,
};

static struct lu_device_type obe_lu_type = {
	.ldt_tags = LU_DEVICE_DT,
	.ldt_name = "obe_device",
	.ldt_ops  = &obe_device_type_ops,
	.ldt_ctx_tags = LCT_LOCAL,
};

static inline struct obe_object *lu2obe_obj(struct lu_object *o)
{
	return container_of0(o, struct obe_object, obo_obj.do_lu);
}

static int obe_object_init(const struct lu_env *env, struct lu_object *o,
			  const struct lu_object_conf *unused)
{
	struct obe_device	*obe;
	struct lu_object	*below;
	struct lu_device	*under;

	ENTRY;

	obe = container_of0(o->lo_dev, struct obe_device,
			    obe_top_dev.dd_lu_dev);
	under = &obe->obe_osd->dd_lu_dev;
	below = under->ld_ops->ldo_object_alloc(env, o->lo_header, under);
	if (below == NULL)
		RETURN(-ENOMEM);

	lu_object_add(o, below);

	RETURN(0);
}

static void obe_object_free(const struct lu_env *env, struct lu_object *o)
{
	struct obe_object	*obj = lu2obe_obj(o);
	struct lu_object_header	*h = o->lo_header;

	dt_object_fini(&obj->obo_obj);
	lu_object_header_fini(h);
	OBD_FREE_PTR(obj);
}

static struct lu_object_operations obe_lu_obj_ops = {
	.loo_object_init  = obe_object_init,
	.loo_object_free  = obe_object_free,
};

static struct lu_object *obe_object_alloc(const struct lu_env *env,
					  const struct lu_object_header *_h,
					  struct lu_device *d)
{
	struct lu_object_header	*h;
	struct obe_object	*o;
	struct lu_object	*l;

	LASSERT(_h == NULL);

	OBD_ALLOC_PTR(o);
	if (o != NULL) {
		l = &o->obo_obj.do_lu;
		h = &o->obo_header;

		lu_object_header_init(h);
		dt_object_init(&o->obo_obj, h, d);
		lu_object_add_top(h, l);

		l->lo_ops = &obe_lu_obj_ops;

		return l;
	} else {
		return NULL;
	}
}

static struct lu_device_operations obe_lu_dev_ops = {
	.ldo_object_alloc = obe_object_alloc
};

static inline struct obe_thread_info *obe_oti_get(const struct lu_env *env)
{
	return lu_context_key_get(&env->le_ctx, &obe_thread_key);
}

typedef int (*osd_update_func)(const struct lu_env *env,
			       struct dt_object *o,
			       struct object_update *u,
			       struct thandle *th, int exec);

typedef int (*osd_access_func)(const struct lu_env *env,
			       struct dt_object *o,
			       struct object_update *u,
			       struct object_update_reply *r, int index);

static int obe_replay_create_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct thandle *th, int exec)
{
	struct dt_object_format	 dof;
	struct lu_attr		*attr;
	size_t			 size = 0;
	__u8			*type = NULL;
	int			 rc;

	attr = object_update_param_get(u, 0, &size);
	if (unlikely(IS_ERR(attr)))
		return PTR_ERR(attr);
	if (unlikely(attr == NULL))
		return -EINVAL;
	if (unlikely(size != sizeof(*attr)))
		return -EINVAL;

	if ((attr->la_mode & S_IFMT) == 0)
		return -EINVAL;

	dof.dof_type = dt_mode_to_dft(attr->la_mode);
	type = object_update_param_get(u, 1, &size);
	if (type != NULL && !IS_ERR(type)) {
		if (size != sizeof(__u8))
			return -EINVAL;
		dof.dof_type = *type;
		if (dof.dof_type == DFT_INDEX) {
			type = object_update_param_get(u, 2, &size);
			if (unlikely(type == NULL))
				return -EINVAL;
			if (unlikely(IS_ERR(type)))
				return PTR_ERR(type);
			if (unlikely(size != sizeof(struct dt_index_features)))
				return -EINVAL;
			dof.u.dof_idx.di_feat = (void *)type;
		}
	}

	/* XXX: hint?* */

	if (exec == 0)
		rc = dt_declare_create(env, o, attr, NULL, &dof, th);
	else {
		dt_write_lock(env, o, 0);
		rc = -EEXIST;
		if (dt_object_exists(o) == 0)
			rc = dt_create(env, o, attr, NULL, &dof, th);
		dt_write_unlock(env, o);
	}

	return rc;
}

static int obe_replay_destroy_update(const struct lu_env *env,
				     struct dt_object *o,
				     struct object_update *u,
				     struct thandle *th, int exec)
{
	int rc;
	if (exec == 0)
		rc = dt_declare_destroy(env, o, th);
	else
		rc = dt_destroy(env, o, th);

	return rc;
}

static int obe_replay_ref_add_update(const struct lu_env *env,
				     struct dt_object *o,
				     struct object_update *u,
				     struct thandle *th, int exec)
{
	int rc;
	if (exec == 0)
		rc = dt_declare_ref_add(env, o, th);
	else
		rc = dt_ref_add(env, o, th);
	return rc;
}

static int obe_replay_ref_del_update(const struct lu_env *env,
				     struct dt_object *o,
				     struct object_update *u,
				     struct thandle *th, int exec)
{
	int rc;
	if (exec == 0)
		rc = dt_declare_ref_del(env, o, th);
	else
		rc = dt_ref_del(env, o, th);
	return rc;
}

static int obe_replay_attr_set_update(const struct lu_env *env,
				      struct dt_object *o,
				      struct object_update *u,
				      struct thandle *th, int exec)
{
	struct lu_attr	*attr;
	int		 rc;
	size_t		 size = 0;

	attr = object_update_param_get(u, 0, &size);
	if (unlikely(IS_ERR(attr)))
		return PTR_ERR(attr);
	if (unlikely(attr == NULL))
		return -EINVAL;
	if (unlikely(size != sizeof(*attr)))
		return -EINVAL;

	if (exec == 0)
		rc = dt_declare_attr_set(env, o, attr, th);
	else
		rc = dt_attr_set(env, o, attr, th);

	return rc;
}

static int obe_replay_attr_get(const struct lu_env *env,
			       struct dt_object *o,
			       struct object_update *u,
			       struct object_update_reply *reply,
			       int index)
{
	struct lu_attr		*attr = &obe_oti_get(env)->oti_la;
	int			 rc;

	rc = dt_attr_get(env, o, attr);

	object_update_result_insert(reply, attr, sizeof(*attr), index, rc);

	return 0;
}

static int obe_replay_xattr_set_update(const struct lu_env *env,
				       struct dt_object *o,
				       struct object_update *u,
				       struct thandle *th, int exec)
{
	struct lu_buf	 lb;
	char		*name;
	size_t		 size = 0;
	int		 rc, *fp, fl = 0;

	name = object_update_param_get(u, 0, &size);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	lb.lb_buf = object_update_param_get(u, 1, &size);
	if (unlikely(IS_ERR(lb.lb_buf)))
		return PTR_ERR(lb.lb_buf);
	if (unlikely(lb.lb_buf == NULL))
		return -EINVAL;
	if (unlikely(size == 0 || size > 32*1024))
		return -EINVAL;
	lb.lb_len = size;

	fp = object_update_param_get(u, 2, &size);
	if (!IS_ERR(fp) && fp != NULL)
		fl = *fp;

	if (exec == 0)
		rc = dt_declare_xattr_set(env, o, &lb, name, fl, th);
	else
		rc = dt_xattr_set(env, o, &lb, name, fl, th);
	return rc;
}

static int obe_replay_xattr_get(const struct lu_env *env,
				struct dt_object *o,
				struct object_update *u,
				struct object_update_reply *reply,
				int index)
{
	struct object_update_result *res;
	char			*name;
	struct lu_buf		 lb;
	int			 rc;

	name = object_update_param_get(u, 0, NULL);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	res = object_update_result_get(reply, index, NULL);
	if (res == NULL)
		return -EPROTO;

	lb.lb_len = u->ou_result_size;
	lb.lb_buf = res->our_data;
	rc = dt_xattr_get(env, o, &lb, name);
	if (rc < 0)
		lb.lb_len = 0;

	object_update_result_insert(reply, lb.lb_buf, lb.lb_len, index, rc);

	return 0;
}


static int obe_replay_xattr_del_update(const struct lu_env *env,
				       struct dt_object *o,
				       struct object_update *u,
				       struct thandle *th, int exec)
{
	char		*name;
	int		 rc;

	name = object_update_param_get(u, 0, NULL);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	if (exec == 0)
		rc = dt_declare_xattr_del(env, o, name, th);
	else
		rc = dt_xattr_del(env, o, name, th);

	return rc;
}

static int obe_replay_lookup(const struct lu_env *env,
			     struct dt_object *o,
			     struct object_update *u,
			     struct object_update_reply *reply,
			     int index)
{
	struct lu_fid fid;
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0)
		return -ENOTDIR;

	name = object_update_param_get(u, 0, NULL);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	rc = dt_lookup(env, o, (struct dt_rec *)&fid, (struct dt_key *)name);

	object_update_result_insert(reply, &fid, sizeof(fid), index, rc);

	return 0;
}

static int obe_replay_insert_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct thandle *th, int exec)
{
	struct dt_insert_rec rec;
	struct lu_fid *fid;
	size_t		size = 0;
	__u32		*ptype;
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0)
		return -ENOTDIR;

	name = object_update_param_get(u, 0, NULL);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	fid = object_update_param_get(u, 1, &size);
	if (unlikely(IS_ERR(fid)))
		return PTR_ERR(fid);
	if (unlikely(fid == NULL || size != sizeof(*fid)))
		return -EINVAL;

	ptype = object_update_param_get(u, 2, &size);
	if (unlikely(IS_ERR(ptype)))
		return PTR_ERR(fid);
	if (unlikely(ptype == NULL || size != sizeof(*ptype)))
		return -EINVAL;

	rec.rec_fid = fid;
	rec.rec_type = *ptype;

	if (exec == 0)
		rc = dt_declare_insert(env, o, (struct dt_rec *)&rec,
				       (struct dt_key *)name, th);
	else
		rc = dt_insert(env, o, (struct dt_rec *)&rec,
			       (struct dt_key *)name, th, 1);

	return rc;
}

static int obe_replay_delete_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct thandle *th, int exec)
{
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0)
		return -ENOTDIR;

	name = object_update_param_get(u, 0, NULL);
	if (unlikely(IS_ERR(name)))
		return PTR_ERR(name);
	if (unlikely(name == NULL))
		return -EINVAL;

	if (exec == 0)
		rc = dt_declare_delete(env, o, (struct dt_key *)name,
					th);
	else
		rc = dt_delete(env, o, (struct dt_key *)name, th);

	return rc;
}

static int obe_replay_read(const struct lu_env *env,
			   struct dt_object *o,
			   struct object_update *u,
			   struct object_update_reply *reply,
			   int index)
{
	struct object_update_result *res;
	__u64 *tmp, pos;
	struct lu_buf lb;
	int rc;

	tmp = object_update_param_get(u, 0, NULL);
	if (tmp == NULL || IS_ERR(tmp))
		return -EPROTO;
	lb.lb_len = le64_to_cpu(*(size_t *)(tmp));
	if (lb.lb_len > u->ou_result_size)
		return -E2BIG;

	tmp = object_update_param_get(u, 1, NULL);
	if (tmp == NULL || IS_ERR(tmp))
		return -EPROTO;
	pos = le64_to_cpu(*(__u64 *)(tmp));

	res = object_update_result_get(reply, index, NULL);
	if (res == NULL)
		return -EPROTO;
	lb.lb_buf = res->our_data;

	rc = dt_read(env, o, &lb, &pos);
	lb.lb_len = rc;
	if (rc < 0)
		lb.lb_len = 0;

	object_update_result_insert(reply, NULL, lb.lb_len, index, rc);
	/* XXX: a bit hacky, need a way to do this with a correct helper */
	res->our_datalen = lb.lb_len;

	return 0;
}

static int obe_replay_write_update(const struct lu_env *env,
				   struct dt_object *o,
				   struct object_update *u,
				   struct thandle *th, int exec)
{
	__u64 *tmp, pos;
	struct lu_buf lb;
	size_t size = 0;
	int rc;

	lb.lb_buf = object_update_param_get(u, 0, &size);
	if (unlikely(IS_ERR(lb.lb_buf)))
		return PTR_ERR(lb.lb_buf);
	if (unlikely(lb.lb_buf == NULL))
		return -EINVAL;
	if (unlikely(size == 0 || size > (16 * 1024)))
		return -EINVAL;
	lb.lb_len = size;

	tmp = object_update_param_get(u, 1, &size);
	if (unlikely(IS_ERR(tmp)))
		return PTR_ERR(tmp);
	if (unlikely(tmp == NULL))
		return -EINVAL;
	pos = *tmp;

	if (exec == 0) {
		rc = dt_declare_write(env, o, &lb, pos, th);
	} else {
		rc = dt_write(env, o, &lb, &pos, th, 0);
		if (rc == lb.lb_len)
			rc = 0;
		else if (rc >= 0)
			rc = -EFAULT;
	}

	return rc;
}

static osd_update_func obe_updates_vec[] = {
	[OUT_CREATE]		= obe_replay_create_update,
	[OUT_DESTROY]		= obe_replay_destroy_update,
	[OUT_REF_ADD]		= obe_replay_ref_add_update,
	[OUT_REF_DEL]		= obe_replay_ref_del_update,
	[OUT_ATTR_SET]		= obe_replay_attr_set_update,
	[OUT_XATTR_SET]		= obe_replay_xattr_set_update,
	[OUT_XATTR_DEL]		= obe_replay_xattr_del_update,
	[OUT_INDEX_INSERT]	= obe_replay_insert_update,
	[OUT_INDEX_DELETE]	= obe_replay_delete_update,
	[OUT_WRITE]		= obe_replay_write_update,
};

static osd_access_func obe_access_vec[] = {
	[OUT_ATTR_GET]		= obe_replay_attr_get,
	[OUT_XATTR_GET]		= obe_replay_xattr_get,
	[OUT_INDEX_LOOKUP]	= obe_replay_lookup,
	[OUT_READ]		= obe_replay_read,
};

static int obe_updates_rw[] = {
	[OUT_CREATE]		= 1,
	[OUT_DESTROY]		= 1,
	[OUT_REF_ADD]		= 1,
	[OUT_REF_DEL]		= 1,
	[OUT_ATTR_SET]		= 1,
	[OUT_ATTR_GET]		= 0,
	[OUT_XATTR_SET]		= 1,
	[OUT_XATTR_GET]		= 0,
	[OUT_XATTR_DEL]		= 1,
	[OUT_INDEX_LOOKUP]	= 0,
	[OUT_INDEX_INSERT]	= 1,
	[OUT_INDEX_DELETE]	= 1,
	[OUT_WRITE]		= 1,
	[OUT_READ]		= 0,
};

static int obe_execute(const struct lu_env *env, struct obe_tx *obt)
{
	struct obe_update *obu, *tmp;
	int rc, rc2, type;
	ENTRY;

	LASSERT(obt != NULL);
	LASSERT(obt->obt_th != NULL);

	CDEBUG(D_OTHER, "apply tx %llu\n", obt->obt_batchid);

	rc = dt_trans_start(env, obt->obt_dt, obt->obt_th);

	list_for_each_entry(obu, &obt->obt_updates_list, obu_list) {

		if (unlikely(rc < 0))
			break;

		type = obu->obu_update->ou_type;
		rc = obe_updates_vec[type](env, obu->obu_object,
					   obu->obu_update, obt->obt_th, 1);
		CDEBUG(D_OTHER, "%s: %d\n",  update_op_str(type), rc);
		object_update_result_insert(obt->obt_reply, NULL, 0,
					    obu->obu_index, rc);
	}

	rc2 = dt_trans_stop(env, obt->obt_dt, obt->obt_th);
	if (rc == 0)
		rc = rc2;

	list_for_each_entry_safe(obu, tmp, &obt->obt_updates_list, obu_list) {
		lu_object_put(env, &obu->obu_object->do_lu);
		list_del(&obu->obu_list);
		OBD_FREE_PTR(obu);
	}

	OBD_FREE_PTR(obt);

	RETURN(rc);
}

static int obe_abort(const struct lu_env *env, struct obe_tx *obt)
{
	struct obe_update *obu, *tmp;
	int rc;
	ENTRY;

	LASSERT(obt != NULL);
	LASSERT(obt->obt_th != NULL);

	list_for_each_entry_safe(obu, tmp, &obt->obt_updates_list, obu_list) {
		if (obu->obu_object != NULL)
			lu_object_put(env, &obu->obu_object->do_lu);
		list_del(&obu->obu_list);
		OBD_FREE_PTR(obu);
	}

	rc = dt_trans_stop(env, obt->obt_dt, obt->obt_th);

	OBD_FREE_PTR(obt);

	RETURN(rc);
}

static struct obe_tx *
obe_create_tx(const struct lu_env *env, struct obe_device *obe)
{
	struct obe_tx *tx;
	ENTRY;

	OBD_ALLOC_PTR(tx);
	if (tx == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	tx->obt_th = dt_trans_create(env, obe->obe_osd);
	if (IS_ERR(tx->obt_th)) {
		void *p = tx->obt_th;
		OBD_FREE_PTR(tx);
		RETURN(p);
	}

	INIT_LIST_HEAD(&tx->obt_updates_list);
	tx->obt_dt = obe->obe_osd;

	RETURN(tx);
}

static int
obe_perform(const struct lu_env *env, struct obe_device *obe,
		 struct object_update_request *ureq,
		 struct object_update_reply *urep)
{
	struct obe_tx	*obt = NULL;
	struct obe_update	*obu;
	__u64 batchid = -1ULL;
	struct dt_object *o;
	int i, rc = 0, type = 0;

	if (ureq->ourq_count == 0)
		RETURN(0);

	for (i = 0; i < ureq->ourq_count; i++) {
		struct object_update *update;
		size_t size;

		update = object_update_request_get(ureq, i, &size);
		LASSERT(update != NULL);
		type = update->ou_type;

		if (obe_updates_rw[type] == 0) {
			o = obe_locate(env, obe, &update->ou_fid);
			if (IS_ERR(o))
				GOTO(out, rc = PTR_ERR(o));
			rc = obe_access_vec[type](env, o, update, urep, i);
			if (rc < 0)
				GOTO(out, rc);
			lu_object_put(env, &o->do_lu);
			continue;
		}

		/* this is a modification update */
		if (update->ou_batchid != batchid) {
			/* new transaction starts, complete the previous one */
			if (obt != NULL) {
				rc = obe_execute(env, obt);
				obt = NULL;
				if (rc < 0) {
					/* actual error code should be stored
					 * in the reply */
					rc = 0;
					break;
				}
			}

			obt = obe_create_tx(env, obe);
			LASSERT(!IS_ERR(obt));
			batchid = update->ou_batchid;
			obt->obt_batchid = batchid;
			obt->obt_reply = urep;
		}

		OBD_ALLOC_PTR(obu);
		if (unlikely(obu == NULL))
			GOTO(abort, rc = -ENOMEM);

		list_add_tail(&obu->obu_list, &obt->obt_updates_list);
		obu->obu_update = update;
		obu->obu_index = i;

		if (type == OUT_CREATE)
			o = obe_locate_new(env, obe, &update->ou_fid);
		else
			o = obe_locate(env, obe, &update->ou_fid);
		if (IS_ERR(o))
			GOTO(abort, rc = PTR_ERR(o));
		obu->obu_object = o;

		rc = obe_updates_vec[type](env, obu->obu_object,
					   obu->obu_update, obt->obt_th, 0);
		if (rc < 0)
			GOTO(abort, rc);
	}
	if (obt != NULL) {
		rc = obe_execute(env, obt);
		/* actual error code should be stored
		 * in the reply */
		rc = 0;
	}

	RETURN(rc);

abort:
	LASSERT(obt != NULL);
	CERROR("can't declare #%d ( %s ): rc = %d\n", i,
	       update_op_str(type), rc);
	obe_abort(env, obt);
	obt = NULL;

out:
	LASSERT(obt == NULL);
	RETURN(rc);
}

static int
obe_ioc_exec(unsigned int cmd, struct obd_export *exp, int len,
		   void *karg, void *uarg)
{
	struct obd_device *obd = exp->exp_obd;
	void *p = &obd->u.obt;
	struct obe_device *obe = *((struct obe_device **)p);
	struct obd_ioctl_data  *data = karg;
	struct object_update_request *ureq = NULL;
	struct lu_env *env = NULL;
	int rc = 0;
	ENTRY;

	if (unlikely(data->ioc_plen1 == 0))
		RETURN(0);

	if (!access_ok(VERIFY_READ, data->ioc_pbuf1, data->ioc_plen1))
		RETURN(-EFAULT);
	ureq = (struct object_update_request *)data->ioc_pbuf1;
	if (unlikely(ureq->ourq_magic != UPDATE_REQUEST_MAGIC))
		GOTO(out, rc = -EINVAL);

	if (data->ioc_pbuf2) {
		if (data->ioc_plen2 == 0)
			RETURN(-EINVAL);
		if (!access_ok(VERIFY_WRITE, data->ioc_pbuf2, data->ioc_plen2))
			RETURN(-EFAULT);
	}

	OBD_ALLOC_PTR(env);
	if (env == NULL)
		RETURN(-ENOMEM);

	rc = lu_env_init(env, LCT_DT_THREAD);
	if (rc)
		GOTO(out, rc = -ENOMEM);

	rc = obe_perform(env, obe, (void *)data->ioc_pbuf1,
			 (void *)data->ioc_pbuf2);

	EXIT;

out:
	if (env) {
		lu_env_fini(env);
		OBD_FREE_PTR(env);
	}

	return rc;
}

static int
obe_ioc_purge(struct obd_export *exp)
{
	struct obd_device *obd = exp->exp_obd;
	void *p = &obd->u.obt;
	struct obe_device *obe = *((struct obe_device **)p);
	struct lu_env *env = NULL;
	int rc = 0;
	ENTRY;

	OBD_ALLOC_PTR(env);
	if (env == NULL)
		RETURN(-ENOMEM);

	rc = lu_env_init(env, LCT_DT_THREAD);
	if (rc)
		GOTO(out, rc = -ENOMEM);

	lu_site_purge(env, obe->obe_osd->dd_lu_dev.ld_site, -1);

	lu_env_fini(env);
out:
	if (env)
		OBD_FREE_PTR(env);

	return rc;
}

static int
obe_iocontrol(unsigned int cmd, struct obd_export *exp, int len,
	      void *karg, void *uarg)
{
	int rc;

	if (cmd == OBD_IOC_OBE_EXEC)
		rc = obe_ioc_exec(cmd, exp, len, karg, uarg);
	else if (cmd == OBD_IOC_OBE_EXEC)
		rc = obe_ioc_purge(exp);
	else
		rc = -EINVAL;
	return rc;
}

static struct obe_device *obe_dev_prepare(struct dt_device *dt)
{
	struct obe_device *obe;

	/* not found, then create */
	OBD_ALLOC_PTR(obe);
	if (unlikely(obe == NULL))
		return ERR_PTR(-ENOMEM);

	atomic_set(&obe->obe_refcount, 1);

	obe->obe_osd = dt;

	LASSERT(dt->dd_lu_dev.ld_site);
	lu_device_init(&obe->obe_top_dev.dd_lu_dev, &obe_lu_type);
	obe->obe_top_dev.dd_lu_dev.ld_ops = &obe_lu_dev_ops;
	obe->obe_top_dev.dd_lu_dev.ld_site = dt->dd_lu_dev.ld_site;

	return obe;
}

static void obe_dev_fini(struct obe_device *obe)
{
	lu_device_fini(&obe->obe_top_dev.dd_lu_dev);
	OBD_FREE_PTR(obe);
}

static int obe_setup(struct obd_device *obddev, struct lustre_cfg *lcfg)
{
	struct obd_uuid obe_uuid = { "OSDBENCH_UUID" };
	struct obe_device *obe;
	struct obd_device *tgt;
	struct obd_connect_data *ocd = NULL;
	void *p = &obddev->u.obt;
	int rc;
	ENTRY;

	if (lcfg->lcfg_bufcount < 2 || LUSTRE_CFG_BUFLEN(lcfg, 1) < 1) {
		CERROR("requires a TARGET OBD name\n");
		RETURN(-EINVAL);
	}

	tgt = class_name2obd(lustre_cfg_string(lcfg, 1));
	if (!tgt || !tgt->obd_attached || !tgt->obd_set_up) {
		CERROR("device not attached or not set up (%s)\n",
				lustre_cfg_string(lcfg, 1));
		RETURN(-EINVAL);
	}
	if (tgt->obd_lu_dev == NULL || !lu_device_is_dt(tgt->obd_lu_dev))
		RETURN(-ENOTSUPP);

	obe = obe_dev_prepare(lu2dt_dev(tgt->obd_lu_dev));
	if (IS_ERR(obe))
		RETURN(PTR_ERR(obe));

	OBD_ALLOC(ocd, sizeof(*ocd));
	if (ocd == NULL) {
		CERROR("Can't alloc ocd connecting to %s\n",
				lustre_cfg_string(lcfg, 1));
		return -ENOMEM;
	}
	ocd->ocd_version = LUSTRE_VERSION_CODE;

	rc = obd_connect(NULL, &obe->obe_exp, tgt, &obe_uuid, ocd, NULL);

	OBD_FREE(ocd, sizeof(*ocd));

	if (rc != 0) {
		CERROR("fail to connect to device %s\n",
				lustre_cfg_string(lcfg, 1));
		RETURN(rc);
	}

	*((struct obe_device **)p) = obe;

	/* XXX: release obe in case of error */

	RETURN(rc);
}

static int obe_cleanup(struct obd_device *obddev)
{
	void *p = &obddev->u.obt;
	struct obe_device *obe = *((struct obe_device **)p);
	int rc;
	ENTRY;

	/*Do nothing for Metadata echo client*/
	if (obe == NULL)
		RETURN(0);

	lu_site_purge(NULL, obe->obe_top_dev.dd_lu_dev.ld_site, ~0);

	LASSERT(atomic_read(&obe->obe_exp->exp_refcount) > 0);
	rc = obd_disconnect(obe->obe_exp);
	if (rc != 0)
		CERROR("fail to disconnect device: %d\n", rc);

	obe_dev_fini(obe);

	RETURN(rc);
}

static int obe_connect(const struct lu_env *env, struct obd_export **exp,
			    struct obd_device *src, struct obd_uuid *cluuid,
			    struct obd_connect_data *data, void *localdata)
{
	int                rc;
	struct lustre_handle conn = { 0 };

	ENTRY;
	rc = class_connect(&conn, src, cluuid);
	if (rc == 0)
		*exp = class_conn2export(&conn);

	RETURN(rc);
}

static int obe_disconnect(struct obd_export *exp)
{
	int                     rc;
	ENTRY;

	if (exp == NULL)
		GOTO(out, rc = -EINVAL);

	rc = class_disconnect(exp);
	GOTO(out, rc);
out:
	return rc;
}

static struct obd_ops obe_obd_ops = {
	.o_owner       = THIS_MODULE,
	.o_iocontrol   = obe_iocontrol,
	.o_connect     = obe_connect,
	.o_disconnect  = obe_disconnect,
	.o_setup       = obe_setup,
	.o_cleanup     = obe_cleanup
};

static int __init osdbench_init(void)
{
	int rc;

	ENTRY;
	LCONSOLE_INFO("OSDBench driver; http://www.lustre.org/\n");

# ifdef HAVE_SERVER_SUPPORT
	rc = class_register_type(&obe_obd_ops, NULL, true, NULL,
				 LUSTRE_OSDBENCH_NAME, NULL);
	LU_CONTEXT_KEY_INIT(&obe_thread_key);
	lu_context_key_register(&obe_thread_key);
#else
	RETURN(-ENOTSUP);
# endif

	RETURN(rc);
}

static void /*__exit*/ osdbench_exit(void)
{
# ifdef HAVE_SERVER_SUPPORT
	lu_context_key_degister(&obe_thread_key);
	class_unregister_type(LUSTRE_OSDBENCH_NAME);
# endif
}

MODULE_AUTHOR("Intel Corporation <http://www.intel.com/>");
MODULE_DESCRIPTION("Lustre OSD Benchmark driver");
MODULE_VERSION(LUSTRE_VERSION_STRING);
MODULE_LICENSE("GPL");

module_init(osdbench_init);
module_exit(osdbench_exit);

/** @} echo_client */
