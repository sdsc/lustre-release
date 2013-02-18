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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/md_local_object.c
 *
 * Lustre Local Object create APIs
 * 'create on first mount' facility. Files registed under llo module will
 * be created on first mount.
 *
 * Author: Pravin Shelar  <pravin.shelar@sun.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd_support.h>
#include <lustre_disk.h>
#include <lustre_fid.h>
#include <lu_object.h>
#include <libcfs/list.h>
#include <md_object.h>


/** List head to hold list of objects to be created. */
static cfs_list_t llo_lobj_list;

/** Lock to protect list manipulations */
static struct mutex	llo_lock;

/**
 * Structure used to maintain state of path parsing.
 * \see llo_find_entry, llo_store_resolve
 */
struct llo_find_hint {
        struct lu_fid        *lfh_cfid;
        struct md_device     *lfh_md;
        struct md_object     *lfh_pobj;
};

/**
 * Thread Local storage for this module.
 */
struct llo_thread_info {
        /** buffer to resolve path */
        char                    lti_buf[DT_MAX_PATH];
        /** used for path resolve */
        struct lu_fid           lti_fid;
        /** used to pass child object fid */
        struct lu_fid           lti_cfid;
        struct llo_find_hint    lti_lfh;
        struct md_op_spec       lti_spc;
        struct md_attr          lti_ma;
        struct lu_name          lti_lname;
};

LU_KEY_INIT(llod_global, struct llo_thread_info);
LU_KEY_FINI(llod_global, struct llo_thread_info);

static struct lu_context_key llod_key = {
        .lct_tags = LCT_MD_THREAD,
        .lct_init = llod_global_key_init,
        .lct_fini = llod_global_key_fini
};

static inline struct llo_thread_info * llo_env_info(const struct lu_env *env)
{
        return lu_context_key_get(&env->le_ctx,  &llod_key);
}

/**
 * Search md object for given fid.
 */
static struct md_object *llo_locate(const struct lu_env *env,
                                    struct md_device *md,
                                    const struct lu_fid *fid)
{
        struct lu_object *obj;
        struct md_object *mdo;

        obj = lu_object_find(env, &md->md_lu_dev, fid, NULL);
        if (!IS_ERR(obj)) {
                obj = lu_object_locate(obj->lo_header, md->md_lu_dev.ld_type);
                LASSERT(obj != NULL);
                mdo = (struct md_object *) obj;
        } else
                mdo = (struct md_object *)obj;
        return mdo;
}

/**
 * Lookup FID for object named \a name in directory \a pobj.
 */
static int llo_lookup(const struct lu_env  *env,
                      struct md_object *pobj,
                      const char *name,
                      struct lu_fid *fid)
{
        struct llo_thread_info *info = llo_env_info(env);
        struct lu_name          *lname = &info->lti_lname;
        struct md_op_spec       *spec = &info->lti_spc;

        spec->sp_feat = NULL;
        spec->sp_cr_flags = 0;
        spec->sp_cr_lookup = 0;
        spec->sp_cr_mode = 0;

        lname->ln_name = name;
        lname->ln_namelen = strlen(name);

        return mdo_lookup(env, pobj, lname, fid, spec);
}

/**
 * Function to look up path component, this is passed to parsing
 * function. \see llo_store_resolve
 *
 * \retval      rc returns error code for lookup or locate operation
 *
 * pointer to object is returned in data (lfh->lfh_pobj)
 */
static int llo_find_entry(const struct lu_env  *env,
                          const char *name, void *data)
{
        struct llo_find_hint    *lfh = data;
        struct md_device        *md = lfh->lfh_md;
        struct lu_fid           *fid = lfh->lfh_cfid;
        struct md_object        *obj = lfh->lfh_pobj;
        int                     result;

        /* lookup fid for object */
        result = llo_lookup(env, obj, name, fid);
        lu_object_put(env, &obj->mo_lu);

        if (result == 0) {
                /* get md object for fid that we got in lookup */
                obj = llo_locate(env, md, fid);
                if (IS_ERR(obj))
                        result = PTR_ERR(obj);
        }

        lfh->lfh_pobj = obj;
        return result;
}

/**
 * Resolve given \a path, on success function returns
 * md object for last directory and \a fid points to
 * its fid.
 */
static struct md_object *llo_store_resolve(const struct lu_env *env,
					   struct md_device *md,
					   struct dt_device *dt,
					   const char *path,
					   struct lu_fid *fid)
{
        struct llo_thread_info *info = llo_env_info(env);
        struct llo_find_hint *lfh = &info->lti_lfh;
        char *local = info->lti_buf;
        struct md_object        *obj;
        int result;

        strncpy(local, path, DT_MAX_PATH);
        local[DT_MAX_PATH - 1] = '\0';

        lfh->lfh_md = md;
        lfh->lfh_cfid = fid;
        /* start path resolution from backend fs root. */
        result = dt->dd_ops->dt_root_get(env, dt, fid);
        if (result == 0) {
                /* get md object for root */
                obj = llo_locate(env, md, fid);
                if (!IS_ERR(obj)) {
                        /* start path parser from root md */
                        lfh->lfh_pobj = obj;
                        result = dt_path_parser(env, local, llo_find_entry, lfh);
                        if (result != 0)
                                obj = ERR_PTR(result);
                        else
                                obj = lfh->lfh_pobj;
                }
        } else {
                obj = ERR_PTR(result);
        }
        return obj;
}

static struct md_object *llo_create_obj(const struct lu_env *env,
                                        struct md_device *md,
                                        struct md_object *dir,
					const struct lu_local_obj_desc *llod)
{
        struct llo_thread_info *info = llo_env_info(env);
        struct md_object        *mdo;
        struct md_attr          *ma = &info->lti_ma;
        struct md_op_spec       *spec = &info->lti_spc;
        struct lu_name          *lname = &info->lti_lname;
        struct lu_attr          *la = &ma->ma_attr;
        int rc;

	mdo = llo_locate(env, md, llod->llod_fid);
	if (IS_ERR(mdo))
		return mdo;

	lname->ln_name = llod->llod_name;
	lname->ln_namelen = strlen(llod->llod_name);

	spec->sp_feat = llod->llod_feat;
	spec->sp_cr_flags = 0;
	spec->sp_cr_lookup = 1;
	spec->sp_cr_mode = 0;

	/* We only copy mode, uid, and gid, so reject anthing else. */
	LASSERT((llod->llod_attr.la_valid & ~(LA_MODE | LA_UID | LA_GID)) == 0);

	if (llod->llod_attr.la_valid & LA_MODE)
		la->la_mode = llod->llod_attr.la_mode;
	else if (llod->llod_feat == &dt_directory_features)
		la->la_mode = S_IFDIR | S_IRUGO | S_IWUSR | S_IXUGO;
	else
		la->la_mode = S_IFREG | S_IRUGO | S_IWUSR;

	if (llod->llod_attr.la_valid & LA_UID)
		la->la_uid = llod->llod_attr.la_uid;
	else
		la->la_uid = 0;

	if (llod->llod_attr.la_valid & LA_GID)
		la->la_gid = llod->llod_attr.la_gid;
	else
		la->la_gid = 0;

        la->la_valid = LA_MODE | LA_UID | LA_GID;

        ma->ma_valid = 0;
        ma->ma_need = 0;

        rc = mdo_create(env, dir, lname, mdo, spec, ma);

        if (rc) {
                lu_object_put(env, &mdo->mo_lu);
                mdo = ERR_PTR(rc);
        }

        return mdo;
}

struct md_object *llo_create(const struct lu_env *env,
			     struct md_device *md,
			     struct dt_device *dt,
			     const struct lu_local_obj_desc *llod)
{
	struct llo_thread_info *info = llo_env_info(env);
	struct md_object *obj;
	struct md_object *dir;
	struct lu_fid *ignore = &info->lti_fid;

	if (fid_is_zero(llod->llod_dir_fid))
		dir = llo_store_resolve(env, md, dt, "", ignore);
	else
		dir = llo_locate(env, md, llod->llod_dir_fid);

	if (!IS_ERR(dir)) {
		obj = llo_create_obj(env, md, dir, llod);
		lu_object_put(env, &dir->mo_lu);
	} else {
		obj = dir;
	}

	return obj;
}
EXPORT_SYMBOL(llo_create);

/**
 * Register object for 'create on first mount' facility.
 * objects are created in order of registration.
 */

void llo_local_obj_register(struct lu_local_obj_desc *llod)
{
	mutex_lock(&llo_lock);
        cfs_list_add_tail(&llod->llod_linkage, &llo_lobj_list);
	mutex_unlock(&llo_lock);
}

EXPORT_SYMBOL(llo_local_obj_register);

void llo_local_obj_unregister(struct lu_local_obj_desc *llod)
{
	mutex_lock(&llo_lock);
        cfs_list_del(&llod->llod_linkage);
	mutex_unlock(&llo_lock);
}

EXPORT_SYMBOL(llo_local_obj_unregister);

/**
 * Created registed objects.
 */

int llo_local_objects_setup(const struct lu_env *env,
			    struct md_device *md,
			    struct dt_device *dt)
{
        struct lu_local_obj_desc *scan;
        struct md_object *mdo;
        int rc = 0;

	mutex_lock(&llo_lock);

	cfs_list_for_each_entry(scan, &llo_lobj_list, llod_linkage) {
		mdo = llo_create(env, md, dt, scan);
		if (IS_ERR(mdo) && PTR_ERR(mdo) != -EEXIST) {
			rc = PTR_ERR(mdo);
			CERROR("creating obj [%s] fid = "DFID" rc = %d\n",
			       scan->llod_name, PFID(scan->llod_fid), rc);
			goto out;
		}

		if (!IS_ERR(mdo))
			lu_object_put(env, &mdo->mo_lu);
	}

out:
	mutex_unlock(&llo_lock);
        return rc;
}

EXPORT_SYMBOL(llo_local_objects_setup);

int llo_global_init(void)
{
        int result;

        CFS_INIT_LIST_HEAD(&llo_lobj_list);
	mutex_init(&llo_lock);

        LU_CONTEXT_KEY_INIT(&llod_key);
        result = lu_context_key_register(&llod_key);
        return result;
}

void llo_global_fini(void)
{
        lu_context_key_degister(&llod_key);
        LASSERT(cfs_list_empty(&llo_lobj_list));
}
