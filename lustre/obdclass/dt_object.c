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
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/dt_object.c
 *
 * Dt Object.
 * Generic functions from dt_object.h
 *
 * Author: Nikita Danilov <nikita@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>
#include <dt_object.h>
#include <libcfs/list.h>
/* fid_be_to_cpu() */
#include <lustre_fid.h>

#include <lquota.h>

static inline const struct dt_index_features *dt_index_feat_select(__u64,
								   __u32);

struct dt_find_hint {
        struct lu_fid        *dfh_fid;
        struct dt_device     *dfh_dt;
        struct dt_object     *dfh_o;
};

struct dt_thread_info {
        char                    dti_buf[DT_MAX_PATH];
        struct dt_find_hint     dti_dfh;
};

/* context key constructor/destructor: dt_global_key_init, dt_global_key_fini */
LU_KEY_INIT(dt_global, struct dt_thread_info);
LU_KEY_FINI(dt_global, struct dt_thread_info);

static struct lu_context_key dt_key = {
        .lct_tags = LCT_MD_THREAD | LCT_DT_THREAD | LCT_MG_THREAD | LCT_LOCAL,
        .lct_init = dt_global_key_init,
        .lct_fini = dt_global_key_fini
};

/* no lock is necessary to protect the list, because call-backs
 * are added during system startup. Please refer to "struct dt_device".
 */
void dt_txn_callback_add(struct dt_device *dev, struct dt_txn_callback *cb)
{
        cfs_list_add(&cb->dtc_linkage, &dev->dd_txn_callbacks);
}
EXPORT_SYMBOL(dt_txn_callback_add);

void dt_txn_callback_del(struct dt_device *dev, struct dt_txn_callback *cb)
{
        cfs_list_del_init(&cb->dtc_linkage);
}
EXPORT_SYMBOL(dt_txn_callback_del);

int dt_txn_hook_start(const struct lu_env *env,
                      struct dt_device *dev, struct thandle *th)
{
        int rc = 0;
        struct dt_txn_callback *cb;

        if (th->th_local)
                return 0;

        cfs_list_for_each_entry(cb, &dev->dd_txn_callbacks, dtc_linkage) {
                if (cb->dtc_txn_start == NULL ||
                    !(cb->dtc_tag & env->le_ctx.lc_tags))
                        continue;
                rc = cb->dtc_txn_start(env, th, cb->dtc_cookie);
                if (rc < 0)
                        break;
        }
        return rc;
}
EXPORT_SYMBOL(dt_txn_hook_start);

int dt_txn_hook_stop(const struct lu_env *env, struct thandle *txn)
{
        struct dt_device       *dev = txn->th_dev;
        struct dt_txn_callback *cb;
        int                     rc = 0;

        if (txn->th_local)
                return 0;

        cfs_list_for_each_entry(cb, &dev->dd_txn_callbacks, dtc_linkage) {
                if (cb->dtc_txn_stop == NULL ||
                    !(cb->dtc_tag & env->le_ctx.lc_tags))
                        continue;
                rc = cb->dtc_txn_stop(env, txn, cb->dtc_cookie);
                if (rc < 0)
                        break;
        }
        return rc;
}
EXPORT_SYMBOL(dt_txn_hook_stop);

void dt_txn_hook_commit(struct thandle *txn)
{
        struct dt_txn_callback *cb;

        if (txn->th_local)
                return;

        cfs_list_for_each_entry(cb, &txn->th_dev->dd_txn_callbacks,
                                dtc_linkage) {
                if (cb->dtc_txn_commit)
                        cb->dtc_txn_commit(txn, cb->dtc_cookie);
        }
}
EXPORT_SYMBOL(dt_txn_hook_commit);

int dt_device_init(struct dt_device *dev, struct lu_device_type *t)
{

        CFS_INIT_LIST_HEAD(&dev->dd_txn_callbacks);
        return lu_device_init(&dev->dd_lu_dev, t);
}
EXPORT_SYMBOL(dt_device_init);

void dt_device_fini(struct dt_device *dev)
{
        lu_device_fini(&dev->dd_lu_dev);
}
EXPORT_SYMBOL(dt_device_fini);

int dt_object_init(struct dt_object *obj,
                   struct lu_object_header *h, struct lu_device *d)

{
        return lu_object_init(&obj->do_lu, h, d);
}
EXPORT_SYMBOL(dt_object_init);

void dt_object_fini(struct dt_object *obj)
{
        lu_object_fini(&obj->do_lu);
}
EXPORT_SYMBOL(dt_object_fini);

int dt_try_as_dir(const struct lu_env *env, struct dt_object *obj)
{
        if (obj->do_index_ops == NULL)
                obj->do_ops->do_index_try(env, obj, &dt_directory_features);
        return obj->do_index_ops != NULL;
}
EXPORT_SYMBOL(dt_try_as_dir);

enum dt_format_type dt_mode_to_dft(__u32 mode)
{
        enum dt_format_type result;

        switch (mode & S_IFMT) {
        case S_IFDIR:
                result = DFT_DIR;
                break;
        case S_IFREG:
                result = DFT_REGULAR;
                break;
        case S_IFLNK:
                result = DFT_SYM;
                break;
        case S_IFCHR:
        case S_IFBLK:
        case S_IFIFO:
        case S_IFSOCK:
                result = DFT_NODE;
                break;
        default:
                LBUG();
                break;
        }
        return result;
}
EXPORT_SYMBOL(dt_mode_to_dft);

/**
 * lookup fid for object named \a name in directory \a dir.
 */

int dt_lookup_dir(const struct lu_env *env, struct dt_object *dir,
                  const char *name, struct lu_fid *fid)
{
        if (dt_try_as_dir(env, dir))
                return dt_lookup(env, dir, (struct dt_rec *)fid,
                                 (const struct dt_key *)name, BYPASS_CAPA);
        return -ENOTDIR;
}
EXPORT_SYMBOL(dt_lookup_dir);
/**
 * get object for given \a fid.
 */
struct dt_object *dt_locate(const struct lu_env *env,
                            struct dt_device *dev,
                            const struct lu_fid *fid)
{
        struct lu_object *obj;
        struct dt_object *dt;

        obj = lu_object_find(env, &dev->dd_lu_dev, fid, NULL);
        if (!IS_ERR(obj)) {
                obj = lu_object_locate(obj->lo_header, dev->dd_lu_dev.ld_type);
                LASSERT(obj != NULL);
                dt = container_of(obj, struct dt_object, do_lu);
        } else
                dt = (struct dt_object *)obj;
        return dt;
}
EXPORT_SYMBOL(dt_locate);

/**
 * find a object named \a entry in given \a dfh->dfh_o directory.
 */
static int dt_find_entry(const struct lu_env *env, const char *entry, void *data)
{
        struct dt_find_hint  *dfh = data;
        struct dt_device     *dt = dfh->dfh_dt;
        struct lu_fid        *fid = dfh->dfh_fid;
        struct dt_object     *obj = dfh->dfh_o;
        int                   result;

        result = dt_lookup_dir(env, obj, entry, fid);
        lu_object_put(env, &obj->do_lu);
        if (result == 0) {
                obj = dt_locate(env, dt, fid);
                if (IS_ERR(obj))
                        result = PTR_ERR(obj);
        }
        dfh->dfh_o = obj;
        return result;
}

/**
 * Abstract function which parses path name. This function feeds
 * path component to \a entry_func.
 */
int dt_path_parser(const struct lu_env *env,
                   char *path, dt_entry_func_t entry_func,
                   void *data)
{
        char *e;
        int rc = 0;

        while (1) {
                e = strsep(&path, "/");
                if (e == NULL)
                        break;

                if (e[0] == 0) {
                        if (!path || path[0] == '\0')
                                break;
                        continue;
                }
                rc = entry_func(env, e, data);
                if (rc)
                        break;
        }

        return rc;
}

static struct dt_object *dt_store_resolve(const struct lu_env *env,
                                          struct dt_device *dt,
                                          const char *path,
                                          struct lu_fid *fid)
{
        struct dt_thread_info *info = lu_context_key_get(&env->le_ctx,
                                                         &dt_key);
        struct dt_find_hint *dfh = &info->dti_dfh;
        struct dt_object     *obj;
        char *local = info->dti_buf;
        int result;

        dfh->dfh_dt = dt;
        dfh->dfh_fid = fid;

        strncpy(local, path, DT_MAX_PATH);
        local[DT_MAX_PATH - 1] = '\0';

        result = dt->dd_ops->dt_root_get(env, dt, fid);
        if (result == 0) {
                obj = dt_locate(env, dt, fid);
                if (!IS_ERR(obj)) {
                        dfh->dfh_o = obj;
                        result = dt_path_parser(env, local, dt_find_entry, dfh);
                        if (result != 0)
                                obj = ERR_PTR(result);
                        else
                                obj = dfh->dfh_o;
                }
        } else {
                obj = ERR_PTR(result);
        }
        return obj;
}

static struct dt_object *dt_reg_open(const struct lu_env *env,
                                     struct dt_device *dt,
                                     struct dt_object *p,
                                     const char *name,
                                     struct lu_fid *fid)
{
        struct dt_object *o;
        int result;

        result = dt_lookup_dir(env, p, name, fid);
        if (result == 0){
                o = dt_locate(env, dt, fid);
        }
        else
                o = ERR_PTR(result);

        return o;
}

/**
 * Open dt object named \a filename from \a dirname directory.
 *      \param  dt      dt device
 *      \param  fid     on success, object fid is stored in *fid
 */
struct dt_object *dt_store_open(const struct lu_env *env,
                                struct dt_device *dt,
                                const char *dirname,
                                const char *filename,
                                struct lu_fid *fid)
{
        struct dt_object *file;
        struct dt_object *dir;

        dir = dt_store_resolve(env, dt, dirname, fid);
        if (!IS_ERR(dir)) {
                file = dt_reg_open(env, dt, dir,
                                   filename, fid);
                lu_object_put(env, &dir->do_lu);
        } else {
                file = dir;
        }
        return file;
}
EXPORT_SYMBOL(dt_store_open);

struct dt_object *dt_find_or_create(const struct lu_env *env,
                                    struct dt_device *dt,
                                    const struct lu_fid *fid,
                                    struct dt_object_format *dof,
                                    struct lu_attr *at)
{
        struct dt_object *dto;
        struct thandle *th;
        int rc;

        ENTRY;

        dto = dt_locate(env, dt, fid);
        if (IS_ERR(dto))
                RETURN(dto);

        LASSERT(dto != NULL);
        if (dt_object_exists(dto))
                RETURN(dto);

        th = dt_trans_create(env, dt);
        if (IS_ERR(th))
                GOTO(out, rc = PTR_ERR(th));

        rc = dt_declare_create(env, dto, at, NULL, dof, th);
        if (rc)
                GOTO(trans_stop, rc);

        rc = dt_trans_start_local(env, dt, th);
        if (rc)
                GOTO(trans_stop, rc);

        dt_write_lock(env, dto, 0);
        if (dt_object_exists(dto))
                GOTO(unlock, rc = 0);

        CDEBUG(D_OTHER, "create new object "DFID"\n", PFID(fid));

        rc = dt_create(env, dto, at, NULL, dof, th);
        if (rc)
                GOTO(unlock, rc);
        LASSERT(dt_object_exists(dto));
unlock:
        dt_write_unlock(env, dto);
trans_stop:
        dt_trans_stop(env, dt, th);
out:
        if (rc) {
                lu_object_put(env, &dto->do_lu);
                RETURN(ERR_PTR(rc));
        }
        RETURN(dto);
}
EXPORT_SYMBOL(dt_find_or_create);

/* dt class init function. */
int dt_global_init(void)
{
        int result;

        LU_CONTEXT_KEY_INIT(&dt_key);
        result = lu_context_key_register(&dt_key);
        return result;
}

void dt_global_fini(void)
{
        lu_context_key_degister(&dt_key);
}

/**
 * Generic read helper. May return an error for partial reads.
 *
 * \param env  lustre environment
 * \param dt   object to be read
 * \param buf  lu_buf to be filled, with buffer pointer and length
 * \param pos position to start reading, updated as data is read
 *
 * \retval real size of data read
 * \retval -ve errno on failure
 */
int dt_read(const struct lu_env *env, struct dt_object *dt,
            struct lu_buf *buf, loff_t *pos)
{
        LASSERTF(dt != NULL, "dt is NULL when we want to read record\n");
        return dt->do_body_ops->dbo_read(env, dt, buf, pos, BYPASS_CAPA);
}
EXPORT_SYMBOL(dt_read);

/**
 * Read structures of fixed size from storage.  Unlike dt_read(), using
 * dt_record_read() will return an error for partial reads.
 *
 * \param env  lustre environment
 * \param dt   object to be read
 * \param buf  lu_buf to be filled, with buffer pointer and length
 * \param pos position to start reading, updated as data is read
 *
 * \retval 0 on successfully reading full buffer
 * \retval -EFAULT on short read
 * \retval -ve errno on failure
 */
int dt_record_read(const struct lu_env *env, struct dt_object *dt,
                   struct lu_buf *buf, loff_t *pos)
{
        int rc;

        LASSERTF(dt != NULL, "dt is NULL when we want to read record\n");

        rc = dt->do_body_ops->dbo_read(env, dt, buf, pos, BYPASS_CAPA);

        if (rc == buf->lb_len)
                rc = 0;
        else if (rc >= 0)
                rc = -EFAULT;
        return rc;
}
EXPORT_SYMBOL(dt_record_read);

int dt_record_write(const struct lu_env *env, struct dt_object *dt,
                    const struct lu_buf *buf, loff_t *pos, struct thandle *th)
{
        int rc;

        LASSERTF(dt != NULL, "dt is NULL when we want to write record\n");
        LASSERT(th != NULL);
        LASSERT(dt->do_body_ops);
        LASSERT(dt->do_body_ops->dbo_write);
        rc = dt->do_body_ops->dbo_write(env, dt, buf, pos, th, BYPASS_CAPA, 1);
        if (rc == buf->lb_len)
                rc = 0;
        else if (rc >= 0)
                rc = -EFAULT;
        return rc;
}
EXPORT_SYMBOL(dt_record_write);

int dt_declare_version_set(const struct lu_env *env, struct dt_object *o,
                           struct thandle *th)
{
        struct lu_buf vbuf;
        char *xname = XATTR_NAME_VERSION;

        LASSERT(o);
        vbuf.lb_buf = NULL;
        vbuf.lb_len = sizeof(dt_obj_version_t);
        return dt_declare_xattr_set(env, o, &vbuf, xname, 0, th);

}
EXPORT_SYMBOL(dt_declare_version_set);

void dt_version_set(const struct lu_env *env, struct dt_object *o,
                    dt_obj_version_t version, struct thandle *th)
{
        struct lu_buf vbuf;
        char *xname = XATTR_NAME_VERSION;
        int rc;

        LASSERT(o);
        vbuf.lb_buf = &version;
        vbuf.lb_len = sizeof(version);

        rc = dt_xattr_set(env, o, &vbuf, xname, 0, th, BYPASS_CAPA);
        if (rc < 0)
                CDEBUG(D_INODE, "Can't set version, rc %d\n", rc);
        return;
}
EXPORT_SYMBOL(dt_version_set);

dt_obj_version_t dt_version_get(const struct lu_env *env, struct dt_object *o)
{
        struct lu_buf vbuf;
        char *xname = XATTR_NAME_VERSION;
        dt_obj_version_t version;
        int rc;

        LASSERT(o);
        vbuf.lb_buf = &version;
        vbuf.lb_len = sizeof(version);
        rc = dt_xattr_get(env, o, &vbuf, xname, BYPASS_CAPA);
        if (rc != sizeof(version)) {
                CDEBUG(D_INODE, "Can't get version, rc %d\n", rc);
                version = 0;
        }
        return version;
}
EXPORT_SYMBOL(dt_version_get);

/*
 * fill one 4KB container with key/record read from the index file
 */
static int dt_index_container_build(const struct lu_env *env,
				    struct idx_container *ic,
				    const struct dt_it_ops *iops,
				    struct dt_it *it, __u32 attr,
				    struct idx_info *ii)
{
	char	*entry;
	int	 rc, nob = LU_PAGE_SIZE;
	ENTRY;

	/* no support for variable key & record size for now */
	LASSERT((ii->ii_flags & II_FL_VARKEY) == 0);
	LASSERT((ii->ii_flags & II_FL_VARREC) == 0);

	/* initialize the header of the container */
	memset(ic, 0, IDX_CT_HDR_SIZE);
	ic->ic_magic = IDX_CT_MAGIC;
	nob -= IDX_CT_HDR_SIZE;

	entry = ic->ic_entries;
	do {
		char		*tmp_entry = entry;
		struct dt_key	*key;
		__u64		 hash;
		int		 size;

		size = ii->ii_recsize + ii->ii_keysize + sizeof(__u64);
		if (nob < size) {
			if (ic->ic_nr == 0)
				GOTO(out, rc = -EINVAL);
			GOTO(out, rc = 0);
		}

		/* copy the 64-bit hash value first */
		hash = iops->store(env, it);
		memcpy(tmp_entry, &hash, sizeof(hash));
		tmp_entry += sizeof(hash);

		/* then the key value */
		LASSERT(iops->key_size(env, it) == ii->ii_keysize);
		key = iops->key(env, it);
		memcpy(tmp_entry, key, ii->ii_keysize);
		tmp_entry += ii->ii_keysize;

		/* and finally the record */
		rc = iops->rec(env, it, (struct dt_rec *)tmp_entry, attr);
		if (rc == -ESTALE)
			GOTO(next, rc);
		if (rc != 0)
			GOTO(out, rc);

		/* hash/key/record successfully copied! */
		ic->ic_nr++;
		ii->ii_hash_end = hash;
		if (unlikely(ic->ic_nr == 1 && ii->ii_count == 0))
			ii->ii_hash_start = hash;
		entry = tmp_entry + ii->ii_recsize;
		nob -= size;
next:
		rc = iops->next(env, it);
		if (rc == -ESTALE)
			GOTO(next, rc);
	} while (rc == 0);

	GOTO(out, rc);
out:
	if (rc >= 0 && ic->ic_nr > 0)
		/* one more container */
		ii->ii_count++;
	return rc;
}

/*
 * Walk index and fill containers with key/record pairs
 */
int dt_index_walk(const struct lu_env *env, struct dt_object *obj,
		  struct lu_rdpg *rdpg, struct idx_info *ii)
{
	struct dt_it		*it;
	const struct dt_it_ops	*iops;
	int			 rc = 0, i;
	unsigned int		 nr;
	ENTRY;

	LASSERT(rdpg->rp_pages != NULL);
	LASSERT(obj->do_index_ops != NULL);

	/* iterate through index and fill containers from @rdpg */
	iops = &obj->do_index_ops->dio_it;
	it = iops->init(env, obj, rdpg->rp_attrs, BYPASS_CAPA);
	if (IS_ERR(it))
		RETURN(PTR_ERR(it));

	rc = iops->load(env, it, rdpg->rp_hash);
	if (rc == 0) {
		/*
		 * Iterator didn't find record with exactly the key requested.
		 *
		 * It is currently either
		 *
		 *     - positioned above record with key less than
		 *     requested---skip it.
		 *     - or not positioned at all (is in IAM_IT_SKEWED
		 *     state)---position it on the next item.
		 */
		rc = iops->next(env, it);
	} else if (rc > 0)
		rc = 0;

	nr = rdpg->rp_count >> LU_PAGE_SHIFT;

	/* At this point and across for-loop:
	 *
	 *  rc == 0 -> ok, proceed.
	 *  rc >  0 -> end of directory.
	 *  rc <  0 -> error. */
	for (i = 0; rc == 0 && nr > 0; i++) {
		struct idx_container	*ic;
		int			 j;

		LASSERT(i < rdpg->rp_npages);
		ic = cfs_kmap(rdpg->rp_pages[i]);
		for (j = 0; j < LU_PAGE_COUNT; j++, nr--) {
			rc = dt_index_container_build(env, ic, iops, it,
					rdpg->rp_attrs, ii);
			if (rc)
				break;
			ic = (struct idx_container *)((char *)ic +LU_PAGE_SIZE);
		}
		cfs_kunmap(rdpg->rp_pages[i]);
	}

	if (rc > 0)
		/* no more entries */
		ii->ii_flags |= II_FL_LAST;
	if (rc >= 0)
		rc = ii->ii_count;

	iops->put(env, it);
	iops->fini(env, it);

	RETURN(rc);
}

/**
 * Walk key/record pairs of an index and copy them into 4KB containers to be
 * transferred over the network. This is used by OBD_IDX_READ RPC.
 *
 * On success, return the number of containers filled
 * Appropriate error otherwise.
 */
int dt_index_read(const struct lu_env *env, struct dt_device *dev,
                  struct idx_info *ii, struct lu_rdpg *rdpg)
{
	const struct dt_index_features	*feat;
	struct dt_object		*obj;
	int				 rc;
	ENTRY;

	/* rp_count shouldn't be null and should be a multiple of the container
	 * size */
	if (rdpg->rp_count <= 0 && (rdpg->rp_count & (LU_PAGE_SIZE - 1)) != 0)
		RETURN(-EFAULT);

	if (fid_seq(&ii->ii_fid) < FID_SEQ_SPECIAL)
		/* block access to local files for now */
		RETURN(-EPERM);

	if (fid_seq(&ii->ii_fid) >= FID_SEQ_NORMAL)
		/* we don't support directory transfer for the time being */
		RETURN(-EOPNOTSUPP);

	/* lookup index object subject to the transfer */
	obj = dt_locate(env, dev, &ii->ii_fid);
	if (IS_ERR(obj))
		RETURN(PTR_ERR(obj));
	if (dt_object_exists(obj) == 0)
		GOTO(out, rc = -ENOENT);

	/* fetch index features associated with index object */
	feat = dt_index_feat_select(fid_seq(&ii->ii_fid),
				    lu_object_attr(&obj->do_lu));
	if (IS_ERR(feat))
		GOTO(out, rc = PTR_ERR(feat));

	/* load index feature if not done already */
	if (obj->do_index_ops == NULL) {
		rc = obj->do_ops->do_index_try(env, obj, feat);
		if (rc)
			GOTO(out, rc);
	}

	/* fill ii_flags with supported index features */
	ii->ii_flags = 0;

	ii->ii_keysize = feat->dif_keysize_max;
	if (feat->dif_flags & DT_IND_VARKEY) {
		/* key size is variable */
		ii->ii_flags |= II_FL_VARKEY;
		/* we don't support variable key size for the time being */
		GOTO(out, rc = -EOPNOTSUPP);
	}

	ii->ii_recsize = feat->dif_recsize_max;
	if (feat->dif_flags & DT_IND_VARREC) {
		/* record size is variable */
		ii->ii_flags |= II_FL_VARREC;
		/* we don't support variable record size for the time being */
		GOTO(out, rc = -EOPNOTSUPP);
	}

	if (feat->dif_flags & DT_IND_NONUNQ)
		/* key isn't necessarily unique */
		ii->ii_flags |= II_FL_NONUNQ;

	dt_read_lock(env, obj, 0);
	/* fetch object version before walking the index */
	ii->ii_version = dt_version_get(env, obj);

	/* walk the index and fill containers with key/record pairs */
	rc = dt_index_walk(env, obj, rdpg, ii);
	dt_read_unlock(env, obj);

	GOTO(out, rc);
out:
	lu_object_put(env, &obj->do_lu);
	return rc;
}
EXPORT_SYMBOL(dt_index_read);

/* list of all supported index types */

/* directories */
const struct dt_index_features dt_directory_features;
EXPORT_SYMBOL(dt_directory_features);

/* scrub iterator */
const struct dt_index_features dt_otable_features;
EXPORT_SYMBOL(dt_otable_features);

/* accounting indexes */
const struct dt_index_features dt_acct_features = {
	.dif_flags		= DT_IND_UPDATE,
	.dif_keysize_min	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_keysize_max	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_recsize_min	= sizeof(struct lquota_acct_rec), /* 16 bytes */
	.dif_recsize_max	= sizeof(struct lquota_acct_rec), /* 16 bytes */
	.dif_ptrsize		= 4
};
EXPORT_SYMBOL(dt_acct_features);

/* global quota files */
const struct dt_index_features dt_quota_glb_features = {
	.dif_flags		= DT_IND_UPDATE,
	/* a different key would have to be used for per-directory quota */
	.dif_keysize_min	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_keysize_max	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_recsize_min	= sizeof(struct lquota_glb_rec), /* 32 bytes */
	.dif_recsize_max	= sizeof(struct lquota_glb_rec), /* 32 bytes */
	.dif_ptrsize		= 4
};
EXPORT_SYMBOL(dt_quota_glb_features);

/* slave quota files */
const struct dt_index_features dt_quota_slv_features = {
	.dif_flags		= DT_IND_UPDATE,
	/* a different key would have to be used for per-directory quota */
	.dif_keysize_min	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_keysize_max	= sizeof(__u64), /* 64-bit uid/gid */
	.dif_recsize_min	= sizeof(struct lquota_slv_rec), /* 8 bytes */
	.dif_recsize_max	= sizeof(struct lquota_slv_rec), /* 8 bytes */
	.dif_ptrsize		= 4
};
EXPORT_SYMBOL(dt_quota_slv_features);

/* helper function returning what dt_index_features structure should be used
 * based on the FID sequence. This is used by OBD_IDX_READ RPC */
static inline const struct dt_index_features *dt_index_feat_select(__u64 seq,
								   __u32 mode)
{
	if (seq == FID_SEQ_QUOTA_GLB) {
		/* global quota index */
		if (!S_ISREG(mode))
			/* global quota index should be a regular file */
			return ERR_PTR(-ENOENT);
		return &dt_quota_glb_features;
	} else if (seq == FID_SEQ_QUOTA) {
		/* quota slave index */
		if (!S_ISREG(mode))
			/* slave index should be a regular file */
			return ERR_PTR(-ENOENT);
		return &dt_quota_slv_features;
	} else if (seq >= FID_SEQ_NORMAL) {
		/* object is part of the namespace, verify that it is a
		 * directory */
		if (!S_ISDIR(mode))
			/* sorry, we can only deal with directory */
			return ERR_PTR(-ENOTDIR);
		return &dt_directory_features;
	}

	return ERR_PTR(-EOPNOTSUPP);
}
