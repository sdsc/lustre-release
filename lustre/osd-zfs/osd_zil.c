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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osd-zfs/osd_zil.c
 *
 * Author: Alex Zhuravlev <alexey.zhuravlev@intel.com>
 */

#define DEBUG_SUBSYSTEM S_OSD

#include <lustre_ver.h>
#include <libcfs/libcfs.h>
#include <obd_support.h>
#include <lustre_net.h>
#include <obd.h>
#include <obd_class.h>
#include <lustre_disk.h>
#include <lustre_fid.h>

#include "osd_internal.h"

#include <sys/dnode.h>
#include <sys/dbuf.h>
#include <sys/spa.h>
#include <sys/stat.h>
#include <sys/zap.h>
#include <sys/spa_impl.h>
#include <sys/zfs_znode.h>
#include <sys/dmu_tx.h>
#include <sys/dmu_objset.h>
#include <sys/dsl_prop.h>
#include <sys/dsl_dataset.h>
#include <sys/dsl_destroy.h>
#include <sys/sa_impl.h>
#include <sys/txg.h>

/*
 * Concerns:
 *  - not enough memory to hold all the structures (objects, updates)
 *  - not enough memory to run many concurrent transactions
 */

static cfs_hash_ops_t zil_object_hash_ops;

struct zil_device {
	struct dt_device	 zl_top_dev;
	/* how many handle's reference this local storage */
	atomic_t		 zl_refcount;
	/* underlaying OSD device */
	struct dt_device	*zl_osd;
};

struct zil_tx {
	struct list_head	 zt_updates;
	struct list_head	 zt_list;
	__u64			 zt_seq; /* ZIL sequence */
};

struct zil_object_state {
	struct hlist_node	zos_list;
	struct lu_fid		zos_fid;
	__u64			zos_version;
};

struct zil_object {
	struct lu_object_header	 zo_header;
	struct dt_object	 zo_obj;
};

struct zil_update {
	struct zil_tx		*zu_tx;	 /* update's transaction */
	struct list_head	 zu_list; /* all updates of tx */
	/* XXX: it's better to replace zu_obj with fid and an index
	 *	so we won't need to have all the objects (including
	 *	dbufs) in memory */
	struct lu_fid		 zu_fid;
	struct dt_object	*zu_obj;
	struct zil_object_state	*zu_zos;
	__u64			 zu_version;
	struct object_update	*zu_update;
	int			 zu_update_size;
};


struct osd_zil_cbdata;
static struct dt_object *zil_locate(const struct lu_env *env,
				    struct osd_zil_cbdata *cb,
				    const struct lu_fid *fid);


static void osd_zil_get_done(zgd_t *zgd, int error)
{
	struct osd_object *obj = zgd->zgd_private;

	if (zgd->zgd_rl != NULL)
		osd_unlock_range(obj, (struct osd_range_lock *)zgd->zgd_rl);
	if (zgd->zgd_db)
		dmu_buf_rele(zgd->zgd_db, zgd);

	if (error == 0 && zgd->zgd_bp)
		zil_add_block(zgd->zgd_zilog, zgd->zgd_bp);

	LASSERT(atomic_read(&obj->oo_zil_in_progress) > 0);
	atomic_dec(&obj->oo_zil_in_progress);
	wake_up(&obj->oo_bitlock_wait);

	OBD_FREE_PTR(zgd);
}

/*
 * Get data to generate a TX_WRITE intent log record.
 */
int osd_zil_get_data(void *arg, lr_write_t *lr, char *buf, zio_t *zio)
{
	struct osd_object *obj = arg;
	struct osd_device *osd = osd_obj2dev(obj);
	objset_t	  *os = osd->od_os;
	uint64_t	   object = lr->lr_foid;
	uint64_t	   offset = lr->lr_offset;
	uint64_t	    size = lr->lr_length;
	blkptr_t	   *bp = &lr->lr_blkptr;
	dmu_buf_t	   *db;
	zgd_t		   *zgd;
	int		    rc;

	ASSERT(zio != NULL);
	ASSERT(size != 0);

	CDEBUG(D_HA, "get data for %p/%llu: %llu/%u\n",
		obj, object, offset, (int)size);

	atomic_inc(&obj->oo_zil_in_progress);
	if (lu_object_is_dying(obj->oo_dt.do_lu.lo_header)) {
		LASSERT(atomic_read(&obj->oo_zil_in_progress) > 0);
		atomic_dec(&obj->oo_zil_in_progress);
		wake_up(&obj->oo_bitlock_wait);
		return (SET_ERROR(ENOENT));
	}

	OBD_ALLOC_PTR(zgd);
	if (unlikely(zgd == NULL))
		return SET_ERROR(ENOMEM);
	zgd->zgd_zilog = osd->od_zilog;

	/* XXX: missing locking, see zfs_write() for the details
	 *	basically we have to protect data from modifications
	 *	as it's still a part of running txg */

	/*
	 * Write records come in two flavors: immediate and indirect.
	 * For small writes it's cheaper to store the data with the
	 * log record (immediate); for large writes it's cheaper to
	 * sync the data and get a pointer to it (indirect) so that
	 * we don't have to write the data twice.
	 */
	if (buf != NULL) { /* immediate write */
		rc = dmu_read(os, object, offset, size, buf,
			      DMU_READ_NO_PREFETCH);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_COPIED, 1);
	} else {
		struct osd_range_lock *l;

		l = osd_lock_range(obj, offset, size, 0);
		if (IS_ERR(l))
			GOTO(out, rc = PTR_ERR(l));
		zgd->zgd_rl = (void *)l;

		offset = P2ALIGN_TYPED(offset, size, uint64_t);
		rc = dmu_buf_hold(os, object, offset, zgd, &db,
				  DMU_READ_NO_PREFETCH);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_INDIRECT, 1);
		if (rc == 0) {
			blkptr_t *obp = dmu_buf_get_blkptr(db);
			if (obp) {
				ASSERT(BP_IS_HOLE(bp));
				*bp = *obp;
			}

			zgd->zgd_db = db;
			zgd->zgd_bp = &lr->lr_blkptr;
			zgd->zgd_private = obj;

			ASSERT(db != NULL);
			ASSERT(db->db_offset == offset);
			ASSERT(db->db_size == size);

			rc = dmu_sync(zio, lr->lr_common.lrc_txg,
				      osd_zil_get_done, zgd);
		}
	}

	if (unlikely(rc != 0))
		osd_zil_get_done(zgd, rc);

	return SET_ERROR(rc);
}

static void osd_zil_write_commit_cb(void *arg)
{
	itx_t *itx = (itx_t *)arg;
	struct osd_object *obj = itx->itx_private;
	struct osd_device *osd = osd_obj2dev(obj);
	atomic_dec(&osd->od_recs_to_write);
	lu_object_put(NULL, &obj->oo_dt.do_lu);
}

/*
 * We store data in the log buffers if it's small enough.
 * Otherwise we will later flush the data out via dmu_sync().
 */
ssize_t zvol_immediate_write_sz = 32768;

void osd_zil_log_write(const struct lu_env *env, struct osd_thandle *oh,
		       struct dt_object *dt, uint64_t offset, uint64_t size)
{
	struct osd_object  *obj  = osd_dt_obj(dt);
	struct osd_device  *osd = osd_obj2dev(obj);
	zilog_t *zilog = osd->od_zilog;
	ssize_t immediate_write_sz;
	dnode_t		*dn;
	int		blocksize;

	if (osd_use_zil(oh, 0) == 0)
		return;

	if (zil_replaying(zilog, oh->ot_tx))
		return;

	DB_DNODE_ENTER((dmu_buf_impl_t *)obj->oo_db);
	dn = DB_DNODE((dmu_buf_impl_t *)obj->oo_db);
	blocksize = dn->dn_datablksz;
	DB_DNODE_EXIT((dmu_buf_impl_t *)obj->oo_db);

	immediate_write_sz = (zilog->zl_logbias == ZFS_LOGBIAS_THROUGHPUT)
		? 0 : zvol_immediate_write_sz;

	while (size) {
		itx_t *itx;
		lr_write_t *lr;
		ssize_t len;
		itx_wr_state_t write_state;

		if (blocksize > immediate_write_sz &&
		    size >= blocksize && offset % blocksize == 0) {
			write_state = WR_INDIRECT; /* uses dmu_sync */
			len = blocksize;
		} else if (1) {
			write_state = WR_COPIED;
			len = MIN(ZIL_MAX_LOG_DATA, size);
		} else {
			write_state = WR_NEED_COPY;
			len = MIN(ZIL_MAX_LOG_DATA, size);
		}

		itx = zil_itx_create(TX_WRITE, sizeof(*lr) +
		    (write_state == WR_COPIED ? len : 0));
		lr = (lr_write_t *)&itx->itx_lr;
		if (write_state == WR_COPIED && dmu_read(osd->od_os,
		    obj->oo_db->db_object, offset, len, lr+1,
		    DMU_READ_NO_PREFETCH) != 0) {
			zil_itx_destroy(itx);
			itx = zil_itx_create(TX_WRITE, sizeof(*lr));
			lr = (lr_write_t *)&itx->itx_lr;
			write_state = WR_NEED_COPY;
		}

		itx->itx_wr_state = write_state;
		if (write_state == WR_NEED_COPY)
			itx->itx_sod += len;
		lr->lr_foid = obj->oo_db->db_object;
		lr->lr_offset = offset;
		lr->lr_length = len;
		lr->lr_blkoff = 0;
		BP_ZERO(&lr->lr_blkptr);

		itx->itx_callback = osd_zil_write_commit_cb;
		itx->itx_callback_data = itx;

		lu_object_get(&obj->oo_dt.do_lu);
		itx->itx_private = obj;
		itx->itx_sync = 1;

		if (itx->itx_wr_state == WR_COPIED)
			lprocfs_counter_add(osd->od_stats,
					    LPROC_OSD_ZIL_COPIED, 1);

		CDEBUG(D_CACHE,
		       "new zil %p in txg %llu: %u/%u type %u to %llu\n",
		       itx, oh->ot_tx->tx_txg, (unsigned)offset, (unsigned)len,
		       (unsigned) write_state, obj->oo_db->db_object);

		atomic_inc(&osd->od_recs_to_write);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_RECORDS,
				    itx->itx_lr.lrc_reclen);

		(void) zil_itx_assign(zilog, itx, oh->ot_tx);

		offset += len;
		size -= len;
	}
}

static int osd_zil_replay_err(struct osd_device *o, lr_t *lr, boolean_t s)
{
	return ENOTSUP;
}

void byteswap_uint64_array(void *vbuf, size_t size)
{
	uint64_t *buf = vbuf;
	size_t count = size >> 3;
	int i;

	ASSERT((size & 7) == 0);

	for (i = 0; i < count; i++)
		buf[i] = BSWAP_64(buf[i]);
}

struct osd_zil_cbdata {
	const struct lu_env	*env;
	struct osd_device	*osd;
	struct zil_device	*zil_dev;
	cfs_hash_t		*hash;
	struct list_head	 zil_txs;
	int			 zil_updates;
	int			 result;
	int			 barriers;
	int			 tx_nr;
	int			 update_nr;
	int			 discarded;
	int			 iterations;
};

struct lr_update {
	lr_t	lr_common;
	uint64_t lr_oid;
};


typedef int (*osd_replay_update)(const struct lu_env *env,
				 struct dt_object *o,
				 struct object_update *u,
				 struct osd_thandle *oh);

#define CHECK_AND_MOVE(ATTRIBUTE, FIELD)			\
do {								\
	if (attr->la_valid & ATTRIBUTE) {			\
		memcpy(&attr->FIELD, ptr, sizeof(attr->FIELD));	\
		ptr += sizeof(attr->FIELD);			\
	}							\
} while (0)
static void osd_unpack_lu_attr(struct lu_attr *attr, void *ptr)
{
	memset(attr, 0, sizeof(*attr));
	memcpy(&attr->la_valid, ptr, sizeof(attr->la_valid));
	ptr += sizeof(attr->la_valid);

	CHECK_AND_MOVE(LA_SIZE, la_size);
	CHECK_AND_MOVE(LA_MTIME, la_mtime);
	CHECK_AND_MOVE(LA_ATIME, la_atime);
	CHECK_AND_MOVE(LA_CTIME, la_ctime);
	CHECK_AND_MOVE(LA_BLOCKS, la_blocks);
	CHECK_AND_MOVE((LA_MODE | LA_TYPE), la_mode);
	CHECK_AND_MOVE(LA_UID, la_uid);
	CHECK_AND_MOVE(LA_GID, la_gid);
	CHECK_AND_MOVE(LA_FLAGS, la_flags);
	CHECK_AND_MOVE(LA_NLINK, la_nlink);
	/*CHECK_AND_MOVE(LA_BLKBITS, la_blkbits);*/
	CHECK_AND_MOVE(LA_BLKSIZE, la_blksize);
	CHECK_AND_MOVE(LA_RDEV, la_rdev);
}
#undef CHECK_AND_MOVE

static int osd_replay_create_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	void			*ptr;
	struct lu_attr		 la;
	struct dt_object_format	 dof;
	int			 rc;
	size_t			 size;

	ptr = object_update_param_get(u, 0, &size);
	LASSERT(ptr != NULL && size >= 0);
	osd_unpack_lu_attr(&la, ptr);

	dof.dof_type = dt_mode_to_dft(la.la_mode);

	/* XXX: hint?* */

	if (oh->ot_assigned == 0)
		rc = dt_declare_create(env, o, &la, NULL, &dof, &oh->ot_super);
	else
		rc = dt_create(env, o, &la, NULL, &dof, &oh->ot_super);

	return rc;
}

static int osd_replay_destroy_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_destroy(env, o, &oh->ot_super);
	else
		rc = dt_destroy(env, o, &oh->ot_super);

	return rc;
}

static int osd_replay_ref_add_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_ref_add(env, o, &oh->ot_super);
	else
		rc = dt_ref_add(env, o, &oh->ot_super);
	return rc;
}

static int osd_replay_ref_del_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	int rc;
	if (oh->ot_assigned == 0)
		rc = dt_declare_ref_del(env, o, &oh->ot_super);
	else
		rc = dt_ref_del(env, o, &oh->ot_super);
	return rc;
}

static int osd_replay_attr_set_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	void			*ptr;
	struct lu_attr		 attr;
	int			 rc;
	size_t			 size;

	ptr = object_update_param_get(u, 0, &size);
	LASSERT(ptr != NULL && size >= 8);
	osd_unpack_lu_attr(&attr, ptr);

	if (oh->ot_assigned == 0)
		rc = dt_declare_attr_set(env, o, &attr, &oh->ot_super);
	else
		rc = dt_attr_set(env, o, &attr, &oh->ot_super);

	return rc;
}

static int osd_replay_xattr_set_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct lu_buf	 b;
	char		*name;
	int		 rc;
	size_t		 size = 0;
	int		 *fp, fl;

	name = object_update_param_get(u, 0, &size);
	if (size == 1) {
		if ((unsigned char)name[0] == 1)
			name = XATTR_NAME_LOV;
		else if ((unsigned char)name[0] == 2)
			name = XATTR_NAME_LINK;
		else if ((unsigned char)name[0] == 3)
			name = XATTR_NAME_VERSION;
		else
			LBUG();
	}
	LASSERT(name != NULL);

	b.lb_buf = object_update_param_get(u, 1, &size);
	LASSERT(b.lb_buf != NULL && size > 0);
	b.lb_len = size;

	fp = object_update_param_get(u, 2, &size);
	fl = 0;
	if (fp != NULL)
		fl = *fp;

	if (oh->ot_assigned == 0)
		rc = dt_declare_xattr_set(env, o, &b, name, fl, &oh->ot_super);
	else
		rc = dt_xattr_set(env, o, &b, name, fl, &oh->ot_super);
	return rc;
}

static int osd_replay_xattr_del_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	char		*name;
	int		 rc;

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	if (oh->ot_assigned == 0)
		rc = dt_declare_xattr_del(env, o, name, &oh->ot_super);
	else
		rc = dt_xattr_del(env, o, name, &oh->ot_super);

	return rc;
}

static int osd_replay_insert_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct dt_insert_rec rec;
	struct lu_fid *fid;
	size_t		size;
	__u32		*ptype;
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0) {
		rc = -ENOTDIR;
		return rc;
	}

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	fid = object_update_param_get(u, 1, &size);
	LASSERT(fid != NULL && size == sizeof(*fid));

	ptype = object_update_param_get(u, 2, &size);
	LASSERT(ptype != NULL && size == sizeof(*ptype));

	rec.rec_fid = fid;
	rec.rec_type = *ptype;

	if (oh->ot_assigned == 0)
		rc = dt_declare_insert(env, o, (struct dt_rec *)&rec,
					(struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_insert(env, o, (struct dt_rec *)&rec,
				(struct dt_key *)name,
				&oh->ot_super, 1);

	return rc;
}

static int osd_replay_delete_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	char *name;
	int rc;

	if (dt_try_as_dir(env, o) == 0) {
		rc = -ENOTDIR;
		return rc;
	}

	name = object_update_param_get(u, 0, NULL);
	LASSERT(name != NULL);

	if (oh->ot_assigned == 0)
		rc = dt_declare_delete(env, o, (struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_delete(env, o, (struct dt_key *)name,
				&oh->ot_super);

	return rc;
}

static int osd_replay_write_update(const struct lu_env *env,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct lu_buf lb;
	__u64 *tmp;
	__u64 pos;
	size_t size;
	int rc;

	lb.lb_buf = object_update_param_get(u, 0, &size);
	LASSERT(lb.lb_buf != NULL && size > 0);
	lb.lb_len = size;

	tmp = object_update_param_get(u, 1, &size);
	LASSERT(tmp != NULL && size == 8);
	pos = *tmp;

	if (oh->ot_assigned == 0)
		rc = dt_declare_write(env, o, &lb, pos, &oh->ot_super);
	else {
		rc = dt_write(env, o, &lb, &pos, &oh->ot_super, 0);
		if (rc == lb.lb_len)
			rc = 0;
		else if (rc >= 0)
			rc = -EFAULT;
	}

	return rc;
}

static osd_replay_update osd_replay_update_vec[] = {
	[OUT_CREATE]	= osd_replay_create_update,
	[OUT_DESTROY]	= osd_replay_destroy_update,
	[OUT_REF_ADD]	= osd_replay_ref_add_update,
	[OUT_REF_DEL]	= osd_replay_ref_del_update,
	[OUT_ATTR_SET]	= osd_replay_attr_set_update,
	[OUT_XATTR_SET]	= osd_replay_xattr_set_update,
	[OUT_XATTR_DEL]	= osd_replay_xattr_del_update,
	[OUT_INDEX_INSERT]	= osd_replay_insert_update,
	[OUT_INDEX_DELETE]	= osd_replay_delete_update,
	[OUT_WRITE]		= osd_replay_write_update,
};

struct zil_object_state *
osd_zil_find_or_create_state(cfs_hash_t *h, const struct lu_fid *fid)
{
	struct zil_object_state *zos;

	zos = cfs_hash_lookup(h, fid);
	if (zos == NULL) {
		OBD_ALLOC_PTR(zos);
		LASSERT(zos != NULL);
		zos->zos_fid = *fid;
		zos->zos_version = -1ULL;
		cfs_hash_add(h, fid, &zos->zos_list);
	}
	return zos;
}

static int osd_zil_release_state(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				 struct hlist_node *hnode, void *data)
{
	struct zil_object_state *zos;

	zos = hlist_entry(hnode, struct zil_object_state, zos_list);
	cfs_hash_bd_del_locked(hs, bd, hnode);
	OBD_FREE_PTR(zos);

	return 0;
}

/*
 * make a copy of the record to memory
 */
static int osd_zil_replay_update(struct osd_zil_cbdata *cb,
				 struct lr_update *lr,
				 boolean_t byteswap)
{
	struct object_update_request	*ureq = (void *)(lr + 1);
	struct zil_tx			*ztx;
	struct zil_update		*zup;
	int				 i;

	if (byteswap && 0)
		byteswap_uint64_array(lr, sizeof(*lr));

	LASSERT(ureq->ourq_magic == UPDATE_REQUEST_MAGIC);
	CDEBUG(D_HA, "REPLAY %d updates\n", ureq->ourq_count);

	OBD_ALLOC_PTR(ztx);
	if (unlikely(ztx == NULL)) {
		cb->result = -ENOMEM;
		RETURN(cb->result);
	}
	INIT_LIST_HEAD(&ztx->zt_updates);
	list_add_tail(&ztx->zt_list, &cb->zil_txs);

	for (i = 0; i < ureq->ourq_count; i++) {
		struct object_update	*update;
		struct zil_object_state	*zos;
		size_t			 size;

		update = object_update_request_get(ureq, i, &size);
		LASSERT(update != NULL);

		OBD_ALLOC_PTR(zup);
		if (unlikely(zup == NULL)) {
			cb->result = -ENOMEM;
			RETURN(cb->result);
		}

		OBD_ALLOC(zup->zu_update, size);
		if (unlikely(zup->zu_update == NULL)) {
			cb->result = -ENOMEM;
			RETURN(cb->result);
		}

		memcpy(zup->zu_update, update, size);
		zup->zu_update_size = size;
		zup->zu_version = update->ou_batchid;
		zup->zu_fid = update->ou_fid;
		list_add_tail(&zup->zu_list, &ztx->zt_updates);

		/* store minimal (initial) version */
		zos = osd_zil_find_or_create_state(cb->hash, &zup->zu_fid);
		if (zos->zos_version > zup->zu_version)
			zos->zos_version = zup->zu_version;
		zup->zu_zos = zos;

	}

	return SET_ERROR(0);
}

static int osd_zil_replay_write(struct osd_zil_cbdata *cb, lr_write_t *lr,
				boolean_t byteswap)
{
	struct osd_device *osd = cb->osd;
	objset_t	*os = osd->od_os;
	char		*data = (char *)(lr + 1);
	uint64_t	 off = lr->lr_offset;
	uint64_t	 len = lr->lr_length;
	sa_handle_t	*sa;
	uint64_t	 end;
	dmu_tx_t	*tx;
	int		 error;

	CDEBUG(D_HA, "REPLAY %u/%u to %llu\n", (unsigned)off,
	       (unsigned)len, lr->lr_foid);

	if (byteswap)
		byteswap_uint64_array(lr, sizeof(*lr));

	error = sa_handle_get(os, lr->lr_foid, lr, SA_HDL_PRIVATE, &sa);
	if (error)
		return SET_ERROR(error);

restart:
	tx = dmu_tx_create(os);
	dmu_tx_hold_write(tx, lr->lr_foid, off, len);
	dmu_tx_hold_sa(tx, sa, 1);
	error = dmu_tx_assign(tx, TXG_WAIT);
	if (error) {
		if (error == ERESTART) {
			dmu_tx_wait(tx);
			dmu_tx_abort(tx);
			goto restart;
		}
		dmu_tx_abort(tx);
	} else {
		dmu_write(os, lr->lr_foid, off, len, data, tx);
		end = lr->lr_offset + lr->lr_length;
		sa_update(sa, SA_ZPL_SIZE(osd), (void *)&end,
			  sizeof(uint64_t), tx);
		/* Ensure the replayed seq is updated */
		(void) zil_replaying(osd->od_zilog, tx);
		dmu_tx_commit(tx);
	}

	sa_handle_destroy(sa);

	return SET_ERROR(error);
}

static int osd_zil_replay_create(struct osd_zil_cbdata *cb, lr_acl_v0_t *lr,
				boolean_t byteswap)
{
	/* used as noop, see osd_ro() */
	CWARN("CREATE replay - NOOP\n");
	return 0;
}

static int osd_zil_replay_tx(struct osd_zil_cbdata *cb, struct zil_tx *ztx)
{
	struct osd_device	*osd = cb->osd;
	struct dt_device	*dt = &osd->od_dt_dev;
	const struct lu_env	*env = cb->env;
	struct thandle		*th;
	struct osd_thandle	*oh;
	struct zil_update	*zup, *tmp;
	struct object_update	*u;
	int			 rc, rc2;
	ENTRY;

	LASSERT(list_empty(&ztx->zt_list) == 0);

	th = dt_trans_create(env, dt);
	if (unlikely(th == NULL))
		RETURN(-ENOMEM);

	oh = container_of0(th, struct osd_thandle, ot_super);

	list_for_each_entry(zup, &ztx->zt_updates, zu_list) {

		LASSERT(zup->zu_obj == NULL);
		zup->zu_obj = zil_locate(env, cb, &zup->zu_fid);
		if (IS_ERR(zup->zu_obj)) {
			rc = PTR_ERR(zup->zu_obj);
			zup->zu_obj = NULL;
			GOTO(out, rc);
		}

		u = zup->zu_update;
		LASSERT(u != NULL);

		rc = osd_replay_update_vec[u->ou_type](env, zup->zu_obj, u, oh);
		if (unlikely(rc < 0))
			GOTO(out, rc);
	}

	rc = dt_trans_start(env, dt, th);
	if (unlikely(rc < 0))
		GOTO(out, rc);
	oh = container_of0(th, struct osd_thandle, ot_super);

	list_for_each_entry_safe(zup, tmp, &ztx->zt_updates, zu_list) {
		if (zup->zu_version != zup->zu_zos->zos_version) {
			/* we have to wait for expected version */
			CDEBUG(D_OTHER, "tx %p "DFID" : %llu != %llu\n",
			       zup, PFID(&zup->zu_fid), zup->zu_version,
			       zup->zu_zos->zos_version);
			lu_object_put(env, &zup->zu_obj->do_lu);
			zup->zu_obj = NULL;
			continue;
		}

		LASSERT(zup->zu_obj != NULL);
		u = zup->zu_update;
		rc = osd_replay_update_vec[u->ou_type](env, zup->zu_obj, u, oh);
		if (unlikely(rc < 0))
			GOTO(out, rc);

		zup->zu_zos->zos_version++;

		lu_object_put(env, &zup->zu_obj->do_lu);

		list_del(&zup->zu_list);
		OBD_FREE(zup->zu_update, zup->zu_update_size);
		OBD_FREE_PTR(zup);

		cb->update_nr++;
	}

out:
	rc2 = dt_trans_stop(env, dt, th);

	RETURN(rc2 < 0 ? rc2 : rc);
}

static int osd_zil_replay_sync(struct osd_zil_cbdata *cb, lr_acl_v0_t *lr,
				boolean_t byteswap)
{
	struct zil_tx		*ztx, *tmp;
	int			 rc;

	/* this is the end of the virtual txg committing all the previous
	 * transactions is enough to make Lustre happy.
	 * given the records don't follow the original ordering,
	 * we use few iterations to apply the updates */

	cb->barriers++;

	while (!list_empty(&cb->zil_txs) && cb->result == 0) {
		cb->iterations++;
		list_for_each_entry_safe(ztx, tmp, &cb->zil_txs, zt_list) {

			rc = osd_zil_replay_tx(cb, ztx);
			if (rc < 0) {
				CERROR("%s: can't replay: rc = %d\n",
				       cb->osd->od_svname, rc);
				cb->result = rc;
				break;
			}

			if (list_empty(&ztx->zt_updates)) {
				cb->tx_nr++;
				list_del(&ztx->zt_list);
				OBD_FREE_PTR(ztx);
			}
		}
	}

	cfs_hash_for_each_safe(cb->hash, osd_zil_release_state, NULL);

	return cb->result;
}

zil_replay_func_t osd_zil_replay_vector[TX_MAX_TYPE] = {
	(zil_replay_func_t)osd_zil_replay_err,	/* no such transaction type */
	(zil_replay_func_t)osd_zil_replay_create,/* TX_CREATE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_MKDIR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_MKXATTR */
	(zil_replay_func_t)osd_zil_replay_update,/* TX_SYMLINK */
	(zil_replay_func_t)osd_zil_replay_sync,	/* TX_REMOVE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_RMDIR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_LINK */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_RENAME */
	(zil_replay_func_t)osd_zil_replay_write,	/* TX_WRITE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_TRUNCATE */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_SETATTR */
	(zil_replay_func_t)osd_zil_replay_err,	/* TX_ACL_V0 */
};

static int osd_zil_create_noop(struct osd_device *o)
{
	itx_t		*itx;
	dmu_tx_t	*tx;
	lr_create_t	*lr;
	int		 rc;

	tx = dmu_tx_create(o->od_os);
	rc = -dmu_tx_assign(tx, TXG_WAITED);
	if (rc != 0) {
		dmu_tx_abort(tx);
		return rc;
	}
	itx = zil_itx_create(TX_CREATE, sizeof(*lr));
	lr = (lr_create_t *)&itx->itx_lr;
	lr->lr_doid = 0;
	lr->lr_foid = 0;
	lr->lr_mode = 0;
	zil_itx_assign(o->od_zilog, itx, tx);
	dmu_tx_commit(tx);

	return 0;
}

void osd_zil_dump_stats(struct osd_device *o)
{
	long ave;
	int i, j;
	char *buf;

	return;

	if (atomic_read(&o->od_recs_in_log) == 0)
		return;

	ave = atomic_read(&o->od_bytes_in_log);
	ave /= atomic_read(&o->od_recs_in_log);
	CWARN("%s: %ld bytes/rec ave\n", o->od_svname, ave);
	OBD_ALLOC(buf, 512);
	for (i = 0, j = 0; i < OUT_LAST; i++) {
		if (atomic_read(&o->od_updates_by_type[i]) == 0)
			continue;
		j += snprintf(buf + j, 512 - j, "  #%d: %d(%d)", i,
			      atomic_read(&o->od_updates_by_type[i]),
			      (int)(atomic_read(&o->od_bytes_by_type[i]) /
				      atomic_read(&o->od_updates_by_type[i])));
	}
	CWARN("%s: %s\n", o->od_svname, buf);
	OBD_FREE(buf, 512);
}

void osd_zil_fini(struct osd_device *o)
{
	return;

	if (o->od_zilog == NULL)
		return;

	osd_zil_dump_stats(o);

	zil_close(o->od_zilog);
}

static void osd_zil_commit_cb(void *arg)
{
	itx_t *itx = (itx_t *)arg;
	struct osd_device *osd = itx->itx_private;
	atomic_dec(&osd->od_recs_to_write);

}

void osd_zil_make_itx(struct osd_thandle *oh)
{
	struct osd_device	*osd = osd_dt_dev(oh->ot_super.th_dev);
	itx_t			*itx;
	struct lr_update	*lr;
	int			 len;
	char			*buf;
#if 0
	int			 i, j;
#endif

	if (oh->ot_buf.ub_req == NULL)
		return;
	if (osd->od_zilog == NULL)
		goto out;
	if (unlikely(oh->ot_buf.ub_req->ourq_count == 0))
		goto out;

#if 0
	for (i = 0; i < oh->ot_buf.ub_req->ourq_count; i++) {
		struct object_update		*update;
		size_t				 size;
		int				 sum;

		update = object_update_request_get(oh->ot_buf.ub_req,
						   i, NULL);
		LASSERT(update != NULL);
		LASSERT(update->ou_type < OUT_LAST);
		atomic_inc(&osd->od_updates_by_type[update->ou_type]);

		sum = offsetof(struct object_update, ou_params[0]);
		for (j = 0; j < update->ou_params_count; j++) {
			size = 0;
			object_update_param_get(update, j, &size);
			sum += size;
		}
		atomic_add(sum, &osd->od_bytes_by_type[update->ou_type]);
		CDEBUG(D_OTHER, "op %d, size %d\n", update->ou_type, sum);
	}
#endif

	len = object_update_request_size(oh->ot_buf.ub_req);
	CDEBUG(D_OTHER, "buf %d\n", len);

	itx = zil_itx_create(TX_SYMLINK, sizeof(*lr) + len);
	if (itx == NULL) {
		CERROR("can't create itx with %d bytes\n", len);
		goto out;
	}

	lr = (struct lr_update *)&itx->itx_lr;
	lr->lr_oid = 1;
	buf = (char *)(lr + 1);
	memcpy(buf, oh->ot_buf.ub_req, len);

	atomic_inc(&osd->od_recs_in_log);
	atomic_add(len, &osd->od_bytes_in_log);

	itx->itx_callback = osd_zil_commit_cb;
	itx->itx_callback_data = itx;

	itx->itx_private = osd;
	itx->itx_sync = 1;

	atomic_inc(&osd->od_recs_to_write);
	lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_RECORDS,
			    itx->itx_lr.lrc_reclen);

	LASSERT(osd->od_zilog != NULL);
	(void) zil_itx_assign(osd->od_zilog, itx, oh->ot_tx);

out:
	OBD_FREE_LARGE(oh->ot_buf.ub_req, oh->ot_buf.ub_req_size);
}

struct osd_range_lock {
	struct list_head	orl_list;
	loff_t			orl_start;
	loff_t			orl_end;
	wait_queue_head_t	orl_wait;
	int			orl_rw:1,
				orl_granted:1;
};

void osd_range_head_alloc(struct osd_object *o)
{
	struct osd_range_head	*h;

	if (o->oo_range_head != NULL)
		return;

	OBD_ALLOC_PTR(h);
	if (unlikely(h == NULL))
		return;
	spin_lock_init(&h->ord_range_lock);
	INIT_LIST_HEAD(&h->ord_range_granted_list);
	INIT_LIST_HEAD(&h->ord_range_waiting_list);

	write_lock(&o->oo_attr_lock);
	if (o->oo_range_head == NULL) {
		o->oo_range_head = h;
		init_waitqueue_head(&o->oo_bitlock_wait);
	} else {
		OBD_FREE_PTR(h);
	}
	write_unlock(&o->oo_attr_lock);
}

static int osd_range_is_compatible(struct list_head *list,
				   struct osd_range_lock *l)
{
	struct osd_range_lock *t;

	list_for_each_entry(t, list, orl_list) {
		if (l->orl_start > t->orl_end || l->orl_end < t->orl_start) {
			/* no overlap */
			continue;
		}
		if (t->orl_rw == 0 && l->orl_rw == 0) {
			/* both locks are read locks */
			continue;
		}

		/* incompatible */
		return 0;
	}

	return 1;
}

struct osd_range_lock *osd_lock_range(struct osd_object *o, loff_t offset,
				      size_t size, int rw)
{
	struct osd_range_lock	*l;
	struct osd_range_head	*h;
	int			 compat;


	osd_range_head_alloc(o);
	h = o->oo_range_head;
	if (unlikely(h == NULL))
		RETURN(ERR_PTR(-ENOMEM));

	OBD_ALLOC_PTR(l);
	if (unlikely(l == NULL))
		RETURN(ERR_PTR(-ENOMEM));

	if (unlikely(size == 0))
		size = 1;

	CDEBUG(D_HA, "lock %p %llu-%llu on %p\n", l, offset, offset + size, o);

	l->orl_start = offset;
	l->orl_end = offset + size - 1;
	l->orl_rw = rw;
	init_waitqueue_head(&l->orl_wait);

	spin_lock(&h->ord_range_lock);

	compat = osd_range_is_compatible(&h->ord_range_granted_list, l);
	compat += osd_range_is_compatible(&h->ord_range_waiting_list, l);

	if (compat == 2) {
		list_add(&l->orl_list, &h->ord_range_granted_list);
		l->orl_granted = 1;
		h->ord_nr++;
		if (h->ord_nr > h->ord_max)
			h->ord_max = h->ord_nr;
		spin_unlock(&h->ord_range_lock);
	} else {
		list_add_tail(&l->orl_list, &h->ord_range_waiting_list);
		spin_unlock(&h->ord_range_lock);

		/* XXX: need a memory barrier? */
		wait_event(l->orl_wait, l->orl_granted != 0);
	}

	return l;
}

void osd_unlock_range(struct osd_object *o, struct osd_range_lock *l)
{
	struct osd_range_lock	*t, *tmp;
	struct osd_range_head	*h;
	int compat;

	LASSERT(!list_empty(&l->orl_list));
	LASSERT(l->orl_granted != 0);

	CDEBUG(D_HA, "unlock %p %llu-%llu on %p\n",
	       l, l->orl_start, l->orl_end, o);

	h = o->oo_range_head;
	LASSERT(h != NULL);

	spin_lock(&h->ord_range_lock);
	list_del(&l->orl_list);
	h->ord_nr--;
	/* our released lock can affect this waiting lock, let's try it */
	list_for_each_entry_safe(t, tmp, &h->ord_range_waiting_list, orl_list) {
		compat = osd_range_is_compatible(&h->ord_range_granted_list, t);
		if (compat != 0) {
			list_del(&t->orl_list);
			list_add(&t->orl_list, &h->ord_range_granted_list);
			t->orl_granted = 1;
			wake_up(&t->orl_wait);
		} else
			break;
	}
	spin_unlock(&h->ord_range_lock);

	OBD_FREE_PTR(l);
}

void osd_zil_commit(struct osd_device *osd)
{
	struct lr_update	*lr;
	dmu_tx_t		*tx;
	itx_t			*itx;
	int			 rc;

	if (atomic_read(&osd->od_recs_to_write) == 0)
		return;

	/* make sure all in-flight txg's are done */
	down_write(&osd->od_tx_barrier);

	/* put a special barrier record to ZIL */
	tx = dmu_tx_create(osd->od_os);
	LASSERT(tx != NULL);
	rc = -dmu_tx_assign(tx, TXG_WAIT);
	LASSERT(rc == 0);

	itx = zil_itx_create(TX_REMOVE, sizeof(*lr));
	LASSERT(itx != NULL);

	itx->itx_callback = osd_zil_commit_cb;
	itx->itx_callback_data = itx;

	itx->itx_private = osd;
	itx->itx_sync = 1;

	atomic_inc(&osd->od_recs_to_write);
	lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_RECORDS,
			    itx->itx_lr.lrc_reclen);

	LASSERT(osd->od_zilog != NULL);
	(void) zil_itx_assign(osd->od_zilog, itx, tx);

	dmu_tx_commit(tx);

	/* now we can enable new transactions */
	up_write(&osd->od_tx_barrier);

	zil_commit(osd->od_zilog, 0);
}

static struct lu_device_type_operations zil_device_type_ops = {
	.ldto_start = NULL,
	.ldto_stop  = NULL,
};

static struct lu_device_type zil_lu_type = {
	.ldt_name = "zil_device",
	.ldt_ops  = &zil_device_type_ops,
};

static inline struct zil_object *lu2zil_obj(struct lu_object *o)
{
	return container_of0(o, struct zil_object, zo_obj.do_lu);
}

static int zil_object_init(const struct lu_env *env, struct lu_object *o,
			  const struct lu_object_conf *unused)
{
	struct zil_device	*zd;
	struct lu_object	*below;
	struct lu_device	*under;

	ENTRY;

	zd = container_of0(o->lo_dev, struct zil_device, zl_top_dev.dd_lu_dev);
	under = &zd->zl_osd->dd_lu_dev;
	below = under->ld_ops->ldo_object_alloc(env, o->lo_header, under);
	if (below == NULL)
		RETURN(-ENOMEM);

	lu_object_add(o, below);

	RETURN(0);
}

static void zil_object_free(const struct lu_env *env, struct lu_object *o)
{
	struct zil_object	*obj = lu2zil_obj(o);
	struct lu_object_header	*h = o->lo_header;

	dt_object_fini(&obj->zo_obj);
	lu_object_header_fini(h);
	OBD_FREE_PTR(obj);
}

static struct lu_object_operations zil_lu_obj_ops = {
	.loo_object_init  = zil_object_init,
	.loo_object_free  = zil_object_free,
};

static struct lu_object *zil_object_alloc(const struct lu_env *env,
					  const struct lu_object_header *_h,
					  struct lu_device *d)
{
	struct lu_object_header	*h;
	struct zil_object	*o;
	struct lu_object	*l;

	LASSERT(_h == NULL);

	OBD_ALLOC_PTR(o);
	if (o != NULL) {
		l = &o->zo_obj.do_lu;
		h = &o->zo_header;

		lu_object_header_init(h);
		dt_object_init(&o->zo_obj, h, d);
		lu_object_add_top(h, l);

		l->lo_ops = &zil_lu_obj_ops;

		return l;
	} else {
		return NULL;
	}
}

static struct lu_device_operations zil_lu_dev_ops = {
	.ldo_object_alloc =	zil_object_alloc
};

static struct zil_device *osd_zil_dev_prepare(const struct lu_env *env,
					      struct osd_device *osd)
{
	struct dt_device *dt = &osd->od_dt_dev;
	struct zil_device *zl;

	/* not found, then create */
	OBD_ALLOC_PTR(zl);
	LASSERT(zl != NULL);

	atomic_set(&zl->zl_refcount, 1);

	zl->zl_osd = dt;

	LASSERT(dt->dd_lu_dev.ld_site);
	lu_device_init(&zl->zl_top_dev.dd_lu_dev, &zil_lu_type);
	zl->zl_top_dev.dd_lu_dev.ld_ops = &zil_lu_dev_ops;
	zl->zl_top_dev.dd_lu_dev.ld_site = dt->dd_lu_dev.ld_site;

	return zl;
}

static void osd_zil_dev_fini(const struct lu_env *env, struct zil_device *zd)
{
	lu_site_purge(env, zd->zl_top_dev.dd_lu_dev.ld_site, ~0);
	lu_device_fini(&zd->zl_top_dev.dd_lu_dev);
	OBD_FREE_PTR(zd);
}

static int osd_make_snapshot(const char *snapname)
{
	nvlist_t *snaps = fnvlist_alloc();
	int rc;

	fnvlist_add_boolean(snaps, snapname);
	rc = dsl_dataset_snapshot(snaps, NULL, NULL);
	fnvlist_free(snaps);

	return rc;
}

static int osd_rollback(const struct lu_env *env, struct osd_device *o)
{
	nvlist_t	*outnvl;
	int		 rc;

	osd_umount(env, o);

	outnvl = fnvlist_alloc();
	rc = -dsl_dataset_rollback(o->od_mntdev, NULL, outnvl);
	fnvlist_free(outnvl);
	if (rc == 0) {
		rc = osd_mount(env, o);
		if (rc < 0)
			CERROR("%s: remount failed: rc = %d\n",
			       o->od_svname, rc);

	} else
		CERROR("%s: rollback failed: rc = %d\n", o->od_svname, rc);

	return rc;
}

int osd_zil_init(const struct lu_env *env, struct osd_device *o)
{
	int rc;

	if (o->od_zilog == NULL)
		return 0;

	while (BP_IS_HOLE(&o->od_zilog->zl_header->zh_log)) {
		rc = osd_zil_create_noop(o);
		if (rc < 0) {
			CERROR("%s: can't create ZIL, rc = %d\n",
			       o->od_svname, rc);
			break;
		}
		zil_commit(o->od_zilog, 0);
	}

	return 0;
}

static void osd_zil_free_records(const struct lu_env *env,
				 struct osd_zil_cbdata *cb)
{
	struct zil_tx	  *ztx, *tmp;
	struct zil_update *zup, *tmp2;

	list_for_each_entry_safe(ztx, tmp, &cb->zil_txs, zt_list) {
		list_for_each_entry_safe(zup, tmp2, &ztx->zt_updates, zu_list) {
			if (zup->zu_obj != NULL)
				lu_object_put(env, &zup->zu_obj->do_lu);
			OBD_FREE(zup->zu_update, zup->zu_update_size);
			OBD_FREE_PTR(zup);
		}
		OBD_FREE_PTR(ztx);
		cb->discarded++;
	}
}

int osd_zil_replay(const struct lu_env *env, struct osd_device *o)
{
	struct osd_zil_cbdata	 cb;
	dsl_pool_t		*dp = dmu_objset_pool(o->od_os);
	char			*snapname = NULL;
	int			 try = 0;
	int			 rc;
	ENTRY;

	/*if (o->od_zil_enabled == 0)
		return 0;*/

	rc = strlen(o->od_mntdev) + strlen("@rollback");
	OBD_ALLOC(snapname, rc + 1);
	if (snapname == NULL)
		RETURN(-ENOMEM);
	snprintf(snapname, rc + 1, "%s@rollback", o->od_mntdev);

	memset(&cb, 0, sizeof(cb));
	cb.env = env;
	cb.osd = o;
	INIT_LIST_HEAD(&cb.zil_txs);
	cb.hash = cfs_hash_create("obj", HASH_POOLS_CUR_BITS,
				  HASH_POOLS_MAX_BITS, HASH_POOLS_BKT_BITS, 0,
				  CFS_HASH_MIN_THETA, CFS_HASH_MAX_THETA,
				  &zil_object_hash_ops, CFS_HASH_DEFAULT);
	if (cb.hash == NULL) {
		CERROR("%s: cant allocate hash\n", o->od_svname);
		GOTO(out, rc = -ENOMEM);
	}

	cb.zil_dev = osd_zil_dev_prepare(env, o);
	if (cb.zil_dev == NULL) {
		CERROR("%s: cant allocate device\n", o->od_svname);
		GOTO(out, rc = -ENOMEM);
	}

again:
	rc = -osd_make_snapshot(snapname);
	if (rc == -EEXIST) {
		/* the previous mount crashed before ZIL replay completion.
		 * we don't know the current state, so we go back to that
		 * snapshot and try again. */
		if (try++ > 0) {
			/* we tried once and failed */
			GOTO(out, rc);
		}
		rc = osd_rollback(env, o);
		if (rc < 0)
			GOTO(out, rc);
		rc = -dsl_destroy_snapshot(snapname, B_FALSE);
		if (rc < 0) {
			CERROR("%s: cant destroy snapshot: rc = %d\n",
			       o->od_svname, rc);
			GOTO(out, rc);
		}
		goto again;
	} else if (rc < 0) {
		CERROR("%s: cant create snapshot: rc = %d\n", o->od_svname, rc);
		GOTO(out, rc);
	}

	zil_replay(o->od_os, &cb, osd_zil_replay_vector);

	/* discard records with no final barrier or
	 * left for another reason (error?) */
	osd_zil_free_records(env, &cb);

	cfs_hash_for_each_safe(cb.hash, osd_zil_release_state, NULL);
	lu_site_purge(env, cb.zil_dev->zl_top_dev.dd_lu_dev.ld_site, ~0);

	if (o->od_zilog->zl_parse_lr_count > 0) {
		LCONSOLE_INFO("%s: ZIL - %llu blocks, %llu recs, synced %llu\n",
			      o->od_svname, o->od_zilog->zl_parse_blk_count,
			      o->od_zilog->zl_parse_lr_count,
			      dp->dp_tx.tx_synced_txg);
		LCONSOLE_INFO("%s: %u in %u/%u tx in %u groups, %d discarded\n",
			      o->od_svname, cb.update_nr, cb.tx_nr,
			      cb.iterations, cb.barriers, cb.discarded);
	}

	if (cb.result == 0) {
		/* ZIL replay succeed, now we can remove the snapshot */
		rc = -dsl_destroy_snapshot(snapname, B_FALSE);
		if (rc < 0)
			CERROR("%s: cant destroy the snapshot\n", o->od_svname);
	} else {
		/* ZIL replay failed for a reason, discard the changes */
		CERROR("%s: cant replay ZIL, rollback\n", o->od_svname);
		rc = osd_rollback(env, o);
	}

out:
	if (cb.zil_dev != NULL)
		osd_zil_dev_fini(env, cb.zil_dev);
	if (cb.hash != NULL)
		cfs_hash_putref(cb.hash);
	if (snapname != NULL)
		OBD_FREE(snapname, strlen(snapname) + 1);

	RETURN(rc);
}

static struct dt_object *zil_locate(const struct lu_env *env,
				    struct osd_zil_cbdata *cb,
				    const struct lu_fid *fid)
{
	struct zil_device *zd = cb->zil_dev;
	struct lu_object  *lo;

	lo = lu_object_find_at(env, &zd->zl_top_dev.dd_lu_dev, fid, NULL);
	if (IS_ERR(lo))
		return ERR_PTR(PTR_ERR(lo));

	return container_of0(lu_object_next(lo), struct dt_object, do_lu);
}

static unsigned
zfs_object_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash((char *)key, sizeof(struct lu_fid), mask);
}

static void *
zfs_object_key(struct hlist_node *hnode)
{
	struct zil_object_state *zos;

	zos = hlist_entry(hnode, struct zil_object_state, zos_list);

	return &zos->zos_fid;
}

static int
zfs_object_keycmp(const void *key, struct hlist_node *hnode)
{
	const struct lu_fid *key1 = key;
	struct zil_object_state *zos;

	LASSERT(key);
	zos = hlist_entry(hnode, struct zil_object_state, zos_list);

	return lu_fid_eq(key1, &zos->zos_fid);
}

static void *
zfs_object_export_object(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct zil_object_state, zos_list);
}

static void
zfs_object_export_get(cfs_hash_t *hs, struct hlist_node *hnode)
{
}

static void
zfs_object_export_put_locked(cfs_hash_t *hs, struct hlist_node *hnode)
{
}

static cfs_hash_ops_t zil_object_hash_ops = {
	.hs_hash	= zfs_object_hash,
	.hs_key		= zfs_object_key,
	.hs_keycmp	= zfs_object_keycmp,
	.hs_object	= zfs_object_export_object,
	.hs_get		= zfs_object_export_get,
	.hs_put_locked	= zfs_object_export_put_locked,
};

