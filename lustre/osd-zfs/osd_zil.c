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
 * ZIL support
 *
 * (as the protocol doesn't support out-of-order commit, we sync everything)
 *
 * there are three steps:
 * 1) copy ZIL records to the regular files
 *    this is needed as we need the records in a snapshot, but ZIL
 *    isn't a subject to snapshot mechanism
 * 2) make a snapshot
 *    this is needed to be able to rollback if we can't apply changes
 *    from the log for a reason (crashed, etc)
 * 3) apply the changes from the log in the original order:
 *    in contrast with ZPL, Lustre's transactions race each other. say,
 *    transaction T1 changes object O1, then T2 changes O2, then T1
 *    changes O2, then T2 changes O1. it's not possible to serialize T1
 *    and T2. also, there are may be many transactions between T1 and T2
 *    in ZIL. so, instead of trying to commit all this within a single
 *    TXG (btw we can't control TXG's size/lifetime) we just use snapshot
 *    and rollback to mimic TXG behavior: remember the stable state using
 *    a snapshot, apply the updates in the original order, each in own
 *    transaction, then discard the snapshot upon completion. if we fail
 *    during application process, then just rollback to the preserved
 *    snapshot and try again
 *
 * there are two log files:
 * the update log is storing regular updates. the data log is storing writes
 * made with osd_commit_write(), namely data went through zero-copy interface.
 * the idea is to save memory during replay and not require everything to fit
 * memory. the same approach can be used for the writes made with dt_write()
 * if we find those are consuming too much memory during replay.
 *
 * the order of updates is tracked using per-object version. every update to an
 * object increments its version which goes to ZIL. every time an object leaves
 * the cache the global (per-osd) version is updated if it has a smaller value.
 * when an object gets cached, it`s initial version is initialized from the
 * global version. this way we ensure any update to an object gets a bigger
 * version, so the order is preserved. notice the version is growing in
 * non-monothonical manner. this means we have to sort all the updates before
 * applying.
 *
 * we have to introduce additional locking to protect data being synced via ZIL
 * from modification from the upper layers (to preserve csum actual). it's range
 * locks, as sometimes the original writes in ZIL aren't block-aligned. let's
 * say we got two OST_WRITE and those both went to block B, while in ZIL we got
 * two records. the writes were large enough to postpone data inlining till we
 * really need to commit ZIL. so at ZIL commit time we'll get two get_data()
 * callbacks in a row and both will be trying to lock the same block B to
 * protect the data.
 *
 * yet another problem is OI which is maintained by OSD internally, but there
 * are two types of OI: for named objects (user-visible files, for simplicity)
 * and unnamed objects (i.e. OST objects), depending on object's type we use
 * different OI. the type isn't passed at creation, so the only way to know
 * that is FLDB which isn't running at ZIL replay time. to deal with this issue
 * we log OI index creation (osd_oi_create()) and then by the time an object
 * is being create at ZIL replay, we can consult with local OI.
 *
 */

/*
 * Concerns:
 *  - not enough memory to hold all the structures (objects, updates)
 *    - do not load data for inline writes
 *    - do not load arguments like attributes, names, values
 *  - time to replay a big txg?
 *  - on-disk space reservation for the logs
 *  - on-disk space consumption due to re-ordered updates?
 *  - sorting alrogithm should be proved with stats
 *
 * memory consumption at replay evaluation:
 * w/o OUT improvements: dbench - 868 bytes/transaction
 * 100K ops/sec - 82MB for a second of txg
 * 82M * 10s = 820MB for a whole txg with 1M transactions
 *
 */

struct osd_replay_state {
	const struct lu_env	*ors_env;
	struct osd_device	*ors_osd;
	struct zil_device	*ors_zil_dev;
	struct cfs_hash		*ors_hash;
	struct list_head	 ors_zil_txs;
	uint64_t		 ors_log_dnode;
	uint64_t		 ors_dlog_dnode;
	int			 ors_zil_updates;
	int			 ors_result;
	int			 ors_barriers;
	int			 ors_tx_nr;
	int			 ors_update_nr;
	int			 ors_discarded;
	int			 ors_iterations;
	int			 ors_mem_allocated;
	int			 ors_max_mem_allocated;
};

/* a trivial device we create to use OSD API */
struct zil_device {
	struct dt_device	 zl_top_dev;
	/* how many handle's reference this local storage */
	atomic_t		 zl_refcount;
	/* underlaying OSD device */
	struct dt_device	*zl_osd;
};

/* a single transaction like a Lustre file creation,
 * list all the involved updates */
struct zil_tx {
	struct list_head	 zt_list;
	struct list_head	 zt_updates;
	int			 zt_repeats;
};

/* a structure representing an object. notice we don't use
 * normal lu_object as it pins osd_object(), dbuf and arcbuf
 * which can be very expensive given we have to retain this
 * state for a whole txg - to sort all corresponding updates */
struct zil_object_state {
	struct hlist_node	zos_list;
	struct list_head	zos_updates;
	struct lu_fid		zos_fid;
	__u64			zos_version;
};

struct zil_object {
	struct lu_object_header	 zo_header;
	struct dt_object	 zo_obj;
};

/* a structure represenging a single update to a specific object */
struct zil_update {
	struct list_head	 zu_list; /* all updates of tx */
	struct list_head	 zu_on_zos; /* all updates of object */
	struct lu_fid		 zu_fid;
	struct dt_object	*zu_obj;
	struct zil_object_state	*zu_zos;
	__u64			 zu_version;
	struct object_update	*zu_update;
	int			 zu_update_size;
};

/* a header for the structure storing Lustre updates in ZIL */
struct lr_update {
	lr_t	lr_common;
};

static struct dt_object *zil_locate(const struct lu_env *env,
				    struct osd_replay_state *ors,
				    const struct lu_fid *fid);
typedef int (*osd_replay_update)(const struct lu_env *env,
				 struct osd_replay_state *ors,
				 struct dt_object *o,
				 struct object_update *u,
				 struct osd_thandle *oh);
static struct cfs_hash_ops zil_object_hash_ops;


/* this function is called by ZIL subsystem when corresponding
 * ZIL record is written and we can release the resources:
 * dbug, range lock, etc. */
void osd_zil_get_done(zgd_t *zgd, int error)
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
	struct osd_object	*obj = arg;
	struct osd_device	*osd = osd_obj2dev(obj);
	const struct lu_fid	*fid = lu_object_fid(&obj->oo_dt.do_lu);
	objset_t		*os = osd->od_os;
	blkptr_t		*bp = &lr->lr_blkptr;
	dmu_buf_t		*db;
	zgd_t			*zgd;
	uint64_t		 offset = lr->lr_offset;
	uint64_t		 size = lr->lr_length;
	int			 rc;

	ASSERT(zio != NULL);
	ASSERT(size != 0);

	CDEBUG(D_HA, "get data for %p/%llu: %llu/%u\n",
		obj, obj->oo_db->db_object, offset, (int)size);

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

	/*
	 * Write records come in two flavors: immediate and indirect.
	 * For small writes it's cheaper to store the data with the
	 * log record (immediate); for large writes it's cheaper to
	 * sync the data and get a pointer to it (indirect) so that
	 * we don't have to write the data twice.
	 */
	if (buf != NULL) { /* immediate write */
		LBUG(); /* no support yet, should be easy to add */
		rc = dmu_read(os, obj->oo_db->db_object, offset, size, buf,
			      DMU_READ_NO_PREFETCH);

		/* XXX: a trick to store FID/version */
		lr->lr_blkptr.blk_pad[0] = lr->lr_blkoff;
		lr->lr_blkptr.blk_pad[1] = fid->f_seq;
		lr->lr_blkptr.blk_fill = fid->f_oid;

		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_COPIED, 1);
	} else {
		struct osd_range_lock *l;

		l = osd_lock_range(obj, offset, size, 0);
		if (IS_ERR(l))
			GOTO(out, rc = PTR_ERR(l));
		zgd->zgd_rl = (void *)l;

		offset = P2ALIGN_TYPED(offset, size, uint64_t);
		rc = dmu_buf_hold(os, obj->oo_db->db_object, offset, zgd, &db,
				  DMU_READ_NO_PREFETCH);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_INDIRECT, 1);
		if (rc == 0) {
			blkptr_t *obp = dmu_buf_get_blkptr(db);
			if (obp) {
				ASSERT(BP_IS_HOLE(bp));
				*bp = *obp;
			}

			/* XXX: a tricky part here
			 * ZIL requires lr_write_t with no extra data
			 * for indirect writes. but space in lr_write_t
			 * is not enough to place FID and version.
			 * so we re-use lr_length for own purposes.
			 * actual length can be taken from the blkptr */
			lr->lr_length = lr->lr_blkoff; /* version */
			lr->lr_length |= (uint64_t)fid->f_oid << 32;
			lr->lr_blkoff = fid->f_seq;

			zgd->zgd_db = db;
			BP_ZERO(&lr->lr_blkptr);
			zgd->zgd_bp = &lr->lr_blkptr;
			zgd->zgd_private = obj;

			ASSERT(db != NULL);
			ASSERT(db->db_offset == offset);
			ASSERT(db->db_size == size);

			rc = dmu_sync(zio, lr->lr_common.lrc_txg,
				      osd_zil_get_done, zgd);
		}
	}

out:
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
	struct osd_object	*obj  = osd_dt_obj(dt);
	struct osd_device	*osd = osd_obj2dev(obj);
	zilog_t			*zilog = osd->od_zilog;
	dnode_t			*dn;
	ssize_t			 immediate_write_sz;
	int			 blocksize;

	/* ZIL is disabled or in replay */
	if (oh->ot_update == NULL)
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
			LBUG();
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
		BP_ZERO(&lr->lr_blkptr);
		/* XXX: as we don't replay directly from ZIL,
		 *	this object can be non-existing-yet,
		 *	but zil_replay() checks foid, so foid
		 *	must point to an existing object */
		lr->lr_foid = 1;
		lr->lr_offset = offset;
		lr->lr_length = len;
		lr->lr_blkoff = obj->oo_init_version +
				atomic_inc_return(&obj->oo_version);
		if (write_state == WR_COPIED) {
			/* XXX: a trick to store FID/version */
			const struct lu_fid *f = lu_object_fid(&dt->do_lu);
			lr->lr_blkptr.blk_pad[0] = lr->lr_blkoff;
			lr->lr_blkptr.blk_pad[1] = f->f_seq;
			lr->lr_blkptr.blk_fill = f->f_oid;
		}

		itx->itx_callback = osd_zil_write_commit_cb;
		itx->itx_callback_data = itx;

		lu_object_get(&obj->oo_dt.do_lu);
		itx->itx_private = obj;
		itx->itx_sync = 1;

		atomic_inc(&osd->od_recs_to_write);
		lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_RECORDS,
				    itx->itx_lr.lrc_reclen);
		if (itx->itx_wr_state == WR_COPIED)
			lprocfs_counter_add(osd->od_stats,
					    LPROC_OSD_ZIL_COPIED, 1);

		zil_itx_assign(zilog, itx, oh->ot_tx);

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
				    struct osd_replay_state *ors,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct obdo		*lobdo = &osd_oti_get(env)->oti_obdo1;
	struct obdo		*wobdo = &osd_oti_get(env)->oti_obdo2;
	struct lu_attr		*attr = &osd_oti_get(env)->oti_la;
	struct dt_object_format	 dof;
	size_t			 size = 0;
	void			*ptr;
	int			 rc;

	ptr = object_update_param_get(u, 0, &size);
	if (ptr == NULL)
		return -EINVAL;
	if (size == sizeof(*wobdo)) {
		wobdo = ptr;

		attr->la_valid = 0;
		attr->la_valid = 0;

		lustre_get_wire_obdo(NULL, lobdo, wobdo);
		la_from_obdo(attr, lobdo, lobdo->o_valid);
	} else {
		osd_unpack_lu_attr(attr, ptr);
	}

	dof.dof_type = dt_mode_to_dft(attr->la_mode);

	/* XXX: hint?* */

	if (oh->ot_assigned == 0)
		rc = dt_declare_create(env, o, attr, NULL, &dof, &oh->ot_super);
	else
		rc = dt_create(env, o, attr, NULL, &dof, &oh->ot_super);

	return rc;
}

static int osd_replay_destroy_update(const struct lu_env *env,
				     struct osd_replay_state *ors,
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
				     struct osd_replay_state *ors,
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
				     struct osd_replay_state *ors,
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
				      struct osd_replay_state *ors,
				      struct dt_object *o,
				      struct object_update *u,
				      struct osd_thandle *oh)
{
	struct obdo		*lobdo = &osd_oti_get(env)->oti_obdo1;
	struct obdo		*wobdo = &osd_oti_get(env)->oti_obdo2;
	struct lu_attr		*attr = &osd_oti_get(env)->oti_la;
	void			*ptr;
	int			 rc;
	size_t			 size = 0;

	ptr = object_update_param_get(u, 0, &size);
	if (ptr == NULL)
		return -EINVAL;
	if (size == sizeof(*wobdo)) {
		wobdo = ptr;
		attr->la_valid = 0;
		attr->la_valid = 0;

		lustre_get_wire_obdo(NULL, lobdo, wobdo);
		la_from_obdo(attr, lobdo, lobdo->o_valid);
	} else {
		osd_unpack_lu_attr(attr, ptr);
	}

	if (oh->ot_assigned == 0)
		rc = dt_declare_attr_set(env, o, attr, &oh->ot_super);
	else
		rc = dt_attr_set(env, o, attr, &oh->ot_super);

	return rc;
}

static int osd_replay_xattr_set_update(const struct lu_env *env,
				       struct osd_replay_state *ors,
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
				       struct osd_replay_state *ors,
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
				    struct osd_replay_state *ors,
				    struct dt_object *o,
				    struct object_update *u,
				    struct osd_thandle *oh)
{
	struct dt_insert_rec rec;
	struct lu_fid *fid;
	size_t		size = 0;
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
				    struct osd_replay_state *ors,
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

static int osd_replay_insert_bin_update(const struct lu_env *env,
					struct osd_replay_state *ors,
					struct dt_object *o,
					struct object_update *u,
					struct osd_thandle *oh)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	struct dt_index_features *dif = &info->oti_dif;
	char			*rec;
	size_t			size = 0;
	char *name;
	int rc;

	name = object_update_param_get(u, 0, &size);
	LASSERT(name != NULL);
	dif->dif_flags = 0;
	dif->dif_keysize_min = size;
	dif->dif_keysize_max = size;

	rec = object_update_param_get(u, 1, &size);
	LASSERT(rec != NULL);
	dif->dif_recsize_min = size;
	dif->dif_recsize_max = size;

	if (o->do_index_ops == NULL) {
		rc = o->do_ops->do_index_try(env, o, dif);
		LASSERT(rc == 0);
	}

	if (oh->ot_assigned == 0)
		rc = dt_declare_insert(env, o, (struct dt_rec *)rec,
					(struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_insert(env, o, (struct dt_rec *)rec,
				(struct dt_key *)name,
				&oh->ot_super, 1);

	return rc;
}

static int osd_replay_delete_bin_update(const struct lu_env *env,
					struct osd_replay_state *ors,
					struct dt_object *o,
					struct object_update *u,
					struct osd_thandle *oh)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	struct dt_index_features *dif = &info->oti_dif;
	size_t			size = 0;
	char *name;
	int rc;

	name = object_update_param_get(u, 0, &size);
	LASSERT(name != NULL);
	dif->dif_flags = 0;
	dif->dif_keysize_min = size;
	dif->dif_keysize_max = size;
	dif->dif_recsize_min = 8;
	dif->dif_recsize_max = 8;

	if (o->do_index_ops == NULL) {
		rc = o->do_ops->do_index_try(env, o, dif);
		LASSERT(rc == 0);
	}

	if (oh->ot_assigned == 0)
		rc = dt_declare_delete(env, o, (struct dt_key *)name,
					&oh->ot_super);
	else
		rc = dt_delete(env, o, (struct dt_key *)name,
				&oh->ot_super);

	return rc;
}

static int osd_replay_write_update(const struct lu_env *env,
				   struct osd_replay_state *ors,
				   struct dt_object *o,
				   struct object_update *u,
				   struct osd_thandle *oh)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	struct osd_device	*osd = osd_dt_dev(oh->ot_super.th_dev);
	struct lu_buf lb;
	__u64 *tmp, pos, offset_in_log;
	size_t size = 0;
	int rc;

	lb.lb_buf = object_update_param_get(u, 0, &size);
	if (lb.lb_buf == NULL) {
		/* data should be read from the data log (indirect) */
		tmp = object_update_param_get(u, 2, &size);
		LASSERT(tmp != NULL && size == 8);
		offset_in_log = *tmp;
		tmp = object_update_param_get(u, 3, &size);
		LASSERT(tmp != NULL && size == 8);
		lb.lb_len = *tmp;
		if (oh->ot_assigned != 0) {
			lu_buf_check_and_alloc(&info->oti_lb, lb.lb_len);
			if (info->oti_lb.lb_buf == NULL)
				GOTO(out, rc = -ENOMEM);
			lb.lb_buf = info->oti_lb.lb_buf;
			rc = -dmu_read(osd->od_os, ors->ors_dlog_dnode,
				       offset_in_log, lb.lb_len,
				       lb.lb_buf, DMU_READ_PREFETCH);
			if (rc < 0)
				GOTO(out, rc);
		}
	} else {
		LASSERT(lb.lb_buf != NULL && size > 0);
		lb.lb_len = size;
	}

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

out:
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
	[OUT_INDEX_BIN_INSERT]	= osd_replay_insert_bin_update,
	[OUT_INDEX_BIN_DELETE]	= osd_replay_delete_bin_update,
};

struct zil_object_state *
osd_zil_find_or_create_state(struct cfs_hash *h, const struct lu_fid *fid)
{
	struct zil_object_state *zos;

	zos = cfs_hash_lookup(h, fid);
	if (zos == NULL) {
		OBD_ALLOC_PTR(zos);
		if (zos != NULL) {
			zos->zos_fid = *fid;
			INIT_LIST_HEAD(&zos->zos_updates);
			cfs_hash_add(h, fid, &zos->zos_list);
		}
	}
	return zos;
}

static int osd_zil_release_state(struct cfs_hash *hs, struct cfs_hash_bd *bd,
				 struct hlist_node *hnode, void *data)
{
	struct zil_object_state *zos;

	zos = hlist_entry(hnode, struct zil_object_state, zos_list);
	LASSERT(list_empty(&zos->zos_updates));
	cfs_hash_bd_del_locked(hs, bd, hnode);
	OBD_FREE_PTR(zos);

	return 0;
}

static void osd_zil_add_update(struct zil_object_state *zos,
			       struct zil_update *new)
{
	struct zil_update *zup;

	/* this is a trivial implementation based on the idea that most of the
	 * updates are ordered and in rare cases very few of them are out of
	 * order. needs to be proved with stats */
	list_for_each_entry_reverse(zup, &zos->zos_updates, zu_on_zos) {
		if (new->zu_version > zup->zu_version) {
			list_add(&new->zu_on_zos, &zup->zu_on_zos);
			return;
		}
	}
	list_add(&new->zu_on_zos, &zos->zos_updates);
}

/*
 * make a copy of the record to memory
 */
static int osd_zil_parse_update(struct osd_replay_state *ors, void *buf)
{
	struct object_update_request	*ureq = buf;
	struct zil_tx			*ztx;
	struct zil_update		*zup;
	int				 i, rc = 0;
	ENTRY;

	if (unlikely(ureq->ourq_magic != UPDATE_REQUEST_MAGIC))
		GOTO(out, rc = -EINVAL);

	OBD_ALLOC_PTR(ztx);
	if (unlikely(ztx == NULL))
		GOTO(out, rc = -ENOMEM);
	INIT_LIST_HEAD(&ztx->zt_updates);
	list_add_tail(&ztx->zt_list, &ors->ors_zil_txs);
	ors->ors_mem_allocated += sizeof(*ztx);

	for (i = 0; i < ureq->ourq_count; i++) {
		struct object_update	*update;
		struct zil_object_state	*zos;
		size_t			 size;

		update = object_update_request_get(ureq, i, &size);
		LASSERT(update != NULL);

		OBD_ALLOC_PTR(zup);
		if (unlikely(zup == NULL))
			GOTO(out, rc = -ENOMEM);

		OBD_ALLOC(zup->zu_update, size);
		if (unlikely(zup->zu_update == NULL)) {
			OBD_FREE_PTR(zup);
			GOTO(out, rc = -ENOMEM);
		}
		ors->ors_mem_allocated += sizeof(*zup) + size;

		CDEBUG(D_OTHER, "update %s\n", update_op_str(update->ou_type));

		memcpy(zup->zu_update, update, size);
		zup->zu_update_size = size;
		zup->zu_version = update->ou_batchid;
		zup->zu_fid = update->ou_fid;
		list_add_tail(&zup->zu_list, &ztx->zt_updates);

		/* store minimal (initial) version */
		zos = osd_zil_find_or_create_state(ors->ors_hash, &zup->zu_fid);
		if (zos == NULL)
			GOTO(out, rc = -ENOMEM);
		zup->zu_zos = zos;

		osd_zil_add_update(zos, zup);
	}

out:
	RETURN(rc);
}

static int osd_zil_replay_create(struct osd_replay_state *ors, lr_create_t *lr,
				boolean_t byteswap)
{
	char *name = (char *)(lr + 1);
	uint64_t foid = lr->lr_foid;
	int rc;

	rc = osd_oi_create(ors->ors_env, ors->ors_osd, lr->lr_doid,
			   name, &foid);
	LASSERT(lr->lr_foid == foid);

	return rc;
}

static int osd_zil_replay_tx(struct osd_replay_state *ors, struct zil_tx *ztx)
{
	struct osd_device	*osd = ors->ors_osd;
	struct dt_device	*dt = &osd->od_dt_dev;
	const struct lu_env	*env = ors->ors_env;
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
		zup->zu_obj = zil_locate(env, ors, &zup->zu_fid);
		if (IS_ERR(zup->zu_obj)) {
			rc = PTR_ERR(zup->zu_obj);
			zup->zu_obj = NULL;
			GOTO(out, rc);
		}

		u = zup->zu_update;
		LASSERT(u != NULL);

		if (CFS_FAIL_CHECK(OBD_FAIL_OSD_LOG_REPLAY))
			GOTO(out, rc = -EIO);

		rc = osd_replay_update_vec[u->ou_type](env, ors,
						       zup->zu_obj, u, oh);
		if (unlikely(rc < 0))
			GOTO(out, rc);
	}

	rc = dt_trans_start(env, dt, th);
	if (unlikely(rc < 0))
		GOTO(out, rc);
	oh = container_of0(th, struct osd_thandle, ot_super);

	list_for_each_entry_safe(zup, tmp, &ztx->zt_updates, zu_list) {
		struct zil_update *zz;
		u = zup->zu_update;
		zz = list_entry(zup->zu_zos->zos_updates.next,
				struct zil_update, zu_on_zos);
		if (zz != zup) {
			/* there is an update from a racing transaction
			 * our update can depend on, let's wait that
			 * to go first */
			CDEBUG(D_OTHER, "tx %p "DFID" : %llu != %llu\n",
			       zup, PFID(&zup->zu_fid), zup->zu_version,
			       zz->zu_version);
			lu_object_put(env, &zup->zu_obj->do_lu);
			zup->zu_obj = NULL;
			continue;
		}

		LASSERTF(zup->zu_version > zup->zu_zos->zos_version,
			 "%llu <= %llu\n", zup->zu_version,
			 zup->zu_zos->zos_version);

		LASSERT(zup->zu_obj != NULL);
		rc = osd_replay_update_vec[u->ou_type](env, ors,
						       zup->zu_obj, u, oh);
		if (unlikely(rc < 0))
			GOTO(out, rc);

		zup->zu_zos->zos_version = zup->zu_version;

		lu_object_put(env, &zup->zu_obj->do_lu);

		list_del(&zup->zu_list);
		list_del(&zup->zu_on_zos);
		OBD_FREE(zup->zu_update, zup->zu_update_size);
		OBD_FREE_PTR(zup);

		ors->ors_update_nr++;
	}

out:
	rc2 = dt_trans_stop(env, dt, th);
	ztx->zt_repeats++;

	RETURN(rc2 < 0 ? rc2 : rc);
}

static void osd_zil_dump_updates(struct osd_replay_state *ors)
{
	struct zil_update	*zup;
	struct zil_tx		*ztx;

	list_for_each_entry(ztx, &ors->ors_zil_txs, zt_list) {
		printk(KERN_INFO "tx %p\n", ztx);
		list_for_each_entry(zup, &ztx->zt_updates, zu_list) {
			struct object_update	*u;
			u = zup->zu_update;
			printk(KERN_INFO "  %s "DFID" ver %llu (%llu now)\n",
			       update_op_str(u->ou_type),
			       PFID(&zup->zu_fid), zup->zu_version,
			       zup->zu_zos->zos_version);
		}
	}
}

static int osd_zil_apply_updates(struct osd_replay_state *ors)
{
	struct zil_tx	*ztx, *tmp;
	int		 rc, iterations = 0;

	/* this is the end of the virtual txg committing all the previous
	 * transactions is enough to make Lustre happy.
	 * given the records don't follow the original ordering,
	 * we use few iterations to apply the updates */

	if (ors->ors_mem_allocated > ors->ors_max_mem_allocated)
		ors->ors_max_mem_allocated = ors->ors_mem_allocated;
	ors->ors_mem_allocated = 0;

	ors->ors_barriers++;

	while (!list_empty(&ors->ors_zil_txs) && ors->ors_result == 0) {
		ors->ors_iterations++;
		iterations++;
		if (iterations > 1000) {
			osd_zil_dump_updates(ors);
			LBUG();
		}
		list_for_each_entry_safe(ztx, tmp, &ors->ors_zil_txs, zt_list) {

			rc = osd_zil_replay_tx(ors, ztx);
			if (rc < 0) {
				CERROR("%s: can't replay: rc = %d\n",
				       ors->ors_osd->od_svname, rc);
				ors->ors_result = rc;
				break;
			}

			if (list_empty(&ztx->zt_updates)) {
				ors->ors_tx_nr++;
				list_del(&ztx->zt_list);
				OBD_FREE_PTR(ztx);
			}
		}
	}

	return ors->ors_result;
}

static int osd_zil_create_file(struct osd_replay_state *ors, char *name,
				uint64_t *dnode)
{
	struct osd_device	*osd = ors->ors_osd;
	struct zpl_direntry	 zde;
	dmu_tx_t		*tx;
	uint64_t		 oid;
	dmu_buf_t		*db;
	struct lu_attr		 la;
	int			 rc;

	rc = -zap_lookup(osd->od_os, osd->od_root, name, 8, 1, (void *)&zde);
	if (rc >= 0) {
		*dnode = zde.zde_dnode;
		return 0;
	}

	if (rc != -ENOENT)
		return rc;

	/* doesn't exist, create a new one */
	tx = dmu_tx_create(osd->od_os);
	if (tx == NULL)
		return -ENOMEM;

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);
	dmu_tx_hold_bonus(tx, osd->od_root);
	dmu_tx_hold_zap(tx, osd->od_root, TRUE, name);
	LASSERT(tx->tx_objset->os_sa);
	dmu_tx_hold_sa_create(tx, ZFS_SA_BASE_ATTR_SIZE);

	rc = -dmu_tx_assign(tx, TXG_WAIT);
	if (rc) {
		dmu_tx_abort(tx);
		return rc;
	}

	la.la_valid = LA_MODE | LA_UID | LA_GID;
	la.la_mode = S_IFREG | S_IRUGO | S_IWUSR | S_IXUGO;
	la.la_uid = la.la_gid = 0;
	la.la_size = 0;
	la.la_nlink = 1;

	oid = dmu_object_alloc(osd->od_os, DMU_OT_PLAIN_FILE_CONTENTS,
			       0, DMU_OT_SA, DN_MAX_BONUSLEN, tx);
	rc = -sa_buf_hold(osd->od_os, oid, osd_obj_tag, &db);
	if (rc < 0)
		goto out;

	rc = __osd_attr_init(ors->ors_env, osd, oid, tx, &la, osd->od_root);
	sa_buf_rele(db, osd_obj_tag);
	if (rc < 0)
		goto out;

	zde.zde_dnode = oid;
	zde.zde_pad = 0;
	zde.zde_type = IFTODT(S_IFREG);

	rc = -zap_add(osd->od_os, osd->od_root, name, 8, 1, (void *)&zde, tx);

out:
	dmu_tx_commit(tx);

	if (rc == 0)
		*dnode = oid;

	return rc;

}

static int osd_zil_check_and_create_log(struct osd_replay_state *ors)
{
	int rc;

	if (ors->ors_log_dnode != 0)
		return 0;

	rc = osd_zil_create_file(ors, "log", &ors->ors_log_dnode);
	if (rc < 0) {
		CERROR("%s: can't create the log: rc = %d\n",
		       ors->ors_osd->od_svname, rc);
	}
	return rc;
}

static int osd_zil_check_and_create_data_log(struct osd_replay_state *ors)
{
	int rc;

	if (ors->ors_dlog_dnode != 0)
		return 0;

	rc = osd_zil_create_file(ors, "dlog", &ors->ors_dlog_dnode);
	if (rc < 0) {
		CERROR("%s: can't create the data log: rc = %d\n",
		       ors->ors_osd->od_svname, rc);
	}
	return rc;
}

static int osd_zil_write_to_log(struct osd_replay_state *ors, uint64_t oid,
				void *buf, int reclen, __u64 *ret_offset)
{
	struct lu_attr		*la = &osd_oti_get(ors->ors_env)->oti_la;
	struct osd_device	*osd = ors->ors_osd;
	dmu_tx_t		*tx;
	__u64			 offset;
	int			 rc;

	rc = __osd_object_attr_get(ors->ors_env, osd, oid, la);
	if (rc < 0)
		return rc;
	offset = la->la_size;

	/* doesn't exist, create a new one */
	tx = dmu_tx_create(osd->od_os);
	if (tx == NULL)
		return -ENOMEM;

	dmu_tx_hold_bonus(tx, oid);
	dmu_tx_hold_write(tx, oid, offset, reclen + 4);
	dmu_tx_hold_sa_create(tx, ZFS_SA_BASE_ATTR_SIZE);

	rc = -dmu_tx_assign(tx, TXG_WAIT);
	if (rc) {
		dmu_tx_abort(tx);
		return rc;
	}

	dmu_write(osd->od_os, oid, offset, 4, &reclen, tx);
	if (reclen > 0)
		dmu_write(osd->od_os, oid, offset + 4, reclen, buf, tx);

	la->la_valid = LA_SIZE;
	la->la_size = offset + reclen + 4;

	rc = __osd_attr_init(ors->ors_env, osd, oid, tx, la, osd->od_root);
	if (rc < 0)
		goto out;

	if (ret_offset == NULL) {
		/* this is a write to the update logs, we have to
		 * ensure no duplicates in the case of a crash during
		 * ZIL replay */
		zil_replaying(osd->od_zilog, tx);
	} else {
		/* for indirect data we don't need to avoid dups as
		 * it can be referenced by an update in the update logs */
		*ret_offset = offset;
	}

out:
	dmu_tx_commit(tx);

	return rc;
}

static int osd_zil_replay_update(struct osd_replay_state *ors,
				 struct lr_update *lr,
				 boolean_t byteswap)
{
	int	rc, size;
	ENTRY;

	if (CFS_FAIL_CHECK(OBD_FAIL_OSD_ZIL_REPLAY))
		GOTO(out, rc = -EIO);

	if (byteswap && 0)
		byteswap_uint64_array(lr, sizeof(*lr));

	rc = osd_zil_check_and_create_log(ors);
	if (rc < 0)
		GOTO(out, rc);

	/* copy record to the stable log */
	size = lr->lr_common.lrc_reclen - sizeof(*lr);
	rc = osd_zil_write_to_log(ors, ors->ors_log_dnode, lr + 1, size, NULL);

out:
	if (ors->ors_result == 0)
		ors->ors_result = rc;
	RETURN(SET_ERROR(-ors->ors_result));
}

static int osd_zil_replay_write(struct osd_replay_state *ors, lr_write_t *lr,
				boolean_t byteswap)
{
	struct osd_device	*osd = ors->ors_osd;
	__u16			 sizes[4];
	const void		*bufs[4];
	struct object_update_request	*ureq = NULL;
	struct object_update	*update;
	struct lu_fid		 fid;
	uint64_t		 pos, len, offset_in_log, version;
	void			*buf = (lr + 1);
	size_t			 size;
	int			 rc;

	if (CFS_FAIL_CHECK(OBD_FAIL_OSD_ZIL_REPLAY))
		GOTO(out, rc = -EIO);

	if (byteswap && 0)
		byteswap_uint64_array(lr, sizeof(*lr));

	/* create a log if it doesn't exist */
	rc = osd_zil_check_and_create_data_log(ors);
	if (rc < 0)
		GOTO(out, rc);

	size = sizeof(struct object_update_request) +
		sizeof(struct object_update) +
		sizeof(struct object_update_param) * 4 + 32;
	OBD_ALLOC(ureq, size);
	if (ureq == NULL)
		GOTO(out, rc = -ENOMEM);
	ureq->ourq_magic = UPDATE_REQUEST_MAGIC;

	sizes[0] = 0;
	sizes[1] = sizeof(__u64);
	sizes[2] = sizeof(__u64);
	sizes[3] = sizeof(__u64);
	update = update_buffer_get_update(ureq, 0);
	LASSERT(update != NULL);

	pos = cpu_to_le64(lr->lr_offset);

	if (lr->lr_common.lrc_reclen == sizeof(*lr)) {
		/* generate a normal update from this indirect write */
		len = BP_GET_LSIZE(&lr->lr_blkptr);
		fid.f_seq = lr->lr_blkoff;
		fid.f_oid = lr->lr_length >> 32;
		fid.f_ver = 0;
		version = lr->lr_length & 0xffffffff;
	} else {
		/* direct write */
		len = lr->lr_length;
		fid.f_seq = lr->lr_blkptr.blk_pad[1];
		fid.f_oid = lr->lr_blkptr.blk_fill;
		fid.f_ver = 0;
		version = lr->lr_blkptr.blk_pad[0];
	}

	rc = osd_zil_write_to_log(ors, ors->ors_dlog_dnode, buf,
				  len, &offset_in_log);
	if (rc < 0) {
		CERROR("%s: can't write data: rc = %d\n", osd->od_svname, rc);
		GOTO(out, rc);
	}
	offset_in_log += 4; /* because of header */

	CDEBUG(D_OTHER, "move %s: ver=%u len=%u to "DFID", at %u\n",
		lr->lr_common.lrc_reclen == sizeof(*lr) ? "indirect" : "direct",
		(unsigned)version, (unsigned)len, PFID(&fid),
		(unsigned)offset_in_log);

	bufs[0] = NULL;
	bufs[1] = &pos;
	bufs[2] = &offset_in_log;
	bufs[3] = &len;

	rc = out_update_pack(ors->ors_env, update, &size, OUT_WRITE, &fid,
			     ARRAY_SIZE(sizes), sizes, bufs, 0);
	if (rc < 0) {
		CERROR("%s: can't pack write: rc = %d\n", osd->od_svname, rc);
		GOTO(out, rc);
	}
	update->ou_batchid = version;
	ureq->ourq_count = 1;

	/* copy record to the stable log */
	rc = osd_zil_write_to_log(ors, ors->ors_log_dnode, ureq,
				object_update_request_size(ureq), NULL);
	if (rc < 0) {
		CERROR("%s: can't write pack: rc = %d\n", osd->od_svname, rc);
		GOTO(out, rc);
	}

out:
	if (ureq != NULL)
		OBD_FREE(ureq, size);

	if (ors->ors_result == 0)
		ors->ors_result = rc;
	RETURN(SET_ERROR(-rc));
}

static int osd_zil_replay_sync(struct osd_replay_state *ors, lr_acl_v0_t *lr,
				boolean_t byteswap)
{
	int			 rc;
	ENTRY;

	if (ors->ors_log_dnode == 0) {
		/* the log is empty, so sync is noop */
		return 0;
	}

	/* XXX: ignore if no records since the last sync */

	rc = osd_zil_write_to_log(ors, ors->ors_log_dnode, NULL, 0, NULL);

	if (ors->ors_result == 0)
		ors->ors_result = rc;
	RETURN(SET_ERROR(-rc));
}

static int osd_zil_replay_noop(struct osd_replay_state *ors, lr_create_t *lr,
				boolean_t byteswap)
{
	/* used as noop, see osd_ro() */
	CWARN("CREATE replay - NOOP\n");
	return 0;
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
	(zil_replay_func_t)osd_zil_replay_noop,	/* TX_ACL_V0 */
};

static int osd_zil_create_noop(struct osd_device *o)
{
	itx_t		*itx;
	dmu_tx_t	*tx;
	lr_acl_v0_t	*lr;
	int		 rc;

	tx = dmu_tx_create(o->od_os);
	rc = -dmu_tx_assign(tx, TXG_WAITED);
	if (rc != 0) {
		dmu_tx_abort(tx);
		return rc;
	}
	itx = zil_itx_create(TX_ACL_V0, sizeof(*lr));
	lr = (lr_acl_v0_t *)&itx->itx_lr;
	lr->lr_foid = 0;
	lr->lr_aclcnt = 0;
	zil_itx_assign(o->od_zilog, itx, tx);
	dmu_tx_commit(tx);

	return 0;
}

void osd_zil_dump_stats(struct osd_device *o)
{
	long ave;
	int i, j;
	char *buf;

	if (atomic_read(&o->od_recs_in_log) == 0)
		return;

	ave = atomic_read(&o->od_bytes_in_log);
	ave /= atomic_read(&o->od_recs_in_log);
	CWARN("%s: %ld bytes/rec ave\n", o->od_svname, ave);
	OBD_ALLOC(buf, 512);
	for (i = 0, j = 0; i < OUT_LAST; i++) {
		if (atomic_read(&o->od_updates_by_type[i]) == 0)
			continue;
		j += snprintf(buf + j, 512 - j, "  %s: %d(%db)",
			      update_op_str(i),
			      atomic_read(&o->od_updates_by_type[i]),
			      (int)(atomic_read(&o->od_bytes_by_type[i]) /
				      atomic_read(&o->od_updates_by_type[i])));
	}
	CWARN("%s: %s\n", o->od_svname, buf);
	OBD_FREE(buf, 512);
}

void osd_zil_fini(struct osd_device *o)
{
	if (o->od_zilog == NULL)
		return;

	osd_zil_dump_stats(o);
}

static void osd_zil_commit_cb(void *arg)
{
	itx_t *itx = (itx_t *)arg;
	struct osd_device *osd = itx->itx_private;
	atomic_dec(&osd->od_recs_to_write);

}

static void osd_zil_count_updates(struct osd_device *osd,
				  struct osd_thandle *oh)
{
	int i, j, total = 0;

	for (i = 0; i < oh->ot_update->ourq_count; i++) {
		struct object_update		*update;
		size_t				 size;
		int				 sum;

		update = object_update_request_get(oh->ot_update,
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
		total += sum;
		atomic_add(sum, &osd->od_bytes_by_type[update->ou_type]);
	}

	if (total <= 4096 || 1)
		return;

	if (oh->ot_update->ourq_count == 1) {
		struct object_update		*update;
		update = object_update_request_get(oh->ot_update, 0, NULL);
		if (update->ou_type == OUT_WRITE) {
			/* this is a known use case: llog cancel
			 * is updating the llog's header which is 8k */
			return;
		}
	}

	/* wanna know details for the big transactions */
	printk(KERN_INFO "wow %u bytes:", total);
	for (i = 0; i < oh->ot_update->ourq_count; i++) {
		struct object_update		*update;
		size_t				 size;
		int				 sum;

		update = object_update_request_get(oh->ot_update, i, NULL);

		sum = offsetof(struct object_update, ou_params[0]);
		for (j = 0; j < update->ou_params_count; j++) {
			size = 0;
			object_update_param_get(update, j, &size);
			sum += size;
		}
		printk(" %s:%u", update_op_str(update->ou_type), sum);
	}
	printk("\n");
}

void osd_zil_make_itx(struct osd_thandle *oh)
{
	struct osd_device	*osd = osd_dt_dev(oh->ot_super.th_dev);
	itx_t			*itx;
	struct lr_update	*lr;
	int			 len;
	char			*buf;

	if (oh->ot_update == NULL)
		return;
	if (osd->od_zilog == NULL)
		return;
	if (unlikely(oh->ot_update->ourq_count == 0))
		return;

	osd_zil_count_updates(osd, oh);

	len = object_update_request_size(oh->ot_update);

	itx = zil_itx_create(TX_SYMLINK, sizeof(*lr) + len);
	if (itx == NULL) {
		CERROR("can't create itx with %d bytes\n", len);
		return;
	}

	lr = (struct lr_update *)&itx->itx_lr;
	buf = (char *)(lr + 1);
	memcpy(buf, oh->ot_update, len);

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
}

/*
 * used to log OI creation which we need at ZIL replay
 * and do not depend on FLDB
 */
void osd_zil_log_create(struct osd_device *osd, uint64_t parent,
			const char *name, uint64_t child, dmu_tx_t *tx)
{
	itx_t			*itx;
	lr_create_t		*lr;
	int			 len;
	char			*buf;

	if (osd->od_zilog == NULL || zil_replaying(osd->od_zilog, tx))
		return;

	len = strlen(name) + 1;
	itx = zil_itx_create(TX_CREATE, sizeof(*lr) + len);
	if (itx == NULL) {
		CERROR("%s: can't create itx with %d bytes\n",
		       osd->od_svname, len);
		return;
	}

	lr = (lr_create_t *)&itx->itx_lr;
	lr->lr_doid = parent;
	lr->lr_foid = child;
	buf = (char *)(lr + 1);
	memcpy(buf, name, len);

	itx->itx_callback = osd_zil_commit_cb;
	itx->itx_callback_data = itx;

	itx->itx_private = osd;
	itx->itx_sync = 1;

	atomic_inc(&osd->od_recs_to_write);

	LASSERT(osd->od_zilog != NULL);
	(void) zil_itx_assign(osd->od_zilog, itx, tx);
}
struct osd_range_lock {
	struct list_head	orl_list;
	loff_t			orl_start;
	loff_t			orl_end;
	wait_queue_head_t	orl_wait;
	struct osd_object      *orl_obj; /* for checks only */
	cfs_time_t		orl_when;
	int			orl_rw:1,
				orl_granted:1;
};

static long interval_to_usec(cfs_time_t start, cfs_time_t end)
{
	struct timeval val;

	cfs_duration_usec(cfs_time_sub(end, start), &val);
	return val.tv_sec * 1000000 + val.tv_usec;
}

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

	l->orl_start = offset;
	l->orl_end = offset + size - 1;
	l->orl_rw = rw;
	init_waitqueue_head(&l->orl_wait);
	l->orl_obj = o;

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
		l->orl_when = cfs_time_current();
		spin_unlock(&h->ord_range_lock);

		/* XXX: need a memory barrier? */
		wait_event(l->orl_wait, l->orl_granted != 0);
	}

	return l;
}

void osd_unlock_range(struct osd_object *o, struct osd_range_lock *l)
{
	struct osd_device *osd = osd_obj2dev(o);
	cfs_time_t now = cfs_time_current();
	struct osd_range_lock	*t, *tmp;
	struct osd_range_head	*h;
	int compat;

	LASSERT(!IS_ERR(l));
	LASSERT(l->orl_obj == o);
	LASSERT(!list_empty(&l->orl_list));
	LASSERT(l->orl_granted != 0);

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
			lprocfs_counter_add(osd->od_stats,
					    LPROC_OSD_ZIL_BLOCKED,
					    interval_to_usec(t->orl_when, now));
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
	int			 rc = 0;
	ENTRY;

	LASSERT(osd->od_zilog != NULL);

	if (atomic_read(&osd->od_recs_to_write) == 0)
		return;

	/* make sure all in-flight txg's are done
	 * this is needed as transaction in ZIL can
	 * depend on in-flight transactions */
	down_write(&osd->od_tx_barrier);

	/* put a special barrier record to ZIL */
	tx = dmu_tx_create(osd->od_os);
	if (unlikely(tx == NULL)) {
		CERROR("%s: can't add a barrier\n", osd->od_svname);
		GOTO(out, rc);
	}

	rc = -dmu_tx_assign(tx, TXG_WAIT);
	if (rc < 0) {
		CERROR("%s: can't add a barrier\n", osd->od_svname);
		dmu_tx_abort(tx);
	}

	itx = zil_itx_create(TX_REMOVE, sizeof(*lr));
	if (unlikely(itx == NULL)) {
		CERROR("%s: can't add a barrier\n", osd->od_svname);
		GOTO(out, rc);
	}

	itx->itx_callback = osd_zil_commit_cb;
	itx->itx_callback_data = itx;

	itx->itx_private = osd;
	itx->itx_sync = 1;

	atomic_inc(&osd->od_recs_to_write);
	lprocfs_counter_add(osd->od_stats, LPROC_OSD_ZIL_RECORDS,
			    itx->itx_lr.lrc_reclen);

	zil_itx_assign(osd->od_zilog, itx, tx);

	dmu_tx_commit(tx);

out:
	/* now we can enable new transactions */
	up_write(&osd->od_tx_barrier);

	zil_commit(osd->od_zilog, 0);

	EXIT;
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
	if (unlikely(zl == NULL))
		return NULL;

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
	rc = -dsl_dataset_snapshot(snaps, NULL, NULL);
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
			CERROR("%s: remount failed after rollback: rc = %d\n",
			       o->od_svname, rc);
	} else {
		CERROR("%s: rollback failed: rc = %d\n", o->od_svname, rc);
	}

	if (rc == 0)
		LCONSOLE_INFO("%s: rollback to found snapshot\n", o->od_svname);

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
				 struct osd_replay_state *ors)
{
	struct zil_tx	  *ztx, *tmp;
	struct zil_update *zup, *tmp2;

	list_for_each_entry_safe(ztx, tmp, &ors->ors_zil_txs, zt_list) {
		list_for_each_entry_safe(zup, tmp2, &ztx->zt_updates, zu_list) {
			if (zup->zu_obj != NULL)
				lu_object_put(env, &zup->zu_obj->do_lu);
			list_del(&zup->zu_on_zos);
			OBD_FREE(zup->zu_update, zup->zu_update_size);
			OBD_FREE_PTR(zup);
		}
		OBD_FREE_PTR(ztx);
		ors->ors_discarded++;
	}
}

static int osd_replay_log(struct osd_replay_state *ors)
{
	struct lu_attr		*attr = &osd_oti_get(ors->ors_env)->oti_la;
	struct osd_device	*osd = ors->ors_osd;
	struct zpl_direntry	 zde;
	struct lu_buf		 lb = { 0 };
	uint64_t		 oid, offset;
	uint32_t		 reclen;
	int			 rc;
	ENTRY;

	rc = -zap_lookup(osd->od_os, osd->od_root, "log", 8, 1, (void *)&zde);
	if (rc < 0) {
		/* this shouldn't happen actually: we have to interrupt replay
		 * before snapshot creation if it's nothing to replay */
		CERROR("%s: can't lookup log: rc = %d\n", osd->od_svname, rc);
		RETURN(rc);
	}
	oid = zde.zde_dnode;

	/* get the size */
	rc = __osd_object_attr_get(ors->ors_env, ors->ors_osd, oid, attr);
	if (rc < 0) {
		CERROR("%s: can't getattr: rc = %d\n", osd->od_svname, rc);
		RETURN(rc);
	}
	CDEBUG(D_OTHER, "found log with %u bytes\n", (unsigned)attr->la_size);

	ors->ors_hash = cfs_hash_create("obj", HASH_POOLS_CUR_BITS,
				  HASH_POOLS_MAX_BITS, HASH_POOLS_BKT_BITS, 0,
				  CFS_HASH_MIN_THETA, CFS_HASH_MAX_THETA,
				  &zil_object_hash_ops, CFS_HASH_DEFAULT);
	if (ors->ors_hash == NULL) {
		CERROR("%s: cant allocate hash\n", osd->od_svname);
		GOTO(out, rc = -ENOMEM);
	}

	ors->ors_zil_dev = osd_zil_dev_prepare(ors->ors_env, osd);
	if (ors->ors_zil_dev == NULL) {
		CERROR("%s: can't allocate device\n", osd->od_svname);
		GOTO(out, rc = -ENOMEM);
	}

	/* scan the log */
	offset = 0;
	while (offset < attr->la_size) {
		/* read the header */
		rc = dmu_read(osd->od_os, oid, offset, 4, &reclen, 0);
		if (rc < 0) {
			CERROR("%s: can't read: rc = %d\n", osd->od_svname, rc);
			break;
		}
		offset += 4;

		if (reclen == 0) {
			/* this is a sync barrier, we can apply all the
			 * previous updates */
			rc = osd_zil_apply_updates(ors);
			if (rc != 0) {
				CERROR("%s: can't apply: rc = %d\n",
				       osd->od_svname, rc);
				break;
			}
			continue;
		}

		lu_buf_check_and_alloc(&lb, reclen);
		if (lb.lb_buf == NULL) {
			rc = -ENOMEM;
			break;
		}

		/* read the record */
		rc = dmu_read(osd->od_os, oid, offset, reclen, lb.lb_buf, 0);
		if (rc < 0) {
			CERROR("%s: can't read: rc = %d\n", osd->od_svname, rc);
			break;
		}

		rc = osd_zil_parse_update(ors, lb.lb_buf);
		if (rc != 0) {
			CERROR("%s: can't parse: rc = %d\n",
			       osd->od_svname, rc);
			break;
		}

		offset += reclen;
	}

	/* discard records with no final barrier or
	 * left for another reason (error?) */
	osd_zil_free_records(ors->ors_env, ors);

	cfs_hash_for_each_safe(ors->ors_hash, osd_zil_release_state, NULL);
	lu_site_purge(ors->ors_env,
		      ors->ors_zil_dev->zl_top_dev.dd_lu_dev.ld_site, ~0);

out:
	lu_buf_free(&lb);
	if (ors->ors_zil_dev != NULL)
		osd_zil_dev_fini(ors->ors_env, ors->ors_zil_dev);
	if (ors->ors_hash != NULL)
		cfs_hash_putref(ors->ors_hash);

	RETURN(rc);
}

static int osd_destroy_log(struct osd_replay_state *ors, const char *name)
{
	struct osd_device	*osd = ors->ors_osd;
	struct zpl_direntry	 zde;
	dmu_tx_t		*tx;
	int			 rc;

	rc = -zap_lookup(osd->od_os, osd->od_root, name, 8, 1, (void *)&zde);
	if (rc == -ENOENT)
		return 0;
	if (rc < 0)
		return rc;

	tx = dmu_tx_create(osd->od_os);
	if (tx == NULL)
		return -ENOMEM;

	dmu_tx_hold_zap(tx, osd->od_root, FALSE, name);
	/* XXX: probably we have to do this in few
	 *	transactions if the object is large */
	dmu_tx_hold_free(tx, zde.zde_dnode, 0, DMU_OBJECT_END);

	rc = -dmu_tx_assign(tx, TXG_WAIT);
	if (rc) {
		dmu_tx_abort(tx);
		return rc;
	}
	rc = -zap_remove(osd->od_os, osd->od_root, name, tx);
	if (rc == 0)
		rc = -dmu_object_free(osd->od_os, zde.zde_dnode, tx);

	dmu_tx_commit(tx);

	return rc;
}

int osd_zil_replay(const struct lu_env *env, struct osd_device *o)
{
	struct osd_replay_state	 ors;
	dsl_pool_t		*dp = dmu_objset_pool(o->od_os);
	char			*snapname = NULL;
	struct zpl_direntry	 zde;
	int			 rc;
	cfs_time_t		 start = cfs_time_current();
	ENTRY;

	memset(&ors, 0, sizeof(ors));
	ors.ors_env = env;
	ors.ors_osd = o;
	INIT_LIST_HEAD(&ors.ors_zil_txs);

	/*
	 * copy ZIL records to a stable regular file which will be a
	 * subject to snapshot/rollback operations. this is needed
	 * because ZIL records can't be retained over snapshot/rollback.
	 */
	zil_replay(o->od_os, &ors, osd_zil_replay_vector);
	if (ors.ors_result != 0) {
		LCONSOLE_ERROR("%s: can't replay ZIL: rc = %d\n",
			      o->od_svname, ors.ors_result);
		RETURN(ors.ors_result);
	}

	if (o->od_zilog->zl_parse_lr_count > 0)
		LCONSOLE_INFO("%s: ZIL - %llu blocks, %llu recs, synced %llu\n",
			      o->od_svname, o->od_zilog->zl_parse_blk_count,
			      o->od_zilog->zl_parse_lr_count,
			      dp->dp_tx.tx_synced_txg);

	/* something could get from ZIL into the stable log at previous try
	 * to mount, so we can't rely on in-core counters, instead we have to
	 * check the stable log */
	rc = -zap_lookup(o->od_os, o->od_root, "log", 8, 1, (void *)&zde);
	if (rc == -ENOENT) {
		/* nothing to replay */
		RETURN(0);
	}
	if (rc < 0) {
		CERROR("%s: can't lookup log: rc = %d\n", o->od_svname, rc);
		RETURN(rc);
	}

	rc = strlen(o->od_mntdev) + strlen("@rollback");
	OBD_ALLOC(snapname, rc + 1);
	if (snapname == NULL)
		RETURN(-ENOMEM);
	snprintf(snapname, rc + 1, "%s@rollback", o->od_mntdev);

	/*
	 * create a snapshot to have a point for rollback.
	 */
	rc = osd_make_snapshot(snapname);
	if (rc == -EEXIST) {
		/* the snapshot exists already. this means we tried to replay
		 * but didn't complete for a reason. let's rollback and try
		 * to replay again */
		rc = osd_rollback(env, o);
		if (rc < 0) {
			/* XXX: how to recover from this?
			 * have a manual option to discard the log, etc? */
			GOTO(out, rc);
		}
	} else if (rc < 0) {
		/* XXX: how to recover from this?
		 * have a manual option to discard the log, etc? */
		CERROR("%s: can't make a snapshot: rc = %d\n",
		       o->od_svname, rc);
		GOTO(out, rc);
	}

	/* replay from the log */
	rc = osd_replay_log(&ors);
	if (rc != 0) {
		CERROR("%s: can't replay log: rc = %d\n", o->od_svname, rc);
		GOTO(out, rc);
	}

	/* destroy the logs */
	rc = osd_destroy_log(&ors, "log");
	if (rc != 0) {
		CERROR("%s: can't destroy log: rc = %d\n", o->od_svname, rc);
		GOTO(out, rc);
	}
	rc = osd_destroy_log(&ors, "dlog");
	if (rc != 0) {
		CERROR("%s: can't destroy dlog: rc = %d\n", o->od_svname, rc);
		GOTO(out, rc);
	}

	/* now we can destroy the snapshot */
	rc = -dsl_destroy_snapshot(snapname, B_FALSE);
	if (rc != 0) {
		CERROR("%s: can't kill snapshot: rc = %d\n", o->od_svname, rc);
		GOTO(out, rc);
	}

	start = cfs_time_sub(cfs_time_current(), start);
	LCONSOLE_INFO("%s: LOG - %u in %u/%u tx in %u groups, %d discarded, "
		      "%u in memory, took %us\n", o->od_svname,
		      ors.ors_update_nr, ors.ors_tx_nr, ors.ors_iterations,
		      ors.ors_barriers, ors.ors_discarded,
		      ors.ors_max_mem_allocated,
		      (unsigned)cfs_duration_sec(start));

out:
	if (snapname != NULL)
		OBD_FREE(snapname, strlen(snapname) + 1);

	RETURN(rc);
}

static struct dt_object *zil_locate(const struct lu_env *env,
				    struct osd_replay_state *ors,
				    const struct lu_fid *fid)
{
	struct zil_device *zd = ors->ors_zil_dev;
	struct lu_object  *lo;

	lo = lu_object_find_at(env, &zd->zl_top_dev.dd_lu_dev, fid, NULL);
	if (IS_ERR(lo))
		return ERR_PTR(PTR_ERR(lo));

	return container_of0(lu_object_next(lo), struct dt_object, do_lu);
}

static unsigned
zfs_object_hash(struct cfs_hash *hs, const void *key, unsigned mask)
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
zfs_object_export_get(struct cfs_hash *hs, struct hlist_node *hnode)
{
}

static void
zfs_object_export_put_locked(struct cfs_hash *hs, struct hlist_node *hnode)
{
}

static struct cfs_hash_ops zil_object_hash_ops = {
	.hs_hash	= zfs_object_hash,
	.hs_key		= zfs_object_key,
	.hs_keycmp	= zfs_object_keycmp,
	.hs_object	= zfs_object_export_object,
	.hs_get		= zfs_object_export_get,
	.hs_put_locked	= zfs_object_export_put_locked,
};

struct object_update *update_buffer_get_update(struct object_update_request *r,
					       unsigned int index)
{
	void	*ptr;
	int	i;

	if (index > r->ourq_count)
		return NULL;

	ptr = &r->ourq_updates[0];
	for (i = 0; i < index; i++)
		ptr += object_update_size(ptr);

	return ptr;
}

#define UPDATE_BUFFER_SIZE_MAX	(256 * 4096)  /*  1M update size now */

int extend_update_buffer(const struct lu_env *env, struct osd_thandle *oh)
{
	struct osd_thread_info		*info = osd_oti_get(env);
	size_t				 new_size;
	int				 rc;

	/* enlarge object update request size */
	new_size = oh->ot_update_max_size + 2048;
	if (new_size > UPDATE_BUFFER_SIZE_MAX) {
		LBUG();
		return -E2BIG;
	}

	rc = lu_buf_check_and_grow(&info->oti_update_lb, new_size);
	if (unlikely(rc < 0))
		return rc;

	oh->ot_update = info->oti_update_lb.lb_buf;
	oh->ot_update_max_size = info->oti_update_lb.lb_len;

	lprocfs_counter_add(osd_dt_dev(oh->ot_super.th_dev)->od_stats,
			    LPROC_OSD_ZIL_REALLOC, 1);
	return 0;
}

