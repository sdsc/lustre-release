/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright  2009 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/lod/lod_qos.c
 *
 */

#define DEBUG_SUBSYSTEM S_LOV

#include <asm/div64.h>
#include <libcfs/libcfs.h>
#include <obd_class.h>
#include <lustre/lustre_idl.h>
#include "lod_internal.h"

/*
 * force QoS policy (not RR) to be used for testing purposes
 */
#define FORCE_QOS_

#define D_QOS   D_OTHER

#if 0
#define QOS_DEBUG(fmt, ...)     CDEBUG(D_OTHER, fmt, ## __VA_ARGS__)
#define QOS_CONSOLE(fmt, ...)   LCONSOLE(D_OTHER, fmt, ## __VA_ARGS__)
#else
#define QOS_DEBUG(fmt, ...)
#define QOS_CONSOLE(fmt, ...)
#endif

#define TGT_BAVAIL(i) (OST_TGT(lod,i)->ltd_statfs.os_bavail * \
		       OST_TGT(lod,i)->ltd_statfs.os_bsize)

int qos_add_tgt(struct lod_device *lod, struct lod_tgt_desc *ost_desc)
{
	struct lod_qos_oss *oss = NULL, *temposs;
	struct obd_export  *exp = ost_desc->ltd_exp;
	int		    rc = 0, found = 0;
	cfs_list_t	   *list;
	ENTRY;

	down_write(&lod->lod_qos.lq_rw_sem);
	/*
	 * a bit hacky approach to learn NID of corresponding connection
	 * but there is no official API to access information like this
	 * with OSD API.
	 */
	cfs_list_for_each_entry(oss, &lod->lod_qos.lq_oss_list, lqo_oss_list) {
		if (obd_uuid_equals(&oss->lqo_uuid,
				    &exp->exp_connection->c_remote_uuid)) {
			found++;
			break;
		}
	}

	if (!found) {
		OBD_ALLOC_PTR(oss);
		if (!oss)
			GOTO(out, rc = -ENOMEM);
		memcpy(&oss->lqo_uuid, &exp->exp_connection->c_remote_uuid,
		       sizeof(oss->lqo_uuid));
	} else {
		/* Assume we have to move this one */
		cfs_list_del(&oss->lqo_oss_list);
	}

	oss->lqo_ost_count++;
	ost_desc->ltd_qos.ltq_oss = oss;

	CDEBUG(D_QOS, "add tgt %s to OSS %s (%d OSTs)\n",
	       obd_uuid2str(&ost_desc->ltd_uuid), obd_uuid2str(&oss->lqo_uuid),
	       oss->lqo_ost_count);

	/* Add sorted by # of OSTs.  Find the first entry that we're
	   bigger than... */
	list = &lod->lod_qos.lq_oss_list;
	cfs_list_for_each_entry(temposs, list, lqo_oss_list) {
		if (oss->lqo_ost_count > temposs->lqo_ost_count)
			break;
	}
	/* ...and add before it.  If we're the first or smallest, temposs
	   points to the list head, and we add to the end. */
	cfs_list_add_tail(&oss->lqo_oss_list, &temposs->lqo_oss_list);

	lod->lod_qos.lq_dirty = 1;
	lod->lod_qos.lq_rr.lqr_dirty = 1;

out:
	up_write(&lod->lod_qos.lq_rw_sem);
	RETURN(rc);
}

int qos_del_tgt(struct lod_device *lod, struct lod_tgt_desc *ost_desc)
{
	struct lod_qos_oss *oss;
	int                 rc = 0;
	ENTRY;

	down_write(&lod->lod_qos.lq_rw_sem);
	oss = ost_desc->ltd_qos.ltq_oss;
	if (!oss)
		GOTO(out, rc = -ENOENT);

	oss->lqo_ost_count--;
	if (oss->lqo_ost_count == 0) {
		CDEBUG(D_QOS, "removing OSS %s\n",
		       obd_uuid2str(&oss->lqo_uuid));
		cfs_list_del(&oss->lqo_oss_list);
		ost_desc->ltd_qos.ltq_oss = NULL;
		OBD_FREE_PTR(oss);
	}

	lod->lod_qos.lq_dirty = 1;
	lod->lod_qos.lq_rr.lqr_dirty = 1;
out:
	up_write(&lod->lod_qos.lq_rw_sem);
	RETURN(rc);
}

/* Calculate the weight for an OST. */
static void lod_qos_calc_weight(struct lod_device *m, __u32 ost_idx)
{
	struct lod_ost_desc *ost = OST_TGT(m, ost_idx);
	struct obd_statfs *sfs = &ost->ltd_statfs;
	__u64 weight;
	__u64 age;

	/* Decrease penalties based on virtual time of last use. */
	age = atomic_read(&m->lod_create_counter) - ost->ltd_last_used + 1;
	ost->ltd_qos.ltq_bavail_penalty /= age;

	/* TODO: Involve more factors in weighting. */
	weight = m->lod_qos.lq_prio_free *
		(sfs->os_bavail - ost->ltd_qos.ltq_bavail_penalty);

	ost->ltd_qos.ltq_weight = weight;
}

/* Modify penalties for an OST after it has been selected for stripe
 * allocation. */
static void lod_qos_used(struct lod_device *m, __u32 ost_idx)
{
	struct lod_ost_desc *ost = OST_TGT(m, ost_idx);
	struct obd_statfs *sfs = &ost->ltd_statfs;
	__u64 counter;

	counter = atomic_inc_return(&m->lod_create_counter);
	ost->ltd_last_used = counter;

	/* TODO: Needs more penalties, verification that increments
	 *       make any sense. */
	ost->ltd_qos.ltq_bavail_penalty += sfs->os_bfree - sfs->os_bavail;
}

static int lod_statfs_and_check(const struct lu_env *env, struct lod_device *d,
				int index, struct obd_statfs *sfs)
{
	struct lod_tgt_desc *ost;
	int		     rc;

	LASSERT(d);
	ost = OST_TGT(d,index);
	LASSERT(ost);

	rc = dt_statfs(env, ost->ltd_ost, sfs);
	if (rc && rc != -ENOTCONN)
		CERROR("%s: statfs: rc = %d\n", lod2obd(d)->obd_name, rc);

	/* If the OST is readonly then we can't allocate objects there */
	if (sfs->os_state & OS_STATE_READONLY)
		rc = -EROFS;

	/* check whether device has changed state (active, inactive) */
	if (rc != 0 && ost->ltd_active) {
		/* turned inactive? */
		spin_lock(&d->lod_desc_lock);
		if (ost->ltd_active) {
			ost->ltd_active = 0;
			LASSERT(d->lod_desc.ld_active_tgt_count > 0);
			d->lod_desc.ld_active_tgt_count--;
			d->lod_qos.lq_dirty = 1;
			d->lod_qos.lq_rr.lqr_dirty = 1;
			CDEBUG(D_CONFIG, "%s: turns inactive\n",
			       ost->ltd_exp->exp_obd->obd_name);
		}
		spin_unlock(&d->lod_desc_lock);
	} else if (rc == 0 && ost->ltd_active == 0) {
		/* turned active? */
		LASSERTF(d->lod_desc.ld_active_tgt_count < d->lod_ostnr,
			 "active tgt count %d, ost nr %d\n",
			 d->lod_desc.ld_active_tgt_count, d->lod_ostnr);
		spin_lock(&d->lod_desc_lock);
		if (ost->ltd_active == 0) {
			ost->ltd_active = 1;
			d->lod_desc.ld_active_tgt_count++;
			d->lod_qos.lq_dirty = 1;
			d->lod_qos.lq_rr.lqr_dirty = 1;
			CDEBUG(D_CONFIG, "%s: turns active\n",
			       ost->ltd_exp->exp_obd->obd_name);
		}
		spin_unlock(&d->lod_desc_lock);
	}

	RETURN(rc);
}

static void lod_qos_statfs_update(const struct lu_env *env,
				  struct lod_device *lod)
{
	struct obd_device	*obd = lod2obd(lod);
	struct ost_pool		*osts = &(lod->lod_pool_info);
	int			 i, idx, rc = 0;
	__u64			 max_age, avail, avg_weight = 0;
	ENTRY;

	max_age = cfs_time_shift_64(-2 * lod->lod_desc.ld_qos_maxage);

	if (cfs_time_beforeq_64(max_age, obd->obd_osfs_age))
		/* statfs data are quite recent, don't need to refresh it */
		RETURN_EXIT;

	down_write(&lod->lod_qos.lq_rw_sem);
	if (cfs_time_beforeq_64(max_age, obd->obd_osfs_age))
		GOTO(out, rc = 0);

	for (i = 0; i < osts->op_count; i++) {
		idx = osts->op_array[i];
		avail = OST_TGT(lod,idx)->ltd_statfs.os_bavail;
		rc = lod_statfs_and_check(env, lod, idx,
					  &OST_TGT(lod,idx)->ltd_statfs);
		if (rc)
			break;
		if (OST_TGT(lod, idx)->ltd_statfs.os_bavail != avail) {
			/* recalculate weights */
			/* XXX is this the best place to call this? */
			lod_qos_calc_weight(lod, idx);
		}
		avg_weight += OST_TGT(lod, idx)->ltd_qos.ltq_weight;
	}
	obd->obd_osfs_age = cfs_time_current_64();
	/*
	 * XXX Since adding the op_avg_weight field seems to cause some tests
	 *     to crash for a reason unknown to me, we don't actually store
	 *     the average weight, only print it out
	 */
#if 0
	osts->op_avg_weight = avg_weight / osts->op_count;
	QOS_DEBUG("pool average weight is "LPX64"\n", osts->op_avg_weight);
#else
	QOS_DEBUG("pool average weight would be "LPX64"\n",
		  avg_weight / osts->op_count);
#endif

out:
	up_write(&lod->lod_qos.lq_rw_sem);
	EXIT;
}

#define LOV_QOS_EMPTY ((__u32)-1)
/* compute optimal round-robin order, based on OSTs per OSS */
static int lod_qos_calc_rr(struct lod_device *lod, struct ost_pool *src_pool,
			   struct lod_qos_rr *lqr)
{
	struct lod_qos_oss  *oss;
	struct lod_tgt_desc *ost;
	unsigned placed, real_count;
	int i, rc;
	ENTRY;

	if (!lqr->lqr_dirty) {
		LASSERT(lqr->lqr_pool.op_size);
		RETURN(0);
	}

	/* Do actual allocation. */
	down_write(&lod->lod_qos.lq_rw_sem);

	/*
	 * Check again. While we were sleeping on @lq_rw_sem something could
	 * change.
	 */
	if (!lqr->lqr_dirty) {
		LASSERT(lqr->lqr_pool.op_size);
		up_write(&lod->lod_qos.lq_rw_sem);
		RETURN(0);
	}

	real_count = src_pool->op_count;

	/* Zero the pool array */
	/* alloc_rr is holding a read lock on the pool, so nobody is adding/
	   deleting from the pool. The lq_rw_sem insures that nobody else
	   is reading. */
	lqr->lqr_pool.op_count = real_count;
	rc = lod_ost_pool_extend(&lqr->lqr_pool, real_count);
	if (rc) {
		up_write(&lod->lod_qos.lq_rw_sem);
		RETURN(rc);
	}
	for (i = 0; i < lqr->lqr_pool.op_count; i++)
		lqr->lqr_pool.op_array[i] = LOV_QOS_EMPTY;

	/* Place all the OSTs from 1 OSS at the same time. */
	placed = 0;
	cfs_list_for_each_entry(oss, &lod->lod_qos.lq_oss_list, lqo_oss_list) {
		int j = 0;

		for (i = 0; i < lqr->lqr_pool.op_count; i++) {
			int next;

			if (!cfs_bitmap_check(lod->lod_ost_bitmap,
						src_pool->op_array[i]))
				continue;

			ost = OST_TGT(lod,src_pool->op_array[i]);
			LASSERT(ost && ost->ltd_ost);
			if (ost->ltd_qos.ltq_oss != oss)
				continue;

			/* Evenly space these OSTs across arrayspace */
			next = j * lqr->lqr_pool.op_count / oss->lqo_ost_count;
			while (lqr->lqr_pool.op_array[next] != LOV_QOS_EMPTY)
				next = (next + 1) % lqr->lqr_pool.op_count;

			lqr->lqr_pool.op_array[next] = src_pool->op_array[i];
			j++;
			placed++;
		}
	}

	lqr->lqr_dirty = 0;
	up_write(&lod->lod_qos.lq_rw_sem);

	if (placed != real_count) {
		/* This should never happen */
		LCONSOLE_ERROR_MSG(0x14e, "Failed to place all OSTs in the "
				   "round-robin list (%d of %d).\n",
				   placed, real_count);
		for (i = 0; i < lqr->lqr_pool.op_count; i++) {
			LCONSOLE(D_WARNING, "rr #%d ost idx=%d\n", i,
				 lqr->lqr_pool.op_array[i]);
		}
		lqr->lqr_dirty = 1;
		RETURN(-EAGAIN);
	}

#if 0
	for (i = 0; i < lqr->lqr_pool.op_count; i++)
		QOS_CONSOLE("rr #%d ost idx=%d\n", i, lqr->lqr_pool.op_array[i]);
#endif

	RETURN(0);
}

/**
 * A helper function to:
 *   create in-core lu object on the specified OSP
 *   declare creation of the object
 * IMPORTANT: at this stage object is anonymouos - it has no fid assigned
 *            this is a workaround till we have natural FIDs on OST
 *
 *            at this point we want to declare (reserve) object for us as
 *            we can't block at execution (when create method is called).
 *            otherwise we'd block whole transaction batch
 */
static struct dt_object *lod_qos_declare_object_on(const struct lu_env *env,
						   struct lod_device *d,
						   int ost_idx,
						   struct thandle *th)
{
	struct lod_tgt_desc *ost;
	struct lu_object *o, *n;
	struct lu_device *nd;
	struct dt_object *dt;
	int               rc;
	ENTRY;

	LASSERT(d);
	LASSERT(ost_idx >= 0);
	LASSERT(ost_idx < d->lod_osts_size);
	ost = OST_TGT(d,ost_idx);
	LASSERT(ost);
	LASSERT(ost->ltd_ost);

	nd = &ost->ltd_ost->dd_lu_dev;

	/*
	 * allocate anonymous object with zero fid, real fid
	 * will be assigned by OSP within transaction
	 * XXX: to be fixed with fully-functional OST fids
	 */
	o = lu_object_anon(env, nd, NULL);
	if (IS_ERR(o))
		GOTO(out, dt = ERR_PTR(PTR_ERR(o)));

	n = lu_object_locate(o->lo_header, nd->ld_type);
	if (unlikely(n == NULL)) {
		CERROR("can't find slice\n");
		lu_object_put(env, o);
		GOTO(out, dt = ERR_PTR(-EINVAL));
	}

	dt = container_of(n, struct dt_object, do_lu);

	rc = dt_declare_create(env, dt, NULL, NULL, NULL, th);
	if (rc) {
		CDEBUG(D_OTHER, "can't declare creation on #%u: %d\n",
		       ost_idx, rc);
		lu_object_put(env, o);
		dt = ERR_PTR(rc);
	}

out:
	RETURN(dt);
}

static int min_stripe_count(int stripe_cnt, int flags)
{
	return (flags & LOV_USES_DEFAULT_STRIPE ?
			stripe_cnt - (stripe_cnt / 4) : stripe_cnt);
}

#define LOV_CREATE_RESEED_MULT 30
#define LOV_CREATE_RESEED_MIN  2000

static int inline lod_qos_dev_is_full(struct obd_statfs *msfs)
{
	__u64 used;
	int   bs = msfs->os_bsize;

	LASSERT(((bs - 1) & bs) == 0);

	/* the minimum of 0.1% used blocks and 1GB bytes. */
	used = min_t(__u64, (msfs->os_blocks - msfs->os_bfree) >> 10,
			1 << (31 - ffs(bs)));
	return (msfs->os_bavail < used);
}

static inline int lod_qos_ost_in_use_clear(const struct lu_env *env, int stripes)
{
	struct lod_thread_info *info = lod_env_info(env);

	if (info->lti_ea_store_size < sizeof(int) * stripes)
		lod_ea_store_resize(info, stripes * sizeof(int));
	if (info->lti_ea_store_size < sizeof(int) * stripes) {
		CERROR("can't allocate memory for ost-in-use array\n");
		return -ENOMEM;
	}
	memset(info->lti_ea_store, -1, sizeof(int) * stripes);
	return 0;
}

static inline void lod_qos_ost_in_use(const struct lu_env *env, int idx, int ost)
{
	struct lod_thread_info *info = lod_env_info(env);
	int *osts = info->lti_ea_store;

	LASSERT(info->lti_ea_store_size >= idx * sizeof(int));
	osts[idx] = ost;
}

static int lod_qos_is_ost_used(const struct lu_env *env, int ost, int stripes)
{
	struct lod_thread_info *info = lod_env_info(env);
	int *osts = info->lti_ea_store;
	int j;

	for (j = 0; j < stripes; j++) {
		if (osts[j] == ost)
			return 1;
	}
	return 0;
}

/* Allocate objects on OSTs using a weighted round-robin algorithm */
static int lod_alloc_qos(const struct lu_env *env, struct lod_object *lo,
			 struct dt_object **stripe, int flags,
			 struct thandle *th)
{
	struct lod_device *m = lu2lod_dev(lo->ldo_obj.do_lu.lo_dev);
	struct obd_statfs *sfs = &lod_env_info(env)->lti_osfs;
	struct pool_desc  *pool = NULL;
	struct ost_pool   *osts;
	struct lod_qos_rr *lqr;
	struct dt_object  *o;
	unsigned	   array_idx;
	int		   i, rc;
	int		   ost_start_idx_temp;
	int		   speed = 0;
	int		   stripe_idx = 0;
	int		   stripe_cnt = lo->ldo_stripenr;
	int		   stripe_cnt_min = min_stripe_count(stripe_cnt, flags);
	__u32		   ost_idx;
	__u64		   threshold;
	ENTRY;

	if (lo->ldo_pool)
		pool = lod_find_pool(m, lo->ldo_pool);

	if (pool != NULL) {
		down_read(&pool_tgt_rw_sem(pool));
		osts = &(pool->pool_obds);
		lqr = &(pool->pool_rr);
	} else {
		osts = &(m->lod_pool_info);
		lqr = &(m->lod_qos.lq_rr);
	}

	rc = lod_qos_calc_rr(m, osts, lqr);
	if (rc)
		GOTO(out, rc);

	rc = lod_qos_ost_in_use_clear(env, lo->ldo_stripenr);
	if (rc)
		GOTO(out, rc);

	if (--lqr->lqr_start_count <= 0) {
		lqr->lqr_start_idx = cfs_rand() % osts->op_count;
		lqr->lqr_start_count =
			(LOV_CREATE_RESEED_MIN / max(osts->op_count, 1U) +
			 LOV_CREATE_RESEED_MULT) * max(osts->op_count, 1U);
	} else if (stripe_cnt_min >= osts->op_count ||
			lqr->lqr_start_idx > osts->op_count) {
		/* If we have allocated from all of the OSTs, slowly
		 * precess the next start if the OST/stripe count isn't
		 * already doing this for us. */
		lqr->lqr_start_idx %= osts->op_count;
		if (stripe_cnt > 1 && (osts->op_count % stripe_cnt) != 1)
			++lqr->lqr_offset_idx;
	}
	down_read(&m->lod_qos.lq_rw_sem);
	ost_start_idx_temp = lqr->lqr_start_idx;
#if 0
	threshold = osts->op_avg_weight;
#else
	threshold = 0;
#endif

repeat_find:
	array_idx = (lqr->lqr_start_idx + lqr->lqr_offset_idx) %
			osts->op_count;

	QOS_DEBUG("pool '%s' want %d startidx %d startcnt %d offset %d "
		  "active %d count %d arrayidx %d threshold "LPX64"\n",
		  lo->ldo_pool ? lo->ldo_pool : "",
		  stripe_cnt, lqr->lqr_start_idx, lqr->lqr_start_count,
		  lqr->lqr_offset_idx, osts->op_count, osts->op_count,
		  array_idx, threshold);

	for (i = 0; i < osts->op_count && stripe_idx < lo->ldo_stripenr;
	     i++, array_idx = (array_idx + 1) % osts->op_count) {
		++lqr->lqr_start_idx;
		ost_idx = lqr->lqr_pool.op_array[array_idx];

		QOS_DEBUG("#%d strt %d act %d strp %d ary %d idx %d\n",
			  i, lqr->lqr_start_idx, /* XXX: active*/ 0,
			  stripe_idx, array_idx, ost_idx);

		if ((ost_idx == LOV_QOS_EMPTY) ||
		    !cfs_bitmap_check(m->lod_ost_bitmap, ost_idx))
			continue;

		/* Fail Check before osc_precreate() is called
		   so we can only 'fail' single OSC. */
		if (OBD_FAIL_CHECK(OBD_FAIL_MDS_OSC_PRECREATE) && ost_idx == 0)
			continue;

		rc = lod_statfs_and_check(env, m, ost_idx, sfs);
		if (rc) {
			/* this OSP doesn't feel well */
			continue;
		}

		/*
		 * skip full devices
		 */
		if (lod_qos_dev_is_full(sfs)) {
			QOS_DEBUG("#%d is full\n", ost_idx);
			continue;
		}

		/*
		 * We expect number of precreated objects in f_ffree at
		 * the first iteration, skip OSPs with no objects ready
		 */
		if (sfs->os_fprecreated == 0 && speed == 0) {
			QOS_DEBUG("#%d: precreation is empty\n", ost_idx);
			continue;
		}

		/*
		 * try to use another OSP if this one is degraded
		 */
		if (sfs->os_state & OS_STATE_DEGRADED && speed < 2) {
			QOS_DEBUG("#%d: degraded\n", ost_idx);
			continue;
		}

		/*
		 * Skip OST if it doesn't have enough weight.
		 */
		if (OST_TGT(m, ost_idx)->ltd_qos.ltq_weight < threshold) {
			QOS_DEBUG("#%d: weight ("LPX64") under current "
				  "threshold ("LPX64")\n", ost_idx,
				  OST_TGT(m, ost_idx)->ltd_qos.ltq_weight,
				  threshold);
			continue;
		}

		/*
		 * do not put >1 objects on a single OST
		 */
		if (speed && lod_qos_is_ost_used(env, ost_idx, stripe_idx))
			continue;

		o = lod_qos_declare_object_on(env, m, ost_idx, th);
		if (IS_ERR(o)) {
			CDEBUG(D_OTHER, "can't declare new object on #%u: %d\n",
			       ost_idx, (int) PTR_ERR(o));
			rc = PTR_ERR(o);
			continue;
		}

		/*
		 * We've successfuly declared (reserved) an object
		 */
		lod_qos_ost_in_use(env, stripe_idx, ost_idx);
		stripe[stripe_idx] = o;
		stripe_idx++;
		lod_qos_used(m, ost_idx);
		QOS_DEBUG("#%d selected: weight ("LPX64") above current "
			  "threshold ("LPX64")\n", ost_idx,
			  OST_TGT(m, ost_idx)->ltd_qos.ltq_weight, threshold);

	}
	if ((speed < 2) && (stripe_idx < stripe_cnt_min)) {
		/* Try again, allowing slower OSCs */
		speed++;
		lqr->lqr_start_idx = ost_start_idx_temp;
		goto repeat_find;
	}

	if ((threshold > 0) && (stripe_idx < stripe_cnt_min)) {
		/* Try again, allowing lighter OSTs. */
		threshold >>= 1;
		lqr->lqr_start_idx = ost_start_idx_temp;
		goto repeat_find;
	}

	up_read(&m->lod_qos.lq_rw_sem);

	if (stripe_idx) {
		lo->ldo_stripenr = stripe_idx;
		/* at least one stripe is allocated */
		rc = 0;
	} else {
		/* nobody provided us with a single object */
		rc = -ENOSPC;
	}

out:
	if (pool != NULL) {
		up_read(&pool_tgt_rw_sem(pool));
		/* put back ref got by lod_find_pool() */
		lod_pool_putref(pool);
	}

	RETURN(rc);
}

/* alloc objects on osts with specific stripe offset */
static int lod_alloc_specific(const struct lu_env *env, struct lod_object *lo,
			      struct dt_object **stripe, int flags,
			      struct thandle *th)
{
	struct lod_device *m = lu2lod_dev(lo->ldo_obj.do_lu.lo_dev);
	struct obd_statfs *sfs = &lod_env_info(env)->lti_osfs;
	struct dt_object  *o;
	unsigned	   ost_idx, array_idx, ost_count;
	int		   i, rc, stripe_num = 0;
	int		   speed = 0;
	struct pool_desc  *pool = NULL;
	struct ost_pool   *osts;
	ENTRY;

	rc = lod_qos_ost_in_use_clear(env, lo->ldo_stripenr);
	if (rc)
		GOTO(out, rc);

	if (lo->ldo_pool)
		pool = lod_find_pool(m, lo->ldo_pool);

	if (pool != NULL) {
		down_read(&pool_tgt_rw_sem(pool));
		osts = &(pool->pool_obds);
	} else {
		osts = &(m->lod_pool_info);
	}

	ost_count = osts->op_count;

repeat_find:
	/* search loi_ost_idx in ost array */
	array_idx = 0;
	for (i = 0; i < ost_count; i++) {
		if (osts->op_array[i] == lo->ldo_def_stripe_offset) {
			array_idx = i;
			break;
		}
	}
	if (i == ost_count) {
		CERROR("Start index %d not found in pool '%s'\n",
		       lo->ldo_def_stripe_offset,
		       lo->ldo_pool ? lo->ldo_pool : "");
		GOTO(out, rc = -EINVAL);
	}

	for (i = 0; i < ost_count;
			i++, array_idx = (array_idx + 1) % ost_count) {
		ost_idx = osts->op_array[array_idx];

		if (!cfs_bitmap_check(m->lod_ost_bitmap, ost_idx))
			continue;

		/* Fail Check before osc_precreate() is called
		   so we can only 'fail' single OSC. */
		if (OBD_FAIL_CHECK(OBD_FAIL_MDS_OSC_PRECREATE) && ost_idx == 0)
			continue;

		/*
		 * do not put >1 objects on a single OST
		 */
		if (lod_qos_is_ost_used(env, ost_idx, stripe_num))
			continue;

		/* Drop slow OSCs if we can, but not for requested start idx.
		 *
		 * This means "if OSC is slow and it is not the requested
		 * start OST, then it can be skipped, otherwise skip it only
		 * if it is inactive/recovering/out-of-space." */

		rc = lod_statfs_and_check(env, m, ost_idx, sfs);
		if (rc) {
			/* this OSP doesn't feel well */
			continue;
		}

		/*
		 * We expect number of precreated objects in f_ffree at
		 * the first iteration, skip OSPs with no objects ready
		 * don't apply this logic to OST specified with stripe_offset
		 */
		if (i != 0 && sfs->os_fprecreated == 0 && speed == 0)
			continue;

		o = lod_qos_declare_object_on(env, m, ost_idx, th);
		if (IS_ERR(o)) {
			CDEBUG(D_OTHER, "can't declare new object on #%u: %d\n",
			       ost_idx, (int) PTR_ERR(o));
			continue;
		}

		/*
		 * We've successfuly declared (reserved) an object
		 */
		lod_qos_ost_in_use(env, stripe_num, ost_idx);
		stripe[stripe_num] = o;
		stripe_num++;

		/* We have enough stripes */
		if (stripe_num == lo->ldo_stripenr)
			GOTO(out, rc = 0);
	}
	if (speed < 2) {
		/* Try again, allowing slower OSCs */
		speed++;
		goto repeat_find;
	}

	/* If we were passed specific striping params, then a failure to
	 * meet those requirements is an error, since we can't reallocate
	 * that memory (it might be part of a larger array or something).
	 *
	 * We can only get here if lsm_stripe_count was originally > 1.
	 */
	CERROR("can't lstripe objid "DFID": have %d want %u\n",
	       PFID(lu_object_fid(lod2lu_obj(lo))), stripe_num,
	       lo->ldo_stripenr);
	rc = -EFBIG;
out:
	if (pool != NULL) {
		up_read(&pool_tgt_rw_sem(pool));
		/* put back ref got by lod_find_pool() */
		lod_pool_putref(pool);
	}

	RETURN(rc);
}

static inline int lod_qos_is_usable(struct lod_device *lod)
{
#ifdef FORCE_QOS
	/* to be able to debug QoS code */
	return 1;
#endif

	/* Detect -EAGAIN early, before expensive lock is taken. */
	if (!lod->lod_qos.lq_dirty && lod->lod_qos.lq_same_space)
		return 0;

	if (lod->lod_desc.ld_active_tgt_count < 2)
		return 0;

	return 1;
}

/* Find the max stripecount we should use */
static __u16 lod_get_stripecnt(struct lod_device *lod, __u32 magic,
			       __u16 stripe_count)
{
	__u32 max_stripes = LOV_MAX_STRIPE_COUNT_OLD;

	if (!stripe_count)
		stripe_count = lod->lod_desc.ld_default_stripe_count;
	if (stripe_count > lod->lod_desc.ld_active_tgt_count)
		stripe_count = lod->lod_desc.ld_active_tgt_count;
	if (!stripe_count)
		stripe_count = 1;

	/* stripe count is based on whether OSD can handle larger EA sizes */
	if (lod->lod_osd_max_easize > 0)
		max_stripes = lov_mds_md_max_stripe_count(
			lod->lod_osd_max_easize, magic);

	return (stripe_count < max_stripes) ? stripe_count : max_stripes;
}

static int lod_use_defined_striping(const struct lu_env *env,
				    struct lod_object *mo,
				    const struct lu_buf *buf)
{
	struct lov_mds_md_v1   *v1 = buf->lb_buf;
	struct lov_mds_md_v3   *v3 = buf->lb_buf;
	struct lov_ost_data_v1 *objs;
	__u32			magic;
	int			rc = 0;
	ENTRY;

	magic = le32_to_cpu(v1->lmm_magic);
	if (magic == LOV_MAGIC_V1_DEF) {
		magic = LOV_MAGIC_V1;
		objs = &v1->lmm_objects[0];
	} else if (magic == LOV_MAGIC_V3_DEF) {
		magic = LOV_MAGIC_V3;
		objs = &v3->lmm_objects[0];
		lod_object_set_pool(mo, v3->lmm_pool_name);
	} else {
		GOTO(out, rc = -EINVAL);
	}

	mo->ldo_pattern = le32_to_cpu(v1->lmm_pattern);
	mo->ldo_stripe_size = le32_to_cpu(v1->lmm_stripe_size);
	mo->ldo_stripenr = le16_to_cpu(v1->lmm_stripe_count);
	mo->ldo_layout_gen = le16_to_cpu(v1->lmm_layout_gen);

	/* fixup for released file before object initialization */
	if (mo->ldo_pattern & LOV_PATTERN_F_RELEASED) {
		mo->ldo_released_stripenr = mo->ldo_stripenr;
		mo->ldo_stripenr = 0;
	}

	LASSERT(buf->lb_len >= lov_mds_md_size(mo->ldo_stripenr, magic));

	if (mo->ldo_stripenr > 0)
		rc = lod_initialize_objects(env, mo, objs);

out:
	RETURN(rc);
}

static int lod_qos_parse_config(const struct lu_env *env,
				struct lod_object *lo,
				const struct lu_buf *buf)
{
	struct lod_device     *d = lu2lod_dev(lod2lu_obj(lo)->lo_dev);
	struct lov_user_md_v1 *v1 = NULL;
	struct lov_user_md_v3 *v3 = NULL;
	struct pool_desc      *pool;
	__u32		       magic;
	int		       rc;
	ENTRY;

	if (buf == NULL || buf->lb_buf == NULL || buf->lb_len == 0)
		RETURN(0);

	v1 = buf->lb_buf;
	magic = v1->lmm_magic;

	if (magic == __swab32(LOV_USER_MAGIC_V1)) {
		lustre_swab_lov_user_md_v1(v1);
		magic = v1->lmm_magic;
	} else if (magic == __swab32(LOV_USER_MAGIC_V3)) {
		v3 = buf->lb_buf;
		lustre_swab_lov_user_md_v3(v3);
		magic = v3->lmm_magic;
	}

	if (unlikely(magic != LOV_MAGIC_V1 && magic != LOV_MAGIC_V3)) {
		/* try to use as fully defined striping */
		rc = lod_use_defined_striping(env, lo, buf);
		RETURN(rc);
	}

	if (unlikely(buf->lb_len < sizeof(*v1))) {
		CERROR("wrong size: %u\n", (unsigned) buf->lb_len);
		RETURN(-EINVAL);
	}

	v1->lmm_magic = magic;
	if (v1->lmm_pattern == 0)
		v1->lmm_pattern = LOV_PATTERN_RAID0;
	if (lov_pattern(v1->lmm_pattern) != LOV_PATTERN_RAID0) {
		CERROR("invalid pattern: %x\n", v1->lmm_pattern);
		RETURN(-EINVAL);
	}
	lo->ldo_pattern = v1->lmm_pattern;

	if (v1->lmm_stripe_size)
		lo->ldo_stripe_size = v1->lmm_stripe_size;
	if (lo->ldo_stripe_size & (LOV_MIN_STRIPE_SIZE - 1))
		lo->ldo_stripe_size = LOV_MIN_STRIPE_SIZE;

	if (v1->lmm_stripe_count)
		lo->ldo_stripenr = v1->lmm_stripe_count;

	if ((v1->lmm_stripe_offset >= d->lod_desc.ld_tgt_count) &&
	    (v1->lmm_stripe_offset != (typeof(v1->lmm_stripe_offset))(-1))) {
		CERROR("invalid offset: %x\n", v1->lmm_stripe_offset);
		RETURN(-EINVAL);
	}
	lo->ldo_def_stripe_offset = v1->lmm_stripe_offset;

	CDEBUG(D_OTHER, "lsm: %u size, %u stripes, %u offset\n",
	       v1->lmm_stripe_size, v1->lmm_stripe_count,
	       v1->lmm_stripe_offset);

	if (v1->lmm_magic == LOV_MAGIC_V3) {
		if (buf->lb_len < sizeof(*v3)) {
			CERROR("wrong size: %u\n", (unsigned) buf->lb_len);
			RETURN(-EINVAL);
		}

		v3 = buf->lb_buf;
		lod_object_set_pool(lo, v3->lmm_pool_name);

		/* In the function below, .hs_keycmp resolves to
		 * pool_hashkey_keycmp() */
		/* coverity[overrun-buffer-val] */
		pool = lod_find_pool(d, v3->lmm_pool_name);
		if (pool != NULL) {
			if (lo->ldo_def_stripe_offset !=
			    (typeof(v1->lmm_stripe_offset))(-1)) {
				rc = lo->ldo_def_stripe_offset;
				rc = lod_check_index_in_pool(rc, pool);
				if (rc < 0) {
					lod_pool_putref(pool);
					CERROR("invalid offset\n");
					RETURN(-EINVAL);
				}
			}

			if (lo->ldo_stripenr > pool_tgt_count(pool))
				lo->ldo_stripenr= pool_tgt_count(pool);

			lod_pool_putref(pool);
		}
	} else
		lod_object_set_pool(lo, NULL);

	/* fixup for released file */
	if (lo->ldo_pattern & LOV_PATTERN_F_RELEASED) {
		lo->ldo_released_stripenr = lo->ldo_stripenr;
		lo->ldo_stripenr = 0;
	}

	RETURN(0);
}

/*
 * buf should be NULL or contain striping settings
 */
int lod_qos_prep_create(const struct lu_env *env, struct lod_object *lo,
			struct lu_attr *attr, const struct lu_buf *buf,
			struct thandle *th)
{
	struct lod_device      *d = lu2lod_dev(lod2lu_obj(lo)->lo_dev);
	struct dt_object      **stripe;
	int			stripe_len;
	int			flag = LOV_USES_ASSIGNED_STRIPE;
	int			i, rc;
	ENTRY;

	LASSERT(lo);

	/* no OST available */
	/* XXX: should we be waiting a bit to prevent failures during
	 * cluster initialization? */
	if (d->lod_ostnr == 0)
		GOTO(out, rc = -EIO);

	/*
	 * by this time, the object's ldo_stripenr and ldo_stripe_size
	 * contain default value for striping: taken from the parent
	 * or from filesystem defaults
	 *
	 * in case the caller is passing lovea with new striping config,
	 * we may need to parse lovea and apply new configuration
	 */
	rc = lod_qos_parse_config(env, lo, buf);
	if (rc)
		GOTO(out, rc);

	/* A released file is being created */
	if (lo->ldo_stripenr == 0)
		GOTO(out, rc = 0);

	if (likely(lo->ldo_stripe == NULL)) {
		/*
		 * no striping has been created so far
		 */
		LASSERT(lo->ldo_stripenr > 0);
		/*
		 * statfs and check OST targets now, since ld_active_tgt_count
		 * could be changed if some OSTs are [de]activated manually.
		 */
		lod_qos_statfs_update(env, d);
		lo->ldo_stripenr = lod_get_stripecnt(d, LOV_MAGIC,
				lo->ldo_stripenr);

		stripe_len = lo->ldo_stripenr;
		OBD_ALLOC(stripe, sizeof(stripe[0]) * stripe_len);
		if (stripe == NULL)
			GOTO(out, rc = -ENOMEM);

		lod_getref(&d->lod_ost_descs);
		/* XXX: support for non-0 files w/o objects */
		CDEBUG(D_OTHER, "tgt_count %d stripenr %d\n",
				d->lod_desc.ld_tgt_count, stripe_len);
		if (lo->ldo_def_stripe_offset >= d->lod_desc.ld_tgt_count) {
			rc = lod_alloc_qos(env, lo, stripe, flag, th);
		} else {
			rc = lod_alloc_specific(env, lo, stripe, flag, th);
		}
		lod_putref(d, &d->lod_ost_descs);

		if (rc < 0) {
			for (i = 0; i < stripe_len; i++)
				if (stripe[i] != NULL)
					lu_object_put(env, &stripe[i]->do_lu);

			OBD_FREE(stripe, sizeof(stripe[0]) * stripe_len);
		} else {
			lo->ldo_stripe = stripe;
			lo->ldo_stripes_allocated = stripe_len;
		}
	} else {
		/*
		 * lod_qos_parse_config() found supplied buf as a predefined
		 * striping (not a hint), so it allocated all the object
		 * now we need to create them
		 */
		for (i = 0; i < lo->ldo_stripenr; i++) {
			struct dt_object  *o;

			o = lo->ldo_stripe[i];
			LASSERT(o);

			rc = dt_declare_create(env, o, attr, NULL, NULL, th);
			if (rc) {
				CERROR("can't declare create: %d\n", rc);
				break;
			}
		}
	}

out:
	RETURN(rc);
}
