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
 * Copyright (c) 2012, Intel Corporation.
 * Use is subject to license terms.
 *
 * Author: Johann Lombardi <johann@whamcloud.com>
 */

#include <lustre_quota.h>
#include <obd.h>
#include "osd_internal.h"

#include <sys/dnode.h>
#include <sys/spa.h>
#include <sys/zap.h>
#include <sys/dmu_tx.h>
#include <sys/dsl_prop.h>
#include <sys/txg.h>

/**
 * Helper function to retrieve DMU object id from fid for accounting object
 */
uint64_t osd_quota_fid2dmu(const struct lu_fid *fid)
{
	LASSERT(fid_is_acct(fid));
	if (fid_oid(fid) == ACCT_GROUP_OID)
		return DMU_GROUPUSED_OBJECT;
	return DMU_USERUSED_OBJECT;
}

/**
 * Space Accounting Management
 */

/**
 * Return space usage consumed by a given uid or gid.
 * Block usage is accurrate since it is maintained by DMU itself.
 * However, DMU does not provide inode accounting, so the #inodes in use
 * is estimated from the block usage and statfs information.
 *
 * \param env   - is the environment passed by the caller
 * \param dtobj - is the accounting object
 * \param dtrec - is the record to fill with space usage information
 * \param dtkey - is the id the of the user or group for which we would
 *                like to access disk usage.
 * \param capa - is the capability, not used.
 *
 * \retval +ve - success : exact match
 * \retval -ve - failure
 */
static struct id_change *lookup_change_by_id(struct acct_change *ac, __u64 id);
static int osd_acct_index_lookup(const struct lu_env *env,
				struct dt_object *dtobj,
				struct dt_rec *dtrec,
				const struct dt_key *dtkey,
				struct lustre_capa *capa)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	char			*buf  = info->oti_buf;
	struct lquota_acct_rec	*rec  = (struct lquota_acct_rec *)dtrec;
	struct osd_object	*obj = osd_dt_obj(dtobj);
	struct osd_device	*osd = osd_obj2dev(obj);
	int			 rc;
	uint64_t		 oid;
	struct id_change	*uc = NULL;
	ENTRY;

	rec->bspace = rec->ispace = 0;

	/* convert the 64-bit uid/gid into a string */
	sprintf(buf, "%llx", *((__u64 *)dtkey));
	/* fetch DMU object ID (DMU_USERUSED_OBJECT/DMU_GROUPUSED_OBJECT) to be
	 * used */
	oid = osd_quota_fid2dmu(lu_object_fid(&dtobj->do_lu));

	/* disk usage (in bytes) is maintained by DMU.
	 * DMU_USERUSED_OBJECT/DMU_GROUPUSED_OBJECT are special objects which
	 * not associated with any dmu_but_t (see dnode_special_open()).
	 * As a consequence, we cannot use udmu_zap_lookup() here since it
	 * requires a valid oo_db. */
	rc = -zap_lookup(osd->od_objset.os, oid, buf, sizeof(uint64_t), 1,
			&rec->bspace);
	if (rc == -ENOENT)
		/* user/group has not created anything yet */
		CDEBUG(D_QUOTA, "%s: id %s not found in DMU accounting ZAP\n",
		       osd->od_svname, buf);
	else if (rc)
		RETURN(rc);

	if (osd->od_quota_iused_est) {
		if (rec->bspace != 0)
			/* estimate #inodes in use */
			rec->ispace = udmu_objset_user_iused(&osd->od_objset,
							     rec->bspace);
		RETURN(+1);
	}

	/* as for inode accounting, it is not maintained by DMU, so we just
	 * use our own ZAP to track inode usage */
	if (oid == DMU_USERUSED_OBJECT) {
		uc = lookup_change_by_id(&osd->od_quota_cache.acs_uid,
					 *((__u64*)dtkey));
	} else if (oid == DMU_GROUPUSED_OBJECT) {
		uc = lookup_change_by_id(&osd->od_quota_cache.acs_gid,
					 *((__u64*)dtkey));
	}
	if (uc) {
		rec->ispace = uc->delta;
	} else {
		rc = -zap_lookup(osd->od_objset.os, obj->oo_db->db_object,
				buf, sizeof(uint64_t), 1, &rec->ispace);
	}

	if (rc == -ENOENT)
		/* user/group has not created any file yet */
		CDEBUG(D_QUOTA, "%s: id %s not found in accounting ZAP\n",
		       osd->od_svname, buf);
	else if (rc)
		RETURN(rc);

	RETURN(+1);
}

/**
 * Initialize osd Iterator for given osd index object.
 *
 * \param  dt    - osd index object
 * \param  attr  - not used
 * \param  capa  - BYPASS_CAPA
 */
static struct dt_it *osd_it_acct_init(const struct lu_env *env,
				      struct dt_object *dt,
				      __u32 attr,
				      struct lustre_capa *capa)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	struct osd_it_quota	*it;
	struct lu_object	*lo   = &dt->do_lu;
	struct osd_device	*osd  = osd_dev(lo->lo_dev);
	int			 rc;
	ENTRY;

	LASSERT(lu_object_exists(lo));

	if (info == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	it = &info->oti_it_quota;
	memset(it, 0, sizeof(*it));
	it->oiq_oid = osd_quota_fid2dmu(lu_object_fid(lo));

	/* initialize zap cursor */
	rc = -udmu_zap_cursor_init(&it->oiq_zc, &osd->od_objset, it->oiq_oid,0);
	if (rc)
		RETURN(ERR_PTR(rc));

	/* take object reference */
	lu_object_get(lo);
	it->oiq_obj   = osd_dt_obj(dt);
	it->oiq_reset = 1;

	RETURN((struct dt_it *)it);
}

/**
 * Free given iterator.
 *
 * \param  di   - osd iterator
 */
static void osd_it_acct_fini(const struct lu_env *env, struct dt_it *di)
{
	struct osd_it_quota *it = (struct osd_it_quota *)di;
	ENTRY;
	udmu_zap_cursor_fini(it->oiq_zc);
	lu_object_put(env, &it->oiq_obj->oo_dt.do_lu);
	EXIT;
}

/**
 * Move on to the next valid entry.
 *
 * \param  di   - osd iterator
 *
 * \retval +ve  - iterator reached the end
 * \retval   0  - iterator has not reached the end yet
 * \retval -ve  - unexpected failure
 */
static int osd_it_acct_next(const struct lu_env *env, struct dt_it *di)
{
	struct osd_it_quota	*it = (struct osd_it_quota *)di;
	int			 rc;
	ENTRY;

	if (it->oiq_reset == 0)
		zap_cursor_advance(it->oiq_zc);
	it->oiq_reset = 0;
	rc = -udmu_zap_cursor_retrieve_key(env, it->oiq_zc, NULL, 32);
	if (rc == -ENOENT) /* reached the end */
		RETURN(+1);
	RETURN(rc);
}

/**
 * Return pointer to the key under iterator.
 *
 * \param  di   - osd iterator
 */
static struct dt_key *osd_it_acct_key(const struct lu_env *env,
				      const struct dt_it *di)
{
	struct osd_it_quota	*it = (struct osd_it_quota *)di;
	struct osd_thread_info	*info = osd_oti_get(env);
	char			*buf  = info->oti_buf;
	char			*p;
	int			 rc;
	ENTRY;

	it->oiq_reset = 0;
	rc = -udmu_zap_cursor_retrieve_key(env, it->oiq_zc, buf, 32);
	if (rc)
		RETURN(ERR_PTR(rc));
	it->oiq_id = simple_strtoull(buf, &p, 16);
	RETURN((struct dt_key *) &it->oiq_id);
}

/**
 * Return size of key under iterator (in bytes)
 *
 * \param  di   - osd iterator
 */
static int osd_it_acct_key_size(const struct lu_env *env,
				const struct dt_it *di)
{
	ENTRY;
	RETURN((int)sizeof(uint64_t));
}

/**
 * Return pointer to the record under iterator.
 *
 * \param  di    - osd iterator
 * \param  attr  - not used
 */
static int osd_it_acct_rec(const struct lu_env *env,
			   const struct dt_it *di,
			   struct dt_rec *dtrec, __u32 attr)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	char			*buf  = info->oti_buf;
	struct osd_it_quota	*it = (struct osd_it_quota *)di;
	struct lquota_acct_rec	*rec  = (struct lquota_acct_rec *)dtrec;
	struct osd_object	*obj = it->oiq_obj;
	struct osd_device	*osd = osd_obj2dev(obj);
	int			 bytes_read;
	int			 rc;
	ENTRY;

	it->oiq_reset = 0;
	rec->ispace = rec->bspace = 0;

	/* retrieve block usage from the DMU accounting object */
	rc = -udmu_zap_cursor_retrieve_value(env, it->oiq_zc,
					     (char *)&rec->bspace,
					     sizeof(uint64_t), &bytes_read);
	if (rc)
		RETURN(rc);

	if (osd->od_quota_iused_est) {
		if (rec->bspace != 0)
			/* estimate #inodes in use */
			rec->ispace = udmu_objset_user_iused(&osd->od_objset,
							     rec->bspace);
		RETURN(0);
	}

	/* retrieve key associated with the current cursor */
	rc = -udmu_zap_cursor_retrieve_key(env, it->oiq_zc, buf, 32);
	if (rc)
		RETURN(rc);

	/* inode accounting is not maintained by DMU, so we use our own ZAP to
	 * track inode usage */
	rc = -zap_lookup(osd->od_objset.os, it->oiq_obj->oo_db->db_object,
			 buf, sizeof(uint64_t), 1, &rec->ispace);
	if (rc == -ENOENT)
		/* user/group has not created any file yet */
		CDEBUG(D_QUOTA, "%s: id %s not found in accounting ZAP\n",
		       osd->od_svname, buf);
	else if (rc)
		RETURN(rc);

	RETURN(0);
}

/**
 * Returns cookie for current Iterator position.
 *
 * \param  di    - osd iterator
 */
static __u64 osd_it_acct_store(const struct lu_env *env,
			       const struct dt_it *di)
{
	struct osd_it_quota *it = (struct osd_it_quota *)di;
	ENTRY;
	it->oiq_reset = 0;
	RETURN(udmu_zap_cursor_serialize(it->oiq_zc));
}

/**
 * Restore iterator from cookie. if the \a hash isn't found,
 * restore the first valid record.
 *
 * \param  di    - osd iterator
 * \param  hash  - iterator location cookie
 *
 * \retval +ve  - di points to exact matched key
 * \retval  0   - di points to the first valid record
 * \retval -ve  - failure
 */
static int osd_it_acct_load(const struct lu_env *env,
			    const struct dt_it *di, __u64 hash)
{
	struct osd_it_quota	*it  = (struct osd_it_quota *)di;
	struct osd_device	*osd = osd_obj2dev(it->oiq_obj);
	zap_cursor_t		*zc;
	int			 rc;
	ENTRY;

	/* create new cursor pointing to the new hash */
	rc = -udmu_zap_cursor_init(&zc, &osd->od_objset, it->oiq_oid, hash);
	if (rc)
		RETURN(rc);
	udmu_zap_cursor_fini(it->oiq_zc);
	it->oiq_zc = zc;
	it->oiq_reset = 0;

	rc = -udmu_zap_cursor_retrieve_key(env, it->oiq_zc, NULL, 32);
	if (rc == 0)
		RETURN(+1);
	else if (rc == -ENOENT)
		RETURN(0);
	RETURN(rc);
}

/**
 * Move Iterator to record specified by \a key, if the \a key isn't found,
 * move to the first valid record.
 *
 * \param  di   - osd iterator
 * \param  key  - uid or gid
 *
 * \retval +ve  - di points to exact matched key
 * \retval 0    - di points to the first valid record
 * \retval -ve  - failure
 */
static int osd_it_acct_get(const struct lu_env *env, struct dt_it *di,
		const struct dt_key *key)
{
	ENTRY;

	/* XXX: like osd_zap_it_get(), API is currently broken */
	LASSERT(*((__u64 *)key) == 0);

	RETURN(osd_it_acct_load(env, di, 0));
}

/**
 * Release Iterator
 *
 * \param  di   - osd iterator
 */
static void osd_it_acct_put(const struct lu_env *env, struct dt_it *di)
{
}

/**
 * Index and Iterator operations for accounting objects
 */
const struct dt_index_operations osd_acct_index_ops = {
	.dio_lookup = osd_acct_index_lookup,
	.dio_it     = {
		.init		= osd_it_acct_init,
		.fini		= osd_it_acct_fini,
		.get		= osd_it_acct_get,
		.put		= osd_it_acct_put,
		.next		= osd_it_acct_next,
		.key		= osd_it_acct_key,
		.key_size	= osd_it_acct_key_size,
		.rec		= osd_it_acct_rec,
		.store		= osd_it_acct_store,
		.load		= osd_it_acct_load
	}
};

/**
 * Quota Enforcement Management
 */

/*
 * Wrapper for qsd_op_begin().
 *
 * \param env    - the environment passed by the caller
 * \param osd    - is the osd_device
 * \param uid    - user id of the inode
 * \param gid    - group id of the inode
 * \param space  - how many blocks/inodes will be consumed/released
 * \param oh     - osd transaction handle
 * \param is_blk - block quota or inode quota?
 * \param flags  - if the operation is write, return no user quota, no
 *                  group quota, or sync commit flags to the caller
 * \param force  - set to 1 when changes are performed by root user and thus
 *                  can't failed with EDQUOT
 *
 * \retval 0      - success
 * \retval -ve    - failure
 */
int osd_declare_quota(const struct lu_env *env, struct osd_device *osd,
		      qid_t uid, qid_t gid, long long space,
		      struct osd_thandle *oh, bool is_blk, int *flags,
		      bool force)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	struct lquota_id_info	*qi = &info->oti_qi;
	struct qsd_instance     *qsd = osd->od_quota_slave;
	int			 rcu, rcg; /* user & group rc */
	ENTRY;

	if (unlikely(qsd == NULL))
		/* quota slave instance hasn't been allocated yet */
		RETURN(0);

	/* let's start with user quota */
	qi->lqi_id.qid_uid = uid;
	qi->lqi_type       = USRQUOTA;
	qi->lqi_space      = space;
	qi->lqi_is_blk     = is_blk;
	rcu = qsd_op_begin(env, qsd, &oh->ot_quota_trans, qi, flags);

	if (force && (rcu == -EDQUOT || rcu == -EINPROGRESS))
		/* ignore EDQUOT & EINPROGRESS when changes are done by root */
		rcu = 0;

	/* For non-fatal error, we want to continue to get the noquota flags
	 * for group id. This is only for commit write, which has @flags passed
	 * in. See osd_declare_write_commit().
	 * When force is set to true, we also want to proceed with the gid */
	if (rcu && (rcu != -EDQUOT || flags == NULL))
		RETURN(rcu);

	/* and now group quota */
	qi->lqi_id.qid_gid = gid;
	qi->lqi_type       = GRPQUOTA;
	rcg = qsd_op_begin(env, qsd, &oh->ot_quota_trans, qi, flags);

	if (force && (rcg == -EDQUOT || rcg == -EINPROGRESS))
		/* as before, ignore EDQUOT & EINPROGRESS for root */
		rcg = 0;

	RETURN(rcu ? rcu : rcg);
}

/*
 *
 */
static void osd_zfs_acct_update(void *arg1, void *arg2, dmu_tx_t *tx)
{
	struct osd_device *osd = arg1;
	struct acct_changes *ac = arg2;
	struct id_change *uc, *tmp;
	int rc;

	CDEBUG(D_OTHER, "COMMIT %Lu on %s\n", tx->tx_txg, osd->od_svname);

	rc = 0;
	list_for_each_entry_safe(uc, tmp, &ac->acs_uid.ac_list, list) {
		CDEBUG(D_OTHER, "change for uid %Lu: %d\n", uc->id, uc->delta);
		rc = -zap_increment_int(osd->od_objset.os, osd->od_iusr_oid,
					uc->id, uc->delta, tx);
		if (rc)
			CERROR("%s: failed to update accounting ZAP for usr %d "
			       "(%d)\n", osd->od_svname, (int)uc->id, rc);
		list_del(&uc->list);
		OBD_FREE_PTR(uc);
	}

	list_for_each_entry_safe(uc, tmp, &ac->acs_gid.ac_list, list) {
		CDEBUG(D_OTHER, "change for gid %Lu: %d\n", uc->id, uc->delta);
		rc = -zap_increment_int(osd->od_objset.os, osd->od_igrp_oid,
					uc->id, uc->delta, tx);
		if (rc)
			CERROR("%s: failed to update accounting ZAP for usr %d "
			       "(%d)\n", osd->od_svname, (int)uc->id, rc);
		list_del(&uc->list);
		OBD_FREE_PTR(uc);
	}

	OBD_FREE_PTR(ac);
}

int osd_zfs_acct_trans_start(const struct lu_env *env, struct osd_thandle *oh)
{
	struct osd_device *osd = osd_dt_dev(oh->ot_super.th_dev);
	struct acct_changes *ac = NULL;
	int add_work = 0;

	if (oh->ot_tx->tx_txg != osd->od_known_txg) {

		OBD_ALLOC_PTR(ac);
		if (unlikely(ac == NULL))
			return -ENOMEM;
		spin_lock_init(&ac->acs_uid.ac_lock);
		INIT_LIST_HEAD(&ac->acs_uid.ac_list);
		spin_lock_init(&ac->acs_gid.ac_lock);
		INIT_LIST_HEAD(&ac->acs_gid.ac_list);

		spin_lock(&osd->od_known_txg_lock);
		if (oh->ot_tx->tx_txg != osd->od_known_txg) {
			osd->od_acct_changes = ac;
			osd->od_known_txg = oh->ot_tx->tx_txg;
			add_work = 1;
		}
		spin_unlock(&osd->od_known_txg_lock);
	}

	/* schedule a callback to be run in the context of txg
	 * once the latter is closed and syncing */
	if (add_work) {
		spa_t *spa = dmu_objset_spa(osd->od_objset.os);
		dsl_sync_task_do_nowait(spa_get_dsl(spa), NULL,
					osd_zfs_acct_update, osd,
					ac, 0, oh->ot_tx);
	} else if (ac != NULL)
		OBD_FREE_PTR(ac);

	return 0;
}

/*
 *
 */
static struct id_change *lookup_change_by_id(struct acct_change *ac, __u64 id)
{
	struct id_change *uc;

	list_for_each_entry_rcu(uc, &ac->ac_list, list) {
		if (uc->id == id)
			return uc;
	}
	return NULL;
}

static struct id_change *lookup_or_create_by_id(struct acct_change *ac, __u64 id)
{
	struct id_change *uc = NULL, *tmp = NULL;

	uc = lookup_change_by_id(ac, id);
	if (uc == NULL) {
		OBD_ALLOC_PTR(uc);
		LASSERT(uc);
		uc->id = id;
		spin_lock(&ac->ac_lock);
		tmp = lookup_change_by_id(ac, id);
		if (tmp == NULL) {
			list_add_tail_rcu(&uc->list, &ac->ac_list);
		} else {
			OBD_FREE_PTR(uc);
			uc = tmp;
		}
		spin_unlock(&ac->ac_lock);
	}

	return uc;
}

int osd_zfs_acct_id(const struct lu_env *env, struct acct_change *ac,
		    __u64 id, int delta, struct osd_thandle *oh)
{
	struct osd_device	*osd = osd_dt_dev(oh->ot_super.th_dev);
	struct id_change	*uc;

	/*
	 * there should be two structures:
	 *  - global structure caching current state
	 *  - per-txg structure traching per-txg changes
	 */

	LASSERT(ac);
	LASSERT(oh->ot_tx);
	LASSERT(oh->ot_tx->tx_txg == osd->od_known_txg);

	/* find structure by txg */
	uc = lookup_or_create_by_id(ac, id);
	/* XXX: error message here */
	LASSERT(uc);

	spin_lock(&ac->ac_lock);
	uc->delta += delta;
	spin_unlock(&ac->ac_lock);

	return 0;
}

void osd_zfs_acct_cache_init(const struct lu_env *env, struct osd_device *osd,
				struct acct_change *ac, __u64 oid, __u64 id)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	char			*buf  = info->oti_buf;
	struct id_change	*uc;
	__u64			 v;
	int			 rc;

	uc = lookup_change_by_id(ac, id);
	if (likely(uc != NULL))
		return;

	down(&osd->od_quota_cache_sem);
	uc = lookup_change_by_id(ac, id);
	if (uc == NULL) {
		uc = lookup_or_create_by_id(ac, id);
		LASSERT(uc != NULL);
		sprintf(buf, "%llx", id);
		rc = -zap_lookup(osd->od_objset.os, oid,
				buf, sizeof(uint64_t), 1, &v);
		if (rc == 0)
			uc->delta = v;
		CDEBUG(D_OTHER, "init %Ld to %d\n", id, uc->delta);

	}
	up(&osd->od_quota_cache_sem);
}

void osd_zfs_acct_uid(const struct lu_env *env, struct osd_device *osd,
		     __u64 uid, int delta, struct osd_thandle *oh)
{
	osd_zfs_acct_trans_start(env, oh);
	LASSERT(osd->od_acct_changes != NULL);
	osd_zfs_acct_id(env, &osd->od_acct_changes->acs_uid, uid, delta, oh);
	osd_zfs_acct_cache_init(env, osd, &osd->od_quota_cache.acs_uid,
				osd->od_iusr_oid, uid);
	osd_zfs_acct_id(env, &osd->od_quota_cache.acs_uid, uid, delta, oh);

}

void osd_zfs_acct_gid(const struct lu_env *env, struct osd_device *osd,
		     __u64 gid, int delta, struct osd_thandle *oh)
{
	osd_zfs_acct_trans_start(env, oh);
	LASSERT(osd->od_acct_changes != NULL);
	osd_zfs_acct_id(env, &osd->od_acct_changes->acs_gid, gid, delta, oh);
	osd_zfs_acct_cache_init(env, osd, &osd->od_quota_cache.acs_gid,
				osd->od_igrp_oid, gid);
	osd_zfs_acct_id(env, &osd->od_quota_cache.acs_gid, gid, delta, oh);
}

static void __osd_zfs_acct_fini(const struct lu_env *env, struct osd_device *osd,
				struct acct_change *ac, __u64 oid)
{
	struct osd_thread_info	*info = osd_oti_get(env);
	char			*buf  = info->oti_buf;
	struct id_change	*uc, *tmp;
	__u64			v;
	int			rc;


	list_for_each_entry_safe(uc, tmp, &ac->ac_list, list) {
		sprintf(buf, "%llx", uc->id);
		rc = -zap_lookup(osd->od_objset.os, oid,
				buf, sizeof(uint64_t), 1, &v);
		/* pairs with zero value are removed by ZAP automatically */
		if (rc == -ENOENT)
			v = 0;
		if (uc->delta != v)
			CERROR("*** INVALID ACCOUNTING FOR %Lu: %d != %Ld (rc %d)\n",
			       uc->id, uc->delta, v, rc);
		list_del(&uc->list);
		OBD_FREE_PTR(uc);
	}
}

void osd_zfs_acct_init(const struct lu_env *env, struct osd_device *o)
{
	spin_lock_init(&o->od_known_txg_lock);
	spin_lock_init(&o->od_quota_cache.acs_uid.ac_lock);
	INIT_LIST_HEAD(&o->od_quota_cache.acs_uid.ac_list);
	spin_lock_init(&o->od_quota_cache.acs_gid.ac_lock);
	INIT_LIST_HEAD(&o->od_quota_cache.acs_gid.ac_list);
	sema_init(&o->od_quota_cache_sem, 1);
}

void osd_zfs_acct_fini(const struct lu_env *env, struct osd_device *osd)
{
	__osd_zfs_acct_fini(env, osd, &osd->od_quota_cache.acs_uid, osd->od_iusr_oid);
	__osd_zfs_acct_fini(env, osd, &osd->od_quota_cache.acs_gid, osd->od_igrp_oid);
}
