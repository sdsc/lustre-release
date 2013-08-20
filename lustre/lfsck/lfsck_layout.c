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
 * Copyright (c) 2013, Intel Corporation.
 */
/*
 * lustre/lfsck/lfsck_layout.c
 *
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LFSCK

#include <linux/bitops.h>

#include <lustre/lustre_idl.h>
#include <lu_object.h>
#include <dt_object.h>
#include <lustre_linkea.h>
#include <lustre_fid.h>
#include <lustre_lib.h>
#include <lustre_net.h>
#include <lustre/lustre_user.h>
#include <md_object.h>
#include <obd_class.h>

#include "lfsck_internal.h"

#define LFSCK_LAYOUT_MAGIC		0xB173AE14

static const char lfsck_layout_name[] = "lfsck_layout";

enum lfsck_layout_inconsistency {
	LLI_NONE		= 0,
	LLI_DANGLING		= 1,
	LLI_UNMATCHED_PAIR	= 2,
	LLI_MULTIPLE_REFERENCED = 3,
	LLI_ORPHAN		= 4,
	LLI_INCONSISTENT_OWNER	= 5,
	LLI_OTHERS		= 6,
};

struct lfsck_layout_seq {
	cfs_list_t		 lls_list;
	__u64			 lls_seq;
	__u64			 lls_lastid;
	__u64			 lls_lastid_known;
	struct dt_object	*lls_lastid_obj;
	unsigned int		 lls_dirty:1;
};

struct lfsck_layout_slave_data {
	/* list for lfsck_layout_seq */
	cfs_list_t	 llsd_seq_list;
};

struct lfsck_layout_object {
	struct dt_object	*llo_obj;
	struct lu_attr		 llo_attr;
	atomic_t		 llo_ref;
	__u16			 llo_gen;
};

struct lfsck_layout_req {
	cfs_list_t			 llr_list;
	struct lfsck_layout_object	*llr_parent;
	struct dt_object		*llr_child;
	__u32				 llr_ost_idx;
	__u32				 llr_lov_idx; /* offset in LOV EA */
};

struct lfsck_layout_master_data {
	cfs_list_t		llmd_list;
	spinlock_t		llmd_lock;
	struct ptlrpc_thread	llmd_thread;
	atomic_t		llmd_rpc_in_flight;
	int			llmd_assistant_status;
	int			llmd_post_result;
	unsigned int		llmd_to_post:1,
				llmd_to_double_scan:1,
				llmd_in_double_scan:1,
				llmd_exit:1;
	int			llmd_buf_size;
	void		       *llmd_buf;
};

struct lfsck_layout_slave_async_args {
	struct obd_export	*llsaa_exp;
	atomic_t		*llsaa_count;
};

static inline void lfsck_layout_object_get(struct lfsck_layout_object *llo)
{
	atomic_inc(&llo->llo_ref);
}

static inline void lfsck_layout_object_put(const struct lu_env *env,
					   struct lfsck_layout_object *llo)
{
	if (atomic_dec_and_test(&llo->llo_ref)) {
		lfsck_object_put(env, llo->llo_obj);
		OBD_FREE_PTR(llo);
	}
}

static struct lfsck_layout_object *
lfsck_layout_object_init(const struct lu_env *env, struct dt_object *obj,
			 __u16 gen)
{
	struct lfsck_layout_object *llo;
	int			    rc;

	OBD_ALLOC_PTR(llo);
	if (llo == NULL)
		return ERR_PTR(-ENOMEM);

	dt_read_lock(env, obj, 0);
	rc = dt_attr_get(env, obj, &llo->llo_attr, BYPASS_CAPA);
	dt_read_unlock(env, obj);
	if (rc != 0) {
		OBD_FREE_PTR(llo);
		return ERR_PTR(rc);
	}

	lu_object_get(&obj->do_lu);
	llo->llo_obj = obj;
	llo->llo_gen = gen;
	atomic_set(&llo->llo_ref, 1);
	return llo;
}

static inline void lfsck_layout_req_fini(const struct lu_env *env,
					 struct lfsck_layout_req *llr)
{
	lu_object_put(env, &llr->llr_child->do_lu);
	lfsck_layout_object_put(env, llr->llr_parent);
	OBD_FREE_PTR(llr);
}

static struct lfsck_layout_req *
lfsck_layout_req_init(struct lfsck_layout_object *parent,
		      struct dt_object *child, __u32 ost_idx, __u32 lov_idx)
{
	struct lfsck_layout_req *llr;

	OBD_ALLOC_PTR(llr);
	if (llr != NULL) {
		CFS_INIT_LIST_HEAD(&llr->llr_list);
		lfsck_layout_object_get(parent);
		llr->llr_parent = parent;
		llr->llr_child = child;
		llr->llr_ost_idx = ost_idx;
		llr->llr_lov_idx = lov_idx;
	}
	return llr;
}

static void lfsck_layout_le_to_cpu(struct lfsck_layout *des,
				   struct lfsck_layout *src)
{
	des->ll_magic = le32_to_cpu(src->ll_magic);
	des->ll_status = le32_to_cpu(src->ll_status);
	des->ll_flags = le32_to_cpu(src->ll_flags);
	des->ll_success_count = le32_to_cpu(src->ll_success_count);
	des->ll_run_time_phase1 = le32_to_cpu(src->ll_run_time_phase1);
	des->ll_run_time_phase2 = le32_to_cpu(src->ll_run_time_phase2);
	des->ll_time_last_complete = le64_to_cpu(src->ll_time_last_complete);
	des->ll_time_latest_start = le64_to_cpu(src->ll_time_latest_start);
	des->ll_time_last_checkpoint =
				le64_to_cpu(src->ll_time_last_checkpoint);
	des->ll_pos_latest_start = le64_to_cpu(src->ll_pos_latest_start);
	des->ll_pos_last_checkpoint = le64_to_cpu(src->ll_pos_last_checkpoint);
	des->ll_pos_first_inconsistent =
			le64_to_cpu(src->ll_pos_first_inconsistent);
	des->ll_objs_checked_phase1 = le64_to_cpu(src->ll_objs_checked_phase1);
	des->ll_objs_failed_phase1 = le64_to_cpu(src->ll_objs_failed_phase1);
	des->ll_objs_checked_phase2 = le64_to_cpu(src->ll_objs_checked_phase2);
	des->ll_objs_failed_phase2 = le64_to_cpu(src->ll_objs_failed_phase2);
	des->ll_objs_repaired_dangling =
			le64_to_cpu(src->ll_objs_repaired_dangling);
	des->ll_objs_repaired_unmatched_pair =
			le64_to_cpu(src->ll_objs_repaired_unmatched_pair);
	des->ll_objs_repaired_multiple_referenced =
			le64_to_cpu(src->ll_objs_repaired_multiple_referenced);
	des->ll_objs_repaired_orphan =
			le64_to_cpu(src->ll_objs_repaired_orphan);
	des->ll_objs_repaired_inconsistent_owner =
			le64_to_cpu(src->ll_objs_repaired_inconsistent_owner);
	des->ll_objs_repaired_others =
			le64_to_cpu(src->ll_objs_repaired_others);
	des->ll_objs_skipped = le64_to_cpu(src->ll_objs_skipped);
}

static void lfsck_layout_cpu_to_le(struct lfsck_layout *des,
				   struct lfsck_layout *src)
{
	des->ll_magic = cpu_to_le32(src->ll_magic);
	des->ll_status = cpu_to_le32(src->ll_status);
	des->ll_flags = cpu_to_le32(src->ll_flags);
	des->ll_success_count = cpu_to_le32(src->ll_success_count);
	des->ll_run_time_phase1 = cpu_to_le32(src->ll_run_time_phase1);
	des->ll_run_time_phase2 = cpu_to_le32(src->ll_run_time_phase2);
	des->ll_time_last_complete = cpu_to_le64(src->ll_time_last_complete);
	des->ll_time_latest_start = cpu_to_le64(src->ll_time_latest_start);
	des->ll_time_last_checkpoint =
				cpu_to_le64(src->ll_time_last_checkpoint);
	des->ll_pos_latest_start = cpu_to_le64(src->ll_pos_latest_start);
	des->ll_pos_last_checkpoint = cpu_to_le64(src->ll_pos_last_checkpoint);
	des->ll_pos_first_inconsistent =
			cpu_to_le64(src->ll_pos_first_inconsistent);
	des->ll_objs_checked_phase1 = cpu_to_le64(src->ll_objs_checked_phase1);
	des->ll_objs_failed_phase1 = cpu_to_le64(src->ll_objs_failed_phase1);
	des->ll_objs_checked_phase2 = cpu_to_le64(src->ll_objs_checked_phase2);
	des->ll_objs_failed_phase2 = cpu_to_le64(src->ll_objs_failed_phase2);
	des->ll_objs_repaired_dangling =
			cpu_to_le64(src->ll_objs_repaired_dangling);
	des->ll_objs_repaired_unmatched_pair =
			cpu_to_le64(src->ll_objs_repaired_unmatched_pair);
	des->ll_objs_repaired_multiple_referenced =
			cpu_to_le64(src->ll_objs_repaired_multiple_referenced);
	des->ll_objs_repaired_orphan =
			cpu_to_le64(src->ll_objs_repaired_orphan);
	des->ll_objs_repaired_inconsistent_owner =
			cpu_to_le64(src->ll_objs_repaired_inconsistent_owner);
	des->ll_objs_repaired_others =
			cpu_to_le64(src->ll_objs_repaired_others);
	des->ll_objs_skipped = cpu_to_le64(src->ll_objs_skipped);
}

/**
 * \retval +ve: the lfsck_layout is broken, the caller should reset it.
 * \retval 0: succeed.
 * \retval -ve: failed cases.
 */
static int lfsck_layout_load(const struct lu_env *env,
			     struct lfsck_component *com)
{
	struct lfsck_layout		*lo	= com->lc_file_ram;
	const struct dt_body_operations *dbo	= com->lc_obj->do_body_ops;
	ssize_t				 size	= com->lc_file_size;
	loff_t				 pos	= 0;
	int				 rc;

	rc = dbo->dbo_read(env, com->lc_obj,
			   lfsck_buf_get(env, com->lc_file_disk, size), &pos,
			   BYPASS_CAPA);
	if (rc == 0) {
		return -ENOENT;
	} else if (rc < 0) {
		CWARN("%s: failed to load lfsck_layout: rc = %d\n",
		      lfsck_lfsck2name(com->lc_lfsck), rc);
		return rc;
	} else if (rc != size) {
		CWARN("%s: crashed lfsck_layout, to be reset, rc = %d\n",
		      lfsck_lfsck2name(com->lc_lfsck), rc);
		return 1;
	}

	lfsck_layout_le_to_cpu(lo, (struct lfsck_layout *)com->lc_file_disk);
	if (lo->ll_magic != LFSCK_LAYOUT_MAGIC) {
		CWARN("%s: invalid lfsck_layout magic 0x%x != 0x%x, "
		      "to be reset\n", lfsck_lfsck2name(com->lc_lfsck),
		      lo->ll_magic, LFSCK_LAYOUT_MAGIC);
		return 1;
	}

	return 0;
}

static int lfsck_layout_store(const struct lu_env *env,
			      struct lfsck_component *com)
{
	struct dt_object	 *obj		= com->lc_obj;
	struct lfsck_instance	 *lfsck		= com->lc_lfsck;
	struct lfsck_layout	 *lo		= com->lc_file_ram;
	struct thandle		 *handle;
	ssize_t			  size		= com->lc_file_size;
	loff_t			  pos		= 0;
	int			  rc;
	ENTRY;

	lfsck_layout_cpu_to_le(lo, (struct lfsck_layout *)com->lc_file_ram);
	handle = dt_trans_create(env, lfsck->li_bottom);
	if (IS_ERR(handle)) {
		rc = PTR_ERR(handle);
		CERROR("%s: fail to create trans for storing lfsck_layout: "
		       "rc = %d\n", lfsck_lfsck2name(lfsck), rc);
		RETURN(rc);
	}

	rc = dt_declare_record_write(env, obj, sizeof(struct lfsck_layout),
				     pos, handle);
	if (rc != 0) {
		CERROR("%s: fail to declare trans for storing lfsck_layout(1): "
		       "rc = %d\n", lfsck_lfsck2name(lfsck), rc);
		GOTO(out, rc);
	}

	rc = dt_trans_start_local(env, lfsck->li_bottom, handle);
	if (rc != 0) {
		CERROR("%s: fail to start trans for storing lfsck_layout: "
		       "rc = %d\n", lfsck_lfsck2name(lfsck), rc);
		GOTO(out, rc);
	}

	rc = dt_record_write(env, obj, lfsck_buf_get(env, lo, size), &pos,
			     handle);
	if (rc != 0)
		CERROR("%s: fail to store lfsck_layout(1): size = %d, "
		       "rc = %d\n", lfsck_lfsck2name(lfsck), (int)size, rc);

	GOTO(out, rc);

out:
	dt_trans_stop(env, lfsck->li_bottom, handle);
	return rc;
}

static int lfsck_layout_init(const struct lu_env *env,
			     struct lfsck_component *com)
{
	struct lfsck_layout *lo = com->lc_file_ram;
	int rc;

	memset(lo, 0, sizeof(*lo));
	lo->ll_magic = LFSCK_LAYOUT_MAGIC;
	lo->ll_status = LS_INIT;
	down_write(&com->lc_sem);
	rc = lfsck_layout_store(env, com);
	up_write(&com->lc_sem);
	return rc;
}

static int fid_is_for_ostobj(const struct lu_env *env, struct dt_device *dt,
			     struct dt_object *obj, const struct lu_fid *fid)
{
	struct seq_server_site	*ss	= lu_site2seq(dt->dd_lu_dev.ld_site);
	struct lu_server_fld	*sf;
	struct lu_seq_range	 range	= { 0 };
	struct lustre_mdt_attrs *lma;
	int			 rc;

	if (unlikely(ss == NULL))
		goto xattr;

	sf = ss->ss_server_fld;
	LASSERT(sf != NULL);

	fld_range_set_any(&range);
	rc = fld_server_lookup(env, sf, fid_seq(fid), &range);
	if (rc != 0)
		goto xattr;

	if (fld_range_is_ost(&range))
		return 1;
	return 0;

xattr:
	lma = &lfsck_env_info(env)->lti_lma;
	rc = dt_xattr_get(env, obj, lfsck_buf_get(env, lma, sizeof(*lma)),
			  XATTR_NAME_LMA, BYPASS_CAPA);
	if (rc != sizeof(*lma))
		/* We do not know whether it is for OST-object,
		 * return false by default. */
		return 0;

	lustre_lma_swab(lma);
	if (lma->lma_compat & LMAC_FID_ON_OST)
		return 1;
	return 0;
}

static struct lfsck_layout_seq *
lfsck_layout_seq_lookup(struct lfsck_layout_slave_data *llsd, __u64 seq)
{
	struct lfsck_layout_seq *lls;

	cfs_list_for_each_entry(lls, &llsd->llsd_seq_list, lls_list) {
		if (lls->lls_seq == seq)
			return lls;
		if (lls->lls_seq > seq)
			return NULL;
	}
	return NULL;
}

static void
lfsck_layout_seq_insert(struct lfsck_layout_slave_data *llsd,
			struct lfsck_layout_seq *lls)
{
	struct lfsck_layout_seq *tmp;

	cfs_list_for_each_entry(tmp, &llsd->llsd_seq_list, lls_list) {
		if (lls->lls_seq < tmp->lls_seq) {
			cfs_list_add(&lls->lls_list, &tmp->lls_list);
			return;
		}
	}
	cfs_list_add_tail(&lls->lls_list, &llsd->llsd_seq_list);
}

static int
lfsck_layout_lastid_create(const struct lu_env *env,
			   struct lfsck_instance *lfsck,
			   struct dt_object *obj)
{
	struct lfsck_thread_info *info	 = lfsck_env_info(env);
	struct lu_attr		 *la	 = &info->lti_la;
	struct dt_object_format  *dof	 = &info->lti_dof;
	struct lfsck_bookmark	 *bk	 = &lfsck->li_bookmark_ram;
	struct dt_device	 *dt	 = lfsck->li_bottom;
	struct thandle		 *th;
	__u64			  lastid = 0;
	loff_t			  pos	 = 0;
	int			  rc;
	ENTRY;

	CDEBUG(D_LFSCK, "To create LAST_ID for <seq> "LPU64"\n",
	       fid_seq(lfsck_dto2fid(obj)));

	if (bk->lb_param & LPF_DRYRUN)
		return 0;

	memset(la, 0, sizeof(*la));
	la->la_mode = S_IFREG |  S_IRUGO | S_IWUSR;
	la->la_valid = LA_MODE | LA_UID | LA_GID;
	dof->dof_type = dt_mode_to_dft(S_IFREG);

	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		RETURN(rc = PTR_ERR(th));

	rc = dt_declare_create(env, obj, la, NULL, dof, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_declare_record_write(env, obj, sizeof(__u64), pos, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dt, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	rc = dt_create(env, obj, la, NULL, dof, th);
	if (rc == 0)
		rc = dt_record_write(env, obj,
				     lfsck_buf_get(env, &lastid, sizeof(__u64)),
				     &pos, th);
	dt_write_unlock(env, obj);

	GOTO(stop, rc);

stop:
	dt_trans_stop(env, dt, th);
	return rc;
}

static void
lfsck_layout_lastid_reload(const struct lu_env *env,
			   struct lfsck_component *com,
			   struct lfsck_layout_seq *lls)
{
	__u64	lastid;
	loff_t	pos	= 0;
	int	rc;

	dt_read_lock(env, lls->lls_lastid_obj, 0);
	rc = dt_record_read(env, lls->lls_lastid_obj,
			    lfsck_buf_get(env, &lastid, sizeof(__u64)), &pos);
	dt_read_unlock(env, lls->lls_lastid_obj);
	if (rc == 0) {
		lastid = le64_to_cpu(lastid);
		if (lastid < lls->lls_lastid_known) {
			struct lfsck_instance	*lfsck	= com->lc_lfsck;
			struct lfsck_layout	*lo	= com->lc_file_ram;

			lls->lls_lastid = lls->lls_lastid_known;
			lls->lls_dirty = 1;
			if (!(lo->ll_flags & LF_CRASHED_LASTID)) {
				LASSERT(lfsck->li_out_notify != NULL);

				lfsck->li_out_notify(lfsck->li_out_notify_data,
					     LNE_LASTID_REBUILDING);
				lo->ll_flags |= LF_CRASHED_LASTID;
			}
		} else if (lastid >= lls->lls_lastid) {
			lls->lls_lastid = lastid;
			lls->lls_dirty = 0;
		}
	}
}

static int
lfsck_layout_lastid_store(const struct lu_env *env,
			  struct lfsck_component *com)
{
	struct lfsck_instance		*lfsck  = com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct dt_device		*dt	= lfsck->li_bottom;
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct lfsck_layout_seq 	*lls;
	struct thandle			*th;
	__u64				 lastid;
	int				 rc	= 0;
	int				 rc1	= 0;

	cfs_list_for_each_entry(lls, &llsd->llsd_seq_list, lls_list) {
		loff_t pos = 0;

		/* XXX: Add the code back if we really found related
		 *	inconsistent cases in the future. */
#if 0
		if (!lls->lls_dirty) {
			/* In OFD, before the pre-creation, the LAST_ID
			 * file will be updated firstly, which may hide
			 * some potential crashed cases. For example:
			 *
			 * The old obj1's ID is higher than old LAST_ID
			 * but lower than the new LAST_ID, but the LFSCK
			 * have not touch the obj1 until the OFD updated
			 * the LAST_ID. So the LFSCK does not regard it
			 * as crashed case. But when OFD does not create
			 * successfully, it will set the LAST_ID as the
			 * real created objects' ID, then LFSCK needs to
			 * found related inconsistency. */
			lfsck_layout_lastid_reload(env, com, lls);
			if (likely(!lls->lls_dirty))
				continue;
		}
#endif

		CDEBUG(D_LFSCK, "To sync the LAST_ID for <seq> "LPU64
		       " as <oid> "LPU64"\n", lls->lls_seq, lls->lls_lastid);

		if (bk->lb_param & LPF_DRYRUN) {
			lls->lls_dirty = 0;
			continue;
		}

		th = dt_trans_create(env, dt);
		if (IS_ERR(th)) {
			rc1 = PTR_ERR(th);
			continue;
		}

		rc = dt_declare_record_write(env, lls->lls_lastid_obj,
					     sizeof(__u64), pos, th);
		if (rc != 0)
			goto stop;

		rc = dt_trans_start_local(env, dt, th);
		if (rc != 0)
			goto stop;

		lastid = cpu_to_le64(lls->lls_lastid);
		dt_write_lock(env, lls->lls_lastid_obj, 0);
		rc = dt_record_write(env, lls->lls_lastid_obj,
				     lfsck_buf_get(env, &lastid, sizeof(__u64)),
				     &pos, th);
		dt_write_unlock(env, lls->lls_lastid_obj);
		if (rc == 0)
			lls->lls_dirty = 0;

stop:
		dt_trans_stop(env, dt, th);
		if (rc != 0)
			rc1 = rc;
	}

	return (rc1 != 0 ? rc1 : rc);
}

static int
lfsck_layout_lastid_load(const struct lu_env *env,
			 struct lfsck_component *com,
			 struct lfsck_layout_seq *lls)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct lfsck_layout	*lo	= com->lc_file_ram;
	struct lu_fid		*fid	= &lfsck_env_info(env)->lti_fid;
	struct dt_object	*obj;
	loff_t			 pos	= 0;
	int			 rc;
	ENTRY;

	lu_last_id_fid(fid, lls->lls_seq);
	obj = dt_locate(env, lfsck->li_bottom, fid);
	if (IS_ERR(obj))
		RETURN(PTR_ERR(obj));

	/* LAST_ID crashed, to be rebuilt */
	if (!dt_object_exists(obj)) {
		if (!(lo->ll_flags & LF_CRASHED_LASTID)) {
			LASSERT(lfsck->li_out_notify != NULL);

			lfsck->li_out_notify(lfsck->li_out_notify_data,
					     LNE_LASTID_REBUILDING);
			lo->ll_flags |= LF_CRASHED_LASTID;

			if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_DELAY4) &&
			    cfs_fail_val > 0) {
				struct l_wait_info lwi = LWI_TIMEOUT(
						cfs_time_seconds(cfs_fail_val),
						NULL, NULL);

				up_write(&com->lc_sem);
				l_wait_event(lfsck->li_thread.t_ctl_waitq,
					     !thread_is_running(&lfsck->li_thread),
					     &lwi);
				down_write(&com->lc_sem);
			}
		}

		rc = lfsck_layout_lastid_create(env, lfsck, obj);
	} else {
		dt_read_lock(env, obj, 0);
		rc = dt_read(env, obj,
			lfsck_buf_get(env, &lls->lls_lastid, sizeof(__u64)),
			&pos);
		dt_read_unlock(env, obj);
		if (rc != 0 && rc != sizeof(__u64))
			GOTO(out, rc = (rc > 0 ? -EFAULT : rc));

		if (rc == 0 && !(lo->ll_flags & LF_CRASHED_LASTID)) {
			LASSERT(lfsck->li_out_notify != NULL);

			lfsck->li_out_notify(lfsck->li_out_notify_data,
					     LNE_LASTID_REBUILDING);
			lo->ll_flags |= LF_CRASHED_LASTID;
		}

		lls->lls_lastid = le64_to_cpu(lls->lls_lastid);
		rc = 0;
	}

	GOTO(out, rc);

out:
	if (rc != 0)
		lfsck_object_put(env, obj);
	else
		lls->lls_lastid_obj = obj;
	return rc;
}

static int lfsck_layout_wait_slave(const struct lu_env *env,
				   struct obd_export *exp)
{
	struct lfsck_event_request *ler  = &lfsck_env_info(env)->lti_dlc.dlc_req;
	int			    size = sizeof(*ler);
	int			    rc;

	ler->ler_event = LNE_LAYOUT_QUERY;
	ler->u.ler_status = -1;
	rc = obd_get_info(env, exp, strlen(KEY_LFSCK_EVENT),
			  KEY_LFSCK_EVENT, &size, ler, NULL);
	if (rc < 0)
		return rc;
	return ler->u.ler_status;
}

static int lfsck_layout_double_scan_result(const struct lu_env *env,
					   struct lfsck_component *com,
					   int rc)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	struct lfsck_bookmark	*bk    = &lfsck->li_bookmark_ram;

	down_write(&com->lc_sem);

	lo->ll_run_time_phase2 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
	lo->ll_time_last_checkpoint = cfs_time_current_sec();
	lo->ll_objs_checked_phase2 += com->lc_new_checked;
	com->lc_new_checked = 0;

	if (rc > 0) {
		com->lc_journal = 0;
		if (lo->ll_flags & LF_INCOMPLETE)
			lo->ll_status = LS_PARTIAL;
		else
			lo->ll_status = LS_COMPLETED;
		if (!(bk->lb_param & LPF_DRYRUN))
			lo->ll_flags &= ~(LF_SCANNED_ONCE | LF_INCONSISTENT);
		lo->ll_time_last_complete = lo->ll_time_last_checkpoint;
		lo->ll_success_count++;
	} else if (rc == 0) {
		lo->ll_status = lfsck->li_status;
		if (lo->ll_status == 0)
			lo->ll_status = LS_STOPPED;
	} else {
		lo->ll_status = LS_FAILED;
	}

	if (lo->ll_status != LS_PAUSED) {
		spin_lock(&lfsck->li_lock);
		cfs_list_del_init(&com->lc_link);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		spin_unlock(&lfsck->li_lock);
	}

	rc = lfsck_layout_store(env, com);

	up_write(&com->lc_sem);

	return rc;
}

static int lfsck_layout_lock(const struct lu_env *env,
			     struct lfsck_component *com,
			     struct dt_object *obj,
			     struct lustre_handle *lh)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	ldlm_policy_data_t		*policy = &info->lti_policy;
	struct ldlm_res_id		*resid	= &info->lti_resid;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	__u64				 flags	= LDLM_FL_ATOMIC_CB;
	int				 rc;

	LASSERT(lfsck->li_namespace != NULL);

	memset(lh, 0, sizeof(*lh));
	memset(policy, 0, sizeof(*policy));
	policy->l_inodebits.bits = MDS_INODELOCK_LAYOUT;
	fid_build_reg_res_name(lfsck_dto2fid(obj), resid);
	rc = ldlm_cli_enqueue_local(lfsck->li_namespace, resid, LDLM_IBITS,
				    policy, LCK_EX, &flags, ldlm_blocking_ast,
				    ldlm_completion_ast, NULL, NULL, 0,
				    LVB_T_NONE, NULL, lh);
	if (rc == ELDLM_OK)
		return 0;

	if (unlikely(lustre_handle_is_used(lh)))
		ldlm_lock_decref(lh, LCK_EX);
	return -EIO;
}

static void lfsck_layout_unlock(struct lustre_handle *lh)
{
	ldlm_lock_decref(lh, LCK_EX);
}

static int lfsck_layout_recreate_ostobj(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_layout_req *llr,
					struct lu_attr *la)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct dt_allocation_hint	*hint	= &info->lti_hint;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct dt_object		*child  = llr->llr_child;
	struct thandle			*handle;
	int				 rc;
	ENTRY;

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		RETURN(PTR_ERR(handle));

	hint->dah_parent = parent;
	hint->dah_flags = DAHF_RECREATE;
	rc = dt_declare_create(env, child, la, hint, NULL, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_create(env, child, la, hint, NULL, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = 1;
	if (unlikely(lu_object_is_dying(parent->do_lu.lo_header))) {
		dt_destroy(env, child, handle);
		rc = 0;
	}

	GOTO(stop, rc);

stop:
	dt_trans_stop(env, dev, handle);
	return rc;
}

static int lfsck_layout_check_parent(const struct lu_env *env,
				     struct lfsck_component *com,
				     struct dt_object *parent,
				     const struct lu_fid *pfid,
				     const struct lu_fid *cfid,
				     struct lu_buf *buf)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct dt_object		*tobj;
	struct lov_mds_md_v1		*lmm;
	struct lov_ost_data_v1		*objs;
	__u32				 magic;
	__u32				 patten;
	int				 rc;
	int				 i;
	__u16				 count;
	ENTRY;

	if (lu_fid_eq(pfid, lu_object_fid(&parent->do_lu)))
		RETURN(0);

	tobj = lfsck_object_find(env, com->lc_lfsck, pfid);
	if (tobj == NULL)
		RETURN(LLI_UNMATCHED_PAIR);

	if (IS_ERR(tobj))
		RETURN(PTR_ERR(tobj));

	if (!dt_object_exists(tobj))
		GOTO(out, rc = LLI_UNMATCHED_PAIR);

	if (buf->lb_buf == NULL) {
		buf->lb_len = PAGE_CACHE_SIZE;

again:
		OBD_ALLOC_LARGE(buf->lb_buf, buf->lb_len);
		if (buf->lb_buf == NULL)
			GOTO(out, rc = -ENOMEM);
	}

	rc = dt_xattr_get(env, tobj, buf, XATTR_NAME_LOV, BYPASS_CAPA);
	if (rc == -ENODATA || rc == 0)
		GOTO(out, rc = LLI_UNMATCHED_PAIR);

	if (rc == -ERANGE) {
		rc = dt_xattr_get(env, tobj, &LU_BUF_NULL, XATTR_NAME_LOV,
				  BYPASS_CAPA);
		if (unlikely(rc == -ENODATA || rc == 0))
			GOTO(out, rc = LLI_UNMATCHED_PAIR);

		if (rc < 0)
			GOTO(out, rc);

		OBD_FREE_LARGE(buf->lb_buf, buf->lb_len);
		buf->lb_buf = NULL;
		buf->lb_len = rc;
		goto again;
	}

	if (rc < 0)
		GOTO(out, rc);

	lmm = buf->lb_buf;
	magic = le32_to_cpu(lmm->lmm_magic);
	/* If magic crashed, we do not know whether it is unmatched pair or not.
	 * Keep it there to avoid more inconsistence introduced. */
	if (magic != LOV_MAGIC_V1 && magic != LOV_MAGIC_V3)
		GOTO(out, rc = -EINVAL);

	patten = le32_to_cpu(lmm->lmm_pattern);
	/* XXX: currently, we only support LOV_PATTERN_RAID0. */
	if (patten != LOV_PATTERN_RAID0)
		GOTO(out, rc = -EOPNOTSUPP);

	if (magic == LOV_MAGIC_V1)
		objs = &(lmm->lmm_objects[0]);
	else
		objs = &((struct lov_mds_md_v3 *)lmm)->lmm_objects[0];

	count = le16_to_cpu(lmm->lmm_stripe_count);
	for (i = 0; i < count; i++, objs++) {
		struct lu_fid		*tfid	= &info->lti_fid2;
		struct ost_id		*oi	= &info->lti_oi;

		ostid_le_to_cpu(&objs->l_ost_oi, oi);
		ostid_to_fid(tfid, oi, le32_to_cpu(objs->l_ost_idx));
		if (lu_fid_eq(cfid, tfid))
			GOTO(out, rc = LLI_MULTIPLE_REFERENCED);
	}

	GOTO(out, rc = LLI_UNMATCHED_PAIR);

out:
	lfsck_object_put(env, tobj);
	return rc;
}

static int lfsck_layout_repair_unmatched_pair(const struct lu_env *env,
					      struct lfsck_component *com,
					      struct lfsck_layout_req *llr,
					      const struct lu_attr* pla)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_attr			*tla	= &info->lti_la3;
	struct lu_fid			*fid	= &info->lti_fid2;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct dt_object		*child  = llr->llr_child;
	struct thandle			*handle;
	struct lu_buf			 buf;
	int				 rc;
	ENTRY;

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		RETURN(PTR_ERR(handle));

	*fid = *lu_object_fid(&parent->do_lu);
	/* XXX: it is some hack, we use LUSTRE_MAX_OID to indicate
	 *	it is for LFSCK repairing. */
	fid->f_oid |= LUSTRE_MAX_OID;
	buf.lb_buf = fid;
	buf.lb_len = sizeof(*fid);
	rc = dt_declare_xattr_set(env, child, &buf, XATTR_NAME_FID, 0, handle);
	if (rc != 0)
		GOTO(stop, rc);

	tla->la_valid = LA_UID | LA_GID;
	tla->la_uid = pla->la_uid;
	tla->la_gid = pla->la_gid;
	rc = dt_declare_attr_set(env, child, tla, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, handle);
	if (rc != 0)
		GOTO(stop, rc);

	dt_read_lock(env, parent, 0);
	if (unlikely(lu_object_is_dying(parent->do_lu.lo_header)))
		GOTO(unlock, rc = 0);

	/* Get the latest parent's owner. */
	rc = dt_attr_get(env, parent, tla, BYPASS_CAPA);
	if (rc != 0)
		GOTO(unlock, rc);

	tla->la_valid = LA_UID | LA_GID;
	rc = dt_attr_set(env, child, tla, handle, BYPASS_CAPA);

	GOTO(unlock, rc = (rc == 0 ? 1 : rc));

unlock:
	dt_read_unlock(env, parent);
stop:
	dt_trans_stop(env, dev, handle);
	return rc;
}

static int lfsck_layout_assistant_handle_one(const struct lu_env *env,
					     struct lfsck_component *com,
					     struct lfsck_layout_req *llr,
					     struct lu_buf *buf)
{
	struct lfsck_layout		*lo	   = com->lc_file_ram;
	struct lfsck_thread_info	*info	   = lfsck_env_info(env);
	struct lu_fid			*pfid	   = &info->lti_fid;
	struct dt_object		*parent    = llr->llr_parent->llo_obj;
	struct dt_object		*child	   = llr->llr_child;
	struct lu_attr			*pla	   = &info->lti_la;
	struct lu_attr			*cla	   = &info->lti_la2;
	struct lfsck_instance		*lfsck	   = com->lc_lfsck;
	struct lfsck_bookmark		*bk	   = &lfsck->li_bookmark_ram;
	enum lfsck_layout_inconsistency  type	   = LLI_NONE;
	int				 rc;
	ENTRY;

	rc = dt_attr_get(env, parent, pla, BYPASS_CAPA);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_attr_get(env, child, cla, BYPASS_CAPA);
	if (rc == -ENOENT) {
		if (lu_object_is_dying(parent->do_lu.lo_header))
			RETURN(0);

		type = LLI_DANGLING;
		goto repair;
	}

	if (rc != 0)
		GOTO(out, rc);

	rc= dt_xattr_get(env, child,
			 lfsck_buf_get(env, pfid, sizeof(*pfid)),
			 XATTR_NAME_FID, BYPASS_CAPA);
	if (rc >= 0 && rc != sizeof(*pfid)) {
		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	if (rc < 0 && rc != -ENODATA)
		GOTO(out, rc);

	if (rc == -ENODATA)
		fid_zero(pfid);

	if (fid_is_zero(pfid)) {
		/* non-modified. */
		if (cla->la_mode & S_ISUID && cla->la_uid == 0 &&
		    cla->la_mode & S_ISGID && cla->la_gid == 0 &&
		    cla->la_size == 0 && cla->la_blocks == 0)
			RETURN(0);

		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	if (!fid_is_sane(pfid)) {
		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	rc = lfsck_layout_check_parent(env, com, parent, pfid,
				       lu_object_fid(&child->do_lu), buf);
	if (rc > 0) {
		type = rc;
		goto repair;
	}

	if (rc < 0)
		GOTO(out, rc);

	/* XXX: other inconsistency will be checked in other patches. */

repair:
	if (bk->lb_param & LPF_DRYRUN) {
		if (type != LLI_NONE)
			GOTO(out, rc = 1);
		else
			GOTO(out, rc = 0);
	}

	switch (type) {
	case LLI_DANGLING:
		rc = lfsck_layout_recreate_ostobj(env, com, llr, pla);
		break;
	case LLI_UNMATCHED_PAIR:
		rc = lfsck_layout_repair_unmatched_pair(env, com, llr, pla);
		break;

	/* XXX: other inconsistency will be fixed in other patches. */

	case LLI_MULTIPLE_REFERENCED:
		break;
	case LLI_INCONSISTENT_OWNER:
		break;
	default:
		rc = 0;
		break;
	}

	GOTO(out, rc);

out:
	down_write(&com->lc_sem);
	if (rc < 0) {
		lo->ll_objs_failed_phase1++;
	} else if (rc > 0) {
		switch (type) {
		case LLI_DANGLING:
			lo->ll_objs_repaired_dangling++;
			break;
		case LLI_UNMATCHED_PAIR:
			lo->ll_objs_repaired_unmatched_pair++;
			break;
		case LLI_MULTIPLE_REFERENCED:
			lo->ll_objs_repaired_multiple_referenced++;
			break;
		case LLI_INCONSISTENT_OWNER:
			lo->ll_objs_repaired_inconsistent_owner++;
			break;
		default:
			LBUG();
		}
	}
	up_write(&com->lc_sem);
	return rc;
}

static int lfsck_layout_assistant(void *args)
{
	struct lu_env			 env;
	struct lfsck_component		*com    = args;
	struct lfsck_instance		*lfsck  = com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_position		*pos	= &com->lc_pos_start;
	struct lfsck_thread_info	*info;
	struct dt_lfsck_control 	*dlc;
	struct lfsck_event_request	*ler;
	struct lfsck_start		*start;
	struct lfsck_stop		*stop;
	struct lfsck_layout_master_data *llmd   = com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct lfsck_layout_req		*llr;
	struct l_wait_info		 lwi    = { 0 };
	struct lu_buf			 buf;
	int				 rc	= 0;
	int				 rc1	= 0;
	ENTRY;

	rc = lu_env_init(&env, LCT_MD_THREAD | LCT_DT_THREAD);
	if (rc != 0) {
		CERROR("%s: lfsck_layout_assistant fail to init env, rc = %d\n",
		       lfsck_lfsck2name(lfsck), rc);
		spin_lock(&llmd->llmd_lock);
		llmd->llmd_assistant_status = rc;
		thread_set_flags(thread, SVC_STOPPED);
		wake_up_all(&thread->t_ctl_waitq);
		spin_unlock(&llmd->llmd_lock);
		RETURN(rc);
	}

	lfsck_instance_get(lfsck);
	lfsck_component_get(com);

	info = lfsck_env_info(&env);

	buf.lb_buf = NULL;
	buf.lb_len = 0;

	dlc = &info->lti_dlc;
	ler = &dlc->dlc_req;
	start = &ler->u.ler_start;
	stop = &ler->u.ler_stop;

	dlc->dlc_key = lfsck->li_bottom;
	ler->ler_event = LNE_LAYOUT_START;
	start->ls_speed_limit = bk->lb_speed_limit;
	start->ls_version = bk->lb_version;
	start->ls_active = LT_LAYOUT;
	start->ls_flags = bk->lb_param;
	start->ls_valid = LSV_SPEED_LIMIT | LSV_ERROR_HANDLE | LSV_DRYRUN;
	if (pos->lp_oit_cookie <= 1)
		start->ls_flags |= LPF_RESET;

	rc = obd_set_info_async(&env, lfsck->li_exp, strlen(KEY_LFSCK_EVENT),
				KEY_LFSCK_EVENT, sizeof(*dlc), dlc, NULL);
	if (rc != 0) {
		CERROR("%s: fail to notify others for layout start: %d\n",
		       lfsck_lfsck2name(lfsck), rc);
		GOTO(fini, rc);
	}

	spin_lock(&llmd->llmd_lock);
	thread_set_flags(thread, SVC_RUNNING);
	spin_unlock(&llmd->llmd_lock);
	wake_up_all(&thread->t_ctl_waitq);

	while (1) {
		while (!cfs_list_empty(&llmd->llmd_list)) {
			llr = cfs_list_entry(llmd->llmd_list.next,
					     struct lfsck_layout_req,
					     llr_list);
			rc = lfsck_layout_assistant_handle_one(&env, com, llr, &buf);
			spin_lock(&llmd->llmd_lock);
			cfs_list_del_init(&llr->llr_list);
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(&env, llr);

			if (rc < 0 && bk->lb_param & LPF_FAILOUT)
				GOTO(cleanup, rc);

			if (unlikely(llmd->llmd_exit))
				GOTO(cleanup, rc = 0);
		}

		l_wait_event(thread->t_ctl_waitq,
			     llmd->llmd_exit ||
			     !cfs_list_empty(&llmd->llmd_list) ||
			     llmd->llmd_to_post ||
			     llmd->llmd_to_double_scan,
			     &lwi);

		if (unlikely(llmd->llmd_exit))
			GOTO(cleanup, rc = 0);

		if (!cfs_list_empty(&llmd->llmd_list))
			continue;

		if (llmd->llmd_to_post) {
			llmd->llmd_to_post = 0;
			stop->ls_index = lfsck_dev_idx(lfsck->li_bottom);
			if (llmd->llmd_post_result > 0) {
				ler->ler_event = LNE_LAYOUT_PHASE1_DONE;
			} else if (llmd->llmd_post_result == 0) {
				ler->ler_event = LNE_LAYOUT_STOP;
				if (lfsck->li_status == LS_PAUSED ||
				    lfsck->li_status == LS_CO_PAUSED)
					stop->ls_status = LS_CO_PAUSED;
				else if (lfsck->li_status == LS_STOPPED ||
					 lfsck->li_status == LS_CO_STOPPED)
					stop->ls_status = LS_CO_STOPPED;
				else
					LBUG();
			} else {
				ler->ler_event = LNE_LAYOUT_STOP;
				stop->ls_status = LS_CO_FAILED;
			}
			rc = obd_set_info_async(&env, lfsck->li_exp,
						strlen(KEY_LFSCK_EVENT),
						KEY_LFSCK_EVENT,
						sizeof(*dlc), dlc, NULL);
			if (rc != 0) {
				CERROR("%s: failed to notify others for layout "
				       "post: %d\n",
				       lfsck_lfsck2name(lfsck), rc);
				GOTO(done, rc);
			}

			/* Wakeup the master engine to go ahead. */
			wake_up_all(&thread->t_ctl_waitq);
		}

		if (llmd->llmd_to_double_scan) {
			llmd->llmd_to_double_scan = 0;
			atomic_inc(&lfsck->li_double_scan_count);
			llmd->llmd_in_double_scan = 1;
			wake_up_all(&thread->t_ctl_waitq);

			/* Wait for slave double scan to be completed. */
			while (llmd->llmd_in_double_scan) {
				/* Pull once per 60 seconds if we are not
				 * notified. */
				lwi = LWI_TIMEOUT(cfs_time_seconds(60),
						  NULL, NULL);
				rc = l_wait_event(thread->t_ctl_waitq,
						  llmd->llmd_exit ||
						  lfsck_layout_wait_slave(&env,
							lfsck->li_exp) <= 0,
						  &lwi);
				if (unlikely(llmd->llmd_exit))
					GOTO(done, rc = 0);

				if (rc == -ETIMEDOUT)
					continue;

				if (rc < 0)
					GOTO(done, rc);

				goto orphan;
			}
		}
	}

orphan:
	/* XXX: handle orphans under /lost+found in the future. */
	GOTO(done, rc = 1);

cleanup:
	if (!llmd->llmd_exit) {
		LASSERT(cfs_list_empty(&llmd->llmd_list));
	} else {
		/* Cleanup the unfinished requests. */
		spin_lock(&llmd->llmd_lock);
		if (rc < 0)
			llmd->llmd_assistant_status = rc;
		while (!cfs_list_empty(&llmd->llmd_list)) {
			llr = cfs_list_entry(llmd->llmd_list.next,
					     struct lfsck_layout_req,
					     llr_list);
			cfs_list_del_init(&llr->llr_list);
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(&env, llr);
			spin_lock(&llmd->llmd_lock);
		}
		spin_unlock(&llmd->llmd_lock);
	}

	stop->ls_index = lfsck_dev_idx(lfsck->li_bottom);
	if (rc > 0) {
		ler->ler_event = LNE_LAYOUT_PHASE1_DONE;
	} else if (rc == 0) {
		ler->ler_event = LNE_LAYOUT_STOP;
		if (lfsck->li_status == LS_PAUSED ||
		    lfsck->li_status == LS_CO_PAUSED)
			stop->ls_status = LS_CO_PAUSED;
		else if (lfsck->li_status == LS_STOPPED ||
			 lfsck->li_status == LS_CO_STOPPED)
			stop->ls_status = LS_CO_STOPPED;
		else
			LBUG();
	} else {
		ler->ler_event = LNE_LAYOUT_STOP;
		stop->ls_status = LS_CO_FAILED;
	}
	rc1 = obd_set_info_async(&env, lfsck->li_exp, strlen(KEY_LFSCK_EVENT),
				 KEY_LFSCK_EVENT, sizeof(*dlc), dlc, NULL);
	if (rc1 != 0) {
		CERROR("%s: failed to notify others for layout quit: %d\n",
		       lfsck_lfsck2name(lfsck), rc1);
		rc = rc1;
	}

done:
	/* Under force exit case, some requests may be just freed without
	 * verification, those objects should be re-handled when next run.
	 * So not update the on-disk tracing file under such case. */
	if (!llmd->llmd_exit)
		rc1 = lfsck_layout_double_scan_result(&env, com, rc);

fini:
	spin_lock(&llmd->llmd_lock);
	if (llmd->llmd_in_double_scan &&
	    atomic_dec_and_test(&lfsck->li_double_scan_count))
		wake_up_all(&lfsck->li_thread.t_ctl_waitq);
	llmd->llmd_assistant_status = (rc1 != 0 ? rc1 : rc);
	thread_set_flags(thread, SVC_STOPPED);
	wake_up_all(&thread->t_ctl_waitq);
	spin_unlock(&llmd->llmd_lock);
	if (buf.lb_buf != NULL)
		OBD_FREE_LARGE(buf.lb_buf, buf.lb_len);
	lfsck_component_put(&env, com);
	lfsck_instance_put(&env, lfsck);
	lu_env_fini(&env);
	return rc;
}

static int
lfsck_layout_slave_async_interpret(const struct lu_env *env,
				  struct ptlrpc_request *req, void *arg, int rc)
{
	struct lfsck_event_request	     *ler;
	struct lfsck_layout_slave_async_args *llsaa = arg;
	struct obd_export		     *exp   = llsaa->llsaa_exp;

	if (rc != 0) {
		/* It is quite probably caused by target crash,
		 * to make the LFSCK can go ahead, assume that
		 * the target finished the LFSCK prcoessing. */
		exp->exp_in_lfsck = 0;
	} else {
		ler = req_capsule_server_get(&req->rq_pill, &RMF_GETINFO_VAL);
		if (ptlrpc_req_need_swab(req))
			lustre_swab_lfsck_event_request(ler);

		if (ler->u.ler_status != LS_SCANNING_PHASE1 &&
		    ler->u.ler_status != LS_SCANNING_PHASE2)
			exp->exp_in_lfsck = 0;

		if (exp->exp_in_lfsck)
			atomic_inc(llsaa->llsaa_count);
	}
	class_export_put(exp);
	return 0;
}

static int
lfsck_layout_slave_get_info(const struct lu_env *env, struct obd_export *exp,
			    enum lfsck_notify_events event,
			    struct ptlrpc_request_set *set, atomic_t *count)
{
	struct lfsck_layout_slave_async_args *llsaa;
	struct ptlrpc_request		     *req;
	char				     *tmp;
	__u32				     *vallen;
	__u32				      size	=
					sizeof(struct lfsck_event_request);
	int				      rc;

	LASSERT(event == LNE_LAYOUT_QUERY);

	/* XXX: Currently we use the reverse import to send LFSCK RPC
	 *	from OST to MDT. It may be changed to normal import
	 *	when we support orphan OST-objects handling in future. */
	req = ptlrpc_request_alloc(exp->exp_imp_reverse, &RQF_OBD_GET_INFO);
	if (req == NULL)
		return -ENOMEM;

	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_KEY,
			     RCL_CLIENT, strlen(KEY_LFSCK_EVENT));
	rc = ptlrpc_request_pack(req, LUSTRE_OBD_VERSION, LDLM_GET_INFO);
	if (rc != 0) {
		ptlrpc_request_free(req);
		return rc;
	}

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_KEY);
	memcpy(tmp, KEY_LFSCK_EVENT, strlen(KEY_LFSCK_EVENT));
	vallen = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_VALLEN);
	*vallen = size;
	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_VAL,
			     RCL_SERVER, size);
	ptlrpc_request_set_replen(req);

	llsaa = ptlrpc_req_async_args(req);
	llsaa->llsaa_exp = exp;
	llsaa->llsaa_count = count;
	req->rq_interpret_reply = lfsck_layout_slave_async_interpret;
	ptlrpc_set_add_req(set, req);
	return 0;
}

/**
 * \retval +v: the active target count which are in LFSCK processing.
 * \retval  0: All involved LFSCK target have finished.
 * \revval -v: failure cases.
 */
static int
lfsck_layout_slave_check_active(const struct lu_env *env,
				struct lfsck_component *com, bool local)
{
	struct obd_device	  *obd   = com->lc_lfsck->li_exp->exp_obd;
	struct obd_export	  *exp;
	struct ptlrpc_request_set *set;
	__u64			   cookie;
	atomic_t		   count;
	int			   rc    = 0;
	int			   rc1   = 0;
	int			   done  = 0;

	atomic_set(&count, 0);
	if (local) {
		spin_lock(&obd->obd_dev_lock);
		cfs_list_for_each_entry(exp, &obd->obd_mdt_exports,
					exp_mdt_list) {
			if (!exp->exp_failed && !exp->exp_disconnected &&
			    exp->exp_in_lfsck)
				atomic_inc(&count);
		}
		spin_unlock(&obd->obd_dev_lock);
		return atomic_read(&count);
	}

	set = ptlrpc_prep_set();
	if (set == NULL)
		return -ENOMEM;

	cookie = cfs_time_current_64();
	spin_lock(&obd->obd_dev_lock);
	while (!cfs_list_empty(&obd->obd_mdt_exports)) {
		exp = cfs_list_entry(obd->obd_mdt_exports.next,
				     struct obd_export, exp_mdt_list);
		if (exp->exp_touch_gen == cookie)
			break;

		exp->exp_touch_gen = cookie;
		cfs_list_del_init(&exp->exp_mdt_list);
		cfs_list_add_tail(&exp->exp_mdt_list, &obd->obd_mdt_exports);
		if (!(exp_connect_flags(exp) & OBD_CONNECT_LFSCK) ||
		    exp->exp_failed || exp->exp_disconnected ||
		    !exp->exp_in_lfsck)
			continue;
		class_export_get(exp);
		spin_unlock(&obd->obd_dev_lock);

		rc = lfsck_layout_slave_get_info(env, exp, LNE_LAYOUT_QUERY,
						 set, &count);
		if (rc != 0) {
			rc1 = rc;
			class_export_put(exp);
		} else {
			done++;
		}
		spin_lock(&obd->obd_dev_lock);
	}
	spin_unlock(&obd->obd_dev_lock);

	if (done > 0)
		rc = ptlrpc_set_wait(set);
	ptlrpc_set_destroy(set);

	if (atomic_read(&count) > 0)
		return atomic_read(&count);
	if (rc1 != 0)
		return rc1;
	return rc;
}

static void
lfsck_layout_slave_notify_master(const struct lu_env *env,
				 struct lfsck_component *com,
				 enum lfsck_notify_events event, int result)
{
	struct lfsck_instance	   *lfsck  = com->lc_lfsck;
	struct lfsck_event_request *ler    = &lfsck_env_info(env)->lti_dlc.dlc_req;
	struct obd_device	   *obd    = lfsck->li_exp->exp_obd;
	struct obd_export	   *exp;
	struct ptlrpc_request_set  *set;
	__u64			    cookie;
	int			    done   = 0;
	int			    rc;

	set = ptlrpc_prep_set();
	if (set == NULL)
		return;

	ler->ler_event = event;
	ler->u.ler_stop.ls_status = result;
	ler->u.ler_stop.ls_index = lfsck_dev_idx(lfsck->li_bottom);
	cookie = cfs_time_current_64();
	spin_lock(&obd->obd_dev_lock);
	while (!cfs_list_empty(&obd->obd_mdt_exports)) {
		exp = cfs_list_entry(obd->obd_mdt_exports.next,
				     struct obd_export, exp_mdt_list);
		if (exp->exp_touch_gen == cookie)
			break;

		exp->exp_touch_gen = cookie;
		cfs_list_del_init(&exp->exp_mdt_list);
		cfs_list_add_tail(&exp->exp_mdt_list, &obd->obd_mdt_exports);
		if (!(exp_connect_flags(exp) & OBD_CONNECT_LFSCK) ||
		    exp->exp_failed || exp->exp_disconnected ||
		    !exp->exp_in_lfsck)
			continue;
		class_export_get(exp);
		spin_unlock(&obd->obd_dev_lock);

		rc = do_set_info_async(exp->exp_imp_reverse, LDLM_SET_INFO,
				       LUSTRE_OBD_VERSION,
				       sizeof(KEY_LFSCK_EVENT),
				       KEY_LFSCK_EVENT, sizeof(*ler), ler, set);
		if (rc == 0)
			done++;
		class_export_put(exp);
		spin_lock(&obd->obd_dev_lock);
	}
	spin_unlock(&obd->obd_dev_lock);

	if (done > 0)
		rc = ptlrpc_set_wait(set);
	ptlrpc_set_destroy(set);
}

/* layout APIs */

static int lfsck_layout_reset(const struct lu_env *env,
			      struct lfsck_component *com, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	struct dt_object	*root;
	struct dt_object	*dto;
	int			 rc;
	ENTRY;

	root = dt_locate(env, lfsck->li_bottom, &lfsck->li_local_root_fid);
	if (IS_ERR(root))
		RETURN(PTR_ERR(root));

	dt_try_as_dir(env, root);

	down_write(&com->lc_sem);
	if (init) {
		memset(lo, 0, sizeof(*lo));
	} else {
		__u32 count = lo->ll_success_count;
		__u64 last_time = lo->ll_time_last_complete;

		memset(lo, 0, sizeof(*lo));
		lo->ll_success_count = count;
		lo->ll_time_last_complete = last_time;
	}
	lo->ll_magic = LFSCK_LAYOUT_MAGIC;
	lo->ll_status = LS_INIT;

	rc = local_object_unlink(env, lfsck->li_bottom, root,
				 lfsck_layout_name);
	if (rc != 0)
		GOTO(out, rc);

	lfsck_object_put(env, com->lc_obj);
	com->lc_obj = NULL;
	dto = local_file_find_or_create(env, lfsck->li_los, root,
					lfsck_layout_name,
					S_IFREG | S_IRUGO | S_IWUSR);
	if (IS_ERR(dto))
		GOTO(out, rc = PTR_ERR(dto));

	com->lc_obj = dto;
	rc = lfsck_layout_store(env, com);

	GOTO(out, rc);

out:
	up_write(&com->lc_sem);
	lu_object_put(env, &root->do_lu);
	return rc;
}

static void lfsck_layout_fail(const struct lu_env *env,
			      struct lfsck_component *com, bool new_checked)
{
	struct lfsck_layout *lo = com->lc_file_ram;

	down_write(&com->lc_sem);
	if (new_checked)
		com->lc_new_checked++;
	lo->ll_objs_failed_phase1++;
	if (lo->ll_pos_first_inconsistent == 0) {
		struct lfsck_instance *lfsck = com->lc_lfsck;

		lo->ll_pos_first_inconsistent =
			lfsck->li_obj_oit->do_index_ops->dio_it.store(env,
							lfsck->li_di_oit);
	}
	up_write(&com->lc_sem);
}

static int lfsck_layout_master_checkpoint(const struct lu_env *env,
					  struct lfsck_component *com, bool init)
{
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct l_wait_info		 lwi	= { 0 };
	int				 rc;

	if (com->lc_new_checked == 0 && !init)
		return 0;

	l_wait_event(thread->t_ctl_waitq,
		     thread_is_stopped(thread) ||
		     cfs_list_empty(&llmd->llmd_list),
		     &lwi);

	if (llmd->llmd_assistant_status < 0)
		return 0;

	down_write(&com->lc_sem);

	if (init) {
		lo->ll_pos_latest_start = lfsck->li_pos_current.lp_oit_cookie;
	} else {
		lo->ll_pos_last_checkpoint =
					lfsck->li_pos_current.lp_oit_cookie;
		lo->ll_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		lo->ll_time_last_checkpoint = cfs_time_current_sec();
		lo->ll_objs_checked_phase1 += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_layout_store(env, com);

	up_write(&com->lc_sem);
	return rc;
}

static int lfsck_layout_slave_checkpoint(const struct lu_env *env,
					 struct lfsck_component *com, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	int			 rc;

	if (com->lc_new_checked == 0 && !init)
		return 0;

	down_write(&com->lc_sem);

	if (init) {
		lo->ll_pos_latest_start = lfsck->li_pos_current.lp_oit_cookie;
	} else {
		lo->ll_pos_last_checkpoint =
					lfsck->li_pos_current.lp_oit_cookie;
		lo->ll_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		lo->ll_time_last_checkpoint = cfs_time_current_sec();
		lo->ll_objs_checked_phase1 += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_layout_store(env, com);

	up_write(&com->lc_sem);
	return rc;
}

static int lfsck_layout_prep(const struct lu_env *env,
			     struct lfsck_component *com)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct lfsck_layout	*lo	= com->lc_file_ram;
	struct lfsck_position	*pos	= &com->lc_pos_start;

	fid_zero(&pos->lp_dir_parent);
	pos->lp_dir_cookie = 0;
	if (lo->ll_status == LS_COMPLETED ||
	    lo->ll_status == LS_PARTIAL) {
		int rc;

		rc = lfsck_layout_reset(env, com, false);
		if (rc != 0)
			return rc;
	}

	down_write(&com->lc_sem);

	lo->ll_time_latest_start = cfs_time_current_sec();

	spin_lock(&lfsck->li_lock);
	if (lo->ll_flags & LF_SCANNED_ONCE) {
		if (!lfsck->li_drop_dryrun ||
		    lo->ll_pos_first_inconsistent == 0) {
			lo->ll_status = LS_SCANNING_PHASE2;
			cfs_list_del_init(&com->lc_link);
			cfs_list_add_tail(&com->lc_link,
					  &lfsck->li_list_double_scan);
			pos->lp_oit_cookie = 0;
		} else {
			lo->ll_status = LS_SCANNING_PHASE1;
			lo->ll_run_time_phase1 = 0;
			lo->ll_run_time_phase2 = 0;
			lo->ll_objs_checked_phase1 = 0;
			lo->ll_objs_checked_phase2 = 0;
			lo->ll_objs_failed_phase1 = 0;
			lo->ll_objs_failed_phase2 = 0;
			lo->ll_objs_repaired_dangling = 0;
			lo->ll_objs_repaired_unmatched_pair = 0;
			lo->ll_objs_repaired_multiple_referenced = 0;
			lo->ll_objs_repaired_orphan = 0;
			lo->ll_objs_repaired_inconsistent_owner = 0;
			lo->ll_objs_repaired_others = 0;
			pos->lp_oit_cookie = lo->ll_pos_first_inconsistent;
		}
	} else {
		lo->ll_status = LS_SCANNING_PHASE1;
		if (!lfsck->li_drop_dryrun ||
		    lo->ll_pos_first_inconsistent == 0)
			pos->lp_oit_cookie = lo->ll_pos_last_checkpoint + 1;
		else
			pos->lp_oit_cookie = lo->ll_pos_first_inconsistent;
	}
	spin_unlock(&lfsck->li_lock);

	up_write(&com->lc_sem);
	return 0;
}

static int lfsck_layout_slave_prep(const struct lu_env *env,
				   struct lfsck_component *com)
{
	/* XXX: For a new scanning, generate OST-objects
	 *	bitmap for orphan detection. */

	return lfsck_layout_prep(env, com);
}

static int lfsck_layout_master_prep(const struct lu_env *env,
				    struct lfsck_component *com)
{
	struct lfsck_instance		*lfsck  = com->lc_lfsck;
	struct lfsck_layout_master_data *llmd   = com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	long				 rc;
	ENTRY;

	rc = lfsck_layout_prep(env, com);
	if (rc != 0)
		RETURN(rc);

	llmd->llmd_assistant_status = 0;
	llmd->llmd_post_result = 0;
	llmd->llmd_to_post = 0;
	llmd->llmd_to_double_scan = 0;
	llmd->llmd_in_double_scan = 0;
	llmd->llmd_exit = 0;
	thread_set_flags(thread, 0);
	rc = PTR_ERR(kthread_run(lfsck_layout_assistant, com, "lfsck_layout"));
	if (IS_ERR_VALUE(rc)) {
		CERROR("%s: Cannot start LFSCK layout assistant thread: %ld\n",
		       lfsck_lfsck2name(lfsck), rc);
	} else {
		struct l_wait_info lwi = { 0 };

		l_wait_event(thread->t_ctl_waitq,
			     thread_is_running(thread) ||
			     thread_is_stopped(thread),
			     &lwi);
		if (unlikely(!thread_is_running(thread))) {
			rc = llmd->llmd_assistant_status;

			LASSERT(rc < 0);
		} else {
			rc = 0;
		}
	}

	RETURN(rc);
}

static int lfsck_layout_scan_stripes(const struct lu_env *env,
				     struct lfsck_component *com,
				     struct dt_object *parent,
				     struct lov_ost_data_v1 **objs,
				     int count, __u16 gen)
{
	struct lfsck_thread_info	*info 	= lfsck_env_info(env);
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct lfsck_layout_object	*llo	= NULL;
	int				 rc	= 0;
	int				 i;

	for (i = 0; i < count; i++) {
		struct lu_fid		*fid	= &info->lti_fid;
		struct ost_id		*oi	= &info->lti_oi;
		struct lfsck_layout_req *llr;
		struct dt_object	*cobj;
		__u32			 index	=
					le32_to_cpu(objs[i]->l_ost_idx);

		ostid_le_to_cpu(&objs[i]->l_ost_oi, oi);
		ostid_to_fid(fid, oi, index);
		cobj = lu2dt(lu_object_find_last_slice(env,
					dt2lu_dev(lfsck->li_next), fid, NULL));
		if (IS_ERR(cobj)) {
			rc = PTR_ERR(cobj);
			if (rc == -ENOENT) {
				down_write(&com->lc_sem);
				com->lc_new_checked++;
				lo->ll_flags |= LF_INCOMPLETE;
				lo->ll_objs_skipped++;
				up_write(&com->lc_sem);
				rc = 0;
				continue;
			}
			goto next;
		}

		rc = dt_declare_attr_get(env, cobj);
		if (rc != 0) {
			lu_object_put(env, &cobj->do_lu);
			goto next;
		}

		if (llo == NULL) {
			llo = lfsck_layout_object_init(env, parent, gen);
			if (IS_ERR(llo)) {
				lu_object_put(env, &cobj->do_lu);
				rc = PTR_ERR(llo);
				llo = NULL;
				goto next;
			}
		}

		llr = lfsck_layout_req_init(llo, cobj, index, i);
		if (llr == NULL) {
			lu_object_put(env, &cobj->do_lu);
			rc = -ENOMEM;
			goto next;
		}

		spin_lock(&llmd->llmd_lock);
		if (llmd->llmd_assistant_status < 0) {
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(env, llr);
			return llmd->llmd_assistant_status;
		}

		cfs_list_add_tail(&llr->llr_list, &llmd->llmd_list);
		spin_unlock(&llmd->llmd_lock);
		wake_up_all(&llmd->llmd_thread.t_ctl_waitq);

next:
		down_write(&com->lc_sem);
		com->lc_new_checked++;
		if (rc < 0)
			lo->ll_objs_failed_phase1++;
		up_write(&com->lc_sem);

		if (rc < 0 && bk->lb_param & LPF_FAILOUT)
			break;

		rc = 0;
	}

	if (llo != NULL)
		lfsck_layout_object_put(env, llo);

	return rc;
}

static int lfsck_layout_master_exec_oit(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *obj)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct ost_id			*oi	= &info->lti_oi;
	struct lustre_handle		*lh	= &info->lti_lh;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct thandle			*handle = NULL;
	struct lu_buf			*buf;
	struct lov_mds_md_v1		*lmm;
	struct lov_ost_data_v1		*objs;
	__u32				 magic;
	__u32				 patten;
	int				 rc	= 0;
	__u16				 count	= 0;
	__u16				 gen	= 0;
	bool				 to_fix	= false;
	bool				 lock1	= false;
	bool				 lock2	= false;
	ENTRY;

	/* For the given object, read its layout EA locally. For each stripe,
	 * pre-fetch the OST-object's attribute and generate an structure
	 * lfsck_layout_req on the list ::llmd_list.
	 *
	 * For each request on the ::llmd_list, the lfsck_layout_assistant
	 * thread will compare the OST side attribute with local attribute,
	 * if inconsistent, then repair it.
	 *
	 * All above processing is async mode with pipeline. */

	if (!S_ISREG(lfsck_object_type(obj)))
		GOTO(out, rc = 0);

	if (llmd->llmd_assistant_status < 0)
		GOTO(out, rc = -ESRCH);

	fid_to_lmm_oi(lfsck_dto2fid(obj), oi);
	lmm_oi_cpu_to_le(oi, oi);
	buf = lfsck_buf_get(env, llmd->llmd_buf, llmd->llmd_buf_size);

lock:
	if (to_fix) {
		rc = lfsck_layout_lock(env, com, obj, lh);
		if (rc != 0)
			GOTO(out, rc);

		lock1 = true;
		handle = dt_trans_create(env, lfsck->li_next);
		if (IS_ERR(handle))
			GOTO(out, rc = PTR_ERR(handle));

		rc = dt_declare_xattr_set(env, obj, buf, XATTR_NAME_LOV,
					  LU_XATTR_REPLACE, handle);
		if (rc != 0)
			GOTO(out, rc);

		rc = dt_trans_start(env, lfsck->li_next, handle);
		if (rc != 0)
			GOTO(out, rc);

		dt_write_lock(env, obj, 0);
	} else {
		dt_read_lock(env, obj, 0);
	}

	lock2 = true;

again:
	rc = dt_xattr_get(env, obj, buf, XATTR_NAME_LOV, BYPASS_CAPA);
	if (rc == -ERANGE) {
		rc = dt_xattr_get(env, obj, &LU_BUF_NULL, XATTR_NAME_LOV,
				  BYPASS_CAPA);
		if (rc <= 0)
			GOTO(out, rc);

		OBD_FREE_LARGE(llmd->llmd_buf, llmd->llmd_buf_size);
		OBD_ALLOC_LARGE(llmd->llmd_buf, rc);
		if (llmd->llmd_buf == NULL)
			GOTO(out, rc = -ENOMEM);

		llmd->llmd_buf_size = rc;
		buf = lfsck_buf_get(env, llmd->llmd_buf, llmd->llmd_buf_size);
		goto again;
	} else if (rc <= 0) {
		if (rc == -ENODATA)
			rc = 0;
		GOTO(out, rc);
	}

	buf->lb_len = rc;
	rc = 0;
	lmm = llmd->llmd_buf;
	magic = le32_to_cpu(lmm->lmm_magic);
	/* If magic crashed, keep it there. Sometime later, during OST-object
	 * orphan handling, if some OST-object(s) back-point to it, it can be
	 * verified and repaired. */
	if (magic != LOV_MAGIC_V1 && magic != LOV_MAGIC_V3)
		GOTO(out, rc = -EINVAL);

	patten = le32_to_cpu(lmm->lmm_pattern);
	/* XXX: currently, we only support LOV_PATTERN_RAID0. */
	if (patten != LOV_PATTERN_RAID0)
		GOTO(out, rc = -EOPNOTSUPP);

	/* Inconsistent lmm_oi, should be repaired. */
	if (memcmp(oi, &lmm->lmm_oi, sizeof(*oi)) != 0) {
		if (!(bk->lb_param & LPF_DRYRUN)) {
			if (!to_fix) {
				dt_read_unlock(env, obj);
				lock2 = false;
				to_fix = true;
				goto lock;
			}

			lmm->lmm_oi = *oi;
			rc = dt_xattr_set(env, obj, buf, XATTR_NAME_LOV,
					  LU_XATTR_REPLACE, handle,
					  BYPASS_CAPA);
			if (rc != 0)
				GOTO(out, rc);
		}

		down_write(&com->lc_sem);
		lo->ll_objs_repaired_others++;
		up_write(&com->lc_sem);
	}

	count = le16_to_cpu(lmm->lmm_stripe_count);
	gen = le16_to_cpu(lmm->lmm_layout_gen);
	if (magic == LOV_MAGIC_V1)
		objs = &(lmm->lmm_objects[0]);
	else
		objs = &((struct lov_mds_md_v3 *)lmm)->lmm_objects[0];

out:
	if (lock2) {
		if (to_fix)
			dt_write_unlock(env, obj);
		else
			dt_read_unlock(env, obj);
	}

	if (handle != NULL && !IS_ERR(handle))
		dt_trans_stop(env, lfsck->li_next, handle);

	if (lock1)
		lfsck_layout_unlock(lh);

	if (rc == 0 && count > 0) {
		rc = lfsck_layout_scan_stripes(env, com, obj, &objs,
					       count, gen);
	} else {
		down_write(&com->lc_sem);
		com->lc_new_checked++;
		if (rc < 0)
			lo->ll_objs_failed_phase1++;
		up_write(&com->lc_sem);
	}
	return rc;
}

static int lfsck_layout_slave_exec_oit(const struct lu_env *env,
				       struct lfsck_component *com,
				       struct dt_object *obj)
{
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	const struct lu_fid		*fid	= lfsck_dto2fid(obj);
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct lfsck_layout_seq		*lls;
	__u64				 seq;
	__u64				 oid;
	int				 rc;
	ENTRY;

	/* XXX: Update OST-objects bitmap for orphan detection. */

	LASSERT(llsd != NULL);

	down_write(&com->lc_sem);
	if (fid_is_idif(fid))
		seq = 0;
	else if (!fid_is_norm(fid) ||
		 /* With UT applied in future, we need to distinguish whether
		  * it is for OST-object or not. It may involve MDT0 for FLDB
		  * querying. The potential issue is that if the MDT0 or FLDB
		  * crashed, then other OSTs cannot check/rebuild LAST_ID. */
		 !fid_is_for_ostobj(env, lfsck->li_next, obj, fid))
		GOTO(unlock, rc = 0);
	else
		seq = fid_seq(fid);
	com->lc_new_checked++;

	lls = lfsck_layout_seq_lookup(llsd, seq);
	if (lls == NULL) {
		OBD_ALLOC(lls, sizeof(*lls));
		if (unlikely(lls == NULL))
			GOTO(unlock, rc = -ENOMEM);

		CFS_INIT_LIST_HEAD(&lls->lls_list);
		lls->lls_seq = seq;
		rc = lfsck_layout_lastid_load(env, com, lls);
		if (rc != 0) {
			lo->ll_objs_failed_phase1++;
			GOTO(unlock, rc);
		}

		lfsck_layout_seq_insert(llsd, lls);
	}

	if (unlikely(fid_is_last_id(fid)))
		GOTO(unlock, rc = 0);

	oid = fid_oid(fid);
	if (oid > lls->lls_lastid_known)
		lls->lls_lastid_known = oid;

	if (oid > lls->lls_lastid) {
		if (!(lo->ll_flags & LF_CRASHED_LASTID)) {
			/* OFD may create new objects during LFSCK scanning. */
			lfsck_layout_lastid_reload(env, com, lls);
			if (oid <= lls->lls_lastid)
				GOTO(unlock, rc = 0);

			LASSERT(lfsck->li_out_notify != NULL);

			lfsck->li_out_notify(lfsck->li_out_notify_data,
					     LNE_LASTID_REBUILDING);
			lo->ll_flags |= LF_CRASHED_LASTID;
		}

		lls->lls_lastid = oid;
		lls->lls_dirty = 1;
	}

	GOTO(unlock, rc = 0);

unlock:
	up_write(&com->lc_sem);
	return rc;
}

static int lfsck_layout_exec_dir(const struct lu_env *env,
				 struct lfsck_component *com,
				 struct dt_object *obj,
				 struct lu_dirent *ent)
{
	return 0;
}

static int lfsck_layout_master_post(const struct lu_env *env,
				    struct lfsck_component *com,
				    int result, bool init)
{
	struct lfsck_instance		*lfsck  = com->lc_lfsck;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct l_wait_info		 lwi	= { 0 };
	int				 rc;
	ENTRY;


	llmd->llmd_post_result = result;
	llmd->llmd_to_post = 1;
	wake_up_all(&thread->t_ctl_waitq);
	if (result <= 0)
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_stopped(thread),
			     &lwi);
	else
		l_wait_event(thread->t_ctl_waitq,
			     thread_is_stopped(thread) ||
			     cfs_list_empty(&llmd->llmd_list),
			     &lwi);

	if (llmd->llmd_assistant_status < 0)
		result = llmd->llmd_assistant_status;

	down_write(&com->lc_sem);

	spin_lock(&lfsck->li_lock);
	if (!init)
		lo->ll_pos_last_checkpoint =
					lfsck->li_pos_current.lp_oit_cookie;
	if (result > 0) {
		lo->ll_status = LS_SCANNING_PHASE2;
		lo->ll_flags |= LF_SCANNED_ONCE;
		lo->ll_flags &= ~LF_UPGRADE;
		cfs_list_del_init(&com->lc_link);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_double_scan);
	} else if (result == 0) {
		lo->ll_status = lfsck->li_status;
		if (lo->ll_status == 0)
			lo->ll_status = LS_STOPPED;
		if (lo->ll_status != LS_PAUSED) {
			cfs_list_del_init(&com->lc_link);
			cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		}
	} else {
		lo->ll_status = LS_FAILED;
		cfs_list_del_init(&com->lc_link);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
	}
	spin_unlock(&lfsck->li_lock);

	if (!init) {
		lo->ll_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		lo->ll_time_last_checkpoint = cfs_time_current_sec();
		lo->ll_objs_checked_phase1 += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_layout_store(env, com);

	up_write(&com->lc_sem);

	RETURN(rc);
}

static int lfsck_layout_slave_post(const struct lu_env *env,
				   struct lfsck_component *com,
				   int result, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	int			 rc;

	rc = lfsck_layout_lastid_store(env, com);
	if (rc != 0) {
		CERROR("%s: failed to store lastid: rc = %d\n",
		       lfsck_lfsck2name(com->lc_lfsck), rc);
		result = rc;
	}

	LASSERT(lfsck->li_out_notify != NULL);

	down_write(&com->lc_sem);

	spin_lock(&lfsck->li_lock);
	if (!init)
		lo->ll_pos_last_checkpoint =
					lfsck->li_pos_current.lp_oit_cookie;
	if (result > 0) {
		lo->ll_status = LS_SCANNING_PHASE2;
		lo->ll_flags |= LF_SCANNED_ONCE;
		if (lo->ll_flags & LF_CRASHED_LASTID) {
			lfsck->li_out_notify(lfsck->li_out_notify_data,
					     LNE_LASTID_REBUILT);
			lo->ll_flags &= ~LF_CRASHED_LASTID;
		}
		lo->ll_flags &= ~LF_UPGRADE;
		cfs_list_del_init(&com->lc_link);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_double_scan);
	} else if (result == 0) {
		lo->ll_status = lfsck->li_status;
		if (lo->ll_status == 0)
			lo->ll_status = LS_STOPPED;
		if (lo->ll_status != LS_PAUSED) {
			cfs_list_del_init(&com->lc_link);
			cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		}
	} else {
		lo->ll_status = LS_FAILED;
		cfs_list_del_init(&com->lc_link);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
	}
	spin_unlock(&lfsck->li_lock);

	if (!init) {
		lo->ll_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		lo->ll_time_last_checkpoint = cfs_time_current_sec();
		lo->ll_objs_checked_phase1 += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_layout_store(env, com);

	up_write(&com->lc_sem);

	lfsck_layout_slave_notify_master(env, com, LNE_LAYOUT_PHASE1_DONE,
					 result);

	return rc;
}

static int lfsck_layout_dump(const struct lu_env *env,
			     struct lfsck_component *com, char *buf, int len)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_bookmark	*bk    = &lfsck->li_bookmark_ram;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	int			 save  = len;
	int			 ret   = -ENOSPC;
	int			 rc;

	down_read(&com->lc_sem);
	rc = snprintf(buf, len,
		      "name: lfsck_layout\n"
		      "magic: 0x%x\n"
		      "version: %d\n"
		      "status: %s\n",
		      lo->ll_magic,
		      bk->lb_version,
		      lfsck_status_names[lo->ll_status]);
	if (rc <= 0)
		goto out;

	buf += rc;
	len -= rc;
	rc = lfsck_bits_dump(&buf, &len, lo->ll_flags, lfsck_flags_names,
			     "flags");
	if (rc < 0)
		goto out;

	rc = lfsck_bits_dump(&buf, &len, bk->lb_param, lfsck_param_names,
			     "param");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(&buf, &len, lo->ll_time_last_complete,
			     "time_since_last_completed");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(&buf, &len, lo->ll_time_latest_start,
			     "time_since_latest_start");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(&buf, &len, lo->ll_time_last_checkpoint,
			     "time_since_last_checkpoint");
	if (rc < 0)
		goto out;

	rc = snprintf(buf, len,
		      "latest_start_position: "LPU64"\n"
		      "last_checkpoint_position: "LPU64"\n"
		      "first_failure_position: "LPU64"\n",
		      lo->ll_pos_latest_start,
		      lo->ll_pos_last_checkpoint,
		      lo->ll_pos_first_inconsistent);
	if (rc <= 0)
		goto out;

	buf += rc;
	len -= rc;

	if (lo->ll_status == LS_SCANNING_PHASE1) {
		__u64 pos;
		const struct dt_it_ops *iops;
		cfs_duration_t duration = cfs_time_current() -
					  lfsck->li_time_last_checkpoint;
		__u64 checked = lo->ll_objs_checked_phase1 + com->lc_new_checked;
		__u64 speed = checked;
		__u64 new_checked = com->lc_new_checked * HZ;
		__u32 rtime = lo->ll_run_time_phase1 +
			      cfs_duration_sec(duration + HALF_SEC);

		if (duration != 0)
			do_div(new_checked, duration);
		if (rtime != 0)
			do_div(speed, rtime);
		rc = snprintf(buf, len,
			      "checked_phase1: "LPU64"\n"
			      "checked_phase2: "LPU64"\n"
			      "repaired_dangling: "LPU64"\n"
			      "repaired_unmatched_pair: "LPU64"\n"
			      "repaired_multiple_referenced: "LPU64"\n"
			      "repaired_orphan: "LPU64"\n"
			      "repaired_inconsistent_owner: "LPU64"\n"
			      "repaired_others: "LPU64"\n"
			      "failed_phase1: "LPU64"\n"
			      "failed_phase2: "LPU64"\n"
			      "success_count: %u\n"
			      "run_time_phase1: %u seconds\n"
			      "run_time_phase2: %u seconds\n"
			      "average_speed_phase1: "LPU64" items/sec\n"
			      "average_speed_phase2: N/A\n"
			      "real-time_speed_phase1: "LPU64" items/sec\n"
			      "real-time_speed_phase2: N/A\n",
			      checked,
			      lo->ll_objs_checked_phase2,
			      lo->ll_objs_repaired_dangling,
			      lo->ll_objs_repaired_unmatched_pair,
			      lo->ll_objs_repaired_multiple_referenced,
			      lo->ll_objs_repaired_orphan,
			      lo->ll_objs_repaired_inconsistent_owner,
			      lo->ll_objs_repaired_others,
			      lo->ll_objs_failed_phase1,
			      lo->ll_objs_failed_phase2,
			      lo->ll_success_count,
			      rtime,
			      lo->ll_run_time_phase2,
			      speed,
			      new_checked);
		if (rc <= 0)
			goto out;

		buf += rc;
		len -= rc;

		LASSERT(lfsck->li_di_oit != NULL);

		iops = &lfsck->li_obj_oit->do_index_ops->dio_it;

		/* The low layer otable-based iteration position may NOT
		 * exactly match the layout-based directory traversal
		 * cookie. Generally, it is not a serious issue. But the
		 * caller should NOT make assumption on that. */
		pos = iops->store(env, lfsck->li_di_oit);
		if (!lfsck->li_current_oit_processed)
			pos--;
		rc = snprintf(buf, len, "current_position: "LPU64"\n", pos);
		if (rc <= 0)
			goto out;

		buf += rc;
		len -= rc;
	} else {
		/* XXX: LS_SCANNING_PHASE2 will be handled in the future. */
		__u64 speed1 = lo->ll_objs_checked_phase1;
		__u64 speed2 = lo->ll_objs_checked_phase2;

		if (lo->ll_run_time_phase1 != 0)
			do_div(speed1, lo->ll_run_time_phase1);
		if (lo->ll_run_time_phase2 != 0)
			do_div(speed2, lo->ll_run_time_phase2);
		rc = snprintf(buf, len,
			      "checked_phase1: "LPU64"\n"
			      "checked_phase2: "LPU64"\n"
			      "repaired_dangling: "LPU64"\n"
			      "repaired_unmatched_pair: "LPU64"\n"
			      "repaired_multiple_referenced: "LPU64"\n"
			      "repaired_orphan: "LPU64"\n"
			      "repaired_inconsistent_owner: "LPU64"\n"
			      "repaired_others: "LPU64"\n"
			      "failed_phase1: "LPU64"\n"
			      "failed_phase2: "LPU64"\n"
			      "success_count: %u\n"
			      "run_time_phase1: %u seconds\n"
			      "run_time_phase2: %u seconds\n"
			      "average_speed_phase1: "LPU64" items/sec\n"
			      "average_speed_phase2: "LPU64" objs/sec\n"
			      "real-time_speed_phase1: N/A\n"
			      "real-time_speed_phase2: N/A\n"
			      "current_position: N/A\n",
			      lo->ll_objs_checked_phase1,
			      lo->ll_objs_checked_phase2,
			      lo->ll_objs_repaired_dangling,
			      lo->ll_objs_repaired_unmatched_pair,
			      lo->ll_objs_repaired_multiple_referenced,
			      lo->ll_objs_repaired_orphan,
			      lo->ll_objs_repaired_inconsistent_owner,
			      lo->ll_objs_repaired_others,
			      lo->ll_objs_failed_phase1,
			      lo->ll_objs_failed_phase2,
			      lo->ll_success_count,
			      lo->ll_run_time_phase1,
			      lo->ll_run_time_phase2,
			      speed1,
			      speed2);
		if (rc <= 0)
			goto out;

		buf += rc;
		len -= rc;
	}
	ret = save - len;

out:
	up_read(&com->lc_sem);
	return ret;
}

static int lfsck_layout_master_double_scan(const struct lu_env *env,
					   struct lfsck_component *com)
{
	struct lfsck_layout_master_data *llmd   = com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct l_wait_info		 lwi	= { 0 };

	if (unlikely(lo->ll_status != LS_SCANNING_PHASE2))
		return 0;

	llmd->llmd_to_double_scan = 1;
	wake_up_all(&thread->t_ctl_waitq);
	l_wait_event(thread->t_ctl_waitq,
		     thread_is_stopped(thread) ||
		     llmd->llmd_in_double_scan,
		     &lwi);
	if (llmd->llmd_assistant_status < 0)
		return llmd->llmd_assistant_status;
	else
		return 0;
}

static int lfsck_layout_slave_double_scan(const struct lu_env *env,
					  struct lfsck_component *com)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct lfsck_layout	*lo	= com->lc_file_ram;
	struct ptlrpc_thread	*thread = &lfsck->li_thread;
	struct l_wait_info	 lwi;
	int			 result;
	int			 rc;

	if (unlikely(lo->ll_status != LS_SCANNING_PHASE2))
		return 0;

	atomic_inc(&lfsck->li_double_scan_count);

	while (1) {
		lwi = LWI_TIMEOUT(cfs_time_seconds(60), NULL, NULL);
		rc = l_wait_event(thread->t_ctl_waitq,
				  !thread_is_running(thread) ||
				  lfsck_layout_slave_check_active(env, com,
								  false) <= 0,
				  &lwi);
		if (unlikely(!thread_is_running(thread)))
				GOTO(done, rc = 0);

		if (rc == -ETIMEDOUT)
			continue;

		if (rc < 0)
			GOTO(done, rc);

		goto orphan;
	}

orphan:
	/* XXX: To be extended for orphan OST-objects handling in the future.
	 * 	set result = 1 now. */
	GOTO(done, result = 1);

done:
	rc = lfsck_layout_double_scan_result(env, com, result);

	lfsck_layout_slave_notify_master(env, com, LNE_LAYOUT_PHASE2_DONE,
					 result);

	if (atomic_dec_and_test(&lfsck->li_double_scan_count))
		wake_up_all(&lfsck->li_thread.t_ctl_waitq);

	return rc;
}

static void lfsck_layout_master_data_release(const struct lu_env *env,
					     struct lfsck_component *com)
{
	struct lfsck_layout_master_data	*llmd = com->lc_data;

	LASSERT(llmd != NULL);
	LASSERT(thread_is_init(&llmd->llmd_thread) ||
		thread_is_stopped(&llmd->llmd_thread));
	LASSERT(cfs_list_empty(&llmd->llmd_list));
	LASSERT(atomic_read(&llmd->llmd_rpc_in_flight) == 0);

	if (llmd->llmd_buf != NULL) {
		LASSERT(llmd->llmd_buf_size != 0);

		OBD_FREE_LARGE(llmd->llmd_buf, llmd->llmd_buf_size);
	}

	com->lc_data = NULL;

	OBD_FREE(llmd, sizeof(*llmd));
}

static void lfsck_layout_slave_data_release(const struct lu_env *env,
					    struct lfsck_component *com)
{
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct lfsck_layout_seq		*lls;
	struct lfsck_layout_seq		*tmp;

	LASSERT(llsd != NULL);

	com->lc_data = NULL;

	cfs_list_for_each_entry_safe(lls, tmp, &llsd->llsd_seq_list, lls_list) {
		cfs_list_del_init(&lls->lls_list);
		lfsck_object_put(env, lls->lls_lastid_obj);
		OBD_FREE(lls, sizeof(*lls));
	}

	OBD_FREE(llsd, sizeof(*llsd));
}

static void lfsck_layout_master_quit(const struct lu_env *env,
				     struct lfsck_component *com)
{
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct l_wait_info		 lwi    = { 0 };

	llmd->llmd_exit = 1;
	wake_up_all(&thread->t_ctl_waitq);
	l_wait_event(thread->t_ctl_waitq,
		     thread_is_init(thread) ||
		     thread_is_stopped(thread),
		     &lwi);
}

static int lfsck_layout_query(const struct lu_env *env,
			      struct lfsck_component *com)
{
	struct lfsck_layout *lo = com->lc_data;

	return lo->ll_status;
}

static int lfsck_layout_master_in_notify(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct lfsck_event_request *ler,
					 struct obd_export *exp)
{
	struct lfsck_info_local 	*lil   = &lfsck_env_info(env)->lti_lil;
	struct lfsck_instance		*lfsck = com->lc_lfsck;
	struct lfsck_layout		*lo    = com->lc_file_ram;
	struct lfsck_layout_master_data *llmd  = com->lc_data;
	bool				 valid = false;
	bool				 clear = false;
	int				 size  = sizeof(*lil);
	int				 rc;

	if (ler->ler_event == LNE_LAYOUT_PHASE1_DONE) {
		valid = true;
		if (ler->u.ler_stop.ls_status <= 0)
			clear = true;
	}  else if (ler->ler_event == LNE_LAYOUT_PHASE2_DONE) {
		valid = true;
		clear = true;
	}

	if (!valid)
		return -EINVAL;

	if (!clear)
		return 0;

	lil->lil_index = ler->u.ler_stop.ls_index;
	lil->lil_event = LLE_LAYOUT_CLEAR;
	rc = obd_set_info_async(env, lfsck->li_exp,
				strlen(KEY_LFSCK_EVENT_LOCAL),
				KEY_LFSCK_EVENT_LOCAL, sizeof(*lil), lil, NULL);
	if (rc != 0 || lo->ll_status != LS_SCANNING_PHASE2)
		return rc;

	lil->lil_event = LLE_LAYOUT_CHECKALL;
	rc = obd_get_info(env, lfsck->li_exp,
			  strlen(KEY_LFSCK_EVENT_LOCAL),
			  KEY_LFSCK_EVENT_LOCAL, &size, lil, NULL);
	if (rc == 0 && lil->lil_status == 0)
		wake_up_all(&llmd->llmd_thread.t_ctl_waitq);

	return 0;
}

static int lfsck_layout_slave_in_notify(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_event_request *ler,
					struct obd_export *exp)
{
	struct lfsck_instance		*lfsck = com->lc_lfsck;
	struct lfsck_layout		*lo    = com->lc_file_ram;
	int				 rc;

	if (ler->ler_event != LNE_LAYOUT_PHASE1_DONE)
		return -EINVAL;

	LASSERT(exp != NULL);

	exp->exp_in_lfsck = 0;
	rc = lfsck_layout_slave_check_active(env, com, true);
	if (rc == 0 && lo->ll_status == LS_SCANNING_PHASE2)
		wake_up_all(&lfsck->li_thread.t_ctl_waitq);

	return 0;
}

static struct lfsck_operations lfsck_layout_master_ops = {
	.lfsck_reset		= lfsck_layout_reset,
	.lfsck_fail		= lfsck_layout_fail,
	.lfsck_checkpoint	= lfsck_layout_master_checkpoint,
	.lfsck_prep		= lfsck_layout_master_prep,
	.lfsck_exec_oit		= lfsck_layout_master_exec_oit,
	.lfsck_exec_dir		= lfsck_layout_exec_dir,
	.lfsck_post		= lfsck_layout_master_post,
	.lfsck_dump		= lfsck_layout_dump,
	.lfsck_double_scan	= lfsck_layout_master_double_scan,
	.lfsck_data_release	= lfsck_layout_master_data_release,
	.lfsck_quit		= lfsck_layout_master_quit,
	.lfsck_query		= lfsck_layout_query,
	.lfsck_in_notify	= lfsck_layout_master_in_notify,
};

static struct lfsck_operations lfsck_layout_slave_ops = {
	.lfsck_reset		= lfsck_layout_reset,
	.lfsck_fail		= lfsck_layout_fail,
	.lfsck_checkpoint	= lfsck_layout_slave_checkpoint,
	.lfsck_prep		= lfsck_layout_slave_prep,
	.lfsck_exec_oit		= lfsck_layout_slave_exec_oit,
	.lfsck_exec_dir		= lfsck_layout_exec_dir,
	.lfsck_post		= lfsck_layout_slave_post,
	.lfsck_dump		= lfsck_layout_dump,
	.lfsck_double_scan	= lfsck_layout_slave_double_scan,
	.lfsck_data_release	= lfsck_layout_slave_data_release,
	.lfsck_query		= lfsck_layout_query,
	.lfsck_in_notify	= lfsck_layout_slave_in_notify,
};

int lfsck_layout_setup(const struct lu_env *env, struct lfsck_instance *lfsck)
{
	struct lfsck_component	*com;
	struct lfsck_layout	*lo;
	struct dt_object	*root = NULL;
	struct dt_object	*obj;
	int			 rc;
	ENTRY;

	OBD_ALLOC_PTR(com);
	if (com == NULL)
		RETURN(-ENOMEM);

	CFS_INIT_LIST_HEAD(&com->lc_link);
	CFS_INIT_LIST_HEAD(&com->lc_link_dir);
	init_rwsem(&com->lc_sem);
	atomic_set(&com->lc_ref, 1);
	com->lc_lfsck = lfsck;
	com->lc_type = LT_LAYOUT;
	if (lfsck->li_master) {
		struct lfsck_layout_master_data *llmd;

		com->lc_ops = &lfsck_layout_master_ops;
		OBD_ALLOC(llmd, sizeof(*llmd));
		if (llmd == NULL)
			GOTO(out, rc = -ENOMEM);

		CFS_INIT_LIST_HEAD(&llmd->llmd_list);
		spin_lock_init(&llmd->llmd_lock);
		init_waitqueue_head(&llmd->llmd_thread.t_ctl_waitq);
		atomic_set(&llmd->llmd_rpc_in_flight, 0);
		com->lc_data = llmd;
		OBD_ALLOC(llmd->llmd_buf, PAGE_CACHE_SIZE);
		if(llmd->llmd_buf == NULL)
			GOTO(out, rc = -ENOMEM);

		llmd->llmd_buf_size = PAGE_CACHE_SIZE;
	} else {
		struct lfsck_layout_slave_data *llsd;

		com->lc_ops = &lfsck_layout_slave_ops;
		OBD_ALLOC(llsd, sizeof(*llsd));
		if (llsd == NULL)
			GOTO(out, rc = -ENOMEM);

		CFS_INIT_LIST_HEAD(&llsd->llsd_seq_list);
		com->lc_data = llsd;
	}
	com->lc_file_size = sizeof(struct lfsck_layout);
	OBD_ALLOC(com->lc_file_ram, com->lc_file_size);
	if (com->lc_file_ram == NULL)
		GOTO(out, rc = -ENOMEM);

	OBD_ALLOC(com->lc_file_disk, com->lc_file_size);
	if (com->lc_file_disk == NULL)
		GOTO(out, rc = -ENOMEM);

	root = dt_locate(env, lfsck->li_bottom, &lfsck->li_local_root_fid);
	if (IS_ERR(root))
		GOTO(out, rc = PTR_ERR(root));

	dt_try_as_dir(env, root);
	obj = local_file_find_or_create(env, lfsck->li_los, root,
					lfsck_layout_name,
					S_IFREG | S_IRUGO | S_IWUSR);
	if (IS_ERR(obj))
		GOTO(out, rc = PTR_ERR(obj));

	com->lc_obj = obj;
	rc = lfsck_layout_load(env, com);
	if (rc > 0)
		rc = lfsck_layout_reset(env, com, true);
	else if (rc == -ENOENT)
		rc = lfsck_layout_init(env, com);
	if (rc != 0)
		GOTO(out, rc);

	lo = (struct lfsck_layout *)com->lc_file_ram;
	switch (lo->ll_status) {
	case LS_INIT:
	case LS_COMPLETED:
	case LS_FAILED:
	case LS_STOPPED:
	case LS_PARTIAL:
	case LS_CO_FAILED:
	case LS_CO_STOPPED:
	case LS_CO_PAUSED:
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		break;
	default:
		CERROR("%s: unknown lfsck_layout status: rc = %u\n",
		       lfsck_lfsck2name(lfsck), lo->ll_status);
		/* fall through */
	case LS_SCANNING_PHASE1:
	case LS_SCANNING_PHASE2:
		/* No need to store the status to disk right now.
		 * If the system crashed before the status stored,
		 * it will be loaded back when next time. */
		lo->ll_status = LS_CRASHED;
		lo->ll_flags |= LF_INCOMPLETE;
		/* fall through */
	case LS_PAUSED:
	case LS_CRASHED:
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_scan);
		break;
	}

	if (lo->ll_flags & LF_CRASHED_LASTID) {
		LASSERT(lfsck->li_out_notify != NULL);

		lfsck->li_out_notify(lfsck->li_out_notify_data,
				     LNE_LASTID_REBUILDING);
	}

	GOTO(out, rc = 0);

out:
	if (root != NULL && !IS_ERR(root))
		lu_object_put(env, &root->do_lu);
	if (rc != 0)
		lfsck_component_cleanup(env, com);
	return rc;
}
