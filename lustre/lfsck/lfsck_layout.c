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
#include <linux/rbtree.h>

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

struct lfsck_layout_slave_target {
	/* link into lfsck_layout_slave_data::llsd_master_list. */
	cfs_list_t	llst_list;
	__u64		llst_gen;
	atomic_t	llst_ref;
	__u32		llst_index;
};

struct lfsck_layout_slave_data {
	/* list for lfsck_layout_seq */
	cfs_list_t		 llsd_seq_list;
	/* list for the masters involve layout verification. */
	cfs_list_t		 llsd_master_list;
	spinlock_t		 llsd_lock;
	__u64			 llsd_touch_gen;
	struct dt_object	*llsd_rb_obj;
	struct rb_root		 llsd_rb_root;
	rwlock_t		 llsd_rb_lock;
	unsigned int		 llsd_rbtree_valid:1;
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
	spinlock_t		llmd_lock;
	cfs_list_t		llmd_req_list;

	/* list for the ost targets involve layout verification. */
	cfs_list_t		llmd_ost_list;

	/* list for the ost targets in phase1 scanning. */
	cfs_list_t		llmd_ost_phase1_list;

	/* list for the ost targets in phase1 scanning. */
	cfs_list_t		llmd_ost_phase2_list;

	/* list for the mdt targets involve layout verification. */
	cfs_list_t		llmd_mdt_list;

	/* list for the mdt targets in phase1 scanning. */
	cfs_list_t		llmd_mdt_phase1_list;

	/* list for the mdt targets in phase1 scanning. */
	cfs_list_t		llmd_mdt_phase2_list;

	struct ptlrpc_thread	llmd_thread;
	atomic_t		llmd_rpc_in_flight;
	__u32			llmd_touch_gen;
	int			llmd_assistant_status;
	int			llmd_post_result;
	unsigned int		llmd_to_post:1,
				llmd_to_double_scan:1,
				llmd_in_double_scan:1,
				llmd_exit:1;
};

struct lfsck_layout_slave_async_args {
	struct obd_export		 *llsaa_exp;
	struct lfsck_component		 *llsaa_com;
	struct lfsck_layout_slave_target *llsaa_llst;
};

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

static inline void lfsck_layout_object_get(struct lfsck_layout_object *llo)
{
	atomic_inc(&llo->llo_ref);
}

static inline void
lfsck_layout_llst_put(struct lfsck_layout_slave_target *llst)
{
	if (atomic_dec_and_test(&llst->llst_ref)) {
		LASSERT(cfs_list_empty(&llst->llst_list));

		OBD_FREE_PTR(llst);
	}
}

static inline int
lfsck_layout_llst_add(struct lfsck_layout_slave_data *llsd, __u32 index)
{
	struct lfsck_layout_slave_target *llst;
	struct lfsck_layout_slave_target *tmp;
	int				  rc   = 0;

	OBD_ALLOC_PTR(llst);
	if (llst == NULL)
		return -ENOMEM;

	CFS_INIT_LIST_HEAD(&llst->llst_list);
	llst->llst_gen = 0;
	llst->llst_index = index;
	atomic_set(&llst->llst_ref, 1);

	spin_lock(&llsd->llsd_lock);
	cfs_list_for_each_entry(tmp, &llsd->llsd_master_list, llst_list) {
		if (tmp->llst_index == index) {
			rc = -EALREADY;
			break;
		}
	}
	if (rc == 0)
		cfs_list_add_tail(&llst->llst_list, &llsd->llsd_master_list);
	spin_unlock(&llsd->llsd_lock);

	if (rc != 0)
		OBD_FREE_PTR(llst);
	return rc;
}

static inline void
lfsck_layout_llst_del(struct lfsck_layout_slave_data *llsd,
		      struct lfsck_layout_slave_target *llst)
{
	bool del = false;

	spin_lock(&llsd->llsd_lock);
	if (!cfs_list_empty(&llst->llst_list)) {
		cfs_list_del_init(&llst->llst_list);
		del = true;
	}
	spin_unlock(&llsd->llsd_lock);

	if (del)
		lfsck_layout_llst_put(llst);
}

static inline struct lfsck_layout_slave_target *
lfsck_layout_llst_find_and_del(struct lfsck_layout_slave_data *llsd,
			       __u32 index)
{
	struct lfsck_layout_slave_target *llst;

	spin_lock(&llsd->llsd_lock);
	cfs_list_for_each_entry(llst, &llsd->llsd_master_list, llst_list) {
		if (llst->llst_index == index) {
			cfs_list_del_init(&llst->llst_list);
			spin_unlock(&llsd->llsd_lock);
			return llst;
		}
	}
	spin_unlock(&llsd->llsd_lock);
	return NULL;
}

static inline void lfsck_layout_object_put(const struct lu_env *env,
					   struct lfsck_layout_object *llo)
{
	if (atomic_dec_and_test(&llo->llo_ref)) {
		lfsck_object_put(env, llo->llo_obj);
		OBD_FREE_PTR(llo);
	}
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

static inline void lfsck_layout_req_fini(const struct lu_env *env,
					 struct lfsck_layout_req *llr)
{
	lu_object_put(env, &llr->llr_child->do_lu);
	lfsck_layout_object_put(env, llr->llr_parent);
	OBD_FREE_PTR(llr);
}

static inline bool lfsck_layout_req_empty(struct lfsck_layout_master_data *llmd)
{
	bool empty = false;

	spin_lock(&llmd->llmd_lock);
	if (cfs_list_empty(&llmd->llmd_req_list))
		empty = true;
	spin_unlock(&llmd->llmd_lock);
	return empty;
}

#define LFSCK_RBTREE_BITMAP_SIZE	PAGE_CACHE_SIZE
#define LFSCK_RBTREE_BITMAP_WIDTH	(LFSCK_RBTREE_BITMAP_SIZE << 3)
#define LFSCK_RBTREE_BITMAP_MASK	(LFSCK_RBTREE_BITMAP_SIZE - 1)

struct lfsck_rbtree_node {
	struct rb_node	 lrn_node;
	__u64		 lrn_seq;
	__u32		 lrn_first_oid;
	atomic_t	 lrn_known_count;
	atomic_t	 lrn_accessed_count;
	void		*lrn_known_bitmap;
	void		*lrn_accessed_bitmap;
};

static inline int lfsck_rbtree_cmp(struct lfsck_rbtree_node *lrn,
				   __u64 seq, __u32 oid)
{
	if (seq < lrn->lrn_seq)
		return -1;

	if (seq > lrn->lrn_seq)
		return 1;

	if (oid < lrn->lrn_first_oid)
		return -1;

	if (oid >= lrn->lrn_first_oid + LFSCK_RBTREE_BITMAP_WIDTH)
		return 1;

	return 0;
}

/* The caller should hold lock. */
static struct lfsck_rbtree_node *
lfsck_rbtree_search(struct lfsck_layout_slave_data *llsd,
		    const struct lu_fid *fid, bool *exact)
{
	struct rb_node		 *node	= llsd->llsd_rb_root.rb_node;
	struct rb_node		 *prev	= NULL;
	struct lfsck_rbtree_node *lrn	= NULL;
	int			  rc	= 0;

	if (exact != NULL)
		*exact = true;

	while (node != NULL) {
		prev = node;
		lrn = rb_entry(node, struct lfsck_rbtree_node, lrn_node);
		rc = lfsck_rbtree_cmp(lrn, fid_seq(fid), fid_oid(fid));
		if (rc < 0)
			node = node->rb_left;
		else if (rc > 0)
			node = node->rb_right;
		else
			return lrn;
	}

	if (exact == NULL)
		return NULL;

	/* If there is no exactly matched one, then to the next valid one. */
	*exact = false;

	/* The rbtree is empty. */
	if (rc == 0)
		return NULL;

	if (rc < 0)
		return lrn;

	node = rb_next(prev);

	/* The end of the rbtree. */
	if (node == NULL)
		return NULL;

	lrn = rb_entry(node, struct lfsck_rbtree_node, lrn_node);
	return lrn;
}

static struct lfsck_rbtree_node *lfsck_rbtree_new(const struct lu_env *env,
						  const struct lu_fid *fid)
{
	struct lfsck_rbtree_node *lrn;

	OBD_ALLOC_PTR(lrn);
	if (lrn == NULL)
		return ERR_PTR(-ENOMEM);

	OBD_ALLOC(lrn->lrn_known_bitmap, LFSCK_RBTREE_BITMAP_SIZE);
	if (lrn->lrn_known_bitmap == NULL) {
		OBD_FREE_PTR(lrn);
		return ERR_PTR(-ENOMEM);
	}

	OBD_ALLOC(lrn->lrn_accessed_bitmap, LFSCK_RBTREE_BITMAP_SIZE);
	if (lrn->lrn_accessed_bitmap == NULL) {
		OBD_FREE(lrn->lrn_known_bitmap, LFSCK_RBTREE_BITMAP_SIZE);
		OBD_FREE_PTR(lrn);
		return ERR_PTR(-ENOMEM);
	}

	rb_init_node(&lrn->lrn_node);
	lrn->lrn_seq = fid_seq(fid);
	lrn->lrn_first_oid = fid_oid(fid) & ~LFSCK_RBTREE_BITMAP_MASK;
	atomic_set(&lrn->lrn_known_count, 0);
	atomic_set(&lrn->lrn_accessed_count, 0);
	return lrn;
}

static void lfsck_rbtree_free(struct lfsck_rbtree_node *lrn)
{
	OBD_FREE(lrn->lrn_accessed_bitmap, LFSCK_RBTREE_BITMAP_SIZE);
	OBD_FREE(lrn->lrn_known_bitmap, LFSCK_RBTREE_BITMAP_SIZE);
	OBD_FREE_PTR(lrn);
}

/* The caller should hold lock. */
static struct lfsck_rbtree_node *
lfsck_rbtree_insert(struct lfsck_layout_slave_data *llsd,
		    struct lfsck_rbtree_node *lrn)
{
	struct rb_node		 **pos    = &(llsd->llsd_rb_root.rb_node);
	struct rb_node		  *parent = NULL;
	struct lfsck_rbtree_node  *tmp;
	int			   rc;

	while (*pos) {
		parent = *pos;
		tmp = rb_entry(*pos, struct lfsck_rbtree_node, lrn_node);
		rc = lfsck_rbtree_cmp(tmp, lrn->lrn_seq, lrn->lrn_first_oid);
		if (rc < 0)
			pos = &((*pos)->rb_left);
		else if (rc > 0)
			pos = &((*pos)->rb_right);
		else
			return tmp;
	}

	rb_link_node(&lrn->lrn_node, parent, pos);
	rb_insert_color(&lrn->lrn_node, &llsd->llsd_rb_root);
	return lrn;
}

extern const struct dt_index_operations lfsck_orphan_index_ops;

static int lfsck_rbtree_setup(const struct lu_env *env,
			      struct lfsck_component *com)
{
	struct lu_fid			*fid	= &lfsck_env_info(env)->lti_fid;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct dt_object		*obj;

	fid->f_seq = FID_SEQ_LAYOUT_RBTREE;
	fid->f_oid = lfsck_dev_idx(dev);
	fid->f_ver = 0;
	obj = dt_locate(env, dev, fid);
	if (IS_ERR(obj))
		RETURN(PTR_ERR(obj));

	/* XXX: Generate an in-RAM object to stand for the layout rbtree.
	 *	Scanning the layout rbtree will be via the iteration over
	 *	the object. In the future, the rbtree may be written onto
	 *	disk with the object.
	 *
	 *	Mark the object to be as exist. */
	obj->do_lu.lo_header->loh_attr |= LOHA_EXISTS;
	obj->do_index_ops = &lfsck_orphan_index_ops;
	llsd->llsd_rb_obj = obj;
	llsd->llsd_rbtree_valid = 1;
	dev->dd_record_fid_accessed = 1;
	return 0;
}

static void lfsck_rbtree_cleanup(const struct lu_env *env,
				 struct lfsck_component *com)
{
	struct lfsck_instance		*lfsck = com->lc_lfsck;
	struct lfsck_layout_slave_data	*llsd  = com->lc_data;
	struct rb_node			*node  = rb_first(&llsd->llsd_rb_root);
	struct rb_node			*next;
	struct lfsck_rbtree_node	*lrn;

	lfsck->li_bottom->dd_record_fid_accessed = 0;
	/* Invalid the rbtree, then no others will use it. */
	write_lock(&llsd->llsd_rb_lock);
	llsd->llsd_rbtree_valid = 0;
	write_unlock(&llsd->llsd_rb_lock);

	while (node != NULL) {
		next = rb_next(node);
		lrn = rb_entry(node, struct lfsck_rbtree_node, lrn_node);
		rb_erase(node, &llsd->llsd_rb_root);
		lfsck_rbtree_free(lrn);
		node = next;
	}

	if (llsd->llsd_rb_obj != NULL) {
		lu_object_put(env, &llsd->llsd_rb_obj->do_lu);
		llsd->llsd_rb_obj = NULL;
	}
}

static void lfsck_rbtree_update_bitmap(const struct lu_env *env,
				       struct lfsck_component *com,
				       const struct lu_fid *fid,
				       bool accessed)
{
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct lfsck_rbtree_node	*lrn;
	bool				 insert = false;
	int				 idx;
	int				 rc	= 0;
	ENTRY;

	CDEBUG(D_LFSCK, "%s: update bitmap for "DFID"\n",
	       lfsck_lfsck2name(com->lc_lfsck), PFID(fid));

	if (unlikely(!fid_is_sane(fid) || fid_is_last_id(fid)))
		RETURN_EXIT;

	if (!fid_is_idif(fid) && !fid_is_norm(fid))
		RETURN_EXIT;

	read_lock(&llsd->llsd_rb_lock);
	if (!llsd->llsd_rbtree_valid)
		GOTO(unlock, rc = 0);

	lrn = lfsck_rbtree_search(llsd, fid, NULL);
	if (lrn == NULL) {
		struct lfsck_rbtree_node *tmp;

		LASSERT(!insert);

		read_unlock(&llsd->llsd_rb_lock);
		tmp = lfsck_rbtree_new(env, fid);
		if (IS_ERR(tmp))
			GOTO(out, rc = PTR_ERR(tmp));

		insert = true;
		write_lock(&llsd->llsd_rb_lock);
		if (!llsd->llsd_rbtree_valid) {
			lfsck_rbtree_free(tmp);
			GOTO(unlock, rc = 0);
		}

		lrn = lfsck_rbtree_insert(llsd, tmp);
		if (lrn != tmp)
			lfsck_rbtree_free(tmp);
	}

	idx = fid_oid(fid) & LFSCK_RBTREE_BITMAP_MASK;
	/* Any accessed object must be a known object. */
	if (!test_and_set_bit(idx, lrn->lrn_known_bitmap))
		atomic_inc(&lrn->lrn_known_count);
	if (accessed) {
		if (!test_and_set_bit(idx, lrn->lrn_accessed_bitmap))
			atomic_inc(&lrn->lrn_accessed_count);
	}

	GOTO(unlock, rc = 0);

unlock:
	if (insert)
		write_unlock(&llsd->llsd_rb_lock);
	else
		read_unlock(&llsd->llsd_rb_lock);
out:
	if (rc != 0 && accessed) {
		struct lfsck_layout *lo = com->lc_file_ram;

		CERROR("%s: Fail to update object accessed bitmap, will cause "
		       "incorrect LFSCK OST-object handling, so disable it to "
		       "cancel orphan handling for related device. rc = %d.\n",
		       lfsck_lfsck2name(com->lc_lfsck), rc);
		lo->ll_flags |= LF_INCOMPLETE;
		lfsck_rbtree_cleanup(env, com);
	}
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
	if (rc == sizeof(*lma)) {
		lustre_lma_swab(lma);
		return lma->lma_compat & LMAC_FID_ON_OST ? 1 : 0;
	}

	rc = dt_xattr_get(env, obj, &LU_BUF_NULL, XATTR_NAME_FID, BYPASS_CAPA);
	return rc > 0 ? 1 : 0;
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
	cfs_list_t		*pos = &llsd->llsd_seq_list;

	cfs_list_for_each_entry(tmp, &llsd->llsd_seq_list, lls_list) {
		if (lls->lls_seq < tmp->lls_seq) {
			pos = &tmp->lls_list;
			break;
		}
	}
	cfs_list_add_tail(&lls->lls_list, pos);
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

				lfsck->li_out_notify(env,
						lfsck->li_out_notify_data,
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

		CDEBUG(D_LFSCK, "To sync the LAST_ID for <seq> "LPX64
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

			lfsck->li_out_notify(env, lfsck->li_out_notify_data,
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

			lfsck->li_out_notify(env, lfsck->li_out_notify_data,
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

static int lfsck_layout_master_async_interpret(const struct lu_env *env,
					       struct ptlrpc_request *req,
					       void *args, int rc)
{
	struct lfsck_async_interpret_args *laia = args;
	struct lfsck_component		  *com  = laia->laia_com;
	struct lfsck_tgt_descs		  *ltds = laia->laia_ltds;
	struct lfsck_tgt_desc		  *ltd  = laia->laia_ltd;
	struct lfsck_event_request	  *ler  = laia->laia_ler;
	struct lfsck_layout_master_data	  *llmd = com->lc_data;

	switch (ler->ler_event) {
	case LNE_LAYOUT_START:
		if (rc == 0) {
			spin_lock(&ltds->ltd_lock);
			if (!ltd->ltd_dead && !ltd->ltd_layout_done) {
				if (ler->ler_flags & LEF_TO_OST) {
					if (cfs_list_empty(
						&ltd->ltd_layout_list))
						cfs_list_add_tail(
						&ltd->ltd_layout_list,
						&llmd->llmd_ost_list);
					if (cfs_list_empty(
						&ltd->ltd_layout_phase_list))
						cfs_list_add_tail(
						&ltd->ltd_layout_phase_list,
						&llmd->llmd_ost_phase1_list);
				} else {
					if (cfs_list_empty(
						&ltd->ltd_layout_list))
						cfs_list_add_tail(
						&ltd->ltd_layout_list,
						&llmd->llmd_mdt_list);
					if (cfs_list_empty(
						&ltd->ltd_layout_phase_list))
						cfs_list_add_tail(
						&ltd->ltd_layout_phase_list,
						&llmd->llmd_mdt_phase1_list);
				}
			}
			spin_unlock(&ltds->ltd_lock);
		} else {
			struct lfsck_layout *lo = com->lc_file_ram;

			lo->ll_flags |= LF_INCOMPLETE;
		}
		lfsck_tgt_put(ltd);
		break;
	default:
		CERROR("%s: unexpected event: %d\n",
		       lfsck_lfsck2name(com->lc_lfsck), ler->ler_event);
	case LNE_LAYOUT_STOP:
	case LNE_LAYOUT_PHASE1_DONE:
	case LNE_LAYOUT_PHASE2_DONE:
		if (rc != 0)
			CERROR("%s: fail to notify %s %x for layout: "
			       "event = %d, rc = %d\n",
			       lfsck_lfsck2name(com->lc_lfsck),
			       (ler->ler_flags & LEF_TO_OST) ? "OST" : "MDT",
			       ltd->ltd_index, ler->ler_event, rc);
		break;
	case LNE_LAYOUT_QUERY:
		if (rc == 0) {
			if (ler->ler_flags & LEF_TO_OST)
				ler = req_capsule_server_get(&req->rq_pill,
							     &RMF_GENERIC_DATA);
			else
				ler = req_capsule_server_get(&req->rq_pill,
							     &RMF_GETINFO_VAL);
			if (ler == NULL) {
				CERROR("%s: invalid return value\n",
				       lfsck_lfsck2name(com->lc_lfsck));
				spin_lock(&ltds->ltd_lock);
				cfs_list_del_init(&ltd->ltd_layout_phase_list);
				cfs_list_del_init(&ltd->ltd_layout_list);
				spin_unlock(&ltds->ltd_lock);
				lfsck_tgt_put(ltd);
				return -EPROTO;
			}

			if (ptlrpc_req_need_swab(req))
				lustre_swab_lfsck_event_request(ler);

			switch (ler->u.ler_status) {
			case LS_SCANNING_PHASE1:
				break;
			case LS_SCANNING_PHASE2:
				spin_lock(&ltds->ltd_lock);
				cfs_list_del_init(&ltd->ltd_layout_phase_list);
				if (!ltd->ltd_dead && !ltd->ltd_layout_done) {
					if (ler->ler_flags & LEF_FROM_OST)
						cfs_list_add_tail(
						&ltd->ltd_layout_phase_list,
						&llmd->llmd_ost_phase2_list);
					else
						cfs_list_add_tail(
						&ltd->ltd_layout_phase_list,
						&llmd->llmd_mdt_phase2_list);
				}
				spin_unlock(&ltds->ltd_lock);
				break;
			default:
				spin_lock(&ltds->ltd_lock);
				cfs_list_del_init(&ltd->ltd_layout_phase_list);
				cfs_list_del_init(&ltd->ltd_layout_list);
				spin_unlock(&ltds->ltd_lock);
				break;
			}
		} else {
			spin_lock(&ltds->ltd_lock);
			cfs_list_del_init(&ltd->ltd_layout_phase_list);
			cfs_list_del_init(&ltd->ltd_layout_list);
			spin_unlock(&ltds->ltd_lock);
		}
		lfsck_tgt_put(ltd);
		break;
	}

	lfsck_component_put(env, com);
	return 0;
}

static int lfsck_layout_master_query_others(const struct lu_env *env,
					    struct lfsck_component *com)
{
	struct lfsck_thread_info	  *info  = lfsck_env_info(env);
	struct lfsck_event_request	  *ler	 = &info->lti_ler;
	struct lfsck_async_interpret_args *laia  = &info->lti_laia;
	struct lfsck_instance		  *lfsck = com->lc_lfsck;
	struct lfsck_layout_master_data	  *llmd  = com->lc_data;
	struct ptlrpc_request_set	  *set;
	struct lfsck_tgt_descs		  *ltds;
	struct lfsck_tgt_desc		  *ltd;
	cfs_list_t			  *head;
	__u32				   cnt   = 0;
	int				   rc    = 0;
	int				   rc1   = 0;
	ENTRY;

	set = ptlrpc_prep_set();
	if (set == NULL)
		RETURN(-ENOMEM);

	llmd->llmd_touch_gen++;
	memset(ler, 0, sizeof(*ler));
	ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
	ler->ler_event = LNE_LAYOUT_QUERY;
	laia->laia_com = com;
	laia->laia_ler = ler;

	if (!cfs_list_empty(&llmd->llmd_mdt_phase1_list)) {
		ltds = &lfsck->li_mdt_descs;
		ler->ler_flags = 0;
		head = &llmd->llmd_mdt_phase1_list;
	} else {

again:
		ltds = &lfsck->li_ost_descs;
		ler->ler_flags = LEF_TO_OST;
		head = &llmd->llmd_ost_phase1_list;
	}

	laia->laia_ltds = ltds;
	spin_lock(&ltds->ltd_lock);
	while (!cfs_list_empty(head)) {
		ltd = cfs_list_entry(head->next,
				     struct lfsck_tgt_desc,
				     ltd_layout_phase_list);
		if (ltd->ltd_layout_gen == llmd->llmd_touch_gen)
			break;

		ltd->ltd_layout_gen = llmd->llmd_touch_gen;
		cfs_list_del_init(&ltd->ltd_layout_phase_list);
		cfs_list_add_tail(&ltd->ltd_layout_phase_list, head);
		atomic_inc(&ltd->ltd_ref);
		laia->laia_ltd = ltd;
		spin_unlock(&ltds->ltd_lock);
		rc = lfsck_async_get_info(env, ltd->ltd_exp, ler, set,
					  lfsck_layout_master_async_interpret,
					  laia);
		if (rc != 0) {
			CERROR("%s: fail to query %s %x for layout: %d\n",
			       lfsck_lfsck2name(lfsck),
			       (ler->ler_flags & LEF_TO_OST) ? "OST" : "MDT",
			       ltd->ltd_index, rc);
			lfsck_tgt_put(ltd);
			rc1 = rc;
		} else {
			cnt++;
		}
		spin_lock(&ltds->ltd_lock);
	}
	spin_unlock(&ltds->ltd_lock);

	if (cnt > 0) {
		rc = ptlrpc_set_wait(set);
		if (rc < 0) {
			ptlrpc_set_destroy(set);
			RETURN(rc);
		}
		cnt = 0;
	}

	if (!(ler->ler_flags & LEF_TO_OST) &&
	    cfs_list_empty(&llmd->llmd_mdt_phase1_list))
		goto again;

	ptlrpc_set_destroy(set);

	RETURN(rc1 != 0 ? rc1 : rc);
}

static int lfsck_layout_master_wait_others(const struct lu_env *env,
					   struct lfsck_component *com)
{
	struct lfsck_layout_master_data *llmd = com->lc_data;
	int				 rc;

	rc = lfsck_layout_master_query_others(env, com);
	if (cfs_list_empty(&llmd->llmd_mdt_phase1_list) &&
	    (!cfs_list_empty(&llmd->llmd_ost_phase2_list) ||
	     cfs_list_empty(&llmd->llmd_ost_phase1_list)))
		return 1;

	return rc;
}

static int lfsck_layout_master_notify_others(const struct lu_env *env,
					     struct lfsck_component *com,
					     struct lfsck_event_request *ler,
					     __u32 flags)
{
	struct lfsck_thread_info	  *info  = lfsck_env_info(env);
	struct lfsck_async_interpret_args *laia  = &info->lti_laia;
	struct lfsck_instance		  *lfsck = com->lc_lfsck;
	struct lfsck_layout_master_data	  *llmd  = com->lc_data;
	struct lfsck_layout		  *lo	 = com->lc_file_ram;
	struct ptlrpc_request_set	  *set;
	struct lfsck_tgt_descs		  *ltds;
	struct lfsck_tgt_desc		  *ltd;
	struct lfsck_tgt_desc		  *next;
	cfs_list_t			  *head;
	__u32				   idx;
	__u32				   cnt   = 0;
	int				   rc    = 0;
	ENTRY;

	set = ptlrpc_prep_set();
	if (set == NULL)
		RETURN(-ENOMEM);

	laia->laia_com = com;
	laia->laia_ler = ler;
	ler->ler_flags = 0;
	switch (ler->ler_event) {
	case LNE_LAYOUT_START:
		/* Notify OSTs firstly, then other MDTs if needed. */
		ler->ler_flags |= LEF_TO_OST;
		ltds = &lfsck->li_ost_descs;

lable1:
		laia->laia_ltds = ltds;
		down_read(&ltds->ltd_rw_sem);
		cfs_foreach_bit(ltds->ltd_tgts_bitmap, idx) {
			ltd = lfsck_tgt_get(ltds, idx);
			LASSERT(ltd != NULL);

			laia->laia_ltd = ltd;
			up_read(&ltds->ltd_rw_sem);
			ltd->ltd_layout_done = 0;
			rc = lfsck_async_set_info(env, ltd->ltd_exp, ler, set,
					lfsck_layout_master_async_interpret,
					laia);
			if (rc != 0) {
				CERROR("%s: fail to notify %s %x for layout "
				       "start: %d\n", lfsck_lfsck2name(lfsck),
				       (ler->ler_flags & LEF_TO_OST) ? "OST" :
				       "MDT", idx, rc);
				lfsck_tgt_put(ltd);
				lo->ll_flags |= LF_INCOMPLETE;
			} else {
				cnt++;
			}
			down_read(&ltds->ltd_rw_sem);
		}
		up_read(&ltds->ltd_rw_sem);

		/* Sync up */
		if (cnt > 0) {
			rc = ptlrpc_set_wait(set);
			if (rc < 0) {
				ptlrpc_set_destroy(set);
				RETURN(rc);
			}
			cnt = 0;
		}

		if (!(flags & LPF_ALL_TARGETS))
			break;

		ltds = &lfsck->li_mdt_descs;
		/* The sponsor broadcasts the request to other MDTs. */
		if (flags & LPF_BROADCAST) {
			flags &= ~LPF_ALL_TARGETS;
			ler->ler_flags &= ~LEF_TO_OST;
			goto lable1;
		}

		/* non-sponsors link other MDT targets locallly. */
		spin_lock(&ltds->ltd_lock);
		cfs_foreach_bit(ltds->ltd_tgts_bitmap, idx) {
			ltd = LTD_TGT(ltds, idx);
			LASSERT(ltd != NULL);

			if (!cfs_list_empty(&ltd->ltd_layout_list))
				continue;

			cfs_list_add_tail(&ltd->ltd_layout_list,
					  &llmd->llmd_mdt_list);
			cfs_list_add_tail(&ltd->ltd_layout_phase_list,
					  &llmd->llmd_mdt_phase1_list);
		}
		spin_unlock(&ltds->ltd_lock);

		break;
	case LNE_LAYOUT_STOP:
		if (flags & LPF_BROADCAST)
			ler->ler_flags |= LEF_FORCE_STOP;
	case LNE_LAYOUT_PHASE2_DONE:
		/* Notify other MDTs if needed, then the OSTs. */
		if (flags & LPF_ALL_TARGETS) {
			/* The sponsor broadcasts the request to other MDTs. */
			if (flags & LPF_BROADCAST) {
				ler->ler_flags &= ~LEF_TO_OST;
				head = &llmd->llmd_mdt_list;
				ltds = &lfsck->li_mdt_descs;
				goto lable3;
			}

			/* non-sponsors unlink other MDT targets locallly. */
			ltds = &lfsck->li_mdt_descs;
			spin_lock(&ltds->ltd_lock);
			cfs_list_for_each_entry_safe(ltd, next,
						     &llmd->llmd_mdt_list,
						     ltd_layout_list) {
				cfs_list_del_init(&ltd->ltd_layout_phase_list);
				cfs_list_del_init(&ltd->ltd_layout_list);
			}
			spin_unlock(&ltds->ltd_lock);
		}

lable2:
		ler->ler_flags |= LEF_TO_OST;
		head = &llmd->llmd_ost_list;
		ltds = &lfsck->li_ost_descs;

lable3:
		laia->laia_ltds = ltds;
		spin_lock(&ltds->ltd_lock);
		while (!cfs_list_empty(head)) {
			ltd = cfs_list_entry(head->next, struct lfsck_tgt_desc,
					     ltd_layout_list);
			if (!cfs_list_empty(&ltd->ltd_layout_phase_list))
				cfs_list_del_init(&ltd->ltd_layout_phase_list);
			cfs_list_del_init(&ltd->ltd_layout_list);
			laia->laia_ltd = ltd;
			spin_unlock(&ltds->ltd_lock);
			rc = lfsck_async_set_info(env, ltd->ltd_exp, ler, set,
					lfsck_layout_master_async_interpret,
					laia);
			if (rc != 0)
				CERROR("%s: fail to notify %s %x for layout "
				       "stop/phase2: %d\n",
				       lfsck_lfsck2name(lfsck),
				       (ler->ler_flags & LEF_TO_OST) ? "OST" :
				       "MDT", ltd->ltd_index, rc);
			else
				cnt++;
			spin_lock(&ltds->ltd_lock);
		}
		spin_unlock(&ltds->ltd_lock);

		if (!(flags & LPF_BROADCAST))
			break;

		/* Sync up */
		if (cnt > 0) {
			rc = ptlrpc_set_wait(set);
			if (rc < 0) {
				ptlrpc_set_destroy(set);
				RETURN(rc);
			}
			cnt = 0;
		}

		flags &= ~LPF_BROADCAST;
		goto lable2;
	case LNE_LAYOUT_PHASE1_DONE:
		llmd->llmd_touch_gen++;
		ler->ler_flags &= ~LEF_TO_OST;
		ltds = &lfsck->li_mdt_descs;
		laia->laia_ltds = ltds;
		spin_lock(&ltds->ltd_lock);
		while (!cfs_list_empty(&llmd->llmd_mdt_phase1_list)) {
			ltd = cfs_list_entry(llmd->llmd_mdt_phase1_list.next,
					     struct lfsck_tgt_desc,
					     ltd_layout_phase_list);
			if (ltd->ltd_layout_gen == llmd->llmd_touch_gen)
				break;

			ltd->ltd_layout_gen = llmd->llmd_touch_gen;
			cfs_list_del_init(&ltd->ltd_layout_phase_list);
			cfs_list_add_tail(&ltd->ltd_layout_phase_list,
					  &llmd->llmd_mdt_phase1_list);
			laia->laia_ltd = ltd;
			spin_unlock(&ltds->ltd_lock);
			rc = lfsck_async_set_info(env, ltd->ltd_exp, ler, set,
					lfsck_layout_master_async_interpret,
					laia);
			if (rc != 0)
				CERROR("%s: fail to notify MDT %x for layout "
				       "phase1 done: %d\n",
				       lfsck_lfsck2name(lfsck),
				       ltd->ltd_index, rc);
			else
				cnt++;
			spin_lock(&ltds->ltd_lock);
		}
		spin_unlock(&ltds->ltd_lock);

		break;
	default:
		CERROR("%s: unexpected LFSCK event: %u\n",
		       lfsck_lfsck2name(lfsck), ler->ler_event);
		rc = -EINVAL;
		break;
	}

	if (cnt > 0)
		rc = ptlrpc_set_wait(set);
	ptlrpc_set_destroy(set);

	if (rc == 0 && ler->ler_event == LNE_LAYOUT_START &&
	    cfs_list_empty(&llmd->llmd_ost_list))
		rc = -ENODEV;

	RETURN(rc);
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
			     struct lustre_handle *lh, __u64 bits)
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
	policy->l_inodebits.bits = bits;
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

static int lfsck_layout_scan_orphan_one(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_tgt_desc *ltd,
					struct lu_orphan_rec *rec,
					struct lu_fid *cfid)
{
	struct lfsck_layout		*lo	= com->lc_file_ram;
	int				 rc	= 0;

	/* XXX: To be extended in other patch. */

	down_write(&com->lc_sem);
	com->lc_new_scanned++;
	com->lc_new_checked++;
	if (rc > 0) {
		lo->ll_objs_repaired_orphan++;
		rc = 0;
	} else if (rc < 0) {
		lo->ll_objs_failed_phase2++;
	}
	up_write(&com->lc_sem);
	return rc;
}

static int lfsck_layout_scan_orphan(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct lfsck_tgt_desc *ltd)
{
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct idx_info			*ii	= &info->lti_ii;
	struct ost_id			*oi	= &info->lti_oi;
	struct lu_fid			*fid	= &info->lti_fid;
	struct dt_object		*obj;
	const struct dt_it_ops		*iops;
	struct dt_it			*di;
	int				 rc	= 0;
	ENTRY;

	CDEBUG(D_LFSCK, "%s: start the orphan scanning for OST%04x\n",
	       lfsck_lfsck2name(lfsck), ltd->ltd_index);

	ostid_set_seq(oi, FID_SEQ_IDIF);
	ostid_set_id(oi, 0);
	ostid_to_fid(fid, oi, ltd->ltd_index);
	obj = lfsck_object_find_by_dev(env, ltd->ltd_tgt, fid);
	if (unlikely(IS_ERR(obj)))
		RETURN(PTR_ERR(obj));

	rc = obj->do_ops->do_index_try(env, obj, &dt_lfsck_orphan_features);
	if (rc != 0)
		GOTO(put, rc);

	iops = &obj->do_index_ops->dio_it;
	di = iops->init(env, obj, 0, BYPASS_CAPA);
	if (IS_ERR(di))
		GOTO(put, rc = PTR_ERR(di));

	memset(ii, 0, sizeof(ii));
	ii->ii_fid.f_seq = FID_SEQ_LAYOUT_RBTREE;
	ii->ii_fid.f_oid = ltd->ltd_index;
	ii->ii_fid.f_ver = 0;
	ii->ii_index = lfsck_dev_idx(ltd->ltd_tgt);
	rc = iops->get(env, di, (const struct dt_key *)ii);
	if (rc == -ESRCH) {
		/* -ESRCH means that the orphan OST-objects rbtree has been
		 * cleanup because of the OSS server restart or other errors. */
		lo->ll_flags |= LF_INCOMPLETE;
		GOTO(fini, rc);
	}

	if (rc == 0)
		rc = iops->next(env, di);
	else if (rc > 0)
		rc = 0;

	if (rc < 0)
		GOTO(fini, rc);

	if (rc > 0)
		GOTO(fini, rc = 0);

	do {
		struct dt_key		*key;
		struct lu_orphan_rec	*rec = &info->lti_rec;

		key = iops->key(env, di);
		com->lc_fid_latest_scanned_phase2 = *(struct lu_fid *)key;
		rc = iops->rec(env, di, (struct dt_rec *)rec, 0);
		if (rc == 0)
			rc = lfsck_layout_scan_orphan_one(env, com, ltd, rec,
					&com->lc_fid_latest_scanned_phase2);
		if (rc != 0 && bk->lb_param & LPF_FAILOUT)
			GOTO(fini, rc);

		lfsck_control_speed_by_self(com);
		do {
			rc = iops->next(env, di);
		} while (rc < 0 && !(bk->lb_param & LPF_FAILOUT));
	} while (rc == 0);

	GOTO(fini, rc);

fini:
	iops->put(env, di);
	iops->fini(env, di);
put:
	lu_object_put(env, &obj->do_lu);

	CDEBUG(D_LFSCK, "%s: finish the orphan scanning for OST%04x, rc = %d\n",
	       lfsck_lfsck2name(lfsck), ltd->ltd_index, rc);

	return (rc > 0 ? 0 : rc);
}

static int lfsck_layout_recreate_ostobj(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_layout_req *llr,
					struct lu_attr *la)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct filter_fid		*pfid	= &info->lti_new_pfid;
	struct dt_allocation_hint	*hint	= &info->lti_hint;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct dt_object		*child  = llr->llr_child;
	const struct lu_fid		*tfid	= lu_object_fid(&parent->do_lu);
	struct thandle			*handle;
	struct lu_buf			*buf;
	int				 rc;
	ENTRY;

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		RETURN(PTR_ERR(handle));

	hint->dah_parent = 0;
	hint->dah_mode = 0;
	hint->dah_flags = DAHF_RECREATE;

	pfid->ff_parent.f_seq = cpu_to_le64(tfid->f_seq);
	pfid->ff_parent.f_oid = cpu_to_le32(tfid->f_oid);
	/* XXX: In fact, the ff_parent::f_ver is not the real parent
	 *	FID::f_ver, instead, it is the OST-object index in
	 *	its parent MDT-object layout EA. */
	pfid->ff_parent.f_ver = cpu_to_le32(llr->llr_lov_idx);
	buf = lfsck_buf_get(env, pfid, sizeof(struct filter_fid));

	rc = dt_declare_create(env, child, la, hint, NULL, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_declare_xattr_set(env, child, buf, XATTR_NAME_FID,
				  LU_XATTR_CREATE, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_create(env, child, la, hint, NULL, handle);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_xattr_set(env, child, buf, XATTR_NAME_FID, LU_XATTR_CREATE,
			  handle, BYPASS_CAPA);
	if (rc != 0)
		GOTO(stop, rc);

	if (unlikely(lu_object_is_dying(parent->do_lu.lo_header)))
		dt_destroy(env, child, handle);
	else
		rc = 1;

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
				     struct lfsck_layout_req *llr, __u32 idx)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_buf			*buf	= &info->lti_big_buf;
	struct dt_object		*tobj;
	struct lov_mds_md_v1		*lmm;
	struct lov_ost_data_v1		*objs;
	__u32				 magic;
	__u32				 patten;
	int				 rc;
	int				 i;
	__u16				 count;
	ENTRY;

	if (lu_fid_eq(pfid, lu_object_fid(&parent->do_lu))) {
		if (llr->llr_lov_idx == idx)
			RETURN(0);
		else
			RETURN(LLI_UNMATCHED_PAIR);
	}

	tobj = lfsck_object_find(env, com->lc_lfsck, pfid);
	if (tobj == NULL)
		RETURN(LLI_UNMATCHED_PAIR);

	if (IS_ERR(tobj))
		RETURN(PTR_ERR(tobj));

	if (!dt_object_exists(tobj))
		GOTO(out, rc = LLI_UNMATCHED_PAIR);

again:
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

		lu_buf_realloc(buf, rc);
		if (buf->lb_buf == NULL)
			GOTO(out, rc = -ENOMEM);
		goto again;
	}

	if (rc < 0)
		GOTO(out, rc);

	if (unlikely(buf->lb_buf == NULL)) {
		lu_buf_alloc(buf, rc);
		if (buf->lb_buf == NULL)
			GOTO(out, rc = -ENOMEM);
		goto again;
	}

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
					      const struct lu_attr *pla)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct filter_fid		*pfid	= &info->lti_new_pfid;
	struct lu_attr			*tla	= &info->lti_la3;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct dt_object		*child  = llr->llr_child;
	struct thandle			*handle;
	const struct lu_fid		*tfid	= lu_object_fid(&parent->do_lu);
	struct lu_buf			*buf;
	int				 rc;
	ENTRY;

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		RETURN(PTR_ERR(handle));

	pfid->ff_parent.f_seq = cpu_to_le64(tfid->f_seq);
	pfid->ff_parent.f_oid = cpu_to_le32(tfid->f_oid);
	/* XXX: In fact, the ff_parent::f_ver is not the real parent
	 *	FID::f_ver, instead, it is the OST-object index in
	 *	its parent MDT-object layout EA. */
	pfid->ff_parent.f_ver = cpu_to_le32(llr->llr_lov_idx);
	buf = lfsck_buf_get(env, pfid, sizeof(struct filter_fid));

	rc = dt_declare_xattr_set(env, child, buf, XATTR_NAME_FID, 0, handle);
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

	rc = dt_xattr_set(env, child, buf, XATTR_NAME_FID, 0, handle,
			  BYPASS_CAPA);
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

static int lfsck_layout_repair_multiple_references(const struct lu_env *env,
						   struct lfsck_component *com,
						   struct lfsck_layout_req *llr,
						   struct lu_attr *la)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct dt_allocation_hint	*hint	= &info->lti_hint;
	struct dt_object_format 	*dof	= &info->lti_dof;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct lustre_handle		*lh	= &info->lti_lh;
	struct thandle			*handle;
	int				 rc;
	ENTRY;

	rc = lfsck_layout_lock(env, com, parent, lh, MDS_INODELOCK_LAYOUT |
						     MDS_INODELOCK_XATTR);
	if (rc != 0)
		RETURN(rc);

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		GOTO(out, rc = PTR_ERR(handle));

	hint->dah_parent = parent;
	hint->dah_child = NULL;
	hint->dah_flags = DAHF_INDEX;
	hint->dah_ost_index = llr->llr_ost_idx;
	hint->dah_lov_offset = llr->llr_lov_idx;
	hint->dah_gen = llr->llr_parent->llo_gen;
	dof->dof_type = DFT_REGULAR;
	dof->u.dof_reg.striped = 1;
	rc = dt_declare_create(env, parent, la, hint, dof, handle);
	if (rc != 0)
		GOTO(stop, rc = (rc == -EAGAIN ? 0 : rc));

	rc = dt_trans_start(env, dev, handle);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, parent, 0);
	if (unlikely(lu_object_is_dying(parent->do_lu.lo_header)))
		GOTO(unlock, rc = 0);

	rc = dt_create(env, parent, la, hint, dof, handle);
	if (rc == 0)
		rc = 1;
	else if (unlikely(rc == -EAGAIN))
		rc = 0;

	GOTO(unlock, rc);

unlock:
	dt_write_unlock(env, parent);
stop:
	if (unlikely(hint->dah_child != NULL)) {
		lu_object_put_nocache(env, &hint->dah_child->do_lu);
		hint->dah_child = NULL;
	}
	dt_trans_stop(env, dev, handle);
out:
	lfsck_layout_unlock(lh);
	return rc;
}

static int lfsck_layout_repair_owner(const struct lu_env *env,
				     struct lfsck_component *com,
				     struct lfsck_layout_req *llr,
				     struct lu_attr *pla)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_attr			*tla	= &info->lti_la3;
	struct dt_device		*dev	= com->lc_lfsck->li_next;
	struct dt_object		*parent = llr->llr_parent->llo_obj;
	struct dt_object		*child  = llr->llr_child;
	struct thandle			*handle;
	int				 rc;
	ENTRY;

	handle = dt_trans_create(env, dev);
	if (IS_ERR(handle))
		RETURN(PTR_ERR(handle));

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

	/* If some others chown/chgrp during the LFSCK, then related
	 * inconsistency will be repaired automatically. */
	if (unlikely(tla->la_uid != pla->la_uid && tla->la_gid != pla->la_gid))
		GOTO(unlock, rc = 0);

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
					     struct lfsck_layout_req *llr)
{
	struct lfsck_layout		*lo	   = com->lc_file_ram;
	struct lfsck_thread_info	*info	   = lfsck_env_info(env);
	struct filter_fid_old		*pfid_ea   = &info->lti_old_pfid;
	struct lu_fid			*pfid	   = &info->lti_fid;
	struct lu_buf			*buf;
	struct dt_object		*parent    = llr->llr_parent->llo_obj;
	struct dt_object		*child	   = llr->llr_child;
	struct lu_attr			*pla	   = &info->lti_la;
	struct lu_attr			*cla	   = &info->lti_la2;
	struct lfsck_instance		*lfsck	   = com->lc_lfsck;
	struct lfsck_bookmark		*bk	   = &lfsck->li_bookmark_ram;
	enum lfsck_layout_inconsistency  type	   = LLI_NONE;
	__u32				 idx	   = 0;
	int				 rc;
	ENTRY;

	rc = dt_attr_get(env, parent, pla, BYPASS_CAPA);
	if (rc != 0) {
		if (lu_object_is_dying(parent->do_lu.lo_header))
			RETURN(0);

		GOTO(out, rc);
	}

	rc = dt_attr_get(env, child, cla, BYPASS_CAPA);
	if (rc == -ENOENT) {
		if (lu_object_is_dying(parent->do_lu.lo_header))
			RETURN(0);

		type = LLI_DANGLING;
		goto repair;
	}

	if (rc != 0)
		GOTO(out, rc);

	buf = lfsck_buf_get(env, pfid_ea, sizeof(struct filter_fid_old));
	rc= dt_xattr_get(env, child, buf, XATTR_NAME_FID, BYPASS_CAPA);
	if (rc >= 0 && rc != sizeof(struct filter_fid_old) &&
	    rc != sizeof(struct filter_fid)) {
		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	if (rc < 0 && rc != -ENODATA)
		GOTO(out, rc);

	if (rc == -ENODATA) {
		fid_zero(pfid);
	} else {
		fid_le_to_cpu(pfid, &pfid_ea->ff_parent);
		/* XXX: OST-object does not save parent FID::f_ver, instead,
		 *	the OST-object index in the parent MDT-object layout
		 *	EA reuses the pfid->f_ver. */
		idx = pfid->f_ver;
		pfid->f_ver = 0;
	}

	if (fid_is_zero(pfid)) {
		/* client never write. */
		if (cla->la_size == 0 && cla->la_blocks == 0) {
			if (cla->la_uid != pla->la_uid ||
			    cla->la_gid != pla->la_gid) {
				type = LLI_INCONSISTENT_OWNER;
				goto repair;
			}

			RETURN(0);
		}

		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	if (!fid_is_sane(pfid)) {
		type = LLI_UNMATCHED_PAIR;
		goto repair;
	}

	rc = lfsck_layout_check_parent(env, com, parent, pfid,
				       lu_object_fid(&child->do_lu),
				       llr, idx);
	if (rc > 0) {
		type = rc;
		goto repair;
	}

	if (rc < 0)
		GOTO(out, rc);

	if (cla->la_uid != pla->la_uid || cla->la_gid != pla->la_gid) {
		type = LLI_INCONSISTENT_OWNER;
		goto repair;
	}

repair:
	if (bk->lb_param & LPF_DRYRUN) {
		if (type != LLI_NONE)
			GOTO(out, rc = 1);
		else
			GOTO(out, rc = 0);
	}

	switch (type) {
	case LLI_DANGLING:
		memset(cla, 0, sizeof(*cla));
		cla->la_uid = pla->la_uid;
		cla->la_gid = pla->la_gid;
		/* Normally, the OST-object is non-executable. We use a special
		 * file mode: 0667, to indicate that it is created by the LSFCK
		 * for repairing dangling reference case. The LFSCK maybe wrong
		 * and it is possible that it will find out the real OST-object
		 * which should be referenced by this MDT-object as processing.
		 * Under such case, via checking the OST-object mode, the LFSCK
		 * will know whether the current OST-object which is referenced
		 * by the MDT-object was created for repairing dangling case or
		 * not. It will indicate the LFSCK how to process next step. */
		cla->la_mode = S_IFREG | S_IRUGO | S_IWUGO | S_IXOTH;
		cla->la_valid = LA_TYPE | LA_MODE | LA_UID | LA_GID |
				LA_ATIME | LA_MTIME | LA_CTIME;
		rc = lfsck_layout_recreate_ostobj(env, com, llr, cla);
		break;
	case LLI_UNMATCHED_PAIR:
		rc = lfsck_layout_repair_unmatched_pair(env, com, llr, pla);
		break;
	case LLI_MULTIPLE_REFERENCED:
		rc = lfsck_layout_repair_multiple_references(env, com,
							     llr, pla);
		break;
	case LLI_INCONSISTENT_OWNER:
		rc = lfsck_layout_repair_owner(env, com, llr, pla);
		break;
	default:
		rc = 0;
		break;
	}

	GOTO(out, rc);

out:
	down_write(&com->lc_sem);
	if (rc < 0) {
		/* If cannot touch the target server,
		 * mark the LFSCK as INCOMPLETE. */
		if (rc == -ENOTCONN || rc == -ESHUTDOWN || rc == -ETIMEDOUT ||
		    rc == -EHOSTDOWN || rc == -EHOSTUNREACH) {
			lo->ll_flags |= LF_INCOMPLETE;
			lo->ll_objs_skipped++;
			rc = 0;
		} else {
			lo->ll_objs_failed_phase1++;
		}
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
	struct lfsck_thread_args	*lta	= args;
	struct lu_env			*env	= &lta->lta_env;
	struct lfsck_component		*com    = lta->lta_com;
	struct lfsck_instance		*lfsck  = lta->lta_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_position		*pos	= &com->lc_pos_start;
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lfsck_event_request	*ler	= &info->lti_ler;
	struct lfsck_start		*start	= &ler->u.ler_start;
	struct lfsck_stop		*stop	= &ler->u.ler_stop;
	struct lfsck_layout_master_data *llmd   = com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct lfsck_layout_req		*llr;
	struct l_wait_info		 lwi    = { 0 };
	int				 rc	= 0;
	int				 rc1	= 0;
	__u32				 flags;
	ENTRY;

	if (lta->lta_lsp->lsp_start != NULL)
		flags  = lta->lta_lsp->lsp_start->ls_flags;
	else
		flags = bk->lb_param;
	memset(ler, 0, sizeof(*ler));
	ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
	ler->ler_event = LNE_LAYOUT_START;
	start->ls_speed_limit = bk->lb_speed_limit;
	start->ls_version = bk->lb_version;
	start->ls_active = LT_LAYOUT;
	start->ls_flags = bk->lb_param;
	start->ls_valid = LSV_SPEED_LIMIT | LSV_ERROR_HANDLE | LSV_DRYRUN;
	if (pos->lp_oit_cookie <= 1)
		start->ls_flags |= LPF_RESET;

	rc = lfsck_layout_master_notify_others(env, com, ler, flags);
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
		while (!cfs_list_empty(&llmd->llmd_req_list)) {
			llr = cfs_list_entry(llmd->llmd_req_list.next,
					     struct lfsck_layout_req,
					     llr_list);
			rc = lfsck_layout_assistant_handle_one(env, com, llr);
			spin_lock(&llmd->llmd_lock);
			cfs_list_del_init(&llr->llr_list);
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(env, llr);

			if (rc < 0 && bk->lb_param & LPF_FAILOUT)
				GOTO(cleanup1, rc);

			if (unlikely(llmd->llmd_exit))
				GOTO(cleanup1, rc = 0);
		}

		l_wait_event(thread->t_ctl_waitq,
			     llmd->llmd_exit ||
			     !lfsck_layout_req_empty(llmd) ||
			     llmd->llmd_to_post ||
			     llmd->llmd_to_double_scan,
			     &lwi);

		if (unlikely(llmd->llmd_exit))
			GOTO(cleanup1, rc = 0);

		if (!cfs_list_empty(&llmd->llmd_req_list))
			continue;

		if (llmd->llmd_to_post) {
			llmd->llmd_to_post = 0;
			memset(ler, 0, sizeof(*ler));
			ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
			if (llmd->llmd_post_result > 0) {
				ler->ler_event = LNE_LAYOUT_PHASE1_DONE;
				ler->u.ler_status = llmd->llmd_post_result;
			} else {
				ler->ler_event = LNE_LAYOUT_STOP;
				if (llmd->llmd_post_result == 0) {
					if (lfsck->li_status == LS_PAUSED ||
					    lfsck->li_status == LS_CO_PAUSED) {
						flags = 0;
						stop->ls_status = LS_CO_PAUSED;
					} else if (lfsck->li_status ==
								LS_STOPPED ||
						 lfsck->li_status ==
								LS_CO_STOPPED) {
						flags = lfsck->li_flags;
						if (flags & LPF_BROADCAST)
							stop->ls_status =
								LS_STOPPED;
						else
							stop->ls_status =
								LS_CO_STOPPED;
					} else {
						LBUG();
					}
				} else {
					flags = 0;
					stop->ls_status = LS_CO_FAILED;
				}
			}

			rc = lfsck_layout_master_notify_others(env, com, ler,
							       flags);
			if (rc != 0)
				CERROR("%s: failed to notify others for layout "
				       "post: %d\n",
				       lfsck_lfsck2name(lfsck), rc);

			if (llmd->llmd_post_result <= 0)
				GOTO(fini, rc);

			/* Wakeup the master engine to go ahead. */
			wake_up_all(&thread->t_ctl_waitq);
		}

		if (llmd->llmd_to_double_scan) {
			llmd->llmd_to_double_scan = 0;
			atomic_inc(&lfsck->li_double_scan_count);
			llmd->llmd_in_double_scan = 1;
			wake_up_all(&thread->t_ctl_waitq);

			com->lc_new_checked = 0;
			com->lc_new_scanned = 0;
			com->lc_time_last_checkpoint = cfs_time_current();
			com->lc_time_next_checkpoint =
				com->lc_time_last_checkpoint +
				cfs_time_seconds(LFSCK_CHECKPOINT_INTERVAL);

			while (llmd->llmd_in_double_scan) {
				struct lfsck_tgt_descs	*ltds =
							&lfsck->li_ost_descs;
				struct lfsck_tgt_desc	*ltd;

				/* Pull LFSCK status on related targets once
				 * per 30 seconds if we are not notified. */
				lwi = LWI_TIMEOUT(cfs_time_seconds(30),
						  NULL, NULL);
				rc = l_wait_event(thread->t_ctl_waitq,
					llmd->llmd_exit ||
					lfsck_layout_master_wait_others(env,
								com) != 0,
					&lwi);
				if (unlikely(llmd->llmd_exit))
					GOTO(cleanup2, rc = 0);

				if (rc == -ETIMEDOUT)
					continue;

				if (rc < 0)
					GOTO(cleanup2, rc);

				spin_lock(&ltds->ltd_lock);
				while (!cfs_list_empty(
						&llmd->llmd_ost_phase2_list)) {
					ltd = cfs_list_entry(
					      llmd->llmd_ost_phase2_list.next,
					      struct lfsck_tgt_desc,
					      ltd_layout_phase_list);
					cfs_list_del_init(
						&ltd->ltd_layout_phase_list);
					spin_unlock(&ltds->ltd_lock);

					if (bk->lb_param & LPF_ALL_TARGETS) {
						rc = lfsck_layout_scan_orphan(
								env, com, ltd);
						if (rc != 0 &&
						    bk->lb_param & LPF_FAILOUT)
							GOTO(cleanup2, rc);
					}

					if (unlikely(llmd->llmd_exit))
						GOTO(cleanup2, rc = 0);

					spin_lock(&ltds->ltd_lock);
				}

				if (cfs_list_empty(
						&llmd->llmd_ost_phase1_list)) {
					spin_unlock(&ltds->ltd_lock);
					GOTO(cleanup2, rc = 1);
				}
				spin_unlock(&ltds->ltd_lock);
			}
		}
	}

cleanup1:
	if (!llmd->llmd_exit) {
		LASSERT(cfs_list_empty(&llmd->llmd_req_list));
	} else {
		/* Cleanup the unfinished requests. */
		spin_lock(&llmd->llmd_lock);
		if (rc < 0)
			llmd->llmd_assistant_status = rc;
		while (!cfs_list_empty(&llmd->llmd_req_list)) {
			llr = cfs_list_entry(llmd->llmd_req_list.next,
					     struct lfsck_layout_req,
					     llr_list);
			cfs_list_del_init(&llr->llr_list);
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(env, llr);
			spin_lock(&llmd->llmd_lock);
		}
		spin_unlock(&llmd->llmd_lock);
	}

cleanup2:
	memset(ler, 0, sizeof(*ler));
	ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
	if (rc > 0) {
		ler->ler_event = LNE_LAYOUT_PHASE2_DONE;
		ler->u.ler_status = rc;
	} else if (rc == 0) {
		ler->ler_event = LNE_LAYOUT_STOP;
		if (lfsck->li_status == LS_PAUSED ||
		    lfsck->li_status == LS_CO_PAUSED) {
			flags = 0;
			stop->ls_status = LS_CO_PAUSED;
		} else if (lfsck->li_status == LS_STOPPED ||
			 lfsck->li_status == LS_CO_STOPPED) {
			flags = lfsck->li_flags;
			if (flags & LPF_BROADCAST)
				stop->ls_status = LS_STOPPED;
			else
				stop->ls_status = LS_CO_STOPPED;
		} else {
			LBUG();
		}
	} else {
		ler->ler_event = LNE_LAYOUT_STOP;
		flags = 0;
		stop->ls_status = LS_CO_FAILED;
	}
	rc1 = lfsck_layout_master_notify_others(env, com, ler, flags);
	if (rc1 != 0) {
		CERROR("%s: failed to notify others for layout quit: %d\n",
		       lfsck_lfsck2name(lfsck), rc1);
		rc = rc1;
	}

	/* Under force exit case, some requests may be just freed without
	 * verification, those objects should be re-handled when next run.
	 * So not update the on-disk tracing file under such case. */
	if (!llmd->llmd_exit)
		rc1 = lfsck_layout_double_scan_result(env, com, rc);

fini:
	spin_lock(&llmd->llmd_lock);
	if (llmd->llmd_in_double_scan &&
	    atomic_dec_and_test(&lfsck->li_double_scan_count))
		wake_up_all(&lfsck->li_thread.t_ctl_waitq);
	llmd->llmd_assistant_status = (rc1 != 0 ? rc1 : rc);
	thread_set_flags(thread, SVC_STOPPED);
	wake_up_all(&thread->t_ctl_waitq);
	spin_unlock(&llmd->llmd_lock);
	lfsck_thread_args_fini(lta);
	return rc;
}

static int
lfsck_layout_slave_async_interpret(const struct lu_env *env,
				   struct ptlrpc_request *req,
				   void *args, int rc)
{
	struct lfsck_event_request	     *ler;
	struct lfsck_layout_slave_async_args *llsaa = args;
	struct obd_export		     *exp   = llsaa->llsaa_exp;
	struct lfsck_component		     *com   = llsaa->llsaa_com;
	struct lfsck_layout_slave_target     *llst  = llsaa->llsaa_llst;
	struct lfsck_layout_slave_data	     *llsd  = com->lc_data;
	bool				      done  = false;

	if (rc != 0) {
		/* It is quite probably caused by target crash,
		 * to make the LFSCK can go ahead, assume that
		 * the target finished the LFSCK prcoessing. */
		done = true;
	} else {
		ler = req_capsule_server_get(&req->rq_pill, &RMF_GETINFO_VAL);
		if (ptlrpc_req_need_swab(req))
			lustre_swab_lfsck_event_request(ler);

		if (ler->u.ler_status != LS_SCANNING_PHASE1 &&
		    ler->u.ler_status != LS_SCANNING_PHASE2)
			done = true;
	}
	if (done)
		lfsck_layout_llst_del(llsd, llst);
	lfsck_layout_llst_put(llst);
	lfsck_component_put(env, com);
	class_export_put(exp);
	return 0;
}

static int lfsck_layout_slave_get_info(const struct lu_env *env,
				       struct lfsck_component *com,
				       struct obd_export *exp,
				       struct lfsck_layout_slave_target *llst,
				       struct ptlrpc_request_set *set)
{
	struct lfsck_layout_slave_async_args *llsaa;
	struct ptlrpc_request		     *req;
	char				     *tmp;
	__u32				     *vallen;
	__u32				      size	=
					sizeof(struct lfsck_event_request);
	int				      rc;
	ENTRY;

	req = ptlrpc_request_alloc(class_exp2cliimp(exp), &RQF_MDS_GET_INFO);
	if (req == NULL)
		RETURN(-ENOMEM);

	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_KEY,
			     RCL_CLIENT, strlen(KEY_LFSCK_EVENT));
	rc = ptlrpc_request_pack(req, LUSTRE_MDS_VERSION, MDS_GET_INFO);
	if (rc != 0) {
		ptlrpc_request_free(req);
		RETURN(rc);
	}

	tmp = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_KEY);
	memcpy(tmp, KEY_LFSCK_EVENT, strlen(KEY_LFSCK_EVENT));
	vallen = req_capsule_client_get(&req->rq_pill, &RMF_GETINFO_VALLEN);
	*vallen = size;
	req_capsule_set_size(&req->rq_pill, &RMF_GETINFO_VAL,
			     RCL_SERVER, size);
	ptlrpc_request_set_replen(req);

	lfsck_component_get(com);
	llsaa = ptlrpc_req_async_args(req);
	llsaa->llsaa_exp = exp;
	llsaa->llsaa_com = com;
	llsaa->llsaa_llst = llst;
	req->rq_interpret_reply = lfsck_layout_slave_async_interpret;
	ptlrpc_set_add_req(set, req);

	RETURN(0);
}

static int
lfsck_layout_slave_query_master(const struct lu_env *env,
				struct lfsck_component *com)
{
	struct lfsck_instance		 *lfsck = com->lc_lfsck;
	struct lfsck_layout_slave_data	 *llsd  = com->lc_data;
	struct lfsck_layout_slave_target *llst;
	struct obd_export		 *exp;
	struct ptlrpc_request_set	 *set;
	int				  cnt   = 0;
	int				  rc    = 0;
	int				  rc1   = 0;
	ENTRY;

	set = ptlrpc_prep_set();
	if (set == NULL)
		RETURN(-ENOMEM);

	llsd->llsd_touch_gen++;
	spin_lock(&llsd->llsd_lock);
	while (!cfs_list_empty(&llsd->llsd_master_list)) {
		llst = cfs_list_entry(llsd->llsd_master_list.next,
				      struct lfsck_layout_slave_target,
				      llst_list);
		if (llst->llst_gen == llsd->llsd_touch_gen)
			break;

		llst->llst_gen = llsd->llsd_touch_gen;
		cfs_list_del_init(&llst->llst_list);
		cfs_list_add_tail(&llst->llst_list,
				  &llsd->llsd_master_list);
		atomic_inc(&llst->llst_ref);
		spin_unlock(&llsd->llsd_lock);

		exp = lustre_find_lwp_by_index(lfsck->li_obd->obd_name,
					       llst->llst_index);
		if (exp == NULL) {
			lfsck_layout_llst_del(llsd, llst);
			lfsck_layout_llst_put(llst);
			spin_lock(&llsd->llsd_lock);
			continue;
		}

		rc = lfsck_layout_slave_get_info(env, com, exp, llst, set);
		if (rc != 0) {
			CERROR("%s: slave fail to query %s for layout: %d\n",
			       lfsck_lfsck2name(lfsck),
			       exp->exp_obd->obd_name, rc);
			rc1 = rc;
			lfsck_layout_llst_put(llst);
			class_export_put(exp);
		} else {
			cnt++;
		}
		spin_lock(&llsd->llsd_lock);
	}
	spin_unlock(&llsd->llsd_lock);

	if (cnt > 0)
		rc = ptlrpc_set_wait(set);
	ptlrpc_set_destroy(set);

	RETURN(rc1 != 0 ? rc1 : rc);
}

static int lfsck_layout_slave_check_active(const struct lu_env *env,
					   struct lfsck_component *com)
{
	struct lfsck_layout_slave_data *llsd = com->lc_data;
	int				rc;

	rc = lfsck_layout_slave_query_master(env, com);
	if (cfs_list_empty(&llsd->llsd_master_list))
		return 1;

	return rc;
}

static void
lfsck_layout_slave_notify_master(const struct lu_env *env,
				 struct lfsck_component *com,
				 enum lfsck_notify_events event, int result)
{
	struct lfsck_instance		 *lfsck = com->lc_lfsck;
	struct lfsck_layout_slave_data	 *llsd  = com->lc_data;
	struct lfsck_event_request	 *ler   = &lfsck_env_info(env)->lti_ler;
	struct lfsck_layout_slave_target *llst;
	struct obd_export		 *exp;
	struct ptlrpc_request_set	 *set;
	int				  cnt   = 0;
	int				  rc;

	set = ptlrpc_prep_set();
	if (set == NULL)
		return;

	memset(ler, 0, sizeof(*ler));
	ler->ler_event = event;
	ler->ler_flags = LEF_FROM_OST;
	ler->u.ler_status = result;
	ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
	llsd->llsd_touch_gen++;
	spin_lock(&llsd->llsd_lock);
	while (!cfs_list_empty(&llsd->llsd_master_list)) {
		llst = cfs_list_entry(llsd->llsd_master_list.next,
				      struct lfsck_layout_slave_target,
				      llst_list);
		if (llst->llst_gen == llsd->llsd_touch_gen)
			break;

		llst->llst_gen = llsd->llsd_touch_gen;
		cfs_list_del_init(&llst->llst_list);
		cfs_list_add_tail(&llst->llst_list,
				  &llsd->llsd_master_list);
		atomic_inc(&llst->llst_ref);
		spin_unlock(&llsd->llsd_lock);

		exp = lustre_find_lwp_by_index(lfsck->li_obd->obd_name,
					       llst->llst_index);
		if (exp == NULL) {
			lfsck_layout_llst_del(llsd, llst);
			lfsck_layout_llst_put(llst);
			spin_lock(&llsd->llsd_lock);
			continue;
		}

		rc = do_set_info_async(class_exp2cliimp(exp), MDS_SET_INFO,
				       LUSTRE_MDS_VERSION,
				       sizeof(KEY_LFSCK_EVENT),
				       KEY_LFSCK_EVENT, sizeof(*ler), ler, set);
		if (rc != 0)
			CERROR("%s: slave fail to notify %s for layout: %d\n",
			       lfsck_lfsck2name(lfsck),
			       exp->exp_obd->obd_name, rc);
		else
			cnt++;
		lfsck_layout_llst_put(llst);
		class_export_put(exp);
		spin_lock(&llsd->llsd_lock);
	}
	spin_unlock(&llsd->llsd_lock);

	if (cnt > 0)
		rc = ptlrpc_set_wait(set);
	ptlrpc_set_destroy(set);
}

/* layout APIs */

static int lfsck_layout_reset(const struct lu_env *env,
			      struct lfsck_component *com, bool init)
{
	struct lfsck_layout	*lo    = com->lc_file_ram;
	int			 rc;

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

	rc = lfsck_layout_store(env, com);
	up_write(&com->lc_sem);
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
		     cfs_list_empty(&llmd->llmd_req_list),
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
			fid_zero(&com->lc_fid_latest_scanned_phase2);
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
				   struct lfsck_component *com,
				   struct lfsck_start_param *lsp)
{
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	int				 rc;

	rc = lfsck_layout_prep(env, com);
	if (rc != 0 || lo->ll_status != LS_SCANNING_PHASE1 ||
	    !lsp->lsp_index_valid)
		return rc;

	rc = lfsck_layout_llst_add(llsd, lsp->lsp_index);
	if (rc == 0 && !(lo->ll_flags & LF_INCOMPLETE)) {
		LASSERT(!llsd->llsd_rbtree_valid);

		write_lock(&llsd->llsd_rb_lock);
		rc = lfsck_rbtree_setup(env, com);
		write_unlock(&llsd->llsd_rb_lock);
	}
	return rc;
}

static int lfsck_layout_master_prep(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct lfsck_start_param *lsp)
{
	struct lfsck_instance		*lfsck  = com->lc_lfsck;
	struct lfsck_layout_master_data *llmd   = com->lc_data;
	struct ptlrpc_thread		*thread = &llmd->llmd_thread;
	struct lfsck_thread_args	*lta;
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

	lta = lfsck_thread_args_init(lfsck, com, lsp);
	if (IS_ERR(lta))
		RETURN(PTR_ERR(lta));

	rc = PTR_ERR(kthread_run(lfsck_layout_assistant, lta, "lfsck_layout"));
	if (IS_ERR_VALUE(rc)) {
		CERROR("%s: Cannot start LFSCK layout assistant thread: %ld\n",
		       lfsck_lfsck2name(lfsck), rc);
		lfsck_thread_args_fini(lta);
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
				     struct lov_mds_md_v1 *lmm)
{
	struct lfsck_thread_info	*info 	= lfsck_env_info(env);
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_master_data *llmd	= com->lc_data;
	struct lfsck_layout_object	*llo	= NULL;
	struct lov_ost_data_v1		*objs;
	struct lfsck_tgt_descs		*ltds	= &lfsck->li_ost_descs;
	struct lu_buf			*buf;
	int				 rc	= 0;
	int				 i;
	__u16				 count;
	__u16				 gen;
	ENTRY;

	buf = lfsck_buf_get(env, &info->lti_old_pfid,
			    sizeof(struct filter_fid_old));
	count = le16_to_cpu(lmm->lmm_stripe_count);
	gen = le16_to_cpu(lmm->lmm_layout_gen);
	if (le32_to_cpu(lmm->lmm_magic) == LOV_MAGIC_V1)
		objs = &(lmm->lmm_objects[0]);
	else
		objs = &((struct lov_mds_md_v3 *)lmm)->lmm_objects[0];

	for (i = 0; i < count; i++, objs++) {
		struct lu_fid		*fid	= &info->lti_fid;
		struct ost_id		*oi	= &info->lti_oi;
		struct lfsck_layout_req *llr;
		struct lfsck_tgt_desc	*tgt	= NULL;
		struct dt_object	*cobj	= NULL;
		struct thandle		*th	= NULL;
		__u32			 index	=
					le32_to_cpu(objs->l_ost_idx);

		ostid_le_to_cpu(&objs->l_ost_oi, oi);
		ostid_to_fid(fid, oi, index);
		tgt = lfsck_tgt_get(ltds, index);
		if (unlikely(tgt == NULL)) {
			lo->ll_flags |= LF_INCOMPLETE;
			goto next;
		}

		cobj = lfsck_object_find_by_dev(env, tgt->ltd_tgt, fid);
		if (IS_ERR(cobj)) {
			rc = PTR_ERR(cobj);
			goto next;
		}

		th = dt_trans_create(env, tgt->ltd_tgt);
		if (IS_ERR(th)) {
			rc = PTR_ERR(th);
			goto next;
		}

		LASSERT(th->th_dummy);

		rc = dt_declare_attr_get(env, cobj, BYPASS_CAPA);
		if (rc != 0)
			goto next;

		rc = dt_declare_xattr_get(env, cobj, buf, XATTR_NAME_FID,
					  BYPASS_CAPA);
		if (rc != 0)
			goto next;

		rc = dt_trans_start(env, tgt->ltd_tgt, th);
		if (rc != 0)
			goto next;

		if (llo == NULL) {
			llo = lfsck_layout_object_init(env, parent, gen);
			if (IS_ERR(llo)) {
				rc = PTR_ERR(llo);
				goto next;
			}
		}

		llr = lfsck_layout_req_init(llo, cobj, index, i);
		if (llr == NULL) {
			rc = -ENOMEM;
			goto next;
		}

		cobj = NULL;
		spin_lock(&llmd->llmd_lock);
		if (llmd->llmd_assistant_status < 0) {
			spin_unlock(&llmd->llmd_lock);
			lfsck_layout_req_fini(env, llr);
			dt_trans_stop(env, tgt->ltd_tgt, th);
			lfsck_tgt_put(tgt);
			RETURN(llmd->llmd_assistant_status);
		}

		cfs_list_add_tail(&llr->llr_list, &llmd->llmd_req_list);
		spin_unlock(&llmd->llmd_lock);
		wake_up_all(&llmd->llmd_thread.t_ctl_waitq);

next:
		down_write(&com->lc_sem);
		com->lc_new_checked++;
		if (rc < 0)
			lo->ll_objs_failed_phase1++;
		up_write(&com->lc_sem);

		if (th != NULL && !IS_ERR(th))
			dt_trans_stop(env, tgt->ltd_tgt, th);

		if (cobj != NULL && !IS_ERR(cobj))
			lu_object_put(env, &cobj->do_lu);

		if (likely(tgt != NULL))
			lfsck_tgt_put(tgt);

		if (rc < 0 && bk->lb_param & LPF_FAILOUT)
			break;

		rc = 0;
	}

	if (llo != NULL && !IS_ERR(llo))
		lfsck_layout_object_put(env, llo);

	RETURN(rc);
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
	struct lu_buf			*buf	= &info->lti_big_buf;
	ssize_t				 buflen = buf->lb_len;
	struct lov_mds_md_v1		*lmm	= NULL;
	__u32				 magic;
	__u32				 patten;
	int				 rc	= 0;
	bool				 to_fix	= false;
	bool				 lock1	= false;
	bool				 lock2	= false;
	bool				 stripe = false;
	ENTRY;

	/* For the given object, read its layout EA locally. For each stripe,
	 * pre-fetch the OST-object's attribute and generate an structure
	 * lfsck_layout_req on the list ::llmd_req_list.
	 *
	 * For each request on the ::llmd_req_list, the lfsck_layout_assistant
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

lock:
	if (to_fix) {
		rc = lfsck_layout_lock(env, com, obj, lh, MDS_INODELOCK_LAYOUT |
							  MDS_INODELOCK_XATTR);
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

		lu_buf_realloc(buf, rc);
		buflen = buf->lb_len;
		if (buf->lb_buf == NULL)
			GOTO(out, rc = -ENOMEM);
		goto again;
	} else if (rc <= 0) {
		if (rc == -ENODATA)
			rc = 0;
		GOTO(out, rc);
	} else if (unlikely(buflen == 0)) {
		lu_buf_alloc(buf, rc);
		buflen = buf->lb_len;
		if (buf->lb_buf == NULL)
			GOTO(out, rc = -ENOMEM);
		goto again;
	}

	buf->lb_len = rc;
	lmm = buf->lb_buf;
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
				buf->lb_len = buflen;
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

	GOTO(out, stripe = true);

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

	if (stripe) {
		rc = lfsck_layout_scan_stripes(env, com, obj, lmm);
	} else {
		down_write(&com->lc_sem);
		com->lc_new_checked++;
		if (rc < 0)
			lo->ll_objs_failed_phase1++;
		up_write(&com->lc_sem);
	}
	buf->lb_len = buflen;
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

	LASSERT(llsd != NULL);

	lfsck_rbtree_update_bitmap(env, com, fid, false);

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

			lfsck->li_out_notify(env, lfsck->li_out_notify_data,
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
			     cfs_list_empty(&llmd->llmd_req_list),
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
	bool			 done  = false;

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
			done = true;
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

	if (done)
		lfsck->li_out_notify(env, lfsck->li_out_notify_data,
				     LNE_LASTID_REBUILT);

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
	if (result <= 0)
		lfsck_rbtree_cleanup(env, com);

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
	} else if (lo->ll_status == LS_SCANNING_PHASE2) {
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
			      "real-time_speed_phase2: N/A\n"
			      "current_position: "DFID"\n",
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
			      new_checked,
			      PFID(&com->lc_fid_latest_scanned_phase2));
		if (rc <= 0)
			goto out;

		buf += rc;
		len -= rc;
	} else {
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
	struct lfsck_instance	*lfsck  = com->lc_lfsck;
	struct lfsck_layout	*lo     = com->lc_file_ram;
	struct ptlrpc_thread	*thread = &lfsck->li_thread;
	int			 rc;
	ENTRY;

	if (unlikely(lo->ll_status != LS_SCANNING_PHASE2)) {
		lfsck_rbtree_cleanup(env, com);
		RETURN(0);
	}

	atomic_inc(&lfsck->li_double_scan_count);

	com->lc_new_checked = 0;
	com->lc_new_scanned = 0;
	com->lc_time_last_checkpoint = cfs_time_current();
	com->lc_time_next_checkpoint = com->lc_time_last_checkpoint +
				cfs_time_seconds(LFSCK_CHECKPOINT_INTERVAL);

	while (1) {
		struct l_wait_info lwi = LWI_TIMEOUT(cfs_time_seconds(30),
						     NULL, NULL);

		rc = l_wait_event(thread->t_ctl_waitq,
				!thread_is_running(thread) ||
				lfsck_layout_slave_check_active(env, com) != 0,
				&lwi);
		if (unlikely(!thread_is_running(thread)))
			GOTO(done, rc = 0);

		if (rc == -ETIMEDOUT)
			continue;

		GOTO(done, rc = (rc < 0 ? rc : 1));
	}

done:
	rc = lfsck_layout_double_scan_result(env, com, rc);

	lfsck_rbtree_cleanup(env, com);
	if (atomic_dec_and_test(&lfsck->li_double_scan_count))
		wake_up_all(&lfsck->li_thread.t_ctl_waitq);

	return rc;
}

static void lfsck_layout_master_data_release(const struct lu_env *env,
					     struct lfsck_component *com)
{
	struct lfsck_layout_master_data	*llmd   = com->lc_data;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_tgt_descs		*ltds;
	struct lfsck_tgt_desc		*ltd;
	struct lfsck_tgt_desc		*next;

	LASSERT(llmd != NULL);
	LASSERT(thread_is_init(&llmd->llmd_thread) ||
		thread_is_stopped(&llmd->llmd_thread));
	LASSERT(cfs_list_empty(&llmd->llmd_req_list));
	LASSERT(atomic_read(&llmd->llmd_rpc_in_flight) == 0);

	com->lc_data = NULL;

	ltds = &lfsck->li_ost_descs;
	spin_lock(&ltds->ltd_lock);
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_ost_phase1_list,
				     ltd_layout_phase_list) {
		cfs_list_del_init(&ltd->ltd_layout_phase_list);
	}
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_ost_phase2_list,
				     ltd_layout_phase_list) {
		cfs_list_del_init(&ltd->ltd_layout_phase_list);
	}
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_ost_list,
				     ltd_layout_list) {
		cfs_list_del_init(&ltd->ltd_layout_list);
	}
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_mdt_phase1_list,
				     ltd_layout_phase_list) {
		cfs_list_del_init(&ltd->ltd_layout_phase_list);
	}
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_mdt_phase2_list,
				     ltd_layout_phase_list) {
		cfs_list_del_init(&ltd->ltd_layout_phase_list);
	}
	cfs_list_for_each_entry_safe(ltd, next, &llmd->llmd_mdt_list,
				     ltd_layout_list) {
		cfs_list_del_init(&ltd->ltd_layout_list);
	}
	spin_unlock(&ltds->ltd_lock);

	OBD_FREE(llmd, sizeof(*llmd));
}

static void lfsck_layout_slave_data_release(const struct lu_env *env,
					    struct lfsck_component *com)
{
	struct lfsck_layout_slave_data	 *llsd	= com->lc_data;
	struct lfsck_layout_seq		 *lls;
	struct lfsck_layout_seq		 *next;
	struct lfsck_layout_slave_target *llst;
	struct lfsck_layout_slave_target *tmp;

	LASSERT(llsd != NULL);

	cfs_list_for_each_entry_safe(lls, next, &llsd->llsd_seq_list,
				     lls_list) {
		cfs_list_del_init(&lls->lls_list);
		lfsck_object_put(env, lls->lls_lastid_obj);
		OBD_FREE(lls, sizeof(*lls));
	}

	cfs_list_for_each_entry_safe(llst, tmp, &llsd->llsd_master_list,
				     llst_list) {
		cfs_list_del_init(&llst->llst_list);
		OBD_FREE_PTR(llst);
	}

	lfsck_rbtree_cleanup(env, com);
	com->lc_data = NULL;
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

static void lfsck_layout_slave_quit(const struct lu_env *env,
				    struct lfsck_component *com)
{
	lfsck_rbtree_cleanup(env, com);
}

static int lfsck_layout_query(const struct lu_env *env,
			      struct lfsck_component *com)
{
	struct lfsck_layout *lo = com->lc_file_ram;

	return lo->ll_status;
}

static int lfsck_layout_master_in_notify(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct lfsck_event_request *ler)
{
	struct lfsck_instance		*lfsck = com->lc_lfsck;
	struct lfsck_layout		*lo    = com->lc_file_ram;
	struct lfsck_layout_master_data *llmd  = com->lc_data;
	struct lfsck_tgt_descs		*ltds;
	struct lfsck_tgt_desc		*ltd;

	if (ler->ler_event != LNE_LAYOUT_PHASE1_DONE &&
	    ler->ler_event != LNE_LAYOUT_PHASE2_DONE &&
	    ler->ler_event != LNE_LAYOUT_STOP)
		return -EINVAL;

	if (ler->ler_flags & LEF_FROM_OST)
		ltds = &lfsck->li_ost_descs;
	else
		ltds = &lfsck->li_mdt_descs;
	spin_lock(&ltds->ltd_lock);
	ltd = LTD_TGT(ltds, ler->ler_index);
	if (ltd == NULL) {
		spin_unlock(&ltds->ltd_lock);
		return -ENODEV;
	}

	cfs_list_del_init(&ltd->ltd_layout_phase_list);
	switch (ler->ler_event) {
	case LNE_LAYOUT_PHASE1_DONE:
		if (ler->u.ler_status > 0) {
			if (ler->ler_flags & LEF_FROM_OST) {
				if (cfs_list_empty(&ltd->ltd_layout_list))
					cfs_list_add_tail(&ltd->ltd_layout_list,
							  &llmd->llmd_ost_list);
				cfs_list_add_tail(&ltd->ltd_layout_phase_list,
						  &llmd->llmd_ost_phase2_list);
			} else {
				if (cfs_list_empty(&ltd->ltd_layout_list))
					cfs_list_add_tail(&ltd->ltd_layout_list,
							  &llmd->llmd_mdt_list);
				cfs_list_add_tail(&ltd->ltd_layout_phase_list,
						  &llmd->llmd_mdt_phase2_list);
			}
		} else {
			ltd->ltd_layout_done = 1;
			cfs_list_del_init(&ltd->ltd_layout_list);
			lo->ll_flags |= LF_INCOMPLETE;
		}
		break;
	case LNE_LAYOUT_PHASE2_DONE:
		ltd->ltd_layout_done = 1;
		cfs_list_del_init(&ltd->ltd_layout_list);
		break;
	case LNE_LAYOUT_STOP:
		ltd->ltd_layout_done = 1;
		cfs_list_del_init(&ltd->ltd_layout_list);
		if (!(ler->ler_flags & LEF_FORCE_STOP))
			lo->ll_flags |= LF_INCOMPLETE;
		break;
	default:
		break;
	}
	spin_unlock(&ltds->ltd_lock);

	if (ler->ler_flags & LEF_FORCE_STOP)
		lfsck_stop(env, lfsck->li_bottom, &ler->u.ler_stop);
	else if (cfs_list_empty(&llmd->llmd_ost_phase1_list))
		wake_up_all(&llmd->llmd_thread.t_ctl_waitq);

	return 0;
}

static int lfsck_layout_slave_in_notify(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_event_request *ler)
{
	struct lfsck_instance		 *lfsck = com->lc_lfsck;
	struct lfsck_layout_slave_data	 *llsd  = com->lc_data;
	struct lfsck_layout_slave_target *llst;

	if (ler->ler_event == LNE_FID_ACCESSED) {
		lfsck_rbtree_update_bitmap(env, com, &ler->u.ler_fid, true);
		return 0;
	}

	if (ler->ler_event != LNE_LAYOUT_PHASE2_DONE &&
	    ler->ler_event != LNE_LAYOUT_STOP)
		return -EINVAL;

	llst = lfsck_layout_llst_find_and_del(llsd, ler->ler_index);
	if (llst == NULL)
		return -ENODEV;

	lfsck_layout_llst_put(llst);
	if (cfs_list_empty(&llsd->llsd_master_list)) {
		switch (ler->ler_event) {
		case LNE_LAYOUT_PHASE2_DONE:
			wake_up_all(&lfsck->li_thread.t_ctl_waitq);
			break;
		case LNE_LAYOUT_STOP:
			lfsck_stop(env, lfsck->li_bottom, &ler->u.ler_stop);
			break;
		default:
			break;
		}
	}

	return 0;
}

static int lfsck_layout_master_stop_notify(const struct lu_env *env,
					   struct lfsck_component *com,
					   struct lfsck_tgt_descs *ltds,
					   struct lfsck_tgt_desc *ltd,
					   struct ptlrpc_request_set *set)
{
	struct lfsck_thread_info	  *info  = lfsck_env_info(env);
	struct lfsck_async_interpret_args *laia  = &info->lti_laia;
	struct lfsck_event_request	  *ler	 = &info->lti_ler;
	struct lfsck_stop		  *stop  = &ler->u.ler_stop;
	struct lfsck_instance		  *lfsck = com->lc_lfsck;
	int				   rc;

	LASSERT(cfs_list_empty(&ltd->ltd_layout_list));
	LASSERT(cfs_list_empty(&ltd->ltd_layout_phase_list));

	memset(ler, 0, sizeof(*ler));
	ler->ler_index = lfsck_dev_idx(lfsck->li_bottom);
	ler->ler_event = LNE_LAYOUT_STOP;
	if (ltds == &lfsck->li_ost_descs) {
		ler->ler_flags = LEF_TO_OST;
	} else {
		if (ltd->ltd_index == lfsck_dev_idx(lfsck->li_bottom))
			return 0;
		ler->ler_flags = 0;
	}
	stop->ls_status = LS_CO_STOPPED;

	laia->laia_com = com;
	laia->laia_ltds = ltds;
	laia->laia_ltd = ltd;
	laia->laia_ler = ler;

	rc = lfsck_async_set_info(env, ltd->ltd_exp, ler, set,
				  lfsck_layout_master_async_interpret,
				  laia);
	if (rc != 0)
		CERROR("%s: Fail to notify %s %x for co-stop: %d\n",
		       lfsck_lfsck2name(lfsck),
		       (ler->ler_flags & LEF_TO_OST) ? "OST" : "MDT",
		       ltd->ltd_index, rc);
	return rc;
}

/* with lfsck::li_lock held */
static int lfsck_layout_slave_join(const struct lu_env *env,
				   struct lfsck_component *com,
				   struct lfsck_start_param *lsp)
{
	struct lfsck_instance		 *lfsck = com->lc_lfsck;
	struct lfsck_layout_slave_data	 *llsd  = com->lc_data;
	struct lfsck_layout_slave_target *llst;
	struct lfsck_start		 *start = lsp->lsp_start;
	int				  rc    = 0;
	ENTRY;

	if (!lsp->lsp_index_valid || start == NULL ||
	    !(start->ls_flags & LPF_ALL_TARGETS))
		RETURN(-EALREADY);

	spin_unlock(&lfsck->li_lock);
	rc = lfsck_layout_llst_add(llsd, lsp->lsp_index);
	spin_lock(&lfsck->li_lock);
	if (rc == 0 && !thread_is_running(&lfsck->li_thread)) {
		spin_unlock(&lfsck->li_lock);
		llst = lfsck_layout_llst_find_and_del(llsd, lsp->lsp_index);
		if (llst != NULL)
			lfsck_layout_llst_put(llst);
		spin_lock(&lfsck->li_lock);
		rc = -EAGAIN;
	}

	RETURN(rc);
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
	.lfsck_stop_notify	= lfsck_layout_master_stop_notify,
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
	.lfsck_quit		= lfsck_layout_slave_quit,
	.lfsck_query		= lfsck_layout_query,
	.lfsck_in_notify	= lfsck_layout_slave_in_notify,
	.lfsck_join		= lfsck_layout_slave_join,
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

		CFS_INIT_LIST_HEAD(&llmd->llmd_req_list);
		spin_lock_init(&llmd->llmd_lock);
		CFS_INIT_LIST_HEAD(&llmd->llmd_ost_list);
		CFS_INIT_LIST_HEAD(&llmd->llmd_ost_phase1_list);
		CFS_INIT_LIST_HEAD(&llmd->llmd_ost_phase2_list);
		CFS_INIT_LIST_HEAD(&llmd->llmd_mdt_list);
		CFS_INIT_LIST_HEAD(&llmd->llmd_mdt_phase1_list);
		CFS_INIT_LIST_HEAD(&llmd->llmd_mdt_phase2_list);
		init_waitqueue_head(&llmd->llmd_thread.t_ctl_waitq);
		atomic_set(&llmd->llmd_rpc_in_flight, 0);
		com->lc_data = llmd;
	} else {
		struct lfsck_layout_slave_data *llsd;

		com->lc_ops = &lfsck_layout_slave_ops;
		OBD_ALLOC(llsd, sizeof(*llsd));
		if (llsd == NULL)
			GOTO(out, rc = -ENOMEM);

		CFS_INIT_LIST_HEAD(&llsd->llsd_seq_list);
		CFS_INIT_LIST_HEAD(&llsd->llsd_master_list);
		spin_lock_init(&llsd->llsd_lock);
		llsd->llsd_rb_root = RB_ROOT;
		rwlock_init(&llsd->llsd_rb_lock);
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

	if (unlikely(!dt_try_as_dir(env, root)))
		GOTO(out, rc = -ENOTDIR);

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
		spin_lock(&lfsck->li_lock);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		spin_unlock(&lfsck->li_lock);
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
	case LS_CO_FAILED:
	case LS_CO_STOPPED:
	case LS_CO_PAUSED:
		spin_lock(&lfsck->li_lock);
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_scan);
		spin_unlock(&lfsck->li_lock);
		break;
	}

	if (lo->ll_flags & LF_CRASHED_LASTID) {
		LASSERT(lfsck->li_out_notify != NULL);

		lfsck->li_out_notify(env, lfsck->li_out_notify_data,
				     LNE_LASTID_REBUILDING);
	}

	GOTO(out, rc = 0);

out:
	if (root != NULL && !IS_ERR(root))
		lu_object_put(env, &root->do_lu);
	if (rc != 0)
		lfsck_component_cleanup(env, com);
	else
		out_register_record_fid_accessed(lfsck_record_fid_accessed);
	return rc;
}

struct lfsck_orphan_it {
	struct lfsck_component		*loi_com;
	struct lfsck_rbtree_node	*loi_lrn;
	struct lu_fid			 loi_key;
	struct lu_orphan_rec		 loi_rec;
	int				 loi_idx;
	unsigned int			 loi_over:1;
};

static int lfsck_fid_match_idx(const struct lu_env *env,
			       struct lfsck_instance *lfsck,
			       const struct lu_fid *fid, int idx)
{
	struct seq_server_site	*ss;
	struct lu_server_fld	*sf;
	struct lu_seq_range	 range	= { 0 };
	int			 rc;

	/* All abnormal cases will be returned to MDT0. */
	if (!fid_is_norm(fid)) {
		if (idx == 0)
			return 1;
		else
			return 0;
	}

	ss = lu_site2seq(lfsck->li_bottom->dd_lu_dev.ld_site);
	if (unlikely(ss == NULL))
		return -ENOTCONN;

	sf = ss->ss_server_fld;
	LASSERT(sf != NULL);

	fld_range_set_any(&range);
	rc = fld_server_lookup(env, sf, fid_seq(fid), &range);
	if (rc != 0)
		return rc;

	if (!fld_range_is_mdt(&range))
		return -EINVAL;

	if (range.lsr_index == idx)
		return 1;
	else
		return 0;
}

static int lfsck_orphan_index_lookup(const struct lu_env *env,
				     struct dt_object *dt,
				     struct dt_rec *rec,
				     const struct dt_key *key,
				     struct lustre_capa *capa)
{
	return -EOPNOTSUPP;
}

static int lfsck_orphan_index_declare_insert(const struct lu_env *env,
					     struct dt_object *dt,
					     const struct dt_rec *rec,
					     const struct dt_key *key,
					     struct thandle *handle)
{
	return -EOPNOTSUPP;
}

static int lfsck_orphan_index_insert(const struct lu_env *env,
				     struct dt_object *dt,
				     const struct dt_rec *rec,
				     const struct dt_key *key,
				     struct thandle *handle,
				     struct lustre_capa *capa,
				     int ignore_quota)
{
	return -EOPNOTSUPP;
}

static int lfsck_orphan_index_declare_delete(const struct lu_env *env,
					     struct dt_object *dt,
					     const struct dt_key *key,
					     struct thandle *handle)
{
	return -EOPNOTSUPP;
}

static int lfsck_orphan_index_delete(const struct lu_env *env,
				     struct dt_object *dt,
				     const struct dt_key *key,
				     struct thandle *handle,
				     struct lustre_capa *capa)
{
	return -EOPNOTSUPP;
}

static struct dt_it *lfsck_orphan_it_init(const struct lu_env *env,
					  struct dt_object *dt,
					  __u32 attr,
					  struct lustre_capa *capa)
{
	struct dt_device		*dev	= lu2dt_dev(dt->do_lu.lo_dev);
	struct lfsck_instance		*lfsck;
	struct lfsck_component		*com	= NULL;
	struct lfsck_layout_slave_data	*llsd;
	struct lfsck_orphan_it		*it	= NULL;
	int				 rc	= 0;
	ENTRY;

	lfsck = lfsck_instance_find(dev, true, false);
	if (unlikely(lfsck == NULL))
		RETURN(ERR_PTR(-ENODEV));

	com = lfsck_component_find(lfsck, LT_LAYOUT);
	if (unlikely(com == NULL))
		GOTO(out, rc = -ENOENT);

	OBD_ALLOC_PTR(it);
	if (it == NULL)
		GOTO(out, rc = -ENOMEM);

	llsd = com->lc_data;
	if (dev->dd_record_fid_accessed) {
		/* The first iteratino against the rbtree, scan the whole rbtree
		 * to remove the nodes which do NOT need to be handled. */
		write_lock(&llsd->llsd_rb_lock);
		if (dev->dd_record_fid_accessed) {
			struct rb_node			*node;
			struct rb_node			*next;
			struct lfsck_rbtree_node	*lrn;

			/* No need to record the fid accessing anymore. */
			dev->dd_record_fid_accessed = 0;

			node = rb_first(&llsd->llsd_rb_root);
			while (node != NULL) {
				next = rb_next(node);
				lrn = rb_entry(node, struct lfsck_rbtree_node,
					       lrn_node);
				if (atomic_read(&lrn->lrn_known_count) <=
				    atomic_read(&lrn->lrn_accessed_count)) {
					rb_erase(node, &llsd->llsd_rb_root);
					lfsck_rbtree_free(lrn);
				}
				node = next;
			}
		}
		write_unlock(&llsd->llsd_rb_lock);
	}

	/* read lock the rbtree when init, and unlock when fini */
	read_lock(&llsd->llsd_rb_lock);
	it->loi_com = com;
	com = NULL;
	it->loi_idx = -1;

	GOTO(out, rc = 0);

out:
	if (com != NULL)
		lfsck_component_put(env, com);
	lfsck_instance_put(env, lfsck);
	if (rc != 0)
		it = (struct lfsck_orphan_it *)ERR_PTR(rc);
	return (struct dt_it *)it;
}

static void lfsck_orphan_it_fini(const struct lu_env *env,
				 struct dt_it *di)
{
	struct lfsck_orphan_it		*it	= (struct lfsck_orphan_it *)di;
	struct lfsck_component		*com	= it->loi_com;
	struct lfsck_layout_slave_data	*llsd;

	if (com != NULL) {
		llsd = com->lc_data;
		read_unlock(&llsd->llsd_rb_lock);
		lfsck_component_put(env, com);
	}
	OBD_FREE_PTR(it);
}

/**
 * \retval	 +1: the iteration finished
 * \retval	  0: on success, not finished
 * \retval	-ve: on error
 */
static int lfsck_orphan_it_next(const struct lu_env *env,
				struct dt_it *di)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct filter_fid_old		*pfid	= &info->lti_old_pfid;
	struct lu_attr			*la	= &info->lti_la;
	struct lfsck_orphan_it		*it	= (struct lfsck_orphan_it *)di;
	struct lu_fid			*key	= &it->loi_key;
	struct lu_orphan_rec		*rec	= &it->loi_rec;
	struct lfsck_component		*com	= it->loi_com;
	struct lfsck_layout_slave_data	*llsd	= com->lc_data;
	struct dt_object		*obj;
	struct lfsck_rbtree_node	*lrn;
	int				 pos;
	int				 rc;
	__u32				 save;
	bool				 exact	= false;
	ENTRY;

	if (it->loi_over)
		RETURN(1);

again0:
	lrn = it->loi_lrn;
	if (lrn == NULL) {
		lrn = lfsck_rbtree_search(llsd, key, &exact);
		if (lrn == NULL) {
			it->loi_over = 1;
			RETURN(1);
		}

		it->loi_lrn = lrn;
		if (!exact) {
			key->f_seq = lrn->lrn_seq;
			key->f_oid = lrn->lrn_first_oid;
			key->f_ver = 0;
		}
	} else {
		key->f_oid++;
		if (unlikely(key->f_oid == 0)) {
			key->f_seq++;
			it->loi_lrn = NULL;
			goto again0;
		}

		if (key->f_oid >=
		    lrn->lrn_first_oid + LFSCK_RBTREE_BITMAP_WIDTH) {
			it->loi_lrn = NULL;
			goto again0;
		}
	}

	if (unlikely(atomic_read(&lrn->lrn_known_count) <=
		     atomic_read(&lrn->lrn_accessed_count))) {
		struct rb_node *next = rb_next(&lrn->lrn_node);

		while (next != NULL) {
			lrn = rb_entry(next, struct lfsck_rbtree_node,
				       lrn_node);
			if (atomic_read(&lrn->lrn_known_count) >
			    atomic_read(&lrn->lrn_accessed_count))
				break;
			next = rb_next(next);
		}

		if (next == NULL) {
			it->loi_over = 1;
			RETURN(1);
		}

		it->loi_lrn = lrn;
		key->f_seq = lrn->lrn_seq;
		key->f_oid = lrn->lrn_first_oid;
		key->f_ver = 0;
	}

	pos = key->f_oid - lrn->lrn_first_oid;

again1:
	pos = find_next_bit(lrn->lrn_known_bitmap,
			    LFSCK_RBTREE_BITMAP_WIDTH, pos);
	if (pos >= LFSCK_RBTREE_BITMAP_WIDTH) {
		key->f_oid = lrn->lrn_first_oid + pos;
		if (unlikely(key->f_oid < lrn->lrn_first_oid)) {
			key->f_seq++;
			key->f_oid = 0;
		}
		it->loi_lrn = NULL;
		goto again0;
	}

	if (test_bit(pos, lrn->lrn_accessed_bitmap)) {
		pos++;
		goto again1;
	}

	key->f_oid = lrn->lrn_first_oid + pos;
	obj = lfsck_object_find(env, com->lc_lfsck, key);
	if (IS_ERR(obj)) {
		rc = PTR_ERR(obj);
		if (rc == -ENOENT) {
			pos++;
			goto again1;
		}
		RETURN(rc);
	}

	dt_read_lock(env, obj, 0);
	if (!dt_object_exists(obj)) {
		dt_read_unlock(env, obj);
		lfsck_object_put(env, obj);
		pos++;
		goto again1;
	}

	rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_xattr_get(env, obj, lfsck_buf_get(env, pfid, sizeof(*pfid)),
			  XATTR_NAME_FID, BYPASS_CAPA);
	if (rc == -ENODATA) {
		/* If the orphan OST-object has no parent information,
		 * regard it as referenced by the MDT-object on MDT0. */
		if (it->loi_idx == 0 && la->la_ctime != 0) {
			fid_zero(&rec->lor_fid);
			rec->lor_uid = la->la_uid;
			rec->lor_gid = la->la_gid;
			GOTO(out, rc = 0);
		}

		/* For the pre-created OST-object, update the bitmap to avoid
		 * others LFSCK (second phase )iteration to touch it again. */
		if (la->la_ctime == 0) {
			if (!test_and_set_bit(pos, lrn->lrn_accessed_bitmap))
				atomic_inc(&lrn->lrn_accessed_count);
		}

		dt_read_unlock(env, obj);
		lfsck_object_put(env, obj);
		pos++;
		goto again1;
	}

	if (rc < 0)
		GOTO(out, rc);

	if (rc != sizeof(struct filter_fid) &&
	    rc != sizeof(struct filter_fid_old))
		GOTO(out, rc = -EINVAL);

	fid_le_to_cpu(&rec->lor_fid, &pfid->ff_parent);
	/* XXX: In fact, the ff_parent::f_ver is not the real parent
	 *	FID::f_ver, instead, it is the OST-object index in
	 *	its parent MDT-object layout EA. */
	save = rec->lor_fid.f_ver;
	rec->lor_fid.f_ver = 0;
	rc = lfsck_fid_match_idx(env, com->lc_lfsck, &rec->lor_fid,
				 it->loi_idx);
	/* If the orphan OST-object does not claim the MDT, then next.
	 *
	 * If we do not know whether it matches or not, then return it
	 * to the MDT for further check. */
	if (rc == 0) {
		dt_read_unlock(env, obj);
		lfsck_object_put(env, obj);
		pos++;
		goto again1;
	}

	rec->lor_fid.f_ver = save;
	rec->lor_uid = la->la_uid;
	rec->lor_gid = la->la_gid;

	GOTO(out, rc = 0);

out:
	dt_read_unlock(env, obj);
	lfsck_object_put(env, obj);
	return rc;
}

/**
 * \retval	 +1: locate to the exactly position
 * \retval	  0: cannot locate to the exactly position,
 *		     call next() to move to a valid position.
 * \retval	-ve: on error
 */
static int lfsck_orphan_it_get(const struct lu_env *env,
			       struct dt_it *di,
			       const struct dt_key *key)
{
	struct lfsck_orphan_it		*it   = (struct lfsck_orphan_it *)di;
	struct idx_info 		*ii   = (struct idx_info *)key;
	struct lfsck_layout_slave_data	*llsd = it->loi_com->lc_data;
	int				 rc;

	/* Forbid to set iteration position after iteration started. */
	if (it->loi_idx != -1)
		return -EPERM;

	LASSERT(ii->ii_flags & IT_FL_BIGKEY);
	LASSERT(ii->ii_index != -1);

	if (!llsd->llsd_rbtree_valid)
		return -ESRCH;

	it->loi_key.f_seq = ii->ii_seq_start;
	it->loi_key.f_oid = ii->ii_oid_start;
	it->loi_key.f_ver = ii->ii_ver_start;
	it->loi_idx = ii->ii_index;
	rc = lfsck_orphan_it_next(env, di);
	if (rc == 1)
		return 0;
	if (rc == 0)
		return 1;
	return rc;
}

static void lfsck_orphan_it_put(const struct lu_env *env,
				struct dt_it *di)
{
}

static struct dt_key *lfsck_orphan_it_key(const struct lu_env *env,
					  const struct dt_it *di)
{
	struct lfsck_orphan_it *it = (struct lfsck_orphan_it *)di;

	return (struct dt_key *)&it->loi_key;
}

static int lfsck_orphan_it_key_size(const struct lu_env *env,
				    const struct dt_it *di)
{
	return sizeof(struct lu_fid);
}

static int lfsck_orphan_it_rec(const struct lu_env *env,
			       const struct dt_it *di,
			       struct dt_rec *rec,
			       __u32 attr)
{
	struct lfsck_orphan_it *it = (struct lfsck_orphan_it *)di;

	*(struct lu_orphan_rec *)rec = it->loi_rec;
	return 0;
}

static __u64 lfsck_orphan_it_store(const struct lu_env *env,
				   const struct dt_it *di)
{
	return -E2BIG;
}

static int lfsck_orphan_it_load(const struct lu_env *env,
				const struct dt_it *di,
				__u64 hash)
{
	return -E2BIG;
}

static int lfsck_orphan_it_key_rec(const struct lu_env *env,
				   const struct dt_it *di,
				   void *key_rec)
{
	return 0;
}

const struct dt_index_operations lfsck_orphan_index_ops = {
	.dio_lookup		= lfsck_orphan_index_lookup,
	.dio_declare_insert	= lfsck_orphan_index_declare_insert,
	.dio_insert		= lfsck_orphan_index_insert,
	.dio_declare_delete	= lfsck_orphan_index_declare_delete,
	.dio_delete		= lfsck_orphan_index_delete,
	.dio_it = {
		.init		= lfsck_orphan_it_init,
		.fini		= lfsck_orphan_it_fini,
		.get		= lfsck_orphan_it_get,
		.put		= lfsck_orphan_it_put,
		.next		= lfsck_orphan_it_next,
		.key		= lfsck_orphan_it_key,
		.key_size	= lfsck_orphan_it_key_size,
		.rec		= lfsck_orphan_it_rec,
		.store		= lfsck_orphan_it_store,
		.load		= lfsck_orphan_it_load,
		.key_rec	= lfsck_orphan_it_key_rec,
	}
};
