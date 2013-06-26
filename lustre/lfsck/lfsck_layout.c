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

#include "lfsck_internal.h"

#define LFSCK_LAYOUT_MAGIC		0xB173AE14

#if BITS_PER_LONG == 32
# define LFSCK_LAYOUT_BITMAP_SHIFT	5
#else
# define LFSCK_LAYOUT_BITMAP_SHIFT	6
#endif
#define LFSCK_LAYOUT_BITMAP_ALIGN	(1 << LFSCK_LAYOUT_BITMAP_SHIFT)
#define LFSCK_LAYOUT_BITMAP_MASK	(LFSCK_LAYOUT_BITMAP_ALIGN - 1)

static const char lfsck_layout_name[] = "lfsck_layout";

struct lfsck_layout_data {
	unsigned int	 lld_dirty:1;
	void		*lld_bitmap;
};

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
	des->ll_objs_repaired_multipe_referenced =
			le64_to_cpu(src->ll_objs_repaired_multipe_referenced);
	des->ll_objs_repaired_orphan =
			le64_to_cpu(src->ll_objs_repaired_orphan);
	des->ll_objs_repaired_inconsistent_owner =
			le64_to_cpu(src->ll_objs_repaired_inconsistent_owner);
	des->ll_objs_repaired_others =
			le64_to_cpu(src->ll_objs_repaired_others);
	des->ll_max_index = le32_to_cpu(src->ll_max_index);
	des->ll_bitmap_size = le32_to_cpu(src->ll_bitmap_size);
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
	des->ll_objs_repaired_multipe_referenced =
			cpu_to_le64(src->ll_objs_repaired_multipe_referenced);
	des->ll_objs_repaired_orphan =
			cpu_to_le64(src->ll_objs_repaired_orphan);
	des->ll_objs_repaired_inconsistent_owner =
			cpu_to_le64(src->ll_objs_repaired_inconsistent_owner);
	des->ll_objs_repaired_others =
			cpu_to_le64(src->ll_objs_repaired_others);
	des->ll_max_index = cpu_to_le32(src->ll_max_index);
	des->ll_bitmap_size = cpu_to_le32(src->ll_bitmap_size);
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
	struct lfsck_layout_data	*lld	= com->lc_data;
	ssize_t				 size	= com->lc_file_size;
	ssize_t				 read	= 0;
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

	if (lld == NULL || lo->ll_bitmap_size == 0)
		return 0;

	OBD_ALLOC_LARGE(lld->lld_bitmap, lo->ll_bitmap_size);
	if (unlikely(lld->lld_bitmap == NULL))
		return -ENOMEM;

	size = lo->ll_bitmap_size;
	/* load bitmap */
	do {
		if (size > OBD_ALLOC_BIG)
			size = OBD_ALLOC_BIG;
		rc = dbo->dbo_read(env, com->lc_obj,
			lfsck_buf_get(env, lld->lld_bitmap + read, size),
			&pos, BYPASS_CAPA);
		if (rc == size) {
			read += size;
			size = lo->ll_bitmap_size - read;
		} else if (rc >= 0) {
			CWARN("%s: crashed lfsck_layout: size = %u, "
			      "read = %u, to be reset\n",
			      lfsck_lfsck2name(com->lc_lfsck),
			      lo->ll_bitmap_size, (int)read);
			return 1;
		} else {
			CWARN("%s: failed to load lfsck_layout: size = %u, "
			      "read = %u, rc = %d\n",
			      lfsck_lfsck2name(com->lc_lfsck),
			      lo->ll_bitmap_size, (int)read, rc);
			return rc;
		}
	} while (size > 0);

	return 0;
}

static int lfsck_layout_store(const struct lu_env *env,
			      struct lfsck_component *com)
{
	struct dt_object	 *obj		= com->lc_obj;
	struct lfsck_instance	 *lfsck		= com->lc_lfsck;
	struct lfsck_layout	 *lo		= com->lc_file_ram;
	struct lfsck_layout_data *lld		= com->lc_data;
	struct thandle		 *handle;
	ssize_t			  size		= com->lc_file_size;
	ssize_t			  written	= 0;
	loff_t			  pos		= 0;
	int			  rc;
	ENTRY;

	lfsck_layout_cpu_to_le(lo, (struct lfsck_layout *)com->lc_file_ram);
	handle = dt_trans_create(env, lfsck->li_bottom);
	if (IS_ERR(handle)) {
		rc = PTR_ERR(handle);
		CERROR("%s: fail to create trans for storing "
		       "lfsck_layout: %d\n,", lfsck_lfsck2name(lfsck), rc);
		RETURN(rc);
	}

	rc = dt_declare_record_write(env, obj, sizeof(struct lfsck_layout),
				     pos, handle);
	if (rc != 0) {
		CERROR("%s: fail to declare trans for storing "
		       "lfsck_layout(1): %d\n", lfsck_lfsck2name(lfsck), rc);
		GOTO(out, rc);
	}

	if (lld != NULL && lld->lld_dirty) {
		rc = dt_declare_record_write(env, obj, lo->ll_bitmap_size,
					     sizeof(struct lfsck_layout),
					     handle);
		if (rc != 0) {
			CERROR("%s: fail to declare trans for storing "
			       "lfsck_layout(2): %d\n",
			       lfsck_lfsck2name(lfsck), rc);
			GOTO(out, rc);
		}
	}

	rc = dt_trans_start_local(env, lfsck->li_bottom, handle);
	if (rc != 0) {
		CERROR("%s: fail to start trans for storing "
		       "lfsck_layout: %d\n,", lfsck_lfsck2name(lfsck), rc);
		GOTO(out, rc);
	}

	rc = dt_record_write(env, obj, lfsck_buf_get(env, lo, size), &pos,
			     handle);
	if (rc != 0)
		CERROR("%s: fail to store lfsck_layout(1), size = %d, "
		       "rc = %d\n", lfsck_lfsck2name(lfsck), (int)size, rc);

	if (lld == NULL || !lld->lld_dirty)
		GOTO(out, rc = 0);

	size = lo->ll_bitmap_size;
	/* write bitmap */
	do {
		if (size > OBD_ALLOC_BIG)
			size = OBD_ALLOC_BIG;
		rc = dt_record_write(env, obj,
			lfsck_buf_get(env, lld->lld_bitmap + written, size),
			&pos, handle);
		if (rc != 0) {
			CERROR("%s: fail to declare trans for storing "
			       "lfsck_layout(2): %d\n",
			       lfsck_lfsck2name(lfsck), rc);
			GOTO(out, rc);
		}
		written += size;
		size = lo->ll_bitmap_size - written;
	} while (size > 0);
	lld->lld_dirty = 0;

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

/* layout APIs */
/* XXX: Some to be implemented in other patch(es). */

static int lfsck_layout_reset(const struct lu_env *env,
			      struct lfsck_component *com, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_layout	*lo    = com->lc_file_ram;
	struct dt_object	*dto;
	int			 rc;
	ENTRY;

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

	rc = local_object_unlink(env, lfsck->li_bottom, lfsck->li_local_root,
				 lfsck_layout_name);
	if (rc != 0)
		GOTO(out, rc);

	dto = local_file_find_or_create(env, lfsck->li_los, lfsck->li_local_root,
					lfsck_layout_name,
					S_IFREG | S_IRUGO | S_IWUSR);
	if (IS_ERR(dto))
		GOTO(out, rc = PTR_ERR(dto));

	com->lc_obj = dto;
	rc = lfsck_layout_store(env, com);

	GOTO(out, rc);

out:
	up_write(&com->lc_sem);
	return rc;
}

static void lfsck_layout_fail(const struct lu_env *env,
			      struct lfsck_component *com, bool new_checked)
{
}

static int lfsck_layout_checkpoint(const struct lu_env *env,
				   struct lfsck_component *com, bool init)
{
	return 0;
}

static int lfsck_layout_master_prep(const struct lu_env *env,
				    struct lfsck_component *com)
{
	return 0;
}

static int lfsck_layout_slave_prep(const struct lu_env *env,
				   struct lfsck_component *com)
{
	return 0;
}

static int lfsck_layout_master_exec_oit(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *obj)
{
	return 0;
}

static int lfsck_layout_slave_exec_oit(const struct lu_env *env,
				       struct lfsck_component *com,
				       struct dt_object *obj)
{
	return 0;
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
	return 0;
}

static int lfsck_layout_slave_post(const struct lu_env *env,
				   struct lfsck_component *com,
				   int result, bool init)
{
	return 0;
}

static int
lfsck_layout_dump(const struct lu_env *env, struct lfsck_component *com,
		     char *buf, int len)
{
	return 0;
}

static int lfsck_layout_master_double_scan(const struct lu_env *env,
					   struct lfsck_component *com)
{
	return 0;
}

static int lfsck_layout_slave_double_scan(const struct lu_env *env,
					  struct lfsck_component *com)
{
	return 0;
}

static void lfsck_layout_data_release(const struct lu_env *env,
				      struct lfsck_component *com)
{
	struct lfsck_layout		*lo	= com->lc_file_ram;
	struct lfsck_layout_data	*lld	= com->lc_data;

	LASSERT(lld != NULL);

	com->lc_data = NULL;
	if (lld->lld_bitmap != NULL)
		OBD_FREE_LARGE(lld->lld_bitmap, lo->ll_bitmap_size);
	OBD_FREE(lld, sizeof(*lld));
}

static struct lfsck_operations lfsck_layout_master_ops = {
	.lfsck_reset		= lfsck_layout_reset,
	.lfsck_fail		= lfsck_layout_fail,
	.lfsck_checkpoint	= lfsck_layout_checkpoint,
	.lfsck_prep		= lfsck_layout_master_prep,
	.lfsck_exec_oit		= lfsck_layout_master_exec_oit,
	.lfsck_exec_dir		= lfsck_layout_exec_dir,
	.lfsck_post		= lfsck_layout_master_post,
	.lfsck_dump		= lfsck_layout_dump,
	.lfsck_double_scan	= lfsck_layout_master_double_scan,
};

static struct lfsck_operations lfsck_layout_slave_ops = {
	.lfsck_reset		= lfsck_layout_reset,
	.lfsck_fail		= lfsck_layout_fail,
	.lfsck_checkpoint	= lfsck_layout_checkpoint,
	.lfsck_prep		= lfsck_layout_slave_prep,
	.lfsck_exec_oit		= lfsck_layout_slave_exec_oit,
	.lfsck_exec_dir		= lfsck_layout_exec_dir,
	.lfsck_post		= lfsck_layout_slave_post,
	.lfsck_dump		= lfsck_layout_dump,
	.lfsck_double_scan	= lfsck_layout_slave_double_scan,
	.lfsck_data_release	= lfsck_layout_data_release,
};

int lfsck_layout_setup(const struct lu_env *env, struct lfsck_instance *lfsck)
{
	struct lfsck_component	*com;
	struct lfsck_layout	*lo;
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
		com->lc_ops = &lfsck_layout_master_ops;
	} else {
		com->lc_ops = &lfsck_layout_slave_ops;
		OBD_ALLOC(com->lc_data, sizeof(struct lfsck_layout_data));
		if (com->lc_data == NULL)
			GOTO(out, rc = -ENOMEM);
	}
	com->lc_file_size = sizeof(struct lfsck_layout);
	OBD_ALLOC(com->lc_file_ram, com->lc_file_size);
	if (com->lc_file_ram == NULL)
		GOTO(out, rc = -ENOMEM);

	OBD_ALLOC(com->lc_file_disk, com->lc_file_size);
	if (com->lc_file_disk == NULL)
		GOTO(out, rc = -ENOMEM);

	obj = local_file_find_or_create(env, lfsck->li_los,
					 lfsck->li_local_root,
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
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		break;
	default:
		CERROR("%s: unknown lfsck_layout status: %u\n",
		       lfsck_lfsck2name(lfsck), lo->ll_status);
		/* fall through */
	case LS_SCANNING_PHASE1:
	case LS_SCANNING_PHASE2:
		/* No need to store the status to disk right now.
		 * If the system crashed before the status stored,
		 * it will be loaded back when next time. */
		lo->ll_status = LS_CRASHED;
		/* fall through */
	case LS_PAUSED:
	case LS_CRASHED:
		cfs_list_add_tail(&com->lc_link, &lfsck->li_list_scan);
		break;
	}

	GOTO(out, rc = 0);

out:
	if (rc != 0)
		lfsck_component_cleanup(env, com);
	return rc;
}
