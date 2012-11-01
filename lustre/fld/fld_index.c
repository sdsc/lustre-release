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
 * lustre/fld/fld_index.c
 *
 * Author: WangDi <wangdi@clusterfs.com>
 * Author: Yury Umanets <umka@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_FLD

#ifdef __KERNEL__
# include <libcfs/libcfs.h>
# include <linux/module.h>
# include <linux/jbd.h>
#else /* __KERNEL__ */
# include <liblustre.h>
#endif

#include <obd.h>
#include <obd_class.h>
#include <lustre_ver.h>
#include <obd_support.h>
#include <lprocfs_status.h>

#include <dt_object.h>
#include <md_object.h>
#include <lustre_mdc.h>
#include <lustre_fid.h>
#include <lustre_fld.h>
#include "fld_internal.h"

const char fld_index_name[] = "fld";

static const struct lu_seq_range IGIF_FLD_RANGE = {
	.lsr_start = FID_SEQ_IGIF,
	.lsr_end   = FID_SEQ_IGIF_MAX + 1,
	.lsr_index = 0,
	.lsr_flags = LU_SEQ_RANGE_MDT
};

const struct dt_index_features fld_index_features = {
	.dif_flags       = DT_IND_UPDATE | DT_IND_RANGE,
        .dif_keysize_min = sizeof(seqno_t),
        .dif_keysize_max = sizeof(seqno_t),
        .dif_recsize_min = sizeof(struct lu_seq_range),
        .dif_recsize_max = sizeof(struct lu_seq_range),
        .dif_ptrsize     = 4
};

extern struct lu_context_key fld_thread_key;

static int fld_write_range(const struct lu_env *env, struct dt_object *dt,
			   const struct lu_seq_range *range, loff_t pos,
			   struct thandle *th)
{
	struct fld_thread_info	*info;
	struct lu_seq_range	*range_written = NULL;
	struct lu_buf		buf;
	int			rc;

	info = lu_context_key_get(&env->le_ctx, &fld_thread_key);
	LASSERT(info != NULL);

	range_written = &info->fti_lrange;
	LASSERT(range != NULL);
	range_cpu_to_be(range_written, range);
	buf.lb_buf = range_written;
	buf.lb_len = sizeof(struct lu_seq_range);
	rc = dt_record_write(env, dt, &buf, &pos, th);
	if (rc != 0)
		CERROR("Write seq range "DRANGE" error: rc = %d\n",
			PRANGE(range), rc);

	return rc;
}

/**
 * insert range in fld store.
 *
 *      \param  range  range to be inserted
 *      \param  th     transaction for this operation as it could compound
 *                     transaction.
 *
 *      \retval  0  success
 *      \retval  -ve error
 */
int fld_index_create(struct lu_server_fld *fld,
                     const struct lu_env *env,
                     const struct lu_seq_range *range,
                     struct thandle *th)
{
	int			rc;
	loff_t			pos;
	struct lu_attr		*attr;

	ENTRY;

	if (fld->lsf_no_range_lookup) {
		/* Stub for underlying FS which can't lookup ranges */
		if (range->lsr_index != 0) {
			CERROR("%s: FLD backend does not support range"
			       "lookups, so DNE and FIDs-on-OST are not"
			       "supported in this configuration\n",
			       fld->lsf_name);
			return -EINVAL;
		}
	}

	LASSERT(range_is_sane(range));

	LASSERT_MUTEX_LOCKED(&fld->lsf_lock);

	OBD_ALLOC_PTR(attr);
	if (attr == NULL)
		return -ENOMEM;
	rc = dt_attr_get(env, fld->lsf_obj, attr, BYPASS_CAPA);
	if (rc != 0) {
		CERROR("%s: can not get attr"DRANGE": rc = %d\n",
			fld->lsf_name, PRANGE(range), rc);
		GOTO(free, rc);
	}

	pos = attr->la_size;
	rc = fld_write_range(env, fld->lsf_obj, range, pos, th);
	if (rc) {
		CERROR("%s: can not insert entry "DRANGE": rc = %d\n",
		       fld->lsf_name, PRANGE(range), rc);
		GOTO(free, rc);
	}

	/* Always add the entry to the end of the file */
	rc = fld_cache_insert(fld->lsf_cache, range, pos);

	CDEBUG(D_INFO, "%s: insert given range : "DRANGE" rc = %d pos"LPU64"\n",
		fld->lsf_name, PRANGE(range), rc, pos);
free:
	OBD_FREE_PTR(attr);
	RETURN(rc);
}

/**
 * delete range in fld store.
 *
 *      \param  range range to be deleted
 *      \param  th     transaction
 *
 *      \retval  0  success
 *      \retval  -ve error
 */
int fld_index_delete(struct lu_server_fld *fld,
                     const struct lu_env *env,
                     struct lu_seq_range *range,
                     struct thandle   *th)
{
	struct fld_cache_entry	*flde;
	struct lu_seq_range     *fld_rec;
	struct fld_thread_info  *info;
	int			rc;

	ENTRY;

	LASSERT_MUTEX_LOCKED(&fld->lsf_lock);
	flde = fld_cache_entry_lookup(fld->lsf_cache, range);
	if (flde == NULL)
		RETURN(-ENOENT);

	info = lu_context_key_get(&env->le_ctx, &fld_thread_key);
	fld_rec = &info->fti_rec;
	LASSERTF(flde->fce_off != 0, "No offset for "DRANGE"\n",
		 PRANGE(&flde->fce_range));

	memset(fld_rec, 0, sizeof(*fld_rec));
	fld_rec->lsr_flags = LU_SEQ_RANGE_EMPTY;
	rc = fld_write_range(env, fld->lsf_obj, fld_rec, flde->fce_off, th);
	if (rc != 0)
		RETURN(rc);

	fld_cache_entry_delete(fld->lsf_cache, flde);
	RETURN(rc);
}

/**
 * lookup range for a seq passed. note here we only care about the start/end,
 * caller should handle the attached location data (flags, index).
 *
 * \param  seq     seq for lookup.
 * \param  range   result of lookup.
 *
 * \retval  0           found, \a range is the matched range;
 * \retval -ENOENT      not found, \a range is the left-side range;
 * \retval  -ve         other error;
 */
int fld_index_lookup(struct lu_server_fld *fld,
                     const struct lu_env *env,
                     seqno_t seq,
                     struct lu_seq_range *range)
{
        struct lu_seq_range     *fld_rec;
        struct fld_thread_info  *info;
        int rc;

        ENTRY;

	if (fld->lsf_no_range_lookup) {
		/* Stub for underlying FS which can't lookup ranges */
		range->lsr_start = 0;
		range->lsr_end = ~0;
		range->lsr_index = 0;
		range->lsr_flags = LU_SEQ_RANGE_MDT;

		range_cpu_to_be(range, range);
		return 0;
	}

        info = lu_context_key_get(&env->le_ctx, &fld_thread_key);
        fld_rec = &info->fti_rec;

	rc = fld_cache_lookup(fld->lsf_cache, seq, fld_rec);
	if (rc == 0) {
                *range = *fld_rec;
                if (range_within(range, seq))
                        rc = 0;
                else
                        rc = -ENOENT;
        }

        CDEBUG(D_INFO, "%s: lookup seq = "LPX64" range : "DRANGE" rc = %d\n",
               fld->lsf_name, seq, PRANGE(range), rc);

        RETURN(rc);
}

static int fld_load_old_index(const struct lu_env *env,
			      struct dt_device *dt,
			      struct dt_object *dt_obj,
			      struct dt_object *old_obj,
			      loff_t pos,
			      struct thandle *th,
			      struct fld_cache *cache)
{
	struct fld_cache_entry	*flde;
	int			rc = 0;
	ENTRY;


	cfs_spin_lock(&cache->fci_lock);
	cfs_list_for_each_entry(flde, &cache->fci_entries_head, fce_list) {
		rc = fld_write_range(env, dt_obj, &flde->fce_range,
				     flde->fce_off, th);
		if (rc != 0)
			break;
	}
	cfs_spin_unlock(&cache->fci_lock);
	RETURN(rc);
}

static int fld_declare_load_old_index(const struct lu_env *env,
				      struct fld_cache *cache,
				      struct dt_device *dt,
				      struct dt_object *dt_obj,
				      struct dt_object **old_objp,
				      loff_t pos,
				      struct thandle *th)
{
	struct lu_fid		fid;
	struct dt_object	*old_obj;
	const struct dt_it_ops	*iops;
	struct dt_it		*it;
	int			rc;
	struct lu_seq_range	*range;
	struct fld_thread_info	*info;
	ENTRY;

	lu_local_obj_fid(&fid, FLD_INDEX_OID);
	old_obj = dt_locate(env, dt, &fid);
	if (IS_ERR(old_obj))
		RETURN(0);

	LASSERT(old_obj != NULL);
	if (!dt_object_exists(old_obj)) {
		lu_object_put(env, &old_obj->do_lu);
		RETURN(0);
	}

	rc = dt_obj->do_ops->do_index_try(env, old_obj,
					  &fld_index_features);
	if (rc != 0) {
		lu_object_put(env, &old_obj->do_lu);
		RETURN(rc);
	}

	info = lu_context_key_get(&env->le_ctx, &fld_thread_key);
	LASSERT(info != NULL);

	range = &info->fti_rec;
	*old_objp = old_obj;
	LASSERT(old_obj->do_index_ops != NULL);
	iops = &old_obj->do_index_ops->dio_it;
	it = iops->init(env, old_obj, 0, NULL);
	if (IS_ERR(it))
		GOTO(out, rc = PTR_ERR(it));

	rc = iops->load(env, it, 0);
	if (rc == 0) {
		rc = iops->next(env, it);
		if (rc > 0)
			GOTO(out_it_fini, rc = 0);
	} else {
		if (rc > 0)
			rc = 0;
		else
			GOTO(out_it_fini, rc);
	}

	do {
		rc = iops->rec(env, it, (struct dt_rec *)range, 0);
		if (rc != 0)
			GOTO(out_it_fini, rc);

		rc = dt_declare_record_write(env, dt_obj,
				     sizeof(struct lu_seq_range),
				     0, th);
		if (rc != 0)
			GOTO(out_it_fini, rc);

		LASSERT(range != NULL);
		range_be_to_cpu(range, range);
		rc = fld_cache_insert(cache, range, pos);
		if (rc != 0)
			GOTO(out_it_fini, rc);

		pos += sizeof(struct lu_seq_range);

		rc = iops->next(env, it);

	} while (rc == 0);
	rc = 0;

out_it_fini:
	iops->fini(env, it);
out:
	if (rc != 0) {
		lu_object_put(env, &old_obj->do_lu);
		*old_objp = NULL;
	}

	RETURN(rc);
}

/**
 * Index initialization, Insert the hearder and some initial index
 **/
static int fld_index_init_internal(const struct lu_env *env,
				   struct dt_device *dt,
				   struct dt_object *dt_obj,
				   struct fld_cache *cache)
{
	struct fld_index_header fih = {0};
	struct thandle		*th;
	loff_t			pos = 0;
	struct lu_buf		buf;
	int			rc;
	struct dt_object	*old_dt = NULL;
	ENTRY;

	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	rc = dt_declare_record_write(env, dt_obj,
				     sizeof(struct fld_index_header),
				     pos, th);
	if (rc)
		GOTO(out_trans, rc);

	pos += sizeof(struct fld_index_header);
	rc = dt_declare_record_write(env, dt_obj,
				     sizeof(struct fld_index_header),
				     pos, th);
	if (rc)
		GOTO(out_trans, rc);

	rc = fld_declare_load_old_index(env, cache, dt, dt_obj, &old_dt, pos,
					th);
	if (rc != 0)
		GOTO(out_trans, rc);

	rc = dt_trans_start_local(env, dt, th);
	if (rc)
		GOTO(out_trans, rc);

	pos = 0;
	fih.fih_magic = cpu_to_be32(FIH_MAGIC_HEADER_V1);
	buf.lb_buf = &fih;
	buf.lb_len = sizeof(fih);
	rc = dt_record_write(env, dt_obj, &buf, &pos, th);
	if (rc != 0) {
		CERROR("write error: rc = %d\n", rc);
		GOTO(out_trans, rc);
	}

	if (old_dt != NULL) {
		rc = fld_load_old_index(env, dt, dt_obj, old_dt, pos, th,
					cache);
		if (rc != 0)
			GOTO(out_trans, rc);
		lu_object_put(env, &old_dt->do_lu);
	} else {
		rc = fld_write_range(env, dt_obj, &IGIF_FLD_RANGE, pos, th);
		if (rc != 0)
			GOTO(out_trans, rc);

		rc = fld_cache_insert(cache, &IGIF_FLD_RANGE, pos);
	}

out_trans:
	dt_trans_stop(env, dt, th);
	RETURN(rc);
}

int fld_index_init(struct lu_server_fld *fld,
                   const struct lu_env *env,
                   struct dt_device *dt)
{
	struct dt_object	*dt_obj;
	struct lu_fid		fid;
	struct lu_attr		*attr = NULL;
	struct fld_thread_info	*info;
	struct dt_object_format	dof;
	struct fld_index_header *fih;
	struct lu_buf		buf = {0};
	int			rc;
	char			*ptr;
	char			*end;
	loff_t			pos = 0;
	int			len;
	loff_t			offset;
	ENTRY;

	lu_local_obj_fid(&fid, FLD_INDEX_FLAT_OID);
	info = lu_context_key_get(&env->le_ctx, &fld_thread_key);
	LASSERT(info != NULL);

	OBD_ALLOC_PTR(attr);
	if (attr == NULL) {
		CERROR("%s: alloc attr error\n", fld->lsf_name);
		RETURN(-ENOMEM);
	}
	/* Find or create index object */
	attr->la_valid = LA_MODE;
	attr->la_mode = S_IFREG | 0666;
	dof.dof_type = DFT_REGULAR;
	dt_obj = dt_find_or_create(env, dt, &fid, &dof, attr);
	if (IS_ERR(dt_obj)) {
		CERROR("%s: Can't find \"%s\" obj: rc = %d\n",
			fld->lsf_name, fld_index_name, (int)PTR_ERR(dt_obj));
		GOTO(free, rc = PTR_ERR(dt_obj));
	}

	rc = dt_attr_get(env, dt_obj, attr, BYPASS_CAPA);
	if (rc != 0)
		GOTO(out_put, rc);

	fld->lsf_obj = dt_obj;
	fld->lsf_cache->fci_no_shrink = 1;
	if (attr->la_size == 0) {
		rc = fld_index_init_internal(env, dt, dt_obj, fld->lsf_cache);
		if (rc != 0)
			CERROR("%s: fld index init error: rc = %d\n",
			       fld->lsf_name, rc);
		GOTO(out_put, rc);
	}

	/* Load the entries to cache */
	if (attr->la_size > OBD_ALLOC_BIG)
		buf.lb_len = sizeof(struct lu_seq_range) *
				FLD_READ_ENTRIES_COUNT;
	else
		buf.lb_len = attr->la_size;

	OBD_ALLOC_LARGE(buf.lb_buf, buf.lb_len);
	if (buf.lb_buf == NULL)
		GOTO(out_put, rc = -ENOMEM);

	len = dt_read(env, dt_obj, &buf, &pos);
	if (len < 0)
		GOTO(out, rc = len);

	if (len != buf.lb_len) {
		CERROR("%s: got different size %d != %d\n",
			fld->lsf_name, rc, (int)buf.lb_len);
		GOTO(out, rc = -EINVAL);
	}

	/* Check the header of the fldb */
	fih = (struct fld_index_header *)buf.lb_buf;
	if (be32_to_cpu(fih->fih_magic) != FIH_MAGIC_HEADER_V1) {
		CERROR("%s: Corrupted index header %x\n",
			fld->lsf_name, be32_to_cpu(fih->fih_magic));
		GOTO(out, rc = -EINVAL);
	}

	/* skip head */
	offset = sizeof(*fih);
	ptr = buf.lb_buf + offset;
	end = buf.lb_buf + len;

	LASSERT(fld->lsf_cache != NULL);
	while (len > 0) {
		/* Load fld entries to cache one by one */
		while (ptr != end) {
			struct lu_seq_range *range;

			range = (struct lu_seq_range *)ptr;
			range_be_to_cpu(range, range);

			if (range->lsr_flags == LU_SEQ_RANGE_EMPTY)
				continue;

			if (range->lsr_flags != LU_SEQ_RANGE_MDT &&
			    range->lsr_flags != LU_SEQ_RANGE_OST) {
				CERROR("%s: invalid entry "DRANGE"\n",
					fld->lsf_name, PRANGE(range));
				GOTO(out, rc = -EINVAL);
			}

			rc = fld_cache_insert(fld->lsf_cache, range, offset);
			if (rc != 0) {
				CERROR("%s: cache insert error: rc = %d\n",
					fld->lsf_name, rc);
				GOTO(out, rc = -EINVAL);
			}
			LASSERT(end - ptr >= sizeof(struct lu_seq_range));
			ptr += sizeof(struct lu_seq_range);
			offset += sizeof(struct lu_seq_range);
		}

		/* continue read */
		len = dt_read(env, dt_obj, &buf, &pos);
		if (len <= 0) {
			if (len < 0) {
				CERROR("%s: read error: rc = %d\n",
					fld->lsf_name, len);
			}
			GOTO(out, rc = len);
			break;
		}
		ptr = buf.lb_buf;
		end = buf.lb_buf + len;
	}

out:
	if (buf.lb_buf != NULL)
		OBD_FREE_LARGE(buf.lb_buf, buf.lb_len);
out_put:
	if (rc < 0 && dt_obj != NULL) {
		lu_object_put(env, &dt_obj->do_lu);
		fld->lsf_obj = NULL;
	}
free:
	OBD_FREE_PTR(attr);
	RETURN(rc);
}

void fld_index_fini(struct lu_server_fld *fld,
                    const struct lu_env *env)
{
        ENTRY;
        if (fld->lsf_obj != NULL) {
                if (!IS_ERR(fld->lsf_obj))
                        lu_object_put(env, &fld->lsf_obj->do_lu);
                fld->lsf_obj = NULL;
        }
        EXIT;
}
