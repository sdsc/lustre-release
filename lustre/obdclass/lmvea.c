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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */

/*
 * Copyright (c) 2014, Intel Corporation.
 */

/*
 * lustre/obdclass/lmvea.c
 *
 * General functions for handling LMV EA.
 *
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#include <lustre/lustre_idl.h>
#include <lu_object.h>
#include <dt_object.h>
#include <md_object.h>
#include <lustre_capa.h>

/**
 * Append the FID for each shard of the striped directory after the
 * given LMV EA header.
 *
 * To simplify striped directory and the consistency verification,
 * we only store the LMV EA header on disk, for both master object
 * and slave objects. When someone wants to know the whole LMV EA,
 * such as client readdir, we will build the LMV EA on the MDT (in
 * RAM) via iterating the sub-directory entries that are contained
 * in the master object of the stripe directory.
 *
 * For the master object of the striped directroy, the valid name
 * for each shard is composed of the ${shard_FID}:${shard_idx}.
 *
 * There may be holes in the LMV EA if some shards' name entries
 * are corrupted or lost.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] obj	pointer to the master object of the striped directory
 * \param[in] ent	pointer to a temporary buffer for iteration
 * \param[in] buf	pointer to the lu_buf which will hold the LMV EA
 * \param[in] resize	whether re-allocate the buffer if it is not big enough
 *
 * \retval		positive size of the LMV EA
 * \retval		0 for nothing to be loaded
 * \retval		negative error number on failure
 */
int lmvea_load_shards(const struct lu_env *env, struct dt_object *obj,
		      struct lu_dirent *ent, struct lu_buf *buf,
		      bool resize)
{
	struct lmv_mds_md_v1	*lmv1	= buf->lb_buf;
	struct dt_it		*it;
	const struct dt_it_ops	*iops;
	__u32			 stripes;
	int			 size;
	int			 rc;

	/* If it is no a striped directory, then load nothing. */
	if (le32_to_cpu(lmv1->lmv_magic) != LMV_MAGIC_V1)
		return 0;

	/* If it is in migration (or failure), then load nothing. */
	if (le32_to_cpu(lmv1->lmv_hash_type) & LMV_HASH_FLAG_MIGRATION)
		return 0;

	stripes = le32_to_cpu(lmv1->lmv_stripe_count);
	size = lmv_mds_md_size(stripes, LMV_MAGIC_V1);
	if (buf->lb_len < size) {
		if (!resize)
			return -ERANGE;

		lu_buf_realloc(buf, size);
		lmv1 = buf->lb_buf;
		if (lmv1 == NULL)
			return -ENOMEM;
	}

	if (unlikely(!dt_try_as_dir(env, obj)))
		return -ENOTDIR;

	memset(&lmv1->lmv_stripe_fids[0], 0, stripes * sizeof(struct lu_fid));
	iops = &obj->do_index_ops->dio_it;
	it = iops->init(env, obj, LUDA_64BITHASH, BYPASS_CAPA);
	if (IS_ERR(it))
		return PTR_ERR(it);

	rc = iops->load(env, it, 0);
	if (rc == 0)
		rc = iops->next(env, it);
	else if (rc > 0)
		rc = 0;

	while (rc == 0) {
		char		 name[48] = { 0 };
		struct lu_fid	 fid;
		__u32		 index;
		int		 len;

		rc = iops->rec(env, it, (struct dt_rec *)ent, LUDA_64BITHASH);
		if (rc != 0)
			break;

		rc = -EINVAL;

		ent->lde_namelen = le16_to_cpu(ent->lde_namelen);
		if (ent->lde_name[0] == '.') {
			if (ent->lde_namelen == 1)
				goto next;

			if (ent->lde_namelen == 2 && ent->lde_name[1] == '.')
				goto next;

			break;
		}

		fid_le_to_cpu(&fid, &ent->lde_fid);
		snprintf(name, 47, DFID, PFID(&ent->lde_fid));
		len = strlen(name);

		/* The ent->lde_name is composed of ${FID}:${index} */
		if (ent->lde_namelen < len + 2)
			break;

		if (memcmp(ent->lde_name, name, len) != 0)
			break;

		if (ent->lde_name[len++] != ':')
			break;

		index = 0;
		do {
			if (ent->lde_name[len] < '0' ||
			    ent->lde_name[len] > '9')
				break;

			index = index * 10 + ent->lde_name[len++] - '0';
		} while (len < ent->lde_namelen);

		if (len == ent->lde_namelen) {
			/* Out of LMV EA range. */
			if (index >= stripes)
				break;

			/* The slot has been occupied. */
			if (!fid_is_zero(&lmv1->lmv_stripe_fids[index]))
				break;

			lmv1->lmv_stripe_fids[index] = ent->lde_fid;

next:
			rc = iops->next(env, it);
		}
	}

	iops->put(env, it);
	iops->fini(env, it);

	return rc > 0 ? lmv_mds_md_size(stripes, LMV_MAGIC_V1) : rc;
}
EXPORT_SYMBOL(lmvea_load_shards);
