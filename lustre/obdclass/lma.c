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
 * Copyright (c) 2012 Intel, Inc.
 * Use is subject to license terms.
 *
 * Author: Johann Lombardi <johann.lombardi@intel.com>
 */

#include <lustre/lustre_idl.h>
#include <obd.h>
#include <md_object.h>

/**
 * Initialize new \a lma. Only fid is stored.
 *
 * \param lma - is the new LMA structure to be initialized
 * \param fid - is the FID of the object this LMA belongs to
 */
void lustre_lma_init(struct lustre_mdt_attrs *lma, const struct lu_fid *fid)
{
	lma->lma_compat      = 0;
	lma->lma_incompat    = 0;
	memcpy(&lma->lma_self_fid, fid, sizeof(*fid));
	lma->lma_flags       = 0;
	lma->lma_ioepoch     = 0;
	lma->lma_som_size    = 0;
	lma->lma_som_blocks  = 0;
	lma->lma_som_mountid = 0;

	/* If a field is added in struct lustre_mdt_attrs, zero it explicitly
	 * and change the test below. */
	LASSERT(sizeof(*lma) ==
		(offsetof(struct lustre_mdt_attrs, lma_som_mountid) +
		 sizeof(lma->lma_som_mountid)));
};
EXPORT_SYMBOL(lustre_lma_init);

/**
 * Swab, if needed, LMA structure which is stored on-disk in little-endian order.
 *
 * \param lma - is a pointed to the LMA structure to be swabbed.
 */
void lustre_lma_swab(struct lustre_mdt_attrs *lma)
{
	/* Use LUSTRE_MSG_MAGIC to detect local endianess. */
	if (LUSTRE_MSG_MAGIC != cpu_to_le32(LUSTRE_MSG_MAGIC)) {
		__swab32s(&lma->lma_compat);
		__swab32s(&lma->lma_incompat);
		lustre_swab_lu_fid(&lma->lma_self_fid);
		__swab64s(&lma->lma_flags);
		__swab64s(&lma->lma_ioepoch);
		__swab64s(&lma->lma_som_size);
		__swab64s(&lma->lma_som_blocks);
		__swab64s(&lma->lma_som_mountid);
	}
};
EXPORT_SYMBOL(lustre_lma_swab);

/*
 * Read LMA from disk and fill \a ma structure (if any) with HSM and SOM
 * attributes
 *
 * \param env - is the environment passed by the caller
 * \param obj - is the md_object for which we want to fetch the LMA
 * \param lma - is the buffer where to store the LMA
 * \param ma  - is the md_attr structure to fill with HSM & SOM attributes
 *              can be NULL.
 */
int lustre_lma_get(const struct lu_env *env, struct md_object *obj,
		   struct lustre_mdt_attrs *lma, struct md_attr *ma)
{
	struct lu_buf	buf;
	int		rc;
	ENTRY;

	LASSERT(lma != NULL);

	buf.lb_buf = lma;
	buf.lb_len = sizeof(*lma);

	/* read LMA from disk */
	rc = mo_xattr_get(env, obj, &buf, XATTR_NAME_LMA);
	if (rc == 0 || rc == -ENODATA)
		/* LMA wasn't initialized */
		RETURN(-ENODATA);
	else if (rc < 0)
		RETURN(rc);

	/* Check LMA compatibility */
	if (lma->lma_incompat & ~cpu_to_le32(LMA_INCOMPAT_SUPP)) {
		CWARN("%s: unsupported incompat LMA feature(s) %#x for "DFID
		      "\n", obj->mo_lu.lo_dev->ld_obd->obd_name,
		      le32_to_cpu(lma->lma_incompat) & ~LMA_INCOMPAT_SUPP,
		      PFID(lu_object_fid(&obj->mo_lu)));
		RETURN(-ENOSYS);
	}

	/* swab LMA */
	lustre_lma_swab(lma);

	if (ma && ma->ma_need & MA_HSM) {
		/* copy HSM attributes */
		if (lma->lma_compat & LMAC_HSM)
			ma->ma_hsm.mh_flags = lma->lma_flags & HSM_FLAGS_MASK;
		else
			ma->ma_hsm.mh_flags = 0;
		ma->ma_valid |= MA_HSM;
	}

	if (ma && ma->ma_need & MA_SOM && lma->lma_compat & LMAC_SOM) {
		/* copy SOM attributes */
		LASSERT(ma->ma_som != NULL);
		ma->ma_som->msd_ioepoch = lma->lma_ioepoch;
		ma->ma_som->msd_size    = lma->lma_som_size;
		ma->ma_som->msd_blocks  = lma->lma_som_blocks;
		ma->ma_som->msd_mountid = lma->lma_som_mountid;
		ma->ma_valid |= MA_SOM;
	}

	RETURN(0);
}
EXPORT_SYMBOL(lustre_lma_get);

/*
 * Swab and write LMA structure to disk
 *
 * \param env - is the environment passed by the caller
 * \param obj - is the md_object for which we want to update the LMA
 * \param lma - is the updated LMA to write
 */
int lustre_lma_set(const struct lu_env *env, struct md_object *obj,
		   struct lustre_mdt_attrs *lma)
{
	struct lu_buf	buf;
	ENTRY;

	LASSERT(lma != NULL);

	buf.lb_buf = lma;
	buf.lb_len = sizeof(*lma);

	/* swab LMA */
	lustre_lma_swab(lma);

	/* write LMA to disk */
	RETURN(mo_xattr_set(env, obj, &buf, XATTR_NAME_LMA, 0));
}
EXPORT_SYMBOL(lustre_lma_set);
