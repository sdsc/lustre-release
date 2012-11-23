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

	/* If a field is added in struct lustre_mdt_attrs, zero it explicitly
	 * and change the test below. */
	LASSERT(sizeof(*lma) ==
		(offsetof(struct lustre_mdt_attrs, lma_flags) +
		 sizeof(lma->lma_flags)));
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
	}
};
EXPORT_SYMBOL(lustre_lma_swab);

/*
 * Swab and extract SOM attributes from on-disk xattr.
 *
 * \param buf - is a buffer containing the on-disk SOM extended attribute.
 * \param rc  - is the SOM xattr stored in \a buf
 * \param ma  - is the md_attr structure where to extract SOM attributes.
 */
int lustre_som2ma(void *buf, int rc, struct md_attr *ma)
{
	struct som_attrs *attrs = (struct som_attrs *)buf;
	ENTRY;

	if (rc == 0 ||  rc == -ENODATA)
		/* no SOM attributes */
		RETURN(0);

	if (rc < 0)
		/* error hit while fetching xattr */
		RETURN(rc);

	/* copy SOM attributes */
	LASSERT(ma->ma_som != NULL);
	ma->ma_som->msd_compat  = le32_to_cpu(attrs->som_compat);
	ma->ma_som->msd_ioepoch = le64_to_cpu(attrs->som_ioepoch);
	ma->ma_som->msd_size    = le64_to_cpu(attrs->som_size);
	ma->ma_som->msd_blocks  = le64_to_cpu(attrs->som_blocks);
	ma->ma_som->msd_mountid = le64_to_cpu(attrs->som_mountid);
	ma->ma_valid |= MA_SOM;

	RETURN(0);
}
EXPORT_SYMBOL(lustre_som2ma);

/*
 * Swab and extract HSM attributes from on-disk xattr.
 *
 * \param buf - is a buffer containing the on-disk HSM extended attribute.
 * \param rc  - is the HSM xattr stored in \a buf
 * \param ma  - is the md_attr structure where to extract HSM attributes.
 */
int lustre_hsm2ma(void *buf, int rc, struct md_attr *ma)
{
	struct hsm_attrs *attrs = (struct hsm_attrs *)buf;
	ENTRY;

	if (rc == 0 ||  rc == -ENODATA)
		/* no HSM attributes */
		RETURN(0);

	if (rc < 0)
		/* error hit while fetching xattr */
		RETURN(rc);

	/* copy HSM attributes */
	ma->ma_hsm.mh_compat   = le32_to_cpu(attrs->hsm_compat);
	ma->ma_hsm.mh_flags    = le32_to_cpu(attrs->hsm_flags);
	ma->ma_hsm.mh_arch_id  = le64_to_cpu(attrs->hsm_arch_id);
	ma->ma_hsm.mh_arch_ver = le64_to_cpu(attrs->hsm_arch_ver);
	ma->ma_valid |= MA_HSM;

	RETURN(0);
}
EXPORT_SYMBOL(lustre_hsm2ma);
