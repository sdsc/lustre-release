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
	lma->lma_compat   = 0;
	lma->lma_incompat = 0;
	lma->lma_self_fid = *fid;
	lma->lma_flags    = 0;

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
 * \param lma - is a pointer to the LMA structure to be swabbed.
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

/**
 * Swab, if needed, SOM structure which is stored on-disk in little-endian
 * order.
 *
 * \param attrs - is a pointer to the SOM structure to be swabbed.
 */
void lustre_som_swab(struct som_attrs *attrs)
{
	/* Use LUSTRE_MSG_MAGIC to detect local endianess. */
	if (LUSTRE_MSG_MAGIC != cpu_to_le32(LUSTRE_MSG_MAGIC)) {
		__swab32s(&attrs->som_compat);
		__swab32s(&attrs->som_incompat);
		__swab64s(&attrs->som_ioepoch);
		__swab64s(&attrs->som_size);
		__swab64s(&attrs->som_blocks);
		__swab64s(&attrs->som_mountid);
	}
};
EXPORT_SYMBOL(lustre_som_swab);

/*
 * Swab and extract SOM attributes from on-disk xattr.
 *
 * \param buf - is a buffer containing the on-disk SOM extended attribute.
 * \param rc  - is the SOM xattr stored in \a buf
 * \param msd - is the md_som_data structure where to extract SOM attributes.
 */
int lustre_buf2som(void *buf, int rc, struct md_som_data *msd)
{
	struct som_attrs *attrs = (struct som_attrs *)buf;
	ENTRY;

	if (rc == 0 ||  rc == -ENODATA)
		/* no SOM attributes */
		RETURN(-ENODATA);

	if (rc < 0)
		/* error hit while fetching xattr */
		RETURN(rc);

	/* check SOM compatibility */
	if (attrs->som_incompat & ~cpu_to_le32(SOM_INCOMPAT_SUPP))
		RETURN(-ENODATA);

	/* unpack SOM attributes */
	lustre_som_swab(attrs);

	/* fill in-memory msd structure */
	msd->msd_compat   = attrs->som_compat;
	msd->msd_incompat = attrs->som_incompat;
	msd->msd_ioepoch  = attrs->som_ioepoch;
	msd->msd_size     = attrs->som_size;
	msd->msd_blocks   = attrs->som_blocks;
	msd->msd_mountid  = attrs->som_mountid;

	RETURN(0);
}
EXPORT_SYMBOL(lustre_buf2som);

/**
 * Swab, if needed, HSM structure which is stored on-disk in little-endian
 * order.
 *
 * \param attrs - is a pointer to the HSM structure to be swabbed.
 */
void lustre_hsm_swab(struct hsm_attrs *attrs)
{
	/* Use LUSTRE_MSG_MAGIC to detect local endianess. */
	if (LUSTRE_MSG_MAGIC != cpu_to_le32(LUSTRE_MSG_MAGIC)) {
		__swab32s(&attrs->hsm_compat);
		__swab32s(&attrs->hsm_flags);
		__swab64s(&attrs->hsm_arch_id);
		__swab64s(&attrs->hsm_arch_ver);
	}
};
EXPORT_SYMBOL(lustre_hsm_swab);

/*
 * Swab and extract HSM attributes from on-disk xattr.
 *
 * \param buf - is a buffer containing the on-disk HSM extended attribute.
 * \param rc  - is the HSM xattr stored in \a buf
 * \param mh  - is the md_hsm structure where to extract HSM attributes.
 */
int lustre_buf2hsm(void *buf, int rc, struct md_hsm *mh)
{
	struct hsm_attrs *attrs = (struct hsm_attrs *)buf;
	ENTRY;

	if (rc == 0 ||  rc == -ENODATA)
		/* no HSM attributes */
		RETURN(-ENODATA);

	if (rc < 0)
		/* error hit while fetching xattr */
		RETURN(rc);

	/* unpack HSM attributes */
	lustre_hsm_swab(attrs);

	/* fill md_hsm structure */
	mh->mh_compat   = attrs->hsm_compat;
	mh->mh_flags    = attrs->hsm_flags;
	mh->mh_arch_id  = attrs->hsm_arch_id;
	mh->mh_arch_ver = attrs->hsm_arch_ver;

	RETURN(0);
}
EXPORT_SYMBOL(lustre_buf2hsm);

/*
 * Pack HSM attributes.
 *
 * \param buf - is the output buffer where to pack the on-disk HSM xattr.
 * \param mh  - is the md_hsm structure to pack.
 */
void lustre_hsm2buf(void *buf, struct md_hsm *mh)
{
	struct hsm_attrs *attrs = (struct hsm_attrs *)buf;
	ENTRY;

	/* copy HSM attributes */
	attrs->hsm_compat   = mh->mh_compat;
	attrs->hsm_flags    = mh->mh_flags;
	attrs->hsm_arch_id  = mh->mh_arch_id;
	attrs->hsm_arch_ver = mh->mh_arch_ver;

	/* pack xattr */
	lustre_hsm_swab(attrs);
}
EXPORT_SYMBOL(lustre_hsm2buf);

int linkea_data_new(struct linkea_data *ldata, struct lu_buf *buf)
{
	ldata->ld_buf = lu_buf_check_and_alloc(buf, CFS_PAGE_SIZE);
	if (ldata->ld_buf->lb_buf == NULL)
		return -ENOMEM;
	ldata->ld_leh = ldata->ld_buf->lb_buf;
	ldata->ld_leh->leh_magic = LINK_EA_MAGIC;
	ldata->ld_leh->leh_len = sizeof(struct link_ea_header);
	ldata->ld_leh->leh_reccount = 0;
	return 0;
}
EXPORT_SYMBOL(linkea_data_new);

/** Read the link EA into a temp buffer.
 * A pointer to the buffer is stored in \a ldata::ld_buf.
 *
 * \retval 0 or error
 */
int linkea_read(const struct lu_env *env, struct dt_object *dt_obj,
		struct linkea_data *ldata)
{
	struct link_ea_header *leh;
	int rc;

	LASSERT(ldata->ld_buf != NULL);
	rc = dt_xattr_get(env, dt_obj, ldata->ld_buf, XATTR_NAME_LINK,
			  BYPASS_CAPA);
	if (rc == -ERANGE) {
		/* Buf was too small, figure out what we need. */
		lu_buf_free(ldata->ld_buf);
		rc = dt_xattr_get(env, dt_obj, ldata->ld_buf,
				   XATTR_NAME_LINK, BYPASS_CAPA);
		if (rc < 0)
			return rc;
		ldata->ld_buf = lu_buf_check_and_alloc(ldata->ld_buf, rc);
		if (ldata->ld_buf->lb_buf == NULL)
			return -ENOMEM;
		rc = dt_xattr_get(env, dt_obj, ldata->ld_buf,
				  XATTR_NAME_LINK, BYPASS_CAPA);
	}
	if (rc < 0)
		return rc;

	leh = ldata->ld_buf->lb_buf;
	if (leh->leh_magic == __swab32(LINK_EA_MAGIC)) {
		leh->leh_magic = LINK_EA_MAGIC;
		leh->leh_reccount = __swab32(leh->leh_reccount);
		leh->leh_len = __swab64(leh->leh_len);
		/* entries are swabbed by linkea_entry_unpack */
	}
	if (leh->leh_magic != LINK_EA_MAGIC)
		return -EINVAL;
	if (leh->leh_reccount == 0)
		return -ENODATA;

	ldata->ld_leh = leh;
	return 0;
}
EXPORT_SYMBOL(linkea_read);

/**
 * Pack a link_ea_entry.
 * All elements are stored as chars to avoid alignment issues.
 * Numbers are always big-endian
 * \retval record length
 */
static int linkea_entry_pack(struct link_ea_entry *lee,
			     const struct lu_name *lname,
			     const struct lu_fid *pfid)
{
	struct lu_fid   tmpfid;
	int             reclen;

	fid_cpu_to_be(&tmpfid, pfid);
	if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_LINKEA_CRASH))
		tmpfid.f_ver = ~0;
	memcpy(&lee->lee_parent_fid, &tmpfid, sizeof(tmpfid));
	memcpy(lee->lee_name, lname->ln_name, lname->ln_namelen);
	reclen = sizeof(struct link_ea_entry) + lname->ln_namelen;

	lee->lee_reclen[0] = (reclen >> 8) & 0xff;
	lee->lee_reclen[1] = reclen & 0xff;
	return reclen;
}
EXPORT_SYMBOL(linkea_entry_pack);

void linkea_entry_unpack(const struct link_ea_entry *lee, int *reclen,
			 struct lu_name *lname, struct lu_fid *pfid)
{
	*reclen = (lee->lee_reclen[0] << 8) | lee->lee_reclen[1];
	memcpy(pfid, &lee->lee_parent_fid, sizeof(*pfid));
	fid_be_to_cpu(pfid, pfid);
	lname->ln_name = lee->lee_name;
	lname->ln_namelen = *reclen - sizeof(struct link_ea_entry);
}
EXPORT_SYMBOL(linkea_entry_unpack);

/**
 * Add a record to the end of link ea buf
 **/
int linkea_add_buf(struct linkea_data *ldata, const struct lu_name *lname,
		   const struct lu_fid *pfid)
{
	LASSERT(ldata->ld_leh != NULL);

	if (lname == NULL || pfid == NULL)
		return -EINVAL;

	ldata->ld_reclen = lname->ln_namelen + sizeof(struct link_ea_entry);
	if (ldata->ld_leh->leh_len + ldata->ld_reclen >
	    ldata->ld_buf->lb_len) {
		if (lu_buf_check_and_grow(ldata->ld_buf,
					  ldata->ld_leh->leh_len +
					  ldata->ld_reclen) < 0)
			return -ENOMEM;
	}

	ldata->ld_leh = ldata->ld_buf->lb_buf;
	ldata->ld_lee = ldata->ld_buf->lb_buf + ldata->ld_leh->leh_len;
	ldata->ld_reclen = linkea_entry_pack(ldata->ld_lee, lname, pfid);
	ldata->ld_leh->leh_len += ldata->ld_reclen;
	ldata->ld_leh->leh_reccount++;
	CDEBUG(D_INODE, "New link_ea name '%.*s' is added\n",
	       lname->ln_namelen, lname->ln_name);
	return 0;
}
EXPORT_SYMBOL(linkea_add_buf);

/** Del the current record from the link ea buf */
void linkea_del_buf(struct linkea_data *ldata, const struct lu_name *lname)
{
	LASSERT(ldata->ld_leh != NULL && ldata->ld_lee != NULL);

	ldata->ld_leh->leh_reccount--;
	ldata->ld_leh->leh_len -= ldata->ld_reclen;
	memmove(ldata->ld_lee, (char *)ldata->ld_lee + ldata->ld_reclen,
		(char *)ldata->ld_leh + ldata->ld_leh->leh_len -
		(char *)ldata->ld_lee);
	CDEBUG(D_INODE, "Old link_ea name '%.*s' is removed\n",
	       lname->ln_namelen, lname->ln_name);
}
EXPORT_SYMBOL(linkea_del_buf);

/**
 * Check if such a link exists in linkEA.
 *
 * \param ldata link data the search to be done on
 * \param lname name in the parent's directory entry pointing to this object
 * \param pfid parent fid the link to be found for
 *
 * \retval   0 success
 * \retval -ENOENT link does not exist
 * \retval -ve on error
 */
int linkea_links_find(struct linkea_data *ldata, const struct lu_name *lname,
		      const struct lu_fid  *pfid)
{
	struct lu_name tmpname;
	struct lu_fid  tmpfid;
	int count;

	LASSERT(ldata->ld_leh != NULL);

	/* link #0 */
	ldata->ld_lee = (struct link_ea_entry *)(ldata->ld_leh + 1);

	for (count = 0; count < ldata->ld_leh->leh_reccount; count++) {
		linkea_entry_unpack(ldata->ld_lee, &ldata->ld_reclen,
				    &tmpname, &tmpfid);
		if (tmpname.ln_namelen == lname->ln_namelen &&
		    lu_fid_eq(&tmpfid, pfid) &&
		    (strncmp(tmpname.ln_name, lname->ln_name,
			     tmpname.ln_namelen) == 0))
			break;
		ldata->ld_lee = (struct link_ea_entry *)((char *)ldata->ld_lee +
							 ldata->ld_reclen);
	}

	if (count == ldata->ld_leh->leh_reccount) {
		CDEBUG(D_INODE, "Old link_ea name '%.*s' not found\n",
		       lname->ln_namelen, lname->ln_name);
		ldata->ld_lee = NULL;
		return -ENOENT;
	}
	return 0;
}
EXPORT_SYMBOL(linkea_links_find);
