/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osd/osd_oi.c
 *
 * Object Index.
 *
 * Author: Nikita Danilov <nikita@clusterfs.com>
 */

/*
 * oi uses two mechanisms to implement fid->cookie mapping:
 *
 *     - persistent index, where cookie is a record and fid is a key, and
 *
 *     - algorithmic mapping for "igif" fids.
 *
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>
/*
 * struct OBD_{ALLOC,FREE}*()
 * OBD_FAIL_CHECK
 */
#include <obd.h>
#include <obd_support.h>

/* fid_cpu_to_be() */
#include <lustre_fid.h>
#include <dt_object.h>

#include "osd_oi.h"
/* osd_lookup(), struct osd_thread_info */
#include "osd_internal.h"
#include "osd_igif.h"
#include "osd_scrub.h"

static unsigned int osd_oi_count = OSD_OI_FID_NR;
CFS_MODULE_PARM(osd_oi_count, "i", int, 0444,
                "Number of Object Index containers to be created, "
                "it's only valid for new filesystem.");

/** to serialize concurrent OI index initialization */
static cfs_mutex_t oi_init_lock;

static struct dt_index_features oi_feat = {
        .dif_flags       = DT_IND_UPDATE,
        .dif_recsize_min = sizeof(struct osd_inode_id),
        .dif_recsize_max = sizeof(struct osd_inode_id),
        .dif_ptrsize     = 4
};

#define OSD_OI_NAME_BASE        "oi.16"

/**
 * Open an OI(Ojbect Index) container.
 *
 * \param       name    Name of OI container
 * \param       objp    Pointer of returned OI
 *
 * \retval      0       success
 * \retval      -ve     failure
 */
static int
osd_oi_open(struct osd_thread_info *info, struct osd_device *osd,
            struct md_device *mdev, char *name, struct osd_oi **oi_slot,
            bool create)
{
        const struct lu_env *env = info->oti_env;
        struct dt_device *dev = &osd->od_dt_dev;
        struct dt_object *obj;
        struct osd_oi *oi;
        int rc;

        OBD_ALLOC_PTR(oi);
        if (oi == NULL)
                return -ENOMEM;

        oi_feat.dif_keysize_min = sizeof(struct lu_fid);
        oi_feat.dif_keysize_max = sizeof(struct lu_fid);

        info->oti_fid2 = info->oti_fid;
open:
        obj = dt_store_open(env, dev, "", name, &info->oti_fid);
        if (IS_ERR(obj)) {
                rc = PTR_ERR(obj);
                if (rc == -ENOENT && create) {
                        struct md_object *mdo;

                        info->oti_fid = info->oti_fid2;
                        mdo = llo_store_create_index(env, mdev, dev, "", name,
                                                     &info->oti_fid, &oi_feat);
                        if (IS_ERR(mdo)) {
                                CERROR("OI fid "DFID"\n", PFID(&info->oti_fid));
                                CERROR("Failed to create OI %s on %s: %ld\n",
                                       name, dev->dd_lu_dev.ld_obd->obd_name,
                                       PTR_ERR(mdo));
                                OBD_FREE_PTR(oi);
                                RETURN(PTR_ERR(mdo));
                        }
                        lu_object_put(env, &mdo->mo_lu);
                        goto open;
                } else {
                        OBD_FREE_PTR(oi);
                        return PTR_ERR(obj);
                }
        }

        rc = obj->do_ops->do_index_try(env, obj, &oi_feat);
        if (rc != 0) {
                lu_object_put(info->oti_env, &obj->do_lu);
                CERROR("%s: wrong index %s: rc = %d\n",
                       dev->dd_lu_dev.ld_obd->obd_name, name, rc);
                OBD_FREE_PTR(oi);
                return rc;
        }

        oi->oi_dir = obj;
        *oi_slot = oi;

        return 0;
}


static void
osd_oi_table_put(struct osd_thread_info *info,
                 struct osd_oi **oi_table, unsigned oi_count)
{
        int     i;

        for (i = 0; i < oi_count; i++) {
		if (oi_table[i] == NULL)
			continue;

                LASSERT(oi_table[i]->oi_dir != NULL);

                lu_object_put(info->oti_env, &oi_table[i]->oi_dir->do_lu);
                oi_table[i]->oi_dir = NULL;
		OBD_FREE_PTR(oi_table[i]);
		oi_table[i] = NULL;
        }
}

/**
 * Open OI(Object Index) table.
 * If \a oi_count is zero, which means caller doesn't know how many OIs there
 * will be, this function can either return 0 for new filesystem, or number
 * of OIs on existed filesystem.
 *
 * If \a oi_count is non-zero, which means caller does know number of OIs on
 * filesystem, this function should return the exactly same number on
 * success, or error code in failure.
 *
 * \param     oi_count  Number of expected OI containers
 * \param     try_all   Try to open all OIs even see failures
 *
 * \retval    +ve       number of opened OI containers
 * \retval      0       no OI containers found
 * \retval    -ve       failure
 */
static int
osd_oi_table_open(struct osd_thread_info *info, struct osd_device *osd,
                  struct md_device *mdev, struct osd_oi **oi_table,
                  unsigned oi_count, bool create)
{
	struct scrub_file *sf = &osd->od_scrub.os_file;
        struct dt_device *dev = &osd->od_dt_dev;
        int count = 0;
        int rc = 0;
        int i;
        ENTRY;

        /* NB: oi_count != 0 means that we have already created/known all OIs
         * and have known exact number of OIs. */
        LASSERT(oi_count <= OSD_OI_FID_NR_MAX);

        for (i = 0; i < (oi_count != 0 ? oi_count : OSD_OI_FID_NR_MAX); i++) {
                char name[12];

                if (oi_table[i] != NULL) {
			count++;
			continue;
		}

                sprintf(name, "%s.%d", OSD_OI_NAME_BASE, i);
                lu_local_obj_fid(&info->oti_fid, OSD_OI_FID_OID_FIRST + i);
                rc = osd_oi_open(info, osd, mdev, name, &oi_table[i], create);
                if (rc == 0) {
                        count++;
                        continue;
                }

                if (rc == -ENOENT && create == false) {
        		if (oi_count == 0)
				RETURN(count);

			rc = 0;
			ldiskfs_set_bit(i, sf->sf_oi_bitmap);
			continue;
                }

                CERROR("%s: can't open %s: rc = %d\n",
                       dev->dd_lu_dev.ld_obd->obd_name, name, rc);

                if (oi_count > 0) {
                        CERROR("%s: expect to open total %d OI files.\n",
                               dev->dd_lu_dev.ld_obd->obd_name, oi_count);
                }

                break;
        }

        if (rc < 0) {
		osd_oi_table_put(info, oi_table, oi_count > 0 ? oi_count : i);
		count = rc;
	}

	RETURN(count);
}

int osd_oi_init(struct osd_thread_info *info,
                struct osd_device *osd,
                struct md_device *mdev)
{
	struct dt_device  *dev = &osd->od_dt_dev;
	struct osd_scrub  *scrub = &osd->od_scrub;
	struct scrub_file *sf = &scrub->os_file;
	struct osd_oi    **oi;
	int		   rc;
	ENTRY;

	OBD_ALLOC(oi, sizeof(*oi) * OSD_OI_FID_NR_MAX);
	if (oi == NULL)
		RETURN(-ENOMEM);

	cfs_mutex_lock(&oi_init_lock);
	/* try to open existing multiple OIs first */
	rc = osd_oi_table_open(info, osd, mdev, oi, sf->sf_oi_count, false);
	if (rc < 0)
		GOTO(out, rc);

	if (rc > 0) {
		if (rc == sf->sf_oi_count || sf->sf_oi_count == 0)
			GOTO(out, rc);

		osd_scrub_file_reset(scrub,
				     LDISKFS_SB(osd_sb(osd))->s_es->s_uuid,
				     SF_RECREATED);
		osd_oi_count = sf->sf_oi_count;
		goto create;
	}

	/* if previous failed then try found single OI from old filesystem */
	rc = osd_oi_open(info, osd, mdev, OSD_OI_NAME_BASE, &oi[0], false);
	if (rc == 0) { /* found single OI from old filesystem */
		GOTO(out, rc = 1);
	} else if (rc != -ENOENT) {
		CERROR("%s: can't open %s: rc = %d\n",
		       dev->dd_lu_dev.ld_obd->obd_name, OSD_OI_NAME_BASE, rc);
		GOTO(out, rc);
	}

	if (sf->sf_oi_count > 0) {
		int i;

		memset(sf->sf_oi_bitmap, 0, SCRUB_OI_BITMAP_SIZE);
		for (i = 0; i < osd_oi_count; i++)
			ldiskfs_set_bit(i, sf->sf_oi_bitmap);
		osd_scrub_file_reset(scrub,
				     LDISKFS_SB(osd_sb(osd))->s_es->s_uuid,
				     SF_RECREATED);
	}
	sf->sf_oi_count = osd_oi_count;

create:
	rc = osd_scrub_file_store(scrub);
	if (rc < 0) {
		osd_oi_table_put(info, oi, sf->sf_oi_count);
		GOTO(out, rc);
	}

	/* No OIs exist, new filesystem, create OI objects */
	rc = osd_oi_table_open(info, osd, mdev, oi, osd_oi_count, true);
	LASSERT(ergo(rc >= 0, rc == osd_oi_count));

	GOTO(out, rc);

out:
	if (rc < 0) {
		OBD_FREE(oi, sizeof(*oi) * OSD_OI_FID_NR_MAX);
	} else {
		LASSERT((rc & (rc - 1)) == 0);
		osd->od_oi_table = oi;
		osd->od_oi_count = rc;
		rc = 0;
	}

	cfs_mutex_unlock(&oi_init_lock);
	return rc;
}

void osd_oi_fini(struct osd_thread_info *info, struct osd_device *osd)
{
        osd_oi_table_put(info, osd->od_oi_table, osd->od_oi_count);
        OBD_FREE(osd->od_oi_table,
                 sizeof(*(osd->od_oi_table)) * OSD_OI_FID_NR_MAX);
        osd->od_oi_table = NULL;
}

int __osd_oi_lookup(struct osd_thread_info *info, struct osd_device *osd,
		    const struct lu_fid *fid, struct osd_inode_id *id)
{
        struct osd_oi *oi = osd_fid2oi(osd, fid);
	struct lu_fid *oi_fid = &info->oti_fid2;
        struct dt_object *idx;
        const struct dt_key *key;
	int rc;

        idx = oi->oi_dir;
        key = (struct dt_key *) oi_fid;
	fid_cpu_to_be(oi_fid, fid);
        rc = idx->do_index_ops->dio_lookup(info->oti_env, idx,
                                           (struct dt_rec *)id, key,
                                           BYPASS_CAPA);
	if (rc > 0) {
		osd_id_unpack(id, id);
		rc = 0;
	} else if (rc == 0) {
		rc = -ENOENT;
	}
	return rc;
}

int osd_oi_lookup(struct osd_thread_info *info, struct osd_device *osd,
                  const struct lu_fid *fid, struct osd_inode_id *id)
{
        int rc = 0;

        if (osd_fid_is_igif(fid))
                lu_igif_to_id(fid, id);
        else if (!fid_is_norm(fid))
                rc = -ENOENT;
        else
		rc = __osd_oi_lookup(info, osd, fid, id);
        return rc;
}

int osd_oi_insert(struct osd_thread_info *info, struct osd_device *osd,
                  const struct lu_fid *fid, const struct osd_inode_id *id0,
                  struct thandle *th, int ignore_quota)
{
        struct lu_fid *oi_fid = &info->oti_fid;
        struct dt_object    *idx;
        struct osd_inode_id *id;
        const struct dt_key *key;

        if (!fid_is_norm(fid))
                return 0;

        idx = osd_fid2oi(osd, fid)->oi_dir;
        fid_cpu_to_be(oi_fid, fid);
        key = (struct dt_key *) oi_fid;

        id  = &info->oti_id;
        id->oii_ino = cpu_to_be32(id0->oii_ino);
        id->oii_gen = cpu_to_be32(id0->oii_gen);
        return idx->do_index_ops->dio_insert(info->oti_env, idx,
                                             (struct dt_rec *)id,
                                             key, th, BYPASS_CAPA,
                                             ignore_quota);
}

int osd_oi_delete(struct osd_thread_info *info, struct osd_device *osd,
                  const struct lu_fid *fid, struct thandle *th)
{
        struct lu_fid *oi_fid = &info->oti_fid;
        struct dt_object    *idx;
        const struct dt_key *key;

        if (!fid_is_norm(fid))
                return 0;

        idx = osd_fid2oi(osd, fid)->oi_dir;
        fid_cpu_to_be(oi_fid, fid);
        key = (struct dt_key *) oi_fid;
        return idx->do_index_ops->dio_delete(info->oti_env, idx,
                                             key, th, BYPASS_CAPA);
}

int osd_oi_mod_init(void)
{
        if (osd_oi_count == 0 || osd_oi_count > OSD_OI_FID_NR_MAX)
                osd_oi_count = OSD_OI_FID_NR;

        if ((osd_oi_count & (osd_oi_count - 1)) != 0) {
                LCONSOLE_WARN("Round up oi_count %d to power2 %d\n",
                              osd_oi_count, size_roundup_power2(osd_oi_count));
                osd_oi_count = size_roundup_power2(osd_oi_count);
        }

        cfs_mutex_init(&oi_init_lock);
        return 0;
}
