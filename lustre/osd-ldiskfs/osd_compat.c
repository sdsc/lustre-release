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
 * Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/osd/osd_compat.c
 *
 * on-disk structure for managing /O
 *
 * Author: Alex Zhuravlev <bzzz@whamcloud.com>
 */

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>
/* prerequisite for linux/xattr.h */
#include <linux/types.h>
/* prerequisite for linux/xattr.h */
#include <linux/fs.h>
/* XATTR_{REPLACE,CREATE} */
#include <linux/xattr.h>

/*
 * struct OBD_{ALLOC,FREE}*()
 * OBD_FAIL_CHECK
 */
#include <obd_support.h>

#include "osd_internal.h"
#include "osd_oi.h"

char LAST_ID[] = "LAST_ID";

static void osd_push_ctxt(const struct osd_device *dev,
                          struct lvfs_run_ctxt *newctxt,
                          struct lvfs_run_ctxt *save)
{
        OBD_SET_CTXT_MAGIC(newctxt);
        newctxt->pwdmnt = dev->od_mnt;
        newctxt->pwd = dev->od_mnt->mnt_root;
        newctxt->fs = get_ds();

        push_ctxt(save, newctxt, NULL);
}

static void osd_pop_ctxt(const struct osd_device *dev,
			 struct lvfs_run_ctxt *new,
			 struct lvfs_run_ctxt *save)
{
	pop_ctxt(save, new, NULL);
}

/* utility to make a directory */
static struct dentry *simple_mkdir(struct dentry *dir, struct vfsmount *mnt,
				   const char *name, int mode, int fix)
{
	struct dentry *dchild;
	int err = 0;
	ENTRY;

	// ASSERT_KERNEL_CTXT("kernel doing mkdir outside kernel context\n");
	CDEBUG(D_INODE, "creating directory %.*s\n", (int)strlen(name), name);
	dchild = ll_lookup_one_len(name, dir, strlen(name));
	if (IS_ERR(dchild))
		GOTO(out_up, dchild);

	if (dchild->d_inode) {
		int old_mode = dchild->d_inode->i_mode;
		if (!S_ISDIR(old_mode)) {
			CERROR("found %s (%lu/%u) is mode %o\n", name,
			       dchild->d_inode->i_ino,
			       dchild->d_inode->i_generation, old_mode);
			GOTO(out_err, err = -ENOTDIR);
		}

		/* Fixup directory permissions if necessary */
		if (fix && (old_mode & S_IALLUGO) != (mode & S_IALLUGO)) {
			CDEBUG(D_CONFIG,
			       "fixing permissions on %s from %o to %o\n",
			       name, old_mode, mode);
			dchild->d_inode->i_mode = (mode & S_IALLUGO) |
						  (old_mode & ~S_IALLUGO);
			mark_inode_dirty(dchild->d_inode);
		}
		GOTO(out_up, dchild);
	}

	err = ll_vfs_mkdir(dir->d_inode, dchild, mnt, mode);
	if (err)
		GOTO(out_err, err);

	RETURN(dchild);

out_err:
	dput(dchild);
	dchild = ERR_PTR(err);
out_up:
	return dchild;
}

int osd_last_rcvd_subdir_count(struct osd_device *osd)
{
        struct lr_server_data lsd;
        struct dentry        *dlast;
        loff_t                off;
        int                   rc = 0;
	int                   count = FILTER_SUBDIR_COUNT;

        ENTRY;

        dlast = ll_lookup_one_len(LAST_RCVD, osd_sb(osd)->s_root,
                                  strlen(LAST_RCVD));
        if (IS_ERR(dlast))
                return PTR_ERR(dlast);
        else if (dlast->d_inode == NULL)
                goto out;

        off = 0;
        rc = osd_ldiskfs_read(dlast->d_inode, &lsd, sizeof(lsd), &off);
        if (rc == sizeof(lsd)) {
                CDEBUG(D_INFO, "read last_rcvd header, uuid = %s, "
                       "subdir count = %d\n", lsd.lsd_uuid,
                       lsd.lsd_subdir_count);
		if (le16_to_cpu(lsd.lsd_subdir_count) > 0)
			count = le16_to_cpu(lsd.lsd_subdir_count);
	} else if (rc != 0) {
		CERROR("Can't read last_rcvd file, rc = %d\n", rc);
		if (rc > 0)
			rc = -EFAULT;
		dput(dlast);
		return rc;
	}
out:
	dput(dlast);
	LASSERT(count > 0);
	return count;
}

static const char remote_parent_dir[] = "REMOTE_PARENT_DIR";
static int osd_mdt_init(const struct lu_env *env, struct osd_device *dev)
{
	struct lvfs_run_ctxt	new;
	struct lvfs_run_ctxt	save;
	struct dentry		*parent;
	struct osd_mdobj_map	*omm;
	struct dentry		*d;
	struct osd_thread_info	*info = osd_oti_get(env);
	struct lu_fid		*fid = &info->oti_fid3;
	int			rc = 0;
	ENTRY;

	OBD_ALLOC_PTR(dev->od_mdt_map);
	if (dev->od_mdt_map == NULL)
		RETURN(-ENOMEM);

	omm = dev->od_mdt_map;

	LASSERT(dev->od_fsops);

	parent = osd_sb(dev)->s_root;
	osd_push_ctxt(dev, &new, &save);

	d = simple_mkdir(parent, dev->od_mnt, remote_parent_dir,
			 0755, 1);
	if (IS_ERR(d))
		GOTO(cleanup, rc = PTR_ERR(d));

	ldiskfs_set_inode_state(d->d_inode, LDISKFS_STATE_LUSTRE_NO_OI);
	omm->omm_remote_parent = d;

	/* Set LMA for remote parent inode */
	lu_local_obj_fid(fid, REMOTE_PARENT_DIR_OID);
	rc = osd_ea_fid_set(info, d->d_inode, fid, LMAC_NOT_IN_OI, 0);
	if (rc != 0)
		GOTO(cleanup, rc);
cleanup:
	pop_ctxt(&save, &new, NULL);
	if (rc) {
		if (omm->omm_remote_parent != NULL)
			dput(omm->omm_remote_parent);
		OBD_FREE_PTR(omm);
		dev->od_mdt_map = NULL;
	}
	RETURN(rc);
}

static void osd_mdt_fini(struct osd_device *osd)
{
	struct osd_mdobj_map *omm = osd->od_mdt_map;

	if (omm == NULL)
		return;

	if (omm->omm_remote_parent)
		dput(omm->omm_remote_parent);

	OBD_FREE_PTR(omm);
	osd->od_ost_map = NULL;
}

int osd_add_to_remote_parent(const struct lu_env *env, struct osd_device *osd,
			     struct osd_object *obj, struct osd_thandle *oh)
{
	struct osd_mdobj_map	*omm = osd->od_mdt_map;
	struct osd_thread_info	*oti = osd_oti_get(env);
	struct lustre_mdt_attrs	*lma = &oti->oti_mdt_attrs;
	char			*name = oti->oti_name;
	struct dentry		*dentry;
	struct dentry		*parent;
	int			rc;

	/* Set REMOTE_PARENT in lma, so other process like unlink or lfsck
	 * can identify this object quickly */
	rc = osd_get_lma(oti, obj->oo_inode, &oti->oti_obj_dentry, lma);
	if (rc != 0)
		RETURN(rc);

	lma->lma_incompat |= LMAI_REMOTE_PARENT;
	lustre_lma_swab(lma);
	rc = __osd_xattr_set(oti, obj->oo_inode, XATTR_NAME_LMA, lma,
			     sizeof(*lma), XATTR_REPLACE);
	if (rc != 0)
		RETURN(rc);

	parent = omm->omm_remote_parent;
	sprintf(name, DFID_NOBRACE, PFID(lu_object_fid(&obj->oo_dt.do_lu)));
	dentry = osd_child_dentry_by_inode(env, parent->d_inode,
					   name, strlen(name));
	mutex_lock(&parent->d_inode->i_mutex);
	rc = osd_ldiskfs_add_entry(oh->ot_handle, dentry, obj->oo_inode,
				   NULL);
	CDEBUG(D_INODE, "%s: add %s:%lu to remote parent %lu.\n", osd_name(osd),
	       name, obj->oo_inode->i_ino, parent->d_inode->i_ino);
	LASSERTF(parent->d_inode->i_nlink > 1, "%s: %lu nlink %d",
		 osd_name(osd), parent->d_inode->i_ino,
		 parent->d_inode->i_nlink);
	parent->d_inode->i_nlink++;
	mark_inode_dirty(parent->d_inode);
	mutex_unlock(&parent->d_inode->i_mutex);
	RETURN(rc);
}

int osd_delete_from_remote_parent(const struct lu_env *env,
				  struct osd_device *osd,
				  struct osd_object *obj,
				  struct osd_thandle *oh)
{
	struct osd_mdobj_map	   *omm = osd->od_mdt_map;
	struct osd_thread_info	   *oti = osd_oti_get(env);
	struct lustre_mdt_attrs    *lma = &oti->oti_mdt_attrs;
	char			   *name = oti->oti_name;
	struct dentry		   *dentry;
	struct dentry		   *parent;
	struct ldiskfs_dir_entry_2 *de;
	struct buffer_head	   *bh;
	int			   rc;

	/* Check lma to see whether it is remote object */
	rc = osd_get_lma(oti, obj->oo_inode, &oti->oti_obj_dentry, lma);
	if (rc != 0)
		RETURN(rc);

	if (likely(!(lma->lma_incompat & LMAI_REMOTE_PARENT)))
		RETURN(0);

	parent = omm->omm_remote_parent;
	sprintf(name, DFID_NOBRACE, PFID(lu_object_fid(&obj->oo_dt.do_lu)));
	dentry = osd_child_dentry_by_inode(env, parent->d_inode,
					   name, strlen(name));
	mutex_lock(&parent->d_inode->i_mutex);
	bh = osd_ldiskfs_find_entry(parent->d_inode, dentry, &de, NULL);
	if (bh == NULL) {
		mutex_unlock(&parent->d_inode->i_mutex);
		RETURN(-ENOENT);
	}
	CDEBUG(D_INODE, "%s: el %s:%lu to remote parent %lu.\n", osd_name(osd),
	       name, obj->oo_inode->i_ino, parent->d_inode->i_ino);
	rc = ldiskfs_delete_entry(oh->ot_handle, parent->d_inode, de, bh);
	LASSERTF(parent->d_inode->i_nlink > 1, "%s: %lu nlink %d",
		 osd_name(osd), parent->d_inode->i_ino,
		 parent->d_inode->i_nlink);
	parent->d_inode->i_nlink--;
	mark_inode_dirty(parent->d_inode);
	mutex_unlock(&parent->d_inode->i_mutex);
	brelse(bh);

	/* Get rid of REMOTE_PARENT flag from incompat */
	lma->lma_incompat &= ~LMAI_REMOTE_PARENT;
	lustre_lma_swab(lma);
	rc = __osd_xattr_set(oti, obj->oo_inode, XATTR_NAME_LMA, lma,
			     sizeof(*lma), XATTR_REPLACE);
	RETURN(rc);
}

int osd_lookup_in_remote_parent(struct osd_thread_info *oti,
				struct osd_device *osd,
				const struct lu_fid *fid,
				struct osd_inode_id *id)
{
	struct osd_mdobj_map	    *omm = osd->od_mdt_map;
	char			    *name = oti->oti_name;
	struct dentry		    *parent;
	struct dentry		    *dentry;
	struct ldiskfs_dir_entry_2 *de;
	struct buffer_head	   *bh;
	int			    rc;
	ENTRY;

	parent = omm->omm_remote_parent;
	sprintf(name, DFID_NOBRACE, PFID(fid));
	dentry = osd_child_dentry_by_inode(oti->oti_env, parent->d_inode,
					   name, strlen(name));
	mutex_lock(&parent->d_inode->i_mutex);
	bh = osd_ldiskfs_find_entry(parent->d_inode, dentry, &de, NULL);
	if (bh == NULL) {
		rc = -ENOENT;
	} else {
		rc = 0;
		osd_id_gen(id, le32_to_cpu(de->inode), OSD_OII_NOGEN);
		brelse(bh);
	}
	mutex_unlock(&parent->d_inode->i_mutex);
	if (rc == 0)
		osd_add_oi_cache(oti, osd, id, fid);
	RETURN(rc);
}

/*
 * directory structure on legacy OST:
 *
 * O/<seq>/d0-31/<objid>
 * O/<seq>/LAST_ID
 * last_rcvd
 * LAST_GROUP
 * CONFIGS
 *
 */
static int osd_ost_init(const struct lu_env *env, struct osd_device *dev)
{
	struct lvfs_run_ctxt	 new;
	struct lvfs_run_ctxt	 save;
	struct dentry		*rootd = osd_sb(dev)->s_root;
	struct dentry		*d;
	struct osd_thread_info	*info = osd_oti_get(env);
	struct inode		*inode;
	struct lu_fid		*fid = &info->oti_fid3;
	int			 rc;
	ENTRY;

	OBD_ALLOC_PTR(dev->od_ost_map);
	if (dev->od_ost_map == NULL)
		RETURN(-ENOMEM);

	/* to get subdir count from last_rcvd */
	rc = osd_last_rcvd_subdir_count(dev);
	if (rc < 0) {
		OBD_FREE_PTR(dev->od_ost_map);
		RETURN(rc);
	}

	dev->od_ost_map->om_subdir_count = rc;
        rc = 0;

	CFS_INIT_LIST_HEAD(&dev->od_ost_map->om_seq_list);
	rwlock_init(&dev->od_ost_map->om_seq_list_lock);
	sema_init(&dev->od_ost_map->om_dir_init_sem, 1);

        LASSERT(dev->od_fsops);
        osd_push_ctxt(dev, &new, &save);

        d = simple_mkdir(rootd, dev->od_mnt, "O", 0755, 1);
	if (IS_ERR(d))
		GOTO(cleanup, rc = PTR_ERR(d));

	inode = d->d_inode;
	ldiskfs_set_inode_state(inode, LDISKFS_STATE_LUSTRE_NO_OI);
	dev->od_ost_map->om_root = d;

	/* 'What the @fid is' is not imporatant, because the object
	 * has no OI mapping, and only is visible inside the OSD.*/
	lu_igif_build(fid, inode->i_ino, inode->i_generation);
	rc = osd_ea_fid_set(info, inode, fid,
			    LMAC_NOT_IN_OI | LMAC_FID_ON_OST, 0);
	if (rc != 0)
		GOTO(cleanup, rc);

cleanup:
	osd_pop_ctxt(dev, &new, &save);
        if (IS_ERR(d)) {
                OBD_FREE_PTR(dev->od_ost_map);
                RETURN(PTR_ERR(d));
        }

	RETURN(rc);
}

static void osd_seq_free(struct osd_obj_map *map,
			 struct osd_obj_seq *osd_seq)
{
	int j;

	cfs_list_del_init(&osd_seq->oos_seq_list);

	if (osd_seq->oos_dirs) {
		for (j = 0; j < osd_seq->oos_subdir_count; j++) {
			if (osd_seq->oos_dirs[j])
				dput(osd_seq->oos_dirs[j]);
                }
		OBD_FREE(osd_seq->oos_dirs,
			 sizeof(struct dentry *) * osd_seq->oos_subdir_count);
        }

	if (osd_seq->oos_last_id_inode != NULL)
		iput(osd_seq->oos_last_id_inode);

	if (osd_seq->oos_root)
		dput(osd_seq->oos_root);

	OBD_FREE_PTR(osd_seq);
}

static void osd_ost_fini(struct osd_device *osd)
{
	struct osd_obj_seq    *osd_seq;
	struct osd_obj_seq    *tmp;
	struct osd_obj_map    *map = osd->od_ost_map;
	ENTRY;

	if (map == NULL)
		return;

	write_lock(&map->om_seq_list_lock);
	cfs_list_for_each_entry_safe(osd_seq, tmp,
				     &map->om_seq_list,
				     oos_seq_list) {
		osd_seq_free(map, osd_seq);
	}
	write_unlock(&map->om_seq_list_lock);
	if (map->om_root)
		dput(map->om_root);
	OBD_FREE_PTR(map);
	osd->od_ost_map = NULL;
	EXIT;
}

int osd_obj_map_init(const struct lu_env *env, struct osd_device *dev)
{
	int rc;
	ENTRY;

	/* prepare structures for OST */
	rc = osd_ost_init(env, dev);
	if (rc)
		RETURN(rc);

	/* prepare structures for MDS */
	rc = osd_mdt_init(env, dev);

        RETURN(rc);
}

struct osd_obj_seq *osd_seq_find_locked(struct osd_obj_map *map, obd_seq seq)
{
	struct osd_obj_seq *osd_seq;

	cfs_list_for_each_entry(osd_seq, &map->om_seq_list, oos_seq_list) {
		if (osd_seq->oos_seq == seq)
			return osd_seq;
	}
	return NULL;
}

struct osd_obj_seq *osd_seq_find(struct osd_obj_map *map, obd_seq seq)
{
	struct osd_obj_seq *osd_seq;

	read_lock(&map->om_seq_list_lock);
	osd_seq = osd_seq_find_locked(map, seq);
	read_unlock(&map->om_seq_list_lock);
	return osd_seq;
}

void osd_obj_map_fini(struct osd_device *dev)
{
	osd_ost_fini(dev);
	osd_mdt_fini(dev);
}

/**
 * Update the specified OI mapping.
 *
 * \retval   1, changed nothing
 * \retval   0, changed successfully
 * \retval -ve, on error
 */
static int osd_obj_update_entry(struct osd_thread_info *info,
				struct osd_device *osd,
				struct dentry *dir, const char *name,
				const struct osd_inode_id *id,
				struct thandle *th)
{
	struct inode		   *parent = dir->d_inode;
	struct osd_thandle	   *oh;
	struct dentry		   *child;
	struct ldiskfs_dir_entry_2 *de;
	struct buffer_head	   *bh;
	int			    rc;
	ENTRY;

	oh = container_of(th, struct osd_thandle, ot_super);
	LASSERT(oh->ot_handle != NULL);
	LASSERT(oh->ot_handle->h_transaction != NULL);

	child = &info->oti_child_dentry;
	child->d_parent = dir;
	child->d_name.hash = 0;
	child->d_name.name = name;
	child->d_name.len = strlen(name);

	ll_vfs_dq_init(parent);
	mutex_lock(&parent->i_mutex);
	bh = osd_ldiskfs_find_entry(parent, child, &de, NULL);
	if (bh == NULL) {
		rc = -ENOENT;
	} else if (le32_to_cpu(de->inode) == id->oii_ino) {
		rc = 1;
	} else {
		/* There may be temporary inconsistency: On one hand, the new
		 * object may be referenced by multiple entries, which is out
		 * of our control unless we traverse the whole /O completely,
		 * which is non-flat order and inefficient, should be avoided;
		 * On the other hand, the old object may become orphan if it
		 * is still valid. Since it was referenced by an invalid entry,
		 * making it as invisible temporary may be not worse. OI scrub
		 * will process it later. */
		rc = ldiskfs_journal_get_write_access(oh->ot_handle, bh);
		if (rc != 0)
			GOTO(out, rc);

		de->inode = cpu_to_le32(id->oii_ino);
		rc = ldiskfs_journal_dirty_metadata(oh->ot_handle, bh);
	}

	GOTO(out, rc);

out:
	brelse(bh);
	mutex_unlock(&parent->i_mutex);
	return rc;
}

static int osd_obj_del_entry(struct osd_thread_info *info,
			     struct osd_device *osd,
			     struct dentry *dird, char *name,
			     struct thandle *th)
{
	struct ldiskfs_dir_entry_2 *de;
	struct buffer_head         *bh;
	struct osd_thandle         *oh;
	struct dentry              *child;
	struct inode               *dir = dird->d_inode;
	int                         rc;
	ENTRY;

	oh = container_of(th, struct osd_thandle, ot_super);
	LASSERT(oh->ot_handle != NULL);
	LASSERT(oh->ot_handle->h_transaction != NULL);


	child = &info->oti_child_dentry;
	child->d_name.hash = 0;
	child->d_name.name = name;
	child->d_name.len = strlen(name);
	child->d_parent = dird;
	child->d_inode = NULL;

	ll_vfs_dq_init(dir);
	mutex_lock(&dir->i_mutex);
	rc = -ENOENT;
	bh = osd_ldiskfs_find_entry(dir, child, &de, NULL);
	if (bh) {
		rc = ldiskfs_delete_entry(oh->ot_handle, dir, de, bh);
		brelse(bh);
	}
	mutex_unlock(&dir->i_mutex);

	RETURN(rc);
}

int osd_obj_add_entry(struct osd_thread_info *info,
		      struct osd_device *osd,
		      struct dentry *dir, char *name,
		      const struct osd_inode_id *id,
		      struct thandle *th)
{
        struct osd_thandle *oh;
        struct dentry *child;
        struct inode *inode;
        int rc;

        ENTRY;

	if (OBD_FAIL_CHECK(OBD_FAIL_OSD_COMPAT_NO_ENTRY))
		RETURN(0);

        oh = container_of(th, struct osd_thandle, ot_super);
        LASSERT(oh->ot_handle != NULL);
        LASSERT(oh->ot_handle->h_transaction != NULL);

        inode = &info->oti_inode;
        inode->i_sb = osd_sb(osd);
	osd_id_to_inode(inode, id);
	inode->i_mode = S_IFREG; /* for type in ldiskfs dir entry */

        child = &info->oti_child_dentry;
        child->d_name.hash = 0;
        child->d_name.name = name;
        child->d_name.len = strlen(name);
        child->d_parent = dir;
        child->d_inode = inode;

	if (OBD_FAIL_CHECK(OBD_FAIL_OSD_COMPAT_INVALID_ENTRY))
		inode->i_ino++;

	ll_vfs_dq_init(dir->d_inode);
	mutex_lock(&dir->d_inode->i_mutex);
	rc = osd_ldiskfs_add_entry(oh->ot_handle, child, inode, NULL);
	mutex_unlock(&dir->d_inode->i_mutex);

	RETURN(rc);
}

/**
 * Use LPU64 for legacy OST sequences, but use LPX64i for new
 * sequences names, so that the O/{seq}/dN/{oid} more closely
 * follows the DFID/PFID format. This makes it easier to map from
 * debug messages to objects in the future, and the legacy space
 * of FID_SEQ_OST_MDT0 will be unused in the future.
 **/
static inline void osd_seq_name(char *seq_name, size_t name_size, obd_seq seq)
{
	snprintf(seq_name, name_size,
		 (fid_seq_is_rsvd(seq) ||
		  fid_seq_is_mdt0(seq)) ? LPU64 : LPX64i,
		 fid_seq_is_idif(seq) ? 0 : seq);
}

static inline void osd_oid_name(char *name, size_t name_size,
				const struct lu_fid *fid, obd_id id)
{
	snprintf(name, name_size,
		 (fid_seq_is_rsvd(fid_seq(fid)) ||
		  fid_seq_is_mdt0(fid_seq(fid)) ||
		  fid_seq_is_idif(fid_seq(fid))) ? LPU64 : LPX64i, id);
}

static int osd_last_id_load(struct osd_thread_info *info,
			    struct osd_device *osd,
			    struct osd_obj_seq *osd_seq,
			    bool create_lastid,
			    const struct osd_inode_id *id,
			    struct thandle *th)
{
	struct dentry	*dentry;
	struct inode	*inode;
	struct inode	*dir	= osd_seq->oos_root->d_inode;
	struct lu_fid	*fid	= &info->oti_fid3;
	obd_id		 last_id;
	loff_t		 pos	= 0;
	handle_t	*jh;
	int		 rc;
	ENTRY;

	fid->f_seq = osd_seq->oos_seq;
	fid->f_oid = LUSTRE_FID_LASTID_OID;
	fid->f_ver = 0;
	dentry = ll_lookup_one_len(LAST_ID, osd_seq->oos_root, strlen(LAST_ID));
	if (IS_ERR(dentry))
		RETURN(PTR_ERR(dentry));

	inode = dentry->d_inode;
	if (inode != NULL) {
		rc = osd_ldiskfs_read(inode, &last_id, sizeof(last_id), &pos);
		if (rc == sizeof(last_id)) {
			osd_seq->oos_last_id = le64_to_cpu(last_id);
			osd_seq->oos_last_id_inode = igrab(inode);
			rc = 0;
		} else if (create_lastid) {
			osd_seq->oos_last_id = 0;
			osd_seq->oos_last_id_inode = igrab(inode);
			rc = 0;
		} else {
			CERROR("%.16s: cannot load last_id: "DFID": rc = %d\n",
			       osd_name(osd), PFID(fid), rc);
		}

		dput(dentry);
		RETURN(rc);
	}

	dput(dentry);
	if (!create_lastid)
		RETURN(0);

	if (id != NULL) {
		struct osd_inode_id *id2 = &info->oti_id2;

		LASSERT(th != NULL);

		rc = osd_obj_add_entry(info, osd, osd_seq->oos_root, LAST_ID,
				       id, th);
		if (rc != 0) {
			CERROR("%.16s: cannot add last_id: "DFID", rc = %d\n",
			       osd_name(osd), PFID(fid), rc);
			RETURN(rc);
		}

		*id2 = *id;
		inode = osd_iget(info, osd, id2);
		if (IS_ERR(inode))
			RETURN(PTR_ERR(inode));

		osd_seq->oos_last_id = 0;
		osd_seq->oos_last_id_inode = inode;
		RETURN(0);
	}

	jh = ldiskfs_journal_start_sb(osd_sb(osd), 100);
	if (IS_ERR(jh))
		RETURN(PTR_ERR(jh));

	inode = ldiskfs_create_inode(jh, dir, S_IFREG | S_IRUGO | S_IWUSR);
	if (IS_ERR(inode)) {
		rc = PTR_ERR(inode);
		CERROR("%.16s: cannot create last_id: "DFID", rc = %d\n",
		       osd_name(osd), PFID(fid), rc);
		inode = NULL;
		GOTO(stop, rc);
	}

	rc = osd_ea_fid_set(info, inode, fid, LMAC_FID_ON_OST, 0);
	if (rc != 0) {
		CERROR("%.16s: cannot set LMA last_id: "DFID", rc = %d\n",
		       osd_name(osd), PFID(fid), rc);
		GOTO(stop, rc);
	}

	dentry = osd_child_dentry_by_inode(info->oti_env, dir, LAST_ID,
					   strlen(LAST_ID));
	rc = osd_ldiskfs_add_entry(jh, dentry, inode, NULL);
	if (rc != 0) {
		CERROR("%.16s: cannot insert last_id: "DFID", rc = %d\n",
		       osd_name(osd), PFID(fid), rc);
		GOTO(stop, rc);
	}

	osd_seq->oos_last_id = 0;
	osd_seq->oos_last_id_inode = inode;
	inode = NULL;

	GOTO(stop, rc = 0);

stop:
	if (inode != NULL)
		iput(inode);
	ldiskfs_journal_stop(jh);
	return rc;
}

/* external locking is required */
static int osd_seq_load_locked(struct osd_thread_info *info,
			       struct osd_device *osd,
			       struct osd_obj_seq *osd_seq,
			       bool create_lastid,
			       const struct osd_inode_id *id,
			       struct thandle *th)
{
	struct osd_obj_map  *map = osd->od_ost_map;
	struct dentry       *seq_dir;
	struct inode	    *inode;
	struct lu_fid	    *fid = &info->oti_fid3;
	int		    rc = 0;
	int		    i;
	char		    dir_name[32];
	ENTRY;

	if (osd_seq->oos_root != NULL)
		RETURN(0);

	LASSERT(map);
	LASSERT(map->om_root);

	osd_seq_name(dir_name, sizeof(dir_name), osd_seq->oos_seq);

	seq_dir = simple_mkdir(map->om_root, osd->od_mnt, dir_name, 0755, 1);
	if (IS_ERR(seq_dir))
		GOTO(out_err, rc = PTR_ERR(seq_dir));
	else if (seq_dir->d_inode == NULL)
		GOTO(out_put, rc = -EFAULT);

	inode = seq_dir->d_inode;
	ldiskfs_set_inode_state(inode, LDISKFS_STATE_LUSTRE_NO_OI);
	osd_seq->oos_root = seq_dir;

	/* 'What the @fid is' is not imporatant, because the object
	 * has no OI mapping, and only is visible inside the OSD.*/
	lu_igif_build(fid, inode->i_ino, inode->i_generation);
	rc = osd_ea_fid_set(info, inode, fid,
			    LMAC_NOT_IN_OI | LMAC_FID_ON_OST, 0);
	if (rc != 0)
		GOTO(out_put, rc);

	rc = osd_last_id_load(info, osd, osd_seq, create_lastid, id, th);
	if (rc != 0)
		GOTO(out_put, rc);

	LASSERT(osd_seq->oos_dirs == NULL);
	OBD_ALLOC(osd_seq->oos_dirs,
		  sizeof(seq_dir) * osd_seq->oos_subdir_count);
	if (osd_seq->oos_dirs == NULL)
		GOTO(out_put, rc = -ENOMEM);

	for (i = 0; i < osd_seq->oos_subdir_count; i++) {
		struct dentry   *dir;

		snprintf(dir_name, sizeof(dir_name), "d%u", i);
		dir = simple_mkdir(osd_seq->oos_root, osd->od_mnt, dir_name,
				   0700, 1);
		if (IS_ERR(dir)) {
			GOTO(out_free, rc = PTR_ERR(dir));
		} else if (dir->d_inode == NULL) {
			dput(dir);
			GOTO(out_free, rc = -EFAULT);
		}

		inode = dir->d_inode;
		ldiskfs_set_inode_state(inode, LDISKFS_STATE_LUSTRE_NO_OI);
		osd_seq->oos_dirs[i] = dir;

		/* 'What the @fid is' is not imporatant, because the object
		 * has no OI mapping, and only is visible inside the OSD.*/
		lu_igif_build(fid, inode->i_ino, inode->i_generation);
		rc = osd_ea_fid_set(info, inode, fid,
				    LMAC_NOT_IN_OI | LMAC_FID_ON_OST, 0);
		if (rc != 0)
			GOTO(out_free, rc);
	}

	if (rc != 0) {
out_free:
		for (i = 0; i < osd_seq->oos_subdir_count; i++) {
			if (osd_seq->oos_dirs[i] != NULL)
				dput(osd_seq->oos_dirs[i]);
		}
		OBD_FREE(osd_seq->oos_dirs,
			 sizeof(seq_dir) * osd_seq->oos_subdir_count);
out_put:
		dput(seq_dir);
		osd_seq->oos_root = NULL;
	}
out_err:
	RETURN(rc);
}

static struct osd_obj_seq *osd_seq_load(struct osd_thread_info *info,
					struct osd_device *osd, obd_seq seq,
					bool create_lastid,
					const struct osd_inode_id *id,
					struct thandle *th)
{
	struct osd_obj_map	*map;
	struct osd_obj_seq	*osd_seq;
	int			rc = 0;
	ENTRY;

	map = osd->od_ost_map;
	LASSERT(map);
	LASSERT(map->om_root);

	osd_seq = osd_seq_find(map, seq);
	if (likely(osd_seq != NULL))
		RETURN(osd_seq);

	/* Serializing init process */
	down(&map->om_dir_init_sem);

	/* Check whether the seq has been added */
	read_lock(&map->om_seq_list_lock);
	osd_seq = osd_seq_find_locked(map, seq);
	if (osd_seq != NULL) {
		read_unlock(&map->om_seq_list_lock);
		GOTO(cleanup, rc = 0);
	}
	read_unlock(&map->om_seq_list_lock);

	OBD_ALLOC_PTR(osd_seq);
	if (osd_seq == NULL)
		GOTO(cleanup, rc = -ENOMEM);

	CFS_INIT_LIST_HEAD(&osd_seq->oos_seq_list);
	spin_lock_init(&osd_seq->oos_last_id_lock);
	sema_init(&osd_seq->oos_last_id_sem, 1);
	osd_seq->oos_last_id_dirty = 0;

	osd_seq->oos_seq = seq;
	/* Init subdir count to be 32, but each seq can have
	 * different subdir count */
	osd_seq->oos_subdir_count = map->om_subdir_count;
	rc = osd_seq_load_locked(info, osd, osd_seq, create_lastid, id, th);
	if (rc != 0)
		GOTO(cleanup, rc);

	write_lock(&map->om_seq_list_lock);
	cfs_list_add(&osd_seq->oos_seq_list, &map->om_seq_list);
	write_unlock(&map->om_seq_list_lock);

cleanup:
	up(&map->om_dir_init_sem);
	if (rc != 0) {
		if (osd_seq != NULL)
			OBD_FREE_PTR(osd_seq);
		RETURN(ERR_PTR(rc));
	}

	RETURN(osd_seq);
}

int osd_obj_map_lookup(struct osd_thread_info *info, struct osd_device *dev,
		       const struct lu_fid *fid, struct osd_inode_id *id)
{
	struct osd_obj_map		*map;
	struct osd_obj_seq		*osd_seq;
	struct dentry			*d_seq;
	struct dentry			*child;
	struct ost_id			*ostid = &info->oti_ostid;
	int				dirn;
	char				name[32];
	struct ldiskfs_dir_entry_2	*de;
	struct buffer_head		*bh;
	struct inode			*dir;
	struct inode			*inode;
        ENTRY;

        /* on the very first lookup we find and open directories */

        map = dev->od_ost_map;
        LASSERT(map);
	LASSERT(map->om_root);

        fid_to_ostid(fid, ostid);
	osd_seq = osd_seq_load(info, dev, ostid_seq(ostid), false, NULL, NULL);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	dirn = ostid_id(ostid) & (osd_seq->oos_subdir_count - 1);
	d_seq = osd_seq->oos_dirs[dirn];
	LASSERT(d_seq);

	osd_oid_name(name, sizeof(name), fid, ostid_id(ostid));

	child = &info->oti_child_dentry;
	child->d_parent = d_seq;
	child->d_name.hash = 0;
	child->d_name.name = name;
	/* XXX: we can use rc from sprintf() instead of strlen() */
	child->d_name.len = strlen(name);

	dir = d_seq->d_inode;
	mutex_lock(&dir->i_mutex);
	bh = osd_ldiskfs_find_entry(dir, child, &de, NULL);
	mutex_unlock(&dir->i_mutex);

	if (bh == NULL)
		RETURN(-ENOENT);

	osd_id_gen(id, le32_to_cpu(de->inode), OSD_OII_NOGEN);
	brelse(bh);

	inode = osd_iget(info, dev, id);
	if (IS_ERR(inode))
		RETURN(PTR_ERR(inode));

	iput(inode);
	RETURN(0);
}

int osd_obj_map_insert(struct osd_thread_info *info,
		       struct osd_device *osd,
		       const struct lu_fid *fid,
		       const struct osd_inode_id *id,
		       struct thandle *th)
{
	struct osd_obj_seq	*osd_seq;
	struct dentry		*d;
	struct ost_id		*ostid = &info->oti_ostid;
	obd_id			 oid;
	int			dirn, rc = 0;
	char			name[32];
	ENTRY;

	fid_to_ostid(fid, ostid);
	oid = ostid_id(ostid);
	osd_seq = osd_seq_load(info, osd, ostid_seq(ostid), false, NULL, NULL);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	dirn = oid & (osd_seq->oos_subdir_count - 1);
	d = osd_seq->oos_dirs[dirn];
	LASSERT(d);

	osd_oid_name(name, sizeof(name), fid, oid);
	rc = osd_obj_add_entry(info, osd, d, name, id, th);
	if (rc != 0) {
		struct inode		   *dir   = d->d_inode;
		struct dentry		   *child;
		struct ldiskfs_dir_entry_2 *de;
		struct buffer_head	   *bh;
		struct osd_inode_id	   *oi_id = &info->oti_id2;
		struct inode		   *inode;
		struct lustre_mdt_attrs	   *lma   = &info->oti_mdt_attrs;

		if (rc != -EEXIST)
			RETURN(rc);

		child = &info->oti_child_dentry;
		child->d_parent = d;
		child->d_name.hash = 0;
		child->d_name.name = name;
		child->d_name.len = strlen(name);

		mutex_lock(&dir->i_mutex);
		bh = osd_ldiskfs_find_entry(dir, child, &de, NULL);
		mutex_unlock(&dir->i_mutex);
		if (unlikely(bh == NULL))
			RETURN(rc);

		osd_id_gen(oi_id, le32_to_cpu(de->inode), OSD_OII_NOGEN);
		brelse(bh);

		if (osd_id_eq(id, oi_id)) {
			CERROR("%.16s: the FID "DFID" is there already:%u/%u\n",
			       LDISKFS_SB(osd_sb(osd))->s_es->s_volume_name,
			       PFID(fid), id->oii_ino, id->oii_gen);
			RETURN(-EEXIST);
		}

		inode = osd_iget(info, osd, oi_id);
		if (IS_ERR(inode)) {
			rc = PTR_ERR(inode);
			if (rc == -ENOENT || rc == -ESTALE)
				goto update;
			RETURN(rc);
		}

		rc = osd_get_lma(info, inode, &info->oti_obj_dentry, lma);
		iput(inode);
		if (rc == -ENODATA)
			goto update;

		if (rc != 0)
			RETURN(rc);

		if (!(lma->lma_compat & LMAC_NOT_IN_OI) &&
		    lu_fid_eq(fid, &lma->lma_self_fid)) {
			CERROR("%.16s: the FID "DFID" is used by two objects: "
			       "%u/%u %u/%u\n",
			       LDISKFS_SB(osd_sb(osd))->s_es->s_volume_name,
			       PFID(fid), oi_id->oii_ino, oi_id->oii_gen,
			       id->oii_ino, id->oii_gen);
			RETURN(-EEXIST);
		}

update:
		rc = osd_obj_update_entry(info, osd, d, name, id, th);
	}

	RETURN(rc);
}

int osd_obj_map_delete(struct osd_thread_info *info, struct osd_device *osd,
		       const struct lu_fid *fid, struct thandle *th)
{
	struct osd_obj_seq	*osd_seq;
	struct dentry		*d;
	struct ost_id		*ostid = &info->oti_ostid;
	int			dirn, rc = 0;
	char			name[32];
        ENTRY;

        fid_to_ostid(fid, ostid);
	osd_seq = osd_seq_load(info, osd, ostid_seq(ostid), false, NULL, NULL);
	if (IS_ERR(osd_seq))
		GOTO(cleanup, rc = PTR_ERR(osd_seq));

	dirn = ostid_id(ostid) & (osd_seq->oos_subdir_count - 1);
	d = osd_seq->oos_dirs[dirn];
	LASSERT(d);

	osd_oid_name(name, sizeof(name), fid, ostid_id(ostid));
	rc = osd_obj_del_entry(info, osd, d, name, th);
cleanup:
        RETURN(rc);
}

int osd_obj_map_update(struct osd_thread_info *info,
		       struct osd_device *osd,
		       const struct lu_fid *fid,
		       const struct osd_inode_id *id,
		       struct thandle *th)
{
	struct osd_obj_seq	*osd_seq;
	struct dentry		*d;
	struct ost_id		*ostid = &info->oti_ostid;
	int			dirn, rc = 0;
	char			name[32];
	ENTRY;

	fid_to_ostid(fid, ostid);
	osd_seq = osd_seq_load(info, osd, ostid_seq(ostid), false, NULL, NULL);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	dirn = ostid_id(ostid) & (osd_seq->oos_subdir_count - 1);
	d = osd_seq->oos_dirs[dirn];
	LASSERT(d);

	osd_oid_name(name, sizeof(name), fid, ostid_id(ostid));
	rc = osd_obj_update_entry(info, osd, d, name, id, th);

	RETURN(rc);
}

int osd_obj_map_rename(struct osd_thread_info *info,
		       struct osd_device *osd,
		       struct dentry *src_parent,
		       struct dentry *src_child,
		       const struct lu_fid *fid)
{
	struct osd_obj_seq	   *osd_seq;
	struct dentry		   *tgt_parent;
	struct dentry		   *tgt_child = &info->oti_child_dentry;
	struct inode		   *dir;
	struct ost_id		   *ostid     = &info->oti_ostid;
	handle_t		   *jh;
	struct ldiskfs_dir_entry_2 *de;
	struct buffer_head	   *bh;
	char			    name[32];
	int			    dirn;
	int			    rc	      = 0;
	ENTRY;

	if (fid_is_last_id(fid)) {
		osd_seq = osd_seq_load(info, osd, fid_seq(fid), false,
				       NULL, NULL);
		if (IS_ERR(osd_seq))
			RETURN(PTR_ERR(osd_seq));

		tgt_parent = osd_seq->oos_root;
		tgt_child->d_name.name = LAST_ID;
		tgt_child->d_name.len = strlen(LAST_ID);
	} else {
		fid_to_ostid(fid, ostid);
		osd_seq = osd_seq_load(info, osd, ostid_seq(ostid), false,
				       NULL, NULL);
		if (IS_ERR(osd_seq))
			RETURN(PTR_ERR(osd_seq));

		dirn = ostid_id(ostid) & (osd_seq->oos_subdir_count - 1);
		tgt_parent = osd_seq->oos_dirs[dirn];
		osd_oid_name(name, sizeof(name), fid, ostid_id(ostid));
		tgt_child->d_name.name = name;
		tgt_child->d_name.len = strlen(name);
	}
	LASSERT(tgt_parent != NULL);

	dir = tgt_parent->d_inode;
	tgt_child->d_name.hash = 0;
	tgt_child->d_parent = tgt_parent;
	tgt_child->d_inode = src_child->d_inode;

	jh = ldiskfs_journal_start_sb(osd_sb(osd),
				osd_dto_credits_noquota[DTO_INDEX_DELETE] +
				osd_dto_credits_noquota[DTO_INDEX_INSERT]);
	if (IS_ERR(jh))
		RETURN(PTR_ERR(jh));

	ll_vfs_dq_init(src_parent->d_inode);
	ll_vfs_dq_init(dir);

	/* There will be only single thread to access the @dsp, needs
	 * NOT lock. So only take mutex against the tgrget parent. */
	mutex_lock(&dir->i_mutex);
	bh = osd_ldiskfs_find_entry(dir, tgt_child, &de, NULL);
	if (bh != NULL) {
		brelse(bh);
		GOTO(unlock, rc = -EEXIST);
	}

	bh = osd_ldiskfs_find_entry(src_parent->d_inode, src_child, &de, NULL);
	if (unlikely(bh == NULL))
		GOTO(unlock, rc = -ENOENT);

	rc = ldiskfs_delete_entry(jh, src_parent->d_inode, de, bh);
	brelse(bh);
	if (rc != 0)
		GOTO(unlock, rc);

	rc = osd_ldiskfs_add_entry(jh, tgt_child, src_child->d_inode, NULL);

	GOTO(unlock, rc);

unlock:
	mutex_unlock(&dir->i_mutex);
	ldiskfs_journal_stop(jh);
	return rc;
}

static struct dentry *
osd_object_spec_find(struct osd_thread_info *info, struct osd_device *osd,
		     const struct lu_fid *fid, char **name, bool create_lastid,
		     const struct osd_inode_id *id, struct thandle *th)
{
	struct dentry *root = ERR_PTR(-ENOENT);

	if (fid_is_last_id(fid)) {
		struct osd_obj_seq *osd_seq;

		/* on creation of LAST_ID we create O/<seq> hierarchy */
		osd_seq = osd_seq_load(info, osd, fid_seq(fid), create_lastid,
				       id, th);
		if (IS_ERR(osd_seq))
			RETURN((struct dentry *)osd_seq);

		if (!create_lastid) {
			*name = LAST_ID;
			root = osd_seq->oos_root;
		} else if (unlikely(osd_seq->oos_last_id_inode == NULL)) {
			int rc;

			/* Found a cached one, we need to add the last_id. */
			rc = osd_last_id_load(info, osd, osd_seq, true, id, th);
			if (rc != 0)
				root = (struct dentry *)ERR_PTR(rc);
		}
	} else {
		*name = osd_lf_fid2name(fid);
		if (*name == NULL)
			CWARN("UNKNOWN COMPAT FID "DFID"\n", PFID(fid));
		else if ((*name)[0])
			root = osd_sb(osd)->s_root;
	}

	return root;
}

int osd_obj_spec_update(struct osd_thread_info *info, struct osd_device *osd,
			const struct lu_fid *fid, const struct osd_inode_id *id,
			struct thandle *th)
{
	struct dentry	*root;
	char		*name;
	int		 rc;
	ENTRY;

	root = osd_object_spec_find(info, osd, fid, &name, false, NULL, NULL);
	if (!IS_ERR(root)) {
		rc = osd_obj_update_entry(info, osd, root, name, id, th);
	} else {
		rc = PTR_ERR(root);
		if (rc == -ENOENT)
			rc = 1;
	}

	RETURN(rc);
}

int osd_obj_spec_insert(struct osd_thread_info *info, struct osd_device *osd,
			const struct lu_fid *fid, const struct osd_inode_id *id,
			struct thandle *th)
{
	struct dentry	*root;
	char		*name;
	int		 rc;
	ENTRY;

	root = osd_object_spec_find(info, osd, fid, &name, true, id, th);
	if (!IS_ERR(root)) {
		rc = osd_obj_add_entry(info, osd, root, name, id, th);
	} else {
		rc = PTR_ERR(root);
		if (rc == -ENOENT)
			rc = 0;
	}

	RETURN(rc);
}

int osd_obj_spec_lookup(struct osd_thread_info *info, struct osd_device *osd,
			const struct lu_fid *fid, struct osd_inode_id *id)
{
	struct dentry	*root;
	struct dentry	*dentry;
	struct inode	*inode;
	char		*name;
	int		 rc;
	ENTRY;

	if (fid_is_last_id(fid)) {
		struct osd_obj_seq *osd_seq;

		osd_seq = osd_seq_load(info, osd, fid_seq(fid), false,
				       NULL, NULL);
		if (IS_ERR(osd_seq))
			RETURN(PTR_ERR(osd_seq));

		if (osd_seq->oos_last_id_inode == NULL)
			RETURN(-ENOENT);

		osd_id_gen(id, osd_seq->oos_last_id_inode->i_ino,
			   osd_seq->oos_last_id_inode->i_generation);
		RETURN(0);
	}

	root = osd_sb(osd)->s_root;
	name = osd_lf_fid2name(fid);
	if (name == NULL || strlen(name) == 0)
		RETURN(-ENOENT);

	dentry = ll_lookup_one_len(name, root, strlen(name));
	if (IS_ERR(dentry))
		RETURN(PTR_ERR(dentry));

	inode = dentry->d_inode;
	if (inode == NULL)
		GOTO(out, rc = -ENOENT);

	if (is_bad_inode(inode))
		GOTO(out, rc = -EIO);

	osd_id_gen(id, inode->i_ino, inode->i_generation);
	GOTO(out, rc = 0);

out:
	if (rc != 0)
		d_invalidate(dentry);
	dput(dentry);
	return rc;
}

/**
 * \retval +v, the data size read.
 * \retval -v, failure cases.
 */
int osd_last_id_read(const struct lu_env *env, struct dt_object *dt,
		     obd_id *last_id, loff_t *pos)
{
	struct osd_thread_info	*info	= osd_oti_get(env);
	struct osd_obj_seq	*osd_seq;
	ENTRY;

	osd_seq = osd_seq_load(info, osd_obj2dev(osd_dt_obj(dt)),
			       fid_seq(lu_object_fid(&dt->do_lu)),
			       false, NULL, NULL);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	if (unlikely(osd_seq->oos_last_id_inode == NULL))
		RETURN(-ENOENT);

	spin_lock(&osd_seq->oos_last_id_lock);
	*last_id = cpu_to_le64(osd_seq->oos_last_id);
	spin_unlock(&osd_seq->oos_last_id_lock);
	*pos = sizeof(*last_id);

	RETURN(sizeof(*last_id));
}

/**
 * \retval +v, the data size written.
 * \retval -v, failure cases.
 */
int osd_last_id_write(const struct lu_env *env, struct dt_object *dt,
		      obd_id *last_id, loff_t *pos, handle_t *handle)
{
	struct osd_thread_info	*info	= osd_oti_get(env);
	struct osd_obj_seq	*osd_seq;
	int			 rc;
	ENTRY;

	osd_seq = osd_seq_load(info, osd_obj2dev(osd_dt_obj(dt)),
			       fid_seq(lu_object_fid(&dt->do_lu)),
			       false, NULL, NULL);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	if (unlikely(osd_seq->oos_last_id_inode == NULL))
		RETURN(-ENOENT);

	down(&osd_seq->oos_last_id_sem);
	spin_lock(&osd_seq->oos_last_id_lock);
	osd_seq->oos_last_id = le64_to_cpu(*last_id);
	osd_seq->oos_last_id_dirty = 0;
	/* Release the spin_lock to allow others (OI scrub) to update the
	 * osd_seq::oss_last_id during the LAST_ID file to be written. */
	spin_unlock(&osd_seq->oos_last_id_lock);

	if (OBD_FAIL_CHECK(OBD_FAIL_OSD_COMPAT_SKIP_LASTID)) {
		up(&osd_seq->oos_last_id_sem);
		RETURN(0);
	}

	rc = osd_ldiskfs_write_record(osd_dt_obj(dt)->oo_inode, last_id,
				      sizeof(*last_id), 0, pos, handle);
	if (rc == 0) {
		rc = sizeof(*last_id);
	} else {
		spin_lock(&osd_seq->oos_last_id_lock);
		osd_seq->oos_last_id_dirty = 1;
		spin_unlock(&osd_seq->oos_last_id_lock);
	}
	up(&osd_seq->oos_last_id_sem);

	RETURN(rc);
}

/**
 * \retval +v, the given fid is higher than the known last id, updated
 *	       or the last_id is lost or crashed, re-created
 * \retval 0, the given fid is not higher the known last id
 * \retval -v, failure cases
 */
int osd_last_id_update(const struct lu_env *env, struct osd_device *osd,
		       struct inode *inode, const struct lu_fid *fid)
{
	struct osd_thread_info	*info	= osd_oti_get(env);
	struct ost_id		*ostid	= &info->oti_ostid;
	struct osd_obj_seq	*osd_seq;
	obd_id			 last_id;
	int			 rc	= 0;
	ENTRY;

	if (fid_is_last_id(fid)) {
		osd_seq = osd_seq_load(info, osd, fid_seq(fid), true,
				       NULL, NULL);
	} else {
		fid_to_ostid(fid, ostid);
		osd_seq = osd_seq_load(info, osd, ostid_seq(ostid), true,
				       NULL, NULL);
	}
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	/* Found a cached one, we need to rebuild the last_id. */
	if (unlikely(osd_seq->oos_last_id_inode == NULL)) {
		rc = osd_last_id_load(info, osd, osd_seq, true, NULL, NULL);
		if (rc != 0)
			RETURN(rc);
	}

	if (fid_is_last_id(fid)) {
		if (osd_seq->oos_last_id == 0)
			RETURN(1);
		RETURN(0);
	}

	last_id = ostid_id(ostid);
	if (last_id <= osd_seq->oos_last_id)
		RETURN(0);

	spin_lock(&osd_seq->oos_last_id_lock);
	if (likely(last_id > osd_seq->oos_last_id)) {
		/* There may be race between OI scrub increasing the LAST_ID
		 * and the OFD shrink the LAST_ID. So if the @inode has been
		 * destroyed when waitting for lock, then skip the update. */
		if (likely((inode->i_nlink != 0) ||
			   !(inode->i_mode & (S_ISUID | S_ISGID)))) {
			osd_seq->oos_last_id = last_id;
			osd_seq->oos_last_id_dirty = 1;
			rc = 1;
		}
	}
	spin_unlock(&osd_seq->oos_last_id_lock);

	RETURN(rc);
}

/**
 * \retval 0, scucceed.
 * \retval -v, failure cases.
 */
int osd_last_id_sync(const struct lu_env *env, struct osd_device *osd)
{
	struct osd_obj_map	*map	= osd->od_ost_map;
	struct osd_obj_seq	*osd_seq;
	int			 rc1	= 0;
	ENTRY;

	/* The "osd_seq" will be unlinked only when the device fini. We do not
	 * care some new "osd_seq" to be added during the list scanning. So it
	 * is safe to scan the list without lock. */
	cfs_list_for_each_entry(osd_seq, &map->om_seq_list, oos_seq_list) {
		obd_id		 last_id;
		loff_t		 pos	= 0;
		handle_t	*jh;
		int		 rc;

		if (!osd_seq->oos_last_id_dirty)
			continue;

		LASSERT(osd_seq->oos_last_id_inode != NULL);

		jh = ldiskfs_journal_start_sb(osd_sb(osd),
				osd_dto_credits_noquota[DTO_WRITE_BASE] +
				osd_dto_credits_noquota[DTO_WRITE_BLOCK]);
		if (IS_ERR(jh)) {
			rc1 = PTR_ERR(jh);
			continue;
		}

		down(&osd_seq->oos_last_id_sem);
		if (unlikely(!osd_seq->oos_last_id_dirty)) {
			up(&osd_seq->oos_last_id_sem);
			ldiskfs_journal_stop(jh);
			continue;
		}

		last_id = cpu_to_le64(osd_seq->oos_last_id);
		rc = osd_ldiskfs_write_record(osd_seq->oos_last_id_inode,
					      &last_id, sizeof(last_id), 0,
					      &pos, jh);
		if (rc == 0) {
			spin_lock(&osd_seq->oos_last_id_lock);
			osd_seq->oos_last_id_dirty = 0;
			spin_unlock(&osd_seq->oos_last_id_lock);
		} else {
			rc1 = rc;
		}
		up(&osd_seq->oos_last_id_sem);
		ldiskfs_journal_stop(jh);
	}

	RETURN(rc1);
}
