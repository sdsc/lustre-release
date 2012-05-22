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
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
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

/*
 * struct OBD_{ALLOC,FREE}*()
 * OBD_FAIL_CHECK
 */
#include <obd_support.h>
#include <lvfs.h>

#include "osd_internal.h"
#include "osd_oi.h"

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

struct dentry *osd_agent_lookup(struct osd_mdobj_map *omm, int index)
{
	struct osd_mdobj *om;

	cfs_down_read(&omm->omm_agent_sem);
	cfs_list_for_each_entry(om, &omm->omm_agent_list, om_list)
		if (om->om_index == index) {
			cfs_up_read(&omm->omm_agent_sem);
			return om->om_root;
		}
	cfs_up_read(&omm->omm_agent_sem);
	return NULL;
}

struct dentry *osd_agent_load(const struct osd_device *osd, int mdt_index,
			      int create)
{
	struct lvfs_run_ctxt  new;
	struct lvfs_run_ctxt  save;
	struct osd_mdobj_map *omm = osd->od_mdt_map;
	struct osd_mdobj     *om;
	struct osd_mdobj     *tmp;
	struct dentry	*d;
	char		  name[9];
	int		   rc = 0;
	ENTRY;

	LASSERT(omm != NULL && omm->omm_agent_dentry != NULL);
	d = osd_agent_lookup(omm, mdt_index);
	if (d != NULL)
		RETURN(d);

	OBD_ALLOC_PTR(om);
	if (om == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	osd_push_ctxt(osd, &new, &save);
	sprintf(name, "%x", mdt_index);
	om->om_index = mdt_index;
	CFS_INIT_LIST_HEAD(&om->om_list);
	cfs_down_write(&omm->omm_agent_sem);
	if (create == 0)
		d = simple_dir_lookup(omm->omm_agent_dentry, osd->od_mnt, name,
				      0755, 1);
	else
		d = simple_mkdir(omm->omm_agent_dentry, osd->od_mnt, name,
				 0755, 1);
	if (IS_ERR(d)) {
		cfs_up_write(&omm->omm_agent_sem);
		CERROR("%s: create agent dir failure %d\n",
			osd_name((struct osd_device *)osd), (int)PTR_ERR(d));
		GOTO(cleanup, rc = PTR_ERR(d));
	}

	om->om_root = d;
	cfs_list_for_each_entry(tmp, &omm->omm_agent_list, om_list)
		if (tmp->om_index == mdt_index) {
			cfs_up_write(&omm->omm_agent_sem);
			dput(d);
			pop_ctxt(&save, &new, NULL);
			OBD_FREE_PTR(om);
			RETURN(tmp->om_root);
		}

	cfs_list_add_tail(&om->om_list, &omm->omm_agent_list);
	cfs_up_write(&omm->omm_agent_sem);
cleanup:
	pop_ctxt(&save, &new, NULL);
	if (rc != 0) {
		OBD_FREE_PTR(om);
		RETURN(ERR_PTR(rc));
	}
	RETURN(om->om_root);
}

/*
 * directory structure on legacy MDT:
 *
 * REM_OBJ_DIR/ per mdt
 * AGENT_OBJ_DIR/ per mdt
 *
 */
static const char remote_obj_dir[] = "REM_OBJ_DIR";
static const char agent_obj_dir[] = "AGENT_OBJ_DIR";
int osd_mdt_init(struct osd_device *dev)
{
	struct lvfs_run_ctxt  new;
	struct lvfs_run_ctxt  save;
	struct dentry	*parent;
	struct osd_mdobj_map *omm;
	struct dentry	*d;
	int		   rc = 0;
	ENTRY;

	OBD_ALLOC_PTR(dev->od_mdt_map);
	if (dev->od_mdt_map == NULL)
		RETURN(-ENOMEM);

	omm = dev->od_mdt_map;

	LASSERT(dev->od_fsops);

	parent = osd_sb(dev)->s_root;
	osd_push_ctxt(dev, &new, &save);

	d = simple_mkdir(parent, dev->od_mnt, remote_obj_dir,
			 0755, 1);
	if (IS_ERR(d))
		GOTO(cleanup, rc = PTR_ERR(d));
	omm->omm_obj_dentry = d;
	cfs_init_rwsem(&omm->omm_obj_sem);
	CFS_INIT_LIST_HEAD(&omm->omm_obj_list);

	d = simple_mkdir(parent, dev->od_mnt, agent_obj_dir,
			 0755, 1);
	if (IS_ERR(d))
		GOTO(cleanup, rc = PTR_ERR(d));

	omm->omm_agent_dentry = d;
	cfs_init_rwsem(&omm->omm_agent_sem);
	CFS_INIT_LIST_HEAD(&omm->omm_agent_list);

	/* Because MDT0 might be agent on every other MDT(.. for remote dir)
	 * we will load agent directory for MDT0 now to save some time for
	 * following operation */
	parent = osd_agent_load(dev, 0, 1);
	if (IS_ERR(parent))
		GOTO(cleanup, rc = PTR_ERR(parent));

cleanup:
	pop_ctxt(&save, &new, NULL);
	if (rc) {
		if (omm->omm_obj_dentry != NULL)
			dput(omm->omm_obj_dentry);
		if (omm->omm_agent_dentry != NULL)
			dput(omm->omm_agent_dentry);
		OBD_FREE_PTR(omm);
		dev->od_mdt_map = NULL;
	}
	RETURN(rc);
}

static void osd_mdt_fini(struct osd_device *osd)
{
	struct osd_mdobj_map *omm = osd->od_mdt_map;
	struct osd_mdobj     *om;
	struct osd_mdobj     *tmp;
	cfs_list_t	    dispose_list;

	if (omm == NULL)
		return;

	CFS_INIT_LIST_HEAD(&dispose_list);
	cfs_down_write(&omm->omm_agent_sem);
	cfs_list_for_each_entry_safe(om, tmp, &omm->omm_agent_list, om_list) {
		cfs_list_move(&om->om_list, &dispose_list);
	}
	cfs_up_write(&omm->omm_agent_sem);

	cfs_list_for_each_entry_safe(om, tmp, &dispose_list, om_list) {
		dput(om->om_root);
		cfs_list_del_init(&om->om_list);
		OBD_FREE_PTR(om);
	}

	if (omm->omm_agent_dentry) {
		dput(omm->omm_agent_dentry);
		omm->omm_agent_dentry = NULL;
	}

	if (omm->omm_obj_dentry) {
		dput(omm->omm_obj_dentry);
		omm->omm_obj_dentry = NULL;
	}

	OBD_FREE_PTR(omm);
	osd->od_ost_map = NULL;
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
int osd_ost_init(struct osd_device *dev)
{
	struct lvfs_run_ctxt  new;
	struct lvfs_run_ctxt  save;
	struct dentry	     *rootd = osd_sb(dev)->s_root;
	struct dentry	     *d;
	int		      rc;
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

	dev->od_ost_map->oom_subdir_count = rc;
        rc = 0;

	CFS_INIT_LIST_HEAD(&dev->od_ost_map->oom_seq_list);
	cfs_rwlock_init(&dev->od_ost_map->oom_seq_list_lock);

        LASSERT(dev->od_fsops);
        osd_push_ctxt(dev, &new, &save);

        d = simple_mkdir(rootd, dev->od_mnt, "O", 0755, 1);
	if (IS_ERR(d))
		GOTO(cleanup, rc = PTR_ERR(d));

	dev->od_ost_map->oom_root = d;

cleanup:
        pop_ctxt(&save, &new, NULL);
        if (IS_ERR(d)) {
                OBD_FREE_PTR(dev->od_ost_map);
                RETURN(PTR_ERR(d));
        }

	dev->od_ost_map->oom_root = d;

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

	if (osd_seq->oos_root)
		dput(osd_seq->oos_root);

	OBD_FREE_PTR(osd_seq);

	return;
}

static void osd_ost_fini(struct osd_device *osd)
{
	struct osd_obj_map *map = osd->od_ost_map;
	struct osd_obj_seq *osd_seq;
	struct osd_obj_seq *tmp;
	ENTRY;

	if (map == NULL)
		RETURN_EXIT;

	cfs_write_lock(&map->oom_seq_list_lock);
	cfs_list_for_each_entry_safe(osd_seq, tmp,
				     &map->oom_seq_list,
				     oos_seq_list) {
		osd_seq_free(map, osd_seq);
	}
	cfs_write_unlock(&map->oom_seq_list_lock);
	if (map->oom_root)
		dput(map->oom_root);
	OBD_FREE_PTR(map);
	osd->od_ost_map = NULL;
	EXIT;
}

int osd_obj_map_init(struct osd_device *dev)
{
	int rc;
	ENTRY;

	/* prepare structures for OST */
	rc = osd_ost_init(dev);
	if (rc)
		RETURN(rc);

	/* prepare structures for MDS */
	rc = osd_mdt_init(dev);

        RETURN(rc);
}

struct osd_obj_seq *osd_seq_find_locked(struct osd_obj_map *map, obd_seq seq)
{
	struct osd_obj_seq *osd_seq;

	cfs_list_for_each_entry(osd_seq, &map->oom_seq_list, oos_seq_list) {
		if (osd_seq->oos_seq == seq)
			return osd_seq;
	}
	return NULL;
}

struct osd_obj_seq *osd_seq_find(struct osd_obj_map *map, obd_seq seq)
{
	struct osd_obj_seq *osd_seq;

	cfs_read_lock(&map->oom_seq_list_lock);
	osd_seq = osd_seq_find_locked(map, seq);
	cfs_read_unlock(&map->oom_seq_list_lock);
	return osd_seq;
}

struct osd_obj_seq *osd_find_or_add_seq(struct osd_obj_map *map, obd_seq seq)
{
	struct osd_obj_seq *osd_seq;
	struct osd_obj_seq *tmp;
	ENTRY;

	osd_seq = osd_seq_find(map, seq);
	if (osd_seq != NULL)
		RETURN(osd_seq);

	OBD_ALLOC_PTR(osd_seq);
	if (osd_seq == NULL)
		RETURN(ERR_PTR(-ENOMEM));

	cfs_sema_init(&osd_seq->oos_dir_init_sem, 1);
	CFS_INIT_LIST_HEAD(&osd_seq->oos_seq_list);
	osd_seq->oos_seq = seq;
	/* Init subdir count to be 32, but each seq can have
	 * different subdir count */
	osd_seq->oos_subdir_count = map->oom_subdir_count;
	cfs_write_lock(&map->oom_seq_list_lock);
	tmp = osd_seq_find_locked(map, seq);
	if (tmp != NULL) {
		cfs_write_unlock(&map->oom_seq_list_lock);
		CDEBUG(D_INFO, "seq: "LPU64" has been added\n", seq);
		OBD_FREE_PTR(osd_seq);
		RETURN(tmp);
	}
	cfs_list_add(&osd_seq->oos_seq_list, &map->oom_seq_list);
	cfs_write_unlock(&map->oom_seq_list_lock);

	return osd_seq;
}

void osd_obj_map_fini(struct osd_device *dev)
{
	osd_ost_fini(dev);
	osd_mdt_fini(dev);
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

        oh = container_of(th, struct osd_thandle, ot_super);
        LASSERT(oh->ot_handle != NULL);
        LASSERT(oh->ot_handle->h_transaction != NULL);

        inode = &info->oti_inode;
        inode->i_sb = osd_sb(osd);
	osd_id_to_inode(inode, id);

        child = &info->oti_child_dentry;
        child->d_name.hash = 0;
        child->d_name.name = name;
        child->d_name.len = strlen(name);
        child->d_parent = dir;
        child->d_inode = inode;

	ll_vfs_dq_init(dir->d_inode);
	mutex_lock(&dir->d_inode->i_mutex);
	rc = osd_ldiskfs_add_entry(oh->ot_handle, child, inode, NULL);
	mutex_unlock(&dir->d_inode->i_mutex);

	RETURN(rc);
}

/* external locking is required */
static int osd_seq_load_locked(struct osd_device *osd,
			       struct osd_obj_seq *osd_seq)
{
	struct osd_obj_map  *map = osd->od_ost_map;
	struct dentry       *seq_dir;
	int		  rc = 0;
	int		  i;
	char		 seq_name[32];
	ENTRY;

	if (osd_seq->oos_root != NULL)
		RETURN(0);

	LASSERT(map);
	LASSERT(map->oom_root);

	/* Use LPU64 for legacy OST sequences, but use LPX64i for new
	 * sequences names, so that the O/{seq}/dN/{oid} more closely
	 * follows the DFID/PFID format. This makes it easier to map from
	 * debug messages to objects in the future, and the legacy space
	 * of FID_SEQ_OST_MDT0 will be unused in the future. */
	sprintf(seq_name, (fid_seq_is_rsvd(osd_seq->oos_seq) ||
		fid_seq_is_mdt0(osd_seq->oos_seq)) ?  LPU64 : LPX64i,
		fid_seq_is_idif(osd_seq->oos_seq) ? 0 : osd_seq->oos_seq);

	seq_dir = simple_mkdir(map->oom_root, osd->od_mnt, seq_name, 0755, 1);
	if (IS_ERR(seq_dir))
		GOTO(out_err, rc = PTR_ERR(seq_dir));
	else if (seq_dir->d_inode == NULL)
		GOTO(out_put, rc = -EFAULT);

	osd_seq->oos_root = seq_dir;

	LASSERT(osd_seq->oos_dirs == NULL);
	OBD_ALLOC(osd_seq->oos_dirs,
		  sizeof(seq_dir) * osd_seq->oos_subdir_count);
	if (osd_seq->oos_dirs == NULL)
		GOTO(out_put, rc = -ENOMEM);

	for (i = 0; i < osd_seq->oos_subdir_count; i++) {
		struct dentry   *dir;
		char	     name[32];

		sprintf(name, "d%u", i);
		dir = simple_mkdir(osd_seq->oos_root, osd->od_mnt, name,
				   0700, 1);
		if (IS_ERR(dir)) {
			rc = PTR_ERR(dir);
		} else if (dir->d_inode) {
			osd_seq->oos_dirs[i] = dir;
			rc = 0;
		} else {
			LBUG();
		}
	}

	if (rc)
		osd_seq_free(map, osd_seq);
out_put:
	if (rc != 0)
		dput(seq_dir);
out_err:
	RETURN(rc);
}

static struct osd_obj_seq *osd_seq_load(struct osd_device *osd, obd_seq seq)
{
	struct osd_obj_map     *map;
	struct osd_obj_seq     *osd_seq;
	int		     rc = 0;
	ENTRY;

	map = osd->od_ost_map;
	LASSERT(map);
	LASSERT(map->oom_root);

	osd_seq = osd_find_or_add_seq(map, seq);
	if (IS_ERR(osd_seq))
		RETURN(osd_seq);

	cfs_down(&osd_seq->oos_dir_init_sem);
	rc = osd_seq_load_locked(osd, osd_seq);
	cfs_up(&osd_seq->oos_dir_init_sem);

	if (rc != 0)
		RETURN(ERR_PTR(rc));

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
	LASSERT(map->oom_root);

        fid_ostid_pack(fid, ostid);
	osd_seq = osd_seq_load(dev, ostid->oi_seq);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	dirn = ostid->oi_id & (osd_seq->oos_subdir_count - 1);
	d_seq = osd_seq->oos_dirs[dirn];
	LASSERT(d_seq);

	sprintf(name, (fid_seq_is_rsvd(fid_seq(fid)) ||
		fid_seq_is_mdt0(fid_seq(fid)) ||
		fid_seq_is_idif(fid_seq(fid))) ? LPU64 : LPX64i,
		ostid->oi_id);

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
	struct osd_obj_map    *map;
	struct osd_obj_seq    *osd_seq;
	struct dentry	 *d;
	struct ost_id	 *ostid = &info->oti_ostid;
	int		    dirn, rc = 0;
	char		   seq_name[32];
        ENTRY;

        map = osd->od_ost_map;
        LASSERT(map);

	/* map fid to seq:objid */
        fid_ostid_pack(fid, ostid);

	osd_seq = osd_seq_load(osd, ostid->oi_seq);
	if (IS_ERR(osd_seq))
		RETURN(PTR_ERR(osd_seq));

	dirn = ostid->oi_id & (osd_seq->oos_subdir_count - 1);
	d = osd_seq->oos_dirs[dirn];
        LASSERT(d);

	sprintf(seq_name, (fid_seq_is_rsvd(fid_seq(fid)) ||
		fid_seq_is_mdt0(fid_seq(fid)) ||
		fid_seq_is_idif(fid_seq(fid)))  ? LPU64 : LPX64i,
		ostid->oi_id);

	rc = osd_obj_add_entry(info, osd, d, seq_name, id, th);

        RETURN(rc);
}

int osd_obj_map_delete(struct osd_thread_info *info, struct osd_device *osd,
		       const struct lu_fid *fid, struct thandle *th)
{
	struct osd_obj_map    *map;
	struct osd_obj_seq    *osd_seq;
	struct dentry	 *d;
	struct ost_id	 *ostid = &info->oti_ostid;
	int		    dirn, rc = 0;
	char		   seq_name[32];
        ENTRY;

        map = osd->od_ost_map;
        LASSERT(map);

	/* map fid to seq:objid */
        fid_ostid_pack(fid, ostid);

	osd_seq = osd_seq_load(osd, ostid->oi_seq);
	if (IS_ERR(osd_seq))
		GOTO(cleanup, rc = PTR_ERR(osd_seq));

	dirn = ostid->oi_id & (osd_seq->oos_subdir_count - 1);
	d = osd_seq->oos_dirs[dirn];
        LASSERT(d);

	sprintf(seq_name, (fid_seq_is_rsvd(fid_seq(fid)) ||
		fid_seq_is_mdt0(fid_seq(fid)) ||
		fid_seq_is_idif(fid_seq(fid))) ? LPU64 : LPX64i,
		ostid->oi_id);

	rc = osd_obj_del_entry(info, osd, d, seq_name, th);
cleanup:
        RETURN(rc);
}

struct named_oid {
        unsigned long  oid;
        char          *name;
};

static const struct named_oid oids[] = {
	{ FLD_INDEX_OID,        "fld" },
	{ FID_SEQ_CTL_OID,      "seq_ctl" },
	{ FID_SEQ_SRV_OID,      "seq_srv" },
	{ MDD_ROOT_INDEX_OID,   "" /* "ROOT" */ },
	{ MDD_ORPHAN_OID,       "" /* "PENDING" */ },
	{ MDD_LOV_OBJ_OID,      LOV_OBJID },
	{ MDD_CAPA_KEYS_OID,    "" /* CAPA_KEYS */ },
	{ MDT_LAST_RECV_OID,    LAST_RCVD },
	{ LFSCK_BOOKMARK_OID,   "" /* "lfsck_bookmark" */ },
	{ OTABLE_IT_OID,	"" /* "otable iterator" */},
	{ OFD_LAST_RECV_OID,    LAST_RCVD },
	{ OFD_LAST_GROUP_OID,   "LAST_GROUP" },
	{ LLOG_CATALOGS_OID,    "CATALOGS" },
	{ MGS_CONFIGS_OID,      "" /* MOUNT_CONFIGS_DIR */ },
	{ OFD_HEALTH_CHECK_OID, HEALTH_CHECK },
	{ MDD_LOV_OBJ_OSEQ,     LOV_OBJSEQ },
	{ 0,                    NULL }
};

static char *oid2name(const unsigned long oid)
{
        int i = 0;

        while (oids[i].oid) {
                if (oids[i].oid == oid)
                        return oids[i].name;
                i++;
        }
        return NULL;
}

int osd_obj_spec_insert(struct osd_thread_info *info, struct osd_device *osd,
			const struct lu_fid *fid,
			const struct osd_inode_id *id,
			struct thandle *th)
{
       	struct osd_obj_map	*map = osd->od_ost_map;
	struct dentry		*root = osd_sb(osd)->s_root;
	char			*name;
	int			rc = 0;
	ENTRY;

	if (fid_is_last_obj(fid)) {
		struct osd_obj_seq	*osd_seq;
		/* on creation of LAST_ID we create O/<seq> hierarchy */
		LASSERT(map);
		osd_seq = osd_seq_load(osd, fid_seq(fid));
		if (IS_ERR(osd_seq))
			RETURN(PTR_ERR(osd_seq));
		rc = osd_obj_add_entry(info, osd, osd_seq->oos_root,
				       "LAST_ID", id, th);
	} else {
		name = oid2name(fid_oid(fid));
		if (name == NULL)
			CWARN("UNKNOWN COMPAT FID "DFID"\n", PFID(fid));
		else if (name[0])
			rc = osd_obj_add_entry(info, osd, root, name, id, th);
	}

	RETURN(rc);
}

int osd_obj_spec_lookup(struct osd_thread_info *info, struct osd_device *osd,
			const struct lu_fid *fid, struct osd_inode_id *id)
{
	struct dentry	*root;
	struct dentry *dentry;
	struct inode  *inode;
	char	      *name;
	int	       rc = -ENOENT;
	ENTRY;

	if (fid_is_last_obj(fid)) {
		struct osd_obj_seq *osd_seq;

		osd_seq = osd_seq_load(osd, fid_seq(fid));
		if (IS_ERR(osd_seq))
			RETURN(PTR_ERR(osd_seq));
		root = osd_seq->oos_root;
		name = "LAST_ID";
	} else if (fid_is_root(fid)) {
		root = osd_sb(osd)->s_root;
		name = "ROOT";
	} else {
		root = osd_sb(osd)->s_root;
		name = oid2name(fid_oid(fid));
		if (name == NULL || strlen(name) == 0)
			RETURN(-ENOENT);
	}

	dentry = ll_lookup_one_len(name, root, strlen(name));
	if (!IS_ERR(dentry)) {
		inode = dentry->d_inode;
		if (inode) {
			if (is_bad_inode(inode)) {
				rc = -EIO;
			} else {
				osd_id_gen(id, inode->i_ino,
					   inode->i_generation);
				rc = 0;
			}
		}
		/* if dentry is accessible after osd_compat_spec_insert it
		 * will still contain NULL inode, so don't keep it in cache */
		d_invalidate(dentry);
		dput(dentry);
	}

	RETURN(rc);
}
