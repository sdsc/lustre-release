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
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <lprocfs_status.h>
#include <obd_class.h>
#include <linux/seq_file.h>
#include "lod_internal.h"
#include <lustre_param.h>

#ifdef LPROCFS
static int lod_stripesize_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, LPU64"\n",
			lod->lod_desc.ld_default_stripe_size);
}

static ssize_t
lod_stripesize_seq_write(struct file *file, const char *buffer,
			 size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	__u64 val;
	int rc;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	rc = lprocfs_write_u64_helper(buffer, count, &val);
	if (rc)
		return rc;

	lod_fix_desc_stripe_size(&val);
	lod->lod_desc.ld_default_stripe_size = val;
	return count;
}
LPROC_SEQ_FOPS(lod_stripesize);

static int lod_stripeoffset_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, LPU64"\n",
			lod->lod_desc.ld_default_stripe_offset);
}

static ssize_t
lod_stripeoffset_seq_write(struct file *file, const char *buffer,
			   size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	__u64 val;
	int rc;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	rc = lprocfs_write_u64_helper(buffer, count, &val);
	if (rc)
		return rc;

	lod->lod_desc.ld_default_stripe_offset = val;
	return count;
}
LPROC_SEQ_FOPS(lod_stripeoffset);

static int lod_stripetype_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%u\n", lod->lod_desc.ld_pattern);
}

static ssize_t
lod_stripetype_seq_write(struct file *file, const char *buffer,
			 size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	int val, rc;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	lod_fix_desc_pattern(&val);
	lod->lod_desc.ld_pattern = val;
	return count;
}
LPROC_SEQ_FOPS(lod_stripetype);

static int lod_stripecount_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%d\n",
			(__s16)(lod->lod_desc.ld_default_stripe_count + 1) - 1);
}

static ssize_t
lod_stripecount_seq_write(struct file *file, const char *buffer,
			  size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	int val, rc;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	lod_fix_desc_stripe_count(&val);
	lod->lod_desc.ld_default_stripe_count = val;
	return count;
}
LPROC_SEQ_FOPS(lod_stripecount);

static int lod_numobd_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%u\n", lod->lod_desc.ld_tgt_count);
}
LPROC_SEQ_FOPS_RO(lod_numobd);

static int lod_activeobd_seq_show(struct seq_file *m, void *v)
{
	struct obd_device* dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%u\n", lod->lod_desc.ld_active_tgt_count);
}
LPROC_SEQ_FOPS_RO(lod_activeobd);

static int lod_desc_uuid_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod  = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%s\n", lod->lod_desc.ld_uuid.uuid);
}
LPROC_SEQ_FOPS_RO(lod_desc_uuid);

/* free priority (0-255): how badly user wants to choose empty osts */
static int lod_qos_priofree_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod = lu2lod_dev(dev->obd_lu_dev);

	LASSERT(lod != NULL);
	return seq_printf(m, "%d%%\n",
			(lod->lod_qos.lq_prio_free * 100 + 255) >> 8);
}

static ssize_t
lod_qos_priofree_seq_write(struct file *file, const char *buffer,
			   size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	int val, rc;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val > 100)
		return -EINVAL;
	lod->lod_qos.lq_prio_free = (val << 8) / 100;
	lod->lod_qos.lq_dirty = 1;
	lod->lod_qos.lq_reset = 1;
	return count;
}
LPROC_SEQ_FOPS(lod_qos_priofree);

static int lod_qos_thresholdrr_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%d%%\n",
			(lod->lod_qos.lq_threshold_rr * 100 + 255) >> 8);
}

static ssize_t
lod_qos_thresholdrr_seq_write(struct file *file, const char *buffer,
			      size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lod_device *lod;
	int val, rc;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val > 100 || val < 0)
		return -EINVAL;

	lod->lod_qos.lq_threshold_rr = (val << 8) / 100;
	lod->lod_qos.lq_dirty = 1;
	return count;
}
LPROC_SEQ_FOPS(lod_qos_thresholdrr);

static int lod_qos_maxage_seq_show(struct seq_file *m, void *v)
{
	struct obd_device *dev = m->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);
	return seq_printf(m, "%u Sec\n", lod->lod_desc.ld_qos_maxage);
}

static ssize_t
lod_qos_maxage_seq_write(struct file *file, const char *buffer,
			 size_t count, loff_t *off)
{
	struct obd_device *dev = ((struct seq_file *)file->private_data)->private;
	struct lustre_cfg_bufs	 bufs;
	struct lod_device	*lod;
	struct lu_device	*next;
	struct lustre_cfg	*lcfg;
	char			 str[32];
	int			 val, rc, i;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val <= 0)
		return -EINVAL;
	lod->lod_desc.ld_qos_maxage = val;

	/*
	 * propogate the value down to OSPs
	 */
	lustre_cfg_bufs_reset(&bufs, NULL);
	sprintf(str, "%smaxage=%d", PARAM_OSP, val);
	lustre_cfg_bufs_set_string(&bufs, 1, str);
	lcfg = lustre_cfg_new(LCFG_PARAM, &bufs);
	lod_getref(&lod->lod_ost_descs);
	lod_foreach_ost(lod, i) {
		next = &OST_TGT(lod,i)->ltd_ost->dd_lu_dev;
		rc = next->ld_ops->ldo_process_config(NULL, next, lcfg);
		if (rc)
			CERROR("can't set maxage on #%d: %d\n", i, rc);
	}
	lod_putref(lod, &lod->lod_ost_descs);
	lustre_cfg_free(lcfg);

	return count;
}
LPROC_SEQ_FOPS(lod_qos_maxage);

static void *lod_osts_seq_start(struct seq_file *p, loff_t *pos)
{
	struct obd_device *dev = p->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);

	lod_getref(&lod->lod_ost_descs); /* released in lod_osts_seq_stop */
	if (*pos >= lod->lod_ost_bitmap->size)
		return NULL;

	*pos = find_next_bit(lod->lod_ost_bitmap->data,
				 lod->lod_ost_bitmap->size, *pos);
	if (*pos < lod->lod_ost_bitmap->size)
		return OST_TGT(lod,*pos);
	else
		return NULL;
}

static void lod_osts_seq_stop(struct seq_file *p, void *v)
{
	struct obd_device *dev = p->private;
	struct lod_device *lod;

	LASSERT(dev != NULL);
	lod = lu2lod_dev(dev->obd_lu_dev);
	lod_putref(lod, &lod->lod_ost_descs);
}

static void *lod_osts_seq_next(struct seq_file *p, void *v, loff_t *pos)
{
	struct obd_device *dev = p->private;
	struct lod_device *lod = lu2lod_dev(dev->obd_lu_dev);

	if (*pos >= lod->lod_ost_bitmap->size - 1)
		return NULL;

	*pos = find_next_bit(lod->lod_ost_bitmap->data,
				 lod->lod_ost_bitmap->size, *pos + 1);
	if (*pos < lod->lod_ost_bitmap->size)
		return OST_TGT(lod,*pos);
	else
		return NULL;
}

static int lod_osts_seq_show(struct seq_file *p, void *v)
{
	struct obd_device   *obd = p->private;
	struct lod_ost_desc *ost_desc = v;
	struct lod_device   *lod;
	int                  idx, rc, active;
	struct dt_device    *next;
	struct obd_statfs    sfs;

	LASSERT(obd->obd_lu_dev);
	lod = lu2lod_dev(obd->obd_lu_dev);

	idx = ost_desc->ltd_index;
	next = OST_TGT(lod,idx)->ltd_ost;
	if (next == NULL)
		return -EINVAL;

	/* XXX: should be non-NULL env, but it's very expensive */
	active = 1;
	rc = dt_statfs(NULL, next, &sfs);
	if (rc == -ENOTCONN) {
		active = 0;
		rc = 0;
	} else if (rc)
		return rc;

	return seq_printf(p, "%d: %s %sACTIVE\n", idx,
			  obd_uuid2str(&ost_desc->ltd_uuid),
			  active ? "" : "IN");
}

static const struct seq_operations lod_osts_sops = {
	.start	= lod_osts_seq_start,
	.stop	= lod_osts_seq_stop,
	.next	= lod_osts_seq_next,
	.show	= lod_osts_seq_show,
};

static int lod_osts_seq_open(struct inode *inode, struct file *file)
{
	struct seq_file *seq;
	int rc;

	rc = seq_open(file, &lod_osts_sops);
	if (rc)
		return rc;

	seq = file->private_data;
	seq->private = PDE_DATA(inode);
	return 0;
}

LPROC_SEQ_FOPS_RO_TYPE(lod, uuid);

LPROC_SEQ_FOPS_RO_TYPE(lod, dt_blksize);
LPROC_SEQ_FOPS_RO_TYPE(lod, dt_kbytestotal);
LPROC_SEQ_FOPS_RO_TYPE(lod, dt_kbytesfree);
LPROC_SEQ_FOPS_RO_TYPE(lod, dt_kbytesavail);
LPROC_SEQ_FOPS_RO_TYPE(lod, dt_filestotal);
LPROC_SEQ_FOPS_RO_TYPE(lod, dt_filesfree);

static struct lprocfs_seq_vars lprocfs_lod_obd_vars[] = {
	{ "uuid",		&lod_uuid_fops		},
	{ "stripesize",		&lod_stripesize_fops	},
	{ "stripeoffset",	&lod_stripeoffset_fops	},
	{ "stripecount",	&lod_stripecount_fops	},
	{ "stripetype",		&lod_stripetype_fops	},
	{ "numobd",		&lod_numobd_fops	},
	{ "activeobd",		&lod_activeobd_fops	},
	{ "desc_uuid",		&lod_desc_uuid_fops	},
	{ "qos_prio_free",	&lod_qos_priofree_fops	},
	{ "qos_threshold_rr",	&lod_qos_thresholdrr_fops },
	{ "qos_maxage",		&lod_qos_maxage_fops	},
	{ 0 }
};

static struct lprocfs_seq_vars lprocfs_lod_osd_vars[] = {
	{ "blocksize",		&lod_dt_blksize_fops		},
	{ "kbytestotal",	&lod_dt_kbytestotal_fops	},
	{ "kbytesfree",		&lod_dt_kbytesfree_fops		},
	{ "kbytesavail",	&lod_dt_kbytesavail_fops	},
	{ "filestotal",		&lod_dt_filestotal_fops		},
	{ "filesfree",		&lod_dt_filesfree_fops		},
	{ 0 }
};

static const struct file_operations lod_proc_target_fops = {
	.owner   = THIS_MODULE,
	.open    = lod_osts_seq_open,
	.read    = seq_read,
	.llseek  = seq_lseek,
	.release = lprocfs_seq_release,
};

int lod_procfs_init(struct lod_device *lod)
{
	struct obd_device *obd = lod2obd(lod);
	cfs_proc_dir_entry_t *lov_proc_dir;
	int rc;

	obd->obd_vars = lprocfs_lod_obd_vars;
	rc = lprocfs_seq_obd_setup(obd);
	if (rc) {
		CERROR("%s: cannot setup procfs entry: %d\n",
		       obd->obd_name, rc);
		RETURN(rc);
	}

	rc = lprocfs_seq_add_vars(obd->obd_proc_entry, lprocfs_lod_osd_vars,
				  &lod->lod_dt_dev);
	if (rc) {
		CERROR("%s: cannot setup procfs entry: %d\n",
		       obd->obd_name, rc);
		GOTO(out, rc);
	}

	rc = lprocfs_seq_create(obd->obd_proc_entry, "target_obd",
				0444, &lod_proc_target_fops, obd);
	if (rc) {
		CWARN("%s: Error adding the target_obd file %d\n",
		      obd->obd_name, rc);
		GOTO(out, rc);
	}

	lod->lod_pool_proc_entry = lprocfs_seq_register("pools",
							obd->obd_proc_entry,
							NULL, NULL);
	if (IS_ERR(lod->lod_pool_proc_entry)) {
		rc = PTR_ERR(lod->lod_pool_proc_entry);
		lod->lod_pool_proc_entry = NULL;
		CWARN("%s: Failed to create pool proc file: %d\n",
		      obd->obd_name, rc);
		GOTO(out, rc);
	}

	/* for compatibility we link old procfs's LOV entries to lod ones */
	lov_proc_dir = obd->obd_proc_private;
	if (lov_proc_dir == NULL) {
		struct obd_type *type = class_search_type(LUSTRE_LOV_NAME);

		/* create "lov" entry in procfs for compatibility purposes */
		if (type == NULL) {
			lov_proc_dir = lprocfs_seq_register("lov",
							    proc_lustre_root,
							    NULL, NULL);
			if (IS_ERR(lov_proc_dir))
				CERROR("lod: can't create compat entry \"lov\""
					": %d\n",(int)PTR_ERR(lov_proc_dir));
		} else {
			lov_proc_dir = type->typ_procroot;
		}

		lod->lod_symlink = lprocfs_add_symlink(obd->obd_name,
							lov_proc_dir,
							"../lod/%s",
							obd->obd_name);
		if (lod->lod_symlink == NULL) {
			CERROR("could not register LOV symlink for "
				"/proc/fs/lustre/lod/%s.", obd->obd_name);
			lprocfs_remove(&lov_proc_dir);
		} else
			obd->obd_proc_private = lov_proc_dir;
	}
	RETURN(0);

out:
	lprocfs_obd_cleanup(obd);

	return rc;
}

void lod_procfs_fini(struct lod_device *lod)
{
	struct obd_device *obd = lod2obd(lod);

	if (lod->lod_symlink != NULL)
		lprocfs_remove(&lod->lod_symlink);

	if (lod->lod_pool_proc_entry != NULL) {
		lprocfs_remove(&lod->lod_pool_proc_entry);
		lod->lod_pool_proc_entry = NULL;
	}

	if (obd->obd_proc_private != NULL)
		lprocfs_remove((struct proc_dir_entry **)&obd->obd_proc_private);

	lprocfs_obd_cleanup(obd);
}

#endif /* LPROCFS */

