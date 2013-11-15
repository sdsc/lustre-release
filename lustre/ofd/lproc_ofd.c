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
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/ofd/lproc_ofd.c
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>
#include <lprocfs_status.h>
#include <linux/seq_file.h>

#include "ofd_internal.h"

#ifdef LPROCFS

static int ofd_seqs_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%u\n", ofd->ofd_seq_count);
}
LPROC_SEQ_FOPS_RO(ofd_seqs);

static int ofd_tot_dirty_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd;

	LASSERT(obd != NULL);
	ofd = ofd_dev(obd->obd_lu_dev);
	return seq_printf(m, LPU64"\n", ofd->ofd_tot_dirty);
}
LPROC_SEQ_FOPS_RO(ofd_tot_dirty);

static int ofd_tot_granted_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd;

	LASSERT(obd != NULL);
	ofd = ofd_dev(obd->obd_lu_dev);
	return seq_printf(m, LPU64"\n", ofd->ofd_tot_granted);
}
LPROC_SEQ_FOPS_RO(ofd_tot_granted);

static int ofd_tot_pending_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd;

	LASSERT(obd != NULL);
	ofd = ofd_dev(obd->obd_lu_dev);
	return seq_printf(m, LPU64"\n", ofd->ofd_tot_pending);
}
LPROC_SEQ_FOPS_RO(ofd_tot_pending);

static int ofd_grant_precreate_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;

	LASSERT(obd != NULL);
	return seq_printf(m, "%ld\n",
			obd->obd_self_export->exp_filter_data.fed_grant);
}
LPROC_SEQ_FOPS_RO(ofd_grant_precreate);

static int ofd_grant_ratio_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd;

	LASSERT(obd != NULL);
	ofd = ofd_dev(obd->obd_lu_dev);
	return seq_printf(m, "%d%%\n",
			(int) ofd_grant_reserved(ofd, 100));
}

static ssize_t
ofd_grant_ratio_seq_write(struct file *file, const char *buffer,
			  size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val;
	int			 rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val > 100 || val < 0)
		return -EINVAL;

	if (val == 0)
		CWARN("%s: disabling grant error margin\n", obd->obd_name);
	if (val > 50)
		CWARN("%s: setting grant error margin >50%%, be warned that "
		      "a huge part of the free space is now reserved for "
		      "grants\n", obd->obd_name);

	spin_lock(&ofd->ofd_grant_lock);
	ofd->ofd_grant_ratio = ofd_grant_ratio_conv(val);
	spin_unlock(&ofd->ofd_grant_lock);
	return count;
}
LPROC_SEQ_FOPS(ofd_grant_ratio);

static int ofd_precreate_batch_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd;

	LASSERT(obd != NULL);
	ofd = ofd_dev(obd->obd_lu_dev);
	return seq_printf(m, "%d\n", ofd->ofd_precreate_batch);
}

static ssize_t
ofd_precreate_batch_seq_write(struct file *file, const char *buffer,
				size_t count, loff_t *off)
{
	struct obd_device *obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device *ofd = ofd_dev(obd->obd_lu_dev);
	int val;
	int rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val < 1)
		return -EINVAL;

	spin_lock(&ofd->ofd_batch_lock);
	ofd->ofd_precreate_batch = val;
	spin_unlock(&ofd->ofd_batch_lock);
	return count;
}
LPROC_SEQ_FOPS(ofd_precreate_batch);

static int ofd_last_id_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct ofd_device	*ofd;
	struct ofd_seq		*oseq = NULL;
	int			retval = 0, rc;

	if (obd == NULL)
		return 0;

	ofd = ofd_dev(obd->obd_lu_dev);

	read_lock(&ofd->ofd_seq_list_lock);
	cfs_list_for_each_entry(oseq, &ofd->ofd_seq_list, os_list) {
		__u64 seq;

		seq = ostid_seq(&oseq->os_oi) == 0 ?
		      fid_idif_seq(ostid_id(&oseq->os_oi),
				   ofd->ofd_lut.lut_lsd.lsd_osd_index) :
		      ostid_seq(&oseq->os_oi);
		rc = seq_printf(m, DOSTID"\n", seq, ostid_id(&oseq->os_oi));
		if (rc < 0) {
			retval = rc;
			break;
		}
		retval += rc;
	}
	read_unlock(&ofd->ofd_seq_list_lock);
	return retval;
}
LPROC_SEQ_FOPS_RO(ofd_last_id);

int ofd_fmd_max_num_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%u\n", ofd->ofd_fmd_max_num);
}

ssize_t
ofd_fmd_max_num_seq_write(struct file *file, const char *buffer,
			size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val;
	int			 rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val > 65536 || val < 1)
		return -EINVAL;

	ofd->ofd_fmd_max_num = val;
	return count;
}
LPROC_SEQ_FOPS(ofd_fmd_max_num);

int ofd_fmd_max_age_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%ld\n", ofd->ofd_fmd_max_age / HZ);
}

ssize_t
ofd_fmd_max_age_seq_write(struct file *file, const char *buffer,
			  size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val;
	int			 rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val > 65536 || val < 1)
		return -EINVAL;

	ofd->ofd_fmd_max_age = val * HZ;
	return count;
}
LPROC_SEQ_FOPS(ofd_fmd_max_age);

static int ofd_capa_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;

	return seq_printf(m, "capability on: %s\n",
			  obd->u.filter.fo_fl_oss_capa ? "oss" : "");
}

static ssize_t
ofd_capa_seq_write(struct file *file, const char *buffer, size_t count,
		   loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	int			 val, rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val & ~0x1) {
		CERROR("invalid capability mode, only 0/1 are accepted.\n"
		       " 1: enable oss fid capability\n"
		       " 0: disable oss fid capability\n");
		return -EINVAL;
	}

	obd->u.filter.fo_fl_oss_capa = val;
	LCONSOLE_INFO("OSS %s %s fid capability.\n", obd->obd_name,
		      val ? "enabled" : "disabled");
	return count;
}
LPROC_SEQ_FOPS(ofd_capa);

static int ofd_capa_count_seq_show(struct seq_file *m, void *data)
{
	return seq_printf(m, "%d %d\n",
			capa_count[CAPA_SITE_CLIENT],
			capa_count[CAPA_SITE_SERVER]);
}
LPROC_SEQ_FOPS_RO(ofd_capa_count);

int ofd_degraded_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%u\n", ofd->ofd_raid_degraded);
}

ssize_t
ofd_degraded_seq_write(struct file *file, const char *buffer,
			size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val, rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	spin_lock(&ofd->ofd_flags_lock);
	ofd->ofd_raid_degraded = !!val;
	spin_unlock(&ofd->ofd_flags_lock);
	return count;
}
LPROC_SEQ_FOPS(ofd_degraded);

int ofd_fstype_seq_show(struct seq_file *m, void *data)
{
	struct obd_device *obd = m->private;
	struct ofd_device *ofd = ofd_dev(obd->obd_lu_dev);
	struct lu_device  *d;

	LASSERT(ofd->ofd_osd);
	d = &ofd->ofd_osd->dd_lu_dev;
	LASSERT(d->ld_type);
	return seq_printf(m, "%s\n", d->ld_type->ldt_name);
}
LPROC_SEQ_FOPS_RO(ofd_fstype);

int ofd_syncjournal_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%u\n", ofd->ofd_syncjournal);
}

ssize_t ofd_syncjournal_seq_write(struct file *file, const char *buffer,
				  size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val;
	int			 rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val < 0)
		return -EINVAL;

	spin_lock(&ofd->ofd_flags_lock);
	ofd->ofd_syncjournal = !!val;
	ofd_slc_set(ofd);
	spin_unlock(&ofd->ofd_flags_lock);

	return count;
}
LPROC_SEQ_FOPS(ofd_syncjournal);

static char *sync_on_cancel_states[] = {"never",
					"blocking",
					"always" };

int ofd_sync_lock_cancel_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct lu_target	*tgt = obd->u.obt.obt_lut;

	return seq_printf(m, "%s\n",
			sync_on_cancel_states[tgt->lut_sync_lock_cancel]);
}

ssize_t
ofd_sync_lock_cancel_seq_write(struct file *file, const char *buffer,
				size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct lu_target	*tgt = obd->u.obt.obt_lut;
	int			 val = -1;
	int			 i;

	for (i = 0 ; i < NUM_SYNC_ON_CANCEL_STATES; i++) {
		if (memcmp(buffer, sync_on_cancel_states[i],
			   strlen(sync_on_cancel_states[i])) == 0) {
			val = i;
			break;
		}
	}
	if (val == -1) {
		int rc;

		rc = lprocfs_write_helper(buffer, count, &val);
		if (rc)
			return rc;
	}

	if (val < 0 || val > 2)
		return -EINVAL;

	spin_lock(&tgt->lut_flags_lock);
	tgt->lut_sync_lock_cancel = val;
	spin_unlock(&tgt->lut_flags_lock);
	return count;
}
LPROC_SEQ_FOPS(ofd_sync_lock_cancel);

int ofd_grant_compat_disable_seq_show(struct seq_file *m, void *data)
{
	struct obd_device	*obd = m->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);

	return seq_printf(m, "%u\n", ofd->ofd_grant_compat_disable);
}

ssize_t
ofd_grant_compat_disable_seq_write(struct file *file, const char *buffer,
					size_t count, loff_t *off)
{
	struct obd_device	*obd = ((struct seq_file *)file->private_data)->private;
	struct ofd_device	*ofd = ofd_dev(obd->obd_lu_dev);
	int			 val;
	int			 rc;

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val < 0)
		return -EINVAL;

	spin_lock(&ofd->ofd_flags_lock);
	ofd->ofd_grant_compat_disable = !!val;
	spin_unlock(&ofd->ofd_flags_lock);

	return count;
}
LPROC_SEQ_FOPS(ofd_grant_compat_disable);

LPROC_SEQ_FOPS_RO_TYPE(ofd, uuid);
LPROC_SEQ_FOPS_RO_TYPE(ofd, blksize);
LPROC_SEQ_FOPS_RO_TYPE(ofd, kbytestotal);
LPROC_SEQ_FOPS_RO_TYPE(ofd, kbytesfree);
LPROC_SEQ_FOPS_RO_TYPE(ofd, kbytesavail);
LPROC_SEQ_FOPS_RO_TYPE(ofd, filestotal);
LPROC_SEQ_FOPS_RO_TYPE(ofd, filesfree);

LPROC_SEQ_FOPS_RO_TYPE(ofd, recovery_status);
LPROC_SEQ_FOPS_RW_TYPE(ofd, recovery_time_soft);
LPROC_SEQ_FOPS_RW_TYPE(ofd, recovery_time_hard);
LPROC_SEQ_FOPS_WO_TYPE(ofd, evict_client);
LPROC_SEQ_FOPS_RO_TYPE(ofd, num_exports);
LPROC_SEQ_FOPS_RO_TYPE(ofd, target_instance);
LPROC_SEQ_FOPS_RW_TYPE(ofd, ir_factor);
LPROC_SEQ_FOPS_RW_TYPE(ofd, job_interval);

static struct lprocfs_seq_vars lprocfs_ofd_obd_vars[] = {
	{ "uuid",			&ofd_uuid_fops			},
	{ "blocksize",			&ofd_blksize_fops		},
	{ "kbytestotal",		&ofd_kbytestotal_fops		},
	{ "kbytesfree",			&ofd_kbytesfree_fops		},
	{ "kbytesavail",		&ofd_kbytesavail_fops		},
	{ "filestotal",			&ofd_filestotal_fops		},
	{ "filesfree",			&ofd_filesfree_fops		},
	{ "seqs_allocated",		&ofd_seqs_fops			},
	{ "fstype",			&ofd_fstype_fops		},
	{ "last_id",			&ofd_last_id_fops		},
	{ "tot_dirty",			&ofd_tot_dirty_fops		},
	{ "tot_pending",		&ofd_tot_pending_fops		},
	{ "tot_granted",		&ofd_tot_granted_fops		},
	{ "grant_precreate",		&ofd_grant_precreate_fops	},
	{ "grant_ratio",		&ofd_grant_ratio_fops		},
	{ "precreate_batch",		&ofd_precreate_batch_fops	},
	{ "recovery_status",		&ofd_recovery_status_fops	},
	{ "recovery_time_soft",		&ofd_recovery_time_soft_fops	},
	{ "recovery_time_hard",		&ofd_recovery_time_hard_fops	},
	{ "evict_client",		&ofd_evict_client_fops		},
	{ "num_exports",		&ofd_num_exports_fops		},
	{ "degraded",			&ofd_degraded_fops		},
	{ "sync_journal",		&ofd_syncjournal_fops		},
	{ "sync_on_lock_cancel",	&ofd_sync_lock_cancel_fops	},
	{ "instance",			&ofd_target_instance_fops	},
	{ "ir_factor",			&ofd_ir_factor_fops		},
	{ "grant_compat_disable",	&ofd_grant_compat_disable_fops	},
	{ "client_cache_count",		&ofd_fmd_max_num_fops		},
	{ "client_cache_seconds",	&ofd_fmd_max_age_fops		},
	{ "capa",			&ofd_capa_fops			},
	{ "capa_count",			&ofd_capa_count_fops		},
	{ "job_cleanup_interval",	&ofd_job_interval_fops		},
	{ 0 }
};

void lprocfs_ofd_init_vars(struct obd_device *obd)
{
	obd->obd_vars = lprocfs_ofd_obd_vars;
}

void ofd_stats_counter_init(struct lprocfs_stats *stats)
{
	LASSERT(stats && stats->ls_num >= LPROC_OFD_STATS_LAST);

	lprocfs_counter_init(stats, LPROC_OFD_STATS_READ,
			     LPROCFS_CNTR_AVGMINMAX, "read_bytes", "bytes");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_WRITE,
			     LPROCFS_CNTR_AVGMINMAX, "write_bytes", "bytes");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_GETATTR,
			     0, "getattr", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_SETATTR,
			     0, "setattr", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_PUNCH,
			     0, "punch", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_SYNC,
			     0, "sync", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_DESTROY,
			     0, "destroy", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_CREATE,
			     0, "create", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_STATFS,
			     0, "statfs", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_GET_INFO,
			     0, "get_info", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_SET_INFO,
			     0, "set_info", "reqs");
	lprocfs_counter_init(stats, LPROC_OFD_STATS_QUOTACTL,
			     0, "quotactl", "reqs");
}

#endif /* LPROCFS */
