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
 * lustre/fld/lproc_fld.c
 *
 * FLD (FIDs Location Database)
 *
 * Author: Yury Umanets <umka@clusterfs.com>
 *	Di Wang <di.wang@whamcloud.com>
 */

#define DEBUG_SUBSYSTEM S_FLD

#ifdef __KERNEL__
# include <libcfs/libcfs.h>
# include <linux/module.h>
#else /* __KERNEL__ */
# include <liblustre.h>
#endif

#include <obd.h>
#include <obd_class.h>
#include <dt_object.h>
#include <md_object.h>
#include <obd_support.h>
#include <lustre_req_layout.h>
#include <lustre_fld.h>
#include <lustre_fid.h>
#include "fld_internal.h"

#ifdef LPROCFS
static int
fld_proc_read_targets(char *page, char **start, off_t off,
                      int count, int *eof, void *data)
{
        struct lu_client_fld *fld = (struct lu_client_fld *)data;
        struct lu_fld_target *target;
	int total = 0, rc;
	ENTRY;

        LASSERT(fld != NULL);

        cfs_spin_lock(&fld->lcf_lock);
        cfs_list_for_each_entry(target,
                                &fld->lcf_targets, ft_chain)
        {
                rc = snprintf(page, count, "%s\n",
                              fld_target_name(target));
                page += rc;
                count -= rc;
                total += rc;
                if (count == 0)
                        break;
        }
        cfs_spin_unlock(&fld->lcf_lock);
	RETURN(total);
}

static int
fld_proc_read_hash(char *page, char **start, off_t off,
                   int count, int *eof, void *data)
{
        struct lu_client_fld *fld = (struct lu_client_fld *)data;
	int rc;
	ENTRY;

        LASSERT(fld != NULL);

        cfs_spin_lock(&fld->lcf_lock);
        rc = snprintf(page, count, "%s\n",
                      fld->lcf_hash->fh_name);
        cfs_spin_unlock(&fld->lcf_lock);

	RETURN(rc);
}

static int
fld_proc_write_hash(struct file *file, const char *buffer,
                    unsigned long count, void *data)
{
        struct lu_client_fld *fld = (struct lu_client_fld *)data;
        struct lu_fld_hash *hash = NULL;
        int i;
	ENTRY;

        LASSERT(fld != NULL);

        for (i = 0; fld_hash[i].fh_name != NULL; i++) {
                if (count != strlen(fld_hash[i].fh_name))
                        continue;

                if (!strncmp(fld_hash[i].fh_name, buffer, count)) {
                        hash = &fld_hash[i];
                        break;
                }
        }

        if (hash != NULL) {
                cfs_spin_lock(&fld->lcf_lock);
                fld->lcf_hash = hash;
                cfs_spin_unlock(&fld->lcf_lock);

                CDEBUG(D_INFO, "%s: Changed hash to \"%s\"\n",
                       fld->lcf_name, hash->fh_name);
        }

        RETURN(count);
}

static int
fld_proc_write_cache_flush(struct file *file, const char *buffer,
                           unsigned long count, void *data)
{
        struct lu_client_fld *fld = (struct lu_client_fld *)data;
	ENTRY;

        LASSERT(fld != NULL);

        fld_cache_flush(fld->lcf_cache);

        CDEBUG(D_INFO, "%s: Lookup cache is flushed\n", fld->lcf_name);

        RETURN(count);
}

struct fld_seq_param {
	struct lu_env  fsp_env;
	struct dt_it  *fsp_it;
};

static void fldb_seq_stop(struct seq_file *p, void *v)
{
	struct lu_server_fld	*fld = p->private;
	struct dt_object        *obj = fld->lsf_obj;
	const struct dt_it_ops	*iops;
	struct fld_seq_param	*fsp = (struct fld_seq_param *) v;

	if (obj == NULL || fsp == NULL || IS_ERR(fsp))
		return;

	iops = &obj->do_index_ops->dio_it;
	iops->put(&fsp->fsp_env, fsp->fsp_it);
	iops->fini(&fsp->fsp_env, fsp->fsp_it);
	lu_env_fini(&fsp->fsp_env);
	OBD_FREE_PTR(fsp);
}

static void *fldb_seq_start(struct seq_file *p, loff_t *pos)
{
	struct lu_server_fld	*fld = p->private;
	struct dt_object        *obj = fld->lsf_obj;
	const struct dt_it_ops	*iops;
	struct fld_seq_param	*fsp;
	__u64                    key = cpu_to_be64(*pos);
	int                      rc;

	if (obj == NULL)
		return NULL;

	OBD_ALLOC_PTR(fsp);
	if (fsp == NULL)
		return ERR_PTR(-ENOMEM);

	lu_env_init(&fsp->fsp_env, LCT_MD_THREAD);

	iops = &obj->do_index_ops->dio_it;
	fsp->fsp_it = iops->init(&fsp->fsp_env, obj, 0, NULL);
	rc = iops->get(&fsp->fsp_env, fsp->fsp_it, (struct dt_key *) &key);
	if (rc > 0)
		rc = 0;
	else if (rc == 0)
		rc = iops->next(&fsp->fsp_env, fsp->fsp_it);

	if (rc != 0) {
		fldb_seq_stop(p, fsp);
		return rc < 0 ? ERR_PTR(rc) : NULL;
	}

	*pos = be64_to_cpu(*(__u64 *) iops->key(&fsp->fsp_env, fsp->fsp_it));

	return fsp;
}

static void *fldb_seq_next(struct seq_file *p, void *v, loff_t *pos)
{
	struct lu_server_fld	*fld = p->private;
	struct dt_object        *obj = fld->lsf_obj;
	const struct dt_it_ops	*iops;
	struct fld_seq_param	*fsp = (struct fld_seq_param *) v;
	int                      rc;

	if (obj == NULL)
		return NULL;

	iops = &obj->do_index_ops->dio_it;
	rc = iops->next(&fsp->fsp_env, fsp->fsp_it);
	if (rc != 0) {
		fldb_seq_stop(p, fsp);
		return rc < 0 ? ERR_PTR(rc) : NULL;
	}

	*pos = be64_to_cpu(*(__u64 *) iops->key(&fsp->fsp_env, fsp->fsp_it));

	return fsp;
}

static int fldb_seq_show(struct seq_file *p, void *v)
{
	struct lu_server_fld	*fld = p->private;
	struct dt_object        *obj = fld->lsf_obj;
	const struct dt_it_ops	*iops;
	struct fld_seq_param	*fsp = (struct fld_seq_param *) v;
	struct lu_seq_range	 lsr;
	int			 rc;

	if (obj == NULL)
		return 0;

	iops = &obj->do_index_ops->dio_it;
	rc = iops->rec(&fsp->fsp_env, fsp->fsp_it, (struct dt_rec *) &lsr, 0);
	if (rc != 0) {
		CERROR("%s: read record error: rc %d\n",
		       fld->lsf_name, rc);
	} else if (lsr.lsr_start != 0) {
		range_be_to_cpu(&lsr, &lsr);
		rc = seq_printf(p, DRANGE"\n", PRANGE(&lsr));
	}

	return rc;
}

struct seq_operations fldb_sops = {
	.start = fldb_seq_start,
	.stop = fldb_seq_stop,
	.next = fldb_seq_next,
	.show = fldb_seq_show,
};

static int fldb_seq_open(struct inode *inode, struct file *file)
{
	struct proc_dir_entry *dp = PDE(inode);
	struct seq_file *seq;
	int rc;

	LPROCFS_ENTRY_AND_CHECK(dp);
	rc = seq_open(file, &fldb_sops);
	if (rc) {
		LPROCFS_EXIT();
		return rc;
	}

	seq = file->private_data;
	seq->private = dp->data;
	return 0;
}

struct lprocfs_vars fld_server_proc_list[] = {
	{ NULL }};

struct lprocfs_vars fld_client_proc_list[] = {
	{ "targets",     fld_proc_read_targets, NULL, NULL },
	{ "hash",        fld_proc_read_hash, fld_proc_write_hash, NULL },
	{ "cache_flush", NULL, fld_proc_write_cache_flush, NULL },
	{ NULL }};

struct file_operations fld_proc_seq_fops = {
	.owner   = THIS_MODULE,
	.open    = fldb_seq_open,
	.read    = seq_read,
	.llseek  = seq_lseek,
	.release = lprocfs_seq_release,
};

#endif
