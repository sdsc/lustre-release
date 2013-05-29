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
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdd/mdd_lproc.c
 *
 * Lustre Metadata Server (mdd) routines
 *
 * Author: Wang Di <wangdi@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_MDS

#include <obd.h>
#include <obd_class.h>
#include <obd_support.h>
#include <lprocfs_status.h>
#include <libcfs/libcfs_string.h>
#include "mdd_internal.h"

int mdd_procfs_init(struct mdd_device *mdd, const char *name)
{
        struct lprocfs_static_vars lvars;
        struct obd_type     *type;
        int                  rc;
        ENTRY;

	/* at the moment there is no linkage between lu_type
	 * and obd_type, so we lookup obd_type this way */
	type = class_search_type(LUSTRE_MDD_NAME);

        LASSERT(name != NULL);
        LASSERT(type != NULL);

        /* Find the type procroot and add the proc entry for this device */
        lprocfs_mdd_init_vars(&lvars);
        mdd->mdd_proc_entry = lprocfs_register(name, type->typ_procroot,
                                               lvars.obd_vars, mdd);
        if (IS_ERR(mdd->mdd_proc_entry)) {
                rc = PTR_ERR(mdd->mdd_proc_entry);
                CERROR("Error %d setting up lprocfs for %s\n",
                       rc, name);
                mdd->mdd_proc_entry = NULL;
                GOTO(out, rc);
        }

	rc = 0;

        EXIT;
out:
        if (rc)
               mdd_procfs_fini(mdd);
        return rc;
}

int mdd_procfs_fini(struct mdd_device *mdd)
{
        if (mdd->mdd_proc_entry) {
                 lprocfs_remove(&mdd->mdd_proc_entry);
                 mdd->mdd_proc_entry = NULL;
        }
        RETURN(0);
}

static ssize_t mdd_atime_diff_seq_write(struct file *file, const char *buffer,
					size_t count, loff_t *off)
{
	struct mdd_device *mdd =
		((struct seq_file *)file->private_data)->private;
        char kernbuf[20], *end;
        unsigned long diff = 0;

        if (count > (sizeof(kernbuf) - 1))
                return -EINVAL;

	if (copy_from_user(kernbuf, buffer, count))
                return -EFAULT;

        kernbuf[count] = '\0';

        diff = simple_strtoul(kernbuf, &end, 0);
        if (kernbuf == end)
                return -EINVAL;

        mdd->mdd_atime_diff = diff;
        return count;
}

static int mdd_atime_diff_seq_show(struct seq_file *m, void *data)
{
	struct mdd_device *mdd = m->private;

	return seq_printf(m, "%lu\n", mdd->mdd_atime_diff);
}
LPROC_SEQ_FOPS(mdd_atime_diff);


/**** changelogs ****/
static int mdd_changelog_mask_seq_show(struct seq_file *m, void *data)
{
	struct mdd_device *mdd = m->private;
        int i = 0, rc = 0;

        while (i < CL_LAST) {
                if (mdd->mdd_cl.mc_mask & (1 << i))
			rc = seq_printf(m, "%s ", changelog_type2str(i));
                i++;
        }
        return rc;
}

static ssize_t
mdd_changelog_mask_seq_write(struct file *file, const char *buffer,
			     size_t count, loff_t *off)
{
	struct mdd_device *mdd =
		((struct seq_file *)file->private_data)->private;
	char *kernbuf;
	int rc;
	ENTRY;

	if (count >= PAGE_CACHE_SIZE)
		RETURN(-EINVAL);
	OBD_ALLOC(kernbuf, PAGE_CACHE_SIZE);
	if (kernbuf == NULL)
		RETURN(-ENOMEM);
	if (copy_from_user(kernbuf, buffer, count))
		GOTO(out, rc = -EFAULT);
	kernbuf[count] = 0;

	rc = cfs_str2mask(kernbuf, changelog_type2str, &mdd->mdd_cl.mc_mask,
			  CHANGELOG_MINMASK, CHANGELOG_ALLMASK);
	if (rc == 0)
		rc = count;
out:
	OBD_FREE(kernbuf, PAGE_CACHE_SIZE);
	return rc;
}
LPROC_SEQ_FOPS(mdd_changelog_mask);

static int lprocfs_changelog_users_cb(const struct lu_env *env,
				      struct llog_handle *llh,
				      struct llog_rec_hdr *hdr, void *data)
{
	struct llog_changelog_user_rec *rec;
	struct seq_file *m = data;

        LASSERT(llh->lgh_hdr->llh_flags & LLOG_F_IS_PLAIN);

        rec = (struct llog_changelog_user_rec *)hdr;

	seq_printf(m, CHANGELOG_USER_PREFIX"%-3d "LPU64"\n",
		   rec->cur_id, rec->cur_endrec);

        return 0;
}

static int mdd_changelog_users_seq_show(struct seq_file *m, void *data)
{
	struct lu_env		 env;
	struct mdd_device	*mdd = m->private;
	struct llog_ctxt	*ctxt;
	__u64			 cur;
	int			 rc;

        ctxt = llog_get_context(mdd2obd_dev(mdd),
				LLOG_CHANGELOG_USER_ORIG_CTXT);
        if (ctxt == NULL)
                return -ENXIO;
        LASSERT(ctxt->loc_handle->lgh_hdr->llh_flags & LLOG_F_IS_CAT);

	rc = lu_env_init(&env, LCT_LOCAL);
	if (rc) {
		llog_ctxt_put(ctxt);
		return rc;
	}

	spin_lock(&mdd->mdd_cl.mc_lock);
	cur = mdd->mdd_cl.mc_index;
	spin_unlock(&mdd->mdd_cl.mc_lock);

	seq_printf(m, "current index: "LPU64"\n%-5s %s\n",
		   cur, "ID", "index");


	llog_cat_process(&env, ctxt->loc_handle, lprocfs_changelog_users_cb,
			 m, 0, 0);

	lu_env_fini(&env);
	llog_ctxt_put(ctxt);
	return 0;
}
LPROC_SEQ_FOPS_RO(mdd_changelog_users);

static int mdd_sync_perm_seq_show(struct seq_file *m, void *data)
{
	struct mdd_device *mdd = m->private;

	LASSERT(mdd != NULL);
	return seq_printf(m, "%d\n", mdd->mdd_sync_permission);
}

static ssize_t mdd_sync_perm_seq_write(struct file *file, const char *buffer,
				       size_t count, loff_t *off)
{
	struct mdd_device *mdd =
		((struct seq_file *)file->private_data)->private;
        int val, rc;

        LASSERT(mdd != NULL);
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        mdd->mdd_sync_permission = !!val;
        return count;
}
LPROC_SEQ_FOPS(mdd_sync_perm);

static int mdd_lfsck_speed_limit_seq_show(struct seq_file *m, void *data)
{
	struct mdd_device *mdd = m->private;

	LASSERT(mdd != NULL);
	return lfsck_get_speed(mdd->mdd_bottom, m);
}

static ssize_t
mdd_lfsck_speed_limit_seq_write(struct file *file, const char *buffer,
				size_t count, loff_t *off)
{
	struct mdd_device *mdd =
		((struct seq_file *)file->private_data)->private;
	__u32 val;
	int rc;

	LASSERT(mdd != NULL);
	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc != 0)
		return rc;

	rc = lfsck_set_speed(mdd->mdd_bottom, val);
	return rc != 0 ? rc : count;
}
LPROC_SEQ_FOPS(mdd_lfsck_speed_limit);

static int mdd_lfsck_namespace_seq_show(struct seq_file *m, void *data)
{
	struct mdd_device *mdd = m->private;

	LASSERT(mdd != NULL);
	return lfsck_dump(mdd->mdd_bottom, m, LT_NAMESPACE);
}
LPROC_SEQ_FOPS_RO(mdd_lfsck_namespace);

static struct lprocfs_vars lprocfs_mdd_obd_vars[] = {
	{ "atime_diff",		&mdd_atime_diff_fops,		0 },
	{ "changelog_mask",	&mdd_changelog_mask_fops,	0 },
	{ "changelog_users",	&mdd_changelog_users_fops,	0, 0},
	{ "sync_permission",	&mdd_sync_perm_fops,		0 },
	{ "lfsck_speed_limit",	&mdd_lfsck_speed_limit_fops,	0 },
	{ "lfsck_namespace",	&mdd_lfsck_namespace_fops,	0, 0 },
	{ 0 }
};

LPROC_SEQ_FOPS_RO_TYPE(mdd, numrefs);
static struct lprocfs_vars lprocfs_mdd_module_vars[] = {
	{ "num_refs",		&mdd_numrefs_fops,		0, 0 },
	{ 0 }
};

void lprocfs_mdd_init_vars(struct lprocfs_static_vars *lvars)
{
        lvars->module_vars  = lprocfs_mdd_module_vars;
        lvars->obd_vars     = lprocfs_mdd_obd_vars;
}

