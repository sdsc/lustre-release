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
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/fid/lproc_fid.c
 *
 * Lustre Sequence Manager
 *
 * Author: Yury Umanets <umka@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_FID

#include <libcfs/libcfs.h>
#include <linux/module.h>
#include <obd.h>
#include <obd_class.h>
#include <obd_support.h>
#include <lustre_fid.h>
#include <lprocfs_status.h>
#include "fid_internal.h"
#include <md_object.h>

#ifdef LPROCFS
/*
 * Note: this function is only used for testing, it is no safe for production
 * use.
 */
static int
seq_proc_write_common(struct file *file, const char *buffer,
                      unsigned long count, void *data,
                      struct lu_seq_range *range)
{
	struct lu_seq_range tmp;
	int rc;
	ENTRY;

	LASSERT(range != NULL);

	if (count == 5 && strcmp(buffer, "clear") == 0) {
		memset(range, 0, sizeof(*range));
		RETURN(0);
	}

        rc = sscanf(buffer, "[%llx - %llx]\n",
                    (long long unsigned *)&tmp.lsr_start,
                    (long long unsigned *)&tmp.lsr_end);
	if (rc != 2 || !range_is_sane(&tmp) || range_is_zero(&tmp))
		RETURN(-EINVAL);
	*range = tmp;
        RETURN(0);
}

static int
seq_proc_read_common(char *page, char **start, off_t off,
                     int count, int *eof, void *data,
                     struct lu_seq_range *range)
{
	int rc;
	ENTRY;

        *eof = 1;
	rc = snprintf(page, count, "["LPX64" - "LPX64"]:%x:%s\n",
			PRANGE(range));
	RETURN(rc);
}

#ifdef HAVE_SERVER_SUPPORT
/*
 * Server side procfs stuff.
 */
static int
seq_server_proc_write_space(struct file *file, const char *buffer,
                            unsigned long count, void *data)
{
        struct lu_server_seq *seq = (struct lu_server_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lss_mutex);
	rc = seq_proc_write_common(file, buffer, count,
                                   data, &seq->lss_space);
	if (rc == 0) {
		CDEBUG(D_INFO, "%s: Space: "DRANGE"\n",
                       seq->lss_name, PRANGE(&seq->lss_space));
	}

	mutex_unlock(&seq->lss_mutex);

        RETURN(count);
}

static int
seq_server_proc_read_space(char *page, char **start, off_t off,
                           int count, int *eof, void *data)
{
        struct lu_server_seq *seq = (struct lu_server_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lss_mutex);
	rc = seq_proc_read_common(page, start, off, count, eof,
                                  data, &seq->lss_space);
	mutex_unlock(&seq->lss_mutex);

	RETURN(rc);
}

static int
seq_server_proc_read_server(char *page, char **start, off_t off,
                            int count, int *eof, void *data)
{
        struct lu_server_seq *seq = (struct lu_server_seq *)data;
        struct client_obd *cli;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	*eof = 1;
	if (seq->lss_cli) {
                if (seq->lss_cli->lcs_exp != NULL) {
                        cli = &seq->lss_cli->lcs_exp->exp_obd->u.cli;
                        rc = snprintf(page, count, "%s\n",
                                      cli->cl_target_uuid.uuid);
                } else {
                        rc = snprintf(page, count, "%s\n",
                                      seq->lss_cli->lcs_srv->lss_name);
                }
	} else {
		rc = snprintf(page, count, "<none>\n");
	}

	RETURN(rc);
}

static int
seq_server_proc_write_width(struct file *file, const char *buffer,
                            unsigned long count, void *data)
{
        struct lu_server_seq *seq = (struct lu_server_seq *)data;
	int rc, val;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lss_mutex);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc != 0) {
		CERROR("%s: invalid width.\n", seq->lss_name);
		GOTO(out_unlock, rc);
	}

	seq->lss_width = val;

	CDEBUG(D_INFO, "%s: Width: "LPU64"\n",
	       seq->lss_name, seq->lss_width);
out_unlock:
	mutex_unlock(&seq->lss_mutex);

        RETURN(count);
}

static int
seq_server_proc_read_width(char *page, char **start, off_t off,
                           int count, int *eof, void *data)
{
        struct lu_server_seq *seq = (struct lu_server_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lss_mutex);
        rc = snprintf(page, count, LPU64"\n", seq->lss_width);
	mutex_unlock(&seq->lss_mutex);

	RETURN(rc);
}

struct lprocfs_vars seq_server_proc_list[] = {
	{ "space",
	  seq_server_proc_read_space, seq_server_proc_write_space, NULL },
	{ "width",
	  seq_server_proc_read_width, seq_server_proc_write_width, NULL },
	{ "server",
	  seq_server_proc_read_server, NULL, NULL },
	{ NULL }
};

struct fld_seq_param {
	struct lu_env		fsp_env;
	struct dt_it		*fsp_it;
	struct lu_server_fld	*fsp_fld;
	struct lu_server_seq	*fsp_seq;
	unsigned int		fsp_stop:1;
};

/*
 * XXX: below is a copy of the functions in lustre/fld/lproc_fld.c.
 * we want to avoid this duplication either by exporting the
 * functions or merging fid and fld into a single module.
 */
static void *fldb_seq_start(struct seq_file *p, loff_t *pos)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld    *fld;
	struct dt_object        *obj;
	const struct dt_it_ops  *iops;

	if (param == NULL || param->fsp_stop)
		return NULL;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	iops->load(&param->fsp_env, param->fsp_it, *pos);

	*pos = be64_to_cpu(*(__u64 *)iops->key(&param->fsp_env, param->fsp_it));
	return param;
}

static void fldb_seq_stop(struct seq_file *p, void *v)
{
	struct fld_seq_param    *param = p->private;
	const struct dt_it_ops	*iops;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;

	if (param == NULL)
		return;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	iops->put(&param->fsp_env, param->fsp_it);
}

static void *fldb_seq_next(struct seq_file *p, void *v, loff_t *pos)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;
	int			rc;

	if (param == NULL || param->fsp_stop)
		return NULL;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	rc = iops->next(&param->fsp_env, param->fsp_it);
	if (rc > 0) {
		param->fsp_stop = 1;
		return NULL;
	}

	*pos = be64_to_cpu(*(__u64 *)iops->key(&param->fsp_env, param->fsp_it));
	return param;
}

static int fldb_seq_show(struct seq_file *p, void *v)
{
	struct fld_seq_param    *param = p->private;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;
	struct lu_seq_range	 fld_rec;
	int			rc;

	if (param == NULL || param->fsp_stop)
		return 0;

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	rc = iops->rec(&param->fsp_env, param->fsp_it,
		       (struct dt_rec *)&fld_rec, 0);
	if (rc != 0) {
		CERROR("%s: read record error: rc = %d\n",
		       fld->lsf_name, rc);
	} else if (fld_rec.lsr_start != 0) {
		range_be_to_cpu(&fld_rec, &fld_rec);
		rc = seq_printf(p, DRANGE"\n", PRANGE(&fld_rec));
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
	struct proc_dir_entry	*dp = PDE(inode);
	struct seq_file		*seq;
	struct lu_server_seq    *ss = (struct lu_server_seq *)dp->data;
	struct lu_server_fld    *fld;
	struct dt_object	*obj;
	const struct dt_it_ops  *iops;
	struct fld_seq_param    *param = NULL;
	int			env_init = 0;
	int			rc;

	fld = ss->lss_site->ss_server_fld;
	LASSERT(fld != NULL);

	LPROCFS_ENTRY_CHECK(dp);
	rc = seq_open(file, &fldb_sops);
	if (rc)
		GOTO(out, rc);

	obj = fld->lsf_obj;
	if (obj == NULL) {
		seq = file->private_data;
		seq->private = NULL;
		return 0;
	}

	OBD_ALLOC_PTR(param);
	if (param == NULL)
		GOTO(out, rc = -ENOMEM);

	rc = lu_env_init(&param->fsp_env, LCT_MD_THREAD);
	if (rc != 0)
		GOTO(out, rc);

	env_init = 1;
	iops = &obj->do_index_ops->dio_it;
	param->fsp_it = iops->init(&param->fsp_env, obj, 0);
	if (IS_ERR(param->fsp_it))
		GOTO(out, rc = PTR_ERR(param->fsp_it));

	param->fsp_fld = fld;
	param->fsp_seq = ss;
	param->fsp_stop = 0;

	seq = file->private_data;
	seq->private = param;
out:
	if (rc != 0) {
		if (env_init == 1)
			lu_env_fini(&param->fsp_env);
		if (param != NULL)
			OBD_FREE_PTR(param);
	}
	return rc;
}

static int fldb_seq_release(struct inode *inode, struct file *file)
{
	struct seq_file		*seq = file->private_data;
	struct fld_seq_param	*param;
	struct lu_server_fld	*fld;
	struct dt_object	*obj;
	const struct dt_it_ops	*iops;

	param = seq->private;
	if (param == NULL) {
		lprocfs_seq_release(inode, file);
		return 0;
	}

	fld = param->fsp_fld;
	obj = fld->lsf_obj;
	LASSERT(obj != NULL);
	iops = &obj->do_index_ops->dio_it;

	LASSERT(iops != NULL);
	LASSERT(obj != NULL);
	LASSERT(param->fsp_it != NULL);
	iops->fini(&param->fsp_env, param->fsp_it);
	lu_env_fini(&param->fsp_env);
	OBD_FREE_PTR(param);
	lprocfs_seq_release(inode, file);

	return 0;
}

static ssize_t fldb_seq_write(struct file *file, const char *buf,
			      size_t len, loff_t *off)
{
	struct seq_file		*seq = file->private_data;
	struct fld_seq_param	*param;
	struct lu_seq_range	 range;
	int			 rc = 0;
	char			*buffer, *_buffer;
	ENTRY;

	param = seq->private;
	if (param == NULL)
		RETURN(-EINVAL);

	OBD_ALLOC(buffer, len + 1);
	if (buffer == NULL)
		RETURN(-ENOMEM);
	memcpy(buffer, buf, len);
	buffer[len] = 0;
	_buffer = buffer;

	/*
	 * format - [0x0000000200000007-0x0000000200000008):0:mdt
	 */
	if (*buffer != '[')
		GOTO(out, rc = -EINVAL);
	buffer++;

	range.lsr_start = simple_strtoull(buffer, &buffer, 0);
	if (*buffer != '-')
		GOTO(out, rc = -EINVAL);
	buffer++;

	range.lsr_end = simple_strtoull(buffer, &buffer, 0);
	if (*buffer != ')')
		GOTO(out, rc = -EINVAL);
	buffer++;
	if (*buffer != ':')
		GOTO(out, rc = -EINVAL);
	buffer++;

	range.lsr_index = simple_strtoul(buffer, &buffer, 0);
	if (*buffer != ':')
		GOTO(out, rc = -EINVAL);
	buffer++;

	if (strncmp(buffer, "mdt", 3) == 0)
		range.lsr_flags = LU_SEQ_RANGE_MDT;
	else if (strncmp(buffer, "ost", 3) == 0)
		range.lsr_flags = LU_SEQ_RANGE_OST;
	else
		GOTO(out, rc = -EINVAL);

	rc = seq_server_alloc_spec(param->fsp_seq->lss_site->ss_control_seq,
				   &range, &param->fsp_env);

out:
	OBD_FREE(_buffer, len + 1);
	RETURN(rc < 0 ? rc : len);
}

const struct file_operations seq_fld_proc_seq_fops = {
	.owner	 = THIS_MODULE,
	.open	 = fldb_seq_open,
	.read	 = seq_read,
	.write	 = fldb_seq_write,
	.release = fldb_seq_release,
};

#endif /* HAVE_SERVER_SUPPORT */

/* Client side procfs stuff */
static int
seq_client_proc_write_space(struct file *file, const char *buffer,
                            unsigned long count, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lcs_mutex);
	rc = seq_proc_write_common(file, buffer, count,
                                   data, &seq->lcs_space);

	if (rc == 0) {
		CDEBUG(D_INFO, "%s: Space: "DRANGE"\n",
                       seq->lcs_name, PRANGE(&seq->lcs_space));
	}

	mutex_unlock(&seq->lcs_mutex);

        RETURN(count);
}

static int
seq_client_proc_read_space(char *page, char **start, off_t off,
                           int count, int *eof, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lcs_mutex);
	rc = seq_proc_read_common(page, start, off, count, eof,
                                  data, &seq->lcs_space);
	mutex_unlock(&seq->lcs_mutex);

	RETURN(rc);
}

static int
seq_client_proc_write_width(struct file *file, const char *buffer,
                            unsigned long count, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
	__u64  max;
	int rc, val;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lcs_mutex);

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc) {
		mutex_unlock(&seq->lcs_mutex);
                RETURN(rc);
        }

	if (seq->lcs_type == LUSTRE_SEQ_DATA)
		max = LUSTRE_DATA_SEQ_MAX_WIDTH;
	else
		max = LUSTRE_METADATA_SEQ_MAX_WIDTH;

	if (val <= max && val > 0) {
		seq->lcs_width = val;

		if (rc == 0) {
			CDEBUG(D_INFO, "%s: Sequence size: "LPU64"\n",
			       seq->lcs_name, seq->lcs_width);
		}
	}

	mutex_unlock(&seq->lcs_mutex);

        RETURN(count);
}

static int
seq_client_proc_read_width(char *page, char **start, off_t off,
                           int count, int *eof, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lcs_mutex);
        rc = snprintf(page, count, LPU64"\n", seq->lcs_width);
	mutex_unlock(&seq->lcs_mutex);

	RETURN(rc);
}

static int
seq_client_proc_read_fid(char *page, char **start, off_t off,
                         int count, int *eof, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

	mutex_lock(&seq->lcs_mutex);
        rc = snprintf(page, count, DFID"\n", PFID(&seq->lcs_fid));
	mutex_unlock(&seq->lcs_mutex);

	RETURN(rc);
}

static int
seq_client_proc_read_server(char *page, char **start, off_t off,
                            int count, int *eof, void *data)
{
        struct lu_client_seq *seq = (struct lu_client_seq *)data;
        struct client_obd *cli;
	int rc;
	ENTRY;

        LASSERT(seq != NULL);

        if (seq->lcs_exp != NULL) {
                cli = &seq->lcs_exp->exp_obd->u.cli;
                rc = snprintf(page, count, "%s\n", cli->cl_target_uuid.uuid);
        } else {
                rc = snprintf(page, count, "%s\n", seq->lcs_srv->lss_name);
        }
	RETURN(rc);
}

struct lprocfs_vars seq_client_proc_list[] = {
	{ "space",    seq_client_proc_read_space, seq_client_proc_write_space, NULL },
	{ "width",    seq_client_proc_read_width, seq_client_proc_write_width, NULL },
	{ "server",   seq_client_proc_read_server, NULL, NULL },
	{ "fid",      seq_client_proc_read_fid, NULL, NULL },
	{ NULL }};
#endif
