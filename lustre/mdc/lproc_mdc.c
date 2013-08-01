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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <linux/version.h>
#include <linux/vfs.h>
#include <obd_class.h>
#include <lprocfs_status.h>

#ifdef LPROCFS

static int mdc_rd_max_rpcs_in_flight(char *page, char **start, off_t off,
                                     int count, int *eof, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int rc;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        rc = snprintf(page, count, "%u\n", cli->cl_max_rpcs_in_flight);
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        return rc;
}

static int mdc_wr_max_rpcs_in_flight(struct file *file, const char *buffer,
                                     unsigned long count, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int val, rc;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        if (val < 1 || val > MDC_MAX_RIF_MAX)
                return -ERANGE;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        cli->cl_max_rpcs_in_flight = val;
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        return count;
}

LPROC_SEQ_FOPS_RO_TYPE(mdc, uuid);
LPROC_SEQ_FOPS_RO_TYPE(mdc, blksize);
LPROC_SEQ_FOPS_RO_TYPE(mdc, kbytestotal);
LPROC_SEQ_FOPS_RO_TYPE(mdc, kbytesfree);
LPROC_SEQ_FOPS_RO_TYPE(mdc, kbytesavail);
LPROC_SEQ_FOPS_RO_TYPE(mdc, filestotal);
LPROC_SEQ_FOPS_RO_TYPE(mdc, filesfree);

static struct lprocfs_vars lprocfs_mdc_obd_vars[] = {
	{ "uuid",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_uuid_fops,		0,	0 },
        { "ping",            0, lprocfs_wr_ping,     0, 0, 0222 },
        { "connect_flags",   lprocfs_rd_connect_flags, 0, 0 },
	{ "blocksize",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_blksize_fops,		0,	0 },
	{ "kbytestotal",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_kbytestotal_fops,	0,	0 },
	{ "kbytesfree",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_kbytesfree_fops,		0,	0 },
	{ "kbytesavail",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_kbytesavail_fops,	0,	0 },
	{ "filestotal",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_filestotal_fops,		0,	0 },
	{ "filesfree",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_filesfree_fops,		0,	0 },
        { "mds_server_uuid", lprocfs_rd_server_uuid, 0, 0 },
        { "mds_conn_uuid",   lprocfs_rd_conn_uuid,   0, 0 },
        /*
         * FIXME: below proc entry is provided, but not in used, instead
         * sbi->sb_md_brw_size is used, the per obd variable should be used
         * when CMD is enabled, and dir pages are managed in MDC layer.
         * Remember to enable proc write function.
         */
        { "max_pages_per_rpc",  lprocfs_obd_rd_max_pages_per_rpc,
                                /* lprocfs_obd_wr_max_pages_per_rpc */0, 0 },
        { "max_rpcs_in_flight", mdc_rd_max_rpcs_in_flight,
                                mdc_wr_max_rpcs_in_flight, 0 },
        { "timeouts",        lprocfs_rd_timeouts,    0, 0 },
        { "import",          lprocfs_rd_import,      lprocfs_wr_import, 0 },
        { "state",           lprocfs_rd_state,       0, 0 },
        { "pinger_recov",    lprocfs_rd_pinger_recov,
                             lprocfs_wr_pinger_recov, 0, 0 },
        { 0 }
};

LPROC_SEQ_FOPS_RO_TYPE(mdc, numrefs);

static struct lprocfs_vars lprocfs_mdc_module_vars[] = {
	{ "num_refs",
#ifndef HAVE_ONLY_PROCFS_SEQ
	   NULL, NULL,
#endif
	   &mdc_numrefs_fops,    0,      0 },
        { 0 }
};

void lprocfs_mdc_init_vars(struct lprocfs_static_vars *lvars)
{
    lvars->module_vars  = lprocfs_mdc_module_vars;
    lvars->obd_vars     = lprocfs_mdc_obd_vars;
}
#endif /* LPROCFS */
