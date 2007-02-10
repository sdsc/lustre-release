/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2002, 2003 Cluster File Systems, Inc.
 *
 *   This file is part of the Lustre file system, http://www.lustre.org
 *   Lustre is a trademark of Cluster File Systems, Inc.
 *
 *   You may have signed or agreed to another license before downloading
 *   this software.  If so, you are bound by the terms and conditions
 *   of that agreement, and the following does not apply to you.  See the
 *   LICENSE file included with this distribution for more information.
 *
 *   If you did not agree to a different license, then this copy of Lustre
 *   is open source software; you can redistribute it and/or modify it
 *   under the terms of version 2 of the GNU General Public License as
 *   published by the Free Software Foundation.
 *
 *   In either case, Lustre is distributed in the hope that it will be
 *   useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 *   of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   license text for more details.
 *
 */
#define DEBUG_SUBSYSTEM S_CLASS

#include <linux/version.h>
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
#include <asm/statfs.h>
#endif
#include <obd_class.h>
#include <lprocfs_status.h>
#include <linux/seq_file.h>
#include "osc_internal.h"

#ifdef LPROCFS
static int osc_rd_active(char *page, char **start, off_t off,
                         int count, int *eof, void *data)
{
        struct obd_device *dev = data;
        int rc;

        LPROCFS_CLIMP_CHECK(dev);
        rc = snprintf(page, count, "%d\n", !dev->u.cli.cl_import->imp_deactive);
        LPROCFS_CLIMP_EXIT(dev);
        return rc;
}

static int osc_wr_active(struct file *file, const char *buffer,
                         unsigned long count, void *data)
{
        struct obd_device *dev = data;
        int val, rc;
        
        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;
        if (val < 0 || val > 1)
                return -ERANGE;

        LPROCFS_CLIMP_CHECK(dev);
        /* opposite senses */
        if (dev->u.cli.cl_import->imp_deactive == val) 
                rc = ptlrpc_set_import_active(dev->u.cli.cl_import, val);
        else
                CDEBUG(D_CONFIG, "activate %d: ignoring repeat request\n", val);
        
        LPROCFS_CLIMP_EXIT(dev);
        return count;
}


static int osc_rd_max_pages_per_rpc(char *page, char **start, off_t off,
                                    int count, int *eof, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int rc;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        rc = snprintf(page, count, "%d\n", cli->cl_max_pages_per_rpc);
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        return rc;
}

static int osc_wr_max_pages_per_rpc(struct file *file, const char *buffer,
                                    unsigned long count, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        struct obd_connect_data *ocd;
        int val, rc;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        LPROCFS_CLIMP_CHECK(dev);
        ocd = &cli->cl_import->imp_connect_data;
        if (val < 1 || val > ocd->ocd_brw_size >> CFS_PAGE_SHIFT) {
                LPROCFS_CLIMP_EXIT(dev);
                return -ERANGE;
        }
        client_obd_list_lock(&cli->cl_loi_list_lock);
        cli->cl_max_pages_per_rpc = val;
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        LPROCFS_CLIMP_EXIT(dev);
        return count;
}

static int osc_rd_max_rpcs_in_flight(char *page, char **start, off_t off,
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

static int osc_wr_max_rpcs_in_flight(struct file *file, const char *buffer,
                                     unsigned long count, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        struct ptlrpc_request_pool *pool;
        int val, rc;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;
        if (val < 1 || val > OSC_MAX_RIF_MAX)
                return -ERANGE;

        LPROCFS_CLIMP_CHECK(dev);
        pool = cli->cl_import->imp_rq_pool;
        if (pool && val > cli->cl_max_rpcs_in_flight)
                pool->prp_populate(pool, val-cli->cl_max_rpcs_in_flight);

        client_obd_list_lock(&cli->cl_loi_list_lock);
        cli->cl_max_rpcs_in_flight = val;
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        LPROCFS_CLIMP_EXIT(dev);
        return count;
}

static int osc_rd_max_dirty_mb(char *page, char **start, off_t off, int count,
                               int *eof, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        long val;
        int mult;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        val = cli->cl_dirty_max;
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        mult = 1 << 20;
        return lprocfs_read_frac_helper(page, count, val, mult);
}

static int osc_wr_max_dirty_mb(struct file *file, const char *buffer,
                               unsigned long count, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int pages_number, mult, rc;

        mult = 1 << (20 - CFS_PAGE_SHIFT);
        rc = lprocfs_write_frac_helper(buffer, count, &pages_number, mult);
        if (rc)
                return rc;

        if (pages_number < 0 || pages_number > OSC_MAX_DIRTY_MB_MAX << (20 - CFS_PAGE_SHIFT) ||
            pages_number > num_physpages / 4) /* 1/4 of RAM */
                return -ERANGE;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        cli->cl_dirty_max = (obd_count)(pages_number << CFS_PAGE_SHIFT);
        osc_wake_cache_waiters(cli);
        client_obd_list_unlock(&cli->cl_loi_list_lock);

        return count;
}

static int osc_rd_cur_dirty_bytes(char *page, char **start, off_t off,
                                  int count, int *eof, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int rc;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        rc = snprintf(page, count, "%lu\n", cli->cl_dirty);
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        return rc;
}

static int osc_rd_cur_grant_bytes(char *page, char **start, off_t off,
                                  int count, int *eof, void *data)
{
        struct obd_device *dev = data;
        struct client_obd *cli = &dev->u.cli;
        int rc;

        client_obd_list_lock(&cli->cl_loi_list_lock);
        rc = snprintf(page, count, "%lu\n", cli->cl_avail_grant);
        client_obd_list_unlock(&cli->cl_loi_list_lock);
        return rc;
}

static int osc_rd_create_count(char *page, char **start, off_t off, int count,
                               int *eof, void *data)
{
        struct obd_device *obd = data;

        if (obd == NULL)
                return 0;

        return snprintf(page, count, "%d\n",
                        obd->u.cli.cl_oscc.oscc_grow_count);
}

static int osc_wr_create_count(struct file *file, const char *buffer,
                               unsigned long count, void *data)
{
        struct obd_device *obd = data;
        int val, rc;

        if (obd == NULL)
                return 0;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        if (val < 0)
                return -ERANGE;
        if (val > OST_MAX_PRECREATE)
                return -ERANGE;

        obd->u.cli.cl_oscc.oscc_grow_count = val;

        return count;
}

static int osc_rd_prealloc_next_id(char *page, char **start, off_t off,
                                   int count, int *eof, void *data)
{
        struct obd_device *obd = data;

        if (obd == NULL)
                return 0;

        return snprintf(page, count, LPU64"\n",
                        obd->u.cli.cl_oscc.oscc_next_id);
}

static int osc_rd_prealloc_last_id(char *page, char **start, off_t off,
                                   int count, int *eof, void *data)
{
        struct obd_device *obd = data;

        if (obd == NULL)
                return 0;

        return snprintf(page, count, LPU64"\n",
                        obd->u.cli.cl_oscc.oscc_last_id);
}

static int osc_rd_checksum(char *page, char **start, off_t off, int count,
                           int *eof, void *data)
{
        struct obd_device *obd = data;

        if (obd == NULL)
                return 0;

        return snprintf(page, count, "%d\n",
                        obd->u.cli.cl_checksum ? 1 : 0);
}

static int osc_wr_checksum(struct file *file, const char *buffer,
                           unsigned long count, void *data)
{
        struct obd_device *obd = data;
        int val, rc;

        if (obd == NULL)
                return 0;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        obd->u.cli.cl_checksum = (val ? 1 : 0);

        return count;
}

static struct lprocfs_vars lprocfs_obd_vars[] = {
        { "uuid",            lprocfs_rd_uuid,        0, 0 },
        { "ping",            0, lprocfs_wr_ping,        0 },
        { "connect_flags",   lprocfs_rd_connect_flags, 0, 0 },
        { "blocksize",       lprocfs_rd_blksize,     0, 0 },
        { "kbytestotal",     lprocfs_rd_kbytestotal, 0, 0 },
        { "kbytesfree",      lprocfs_rd_kbytesfree,  0, 0 },
        { "kbytesavail",     lprocfs_rd_kbytesavail, 0, 0 },
        { "filestotal",      lprocfs_rd_filestotal,  0, 0 },
        { "filesfree",       lprocfs_rd_filesfree,   0, 0 },
        //{ "filegroups",      lprocfs_rd_filegroups,  0, 0 },
        { "ost_server_uuid", lprocfs_rd_server_uuid, 0, 0 },
        { "ost_conn_uuid",   lprocfs_rd_conn_uuid, 0, 0 },
        { "active",          osc_rd_active, 
                             osc_wr_active, 0 },
        { "max_pages_per_rpc", osc_rd_max_pages_per_rpc,
                               osc_wr_max_pages_per_rpc, 0 },
        { "max_rpcs_in_flight", osc_rd_max_rpcs_in_flight,
                                osc_wr_max_rpcs_in_flight, 0 },
        { "max_dirty_mb",    osc_rd_max_dirty_mb, osc_wr_max_dirty_mb, 0 },
        { "cur_dirty_bytes", osc_rd_cur_dirty_bytes, 0, 0 },
        { "cur_grant_bytes", osc_rd_cur_grant_bytes, 0, 0 },
        { "create_count",    osc_rd_create_count, osc_wr_create_count, 0 },
        { "prealloc_next_id", osc_rd_prealloc_next_id, 0, 0 },
        { "prealloc_last_id", osc_rd_prealloc_last_id, 0, 0 },
        { "checksums",       osc_rd_checksum, osc_wr_checksum, 0 },
        { 0 }
};

static struct lprocfs_vars lprocfs_module_vars[] = {
        { "num_refs",        lprocfs_rd_numrefs,     0, 0 },
        { 0 }
};

#define pct(a,b) (b ? a * 100 / b : 0)

static int osc_rpc_stats_seq_show(struct seq_file *seq, void *v)
{
        struct timeval now;
        struct obd_device *dev = seq->private;
        struct client_obd *cli = &dev->u.cli;
        unsigned long read_tot = 0, write_tot = 0, read_cum, write_cum;
        int i;

        do_gettimeofday(&now);

        client_obd_list_lock(&cli->cl_loi_list_lock);

        seq_printf(seq, "snapshot_time:         %lu.%lu (secs.usecs)\n",
                   now.tv_sec, now.tv_usec);
        seq_printf(seq, "read RPCs in flight:  %d\n",
                   cli->cl_r_in_flight);
        seq_printf(seq, "write RPCs in flight: %d\n",
                   cli->cl_w_in_flight);
        seq_printf(seq, "pending write pages:  %d\n",
                   cli->cl_pending_w_pages);
        seq_printf(seq, "pending read pages:   %d\n",
                   cli->cl_pending_r_pages);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "pages per rpc         rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        read_tot = lprocfs_oh_sum(&cli->cl_read_page_hist);
        write_tot = lprocfs_oh_sum(&cli->cl_write_page_hist);

        read_cum = 0;
        write_cum = 0;
        for (i = 0; i < OBD_HIST_MAX; i++) {
                unsigned long r = cli->cl_read_page_hist.oh_buckets[i];
                unsigned long w = cli->cl_write_page_hist.oh_buckets[i];
                read_cum += r;
                write_cum += w;
                seq_printf(seq, "%d:\t\t%10lu %3lu %3lu   | %10lu %3lu %3lu\n",
                                 1 << i, r, pct(r, read_tot),
                                 pct(read_cum, read_tot), w,
                                 pct(w, write_tot),
                                 pct(write_cum, write_tot));
                if (read_cum == read_tot && write_cum == write_tot)
                        break;
        }

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "rpcs in flight        rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        read_tot = lprocfs_oh_sum(&cli->cl_read_rpc_hist);
        write_tot = lprocfs_oh_sum(&cli->cl_write_rpc_hist);

        read_cum = 0;
        write_cum = 0;
        for (i = 0; i < OBD_HIST_MAX; i++) {
                unsigned long r = cli->cl_read_rpc_hist.oh_buckets[i];
                unsigned long w = cli->cl_write_rpc_hist.oh_buckets[i];
                read_cum += r;
                write_cum += w;
                seq_printf(seq, "%d:\t\t%10lu %3lu %3lu   | %10lu %3lu %3lu\n",
                                 i, r, pct(r, read_tot),
                                 pct(read_cum, read_tot), w,
                                 pct(w, write_tot),
                                 pct(write_cum, write_tot));
                if (read_cum == read_tot && write_cum == write_tot)
                        break;
        }

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "offset                rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        read_tot = lprocfs_oh_sum(&cli->cl_read_offset_hist);
        write_tot = lprocfs_oh_sum(&cli->cl_write_offset_hist);

        read_cum = 0;
        write_cum = 0;
        for (i = 0; i < OBD_HIST_MAX; i++) {
                unsigned long r = cli->cl_read_offset_hist.oh_buckets[i];
                unsigned long w = cli->cl_write_offset_hist.oh_buckets[i];
                read_cum += r;
                write_cum += w;
                seq_printf(seq, "%d:\t\t%10lu %3lu %3lu   | %10lu %3lu %3lu\n",
                           (i == 0) ? 0 : 1 << (i - 1),
                           r, pct(r, read_tot), pct(read_cum, read_tot), 
                           w, pct(w, write_tot), pct(write_cum, write_tot));
                if (read_cum == read_tot && write_cum == write_tot)
                        break;
        }

        client_obd_list_unlock(&cli->cl_loi_list_lock);

        return 0;
}
#undef pct

static ssize_t osc_rpc_stats_seq_write(struct file *file, const char *buf,
                                       size_t len, loff_t *off)
{
        struct seq_file *seq = file->private_data;
        struct obd_device *dev = seq->private;
        struct client_obd *cli = &dev->u.cli;

        lprocfs_oh_clear(&cli->cl_read_rpc_hist);
        lprocfs_oh_clear(&cli->cl_write_rpc_hist);
        lprocfs_oh_clear(&cli->cl_read_page_hist);
        lprocfs_oh_clear(&cli->cl_write_page_hist);
        lprocfs_oh_clear(&cli->cl_read_offset_hist);
        lprocfs_oh_clear(&cli->cl_write_offset_hist);

        return len;
}

LPROC_SEQ_FOPS(osc_rpc_stats);

int lproc_osc_attach_seqstat(struct obd_device *dev)
{
        return lprocfs_obd_seq_create(dev, "rpc_stats", 0444,
                                      &osc_rpc_stats_fops, dev);
}

LPROCFS_INIT_VARS(osc, lprocfs_module_vars, lprocfs_obd_vars)
#endif /* LPROCFS */
