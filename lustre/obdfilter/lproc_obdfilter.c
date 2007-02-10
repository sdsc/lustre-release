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
#include <lprocfs_status.h>
#include <obd.h>
#include <linux/seq_file.h>
#include <linux/version.h>

#include "filter_internal.h"

#ifdef LPROCFS
static int lprocfs_filter_rd_groups(char *page, char **start, off_t off,
                                    int count, int *eof, void *data)
{
        *eof = 1;
        return snprintf(page, count, "%u\n", FILTER_GROUPS);
}

static int lprocfs_filter_rd_tot_dirty(char *page, char **start, off_t off,
                                       int count, int *eof, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;

        LASSERT(obd != NULL);
        *eof = 1;
        return snprintf(page, count, LPU64"\n", obd->u.filter.fo_tot_dirty);
}

static int lprocfs_filter_rd_tot_granted(char *page, char **start, off_t off,
                                         int count, int *eof, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;

        LASSERT(obd != NULL);
        *eof = 1;
        return snprintf(page, count, LPU64"\n", obd->u.filter.fo_tot_granted);
}

static int lprocfs_filter_rd_tot_pending(char *page, char **start, off_t off,
                                         int count, int *eof, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;

        LASSERT(obd != NULL);
        *eof = 1;
        return snprintf(page, count, LPU64"\n", obd->u.filter.fo_tot_pending);
}

static int lprocfs_filter_rd_mntdev(char *page, char **start, off_t off,
                                    int count, int *eof, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;

        LASSERT(obd != NULL);
        LASSERT(obd->u.filter.fo_vfsmnt->mnt_devname);
        *eof = 1;
        return snprintf(page, count, "%s\n",
                        obd->u.filter.fo_vfsmnt->mnt_devname);
}

static int lprocfs_filter_rd_last_id(char *page, char **start, off_t off,
                                     int count, int *eof, void *data)
{
        struct obd_device *obd = data;

        if (obd == NULL)
                return 0;

        return snprintf(page, count, LPU64"\n",
                        filter_last_id(&obd->u.filter, 0));
}

int lprocfs_filter_rd_readcache(char *page, char **start, off_t off, int count,
                                int *eof, void *data)
{
        struct obd_device *obd = data;
        int rc;

        rc = snprintf(page, count, LPU64"\n",
                      obd->u.filter.fo_readcache_max_filesize);
        return rc;
}

int lprocfs_filter_wr_readcache(struct file *file, const char *buffer,
                                unsigned long count, void *data)
{
        struct obd_device *obd = data;
        __u64 val;
        int rc;

        rc = lprocfs_write_u64_helper(buffer, count, &val);
        if (rc)
                return rc;

        obd->u.filter.fo_readcache_max_filesize = val;
        return count;
}

int lprocfs_filter_rd_fmd_max_num(char *page, char **start, off_t off,
                                  int count, int *eof, void *data)
{
        struct obd_device *obd = data;
        int rc;

        rc = snprintf(page, count, "%u\n", obd->u.filter.fo_fmd_max_num);
        return rc;
}

int lprocfs_filter_wr_fmd_max_num(struct file *file, const char *buffer,
                                  unsigned long count, void *data)
{
        struct obd_device *obd = data;
        int val;
        int rc;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        if (val > 65536 || val < 1)
                return -EINVAL;

        obd->u.filter.fo_fmd_max_num = val;
        return count;
}

int lprocfs_filter_rd_fmd_max_age(char *page, char **start, off_t off,
                                  int count, int *eof, void *data)
{
        struct obd_device *obd = data;
        int rc;

        rc = snprintf(page, count, "%u\n", obd->u.filter.fo_fmd_max_age / HZ);
        return rc;
}

int lprocfs_filter_wr_fmd_max_age(struct file *file, const char *buffer,
                                  unsigned long count, void *data)
{
        struct obd_device *obd = data;
        int val;
        int rc;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        if (val > 65536 || val < 1)
                return -EINVAL;

        obd->u.filter.fo_fmd_max_age = val * HZ;
        return count;
}

static struct lprocfs_vars lprocfs_obd_vars[] = {
        { "uuid",         lprocfs_rd_uuid,          0, 0 },
        { "blocksize",    lprocfs_rd_blksize,       0, 0 },
        { "kbytestotal",  lprocfs_rd_kbytestotal,   0, 0 },
        { "kbytesfree",   lprocfs_rd_kbytesfree,    0, 0 },
        { "kbytesavail",  lprocfs_rd_kbytesavail,   0, 0 },
        { "filestotal",   lprocfs_rd_filestotal,    0, 0 },
        { "filesfree",    lprocfs_rd_filesfree,     0, 0 },
        { "filegroups",   lprocfs_filter_rd_groups, 0, 0 },
        { "fstype",       lprocfs_rd_fstype,        0, 0 },
        { "mntdev",       lprocfs_filter_rd_mntdev, 0, 0 },
        { "last_id",      lprocfs_filter_rd_last_id,0, 0 },
        { "tot_dirty",    lprocfs_filter_rd_tot_dirty,   0, 0 },
        { "tot_pending",  lprocfs_filter_rd_tot_pending, 0, 0 },
        { "tot_granted",  lprocfs_filter_rd_tot_granted, 0, 0 },
        { "recovery_status", lprocfs_obd_rd_recovery_status, 0, 0 },
        { "evict_client", 0, lprocfs_wr_evict_client, 0 },
        { "num_exports",  lprocfs_rd_num_exports,   0, 0 },
        { "readcache_max_filesize",
                          lprocfs_filter_rd_readcache,
                          lprocfs_filter_wr_readcache, 0 },
#ifdef HAVE_QUOTA_SUPPORT
        { "quota_bunit_sz", lprocfs_rd_bunit, lprocfs_wr_bunit, 0},
        { "quota_btune_sz", lprocfs_rd_btune, lprocfs_wr_btune, 0},
        { "quota_iunit_sz", lprocfs_rd_iunit, lprocfs_wr_iunit, 0},
        { "quota_itune_sz", lprocfs_rd_itune, lprocfs_wr_itune, 0},
        { "quota_type",     lprocfs_rd_type, lprocfs_wr_type, 0},
#endif
        { "client_cache_count", lprocfs_filter_rd_fmd_max_num,
                          lprocfs_filter_wr_fmd_max_num, 0 },
        { "client_cache_seconds", lprocfs_filter_rd_fmd_max_age,
                          lprocfs_filter_wr_fmd_max_age, 0 },
        { 0 }
};

static struct lprocfs_vars lprocfs_module_vars[] = {
        { "num_refs",     lprocfs_rd_numrefs,       0, 0 },
        { 0 }
};

void filter_tally_write(struct obd_export *exp, struct page **pages,
                        int nr_pages, unsigned long *blocks,int blocks_per_page)
{
        struct filter_obd *filter = &exp->exp_obd->u.filter;
        struct filter_export_data *fed = &exp->exp_filter_data;
        struct page *last_page = NULL;
        unsigned long *last_block = NULL;
        unsigned long discont_pages = 0;
        unsigned long discont_blocks = 0;
        int i;

        if (nr_pages == 0)
                return;

        lprocfs_oh_tally_log2(&filter->fo_filter_stats.hist[BRW_W_PAGES],
                              nr_pages);
        lprocfs_oh_tally_log2(&fed->fed_brw_stats.hist[BRW_W_PAGES],
                              nr_pages);

        while (nr_pages-- > 0) {
                if (last_page && (*pages)->index != (last_page->index + 1))
                        discont_pages++;
                last_page = *pages;
                pages++;
                for (i = 0; i < blocks_per_page; i++) {
                        if (last_block && *blocks != (*last_block + 1))
                                discont_blocks++;
                        last_block = blocks++;
                }
        }

        lprocfs_oh_tally(&filter->fo_filter_stats.hist[BRW_W_DISCONT_PAGES],
                         discont_pages);
        lprocfs_oh_tally(&filter->fo_filter_stats.hist[BRW_W_DISCONT_BLOCKS],
                         discont_blocks);

        lprocfs_oh_tally(&fed->fed_brw_stats.hist[BRW_W_DISCONT_PAGES],
                         discont_pages);
        lprocfs_oh_tally(&fed->fed_brw_stats.hist[BRW_W_DISCONT_BLOCKS],
                         discont_blocks);
}

void filter_tally_read(struct obd_export *exp, struct page **pages,
                       int nr_pages, unsigned long *blocks, int blocks_per_page)
{
        struct filter_obd *filter = &exp->exp_obd->u.filter;
        struct page *last_page = NULL;
        unsigned long *last_block = NULL;
        unsigned long discont_pages = 0;
        unsigned long discont_blocks = 0;
        int i;

        if (nr_pages == 0)
                return;

        lprocfs_oh_tally_log2(&filter->fo_filter_stats.hist[BRW_R_PAGES], nr_pages);

        while (nr_pages-- > 0) {
                if (last_page && (*pages)->index != (last_page->index + 1))
                        discont_pages++;
                last_page = *pages;
                pages++;
                for (i = 0; i < blocks_per_page; i++) {
                        if (last_block && *blocks != (*last_block + 1))
                                discont_blocks++;
                        last_block = blocks++;
                }
        }

        lprocfs_oh_tally_log2(&filter->fo_filter_stats.hist[BRW_R_PAGES], nr_pages);
        lprocfs_oh_tally(&filter->fo_filter_stats.hist[BRW_R_DISCONT_PAGES], discont_pages);
        lprocfs_oh_tally(&filter->fo_filter_stats.hist[BRW_R_DISCONT_BLOCKS], discont_blocks);

        lprocfs_oh_tally_log2(&exp->exp_filter_data.fed_brw_stats.hist[BRW_R_PAGES],
                              nr_pages);
        lprocfs_oh_tally(&exp->exp_filter_data.fed_brw_stats.hist[BRW_R_DISCONT_PAGES],
                         discont_pages);
        lprocfs_oh_tally(&exp->exp_filter_data.fed_brw_stats.hist[BRW_R_DISCONT_BLOCKS],
                         discont_blocks);
}

#define pct(a,b) (b ? a * 100 / b : 0)

static void display_brw_stats(struct seq_file *seq, struct obd_histogram *read,
                              struct obd_histogram *write)
{
        unsigned long read_tot = 0, write_tot = 0, read_cum, write_cum;
        int i;

        read_tot = lprocfs_oh_sum(read);
        write_tot = lprocfs_oh_sum(write);

        read_cum = 0;
        write_cum = 0;
        for (i = 0; i < OBD_HIST_MAX; i++) {
                unsigned long r = read->oh_buckets[i];
                unsigned long w = write->oh_buckets[i];
                read_cum += r;
                write_cum += w;
                seq_printf(seq, "%u:\t\t%10lu %3lu %3lu   | %10lu %3lu %3lu\n",
                                 1 << i, r, pct(r, read_tot),
                                 pct(read_cum, read_tot), w,
                                 pct(w, write_tot),
                                 pct(write_cum, write_tot));
                if (read_cum == read_tot && write_cum == write_tot)
                        break;
        }
}

static void brw_stats_show(struct seq_file *seq, struct brw_stats *brw_stats)
{
        struct timeval now;
#if (LINUX_VERSION_CODE > KERNEL_VERSION(2,5,0))
        unsigned long read_tot = 0, write_tot = 0, read_cum, write_cum;
        int i;
#endif

        do_gettimeofday(&now);

        /* this sampling races with updates */

        seq_printf(seq, "snapshot_time:         %lu.%lu (secs.usecs)\n",
                   now.tv_sec, now.tv_usec);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "pages per brw         brws   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_PAGES], &brw_stats->hist[BRW_W_PAGES]);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "discont pages         rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_DISCONT_PAGES],
                          &brw_stats->hist[BRW_W_DISCONT_PAGES]);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "discont blocks        rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_DISCONT_BLOCKS],
                          &brw_stats->hist[BRW_W_DISCONT_BLOCKS]);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "dio frags             rpcs   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_DIO_FRAGS],
                          &brw_stats->hist[BRW_W_DIO_FRAGS]);

#if (LINUX_VERSION_CODE > KERNEL_VERSION(2,5,0))
        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "disk ios in flight     ios   %% cum %% |");
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_RPC_HIST],
                          &brw_stats->hist[BRW_W_RPC_HIST]);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "io time (1/%ds)     rpcs   %% cum %% |", HZ);
        seq_printf(seq, "       rpcs   %% cum %%\n");

        display_brw_stats(seq, &brw_stats->hist[BRW_R_IO_TIME],
                          &brw_stats->hist[BRW_W_IO_TIME]);

        seq_printf(seq, "\n\t\t\tread\t\t\twrite\n");
        seq_printf(seq, "disk I/O size         count  %% cum %% |");
        seq_printf(seq, "       count  %% cum %%\n");

        read_tot = lprocfs_oh_sum(&brw_stats->hist[BRW_R_DISK_IOSIZE]);
        write_tot = lprocfs_oh_sum(&brw_stats->hist[BRW_W_DISK_IOSIZE]);

        read_cum = 0;
        write_cum = 0;
        for (i = 0; i < OBD_HIST_MAX; i++) {
                unsigned long r = brw_stats->hist[BRW_R_DISK_IOSIZE].oh_buckets[i];
                unsigned long w = brw_stats->hist[BRW_W_DISK_IOSIZE].oh_buckets[i];

                read_cum += r;
                write_cum += w;
                if (read_cum == 0 && write_cum == 0)
                        continue;

                if (i < 10)
                        seq_printf(seq, "%u", 1<<i);
                else if (i < 20)
                        seq_printf(seq, "%uK", 1<<(i-10));
                else
                        seq_printf(seq, "%uM", 1<<(i-20));

                seq_printf(seq, ":\t\t%10lu %3lu %3lu   | %10lu %3lu %3lu\n",
                           r, pct(r, read_tot), pct(read_cum, read_tot),
                           w, pct(w, write_tot), pct(write_cum, write_tot));
                if (read_cum == read_tot && write_cum == write_tot)
                        break;
        }
#endif
}

static int filter_brw_stats_seq_show(struct seq_file *seq, void *v)
{
        struct obd_device *dev = seq->private;
        struct filter_obd *filter = &dev->u.filter;

        brw_stats_show(seq, &filter->fo_filter_stats);

        return 0;
}

static ssize_t filter_brw_stats_seq_write(struct file *file, const char *buf,
                                       size_t len, loff_t *off)
{
        struct seq_file *seq = file->private_data;
        struct obd_device *dev = seq->private;
        struct filter_obd *filter = &dev->u.filter;
        int i;

        for (i = 0; i < BRW_LAST; i++)
                lprocfs_oh_clear(&filter->fo_filter_stats.hist[i]);

        return len;
}

LPROC_SEQ_FOPS(filter_brw_stats);

int lproc_filter_attach_seqstat(struct obd_device *dev)
{
        return lprocfs_obd_seq_create(dev, "brw_stats", 0444,
                                      &filter_brw_stats_fops, dev);
}

static int filter_per_export_stats_seq_show(struct seq_file *seq, void *v)
{
        struct filter_export_data *fed = seq->private;

        brw_stats_show(seq, &fed->fed_brw_stats);

        return 0;
}

static ssize_t filter_per_export_stats_seq_write(struct file *file,
                                       const char *buf, size_t len, loff_t *off)
{
        struct seq_file *seq = file->private_data;
        struct filter_export_data *fed = seq->private;
        int i;

        for (i = 0; i < BRW_LAST; i++)
                lprocfs_oh_clear(&fed->fed_brw_stats.hist[i]);

        return len;
}

LPROC_SEQ_FOPS(filter_per_export_stats);

LPROCFS_INIT_VARS(filter, lprocfs_module_vars, lprocfs_obd_vars)
#endif /* LPROCFS */
