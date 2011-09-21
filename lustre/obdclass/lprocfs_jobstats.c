/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011 Whamcloud, Inc.
 * Use is subject to license terms.
 *
 * Author: Niu Yawei <niu@whamcloud.com>
 */
/*
 * lustre/obdclass/lprocfs_jobstats.c
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_CLASS

#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <obd_class.h>
#include <lprocfs_status.h>
#include <lustre/lustre_idl.h>

#if defined(LPROCFS)

/*
 * JobID formats & JobID environment variable names for supported
 * job schedulers:
 *
 * SLURM:
 *   JobID format:  32 bit integer.
 *   JobID env var: SLURM_JOB_ID.
 * SGE:
 *   JobID format:  Decimal integer range to 99999.
 *   JobID env var: JOB_ID.
 * LSF:
 *   JobID format:  6 digit integer by default (up to 999999), can be
 *                  increased to 10 digit (up to 2147483646).
 *   JobID env var: LSB_JOBID.
 * Loadleveler:
 *   JobID format:  String of machine_name.cluster_id.process_id, for
 *                  example: fr2n02.32.0
 *   JobID env var: LOADL_STEP_ID.
 * PBS:
 *   JobID format:  String of sequence_number[.server_name][@server].
 *   JobID env var: PBS_JOBID.
 * Maui/MOAB:
 *   JobID format:  Same as PBS.
 *   JobID env var: Same as PBS.
 */

struct job_stat {
        cfs_hlist_node_t      js_hash;
        cfs_list_t            js_list;
        cfs_atomic_t          js_refcount;
        char                  js_jobid[JOBSTATS_JOBID_SIZE];
        time_t                js_timestamp;
        struct lprocfs_stats *js_stats;
        struct obd_job_stats *js_jobstats;
};

static unsigned job_stat_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
        LASSERT(strlen(key) < JOBSTATS_JOBID_SIZE);
        return cfs_hash_djb2_hash(key, strlen(key), mask);
}

static void *job_stat_key(cfs_hlist_node_t *hnode)
{
        struct job_stat *job;
        job = cfs_hlist_entry(hnode, struct job_stat, js_hash);
        return job->js_jobid;
}

static int job_stat_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
        struct job_stat *job;
        job = cfs_hlist_entry(hnode, struct job_stat, js_hash);
        return (strlen(job->js_jobid) == strlen(key)) &&
               !strncmp(job->js_jobid, key, strlen(key));
}

static void *job_stat_object(cfs_hlist_node_t *hnode)
{
        return cfs_hlist_entry(hnode, struct job_stat, js_hash);
}

static void job_stat_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        struct job_stat *job;
        job = cfs_hlist_entry(hnode, struct job_stat, js_hash);
        cfs_atomic_inc(&job->js_refcount);
}

static void job_free(struct job_stat *job)
{
        LASSERT(atomic_read(&job->js_refcount) == 0);
        LASSERT(job->js_jobstats);

        cfs_write_lock(&job->js_jobstats->ojs_lock);
        cfs_list_del_init(&job->js_list);
        cfs_write_unlock(&job->js_jobstats->ojs_lock);

        lprocfs_free_stats(&job->js_stats);
        OBD_FREE_PTR(job);
}

static void job_putref(struct job_stat *job)
{
        LASSERT(atomic_read(&job->js_refcount) > 0);
        if (atomic_dec_and_test(&job->js_refcount))
                job_free(job);
}

static void job_stat_put_locked(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        struct job_stat *job;
        job = cfs_hlist_entry(hnode, struct job_stat, js_hash);
        job_putref(job);
}

static void job_stat_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        CERROR("Should not have any items!");
}

static cfs_hash_ops_t job_stats_hash_ops = {
        .hs_hash        = job_stat_hash,
        .hs_key         = job_stat_key,
        .hs_keycmp      = job_stat_keycmp,
        .hs_object      = job_stat_object,
        .hs_get         = job_stat_get,
        .hs_put_locked  = job_stat_put_locked,
        .hs_exit        = job_stat_exit,
};

static struct job_stat *job_alloc(char *jobid, struct obd_job_stats *jobs)
{
        struct job_stat *job;

        LASSERT(jobs->ojs_cntr_num && jobs->ojs_cntr_init_fn);

        OBD_ALLOC_PTR(job);
        if (job == NULL) {
                CERROR("Not enough memory.\n");
                return NULL;
        }

        job->js_stats = lprocfs_alloc_stats(jobs->ojs_cntr_num, 0);
        if (job->js_stats == NULL) {
                OBD_FREE_PTR(job);
                CERROR("Not enough memroy.\n");
                return NULL;
        }

        jobs->ojs_cntr_init_fn(job->js_stats);

        strncpy(job->js_jobid, jobid, strlen(jobid));
        job->js_timestamp = cfs_time_current_sec();
        job->js_jobstats = jobs;
        CFS_INIT_HLIST_NODE(&job->js_hash);
        CFS_INIT_LIST_HEAD(&job->js_list);
        cfs_atomic_set(&job->js_refcount, 1);

        return job;
}

int lprocfs_job_stats_log(struct obd_device *obd, char *jobid,
                          int event, long amount)
{
        struct obd_job_stats *stats = &obd->obd_jobstats;
        struct job_stat *job, *job2;
        ENTRY;

        LASSERT(stats && stats->ojs_hash);

        if (!jobid || !strlen(jobid))
                RETURN(-EINVAL);

        if (strlen(jobid) >= JOBSTATS_JOBID_SIZE) {
                CERROR("Invalid jobid size (%lu), expect(%d)\n",
                       strlen(jobid) + 1, JOBSTATS_JOBID_SIZE);
                RETURN(-EINVAL);
        }

        job = cfs_hash_lookup(stats->ojs_hash, jobid);
        if (job)
                goto found;

        job = job_alloc(jobid, stats);
        if (job == NULL)
                RETURN(-ENOMEM);

        job2 = cfs_hash_findadd_unique(stats->ojs_hash, job->js_jobid,
                                       &job->js_hash);
        if (job2 != job) {
                job_putref(job);
                job = job2;
                LASSERT(!cfs_list_empty(&job->js_list));
        } else {
                LASSERT(cfs_list_empty(&job->js_list));
                cfs_write_lock(&stats->ojs_lock);
                cfs_list_add_tail(&job->js_list, &stats->ojs_list);
                cfs_write_unlock(&stats->ojs_lock);
        }

found:
        LASSERT(stats == job->js_jobstats);
        LASSERT(stats->ojs_cntr_num > event);
        job->js_timestamp = cfs_time_current_sec();
        lprocfs_counter_add(job->js_stats, event, amount);

        job_putref(job);
        RETURN(0);
}
EXPORT_SYMBOL(lprocfs_job_stats_log);

static int job_iter_callback(cfs_hash_t *hs, cfs_hash_bd_t *bd,
                             cfs_hlist_node_t *hnode, void *data)
{
        int force = *((int *)data);
        struct job_stat *job;
        int interval;

        job = cfs_hlist_entry(hnode, struct job_stat, js_hash);
        interval = job->js_jobstats->ojs_cleanup_interval;
        if (force || (interval && cfs_time_current_sec() >
                      job->js_timestamp + interval))
                cfs_hash_bd_del_locked(hs, bd, hnode);

        return 0;
}

void lprocfs_job_stats_fini(struct obd_device *obd)
{
        struct obd_job_stats *stats = &obd->obd_jobstats;
        int force = 1;

        if (stats->ojs_hash == NULL)
                return;
        cfs_timer_disarm(&stats->ojs_cleanup_timer);
        cfs_hash_for_each_safe(stats->ojs_hash, job_iter_callback, &force);
        cfs_hash_putref(stats->ojs_hash);
        stats->ojs_hash = NULL;
        LASSERT(cfs_list_empty(&stats->ojs_list));
}
EXPORT_SYMBOL(lprocfs_job_stats_fini);

static void *lprocfs_jobstats_seq_start(struct seq_file *p, loff_t *pos)
{
        struct obd_job_stats *stats = p->private;
        loff_t off = *pos;
        struct job_stat *job;

        cfs_read_lock(&stats->ojs_lock);
        if (off == 0)
                return SEQ_START_TOKEN;
        off--;
        cfs_list_for_each_entry(job, &stats->ojs_list, js_list) {
                if (!off--)
                        return job;
        }
        return NULL;
}

static void lprocfs_jobstats_seq_stop(struct seq_file *p, void *v)
{
        struct obd_job_stats *stats = p->private;

        cfs_read_unlock(&stats->ojs_lock);
}

static void *lprocfs_jobstats_seq_next(struct seq_file *p, void *v, loff_t *pos)
{
        struct obd_job_stats *stats = p->private;
        struct job_stat *job;
        cfs_list_t *next;

        ++*pos;
        if (v == SEQ_START_TOKEN) {
                next = stats->ojs_list.next;
        } else {
                job = (struct job_stat *)v;
                next = job->js_list.next;
        }

        return next == &stats->ojs_list ? NULL :
                cfs_list_entry(next, struct job_stat, js_list);
}

static int lprocfs_jobstats_seq_show(struct seq_file *p, void *v)
{
        struct job_stat *job = v;
        struct lprocfs_stats *s;
        struct lprocfs_counter ret, *cntr;
        int i;

        if (v == SEQ_START_TOKEN) {
                seq_printf(p, "# All job stats:\n");
                return 0;
        }

        seq_printf(p, "---\n");
        seq_printf(p, "Job Stats:\n");
        seq_printf(p, "    Job ID: %s\n", job->js_jobid);
        seq_printf(p, "    Timestamp: %ld\n", job->js_timestamp);

        s = job->js_stats;
        for (i = 0; i < s->ls_num; i++) {
                cntr = &(s->ls_percpu[0]->lp_cntr[i]);
                lprocfs_stats_collect(s, i, &ret);

                if (!ret.lc_count)
                        continue;

                seq_printf(p, "    %s:", cntr->lc_name);

                if (cntr->lc_config & LPROCFS_CNTR_AVGMINMAX) {
                        seq_printf(p, "\n");
                        seq_printf(p, "        samples: "LPD64"\n", ret.lc_count);
                        seq_printf(p, "        min: "LPD64" %s\n",
                                   ret.lc_min, cntr->lc_units);
                        seq_printf(p, "        max: "LPD64" %s\n",
                                   ret.lc_max, cntr->lc_units);
                        seq_printf(p, "        sum: "LPD64" %s\n",
                                   ret.lc_sum, cntr->lc_units);
                } else {
                        seq_printf(p, " "LPD64"\n", ret.lc_count);
                }

        }
        return 0;
}

struct seq_operations lprocfs_jobstats_seq_sops = {
        start: lprocfs_jobstats_seq_start,
        stop:  lprocfs_jobstats_seq_stop,
        next:  lprocfs_jobstats_seq_next,
        show:  lprocfs_jobstats_seq_show,
};

static int lprocfs_jobstats_seq_open(struct inode *inode, struct file *file)
{
        struct proc_dir_entry *dp = PDE(inode);
        struct seq_file *seq;
        int rc;

        if (LPROCFS_ENTRY_AND_CHECK(dp))
                return -ENOENT;

        rc = seq_open(file, &lprocfs_jobstats_seq_sops);
        if (rc) {
                LPROCFS_EXIT();
                return rc;
        }
        seq = file->private_data;
        seq->private = dp->data;
        return 0;
}

static ssize_t lprocfs_jobstats_seq_write(struct file *file, const char *buf,
                                          size_t len, loff_t *off)
{
        struct seq_file *seq = file->private_data;
        struct obd_job_stats *stats = seq->private;
        char jobid[JOBSTATS_JOBID_SIZE];
        const char *act[] = {"clear ", "all"};
        int all = 0;
        struct job_stat *job;
        char *tmp = (char *)buf;
        int tmplen = len;

        if (tmplen > strlen(act[0]) && !memcmp(tmp, act[0], strlen(act[0]))) {
                tmp += strlen(act[0]);
                tmplen -= strlen(act[0]);
        } else {
                return -EINVAL;
        }

        if (tmplen > strlen(act[1]) && !memcmp(tmp, act[1], strlen(act[1]))) {
                all = 1;
        } else if (tmplen < JOBSTATS_JOBID_SIZE) {
                memset(jobid, 0, JOBSTATS_JOBID_SIZE);
                /* Trim '\n' if any */
                if (tmp[tmplen - 1] == '\n')
                        memcpy(jobid, tmp, tmplen - 1);
                else
                        memcpy(jobid, tmp, tmplen);
        } else {
                return -EINVAL;
        }

        LASSERT(stats->ojs_hash);
        if (all) {
                int force = 1;
                cfs_hash_for_each_safe(stats->ojs_hash, job_iter_callback,
                                       &force);
                return len;
        }

        if (!strlen(jobid))
                return -EINVAL;

        job = cfs_hash_lookup(stats->ojs_hash, jobid);
        if (!job)
                return -EINVAL;

        cfs_hash_del_key(stats->ojs_hash, jobid);

        job_putref(job);
        return len;
}

struct file_operations lprocfs_jobstats_seq_fops = {
        .owner   = THIS_MODULE,
        .open    = lprocfs_jobstats_seq_open,
        .read    = seq_read,
        .write   = lprocfs_jobstats_seq_write,
        .llseek  = seq_lseek,
        .release = lprocfs_seq_release,
};

static void job_cleanup_callback(unsigned long data)
{
        struct obd_job_stats *stats = (struct obd_job_stats *)data;
        int force = 0;

        cfs_hash_for_each_safe(stats->ojs_hash, job_iter_callback,
                               &force);
        if (stats->ojs_cleanup_interval)
                cfs_timer_arm(&stats->ojs_cleanup_timer,
                              cfs_time_shift(stats->ojs_cleanup_interval));
}

int lprocfs_job_stats_init(struct obd_device *obd, int cntr_num,
                           cntr_init_callback init_fn)
{
        struct proc_dir_entry *entry;
        struct obd_job_stats *stats = &obd->obd_jobstats;
        ENTRY;

        LASSERT(obd->obd_proc_entry != NULL);
        LASSERT(obd->obd_type->typ_name);

        if (strcmp(obd->obd_type->typ_name, LUSTRE_MDT_NAME) &&
            strcmp(obd->obd_type->typ_name, LUSTRE_OST_NAME)) {
                CERROR("Invalid obd device type.\n");
                RETURN(-EINVAL);
        }

        LASSERT(stats->ojs_hash == NULL);
        stats->ojs_hash = cfs_hash_create("JOB_STATS_HASH",
                                          HASH_JOB_STATS_CUR_BITS,
                                          HASH_JOB_STATS_MAX_BITS,
                                          HASH_JOB_STATS_BKT_BITS, 0,
                                          CFS_HASH_MIN_THETA,
                                          CFS_HASH_MAX_THETA,
                                          &job_stats_hash_ops,
                                          CFS_HASH_DEFAULT);
        if (stats->ojs_hash == NULL) {
                CERROR("Create jobid stats hash failed.\n");
                RETURN(-ENOMEM);
        }
        CFS_INIT_LIST_HEAD(&stats->ojs_list);
        cfs_rwlock_init(&stats->ojs_lock);
        stats->ojs_cntr_num = cntr_num;
        stats->ojs_cntr_init_fn = init_fn;
        cfs_timer_init(&stats->ojs_cleanup_timer, job_cleanup_callback, stats);
        stats->ojs_cleanup_interval = 600; /* 10 mins by default */
        cfs_timer_arm(&stats->ojs_cleanup_timer,
                      cfs_time_shift(stats->ojs_cleanup_interval));

        LPROCFS_WRITE_ENTRY();
        entry = create_proc_entry("job_stats", 0644, obd->obd_proc_entry);
        LPROCFS_WRITE_EXIT();
        if (entry) {
                entry->proc_fops = &lprocfs_jobstats_seq_fops;
                entry->data = stats;
                RETURN(0);
        } else {
                lprocfs_job_stats_fini(obd);
                RETURN(-ENOMEM);
        }
}
EXPORT_SYMBOL(lprocfs_job_stats_init);

int lprocfs_rd_job_interval(char *page, char **start, off_t off,
                            int count, int *eof, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;
        struct obd_job_stats *stats;

        LASSERT(obd != NULL);
        stats = &obd->obd_jobstats;
        *eof = 1;
        return snprintf(page, count, "%d\n", stats->ojs_cleanup_interval);
}
EXPORT_SYMBOL(lprocfs_rd_job_interval);

int lprocfs_wr_job_interval(struct file *file, const char *buffer,
                            unsigned long count, void *data)
{
        struct obd_device *obd = (struct obd_device *)data;
        struct obd_job_stats *stats;
        int val, rc;

        LASSERT(obd != NULL);
        stats = &obd->obd_jobstats;

        rc = lprocfs_write_helper(buffer, count, &val);
        if (rc)
                return rc;

        stats->ojs_cleanup_interval = val;
        if (!stats->ojs_cleanup_interval)
                cfs_timer_disarm(&stats->ojs_cleanup_timer);
        else
                cfs_timer_arm(&stats->ojs_cleanup_timer,
                              cfs_time_shift(stats->ojs_cleanup_interval));

        return count;

}
EXPORT_SYMBOL(lprocfs_wr_job_interval);

#endif /* LPROCFS*/
