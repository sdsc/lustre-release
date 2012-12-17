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
 * Copyright (c) 2011, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/lprocfs_status_server.c
 *
 * Author: Hariharan Thantry <thantry@users.sourceforge.net>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#ifndef __KERNEL__
# include <liblustre.h>
#endif

#include <obd_class.h>
#include <lprocfs_status.h>
#include <lustre_fsfilt.h>
#include <lustre_log.h>
#include <lustre_disk.h>
#include <lustre/lustre_idl.h>
#include <dt_object.h>

#if defined(LPROCFS)

int lprocfs_evict_client_open(struct inode *inode, struct file *f)
{
	struct proc_dir_entry *dp = PDE(f->f_dentry->d_inode);
	struct obd_device *obd = dp->data;

	cfs_atomic_inc(&obd->obd_evict_inprogress);

	return 0;
}

int lprocfs_evict_client_release(struct inode *inode, struct file *f)
{
	struct proc_dir_entry *dp = PDE(f->f_dentry->d_inode);
	struct obd_device *obd = dp->data;

	cfs_atomic_dec(&obd->obd_evict_inprogress);
	cfs_waitq_signal(&obd->obd_evict_inprogress_waitq);

	return 0;
}

struct file_operations lprocfs_evict_client_fops = {
	.owner = THIS_MODULE,
	.read = lprocfs_fops_read,
	.write = lprocfs_fops_write,
	.open = lprocfs_evict_client_open,
	.release = lprocfs_evict_client_release,
};
EXPORT_SYMBOL(lprocfs_evict_client_fops);

static void lprocfs_free_client_stats(struct nid_stat *client_stat)
{
	CDEBUG(D_CONFIG, "stat %p - data %p/%p/%p\n", client_stat,
	       client_stat->nid_proc, client_stat->nid_stats,
	       client_stat->nid_brw_stats);

	LASSERTF(cfs_atomic_read(&client_stat->nid_exp_ref_count) == 0,
		 "nid %s:count %d\n", libcfs_nid2str(client_stat->nid),
		 atomic_read(&client_stat->nid_exp_ref_count));

	if (client_stat->nid_proc)
		lprocfs_remove(&client_stat->nid_proc);

	if (client_stat->nid_stats)
		lprocfs_free_stats(&client_stat->nid_stats);

	if (client_stat->nid_brw_stats)
		OBD_FREE_PTR(client_stat->nid_brw_stats);

	if (client_stat->nid_ldlm_stats)
		lprocfs_free_stats(&client_stat->nid_ldlm_stats);

	OBD_FREE_PTR(client_stat);
	return;

}

void lprocfs_free_per_client_stats(struct obd_device *obd)
{
	cfs_hash_t *hash = obd->obd_nid_stats_hash;
	struct nid_stat *stat;
	ENTRY;

	/* we need extra list - because hash_exit called to early */
	/* not need locking because all clients is died */
	while (!cfs_list_empty(&obd->obd_nid_stats)) {
		stat = cfs_list_entry(obd->obd_nid_stats.next,
				      struct nid_stat, nid_list);
		cfs_list_del_init(&stat->nid_list);
		cfs_hash_del(hash, &stat->nid, &stat->nid_hash);
		lprocfs_free_client_stats(stat);
	}
	EXIT;
}
EXPORT_SYMBOL(lprocfs_free_per_client_stats);

#define LPROCFS_OBD_OP_INIT(base, stats, op)                               \
do {                                                                       \
	unsigned int coffset = base + OBD_COUNTER_OFFSET(op);              \
	LASSERT(coffset < stats->ls_num);                                  \
	lprocfs_counter_init(stats, coffset, 0, #op, "reqs");              \
} while (0)

void lprocfs_init_ops_stats(int num_private_stats, struct lprocfs_stats *stats)
{
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, iocontrol);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, get_info);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, set_info_async);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, attach);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, detach);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, setup);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, precleanup);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, cleanup);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, process_config);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, postrecov);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, add_conn);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, del_conn);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, connect);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, reconnect);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, disconnect);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, fid_init);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, fid_fini);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, fid_alloc);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, statfs);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, statfs_async);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, packmd);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, unpackmd);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, preallocate);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, precreate);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, create);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, create_async);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, destroy);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, setattr);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, setattr_async);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, getattr);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, getattr_async);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, brw);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, merge_lvb);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, adjust_kms);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, punch);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, sync);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, migrate);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, copy);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, iterate);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, preprw);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, commitrw);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, enqueue);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, change_cbdata);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, find_cbdata);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, cancel);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, cancel_unused);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, init_export);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, destroy_export);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, extent_calc);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, llog_init);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, llog_connect);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, llog_finish);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, pin);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, unpin);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, import_event);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, notify);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, health_check);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, get_uuid);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, quotacheck);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, quotactl);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, ping);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, pool_new);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, pool_rem);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, pool_add);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, pool_del);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, getref);
	LPROCFS_OBD_OP_INIT(num_private_stats, stats, putref);
}
EXPORT_SYMBOL(lprocfs_init_ops_stats);

int lprocfs_alloc_obd_stats(struct obd_device *obd, unsigned num_private_stats)
{
	struct lprocfs_stats *stats;
	unsigned int num_stats;
	int rc, i;

	LASSERT(obd->obd_stats == NULL);
	LASSERT(obd->obd_proc_entry != NULL);
	LASSERT(obd->obd_cntr_base == 0);

	num_stats =
	    ((int) sizeof(*obd->obd_type->typ_dt_ops) / sizeof(void *)) +
	    num_private_stats - 1 /* o_owner */ ;
	stats = lprocfs_alloc_stats(num_stats, 0);
	if (stats == NULL)
		return -ENOMEM;

	lprocfs_init_ops_stats(num_private_stats, stats);

	for (i = num_private_stats; i < num_stats; i++) {
		/* If this LBUGs, it is likely that an obd
		 * operation was added to struct obd_ops in
		 * <obd.h>, and that the corresponding line item
		 * LPROCFS_OBD_OP_INIT(.., .., opname)
		 * is missing from the list above. */
		LASSERTF(stats->ls_percpu[0]->lp_cntr[i].lc_name != NULL,
			 "Missing obd_stat initializer obd_op "
			 "operation at offset %d.\n", i - num_private_stats);
	}
	rc = lprocfs_register_stats(obd->obd_proc_entry, "stats", stats);
	if (rc < 0) {
		lprocfs_free_stats(&stats);
	} else {
		obd->obd_stats = stats;
		obd->obd_cntr_base = num_private_stats;
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_alloc_obd_stats);

void lprocfs_free_obd_stats(struct obd_device *obd)
{
	if (obd->obd_stats)
		lprocfs_free_stats(&obd->obd_stats);
}
EXPORT_SYMBOL(lprocfs_free_obd_stats);

#define LPROCFS_MD_OP_INIT(base, stats, op)                             \
do {                                                                    \
	unsigned int coffset = base + MD_COUNTER_OFFSET(op);            \
	LASSERT(coffset < stats->ls_num);                               \
	lprocfs_counter_init(stats, coffset, 0, #op, "reqs");           \
} while (0)

void lprocfs_init_mps_stats(int num_private_stats, struct lprocfs_stats *stats)
{
	LPROCFS_MD_OP_INIT(num_private_stats, stats, getstatus);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, change_cbdata);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, find_cbdata);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, close);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, create);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, done_writing);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, enqueue);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, getattr);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, getattr_name);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, intent_lock);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, link);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, rename);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, is_subdir);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, setattr);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, sync);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, readpage);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, unlink);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, setxattr);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, getxattr);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, init_ea_size);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, get_lustre_md);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, free_lustre_md);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, set_open_replay_data);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, clear_open_replay_data);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, set_lock_data);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, lock_match);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, cancel_unused);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, renew_capa);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, unpack_capa);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, get_remote_perm);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, intent_getattr_async);
	LPROCFS_MD_OP_INIT(num_private_stats, stats, revalidate_lock);
}
EXPORT_SYMBOL(lprocfs_init_mps_stats);

int lprocfs_alloc_md_stats(struct obd_device *obd, unsigned num_private_stats)
{
	struct lprocfs_stats *stats;
	unsigned int num_stats;
	int rc, i;

	LASSERT(obd->md_stats == NULL);
	LASSERT(obd->obd_proc_entry != NULL);
	LASSERT(obd->md_cntr_base == 0);

	num_stats = 1 + MD_COUNTER_OFFSET(revalidate_lock) + num_private_stats;
	stats = lprocfs_alloc_stats(num_stats, 0);
	if (stats == NULL)
		return -ENOMEM;

	lprocfs_init_mps_stats(num_private_stats, stats);

	for (i = num_private_stats; i < num_stats; i++) {
		if (stats->ls_percpu[0]->lp_cntr[i].lc_name == NULL) {
			CERROR("Missing md_stat initializer md_op "
			       "operation at offset %d. Aborting.\n",
			       i - num_private_stats);
			LBUG();
		}
	}
	rc = lprocfs_register_stats(obd->obd_proc_entry, "md_stats", stats);
	if (rc < 0) {
		lprocfs_free_stats(&stats);
	} else {
		obd->md_stats = stats;
		obd->md_cntr_base = num_private_stats;
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_alloc_md_stats);

void lprocfs_free_md_stats(struct obd_device *obd)
{
	struct lprocfs_stats *stats = obd->md_stats;

	if (stats != NULL) {
		obd->md_stats = NULL;
		obd->md_cntr_base = 0;
		lprocfs_free_stats(&stats);
	}
}
EXPORT_SYMBOL(lprocfs_free_md_stats);

int lprocfs_nid_stats_clear_read(char *page, char **start, off_t off,
				 int count, int *eof, void *data)
{
	*eof = 1;
	return snprintf(page, count, "%s\n",
			"Write into this file to clear all nid stats and "
			"stale nid entries");
}
EXPORT_SYMBOL(lprocfs_nid_stats_clear_read);

static int lprocfs_nid_stats_clear_write_cb(void *obj, void *data)
{
	struct nid_stat *stat = obj;
	int i;
	ENTRY;

	CDEBUG(D_INFO, "refcnt %d\n",
	       cfs_atomic_read(&stat->nid_exp_ref_count));
	if (cfs_atomic_read(&stat->nid_exp_ref_count) == 1) {
		/* object has only hash references. */
		spin_lock(&stat->nid_obd->obd_nid_lock);
		cfs_list_move(&stat->nid_list, data);
		spin_unlock(&stat->nid_obd->obd_nid_lock);
		RETURN(1);
	}
	/* we has reference to object - only clear data */
	if (stat->nid_stats)
		lprocfs_clear_stats(stat->nid_stats);

	if (stat->nid_brw_stats) {
		for (i = 0; i < BRW_LAST; i++)
			lprocfs_oh_clear(&stat->nid_brw_stats->hist[i]);
	}
	RETURN(0);
}

int lprocfs_nid_stats_clear_write(struct file *file, const char *buffer,
				  unsigned long count, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	struct nid_stat *client_stat;
	CFS_LIST_HEAD(free_list);

	cfs_hash_cond_del(obd->obd_nid_stats_hash,
			  lprocfs_nid_stats_clear_write_cb, &free_list);

	while (!cfs_list_empty(&free_list)) {
		client_stat = cfs_list_entry(free_list.next, struct nid_stat,
					     nid_list);
		cfs_list_del_init(&client_stat->nid_list);
		lprocfs_free_client_stats(client_stat);
	}

	return count;
}
EXPORT_SYMBOL(lprocfs_nid_stats_clear_write);

int lprocfs_obd_rd_hash(char *page, char **start, off_t off,
			int count, int *eof, void *data)
{
	struct obd_device *obd = data;
	int c = 0;

	if (obd == NULL)
		return 0;

	c += cfs_hash_debug_header(page, count);
	c += cfs_hash_debug_str(obd->obd_uuid_hash, page + c, count - c);
	c += cfs_hash_debug_str(obd->obd_nid_hash, page + c, count - c);
	c += cfs_hash_debug_str(obd->obd_nid_stats_hash, page + c, count - c);

	return c;
}
EXPORT_SYMBOL(lprocfs_obd_rd_hash);

/* Function that emulates snprintf but also has the side effect of advancing
   the page pointer for the next write into the buffer, incrementing the total
   length written to the buffer, and decrementing the size left in the
   buffer. */
static int lprocfs_obd_snprintf(char **page, int end, int *len,
				const char *format, ...)
{
	va_list list;
	int n;

	if (*len >= end)
		return 0;

	va_start(list, format);
	n = vsnprintf(*page, end - *len, format, list);
	va_end(list);

	*page += n;
	*len += n;
	return n;
}

int lprocfs_obd_rd_recovery_status(char *page, char **start, off_t off,
				   int count, int *eof, void *data)
{
	struct obd_device *obd = data;
	int len = 0, size;

	LASSERT(obd != NULL);
	LASSERT(count >= 0);

	/* Set start of user data returned to
	   page + off since the user may have
	   requested to read much smaller than
	   what we need to read */
	*start = page + off;

	/* We know we are allocated a page here.
	   Also we know that this function will
	   not need to write more than a page
	   so we can truncate at CFS_PAGE_SIZE.  */
	size = min(count + (int) off + 1, (int) CFS_PAGE_SIZE);

	/* Initialize the page */
	memset(page, 0, size);

	if (lprocfs_obd_snprintf(&page, size, &len, "status: ") <= 0)
		goto out;
	if (obd->obd_max_recoverable_clients == 0) {
		if (lprocfs_obd_snprintf(&page, size, &len, "INACTIVE\n") <= 0)
			goto out;

		goto fclose;
	}

	/* sampled unlocked, but really... */
	if (obd->obd_recovering == 0) {
		if (lprocfs_obd_snprintf(&page, size, &len, "COMPLETE\n") <= 0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len,
					 "recovery_start: %lu\n",
					 obd->obd_recovery_start) <= 0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len,
					 "recovery_duration: %lu\n",
					 obd->obd_recovery_end -
					 obd->obd_recovery_start) <= 0)
			goto out;
		/* Number of clients that have completed recovery */
		if (lprocfs_obd_snprintf(&page, size, &len,
					 "completed_clients: %d/%d\n",
					 obd->obd_max_recoverable_clients -
					 obd->obd_stale_clients,
					 obd->obd_max_recoverable_clients) <= 0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len,
					 "replayed_requests: %d\n",
					 obd->obd_replayed_requests) <= 0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len,
					 "last_transno: " LPD64 "\n",
					 obd->obd_next_recovery_transno - 1)<=0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len, "VBR: %s\n",
					 obd->obd_version_recov ?
					 "ENABLED" : "DISABLED") <=0)
			goto out;
		if (lprocfs_obd_snprintf(&page, size, &len, "IR: %s\n",
					 obd->obd_no_ir ?
					 "DISABLED" : "ENABLED") <= 0)
			goto out;
		goto fclose;
	}

	if (lprocfs_obd_snprintf(&page, size, &len, "RECOVERING\n") <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "recovery_start: %lu\n",
				 obd->obd_recovery_start) <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "time_remaining: %lu\n",
				 cfs_time_current_sec() >=
				 obd->obd_recovery_start +
				 obd->obd_recovery_timeout ? 0 :
				 obd->obd_recovery_start +
				 obd->obd_recovery_timeout -
				 cfs_time_current_sec()) <= 0)
		goto out;
	if (lprocfs_obd_snprintf
	    (&page, size, &len, "connected_clients: %d/%d\n",
	     cfs_atomic_read(&obd->obd_connected_clients),
	     obd->obd_max_recoverable_clients) <= 0)
		goto out;
	/* Number of clients that have completed recovery */
	if (lprocfs_obd_snprintf(&page, size, &len, "req_replay_clients: %d\n",
				 cfs_atomic_read(&obd->obd_req_replay_clients))
	    <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "lock_repay_clients: %d\n",
				 cfs_atomic_read(&obd->obd_lock_replay_clients))
	    <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "completed_clients: %d\n",
				 cfs_atomic_read(&obd->obd_connected_clients) -
				 cfs_atomic_read(&obd->obd_lock_replay_clients))
	    <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "evicted_clients: %d\n",
				 obd->obd_stale_clients) <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "replayed_requests: %d\n",
				 obd->obd_replayed_requests) <= 0)
		goto out;
	if (lprocfs_obd_snprintf(&page, size, &len, "queued_requests: %d\n",
				 obd->obd_requests_queued_for_recovery) <= 0)
		goto out;

	if (lprocfs_obd_snprintf(&page, size, &len, "next_transno: " LPD64 "\n",
				 obd->obd_next_recovery_transno) <= 0)
		goto out;

fclose:
	*eof = 1;
out:
	return min(count, len - (int) off);
}
EXPORT_SYMBOL(lprocfs_obd_rd_recovery_status);

int lprocfs_obd_rd_ir_factor(char *page, char **start, off_t off,
			     int count, int *eof, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	LASSERT(obd != NULL);

	return snprintf(page, count, "%d\n", obd->obd_recovery_ir_factor);
}
EXPORT_SYMBOL(lprocfs_obd_rd_ir_factor);

int lprocfs_obd_wr_ir_factor(struct file *file, const char *buffer,
			     unsigned long count, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	int val, rc;
	LASSERT(obd != NULL);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	if (val < OBD_IR_FACTOR_MIN || val > OBD_IR_FACTOR_MAX)
		return -EINVAL;

	obd->obd_recovery_ir_factor = val;
	return count;
}
EXPORT_SYMBOL(lprocfs_obd_wr_ir_factor);

int lprocfs_obd_rd_recovery_time_soft(char *page, char **start, off_t off,
				      int count, int *eof, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	LASSERT(obd != NULL);

	return snprintf(page, count, "%d\n", obd->obd_recovery_timeout);
}
EXPORT_SYMBOL(lprocfs_obd_rd_recovery_time_soft);

int lprocfs_obd_wr_recovery_time_soft(struct file *file, const char *buffer,
				      unsigned long count, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	int val, rc;
	LASSERT(obd != NULL);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	obd->obd_recovery_timeout = val;
	return count;
}
EXPORT_SYMBOL(lprocfs_obd_wr_recovery_time_soft);

int lprocfs_obd_rd_recovery_time_hard(char *page, char **start, off_t off,
				      int count, int *eof, void *data)
{
	struct obd_device *obd = data;
	LASSERT(obd != NULL);

	return snprintf(page, count, "%u\n", obd->obd_recovery_time_hard);
}
EXPORT_SYMBOL(lprocfs_obd_rd_recovery_time_hard);

int lprocfs_obd_wr_recovery_time_hard(struct file *file, const char *buffer,
				      unsigned long count, void *data)
{
	struct obd_device *obd = data;
	int val, rc;
	LASSERT(obd != NULL);

	rc = lprocfs_write_helper(buffer, count, &val);
	if (rc)
		return rc;

	obd->obd_recovery_time_hard = val;
	return count;
}
EXPORT_SYMBOL(lprocfs_obd_wr_recovery_time_hard);

int lprocfs_obd_rd_mntdev(char *page, char **start, off_t off,
			  int count, int *eof, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	struct lustre_mount_info *lmi;
	const char *dev_name;

	LASSERT(obd != NULL);
	lmi = server_get_mount_2(obd->obd_name);
	dev_name = get_mntdev_name(lmi->lmi_sb);
	LASSERT(dev_name != NULL);
	*eof = 1;
	server_put_mount_2(obd->obd_name, lmi->lmi_mnt);
	return snprintf(page, count, "%s\n", dev_name);
}
EXPORT_SYMBOL(lprocfs_obd_rd_mntdev);

int lprocfs_target_rd_instance(char *page, char **start, off_t off,
			       int count, int *eof, void *data)
{
	struct obd_device *obd = (struct obd_device *) data;
	struct obd_device_target *target = &obd->u.obt;

	LASSERT(obd != NULL);
	LASSERT(target->obt_magic == OBT_MAGIC);
	*eof = 1;
	return snprintf(page, count, "%u\n", obd->u.obt.obt_instance);
}
EXPORT_SYMBOL(lprocfs_target_rd_instance);

int lprocfs_osd_rd_blksize(char *page, char **start, off_t off,
			   int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		*eof = 1;
		rc = snprintf(page, count, "%d\n", (unsigned) osfs.os_bsize);
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_blksize);

int lprocfs_osd_rd_kbytestotal(char *page, char **start, off_t off,
			       int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		__u32 blk_size = osfs.os_bsize >> 10;
		__u64 result = osfs.os_blocks;

		while (blk_size >>= 1)
			result <<= 1;

		*eof = 1;
		rc = snprintf(page, count, LPU64 "\n", result);
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_kbytestotal);

int lprocfs_osd_rd_kbytesfree(char *page, char **start, off_t off,
			      int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		__u32 blk_size = osfs.os_bsize >> 10;
		__u64 result = osfs.os_bfree;

		while (blk_size >>= 1)
			result <<= 1;

		*eof = 1;
		rc = snprintf(page, count, LPU64 "\n", result);
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_kbytesfree);

int lprocfs_osd_rd_kbytesavail(char *page, char **start, off_t off,
			       int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		__u32 blk_size = osfs.os_bsize >> 10;
		__u64 result = osfs.os_bavail;

		while (blk_size >>= 1)
			result <<= 1;

		*eof = 1;
		rc = snprintf(page, count, LPU64 "\n", result);
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_kbytesavail);

int lprocfs_osd_rd_filestotal(char *page, char **start, off_t off,
			      int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		*eof = 1;
		rc = snprintf(page, count, LPU64 "\n", osfs.os_files);
	}

	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_filestotal);

int lprocfs_osd_rd_filesfree(char *page, char **start, off_t off,
			     int count, int *eof, void *data)
{
	struct dt_device *dt = data;
	struct obd_statfs osfs;
	int rc = dt_statfs(NULL, dt, &osfs);
	if (!rc) {
		*eof = 1;
		rc = snprintf(page, count, LPU64 "\n", osfs.os_ffree);
	}
	return rc;
}
EXPORT_SYMBOL(lprocfs_osd_rd_filesfree);
#endif /* LPROCFS */
