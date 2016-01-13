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
 * Copyright (c) 2016, Commissariat a l'Energie Atomique et aux Energies
 *                     Alternatives.
 */

#define DEBUG_SUBSYSTEM S_MDC

#include <linux/init.h>
#include <linux/kthread.h>
#include <linux/poll.h>
#include <linux/miscdevice.h>

#include <lustre_log.h>

#include "mdc_internal.h"


/*
 * -- Changelog delivery through character device --
 */

/**
 * Mutex to protect chlg_exposed_devices below
 */
static DEFINE_MUTEX(obd_dev_list_lock);

/**
 * Global linked list of all registered devices (one per MDT).
 */
static LIST_HEAD(chlg_exposed_devices);


struct chlg_exposed_dev {
	/* Device name of the form "/dev/changelog-{MDTNAME}" */
	char			ced_name[32];
	/* Misc device descriptor */
	struct miscdevice	ced_misc;
	/* OBDs referencing this device (multiple mount point) */
	struct list_head	ced_obds;
	/* Reference counter for proper deregistration */
	struct kref		ced_refs;
	/* Link within the global chlg_exposed_devices */
	struct list_head	ced_link;
};

struct chlg_reader_state {
	/* Shortcut to the corresponding OBD device */
	struct obd_device	*crs_obd;
	/* EOF no more records available */
	bool			 crs_eof;
	/* Userland reader closed connection */
	bool			 crs_closed;
	/* Desired start position */
	__u64			 crs_start_offset;
	/* Wait queue for the catalog processing thread */
	wait_queue_head_t	 crs_waitq_prod;
	/* Wait queue for the record copy threads */
	wait_queue_head_t	 crs_waitq_cons;
	/* Mutex protecting the list below */
	struct mutex		 crs_lock;
	/* Number of item in the list */
	__u64			 crs_rec_count;
	/* Linked list of prefetched struct enqueued_record */
	struct list_head	 crs_rec_queue;
};

struct enqueued_record {
	/* Link within the chlg_reader_state::crs_rec_queue list */
	struct list_head	enq_linkage;
	/* Data (enq_record) field length */
	__u64			enq_length;
	/* Copy of a changelog record (see struct llog_changelog_rec) */
	struct changelog_rec	enq_record[];
};

enum {
	/* Number of records to prefetch locally. */
	CDEV_CHLG_MAX_PREFETCH = 1024,
};

static int chlg_read_cat_process_cb(const struct lu_env *env,
				    struct llog_handle *llh,
				    struct llog_rec_hdr *hdr, void *data)
{
	struct llog_changelog_rec	*rec;
	struct chlg_reader_state	*crs = data;
	struct enqueued_record		*enq;
	struct l_wait_info		 lwi = { 0 };
	size_t				 len;
	int				 rc;
	ENTRY;

	LASSERT(crs != NULL);
	LASSERT(hdr != NULL);

	rec = container_of(hdr, struct llog_changelog_rec, cr_hdr);

	if (rec->cr_hdr.lrh_type != CHANGELOG_REC) {
		rc = -EINVAL;
		CERROR("not a changelog rec %x/%d: rc = %d\n",
		       rec->cr_hdr.lrh_type, rec->cr.cr_type, rc);
		RETURN(rc);
	}

	/* Skip undesired records */
	if (rec->cr.cr_index < crs->crs_start_offset)
		RETURN(0);

	CDEBUG(D_HSM, LPU64" %02d%-5s "LPU64" 0x%x t="DFID" p="DFID" %.*s\n",
	       rec->cr.cr_index, rec->cr.cr_type,
	       changelog_type2str(rec->cr.cr_type), rec->cr.cr_time,
	       rec->cr.cr_flags & CLF_FLAGMASK,
	       PFID(&rec->cr.cr_tfid), PFID(&rec->cr.cr_pfid),
	       rec->cr.cr_namelen, changelog_rec_name(&rec->cr));

	l_wait_event(crs->crs_waitq_prod,
		     (crs->crs_rec_count < CDEV_CHLG_MAX_PREFETCH ||
		      crs->crs_closed), &lwi);

	if (crs->crs_closed)
		RETURN(LLOG_PROC_BREAK);

	len = changelog_rec_size(&rec->cr) + rec->cr.cr_namelen;
	OBD_ALLOC(enq, sizeof(*enq) + len);
	if (enq == NULL)
		RETURN(-ENOMEM);

	INIT_LIST_HEAD(&enq->enq_linkage);
	enq->enq_length = len;
	memcpy(enq->enq_record, &rec->cr, len);

	mutex_lock(&crs->crs_lock);
	list_add_tail(&enq->enq_linkage, &crs->crs_rec_queue);
	crs->crs_rec_count++;
	mutex_unlock(&crs->crs_lock);

	wake_up_all(&crs->crs_waitq_cons);

	RETURN(0);
}

static void crs_free(struct chlg_reader_state *crs)
{
	struct enqueued_record	*rec;
	struct enqueued_record	*tmp;

	list_for_each_entry_safe(rec, tmp, &crs->crs_rec_queue, enq_linkage) {
		list_del(&rec->enq_linkage);
		OBD_FREE(rec, sizeof(*rec) + rec->enq_length);
	}
	OBD_FREE_PTR(crs);
}

static int chlg_load(void *args)
{
	struct chlg_reader_state	*crs = args;
	struct obd_device		*obd = crs->crs_obd;
	struct llog_ctxt		*ctx = NULL;
	struct llog_handle		*llh = NULL;
	struct l_wait_info		 lwi = { 0 };
	int				 rc;
	ENTRY;

	ctx = llog_get_context(obd, LLOG_CHANGELOG_REPL_CTXT);
	if (ctx == NULL)
		GOTO(err_out, rc = -ENOENT);

	rc = llog_open(NULL, ctx, &llh, NULL, CHANGELOG_CATALOG,
		       LLOG_OPEN_EXISTS);
	if (rc) {
		CERROR("%s: fail to open changelog catalog: rc=%d\n",
		       obd->obd_name, rc);
		GOTO(err_out, rc);
	}

	rc = llog_init_handle(NULL, llh, LLOG_F_IS_CAT|LLOG_F_EXT_JOBID, NULL);
	if (rc) {
		CERROR("%s: fail to init llog handle: rc=%d\n",
		       obd->obd_name, rc);
		GOTO(err_out, rc);
	}

	rc = llog_cat_process(NULL, llh, chlg_read_cat_process_cb, crs, 0, 0);
	if (rc < 0) {
		CERROR("%s: fail to process llog: rc=%d\n", obd->obd_name, rc);
		GOTO(err_out, rc);
	}

err_out:
	crs->crs_eof = true;
	wake_up_all(&crs->crs_waitq_cons);

	if (llh != NULL)
		llog_cat_close(NULL, llh);

	if (ctx != NULL)
		llog_ctxt_put(ctx);

	l_wait_event(crs->crs_waitq_prod, crs->crs_closed, &lwi);
	crs_free(crs);
	RETURN(rc);
}

static ssize_t chlg_read(struct file *file, char __user *buff, size_t count,
			 loff_t *ppos)
{
	struct chlg_reader_state	*crs = file->private_data;
	struct enqueued_record		*rec;
	struct enqueued_record		*tmp;
	struct l_wait_info		 lwi = { 0 };
	__u64				 orig_count;
	ssize_t				 written_total = 0;
	LIST_HEAD(consumed);
	ENTRY;

	if (file->f_flags & O_NONBLOCK && crs->crs_rec_count == 0)
		RETURN(-EAGAIN);

	l_wait_event(crs->crs_waitq_cons,
		     crs->crs_rec_count > 0 || crs->crs_eof, &lwi);

	mutex_lock(&crs->crs_lock);
	orig_count = crs->crs_rec_count;
	list_for_each_entry_safe(rec, tmp, &crs->crs_rec_queue, enq_linkage) {
		if (written_total + rec->enq_length > count)
			break;

		if (copy_to_user(buff, rec->enq_record, rec->enq_length)) {
			list_splice_tail(&consumed, &crs->crs_rec_queue);
			crs->crs_rec_count = orig_count;
			mutex_unlock(&crs->crs_lock);
			RETURN(-EFAULT);
		}

		buff += rec->enq_length;
		written_total += rec->enq_length;

		crs->crs_rec_count--;
		list_del_init(&rec->enq_linkage);
		list_add_tail(&rec->enq_linkage, &consumed);
	}
	mutex_unlock(&crs->crs_lock);

	if (written_total > 0)
		wake_up_all(&crs->crs_waitq_prod);

	list_for_each_entry_safe(rec, tmp, &consumed, enq_linkage) {
		list_del(&rec->enq_linkage);
		OBD_FREE(rec, sizeof(*rec) + rec->enq_length);
		(*ppos)++;
	}

	RETURN(written_total);
}

static int chlg_set_start_offset(struct chlg_reader_state *crs, __u64 offset)
{
	struct enqueued_record	*rec;
	struct enqueued_record	*tmp;

	mutex_lock(&crs->crs_lock);
	if (offset < crs->crs_start_offset) {
		mutex_unlock(&crs->crs_lock);
		return -ERANGE;
	}

	crs->crs_start_offset = offset;
	list_for_each_entry_safe(rec, tmp, &crs->crs_rec_queue, enq_linkage) {
		struct changelog_rec *cr = rec->enq_record;

		if (cr->cr_index >= crs->crs_start_offset)
			break;

		crs->crs_rec_count--;
		list_del(&rec->enq_linkage);
		OBD_FREE(rec, rec->enq_length);
	}

	mutex_unlock(&crs->crs_lock);
	wake_up_all(&crs->crs_waitq_prod);
	return 0;
}

/**
 * Move read pointer to a certain record index, encoded as an offset.
 */
static loff_t chlg_llseek(struct file *file, loff_t off, int whence)
{
	struct chlg_reader_state	*crs = file->private_data;
	loff_t				 pos;
	int				 rc;

	switch (whence) {
	case SEEK_SET:
		pos = off;
		break;
	case SEEK_CUR:
		pos = file->f_pos + off;
		break;
	case SEEK_END:
	default:
		return -EINVAL;
	}

	/* We cannot go backward */
	if (pos < file->f_pos)
		return -EINVAL;

	rc = chlg_set_start_offset(crs, pos);
	if (rc != 0)
		return rc;

	file->f_pos = pos;
	return pos;
}

static int chlg_clear(struct chlg_reader_state *crs, __u32 reader, __u64 record)
{
	struct obd_device		*obd = crs->crs_obd;
	struct changelog_setinfo	 cs  = {
		.cs_recno = record,
		.cs_id    = reader
	};

	return obd_set_info_async(NULL, obd->obd_self_export,
				strlen(KEY_CHANGELOG_CLEAR),
				KEY_CHANGELOG_CLEAR, sizeof(cs), &cs, NULL);
}

/** Maximum changelog control command size */
#define CHLG_CONTROL_CMD_MAX	64

static ssize_t chlg_write(struct file *file, const char __user *buff,
			  size_t count, loff_t *off)
{
	struct chlg_reader_state	*crs = file->private_data;
	char				*kbuff;
	__u64				 record;
	__u32				 reader;
	int				 rc = 0;
	ENTRY;

	if (count > CHLG_CONTROL_CMD_MAX)
		RETURN(-EINVAL);

	OBD_ALLOC(kbuff, CHLG_CONTROL_CMD_MAX);
	if (kbuff == NULL)
		RETURN(-ENOMEM);

	if (copy_from_user(kbuff, buff, count))
		GOTO(out_free, rc = -EFAULT);

	kbuff[CHLG_CONTROL_CMD_MAX - 1] = '\0';

	if (sscanf(kbuff, "clear:cl%u:%llu", &reader, &record) == 2)
		rc = chlg_clear(crs, reader, record);
	else
		rc = -EINVAL;

	EXIT;
out_free:
	OBD_FREE(kbuff, CHLG_CONTROL_CMD_MAX);
	return rc < 0 ? rc : count;
}

static struct obd_device *chlg_obd_get(dev_t cdev)
{
	int			 minor = MINOR(cdev);
	struct obd_device	*obd = NULL;
	struct chlg_exposed_dev	*curr;

	mutex_lock(&obd_dev_list_lock);
	list_for_each_entry(curr, &chlg_exposed_devices, ced_link) {
		if (curr->ced_misc.minor == minor) {
			/* take the first available OBD device attached */
			obd = list_first_entry(&curr->ced_obds,
					       struct obd_device,
					       u.cli.cl_kuc_chain);
			break;
		}
	}
	mutex_unlock(&obd_dev_list_lock);
	return obd;
}

static int chlg_open(struct inode *inode, struct file *file)
{
	struct chlg_reader_state	*crs;
	struct obd_device		*obd = chlg_obd_get(inode->i_rdev);
	struct task_struct		*task;
	int				 rc;
	ENTRY;

	LASSERT(obd != NULL);

	OBD_ALLOC_PTR(crs);
	if (!crs)
		RETURN(-ENOMEM);

	crs->crs_obd = obd;
	crs->crs_eof = false;
	crs->crs_closed = false;

	mutex_init(&crs->crs_lock);
	INIT_LIST_HEAD(&crs->crs_rec_queue);
	init_waitqueue_head(&crs->crs_waitq_prod);
	init_waitqueue_head(&crs->crs_waitq_cons);

	if (file->f_mode & FMODE_READ) {
		task = kthread_run(chlg_load, crs, "chlg_load_thread");
		if (IS_ERR(task)) {
			rc = PTR_ERR(task);
			CERROR("%s: cannot start changelog thread: rc = %d\n",
			       obd->obd_name, rc);
			GOTO(err_out, rc);
		}
	}

	file->private_data = crs;
	RETURN(0);

err_out:
	OBD_FREE_PTR(crs);
	return rc;
}

static int chlg_release(struct inode *inode, struct file *file)
{
	struct chlg_reader_state	*crs = file->private_data;

	if (file->f_mode & FMODE_READ) {
		crs->crs_closed = true;
		wake_up_all(&crs->crs_waitq_prod);
	} else {
		/* No producer thread, release resource ourselve */
		crs_free(crs);
	}
	return 0;
}

static unsigned int chlg_poll(struct file *file, poll_table *wait)
{
	struct chlg_reader_state	*crs  = file->private_data;
	unsigned int			 mask = 0;

	mutex_lock(&crs->crs_lock);
	poll_wait(file, &crs->crs_waitq_cons, wait);
	if (crs->crs_rec_count > 0)
		mask |= POLLIN | POLLRDNORM;
	if (crs->crs_eof)
		mask |= POLLHUP;
	mutex_unlock(&crs->crs_lock);
	return mask;
}

static const struct file_operations chlg_fops = {
	.owner		= THIS_MODULE,
	.llseek		= chlg_llseek,
	.read		= chlg_read,
	.write		= chlg_write,
	.open		= chlg_open,
	.release	= chlg_release,
	.poll		= chlg_poll,
};

static void get_chlg_name(char *name, size_t name_len, struct obd_device *obd)
{
	char	*p = name;
	int	 i;

	snprintf(name, name_len, "changelog-%s", obd->obd_name);
	name[name_len - 1] = '\0';

	/* Find the 2nd '-' from the end and truncate on it */
	for (i = 0; i < 2; i++) {
		p = strrchr(name, '-');
		if (p == NULL)
			return;
		*p = '\0';
	}
}

static struct chlg_exposed_dev *
chlg_exposed_dev_find_by_name(const char *name)
{
	struct chlg_exposed_dev	*dit;

	list_for_each_entry(dit, &chlg_exposed_devices, ced_link)
		if (strcmp(name, dit->ced_name) == 0)
			return dit;
	return NULL;
}

/**
 * Find chlg_exposed_dev structure for a given OBD device.
 * This is bad O(n^2) but:
 *   - N is # of MDTs by mount points
 *   - this only runs at shutdown
 */
static struct chlg_exposed_dev *
chlg_exposed_dev_find_by_obd(const struct obd_device *obd)
{
	struct chlg_exposed_dev	*dit;
	struct obd_device	*oit;

	list_for_each_entry(dit, &chlg_exposed_devices, ced_link)
		list_for_each_entry(oit, &dit->ced_obds, u.cli.cl_kuc_chain)
			if (oit == obd)
				return dit;
	return NULL;
}

int mdc_changelog_cdev_init(struct obd_device *obd)
{
	struct chlg_exposed_dev	*exist;
	struct chlg_exposed_dev	*entry;
	int			 rc;
	ENTRY;

	OBD_ALLOC_PTR(entry);
	if (entry == NULL)
		RETURN(-ENOMEM);

	get_chlg_name(entry->ced_name, sizeof(entry->ced_name), obd);

	entry->ced_misc.minor = MISC_DYNAMIC_MINOR;
	entry->ced_misc.name  = entry->ced_name;
	entry->ced_misc.fops  = &chlg_fops;

	kref_init(&entry->ced_refs);
	INIT_LIST_HEAD(&entry->ced_obds);
	INIT_LIST_HEAD(&entry->ced_link);

	mutex_lock(&obd_dev_list_lock);
	exist = chlg_exposed_dev_find_by_name(entry->ced_name);
	if (exist != NULL) {
		kref_get(&exist->ced_refs);
		list_add_tail(&obd->u.cli.cl_kuc_chain, &exist->ced_obds);
		OBD_FREE_PTR(entry);
		GOTO(out_unlock, rc = 0);
	}

	/* Register new character device */
	rc = misc_register(&entry->ced_misc);
	if (rc != 0) {
		OBD_FREE_PTR(entry);
		GOTO(out_unlock, rc);
	}

	list_add_tail(&obd->u.cli.cl_kuc_chain, &entry->ced_obds);
	list_add_tail(&entry->ced_link, &chlg_exposed_devices);

out_unlock:
	mutex_unlock(&obd_dev_list_lock);
	RETURN(rc);
}

static void dev_clear(struct kref *kref)
{
	struct chlg_exposed_dev *entry = container_of(kref,
						      struct chlg_exposed_dev,
						      ced_refs);
	ENTRY;

	list_del(&entry->ced_link);
	misc_deregister(&entry->ced_misc);
	OBD_FREE_PTR(entry);
	EXIT;
}

void mdc_changelog_cdev_finish(struct obd_device *obd)
{
	struct chlg_exposed_dev	*entry = chlg_exposed_dev_find_by_obd(obd);
	ENTRY;

	mutex_lock(&obd_dev_list_lock);
	list_del(&obd->u.cli.cl_kuc_chain);
	kref_put(&entry->ced_refs, dev_clear);
	mutex_unlock(&obd_dev_list_lock);
	EXIT;
}

