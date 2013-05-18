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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/mm.h>
#include <linux/highmem.h>
#include <linux/pagemap.h>

#define DEBUG_SUBSYSTEM S_LLITE

#include <obd_support.h>
#include <lustre_lite.h>
#include <lustre_dlm.h>
#include "llite_internal.h"

#define SA_OMITTED_ENTRY_MAX 8ULL

typedef enum {
        /** negative values are for error cases */
        SA_ENTRY_INIT = 0,      /** init entry */
        SA_ENTRY_SUCC = 1,      /** stat succeed */
        SA_ENTRY_INVA = 2,      /** invalid entry */
        SA_ENTRY_DEST = 3,      /** entry to be destroyed */
} se_stat_t;

struct ll_sa_entry {
	/* link into sai->sai_entries */
	cfs_list_t              se_link;
	/* link into sai->sai_entries_{received,stated} */
	cfs_list_t              se_list;
	/* link into sai hash table locally */
	cfs_list_t              se_hash;
	/* entry index in the sai */
	__u64                   se_index;
	/* low layer ldlm lock handle */
	__u64                   se_handle;
	/* entry status */
	se_stat_t               se_stat;
	/* entry size, contains name */
	int                     se_size;
	/* pointer to async getattr enqueue info */
	struct md_enqueue_info *se_minfo;
	/* pointer to the async getattr request */
	struct ptlrpc_request  *se_req;
	/* pointer to the target inode */
	struct inode           *se_inode;
	/* entry name */
	struct qstr             se_qstr;
};

static unsigned int sai_generation = 0;
static DEFINE_SPINLOCK(sai_generation_lock);

static inline int ll_sa_entry_unhashed(struct ll_sa_entry *entry)
{
        return cfs_list_empty(&entry->se_hash);
}

/*
 * The entry only can be released by the caller, it is necessary to hold lock.
 */
static inline int ll_sa_entry_stated(struct ll_sa_entry *entry)
{
        smp_rmb();
        return (entry->se_stat != SA_ENTRY_INIT);
}

static inline int ll_sa_entry_hash(int val)
{
        return val & LL_SA_CACHE_MASK;
}

/*
 * Insert entry to hash SA table.
 */
static inline void
ll_sa_entry_enhash(struct ll_statahead_info *sai, struct ll_sa_entry *entry)
{
	int i = ll_sa_entry_hash(entry->se_qstr.hash);

	spin_lock(&sai->sai_cache_lock[i]);
	cfs_list_add_tail(&entry->se_hash, &sai->sai_cache[i]);
	spin_unlock(&sai->sai_cache_lock[i]);
}

/*
 * Remove entry from SA table.
 */
static inline void
ll_sa_entry_unhash(struct ll_statahead_info *sai, struct ll_sa_entry *entry)
{
	int i = ll_sa_entry_hash(entry->se_qstr.hash);

	spin_lock(&sai->sai_cache_lock[i]);
	cfs_list_del_init(&entry->se_hash);
	spin_unlock(&sai->sai_cache_lock[i]);
}

static inline int agl_should_run(struct ll_statahead_info *sai,
                                 struct inode *inode)
{
	return (inode != NULL && S_ISREG(inode->i_mode) && sai->sai_agl_valid);
}

static inline struct ll_sa_entry *
sa_first_received_entry(struct ll_statahead_info *sai)
{
        return cfs_list_entry(sai->sai_entries_received.next,
                              struct ll_sa_entry, se_list);
}

static inline struct ll_inode_info *
agl_first_entry(struct ll_statahead_info *sai)
{
        return cfs_list_entry(sai->sai_entries_agl.next,
                              struct ll_inode_info, lli_agl_list);
}

static inline int sa_sent_full(struct ll_statahead_info *sai)
{
        return cfs_atomic_read(&sai->sai_cache_count) >= sai->sai_max;
}

static inline int sa_received_empty(struct ll_statahead_info *sai)
{
        return cfs_list_empty(&sai->sai_entries_received);
}

static inline int agl_list_empty(struct ll_statahead_info *sai)
{
        return cfs_list_empty(&sai->sai_entries_agl);
}

/**
 * (1) hit ratio less than 80%
 * or
 * (2) consecutive miss more than 8
 * then means low hit.
 */
static inline int sa_low_hit(struct ll_statahead_info *sai)
{
        return ((sai->sai_hit > 7 && sai->sai_hit < 4 * sai->sai_miss) ||
                (sai->sai_consecutive_miss > 8));
}

/*
 * If the given index is behind of statahead window more than
 * SA_OMITTED_ENTRY_MAX, then it is old.
 */
static inline int is_omitted_entry(struct ll_statahead_info *sai, __u64 index)
{
        return ((__u64)sai->sai_max + index + SA_OMITTED_ENTRY_MAX <
                 sai->sai_index);
}

/*
 * Insert it into sai_entries tail when init.
 */
static struct ll_sa_entry *
ll_sa_entry_alloc(struct ll_statahead_info *sai, __u64 index,
                  const char *name, int len)
{
        struct ll_inode_info *lli;
        struct ll_sa_entry   *entry;
        int                   entry_size;
        char                 *dname;
        ENTRY;

        entry_size = sizeof(struct ll_sa_entry) + (len & ~3) + 4;
        OBD_ALLOC(entry, entry_size);
        if (unlikely(entry == NULL))
                RETURN(ERR_PTR(-ENOMEM));

	CDEBUG(D_READA, "alloc sa entry %.*s(%p) index "LPU64"\n",
	       len, name, entry, index);

        entry->se_index = index;

	/*
	 * NB sa_entry is not refcounted, because only directory scanner can
	 * remove entry, and it only removes stated entries, that means
	 * after ll_post_statahead() the entries cannot be touched except the
	 * scanner.
	 */
        entry->se_stat = SA_ENTRY_INIT;
        entry->se_size = entry_size;
        dname = (char *)entry + sizeof(struct ll_sa_entry);
        memcpy(dname, name, len);
        dname[len] = 0;
        entry->se_qstr.hash = full_name_hash(name, len);
        entry->se_qstr.len = len;
        entry->se_qstr.name = dname;

	lli = ll_i2info(sai->sai_dentry->d_inode);

	spin_lock(&lli->lli_sa_lock);
	cfs_list_add_tail(&entry->se_link, &sai->sai_entries);
	CFS_INIT_LIST_HEAD(&entry->se_list);
	ll_sa_entry_enhash(sai, entry);
	spin_unlock(&lli->lli_sa_lock);

	cfs_atomic_inc(&sai->sai_cache_count);

	RETURN(entry);
}

/*
 * Used by the directory scanner to search entry with name.
 *
 * Only the caller can remove the entry from hash, so it is unnecessary to hold
 * hash lock. It is caller's duty to release the init refcount on the entry, so
 * it is also unnecessary to increase refcount on the entry.
 */
static struct ll_sa_entry *
ll_sa_entry_get_byname(struct ll_statahead_info *sai, const struct qstr *qstr)
{
        struct ll_sa_entry *entry;
        int i = ll_sa_entry_hash(qstr->hash);

        cfs_list_for_each_entry(entry, &sai->sai_cache[i], se_hash) {
                if (entry->se_qstr.hash == qstr->hash &&
                    entry->se_qstr.len == qstr->len &&
                    memcmp(entry->se_qstr.name, qstr->name, qstr->len) == 0)
                        return entry;
        }
        return NULL;
}

static void ll_sa_entry_cleanup(struct ll_statahead_info *sai,
                                 struct ll_sa_entry *entry)
{
        struct md_enqueue_info *minfo = entry->se_minfo;
        struct ptlrpc_request  *req   = entry->se_req;

        if (minfo) {
                entry->se_minfo = NULL;
                ll_intent_release(&minfo->mi_it);
                iput(minfo->mi_dir);
                OBD_FREE_PTR(minfo);
        }

        if (req) {
                entry->se_req = NULL;
                ptlrpc_req_finished(req);
        }
}

static void ll_sa_entry_free(struct ll_statahead_info *sai,
                             struct ll_sa_entry *entry)
{
	CDEBUG(D_READA, "free sa entry %.*s(%p) index "LPU64"\n",
	       entry->se_qstr.len, entry->se_qstr.name, entry,
	       entry->se_index);

	LASSERT(cfs_list_empty(&entry->se_link));
	LASSERT(cfs_list_empty(&entry->se_list));
	LASSERT(ll_sa_entry_unhashed(entry));

	ll_sa_entry_cleanup(sai, entry);
	if (entry->se_inode)
		iput(entry->se_inode);

	OBD_FREE(entry, entry->se_size);
	cfs_atomic_dec(&sai->sai_cache_count);
}

static inline void
do_sa_entry_fini(struct ll_statahead_info *sai, struct ll_sa_entry *entry)
{
	struct ll_inode_info *lli = ll_i2info(sai->sai_dentry->d_inode);

	LASSERT(!ll_sa_entry_unhashed(entry));
	LASSERT(!cfs_list_empty(&entry->se_link));

	ll_sa_entry_unhash(sai, entry);

	spin_lock(&lli->lli_sa_lock);
	entry->se_stat = SA_ENTRY_DEST;
	cfs_list_del_init(&entry->se_link);
	if (likely(!cfs_list_empty(&entry->se_list)))
		cfs_list_del_init(&entry->se_list);
	spin_unlock(&lli->lli_sa_lock);

	ll_sa_entry_free(sai, entry);
}

/*
 * Delete it from sai_entries_stated list when fini.
 */
static void
ll_sa_entry_fini(struct ll_statahead_info *sai, struct ll_sa_entry *entry)
{
	struct ll_sa_entry *pos, *next;

	if (entry)
		do_sa_entry_fini(sai, entry);

	/* drop old entry, only 'scanner' process does this, no need to lock */
	cfs_list_for_each_entry_safe(pos, next, &sai->sai_entries, se_link) {
		if (!is_omitted_entry(sai, pos->se_index))
			break;
		/* keep those whose statahead RPC not finished */
		if (pos->se_stat == SA_ENTRY_SUCC ||
		    pos->se_stat == SA_ENTRY_INVA)
			do_sa_entry_fini(sai, pos);
	}
}

/*
 * Inside lli_sa_lock.
 */
static void
do_sa_entry_to_stated(struct ll_statahead_info *sai,
		      struct ll_sa_entry *entry, se_stat_t stat)
{
	struct ll_sa_entry *se;
	cfs_list_t         *pos = &sai->sai_entries_stated;

	if (!cfs_list_empty(&entry->se_list))
		cfs_list_del_init(&entry->se_list);

	cfs_list_for_each_entry_reverse(se, &sai->sai_entries_stated, se_list) {
		if (se->se_index < entry->se_index) {
			pos = &se->se_list;
			break;
		}
	}

	cfs_list_add(&entry->se_list, pos);
	entry->se_stat = stat;
}

/*
 * Move entry to sai_entries_stated and sort with the index.
 * \retval 1    -- entry to be destroyed.
 * \retval 0    -- entry is inserted into stated list.
 */
static int
ll_sa_entry_to_stated(struct ll_statahead_info *sai,
		      struct ll_sa_entry *entry, se_stat_t stat)
{
	struct ll_inode_info *lli = ll_i2info(sai->sai_dentry->d_inode);
	int                   ret = 1;

	ll_sa_entry_cleanup(sai, entry);

	spin_lock(&lli->lli_sa_lock);
	if (likely(entry->se_stat != SA_ENTRY_DEST)) {
		do_sa_entry_to_stated(sai, entry, stat);
		ret = 0;
	}
	spin_unlock(&lli->lli_sa_lock);

	return ret;
}

/*
 * Insert inode into the list of sai_entries_agl.
 */
static void ll_agl_add(struct ll_statahead_info *sai,
                       struct inode *inode, int index)
{
	struct ll_inode_info *child  = ll_i2info(inode);
	struct ll_inode_info *parent = ll_i2info(sai->sai_dentry->d_inode);
	int                   added  = 0;

	spin_lock(&child->lli_agl_lock);
	if (child->lli_agl_index == 0) {
		child->lli_agl_index = index;
		spin_unlock(&child->lli_agl_lock);

		LASSERT(cfs_list_empty(&child->lli_agl_list));

		igrab(inode);
		spin_lock(&parent->lli_agl_lock);
		if (agl_list_empty(sai))
			added = 1;
		cfs_list_add_tail(&child->lli_agl_list, &sai->sai_entries_agl);
		spin_unlock(&parent->lli_agl_lock);
	} else {
		spin_unlock(&child->lli_agl_lock);
	}

	if (added > 0)
		wake_up(&sai->sai_agl_thread.t_ctl_waitq);
}

static struct ll_statahead_info *ll_sai_alloc(struct dentry *dentry)
{
	struct ll_statahead_info *sai;
	struct ll_inode_info	 *lli = ll_i2info(dentry->d_inode);
	int                       i;

	OBD_ALLOC_PTR(sai);
	if (!sai)
		return NULL;

	sai->sai_dentry = dget(dentry);
	cfs_atomic_set(&sai->sai_refcount, 1);
	sai->sai_max = LL_SA_RPC_MIN;
	sai->sai_index = 1;
	init_waitqueue_head(&sai->sai_waitq);
	init_waitqueue_head(&sai->sai_thread.t_ctl_waitq);
	init_waitqueue_head(&sai->sai_agl_thread.t_ctl_waitq);

	CFS_INIT_LIST_HEAD(&sai->sai_entries);
	CFS_INIT_LIST_HEAD(&sai->sai_entries_received);
	CFS_INIT_LIST_HEAD(&sai->sai_entries_stated);
	CFS_INIT_LIST_HEAD(&sai->sai_entries_agl);

	for (i = 0; i < LL_SA_CACHE_SIZE; i++) {
		CFS_INIT_LIST_HEAD(&sai->sai_cache[i]);
		spin_lock_init(&sai->sai_cache_lock[i]);
	}
	cfs_atomic_set(&sai->sai_cache_count, 0);

	spin_lock(&sai_generation_lock);
	lli->lli_sa_generation = ++sai_generation;
	if (unlikely(sai_generation == 0))
		lli->lli_sa_generation = ++sai_generation;
	spin_unlock(&sai_generation_lock);

	lli->lli_sai = sai;
	atomic_inc(&ll_i2sbi(dentry->d_inode)->ll_sa_running);

	return sai;
}

static inline struct ll_statahead_info *ll_sai_get(struct inode *dir)
{
	struct ll_inode_info *lli = ll_i2info(dir);
	struct ll_statahead_info *sai = NULL;

	spin_lock(&lli->lli_sa_lock);
	sai = lli->lli_sai;
	if (sai != NULL)
		atomic_inc(&sai->sai_refcount);
	spin_unlock(&lli->lli_sa_lock);

	return sai;
}

static void ll_sai_put(struct ll_statahead_info *sai)
{
	struct ll_inode_info *lli = ll_i2info(sai->sai_dentry->d_inode);
	ENTRY;

	if (atomic_dec_and_lock(&sai->sai_refcount, &lli->lli_sa_lock)) {
		struct ll_sa_entry *entry, *next;
		struct ll_sb_info *sbi = ll_i2sbi(sai->sai_dentry->d_inode);

		lli->lli_sai = NULL;
		spin_unlock(&lli->lli_sa_lock);

		LASSERT(thread_is_stopped(&sai->sai_thread));
		LASSERT(thread_is_stopped(&sai->sai_agl_thread));
		LASSERT(sai->sai_sent == sai->sai_replied);

		cfs_list_for_each_entry_safe(entry, next,
					     &sai->sai_entries, se_link)
			do_sa_entry_fini(sai, entry);

		LASSERT(list_empty(&sai->sai_entries));
		LASSERT(sa_received_empty(sai));
		LASSERT(list_empty(&sai->sai_entries_stated));

		LASSERT(atomic_read(&sai->sai_cache_count) == 0);
		LASSERT(agl_list_empty(sai));
		LASSERT(atomic_read(&sai->sai_refcount) == 0);

		dput(sai->sai_dentry);
		OBD_FREE_PTR(sai);
		atomic_dec(&sbi->ll_sa_running);
	}

	EXIT;
}

/* Do NOT forget to drop inode refcount when into sai_entries_agl. */
static void ll_agl_trigger(struct inode *inode, struct ll_statahead_info *sai)
{
        struct ll_inode_info *lli   = ll_i2info(inode);
        __u64                 index = lli->lli_agl_index;
        int                   rc;
        ENTRY;

        LASSERT(cfs_list_empty(&lli->lli_agl_list));

        /* AGL maybe fall behind statahead with one entry */
        if (is_omitted_entry(sai, index + 1)) {
                lli->lli_agl_index = 0;
                iput(inode);
                RETURN_EXIT;
        }

        /* Someone is in glimpse (sync or async), do nothing. */
	rc = down_write_trylock(&lli->lli_glimpse_sem);
        if (rc == 0) {
                lli->lli_agl_index = 0;
                iput(inode);
                RETURN_EXIT;
        }

        /*
         * Someone triggered glimpse within 1 sec before.
         * 1) The former glimpse succeeded with glimpse lock granted by OST, and
         *    if the lock is still cached on client, AGL needs to do nothing. If
         *    it is cancelled by other client, AGL maybe cannot obtaion new lock
         *    for no glimpse callback triggered by AGL.
         * 2) The former glimpse succeeded, but OST did not grant glimpse lock.
         *    Under such case, it is quite possible that the OST will not grant
         *    glimpse lock for AGL also.
         * 3) The former glimpse failed, compared with other two cases, it is
         *    relative rare. AGL can ignore such case, and it will not muchly
         *    affect the performance.
         */
        if (lli->lli_glimpse_time != 0 &&
            cfs_time_before(cfs_time_shift(-1), lli->lli_glimpse_time)) {
		up_write(&lli->lli_glimpse_sem);
                lli->lli_agl_index = 0;
                iput(inode);
                RETURN_EXIT;
        }

        CDEBUG(D_READA, "Handling (init) async glimpse: inode = "
               DFID", idx = "LPU64"\n", PFID(&lli->lli_fid), index);

        cl_agl(inode);
        lli->lli_agl_index = 0;
        lli->lli_glimpse_time = cfs_time_current();
	up_write(&lli->lli_glimpse_sem);

        CDEBUG(D_READA, "Handled (init) async glimpse: inode= "
               DFID", idx = "LPU64", rc = %d\n",
               PFID(&lli->lli_fid), index, rc);

        iput(inode);

        EXIT;
}

static void ll_post_statahead(struct ll_statahead_info *sai)
{
	struct inode		*dir = sai->sai_dentry->d_inode;
	struct inode		*child;
	struct ll_inode_info	*lli = ll_i2info(dir);
	struct ll_sa_entry	*entry;
	struct md_enqueue_info	*minfo;
	struct lookup_intent	*it;
	struct ptlrpc_request	*req;
	struct mdt_body		*body;
	__u64			 index;
	int			 rc    = 0;
	ENTRY;

	spin_lock(&lli->lli_sa_lock);
	if (unlikely(sa_received_empty(sai))) {
		spin_unlock(&lli->lli_sa_lock);
		RETURN_EXIT;
	}
	entry = sa_first_received_entry(sai);
	cfs_list_del_init(&entry->se_list);
	spin_unlock(&lli->lli_sa_lock);

        LASSERT(entry->se_handle != 0);

        minfo = entry->se_minfo;
        it = &minfo->mi_it;
        req = entry->se_req;
        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
        if (body == NULL)
                GOTO(out, rc = -EFAULT);

        child = entry->se_inode;
        if (child == NULL) {
                /*
                 * lookup.
                 */
                LASSERT(fid_is_zero(&minfo->mi_data.op_fid2));

                /* XXX: No fid in reply, this is probaly cross-ref case.
                 * SA can't handle it yet. */
                if (body->valid & OBD_MD_MDS)
                        GOTO(out, rc = -EAGAIN);
        } else {
                /*
                 * revalidate.
                 */
                /* unlinked and re-created with the same name */
                if (unlikely(!lu_fid_eq(&minfo->mi_data.op_fid2, &body->fid1))){
                        entry->se_inode = NULL;
                        iput(child);
                        child = NULL;
                }
        }

        it->d.lustre.it_lock_handle = entry->se_handle;
	rc = md_revalidate_lock(ll_i2mdexp(dir), it, ll_inode2fid(dir), NULL);
        if (rc != 1)
                GOTO(out, rc = -EAGAIN);

        rc = ll_prep_inode(&child, req, dir->i_sb, it);
        if (rc)
                GOTO(out, rc);

	CDEBUG(D_DLMTRACE, "%s: setting l_data to inode "DFID"(%p)\n",
	       ll_get_fsname(child->i_sb, NULL, 0),
	       PFID(ll_inode2fid(child)), child);
        ll_set_lock_data(ll_i2sbi(dir)->ll_md_exp, child, it, NULL);

        entry->se_inode = child;

        if (agl_should_run(sai, child))
                ll_agl_add(sai, child, entry->se_index);

        EXIT;

out:
	/* The "ll_sa_entry_to_stated()" will drop related ldlm ibits lock
	 * reference count by calling "ll_intent_drop_lock()" in spite of the
	 * above operations failed or not. Do not worry about calling
	 * "ll_intent_drop_lock()" more than once. */
	index = entry->se_index;
	rc = ll_sa_entry_to_stated(sai, entry,
				   rc < 0 ? SA_ENTRY_INVA : SA_ENTRY_SUCC);
	if (rc == 0 && index == sai->sai_index_wait)
		wake_up(&sai->sai_waitq);
}

static int ll_statahead_interpret(struct ptlrpc_request *req,
				  struct md_enqueue_info *minfo, int rc)
{
	struct lookup_intent	 *it  = &minfo->mi_it;
	struct inode		 *dir = minfo->mi_dir;
	struct ll_inode_info	 *lli = ll_i2info(dir);
	struct ll_statahead_info *sai = ll_sai_get(dir);
	struct ll_sa_entry	 *entry =
					(struct ll_sa_entry *)minfo->mi_cbdata;
	bool			  wakeup;
	__u64			  handle = 0;
	ENTRY;

	LASSERT(sai != NULL);
	LASSERT(!thread_is_stopped(&sai->sai_thread));

	if (it_disposition(it, DISP_LOOKUP_NEG))
		rc = -ENOENT;

	if (rc == 0) {
		/*
		 * Release the async ibits lock ASAP to avoid deadlock
		 * when statahead thread tries to enqueue lock on parent
		 * for readpage and other tries to enqueue lock on child
		 * with parent's lock held, for example: unlink.
		 *
		 * ll_intent_drop_lock() may sleep, call it outside of
		 * lli_sa_lock.
		 */
		handle = it->d.lustre.it_lock_handle;
		ll_intent_drop_lock(it);
	} else {
		ll_intent_release(it);
		iput(dir);
		OBD_FREE_PTR(minfo);
	}

	spin_lock(&lli->lli_sa_lock);
	if (rc == 0) {
		entry->se_handle = handle;
		entry->se_minfo = minfo;
		entry->se_req = ptlrpc_request_addref(req);
		wakeup = sa_received_empty(sai);
		cfs_list_add_tail(&entry->se_list,
				  &sai->sai_entries_received);
	} else {
		wakeup = (entry->se_index == sai->sai_index_wait);
		do_sa_entry_to_stated(sai, entry, SA_ENTRY_INVA);
	}
	sai->sai_replied++;
	spin_unlock(&lli->lli_sa_lock);

	if (wakeup)
		wake_up(&sai->sai_thread.t_ctl_waitq);
	ll_sai_put(sai);

	RETURN(rc);
}

static void sa_args_fini(struct md_enqueue_info *minfo,
                         struct ldlm_enqueue_info *einfo)
{
        LASSERT(minfo && einfo);
        iput(minfo->mi_dir);
        capa_put(minfo->mi_data.op_capa1);
        capa_put(minfo->mi_data.op_capa2);
        OBD_FREE_PTR(minfo);
        OBD_FREE_PTR(einfo);
}

/**
 * There is race condition between "capa_put" and "ll_statahead_interpret" for
 * accessing "op_data.op_capa[1,2]" as following:
 * "capa_put" releases "op_data.op_capa[1,2]"'s reference count after calling
 * "md_intent_getattr_async". But "ll_statahead_interpret" maybe run first, and
 * fill "op_data.op_capa[1,2]" as POISON, then cause "capa_put" access invalid
 * "ocapa". So here reserve "op_data.op_capa[1,2]" in "pcapa" before calling
 * "md_intent_getattr_async".
 */
static int sa_args_init(struct inode *dir, struct inode *child,
                        struct ll_sa_entry *entry, struct md_enqueue_info **pmi,
                        struct ldlm_enqueue_info **pei,
                        struct obd_capa **pcapa)
{
        struct qstr              *qstr = &entry->se_qstr;
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct md_op_data        *op_data;

        OBD_ALLOC_PTR(einfo);
        if (einfo == NULL)
                return -ENOMEM;

        OBD_ALLOC_PTR(minfo);
        if (minfo == NULL) {
                OBD_FREE_PTR(einfo);
                return -ENOMEM;
        }

        op_data = ll_prep_md_op_data(&minfo->mi_data, dir, child, qstr->name,
                                     qstr->len, 0, LUSTRE_OPC_ANY, NULL);
        if (IS_ERR(op_data)) {
                OBD_FREE_PTR(einfo);
                OBD_FREE_PTR(minfo);
                return PTR_ERR(op_data);
        }

	minfo->mi_it.it_op = IT_GETATTR;
	minfo->mi_dir = igrab(dir);
	minfo->mi_cb = ll_statahead_interpret;
	minfo->mi_cbdata = entry;

        einfo->ei_type   = LDLM_IBITS;
        einfo->ei_mode   = it_to_lock_mode(&minfo->mi_it);
        einfo->ei_cb_bl  = ll_md_blocking_ast;
        einfo->ei_cb_cp  = ldlm_completion_ast;
        einfo->ei_cb_gl  = NULL;
        einfo->ei_cbdata = NULL;

        *pmi = minfo;
        *pei = einfo;
        pcapa[0] = op_data->op_capa1;
        pcapa[1] = op_data->op_capa2;

        return 0;
}

static int do_sa_lookup(struct inode *dir, struct ll_sa_entry *entry)
{
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct obd_capa          *capas[2];
        int                       rc;
        ENTRY;

        rc = sa_args_init(dir, NULL, entry, &minfo, &einfo, capas);
        if (rc)
                RETURN(rc);

        rc = md_intent_getattr_async(ll_i2mdexp(dir), minfo, einfo);
        if (!rc) {
                capa_put(capas[0]);
                capa_put(capas[1]);
        } else {
                sa_args_fini(minfo, einfo);
        }

        RETURN(rc);
}

/**
 * similar to ll_revalidate_it().
 * \retval      1 -- dentry valid
 * \retval      0 -- will send stat-ahead request
 * \retval others -- prepare stat-ahead request failed
 */
static int do_sa_revalidate(struct inode *dir, struct ll_sa_entry *entry,
                            struct dentry *dentry)
{
        struct inode             *inode = dentry->d_inode;
        struct lookup_intent      it = { .it_op = IT_GETATTR,
                                         .d.lustre.it_lock_handle = 0 };
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct obd_capa          *capas[2];
        int rc;
        ENTRY;

        if (unlikely(inode == NULL))
                RETURN(1);

        if (d_mountpoint(dentry))
                RETURN(1);

        entry->se_inode = igrab(inode);
        rc = md_revalidate_lock(ll_i2mdexp(dir), &it, ll_inode2fid(inode),NULL);
        if (rc == 1) {
                entry->se_handle = it.d.lustre.it_lock_handle;
                ll_intent_release(&it);
                RETURN(1);
        }

        rc = sa_args_init(dir, inode, entry, &minfo, &einfo, capas);
        if (rc) {
                entry->se_inode = NULL;
                iput(inode);
                RETURN(rc);
        }

        rc = md_intent_getattr_async(ll_i2mdexp(dir), minfo, einfo);
        if (!rc) {
                capa_put(capas[0]);
                capa_put(capas[1]);
        } else {
                entry->se_inode = NULL;
                iput(inode);
                sa_args_fini(minfo, einfo);
        }

        RETURN(rc);
}

static void ll_statahead_one(struct dentry *parent, const char* entry_name,
                             int entry_name_len)
{
        struct inode             *dir    = parent->d_inode;
        struct ll_inode_info     *lli    = ll_i2info(dir);
        struct ll_statahead_info *sai    = lli->lli_sai;
        struct dentry            *dentry = NULL;
        struct ll_sa_entry       *entry;
        int                       rc;
        int                       rc1;
        ENTRY;

        entry = ll_sa_entry_alloc(sai, sai->sai_index, entry_name,
                                  entry_name_len);
        if (IS_ERR(entry))
                RETURN_EXIT;

        dentry = d_lookup(parent, &entry->se_qstr);
        if (!dentry) {
                rc = do_sa_lookup(dir, entry);
        } else {
                rc = do_sa_revalidate(dir, entry, dentry);
                if (rc == 1 && agl_should_run(sai, dentry->d_inode))
                        ll_agl_add(sai, dentry->d_inode, entry->se_index);
        }

        if (dentry != NULL)
                dput(dentry);

	if (rc) {
		__u64 index = entry->se_index;

		rc1 = ll_sa_entry_to_stated(sai, entry,
					rc < 0 ? SA_ENTRY_INVA : SA_ENTRY_SUCC);
		if (rc1 == 0 && index == sai->sai_index_wait)
			wake_up(&sai->sai_waitq);
	} else {
		sai->sai_sent++;
	}

	sai->sai_index++;

	EXIT;
}

static int ll_agl_thread(void *arg)
{
	struct dentry            *parent = (struct dentry *)arg;
	struct inode             *dir    = parent->d_inode;
	struct ll_inode_info     *plli   = ll_i2info(dir);
	struct ll_inode_info     *clli;
	struct ll_sb_info        *sbi    = ll_i2sbi(dir);
	struct ll_statahead_info *sai    = plli->lli_sai;
	struct ptlrpc_thread     *thread = &sai->sai_agl_thread;
	struct l_wait_info        lwi    = { 0 };
	ENTRY;

	CDEBUG(D_READA, "agl thread started: [pid %d] [parent %.*s]\n",
	       current_pid(), parent->d_name.len, parent->d_name.name);

	atomic_inc(&sbi->ll_agl_total);
	spin_lock(&plli->lli_agl_lock);
	sai->sai_agl_valid = 1;
	thread_set_flags(thread, SVC_RUNNING);
	spin_unlock(&plli->lli_agl_lock);
	wake_up(&thread->t_ctl_waitq);

        while (1) {
                l_wait_event(thread->t_ctl_waitq,
                             !agl_list_empty(sai) ||
                             !thread_is_running(thread),
                             &lwi);

                if (!thread_is_running(thread))
                        break;

		spin_lock(&plli->lli_agl_lock);
		/* The statahead thread maybe help to process AGL entries,
		 * so check whether list empty again. */
		if (!agl_list_empty(sai)) {
			clli = agl_first_entry(sai);
			cfs_list_del_init(&clli->lli_agl_list);
			spin_unlock(&plli->lli_agl_lock);
			ll_agl_trigger(&clli->lli_vfs_inode, sai);
		} else {
			spin_unlock(&plli->lli_agl_lock);
		}
	}

	spin_lock(&plli->lli_agl_lock);
	sai->sai_agl_valid = 0;
	while (!agl_list_empty(sai)) {
		clli = agl_first_entry(sai);
		cfs_list_del_init(&clli->lli_agl_list);
		spin_unlock(&plli->lli_agl_lock);
		clli->lli_agl_index = 0;
		iput(&clli->lli_vfs_inode);
		spin_lock(&plli->lli_agl_lock);
	}
	thread_set_flags(thread, SVC_STOPPED);
	spin_unlock(&plli->lli_agl_lock);
	wake_up(&thread->t_ctl_waitq);
	CDEBUG(D_READA, "agl thread for "DFID" stopped.\n",
	       PFID(&plli->lli_fid));
	RETURN(0);
}

static void ll_start_agl(struct dentry *parent, struct ll_statahead_info *sai)
{
	struct ptlrpc_thread *thread = &sai->sai_agl_thread;
	struct l_wait_info    lwi    = { 0 };
	struct ll_inode_info  *plli;
	struct task_struct	      *task;
	ENTRY;

	CDEBUG(D_READA, "start agl thread: [pid %d] [parent %.*s]\n",
	       current_pid(), parent->d_name.len, parent->d_name.name);

	plli = ll_i2info(parent->d_inode);
	task = kthread_run(ll_agl_thread, parent,
			       "ll_agl_%u", plli->lli_opendir_pid);
	if (IS_ERR(task)) {
		CERROR("can't start ll_agl thread, rc: %ld\n", PTR_ERR(task));
		thread_set_flags(thread, SVC_STOPPED);
		RETURN_EXIT;
	}

	l_wait_event(thread->t_ctl_waitq,
		     thread_is_running(thread) || thread_is_stopped(thread),
		     &lwi);
	EXIT;
}

static int ll_statahead_thread(void *arg)
{
	struct dentry            *parent = (struct dentry *)arg;
	struct inode             *dir    = parent->d_inode;
	struct ll_inode_info     *plli   = ll_i2info(dir);
	struct ll_inode_info     *clli;
	struct ll_sb_info        *sbi    = ll_i2sbi(dir);
	struct ll_statahead_info *sai    = ll_sai_get(dir);
	struct ptlrpc_thread     *thread = &sai->sai_thread;
	struct ptlrpc_thread *agl_thread = &sai->sai_agl_thread;
	int                       first  = 0;
	int                       rc     = 0;
	struct md_op_data	 *op_data;
	struct ll_dir_chain       chain;
	struct l_wait_info        lwi    = { 0 };
	struct lu_dirent	 *ent;
	struct page		 *page = NULL;
	cfs_time_t		  deadline;
	ENTRY;

	CDEBUG(D_READA, "statahead thread started: parent %.*s "DFID".\n",
	       parent->d_name.len, parent->d_name.name, PFID(&plli->lli_fid));

	op_data = ll_prep_md_op_data(NULL, dir, dir, NULL, 0, 0,
				     LUSTRE_OPC_ANY, dir);
	if (IS_ERR(op_data))
		RETURN(PTR_ERR(op_data));

	op_data->op_hash_offset = 0;
	op_data->op_max_pages =
		ll_i2sbi(dir)->ll_md_brw_size >> PAGE_CACHE_SHIFT;

	if (sbi->ll_flags & LL_SBI_AGL_ENABLED)
		ll_start_agl(parent, sai);

	atomic_inc(&sbi->ll_sa_total);

	spin_lock(&plli->lli_sa_lock);
	thread_set_flags(thread, SVC_RUNNING);
	spin_unlock(&plli->lli_sa_lock);
	wake_up(&thread->t_ctl_waitq);

	ll_dir_chain_init(&chain);
	for (ent = ll_dir_entry_start(dir, op_data, &page);
	     ent != NULL && !IS_ERR(ent);
	     ent = ll_dir_entry_next(dir, op_data, ent, &page)) {
		__u64 hash;
		int namelen;
		char *name;

		hash = le64_to_cpu(ent->lde_hash);
		if (unlikely(hash < op_data->op_hash_offset))
			/*
			 * Skip until we find target hash value.
			 */
			continue;

		namelen = le16_to_cpu(ent->lde_namelen);
		if (unlikely(namelen == 0))
			/*
			 * Skip dummy record.
			 */
			continue;

		name = ent->lde_name;
		if (name[0] == '.') {
			if (namelen == 1) {
				/*
				 * skip "."
				 */
				continue;
			} else if (name[1] == '.' && namelen == 2) {
				/*
				 * skip ".."
				 */
				continue;
			} else if (!sai->sai_ls_all) {
				/*
				 * skip hidden files.
				 */
				sai->sai_skip_hidden++;
				continue;
			}
		}

		/*
		 * don't stat-ahead first entry.
		 */
		if (unlikely(++first == 1))
			continue;

keep_it:
		l_wait_event(thread->t_ctl_waitq,
			     !sa_sent_full(sai) ||
			     !sa_received_empty(sai) ||
			     !agl_list_empty(sai) ||
			     !thread_is_running(thread),
			     &lwi);

interpret_it:
		while (!sa_received_empty(sai))
			ll_post_statahead(sai);

		if (sa_low_hit(sai)) {
			spin_lock(&plli->lli_sa_lock);
			thread_set_flags(thread, SVC_STOPPING);
			spin_unlock(&plli->lli_sa_lock);

			atomic_inc(&sbi->ll_sa_wrong);
			CDEBUG(D_READA, "Statahead for dir "DFID" hit "
				"ratio too low: hit/miss "LPU64"/"LPU64
				", sent/replied "LPU64"/"LPU64", "
				"stopping statahead thread.\n",
				PFID(&plli->lli_fid), sai->sai_hit,
				sai->sai_miss, sai->sai_sent,
				sai->sai_replied);
		}

		if (unlikely(!thread_is_running(thread)))
			GOTO(out, rc = 0);

		/* If no window for metadata statahead, but there are
		 * some AGL entries to be triggered, then try to help
		 * to process the AGL entries. */
		if (sa_sent_full(sai)) {
			spin_lock(&plli->lli_agl_lock);
			while (!agl_list_empty(sai)) {
				clli = agl_first_entry(sai);
				cfs_list_del_init(&clli->lli_agl_list);
				spin_unlock(&plli->lli_agl_lock);
				ll_agl_trigger(&clli->lli_vfs_inode,
					       sai);

				if (!sa_received_empty(sai))
					goto interpret_it;

				if (unlikely(!thread_is_running(thread)))
					GOTO(out, rc = 0);

				if (!sa_sent_full(sai))
					goto do_it;

				spin_lock(&plli->lli_agl_lock);
			}
			spin_unlock(&plli->lli_agl_lock);

			goto keep_it;
		}

do_it:
		ll_statahead_one(parent, name, namelen);
	}

	if (page != NULL) {
		kunmap(page);
		page_cache_release(page);
	}

	/*
	 * End of directory reached.
	 */

	/* wait 3 sec for ll_file_release to stop statahead */
	deadline = cfs_time_add(cfs_time_current(), cfs_time_seconds(3));
	while (thread_is_running(thread) &&
	       cfs_time_before(cfs_time_current(), deadline)) {
		lwi = LWI_TIMEOUT(HZ, NULL, NULL);
		l_wait_event(thread->t_ctl_waitq,
			     !sa_received_empty(sai) ||
			     !thread_is_running(thread),
			     &lwi);

		if (!sa_received_empty(sai))
			ll_post_statahead(sai);
	}

	/* stop statahead anyway */
	spin_lock(&plli->lli_sa_lock);
	if (thread_is_running(thread))
		thread_set_flags(thread, SVC_STOPPING);
	spin_unlock(&plli->lli_sa_lock);

	EXIT;
out:
	ll_finish_md_op_data(op_data);
	if (sai->sai_agl_valid) {
		spin_lock(&plli->lli_agl_lock);
		thread_set_flags(agl_thread, SVC_STOPPING);
		spin_unlock(&plli->lli_agl_lock);
		wake_up(&agl_thread->t_ctl_waitq);

		CDEBUG(D_READA, "stop agl thread for "DFID".\n",
		       PFID(&plli->lli_fid));
		lwi = LWI_TIMEOUT(0, NULL, NULL);
		l_wait_event(agl_thread->t_ctl_waitq,
			     thread_is_stopped(agl_thread),
			     &lwi);
	} else {
		/* Set agl_thread flags anyway. */
		thread_set_flags(&sai->sai_agl_thread, SVC_STOPPED);
	}

	/* wait for inflight statahead RPCs to finish. */
	while (sai->sai_sent != sai->sai_replied) {
		/* in case we're not woken up, timeout wait */
		lwi = LWI_TIMEOUT(HZ >> 3, NULL, NULL);
		l_wait_event(thread->t_ctl_waitq,
			     sai->sai_sent == sai->sai_replied, &lwi);
	}

	/* To release the resources held by received entries. */
	while (!sa_received_empty(sai))
		ll_post_statahead(sai);

	spin_lock(&plli->lli_sa_lock);
	thread_set_flags(thread, SVC_STOPPED);
	spin_unlock(&plli->lli_sa_lock);

	CDEBUG(D_READA, "statahead thread for "DFID" stopped.\n",
	       PFID(&plli->lli_fid));

	wake_up(&sai->sai_waitq);
	wake_up(&thread->t_ctl_waitq);
	ll_sai_put(sai);

	return rc;
}

/*
 * disable statahead for current opened file to avoid retry statahead for
 * subsequent stat.
 */
static inline void ll_disable_statahead(struct ll_inode_info *lli)
{
	lli->lli_sa_enabled = 0;
}

static inline void ll_enable_statahead(struct ll_inode_info *lli)
{
	lli->lli_sa_enabled = 1;
}

void ll_authorize_statahead(struct inode *dir, void *key)
{
	struct ll_inode_info *lli = ll_i2info(dir);

	spin_lock(&lli->lli_sa_lock);
	if (lli->lli_opendir_key == NULL && lli->lli_sai == NULL) {
		/*
		 * if lli_sai is not NULL, it means previous statahead is not
		 * finished yet, we'd better not start a new statahead for now.
		 */
		LASSERT(lli->lli_opendir_pid == 0);
		lli->lli_opendir_key = key;
		lli->lli_opendir_pid = current_pid();

		ll_enable_statahead(lli);
	}
	spin_unlock(&lli->lli_sa_lock);
}

void ll_deauthorize_statahead(struct inode *dir, void *key)
{
	struct ll_inode_info *lli = ll_i2info(dir);
	struct ll_statahead_info *sai = ll_sai_get(dir);

	LASSERT(lli->lli_opendir_key == key);
	LASSERT(lli->lli_opendir_pid != 0);

	CDEBUG(D_READA, "deauthorize statahead for "DFID"\n",
	       PFID(&lli->lli_fid));

	spin_lock(&lli->lli_sa_lock);
	lli->lli_opendir_key = NULL;
	lli->lli_opendir_pid = 0;
	spin_unlock(&lli->lli_sa_lock);

	if (sai) {
		/*
		 * statahead thread may not quit yet because it needs to cache
		 * stated entries, now it's time to tell it to quit.
		 */
		if (thread_is_running(&sai->sai_thread)) {
			thread_set_flags(&sai->sai_thread, SVC_STOPPING);
			wake_up(&sai->sai_thread.t_ctl_waitq);
		}
		ll_sai_put(sai);
	}
}

enum {
        /**
         * not first dirent, or is "."
         */
        LS_NONE_FIRST_DE = 0,
        /**
         * the first non-hidden dirent
         */
        LS_FIRST_DE,
        /**
         * the first hidden dirent, that is "."
         */
        LS_FIRST_DOT_DE
};

static int is_first_dirent(struct inode *dir, struct dentry *dentry)
{
	struct ll_dir_chain   chain;
	struct qstr          *target = &dentry->d_name;
	struct md_op_data    *op_data;
	int                   dot_de;
	struct lu_dirent     *ent;
	struct page	     *page = NULL;
	int                   rc     = LS_NONE_FIRST_DE;
	ENTRY;

	ll_dir_chain_init(&chain);

	op_data = ll_prep_md_op_data(NULL, dir, dir, NULL, 0, 0,
				     LUSTRE_OPC_ANY, dir);
	if (IS_ERR(op_data))
		GOTO(out, rc = PTR_ERR(op_data));
	/**
	 *FIXME choose the start offset of the readdir
	 */
	op_data->op_stripe_offset = 0;
	op_data->op_hash_offset = 0;
	op_data->op_max_pages =
		ll_i2sbi(dir)->ll_md_brw_size >> PAGE_CACHE_SHIFT;

	for (ent = ll_dir_entry_start(dir, op_data, &page);
	     ent != NULL && !IS_ERR(ent);
	     ent = ll_dir_entry_next(dir, op_data, ent, &page)) {
		__u64 hash;
		int namelen;
		char *name;

		hash = le64_to_cpu(ent->lde_hash);
		/* The ll_get_dir_page() can return any page containing
		 * the given hash which may be not the start hash. */
		if (unlikely(hash < op_data->op_hash_offset))
			continue;

		namelen = le16_to_cpu(ent->lde_namelen);
		if (unlikely(namelen == 0))
			/*
			 * skip dummy record.
			 */
			continue;

		name = ent->lde_name;
		if (name[0] == '.') {
			if (namelen == 1)
				/*
				 * skip "."
				 */
				continue;
			else if (name[1] == '.' && namelen == 2)
				/*
				 * skip ".."
				 */
				continue;
			else
				dot_de = 1;
		} else {
			dot_de = 0;
		}

		if (dot_de && target->name[0] != '.') {
			CDEBUG(D_READA, "%.*s skip hidden file %.*s\n",
			       target->len, target->name,
			       namelen, name);
			continue;
		}

		if (target->len != namelen ||
		    memcmp(target->name, name, namelen) != 0)
			rc = LS_NONE_FIRST_DE;
		else if (!dot_de)
			rc = LS_FIRST_DE;
		else
			rc = LS_FIRST_DOT_DE;

		break;
	}
        EXIT;

	if (page != NULL) {
		kunmap(page);
		page_cache_release(page);
	}
	ll_finish_md_op_data(op_data);
out:
	ll_dir_chain_fini(&chain);
        return rc;
}

static void
ll_sai_unplug(struct ll_statahead_info *sai, struct ll_sa_entry *entry)
{
	struct ll_sb_info *sbi = ll_i2sbi(sai->sai_dentry->d_inode);
	ENTRY;

	if (entry != NULL && entry->se_stat == SA_ENTRY_SUCC) {
		sai->sai_hit++;
		sai->sai_consecutive_miss = 0;
		sai->sai_max = min(2 * sai->sai_max, sbi->ll_sa_max);
	} else {
		sai->sai_miss++;
		sai->sai_consecutive_miss++;
	}

	ll_sa_entry_fini(sai, entry);
	wake_up(&sai->sai_thread.t_ctl_waitq);

	EXIT;
}

static int revalidate_statahead_dentry(struct inode *dir,
				       struct ll_statahead_info *sai,
				       struct dentry **dentryp,
				       int only_unplug)
{
	struct ll_sa_entry *entry = NULL;
	struct l_wait_info lwi = { 0 };
	int rc = 0;
	ENTRY;

	if ((*dentryp)->d_name.name[0] == '.') {
		if (sai->sai_ls_all ||
		    sai->sai_miss_hidden >= sai->sai_skip_hidden) {
			/*
			 * Hidden dentry is the first one, or statahead
			 * thread does not skip so many hidden dentries
			 * before "sai_ls_all" enabled as below.
			 */
		} else {
			if (!sai->sai_ls_all)
				/*
				 * It maybe because hidden dentry is not
				 * the first one, "sai_ls_all" was not
				 * set, then "ls -al" missed. Enable
				 * "sai_ls_all" for such case.
				 */
				sai->sai_ls_all = 1;

			/*
			 * Such "getattr" has been skipped before
			 * "sai_ls_all" enabled as above.
			 */
			sai->sai_miss_hidden++;
			RETURN(-EAGAIN);
		}
	}

	if (only_unplug)
		GOTO(out, rc = 1);

	entry = ll_sa_entry_get_byname(sai, &(*dentryp)->d_name);
	if (entry == NULL)
		GOTO(out, rc = -EAGAIN);

	/* if statahead is busy in readdir, help it do post-work */
	while (!ll_sa_entry_stated(entry) &&
	       sai->sai_in_readpage &&
	       !sa_received_empty(sai))
		ll_post_statahead(sai);

	if (!ll_sa_entry_stated(entry)) {
		sai->sai_index_wait = entry->se_index;
		lwi = LWI_TIMEOUT_INTR(cfs_time_seconds(30), NULL,
				       LWI_ON_SIGNAL_NOOP, NULL);
		rc = l_wait_event(sai->sai_waitq,
				  ll_sa_entry_stated(entry) ||
				  thread_is_stopped(&sai->sai_thread),
				  &lwi);
		if (rc < 0)
			GOTO(out, entry = NULL);
	}

	if (entry->se_stat == SA_ENTRY_SUCC &&
	    entry->se_inode != NULL) {
		struct inode *inode = entry->se_inode;
		struct lookup_intent it = { .it_op = IT_GETATTR,
					    .d.lustre.it_lock_handle =
						entry->se_handle };
		__u64 bits;

		rc = md_revalidate_lock(ll_i2mdexp(dir), &it,
					ll_inode2fid(inode), &bits);
		if (rc == 1) {
			if ((*dentryp)->d_inode == NULL) {
				struct dentry *alias;

				alias = ll_splice_alias(inode,
							*dentryp);
				if (IS_ERR(alias))
					GOTO(out, rc = PTR_ERR(alias));
				*dentryp = alias;
				/* transfer inode refcount to dentry */
				entry->se_inode = NULL;
			} else if ((*dentryp)->d_inode != inode) {
				/* revalidate, but inode is recreated */
				CDEBUG(D_READA, "stale dentry %.*s inode "DFID
					"!= statahead inode "DFID"\n",
					(*dentryp)->d_name.len,
					(*dentryp)->d_name.name,
					PFID(ll_inode2fid((*dentryp)->d_inode)),
					PFID(ll_inode2fid(inode)));
				GOTO(out, rc = -ESTALE);
			}

			if ((bits & MDS_INODELOCK_LOOKUP) &&
			    d_lustre_invalid(*dentryp))
				d_lustre_revalidate(*dentryp);
			ll_intent_release(&it);
		}
	}
	EXIT;
out:
	ll_sai_unplug(sai, entry);
	return rc;
}

static int start_statahead_thread(struct inode *dir, struct dentry *dentry)
{
	struct ll_inode_info *lli = ll_i2info(dir);
	struct ll_statahead_info *sai;
	struct ptlrpc_thread *thread;
	struct l_wait_info lwi = { 0 };
	int rc;
	ENTRY;

	/* I am the "lli_opendir_pid" owner, only me can set "lli_sai". */
	rc = is_first_dirent(dir, dentry);
	if (rc == LS_NONE_FIRST_DE)
		/* It is not "ls -{a}l" operation, no need statahead for it. */
		GOTO(out, rc = -EAGAIN);

	sai = ll_sai_alloc(dentry->d_parent);
	if (sai == NULL)
		GOTO(out, rc = -ENOMEM);

	sai->sai_ls_all = (rc == LS_FIRST_DOT_DE);

	CDEBUG(D_READA, "start statahead thread for dir "DFID"\n",
	       PFID(&lli->lli_fid));

	rc = PTR_ERR(kthread_run(ll_statahead_thread, dentry->d_parent,
				 "ll_sa_%u", lli->lli_opendir_pid));
	thread = &sai->sai_thread;
	if (IS_ERR_VALUE(rc)) {
		CERROR("can't start ll_sa thread, rc: %d\n", rc);

		spin_lock(&lli->lli_sa_lock);
		thread_set_flags(thread, SVC_STOPPED);
		thread_set_flags(&sai->sai_agl_thread, SVC_STOPPED);
		spin_unlock(&lli->lli_sa_lock);

		ll_sai_put(sai);
		GOTO(out, rc = -EAGAIN);
	}

	l_wait_event(thread->t_ctl_waitq,
		     thread_is_running(thread) || thread_is_stopped(thread),
		     &lwi);
	ll_sai_put(sai);

	/*
	 * We don't stat-ahead for the first dirent since we are already in
	 * lookup.
	 */
	RETURN(-EAGAIN);

out:
	ll_disable_statahead(lli);
	return rc;
}

/**
 * Start statahead thread if this is the first dir entry.
 * Otherwise if a thread is started already, wait it until it is ahead of me.
 * \retval 1       -- find entry with lock in cache, the caller needs to do
 *                    nothing.
 * \retval 0       -- find entry in cache, but without lock, the caller needs
 *                    refresh from MDS.
 * \retval others  -- the caller need to process as non-statahead.
 */
int do_statahead_enter(struct inode *dir, struct dentry **dentryp,
		       int only_unplug)
{
	struct ll_statahead_info *sai = ll_sai_get(dir);

	if (sai != NULL) {
		int rc;

		rc = revalidate_statahead_dentry(dir, sai, dentryp,
						 only_unplug);
		CDEBUG(D_READA, "revalidate statahead %.*s: %d.\n",
			(*dentryp)->d_name.len, (*dentryp)->d_name.name, rc);
		ll_sai_put(sai);
		return rc;
	}

	return start_statahead_thread(dir, *dentryp);
}
