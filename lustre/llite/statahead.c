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
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/mm.h>
#include <linux/smp_lock.h>
#include <linux/highmem.h>
#include <linux/pagemap.h>

#define DEBUG_SUBSYSTEM S_LLITE

#include <obd_support.h>
#include <lustre_lite.h>
#include <lustre_dlm.h>
#include <linux/lustre_version.h>
#include "llite_internal.h"

struct ll_sai_entry {
        cfs_list_t              se_list;
        unsigned int            se_index;
        int                     se_stat;
        struct ptlrpc_request  *se_req;
        struct md_enqueue_info *se_minfo;
        unsigned int            se_is_reg:1;
};

enum {
        SA_ENTRY_UNSTATED = 0,
        SA_ENTRY_STATED
};

static unsigned int sai_generation = 0;
static cfs_spinlock_t sai_generation_lock = CFS_SPIN_LOCK_UNLOCKED;

/**
 * Check whether first entry was stated already or not.
 * No need to hold lli_sa_lock, for:
 * (1) it is me that remove entry from the list
 * (2) the statahead thread only add new entry to the list
 */
static int ll_sai_entry_stated(struct ll_statahead_info *sai)
{
        struct ll_sai_entry  *entry;
        int                   rc = 0;

        if (!cfs_list_empty(&sai->sai_entries_stated)) {
                entry = cfs_list_entry(sai->sai_entries_stated.next,
                                       struct ll_sai_entry, se_list);
                if (entry->se_index == sai->sai_index_next)
                        rc = 1;
        }
        return rc;
}

static inline int sa_received_empty(struct ll_statahead_info *sai)
{
        return cfs_list_empty(&sai->sai_entries_received);
}

static inline int sa_not_full(struct ll_statahead_info *sai)
{
        return !!(sai->sai_index < sai->sai_index_next + sai->sai_max);
}

static inline int sa_is_running(struct ll_statahead_info *sai)
{
        return !!(sai->sai_thread.t_flags & SVC_RUNNING);
}

static inline int sa_is_stopping(struct ll_statahead_info *sai)
{
        return !!(sai->sai_thread.t_flags & SVC_STOPPING);
}

static inline int sa_is_stopped(struct ll_statahead_info *sai)
{
        return !!(sai->sai_thread.t_flags & SVC_STOPPED);
}

static inline int agl_should_run(struct ll_statahead_info *sai,
                                 struct inode *inode, int index)
{
        if (inode != NULL && S_ISREG(inode->i_mode) &&
            ll_i2info(inode)->lli_smd != NULL) {
                struct ll_sb_info *sbi = ll_i2sbi(inode);

                if (sbi->ll_sa_max > 0 && sbi->ll_sa_agl > 0 &&
                    sai->sai_index_next + sbi->ll_sa_max <=
                    index + sbi->ll_sa_agl)
                        return 1;
        }
        return 0;
}

static inline int sa_should_run(struct ll_statahead_info *sai)
{
        if (ll_i2sbi(sai->sai_inode)->ll_sa_max > 0)
                return sa_is_running(sai);
        return 0;
}

/**
 * (1) hit ratio less than 80%
 * or
 * (2) consecutive miss more than 8
 */
static inline int sa_low_hit(struct ll_statahead_info *sai)
{
        return ((sai->sai_hit > 7 && sai->sai_hit < 4 * sai->sai_miss) ||
                (sai->sai_consecutive_miss > 8));
}

/**
 * process the deleted entry's member and free the entry.
 * (1) release intent
 * (2) free md_enqueue_info
 * (3) drop dentry's ref count
 * (4) release request's ref count
 */
static void ll_sai_entry_cleanup(struct ll_sai_entry *entry, int free)
{
        struct md_enqueue_info *minfo = entry->se_minfo;
        struct ptlrpc_request  *req   = entry->se_req;
        ENTRY;

        if (minfo) {
                entry->se_minfo = NULL;
                ll_intent_release(&minfo->mi_it);
                dput(minfo->mi_dentry);
                iput(minfo->mi_dir);
                OBD_FREE_PTR(minfo);
        }
        if (req) {
                entry->se_req = NULL;
                ptlrpc_req_finished(req);
        }
        if (free) {
                LASSERT(cfs_list_empty(&entry->se_list));
                OBD_FREE_PTR(entry);
        }

        EXIT;
}

static void ll_agl_trigger(struct dentry *dentry, struct ll_statahead_info *sai,
                           unsigned int index)
{
        struct inode *child = dentry->d_inode;
        struct ll_inode_info *clli = ll_i2info(child);
        struct inode *parent = sai->sai_inode;
        struct ll_inode_info *plli = ll_i2info(parent);
        int rc;
        ENTRY;

        cfs_spin_lock(&clli->lli_sa_lock);
        if (unlikely(clli->lli_agl_parent != NULL)) {
                cfs_spin_unlock(&clli->lli_sa_lock);
                CDEBUG(D_READA, "Found former async glimpse stub: inode = "
                       DFID", old_idx = %u, new_idx = %u\n",
                       PFID(&clli->lli_fid), clli->lli_agl_idx, index);
                RETURN_EXIT;
        }

        /* hold reference on parent */
        clli->lli_agl_parent = igrab(parent);
        cfs_spin_unlock(&clli->lli_sa_lock);

        CDEBUG(D_READA, "Handling (init) async glimpse: inode = "
               DFID", idx = %u\n", PFID(&clli->lli_fid), index);

        rc = cl_agl_init(child, &clli->lli_agl_args, ll_i2sbi(child)->ll_agl_rough);
        if (likely(rc == 0)) {
                cfs_list_t *pos = &sai->sai_agl_list;
                struct ll_inode_info *tlli;

                /* hold reference on child */
                igrab(child);
                clli->lli_agl_idx = index;

                cfs_spin_lock(&plli->lli_sa_lock);
                cfs_list_for_each_entry_reverse(tlli, &sai->sai_agl_list,
                                                lli_agl_list) {
                        if (tlli->lli_agl_idx < index) {
                                pos = &tlli->lli_agl_list;
                                break;
                        }
                }
                cfs_list_add(&clli->lli_agl_list, pos);
                sai->sai_agl_count++;
                cfs_spin_unlock(&plli->lli_sa_lock);
        } else {
                cfs_spin_lock(&clli->lli_sa_lock);
                clli->lli_agl_parent = NULL;
                cfs_spin_unlock(&clli->lli_sa_lock);
                iput(parent);
        }

        CDEBUG(D_READA, "Handled (init) async glimpse: inode= "
               DFID", idx = %u, rc = %d\n", PFID(&clli->lli_fid), index, rc);

        EXIT;
}

/* hold parent lock already */
static void do_agl_cleanup_one(struct ll_inode_info *clli,
                               struct ll_inode_info *plli,
                               struct ll_statahead_info *sai,
                               int wait)
{
        ENTRY;

        LASSERT_SPIN_LOCKED(&plli->lli_sa_lock);
        LASSERT(!cfs_list_empty(&clli->lli_agl_list));

        sai->sai_agl_count--;
        cfs_list_del_init(&clli->lli_agl_list);
        cfs_spin_unlock(&plli->lli_sa_lock);

        cl_agl_fini(&clli->lli_vfs_inode, &clli->lli_agl_args, wait);

        cfs_spin_lock(&clli->lli_sa_lock);
        clli->lli_agl_parent = NULL;
        cfs_spin_unlock(&clli->lli_sa_lock);

        /* drop reference on parent */
        iput(&plli->lli_vfs_inode);
        /* drop reference on child */
        iput(&clli->lli_vfs_inode);
        cfs_spin_lock(&plli->lli_sa_lock);

        EXIT;
}

static void ll_agl_cleanup_old(struct ll_statahead_info *sai)
{
        struct ll_inode_info *plli = ll_i2info(sai->sai_inode);
        struct ll_inode_info *clli;
        ENTRY;

        cfs_spin_lock(&plli->lli_sa_lock);
        while (!cfs_list_empty(&sai->sai_agl_list)) {
                clli = cfs_list_entry(sai->sai_agl_list.next,
                                      struct ll_inode_info, lli_agl_list);
                if (clli->lli_agl_idx + 1 >= sai->sai_index_next)
                        break;
                do_agl_cleanup_one(clli, plli, sai, 0);
        }
        cfs_spin_unlock(&plli->lli_sa_lock);

        EXIT;
}

int ll_glimpse_size(struct inode *inode)
{
        struct ll_inode_info *clli = ll_i2info(inode);
        struct ll_inode_info *plli;
        int rc;
        ENTRY;

        cfs_spin_lock(&clli->lli_sa_lock);
        if (clli->lli_agl_parent != NULL) {
                plli = ll_i2info(clli->lli_agl_parent);
                cfs_spin_lock(&plli->lli_sa_lock);
                if (plli->lli_sai != NULL &&
                    !cfs_list_empty(&clli->lli_agl_list)) {
                        cfs_list_del_init(&clli->lli_agl_list);
                        plli->lli_sai->sai_agl_count--;
                        cfs_spin_unlock(&plli->lli_sa_lock);
                        clli->lli_agl_parent = NULL;
                        cfs_spin_unlock(&clli->lli_sa_lock);

                        CDEBUG(D_READA, "Handling (fini) async glimpse: "
                               "inode = "DFID", idx = %u\n",
                               PFID(&clli->lli_fid), clli->lli_agl_idx);

                        rc = cl_agl_fini(inode, &clli->lli_agl_args, 1);

                        CDEBUG(D_READA, "Handled (fini) async glimpse: "
                               "inode = "DFID", idx = %u, rc = %d\n",
                               PFID(&clli->lli_fid), clli->lli_agl_idx, rc);

                        /* drop reference on parent */
                        iput(&plli->lli_vfs_inode);
                        /* drop reference on child */
                        iput(&clli->lli_vfs_inode);
                } else {
                        /* statahead wants to cleanup this entry */
                        cfs_spin_unlock(&plli->lli_sa_lock);
                        cfs_spin_unlock(&clli->lli_sa_lock);
                        rc = cl_glimpse_size(inode);
                }
        } else {
                cfs_spin_unlock(&clli->lli_sa_lock);
                rc = cl_glimpse_size(inode);
        }

        RETURN(rc);
}

static struct ll_statahead_info *ll_sai_alloc(void)
{
        struct ll_statahead_info *sai;

        OBD_ALLOC_PTR(sai);
        if (!sai)
                return NULL;

        cfs_spin_lock(&sai_generation_lock);
        sai->sai_generation = ++sai_generation;
        if (unlikely(sai_generation == 0))
                sai->sai_generation = ++sai_generation;
        cfs_spin_unlock(&sai_generation_lock);
        cfs_atomic_set(&sai->sai_refcount, 1);
        sai->sai_max = LL_SA_RPC_MIN;
        cfs_waitq_init(&sai->sai_waitq);
        cfs_waitq_init(&sai->sai_thread.t_ctl_waitq);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_sent);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_received);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_stated);
        CFS_INIT_LIST_HEAD(&sai->sai_agl_list);
        return sai;
}

static inline
struct ll_statahead_info *ll_sai_get(struct ll_statahead_info *sai)
{
        LASSERT(sai);
        cfs_atomic_inc(&sai->sai_refcount);
        return sai;
}

static void ll_sai_put(struct ll_statahead_info *sai)
{
        struct inode         *inode = sai->sai_inode;
        struct ll_inode_info *plli;
        ENTRY;

        LASSERT(inode != NULL);
        plli = ll_i2info(inode);
        LASSERT(plli->lli_sai == sai);

        if (cfs_atomic_dec_and_lock(&sai->sai_refcount, &plli->lli_sa_lock)) {
                struct ll_sai_entry *entry, *next;
                struct ll_inode_info *clli;

                if (unlikely(cfs_atomic_read(&sai->sai_refcount) > 0)) {
                        /* It is race case, the interpret callback just hold
                         * a reference count */
                        cfs_spin_unlock(&plli->lli_sa_lock);
                        RETURN_EXIT;
                }

                LASSERT(plli->lli_opendir_key == NULL);
                plli->lli_sai = NULL;
                plli->lli_opendir_pid = 0;
                cfs_spin_unlock(&plli->lli_sa_lock);

                LASSERT(sa_is_stopped(sai));

                if (sai->sai_sent > sai->sai_replied)
                        CDEBUG(D_READA,"statahead for dir "DFID" does not "
                              "finish: [sent:%u] [replied:%u]\n",
                              PFID(&plli->lli_fid),
                              sai->sai_sent, sai->sai_replied);

                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_sent, se_list) {
                        cfs_list_del_init(&entry->se_list);
                        ll_sai_entry_cleanup(entry, 1);
                }
                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_received,
                                             se_list) {
                        cfs_list_del_init(&entry->se_list);
                        ll_sai_entry_cleanup(entry, 1);
                }
                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_stated,
                                             se_list) {
                        cfs_list_del_init(&entry->se_list);
                        ll_sai_entry_cleanup(entry, 1);
                }

                cfs_spin_lock(&plli->lli_sa_lock);
                while (!cfs_list_empty(&sai->sai_agl_list)) {
                        clli = cfs_list_entry(sai->sai_agl_list.next,
                                              struct ll_inode_info,
                                              lli_agl_list);
                        do_agl_cleanup_one(clli, plli, sai, 0);
                }
                LASSERTF(cfs_list_empty(&sai->sai_agl_list) &&
                         sai->sai_agl_count == 0, "invalid agl_count = %d\n",
                         sai->sai_agl_count);
                cfs_spin_unlock(&plli->lli_sa_lock);

                iput(inode);
                OBD_FREE_PTR(sai);
        }
        EXIT;
}

/**
 * insert it into sai_entries_sent tail when init.
 */
static struct ll_sai_entry *
ll_sai_entry_init(struct ll_statahead_info *sai, unsigned int index)
{
        struct ll_inode_info *lli = ll_i2info(sai->sai_inode);
        struct ll_sai_entry  *entry;
        ENTRY;

        OBD_ALLOC_PTR(entry);
        if (entry == NULL)
                RETURN(ERR_PTR(-ENOMEM));

        entry->se_index = index;
        entry->se_stat = SA_ENTRY_UNSTATED;

        cfs_spin_lock(&lli->lli_sa_lock);
        cfs_list_add_tail(&entry->se_list, &sai->sai_entries_sent);
        cfs_spin_unlock(&lli->lli_sa_lock);

        RETURN(entry);
}

/**
 * delete it from sai_entries_stated head when fini, it need not
 * to process entry's member.
 */
static int ll_sai_entry_fini(struct ll_statahead_info *sai)
{
        struct ll_inode_info *lli = ll_i2info(sai->sai_inode);
        struct ll_sai_entry  *entry;
        int rc = 0;
        ENTRY;

        cfs_spin_lock(&lli->lli_sa_lock);
        sai->sai_index_next++;
        if (likely(!cfs_list_empty(&sai->sai_entries_stated))) {
                entry = cfs_list_entry(sai->sai_entries_stated.next,
                                       struct ll_sai_entry, se_list);
                if (entry->se_index < sai->sai_index_next) {
                        cfs_list_del_init(&entry->se_list);
                        rc = entry->se_stat;
                        OBD_FREE_PTR(entry);
                }
        } else {
                LASSERT(sa_is_stopped(sai));
        }
        cfs_spin_unlock(&lli->lli_sa_lock);

        RETURN(rc);
}

/**
 * inside lli_sa_lock.
 * \retval NULL : can not find the entry in sai_entries_sent with the index
 * \retval entry: find the entry in sai_entries_sent with the index
 */
static struct ll_sai_entry *
ll_sai_entry_set(struct ll_statahead_info *sai, unsigned int index, int stat,
                 struct ptlrpc_request *req, struct md_enqueue_info *minfo)
{
        struct ll_sai_entry *entry;
        ENTRY;

        if (!cfs_list_empty(&sai->sai_entries_sent)) {
                cfs_list_for_each_entry(entry, &sai->sai_entries_sent,
                                        se_list) {
                        if (entry->se_index == index) {
                                entry->se_stat = stat;
                                entry->se_req = ptlrpc_request_addref(req);
                                entry->se_minfo = minfo;
                                RETURN(entry);
                        } else if (entry->se_index > index) {
                                RETURN(NULL);
                        }
                }
        }
        RETURN(NULL);
}

/**
 * inside lli_sa_lock.
 * Move entry to sai_entries_received and
 * insert it into sai_entries_received tail.
 */
static inline void
ll_sai_entry_to_received(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        if (!cfs_list_empty(&entry->se_list))
                cfs_list_del_init(&entry->se_list);
        if (cfs_list_empty(&sai->sai_entries_received))
                sai->sai_nextent_is_reg = entry->se_is_reg;
        cfs_list_add_tail(&entry->se_list, &sai->sai_entries_received);
}

/**
 * Move entry to sai_entries_stated and
 * sort with the index.
 */
static int
ll_sai_entry_to_stated(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        struct ll_inode_info *lli = ll_i2info(sai->sai_inode);
        struct ll_sai_entry  *se;
        cfs_list_t           *pos = &sai->sai_entries_stated;
        ENTRY;

        ll_sai_entry_cleanup(entry, 0);

        cfs_spin_lock(&lli->lli_sa_lock);
        if (!cfs_list_empty(&entry->se_list))
                cfs_list_del_init(&entry->se_list);

        /* stale entry */
        if (unlikely(entry->se_index < sai->sai_index_next)) {
                cfs_spin_unlock(&lli->lli_sa_lock);
                OBD_FREE_PTR(entry);
                RETURN(0);
        }

        cfs_list_for_each_entry_reverse(se, &sai->sai_entries_stated, se_list) {
                if (se->se_index < entry->se_index) {
                        pos = &se->se_list;
                        break;
                }
        }

        cfs_list_add(&entry->se_list, pos);
        cfs_spin_unlock(&lli->lli_sa_lock);
        RETURN(1);
}

/**
 * finish lookup/revalidate.
 */
static int do_statahead_interpret(struct ll_statahead_info *sai, int only_reg)
{
        struct ll_inode_info   *lli = ll_i2info(sai->sai_inode);
        struct ll_sai_entry    *entry = NULL, *next;
        struct ptlrpc_request  *req;
        struct md_enqueue_info *minfo;
        struct lookup_intent   *it;
        struct dentry          *dentry;
        int                     rc = 0;
        struct mdt_body        *body;
        ENTRY;

        cfs_spin_lock(&lli->lli_sa_lock);
        if (!sa_received_empty(sai) && (!only_reg || sai->sai_nextent_is_reg)) {
                entry = cfs_list_entry(sai->sai_entries_received.next,
                                       struct ll_sai_entry, se_list);
                cfs_list_del_init(&entry->se_list);
                if (!sa_received_empty(sai)) {
                        next = cfs_list_entry(sai->sai_entries_received.next,
                                              struct ll_sai_entry, se_list);
                        sai->sai_nextent_is_reg = next->se_is_reg;
                } else {
                        sai->sai_nextent_is_reg = 0;
                }
        }
        cfs_spin_unlock(&lli->lli_sa_lock);

        if (entry == NULL)
                RETURN(0);

        if (unlikely(entry->se_index < sai->sai_index_next)) {
                CWARN("Found stale entry: [index %u] [next %u]\n",
                      entry->se_index, sai->sai_index_next);
                ll_sai_entry_cleanup(entry, 1);
                RETURN(0);
        }

        if (entry->se_stat != SA_ENTRY_STATED)
                GOTO(out, rc = entry->se_stat);

        req = entry->se_req;
        minfo = entry->se_minfo;
        it = &minfo->mi_it;
        dentry = minfo->mi_dentry;

        body = req_capsule_server_get(&req->rq_pill, &RMF_MDT_BODY);
        if (body == NULL)
                GOTO(out, rc = -EFAULT);

        if (dentry->d_inode == NULL) {
                /*
                 * lookup.
                 */
                struct dentry    *save = dentry, *exist;
                struct it_cb_data icbd = {
                        .icbd_parent   = minfo->mi_dir,
                        .icbd_childp   = &dentry
                };

                LASSERT(fid_is_zero(&minfo->mi_data.op_fid2));

                /* XXX: No fid in reply, this is probaly cross-ref case.
                 * SA can't handle it yet. */
                if (body->valid & OBD_MD_MDS)
                        GOTO(out, rc = -EAGAIN);

                /* Someone has inserted the dentry with the same name earlier
                 * than me. */
                exist = d_lookup(dentry->d_parent, &dentry->d_name);
                if (exist != NULL) { 
                        dput(exist);
                        GOTO(out, rc = 1);
                }

                /* Here dentry->d_inode might be NULL, because the entry may
                 * have been removed before we start doing stat ahead. */
                rc = ll_lookup_it_finish(req, it, &icbd);
                if (!rc)
                        ll_lookup_finish_locks(it, dentry);

                if (dentry != save) {
                        minfo->mi_dentry = dentry;
                        dput(save);
                }
        } else {
                struct inode *inode = dentry->d_inode;
                __u32 bits = 0;

                /*
                 * revalidate.
                 */
                if (!lu_fid_eq(&minfo->mi_data.op_fid2, &body->fid1)) {
                        ll_unhash_aliases(inode);
                        GOTO(out, rc = -EAGAIN);
                }

                rc = ll_revalidate_it_finish(req, it, dentry);
                if (rc) {
                        ll_unhash_aliases(inode);
                        GOTO(out, rc);
                }

                ll_set_lock_data(ll_i2sbi(inode)->ll_md_exp, inode, it, &bits);

                cfs_spin_lock(&ll_lookup_lock);
                spin_lock(&dcache_lock);
                lock_dentry(dentry);
                __d_drop(dentry);
                if (bits & MDS_INODELOCK_LOOKUP)
                        dentry->d_flags &= ~DCACHE_LUSTRE_INVALID;
                unlock_dentry(dentry);
                d_rehash_cond(dentry, 0);
                spin_unlock(&dcache_lock);
                cfs_spin_unlock(&ll_lookup_lock);

                ll_lookup_finish_locks(it, dentry);
        }

        if (rc == 0 && agl_should_run(sai, dentry->d_inode, entry->se_index))
                ll_agl_trigger(dentry, sai, entry->se_index);

        EXIT;

out:
        /* The "ll_sai_entry_to_stated()" will drop related ldlm ibits lock
         * reference count by calling "ll_intent_drop_lock()" in spite of the
         * above operations failed or not. Do not worry about calling
         * "ll_intent_drop_lock()" more than once. */
        if (likely(ll_sai_entry_to_stated(sai, entry)))
                cfs_waitq_signal(&sai->sai_waitq);
        return rc;
}

static int ll_statahead_interpret(struct ptlrpc_request *req,
                                  struct md_enqueue_info *minfo,
                                  int rc)
{
        struct lookup_intent     *it = &minfo->mi_it;
        struct dentry            *dentry = minfo->mi_dentry;
        struct inode             *dir = minfo->mi_dir;
        struct ll_inode_info     *lli = ll_i2info(dir);
        struct ll_statahead_info *sai;
        struct ll_sai_entry      *entry;
        struct mdt_body          *body;
        ENTRY;

        CDEBUG(D_READA, "interpret statahead %.*s rc %d\n",
               dentry->d_name.len, dentry->d_name.name, rc);

        cfs_spin_lock(&lli->lli_sa_lock);
        /* stale entry */
        if (unlikely(lli->lli_sai == NULL ||
            lli->lli_sai->sai_generation != minfo->mi_generation)) {
                cfs_spin_unlock(&lli->lli_sa_lock);
                ll_intent_release(it);
                dput(dentry);
                iput(dir);
                OBD_FREE_PTR(minfo);
                RETURN(-ESTALE);
        } else {
                sai = ll_sai_get(lli->lli_sai);
                entry = ll_sai_entry_set(sai,
                                         (unsigned int)(long)minfo->mi_cbdata,
                                         rc < 0 ? rc : SA_ENTRY_STATED, req,
                                         minfo);
                LASSERT(entry != NULL);
                if (likely(sa_is_running(sai))) {
                        body = req_capsule_server_get(&req->rq_pill,
                                                      &RMF_MDT_BODY);
                        if (body != NULL && body->valid & OBD_MD_FLMODE &&
                            S_ISREG(body->mode))
                                entry->se_is_reg = 1;
                        ll_sai_entry_to_received(sai, entry);
                        sai->sai_replied++;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        cfs_waitq_signal(&sai->sai_thread.t_ctl_waitq);
                } else {
                        if (!cfs_list_empty(&entry->se_list))
                                cfs_list_del_init(&entry->se_list);
                        sai->sai_replied++;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        ll_sai_entry_cleanup(entry, 1);
                }
                ll_sai_put(sai);
                RETURN(rc);
        }
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
static int sa_args_init(struct inode *dir, struct dentry *dentry,
                        struct md_enqueue_info **pmi,
                        struct ldlm_enqueue_info **pei,
                        struct obd_capa **pcapa)
{
        struct ll_inode_info     *lli = ll_i2info(dir);
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

        op_data = ll_prep_md_op_data(&minfo->mi_data, dir, dentry->d_inode,
                                     dentry->d_name.name, dentry->d_name.len,
                                     0, LUSTRE_OPC_ANY, NULL);
        if (IS_ERR(op_data)) {
                OBD_FREE_PTR(einfo);
                OBD_FREE_PTR(minfo);
                return PTR_ERR(op_data);
        }

        minfo->mi_it.it_op = IT_GETATTR;
        minfo->mi_dentry = dentry;
        minfo->mi_dir = igrab(dir);
        minfo->mi_cb = ll_statahead_interpret;
        minfo->mi_generation = lli->lli_sai->sai_generation;
        minfo->mi_cbdata = (void *)(long)lli->lli_sai->sai_index;

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

/**
 * similar to ll_lookup_it().
 */
static int do_sa_lookup(struct inode *dir, struct dentry *dentry)
{
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct obd_capa          *capas[2];
        int                       rc;
        ENTRY;

        rc = sa_args_init(dir, dentry, &minfo, &einfo, capas);
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
static int do_sa_revalidate(struct inode *dir, struct dentry *dentry)
{
        struct inode             *inode = dentry->d_inode;
        struct lookup_intent      it = { .it_op = IT_GETATTR };
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct obd_capa          *capas[2];
        int rc;
        ENTRY;

        if (unlikely(inode == NULL))
                RETURN(1);

        if (d_mountpoint(dentry))
                RETURN(1);

        if (unlikely(dentry == dentry->d_sb->s_root))
                RETURN(1);

        rc = md_revalidate_lock(ll_i2mdexp(dir), &it, ll_inode2fid(inode));
        if (rc == 1) {
                ll_intent_release(&it);
                RETURN(1);
        }

        rc = sa_args_init(dir, dentry, &minfo, &einfo, capas);
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

static inline void ll_name2qstr(struct qstr *q, const char *name, int namelen)
{
        q->name = name;
        q->len  = namelen;
        q->hash = full_name_hash(name, namelen);
}

static int ll_statahead_one(struct dentry *parent, const char *entry_name,
                            int entry_name_len)
{
        struct inode             *dir = parent->d_inode;
        struct ll_inode_info     *lli = ll_i2info(dir);
        struct ll_statahead_info *sai = lli->lli_sai;
        struct qstr               name;
        struct dentry            *dentry = NULL;
        struct ll_sai_entry      *se;
        int                       rc;
        ENTRY;

        se = ll_sai_entry_init(sai, sai->sai_index);
        if (IS_ERR(se))
                RETURN(PTR_ERR(se));

        ll_name2qstr(&name, entry_name, entry_name_len);
        dentry = d_lookup(parent, &name);
        if (!dentry) {
                dentry = d_alloc(parent, &name);
                if (dentry)
                        rc = do_sa_lookup(dir, dentry);
                else
                        GOTO(out, rc = -ENOMEM);
        } else {
                if (unlikely(dentry->d_flags & DCACHE_LUSTRE_EARLY)) {
                        /* we found the half-created entry, ignore it */
                        CDEBUG(D_READA, "find half-created entry, ignore it\n");
                        GOTO(out, rc = 1);
                }
                rc = do_sa_revalidate(dir, dentry);
                if (rc == 1 && agl_should_run(sai, dentry->d_inode,
                                              se->se_index))
                        ll_agl_trigger(dentry, sai, se->se_index);
        }

        EXIT;

out:
        if (rc) {
                if (dentry != NULL)
                        dput(dentry);
                se->se_stat = rc < 0 ? rc : SA_ENTRY_STATED;
                CDEBUG(D_READA, "set sai entry %p index %u stat %d rc %d\n",
                       se, se->se_index, se->se_stat, rc);
                if (ll_sai_entry_to_stated(sai, se))
                        cfs_waitq_signal(&sai->sai_waitq);
        } else {
                sai->sai_sent++;
        }

        sai->sai_index++;
        return rc;
}

static inline int has_old_agl(struct ll_statahead_info *sai)
{
        struct ll_inode_info *plli = ll_i2info(sai->sai_inode);
        struct ll_inode_info *clli;
        int rc = 0;

        cfs_spin_lock(&plli->lli_sa_lock);
        if (!cfs_list_empty(&sai->sai_agl_list)) {
                clli = cfs_list_entry(sai->sai_agl_list.next,
                                      struct ll_inode_info, lli_agl_list);
                if (clli->lli_agl_idx + 1 < sai->sai_index_next)
                        rc = 1;
        }
        cfs_spin_unlock(&plli->lli_sa_lock);
        return rc;
}

static int ll_statahead_thread(void *arg)
{
        struct dentry            *parent = (struct dentry *)arg;
        struct inode             *dir = parent->d_inode;
        struct ll_inode_info     *lli = ll_i2info(dir);
        struct ll_sb_info        *sbi = ll_i2sbi(dir);
        struct ll_statahead_info *sai = ll_sai_get(lli->lli_sai);
        struct ptlrpc_thread     *thread = &sai->sai_thread;
        struct page              *page;
        __u64                     pos = 0;
        int                       first = 0;
        int                       rc = 0;
        struct ll_dir_chain       chain;
        ENTRY;

        {
                char pname[16];
                snprintf(pname, 15, "ll_sa_%u", lli->lli_opendir_pid);
                cfs_daemonize(pname);
        }

        atomic_inc(&sbi->ll_sa_total);
        cfs_spin_lock(&lli->lli_sa_lock);
        thread->t_flags = SVC_RUNNING;
        lli->lli_statahead_pid = cfs_curproc_pid();
        cfs_spin_unlock(&lli->lli_sa_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        CDEBUG(D_READA, "start doing statahead for %s\n", parent->d_name.name);

        ll_dir_chain_init(&chain);
        page = ll_get_dir_page(dir, pos, 0, &chain);

        while (1) {
                struct l_wait_info lwi = { 0 };
                struct lu_dirpage *dp;
                struct lu_dirent  *ent;

                if (IS_ERR(page)) {
                        rc = PTR_ERR(page);
                        CDEBUG(D_READA, "error reading dir "DFID" at "LPU64
                               "/%u: [rc %d] [parent %u]\n",
                               PFID(ll_inode2fid(dir)), pos, sai->sai_index,
                               rc, lli->lli_opendir_pid);
                        break;
                }

                dp = page_address(page);
                for (ent = lu_dirent_start(dp); ent != NULL;
                     ent = lu_dirent_next(ent)) {
                        char *name = ent->lde_name;
                        int namelen = le16_to_cpu(ent->lde_namelen);

                        if (unlikely(namelen == 0))
                                /*
                                 * Skip dummy record.
                                 */
                                continue;

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

keep_de:
                        l_wait_event(thread->t_ctl_waitq,
                                     !sa_should_run(sai) ||
                                     sa_not_full(sai) ||
                                     !sa_received_empty(sai) ||
                                     has_old_agl(sai),
                                     &lwi);

                        if (unlikely(has_old_agl(sai)))
                                ll_agl_cleanup_old(sai);

                        while (!sa_received_empty(sai) && sa_should_run(sai))
                                do_statahead_interpret(sai, 0);

                        if (unlikely(!sa_should_run(sai))) {
                                ll_put_page(page);
                                GOTO(out, rc = 0);
                        }

                        if (!sa_not_full(sai))
                                /*
                                 * do not skip the current de.
                                 */
                                goto keep_de;

                        rc = ll_statahead_one(parent, name, namelen);
                        if (rc < 0) {
                                ll_put_page(page);
                                GOTO(out, rc);
                        }
                }
                pos = le64_to_cpu(dp->ldp_hash_end);
                ll_put_page(page);
                if (pos == DIR_END_OFF) {
                        struct ll_inode_info *clli;

                        /*
                         * End of directory reached.
                         */
                        first = 0;
                        while (1) {
                                l_wait_event(thread->t_ctl_waitq,
                                             !sa_should_run(sai) ||
                                             !sa_received_empty(sai) ||
                                             has_old_agl(sai) ||
                                             sai->sai_sent == sai->sai_replied,
                                             &lwi);

                                if (unlikely(has_old_agl(sai)))
                                        ll_agl_cleanup_old(sai);

                                while (!sa_received_empty(sai) &&
                                       sa_should_run(sai))
                                        do_statahead_interpret(sai, 0);

                                if (unlikely(!sa_should_run(sai)))
                                        GOTO(out, rc = 0);

                                if (sai->sai_sent == sai->sai_replied) {
                                        /* maybe race just after above while()*/
                                        if(++first == 1)
                                                continue;
                                        break;
                                }
                        }

                        cfs_spin_lock(&lli->lli_sa_lock);
                        while (!cfs_list_empty(&sai->sai_agl_list)) {
                                clli = cfs_list_entry(sai->sai_agl_list.next,
                                                      struct ll_inode_info,
                                                      lli_agl_list);
                                do_agl_cleanup_one(clli, lli, sai, 1);
                                if (unlikely(!sa_should_run(sai)))
                                        break;
                        }
                        cfs_spin_unlock(&lli->lli_sa_lock);

                        GOTO(out, rc = 0);
                } else if (1) {
                        /*
                         * chain is exhausted.
                         * Normal case: continue to the next page.
                         */
                        page = ll_get_dir_page(dir, pos, 1, &chain);
                } else {
                        /*
                         * go into overflow page.
                         */
                }
        }
        EXIT;

out:
        ll_dir_chain_fini(&chain);
        cfs_spin_lock(&lli->lli_sa_lock);
        thread->t_flags = SVC_STOPPED;
        lli->lli_statahead_pid = 0;
        cfs_spin_unlock(&lli->lli_sa_lock);
        cfs_waitq_signal(&sai->sai_waitq);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        ll_sai_put(sai);
        dput(parent);
        CDEBUG(D_READA, "statahead thread stopped, pid %d\n",
               cfs_curproc_pid());
        return rc;
}

/**
 * called in ll_file_release().
 */
void ll_stop_statahead(struct inode *dir, void *key)
{
        struct ll_inode_info *lli = ll_i2info(dir);

        if (unlikely(key == NULL))
                return;

        cfs_spin_lock(&lli->lli_sa_lock);
        if (lli->lli_opendir_key != key || lli->lli_opendir_pid == 0) {
                cfs_spin_unlock(&lli->lli_sa_lock);
                return;
        }

        lli->lli_opendir_key = NULL;

        if (lli->lli_sai) {
                struct l_wait_info lwi = { 0 };
                struct ptlrpc_thread *thread = &lli->lli_sai->sai_thread;

                if (!sa_is_stopped(lli->lli_sai)) {
                        thread->t_flags = SVC_STOPPING;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        cfs_waitq_signal(&thread->t_ctl_waitq);

                        CDEBUG(D_READA, "stopping statahead thread, pid %d\n",
                               cfs_curproc_pid());
                        l_wait_event(thread->t_ctl_waitq,
                                     sa_is_stopped(lli->lli_sai),
                                     &lwi);
                } else {
                        cfs_spin_unlock(&lli->lli_sa_lock);
                }

                /*
                 * Put the ref which was held when first statahead_enter.
                 * It maybe not the last ref for some statahead requests
                 * maybe inflight.
                 */
                ll_sai_put(lli->lli_sai);
        } else {
                lli->lli_opendir_pid = 0;
                cfs_spin_unlock(&lli->lli_sa_lock);
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
        struct ll_dir_chain chain;
        struct qstr        *target = &dentry->d_name;
        struct page        *page;
        __u64               pos = 0;
        int                 dot_de;
        int                 rc = LS_NONE_FIRST_DE;
        ENTRY;

        ll_dir_chain_init(&chain);
        page = ll_get_dir_page(dir, pos, 0, &chain);

        while (1) {
                struct lu_dirpage *dp;
                struct lu_dirent  *ent;

                if (IS_ERR(page)) {
                        struct ll_inode_info *lli = ll_i2info(dir);

                        rc = PTR_ERR(page);
                        CERROR("error reading dir "DFID" at "LPU64": "
                               "[rc %d] [parent %u]\n",
                               PFID(ll_inode2fid(dir)), pos,
                               rc, lli->lli_opendir_pid);
                        break;
                }

                dp = page_address(page);
                for (ent = lu_dirent_start(dp); ent != NULL;
                     ent = lu_dirent_next(ent)) {
                        char *name = ent->lde_name;
                        int namelen = le16_to_cpu(ent->lde_namelen);

                        if (namelen == 0)
                                /*
                                 * skip dummy record.
                                 */
                                continue;

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

                        ll_put_page(page);
                        GOTO(out, rc);
                }
                pos = le64_to_cpu(dp->ldp_hash_end);
                ll_put_page(page);
                if (pos == DIR_END_OFF) {
                        /*
                         * End of directory reached.
                         */
                        break;
                } else if (1) {
                        /*
                         * chain is exhausted
                         * Normal case: continue to the next page.
                         */
                        page = ll_get_dir_page(dir, pos, 1, &chain);
                } else {
                        /*
                         * go into overflow page.
                         */
                }
        }
        EXIT;

out:
        ll_dir_chain_fini(&chain);
        return rc;
}

/**
 * Start statahead thread if this is the first dir entry.
 * Otherwise if a thread is started already, wait it until it is ahead of me.
 * \retval 0       -- stat ahead thread process such dentry, for lookup, it miss
 * \retval 1       -- stat ahead thread process such dentry, for lookup, it hit
 * \retval -EEXIST -- stat ahead thread started, and this is the first dentry
 * \retval -EBADFD -- statahead thread exit and not dentry available
 * \retval -EAGAIN -- try to stat by caller
 * \retval others  -- error
 */
int do_statahead_enter(struct inode *dir, struct dentry **dentryp, int lookup)
{
        struct ll_sb_info        *sbi = ll_i2sbi(dir);
        struct ll_inode_info     *lli;
        struct ll_statahead_info *sai = NULL;
        struct dentry            *parent;
        struct l_wait_info        lwi = { 0 };
        int                       rc = 0;
        ENTRY;

        LASSERT(dir != NULL);
        lli = ll_i2info(dir);
        LASSERT(lli->lli_opendir_pid == cfs_curproc_pid());
        sai = lli->lli_sai;

        if (sai) {
                if (unlikely(sa_is_stopped(sai) &&
                             cfs_list_empty(&sai->sai_entries_stated)))
                        RETURN(-EBADFD);

                if ((*dentryp)->d_name.name[0] == '.') {
                        if (likely(sai->sai_ls_all ||
                            sai->sai_miss_hidden >= sai->sai_skip_hidden)) {
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
                                RETURN(-ENOENT);
                        }
                }

                if (sbi->ll_sa_speedup && sai->sai_nextent_is_reg)
                        do_statahead_interpret(sai, 1);

                if (!ll_sai_entry_stated(sai)) {
                        /*
                         * Thread started already, avoid double-stat.
                         * But to avoid blocked for ever, wait at most 30 sec.
                         */
                        lwi = LWI_TIMEOUT_INTR(cfs_time_seconds(30), NULL,
                                               LWI_ON_SIGNAL_NOOP, NULL);
                        rc = l_wait_event(sai->sai_waitq,
                                          ll_sai_entry_stated(sai) ||
                                          sa_is_stopped(sai),
                                          &lwi);
                        if (unlikely(rc < 0))
                                RETURN(rc);
                }

                if (lookup) {
                        struct dentry *result;

                        result = d_lookup((*dentryp)->d_parent,
                                          &(*dentryp)->d_name);
                        if (result) {
                                LASSERT(result != *dentryp);
                                /* BUG 16303: do not drop reference count for
                                 * "*dentryp", VFS will do that by itself. */
                                *dentryp = result;
                                RETURN(1);
                        }
                }
                /*
                 * do nothing for revalidate.
                 */
                RETURN(0);
        }

        /* I am the "lli_opendir_pid" owner, only me can set "lli_sai". */
        rc = is_first_dirent(dir, *dentryp);
        if (rc == LS_NONE_FIRST_DE)
                /* It is not "ls -{a}l" operation, no need statahead for it. */
                GOTO(out, rc = -EAGAIN);

        sai = ll_sai_alloc();
        if (sai == NULL)
                GOTO(out, rc = -ENOMEM);

        sai->sai_ls_all = (rc == LS_FIRST_DOT_DE);
        sai->sai_inode = igrab(dir);
        if (unlikely(sai->sai_inode == NULL)) {
                CWARN("Do not start stat ahead on dying inode "DFID"\n",
                      PFID(&lli->lli_fid));
                GOTO(out, rc = -ESTALE);
        }

        /* get parent reference count here, and put it in ll_statahead_thread */
        parent = dget((*dentryp)->d_parent);
        if (unlikely(sai->sai_inode != parent->d_inode)) {
                struct ll_inode_info *nlli = ll_i2info(parent->d_inode);

                CWARN("Race condition, someone changed %.*s just now: "
                      "old parent "DFID", new parent "DFID"\n",
                      (*dentryp)->d_name.len, (*dentryp)->d_name.name,
                      PFID(&lli->lli_fid), PFID(&nlli->lli_fid));
                dput(parent);
                iput(sai->sai_inode);
                GOTO(out, rc = -EAGAIN);
        }

        lli->lli_sai = sai;
        rc = cfs_kernel_thread(ll_statahead_thread, parent, 0);
        if (rc < 0) {
                CERROR("can't start ll_sa thread, rc: %d\n", rc);
                dput(parent);
                lli->lli_opendir_key = NULL;
                sai->sai_thread.t_flags = SVC_STOPPED;
                ll_sai_put(sai);
                LASSERT(lli->lli_sai == NULL);
                RETURN(-EAGAIN);
        }

        l_wait_event(sai->sai_thread.t_ctl_waitq,
                     sa_is_running(sai) || sa_is_stopped(sai),
                     &lwi);

        /*
         * We don't stat-ahead for the first dirent since we are already in
         * lookup, and -EEXIST also indicates that this is the first dirent.
         */
        RETURN(-EEXIST);

out:
        if (sai != NULL)
                OBD_FREE_PTR(sai);
        cfs_spin_lock(&lli->lli_sa_lock);
        lli->lli_opendir_key = NULL;
        lli->lli_opendir_pid = 0;
        cfs_spin_unlock(&lli->lli_sa_lock);
        return rc;
}

/**
 * update hit/miss count.
 */
void ll_statahead_exit(struct inode *dir, struct dentry *dentry, int result)
{
        struct ll_inode_info     *lli;
        struct ll_statahead_info *sai;
        struct ll_sb_info        *sbi;
        struct ll_dentry_data    *ldd = ll_d2d(dentry);
        int                       rc;
        ENTRY;

        LASSERT(dir != NULL);
        lli = ll_i2info(dir);
        LASSERT(lli->lli_opendir_pid == cfs_curproc_pid());
        sai = lli->lli_sai;
        LASSERT(sai != NULL);
        sbi = ll_i2sbi(dir);

        rc = ll_sai_entry_fini(sai);
        /* rc == -ENOENT means such dentry was removed just between statahead
         * readdir and pre-fetched, count it as hit.
         *
         * result == -ENOENT has two meanings:
         * 1. such dentry was removed just between statahead pre-fetched and
         *    main process stat such dentry.
         * 2. main process stat non-exist dentry.
         * We can not distinguish such two cases, just count them as miss. */
        if (result >= 1 || unlikely(rc == -ENOENT)) {
                sai->sai_hit++;
                sai->sai_consecutive_miss = 0;
                sai->sai_max = min(2 * sai->sai_max, sbi->ll_sa_max);
        } else {
                sai->sai_miss++;
                sai->sai_consecutive_miss++;
                if (sa_low_hit(sai) && sa_is_running(sai)) {
                        atomic_inc(&sbi->ll_sa_wrong);
                        CDEBUG(D_READA, "Statahead for dir "DFID" hit ratio "
                               "too low: hit/miss %u/%u, sent/replied %u/%u, "
                               "stopping statahead thread: pid %d\n",
                               PFID(&lli->lli_fid), sai->sai_hit,
                               sai->sai_miss, sai->sai_sent,
                               sai->sai_replied, cfs_curproc_pid());
                        cfs_spin_lock(&lli->lli_sa_lock);
                        if (!sa_is_stopped(sai))
                                sai->sai_thread.t_flags = SVC_STOPPING;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                }
        }

        if (!sa_is_stopped(sai))
                cfs_waitq_signal(&sai->sai_thread.t_ctl_waitq);
        if (likely(ldd != NULL))
                ldd->lld_sa_generation = sai->sai_generation;

        EXIT;
}
