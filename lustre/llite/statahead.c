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
 * Copyright (c) 2011 Whamcloud, Inc.
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

#define SA_OLD_ENTRY_MAX        8

enum {
        /** negative values are for error cases */
        SA_ENTRY_INIT = 0,      /** init entry */
        SA_ENTRY_SUCC = 1,      /** stat succeed */
        SA_ENTRY_INVA = 2,      /** invalid entry */
        SA_ENTRY_DEST = 3,      /** entry to be destroyed */
};

struct ll_sai_entry {
        cfs_list_t              se_list;
        cfs_list_t              se_hash;
        cfs_atomic_t            se_refcount;
        __u64                   se_index;
        __u64                   se_handle;
        int                     se_stat;
        int                     se_size;
        struct md_enqueue_info *se_minfo;
        struct ptlrpc_request  *se_req;
        struct inode           *se_inode;
        struct qstr             se_qstr;
};

static unsigned int sai_generation = 0;
static cfs_spinlock_t sai_generation_lock = CFS_SPIN_LOCK_UNLOCKED;

static inline int ll_sai_entry_unlinked(struct ll_sai_entry *entry)
{
        return cfs_list_empty(&entry->se_list);
}

static inline int ll_sai_entry_unhashed(struct ll_sai_entry *entry)
{
        return cfs_list_empty(&entry->se_hash);
}

/* The entry only can be released by the caller, it is necessary to hold lock */
static inline int ll_sai_entry_stated(struct ll_sai_entry *entry)
{
        smp_rmb();
        return !!(entry->se_stat != SA_ENTRY_INIT);
}

static inline int ll_sai_entry_hash(int val)
{
        return (val & LL_SA_CACHE_MASK);
}

/* Insert entry to hash SA table */
static inline void
ll_sai_entry_enhash(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        int i = ll_sai_entry_hash(entry->se_qstr.hash);

        cfs_spin_lock(&sai->sai_cache_lock[i]);
        cfs_list_add_tail(&entry->se_hash, &sai->sai_cache[i]);
        cfs_spin_unlock(&sai->sai_cache_lock[i]);
}

/* Remove entry from SA table */
static inline void
ll_sai_entry_unhash(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        int i = ll_sai_entry_hash(entry->se_qstr.hash);

        cfs_spin_lock(&sai->sai_cache_lock[i]);
        cfs_list_del_init(&entry->se_hash);
        cfs_spin_unlock(&sai->sai_cache_lock[i]);
}

static inline int sa_received_empty(struct ll_statahead_info *sai)
{
        return cfs_list_empty(&sai->sai_entries_received);
}

static inline int sa_not_full(struct ll_statahead_info *sai)
{
        return (cfs_atomic_read(&sai->sai_cache_count) < sai->sai_max);
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

/* Insert it into sai_entries_sent tail when init. */
static struct ll_sai_entry *
ll_sai_entry_init(struct ll_statahead_info *sai, __u64 index,
                  const char *name, int len)
{
        struct ll_inode_info *lli;
        struct ll_sai_entry  *entry;
        int                   entry_size;
        char                 *dname;
        ENTRY;

        entry_size = sizeof(struct ll_sai_entry) + ((len >> 2) << 2) + 4;
        OBD_ALLOC(entry, entry_size);
        if (unlikely(entry == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        CDEBUG(D_READA, "alloc sai entry %.*s(%p) index %Lu\n",
               len, name, entry, index);

        entry->se_index = index;
        /* one refcount will be put when entry_fini,
         * another refcount will be put when into received or stated list. */
        cfs_atomic_set(&entry->se_refcount, 2);
        entry->se_stat = SA_ENTRY_INIT;
        entry->se_size = entry_size;
        dname = (char *)entry + sizeof(struct ll_sai_entry);
        memcpy(dname, name, len);
        dname[len] = 0;
        entry->se_qstr.hash = full_name_hash(name, len);
        entry->se_qstr.len = len;
        entry->se_qstr.name = dname;

        lli = ll_i2info(sai->sai_inode);
        cfs_spin_lock(&lli->lli_sa_lock);
        cfs_list_add_tail(&entry->se_list, &sai->sai_entries_sent);
        cfs_spin_unlock(&lli->lli_sa_lock);

        cfs_atomic_inc(&sai->sai_cache_count);
        ll_sai_entry_enhash(sai, entry);

        RETURN(entry);
}

/*
 * Only the caller can remove the entry from hash, it is necessary to hold hash
 * lock. It is caller duty to release the init refcount on the entry, so it is
 * also unnecessary to increase refcount on the entry.
 */
static struct ll_sai_entry *
ll_sai_entry_get_byname(struct ll_statahead_info *sai, const struct qstr *qstr)
{
        struct ll_sai_entry *entry;
        int i = ll_sai_entry_hash(qstr->hash);

        cfs_list_for_each_entry(entry, &sai->sai_cache[i], se_hash) {
                if (entry->se_qstr.hash == qstr->hash &&
                    entry->se_qstr.len == qstr->len &&
                    memcmp(entry->se_qstr.name, qstr->name, qstr->len) == 0)
                        return entry;
        }
        return NULL;
}

/* Inside lli_sa_lock, need increase entry refcount. */
static struct ll_sai_entry *
ll_sai_entry_get_byindex(struct ll_statahead_info *sai, __u64 index)
{
        struct ll_sai_entry *entry;

        cfs_list_for_each_entry(entry, &sai->sai_entries_sent, se_list) {
                if (entry->se_index == index) {
                        cfs_atomic_inc(&entry->se_refcount);
                        return entry;
                } else if (entry->se_index > index) {
                        break;
                }
        }
        return NULL;
}

static void ll_sai_entry_cleanup(struct ll_statahead_info *sai,
                                 struct ll_sai_entry *entry)
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

static void ll_sai_entry_put(struct ll_statahead_info *sai,
                             struct ll_sai_entry *entry)
{
        ENTRY;

        if (cfs_atomic_dec_and_test(&entry->se_refcount)) {
                CDEBUG(D_READA, "free sai entry %.*s(%p) index %Lu\n",
                       entry->se_qstr.len, entry->se_qstr.name, entry,
                       entry->se_index);

                LASSERT(ll_sai_entry_unhashed(entry));
                LASSERT(ll_sai_entry_unlinked(entry));

                ll_sai_entry_cleanup(sai, entry);
                if (entry->se_inode)
                        iput(entry->se_inode);

                OBD_FREE(entry, entry->se_size);
                cfs_atomic_dec(&sai->sai_cache_count);
        }

        EXIT;
}

static inline void
do_sai_entry_fini(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        struct ll_inode_info *lli = ll_i2info(sai->sai_inode);

        ll_sai_entry_unhash(sai, entry);

        cfs_spin_lock(&lli->lli_sa_lock);
        entry->se_stat = SA_ENTRY_DEST;
        if (likely(!ll_sai_entry_unlinked(entry)))
                cfs_list_del_init(&entry->se_list);
        cfs_spin_unlock(&lli->lli_sa_lock);

        ll_sai_entry_put(sai, entry);
}

/* Delete it from sai_entries_stated head when fini. */
static void
ll_sai_entry_fini(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        struct ll_sai_entry *pos, *next;
        ENTRY;

        if (entry)
                do_sai_entry_fini(sai, entry);

        /* drop old entry from sent list */
        cfs_list_for_each_entry_safe(pos, next, &sai->sai_entries_sent,
                                     se_list) {
                if (unlikely(sai->sai_max + pos->se_index + SA_OLD_ENTRY_MAX <
                             sai->sai_index))
                        do_sai_entry_fini(sai, pos);
                else
                        break;
        }

        /* drop old entry from stated list */
        cfs_list_for_each_entry_safe(pos, next, &sai->sai_entries_stated,
                                     se_list) {
                if (unlikely(sai->sai_max + pos->se_index + SA_OLD_ENTRY_MAX <
                             sai->sai_index))
                        do_sai_entry_fini(sai, pos);
                else
                        break;
        }

        EXIT;
}

static void
do_sai_entry_to_stated(struct ll_statahead_info *sai,
                       struct ll_sai_entry *entry, int rc)
{
        struct ll_sai_entry  *se;
        cfs_list_t           *pos = &sai->sai_entries_stated;

        if (!ll_sai_entry_unlinked(entry));
                cfs_list_del_init(&entry->se_list);

        cfs_list_for_each_entry_reverse(se, &sai->sai_entries_stated, se_list) {
                if (se->se_index < entry->se_index) {
                        pos = &se->se_list;
                        break;
                }
        }

        cfs_list_add(&entry->se_list, pos);
        entry->se_stat = rc;
}

/**
 * Move entry to sai_entries_stated and sort with the index.
 * \retval 1    -- entry to be destroyed.
 * \retval 0    -- entry is inserted into stated list.
 */
static int
ll_sai_entry_to_stated(struct ll_statahead_info *sai,
                       struct ll_sai_entry *entry, int rc)
{
        struct ll_inode_info *lli = ll_i2info(sai->sai_inode);
        int                   ret = 1;
        ENTRY;

        ll_sai_entry_cleanup(sai, entry);

        cfs_spin_lock(&lli->lli_sa_lock);
        if (likely(entry->se_stat != SA_ENTRY_DEST)) {
                do_sai_entry_to_stated(sai, entry, rc);
                ret = 0;
        }
        cfs_spin_unlock(&lli->lli_sa_lock);

        RETURN(ret);
}

static struct ll_statahead_info *ll_sai_alloc(void)
{
        struct ll_statahead_info *sai;
        int                       i;
        ENTRY;

        OBD_ALLOC_PTR(sai);
        if (!sai)
                RETURN(NULL);

        cfs_atomic_set(&sai->sai_refcount, 1);
        cfs_spin_lock(&sai_generation_lock);
        sai->sai_generation = ++sai_generation;
        if (unlikely(sai_generation == 0))
                sai->sai_generation = ++sai_generation;
        cfs_spin_unlock(&sai_generation_lock);
        sai->sai_max = LL_SA_RPC_MIN;
        cfs_waitq_init(&sai->sai_waitq);
        cfs_waitq_init(&sai->sai_thread.t_ctl_waitq);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_sent);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_received);
        CFS_INIT_LIST_HEAD(&sai->sai_entries_stated);
        for (i = 0; i < LL_SA_CACHE_SIZE; i++) {
                CFS_INIT_LIST_HEAD(&sai->sai_cache[i]);
                cfs_spin_lock_init(&sai->sai_cache_lock[i]);
        }
        cfs_atomic_set(&sai->sai_cache_count, 0);

        RETURN(sai);
}

static inline struct ll_statahead_info *
ll_sai_get(struct ll_statahead_info *sai)
{
        LASSERT(sai);
        cfs_atomic_inc(&sai->sai_refcount);
        return sai;
}

static void ll_sai_put(struct ll_statahead_info *sai)
{
        struct inode         *inode = sai->sai_inode;
        struct ll_inode_info *lli;
        ENTRY;

        LASSERT(inode != NULL);
        lli = ll_i2info(inode);
        LASSERT(lli->lli_sai == sai);

        if (cfs_atomic_dec_and_lock(&sai->sai_refcount, &lli->lli_sa_lock)) {
                struct ll_sai_entry *entry, *next;

                if (unlikely(cfs_atomic_read(&sai->sai_refcount) > 0)) {
                        /* It is race case, the interpret callback just hold
                         * a reference count */
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        RETURN_EXIT;
                }

                LASSERT(lli->lli_opendir_key == NULL);
                lli->lli_sai = NULL;
                lli->lli_opendir_pid = 0;
                cfs_spin_unlock(&lli->lli_sa_lock);

                LASSERT(sa_is_stopped(sai));

                if (sai->sai_sent > sai->sai_replied)
                        CDEBUG(D_READA,"statahead for dir "DFID" does not "
                              "finish: [sent:%Lu] [replied:%Lu]\n",
                              PFID(&lli->lli_fid),
                              sai->sai_sent, sai->sai_replied);

                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_sent, se_list)
                        do_sai_entry_fini(sai, entry);

                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_received,se_list)
                        do_sai_entry_fini(sai, entry);

                cfs_list_for_each_entry_safe(entry, next,
                                             &sai->sai_entries_stated, se_list)
                        do_sai_entry_fini(sai, entry);

                LASSERT(cfs_atomic_read(&sai->sai_cache_count) == 0);

                iput(inode);
                OBD_FREE_PTR(sai);
        }

        EXIT;
}

/* finish lookup/revalidate. */
static void do_statahead_interpret(struct ll_statahead_info *sai)
{
        struct inode           *dir   = sai->sai_inode;
        struct inode           *child;
        struct ll_inode_info   *lli   = ll_i2info(dir);
        struct ll_sb_info      *sbi   = ll_i2sbi(dir);
        struct ll_sai_entry    *entry;
        struct md_enqueue_info *minfo;
        struct lookup_intent   *it;
        struct ptlrpc_request  *req;
        struct mdt_body        *body;
        int                     rc    = 0;
        ENTRY;

        cfs_spin_lock(&lli->lli_sa_lock);
        if(unlikely(sa_received_empty(sai))) {
                cfs_spin_unlock(&lli->lli_sa_lock);
                RETURN_EXIT;
        }
        entry = cfs_list_entry(sai->sai_entries_received.next,
                               struct ll_sai_entry, se_list);
        cfs_atomic_inc(&entry->se_refcount);
        cfs_list_del_init(&entry->se_list);
        cfs_spin_unlock(&lli->lli_sa_lock);

        minfo = entry->se_minfo;
        it = &minfo->mi_it;
        if (unlikely(it_disposition(it, DISP_LOOKUP_NEG)))
                GOTO(out, rc = -ENOENT);

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
                /* unlinked and re-receated with the same name */
                if (unlikely(!lu_fid_eq(&minfo->mi_data.op_fid2, &body->fid1))) {
                        entry->se_inode = NULL;
                        iput(child);
                        child = NULL;
                }
        }

        rc = ll_prep_inode(&child, req, dir->i_sb);
        if (rc)
                GOTO(out, rc);

        CDEBUG(D_DLMTRACE, "setting l_data to inode %p (%lu/%u)\n",
               child, child->i_ino, child->i_generation);
        md_set_lock_data(sbi->ll_md_exp, &it->d.lustre.it_lock_handle, child,
                         NULL);

        entry->se_handle = it->d.lustre.it_lock_handle;
        entry->se_inode = child;

        EXIT;

out:
        /* The "ll_sai_entry_to_stated()" will drop related ldlm ibits lock
         * reference count with ll_intent_drop_lock() called in spite of the
         * above operations failed or not. Do not worry about calling
         * "ll_intent_drop_lock()" more than once. */
        rc = ll_sai_entry_to_stated(sai, entry, rc < 0 ? : SA_ENTRY_SUCC);
        if (rc == 0 && entry->se_index == sai->sai_index_wait)
                cfs_waitq_signal(&sai->sai_waitq);
        ll_sai_entry_put(sai, entry);
}

static int ll_statahead_interpret(struct ptlrpc_request *req,
                                  struct md_enqueue_info *minfo, int rc)
{
        struct lookup_intent     *it  = &minfo->mi_it;
        struct inode             *dir = minfo->mi_dir;
        struct ll_inode_info     *lli = ll_i2info(dir);
        struct ll_statahead_info *sai = NULL;
        struct ll_sai_entry      *entry;
        ENTRY;

        cfs_spin_lock(&lli->lli_sa_lock);
        /* stale entry */
        if (unlikely(lli->lli_sai == NULL ||
                     lli->lli_sai->sai_generation != minfo->mi_generation)) {
                cfs_spin_unlock(&lli->lli_sa_lock);
                GOTO(out, rc = -ESTALE);
        } else {
                sai = ll_sai_get(lli->lli_sai);
                if (unlikely(!sa_is_running(sai))) {
                        sai->sai_replied++;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        GOTO(out, rc = -EBADFD);
                }

                entry = ll_sai_entry_get_byindex(sai,
                                         (unsigned int)(long)minfo->mi_cbdata);
                if (unlikely(entry == NULL)) {
                        sai->sai_replied++;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        GOTO(out, rc = -EIDRM);
                }

                cfs_list_del_init(&entry->se_list);
                if (rc != 0) {
                        sai->sai_replied++;
                        do_sai_entry_to_stated(sai, entry, rc);
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        if (entry->se_index == sai->sai_index_wait)
                                cfs_waitq_signal(&sai->sai_waitq);
                } else {
                        entry->se_minfo = minfo;
                        entry->se_req = ptlrpc_request_addref(req);
                        cfs_list_add_tail(&entry->se_list, &sai->sai_entries_received);
                        sai->sai_replied++;
                        cfs_spin_unlock(&lli->lli_sa_lock);
                        cfs_waitq_signal(&sai->sai_thread.t_ctl_waitq);
                }
                ll_sai_entry_put(sai, entry);
        }

        EXIT;

out:
        if (rc != 0) {
                ll_intent_release(it);
                iput(dir);
                OBD_FREE_PTR(minfo);
        }
        if (sai != NULL)
                ll_sai_put(sai);
        return rc;
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
                        struct qstr *qstr, struct md_enqueue_info **pmi,
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

static int do_sa_lookup(struct inode *dir, struct ll_sai_entry *entry)
{
        struct md_enqueue_info   *minfo;
        struct ldlm_enqueue_info *einfo;
        struct obd_capa          *capas[2];
        int                       rc;
        ENTRY;

        rc = sa_args_init(dir, NULL, &entry->se_qstr, &minfo, &einfo, capas);
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
static int do_sa_revalidate(struct inode *dir, struct ll_sai_entry *entry,
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

        if (unlikely(dentry == dentry->d_sb->s_root))
                RETURN(1);

        entry->se_inode = igrab(inode);
        rc = md_revalidate_lock(ll_i2mdexp(dir), &it, ll_inode2fid(inode),NULL);
        if (rc == 1) {
                entry->se_handle = it.d.lustre.it_lock_handle;
                ll_intent_release(&it);
                RETURN(1);
        }

        rc = sa_args_init(dir, inode, &entry->se_qstr, &minfo, &einfo, capas);
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

static int ll_statahead_one(struct dentry *parent, const char* entry_name,
                            int entry_name_len)
{
        struct inode             *dir = parent->d_inode;
        struct ll_inode_info     *lli = ll_i2info(dir);
        struct ll_statahead_info *sai = lli->lli_sai;
        struct dentry            *dentry = NULL;
        struct ll_sai_entry      *entry;
        int                       rc;
        int                       rc1;
        ENTRY;

        entry = ll_sai_entry_init(sai, sai->sai_index, entry_name, entry_name_len);
        if (IS_ERR(entry))
                RETURN(PTR_ERR(entry));

        dentry = d_lookup(parent, &entry->se_qstr);
        if (!dentry)
                rc = do_sa_lookup(dir, entry);
        else
                rc = do_sa_revalidate(dir, entry, dentry);

        if (dentry != NULL)
                dput(dentry);

        if (rc) {
                rc1 = ll_sai_entry_to_stated(sai, entry,
                                             rc < 0 ? : SA_ENTRY_SUCC);
                if (rc1 == 0 && entry->se_index == sai->sai_index_wait)
                        cfs_waitq_signal(&sai->sai_waitq);
        } else {
                sai->sai_sent++;
        }

        sai->sai_index++;
        /* drop one refcount on entry by ll_sai_entry_init */
        ll_sai_entry_put(sai, entry);

        RETURN(rc < 0 ? : 0);
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
        cfs_spin_unlock(&lli->lli_sa_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        CDEBUG(D_READA, "start doing statahead for %s\n", parent->d_name.name);

        lli->lli_sa_pos = 0;
        ll_dir_chain_init(&chain);
        page = ll_get_dir_page(NULL, dir, pos, &chain);

        while (1) {
                struct l_wait_info lwi = { 0 };
                struct lu_dirpage *dp;
                struct lu_dirent  *ent;

                if (IS_ERR(page)) {
                        rc = PTR_ERR(page);
                        CDEBUG(D_READA, "error reading dir "DFID" at "LPU64
                               "/%Lu: [rc %d] [parent %u]\n",
                               PFID(ll_inode2fid(dir)), pos, sai->sai_index,
                               rc, lli->lli_opendir_pid);
                        break;
                }

                dp = page_address(page);
                for (ent = lu_dirent_start(dp); ent != NULL;
                     ent = lu_dirent_next(ent)) {
                        __u64 hash;
                        int namelen;
                        char *name;

                        hash = le64_to_cpu(ent->lde_hash);
                        if (unlikely(hash < pos))
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
                        if (unlikely(!first)) {
                                first++;
                                continue;
                        }

keep_de:
                        l_wait_event(thread->t_ctl_waitq,
                                     !sa_is_running(sai) || sa_not_full(sai) ||
                                     !sa_received_empty(sai),
                                     &lwi);

                        while (!sa_received_empty(sai) && sa_is_running(sai))
                                do_statahead_interpret(sai);

                        if (unlikely(!sa_is_running(sai))) {
                                ll_release_page(page, 0);
                                GOTO(out, rc);
                        }

                        if (!sa_not_full(sai))
                                /*
                                 * do not skip the current de.
                                 */
                                goto keep_de;

                        rc = ll_statahead_one(parent, name, namelen);
                        if (rc < 0) {
                                ll_release_page(page, 0);
                                GOTO(out, rc);
                        }
                }
                pos = le64_to_cpu(dp->ldp_hash_end);
                if (pos == MDS_DIR_END_OFF) {
                        /*
                         * End of directory reached.
                         */
                        ll_release_page(page, 0);
                        while (1) {
                                l_wait_event(thread->t_ctl_waitq,
                                             !sa_is_running(sai) ||
                                             !sa_received_empty(sai) ||
                                             sai->sai_sent == sai->sai_replied,
                                             &lwi);
                                if (!sa_received_empty(sai) &&
                                    sa_is_running(sai))
                                        do_statahead_interpret(sai);
                                else
                                        GOTO(out, rc);
                        }
                } else if (1) {
                        /*
                         * chain is exhausted.
                         * Normal case: continue to the next page.
                         */
                        ll_release_page(page, le32_to_cpu(dp->ldp_flags) &
                                              LDF_COLLIDE);
                        lli->lli_sa_pos = pos;
                        page = ll_get_dir_page(NULL, dir, pos, &chain);
                } else {
                        /*
                         * go into overflow page.
                         */
                        ll_release_page(page, 0);
                }
        }
        EXIT;

out:
        ll_dir_chain_fini(&chain);
        cfs_spin_lock(&lli->lli_sa_lock);
        thread->t_flags = SVC_STOPPED;
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
        struct ll_inode_info *lli = ll_i2info(dir);
        struct ll_dir_chain chain;
        struct qstr        *target = &dentry->d_name;
        struct page        *page;
        __u64               pos = 0;
        int                 dot_de;
        int                 rc = LS_NONE_FIRST_DE;
        ENTRY;

        lli->lli_sa_pos = 0;
        ll_dir_chain_init(&chain);
        page = ll_get_dir_page(NULL, dir, pos, &chain);

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
                        __u64 hash;
                        int namelen;
                        char *name;

                        hash = le64_to_cpu(ent->lde_hash);
                        if (unlikely(hash < pos))
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

                        ll_release_page(page, 0);
                        GOTO(out, rc);
                }
                pos = le64_to_cpu(dp->ldp_hash_end);
                if (pos == MDS_DIR_END_OFF) {
                        /*
                         * End of directory reached.
                         */
                        ll_release_page(page, 0);
                        break;
                } else if (1) {
                        /*
                         * chain is exhausted
                         * Normal case: continue to the next page.
                         */
                        ll_release_page(page, le32_to_cpu(dp->ldp_flags) &
                                              LDF_COLLIDE);
                        lli->lli_sa_pos = pos;
                        page = ll_get_dir_page(NULL, dir, pos, &chain);
                } else {
                        /*
                         * go into overflow page.
                         */
                        ll_release_page(page, 0);
                }
        }
        EXIT;

out:
        ll_dir_chain_fini(&chain);
        return rc;
}

static void
ll_sai_unplug(struct ll_statahead_info *sai, struct ll_sai_entry *entry)
{
        struct ll_sb_info *sbi = ll_i2sbi(sai->sai_inode);
        int                hit = entry ? 1 : 0;
        ENTRY;

        ll_sai_entry_fini(sai, entry);
        if (hit) {
                sai->sai_hit++;
                sai->sai_consecutive_miss = 0;
                sai->sai_max = min(2 * sai->sai_max, sbi->ll_sa_max);
        } else {
                struct ll_inode_info *lli = ll_i2info(sai->sai_inode);

                sai->sai_miss++;
                sai->sai_consecutive_miss++;
                if (sa_low_hit(sai) && sa_is_running(sai)) {
                        atomic_inc(&sbi->ll_sa_wrong);
                        CDEBUG(D_READA, "Statahead for dir "DFID" hit ratio "
                               "too low: hit/miss %Lu/%Lu, sent/replied %Lu/%Lu, "
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

        EXIT;
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
        struct ll_inode_info     *lli   = ll_i2info(dir);
        struct ll_statahead_info *sai   = lli->lli_sai;
        struct dentry            *parent;
        struct ll_sai_entry      *entry;
        struct l_wait_info        lwi   = { 0 };
        int                       rc    = 0;
        ENTRY;

        LASSERT(lli->lli_opendir_pid == cfs_curproc_pid());

        if (sai) {
                if (unlikely(sa_is_stopped(sai) &&
                             cfs_list_empty(&sai->sai_entries_stated)))
                        RETURN(-EAGAIN);

                if ((*dentryp)->d_name.name[0] == '.') {
                        if (likely(sai->sai_ls_all ||
                                   sai->sai_miss_hidden >=
                                   sai->sai_skip_hidden)) {
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

                entry = ll_sai_entry_get_byname(sai, &(*dentryp)->d_name);
                if (entry == NULL || only_unplug) {
                        ll_sai_unplug(sai, entry);
                        RETURN(entry ? 1 : -EAGAIN);
                }

                if (!ll_sai_entry_stated(entry)) {
                        sai->sai_index_wait = entry->se_index;
                        lwi = LWI_TIMEOUT_INTR(cfs_time_seconds(30), NULL,
                                               LWI_ON_SIGNAL_NOOP, NULL);
                        rc = l_wait_event(sai->sai_waitq,
                                          ll_sai_entry_stated(entry) ||
                                          sa_is_stopped(sai),
                                          &lwi);
                        if (unlikely(rc < 0)) {
                                ll_sai_unplug(sai, entry);
                                RETURN(-EAGAIN);
                        }
                }

                if (entry->se_stat == SA_ENTRY_SUCC &&
                    entry->se_inode != NULL) {
                        struct inode *inode = entry->se_inode;
                        struct lookup_intent it = { .it_op = IT_GETATTR,
                                                    .d.lustre.it_lock_handle =
                                                     entry->se_handle };
                        struct ll_dentry_data *lld;
                        __u32 bits;

                        rc = md_revalidate_lock(ll_i2mdexp(dir), &it,
                                                ll_inode2fid(inode), &bits);
                        if (rc == 1) {
                                if ((*dentryp)->d_inode == NULL) {
                                        *dentryp = ll_find_alias(inode, *dentryp);
                                        lld = ll_d2d(*dentryp);
                                        if (unlikely(lld == NULL))
                                                ll_dops_init(*dentryp, 1, 1);
                                } else {
                                        LASSERT((*dentryp)->d_inode == inode);

                                        cfs_spin_lock(&ll_lookup_lock);
                                        spin_lock(&dcache_lock);
                                        lock_dentry(*dentryp);
                                        __d_drop(*dentryp);
                                        unlock_dentry(*dentryp);
                                        d_rehash_cond(*dentryp, 0);
                                        spin_unlock(&dcache_lock);
                                        cfs_spin_unlock(&ll_lookup_lock);
                                        iput(inode);
                                }
                                entry->se_inode = NULL;

                                if (bits & MDS_INODELOCK_LOOKUP) {
                                        lock_dentry(*dentryp);
                                        (*dentryp)->d_flags &= ~DCACHE_LUSTRE_INVALID;
                                        unlock_dentry(*dentryp);
                                }
                                ll_intent_release(&it);
                        }
                }

                ll_sai_unplug(sai, entry);
                RETURN(rc);
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
