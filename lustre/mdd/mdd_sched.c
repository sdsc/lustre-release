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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mdd/mdd_sched.c
 *
 * Lustre Metadata Server (mdd) routines
 *
 * Author: Liang Zhen <liang@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>
#include <obd.h>
#include <obd_class.h>
#include <lustre_ver.h>
#include <obd_support.h>
#include <lprocfs_status.h>

#include <lustre_mds.h>
#include <lustre/lustre_idl.h>

#include "mdd_internal.h"

enum {
        /** # threads on each CPU for MDD transaction */
        MDD_TRANS_THREADS_NUM   = 4,
        /** max # threads on each CPU for MDD transaction */
        MDD_TRANS_THREADS_MAX   = 16,
};

enum {  /** bucket bits for global hash */
        MDD_OBJ_GCACHE_BBITS    = 4,
};

enum {
        /** default cache depth */
        MDD_OBJ_LCACHE_DEP      = 8,
        /** max cache depth */
        MDD_OBJ_LCACHE_DEP_MAX  = 16,
};

enum {  /** percpu cache hash bits */
        MDD_OBJ_LCACHE_HBITS    = 3,
};

enum {  /** max number of local cache */
        MDD_OBJ_LCACHE_MAX      = 128,
};

enum {
        /** # CPUs for processing transaction of directory */
        MDD_OBJ_CPU_MIN         = 1,
        /** default # CPUs for processing transaction of directory */
        MDD_OBJ_CPU_DEFAULT     = 4,
        /**
         * we don't bother to do this if performance of creation/removal
         * under shared dir is good enough on 8+ cores.
         */
        MDD_OBJ_CPU_MAX         = 8,
};

enum {
        MDD_OBJ_ENQUEUE_MIN     = (1 << 1),
        /** enqueue to global cache if more than 32 local refcount */
        MDD_OBJ_ENQUEUE_DEFAULT = (1 << 5),
        MDD_OBJ_ENQUEUE_MAX     = (1 << 12),
};

enum {  /** abandon cached fid older than 10 seconds */
        MDD_OBJ_STAMP_OLD       = 10,
};

unsigned trans_threads_num = MDD_TRANS_THREADS_NUM;
CFS_MODULE_PARM(trans_threads_num, "i", int, 0444,
                "# mdd transaction threads");

unsigned obj_cpu_num = MDD_OBJ_CPU_DEFAULT;
CFS_MODULE_PARM(obj_cpu_num, "i", int, 0644,
                "number of CPUs for each object");

unsigned obj_cache_dep = 0;
CFS_MODULE_PARM(obj_cache_dep, "i", int, 0644,
                "cache depth on each CPU");

unsigned obj_enqueue_lw = MDD_OBJ_ENQUEUE_DEFAULT;
CFS_MODULE_PARM(obj_enqueue_lw, "i", int, 0444,
                "refcount low water for adding object to global cache");

typedef struct {
        /** object fid */
        struct lu_fid           mfo_fid;
        /** chain on global hash table */
        cfs_hlist_node_t        mfo_hnode;
        /** refcount */
        cfs_atomic_t            mfo_ref;
        /** dedicated CPUs for this object */
        short                   mfo_cpus[MDD_OBJ_CPU_MAX];
} mdd_fid_object_t;

typedef struct {
        /** object fid */
        struct lu_fid           mfc_fid;
        /** hit counter */
        unsigned                mfc_hit;
        /** time stamp of the last hit */
        unsigned                mfc_stamp;
        /** refer to object in global hash */
        mdd_fid_object_t       *mfc_obj;
} mdd_fid_cache_t;

#define MDD_DIR_CACHE_SIZE     (MDD_OBJ_LCACHE_DEP_MAX << MDD_OBJ_LCACHE_HBITS)

typedef struct {
        /** temporary buffer for shifting cached objects */
        mdd_fid_cache_t         mcc_swappers[2];
        /** it's a hash cache */
        mdd_fid_cache_t         mcc_caches[MDD_DIR_CACHE_SIZE];
} mdd_dir_cache_cpud_t;

/**
 * hash bucket data for dir cache
 */
typedef struct {
        /** list of preallocated objects */
        cfs_hlist_head_t        chd_obj_idle;
        /** CPU ID for object */
        unsigned                chd_cpuid;
} mdd_dir_cache_hasd_t;

typedef struct {
        /** global hash for cached dir fid */
        cfs_hash_t             *mdc_obj_hash;
        /** scalable lock and percpu cache */
        cfs_scale_lock_t       *mdc_sclock;
        /** roundrobin CPU id */
        unsigned                mdc_ncpus;
        /** # CPUs for each cached object */
        unsigned                mdc_obj_ncpu;
        /** depth of local CPU cache */
        unsigned                mdc_cpu_cache_dep;
        /** low water to enqueue global cacneh */
        unsigned                mdc_gcache_lw;
        /** see comment in mdd_dir_cache_find */
        unsigned                mdc_lock_shift;
} mdd_dir_cache_t;

static mdd_dir_cache_t          mdd_dir_cache;

typedef struct mdd_trans_sched {
        /** serialize */
        cfs_spinlock_t          mts_lock;
        /** CPU id */
        unsigned                mts_cpuid;
        /** transaction driving threads wait at here */
        cfs_waitq_t             mts_waitq;
        /** # started threads */
        cfs_atomic_t            mts_nstarted;
        /** # stopped threads */
        cfs_atomic_t            mts_nstopped;
        /** list of pending transaction requests */
        cfs_list_t              mts_req_list;
        /** # pending transaction requests, reserved for debug */
        int                     mts_req_pending;
        /** # in progress transaction requests, reserved for debug */
        int                     mts_req_active;
} mdd_trans_sched_t;

typedef struct mdd_trans_sched_data {
        /** threads controller wait at here */
        cfs_waitq_t             mtd_waitq;
        /** bitmap of enabled transaction operations */
        unsigned long           mtd_ops_enabled;
        /** is shutting down */
        unsigned                mtd_shutdown;
        /** # threads for each CPU node */
        unsigned                mtd_sched_nthr;
        /** percpu data for transaction schedulers */
        mdd_trans_sched_t     **mtd_scheds;
} mdd_trans_data_t;

static mdd_trans_data_t         mdd_trans_data;

/**
 * release refcount of cached object, recycle it if it's the last refcount
 */
static void
mdd_fid_cache_obj_put(mdd_fid_cache_t *fcache)
{
        cfs_hash_t           *hash = mdd_dir_cache.mdc_obj_hash;
        mdd_fid_object_t     *obj  = fcache->mfc_obj;
        mdd_dir_cache_hasd_t *hasd;
        cfs_hash_bd_t         bd;

        cfs_hash_bd_get(hash, &fcache->mfc_fid, &bd);
        if (!cfs_hash_bd_dec_and_lock(hash, &bd, &obj->mfo_ref))
                return;

        /* remove mdd_fid_object from global cache (hash) */
        cfs_hash_bd_del_locked(hash, &bd, &obj->mfo_hnode);

        hasd = cfs_hash_bd_extra_get(hash, &bd);
        cfs_hlist_add_head(&obj->mfo_hnode, &hasd->chd_obj_idle);

        cfs_hash_bd_unlock(hash, &bd, 1);
}

/**
 * find cached object from global hash table, or create one
 * if it's not there.
 */
static mdd_fid_object_t *
mdd_fid_cache_obj_get(const struct lu_fid *fid)
{
        cfs_hash_t           *hash = mdd_dir_cache.mdc_obj_hash;
        mdd_dir_cache_hasd_t *hasd;
        cfs_hlist_node_t     *hnode;
        mdd_fid_object_t     *obj;
        cfs_hash_bd_t         bd;
        int                   i;

        cfs_hash_bd_get_and_lock(hash, (void *)fid, &bd, 1);

        hnode = cfs_hash_bd_lookup_locked(hash, &bd, (void *)fid);
        if (hnode != NULL) {
                cfs_hash_bd_unlock(hash, &bd, 1);
                return cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);
        }

        hasd = cfs_hash_bd_extra_get(hash, &bd);
        if (likely(!cfs_hlist_empty(&hasd->chd_obj_idle))) {
                /* most likely, we have preallocated enough */
                hnode = hasd->chd_obj_idle.first;
                cfs_hlist_del(hnode);
                obj = cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);

        } else { /* however... */
                OBD_ALLOC_GFP(obj, sizeof(*obj), CFS_ALLOC_ATOMIC_TRY);
                if (unlikely(obj == NULL)) {
                        /* don't need to complain on failutre */
                        cfs_hash_bd_unlock(hash, &bd, 1);
                        return NULL;
                }
        }

        obj->mfo_fid = *fid;
        cfs_atomic_set(&obj->mfo_ref, 1);
        for (i = 0; i < mdd_dir_cache.mdc_obj_ncpu; i++) {
                /* dedicated CPU for this object */
                obj->mfo_cpus[i] = hasd->chd_cpuid++ % mdd_dir_cache.mdc_ncpus;
        }

        cfs_hash_bd_add_locked(hash, &bd, hnode);
        cfs_hash_bd_unlock(hash, &bd, 1);

        return obj;
}

static inline int
mdd_dir_cache_disabled(void)
{
        return mdd_trans_data.mtd_sched_nthr == 0 ||
               mdd_dir_cache.mdc_cpu_cache_dep == 0;
}

/**
 * try to find @fid in @fcache:
 * - if @fid is already in @fcache, it will be poped to top of @fcache
 * - if @ifd is not in @fcache, it's enqueued to top of @fcache, all other
 *   items will be shifted down, the last one will be removed from cache
 */
static mdd_fid_object_t *
mdd_dir_cache_obj_find(mdd_dir_cache_cpud_t *ccache,
                       mdd_fid_cache_t *fcache, const struct lu_fid *fid)
{
        mdd_fid_object_t *obj = NULL;
        unsigned sec  = cfs_time_current_sec();
        __u8 nuke     = (fid == NULL);
        __u8 cached   = 0;
        __u8 swapper  = 0;
        int i;

        for (i = 0; i < mdd_dir_cache.mdc_cpu_cache_dep; i++) {
                __u8 valid = !lu_fid_invalid(&fcache[i].mfc_fid);
                __u8 last  = (mdd_dir_cache.mdc_cpu_cache_dep - 1) == i;
                __u8 hit   = !nuke && valid &&
                             lu_fid_eq(fid, &fcache[i].mfc_fid);
                __u8 old   = valid && (sec - fcache[i].mfc_stamp >=
                                       MDD_OBJ_STAMP_OLD);

                /*
                 * valid  : current cache-item has valid fid
                 * last   : it's the last cache-item
                 * hit    : current cache-item can match @fid
                 * old    : time stamp of current cache-item is old
                 * cached : @fid has been enqueued into cache
                 * nuke   : don't need to enqueue anything, just want
                 *          to nuke all items with old timestamp
                 */
                if (old || (!nuke && last && valid && !cached && !hit)) {
                        /*
                         * we abandon object in local cache if:
                         *   a) cached object is old (more than 10 secs)
                         *   b) it's the last one and not a hit
                         */
                        if (fcache[i].mfc_obj != NULL) {
                                mdd_fid_cache_obj_put(&fcache[i]);
                                fcache[i].mfc_obj = NULL;
                        }
                        lu_fid_invalidate(&fcache[i].mfc_fid);
                        fcache[i].mfc_hit = 0;
                        valid = 0;
                        /*
                         * NB: if new object replaced an "old" object in cache,
                         * we have to pop all out old objects right now to make
                         * sure there is no duplicated refs on new object.
                         *
                         * Of course, we need to do this for "nuke" as well.
                         */
                        if (cached || nuke)
                                continue;

                } else if (cached || nuke) {
                        /* no more old object */
                        return obj;
                }

                if (!valid) { /* push into cache */
                        fcache[i].mfc_fid = *fid;
                        hit = 1;
                }

                if (!hit) { /* shift cached item */
                        ccache->mcc_swappers[!swapper] = fcache[i];
                        if (i > 0)
                                fcache[i] = ccache->mcc_swappers[swapper];
                        swapper = !swapper;
                        continue;
                }

                cached = 1;
                fcache[i].mfc_hit++; /* can overflow but harmless */
                fcache[i].mfc_stamp = sec;
                if (fcache[i].mfc_obj == NULL &&
                    fcache[i].mfc_hit >= mdd_dir_cache.mdc_gcache_lw) {
                        /* mdd_fid_cache_obj_get can fail(very unlikely),
                         * do nothing for it */
                        fcache[i].mfc_obj = mdd_fid_cache_obj_get(fid);
                }

                if (i > 0) { /* move the hit item to top of cache */
                        fcache[0] = fcache[i];
                        fcache[i] = ccache->mcc_swappers[swapper];
                }
                obj = fcache[i].mfc_obj;
        }
        return obj;
}

/**
 * Return CPU id to processing @fid. if @fid == NULL, it's a nuke and
 * will remove all old objects from local CPU hash:
 *      rc >= 0 : CPU id for processing transaction for @fid
 *      rc <  0 : just process transaction in caller's thread context
 */
static int
mdd_dir_cache_find(const struct lu_fid *fid)
{
        mdd_fid_object_t     *obj = NULL;
        mdd_dir_cache_cpud_t *ccache;
        mdd_fid_cache_t      *fcache;
        unsigned long         hash  = 0;
        int                   start = 0;
        int                   end   = 0;
        int                   target;
        int                   lockid;
        int                   cpuid;
        int                   i;

        if (mdd_dir_cache_disabled())
                return -1;

        if (likely(MDD_OBJ_LCACHE_HBITS > 0)) {
                /*
                 * NB: MDD_OBJ_LCACHE_HBITS == 0 is only for debug
                 *
                 * local cache is a small hash-table, if it's a nuke we
                 * need to iterate over all buckets of the hash, otherwise
                 * we only need to search on one hashed bucket.
                 */
                if (fid != NULL) {
                        hash = fid_seq(fid) + fid_oid(fid);
                        hash = cfs_hash_long(hash, MDD_OBJ_LCACHE_HBITS);
                        start = end = (int)hash;
                } else {
                        /* caller wants to nuke old items in local cache */
                        end = (1 << MDD_OBJ_LCACHE_HBITS) - 1;
                }
        }

        cpuid = cfs_cpu_current();
        /* NB: if server has many many cores(i.e: 1024), it will take long
         * time to activate mdd_trans_scheduler, because each core can has
         * (32 - 1) local refcount before enqueue the object in global hash,
         * in the worst case we have (32 - 1) * 1024 local refcount before
         * we can schedule transaction on mdd_trans_scheduler
         * So we shadow cpuid to lockid at here, and we will have max to
         * (32 - 1) * 128 local refcount before schedule transaction in
         * mdd_trans_scheduler which is reasonable.
         */
        lockid = mdd_dir_cache.mdc_lock_shift ==  0 ?
                 cpuid : (cpuid >> mdd_dir_cache.mdc_lock_shift);
        ccache = cfs_sclock_index_data(mdd_dir_cache.mdc_sclock, lockid);

        cfs_sclock_lock(mdd_dir_cache.mdc_sclock, lockid);

        if (unlikely(mdd_dir_cache_disabled())) { /* not changed at runtime */
                cfs_sclock_unlock(mdd_dir_cache.mdc_sclock, lockid);
                return -1;
        }

        for (i = start; i <= end; i++) {
                fcache = &ccache->mcc_caches[i * MDD_OBJ_LCACHE_DEP_MAX];
                obj = mdd_dir_cache_obj_find(ccache, fcache, fid);
        }

        if (obj == NULL || /* no cached object */
            cfs_atomic_read(&obj->mfo_ref) <= mdd_dir_cache.mdc_obj_ncpu) {
                cfs_sclock_unlock(mdd_dir_cache.mdc_sclock, lockid);
                return -1;
        }

        for (i = 0; i < mdd_dir_cache.mdc_obj_ncpu; i++) {
                if (obj->mfo_cpus[i] == cpuid) {
                        /* no need to switch to a different CPU */
                        cfs_sclock_unlock(mdd_dir_cache.mdc_sclock, lockid);
                        return -1;
                }
        }

        /* shadow current CPU to a dedicated CPU */
        target = obj->mfo_cpus[cpuid % mdd_dir_cache.mdc_obj_ncpu];
        cfs_sclock_unlock(mdd_dir_cache.mdc_sclock, lockid);

        LASSERT(target >= 0);

        return target;
}

/**
 * change depth of local cache at runtime
 * we need to abandon cached objects if we decreased cache depth.
 * if we change it to ZERO, dir-cache is disabled.
 */
int
mdd_dir_cache_depth_adjust(unsigned cache_depth)
{
        int     i;
        int     j;
        int     k;

        if (cfs_cpu_node_num() < MDD_OBJ_CPU_MAX ||
            mdd_dir_cache.mdc_sclock == NULL) {
                CERROR("mdd object cache is disabled\n");
                return -EPERM;
        }

        cache_depth = min(cache_depth, (unsigned)MDD_OBJ_LCACHE_DEP_MAX);

        if (cache_depth == mdd_dir_cache.mdc_cpu_cache_dep)
                return 0;

        cfs_sclock_lock_all(mdd_dir_cache.mdc_sclock);
        if (cache_depth == mdd_dir_cache.mdc_cpu_cache_dep) { /* recheck ... */
                cfs_sclock_unlock_all(mdd_dir_cache.mdc_sclock);
                return 0;
        }

        /* NB: it's not quite SMP safe on large scale SMP system,
         * by defaut each CPU can cache max to 64 objects, for system has
         * 128 cores, we iterate max to 8K objects with spinlock */
        cfs_sclock_for_each(i, mdd_dir_cache.mdc_sclock) {
                mdd_dir_cache_cpud_t *ccache;

                ccache = cfs_sclock_index_data(mdd_dir_cache.mdc_sclock, i);
                for (j = 0; j < (1 << MDD_OBJ_LCACHE_HBITS); j++) {
                        mdd_fid_cache_t      *fcc;

                        fcc = &ccache->mcc_caches[j * MDD_OBJ_LCACHE_DEP_MAX];
                        /* it will do nothing if new depth is larger
                         * than current depth */
                        for (k = cache_depth;
                             k < mdd_dir_cache.mdc_cpu_cache_dep; k++) {
                                if (lu_fid_invalid(&fcc[k].mfc_fid)) {
                                        LASSERT(fcc[k].mfc_obj == NULL);
                                        break;
                                }

                                if (fcc[k].mfc_obj != NULL)
                                        mdd_fid_cache_obj_put(&fcc[k]);

                                memset(&fcc[k], 0, sizeof(fcc[k]));
                        }
                }
        }
        mdd_dir_cache.mdc_cpu_cache_dep = cache_depth;

        cfs_sclock_unlock_all(mdd_dir_cache.mdc_sclock);
        return 0;
}

/**
 * change number of CPUs to schedule transactions on for each directory.
 * we can't change it to zero.
 */
int
mdd_dir_cache_ncpu_adjust(unsigned ncpu)
{
        cfs_hash_t       *hs = mdd_dir_cache.mdc_obj_hash;
        cfs_hlist_head_t *hhead;
        cfs_hlist_node_t *tmp;
        mdd_fid_object_t *fo;
        cfs_hash_bd_t     bd;
        int               i;
        int               j;

        if (cfs_cpu_node_num() < MDD_OBJ_CPU_MAX || hs == NULL) {
                CERROR("mdd object cache is disabled\n");
                return -EPERM;
        }

        if (ncpu == 0 || ncpu == mdd_dir_cache.mdc_obj_ncpu)
                return 0;

        ncpu = min(ncpu, (unsigned)MDD_OBJ_CPU_MAX);

        LASSERT(mdd_dir_cache.mdc_obj_ncpu > 0);

        cfs_sclock_lock_all(mdd_dir_cache.mdc_sclock);
        if (ncpu <= mdd_dir_cache.mdc_obj_ncpu)
                goto out;

        /*
         * ncpu > mdd_dir_cache.mdc_obj_ncpu
         *
         * NB: don't call cfs_hash_for_each(), because it might reschedule.
         *
         * NB: it's not quite SMP safe on large scale SMP system,
         * by defaut each CPU can cache max to 64 objects, for system has
         * 128 cores, we iterate max to 8K objects with spinlock
         */
        cfs_hash_for_each_bucket(hs, &bd, i) {
                cfs_hash_bd_for_each_hlist(hs, &bd, hhead) {
                        cfs_hlist_for_each_entry(fo, tmp, hhead, mfo_hnode) {
                                for (j = mdd_dir_cache.mdc_obj_ncpu;
                                     j < ncpu; j++) {
                                        fo->mfo_cpus[j] = fo->mfo_cpus[j - 1];
                                        fo->mfo_cpus[j] += 1;
                                        fo->mfo_cpus[j] %= cfs_cpu_node_num();
                                }
                        }
                }
        }

 out:
        mdd_dir_cache.mdc_obj_ncpu = ncpu;
        cfs_sclock_unlock_all(mdd_dir_cache.mdc_sclock);

        return 0;
}

static unsigned
mdd_dcache_hop_hash(cfs_hash_t *hs, void *key, unsigned mask)
{
        struct lu_fid *fid = (struct lu_fid *)key;

        return (fid_seq(fid) + fid_oid(fid)) & mask;
}

static void *
mdd_dcache_hop_object(cfs_hlist_node_t *hnode)
{
        return cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);
}

static void *
mdd_dcache_hop_key(cfs_hlist_node_t *hnode)
{
        mdd_fid_object_t *obj;

        obj = cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);
        return &obj->mfo_fid;
}

static int
mdd_dcache_hop_keycmp(void *key, cfs_hlist_node_t *hnode)
{
        mdd_fid_object_t *obj;

        obj = cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);
        return lu_fid_eq(&obj->mfo_fid, (struct lu_fid *)key);
}

static void
mdd_dcache_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        mdd_fid_object_t *obj;

        obj = cfs_hlist_entry(hnode, mdd_fid_object_t, mfo_hnode);
        cfs_atomic_inc(&obj->mfo_ref);
}

static void
mdd_dcache_hop_put_locked(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
        LBUG(); /* we should never call it */
}

cfs_hash_ops_t mdd_dir_cache_hops = {
        . hs_hash       = mdd_dcache_hop_hash,
        . hs_key        = mdd_dcache_hop_key,
        . hs_keycmp     = mdd_dcache_hop_keycmp,
        . hs_object     = mdd_dcache_hop_object,
        . hs_get        = mdd_dcache_hop_get,
        . hs_put_locked = mdd_dcache_hop_put_locked,
};

/**
 * finalize MDD dir cache
 */
static void
mdd_dir_cache_fini(void)
{
        cfs_hash_t    *hs;
        cfs_hash_bd_t  bd;
        int            i;

        if (mdd_dir_cache.mdc_sclock == NULL)
                return;

        /* empty cache */
        mdd_dir_cache_depth_adjust(0);

        hs = mdd_dir_cache.mdc_obj_hash;
        if (hs == NULL)
                goto out;

        cfs_hash_for_each_bucket(hs, &bd, i) {
                mdd_dir_cache_hasd_t *hasd = cfs_hash_bd_extra_get(hs, &bd);

                /* no lock is needed at this point */
                while (!cfs_hlist_empty(&hasd->chd_obj_idle)) {
                        mdd_fid_object_t *obj;

                        obj = cfs_hlist_entry(hasd->chd_obj_idle.first,
                                              mdd_fid_object_t, mfo_hnode);
                        LASSERT_ATOMIC_ZERO(&obj->mfo_ref);
                        cfs_hlist_del(&obj->mfo_hnode);
                        OBD_FREE_PTR(obj);
                }
        }

        cfs_hash_putref(hs);
        mdd_dir_cache.mdc_obj_hash = NULL;

out:
        cfs_sclock_free(mdd_dir_cache.mdc_sclock);
        mdd_dir_cache.mdc_sclock = NULL;
}

/**
 * initialize MDD dir cache
 */
static int
mdd_dir_cache_init(void)
{
        cfs_hash_t       *hs;
        cfs_hash_bd_t     bd;
        int               ncpus;
        int               hsize;
        int               hbits;
        int               i;
        int               j;

        memset(&mdd_dir_cache, 0, sizeof(mdd_dir_cache));
        if (cfs_cpu_node_num() < MDD_OBJ_CPU_MAX)
                return 0;

        ncpus = cfs_cpu_node_num();
        while (ncpus > MDD_OBJ_LCACHE_MAX) {
                /* NB: see comment in mdd_dir_cache_find for detail */
                mdd_dir_cache.mdc_lock_shift++;
                ncpus >>= 1;
        }

        /* validate obj_cpu_num */
        obj_cpu_num = max(obj_cpu_num, (unsigned)MDD_OBJ_CPU_MIN);
        obj_cpu_num = min(obj_cpu_num, (unsigned)MDD_OBJ_CPU_MAX);
        obj_cpu_num = min(obj_cpu_num, (unsigned)(cfs_cpu_node_num() / 2));

        /* validate obj_enqueue_lw */
        obj_enqueue_lw = max(obj_enqueue_lw, (unsigned)MDD_OBJ_ENQUEUE_MIN);
        obj_enqueue_lw = min(obj_enqueue_lw, (unsigned)MDD_OBJ_ENQUEUE_MAX);

        /* obj_cache_dep can be zero */
        obj_cache_dep = min(obj_cache_dep, (unsigned)MDD_OBJ_LCACHE_DEP_MAX);

        mdd_dir_cache.mdc_cpu_cache_dep = obj_cache_dep;
        mdd_dir_cache.mdc_gcache_lw     = obj_enqueue_lw;
        mdd_dir_cache.mdc_obj_ncpu      = obj_cpu_num;
        mdd_dir_cache.mdc_ncpus         = cfs_cpu_node_num();

        mdd_dir_cache.mdc_sclock =
                cfs_sclock_alloc(sizeof(mdd_dir_cache_cpud_t));

        if (mdd_dir_cache.mdc_sclock == NULL)
                return -ENOMEM;

        hsize = min(MDD_OBJ_LCACHE_MAX, (int)cfs_cpu_node_num()) *
                MDD_DIR_CACHE_SIZE;
        hbits = min(13, (int)cfs_ffz(~(unsigned long)hsize));

        LASSERT(hbits > MDD_OBJ_GCACHE_BBITS);

        /* NB: mdc_obj_hash should be a low contention hash, so just allocate
         * one bucket to simplify logic of preallocating */
        hs = cfs_hash_create("MDD_DIR_CACHE",
                             hbits, hbits, hbits - MDD_OBJ_GCACHE_BBITS,
                             sizeof(mdd_dir_cache_hasd_t),
                             CFS_HASH_MIN_THETA, CFS_HASH_MAX_THETA,
                             &mdd_dir_cache_hops,
                             CFS_HASH_NO_ITEMREF |
                             CFS_HASH_SPIN_BKTLOCK |
                             CFS_HASH_ASSERT_EMPTY);
        if (hs == NULL) {
                mdd_dir_cache_fini();
                return -ENOMEM;
        }

        mdd_dir_cache.mdc_obj_hash = hs;

        cfs_hash_for_each_bucket(hs, &bd, i) {
                mdd_dir_cache_hasd_t *hasd = cfs_hash_bd_extra_get(hs, &bd);

                /* initialize it before allocating anything, simplify
                 * rollback on failure */
                CFS_INIT_HLIST_HEAD(&hasd->chd_obj_idle);
        }

        cfs_hash_for_each_bucket(hs, &bd, i) {
                mdd_dir_cache_hasd_t *hasd = cfs_hash_bd_extra_get(hs, &bd);

                hasd->chd_cpuid = (i * mdd_dir_cache.mdc_obj_ncpu) %
                                  mdd_dir_cache.mdc_ncpus;
                /* NB: we always try to preallocate enough objects,
                 * but it's OK if we got unbalanced number of objects on
                 * each hash bucket, because we can try to allocate(atomic)
                 * at runtime */
                for (j = 0; j < (hsize >> MDD_OBJ_GCACHE_BBITS); j++) {
                        mdd_fid_object_t *obj;

                        OBD_ALLOC_PTR(obj);
                        if (obj != NULL) {
                                cfs_hlist_add_head(&obj->mfo_hnode,
                                                   &hasd->chd_obj_idle);
                                continue;
                        }

                        CERROR("MDD failed to allocate cache object\n");
                        mdd_dir_cache_fini();
                        return -ENOMEM;
                }
        }

        return 0;
}

/**
 * setup transaction request:
 *   rc == 1 : we can schedule this request on MDD transaction threads
 *   rc == 0 : we can't/don't want to schedule this request on transaction
 *             threads, just run transaction in the context of caller
 */
int
mdd_trans_req_init(const struct lu_env *env, mdd_trans_req_t *req,
                   const struct lu_fid *pfid, mdd_trans_opc_t opc)
{
        int cpuid;

        if (mdd_trans_data.mtd_sched_nthr == 0)
                return 0;

        if (req->mtr_env != NULL) /* recursion breaker */
                return 0;

        if ((opc & mdd_trans_data.mtd_ops_enabled) == 0) /* opc is disabled */
                return 0;

        cpuid = mdd_dir_cache_find(pfid);
        if (cpuid < 0) /* it's OK to run on current context */
                return 0;

        req->mtr_env   = env;
        req->mtr_opc   = opc;
        req->mtr_cpuid = cpuid;
        cfs_waitq_init(&req->mtr_waitq);

        return 1;
}

/**
 * run transaction request on MDD transaction threads and wait
 * until it's done.
 */
int
mdd_trans_req_run(mdd_trans_req_t *req)
{
        mdd_trans_sched_t *sched = mdd_trans_data.mtd_scheds[req->mtr_cpuid];
        int                rc;

        cfs_spin_lock(&sched->mts_lock);

        sched->mts_req_pending++;
        cfs_list_add_tail(&req->mtr_link, &sched->mts_req_list);
        if (cfs_waitq_active(&sched->mts_waitq))
                cfs_waitq_signal(&sched->mts_waitq);

        cfs_spin_unlock(&sched->mts_lock);

        cfs_wait_event(req->mtr_waitq, req->mtr_env == NULL);

        rc = req->mtr_rc;
        return rc;
}

static void
mdd_trans_handler(mdd_trans_sched_t *sched, mdd_trans_req_t *req)
{
        int     rc = 0;

        if (mdd_trans_data.mtd_shutdown) {
                rc = -ESHUTDOWN;
                goto out;
        }

        switch (req->mtr_opc) {
        default:
                LBUG();

        case MDD_TRANS_OPC_CREATE:
                {
                mdd_create_req_t *create = &req->mtr_req.create;

                rc = mdd_dir_ops.mdo_create(req->mtr_env,
                                            create->mc_pobj, create->mc_lname,
                                            create->mc_child, create->mc_spec,
                                            create->mc_ma);
                break;
                }

        case MDD_TRANS_OPC_UNLINK:
                {
                mdd_unlink_req_t *unlink = &req->mtr_req.unlink;

                rc = mdd_dir_ops.mdo_unlink(req->mtr_env,
                                            unlink->mu_pobj, unlink->mu_cobj,
                                            unlink->mu_lname, unlink->mu_ma);
                break;
                }
        }

 out:
        req->mtr_rc = rc;
        req->mtr_env = NULL; /* caller is waiting on this */
        cfs_waitq_signal(&req->mtr_waitq);
}

static int
mdd_trans_scheduler(void *arg)
{
        mdd_trans_sched_t *sched = arg;
        mdd_trans_req_t   *req;
        cfs_duration_t     timeout;
        time_t             last_nuke;
        int                tid;
        char               name[32];

        tid = cfs_atomic_inc_return(&sched->mts_nstarted);
        cfs_waitq_signal(&mdd_trans_data.mtd_waitq);

        snprintf(name, sizeof(name), "mdd_trans_%d_%d", sched->mts_cpuid, tid);

        cfs_daemonize_ctxt(name);
        if (cfs_cpu_bind(sched->mts_cpuid) != 0) {
                /* we probably should just disable MDD scheduler
                 * if failed to bind on CPU */
                CWARN("Can't bind %s on CPU node: %d\n",
                      name, sched->mts_cpuid);
        }

        /* only the first thread has valid timeout */
        timeout = tid == 1 ? cfs_time_seconds(MDD_OBJ_STAMP_OLD) : 0;
        last_nuke = cfs_time_current_sec();

        while (!mdd_trans_data.mtd_shutdown ||
               !cfs_list_empty(&sched->mts_req_list)) { /* drain requests */
                struct l_wait_info lwi = LWI_TIMEOUT(timeout, NULL, NULL);

                l_wait_event_exclusive_head(sched->mts_waitq,
                                 mdd_trans_data.mtd_shutdown ||
                                 !cfs_list_empty(&sched->mts_req_list), &lwi);

                if (timeout != 0 &&
                    last_nuke - cfs_time_current_sec() >= MDD_OBJ_STAMP_OLD) {
                        /*
                         * NB: nuke old objects in local CPU cache for each
                         * 10 seconds.
                         * Otherwise there could be some old objects stay
                         * in local-cache forever, they will keep reference
                         * on objects in global hash so even very few CPU
                         * is accessing those dirs, we still think they
                         * are high contention dirs.
                         */
                        mdd_dir_cache_find(NULL);
                        last_nuke = cfs_time_current_sec();
                }

                cfs_spin_lock(&sched->mts_lock);
                while (!cfs_list_empty(&sched->mts_req_list)) {
                        req = cfs_list_entry(sched->mts_req_list.next,
                                             mdd_trans_req_t, mtr_link);
                        cfs_list_del(&req->mtr_link);
                        sched->mts_req_pending--;
                        sched->mts_req_active++;

                        cfs_spin_unlock(&sched->mts_lock);

                        LASSERT(req->mtr_cpuid == sched->mts_cpuid);
                        /* NB: no RPC is allowed in mdd_trans_handler */
                        mdd_trans_handler(sched, req);

                        cfs_spin_lock(&sched->mts_lock);

                        sched->mts_req_active--;
                }
                cfs_spin_unlock(&sched->mts_lock);
        }
        cfs_atomic_inc(&sched->mts_nstopped);
        cfs_waitq_signal(&mdd_trans_data.mtd_waitq);

        return 0;
}

static void
mdd_trans_threads_stop(void)
{
        mdd_trans_sched_t      *sched;
        int                     i;

        mdd_trans_data.mtd_shutdown = 1;
        cfs_percpu_for_each(sched, i, mdd_trans_data.mtd_scheds) {
                if (sched != NULL)
                        cfs_waitq_broadcast(&sched->mts_waitq);
        }

        cfs_percpu_for_each(sched, i, mdd_trans_data.mtd_scheds) {
                if (sched == NULL)
                        continue;
                cfs_wait_event(mdd_trans_data.mtd_waitq,
                               cfs_atomic_read(&sched->mts_nstopped) ==
                               cfs_atomic_read(&sched->mts_nstarted));
        }
}

static int
mdd_trans_threads_start(void)
{
        mdd_trans_sched_t   *sched;
        int                  rc = 0;
        int                  i;
        int                  j;

        cfs_percpu_for_each(sched, i, mdd_trans_data.mtd_scheds) {
                for (j = 0; j < mdd_trans_data.mtd_sched_nthr; j++) {
                        rc = cfs_kernel_thread(mdd_trans_scheduler,
                                               sched, CLONE_VM | CLONE_FILES);
                        if (rc < 0)
                                break;
                }

                cfs_wait_event(mdd_trans_data.mtd_waitq,
                               cfs_atomic_read(&sched->mts_nstarted) == j);
                if (rc >= 0)
                        continue;
                CERROR("Failed to start %d: %d mdd service threads\n", i, j);
                mdd_trans_threads_stop();
                return -ENOMEM;
        }
        return 0;
}

void
mdd_trans_mod_fini(void)
{
        if (mdd_trans_data.mtd_scheds == NULL)
                return;

        mdd_trans_threads_stop();
        cfs_percpu_free(mdd_trans_data.mtd_scheds);
        mdd_trans_data.mtd_scheds = NULL;

        mdd_dir_cache_fini();
}

int
mdd_trans_mod_init(void)
{
        mdd_trans_sched_t *sched;
        int                rc;
        int                i;

        memset(&mdd_trans_data, 0, sizeof(mdd_trans_data));
        if (cfs_cpu_node_num() < MDD_OBJ_CPU_MAX || trans_threads_num == 0)
                return 0;

        rc = mdd_dir_cache_init();
        if (rc != 0)
                return rc;

        trans_threads_num = trans_threads_num * cfs_cpu_node_weight(0);
        trans_threads_num = min(trans_threads_num,
                                (unsigned)MDD_TRANS_THREADS_MAX);

        cfs_waitq_init(&mdd_trans_data.mtd_waitq);
        mdd_trans_data.mtd_sched_nthr  = trans_threads_num;
        mdd_trans_data.mtd_ops_enabled = MDD_TRANS_OPC_DEFAULT;
        mdd_trans_data.mtd_scheds = cfs_percpu_alloc(sizeof(mdd_trans_sched_t));

        if (mdd_trans_data.mtd_scheds == NULL) {
                mdd_trans_mod_fini();
                return -ENOMEM;
        }

        cfs_percpu_for_each(sched, i, mdd_trans_data.mtd_scheds) {
                cfs_spin_lock_init(&sched->mts_lock);
                cfs_waitq_init(&sched->mts_waitq);
                CFS_INIT_LIST_HEAD(&sched->mts_req_list);
                cfs_atomic_set(&sched->mts_nstarted, 0);
                cfs_atomic_set(&sched->mts_nstopped, 0);
                sched->mts_cpuid = i;
        }

        rc = mdd_trans_threads_start();
        if (rc != 0)
                mdd_trans_mod_fini();

        return rc;
}
