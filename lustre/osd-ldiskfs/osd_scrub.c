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
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * lustre/osd-ldiskfs/osd_scrub.c
 *
 * Top-level entry points into osd module
 *
 * OI Scrub is used to rebuild Object Index files when restore MDT from
 * file-level backup. And also can be part of consistency routine check.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

/*
 * Inode table based iterator against osd-ldiskfs. The kernel thread "osd_scrub"
 * scans ldiskfs inode table, for each allocated bit, reads corresponding inode.
 * To accelerate inode table scanning, the "osd_scrub" triggers async inode read
 * request. Means that it only submits read request, but without waiting for the
 * read result. The up layer iterator can wait and process the result when need.
 * Since one inode block may contains several inodes, depends on inode size. For
 * inodes contained in the same iblock, only single read request is triggered.
 *
 * These requests are recorded by the following structures:
 *
 * 1) osd_scrub_unit
 * The basic OSD iterator record unit. "osd_scrub_unit" corresponds to iblock.
 * An "osd_scrub_unit" records all the scanned inodes contained in the iblock.
 *
 * All these "osd_scrub_unit" belonging to the same block group links into
 * "osd_scrub_group::osg_units" by "osd_scrub_unit::osu_list".
 *
 * 2) osd_scrub_group
 * The structure for recording the "osd_scrub_unit" belonging to the same block
 * group as described above in "osd_scrub_unit" section.
 *
 * All these "osd_scrub_group" belonging to the same super_block links into
 * "osd_scrub_it::osi_groups" by "osd_scrub_group::osg_list".
 *
 * 3) osd_scrub_it
 * The structure for recording the "osd_scrub_group" belonging to the same
 * super_block as described above in "osd_scrub_group" section.
 *
 * The "osd_scrub_it" is OSD iterator handler, contains all the scanned inodes,
 * current iteration position, next record to be returned for up layer iterator,
 * and so on, in the same ldiskfs partion. For each osd_device, there is at most
 * one "osd_scrub_it", which is pointed by "osd_device::od_osi".
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include "osd_internal.h"

static struct osd_scrub_unit *osu_init(struct buffer_head *bh, __u32 base)
{
        struct osd_scrub_unit *osu;

        OBD_ALLOC_PTR(osu);
        if (unlikely(osu == NULL))
                return NULL;

        CFS_INIT_LIST_HEAD(&osu->osu_list);
        osu->osu_bh = bh;
        osu->osu_base = base;

        return osu;
}

static void osu_fini(struct osd_scrub_unit *osu)
{
        LASSERT(cfs_list_empty(&osu->osu_list));

        brelse(osu->osu_bh);
        OBD_FREE_PTR(osu);
}

/**
 * link osd_scrub_unit::osu_list into osd_scrub_group::osg_units
 */
static inline void osu_link(struct osd_scrub_unit *osu,
                            struct osd_scrub_group *osg,
                            struct osd_scrub_it *osi)
{
        LASSERT(cfs_list_empty(&osu->osu_list));

        cfs_spin_lock(&osg->osg_lock);
        cfs_list_add_tail(&osu->osu_list, &osg->osg_units);
        cfs_spin_unlock(&osg->osg_lock);

        if (cfs_atomic_inc_return(&osi->osi_unit_count) == 1)
                cfs_waitq_signal(&osi->osi_thread.t_ctl_waitq);
}

/**
 * unlink osd_scrub_unit::osu_list from osd_scrub_group::osg_units
 */
static inline void osu_unlink(struct osd_scrub_unit *osu,
                              struct osd_scrub_group *osg,
                              struct osd_scrub_it *osi)
{
        LASSERT(!cfs_list_empty(&osu->osu_list));

        cfs_atomic_dec(&osi->osi_unit_count);

        cfs_spin_lock(&osg->osg_lock);
        cfs_list_del_init(&osu->osu_list);
        cfs_spin_unlock(&osg->osg_lock);
}

static struct osd_scrub_group *osg_init(struct buffer_head *bh,
                                        __u32 base, __u32 offset)
{
        struct osd_scrub_group *osg;

        OBD_ALLOC_PTR(osg);
        if (unlikely(osg == NULL))
                return NULL;

        CFS_INIT_LIST_HEAD(&osg->osg_list);
        CFS_INIT_LIST_HEAD(&osg->osg_units);
        cfs_spin_lock_init(&osg->osg_lock);
        osg->osg_bitmap = bh;
        osg->osg_base = base;
        osg->osg_offset = offset;

        return osg;
}

static void osg_fini(struct osd_scrub_group *osg,
                     struct osd_scrub_it *osi)
{
        struct osd_scrub_unit *osu, *next;

        LASSERT(cfs_list_empty(&osg->osg_list));

        cfs_list_for_each_entry_safe(osu, next, &osg->osg_units, osu_list) {
                osu_unlink(osu, osg, osi);
                osu_fini(osu);
        }

        brelse(osg->osg_bitmap);
        OBD_FREE_PTR(osg);
}

/**
 * link osd_scrub_group::osg_list into osd_scrub_it:osi_groups
 */
static inline void osg_link(struct osd_scrub_group *osg,
                            struct osd_scrub_it *osi)
{
        LASSERT(cfs_list_empty(&osg->osg_list));

        cfs_spin_lock(&osi->osi_lock);
        cfs_list_add_tail(&osg->osg_list, &osi->osi_groups);
        cfs_spin_unlock(&osi->osi_lock);
}

/**
 * unlink osd_scrub_group::osg_list from osd_scrub_it:osi_groups
 */
static inline void osg_unlink(struct osd_scrub_group *osg,
                              struct osd_scrub_it *osi)
{
        LASSERT(!cfs_list_empty(&osg->osg_list));

        cfs_spin_lock(&osi->osi_lock);
        cfs_list_del_init(&osg->osg_list);
        cfs_spin_unlock(&osi->osi_lock);
}

static inline void osg_clean_current(struct osd_scrub_group *osg,
                                     struct osd_scrub_it *osi)
{
        struct osd_scrub_unit *osu = osg->osg_current_unit;

        osg->osg_current_unit = NULL;
        osu_unlink(osu, osg, osi);
        osu_fini(osu);
}

static struct osd_scrub_it *osi_init(struct osd_device *dev,
                                     struct dt_scrub_param *args)
{
        struct super_block  *sb   = osd_sb(dev);
        struct osd_scrub_it *osi;

        OBD_ALLOC_PTR(osi);
        if (unlikely(osi == NULL))
                return NULL;

        CFS_INIT_LIST_HEAD(&osi->osi_groups);
        cfs_spin_lock_init(&osi->osi_lock);
        osi->osi_sb = sb;
        osi->osi_dentry.d_sb = sb;
        cfs_waitq_init(&osi->osi_thread.t_ctl_waitq);
        osi->osi_args = *args;
        osi->osi_dev = dev;
        cfs_atomic_set(&osi->osi_unit_count, 0);
        cfs_atomic_set(&osi->osi_refcount, 1);
        osi->osi_inodes_per_block = LDISKFS_BLOCK_SIZE(sb) /
                                    LDISKFS_INODE_SIZE(sb);

        return osi;
}

static void osi_fini(struct osd_scrub_it *osi)
{
        struct osd_scrub_group *osg, *next;

        cfs_list_for_each_entry_safe(osg, next, &osi->osi_groups, osg_list) {
                osg_unlink(osg, osi);
                osg_fini(osg, osi);
        }

        LASSERT(cfs_atomic_read(&osi->osi_unit_count) == 0);
        OBD_FREE_PTR(osi);
}

static inline void osi_clean_current(struct osd_scrub_it *osi)
{
        struct osd_scrub_group *osg = osi->osi_current_group;

        LASSERT(osg->osg_completed);

        osi->osi_current_group = NULL;
        osg_unlink(osg, osi);
        osg_fini(osg, osi);
}

static inline struct osd_scrub_it *osi_get(struct osd_scrub_it *osi)
{
        cfs_atomic_inc(&osi->osi_refcount);

        return osi;
}

static inline void osi_put(struct osd_scrub_it *osi)
{
        if (cfs_atomic_dec_and_test(&osi->osi_refcount))
                osi_fini(osi);
}

static inline int scrub_has_window(struct osd_scrub_it *osi)
{
        /* window == 0 means go without limitation. */
        return osi->osi_args.dsp_window == 0 ||
               osi->osi_args.dsp_window > cfs_atomic_read(&osi->osi_unit_count);
}

static inline int scrub_has_urgent(struct osd_device *dev)
{
        return !cfs_list_empty(&dev->od_unmatched_list);
}

static inline int scrub_next_ready(struct osd_scrub_it *osi)
{
        if (!cfs_list_empty(&osi->osi_groups)) {
                struct osd_scrub_group *osg;

                osg = cfs_list_entry(osi->osi_groups.next,
                                     struct osd_scrub_group, osg_list);
                if (!cfs_list_empty(&osg->osg_units) || osg->osg_completed)
                        return 1;
        }
        return 0;
}

static int osd_scrub_main(void *arg)
{
        struct osd_scrub_it  *osi    = osi_get((struct osd_scrub_it *)arg);
        struct ptlrpc_thread *thread = &osi->osi_thread;
        __u32                 start  = osi->osi_args.dsp_lid.lli_u32[0];
        __u32                 max    = le32_to_cpu(LDISKFS_SB(osi->osi_sb)->
                                                   s_es->s_inodes_count);
        struct l_wait_info    lwi    = { 0 };
        int                   rc     = 0;
        ENTRY;

        cfs_daemonize("osd_scrub");

        if (start < LDISKFS_FIRST_INO(osi->osi_sb))
                osi->osi_current_position = LDISKFS_FIRST_INO(osi->osi_sb);
        else
                osi->osi_current_position = start;
        osi->osi_current_key = osi->osi_current_position;

        CDEBUG(D_SCRUB, "OSD start iteration: start = %u, cur_position = %u, "
               "window = %u, flags = %u, pid = %d\n", start,
               osi->osi_current_position, osi->osi_args.dsp_window,
               osi->osi_args.dsp_flags, cfs_curproc_pid());

        cfs_spin_lock(&osi->osi_lock);
        thread_set_flags(thread, SVC_RUNNING);
        cfs_spin_unlock(&osi->osi_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);

        while (osi->osi_current_position <= max) {
                ldiskfs_group_t bg = (osi->osi_current_position - 1) /
                                     LDISKFS_INODES_PER_GROUP(osi->osi_sb);
                __u32 offset = (osi->osi_current_position - 1) %
                               LDISKFS_INODES_PER_GROUP(osi->osi_sb);
                struct buffer_head *bitmap_bh;
                struct osd_scrub_group *osg;
                __u32 gbase;

                bitmap_bh = ldiskfs_read_inode_bitmap(osi->osi_sb, bg);
                if (bitmap_bh == NULL) {
                        CERROR("Fail to read bitmap for %u\n", bg);
                        GOTO(out, rc = -EIO);
                }

                offset = ldiskfs_find_next_bit((unsigned long *)
                                               bitmap_bh->b_data,
                                LDISKFS_INODES_PER_GROUP(osi->osi_sb), offset);
                if (offset >= LDISKFS_INODES_PER_GROUP(osi->osi_sb)) {
                        brelse(bitmap_bh);
                        osi->osi_current_position = 1 + (bg + 1) *
                                        LDISKFS_INODES_PER_GROUP(osi->osi_sb);
                        continue;
                }

                gbase = 1 + bg * LDISKFS_INODES_PER_GROUP(osi->osi_sb);
                osg = osg_init(bitmap_bh, gbase, offset);
                if (unlikely(osg == NULL)) {
                        brelse(bitmap_bh);
                        CERROR("Fail to init group for %u\n", bg);
                        GOTO(out, rc = -ENOMEM);
                }

                osg_link(osg, osi);

                while (osi->osi_current_position <= max &&
                       offset < LDISKFS_INODES_PER_GROUP(osi->osi_sb)) {
                        struct buffer_head *inode_bh;
                        struct osd_scrub_unit *osu;
                        __u32 ubase;

                        l_wait_event(osi->osi_thread.t_ctl_waitq,
                                     scrub_has_window(osi) ||
                                     osg->osg_skip ||
                                     !thread_is_running(&osi->osi_thread),
                                     &lwi);
                        if (!thread_is_running(&osi->osi_thread))
                                GOTO(out, rc = 0);

                        if (osg->osg_skip) {
                                osi->osi_current_position = 1 + (bg + 1) *
                                        LDISKFS_INODES_PER_GROUP(osi->osi_sb);
                                goto completed;
                        }

                        /* Someone deleted the inode when the scrub waiting */
                        if (!ldiskfs_test_bit(offset, bitmap_bh->b_data)) {
                                offset = ldiskfs_find_next_bit(
                                         (unsigned long *)bitmap_bh->b_data,
                                         LDISKFS_INODES_PER_GROUP(osi->osi_sb),
                                         offset + 1);
                                osi->osi_current_position = gbase + offset;
                                continue;
                        }

                        inode_bh = ldiskfs_iget_bh(osi->osi_sb,
                                                   osi->osi_current_position);
                        if (IS_ERR(inode_bh)) {
                                rc = PTR_ERR(inode_bh);
                                CERROR("Fail to read iblock: group = %u, "
                                       "base = %u, offset = %u, rc = %d\n",
                                       bg, gbase, offset, rc);
                                GOTO(out, rc);
                        }

                        ubase = offset & ~(osi->osi_inodes_per_block - 1);
                        osu = osu_init(inode_bh, ubase);
                        if (unlikely(osu == NULL)) {
                                brelse(inode_bh);
                                CERROR("Fail to init unit: group = %u, "
                                       "base = %u, offset = %u\n",
                                       bg, gbase, offset);
                                GOTO(out, rc = -ENOMEM);
                        }

                        osu_link(osu, osg, osi);

                        offset = ldiskfs_find_next_bit(
                                 (unsigned long *)bitmap_bh->b_data,
                                 LDISKFS_INODES_PER_GROUP(osi->osi_sb),
                                 ubase + osi->osi_inodes_per_block);
                        osi->osi_current_position = gbase + offset;
                }

completed:
                osg->osg_completed = 1;
                cfs_waitq_signal(&thread->t_ctl_waitq);
        };

        EXIT;

out:
        CDEBUG(D_SCRUB, "OSD stop iteration: start = %u, cur_position = %u, "
               "window = %u, flags = %u, pid = %d, rd = %d\n", start,
               osi->osi_current_position, osi->osi_args.dsp_window,
               osi->osi_args.dsp_flags, cfs_curproc_pid(), rc);

        osi->osi_exit_value = rc;
        cfs_spin_lock(&osi->osi_lock);
        thread_set_flags(thread, SVC_STOPPED);
        cfs_spin_unlock(&osi->osi_lock);
        cfs_waitq_signal(&thread->t_ctl_waitq);
        osi_put(osi);

        return rc;
}

static int osd_scrub_lookup(const struct lu_env *env, struct dt_object *dt,
                            struct dt_rec *rec, const struct dt_key *key,
                            struct lu_object_hint *unused,
                            struct lustre_capa *capa)
{
        struct osd_device *dev = osd_obj2dev(osd_dt_obj(dt));
        struct lu_fid     *fid = (struct lu_fid *)key;
        int                rc;
        ENTRY;

        rc = osd_oi_lookup(osd_oti_get(env), osd_fid2oi(dev, fid), fid,
                           (struct osd_inode_id *)rec);
        RETURN(rc);
}

static int osd_scrub_update(const struct lu_env *env, struct dt_object *dt,
                            const struct dt_rec *rec, const struct dt_key *key,
                            struct thandle *th, struct lustre_capa *capa,
                            int ignore_quota)
{
        struct osd_object      *obj  = osd_dt_obj(dt);
        struct iam_container   *bag  = &obj->oo_dir->od_container;
        struct iam_path_descr  *ipd;
        struct osd_thandle     *oh;
#ifdef HAVE_QUOTA_SUPPORT
        cfs_cap_t               save = cfs_curproc_cap_pack();
#endif
        int rc;
        ENTRY;

        ipd = osd_idx_ipd_get(env, bag);
        if (unlikely(ipd == NULL))
                RETURN(-ENOMEM);

        oh = container_of0(th, struct osd_thandle, ot_super);
        LASSERT(oh->ot_handle != NULL);
        LASSERT(oh->ot_handle->h_transaction != NULL);
#ifdef HAVE_QUOTA_SUPPORT
        if (ignore_quota)
                cfs_cap_raise(CFS_CAP_SYS_RESOURCE);
        else
                cfs_cap_lower(CFS_CAP_SYS_RESOURCE);
#endif
        rc = iam_update(oh->ot_handle, bag, (const struct iam_key *)key,
                        (struct iam_rec *)rec, ipd);
#ifdef HAVE_QUOTA_SUPPORT
        cfs_curproc_cap_unpack(save);
#endif
        osd_ipd_put(env, bag, ipd);

        RETURN(rc);
}

/*
 * Return value:
 * +1: skip this object for: igif object, oi object, unlinked object,
       and the object which mapping needs not to be updated.
 *  0: updated/inserted the mapping.
 * -v: some failure occurred.
 */
static int osd_scrub_insert(const struct lu_env *env, struct dt_object *dt,
                            const struct dt_rec *rec, const struct dt_key *key,
                            struct thandle *th, struct lustre_capa *capa,
                            int ignore_quota)
{
        struct osd_device   *dev    = osd_obj2dev(osd_dt_obj(dt));
        struct osd_scrub_it *osi    = dev->od_osi;
        struct lu_fid       *fid    = (struct lu_fid *)key;
        struct lu_fid       *oi_fid = &osi->osi_fid;
        struct osd_inode_id *id     = (struct osd_inode_id *)rec;
        struct osd_inode_id *oi_id  = &osi->osi_id;
        struct dt_object    *idx    = osd_fid2oi(dev, fid)->oi_dir;
        struct inode        *inode;
        int                  rc;
        ENTRY;

        LASSERT(osi != NULL);
        LASSERT(th != NULL);

        if (!fid_is_norm(fid))
                RETURN(1);

        fid_cpu_to_be(oi_fid, fid);
        osd_id_pack(oi_id, id);

        rc = osd_scrub_update(env, idx, (struct dt_rec *)oi_id,
                              (struct dt_key *)oi_fid, th, capa, ignore_quota);
        if (rc == -ENOENT) {
                inode = osd_iget(osi->osi_dev, id, &osi->osi_dentry,
                                 &osi->osi_lma, fid, OSD_IF_VERIFY, NULL);
                if (IS_ERR(inode)) {
                        rc = PTR_ERR(inode);
                        if (rc == -ENOENT || rc == -ESTALE || rc == -EREMCHG)
                                rc = 1;
                        RETURN(rc);
                }

                /* Maybe race with osd_object_destroy(). */
                cfs_mutex_lock(&inode->i_mutex);
                if (unlikely(inode->i_nlink == 0)) {
                        cfs_mutex_unlock(&inode->i_mutex);
                        iput(inode);
                        RETURN(1);
                }

                rc = idx->do_index_ops->dio_insert(env, idx,
                                                   (struct dt_rec *)oi_id,
                                                   (struct dt_key *)oi_fid, th,
                                                   capa, ignore_quota);
                cfs_mutex_unlock(&inode->i_mutex);
                iput(inode);
                if (unlikely(rc == -EEXIST))
                        rc = 1;
        }

        RETURN(rc);
}

static int osd_scrub_delete(const struct lu_env *env, struct dt_object *dt,
                            const struct dt_key *key, struct thandle *handle,
                            struct lustre_capa *capa)
{
        return -ENOSYS;
}

static void osd_unmatched_clean(cfs_list_t *wait_list)
{
        struct osd_unmatched_item *oui;

        while (!cfs_list_empty(wait_list)) {
                oui = cfs_list_entry(wait_list->next, struct osd_unmatched_item,
                                     oui_list);
                cfs_list_del_init(&oui->oui_list);
                oui->oui_status = OUF_UPDATED;
                cfs_waitq_signal(&oui->oui_waitq);
        }
}

static struct dt_it *osd_scrub_init(const struct lu_env *env,
                                    struct dt_object *dt, void *args,
                                    struct lustre_capa *capa)
{
        struct osd_device   *dev = osd_dev(dt->do_lu.lo_dev);
        struct osd_scrub_it *osi;
        struct l_wait_info   lwi = { 0 };
        int                  rc;
        ENTRY;

        if (dev->od_osi != NULL)
                RETURN(ERR_PTR(-EALREADY));

        osi = osi_init(dev, args);
        if (unlikely(osi == NULL))
                RETURN(ERR_PTR(-ENOMEM));

        cfs_spin_lock(&dev->od_osi_lock);
        if (unlikely(dev->od_osi != NULL)) {
                cfs_spin_unlock(&dev->od_osi_lock);
                osi_put(osi);
                RETURN(ERR_PTR(-EALREADY));
        } else {
                dev->od_osi = osi;
                cfs_spin_unlock(&dev->od_osi_lock);
        }

        rc = cfs_create_thread(osd_scrub_main, osi, 0);
        if (rc < 0) {
                CFS_LIST_HEAD(wait_list);

                CERROR("Cannot start osd scrub thread: rc %d\n", rc);
                cfs_spin_lock(&dev->od_osi_lock);
                dev->od_osi = NULL;
                cfs_list_splice_init(&dev->od_unmatched_list, &wait_list);
                cfs_spin_unlock(&dev->od_osi_lock);
                osi_put(osi);
                osd_unmatched_clean(&wait_list);
                RETURN(ERR_PTR(rc));
        }

        l_wait_event(osi->osi_thread.t_ctl_waitq,
                     thread_is_running(&osi->osi_thread) ||
                     thread_is_stopped(&osi->osi_thread),
                     &lwi);

        RETURN((struct dt_it *)osi);
}

static void osd_scrub_fini(const struct lu_env *env, struct dt_it *di)
{
        struct osd_scrub_it  *osi    = (struct osd_scrub_it *)di;
        struct ptlrpc_thread *thread = &osi->osi_thread;
        struct osd_device    *dev    = osi->osi_dev;
        struct l_wait_info    lwi    = { 0 };
        CFS_LIST_HEAD(wait_list);
        ENTRY;

        LASSERT(dev != NULL);

        cfs_spin_lock(&osi->osi_lock);
        if (!thread_is_stopped(thread)) {
                thread_set_flags(thread, SVC_STOPPING);
                cfs_spin_unlock(&osi->osi_lock);
                cfs_waitq_signal(&thread->t_ctl_waitq);
                l_wait_event(thread->t_ctl_waitq,
                             thread_is_stopped(thread),
                             &lwi);
        } else {
                cfs_spin_unlock(&osi->osi_lock);
        }

        cfs_spin_lock(&dev->od_osi_lock);
        LASSERT(osi->osi_next_oui == NULL);
        dev->od_osi = NULL;
        cfs_list_splice_init(&dev->od_unmatched_list, &wait_list);
        cfs_spin_unlock(&dev->od_osi_lock);
        osi_put(osi);
        osd_unmatched_clean(&wait_list);

        EXIT;
}

static int osd_scrub_get(const struct lu_env *env,
                         struct dt_it *di, const struct dt_key *key)
{
        return -ENOSYS;
}

static void osd_scrub_put(const struct lu_env *env, struct dt_it *di)
{
        /* Do nothing. */
}

/*
 * Return value:
 * +1: osd layer iteration completed.
 *  0: navigated to next position.
 * -v: some failure occurred, caused OSD layer iterator exit.
 */
static int osd_scrub_next(const struct lu_env *env, struct dt_it *di)
{
        struct osd_scrub_it    *osi    = (struct osd_scrub_it *)di;
        struct ptlrpc_thread   *thread = &osi->osi_thread;
        struct osd_scrub_group *osg;
        struct osd_scrub_unit  *osu;
        struct osd_device      *dev    = osi->osi_dev;
        struct l_wait_info      lwi    = { 0 };
        __u32                   next;
        ENTRY;

        LASSERT(osi->osi_next_oui == NULL);

        while (1) {
                l_wait_event(thread->t_ctl_waitq,
                             scrub_has_urgent(dev) ||
                             scrub_next_ready(osi) ||
                             !thread_is_running(thread),
                             &lwi);

                if (scrub_has_urgent(dev)) {
                        osi->osi_next_oui =
                                cfs_list_entry(dev->od_unmatched_list.next,
                                               struct osd_unmatched_item,
                                               oui_list);
                        break;
                }

                if (!scrub_next_ready(osi)) {
                        LASSERT(!thread_is_running(thread));

                        RETURN(osi->osi_exit_value < 0 ? osi->osi_exit_value:1);
                }

                if (osi->osi_current_group == NULL) {
                        osg = cfs_list_entry(osi->osi_groups.next,
                                             struct osd_scrub_group, osg_list);
                        osi->osi_current_group = osg;
                        next = osg->osg_offset;
                } else {
                        osg = osi->osi_current_group;
                        if (osg->osg_skip)
                                goto skip;

                        if (osg->osg_keep_offset) {
                                next = osg->osg_offset;
                                osg->osg_keep_offset = 0;
                        } else {
                                next = osg->osg_offset + 1;
                        }
                }

                /* empty group */
                if (cfs_list_empty(&osg->osg_units)) {
                        LASSERT(osg->osg_completed);

                        osi_clean_current(osi);
                        continue;
                }

next_bit:
                if (next < LDISKFS_INODES_PER_GROUP(osi->osi_sb))
                        next = ldiskfs_find_next_bit((unsigned long *)
                                                     osg->osg_bitmap->b_data,
                                LDISKFS_INODES_PER_GROUP(osi->osi_sb), next);

                /* exceed current group */
                if (next >= LDISKFS_INODES_PER_GROUP(osi->osi_sb)) {
                        osg->osg_skip = 1;
                        cfs_waitq_signal(&thread->t_ctl_waitq);

skip:
                        l_wait_event(thread->t_ctl_waitq,
                                     scrub_has_urgent(dev) ||
                                     osg->osg_completed ||
                                     !thread_is_running(thread),
                                     &lwi);

                        if (osg->osg_completed) {
                                osi_clean_current(osi);
                                continue;
                        } else {
                                if (scrub_has_urgent(dev)) {
                                        osi->osi_next_oui = cfs_list_entry(
                                               dev->od_unmatched_list.next,
                                               struct osd_unmatched_item,
                                               oui_list);
                                        /* osg_skip is set, no need to process
                                         * osg_keep_offset. */
                                        break;
                                }
                                LASSERT(!thread_is_running(thread));

                                RETURN(osi->osi_exit_value < 0 ?
                                       osi->osi_exit_value : 1);
                        }
                }

next_osu:
                if (osg->osg_current_unit == NULL) {
                        osu = cfs_list_entry(osg->osg_units.next,
                                             struct osd_scrub_unit, osu_list);
                        osg->osg_current_unit = osu;
                } else {
                        osu = osg->osg_current_unit;
                }

                /* exceed current unit */
                /* Someone deleted some inodes after osd layer iteration.
                 * Related iteration work will be dropped. */
                if (next >= osu->osu_base + osi->osi_inodes_per_block) {
                        osg_clean_current(osg, osi);

                        l_wait_event(thread->t_ctl_waitq,
                                     scrub_has_urgent(dev) ||
                                     !cfs_list_empty(&osg->osg_units) ||
                                     osg->osg_completed ||
                                     !thread_is_running(thread),
                                     &lwi);

                        if (scrub_has_urgent(dev)) {
                                osi->osi_next_oui = cfs_list_entry(
                                               dev->od_unmatched_list.next,
                                               struct osd_unmatched_item,
                                               oui_list);
                                osg->osg_offset = next;
                                osg->osg_keep_offset = 1;
                                break;
                        }

                        if (!cfs_list_empty(&osg->osg_units))
                                goto next_osu;

                        if (!osg->osg_completed) {
                                LASSERT(!thread_is_running(thread));

                                RETURN(osi->osi_exit_value < 0 ?
                                       osi->osi_exit_value : 1);
                        }

                        /* exceed current group */
                        osi_clean_current(osi);
                        continue;
                }

                /* Someone allocated the inode after osd layer iteration.
                 * So related FID<=>ino# mapping should has been in OI file.
                 * Skip this inode to next one. */
                if (next < osu->osu_base) {
                        next = osu->osu_base;
                        goto next_bit;
                }

                osg->osg_offset = next;
                osi->osi_current_key = osg->osg_base + osg->osg_offset;
                break;
        }

        RETURN(0);
}

static struct dt_key *osd_scrub_key(const struct lu_env *env,
                                    const struct dt_it *di)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;

        if (osi->osi_next_oui != NULL)
                return (struct dt_key *)(&osi->osi_next_oui->oui_id.oii_ino);

        return (struct dt_key *)(&osi->osi_current_key);
}

static int osd_scrub_key_size(const struct lu_env *env, const struct dt_it *di)
{
        struct osd_scrub_it *osi = (struct osd_scrub_it *)di;

        return sizeof(osi->osi_current_key);
}

static int osd_scrub_rec(const struct lu_env *env,
                         const struct dt_it *di, struct dt_rec *rec, __u32 attr)
{
        struct osd_scrub_it   *osi   = (struct osd_scrub_it *)di;
        struct osd_device     *dev   = osi->osi_dev;
        struct dt_scrub_rec   *dsr   = (struct dt_scrub_rec *)rec;
        struct osd_inode_id   *id    = (struct osd_inode_id *)(&dsr->dsr_lid);
        enum osd_iget_flags    flags = OSD_IF_GEN_OID;
        __u32                  ino   = osi->osi_current_key;
        struct inode          *inode;
        enum osd_iget_valid    valid;
        int                    rc    = 0;
        ENTRY;

        if (osi->osi_next_oui != NULL) {
                struct osd_unmatched_item *oui = osi->osi_next_oui;

                if (!cfs_list_empty(&oui->oui_list)) {
                        LASSERT(oui->oui_status == OUF_NEW);

                        cfs_spin_lock(&dev->od_osi_lock);
                        cfs_list_del_init(&oui->oui_list);
                        oui->oui_status = OUF_UPDATING;
                        cfs_spin_unlock(&dev->od_osi_lock);
                }

                dsr->dsr_fid = oui->oui_fid;
                dsr->dsr_lid.lli_u32[0] = oui->oui_id.oii_ino;
                dsr->dsr_lid.lli_u32[1] = oui->oui_id.oii_gen;

                LASSERT(fid_is_norm(&dsr->dsr_fid));
                dsr->dsr_valid = DSV_LOCAL_ID | DSV_PRIOR | DSV_FID_NOR;

                GOTO(out, rc = 0);
        }

        dsr->dsr_valid = 0;
        if (osi->osi_args.dsp_flags & DSF_OI_REBUILD)
                flags |= OSD_IF_RET_FID;

        osd_id_gen(id, ino, OSD_OII_NOGEN);
        inode = osd_iget(osi->osi_dev, id, &osi->osi_dentry, &osi->osi_lma,
                         &dsr->dsr_fid, flags, &valid);
        if (IS_ERR(inode)) {
                rc = PTR_ERR(inode);
                if (rc == -ENOENT || rc == -ESTALE) {
                        rc = 1; /* 'rc == 1' means to ignore */
                } else {
                        if (valid & OSD_IV_OID)
                                dsr->dsr_valid |= DSV_LOCAL_ID;
                        CDEBUG(D_SCRUB, "Fail to read inode %u, rc = %d\n",
                               ino, rc);
                }
                GOTO(out, rc);
        }

        if (valid & OSD_IV_OID)
                dsr->dsr_valid |= DSV_LOCAL_ID;
        if (valid & OSD_IV_FID_NOR)
                dsr->dsr_valid |= DSV_FID_NOR;
        else if (valid & OSD_IV_FID_IGIF &&
                 osi->osi_args.dsp_flags & DSF_IGIF)
                dsr->dsr_valid |= DSV_FID_IGIF;

        iput(inode);
        EXIT;

out:
        if (scrub_has_urgent(dev))
                dsr->dsr_valid |= DSV_PRIOR_MORE;

        return rc;
}

static __u64 osd_scrub_store(const struct lu_env *env, const struct dt_it *di)
{
        return -ENOSYS;
}

static int osd_scrub_load(const struct lu_env *env, const struct dt_it *di,
                          __u64 hash)
{
        return -ENOSYS;
}

static int osd_scrub_set(const struct lu_env *env, struct dt_it *di, void *attr)
{
        struct osd_scrub_it       *osi    = (struct osd_scrub_it *)di;
        struct ptlrpc_thread      *thread = &osi->osi_thread;
        struct dt_scrub_set_param *dssp   = (struct dt_scrub_set_param *)attr;
        int                        rc     = 0;
        ENTRY;

        switch (dssp->dssp_flags) {
        case DSSF_ADJUST_WINDOW: {
                __u32 old = osi->osi_args.dsp_window;

                CDEBUG(D_SCRUB, "set scrub window: [old %u], [new %u]\n",
                       old, dssp->dssp_window);

                osi->osi_args.dsp_window = dssp->dssp_window;
                if (old != 0 && cfs_atomic_read(&osi->osi_unit_count) >= old &&
                    (dssp->dssp_window == 0 || dssp->dssp_window > old))
                        cfs_waitq_signal(&thread->t_ctl_waitq);
                break;
        }
        case DSSF_WAKEUP_PRIOR: {
                struct osd_unmatched_item *oui = osi->osi_next_oui;

                if (oui != NULL) {
                        osi->osi_next_oui = NULL;
                        oui->oui_status = OUF_UPDATED;
                        cfs_waitq_signal(&oui->oui_waitq);
                }
                break;
        }
        default: {
                CERROR("Unknown flags = %u\n", dssp->dssp_flags);
                rc = -EINVAL;
                break;
        }
        }

        RETURN(rc);
}

const struct dt_index_operations osd_scrub_ops = {
        .dio_lookup         = osd_scrub_lookup,
        .dio_declare_insert = osd_index_declare_ea_insert,
        .dio_insert         = osd_scrub_insert,
        .dio_declare_delete = osd_index_declare_ea_delete,
        .dio_delete         = osd_scrub_delete,
        .dio_it             = {
                .init     = osd_scrub_init,
                .fini     = osd_scrub_fini,
                .get      = osd_scrub_get,
                .put      = osd_scrub_put,
                .next     = osd_scrub_next,
                .key      = osd_scrub_key,
                .key_size = osd_scrub_key_size,
                .rec      = osd_scrub_rec,
                .store    = osd_scrub_store,
                .load     = osd_scrub_load,
                .set      = osd_scrub_set
        }
};

int osd_oui_insert(struct osd_device *dev,
                   struct osd_unmatched_item *oui)
{
        struct osd_scrub_it *osi = NULL;

        cfs_spin_lock(&dev->od_osi_lock);
        if (unlikely(dev->od_osi == NULL)) {
                cfs_spin_unlock(&dev->od_osi_lock);
                return -1;
        }

        if (cfs_list_empty(&dev->od_unmatched_list))
                osi = osi_get(dev->od_osi);

        cfs_list_add_tail(&oui->oui_list, &dev->od_unmatched_list);
        cfs_spin_unlock(&dev->od_osi_lock);

        if (osi != NULL) {
                cfs_waitq_broadcast(&osi->osi_thread.t_ctl_waitq);
                osi->osi_args.dsp_notify(osi->osi_args.dsp_data);
                osi_put(osi);
        }

        return 0;
}
