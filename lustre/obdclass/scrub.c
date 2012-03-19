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
 * Copyright (c) 2012 Whamcloud, Inc.
 */
/*
 * lustre/obdclass/scrub.c
 *
 * Common functions for Lustre scrub components.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_CLASS

#include <obd_support.h>
#include <lustre_lib.h>
#include <lu_object.h>
#include <dt_object.h>
#include <lustre_capa.h>
#include <lustre_scrub.h>

#define HALF_SEC        (CFS_HZ >> 1)

static inline int scrub_type_dump(char **buf, int *len, __u16 type)
{
        int rc;

        switch (type) {
        case ST_OI_SCRUB:
                rc = snprintf(*buf, *len, "Type: OI scrub\n");
                break;
        case ST_LAYOUT_SCRUB:
                rc = snprintf(*buf, *len, "Type: layout scrub\n");
                break;
        case ST_DNE_SCRUB:
                rc = snprintf(*buf, *len, "Type: DNE scrub\n");
                break;
        default:
                rc = snprintf(*buf, *len, "Type: unknown type(%x)\n", type);
                break;
        }
        if (rc <= 0)
                return -ENOSPC;

        *buf += rc;
        *len -= rc;
        return rc;
}

static const char *scrub_status_names[] = {
        "init",
        "scanning",
        "completed",
        "failed",
        "paused",
        "crashed",
        NULL
};

static const char *scrub_unit_flags_names[] = {
        "dryrun",
        "inconsistent",
        NULL
};

static inline int scrub_bits_dump(char **buf, int *len, __u16 flags,
                                  char *c, const char *names[], char *estr)
{
        int save = *len;
        int rc;
        int i;
        __u16 bit;

        for (i = 0; flags != 0; i++) {
                bit = 1 << i;
                if (flags & bit) {
                        if (names[i] != NULL)
                                rc = snprintf(*buf, *len, "%c %s",
                                              *c, names[i]);
                        else
                                rc = snprintf(*buf, *len, "%c %s(%x)",
                                              *c, estr, bit);
                        if (rc <= 0)
                                return -ENOSPC;

                        *buf += rc;
                        *len -= rc;
                        *c = ',';
                        flags &= ~bit;
                }
        }
        return save - *len;
}

static inline int scrub_time_dump(char **buf, int *len, __u64 time, char *str)
{
        int rc;

        if (time != 0)
                rc = snprintf(*buf, *len, "%s "LPU64" seconds\n", str,
                              cfs_time_current_sec() - time);
        else
                rc = snprintf(*buf, *len, "%s N/A\n", str);
        if (rc <= 0)
                return -ENOSPC;

        *buf += rc;
        *len -= rc;
        return rc;
}

static inline int scrub_param_method_dump(char **buf, int *len, __u16 method)
{
        int rc;

        switch (method) {
        case SM_OTABLE:
                rc = snprintf(*buf, *len, "Method: otable\n");
                break;
        case SM_NAMESPACE:
                rc = snprintf(*buf, *len, "Method: namespace\n");
                break;
        default:
                rc = snprintf(*buf, *len, "Method: unknown method(%x)\n",
                              method);
                break;
        }
        if (rc <= 0)
                return -ENOSPC;

        *buf += rc;
        *len -= rc;
        return rc;
}

static const char *scrub_param_flags_names[] = {
        NULL,           /* reset flag is invisible. */
        "failout",
        NULL,           /* dryrun flag will be processed at other place. */
        NULL
};

static inline int scrub_statistics_dump(char **buf, int *len,
                                        struct scrub_unit_desc *sud,
                                        struct scrub_statistics *ss, __u64 new,
                                        cfs_duration_t duration, int dryrun)
{
        __u64 checked;
        __u64 speed;
        __u32 rtime;
        int save = *len;
        int rc;

        if ((sud->sud_status == SS_SCANNING) &&
            ((dryrun != 0 && sud->sud_flags & SUF_DRYRUN) ||
             (dryrun == 0 && !(sud->sud_flags & SUF_DRYRUN)))) {
                checked = ss->ss_items_checked + new;
                speed = checked;
                rtime = ss->ss_time + cfs_duration_sec(duration + HALF_SEC);
                new *= CFS_HZ;
                if (duration != 0)
                        do_div(new, duration);
                if (rtime != 0)
                        do_div(speed, rtime);
                rc = snprintf(*buf, *len,
                              "Last Checkpoint Position: %s\n"
                              "First Failure Position: %s\n"
                              "Checked: "LPU64"\nUpdated: "LPU64"\n"
                              "Failed: "LPU64"\nPrior Updated: "LPU64"\n"
                              "Run Time: %u seconds\n"
                              "Average Speed: "LPU64" objects/sec\n"
                              "Real-Time Speed: "LPU64" objects/sec\n",
                              scrub_key_is_empty(ss->ss_pos_last_checkpoint) ?
                                "N/A" : (char *)ss->ss_pos_last_checkpoint,
                              scrub_key_is_empty(ss->ss_pos_first_inconsistent)?
                                "N/A" : (char *)ss->ss_pos_first_inconsistent,
                              checked, ss->ss_items_updated,
                              ss->ss_items_failed, ss->ss_items_updated_prior,
                              rtime, speed, new);
        } else {
                checked = ss->ss_items_checked;
                speed = checked;
                rtime = ss->ss_time;
                if (rtime != 0)
                        do_div(speed, rtime);
                rc = snprintf(*buf, *len,
                              "Last Checkpoint Position: %s\n"
                              "First Failure Position: %s\n"
                              "Checked: "LPU64"\nUpdated: "LPU64"\n"
                              "Failed: "LPU64"\nPrior Updated: "LPU64"\n"
                              "Run Time: %u seconds\n"
                              "Average Speed: "LPU64" objects/sec\n"
                              "Real-Time Speed: N/A\n",
                              scrub_key_is_empty(ss->ss_pos_last_checkpoint) ?
                                "N/A" : (char *)ss->ss_pos_last_checkpoint,
                              scrub_key_is_empty(ss->ss_pos_first_inconsistent)?
                                "N/A" : (char *)ss->ss_pos_first_inconsistent,
                              checked, ss->ss_items_updated,
                              ss->ss_items_failed, ss->ss_items_updated_prior,
                              rtime, speed);
        }
        if (rc <= 0)
                return -ENOSPC;

        *buf += rc;
        *len -= rc;
        return save - *len;
}

static int do_scrub_unit_dump(char *buf, int len, struct scrub_exec_head *seh,
                              struct scrub_unit_desc *sud,struct scrub_unit *su,
                              __u64 new, cfs_duration_t duration,
                              const char *flags[])
{
        struct scrub_head *sh = &seh->seh_head;
        int save = len;
        int rc;
        char c = ':';

        rc = snprintf(buf, len, "===============Version %u===============\n",
                      sh->sh_version);
        if (rc <= 0)
                return -ENOSPC;
        buf += rc;
        len -= rc;

        rc = scrub_type_dump(&buf, &len, sud->sud_type);
        if (rc < 0)
                return rc;

        if (scrub_status_names[sud->sud_status] != NULL)
                rc = snprintf(buf, len, "Status: %s\nFlags",
                              scrub_status_names[sud->sud_status]);
        else
                rc = snprintf(buf, len, "Status: unknown status(%d)\nFlags",
                              sud->sud_status);
        if (rc <= 0)
                return -ENOSPC;
        buf += rc;
        len -= rc;

        rc = scrub_bits_dump(&buf, &len, sud->sud_flags, &c,
                             scrub_unit_flags_names, "unknown desc flag");
        if (rc < 0)
                return rc;

        rc = scrub_bits_dump(&buf, &len, su->su_flags, &c,
                             flags, "unknown su flag");
        if (rc < 0)
                return rc;

        if (c == ':')
                rc = snprintf(buf, len,
                              ":\nSuccess Count: %u\n"
                              "Latest Start Position: %s\n"
                              "Current Position: %s\n",
                              su->su_success_count,
                              scrub_key_is_empty(su->su_pos_latest_start) ?
                                "N/A" : (char *)su->su_pos_latest_start,
                              seh->seh_pos_dump);
        else
                rc = snprintf(buf, len,
                              "\nSuccess Count: %u\n"
                              "Latest Start Position: %s\n"
                              "Current Position: %s\n",
                              su->su_success_count,
                              scrub_key_is_empty(su->su_pos_latest_start) ?
                                "N/A" : (char *)su->su_pos_latest_start,
                              seh->seh_pos_dump);
        if (rc <= 0)
                return rc;
        buf += rc;
        len -= rc;

        rc = scrub_time_dump(&buf, &len, su->su_time_last_complete,
                             "Time Since Last Completed:");
        if (rc < 0)
                return rc;

        rc = scrub_time_dump(&buf, &len, su->su_time_latest_start,
                             "Time Since Latest Start:");
        if (rc < 0)
                return rc;

        rc = scrub_time_dump(&buf, &len, su->su_time_last_checkpoint,
                             "Time Since Last Checkpoint:");
        if (rc < 0)
                return rc;

        rc = snprintf(buf, len, "---------------Parameters---------------\n");
        if (rc <= 0)
                return rc;
        buf += rc;
        len -= rc;

        rc = scrub_param_method_dump(&buf, &len, sh->sh_param_method);
        if (rc < 0)
                return rc;

        rc = snprintf(buf, len,
                      "Checkpoint Interval: %u seconds\nFlags",
                       (__u32)sh->sh_param_checkpoint_interval);
        if (rc <= 0)
                return rc;
        buf += rc;
        len -= rc;

        c = ':';
        rc = scrub_bits_dump(&buf, &len, sh->sh_param_flags & ~SPF_DRYRUN, &c,
                             scrub_param_flags_names, "unknown param flag");
        if (rc < 0)
                return rc;

        if (c == ':')
                rc = snprintf(buf, len,
                              ":\n------------Repair Statistic------------\n");
        else
                rc = snprintf(buf, len,
                              "\n------------Repair Statistic------------\n");
        if (rc <= 0)
                return rc;
        buf += rc;
        len -= rc;

        rc = scrub_statistics_dump(&buf, &len, sud, &su->su_repair, new,
                                   duration, 0);
        if (rc < 0)
                return rc;

        rc = snprintf(buf, len, "------------Dryrun Statistic------------\n");
        if (rc <= 0)
                return rc;
        buf += rc;
        len -= rc;

        rc = scrub_statistics_dump(&buf, &len, sud, &su->su_dryrun, new,
                                   duration, 1);
        if (rc < 0)
                return rc;

        return save - len;
}

static inline void scrub_build_lbuf(struct lu_buf *lbuf, void *area, size_t len)
{
        lbuf->lb_buf = area;
        lbuf->lb_len = len;
}

struct scrub_unit_desc *scrub_desc_find(struct scrub_head *head,
                                        __u16 type, int *idx)
{
        struct scrub_unit_desc *desc = NULL;
        int i;

        *idx = -1;
        for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                if (head->sh_desc[i].sud_type == type) {
                        *idx = i;
                        desc = &head->sh_desc[i];
                        break;
                }
        }

        return desc;
}
EXPORT_SYMBOL(scrub_desc_find);

void scrub_set_speed(struct scrub_exec_head *seh)
{
        __u32 limit;

        cfs_spin_lock(&seh->seh_lock);
        limit = seh->seh_head.sh_param_speed_limit;
        if (limit != SP_SPEED_NO_LIMIT) {
                if (limit > CFS_HZ) {
                        seh->seh_sleep_rate = limit /CFS_HZ;
                        seh->seh_sleep_jif = 1;
                } else {
                        seh->seh_sleep_rate = 1;
                        seh->seh_sleep_jif = CFS_HZ / limit;
                }
        } else {
                seh->seh_sleep_jif = 0;
                seh->seh_sleep_rate = 0;
        }
        cfs_spin_unlock(&seh->seh_lock);
}
EXPORT_SYMBOL(scrub_set_speed);

void scrub_check_speed(struct scrub_exec_head *seh,
                       struct ptlrpc_thread *thread)
{
        struct l_wait_info lwi;

        if (seh->seh_sleep_jif > 0 &&
            seh->seh_new_scanned >= seh->seh_sleep_rate) {
                cfs_spin_lock(&seh->seh_lock);
                if (likely(seh->seh_sleep_jif > 0 &&
                           seh->seh_new_scanned >= seh->seh_sleep_rate)) {
                        lwi = LWI_TIMEOUT_INTR(seh->seh_sleep_jif, NULL,
                                               LWI_ON_SIGNAL_NOOP, NULL);
                        cfs_spin_unlock(&seh->seh_lock);

                        l_wait_event(thread->t_ctl_waitq,
                                     !thread_is_running(thread),
                                     &lwi);
                        seh->seh_new_scanned = 0;
                } else {
                        cfs_spin_unlock(&seh->seh_lock);
                }
        }
}
EXPORT_SYMBOL(scrub_check_speed);

struct scrub_exec_unit *scrub_unit_new(scrub_prep_policy prep,
                                       scrub_exec_policy exec,
                                       scrub_post_policy post)
{
        struct scrub_exec_unit *seu;

        LASSERT(exec != NULL);

        OBD_ALLOC_PTR(seu);
        if (likely(seu != NULL)) {
                CFS_INIT_LIST_HEAD(&seu->seu_list);
                seu->seu_prep_policy = prep;
                seu->seu_exec_policy = exec;
                seu->seu_post_policy = post;
        }

        return seu;
}
EXPORT_SYMBOL(scrub_unit_new);

void scrub_unit_free(struct scrub_exec_unit *seu)
{
        LASSERT(cfs_list_empty(&seu->seu_list));

        OBD_FREE_PTR(seu);
}
EXPORT_SYMBOL(scrub_unit_free);

void scrub_unit_init(struct scrub_exec_unit *seu, __u16 type)
{
        struct scrub_unit *su = &seu->seu_unit;

        memset(su, 0, sizeof(*su));
        su->su_type = type;
        seu->seu_dirty = 1;
}
EXPORT_SYMBOL(scrub_unit_init);

void scrub_unit_reset(struct scrub_exec_unit *seu)
{
        struct scrub_unit *su = &seu->seu_unit;

        su->su_flags               = 0;
        su->su_time_latest_start   = 0;
        scrub_key_set_empty(su->su_pos_latest_start);
        memset(&su->su_repair, 0, sizeof(su->su_repair));
        memset(&su->su_dryrun, 0, sizeof(su->su_dryrun));
        seu->seu_dirty = 1;
}
EXPORT_SYMBOL(scrub_unit_reset);

static void scrub_statistics_to_cpu(struct scrub_statistics *des,
                                    struct scrub_statistics *src)
{
        memcpy(des->ss_pos_last_checkpoint, src->ss_pos_last_checkpoint,
               SCRUB_BOOKMARK_MAXLEN);
        memcpy(des->ss_pos_first_inconsistent, src->ss_pos_first_inconsistent,
               SCRUB_BOOKMARK_MAXLEN);
        des->ss_items_checked        = be64_to_cpu(src->ss_items_checked);
        des->ss_items_updated        = be64_to_cpu(src->ss_items_updated);
        des->ss_items_failed         = be64_to_cpu(src->ss_items_failed);
        des->ss_items_updated_prior  = be64_to_cpu(src->ss_items_updated_prior);
        des->ss_time                 = be32_to_cpu(src->ss_time);
        des->ss_padding              = be32_to_cpu(src->ss_padding);
}

static void scrub_unit_to_cpu(struct scrub_unit *des, struct scrub_unit *src)
{
        des->su_type                 = be16_to_cpu(src->su_type);
        des->su_flags                = be16_to_cpu(src->su_flags);
        des->su_success_count        = be32_to_cpu(src->su_success_count);
        des->su_time_last_complete   = be64_to_cpu(src->su_time_last_complete);
        des->su_time_latest_start    = be64_to_cpu(src->su_time_latest_start);
        des->su_time_last_checkpoint =
                        be64_to_cpu(src->su_time_last_checkpoint);
        memcpy(des->su_pos_latest_start, src->su_pos_latest_start,
               SCRUB_BOOKMARK_MAXLEN);
        scrub_statistics_to_cpu(&des->su_repair, &src->su_repair);
        scrub_statistics_to_cpu(&des->su_dryrun, &src->su_dryrun);
}

static void scrub_statistics_to_be(struct scrub_statistics *des,
                                    struct scrub_statistics *src)
{
        memcpy(des->ss_pos_last_checkpoint, src->ss_pos_last_checkpoint,
               SCRUB_BOOKMARK_MAXLEN);
        memcpy(des->ss_pos_first_inconsistent, src->ss_pos_first_inconsistent,
               SCRUB_BOOKMARK_MAXLEN);
        des->ss_items_checked        = cpu_to_be64(src->ss_items_checked);
        des->ss_items_updated        = cpu_to_be64(src->ss_items_updated);
        des->ss_items_failed         = cpu_to_be64(src->ss_items_failed);
        des->ss_items_updated_prior  = cpu_to_be64(src->ss_items_updated_prior);
        des->ss_time                 = cpu_to_be32(src->ss_time);
        des->ss_padding              = cpu_to_be32(src->ss_padding);
}

static void scrub_unit_to_be(struct scrub_unit *des, struct scrub_unit *src)
{
        des->su_type                 = cpu_to_be16(src->su_type);
        des->su_flags                = cpu_to_be16(src->su_flags);
        des->su_success_count        = cpu_to_be32(src->su_success_count);
        des->su_time_last_complete   = cpu_to_be64(src->su_time_last_complete);
        des->su_time_latest_start    = cpu_to_be64(src->su_time_latest_start);
        des->su_time_last_checkpoint =
                        cpu_to_be64(src->su_time_last_checkpoint);
        memcpy(des->su_pos_latest_start, src->su_pos_latest_start,
               SCRUB_BOOKMARK_MAXLEN);
        scrub_statistics_to_be(&des->su_repair, &src->su_repair);
        scrub_statistics_to_be(&des->su_dryrun, &src->su_dryrun);
}

int scrub_unit_load(const struct lu_env *env, struct scrub_exec_head *seh,
                    __u32 pos, struct scrub_exec_unit *seu)
{
        struct lu_buf lbuf;
        loff_t offset = pos;
        int rc;
        ENTRY;

        scrub_build_lbuf(&lbuf, &seh->seh_unit_buf, sizeof(seh->seh_unit_buf));
        rc = seh->seh_obj->do_body_ops->dbo_read(env, seh->seh_obj, &lbuf,
                                                 &offset, BYPASS_CAPA);
        if (rc == lbuf.lb_len) {
                scrub_unit_to_cpu(&seu->seu_unit, &seh->seh_unit_buf);
                rc = 0;
        } else if (rc != 0) {
                CERROR("Fail to load unit for scrub: rc = %d, "
                       "expected = %d.\n", rc, (int)lbuf.lb_len);
                if (rc > 0)
                        rc = -EFAULT;
        } else {
                /* return -ENOENT for empty scrub unit case. */
                rc = -ENOENT;
        }

        RETURN(rc);
}
EXPORT_SYMBOL(scrub_unit_load);

int scrub_unit_store(const struct lu_env *env, struct scrub_exec_head *seh,
                     __u32 pos, struct scrub_exec_unit *seu)
{
        struct dt_object *dt = seh->seh_obj;
        struct dt_device *dev = lu2dt_dev(dt->do_lu.lo_dev);
        struct thandle *handle;
        struct lu_buf lbuf;
        loff_t offset = pos;
        int rc;
        ENTRY;

        if (!seu->seu_dirty)
                RETURN(0);

        handle = dt_trans_create(env, dev);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for unit_store: rc = %d\n", rc);
                RETURN(rc);
        }

        rc = dt_declare_record_write(env, dt, sizeof(seu->seu_unit),
                                     (loff_t)pos, handle);
        if (rc != 0) {
                CERROR("Fail to declare trans for unit_store: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        rc = dt_trans_start(env, dev, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for unit_store: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        scrub_build_lbuf(&lbuf, &seh->seh_unit_buf, sizeof(seh->seh_unit_buf));
        scrub_unit_to_be(&seh->seh_unit_buf, &seu->seu_unit);
        rc = dt->do_body_ops->dbo_write(env, dt, &lbuf, &offset, handle,
                                        BYPASS_CAPA, 1);
        if (rc == lbuf.lb_len)
                rc = 0;
        else if (rc >= 0)
                rc = -ENOSPC;

        GOTO(stop, rc);

stop:
        handle->th_result = rc;
        dt_trans_stop(env, dev, handle);
        if (rc == 0)
                seu->seu_dirty = 0;
        return rc;
}
EXPORT_SYMBOL(scrub_unit_store);

int scrub_unit_register(struct scrub_exec_head *seh,
                        struct scrub_exec_unit *seu,
                        __u8 status, __u8 flags)
{
        struct scrub_head *sh = &seh->seh_head;
        struct scrub_unit *su = &seu->seu_unit;
        struct scrub_unit_desc *sud;
        ENTRY;

        LASSERT(cfs_list_empty(&seu->seu_list));

        if (seh->seh_active & su->su_type)
                RETURN(-EALREADY);

        sud = scrub_desc_find(&seh->seh_head, su->su_type, &seu->seu_idx);
        if (sud == NULL) {
                sud = scrub_desc_find(&seh->seh_head, 0, &seu->seu_idx);
                if (unlikely(sud == NULL))
                        RETURN(-ENOSPC);

                sh->sh_units_count++;
                sud->sud_type = su->su_type;
                sud->sud_status = status;
                sud->sud_flags = flags;
                sud->sud_offset = sizeof(struct scrub_head) +
                                  sizeof(struct scrub_unit) * seu->seu_idx;
                seh->seh_dirty = 1;
        } else {
                if (sud->sud_status != status) {
                        sud->sud_status = status;
                        seh->seh_dirty = 1;
                }

                if (sud->sud_flags != flags) {
                        sud->sud_flags = flags;
                        seh->seh_dirty = 1;
                }
        }

        cfs_list_add_tail(&seu->seu_list, &seh->seh_units);
        seh->seh_active |= su->su_type;

        RETURN(0);
}
EXPORT_SYMBOL(scrub_unit_register);

void scrub_unit_degister(struct scrub_exec_head *seh,
                         struct scrub_exec_unit *seu)
{
        LASSERT(!cfs_list_empty(&seu->seu_list));

        seh->seh_active &= ~seu->seu_unit.su_type;
        cfs_list_del_init(&seu->seu_list);
}
EXPORT_SYMBOL(scrub_unit_degister);

/* Return the size of output. */
int scrub_unit_dump(struct scrub_exec_head *seh, char *buf, int len,
                    __u16 type, const char *flags[])
{
        struct lu_env *env = NULL;
        struct scrub_unit_desc *sud;
        struct scrub_exec_unit *seu = NULL;
        struct scrub_unit *su;
        struct lu_buf lbuf;
        __u64 new = 0;
        cfs_duration_t duration = 0;
        loff_t pos;
        int idx;
        int rc;

        sud = scrub_desc_find(&seh->seh_head, type, &idx);
        if (sud == NULL)
                RETURN(0);

        if (sud->sud_status == SS_SCANNING) {
                cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                        su = &seu->seu_unit;
                        if (su->su_type == type) {
                                new = seu->seu_new_checked;
                                duration = cfs_time_current() -
                                           seh->seh_time_last_checkpoint;
                                goto dump;
                        }
                }
                LBUG();
        }

        OBD_ALLOC_PTR(env);
        if (unlikely(env == NULL))
                RETURN(-ENOMEM);

        rc = lu_env_init(env, LCT_MD_THREAD | LCT_DT_THREAD);
        if (rc != 0)
                GOTO(free_env, rc);

        OBD_ALLOC_PTR(su);
        if (unlikely(su == NULL))
                GOTO(fini_env, rc = -ENOMEM);

        scrub_build_lbuf(&lbuf, &seh->seh_unit_buf, sizeof(seh->seh_unit_buf));
        pos = sud->sud_offset;
        rc = seh->seh_obj->do_body_ops->dbo_read(env, seh->seh_obj, &lbuf, &pos,
                                                 BYPASS_CAPA);
        if (rc != lbuf.lb_len) {
                CERROR("Fail to load unit for dump scrub (%x): rc = %d, "
                       "expected = %d.\n", type, rc, (int)lbuf.lb_len);
                GOTO(free_su, rc = (rc >= 0 ? -EFAULT : rc));
        }

        scrub_unit_to_cpu(su, &seh->seh_unit_buf);

dump:
        rc = do_scrub_unit_dump(buf, len, seh, sud, su, new, duration, flags);

        GOTO(free_su, rc);

free_su:
        if (seu == NULL)
                OBD_FREE_PTR(su);
fini_env:
        if (env != NULL)
                lu_env_fini(env);
free_env:
        if (env != NULL)
                OBD_FREE_PTR(env);
        return rc;
}
EXPORT_SYMBOL(scrub_unit_dump);

struct scrub_exec_head *scrub_head_new(struct dt_object *obj)
{
        struct scrub_exec_head *seh;

        OBD_ALLOC_PTR(seh);
        if (likely(seh != NULL)) {
                CFS_INIT_LIST_HEAD(&seh->seh_units);
                cfs_spin_lock_init(&seh->seh_lock);
                cfs_init_rwsem(&seh->seh_rwsem);
                cfs_sema_init(&seh->seh_sem, 1);
                lu_object_get(&obj->do_lu);
                seh->seh_obj = obj;
        }
        return seh;
}
EXPORT_SYMBOL(scrub_head_new);

static inline void scrub_head_drop_units(struct scrub_exec_head *seh)
{
        struct scrub_exec_unit *seu, *next;

        cfs_list_for_each_entry_safe(seu, next, &seh->seh_units, seu_list) {
                scrub_unit_degister(seh, seu);
                scrub_unit_free(seu);
        }
}

void scrub_head_free(const struct lu_env *env, struct scrub_exec_head *seh)
{
        LASSERT(seh->seh_private == NULL);

        scrub_head_drop_units(seh);
        lu_object_put(env, &seh->seh_obj->do_lu);
        OBD_FREE_PTR(seh);
}
EXPORT_SYMBOL(scrub_head_free);

void scrub_head_init(struct scrub_exec_head *seh)
{
        struct scrub_head *sh = &seh->seh_head;

        memset(sh, 0, sizeof(*sh));
        sh->sh_version                   = SCRUB_VERSION_V1;
        sh->sh_param_method              = SM_OTABLE;
        sh->sh_param_checkpoint_interval = SP_CHECKPOING_INTERVAL_DEF;
        sh->sh_param_speed_limit         = SP_SPEED_LIMIT_DEF;
        seh->seh_dirty = 1;
}
EXPORT_SYMBOL(scrub_head_init);

static void scrub_head_to_cpu(struct scrub_head *des,
                              struct scrub_head *src)
{
        int i;

        des->sh_version                   = be32_to_cpu(src->sh_version);
        des->sh_units_count               = be16_to_cpu(src->sh_units_count);
        des->sh_param_flags               = be16_to_cpu(src->sh_param_flags);
        des->sh_param_method              = be16_to_cpu(src->sh_param_method);
        des->sh_param_checkpoint_interval =
                        be16_to_cpu(src->sh_param_checkpoint_interval);
        des->sh_param_speed_limit         =
                        be32_to_cpu(src->sh_param_speed_limit);

        for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                des->sh_desc[i].sud_type   =
                        be16_to_cpu(src->sh_desc[i].sud_type);
                des->sh_desc[i].sud_status = src->sh_desc[i].sud_status;
                des->sh_desc[i].sud_flags  = src->sh_desc[i].sud_flags;
                des->sh_desc[i].sud_offset =
                        be32_to_cpu(src->sh_desc[i].sud_offset);
        }
}

static void scrub_head_to_be(struct scrub_head *des,
                             struct scrub_head *src)
{
        int i;

        des->sh_version                   = cpu_to_be32(src->sh_version);
        des->sh_units_count               = cpu_to_be16(src->sh_units_count);
        des->sh_param_flags               = cpu_to_be16(src->sh_param_flags);
        des->sh_param_method              = cpu_to_be16(src->sh_param_method);
        des->sh_param_checkpoint_interval =
                        cpu_to_be16(src->sh_param_checkpoint_interval);
        des->sh_param_speed_limit         =
                        cpu_to_be32(src->sh_param_speed_limit);

        for (i = 0; i < SCRUB_DESC_COUNT; i++) {
                des->sh_desc[i].sud_type   =
                        cpu_to_be16(src->sh_desc[i].sud_type);
                des->sh_desc[i].sud_status = src->sh_desc[i].sud_status;
                des->sh_desc[i].sud_flags  = src->sh_desc[i].sud_flags;
                des->sh_desc[i].sud_offset =
                        cpu_to_be32(src->sh_desc[i].sud_offset);
        }
}

int scrub_head_load(const struct lu_env *env, struct scrub_exec_head *seh)
{
        struct lu_buf lbuf;
        loff_t pos = 0;
        int rc;
        ENTRY;

        scrub_build_lbuf(&lbuf, &seh->seh_head_buf, sizeof(seh->seh_head_buf));
        rc = seh->seh_obj->do_body_ops->dbo_read(env, seh->seh_obj, &lbuf, &pos,
                                                 BYPASS_CAPA);
        if (rc == lbuf.lb_len) {
                scrub_head_to_cpu(&seh->seh_head, &seh->seh_head_buf);
                rc = 0;
        } else if (rc != 0) {
                CERROR("Fail to load head for scrub: rc = %d, "
                       "expected = %d.\n", rc, (int)lbuf.lb_len);
                if (rc > 0)
                        rc = -EFAULT;
        } else {
                /* return -ENOENT for empty scrub file case. */
                rc = -ENOENT;
        }

        RETURN(rc);
}
EXPORT_SYMBOL(scrub_head_load);

/**
 * \retval      +v: 'v' parts (head or units) have been written back.
 * \retval       0: no dirty parts (head or units) need to be written back.
 * \retva       -v: on error.
 */
static int do_scrub_head_store(const struct lu_env *env,
                               struct scrub_exec_head *seh)
{
        struct dt_object *dt = seh->seh_obj;
        struct dt_device *dev = lu2dt_dev(dt->do_lu.lo_dev);
        struct scrub_exec_unit *seu;
        struct scrub_unit_desc *sud;
        struct thandle *handle;
        struct lu_buf lbuf;
        loff_t pos = 0;
        int dirty = 0;
        int rc;
        ENTRY;

        handle = dt_trans_create(env, dev);
        if (IS_ERR(handle)) {
                rc = PTR_ERR(handle);
                CERROR("Fail to create trans for head_store: rc = %d\n", rc);
                RETURN(rc);
        }

        if (seh->seh_dirty) {
                dirty++;
                rc = dt_declare_record_write(env, dt, sizeof(struct scrub_head),
                                             pos, handle);
                if (rc != 0) {
                        CERROR("Fail to declare trans for head_store (1): "
                               "rc = %d\n", rc);
                        GOTO(stop, rc);
                }
        }

        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                if (!seu->seu_dirty)
                        continue;

                dirty++;
                sud = &seh->seh_head.sh_desc[seu->seu_idx];
                rc = dt_declare_record_write(env, dt, sizeof(struct scrub_unit),
                                             (loff_t)sud->sud_offset, handle);
                if (rc != 0) {
                        CERROR("Fail to declare trans for head_store (2): "
                               "rc = %d\n", rc);
                        GOTO(stop, rc);
                }
        }

        if (dirty == 0)
                GOTO(stop, rc = 0);

        rc = dt_trans_start(env, dev, handle);
        if (rc != 0) {
                CERROR("Fail to start trans for head_store: rc = %d\n", rc);
                GOTO(stop, rc);
        }

        if (seh->seh_dirty) {
                scrub_build_lbuf(&lbuf, &seh->seh_head_buf,
                                 sizeof(struct scrub_head));
                scrub_head_to_be(&seh->seh_head_buf, &seh->seh_head);
                rc = dt->do_body_ops->dbo_write(env, dt, &lbuf, &pos, handle,
                                                BYPASS_CAPA, 1);
                if (rc == lbuf.lb_len) {
                        rc = 0;
                } else {
                        if (rc >= 0)
                                rc = -ENOSPC;
                        GOTO(stop, rc);
                }
        }

        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                if (!seu->seu_dirty)
                        continue;

                sud = &seh->seh_head.sh_desc[seu->seu_idx];
                scrub_build_lbuf(&lbuf, &seh->seh_unit_buf,
                                 sizeof(struct scrub_unit));
                scrub_unit_to_be(&seh->seh_unit_buf, &seu->seu_unit);
                pos = sud->sud_offset;
                rc = dt->do_body_ops->dbo_write(env, dt, &lbuf, &pos, handle,
                                                BYPASS_CAPA, 1);
                if (rc == lbuf.lb_len) {
                        rc = 0;
                } else {
                        if (rc >= 0)
                                rc = -ENOSPC;
                        GOTO(stop, rc);
                }
        }

        GOTO(stop, rc = 0);

stop:
        handle->th_result = rc;
        dt_trans_stop(env, dev, handle);
        return rc < 0 ? rc : dirty;
}

int scrub_head_store(const struct lu_env *env, struct scrub_exec_head *seh)
{
        struct scrub_exec_unit *seu;
        int rc;

        rc = do_scrub_head_store(env, seh);
        if (rc > 0) {
                /* Clear dirty flags after all parts are written. */
                seh->seh_dirty = 0;
                cfs_list_for_each_entry(seu, &seh->seh_units, seu_list)
                        seu->seu_dirty = 0;
                rc = 0;
        }
        return rc;
}
EXPORT_SYMBOL(scrub_head_store);

static inline void scrub_head_cleanup(struct scrub_exec_head *seh)
{
        scrub_head_drop_units(seh);
        seh->seh_new_scanned = 0;
        seh->seh_time_last_checkpoint = 0;
        seh->seh_time_next_checkpoint = 0;
        scrub_key_set_empty(seh->seh_pos_checkpoint);
        seh->seh_dirty = 0;
}

void scrub_head_reset(const struct lu_env *env, struct scrub_exec_head *seh)
{
        scrub_head_cleanup(seh);
        scrub_head_load(env, seh);
}
EXPORT_SYMBOL(scrub_head_reset);

int scrub_prep(const struct lu_env *env, struct scrub_exec_head *seh)
{
        struct scrub_exec_unit *seu;
        struct scrub_unit *su;
        int rc;
        ENTRY;

        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                su = &seu->seu_unit;
                if (seu->seu_prep_policy != NULL) {
                        rc = seu->seu_prep_policy(env, seh, seu);
                        if (rc != 0)
                                RETURN(rc);
                }
                if (!scrub_key_is_empty(seh->seh_pos_checkpoint))
                        memcpy(su->su_pos_latest_start,
                               seh->seh_pos_checkpoint,
                               SCRUB_BOOKMARK_MAXLEN);
                su->su_time_latest_start = cfs_time_current_sec();
                seu->seu_dirty = 1;
        }

        rc = do_scrub_head_store(env, seh);
        if (rc > 0) {
                /* Clear dirty flags after all parts are written. */
                seh->seh_dirty = 0;
                cfs_list_for_each_entry(seu, &seh->seh_units, seu_list)
                        seu->seu_dirty = 0;
                rc = 0;
        } else if (rc < 0) {
                RETURN(rc);
        }

        seh->seh_time_last_checkpoint = cfs_time_current();
        seh->seh_time_next_checkpoint = seh->seh_time_last_checkpoint +
                cfs_time_seconds(seh->seh_head.sh_param_checkpoint_interval);

        RETURN(0);
}
EXPORT_SYMBOL(scrub_prep);

/**
 * \retval      0: continue in spite of failure or not.
 * \retval others: scrub scanning will failout.
 */
int scrub_exec(const struct lu_env *env, struct scrub_exec_head *seh, int rc)
{
        struct scrub_exec_unit *seu;
        int rc1 = 0;
        int rc2;
        ENTRY;

        seh->seh_new_scanned++;
        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                rc2 = seu->seu_exec_policy(env, seh, seu, rc);
                if (rc2 != 0)
                        rc1 = rc2;
        }
        rc = seh->seh_head.sh_param_flags & SPF_FAILOUT ? rc1 : 0;

        RETURN(rc);
}
EXPORT_SYMBOL(scrub_exec);

static void scrub_statistics_update(struct scrub_exec_head *seh,
                                    struct scrub_exec_unit *seu,
                                    __u64 ctime, cfs_duration_t duration)
{
        struct scrub_unit_desc *sud = &seh->seh_head.sh_desc[seu->seu_idx];
        struct scrub_unit *su = &seu->seu_unit;
        struct scrub_statistics *ss;

        LASSERT(!scrub_key_is_empty(seh->seh_pos_checkpoint));

        su->su_time_last_checkpoint = ctime;
        if (sud->sud_flags & SUF_DRYRUN)
                ss = &su->su_dryrun;
        else
                ss = &su->su_repair;

        memcpy(ss->ss_pos_last_checkpoint,
               seh->seh_pos_checkpoint, SCRUB_BOOKMARK_MAXLEN);
        ss->ss_items_checked += seu->seu_new_checked;
        ss->ss_time += cfs_duration_sec(duration + HALF_SEC);

        seu->seu_new_checked = 0;
        seu->seu_dirty = 1;
}

void scrub_post(const struct lu_env *env, struct scrub_exec_head *seh, int rc)
{
        struct scrub_unit_desc *sud;
        struct scrub_exec_unit *seu;
        struct scrub_unit *su;
        __u64 ctime;
        cfs_duration_t duration;
        ENTRY;

        ctime = cfs_time_current_sec();
        duration = cfs_time_current() - seh->seh_time_last_checkpoint;
        if (unlikely(duration == 0))
                duration = 1;

        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                if (seu->seu_new_checked > 0)
                        scrub_statistics_update(seh, seu, ctime, duration);
                sud = &seh->seh_head.sh_desc[seu->seu_idx];
                if (rc > 0) {
                        sud->sud_status = SS_COMPLETED;
                        su = &seu->seu_unit;
                        if (!(sud->sud_flags & SUF_DRYRUN) ||
                            su->su_dryrun.ss_items_failed == 0) {
                                sud->sud_flags &= ~ SUF_INCONSISTENT;
                                su->su_success_count++;
                                su->su_time_last_complete = ctime;
                                seu->seu_dirty = 1;
                        }
                } else if (rc == 0) {
                        sud->sud_status = SS_PAUSED;
                } else {
                        sud->sud_status = SS_FAILED;
                }

                seh->seh_dirty = 1;
                if (seu->seu_post_policy != NULL)
                        seu->seu_post_policy(env, seh, seu, rc);
        }

        rc = do_scrub_head_store(env, seh);
        if (rc < 0)
                CERROR("Fail to write the last checkpoint: rc = %d\n", rc);

        /* For stop case, cleanup the environment in spite of whether
         * write succeeded or not, because we have no chance to write
         * again even if the do_scrub_head_store() failed. */
        scrub_head_cleanup(seh);

        EXIT;
}
EXPORT_SYMBOL(scrub_post);

int scrub_checkpoint(const struct lu_env *env, struct scrub_exec_head *seh)
{
        struct scrub_exec_unit *seu;
        __u64 ctime;
        cfs_duration_t duration;
        int rc;
        ENTRY;

        ctime = cfs_time_current_sec();
        duration = cfs_time_current() - seh->seh_time_last_checkpoint;
        cfs_list_for_each_entry(seu, &seh->seh_units, seu_list) {
                if (seu->seu_new_checked > 0)
                        scrub_statistics_update(seh, seu, ctime, duration);
        }
        rc = do_scrub_head_store(env, seh);
        if (rc > 0) {
                /* Clear dirty flags after all parts are written. */
                seh->seh_dirty = 0;
                cfs_list_for_each_entry(seu, &seh->seh_units, seu_list)
                        seu->seu_dirty = 0;
                rc = 0;
        } else if (rc < 0) {
                RETURN(rc);
        }

        seh->seh_time_last_checkpoint = cfs_time_current();
        seh->seh_time_next_checkpoint = seh->seh_time_last_checkpoint +
                cfs_time_seconds(seh->seh_head.sh_param_checkpoint_interval);

        RETURN(0);
}
EXPORT_SYMBOL(scrub_checkpoint);
