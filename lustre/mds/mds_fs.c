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
 * version 2 along with this program; If not, see [sun.com URL with a
 * copy of GPLv2].
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/mds/mds_fs.c
 *
 * Lustre Metadata Server (MDS) filesystem interface code
 *
 * Author: Andreas Dilger <adilger@clusterfs.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_MDS

#include <linux/module.h>
#include <linux/kmod.h>
#include <linux/version.h>
#include <linux/sched.h>
#include <lustre_quota.h>
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
#include <linux/mount.h>
#endif
#include <lustre_mds.h>
#include <obd_class.h>
#include <obd_support.h>
#include <lustre_lib.h>
#include <lustre_fsfilt.h>
#include <lustre_disk.h>
#include <libcfs/list.h>

#include "mds_internal.h"


static int mds_export_stats_init(struct obd_device *obd,
                                 struct obd_export *exp,
                                 void *client_nid)
  {
        int rc, num_stats, newnid = 0;

        rc = lprocfs_exp_setup(exp, client_nid, &newnid);
        if (rc)
                return rc;

        if (newnid) {
                struct nid_stat *tmp = exp->exp_nid_stats;
                LASSERT(tmp != NULL);

                num_stats = (sizeof(*obd->obd_type->typ_ops) / sizeof(void *)) +
                             LPROC_MDS_LAST - 1;
                tmp->nid_stats = lprocfs_alloc_stats(num_stats,
                                                     LPROCFS_STATS_FLAG_NOPERCPU);
                if (tmp->nid_stats == NULL)
                        return -ENOMEM;

                lprocfs_init_ops_stats(LPROC_MDS_LAST, tmp->nid_stats);
                rc = lprocfs_register_stats(tmp->nid_proc, "stats",
                                            tmp->nid_stats);
                if (rc)
                        return rc;

                mds_stats_counter_init(tmp->nid_stats);
        }

        return 0;
}

/* Add client data to the MDS.  We use a bitmap to locate a free space
 * in the last_rcvd file if cl_off is -1 (i.e. a new client).
 * Otherwise, we have just read the data from the last_rcvd file and
 * we know its offset.
 *
 * It should not be possible to fail adding an existing client - otherwise
 * mds_init_server_data() callsite needs to be fixed.
 */
int mds_client_add(struct obd_device *obd, struct obd_export *exp,
                   int cl_idx, void *localdata)
{
        struct mds_obd *mds = &obd->u.mds;
        struct mds_export_data *med = &exp->exp_mds_data;
        unsigned long *bitmap = mds->mds_client_bitmap;
        int new_client = (cl_idx == -1);
        int rc;
        ENTRY;

        LASSERT(bitmap != NULL);
        LASSERTF(cl_idx > -2, "%d\n", cl_idx);

        /* XXX if lcd_uuid were a real obd_uuid, I could use obd_uuid_equals */
        if (!strcmp(med->med_lcd->lcd_uuid, obd->obd_uuid.uuid))
                RETURN(0);

        /* the bitmap operations can handle cl_idx > sizeof(long) * 8, so
         * there's no need for extra complication here
         */
        if (new_client) {
                cl_idx = find_first_zero_bit(bitmap, LR_MAX_CLIENTS);
        repeat:
                if (cl_idx >= LR_MAX_CLIENTS ||
                    OBD_FAIL_CHECK_ONCE(OBD_FAIL_MDS_CLIENT_ADD)) {
                        CERROR("no room for %u clients - fix LR_MAX_CLIENTS\n",
                               cl_idx);
                        return -EOVERFLOW;
                }
                if (test_and_set_bit(cl_idx, bitmap)) {
                        cl_idx = find_next_zero_bit(bitmap, LR_MAX_CLIENTS,
                                                    cl_idx);
                        goto repeat;
                }
        } else {
                if (test_and_set_bit(cl_idx, bitmap)) {
                        CERROR("MDS client %d: bit already set in bitmap!!\n",
                               cl_idx);
                        LBUG();
                }
        }

        CDEBUG(D_INFO, "client at idx %d with UUID '%s' added\n",
               cl_idx, med->med_lcd->lcd_uuid);

        med->med_lr_idx = cl_idx;
        med->med_lr_off = le32_to_cpu(mds->mds_server_data->lsd_client_start) +
                (cl_idx * le16_to_cpu(mds->mds_server_data->lsd_client_size));
        LASSERTF(med->med_lr_off > 0, "med_lr_off = %llu\n", med->med_lr_off);
        mds_export_stats_init(obd, exp, localdata);

        if (new_client) {
                struct lvfs_run_ctxt saved;
                loff_t off = med->med_lr_off;
                struct file *file = mds->mds_rcvd_filp;
                void *handle;

                push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                handle = fsfilt_start(obd, file->f_dentry->d_inode,
                                      FSFILT_OP_SETATTR, NULL);
                if (IS_ERR(handle)) {
                        rc = PTR_ERR(handle);
                        CERROR("unable to start transaction: rc %d\n", rc);
                } else {
                        rc = fsfilt_add_journal_cb(obd, 0, handle,
                                                   target_client_add_cb, exp);
                        if (rc == 0) {
                                spin_lock(&exp->exp_lock);
                                exp->exp_need_sync = 1;
                                spin_unlock(&exp->exp_lock);
                        }
                        rc = fsfilt_write_record(obd, file, med->med_lcd,
                                                 sizeof(*med->med_lcd),
                                                 &off, rc /* sync if no cb */);
                        fsfilt_commit(obd, file->f_dentry->d_inode, handle, 0);
                }

                pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);

                if (rc)
                        return rc;
                CDEBUG(D_INFO, "wrote client lcd at idx %u off %llu (len %u)\n",
                       med->med_lr_idx, med->med_lr_off,
                       (unsigned int)sizeof(*med->med_lcd));
        }
        return 0;
}

int mds_client_free(struct obd_export *exp)
{
        struct mds_export_data *med = &exp->exp_mds_data;
        struct mds_obd *mds = &exp->exp_obd->u.mds;
        struct obd_device *obd = exp->exp_obd;
        struct lsd_client_data zero_lcd;
        struct lvfs_run_ctxt saved;
        int rc;
        loff_t off;
        ENTRY;

        if (!med->med_lcd)
                RETURN(0);

        /* XXX if lcd_uuid were a real obd_uuid, I could use obd_uuid_equals */
        if (!strcmp(med->med_lcd->lcd_uuid, obd->obd_uuid.uuid))
                GOTO(free, 0);

        CDEBUG(D_INFO, "freeing client at idx %u, offset %lld with UUID '%s'\n",
               med->med_lr_idx, med->med_lr_off, med->med_lcd->lcd_uuid);

        LASSERT(mds->mds_client_bitmap != NULL);

        lprocfs_exp_cleanup(exp);

        off = med->med_lr_off;

        /* Don't clear med_lr_idx here as it is likely also unset.  At worst
         * we leak a client slot that will be cleaned on the next recovery. */
        if (off <= 0) {
                CERROR("%s: client idx %d has offset %lld\n",
                        obd->obd_name, med->med_lr_idx, off);
                GOTO(free, rc = -EINVAL);
        }

        /* Clear the bit _after_ zeroing out the client so we don't
           race with mds_client_add and zero out new clients.*/
        if (!test_bit(med->med_lr_idx, mds->mds_client_bitmap)) {
                CERROR("MDS client %u: bit already clear in bitmap!!\n",
                       med->med_lr_idx);
                LBUG();
        }

        if (!(exp->exp_flags & OBD_OPT_FAILOVER)) {
                memset(&zero_lcd, 0, sizeof(zero_lcd));
                push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
                rc = fsfilt_write_record(obd, mds->mds_rcvd_filp, &zero_lcd,
                                         sizeof(zero_lcd), &off,
                                         (!exp->exp_libclient ||
                                          exp->exp_need_sync));
                pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);

                CDEBUG(rc == 0 ? D_INFO : D_ERROR,
                       "zeroing out client %s idx %u in %s rc %d\n",
                       med->med_lcd->lcd_uuid, med->med_lr_idx, LAST_RCVD, rc);
        }

        if (!test_and_clear_bit(med->med_lr_idx, mds->mds_client_bitmap)) {
                CERROR("MDS client %u: bit already clear in bitmap!!\n",
                       med->med_lr_idx);
                LBUG();
        }


        /* Make sure the server's last_transno is up to date. Do this
         * after the client is freed so we know all the client's
         * transactions have been committed. */
        mds_update_server_data(exp->exp_obd, 0);

        EXIT;
 free:
        OBD_FREE_PTR(med->med_lcd);
        med->med_lcd = NULL;

        return 0;
}

static int mds_server_free_data(struct mds_obd *mds)
{
        OBD_FREE(mds->mds_client_bitmap, LR_MAX_CLIENTS / 8);
        OBD_FREE(mds->mds_server_data, sizeof(*mds->mds_server_data));
        mds->mds_server_data = NULL;

        return 0;
}

static int mds_init_server_data(struct obd_device *obd, struct file *file)
{
        struct mds_obd *mds = &obd->u.mds;
        struct lr_server_data *lsd;
        struct lsd_client_data *lcd = NULL;
        loff_t off = 0;
        unsigned long last_rcvd_size = i_size_read(file->f_dentry->d_inode);
        __u64 mount_count;
        int cl_idx, rc = 0;
        ENTRY;

        /* ensure padding in the struct is the correct size */
        LASSERT(offsetof(struct lr_server_data, lsd_padding) +
                sizeof(lsd->lsd_padding) == LR_SERVER_SIZE);
        LASSERT(offsetof(struct lsd_client_data, lcd_padding) +
                sizeof(lcd->lcd_padding) == LR_CLIENT_SIZE);

        OBD_ALLOC_WAIT(lsd, sizeof(*lsd));
        if (!lsd)
                RETURN(-ENOMEM);

        OBD_ALLOC_WAIT(mds->mds_client_bitmap, LR_MAX_CLIENTS / 8);
        if (!mds->mds_client_bitmap) {
                OBD_FREE(lsd, sizeof(*lsd));
                RETURN(-ENOMEM);
        }

        mds->mds_server_data = lsd;

        if (last_rcvd_size == 0) {
                LCONSOLE_WARN("%s: new disk, initializing\n", obd->obd_name);

                memcpy(lsd->lsd_uuid, obd->obd_uuid.uuid,sizeof(lsd->lsd_uuid));
                lsd->lsd_last_transno = 0;
                mount_count = lsd->lsd_mount_count = 0;
                lsd->lsd_server_size = cpu_to_le32(LR_SERVER_SIZE);
                lsd->lsd_client_start = cpu_to_le32(LR_CLIENT_START);
                lsd->lsd_client_size = cpu_to_le16(LR_CLIENT_SIZE);
                lsd->lsd_feature_rocompat = cpu_to_le32(OBD_ROCOMPAT_LOVOBJID);
                lsd->lsd_feature_incompat = cpu_to_le32(OBD_INCOMPAT_MDT);
        } else {
                rc = fsfilt_read_record(obd, file, lsd, sizeof(*lsd), &off);
                if (rc) {
                        CERROR("error reading MDS %s: rc %d\n", LAST_RCVD, rc);
                        GOTO(err_msd, rc);
                }
                if (strcmp(lsd->lsd_uuid, obd->obd_uuid.uuid) != 0) {
                        LCONSOLE_ERROR_MSG(0x157, "Trying to start OBD %s using"
                                           " the wrong disk %s. Were the /dev/ "
                                           "assignments rearranged?\n",
                                           obd->obd_uuid.uuid, lsd->lsd_uuid);
                        GOTO(err_msd, rc = -EINVAL);
                }
                /* COMPAT_146 */
                /* Assume old last_rcvd format unless I_C_LR is set */
                if (!(lsd->lsd_feature_incompat &
                      cpu_to_le32(OBD_INCOMPAT_COMMON_LR)))
                        lsd->lsd_mount_count = lsd->lsd_compat14;
                /* end COMPAT_146 */
                mount_count = le64_to_cpu(lsd->lsd_mount_count);
        }

        if (lsd->lsd_feature_incompat & ~cpu_to_le32(MDT_INCOMPAT_SUPP)) {
                CERROR("%s: unsupported incompat filesystem feature(s) %x\n",
                       obd->obd_name, le32_to_cpu(lsd->lsd_feature_incompat) &
                       ~MDT_INCOMPAT_SUPP);
                GOTO(err_msd, rc = -EINVAL);
        }
        if (lsd->lsd_feature_rocompat & ~cpu_to_le32(MDT_ROCOMPAT_SUPP)) {
                CERROR("%s: unsupported read-only filesystem feature(s) %x\n",
                       obd->obd_name, le32_to_cpu(lsd->lsd_feature_rocompat) &
                       ~MDT_ROCOMPAT_SUPP);
                /* Do something like remount filesystem read-only */
                GOTO(err_msd, rc = -EINVAL);
        }

        lsd->lsd_feature_compat = cpu_to_le32(OBD_COMPAT_MDT);

        mds->mds_last_transno = le64_to_cpu(lsd->lsd_last_transno);

        CDEBUG(D_INODE, "%s: server last_transno: "LPU64"\n",
               obd->obd_name, mds->mds_last_transno);
        CDEBUG(D_INODE, "%s: server mount_count: "LPU64"\n",
               obd->obd_name, mount_count + 1);
        CDEBUG(D_INODE, "%s: server data size: %u\n",
               obd->obd_name, le32_to_cpu(lsd->lsd_server_size));
        CDEBUG(D_INODE, "%s: per-client data start: %u\n",
               obd->obd_name, le32_to_cpu(lsd->lsd_client_start));
        CDEBUG(D_INODE, "%s: per-client data size: %u\n",
               obd->obd_name, le32_to_cpu(lsd->lsd_client_size));
        CDEBUG(D_INODE, "%s: last_rcvd size: %lu\n",
               obd->obd_name, last_rcvd_size);
        CDEBUG(D_INODE, "%s: last_rcvd clients: %lu\n", obd->obd_name,
               last_rcvd_size <= le32_to_cpu(lsd->lsd_client_start) ? 0 :
               (last_rcvd_size - le32_to_cpu(lsd->lsd_client_start)) /
                le16_to_cpu(lsd->lsd_client_size));

        if (!lsd->lsd_server_size || !lsd->lsd_client_start ||
            !lsd->lsd_client_size) {
                CERROR("Bad last_rcvd contents!\n");
                GOTO(err_msd, rc = -EINVAL);
        }

        /* When we do a clean MDS shutdown, we save the last_transno into
         * the header.  If we find clients with higher last_transno values
         * then those clients may need recovery done. */
        for (cl_idx = 0, off = le32_to_cpu(lsd->lsd_client_start);
             off < last_rcvd_size; cl_idx++) {
                __u64 last_transno;
                struct obd_export *exp;
                struct mds_export_data *med;

                if (!lcd) {
                        OBD_ALLOC_WAIT(lcd, sizeof(*lcd));
                        if (!lcd)
                                GOTO(err_client, rc = -ENOMEM);
                }

                /* Don't assume off is incremented properly by
                 * fsfilt_read_record(), in case sizeof(*lcd)
                 * isn't the same as lsd->lsd_client_size.  */
                off = le32_to_cpu(lsd->lsd_client_start) +
                        cl_idx * le16_to_cpu(lsd->lsd_client_size);
                rc = fsfilt_read_record(obd, file, lcd, sizeof(*lcd), &off);
                if (rc) {
                        CERROR("error reading MDS %s idx %d, off %llu: rc %d\n",
                               LAST_RCVD, cl_idx, off, rc);
                        break; /* read error shouldn't cause startup to fail */
                }

                if (lcd->lcd_uuid[0] == '\0') {
                        CDEBUG(D_INFO, "skipping zeroed client at offset %d\n",
                               cl_idx);
                        continue;
                }

                last_transno = le64_to_cpu(lcd->lcd_last_transno) >
                               le64_to_cpu(lcd->lcd_last_close_transno) ?
                               le64_to_cpu(lcd->lcd_last_transno) :
                               le64_to_cpu(lcd->lcd_last_close_transno);

                /* These exports are cleaned up by mds_disconnect(), so they
                 * need to be set up like real exports as mds_connect() does.
                 */
                CDEBUG(D_HA, "RCVRNG CLIENT uuid: %s idx: %d lr: "LPU64
                       " srv lr: "LPU64" lx: "LPU64"\n", lcd->lcd_uuid, cl_idx,
                       last_transno, le64_to_cpu(lsd->lsd_last_transno),
                       le64_to_cpu(lcd->lcd_last_xid));

                exp = class_new_export(obd, (struct obd_uuid *)lcd->lcd_uuid);
                if (IS_ERR(exp)) {
                        if (PTR_ERR(exp) == -EALREADY) {
                                /* export already exists, zero out this one */
                                lcd->lcd_uuid[0] = '\0';
                        } else {
                                GOTO(err_client, rc = PTR_ERR(exp));
                        }
                } else {
                        med = &exp->exp_mds_data;
                        med->med_lcd = lcd;
                        rc = mds_client_add(obd, exp, cl_idx, NULL);
                        /* can't fail for existing client */
                        LASSERTF(rc == 0, "rc = %d\n", rc);

                        lcd = NULL;

                        spin_lock(&exp->exp_lock);
                        exp->exp_replay_needed = 1;
                        exp->exp_connecting = 0;
                        spin_unlock(&exp->exp_lock);

                        obd->obd_recoverable_clients++;
                        obd->obd_max_recoverable_clients++;
                        class_export_put(exp);
                }

                /* Need to check last_rcvd even for duplicated exports. */
                CDEBUG(D_OTHER, "client at idx %d has last_transno = "LPU64"\n",
                       cl_idx, last_transno);

                if (last_transno > mds->mds_last_transno)
                        mds->mds_last_transno = last_transno;
        }

        if (lcd)
                OBD_FREE_PTR(lcd);

        obd->obd_last_committed = mds->mds_last_transno;

        if (obd->obd_recoverable_clients) {
                CWARN("RECOVERY: service %s, %d recoverable clients, "
                      "last_transno "LPU64"\n", obd->obd_name,
                      obd->obd_recoverable_clients, mds->mds_last_transno);
                obd->obd_next_recovery_transno = obd->obd_last_committed + 1;
                obd->obd_recovering = 1;
                obd->obd_recovery_start = 0;
                obd->obd_recovery_end = 0;
                obd->obd_recovery_timeout = OBD_RECOVERY_FACTOR * obd_timeout;
#ifdef CRAY_XT3
                /* bz13079: this won't be changed for mds */
                obd->obd_recovery_max_time = OBD_RECOVERY_MAX_TIME;
#endif
        }

        mds->mds_mount_count = mount_count + 1;
        lsd->lsd_mount_count = lsd->lsd_compat14 =
                cpu_to_le64(mds->mds_mount_count);

        /* save it, so mount count and last_transno is current */
        rc = mds_update_server_data(obd, 1);
        if (rc)
                GOTO(err_client, rc);

        RETURN(0);

err_client:
        class_disconnect_exports(obd);
err_msd:
        mds_server_free_data(mds);
        RETURN(rc);
}

int mds_fs_setup(struct obd_device *obd, struct vfsmount *mnt)
{
        struct mds_obd *mds = &obd->u.mds;
        struct lvfs_run_ctxt saved;
        struct dentry *dentry;
        struct file *file;
        int rc;
        ENTRY;

        OBD_FAIL_RETURN(OBD_FAIL_MDS_FS_SETUP, -ENOENT);

        rc = cleanup_group_info();
        if (rc)
                RETURN(rc);

        mds->mds_vfsmnt = mnt;
        /* why not mnt->mnt_sb instead of mnt->mnt_root->d_inode->i_sb? */
        obd->u.obt.obt_sb = mnt->mnt_root->d_inode->i_sb;

        rc = fsfilt_setup(obd, obd->u.obt.obt_sb);
        if (rc)
                RETURN(rc);

        OBD_SET_CTXT_MAGIC(&obd->obd_lvfs_ctxt);
        obd->obd_lvfs_ctxt.pwdmnt = mnt;
        obd->obd_lvfs_ctxt.pwd = mnt->mnt_root;
        obd->obd_lvfs_ctxt.fs = get_ds();
        obd->obd_lvfs_ctxt.cb_ops = mds_lvfs_ops;

        /* setup the directory tree */
        push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
        dentry = simple_mkdir(current->fs->pwd, mnt, "ROOT", 0755, 0);
        if (IS_ERR(dentry)) {
                rc = PTR_ERR(dentry);
                CERROR("cannot create ROOT directory: rc = %d\n", rc);
                GOTO(err_pop, rc);
        }

        mds->mds_rootfid.id = dentry->d_inode->i_ino;
        mds->mds_rootfid.generation = dentry->d_inode->i_generation;
        mds->mds_rootfid.f_type = S_IFDIR;

        dput(dentry);

        dentry = lookup_one_len("__iopen__", current->fs->pwd,
                                strlen("__iopen__"));
        if (IS_ERR(dentry)) {
                rc = PTR_ERR(dentry);
                CERROR("cannot lookup __iopen__ directory: rc = %d\n", rc);
                GOTO(err_pop, rc);
        }

        mds->mds_fid_de = dentry;
        if (!dentry->d_inode || is_bad_inode(dentry->d_inode)) {
                rc = -ENOENT;
                CERROR("__iopen__ directory has no inode? rc = %d\n", rc);
                GOTO(err_fid, rc);
        }

        dentry = simple_mkdir(current->fs->pwd, mnt, "PENDING", 0777, 1);
        if (IS_ERR(dentry)) {
                rc = PTR_ERR(dentry);
                CERROR("cannot create PENDING directory: rc = %d\n", rc);
                GOTO(err_fid, rc);
        }
        mds->mds_pending_dir = dentry;

        /* COMPAT_146 */
        dentry = simple_mkdir(current->fs->pwd, mnt, MDT_LOGS_DIR, 0777, 1);
        if (IS_ERR(dentry)) {
                rc = PTR_ERR(dentry);
                CERROR("cannot create %s directory: rc = %d\n",
                       MDT_LOGS_DIR, rc);
                GOTO(err_pending, rc);
        }
        mds->mds_logs_dir = dentry;
        /* end COMPAT_146 */

        dentry = simple_mkdir(current->fs->pwd, mnt, "OBJECTS", 0777, 1);
        if (IS_ERR(dentry)) {
                rc = PTR_ERR(dentry);
                CERROR("cannot create OBJECTS directory: rc = %d\n", rc);
                GOTO(err_logs, rc);
        }
        mds->mds_objects_dir = dentry;

        /* open and test the last rcvd file */
        file = filp_open(LAST_RCVD, O_RDWR | O_CREAT, 0644);
        if (IS_ERR(file)) {
                rc = PTR_ERR(file);
                CERROR("cannot open/create %s file: rc = %d\n", LAST_RCVD, rc);
                GOTO(err_objects, rc = PTR_ERR(file));
        }
        mds->mds_rcvd_filp = file;
        if (!S_ISREG(file->f_dentry->d_inode->i_mode)) {
                CERROR("%s is not a regular file!: mode = %o\n", LAST_RCVD,
                       file->f_dentry->d_inode->i_mode);
                GOTO(err_last_rcvd, rc = -ENOENT);
        }

        rc = mds_init_server_data(obd, file);
        if (rc) {
                CERROR("cannot read %s: rc = %d\n", LAST_RCVD, rc);
                GOTO(err_last_rcvd, rc);
        }

        rc = mds_lov_init_objids(obd);
        if (rc != 0) {
               CERROR("cannot init lov objid rc = %d\n", rc);
               GOTO(err_client, rc );
        }

        /* open and test the check io file junk */
        file = filp_open(HEALTH_CHECK, O_RDWR | O_CREAT, 0644);
        if (IS_ERR(file)) {
                rc = PTR_ERR(file);
                CERROR("cannot open/create %s file: rc = %d\n",HEALTH_CHECK,rc);
                GOTO(err_lov_objid, rc = PTR_ERR(file));
        }
        mds->mds_health_check_filp = file;
        if (!S_ISREG(file->f_dentry->d_inode->i_mode)) {
                CERROR("%s is not a regular file!: mode = %o\n", HEALTH_CHECK,
                       file->f_dentry->d_inode->i_mode);
                GOTO(err_health_check, rc = -ENOENT);
        }
        rc = lvfs_check_io_health(obd, file);
        if (rc)
                GOTO(err_health_check, rc);
err_pop:
        pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);

        return rc;

err_health_check:
        if (mds->mds_health_check_filp &&
            filp_close(mds->mds_health_check_filp, 0))
                CERROR("can't close %s after error\n", HEALTH_CHECK);
err_lov_objid:
         mds_lov_destroy_objids(obd);
err_client:
        class_disconnect_exports(obd);
err_last_rcvd:
        if (mds->mds_rcvd_filp && filp_close(mds->mds_rcvd_filp, 0))
                CERROR("can't close %s after error\n", LAST_RCVD);
err_objects:
        dput(mds->mds_objects_dir);
err_logs:
        dput(mds->mds_logs_dir);
err_pending:
        dput(mds->mds_pending_dir);
err_fid:
        dput(mds->mds_fid_de);
        goto err_pop;
}

int mds_fs_cleanup(struct obd_device *obd)
{
        struct mds_obd *mds = &obd->u.mds;
        struct lvfs_run_ctxt saved;
        int rc = 0;

        if (obd->obd_fail)
                LCONSOLE_WARN("%s: shutting down for failover; client state "
                              "will be preserved.\n", obd->obd_name);

        class_disconnect_exports(obd); /* cleans up client info too */
        mds_server_free_data(mds);

        push_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
        if (mds->mds_rcvd_filp) {
                rc = filp_close(mds->mds_rcvd_filp, 0);
                mds->mds_rcvd_filp = NULL;
                if (rc)
                        CERROR("%s file won't close, rc=%d\n", LAST_RCVD, rc);
        }

        mds_lov_destroy_objids(obd);

        if (mds->mds_health_check_filp) {
                rc = filp_close(mds->mds_health_check_filp, 0);
                mds->mds_health_check_filp = NULL;
                if (rc)
                        CERROR("%s file won't close, rc=%d\n", HEALTH_CHECK,rc);
        }
        if (mds->mds_objects_dir != NULL) {
                l_dput(mds->mds_objects_dir);
                mds->mds_objects_dir = NULL;
        }
        if (mds->mds_logs_dir) {
                l_dput(mds->mds_logs_dir);
                mds->mds_logs_dir = NULL;
        }
        if (mds->mds_pending_dir) {
                l_dput(mds->mds_pending_dir);
                mds->mds_pending_dir = NULL;
        }

        lquota_fs_cleanup(mds_quota_interface_ref, obd);

        pop_ctxt(&saved, &obd->obd_lvfs_ctxt, NULL);
        shrink_dcache_parent(mds->mds_fid_de);
        dput(mds->mds_fid_de);
        LL_DQUOT_OFF(obd->u.obt.obt_sb);

        return rc;
}

/* Creates an object with the same name as its fid.  Because this is not at all
 * performance sensitive, it is accomplished by creating a file, checking the
 * fid, and renaming it. */
int mds_obd_create(struct obd_export *exp, struct obdo *oa,
                   struct lov_stripe_md **ea, struct obd_trans_info *oti)
{
        struct mds_obd *mds = &exp->exp_obd->u.mds;
        struct inode *parent_inode = mds->mds_objects_dir->d_inode;
        unsigned int tmpname = ll_rand();
        struct file *filp;
        struct dentry *new_child;
        struct lvfs_run_ctxt saved;
        char fidname[LL_FID_NAMELEN];
        void *handle;
        struct lvfs_ucred ucred = { 0 };
        int rc = 0, err, namelen;
        ENTRY;

        /* the owner of object file should always be root */
        cap_raise(ucred.luc_cap, CAP_SYS_RESOURCE);

        push_ctxt(&saved, &exp->exp_obd->obd_lvfs_ctxt, &ucred);

        sprintf(fidname, "OBJECTS/%u.%u", tmpname, current->pid);
        filp = filp_open(fidname, O_CREAT | O_EXCL, 0666);
        if (IS_ERR(filp)) {
                rc = PTR_ERR(filp);
                if (rc == -EEXIST) {
                        CERROR("impossible object name collision %u\n",
                               tmpname);
                        LBUG();
                }
                CERROR("error creating tmp object %u: rc %d\n", tmpname, rc);
                GOTO(out_pop, rc);
        }

        LASSERT(mds->mds_objects_dir == filp->f_dentry->d_parent);

        oa->o_id = filp->f_dentry->d_inode->i_ino;
        oa->o_generation = filp->f_dentry->d_inode->i_generation;
        namelen = ll_fid2str(fidname, oa->o_id, oa->o_generation);

        LOCK_INODE_MUTEX(parent_inode);
        new_child = lookup_one_len(fidname, mds->mds_objects_dir, namelen);

        if (IS_ERR(new_child)) {
                CERROR("getting neg dentry for obj rename: %d\n", rc);
                GOTO(out_close, rc = PTR_ERR(new_child));
        }
        if (new_child->d_inode != NULL) {
                CERROR("impossible non-negative obj dentry " LPU64":%u!\n",
                       oa->o_id, oa->o_generation);
                LBUG();
        }

        handle = fsfilt_start(exp->exp_obd, mds->mds_objects_dir->d_inode,
                              FSFILT_OP_RENAME, NULL);
        if (IS_ERR(handle))
                GOTO(out_dput, rc = PTR_ERR(handle));

        lock_kernel();
        rc = ll_vfs_rename(mds->mds_objects_dir->d_inode, filp->f_dentry,
                           filp->f_vfsmnt, mds->mds_objects_dir->d_inode, 
                           new_child, filp->f_vfsmnt);
        unlock_kernel();
        if (rc)
                CERROR("error renaming new object "LPU64":%u: rc %d\n",
                       oa->o_id, oa->o_generation, rc);

        err = fsfilt_commit(exp->exp_obd, mds->mds_objects_dir->d_inode,
                            handle, 0);
        if (!err)
                oa->o_valid |= OBD_MD_FLID | OBD_MD_FLGENER;
        else if (!rc)
                rc = err;
out_dput:
        dput(new_child);
out_close:
        UNLOCK_INODE_MUTEX(parent_inode);
        err = filp_close(filp, 0);
        if (err) {
                CERROR("closing tmpfile %u: rc %d\n", tmpname, rc);
                if (!rc)
                        rc = err;
        }
out_pop:
        pop_ctxt(&saved, &exp->exp_obd->obd_lvfs_ctxt, &ucred);
        RETURN(rc);
}

int mds_obd_destroy(struct obd_export *exp, struct obdo *oa,
                    struct lov_stripe_md *ea, struct obd_trans_info *oti,
                    struct obd_export *md_exp)
{
        struct mds_obd *mds = &exp->exp_obd->u.mds;
        struct inode *parent_inode = mds->mds_objects_dir->d_inode;
        struct obd_device *obd = exp->exp_obd;
        struct lvfs_run_ctxt saved;
        struct lvfs_ucred ucred = { 0 };
        char fidname[LL_FID_NAMELEN];
        struct inode *inode = NULL;
        struct dentry *de;
        void *handle;
        int err, namelen, rc = 0;
        ENTRY;

        cap_raise(ucred.luc_cap, CAP_SYS_RESOURCE);
        push_ctxt(&saved, &obd->obd_lvfs_ctxt, &ucred);

        namelen = ll_fid2str(fidname, oa->o_id, oa->o_generation);

        LOCK_INODE_MUTEX(parent_inode);
        de = lookup_one_len(fidname, mds->mds_objects_dir, namelen);
        if (IS_ERR(de)) {
                rc = IS_ERR(de);
                de = NULL;
                CERROR("error looking up object "LPU64" %s: rc %d\n",
                       oa->o_id, fidname, rc);
                GOTO(out_dput, rc);
        }
        if (de->d_inode == NULL) {
                CERROR("destroying non-existent object "LPU64" %s: rc %d\n",
                       oa->o_id, fidname, rc);
                GOTO(out_dput, rc = -ENOENT);
        }

        /* Stripe count is 1 here since this is some MDS specific stuff
           that is unlinked, not spanned across multiple OSTs */
        handle = fsfilt_start_log(obd, mds->mds_objects_dir->d_inode,
                                  FSFILT_OP_UNLINK, oti, 1);

        if (IS_ERR(handle))
                GOTO(out_dput, rc = PTR_ERR(handle));

        /* take a reference to protect inode from truncation within
           vfs_unlink() context. bug 10409 */
        inode = de->d_inode;
        atomic_inc(&inode->i_count);
        rc = ll_vfs_unlink(mds->mds_objects_dir->d_inode, de, mds->mds_vfsmnt);
        if (rc)
                CERROR("error destroying object "LPU64":%u: rc %d\n",
                       oa->o_id, oa->o_generation, rc);

        err = fsfilt_commit(obd, mds->mds_objects_dir->d_inode, handle, 0);
        if (err && !rc)
                rc = err;
out_dput:
        if (de != NULL)
                l_dput(de);
        UNLOCK_INODE_MUTEX(parent_inode);

        if (inode)
                iput(inode);

        pop_ctxt(&saved, &obd->obd_lvfs_ctxt, &ucred);
        RETURN(rc);
}
