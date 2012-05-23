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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/obd_mount_server.c
 *
 * Server mount routines
 *
 * Author: Nathan Rutman <nathan@clusterfs.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS
#define D_MOUNT D_SUPER|D_CONFIG /*|D_WARNING */
#define PRINT_CMD CDEBUG
#define PRINT_MASK D_SUPER|D_CONFIG

#include <obd.h>
#include <lvfs.h>
#include <lustre_fsfilt.h>
#include <obd_class.h>
#include <lustre/lustre_user.h>
#include <linux/version.h>
#include <lustre_log.h>
#include <lustre_disk.h>
#include <lustre_param.h>
#ifdef HAVE_KERNEL_LOCKED
#include <linux/smp_lock.h>
#endif

/*********** mount lookup *********/

CFS_DEFINE_MUTEX(lustre_mount_info_lock);
static CFS_LIST_HEAD(server_mount_info_list);

static struct lustre_mount_info *server_find_mount(const char *name)
{
	cfs_list_t *tmp;
	struct lustre_mount_info *lmi;
	ENTRY;

	cfs_list_for_each(tmp, &server_mount_info_list) {
		lmi = cfs_list_entry(tmp, struct lustre_mount_info,
				     lmi_list_chain);
		if (strcmp(name, lmi->lmi_name) == 0)
			RETURN(lmi);
	}
	RETURN(NULL);
}

/* we must register an obd for a mount before we call the setup routine.
   *_setup will call lustre_get_mount to get the mnt struct
   by obd_name, since we can't pass the pointer to setup. */
static int server_register_mount(const char *name, struct super_block *sb,
				 struct vfsmount *mnt)
{
	struct lustre_mount_info *lmi;
	char *name_cp;
	ENTRY;

	LASSERT(sb);

	OBD_ALLOC(lmi, sizeof(*lmi));
	if (!lmi)
		RETURN(-ENOMEM);
	OBD_ALLOC(name_cp, strlen(name) + 1);
	if (!name_cp) {
		OBD_FREE(lmi, sizeof(*lmi));
		RETURN(-ENOMEM);
	}
	strcpy(name_cp, name);

	cfs_mutex_lock(&lustre_mount_info_lock);

	if (server_find_mount(name)) {
		cfs_mutex_unlock(&lustre_mount_info_lock);
		OBD_FREE(lmi, sizeof(*lmi));
		OBD_FREE(name_cp, strlen(name) + 1);
		CERROR("Already registered %s\n", name);
		RETURN(-EEXIST);
	}
	lmi->lmi_name = name_cp;
	lmi->lmi_sb = sb;
	lmi->lmi_mnt = mnt;
	cfs_list_add(&lmi->lmi_list_chain, &server_mount_info_list);

	cfs_mutex_unlock(&lustre_mount_info_lock);

	CDEBUG(D_MOUNT, "reg_mnt %p from %s, vfscount=%d\n",
	       lmi->lmi_mnt, name,
	       lmi->lmi_mnt ? mnt_get_count(lmi->lmi_mnt) : -1);

	RETURN(0);
}

/* when an obd no longer needs a mount */
static int server_deregister_mount(const char *name)
{
	struct lustre_mount_info *lmi;
	ENTRY;

	cfs_mutex_lock(&lustre_mount_info_lock);
	lmi = server_find_mount(name);
	if (!lmi) {
		cfs_mutex_unlock(&lustre_mount_info_lock);
		CERROR("%s not registered\n", name);
		RETURN(-ENOENT);
	}

	CDEBUG(D_MOUNT, "dereg_mnt %p from %s, vfscount=%d\n",
	       lmi->lmi_mnt, name,
	       lmi->lmi_mnt ? mnt_get_count(lmi->lmi_mnt) : -1);

	OBD_FREE(lmi->lmi_name, strlen(lmi->lmi_name) + 1);
	cfs_list_del(&lmi->lmi_list_chain);
	OBD_FREE(lmi, sizeof(*lmi));
	cfs_mutex_unlock(&lustre_mount_info_lock);

	RETURN(0);
}

/* obd's look up a registered mount using their obdname. This is just
   for initial obd setup to find the mount struct.  It should not be
   called every time you want to mntget. */
struct lustre_mount_info *server_get_mount(const char *name)
{
	struct lustre_mount_info *lmi;
	struct lustre_sb_info *lsi;
	ENTRY;

	cfs_mutex_lock(&lustre_mount_info_lock);
	lmi = server_find_mount(name);
	cfs_mutex_unlock(&lustre_mount_info_lock);
	if (!lmi) {
		CERROR("Can't find mount for %s\n", name);
		RETURN(NULL);
	}
	lsi = s2lsi(lmi->lmi_sb);

	if (lmi->lmi_mnt)
		mntget(lmi->lmi_mnt);
	cfs_atomic_inc(&lsi->lsi_mounts);

	CDEBUG(D_MOUNT, "get_mnt %p from %s, refs=%d, vfscount=%d\n",
	       lmi->lmi_mnt, name, cfs_atomic_read(&lsi->lsi_mounts),
	       lmi->lmi_mnt ? mnt_get_count(lmi->lmi_mnt) - 1 : -1);

	RETURN(lmi);
}
EXPORT_SYMBOL(server_get_mount);

/*
 * Used by mdt to get mount_info from obdname.
 * There are no blocking when using the mount_info.
 * Do not use server_get_mount for this purpose.
 */
struct lustre_mount_info *server_get_mount_2(const char *name)
{
	struct lustre_mount_info *lmi;
	ENTRY;

	cfs_mutex_lock(&lustre_mount_info_lock);
	lmi = server_find_mount(name);
	cfs_mutex_unlock(&lustre_mount_info_lock);
	if (!lmi)
		CERROR("Can't find mount for %s\n", name);

	RETURN(lmi);
}
EXPORT_SYMBOL(server_get_mount_2);

static void unlock_mntput(struct vfsmount *mnt)
{
#ifdef HAVE_KERNEL_LOCKED
	/* for kernel < 2.6.37 */
	if (kernel_locked()) {
		unlock_kernel();
		mntput(mnt);
		lock_kernel();
	} else {
		mntput(mnt);
	}
#else
	mntput(mnt);
#endif
}

/* to be called from obd_cleanup methods */
int server_put_mount(const char *name, struct vfsmount *mnt)
{
	struct lustre_mount_info *lmi;
	struct lustre_sb_info *lsi;
	int count = 0;
	ENTRY;

	/* This might be the last one, can't deref after this */
	if (mnt) {
		count = mnt_get_count(mnt) - 1;
		unlock_mntput(mnt);
	}

	cfs_mutex_lock(&lustre_mount_info_lock);
	lmi = server_find_mount(name);
	cfs_mutex_unlock(&lustre_mount_info_lock);
	if (!lmi) {
		CERROR("Can't find mount for %s\n", name);
		RETURN(-ENOENT);
	}
	lsi = s2lsi(lmi->lmi_sb);
	LASSERT(lmi->lmi_mnt == mnt);

	CDEBUG(D_MOUNT, "put_mnt %p from %s, refs=%d, vfscount=%d\n",
	       lmi->lmi_mnt, name, cfs_atomic_read(&lsi->lsi_mounts), count);

	if (lustre_put_lsi(lmi->lmi_sb)) {
		CDEBUG(D_MOUNT, "Last put of mnt %p from %s, vfscount=%d\n",
		       lmi->lmi_mnt, name, count);
		/* last mount is the One True Mount */
		if (count > 1)
			CERROR("%s: mount busy, vfscount=%d!\n", name, count);
	}

	/* this obd should never need the mount again */
	server_deregister_mount(name);

	RETURN(0);
}
EXPORT_SYMBOL(server_put_mount);

/* Corresponding to server_get_mount_2 */
int server_put_mount_2(const char *name, struct vfsmount *mnt)
{
	ENTRY;
	RETURN(0);
}
EXPORT_SYMBOL(server_put_mount_2);

/* Set up a MGS to serve startup logs */
static int server_start_mgs(struct super_block *sb)
{
	struct lustre_sb_info *lsi = s2lsi(sb);
	struct vfsmount *mnt = lsi->lsi_srv_mnt;
	struct lustre_mount_info *lmi;
	int rc = 0;
	ENTRY;
	LASSERT(mnt);

	/* It is impossible to have more than 1 MGS per node, since
	   MGC wouldn't know which to connect to */
	lmi = server_find_mount(LUSTRE_MGS_OBDNAME);
	if (lmi) {
		lsi = s2lsi(lmi->lmi_sb);
		LCONSOLE_ERROR_MSG(0x15d, "The MGS service was already started"
				   " from server\n");
		RETURN(-EALREADY);
	}

	CDEBUG(D_CONFIG, "Start MGS service %s\n", LUSTRE_MGS_OBDNAME);

	rc = server_register_mount(LUSTRE_MGS_OBDNAME, sb, mnt);

	if (!rc) {
		rc = lustre_start_simple(LUSTRE_MGS_OBDNAME, LUSTRE_MGS_NAME,
					 LUSTRE_MGS_OBDNAME, 0, 0);
		/* Do NOT call server_deregister_mount() here. This leads to
		 * inability cleanup cleanly and free lsi and other stuff when
		 * mgs calls server_put_mount() in error handling case. -umka */
	}

	if (rc)
		LCONSOLE_ERROR_MSG(0x15e, "Failed to start MGS '%s' (%d). "
				   "Is the 'mgs' module loaded?\n",
				   LUSTRE_MGS_OBDNAME, rc);
	RETURN(rc);
}

static int server_stop_mgs(struct super_block *sb)
{
	struct obd_device *obd;
	int rc;
	ENTRY;

	CDEBUG(D_MOUNT, "Stop MGS service %s\n", LUSTRE_MGS_OBDNAME);

	/* There better be only one MGS */
	obd = class_name2obd(LUSTRE_MGS_OBDNAME);
	if (!obd) {
		CDEBUG(D_CONFIG, "mgs %s not running\n", LUSTRE_MGS_OBDNAME);
		RETURN(-EALREADY);
	}

	/* The MGS should always stop when we say so */
	obd->obd_force = 1;
	rc = class_manual_cleanup(obd);
	RETURN(rc);
}

/* Since there's only one mgc per node, we have to change it's fs to get
   access to the right disk. */
static int server_mgc_set_fs(struct obd_device *mgc, struct super_block *sb)
{
	struct lustre_sb_info *lsi = s2lsi(sb);
	int rc;
	ENTRY;

	CDEBUG(D_MOUNT, "Set mgc disk for %s\n", lsi->lsi_lmd->lmd_dev);

	/* cl_mgc_sem in mgc insures we sleep if the mgc_fs is busy */
	rc = obd_set_info_async(NULL, mgc->obd_self_export,
				sizeof(KEY_SET_FS), KEY_SET_FS,
				sizeof(*sb), sb, NULL);
	if (rc)
		CERROR("can't set_fs %d\n", rc);

	RETURN(rc);
}

static int server_mgc_clear_fs(struct obd_device *mgc)
{
	int rc;
	ENTRY;

	CDEBUG(D_MOUNT, "Unassign mgc disk\n");

	rc = obd_set_info_async(NULL, mgc->obd_self_export,
				sizeof(KEY_CLEAR_FS), KEY_CLEAR_FS,
				0, NULL, NULL);
	RETURN(rc);
}

CFS_DEFINE_MUTEX(server_start_lock);

/* Stop MDS/OSS if nobody is using them */
static int server_stop_servers(int lsiflags)
{
	struct obd_device *obd = NULL;
	struct obd_type *type = NULL;
	int rc = 0;
	ENTRY;

	cfs_mutex_lock(&server_start_lock);

	/* Either an MDT or an OST or neither  */
	/* if this was an MDT, and there are no more MDT's, clean up the MDS */
	if ((lsiflags & LDD_F_SV_TYPE_MDT) &&
	    (obd = class_name2obd(LUSTRE_MDS_OBDNAME))) {
		/*FIXME pre-rename, should eventually be LUSTRE_MDT_NAME */
		type = class_search_type(LUSTRE_MDS_NAME);
	}
	/* if this was an OST, and there are no more OST's, clean up the OSS */
	if ((lsiflags & LDD_F_SV_TYPE_OST) &&
	    (obd = class_name2obd(LUSTRE_OSS_OBDNAME))) {
		type = class_search_type(LUSTRE_OST_NAME);
	}

	if (obd && (!type || !type->typ_refcnt)) {
		int err;
		obd->obd_force = 1;
		/* obd_fail doesn't mean much on a server obd */
		err = class_manual_cleanup(obd);
		if (!rc)
			rc = err;
	}

	cfs_mutex_unlock(&server_start_lock);

	RETURN(rc);
}

int server_mti_print(char *title, struct mgs_target_info *mti)
{
	PRINT_CMD(PRINT_MASK, "mti %s\n", title);
	PRINT_CMD(PRINT_MASK, "server: %s\n", mti->mti_svname);
	PRINT_CMD(PRINT_MASK, "fs:     %s\n", mti->mti_fsname);
	PRINT_CMD(PRINT_MASK, "uuid:   %s\n", mti->mti_uuid);
	PRINT_CMD(PRINT_MASK, "ver: %d  flags: %#x\n",
		  mti->mti_config_ver, mti->mti_flags);
	return 0;
}
EXPORT_SYMBOL(server_mti_print);

/* Generate data for registration */
static int server_lsi2mti(struct lustre_sb_info *lsi,
			  struct mgs_target_info *mti)
{
	lnet_process_id_t id;
	int rc, i = 0;
	ENTRY;

	if (!IS_SERVER(lsi))
		RETURN(-EINVAL);

	strncpy(mti->mti_svname, lsi->lsi_svname, sizeof(mti->mti_svname));

        mti->mti_nid_count = 0;
        while (LNetGetId(i++, &id) != -ENOENT) {
                if (LNET_NETTYP(LNET_NIDNET(id.nid)) == LOLND)
                        continue;

                /* server use --servicenode param, only allow specified
                 * nids be registered */
		if ((lsi->lsi_lmd->lmd_flags & LMD_FLG_NO_PRIMNODE) != 0 &&
		    class_match_nid(lsi->lsi_lmd->lmd_params,
                                    PARAM_FAILNODE, id.nid) < 1)
                        continue;

                /* match specified network */
		if (!class_match_net(lsi->lsi_lmd->lmd_params,
                                     PARAM_NETWORK, LNET_NIDNET(id.nid)))
                        continue;

                mti->mti_nids[mti->mti_nid_count] = id.nid;
                mti->mti_nid_count++;
                if (mti->mti_nid_count >= MTI_NIDS_MAX) {
                        CWARN("Only using first %d nids for %s\n",
                              mti->mti_nid_count, mti->mti_svname);
                        break;
                }
        }

        mti->mti_lustre_ver = LUSTRE_VERSION_CODE;
        mti->mti_config_ver = 0;

	rc = server_name2fsname(lsi->lsi_svname, mti->mti_fsname, NULL);
	if (rc != 0)
		return rc;

	rc = server_name2index(lsi->lsi_svname, &mti->mti_stripe_index, NULL);
	if (rc < 0)
		return rc;
	/* Orion requires index to be set */
	LASSERT(!(rc & LDD_F_NEED_INDEX));
	/* keep only LDD flags */
	mti->mti_flags = lsi->lsi_flags & LDD_F_MASK;
	mti->mti_flags |= LDD_F_UPDATE;
	strncpy(mti->mti_params, lsi->lsi_lmd->lmd_params,
		sizeof(mti->mti_params));
	return 0;
}

/* Register an old or new target with the MGS. If needed MGS will construct
   startup logs and assign index */
static int server_register_target(struct lustre_sb_info *lsi)
{
        struct obd_device *mgc = lsi->lsi_mgc;
        struct mgs_target_info *mti = NULL;
        bool writeconf;
        int rc;
        ENTRY;

        LASSERT(mgc);

	if (!IS_SERVER(lsi))
                RETURN(-EINVAL);

        OBD_ALLOC_PTR(mti);
        if (!mti)
                RETURN(-ENOMEM);

	rc = server_lsi2mti(lsi, mti);
        if (rc)
                GOTO(out, rc);

        CDEBUG(D_MOUNT, "Registration %s, fs=%s, %s, index=%04x, flags=%#x\n",
               mti->mti_svname, mti->mti_fsname,
               libcfs_nid2str(mti->mti_nids[0]), mti->mti_stripe_index,
               mti->mti_flags);

        /* if write_conf is true, the registration must succeed */
	writeconf = !!(lsi->lsi_flags & (LDD_F_NEED_INDEX | LDD_F_UPDATE));
        mti->mti_flags |= LDD_F_OPC_REG;

        /* Register the target */
        /* FIXME use mgc_process_config instead */
        rc = obd_set_info_async(NULL, mgc->u.cli.cl_mgc_mgsexp,
                                sizeof(KEY_REGISTER_TARGET), KEY_REGISTER_TARGET,
                                sizeof(*mti), mti, NULL);
        if (rc) {
                if (mti->mti_flags & LDD_F_ERROR) {
                        LCONSOLE_ERROR_MSG(0x160,
                                "The MGS is refusing to allow this "
                                "server (%s) to start. Please see messages"
				" on the MGS node.\n", lsi->lsi_svname);
                } else if (writeconf) {
                        LCONSOLE_ERROR_MSG(0x15f,
                                "Communication to the MGS return error %d. "
                                "Is the MGS running?\n", rc);
                } else {
                        CERROR("Cannot talk to the MGS: %d, not fatal\n", rc);
                        /* reset the error code for non-fatal error. */
                        rc = 0;
                }
                GOTO(out, rc);
        }

out:
        if (mti)
                OBD_FREE_PTR(mti);
        RETURN(rc);
}

/**
 * Notify the MGS that this target is ready.
 * Used by IR - if the MGS receives this message, it will notify clients.
 */
static int server_notify_target(struct super_block *sb, struct obd_device *obd)
{
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct obd_device *mgc = lsi->lsi_mgc;
        struct mgs_target_info *mti = NULL;
        int rc;
        ENTRY;

        LASSERT(mgc);

	if (!(IS_SERVER(lsi)))
                RETURN(-EINVAL);

        OBD_ALLOC_PTR(mti);
        if (!mti)
                RETURN(-ENOMEM);
	rc = server_lsi2mti(lsi, mti);
        if (rc)
                GOTO(out, rc);

        mti->mti_instance = obd->u.obt.obt_instance;
        mti->mti_flags |= LDD_F_OPC_READY;

        /* FIXME use mgc_process_config instead */
        rc = obd_set_info_async(NULL, mgc->u.cli.cl_mgc_mgsexp,
                                sizeof(KEY_REGISTER_TARGET),
                                KEY_REGISTER_TARGET,
                                sizeof(*mti), mti, NULL);

        /* Imperative recovery: if the mgs informs us to use IR? */
        if (!rc && !(mti->mti_flags & LDD_F_ERROR) &&
            (mti->mti_flags & LDD_F_IR_CAPABLE))
		lsi->lsi_flags |= LDD_F_IR_CAPABLE;

out:
        if (mti)
                OBD_FREE_PTR(mti);
        RETURN(rc);

}

/** Start server targets: MDTs and OSTs
 */
static int server_start_targets(struct super_block *sb, struct vfsmount *mnt)
{
        struct obd_device *obd;
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct config_llog_instance cfg;
        int rc;
        ENTRY;

	CDEBUG(D_MOUNT, "starting target %s\n", lsi->lsi_svname);

#if 0
        /* If we're an MDT, make sure the global MDS is running */
        if (lsi->lsi_ldd->ldd_flags & LDD_F_SV_TYPE_MDT) {
                /* make sure the MDS is started */
                cfs_mutex_lock(&server_start_lock);
                obd = class_name2obd(LUSTRE_MDS_OBDNAME);
                if (!obd) {
                        rc = lustre_start_simple(LUSTRE_MDS_OBDNAME,
                    /* FIXME pre-rename, should eventually be LUSTRE_MDS_NAME */
                                                 LUSTRE_MDT_NAME,
                                                 LUSTRE_MDS_OBDNAME"_uuid",
                                                 0, 0);
                        if (rc) {
                                cfs_mutex_unlock(&server_start_lock);
                                CERROR("failed to start MDS: %d\n", rc);
                                RETURN(rc);
                        }
                }
                cfs_mutex_unlock(&server_start_lock);
        }
#endif

        /* If we're an OST, make sure the global OSS is running */
	if (IS_OST(lsi)) {
                /* make sure OSS is started */
                cfs_mutex_lock(&server_start_lock);
                obd = class_name2obd(LUSTRE_OSS_OBDNAME);
                if (!obd) {
                        rc = lustre_start_simple(LUSTRE_OSS_OBDNAME,
                                                 LUSTRE_OSS_NAME,
                                                 LUSTRE_OSS_OBDNAME"_uuid",
                                                 0, 0);
                        if (rc) {
                                cfs_mutex_unlock(&server_start_lock);
                                CERROR("failed to start OSS: %d\n", rc);
                                RETURN(rc);
                        }
                }
                cfs_mutex_unlock(&server_start_lock);
        }

        /* Set the mgc fs to our server disk.  This allows the MGC to
         * read and write configs locally, in case it can't talk to the MGS. */
	if (lsi->lsi_srv_mnt) {
		rc = server_mgc_set_fs(lsi->lsi_mgc, sb);
		if (rc)
			RETURN(rc);
	}

        /* Register with MGS */
	rc = server_register_target(lsi);
        if (rc)
                GOTO(out_mgc, rc);

        /* Let the target look up the mount using the target's name
           (we can't pass the sb or mnt through class_process_config.) */
	rc = server_register_mount(lsi->lsi_svname, sb, mnt);
        if (rc)
                GOTO(out_mgc, rc);

	/* Start targets using the llog named for the target */
	memset(&cfg, 0, sizeof(cfg));
	rc = lustre_process_log(sb, lsi->lsi_svname, &cfg);
	if (rc) {
		CERROR("failed to start server %s: %d\n",
		       lsi->lsi_svname, rc);
		/* Do NOT call server_deregister_mount() here. This makes it
		 * impossible to find mount later in cleanup time and leaves
		 * @lsi and othder stuff leaked. -umka */
		GOTO(out_mgc, rc);
	}

out_mgc:
        /* Release the mgc fs for others to use */
	if (lsi->lsi_srv_mnt)
		server_mgc_clear_fs(lsi->lsi_mgc);

        if (!rc) {
		obd = class_name2obd(lsi->lsi_svname);
                if (!obd) {
                        CERROR("no server named %s was started\n",
			       lsi->lsi_svname);
                        RETURN(-ENXIO);
                }

                if ((lsi->lsi_lmd->lmd_flags & LMD_FLG_ABORT_RECOV) &&
                    (OBP(obd, iocontrol))) {
                        obd_iocontrol(OBD_IOC_ABORT_RECOVERY,
                                      obd->obd_self_export, 0, NULL, NULL);
                }

                server_notify_target(sb, obd);

                /* calculate recovery timeout, do it after lustre_process_log */
                server_calc_timeout(lsi, obd);

                /* log has been fully processed */
                obd_notify(obd, NULL, OBD_NOTIFY_CONFIG, (void *)CONFIG_LOG);
        }

        RETURN(rc);
}

/*************** server mount ******************/

static int lsi_prepare(struct lustre_sb_info *lsi)
{
	__u32 index;
	int rc;
	ENTRY;

	LASSERT(lsi);
	LASSERT(lsi->lsi_lmd);

	/* The server name is given as a mount line option */
	if (lsi->lsi_lmd->lmd_profile == NULL) {
		LCONSOLE_ERROR("Can't determine server name\n");
		RETURN(-EINVAL);
	}

	if (strlen(lsi->lsi_lmd->lmd_profile) >= sizeof(lsi->lsi_svname))
		RETURN(-ENAMETOOLONG);

	strcpy(lsi->lsi_svname, lsi->lsi_lmd->lmd_profile);

	/* Determine osd type */
	if (lsi->lsi_lmd->lmd_osd_type != NULL) {
		if (strlen(lsi->lsi_lmd->lmd_osd_type) >=
			   sizeof(lsi->lsi_osd_type))
			RETURN(-ENAMETOOLONG);

		strcpy(lsi->lsi_osd_type, lsi->lsi_lmd->lmd_osd_type);
	} else {
		strcpy(lsi->lsi_osd_type, LUSTRE_OSD_NAME);
	}

	/* XXX: a temp. solution for components using fsfilt
	 *      to be removed in one of the subsequent patches */
	if (!strcmp(lsi->lsi_lmd->lmd_osd_type, "osd-ldiskfs")) {
		strcpy(lsi->lsi_fstype, "ldiskfs");
	} else {
		strcpy(lsi->lsi_fstype, lsi->lsi_lmd->lmd_osd_type);
	}

	/* Determine server type */
	rc = server_name2index(lsi->lsi_svname, &index, NULL);
	if (rc < 0) {
		if (lsi->lsi_lmd->lmd_flags & LMD_FLG_MGS) {
			/* Assume we're a bare MGS */
			rc = 0;
			lsi->lsi_lmd->lmd_flags |= LMD_FLG_NOSVC;
		} else {
			LCONSOLE_ERROR("Can't determine server type of '%s'\n",
				       lsi->lsi_svname);
			RETURN(rc);
		}
	}
	lsi->lsi_flags |= rc;

	/* Add mount line flags that used to be in ldd:
	 * writeconf, mgs, iam, anything else?
	 */
	lsi->lsi_flags |= (lsi->lsi_lmd->lmd_flags & LMD_FLG_WRITECONF) ?
		LDD_F_WRITECONF : 0;
	lsi->lsi_flags |= (lsi->lsi_lmd->lmd_flags & LMD_FLG_VIRGIN) ?
		LDD_F_VIRGIN : 0;
	lsi->lsi_flags |= (lsi->lsi_lmd->lmd_flags & LMD_FLG_MGS) ?
		LDD_F_SV_TYPE_MGS : 0;
	lsi->lsi_flags |= (lsi->lsi_lmd->lmd_flags & LMD_FLG_IAM) ?
		LDD_F_IAM_DIR : 0;
	lsi->lsi_flags |= (lsi->lsi_lmd->lmd_flags & LMD_FLG_NO_PRIMNODE) ?
		LDD_F_NO_PRIMNODE : 0;

	RETURN(0);
}

/** Kernel mount using mount options in MOUNT_DATA_FILE.
 * Since this file lives on the disk, we pre-mount using a common
 * type, read the file, then re-mount using the type specified in the
 * file.
 */
static struct vfsmount *server_kernel_mount(struct super_block *sb)
{
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct lustre_mount_data *lmd = lsi->lsi_lmd;
        struct vfsmount *mnt;
        struct file_system_type *type;
        char *options = NULL;
        unsigned long page, s_flags;
        struct page *__page;
        int len;
        int rc;
        ENTRY;

	rc = lsi_prepare(lsi);
	if (rc)
		RETURN(ERR_PTR(rc));

	if (strcmp(lmd->lmd_osd_type, "osd-ldiskfs") == 0) {
		/* with ldiskfs we're still mounting in the kernel space */
		OBD_FREE(lmd->lmd_osd_type,
			 strlen(lmd->lmd_osd_type) + 1);
		lmd->lmd_osd_type = NULL;
	} else {
		/* non-ldiskfs backends (zfs) do mounting internally */
		RETURN(NULL);
	}

        /* In the past, we have always used flags = 0.
           Note ext3/ldiskfs can't be mounted ro. */
        s_flags = sb->s_flags;

        /* allocate memory for options */
        OBD_PAGE_ALLOC(__page, CFS_ALLOC_STD);
        if (!__page)
                GOTO(out_free, rc = -ENOMEM);
        page = (unsigned long)cfs_page_address(__page);
        options = (char *)page;
        memset(options, 0, CFS_PAGE_SIZE);

        /* Glom up mount options */
        memset(options, 0, CFS_PAGE_SIZE);
	strncpy(options, lsi->lsi_lmd->lmd_opts, CFS_PAGE_SIZE - 2);

        len = CFS_PAGE_SIZE - strlen(options) - 2;
        if (*options != 0)
                strcat(options, ",");
        strncat(options, "no_mbcache", len);

        /* Add in any mount-line options */
        if (lmd->lmd_opts && (*(lmd->lmd_opts) != 0)) {
                len = CFS_PAGE_SIZE - strlen(options) - 2;
                strcat(options, ",");
                strncat(options, lmd->lmd_opts, len);
        }

        /* Special permanent mount flags */
	if (IS_OST(lsi))
            s_flags |= MS_NOATIME | MS_NODIRATIME;

        CDEBUG(D_MOUNT, "kern_mount: %s %s %s\n",
	       lsi->lsi_osd_type, lmd->lmd_dev, options);
	type = get_fs_type(lsi->lsi_fstype);
        if (!type) {
                CERROR("get_fs_type failed\n");
                GOTO(out_free, rc = -ENODEV);
        }
        mnt = vfs_kern_mount(type, s_flags, lmd->lmd_dev, (void *)options);
        cfs_module_put(type->owner);
        if (IS_ERR(mnt)) {
                rc = PTR_ERR(mnt);
                CERROR("vfs_kern_mount failed: rc = %d\n", rc);
                GOTO(out_free, rc);
        }

        if (lmd->lmd_flags & LMD_FLG_ABORT_RECOV)
                simple_truncate(mnt->mnt_sb->s_root, mnt, LAST_RCVD,
                                LR_CLIENT_START);

        OBD_PAGE_FREE(__page);
        CDEBUG(D_SUPER, "%s: mnt = %p\n", lmd->lmd_dev, mnt);
        RETURN(mnt);

out_free:
        if (__page)
                OBD_PAGE_FREE(__page);
        RETURN(ERR_PTR(rc));
}

/** Wait here forever until the mount refcount is 0 before completing umount,
 * else we risk dereferencing a null pointer.
 * LNET may take e.g. 165s before killing zombies.
 */
static void server_wait_finished(struct vfsmount *mnt)
{
	cfs_waitq_t waitq;
	int rc, waited = 0;
	cfs_sigset_t blocked;

	if (mnt == NULL) {
		cfs_waitq_init(&waitq);
		cfs_waitq_wait_event_interruptible_timeout(waitq, 0,
							   cfs_time_seconds(3),
							   rc);
		return;
	}

	LASSERT(mnt);
	cfs_waitq_init(&waitq);

	while (mnt_get_count(mnt) > 1) {
		if (waited && (waited % 30 == 0))
			LCONSOLE_WARN("Mount still busy with %d refs after "
				      "%d secs.\n", mnt_get_count(mnt), waited);
		/* Cannot use l_event_wait() for an interruptible sleep. */
		waited += 3;
		blocked = cfs_block_sigsinv(sigmask(SIGKILL));
		cfs_waitq_wait_event_interruptible_timeout(waitq,
							   (mnt_get_count(mnt)
							    == 1),
							   cfs_time_seconds(3),
							   rc);
		cfs_restore_sigs(blocked);
		if (rc < 0) {
			LCONSOLE_EMERG("Danger: interrupted umount %s with "
				       "%d refs!\n", mnt_get_devname(mnt),
				       mnt_get_count(mnt));
			break;
		}

	}
}

/** Start the shutdown of servers at umount.
 */
static void server_put_super(struct super_block *sb)
{
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct obd_device     *obd;
        struct vfsmount       *mnt = lsi->lsi_srv_mnt;
        char *tmpname, *extraname = NULL;
        int tmpname_sz;
        int lsiflags = lsi->lsi_flags;
        ENTRY;

	LASSERT(IS_SERVER(lsi));

	tmpname_sz = strlen(lsi->lsi_svname) + 1;
        OBD_ALLOC(tmpname, tmpname_sz);
	memcpy(tmpname, lsi->lsi_svname, tmpname_sz);
        CDEBUG(D_MOUNT, "server put_super %s\n", tmpname);
	if (IS_MDT(lsi) && (lsi->lsi_lmd->lmd_flags & LMD_FLG_NOSVC))
                snprintf(tmpname, tmpname_sz, "MGS");

        /* Stop the target */
        if (!(lsi->lsi_lmd->lmd_flags & LMD_FLG_NOSVC) &&
	    (IS_MDT(lsi) || IS_OST(lsi))) {
                struct lustre_profile *lprof = NULL;

                /* tell the mgc to drop the config log */
		lustre_end_log(sb, lsi->lsi_svname, NULL);

                /* COMPAT_146 - profile may get deleted in mgc_cleanup.
                   If there are any setup/cleanup errors, save the lov
                   name for safety cleanup later. */
		lprof = class_get_profile(lsi->lsi_svname);
                if (lprof && lprof->lp_dt) {
                        OBD_ALLOC(extraname, strlen(lprof->lp_dt) + 1);
                        strcpy(extraname, lprof->lp_dt);
                }

		obd = class_name2obd(lsi->lsi_svname);
                if (obd) {
                        CDEBUG(D_MOUNT, "stopping %s\n", obd->obd_name);
                        if (lsi->lsi_flags & LSI_UMOUNT_FAILOVER)
                                obd->obd_fail = 1;
                        /* We can't seem to give an error return code
                         * to .put_super, so we better make sure we clean up! */
                        obd->obd_force = 1;
                        class_manual_cleanup(obd);
                } else {
			CERROR("no obd %s\n", lsi->lsi_svname);
			server_deregister_mount(lsi->lsi_svname);
                }
        }

        /* If they wanted the mgs to stop separately from the mdt, they
           should have put it on a different device. */
	if (IS_MGS(lsi)) {
                /* if MDS start with --nomgs, don't stop MGS then */
                if (!(lsi->lsi_lmd->lmd_flags & LMD_FLG_NOMGS))
                        server_stop_mgs(sb);
        }

        /* Clean the mgc and sb */
        lustre_common_put_super(sb);

        /* Wait for the targets to really clean up - can't exit (and let the
           sb get destroyed) while the mount is still in use */
        server_wait_finished(mnt);

        /* drop the One True Mount */
	if (mnt)
		unlock_mntput(mnt);

	/* Stop the servers (MDS, OSS) if no longer needed.  We must wait
	   until the target is really gone so that our type refcount check
	   is right. */
	server_stop_servers(lsiflags);

	/* In case of startup or cleanup err, stop related obds */
	if (extraname) {
		obd = class_name2obd(extraname);
                if (obd) {
                        CWARN("Cleaning orphaned obd %s\n", extraname);
                        obd->obd_force = 1;
                        class_manual_cleanup(obd);
                }
                OBD_FREE(extraname, strlen(extraname) + 1);
        }

        LCONSOLE_WARN("server umount %s complete\n", tmpname);
        OBD_FREE(tmpname, tmpname_sz);
        EXIT;
}

/** Called only for 'umount -f'
 */
#ifdef HAVE_UMOUNTBEGIN_VFSMOUNT
static void server_umount_begin(struct vfsmount *vfsmnt, int flags)
{
	struct super_block *sb = vfsmnt->mnt_sb;
#else
static void server_umount_begin(struct super_block *sb)
{
#endif
	struct lustre_sb_info *lsi = s2lsi(sb);
	ENTRY;

#ifdef HAVE_UMOUNTBEGIN_VFSMOUNT
	if (!(flags & MNT_FORCE)) {
		EXIT;
		return;
	}
#endif

	CDEBUG(D_MOUNT, "umount -f\n");
	/* umount = failover
	   umount -f = force
	   no third way to do non-force, non-failover */
	lsi->lsi_flags &= ~LSI_UMOUNT_FAILOVER;
	EXIT;
}

static int server_statfs(struct dentry *dentry, cfs_kstatfs_t *buf)
{
	struct super_block *sb = dentry->d_sb;
	struct vfsmount *mnt = s2lsi(sb)->lsi_srv_mnt;
	ENTRY;

	if (mnt && mnt->mnt_sb && mnt->mnt_sb->s_op->statfs) {
		int rc = mnt->mnt_sb->s_op->statfs(mnt->mnt_root, buf);
		if (!rc) {
			buf->f_type = sb->s_magic;
			RETURN(0);
		}
	}

	/* just return 0 */
	buf->f_type = sb->s_magic;
	buf->f_bsize = sb->s_blocksize;
	buf->f_blocks = 1;
	buf->f_bfree = 0;
	buf->f_bavail = 0;
	buf->f_files = 1;
	buf->f_ffree = 0;
	buf->f_namelen = NAME_MAX;
	RETURN(0);
}

/** The operations we support directly on the superblock:
 * mount, umount, and df.
 */
static struct super_operations server_ops = {
	.put_super = server_put_super,
	.umount_begin = server_umount_begin,	/* umount -f */
	.statfs = server_statfs,
};

#define log2(n) cfs_ffz(~(n))
#define LUSTRE_SUPER_MAGIC 0x0BD00BD1

static int server_fill_super_common(struct super_block *sb)
{
	struct inode *root = 0;
	ENTRY;

	CDEBUG(D_MOUNT, "Server sb, dev=%d\n", (int) sb->s_dev);

	sb->s_blocksize = 4096;
	sb->s_blocksize_bits = log2(sb->s_blocksize);
	sb->s_magic = LUSTRE_SUPER_MAGIC;
	sb->s_maxbytes = 0;	/* don't allow file IO on server mountpoints */
	sb->s_flags |= MS_RDONLY;
	sb->s_op = &server_ops;

	root = new_inode(sb);
	if (!root) {
		CERROR("Can't make root inode\n");
		RETURN(-EIO);
	}

	/* returns -EIO for every operation */
	/* make_bad_inode(root); -- badness - can't umount */
	/* apparently we need to be a directory for the mount to finish */
	root->i_mode = S_IFDIR;

	sb->s_root = d_alloc_root(root);
	if (!sb->s_root) {
		CERROR("Can't make root dentry\n");
		iput(root);
		RETURN(-EIO);
	}

	RETURN(0);
}

/** Fill in the superblock info for a Lustre server.
 * Mount the device with the correct options.
 * Read the on-disk config file.
 * Start the services.
 */
int server_fill_super(struct super_block *sb)
{
        struct lustre_sb_info *lsi = s2lsi(sb);
        struct vfsmount *mnt;
        int rc;
        ENTRY;

        /* the One True Mount */
        mnt = server_kernel_mount(sb);
        if (IS_ERR(mnt)) {
                rc = PTR_ERR(mnt);
                CERROR("Unable to mount device %s: %d\n",
                       lsi->lsi_lmd->lmd_dev, rc);
                lustre_put_lsi(sb);
                RETURN(rc);
        }
        lsi->lsi_srv_mnt = mnt;

	CDEBUG(D_MOUNT, "Found service %s on device %s\n",
	       lsi->lsi_svname, lsi->lsi_lmd->lmd_dev);

	if (class_name2obd(lsi->lsi_svname)) {
                LCONSOLE_ERROR_MSG(0x161, "The target named %s is already "
                                   "running. Double-mount may have compromised"
                                   " the disk journal.\n",
				   lsi->lsi_svname);
                lustre_put_lsi(sb);
                unlock_mntput(mnt);
                RETURN(-EALREADY);
        }

        /* Start MGS before MGC */
	if (IS_MGS(lsi) && !(lsi->lsi_lmd->lmd_flags & LMD_FLG_NOMGS)){
                rc = server_start_mgs(sb);
                if (rc)
                        GOTO(out_mnt, rc);
        }

        /* Start MGC before servers */
        rc = lustre_start_mgc(sb);
        if (rc)
                GOTO(out_mnt, rc);

        /* Set up all obd devices for service */
        if (!(lsi->lsi_lmd->lmd_flags & LMD_FLG_NOSVC) &&
			(IS_OST(lsi) || IS_MDT(lsi))) {
                rc = server_start_targets(sb, mnt);
                if (rc < 0) {
                        CERROR("Unable to start targets: %d\n", rc);
                        GOTO(out_mnt, rc);
                }
        /* FIXME overmount client here,
           or can we just start a client log and client_fill_super on this sb?
           We need to make sure server_put_super gets called too - ll_put_super
           calls lustre_common_put_super; check there for LSI_SERVER flag,
           call s_p_s if so.
           Probably should start client from new thread so we can return.
           Client will not finish until all servers are connected.
           Note - MGS-only server does NOT get a client, since there is no
           lustre fs associated - the MGS is for all lustre fs's */
        }

        rc = server_fill_super_common(sb);
        if (rc)
                GOTO(out_mnt, rc);

        RETURN(0);
out_mnt:
        /* We jump here in case of failure while starting targets or MGS.
         * In this case we can't just put @mnt and have to do real cleanup
         * with stoping targets, etc. */
        server_put_super(sb);
        return rc;
}

/*
 * Calculate timeout value for a target.
 */
void server_calc_timeout(struct lustre_sb_info *lsi, struct obd_device *obd)
{
        struct lustre_mount_data *lmd;
        int soft = 0;
        int hard = 0;
        int factor = 0;
	bool has_ir = !!(lsi->lsi_flags & LDD_F_IR_CAPABLE);
        int min = OBD_RECOVERY_TIME_MIN;

	LASSERT(IS_SERVER(lsi));

        lmd = lsi->lsi_lmd;
        if (lmd) {
                soft   = lmd->lmd_recovery_time_soft;
                hard   = lmd->lmd_recovery_time_hard;
                has_ir = has_ir && !(lmd->lmd_flags & LMD_FLG_NOIR);
                obd->obd_no_ir = !has_ir;
        }

        if (soft == 0)
                soft = OBD_RECOVERY_TIME_SOFT;
        if (hard == 0)
                hard = OBD_RECOVERY_TIME_HARD;

        /* target may have ir_factor configured. */
        factor = OBD_IR_FACTOR_DEFAULT;
        if (obd->obd_recovery_ir_factor)
                factor = obd->obd_recovery_ir_factor;

        if (has_ir) {
                int new_soft = soft;
                int new_hard = hard;

                /* adjust timeout value by imperative recovery */

                new_soft = (soft * factor) / OBD_IR_FACTOR_MAX;
                new_hard = (hard * factor) / OBD_IR_FACTOR_MAX;

                /* make sure the timeout is not too short */
                new_soft = max(min, new_soft);
                new_hard = max(new_soft, new_hard);

                LCONSOLE_INFO("%s: Imperative Recovery enabled, recovery "
                              "window shrunk from %d-%d down to %d-%d\n",
                              obd->obd_name, soft, hard, new_soft, new_hard);

                soft = new_soft;
                hard = new_hard;
        }

        /* we're done */
        obd->obd_recovery_timeout   = max(obd->obd_recovery_timeout, soft);
        obd->obd_recovery_time_hard = hard;
        obd->obd_recovery_ir_factor = factor;
}
EXPORT_SYMBOL(server_calc_timeout);
