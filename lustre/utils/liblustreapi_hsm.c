/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * lustre/utils/liblustreapi_hsm.c
 *
 * lustreapi library for hsm calls
 *
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Author: JC Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 * Author: Thomas Leibovici <thomas.leibovici@cea.fr>
 * Author: Henri Doreau <henri.doreau@cea.fr>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <malloc.h>
#include <errno.h>
#include <dirent.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#include <sys/syscall.h>
#include <fnmatch.h>
#include <glob.h>
#ifdef HAVE_LINUX_UNISTD_H
#include <linux/unistd.h>
#else
#include <unistd.h>
#endif

#include <liblustre.h>
#include <lnet/lnetctl.h>
#include <obd.h>
#include <obd_lov.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

/****** HSM Copytool API ********/
#define CT_PRIV_MAGIC 0xC0BE2001
struct hsm_copytool_private {
	int			 magic;
	char			*mnt;
	int			 mnt_fd;
	lustre_kernelcomm	 kuc;
	__u32			 archives;
};

#define CP_PRIV_MAGIC 0x19880429
struct hsm_copyaction_private {
	__u32		magic;
	__s32		mnt_fd;
	__s32		data_fd;
	struct hsm_copy	copy;
};

#include <libcfs/libcfs.h>

/** Register a copytool
 * \param[out] priv Opaque private control structure
 * \param mnt Lustre filesystem mount point
 * \param flags Open flags, currently unused (e.g. O_NONBLOCK)
 * \param archive_count
 * \param archives Which archive numbers this copytool is responsible for
 */
int llapi_hsm_copytool_register(struct hsm_copytool_private **priv,
				const char *mnt, int flags, int archive_count,
				int *archives)
{
	struct hsm_copytool_private	*ct;
	int				 rc;

	if (archive_count > 0 && archives == NULL) {
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "NULL archive numbers");
		return -EINVAL;
	}

	ct = calloc(1, sizeof(*ct));
	if (ct == NULL)
		return -ENOMEM;

	ct->mnt_fd = open(mnt, O_DIRECTORY | O_RDONLY | O_NONBLOCK);
	if (ct->mnt_fd == -1) {
		rc = -errno;
		goto out_err;
	}

	ct->mnt = strdup(mnt);
	if (ct->mnt == NULL) {
		rc = -ENOMEM;
		goto out_err;
	}

	ct->magic = CT_PRIV_MAGIC;

	/* no archives specified means "match all". */
	ct->archives = 0;
	for (rc = 0; rc < archive_count; rc++) {
		if (archives[rc] > 8 * sizeof(ct->archives)) {
			llapi_err_noerrno(LLAPI_MSG_ERROR,
					  "Maximum of %d archives supported",
					  8 * sizeof(ct->archives));
			goto out_err;
		}
		/* in the list we have a all archive wildcard
		 * so move to all archives mode
		 */
		if (archives[rc] == 0) {
			ct->archives = 0;
			archive_count = 0;
			break;
		}
		ct->archives |= (1 << (archives[rc] - 1));
	}

	rc = libcfs_ukuc_start(&ct->kuc, KUC_GRP_HSM);
	if (rc < 0)
		goto out_err;

	/* Storing archive(s) in lk_data; see mdc_ioc_hsm_ct_start */
	ct->kuc.lk_data = ct->archives;
	rc = ioctl(ct->mnt_fd, LL_IOC_HSM_CT_START, &(ct->kuc));
	if (rc == -1) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "ioctl %d err %d",
			    LL_IOC_HSM_CT_START, rc);
		goto out_err;
	} else
		rc = 0;

	/* Only the kernel reference keeps the write side open */
	close(ct->kuc.lk_wfd);
	ct->kuc.lk_wfd = 0;
	if (rc < 0)
		goto out_kuc;

	*priv = ct;
	return 0;

out_kuc:
	/* cleanup the kuc channel */
	libcfs_ukuc_stop(&ct->kuc);
out_err:
	if (ct->mnt_fd != -1)
		close(ct->mnt_fd);
	if (ct->mnt)
		free(ct->mnt);
	free(ct);
	return rc;
}

/** Deregister a copytool
 * Note: under Linux, until llapi_hsm_copytool_unregister is called
 * (or the program is killed), the libcfs module will be referenced
 * and unremovable, even after Lustre services stop.
 */
int llapi_hsm_copytool_unregister(struct hsm_copytool_private **priv)
{
	struct hsm_copytool_private *ct;

	ct = *priv;
	if (!ct || (ct->magic != CT_PRIV_MAGIC))
		return -EINVAL;

	/* Tell the kernel to stop sending us messages */
	ct->kuc.lk_flags = LK_FLG_STOP;
	ioctl(ct->mnt_fd, LL_IOC_HSM_CT_START, &(ct->kuc));

	/* Shut down the kernelcomms */
	libcfs_ukuc_stop(&ct->kuc);

	close(ct->mnt_fd);
	free(ct->mnt);
	free(ct);
	*priv = NULL;
	return 0;
}

/** Wait for the next hsm_action_list
 * \param ct Opaque private control structure
 * \param halh Action list handle, will be allocated here
 * \param msgsize Number of bytes in the message, will be set here
 * \return 0 valid message received; halh and msgsize are set
 *	   <0 error code
 */
int llapi_hsm_copytool_recv(struct hsm_copytool_private *ct,
			    struct hsm_action_list **halh, int *msgsize)
{
	struct kuc_hdr			*kuch;
	struct hsm_action_list		*hal;
	int				 rc = 0;

	if (!ct || (ct->magic != CT_PRIV_MAGIC))
		return -EINVAL;
	if (halh == NULL || msgsize == NULL)
		return -EINVAL;

	kuch = malloc(HAL_MAXSIZE + sizeof(*kuch));
	if (kuch == NULL)
		return -ENOMEM;

	rc = libcfs_ukuc_msg_get(&ct->kuc, (char *)kuch,
				 HAL_MAXSIZE + sizeof(*kuch),
				 KUC_TRANSPORT_HSM);
	if (rc < 0)
		goto out_free;

	/* Handle generic messages */
	if (kuch->kuc_transport == KUC_TRANSPORT_GENERIC &&
	    kuch->kuc_msgtype == KUC_MSG_SHUTDOWN) {
		rc = -ESHUTDOWN;
		goto out_free;
	}

	if (kuch->kuc_transport != KUC_TRANSPORT_HSM ||
	    kuch->kuc_msgtype != HMT_ACTION_LIST) {
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "Unknown HSM message type %d:%d\n",
				  kuch->kuc_transport, kuch->kuc_msgtype);
		rc = -EPROTO;
		goto out_free;
	}

	if (kuch->kuc_msglen < sizeof(*kuch) + sizeof(*hal)) {
		llapi_err_noerrno(LLAPI_MSG_ERROR, "Short HSM message %d",
				  kuch->kuc_msglen);
		rc = -EPROTO;
		goto out_free;
	}

	/* Our message is a hsm_action_list. Use pointer math to skip
	* kuch_hdr and point directly to the message payload.
	*/
	hal = (struct hsm_action_list *)(kuch + 1);

	/* Check that we have registered for this archive #
	 * if 0 registered, we serve any archive */
	if (ct->archives &&
	    ((1 << (hal->hal_archive_id - 1)) & ct->archives) == 0) {
		llapi_err_noerrno(LLAPI_MSG_INFO,
				  "This copytool does not service archive #%d,"
				  " ignoring this request."
				  " Mask of served archive is 0x%.8X",
				  hal->hal_archive_id, ct->archives);
		rc = -EAGAIN;

		goto out_free;
	}

	*halh = hal;
	*msgsize = kuch->kuc_msglen - sizeof(*kuch);
	return 0;

out_free:
	*halh = NULL;
	*msgsize = 0;
	free(kuch);
	return rc;
}

/** Release the action list when done with it. */
int llapi_hsm_action_list_free(struct hsm_action_list **hal)
{
	/* Reuse the llapi_changelog_free function */
	return llapi_changelog_free((struct changelog_ext_rec **)hal);
}

/** Get parent path from mount point and fid.
 *
 * \param mnt        Filesystem root path.
 * \param fid        Object FID.
 * \param parent     Destination buffer.
 * \param parent_len Destination buffer size.
 * \return 0 on success.
 */
static int fid_parent(const char *mnt, const lustre_fid *fid, char *parent,
		      size_t parent_len)
{
	int		 rc;
	int		 linkno;
	long long	 recno;
	char		 file[PATH_MAX];
	char		 strfid[FID_NOBRACE_LEN + 1];
	char		*ptr;

	sprintf(strfid, DFID_NOBRACE, PFID(fid));

	rc = llapi_fid2path(mnt, strfid, file, sizeof(file),
			    &recno, &linkno);
	if (rc)
		return rc;

	/* fid2path returns a relative path */
	snprintf(parent, parent_len, "%s/%s", mnt, file);

	/* remove file name */
	ptr = strrchr(parent, '/');
	if ((ptr == NULL) || (ptr == parent))
		strcpy(parent, "/");
	else
		*ptr = '\0';

	return 0;
}

/** Prepare a restore operation (create the destination volatile file).
 *
 * \param hcp  Private copyaction handle.
 * \param mnt  Filesystem mount point.
 * \return 0 on success.
 */
static int ct_restore_initialize(struct hsm_copyaction_private *hcp,
				 const char *mnt)
{
	int			 rc;
	int			 fd;
	char			 parent[PATH_MAX + 1];
	struct hsm_action_item	*hai = &hcp->copy.hc_hai;

	rc = fid_parent(mnt, &hai->hai_fid, parent, sizeof(parent));
	if (rc)
		return rc;

	fd = llapi_create_volatile_idx(parent, 0, O_LOV_DELAY_CREATE);
	if (fd < 0)
		return fd;

	rc = llapi_fd2fid(fd, &hai->hai_dfid);
	if (rc)
		goto err_cleanup;

	hcp->data_fd = fd;

	return 0;

err_cleanup:
	if (fd) {
		hcp->data_fd = -1;
		close(fd);
	}

	return rc;
}

/** Start processing an HSM action.
 * Should be called by copytools just before starting handling a request.
 * It could be skipped if copytool only want to directly report an error,
 * \see llapi_hsm_action_end().
 *
 * \param hcp   Opaque action handle to be passed to llapi_hsm_action_progress
 *              and llapi_hsm_action_end.
 * \param mnt   Mount point of the corresponding Lustre filesystem.
 * \param hai   The hsm_action_item describing the request.
 * \param is_error  Whether this call is just to report an error.
 *
 * \return 0 on success.
 */
int llapi_hsm_action_begin(struct hsm_copyaction_private **hcp,
			   const char *mnt,
			   const struct hsm_action_item *hai, bool is_error)
{
	struct hsm_copyaction_private	*priv;
	int				 rc;

	priv = calloc(1, sizeof(*priv));
	if (priv == NULL)
		return -ENOMEM;

	priv->data_fd = -1;
	priv->mnt_fd = open(mnt, O_DIRECTORY | O_RDONLY | O_NONBLOCK);
	if (priv->mnt_fd == -1) {
		rc = -errno;
		goto err_out;
	}

	memcpy(&priv->copy.hc_hai, hai, sizeof(*hai));
	priv->copy.hc_hai.hai_len = sizeof(*hai);

	if (is_error)
		goto ok_out;

	if (hai->hai_action == HSMA_RESTORE) {
		rc = ct_restore_initialize(priv, mnt);
		if (rc)
			goto err_out;
	}

	rc = ioctl(priv->mnt_fd, LL_IOC_HSM_COPY_START, &priv->copy);
	if (rc) {
		rc = -errno;
		goto err_out;
	}

ok_out:
	priv->magic = CP_PRIV_MAGIC;
	*hcp = priv;
	return 0;

err_out:
	if (priv->data_fd != -1)
		close(priv->data_fd);

	if (priv->mnt_fd != -1)
		close(priv->mnt_fd);

	free(priv);

	return rc;
}

/** Terminate an HSM action processing.
 * Should be called by copytools just having finished handling the request.
 * \param hdl[in,out]  Handle returned by llapi_hsm_action_start.
 * \param he[in]       The final range of copied data (for copy actions).
 * \param errval[in]   The status code of the operation.
 * \param flags[in]    The flags about the termination status (HP_FLAG_RETRY if
 *                     the error is retryable).
 *
 * \return 0 on success.
 */
int llapi_hsm_action_end(struct hsm_copyaction_private **hcp,
			 const struct hsm_extent *he, int flags, int errval)
{
	struct hsm_copyaction_private	*priv;
	struct hsm_action_item		*hai;
	int				 rc;

	if (hcp == NULL || *hcp == NULL || he == NULL)
		return -EINVAL;

	priv = *hcp;

	if (priv->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	hai = &priv->copy.hc_hai;

	/* In some cases, like restore, 2 FIDs are used.
	 * Set the right FID to use here. */
	if (hai->hai_action == HSMA_ARCHIVE || hai->hai_action == HSMA_RESTORE)
		hai->hai_fid = hai->hai_dfid;

	/* Fill the last missing data that will be needed by
	 * kernel to send a hsm_progress. */
	priv->copy.hc_flags  = flags;
	priv->copy.hc_errval = abs(errval);

	priv->copy.hc_hai.hai_extent = *he;

	rc = ioctl(priv->mnt_fd, LL_IOC_HSM_COPY_END, &priv->copy);
	if (rc) {
		rc = -errno;
		goto err_cleanup;
	}

err_cleanup:
	if (priv->data_fd != -1)
		close(priv->data_fd);

	if (priv->mnt_fd != -1)
		close(priv->mnt_fd);

	free(priv);
	*hcp = NULL;

	return rc;
}

/** Notify a progress in processing an HSM action.
 * \param hdl[in,out]   handle returned by llapi_hsm_action_start.
 * \param he[in]        the range of copied data (for copy actions).
 * \param hp_flags[in]  HSM progress flags.
 * \return 0 on success.
 */
int llapi_hsm_action_progress(struct hsm_copyaction_private *hcp,
			      const struct hsm_extent *he, int hp_flags)
{
	int			 rc;
	struct hsm_progress	 hp;
	struct hsm_action_item	*hai;

	if (hcp == NULL || he == NULL)
		return -EINVAL;

	if (hcp->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	hai = &hcp->copy.hc_hai;

	memset(&hp, 0, sizeof(hp));

	hp.hp_cookie = hai->hai_cookie;
	hp.hp_flags  = hp_flags;

	/* Progress is made on the data fid */
	hp.hp_fid = hai->hai_dfid;

	memcpy(&hp.hp_extent, he, sizeof(*he));

	rc = ioctl(hcp->mnt_fd, LL_IOC_HSM_PROGRESS, &hp);
	if (rc)
		rc = -errno;

	return rc;
}

/** Get the fid of object to be used for copying data.
 * @return error code if the action is not a copy operation.
 */
int llapi_hsm_action_get_dfid(const struct hsm_copyaction_private *hcp,
			      lustre_fid *fid)
{
	const struct hsm_action_item	*hai = &hcp->copy.hc_hai;

	if (hcp->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	if (hai->hai_action != HSMA_RESTORE && hai->hai_action != HSMA_ARCHIVE)
		return -EINVAL;

	*fid = hai->hai_dfid;

	return 0;
}

/**
 * Get a file descriptor to be used for copying data. It's up to the
 * caller to close the FDs obtained from this function.
 *
 * @retval a file descriptor on success.
 * @retval a negative error code on failure.
 */
int llapi_hsm_action_get_fd(const struct hsm_copyaction_private *hcp)
{
	const struct hsm_action_item	*hai = &hcp->copy.hc_hai;

	if (hcp->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	if (hai->hai_action != HSMA_RESTORE)
		return -EINVAL;

	return dup(hcp->data_fd);
}

/**
 * Import an existing hsm-archived file into Lustre.
 *
 * Caller must access file by (returned) newfid value from now on.
 *
 * \param dst      path to Lustre destination (e.g. /mnt/lustre/my/file).
 * \param archive  archive number.
 * \param st       struct stat buffer containing file ownership, perm, etc.
 * \param stripe_* Striping options.  Currently ignored, since the restore
 *                 operation will set the striping.  In V2, this striping might
 *                 be used.
 * \param newfid[out] Filled with new Lustre fid.
 */
int llapi_hsm_import(const char *dst, int archive, const struct stat *st,
		     unsigned long long stripe_size, int stripe_offset,
		     int stripe_count, int stripe_pattern, char *pool_name,
		     lustre_fid *newfid)
{
	struct utimbuf	time;
	int		fd;
	int		rc = 0;

	/* Create a non-striped file */
	fd = open(dst, O_CREAT | O_EXCL | O_LOV_DELAY_CREATE | O_NONBLOCK,
		  st->st_mode);

	if (fd < 0)
		return -errno;
	close(fd);

	/* set size on MDT */
	if (truncate(dst, st->st_size) != 0) {
		rc = -errno;
		goto out_unlink;
	}
	/* Mark archived */
	rc = llapi_hsm_state_set(dst, HS_EXISTS | HS_RELEASED | HS_ARCHIVED, 0,
				 archive);
	if (rc)
		goto out_unlink;

	/* Get the new fid in the archive. Caller needs to use this fid
	   from now on. */
	rc = llapi_path2fid(dst, newfid);
	if (rc)
		goto out_unlink;

	/* Copy the file attributes */
	time.actime = st->st_atime;
	time.modtime = st->st_mtime;
	if (utime(dst, &time) == -1 ||
		chmod(dst, st->st_mode) == -1 ||
		chown(dst, st->st_uid, st->st_gid) == -1) {
		/* we might fail here because we change perms/owner */
		rc = -errno;
		goto out_unlink;
	}

out_unlink:
	if (rc)
		unlink(dst);
	return rc;
}

/**
 * Return the current HSM states and HSM requests related to file pointed by \a
 * path.
 *
 * \param hus  Should be allocated by caller. Will be filled with current file
 *             states.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_state_get(const char *path, struct hsm_user_state *hus)
{
	int fd;
	int rc;

	fd = open(path, O_RDONLY | O_NONBLOCK);
	if (fd < 0)
		return -errno;

	rc = ioctl(fd, LL_IOC_HSM_STATE_GET, hus);
	/* If error, save errno value */
	rc = rc ? -errno : 0;

	close(fd);
	return rc;
}

/**
 * Set HSM states of file pointed by \a path.
 *
 * Using the provided bitmasks, the current HSM states for this file will be
 * changed. \a archive_id could be used to change the archive number also. Set
 * it to 0 if you do not want to change it.
 *
 * \param setmask      Bitmask for flag to be set.
 * \param clearmask    Bitmask for flag to be cleared.
 * \param archive_id  Archive number identifier to use. 0 means no change.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_state_set(const char *path, __u64 setmask, __u64 clearmask,
			__u32 archive_id)
{
	struct hsm_state_set hss;
	int fd;
	int rc;

	fd = open(path, O_WRONLY | O_LOV_DELAY_CREATE | O_NONBLOCK);
	if (fd < 0)
		return -errno;

	hss.hss_valid = HSS_SETMASK|HSS_CLEARMASK;
	hss.hss_setmask = setmask;
	hss.hss_clearmask = clearmask;
	/* Change archive_id if provided. We can only change
	 * to set something different than 0. */
	if (archive_id > 0) {
		hss.hss_valid |= HSS_ARCHIVE_ID;
		hss.hss_archive_id = archive_id;
	}
	rc = ioctl(fd, LL_IOC_HSM_STATE_SET, &hss);
	/* If error, save errno value */
	rc = rc ? -errno : 0;

	close(fd);
	return rc;
}


/**
 * Return the current HSM request related to file pointed by \a path.
 *
 * \param hca  Should be allocated by caller. Will be filled with current file
 *             actions.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_current_action(const char *path, struct hsm_current_action *hca)
{
	int fd;
	int rc;

	fd = open(path, O_RDONLY | O_NONBLOCK);
	if (fd < 0)
		return -errno;

	rc = ioctl(fd, LL_IOC_HSM_ACTION, hca);
	/* If error, save errno value */
	rc = rc ? -errno : 0;

	close(fd);
	return rc;
}

/**
 * Allocate a hsm_user_request with the specified carateristics.
 * This structure should be freed with free().
 *
 * \return an allocated structure on success, NULL otherwise.
 */
struct hsm_user_request *llapi_hsm_user_request_alloc(int itemcount,
						      int data_len)
{
	int len = 0;

	len += sizeof(struct hsm_user_request);
	len += sizeof(struct hsm_user_item) * itemcount;
	len += data_len;

	return (struct hsm_user_request *)malloc(len);
}

/**
 * Send a HSM request to Lustre, described in \param request.
 *
 * \param path	  Fullpath to the file to operate on.
 * \param request The request, allocated with llapi_hsm_user_request_alloc().
 *
 * \return 0 on success, an error code otherwise.
 */
int llapi_hsm_request(const char *path, const struct hsm_user_request *request)
{
	int rc;
	int fd;

	rc = get_root_path(WANT_FD, NULL, &fd, (char *)path, -1);
	if (rc)
		return rc;

	rc = ioctl(fd, LL_IOC_HSM_REQUEST, request);
	/* If error, save errno value */
	rc = rc ? -errno : 0;

	close(fd);
	return rc;
}

