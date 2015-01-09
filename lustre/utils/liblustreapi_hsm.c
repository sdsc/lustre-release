/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 * Copyright (c) 2013, 2014, Intel Corporation.
 * Copyright 2014 Cray Inc., All rights reserved.
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
#include <signal.h>
#ifdef HAVE_LINUX_UNISTD_H
#include <linux/unistd.h>
#else
#include <unistd.h>
#endif

#include <libcfs/libcfs.h>
#include <lnet/lnetctl.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustreapi.h>

#include "lustreapi_internal.h"
#include "ccan/json/json.h"

#define OPEN_BY_FID_PATH dot_lustre_name"/fid"

/****** HSM Copytool API ********/
#define CT_PRIV_MAGIC 0xC0BE2001
struct hsm_copytool_private {
	int			 magic;
	char			*mnt;
	struct kuc_hdr		*kuch;
	int			 mnt_fd;
	int			 open_by_fid_fd;
	struct lustre_kernelcomm kuc;
	__u32			 archives;
};

#define CP_PRIV_MAGIC 0x19880429
struct hsm_copyaction_private {
	__u32					 magic;
	__s32					 data_fd;
	const struct hsm_copytool_private	*ct_priv;
	struct hsm_copy				 copy;
	lstat_t					 stat;
};

#include <libcfs/libcfs.h>

enum ct_progress_type {
	CT_START	= 0,
	CT_RUNNING	= 50,
	CT_FINISH	= 100,
	CT_CANCEL	= 150,
	CT_ERROR	= 175
};

enum ct_event {
	CT_REGISTER		= 1,
	CT_UNREGISTER		= 2,
	CT_ARCHIVE_START	= HSMA_ARCHIVE,
	CT_ARCHIVE_RUNNING	= HSMA_ARCHIVE + CT_RUNNING,
	CT_ARCHIVE_FINISH	= HSMA_ARCHIVE + CT_FINISH,
	CT_ARCHIVE_CANCEL	= HSMA_ARCHIVE + CT_CANCEL,
	CT_ARCHIVE_ERROR	= HSMA_ARCHIVE + CT_ERROR,
	CT_RESTORE_START	= HSMA_RESTORE,
	CT_RESTORE_RUNNING	= HSMA_RESTORE + CT_RUNNING,
	CT_RESTORE_FINISH	= HSMA_RESTORE + CT_FINISH,
	CT_RESTORE_CANCEL	= HSMA_RESTORE + CT_CANCEL,
	CT_RESTORE_ERROR	= HSMA_RESTORE + CT_ERROR,
	CT_REMOVE_START		= HSMA_REMOVE,
	CT_REMOVE_RUNNING	= HSMA_REMOVE + CT_RUNNING,
	CT_REMOVE_FINISH	= HSMA_REMOVE + CT_FINISH,
	CT_REMOVE_CANCEL	= HSMA_REMOVE + CT_CANCEL,
	CT_REMOVE_ERROR		= HSMA_REMOVE + CT_ERROR,
	CT_MIGRATE_START	= HSMA_MIGRATE,
	CT_MIGRATE_RUNNING	= HSMA_MIGRATE + CT_RUNNING,
	CT_MIGRATE_FINISH	= HSMA_MIGRATE + CT_FINISH,
	CT_MIGRATE_CANCEL	= HSMA_MIGRATE + CT_CANCEL,
	CT_MIGRATE_ERROR	= HSMA_MIGRATE + CT_ERROR,
	CT_EVENT_MAX
};

/* initialized in llapi_hsm_register_event_fifo() */
static int llapi_hsm_event_fd = -1;
static bool created_hsm_event_fifo;

static inline const char *llapi_hsm_ct_ev2str(int type)
{
	switch (type) {
	case CT_REGISTER:
		return "REGISTER";
	case CT_UNREGISTER:
		return "UNREGISTER";
	case CT_ARCHIVE_START:
		return "ARCHIVE_START";
	case CT_ARCHIVE_RUNNING:
		return "ARCHIVE_RUNNING";
	case CT_ARCHIVE_FINISH:
		return "ARCHIVE_FINISH";
	case CT_ARCHIVE_CANCEL:
		return "ARCHIVE_CANCEL";
	case CT_ARCHIVE_ERROR:
		return "ARCHIVE_ERROR";
	case CT_RESTORE_START:
		return "RESTORE_START";
	case CT_RESTORE_RUNNING:
		return "RESTORE_RUNNING";
	case CT_RESTORE_FINISH:
		return "RESTORE_FINISH";
	case CT_RESTORE_CANCEL:
		return "RESTORE_CANCEL";
	case CT_RESTORE_ERROR:
		return "RESTORE_ERROR";
	case CT_REMOVE_START:
		return "REMOVE_START";
	case CT_REMOVE_RUNNING:
		return "REMOVE_RUNNING";
	case CT_REMOVE_FINISH:
		return "REMOVE_FINISH";
	case CT_REMOVE_CANCEL:
		return "REMOVE_CANCEL";
	case CT_REMOVE_ERROR:
		return "REMOVE_ERROR";
	case CT_MIGRATE_START:
		return "MIGRATE_START";
	case CT_MIGRATE_RUNNING:
		return "MIGRATE_RUNNING";
	case CT_MIGRATE_FINISH:
		return "MIGRATE_FINISH";
	case CT_MIGRATE_CANCEL:
		return "MIGRATE_CANCEL";
	case CT_MIGRATE_ERROR:
		return "MIGRATE_ERROR";
	default:
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "Unknown event type: %d", type);
		return NULL;
	}
}

/**
 * Writes a JSON event to the monitor FIFO. Noop if no FIFO has been
 * registered.
 *
 * \param json_items              A JSON object
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
static int llapi_hsm_write_json_event(JsonNode *json_items)
{
	int				rc;
	char				time_string[40];
	time_t				event_time = time(0);
	struct tm			time_components;
	char				*json_str;
	JsonNode			*obj;

	/* Noop unless the event fd was initialized */
	if (llapi_hsm_event_fd < 0)
		return 0;

	if (json_items == NULL)
		return -EINVAL;

	localtime_r(&event_time, &time_components);

	if (strftime(time_string, sizeof(time_string), "%Y-%m-%d %T %z",
		     &time_components) == 0) {
		rc = -EINVAL;
		llapi_error(LLAPI_MSG_ERROR, rc, "strftime() failed");
		return rc;
	}

	obj = json_mkstring(time_string);
	if (obj == NULL) {
		llapi_err_noerrno(LLAPI_MSG_ERROR, "error in json_mkstring");
		return -ENOMEM;
	}
	json_append_member(json_items, "event_time", obj);

	json_str = json_stringify(json_items, NULL);
	if (json_str == NULL)
		return -ENOMEM;

	rc = write(llapi_hsm_event_fd, json_str, strlen(json_str));
	free(json_str);
	if (rc < 0) {
		/* Ignore write failures due to missing reader. */
		if (errno != EPIPE)
			return -errno;
	}

	rc = write(llapi_hsm_event_fd, "\n", 1);
	if (rc < 0) {
		/* Ignore write failures due to missing reader. */
		if (errno != EPIPE)
			return -errno;
	}

	return 0;
}

/**
 * Hook for llapi_hsm_copytool_register and llapi_hsm_copytool_unregister
 * to generate JSON events suitable for consumption by a copytool
 * monitoring process.
 *
 * \param priv               Opaque private control structure.
 * \param event_type         The type of event (register or unregister).
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
static int llapi_hsm_log_ct_registration(struct hsm_copytool_private **priv,
					 __u32 event_type)
{
	int				rc;
	char				agent_uuid[UUID_MAX];
	struct hsm_copytool_private	*ct;
	JsonNode			*json_items;
	JsonNode			*obj;

	/* Noop unless the event fd was initialized */
	if (llapi_hsm_event_fd < 0)
		return 0;

	if (priv == NULL || *priv == NULL)
		return -EINVAL;

	ct = *priv;
	if (ct->magic != CT_PRIV_MAGIC)
		return -EINVAL;

	if (event_type != CT_REGISTER && event_type != CT_UNREGISTER)
		return -EINVAL;

	json_items = json_mkobject();
	if (json_items == NULL) {
		rc = -ENOMEM;
		goto err;
	}

	rc = llapi_get_agent_uuid(ct->mnt, agent_uuid, sizeof(agent_uuid));
	if (rc < 0)
		goto err;
	llapi_chomp_string(agent_uuid);

	obj = json_mkstring(agent_uuid);
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "uuid", obj);

	obj = json_mkstring(ct->mnt);
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "mount_point", obj);

	obj = json_mknumber_int(ct->archives);
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "archive", obj);

	obj = json_mkstring((char *)llapi_hsm_ct_ev2str(event_type));
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "event_type", obj);

	rc = llapi_hsm_write_json_event(json_items);
	if (rc < 0)
		goto err;

	goto out_free;

err:
	llapi_error(LLAPI_MSG_ERROR, rc, "error in "
		    "llapi_hsm_log_ct_registration()");

out_free:
	if (json_items != NULL)
		json_delete(json_items);

	return rc;
}

/**
 * Given a copytool progress update, construct a JSON event suitable for
 * consumption by a copytool monitoring process.
 *
 * Examples of various events generated here and written by
 * llapi_hsm_write_json_event:
 *
 * Copytool registration and deregistration:
 * {"event_time": "2014-02-26 14:58:01 -0500", "event_type": "REGISTER",
 *  "archive": 0, "mount_point": "/mnt/lustre",
 *  "uuid": "80379a60-1f8a-743f-daf2-307cde793ec2"}
 * {"event_time": "2014-02-26 14:58:01 -0500", "event_type": "UNREGISTER",
 *  "archive": 0, "mount_point": "/mnt/lustre",
 *  "uuid": "80379a60-1f8a-743f-daf2-307cde793ec2"}
 *
 * An archive action, start to completion:
 * {"event_time": "2014-02-26 14:50:13 -0500", "event_type": "ARCHIVE_START",
 *  "total_bytes": 0, "lustre_path": "d71.sanity-hsm/f71.sanity-hsm",
 *  "source_fid": "0x2000013a1:0x2:0x0", "data_fid": "0x2000013a1:0x2:0x0"}
 * {"event_time": "2014-02-26 14:50:18 -0500", "event_type": "ARCHIVE_RUNNING",
 *  "current_bytes": 5242880, "total_bytes": 39000000,
 *  "lustre_path": "d71.sanity-hsm/f71.sanity-hsm",
 *  "source_fid": "0x2000013a1:0x2:0x0", "data_fid": "0x2000013a1:0x2:0x0"}
 * {"event_time": "2014-02-26 14:50:50 -0500", "event_type": "ARCHIVE_FINISH",
 *  "source_fid": "0x2000013a1:0x2:0x0", "data_fid": "0x2000013a1:0x2:0x0"}
 *
 * A log message:
 * {"event_time": "2014-02-26 14:50:13 -0500", "event_type": "LOGGED_MESSAGE",
 *  "level": "INFO",
 *  "message": "lhsmtool_posix[42]: copytool fs=lustre archive#=2 item_count=1"}
 *
 * \param hcp                Opaque action handle returned by
 *                           llapi_hsm_action_start.
 * \param hai                The hsm_action_item describing the request.
 * \param progress_type      The ct_progress_type describing the update.
 * \param total              The total expected bytes for the request.
 * \param current            The current copied byte count for the request.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
static int llapi_hsm_log_ct_progress(struct hsm_copyaction_private **phcp,
				     const struct hsm_action_item *hai,
				     __u32 progress_type,
				     __u64 total, __u64 current)
{
	int				rc;
	int				linkno = 0;
	long long			recno = -1;
	char				lustre_path[PATH_MAX];
	char				strfid[FID_NOBRACE_LEN + 1];
	struct hsm_copyaction_private	*hcp;
	JsonNode			*json_items;
	JsonNode			*obj;

	/* Noop unless the event fd was initialized */
	if (llapi_hsm_event_fd < 0)
		return 0;

	if (phcp == NULL || *phcp == NULL)
		return -EINVAL;

	hcp = *phcp;

	json_items = json_mkobject();
	if (json_items == NULL) {
		rc = -ENOMEM;
		goto err;
	}

	snprintf(strfid, sizeof(strfid), DFID_NOBRACE, PFID(&hai->hai_dfid));
	obj = json_mkstring(strfid);
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "data_fid", obj);

	snprintf(strfid, sizeof(strfid), DFID_NOBRACE, PFID(&hai->hai_fid));
	obj = json_mkstring(strfid);
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "source_fid", obj);

	if (hcp->copy.hc_errval == ECANCELED) {
		progress_type = CT_CANCEL;
		goto cancel;
	}

	if (hcp->copy.hc_errval != 0) {
		progress_type = CT_ERROR;

		obj = json_mknumber_int(hcp->copy.hc_errval);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "errno", obj);

		obj = json_mkstring(strerror(hcp->copy.hc_errval));
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "error", obj);

		goto cancel;
	}

	/* lustre_path isn't available after a restore completes */
	/* total_bytes isn't available after a restore or archive completes */
	if (progress_type != CT_FINISH) {
		rc = llapi_fid2path(hcp->ct_priv->mnt, strfid, lustre_path,
				    sizeof(lustre_path), &recno, &linkno);
		if (rc < 0)
			goto err;

		obj = json_mkstring(lustre_path);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "lustre_path", obj);

		obj = json_mknumber_int(total);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "total_bytes", obj);
	}

	if (progress_type == CT_RUNNING) {
		obj = json_mknumber_int(current);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "current_bytes", obj);
	}

cancel:
	obj = json_mkstring(llapi_hsm_ct_ev2str(hai->hai_action +
						progress_type));
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "event_type", obj);

	rc = llapi_hsm_write_json_event(json_items);
	if (rc < 0)
		goto err;

	goto out_free;

err:
	llapi_error(LLAPI_MSG_ERROR, rc, "error in "
		    "llapi_hsm_log_ct_progress()");

out_free:
	if (json_items != NULL)
		json_delete(json_items);

	return rc;
}

/**
 * Given a path to a FIFO, create a filehandle for nonblocking writes to it.
 * Intended to be used for copytool monitoring processes that read an
 * event stream from the FIFO. Events written in the absence of a reader
 * are lost.
 *
 * \param path               Path to monitor FIFO.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_register_event_fifo(const char *path)
{
	int read_fd;
	struct stat statbuf;

	/* Create the FIFO if necessary. */
	if ((mkfifo(path, 0644) < 0) && (errno != EEXIST)) {
		llapi_error(LLAPI_MSG_ERROR, errno, "mkfifo(%s) failed", path);
		return -errno;
	}
	if (errno == EEXIST) {
		if (stat(path, &statbuf) < 0) {
			llapi_error(LLAPI_MSG_ERROR, errno, "mkfifo(%s) failed",
				    path);
			return -errno;
		}
		if (!S_ISFIFO(statbuf.st_mode) ||
		    ((statbuf.st_mode & 0777) != 0644)) {
			llapi_error(LLAPI_MSG_ERROR, errno, "%s exists but is "
				    "not a pipe or has a wrong mode", path);
			return -errno;
		}
	} else {
		created_hsm_event_fifo = true;
	}

	/* Open the FIFO for read so that the subsequent open for write
	 * doesn't immediately fail. */
	read_fd = open(path, O_RDONLY | O_NONBLOCK);
	if (read_fd < 0) {
		llapi_error(LLAPI_MSG_ERROR, errno,
			    "cannot open(%s) for read", path);
		return -errno;
	}

	/* Open the FIFO for writes, but don't block on waiting
	 * for a reader. */
	llapi_hsm_event_fd = open(path, O_WRONLY | O_NONBLOCK);
	if (llapi_hsm_event_fd < 0) {
		llapi_error(LLAPI_MSG_ERROR, errno,
			    "cannot open(%s) for write", path);
		return -errno;
	}

	/* Now close the reader. An external monitoring process can
	 * now open the FIFO for reads. If no reader comes along the
	 * events are lost. NOTE: Only one reader at a time! */
	close(read_fd);

	/* Ignore SIGPIPEs -- can occur if the reader goes away. */
	signal(SIGPIPE, SIG_IGN);

	return 0;
}

/**
 * Given a path to a FIFO, close its filehandle and delete the FIFO.
 *
 * \param path               Path to monitor FIFO.
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_unregister_event_fifo(const char *path)
{
	/* Noop unless the event fd was initialized */
	if (llapi_hsm_event_fd < 0)
		return 0;

	if (close(llapi_hsm_event_fd) < 0)
		return -errno;

	if (created_hsm_event_fifo) {
		unlink(path);
		created_hsm_event_fifo = false;
	}

	llapi_hsm_event_fd = -1;

	return 0;
}

/**
 * Custom logging callback to be used when a monitoring FIFO has been
 * registered. Formats log entries as JSON events suitable for
 * consumption by a copytool monitoring process.
 *
 * \param level              The message loglevel.
 * \param _rc                The returncode associated with the message.
 * \param fmt                The message format string.
 * \param args               Arguments to be formatted by the format string.
 *
 * \retval None.
 */
void llapi_hsm_log_error(enum llapi_message_level level, int _rc,
			 const char *fmt, va_list args)
{
	int				rc;
	int				msg_len;
	int				real_level;
	char				*msg = NULL;
	va_list				args2;
	JsonNode			*json_items;
	JsonNode			*obj;

	/* Noop unless the event fd was initialized */
	if (llapi_hsm_event_fd < 0)
		return;

	json_items = json_mkobject();
	if (json_items == NULL) {
		rc = -ENOMEM;
		goto err;
	}

	if ((level & LLAPI_MSG_NO_ERRNO) == 0) {
		obj = json_mknumber_int(_rc);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "errno", obj);

		obj = json_mkstring(strerror(abs(_rc)));
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "error", obj);
	}

	va_copy(args2, args);
	msg_len = vsnprintf(NULL, 0, fmt, args2) + 1;
	va_end(args2);
	if (msg_len >= 0) {
		msg = (char *) alloca(msg_len);
		if (msg == NULL) {
			rc = -ENOMEM;
			goto err;
		}

		rc = vsnprintf(msg, msg_len, fmt, args);
		if (rc < 0)
			goto err;

		obj = json_mkstring(msg);
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "message", obj);
	} else {
		obj = json_mkstring("INTERNAL ERROR: message failed");
		if (obj == NULL) {
			rc = -ENOMEM;
			goto err;
		}
		json_append_member(json_items, "message", obj);
	}

	real_level = level & LLAPI_MSG_NO_ERRNO;
	real_level = real_level > 0 ? level - LLAPI_MSG_NO_ERRNO : level;

	obj = json_mkstring(llapi_msg_level2str(real_level));
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "level", obj);

	obj = json_mkstring("LOGGED_MESSAGE");
	if (obj == NULL) {
		rc = -ENOMEM;
		goto err;
	}
	json_append_member(json_items, "event_type", obj);

	rc = llapi_hsm_write_json_event(json_items);
	if (rc < 0)
		goto err;

	goto out_free;

err:
	/* Write directly to stderr to avoid llapi_error, which now
	 * emits JSON event messages. */
	fprintf(stderr, "\nFATAL ERROR IN llapi_hsm_log_error(): rc %d,", rc);

out_free:
	if (json_items != NULL)
		json_delete(json_items);

	return;
}

/** Register a copytool
 * \param[out] priv		Opaque private control structure
 * \param mnt			Lustre filesystem mount point
 * \param archive_count		Number of valid archive IDs in \a archives
 * \param archives		Which archive numbers this copytool is
 *				responsible for
 * \param rfd_flags		flags applied to read fd of pipe
 *				(e.g. O_NONBLOCK)
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
int llapi_hsm_copytool_register(struct hsm_copytool_private **priv,
				const char *mnt, int archive_count,
				int *archives, int rfd_flags)
{
	struct hsm_copytool_private	*ct;
	int				 rc;

	if (archive_count > 0 && archives == NULL) {
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "NULL archive numbers");
		return -EINVAL;
	}

	if (archive_count > LL_HSM_MAX_ARCHIVE) {
		llapi_err_noerrno(LLAPI_MSG_ERROR, "%d requested when maximum "
				  "of %zu archives supported", archive_count,
				  LL_HSM_MAX_ARCHIVE);
		return -EINVAL;
	}

	ct = calloc(1, sizeof(*ct));
	if (ct == NULL)
		return -ENOMEM;

	ct->magic = CT_PRIV_MAGIC;
	ct->mnt_fd = -1;
	ct->open_by_fid_fd = -1;
	ct->kuc.lk_rfd = LK_NOFD;
	ct->kuc.lk_wfd = LK_NOFD;

	ct->mnt = strdup(mnt);
	if (ct->mnt == NULL) {
		rc = -ENOMEM;
		goto out_err;
	}

	ct->kuch = malloc(HAL_MAXSIZE + sizeof(*ct->kuch));
	if (ct->kuch == NULL) {
		rc = -ENOMEM;
		goto out_err;
	}

	ct->mnt_fd = open(ct->mnt, O_RDONLY);
	if (ct->mnt_fd < 0) {
		rc = -errno;
		goto out_err;
	}

	ct->open_by_fid_fd = openat(ct->mnt_fd, OPEN_BY_FID_PATH, O_RDONLY);
	if (ct->open_by_fid_fd < 0) {
		rc = -errno;
		goto out_err;
	}

	/* no archives specified means "match all". */
	ct->archives = 0;
	for (rc = 0; rc < archive_count; rc++) {
		if ((archives[rc] > LL_HSM_MAX_ARCHIVE) || (archives[rc] < 0)) {
			llapi_err_noerrno(LLAPI_MSG_ERROR, "%d requested when "
					  "archive id [0 - %zu] is supported",
					  archives[rc], LL_HSM_MAX_ARCHIVE);
			rc = -EINVAL;
			goto out_err;
		}
		/* in the list we have an all archive wildcard
		 * so move to all archives mode
		 */
		if (archives[rc] == 0) {
			ct->archives = 0;
			archive_count = 0;
			break;
		}
		ct->archives |= (1 << (archives[rc] - 1));
	}

	rc = libcfs_ukuc_start(&ct->kuc, KUC_GRP_HSM, rfd_flags);
	if (rc < 0)
		goto out_err;

	/* Storing archive(s) in lk_data; see mdc_ioc_hsm_ct_start */
	ct->kuc.lk_data = ct->archives;
	rc = ioctl(ct->mnt_fd, LL_IOC_HSM_CT_START, &ct->kuc);
	if (rc < 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "cannot start copytool on '%s'", mnt);
		goto out_kuc;
	}

	llapi_hsm_log_ct_registration(&ct, CT_REGISTER);

	/* Only the kernel reference keeps the write side open */
	close(ct->kuc.lk_wfd);
	ct->kuc.lk_wfd = LK_NOFD;
	*priv = ct;

	return 0;

out_kuc:
	/* cleanup the kuc channel */
	libcfs_ukuc_stop(&ct->kuc);

out_err:
	if (!(ct->mnt_fd < 0))
		close(ct->mnt_fd);

	if (!(ct->open_by_fid_fd < 0))
		close(ct->open_by_fid_fd);

	free(ct->mnt);

	free(ct->kuch);

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

	if (priv == NULL || *priv == NULL)
		return -EINVAL;

	ct = *priv;
	if (ct->magic != CT_PRIV_MAGIC)
		return -EINVAL;

	/* Tell the kernel to stop sending us messages */
	ct->kuc.lk_flags = LK_FLG_STOP;
	ioctl(ct->mnt_fd, LL_IOC_HSM_CT_START, &ct->kuc);

	/* Shut down the kernelcomms */
	libcfs_ukuc_stop(&ct->kuc);

	llapi_hsm_log_ct_registration(&ct, CT_UNREGISTER);

	close(ct->open_by_fid_fd);
	close(ct->mnt_fd);
	free(ct->mnt);
	free(ct->kuch);
	free(ct);
	*priv = NULL;

	return 0;
}

/** Returns a file descriptor to poll/select on.
 * \param ct Opaque private control structure
 * \retval -EINVAL on error
 * \retval the file descriptor for reading HSM events from the kernel
 */
int llapi_hsm_copytool_get_fd(struct hsm_copytool_private *ct)
{
	if (ct == NULL || ct->magic != CT_PRIV_MAGIC)
		return -EINVAL;

	return libcfs_ukuc_get_rfd(&ct->kuc);
}

/** Wait for the next hsm_action_list
 * \param ct Opaque private control structure
 * \param halh Action list handle, will be allocated here
 * \param msgsize Number of bytes in the message, will be set here
 * \return 0 valid message received; halh and msgsize are set
 *	   <0 error code
 * Note: The application must not call llapi_hsm_copytool_recv until it has
 * cleared the data in ct->kuch from the previous call.
 */
int llapi_hsm_copytool_recv(struct hsm_copytool_private *ct,
			    struct hsm_action_list **halh, int *msgsize)
{
	struct kuc_hdr		*kuch;
	struct hsm_action_list	*hal;
	int			 rc = 0;

	if (ct == NULL || ct->magic != CT_PRIV_MAGIC)
		return -EINVAL;

	if (halh == NULL || msgsize == NULL)
		return -EINVAL;

	kuch = ct->kuch;

repeat:
	rc = libcfs_ukuc_msg_get(&ct->kuc, (char *)kuch,
				 HAL_MAXSIZE + sizeof(*kuch),
				 KUC_TRANSPORT_HSM);
	if (rc < 0)
		goto out_err;

	/* Handle generic messages */
	if (kuch->kuc_transport == KUC_TRANSPORT_GENERIC &&
	    kuch->kuc_msgtype == KUC_MSG_SHUTDOWN) {
		rc = -ESHUTDOWN;
		goto out_err;
	}

	if (kuch->kuc_transport != KUC_TRANSPORT_HSM ||
	    kuch->kuc_msgtype != HMT_ACTION_LIST) {
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "Unknown HSM message type %d:%d\n",
				  kuch->kuc_transport, kuch->kuc_msgtype);
		rc = -EPROTO;
		goto out_err;
	}

	if (kuch->kuc_msglen < sizeof(*kuch) + sizeof(*hal)) {
		llapi_err_noerrno(LLAPI_MSG_ERROR, "Short HSM message %d",
				  kuch->kuc_msglen);
		rc = -EPROTO;
		goto out_err;
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

		goto repeat;
	}

	*halh = hal;
	*msgsize = kuch->kuc_msglen - sizeof(*kuch);
	return 0;

out_err:
	*halh = NULL;
	*msgsize = 0;
	return rc;
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
	int		 linkno = 0;
	long long	 recno = -1;
	char		 file[PATH_MAX];
	char		 strfid[FID_NOBRACE_LEN + 1];
	char		*ptr;

	snprintf(strfid, sizeof(strfid), DFID_NOBRACE, PFID(fid));

	rc = llapi_fid2path(mnt, strfid, file, sizeof(file),
			    &recno, &linkno);
	if (rc < 0)
		return rc;

	/* fid2path returns a relative path */
	rc = snprintf(parent, parent_len, "%s/%s", mnt, file);
	if (rc >= parent_len)
		return -ENAMETOOLONG;

	/* remove file name */
	ptr = strrchr(parent, '/');
	if (ptr == NULL || ptr == parent) {
		rc = -EINVAL;
	} else {
		*ptr = '\0';
		rc = 0;
	}

	return rc;
}

static int ct_open_by_fid(const struct hsm_copytool_private *ct,
			  const struct lu_fid *fid, int open_flags)
{
	char fid_name[FID_NOBRACE_LEN + 1];
	int fd;

	snprintf(fid_name, sizeof(fid_name), DFID_NOBRACE, PFID(fid));

	fd = openat(ct->open_by_fid_fd, fid_name, open_flags);
	return fd < 0 ? -errno : fd;
}

/**
 * Get metadata attributes of file by FID.
 *
 * Use the IOC_MDC_GETFILEINFO ioctl (to send a MDS_GETATTR_NAME RPC)
 * to get the attributes of the file identified by \a fid. This
 * returns only the attributes stored on the MDT and avoids taking
 * layout locks or accessing OST objects. It also bypasses the inode
 * cache. Attributes are returned in \a st.
 */
static int ct_md_getattr(const struct hsm_copytool_private *ct,
			 const struct lu_fid *fid,
			 lstat_t *st)
{
	struct lov_user_mds_data *lmd;
	size_t lmd_size;
	int rc;

	lmd_size = sizeof(lmd->lmd_st) +
		lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);

	if (lmd_size < sizeof(lmd->lmd_st) + XATTR_SIZE_MAX)
		lmd_size = sizeof(lmd->lmd_st) + XATTR_SIZE_MAX;

	if (lmd_size < FID_NOBRACE_LEN + 1)
		lmd_size = FID_NOBRACE_LEN + 1;

	lmd = malloc(lmd_size);
	if (lmd == NULL)
		return -ENOMEM;

	snprintf((char *)lmd, lmd_size, DFID_NOBRACE, PFID(fid));

	rc = ioctl(ct->open_by_fid_fd, IOC_MDC_GETFILEINFO, lmd);
	if (rc != 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "cannot get metadata attributes of "DFID" in '%s'",
			    PFID(fid), ct->mnt);
		goto out;
	}

	*st = lmd->lmd_st;
out:
	free(lmd);

	return rc;
}

/** Create the destination volatile file for a restore operation.
 *
 * \param hcp        Private copyaction handle.
 * \param mdt_index  MDT index where to create the volatile file.
 * \param flags      Volatile file creation flags.
 * \return 0 on success.
 */
static int create_restore_volatile(struct hsm_copyaction_private *hcp,
				   int mdt_index, int open_flags)
{
	int			 rc;
	int			 fd;
	char			 parent[PATH_MAX + 1];
	const char		*mnt = hcp->ct_priv->mnt;
	struct hsm_action_item	*hai = &hcp->copy.hc_hai;

	rc = fid_parent(mnt, &hai->hai_fid, parent, sizeof(parent));
	if (rc < 0) {
		/* fid_parent() failed, try to keep on going */
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "cannot get parent path to restore "DFID" "
			    "using '%s'", PFID(&hai->hai_fid), mnt);
		snprintf(parent, sizeof(parent), "%s", mnt);
	}

	fd = llapi_create_volatile_idx(parent, mdt_index, open_flags);
	if (fd < 0)
		return fd;

	rc = fchown(fd, hcp->stat.st_uid, hcp->stat.st_gid);
	if (rc < 0)
		goto err_cleanup;

	rc = llapi_fd2fid(fd, &hai->hai_dfid);
	if (rc < 0)
		goto err_cleanup;

	hcp->data_fd = fd;

	return 0;

err_cleanup:
	hcp->data_fd = -1;
	close(fd);

	return rc;
}

#define set_number(root, name, number, errors) \
	do {							\
		JsonNode *_obj = json_find_member(root, name);	\
		if (!_obj || _obj->tag != JSON_NUMBER_INT)	\
			errors++;				\
		else						\
			number = _obj->number_int;		\
	} while (0)

#define set_string(root, name, string, errors) \
	do {					       \
		JsonNode *_obj = json_find_member(root, name);	\
		if (!_obj || _obj->tag != JSON_STRING) {	\
			string = NULL;				\
			errors++;				\
		} else {					\
			string = _obj->string_;			\
		}						\
	} while (0)

/**
 * Converts the stripe info in an hsm_action_item back to a
 * struct llapi_stripe_param. Used by migration.
 *
 * \param[in]	hai		hsm action item containing the stripe param as
 *				its data payload
 * \param[out]	hsm_param	the stripe parameters
 * \param[out]	mdt_index	the MDT index
 * \retval 0	Success
 * \retval	negative errno on error
 */
static int hai_to_stripe_info(const struct hsm_action_item *hai,
			      struct llapi_stripe_param **param_out,
			      int *mdt_index)
{
	struct llapi_stripe_param *param;
	const char *hsm_param;
	size_t data_len;
	int rc;
	JsonNode *jroot;
	JsonNode *obj;
	JsonNode *lsp_obj;
	JsonNode *osts_obj;
	size_t osts_count;
	int errors;
	int i;
	int json_mdt_index;
	char *json_pool;

	*param_out = NULL;
	*mdt_index = -1;

	/* Ensure the hai data is long enough. */
	if (hai->hai_len < sizeof(*hai))  {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR, "HAI is too short (%u/%zd)",
				  hai->hai_len, sizeof(*hai));
		return rc;
	}

	data_len = hai->hai_len - sizeof(*hai);
	if (data_len < 3) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR, "migrate info is too short");
		return rc;
	}
	hsm_param = hai->hai_data;

	/* Ensure the string is terminated */
	if (hsm_param[data_len-1] != 0) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "migrate info is not NUL terminated");
		return rc;
	}

	/* Deserialize migrate request */
	jroot = json_decode(hsm_param);
	if (!jroot) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR, "received invalid JSON: %s",
				  hsm_param);
		return rc;
	}

	errors = 0;

	if (jroot->tag != JSON_OBJECT) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "JSON message root is not an object");
		goto fail;
	}

	lsp_obj = json_find_member(jroot, "lsp");
	if (!lsp_obj) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR, "Cannot find lsp member");
		goto fail;
	}

	osts_count = 0;
	osts_obj = json_find_member(lsp_obj, "osts");
	if (osts_obj) {
		json_foreach(obj, osts_obj)
			osts_count++;
	}

	param = calloc(1, offsetof(typeof(*param), lsp_osts[osts_count]));
	if (param == NULL) {
		rc = -errno;
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "cannot allocate new stripe param");
		goto fail;
	}

	set_string(lsp_obj, "pool", json_pool, errors);
	if (json_pool && json_pool[0] != 0) {
		if (strlen(json_pool) > LOV_MAXPOOLNAME)
			errors++;
		else
			param->lsp_pool = strdup(json_pool);
	}
	set_number(lsp_obj, "stripe_size", param->lsp_stripe_size, errors);
	set_number(lsp_obj, "stripe_pattern", param->lsp_stripe_pattern,
		   errors);
	set_number(lsp_obj, "stripe_offset", param->lsp_stripe_offset, errors);
	set_number(lsp_obj, "stripe_pattern", param->lsp_stripe_pattern,
		   errors);

	if (osts_obj) {
		param->lsp_is_specific = true;
		param->lsp_stripe_count = osts_count;
		i = 0;
		json_foreach(obj, osts_obj) {
			if (obj->tag != JSON_NUMBER_INT)
				errors++;
			else
				param->lsp_osts[i] = obj->number_int;
			i++;
		}
	} else {
		param->lsp_is_specific = false;
		set_number(lsp_obj, "stripe_count", param->lsp_stripe_count,
			   errors);
	}

	set_number(jroot, "mdt_index", json_mdt_index, errors);

	if (errors) {
		rc = -EINVAL;
		llapi_err_noerrno(LLAPI_MSG_ERROR,
				  "invalid values in migrate JSON string: %s",
				  hsm_param);
		return rc;
	}

	json_delete(jroot);

	*param_out = param;
	*mdt_index = json_mdt_index;

	return 0;

fail:
	json_delete(jroot);
	return rc;
}

/** Start processing an HSM action.
 * Should be called by copytools just before starting handling a request.
 * It could be skipped if copytool only want to directly report an error,
 * \see llapi_hsm_action_end().
 *
 * \param hcp                Opaque action handle to be passed to
 *                           llapi_hsm_action_progress and llapi_hsm_action_end.
 * \param ct                 Copytool handle acquired at registration.
 * \param hai                The hsm_action_item describing the request.
 * \param restore_mdt_index  On restore: MDT index where to create the volatile
 *                           file. Use -1 for default.
 * \param restore_open_flags On restore: volatile file creation mode. Use
 *                           O_LOV_DELAY_CREATE to manually set the LOVEA
 *                           afterwards.
 * \param is_error           Whether this call is just to report an error.
 *
 * \return 0 on success.
 */
int llapi_hsm_action_begin(struct hsm_copyaction_private **phcp,
			   const struct hsm_copytool_private *ct,
			   const struct hsm_action_item *hai,
			   int restore_mdt_index, int restore_open_flags,
			   bool is_error)
{
	struct hsm_copyaction_private	*hcp;
	int				 rc;
	struct llapi_stripe_param *sparam = NULL;

	hcp = calloc(1, sizeof(*hcp));
	if (hcp == NULL)
		return -ENOMEM;

	hcp->data_fd = -1;
	hcp->ct_priv = ct;
	hcp->copy.hc_hai = *hai;
	hcp->copy.hc_hai.hai_len = sizeof(*hai);

	if (is_error)
		goto ok_out;

	if (hai->hai_action == HSMA_RESTORE) {
		rc = ct_md_getattr(hcp->ct_priv, &hai->hai_fid, &hcp->stat);
		if (rc < 0)
			goto err_out;

		rc = create_restore_volatile(hcp, restore_mdt_index,
					     restore_open_flags);
		if (rc < 0)
			goto err_out;
	} else if (hai->hai_action == HSMA_MIGRATE) {
		/* Convert the stripe info back to a struct
		 * llapi_stripe_param and extract the MDT index */
		int mdt_index;

		rc = hai_to_stripe_info(hai, &sparam, &mdt_index);
		if (rc < 0)
			goto err_out;

		/* Create volatile destination */
		rc = llapi_create_volatile_param(hcp->ct_priv->mnt,
						 mdt_index,
						 0, S_IRUSR | S_IWUSR,
						 sparam);
		if (rc < 0)
			goto err_out;

		hcp->data_fd = rc;

		rc = llapi_fd2fid(hcp->data_fd, &hcp->copy.hc_hai.hai_dfid);
		if (rc < 0)
			goto err_out;

		free(sparam->lsp_pool);
		free(sparam);
		sparam = NULL;
	}

	rc = ioctl(ct->mnt_fd, LL_IOC_HSM_COPY_START, &hcp->copy);
	if (rc < 0) {
		rc = -errno;
		goto err_out;
	}

	llapi_hsm_log_ct_progress(&hcp, hai, CT_START, 0, 0);

ok_out:
	hcp->magic = CP_PRIV_MAGIC;
	*phcp = hcp;
	return 0;

err_out:
	if (hcp->data_fd >= 0)
		close(hcp->data_fd);

	if (sparam) {
		free(sparam->lsp_pool);
		free(sparam);
	}
	free(hcp);

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
int llapi_hsm_action_end(struct hsm_copyaction_private **phcp,
			 const struct hsm_extent *he, int hp_flags, int errval)
{
	struct hsm_copyaction_private	*hcp;
	struct hsm_action_item		*hai;
	int				 rc;

	if (phcp == NULL || *phcp == NULL || he == NULL)
		return -EINVAL;

	hcp = *phcp;

	if (hcp->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	hai = &hcp->copy.hc_hai;

	if (hai->hai_action == HSMA_RESTORE && errval == 0) {
		struct timeval tv[2];

		/* Set {a,m}time of volatile file to that of original. */
		tv[0].tv_sec = hcp->stat.st_atime;
		tv[0].tv_usec = 0;
		tv[1].tv_sec = hcp->stat.st_mtime;
		tv[1].tv_usec = 0;
		if (futimes(hcp->data_fd, tv) < 0) {
			errval = -errno;
			goto end;
		}

		rc = fsync(hcp->data_fd);
		if (rc < 0) {
			errval = -errno;
			goto end;
		}
	}

end:
	/* In some cases, like restore, 2 FIDs are used.
	 * Set the right FID to use here. */
	if (hai->hai_action == HSMA_ARCHIVE || hai->hai_action == HSMA_RESTORE)
		hai->hai_fid = hai->hai_dfid;

	/* Fill the last missing data that will be needed by
	 * kernel to send a hsm_progress. */
	hcp->copy.hc_flags  = hp_flags;
	hcp->copy.hc_errval = abs(errval);

	hcp->copy.hc_hai.hai_extent = *he;

	rc = ioctl(hcp->ct_priv->mnt_fd, LL_IOC_HSM_COPY_END, &hcp->copy);
	if (rc) {
		rc = -errno;
		goto err_cleanup;
	}

	llapi_hsm_log_ct_progress(&hcp, hai, CT_FINISH, 0, 0);

err_cleanup:
	if (!(hcp->data_fd < 0))
		close(hcp->data_fd);

	free(hcp);
	*phcp = NULL;

	return rc;
}

/** Notify a progress in processing an HSM action.
 * \param hdl[in,out]   handle returned by llapi_hsm_action_start.
 * \param he[in]        the range of copied data (for copy actions).
 * \param total[in]     the expected total of copied data (for copy actions).
 * \param hp_flags[in]  HSM progress flags.
 * \return 0 on success.
 */
int llapi_hsm_action_progress(struct hsm_copyaction_private *hcp,
			      const struct hsm_extent *he, __u64 total,
			      int hp_flags)
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

	if (hai->hai_action == HSMA_MIGRATE) {
		/* Progress is made on the fid */
		hp.hp_fid = hai->hai_fid;
	} else {
		/* Progress is made on the data fid */
		hp.hp_fid = hai->hai_dfid;
	}
	hp.hp_extent = *he;

	rc = ioctl(hcp->ct_priv->mnt_fd, LL_IOC_HSM_PROGRESS, &hp);
	if (rc < 0)
		rc = -errno;

	llapi_hsm_log_ct_progress(&hcp, hai, CT_RUNNING, total, he->length);

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

	if (hai->hai_action != HSMA_RESTORE &&
		hai->hai_action != HSMA_ARCHIVE &&
		hai->hai_action != HSMA_MIGRATE)
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
	int fd;

	if (hcp->magic != CP_PRIV_MAGIC)
		return -EINVAL;

	if (hai->hai_action == HSMA_ARCHIVE) {
		return ct_open_by_fid(hcp->ct_priv, &hai->hai_dfid,
				O_RDONLY | O_NOATIME | O_NOFOLLOW | O_NONBLOCK);
	} else if (hai->hai_action == HSMA_RESTORE ||
		   hai->hai_action == HSMA_MIGRATE) {
		fd = dup(hcp->data_fd);

		return fd < 0 ? -errno : fd;
	} else {
		return -EINVAL;
	}
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
	struct hsm_user_import	 hui;
	int			 fd;
	int			 rc = 0;

	if (stripe_pattern == 0)
		stripe_pattern = LOV_PATTERN_RAID0;

	/* Create a non-striped file */
	fd = llapi_file_open_pool(dst, O_CREAT | O_WRONLY, st->st_mode,
				  stripe_size, stripe_offset, stripe_count,
				  stripe_pattern | LOV_PATTERN_F_RELEASED,
				  pool_name);
	if (fd < 0) {
		llapi_error(LLAPI_MSG_ERROR, fd,
			    "cannot create '%s' for import", dst);
		return fd;
	}

	/* Get the new fid in Lustre. Caller needs to use this fid
	   from now on. */
	rc = llapi_fd2fid(fd, newfid);
	if (rc != 0) {
		llapi_error(LLAPI_MSG_ERROR, rc,
			    "cannot get fid of '%s' for import", dst);
		goto out_unlink;
	}

	hui.hui_uid = st->st_uid;
	hui.hui_gid = st->st_gid;
	hui.hui_mode = st->st_mode;
	hui.hui_size = st->st_size;
	hui.hui_archive_id = archive;
	hui.hui_atime = st->st_atime;
	hui.hui_atime_ns = st->st_atim.tv_nsec;
	hui.hui_mtime = st->st_mtime;
	hui.hui_mtime_ns = st->st_mtim.tv_nsec;
	rc = ioctl(fd, LL_IOC_HSM_IMPORT, &hui);
	if (rc != 0) {
		rc = -errno;
		llapi_error(LLAPI_MSG_ERROR, rc, "cannot import '%s'", dst);
		goto out_unlink;
	}

out_unlink:
	if (fd >= 0)
		close(fd);
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
int llapi_hsm_state_get_fd(int fd, struct hsm_user_state *hus)
{
	int rc;

	rc = ioctl(fd, LL_IOC_HSM_STATE_GET, hus);
	/* If error, save errno value */
	rc = rc ? -errno : 0;

	return rc;
}

/**
 * Return the current HSM states and HSM requests related to file pointed by \a
 * path.
 *
 * see llapi_hsm_state_get_fd() for args use and return
 */
int llapi_hsm_state_get(const char *path, struct hsm_user_state *hus)
{
	int fd;
	int rc;

	fd = open(path, O_RDONLY | O_NONBLOCK);
	if (fd < 0)
		return -errno;

	rc = llapi_hsm_state_get_fd(fd, hus);

	close(fd);
	return rc;
}

/**
 * Set HSM states of file pointed by \a fd
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
int llapi_hsm_state_set_fd(int fd, __u64 setmask, __u64 clearmask,
			   __u32 archive_id)
{
	struct hsm_state_set	 hss;
	int			 rc;

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

	return rc;
}

/**
 * Set HSM states of file pointed by \a path.
 *
 * see llapi_hsm_state_set_fd() for args use and return
 */
int llapi_hsm_state_set(const char *path, __u64 setmask, __u64 clearmask,
			__u32 archive_id)
{
	int fd;
	int rc;

	fd = open(path, O_WRONLY | O_LOV_DELAY_CREATE | O_NONBLOCK);
	if (fd < 0)
		return -errno;

	rc = llapi_hsm_state_set_fd(fd, setmask, clearmask, archive_id);

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
