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
 * Copyright (c) 2011, 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * cl code shared between vvp and liblustre (and other Lustre clients in the
 * future).
 *
 */
#define DEBUG_SUBSYSTEM S_LLITE
#include <obd_class.h>
#include <obd_support.h>
#include <obd.h>
#include <cl_object.h>

#include "llite_internal.h"

/* Initialize the default and maximum LOV EA and cookie sizes.  This allows
 * us to make MDS RPCs with large enough reply buffers to hold the
 * maximum-sized (= maximum striped) EA and cookie without having to
 * calculate this (via a call into the LOV + OSCs) each time we make an RPC. */
static int cl_init_ea_size(struct obd_export *md_exp, struct obd_export *dt_exp)
{
	u32 val_size;
	u32 max_easize;
	u32 def_easize;
	int rc;
	ENTRY;

	val_size = sizeof(max_easize);
	rc = obd_get_info(NULL, dt_exp, sizeof(KEY_MAX_EASIZE), KEY_MAX_EASIZE,
			  &val_size, &max_easize);
	if (rc != 0)
		RETURN(rc);

	val_size = sizeof(def_easize);
	rc = obd_get_info(NULL, dt_exp, sizeof(KEY_DEFAULT_EASIZE),
			  KEY_DEFAULT_EASIZE, &val_size, &def_easize);
	if (rc != 0)
		RETURN(rc);

	/* default cookiesize is 0 because from 2.4 server doesn't send
	 * llog cookies to client. */
	CDEBUG(D_HA, "updating def/max_easize: %d/%d\n",
	       def_easize, max_easize);

	rc = md_init_ea_size(md_exp, max_easize, def_easize);
	RETURN(rc);
}

/**
 * This function is used as an upcall-callback hooked by liblustre and llite
 * clients into obd_notify() listeners chain to handle notifications about
 * change of import connect_flags. See llu_fsswop_mount() and
 * lustre_common_fill_super().
 */
int cl_ocd_update(struct obd_device *host,
                  struct obd_device *watched,
                  enum obd_notify_event ev, void *owner, void *data)
{
        struct lustre_client_ocd *lco;
        struct client_obd        *cli;
        __u64 flags;
        int   result;

        ENTRY;
	if (!strcmp(watched->obd_type->typ_name, LUSTRE_OSC_NAME) &&
	    watched->obd_set_up && !watched->obd_stopping) {
                cli = &watched->u.cli;
                lco = owner;
                flags = cli->cl_import->imp_connect_data.ocd_connect_flags;
                CDEBUG(D_SUPER, "Changing connect_flags: "LPX64" -> "LPX64"\n",
                       lco->lco_flags, flags);
		mutex_lock(&lco->lco_lock);
                lco->lco_flags &= flags;
                /* for each osc event update ea size */
                if (lco->lco_dt_exp)
                        cl_init_ea_size(lco->lco_md_exp, lco->lco_dt_exp);

		mutex_unlock(&lco->lco_lock);
                result = 0;
        } else {
		CERROR("unexpected notification from %s %s"
		       "(setup:%d,stopping:%d)!\n",
		       watched->obd_type->typ_name,
		       watched->obd_name, watched->obd_set_up,
		       watched->obd_stopping);
		result = -EINVAL;
        }
        RETURN(result);
}

#define GROUPLOCK_SCOPE "grouplock"

int cl_get_grouplock(struct cl_object *obj, unsigned long gid, int nonblock,
		     struct ll_grouplock *lg)
{
        struct lu_env          *env;
        struct cl_io           *io;
        struct cl_lock         *lock;
        struct cl_lock_descr   *descr;
        __u32                   enqflags;
	__u16                   refcheck;
        int                     rc;

        env = cl_env_get(&refcheck);
        if (IS_ERR(env))
                return PTR_ERR(env);

	io = vvp_env_thread_io(env);
        io->ci_obj = obj;

	rc = cl_io_init(env, io, CIT_MISC, io->ci_obj);
	if (rc != 0) {
		cl_io_fini(env, io);
		cl_env_put(env, &refcheck);
		/* Does not make sense to take GL for released layout */
		if (rc > 0)
			rc = -ENOTSUPP;
		return rc;
	}

	lock = vvp_env_lock(env);
	descr = &lock->cll_descr;
        descr->cld_obj = obj;
        descr->cld_start = 0;
        descr->cld_end = CL_PAGE_EOF;
        descr->cld_gid = gid;
        descr->cld_mode = CLM_GROUP;

	enqflags = CEF_MUST | (nonblock ? CEF_NONBLOCK : 0);
	descr->cld_enq_flags = enqflags;

	rc = cl_lock_request(env, io, lock);
	if (rc < 0) {
		cl_io_fini(env, io);
		cl_env_put(env, &refcheck);
		return rc;
	}

	lg->lg_env = env;
	lg->lg_io = io;
	lg->lg_lock = lock;
	lg->lg_gid = gid;

	return 0;
}

void cl_put_grouplock(struct ll_grouplock *lg)
{
	struct lu_env  *env  = lg->lg_env;
	struct cl_io   *io   = lg->lg_io;
	struct cl_lock *lock = lg->lg_lock;

	LASSERT(lg->lg_env != NULL);
	LASSERT(lg->lg_gid != 0);

	cl_lock_release(env, lock);
	cl_io_fini(env, io);
	cl_env_put(env, NULL);
}

static enum cl_lock_mode cl_mode_user_to_kernel(enum lock_mode_user mode)
{
	switch (mode) {
	case MODE_READ_USER:
		return CLM_READ;
	case MODE_WRITE_USER:
		return CLM_WRITE;
	default:
		return -EINVAL;
	}
}

static const char *const user_lockname[] = LOCK_MODE_NAMES;

/* Used to allow the upper layers of the client to request an LDLM lock
 * without doing an actual read or write.
 *
 * Used for the lock ahead ioctl to request locks in advance of IO.
 *
 * \param[in] inode	inode for the file this lock request is on
 * \param[in] start	start of the lock extent (bytes)
 * \param[in] end	end of the lock extent (bytes)
 * \param[in] mode	lock mode (read, write)
 * \param[in] flags	flags
 *
 * \retval 0		on success (CIT_MISC type should never have a
 *			positive io->ci_result)
 * \retval negative	negative errno on error
 */
int cl_request_lock(struct file *file, struct llapi_lu_ladvise *ladvise)
{
	struct lu_env		*env = NULL;
	struct cl_io		*io  = NULL;
	struct cl_lock		*lock = NULL;
	struct cl_lock_descr	*descr = NULL;
	struct dentry		*dentry = file->f_path.dentry;
	struct inode		*inode = dentry->d_inode;
	enum cl_lock_mode	cl_mode;
	off_t			start = ladvise->lla_start;
	off_t			end = ladvise->lla_end;
	int                     result;
	__u16                     refcheck;

	ENTRY;

	CDEBUG(D_VFSTRACE, "Lock request: file=%.*s, inode=%p, mode=%s "
	       "start=%llu, end=%llu\n", dentry->d_name.len,
	       dentry->d_name.name, dentry->d_inode,
	       user_lockname[ladvise->lla_requestlock_mode], (__u64) start,
	       (__u64) end);

	/* Get IO environment */
	result = cl_io_get(inode, &env, &io, &refcheck);
	if (result > 0) {
again:
		result = cl_io_init(env, io, CIT_MISC, io->ci_obj);

		if (result > 0) {
			/*
			 * nothing to do for this io. This currently happens
			 * when stripe sub-object's are not yet created.
			 */
			result = io->ci_result;
		} else if (result == 0) {
			lock = vvp_env_lock(env);
			descr = &lock->cll_descr;

			cl_mode = cl_mode_user_to_kernel(
						ladvise->lla_requestlock_mode);
			if (cl_mode < 0) {
				cl_io_fini(env, io);
				cl_env_put(env, &refcheck);
				RETURN(cl_mode);
			}

			descr->cld_obj   = io->ci_obj;
			/* Convert byte offsets to pages */
			descr->cld_start = cl_index(io->ci_obj, start);
			descr->cld_end   = cl_index(io->ci_obj, end);
			descr->cld_mode  = cl_mode;
			/* CEF_MUST is used because we do not want to convert a
			 * lock ahead request to a lockless lock */
			descr->cld_enq_flags = CEF_MUST | CEF_LOCK_NO_EXPAND |
					       CEF_SPECULATIVE |
					       ladvise->lla_peradvice_flags;

			result = cl_lock_request(env, io, lock);

			/* On success, we need to release the cl_lock */
			if (result >= 0)
				cl_lock_release(env, lock);
		}
		cl_io_fini(env, io);
		if (unlikely(io->ci_need_restart))
			goto again;
		cl_env_put(env, &refcheck);
	}

	/* -ECANCELED indicates a matching lock with a different extent
	 * was already present, and -EEXIST indicates a matching lock
	 * on exactly the same extent was already present.
	 * We convert them to positive values for userspace to make
	 * recognizing true errors easier. */
	if (result == -ECANCELED)
		result = LRL_RESULT_DIFFERENT;
	else if (result == -EEXIST)
		result = LRL_RESULT_SAME;

	RETURN(result);
}
