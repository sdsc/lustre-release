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
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/obdclass/jobid_cache.c
 *
 * Author: Lai Siyao <lsy@clusterfs.com>
 * Author: Fan Yong <fanyong@clusterfs.com>
 * Author: Oleg Drokin <oleg.drokin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/string.h>
#include <linux/errno.h>
#include <linux/version.h>
#include <asm/uaccess.h>
#include <linux/slab.h>

#include <libcfs/libcfs.h>
#include <libcfs/lucache.h>
#include <obd.h>
#include <obd_class.h>
#include <obd_support.h>
#include <lustre_lib.h>

#include "jobid_internal.h"

struct upcall_cache *obd_jobid_upcall;

static void jobid_cache_entry_init(struct upcall_cache_entry *entry,
				    void *unused)
{
	entry->u.jobid.jce_uc_entry = entry;
}

static void jobid_cache_entry_free(struct upcall_cache *cache,
				   struct upcall_cache_entry *entry)
{
	if (entry->u.jobid.jce_jobid)
		OBD_FREE(entry->u.jobid.jce_jobid, JOBSTATS_JOBID_SIZE);
}

static int jobid_cache_do_upcall(struct upcall_cache *cache,
				 struct upcall_cache_entry *entry)
{
	char keystr[32];
	char *argv[] = {
		[0] = cache->uc_upcall,
		[1] = cache->uc_name,
		[2] = keystr,
		[3] = NULL
	};
	char *envp[] = {
		[0] = "HOME=/",
		[1] = "PATH=/sbin:/usr/sbin",
		[2] = NULL
	};
	struct timeval start, end;
	int rc;
	ENTRY;

	read_lock(&cache->uc_upcall_rwlock);
	CDEBUG(D_INFO, "The upcall is: '%s'\n", cache->uc_upcall);

	if (unlikely(!strcmp(cache->uc_upcall, "/NONE")))
		GOTO(out, rc = -ENOENT);

	argv[0] = cache->uc_upcall;
	snprintf(keystr, sizeof(keystr), LPU64, entry->ue_key);

	do_gettimeofday(&start);
	rc = call_usermodehelper(argv[0], argv, envp, 1);
	do_gettimeofday(&end);
	if (rc < 0) {
		CERROR("%s: error invoking upcall %s %s %s: rc %d; "
		       "check /proc/fs/lustre/jobid_upcall, "
		       "time %ldus\n",
		       cache->uc_name, argv[0], argv[1], argv[2], rc,
		       cfs_timeval_sub(&end, &start, NULL));
	} else {
		CDEBUG(D_CACHE, "%s: invoked upcall %s %s %s, time %ldus\n",
		       cache->uc_name, argv[0], argv[1], argv[2],
		       cfs_timeval_sub(&end, &start, NULL));
		rc = 0;
	}
	EXIT;
out:
	read_unlock(&cache->uc_upcall_rwlock);
	return rc;
}

static int jobid_cache_parse_downcall(struct upcall_cache *cache,
				       struct upcall_cache_entry *entry,
				       void *args)
{
	struct jobid_cache_entry *jobid = &entry->u.jobid;
	char *val = args;
	ENTRY;

	if (jobid == NULL)
		RETURN(-ENOENT);

	if (jobid->jce_jobid == NULL)
		OBD_ALLOC(jobid->jce_jobid, JOBSTATS_JOBID_SIZE);

	strncpy(jobid->jce_jobid, val, JOBSTATS_JOBID_SIZE);

	RETURN(0);
}

struct jobid_cache_entry *jobid_cache_get(__u64 pid)
{
	struct upcall_cache_entry *entry;

	if (!obd_jobid_upcall)
		return ERR_PTR(-ENOENT);

	entry = upcall_cache_get_entry(obd_jobid_upcall, pid, NULL);
	if (IS_ERR(entry))
		return ERR_PTR(PTR_ERR(entry));
	else if (unlikely(!entry))
		return ERR_PTR(-ENOENT);
	else
		return &entry->u.jobid;
}

void jobid_cache_put(struct jobid_cache_entry *jobid)
{
	if (!obd_jobid_upcall)
		return;

	LASSERT(jobid);
	upcall_cache_put_entry(obd_jobid_upcall, jobid->jce_uc_entry);
}

struct upcall_cache_ops jobid_cache_upcall_cache_ops = {
	.init_entry     = jobid_cache_entry_init,
	.free_entry     = jobid_cache_entry_free,
	.do_upcall      = jobid_cache_do_upcall,
	.parse_downcall = jobid_cache_parse_downcall,
};

void jobid_flush_cache(__u64 pid)
{
	if (!obd_jobid_upcall)
		return;

	if (pid < 0)
		upcall_cache_flush_idle(obd_jobid_upcall);
	else
		upcall_cache_flush_one(obd_jobid_upcall, pid, NULL);
}
