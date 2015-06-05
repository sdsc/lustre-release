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

/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *   Copyright(c) 2015 FUJITSU LIMITED.
 *   All rights reserved.
 */
/* written by Hiroya Nozaki <nozaki.hiroya@jp.fujitsu.com> */

/*
 * For iosvc
 */

#include "cl_object.h"

#define IOSVC_THREAD_MIN   0
#define IOSVC_THREAD_MAX 256

enum iosvc_state {
	IOSVC_NOT_READY = 0,
	IOSVC_READY,
	IOSVC_TERMINATE,
	IOSVC_NR
};

struct iosvc_item {
	struct file           *ii_file;
	struct range_lock     *ii_rlock;
	struct list_head       ii_inode_list;
	struct completion      ii_io_finished;
	atomic_t               ii_refcount;
	pid_t                  ii_syscall_pid;
	/* to calcuate the number of pages iosvc used */
	struct cl_io_rw_common ii_rw_common;
};

enum iosvc_thread_avail {
	ITI_STARTING = 0,
	ITI_UNAVAIL,
	ITI_AVAIL,
	ITI_STOPPING,
	ITI_NR
};

struct iosvc_thread_info {
	unsigned int       iti_avail;
	pid_t              iti_pid;
	void		  *iti_cookie;
	int	           iti_refcount;
	struct lu_env     *iti_env;
	struct iosvc_item *iti_item;
	wait_queue_head_t  iti_waitq;
	rwlock_t           iti_rwlock;

	struct iovec      *iti_iovec;
	struct iovec      *iti_iovec_frame;
	struct iovec      *iti_iovec_src;
	unsigned long      iti_nrsegs;
	__kernel_size_t    iti_max_len;

	/* FUTURE WORK: adds CPT support */
	unsigned short     iti_cpu;

	struct completion  iti_completion;
	int                iti_rc;
};

int  iosvc_copy_iovec(struct iosvc_thread_info *iti);
void iosvc_free_iovec(struct iovec *iov, unsigned long nr_segs,
		      __kernel_size_t max_len, int clear);
int  iosvc_manage_iovec(const struct lu_env *env,
			struct iovec *iov, struct iovec *src,
			   unsigned long nr_segs, __kernel_size_t max_len);
