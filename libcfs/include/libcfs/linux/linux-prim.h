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
 * Copyright (c) 2012, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/linux/linux-prim.h
 *
 * Basic library routines.
 */

#ifndef __LIBCFS_LINUX_CFS_PRIM_H__
#define __LIBCFS_LINUX_CFS_PRIM_H__

#ifndef __LIBCFS_LIBCFS_H__
#error Do not #include this file directly. #include <libcfs/libcfs.h> instead
#endif

#ifndef __KERNEL__
#error This include is only for kernel use.
#endif

#include <linux/sched.h>
#include <linux/wait.h>

/*
 * Wait Queue
 */
#ifndef HAVE___ADD_WAIT_QUEUE_EXCLUSIVE
static inline void __add_wait_queue_exclusive(wait_queue_head_t *q,
					      wait_queue_t *wait)
{
	wait->flags |= WQ_FLAG_EXCLUSIVE;
	__add_wait_queue(q, wait);
}
#endif /* HAVE___ADD_WAIT_QUEUE_EXCLUSIVE */

/**
 * wait_queue_t of Linux (version < 2.6.34) is a FIFO list for exclusively
 * waiting threads, which is not always desirable because all threads will
 * be waken up again and again, even user only needs a few of them to be
 * active most time. This is not good for performance because cache can
 * be polluted by different threads.
 *
 * LIFO list can resolve this problem because we always wakeup the most
 * recent active thread by default.
 *
 * NB: please don't call non-exclusive & exclusive wait on the same
 * waitq if add_wait_queue_exclusive_head is used.
 */
#define add_wait_queue_exclusive_head(waitq, link)			\
{									\
	unsigned long flags;						\
									\
	spin_lock_irqsave(&((waitq)->lock), flags);			\
	__add_wait_queue_exclusive(waitq, link);			\
	spin_unlock_irqrestore(&((waitq)->lock), flags);		\
}

#endif
