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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/libcfs_time.h
 *
 * Time functions.
 *
 */

#ifndef __LIBCFS_TIME_H__
#define __LIBCFS_TIME_H__

/* Portable time API */

#define ONE_BILLION ((u_int64_t)1000000000)
#define ONE_MILLION 1000000

#ifndef __KERNEL__
# error This include is only for kernel use.
#endif

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/version.h>
#include <linux/jiffies.h>
#include <linux/time.h>
#include <asm/div64.h>

/*
 * internal helper function used by cfs_fs_time_before*()
 */
static inline unsigned long long __cfs_fs_time_flat(struct timespec *t)
{
	return (unsigned long long)t->tv_sec * NSEC_PER_SEC + t->tv_nsec;
}

/*
 * Generic kernel stuff
 */
static inline int cfs_fs_time_before(struct timespec *t1, struct timespec *t2)
{
	return __cfs_fs_time_flat(t1) <  __cfs_fs_time_flat(t2);
}

static inline int cfs_fs_time_beforeq(struct timespec *t1, struct timespec *t2)
{
	return __cfs_fs_time_flat(t1) <= __cfs_fs_time_flat(t2);
}

static inline long cfs_time_seconds(int seconds)
{
	return ((long)seconds) * msecs_to_jiffies(MSEC_PER_SEC);
}

static inline time_t cfs_duration_sec(long d)
{
	return d / msecs_to_jiffies(MSEC_PER_SEC);
}

static inline void cfs_duration_usec(long d, struct timeval *s)
{
#if (BITS_PER_LONG == 32)
	if (msecs_to_jiffies(MSEC_PER_SEC) > 4096) {
		__u64 t;

		s->tv_sec = d / msecs_to_jiffies(MSEC_PER_SEC);
		t = (d - (long)s->tv_sec *
		     msecs_to_jiffies(MSEC_PER_SEC)) * USEC_PER_SEC;
		do_div(t, msecs_to_jiffies(MSEC_PER_SEC));
		s->tv_usec = t;
	} else {
		s->tv_sec = d / msecs_to_jiffies(MSEC_PER_SEC);
		s->tv_usec = ((d - (long)s->tv_sec *
			       msecs_to_jiffies(MSEC_PER_SEC)) *
			       USEC_PER_SEC) / msecs_to_jiffies(MSEC_PER_SEC);
	}
#else
	s->tv_sec = d / msecs_to_jiffies(MSEC_PER_SEC);
	s->tv_usec = ((d - (long)s->tv_sec *
		       msecs_to_jiffies(MSEC_PER_SEC)) *
		       USEC_PER_SEC) / msecs_to_jiffies(MSEC_PER_SEC);
#endif
}

static inline void cfs_duration_nsec(long d, struct timespec *s)
{
#if (BITS_PER_LONG == 32)
	__u64 t;

	s->tv_sec = d / msecs_to_jiffies(MSEC_PER_SEC);
	t = (d - s->tv_sec * msecs_to_jiffies(MSEC_PER_SEC)) * NSEC_PER_SEC;
	do_div(t, msecs_to_jiffies(MSEC_PER_SEC));
	s->tv_nsec = t;
#else
	s->tv_sec = d / msecs_to_jiffies(MSEC_PER_SEC);
	s->tv_nsec = ((d - s->tv_sec * msecs_to_jiffies(MSEC_PER_SEC)) *
		      NSEC_PER_SEC) / msecs_to_jiffies(MSEC_PER_SEC);
#endif
}

static inline __u64 cfs_time_shift_64(int seconds)
{
	return (get_jiffies_64() + cfs_time_seconds(seconds));
}

/*
 * One jiffy
 */
#define CFS_TICK                (1)

/*
 * generic time manipulation functions.
 */
static inline long cfs_time_sub(unsigned long t1, unsigned long t2)
{
	return (unsigned long)(t1 - t2);
}

static inline unsigned long cfs_time_shift(int seconds)
{
	return (jiffies + cfs_time_seconds(seconds));
}

static inline long cfs_timeval_sub(struct timeval *large, struct timeval *small,
                                   struct timeval *result)
{
        long r = (long) (
                (large->tv_sec - small->tv_sec) * ONE_MILLION +
                (large->tv_usec - small->tv_usec));
        if (result != NULL) {
                result->tv_usec = r % ONE_MILLION;
                result->tv_sec = r / ONE_MILLION;
        }
        return r;
}

#define CFS_RATELIMIT(seconds)                                  \
({                                                              \
        /*                                                      \
         * XXX nikita: non-portable initializer                 \
         */                                                     \
        static time_t __next_message = 0;                       \
        int result;                                             \
                                                                \
	if (time_after(jiffies, __next_message))		\
                result = 1;                                     \
        else {                                                  \
                __next_message = cfs_time_shift(seconds);       \
                result = 0;                                     \
        }                                                       \
        result;                                                 \
})

/*
 * helper function similar to do_gettimeofday() of Linux kernel
 */
static inline void cfs_fs_timeval(struct timeval *tv)
{
	struct timespec time = CURRENT_TIME;

	tv->tv_sec  = time.tv_sec;
	tv->tv_usec = time.tv_nsec / 1000;
}

/*
 * return valid time-out based on user supplied one. Currently we only check
 * that time-out is not shorted than allowed.
 */
static inline long cfs_timeout_cap(long timeout)
{
        if (timeout < CFS_TICK)
                timeout = CFS_TICK;
        return timeout;
}

#endif
