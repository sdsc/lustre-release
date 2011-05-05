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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * GPL HEADER END
 */
/*
 * Copyright 2011 UT-Battelle. All rights reserved
 * Use is subject to license terms. Inspired by work
 * of Cray for Lustre 1.8.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Oracle, Inc.
 */

#ifndef _LIBCFS_FAIL_H
#define _LIBCFS_FAIL_H

extern unsigned long cfs_fail_loc;
extern unsigned int cfs_fail_val;

extern cfs_waitq_t cfs_race_waitq;
extern int cfs_race_state;

int __cfs_fail_check_set(__u32 id, __u32 value, int set);
int __cfs_fail_timeout_set(__u32 id, __u32 value, int ms, int set);

enum {  
        CFS_FAIL_LOC_NOSET      = 0,
        CFS_FAIL_LOC_ORSET      = 1,
        CFS_FAIL_LOC_RESET      = 2
};

/* Failure injection control */
#define CFS_FAIL_MASK_SYS    0x0000FF00
#define CFS_FAIL_MASK_LOC   (0x000000FF | CFS_FAIL_MASK_SYS)

#define CFS_FAILED_BIT       30
/* CFS_FAILED is 0x40000000 */
#define CFS_FAILED          (1 << CFS_FAILED_BIT)

#define CFS_FAIL_ONCE_BIT    31
/* CFS_FAIL_ONCE is 0x80000000 */
#define CFS_FAIL_ONCE       (1 << CFS_FAIL_ONCE_BIT)

/* The following flags aren't made to be combined */
#define CFS_FAIL_SKIP        0x20000000 /* skip N times then fail */
#define CFS_FAIL_SOME        0x10000000 /* only fail N times */
#define CFS_FAIL_RAND        0x08000000 /* fail 1/N of the times */
#define CFS_FAIL_USR1        0x04000000 /* user flag */

#define CFS_FAIL_PRECHECK(id) (cfs_fail_loc &&                                \
                              (cfs_fail_loc & CFS_FAIL_MASK_LOC) ==           \
                              ((id) & CFS_FAIL_MASK_LOC))

static inline int cfs_fail_check_set(__u32 id, __u32 value, int set)
{
        int ret = 0;
        if (unlikely(CFS_FAIL_PRECHECK(id) &&
            (ret = __cfs_fail_check_set(id, value, set)))) {
                CERROR("*** cfs_fail_loc=%x ***\n", id);
        }
        return ret;
}

/* If id hit cfs_fail_loc, return 1, otherwise return 0 */
#define CFS_FAIL_CHECK(id) \
        cfs_fail_check_set(id, 0, CFS_FAIL_LOC_NOSET)

/* If id hit cfs_fail_loc, cfs_fail_loc |= value and return 1,
 * otherwise return 0 */
#define CFS_FAIL_CHECK_ORSET(id, value) \
        cfs_fail_check_set(id, value, CFS_FAIL_LOC_ORSET)

/* If id hit cfs_fail_loc, cfs_fail_loc = value and return 1,
 * otherwise return 0 */
#define CFS_FAIL_CHECK_RESET(id, value) \
        cfs_fail_check_set(id, value, CFS_FAIL_LOC_RESET)

static inline int cfs_fail_timeout_set(__u32 id, __u32 value, int ms, int set)
{
        if (unlikely(CFS_FAIL_PRECHECK(id)))
                return __cfs_fail_timeout_set(id, value, ms, set);
        else
                return 0;
}

/* If id hit cfs_fail_loc, sleep for seconds or milliseconds */
#define CFS_FAIL_TIMEOUT(id, secs) \
        cfs_fail_timeout_set(id, 0, secs * 1000, CFS_FAIL_LOC_NOSET)

#define CFS_FAIL_TIMEOUT_MS(id, ms) \
        cfs_fail_timeout_set(id, 0, ms, CFS_FAIL_LOC_NOSET)

/* If id hit cfs_fail_loc, cfs_fail_loc |= value and
 * sleep seconds or milliseconds */
#define CFS_FAIL_TIMEOUT_ORSET(id, value, secs) \
        cfs_fail_timeout_set(id, value, secs * 1000, CFS_FAIL_LOC_ORSET)

#define CFS_FAIL_TIMEOUT_MS_ORSET(id, value, ms) \
        cfs_fail_timeout_set(id, value, ms, CFS_FAIL_LOC_ORSET)

#ifdef __KERNEL__
/* The idea here is to synchronise two threads to force a race. The
 * first thread that calls this with a matching fail_loc is put to
 * sleep. The next thread that calls with the same fail_loc wakes up
 * the first and continues. */
static inline void cfs_race(__u32 id)
{
        int rc;

        if (CFS_FAIL_PRECHECK(id)) {
                if (unlikely(__cfs_fail_check_set(id, 0, CFS_FAIL_LOC_NOSET))) {
                        cfs_race_state = 0;
                        CERROR("cfs_race id %x sleeping\n", id);
                        cfs_wait_event_interruptible(cfs_race_waitq, cfs_race_state != 0, rc);
                        CERROR("cfs_fail_race id %x awake\n", id);
                } else {
                        CERROR("cfs_fail_race id %x waking\n", id);
                        cfs_race_state = 1;
                        cfs_waitq_signal(&cfs_race_waitq);
                }
        }
}
#define CFS_RACE(id) cfs_race(id)
#else
/* sigh.  an expedient fix until CFS_RACE is fixed up */
#define CFS_RACE(foo) do {} while(0)
#endif

#endif /* _LIBCFS_FAIL_H */
