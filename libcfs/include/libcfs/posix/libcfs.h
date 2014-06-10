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
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/posix/libcfs.h
 *
 * Defines for posix userspace.
 *
 * Author: Robert Read <rread@sun.com>
 */

#ifndef __LIBCFS_POSIX_LIBCFS_H__
#define __LIBCFS_POSIX_LIBCFS_H__

#include <errno.h>
#include <sys/errno.h>
#include <string.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <sys/signal.h>
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <getopt.h>
#include <signal.h>
#include <pwd.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <ctype.h>
#include <stdbool.h>
#include <limits.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/user.h>
#include <sys/vfs.h>
#include <stdint.h>
#include <sys/mount.h>
#include <mntent.h>

#include <libcfs/list.h>
#include <libcfs/user-time.h>
#include <libcfs/user-prim.h>
#include <libcfs/user-mem.h>
#include <libcfs/user-lock.h>
#include <libcfs/user-tcpip.h>
#include <libcfs/user-bitops.h>

#define do_gettimeofday(tv) gettimeofday(tv, NULL);
typedef unsigned long long cfs_cycles_t;

/* Userpace byte flipping */
#include <endian.h>
#include <byteswap.h>
#define __swab16(x) bswap_16(x)
#define __swab32(x) bswap_32(x)
#define __swab64(x) bswap_64(x)
#define __swab16s(x) do {*(x) = bswap_16(*(x));} while (0)
#define __swab32s(x) do {*(x) = bswap_32(*(x));} while (0)
#define __swab64s(x) do {*(x) = bswap_64(*(x));} while (0)
#if __BYTE_ORDER == __LITTLE_ENDIAN
# define le16_to_cpu(x) (x)
# define cpu_to_le16(x) (x)
# define le32_to_cpu(x) (x)
# define cpu_to_le32(x) (x)
# define le64_to_cpu(x) (x)
# define cpu_to_le64(x) (x)

# define be16_to_cpu(x) bswap_16(x)
# define cpu_to_be16(x) bswap_16(x)
# define be32_to_cpu(x) bswap_32(x)
# define cpu_to_be32(x) bswap_32(x)
# define be64_to_cpu(x) ((__u64)bswap_64(x))
# define cpu_to_be64(x) ((__u64)bswap_64(x))
#elif __BYTE_ORDER == __BIG_ENDIAN
# define le16_to_cpu(x) bswap_16(x)
# define cpu_to_le16(x) bswap_16(x)
# define le32_to_cpu(x) bswap_32(x)
# define cpu_to_le32(x) bswap_32(x)
# define le64_to_cpu(x) ((__u64)bswap_64(x))
# define cpu_to_le64(x) ((__u64)bswap_64(x))

# define be16_to_cpu(x) (x)
# define cpu_to_be16(x) (x)
# define be32_to_cpu(x) (x)
# define cpu_to_be32(x) (x)
# define be64_to_cpu(x) (x)
# define cpu_to_be64(x) (x)
#else /*  __BYTE_ORDER == __BIG_ENDIAN */
# error "Unknown byte order"
#endif /* __BYTE_ORDER != __BIG_ENDIAN */

/* initial pid  */
#define LUSTRE_LNET_PID          12345

#define ENTRY_NESTING_SUPPORT (1)
#define ENTRY_NESTING   do {;} while (0)
#define EXIT_NESTING   do {;} while (0)
#define __current_nesting_level() (0)

#endif
