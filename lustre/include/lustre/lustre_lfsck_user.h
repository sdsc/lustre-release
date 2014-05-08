/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * lustre/include/lustre/lustre_lfsck_user.h
 *
 * Lustre LFSCK userspace interfaces.
 *
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#ifndef _LUSTRE_LFSCK_USER_H
# define _LUSTRE_LFSCK_USER_H
# include <lustre/lustre_user.h>

enum lfsck_param_flags {
	/* Reset LFSCK iterator position to the device beginning. */
	LPF_RESET       = 0x0001,

	/* Exit when fail. */
	LPF_FAILOUT     = 0x0002,

	/* Dryrun mode, only check without modification */
	LPF_DRYRUN      = 0x0004,

	/* LFSCK runs on all targets. */
	LPF_ALL_TGT	= 0x0008,

	/* Broadcast the command to other MDTs. Only valid on the sponsor MDT */
	LPF_BROADCAST	= 0x0010,

	/* Handle orphan objects. */
	LPF_ORPHAN	= 0x0020,
};

enum lfsck_type {
	/* For MDT-OST consistency check/repair. */
	LT_LAYOUT	= 0x0001,

	/* For MDT-MDT consistency check/repair. */
	LT_DNE  	= 0x0002,

	/* For FID-in-dirent and linkEA consistency check/repair. */
	LT_NAMESPACE	= 0x0004,
};

#define LFSCK_VERSION_V1	1
#define LFSCK_VERSION_V2	2

#define LFSCK_TYPES_ALL		((uint16_t)(~0))
#define LFSCK_TYPES_DEF		((uint16_t)0)
#define LFSCK_TYPES_SUPPORTED	(LT_LAYOUT | LT_NAMESPACE)

#define LFSCK_SPEED_NO_LIMIT	0
#define LFSCK_SPEED_LIMIT_DEF	LFSCK_SPEED_NO_LIMIT
#define LFSCK_ASYNC_WIN_DEFAULT 1024
#define LFSCK_ASYNC_WIN_MAX	((uint16_t)(~0))

enum lfsck_start_valid {
	LSV_SPEED_LIMIT 	= 0x00000001,
	LSV_ERROR_HANDLE	= 0x00000002,
	LSV_DRYRUN		= 0x00000004,
	LSV_ASYNC_WINDOWS	= 0x00000008,
};

/* Arguments for starting lfsck. */
struct lfsck_start {
	/* Which arguments are valid, see 'enum lfsck_start_valid'. */
	uint32_t   ls_valid;

	/* How many items can be scanned at most per second. */
	uint32_t   ls_speed_limit;

	/* For compatibility between user space tools and kernel service. */
	uint16_t   ls_version;

	/* Which LFSCK components to be (have been) started. */
	uint16_t   ls_active;

	/* Flags for the LFSCK, see 'enum lfsck_param_flags'. */
	uint16_t   ls_flags;

	/* The windows size for async requests pipeline. */
	uint16_t   ls_async_windows;
};

struct lfsck_stop {
	uint32_t	ls_status;
	uint16_t	ls_flags;
	uint16_t	ls_padding_1; /* For 64-bits aligned. */
	uint64_t	ls_padding_2;
};

#endif /* _LUSTRE_LFSCK_USER_H */
