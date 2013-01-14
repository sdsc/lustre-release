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
 * Copyright (c) 2012 Intel Corporation.
 */
/*
 * lustre/mdd/mdd_lfsck.h
 *
 * Shared definitions and declarations for the LFSCK.
 *
 * Author: Fan, Yong <fan.yong@intel.com>
 */

#ifndef _MDD_LFSCK_H
# define _MDD_LFSCK_H

#include <lustre/lustre_lfsck_user.h>

struct md_lfsck {
	struct mutex	      ml_mutex;
	spinlock_t	      ml_lock;
	struct ptlrpc_thread  ml_thread;
	struct dt_object     *ml_bookmark_obj;
	struct dt_object     *ml_it_obj;
	__u32		      ml_new_scanned;
	/* Arguments for low layer iteration. */
	__u32		      ml_args;

	/* Raw value for LFSCK speed limit. */
	__u32		      ml_speed_limit;

	/* Schedule for every N objects. */
	__u32		      ml_sleep_rate;

	/* Sleep N jiffies for each schedule. */
	__u32		      ml_sleep_jif;
	__u16		      ml_version;
	unsigned int	      ml_paused:1; /* The lfsck is paused. */
};

struct lfsck_linkea_header {
	cfs_list_t	llh_list;

	/* How many linkea entries have been verified. */
	int		llh_count;
	__u8		llh_flags;
};

struct lfsck_linkea_entry {
	cfs_list_t	lle_link;
	struct lu_fid	lle_fid;
	unsigned short	lle_namelen;
	char		lle_name[0];
};

#endif /* _MDD_LFSCK_H */
