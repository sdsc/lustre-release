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
 * Copyright (c) 2014, Intel Corporation.
 */
/*
 *
 * Network Request Scheduler (NRS) First-in First-out (FIFO) policy
 *
 */

#ifndef _LUSTRE_NRS_FIFO_H
#define _LUSTRE_NRS_FIFO_H
#include <lustre_nrs.h>

/* \name fifo
 *
 * FIFO policy
 *
 * This policy is a logical wrapper around previous, non-NRS functionality.
 * It dispatches RPCs in the same order as they arrive from the network. This
 * policy is currently used as the fallback policy, and the only enabled policy
 * on all NRS heads of all PTLRPC service partitions.
 * @{
 */

/**
 * Private data structure for the FIFO policy
 */
struct nrs_fifo_head {
	/**
	 * Resource object for policy instance.
	 */
	struct ptlrpc_nrs_resource	fh_res;
	/**
	 * List of queued requests.
	 */
	struct list_head		fh_list;
	/**
	 * For debugging purposes.
	 */
	__u64				fh_sequence;
};

struct nrs_fifo_req {
	struct list_head	fr_list;
	__u64			fr_sequence;
};

/** @} fifo */
#endif
