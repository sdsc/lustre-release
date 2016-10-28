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
 * Copyright (c) 2016, Commissariat a l'Energie Atomique et aux Energies
 *                     Alternatives.
 */

#ifndef __MDT_COORDINATOR_SCAN_POLICIES_H
#define __MDT_COORDINATOR_SCAN_POLICIES_H

#include "mdt_internal.h"

#define PROC_BUF_SIZE 4096 /* Not bigger than a page size */

struct scan_policy_operations {
	/* Scan operations */
	int (*spo_init_policy)(struct coordinator *cdt);
	void (*spo_exit_policy)(struct coordinator *cdt);
	int (*spo_process_requests)(struct coordinator *cdt);
};

#define CSP_DEFAULT CSP_PRIORITY
enum cdt_scan_policy_id {
	CSP_PRIORITY = 0,
	CSP_MAX = 1,
};

struct cdt_scan_policy {
	char				*csp_name;
	enum cdt_scan_policy_id		 csp_id;
	struct scan_policy_operations	*csp_ops;
	void				*csp_private_data;
	struct lprocfs_vars		*csp_lprocfs_vars;
};

extern struct cdt_scan_policy csp_priority;

extern struct cdt_scan_policy *scan_policies[];

/**
 * Shortcut to cdt->cdt_scan_policy->csp_ops->spo_process_requests
 */
static inline int csp_process_requests(struct coordinator *cdt)
{
	return cdt->cdt_scan_policy->csp_ops->spo_process_requests(cdt);
}

/**
 * Set and initialize (if necessary) a scan policy
 */
static inline int cdt_scan_policy_init(struct coordinator *cdt,
				       enum cdt_scan_policy_id csp_type)
{
	*cdt->cdt_scan_policy = *scan_policies[csp_type];
	if (cdt->cdt_scan_policy->csp_ops->spo_init_policy != NULL)
		return cdt->cdt_scan_policy->csp_ops->spo_init_policy(cdt);
	return 0;
}

/**
 * Tear down (if necessary) a scan policy
 */
static inline void cdt_scan_policy_exit(struct coordinator *cdt)
{
	if (cdt->cdt_scan_policy->csp_ops->spo_exit_policy != NULL)
		cdt->cdt_scan_policy->csp_ops->spo_exit_policy(cdt);
}

int scan_policy_name2type(const char *policy_name);
int cdt_scan_policy_procfs_init(struct coordinator *cdt);

#endif
