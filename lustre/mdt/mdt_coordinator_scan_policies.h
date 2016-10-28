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
};

struct cdt_scan_policy {
	char				*csp_name;
	enum cdt_scan_policy_id		 csp_id;
	struct scan_policy_operations	*csp_ops;
	void				*csp_private_data;
	struct lprocfs_vars		*csp_lprocfs_vars;
};

extern struct cdt_scan_policy csp_priority;

static struct cdt_scan_policy *scan_policies[] = {
	[CSP_PRIORITY] = &csp_priority,
};

static const int CSP_MAX = sizeof(scan_policies) / sizeof(scan_policies[0]);

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

int scan_policy_name2type(char *policy_name);
int cdt_scan_policy_procfs_init(struct coordinator *cdt);

#endif
