#include "mdt_coordinator_scan_policies.h"

/**
 * Get a policy from its name
 */
int scan_policy_name2type(char *policy_name)
{
	int csp_idx;

	for (csp_idx = 0; csp_idx < CSP_MAX; csp_idx++) {
		struct cdt_scan_policy *policy = scan_policies[csp_idx];

		if (strncmp(policy_name, policy->csp_name,
			    strlen(policy->csp_name)) == 0)
			return csp_idx;
	}
	return -EINVAL;
}

/* Procfs definitions */

/**
 * Read handler for the "hsm/priority_poly" proc file
 *
 * Prints the name of the policy and then calls the policy's own read handler
 * if there is one.
 */
static int cdt_scan_policy_seq_show(struct seq_file *m, void *v)
{
	struct coordinator *cdt = m->private;
	int rc = 0;

	ENTRY;
	down_read(&cdt->cdt_scan_policy_rwsem);

	LASSERT(cdt->cdt_scan_policy->csp_name != NULL);
	seq_printf(m, "%s\n", cdt->cdt_scan_policy->csp_name);

	up_read(&cdt->cdt_scan_policy_rwsem);
	RETURN(rc);
}

/**
 * Write handler for the "hsm/priority_poly" proc file
 *
 * Allow a user to set a new scan_policy. If the user is trying to do something
 * else, call the policy's own write handler.
 */
static ssize_t cdt_scan_policy_seq_write(struct file *file,
					 const char __user *buff, size_t count,
					 loff_t *off)
{
	struct seq_file *m = file->private_data;
	struct coordinator *cdt = m->private;
	int new_policy;
	char *kbuff;
	int rc;

	ENTRY;
	if (count > PROC_BUF_SIZE)
		RETURN(-EINVAL);

	OBD_ALLOC(kbuff, count + 1);
	if (kbuff == NULL)
		RETURN(-ENOMEM);

	if (copy_from_user(kbuff, buff, count)) {
		rc = -EFAULT;
		goto out_free;
	}

	kbuff[count] = '\0';

	new_policy = scan_policy_name2type(kbuff);
	if (new_policy < 0) {
		rc = -EINVAL;
		goto out_free;
	}

	down_write(&cdt->cdt_scan_policy_rwsem);

	cdt_scan_policy_exit(cdt);
	rc = cdt_scan_policy_init(cdt, new_policy);

	up_write(&cdt->cdt_scan_policy_rwsem);

out_free:
	OBD_FREE(kbuff, count + 1);

	RETURN(rc < 0 ? rc : count);
}

LPROC_SEQ_FOPS(cdt_scan_policy);

static struct lprocfs_vars lprocfs_cdt_scan_policy[] = {
	{ .name = "scan_policy",
	  .fops = &cdt_scan_policy_fops,
	},
	{ 0 }
};

int cdt_scan_policy_procfs_init(struct coordinator *cdt)
{
	int i;

	for (i = 0; i < CSP_MAX; i++) {
		struct cdt_scan_policy *policy = scan_policies[i];
		int rc;

		if (policy->csp_lprocfs_vars == NULL)
			continue;

		rc = lprocfs_add_vars(cdt->cdt_proc_dir,
				      policy->csp_lprocfs_vars, cdt);
		if (rc < 0)
			return rc;
	}

	return lprocfs_add_vars(cdt->cdt_proc_dir, lprocfs_cdt_scan_policy,
				cdt);
}
