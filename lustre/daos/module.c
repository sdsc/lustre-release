/**
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/**
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/**
 * This file is part of lustre/DAOS
 *
 * lustre/daos/module.c
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#include <libcfs/libcfs.h>
#include <lnet/lnetctl.h>
#include <lustre/lustre_build_version.h>
#include <daos/daos_lib.h>
#include "daos_internal.h"

static int			eq_eventn = DAOS_EQ_EVN_DEF;
CFS_MODULE_PARM(eq_eventn, "i", int, 0444, "Max number of events per EQ");

static int			can_abort = 0;
CFS_MODULE_PARM(can_abort, "i", int, 0444, "Allow event abort");

struct daos_module		the_daos;

struct daos_module_params	the_daos_params = {
	.dmp_eq_eventn		= &eq_eventn,
	.dmp_can_abort		= &can_abort,
};

/* opening /dev/daos */
static int
daos_dev_open(struct inode * inode, struct file * file)
{
	struct daos_dev_env	*denv;

	ENTRY;

	OBD_ALLOC_PTR(denv);
	if (denv == NULL)
		return -ENOMEM;

	INIT_LIST_HEAD(&denv->den_eqs);
	spin_lock_init(&denv->den_lock);

	file->private_data = denv;

	PORTAL_MODULE_USE;
	RETURN(0);
}

/* closing /dev/daos */
static int
daos_dev_release(struct inode * inode, struct file * file)
{
	struct daos_dev_env	*denv;

	ENTRY;
	denv = file->private_data;

	if (denv == NULL)
		goto out;

	spin_lock(&denv->den_lock);
	while (!list_empty(&denv->den_eqs)) {
		struct daos_usr_eq_destroy eqd;
		struct daos_eq *eq;

		eq = list_entry(denv->den_eqs.next, struct daos_eq, eq_flink);
		daos_eq_handle(eq, &eqd.eqd_handle);
		spin_unlock(&denv->den_lock);

		eqd.eqd_force = 1;
		daos_eq_destroy(denv, &eqd);

		spin_lock(&denv->den_lock);
	}
	spin_unlock(&denv->den_lock);

	OBD_FREE_PTR(denv);
out:
	PORTAL_MODULE_UNUSE;
	RETURN(0);
}

int
daos_user_param_alloc(void *arg, struct daos_usr_param **uparam_pp)
{
	struct daos_usr_param	*up;
	struct daos_ioc_hdr	hdr;
	int			rc;

	rc = copy_from_user(&hdr, (void *)arg, sizeof(hdr));
	if (rc != 0) {
		CERROR("Failed to copy ioc parameter from userspace\n");
		return -EFAULT;
	}

	if (hdr.ih_version != DAOS_IOC_VERSION) {
		CERROR("Version mismatch kernel (%x) vs application (%x)\n",
		       DAOS_IOC_VERSION, hdr.ih_version);
		return -EINVAL;
	}

	if (hdr.ih_len > DAOS_IOC_BUF_LEN) {
		CERROR("User buffer len %d exceeds %d max buffer\n",
		       hdr.ih_len, DAOS_IOC_BUF_LEN);
		return -EINVAL;
	}

	if (hdr.ih_len < sizeof(struct daos_usr_data)) {
		CERROR("User buffer is too small for ioctl (%d)\n", hdr.ih_len);
		return -EINVAL;
	}

	OBD_ALLOC(up, offsetof(struct daos_usr_param, up_data[hdr.ih_len]));
	if (up == NULL)
		return -ENOMEM;

	atomic_set(&up->up_refcount, 1); /* 1 for caller */
	up->up_len = offsetof(struct daos_usr_param, up_data[hdr.ih_len]);

	rc = copy_from_user(&up->up_data[0], (void *)arg, hdr.ih_len);
	if (rc != 0) {
		CERROR("Failed to copy ioc parameter from userspace\n");
		GOTO(failed, rc = -EFAULT);
	}

	rc = daos_ioc_data_parse((struct daos_usr_data *)&up->up_data[0]);
	if (rc != 0) {
		CERROR("DAOS ioctl not correctly formatted\n");
		GOTO(failed, rc);
	}

	*uparam_pp = up;
	return 0;
failed:
	OBD_FREE(up, up->up_len);
	return rc;
}

static long
daos_dev_ioctl(struct file *filp, unsigned int cmd, unsigned long arg)
{
	struct daos_usr_param	*uparam;
	struct daos_dev_env	*denv;
	int			 rc = 0;

	ENTRY;

	denv = filp->private_data;
	rc = daos_user_param_alloc((void *)arg, &uparam);
	if (rc != 0) {
		CERROR("DAOS ioctl: data error\n");
		return rc;
	}

	switch (_IOC_TYPE(cmd)) {
	default:
		CERROR("DAOS ioctl: unknown cmd type: %d\n", _IOC_TYPE(cmd));
		rc = -EINVAL;
		break;
	case DAOS_IOC_EQ:
		rc = daos_eq_ioctl(denv, cmd, uparam);
		break;
	}

	daos_usr_param_put(uparam);
	RETURN(rc);
}

/* declare device for DAOS */
static struct file_operations daos_dev_fops = {
	.owner		= THIS_MODULE,
	.unlocked_ioctl	= daos_dev_ioctl,	/* unlocked ioctl */
	.open		= daos_dev_open,	/* open device */
	.release	= daos_dev_release,	/* release device */
};

/* modules setup */
struct miscdevice	daos_device = {
	.minor		= DAOS_DEV_MINOR,
	.name		= DAOS_DEV_NAME,
	.fops		= &daos_dev_fops,
};

static int
daos_params_check(void)
{
	if (*the_daos_params.dmp_eq_eventn <= 0)
		*the_daos_params.dmp_eq_eventn = DAOS_EQ_EVN_DEF;

	if (*the_daos_params.dmp_eq_eventn > DAOS_EQ_EVN_MAX)
		*the_daos_params.dmp_eq_eventn = DAOS_EQ_EVN_MAX;

	return 0;
}

static void
daos_finalize(void)
{
	if (the_daos.dm_dev_registered) {
		misc_deregister(&daos_device);
		the_daos.dm_dev_registered = 0;
	}
	daos_hhash_finalize();
}

static int
daos_initialize(void)
{
	int	rc;

	rc = daos_params_check();
	if (rc != 0)
		return rc;

	spin_lock_init(&the_daos.dm_lock);

	rc = daos_hhash_initialize();
	if (rc != 0)
		return rc;

	rc = misc_register(&daos_device);
	if (rc != 0)
		goto failed;

	the_daos.dm_dev_registered = 1;
	return 0;
failed:
	daos_finalize();
	return rc;
}

MODULE_AUTHOR("Intel Corporation. <http://hpdd.intel.com/>");
MODULE_DESCRIPTION("DAOS Build Version: " BUILD_VERSION);
MODULE_LICENSE("GPL");

cfs_module(daos, LUSTRE_VERSION_STRING, daos_initialize, daos_finalize);
