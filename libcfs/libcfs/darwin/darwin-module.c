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
 */

#include <mach/mach_types.h>
#include <string.h>
#include <sys/file.h>
#include <sys/conf.h>
#include <miscfs/devfs/devfs.h>

#define DEBUG_SUBSYSTEM S_LNET
#include <libcfs/libcfs.h>

int libcfs_ioctl_data_adjust(struct libcfs_ioctl_data *data)
{
	char *buf = &data->ioc_bulk[0];

	if (libcfs_ioctl_is_invalid(data)) {
		CERROR("PORTALS: ioctl not correctly formatted\n");
		RETURN(-EINVAL);
	}

	if (data->ioc_inllen1) {
		data->ioc_inlbuf1 = buf;
		buf += size_round(data->ioc_inllen1);
	}

	if (data->ioc_inllen2) {
		data->ioc_inlbuf2 = buf;
	}

	RETURN(0);
}

int libcfs_ioctl_getdata_len(void *arg, __u32 *buf_len)
{
	int err;
	struct libcfs_ioctl_hdr hdr;
	ENTRY;

	memcpy(&hdr, arg, sizeof(hdr));

	if (hdr.ioc_version != LIBCFS_IOCTL_VERSION) {
		CERROR("PORTALS: version mismatch kernel vs application\n");
		RETURN(-EINVAL);
	}

	*buf_len = hdr.ioc_len;
	RETURN(0);
}

int libcfs_ioctl_getdata(char *buf, __u32 buf_len, void *arg)
{
	int err;
	ENTRY;

	memcpy(buf, arg, buf_len);

	RETURN(0);
}

int libcfs_ioctl_popdata(void *arg, void *data, int size)
{
	/* 
	 * system call will copy out ioctl arg to user space
	 */
	memcpy(arg, data, size);
	return 0;
}

extern struct cfs_psdev_ops		libcfs_psdev_ops;
struct libcfs_device_userstate		*mdev_state[16];

static int
libcfs_psdev_open(dev_t dev, int flags, int devtype, struct proc *p)
{
	struct	libcfs_device_userstate *mstat = NULL;
	int	rc = 0;
	int	devid;
	devid = minor(dev);

	if (devid > 16) return (ENXIO);

	if (libcfs_psdev_ops.p_open != NULL)
		rc = -libcfs_psdev_ops.p_open(0, &mstat);
	else
		rc = EPERM;
	if (rc == 0)
		mdev_state[devid] = mstat;
	return rc;
}

static int
libcfs_psdev_close(dev_t dev, int flags, int mode, struct proc *p)
{
	int	devid;
	devid = minor(dev);
	int	rc = 0;

	if (devid > 16) return (ENXIO);

	if (libcfs_psdev_ops.p_close != NULL)
		rc = -libcfs_psdev_ops.p_close(0, mdev_state[devid]);
	else
		rc = EPERM;
	if (rc == 0)
		mdev_state[devid] = NULL;
	return rc;
}

static int
libcfs_ioctl (dev_t dev, u_long cmd, caddr_t arg, int flag, struct proc *p)
{
	int rc = 0;
        struct cfs_psdev_file    pfile;
	int     devid;
	devid = minor(dev);
	
	if (devid > 16) return (ENXIO);

	if (!is_suser())
		return (EPERM);
	
	pfile.off = 0;
	pfile.private_data = mdev_state[devid];

	if (libcfs_psdev_ops.p_ioctl != NULL)
		rc = -libcfs_psdev_ops.p_ioctl(&pfile, cmd, (void *)arg);
	else
		rc = EPERM;
	return rc;
}

static struct cdevsw libcfs_devsw =
{
	.d_open     = libcfs_psdev_open,
	.d_close    = libcfs_psdev_close,
	.d_read     = eno_rdwrt,
	.d_write    = eno_rdwrt,
	.d_ioctl    = libcfs_ioctl,
	.d_stop     = eno_stop,
	.d_reset    = eno_reset,
	.d_ttys     = NULL,
	.d_select   = eno_select,
	.d_mmap     = eno_mmap,
	.d_strategy = eno_strat,
	.d_getc     = eno_getc,
	.d_putc     = eno_putc,
	.d_type     = 0
};

struct miscdevice libcfs_dev = {
	-1,
	NULL,
	"lnet",
	&libcfs_devsw,
	NULL
};

extern spinlock_t trace_cpu_serializer;
extern void cfs_sync_init(void);
extern void cfs_sync_fini(void);
extern int cfs_sysctl_init(void);
extern void cfs_sysctl_fini(void);
extern int cfs_mem_init(void);
extern int cfs_mem_fini(void);
extern void raw_page_death_row_clean(void);
extern void cfs_thread_agent_init(void);
extern void cfs_thread_agent_fini(void);
extern void cfs_symbol_init(void);
extern void cfs_symbol_fini(void);

int libcfs_arch_init(void)
{
	cfs_sync_init();
	cfs_sysctl_init();
	cfs_mem_init();
	cfs_thread_agent_init();
	cfs_symbol_init();

	spin_lock_init(&trace_cpu_serializer);

	return 0;
}

void libcfs_arch_cleanup(void)
{
	spin_lock_done(&trace_cpu_serializer);

	cfs_symbol_fini();
	cfs_thread_agent_fini();
	cfs_mem_fini();
	cfs_sysctl_fini();
	cfs_sync_fini();
}
