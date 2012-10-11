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
 * Copyright (c) 2012, Intel Corporation.
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <linux/kernel.h>
#include <linux/string.h>
#include <libcfs/libcfs.h>
#include <lnet/lib-lnet.h>

#if defined(__linux__) && defined(__KERNEL__)

#include <linux/device.h>

#define LNET_SYSFS_MAX_BUF (PAGE_SIZE)
#define MAXARGS 10
#define LNET_SYSFS_MAX_PID 10
#define LNET_SYSFS_MESSAGE_FORAMAT "%s:%s"

static char *lnet_sysfs_read_route;
static char *lnet_sysfs_read_ni;

static char lnet_sysfs_route_pid[LNET_SYSFS_MAX_PID];
static char lnet_sysfs_ni_pid[LNET_SYSFS_MAX_PID];

/* This routine will be used for parsing out the command arguments.  ifdef'ing
   it out now to prevent compilations errors. */
#if 0
static int
line2args(char *line, char **argv, int maxargs)
{
	char *arg;
	int i = 0;

	while ((arg = strsep(&line, " \t")) &&
	       (i <= maxargs)) {
		argv[i] = arg;
		i++;
	}
	return i;
}
#endif

static ssize_t
lnet_sysfs_route_write(struct device *dev, struct device_attribute *attr,
		       const char *buf, size_t count)
{
	int i;

	/* Parse out the pid from the begining of message. */
	for (i = 0; (i < sizeof(lnet_sysfs_route_pid)) && (i < count) &&
	     (buf[i] != ':'); i++)
		lnet_sysfs_route_pid[i] = buf[i];
	if ((i >= sizeof(lnet_sysfs_route_pid)) || (buf[i] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_route, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_route_pid, "0");
		return count;
	}
	lnet_sysfs_route_pid[i] = '\0';
	i++;

	/* Here is where processing of route commands will go. */

	return count;
}

static ssize_t
lnet_sysfs_route_read(struct device *dev, struct device_attribute *attr,
		      char *buf)
{
	return scnprintf(buf, LNET_SYSFS_MAX_BUF, LNET_SYSFS_MESSAGE_FORAMAT,
			 lnet_sysfs_route_pid, lnet_sysfs_read_route);
}

static ssize_t
lnet_sysfs_ni_write(struct device *dev, struct device_attribute *attr,
		    const char *buf, size_t count)
{
	int i;

	/* Parse out the pid from the begining of message. */
	for (i = 0; (i < sizeof(lnet_sysfs_ni_pid)) && (i < count) &&
	     (buf[i] != ':'); i++)
		lnet_sysfs_ni_pid[i] = buf[i];
	if ((i >= sizeof(lnet_sysfs_ni_pid)) || (buf[i] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_ni, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_ni_pid, "0");
		return count;
	}
	lnet_sysfs_ni_pid[i] = '\0';
	i++;

	/* Here is where processing of ni commands will go. */

	return count;
}

static ssize_t
lnet_sysfs_ni_read(struct device *dev, struct device_attribute *attr,
		   char *buf)
{
	return scnprintf(buf, LNET_SYSFS_MAX_BUF, LNET_SYSFS_MESSAGE_FORAMAT,
			 lnet_sysfs_ni_pid, lnet_sysfs_read_ni);
}

DEVICE_ATTR(route, 644, lnet_sysfs_route_read, lnet_sysfs_route_write);
DEVICE_ATTR(ni, 644, lnet_sysfs_ni_read, lnet_sysfs_ni_write);

void
lnet_sysfs_init(void)
{
	int rc = 0;

	/* create the route sysfs file */
	rc = cfs_sysfs_create_file(&dev_attr_route);
	if (rc)
		CERROR("Unable to create sysfs file: route, rc = %d\n", rc);

	/* create the ni sysfs file */
	rc = cfs_sysfs_create_file(&dev_attr_ni);
	if (rc)
		CERROR("Unable to create sysfs file: ni, rc = %d\n", rc);

	/* initialize all read buffers */
	LIBCFS_ALLOC(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF);
	LASSERT(lnet_sysfs_read_route != NULL);
	LIBCFS_ALLOC(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);
	LASSERT(lnet_sysfs_read_ni != NULL);

	strcpy(lnet_sysfs_read_route, "No action taken\n");
	strcpy(lnet_sysfs_read_ni, "No action taken\n");
}

void
lnet_sysfs_fini(void)
{
	LIBCFS_FREE(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF);
	LIBCFS_FREE(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);

	cfs_sysfs_remove_file(&dev_attr_route);
	cfs_sysfs_remove_file(&dev_attr_ni);
}

#else
void
lnet_sysfs_init(void)
{
}

void
lnet_sysfs_fini(void)
{
}
#endif
