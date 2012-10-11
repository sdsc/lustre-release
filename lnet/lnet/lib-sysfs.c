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
 * along with Portals; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
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
#define LNET_SYSFS_MAXARGS 10
#define LNET_SYSFS_MAX_PID 10
#define LNET_SYSFS_MESSAGE_FORMAT "%s:%s"

static char *lnet_sysfs_read_route;
static char *lnet_sysfs_read_ni;
static struct mutex lnet_sysfs_mutex;  /* To protect above two buffers. */

static char lnet_sysfs_route_pid[LNET_SYSFS_MAX_PID];
static char lnet_sysfs_ni_pid[LNET_SYSFS_MAX_PID];

static unsigned int
line2args(char *line, char **argv, int maxargs)
{
	char *arg;
	unsigned int i = 0;

	while ((arg = strsep(&line, " \t")) &&
	       (i <= maxargs)) {
		argv[i] = arg;
		i++;
	}
	return i;
}

static ssize_t
lnet_sysfs_route_write(struct device *dev, struct device_attribute *attr,
		       const char *buf, size_t count)
{
	lnet_nid_t	nid;
	lnet_nid_t	hold_nid = 0;
	__u32		net = 0;
	__u32		hold_net = 0;
	__u32		get_hops = 0;
	long		hops = 1;
	int		alive;
	int		i;
	int		cmd_start;
	char		*argv[LNET_SYSFS_MAXARGS];
	unsigned int	num_args;
	int		rc;
	char		*tmpstr;
	char		*pos;

	/* If we have not added a NI yet, we cannot do anything with routes.
	   Doing so will freeze the kernel as some link lists have not been
	   initialized yet. */
	if (the_lnet.ln_refcount == 0) {
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
			 "Fail: Configure a NI before playing with routes.\n");
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		return -EPROTO;
	}

	/* Parse out the pid from the beginning of message. */
	for (cmd_start = 0; (cmd_start < sizeof(lnet_sysfs_route_pid)) &&
	     (cmd_start < count) && (buf[cmd_start] != ':'); cmd_start++)
		lnet_sysfs_route_pid[cmd_start] = buf[cmd_start];
	if ((cmd_start >= sizeof(lnet_sysfs_route_pid)) ||
	    (buf[cmd_start] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_route, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_route_pid, "0");
		return -EPROTO;
	}
	lnet_sysfs_route_pid[cmd_start] = '\0';
	cmd_start++;

	/* Get a local copy of the buffer we can alter. */
	LIBCFS_ALLOC(tmpstr, count - cmd_start + 1);
	if (tmpstr == NULL)
		return -ENOMEM;
	memcpy(tmpstr, buf + cmd_start, count - cmd_start);

	/* Parse out the parameters */
	num_args = line2args(tmpstr, argv, LNET_SYSFS_MAXARGS);
	if (num_args == 0) {
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
			  "Fail: Unable to parse arguments\n");
		CERROR(lnet_sysfs_read_route);
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		rc = -EPROTO;
		goto failure;
	}
	if (num_args >= 2) {
		net = libcfs_str2net(argv[1]);
		if (net == LNET_NIDNET(LNET_NID_ANY)) {
			LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Invalid net parameter: %s\n",
				  argv[1]);
			CERROR(lnet_sysfs_read_route);
			LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
			goto early_out;
		}
	}
	if (num_args >= 3) {
		nid = libcfs_str2nid(argv[2]);
		if (nid == LNET_NID_ANY) {
			LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Invalid NID parameter: %s\n",
				  argv[2]);
			CERROR(lnet_sysfs_read_route);
			LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
			goto early_out;
		}
	}
	if (num_args >= 4) {
		hops = simple_strtol(argv[3], NULL, 10);
		if (hops < 1)
			hops = 1;
	}

	/* Act on the command. */
	switch (tmpstr[0]) {
	case 'A':
		/* Add a route command. */
		if (num_args < 3) {
			LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 3 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
			break;
		}
		rc = lnet_add_route(net, (unsigned int)hops, nid, 1);
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		if (rc) {
			scnprintf(lnet_sysfs_read_route,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to add route: errno = %d\n",
				  rc);
			CERROR(lnet_sysfs_read_route);
		} else {
			strncpy(lnet_sysfs_read_route, "Success: Route added\n",
				LNET_SYSFS_MAX_BUF);
		}
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		break;
	case 'D':
		/* Delete a route command. */
		if (num_args < 3) {
			LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 3 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
			break;
		}
		rc = lnet_del_route(net, nid);
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		if (rc) {
			scnprintf(lnet_sysfs_read_route,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to delete route: errno = %d\n",
				  rc);
			CERROR(lnet_sysfs_read_route);
		} else {
			strncpy(lnet_sysfs_read_route,
				"Success: Route deleted\n",
				LNET_SYSFS_MAX_BUF);
		}
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		break;
	case 'S':
		/* Show route information. */
		if (num_args < 2) {
			LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 2 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
			break;
		}
		hold_net = net;
		hold_nid = nid;
	case 'L':
		/* List all routes. */
		pos = lnet_sysfs_read_route;
		*pos = '\0';
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		for (i = 0; ; i++) {
			rc = lnet_get_route(i, &net, &get_hops, &nid, &alive);
			if (rc != 0)
				break;
			if (tmpstr[0] == 'L')
				pos += scnprintf(pos, lnet_sysfs_read_route +
					LNET_SYSFS_MAX_BUF - pos,
					"%18s %32s %s\n", libcfs_net2str(net),
					libcfs_nid2str(nid), alive ?
					"up" : "down");
			else if ((net == hold_net) &&
				   ((num_args < 3) ||
				    ((num_args >= 3) && (nid == hold_nid))))
				pos += scnprintf(pos, lnet_sysfs_read_route +
					LNET_SYSFS_MAX_BUF - pos,
					"Net: %s\nGw: %s\nHops: %u\nState: %s\n\n",
					libcfs_net2str(net),
					libcfs_nid2str(nid), get_hops, alive ?
					"up" : "down");
		}
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		break;
	default:
		LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
		scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
			  "Fail: Invalid command letter: %c\n", tmpstr[0]);
		CERROR(lnet_sysfs_read_route);
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		rc = -EPROTO;
		goto failure;
	}

early_out:
	LIBCFS_FREE(tmpstr, count - cmd_start + 1);
	return count;

failure:
	LIBCFS_FREE(tmpstr, count - cmd_start + 1);
	return rc;
}

static ssize_t
lnet_sysfs_route_read(struct device *dev, struct device_attribute *attr,
		      char *buf)
{
	int rc;

	LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
	rc = scnprintf(buf, LNET_SYSFS_MAX_BUF, LNET_SYSFS_MESSAGE_FORMAT,
		       lnet_sysfs_route_pid, lnet_sysfs_read_route);
	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
	return rc;
}

static ssize_t
lnet_sysfs_ni_write(struct device *dev, struct device_attribute *attr,
		    const char *buf, size_t count)
{
	int i;

	/* Parse out the pid from the beginning of message. */
	for (i = 0; (i < sizeof(lnet_sysfs_ni_pid)) && (i < count) &&
	     (buf[i] != ':'); i++)
		lnet_sysfs_ni_pid[i] = buf[i];
	if ((i >= sizeof(lnet_sysfs_ni_pid)) || (buf[i] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_ni, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_ni_pid, "0");
		return -EPROTO;
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
	int rc;

	LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
	rc = scnprintf(buf, LNET_SYSFS_MAX_BUF, LNET_SYSFS_MESSAGE_FORMAT,
		       lnet_sysfs_ni_pid, lnet_sysfs_read_ni);
	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
	return rc;
}

DEVICE_ATTR(route, 644, lnet_sysfs_route_read, lnet_sysfs_route_write);
DEVICE_ATTR(ni, 644, lnet_sysfs_ni_read, lnet_sysfs_ni_write);

void
lnet_sysfs_init(void)
{
	int rc = 0;
	cfs_psdev_t *our_device;

	/* get the device structure used by libcfs */
	our_device = cfs_get_device();
	if (our_device == NULL) {
		CWARN("Unable to get device from libcfs\n");
		return;
	}

	/* initialize all read buffers */
	LIBCFS_ALLOC(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF);
	if (lnet_sysfs_read_route == NULL) {
		CWARN("Unable to allocate sysfs read buffer\n");
		goto failed;
	}
	LIBCFS_ALLOC(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);
	if (lnet_sysfs_read_ni == NULL) {
		CWARN("Unable to allocate sysfs read buffer\n");
		goto failed;
	}

	mutex_init(&lnet_sysfs_mutex);

	LNET_MUTEX_LOCK(&lnet_sysfs_mutex);
	strncpy(lnet_sysfs_read_route, "No action taken\n", LNET_SYSFS_MAX_BUF);
	lnet_sysfs_read_route[LNET_SYSFS_MAX_BUF-1] = '\0';
	strncpy(lnet_sysfs_read_ni, "No action taken\n", LNET_SYSFS_MAX_BUF);
	lnet_sysfs_read_ni[LNET_SYSFS_MAX_BUF-1] = '\0';
	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);

	rc = device_create_file(our_device->this_device, &dev_attr_route);
	if (rc)
		CWARN("Unable to create sysfs file: route, rc = %d\n", rc);

	rc = device_create_file(our_device->this_device, &dev_attr_ni);
	if (rc)
		CWARN("Unable to create sysfs file: ni, rc = %d\n", rc);
	return;

failed:
	if (lnet_sysfs_read_route) {
		LIBCFS_FREE(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF);
		lnet_sysfs_read_route = NULL;
	}
	if (lnet_sysfs_read_ni) {
		LIBCFS_FREE(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);
		lnet_sysfs_read_ni = NULL;
	}
}

void
lnet_sysfs_fini(void)
{
	cfs_psdev_t *our_device;

	/* get the device structure used by libcfs */
	our_device = cfs_get_device();
	LASSERT(our_device != NULL);

	device_remove_file(our_device->this_device, &dev_attr_route);
	device_remove_file(our_device->this_device, &dev_attr_ni);

	if (lnet_sysfs_read_route) {
		LIBCFS_FREE(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF);
		lnet_sysfs_read_route = NULL;
	}
	if (lnet_sysfs_read_ni) {
		LIBCFS_FREE(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);
		lnet_sysfs_read_ni = NULL;
	}
}
#endif
