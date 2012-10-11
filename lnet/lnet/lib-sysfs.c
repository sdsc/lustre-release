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

static ssize_t
lnet_sysfs_route_write(struct device *dev, struct device_attribute *attr,
		       const char *buf, size_t count)
{
	lnet_nid_t	nid;
	lnet_nid_t	hold_nid = 0;
	__u32		net = 0;
	__u32		hold_net = 0;
	__u32		get_hops = 0;
	unsigned long	hops = 1;
	int		alive;
	int		i;
	int		cmd_start;
	char		*argv[MAXARGS];
	int		num_args;
	int		rc;
	char		*tmpstr;
	char		*pos;
	int		tiny;
	int		small;
	int		large;

	/* Parse out the pid from the begining of message. */
	for (cmd_start = 0; (cmd_start < sizeof(lnet_sysfs_route_pid)) &&
	     (cmd_start < count) && (buf[cmd_start] != ':'); cmd_start++)
		lnet_sysfs_route_pid[cmd_start] = buf[cmd_start];
	if ((cmd_start >= sizeof(lnet_sysfs_route_pid)) ||
	    (buf[cmd_start] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_route, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_route_pid, "0");
		return count;
	}
	lnet_sysfs_route_pid[cmd_start] = '\0';
	cmd_start++;

	/* If we have not addded an NI yet, we cannot do anything with routes.
	   Doing so will freeze the kernel as some link lists have not been
	   initialized yet. */
	if (the_lnet.ln_refcount == 0) {
		scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
			 "Fail: Configure an NI before playing with routes.\n");
		return count;
	}

	/* Get a local copy of the buffer we can alter. */
	LIBCFS_ALLOC(tmpstr, count - cmd_start + 1);
	if (tmpstr == NULL)
		return -ENOMEM;
	memcpy(tmpstr, buf + cmd_start, count);
	tmpstr[count - cmd_start] = '\0';

	/* Parse out the parameters */
	num_args = line2args(tmpstr, argv, MAXARGS);
	if (num_args < 0) {
		LIBCFS_FREE(tmpstr, count - cmd_start + 1);
		return -EFAULT;
	}
	if (buf[0] != 'B') {
		if (num_args >= 2)
			net = libcfs_str2net(argv[1]);
	if (num_args >= 3)
		nid = libcfs_str2nid(argv[2]);
	if (num_args >= 4) {
		hops = simple_strtoul(argv[3], NULL, 10);
		if (hops < 1)
			hops = 1;
		}
	}

	/* Act on the command. */
	switch (tmpstr[0]) {
	case 'A':
		if (num_args < 3) {
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 3 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			break;
		}
		rc = lnet_add_route(net, (unsigned int)hops, nid, 1);
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
		break;
	case 'D':
		if (num_args < 3) {
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 3 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			break;
		}
		rc = lnet_del_route(net, nid);
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
		break;
	case 'S':
		if (num_args < 2) {
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Less than 2 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			break;
		}
		hold_net = net;
		hold_nid = nid;
	case 'L':
		pos = lnet_sysfs_read_route;
		*pos = '\0';
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
		break;
	case 'B':
		if ((num_args < 4) && (num_args != 1)) {
			scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
				  "Fail: Not 4 or 1 args to route sysfs: %d\n",
				  num_args);
			CERROR(lnet_sysfs_read_route);
			break;
		}
		if (num_args == 1) {
			int idx;

			/* Just querying the route buffer info. */
			pos = lnet_sysfs_read_route;
			*pos = '\0';
			pos += scnprintf(pos, lnet_sysfs_read_route +
					 LNET_SYSFS_MAX_BUF - pos,
					 "%5s %5s %7s %7s\n",
					 "pages", "count", "credits", "min");
			if (the_lnet.ln_rtrpools == NULL)
				break;
			for (idx = 0; idx < LNET_NRBPOOLS; idx++) {
				lnet_rtrbufpool_t *rbp;

				lnet_net_lock(LNET_LOCK_EX);
				cfs_percpt_for_each(rbp, i,
						    the_lnet.ln_rtrpools) {
					pos += scnprintf(pos,
						lnet_sysfs_read_route +
						LNET_SYSFS_MAX_BUF -  pos,
						"%5d %5d %7d %7d\n",
						rbp[idx].rbp_npages,
						rbp[idx].rbp_nbuffers,
						rbp[idx].rbp_credits,
						rbp[idx].rbp_mincredits);
				}
				lnet_net_unlock(LNET_LOCK_EX);
			}
		} else {
			tiny = simple_strtoul(argv[1], NULL, 10);
			if (tiny < 0)
				tiny = 0;
			small = simple_strtoul(argv[2], NULL, 10);
			if (small < 0)
				small = 0;
			large = simple_strtoul(argv[3], NULL, 10);
			if (large < 0)
				large = 0;
			rc = lnet_adjust_rtrpools(tiny, small, large);
			if (rc) {
				scnprintf(lnet_sysfs_read_route,
				 LNET_SYSFS_MAX_BUF,
				 "Fail: Unable to change buffers: errno = %d\n",
				 rc);
				CERROR(lnet_sysfs_read_route);
			} else {
				strncpy(lnet_sysfs_read_route,
					"Success: Routing buffers changed\n",
					LNET_SYSFS_MAX_BUF);
			}
		}
		break;
	default:
		scnprintf(lnet_sysfs_read_route, LNET_SYSFS_MAX_BUF,
			  "Fail: Invalid command letter: %c\n", tmpstr[0]);
		CERROR(lnet_sysfs_read_route);
		break;
	}
	LIBCFS_FREE(tmpstr, count - cmd_start + 1);
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
