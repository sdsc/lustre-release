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
	int i;

	LNET_MUTEX_LOCK(&lnet_sysfs_mutex);

	/* Parse out the pid from the beginning of message. */
	for (i = 0; (i < sizeof(lnet_sysfs_route_pid)) && (i < count) &&
	     (buf[i] != ':'); i++)
		lnet_sysfs_route_pid[i] = buf[i];

	if ((i >= sizeof(lnet_sysfs_route_pid)) || (buf[i] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_route, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_route_pid, "0");
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		return count;
	}
	lnet_sysfs_route_pid[i] = '\0';
	i++;

	/* Here is where processing of route commands will go. */

	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
	return count;
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
	int		i;
	char		*argv[LNET_SYSFS_MAXARGS];
	int		num_args;
	int		rc;
	char		*tmpstr;
	__u32		net = 0;
	int		cmd_start;

	LNET_MUTEX_LOCK(&lnet_sysfs_mutex);

	/* Parse out the pid from the beginning of message. */
	for (cmd_start = 0; (cmd_start < sizeof(lnet_sysfs_ni_pid)) &&
	     (cmd_start < count) && (buf[cmd_start] != ':'); cmd_start++)
		lnet_sysfs_ni_pid[cmd_start] = buf[cmd_start];
	if ((cmd_start >= sizeof(lnet_sysfs_ni_pid)) ||
	    (buf[cmd_start] != ':')) {
		/* We did not hit a colon.  Message not formatted properly. */
		strcpy(lnet_sysfs_read_ni, "Fail: Bad message format\n");
		strcpy(lnet_sysfs_ni_pid, "0");
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		return -EPROTO;
	}
	lnet_sysfs_ni_pid[cmd_start] = '\0';
	cmd_start++;

	/* Get a local copy of the buffer we can alter. */
	LIBCFS_ALLOC(tmpstr, count - cmd_start + 1);
	if (tmpstr == NULL) {
		LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
		return -ENOMEM;
	}
	memcpy(tmpstr, buf + cmd_start, count - cmd_start);

	/* Parse out the parameters */
	num_args = line2args(tmpstr, argv, LNET_SYSFS_MAXARGS);
	if (num_args == 0) {
		scnprintf(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF,
			  "Fail: Unable to parse arguments\n");
		CERROR(lnet_sysfs_read_ni);
		rc = -EPROTO;
		goto net_failure;
	}
	if (tmpstr[0] != 'L') {
		if (num_args >= 2) {
			net = libcfs_str2net(argv[1]);
			if (net == LNET_NIDNET(LNET_NID_ANY)) {
				scnprintf(lnet_sysfs_read_ni,
					  LNET_SYSFS_MAX_BUF,
					  "Fail: Invalid net parameter: %s\n",
					  argv[1]);
				CERROR(lnet_sysfs_read_ni);
				goto early_out;
			}
		} else {
			scnprintf(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF,
				  "Fail: Missing <net> argument\n");
			CERROR(lnet_sysfs_read_ni);
			goto early_out;
		}
	}

	switch (tmpstr[0]) {
	case 'A':
		/* Add a Network Interface command. */
		/* Temporary: as long as we still support the networks module
		 * parameter, we want to re-use the parsing for that.  In the
		 * future, if that module support is removed, this can all be
		 * simplified. */
		/* Form a typical network config string by putting the
		 * interface (if present) in brackets after the network. */
		if (num_args >= 3) {
			char		tmp_buffer[256];
			char		*lnd_params[LNET_SYSFS_MAXARGS];
			unsigned int	num_lnd_params = 0;

			rc = scnprintf(tmp_buffer, sizeof(tmp_buffer), "%s(%s)",
				       argv[1], argv[2]);
			if ((rc < 0) || (rc >= sizeof(tmp_buffer))) {
				scnprintf(lnet_sysfs_read_ni,
					  LNET_SYSFS_MAX_BUF,
					  "Fail: Network and interface strings"
					  " too long\n");
				break;
			}
			for (i = 3; i < num_args; i++) {
				if (argv[i][0] == '[') {
					/* This must be SMP parameters.  Just
					   append to the buffer. */
					strlcat(tmp_buffer, argv[i],
						sizeof(tmp_buffer));
				} else {
					/* This must be an LND tunable
					   parameter. */
					lnd_params[num_lnd_params] = argv[i];
					num_lnd_params++;
				}
			}
			LNET_MUTEX_LOCK(&the_lnet.ln_api_mutex);
			rc = lnet_startup_lndnis(tmp_buffer, 0, lnd_params,
						 num_lnd_params);
			LNET_MUTEX_UNLOCK(&the_lnet.ln_api_mutex);
		} else {
			LNET_MUTEX_LOCK(&the_lnet.ln_api_mutex);
			rc = lnet_startup_lndnis(argv[1], 0, NULL, 0);
			LNET_MUTEX_UNLOCK(&the_lnet.ln_api_mutex);
		}
		if (rc < 0) {
			scnprintf(lnet_sysfs_read_ni,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to startup net.  "
				  "Check logs for reason.\n");
		} else
			strlcpy(lnet_sysfs_read_ni, "Success: Network up\n",
				LNET_SYSFS_MAX_BUF);
		break;
	case 'D':
		/* Delete a Network Interface command. */
		rc = lnet_shutdown_lndni(net);
		if (rc) {
			scnprintf(lnet_sysfs_read_ni,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to shutdown net: errno = %d\n",
				  rc);
			CERROR(lnet_sysfs_read_ni);
		} else
			strlcpy(lnet_sysfs_read_ni, "Success: Network down\n",
				LNET_SYSFS_MAX_BUF);
		break;
	case 'S':
		/* Show a Network Interface comamnd. */
		if (the_lnet.ln_refcount == 0) {
			scnprintf(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF,
				 "Fail: No networks configured\n");
			break;
		}
		rc = lnet_show_lndni(net, lnet_sysfs_read_ni,
				     LNET_SYSFS_MAX_BUF);
		if (rc) {
			scnprintf(lnet_sysfs_read_ni,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to show net: errno = %d\n",
				  rc);
			CERROR(lnet_sysfs_read_ni);
		}
		break;
	case 'L':
		/* List all Network Interfaces command. */
		if (the_lnet.ln_refcount == 0) {
			scnprintf(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF,
				 "Fail: No networks configured\n");
			break;
		}
		rc = lnet_list_lndni(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF);
		if (rc) {
			scnprintf(lnet_sysfs_read_ni,
				  LNET_SYSFS_MAX_BUF,
				  "Fail: Unable to list nets: errno = %d\n",
				  rc);
			CERROR(lnet_sysfs_read_ni);
		}
		break;
	default:
		scnprintf(lnet_sysfs_read_ni, LNET_SYSFS_MAX_BUF,
			  "Fail: Invalid command letter: %c\n", tmpstr[0]);
		CERROR(lnet_sysfs_read_ni);
		rc = -EPROTO;
		goto net_failure;
		break;
	}

early_out:
	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
	LIBCFS_FREE(tmpstr, count - cmd_start + 1);
	return count;

net_failure:
	LNET_MUTEX_UNLOCK(&lnet_sysfs_mutex);
	LIBCFS_FREE(tmpstr, count - cmd_start + 1);
	return rc;
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
	LASSERT(our_device != NULL);

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

	strncpy(lnet_sysfs_read_route, "No action taken\n", LNET_SYSFS_MAX_BUF);
	lnet_sysfs_read_route[LNET_SYSFS_MAX_BUF-1] = '\0';
	strncpy(lnet_sysfs_read_ni, "No action taken\n", LNET_SYSFS_MAX_BUF);
	lnet_sysfs_read_ni[LNET_SYSFS_MAX_BUF-1] = '\0';

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

int
lnet_show_lndni(__u32 net, char *buffer, unsigned int buffer_size)
{
	struct lnet_ni	*ni;
	cfs_list_t	*tmp;
	int		cpt;
	int		rc = -EINVAL;
	char		*pos;

	if ((!the_lnet.ln_init) || (the_lnet.ln_refcount <= 0))
		return rc;

	cpt = lnet_net_lock_current();

	pos = buffer;
	*pos = '\0';
	cfs_list_for_each(tmp, &the_lnet.ln_nis) {
		ni = cfs_list_entry(tmp, lnet_ni_t, ni_list);

		/* See if this is the NI we are after. */
		if (LNET_NIDNET(ni->ni_nid) == net) {
			char *stat;

			lnet_ni_lock(ni);

			if (ni->ni_interfaces[0]) {
				int i;

				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 "NID: %s ( ",
						 libcfs_nid2str(ni->ni_nid));

				for (i = 0; i < LNET_MAX_INTERFACES; i++) {
					if (ni->ni_interfaces[i]) {
						pos += scnprintf(pos, buffer +
							 buffer_size -
							 pos, "%s ",
							 ni->ni_interfaces[i]);
		}
				}
				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 ")\n");
			 } else {
				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 "NID: %s\n",
						 libcfs_nid2str(ni->ni_nid));
			}

			LNET_MUTEX_LOCK(&the_lnet.ln_ping_info_mutex);
			stat = (ni->ni_status->ns_status ==
				LNET_NI_STATUS_UP) ? "up" : "down";
			LNET_MUTEX_UNLOCK(&the_lnet.ln_ping_info_mutex);
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "State: %s\n", stat);

			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "peer_credits: %d\n",
					 ni->ni_peertxcredits);
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "peer_buffer_credits: %d\n",
					 ni->ni_peerrtrcredits);
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "peer_timeout: %d\n",
					 ni->ni_peertimeout);
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "credits: %d\n", ni->ni_maxtxcredits);

			if (ni->ni_cpts == NULL) {
				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 "CPTs: None Set\n");
			} else {
				int i;

				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 "CPTs:");
				for (i = 0; i < ni->ni_ncpts; i++) {
					pos += scnprintf(pos,
							 buffer + buffer_size
							 - pos,
							 " %d", ni->ni_cpts[i]);
				}
				pos += scnprintf(pos,
						 buffer + buffer_size - pos,
						 "\n");
			}
			lnet_ni_unlock(ni);
		}
	}
	lnet_net_unlock(cpt);
	if (pos == buffer)
		scnprintf(buffer, buffer_size, "No interface to network\n");
	return 0;
}

int
lnet_list_lndni(char *buffer, unsigned int buffer_size)
{
	struct lnet_ni	*ni;
	cfs_list_t	*tmp;
	int		cpt;
	int		rc = -EINVAL;
	char		*pos;

	if ((!the_lnet.ln_init) || (the_lnet.ln_refcount <= 0))
		return rc;

	cpt = lnet_net_lock_current();

	pos = buffer;
	*pos = '\0';
	cfs_list_for_each(tmp, &the_lnet.ln_nis) {
		ni = cfs_list_entry(tmp, lnet_ni_t, ni_list);

		/* Check if there are any interfaces defined. */
		if (ni->ni_interfaces[0]) {
			int i;

			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "%s ( ",
					 libcfs_nid2str(ni->ni_nid));

			for (i = 0; i < LNET_MAX_INTERFACES; i++) {
				if (ni->ni_interfaces[i]) {
					pos += scnprintf(pos, buffer +
							 buffer_size -
							 pos, "%s ",
							 ni->ni_interfaces[i]);
	}
			}
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 ")\n");
		 } else {
			pos += scnprintf(pos, buffer + buffer_size - pos,
					 "%s\n",
					 libcfs_nid2str(ni->ni_nid));
		}
	}

	lnet_net_unlock(cpt);
	if (pos == buffer)
		scnprintf(buffer, buffer_size, "No NID's configured\n");
	return 0;
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
