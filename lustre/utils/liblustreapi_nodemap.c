/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <linux/netlink.h>
#include <liblustre.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

#define NODEMAP_NL_NIDTEST 0
#define NODEMAP_NL_BUFSIZE 128

int llapi_nodemap_exists(const char *nodemap)
{
	char mapname[PATH_MAX + 1];

	snprintf(mapname, sizeof(mapname) - 1, "nodemap/%s", nodemap);

	return get_param(mapname, NULL, 0);
}

int llapi_search_nodemap_range(char *range)
{
	char buffer[PATH_MAX + 1];
	char *start, *end;
	__u32 start_range_id = 0, end_range_id = 0;

	snprintf(buffer, PATH_MAX, "%s", range);

	start = strtok(buffer, ":");
	if (start == NULL)
		return 1;

	end = strtok(NULL, ":");
	if (end == NULL)
		return 1;

	start_range_id = llapi_search_nodemap_nid(start);
	end_range_id = llapi_search_nodemap_nid(end);

	if (start_range_id != end_range_id)
		return 1;

	if ((start_range_id != 0) || (end_range_id != 0))
		return 1;

	return 0;
}

char *nl_check_nodemap(char *param)
{
	struct sockaddr_nl src_addr, dst_addr;
	struct timeval timeout = {5, 0};
	struct nlmsghdr *nlh = NULL;
	struct iovec iov;
	int sock_fd;
	struct msghdr msg;
	char *response = NULL;

	sock_fd = socket(PF_NETLINK, SOCK_RAW, 31);
	if (sock_fd < 0)
		return NULL;

	memset(&src_addr, 0, sizeof(src_addr));
	src_addr.nl_family = AF_NETLINK;
	src_addr.nl_pid = getpid();

	bind(sock_fd, (struct sockaddr *)&src_addr, sizeof(src_addr));

	memset(&dst_addr, 0, sizeof(dst_addr));
	dst_addr.nl_family = AF_NETLINK;
	dst_addr.nl_pid = 0;
	dst_addr.nl_groups = 0;

	nlh = (struct nlmsghdr *)malloc(NLMSG_SPACE(NODEMAP_NL_BUFSIZE));

	memset(nlh, 0, NLMSG_SPACE(NODEMAP_NL_BUFSIZE));
	nlh->nlmsg_len = NLMSG_SPACE(NODEMAP_NL_BUFSIZE);
	nlh->nlmsg_pid = getpid();
	nlh->nlmsg_flags = 0;

	strcpy(NLMSG_DATA(nlh), param);

	iov.iov_base = (void *)nlh;
	iov.iov_len = nlh->nlmsg_len;

	memset(&msg, 0, sizeof(msg));
	msg.msg_name = (void *)&dst_addr;
	msg.msg_namelen = sizeof(dst_addr);
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO,
	    (char *)&timeout, sizeof(timeout)) < 0) {
		goto out;
	}

	if (setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO,
	    (char *)&timeout, sizeof(timeout)) < 0) {
		goto out;
	}

	if (sendmsg(sock_fd, &msg, 0) < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			printf("sendmsg timed out\n");
			response = NULL;
			goto out;
		}
	}

	if (recvmsg(sock_fd, &msg, 0) < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			printf("sendmsg timed out\n");
			response = NULL;
			goto out;
		}
	}

	response = (char *)malloc(strlen(NLMSG_DATA(nlh)));
	strcpy(response, NLMSG_DATA(nlh));

out:
	close(sock_fd);

	return response;
}

char *llapi_find_nodemap_name(char *nid)
{
	char *response, *nodemap = NULL;
	char query[PATH_MAX + 1];

	snprintf(query, PATH_MAX, "%d %s", NODEMAP_NL_NIDTEST, nid);

	response = nl_check_nodemap(query);

	if (response == NULL)
		goto out;

	nodemap = (char *)malloc(strlen(response) + 1);
	strncpy(nodemap, response, strlen(response));
	free(response);
out:
	return nodemap;
}

int llapi_search_nodemap_nid(char *nid)
{
	int rc;
	char *response, *nodemap, *range_id;
	char query[PATH_MAX + 1];

	snprintf(query, PATH_MAX, "%d %s", NODEMAP_NL_NIDTEST, nid);

	response = nl_check_nodemap(query);

	if (response == NULL) {
		rc = -1;
		goto out;
	}

	nodemap = strtok(response, ":");
	if (nodemap == NULL) {
		rc = -2;
		goto out;
	}

	range_id = strtok(NULL, ":");
	if (range_id == NULL) {
		rc = -3;
		goto out;
	}

	rc = atoi(range_id);

out:
	free(response);
	return rc;
}
