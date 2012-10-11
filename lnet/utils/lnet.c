/*
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 *   This file is part of Portals, http://www.sf.net/projects/lustre/
 *
 *   Portals is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Portals is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Portals; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */
/*
 * Copyright (c) 2012, Intel Corporation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <lnet/lnetctl.h>
#include <libcfs/libcfsutil.h>

#define SYSFS_MAX_BUFFER_SIZE 4096

#define SYSFS_MESSAGE_FORMAT "%d:%s"

static void
lnet_print_usage(char *cmd)
{
	Parser_printhelp(cmd);
}

/* This routine is used to execute sysfs actions. */
static int
lnet_sysfs_take_action(char *filename, char *buffer)
{
	int rc;
	char local_buf[SYSFS_MAX_BUFFER_SIZE];
	int hold_pid;

	/* Form the final write buffer by pre-pending it with our process id. */
	snprintf(local_buf, sizeof(local_buf), SYSFS_MESSAGE_FORMAT,
		 getpid(), buffer);
	local_buf[sizeof(local_buf) - 1] = '\0';

	rc = libcfs_set_param(filename, local_buf, strlen(local_buf));
	if (rc) {
		libcfs_error(LIBCFS_MSG_ERROR, rc,
			     "Error writing to sysfs file\n");
		return rc;
	}
	local_buf[0] = '\0';
	rc = libcfs_get_param(filename, local_buf, sizeof(local_buf));
	if (rc == 0)  {
		int i;

		/* Check if the returned PID is ours (conflict check). */
		for (i = 0; (i < strlen(local_buf)) && (local_buf[i] != ':');
		     i++);
		if ((i >= strlen(local_buf)) || (local_buf[i] != ':')) {
			/* Cannot find the pid. Fail message. */
			libcfs_error(LIBCFS_MSG_ERROR, -1,
				     "Badly formed response message: %s\n",
				     local_buf);
		} else {
			local_buf[i] = '\0';
			hold_pid = atoi(local_buf);
			if (hold_pid != getpid())
				libcfs_error(LIBCFS_MSG_ERROR, -1,
				       "Conflict with another user of sysfs\n");
			else
				libcfs_printf(LIBCFS_MSG_NORMAL,
					      local_buf + i + 1);
		}
	} else
		libcfs_error(LIBCFS_MSG_ERROR, rc,
			     "Error reading from sysfs file\n");
	return 0;
}

static int
jt_lnet_route(int argc, char **argv)
{
	libcfs_printf(LIBCFS_MSG_WARN, "route command not implemented yet\n");
	return 0;
}

static int
jt_lnet_route_list(int argc, char **argv)
{
	libcfs_printf(LIBCFS_MSG_WARN,
		      "route_list command not implemented yet\n");
	return 0;
}

static int
jt_lnet_route_buffers(int argc, char **argv)
{
	libcfs_printf(LIBCFS_MSG_WARN,
		      "route_buffers command not implemented yet\n");
	return 0;
}

static int
jt_lnet_ni(int argc, char **argv)
{
	libcfs_printf(LIBCFS_MSG_WARN, "net command not implemented yet\n");
	return 0;
}

static int
jt_lnet_ni_list(int argc, char **argv)
{
	libcfs_printf(LIBCFS_MSG_WARN,
		      "net_list command not implemented yet\n");
	return 0;
}

command_t list[] = {
	{"route", jt_lnet_route, NULL,
	 "Usage: lnet route <net> add | del | show <gateway> [<hops>]"},
	{"route_list", jt_lnet_route_list, NULL,
	 "Usage: lnet route_list"},
	{"route_buffers", jt_lnet_route_buffers, NULL,
	 "Usage: lnet route_buffers <tiny_size> <small_size> <large_size>"},
	{"net", jt_lnet_ni, NULL,
	 "Usage: lnet net <net> up | down | show [<interfaces> [<net parameters>] [<SMP parameters>]]"},
	{"net_list", jt_lnet_ni_list, NULL,
	 "Usage: lnet net_list"},
	{"help", Parser_help, 0, "help"},
	{"exit", Parser_quit, 0, "quit"},
	{"quit", Parser_quit, 0, "quit"},
	{ 0, 0, 0, NULL }
};

int main(int argc, char **argv)
{
	int rc = 0;

	rc = libcfs_arch_init();
	if (rc < 0)
		return rc;

	Parser_init("lnet > ", list);
	if (argc > 1) {
		rc = Parser_execarg(argc - 1, &argv[1], list);
		goto errorout;
	}

	Parser_commands();

errorout:
	libcfs_arch_cleanup();
	return rc;
}
