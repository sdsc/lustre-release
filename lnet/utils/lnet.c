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

#define SYSFS_NI_QUERY "L"
#define SYSFS_NI_SHOW_CMD "S %s"
#define SYSFS_NI_ADD_CMD "A %s"
#define SYSFS_NI_DEL_CMD "D %s"

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
	int i;

	/* Form the final write buffer by pre-pending it with our process id. */
	snprintf(local_buf, sizeof(local_buf), SYSFS_MESSAGE_FORMAT,
		 getpid(), buffer);
	local_buf[sizeof(local_buf) - 1] = '\0';

	rc = cfs_set_param(filename, local_buf, strlen(local_buf));
	if (rc) {
		cfs_error(CFS_MSG_ERROR, rc,
			  "Error writing to sysfs file\n");
		return rc;
	}
	local_buf[0] = '\0';
	rc = cfs_get_param(filename, local_buf, sizeof(local_buf));
	if (rc != 0)  {
		cfs_error(CFS_MSG_ERROR, rc,
			  "Error reading from sysfs file\n");
		return rc;
	}

	/* Check if the returned PID is ours (conflict check). */
	for (i = 0; (i < strlen(local_buf)) && (local_buf[i] != ':');
	     i++);
	if ((i >= strlen(local_buf)) || (local_buf[i] != ':')) {
		/* Cannot find the pid. Fail message. */
		cfs_error(CFS_MSG_ERROR, -1,
			  "Badly formed response message: %s\n",
			     local_buf);
	} else {
		local_buf[i] = '\0';
		hold_pid = atoi(local_buf);
		if (hold_pid != getpid())
			cfs_error(CFS_MSG_ERROR, -1,
			       "Conflict with another user of sysfs\n");
		else
			cfs_printf(CFS_MSG_NORMAL,
				   local_buf + i + 1);
	}
	return 0;
}

static int
jt_lnet_route(int argc, char **argv)
{
	cfs_printf(CFS_MSG_WARN, "route command not implemented yet\n");
	return 0;
}

static int
jt_lnet_route_list(int argc, char **argv)
{
	cfs_printf(CFS_MSG_WARN,
		   "route_list command not implemented yet\n");
	return 0;
}

static int
jt_lnet_route_buffers(int argc, char **argv)
{
	cfs_printf(CFS_MSG_WARN,
		   "route_buffers command not implemented yet\n");
	return 0;
}

static int
jt_lnet_ni(int argc, char **argv)
{
	char *net_arg = NULL;
	char *cmd_arg = "show";
	char buffer[SYSFS_MAX_BUFFER_SIZE];

	/* There needs to be 1 argument in all net commands. */
	if (argc < 2) {
		cfs_err_noerrno(CFS_MSG_ERROR,
			"Wrong number of arguments to net command, got %d, "
			"expected >= 2\n",
			argc - 1);
		lnet_print_usage(argv[0]);
		return -1;
	}

	/* First argument is <net>. */
	net_arg = argv[1];

	/* If present, the second argument is the command.  If it is not
	   present, assume a "show" command. */
	if (argc > 2)
		cmd_arg = argv[2];

	if (strcasecmp(cmd_arg, "up") == 0) {
		int i;

		snprintf(buffer, sizeof(buffer), SYSFS_NI_ADD_CMD,
			 net_arg);
		buffer[sizeof(buffer) - 1] = '\0';

		/* Just append all parameters as given. */
		for (i = 3; i < argc; i++) {
			strlcat(buffer, " ", sizeof(buffer));
			strlcat(buffer, argv[i], sizeof(buffer));
		}
	} else if (strcasecmp(cmd_arg, "down") == 0) {
		snprintf(buffer, sizeof(buffer), SYSFS_NI_DEL_CMD,
			 net_arg);
	} else if (strcasecmp(cmd_arg, "show") == 0) {
		snprintf(buffer, sizeof(buffer), SYSFS_NI_SHOW_CMD,
			 net_arg);
	} else {
		cfs_err_noerrno(CFS_MSG_ERROR,
				"Unknown net command: %s",
				argv[2]);
		lnet_print_usage(argv[0]);
		return -1;
	}
	buffer[sizeof(buffer) - 1] = '\0';
	return lnet_sysfs_take_action("ni", buffer);
}

static int
jt_lnet_ni_list(int argc, char **argv)
{
	return lnet_sysfs_take_action("ni", SYSFS_NI_QUERY);
}

command_t list[] = {
	{"route", jt_lnet_route, NULL,
	 "Usage: lnet route <net> add | del | show <gateway> [<hops>]"},
	{"route_list", jt_lnet_route_list, NULL,
	 "Usage: lnet route_list"},
	{"route_buffers", jt_lnet_route_buffers, NULL,
	 "Usage: lnet route_buffers <tiny_size> <small_size> <large_size>"},
	{"net", jt_lnet_ni, NULL,
	 "Usage: lnet net <net> up | down | show [<interfaces> "
	 "[<net parameters>] [[SMP Settings]]]"},
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
