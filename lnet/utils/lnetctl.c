/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * Copyright (c) 2013, Intel Corporation, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 *
 * LGPL HEADER END
 *
 * Contributers:
 *   Amir Shehata
 */

#include <stdio.h>
#include <stdlib.h>
#include <lnet/lnetctl.h>
#include <libcfs/libcfsutil.h>
#include "cyaml/cyaml.h"
#include "lnetconfig/liblnetconfig.h"

extern int optind;

static int jt_add_route(int argc, char **argv);
static int jt_add_net(int argc, char **argv);
static int jt_add_routing(int argc, char **argv);
static int jt_add_buffers(int argc, char **argv);
static int jt_add_file(int argc, char **argv);
static int jt_add_help(int argc, char **argv);
static int jt_del_route(int argc, char **argv);
static int jt_del_net(int argc, char **argv);
static int jt_del_routing(int argc, char **argv);
static int jt_del_buffers(int argc, char **argv);
static int jt_del_file(int argc, char **argv);
static int jt_del_help(int argc, char **argv);
static int jt_show_route(int argc, char **argv);
static int jt_show_net(int argc, char **argv);
static int jt_show_buffers(int argc, char **argv);
static int jt_show_file(int argc, char **argv);
static int jt_show_stats(int argc, char **argv);
static int jt_show_help(int argc, char **argv);
static int jt_help(int argc, char **argv);

command_t add_list[] = {
	{"route", jt_add_route, 0, "add a route\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp)\n"
	 "\t--hop: number to final destination (1 < hops < 255)\n"
	 "\t--priority: priority of route (0 - highest prio\n"},
	{"net", jt_add_net, 0, "add a network\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--if: physical interface (ex eth0)\n"
	 "\t--peer_timeout: time to wait before declaring a peer dead\n"
	 "\t--peer_credits: define the max number of inflight messages\n"
	 "\t--peer_buffer_credits: the number of buffer credits per peer\n"
	 "\t--credits: Network Interface credits\n"
	 "\t--cpts: CPU Partitions configured net uses\n"},
	{"routing", jt_add_routing, 0, "enable routing\n"},
	{"buffers", jt_add_buffers, 0, "configure routing buffers\n"
	 "\t--tiny: specify size of tiny buffers\n"
	 "\t--small: spcify size of small buffers\n"
	 "\t--large: specify size of large buffers\n"},
	{"file", jt_add_file, 0, "add configuration based on YAML file\n"
	 "\t--name: name of YAML file\n"},
	{"help", jt_add_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t del_list[] = {
	{"route", jt_del_route, 0, "delete a route\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp)\n"},
	{"net", jt_del_net, 0, "delete a network\n"
	 "\t--net: net name (ex tcp0)\n"},
	{"routing", jt_del_routing, 0, "disable routing\n"},
	{"buffers", jt_del_buffers, 0,
	 "reset router buffers to default values\n"},
	{"file", jt_del_file, 0, "del configuration based on YAML file\n"
	 "\t--name: name of YAML file\n"},
	{"help", jt_del_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t show_list[] = {
	{"route", jt_show_route, 0, "show routes\n"
	 "\t--net: net name (ex tcp0) to filter on\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp) to filter on\n"
	 "\t--hop: number to final destination (1 < hops < 255) to filter on \n"
	 "\t--priority: priority of route (0 - highest prio to filter on\n"
	 "\t--detail: display detailed output per route\n"},
	{"net", jt_show_net, 0, "show networks\n"
	 "\t--net: net name (ex tcp0) to filter on\n"
	 "\t--detail: display detailed output per network\n"},
	{"buffers", jt_show_buffers, 0, "show routing buffers\n"},
	{"statistics", jt_show_stats, 0, "show lnet statistics\n"},
	{"file", jt_show_file, 0, "show configuration based on YAML file\n"
	 "\t--name: name of YAML file\n"},
	{"help", jt_show_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

static inline void print_help(command_t cmds[], char *cmd_type)
{
	command_t *cmd;

	for (cmd = cmds; cmd->pc_name; cmd++)
		printf("%s %s: %s\n", cmd_type, cmd->pc_name, cmd->pc_help);
}

static long int parse_long(char *number)
{
	char *end;
	long int value;

	value = strtol(number,  &end, 0);
	if (end != NULL && *end != 0) {
		return -1;
	}

	return value;
}

static int jt_add_route(int argc, char **argv)
{
	char *nw = NULL, *gw = NULL;
	long int hop = -1, prio = -1;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "n:g:h:p:";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{"gateway", 1, NULL, 'g'},
		{"hop", 1, NULL, 'h'},
		{"priority", 1, NULL, 'p'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		case 'g':
			gw = optarg;
			break;
		case 'h':
			hop = parse_long(optarg);
			break;
		case 'p':
			prio = parse_long(optarg);
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_config_route(nw, gw, hop, prio, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_add_net(int argc, char **argv)
{
	char *nw = NULL, *intf = NULL, *cpt = NULL;
	long int pto = -1, pc = -1, pbc = -1, cre = -1;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "n:i:t:c:b:r:s:";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{"interface", 1, NULL, 'i'},
		{"peer_timeout", 1, NULL, 't'},
		{"peer_credits", 1, NULL, 'c'},
		{"peer_buffer_credits", 1, NULL, 'b'},
		{"credits", 1, NULL, 'r'},
		{"cpt", 1, NULL, 's'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		case 'i':
			intf = optarg;
			break;
		case 't':
			pto = parse_long(optarg);
			break;
		case 'c':
			pc = parse_long(optarg);
			break;
		case 'b':
			pbc = parse_long(optarg);
			break;
		case 'r':
			cre = parse_long(optarg);
			break;
		case 's':
			cpt = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_config_net(nw, intf, pto, pc, pbc, cre, cpt, -1,
				    &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_add_routing(int argc, char **argv)
{
	struct cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_enable_routing(1, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_add_buffers(int argc, char **argv)
{
	char *nw = NULL, *intf = NULL, *cpt = NULL;
	long int tiny = -1, small = -1, large = -1;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "t:s:l:";
	const struct option long_options[] = {
		{"tiny", 1, NULL, 't'},
		{"small", 1, NULL, 's'},
		{"large", 1, NULL, 'l'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 't':
			tiny = parse_long(optarg);
			break;
		case 's':
			small = parse_long(optarg);
			break;
		case 'l':
			large = parse_long(optarg);
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_config_buf(tiny, small, large, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_add_file(int argc, char **argv)
{
	char *file = NULL;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "f:";
	const struct option long_options[] = {
		{"file", 1, NULL, 'f'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'f':
			file = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_yaml_config(file, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_add_help(int argc, char **argv)
{
	print_help(add_list, "add");
}

static int jt_del_route(int argc, char **argv)
{
	char *nw = NULL, *gw = NULL;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "n:g:";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{"gateway", 1, NULL, 'g'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		case 'g':
			gw = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_del_route(nw, gw, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_del_net(int argc, char **argv)
{
	char *nw = NULL;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "n:";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_del_net(nw, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_del_routing(int argc, char **argv)
{
	struct cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_enable_routing(0, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_del_buffers(int argc, char **argv)
{
	struct cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_config_buf(0, 0, 0, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_del_file(int argc, char **argv)
{
	char *file = NULL;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	optind = 0;

	const char *const short_options = "f:";
	const struct option long_options[] = {
		{"file", 1, NULL, 'f'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'f':
			file = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_yaml_del(file, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_del_help(int argc, char **argv)
{
	print_help(del_list, "del");
}

static int jt_show_route(int argc, char **argv)
{
	char *nw = NULL, *gw = NULL;
	long int hop = -1, prio = -1;
	int detail = 0, rc, opt;
	struct cYAML *err_rc = NULL, *show_rc = NULL;
	optind = 0;

	const char *const short_options = "n:g:h:p:d";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{"gateway", 1, NULL, 'g'},
		{"hop", 1, NULL, 'h'},
		{"priority", 1, NULL, 'p'},
		{"detail", 0, NULL, 'd'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		case 'g':
			gw = optarg;
			break;
		case 'h':
			hop = parse_long(optarg);
			break;
		case 'p':
			prio = parse_long(optarg);
			break;
		case 'd':
			detail = 1;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_show_route(nw, gw, hop, prio, detail, -1,
				    &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_net(int argc, char **argv)
{
	char *nw = NULL;
	int detail = 0, rc, opt;
	struct cYAML *err_rc = NULL, *show_rc = NULL;
	optind = 0;

	const char *const short_options = "n:d";
	const struct option long_options[] = {
		{"net", 1, NULL, 'n'},
		{"detail", 0, NULL, 'd'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'n':
			nw = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_show_net(nw, detail, -1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_buffers(int argc, char **argv)
{
	struct cYAML *err_rc = NULL, *show_rc = NULL;
	int rc;

	rc = lustre_lnet_show_buf(-1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_file(int argc, char **argv)
{
	char *file = NULL;
	struct cYAML *err_rc = NULL, *show_rc = NULL;
	int opt, rc;
	optind = 0;

	const char *const short_options = "f:";
	const struct option long_options[] = {
		{"file", 1, NULL, 'f'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'f':
			file = optarg;
			break;
		default:
			break;
		}
	}

	rc = lustre_yaml_show(file, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_stats(int argc, char **argv)
{
	int rc;
	struct cYAML *show_rc = NULL, *err_rc = NULL;

	rc = lustre_lnet_show_stats(-1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_help(int argc, char **argv)
{
	print_help(show_list, "show");
}

static inline int jt_add(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], add_list);
}

static inline int jt_del(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], del_list);
}

static inline int jt_show(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], show_list);
}

command_t list[] = {
	{"add", jt_add, 0, "add {route | net | routing | buffer}"},
	{"del", jt_del, 0, "del {route | net | routing | buffer}"},
	{"show", jt_show, 0, "show {route | net | buffer | statistics"},
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

	Parser_init("lnetctl > ", list);
	if (argc > 1) {
		rc = Parser_execarg(argc - 1, &argv[1], list);
		goto errorout;
	}

	Parser_commands();

errorout:
	libcfs_arch_cleanup();
	return rc;
}
