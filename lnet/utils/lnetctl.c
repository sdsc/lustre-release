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

static int jt_add_route(int argc, char **argv);
static int jt_add_net(int argc, char **argv);
static int jt_set_routing(int argc, char **argv);
static int jt_route_help(int argc, char **argv);
static int jt_del_route(int argc, char **argv);
static int jt_del_net(int argc, char **argv);
static int jt_net_help(int argc, char **argv);
static int jt_show_route(int argc, char **argv);
static int jt_show_net(int argc, char **argv);
static int jt_show_routing(int argc, char **argv);
static int jt_show_stats(int argc, char **argv);
static int jt_show_peer_credits(int argc, char **argv);
static int jt_set_help(int argc, char **argv);
static int jt_stats_help(int argc, char **argv);
static int jt_peer_credits_help(int argc, char **argv);
static int jt_routing_help(int argc, char **argv);
static int jt_set_tiny(int argc, char **argv);
static int jt_set_small(int argc, char **argv);
static int jt_set_large(int argc, char **argv);

command_t route_cmds[] = {
	{"add", jt_add_route, 0, "add a route\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp)\n"
	 "\t--hop: number to final destination (1 < hops < 255)\n"
	 "\t--priority: priority of route (0 - highest prio\n"},
	{"del", jt_del_route, 0, "delete a route\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp)\n"},
	{"show", jt_show_route, 0, "show routes\n"
	 "\t--net: net name (ex tcp0) to filter on\n"
	 "\t--gateway: gateway nid (ex 10.1.1.2@tcp) to filter on\n"
	 "\t--hop: number to final destination (1 < hops < 255) to filter on\n"
	 "\t--priority: priority of route (0 - highest prio to filter on\n"
	 "\t--detail: display detailed output per route\n"},
	{"help", jt_route_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t net_cmds[] = {
	{"add", jt_add_net, 0, "add a network\n"
	 "\t--net: net name (ex tcp0)\n"
	 "\t--if: physical interface (ex eth0)\n"
	 "\t--peer_timeout: time to wait before declaring a peer dead\n"
	 "\t--peer_credits: define the max number of inflight messages\n"
	 "\t--peer_buffer_credits: the number of buffer credits per peer\n"
	 "\t--credits: Network Interface credits\n"
	 "\t--cpts: CPU Partitions configured net uses\n"},
	{"del", jt_del_net, 0, "delete a network\n"
	 "\t--net: net name (ex tcp0)\n"},
	{"show", jt_show_net, 0, "show networks\n"
	 "\t--net: net name (ex tcp0) to filter on\n"
	 "\t--detail: display detailed output per network\n"},
	{"help", jt_net_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t routing_cmds[] = {
	{"show", jt_show_routing, 0, "show routing information\n"},
	{"help", jt_routing_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t stats_cmds[] = {
	{"show", jt_show_stats, 0, "show LNET statistics\n"},
	{"help", jt_stats_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t credits_cmds[] = {
	{"show", jt_show_peer_credits, 0, "show peer credits\n"},
	{"help", jt_peer_credits_help, 0, "display this help\n"},
	{ 0, 0, 0, NULL }
};

command_t set_cmds[] = {
	{"tiny_buffers", jt_set_tiny, 0, "set tiny routing buffers\n"
	 "\tVALUE must be greater than 0\n"},
	{"small_buffers", jt_set_small, 0, "set small routing buffers\n"
	 "\tVALUE must be greater than 0\n"},
	{"large_buffers", jt_set_large, 0, "set large routing buffers\n"
	 "\tVALUE must be greater than 0\n"},
	{"routing", jt_set_routing, 0, "enable/disable routing\n"
	 "\t0 - disable routing\n"
	 "\t1 - enable routing\n"},
	{"help", jt_set_help, 0, "display this help\n"},
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
	if (end != NULL && *end != 0)
		return -1;

	return value;
}

static int jt_route_help(int argc, char **argv)
{
	print_help(route_cmds, "route");
}

static int jt_net_help(int argc, char **argv)
{
	print_help(net_cmds, "net");
}

static int jt_set_help(int argc, char **argv)
{
	print_help(set_cmds, "set");
}

static int jt_stats_help(int argc, char **argv)
{
	print_help(stats_cmds, "stats");
}

static int jt_peer_credits_help(int argc, char **argv)
{
	print_help(credits_cmds, "peer_credits");
}

static int jt_routing_help(int argc, char **argv)
{
	print_help(routing_cmds, "routing");
}

static int jt_set_tiny(int argc, char **argv)
{
	long int value;
	int rc;
	char *end;
	struct cYAML *err_rc = NULL;

	value = parse_long(argv[1]);
	if (value == -1) {
		cYAML_build_error(-1, -1, "parser", "set",
				  "Can not parse tiny_buffers value", &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
		return -1;
	}

	rc = lustre_lnet_config_buffers(value, -1, -1, -1, &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_set_small(int argc, char **argv)
{
	long int value;
	int rc;
	char *end;
	struct cYAML *err_rc = NULL;

	value = parse_long(argv[1]);
	if (value == -1) {
		cYAML_build_error(-1, -1, "parser", "set",
				  "Can not parse small_buffers value", &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
		return -1;
	}

	rc = lustre_lnet_config_buffers(-1, value, -1, -1, &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_set_large(int argc, char **argv)
{
	long int value;
	int rc;
	char *end;
	struct cYAML *err_rc = NULL;

	value = parse_long(argv[1]);
	if (value == -1) {
		cYAML_build_error(-1, -1, "parser", "set",
				  "Can not parse large_buffers value", &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
		return -1;
	}

	rc = lustre_lnet_config_buffers(-1, -1, value, -1, &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_set_routing(int argc, char **argv)
{
	long int value;
	char *end;
	struct cYAML *err_rc = NULL;
	int rc;

	value = parse_long(argv[1]);
	if ((value != 0) && (value != 1)) {
		cYAML_build_error(-1, -1, "parser", "set",
				  "Can not parse routing value.\n"
				  "Must be 0 for disable or 1 for enable",
				  &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
		return -1;
	}

	rc = lustre_lnet_enable_routing(value, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
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
		cYAML_print_tree2file(stderr, err_rc);

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
		{"if", 1, NULL, 'i'},
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
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
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
		cYAML_print_tree2file(stderr, err_rc);

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
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
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
		cYAML_print_tree2file(stderr, err_rc);
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
		case 'd':
			detail = 1;
			break;
		default:
			break;
		}
	}

	rc = lustre_lnet_show_net(nw, detail, -1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_routing(int argc, char **argv)
{
	struct cYAML *err_rc = NULL, *show_rc = NULL;
	int rc;

	rc = lustre_lnet_show_routing(-1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);
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
		cYAML_print_tree2file(stderr, err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static int jt_show_peer_credits(int argc, char **argv)
{
	int rc;
	struct cYAML *show_rc = NULL, *err_rc = NULL;

	rc = lustre_lnet_show_peer_credits(-1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);
	else if (show_rc)
		cYAML_print_tree(show_rc);

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

static inline int jt_route(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], route_cmds);
}

static inline int jt_net(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], net_cmds);
}

static inline int jt_routing(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], routing_cmds);
}

static inline int jt_stats(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], stats_cmds);
}

static inline int jt_peer_credits(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], credits_cmds);
}

static inline int jt_set(int argc, char **argv)
{
	if (argc < 2)
		return CMD_HELP;

	return Parser_execarg(argc - 1, &argv[1], set_cmds);
}

static int jt_import(int argc, char **argv)
{
	char *file = NULL;
	struct cYAML *err_rc = NULL;
	struct cYAML *show_rc = NULL;
	int rc, opt, opt_found = 0;
	optind = 0;
	char cmd = 'a';

	const char *const short_options = "adsh";
	const struct option long_options[] = {
		{"add", 0, NULL, 'a'},
		{"del", 0, NULL, 'd'},
		{"show", 0, NULL, 's'},
		{"help", 0, NULL, 'h'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		opt_found = 1;
		switch (opt) {
		case 'a':
		case 'd':
		case 's':
			cmd = opt;
			break;
		case 'h':
			printf("import FILE\n"
			       "import < FILE : import a file\n"
			       "\t--add: add configuration\n"
			       "\t--del: delete configuration\n"
			       "\t--show: show configuration\n"
			       "\t--help: display this help\n"
			       "If no command option is given then --add"
			       " is assumed by default\n");
		default:
			break;
		}
	}

	/* grab the file name if one exists */
	if ((opt_found) && (argc == 3))
		file = argv[2];
	else if ((!opt_found) && (argc == 2))
		file = argv[1];

	switch (cmd) {
	case 'a':
		rc = lustre_yaml_config(file, &err_rc);
		break;
	case 'd':
		rc = lustre_yaml_del(file, &err_rc);
		break;
	case 's':
		rc = lustre_yaml_show(file, &show_rc, &err_rc);
		cYAML_print_tree(show_rc);
		cYAML_free_tree(show_rc);
		break;
	}

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree2file(stderr, err_rc);

	cYAML_free_tree(err_rc);

	return rc;
}

static int jt_export(int argc, char **argv)
{
	struct cYAML *show_rc = NULL;
	struct cYAML *err_rc = NULL;
	int rc, opt;
	FILE *f = NULL;
	optind = 0;

	const char *const short_options = "h";
	const struct option long_options[] = {
		{"help", 0, NULL, 'h'},
		{NULL, 0, NULL, 0},
	};

	while ((opt = getopt_long(argc, argv, short_options,
				   long_options, NULL)) != -1) {
		switch (opt) {
		case 'h':
			printf("export FILE\n"
			       "export > FILE : export configuration\n"
			       "\t--help: display this help\n");
		default:
			break;
		}
	}

	if (argc >= 2) {
		f = fopen(argv[1], "w");
		if (f == NULL)
			return -1;
	} else
		f = stdout;

	rc = lustre_lnet_show_net(NULL, 1, -1, &show_rc, &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR) {
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
	}

	rc = lustre_lnet_show_route(NULL, NULL, -1, -1, 1, -1, &show_rc,
				    &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR) {
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
	}

	rc = lustre_lnet_show_routing(-1, &show_rc, &err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR) {
		cYAML_print_tree2file(stderr, err_rc);
		cYAML_free_tree(err_rc);
	}

	if (show_rc != NULL) {
		cYAML_print_tree2file(f, show_rc);
		cYAML_free_tree(show_rc);
	}

	if (argc >= 2)
		fclose(f);

	return 0;
}

command_t list[] = {
	{"route", jt_route, 0, "route {add | del | show | help}"},
	{"net", jt_net, 0, "route {add | del | show | help}"},
	{"routing", jt_routing, 0, "routing {show | help}"},
	{"set", jt_set, 0, "set {tiny_buffers | small_buffers | large_buffers"
			   " | routing}"},
	{"import", jt_import, 0, "import {--add | --del | --show | "
				 "--help} FILE.yaml"},
	{"export", jt_export, 0, "export {--help} FILE.yaml"},
	{"stats", jt_stats, 0, "stats {show | help}"},
	{"peer_credits", jt_peer_credits, 0, "stats {show | help}"},
	{"help", Parser_help, 0, "help"},
	{"exit", Parser_quit, 0, "quit"},
	{"quit", Parser_quit, 0, "quit"},
	{ 0, 0, 0, NULL }
};

int main(int argc, char **argv)
{
	int rc = 0;
	struct cYAML *err_rc = NULL;

	rc = lustre_lnet_config_lib_init();
	if (rc < 0) {
		cYAML_build_error(-1, -1, "lnetctl", "startup",
				  "cannot register LNet device", &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		return rc;
	}

	rc = libcfs_arch_init();
	if (rc < 0) {
		cYAML_build_error(-1, -1, "lnetctl", "startup",
				  "cannot initialize libcfs", &err_rc);
		cYAML_print_tree2file(stderr, err_rc);
		return rc;
	}

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
