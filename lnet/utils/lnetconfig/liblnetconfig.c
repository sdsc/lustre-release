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

/*
 * There are two APIs:
 *  1. APIs that take the actual parameters expanded.  This is for other
 *  entities that would like to link against the library and call the APIs
 *  directly without having to form an intermediate representation.
 *  2. APIs that take a YAML file and parses out the information there and
 *  calls the APIs mentioned in 1
 */

#include <stdio.h>
#include <stdlib.h>
#include <libcfs/libcfsutil.h>
#include <lnet/api-support.h>
#include <lnet/lnetctl.h>
#include <lnet/socklnd.h>
#include "liblnetconfig.h"
#include "cyaml.h"

#define CONFIG_CMD		"add"
#define DEL_CMD			"del"
#define SHOW_CMD		"show"

static bool g_lib_init;

static void lustre_config_api_init()
{
	register_ioc_dev(LNET_DEV_ID, LNET_DEV_PATH,
			 LNET_DEV_MAJOR, LNET_DEV_MINOR);
}

int lustre_lnet_config_route(char *nw, char *gw, int hops, int prio,
			     int seq_no, struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (nw == NULL || gw == NULL) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Missing mandatory parameter(s) - %s%s",
			 (!nw) ? "network" : "",
			 (nw && !gw) ? "gateway" : (!gw) ? ", gateway" : "");
		rc = LUSTRE_CFG_RC_MISSING_PARAM;
		goto fn_exit;
	}

	net = libcfs_str2net(nw);
	if (net == LNET_NIDNET(LNET_NID_ANY)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Can't parse net %s", nw);
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	if (LNET_NETTYP(net) == CIBLND    ||
	    LNET_NETTYP(net) == OPENIBLND ||
	    LNET_NETTYP(net) == IIBLND    ||
	    LNET_NETTYP(net) == VIBLND) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Net %s obsolete", libcfs_lnd2str(net));
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	gateway_nid = libcfs_str2nid(gw);
	if (gateway_nid == LNET_NID_ANY) {
		snprintf(err_str,
			LNET_MAX_STR_LEN,
			"Can't parse gateway NID \"%s\"", gw);
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	if (hops == -1)
		/* -1 indicates to use the default hop value */
		hops = 1;
	else if ((hops < 1) || (hops > 255)) {
		snprintf(err_str,
			LNET_MAX_STR_LEN,
			"Num of hops must be 0 < hops < 256. hops = %d",
			hops);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	if (prio == -1)
		prio = 0;
	else if (prio < 0) {
		snprintf(err_str,
			LNET_MAX_STR_LEN,
			"Num priority must be > 0.  Priority = %d",
			prio);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	LIBCFS_IOC_INIT_V2(data);
	data.ioc_net = net;
	data.ioc_config_u.route.hop = (unsigned int) hops;
	data.ioc_config_u.route.priority = (unsigned int) prio;
	data.ioc_nid = gateway_nid;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_ROUTE, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_ROUTE failed - %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, CONFIG_CMD, "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_del_route(char *nw, char *gw,
			  int seq_no, struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (nw == NULL || gw == NULL) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Missing mandatory parameter(s) - %s%s",
			 (!nw) ? "network" : "",
			 (nw && !gw) ? "gateway" : (!gw) ? ", gateway" : "");
		rc = LUSTRE_CFG_RC_MISSING_PARAM;
		goto fn_exit;
	}

	net = libcfs_str2net(nw);
	if (net == LNET_NIDNET(LNET_NID_ANY)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Can't parse net %s", nw);
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	if (LNET_NETTYP(net) == CIBLND    ||
	    LNET_NETTYP(net) == OPENIBLND ||
	    LNET_NETTYP(net) == IIBLND    ||
	    LNET_NETTYP(net) == VIBLND) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Net %s obsoleted", libcfs_lnd2str(net));
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	gateway_nid = libcfs_str2nid(gw);
	if (gateway_nid == LNET_NID_ANY) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Can't parse gateway NID \"%s\"", gw);
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	LIBCFS_IOC_INIT_V2(data);
	data.ioc_net = net;
	data.ioc_nid = gateway_nid;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_DEL_ROUTE, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_DEL_ROUTE failed - %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, DEL_CMD, "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_route(char *nw, char *gw, int hops, int prio, int detail,
			   int seq_no, struct cYAML **show_rc,
			   struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int i;
	struct cYAML *root = NULL, *route = NULL, *item = NULL;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN,
		 "Out of Memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (nw != NULL) {
		net = libcfs_str2net(nw);
		if (net == LNET_NIDNET(LNET_NID_ANY)) {
			snprintf(err_str,
				 LNET_MAX_STR_LEN,
				 "Can't parse net %s", nw);
			rc = LUSTRE_CFG_RC_BAD_PARAM;
			goto fn_exit;
		}

		if (LNET_NETTYP(net) == CIBLND    ||
		    LNET_NETTYP(net) == OPENIBLND ||
		    LNET_NETTYP(net) == IIBLND    ||
		    LNET_NETTYP(net) == VIBLND) {
			snprintf(err_str,
				 LNET_MAX_STR_LEN,
				 "Net %s obsoleted\n", libcfs_lnd2str(net));
			rc = LUSTRE_CFG_RC_BAD_PARAM;
			goto fn_exit;
		}
	} else
		/* show all routes without filtering on net */
		net = LNET_NIDNET(LNET_NID_ANY);

	if (gw != NULL) {
		gateway_nid = libcfs_str2nid(gw);
		if (gateway_nid == LNET_NID_ANY) {
			snprintf(err_str,
				 LNET_MAX_STR_LEN,
				 "Can't parse gateway NID \"%s\"", gw);
			rc = LUSTRE_CFG_RC_BAD_PARAM;
			goto fn_exit;
		}
	} else
		/* show all routes with out filtering on gateway */
		gateway_nid = LNET_NID_ANY;

	if (((hops < 1) && (hops != -1)) || (hops > 255)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Num of hops must be 0 < hops < 256.  hops = %d\n",
			 hops);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	/* create struct cYAML root object */
	root = cYAML_create_object(NULL, NULL);
	if (root == NULL)
		goto fn_exit;

	route = cYAML_create_seq(root, "route");
	if (route == NULL)
		goto fn_exit;

	for (i = 0;; i++) {
		LIBCFS_IOC_INIT_V2(data);
		data.ioc_count = i;

		rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_ROUTE, &data);
		if (rc != 0)
			break;

		/* filter on provided data */
		if ((net != LNET_NIDNET(LNET_NID_ANY)) &&
		    (net != data.ioc_net))
			continue;

		if ((gateway_nid != LNET_NID_ANY) &&
		    (gateway_nid != data.ioc_nid))
			continue;

		if ((hops != -1) &&
		    (hops != data.ioc_config_u.route.hop))
			continue;

		if ((prio != -1) &&
		    (prio != data.ioc_config_u.route.priority))
			continue;

		/* default rc to -1 incase we hit the goto */
		rc = -1;

		item = cYAML_create_seq_item(route);
		if (item == NULL)
			goto fn_exit;

		if (!cYAML_create_string
			(item, "net",
			 libcfs_net2str(data.ioc_net)))
			goto fn_exit;

		if (detail) {
			if (cYAML_create_string(item, "gateway",
						libcfs_nid2str(data.ioc_nid))
			    == NULL)
				goto fn_exit;

			if (cYAML_create_number(item, "hop",
						data.ioc_config_u.route.hop)
			    == NULL)
				goto fn_exit;

			if (cYAML_create_number(item, "priority",
						data.ioc_config_u.
						route.priority) == NULL)
				goto fn_exit;

			if (cYAML_create_string(item, "state",
						data.ioc_config_u.route.flags ?
						"up" : "down") == NULL)
				goto fn_exit;
		}
	}

	/* print output iff show_rc is not provided */
	if (show_rc == NULL)
		cYAML_print_tree(root);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting routes - %s. check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((show_rc == NULL) || (rc != LUSTRE_CFG_RC_NO_ERR)) {
		cYAML_free_tree(root);
	} else if (show_rc != NULL) {
		if ((*show_rc) != NULL) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, SHOW_CMD, "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_config_net(char *net, char *intf, int peer_to, int peer_cr,
			   int peer_buf_cr, int credits, char *smp,
			   int seq_no, struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	char buf[LNET_MAX_STR_LEN];
	int rc = LUSTRE_CFG_RC_NO_ERR, num_of_nets = 0;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if ((intf == NULL) || (net == NULL)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Mandatory parameter not specified");
		rc = LUSTRE_CFG_RC_MISSING_PARAM;
		goto fn_exit;
	}

	if ((peer_to != -1) && (peer_to <= 0)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "peer timeout [%d] must be a > 0",
			 peer_to);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	snprintf(buf, LNET_MAX_STR_LEN - 1, "%s(%s)%s",
		 net, intf,
		 (smp) ? smp : "");

	buf[LNET_MAX_STR_LEN - 1] = '\0';

	LIBCFS_IOC_INIT_V2(data);
	strncpy(data.ioc_config_u.net.intf, buf, LNET_MAX_STR_LEN);
	data.ioc_config_u.net.peer_to = peer_to;
	data.ioc_config_u.net.peer_cr = peer_cr;
	data.ioc_config_u.net.peer_buf_cr = peer_buf_cr;
	data.ioc_config_u.net.credits = credits;

	num_of_nets = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_NET, &data);
	if (num_of_nets < 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_NET failed - %s", strerror(errno));
		rc = -errno;
	}

fn_exit:
	cYAML_build_error((num_of_nets >= 0) ? num_of_nets : rc,
			 seq_no, CONFIG_CMD, "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_del_net(char *nw, int seq_no, struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int rc = LUSTRE_CFG_RC_NO_ERR;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (nw == NULL) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Missing mandatory parameter");
		rc = LUSTRE_CFG_RC_MISSING_PARAM;
		goto fn_exit;
	}

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	net = libcfs_str2net(nw);
	if (net == LNET_NIDNET(LNET_NID_ANY)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Can't parse net %s", nw);
		rc = LUSTRE_CFG_RC_BAD_PARAM;
		goto fn_exit;
	}

	LIBCFS_IOC_INIT_V2(data);
	data.ioc_net = net;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_DEL_NET, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_DEL_NET failed - %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, DEL_CMD, "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_net(char *nw, int detail, int seq_no,
			 struct cYAML **show_rc, struct cYAML **err_rc)
{
	char *buf;
	struct libcfs_ioctl_config_data *data;
	struct libcfs_ioctl_net_config *net_config;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM, i, j;
	struct cYAML *root = NULL, *tunables = NULL,
		*net_node = NULL, *interfaces = NULL,
		*item = NULL;
	int str_buf_len = LNET_MAX_SHOW_NUM_CPT * 2;
	char str_buf[str_buf_len];
	char *pos = str_buf;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Out of memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	buf = calloc(sizeof(char),
		     sizeof(struct libcfs_ioctl_config_data) +
		     sizeof(struct libcfs_ioctl_net_config));
	if (buf == NULL)
		goto fn_exit;

	data = (struct libcfs_ioctl_config_data *)buf;

	if (nw != NULL) {
		net = libcfs_str2net(nw);
		if (net == LNET_NIDNET(LNET_NID_ANY)) {
			snprintf(err_str,
				 LNET_MAX_STR_LEN,
				 "Can't parse net %s", nw);
			rc = LUSTRE_CFG_RC_BAD_PARAM;
			goto fn_exit;
		}
	}

	root = cYAML_create_object(NULL, NULL);
	if (!root)
		goto fn_exit;

	net_node = cYAML_create_seq(root, "net");
	if (net_node == NULL)
		goto fn_exit;

	for (i = 0;; i++) {
		LIBCFS_IOC_INIT_V2((*data));
		/*
		 * set the ioc_len to the proper value since INIT assumes
		 * size of data
		 */
		data->hdr.ioc_len = sizeof(struct libcfs_ioctl_config_data) +
		  sizeof(struct libcfs_ioctl_net_config);
		data->ioc_count = i;

		rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_NET, data);
		if (rc != 0)
			break;

		/* filter on provided data */
		if ((net != LNET_NIDNET(LNET_NID_ANY)) &&
		    (net != LNET_NIDNET(data->ioc_nid)))
			continue;

		/* default rc to -1 in case we hit the goto */
		rc = -1;

		net_config = (struct libcfs_ioctl_net_config *)data->ioc_bulk;

		/* create the tree to be printed. */
		item = cYAML_create_seq_item(net_node);
		if (item == NULL)
			goto fn_exit;

		if (cYAML_create_string(item,
					"nid",
					libcfs_nid2str(data->ioc_nid)) == NULL)
			goto fn_exit;

		if (cYAML_create_string(item,
					"status",
					(net_config->ni_status ==
					  LNET_NI_STATUS_UP) ?
					    "up" : "down") == NULL)
			goto fn_exit;

		interfaces = cYAML_create_object(item, "interfaces");
		if (interfaces == NULL)
			goto fn_exit;

		for (j = 0; j < LNET_MAX_INTERFACES; j++) {
			if (strlen(net_config->ni_interfaces[j]) > 0) {
				sprintf(str_buf, "intf-%d", j);
				if (cYAML_create_string(interfaces, str_buf,
						net_config->ni_interfaces[j])
				    == NULL)
					goto fn_exit;
			}
		}

		if (detail) {
			tunables = cYAML_create_object(item, "tunables");
			if (tunables == NULL)
				goto fn_exit;

			if (cYAML_create_number(tunables, "peer_timeout",
						data->ioc_config_u.net.peer_to)
			    == NULL)
				goto fn_exit;

			if (cYAML_create_number(tunables, "peer_credits",
						data->ioc_config_u.net.peer_cr)
			    == NULL)
				goto fn_exit;

			if (cYAML_create_number(tunables,
						"peer_buffer_credits",
						data->ioc_config_u.net.
						  peer_buf_cr) == NULL)
				goto fn_exit;

			if (cYAML_create_number(tunables, "credits",
						data->ioc_config_u.net.credits)
			    == NULL)
				goto fn_exit;

			for (j = 0 ; (data->ioc_ncpts > 1) &&
			     (j < data->ioc_ncpts); j++) {
				pos += snprintf(str_buf,
						str_buf + str_buf_len - pos,
						" %d", net_config->cpts[j]);
			}

			if ((data->ioc_ncpts > 1) &&
			    (!cYAML_create_string(tunables, "CPTs",
						str_buf)))
				goto fn_exit;
		}
	}

	/* Print out the net information only if show_rc is not provided */
	if (show_rc == NULL)
		cYAML_print_tree(root);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting networks - %s. check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((show_rc == NULL) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc != NULL) {
		if ((*show_rc) != NULL) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, SHOW_CMD, "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_enable_routing(int enable, int seq_no, struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	LIBCFS_IOC_INIT_V2(data);
	data.ioc_config_u.buffers.enable = (enable) ? 1 : 0;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ENABLE_RTR, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ENABLE_RTR failed - %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no,
			 (enable) ? CONFIG_CMD : DEL_CMD,
			 "routing", err_str, err_rc);
	return rc;
}

int lustre_lnet_config_buf(int tiny, int small, int large, int seq_no,
			   struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data data;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	/* -1 indicates to ignore changes to this field */
	if ((tiny < -1) || (small < -1) || (large < -1)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "tiny, small and large must be >= 0");
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	LIBCFS_IOC_INIT_V2(data);
	data.ioc_config_u.buffers.tiny = tiny;
	data.ioc_config_u.buffers.small = small;
	data.ioc_config_u.buffers.large = large;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_BUF, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_BUF failed - %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, DEL_CMD, "buf", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_buf(int seq_no, struct cYAML **show_rc,
			 struct cYAML **err_rc)
{
	struct libcfs_ioctl_config_data *data;
	struct libcfs_ioctl_pool_cfg *pool_cfg;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	char *buf;
	char *pools[LNET_NRBPOOLS] = {"tiny", "small", "large"};
	struct cYAML *root = NULL, *pools_node = NULL,
		     *type_node = NULL, *item = NULL;
	int i;
	char err_str[LNET_MAX_STR_LEN];
	char node_name[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Out of memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	buf = calloc(sizeof(char),
		     sizeof(struct libcfs_ioctl_config_data) +
		     sizeof(struct libcfs_ioctl_pool_cfg));

	if (buf == NULL)
		goto fn_exit;

	data = (struct libcfs_ioctl_config_data *)buf;

	root = cYAML_create_object(NULL, NULL);
	if (root == NULL)
		goto fn_exit;

	pools_node = cYAML_create_seq(root, "buffers");
	if (pools_node == NULL)
		goto fn_exit;

	for (i = 0;; i++) {
		LIBCFS_IOC_INIT_V2((*data));
		data->hdr.ioc_len = sizeof(struct libcfs_ioctl_config_data) +
				    sizeof(struct libcfs_ioctl_pool_cfg);
		data->ioc_count = i;

		rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_BUF, data);
		if (rc != 0)
			break;

		pool_cfg = (struct libcfs_ioctl_pool_cfg *)data->ioc_bulk;

		snprintf(node_name, LNET_MAX_STR_LEN, "cpt[%d]", i);
		item = cYAML_create_seq_item(pools_node);
		if (item == NULL)
			goto fn_exit;

		/* create the tree  and print */
		for (i = 0; i < LNET_NRBPOOLS; i++) {
			type_node = cYAML_create_object(item, pools[i]);
			if (cYAML_create_number(type_node, "npages",
						pool_cfg->pools[i].npages)
			    == NULL)
				goto fn_exit;
			if (cYAML_create_number(type_node, "nbuffers",
						pool_cfg->pools[i].nbuffers)
			    == NULL)
				goto fn_exit;
			if (cYAML_create_number(type_node, "credits",
						pool_cfg->pools[i].credits)
			    == NULL)
				goto fn_exit;
			if (cYAML_create_number(type_node, "mincredits",
						pool_cfg->pools[i].mincredits)
			    == NULL)
				goto fn_exit;
		}
	}

	if (show_rc == NULL)
		cYAML_print_tree(root);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting buffers - %s. check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
	rc = LUSTRE_CFG_RC_NO_ERR;

fn_exit:
	free(buf);
	if ((show_rc == NULL) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc != NULL) {
		if ((*show_rc) != NULL) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, SHOW_CMD, "buf", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_peer_credits(int seq_no, struct cYAML **show_rc,
				  struct cYAML **err_rc)
{
	struct libcfs_ioctl_peer peer_info;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM, ncpt = 0, i = 0, j = 0;
	struct cYAML *root = NULL, *peer = NULL;
	char err_str[LNET_MAX_STR_LEN];
	bool ncpt_set = false;

	snprintf(err_str, LNET_MAX_STR_LEN,
		 "Out of Memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	/* create struct cYAML root object */
	root = cYAML_create_object(NULL, NULL);
	if (root == NULL)
		goto fn_exit;

	do {
		for (i = 0;; i++) {
			LIBCFS_IOC_INIT_V2(peer_info);
			peer_info.ioc_count = i;
			peer_info.lnd_u.peer_credits.ncpt = j;
			rc = l_ioctl(LNET_DEV_ID,
				     IOC_LIBCFS_GET_PEER_INFO, &peer_info);
			if (rc != 0)
				break;

			if (ncpt_set != 0) {
				ncpt = peer_info.lnd_u.peer_credits.ncpt;
				ncpt_set = true;
			}

			peer = cYAML_create_object(root, "peer");
			if (peer == NULL)
				goto fn_exit;

			if (cYAML_create_string(peer, "nid",
						libcfs_nid2str
						 (peer_info.ioc_nid)) == NULL)
				goto fn_exit;

			if (cYAML_create_string(peer, "state",
						peer_info.lnd_u.
						  peer_credits.aliveness)
			    == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "refcount",
						peer_info.lnd_u.peer_credits.
						  refcount) == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "max_ni_tx_credits",
						peer_info.lnd_u.peer_credits.
						  ni_peertxcredits) == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "available_tx_credits",
						peer_info.lnd_u.peer_credits.
						  peertxcredits) == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "available_rtr_credits",
						peer_info.lnd_u.peer_credits.
						  peerrtrcredits) == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "min_rtr_credits",
						peer_info.lnd_u.peer_credits.
						  peerminrtrcredtis) == NULL)
				goto fn_exit;

			if (cYAML_create_number(peer, "tx_q_num_of_buf",
						peer_info.lnd_u.peer_credits.
						  peertxqnob) == NULL)
				goto fn_exit;
		}

		if (errno != ENOENT) {
			snprintf(err_str,
				LNET_MAX_STR_LEN,
				"IOC_LIBCFS_GET_PEER_INFO failed - %s",
				strerror(errno));
			rc = -errno;
			goto fn_exit;
		}

		j++;
	} while (j < ncpt);

	/* print output iff show_rc is not provided */
	if (show_rc == NULL)
		cYAML_print_tree(root);

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
	rc = LUSTRE_CFG_RC_NO_ERR;

fn_exit:
	if ((show_rc == NULL) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc != NULL) {
		if ((*show_rc) != NULL) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, SHOW_CMD, "peer_credits", err_str, err_rc);
	return rc;

}

int lustre_lnet_show_stats(int seq_no, struct cYAML **show_rc,
			   struct cYAML **err_rc)
{
	struct libcfs_ioctl_lnet_stats data;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	char err_str[LNET_MAX_STR_LEN];
	struct cYAML *root = NULL, *stats = NULL;

	snprintf(err_str, LNET_MAX_STR_LEN, "Out of memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	LIBCFS_IOC_INIT_V2(data);

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_LNET_STATS, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting lnet statistics - %s. check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

	root = cYAML_create_object(NULL, NULL);
	if (root == NULL)
		goto fn_exit;

	stats = cYAML_create_object(root, "statistics");
	if (stats == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "msgs_alloc",
				data.cntrs.msgs_alloc) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "msgs_max",
				data.cntrs.msgs_max) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "errors",
				data.cntrs.errors) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "send_count",
				data.cntrs.send_count) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "recv_count",
				data.cntrs.recv_count) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "route_count",
				data.cntrs.route_count) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "drop_count",
				data.cntrs.drop_count) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "send_length",
				data.cntrs.send_length) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "recv_length",
				data.cntrs.recv_length) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "route_length",
				data.cntrs.route_length) == NULL)
		goto fn_exit;

	if (cYAML_create_number(stats, "drop_length",
				data.cntrs.drop_length) == NULL)
		goto fn_exit;

	if (show_rc == NULL)
		cYAML_print_tree(root);

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((show_rc != NULL) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc != NULL) {
		if ((*show_rc) != NULL) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, SHOW_CMD, "statistics", err_str, err_rc);
	return rc;
}

typedef int (*cmd_handler_t)(struct cYAML *tree,
			     struct cYAML **show_rc,
			     struct cYAML **err_rc);

static int handle_yaml_config_route(struct cYAML *tree, struct cYAML **show_rc,
				    struct cYAML **err_rc)
{
	struct cYAML *net, *gw, *hop, *prio, *seq_no;

	net = cYAML_get_object_item(tree, "net");
	gw = cYAML_get_object_item(tree, "gateway");
	hop = cYAML_get_object_item(tree, "hop");
	prio = cYAML_get_object_item(tree, "priority");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_config_route((net) ? net->valuestring : NULL,
					(gw) ? gw->valuestring : NULL,
					(hop) ? hop->valueint : -1,
					(prio) ? prio->valueint : -1,
					(seq_no) ? seq_no->valueint : -1,
					err_rc);
}

static int handle_yaml_config_net(struct cYAML *tree, struct cYAML **show_rc,
				  struct cYAML **err_rc)
{
	struct cYAML *net, *intf, *tunables, *seq_no,
	      *peer_to = NULL, *peer_buf_cr = NULL, *peer_cr = NULL,
	      *credits = NULL, *smp = NULL;

	net = cYAML_get_object_item(tree, "net");
	intf = cYAML_get_object_item(tree, "interface");

	tunables = cYAML_get_object_item(tree, "tunables");
	if (tunables != NULL) {
		peer_to = cYAML_get_object_item(tunables, "peer_timeout");
		peer_cr = cYAML_get_object_item(tunables, "peer_credits");
		peer_buf_cr = cYAML_get_object_item(tunables,
						    "peer_buffer_credits");
		credits = cYAML_get_object_item(tunables, "credits");
		smp = cYAML_get_object_item(tunables, "SMP");
	}
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_config_net((net) ? net->valuestring : NULL,
				      (intf) ? intf->valuestring : NULL,
				      (peer_to) ? peer_to->valueint : -1,
				      (peer_cr) ? peer_cr->valueint : -1,
				      (peer_buf_cr) ?
					peer_buf_cr->valueint : -1,
				      (credits) ? credits->valueint : -1,
				      (smp) ? smp->valuestring : NULL,
				      (seq_no) ? seq_no->valueint : -1,
				      err_rc);
}

static int handle_yaml_config_buf(struct cYAML *tree, struct cYAML **show_rc,
				  struct cYAML **err_rc)
{
	int rc;
	struct cYAML *tiny, *small, *large, *seq_no, *enable;

	tiny = cYAML_get_object_item(tree, "tiny");
	small = cYAML_get_object_item(tree, "small");
	large = cYAML_get_object_item(tree, "large");
	seq_no = cYAML_get_object_item(tree, "seq_no");
	enable = cYAML_get_object_item(tree, "enable");

	rc = lustre_lnet_config_buf((tiny) ? tiny->valueint : -1,
				    (small) ? small->valueint : -1,
				    (large) ? large->valueint : -1,
				    (seq_no) ? seq_no->valueint : -1,
				    err_rc);
	if (rc != LUSTRE_CFG_RC_NO_ERR)
		return rc;

	if (enable) {
		rc = lustre_lnet_enable_routing(enable->valueint,
						(seq_no) ?
						    seq_no->valueint : -1,
						err_rc);
		if (rc != LUSTRE_CFG_RC_NO_ERR)
			return rc;
	}

	return rc;
}

static int handle_yaml_del_route(struct cYAML *tree, struct cYAML **show_rc,
				 struct cYAML **err_rc)
{
	struct cYAML *net;
	struct cYAML *gw;
	struct cYAML *seq_no;

	net = cYAML_get_object_item(tree, "net");
	gw = cYAML_get_object_item(tree, "gateway");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_del_route((net) ? net->valuestring : NULL,
				     (gw) ? gw->valuestring : NULL,
				     (seq_no) ? seq_no->valueint : -1,
				     err_rc);
}

static int handle_yaml_del_net(struct cYAML *tree, struct cYAML **show_rc,
			       struct cYAML **err_rc)
{
	struct cYAML *net, *seq_no;

	net = cYAML_get_object_item(tree, "net");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_del_net((net) ? net->valuestring : NULL,
				   (seq_no) ? seq_no->valueint : -1,
				   err_rc);
}

static int handle_yaml_del_buf(struct cYAML *tree, struct cYAML **show_rc,
			       struct cYAML **err_rc)
{
	struct cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_config_buf(0, 0, 0,
				      (seq_no) ? seq_no->valueint : -1,
				      err_rc);
}

static int handle_yaml_show_route(struct cYAML *tree, struct cYAML **show_rc,
				  struct cYAML **err_rc)
{
	struct cYAML *net;
	struct cYAML *gw;
	struct cYAML *hop;
	struct cYAML *prio;
	struct cYAML *detail;
	struct cYAML *seq_no;

	net = cYAML_get_object_item(tree, "net");
	gw = cYAML_get_object_item(tree, "gateway");
	hop = cYAML_get_object_item(tree, "hop");
	prio = cYAML_get_object_item(tree, "priority");
	detail = cYAML_get_object_item(tree, "detail");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_route((net) ? net->valuestring : NULL,
				      (gw) ? gw->valuestring : NULL,
				      (hop) ? hop->valueint : -1,
				      (prio) ? prio->valueint : -1,
				      (detail) ? detail->valueint : 0,
				      (seq_no) ? seq_no->valueint : -1,
				      show_rc,
				      err_rc);
}

static int handle_yaml_show_net(struct cYAML *tree, struct cYAML **show_rc,
				struct cYAML **err_rc)
{
	struct cYAML *net, *detail, *seq_no;

	net = cYAML_get_object_item(tree, "net");
	detail = cYAML_get_object_item(tree, "detail");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_net((net) ? net->valuestring : NULL,
				    (detail) ? detail->valueint : 0,
				    (seq_no) ? seq_no->valueint : -1,
				    show_rc,
				    err_rc);
}

static int handle_yaml_show_buf(struct cYAML *tree, struct cYAML **show_rc,
				struct cYAML **err_rc)
{
	struct cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_buf((seq_no) ? seq_no->valueint : -1,
				    show_rc, err_rc);
}

static int handle_yaml_show_credits(struct cYAML *tree, struct cYAML **show_rc,
				    struct cYAML **err_rc)
{
	struct cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_peer_credits((seq_no) ? seq_no->valueint : -1,
					     show_rc, err_rc);
}

static int handle_yaml_show_stats(struct cYAML *tree, struct cYAML **show_rc,
				  struct cYAML **err_rc)
{
	struct cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_stats((seq_no) ? seq_no->valueint : -1,
				      show_rc, err_rc);
}

struct lookup_cmd_hdlr_tbl {
	char *name;
	cmd_handler_t cb;
};

static struct lookup_cmd_hdlr_tbl lookup_config_tbl[] = {
	{"route", handle_yaml_config_route},
	{"net", handle_yaml_config_net},
	{"buffer", handle_yaml_config_buf},
	{NULL, NULL}
};

static struct lookup_cmd_hdlr_tbl lookup_del_tbl[] = {
	{"route", handle_yaml_del_route},
	{"net", handle_yaml_del_net},
	{"buffer", handle_yaml_del_buf},
	{NULL, NULL}
};

static struct lookup_cmd_hdlr_tbl lookup_show_tbl[] = {
	{"route", handle_yaml_show_route},
	{"net", handle_yaml_show_net},
	{"buffer", handle_yaml_show_buf},
	{"credits", handle_yaml_show_credits},
	{"statistics", handle_yaml_show_stats},
	{NULL, NULL}
};

static cmd_handler_t lookup_fn(char *key,
			       struct lookup_cmd_hdlr_tbl *tbl)
{
	int i;
	for (i = 0; tbl[i].name != NULL; i++) {
		if (strncmp(key, tbl[i].name, strlen(tbl[i].name)) == 0)
			return tbl[i].cb;
	}

	return NULL;
}

static int lustre_yaml_cb_helper(char *f, struct lookup_cmd_hdlr_tbl *table,
				 struct cYAML **show_rc, struct cYAML **err_rc)
{
	struct cYAML *tree, *item = NULL, *head;
	cmd_handler_t cb;
	char err_str[LNET_MAX_STR_LEN];
	int rc = 0;

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	tree = cYAML_build_tree(f, NULL, 0, err_rc);
	if (tree == NULL)
		return LUSTRE_CFG_RC_BAD_PARAM;

	while ((head = cYAML_get_next_seq_item(tree->child, &item)) != NULL) {
		cb = lookup_fn(head->string, table);
		if (cb != NULL)
			rc = cb(head, show_rc, err_rc);

		/* if processing fails or no cb is found then fail */
		if ((rc != LUSTRE_CFG_RC_NO_ERR) || (cb == NULL)) {
			sprintf(err_str,
				"Failed to process request -  %s [%d, %p]",
				head->string, rc, cb);
			cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM, -1,
					 "yaml", "helper", err_str, err_rc);
		}
	}

	cYAML_free_tree(tree);

	return LUSTRE_CFG_RC_NO_ERR;
}

int lustre_yaml_config(char *f, struct cYAML **err_rc)
{
	if (f == NULL) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, CONFIG_CMD, "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_config_tbl,
				     NULL, err_rc);
}

int lustre_yaml_del(char *f, struct cYAML **err_rc)
{
	if (f == NULL) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, DEL_CMD, "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_del_tbl,
				     NULL, err_rc);
}

int lustre_yaml_show(char *f, struct cYAML **show_rc, struct cYAML **err_rc)
{
	if (f == NULL) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, SHOW_CMD, "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_show_tbl,
				     show_rc, err_rc);
}
