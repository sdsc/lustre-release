/*
 * ARGUMENT
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
#include <libcYAML.h>
#include <liblustreconfigapi.h>

static bool g_lib_init = false;

static void lustre_config_api_init()
{
	register_ioc_dev(LNET_DEV_ID, LNET_DEV_PATH,
			 LNET_DEV_MAJOR, LNET_DEV_MINOR);
}

int lustre_lnet_config_route(char *nw, char *gw,
			     int hops,
			     int prio,
			     int seq_no,
			     cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (!nw || !gw) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Missing mandatory parameter(s): %s%s",
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
	else if ((hops < 1) && (hops > 255)) {
		snprintf(err_str,
			LNET_MAX_STR_LEN,
			"Num of hops must be 0 < hops < 256. hops = %d",
			hops);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	/* TODO: add prio support */

	LIBCFS_IOC_INIT(data);
	data.ioc_net = net;
	data.ioc_config_u.route.hop = (unsigned int) hops;
	data.ioc_nid = gateway_nid;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_ROUTE, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_ROUTE failed: %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, "config", "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_del_route(char *nw, char *gw,
			  int seq_no, cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (!nw || !gw) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Missing mandatory parameter(s): %s%s",
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

	LIBCFS_IOC_INIT(data);
	data.ioc_net = net;
	data.ioc_nid = gateway_nid;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_DEL_ROUTE, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_DEL_ROUTE failed: %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, "del", "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_route(char *nw, char *gw,
			   int hops, int prio, int detail,
			   int seq_no,
			   cYAML **show_rc,
			   cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	lnet_nid_t gateway_nid;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int i;
	cYAML *root = NULL, *route = NULL;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN,
		 "Out of Memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if (nw) {
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

	if (gw) {
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

	if ((hops < 1) && (hops != -1) && (hops > 255)) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Num of hops must be 0 < hops < 256.  hops = %d\n",
			 hops);
		rc = LUSTRE_CFG_RC_OUT_OF_RANGE_PARAM;
		goto fn_exit;
	}

	/* create cYAML root object */
	root = cYAML_create_object(NULL, NULL);
	if (!root)
		goto fn_exit;

	/* TODO: add prio support */
	for (i = 0;; i++) {
		LIBCFS_IOC_INIT(data);
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

		/* default rc to -1 incase we hit the goto */
		rc = -1;

		route = cYAML_create_object(root, "route");
		if (!route)
			goto fn_exit;

		if (!cYAML_create_string
			(route, "net",
			 libcfs_net2str(data.ioc_net)))
			goto fn_exit;

		if (detail) {
			if (!cYAML_create_string(route, "gateway",
						 libcfs_nid2str(data.ioc_nid)))
				goto fn_exit;

			if (!cYAML_create_number(route, "hop",
						 data.ioc_config_u.route.hop))
				goto fn_exit;

			if (!cYAML_create_number(route, "priority",
						 data.ioc_config_u.
						 route.priority))
				goto fn_exit;

			if (!cYAML_create_string(route, "state",
						 data.ioc_config_u.route.flags ?
						 "up" : "down"))
				goto fn_exit;
		}
	}

	/* print output iff show_rc is not provided */
	if (!show_rc)
		cYAML_print_tree(root, 0);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting routes: %s: check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((!show_rc) || (rc != LUSTRE_CFG_RC_NO_ERR)) {
		cYAML_free_tree(root);
	} else if (show_rc) {
		if (*show_rc) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, "show", "route", err_str, err_rc);
	return rc;
}

int lustre_lnet_config_net(char *net,
			   char *intf,
			   int peer_to,
			   int peer_cr,
			   int peer_buf_cr,
			   int credits,
			   char *smp,
			   int seq_no,
			   cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	char buf[LNET_MAX_STR_LEN];
	int rc = LUSTRE_CFG_RC_NO_ERR, num_of_nets = 0;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	if ((!intf) || (!net)) {
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

	LIBCFS_IOC_INIT(data);
	strncpy(data.ioc_config_u.net.intf, buf, LNET_MAX_STR_LEN);
	data.ioc_config_u.net.peer_to = peer_to;
	data.ioc_config_u.net.peer_cr = peer_cr;
	data.ioc_config_u.net.peer_buf_cr = peer_buf_cr;
	data.ioc_config_u.net.credits = credits;

	num_of_nets = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_NET, &data);
	if (num_of_nets < 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_NET failed: %s", strerror(errno));
		rc = -errno;
	}

fn_exit:
	cYAML_build_error((num_of_nets >= 0) ? num_of_nets : rc,
			 seq_no, "config", "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_del_net(char *nw,
			int seq_no,
			cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int rc = LUSTRE_CFG_RC_NO_ERR;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!nw) {
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

	LIBCFS_IOC_INIT(data);
	data.ioc_net = net;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_DEL_NET, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_DEL_NET failed: %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, "del", "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_net(char *nw, int detail, int seq_no,
			 cYAML **show_rc, cYAML **err_rc)
{
	char *buf;
	struct libcfs_ioctl_config_data_s *data;
	struct libcfs_ioctl_net_config_s *net_config;
	__u32 net = LNET_NIDNET(LNET_NID_ANY);
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM, i, j;
	cYAML *root = NULL, *tunables = NULL,
	      *net_node = NULL, *interfaces = NULL;
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
		     sizeof(struct libcfs_ioctl_config_data_s) +
		     sizeof(struct libcfs_ioctl_net_config_s));
	if (!buf)
		goto fn_exit;

	data = (struct libcfs_ioctl_config_data_s *)buf;

	if (nw) {
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

	for (i = 0;; i++) {
		LIBCFS_IOC_INIT((*data));
		/*
		 * set the ioc_len to the proper value since INIT assumes
		 * size of data
		 */
		data->hdr.ioc_len = sizeof(struct libcfs_ioctl_config_data_s) +
		  sizeof(struct libcfs_ioctl_net_config_s);
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

		net_config = (struct libcfs_ioctl_net_config_s *)data->ioc_bulk;

		/* create the tree to be printed. */
		net_node = cYAML_create_object(root, "net");
		if (!net_node)
			goto fn_exit;

		if (!cYAML_create_string(net_node,
					 "nid",
					 libcfs_nid2str(data->ioc_nid)))
			goto fn_exit;

		if (!cYAML_create_string(net_node,
					 "status",
					 (net_config->ni_status ==
					  LNET_NI_STATUS_UP) ?
					    "up" : "down"))
			goto fn_exit;

		interfaces = cYAML_create_object(net_node, "interfaces");
		if (!interfaces)
			goto fn_exit;

		for (j = 0; j < LNET_MAX_INTERFACES; j++) {
			if (strlen(net_config->ni_interfaces[j]) > 0) {
				sprintf(str_buf, "intf-%d", j);
				if (!cYAML_create_string(interfaces, str_buf,
						net_config->ni_interfaces[j]))
					goto fn_exit;
			}
		}

		if (detail) {
			tunables = cYAML_create_object(net_node, "tunables");
			if (!tunables)
				goto fn_exit;

			if (!cYAML_create_number(tunables, "peer_timeout",
						data->ioc_config_u.net.peer_to))
				goto fn_exit;

			if (!cYAML_create_number(tunables, "peer_credits",
						data->ioc_config_u.net.peer_cr))
				goto fn_exit;

			if (!cYAML_create_number(tunables,
						 "peer_buffer_credits",
						 data->ioc_config_u.net.
						   peer_buf_cr))
				goto fn_exit;

			if (!cYAML_create_number(tunables, "credits",
						data->ioc_config_u.net.credits))
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
	if (!show_rc)
		cYAML_print_tree(root, 0);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting networks: %s: check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((!show_rc) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc) {
		if (*show_rc) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, "show", "net", err_str, err_rc);
	return rc;
}

int lustre_lnet_enable_routing(int enable,
			       int seq_no,
			       cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
	int rc = LUSTRE_CFG_RC_NO_ERR;
	char err_str[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	LIBCFS_IOC_INIT(data);
	data.ioc_config_u.buffers.enable = (enable) ? 1 : 0;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ENABLE_RTR, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ENABLE_RTR failed: %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no,
			 (enable) ? "config" : "del",
			 "routing", err_str, err_rc);
	return rc;
}

int lustre_lnet_config_buf(int tiny,
			   int small,
			   int large,
			   int seq_no,
			   cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s data;
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

	LIBCFS_IOC_INIT(data);
	data.ioc_config_u.buffers.tiny = tiny;
	data.ioc_config_u.buffers.small = small;
	data.ioc_config_u.buffers.large = large;

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_ADD_BUF, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "IOC_LIBCFS_ADD_BUF failed: %s", strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

fn_exit:
	cYAML_build_error(rc, seq_no, "config", "buf", err_str, err_rc);
	return rc;
}

int lustre_lnet_show_buf(int seq_no, cYAML **show_rc, cYAML **err_rc)
{
	struct libcfs_ioctl_config_data_s *data;
	struct libcfs_ioctl_pool_cfg_s *pool_cfg;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	char *buf;
	char *pools[LNET_NRBPOOLS] = {"tiny", "small", "large"};
	cYAML *root = NULL, *pools_node = NULL, *type_node = NULL;
	int i;
	char err_str[LNET_MAX_STR_LEN];
	char node_name[LNET_MAX_STR_LEN];

	snprintf(err_str, LNET_MAX_STR_LEN, "Out of memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	buf = calloc(sizeof(char),
		     sizeof(struct libcfs_ioctl_config_data_s) +
		     sizeof(struct libcfs_ioctl_pool_cfg_s));

	if (!buf)
		goto fn_exit;

	data = (struct libcfs_ioctl_config_data_s *)buf;

	root = cYAML_create_object(NULL, NULL);
	if (!root)
		goto fn_exit;

	for (i = 0;; i++) {
		LIBCFS_IOC_INIT((*data));
		data->hdr.ioc_len = sizeof(struct libcfs_ioctl_config_data_s) +
				    sizeof(struct libcfs_ioctl_pool_cfg_s);
		data->ioc_count = i;

		rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_BUF, data);
		if (rc != 0)
			break;

		pool_cfg = (struct libcfs_ioctl_pool_cfg_s *)data->ioc_bulk;

		snprintf(node_name, LNET_MAX_STR_LEN, "pools[%d]", i);
		pools_node = cYAML_create_object(root, node_name);
		if (!pools_node)
			goto fn_exit;

		/* create the tree  and print */
		for (i = 0; i < LNET_NRBPOOLS; i++) {
			type_node = cYAML_create_object(pools_node, pools[i]);
			if (!cYAML_create_number(type_node, "npages",
						pool_cfg->pools[i].npages))
				goto fn_exit;
			if (!cYAML_create_number(type_node, "nbuffers",
						pool_cfg->pools[i].nbuffers))
				goto fn_exit;
			if (!cYAML_create_number(type_node, "credits",
						pool_cfg->pools[i].credits))
				goto fn_exit;
			if (!cYAML_create_number(type_node, "mincredits",
						pool_cfg->pools[i].mincredits))
				goto fn_exit;
		}
	}

	if (!show_rc)
		cYAML_print_tree(root, 0);

	if (errno != ENOENT) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting buffers: %s: check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	} else
		rc = LUSTRE_CFG_RC_NO_ERR;

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
	rc = LUSTRE_CFG_RC_NO_ERR;

fn_exit:
	free(buf);
	if ((!show_rc) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc) {
		if (*show_rc) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, "show", "buf", err_str, err_rc);
	return rc;
}

extern int lustre_lnet_show_peer_credits(int seq_no,
					 cYAML **show_rc,
					 cYAML **err_rc)
{
	struct libcfs_ioctl_peer peer_info;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM, ncpt = 0, i = 0, j = 0;
	cYAML *root = NULL, *peer = NULL;
	char err_str[LNET_MAX_STR_LEN];
	bool ncpt_set = false;

	snprintf(err_str, LNET_MAX_STR_LEN,
		 "Out of Memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	/* create cYAML root object */
	root = cYAML_create_object(NULL, NULL);
	if (!root)
		goto fn_exit;

	do {
		for (i = 0;; i++) {
			LIBCFS_IOC_INIT(peer_info);
			peer_info.ioc_count = i;
			peer_info.lnd_u.peer_credits.ncpt = j;
			rc = l_ioctl(LNET_DEV_ID,
				     IOC_LIBCFS_GET_PEER_INFO, &peer_info);
			if (rc != 0)
				break;

			if (!ncpt_set) {
				ncpt = peer_info.lnd_u.peer_credits.ncpt;
				ncpt_set = true;
			}

			peer = cYAML_create_object(root, "peer");
			if (!peer)
				goto fn_exit;

			if (!cYAML_create_string(peer, "nid",
						 libcfs_nid2str
						  (peer_info.ioc_nid)))
				goto fn_exit;

			if (!cYAML_create_string(peer, "state",
						 peer_info.lnd_u.
						   peer_credits.aliveness))
				goto fn_exit;

			if (!cYAML_create_number(peer, "refcount",
						peer_info.lnd_u.peer_credits.
						  refcount))
				goto fn_exit;

			if (!cYAML_create_number(peer, "max_ni_tx_credits",
						peer_info.lnd_u.peer_credits.
						  ni_peertxcredits))
				goto fn_exit;

			if (!cYAML_create_number(peer, "available_tx_credits",
						peer_info.lnd_u.peer_credits.
						  peertxcredits))
				goto fn_exit;

			if (!cYAML_create_number(peer, "available_rtr_credits",
						peer_info.lnd_u.peer_credits.
						  peerrtrcredits))
				goto fn_exit;

			if (!cYAML_create_number(peer, "min_rtr_credits",
						peer_info.lnd_u.peer_credits.
						  peerminrtrcredtis))
				goto fn_exit;

			if (!cYAML_create_number(peer, "tx_q_num_of_buf",
						peer_info.lnd_u.peer_credits.
						  peertxqnob))
				goto fn_exit;
		}

		if (errno != ENOENT) {
			snprintf(err_str,
				LNET_MAX_STR_LEN,
				"IOC_LIBCFS_GET_PEER_INFO failed: %s",
				strerror(errno));
			rc = -errno;
			goto fn_exit;
		}

		j++;
	} while (j < ncpt);

	/* print output iff show_rc is not provided */
	if (!show_rc)
		cYAML_print_tree(root, 0);

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
	rc = LUSTRE_CFG_RC_NO_ERR;

fn_exit:
	if ((!show_rc) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc) {
		if (*show_rc) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, "show", "peer_credits", err_str, err_rc);
	return rc;

}

extern int lustre_lnet_show_stats(int seq_no, cYAML **show_rc,
				  cYAML **err_rc)
{
	struct libcfs_ioctl_lnet_stats data;
	int rc = LUSTRE_CFG_RC_OUT_OF_MEM;
	char err_str[LNET_MAX_STR_LEN];
	cYAML *root = NULL, *stats = NULL;

	snprintf(err_str, LNET_MAX_STR_LEN, "Out of memory");

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	LIBCFS_IOC_INIT(data);

	rc = l_ioctl(LNET_DEV_ID, IOC_LIBCFS_GET_LNET_STATS, &data);
	if (rc != 0) {
		snprintf(err_str,
			 LNET_MAX_STR_LEN,
			 "Error getting lnet statistics: %s: check dmesg.",
			 strerror(errno));
		rc = -errno;
		goto fn_exit;
	}

	root = cYAML_create_object(NULL, NULL);
	if (!root)
		goto fn_exit;

	stats = cYAML_create_object(root, "statistics");
	if (!stats)
		goto fn_exit;

	if (!cYAML_create_number(stats, "msgs_alloc",
				 data.cntrs.msgs_alloc))
		goto fn_exit;

	if (!cYAML_create_number(stats, "msgs_max",
				 data.cntrs.msgs_max))
		goto fn_exit;

	if (!cYAML_create_number(stats, "errors",
				 data.cntrs.errors))
		goto fn_exit;

	if (!cYAML_create_number(stats, "send_count",
				 data.cntrs.send_count))
		goto fn_exit;

	if (!cYAML_create_number(stats, "recv_count",
				 data.cntrs.recv_count))
		goto fn_exit;

	if (!cYAML_create_number(stats, "route_count",
				 data.cntrs.route_count))
		goto fn_exit;

	if (!cYAML_create_number(stats, "drop_count",
				 data.cntrs.drop_count))
		goto fn_exit;

	if (!cYAML_create_number(stats, "send_length",
				 data.cntrs.send_length))
		goto fn_exit;

	if (!cYAML_create_number(stats, "recv_length",
				 data.cntrs.recv_length))
		goto fn_exit;

	if (!cYAML_create_number(stats, "route_length",
				 data.cntrs.route_length))
		goto fn_exit;

	if (!cYAML_create_number(stats, "drop_length",
				 data.cntrs.drop_length))
		goto fn_exit;

	if (!show_rc)
		cYAML_print_tree(root, 0);

	snprintf(err_str, LNET_MAX_STR_LEN, "Success");
fn_exit:
	if ((!show_rc) || (rc != LUSTRE_CFG_RC_NO_ERR))
		cYAML_free_tree(root);
	else if (show_rc) {
		if (*show_rc) {
			cYAML_insert_sibling((*show_rc)->child, root->child);
			free(root);
		} else
			*show_rc = root;
	}

	cYAML_build_error(rc, seq_no, "show", "statistics", err_str, err_rc);
	return rc;
}

typedef int (*cmd_handler_t)(cYAML *tree,
			     cYAML **show_rc,
			     cYAML **err_rc);

static int handle_yaml_config_route(cYAML *tree,
				    cYAML **show_rc,
				    cYAML **err_rc)
{
	cYAML *net, *gw, *hop, *prio, *seq_no;

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

static int handle_yaml_config_net(cYAML *tree,
				  cYAML **show_rc,
				  cYAML **err_rc)
{
	cYAML *net, *intf, *tunables, *seq_no,
	      *peer_to = NULL, *peer_buf_cr = NULL, *peer_cr = NULL,
	      *credits = NULL, *smp = NULL;

	net = cYAML_get_object_item(tree, "net");
	intf = cYAML_get_object_item(tree, "interface");

	tunables = cYAML_get_object_item(tree, "tunables");
	if (tunables) {
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

static int handle_yaml_config_buf(cYAML *tree,
				  cYAML **show_rc,
				  cYAML **err_rc)
{
	int rc;
	cYAML *tiny, *small, *large, *seq_no, *enable;

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

static int handle_yaml_del_route(cYAML *tree,
				 cYAML **show_rc,
				 cYAML **err_rc)
{
	cYAML *net;
	cYAML *gw;
	cYAML *seq_no;

	net = cYAML_get_object_item(tree, "net");
	gw = cYAML_get_object_item(tree, "gateway");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_del_route((net) ? net->valuestring : NULL,
				     (gw) ? gw->valuestring : NULL,
				     (seq_no) ? seq_no->valueint : -1,
				     err_rc);
}

static int handle_yaml_del_net(cYAML *tree,
			       cYAML **show_rc,
			       cYAML **err_rc)
{
	cYAML *net, *seq_no;

	net = cYAML_get_object_item(tree, "net");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_del_net((net) ? net->valuestring : NULL,
				   (seq_no) ? seq_no->valueint : -1,
				   err_rc);
}

static int handle_yaml_del_buf(cYAML *tree,
			       cYAML **show_rc,
			       cYAML **err_rc)
{
	cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_config_buf(0, 0, 0,
				      (seq_no) ? seq_no->valueint : -1,
				      err_rc);
}

static int handle_yaml_show_route(cYAML *tree,
				  cYAML **show_rc,
				  cYAML **err_rc)
{
	cYAML *net;
	cYAML *gw;
	cYAML *hop;
	cYAML *prio;
	cYAML *detail;
	cYAML *seq_no;

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

static int handle_yaml_show_net(cYAML *tree,
				cYAML **show_rc,
				cYAML **err_rc)
{
	cYAML *net, *detail, *seq_no;

	net = cYAML_get_object_item(tree, "net");
	detail = cYAML_get_object_item(tree, "detail");
	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_net((net) ? net->valuestring : NULL,
				    (detail) ? detail->valueint : 0,
				    (seq_no) ? seq_no->valueint : -1,
				    show_rc,
				    err_rc);
}

static int handle_yaml_show_buf(cYAML *tree,
				cYAML **show_rc,
				cYAML **err_rc)
{
	cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_buf((seq_no) ? seq_no->valueint : -1,
				    show_rc, err_rc);
}

static int handle_yaml_show_credits(cYAML *tree,
				    cYAML **show_rc,
				    cYAML **err_rc)
{
	cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_peer_credits((seq_no) ? seq_no->valueint : -1,
					     show_rc, err_rc);
}

static int handle_yaml_show_stats(cYAML *tree,
				  cYAML **show_rc,
				  cYAML **err_rc)
{
	cYAML *seq_no;

	seq_no = cYAML_get_object_item(tree, "seq_no");

	return lustre_lnet_show_stats((seq_no) ? seq_no->valueint : -1,
				      show_rc, err_rc);
}

typedef struct lookup_cmd_hdlr_tbl_s {
	char *name;
	cmd_handler_t cb;
} lookup_cmd_hdlr_tbl_t;

static lookup_cmd_hdlr_tbl_t lookup_config_tbl[] = {
	{"route", handle_yaml_config_route},
	{"net", handle_yaml_config_net},
	{"buffer", handle_yaml_config_buf},
	{NULL, NULL}
};

static lookup_cmd_hdlr_tbl_t lookup_del_tbl[] = {
	{"route", handle_yaml_del_route},
	{"net", handle_yaml_del_net},
	{"buffer", handle_yaml_del_buf},
	{NULL, NULL}
};

static lookup_cmd_hdlr_tbl_t lookup_show_tbl[] = {
	{"route", handle_yaml_show_route},
	{"net", handle_yaml_show_net},
	{"buffer", handle_yaml_show_buf},
	{"credits", handle_yaml_show_credits},
	{"statistics", handle_yaml_show_stats},
	{NULL, NULL}
};

static cmd_handler_t lookup_fn(char *key,
			       lookup_cmd_hdlr_tbl_t *tbl)
{
	int i;
	for (i = 0; tbl[i].name != NULL; i++) {
		if (strncmp(key, tbl[i].name, strlen(tbl[i].name)) == 0)
			return tbl[i].cb;
	}

	return NULL;
}

static int lustre_yaml_cb_helper(char *f, lookup_cmd_hdlr_tbl_t *table,
				 cYAML **show_rc, cYAML **err_rc)
{
	cYAML *tree, *cur;
	cmd_handler_t cb;
	char err_str[LNET_MAX_STR_LEN];
	int rc = 0;

	if (!g_lib_init) {
		lustre_config_api_init();
		g_lib_init = true;
	}

	tree = cYAML_build_tree(f, NULL, 0, err_rc);
	if (!tree)
		return LUSTRE_CFG_RC_BAD_PARAM;

	cur = tree->child;

	while (cur) {
		cb = lookup_fn(cur->string, table);
		if (cb)
			rc = cb(cur, show_rc, err_rc);

		/* if processing fails or no cb is found then fail */
		if ((rc) || (!cb)) {
			sprintf(err_str,
				"Failed to process request: %s [%d, %p]",
				cur->string, rc, cb);
			cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM, -1,
					 "yaml", "helper", err_str, err_rc);
		}

		cur = cur->next;
	}

	cYAML_free_tree(tree);

	return LUSTRE_CFG_RC_NO_ERR;
}

int lustre_yaml_config(char *f, cYAML **err_rc)
{
	if (!f) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, "config", "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_config_tbl,
				     NULL, err_rc);
}

int lustre_yaml_del(char *f, cYAML **err_rc)
{
	if (!f) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, "del", "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_del_tbl,
				     NULL, err_rc);
}

int lustre_yaml_show(char *f, cYAML **show_rc, cYAML **err_rc)
{
	if (!f) {
		cYAML_build_error(LUSTRE_CFG_RC_BAD_PARAM,
				 -1, "show", "yaml file",
				 "Missing file Parameter",
				 err_rc);

		return LUSTRE_CFG_RC_BAD_PARAM;
	}

	return lustre_yaml_cb_helper(f, lookup_show_tbl,
				     show_rc, err_rc);
}
