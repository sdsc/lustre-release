#include <stdio.h>
#include <stdlib.h>
#include <liblustreconfigapi.h>
#include <libcYAML.h>
#include "kvl.h"

#define NID_PARAM "--nid"

#define NET_PARAM "--net"
#define IF_PARAM "--if"
#define PEER_TO_PARAM  "--peer_timeout"
#define PEER_CR_PARAM "--peer_credits"
#define PEER_BUF_CR_PARAM "--peer_buffer_credits"
#define CREDITS_PARAM "--credits"
#define SMP_PARAM "--SMP"

#define GW_PARAM  "--gateway"
#define PRIO_PARAM "--priority"
#define HOP_PARAM  "--hop"

#define TINY_PARAM "--tiny"
#define SMALL_PARAM  "--small"
#define LARGE_PARAM "--large"

#define DETAIL_PARAM "detail"

#define FILE_PARAM "file"

#define TX_Q_NOOP "tx_queue_noop"
#define TX_Q_CR "tx_queue_cr"
#define TX_Q_NCR "tx_queue_ncr"
#define TX_Q_RSRVD "tx_queue_rsrvd"
#define TX_Q_ACTV "tx_queue_active"

/*
 * lnet_config_route
 *   calls into the Lustre API to configure routes
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_config_route(kvl_t *params)
{
	kvl_t *net;
	kvl_t *gw;
	kvl_t *prio;
	kvl_t *hop;
	unsigned int hops_v;
	unsigned int prio_v;
	cYAML *err_rc = NULL;
	int rc;

	net = kvl_find(params, NET_PARAM);
	gw = kvl_find(params, GW_PARAM);
	prio = kvl_find(params, PRIO_PARAM);
	hop = kvl_find(params, HOP_PARAM);

	if (kvl_convert_to_ui(hop, &hops_v))
		return -1;

	if (kvl_convert_to_ui(prio, &prio_v))
		return -1;

	rc = lustre_lnet_config_route((net) ? net->value : NULL,
				      (gw) ? gw->value : NULL,
				      (hop) ? hops_v : -1,
				      (prio) ? prio_v : -1,
				      -1,
				      &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_net
 *   calls into the Lustre API to configure a network
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_config_net(kvl_t *params)
{
	kvl_t *net, *intf, *peer_to, *peer_cr, *peer_buf_cr, *smp_param, *credits;
	unsigned int peer_to_v, peer_cr_v, peer_buf_cr_v, credits_v;
	cYAML *err_rc = NULL;
	int rc;

	net = kvl_find(params, NET_PARAM);
	intf = kvl_find(params, IF_PARAM);
	peer_to = kvl_find(params, PEER_TO_PARAM);
	peer_cr = kvl_find(params, PEER_CR_PARAM);
	peer_buf_cr = kvl_find(params, PEER_BUF_CR_PARAM);
	smp_param = kvl_find(params, SMP_PARAM);
	credits = kvl_find(params, CREDITS_PARAM);

	/* TODO: are the peer* suppose to be unsigned or signed? */
	if (kvl_convert_to_ui(peer_to, &peer_to_v))
		return -1;

	if (kvl_convert_to_ui(peer_cr, &peer_cr_v))
		return -1;

	if (kvl_convert_to_ui(peer_buf_cr, &peer_buf_cr_v))
		return -1;

	if (kvl_convert_to_ui(credits, &credits_v))
		return -1;

	rc = lustre_lnet_config_net((net) ? net->value : NULL,
				    (intf) ? intf->value : NULL,
				    (peer_to) ? peer_to_v : -1,
				    (peer_cr) ? peer_cr_v : -1,
				    (peer_buf_cr) ? peer_buf_cr_v : -1,
				    (credits) ? credits_v : -1,
				    (smp_param) ? smp_param->value : NULL,
				    -1,
				    &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_enable_routing
 *   calls into the Lustre API to enable routing
 */
int lnet_config_enable_routing(kvl_t *params)
{
	cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_enable_routing(1,
					-1,
					&err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_disable_routing
 *   calls into the Lustre API to enable routing
 */
int lnet_config_disable_routing(kvl_t *params)
{
	cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_enable_routing(0,
					-1,
					&err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_buf
 *   calls into the Lustre API to configure routing buffers
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_config_buf(kvl_t *params)
{
	kvl_t *tiny, *small, *large;
	unsigned int tiny_v, small_v, large_v;
	cYAML *err_rc = NULL;
	int rc;

	tiny = kvl_find(params, TINY_PARAM);
	small = kvl_find(params, SMALL_PARAM);
	large = kvl_find(params, LARGE_PARAM);

	/* TODO: are the peer* suppose to be unsigned or signed? */
	if (kvl_convert_to_ui(tiny, &tiny_v))
		return -1;

	if (kvl_convert_to_ui(small, &small_v))
		return -1;

	if (kvl_convert_to_ui(large, &large_v))
		return -1;

	rc = lustre_lnet_config_buf((tiny) ? tiny_v: -1,
				    (small) ? small_v : -1,
				    (large) ? large_v : -1,
				    -1,
				    &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_del_route
 *   calls into the Lustre API to delete routes
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_del_route(kvl_t *params)
{
	kvl_t *net;
	kvl_t *gw;
	cYAML *err_rc = NULL;
	int rc;

	net = kvl_find(params, NET_PARAM);
	gw = kvl_find(params, GW_PARAM);

	rc = lustre_lnet_del_route((net) ? net->value : NULL,
				   (gw) ? gw->value : NULL,
				   -1,
				   &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_del_net
 *   calls into the Lustre API to delete a network
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_del_net(kvl_t *params)
{
	kvl_t *net;
	cYAML *err_rc = NULL;
	int rc;

	net = kvl_find(params, NET_PARAM);

	rc = lustre_lnet_del_net((net) ? net->value : NULL,
				 -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_del_buf
 *   calls into the Lustre API to del buffers
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_del_buf(kvl_t *params)
{
	cYAML *err_rc = NULL;
	int rc;

	rc = lustre_lnet_config_buf(0, 0, 0, -1, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_show_route
 *   calls into the Lustre API to show routes
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_show_route(kvl_t *params)
{
	kvl_t *net;
	kvl_t *gw;
	kvl_t *prio;
	kvl_t *hop;
	kvl_t *detail;
	unsigned int hops_v;
	unsigned int prio_v;
	cYAML *show_rc = NULL, *err_rc = NULL;
	int rc;

	net = kvl_find(params, NET_PARAM);
	gw = kvl_find(params, GW_PARAM);
	prio = kvl_find(params, PRIO_PARAM);
	hop = kvl_find(params, HOP_PARAM);
	detail = kvl_find(params, DETAIL_PARAM);

	if (kvl_convert_to_ui(hop, &hops_v))
		return -1;

	if (kvl_convert_to_ui(prio, &prio_v))
		return -1;

	rc = lustre_lnet_show_route((net && net->value) ? net->value : NULL,
				    (gw && gw->value) ? gw->value : NULL,
				    (hop) ? hops_v : -1,
				    (prio) ? prio_v : -1,
				    (detail) ? 1 : 0,
				    -1,
				    &show_rc,
				    &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

/*
 * lnet_show_net
 *   calls into the Lustre API to show a network
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_show_net(kvl_t *params)
{
	kvl_t *net;
	kvl_t *detail;
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	net = kvl_find(params, NET_PARAM);
	detail = kvl_find(params, DETAIL_PARAM);

	rc = lustre_lnet_show_net((net && net->value) ? net->value : NULL,
				  (detail) ? 1 : 0,
				  -1,
				  &show_rc,
				  &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stderr, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

/*
 * lnet_show_buf
 *   calls into the Lustre API to show buffers
 *
 *   Ensures that mandatory parameters are present
 *   Ignore any extra paramters which might be on the stack.
 *   Call the API to configure a route
 */
int lnet_show_buf(kvl_t *params)
{
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	rc = lustre_lnet_show_buf(-1, &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stderr, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

/*
 * lnet_config_buf
 *   calls into the Lustre API to configure entities specified in the file
 */
int lnet_config_file(kvl_t *params)
{
	kvl_t *file;
	int rc;
	cYAML *err_rc = NULL;

	file = kvl_find(params, FILE_PARAM);

	rc = lustre_yaml_config((file) ? file->value : NULL,
				&err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_buf
 *   calls into the Lustre API to configure entities specified in the file
 */
int lnet_del_file(kvl_t *params)
{
	kvl_t *file;
	int rc;
	cYAML *err_rc = NULL;

	file = kvl_find(params, FILE_PARAM);

	rc = lustre_yaml_del((file) ? file->value : NULL,
			     &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);

	cYAML_free_tree(err_rc);

	return rc;
}

/*
 * lnet_config_buf
 *   calls into the Lustre API to configure entities specified in the file
 */
int lnet_show_file(kvl_t *params)
{
	kvl_t *file;
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	file = kvl_find(params, FILE_PARAM);

	rc = lustre_yaml_show((file) ? file->value : NULL,
			      &show_rc, &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

int lnet_show_tx_queue(kvl_t *params)
{
	kvl_t *net, *q;
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	net = kvl_find(params, NET_PARAM);
	if ((q = kvl_find(params, TX_Q_NOOP)))
		goto fn_cont;
	if ((q = kvl_find(params, TX_Q_CR)))
		goto fn_cont;
	if ((q = kvl_find(params, TX_Q_NCR)))
		goto fn_cont;
	if ((q = kvl_find(params, TX_Q_RSRVD)))
		goto fn_cont;
	if ((q = kvl_find(params, TX_Q_ACTV)))
		goto fn_cont;

fn_cont:
	rc = lustre_lnet_show_conn_queue((net) ? net->value : NULL,
					 (q) ? q->key : NULL,
					 -1,
					 &show_rc,
					 &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

int lnet_show_peer(kvl_t *params)
{
	kvl_t *net;
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	net = kvl_find(params, NET_PARAM);

	rc = lustre_lnet_show_peer((net) ? net->value : NULL,
				   -1,
				   &show_rc,
				   &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

int lnet_show_peer_credits(kvl_t *params)
{
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	rc = lustre_lnet_show_peer_credits(-1,
					   &show_rc,
					   &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}

int lnet_show_stats(kvl_t *params)
{
	int rc;
	cYAML *show_rc = NULL, *err_rc = NULL;

	rc = lustre_lnet_show_stats(-1,
				    &show_rc,
				    &err_rc);

	if (rc != LUSTRE_CFG_RC_NO_ERR)
		cYAML_print_tree(err_rc, 0);
	else if (show_rc)
		cYAML_print_tree(show_rc, 0);
	else if ((!show_rc) || (show_rc && (show_rc->child == NULL)))
		fprintf(stdout, "Nothing to show\n");

	cYAML_free_tree(err_rc);
	cYAML_free_tree(show_rc);

	return rc;
}
