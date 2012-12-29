/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2011 Intel Corporation
 *
 * Copyright 2012 Xyratex Technology Limited
 */
/*
 * lustre/ptlrpc/nrs_orr.c
 *
 * Network Request Scheduler (NRS) ORR and TRR policies
 *
 * Request ordering in a RR manner over backend-fs objects and OSTs respectively
 *
 * Author: Liang Zhen <liang@whamcloud.com>
 * Author: Nikitas Angelinas <nikitas_angelinas@xyratex.com>
 */
/**
 * \addtogoup nrs
 * @{
 */

#define DEBUG_SUBSYSTEM S_RPC
#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_support.h>
#include <obd_class.h>
#include <lustre_net.h>
#include <lustre/lustre_idl.h>
#include <lustre_req_layout.h>
#include <libcfs/libcfs.h>
#include "ptlrpc_internal.h"

/**
 * \name ORR/TRR policy
 *
 * ORR/TRR (Object Round Robin/Target Round Robin) NRS policies
 * @{
 */

/**
 * Binary heap predicate.
 *
 * Uses
 * ptlrpc_nrs_request::nr_u::orr::or_round and
 * ptlrpc_nrs_request::nr_u::orr::or_range to compare two binheap nodes and
 * produce a binary predicate that shows their relative priority, so that the
 * binary heap can perform the necessary sorting operations.
 *
 * \param[in] e1 The first binheap node to compare
 * \param[in] e2 The first binheap node to compare
 *
 * \retval 0 e1 > e2
 * \retval 1 e1 < e2
 */
static int
orr_req_compare(cfs_binheap_node_t *e1, cfs_binheap_node_t *e2)
{
	struct ptlrpc_nrs_request *nrq1;
	struct ptlrpc_nrs_request *nrq2;

	nrq1 = container_of(e1, struct ptlrpc_nrs_request, nr_node);
	nrq2 = container_of(e2, struct ptlrpc_nrs_request, nr_node);

	if (nrq1->nr_u.orr.or_round < nrq2->nr_u.orr.or_round)
		return 1;
	if (nrq1->nr_u.orr.or_round > nrq2->nr_u.orr.or_round)
		return 0;

	/**
	 * If round numbers are equal, requests should be sorted by
	 * ascending offset
	 */
	if (nrq1->nr_u.orr.or_range.or_start < nrq2->nr_u.orr.or_range.or_start)
		return 1;
	else if (nrq1->nr_u.orr.or_range.or_start >
		 nrq2->nr_u.orr.or_range.or_start)
		return 0;
	/**
	 * Requests start from the same offset
	 */
	else
		/**
		 * Do the longer one first; maybe slightly more chances of
		 * hitting the disk drive cache later with the lengthiest
		 * request.
		 */
		if (nrq1->nr_u.orr.or_range.or_end >
		    nrq2->nr_u.orr.or_range.or_end)
			return 0;
		else
			return 1;
}

/**
 * ORR binary heap operations
 */
static cfs_binheap_ops_t nrs_orr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= orr_req_compare,
};

#define NRS_ORR_DFLT_OID	0x0ULL

/**
 * Populate the ORR/TRR key fields for the RPC.
 */
static int
nrs_orr_key_fill(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq,
		 __u8 is_orr, struct nrs_orr_key *key)
{
	struct ptlrpc_request  *req;
	struct ost_body        *body;
	__u32			opc;
	int			rc = 0;


	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	LASSERT(req != NULL);

	opc = lustre_msg_get_opc(req->rq_reqmsg);

	req_capsule_init(&req->rq_pill, req, RCL_SERVER);

	if (opc == OST_READ) {
		req_capsule_set(&req->rq_pill, &RQF_OST_BRW_READ);
		nrq->nr_u.orr.or_write = false;
	} else if (opc == OST_WRITE) {
		req_capsule_set(&req->rq_pill, &RQF_OST_BRW_WRITE);
		nrq->nr_u.orr.or_write = true;
	} else {
		/* Only OST_[READ|WRITE] supported by ORR/TRR */
		LBUG();
	}

	if (is_orr) {
		/* request pill has been initialized in ptlrpc_hpreq_init() */
		body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			GOTO(out, rc = -EFAULT);

		/* XXX: This really needs a call to ost_validate_obdo(), to
		 * get a proper objid although it seems not to be essential at
		 * the moment, as long as we get something that is unique; maybe
		 * this will become more important with FID-on-OST later?
		 */
		key->ok_id = body->oa.o_id;
	} else
		/**
		 * For TRR we don't use the objid.
		 */
		key->ok_id = NRS_ORR_DFLT_OID;

#ifdef HAVE_SERVER_SUPPORT
	{
		/* lsd variable would go unused in #else if declared in function
		 * scope. */
		struct lr_server_data  *lsd;

		lsd = class_server_data(req->rq_export->exp_obd);
		/* XXX: Redundant check? */
		if (lsd == NULL)
			GOTO(out, rc = -EFAULT);

		key->ok_idx = lsd->lsd_ost_index;
	}
#else
	/* XXX: Can we do something better than this here? This is just to fix
	 * builds made with --disable-server. */
	key->ok_idx = 0;
#endif

out:
	return rc;
}

/* Checks if the RPC type handled by ORR or TRR */
static bool
nrs_orr_req_supp(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq)
{
	struct ptlrpc_request  *req;
	__u32			opc;
	enum nrs_orr_supp	supp;

	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	opc = lustre_msg_get_opc(req->rq_reqmsg);

	read_lock(&orrd->od_lock);
	supp = orrd->od_supp;
	read_unlock(&orrd->od_lock);

	switch (supp) {
	case NOS_OST_READ:
		if (opc == OST_READ)
			return true;
		break;
	case NOS_OST_WRITE:
		if (opc == OST_WRITE)
			return true;
		break;
	case NOS_OST_RW:
		if (opc == OST_READ || opc == OST_WRITE)
			return true;
		break;
	default:
		LBUG();
	}
	return false;
}

/* Populate the range values for the request with logical offsets */
static void
nrs_orr_logical(struct niobuf_remote *nb, int niocount,
		struct nrs_orr_req_range *range)
{
	/* Should we do this at page boundaries ? */
	range->or_start = nb[0].offset & CFS_PAGE_MASK;
	range->or_end = (nb[niocount - 1].offset +
			 nb[niocount - 1].len - 1) | ~CFS_PAGE_MASK;
}

static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys);

/* Set ORR range values in RPC */
static void
nrs_orr_set_range(struct ptlrpc_nrs_request *nrq,
		  struct nrs_orr_req_range *range)
{
	nrq->nr_u.orr.or_range.or_start = range->or_start;
	nrq->nr_u.orr.or_range.or_end = range->or_end;
}

static int
nrs_orr_get_range(struct ptlrpc_nrs_request *nrq, struct nrs_orr_data *orrd)
{
	struct ptlrpc_request	*req = container_of(nrq, struct ptlrpc_request,
						    rq_nrq);
	struct ptlrpc_service_part     *svcpt = req->rq_rqbd->rqbd_svcpt;
	struct obd_ioobj	       *ioo;
	struct niobuf_remote	       *nb;
	struct ost_body		       *body;
	struct nrs_orr_req_range	range;
	int				niocount;
	int				objcount;
	int				rc = 0;
	int				i;
	bool				phys;

	objcount = req_capsule_get_size(&req->rq_pill, &RMF_OBD_IOOBJ,
					RCL_CLIENT) / sizeof(*ioo);

	ioo = req_capsule_client_get(&req->rq_pill, &RMF_OBD_IOOBJ);
	if (ioo == NULL)
		GOTO(out, rc = -EFAULT);

	/* Should this be only ioo.ioo_bufcnt? */
	for (niocount = i = 0; i < objcount; i++)
		niocount += ioo[i].ioo_bufcnt;

	nb = req_capsule_client_get(&req->rq_pill, &RMF_NIOBUF_REMOTE);
	if (nb == NULL)
		GOTO(out, rc = -EFAULT);

	/* Use logical information from niobuf_remote structures */
	nrs_orr_logical(nb, niocount, &range);

	/**
	 * Obtain physical offsets if selected, and this is not an OST_WRITE
	 * RPC
	 */
	read_lock(&orrd->od_lock);
	phys = orrd->od_physical && !nrq->nr_u.orr.or_write;
	read_unlock(&orrd->od_lock);

	if (phys) {
		body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			GOTO(out, rc = -EFAULT);

		/**
		 * Release the lock here temporarily (we don't really need it
		 * here), as some operations that need to be carried out as
		 * part of performing the fiemap call may need to sleep.
		 */
		spin_unlock(&svcpt->scp_req_lock);

		/**
		 * Translate to physical block offsets from backend filesystem
		 * extents
		 */
		rc = nrs_orr_log2phys(req->rq_export, &body->oa, &range,
				      &range);

		spin_lock(&svcpt->scp_req_lock);

		if (rc)
			GOTO(out, rc);
	}
	/**
	 * Assign retrieved range values to request.
	 */
	nrs_orr_set_range(nrq, &range);
out:
	return rc;
}

#define NRS_ORR_HBITS_LOW	10
#define NRS_ORR_HBITS_HIGH	16
#define NRS_ORR_HBBITS		 8

/**
 * ORR hash operations
 */
static int nrs_orr_hop_keys_eq(struct nrs_orr_key *k1, struct nrs_orr_key *k2)
{
	return (k1->ok_idx == k2->ok_idx && k1->ok_id == k2->ok_id);
}

static unsigned
nrs_orr_hop_hash(cfs_hash_t *hs, const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, sizeof(struct nrs_orr_key), mask);
}

static void *
nrs_orr_hop_key(cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);

	return &orro->oo_key;
}

static int
nrs_orr_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);

	return nrs_orr_hop_keys_eq(&orro->oo_key, (struct nrs_orr_key *)key);
}

static void *
nrs_orr_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
}

static void
nrs_orr_hop_get(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);
	cfs_atomic_inc(&orro->oo_ref);
}

static void
nrs_orr_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_object *orro = cfs_hlist_entry(hnode,
						      struct nrs_orr_object,
						      oo_hnode);
	cfs_atomic_dec(&orro->oo_ref);
}

static void
nrs_orr_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orro = cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
	orrd = container_of(orro->oo_res.res_parent, struct nrs_orr_data,
			    od_res);

	LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
		 "Busy NRS orr policy for object with objid "LPX64" at OST "
		 "with OST index %u, with %d refs\n", orro->oo_key.ok_id,
		 orro->oo_key.ok_idx, cfs_atomic_read(&orro->oo_ref));

	OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
}

static cfs_hash_ops_t nrs_orr_hash_ops = {
	.hs_hash	= nrs_orr_hop_hash,
	.hs_key		= nrs_orr_hop_key,
	.hs_keycpy	= NULL,
	.hs_keycmp	= nrs_orr_hop_keycmp,
	.hs_object	= nrs_orr_hop_object,
	.hs_get		= nrs_orr_hop_get,
	.hs_put		= nrs_orr_hop_put,
	.hs_put_locked	= nrs_orr_hop_put,
	.hs_exit	= nrs_orr_hop_exit
};

#define NRS_ORR_QUANTUM_DFLT 256

static void
nrs_orr_genobjname(struct nrs_orr_data *orrd, struct ptlrpc_nrs_policy *policy)
{
	int		cptid = nrs_pol2cptid(policy);
	char		cptidstr[4];

	strcpy(orrd->od_objname, !!strcmp(policy->pol_name, "orr") ?
	       policy->pol_nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG ?
	       "nrs_orr_reg_cpt_" : "nrs_orr_hp_cpt_" :
	       policy->pol_nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG ?
	       "nrs_trr_reg_cpt_" : "nrs_trr_hp_cpt_");
	snprintf(cptidstr, sizeof(cptidstr), "%d", cptid);
	strcat(orrd->od_objname, cptidstr);
}

#ifdef LPROCFS
int nrs_orr_lprocfs_init(struct ptlrpc_nrs_policy *policy);
void nrs_orr_lprocfs_fini(struct ptlrpc_nrs_policy *policy);
int nrs_trr_lprocfs_init(struct ptlrpc_nrs_policy *policy);
void nrs_trr_lprocfs_fini(struct ptlrpc_nrs_policy *policy);
#else
int nrs_orr_lprocfs_init(struct ptlrpc_nrs_policy *policy) { return 0; }
void nrs_orr_lprocfs_fini(struct ptlrpc_nrs_policy *policy) { }
int nrs_trr_lprocfs_init(struct ptlrpc_nrs_policy *policy) { return 0; }
void nrs_trr_lprocfs_fini(struct ptlrpc_nrs_policy *policy) { }
#endif

/**
 * An ORR/TRR policy instance is started.
 *
 * \param[in] policy The policy
 *
 * \
 */
static int
nrs_orr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	       *orrd;
	int				rc = 0;
	ENTRY;

	OBD_CPT_ALLOC_PTR(orrd, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (orrd == NULL)
		RETURN(-ENOMEM);

	/* Binary heap for sorted incoming requests */
	orrd->od_binheap = cfs_binheap_create(&nrs_orr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL,
					      nrs_pol2cptab(policy),
					      nrs_pol2cptid(policy));
	if (orrd->od_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	nrs_orr_genobjname(orrd, policy);

	/* Slab cache for NRS ORR objects */
	orrd->od_cache = cfs_mem_cache_create(orrd->od_objname,
					      sizeof(struct nrs_orr_object),
					      0, 0);
	if (orrd->od_cache == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Hash for finding objects by struct nrs_orr_key */
	orrd->od_obj_hash = cfs_hash_create(orrd->od_objname,
					    NRS_ORR_HBITS_LOW,
					    NRS_ORR_HBITS_HIGH,
					    NRS_ORR_HBBITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_orr_hash_ops,
					    CFS_HASH_DEFAULT);

	if (orrd->od_obj_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	rwlock_init(&orrd->od_lock);
	orrd->od_quantum = NRS_ORR_QUANTUM_DFLT;
	orrd->od_supp = NOS_DFLT;
	orrd->od_physical = true;

	rc = nrs_orr_lprocfs_init(policy);
	if (rc != 0)
		GOTO(failed, rc);

	policy->pol_private = orrd;

	RETURN(rc);

failed:
	if (orrd->od_obj_hash) {
		cfs_hash_putref(orrd->od_obj_hash);
		orrd->od_obj_hash = NULL;
	}
	if (orrd->od_cache) {
		rc = cfs_mem_cache_destroy(orrd->od_cache);
		LASSERTF(rc == 0, "Could not destroy od_cache slab\n");
	}
	if (orrd->od_binheap != NULL)
		cfs_binheap_destroy(orrd->od_binheap);

	OBD_FREE_PTR(orrd);

	RETURN(rc);
}


/**
 * An ORR policy instance is stopped.
 *
 * Called when the policy has been instructed to transition to the
 * ptlrpc_nrs_pol_state::NRS_POL_STATE_STOPPED state and has no more
 * pending requests to serve.
 *
 * \param[in] policy The policy
 *
 */
static void
nrs_orr_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data *orrd = policy->pol_private;
	ENTRY;

	LASSERT(orrd != NULL);
	LASSERT(cfs_binheap_is_empty(orrd->od_binheap));

	cfs_binheap_destroy(orrd->od_binheap);
	cfs_hash_putref(orrd->od_obj_hash);
	cfs_mem_cache_destroy(orrd->od_cache);

	nrs_orr_lprocfs_fini(policy);

	OBD_FREE_PTR(orrd);
}

/**
 * Performs a policy-specific ctl function on ORR/TRR policy instances; similar
 * to ioctl.
 *
 * \param[in]	  policy The policy instance
 * \param[in]	  opc	 The opcode
 * \param[in,out] arg	 Used for passing parameters and information
 *
 * \pre spin_is_locked(&policy->pol_nrs->->nrs_lock)
 * \post spin_is_locked(&policy->pol_nrs->->nrs_lock)
 *
 * \retval 0   Operation carried successfully
 * \retval -ve Error
 */
int
nrs_orr_ctl(struct ptlrpc_nrs_policy *policy, enum ptlrpc_nrs_ctl opc,
	    void *arg)
{
	int rc = 0;

	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	switch(opc) {
	default:
		rc = -EINVAL;
		break;

	case NRS_CTL_ORR_RD_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;

		read_lock(&orrd->od_lock);
		info->oi_quantum = orrd->od_quantum;
		read_unlock(&orrd->od_lock);
		}
		break;

	case NRS_CTL_ORR_WR_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		write_lock(&orrd->od_lock);
		orrd->od_quantum = *(__u16 *)arg;
		write_unlock(&orrd->od_lock);
		}
		break;

	case NRS_CTL_ORR_RD_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;

		read_lock(&orrd->od_lock);
		info->oi_physical = orrd->od_physical;
		read_unlock(&orrd->od_lock);
		}
		break;

	case NRS_CTL_ORR_WR_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		char			*off_type = arg;

		write_lock(&orrd->od_lock);
		if (!strcmp(off_type, "physical"))
			orrd->od_physical = true;
		else if (!strcmp(off_type, "logical"))
			orrd->od_physical = false;
		else
			rc = -EINVAL;
		write_unlock(&orrd->od_lock);
		}
		break;

	case NRS_CTL_ORR_RD_SUPP_REQ: {
		struct nrs_orr_info	*info = (struct nrs_orr_info *)arg;
		struct nrs_orr_data	*orrd = policy->pol_private;

		read_lock(&orrd->od_lock);
		info->oi_supp_req = orrd->od_supp;
		read_unlock(&orrd->od_lock);
		}
		break;

	case NRS_CTL_ORR_WR_SUPP_REQ: {
		struct nrs_orr_data	*orrd = policy->pol_private;
		char			*supp = arg;

		write_lock(&orrd->od_lock);
		if (!strcmp(supp, "read"))
			orrd->od_supp = NOS_OST_READ;
		else if (!strcmp(supp, "write"))
			orrd->od_supp = NOS_OST_WRITE;
		else if (!strcmp(supp, "readwrite"))
			orrd->od_supp = NOS_OST_RW;
		else
			rc = -EINVAL;
		write_unlock(&orrd->od_lock);
		}
		break;
	}
	RETURN(rc);
}

/**
 * Obtains resources from ORR and TRR policy instances. The top-level resource
 * lives inside \e nrs_orr_data and the second-level resource inside
 * \e nrs_orr_object .
 */
enum nrs_resource_level
nrs_orr_res_get(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq,
		struct ptlrpc_nrs_resource *parent,
		struct ptlrpc_nrs_resource **resp, bool ltd)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	struct nrs_orr_object	*tmp;
	struct nrs_orr_key	key;
	int			rc = 0;
	ENTRY;

	/* struct nrs_orr_data is requested */
	if (parent == NULL) {
		*resp = &((struct nrs_orr_data *)policy->pol_private)->od_res;
		return NRS_RES_LVL_PARENT;
	}

	orrd = container_of(parent, struct nrs_orr_data, od_res);

	/* If the request type is not supported, fail the enqueuing; the RPC
	 * will be handled by the default NRS policy. */
	if (!nrs_orr_req_supp(orrd, nrq))
		RETURN(-1);

	/**
	 * Don't re-do the follwing if we are placing or moving the request to
	 * the HP NRS head.
	 */
	if (!ltd) {
		/* struct nrs_orr_object is requested */
		rc = nrs_orr_key_fill(orrd, nrq,
				      !!strcmp(policy->pol_name, "orr"), &key);
		if (rc != 0)
			RETURN(rc);
	}

	orro = cfs_hash_lookup(orrd->od_obj_hash, &key);
	if (orro != NULL)
		GOTO(out, NRS_RES_LVL_CHILD1);

	OBD_SLAB_CPT_ALLOC_PTR_GFP(orro, orrd->od_cache,
				   nrs_pol2cptab(policy), nrs_pol2cptid(policy),
				   !ltd ? CFS_ALLOC_IO : CFS_ALLOC_ATOMIC);
	if (orro == NULL)
		RETURN(-ENOMEM);

	orro->oo_key = key;
	/* TODO: This needs to be locked, really */
	orro->oo_quantum = orrd->od_quantum;

	cfs_atomic_set(&orro->oo_ref, 1);
	tmp = cfs_hash_findadd_unique(orrd->od_obj_hash, &orro->oo_key,
				      &orro->oo_hnode);
	if (tmp != orro) {
		OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
		orro = tmp;
	}
out:
	/* For debugging purposes */
	nrq->nr_u.orr.or_key = orro->oo_key;

	*resp = &orro->oo_res;

	return NRS_RES_LVL_CHILD1;
}

static void
nrs_orr_res_put(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_resource *res)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	ENTRY;

	if (res->res_parent == NULL)
		return;

	orro = container_of(res, struct nrs_orr_object, oo_res);
	LASSERT(res->res_parent != NULL);
	orrd = container_of(res->res_parent, struct nrs_orr_data, od_res);

	cfs_hash_put(orrd->od_obj_hash, &orro->oo_hnode);
}

/* Picks up a request from the root of the binheap */
static struct ptlrpc_nrs_request *
nrs_orr_req_poll(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	*orrd = policy->pol_private;
	cfs_binheap_node_t	*node = cfs_binheap_root(orrd->od_binheap);

	return node == NULL ? NULL :
		container_of(node, struct ptlrpc_nrs_request, nr_node);
}

/* Sort-adds a request to the binary heap */
static int
nrs_orr_req_add(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	int			 rc;

	orro = container_of(nrs_request_resource(nrq),
			    struct nrs_orr_object, oo_res);
	orrd = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_orr_data, od_res);

	LASSERT(orro->oo_res.res_parent != NULL);
	LASSERT(orro->oo_res.res_parent ==
		nrs_request_resource(nrq)->res_parent);

	rc = nrs_orr_get_range(nrq, orrd);
	if (rc)
		RETURN(rc);

	if (orro->oo_started == false || orro->oo_active == 0) {
		orro->oo_round = orrd->od_round;
		orrd->od_round++;
		orro->oo_started = true;
	}

	nrq->nr_u.orr.or_round = orro->oo_round;
	rc = cfs_binheap_insert(orrd->od_binheap, &nrq->nr_node);
	if (rc == 0) {
		orro->oo_quantum--;
		orro->oo_active++;
		if (orro->oo_quantum == 0) {
			/* TODO: This needs to be locked, really */
			orro->oo_quantum = orrd->od_quantum;
			orro->oo_started = false;
		}
	}
	RETURN(rc);
}

static void
nrs_orr_req_del(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orrd = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_orr_data, od_res);
	orro = container_of(nrs_request_resource(nrq),
			    struct nrs_orr_object, oo_res);

	LASSERT(nrq->nr_u.orr.or_round < orrd->od_round);
	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	cfs_binheap_remove(orrd->od_binheap, &nrq->nr_node);
	orro->oo_active--;
}

static void
nrs_orr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS start %s request for object with ID "LPX64""
	       " from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static void
nrs_orr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS stop %s request for object with ID "LPX64" "
	       "from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static struct ptlrpc_nrs_pol_ops nrs_orr_ops = {
	.op_policy_start	= nrs_orr_start,
	.op_policy_stop		= nrs_orr_stop,
	.op_policy_ctl		= nrs_orr_ctl,
	.op_res_get		= nrs_orr_res_get,
	.op_res_put		= nrs_orr_res_put,
	.op_req_poll		= nrs_orr_req_poll,
	.op_req_enqueue		= nrs_orr_req_add,
	.op_req_dequeue		= nrs_orr_req_del,
	.op_req_start		= nrs_orr_req_start,
	.op_req_stop		= nrs_orr_req_stop,
};

struct ptlrpc_nrs_pol_desc ptlrpc_nrs_orr_desc = {
	.pd_name		= "orr",
	.pd_ops			= &nrs_orr_ops,
	.pd_compat		= nrs_policy_compat_one,
	.pd_compat_svc_name	= "ost_io",
};

#ifdef __KERNEL__

#define ORR_NUM_EXTENTS 1

/* This structure mirrors struct ll_user_fiemap, but uses a fixed length for the
 * array of ll_fiemap_extent structs; its prime intention is for statically
 * allocated variables.
 */
struct nrs_orr_fiemap {
	__u64 of_start;
	__u64 of_length;
	__u32 of_flags;
	__u32 of_mapped_extents;
	__u32 of_extent_count;
	__u32 of_reserved;
	struct ll_fiemap_extent of_extents[ORR_NUM_EXTENTS];
};

/* Get extents from the range of niobuf_remotes here by doing fiemap calls via
 * obd_get_info(), and then return a lower and higher physical block number for
 * TODO: This is a layering violation; maybe this should be placed in the OST
 * layer?
 * range is in and out parameter
 */
static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys)
{
	/* Use ll_fiemap_info_key, as done in ost_get_info() */
	struct ll_fiemap_info_key  fm_key = { .name = KEY_FIEMAP };
	struct nrs_orr_fiemap	   of_fiemap;
	struct ll_user_fiemap	  *fiemap = (struct ll_user_fiemap *)&of_fiemap;
	int			   rc;
	int			   replylen;
	loff_t			   start = OBD_OBJECT_EOF;
	loff_t			   end = 0;
	__u64			   log_req_len = log->or_end - log->or_start;
	ENTRY;

	fm_key.oa = *oa;
	fm_key.fiemap.fm_start = log->or_start;
	fm_key.fiemap.fm_length = log_req_len;
	fm_key.fiemap.fm_extent_count = ORR_NUM_EXTENTS;

	/* Here we perform a fiemap call in order to get extent descriptions;
	 * ideally, we could pass '0' in to fm_extent_count in order to obtain
	 * the number of extents required in order to map the whole file and
	 * allocate as much memory as we require, but here we adopt a faster
	 * route by performing only one fiemap call to get the extent
	 * information and assume the nrs_orr_fiemap struct that is allocated
	 * on the stack will suffice; i fit does not, we can either fail the
	 * operation, or resort to allocating memory on demand or from a pool.
	 */
	/* Do an obd_get_info to get the extent descriptions */
	rc = obd_get_info(NULL, exp, sizeof(fm_key), &fm_key, &replylen,
			  (struct ll_user_fiemap *)&of_fiemap, NULL);
	if (unlikely(rc)) {
		CERROR("obd_get_info failed: rc = %d\n", rc);
		goto out;
	}

	/* XXX: == 0 seems to happen regularly, not sure why */
	if (fiemap->fm_mapped_extents == 0 ||
	    fiemap->fm_mapped_extents > ORR_NUM_EXTENTS)
		GOTO(out, rc = -EFAULT);

	/* Optimize start and end calculation for the one fiemap case; this will
	 * have to be changed if we include more than one fiemap structs in the
	 * future.
	 */
	start = fiemap->fm_extents[0].fe_physical;
	start += log->or_start - fiemap->fm_extents[0].fe_logical;
	end = start + log_req_len;

	phys->or_start = start;
	phys->or_end = end;
out:
	return rc;
}
#else
/* TODO: Physical offset stub for liblustre */
static int
nrs_orr_log2phys(struct obd_export *exp, struct obdo *oa,
		 struct nrs_orr_req_range *log, struct nrs_orr_req_range *phys)
{
	return 0;
}
#endif

/*
 * TRR, Target-based Round Robin policy
 *
 * TRR reuses much of the functions and data structures of ORR
 */

/* TRR binary heap operations */
static cfs_binheap_ops_t nrs_trr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= orr_req_compare,
};

static void
nrs_trr_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orro = cfs_hlist_entry(hnode, struct nrs_orr_object, oo_hnode);
	orrd = container_of(orro->oo_res.res_parent, struct nrs_orr_data,
			    od_res);

	LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
		 "Busy NRS TRR policy at OST with index %u, with %d refs\n",
		 orro->oo_key.ok_idx, cfs_atomic_read(&orro->oo_ref));

	OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
}

static cfs_hash_ops_t nrs_trr_hash_ops = {
	.hs_hash	= nrs_orr_hop_hash,
	.hs_key		= nrs_orr_hop_key,
	.hs_keycpy	= NULL,
	.hs_keycmp	= nrs_orr_hop_keycmp,
	.hs_object	= nrs_orr_hop_object,
	.hs_get		= nrs_orr_hop_get,
	.hs_put		= nrs_orr_hop_put,
	.hs_put_locked	= nrs_orr_hop_put,
	.hs_exit	= nrs_trr_hop_exit
};

#define NRS_TRR_HBITS_LOW	3
#define NRS_TRR_HBITS_HIGH	8
#define NRS_TRR_HBBITS		2


static int
nrs_trr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	       *orrd;
	int				rc = 0;
	ENTRY;

	OBD_CPT_ALLOC_PTR(orrd, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (orrd == NULL)
		RETURN(-ENOMEM);

	/* Binary heap for sorted incoming requests */
	orrd->od_binheap = cfs_binheap_create(&nrs_trr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL,
					      nrs_pol2cptab(policy),
					      nrs_pol2cptid(policy));
	if (orrd->od_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	nrs_orr_genobjname(orrd, policy);

	/* Slab cache for TRR targets */
	orrd->od_cache = cfs_mem_cache_create(orrd->od_objname,
					      sizeof(struct nrs_orr_object),
					      0, 0);
	if (orrd->od_cache == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* Hash for finding objects by nrs_trr_key_t */
	orrd->od_obj_hash = cfs_hash_create(orrd->od_objname,
					    NRS_TRR_HBITS_LOW,
					    NRS_TRR_HBITS_HIGH,
					    NRS_TRR_HBBITS, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_trr_hash_ops,
					    CFS_HASH_DEFAULT);

	if (orrd->od_obj_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	rwlock_init(&orrd->od_lock);
	orrd->od_quantum = NRS_ORR_QUANTUM_DFLT;
	orrd->od_supp = NOS_DFLT;
	orrd->od_physical = true;

	rc = nrs_trr_lprocfs_init(policy);
	if (rc != 0)
		GOTO(failed, rc);

	policy->pol_private = orrd;

	RETURN(rc);

failed:
	if (orrd->od_obj_hash) {
		cfs_hash_putref(orrd->od_obj_hash);
		orrd->od_obj_hash = NULL;
	}
	if (orrd->od_cache) {
		rc = cfs_mem_cache_destroy(orrd->od_cache);
		LASSERTF(rc == 0, "Could not destroy od_cache slab\n");
	}
	if (orrd->od_binheap != NULL)
		cfs_binheap_destroy(orrd->od_binheap);

	OBD_FREE_PTR(orrd);

	RETURN(rc);
}

static void
nrs_trr_stop(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data *orrd = policy->pol_private;
	ENTRY;

	LASSERT(orrd != NULL);
	LASSERT(cfs_binheap_is_empty(orrd->od_binheap));

	cfs_binheap_destroy(orrd->od_binheap);
	cfs_hash_putref(orrd->od_obj_hash);
	cfs_mem_cache_destroy(orrd->od_cache);

	nrs_trr_lprocfs_fini(policy);

	OBD_FREE_PTR(orrd);
}

static void
nrs_trr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS start %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

static void
nrs_trr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_object *orro = container_of(nrs_request_resource(nrq),
						   struct nrs_orr_object,
						   oo_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS stop %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

/* Reuse much of the ORR functionality for TRR */
static struct ptlrpc_nrs_pol_ops nrs_trr_ops = {
	.op_policy_start	= nrs_trr_start,
	.op_policy_stop		= nrs_trr_stop,
	.op_policy_ctl		= nrs_orr_ctl,
	.op_res_get		= nrs_orr_res_get,
	.op_res_put		= nrs_orr_res_put,
	.op_req_poll		= nrs_orr_req_poll,
	.op_req_enqueue		= nrs_orr_req_add,
	.op_req_dequeue		= nrs_orr_req_del,
	.op_req_start		= nrs_trr_req_start,
	.op_req_stop		= nrs_trr_req_stop
};

struct ptlrpc_nrs_pol_desc ptlrpc_nrs_trr_desc = {
	.pd_name		= "trr",
	.pd_ops			= &nrs_trr_ops,
	.pd_compat		= nrs_policy_compat_one,
	.pd_compat_svc_name	= "ost_io",
};
/** @} ORR/TRR policy */

/** @} nrs */
