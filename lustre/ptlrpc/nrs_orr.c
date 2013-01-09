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
 * Request scheduling in a Round-Robin manner over backend-fs objects and OSTs
 * respectively
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
 *
 * The TRR policy reuses much of the functionality of ORR. TRR only differs
 * from ORR in that it ignores the target object ID when grouping request
 * batches. In fact, these two scheduling algorithms could be implemented under
 * a single NRS policy, that uses an lprocfs tunable in order to switch between
 * the two types of scheduling behaviour. The two algorithms have been
 * implemented as separate policies for reasons of clarity to the user, and to
 * avoid issues that would otherwise arise at the point of switching between
 * behaviours in the case of having a single policy, such as resource cleanup
 * for nrs_orr_object instances. It is possible that this may need to be
 * re-examined in the future, along with potentially coalescing other policies
 * that perform batch request scheduling in a Round-Robin manner, all into one
 * policy.
 *
 * @{
 */

/**
 * Checks if the RPC type of \a nrq is handled by the ORR/TRR policies.
 *
 * \param[in]  orrd   The ORR/TRR policy scheduler instance
 * \param[in]  nrq    The request
 * \param[out] opcode The opcode is saved here, just in order to avoid calling
 *		      lustre_msg_get_opc() again later
 *
 * \retval true  Request type is supported by the policy instance
 * \retval false Reuqest type is not supported by the policy instance
 */
static bool
nrs_orr_req_supported(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq,
		      __u32 *opcode)
{
	struct ptlrpc_request  *req;
	__u32			opc;
	bool			rc = false;

	req = container_of(nrq, struct ptlrpc_request, rq_nrq);

	opc = lustre_msg_get_opc(req->rq_reqmsg);

	/**
	 * XXX: nrs_orr_data::od_supp accessed unlocked.
	 */
	switch (orrd->od_supp) {
	default:
		LBUG();
	case NOS_OST_READ:
		if (opc == OST_READ)
			rc = true;
		break;
	case NOS_OST_WRITE:
		if (opc == OST_WRITE)
			rc = true;
		break;
	case NOS_OST_RW:
		if (opc == OST_READ || opc == OST_WRITE)
			rc = true;
		break;
	}

	if (rc)
		*opcode = opc;

	return rc;
}

/**
 * Returns the ORR/TRR key fields for the request \a nrq in \a key.
 *
 * \param[in]  orrd The ORR/TRR policy scheduler instance
 * \param[in]  nrq  The request
 * \param[in]  opc  The request's opcode
 * \param[in]  name The policy name
 * \param[out] key  Fields of the key are returned here.
 *
 * \retval 0	Key filled successfully
 * \retval != 0 Error
 */
static int
nrs_orr_key_fill(struct nrs_orr_data *orrd, struct ptlrpc_nrs_request *nrq,
		 __u32 opc, char *name, struct nrs_orr_key *key)
{
	struct ptlrpc_request  *req;
	struct ost_body        *body;
	bool			is_orr = strcmp(name, NRS_POL_NAME_ORR) == 0;

	/**
	 * This is an attempt to fill in the request key fields while
	 * moving a request from the regular to the high-priority NRS
	 * head (via ldlm_lock_reorder_req()), but the request key has
	 * been adequately filled when nrs_orr_res_get() was called on
	 * the ORR/TRR policy for the regular NRS head, so there is
	 * nothing to do.
	 */
	if ((is_orr && nrq->nr_u.orr.or_orr_set) ||
	    (!is_orr && nrq->nr_u.orr.or_trr_set)) {
		return 0;
	}

	req = container_of(nrq, struct ptlrpc_request, rq_nrq);
	LASSERT(req != NULL);

	/**
	 * Set the key's object ID. For TRR policy instances we ignore the
	 * object ID, and just schedule over the OST indexes.
	 */
	if (is_orr) {
		/**
		 * The request pill for OST_READ and OST_WRITE requests is
		 * initialized in the ost_io service's
		 * ptlrpc_service_ops::so_hpreq_handler, ost_io_hpreq_handler(),
		 * so no need to redo it here.
		 */
		body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			RETURN(-EFAULT);

		/**
		 * XXX: Is a call to ost_validate_obdo() required here?
		 */
		key->ok_id = body->oa.o_id;

		nrq->nr_u.orr.or_orr_set = 1;
	}

	/**
	 * Set the key's OST index.
	 */
#ifdef HAVE_SERVER_SUPPORT
	{
		/* lsd variable would go unused if HAVE_SERVER_SUPPORT was
		 * not defined, if declared in function scope. */
		struct lr_server_data  *lsd;

		lsd = class_server_data(req->rq_export->exp_obd);

		key->ok_idx = lsd->lsd_osd_index;

		if (!is_orr)
			nrq->nr_u.orr.or_trr_set = 1;
	}
#endif

	return 0;
}

/**
 * Populates the range values in \a range with logical offsets obtained via
 * \a nb.
 *
 * \param[in]  nb	niobuf_remote struct array for this request
 * \param[in]  niocount	count of niobuf_remote structs for this request
 * \param[out] range	The offset range is returned here
 */
static void
nrs_orr_range_fill_logical(struct niobuf_remote *nb, int niocount,
			   struct nrs_orr_req_range *range)
{
	/* Should we do this at page boundaries ? */
	range->or_start = nb[0].offset & CFS_PAGE_MASK;
	range->or_end = (nb[niocount - 1].offset +
			 nb[niocount - 1].len - 1) | ~CFS_PAGE_MASK;
}

#ifdef __KERNEL__

#define ORR_NUM_EXTENTS 1

/**
 * This structure mirrors struct ll_user_fiemap, but uses a fixed length for the
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

/**
 * Converts the logical file offset range in \a range, to a physical disk offset
 * range in \a range, for a request. Uses obd_get_info() in order to carry out a
 * fiemap call and obtain backend-fs extent information. The returned range is
 * in physical block numbers.
 * XXX: This is a layering violation; maybe this should be placed in the OST
 * layer?
 *
 * \param[in] nrq	The request
 * \param[in] oa	obdo struct for this request
 * \param[in,out] range	The offset range
 *
 * \retval 0	Physical offsets obtained successfully
 * \retvall !=0 Error
 */
static int
nrs_orr_range_fill_physical(struct ptlrpc_nrs_request *nrq, struct obdo *oa,
			    struct nrs_orr_req_range *range)
{
	struct ptlrpc_request     *req = container_of(nrq,
						      struct ptlrpc_request,
						      rq_nrq);
	/**
	 * Use ll_fiemap_info_key, as done in ost_get_info()
	 */
	struct ll_fiemap_info_key  fm_key = { .name = KEY_FIEMAP };
	struct nrs_orr_fiemap	   of_fiemap;
	struct ll_user_fiemap	  *fiemap = (struct ll_user_fiemap *)&of_fiemap;
	int			   replylen;
	__u64			   log_req_len = range->or_end -
						 range->or_start;
	loff_t			   start = OBD_OBJECT_EOF;
	loff_t			   end = 0;
	int			   rc;

	fm_key.oa = *oa;
	fm_key.fiemap.fm_start = range->or_start;
	fm_key.fiemap.fm_length = log_req_len;

	/**
	 * We are making the assumption here, that the offset range that a
	 * brw RPC relates to fits into a single backend-fs extent, since we do
	 * not know the real number of extents that we need to obtained
	 * information for; i.e. we are catering for the most common case;
	 * perhaps this could be improved.
	 */
	fm_key.fiemap.fm_extent_count = ORR_NUM_EXTENTS;

	/**
	 * Here we perform a fiemap call in order to get extent descriptions;
	 * ideally in theory, we could pass '0' in to fm_extent_count in order
	 * to obtain the number of extents required in order to map the whole
	 * file and allocate as much memory as we require, but this would be
	 * too wasteful and slow. We adopt a faster route by performing only
	 * one fiemap call to get the extent information and assume
	 * the nrs_orr_fiemap struct that is allocated on the stack will
	 * suffice.
	 */
	rc = obd_get_info(NULL, req->rq_export, sizeof(fm_key), &fm_key,
			  &replylen, (struct ll_user_fiemap *)&of_fiemap, NULL);
	if (rc)
		GOTO(out, rc);

	/**
	 * XXX: == 0 seemed to happen regularly on some test runs, not sure why.
	 */
	if (fiemap->fm_mapped_extents == 0 ||
	    fiemap->fm_mapped_extents > ORR_NUM_EXTENTS)
		GOTO(out, rc = -EFAULT);

	/**
	 * Translate extent start and end block numbers into offset block
	 * numbers for the request.
	 */
	start = fiemap->fm_extents[0].fe_physical;
	start += range->or_start - fiemap->fm_extents[0].fe_logical;
	end = start + log_req_len;

	range->or_start = start;
	range->or_end = end;

	nrq->nr_u.orr.or_physical_set = 1;
out:
	return rc;
}
#else
/**
 * For liblustre.
 */
static int
nrs_orr_range_fill_physical(struct ptlrpc_nrs_request *nrq, struct obdo *oa,
			    struct nrs_orr_req_range *range)
{
	return 0;
}
#endif

/**
 * Sets the offset range the request covers; either in logical file
 * offsets or in physical disk offsets.
 *
 * \param[in] nrq	 The request
 * \param[in] orrd	 The ORR/TRR policy scheduler instance
 * \param[in] opc	 The request's opcode
 * \param[in] moving_req Is the request in the process of moving onto the
 *			 high-priority NRS head?
 *
 * \retval 0	Range filled successfully
 * \retval != 0 Error
 *
 * XXX: Some combinations of nrs_orr_data::od_supported and
 * nrs_orr_data::offset_type may be pathological. Imagine
 * od_supported = readwrite + offset_type = physical offsets: reads will be
 * scheduled on physical offsets, but writes will be scheduled on logical
 * offsets.
 */
static int
nrs_orr_range_fill(struct ptlrpc_nrs_request *nrq, struct nrs_orr_data *orrd,
		   __u32 opc, bool moving_req)
{
	struct ptlrpc_request	    *req = container_of(nrq,
							struct ptlrpc_request,
							rq_nrq);
	struct obd_ioobj	    *ioo;
	struct niobuf_remote	    *nb;
	struct ost_body		    *body;
	struct nrs_orr_req_range     range;
	int			     niocount;
	int			     rc = 0;

	/**
	 * If we are scheduling using physical disk offsets, but we have filled
	 * the offset information in the request previously
	 * (i.e. ldlm_lock_reorder_req() is moving the request to the
	 * high-priority NRS head), there is no need to do anything, and we can
	 * exit. Moreover than the lack of need, we would be unable to perform
	 * the obd_get_info() call required in nrs_orr_range_fill_physical(),
	 * because ldlm_lock_reorder_lock() calls into here while holding a
	 * spinlock, and retrieving fiemap information via obd_get_info() is a
	 * potentially sleeping operation.
	 */
	if (orrd->od_physical && nrq->nr_u.orr.or_physical_set)
		return 0;

	ioo = req_capsule_client_get(&req->rq_pill, &RMF_OBD_IOOBJ);
	if (ioo == NULL)
		GOTO(out, rc = -EFAULT);

	niocount = ioo->ioo_bufcnt;

	nb = req_capsule_client_get(&req->rq_pill, &RMF_NIOBUF_REMOTE);
	if (nb == NULL)
		GOTO(out, rc = -EFAULT);

	/**
	 * Use logical information from niobuf_remote structures.
	 */
	nrs_orr_range_fill_logical(nb, niocount, &range);

	/**
	 * Obtain physical offsets if selected, and this is an OST_READ RPC
	 * RPC. We do not enter this block if moving_req is set which indicates
	 * that the request is being moved to the high-priority NRS head by
	 * ldlm_lock_reorder_req(), as that function calls in here while holding
	 * a spinlock, and nrs_orr_range_physical() can sleep, so we just use
	 * logical file offsets for the range values for such requests.
	 */
	if (orrd->od_physical && opc == OST_READ && !moving_req) {
		body = req_capsule_client_get(&req->rq_pill, &RMF_OST_BODY);
		if (body == NULL)
			GOTO(out, rc = -EFAULT);

		/**
		 * Translate to physical block offsets from backend filesystem
		 * extents.
		 */
		rc = nrs_orr_range_fill_physical(nrq, &body->oa, &range);
		if (rc)
			GOTO(out, rc);
	}

	/**
	 * Assign retrieved range values to the request.
	 */
	nrq->nr_u.orr.or_range = range;
out:
	return rc;
}

/**
 * Generates a character string that can be used in order to register uniquely
 * named libcfs_hash and slab objects for ORR/TRR policy instances. The
 * character string is unique per policy instance, as it includes the policy's
 * name and the CPT number, and tehre is one policy instance per CPT for the
 * ost_io service.
 *
 * \param[in] policy   The policy instance
 * \param[in] is_orr   Whether the policy is an ORR or TRR instance
 * \param[out] objname The character array that will hold the generated name
 */
static void
nrs_orr_genobjname(struct ptlrpc_nrs_policy *policy, bool is_orr, char *objname)
{
	int		cptid = nrs_pol2cptid(policy);
	char		cptidstr[4];

	strcpy(objname, is_orr ?
	       policy->pol_nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG ?
	       "nrs_orr_reg_cpt_" : "nrs_orr_hp_cpt_" :
	       policy->pol_nrs->nrs_queue_type == PTLRPC_NRS_QUEUE_REG ?
	       "nrs_trr_reg_cpt_" : "nrs_trr_hp_cpt_");
	snprintf(cptidstr, sizeof(cptidstr), "%d", cptid);
	strcat(objname, cptidstr);
}

#define NRS_ORR_HBITS_LOW	10
#define NRS_ORR_HBITS_HIGH	16
#define NRS_ORR_HBBITS		 8

#define NRS_TRR_HBITS_LOW	 3
#define NRS_TRR_HBITS_HIGH	 8
#define NRS_TRR_HBBITS		 2

/**
 * ORR/TRR hash operations
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

	if (strcmp(orrd->od_policy->pol_name, NRS_POL_NAME_ORR) == 0)
		LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
			 "Busy NRS orr policy for object with objid "LPX64" at "
			 "OST with OST index %u, with %d refs\n",
			 orro->oo_key.ok_id, orro->oo_key.ok_idx,
			 cfs_atomic_read(&orro->oo_ref));
	else
		LASSERTF(cfs_atomic_read(&orro->oo_ref) == 0,
			 "Busy NRS TRR policy at OST with index %u, with %d "
			 "refs\n", orro->oo_key.ok_idx,
			 cfs_atomic_read(&orro->oo_ref));

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
	 * If round numbers are equal, the two requests are either for the same
	 * backend-fs object (if this is an ORR policy instance), or for the
	 * same OST (if this is a TRR policy instance), so these requests should
	 * be sorted by ascending offset.
	 */
	if (nrq1->nr_u.orr.or_range.or_start < nrq2->nr_u.orr.or_range.or_start)
		return 1;
	else if (nrq1->nr_u.orr.or_range.or_start >
		 nrq2->nr_u.orr.or_range.or_start)
		return 0;
	/**
	 * Requests start from the same offset.
	 */
	else
		/**
		 * Do the shorter one first; maybe slightly more chances of
		 * hitting caches like this.
		 */
		if (nrq1->nr_u.orr.or_range.or_end <
		    nrq2->nr_u.orr.or_range.or_end)
			return 1;
		else
			return 0;
}

/**
 * ORR binary heap operations
 */
static cfs_binheap_ops_t nrs_orr_heap_ops = {
	.hop_enter	= NULL,
	.hop_exit	= NULL,
	.hop_compare	= orr_req_compare,
};

/**
 * Called when an ORR policy instance is started.
 *
 * \param[in] policy The policy
 *
 * \retval -ENOMEM OOM error
 * \retval 0	   success
 */
static int
nrs_orr_start(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	       *orrd;
	unsigned int			cur_bits;
	unsigned int			max_bits;
	unsigned int			bkt_bits;
	bool				is_orr;
	int				rc = 0;
	ENTRY;

	OBD_CPT_ALLOC_PTR(orrd, nrs_pol2cptab(policy), nrs_pol2cptid(policy));
	if (orrd == NULL)
		RETURN(-ENOMEM);

	/*
	 * Binary heap instance for sorted incoming requests.
	 */
	orrd->od_binheap = cfs_binheap_create(&nrs_orr_heap_ops,
					      CBH_FLAG_ATOMIC_GROW,
					      4096, NULL,
					      nrs_pol2cptab(policy),
					      nrs_pol2cptid(policy));
	if (orrd->od_binheap == NULL)
		GOTO(failed, rc = -ENOMEM);

	is_orr = strcmp(policy->pol_name, NRS_POL_NAME_ORR) == 0;

	nrs_orr_genobjname(policy, is_orr, orrd->od_objname);

	/**
	 * Slab cache for NRS ORR objects.
	 */
	orrd->od_cache = cfs_mem_cache_create(orrd->od_objname,
					      sizeof(struct nrs_orr_object),
					      0, 0);
	if (orrd->od_cache == NULL)
		GOTO(failed, rc = -ENOMEM);

	if (is_orr) {
		cur_bits = NRS_ORR_HBITS_LOW;
		max_bits = NRS_ORR_HBITS_HIGH;
		bkt_bits = NRS_ORR_HBBITS;
	} else {
		cur_bits = NRS_TRR_HBITS_LOW;
		max_bits = NRS_TRR_HBITS_HIGH;
		bkt_bits = NRS_TRR_HBBITS;
	}

	/**
	 * Hash for finding objects by struct nrs_orr_key.
	 */
	orrd->od_obj_hash = cfs_hash_create(orrd->od_objname, cur_bits,
					    max_bits, bkt_bits, 0,
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &nrs_orr_hash_ops,
					    CFS_HASH_DEFAULT);

	if (orrd->od_obj_hash == NULL)
		GOTO(failed, rc = -ENOMEM);

	/* XXX: Fields accessed unlocked */
	orrd->od_quantum = NRS_ORR_QUANTUM_DFLT;
	orrd->od_supp = NOS_DFLT;
	orrd->od_physical = true;
	orrd->od_policy = policy;

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
 * Called when an ORR/TRR policy instance is stopped.
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
	LASSERT(spin_is_locked(&policy->pol_nrs->nrs_lock));

	/**
	 * The policy may be stopped, but the lprocfs files and
	 * ptlrpc_nrs_policy instances remain present until unregistration
	 * time.
	 */
	if (policy->pol_private == NULL)
		RETURN(-ENODEV);

	switch(opc) {
	default:
		RETURN(-EINVAL);
		break;

	case NRS_CTL_ORR_RD_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		*(__u16 *)arg = orrd->od_quantum;
		}
		break;

	case NRS_CTL_ORR_WR_QUANTUM: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		orrd->od_quantum = *(__u16 *)arg;
		}
		break;

	case NRS_CTL_ORR_RD_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		*(bool *)arg = orrd->od_physical;
		}
		break;

	case NRS_CTL_ORR_WR_OFF_TYPE: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		orrd->od_physical = *(bool *)arg;
		}
		break;

	case NRS_CTL_ORR_RD_SUPP_REQ: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		*(enum nrs_orr_supp *)arg = orrd->od_supp;
		}
		break;

	case NRS_CTL_ORR_WR_SUPP_REQ: {
		struct nrs_orr_data	*orrd = policy->pol_private;

		orrd->od_supp = *(enum nrs_orr_supp *)arg;
		}
		break;
	}
	RETURN(0);
}

/**
 * Obtains resources for ORR/TRR policy instances. The top-level resource lives
 * inside \e nrs_orr_data and the second-level resource inside
 * \e nrs_orr_object instances.
 *
 * \param[in]  policy	  The policy for which resources are being taken for
 *			  request \a nrq
 * \param[in]  nrq	  The request for which resources are being taken
 * \param[in]  parent	  Parent resource, embedded in nrs_orr_data for the
 *			  ORR/TRR policies
 * \param[out] resp	  Resources references are placed in this array
 * \param[in]  moving_req Signifies limited caller context; used to perform
 *			  memory allocations in an atomic context in this
 *			  policy
 *
 * \retval 0   We are returning a top-level, parent resource, one that is
 *	       embedded in an nrs_orr_data object
 * \retval 1   We are returning a bottom-level resource, one that is embedded
 *	       in an nrs_orr_object object
 *
 * \see nrs_resource_get_safe()
 */
int
nrs_orr_res_get(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq,
		struct ptlrpc_nrs_resource *parent,
		struct ptlrpc_nrs_resource **resp, bool moving_req)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	struct nrs_orr_object	*tmp;
	struct nrs_orr_key	key = { 0 };
	__u32			opc;
	int			rc = 0;
	ENTRY;

	/**
	 * struct nrs_orr_data is requested.
	 */
	if (parent == NULL) {
		*resp = &((struct nrs_orr_data *)policy->pol_private)->od_res;
		return 0;
	}

	orrd = container_of(parent, struct nrs_orr_data, od_res);

	/**
	 * If the request type is not supported, fail the enqueuing; the RPC
	 * will be handled by the fallback NRS policy.
	 */
	if (!nrs_orr_req_supported(orrd, nrq, &opc))
		RETURN(-1);

	/**
	 * Fill in the key for the request; OST index + (for ORR) object ID.
	 */
	rc = nrs_orr_key_fill(orrd, nrq, opc, policy->pol_name, &key);
	if (rc)
		RETURN(rc);

	/**
	 * Set the offset range the request covers
	 */
	rc = nrs_orr_range_fill(nrq, orrd, opc, moving_req);
	if (rc)
		RETURN(rc);

	orro = cfs_hash_lookup(orrd->od_obj_hash, &key);
	if (orro != NULL)
		GOTO(out, 1);

	OBD_SLAB_CPT_ALLOC_PTR_GFP(orro, orrd->od_cache,
				   nrs_pol2cptab(policy), nrs_pol2cptid(policy),
				   moving_req ? CFS_ALLOC_ATOMIC :
				   CFS_ALLOC_IO);
	if (orro == NULL)
		RETURN(-ENOMEM);

	orro->oo_key = key;
	/* XXX: Accessed unlocked. */
	orro->oo_quantum = orrd->od_quantum;
	orro->oo_finished = true;

	cfs_atomic_set(&orro->oo_ref, 1);
	tmp = cfs_hash_findadd_unique(orrd->od_obj_hash, &orro->oo_key,
				      &orro->oo_hnode);
	if (tmp != orro) {
		OBD_SLAB_FREE_PTR(orro, orrd->od_cache);
		orro = tmp;
	}
out:
	/**
	 * For debugging purposes
	 */
	nrq->nr_u.orr.or_key = orro->oo_key;

	*resp = &orro->oo_res;

	return 1;
}

/**
 * Called when releasing references to the resource
 * hierachy obtained for a request for scheduling using ORR/TRR policy instances
 *
 * \param[in] policy   The policy the resource belongs to
 * \param[in] res      The resource to be released
 */
static void
nrs_orr_res_put(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_resource *res)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;
	ENTRY;

	/**
	 * Do nothing for freeing parent, nrs_orr_data resources.
	 */
	if (res->res_parent == NULL)
		return;

	orro = container_of(res, struct nrs_orr_object, oo_res);
	orrd = container_of(res->res_parent, struct nrs_orr_data, od_res);

	cfs_hash_put(orrd->od_obj_hash, &orro->oo_hnode);
}

/**
 * Called when polling an ORR/TRR policy instance for a request so that it can
 * be served. Returns the request that is at the root of the binary heap, as
 * that is the lowest priority one (i.e. libcfs_heap is an implementation of a
 * min-heap)
 *
 * \param[in] policy The policy instance being polled
 *
 * \retval The request to be handled
 *
 * \see ptlrpc_nrs_req_poll_nolock()
 */
static struct ptlrpc_nrs_request *
nrs_orr_req_poll(struct ptlrpc_nrs_policy *policy)
{
	struct nrs_orr_data	*orrd = policy->pol_private;
	cfs_binheap_node_t	*node = cfs_binheap_root(orrd->od_binheap);

	return node == NULL ? NULL :
		container_of(node, struct ptlrpc_nrs_request, nr_node);
}

/**
 * Sort-adds request \a nrq to an ORR/TRR \a policy instance's set of queued
 * requests in the policy's binary heap.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to add
 *
 * \retval 0 request successfully added
 * \retval != 0 error
 */
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

	if (orro->oo_finished == true || orro->oo_active == 0) {
		orro->oo_round = orrd->od_round;
		orrd->od_round++;
		/**
		 * Reset the quantum if we have reached the maximum quantum
		 * size for this batch, or even if we have not managed to
		 * complete a batch size up to its maximum allowed size.
		 * XXX: Accessed unlocked
		 */
		orro->oo_quantum = orrd->od_quantum;
		orro->oo_finished = false;
	}

	nrq->nr_u.orr.or_round = orro->oo_round;
	rc = cfs_binheap_insert(orrd->od_binheap, &nrq->nr_node);
	if (rc == 0) {
		orro->oo_quantum--;
		orro->oo_active++;
		if (orro->oo_quantum == 0) {
			orro->oo_finished = true;
		}
	}
	RETURN(rc);
}

/**
 * Removes request \a nrq from an ORR/TRR \a policy instance's set of queued
 * requests.
 *
 * \param[in] policy The policy
 * \param[in] nrq    The request to remove
 */
static void
nrs_orr_req_del(struct ptlrpc_nrs_policy *policy,
		struct ptlrpc_nrs_request *nrq)
{
	struct nrs_orr_data	*orrd;
	struct nrs_orr_object	*orro;

	orro = container_of(nrs_request_resource(nrq),
			    struct nrs_orr_object, oo_res);
	orrd = container_of(nrs_request_resource(nrq)->res_parent,
			    struct nrs_orr_data, od_res);

	LASSERT(nrq->nr_u.orr.or_round <= orro->oo_round);

	cfs_binheap_remove(orrd->od_binheap, &nrq->nr_node);

	orro->oo_active--;
}

/**
 * Called right before the request \a nrq starts being handled by ORR policy
 * instance \a policy.
 *
 * \param[in] policy The policy handling the request
 * \param[in] nrq    The request being handled
 */
static void
nrs_orr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS start %s request for object with ID "LPX64""
	       " from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

/**
 * Called right after the request \a nrq finishes being handled by ORR policy
 * instance \a policy.
 *
 * \param[in] policy The policy that handled the request
 * \param[in] nrq    The request that was handled
 */
static void
nrs_orr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS stop %s request for object with ID "LPX64" "
	       "from OST with index %u, with round "LPU64"\n",
	       nrs_request_policy(nrq)->pol_name, nrq->nr_u.orr.or_key.ok_id,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

#ifdef LPROCFS
int nrs_orr_lprocfs_init(struct ptlrpc_service *svc);
void nrs_orr_lprocfs_fini(struct ptlrpc_service *svc);
int nrs_trr_lprocfs_init(struct ptlrpc_service *svc);
void nrs_trr_lprocfs_fini(struct ptlrpc_service *svc);
#else
int nrs_orr_lprocfs_init(struct ptlrpc_service *svc) { return 0; }
void nrs_orr_lprocfs_fini(struct ptlrpc_service *svc) { }
int nrs_trr_lprocfs_init(struct ptlrpc_service *svc) { return 0; }
void nrs_trr_lprocfs_fini(struct ptlrpc_service *svc) { }
#endif

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
	.op_lprocfs_init	= nrs_orr_lprocfs_init,
	.op_lprocfs_fini	= nrs_orr_lprocfs_fini,
};

struct ptlrpc_nrs_pol_desc ptlrpc_nrs_orr_desc = {
	.pd_name		= NRS_POL_NAME_ORR,
	.pd_ops			= &nrs_orr_ops,
	.pd_compat		= nrs_policy_compat_one,
	.pd_compat_svc_name	= "ost_io",
};

/**
 * TRR, Target-based Round Robin policy
 *
 * TRR reuses much of the functions and data structures of ORR
 */

/**
 * Called right before the request \a nrq starts being handled by TRR policy
 * instance \a policy.
 *
 * \param[in] policy The policy handling the request
 * \param[in] nrq    The request being handled
 */
static void
nrs_trr_req_start(struct ptlrpc_nrs_policy *policy,
		  struct ptlrpc_nrs_request *nrq)
{
	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS start %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

/**
 * Called right after the request \a nrq finishes being handled by TRR policy
 * instance \a policy.
 *
 * \param[in] policy The policy that handled the request
 * \param[in] nrq    The request that was handled
 */
static void
nrs_trr_req_stop(struct ptlrpc_nrs_policy *policy,
		 struct ptlrpc_nrs_request *nrq)
{
	/* NB: resource control, credits etc can be added to here */
	CDEBUG(D_RPCTRACE, "NRS stop %s request from OST with index %u,"
	       "with round "LPU64"\n", nrs_request_policy(nrq)->pol_name,
	       nrq->nr_u.orr.or_key.ok_idx, nrq->nr_u.orr.or_round);
}

/**
 * Reuse much of the ORR functionality for TRR.
 */
static struct ptlrpc_nrs_pol_ops nrs_trr_ops = {
	.op_policy_start	= nrs_orr_start,
	.op_policy_stop		= nrs_orr_stop,
	.op_policy_ctl		= nrs_orr_ctl,
	.op_res_get		= nrs_orr_res_get,
	.op_res_put		= nrs_orr_res_put,
	.op_req_poll		= nrs_orr_req_poll,
	.op_req_enqueue		= nrs_orr_req_add,
	.op_req_dequeue		= nrs_orr_req_del,
	/**
	 * XXX: Might want to have one version of these functions as well for
	 * both policies ORR and TRR, as in other functions.
	 */
	.op_req_start		= nrs_trr_req_start,
	.op_req_stop		= nrs_trr_req_stop,
	.op_lprocfs_init	= nrs_trr_lprocfs_init,
	.op_lprocfs_fini	= nrs_trr_lprocfs_fini,
};

struct ptlrpc_nrs_pol_desc ptlrpc_nrs_trr_desc = {
	.pd_name		= NRS_POL_NAME_TRR,
	.pd_ops			= &nrs_trr_ops,
	.pd_compat		= nrs_policy_compat_one,
	.pd_compat_svc_name	= "ost_io",
};

/** @} ORR/TRR policy */

/** @} nrs */
