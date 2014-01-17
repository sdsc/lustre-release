#include <interval_tree.h>
#include <lustre_net.h>
#include "nodemap_internal.h"

/*
 * Range trees
 *
 * To classify clients when they connect, build a global range tree
 * containing all admin defined ranges. Incoming clients can then be
 * classified into their nodemaps, and the lu_nodemap structure will be
 * set in the export structure for the connecting client. Pointers to
 * the lu_nid_range nodes will be added to linked links within the
 * lu_nodemap structure for reporting purposes.A
 */

static struct interval_node *root;
static atomic_t range_highest_id;

void range_init_tree(void)
{
	root = NULL;
}

/*
 * callback for interating over the interval tree
 *
 * \param	n		interval_node matched
 * \param	data		void pointer for return
 *
 * This dunction stops after a single match. There should be
 * no intervals containing multiple ranges
 */
static enum interval_iter range_cb(struct interval_node *n, void *data)
{
	struct lu_nid_range	*range = container_of(n, struct lu_nid_range,
						      rn_node);
	struct lu_nid_range	**ret;

	ret = (struct lu_nid_range **)data;
	*ret = range;

	return INTERVAL_ITER_STOP;
}

/*
 * range constructor
 *
 * \param	min		starting nid of the range
 * \param	max		ending nid of the range
 * \param	nodemap		nodemap that contains this range
 * \retval	lu_nid_range on success, NULL on failure
 */
struct lu_nid_range *range_create(lnet_nid_t start_nid, lnet_nid_t end_nid,
				  struct lu_nodemap *nodemap)
{
	struct lu_nid_range *range;

	if ((LNET_NIDNET(start_nid) != LNET_NIDNET(end_nid)) ||
	    (LNET_NIDADDR(start_nid) > LNET_NIDADDR(end_nid)))
		return NULL;

	OBD_ALLOC_PTR(range);
	if (range == NULL) {
		CERROR("cannot allocate lu_nid_range of size %zu bytes\n",
		       sizeof(*range));
		return NULL;
	}

	range->rn_id = atomic_inc_return(&range_highest_id);
	range->rn_nodemap = nodemap;
	interval_set(&range->rn_node, (__u64)start_nid,
		     (__u64)end_nid);
	INIT_LIST_HEAD(&range->rn_list);

	return range;
}

/*
 * find the exact range
 *
 * \param	start_nid		starting nid
 * \param	end_nid			ending nid
 * \retval	matching range or NULL
 */
struct lu_nid_range *range_find(lnet_nid_t start_nid, lnet_nid_t end_nid)
{
	struct lu_nid_range		*range = NULL;
	struct interval_node		*interval = NULL;
	struct interval_node_extent	ext = {
		.start	= (__u64)start_nid,
		.end	= (__u64)end_nid
	};

	interval = interval_find(root, &ext);

	if (interval != NULL)
		range = container_of(interval, struct lu_nid_range,
				     rn_node);

	return range;
}

/*
 * range destructor
 */
void range_destroy(struct lu_nid_range *range)
{
	LASSERT(list_empty(&range->rn_list) == 0);
	LASSERT(interval_is_intree(&range->rn_node) == 0);

	OBD_FREE_PTR(range);
}

/*
 * insert an nid range into the interval tree
 *
 * \param	range		range to insetr
 * \retval	0 on success
 *
 * This function checks that the given nid range
 * does not overlap so that each nid can belong
 * to exactly one range
 */
int range_insert(struct lu_nid_range *range)
{
	struct interval_node_extent ext =
			range->rn_node.in_extent;

	if (interval_is_overlapped(root, &ext) != 0)
		return -EEXIST;

	interval_insert(&range->rn_node, &root);

	return 0;
}

/*
 * delete a range from the interval tree and any
 * associated nodemap references
 *
 * \param	range		range to remove
 */
void range_delete(struct lu_nid_range *range)
{
	if (range == NULL || interval_is_intree(&range->rn_node) == 0)
		return;
	list_del(&range->rn_list);
	interval_erase(&range->rn_node, &root);
	range_destroy(range);
}

/*
 * search the interval tree for an nid within a range
 *
 * \param	nid		nid to search for
 */
struct lu_nid_range *range_search(lnet_nid_t nid)
{
	struct lu_nid_range		*ret = NULL;
	struct interval_node_extent	ext = {
		.start	= (__u64)nid,
		.end	= (__u64)nid
	};

	interval_search(root, &ext, range_cb, &ret);

	return ret;
}
