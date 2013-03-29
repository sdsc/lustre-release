/**
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/**
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 */
/**
 * This file is part of lustre/DAOS
 *
 * lustre/daos/daos_handle.c
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#include "daos_internal.h"

#define DAOS_HHASH_BITS_BKT	10
#define DAOS_HHASH_BITS_PID	8
#define DAOS_HHASH_BITS		(DAOS_HHASH_BITS_BKT + DAOS_HHASH_BITS_PID)

#define DAOS_HHASH_PID_MASK	((1ULL << DAOS_HHASH_BITS_PID) - 1)

#define DAOS_HHASH_KEY_MAKE(type, pid, cookie)				\
	((type) | (((pid) & DAOS_HHASH_PID_MASK) << DAOS_HTYPE_BITS) |	\
	 ((cookie) << (DAOS_HTYPE_BITS + DAOS_HHASH_BITS_PID)))

struct daos_handle_bucket {
	__u64		hb_cookie;
	__u32		hb_ref;
};

static unsigned int
daos_hop_hash(cfs_hash_t *hs, const void *key, unsigned int mask)
{
	__u64	val = *(__u64 *)key;

	return (val >> DAOS_HTYPE_BITS) & mask;
}

static void *
daos_hop_object(cfs_hlist_node_t *hnode)
{
	return cfs_hlist_entry(hnode, struct daos_hlink, hl_link);
}

static void *
daos_hop_key(cfs_hlist_node_t *hnode)
{
	struct daos_hlink *hlink = daos_hop_object(hnode);

	return &hlink->hl_key;
}

static int
daos_hop_keycmp(const void *key, cfs_hlist_node_t *hnode)
{
	struct daos_hlink *hlink = daos_hop_object(hnode);

	return *(__u64 *)key == hlink->hl_key;
}

static void
daos_hop_get_locked(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct daos_hlink *hlink = daos_hop_object(hnode);

	hlink->hl_ref++;
}

static void
daos_hop_put_locked(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct daos_hlink *hlink = daos_hop_object(hnode);

	/* the last refcount should always be released by daos_hop_put */
	LASSERT(hlink->hl_ref > 1);
	hlink->hl_ref--;
}

static void
daos_hop_put(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	struct daos_hlink	*hlink = daos_hop_object(hnode);
	cfs_hash_bd_t		bd;
	int			zombie = 0;

	if (likely(hlink->hl_key != 0)) {
		cfs_hash_bd_get_and_lock(hs, &hlink->hl_key, &bd, 1);

		LASSERT(hlink->hl_ref > 0);
		hlink->hl_ref--;
		zombie = hlink->hl_ref == 0;
		if (zombie) { /* decrease counter in hash bucket */
			struct daos_handle_bucket *hb;

			hb = cfs_hash_bd_extra_get(hs, &bd);
			LASSERT(hb->hb_ref > 0);
			hb->hb_ref--;
		}
		cfs_hash_bd_unlock(hs, &bd, 1);
	} else { /* not in hash */
		LASSERT(hlink->hl_ref == 1);
		hlink->hl_ref = 0;
		zombie = 1;
	}

	if (zombie) {
		LASSERT(hlink->hl_ops != NULL);
		hlink->hl_ops->hop_free(hlink);
	}
}

static void
daos_hop_exit(cfs_hash_t *hs, cfs_hlist_node_t *hnode)
{
	LBUG();
}

cfs_hash_ops_t daos_hops = {
	.hs_hash	= daos_hop_hash,
	.hs_key		= daos_hop_key,
	.hs_keycmp	= daos_hop_keycmp,
	.hs_keycpy	= NULL,
	.hs_object	= daos_hop_object,
	.hs_get		= daos_hop_get_locked,
	.hs_put_locked	= daos_hop_put_locked,
	.hs_put		= daos_hop_put,
	.hs_exit	= daos_hop_exit,
};

int
daos_hhash_initialize(void)
{
	the_daos.dm_hhash = cfs_hash_create("daos",
					    DAOS_HHASH_BITS, DAOS_HHASH_BITS,
					    DAOS_HHASH_BITS_BKT,
					    sizeof(struct daos_handle_bucket),
					    CFS_HASH_MIN_THETA,
					    CFS_HASH_MAX_THETA,
					    &daos_hops,
					    CFS_HASH_SPIN_BKTLOCK |
					    CFS_HASH_ASSERT_EMPTY);
	return the_daos.dm_hhash == NULL ? -ENOMEM : 0;
}

void
daos_hhash_finalize(void)
{
	cfs_hash_bd_t	bd;
	int		i;
	int		j = 2;

	if (the_daos.dm_hhash == NULL)
		return;

	cfs_hash_for_each_bucket(the_daos.dm_hhash, &bd, i) {
		struct daos_handle_bucket *hb;

		hb = cfs_hash_bd_extra_get(the_daos.dm_hhash, &bd);

		/* check if there is any alive handle */
		/* NB: even those deleted handles still can have
		 * reference on hash bucket */
		cfs_hash_bd_lock(the_daos.dm_hhash, &bd, 1);
		while (hb->hb_ref > 0) {
			cfs_hash_bd_unlock(the_daos.dm_hhash, &bd, 1);

			CDEBUG(IS_PO2(++j) ? D_WARNING : D_DAOS,
			       "%d daos handles are still alive in bucket[%d] "
			       "of handle hash\n", hb->hb_ref, i);
			set_current_state(TASK_UNINTERRUPTIBLE);
			schedule_timeout(HZ >> 2);

			cfs_hash_bd_lock(the_daos.dm_hhash, &bd, 1);
		}
		cfs_hash_bd_unlock(the_daos.dm_hhash, &bd, 1);
	}

	cfs_hash_putref(the_daos.dm_hhash);
	the_daos.dm_hhash = NULL;
}

struct daos_hlink *
daos_hlink_lookup(__u64 key)
{
	cfs_hlist_node_t *hnode;

	hnode = cfs_hash_lookup(the_daos.dm_hhash, &key);
	return hnode == NULL ? NULL : daos_hop_object(hnode);
}

void
daos_hlink_delete(struct daos_hlink *hlink)
{
	cfs_hash_del(the_daos.dm_hhash, &hlink->hl_key, &hlink->hl_link);
}

void
daos_hlink_putref(struct daos_hlink *hlink)
{
	cfs_hash_put(the_daos.dm_hhash, &hlink->hl_link);
}

void
daos_hlink_key(struct daos_hlink *hlink, __u64 *key)
{
	*key = hlink->hl_key;
}

int
daos_hlink_empty(struct daos_hlink *hlink)
{
	if (hlink->hl_ops == NULL) /* not initialized */
		return 1;

	LASSERT(hlink->hl_ref != 0 || cfs_hlist_unhashed(&hlink->hl_link));
	return cfs_hlist_unhashed(&hlink->hl_link);
}

void
daos_hlink_init(struct daos_hlink *hlink)
{
	CFS_INIT_HLIST_NODE(&hlink->hl_link);
	hlink->hl_key = 0;
	hlink->hl_ref = 1; /* for caller */
}

void
daos_hlink_insert(daos_hlink_type_t type,
		  struct daos_hlink *hlink, struct daos_hlink_ops *ops)
{
	struct daos_handle_bucket	*hb;
	cfs_hash_bd_t			bd;

	hlink->hl_ops = ops;
	hlink->hl_key = DAOS_HHASH_KEY_MAKE(type, current->pid, 0);
	/* err... hacking into cfs_hash because we allocate key within
	 * cfs_hash lock, which violates the rule that key has to be
	 * assigned before adding it into cfs_hash */
	cfs_hash_bd_get_and_lock(the_daos.dm_hhash, &hlink->hl_key, &bd, 1);

	hb = cfs_hash_bd_extra_get(the_daos.dm_hhash, &bd);
	hb->hb_cookie++;
	hb->hb_ref++; /* take reference on hash bucket */
	hlink->hl_key = DAOS_HHASH_KEY_MAKE(type, current->pid, hb->hb_cookie);

	/* refresh bucket descriptor */
	cfs_hash_bd_get(the_daos.dm_hhash, &hlink->hl_key, &bd);

	cfs_hash_bd_add_locked(the_daos.dm_hhash, &bd, &hlink->hl_link);
	cfs_hash_bd_unlock(the_daos.dm_hhash, &bd, 1);
}

int
daos_hlink_insert_key(__u64 key, struct daos_hlink *hlink,
		      struct daos_hlink_ops *ops)
{
	struct daos_handle_bucket *hb;
	cfs_hlist_node_t	  *hnode;
	cfs_hash_bd_t		   bd;
	int			   type;

	type = key & ((1ULL << DAOS_HTYPE_BITS) - 1);
	if (type >= DAOS_HTYPE_MAX)
		return -EINVAL;

	hlink->hl_key = key;

	cfs_hash_bd_get_and_lock(the_daos.dm_hhash, &hlink->hl_key, &bd, 1);
	hb = cfs_hash_bd_extra_get(the_daos.dm_hhash, &bd);

	hnode = cfs_hash_bd_findadd_locked(the_daos.dm_hhash, &bd,
					   &hlink->hl_key, &hlink->hl_link, 1);
	if (hnode == &hlink->hl_link)
		hb->hb_ref++; /* take reference on hash bucket */
	else
		hlink->hl_key = 0;

	cfs_hash_bd_unlock(the_daos.dm_hhash, &bd, 1);

	return hnode == &hlink->hl_link ? 0 : -EEXIST;
}
