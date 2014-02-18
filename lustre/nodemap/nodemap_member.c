/*
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
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */
#include <linux/module.h>
#include <lustre_net.h>
#include <obd_class.h>
#include "nodemap_internal.h"

#define HASH_NODEMAP_MEMBER_BKT_BITS 3
#define HASH_NODEMAP_MEMBER_CUR_BITS 3
#define HASH_NODEMAP_MEMBER_MAX_BITS 7

#define NODEMAP_MEMBER_ID_LENGTH 17

void member_destroy(struct lu_nodemap_member *member)
{
	OBD_FREE_PTR(member);
}

static void member_getref(struct lu_nodemap_member *member)
{
	CDEBUG(D_INFO, "member %p\n", member);
	atomic_inc(&member->mem_refcount);
}

void member_putref(struct lu_nodemap_member *member)
{
	LASSERT(member != NULL);
	LASSERT(atomic_read(&member->mem_refcount) > 0);

	if (atomic_dec_and_test(&member->mem_refcount))
		member_destroy(member);
}

static __u32 member_hashfn(cfs_hash_t *hash_body,
				   const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, strlen(key), mask);
}

static void *member_hs_key(struct hlist_node *hnode)
{
	struct lu_nodemap_member *member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);

	return member->mem_id;
}

static int member_hs_keycmp(const void *key, struct hlist_node *hnode)
{
	struct lu_nodemap_member	*member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);

	return !strcmp(key, member->mem_id);
}

static void *member_hs_hashobject(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct lu_nodemap_member, mem_hash);
}

static void member_hs_get(cfs_hash_t *hs, struct hlist_node *hnode)
{
	struct lu_nodemap_member *member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);
	member_getref(member);
}

static void member_hs_put_locked(cfs_hash_t *hs,
				 struct hlist_node *hnode)
{
	struct lu_nodemap_member *member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);
	member_putref(member);
}

/**
 * Delete a member from a member hash
 *
 * \param	nodemap		nodemap containing hash
 * \paraa	nid		nid of member to delete
 */
int member_del(struct lu_nodemap *nodemap, struct obd_export *exp)
{
	struct lu_nodemap_member	*member = NULL;
	char				id[NODEMAP_MEMBER_ID_LENGTH];
	int				rc = 0;

	snprintf(id, sizeof(id), "%p", exp);

	member = cfs_hash_del_key(nodemap->nm_member_hash, id);
	if (member == NULL)
		GOTO(out, rc = -ENOENT);

	atomic_dec(&exp->exp_refcount);
	member_putref(member);
out:
	return rc;
}

/**
 * Create a member structure from an nid
 *
 * \param	nid		nid of the member
 */
struct lu_nodemap_member *member_create(lnet_nid_t nid)
{
	struct lu_nodemap_member	*member;

	OBD_ALLOC_PTR(member);
	if (member == NULL) {
		CERROR("cannot allocate lu_nodemap_member of size %zu bytes",
		       sizeof(member));
		return NULL;
	}

	member->mem_nid = nid;

	return member;
}

static cfs_hash_ops_t member_hash_operations = {
	.hs_hash	= member_hashfn,
	.hs_key		= member_hs_key,
	.hs_keycmp	= member_hs_keycmp,
	.hs_object	= member_hs_hashobject,
	.hs_get		= member_hs_get,
	.hs_put_locked	= member_hs_put_locked,
};

/**
 * Init a member hash of a nodemap
 *
 * \param	nodemap		nodemap containing the member hash
 */
int member_init_hash(struct lu_nodemap *nodemap)
{
	nodemap->nm_member_hash = cfs_hash_create(nodemap->nm_name,
					  HASH_NODEMAP_MEMBER_CUR_BITS,
					  HASH_NODEMAP_MEMBER_MAX_BITS,
					  HASH_NODEMAP_MEMBER_BKT_BITS, 0,
					  CFS_HASH_MIN_THETA,
					  CFS_HASH_MAX_THETA,
					  &member_hash_operations,
					  CFS_HASH_DEFAULT);
	if (nodemap->nm_member_hash == NULL) {
		CERROR("cannot create %s->nm_member_hash table\n",
		       nodemap->nm_name);
		return -ENOMEM;
	}

	return 0;
}

/**
 * Callback from deleting a hash member
 */
static int member_delete_hash_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
					 struct hlist_node *hnode, void *data)
{
	struct lu_nodemap_member	*member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);
	member_putref(member);

	return 0;
}

/**
 * Delete a member hash from a nodemap
 *
 * \param	nodemap		nodemap to remove the hash from
 */
void member_delete_hash(struct lu_nodemap *nodemap)
{
	cfs_hash_for_each_safe(nodemap->nm_member_hash,
			       member_delete_hash_cb,
			       nodemap);
	cfs_hash_putref(nodemap->nm_member_hash);
}

/**
 * Add a member export to a nodemap
 *
 * \param	nodemap		nodemap to search
 * \param	exp		obd_export to search
 */
void member_add(struct lu_nodemap *nodemap, lnet_nid_t nid,
		struct obd_export *exp)
{
	struct lu_nodemap_member	*member;
	char				id[NODEMAP_MEMBER_ID_LENGTH];

	/* make sure the export doesn't disappear while in the mmember hash */
	atomic_inc(&exp->exp_refcount);

	snprintf(id, sizeof(id), "%p", exp);

	member = cfs_hash_lookup(nodemap->nm_member_hash, id);
	if (member == NULL) {
		member = member_create(nid);
		member->mem_exp = exp;
		snprintf(member->mem_id, sizeof(member->mem_id), "%p", exp);
		atomic_set(&member->mem_refcount, 2);
		cfs_hash_add_unique(nodemap->nm_member_hash, id,
				    &member->mem_hash);
	}
	exp->exp_nodemap = nodemap;
	member_putref(member);
}

/**
 * Callback from member_reclassify_nodemap
 */
static int member_reclassify_nodemap_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
					struct hlist_node *hnode, void *data)
{
	struct lu_nodemap		*nodemap = data;
	struct lu_nodemap		*new_nodemap;
	struct lu_nodemap_member	*member;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);
	new_nodemap = nodemap_classify_nid(member->mem_nid);

	if (nodemap->nm_id != new_nodemap->nm_id) {
		atomic_inc(&member->mem_exp->exp_refcount);
		member_del(nodemap, member->mem_exp);
		member_add(new_nodemap, member->mem_nid, member->mem_exp);
		atomic_dec(&member->mem_exp->exp_refcount);
		cfs_hash_bd_del_locked(hs, bd, hnode);
		member_putref(member);
	}

	return 0;
}

/**
 * Reclassify the members of a nodemap after range changes or activation
 *
 * \param	nodemap		nodemap with members to reclassify
 */
void member_reclassify_nodemap(struct lu_nodemap *nodemap)
{
	cfs_hash_for_each_safe(nodemap->nm_member_hash,
			       member_reclassify_nodemap_cb,
			       nodemap);
}

struct return_export {
	struct obd_export	*re_exp;
	struct list_head	re_list;
};

static int member_revoke_locks_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				  struct hlist_node *hnode, void *data)
{
	struct lu_nodemap_member	*member;
	struct list_head		*exports = data;
	struct return_export		*ret_exp;

	member = hlist_entry(hnode, struct lu_nodemap_member, mem_hash);

	OBD_ALLOC_PTR(ret_exp);
	ret_exp->re_exp = member->mem_exp;
	/* Make sure the export sticks around while we process the list */
	atomic_inc(&member->mem_exp->exp_refcount);
	list_add_tail(&ret_exp->re_list, exports);

	return 0;
}

/**
 * Revoke the locks for member exports. Changing the idmap is
 * akin to deleting the security context. If the locks are not
 * cacneled, the client could cache permissions that are no
 * longer correct with the map.
 *
 * \param	nodemap		nodemap that has been altered
 */
void member_revoke_locks(struct lu_nodemap *nodemap)
{
	struct list_head	exports;
	struct return_export	*ret_exp;
	struct return_export	*tmp_exp;

	INIT_LIST_HEAD(&exports);
	cfs_hash_for_each(nodemap->nm_member_hash,
			  member_revoke_locks_cb,
			  &exports);

	list_for_each_entry_safe(ret_exp, tmp_exp, &exports, re_list) {
		ldlm_revoke_export_locks(ret_exp->re_exp);
		list_del(&ret_exp->re_list);
		atomic_dec(&ret_exp->re_exp->exp_refcount);
		OBD_FREE_PTR(ret_exp);
	}
}
