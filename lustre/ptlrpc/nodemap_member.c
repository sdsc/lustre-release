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

/**
 * member hash functions
 *
 * The purpose of this hash is to maintain the list of
 * exports that are connected and associated with a
 * particular nodemap
 */
static void member_getref(struct obd_export *exp)
{
	return;
}

void member_putref(struct obd_export *exp)
{
	return;
}

static __u32 member_hashfn(cfs_hash_t *hash_body,
			   const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, strlen(key), mask);
}

static void *member_hs_key(struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);

	return exp;
}

static int member_hs_keycmp(const void *key, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);

	return key == exp;
}

static void *member_hs_hashobject(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct obd_export,
			   exp_target_data.ted_nodemap_member);
}

static void member_hs_get(cfs_hash_t *hs, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	member_getref(exp);
}

static void member_hs_put_locked(cfs_hash_t *hs,
				 struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
}

/**
 * Delete a member from a member hash
 *
 * \param	nodemap		nodemap containing hash
 * \paraa	nid		nid of member to delete
 */
int member_del(struct lu_nodemap *nodemap, struct obd_export *exp)
{

	exp->exp_target_data.ted_nodemap = NULL;
	exp = cfs_hash_del_key(nodemap->nm_member_hash, exp);
	if (exp == NULL)
		goto out;

	class_export_put(exp);
out:
	return 0;
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
	char nodemap_hashname[LUSTRE_NODEMAP_NAME_LENGTH + 3];


	snprintf(nodemap_hashname, LUSTRE_NODEMAP_NAME_LENGTH + 3,
		 "nm-%s", nodemap->nm_name);
	nodemap->nm_member_hash = cfs_hash_create(nodemap_hashname,
					  HASH_NODEMAP_MEMBER_CUR_BITS,
					  HASH_NODEMAP_MEMBER_MAX_BITS,
					  HASH_NODEMAP_MEMBER_BKT_BITS, 0,
					  CFS_HASH_MIN_THETA,
					  CFS_HASH_MAX_THETA,
					  &member_hash_operations,
					  CFS_HASH_DEFAULT);
	if (nodemap->nm_member_hash == NULL)
		return -ENOMEM;

	return 0;
}

/**
 * Callback from deleting a hash member
 */
static int member_delete_hash_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				 struct hlist_node *hnode, void *data)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);

	exp->exp_target_data.ted_nodemap = NULL;
	cfs_hash_bd_del_locked(hs, bd, hnode);
	class_export_put(exp);

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
void member_add(struct lu_nodemap *nodemap, struct obd_export *exp)
{
	int	rc = 0;

	if (exp == NULL) {
		CWARN("attempted to add null export to nodemap %s\n",
		      nodemap->nm_name);
		return;
	}

	if (hlist_unhashed(&exp->exp_target_data.ted_nodemap_member) == 0) {
		CWARN("export %p %s already hashed, failed to add to "
		      "nodemap %s\n", exp, exp->exp_client_uuid.uuid,
		      nodemap->nm_name);
		return;
	}

	rc = cfs_hash_add_unique(nodemap->nm_member_hash, exp,
				 &exp->exp_target_data.ted_nodemap_member);
	if (rc == 0) {
		exp->exp_target_data.ted_nodemap = nodemap;
		class_export_get(exp);
	} else {
		CWARN("unable to add exp: %p %s to nodemap %s: rc %d\n",
		      exp, exp->exp_client_uuid.uuid, nodemap->nm_name, rc);
	}

	return;
}

/**
 * Callback to generate a linked list of all exports in a nodemap
 */
static int member_get_exports_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				  struct hlist_node *hnode, void *data)
{
	struct list_head	*export_head = data;
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	if (exp == NULL)
		goto out;
	list_add_tail(&exp->exp_target_data.ted_nodemap_iterator, export_head);
out:
	return 0;
}

/**
 * Reclassify the members of a nodemap after range changes or activation
 *
 * \param	nodemap		nodemap with members to reclassify
 */
void member_reclassify_nodemap(struct lu_nodemap *nodemap)
{
	struct list_head	reclassify_exports;
	struct obd_export	*exp;
	struct obd_export	*tmp_exp;
	struct lu_nodemap	*new_nodemap;
	lnet_nid_t		nid;

	INIT_LIST_HEAD(&reclassify_exports);
	cfs_hash_for_each(nodemap->nm_member_hash,
			  member_get_exports_cb,
			  &reclassify_exports);

	list_for_each_entry_safe(exp, tmp_exp, &reclassify_exports,
				 exp_target_data.ted_nodemap_iterator) {
		list_del(&exp->exp_target_data.ted_nodemap_iterator);
		nid = exp->exp_connection->c_peer.nid;
		new_nodemap = nodemap_classify_nid(nid);

		if (nodemap->nm_id != new_nodemap->nm_id) {
			member_del(nodemap, exp);
			member_add(new_nodemap, exp);
		}
	}

	return;
}

/**
 * Revoke the locks for member exports. Changing the idmap is
 * akin to deleting the security context. If the locks are not
 * canceled, the client could cache permissions that are no
 * longer correct with the map.
 *
 * \param	nodemap		nodemap that has been altered
 */
void member_revoke_locks(struct lu_nodemap *nodemap)
{
	struct list_head	revoke_exports;
	struct obd_export	*exp;
	struct obd_export	*tmp_exp;

	INIT_LIST_HEAD(&revoke_exports);
	cfs_hash_for_each(nodemap->nm_member_hash,
			  member_get_exports_cb,
			  &revoke_exports);

	list_for_each_entry_safe(exp, tmp_exp, &revoke_exports,
				 exp_target_data.ted_nodemap_iterator) {
		ldlm_revoke_export_locks(exp);
		list_del(&exp->exp_target_data.ted_nodemap_iterator);
	}
}
