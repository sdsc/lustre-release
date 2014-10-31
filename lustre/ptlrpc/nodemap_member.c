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
static void nm_member_getref(struct obd_export *exp)
{
}

void nm_member_putref(struct obd_export *exp)
{
}

static __u32 nm_member_hashfn(cfs_hash_t *hash_body,
			   const void *key, unsigned mask)
{
	return cfs_hash_djb2_hash(key, strlen(key), mask);
}

static void *nm_member_hs_key(struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);

	return exp;
}

static int nm_member_hs_keycmp(const void *key, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);

	return key == exp;
}

static void *nm_member_hs_hashobject(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct obd_export,
			   exp_target_data.ted_nodemap_member);
}

static void nm_member_hs_get(cfs_hash_t *hs, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	nm_member_getref(exp);
}

static void nm_member_hs_put_locked(cfs_hash_t *hs,
				 struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	nm_member_putref(exp);
}

/**
 * Delete a member from a member hash
 *
 * \param	nodemap		nodemap containing hash
 * \paraa	nid		nid of member to delete
 */
void nm_member_del(struct lu_nodemap *nodemap, struct obd_export *exp)
{

	exp->exp_target_data.ted_nodemap = NULL;
	exp = cfs_hash_del_key(nodemap->nm_member_hash, exp);
	if (exp == NULL)
		return;

	class_export_put(exp);
}

static cfs_hash_ops_t nm_member_hash_operations = {
	.hs_hash	= nm_member_hashfn,
	.hs_key		= nm_member_hs_key,
	.hs_keycmp	= nm_member_hs_keycmp,
	.hs_object	= nm_member_hs_hashobject,
	.hs_get		= nm_member_hs_get,
	.hs_put_locked	= nm_member_hs_put_locked,
};

/**
 * Init a member hash of a nodemap
 *
 * \param	nodemap		nodemap containing the member hash
 */
int nm_member_init_hash(struct lu_nodemap *nodemap)
{
	char nodemap_hashname[LUSTRE_NODEMAP_NAME_LENGTH + 3];


	snprintf(nodemap_hashname, sizeof(nodemap_hashname),
		 "nm-%s", nodemap->nm_name);
	nodemap->nm_member_hash = cfs_hash_create(nodemap_hashname,
					  HASH_NODEMAP_MEMBER_CUR_BITS,
					  HASH_NODEMAP_MEMBER_MAX_BITS,
					  HASH_NODEMAP_MEMBER_BKT_BITS, 0,
					  CFS_HASH_MIN_THETA,
					  CFS_HASH_MAX_THETA,
					  &nm_member_hash_operations,
					  CFS_HASH_DEFAULT);
	if (nodemap->nm_member_hash == NULL)
		return -ENOMEM;

	return 0;
}

/**
 * Callback from deleting a hash member
 */
static int nm_member_delete_hash_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
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
void nm_member_delete_hash(struct lu_nodemap *nodemap)
{
	cfs_hash_for_each_safe(nodemap->nm_member_hash,
			       nm_member_delete_hash_cb,
			       nodemap);
	cfs_hash_putref(nodemap->nm_member_hash);
}

/**
 * Add a member export to a nodemap
 *
 * \param	nodemap		nodemap to search
 * \param	exp		obd_export to search
 * \retval	-EEXIST		export is already hashed to a different nodemap
 * \retval	-EINVAL		export is NULL
 */
int nm_member_add(struct lu_nodemap *nodemap, struct obd_export *exp)
{
	int	rc = 0;

	if (exp == NULL) {
		CWARN("attempted to add null export to nodemap %s\n",
		      nodemap->nm_name);
		return -EINVAL;
	}

	if (hlist_unhashed(&exp->exp_target_data.ted_nodemap_member) == 0) {
		/* export is already member of nodemap */
		if (exp->exp_target_data.ted_nodemap == nodemap)
			return 0;

		/* possibly reconnecting while about to be reclassified */
		CWARN("export %p %s already hashed, failed to add to "
		      "nodemap %s already member of %s\n", exp,
		      exp->exp_client_uuid.uuid,
		      nodemap->nm_name,
		      (exp->exp_target_data.ted_nodemap == NULL) ? "unknown" :
				exp->exp_target_data.ted_nodemap->nm_name);
		return -EEXIST;
	}

	exp->exp_target_data.ted_nodemap = nodemap;

	rc = cfs_hash_add_unique(nodemap->nm_member_hash, exp,
				 &exp->exp_target_data.ted_nodemap_member);

	if (rc == 0 )
		class_export_get(exp);
	/* else -EALREADY - exp already in nodemap hash */

	return rc;
}

/**
 * Revokes the locks on an export if it is attached to an MDT and not in
 * recovery. As a performance enhancement, the lock revoking process could
 * revoke only the locks that cover files affected by the nodemap change.
 */
static void nm_member_exp_revoke(struct obd_export *exp)
{
	struct obd_type *type = exp->exp_obd->obd_type;
	if (strcmp(type->typ_name, LUSTRE_MDT_NAME) != 0)
		return;
	if (exp->exp_obd->obd_recovering)
		return;

	ldlm_revoke_export_locks(exp);
}

static int nm_member_reclassify_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				   struct hlist_node *hnode, void *data)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	if (exp == NULL)
		goto out;

	/* Call member add before del so exp->nodemap is never NULL. Must use
	 * bd_del_locked inside a cfs_hash callback. For those reasons, can't
	 * use member_del.
	 */
	nodemap_add_member(exp->exp_connection->c_peer.nid, exp);
	cfs_hash_bd_del_locked(hs, bd, hnode);
	class_export_put(exp);

	nm_member_exp_revoke(exp);
out:
	return 0;
}

/**
 * Reclassify the members of a nodemap after range changes or activation,
 * based on the member export's NID and the nodemap's new NID ranges. Exports
 * that are no longer classified as being part of this nodemap are moved to the
 * nodemap whose NID ranges contain the export's NID, and their locks are
 * revoked.
 *
 * \param	nodemap		nodemap with members to reclassify
 */
void nm_member_reclassify_nodemap(struct lu_nodemap *nodemap)
{
	cfs_hash_for_each_safe(nodemap->nm_member_hash,
			       nm_member_reclassify_cb,
			       NULL);
}

static int nm_member_revoke_locks_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				     struct hlist_node *hnode, void *data)
{
	struct obd_export	*exp;
	exp = hlist_entry(hnode, struct obd_export,
			  exp_target_data.ted_nodemap_member);
	if (exp == NULL)
		return 0;

	nm_member_exp_revoke(exp);
	return 0;
}

/**
 * Revoke the locks for member exports. Changing the idmap is
 * akin to deleting the security context. If the locks are not
 * canceled, the client could cache permissions that are no
 * longer correct with the map.
 *
 * \param	nodemap		nodemap that has been altered
 */
void nm_member_revoke_locks(struct lu_nodemap *nodemap)
{
	cfs_hash_for_each(nodemap->nm_member_hash, nm_member_revoke_locks_cb,
			  NULL);
}
