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

#define MEMBER_KEYLENGTH (2 * UUID_MAX + 1)

/**
 * Create the hash key from the obd_export information
 *
 * \param	obd_export	export structure
 * \param	char		returned key value
 */
void member_key(struct obd_export *exp, char *key)
{
	snprintf(key, MEMBER_KEYLENGTH, "%s:%s",
		 exp->exp_client_uuid.uuid,
		 exp->exp_obd->obd_uuid.uuid);
}

/**
 * member hash functions
 *
 * The purpose of this hash is to maintain the list of
 * exports that are connected and associated with a
 * particular nodemap
 */
static void member_getref(struct obd_export *exp)
{
	CDEBUG(D_INFO, "exp: %s\n", exp->exp_client_uuid.uuid);
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

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);

	return exp->exp_nodemap_member_key;
}

static int member_hs_keycmp(const void *key, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);

	return !strcmp(key, exp->exp_nodemap_member_key);
}

static void *member_hs_hashobject(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct obd_export, exp_nodemap_member);
}

static void member_hs_get(cfs_hash_t *hs, struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);
	member_getref(exp);
}

static void member_hs_put_locked(cfs_hash_t *hs,
				 struct hlist_node *hnode)
{
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);
}

/**
 * Delete a member from a member hash
 *
 * \param	nodemap		nodemap containing hash
 * \paraa	nid		nid of member to delete
 */
int member_del(struct lu_nodemap *nodemap, struct obd_export *exp)
{

	exp->exp_nodemap = NULL;
	exp = cfs_hash_del_key(nodemap->nm_member_hash,
			       exp->exp_nodemap_member_key);
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
	struct obd_export	*exp;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);

	exp->exp_nodemap = NULL;
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
	char	key[MEMBER_KEYLENGTH];
	int	rc = 0;

	if (exp == NULL)
		return;

	if (hlist_unhashed(&exp->exp_nodemap_member) == 0)
		return;

	member_key(exp, key);

	rc = cfs_hash_add_unique(nodemap->nm_member_hash,
				 key,
				 &exp->exp_nodemap_member);
	if (rc == 0) {
		snprintf(exp->exp_nodemap_member_key, MEMBER_KEYLENGTH,
			 "%s", key);
		exp->exp_nodemap = nodemap;
		class_export_get(exp);
	}

	return;
}

struct return_reclassify {
	struct lu_nodemap	*rr_nodemap;
	struct obd_export	*rr_exp;
	struct list_head	rr_list;
};

/**
 * Callback from member_reclassify_nodemap
 */
static int member_reclassify_nodemap_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
					struct hlist_node *hnode, void *data)
{
	struct return_reclassify	*param = data;
	struct return_reclassify	*ret_reclassify;
	struct list_head		*list;
	struct lu_nodemap		*nodemap;
	struct lu_nodemap		*new_nodemap;
	struct obd_export		*exp;
	lnet_nid_t			nid;

	list = &param->rr_list;
	nodemap = param->rr_nodemap;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);
	nid = exp->exp_connection->c_peer.nid;

	new_nodemap = nodemap_classify_nid(nid);

	if (nodemap->nm_id != new_nodemap->nm_id) {
		OBD_ALLOC_PTR(ret_reclassify);
		if (ret_reclassify == NULL) {
			CERROR("cannot allocate 'return_reclassify' "
			       "of size %zu bytes", sizeof(ret_reclassify));
			goto out;
		}
		ret_reclassify->rr_nodemap = new_nodemap;
		ret_reclassify->rr_exp = exp;
		list_add_tail(&ret_reclassify->rr_list, list);
	}
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
	struct return_reclassify	*ret_reclassify;
	struct return_reclassify	*ret_rec;
	struct return_reclassify	*ret_tmp;

	OBD_ALLOC_PTR(ret_reclassify);
	if (ret_reclassify == NULL) {
		CERROR("cannot allocate 'return_reclassify' "
		       "of size %zu bytes", sizeof(struct return_reclassify));
		goto out;
	}

	INIT_LIST_HEAD(&ret_reclassify->rr_list);

	ret_reclassify->rr_nodemap = nodemap;

	cfs_hash_for_each_safe(nodemap->nm_member_hash,
			       member_reclassify_nodemap_cb,
			       ret_reclassify);

	list_for_each_entry_safe(ret_rec, ret_tmp, &ret_reclassify->rr_list,
				 rr_list) {
		list_del(&ret_rec->rr_list);
		if (ret_rec->rr_nodemap->nm_id != nodemap->nm_id) {
			member_del(nodemap, ret_rec->rr_exp);
			member_add(ret_rec->rr_nodemap, ret_rec->rr_exp);
		}
		OBD_FREE_PTR(ret_rec);
	}

	OBD_FREE_PTR(ret_reclassify);
out:
	return;
}

struct return_export {
	struct obd_export	*re_exp;
	struct list_head	re_list;
};

static int member_revoke_locks_cb(cfs_hash_t *hs, cfs_hash_bd_t *bd,
				  struct hlist_node *hnode, void *data)
{
	struct list_head		*exports = data;
	struct obd_export		*exp;
	struct return_export		*ret_exp;

	exp = hlist_entry(hnode, struct obd_export, exp_nodemap_member);

	OBD_ALLOC_PTR(ret_exp);
	if (ret_exp == NULL) {
		CERROR("cannot allocate 'return_export' "
		       "of size %zu bytes", sizeof(struct return_export));
		goto out;
	}
	ret_exp->re_exp = exp;
	/* Make sure the export sticks around while we process the list */
	list_add_tail(&ret_exp->re_list, exports);
out:
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
		OBD_FREE_PTR(ret_exp);
	}
}
