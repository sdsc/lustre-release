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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif

#include <linux/version.h>
#include <asm/statfs.h>

#include <linux/module.h>

/* LUSTRE_VERSION_CODE */
#include <lustre_ver.h>

#include <lustre_nodemap.h>
#include <lustre_fid.h>
#include <lustre_capa.h>
#include <lustre_net.h>

#include "nodemap_internal.h"

/* Functions to update the nodemap index file in the
 * CONFIGS directory on the MGS.
 */

static const char nodemap_idx_filename[] = "nodemap";

static void nodemap_prep_cluster_key(struct nodemap_key *nk, unsigned int nm_id)
{
	nk->uid.n.id = nm_idx_set_type(nm_id, NODEMAP_CLUSTER_IDX);
	nk->uid.n.unused = 0;
}

static void nodemap_prep_cluster_rec(struct nodemap_rec *nr,
				     struct lu_nodemap *nodemap)
{
	strncpy(nr->val.n.name, nodemap->nm_name, LUSTRE_NODEMAP_NAME_LENGTH);
	nr->val.n.squash_uid = cpu_to_le32(nodemap->nm_squash_uid);
	nr->val.n.squash_gid = cpu_to_le32(nodemap->nm_squash_gid);
	nr->val.n.flags = nodemap->nmf_trust_client_ids << 4
		| nodemap->nmf_allow_root_access << 3
		| nodemap->nmf_block_lookups << 2
		| nodemap->nmf_hmac_required << 1
		| nodemap->nmf_encryption_required << 0;
}

static void nodemap_prep_idmap_key(struct nodemap_key *nk, unsigned int nm_id,
				   __u32 id_client, int id_type)
{
	int idx_type;

	if (id_type == NODEMAP_UID)
		idx_type = NODEMAP_UIDMAP_IDX;
	else
		idx_type = NODEMAP_GIDMAP_IDX;

	nk->uid.i.nodemap_id = nm_idx_set_type(nm_id, idx_type);
	nk->uid.i.id_client = id_client;
	nk->uid.global = cpu_to_le64(nk->uid.global);
}

static void nodemap_prep_idmap_rec(struct nodemap_rec *nr, const __u32 id_fs)
{
	nr->val.i.id_fs = cpu_to_le32(id_fs);
}

static void nodemap_prep_range_key(struct nodemap_key *nk, unsigned int nm_id,
				   unsigned int rn_id)
{
	nk->uid.r.nodemap_id = nm_idx_set_type(nm_id, NODEMAP_RANGE_IDX);
	nk->uid.r.range_id = rn_id;
	nk->uid.global = cpu_to_le64(nk->uid.global);
}

static void nodemap_prep_range_rec(struct nodemap_rec *nr,
				   const lnet_nid_t nid[2])
{
	nr->val.r.start_nid = cpu_to_le64(nid[0]);
	nr->val.r.end_nid = cpu_to_le64(nid[1]);
}

static struct dt_object *nodemap_idx_find_create(const struct lu_env *env,
						 struct local_oid_storage *los,
						 struct dt_object *parent)
{
	struct dt_object	*nodemap_idx;
	int			 rc;
	__u32			 mode = S_IFREG | S_IRUGO | S_IWUSR;

	nodemap_idx = local_index_find_or_create(env, los, parent,
						 nodemap_idx_filename,
						 mode,
						 &dt_nodemap_features);

	if (IS_ERR(nodemap_idx))
		RETURN(nodemap_idx);

	if (nodemap_idx->do_index_ops == NULL) {
		rc = nodemap_idx->do_ops->do_index_try(env, nodemap_idx,
				&dt_nodemap_features);

		if (rc) {
			lu_object_put(env, &nodemap_idx->do_lu);
			nodemap_idx = ERR_PTR(rc);
		}
	}

	RETURN(nodemap_idx);
}

/* should be called with write lock */
static void nodemap_inc_version(const struct lu_env *env,
				struct dt_object *nodemap_idx,
				struct thandle *th)
{
	__u64 ver = dt_version_get(env, nodemap_idx);
	ver++;
	dt_version_set(env, nodemap_idx, ver, th);
}

static int nodemap_idx_insert(const struct lu_env *env,
			      struct local_oid_storage *los,
			      struct dt_device *dev,
			      struct dt_object *parent,
			      struct nodemap_key *nk,
			      struct nodemap_rec *nr)
{
	struct thandle		*th;
	struct dt_object	*nodemap_idx;
	int			 rc = 0;

	nodemap_idx = nodemap_idx_find_create(env, los, parent);
	if (IS_ERR(nodemap_idx)) {
		rc = PTR_ERR(nodemap_idx);
		CERROR("failed to create nodemap index, rc: %d\n", rc);
		RETURN(rc);
	}
	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_insert(env, nodemap_idx, (const struct dt_rec *)nr,
			       (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, nodemap_idx, 0);

	rc = dt_insert(env, nodemap_idx, (const struct dt_rec *)nr,
		       (const struct dt_key *)nk, th, BYPASS_CAPA, 1);

	nodemap_inc_version(env, nodemap_idx, th);
	dt_write_unlock(env, nodemap_idx);
out:
	dt_trans_stop(env, dev, th);
	lu_object_put(env, &nodemap_idx->do_lu);
	return rc;
}

static int nodemap_idx_update(const struct lu_env *env,
			      struct local_oid_storage *los,
			      struct dt_device *dev,
			      struct dt_object *parent,
			      struct nodemap_key *nk, struct nodemap_rec *nr)
{
	struct thandle		*th;
	struct dt_object	*nodemap_idx;
	int			 rc = 0;

	nodemap_idx = nodemap_idx_find_create(env, los, parent);
	if (IS_ERR(nodemap_idx)) {
		rc = PTR_ERR(nodemap_idx);
		CERROR("failed to create nodemap index, rc: %d\n", rc);
		RETURN(rc);
	}
	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, nodemap_idx, (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_insert(env, nodemap_idx, (const struct dt_rec *)nr,
			       (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_version_set(env, nodemap_idx, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, nodemap_idx, 0);

	rc = dt_delete(env, nodemap_idx, (const struct dt_key *)nk, th,
		       BYPASS_CAPA);
	if (rc != 0)
		GOTO(out_lock, rc);

	rc = dt_insert(env, nodemap_idx, (const struct dt_rec *)nr,
		       (const struct dt_key *)nk, th, BYPASS_CAPA, 1);
	if (rc != 0)
		GOTO(out_lock, rc);

	nodemap_inc_version(env, nodemap_idx, th);
out_lock:
	dt_write_unlock(env, nodemap_idx);
out:
	dt_trans_stop(env, dev, th);
	lu_object_put(env, &nodemap_idx->do_lu);
	return rc;
}

static int nodemap_idx_delete(const struct lu_env *env,
			      struct local_oid_storage *los,
			      struct dt_device *dev,
			      struct dt_object *parent,
			      struct nodemap_key *nk)
{
	struct thandle		*th;
	struct dt_object	*nodemap_idx;
	int			 rc = 0;

	nodemap_idx = nodemap_idx_find_create(env, los, parent);
	if (IS_ERR(nodemap_idx)) {
		rc = PTR_ERR(nodemap_idx);
		CERROR("failed to create nodemap index, rc: %d\n", rc);
		RETURN(rc);
	}
	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, nodemap_idx, (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_version_set(env, nodemap_idx, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, nodemap_idx, 0);

	rc = dt_delete(env, nodemap_idx, (const struct dt_key *)nk, th,
		       BYPASS_CAPA);

	nodemap_inc_version(env, nodemap_idx, th);

	dt_write_unlock(env, nodemap_idx);
out:
	dt_trans_stop(env, dev, th);
	lu_object_put(env, &nodemap_idx->do_lu);
	return rc;
}

int nodemap_idx_nodemap_add_update(const struct lu_env *env,
				   struct local_oid_storage *los,
				   struct dt_device *dev,
				   struct dt_object *mgs_configs_dir,
				   const char *nodemap_name, bool update)
{
	struct lu_nodemap	*nodemap;
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	int rc = 0;

	rc = nodemap_lookup(nodemap_name, &nodemap);
	if (rc != 0)
		GOTO(out, rc = -EINVAL);

	nodemap_prep_cluster_key(&nk, nodemap->nm_id);
	nodemap_prep_cluster_rec(&nr, nodemap);

	if (update)
		rc = nodemap_idx_update(env, los, dev, mgs_configs_dir,
					&nk, &nr);
	else
		rc = nodemap_idx_insert(env, los, dev, mgs_configs_dir,
					&nk, &nr);
	nodemap_putref(nodemap);
out:
	return rc;
}

int nodemap_idx_nodemap_add(const struct lu_env *env,
			    struct local_oid_storage *los,
			    struct dt_device *dev,
			    struct dt_object *mgs_configs_dir,
			    const char *nodemap_name)
{
	return nodemap_idx_nodemap_add_update(env, los, dev, mgs_configs_dir,
					      nodemap_name, 0);
}
EXPORT_SYMBOL(nodemap_idx_nodemap_add);

int nodemap_idx_nodemap_update(const struct lu_env *env,
			       struct local_oid_storage *los,
			       struct dt_device *dev,
			       struct dt_object *mgs_configs_dir,
			       const char *nodemap_name)
{
	return nodemap_idx_nodemap_add_update(env, los, dev, mgs_configs_dir,
					      nodemap_name, 1);
}
EXPORT_SYMBOL(nodemap_idx_nodemap_update);

int nodemap_idx_nodemap_del(const struct lu_env *env,
			    struct local_oid_storage *los,
			    struct dt_device *mgs_bottom,
			    struct dt_object *mgs_configs_dir,
			    const char *nodemap_name)
{
	struct lu_nodemap	*nodemap;
	struct rb_root		 root;
	struct lu_idmap		*idmap;
	struct lu_idmap		*temp;
	struct lu_nid_range	*range;
	struct lu_nid_range	*range_temp;
	struct nodemap_key	 nk;
	int			 rc = 0;

	rc = nodemap_lookup(nodemap_name, &nodemap);
	if (rc != 0)
		GOTO(out, rc = -EINVAL);

	/* XXX should we take a lock here to prevent idmap additions? */
	root = nodemap->nm_fs_to_client_uidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_fs_to_client) {
		nodemap_prep_idmap_key(&nk, nodemap->nm_id, idmap->id_client,
				       NODEMAP_UID);
		rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir,
					&nk);
		if (rc != 0)
			GOTO(out_putref, rc);
	}

	root = nodemap->nm_client_to_fs_gidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_client_to_fs) {
		nodemap_prep_idmap_key(&nk, nodemap->nm_id, idmap->id_client,
				       NODEMAP_GID);
		rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir,
					&nk);
		if (rc != 0)
			GOTO(out_putref, rc);
	}

	list_for_each_entry_safe(range, range_temp, &nodemap->nm_ranges,
				 rn_list) {
		nodemap_prep_range_key(&nk, nodemap->nm_id, range->rn_id);
		rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir,
					&nk);
		if (rc != 0)
			GOTO(out_putref, rc);
	}

	nodemap_prep_cluster_key(&nk, nodemap->nm_id);
	rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir, &nk);
	if (rc != 0)
		GOTO(out, rc);

out_putref:
	nodemap_putref(nodemap);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_idx_nodemap_del);

int nodemap_idx_add_range(const struct lu_env *env,
			  struct local_oid_storage *los,
			  struct dt_device *mgs_bottom,
			  struct dt_object *mgs_configs_dir,
			  const lnet_nid_t nid[2])
{
	struct lu_nid_range	*range;
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	int			 rc = 0;

	range = range_find(nid[0], nid[1]);
	if (range == NULL)
		GOTO(out, rc = -EINVAL);

	nodemap_prep_range_key(&nk, range->rn_nodemap->nm_id, range->rn_id);
	nodemap_prep_range_rec(&nr, nid);

	rc = nodemap_idx_insert(env, los, mgs_bottom, mgs_configs_dir,
				&nk, &nr);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_idx_add_range);

int nodemap_idx_del_range(const struct lu_env *env,
			  struct local_oid_storage *los,
			  struct dt_device *mgs_bottom,
			  struct dt_object *mgs_configs_dir,
			  const lnet_nid_t nid[2])
{
	struct lu_nid_range	*range;
	struct nodemap_key	 nk;
	int			 rc = 0;

	range = range_find(nid[0], nid[1]);
	if (range == NULL)
		GOTO(out, rc = -EINVAL);

	nodemap_prep_range_key(&nk, range->rn_nodemap->nm_id, range->rn_id);
	rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir, &nk);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_idx_del_range);

int nodemap_idx_add_idmap(const struct lu_env *env,
			  struct local_oid_storage *los,
			  struct dt_device *mgs_bottom,
			  struct dt_object *mgs_configs_dir,
			  const char *nodemap_name, const __u32 map[2],
			  bool is_gid)
{
	struct lu_nodemap	*nodemap;
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	int			 rc = 0;

	rc = nodemap_lookup(nodemap_name, &nodemap);
	if (rc != 0)
		GOTO(out, rc = -EINVAL);

	nodemap_prep_idmap_key(&nk, nodemap->nm_id, map[0], is_gid);
	nodemap_prep_idmap_rec(&nr, map[1]);

	rc = nodemap_idx_insert(env, los, mgs_bottom, mgs_configs_dir,
				&nk, &nr);
	nodemap_putref(nodemap);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_idx_add_idmap);

int nodemap_idx_del_idmap(const struct lu_env *env,
			  struct local_oid_storage *los,
			  struct dt_device *mgs_bottom,
			  struct dt_object *mgs_configs_dir,
			  const char *nodemap_name, const __u32 map[2],
			  bool is_gid)
{
	struct lu_nodemap	*nodemap;
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	int			 rc = 0;

	rc = nodemap_lookup(nodemap_name, &nodemap);
	if (rc != 0)
		GOTO(out, rc = -EINVAL);

	nodemap_prep_idmap_key(&nk, nodemap->nm_id, map[0], is_gid);
	nodemap_prep_idmap_rec(&nr, map[1]);

	rc = nodemap_idx_delete(env, los, mgs_bottom, mgs_configs_dir, &nk);
	nodemap_putref(nodemap);
out:
	return rc;
}
EXPORT_SYMBOL(nodemap_idx_del_idmap);

int nodemap_process_keyrec(struct nodemap_key *key,
			   struct nodemap_rec *rec,
			   struct lu_nodemap **first_nodemap)
{
	struct lu_nodemap	*nodemap_head;
	struct lu_nodemap	*nodemap = NULL;
	int			 type;
	__u32			 nodemap_id;
	lnet_nid_t		 nid[2];
	__u32			 map[2];
	int			 rc = 0;

	key->uid.global = le64_to_cpu(key->uid.global);
	type = nm_idx_get_type(key->uid.n.id);
	nodemap_id = nm_idx_set_type(key->uid.n.id, 0);

	nodemap_head = *first_nodemap;

	/* find the correct nodemap in the load list */
	if (type == NODEMAP_RANGE_IDX || type == NODEMAP_UIDMAP_IDX
			|| type == NODEMAP_GIDMAP_IDX) {
		if (nodemap_head == NULL) {
			CERROR("Nodemap index file corrupted\n");
			GOTO(out, rc = -EINVAL);
		}

		/* gets a reference, put by the helpers */
		nodemap = nodemap_get_by_id(nodemap_head, nodemap_id);
		if (nodemap == NULL)
			GOTO(out, rc = -ENOENT);

		if (nodemap != nodemap_head)
			*first_nodemap = nodemap;
	}

	switch (type) {
	case NODEMAP_EMPTY_IDX:
		if (nodemap_id != 0)
			CWARN("Found an empty index type, but non-zero nodemap"
			      " ID. Index corrupt?\n");

		GOTO(out, rc = 0);
	case NODEMAP_CLUSTER_IDX:
		rc = nodemap_lookup(rec->val.n.name, &nodemap);
		if (rc == -EINVAL) {
			CERROR("Invalid nodemap name %s, index corrupted?\n",
			       rec->val.n.name);
			goto out;
		}
		if (rc == -ENOENT) {
			rc = nodemap_add(rec->val.n.name);
			if (rc != 0)
				goto out;

			rc = nodemap_lookup(rec->val.n.name, &nodemap);
			if (rc != 0)
				goto out;

			/* we need to override the local ID with the saved ID */
			nodemap_set_id(nodemap, nodemap_id);
		} else if (nodemap->nm_id != nodemap_id) {
			GOTO(out_putref, rc = -EINVAL);
		}

		if (rc == -EEXIST) {
			/* index file should only contain each nodemap once */
			GOTO(out_putref, rc = -EINVAL);
		}
		if (rc != 0)
			GOTO(out_putref, rc);

		nodemap->nm_squash_uid = le32_to_cpu(rec->val.n.squash_uid);
		nodemap->nm_squash_gid = le32_to_cpu(rec->val.n.squash_gid);
		nodemap->nmf_encryption_required = rec->val.n.flags & (1 << 0);
		nodemap->nmf_hmac_required = rec->val.n.flags & (1 << 1);
		nodemap->nmf_block_lookups = rec->val.n.flags & (1 << 2);
		nodemap->nmf_allow_root_access = rec->val.n.flags & (1 << 3);
		nodemap->nmf_trust_client_ids = rec->val.n.flags & (1 << 4);
		if (nodemap_head == NULL) {
			*first_nodemap = nodemap;
			INIT_LIST_HEAD(&nodemap->nm_load_list);
		} else {
			list_add_tail(&nodemap->nm_load_list,
				      &nodemap_head->nm_load_list);
		}
		GOTO(out_putref, rc = 0);
		break;
	case NODEMAP_RANGE_IDX:
		nid[0] = le64_to_cpu(rec->val.r.start_nid);
		nid[1] = le64_to_cpu(rec->val.r.end_nid);

		rc = nodemap_add_range_helper(nodemap, nid);
		break;
	case NODEMAP_UIDMAP_IDX:
		map[0] = key->uid.i.id_client;
		map[1] = le32_to_cpu(rec->val.i.id_fs);

		rc = nodemap_add_idmap_helper(nodemap, NODEMAP_UID, map);
		break;
	case NODEMAP_GIDMAP_IDX:
		map[0] = key->uid.i.id_client;
		map[1] = le32_to_cpu(rec->val.i.id_fs);

		rc = nodemap_add_idmap_helper(nodemap, NODEMAP_GID, map);
		break;
	case NODEMAP_GLOBAL_IDX:
		/* XXX not entirely sure how to handle this */
	default:
		CERROR("got keyrec pair for type %d, not yet implemented\n",
		       type);
	}

	goto out;
out_putref:
	nodemap_putref(nodemap);
out:
	return rc;
}

int nodemap_load_entries(const struct lu_env *env,
			 struct local_oid_storage *los,
			 struct dt_object *parent)
{
	const struct dt_it_ops  *iops;
	struct dt_object	*nodemap_idx;
	struct dt_it            *it;
	struct lu_nodemap	*first_nodemap = NULL;
	__u64			 hash = 0;
	int			 rc = 0;

	/* wipe out any pre-existing configuration */
	nodemap_cleanup_light();

	nodemap_idx = nodemap_idx_find_create(env, los, parent);
	if (IS_ERR(nodemap_idx)) {
		rc = PTR_ERR(nodemap_idx);
		CERROR("failed to create nodemap index, rc: %d\n", rc);
		RETURN(rc);
	}
	iops = &nodemap_idx->do_index_ops->dio_it;

	dt_read_lock(env, nodemap_idx, 0);
	it = iops->init(env, nodemap_idx, 0, BYPASS_CAPA);
	if (IS_ERR(it))
		GOTO(out, PTR_ERR(it));

	rc = iops->load(env, it, hash);
	if (rc == 0) {
		/*
		 * Iterator didn't find record with exactly the key requested.
		 *
		 * It is currently either
		 *
		 *     - positioned above record with key less than
		 *     requested---skip it.
		 *     - or not positioned at all (is in IAM_IT_SKEWED
		 *     state)---position it on the next item.
		 */
		rc = iops->next(env, it);
		if (rc != 0)
			GOTO(out_iops, rc = 0);
	} else if (rc > 0) {
		rc = 0;
	}

	do {
		struct nodemap_key *key;
		struct nodemap_rec rec;

		key = (struct nodemap_key *)iops->key(env, it);
		rc = iops->rec(env, it, (struct dt_rec *)&rec, 0);
		if (rc != -ESTALE) {
			if (rc != 0)
				GOTO(out_iops, rc);
			rc = nodemap_process_keyrec(key, &rec, &first_nodemap);
			if (rc != 0)
				GOTO(out_iops, rc);
		}

		do {
			rc = iops->next(env, it);
		} while (rc == -ESTALE);
	} while (rc == 0);
out_iops:
	iops->put(env, it);
	iops->fini(env, it);
out:
	dt_read_unlock(env, nodemap_idx);
	lu_object_put(env, &nodemap_idx->do_lu);
	return rc;
}
EXPORT_SYMBOL(nodemap_load_entries);
