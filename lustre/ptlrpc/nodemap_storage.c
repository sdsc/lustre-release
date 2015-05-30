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
 * Copyright (C) 2015, Trustees of Indiana University
 *
 * Copyright (c) 2014, Intel Corporation.
 *
 * Author: Joshua Walgenbach <jjw@iu.edu>
 * Author: Kit Westneat <cwestnea@iu.edu>
 *
 * Implements the storage functionality for the nodemap configuration. Functions
 * in this file prepare, store, and load nodemap configuration data. Targets
 * using nodemap services should register a configuration file object. Nodemap
 * configuration changes that need to persist should call the appropriate
 * storage function for the data being modified.
 *
 * There are several index types:
 *	CLUSTER_IDX	stores the data found on the lu_nodemap struct, like
 *			root squash and config flags, as well as the name.
 *	RANGE_IDX	stores NID range information for a nodemap/cluster
 *	UIDMAP_IDX	stores the fs/client UID pair that describes a map
 *	GIDMAP_IDX	stores the fs/client GID pair that describes a map
 *	GLOBAL_IDX	stores whether or not nodemaps are active
 */

#include <lustre_net.h>
#include <obd_class.h>
#include <dt_object.h>
#include "nodemap_internal.h"

/* list of registered nodemap index files */
static struct list_head ncf_list_head = LIST_HEAD_INIT(ncf_list_head);
static DEFINE_MUTEX(ncf_list_lock);

/* lu_nodemap flags */
#define NM_FL_ROOT	0	/* allow root access */
#define NM_FL_TRUST	1	/* trust client ids */

static void nodemap_prep_cluster_key(struct nodemap_key *nk, unsigned int nm_id)
{
	nk->nk.ck.ck_id = nm_idx_set_type(nm_id, NODEMAP_CLUSTER_IDX);
	nk->nk.ck.unused = 0;
}

static void nodemap_prep_cluster_rec(struct nodemap_rec *nr,
				     struct lu_nodemap *nodemap)
{
	strncpy(nr->nr.ncr.ncr_name, nodemap->nm_name,
		LUSTRE_NODEMAP_NAME_LENGTH);
	nr->nr.ncr.ncr_squash_uid = cpu_to_le32(nodemap->nm_squash_uid);
	nr->nr.ncr.ncr_squash_gid = cpu_to_le32(nodemap->nm_squash_gid);
	nr->nr.ncr.ncr_flags = nodemap->nmf_trust_client_ids << NM_FL_TRUST |
		nodemap->nmf_allow_root_access << NM_FL_ROOT;
}

static void nodemap_prep_idmap_key(struct nodemap_key *nk, unsigned int nm_id,
				   enum nodemap_id_type id_type,
				   __u32 id_client)
{
	int idx_type;

	if (id_type == NODEMAP_UID)
		idx_type = NODEMAP_UIDMAP_IDX;
	else
		idx_type = NODEMAP_GIDMAP_IDX;

	nk->nk.ik.ik_nodemap_id = nm_idx_set_type(nm_id, idx_type);
	nk->nk.ik.ik_id_client = id_client;
	nk->nk.global = cpu_to_le64(nk->nk.global);
}

static void nodemap_prep_idmap_rec(struct nodemap_rec *nr, const __u32 id_fs)
{
	nr->nr.nir.nir_id_fs = cpu_to_le32(id_fs);
}

static void nodemap_prep_range_key(struct nodemap_key *nk, unsigned int nm_id,
				   unsigned int rn_id)
{
	nk->nk.rk.rk_nodemap_id = nm_idx_set_type(nm_id, NODEMAP_RANGE_IDX);
	nk->nk.rk.rk_range_id = rn_id;
	nk->nk.global = cpu_to_le64(nk->nk.global);
}

static void nodemap_prep_range_rec(struct nodemap_rec *nr,
				   const lnet_nid_t nid[2])
{
	nr->nr.nrr.nrr_start_nid = cpu_to_le64(nid[0]);
	nr->nr.nrr.nrr_end_nid = cpu_to_le64(nid[1]);
}

static void nodemap_prep_global_key(struct nodemap_key *nk)
{
	nk->nk.global = cpu_to_le64(nm_idx_set_type(0, NODEMAP_GLOBAL_IDX));
}

static void nodemap_prep_global_rec(struct nodemap_rec *nr, const bool active)
{
	nr->nr.ngr.ngr_is_active = active;
}

/* should be called with dt_write lock */
static void nodemap_inc_version(const struct lu_env *env,
				struct dt_object *nodemap_idx,
				struct thandle *th)
{
	__u64 ver = dt_version_get(env, nodemap_idx);
	ver++;
	dt_version_set(env, nodemap_idx, ver, th);
}

static inline struct dt_device *dt_obj2dev(struct dt_object *obj)
{
	return lu2dt_dev(obj->do_lu.lo_dev);
}

static int nodemap_idx_insert(struct lu_env *env,
			      struct dt_object *idx,
			      struct nodemap_key *nk,
			      struct nodemap_rec *nr)
{
	struct thandle		*th;
	struct dt_device	*dev = dt_obj2dev(idx);
	int			 rc;

	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_insert(env, idx,
			       (const struct dt_rec *)nr,
			       (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, idx, 0);

	rc = dt_insert(env, idx, (const struct dt_rec *)nr,
		       (const struct dt_key *)nk, th, 1);

	nodemap_inc_version(env, idx, th);
	dt_write_unlock(env, idx);
out:
	dt_trans_stop(env, dev, th);

	return rc;
}

static int nodemap_idx_update(struct lu_env *env,
			      struct dt_object *idx,
			      struct nodemap_key *nk,
			      struct nodemap_rec *nr)
{
	struct thandle		*th;
	struct dt_device	*dev = dt_obj2dev(idx);
	int			 rc = 0;

	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, idx, (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_insert(env, idx, (const struct dt_rec *)nr,
			       (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_version_set(env, idx, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, idx, 0);

	rc = dt_delete(env, idx, (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out_lock, rc);

	rc = dt_insert(env, idx, (const struct dt_rec *)nr,
		       (const struct dt_key *)nk, th, 1);
	if (rc != 0)
		GOTO(out_lock, rc);

	nodemap_inc_version(env, idx, th);
out_lock:
	dt_write_unlock(env, idx);
out:
	dt_trans_stop(env, dev, th);

	return rc;
}

static int nodemap_idx_delete(struct lu_env *env,
			      struct dt_object *idx,
			      struct nodemap_key *nk,
			      struct nodemap_rec *unused)
{
	struct thandle		*th;
	struct dt_device	*dev = dt_obj2dev(idx);
	int			 rc = 0;

	th = dt_trans_create(env, dev);

	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, idx, (const struct dt_key *)nk, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_declare_version_set(env, idx, th);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(out, rc);

	dt_write_lock(env, idx, 0);

	rc = dt_delete(env, idx, (const struct dt_key *)nk, th);

	nodemap_inc_version(env, idx, th);

	dt_write_unlock(env, idx);
out:
	dt_trans_stop(env, dev, th);

	return rc;
}

typedef int (*nm_idx_cb_t)(struct lu_env *env, struct dt_object *idx,
			       struct nodemap_key *nk, struct nodemap_rec *nr);

/**
 * Iterates through all the registered nodemap_config_files and calls the
 * given callback with the ncf as a parameter, as well as the given key and rec.
 *
 * \param	cb_f		callback function to call
 * \param	nk		key of the record to act upon
 * \param	nr		record to act upon, NULL for the delete action
 */
static int nodemap_idx_action(nm_idx_cb_t cb_f, struct nodemap_key *nk,
			      struct nodemap_rec *nr)
{
	struct nm_config_file	*ncf;
	struct lu_env		 env;
	int			 rc = 0;
	int			 errors = 0;

	rc = lu_env_init(&env, LCT_MG_THREAD);
	if (rc != 0)
		return rc;

	mutex_lock(&ncf_list_lock);
	list_for_each_entry(ncf, &ncf_list_head, ncf_list) {
		rc = cb_f(&env, ncf->ncf_obj, nk, nr);
		if (rc != 0) {
			CWARN("%s: error writing to nodemap config: rc = %d\n",
			      ncf->ncf_obj->do_lu.lo_dev->ld_obd->obd_name, rc);
			errors++;
		}
	}
	mutex_unlock(&ncf_list_lock);
	lu_env_fini(&env);

	if (errors != 0)
		return -EIO;

	return 0;
}

static int nodemap_idx_nodemap_add_update(struct lu_nodemap *nodemap,
					  bool update)
{
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	int rc = 0;

	ENTRY;

	nodemap_prep_cluster_key(&nk, nodemap->nm_id);
	nodemap_prep_cluster_rec(&nr, nodemap);

	if (update)
		rc = nodemap_idx_action(nodemap_idx_update, &nk, &nr);
	else
		rc = nodemap_idx_action(nodemap_idx_insert, &nk, &nr);

	RETURN(rc);
}

int nodemap_idx_nodemap_add(struct lu_nodemap *nodemap)
{
	return nodemap_idx_nodemap_add_update(nodemap, 0);
}

int nodemap_idx_nodemap_update(struct lu_nodemap *nodemap)
{
	return nodemap_idx_nodemap_add_update(nodemap, 1);
}

int nodemap_idx_nodemap_del(struct lu_nodemap *nodemap)
{
	struct rb_root		 root;
	struct lu_idmap		*idmap;
	struct lu_idmap		*temp;
	struct lu_nid_range	*range;
	struct lu_nid_range	*range_temp;
	struct nodemap_key	 nk;
	int			 rc = 0;
	int			 errors = 0;

	ENTRY;

	root = nodemap->nm_fs_to_client_uidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_fs_to_client) {
		nodemap_prep_idmap_key(&nk, nodemap->nm_id, NODEMAP_UID,
				       idmap->id_client);
		rc = nodemap_idx_action(nodemap_idx_delete, &nk, NULL);
		if (rc != 0)
			errors++;
	}

	root = nodemap->nm_client_to_fs_gidmap;
	nm_rbtree_postorder_for_each_entry_safe(idmap, temp, &root,
						id_client_to_fs) {
		nodemap_prep_idmap_key(&nk, nodemap->nm_id, NODEMAP_GID,
				       idmap->id_client);
		rc = nodemap_idx_action(nodemap_idx_delete, &nk, NULL);
		if (rc != 0)
			errors++;
	}

	list_for_each_entry_safe(range, range_temp, &nodemap->nm_ranges,
				 rn_list) {
		nodemap_prep_range_key(&nk, nodemap->nm_id, range->rn_id);
		rc = nodemap_idx_action(nodemap_idx_delete, &nk, NULL);
		if (rc != 0)
			errors++;
	}

	nodemap_prep_cluster_key(&nk, nodemap->nm_id);
	rc = nodemap_idx_action(nodemap_idx_delete, &nk, NULL);
	if (rc != 0)
		errors++;

	if (errors)
		RETURN(-EIO);

	RETURN(0);
}

int nodemap_idx_range_add(struct lu_nid_range *range, const lnet_nid_t nid[2])
{
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	ENTRY;

	nodemap_prep_range_key(&nk, range->rn_nodemap->nm_id, range->rn_id);
	nodemap_prep_range_rec(&nr, nid);

	RETURN(nodemap_idx_action(nodemap_idx_insert, &nk, &nr));
}

int nodemap_idx_range_del(struct lu_nid_range *range)
{
	struct nodemap_key	 nk;
	ENTRY;

	nodemap_prep_range_key(&nk, range->rn_nodemap->nm_id, range->rn_id);

	RETURN(nodemap_idx_action(nodemap_idx_delete, &nk, NULL));
}

int nodemap_idx_idmap_add(struct lu_nodemap *nodemap,
			  enum nodemap_id_type id_type,
			  const __u32 map[2])
{
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	ENTRY;

	nodemap_prep_idmap_key(&nk, nodemap->nm_id, id_type, map[0]);
	nodemap_prep_idmap_rec(&nr, map[1]);

	RETURN(nodemap_idx_action(nodemap_idx_insert, &nk, &nr));
}

int nodemap_idx_idmap_del(struct lu_nodemap *nodemap,
			  enum nodemap_id_type id_type,
			  const __u32 map[2])
{
	struct nodemap_key	 nk;
	ENTRY;

	nodemap_prep_idmap_key(&nk, nodemap->nm_id, id_type, map[0]);

	RETURN(nodemap_idx_action(nodemap_idx_delete, &nk, NULL));
}

static int nodemap_idx_global_add_update(const bool value, bool update)
{
	struct nodemap_key	 nk;
	struct nodemap_rec	 nr;
	ENTRY;

	nodemap_prep_global_key(&nk);
	nodemap_prep_global_rec(&nr, value);

	if (update)
		RETURN(nodemap_idx_action(nodemap_idx_update, &nk, &nr));
	else
		RETURN(nodemap_idx_action(nodemap_idx_insert, &nk, &nr));
}

int nodemap_idx_nodemap_activate(const bool value)
{
	return nodemap_idx_global_add_update(value, 1);
}

/**
 * Process a key/rec pair and modify the new configuration.
 *
 * \param	config		configuration to update with this key/rec data
 * \param	key		key of the record that was loaded
 * \param	rec		record that was loaded
 * \param	recent_nodemap	last referenced nodemap
 */
static int nodemap_process_keyrec(struct nodemap_config *config,
				  struct nodemap_key *key,
				  struct nodemap_rec *rec,
				  struct lu_nodemap **recent_nodemap)
{
	struct lu_nodemap	*nodemap = NULL;
	int			 type;
	__u32			 nodemap_id;
	lnet_nid_t		 nid[2];
	__u32			 map[2];
	int			 rc;

	key->nk.global = le64_to_cpu(key->nk.global);
	type = nm_idx_get_type(key->nk.ck.ck_id);
	nodemap_id = nm_idx_set_type(key->nk.ck.ck_id, 0);

	/* find the correct nodemap in the load list */
	if (type == NODEMAP_RANGE_IDX || type == NODEMAP_UIDMAP_IDX ||
			type == NODEMAP_GIDMAP_IDX) {
		struct lu_nodemap *tmp = NULL;

		nodemap = *recent_nodemap;

		if (nodemap == NULL)
			GOTO(out, rc = -EINVAL);

		if (nodemap->nm_id != nodemap_id) {
			list_for_each_entry(tmp, &nodemap->nm_list, nm_list) {
				if (tmp->nm_id != nodemap_id)
					continue;

				nodemap = tmp;
				break;
			}
			if (nodemap->nm_id != nodemap_id)
				GOTO(out, rc = -ENOENT);
		}

		/* update most recently used nodemap if necessay */
		if (nodemap != *recent_nodemap)
			*recent_nodemap = nodemap;
	}

	switch (type) {
	case NODEMAP_EMPTY_IDX:
		if (nodemap_id != 0)
			CWARN("Found nodemap config record without type field, "
			      " nodemap_id=%d. nodemap config file corrupt?\n",
			      nodemap_id);

		GOTO(out, rc = 0);
	case NODEMAP_CLUSTER_IDX:
		nodemap = cfs_hash_lookup(config->nmc_nodemap_hash,
					  rec->nr.ncr.ncr_name);
		if (nodemap == NULL) {
			if (nodemap_id == LUSTRE_NODEMAP_DEFAULT_ID) {
				nodemap = nodemap_create(rec->nr.ncr.ncr_name,
							 config, 1);
				config->nmc_default_nodemap = nodemap;
			} else {
				nodemap = nodemap_create(rec->nr.ncr.ncr_name,
							 config, 0);
			}
			if (IS_ERR(nodemap))
				GOTO(out, rc = PTR_ERR(nodemap));

			/* we need to override the local ID with the saved ID */
			nodemap->nm_id = nodemap_id;
			if (nodemap_id > config->nmc_nodemap_highest_id)
				config->nmc_nodemap_highest_id = nodemap_id;

		} else if (nodemap->nm_id != nodemap_id) {
			GOTO(out, rc = -EINVAL);
		}

		nodemap->nm_squash_uid =
				le32_to_cpu(rec->nr.ncr.ncr_squash_uid);
		nodemap->nm_squash_gid =
				le32_to_cpu(rec->nr.ncr.ncr_squash_gid);
		nodemap->nmf_allow_root_access =
				rec->nr.ncr.ncr_flags & (1 << NM_FL_ROOT);
		nodemap->nmf_trust_client_ids =
				rec->nr.ncr.ncr_flags & (1 << NM_FL_TRUST);

		if (*recent_nodemap == NULL) {
			*recent_nodemap = nodemap;
			INIT_LIST_HEAD(&nodemap->nm_list);
		} else {
			list_add(&nodemap->nm_list,
				 &(*recent_nodemap)->nm_list);
		}
		nodemap_putref(nodemap);
		break;
	case NODEMAP_RANGE_IDX:
		nid[0] = le64_to_cpu(rec->nr.nrr.nrr_start_nid);
		nid[1] = le64_to_cpu(rec->nr.nrr.nrr_end_nid);

		rc = nodemap_add_range_helper(config, nodemap, nid,
					le32_to_cpu(key->nk.rk.rk_range_id));
		break;
	case NODEMAP_UIDMAP_IDX:
		map[0] = key->nk.ik.ik_id_client;
		map[1] = le32_to_cpu(rec->nr.nir.nir_id_fs);

		rc = nodemap_add_idmap_helper(nodemap, NODEMAP_UID, map);
		break;
	case NODEMAP_GIDMAP_IDX:
		map[0] = key->nk.ik.ik_id_client;
		map[1] = le32_to_cpu(rec->nr.nir.nir_id_fs);

		rc = nodemap_add_idmap_helper(nodemap, NODEMAP_GID, map);
		break;
	case NODEMAP_GLOBAL_IDX:
		config->nmc_nodemap_active = rec->nr.ngr.ngr_is_active;
		break;
	default:
		CERROR("got keyrec pair for unknown type %d\n", type);
	}
	rc = type;

out:
	return rc;
}

static int nodemap_load_entries(struct dt_object *nodemap_idx)
{
	struct lu_env		 env;
	const struct dt_it_ops  *iops;
	struct dt_it            *it;
	struct lu_nodemap	*recent_nodemap = NULL;
	struct nodemap_config	*new_config = NULL;
	__u64			 hash = 0;
	bool			 activate_nodemap = false;
	bool			 loaded_global_idx = false;
	int			 rc = 0;

	ENTRY;

	rc = lu_env_init(&env, LCT_MG_THREAD);
	if (rc != 0)
		return rc;

	iops = &nodemap_idx->do_index_ops->dio_it;

	dt_read_lock(&env, nodemap_idx, 0);
	it = iops->init(&env, nodemap_idx, 0);
	if (IS_ERR(it))
		GOTO(out, rc = PTR_ERR(it));

	rc = iops->load(&env, it, hash);
	if (rc == 0) {
		rc = iops->next(&env, it);
		if (rc != 0)
			GOTO(out_iops, rc = 0);
	}

	/* acquires active config lock */
	new_config = nodemap_config_alloc();

	if (IS_ERR(new_config)) {
		rc = PTR_ERR(new_config);
		new_config = NULL;
		GOTO(out_lock, rc);
	}

	do {
		struct nodemap_key *key;
		struct nodemap_rec rec;

		key = (struct nodemap_key *)iops->key(&env, it);
		rc = iops->rec(&env, it, (struct dt_rec *)&rec, 0);
		if (rc != -ESTALE) {
			if (rc != 0)
				GOTO(out_lock, rc);
			rc = nodemap_process_keyrec(new_config, key, &rec,
						    &recent_nodemap);
			if (rc < 0)
				GOTO(out_lock, rc);
			if (rc == NODEMAP_GLOBAL_IDX)
				loaded_global_idx = true;
		}

		do {
			rc = iops->next(&env, it);
		} while (rc == -ESTALE);
	} while (rc == 0);

	rc = 0;
out_lock:
	if (rc != 0)
		nodemap_config_dealloc(new_config);
	else
		/* creating new default needs to be done outside dt read lock */
		activate_nodemap = true;
out_iops:
	iops->put(&env, it);
	iops->fini(&env, it);
out:
	dt_read_unlock(&env, nodemap_idx);
	lu_env_fini(&env);

	if (rc != 0)
		CWARN("%s: failed to load nodemap configuration: rc=%d\n",
		      nodemap_idx->do_lu.lo_dev->ld_obd->obd_name, rc);

	if (!activate_nodemap)
		RETURN(rc);

	if (new_config->nmc_default_nodemap == NULL) {
		/* new MGS won't have a default nm on disk, so create it here */
		new_config->nmc_default_nodemap =
			nodemap_create(DEFAULT_NODEMAP, new_config, 1);
		if (IS_ERR(new_config->nmc_default_nodemap)) {
			rc = PTR_ERR(new_config->nmc_default_nodemap);
		} else {
			rc = nodemap_idx_nodemap_add_update(
					new_config->nmc_default_nodemap, 0);
			nodemap_putref(new_config->nmc_default_nodemap);
		}
	}

	/* new nodemap config won't have a global active/inactive record */
	if (loaded_global_idx == false)
		nodemap_idx_global_add_update(false, 0);

	if (rc == 0)
		nodemap_config_set_active(new_config);

	RETURN(rc);
}

/**
 * Register a dt_object representing the config index file. This should be
 * called by targets in order to load the nodemap configuration from disk. The
 * dt_object should be created with local_index_find_or_create and the index
 * features should be enabled with do_index_try.
 *
 * \param obj	dt_object returned by local_index_find_or_create
 *
 * \retval	on success: nm_config_file handle for later deregistration
 * \retval	NULL on memory allocation failure
 */
struct nm_config_file *nm_config_file_register(struct dt_object *obj)
{
	struct nm_config_file *ncf;
	bool load_entries = false;
	ENTRY;

	OBD_ALLOC_PTR(ncf);
	if (ncf == NULL)
		RETURN(NULL);

	ncf->ncf_obj = obj;
	mutex_lock(&ncf_list_lock);

	/* if this is first config file, we load it from disk */
	if (list_empty(&ncf_list_head))
		load_entries = true;

	list_add(&ncf->ncf_list, &ncf_list_head);
	mutex_unlock(&ncf_list_lock);

	if (load_entries)
		nodemap_load_entries(obj);

	RETURN(ncf);
}
EXPORT_SYMBOL(nm_config_file_register);

/**
 * Deregister a nm_config_file. Should be called by targets during cleanup.
 *
 * \param ncf	config file to deregister
 */
void nm_config_file_deregister(struct nm_config_file *ncf)
{
	struct lu_env env;
	int rc;
	ENTRY;

	rc = lu_env_init(&env, LCT_MG_THREAD);
	if (rc != 0) {
		CWARN("error with env init while deregistering config file: "
		      "rc=%d\n", rc);
		return;
	}

	lu_object_put_nocache(&env, &ncf->ncf_obj->do_lu);
	lu_env_fini(&env);

	list_del(&ncf->ncf_list);
	OBD_FREE_PTR(ncf);

	EXIT;
}
EXPORT_SYMBOL(nm_config_file_deregister);
