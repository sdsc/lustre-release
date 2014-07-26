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
 * Copyright (c) 2014, Intel Corporation.
 */
/*
 * lustre/lfsck/lfsck_striped_dir.c
 *
 * Author: Fan, Yong <fan.yong@intel.com>
 */

/*
 * About the verification for striped directory. Some rules and assumptions:
 *
 * 1) lmv_magic: The magic may be wrong. But it is almost impossible (1/2^32
 *    probability) that a master LMV EA claims as a slave LMV EA by wrong,
 *    so we can ignore such race case and the reverse case.
 *
 * 2) lmv_master_mdt_index: The master index can be self-verified via compared
 *    with the MDT index directly. The slave stripe index can be verified via
 *    compared with the file name. Although both the name entry and the LMV EA
 *    can be wrong, but it is almost impossible that they hit the same bad data
 *    So if they match each other, then trust them. Similarly, for the shard,
 *    it stores index in both slave LMV EA and in linkEA, if the two copies
 *    match, then trust them.
 *
 * 3) lmv_hash_type: The valid hash type should be LMV_HASH_TYPE_ALL_CHARS or
 *    LMV_HASH_TYPE_FNV_1A_64. If the LFSCK instance on some slave finds that
 *    the name hash against the hash function does not match the MDT, then it
 *    will change the master LMV EA hash type as LMV_HASH_TYPE_UNKNOWN. With
 *    such hash type, the whole striped directory still can be accessed via
 *    lookup/readdir, and also support unlink, but cannot add new name entry.
 *
 * 3.1) If the master hash type is one of the valid values, then trust the
 *	master LMV EA. Because:
 *
 * 3.1.1) The master hash type is visible to the client and used by the client.
 *
 * 3.1.2) For a given name, different hash types may map the name entry to the
 *	  same MDT. So simply checking one name entry or some name entries may
 *	  cannot verify whether the hash type is correct or not.
 *
 * 3.1.3) Different shards can claim different hash types, it is not easy to
 *	  distinguish which ones are correct. Even though the master is wrong,
 *	  as the LFSCK processing, some LFSCK instance on other MDT may finds
 *	  unmatched name hash, then it will change the master hash type to
 *	  LMV_HASH_TYPE_UNKNOWN as described above. The worst case is euqal
 *	  to the case without the LFSCK.
 *
 * 3.2) If the master hash type is invalid, nor LMV_HASH_TYPE_UNKNOWN, then
 *	trust the first shard with valid hash type (ALL_CHARS or FNV_1A_64).
 *	If the shard is also worng, means there are double failures, then as
 *	the LFSCK processing, other LFSCK instances on the other MDTs may
 *	find unmatched name hash, and then, the master hash type will be
 *	changed to LMV_HASH_TYPE_UNKNOWN as described in the 3).
 *
 * 3.3) If the master hash type is LMV_HASH_TYPE_UNKNOWN, then it is possible
 *	that some other LFSCK instance on other MDT found bad name hash, then
 *	changed the master hash type to LMV_HASH_TYPE_UNKNOWN as described in
 *	the 3). But it also maybe because of data corruption in master LMV EA.
 *	To make such two cases to be distinguishable, when the LFSCK changes
 *	the master hash type to LMV_HASH_TYPE_UNKNOWN, it will mark in the
 *	master LMV EA (new lmv flags LMV_HASH_FLAG_BAD_TYPE). Then subsequent
 *	LFSCK checking can distinguish them: for former case, turst the master
 *	LMV EA with nothing to be done; otherwise, trust the first shard with
 *	valid hash type (ALL_CHARS or FNV_1A_64) as the 3.2) does.
 *
 * 4) lmv_stripe_count: For a shard of a striped directory, if its index has
 *    been verified as the 2), then the stripe count must be larger than its
 *    index. For the master object, by scanning each shard's index, the LFSCK
 *    can know the highest index, and the stripe count must be larger than the
 *    known highest index. If the stipe count in the LMV EA matches above two
 *    rules, then it is may be trustable. If both the master claimed stripe
 *    count and the slave claimed stripe count match each own rule, but they
 *    are not the same, then trust the master. Because the stripe count in
 *    the master LMV EA is visible to client and used to distribute the name
 *    entry to some shard, but the slave LMV EA is only used for verification
 *    and invisible to client.
 *
 * 5) If the master LMV EA is lost, then there are two possible cases:
 *
 * 5.1) The slave claims slave LMV EA by wrong, means that the parent was not
 *	a striped directory, but its sub-directory has a wrong slave LMV EA.
 *	It is very very race case, similar as the 1), can be ignored.
 *
 * 5.2) The parent directory is a striped directory, but the master LMV EA
 *	is lost or crashed. Then the LFSCK needs to re-generate the master
 *	LMV EA: the lmv_master_mdt_index is from the MDT device index; the
 *	lmv_hash_type is from the first valid shard; the lmv_stripe_count
 *	will be calculated via scanning all the shards.
 *
 * 5.2.1) Before re-generating the master LMV EA, the LFSCK needs to check
 *	  whether someone has created some file(s) under the master object
 *	  after the master LMV EA disappear. If yes, the LFSCK will cannot
 *	  re-generate the master LMV EA, otherwise, such new created files
 *	  will be invisible to client. Under such case, the LFSCK will mark
 *	  the master object as read only (without master LMV EA). Then all
 *	  things under the master MDT-object, including those new created
 *	  files and the shards themselves, will be visibile to client. And
 *	  then the administrator can handle the bad striped directory with
 *	  more human knowledge.
 *
 * 5.2.2) If someone created some special sub-directory under the master
 *	  MDT-object with the same naming rule as shard name $FID:$index,
 *	  as to the LFSCK cannot detect it before re-generating the master
 *	  LMV EA, then such sub-directory itself will be invisible after
 *	  the LFSCK re-generating the master LMV EA. The sub-items under
 *	  such sub-directory are still visible to client. As the LFSCK
 *	  processing, if such sub-directory cause some conflict with other
 *	  normal shard, such as the index conflict, then the LFSCK will
 *	  remove the master LMV EA and change the master MDT-object to
 *	  read-only mode as the 5.2.1). But if there is no conflict, the
 *	  LFSCK will regard such sub-directory as a striped shard that
 *	  lost its slave LMV EA, and will re-generate slave LMV EA for it.
 *
 * 5.2.3) Anytime, if the LFSCK found some shards name/index conflict,
 *	  and cannot make the distinguish which one is right, then it
 *	  will remove the master LMV EA and change the MDT-object to
 *	  read-only mode as the 5.2.2).
 */

#define DEBUG_SUBSYSTEM S_LFSCK

#include <lustre/lustre_idl.h>
#include <lu_object.h>
#include <dt_object.h>
#include <md_object.h>
#include <lustre_fid.h>
#include <lustre_lib.h>
#include <lustre_net.h>
#include <lustre/lustre_user.h>

#include "lfsck_internal.h"

void lfsck_lmv_put(const struct lu_env *env, struct lfsck_lmv *llmv)
{
	if (llmv != NULL && atomic_dec_and_test(&llmv->ll_ref)) {
		if (llmv->ll_inline) {
			struct lfsck_lmv_unit	*llu;
			struct lfsck_instance	*lfsck;

			llu = list_entry(llmv, struct lfsck_lmv_unit, llu_lmv);
			lfsck = llu->llu_lfsck;

			spin_lock(&lfsck->li_lock);
			list_del(&llu->llu_link);
			spin_unlock(&lfsck->li_lock);

			lfsck_object_put(env, llu->llu_obj);

			LASSERT(llmv->ll_lslr != NULL);

			OBD_FREE_LARGE(llmv->ll_lslr,
				       sizeof(struct lfsck_slave_lmv_rec) *
				       llmv->ll_stripes_allocated);
			OBD_FREE_PTR(llu);
		} else {
			if (llmv->ll_lslr != NULL)
				OBD_FREE_LARGE(llmv->ll_lslr,
					sizeof(struct lfsck_slave_lmv_rec) *
					llmv->ll_stripes_allocated);

			OBD_FREE_PTR(llmv);
		}
	}
}

/**
 * Mark the specified directory as read-only by set LUSTRE_IMMUTABLE_FL.
 *
 * The caller has taken the ldlm lock on the @obj already.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object to be handled
 * \param[in] del_lmv	true if need to drop the LMV EA
 *
 * \retval		positive number if nothing to be done
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
static int lfsck_disable_master_lmv(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct dt_object *obj, bool del_lmv)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_attr			*la	= &info->lti_la;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck_obj2dt_dev(obj);
	struct thandle			*th	= NULL;
	int				 rc	= 0;
	ENTRY;

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	if (del_lmv) {
		rc = dt_declare_xattr_del(env, obj, XATTR_NAME_LMV, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	la->la_valid = LA_FLAGS;
	rc = dt_declare_attr_set(env, obj, la, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock, rc = 1);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(unlock, rc = 0);

	if (del_lmv) {
		rc = dt_xattr_del(env, obj, XATTR_NAME_LMV, th, BYPASS_CAPA);
		if (rc != 0)
			GOTO(unlock, rc);
	}

	rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
	if (rc == 0 && !(la->la_flags & LUSTRE_IMMUTABLE_FL)) {
		la->la_valid = LA_FLAGS;
		la->la_flags |= LUSTRE_IMMUTABLE_FL;
		rc = dt_attr_set(env, obj, la, th, BYPASS_CAPA);
		if (rc == 0)
			lu_object_set_immutable(obj->do_lu.lo_header);
	}

	GOTO(unlock, rc);

unlock:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK set the master MDT-object of "
	       "the striped directory "DFID" as read-only: rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(lfsck_dto2fid(obj)), rc);

	if (rc <= 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		down_write(&com->lc_sem);
		ns->ln_flags |= LF_INCONSISTENT;
		if (rc == 0)
			ns->ln_striped_dirs_disabled++;
		up_write(&com->lc_sem);
	}

	return rc;
}

static inline bool lfsck_is_invalid_slave_lmv(struct lmv_mds_md_v1 *lmv)
{
	if (lmv->lmv_stripe_count < 1 ||
	    lmv->lmv_stripe_count > LFSCK_LMV_MAX_STRIPES ||
	    lmv->lmv_stripe_count <= lmv->lmv_master_mdt_index ||
	    !lmv_is_known_hash_type(lmv->lmv_hash_type))
		return true;

	return false;
}

int lfsck_read_stripe_lmv(const struct lu_env *env, struct dt_object *obj,
			  struct lmv_mds_md_v1 *lmv)
{
	int rc;

	/* The LOD will iterate all shards' FIDs when load LMV EA,
	 * but here, we only need the LMV EA header, so we use new
	 * EA name XATTR_NAME_LMV_HEADER. */
	dt_read_lock(env, obj, 0);
	rc = dt_xattr_get(env, obj, lfsck_buf_get(env, lmv, sizeof(*lmv)),
			  XATTR_NAME_LMV_HEADER, BYPASS_CAPA);
	dt_read_unlock(env, obj);
	if (rc != sizeof(struct lmv_mds_md_v1))
		return rc > 0 ? -EINVAL : rc;

	lfsck_lmv_header_le_to_cpu(lmv, lmv);
	if ((lmv->lmv_magic == LMV_MAGIC &&
	     !(lmv->lmv_hash_type & LMV_HASH_FLAG_MIGRATION)) ||
	    (lmv->lmv_magic == LMV_MAGIC_STRIPE &&
	     !(lmv->lmv_hash_type & LMV_HASH_FLAG_DEAD)))
		return 0;

	return -ENODATA;
}

/**
 * Check whether the given entry matches shard's name rule or not.
 *
 * The valid shard name/type should be:
 * 1) The type must be S_IFDIR
 * 2) The name should be $FID:$index
 * 3) the index should within valid range.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] name	the entry's name
 * \param[in] namelen	the name length
 * \param[in] type	the entry's type
 * \param[in] fid	the entry's FID
 *
 * \retval		zero or positive number for the index from the name
 * \retval		negative error number on failure
 */
int lfsck_is_valid_master_name_entry(const struct lu_env *env,
				     const char *name, int namelen,
				     __u16 type, const struct lu_fid *fid)
{
	char	*name2	= lfsck_env_info(env)->lti_tmpbuf2;
	int	 len;
	int	 idx	= 0;

	if (!S_ISDIR(type))
		return -ENOTDIR;

	LASSERT(name != name2);

	len = snprintf(name2, FID_LEN + 1, DFID":", PFID(fid));
	if (namelen < len + 1 || memcmp(name, name2, len) != 0)
		return -EINVAL;

	do {
		if (name[len] < '0' || name[len] > '9')
			return -EINVAL;

		idx = idx * 10 + name[len++] - '0';
	} while (len < namelen);

	if (idx >= LFSCK_LMV_MAX_STRIPES)
		return -EINVAL;

	return idx;
}

bool lfsck_is_valid_slave_name_entry(const struct lu_env *env,
				     struct lfsck_lmv *llmv,
				     const char *name, int namelen)
{
	struct lmv_mds_md_v1	*lmv;
	int			 idx;

	if (llmv == NULL || !llmv->ll_lmv_slave || !llmv->ll_lmv_verified)
		return true;

	lmv = &llmv->ll_lmv;
	idx = lmv_name_to_stripe_index(lmv->lmv_hash_type,
				       lmv->lmv_stripe_count,
				       name, namelen);
	if (unlikely(idx != lmv->lmv_master_mdt_index))
		return false;

	return true;
}

/**
 * Check whether the given name is a valid entry under the @parent.
 *
 * If the @parent is a striped directory, then the @child should one
 * shard of the striped directorry, its name should be $FID:$index.
 *
 * If the @parent is a shard of a striped directory, then the name hash
 * should match the MDT, otherwise it is invalid.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] parent	the parent directory
 * \param[in] child	the child object to be checked
 * \param[in] cname	the name for the @child in the parent directory
 *
 * \retval		positive number for invalid name entry
 * \retval		0 if the name is valid or uncertain
 * \retval		negative error number on failure
 */
int lfsck_namespace_check_name_validation(const struct lu_env *env,
					  struct dt_object *parent,
					  struct dt_object *child,
					  const struct lu_name *cname)
{
	struct lmv_mds_md_v1	*lmv = &lfsck_env_info(env)->lti_lmv;
	int			 idx;
	int			 rc;

	rc = lfsck_read_stripe_lmv(env, parent, lmv);
	if (rc != 0)
		RETURN(rc == -ENODATA ? 0 : rc);

	if (lmv->lmv_magic == LMV_MAGIC_STRIPE) {
		if (lfsck_is_invalid_slave_lmv(lmv))
			return 0;

		idx = lmv_name_to_stripe_index(lmv->lmv_hash_type,
					       lmv->lmv_stripe_count,
					       cname->ln_name,
					       cname->ln_namelen);
		if (unlikely(idx != lmv->lmv_master_mdt_index))
			return 1;
	} else if (lfsck_is_valid_master_name_entry(env, cname->ln_name,
			cname->ln_namelen, lfsck_object_type(child),
			lfsck_dto2fid(child)) < 0) {
		return 1;
	}

	return 0;
}

/**
 * Update the object's LMV EA with the given @lmv.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object which LMV EA will be updated
 * \param[in] lmv	pointer to buffer holding the new LMV EA
 * \param[in] locked	whether the caller has held ldlm lock on the @obj or not
 *
 * \retval		positive number for nothing to be done
 * \retval		zero if updated successfully
 * \retval		negative error number on failure
 */
int lfsck_namespace_update_lmv(const struct lu_env *env,
			       struct lfsck_component *com,
			       struct dt_object *obj,
			       struct lmv_mds_md_v1 *lmv, bool locked)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lmv_mds_md_v1		*lmv4	= &info->lti_lmv4;
	struct lu_buf			*buf	= &info->lti_buf;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck_obj2dt_dev(obj);
	struct thandle			*th	= NULL;
	struct lustre_handle		 lh	= { 0 };
	int				 rc	= 0;
	ENTRY;

	LASSERT(lmv4 != lmv);

	lfsck_lmv_header_cpu_to_le(lmv4, lmv);
	lfsck_buf_init(buf, lmv4, sizeof(*lmv4));

	if (!locked) {
		rc = lfsck_ibits_lock(env, lfsck, obj, &lh,
				      MDS_INODELOCK_UPDATE |
				      MDS_INODELOCK_XATTR, LCK_EX);
		if (rc != 0)
			GOTO(log, rc);
	}

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	rc = dt_declare_xattr_set(env, obj, buf, XATTR_NAME_LMV, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock, rc = 1);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(unlock, rc = 0);

	rc = dt_xattr_set(env, obj, buf, XATTR_NAME_LMV, 0, th, BYPASS_CAPA);

	GOTO(unlock, rc);

unlock:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

log:
	lfsck_ibits_unlock(&lh, LCK_EX);
	CDEBUG(D_LFSCK, "%s: namespace LFSCK updated the %s LMV EA "
	       "for the object "DFID": rc = %d\n",
	       lfsck_lfsck2name(lfsck),
	       lmv->lmv_magic == LMV_MAGIC ? "master" : "slave",
	       PFID(lfsck_dto2fid(obj)), rc);

	return rc;
}

/**
 * Notify remote LFSCK instance to set LMV EA for the specified object.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object on which the LMV EA will be set
 * \param[in] lmv	pointer to the buffer holding the new LMV EA
 * \param[in] cfid	the shard's FID used for verification if set master
 * \param[in] cidx	the shard's index used for verification if set master
 * \param[in] flags	to indicate which element(s) in the LMV EA will be set
 * \param[in] event	to indicate set master LMV EA or slave LMV EA
 *
 * \retval		positive number if nothing to be done
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
static int lfsck_namespace_set_lmv_remote(const struct lu_env *env,
					  struct lfsck_component *com,
					  struct dt_object *obj,
					  struct lmv_mds_md_v1 *lmv,
					  const struct lu_fid *cfid, __u32 cidx,
					  __u32 flags, __u32 event)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lfsck_request		*lr	= &info->lti_lr;
	struct lu_seq_range		*range	= &info->lti_range;
	const struct lu_fid		*fid	= lfsck_dto2fid(obj);
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct seq_server_site		*ss	=
			lu_site2seq(lfsck->li_bottom->dd_lu_dev.ld_site);
	struct lfsck_tgt_desc		*ltd	= NULL;
	struct ptlrpc_request		*req	= NULL;
	int				 rc;
	ENTRY;

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(out, rc = 0);

	fld_range_set_mdt(range);
	rc = fld_server_lookup(env, ss->ss_server_fld, fid_seq(fid), range);
	if (rc != 0)
		GOTO(out, rc);

	ltd = lfsck_tgt_get(&lfsck->li_mdt_descs, range->lsr_index);
	if (ltd == NULL)
		GOTO(out, rc = -ENODEV);

	req = ptlrpc_request_alloc(class_exp2cliimp(ltd->ltd_exp),
				   &RQF_LFSCK_NOTIFY);
	if (req == NULL)
		GOTO(out, rc = -ENOMEM);

	rc = ptlrpc_request_pack(req, LUSTRE_OBD_VERSION, LFSCK_NOTIFY);
	if (rc != 0) {
		ptlrpc_request_free(req);

		GOTO(out, rc);
	}

	lr = req_capsule_client_get(&req->rq_pill, &RMF_LFSCK_REQUEST);
	memset(lr, 0, sizeof(*lr));
	lr->lr_event = event;
	lr->lr_index = lfsck_dev_idx(lfsck->li_bottom);
	lr->lr_active = LFSCK_TYPE_NAMESPACE;
	lr->lr_fid = *fid;
	lr->lr_fid2 = *cfid;
	lr->lr_flags = flags;
	lr->lr_index2 = cidx;
	lr->lr_hash_type = lmv->lmv_hash_type;
	lr->lr_stripe_count = lmv->lmv_stripe_count;
	lr->lr_stripe_index = lmv->lmv_master_mdt_index;
	lr->lr_layout_version = lmv->lmv_layout_version;
	memcpy(lr->lr_pool_name, lmv->lmv_pool_name, LOV_MAXPOOLNAME);

	ptlrpc_request_set_replen(req);
	rc = ptlrpc_queue_wait(req);
	ptlrpc_req_finished(req);

	GOTO(out, rc = (rc == -ENOENT ? 1 : rc));

out:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK set %s LMV EA for the object "
	       DFID" on the MDT %x remotely with event %u: rc = %d\n",
	       lfsck_lfsck2name(lfsck),
	       lmv->lmv_magic == LMV_MAGIC ? "master" : "slave",
	       PFID(fid), ltd != NULL ? ltd->ltd_index : -1, event, rc);

	if (ltd != NULL)
		lfsck_tgt_put(ltd);

	return rc;
}

/**
 * Check whether there are non-shard objects under the striped directory.
 *
 * If the master MDT-object of the striped directory lost its master LMV EA,
 * then before the LFSCK repaired the striped directory, some ones may have
 * created some non-shard objects under the master MDT-object. If such case
 * happend, then the LFSCK cannot re-generate the lost master LMV EA to keep
 * those non-shard objects to be visible to client.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the master MDT-object to be checked
 * \param[in] cfid	the shard's FID used for verification
 * \param[in] cidx	the shard's index used for verification
 *
 * \retval		positive number if not allow to re-generate LMV EA
 * \retval		zero if allow to re-generate LMV EA
 * \retval		negative error number on failure
 */
static int lfsck_allow_set_master_lmv(const struct lu_env *env,
				      struct lfsck_component *com,
				      struct dt_object *obj,
				      const struct lu_fid *cfid, __u32 cidx)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_fid			*tfid	= &info->lti_fid3;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lu_dirent		*ent	=
			(struct lu_dirent *)info->lti_key;
	const struct dt_it_ops		*iops;
	struct dt_it			*di;
	__u64				 cookie;
	__u32				 args;
	int				 rc;
	__u16				 type;
	ENTRY;

	if (unlikely(!dt_try_as_dir(env, obj)))
		RETURN(-ENOTDIR);

	/* Check whether the shard and the master MDT-object matches or not. */
	snprintf(info->lti_tmpbuf, sizeof(info->lti_tmpbuf), DFID":%u",
		 PFID(cfid), cidx);
	rc = dt_lookup(env, obj, (struct dt_rec *)tfid,
		       (const struct dt_key *)info->lti_tmpbuf, BYPASS_CAPA);
	if (rc != 0)
		RETURN(rc);

	if (!lu_fid_eq(tfid, cfid))
		RETURN(-ENOENT);

	args = lfsck->li_args_dir & ~(LUDA_VERIFY | LUDA_VERIFY_DRYRUN);
	iops = &obj->do_index_ops->dio_it;
	di = iops->init(env, obj, args, BYPASS_CAPA);
	if (IS_ERR(di))
		RETURN(PTR_ERR(di));

	rc = iops->load(env, di, 0);
	if (rc == 0)
		rc = iops->next(env, di);
	else if (rc > 0)
		rc = 0;

	if (rc != 0)
		GOTO(out, rc);

	do {
		rc = iops->rec(env, di, (struct dt_rec *)ent, args);
		lfsck_unpack_ent(ent, &cookie, &type);
		if (rc != 0)
			GOTO(out, rc);

		/* skip dot and dotdot entries */
		if (name_is_dot_dotdot(ent->lde_name, ent->lde_namelen))
			goto next;

		/* If the subdir name does not match the shard name rule, then
		 * it is quite possible that it is NOT a shard, but created by
		 * someone after the master MDT-object lost the master LMV EA.
		 * But it is also possible that the subdir name entry crashed,
		 * under such double failure cases, the LFSCK cannot know how
		 * to repair the inconsistency. For data safe, the LFSCK will
		 * mark the master MDT-object as read-only. The administrator
		 * can fix the bad shard name manually, then run LFSCK again.
		 *
		 * XXX: If the subdir name matches the shard name rule, but it
		 *	is not a real shard of the striped directory, instead,
		 *	it was created by someone after the master MDT-object
		 *	lost the LMV EA, then re-generating the master LMV EA
		 *	will cause such subdir to be invisible to client, and
		 *	if its index occupies some lost shard index, then the
		 *	LFSCK will use it to replace the bad shard, and cause
		 *	the subdir (itself) to be invisible for ever. */
		if (lfsck_is_valid_master_name_entry(env, ent->lde_name,
				ent->lde_namelen, type, &ent->lde_fid) < 0)
			GOTO(out, rc = 1);

next:
		rc = iops->next(env, di);
	} while (rc == 0);

	GOTO(out, rc = 0);

out:
	iops->put(env, di);
	iops->fini(env, di);

	return rc;
}

/**
 * Set master LMV EA for the specified object locally.
 *
 * First, if the master MDT-object of a striped directory lost its LMV EA,
 * then there may be some users have created some files under the master
 * MDT-object directly. Under such case, the LFSCK cannot re-generate LMV
 * EA for the master MDT-object, because we should keep the existing files
 * to be visible to client. Then the LFSCK will mark the striped directory
 * as read-only and keep it there to be handled by administrator manually.
 *
 * If nobody has created files under the master MDT-object of the striped
 * directory, then we will set the master LMV EA and generate a new rescan
 * (the striped directory) request that will be handled later by the LFSCK
 * instance on the MDT later.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object on which the LMV EA will be set
 * \param[in] lmv	pointer to the buffer holding the new LMV EA
 * \param[in] cfid	the shard's FID used for verification
 * \param[in] cidx	the shard's index used for verification
 * \param[in] flags	to indicate which element(s) in the LMV EA will be set
 *
 * \retval		positive number if not re-genereate the master LMV EA
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
int lfsck_namespace_set_lmv_master_local(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct dt_object *obj,
					 struct lmv_mds_md_v1 *lmv,
					 const struct lu_fid *cfid,
					 __u32 cidx, __u32 flags)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lmv_mds_md_v1		*lmv3	= &info->lti_lmv3;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct lfsck_namespace		*ns	= com->lc_file_ram;
	struct lfsck_lmv_unit		*llu;
	struct lfsck_lmv		*llmv;
	struct lfsck_slave_lmv_rec	*lslr;
	struct lustre_handle		 lh	= { 0 };
	int				 count	= 0;
	int				 rc	= 0;
	ENTRY;

	rc = lfsck_ibits_lock(env, lfsck, obj, &lh,
			      MDS_INODELOCK_UPDATE | MDS_INODELOCK_XATTR,
			      LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	rc = lfsck_read_stripe_lmv(env, obj, lmv3);
	if (rc == -ENODATA) {
		if (!(flags & LEF_SET_LMV_ALL))
			GOTO(log, rc);

		memcpy(lmv3, lmv, sizeof(*lmv3));
	} else if (rc == 0) {
		if (flags & LEF_SET_LMV_ALL)
			GOTO(log, rc = 1);

		if (flags & LEF_SET_LMV_HASH)
			lmv3->lmv_hash_type = lmv->lmv_hash_type;
	} else {
		GOTO(log, rc);
	}

	lmv3->lmv_magic = LMV_MAGIC;
	lmv3->lmv_master_mdt_index = lfsck_dev_idx(dev);

	if (flags & LEF_SET_LMV_ALL) {
		rc = lfsck_allow_set_master_lmv(env, com, obj, cfid, cidx);
		if (rc > 0) {
			rc = lfsck_disable_master_lmv(env, com, obj, false);

			GOTO(log, rc = (rc == 0 ? 1 : rc));
		}

		if (rc < 0)
			GOTO(log, rc);

		/* To indicate that the master has ever lost LMV EA. */
		lmv3->lmv_hash_type |= LMV_HASH_FLAG_LOST_LMV;
	}

	rc = lfsck_namespace_update_lmv(env, com, obj, lmv3, true);
	if (rc != 0 || !(flags & LEF_SET_LMV_ALL))
		GOTO(log, rc);

	OBD_ALLOC_PTR(llu);
	if (unlikely(llu == NULL))
		GOTO(log, rc = -ENOMEM);

	if (lmv3->lmv_stripe_count < 1)
		count = LFSCK_LMV_DEF_STRIPES;
	else if (lmv3->lmv_stripe_count > LFSCK_LMV_MAX_STRIPES)
		count = LFSCK_LMV_MAX_STRIPES;
	else
		count = lmv3->lmv_stripe_count;

	OBD_ALLOC_LARGE(lslr, sizeof(struct lfsck_slave_lmv_rec) * count);
	if (lslr == NULL) {
		OBD_FREE_PTR(llu);

		GOTO(log, rc = -ENOMEM);
	}

	INIT_LIST_HEAD(&llu->llu_link);
	llu->llu_lfsck = lfsck;
	llu->llu_obj = lfsck_object_get(obj);
	llmv = &llu->llu_lmv;
	llmv->ll_lmv_master = 1;
	llmv->ll_inline = 1;
	atomic_set(&llmv->ll_ref, 1);
	llmv->ll_stripes_allocated = count;
	llmv->ll_hash_type = LMV_HASH_TYPE_UNKNOWN;
	llmv->ll_lslr = lslr;
	memcpy(&llmv->ll_lmv, lmv3, sizeof(*lmv3));

	down_write(&com->lc_sem);
	ns->ln_striped_dirs_repaired++;
	if (list_empty(&com->lc_link_dir) ||
	    ns->ln_status != LS_SCANNING_PHASE1) {
		ns->ln_striped_dirs_skipped++;
		up_write(&com->lc_sem);
		lfsck_lmv_put(env, llmv);
	} else {
		spin_lock(&lfsck->li_lock);
		list_add_tail(&llu->llu_link, &lfsck->li_list_lmv);
		spin_unlock(&lfsck->li_lock);
		up_write(&com->lc_sem);
	}

	GOTO(log, rc = 0);

log:
	lfsck_ibits_unlock(&lh, LCK_EX);
	CDEBUG(D_LFSCK, "%s: namespace LFSCK set master LMV EA for the object "
	       DFID" on the local MDT: rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(lfsck_dto2fid(obj)), rc);

	if (rc <= 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

/**
 * Set master LMV EA for the specified object.
 *
 * Ideally, we can use general dt_xattr_set() API for that, but because
 * there will other additional actions after the LMV EA updating on the
 * remote MDT. So the LFSCK will use LFSCK_NOTIFY RPC for these purpose
 * via single RPC.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object on which the LMV EA will be set
 * \param[in] lmv	pointer to the buffer holding the new LMV EA
 * \param[in] cfid	the shard's FID used for verification
 * \param[in] cidx	the shard's index used for verification
 * \param[in] flags	to indicate which element(s) in the LMV EA will be set
 *
 * \retval		positive number if nothing to be done
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
static int lfsck_namespace_set_lmv_master(const struct lu_env *env,
					  struct lfsck_component *com,
					  struct dt_object *obj,
					  struct lmv_mds_md_v1 *lmv,
					  const struct lu_fid *cfid,
					  __u32 cidx, __u32 flags)
{
	int rc;

	if (dt_object_remote(obj) != 0) {
		rc = lfsck_namespace_set_lmv_remote(env, com, obj, lmv, cfid,
						cidx, flags, LE_SET_LMV_MASTER);
	} else {
		obj = lfsck_object_find_by_dev(env, com->lc_lfsck->li_bottom,
					       lfsck_dto2fid(obj));
		if (IS_ERR(obj))
			return PTR_ERR(obj);

		rc = lfsck_namespace_set_lmv_master_local(env, com, obj, lmv,
							  cfid, cidx, flags);
		lfsck_object_put(env, obj);
	}

	return rc;
}

/**
 * Repair the bad name hash.
 *
 * If the name hash of some name entry under the striped directory does not
 * match the shard of the striped directory, then the LFSCK will repair the
 * inconsistency. Ideally, the LFSCK should migrate the name entry from the
 * current MDT to the right MDT (another one), but before the async commit
 * finished, the LFSCK will change the striped directory's hash type as
 * LMV_HASH_TYPE_UNKNOWN and mark the lmv flags as LMV_HASH_FLAG_BAD_TYPE.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the shard of the striped directory that
 *			contains the bad name entry
 * \param[in] llmv	pointer to lfsck LMV EA structure
 * \param[in] name	the name of the bad name hash
 *
 * \retval		positive number if nothing to be done
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
int lfsck_namespace_repair_bad_namehash(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *child,
					struct lfsck_lmv *llmv,
					const char *name)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_fid			*pfid	= &info->lti_fid3;
	struct lmv_mds_md_v1		*lmv2	= &info->lti_lmv2;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_object		*parent	= NULL;
	int				 rc	= 0;
	ENTRY;

	rc = dt_lookup(env, child, (struct dt_rec *)pfid,
		       (const struct dt_key *)dotdot, BYPASS_CAPA);
	if (rc != 0 || !fid_is_sane(pfid))
		GOTO(log, rc);

	parent = lfsck_object_find_bottom(env, lfsck, pfid);
	if (IS_ERR(parent))
		GOTO(log, rc = PTR_ERR(parent));

	*lmv2 = llmv->ll_lmv;
	lmv2->lmv_hash_type = LMV_HASH_TYPE_UNKNOWN | LMV_HASH_FLAG_BAD_TYPE;
	rc = lfsck_namespace_set_lmv_master(env, com, parent, lmv2,
					    lfsck_dto2fid(child),
					    llmv->ll_lmv.lmv_master_mdt_index,
					    LEF_SET_LMV_HASH);

	GOTO(log, rc);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK assistant found bad name hash"
	       "on the MDT %x, parent "DFID", name %s, shard %x: rc = %d\n",
	       lfsck_lfsck2name(lfsck), lfsck_dev_idx(lfsck->li_bottom),
	       PFID(pfid), name, llmv->ll_lmv.lmv_master_mdt_index, rc);

	if (parent != NULL && !IS_ERR(parent))
		lfsck_object_put(env, parent);

	return rc;
}

/**
 * Scan the shard of a striped directory for name hash verification.
 *
 * During the first-stage scanning, if the LFSCK cannot make sure whether
 * the shard of a stripe directory contains valid slave LMV EA or not, then
 * it will skip the name hash verification for this shard temporarily, and
 * record the shard's FID in the LFSCK tracing file. As the LFSCK processing,
 * the slave LMV EA may has been verified/fixed by LFSCK instance on master.
 * Then in the second-stage scanning, the shard will be re-scanned, and for
 * every name entry under the shard, the name hash will be verified, and for
 * unmatched name entry, the LFSCK will try to fix it.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the directory object to be handled
 *
 * \retval		positive number for scanning successfully
 * \retval		zero for the scanning is paused
 * \retval		negative error number on failure
 */
int lfsck_namespace_scan_shard(const struct lu_env *env,
			       struct lfsck_component *com,
			       struct dt_object *child)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lmv_mds_md_v1		*lmv	= &info->lti_lmv;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_namespace		*ns	= com->lc_file_ram;
	struct ptlrpc_thread		*thread = &lfsck->li_thread;
	struct lu_dirent		*ent	=
			(struct lu_dirent *)info->lti_key;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_lmv		*llmv	= NULL;
	const struct dt_it_ops		*iops;
	struct dt_it			*di;
	__u64				 cookie;
	__u32				 args;
	int				 rc;
	__u16				 type;
	ENTRY;

	rc = lfsck_read_stripe_lmv(env, child, lmv);
	if (rc != 0)
		RETURN(rc == -ENODATA ? 1 : rc);

	if (lmv->lmv_magic != LMV_MAGIC_STRIPE)
		RETURN(1);

	if (unlikely(!dt_try_as_dir(env, child)))
		RETURN(-ENOTDIR);

	OBD_ALLOC_PTR(llmv);
	if (llmv == NULL)
		RETURN(-ENOMEM);

	llmv->ll_lmv_slave = 1;
	llmv->ll_lmv_verified = 1;
	memcpy(&llmv->ll_lmv, lmv, sizeof(*lmv));
	atomic_set(&llmv->ll_ref, 1);

	args = lfsck->li_args_dir & ~(LUDA_VERIFY | LUDA_VERIFY_DRYRUN);
	iops = &child->do_index_ops->dio_it;
	di = iops->init(env, child, args, BYPASS_CAPA);
	if (IS_ERR(di))
		GOTO(out, rc = PTR_ERR(di));

	rc = iops->load(env, di, 0);
	if (rc == 0)
		rc = iops->next(env, di);
	else if (rc > 0)
		rc = 0;

	while (rc == 0) {
		if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_DELAY3) &&
		    cfs_fail_val > 0) {
			struct l_wait_info lwi;

			lwi = LWI_TIMEOUT(cfs_time_seconds(cfs_fail_val),
					  NULL, NULL);
			l_wait_event(thread->t_ctl_waitq,
				     !thread_is_running(thread),
				     &lwi);

			if (unlikely(!thread_is_running(thread)))
				GOTO(out, rc = 0);
		}

		rc = iops->rec(env, di, (struct dt_rec *)ent, args);
		lfsck_unpack_ent(ent, &cookie, &type);
		if (rc != 0) {
			if (bk->lb_param & LPF_FAILOUT)
				GOTO(out, rc);

			goto next;
		}

		/* skip dot and dotdot entries */
		if (name_is_dot_dotdot(ent->lde_name, ent->lde_namelen))
			goto next;

		if (!lfsck_is_valid_slave_name_entry(env, llmv, ent->lde_name,
						     ent->lde_namelen)) {
			ns->ln_flags |= LF_INCONSISTENT;
			rc = lfsck_namespace_repair_bad_namehash(env, com,
						child, llmv, ent->lde_name);
			if (rc >= 0) {
				down_write(&com->lc_sem);
				ns->ln_namehash_repaired++;
				up_write(&com->lc_sem);
			}
		}

		if (rc < 0 && bk->lb_param & LPF_FAILOUT)
			GOTO(out, rc);

		/* Rate control. */
		lfsck_control_speed(lfsck);
		if (unlikely(!thread_is_running(thread)))
			GOTO(out, rc = 0);

		if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_FATAL2)) {
			spin_lock(&lfsck->li_lock);
			thread_set_flags(thread, SVC_STOPPING);
			spin_unlock(&lfsck->li_lock);

			GOTO(out, rc = -EINVAL);
		}

next:
		rc = iops->next(env, di);
	}

	GOTO(out, rc);

out:
	iops->put(env, di);
	iops->fini(env, di);
	lfsck_lmv_put(env, llmv);

	return rc;
}

/**
 * Verify the slave object's (of striped directory) LMV EA.
 *
 * For the slave object of a striped directory, before traversaling the shard
 * the LFSCK will verify whether its slave LMV EA matches its parent's master
 * LMV EA or not.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the object which LMV EA will be checked
 * \param[in] llmv	pointer to buffer holding the slave LMV EA
 *
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
int lfsck_namespace_verify_stripe_slave(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *obj,
					struct lfsck_lmv *llmv)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	char				*name	= info->lti_key;
	char				*name2;
	struct lu_fid			*pfid	= &info->lti_fid3;
	struct lu_fid			*tfid	= &info->lti_fid4;
	const struct lu_fid		*cfid	= lfsck_dto2fid(obj);
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lmv_mds_md_v1		*clmv	= &llmv->ll_lmv;
	struct lmv_mds_md_v1		*plmv	= &info->lti_lmv;
	struct dt_object		*parent	= NULL;
	int				 rc	= 0;
	ENTRY;

	if (lfsck_is_invalid_slave_lmv(clmv)) {
		rc = lfsck_namespace_trace_update(env, com, cfid,
					LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc);
	}

	rc = dt_lookup(env, obj, (struct dt_rec *)pfid,
		       (const struct dt_key *)dotdot, BYPASS_CAPA);
	if (rc != 0 || !fid_is_sane(pfid)) {
		rc = lfsck_namespace_trace_update(env, com, cfid,
					LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc);
	}

	parent = lfsck_object_find(env, lfsck, pfid);
	if (IS_ERR(parent)) {
		rc = lfsck_namespace_trace_update(env, com, cfid,
					LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc);
	}

	rc = lfsck_read_stripe_lmv(env, parent, plmv);
	if (rc != 0) {
		int rc1;

		/* If the parent has no LMV EA, then it maybe because:
		 * 1) The parent lost the LMV EA.
		 * 2) The child claims a wrong (slave) LMV EA. */
		if (rc == -ENODATA)
			rc = lfsck_namespace_set_lmv_master(env, com, parent,
					clmv, cfid, clmv->lmv_master_mdt_index,
					LEF_SET_LMV_ALL);
		else
			rc = 0;

		rc1 = lfsck_namespace_trace_update(env, com, cfid,
						   LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc = (rc < 0 ? rc : rc1));
	}

	/* Unmatched magic or stripe count. */
	if (unlikely(plmv->lmv_magic != LMV_MAGIC ||
		     plmv->lmv_stripe_count != clmv->lmv_stripe_count)) {
		rc = lfsck_namespace_trace_update(env, com, cfid,
						  LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc);
	}

	/* If the master hash type has been set as LMV_HASH_TYPE_UNKNOWN,
	 * then the slave hash type is not important. */
	if ((plmv->lmv_hash_type & LMV_HASH_TYPE_MASK) ==
	    LMV_HASH_TYPE_UNKNOWN &&
	    plmv->lmv_hash_type & LMV_HASH_FLAG_BAD_TYPE)
		GOTO(out, rc = 0);

	/* Unmatched hash type. */
	if (unlikely((plmv->lmv_hash_type & LMV_HASH_TYPE_MASK) !=
		     (clmv->lmv_hash_type & LMV_HASH_TYPE_MASK))) {
		rc = lfsck_namespace_trace_update(env, com, cfid,
						  LNTF_UNCERTAION_LMV, true);

		GOTO(out, rc);
	}

	snprintf(info->lti_tmpbuf2, sizeof(info->lti_tmpbuf2), DFID":%u",
		 PFID(cfid), clmv->lmv_master_mdt_index);
	name2 = info->lti_tmpbuf2;

	rc = lfsck_links_get_first(env, obj, name, tfid);
	if (rc == 0 && strcmp(name, name2) == 0 && lu_fid_eq(pfid, tfid)) {
		llmv->ll_lmv_verified = 1;

		GOTO(out, rc);
	}

	rc = dt_lookup(env, parent, (struct dt_rec *)tfid,
		       (const struct dt_key *)name2, BYPASS_CAPA);
	if (rc != 0 || !lu_fid_eq(cfid, tfid))
		rc = lfsck_namespace_trace_update(env, com, cfid,
						  LNTF_UNCERTAION_LMV, true);
	else
		llmv->ll_lmv_verified = 1;

	GOTO(out, rc);

out:
	if (parent != NULL && !IS_ERR(parent))
		lfsck_object_put(env, parent);

	return rc;
}

/**
 * Double scan the striped directory or the shard.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] lnr	pointer to the namespace request that contains the
 *			striped directory or the shard
 *
 * \retval		zero for succeed
 * \retval		negative error number on failure
 */
int lfsck_namespace_striped_dir_scanned(const struct lu_env *env,
					struct lfsck_component *com,
					struct lfsck_namespace_req *lnr)
{
	struct lfsck_namespace		*ns	= com->lc_file_ram;
	struct lfsck_lmv		*llmv	= lnr->lnr_lmv;
	struct dt_object		*dir	= lnr->lnr_obj;
	ENTRY;

	/* XXX: it will be improved with subsequent patches landed. */

	if (llmv->ll_lmv_slave  && llmv->ll_lmv_verified) {
		down_write(&com->lc_sem);
		ns->ln_striped_shards_scanned++;
		up_write(&com->lc_sem);
		lfsck_namespace_trace_update(env, com,
					lfsck_dto2fid(dir),
					LNTF_UNCERTAION_LMV |
					LNTF_RECHECK_NAMEHASH, false);
	}

	RETURN(0);
}
