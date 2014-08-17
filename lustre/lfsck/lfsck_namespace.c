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
 * Copyright (c) 2012, 2013, Intel Corporation.
 */
/*
 * lustre/lfsck/lfsck_namespace.c
 *
 * Author: Fan, Yong <fan.yong@intel.com>
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

#define LFSCK_NAMESPACE_MAGIC	0xA0629D03

enum lfsck_nameentry_check {
	LFSCK_NAMEENTRY_DEAD		= 1, /* The object has been unlinked. */
	LFSCK_NAMEENTRY_REMOVED		= 2, /* The entry has been removed. */
	LFSCK_NAMEENTRY_RECREATED	= 3, /* The entry has been recreated. */
};

static const char lfsck_namespace_name[] = "lfsck_namespace";

static struct lfsck_namespace_req *
lfsck_namespace_assistant_req_init(struct lfsck_instance *lfsck,
				   struct lu_dirent *ent, __u16 type)
{
	struct lfsck_namespace_req *lnr;
	int			    size;

	size = sizeof(*lnr) + (ent->lde_namelen & ~3) + 4;
	OBD_ALLOC(lnr, size);
	if (lnr == NULL)
		return ERR_PTR(-ENOMEM);

	INIT_LIST_HEAD(&lnr->lnr_lar.lar_list);
	lu_object_get(&lfsck->li_obj_dir->do_lu);
	lnr->lnr_obj = lfsck->li_obj_dir;
	lnr->lnr_fid = ent->lde_fid;
	lnr->lnr_oit_cookie = lfsck->li_pos_current.lp_oit_cookie;
	lnr->lnr_dir_cookie = ent->lde_hash;
	lnr->lnr_attr = ent->lde_attrs;
	lnr->lnr_size = size;
	lnr->lnr_type = type;
	lnr->lnr_namelen = ent->lde_namelen;
	memcpy(lnr->lnr_name, ent->lde_name, ent->lde_namelen);

	return lnr;
}

static void lfsck_namespace_assistant_req_fini(const struct lu_env *env,
					       struct lfsck_assistant_req *lar)
{
	struct lfsck_namespace_req *lnr =
			container_of0(lar, struct lfsck_namespace_req, lnr_lar);

	lu_object_put(env, &lnr->lnr_obj->do_lu);
	OBD_FREE(lnr, lnr->lnr_size);
}

static void lfsck_namespace_le_to_cpu(struct lfsck_namespace *dst,
				      struct lfsck_namespace *src)
{
	dst->ln_magic = le32_to_cpu(src->ln_magic);
	dst->ln_status = le32_to_cpu(src->ln_status);
	dst->ln_flags = le32_to_cpu(src->ln_flags);
	dst->ln_success_count = le32_to_cpu(src->ln_success_count);
	dst->ln_run_time_phase1 = le32_to_cpu(src->ln_run_time_phase1);
	dst->ln_run_time_phase2 = le32_to_cpu(src->ln_run_time_phase2);
	dst->ln_time_last_complete = le64_to_cpu(src->ln_time_last_complete);
	dst->ln_time_latest_start = le64_to_cpu(src->ln_time_latest_start);
	dst->ln_time_last_checkpoint =
				le64_to_cpu(src->ln_time_last_checkpoint);
	lfsck_position_le_to_cpu(&dst->ln_pos_latest_start,
				 &src->ln_pos_latest_start);
	lfsck_position_le_to_cpu(&dst->ln_pos_last_checkpoint,
				 &src->ln_pos_last_checkpoint);
	lfsck_position_le_to_cpu(&dst->ln_pos_first_inconsistent,
				 &src->ln_pos_first_inconsistent);
	dst->ln_items_checked = le64_to_cpu(src->ln_items_checked);
	dst->ln_items_repaired = le64_to_cpu(src->ln_items_repaired);
	dst->ln_items_failed = le64_to_cpu(src->ln_items_failed);
	dst->ln_dirs_checked = le64_to_cpu(src->ln_dirs_checked);
	dst->ln_objs_checked_phase2 = le64_to_cpu(src->ln_objs_checked_phase2);
	dst->ln_objs_repaired_phase2 =
				le64_to_cpu(src->ln_objs_repaired_phase2);
	dst->ln_objs_failed_phase2 = le64_to_cpu(src->ln_objs_failed_phase2);
	dst->ln_objs_nlink_repaired = le64_to_cpu(src->ln_objs_nlink_repaired);
	dst->ln_objs_lost_found = le64_to_cpu(src->ln_objs_lost_found);
	fid_le_to_cpu(&dst->ln_fid_latest_scanned_phase2,
		      &src->ln_fid_latest_scanned_phase2);
	dst->ln_dirent_repaired = le64_to_cpu(src->ln_dirent_repaired);
	dst->ln_linkea_repaired = le64_to_cpu(src->ln_linkea_repaired);
	dst->ln_mul_linked_checked = le64_to_cpu(src->ln_mul_linked_checked);
	dst->ln_mul_linked_repaired = le64_to_cpu(src->ln_mul_linked_repaired);
	dst->ln_unknown_inconsistency =
				le64_to_cpu(src->ln_unknown_inconsistency);
	dst->ln_unmatched_pairs_repaired =
				le64_to_cpu(src->ln_unmatched_pairs_repaired);
	dst->ln_dangling_repaired = le64_to_cpu(src->ln_dangling_repaired);
	dst->ln_mul_ref_repaired = le64_to_cpu(src->ln_mul_ref_repaired);
}

static void lfsck_namespace_cpu_to_le(struct lfsck_namespace *dst,
				      struct lfsck_namespace *src)
{
	dst->ln_magic = cpu_to_le32(src->ln_magic);
	dst->ln_status = cpu_to_le32(src->ln_status);
	dst->ln_flags = cpu_to_le32(src->ln_flags);
	dst->ln_success_count = cpu_to_le32(src->ln_success_count);
	dst->ln_run_time_phase1 = cpu_to_le32(src->ln_run_time_phase1);
	dst->ln_run_time_phase2 = cpu_to_le32(src->ln_run_time_phase2);
	dst->ln_time_last_complete = cpu_to_le64(src->ln_time_last_complete);
	dst->ln_time_latest_start = cpu_to_le64(src->ln_time_latest_start);
	dst->ln_time_last_checkpoint =
				cpu_to_le64(src->ln_time_last_checkpoint);
	lfsck_position_cpu_to_le(&dst->ln_pos_latest_start,
				 &src->ln_pos_latest_start);
	lfsck_position_cpu_to_le(&dst->ln_pos_last_checkpoint,
				 &src->ln_pos_last_checkpoint);
	lfsck_position_cpu_to_le(&dst->ln_pos_first_inconsistent,
				 &src->ln_pos_first_inconsistent);
	dst->ln_items_checked = cpu_to_le64(src->ln_items_checked);
	dst->ln_items_repaired = cpu_to_le64(src->ln_items_repaired);
	dst->ln_items_failed = cpu_to_le64(src->ln_items_failed);
	dst->ln_dirs_checked = cpu_to_le64(src->ln_dirs_checked);
	dst->ln_objs_checked_phase2 = cpu_to_le64(src->ln_objs_checked_phase2);
	dst->ln_objs_repaired_phase2 =
				cpu_to_le64(src->ln_objs_repaired_phase2);
	dst->ln_objs_failed_phase2 = cpu_to_le64(src->ln_objs_failed_phase2);
	dst->ln_objs_nlink_repaired = cpu_to_le64(src->ln_objs_nlink_repaired);
	dst->ln_objs_lost_found = cpu_to_le64(src->ln_objs_lost_found);
	fid_cpu_to_le(&dst->ln_fid_latest_scanned_phase2,
		      &src->ln_fid_latest_scanned_phase2);
	dst->ln_dirent_repaired = cpu_to_le64(src->ln_dirent_repaired);
	dst->ln_linkea_repaired = cpu_to_le64(src->ln_linkea_repaired);
	dst->ln_mul_linked_checked = cpu_to_le64(src->ln_mul_linked_checked);
	dst->ln_mul_linked_repaired = cpu_to_le64(src->ln_mul_linked_repaired);
	dst->ln_unknown_inconsistency =
				cpu_to_le64(src->ln_unknown_inconsistency);
	dst->ln_unmatched_pairs_repaired =
				cpu_to_le64(src->ln_unmatched_pairs_repaired);
	dst->ln_dangling_repaired = cpu_to_le64(src->ln_dangling_repaired);
	dst->ln_mul_ref_repaired = cpu_to_le64(src->ln_mul_ref_repaired);
}

static void lfsck_namespace_record_failure(const struct lu_env *env,
					   struct lfsck_instance *lfsck,
					   struct lfsck_namespace *ns)
{
	struct lfsck_position pos;

	ns->ln_items_failed++;
	lfsck_pos_fill(env, lfsck, &pos, false);
	if (lfsck_pos_is_zero(&ns->ln_pos_first_inconsistent) ||
	    lfsck_pos_is_eq(&pos, &ns->ln_pos_first_inconsistent) < 0) {
		ns->ln_pos_first_inconsistent = pos;

		CDEBUG(D_LFSCK, "%s: namespace LFSCK hit first non-repaired "
		       "inconsistency at the pos ["LPU64", "DFID", "LPX64"]\n",
		       lfsck_lfsck2name(lfsck),
		       ns->ln_pos_first_inconsistent.lp_oit_cookie,
		       PFID(&ns->ln_pos_first_inconsistent.lp_dir_parent),
		       ns->ln_pos_first_inconsistent.lp_dir_cookie);
	}
}

/**
 * \retval +ve: the lfsck_namespace is broken, the caller should reset it.
 * \retval 0: succeed.
 * \retval -ve: failed cases.
 */
static int lfsck_namespace_load(const struct lu_env *env,
				struct lfsck_component *com)
{
	int len = com->lc_file_size;
	int rc;

	rc = dt_xattr_get(env, com->lc_obj,
			  lfsck_buf_get(env, com->lc_file_disk, len),
			  XATTR_NAME_LFSCK_NAMESPACE, BYPASS_CAPA);
	if (rc == len) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		lfsck_namespace_le_to_cpu(ns,
				(struct lfsck_namespace *)com->lc_file_disk);
		if (ns->ln_magic != LFSCK_NAMESPACE_MAGIC) {
			CDEBUG(D_LFSCK, "%s: invalid lfsck_namespace magic "
			       "%#x != %#x\n", lfsck_lfsck2name(com->lc_lfsck),
			       ns->ln_magic, LFSCK_NAMESPACE_MAGIC);
			rc = 1;
		} else {
			rc = 0;
		}
	} else if (rc != -ENODATA) {
		CDEBUG(D_LFSCK, "%s: fail to load lfsck_namespace, "
		       "expected = %d: rc = %d\n",
		       lfsck_lfsck2name(com->lc_lfsck), len, rc);
		if (rc >= 0)
			rc = 1;
	}
	return rc;
}

static int lfsck_namespace_store(const struct lu_env *env,
				 struct lfsck_component *com, bool init)
{
	struct dt_object	*obj    = com->lc_obj;
	struct lfsck_instance	*lfsck  = com->lc_lfsck;
	struct thandle		*handle;
	int			 len    = com->lc_file_size;
	int			 rc;
	ENTRY;

	lfsck_namespace_cpu_to_le((struct lfsck_namespace *)com->lc_file_disk,
				  (struct lfsck_namespace *)com->lc_file_ram);
	handle = dt_trans_create(env, lfsck->li_bottom);
	if (IS_ERR(handle))
		GOTO(log, rc = PTR_ERR(handle));

	rc = dt_declare_xattr_set(env, obj,
				  lfsck_buf_get(env, com->lc_file_disk, len),
				  XATTR_NAME_LFSCK_NAMESPACE, 0, handle);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_trans_start_local(env, lfsck->li_bottom, handle);
	if (rc != 0)
		GOTO(out, rc);

	rc = dt_xattr_set(env, obj,
			  lfsck_buf_get(env, com->lc_file_disk, len),
			  XATTR_NAME_LFSCK_NAMESPACE,
			  init ? LU_XATTR_CREATE : LU_XATTR_REPLACE,
			  handle, BYPASS_CAPA);

	GOTO(out, rc);

out:
	dt_trans_stop(env, lfsck->li_bottom, handle);

log:
	if (rc != 0)
		CDEBUG(D_LFSCK, "%s: fail to store lfsck_namespace: rc = %d\n",
		       lfsck_lfsck2name(lfsck), rc);
	return rc;
}

static int lfsck_namespace_init(const struct lu_env *env,
				struct lfsck_component *com)
{
	struct lfsck_namespace *ns = com->lc_file_ram;
	int rc;

	memset(ns, 0, sizeof(*ns));
	ns->ln_magic = LFSCK_NAMESPACE_MAGIC;
	ns->ln_status = LS_INIT;
	down_write(&com->lc_sem);
	rc = lfsck_namespace_store(env, com, true);
	up_write(&com->lc_sem);
	return rc;
}

/**
 * Update the namespace LFSCK tracing file for the given @fid
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] fid	the fid which flags to be updated in the lfsck
 *			tracing file
 * \param[in] add	true if add new flags, otherwise remove flags
 *
 * \retval		0 for succeed or nothing to be done
 * \retval		negative error number on failure
 */
int lfsck_namespace_trace_update(const struct lu_env *env,
				 struct lfsck_component *com,
				 const struct lu_fid *fid,
				 const __u8 flags, bool add)
{
	struct lfsck_instance	*lfsck  = com->lc_lfsck;
	struct dt_object	*obj    = com->lc_obj;
	struct lu_fid		*key    = &lfsck_env_info(env)->lti_fid3;
	struct dt_device	*dev	= lfsck->li_bottom;
	struct thandle		*th	= NULL;
	int			 rc	= 0;
	__u8			 old	= 0;
	__u8			 new	= 0;
	ENTRY;

	LASSERT(flags != 0);

	down_write(&com->lc_sem);
	fid_cpu_to_be(key, fid);
	rc = dt_lookup(env, obj, (struct dt_rec *)&old,
		       (const struct dt_key *)key, BYPASS_CAPA);
	if (rc == -ENOENT) {
		if (!add)
			GOTO(unlock, rc = 0);

		old = 0;
		new = flags;
	} else if (rc == 0) {
		if (add) {
			if ((old & flags) == flags)
				GOTO(unlock, rc = 0);

			new = old | flags;
		} else {
			if ((old & flags) == 0)
				GOTO(unlock, rc = 0);

			new = old & ~flags;
		}
	} else {
		GOTO(log, rc);
	}

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	if (old != 0) {
		rc = dt_declare_delete(env, obj,
				       (const struct dt_key *)key, th);
		if (rc != 0)
			GOTO(log, rc);
	}

	if (new != 0) {
		rc = dt_declare_insert(env, obj,
				       (const struct dt_rec *)&new,
				       (const struct dt_key *)key, th);
		if (rc != 0)
			GOTO(log, rc);
	}

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(log, rc);

	if (old != 0) {
		rc = dt_delete(env, obj, (const struct dt_key *)key,
			       th, BYPASS_CAPA);
		if (rc != 0)
			GOTO(log, rc);
	}

	if (new != 0) {
		rc = dt_insert(env, obj, (const struct dt_rec *)&new,
			       (const struct dt_key *)key, th, BYPASS_CAPA, 1);
		if (rc != 0)
			GOTO(log, rc);
	}

	GOTO(log, rc);

log:
	if (th != NULL && !IS_ERR(th))
		dt_trans_stop(env, dev, th);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK %s flags for "DFID" in the "
	       "tracing file, flags %x, old %x, new %x: rc = %d\n",
	       lfsck_lfsck2name(lfsck), add ? "add" : "del", PFID(fid),
	       (__u32)flags, (__u32)old, (__u32)new, rc);

unlock:
	up_write(&com->lc_sem);

	return rc;
}

static int lfsck_namespace_check_exist(const struct lu_env *env,
				       struct dt_object *dir,
				       struct dt_object *obj, const char *name)
{
	struct lu_fid	 *fid = &lfsck_env_info(env)->lti_fid;
	int		  rc;
	ENTRY;

	if (unlikely(lfsck_is_dead_obj(obj)))
		RETURN(LFSCK_NAMEENTRY_DEAD);

	rc = dt_lookup(env, dir, (struct dt_rec *)fid,
		       (const struct dt_key *)name, BYPASS_CAPA);
	if (rc == -ENOENT)
		RETURN(LFSCK_NAMEENTRY_REMOVED);

	if (rc < 0)
		RETURN(rc);

	if (!lu_fid_eq(fid, lfsck_dto2fid(obj)))
		RETURN(LFSCK_NAMEENTRY_RECREATED);

	RETURN(0);
}

static int lfsck_declare_namespace_exec_dir(const struct lu_env *env,
					    struct dt_object *obj,
					    struct thandle *handle)
{
	int rc;

	/* For destroying all invalid linkEA entries. */
	rc = dt_declare_xattr_del(env, obj, XATTR_NAME_LINK, handle);
	if (rc != 0)
		return rc;

	/* For insert new linkEA entry. */
	rc = dt_declare_xattr_set(env, obj,
			lfsck_buf_get_const(env, NULL, DEFAULT_LINKEA_SIZE),
			XATTR_NAME_LINK, 0, handle);
	return rc;
}

int __lfsck_links_read(const struct lu_env *env, struct dt_object *obj,
		       struct linkea_data *ldata)
{
	int rc;

	if (ldata->ld_buf->lb_buf == NULL)
		return -ENOMEM;

	if (!dt_object_exists(obj))
		return -ENOENT;

	rc = dt_xattr_get(env, obj, ldata->ld_buf, XATTR_NAME_LINK, BYPASS_CAPA);
	if (rc == -ERANGE) {
		/* Buf was too small, figure out what we need. */
		rc = dt_xattr_get(env, obj, &LU_BUF_NULL, XATTR_NAME_LINK,
				  BYPASS_CAPA);
		if (rc <= 0)
			return rc;

		lu_buf_realloc(ldata->ld_buf, rc);
		if (ldata->ld_buf->lb_buf == NULL)
			return -ENOMEM;

		rc = dt_xattr_get(env, obj, ldata->ld_buf, XATTR_NAME_LINK,
				  BYPASS_CAPA);
	}

	if (rc > 0)
		rc = linkea_init(ldata);

	return rc;
}

/**
 * Remove linkEA for the given object.
 *
 * The caller should take the ldlm lock before the calling.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the dt_object to be handled
 *
 * \retval		0 for repaired cases
 * \retval		negative error number on failure
 */
static int lfsck_namespace_links_remove(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *obj)
{
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct thandle			*th	= NULL;
	int				 rc	= 0;
	ENTRY;

	LASSERT(dt_object_remote(obj) == 0);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	rc = dt_declare_xattr_del(env, obj, XATTR_NAME_LINK, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock, rc = -ENOENT);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(unlock, rc = 0);

	rc = dt_xattr_del(env, obj, XATTR_NAME_LINK, th, BYPASS_CAPA);

	GOTO(unlock, rc);

unlock:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK remove invalid linkEA "
	       "for the object "DFID": rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(lfsck_dto2fid(obj)), rc);

	if (rc == 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

static int lfsck_links_write(const struct lu_env *env, struct dt_object *obj,
			     struct linkea_data *ldata, struct thandle *handle)
{
	const struct lu_buf *buf = lfsck_buf_get_const(env,
						       ldata->ld_buf->lb_buf,
						       ldata->ld_leh->leh_len);

	return dt_xattr_set(env, obj, buf, XATTR_NAME_LINK, 0, handle,
			    BYPASS_CAPA);
}

static void lfsck_namespace_unpack_linkea_entry(struct linkea_data *ldata,
						struct lu_name *cname,
						struct lu_fid *pfid,
						char *buf)
{
	linkea_entry_unpack(ldata->ld_lee, &ldata->ld_reclen, cname, pfid);
	/* To guarantee the 'name' is terminated with '0'. */
	memcpy(buf, cname->ln_name, cname->ln_namelen);
	buf[cname->ln_namelen] = 0;
	cname->ln_name = buf;
}

static int lfsck_namespace_filter_linkea_entry(struct linkea_data *ldata,
					       struct lu_name *cname,
					       struct lu_fid *pfid,
					       bool remove)
{
	struct link_ea_entry	*oldlee;
	int			 oldlen;
	int			 repeated = 0;

	oldlee = ldata->ld_lee;
	oldlen = ldata->ld_reclen;
	linkea_next_entry(ldata);
	while (ldata->ld_lee != NULL) {
		ldata->ld_reclen = (ldata->ld_lee->lee_reclen[0] << 8) |
				   ldata->ld_lee->lee_reclen[1];
		if (unlikely(ldata->ld_reclen == oldlen &&
			     memcmp(ldata->ld_lee, oldlee, oldlen) == 0)) {
			repeated++;
			if (!remove)
				break;

			linkea_del_buf(ldata, cname);
		} else {
			linkea_next_entry(ldata);
		}
	}
	ldata->ld_lee = oldlee;
	ldata->ld_reclen = oldlen;

	return repeated;
}

/**
 * Insert orphan into .lustre/lost+found/MDTxxxx/ locally.
 *
 * Add the specified orphan MDT-object to the .lustre/lost+found/MDTxxxx/
 * with the given type to generate the name, the detailed rules for name
 * have been described as following.
 *
 * The function also generates the linkEA corresponding to the name entry
 * under the .lustre/lost+found/MDTxxxx/ for the orphan MDT-object.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] orphan	pointer to the orphan MDT-object
 * \param[in] infix	additional information for the orphan name, such as
 *			the FID for original
 * \param[in] type	the type for describing why the orphan MDT-object is
 *			created. The rules are as following:
 *
 *  type "D":		The MDT-object is a directory, it may knows its parent
 *			but because there is no valid linkEA, the LFSCK cannot
 *			know where to put it back to the namespace.
 *  type "O":		The MDT-object has no linkEA, and there is no name
 *			entry that references the MDT-object.
 *
 * \see lfsck_layout_recreate_parent() for more types.
 *
 * The orphan name will be like:
 * ${FID}-${infix}-${type}-${conflict_version}
 *
 * \param[out] count	if some others inserted some linkEA entries by race,
 *			then return the linkEA entries count.
 *
 * \retval		positive number for repaired cases
 * \retval		0 if needs to repair nothing
 * \retval		negative error number on failure
 */
static int lfsck_namespace_insert_orphan(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct dt_object *orphan,
					 const char *infix, const char *type,
					 int *count)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_name			*cname	= &info->lti_name;
	struct dt_insert_rec		*rec	= &info->lti_dt_rec;
	struct lu_fid			*tfid	= &info->lti_fid5;
	const struct lu_fid		*cfid	= lfsck_dto2fid(orphan);
	const struct lu_fid		*pfid;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct dt_object		*parent;
	struct thandle			*th	= NULL;
	struct lustre_handle		 plh	= { 0 };
	struct lustre_handle		 clh	= { 0 };
	struct linkea_data		 ldata	= { 0 };
	struct lu_buf			 linkea_buf;
	int				 namelen;
	int				 idx	= 0;
	int				 rc	= 0;
	bool				 exist	= false;
	ENTRY;

	cname->ln_name = NULL;
	/* Create .lustre/lost+found/MDTxxxx when needed. */
	if (unlikely(lfsck->li_lpf_obj == NULL)) {
		rc = lfsck_create_lpf(env, lfsck);
		if (rc != 0)
			GOTO(log, rc);
	}

	parent = lfsck->li_lpf_obj;
	pfid = lfsck_dto2fid(parent);

	/* Hold update lock on the parent to prevent others to access. */
	rc = lfsck_ibits_lock(env, lfsck, parent, &plh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	do {
		namelen = snprintf(info->lti_key, NAME_MAX, DFID"%s-%s-%d",
				   PFID(cfid), infix, type, idx++);
		rc = dt_lookup(env, parent, (struct dt_rec *)tfid,
			       (const struct dt_key *)info->lti_key,
			       BYPASS_CAPA);
		if (rc != 0 && rc != -ENOENT)
			GOTO(log, rc);

		if (unlikely(rc == 0 && lu_fid_eq(cfid, tfid)))
			exist = true;
	} while (rc == 0 && !exist);

	cname->ln_name = info->lti_key;
	cname->ln_namelen = namelen;
	rc = linkea_data_new(&ldata, &info->lti_linkea_buf2);
	if (rc != 0)
		GOTO(log, rc);

	rc = linkea_add_buf(&ldata, cname, pfid);
	if (rc != 0)
		GOTO(log, rc);

	rc = lfsck_ibits_lock(env, lfsck, orphan, &clh,
			      MDS_INODELOCK_UPDATE | MDS_INODELOCK_LOOKUP,
			      LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	lfsck_buf_init(&linkea_buf, ldata.ld_buf->lb_buf,
		       ldata.ld_leh->leh_len);
	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	if (S_ISDIR(lfsck_object_type(orphan))) {
		rc = dt_declare_delete(env, orphan,
				       (const struct dt_key *)dotdot, th);
		if (rc != 0)
			GOTO(stop, rc);

		rec->rec_type = S_IFDIR;
		rec->rec_fid = pfid;
		rc = dt_declare_insert(env, orphan, (const struct dt_rec *)rec,
				       (const struct dt_key *)dotdot, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	rc = dt_declare_xattr_set(env, orphan, &linkea_buf,
				  XATTR_NAME_LINK, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	if (!exist) {
		rec->rec_type = lfsck_object_type(orphan) & S_IFMT;
		rec->rec_fid = cfid;
		rc = dt_declare_insert(env, parent, (const struct dt_rec *)rec,
				       (const struct dt_key *)cname->ln_name,
				       th);
		if (rc != 0)
			GOTO(stop, rc);

		if (S_ISDIR(rec->rec_type)) {
			rc = dt_declare_ref_add(env, parent, th);
			if (rc != 0)
				GOTO(stop, rc);
		}
	}

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, orphan, 0);
	rc = lfsck_links_read(env, orphan, &ldata);
	if (likely((rc == -ENODATA) || (rc == -EINVAL) ||
		   (rc == 0 && ldata.ld_leh->leh_reccount == 0))) {
		if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
			GOTO(unlock, rc = 1);

		if (S_ISDIR(lfsck_object_type(orphan))) {
			rc = dt_delete(env, orphan,
				       (const struct dt_key *)dotdot, th,
				       BYPASS_CAPA);
			if (rc != 0)
				GOTO(unlock, rc);

			rec->rec_type = S_IFDIR;
			rec->rec_fid = pfid;
			rc = dt_insert(env, orphan, (const struct dt_rec *)rec,
				       (const struct dt_key *)dotdot, th,
				       BYPASS_CAPA, 1);
			if (rc != 0)
				GOTO(unlock, rc);
		}

		rc = dt_xattr_set(env, orphan, &linkea_buf, XATTR_NAME_LINK, 0,
				  th, BYPASS_CAPA);
	} else {
		if (rc == 0 && count != NULL)
			*count = ldata.ld_leh->leh_reccount;

		GOTO(unlock, rc);
	}
	dt_write_unlock(env, orphan);

	if (rc == 0 && !exist) {
		rec->rec_type = lfsck_object_type(orphan) & S_IFMT;
		rec->rec_fid = cfid;
		rc = dt_insert(env, parent, (const struct dt_rec *)rec,
			       (const struct dt_key *)cname->ln_name,
			       th, BYPASS_CAPA, 1);
		if (rc == 0 && S_ISDIR(rec->rec_type)) {
			dt_write_lock(env, parent, 0);
			rc = dt_ref_add(env, parent, th);
			dt_write_unlock(env, parent);
		}
	}

	GOTO(stop, rc = (rc == 0 ? 1 : rc));

unlock:
	dt_write_unlock(env, orphan);

stop:
	dt_trans_stop(env, dev, th);

log:
	lfsck_ibits_unlock(&clh, LCK_EX);
	lfsck_ibits_unlock(&plh, LCK_EX);
	CDEBUG(D_LFSCK, "%s: namespace LFSCK insert orphan for the "
	       "object "DFID", name = %s: rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(cfid),
	       cname->ln_name != NULL ? cname->ln_name : "<NULL>", rc);

	if (rc != 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

static int lfsck_namespace_insert_normal(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct dt_object *parent,
					 struct dt_object *child,
					 const char *name)
{
	/* XXX: TBD */
	return 0;
}

static int lfsck_namespace_create_orphan(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct dt_object *orphan)
{
	/* XXX: TBD */
	return 0;
}

/**
 * Remove the specified entry from the linkEA.
 *
 * Locate the linkEA entry with the given @cname and @pfid, then
 * remove this entry or the other entries those are repeated with
 * this entry.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the dt_object to be handled
 * \param[in,out]ldata  pointer to the buffer that holds the linkEA
 * \param[in] cname	the name for the child in the parent directory
 * \param[in] pfid	the parent directory's FID for the linkEA
 * \param[in] next	if true, then remove the first found linkEA
 *			entry, and move the ldata->ld_lee to next entry
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_shrink_linkea(const struct lu_env *env,
					 struct lfsck_component *com,
					 struct dt_object *obj,
					 struct linkea_data *ldata,
					 struct lu_name *cname,
					 struct lu_fid *pfid,
					 bool next)
{
	struct lfsck_instance		*lfsck	   = com->lc_lfsck;
	struct dt_device		*dev	   = lfsck->li_bottom;
	struct lfsck_bookmark		*bk	   = &lfsck->li_bookmark_ram;
	struct thandle			*th	   = NULL;
	struct lustre_handle		 lh	   = { 0 };
	struct linkea_data		 ldata_new = { 0 };
	struct lu_buf			 linkea_buf;
	int				 rc	   = 0;
	ENTRY;

	rc = lfsck_ibits_lock(env, lfsck, obj, &lh,
			      MDS_INODELOCK_UPDATE |
			      MDS_INODELOCK_XATTR, LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	if (next)
		linkea_del_buf(ldata, cname);
	else
		lfsck_namespace_filter_linkea_entry(ldata, cname, pfid,
						    true);
	lfsck_buf_init(&linkea_buf, ldata->ld_buf->lb_buf,
		       ldata->ld_leh->leh_len);

again:
	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(unlock1, rc = PTR_ERR(th));

	rc = dt_declare_xattr_set(env, obj, &linkea_buf,
				  XATTR_NAME_LINK, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock2, rc = -ENOENT);

	rc = lfsck_links_read2(env, obj, &ldata_new);
	if (rc != 0)
		GOTO(unlock2, rc);

	/* The specified linkEA entry has been removed by race. */
	rc = linkea_links_find(&ldata_new, cname, pfid);
	if (rc != 0)
		GOTO(unlock2, rc = 0);

	if (bk->lb_param & LPF_DRYRUN)
		GOTO(unlock2, rc = 1);

	if (next)
		linkea_del_buf(&ldata_new, cname);
	else
		lfsck_namespace_filter_linkea_entry(&ldata_new, cname, pfid,
						    true);

	if (linkea_buf.lb_len < ldata_new.ld_leh->leh_len) {
		dt_write_unlock(env, obj);
		dt_trans_stop(env, dev, th);
		lfsck_buf_init(&linkea_buf, ldata_new.ld_buf->lb_buf,
			       ldata_new.ld_leh->leh_len);
		goto again;
	}

	lfsck_buf_init(&linkea_buf, ldata_new.ld_buf->lb_buf,
		       ldata_new.ld_leh->leh_len);
	rc = dt_xattr_set(env, obj, &linkea_buf,
			  XATTR_NAME_LINK, 0, th, BYPASS_CAPA);

	GOTO(unlock2, rc = (rc == 0 ? 1 : rc));

unlock2:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

unlock1:
	lfsck_ibits_unlock(&lh, LCK_EX);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK remove %s linkEA entry "
	       "for the object: "DFID", parent "DFID", name %.*s\n",
	       lfsck_lfsck2name(lfsck), next ? "invalid" : "redundant",
	       PFID(lfsck_dto2fid(obj)), PFID(pfid), cname->ln_namelen,
	       cname->ln_name);

	if (rc != 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

/**
 * Conditionally remove the specified entry from the linkEA.
 *
 * Take the parent lock firstly, then check whether the specified
 * name entry exists or not: if yes, do nothing; otherwise, call
 * lfsck_namespace_shrink_linkea() to remove the linkea entry.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] parent	pointer to the parent directory
 * \param[in] child	pointer to the child object that holds the linkEA
 * \param[in,out]ldata  pointer to the buffer that holds the linkEA
 * \param[in] cname	the name for the child in the parent directory
 * \param[in] pfid	the parent directory's FID for the linkEA
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_shrink_linkea_cond(const struct lu_env *env,
					      struct lfsck_component *com,
					      struct dt_object *parent,
					      struct dt_object *child,
					      struct linkea_data *ldata,
					      struct lu_name *cname,
					      struct lu_fid *pfid)
{
	struct lu_fid		*cfid	= &lfsck_env_info(env)->lti_fid3;
	struct lustre_handle	 lh	= { 0 };
	int			 rc;
	ENTRY;

	rc = lfsck_ibits_lock(env, com->lc_lfsck, parent, &lh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		RETURN(rc);

	dt_read_lock(env, parent, 0);
	if (unlikely(lfsck_is_dead_obj(parent))) {
		dt_read_unlock(env, parent);
		lfsck_ibits_unlock(&lh, LCK_EX);
		rc = lfsck_namespace_shrink_linkea(env, com, child, ldata,
						   cname, pfid, true);

		RETURN(rc);
	}

	rc = dt_lookup(env, parent, (struct dt_rec *)cfid,
		       (const struct dt_key *)cname->ln_name,
		       BYPASS_CAPA);
	dt_read_unlock(env, parent);

	/* It is safe to release the ldlm lock, because when the logic come
	 * here, we have got all the needed information above whether the
	 * linkEA entry is valid or not. It is not important that others
	 * may add new linkEA entry after the ldlm lock released. If other
	 * has removed the specified linkEA entry by race, then it is OK,
	 * because the subsequent lfsck_namespace_shrink_linkea() can handle
	 * such case. */
	lfsck_ibits_unlock(&lh, LCK_EX);
	if (rc == -ENOENT) {
		rc = lfsck_namespace_shrink_linkea(env, com, child, ldata,
						   cname, pfid, true);

		RETURN(rc);
	}

	if (rc != 0)
		RETURN(rc);

	/* The LFSCK just found some internal status of cross-MDTs
	 * create operation. That is normal. */
	if (lu_fid_eq(cfid, lfsck_dto2fid(child))) {
		linkea_next_entry(ldata);

		RETURN(0);
	}

	rc = lfsck_namespace_shrink_linkea(env, com, child, ldata, cname,
					   pfid, true);

	RETURN(rc);
}

/**
 * Conditionally replace name entry in the parent.
 *
 * As required, the LFSCK may re-create the lost MDT-object for dangling
 * name entry, but such repairing may be wrong because of bad FID in the
 * name entry. As the LFSCK processing, the real MDT-object may be found,
 * then the LFSCK should check whether the former re-created MDT-object
 * has been modified or not, if not, then destroy it and update the name
 * entry in the parent to reference the real MDT-object.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] parent	pointer to the parent directory
 * \param[in] child	pointer to the MDT-object that may be the real
 *			MDT-object corresponding to the name entry in parent
 * \param[in] cfid	the current FID in the name entry
 * \param[in] cname	contains the name of the child in the parent directory
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_replace_cond(const struct lu_env *env,
					struct lfsck_component *com,
					struct dt_object *parent,
					struct dt_object *child,
					const struct lu_fid *cfid,
					const struct lu_name *cname)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_fid			*tfid	= &info->lti_fid5;
	struct lu_attr			*la	= &info->lti_la;
	struct dt_insert_rec		*rec	= &info->lti_dt_rec;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_next;
	const char			*name	= cname->ln_name;
	struct dt_object		*obj	= NULL;
	struct lustre_handle		 plh	= { 0 };
	struct lustre_handle		 clh	= { 0 };
	struct linkea_data		 ldata	= { 0 };
	struct thandle			*th	= NULL;
	bool				 exist	= true;
	int				 rc	= 0;
	ENTRY;

	rc = lfsck_ibits_lock(env, lfsck, parent, &plh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	if (!fid_is_sane(cfid)) {
		exist = false;
		goto replace;
	}

	obj = lfsck_object_find(env, lfsck, cfid);
	if (IS_ERR(obj)) {
		rc = PTR_ERR(obj);
		if (rc == -ENOENT) {
			exist = false;
			goto replace;
		}

		GOTO(log, rc);
	}

	if (!dt_object_exists(obj)) {
		exist = false;
		goto replace;
	}

	rc = dt_lookup(env, parent, (struct dt_rec *)tfid,
		       (const struct dt_key *)name, BYPASS_CAPA);
	if (rc == -ENOENT) {
		exist = false;
		goto replace;
	}

	if (rc != 0)
		GOTO(log, rc);

	/* Someone changed the name entry, cannot replace it. */
	if (!lu_fid_eq(cfid, tfid))
		GOTO(log, rc = 0);

	/* lock the object to be destroyed. */
	rc = lfsck_ibits_lock(env, lfsck, obj, &clh,
			      MDS_INODELOCK_UPDATE |
			      MDS_INODELOCK_XATTR, LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	if (unlikely(lfsck_is_dead_obj(obj))) {
		exist = false;
		goto replace;
	}

	rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
	if (rc != 0)
		GOTO(log, rc);

	/* The object has been modified by other(s), or it is not created by
	 * LFSCK, the two cases are indistinguishable. So cannot replace it. */
	if (la->la_ctime != 0)
		GOTO(log, rc);

	if (S_ISREG(la->la_mode)) {
		rc = dt_xattr_get(env, obj, &LU_BUF_NULL, XATTR_NAME_LOV,
				  BYPASS_CAPA);
		/* If someone has created related OST-object(s),
		 * then keep it. */
		if ((rc > 0) || (rc < 0 && rc != -ENODATA))
			GOTO(log, rc = (rc > 0 ? 0 : rc));
	}

replace:
	dt_read_lock(env, child, 0);
	rc = lfsck_links_read2(env, child, &ldata);
	dt_read_unlock(env, child);

	/* Someone changed the child, no need to replace. */
	if (rc == -ENODATA)
		GOTO(log, rc = 0);

	if (rc != 0)
		GOTO(log, rc);

	rc = linkea_links_find(&ldata, cname, lfsck_dto2fid(parent));
	/* Someone moved the child, no need to replace. */
	if (rc != 0)
		GOTO(log, rc = 0);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(log, rc = 1);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	if (exist) {
		rc = dt_declare_destroy(env, obj, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	rc = dt_declare_delete(env, parent, (const struct dt_key *)name, th);
	if (rc != 0)
		GOTO(stop, rc);

	rec->rec_type = S_IFDIR;
	rec->rec_fid = lfsck_dto2fid(child);
	rc = dt_declare_insert(env, parent, (const struct dt_rec *)rec,
			       (const struct dt_key *)name, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	if (exist) {
		rc = dt_destroy(env, obj, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	/* The old name entry maybe not exist. */
	dt_delete(env, parent, (const struct dt_key *)name, th,
		  BYPASS_CAPA);

	rc = dt_insert(env, parent, (const struct dt_rec *)rec,
		       (const struct dt_key *)name, th, BYPASS_CAPA, 1);

	GOTO(stop, rc = (rc == 0 ? 1 : rc));

stop:
	dt_trans_stop(env, dev, th);

log:
	lfsck_ibits_unlock(&clh, LCK_EX);
	lfsck_ibits_unlock(&plh, LCK_EX);
	if (obj != NULL && !IS_ERR(obj))
		lfsck_object_put(env, obj);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK conditionally destroy the "
	       "object "DFID" because of conflict with the object "DFID
	       " under the parent "DFID" with name %s: rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(cfid),
	       PFID(lfsck_dto2fid(child)), PFID(lfsck_dto2fid(parent)),
	       name, rc);

	return rc;
}

/**
 * Overwrite the linkEA for the object with the given ldata.
 *
 * The caller should take the ldlm lock before the calling.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the dt_object to be handled
 * \param[in] ldata	pointer to the new linkEA data
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
int lfsck_namespace_rebuild_linkea(const struct lu_env *env,
				   struct lfsck_component *com,
				   struct dt_object *obj,
				   struct linkea_data *ldata)
{
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct thandle			*th	= NULL;
	struct lu_buf			 linkea_buf;
	int				 rc	= 0;
	ENTRY;

	LASSERT(!dt_object_remote(obj));

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	lfsck_buf_init(&linkea_buf, ldata->ld_buf->lb_buf,
		       ldata->ld_leh->leh_len);
	rc = dt_declare_xattr_set(env, obj, &linkea_buf,
				  XATTR_NAME_LINK, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock, rc = 0);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(unlock, rc = 1);

	rc = dt_xattr_set(env, obj, &linkea_buf,
			  XATTR_NAME_LINK, 0, th, BYPASS_CAPA);

	GOTO(unlock, rc = (rc == 0 ? 1 : rc));

unlock:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK rebuild linkEA for the "
	       "object "DFID": rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(lfsck_dto2fid(obj)), rc);

	if (rc != 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

/**
 * Update the ".." name entry for the given object.
 *
 * The object's ".." is corrupted, this function will update the ".." name
 * entry with the given pfid, and the linkEA with the given ldata.
 *
 * The caller should take the ldlm lock before the calling.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the dt_object to be handled
 * \param[in] pfid	the new fid for the object's ".." name entry
 * \param[in] cname	the name for the @obj in the parent directory
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_repair_unmatched_pairs(const struct lu_env *env,
						  struct lfsck_component *com,
						  struct dt_object *obj,
						  const struct lu_fid *pfid,
						  struct lu_name *cname)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct dt_insert_rec		*rec	= &info->lti_dt_rec;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct dt_device		*dev	= lfsck->li_bottom;
	struct thandle			*th	= NULL;
	struct linkea_data		 ldata	= { 0 };
	struct lu_buf			 linkea_buf;
	int				 rc	= 0;
	ENTRY;

	LASSERT(!dt_object_remote(obj));
	LASSERT(S_ISDIR(lfsck_object_type(obj)));

	rc = linkea_data_new(&ldata, &info->lti_big_buf);
	if (rc != 0)
		GOTO(log, rc);

	rc = linkea_add_buf(&ldata, cname, pfid);
	if (rc != 0)
		GOTO(log, rc);

	lfsck_buf_init(&linkea_buf, ldata.ld_buf->lb_buf,
		       ldata.ld_leh->leh_len);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, obj, (const struct dt_key *)dotdot, th);
	if (rc != 0)
		GOTO(stop, rc);

	rec->rec_type = S_IFDIR;
	rec->rec_fid = pfid;
	rc = dt_declare_insert(env, obj, (const struct dt_rec *)rec,
			       (const struct dt_key *)dotdot, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_declare_xattr_set(env, obj, &linkea_buf,
				  XATTR_NAME_LINK, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	if (unlikely(lfsck_is_dead_obj(obj)))
		GOTO(unlock, rc = 0);

	if (lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN)
		GOTO(unlock, rc = 1);

	/* The old ".." name entry maybe not exist. */
	dt_delete(env, obj, (const struct dt_key *)dotdot, th,
		  BYPASS_CAPA);

	rc = dt_insert(env, obj, (const struct dt_rec *)rec,
		       (const struct dt_key *)dotdot, th, BYPASS_CAPA, 1);
	if (rc != 0)
		GOTO(unlock, rc);

	rc = dt_xattr_set(env, obj, &linkea_buf,
			  XATTR_NAME_LINK, 0, th, BYPASS_CAPA);

	GOTO(unlock, rc = (rc == 0 ? 1 : rc));

unlock:
	dt_write_unlock(env, obj);

stop:
	dt_trans_stop(env, dev, th);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK rebuild dotdot name entry for "
	       "the object "DFID", new parent "DFID": rc = %d\n",
	       lfsck_lfsck2name(lfsck), PFID(lfsck_dto2fid(obj)),
	       PFID(pfid), rc);

	if (rc != 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

/**
 * Handle orphan @obj during Double Scan Directory.
 *
 * Remove the @obj's current (invalid) linkEA entries, and insert
 * it in the directory .lustre/lost+found/MDTxxxx/ with the name:
 * ${FID}-${PFID}-D-${conflict_version}
 *
 * The caller should take the ldlm lock before the calling.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] obj	pointer to the orphan object to be handled
 * \param[in] pfid	the new fid for the object's ".." name entry
 * \param[in,out] lh	ldlm lock handler for the given @obj
 * \param[out] type	to tell the caller what the inconsistency is
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int
lfsck_namespace_dsd_orphan(const struct lu_env *env,
			   struct lfsck_component *com,
			   struct dt_object *obj,
			   const struct lu_fid *pfid,
			   struct lustre_handle *lh,
			   enum lfsck_namespace_inconsistency_type *type)
{
	struct lfsck_thread_info *info = lfsck_env_info(env);
	int			  rc;
	ENTRY;

	/* Remove the unrecognized linkEA. */
	rc = lfsck_namespace_links_remove(env, com, obj);
	lfsck_ibits_unlock(lh, LCK_EX);
	if (rc < 0 && rc != -ENODATA)
		RETURN(rc);

	*type = LNIT_MUL_REF;
	/* The unique linkEA is invalid, even if the ".." name entry may be
	 * valid, we still cannot know via which name entry this directory
	 * will be referenced. Then handle it as pure orphan. */
	snprintf(info->lti_tmpbuf, sizeof(info->lti_tmpbuf),
		 "-"DFID, PFID(pfid));
	rc = lfsck_namespace_insert_orphan(env, com, obj,
					   info->lti_tmpbuf, "D", NULL);

	RETURN(rc);
}

/**
 * Double Scan Directory object for single linkEA entry case.
 *
 * The given @child has unique linkEA entry. If the linkEA entry is valid,
 * then check whether the name is in the namespace or not, if not, add the
 * missing name entry back to namespace. If the linkEA entry is invalid,
 * then remove it and insert the @child in the .lustre/lost+found/MDTxxxx/
 * as an orphan.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the directory to be double scanned
 * \param[in] pfid	the FID corresponding to the ".." entry
 * \param[in] ldata	pointer to the linkEA data for the given @child
 * \param[in,out] lh	ldlm lock handler for the given @child
 * \param[out] type	to tell the caller what the inconsistency is
 * \param[in] retry	if found inconsistency, but the caller does not hold
 *			ldlm lock on the @child, then set @retry as true
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int
lfsck_namespace_dsd_single(const struct lu_env *env,
			   struct lfsck_component *com,
			   struct dt_object *child,
			   const struct lu_fid *pfid,
			   struct linkea_data *ldata,
			   struct lustre_handle *lh,
			   enum lfsck_namespace_inconsistency_type *type,
			   bool *retry)
{
	struct lfsck_thread_info *info		= lfsck_env_info(env);
	struct lu_name		 *cname		= &info->lti_name;
	const struct lu_fid	 *cfid		= lfsck_dto2fid(child);
	struct lu_fid		 *tfid		= &info->lti_fid3;
	struct lfsck_instance	 *lfsck		= com->lc_lfsck;
	struct dt_object	 *parent	= NULL;
	int			  rc		= 0;
	ENTRY;

	lfsck_namespace_unpack_linkea_entry(ldata, cname, tfid, info->lti_key);
	/* The unique linkEA entry with bad parent will be handled as orphan. */
	if (!fid_is_sane(tfid)) {
		if (!lustre_handle_is_used(lh) && retry != NULL)
			*retry = true;
		else
			rc = lfsck_namespace_dsd_orphan(env, com, child,
							pfid, lh, type);

		GOTO(out, rc);
	}

	parent = lfsck_object_find_bottom(env, lfsck, tfid);
	if (IS_ERR(parent))
		GOTO(out, rc = PTR_ERR(parent));

	/* We trust the unique linkEA entry in spite of whether it matches the
	 * ".." name entry or not. Because even if the linkEA entry is wrong
	 * and the ".." name entry is right, we still cannot know via which
	 * name entry the child will be referenced, since all known entries
	 * have been verified during the first-stage scanning. */
	if (!dt_object_exists(parent)) {
		if (!lustre_handle_is_used(lh) && retry != NULL) {
			*retry = true;

			GOTO(out, rc = 0);
		}

		lfsck_ibits_unlock(lh, LCK_EX);
		/* Create the lost parent as an orphan. */
		rc = lfsck_namespace_create_orphan(env, com, parent);
		if (rc >= 0)
			/* Add the missing name entry to the parent. */
			rc = lfsck_namespace_insert_normal(env, com, parent,
							child, cname->ln_name);

		GOTO(out, rc);
	}

	/* The unique linkEA entry with bad parent will be handled as orphan. */
	if (unlikely(!dt_try_as_dir(env, parent))) {
		if (!lustre_handle_is_used(lh) && retry != NULL)
			*retry = true;
		else
			rc = lfsck_namespace_dsd_orphan(env, com, child,
							pfid, lh, type);

		GOTO(out, rc);
	}

	rc = dt_lookup(env, parent, (struct dt_rec *)tfid,
		       (const struct dt_key *)cname->ln_name, BYPASS_CAPA);
	if (rc == -ENOENT) {
		if (!lustre_handle_is_used(lh) && retry != NULL) {
			*retry = true;

			GOTO(out, rc = 0);
		}

		lfsck_ibits_unlock(lh, LCK_EX);
		/* Add the missing name entry back to the namespace. */
		rc = lfsck_namespace_insert_normal(env, com, parent, child,
						   cname->ln_name);

		GOTO(out, rc);
	}

	if (rc != 0)
		GOTO(out, rc);

	if (!lu_fid_eq(tfid, cfid)) {
		if (!lustre_handle_is_used(lh) && retry != NULL) {
			*retry = true;

			GOTO(out, rc = 0);
		}

		lfsck_ibits_unlock(lh, LCK_EX);
		/* The name entry references another MDT-object that
		 * may be created by the LFSCK for repairing dangling
		 * name entry. Try to replace it. */
		rc = lfsck_namespace_replace_cond(env, com, parent, child,
						  tfid, cname);
		if (rc == 0)
			rc = lfsck_namespace_dsd_orphan(env, com, child,
							pfid, lh, type);

		GOTO(out, rc);
	}

	/* The ".." name entry is wrong, update it. */
	if (!lu_fid_eq(pfid, lfsck_dto2fid(parent))) {
		if (!lustre_handle_is_used(lh) && retry != NULL) {
			*retry = true;

			GOTO(out, rc = 0);
		}

		*type = LNIT_UNMATCHED_PAIRS;
		rc = lfsck_namespace_repair_unmatched_pairs(env, com, child,
						lfsck_dto2fid(parent), cname);
	}

	GOTO(out, rc);

out:
	if (parent != NULL && !IS_ERR(parent))
		lfsck_object_put(env, parent);

	return rc;
}

/**
 * Double Scan Directory object for multiple linkEA entries case.
 *
 * The given @child has multiple linkEA entries. There is at most one linkEA
 * entry will be valid, all the others will be removed. Firstly, the function
 * will try to find out the linkEA entry for which the name entry exists under
 * the given parent (@pfid). If there is no linkEA entry that matches the given
 * ".." name entry, then tries to find out the first linkEA entry that both the
 * parent and the name entry exist to rebuild a new ".." name entry.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the directory to be double scanned
 * \param[in] pfid	the FID corresponding to the ".." entry
 * \param[in] ldata	pointer to the linkEA data for the given @child
 * \param[in,out] lh	ldlm lock handler for the given @child
 * \param[out] type	to tell the caller what the inconsistency is
 * \param[in] lpf	true if the ".." entry is under lost+found/MDTxxxx/
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int
lfsck_namespace_dsd_multiple(const struct lu_env *env,
			     struct lfsck_component *com,
			     struct dt_object *child,
			     const struct lu_fid *pfid,
			     struct linkea_data *ldata,
			     struct lustre_handle *lh,
			     enum lfsck_namespace_inconsistency_type *type,
			     bool lpf)
{
	struct lfsck_thread_info *info		= lfsck_env_info(env);
	struct lu_name		 *cname		= &info->lti_name;
	const struct lu_fid	 *cfid		= lfsck_dto2fid(child);
	struct lu_fid		 *tfid		= &info->lti_fid3;
	struct lu_fid		 *pfid2		= &info->lti_fid4;
	struct lfsck_instance	 *lfsck		= com->lc_lfsck;
	struct dt_object	 *parent	= NULL;
	struct linkea_data	  ldata_new	= { 0 };
	int			  rc		= 0;
	bool			  once		= true;
	ENTRY;

again:
	while (ldata->ld_lee != NULL) {
		lfsck_namespace_unpack_linkea_entry(ldata, cname, tfid,
						    info->lti_key);
		/* Drop repeated linkEA entries. */
		lfsck_namespace_filter_linkea_entry(ldata, cname, tfid, true);
		/* Drop invalid linkEA entry. */
		if (!fid_is_sane(tfid)) {
			linkea_del_buf(ldata, cname);
			continue;
		}

		/* If current dotdot is the .lustre/lost+found/MDTxxxx/,
		 * then it is possible that: the directry object has ever
		 * been lost, but its name entry was there. In the former
		 * LFSCK run, during the first-stage scanning, the LFSCK
		 * found the dangling name entry, but it did not recreate
		 * the lost object, and when moved to the second-stage
		 * scanning, some children objects of the lost directory
		 * object were found, then the LFSCK recreated such lost
		 * directory object as an orphan.
		 *
		 * When the LFSCK runs again, if the dangling name is still
		 * there, the LFSCK should move the orphan directory object
		 * back to the normal namespace. */
		if (!lpf && !lu_fid_eq(pfid, tfid) && once) {
			linkea_next_entry(ldata);
			continue;
		}

		parent = lfsck_object_find_bottom(env, lfsck, tfid);
		if (IS_ERR(parent))
			RETURN(PTR_ERR(parent));

		if (!dt_object_exists(parent)) {
			lfsck_object_put(env, parent);
			if (ldata->ld_leh->leh_reccount > 1) {
				/* If it is NOT the last linkEA entry, then
				 * there is still other chance to make the
				 * child to be visible via other parent, then
				 * remove this linkEA entry. */
				linkea_del_buf(ldata, cname);
				continue;
			}

			break;
		}

		/* The linkEA entry with bad parent will be removed. */
		if (unlikely(!dt_try_as_dir(env, parent))) {
			lfsck_object_put(env, parent);
			linkea_del_buf(ldata, cname);
			continue;
		}

		rc = dt_lookup(env, parent, (struct dt_rec *)tfid,
			       (const struct dt_key *)cname->ln_name,
			       BYPASS_CAPA);
		*pfid2 = *lfsck_dto2fid(parent);
		if (rc == -ENOENT) {
			lfsck_object_put(env, parent);
			linkea_next_entry(ldata);
			continue;
		}

		if (rc != 0) {
			lfsck_object_put(env, parent);

			RETURN(rc);
		}

		if (lu_fid_eq(tfid, cfid)) {
			lfsck_object_put(env, parent);
			if (!lu_fid_eq(pfid, pfid2)) {
				*type = LNIT_UNMATCHED_PAIRS;
				rc = lfsck_namespace_repair_unmatched_pairs(env,
						com, child, pfid2, cname);

				RETURN(rc);
			}

rebuild:
			/* It is the most common case that we find the
			 * name entry corresponding to the linkEA entry
			 * that matches the ".." name entry. */
			rc = linkea_data_new(&ldata_new, &info->lti_big_buf);
			if (rc != 0)
				RETURN(rc);

			rc = linkea_add_buf(&ldata_new, cname, pfid2);
			if (rc != 0)
				RETURN(rc);

			rc = lfsck_namespace_rebuild_linkea(env, com, child,
							    &ldata_new);

			/* XXX: there will be other patch. */

			RETURN(rc);
		}

		lfsck_ibits_unlock(lh, LCK_EX);
		/* The name entry references another MDT-object that may be
		 * created by the LFSCK for repairing dangling name entry.
		 * Try to replace it. */
		rc = lfsck_namespace_replace_cond(env, com, parent, child,
						  tfid, cname);
		lfsck_object_put(env, parent);
		if (rc < 0)
			RETURN(rc);

		if (rc > 0)
			goto rebuild;

		linkea_del_buf(ldata, cname);
	}

	if (ldata->ld_leh->leh_reccount == 1) {
		rc = lfsck_namespace_dsd_single(env, com, child, pfid, ldata,
						lh, type, NULL);

		RETURN(rc);
	}

	/* All linkEA entries are invalid and removed, then handle the @child
	 * as an orphan.*/
	if (ldata->ld_leh->leh_reccount == 0) {
		rc = lfsck_namespace_dsd_orphan(env, com, child, pfid, lh,
						type);

		RETURN(rc);
	}

	linkea_first_entry(ldata);
	/* If the dangling name entry for the orphan directory object has
	 * been remvoed, then just check whether the directory object is
	 * still under the .lustre/lost+found/MDTxxxx/ or not. */
	if (lpf) {
		lpf = false;
		goto again;
	}

	/* There is no linkEA entry that matches the ".." name entry. Find
	 * the first linkEA entry that both parent and name entry exist to
	 * rebuild a new ".." name entry. */
	if (once) {
		once = false;
		goto again;
	}

	RETURN(rc);
}

/**
 * Double scan the directory object for namespace LFSCK.
 *
 * This function will verify the <parent, child> pairs in the namespace tree:
 * the parent references the child via some name entry that should be in the
 * child's linkEA entry, the child should back references the parent via its
 * ".." name entry.
 *
 * The LFSCK will scan every linkEA entry in turn until find out the first
 * matched pairs. If found, then all other linkEA entries will be dropped.
 * If all the linkEA entries cannot match the ".." name entry, then there
 * are serveral possible cases:
 *
 * 1) If there is only one linkEA entry, then trust it as long as the PFID
 *    in the linkEA entry is valid.
 *
 * 2) If there are multiple linkEA entries, then try to find the linkEA
 *    that matches the ".." name entry. If found, then all other entries
 *    are invalid; otherwise, it is quite possible that the ".." name entry
 *    is corrupted. Under such case, the LFSCK will rebuild the ".." name
 *    entry according to the first valid linkEA entry (both the parent and
 *    the name entry should exist).
 *
 * 3) If the directory object has no (valid) linkEA entry, then the
 *    directory object will be handled as pure orphan and inserted
 *    in the .lustre/lost+found/MDTxxxx/ with the name:
 *    ${self_FID}-${PFID}-D-${conflict_version}
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the directory object to be handled
 * \param[in] flags	to indicate the specical checking on the @child
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_double_scan_dir(const struct lu_env *env,
					   struct lfsck_component *com,
					   struct dt_object *child, __u8 flags)
{
	struct lfsck_thread_info *info		= lfsck_env_info(env);
	const struct lu_fid	 *cfid		= lfsck_dto2fid(child);
	struct lu_fid		 *pfid		= &info->lti_fid2;
	struct lfsck_namespace	 *ns		= com->lc_file_ram;
	struct lfsck_instance	 *lfsck		= com->lc_lfsck;
	struct lustre_handle	  lh		= { 0 };
	struct linkea_data	  ldata		= { 0 };
	bool			  unknown	= false;
	bool			  lpf		= false;
	bool			  retry		= false;
	enum lfsck_namespace_inconsistency_type type = LNIT_BAD_LINKEA;
	int			  rc		= 0;
	ENTRY;

	LASSERT(!dt_object_remote(child));

	if (!(lfsck->li_bookmark_ram.lb_param & LPF_ALL_TGT)) {
		CDEBUG(D_LFSCK, "%s: some MDT(s) maybe NOT take part in the"
		       "the namespace LFSCK, then the LFSCK cannot guarantee"
		       "all the name entries have been verified in first-stage"
		       "scanning. So have to skip orphan related handling for"
		       "the directory object "DFID" with remote name entry\n",
		       lfsck_lfsck2name(lfsck), PFID(cfid));

		RETURN(0);
	}

	if (unlikely(!dt_try_as_dir(env, child)))
		GOTO(out, rc = -ENOTDIR);

	/* We only take ldlm lock on the @child when required. When the
	 * logic comes here for the first time, it is always false. */
	if (0) {

lock:
		rc = lfsck_ibits_lock(env, lfsck, child, &lh,
				      MDS_INODELOCK_UPDATE |
				      MDS_INODELOCK_XATTR, LCK_EX);
		if (rc != 0)
			GOTO(out, rc);
	}

	dt_read_lock(env, child, 0);
	if (unlikely(lfsck_is_dead_obj(child))) {
		dt_read_unlock(env, child);

		GOTO(out, rc = 0);
	}

	rc = dt_lookup(env, child, (struct dt_rec *)pfid,
		       (const struct dt_key *)dotdot, BYPASS_CAPA);
	if (rc != 0) {
		if (rc != -ENOENT && rc != -ENODATA && rc != -EINVAL) {
			dt_read_unlock(env, child);

			GOTO(out, rc);
		}

		if (!lustre_handle_is_used(&lh)) {
			dt_read_unlock(env, child);
			goto lock;
		}

		fid_zero(pfid);
	} else if (lfsck->li_lpf_obj != NULL &&
		   lu_fid_eq(pfid, lfsck_dto2fid(lfsck->li_lpf_obj))) {
		lpf = true;
	}

	rc = lfsck_links_read(env, child, &ldata);
	dt_read_unlock(env, child);
	if (rc != 0) {
		if (rc != -ENODATA && rc != -EINVAL)
			GOTO(out, rc);

		if (!lustre_handle_is_used(&lh))
			goto lock;

		if (rc == -EINVAL && !fid_is_zero(pfid)) {
			/* Remove the corrupted linkEA. */
			rc = lfsck_namespace_links_remove(env, com, child);
			if (rc == 0)
				/* Here, because of the crashed linkEA, we
				 * cannot know whether there is some parent
				 * that references the child directory via
				 * some name entry or not. So keep it there,
				 * when the LFSCK run next time, if there is
				 * some parent that references this object,
				 * then the LFSCK can rebuild the linkEA;
				 * otherwise, this object will be handled
				 * as orphan as above. */
				unknown = true;
		} else {
			/* 1. If we have neither ".." nor linkEA,
			 *    then it is an orphan.
			 *
			 * 2. If we only have the ".." name entry,
			 *    but no parent references this child
			 *    directory, then handle it as orphan. */
			lfsck_ibits_unlock(&lh, LCK_EX);
			type = LNIT_MUL_REF;
			snprintf(info->lti_tmpbuf, sizeof(info->lti_tmpbuf),
				 "-"DFID, PFID(pfid));
			rc = lfsck_namespace_insert_orphan(env, com, child,
						info->lti_tmpbuf, "D", NULL);
		}

		GOTO(out, rc);
	}

	linkea_first_entry(&ldata);
	/* This is the most common case: the object has unique linkEA entry. */
	if (ldata.ld_leh->leh_reccount == 1) {
		rc = lfsck_namespace_dsd_single(env, com, child, pfid, &ldata,
						&lh, &type, &retry);
		if (retry) {
			LASSERT(!lustre_handle_is_used(&lh));

			retry = false;
			goto lock;
		}

		GOTO(out, rc);
	}

	if (!lustre_handle_is_used(&lh))
		goto lock;

	if (unlikely(ldata.ld_leh->leh_reccount == 0)) {
		rc = lfsck_namespace_dsd_orphan(env, com, child, pfid, &lh,
						&type);

		GOTO(out, rc);
	}

	/* When we come here, the cases usually like that:
	 * 1) The directory object has a corrupted linkEA entry. During the
	 *    first-stage scanning, the LFSCK cannot know such corruption,
	 *    then it appends the right linkEA entry according to the found
	 *    name entry after the bad one.
	 *
	 * 2) The directory object has a right linkEA entry. During the
	 *    first-stage scanning, the LFSCK finds some bad name entry,
	 *    but the LFSCK cannot aware that at that time, then it adds
	 *    the bad linkEA entry for further processing. */
	rc = lfsck_namespace_dsd_multiple(env, com, child, pfid, &ldata,
					  &lh, &type, lpf);

	GOTO(out, rc);

out:
	lfsck_ibits_unlock(&lh, LCK_EX);
	if (rc > 0) {
		switch (type) {
		case LNIT_BAD_LINKEA:
			ns->ln_linkea_repaired++;
			break;
		case LNIT_UNMATCHED_PAIRS:
			ns->ln_unmatched_pairs_repaired++;
			break;
		case LNIT_MUL_REF:
			ns->ln_mul_ref_repaired++;
			break;
		default:
			break;
		}
	}

	if (unknown)
		ns->ln_unknown_inconsistency++;

	return rc;
}

/**
 * Double scan the MDT-object for namespace LFSCK.
 *
 * If the MDT-object contains invalid or repeated linkEA entries, then drop
 * those entries from the linkEA; if the linkEA becomes empty or the object
 * has no linkEA, then it is an orphan and will be added into the directory
 * .lustre/lost+found/MDTxxxx/; if the remote parent is lost, then recreate
 * the remote parent; if the name entry corresponding to some linkEA entry
 * is lost, then add the name entry back to the namespace.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the dt_object to be handled
 * \param[in] flags	some hints to indicate how the @child should be handled
 *
 * \retval		positive number for repaired cases
 * \retval		0 if nothing to be repaired
 * \retval		negative error number on failure
 */
static int lfsck_namespace_double_scan_one(const struct lu_env *env,
					   struct lfsck_component *com,
					   struct dt_object *child, __u8 flags)
{
	struct lfsck_thread_info *info	   = lfsck_env_info(env);
	struct lu_attr		 *la	   = &info->lti_la;
	struct lu_name		 *cname	   = &info->lti_name;
	struct lu_fid		 *pfid	   = &info->lti_fid;
	struct lu_fid		 *cfid	   = &info->lti_fid2;
	struct lfsck_instance	 *lfsck	   = com->lc_lfsck;
	struct lfsck_namespace	 *ns	   = com->lc_file_ram;
	struct dt_object	 *parent   = NULL;
	struct linkea_data	  ldata	   = { 0 };
	bool			  repaired = false;
	int			  count	   = 0;
	int			  rc;
	ENTRY;

	dt_read_lock(env, child, 0);
	if (unlikely(lfsck_is_dead_obj(child))) {
		dt_read_unlock(env, child);

		RETURN(0);
	}

	if (S_ISDIR(lfsck_object_type(child))) {
		dt_read_unlock(env, child);
		rc = lfsck_namespace_double_scan_dir(env, com, child, flags);

		RETURN(rc);
	}

	rc = lfsck_links_read(env, child, &ldata);
	dt_read_unlock(env, child);
	if (rc != 0)
		GOTO(out, rc);

	linkea_first_entry(&ldata);
	while (ldata.ld_lee != NULL) {
		lfsck_namespace_unpack_linkea_entry(&ldata, cname, pfid,
						    info->lti_key);
		rc = lfsck_namespace_filter_linkea_entry(&ldata, cname, pfid,
							 false);
		/* Found repeated linkEA entries */
		if (rc > 0) {
			rc = lfsck_namespace_shrink_linkea(env, com, child,
						&ldata, cname, pfid, false);
			if (rc < 0)
				GOTO(out, rc);

			if (rc == 0)
				continue;

			repaired = true;

			/* fall through */
		}

		/* Invalid PFID in the linkEA entry. */
		if (!fid_is_sane(pfid)) {
			rc = lfsck_namespace_shrink_linkea(env, com, child,
						&ldata, cname, pfid, true);
			if (rc < 0)
				GOTO(out, rc);

			if (rc > 0)
				repaired = true;

			continue;
		}

		parent = lfsck_object_find_bottom(env, lfsck, pfid);
		if (IS_ERR(parent))
			GOTO(out, rc = PTR_ERR(parent));

		if (!dt_object_exists(parent)) {
			if (ldata.ld_leh->leh_reccount > 1) {
				/* If it is NOT the last linkEA entry, then
				 * there is still other chance to make the
				 * child to be visible via other parent, then
				 * remove this linkEA entry. */
				rc = lfsck_namespace_shrink_linkea(env, com,
					child, &ldata, cname, pfid, true);
			} else {
				/* Create the lost parent as an orphan. */
				rc = lfsck_namespace_create_orphan(env, com,
								   parent);
				if (rc < 0) {
					lfsck_object_put(env, parent);

					GOTO(out, rc);
				}

				if (rc > 0)
					repaired = true;

				/* Add the missing name entry to the parent. */
				rc = lfsck_namespace_insert_normal(env, com,
						parent, child, cname->ln_name);
				linkea_next_entry(&ldata);
			}

			lfsck_object_put(env, parent);
			if (rc < 0)
				GOTO(out, rc);

			if (rc > 0)
				repaired = true;

			continue;
		}

		/* The linkEA entry with bad parent will be removed. */
		if (unlikely(!dt_try_as_dir(env, parent))) {
			lfsck_object_put(env, parent);
			rc = lfsck_namespace_shrink_linkea(env, com, child,
						&ldata, cname, pfid, true);
			if (rc < 0)
				GOTO(out, rc);

			if (rc > 0)
				repaired = true;

			continue;
		}

		rc = dt_lookup(env, parent, (struct dt_rec *)cfid,
			       (const struct dt_key *)cname->ln_name,
			       BYPASS_CAPA);
		if (rc != 0 && rc != -ENOENT) {
			lfsck_object_put(env, parent);

			GOTO(out, rc);
		}

		if (rc == 0) {
			if (lu_fid_eq(cfid, lfsck_dto2fid(child))) {
				/* It is the most common case that we
				 * find the name entry corresponding
				 * to the linkEA entry. */
				lfsck_object_put(env, parent);
				linkea_next_entry(&ldata);
			} else {
				/* The name entry references another
				 * MDT-object that may be created by
				 * the LFSCK for repairing dangling
				 * name entry. Try to replace it. */
				rc = lfsck_namespace_replace_cond(env, com,
						parent, child, cfid, cname);
				lfsck_object_put(env, parent);
				if (rc < 0)
					GOTO(out, rc);

				if (rc > 0) {
					repaired = true;
					linkea_next_entry(&ldata);
				} else {
					rc = lfsck_namespace_shrink_linkea(env,
							com, child, &ldata,
							cname, pfid, true);
					if (rc < 0)
						GOTO(out, rc);

					if (rc > 0)
						repaired = true;
				}
			}

			continue;
		}

		rc = dt_attr_get(env, child, la, BYPASS_CAPA);
		if (rc != 0)
			GOTO(out, rc);

		/* If there is no name entry in the parent dir and the object
		 * link count is less than the linkea entries count, then the
		 * linkea entry should be removed. */
		if (ldata.ld_leh->leh_reccount > la->la_nlink) {
			rc = lfsck_namespace_shrink_linkea_cond(env, com,
					parent, child, &ldata, cname, pfid);
			lfsck_object_put(env, parent);
			if (rc < 0)
				GOTO(out, rc);

			if (rc > 0)
				repaired = true;

			continue;
		}

		/* Add the missing name entry back to the namespace. */
		rc = lfsck_namespace_insert_normal(env, com, parent, child,
						   cname->ln_name);
		lfsck_object_put(env, parent);
		if (rc < 0)
			GOTO(out, rc);

		if (rc > 0)
			repaired = true;

		linkea_next_entry(&ldata);
	}

	GOTO(out, rc = 0);

out:
	if (rc < 0 && rc != -ENODATA)
		return rc;

	if (rc == 0) {
		LASSERT(ldata.ld_leh != NULL);

		count = ldata.ld_leh->leh_reccount;
	}

	if (count == 0) {
		/* If the child becomes orphan, then insert it into
		 * the global .lustre/lost+found/MDTxxxx directory. */
		rc = lfsck_namespace_insert_orphan(env, com, child, "", "O",
						   &count);
		if (rc < 0)
			return rc;

		if (rc > 0) {
			ns->ln_mul_ref_repaired++;
			repaired = true;
		}
	}

	rc = dt_attr_get(env, child, la, BYPASS_CAPA);
	if (rc != 0)
		return rc;

	if (la->la_nlink != count) {
		/* XXX: there will be other patch(es) for MDT-object
		 *	hard links verification. */
	}

	if (repaired) {
		if (la->la_nlink > 1)
			ns->ln_mul_linked_repaired++;

		if (rc == 0)
			rc = 1;
	}

	return rc;
}

static void lfsck_namespace_dump_statistics(struct seq_file *m,
					    struct lfsck_namespace *ns,
					    __u64 checked_phase1,
					    __u64 checked_phase2,
					    __u32 time_phase1,
					    __u32 time_phase2)
{
	seq_printf(m, "checked_phase1: "LPU64"\n"
		      "checked_phase2: "LPU64"\n"
		      "updated_phase1: "LPU64"\n"
		      "updated_phase2: "LPU64"\n"
		      "failed_phase1: "LPU64"\n"
		      "failed_phase2: "LPU64"\n"
		      "directories: "LPU64"\n"
		      "dirent_repaired: "LPU64"\n"
		      "linkea_repaired: "LPU64"\n"
		      "nlinks_repaired: "LPU64"\n"
		      "lost_found: "LPU64"\n"
		      "multiple_linked_checked: "LPU64"\n"
		      "multiple_linked_repaired: "LPU64"\n"
		      "unknown_inconsistency: "LPU64"\n"
		      "unmatched_pairs_repaired: "LPU64"\n"
		      "dangling_repaired: "LPU64"\n"
		      "multiple_referenced_repaired: "LPU64"\n"
		      "success_count: %u\n"
		      "run_time_phase1: %u seconds\n"
		      "run_time_phase2: %u seconds\n",
		      checked_phase1,
		      checked_phase2,
		      ns->ln_items_repaired,
		      ns->ln_objs_repaired_phase2,
		      ns->ln_items_failed,
		      ns->ln_objs_failed_phase2,
		      ns->ln_dirs_checked,
		      ns->ln_dirent_repaired,
		      ns->ln_linkea_repaired,
		      ns->ln_objs_nlink_repaired,
		      ns->ln_objs_lost_found,
		      ns->ln_mul_linked_checked,
		      ns->ln_mul_linked_repaired,
		      ns->ln_unknown_inconsistency,
		      ns->ln_unmatched_pairs_repaired,
		      ns->ln_dangling_repaired,
		      ns->ln_mul_ref_repaired,
		      ns->ln_success_count,
		      time_phase1,
		      time_phase2);
}

/* namespace APIs */

static int lfsck_namespace_reset(const struct lu_env *env,
				 struct lfsck_component *com, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_namespace	*ns    = com->lc_file_ram;
	struct dt_object	*root;
	struct dt_object	*dto;
	int			 rc;
	ENTRY;

	root = dt_locate(env, lfsck->li_bottom, &lfsck->li_local_root_fid);
	if (IS_ERR(root))
		GOTO(log, rc = PTR_ERR(root));

	if (unlikely(!dt_try_as_dir(env, root)))
		GOTO(put, rc = -ENOTDIR);

	down_write(&com->lc_sem);
	if (init) {
		memset(ns, 0, sizeof(*ns));
	} else {
		__u32 count = ns->ln_success_count;
		__u64 last_time = ns->ln_time_last_complete;

		memset(ns, 0, sizeof(*ns));
		ns->ln_success_count = count;
		ns->ln_time_last_complete = last_time;
	}
	ns->ln_magic = LFSCK_NAMESPACE_MAGIC;
	ns->ln_status = LS_INIT;

	rc = local_object_unlink(env, lfsck->li_bottom, root,
				 lfsck_namespace_name);
	if (rc != 0)
		GOTO(out, rc);

	lfsck_object_put(env, com->lc_obj);
	com->lc_obj = NULL;
	dto = local_index_find_or_create(env, lfsck->li_los, root,
					 lfsck_namespace_name,
					 S_IFREG | S_IRUGO | S_IWUSR,
					 &dt_lfsck_features);
	if (IS_ERR(dto))
		GOTO(out, rc = PTR_ERR(dto));

	com->lc_obj = dto;
	rc = dto->do_ops->do_index_try(env, dto, &dt_lfsck_features);
	if (rc != 0)
		GOTO(out, rc);

	rc = lfsck_namespace_store(env, com, true);

	GOTO(out, rc);

out:
	up_write(&com->lc_sem);

put:
	lu_object_put(env, &root->do_lu);
log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK reset: rc = %d\n",
	       lfsck_lfsck2name(lfsck), rc);
	return rc;
}

static void
lfsck_namespace_fail(const struct lu_env *env, struct lfsck_component *com,
		     bool new_checked)
{
	struct lfsck_namespace *ns = com->lc_file_ram;

	down_write(&com->lc_sem);
	if (new_checked)
		com->lc_new_checked++;
	lfsck_namespace_record_failure(env, com->lc_lfsck, ns);
	up_write(&com->lc_sem);
}

static int lfsck_namespace_checkpoint(const struct lu_env *env,
				      struct lfsck_component *com, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_namespace	*ns    = com->lc_file_ram;
	int			 rc;

	if (!init) {
		rc = lfsck_checkpoint_generic(env, com);
		if (rc != 0)
			goto log;
	}

	down_write(&com->lc_sem);
	if (init) {
		ns->ln_pos_latest_start = lfsck->li_pos_checkpoint;
	} else {
		ns->ln_pos_last_checkpoint = lfsck->li_pos_checkpoint;
		ns->ln_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		ns->ln_time_last_checkpoint = cfs_time_current_sec();
		ns->ln_items_checked += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_namespace_store(env, com, false);
	up_write(&com->lc_sem);

log:
	CDEBUG(D_LFSCK, "%s: namespace LFSCK checkpoint at the pos ["LPU64
	       ", "DFID", "LPX64"]: rc = %d\n", lfsck_lfsck2name(lfsck),
	       lfsck->li_pos_current.lp_oit_cookie,
	       PFID(&lfsck->li_pos_current.lp_dir_parent),
	       lfsck->li_pos_current.lp_dir_cookie, rc);

	return rc > 0 ? 0 : rc;
}

static int lfsck_namespace_prep(const struct lu_env *env,
				struct lfsck_component *com,
				struct lfsck_start_param *lsp)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct lfsck_namespace	*ns	= com->lc_file_ram;
	struct lfsck_position	*pos	= &com->lc_pos_start;
	int			 rc;

	if (ns->ln_status == LS_COMPLETED) {
		rc = lfsck_namespace_reset(env, com, false);
		if (rc == 0)
			rc = lfsck_set_param(env, lfsck, lsp->lsp_start, true);

		if (rc != 0) {
			CDEBUG(D_LFSCK, "%s: namespace LFSCK prep failed: "
			       "rc = %d\n", lfsck_lfsck2name(lfsck), rc);

			return rc;
		}
	}

	down_write(&com->lc_sem);
	ns->ln_time_latest_start = cfs_time_current_sec();
	spin_lock(&lfsck->li_lock);

	if (ns->ln_flags & LF_SCANNED_ONCE) {
		if (!lfsck->li_drop_dryrun ||
		    lfsck_pos_is_zero(&ns->ln_pos_first_inconsistent)) {
			ns->ln_status = LS_SCANNING_PHASE2;
			list_move_tail(&com->lc_link,
				       &lfsck->li_list_double_scan);
			if (!list_empty(&com->lc_link_dir))
				list_del_init(&com->lc_link_dir);
			lfsck_pos_set_zero(pos);
		} else {
			ns->ln_status = LS_SCANNING_PHASE1;
			ns->ln_run_time_phase1 = 0;
			ns->ln_run_time_phase2 = 0;
			ns->ln_items_checked = 0;
			ns->ln_items_repaired = 0;
			ns->ln_items_failed = 0;
			ns->ln_dirs_checked = 0;
			ns->ln_objs_checked_phase2 = 0;
			ns->ln_objs_repaired_phase2 = 0;
			ns->ln_objs_failed_phase2 = 0;
			ns->ln_objs_nlink_repaired = 0;
			ns->ln_objs_lost_found = 0;
			ns->ln_dirent_repaired = 0;
			ns->ln_linkea_repaired = 0;
			ns->ln_mul_linked_checked = 0;
			ns->ln_mul_linked_repaired = 0;
			ns->ln_unknown_inconsistency = 0;
			ns->ln_unmatched_pairs_repaired = 0;
			ns->ln_dangling_repaired = 0;
			ns->ln_mul_ref_repaired = 0;
			fid_zero(&ns->ln_fid_latest_scanned_phase2);
			if (list_empty(&com->lc_link_dir))
				list_add_tail(&com->lc_link_dir,
					      &lfsck->li_list_dir);
			*pos = ns->ln_pos_first_inconsistent;
		}
	} else {
		ns->ln_status = LS_SCANNING_PHASE1;
		if (list_empty(&com->lc_link_dir))
			list_add_tail(&com->lc_link_dir,
				      &lfsck->li_list_dir);
		if (!lfsck->li_drop_dryrun ||
		    lfsck_pos_is_zero(&ns->ln_pos_first_inconsistent)) {
			*pos = ns->ln_pos_last_checkpoint;
			pos->lp_oit_cookie++;
		} else {
			*pos = ns->ln_pos_first_inconsistent;
		}
	}

	spin_unlock(&lfsck->li_lock);
	up_write(&com->lc_sem);

	rc = lfsck_start_assistant(env, com, lsp);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK prep done, start pos ["LPU64", "
	       DFID", "LPX64"]: rc = %d\n",
	       lfsck_lfsck2name(lfsck), pos->lp_oit_cookie,
	       PFID(&pos->lp_dir_parent), pos->lp_dir_cookie, rc);

	return rc;
}

static int lfsck_namespace_exec_oit(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct dt_object *obj)
{
	struct lfsck_thread_info *info	= lfsck_env_info(env);
	struct lfsck_namespace	 *ns	= com->lc_file_ram;
	struct lfsck_instance	 *lfsck	= com->lc_lfsck;
	const struct lu_fid	 *fid	= lfsck_dto2fid(obj);
	struct lu_attr		 *la	= &info->lti_la;
	struct lu_fid		 *pfid	= &info->lti_fid2;
	struct lu_name		 *cname	= &info->lti_name;
	struct lu_seq_range	 *range = &info->lti_range;
	struct dt_device	 *dev	= lfsck->li_bottom;
	struct seq_server_site	 *ss	=
				lu_site2seq(dev->dd_lu_dev.ld_site);
	struct linkea_data	  ldata	= { 0 };
	__u32			  idx	= lfsck_dev_idx(dev);
	int			  rc;
	ENTRY;

	rc = lfsck_links_read(env, obj, &ldata);
	if (rc == -ENOENT)
		GOTO(out, rc = 0);

	/* -EINVAL means crashed linkEA, should be verified. */
	if (rc == -EINVAL) {
		rc = lfsck_namespace_trace_update(env, com, fid,
						  LNTF_CHECK_LINKEA, true);
		if (rc == 0) {
			struct lustre_handle lh	= { 0 };

			rc = lfsck_ibits_lock(env, lfsck, obj, &lh,
					      MDS_INODELOCK_UPDATE |
					      MDS_INODELOCK_XATTR, LCK_EX);
			if (rc == 0) {
				rc = lfsck_namespace_links_remove(env, com,
								  obj);
				lfsck_ibits_unlock(&lh, LCK_EX);
			}
		}

		GOTO(out, rc = (rc == -ENOENT ? 0 : rc));
	}

	/* zero-linkEA object may be orphan, but it also maybe because
	 * of upgrading. Currently, we cannot record it for double scan.
	 * Because it may cause the LFSCK tracing file to be too large. */
	if (rc == -ENODATA) {
		if (S_ISDIR(lfsck_object_type(obj)))
			GOTO(out, rc = 0);

		rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
		if (rc != 0)
			GOTO(out, rc);

		if (la->la_nlink > 1)
			rc = lfsck_namespace_trace_update(env, com, fid,
						LNTF_CHECK_LINKEA, true);

		GOTO(out, rc);
	}

	if (rc != 0)
		GOTO(out, rc);

	/* Record multiple-linked object. */
	if (ldata.ld_leh->leh_reccount > 1) {
		rc = lfsck_namespace_trace_update(env, com, fid,
						  LNTF_CHECK_LINKEA, true);

		GOTO(out, rc);
	}

	linkea_first_entry(&ldata);
	linkea_entry_unpack(ldata.ld_lee, &ldata.ld_reclen, cname, pfid);
	if (!fid_is_sane(pfid)) {
		rc = lfsck_namespace_trace_update(env, com, fid,
						  LNTF_CHECK_PARENT, true);
	} else {
		fld_range_set_mdt(range);
		rc = fld_local_lookup(env, ss->ss_server_fld,
				      fid_seq(pfid), range);
		if ((rc == -ENOENT) ||
		    (rc == 0 && range->lsr_index != idx)) {
			rc = lfsck_namespace_trace_update(env, com, fid,
						LNTF_CHECK_LINKEA, true);
		} else {
			if (S_ISDIR(lfsck_object_type(obj)))
				GOTO(out, rc = 0);

			rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
			if (rc != 0)
				GOTO(out, rc);

			if (la->la_nlink > 1)
				rc = lfsck_namespace_trace_update(env, com,
						fid, LNTF_CHECK_LINKEA, true);
		}
	}

	GOTO(out, rc);

out:
	down_write(&com->lc_sem);
	com->lc_new_checked++;
	if (S_ISDIR(lfsck_object_type(obj)))
		ns->ln_dirs_checked++;
	if (rc != 0)
		lfsck_namespace_record_failure(env, com->lc_lfsck, ns);
	up_write(&com->lc_sem);

	return rc;
}

static int lfsck_namespace_exec_dir(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct lu_dirent *ent, __u16 type)
{
	struct lfsck_assistant_data	*lad	= com->lc_data;
	struct lfsck_namespace_req	*lnr;
	bool				 wakeup	= false;

	lnr = lfsck_namespace_assistant_req_init(com->lc_lfsck, ent, type);
	if (IS_ERR(lnr)) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		lfsck_namespace_record_failure(env, com->lc_lfsck, ns);
		return PTR_ERR(lnr);
	}

	spin_lock(&lad->lad_lock);
	if (lad->lad_assistant_status < 0) {
		spin_unlock(&lad->lad_lock);
		lfsck_namespace_assistant_req_fini(env, &lnr->lnr_lar);
		return lad->lad_assistant_status;
	}

	list_add_tail(&lnr->lnr_lar.lar_list, &lad->lad_req_list);
	if (lad->lad_prefetched == 0)
		wakeup = true;

	lad->lad_prefetched++;
	spin_unlock(&lad->lad_lock);
	if (wakeup)
		wake_up_all(&lad->lad_thread.t_ctl_waitq);

	down_write(&com->lc_sem);
	com->lc_new_checked++;
	up_write(&com->lc_sem);

	return 0;
}

static int lfsck_namespace_post(const struct lu_env *env,
				struct lfsck_component *com,
				int result, bool init)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_namespace	*ns    = com->lc_file_ram;
	int			 rc;
	ENTRY;

	lfsck_post_generic(env, com, &result);

	down_write(&com->lc_sem);
	spin_lock(&lfsck->li_lock);
	if (!init)
		ns->ln_pos_last_checkpoint = lfsck->li_pos_checkpoint;
	if (result > 0) {
		ns->ln_status = LS_SCANNING_PHASE2;
		ns->ln_flags |= LF_SCANNED_ONCE;
		ns->ln_flags &= ~LF_UPGRADE;
		list_del_init(&com->lc_link_dir);
		list_move_tail(&com->lc_link, &lfsck->li_list_double_scan);
	} else if (result == 0) {
		ns->ln_status = lfsck->li_status;
		if (ns->ln_status == 0)
			ns->ln_status = LS_STOPPED;
		if (ns->ln_status != LS_PAUSED) {
			list_del_init(&com->lc_link_dir);
			list_move_tail(&com->lc_link, &lfsck->li_list_idle);
		}
	} else {
		ns->ln_status = LS_FAILED;
		list_del_init(&com->lc_link_dir);
		list_move_tail(&com->lc_link, &lfsck->li_list_idle);
	}
	spin_unlock(&lfsck->li_lock);

	if (!init) {
		ns->ln_run_time_phase1 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
		ns->ln_time_last_checkpoint = cfs_time_current_sec();
		ns->ln_items_checked += com->lc_new_checked;
		com->lc_new_checked = 0;
	}

	rc = lfsck_namespace_store(env, com, false);
	up_write(&com->lc_sem);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK post done: rc = %d\n",
	       lfsck_lfsck2name(lfsck), rc);

	RETURN(rc);
}

static int
lfsck_namespace_dump(const struct lu_env *env, struct lfsck_component *com,
		     struct seq_file *m)
{
	struct lfsck_instance	*lfsck = com->lc_lfsck;
	struct lfsck_bookmark	*bk    = &lfsck->li_bookmark_ram;
	struct lfsck_namespace	*ns    = com->lc_file_ram;
	int			 rc;

	down_read(&com->lc_sem);
	seq_printf(m, "name: lfsck_namespace\n"
		   "magic: %#x\n"
		   "version: %d\n"
		   "status: %s\n",
		   ns->ln_magic,
		   bk->lb_version,
		   lfsck_status2names(ns->ln_status));

	rc = lfsck_bits_dump(m, ns->ln_flags, lfsck_flags_names, "flags");
	if (rc < 0)
		goto out;

	rc = lfsck_bits_dump(m, bk->lb_param, lfsck_param_names, "param");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(m, ns->ln_time_last_complete,
			     "time_since_last_completed");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(m, ns->ln_time_latest_start,
			     "time_since_latest_start");
	if (rc < 0)
		goto out;

	rc = lfsck_time_dump(m, ns->ln_time_last_checkpoint,
			     "time_since_last_checkpoint");
	if (rc < 0)
		goto out;

	rc = lfsck_pos_dump(m, &ns->ln_pos_latest_start,
			    "latest_start_position");
	if (rc < 0)
		goto out;

	rc = lfsck_pos_dump(m, &ns->ln_pos_last_checkpoint,
			    "last_checkpoint_position");
	if (rc < 0)
		goto out;

	rc = lfsck_pos_dump(m, &ns->ln_pos_first_inconsistent,
			    "first_failure_position");
	if (rc < 0)
		goto out;

	if (ns->ln_status == LS_SCANNING_PHASE1) {
		struct lfsck_position pos;
		const struct dt_it_ops *iops;
		cfs_duration_t duration = cfs_time_current() -
					  lfsck->li_time_last_checkpoint;
		__u64 checked = ns->ln_items_checked + com->lc_new_checked;
		__u64 speed = checked;
		__u64 new_checked = com->lc_new_checked * HZ;
		__u32 rtime = ns->ln_run_time_phase1 +
			      cfs_duration_sec(duration + HALF_SEC);

		if (duration != 0)
			do_div(new_checked, duration);
		if (rtime != 0)
			do_div(speed, rtime);
		lfsck_namespace_dump_statistics(m, ns, checked,
						ns->ln_objs_checked_phase2,
						rtime, ns->ln_run_time_phase2);

		seq_printf(m, "average_speed_phase1: "LPU64" items/sec\n"
			      "average_speed_phase2: N/A\n"
			      "real_time_speed_phase1: "LPU64" items/sec\n"
			      "real_time_speed_phase2: N/A\n",
			      speed,
			      new_checked);

		LASSERT(lfsck->li_di_oit != NULL);

		iops = &lfsck->li_obj_oit->do_index_ops->dio_it;

		/* The low layer otable-based iteration position may NOT
		 * exactly match the namespace-based directory traversal
		 * cookie. Generally, it is not a serious issue. But the
		 * caller should NOT make assumption on that. */
		pos.lp_oit_cookie = iops->store(env, lfsck->li_di_oit);
		if (!lfsck->li_current_oit_processed)
			pos.lp_oit_cookie--;

		spin_lock(&lfsck->li_lock);
		if (lfsck->li_di_dir != NULL) {
			pos.lp_dir_cookie = lfsck->li_cookie_dir;
			if (pos.lp_dir_cookie >= MDS_DIR_END_OFF) {
				fid_zero(&pos.lp_dir_parent);
				pos.lp_dir_cookie = 0;
			} else {
				pos.lp_dir_parent =
					*lfsck_dto2fid(lfsck->li_obj_dir);
			}
		} else {
			fid_zero(&pos.lp_dir_parent);
			pos.lp_dir_cookie = 0;
		}
		spin_unlock(&lfsck->li_lock);
		lfsck_pos_dump(m, &pos, "current_position");
	} else if (ns->ln_status == LS_SCANNING_PHASE2) {
		cfs_duration_t duration = cfs_time_current() -
					  lfsck->li_time_last_checkpoint;
		__u64 checked = ns->ln_objs_checked_phase2 +
				com->lc_new_checked;
		__u64 speed1 = ns->ln_items_checked;
		__u64 speed2 = checked;
		__u64 new_checked = com->lc_new_checked * HZ;
		__u32 rtime = ns->ln_run_time_phase2 +
			      cfs_duration_sec(duration + HALF_SEC);

		if (duration != 0)
			do_div(new_checked, duration);
		if (ns->ln_run_time_phase1 != 0)
			do_div(speed1, ns->ln_run_time_phase1);
		if (rtime != 0)
			do_div(speed2, rtime);
		lfsck_namespace_dump_statistics(m, ns, ns->ln_items_checked,
						checked,
						ns->ln_run_time_phase1, rtime);

		seq_printf(m, "average_speed_phase1: "LPU64" items/sec\n"
			      "average_speed_phase2: "LPU64" objs/sec\n"
			      "real_time_speed_phase1: N/A\n"
			      "real_time_speed_phase2: "LPU64" objs/sec\n"
			      "current_position: "DFID"\n",
			      speed1,
			      speed2,
			      new_checked,
			      PFID(&ns->ln_fid_latest_scanned_phase2));
	} else {
		__u64 speed1 = ns->ln_items_checked;
		__u64 speed2 = ns->ln_objs_checked_phase2;

		if (ns->ln_run_time_phase1 != 0)
			do_div(speed1, ns->ln_run_time_phase1);
		if (ns->ln_run_time_phase2 != 0)
			do_div(speed2, ns->ln_run_time_phase2);
		lfsck_namespace_dump_statistics(m, ns, ns->ln_items_checked,
						ns->ln_objs_checked_phase2,
						ns->ln_run_time_phase1,
						ns->ln_run_time_phase2);

		seq_printf(m, "average_speed_phase1: "LPU64" items/sec\n"
			      "average_speed_phase2: "LPU64" objs/sec\n"
			      "real_time_speed_phase1: N/A\n"
			      "real_time_speed_phase2: N/A\n"
			      "current_position: N/A\n",
			      speed1,
			      speed2);
	}
out:
	up_read(&com->lc_sem);
	return 0;
}

static int lfsck_namespace_double_scan(const struct lu_env *env,
				       struct lfsck_component *com)
{
	struct lfsck_namespace *ns = com->lc_file_ram;

	return lfsck_double_scan_generic(env, com, ns->ln_status);
}

static void lfsck_namespace_data_release(const struct lu_env *env,
					 struct lfsck_component *com)
{
	struct lfsck_assistant_data	*lad	= com->lc_data;
	struct lfsck_tgt_descs		*ltds	= &com->lc_lfsck->li_mdt_descs;
	struct lfsck_tgt_desc		*ltd;
	struct lfsck_tgt_desc		*next;

	LASSERT(lad != NULL);
	LASSERT(thread_is_init(&lad->lad_thread) ||
		thread_is_stopped(&lad->lad_thread));
	LASSERT(list_empty(&lad->lad_req_list));

	com->lc_data = NULL;

	spin_lock(&ltds->ltd_lock);
	list_for_each_entry_safe(ltd, next, &lad->lad_mdt_phase1_list,
				 ltd_namespace_phase_list) {
		list_del_init(&ltd->ltd_namespace_phase_list);
	}
	list_for_each_entry_safe(ltd, next, &lad->lad_mdt_phase2_list,
				 ltd_namespace_phase_list) {
		list_del_init(&ltd->ltd_namespace_phase_list);
	}
	list_for_each_entry_safe(ltd, next, &lad->lad_mdt_list,
				 ltd_namespace_list) {
		list_del_init(&ltd->ltd_namespace_list);
	}
	spin_unlock(&ltds->ltd_lock);

	CFS_FREE_BITMAP(lad->lad_bitmap);

	OBD_FREE_PTR(lad);
}

static int lfsck_namespace_in_notify(const struct lu_env *env,
				     struct lfsck_component *com,
				     struct lfsck_request *lr)
{
	struct lfsck_instance		*lfsck = com->lc_lfsck;
	struct lfsck_namespace		*ns    = com->lc_file_ram;
	struct lfsck_assistant_data	*lad   = com->lc_data;
	struct lfsck_tgt_descs		*ltds  = &lfsck->li_mdt_descs;
	struct lfsck_tgt_desc		*ltd;
	bool				 fail  = false;
	ENTRY;

	if (lr->lr_event != LE_PHASE1_DONE &&
	    lr->lr_event != LE_PHASE2_DONE &&
	    lr->lr_event != LE_PEER_EXIT)
		RETURN(-EINVAL);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK handles notify %u from MDT %x, "
	       "status %d\n", lfsck_lfsck2name(lfsck), lr->lr_event,
	       lr->lr_index, lr->lr_status);

	spin_lock(&ltds->ltd_lock);
	ltd = LTD_TGT(ltds, lr->lr_index);
	if (ltd == NULL) {
		spin_unlock(&ltds->ltd_lock);

		RETURN(-ENXIO);
	}

	list_del_init(&ltd->ltd_namespace_phase_list);
	switch (lr->lr_event) {
	case LE_PHASE1_DONE:
		if (lr->lr_status <= 0) {
			ltd->ltd_namespace_done = 1;
			list_del_init(&ltd->ltd_namespace_list);
			CDEBUG(D_LFSCK, "%s: MDT %x failed/stopped at "
			       "phase1 for namespace LFSCK: rc = %d.\n",
			       lfsck_lfsck2name(lfsck),
			       ltd->ltd_index, lr->lr_status);
			ns->ln_flags |= LF_INCOMPLETE;
			fail = true;
			break;
		}

		if (list_empty(&ltd->ltd_namespace_list))
			list_add_tail(&ltd->ltd_namespace_list,
				      &lad->lad_mdt_list);
		list_add_tail(&ltd->ltd_namespace_phase_list,
			      &lad->lad_mdt_phase2_list);
		break;
	case LE_PHASE2_DONE:
		ltd->ltd_namespace_done = 1;
		list_del_init(&ltd->ltd_namespace_list);
		break;
	case LE_PEER_EXIT:
		fail = true;
		ltd->ltd_namespace_done = 1;
		list_del_init(&ltd->ltd_namespace_list);
		if (!(lfsck->li_bookmark_ram.lb_param & LPF_FAILOUT)) {
			CDEBUG(D_LFSCK,
			       "%s: the peer MDT %x exit namespace LFSCK\n",
			       lfsck_lfsck2name(lfsck), ltd->ltd_index);
			ns->ln_flags |= LF_INCOMPLETE;
		}
		break;
	default:
		break;
	}
	spin_unlock(&ltds->ltd_lock);

	if (fail && lfsck->li_bookmark_ram.lb_param & LPF_FAILOUT) {
		struct lfsck_stop *stop = &lfsck_env_info(env)->lti_stop;

		memset(stop, 0, sizeof(*stop));
		stop->ls_status = lr->lr_status;
		stop->ls_flags = lr->lr_param & ~LPF_BROADCAST;
		lfsck_stop(env, lfsck->li_bottom, stop);
	} else if (lfsck_phase2_next_ready(lad)) {
		wake_up_all(&lad->lad_thread.t_ctl_waitq);
	}

	RETURN(0);
}

static int lfsck_namespace_query(const struct lu_env *env,
				 struct lfsck_component *com)
{
	struct lfsck_namespace *ns = com->lc_file_ram;

	return ns->ln_status;
}

static struct lfsck_operations lfsck_namespace_ops = {
	.lfsck_reset		= lfsck_namespace_reset,
	.lfsck_fail		= lfsck_namespace_fail,
	.lfsck_checkpoint	= lfsck_namespace_checkpoint,
	.lfsck_prep		= lfsck_namespace_prep,
	.lfsck_exec_oit		= lfsck_namespace_exec_oit,
	.lfsck_exec_dir		= lfsck_namespace_exec_dir,
	.lfsck_post		= lfsck_namespace_post,
	.lfsck_dump		= lfsck_namespace_dump,
	.lfsck_double_scan	= lfsck_namespace_double_scan,
	.lfsck_data_release	= lfsck_namespace_data_release,
	.lfsck_quit		= lfsck_quit_generic,
	.lfsck_in_notify	= lfsck_namespace_in_notify,
	.lfsck_query		= lfsck_namespace_query,
};

/**
 * Repair dangling name entry.
 *
 * For the name entry with dangling reference, we need to repare the
 * inconsistency according to the LFSCK sponsor's requirement:
 *
 * 1) Keep the inconsistency there and report the inconsistency case,
 *    then give the chance to the application to find related issues,
 *    and the users can make the decision about how to handle it with
 *    more human knownledge. (by default)
 *
 * 2) Re-create the missed MDT-object with the FID information.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] com	pointer to the lfsck component
 * \param[in] child	pointer to the object corresponding to the dangling
 *			name entry
 * \param[in] lnr	pointer to the namespace request that contains the
 *			name's name, parent object, parent's LMV, and ect.
 *
 * \retval		positive number if no need to repair
 * \retval		zero for repaired successfully
 * \retval		negative error number on failure
 */
int lfsck_namespace_repair_dangling(const struct lu_env *env,
				    struct lfsck_component *com,
				    struct dt_object *child,
				    struct lfsck_namespace_req *lnr)
{
	struct lfsck_thread_info	*info	= lfsck_env_info(env);
	struct lu_attr			*la	= &info->lti_la;
	struct dt_allocation_hint	*hint	= &info->lti_hint;
	struct dt_object_format		*dof	= &info->lti_dof;
	struct dt_insert_rec		*rec	= &info->lti_dt_rec;
	struct dt_object		*parent	= lnr->lnr_obj;
	const struct lu_name		*cname;
	struct linkea_data		 ldata	= { 0 };
	struct lustre_handle		 lh     = { 0 };
	struct lu_buf			 linkea_buf;
	struct lfsck_instance		*lfsck	= com->lc_lfsck;
	struct lfsck_bookmark		*bk	= &lfsck->li_bookmark_ram;
	struct dt_device		*dev	= lfsck_obj2dt_dev(child);
	struct thandle			*th	= NULL;
	int				 rc	= 0;
	__u16				 type	= lnr->lnr_type;
	bool				 create;
	ENTRY;

	cname = lfsck_name_get_const(env, lnr->lnr_name, lnr->lnr_namelen);
	if (bk->lb_param & LPF_CREATE_MDTOBJ)
		create = true;
	else
		create = false;

	if (!create || bk->lb_param & LPF_DRYRUN)
		GOTO(log, rc = 0);

	rc = linkea_data_new(&ldata, &info->lti_linkea_buf2);
	if (rc != 0)
		GOTO(log, rc);

	rc = linkea_add_buf(&ldata, cname, lfsck_dto2fid(parent));
	if (rc != 0)
		GOTO(log, rc);

	rc = lfsck_ibits_lock(env, lfsck, parent, &lh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		GOTO(log, rc);

	rc = lfsck_namespace_check_exist(env, parent, child, lnr->lnr_name);
	if (rc != 0)
		GOTO(log, rc);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(log, rc = PTR_ERR(th));

	/* Set the ctime as zero, then others can know it is created for
	 * repairing dangling name entry by LFSCK. And if the LFSCK made
	 * wrong decision and the real MDT-object has been found later,
	 * then the LFSCK has chance to fix the incosistency properly. */
	memset(la, 0, sizeof(*la));
	la->la_mode = (type & S_IFMT) | 0600;
	la->la_valid = LA_TYPE | LA_MODE | LA_UID | LA_GID |
			LA_ATIME | LA_MTIME | LA_CTIME;

	child->do_ops->do_ah_init(env, hint, parent, child,
				  la->la_mode & S_IFMT);

	memset(dof, 0, sizeof(*dof));
	dof->dof_type = dt_mode_to_dft(type);
	/* If the target is a regular file, then the LFSCK will only create
	 * the MDT-object without stripes (dof->dof_reg.striped = 0). related
	 * OST-objects will be created when write open. */

	/* 1a. create child. */
	rc = dt_declare_create(env, child, la, hint, dof, th);
	if (rc != 0)
		GOTO(stop, rc);

	if (S_ISDIR(type)) {
		if (unlikely(!dt_try_as_dir(env, child)))
			GOTO(stop, rc = -ENOTDIR);

		/* 2a. insert dot into child dir */
		rec->rec_type = S_IFDIR;
		rec->rec_fid = lfsck_dto2fid(child);
		rc = dt_declare_insert(env, child,
				       (const struct dt_rec *)rec,
				       (const struct dt_key *)dot, th);
		if (rc != 0)
			GOTO(stop, rc);

		/* 3a. insert dotdot into child dir */
		rec->rec_fid = lfsck_dto2fid(parent);
		rc = dt_declare_insert(env, child,
				       (const struct dt_rec *)rec,
				       (const struct dt_key *)dotdot, th);
		if (rc != 0)
			GOTO(stop, rc);

		/* 4a. increase child nlink */
		rc = dt_declare_ref_add(env, child, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	/* 5a. insert linkEA for child */
	lfsck_buf_init(&linkea_buf, ldata.ld_buf->lb_buf,
		       ldata.ld_leh->leh_len);
	rc = dt_declare_xattr_set(env, child, &linkea_buf,
				  XATTR_NAME_LINK, 0, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc = (rc == -EEXIST ? 1 : rc));

	dt_write_lock(env, child, 0);
	/* 1b. create child */
	rc = dt_create(env, child, la, hint, dof, th);
	if (rc != 0)
		GOTO(unlock, rc = (rc == -EEXIST ? 1 : rc));

	if (S_ISDIR(type)) {
		if (unlikely(!dt_try_as_dir(env, child)))
			GOTO(unlock, rc = -ENOTDIR);

		/* 2b. insert dot into child dir */
		rec->rec_type = S_IFDIR;
		rec->rec_fid = lfsck_dto2fid(child);
		rc = dt_insert(env, child, (const struct dt_rec *)rec,
			       (const struct dt_key *)dot, th, BYPASS_CAPA, 1);
		if (rc != 0)
			GOTO(unlock, rc);

		/* 3b. insert dotdot into child dir */
		rec->rec_fid = lfsck_dto2fid(parent);
		rc = dt_insert(env, child, (const struct dt_rec *)rec,
			       (const struct dt_key *)dotdot, th,
			       BYPASS_CAPA, 1);
		if (rc != 0)
			GOTO(unlock, rc);

		/* 4b. increase child nlink */
		rc = dt_ref_add(env, child, th);
		if (rc != 0)
			GOTO(unlock, rc);
	}

	/* 5b. insert linkEA for child. */
	rc = dt_xattr_set(env, child, &linkea_buf,
			  XATTR_NAME_LINK, 0, th, BYPASS_CAPA);

	GOTO(unlock, rc);

unlock:
	dt_write_unlock(env, child);

stop:
	dt_trans_stop(env, dev, th);

log:
	lfsck_ibits_unlock(&lh, LCK_EX);
	CDEBUG(D_LFSCK, "%s: namespace LFSCK assistant found dangling "
	       "reference for: parent "DFID", child "DFID", type %u, "
	       "name %s. %s: rc = %d\n", lfsck_lfsck2name(lfsck),
	       PFID(lfsck_dto2fid(parent)), PFID(lfsck_dto2fid(child)),
	       type, cname->ln_name,
	       create ? "Create the lost OST-object as required" :
			"Keep the MDT-object there by default", rc);

	if (rc <= 0) {
		struct lfsck_namespace *ns = com->lc_file_ram;

		ns->ln_flags |= LF_INCONSISTENT;
	}

	return rc;
}

static int lfsck_namespace_assistant_handler_p1(const struct lu_env *env,
						struct lfsck_component *com,
						struct lfsck_assistant_req *lar)
{
	struct lfsck_thread_info   *info     = lfsck_env_info(env);
	struct lu_attr		   *la	     = &info->lti_la;
	struct lfsck_instance	   *lfsck    = com->lc_lfsck;
	struct lfsck_bookmark	   *bk	     = &lfsck->li_bookmark_ram;
	struct lfsck_namespace	   *ns	     = com->lc_file_ram;
	struct linkea_data	    ldata    = { 0 };
	const struct lu_name	   *cname;
	struct thandle		   *handle   = NULL;
	struct lfsck_namespace_req *lnr      =
			container_of0(lar, struct lfsck_namespace_req, lnr_lar);
	struct dt_object	   *dir      = lnr->lnr_obj;
	struct dt_object	   *obj      = NULL;
	const struct lu_fid	   *pfid     = lfsck_dto2fid(dir);
	struct dt_device	   *dev;
	struct lustre_handle	    lh       = { 0 };
	bool			    repaired = false;
	bool			    dtlocked = false;
	bool			    remove;
	bool			    newdata;
	bool			    log      = false;
	int			    idx;
	int			    count    = 0;
	int			    rc;
	enum lfsck_namespace_inconsistency_type type = LNIT_NONE;
	ENTRY;

	if (lnr->lnr_attr & LUDA_UPGRADE) {
		ns->ln_flags |= LF_UPGRADE;
		ns->ln_dirent_repaired++;
		repaired = true;
	} else if (lnr->lnr_attr & LUDA_REPAIR) {
		ns->ln_flags |= LF_INCONSISTENT;
		ns->ln_dirent_repaired++;
		repaired = true;
	}

	if (unlikely(fid_is_zero(&lnr->lnr_fid))) {
		if (strcmp(lnr->lnr_name, dotdot) != 0)
			LBUG();
		else
			rc = lfsck_namespace_trace_update(env, com, pfid,
						LNTF_CHECK_PARENT, true);

		GOTO(out, rc);
	}

	if (lnr->lnr_name[0] == '.' &&
	    (lnr->lnr_namelen == 1 || fid_seq_is_dot(fid_seq(&lnr->lnr_fid))))
		GOTO(out, rc = 0);

	idx = lfsck_find_mdt_idx_by_fid(env, lfsck, &lnr->lnr_fid);
	if (idx < 0)
		GOTO(out, rc = idx);

	if (idx == lfsck_dev_idx(lfsck->li_bottom)) {
		if (unlikely(strcmp(lnr->lnr_name, dotdot) == 0))
			GOTO(out, rc = 0);

		dev = lfsck->li_next;
	} else {
		struct lfsck_tgt_desc *ltd;

		/* Usually, some local filesystem consistency verification
		 * tools can guarantee the local namespace tree consistenct.
		 * So the LFSCK will only verify the remote directory. */
		if (unlikely(strcmp(lnr->lnr_name, dotdot) == 0)) {
			rc = lfsck_namespace_trace_update(env, com, pfid,
						LNTF_CHECK_PARENT, true);

			GOTO(out, rc);
		}

		ltd = LTD_TGT(&lfsck->li_mdt_descs, idx);
		if (unlikely(ltd == NULL)) {
			CDEBUG(D_LFSCK, "%s: cannot talk with MDT %x which "
			       "did not join the namespace LFSCK\n",
			       lfsck_lfsck2name(lfsck), idx);
			ns->ln_flags |= LF_INCOMPLETE;

			GOTO(out, rc = -ENODEV);
		}

		dev = ltd->ltd_tgt;
	}

	obj = lfsck_object_find_by_dev(env, dev, &lnr->lnr_fid);
	if (IS_ERR(obj))
		GOTO(out, rc = PTR_ERR(obj));

	cname = lfsck_name_get_const(env, lnr->lnr_name, lnr->lnr_namelen);
	if (dt_object_exists(obj) == 0) {

dangling:
		rc = lfsck_namespace_check_exist(env, dir, obj, lnr->lnr_name);
		if (rc == 0) {
			type = LNIT_DANGLING;
			rc = lfsck_namespace_repair_dangling(env, com,
							     obj, lnr);
			if (rc == 0)
				repaired = true;
		}

		GOTO(out, rc);
	}

	if (!(bk->lb_param & LPF_DRYRUN) && repaired) {

again:
		rc = lfsck_ibits_lock(env, lfsck, obj, &lh,
				      MDS_INODELOCK_UPDATE |
				      MDS_INODELOCK_XATTR, LCK_EX);
		if (rc != 0)
			GOTO(out, rc);

		handle = dt_trans_create(env, dev);
		if (IS_ERR(handle))
			GOTO(out, rc = PTR_ERR(handle));

		rc = lfsck_declare_namespace_exec_dir(env, obj, handle);
		if (rc != 0)
			GOTO(stop, rc);

		rc = dt_trans_start(env, dev, handle);
		if (rc != 0)
			GOTO(stop, rc);

		dt_write_lock(env, obj, 0);
		dtlocked = true;
	}

	rc = lfsck_namespace_check_exist(env, dir, obj, lnr->lnr_name);
	if (rc != 0)
		GOTO(stop, rc);

	rc = lfsck_links_read(env, obj, &ldata);
	if (unlikely(rc == -ENOENT)) {
		if (handle != NULL) {
			dt_write_unlock(env, obj);
			dtlocked = false;

			dt_trans_stop(env, dev, handle);
			handle = NULL;

			lfsck_ibits_unlock(&lh, LCK_EX);
		}

		/* It may happen when the remote object has been removed,
		 * but the local MDT does not aware of that. */
		goto dangling;
	} else if (rc == 0) {
		count = ldata.ld_leh->leh_reccount;
		rc = linkea_links_find(&ldata, cname, pfid);
		if ((rc == 0) &&
		    (count == 1 || !S_ISDIR(lfsck_object_type(obj))))
			goto record;

		ns->ln_flags |= LF_INCONSISTENT;
		/* For sub-dir object, we cannot make sure whether the sub-dir
		 * back references the parent via ".." name entry correctly or
		 * not in the LFSCK first-stage scanning. It may be that the
		 * (remote) sub-dir ".." name entry has no parent FID after
		 * file-level backup/restore and its linkEA may be wrong.
		 * So under such case, we should replace the linkEA according
		 * to current name entry. But this needs to be done during the
		 * LFSCK second-stage scanning. The LFSCK will record the name
		 * entry for further possible using. */
		remove = false;
		newdata = false;
		goto nodata;
	} else if (unlikely(rc == -EINVAL)) {
		count = 1;
		ns->ln_flags |= LF_INCONSISTENT;
		/* The magic crashed, we are not sure whether there are more
		 * corrupt data in the linkea, so remove all linkea entries. */
		remove = true;
		newdata = true;
		goto nodata;
	} else if (rc == -ENODATA) {
		count = 1;
		ns->ln_flags |= LF_UPGRADE;
		remove = false;
		newdata = true;

nodata:
		if (bk->lb_param & LPF_DRYRUN) {
			ns->ln_linkea_repaired++;
			repaired = true;
			log = true;
			goto record;
		}

		if (!lustre_handle_is_used(&lh))
			goto again;

		if (remove) {
			LASSERT(newdata);

			rc = dt_xattr_del(env, obj, XATTR_NAME_LINK, handle,
					  BYPASS_CAPA);
			if (rc != 0)
				GOTO(stop, rc);
		}

		if (newdata) {
			rc = linkea_data_new(&ldata,
					&lfsck_env_info(env)->lti_linkea_buf);
			if (rc != 0)
				GOTO(stop, rc);
		}

		rc = linkea_add_buf(&ldata, cname, pfid);
		if (rc != 0)
			GOTO(stop, rc);

		rc = lfsck_links_write(env, obj, &ldata, handle);
		if (rc != 0)
			GOTO(stop, rc);

		count = ldata.ld_leh->leh_reccount;
		if (!S_ISDIR(lfsck_object_type(obj)) ||
		    !dt_object_remote(obj)) {
			ns->ln_linkea_repaired++;
			repaired = true;
			log = true;
		}
	} else {
		GOTO(stop, rc);
	}

record:
	LASSERT(count > 0);

	rc = dt_attr_get(env, obj, la, BYPASS_CAPA);
	if (rc != 0)
		GOTO(stop, rc);

	if ((count == 1 && la->la_nlink == 1) ||
	    S_ISDIR(lfsck_object_type(obj)))
		/* Usually, it is for single linked object or dir, do nothing.*/
		GOTO(stop, rc);

	/* Following modification will be in another transaction.  */
	if (handle != NULL) {
		dt_write_unlock(env, obj);
		dtlocked = false;

		dt_trans_stop(env, dev, handle);
		handle = NULL;

		lfsck_ibits_unlock(&lh, LCK_EX);
	}

	ns->ln_mul_linked_checked++;
	rc = lfsck_namespace_trace_update(env, com, &lnr->lnr_fid,
					  LNTF_CHECK_LINKEA, true);

	GOTO(out, rc);

stop:
	if (dtlocked)
		dt_write_unlock(env, obj);

	if (handle != NULL && !IS_ERR(handle))
		dt_trans_stop(env, dev, handle);

out:
	lfsck_ibits_unlock(&lh, LCK_EX);
	down_write(&com->lc_sem);
	if (rc < 0) {
		CDEBUG(D_LFSCK, "%s: namespace LFSCK assistant fail to handle "
		       "the entry: "DFID", parent "DFID", name %.*s: rc = %d\n",
		       lfsck_lfsck2name(lfsck), PFID(&lnr->lnr_fid),
		       PFID(lfsck_dto2fid(lnr->lnr_obj)),
		       lnr->lnr_namelen, lnr->lnr_name, rc);

		lfsck_namespace_record_failure(env, lfsck, ns);
		if (!(bk->lb_param & LPF_FAILOUT))
			rc = 0;
	} else {
		if (log)
			CDEBUG(D_LFSCK, "%s: namespace LFSCK assistant "
			       "repaired the entry: "DFID", parent "DFID
			       ", name %.*s\n", lfsck_lfsck2name(lfsck),
			       PFID(&lnr->lnr_fid),
			       PFID(lfsck_dto2fid(lnr->lnr_obj)),
			       lnr->lnr_namelen, lnr->lnr_name);

		if (repaired) {
			ns->ln_items_repaired++;

			switch (type) {
			case LNIT_DANGLING:
				ns->ln_dangling_repaired++;
				break;
			default:
				break;
			}

			if (bk->lb_param & LPF_DRYRUN &&
			    lfsck_pos_is_zero(&ns->ln_pos_first_inconsistent))
				lfsck_pos_fill(env, lfsck,
					       &ns->ln_pos_first_inconsistent,
					       false);
		}
		rc = 0;
	}
	up_write(&com->lc_sem);

	if (obj != NULL && !IS_ERR(obj))
		lfsck_object_put(env, obj);
	return rc;
}

static int lfsck_namespace_assistant_handler_p2(const struct lu_env *env,
						struct lfsck_component *com)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct ptlrpc_thread	*thread = &lfsck->li_thread;
	struct lfsck_bookmark	*bk	= &lfsck->li_bookmark_ram;
	struct lfsck_namespace	*ns	= com->lc_file_ram;
	struct dt_object	*obj	= com->lc_obj;
	const struct dt_it_ops	*iops	= &obj->do_index_ops->dio_it;
	struct dt_object	*target;
	struct dt_it		*di;
	struct dt_key		*key;
	struct lu_fid		 fid;
	int			 rc;
	__u8			 flags	= 0;
	ENTRY;

	CDEBUG(D_LFSCK, "%s: namespace LFSCK phase2 scan start\n",
	       lfsck_lfsck2name(lfsck));

	com->lc_new_checked = 0;
	com->lc_new_scanned = 0;
	com->lc_time_last_checkpoint = cfs_time_current();
	com->lc_time_next_checkpoint = com->lc_time_last_checkpoint +
				cfs_time_seconds(LFSCK_CHECKPOINT_INTERVAL);

	di = iops->init(env, obj, 0, BYPASS_CAPA);
	if (IS_ERR(di))
		RETURN(PTR_ERR(di));

	fid_cpu_to_be(&fid, &ns->ln_fid_latest_scanned_phase2);
	rc = iops->get(env, di, (const struct dt_key *)&fid);
	if (rc < 0)
		GOTO(fini, rc);

	/* Skip the start one, which either has been processed or non-exist. */
	rc = iops->next(env, di);
	if (rc != 0)
		GOTO(put, rc);

	do {
		if (OBD_FAIL_CHECK(OBD_FAIL_LFSCK_DELAY3) &&
		    cfs_fail_val > 0) {
			struct l_wait_info lwi;

			lwi = LWI_TIMEOUT(cfs_time_seconds(cfs_fail_val),
					  NULL, NULL);
			l_wait_event(thread->t_ctl_waitq,
				     !thread_is_running(thread),
				     &lwi);

			if (unlikely(!thread_is_running(thread)))
				GOTO(put, rc = 0);
		}

		key = iops->key(env, di);
		fid_be_to_cpu(&fid, (const struct lu_fid *)key);
		if (!fid_is_sane(&fid)) {
			rc = 0;
			goto checkpoint;
		}

		target = lfsck_object_find(env, lfsck, &fid);
		if (IS_ERR(target)) {
			rc = PTR_ERR(target);
			goto checkpoint;
		}

		if (dt_object_exists(target)) {
			rc = iops->rec(env, di, (struct dt_rec *)&flags, 0);
			if (rc == 0) {
				rc = lfsck_namespace_double_scan_one(env, com,
								target, flags);
				if (rc == -ENOENT)
					rc = 0;
			}
		}

		lfsck_object_put(env, target);

checkpoint:
		down_write(&com->lc_sem);
		com->lc_new_checked++;
		com->lc_new_scanned++;
		ns->ln_fid_latest_scanned_phase2 = fid;
		if (rc > 0)
			ns->ln_objs_repaired_phase2++;
		else if (rc < 0)
			ns->ln_objs_failed_phase2++;
		up_write(&com->lc_sem);

		if (rc < 0 && bk->lb_param & LPF_FAILOUT)
			GOTO(put, rc);

		if (unlikely(cfs_time_beforeq(com->lc_time_next_checkpoint,
					      cfs_time_current())) &&
		    com->lc_new_checked != 0) {
			down_write(&com->lc_sem);
			ns->ln_run_time_phase2 +=
				cfs_duration_sec(cfs_time_current() +
				HALF_SEC - com->lc_time_last_checkpoint);
			ns->ln_time_last_checkpoint = cfs_time_current_sec();
			ns->ln_objs_checked_phase2 += com->lc_new_checked;
			com->lc_new_checked = 0;
			rc = lfsck_namespace_store(env, com, false);
			up_write(&com->lc_sem);
			if (rc != 0)
				GOTO(put, rc);

			com->lc_time_last_checkpoint = cfs_time_current();
			com->lc_time_next_checkpoint =
				com->lc_time_last_checkpoint +
				cfs_time_seconds(LFSCK_CHECKPOINT_INTERVAL);
		}

		lfsck_control_speed_by_self(com);
		if (unlikely(!thread_is_running(thread)))
			GOTO(put, rc = 0);

		rc = iops->next(env, di);
	} while (rc == 0);

	GOTO(put, rc);

put:
	iops->put(env, di);

fini:
	iops->fini(env, di);

	CDEBUG(D_LFSCK, "%s: namespace LFSCK phase2 scan stop: rc = %d\n",
	       lfsck_lfsck2name(lfsck), rc);

	return rc;
}

static void lfsck_namespace_assistant_fill_pos(const struct lu_env *env,
					       struct lfsck_component *com,
					       struct lfsck_position *pos)
{
	struct lfsck_assistant_data	*lad = com->lc_data;
	struct lfsck_namespace_req	*lnr;

	if (list_empty(&lad->lad_req_list))
		return;

	lnr = list_entry(lad->lad_req_list.next,
			 struct lfsck_namespace_req,
			 lnr_lar.lar_list);
	pos->lp_oit_cookie = lnr->lnr_oit_cookie;
	pos->lp_dir_cookie = lnr->lnr_dir_cookie - 1;
	pos->lp_dir_parent = *lfsck_dto2fid(lnr->lnr_obj);
}

static int lfsck_namespace_double_scan_result(const struct lu_env *env,
					      struct lfsck_component *com,
					      int rc)
{
	struct lfsck_instance	*lfsck	= com->lc_lfsck;
	struct lfsck_namespace	*ns	= com->lc_file_ram;

	down_write(&com->lc_sem);
	ns->ln_run_time_phase2 += cfs_duration_sec(cfs_time_current() +
				HALF_SEC - lfsck->li_time_last_checkpoint);
	ns->ln_time_last_checkpoint = cfs_time_current_sec();
	ns->ln_objs_checked_phase2 += com->lc_new_checked;
	com->lc_new_checked = 0;

	if (rc > 0) {
		if (ns->ln_flags & LF_INCOMPLETE)
			ns->ln_status = LS_PARTIAL;
		else
			ns->ln_status = LS_COMPLETED;
		if (!(lfsck->li_bookmark_ram.lb_param & LPF_DRYRUN))
			ns->ln_flags &= ~(LF_SCANNED_ONCE | LF_INCONSISTENT);
		ns->ln_time_last_complete = ns->ln_time_last_checkpoint;
		ns->ln_success_count++;
	} else if (rc == 0) {
		ns->ln_status = lfsck->li_status;
		if (ns->ln_status == 0)
			ns->ln_status = LS_STOPPED;
	} else {
		ns->ln_status = LS_FAILED;
	}

	rc = lfsck_namespace_store(env, com, false);
	up_write(&com->lc_sem);

	return rc;
}

static void lfsck_namespace_assistant_sync_failures(const struct lu_env *env,
						    struct lfsck_component *com,
						    struct lfsck_request *lr)
{
	/* XXX: TBD */
}

struct lfsck_assistant_operations lfsck_namespace_assistant_ops = {
	.la_handler_p1		= lfsck_namespace_assistant_handler_p1,
	.la_handler_p2		= lfsck_namespace_assistant_handler_p2,
	.la_fill_pos		= lfsck_namespace_assistant_fill_pos,
	.la_double_scan_result	= lfsck_namespace_double_scan_result,
	.la_req_fini		= lfsck_namespace_assistant_req_fini,
	.la_sync_failures	= lfsck_namespace_assistant_sync_failures,
};

/**
 * Verify the specified linkEA entry for the given directory object.
 * If the object has no such linkEA entry or it has more other linkEA
 * entries, then re-generate the linkEA with the given information.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] dev	pointer to the dt_device
 * \param[in] obj	pointer to the dt_object to be handled
 * \param[in] cname	the name for the child in the parent directory
 * \param[in] pfid	the parent directory's FID for the linkEA
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
int lfsck_verify_linkea(const struct lu_env *env, struct dt_device *dev,
			struct dt_object *obj, const struct lu_name *cname,
			const struct lu_fid *pfid)
{
	struct linkea_data	 ldata	= { 0 };
	struct lu_buf		 linkea_buf;
	struct thandle		*th;
	int			 rc;
	int			 fl	= LU_XATTR_CREATE;
	bool			 dirty	= false;
	ENTRY;

	LASSERT(S_ISDIR(lfsck_object_type(obj)));

	rc = lfsck_links_read(env, obj, &ldata);
	if (rc == -ENODATA) {
		dirty = true;
	} else if (rc == 0) {
		fl = LU_XATTR_REPLACE;
		if (ldata.ld_leh->leh_reccount != 1) {
			dirty = true;
		} else {
			rc = linkea_links_find(&ldata, cname, pfid);
			if (rc != 0)
				dirty = true;
		}
	}

	if (!dirty)
		RETURN(rc);

	rc = linkea_data_new(&ldata, &lfsck_env_info(env)->lti_linkea_buf);
	if (rc != 0)
		RETURN(rc);

	rc = linkea_add_buf(&ldata, cname, pfid);
	if (rc != 0)
		RETURN(rc);

	lfsck_buf_init(&linkea_buf, ldata.ld_buf->lb_buf,
		       ldata.ld_leh->leh_len);
	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	rc = dt_declare_xattr_set(env, obj, &linkea_buf,
				  XATTR_NAME_LINK, fl, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start_local(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	dt_write_lock(env, obj, 0);
	rc = dt_xattr_set(env, obj, &linkea_buf,
			  XATTR_NAME_LINK, fl, th, BYPASS_CAPA);
	dt_write_unlock(env, obj);

	GOTO(stop, rc);

stop:
	dt_trans_stop(env, dev, th);
	return rc;
}

/**
 * Get the name and parent directory's FID from the first linkEA entry.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] obj	pointer to the object which get linkEA from
 * \param[out] name	pointer to the buffer to hold the name
 *			in the first linkEA entry
 * \param[out] pfid	pointer to the buffer to hold the parent
 *			directory's FID in the first linkEA entry
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
int lfsck_links_get_first(const struct lu_env *env, struct dt_object *obj,
			  char *name, struct lu_fid *pfid)
{
	struct lu_name		 *cname = &lfsck_env_info(env)->lti_name;
	struct linkea_data	  ldata = { 0 };
	int			  rc;

	rc = lfsck_links_read(env, obj, &ldata);
	if (rc != 0)
		return rc;

	linkea_first_entry(&ldata);
	if (ldata.ld_lee == NULL)
		return -ENODATA;

	linkea_entry_unpack(ldata.ld_lee, &ldata.ld_reclen, cname, pfid);
	/* To guarantee the 'name' is terminated with '0'. */
	memcpy(name, cname->ln_name, cname->ln_namelen);
	name[cname->ln_namelen] = 0;

	return 0;
}

/**
 * Remove the name entry from the parent directory.
 *
 * No need to care about the object referenced by the name entry,
 * either the name entry is invalid or redundant, or the referenced
 * object has been processed has been or will be handled by others.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] lfsck	pointer to the lfsck instance
 * \param[in] parent	pointer to the lost+found object
 * \param[in] name	the name for the name entry to be removed
 * \param[in] type	the type for the name entry to be removed
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
int lfsck_remove_name_entry(const struct lu_env *env,
			    struct lfsck_instance *lfsck,
			    struct dt_object *parent,
			    const char *name, __u32 type)
{
	struct dt_device	*dev	= lfsck->li_next;
	struct thandle		*th;
	struct lustre_handle	 lh	= { 0 };
	int			 rc;
	ENTRY;

	rc = lfsck_ibits_lock(env, lfsck, parent, &lh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		RETURN(rc);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(unlock, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, parent, (const struct dt_key *)name, th);
	if (rc != 0)
		GOTO(stop, rc);

	if (S_ISDIR(type)) {
		rc = dt_declare_ref_del(env, parent, th);
		if (rc != 0)
			GOTO(stop, rc);
	}

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_delete(env, parent, (const struct dt_key *)name, th,
		       BYPASS_CAPA);
	if (rc != 0)
		GOTO(stop, rc);

	if (S_ISDIR(type)) {
		dt_write_lock(env, parent, 0);
		rc = dt_ref_del(env, parent, th);
		dt_write_unlock(env, parent);
	}

	GOTO(stop, rc);

stop:
	dt_trans_stop(env, dev, th);

unlock:
	lfsck_ibits_unlock(&lh, LCK_EX);

	CDEBUG(D_LFSCK, "%s: remove name entry "DFID"/%s "
	       "with type %o: rc = %d\n", lfsck_lfsck2name(lfsck),
	       PFID(lfsck_dto2fid(parent)), name, type, rc);

	return rc;
}

/**
 * Update the object's name entry with the given FID.
 *
 * \param[in] env	pointer to the thread context
 * \param[in] lfsck	pointer to the lfsck instance
 * \param[in] parent	pointer to the parent directory that holds
 *			the name entry
 * \param[in] name	the name for the entry to be updated
 * \param[in] pfid	the new PFID for the name entry
 * \param[in] type	the type for the name entry to be updated
 *
 * \retval		0 for success
 * \retval		negative error number on failure
 */
int lfsck_update_name_entry(const struct lu_env *env,
			    struct lfsck_instance *lfsck,
			    struct dt_object *parent, const char *name,
			    const struct lu_fid *pfid, __u32 type)
{
	struct dt_insert_rec	*rec	= &lfsck_env_info(env)->lti_dt_rec;
	struct dt_device	*dev	= lfsck->li_next;
	struct lustre_handle	 lh	= { 0 };
	struct thandle		*th;
	int			 rc;
	bool			 exists = true;
	ENTRY;

	rc = lfsck_ibits_lock(env, lfsck, parent, &lh,
			      MDS_INODELOCK_UPDATE, LCK_EX);
	if (rc != 0)
		RETURN(rc);

	th = dt_trans_create(env, dev);
	if (IS_ERR(th))
		GOTO(unlock, rc = PTR_ERR(th));

	rc = dt_declare_delete(env, parent, (const struct dt_key *)name, th);
	if (rc != 0)
		GOTO(stop, rc);

	rec->rec_type = type;
	rec->rec_fid = pfid;
	rc = dt_declare_insert(env, parent, (const struct dt_rec *)rec,
			       (const struct dt_key *)name, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_declare_ref_add(env, parent, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_trans_start(env, dev, th);
	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_delete(env, parent, (const struct dt_key *)name, th,
		       BYPASS_CAPA);
	if (rc == -ENOENT) {
		exists = false;
		rc = 0;
	}

	if (rc != 0)
		GOTO(stop, rc);

	rc = dt_insert(env, parent, (const struct dt_rec *)rec,
		       (const struct dt_key *)name, th, BYPASS_CAPA, 1);
	if (rc == 0 && S_ISDIR(type) && !exists) {
		dt_write_lock(env, parent, 0);
		rc = dt_ref_add(env, parent, th);
		dt_write_unlock(env, parent);
	}

	GOTO(stop, rc);

stop:
	dt_trans_stop(env, dev, th);

unlock:
	lfsck_ibits_unlock(&lh, LCK_EX);

	CDEBUG(D_LFSCK, "%s: update name entry "DFID"/%s with the FID "DFID
	       " and the type %o: rc = %d\n", lfsck_lfsck2name(lfsck),
	       PFID(lfsck_dto2fid(parent)), name, PFID(pfid), type, rc);

	return rc;
}

int lfsck_namespace_setup(const struct lu_env *env,
			  struct lfsck_instance *lfsck)
{
	struct lfsck_component	*com;
	struct lfsck_namespace	*ns;
	struct dt_object	*root = NULL;
	struct dt_object	*obj;
	int			 rc;
	ENTRY;

	LASSERT(lfsck->li_master);

	OBD_ALLOC_PTR(com);
	if (com == NULL)
		RETURN(-ENOMEM);

	INIT_LIST_HEAD(&com->lc_link);
	INIT_LIST_HEAD(&com->lc_link_dir);
	init_rwsem(&com->lc_sem);
	atomic_set(&com->lc_ref, 1);
	com->lc_lfsck = lfsck;
	com->lc_type = LFSCK_TYPE_NAMESPACE;
	com->lc_ops = &lfsck_namespace_ops;
	com->lc_data = lfsck_assistant_data_init(
			&lfsck_namespace_assistant_ops,
			"lfsck_namespace");
	if (com->lc_data == NULL)
		GOTO(out, rc = -ENOMEM);

	com->lc_file_size = sizeof(struct lfsck_namespace);
	OBD_ALLOC(com->lc_file_ram, com->lc_file_size);
	if (com->lc_file_ram == NULL)
		GOTO(out, rc = -ENOMEM);

	OBD_ALLOC(com->lc_file_disk, com->lc_file_size);
	if (com->lc_file_disk == NULL)
		GOTO(out, rc = -ENOMEM);

	root = dt_locate(env, lfsck->li_bottom, &lfsck->li_local_root_fid);
	if (IS_ERR(root))
		GOTO(out, rc = PTR_ERR(root));

	if (unlikely(!dt_try_as_dir(env, root)))
		GOTO(out, rc = -ENOTDIR);

	obj = local_index_find_or_create(env, lfsck->li_los, root,
					 lfsck_namespace_name,
					 S_IFREG | S_IRUGO | S_IWUSR,
					 &dt_lfsck_features);
	if (IS_ERR(obj))
		GOTO(out, rc = PTR_ERR(obj));

	com->lc_obj = obj;
	rc = obj->do_ops->do_index_try(env, obj, &dt_lfsck_features);
	if (rc != 0)
		GOTO(out, rc);

	rc = lfsck_namespace_load(env, com);
	if (rc > 0)
		rc = lfsck_namespace_reset(env, com, true);
	else if (rc == -ENODATA)
		rc = lfsck_namespace_init(env, com);
	if (rc != 0)
		GOTO(out, rc);

	ns = com->lc_file_ram;
	switch (ns->ln_status) {
	case LS_INIT:
	case LS_COMPLETED:
	case LS_FAILED:
	case LS_STOPPED:
		spin_lock(&lfsck->li_lock);
		list_add_tail(&com->lc_link, &lfsck->li_list_idle);
		spin_unlock(&lfsck->li_lock);
		break;
	default:
		CERROR("%s: unknown lfsck_namespace status %d\n",
		       lfsck_lfsck2name(lfsck), ns->ln_status);
		/* fall through */
	case LS_SCANNING_PHASE1:
	case LS_SCANNING_PHASE2:
		/* No need to store the status to disk right now.
		 * If the system crashed before the status stored,
		 * it will be loaded back when next time. */
		ns->ln_status = LS_CRASHED;
		/* fall through */
	case LS_PAUSED:
	case LS_CRASHED:
		spin_lock(&lfsck->li_lock);
		list_add_tail(&com->lc_link, &lfsck->li_list_scan);
		list_add_tail(&com->lc_link_dir, &lfsck->li_list_dir);
		spin_unlock(&lfsck->li_lock);
		break;
	}

	GOTO(out, rc = 0);

out:
	if (root != NULL && !IS_ERR(root))
		lu_object_put(env, &root->do_lu);
	if (rc != 0) {
		lfsck_component_cleanup(env, com);
		CERROR("%s: fail to init namespace LFSCK component: rc = %d\n",
		       lfsck_lfsck2name(lfsck), rc);
	}
	return rc;
}
