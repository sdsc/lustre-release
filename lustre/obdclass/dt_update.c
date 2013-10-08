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
 * Copyright (c) 2013, Intel Corporation.
 */
/*
 * lustre/obdclass/dt_object.c
 *
 * These funcs are used by OSD/OSP to store update into the buffer.
 *
 * Author: di wang <di.wang@intel.com>
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd.h>
#include <obd_class.h>
#include <dt_object.h>
#include <lustre_fid.h>
#include <lustre_update.h>
#include <lustre_log.h>
#include "llog_internal.h"

struct update_opcode {
     __u32       opcode;
     const char *opname;
} update_opcode_table[OBJ_LAST] = {
        { OBJ_START,		"start" },
        { OBJ_CREATE,		"create" },
        { OBJ_DESTROY,		"destroy" },
        { OBJ_REF_ADD,		"ref_add" },
        { OBJ_REF_DEL,		"ref_del" },
	{ OBJ_ATTR_SET,		"attr_set" },
	{ OBJ_ATTR_GET,		"attr_get" },
	{ OBJ_XATTR_SET,	"xattr_set" },
	{ OBJ_XATTR_GET,	"xattr_get" },
	{ OBJ_INDEX_LOOKUP,	"lookup" },
	{ OBJ_INDEX_INSERT,	"insert" },
	{ OBJ_INDEX_DELETE,	"delete" },
	{ OBJ_LOG_CANCEL,	"log_cancel" },
};

const char *update_op_str(__u16 opcode)
{
	LASSERT(update_opcode_table[opcode].opcode == opcode);
	return update_opcode_table[opcode].opname;
}
EXPORT_SYMBOL(update_op_str);

/**
 * pack update into the update_buffer
 **/
struct update *update_pack(const struct lu_env *env, struct update_buf *ubuf,
			   int buf_len, int op, const struct lu_fid *fid,
			   int count, int *lens, __u64 batchid,
			   int index, int master_index)
{
	struct update        *update;
	int                   i;
	int                   update_length;
	ENTRY;

	LASSERT(ubuf != NULL && buf_len > 0);
	update = (struct update *)((char *)ubuf + update_buf_size(ubuf));
	/* Check update size to make sure it can fit into the buffer */
	update_length = cfs_size_round(offsetof(struct update, u_bufs[0]));
	for (i = 0; i < count; i++)
		update_length += cfs_size_round(lens[i]);

	if (update_buf_size(ubuf) + update_length > buf_len ||
	    count > UPDATE_PARAM_COUNT) {
		CERROR("insert up %p, idx %d ucnt %d ulen %lu, count %d:"
		       " rc = %d\n", ubuf, update_length, ubuf->ub_count,
			update_buf_size(ubuf), count, -E2BIG);
		RETURN(ERR_PTR(-E2BIG));
	}

	/* fill the update into the update buffer */
	update->u_fid = *fid;
	update->u_type = op;
	update->u_batchid = batchid;
	update->u_index = index;
	update->u_master_index = master_index;
	for (i = 0; i < count; i++)
		update->u_lens[i] = lens[i];

	ubuf->ub_count++;

	CDEBUG(D_INFO, "%p "DFID" idx %d: op %s params %d:%lu\n", ubuf,
	       PFID(fid), ubuf->ub_count, update_op_str((__u16)op), count,
	       update_buf_size(ubuf));

	RETURN(update);
}
EXPORT_SYMBOL(update_pack);

int update_insert(const struct lu_env *env, struct update_buf *ubuf,
		  int buf_len, int op, const struct lu_fid *fid,
		  int count, int *lens, char **bufs, __u64 batchid,
		  int index, int master_index)
{
	struct update	*update;
	char		*ptr;
	int		i;
	ENTRY;

	update = update_pack(env, ubuf, buf_len, op, fid, count, lens,
			     batchid, index, master_index);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	ptr = (char *)update + cfs_size_round(offsetof(struct update,
						       u_bufs[0]));
	for (i = 0; i < count; i++)
		LOGL(bufs[i], lens[i], ptr);

	RETURN(0);
}
EXPORT_SYMBOL(update_insert);

int dt_trans_update_create(const struct lu_env *env, struct dt_object *dt,
			   struct lu_attr *attr, struct dt_allocation_hint *hint,
			   struct dt_object_format *dof, int index,
			   struct thandle *th)
{
	struct obdo		*obdo;
	int			sizes[2] = {sizeof(struct obdo), 0};
	int			buf_count = 1;
	struct lu_fid		*fid1 = NULL;
	struct update		*update;
	ENTRY;

	if (hint != NULL && hint->dah_parent) {
		fid1 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	update = update_pack(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_CREATE,
			     lu_object_fid(&dt->do_lu), buf_count, sizes,
			     th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	obdo = (struct obdo *)update_param_buf(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);
	if (fid1 != NULL) {
		struct lu_fid *fid;
		fid = (struct lu_fid *)update_param_buf(update, 1, NULL);
		fid_cpu_to_le(fid, fid1);
	}

	RETURN(0);
}
EXPORT_SYMBOL(dt_trans_update_create);

int dt_trans_update_ref_del(const struct lu_env *env, struct dt_object *dt,
			    int index, struct thandle *th)
{
	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_REF_DEL,
			     lu_object_fid(&dt->do_lu), 0, NULL, NULL,
			     th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_ref_del);

int dt_trans_update_ref_add(const struct lu_env *env, struct dt_object *dt,
			    int index, struct thandle *th)
{
	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_REF_ADD,
			     lu_object_fid(&dt->do_lu), 0, NULL, NULL,
			     th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_ref_add);

int dt_trans_update_attr_set(const struct lu_env *env, struct dt_object *dt,
			     const struct lu_attr *attr, int index,
			     struct thandle *th)
{
	struct update	*update;
	struct obdo	*obdo;
	struct lu_fid	*fid;
	int		 size = sizeof(struct obdo);
	ENTRY;

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	update = update_pack(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_ATTR_SET,
			     fid, 1, &size, th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
	if (IS_ERR(update))
		RETURN(PTR_ERR(update));

	obdo = (struct obdo *)update_param_buf(update, 0, NULL);
	obdo->o_valid = 0;
	obdo_from_la(obdo, (struct lu_attr *)attr, attr->la_valid);
	lustre_set_wire_obdo(NULL, obdo, obdo);

	RETURN(0);
}
EXPORT_SYMBOL(dt_trans_update_attr_set);

int dt_trans_update_xattr_set(const struct lu_env *env, struct dt_object *dt,
			      const struct lu_buf *buf, const char *name,
			      int flag, int index, struct thandle *th)
{
	int	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(int)};
	char	*bufs[3] = {(char *)name, (char *)buf->lb_buf, (char *)&flag};

	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_XATTR_SET,
			     lu_object_fid(&dt->do_lu), 3, sizes, bufs,
			     th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_xattr_set);

int dt_trans_update_index_insert(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key, int index,
				 struct thandle *th)
{
	int	sizes[2] = {strlen((char *)key) + 1, sizeof(struct lu_fid)};
	char	*bufs[2] = {(char *)key, (char *)rec};

	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size,
			     OBJ_INDEX_INSERT, lu_object_fid(&dt->do_lu), 2,
			     sizes, bufs, th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_index_insert);

int dt_trans_update_index_delete(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_key *key, int index,
				 struct thandle *th)
{
	int	size = strlen((char *)key) + 1;
	char	*buf = (char *)key;

	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size,
			     OBJ_INDEX_DELETE, lu_object_fid(&dt->do_lu), 1,
			     &size, &buf, th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_index_delete);

int dt_trans_update_object_destroy(const struct lu_env *env,
				   struct dt_object *dt, int index,
				   struct thandle *th)
{
	return update_insert(env, th->th_update->tu_update_buf,
			     th->th_update->tu_update_buf_size, OBJ_DESTROY,
			     lu_object_fid(&dt->do_lu), 0, NULL, NULL,
			     th->th_update->tu_batchid, index,
			     th->th_update->tu_master_index);
}
EXPORT_SYMBOL(dt_trans_update_object_destroy);

struct obd_llog_group *dt_update_find_olg(struct dt_device *dt, int index)
{
	struct obd_device	*obd = dt->dd_lu_dev.ld_obd;
	struct obd_llog_group	*olg;
	struct obd_llog_group	*found = NULL;

	down_read(&obd->obd_olg_list_sem);
	cfs_list_for_each_entry(olg, &obd->obd_olg_list, olg_list) {
		if (olg->olg_seq == index) {
			found = olg;
			break;
		}
	}
	up_read(&obd->obd_olg_list_sem);
	return found;
}
EXPORT_SYMBOL(dt_update_find_olg);

static int dt_update_add_olg(struct dt_device *dt, struct obd_llog_group *olg)
{
	struct obd_device	*obd = dt->dd_lu_dev.ld_obd;
	struct obd_llog_group	*tmp;
	int rc = 0;

	down_write(&obd->obd_olg_list_sem);
	cfs_list_for_each_entry(tmp, &obd->obd_olg_list, olg_list) {
		if (tmp->olg_seq == olg->olg_seq) {
			rc = -EEXIST;
			break;
		}
	}
	if (rc == 0)
		cfs_list_add_tail(&olg->olg_list, &obd->obd_olg_list);
	up_write(&obd->obd_olg_list_sem);
	RETURN(rc);
}

int dt_trans_update_declare_llog_add(const struct lu_env *env,
				     struct dt_device *dt, struct thandle *th,
				     int index)
{
	struct llog_thread_info	*info = llog_info(env);
	struct obd_llog_group	*olg;
	struct llog_ctxt	*ctxt;
	struct llog_rec_hdr	*hdr = &info->lgi_lrh;
	int			rc;

	olg = dt_update_find_olg(dt, index);
	LASSERTF(olg != NULL, "%s: olg %d does not exist!\n",
		dt->dd_lu_dev.ld_obd->obd_name, index);
	ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL)
		return -ENXIO;

	hdr->lrh_len = llog_data_len(UPDATE_BUFFER_SIZE);
	rc = llog_declare_add(env, ctxt->loc_handle, hdr, th);
	llog_ctxt_put(ctxt);
	return rc;
}
EXPORT_SYMBOL(dt_trans_update_declare_llog_add);

int dt_update_llog_init(const struct lu_env *env, struct dt_device *dt,
			int index, struct llog_operations *logops)
{
	struct llog_thread_info		*info = llog_info(env);
	struct lu_fid			fid;
	struct llog_catid		*cid = &info->lgi_cid;
	struct llog_handle		*lgh = NULL;
	struct obd_device		*obd = dt->dd_lu_dev.ld_obd;
	struct llog_ctxt		*ctxt;
	struct obd_llog_group		*olg;
	int				rc;
	ENTRY;

	olg = dt_update_find_olg(dt, index);
	if (olg != NULL)
		RETURN(0);

	OBD_ALLOC_PTR(olg);
	if (olg == NULL)
		RETURN(-ENOMEM);

	llog_group_init(olg, index);

	OBD_SET_CTXT_MAGIC(&obd->obd_lvfs_ctxt);
	obd->obd_lvfs_ctxt.dt = dt;
	lu_local_obj_fid(&fid, UPDATE_LLOG_CATALOGS_OID);
	rc = llog_osd_get_cat_list(env, dt, index, 1, cid, &fid);
	if (rc) {
		CERROR("%s: can't get id from catalogs: rc = %d\n",
		       obd->obd_name, rc);
		RETURN(rc);
	}

	rc = llog_setup(env, obd, olg, LLOG_UPDATE_ORIG_CTXT, obd, logops);
	if (rc)
		RETURN(rc);

        ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	LASSERT(ctxt);

	if (likely(logid_id(&cid->lci_logid) != 0)) {
		rc = llog_open(env, ctxt, &lgh, &cid->lci_logid, NULL,
			       LLOG_OPEN_EXISTS);
		/* re-create llog if it is missing */
		if (rc == -ENOENT)
			logid_set_id(&cid->lci_logid, 0);
		else if (rc < 0)
			GOTO(out_cleanup, rc);
	}

	if (unlikely(logid_id(&cid->lci_logid) == 0)) {
		rc = llog_open_create(env, ctxt, &lgh, NULL, NULL);
		if (rc < 0)
			GOTO(out_cleanup, rc);
		cid->lci_logid = lgh->lgh_id;
	}

	LASSERT(lgh != NULL);
	ctxt->loc_handle = lgh;
	rc = llog_cat_init_and_process(env, ctxt->loc_handle);
	if (rc)
		GOTO(out_close, rc);

	rc = llog_osd_put_cat_list(env, dt, index, 1, cid, &fid);
	if (rc)
		GOTO(out_close, rc);

	rc = dt_update_add_olg(dt, olg);
	if (rc != 0)
		GOTO(out_close, rc);

	llog_ctxt_put(ctxt);
	RETURN(0);
out_close:
	llog_cat_close(env, lgh);
out_cleanup:
	llog_cleanup(env, ctxt);
	OBD_FREE_PTR(olg);
	RETURN(rc);
}
EXPORT_SYMBOL(dt_update_llog_init);

void dt_update_llog_fini(const struct lu_env *env, struct dt_device *dt,
			 int index)
{
	struct obd_device	*obd = dt->dd_lu_dev.ld_obd;
	struct obd_llog_group	*olg;
	struct obd_llog_group	*found = NULL;
	struct llog_ctxt	*ctxt;

	down_write(&obd->obd_olg_list_sem);
	cfs_list_for_each_entry(olg, &obd->obd_olg_list, olg_list) {
		if (olg->olg_seq == index) {
			found = olg;
			break;
		}
	}
	if (found != NULL)
		cfs_list_del_init(&found->olg_list);
	up_write(&obd->obd_olg_list_sem);
	if (found == NULL)
		return;

	ctxt = llog_group_get_ctxt(found, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL)
		return;
	llog_cat_close(env, ctxt->loc_handle);
	llog_cleanup(env, ctxt);
	OBD_FREE_PTR(found);
	return;
}
EXPORT_SYMBOL(dt_update_llog_fini);

int dt_trans_update_llog_add(const struct lu_env *env, struct dt_device *dt,
			     struct update_buf *ubuf,
			     struct llog_cookie *cookie, int index,
			     struct thandle *th)
{
	struct llog_thread_info		*info = llog_info(env);
	struct llog_updatelog_rec	*rec = &info->lgi_update_lrec;
	struct lu_buf			*lbuf = &info->lgi_update_lb;
	struct obd_llog_group		*olg;
	struct llog_ctxt		*ctxt;
	int				reclen;
	int				rc;
	ENTRY;

	reclen = llog_data_len(sizeof(*rec) + update_buf_size(ubuf));
	lbuf = lu_buf_check_and_alloc(lbuf, reclen);
	if (lbuf->lb_buf == NULL)
		RETURN(-ENOMEM);

	rec = lbuf->lb_buf;
	rec->ur_hdr.lrh_len = sizeof(struct llog_rec_hdr) +
			      sizeof(struct llog_rec_tail) +
			      llog_data_len(update_buf_size(ubuf));
	rec->ur_hdr.lrh_type = UPDATE_REC;
	update_dump_buf(ubuf, D_INFO);
	memcpy(&rec->urb, ubuf, update_buf_size(ubuf));
	update_buf_cpu_to_le(&rec->urb, &rec->urb);

	olg = dt_update_find_olg(dt, index);
	LASSERTF(olg != NULL, "olg %d does not exist!\n", index);
	ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL)
		RETURN(-ENXIO);

	LASSERT(ctxt->loc_handle != NULL);
	rc = llog_add(env, ctxt->loc_handle, &rec->ur_hdr, cookie, NULL, th);
	llog_ctxt_put(ctxt);
	if (rc > 0)
		rc = 0;
	RETURN(rc);
}
EXPORT_SYMBOL(dt_trans_update_llog_add);

int dt_update_llog_cancel(const struct lu_env *env, struct dt_device *dt,
			  struct llog_cookie *cookie, int index)
{
	struct obd_llog_group	*olg;
	struct llog_ctxt	*ctxt;
	int			rc;
	ENTRY;

	olg = dt_update_find_olg(dt, index);
	LASSERTF(olg != NULL, "olg %d does not exist!\n", index);
	ctxt = llog_group_get_ctxt(olg, LLOG_UPDATE_ORIG_CTXT);
	if (ctxt == NULL)
		RETURN(-ENXIO);

	LASSERT(ctxt->loc_handle != NULL);
	rc = llog_cat_cancel_records(env, ctxt->loc_handle, 1, cookie);
	llog_ctxt_put(ctxt);
	RETURN(rc);
}
EXPORT_SYMBOL(dt_update_llog_cancel);

/**
 * The following update funcs are only used by read-only ops, lookup,
 * getattr etc, so it does not need transaction here. Currently they 
 * are only used by OSP.
 **/
int dt_update_index_lookup(const struct lu_env *env, struct update_buf *ubuf,
			   int buffer_len, struct dt_object *dt,
			   struct dt_rec *rec, const struct dt_key *key,
			   int index, int master_index)
{
	int	size = strlen((char *)key) + 1;
	char	*name = (char *)key;

	return update_insert(env, ubuf, buffer_len, OBJ_INDEX_LOOKUP,
			     lu_object_fid(&dt->do_lu), 1, &size,
			     (char **)&name, 0, index, master_index);
}
EXPORT_SYMBOL(dt_update_index_lookup);

int dt_update_attr_get(const struct lu_env *env, struct update_buf *ubuf,
		       int buffer_len, struct dt_object *dt, int index,
		       int master_index)
{
	return update_insert(env, ubuf, buffer_len, OBJ_ATTR_GET,
			     (struct lu_fid *)lu_object_fid(&dt->do_lu), 0,
			     NULL, NULL, 0, index, master_index);
}
EXPORT_SYMBOL(dt_update_attr_get);

int dt_update_xattr_get(const struct lu_env *env, struct update_buf *ubuf,
			int buffer_len, struct dt_object *dt, char *name,
			int index, int master_index)
{
	int	size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;
	return update_insert(env, ubuf, UPDATE_BUFFER_SIZE, OBJ_XATTR_GET,
			     (struct lu_fid *)lu_object_fid(&dt->do_lu), 1,
			     &size, (char **)&name, 0, index, master_index);
}
EXPORT_SYMBOL(dt_update_xattr_get);

void dt_update_xid(struct update_buf *ubuf, int index, __u64 xid)
{
	struct update *update;

	LASSERT(index < ubuf->ub_count);
	update = update_buf_get(ubuf, index, NULL);
	LASSERT(update != NULL);
	update->u_xid = xid;
}
EXPORT_SYMBOL(dt_update_xid);

int dt_trans_update_hook_stop(const struct lu_env *env, struct thandle *th)
{
	struct thandle_update	 *tu = th->th_update;
        struct thandle_update_dt *tud;
        int			 rc = 0;
	ENTRY;

        if (tu == NULL)
                RETURN(0);

	/* local dt call back first */
	if (tu->tu_txn_stop_cb != NULL) {
		rc = tu->tu_txn_stop_cb(env, th, tu->tu_cb_data);
		if (rc < 0)
			RETURN(rc);
	}

	/* then remote dt call back */
        cfs_list_for_each_entry(tud, &tu->tu_remote_update_list, tud_list) {
                if (tud->tud_txn_stop_cb == NULL)
                        continue;

                rc = tud->tud_txn_stop_cb(env, th, tud->tud_cb_data);
                if (rc < 0)
                        break;
	}

	RETURN(rc);
}
EXPORT_SYMBOL(dt_trans_update_hook_stop);
