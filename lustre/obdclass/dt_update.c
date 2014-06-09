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
			   int count, int *lens, __u64 batchid)
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
		  int count, int *lens, char **bufs, __u64 batchid)
{
	struct update	*update;
	char		*ptr;
	int		i;
	ENTRY;

	update = update_pack(env, ubuf, buf_len, op, fid, count, lens, batchid);
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
			   struct dt_object_format *dof, struct thandle *th)
{
	struct obdo		*obdo;
	int			sizes[2] = {sizeof(struct obdo), 0};
	int			buf_count = 1;
	struct lu_fid		*fid1 = NULL;
	struct update		*update;

	if (hint != NULL && hint->dah_parent) {
		fid1 = (struct lu_fid *)lu_object_fid(&hint->dah_parent->do_lu);
		sizes[1] = sizeof(*fid1);
		buf_count++;
	}

	update = update_pack(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_CREATE, lu_object_fid(&dt->do_lu), buf_count,
			     sizes, th->th_batchid);
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
			    struct thandle *th)
{
	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_REF_DEL, lu_object_fid(&dt->do_lu), 0, NULL,
			     NULL, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_ref_del);

int dt_trans_update_ref_add(const struct lu_env *env, struct dt_object *dt,
			    struct thandle *th)
{
	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_REF_ADD, lu_object_fid(&dt->do_lu), 0, NULL,
			     NULL, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_ref_add);

int dt_trans_update_attr_set(const struct lu_env *env, struct dt_object *dt,
			     const struct lu_attr *attr, struct thandle *th)
{
	struct update	*update;
	struct obdo	*obdo;
	struct lu_fid	*fid;
	int		 size = sizeof(struct obdo);
	ENTRY;

	fid = (struct lu_fid *)lu_object_fid(&dt->do_lu);
	update = update_pack(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_ATTR_SET, fid, 1, &size, th->th_batchid);
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
			      int flag, struct thandle *th)
{
	int	sizes[3] = {strlen(name) + 1, buf->lb_len, sizeof(int)};
	char	*bufs[3] = {(char *)name, (char *)buf->lb_buf, (char *)&flag};

	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_XATTR_SET, lu_object_fid(&dt->do_lu), 3,
			     sizes, bufs, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_xattr_set);

int dt_trans_update_index_insert(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_rec *rec,
				 const struct dt_key *key, struct thandle *th)
{
	int	sizes[2] = {strlen((char *)key) + 1, sizeof(struct lu_fid)};
	char	*bufs[2] = {(char *)key, (char *)rec};

	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_INDEX_INSERT, lu_object_fid(&dt->do_lu), 2,
			     sizes, bufs, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_index_insert);

int dt_trans_update_index_delete(const struct lu_env *env, struct dt_object *dt,
				 const struct dt_key *key, struct thandle *th)
{
	int	size = strlen((char *)key) + 1;
	char	*buf = (char *)key;

	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_INDEX_DELETE, lu_object_fid(&dt->do_lu), 1,
			     &size, &buf, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_index_delete);

int dt_trans_update_object_destroy(const struct lu_env *env,
				   struct dt_object *dt, struct thandle *th)
{
	return update_insert(env, th->th_update_buf, th->th_update_buf_size,
			     OBJ_DESTROY, lu_object_fid(&dt->do_lu), 0, NULL,
			     NULL, th->th_batchid);
}
EXPORT_SYMBOL(dt_trans_update_object_destroy);

/**
 * The following update funcs are only used by read-only ops, lookup,
 * getattr etc, so it does not need transaction here. Currently they 
 * are only used by OSP.
 **/
int dt_update_index_lookup(const struct lu_env *env, struct update_buf *ubuf,
			   int buffer_len, struct dt_object *dt,
			   struct dt_rec *rec, const struct dt_key *key)
{
	int	size = strlen((char *)key) + 1;
	char	*name = (char *)key;

	return update_insert(env, ubuf, buffer_len, OBJ_INDEX_LOOKUP,
			     lu_object_fid(&dt->do_lu), 1, &size,
			     (char **)&name, 0);
}
EXPORT_SYMBOL(dt_update_index_lookup);

int dt_update_attr_get(const struct lu_env *env, struct update_buf *ubuf,
		       int buffer_len, struct dt_object *dt) 
{
	return update_insert(env, ubuf, buffer_len, OBJ_ATTR_GET,
			     (struct lu_fid *)lu_object_fid(&dt->do_lu), 0,
			     NULL, NULL, 0);
}
EXPORT_SYMBOL(dt_update_attr_get);

int dt_update_xattr_get(const struct lu_env *env, struct update_buf *ubuf,
			int buffer_len, struct dt_object *dt, char *name)
{
	int	size;

	LASSERT(name != NULL);
	size = strlen(name) + 1;
	return update_insert(env, ubuf, UPDATE_BUFFER_SIZE, OBJ_XATTR_GET,
			     (struct lu_fid *)lu_object_fid(&dt->do_lu), 1,
			     &size, (char **)&name, 0);
}
EXPORT_SYMBOL(dt_update_xattr_get);
