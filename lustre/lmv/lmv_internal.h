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
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _LMV_INTERNAL_H_
#define _LMV_INTERNAL_H_

#include <lustre/lustre_idl.h>
#include <obd.h>
#include <lustre_lmv.h>

#define LMV_MAX_TGT_COUNT 128

#define lmv_init_lock(lmv)   mutex_lock(&lmv->init_mutex);
#define lmv_init_unlock(lmv) mutex_unlock(&lmv->init_mutex);

#define LL_IT2STR(it)				        \
	((it) ? ldlm_it2str((it)->it_op) : "0")

int lmv_check_connect(struct obd_device *obd);

int lmv_intent_lock(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
                    ldlm_blocking_callback cb_blocking,
		    __u64 extra_lock_flags);

int lmv_intent_lookup(struct obd_export *exp, struct md_op_data *op_data,
                      void *lmm, int lmmsize, struct lookup_intent *it,
                      int flags, struct ptlrpc_request **reqp,
                      ldlm_blocking_callback cb_blocking,
		      __u64 extra_lock_flags);

int lmv_intent_open(struct obd_export *exp, struct md_op_data *op_data,
                    void *lmm, int lmmsize, struct lookup_intent *it,
                    int flags, struct ptlrpc_request **reqp,
                    ldlm_blocking_callback cb_blocking,
		    __u64 extra_lock_flags);

int lmv_blocking_ast(struct ldlm_lock *, struct ldlm_lock_desc *,
		     void *, int);
int lmv_fld_lookup(struct lmv_obd *lmv, const struct lu_fid *fid,
                   mdsno_t *mds);
int __lmv_fid_alloc(struct lmv_obd *lmv, struct lu_fid *fid,
                    mdsno_t mds);
int lmv_fid_alloc(struct obd_export *exp, struct lu_fid *fid,
                  struct md_op_data *op_data);

static inline struct lmv_tgt_desc *
lmv_get_target(struct lmv_obd *lmv, mdsno_t mds)
{
	int count = lmv->desc.ld_tgt_count;
	int i;

	for (i = 0; i < count; i++) {
		if (lmv->tgts[i] == NULL)
			continue;

		if (lmv->tgts[i]->ltd_idx == mds)
			return lmv->tgts[i];
	}

	return ERR_PTR(-ENODEV);
}

static inline struct lmv_tgt_desc *
lmv_find_target(struct lmv_obd *lmv, const struct lu_fid *fid)
{
        mdsno_t mds = 0;
        int rc;

        if (lmv->desc.ld_tgt_count > 1) {
                rc = lmv_fld_lookup(lmv, fid, &mds);
                if (rc)
                        return ERR_PTR(rc);
        }

        return lmv_get_target(lmv, mds);
}

static inline unsigned int
mea_last_char_hash(unsigned int count, const char *name, int namelen)
{
	unsigned int c;

	c = name[namelen - 1];
	if (c == 0)
		CWARN("invalid name %.*s\n", namelen, name);

	c = c % count;

	return c;
}

static inline unsigned int
mea_all_chars_hash(unsigned int count, const char *name, int namelen)
{
	unsigned int c = 0;

	while (--namelen >= 0)
		c += name[namelen];

	c = c % count;

	return c;
}

static inline int lmv_stripe_md_size(int stripe_count)
{
	struct lmv_stripe_md *lsm;

	return sizeof(*lsm) + stripe_count * sizeof(lsm->lsm_md_oinfo[0]);
}

int raw_name2idx(int hashtype, int count, const char *name, int namelen);

struct lmv_tgt_desc
*lmv_locate_mds(struct lmv_obd *lmv, struct md_op_data *op_data,
		struct lu_fid *fid);
/* lproc_lmv.c */
#ifdef LPROCFS
extern struct lprocfs_seq_vars lprocfs_lmv_obd_vars[];
#endif
extern struct file_operations lmv_proc_target_fops;

#endif
