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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2012 Whamcloud, Inc.
 * Use is subject to license terms.
 *
 * Author: Johann Lombardi <johann.lombardi@intel.com>
 * Author: Niu    Yawei    <yawei.niu@intel.com>
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#define DEBUG_SUBSYSTEM S_LQUOTA

#include "qsd_internal.h"

/*
 * Enforce quota, it's called in the declaration of each operation.
 * qsd_op_end() will then be called later once all the operations have been
 * completed in order to release/adjust the quota space.
 *
 * \param env        - the environment passed by the caller
 * \param qsd        - is the qsd instance associated with the device in charge
 *                     of the operation.
 * \param trans      - is the quota transaction information
 * \param qi         - qid & space required by current operation
 * \param flags      - if the operation is write, return caller no user/group
 *                     and sync commit flags
 *
 * \retval 0        - success
 * \retval -EDQUOT      : out of quota
 *         -EINPROGRESS : inform client to retry write
 *         -ve          : other appropriate errors
 */
int qsd_op_begin(const struct lu_env *env, struct qsd_instance *qsd,
		 struct lquota_trans *trans, struct lquota_id_info *qi,
		 int *flags)
{
	return 0;
}
EXPORT_SYMBOL(qsd_op_begin);

/*
 * Post quota operation. It's called after each operation transaction stopped.
 *
 * \param  env   - the environment passed by the caller
 * \param  qsd   - is the qsd instance associated with device which is handling
 *                 the operation.
 * \param  qids  - all qids information attached in the transaction handle
 * \param  count - is the number of qid entries in the qids array.
 *
 * \retval 0     - success
 * \retval -ve   - failure
 */
void qsd_op_end(const struct lu_env *env, struct qsd_instance *qsd,
		struct lquota_trans *trans)
{
}
EXPORT_SYMBOL(qsd_op_end);
