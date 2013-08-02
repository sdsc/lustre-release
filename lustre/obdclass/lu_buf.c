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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2013, Intel Corporation.
 */
/*
 * Lustre Buffers.
 * These are the only exported functions, they provide some generic
 * infrastructure for managing buffers.
 */

#define DEBUG_SUBSYSTEM S_CLASS

#include <obd_support.h>
#include <lu_object.h>

struct lu_buf LU_BUF_NULL = {
	.lb_buf = NULL,
	.lb_len = 0
};
EXPORT_SYMBOL(LU_BUF_NULL);

void lu_buf_free(struct lu_buf *buf)
{
	LASSERT(buf);
	if (buf->lb_buf) {
		LASSERT(buf->lb_len > 0);
		OBD_FREE_LARGE(buf->lb_buf, buf->lb_len);
		buf->lb_buf = NULL;
		buf->lb_len = 0;
	}
}
EXPORT_SYMBOL(lu_buf_free);

void lu_buf_alloc(struct lu_buf *buf, int size)
{
	LASSERT(buf);
	LASSERT(buf->lb_buf == NULL);
	LASSERT(buf->lb_len == 0);
	OBD_ALLOC_LARGE(buf->lb_buf, size);
	if (likely(buf->lb_buf))
		buf->lb_len = size;
}
EXPORT_SYMBOL(lu_buf_alloc);

void lu_buf_realloc(struct lu_buf *buf, int size)
{
	lu_buf_free(buf);
	lu_buf_alloc(buf, size);
}
EXPORT_SYMBOL(lu_buf_realloc);

struct lu_buf *lu_buf_check_and_alloc(struct lu_buf *buf, int len)
{
	if (buf->lb_buf == NULL && buf->lb_len == 0)
		lu_buf_alloc(buf, len);

	if ((len > buf->lb_len) && (buf->lb_buf != NULL))
		lu_buf_realloc(buf, len);

	return buf;
}
EXPORT_SYMBOL(lu_buf_check_and_alloc);

/**
 * Increase the size of the \a buf.
 * preserves old data in buffer
 * old buffer remains unchanged on error
 * \retval 0 or -ENOMEM
 */
int lu_buf_check_and_grow(struct lu_buf *buf, int len)
{
	char *ptr;

	if (len <= buf->lb_len)
		return 0;

	OBD_ALLOC_LARGE(ptr, len);
	if (ptr == NULL)
		return -ENOMEM;

	/* Free the old buf */
	if (buf->lb_buf != NULL) {
		memcpy(ptr, buf->lb_buf, buf->lb_len);
		OBD_FREE_LARGE(buf->lb_buf, buf->lb_len);
	}

	buf->lb_buf = ptr;
	buf->lb_len = len;
	return 0;
}
EXPORT_SYMBOL(lu_buf_check_and_grow);
