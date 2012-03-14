/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, Whamcloud, Inc.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/libcfs_string.h
 *
 * Generic string manipulation functions.
 *
 * Author: Nathan Rutman <nathan.rutman@sun.com>
 */

#ifndef __LIBCFS_STRING_H__
#define __LIBCFS_STRING_H__

/* libcfs_string.c */
/* Convert a text string to a bitmask */
int cfs_str2mask(const char *str, const char *(*bit2str)(int bit),
                 int *oldmask, int minmask, int allmask);

/* Allocate space for and copy an existing string.
 * Must free with cfs_free().
 */
char *cfs_strdup(const char *str, u_int32_t flags);

/* safe vsnprintf */
int cfs_vsnprintf(char *buf, size_t size, const char *fmt, va_list args);

/* safe snprintf */
int cfs_snprintf(char *buf, size_t size, const char *fmt, ...);

/* trim leading and trailing space characters */
char *cfs_firststr(char *str, size_t size);

/**
 * Structure to represent NULL-less strings.
 */
struct cfs_lstr {
        char           *ls_str;
        int             ls_len;
};

/**
 * Structure to represent \<range_expr\> token of the syntax.
 */
struct cfs_range_expr {
        /**
         * Link to cfs_num_exprs::el_exprs.
         */
        cfs_list_t      re_link;
        __u32           re_lo;
        __u32           re_hi;
        __u32           re_stride;
};

struct cfs_num_exprs {
        cfs_list_t      el_link;
        cfs_list_t      el_exprs;
};

static inline int
cfs_iswhite(char c)
{
        switch (c) {
        case ' ':
        case '\t':
        case '\n':
        case '\r':
                return 1;
        default:
                break;
        }
        return 0;
}

char *cfs_trimwhite(char *str);
int cfs_gettok(struct cfs_lstr *next, char delim, struct cfs_lstr *res);
int cfs_str2num_check(const char *str, int nob,
                      unsigned *num, unsigned min, unsigned max);
int cfs_range_expr_parse(struct cfs_lstr *src, unsigned min, unsigned max,
                         int single_tok, struct cfs_range_expr **expr);
int cfs_range_expr_list_match(__u32 value, cfs_list_t *expr_list);
void cfs_range_expr_list_free(cfs_list_t *expr_list);
void cfs_range_expr_list_print(cfs_list_t *expr_list);
int cfs_num_exprs_parse(char *str, int len, unsigned min, unsigned max,
                        struct cfs_num_exprs **epp);
int cfs_num_exprs_match(__u32 value, struct cfs_num_exprs *exprs);
void cfs_num_exprs_list_free(cfs_list_t *list);
int cfs_ip_addr_parse(char *str, int len, cfs_list_t *list);
int cfs_ip_addr_match(__u32 addr, cfs_list_t *list);
void cfs_ip_addr_free(cfs_list_t *list);

#endif
