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
 * Copyright 2015 Cray Inc, all rights reserved.
 * Author: Ben Evans.
 *
 * Code moved from lustre_idl.h
 *
 * Define lu_seq_range, lu_seq_range_array and associated functions
 *
 */

#ifndef _SEQ_RANGE_H_
#define _SEQ_RANGE_H_

/**
 * Describes a range of sequence, lsr_start is included but lsr_end is
 * not in the range.
 * Same structure is used in fld module where lsr_index field holds mdt id
 * of the home mdt.
 */
struct lu_seq_range {
	__u64 lsr_start;
	__u64 lsr_end;
	__u32 lsr_index;
	__u32 lsr_flags;
};

struct lu_seq_range_array {
	__u32 lsra_count;
	__u32 lsra_padding;
	struct lu_seq_range lsra_lsr[0];
};

#define LU_SEQ_RANGE_MDT	0x0
#define LU_SEQ_RANGE_OST	0x1
#define LU_SEQ_RANGE_ANY	0x3

#define LU_SEQ_RANGE_MASK	0x3

static inline unsigned fld_range_type(const struct lu_seq_range *range)
{
	return range->lsr_flags & LU_SEQ_RANGE_MASK;
}

static inline bool fld_range_is_ost(const struct lu_seq_range *range)
{
	return fld_range_type(range) == LU_SEQ_RANGE_OST;
}

static inline bool fld_range_is_mdt(const struct lu_seq_range *range)
{
	return fld_range_type(range) == LU_SEQ_RANGE_MDT;
}

/**
 * This all range is only being used when fld client sends fld query request,
 * but it does not know whether the seq is MDT or OST, so it will send req
 * with ALL type, which means either seq type gotten from lookup can be
 * expected.
 */
static inline unsigned fld_range_is_any(const struct lu_seq_range *range)
{
	return fld_range_type(range) == LU_SEQ_RANGE_ANY;
}

static inline void fld_range_set_type(struct lu_seq_range *range,
				      unsigned flags)
{
	range->lsr_flags |= flags;
}

static inline void fld_range_set_mdt(struct lu_seq_range *range)
{
	fld_range_set_type(range, LU_SEQ_RANGE_MDT);
}

static inline void fld_range_set_ost(struct lu_seq_range *range)
{
	fld_range_set_type(range, LU_SEQ_RANGE_OST);
}

static inline void fld_range_set_any(struct lu_seq_range *range)
{
	fld_range_set_type(range, LU_SEQ_RANGE_ANY);
}

/**
 * returns  width of given range \a r
 */

static inline __u64 range_space(const struct lu_seq_range *range)
{
	return range->lsr_end - range->lsr_start;
}

/**
 * initialize range to zero
 */

static inline void range_init(struct lu_seq_range *range)
{
	memset(range, 0, sizeof(*range));
}

/**
 * check if given seq id \a s is within given range \a r
 */

static inline bool range_within(const struct lu_seq_range *range,
				__u64 s)
{
	return s >= range->lsr_start && s < range->lsr_end;
}

static inline bool range_is_sane(const struct lu_seq_range *range)
{
	return range->lsr_end >= range->lsr_start;
}

static inline bool range_is_zero(const struct lu_seq_range *range)
{
	return range->lsr_start == 0 && range->lsr_end == 0;
}

static inline bool range_is_exhausted(const struct lu_seq_range *range)
{
	return range_space(range) == 0;
}

/* return 0 if two range have the same location */
static inline int range_compare_loc(const struct lu_seq_range *r1,
				    const struct lu_seq_range *r2)
{
	return r1->lsr_index != r2->lsr_index ||
		r1->lsr_flags != r2->lsr_flags;
}

#if !defined(__REQ_LAYOUT_USER__)
extern void lustre_swab_lu_seq_range(struct lu_seq_range *range);
#endif

#define DRANGE "[%#16.16"LPF64"x-%#16.16"LPF64"x):%x:%s"

#define PRANGE(range)		\
	(range)->lsr_start,	\
	(range)->lsr_end,	\
	(range)->lsr_index,	\
	fld_range_is_mdt(range) ? "mdt" : "ost"

#endif
