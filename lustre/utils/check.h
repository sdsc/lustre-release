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
 * Copyright (c) 2016, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _CHECK_H_
#define _CHECK_H_

#include <stdio.h>

#define BLANK_LINE()						\
do {								\
	printf("\n");						\
} while (0)

#define COMMENT(c)						\
do {								\
	printf("	/* "c" */\n");				\
} while (0)

#define STRINGIFY(a) #a

#define CHECK_CDEFINE(a)					\
	printf("	CLASSERT("#a" == "STRINGIFY(a) ");\n")

#define CHECK_CVALUE(a)					 \
	printf("	CLASSERT("#a" == %lld);\n", (long long)a)

#define CHECK_CVALUE_X(a)					\
	printf("	CLASSERT("#a" == 0x%.8x);\n", a)

#define CHECK_DEFINE(a)						\
do {								\
	printf("	LASSERTF("#a" == "STRINGIFY(a)		\
		", \"found %%lld\\n\",\n			"\
		"(long long)"#a");\n");				\
} while (0)

#define CHECK_DEFINE_X(a)					\
do {								\
	printf("	LASSERTF("#a" == "STRINGIFY(a)		\
		", \"found 0x%%.8x\\n\",\n		"#a	\
		");\n");					\
} while (0)

#define CHECK_DEFINE_64X(a)					\
do {								\
	printf("	LASSERTF("#a" == "STRINGIFY(a)		\
		", \"found 0x%%.16llxULL\\n\",\n		"\
		" "#a");\n");					\
} while (0)

#define CHECK_VALUE(a)						\
do {								\
	printf("	LASSERTF("#a				\
		" == %lld, \"found %%lld\\n\",\n		"\
		" (long long)"#a");\n", (long long)a);		\
} while (0)

#define CHECK_VALUE_X(a)					\
do {								\
	printf("	LASSERTF("#a				\
		" == 0x%.8xUL, \"found 0x%%.8xUL\\n\",\n	"\
		"	(unsigned)"#a");\n", (unsigned)a);	\
} while (0)

#define CHECK_VALUE_O(a)					\
do {								\
	printf("	LASSERTF("#a				\
		" == 0%.11oUL, \"found 0%%.11oUL\\n\",\n	"\
		"	"#a");\n", a);				\
} while (0)

#define CHECK_VALUE_64X(a)					\
do {								\
	printf("	LASSERTF("#a" == 0x%.16llxULL, "	\
		"\"found 0x%%.16llxULL\\n\",\n			"\
		"(long long)"#a");\n", (long long)a);		\
} while (0)

#define CHECK_VALUE_64O(a)					\
do {								\
	printf("	LASSERTF("#a" == 0%.22lloULL, "		\
		"\"found 0%%.22lloULL\\n\",\n			"\
		"(long long)"#a");\n", (long long)a);		\
} while (0)

#define CHECK_MEMBER_OFFSET(s, m)				\
do {								\
	CHECK_VALUE((int)offsetof(struct s, m));		\
} while (0)

#define CHECK_MEMBER_OFFSET_TYPEDEF(s, m)			\
do {								\
	CHECK_VALUE((int)offsetof(s, m));			\
} while (0)

#define CHECK_MEMBER_SIZEOF(s, m)				\
do {								\
	CHECK_VALUE((int)sizeof(((struct s *)0)->m));		\
} while (0)

#define CHECK_MEMBER_SIZEOF_TYPEDEF(s, m)			\
do {								\
	CHECK_VALUE((int)sizeof(((s *)0)->m));			\
} while (0)

#define CHECK_MEMBER(s, m)					\
do {								\
	CHECK_MEMBER_OFFSET(s, m);				\
	CHECK_MEMBER_SIZEOF(s, m);				\
} while (0)

#define CHECK_MEMBER_TYPEDEF(s, m)				\
do {								\
	CHECK_MEMBER_OFFSET_TYPEDEF(s, m);			\
	CHECK_MEMBER_SIZEOF_TYPEDEF(s, m);			\
} while (0)

#define CHECK_STRUCT(s)						\
do {								\
	COMMENT("Checks for struct "#s);			\
		CHECK_VALUE((int)sizeof(struct s));		\
} while (0)

#define CHECK_STRUCT_TYPEDEF(s)					\
do {								\
	COMMENT("Checks for type "#s);				\
		CHECK_VALUE((int)sizeof(s));			\
} while (0)

#define CHECK_UNION(s)						\
do {								\
	COMMENT("Checks for union "#s);				\
	CHECK_VALUE((int)sizeof(union s));			\
} while (0)

#define CHECK_VALUE_SAME(v1, v2)				\
do {								\
	printf("\tLASSERTF("#v1" == "#v2", "			\
		"\"%%d != %%d\\n\",\n"				\
		"\t\t "#v1", "#v2");\n");			\
} while (0)

#define CHECK_MEMBER_SAME(s1, s2, m)				\
do {								\
	CHECK_VALUE_SAME((int)offsetof(struct s1, m),		\
			 (int)offsetof(struct s2, m));		\
	CHECK_VALUE_SAME((int)sizeof(((struct s1 *)0)->m),	\
			 (int)sizeof(((struct s2 *)0)->m));	\
} while (0)

#endif
