/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *   This file is part of the Lustre file system, http://www.lustre.org
 *   Lustre is a trademark of Cluster File Systems, Inc.
 *
 * concatenation of following two 16-bit multiply with carry generators
 * x(n)=a*x(n-1)+carry mod 2^16 and y(n)=b*y(n-1)+carry mod 2^16,
 * number and carry packed within the same 32 bit integer.
 * algorithm recommended by Marsaglia
 ******************************************************************/
#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif

#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <obd_class.h>

/*
From: George Marsaglia <geo@stat.fsu.edu>
Newsgroups: sci.math
Subject: Re: A RANDOM NUMBER GENERATOR FOR C
Date: Tue, 30 Sep 1997 05:29:35 -0700

 * You may replace the two constants 36969 and 18000 by any
 * pair of distinct constants from this list:
 * 18000 18030 18273 18513 18879 19074 19098 19164 19215 19584
 * 19599 19950 20088 20508 20544 20664 20814 20970 21153 21243
 * 21423 21723 21954 22125 22188 22293 22860 22938 22965 22974
 * 23109 23124 23163 23208 23508 23520 23553 23658 23865 24114
 * 24219 24660 24699 24864 24948 25023 25308 25443 26004 26088
 * 26154 26550 26679 26838 27183 27258 27753 27795 27810 27834
 * 27960 28320 28380 28689 28710 28794 28854 28959 28980 29013
 * 29379 29889 30135 30345 30459 30714 30903 30963 31059 31083
 * (or any other 16-bit constants k for which both k*2^16-1
 * and k*2^15-1 are prime) */

#define RANDOM_CONST_A 18030
#define RANDOM_CONST_B 29013

static unsigned int seed_x = 521288629;
static unsigned int seed_y = 362436069;
unsigned int ll_rand(void)
{

	seed_x = RANDOM_CONST_A * (seed_x & 65535) + (seed_x >> 16);
	seed_y = RANDOM_CONST_B * (seed_y & 65535) + (seed_y >> 16);

	return ((seed_x << 16) + (seed_y & 65535));
}
EXPORT_SYMBOL(ll_rand);

/* Note that if the input seeds are not completely random, then there is
 * a preferred location for the entropy in the two seeds, in order to avoid
 * the initial values from the PRNG to be the same each time.
 *
 * seed1 (seed_x) should have the most entropy in the low bits of the word
 * seed2 (seed_y) should have the most entropy in the high bits of the word */
void ll_srand(unsigned int seed1, unsigned int seed2)
{
	if (seed1)
		seed_x = seed1;	/* use default seeds if parameter is 0 */
	if (seed2)
		seed_y = seed2;
}
EXPORT_SYMBOL(ll_srand);
