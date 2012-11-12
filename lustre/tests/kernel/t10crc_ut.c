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

#define DEBUG_SUBSYSTEM S_CLASS

#include <linux/module.h>
#include <linux/init.h>
#include <libcfs/libcfs.h>
#include <linux/crc-t10dif.h>

static int t10crc_block_generic_test(unsigned char *data, unsigned int size)
{
	unsigned int libcfs, kernel;

	LASSERT(size == 512 || size == 4096);

	libcfs = cfs_crypto_hash_crc_t10dif(data, size);
	kernel = crc_t10dif(data, size);

	if (kernel != libcfs) {
		CERROR("Test failed libcfs=0x%04x kernel=0x%04x size=%d\n",
		       libcfs, kernel, size);
		print_hex_dump(KERN_DEBUG, "raw data: ", DUMP_PREFIX_ADDRESS,
			       16, 1, data, size, 1);
		return -EINVAL;
	}
	return 0;
}

static int t10crc_performance_test(
				__u16 (*crct10)(const unsigned char*, size_t),
				const unsigned char *buf,
				unsigned int buf_len)
{
	unsigned long		start, end;
	int			bcount, res = 0;
	int			sec = 1; /* do test only 1 sec */
	unsigned long   tmp;

	for (start = jiffies, end = start + sec * HZ, bcount = 0;
	     time_before(jiffies, end); bcount++) {
		res = crct10(buf, buf_len);
	}
	end = jiffies;

	tmp = ((bcount * buf_len / jiffies_to_msecs(end - start)) *
	       1000) / (1024 * 1024);

	return (int)tmp;
}


static int t10crc_block_tests(unsigned int size, unsigned int loops)
{
	unsigned int	   i;
	unsigned char	   *data;
	int		   res = 0, libcfs, kernel;

	LCONSOLE_INFO("Checking block size %d loops %d ...\n", size, loops);

	data = cfs_alloc(size, 0);
	if (data == NULL) {
		CERROR("Failed to allocate mem\n");
		return -ENOMEM;
	}

	memset(data, 0, size);
	res = t10crc_block_generic_test(data, size);

	memset(data, 0xff, size);
	res |= t10crc_block_generic_test(data, size);


	for (i = 0; i < loops; i++) {
		cfs_get_random_bytes(data, size);
		res |= t10crc_block_generic_test(data, size);
	}

	/* Performance tests */
	libcfs = t10crc_performance_test(cfs_crypto_hash_crc_t10dif, data,
					 size);
	kernel = t10crc_performance_test(crc_t10dif, data, size);
	LCONSOLE_INFO("Speed: libcfs %dMB/s\t kernel %dMB/s\n", libcfs, kernel);
	cfs_free(data);

	if (res)
		LCONSOLE_INFO("FAILED\n");
	else
		LCONSOLE_INFO("PASS\n");
	return res;
}

static int __init t10crc_test_init(void)
{
	int res;

	LCONSOLE_INFO("t10crc UT\n");

	res = t10crc_block_tests(512, 4096);
	res |= t10crc_block_tests(4096, 4096);

	return res;
}

static void __exit t10crc_test_exit(void)
{
}

MODULE_AUTHOR("Sun Microsystems, Inc. <http://www.lustre.org/>");
MODULE_DESCRIPTION("t10crc test module");
MODULE_LICENSE("GPL");

module_init(t10crc_test_init);
module_exit(t10crc_test_exit);
