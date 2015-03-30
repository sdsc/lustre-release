/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 *
 * lustre/utils/lustreapi_internal.h
 *
 */
/*
 *
 * Author: Aurelien Degremont <aurelien.degremont@cea.fr>
 * Author: JC Lafoucriere <jacques-charles.lafoucriere@cea.fr>
 * Author: Thomas Leibovici <thomas.leibovici@cea.fr>
 */

#ifndef _LUSTREAPI_INTERNAL_H_
#define _LUSTREAPI_INTERNAL_H_

#define WANT_PATH   0x1
#define WANT_FSNAME 0x2
#define WANT_FD     0x4
#define WANT_INDEX  0x8
#define WANT_ERROR  0x10
int get_root_path(int want, char *fsname, int *outfd, char *path, int index);
int root_ioctl(const char *mdtname, int opc, void *data, int *mdtidxp,
	       int want_error);
int get_param(const char *param_path, char *result,
	      unsigned int result_size);

#define LLAPI_LAYOUT_MAGIC 0x11AD1107 /* LLAPILOT */

/* Helper functions for testing validity of stripe attributes. */

static inline bool llapi_stripe_size_is_aligned(uint64_t size)
{
	return (size & (LOV_MIN_STRIPE_SIZE - 1)) == 0;
}

static inline bool llapi_stripe_size_is_too_big(uint64_t size)
{
	return size >= (1ULL << 32);
}

static inline bool llapi_stripe_count_is_valid(int64_t count)
{
	return count >= -1 && count <= LOV_MAX_STRIPE_COUNT;
}

static inline bool llapi_stripe_index_is_valid(int64_t index)
{
	return index >= -1 && index <= LOV_V1_INSANE_STRIPE_COUNT;
}

/* Compatibility macro for legacy llapi functions that use "offset"
 * terminology instead of the preferred "index". */
#define llapi_stripe_offset_is_valid(os) llapi_stripe_index_is_valid(os)

/*
 * Kernel communication for Changelogs and HSM requests.
 */

/* KUC message header.
 * All current and future KUC messages should use this header.
 * To avoid having to include Lustre headers from libcfs, define this here.
 */
struct kuc_hdr {
	__u16 kuc_magic;
	__u8  kuc_transport;  /* Each new Lustre feature should use a different
				 transport */
	__u8  kuc_flags;
	__u16 kuc_msgtype;    /* Message type or opcode, transport-specific */
	__u16 kuc_msglen;     /* Including header */
} __attribute__((aligned(sizeof(__u64))));

#define KUC_CHANGELOG_MSG_MAXSIZE (sizeof(struct kuc_hdr)+CR_MAXSIZE)

#define KUC_MAGIC  0x191C /*Lustre9etLinC */

/* kuc_msgtype values are defined in each transport */
enum kuc_transport_type {
	KUC_TRANSPORT_GENERIC   = 1,
	KUC_TRANSPORT_HSM       = 2,
	KUC_TRANSPORT_CHANGELOG = 3,
};

enum kuc_generic_message_type {
	KUC_MSG_SHUTDOWN = 1,
};

/* KUC Broadcast Groups. This determines which userspace process hears which
 * messages.  Mutliple transports may be used within a group, or multiple
 * groups may use the same transport.  Broadcast
 * groups need not be used if e.g. a UID is specified instead;
 * use group 0 to signify unicast.
 */
#define KUC_GRP_HSM	0x02
#define KUC_GRP_MAX	KUC_GRP_HSM

#define LK_FLG_STOP 0x01
#define LK_NOFD -1U

/* kernelcomm control structure, passed from userspace to kernel */
struct lustre_kernelcomm {
	__u32 lk_wfd;
	__u32 lk_rfd;
	__u32 lk_uid;
	__u32 lk_group;
	__u32 lk_data;
	__u32 lk_flags;
} __attribute__((packed));

int libcfs_ukuc_start(struct lustre_kernelcomm *l, int groups, int rfd_flags);
int libcfs_ukuc_stop(struct lustre_kernelcomm *l);
int libcfs_ukuc_get_rfd(struct lustre_kernelcomm *link);
int libcfs_ukuc_msg_get(struct lustre_kernelcomm *l, char *buf, int maxsize,
			int transport);

#endif /* _LUSTREAPI_INTERNAL_H_ */
