/*
 *	Definitions of structures for vfsv0 quota format
 */

/* Source linux/fs/quota/quotaio_v2.h */

#ifndef _LINUX_QUOTAIO_V2_H
#define _LINUX_QUOTAIO_V2_H

#include <linux/types.h>
#include <linux/quota.h>

#define V2_INITQMAGICS {\
	0xd9c01f11,	/* USRQUOTA */\
	0xd9c01927	/* GRPQUOTA */\
}

/* Header with type and version specific information */
struct v2_disk_dqinfo {
	__le32 dqi_bgrace;	/* Time before block soft limit becomes hard limit */
	__le32 dqi_igrace;	/* Time before inode soft limit becomes hard limit */
	__le32 dqi_flags;	/* Flags for quotafile (DQF_*) */
	__le32 dqi_blocks;	/* Number of blocks in file */
	__le32 dqi_free_blk;	/* Number of first free block in the list */
	__le32 dqi_free_entry;	/* Number of block with at least one free entry */
};

/* First generic header */
struct v2_disk_dqheader {
	__le32 dqh_magic;	/* Magic number identifying file */
	__le32 dqh_version;	/* File version */
};
#define V2_DQINFOOFF	sizeof(struct v2_disk_dqheader)	/* Offset of info header in file */

#endif /* _LINUX_QUOTAIO_V2_H */
