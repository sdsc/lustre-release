/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2006 Cluster File Systems, Inc.
 *   Author: Nathan Rutman <nathan@clusterfs.com>
 *
 *   This file is part of Lustre, http://www.lustre.org.
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *
 * Lustre disk format definitions.
 */

#ifndef _LUSTRE_DISK_H
#define _LUSTRE_DISK_H

#include <lnet/types.h>

/****************** on-disk files *********************/

#define MDT_LOGS_DIR      "LOGS"  /* COMPAT_146 */
#define MOUNT_CONFIGS_DIR "CONFIGS"
/* Persistent mount data are stored on the disk in this file. */
#define MOUNT_DATA_FILE    MOUNT_CONFIGS_DIR"/mountdata"
#define LAST_RCVD         "last_rcvd"
#define LOV_OBJID         "lov_objid"
#define HEALTH_CHECK      "health_check"


/****************** persistent mount data *********************/

#define LDD_F_SV_TYPE_MDT   0x0001
#define LDD_F_SV_TYPE_OST   0x0002
#define LDD_F_SV_TYPE_MGS   0x0004
#define LDD_F_NEED_INDEX    0x0010 /* need an index assignment */
#define LDD_F_VIRGIN        0x0020 /* never registered */
#define LDD_F_UPDATE        0x0040 /* update all related config logs */
#define LDD_F_REWRITE_LDD   0x0080 /* rewrite the LDD */
#define LDD_F_WRITECONF     0x0100 /* regenerate all logs for this fs */
#define LDD_F_UPGRADE14     0x0200 /* COMPAT_14 */
#define LDD_F_PARAM_FNID    0x0400 /* process failover nids as params */

enum ldd_mount_type {
        LDD_MT_EXT3 = 0, 
        LDD_MT_LDISKFS,
        LDD_MT_SMFS,   
        LDD_MT_REISERFS,
        LDD_MT_LAST
};
       
static inline char *mt_str(enum ldd_mount_type mt)
{
        static char *mount_type_string[] = {
                "ext3",
                "ldiskfs",
                "smfs",
                "reiserfs",
        };
        return mount_type_string[mt];
}

#define LDD_INCOMPAT_SUPP 0
#define LDD_ROCOMPAT_SUPP 0

#define LDD_MAGIC 0x1dd00001

/* On-disk configuration file. In host-endian order. */
struct lustre_disk_data {
        __u32      ldd_magic;
        __u32      ldd_feature_compat;  /* compatible feature flags */
        __u32      ldd_feature_rocompat;/* read-only compatible feature flags */
        __u32      ldd_feature_incompat;/* incompatible feature flags */
        
        __u32      ldd_config_ver;      /* config rewrite count - not used */
        __u32      ldd_flags;           /* LDD_SV_TYPE */
        __u32      ldd_svindex;         /* server index (0001), must match 
                                           svname */
        __u32      ldd_mount_type;      /* target fs type LDD_MT_* */
        char       ldd_fsname[64];      /* filesystem this server is part of */
        char       ldd_svname[64];      /* this server's name (lustre-mdt0001)*/
        __u8       ldd_uuid[40];        /* server UUID (COMPAT_146) */
   
/*200*/ char       ldd_userdata[1024 - 200]; /* arbitrary user string */
/*1024*/__u8       ldd_padding[4096 - 1024];
/*4096*/char       ldd_mount_opts[4096]; /* target fs mount opts */
/*8192*/char       ldd_params[4096];     /* key=value pairs */
};

#define IS_MDT(data)   ((data)->ldd_flags & LDD_F_SV_TYPE_MDT)
#define IS_OST(data)   ((data)->ldd_flags & LDD_F_SV_TYPE_OST)
#define IS_MGS(data)  ((data)->ldd_flags & LDD_F_SV_TYPE_MGS)
#define MT_STR(data)   mt_str((data)->ldd_mount_type)

/* Make the mdt/ost server obd name based on the filesystem name */
static inline int server_make_name(__u32 flags, __u16 index, char *fs,
                                   char *name)
{
        if (flags & (LDD_F_SV_TYPE_MDT | LDD_F_SV_TYPE_OST)) {
                sprintf(name, "%.8s-%s%04x", fs,
                        (flags & LDD_F_SV_TYPE_MDT) ? "MDT" : "OST",  
                        index);
        } else if (flags & LDD_F_SV_TYPE_MGS) {
                sprintf(name, "MGS");
        } else {
                CERROR("unknown server type %#x\n", flags);
                return 1;
        }
        return 0;
}

/* Get the index from the obd name */
int server_name2index(char *svname, __u32 *idx, char **endptr);


/****************** mount command *********************/

/* The lmd is only used internally by Lustre; mount simply passes 
   everything as string options */

#define LMD_MAGIC    0xbdacbd03

/* gleaned from the mount command - no persistent info here */
struct lustre_mount_data {
        __u32      lmd_magic;
        __u32      lmd_flags;         /* lustre mount flags */
        int        lmd_mgs_failnodes; /* mgs failover node count */
        int        lmd_exclude_count;
        char      *lmd_dev;           /* device name */
        char      *lmd_profile;       /* client only */
        char      *lmd_opts;          /* lustre mount options (as opposed to 
                                         _device_ mount options) */
        __u32     *lmd_exclude;       /* array of OSTs to ignore */
};

#define LMD_FLG_SERVER       0x0001  /* Mounting a server */
#define LMD_FLG_CLIENT       0x0002  /* Mounting a client */
#define LMD_FLG_ABORT_RECOV  0x0008  /* Abort recovery */
#define LMD_FLG_NOSVC        0x0010  /* Only start MGS/MGC for servers, 
                                        no other services */

#define lmd_is_client(x) ((x)->lmd_flags & LMD_FLG_CLIENT) 


/****************** last_rcvd file *********************/

#define LR_SERVER_SIZE   512
#define LR_CLIENT_START 8192
#define LR_CLIENT_SIZE   128
#if LR_CLIENT_START < LR_SERVER_SIZE
#error "Can't have LR_CLIENT_START < LR_SERVER_SIZE"
#endif
/* This limit is arbitrary (32k clients on x86), but it is convenient to use
 * 2^n * CFS_PAGE_SIZE * 8 for the number of bits that fit an order-n allocation. */
#define LR_MAX_CLIENTS (CFS_PAGE_SIZE * 8)

                                                                                
/* COMPAT_146 */
#define OBD_COMPAT_OST          0x00000002 /* this is an OST (temporary) */
#define OBD_COMPAT_MDT          0x00000004 /* this is an MDT (temporary) */
/* end COMPAT_146 */

#define OBD_ROCOMPAT_LOVOBJID   0x00000001 /* MDS handles LOV_OBJID file */
#define OBD_ROCOMPAT_CROW       0x00000002 /* OST will CROW create objects */

#define OBD_INCOMPAT_GROUPS     0x00000001 /* OST handles group subdirs */
#define OBD_INCOMPAT_OST        0x00000002 /* this is an OST */
#define OBD_INCOMPAT_MDT        0x00000004 /* this is an MDT */
#define OBD_INCOMPAT_COMMON_LR  0x00000008 /* common last_rvcd format */


/* Data stored per server at the head of the last_rcvd file.  In le32 order.
   This should be common to filter_internal.h, lustre_mds.h */
struct lr_server_data {
        __u8  lsd_uuid[40];        /* server UUID */
        __u64 lsd_last_transno;    /* last completed transaction ID */
        __u64 lsd_compat14;        /* reserved - compat with old last_rcvd */
        __u64 lsd_mount_count;     /* incarnation number */
        __u32 lsd_feature_compat;  /* compatible feature flags */
        __u32 lsd_feature_rocompat;/* read-only compatible feature flags */
        __u32 lsd_feature_incompat;/* incompatible feature flags */
        __u32 lsd_server_size;     /* size of server data area */
        __u32 lsd_client_start;    /* start of per-client data area */
        __u16 lsd_client_size;     /* size of per-client data area */
        __u16 lsd_subdir_count;    /* number of subdirectories for objects */
        __u64 lsd_catalog_oid;     /* recovery catalog object id */
        __u32 lsd_catalog_ogen;    /* recovery catalog inode generation */
        __u8  lsd_peeruuid[40];    /* UUID of MDS associated with this OST */
        __u32 lsd_ost_index;       /* index number of OST in LOV */
        __u32 lsd_mdt_index;       /* index number of MDT in LMV */
        __u8  lsd_padding[LR_SERVER_SIZE - 148];
};

/* Data stored per client in the last_rcvd file.  In le32 order. */
struct lsd_client_data {
        __u8  lcd_uuid[40];      /* client UUID */
        __u64 lcd_last_transno; /* last completed transaction ID */
        __u64 lcd_last_xid;     /* xid for the last transaction */
        __u32 lcd_last_result;  /* result from last RPC */
        __u32 lcd_last_data;    /* per-op data (disposition for open &c.) */
        /* for MDS_CLOSE requests */
        __u64 lcd_last_close_transno; /* last completed transaction ID */
        __u64 lcd_last_close_xid;     /* xid for the last transaction */
        __u32 lcd_last_close_result;  /* result from last RPC */
        __u32 lcd_last_close_data;    /* per-op data */
        __u8  lcd_padding[LR_CLIENT_SIZE - 88];
};


#ifdef __KERNEL__
/****************** superblock additional info *********************/
struct ll_sb_info;

struct lustre_sb_info {
        int                       lsi_flags;
        struct obd_device        *lsi_mgc;     /* mgc obd */
        struct lustre_mount_data *lsi_lmd;     /* mount command info */
        struct lustre_disk_data  *lsi_ldd;     /* mount info on-disk */
        struct ll_sb_info        *lsi_llsbi;   /* add'l client sbi info */
        struct vfsmount          *lsi_srv_mnt; /* the one server mount */
        atomic_t                  lsi_mounts;  /* references to the srv_mnt */
};

#define LSI_SERVER                       0x00000001
#define LSI_UMOUNT_FORCE                 0x00000010
#define LSI_UMOUNT_FAILOVER              0x00000020

#if  (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
# define    s2lsi(sb)        ((struct lustre_sb_info *)((sb)->s_fs_info))
# define    s2lsi_nocast(sb) ((sb)->s_fs_info)
#else  /* 2.4 here */
# define    s2lsi(sb)        ((struct lustre_sb_info *)((sb)->u.generic_sbp))
# define    s2lsi_nocast(sb) ((sb)->u.generic_sbp)
#endif

#define     get_profile_name(sb)   (s2lsi(sb)->lsi_lmd->lmd_profile)

#endif /* __KERNEL__ */

/****************** mount lookup info *********************/

struct lustre_mount_info {
        char               *lmi_name;
        struct super_block *lmi_sb;
        struct vfsmount    *lmi_mnt;
        struct list_head    lmi_list_chain;
};

/****************** prototypes *********************/

#ifdef __KERNEL__
#include <obd_class.h>

/* obd_mount.c */
void lustre_register_client_fill_super(int (*cfs)(struct super_block *sb));
int lustre_common_put_super(struct super_block *sb);
int lustre_process_log(struct super_block *sb, char *logname, 
                     struct config_llog_instance *cfg);
int lustre_end_log(struct super_block *sb, char *logname, 
                       struct config_llog_instance *cfg);
struct lustre_mount_info *server_get_mount(char *name);
int server_put_mount(char *name, struct vfsmount *mnt);
int server_register_target(struct super_block *sb);
struct mgs_target_info;
int server_mti_print(char *title, struct mgs_target_info *mti);

/* mgc_request.c */
int mgc_logname2resid(char *logname, struct ldlm_res_id *res_id);

#endif

#endif // _LUSTRE_DISK_H
