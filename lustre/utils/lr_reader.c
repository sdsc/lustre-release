/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *   Copyright (C) 2006 Cluster File Systems, Inc.
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
 */
 /* Safely read the last_rcvd file from a device */

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <mntent.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>

#include <string.h>
#include <getopt.h>

#include <lustre_disk.h>
#include <lustre_ver.h>

int run_command(char *cmd)
{
        char log[] = "/tmp/mkfs_logXXXXXX";
        int fd, rc;
        
        
        if ((fd = mkstemp(log)) >= 0) {
                close(fd);
                strcat(cmd, " >");
                strcat(cmd, log);
        }
        strcat(cmd, " 2>&1");

        /* Can't use popen because we need the rv of the command */
        rc = system(cmd);
        if (rc && fd >= 0) {
                char buf[128];
                FILE *fp;
                fp = fopen(log, "r");
                if (fp) {
                        while (fgets(buf, sizeof(buf), fp) != NULL) {
                                if (rc) 
                                        printf("   %s", buf);
                        }
                        fclose(fp);
                }
        }
        if (fd >= 0) 
                remove(log);
        return rc;
}                                                       



int main(int argc, char *const argv[])
{
        char tmpdir[] = "/tmp/dirXXXXXX";
        char cmd[128];
        char filepnm[128];
        char *progname, *dev;
        struct lr_server_data lsd;
        FILE *filep;
        int ret;

        if ((argc < 2) || (argv[argc - 1][0] == '-')) {
                printf("Usage: %s devicename\n", argv[0]);
                printf("Read and print the last_rcvd file from a device\n");
                printf("(safe for mounted devices)\n");
                return EINVAL;
        }

        progname = argv[0];
        dev = argv[argc - 1];

        /* Make a temporary directory to hold Lustre data files. */
        if (!mkdtemp(tmpdir)) {
                fprintf(stderr, "%s: Can't create temporary directory %s: %s\n",
                        progname, tmpdir, strerror(errno));
                return errno;
        }

        memset(cmd, 0, sizeof(cmd));
        sprintf(cmd,
                "debugfs -c -R 'dump /%s %s/%s' %s",
                LAST_RCVD, tmpdir, LAST_RCVD, dev);

        ret = run_command(cmd);
        if (ret) {
                fprintf(stderr, "%s: Unable to dump %s file\n",
                        progname, LAST_RCVD);
                goto out_rmdir;
        }

        sprintf(filepnm, "%s/%s", tmpdir, LAST_RCVD);
        filep = fopen(filepnm, "r");
        if (!filep) {
                fprintf(stderr, "%s: Unable to read old data\n",
                        progname);
                ret = -errno;
                goto out_rmdir;
        }

        printf("Reading %s\n", LAST_RCVD);
        ret = fread(&lsd, 1, sizeof(lsd), filep);
        if (ret < sizeof(lsd)) {
                fprintf(stderr, "%s: Short read (%d of %d)\n",
                        progname, ret, (int)sizeof(lsd));
                ret = -ferror(filep);
                if (ret) 
                        goto out_close;
        }

        #if 0
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
        #endif

        printf("UUID %s\n", lsd.lsd_uuid);
        printf("Feature compat=%#x\n", lsd.lsd_feature_compat);
        printf("Feature incompat=%#x\n", lsd.lsd_feature_incompat);
        printf("Feature rocompat=%#x\n", lsd.lsd_feature_rocompat);
        printf("Last transaction %llu\n", (long long)lsd.lsd_last_transno);
        printf("ost index %u\n", lsd.lsd_ost_index);
        printf("mdt index %u\n", lsd.lsd_mdt_index);

        if ((lsd.lsd_feature_compat & OBD_COMPAT_OST) ||
            (lsd.lsd_feature_incompat & OBD_INCOMPAT_OST)) {
                printf("OST, index %d\n", lsd.lsd_ost_index);
        } else if ((lsd.lsd_feature_compat & OBD_COMPAT_MDT) ||
                   (lsd.lsd_feature_incompat & OBD_INCOMPAT_MDT)) {
                /* We must co-locate so mgs can see old logs.
                   If user doesn't want this, they can copy the old
                   logs manually and re-tunefs. */
                printf("MDS, index %d\n", lsd.lsd_mdt_index);
        } else  {
                /* If neither is set, we're pre-1.4.6, make a guess. */
                /* Construct debugfs command line. */
                memset(cmd, 0, sizeof(cmd));
                sprintf(cmd,
                        "debugfs -c -R 'rdump /%s %s' %s",
                        MDT_LOGS_DIR, tmpdir, dev);

                run_command(cmd);

                sprintf(filepnm, "%s/%s", tmpdir, MDT_LOGS_DIR);
                if (lsd.lsd_ost_index > 0) {
                        printf("non-flagged OST, index %d\n", 
                               lsd.lsd_ost_index);
                } else {
                        /* If there's a LOGS dir, it's an MDT */
                        if ((ret = access(filepnm, F_OK)) == 0) {
                                /* Old MDT's are always index 0 
                                   (pre CMD) */
                                printf("non-flagged MDS, index 0\n");
                        } else {
                                printf("non-flagged OST, index unknown\n");
                        }
                }
        }
        
out_close:        
        fclose(filep);

out_rmdir:
        memset(cmd, 0, sizeof(cmd));
        sprintf(cmd, "rm -rf %s", tmpdir);
        run_command(cmd);
        return ret;
}



