/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 *  Copyright (C) 2002 Cluster File Systems, Inc.
 *   Author: Robert Read <rread@clusterfs.com>
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


#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/mount.h>
#include <mntent.h>

#include "obdctl.h"
#include <portals/ptlctl.h>

int debug = 0;
int verbose = 0;
int nomtab = 0;

static void
update_mtab_entry(char *spec, char *node, char *type, char *opts,
		  int flags, int freq, int pass)
{
        FILE *fp;
        struct mntent mnt;

        mnt.mnt_fsname = spec;
        mnt.mnt_dir = node;
        mnt.mnt_type = type;
        mnt.mnt_opts = opts ? opts : "";
        mnt.mnt_freq = freq;
        mnt.mnt_passno = pass;

        if (!nomtab) {
                fp = setmntent(MOUNTED, "a+");
                if (fp == NULL) {
                        fprintf(stderr, "setmntent(%s): %s:", MOUNTED,
                                strerror (errno));
                } else {
                        if ((addmntent (fp, &mnt)) == 1) {
                                fprintf(stderr, "addmntent: %s:",
                                        strerror (errno));
                        }
                        endmntent(fp);
                }
        }
}

int
init_options(struct lustre_mount_data *lmd)
{
        memset(lmd, 0, sizeof(lmd));
        lmd->lmd_server_nid = PTL_NID_ANY;
        lmd->lmd_local_nid = PTL_NID_ANY;
        lmd->lmd_port = 988;    /* XXX define LUSTRE_DEFAULT_PORT */
        lmd->lmd_nal = SOCKNAL;
        return 0;
}

int
print_options(struct lustre_mount_data *lmd)
{
        printf("mds:             %s\n", lmd->lmd_mds);
        printf("profile:         %s\n", lmd->lmd_profile);
        printf("server_nid:      "LPX64"\n", lmd->lmd_server_nid);
        printf("local_nid:       "LPX64"\n", lmd->lmd_local_nid);
        printf("nal:             %d\n", lmd->lmd_nal);
        printf("server_ipaddr:   0x%x\n", lmd->lmd_server_ipaddr);
        printf("port:            %d\n", lmd->lmd_port);

        return 0;
}

int
parse_options(char * options, struct lustre_mount_data *lmd)
{
        ptl_nid_t nid = 0;
        int val;
        char *opt;
        char * opteq;
        
        /* parsing ideas here taken from util-linux/mount/nfsmount.c */
        for (opt = strtok(options, ","); opt; opt = strtok(NULL, ",")) {
                if ((opteq = strchr(opt, '='))) {
                        val = atoi(opteq + 1);
                        *opteq = '\0';
                        if (!strcmp(opt, "nettype")) {
                                lmd->lmd_nal = ptl_name2nal(opteq+1);
                        } else if(!strcmp(opt, "local_nid")) {
                                if (ptl_parse_nid(&nid, opteq+1) != 0) {
                                        fprintf (stderr, "mount: "
                                                 "can't parse NID %s\n",
                                                 opteq+1);
                                        return (-1);
                                }
                                lmd->lmd_local_nid = nid;
                        } else if(!strcmp(opt, "server_nid")) {
                                if (ptl_parse_nid(&nid, opteq+1) != 0) {
                                        fprintf (stderr, "mount: "
                                                 "can't parse NID %s\n",
                                                 opteq+1);
                                        return (-1);
                                }
                                lmd->lmd_server_nid = nid;
                        } else if (!strcmp(opt, "port")) {
                                lmd->lmd_port = val;
                        }
                } else {
                        val = 1;
                        if (!strncmp(opt, "no", 2)) {
                                val = 0;
                                opt += 2;
                        }
                        if (!strcmp(opt, "debug")) {
                                debug = val;
                        }
                }
        }
        return 0;
}

int
get_local_elan_id(char *fname, char *buf)
{
        FILE *fp = fopen(fname, "r");
        int   rc;

        if (fp == NULL)
                return -1;

        rc = fscanf(fp, "NodeId %255s", buf);

        fclose(fp);

        return (rc == 1) ? 0 : -1;
}

int
set_local(struct lustre_mount_data *lmd)
{
        /* XXX ClusterID?
         * XXX PtlGetId() will be safer if portals is loaded and
         * initialised correctly at this time... */
        char buf[256];
        ptl_nid_t nid;
        int rc;

        if (lmd->lmd_local_nid != PTL_NID_ANY)
                return 0;

        memset(buf, 0, sizeof(buf));

        if (lmd->lmd_nal == SOCKNAL || lmd->lmd_nal == TCPNAL) {
                rc = gethostname(buf, sizeof(buf) - 1);
                if (rc) {
                        fprintf (stderr, "mount: can't get local buf:"
                                 "%d\n", rc);
                        return rc;
                }
        } else if (lmd->lmd_nal == QSWNAL) {
#if MULTIRAIL_EKC
                char *pfiles[] = {"/proc/qsnet/elan3/device0/position",
                                  "/proc/qsnet/elan4/device0/position",
                                  NULL};
#else
                char *pfiles[] = {"/proc/elan/device0/position",
                                  NULL};
#endif
                int   i = 0;

                do {
                        rc = get_local_elan_id(pfiles[i], buf);
                } while (rc != 0 &&
                         pfiles[++i] != NULL);
                
                if (rc != 0) {
                        fprintf(stderr, "mount: can't read elan ID"
                                " from /proc\n");
                        return -1;
                }
        }

        if (ptl_parse_nid (&nid, buf) != 0) {
                fprintf (stderr, "mount: can't parse NID %s\n", 
                         buf);
                return (-1);
        }

        lmd->lmd_local_nid = nid;
        return 0;
}

int
set_peer(char *hostname, struct lustre_mount_data *lmd)
{
        ptl_nid_t nid = 0;
        int rc;

        if (lmd->lmd_nal == SOCKNAL || lmd->lmd_nal == TCPNAL) {
                if (lmd->lmd_server_nid == PTL_NID_ANY) {
                        if (ptl_parse_nid (&nid, hostname) != 0) {
                                fprintf (stderr, "mount: can't parse NID %s\n",
                                         hostname);
                                return (-1);
                        }
                        lmd->lmd_server_nid = nid;
                }

                if (ptl_parse_ipaddr(&lmd->lmd_server_ipaddr, hostname) != 0) {
                        fprintf (stderr, "mount: can't parse host %s\n",
                                 hostname);
                        return (-1);
                }
        } else if (lmd->lmd_nal == QSWNAL) {
                char buf[64];
                rc = sscanf(hostname, "%*[^0-9]%63[0-9]", buf);
                if (rc != 1) {
                        fprintf (stderr, "mount: can't get elan id from host %s\n",
                                 hostname);
                        return -1;
                }
                if (ptl_parse_nid (&nid, buf) != 0) {
                        fprintf (stderr, "mount: can't parse NID %s\n",
                                 hostname);
                        return (-1);
                }
                lmd->lmd_server_nid = nid;
        }


        return 0;
}

int
build_data(char *source, char *options, struct lustre_mount_data *lmd)
{
        char target[1024];
        char *hostname = NULL;
        char *mds = NULL;
        char *profile = NULL;
        char *s;
        int rc;

        if (strlen(source) > sizeof(target) + 1) {
                fprintf(stderr, "mount: "
                        "exessively long host:/mds/profile argument\n");
                return -EINVAL;
        }
        strcpy(target, source);
        if ((s = strchr(target, ':'))) {
                hostname = target;
                *s = '\0';

                while (*++s == '/')
                        ;
                mds = s;
                if ((s = strchr(mds, '/'))) {
                        *s = '\0';
                        profile = s + 1;
                } else {
                        fprintf(stderr, "mount: "
                                "directory to mount not in "
                                "host:/mds/profile format\n");
                        return(-1);
                }
        } else {
                fprintf(stderr, "mount: "
                        "directory to mount not in host:/mds/profile format\n");
                return(-1);
        }
        if (verbose)
                printf("host: %s\nmds: %s\nprofile: %s\n", hostname, mds,
                       profile);

        rc = parse_options(options, lmd);
        if (rc)
                return rc;

        rc = set_local(lmd);
        if (rc)
                return rc;

        rc = set_peer(hostname, lmd);
        if (rc)
                return rc;
        if (strlen(mds) > sizeof(lmd->lmd_mds) + 1) {
                fprintf(stderr, "mount: mds name too long\n");
                return(-1);
        }
        strcpy(lmd->lmd_mds, mds);

        if (strlen(profile) > sizeof(lmd->lmd_profile) + 1) {
                fprintf(stderr, "mount: profile name too long\n");
                return(-1);
        }
        strcpy(lmd->lmd_profile, profile);

        
        if (verbose)
                print_options(lmd);
        return 0;
}

int
main(int argc, char * const argv[])
{
        char * source = argv[1];
        char * target = argv[2];
        char * options = "";
        int opt;
        int i;
        struct lustre_mount_data lmd;

        int rc;

        while ((opt = getopt(argc, argv, "vno:")) != EOF) {
                switch (opt) {
                case 'v':
                        verbose = 1;
                        printf("verbose: %d\n", verbose);
                        break;
                case 'n':
                        nomtab = 1;
                        printf("nomtab: %d\n", nomtab);
                        break;
                case 'o':
                        options = optarg;
                        break;
                default:
                        break;
                }
        }

        if (verbose)
                for (i = 0; i < argc; i++) {
                        printf("arg[%d] = %s\n", i, argv[i]);
                }

        init_options(&lmd);
        rc = build_data(source, options, &lmd);
        if (rc) {
                exit(1);
        }

        if (debug) {
                printf("mount: debug mode, not mounting\n");
                exit(0);
        }

        rc = mount(source, target, "lustre", 0, (void *)&lmd);
        if (rc) {
                perror(argv[0]);
        } else {
                update_mtab_entry(source, target, "lustre", options, 0, 0, 0);
        }
        return rc;
}
