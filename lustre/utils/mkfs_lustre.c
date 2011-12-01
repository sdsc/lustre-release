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
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011 Whamcloud, Inc.
 *
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/utils/mkfs_lustre.c
 *
 * Author: Nathan Rutman <nathan@clusterfs.com>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>

#include "mount_utils.h"

char *progname;
int verbose = 1;

#ifdef HAVE_LDISKFS_OSD
 #define FSLIST_LDISKFS "ldiskfs"
 #define HAVE_FSLIST
#else
 #define FSLIST_LDISKFS ""
#endif /* HAVE_LDISKFS_OSD */
#ifdef HAVE_ZFS_OSD
 #ifdef HAVE_FSLIST
   #define FSLIST_ZFS "|zfs"
 #else
  #define FSLIST_ZFS "zfs"
  #define HAVE_FSLIST
 #endif
#else
 #define FSLIST_ZFS ""
#endif /* HAVE_ZFS_OSD */
#ifdef HAVE_BTRFS_OSD
 #ifdef HAVE_FSLIST
   #define FSLIST_BTRFS "|btrfs"
 #else
  #define FSLIST_BTRFS "btrfs"
  #define HAVE_FSLIST
 #endif
#else
 #define FSLIST_BTRFS ""
#endif /* HAVE_BTRFS_OSD */

#ifndef HAVE_FSLIST
 #error "no backing OSD types (ldiskfs or ZFS) are configured"
#endif

#define FSLIST FSLIST_LDISKFS FSLIST_ZFS FSLIST_BTRFS

static void usage(FILE *out)
{
        fprintf(out, "%s v"LUSTRE_VERSION_STRING"\n", progname);
#ifdef HAVE_ZFS_OSD
        fprintf(out, "usage: %s <target types> [--backfstype=zfs] [options] "
                "<pool name>/<dataset name> [[<vdev type>] <device> "
                "[<device> ...] [[vdev type>] ...]]\n", progname);
#endif

        fprintf(out, "usage: %s <target types> --backfstype="FSLIST" "
                "[options] <device>\n", progname);
        fprintf(out,
                "\t<device>:block device or file (e.g /dev/sda or /tmp/ost1)\n"
#ifdef HAVE_ZFS_OSD
                "\t<pool name>: name of the ZFS pool where to create the "
                "target (e.g. tank)\n"
                "\t<dataset name>: name of the new dataset (e.g. ost1). The "
                "dataset name must be unique within the ZFS pool\n"
                "\t<vdev type>: type of vdev (mirror, raidz, raidz2, spare, "
                "cache, log)\n"
#endif
                "\n"
                "\ttarget types:\n"
                "\t\t--ost: object storage, mutually exclusive with mdt,mgs\n"
                "\t\t--mdt: metadata storage, mutually exclusive with ost\n"
                "\t\t--mgs: configuration management service - one per site\n"
                "\n"
                "\toptions (in order of popularity):\n"
                "\t\t--mgsnode=<nid>[,<...>] : NID(s) of a remote mgs node\n"
                "\t\t\trequired for all targets other than the mgs node\n"
                "\t\t--fsname=<filesystem_name> : default is 'lustre'\n"
                "\t\t--failnode=<nid>[,<...>] : NID(s) of a failover partner\n"
                "\t\t\tcannot be used with --servicenode\n"
                "\t\t--servicenode=<nid>[,<...>] : NID(s) of all service \n"
                "\t\tpartners treat all nodes as equal service node, cannot\n"
                "\t\tbe used with --failnode\n"
                "\t\t--param <key>=<value> : set a permanent parameter\n"
                "\t\t\te.g. --param sys.timeout=40\n"
                "\t\t\t     --param lov.stripesize=2M\n"
                "\t\t--index=#N : target index (i.e. ost index within lov)\n"
                "\t\t--comment=<user comment>: arbitrary string (%d bytes)\n"
                "\t\t--mountfsoptions=<opts> : permanent mount options\n"
                "\t\t--network=<net>[,<...>] : restrict OST/MDT to network(s)\n"
                "\t\t--backfstype=<fstype> : backing fs type ("FSLIST")\n"
                "\t\t--device-size=#N(KB) : loop device or pool/dataset size\n"
#ifdef HAVE_ZFS_OSD
                "\t\t--vdev-size=#N(KB) : size for file based vdevs\n"
#endif
                "\t\t--mkfsoptions=<opts> : format options\n"
                "\t\t--reformat: overwrite an existing disk\n"
                "\t\t--stripe-count-hint=#N : for optimizing MDT inode size\n"
                "\t\t--iam-dir: use IAM directory format, not ext3 compatible\n"
                "\t\t--dryrun: just report, don't write to disk\n"
                "\t\t--verbose : e.g. show mkfs progress\n"
                "\t\t--quiet\n",
                (int)sizeof(((struct lustre_disk_data *)0)->ldd_userdata));
        return;
}

static void set_defaults(struct mkfs_opts *mop)
{
        memset(mop, 0, sizeof(*mop));
        mop->mo_ldd.ldd_magic = LDD_MAGIC;
        mop->mo_ldd.ldd_config_ver = 1;
        mop->mo_ldd.ldd_flags = LDD_F_NEED_INDEX | LDD_F_UPDATE | LDD_F_VIRGIN;
        mop->mo_mgs_failnodes = 0;
        strcpy(mop->mo_ldd.ldd_fsname, "lustre");
#ifdef HAVE_LDISKFS_OSD
        mop->mo_ldd.ldd_mount_type = LDD_MT_LDISKFS;
#else
        mop->mo_ldd.ldd_mount_type = LDD_MT_ZFS;
#endif
        /* mop->mo_ldd.ldd_svindex = 0; */
        /* 2.1 Lustre allows unassigned index */
        mop->mo_ldd.ldd_svindex = 0xFFFF;
        mop->mo_stripe_count = 1;
        mop->mo_pool_vdevs = NULL;
        mop->mo_vdev_sz = 0;
}

static void badopt(const char *opt, char *type)
{
        fprintf(stderr, "%s: '--%s' only valid for %s\n", progname, opt, type);
        usage(stderr);
}

static int parse_opts(int argc, char *const argv[], struct mkfs_opts *mop,
               char **mountopts)
{
        static struct option long_opt[] = {
                {"iam-dir", 0, 0, 'a'},
                {"backfstype", 1, 0, 'b'},
                {"stripe-count-hint", 1, 0, 'c'},
                {"comment", 1, 0, 'u'},
                {"device-size", 1, 0, 'd'},
                {"vdev-size", 1, 0, 'e'},
                {"dryrun", 0, 0, 'n'},
                {"failnode", 1, 0, 'f'},
                {"failover", 1, 0, 'f'},
                {"mgs", 0, 0, 'G'},
                {"help", 0, 0, 'h'},
                {"index", 1, 0, 'i'},
                {"mkfsoptions", 1, 0, 'k'},
                {"mgsnode", 1, 0, 'm'},
                {"mgsnid", 1, 0, 'm'},
                {"mdt", 0, 0, 'M'},
                {"fsname",1, 0, 'L'},
                {"noformat", 0, 0, 'n'},
                {"mountfsoptions", 1, 0, 'o'},
                {"ost", 0, 0, 'O'},
                {"param", 1, 0, 'p'},
                {"print", 0, 0, 'n'},
                {"quiet", 0, 0, 'q'},
                {"reformat", 0, 0, 'r'},
                {"servicenode", 1, 0, 's'},
                {"verbose", 0, 0, 'v'},
                {"network", 1, 0, 't'},
                {0, 0, 0, 0}
        };
        char *optstring = "b:c:C:d:e:f:Ghi:k:L:m:Mno:Op:Pqrs:t:u:v";
        int opt;
        int rc, longidx;
        int failnode_set = 0, servicenode_set = 0;

        while ((opt = getopt_long(argc, argv, optstring, long_opt, &longidx)) !=
               EOF) {
                switch (opt) {
                case 'a': {
                        if (IS_MDT(&mop->mo_ldd))
                                mop->mo_ldd.ldd_flags |= LDD_F_IAM_DIR;
                        break;
                }
                case 'b': {
                        int i = 0;
                        while (i < LDD_MT_LAST) {
                                if (strcmp(optarg, mt_str(i)) == 0) {
                                        mop->mo_ldd.ldd_mount_type = i;
                                        break;
                                }
                                i++;
                        }
                        if (i == LDD_MT_LAST) {
                                fprintf(stderr, "%s: invalid backend filesystem"
                                        " type %s\n", progname, optarg);
                                return 1;
                        }
                        break;
                }
                case 'c':
                        if (IS_MDT(&mop->mo_ldd)) {
                                int stripe_count = atol(optarg);
                                if (stripe_count <= 0) {
                                        fprintf(stderr, "%s: bad stripe count "
                                                "%d\n", progname, stripe_count);
                                        return 1;
                                }
                                mop->mo_stripe_count = stripe_count;
                        } else {
                                badopt(long_opt[longidx].name, "MDT");
                                return 1;
                        }
                        break;
                case 'd':
                        mop->mo_device_sz = atol(optarg);
                        break;
                case 'e':
                        mop->mo_vdev_sz = atol(optarg);
                        break;
                case 'f':
                case 's': {
                        char *nids;

                        if ((opt == 'f' && servicenode_set)
                            || (opt == 's' && failnode_set)) {
                                fprintf(stderr, "%s: %s cannot use with --%s\n",
                                        progname, long_opt[longidx].name,
                                        opt == 'f' ? "servicenode" : "failnode");
                                return 1;
                        }

                        nids = convert_hostnames(optarg);
                        if (!nids)
                                return 1;
                        rc = add_param(mop->mo_ldd.ldd_params, PARAM_FAILNODE,
                                       nids);
                        free(nids);
                        if (rc)
                                return rc;
                        /* Must update the mgs logs */
                        mop->mo_ldd.ldd_flags |= LDD_F_UPDATE;
                        if (opt == 'f') {
                                failnode_set = 1;
                        } else {
                                mop->mo_ldd.ldd_flags |= LDD_F_NO_PRIMNODE;
                                servicenode_set = 1;
                        }
                        mop->mo_flags |= MO_FAILOVER;
                        break;
                }
                case 'G':
                        mop->mo_ldd.ldd_flags |= LDD_F_SV_TYPE_MGS;
                        break;
                case 'h':
                        usage(stdout);
                        return 1;
                case 'i':
                        if (!(mop->mo_ldd.ldd_flags &
                              (LDD_F_UPGRADE14 | LDD_F_VIRGIN |
                               LDD_F_WRITECONF))) {
                                fprintf(stderr, "%s: cannot change the index of"
                                        " a registered target\n", progname);
                                return 1;
                        }
                        if (IS_MDT(&mop->mo_ldd) || IS_OST(&mop->mo_ldd)) {
                                mop->mo_ldd.ldd_svindex = atol(optarg);
                                mop->mo_ldd.ldd_flags &= ~LDD_F_NEED_INDEX;
                        } else {
                                badopt(long_opt[longidx].name, "MDT,OST");
                                return 1;
                        }
                        break;
                case 'k':
                        strscpy(mop->mo_mkfsopts, optarg,
                                sizeof(mop->mo_mkfsopts));
                        break;
                case 'L': {
                        char *tmp;
                        if (!(mop->mo_flags & MO_FORCEFORMAT) &&
                            (!(mop->mo_ldd.ldd_flags &
                               (LDD_F_UPGRADE14 | LDD_F_VIRGIN |
                                LDD_F_WRITECONF)))) {
                                fprintf(stderr, "%s: cannot change the name of"
                                        " a registered target\n", progname);
                                return 1;
                        }
                        if ((strlen(optarg) < 1) || (strlen(optarg) > 8)) {
                                fprintf(stderr, "%s: filesystem name must be "
                                        "1-8 chars\n", progname);
                                return 1;
                        }
                        if ((tmp = strpbrk(optarg, "/:"))) {
                                fprintf(stderr, "%s: char '%c' not allowed in "
                                        "filesystem name\n", progname, *tmp);
                                return 1;
                        }
                        strscpy(mop->mo_ldd.ldd_fsname, optarg,
                                sizeof(mop->mo_ldd.ldd_fsname));
                        break;
                }
                case 'm': {
                        char *nids = convert_hostnames(optarg);
                        if (!nids)
                                return 1;
                        rc = add_param(mop->mo_ldd.ldd_params, PARAM_MGSNODE,
                                       nids);
                        free(nids);
                        if (rc)
                                return rc;
                        mop->mo_mgs_failnodes++;
                        break;
                }
                case 'M':
                        mop->mo_ldd.ldd_flags |= LDD_F_SV_TYPE_MDT;
                        break;
                case 'n':
                        mop->mo_flags |= MO_DRYRUN;
                        break;
                case 'o':
                        *mountopts = optarg;
                        break;
                case 'O':
                        mop->mo_ldd.ldd_flags |= LDD_F_SV_TYPE_OST;
                        break;
                case 'p':
                        rc = add_param(mop->mo_ldd.ldd_params, NULL, optarg);
                        if (rc)
                                return rc;
                        /* Must update the mgs logs */
                        mop->mo_ldd.ldd_flags |= LDD_F_UPDATE;
                        break;
                case 'q':
                        verbose--;
                        break;
                case 'r':
                        mop->mo_flags |= MO_FORCEFORMAT;
                        break;
                case 't':
                        if (!IS_MDT(&mop->mo_ldd) && !IS_OST(&mop->mo_ldd)) {
                                badopt(long_opt[longidx].name, "MDT,OST");
                                return 1;
                        }

                        if (!optarg)
                                return 1;

                        rc = add_param(mop->mo_ldd.ldd_params,
                                       PARAM_NETWORK, optarg);
                        if (rc != 0)
                                return rc;
                        /* Must update the mgs logs */
                        mop->mo_ldd.ldd_flags |= LDD_F_UPDATE;
                        break;
                case 'u':
                        strscpy(mop->mo_ldd.ldd_userdata, optarg,
                                sizeof(mop->mo_ldd.ldd_userdata));
                        break;
                case 'v':
                        verbose++;
                        break;
                default:
                        if (opt != '?') {
                                fatal();
                                fprintf(stderr, "Unknown option '%c'\n", opt);
                        }
                        return EINVAL;
                }
        }

        if (optind == argc) {
                /* The user didn't specify device name */
                fatal();
                fprintf(stderr, "Not enough arguments - device name or "
                        "pool/dataset name not specified.\n");
                return EINVAL;
        } else {
                /*  The device or pool/filesystem name */
                strscpy(mop->mo_device, argv[optind], sizeof(mop->mo_device));

                /* Followed by optional vdevs */
                if (optind < argc - 1)
                        mop->mo_pool_vdevs = (char **) &argv[optind + 1];
        }

        return 0;
}

int main(int argc, char *const argv[])
{
        struct mkfs_opts mop;
        struct lustre_disk_data *ldd = &mop.mo_ldd;
        char *mountopts = NULL;
        char always_mountopts[512] = "";
        char default_mountopts[512] = "";
        unsigned mount_type;
        int ret = 0;

        if ((progname = strrchr(argv[0], '/')) != NULL)
                progname++;
        else
                progname = argv[0];

        if ((argc < 2) || (argv[argc - 1][0] == '-')) {
                usage(stderr);
                return EINVAL;
        }

        set_defaults(&mop);

        ret = osd_init();
        if (ret)
                return ret;

        ret = parse_opts(argc, argv, &mop, &mountopts);
        if (ret)
                goto out;

        if (!(IS_MDT(ldd) || IS_OST(ldd) || IS_MGS(ldd))) {
                fatal();
                fprintf(stderr, "must set target type: MDT,OST,MGS\n");
                ret = EINVAL;
                goto out;
        }

        if (((IS_MDT(ldd) || IS_MGS(ldd))) && IS_OST(ldd)) {
                fatal();
                fprintf(stderr, "OST type is exclusive with MDT,MGS\n");
                ret = EINVAL;
                goto out;
        }

        if (IS_MGS(ldd) || IS_MDT(ldd)) {
                /* Old MDT's are always index 0 (pre CMD) */
                mop.mo_ldd.ldd_flags &= ~LDD_F_NEED_INDEX;
                mop.mo_ldd.ldd_svindex = 0;
        }

        if ((mop.mo_ldd.ldd_flags & (LDD_F_NEED_INDEX | LDD_F_UPGRADE14)) ==
            (LDD_F_NEED_INDEX | LDD_F_UPGRADE14)) {
                fatal();
                fprintf(stderr, "The target index must be specified with "
                        "--index\n");
                ret = EINVAL;
                goto out;
        }

        ret = osd_prepare_lustre(&mop,
                                 default_mountopts, sizeof(default_mountopts),
                                 always_mountopts, sizeof(always_mountopts));
        if (ret) {
                fatal();
                fprintf(stderr, "unable to prepare backend (%d)\n", ret);
                goto out;
        }

        if (mountopts) {
                trim_mountfsoptions(mountopts);
                (void)check_mountfsoptions(mountopts, default_mountopts, 1);
                if (check_mountfsoptions(mountopts, always_mountopts, 0)) {
                        ret = EINVAL;
                        goto out;
                }
                sprintf(ldd->ldd_mount_opts, "%s", mountopts);
        } else {
                sprintf(ldd->ldd_mount_opts, "%s%s",
                        always_mountopts, default_mountopts);
                trim_mountfsoptions(ldd->ldd_mount_opts);
        }

        server_make_name(ldd->ldd_flags, ldd->ldd_svindex,
                         ldd->ldd_fsname, ldd->ldd_svname);

        if (verbose >= 0)
                osd_print_ldd("Permanent disk data", ldd);

        if (mop.mo_flags & MO_DRYRUN) {
                fprintf(stderr, "Dry-run, exiting before disk write.\n");
                goto out;
        }

        if (check_mtab_entry(mop.mo_device, mop.mo_device, NULL, NULL)) {
                fprintf(stderr, "filesystem %s is mounted.\n", mop.mo_device);
                ret = EEXIST;
                goto out;
        }

        /* Check whether the disk has already been formatted by mkfs.lustre */
        if (!(mop.mo_flags & MO_FORCEFORMAT)) {
                ret = osd_is_lustre(mop.mo_device, &mount_type);
                if (ret) {
                        fatal();
                        fprintf(stderr, "Device %s was previously formatted "
                                "for lustre.\nUse --reformat to reformat it, "
                                "or tunefs.lustre to modify.\n",
                                mop.mo_device);
                        goto out;
                }
        }

        /* Format the backing filesystem */
        ret = osd_make_lustre(&mop);
        if (ret != 0) {
                fatal();
                fprintf(stderr, "mkfs failed %d\n", ret);
                goto out;
        }

        /* Write our config files */
        ret = osd_write_ldd(&mop);
        if (ret != 0) {
                fatal();
                fprintf(stderr, "failed to write local files\n");
                goto out;
        }

out:
        loop_cleanup(&mop);
        osd_fini();

        /* Fix any crazy return values from system() */
        if (ret && ((ret & 255) == 0))
                return (1);
        if (ret)
                verrprint("%s: exiting with %d (%s)\n",
                          progname, ret, strerror(ret));
        return (ret);
}
