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
 * Copyright (c) 2011 Whamcloud, Inc.
 */
/*
 * lustre/utils/lustre_scrub.c
 *
 * Lustre user-space tools for Lustre Scrub.
 *
 * Author: Fan Yong <yong.fan@whamcloud.com>
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <time.h>

#include "obdctl.h"

#include <obd.h>
#include <lustre/lustre_scrub_user.h>
#include <libcfs/libcfsutil.h>
#include <lnet/lnetctl.h>

static struct option long_opt_start[] = {
        {"device",      required_argument, 0, 'M'},
        {"error",       required_argument, 0, 'e'},
        {"help",        no_argument,       0, 'h'},
        {"method",      required_argument, 0, 'm'},
        {"dryrun",      required_argument, 0, 'n'},
        {"reset",       no_argument,       0, 'r'},
        {"speed",       required_argument, 0, 's'},
        {"type",        required_argument, 0, 't'},
        {0,             0,                 0,   0}
};

static struct option long_opt_stop[] = {
        {"device",      required_argument, 0, 'M'},
        {"help",        no_argument,       0, 'h'},
        {0,             0,                 0,   0}
};

struct scrub_types_names {
        char   *name;
        __u16   type;
};

static struct scrub_types_names scrub_types_names[3] = {
        { "OI",         ST_OI_SCRUB },
        { "layout",     ST_LAYOUT_SCRUB },
        { "DNE",        ST_DNE_SCRUB }
};

static void usage_start(void)
{
        fprintf(stderr, "Start Lustre Scrub.\n"
                "SYNOPSIS:\n"
                "scrub_start <-M | --device MDT_device>\n"
                "            [-e | --error error_handle] [-h | --help]\n"
                "            [-m | --method iteration_method]\n"
                "            [-n | --dryrun switch] [-r | --reset]\n"
                "            [-s | --speed speed_limit]\n"
                "            [-t | --type scrub_type[,scrub_type...]]\n"
                "OPTIONS:\n"
                "-M: The MDT device to start Lustre Scrub on.\n"
                "-e: Error handle, 'continue'(default) or 'abort'.\n"
                "-h: Help information.\n"
                "-m: Method for scanning the MDT device. "
                    "'otable' (otable-based iteration, default), "
                    "'namespace' (not support yet), or others (in future).\n"
                "-n: Check without modification. 'off'(default) or 'on'.\n"
                "-r: Reset scanning start position to the device beginning.\n"
                "-s: How many items can be scanned at most per second. "
                    "'%d' means no limit (default).\n"
                "-t: The Scrub type(s) to be started.\n",
                SP_SPEED_NO_LIMIT);
}

static void usage_stop(void)
{
        fprintf(stderr, "Stop Lustre Scrub.\n"
                "SYNOPSIS:\n"
                "scrub_stop <-M | --device MDT_device> [-h | --help]\n"
                "OPTIONS:\n"
                "-M: The MDT device to stop Lustre Scrub on.\n"
                "-h: Help information.\n");
}

int jt_scrub_start(int argc, char **argv)
{
        struct obd_ioctl_data data;
        char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
        char device[MAX_OBD_NAME];
        struct scrub_start start;
        char *optstring = "M:e:hi:m:n:rs:t:";
        int opt, index, rc, val, i;

        memset(&data, 0, sizeof(data));
        memset(&start, 0, sizeof(start));
        start.ss_version = SCRUB_VERSION_V1;
        start.ss_flags = SPF_FORCE;
        start.ss_active = SCRUB_TYPES_DEF;
        while ((opt = getopt_long(argc, argv, optstring, long_opt_start,
                                  &index)) != EOF) {
                switch (opt) {
                case 'M':
                        data.ioc_inllen4 = strlen(optarg) + 1;
                        if (data.ioc_inllen4 > MAX_OBD_NAME) {
                                fprintf(stderr, "MDT device name is too long. "
                                        "Valid length should be less than %d\n",
                                        MAX_OBD_NAME);
                                return -EINVAL;
                        }

                        data.ioc_inlbuf4 = optarg;
                        data.ioc_dev = OBD_DEV_BY_DEVNAME;
                        break;
                case 'e':
                        if (strcmp(optarg, "abort") == 0) {
                                start.ss_flags |= SPF_FAILOUT;
                        } else if (strcmp(optarg, "continue") != 0) {
                                fprintf(stderr, "Invalid error handler: %s. "
                                        "The valid value should be: 'continue'"
                                        "(default) or 'abort'.\n", optarg);
                                return -EINVAL;
                        }
                        start.ss_valid |= SSV_ERROR_HANDLE;
                        break;
                case 'h':
                        usage_start();
                        return 0;
                case 'm':
                        if (strcmp(optarg, "otable") == 0) {
                                start.ss_method = SM_OTABLE;
                        } else if (strcmp(optarg, "namespace") == 0) {
                                start.ss_method = SM_NAMESPACE;
                        } else {
                                fprintf(stderr,
                                        "Invalid method: %s. The valid "
                                        "value should be: 'otable'(default), "
                                        "or 'namespace'(not support yet), "
                                        "or others (in future).\n", optarg);
                                return -EINVAL;
                        }
                        start.ss_valid |= SSV_METHOD;
                        break;
                case 'n':
                        if (strcmp(optarg, "on") == 0) {
                                start.ss_flags |= SPF_DRYRUN;
                        } else if (strcmp(optarg, "off") != 0) {
                                fprintf(stderr, "Invalid dryrun switch: %s. "
                                        "The valid value shou be: 'off'"
                                        "(default) or 'on'\n", optarg);
                                return -EINVAL;
                        }
                        start.ss_valid |= SSV_DRYRUN;
                        break;
                case 'r':
                        start.ss_flags |= SPF_RESET;
                        break;
                case 's':
                        val = atoi(optarg);
                        start.ss_speed_limit = val;
                        start.ss_valid |= SSV_SPEED_LIMIT;
                        break;
                case 't': {
                        char *str = optarg, *p, c;

                        start.ss_active = 0;
                        while (*str) {
                                while (*str == ' ' || *str == ',')
                                        str++;

                                if (*str == 0)
                                        break;

                                p = str;
                                while (*p != 0 && *p != ' ' && *p != ',')
                                        p++;

                                c = *p;
                                *p = 0;
                                for (i = 0; i < 3; i++) {
                                        if (strcmp(str,
                                                   scrub_types_names[i].name)
                                                   == 0) {
                                                start.ss_active |=
                                                    scrub_types_names[i].type;
                                                break;
                                        }
                                }
                                *p = c;
                                str = p;

                                if (i >= 3 ) {
                                        fprintf(stderr, "Invalid scrub type.\n"
                                                "The valid value should be "
                                                "'OI' or 'layout' or 'DNE'.\n");
                                        return -EINVAL;
                                }
                        }
                        if (start.ss_active == 0) {
                                fprintf(stderr, "Miss scrub type(s).\n"
                                        "The valid value should be 'OI' or "
                                        "'layout' or 'DNE'.\n");
                                return -EINVAL;
                        }
                        break;
                }
                default:
                        fprintf(stderr, "Invalid option, '-h' for help.\n");
                        return -EINVAL;
                }
        }

        if (data.ioc_inlbuf4 == NULL) {
                fprintf(stderr,
                        "Must sepcify MDT device to start Lustre Scrub.\n");
                return -EINVAL;
        }

        memset(device, 0, MAX_OBD_NAME);
        memcpy(device, data.ioc_inlbuf4, data.ioc_inllen4);
        data.ioc_inlbuf4 = device;
        data.ioc_inlbuf1 = (char *)&start;
        data.ioc_inllen1 = sizeof(start);
        memset(buf, 0, sizeof(rawbuf));
        rc = obd_ioctl_pack(&data, &buf, sizeof(rawbuf));
        if (rc) {
                fprintf(stderr, "Fail to pack ioctl data: rc = %d.\n", rc);
                return rc;
        }

        rc = l_ioctl(OBD_DEV_ID, OBD_IOC_START_SCRUB, buf);
        if (rc < 0) {
                perror("Fail to start OI Scrub");
                return rc;
        }

        return 0;
}

int jt_scrub_stop(int argc, char **argv)
{
        struct obd_ioctl_data data;
        char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
        char device[MAX_OBD_NAME];
        char *optstring = "M:h";
        int opt, index, rc;

        memset(&data, 0, sizeof(data));
        while ((opt = getopt_long(argc, argv, optstring, long_opt_stop,
                                  &index)) != EOF) {
                switch (opt) {
                case 'M':
                        data.ioc_inllen4 = strlen(optarg) + 1;
                        if (data.ioc_inllen4 > MAX_OBD_NAME) {
                                fprintf(stderr, "MDT device name is too long. "
                                        "Valid length should be less than %d\n",
                                        MAX_OBD_NAME);
                                return -EINVAL;
                        }

                        data.ioc_inlbuf4 = optarg;
                        data.ioc_dev = OBD_DEV_BY_DEVNAME;
                        break;
                case 'h':
                        usage_stop();
                        return 0;
                default:
                        fprintf(stderr, "Invalid option, '-h' for help.\n");
                        return -EINVAL;
                }
        }

        if (data.ioc_inlbuf4 == NULL) {
                fprintf(stderr,
                        "Must sepcify MDT device to stop Lustre Scrub.\n");
                return -EINVAL;
        }

        memset(device, 0, MAX_OBD_NAME);
        memcpy(device, data.ioc_inlbuf4, data.ioc_inllen4);
        data.ioc_inlbuf4 = device;
        memset(buf, 0, sizeof(rawbuf));
        rc = obd_ioctl_pack(&data, &buf, sizeof(rawbuf));
        if (rc) {
                fprintf(stderr, "Fail to pack ioctl data: rc = %d.\n", rc);
                return rc;
        }

        rc = l_ioctl(OBD_DEV_ID, OBD_IOC_STOP_SCRUB, buf);
        if (rc < 0) {
                perror("Fail to stop Lustre Scrub");
                return rc;
        }

        return 0;
}
