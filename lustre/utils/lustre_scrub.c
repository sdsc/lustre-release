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
 * OI Scrub is used to rebuild Object Index files when restore MDT from
 * file-level backup. And also can be part of consistency routine check.
 *
 * Lustre user-space tools for OI Scrub.
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
#include <lustre/lustre_user.h>
#include <libcfs/libcfsutil.h>
#include <lnet/lnetctl.h>

#define KEY_STRING_LEN          32
#define TIME_STRING_LEN         32
#define INFO_STRING_LEN         4096
#define PARAM_STRING_LEN        512

static struct option long_opt_start[] = {
        {"device",      required_argument, 0, 'M'},
        {"help",        no_argument,       0, 'h'},
        {"error",       required_argument, 0, 'e'},
        {"interval",    required_argument, 0, 'i'},
        {"method",      required_argument, 0, 'm'},
        {"dryrun",      required_argument, 0, 'n'},
        {"reset",       no_argument,       0, 'r'},
        {"speed",       required_argument, 0, 's'},
        {"window",      required_argument, 0, 'w'},
        {0,             0,                 0,   0}
};

static struct option long_opt_stop[] = {
        {"device",      required_argument, 0, 'M'},
        {"help",        no_argument,       0, 'h'},
        {0,             0,                 0,   0}
};

static struct option long_opt_show[] = {
        {"device",      required_argument, 0, 'M'},
        {"help",        no_argument,       0, 'h'},
        {"info",        no_argument,       0, 'i'},
        {"param",       no_argument,       0, 'p'},
        {0,             0,                 0,   0}
};

static void usage_start(void)
{
        fprintf(stderr, "Start OI Scrub.\n"
                "SYNOPSIS:\n"
                "scrub_start <-M | --device MDT_device> [-h | --help]\n"
                "            [-e | --error error_handle]\n"
                "            [-i | --interval checkpoint_interval]\n"
                "            [-m | --method iteration_method]\n"
                "            [-n | --dryrun switch] [-r | --reset]\n"
                "            [-s | --speed speed_limit]\n"
                "            [-w | --window pipeline_window]\n"
                "OPTIONS:\n"
                "-M: The MDT device to start OI Scrub on.\n"
                "-h: Help information.\n"
                "-e: Error handle, 'continue'(default)|'abort'.\n"
                "-i: Interval time (seconds) between two checkpoints. "
                    "The valid value should be [1 - %d]. "
                    "The default value is %d.\n"
                "-m: Method for scanning the MDT device. "
                    "'itable' (itable-based iteration, default), "
                    "'namespace' (not support yet), or others (in future).\n"
                "-n: Check without modification. 'off'(default)|'on'.\n"
                "-r: Reset OI Scrub start position to the device beginning.\n"
                "-s: How many items can be scanned at most per second. "
                    "'0' means no limit (default).\n"
                "-w: For pipeline iterators, how many items the low layer "
                    "iterator can be ahead of the up layer iterator. "
                    "'0' means on limit. '%d' by default.\n",
                SPM_CHECKPOING_INTERVAL, SPD_CHECKPOING_INTERVAL,
                SPD_PIPELINE_WINDOW);
}

static void usage_stop(void)
{
        fprintf(stderr, "Stop OI Scrub.\n"
                "SYNOPSIS:\n"
                "scrub_show <-M | --device MDT_device> [-h | --help]\n"
                "OPTIONS:\n"
                "-M: The MDT device to stop OI Scrub on.\n"
                "-h: Help information.\n");
}

static void usage_show(void)
{
        fprintf(stderr, "Show OI Scrub.\n"
                "SYNOPSIS:\n"
                "scrub_show <-M | --device MDT_device> [-h | --help]\n"
                "           [-i | --info] [-p | --param]\n"
                "OPTIONS:\n"
                "-M: The MDT device to show OI Scrub on.\n"
                "-h: Help information.\n"
                "-i: Show OI Scrub statistics, and other information.\n"
                "-p: Show OI Scrub parameters.\n");
}

static const char *scrub_flags_names[32] = {
        "failout",
        "dryrun",
        "rebuild OI",
        "convert igif",
        "rebuild linkea",
        NULL
};

static const char *scrub_status_names[32] = {
        "init",
        "restored",
        "rebuilding",
        "completed",
        "failed",
        "paused",
        "dryrun-scanning",
        "dryrun-scanned",
        NULL
};

static void
scrub_key_to_str(char *str, const union scrub_key *key, int keysize)
{
        switch (keysize) {
        case 16:
                snprintf(str, KEY_STRING_LEN - 1, LPU64"/"LPU64,
                         key->sk_u64[0], key->sk_u64[1]);
                break;
        case 8:
                snprintf(str, KEY_STRING_LEN - 1, LPU64, key->sk_u64[0]);
                break;
        case 4:
                snprintf(str, KEY_STRING_LEN - 1, "%u", key->sk_u32[0]);
                break;
        default:
                snprintf(str, KEY_STRING_LEN - 1, "unknown");
                break;
        }
}

static int
scrub_param_to_str(char *str, const struct scrub_param *param, int size)
{
        char *save = str, c = ':';
        __u32 flags;
        int rc, i;

        memset(str, 0, size);
        size--; /* for the last '\0' */

        rc = snprintf(str, size, "Parameters:\n");
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        if (param->sp_method == SM_ITABLE)
                rc = snprintf(str, size,
                              "method: inode table based iteration\n");
        else
                rc = snprintf(str, size, "method: unknown");
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        rc = snprintf(str, size, "checkpoint interval: %u sec\n",
                      param->sp_checkpoint_interval);
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        if (param->sp_speed_limit == 0)
                rc = snprintf(str, size, "speed limit: no limit\n");
        else
                rc = snprintf(str, size, "speed limit: %u objs/sec\n",
                              param->sp_speed_limit);
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        rc = snprintf(str, size, "pipeline window: %u\n",
                      param->sp_pipeline_window);
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        rc = snprintf(str, size, "flags");
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        for (flags = param->sp_flags, i = 0; flags != 0; i++) {
                __u32 bit = 1 << i;

                if (flags & bit) {
                        if (scrub_flags_names[i] != NULL)
                                rc = snprintf(str, size, "%c %s", c,
                                              scrub_flags_names[i]);
                        else
                                rc = snprintf(str, size, "%c unknown flag "
                                              "(0x%x)", c, bit);
                        if (rc <= 0)
                                return -ENOSPC;
                        size -= rc;
                        str  += rc;

                        c = ',';
                        flags &= ~bit;
                }
        }

        rc = snprintf(str, size, "\n");
        if (rc <= 0)
                return -ENOSPC;

        return str + rc - save;
}

static inline void scrub_ctime(__u64 src, char *str)
{
        time_t des = (time_t)src;

        if (src == 0)
                sprintf(str, "N/A\n");
        else
                sprintf(str, "%s", ctime(&des));
}

static int
scrub_info_to_str(char *str, struct scrub_header *header, int size)
{
        char key_str1[KEY_STRING_LEN];
        char key_str2[KEY_STRING_LEN];
        char time_str1[TIME_STRING_LEN];
        char time_str2[TIME_STRING_LEN];
        char time_str3[TIME_STRING_LEN];
        char *save = str;
        int rc;

        memset(str, 0, size);
        size--; /* for the last '\0' */

        scrub_ctime(header->sh_time_last_complete, time_str1);
        scrub_ctime(header->sh_time_initial_start, time_str2);
        scrub_ctime(header->sh_time_latest_start, time_str3);
        rc = snprintf(str, size,
                      "Information:\n"
                      "version: %d\n"
                      "keysize: %d\n"
                      "success count: %u\n"
                      "last completed time: %s"
                      "initial start time: %s"
                      "latest start time: %s",
                      header->sh_version,
                      header->sh_keysize,
                      header->sh_success_count,
                      time_str1,
                      time_str2,
                      time_str3);
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        if (header->sh_keysize == 0)
                goto out;

        scrub_key_to_str(key_str1, &header->sh_position_latest_start,
                         header->sh_keysize);
        rc = snprintf(str, size, "latest start position: %s\n", key_str1);
        if (rc <= 0)
                return -ENOSPC;
        size -= rc;
        str  += rc;

        if (header->sh_param.sp_flags & SPF_DRYRUN) {
                struct scrub_info_dryrun *dinfo = &header->sh_dryrun;

                scrub_ctime(dinfo->sid_time_last_checkpoint, time_str1);
                scrub_key_to_str(key_str1, &dinfo->sid_position_last_checkpoint,
                                 header->sh_keysize);
                scrub_key_to_str(key_str2, &dinfo->sid_position_first_unmatched,
                                 header->sh_keysize);
                rc = snprintf(str, size, "\nStatistics for dryrun mode:\n"
                              "last checkpoint time: %s"
                              "last checkpoint position: %s\n"
                              "first unmatched position: %s\n"
                              "scanned: "LPU64"\n"
                              "unmatched: "LPU64"\n"
                              "failed: "LPU64"\n"
                              "prior updated: "LPU64"\n"
                              "run time: %u sec\n"
                              "average speed: "LPU64" objs/sec\n",
                              time_str1,
                              key_str1,
                              key_str2,
                              dinfo->sid_items_scanned,
                              dinfo->sid_items_unmatched,
                              dinfo->sid_items_failed,
                              dinfo->sid_items_updated_prior,
                              dinfo->sid_time,
                              dinfo->sid_time == 0 ? dinfo->sid_items_scanned :
                              dinfo->sid_items_scanned / dinfo->sid_time);
                if (rc <= 0)
                        return -ENOSPC;
                size -= rc;
                str  += rc;

                if (header->sh_current_status == SS_DRURUN_SACNNING) {
                        rc = snprintf(str, size, "real-time speed: %u "
                                      "objs/sec\n", dinfo->sid_speed);
                        if (rc <= 0)
                                return -ENOSPC;
                        size -= rc;
                        str  += rc;
                }
        } else {
                struct scrub_info_normal *ninfo = &header->sh_normal;

                scrub_ctime(ninfo->sin_time_last_checkpoint, time_str1);
                scrub_key_to_str(key_str1, &ninfo->sin_position_last_checkpoint,
                                 header->sh_keysize);
                rc = snprintf(str, size, "\nStatistics for normal mode:\n"
                              "last checkpoint time: %s"
                              "last checkpoint position: %s\n"
                              "scanned: "LPU64"\n"
                              "updated: "LPU64"\n"
                              "failed: "LPU64"\n"
                              "run time: %u sec\n"
                              "average speed: "LPU64" objs/sec\n",
                              time_str1,
                              key_str1,
                              ninfo->sin_items_scanned,
                              ninfo->sin_items_updated,
                              ninfo->sin_items_failed,
                              ninfo->sin_time,
                              ninfo->sin_time == 0 ? ninfo->sin_items_scanned :
                              ninfo->sin_items_scanned / ninfo->sin_time);
                if (rc <= 0)
                        return -ENOSPC;
                size -= rc;
                str  += rc;

                if (header->sh_current_status == SS_REBUILDING) {
                        rc = snprintf(str, size, "real-time speed: %u "
                                      "objs/sec\n", ninfo->sin_speed);
                        if (rc <= 0)
                                return -ENOSPC;
                        size -= rc;
                        str  += rc;
                }
        }

out:
        return str - save;
}

int jt_scrub_start(int argc, char **argv)
{
        struct obd_ioctl_data data;
        char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
        char key_str[KEY_STRING_LEN];
        char param_str[PARAM_STRING_LEN];
        char device[MAX_OBD_NAME];
        struct start_scrub_param ssp;
        struct scrub_param *sp = &ssp.ssp_param;
        char *optstring = "M:he:i:m:n:rs:w:";
        int opt, index, rc, val;

        memset(&data, 0, sizeof(data));
        memset(&ssp, 0, sizeof(ssp));
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
                case 'h':
                        usage_start();
                        return 0;
                case 'e':
                        if (strcmp(optarg, "abort") == 0) {
                                sp->sp_flags |= SPF_FAILOUT;
                        } else if (strcmp(optarg, "continue") != 0) {
                                fprintf(stderr, "Invalid error handler: %s. "
                                        "The valid ones are: 'continue'"
                                        "(default)|'abort'.\n", optarg);
                                return -EINVAL;
                        }
                        ssp.ssp_valid |= SSP_ERROR_HANDLE;
                        break;
                case 'i':
                        val = atoi(optarg);
                        if (val < 1 || val > SPM_CHECKPOING_INTERVAL) {
                                fprintf(stderr, "Invalid checkpoint interval "
                                        "%d. The valid interval should be [1 - "
                                        "%d].\n", val, SPM_CHECKPOING_INTERVAL);
                                return -EINVAL;
                        }
                        sp->sp_checkpoint_interval = val;
                        ssp.ssp_valid |= SSP_CHECKPOING_INTERVAL;
                        break;
                case 'm':
                        if (strcmp(optarg, "itable") == 0) {
                                sp->sp_method = SM_ITABLE;
                        } else if (strcmp(optarg, "namespace") == 0) {
                                sp->sp_method = SM_NAMESPACE;
                        } else {
                                fprintf(stderr, "Invalid method: %s. "
                                        "The valid ones are: 'itable'(default)"
                                        ", or 'namespace'(not support yet)"
                                        ", or others (in future).\n", optarg);
                                return -EINVAL;
                        }
                        ssp.ssp_valid |= SSP_METHOD;
                        break;
                case 'n':
                        if (strcmp(optarg, "on") == 0) {
                                sp->sp_flags |= SPF_DRYRUN;
                        } else if (strcmp(optarg, "off") != 0) {
                                fprintf(stderr, "Invalid dryrun switch: %s. "
                                        "The valid ones are: 'off'(default)|"
                                        "'on'\n", optarg);
                                return -EINVAL;
                        }
                        ssp.ssp_valid |= SSP_DRYRUN;
                        break;
                case 'r':
                        ssp.ssp_valid |= SSP_RESET;
                        break;
                case 's':
                        val = atoi(optarg);
                        sp->sp_speed_limit = val;
                        ssp.ssp_valid |= SSP_SPEED_LIMIT;
                        break;
                case 'w':
                        val = atoi(optarg);
                        sp->sp_pipeline_window = val;
                        ssp.ssp_valid |= SSP_ITERATOR_WINDOW;
                        break;
                default:
                        fprintf(stderr, "Invalid option, '-h' for help.\n");
                        return -EINVAL;
                }
        }

        if (data.ioc_inlbuf4 == NULL) {
                fprintf(stderr,
                        "Miss to sepcify MDT device for OI Scrub start.\n");
                return -EINVAL;
        }

        memset(device, 0, MAX_OBD_NAME);
        memcpy(device, data.ioc_inlbuf4, data.ioc_inllen4);
        data.ioc_inlbuf4 = device;
        data.ioc_inlbuf1 = (char *)&ssp;
        data.ioc_inllen1 = sizeof(ssp);
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

        obd_ioctl_unpack(&data, buf, sizeof(rawbuf));
        scrub_key_to_str(key_str, &ssp.ssp_start_point, ssp.ssp_keysize);
        rc = scrub_param_to_str(param_str, sp, PARAM_STRING_LEN);
        /* For debug: if the buffer is not enough, enlarge it. */
        LASSERT(rc > 0);

        printf("Start OI Scrub on the MDT device %s:\n"
               "version: %d\nkeysize: %d\nstart position: %s\n\n%s",
               device, ssp.ssp_version, ssp.ssp_keysize, key_str, param_str);

        return 0;
}

int jt_scrub_stop(int argc, char **argv)
{
        struct obd_ioctl_data data;
        char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
        char info_str[INFO_STRING_LEN];
        char device[MAX_OBD_NAME];
        struct scrub_show show;
        char *optstring = "M:h";
        int opt, index, rc;

        memset(&data, 0, sizeof(data));
        memset(&show, 0, sizeof(show));
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
                        "Miss to sepcify MDT device for OI Scrub stop.\n");
                return -EINVAL;
        }

        memset(device, 0, MAX_OBD_NAME);
        memcpy(device, data.ioc_inlbuf4, data.ioc_inllen4);
        data.ioc_inlbuf4 = device;
        data.ioc_inlbuf1 = (char *)&show;
        data.ioc_inllen1 = sizeof(show);
        memset(buf, 0, sizeof(rawbuf));
        rc = obd_ioctl_pack(&data, &buf, sizeof(rawbuf));
        if (rc) {
                fprintf(stderr, "Fail to pack ioctl data: rc = %d.\n", rc);
                return rc;
        }

        rc = l_ioctl(OBD_DEV_ID, OBD_IOC_STOP_SCRUB, buf);
        if (rc < 0) {
                perror("Fail to stop OI Scrub");
                return rc;
        }

        obd_ioctl_unpack(&data, buf, sizeof(rawbuf));
        rc = scrub_info_to_str(info_str, &show.ss_header, INFO_STRING_LEN);
        /* For debug: if the buffer is not enough, enlarge it. */
        LASSERT(rc > 0);

        printf("Stop OI Scrub on the MDT device %s:\n\n%s",
               device, info_str);

        return 0;
}

int jt_scrub_show(int argc, char **argv)
{
        struct obd_ioctl_data data;
        char rawbuf[MAX_IOC_BUFLEN], *buf = rawbuf;
        char key_str[KEY_STRING_LEN];
        char info_str[INFO_STRING_LEN];
        char param_str[PARAM_STRING_LEN];
        char device[MAX_OBD_NAME];
        struct scrub_show show;
        struct scrub_header *header = &show.ss_header;
        char *optstring = "M:hip";
        int opt, index, rc, info = 0, param = 0;

        memset(&data, 0, sizeof(data));
        memset(&show, 0, sizeof(show));
        while ((opt = getopt_long(argc, argv, optstring, long_opt_show,
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
                        usage_show();
                        return 0;
                case 'i':
                        info = 1;
                        break;
                case 'p':
                        param = 1;
                        break;
                default:
                        fprintf(stderr, "Invalid option, '-h' for help.\n");
                        return -EINVAL;
                }
        }

        if (data.ioc_inlbuf4 == NULL) {
                fprintf(stderr,
                        "Miss to sepcify MDT device for OI Scrub show.\n");
                return -EINVAL;
        }

        memset(device, 0, MAX_OBD_NAME);
        memcpy(device, data.ioc_inlbuf4, data.ioc_inllen4);
        data.ioc_inlbuf4 = device;
        data.ioc_inlbuf1 = (char *)&show;
        data.ioc_inllen1 = sizeof(show);
        memset(buf, 0, sizeof(rawbuf));
        rc = obd_ioctl_pack(&data, &buf, sizeof(rawbuf));
        if (rc) {
                fprintf(stderr, "Fail to pack ioctl data: rc = %d.\n", rc);
                return rc;
        }

        rc = l_ioctl(OBD_DEV_ID, OBD_IOC_SHOW_SCRUB, buf);
        if (rc < 0) {
                perror("Fail to show OI Scrub");
                return rc;
        }

        obd_ioctl_unpack(&data, buf, sizeof(rawbuf));
        printf("Show OI Scrub on the MDT device %s:\n"
               "Current Status: %s\n",
               device,
               scrub_status_names[header->sh_current_status] != NULL ?
               scrub_status_names[header->sh_current_status] : "unknown");

        if (header->sh_current_status == SS_REBUILDING ||
            header->sh_current_status == SS_DRURUN_SACNNING) {
                scrub_key_to_str(key_str, &show.ss_current_position,
                                 header->sh_keysize);
                printf("Current Position: %s\n", key_str);
        }

        if (info != 0) {
                rc = scrub_info_to_str(info_str, header, INFO_STRING_LEN);
                /* For debug: if the buffer is not enough, enlarge it. */
                LASSERT(rc > 0);

                printf("\n%s", info_str);
        }

        if (param != 0) {
                rc = scrub_param_to_str(param_str, &header->sh_param,
                                        PARAM_STRING_LEN);
                /* For debug: if the buffer is not enough, enlarge it. */
                LASSERT(rc > 0);

                printf("\n%s", param_str);
        }

        return 0;
}
