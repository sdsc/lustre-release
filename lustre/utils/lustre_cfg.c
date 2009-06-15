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
 * Copyright  2008 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/utils/lustre_cfg.c
 *
 * Author: Peter J. Braam <braam@clusterfs.com>
 * Author: Phil Schwan <phil@clusterfs.com>
 * Author: Andreas Dilger <adilger@clusterfs.com>
 * Author: Robert Read <rread@clusterfs.com>
 */

#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <glob.h>

#ifndef __KERNEL__
#include <liblustre.h>
#endif
#include <lustre_lib.h>
#include <lustre_cfg.h>
#include <lustre/lustre_idl.h>
#include <lustre_dlm.h>
#include <obd.h>          /* for struct lov_stripe_md */
#include <obd_lov.h>
#include <lustre/lustre_build_version.h>

#include <unistd.h>
#include <sys/un.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>


#include "obdctl.h"
#include <lnet/lnetctl.h>
#include "parser.h"
#include <stdio.h>

static char * lcfg_devname;

int lcfg_set_devname(char *name)
{
        if (name) {
                if (lcfg_devname)
                        free(lcfg_devname);
                /* quietly strip the unnecessary '$' */
                if (*name == '$' || *name == '%')
                        name++;
                if (isdigit(*name)) { 
                        /* We can't translate from dev # to name */
                        lcfg_devname = NULL;
                } else {
                        lcfg_devname = strdup(name);
                }
        } else {
                lcfg_devname = NULL;
        } 
        return 0;
}

char * lcfg_get_devname(void)
{
        return lcfg_devname;
}

int jt_lcfg_device(int argc, char **argv)
{
        return jt_obd_device(argc, argv);
}

int jt_lcfg_attach(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        int rc;

        if (argc != 4)
                return CMD_HELP;

        lustre_cfg_bufs_reset(&bufs, NULL);

        lustre_cfg_bufs_set_string(&bufs, 1, argv[1]);
        lustre_cfg_bufs_set_string(&bufs, 0, argv[2]);
        lustre_cfg_bufs_set_string(&bufs, 2, argv[3]);

        lcfg = lustre_cfg_new(LCFG_ATTACH, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: LCFG_ATTACH %s\n",
                        jt_cmdname(argv[0]), strerror(rc = errno));
        } else if (argc == 3) {
                char name[1024];

                lcfg_set_devname(argv[2]);
                if (strlen(argv[2]) > 128) {
                        printf("Name too long to set environment\n");
                        return -EINVAL;
                }
                snprintf(name, 512, "LUSTRE_DEV_%s", argv[2]);
                rc = setenv(name, argv[1], 1);
                if (rc) {
                        printf("error setting env variable %s\n", name);
                }
        } else {
                lcfg_set_devname(argv[2]);
        }

        return rc;
}

int jt_lcfg_setup(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        int i;
        int rc;

        if (lcfg_devname == NULL) {
                fprintf(stderr, "%s: please use 'device name' to set the "
                        "device name for config commands.\n", 
                        jt_cmdname(argv[0])); 
                return -EINVAL;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        if (argc > 6)
                return CMD_HELP;

        for (i = 1; i < argc; i++) {
                lustre_cfg_bufs_set_string(&bufs, i, argv[i]);
        }

        lcfg = lustre_cfg_new(LCFG_SETUP, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0)
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));

        return rc;
}

int jt_obd_detach(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        int rc;

        if (lcfg_devname == NULL) {
                fprintf(stderr, "%s: please use 'device name' to set the "
                        "device name for config commands.\n", 
                        jt_cmdname(argv[0])); 
                return -EINVAL;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        if (argc != 1)
                return CMD_HELP;

        lcfg = lustre_cfg_new(LCFG_DETACH, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0)
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));

        return rc;
}

int jt_obd_cleanup(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        char force = 'F';
        char failover = 'A';
        char flags[3] = { 0 };
        int flag_cnt = 0, n;
        int rc;

        if (lcfg_devname == NULL) {
                fprintf(stderr, "%s: please use 'device name' to set the "
                        "device name for config commands.\n", 
                        jt_cmdname(argv[0])); 
                return -EINVAL;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        if (argc < 1 || argc > 3)
                return CMD_HELP;

        /* we are protected from overflowing our buffer by the argc
         * check above
         */
        for (n = 1; n < argc; n++) {
                if (strcmp(argv[n], "force") == 0) {
                        flags[flag_cnt++] = force;
                } else if (strcmp(argv[n], "failover") == 0) {
                        flags[flag_cnt++] = failover;
                } else {
                        fprintf(stderr, "unknown option: %s", argv[n]);
                        return CMD_HELP;
                }
        }

        if (flag_cnt) {
                lustre_cfg_bufs_set_string(&bufs, 1, flags);
        }

        lcfg = lustre_cfg_new(LCFG_CLEANUP, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0)
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));

        return rc;
}

static 
int do_add_uuid(char * func, char *uuid, lnet_nid_t nid) 
{
        int rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);
        if (uuid)
                lustre_cfg_bufs_set_string(&bufs, 1, uuid);

        lcfg = lustre_cfg_new(LCFG_ADD_UUID, &bufs);
        lcfg->lcfg_nid = nid;
        /* Poison NAL -- pre 1.4.6 will LASSERT on 0 NAL, this way it 
           doesn't work without crashing (bz 10130) */
        lcfg->lcfg_nal = 0x5a;

#if 0
        fprintf(stderr, "adding\tnid: %d\tuuid: %s\n",
               lcfg->lcfg_nid, uuid);
#endif
        rc = lcfg_ioctl(func, OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc) {
                fprintf(stderr, "IOC_PORTAL_ADD_UUID failed: %s\n",
                        strerror(errno));
                return -1;
        }

        printf ("Added uuid %s: %s\n", uuid, libcfs_nid2str(nid));
        return 0;
}

int jt_lcfg_add_uuid(int argc, char **argv)
{
        lnet_nid_t nid;
        
        if (argc != 3) {                
                return CMD_HELP;
        }

        nid = libcfs_str2nid(argv[2]);
        if (nid == LNET_NID_ANY) {
                fprintf (stderr, "Can't parse NID %s\n", argv[2]);
                return (-1);
        }

        return do_add_uuid(argv[0], argv[1], nid);
}

int obd_add_uuid(char *uuid, lnet_nid_t nid)
{
        return do_add_uuid("obd_add_uuid", uuid, nid);
}

int jt_lcfg_del_uuid(int argc, char **argv)
{
        int rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        if (argc != 2) {
                fprintf(stderr, "usage: %s <uuid>\n", argv[0]);
                return 0;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);
        if (strcmp (argv[1], "_all_"))
                lustre_cfg_bufs_set_string(&bufs, 1, argv[1]);
        
        lcfg = lustre_cfg_new(LCFG_DEL_UUID, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc) {
                fprintf(stderr, "IOC_PORTAL_DEL_UUID failed: %s\n",
                        strerror(errno));
                return -1;
        }
        return 0;
}




int jt_lcfg_del_mount_option(int argc, char **argv)
{
        int rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        if (argc != 2)
                return CMD_HELP;

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        /* profile name */
        lustre_cfg_bufs_set_string(&bufs, 1, argv[1]);

        lcfg = lustre_cfg_new(LCFG_DEL_MOUNTOPT, &bufs);
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }
        return rc;
}

int jt_lcfg_set_timeout(int argc, char **argv)
{
        int rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        fprintf(stderr, "%s has been deprecated. Use conf_param instead.\n"
                "e.g. conf_param lustre-MDT0000 obd_timeout=50\n",
                jt_cmdname(argv[0]));
        return CMD_HELP;


        if (argc != 2)
                return CMD_HELP;

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);
        lcfg = lustre_cfg_new(LCFG_SET_TIMEOUT, &bufs);
        lcfg->lcfg_num = atoi(argv[1]);
        
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        //rc = lcfg_mgs_ioctl(argv[0], OBD_DEV_ID, lcfg);

        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }
        return rc;
}



int jt_lcfg_add_conn(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        int priority;
        int rc;

        if (argc == 2)
                priority = 0;
        else if (argc == 3)
                priority = 1;
        else
                return CMD_HELP;

        if (lcfg_devname == NULL) {
                fprintf(stderr, "%s: please use 'device name' to set the "
                        "device name for config commands.\n", 
                        jt_cmdname(argv[0])); 
                return -EINVAL;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        lustre_cfg_bufs_set_string(&bufs, 1, argv[1]);

        lcfg = lustre_cfg_new(LCFG_ADD_CONN, &bufs);
        lcfg->lcfg_num = priority;

        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free (lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }

        return rc;
}

int jt_lcfg_del_conn(int argc, char **argv)
{
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;
        int rc;

        if (argc != 2)
                return CMD_HELP;

        if (lcfg_devname == NULL) {
                fprintf(stderr, "%s: please use 'device name' to set the "
                        "device name for config commands.\n", 
                        jt_cmdname(argv[0])); 
                return -EINVAL;
        }

        lustre_cfg_bufs_reset(&bufs, lcfg_devname);

        /* connection uuid */
        lustre_cfg_bufs_set_string(&bufs, 1, argv[1]);

        lcfg = lustre_cfg_new(LCFG_DEL_MOUNTOPT, &bufs);

        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }

        return rc;
}

/* Param set locally, directly on target */
int jt_lcfg_param(int argc, char **argv)
{
        int i, rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        if (argc >= LUSTRE_CFG_MAX_BUFCOUNT)
                return CMD_HELP;

        lustre_cfg_bufs_reset(&bufs, NULL);

        for (i = 1; i < argc; i++) {
                lustre_cfg_bufs_set_string(&bufs, i, argv[i]);
        }

        lcfg = lustre_cfg_new(LCFG_PARAM, &bufs);
        
        rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }
        return rc;
}

/* Param set in config log on MGS */
/* conf_param key1=value1 [key2=value2...] */
int jt_lcfg_mgsparam(int argc, char **argv)
{
        int i, rc;
        struct lustre_cfg_bufs bufs;
        struct lustre_cfg *lcfg;

        if ((argc >= LUSTRE_CFG_MAX_BUFCOUNT) || (argc <= 1))
                return CMD_HELP;

        lustre_cfg_bufs_reset(&bufs, NULL);

        for (i = 1; i < argc; i++) {
                lustre_cfg_bufs_set_string(&bufs, i, argv[i]);
        }

        /* We could put other opcodes here. */
        lcfg = lustre_cfg_new(LCFG_PARAM, &bufs);

        rc = lcfg_mgs_ioctl(argv[0], OBD_DEV_ID, lcfg);
        lustre_cfg_free(lcfg);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }

        return rc;
}

/* Display the path in the same format as sysctl
 * For eg. obdfilter.lustre-OST0000.stats */
static char *display_name(char *filename, int verbose)
{
        char *tmp;
        struct stat st;

        if (verbose) {
                if (stat(filename,  &st) < 0)
                        return NULL;
        }

        filename += strlen("/proc/");
        if (strncmp(filename, "fs/", strlen("fs/")) == 0)
                filename += strlen("fs/");
        else
                filename += strlen("sys/");

        if (strncmp(filename, "lustre/", strlen("lustre/")) == 0)
                filename += strlen("lustre/");

        /* replace '/' with '.' to match conf_param and sysctl */
        tmp = filename;
        while ((tmp = strchr(tmp, '/')) != NULL)
                *tmp = '.';

        /* append the indicator to entries*/
        if (verbose) {
                if (S_ISDIR(st.st_mode))
                        filename[strlen(filename)] = '/';
                else if (S_ISLNK(st.st_mode))
                        filename[strlen(filename)] = '@';
                else if (st.st_mode & S_IWUSR)
                        filename[strlen(filename)] = '=';
        }

        return filename;
}

/* Find a character in a length limited string */
static char *strnchr(const char *p, char c, size_t n)
{
       if (!p)
               return (0);

       while (n-- > 0) {
               if (*p == c)
                       return ((char *)p);
               p++;
       }
       return (0);
}


struct param_opts {
        int only_path;
        int show_path;
        int verbose;
};

static int getparam_cmdline(int argc, char **argv,
                            struct param_opts *popt)
{
        int ch;

        popt->show_path = 1;
        popt->only_path = 0;
        popt->verbose = 0;

        while ((ch = getopt(argc, argv, "nNv")) != -1) {
                switch (ch) {
                case 'N':
                        popt->only_path = 1;
                        break;
                case 'n':
                        popt->show_path = 0;
                case 'v':
                        popt->verbose = 1;
                        break;
                default:
                        return -1;
                }
        }
        return optind;
}

static int getparam_display(struct param_opts *popt, char *pattern)
{
        int rc;
        int fd;
        int i;
        char *buf;
        glob_t glob_info;
        char filename[PATH_MAX + 1];    /* extra 1 byte for file type */

        rc = glob(pattern, GLOB_BRACE, NULL, &glob_info);
        if (rc) {
                fprintf(stderr, "error: get_param: %s: %s\n",
                        pattern, strerror(rc));
                return rc;
        }

        buf = malloc(CFS_PAGE_SIZE);
        for (i = 0; i  < glob_info.gl_pathc; i++) {
                char *valuename = NULL;

                memset(buf, 0, CFS_PAGE_SIZE);
                memset(filename, 0, PATH_MAX + 2);
                if (popt->show_path) {
                        strcpy(filename, glob_info.gl_pathv[i]);
                        if (popt->only_path) {
                                valuename = display_name(filename,
                                                         popt->verbose);
                                if (valuename)
                                        printf("%s\n", valuename);
                                continue;
                        } else {
                                valuename = display_name(filename, 0);
                        }
                }

                /* Write the contents of file to stdout */
                fd = open(glob_info.gl_pathv[i], O_RDONLY);
                if (fd < 0) {
                        fprintf(stderr,
                                "error: get_param: opening('%s') failed: %s\n",
                                glob_info.gl_pathv[i], strerror(errno));
                        continue;
                }

                do {
                        rc = read(fd, buf, CFS_PAGE_SIZE);
                        if (rc == 0)
                                break;
                        if (rc < 0) {
                                fprintf(stderr, "error: get_param: "
                                        "read('%s') failed: %s\n",
                                        glob_info.gl_pathv[i], strerror(errno));
                                break;
                        }
                        /* Print the output in the format path=value if the
                         * value contains no new line character or cab be
                         * occupied in a line, else print value on new line */
                        if (valuename && popt->show_path) {
                                int longbuf = strnchr(buf, rc - 1, '\n') != NULL
                                              || rc > 60;
                                printf("%s=%s", valuename, longbuf ? "\n" : buf);
                                valuename = NULL;
                                if (!longbuf)
                                        continue;
                                fflush(stdout);
                        }
                        rc = write(fileno(stdout), buf, rc);
                        if (rc < 0) {
                                fprintf(stderr, "error: get_param: "
                                        "write to stdout failed: %s\n",
                                        strerror(errno));
                                break;
                        }
                } while (1);
                close(fd);
        }

        globfree(&glob_info);
        free(buf);
        return rc;
}

int jt_lcfg_getparam(int argc, char **argv)
{
        int fp;
        int rc = 0, i;
        struct param_opts popt;
        char pattern[PATH_MAX];
        char *path, *tmp;

        rc = getparam_cmdline(argc, argv, &popt);
        if (rc < 0 || rc >= argc) {
                return CMD_HELP;
        }

        for (i = rc; i < argc; i++) {
                path = argv[i];

                /* If the input is in form Eg. obdfilter.*.stats */
                if (strchr(path, '.')) {
                        tmp = path;
                        while (*tmp != '\0') {
                                if (*tmp == '.')
                                        *tmp = '/';
                                tmp ++;
                        }
                }

                /* If the entire path is specified as input */
                fp = open(path, O_RDONLY);
                if (fp < 0) {
                        snprintf(pattern, PATH_MAX, "/proc/{fs,sys}/{lnet,lustre}/%s",
                                 path);
                } else {
                        strcpy(pattern, path);
                        close(fp);
                }

                rc = getparam_display(&popt, pattern);
                if (rc < 0)
                        return rc;
        }

        return 0;
}

static int setparam_cmdline(int argc, char **argv,
                            struct param_opts *popt)
{
        int ch;

        popt->show_path = 1;
        popt->only_path = 0;
        popt->verbose = 0;

        while ((ch = getopt(argc, argv, "n")) != -1) {
                switch (ch) {
                case 'n':
                        popt->show_path = 0;
                        break;
                default:
                        return -1;
                }
        }
        return optind;
}

static int setparam_display(struct param_opts *popt, char *pattern, char *value)
{
        int rc;
        int fd;
        int i;
        glob_t glob_info;
        char filename[PATH_MAX + 1];    /* extra 1 byte for file type */

        rc = glob(pattern, GLOB_BRACE, NULL, &glob_info);
        if (rc) {
                fprintf(stderr, "error: set_param: %s: %s\n",
                        pattern, strerror(rc));
                return rc;
        }
        for (i = 0; i  < glob_info.gl_pathc; i++) {
                char *valuename = NULL;

                memset(filename, 0, PATH_MAX + 2);
                if (popt->show_path) {
                        strcpy(filename, glob_info.gl_pathv[i]);
                        valuename = display_name(filename, 0);
                        if (valuename)
                                printf("%s=%s\n", valuename, value);
                }
                /* Write the new value to the file */
                fd = open(glob_info.gl_pathv[i], O_WRONLY);
                if (fd > 0) {
                        rc = write(fd, value, strlen(value));
                        if (rc < 0)
                                fprintf(stderr, "error: set_param: "
                                        "writing to file %s: %s\n",
                                        glob_info.gl_pathv[i], strerror(errno));
                        else
                                rc = 0;
                        close(fd);
                } else {
                        fprintf(stderr, "error: set_param: %s opening %s\n",
                                strerror(rc = errno), glob_info.gl_pathv[i]);
                }
        }

        globfree(&glob_info);
        return rc;
}

int jt_lcfg_setparam(int argc, char **argv)
{
        int fp;
        int rc = 0, i;
        struct param_opts popt;
        char pattern[PATH_MAX];
        char *path = NULL, *value = NULL, *tmp;

        rc = setparam_cmdline(argc, argv, &popt);
        if (rc < 0 || rc >= argc) {
                return CMD_HELP;
        }

        for (i = rc; i < argc; i++) {
                if ((value = strchr(argv[i], '=')) != NULL) {
                        /* format: set_param a=b */
                        *value = '\0';
                        value ++;
                        path = argv[i];
                } else {
                        /* format: set_param a b */
                        if (path == NULL) {
                                path = argv[i];
                                continue;
                        } else {
                                value = argv[i];
                        }
                }

                /* If the input is in form Eg. obdfilter.*.stats */
                if (strchr(path, '.')) {
                        tmp = path;
                        while (*tmp != '\0') {
                                if (*tmp == '.')
                                        *tmp = '/';
                                tmp ++;
                        }
                }

                /* If the entire path is specified as input */
                fp = open(path, O_RDONLY);
                if (fp < 0) {
                        snprintf(pattern, PATH_MAX, "/proc/{fs,sys}/{lnet,lustre}/%s",
                                 path);
                } else {
                        strcpy(pattern, path);
                        close(fp);
                }

                rc = setparam_display(&popt, pattern, value);
                path = NULL;
                value = NULL;
                if (rc < 0)
                        return rc;
        }

        return 0;
}
