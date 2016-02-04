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
/*
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2015, Intel Corporation.
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

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#if HAVE_LIBPTHREAD
#include <pthread.h>
#endif

#include <libcfs/util/string.h>
#include <libcfs/util/param.h>
#include <libcfs/util/parser.h>
#include <lnet/nidstr.h>
#include <lustre_cfg.h>
#include <lustre_ioctl.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustre_build_version.h>

#include <sys/un.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

#include "obdctl.h"
#include <lnet/lnetctl.h>
#include <stdio.h>

static char * lcfg_devname;

int lcfg_set_devname(char *name)
{
        char *ptr;
        int digit = 1;

        if (name) {
                if (lcfg_devname)
                        free(lcfg_devname);
                /* quietly strip the unnecessary '$' */
                if (*name == '$' || *name == '%')
                        name++;

                ptr = name;
                while (*ptr != '\0') {
                        if (!isdigit(*ptr)) {
                            digit = 0;
                            break;
                        }
                        ptr++;
                }

                if (digit) {
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
        if (rc < 0) {
                fprintf(stderr, "error: %s: LCFG_ATTACH %s\n",
                        jt_cmdname(argv[0]), strerror(rc = errno));
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
			fprintf(stderr, "unknown option: %s\n", argv[n]);
			return CMD_HELP;
		}
	}

        if (flag_cnt) {
                lustre_cfg_bufs_set_string(&bufs, 1, flags);
        }

        lcfg = lustre_cfg_new(LCFG_CLEANUP, &bufs);
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (uuid != NULL)
		lustre_cfg_bufs_set_string(&bufs, 1, uuid);

        lcfg = lustre_cfg_new(LCFG_ADD_UUID, &bufs);
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		lcfg->lcfg_nid = nid;

		rc = lcfg_ioctl(func, OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
        if (rc) {
                fprintf(stderr, "IOC_PORTAL_ADD_UUID failed: %s\n",
                        strerror(errno));
                return -1;
        }

	if (uuid != NULL)
		printf("Added uuid %s: %s\n", uuid, libcfs_nid2str(nid));

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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		lcfg->lcfg_num = atoi(argv[1]);

		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		lcfg->lcfg_num = priority;

		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
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
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }
        return rc;
}

struct param_opts {
	unsigned int po_only_path:1;
	unsigned int po_show_path:1;
	unsigned int po_show_type:1;
	unsigned int po_recursive:1;
	unsigned int po_params2:1;
	unsigned int po_delete:1;
	unsigned int po_only_dir:1;
	unsigned int po_parallel:1;
};

/* Param set to single log file, used by all clients and servers.
 * This should be loaded after the individual config logs.
 * Called from set param with -P option.
 */
static int jt_lcfg_mgsparam2(int argc, char **argv, struct param_opts *popt)
{
	int	rc, i;
	int	first_param;
	struct	lustre_cfg_bufs bufs;
	struct	lustre_cfg *lcfg;
	char	*buf = NULL;
	int	len;

	first_param = optind;
	if (first_param < 0 || first_param >= argc)
		return CMD_HELP;

	for (i = first_param, rc = 0; i < argc; i++) {
		lustre_cfg_bufs_reset(&bufs, NULL);
		/* This same command would be executed on all nodes, many
		 * of which should fail (silently) because they don't have
		 * that proc file existing locally. There would be no
		 * preprocessing on the MGS to try to figure out which
		 * parameter files to add this to, there would be nodes
		 * processing on the cluster nodes to try to figure out
		 * if they are the intended targets. They will blindly
		 * try to set the parameter, and ENOTFOUND means it wasn't
		 * for them.
		 * Target name "general" means call on all targets. It is
		 * left here in case some filtering will be added in
		 * future.
		 */
		lustre_cfg_bufs_set_string(&bufs, 0, "general");

		len = strlen(argv[i]);

		/* put an '=' on the end in case it doesn't have one */
		if (popt->po_delete && argv[i][len - 1] != '=') {
			buf = malloc(len + 1);
			sprintf(buf, "%s=", argv[i]);
		} else {
			buf = argv[i];
		}
		lustre_cfg_bufs_set_string(&bufs, 1, buf);

		lcfg = lustre_cfg_new(LCFG_SET_PARAM, &bufs);
		if (lcfg == NULL) {
			fprintf(stderr, "error: allocating lcfg for %s: %s\n",
				jt_cmdname(argv[0]), strerror(-ENOMEM));
			if (rc == 0)
				rc = -ENOMEM;
		} else {
			int rc2 = lcfg_mgs_ioctl(argv[0], OBD_DEV_ID, lcfg);
			if (rc2 != 0) {
				fprintf(stderr, "error: executing %s: %s\n",
					jt_cmdname(argv[0]), strerror(errno));
				if (rc == 0)
					rc = rc2;
			}
			lustre_cfg_free(lcfg);
		}
		if (buf != argv[i])
			free(buf);
	}

	return rc;
}

/* Param set in config log on MGS */
/* conf_param key=value */
/* Note we can actually send mgc conf_params from clients, but currently
 * that's only done for default file striping (see ll_send_mgc_param),
 * and not here. */
/* After removal of a parameter (-d) Lustre will use the default
 * AT NEXT REBOOT, not immediately. */
int jt_lcfg_mgsparam(int argc, char **argv)
{
	int rc;
	int del = 0;
	struct lustre_cfg_bufs bufs;
	struct lustre_cfg *lcfg;
	char *buf = NULL;

#if LUSTRE_VERSION_CODE >= OBD_OCD_VERSION(2, 8, 53, 0)
	fprintf(stderr, "warning: 'lctl conf_param' is deprecated, "
		"use 'lctl set_param -P' instead\n");
#endif

        /* mgs_setparam processes only lctl buf #1 */
        if ((argc > 3) || (argc <= 1))
                return CMD_HELP;

        while ((rc = getopt(argc, argv, "d")) != -1) {
                switch (rc) {
                        case 'd':
                                del = 1;
                                break;
                        default:
                                return CMD_HELP;
                }
        }

        lustre_cfg_bufs_reset(&bufs, NULL);
        if (del) {
                char *ptr;

                /* for delete, make it "<param>=\0" */
                buf = malloc(strlen(argv[optind]) + 2);
                /* put an '=' on the end in case it doesn't have one */
                sprintf(buf, "%s=", argv[optind]);
                /* then truncate after the first '=' */
                ptr = strchr(buf, '=');
                *(++ptr) = '\0';
                lustre_cfg_bufs_set_string(&bufs, 1, buf);
        } else {
                lustre_cfg_bufs_set_string(&bufs, 1, argv[optind]);
        }

        /* We could put other opcodes here. */
        lcfg = lustre_cfg_new(LCFG_PARAM, &bufs);
	if (lcfg == NULL) {
		rc = -ENOMEM;
	} else {
		rc = lcfg_mgs_ioctl(argv[0], OBD_DEV_ID, lcfg);
		lustre_cfg_free(lcfg);
	}
        if (buf)
                free(buf);
        if (rc < 0) {
                fprintf(stderr, "error: %s: %s\n", jt_cmdname(argv[0]),
                        strerror(rc = errno));
        }

        return rc;
}

/**
 * Display a parameter path in the same format as sysctl.
 * E.g. obdfilter.lustre-OST0000.stats
 *
 * \param[in] filename	file name of the parameter
 * \param[in] st	parameter file stats
 * \param[in] popt	set/get param options
 *
 * \retval allocated pointer containing modified filename
 */
static char *
display_name(const char *filename, struct stat *st, struct param_opts *popt)
{
	size_t suffix_len = 0;
	char *suffix = NULL;
	char *param_name;
	char *tmp;

	if (popt->po_show_type) {
		if (S_ISDIR(st->st_mode))
			suffix = "/";
		else if (S_ISLNK(st->st_mode))
			suffix = "@";
		else if (st->st_mode & S_IWUSR)
			suffix = "=";

		if (suffix != NULL)
			suffix_len = strlen(suffix);
	} else if (popt->po_only_dir) {
		if (!S_ISDIR(st->st_mode))
			return NULL;
	}

	/* Take the original filename string and chop off the glob addition */
	tmp = strstr(filename, "/lustre/");
	if (tmp == NULL) {
		tmp = strstr(filename, "/lnet/");
		if (tmp != NULL)
			tmp += strlen("/lnet/");
	} else {
		tmp += strlen("/lustre/");
	}

	/* Allocate return string */
	param_name = strdup(tmp);
	if (param_name == NULL)
		return NULL;

	/* replace '/' with '.' to match conf_param and sysctl */
	for (tmp = strchr(param_name, '/'); tmp != NULL; tmp = strchr(tmp, '/'))
		*tmp = '.';

	/* Append the indicator to entries if needed. */
	if (popt->po_show_type && suffix != NULL) {
		param_name = realloc(param_name,
				     suffix_len + strlen(param_name) + 1);
		if (param_name != NULL)
			strncat(param_name, suffix, suffix_len);
	}

	return param_name;
}

/* Find a character in a length limited string */
/* BEWARE - kernel definition of strnchr has args in different order! */
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

/**
 * Turns a lctl parameter string into a procfs/sysfs subdirectory path pattern.
 *
 * \param[in] popt		Used to control parameter usage. For this
 *				function it is used to see if the path has
 *				a added suffix.
 * \param[in,out] path		lctl parameter string that is turned into
 *				the subdirectory path pattern that is used
 *				to search the procfs/sysfs tree.
 *
 * \retval -errno on error.
 */
static int
clean_path(struct param_opts *popt, char *path)
{
	char *nidstr = NULL;
	char *tmp;

	if (popt == NULL || path == NULL || strlen(path) == 0)
		return -EINVAL;

	/* If path contains a suffix we need to remove it */
	if (popt->po_show_type) {
		size_t path_end = strlen(path) - 1;

		tmp = path + path_end;
		switch (*tmp) {
		case '@':
		case '=':
		case '/':
			*tmp = '\0';
		default:
			break;
		}
	}

	/* get rid of '\', glob doesn't like it */
	tmp = strrchr(path, '\\');
	if (tmp != NULL) {
		char *tail = path + strlen(path);

		while (tmp != path) {
			if (*tmp == '\\') {
				memmove(tmp, tmp + 1, tail - tmp);
				--tail;
			}
			--tmp;
		}
	}

	/* Does this path contain a NID string ? */
	tmp = strchr(path, '@');
	if (tmp != NULL) {
		char *find_nid = strdup(path);
		lnet_nid_t nid;

		if (find_nid == NULL)
			return -ENOMEM;

		/* First we need to chop off rest after nid string.
		 * Since find_nid is a clone of path it better have
		 * '@' */
		tmp = strchr(find_nid, '@');
		tmp = strchr(tmp, '.');
		if (tmp != NULL)
			*tmp = '\0';

		/* Now chop off the front. */
		for (tmp = strchr(find_nid, '.'); tmp != NULL;
		     tmp = strchr(tmp, '.')) {
			/* Remove MGC to make it NID format */
			if (!strncmp(++tmp, "MGC", 3))
				tmp += 3;

			nid = libcfs_str2nid(tmp);
			if (nid != LNET_NID_ANY) {
				nidstr = libcfs_nid2str(nid);
				if (nidstr == NULL)
					return -EINVAL;
				break;
			}
		}
		free(find_nid);
	}

	/* replace param '.' with '/' */
	for (tmp = strchr(path, '.'); tmp != NULL; tmp = strchr(tmp, '.')) {
		*tmp++ = '/';

		/* Remove MGC to make it NID format */
		if (!strncmp(tmp, "MGC", 3))
			tmp += 3;

		/* There exist cases where some of the subdirectories of the
		 * the parameter tree has embedded in its name a NID string.
		 * This means that it is possible that these subdirectories
		 * could have actual '.' in its name. If this is the case we
		 * don't want to blindly replace the '.' with '/'. */
		if (nidstr != NULL) {
			char *match = strstr(tmp, nidstr);

			if (tmp == match)
				tmp += strlen(nidstr);
		}
	}

	return 0;
}

/**
 * The application lctl can perform three operations for lustre
 * tunables. This enum defines those three operations which are
 *
 * 1) LIST_PARAM	- list available tunables
 * 2) GET_PARAM		- report the current setting of a tunable
 * 3) SET_PARAM		- set the tunable to a new value
 */
enum parameter_operation {
	LIST_PARAM,
	GET_PARAM,
	SET_PARAM,
};

/**
 * Read the value of parameter
 *
 * \param[in]	path		full path to the parameter
 * \param[in]	param_name	lctl parameter format of the
 *				parameter path
 * \param[in]	popt		set/get param options
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
static int
read_param(const char *path, const char *param_name, struct param_opts *popt)
{
	bool display_path = popt->po_show_path;
	long page_size = sysconf(_SC_PAGESIZE);
	int rc = 0;
	char *buf;
	int fd;

	/* Read the contents of file to stdout */
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		rc = -errno;
		fprintf(stderr,
			"error: get_param: opening('%s') failed: %s\n",
			path, strerror(errno));
		return rc;
	}

	buf = calloc(1, page_size);
	if (buf == NULL) {
		close(fd);
		return -ENOMEM;
	}

	while (1) {
		ssize_t count = read(fd, buf, page_size);

		if (count == 0)
			break;
		if (count < 0) {
			rc = -errno;
			if (errno != EIO) {
				fprintf(stderr, "error: get_param: "
					"read('%s') failed: %s\n",
					param_name, strerror(errno));
			}
			break;
		}

		/* Print the output in the format path=value if the value does
		 * not contain a new line character and the output can fit in
		 * a single line, else print value on new line */
		if (display_path) {
			bool longbuf;

			longbuf = strnchr(buf, count - 1, '\n') != NULL ||
					  count + strlen(param_name) >= 80;
			printf("%s=%s", param_name, longbuf ? "\n" : buf);

			/* Make sure it doesn't print again while looping */
			display_path = false;

			if (!longbuf)
				continue;
		}

		if (fwrite(buf, 1, count, stdout) != count) {
			rc = -errno;
			fprintf(stderr, "error: get_param: "
				"write to stdout failed: %s\n",
				strerror(errno));
			break;
		}
	}
	close(fd);
	free(buf);

	return rc;
}

/**
 * Set a parameter to a specified value
 *
 * \param[in] path		full path to the parameter
 * \param[in] param_name	lctl parameter format of the parameter path
 * \param[in] popt		set/get param options
 * \param[in] value		value to set the parameter to
 *
 * \retval number of bytes written on success.
 * \retval -errno on error.
 */
static int
write_param(const char *path, const char *param_name, struct param_opts *popt,
	    const char *value)
{
	int fd, rc = 0;
	ssize_t count;

	if (value == NULL)
		return -EINVAL;

	/* Write the new value to the file */
	fd = open(path, O_WRONLY);
	if (fd < 0) {
		rc = -errno;
		fprintf(stderr, "error: set_param: opening %s: %s\n",
			path, strerror(errno));
		return rc;
	}

	count = write(fd, value, strlen(value));
	if (count < 0) {
		rc = -errno;
		if (errno != EIO) {
			fprintf(stderr, "error: set_param: setting "
				"%s=%s: %s\n", path, value,
				strerror(errno));
		}
	} else if (count < strlen(value)) { /* Truncate case */
		rc = -EINVAL;
		fprintf(stderr, "error: set_param: setting "
			"%s=%s: wrote only %zd\n", path, value, count);
	} else if (popt->po_show_path) {
		printf("%s=%s\n", param_name, value);
	}
	close(fd);
	return rc;
}

#if HAVE_LIBPTHREAD
#define LCTL_THREADS_PER_CPU 8

/* A work item for parallel set_param */
struct sp_work_item {
	/* The full path to the parameter file */
	char *spwi_path;

	/* The parameter name as returned by display_name */
	char *spwi_param_name;

	/* The value to which the parameter is to be set */
	char *spwi_value;
};

/* A work queue struct for parallel set_param */
struct sp_workq {
	/* The parameter options passed to set_param */
	struct param_opts *spwq_popt;

	/* The number of valid items in spwq_items */
	int spwq_len;

	/* The size of the spwq_items list */
	int spwq_size;

	/* The current index into the spwq_items list */
	int spwq_cur_index;

	/* Array of work items. */
	struct sp_work_item *spwq_items;

	/* A mutex to control access to the work queue */
	pthread_mutex_t spwq_mutex;
};

/**
 * Initialize the given set_param work queue.
 *
 * \param[out] wq  the work queue to initialize
 * \param[in] popt the options passed to set_param
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static int spwq_init(struct sp_workq *wq, struct param_opts *popt)
{
	wq->spwq_popt = popt;
	wq->spwq_len = 0;
	wq->spwq_size = 0;
	wq->spwq_cur_index = 0;
	wq->spwq_items = NULL;

	/* pthread_mutex_init returns 0 for success, or errno for failure */
	return -pthread_mutex_init(&wq->spwq_mutex, NULL);
}

/**
 * Destroy and free space used by a set_param work queue.
 *
 * \param[in] wq the work queue to destroy
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static int spwq_destroy(struct sp_workq *wq)
{
	if (wq == NULL)
		return 0;

	if (wq->spwq_items != NULL) {
		int i;
		for (i = 0; i < wq->spwq_len; i++) {
			free(wq->spwq_items[i].spwi_path);
			free(wq->spwq_items[i].spwi_param_name);
			/* wq->spwq_items[i].spwi_value was not malloc'd */
		}
		free(wq->spwq_items);
		wq->spwq_items = NULL;
	}

	wq->spwq_len = 0;
	wq->spwq_size = 0;
	wq->spwq_cur_index = 0;
	/* spwq_popt was not malloc'd either */
	wq->spwq_popt = NULL;

	/* pthread_mutex_destroy returns 0 for success, or errno for failure */
	return -pthread_mutex_destroy(&wq->spwq_mutex);
}

/**
 * Expand the size of a work queue to fit the requested number of items.
 *
 * \param[in,out] wq    the work queue to expand
 * \param[in] num_items the number of items to make room for in \a wq
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static int spwq_expand(struct sp_workq *wq, size_t num_items)
{
	int space;
	int new_size;
	struct sp_work_item *tmp;

	if (wq == NULL)
		return -EINVAL;

	space = wq->spwq_size - wq->spwq_len;

	/* First check if there's already enough room. */
	if (space >= num_items)
		return 0;

	new_size = wq->spwq_len + num_items;

	/* When spwq_items is NULL, realloc behaves like malloc */
	tmp = realloc(wq->spwq_items, new_size * sizeof(struct sp_work_item));

	if (tmp == NULL)
		return -ENOMEM;

	wq->spwq_items = tmp;
	wq->spwq_size = new_size;
	return 0;
}

/**
 * Add an item to a set_param work queue. Not thread-safe.
 *
 * \param[in,out] wq     the work queue to which the item should be added
 * \param[in] path       the full path to the parameter file (will be copied)
 * \param[in] param_name the name of the parameter (will be copied)
 * \param[in] value      the value for the parameter (will not be copied)
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static int spwq_add_item(struct sp_workq *wq, char *path,
			 char *param_name, char *value)
{
	int rc;
	char *path_copy;
	char *param_name_copy;

	if (wq == NULL || path == NULL || param_name == NULL || value == NULL)
		return -EINVAL;

	/* Hopefully the caller has expanded the work queue before calling this
	 * function, but make sure there's room just in case. */
	rc = spwq_expand(wq, 1);

	if (rc < 0)
		return rc;

	path_copy = strdup(path);
	if (path_copy == NULL)
		return -ENOMEM;

	param_name_copy = strdup(param_name);
	if (param_name_copy == NULL) {
		free(path_copy);
		return -ENOMEM;
	}

	wq->spwq_items[wq->spwq_len].spwi_param_name = param_name_copy;
	wq->spwq_items[wq->spwq_len].spwi_path = path_copy;
	wq->spwq_items[wq->spwq_len].spwi_value = value;

	wq->spwq_len++;

	return 0;
}

/**
 * Gets the next item from the set_param \a wq in a thread-safe manner.
 *
 * \param[in] wq  the workq from which to obtain the next item
 * \param[out] wi the next work item in \a wa, will be set to NULL if \wq empty
 *
 * \retval 0 if successful (empty work queue is considered successful)
 * \retval -errno if unsuccessful
 */
static int spwq_next_item(struct sp_workq *wq, struct sp_work_item **wi)
{
	int rc_lock;
	int rc_unlock;

	if (wq == NULL || wi == NULL)
		return -EINVAL;

	*wi = NULL;

	rc_lock = pthread_mutex_lock(&wq->spwq_mutex);
	if (rc_lock == 0 && wq->spwq_cur_index < wq->spwq_len)
		*wi = &wq->spwq_items[wq->spwq_cur_index++];

	rc_unlock = pthread_mutex_unlock(&wq->spwq_mutex);
	/* Ignore failures due to not owning the mutex */
	if (rc_unlock == EPERM)
		rc_unlock = 0;

	return rc_lock != 0 ? -rc_lock : -rc_unlock;
}

/**
 * A set_param worker thread which sets params from the workq.
 *
 * \param[in] arg a pointer to a struct sp_workq
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static void *sp_thread(void *arg)
{
	long int rc = 0;
	int rc2 = 0;
	struct sp_workq *wq = (struct sp_workq *)arg;
	struct param_opts *popt = wq->spwq_popt;
	struct sp_work_item *witem;

	rc = spwq_next_item(wq, &witem);
	if (rc < 0)
		return (void *)rc;

	while (witem != NULL) {
		char *path = witem->spwi_path;
		char *param_name = witem->spwi_param_name;
		char *value = witem->spwi_value;
		rc2 = write_param(path, param_name, popt, value);
		if (rc2 < 0)
			rc = rc2;
		rc2 = spwq_next_item(wq, &witem);
		if (rc2 < 0)
			rc = rc2;
	}

	return (void *)rc;
}

/**
 * Spawn threads and set parameters in a work queue in parallel.
 *
 * \param[in] wq the work queue containing parameters to set
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
static int sp_run_threads(struct sp_workq *wq)
{
	int rc = 0;
	int join_rc;
	void *res;
	int i;
	int j;
	int num_threads;
	pthread_t *sp_threads;

	if (wq == NULL)
		return -EINVAL;

	if (wq->spwq_len == 0)
		return 0;

	num_threads = LCTL_THREADS_PER_CPU * sysconf(_SC_NPROCESSORS_ONLN);
	if (num_threads > wq->spwq_len)
		num_threads = wq->spwq_len;

	sp_threads = malloc(sizeof(pthread_t) * num_threads);
	if (sp_threads == NULL)
		return -ENOMEM;

	for (i = 0; i < num_threads; i++) {
		rc = pthread_create(&sp_threads[i], NULL,
				    &sp_thread, (void *)wq);
		if (rc != 0) {
			fprintf(stderr, "error: set_param: spawning "
					"thread %d/%d failed: rc=%d.\n",
				i + 1, num_threads, rc);
			break;
		}
	}

	for (j = 0; j < i; j++) {
		join_rc = pthread_join(sp_threads[j], &res);
		if (join_rc != 0) {
			fprintf(stderr, "error: set_param: joining "
					"thread %d/%d failed: rc=%d.\n",
				j + 1, i, join_rc);
			/* thread creation errors take priority */
			if (rc == 0)
				rc = join_rc;
		}
		if (res != 0) {
			/* this error takes priority over create/join errors */
			rc = (long int)res;
			fprintf(stderr, "error: set_param: "
					"thread %d/%d failed: rc=%d.\n",
				j + 1, i, rc);
		}
	}

	free(sp_threads);
	return rc;
}

#endif /* HAVE_LIBPTHREAD */

/**
 * Perform a read, write or just a listing of a parameter
 *
 * \param[in] popt		list,set,get parameter options
 * \param[in] pattern		search filter for the path of the parameter
 * \param[in] value		value to set the parameter if write operation
 * \param[in] oper		what operation to perform with the parameter
 *
 * \retval number of bytes written on success.
 * \retval -errno on error.
 */
#if HAVE_LIBPTHREAD
#define do_param_op(popt, pattern, value, oper, wq) \
	_do_param_op(popt, pattern, value, oper, wq)
static int
_do_param_op(struct param_opts *popt, char *pattern, char *value,
	     enum parameter_operation oper, struct sp_workq *wq)
#else
#define do_param_op(popt, pattern, value, oper, wq) \
	_do_param_op(popt, pattern, value, oper)
static int
_do_param_op(struct param_opts *popt, char *pattern, char *value,
	     enum parameter_operation oper)
#endif
{
	int dir_count = 0;
	char **dir_cache;
	glob_t paths;
	int rc, i;

#ifdef HAVE_LIBPTHREAD
	if (wq == NULL && popt->po_parallel)
		return -EINVAL;
#endif

	rc = cfs_get_param_paths(&paths, "%s", pattern);
	if (rc != 0) {
		rc = -errno;
		if (!popt->po_recursive) {
			fprintf(stderr, "error: '%s': %s\n",
				pattern, strerror(errno));
		}
		return rc;
	}

#if HAVE_LIBPTHREAD
	if (popt->po_parallel) {
		/* Allocate space for the glob paths in advance. */
		rc = spwq_expand(wq, paths.gl_pathc);
		if (rc < 0)
			goto out_param;
	}
#endif

	dir_cache = calloc(paths.gl_pathc, sizeof(char *));
	if (dir_cache == NULL) {
		rc = -ENOMEM;
		goto out_param;
	}

	for (i = 0; i < paths.gl_pathc; i++) {
		char *param_name = NULL, *tmp;
		char pathname[PATH_MAX];
		struct stat st;

		if (stat(paths.gl_pathv[i], &st) == -1) {
			rc = -errno;
			break;
		}

		param_name = display_name(paths.gl_pathv[i], &st, popt);
		if (param_name == NULL) {
			rc = -ENOMEM;
			break;
		}

		/**
		 * For the upstream client the parameter files locations
		 * are split between under both /sys/kernel/debug/lustre
		 * and /sys/fs/lustre. The parameter files containing
		 * small amounts of data, less than a page in size, are
		 * located under /sys/fs/lustre and in the case of large
		 * parameter data files, think stats for example, are
		 * located in the debugfs tree. Since the files are split
		 * across two trees the directories are often duplicated
		 * which means these directories are listed twice which
		 * leads to duplicate output to the user. To avoid scanning
		 * a directory twice we have to cache any directory and
		 * check if a search has been requested twice.
		 */
		if (S_ISDIR(st.st_mode)) {
			int j;

			for (j = 0; j < dir_count; j++) {
				if (!strcmp(dir_cache[j], param_name))
					break;
			}
			if (j != dir_count) {
				free(param_name);
				param_name = NULL;
				continue;
			}
			dir_cache[dir_count++] = strdup(param_name);
		}

		switch (oper) {
		case GET_PARAM:
			/* Read the contents of file to stdout */
			if (S_ISREG(st.st_mode))
				read_param(paths.gl_pathv[i], param_name, popt);
			break;
		case SET_PARAM:
			if (S_ISREG(st.st_mode) && !popt->po_parallel)
				rc = write_param(paths.gl_pathv[i], param_name,
						 popt, value);
#if HAVE_LIBPTHREAD
			else if (S_ISREG(st.st_mode) && popt->po_parallel)
				rc = spwq_add_item(wq, paths.gl_pathv[i],
						   param_name, value);
#endif
			break;
		case LIST_PARAM:
		default:
			if (popt->po_show_path)
				printf("%s\n", param_name);
			break;
		}

		/* Only directories are searched recursively if
		 * requested by the user */
		if (!S_ISDIR(st.st_mode) || !popt->po_recursive) {
			free(param_name);
			param_name = NULL;
			continue;
		}

		/* Turn param_name into file path format */
		rc = clean_path(popt, param_name);
		if (rc < 0) {
			free(param_name);
			param_name = NULL;
			break;
		}

		/* Use param_name to grab subdirectory tree from full path */
		tmp = strstr(paths.gl_pathv[i], param_name);

		/* cleanup param_name now that we are done with it */
		free(param_name);
		param_name = NULL;

		/* Shouldn't happen but just in case */
		if (tmp == NULL) {
			rc = -EINVAL;
			break;
		}

		rc = snprintf(pathname, sizeof(pathname), "%s/*", tmp);
		if (rc < 0) {
			break;
		} else if (rc >= sizeof(pathname)) {
			rc = -EINVAL;
			break;
		}

		/* The C preprocessor will replace this with
		 * _do_param_op(popt, pathname, value, oper) if we don't
		 * HAVE_LIBPTHREAD, so it's okay to use wq. */
		rc = do_param_op(popt, pathname, value, oper, wq);

		if (rc != 0 && rc != -ENOENT)
			break;
	}

	for (i = 0; i < dir_count; i++)
		free(dir_cache[i]);
	free(dir_cache);
out_param:
	cfs_free_param_data(&paths);
	return rc;
}

static int listparam_cmdline(int argc, char **argv, struct param_opts *popt)
{
	int ch;

	popt->po_show_path = 1;
	popt->po_only_path = 1;

	while ((ch = getopt(argc, argv, "FRD")) != -1) {
		switch (ch) {
		case 'F':
			popt->po_show_type = 1;
			break;
		case 'R':
			popt->po_recursive = 1;
			break;
		case 'D':
			popt->po_only_dir = 1;
			break;
		default:
			return -1;
		}
	}

	return optind;
}

int jt_lcfg_listparam(int argc, char **argv)
{
	int rc = 0, index, i;
	struct param_opts popt;
	char *path;

	memset(&popt, 0, sizeof(popt));
	index = listparam_cmdline(argc, argv, &popt);
	if (index < 0 || index >= argc)
		return CMD_HELP;

	for (i = index; i < argc; i++) {
		path = argv[i];

		rc = clean_path(&popt, path);
		if (rc < 0)
			break;

		rc = do_param_op(&popt, path, NULL, LIST_PARAM, NULL);
		if (rc < 0)
			break;
	}

	return rc;
}

static int getparam_cmdline(int argc, char **argv, struct param_opts *popt)
{
	int ch;

	popt->po_show_path = 1;

	while ((ch = getopt(argc, argv, "FnNR")) != -1) {
		switch (ch) {
		case 'F':
			popt->po_show_type = 1;
			break;
		case 'n':
			popt->po_show_path = 0;
			break;
		case 'N':
			popt->po_only_path = 1;
			break;
		case 'R':
			popt->po_recursive = 1;
			break;
		default:
			return -1;
		}
	}

	return optind;
}

int jt_lcfg_getparam(int argc, char **argv)
{
	int rc = 0, index, i;
	struct param_opts popt;
	char *path;

	memset(&popt, 0, sizeof(popt));
	index = getparam_cmdline(argc, argv, &popt);
	if (index < 0 || index >= argc)
		return CMD_HELP;

	for (i = index; i < argc; i++) {
		path = argv[i];

		rc = clean_path(&popt, path);
		if (rc < 0)
			break;

		rc = do_param_op(&popt, path, NULL,
				 popt.po_only_path ? LIST_PARAM : GET_PARAM,
				 NULL);
		if (rc < 0)
			break;
	}

	return rc;
}

/**
 * Output information about nodemaps.
 * \param	argc		number of args
 * \param	argv[]		variable string arguments
 *
 * [list|nodemap_name|all]	\a list will list all nodemaps (default).
 *				Specifying a \a nodemap_name will
 *				display info about that specific nodemap.
 *				\a all will display info for all nodemaps.
 * \retval			0 on success
 */
int jt_nodemap_info(int argc, char **argv)
{
	const char		usage_str[] = "usage: nodemap_info "
					      "[list|nodemap_name|all]\n";
	struct param_opts	popt;
	int			rc = 0;

	memset(&popt, 0, sizeof(popt));
	popt.po_show_path = 1;

	if (argc > 2) {
		fprintf(stderr, usage_str);
		return -1;
	}

	if (argc == 1 || strcmp("list", argv[1]) == 0) {
		popt.po_only_path = 1;
		popt.po_only_dir = 1;
		rc = do_param_op(&popt, "nodemap/*", NULL, LIST_PARAM, NULL);
	} else if (strcmp("all", argv[1]) == 0) {
		rc = do_param_op(&popt, "nodemap/*/*", NULL, LIST_PARAM, NULL);
	} else {
		char	pattern[PATH_MAX];

		snprintf(pattern, sizeof(pattern), "nodemap/%s/*", argv[1]);
		rc = do_param_op(&popt, pattern, NULL, LIST_PARAM, NULL);
		if (rc == -ESRCH)
			fprintf(stderr, "error: nodemap_info: cannot find"
					"nodemap %s\n", argv[1]);
	}
	return rc;
}

/**
 * Parses the command-line options to set_param.
 *
 * \param[in] argc   count of arguments given to set_param
 * \param[in] argv   array of arguments given to set_param
 * \param[out] popt  where set_param options will be saved
 *
 * \retval index in argv of the first non-option argv element (optind value)
 */
static int setparam_cmdline(int argc, char **argv, struct param_opts *popt)
{
        int ch;

        popt->po_show_path = 1;
        popt->po_only_path = 0;
        popt->po_show_type = 0;
        popt->po_recursive = 0;
	popt->po_params2 = 0;
	popt->po_delete = 0;
	popt->po_parallel = 0;

	while ((ch = getopt(argc, argv, "nPdp")) != -1) {
                switch (ch) {
                case 'n':
                        popt->po_show_path = 0;
                        break;
		case 'P':
			popt->po_params2 = 1;
			break;
		case 'd':
			popt->po_delete = 1;
			break;
		case 'p':
#if HAVE_LIBPTHREAD
			popt->po_parallel = 1;
#else
			fprintf(stderr, "warning: set_param: no pthread "
					"support, proceeding serially.\n");
#endif
			break;
                default:
                        return -1;
                }
        }
        return optind;
}

/**
 * Parse the arguments to set_param and return the first parameter and value
 * pair and the number of arguments consumed.
 *
 * \param[in] argc   number of arguments remaining in argv
 * \param[in] argv   list of param-value arguments to set_param (this function
 *                   will modify the strings by overwriting '=' with '\0')
 * \param[out] param the parameter name
 * \param[out] value the parameter value
 *
 * \retval the number of args consumed from argv (1 for "param=value" format, 2
 *         for "param value" format)
 * \retval -errno if unsuccessful
 */
static int sp_parse_param_value(int argc, char **argv,
				char **param, char **value) {
	char *tmp;

	if (argc < 1 || argv == NULL || param == NULL || value == NULL)
		return -EINVAL;

	*param = argv[0];
	tmp = strchr(*param, '=');
	if (tmp != NULL) {
		/* format: set_param a=b */
		*tmp = '\0';
		tmp++;
		if (*tmp == '\0')
			return -EINVAL;
		*value = tmp;
		return 1;
	}
	/* format: set_param a b */
	if (argc < 2)
		return -EINVAL;
	*value = argv[1];

	return 2;
}

/**
 * Main set_param function.
 *
 * \param[in] argc  count of arguments given to set_param
 * \param[in] argv  array of arguments given to set_param
 *
 * \retval 0 if successful
 * \retval -errno if unsuccessful
 */
int jt_lcfg_setparam(int argc, char **argv)
{
	int rc = 0;
	int index;
	struct param_opts popt;
#if HAVE_LIBPTHREAD
	struct sp_workq wq;
	int rc2 = 0;
#endif

	memset(&popt, 0, sizeof(popt));
	index = setparam_cmdline(argc, argv, &popt);
	if (index < 0 || index >= argc)
		return CMD_HELP;

	if (popt.po_params2)
		/* We can't delete parameters that were
		 * set with old conf_param interface */
		return jt_lcfg_mgsparam2(argc, argv, &popt);

#if HAVE_LIBPTHREAD
	if (popt.po_parallel) {
		rc = spwq_init(&wq, &popt);
		if (rc < 0) {
			fprintf(stderr, "error: %s: "
					"failed to init work queue: %s",
				jt_cmdname(argv[0]), strerror(-rc));
			return rc;
		}
	}
#endif

	while (index < argc) {
		char *path = NULL;
		char *value = NULL;

		rc = sp_parse_param_value(argc - index, argv + index,
					  &path, &value);
		if (rc < 0) {
			fprintf(stderr, "error: %s: setting %s=: %s\n",
				jt_cmdname(argv[0]), path, strerror(-rc));
			break;
		}
		/* Increment index by the number or arguments consumed. */
		index += rc;

		rc = clean_path(&popt, path);
		if (rc < 0)
			break;

#if HAVE_LIBPTHREAD
		rc = do_param_op(&popt, path, value, SET_PARAM,
				 popt.po_parallel ? &wq : NULL);
#else
		rc = do_param_op(&popt, path, value, SET_PARAM, NULL);
#endif

		if (rc < 0) {
			fprintf(stderr, "error: %s: setting '%s'='%s': %s\n",
				jt_cmdname(argv[0]), path, value,
				strerror(-rc));
		}
	}

#if HAVE_LIBPTHREAD
	if (popt.po_parallel) {
		/* Spawn threads to set the parameters which made it into the
		 * work queue to emulate serial set_param behavior when errors
		 * are encountered above. */
		rc2 = sp_run_threads(&wq);
		if (rc2 < 0) {
			fprintf(stderr, "error: %s: failed to run threads: %s",
				jt_cmdname(argv[0]), strerror(-rc2));
			if (rc == 0)
				rc = rc2;
		}
		rc2 = spwq_destroy(&wq);
		if (rc2 < 0) {
			fprintf(stderr, "error: %s: failed workq cleanup: %s",
				jt_cmdname(argv[0]), strerror(-rc2));
			if (rc == 0)
				rc = rc2;
		}
	}
#endif

	return rc;
}
