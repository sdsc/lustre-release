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

#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>

#include <libcfs/util/string.h>
#include <libcfs/util/param.h>
#include <libcfs/util/parser.h>
#include <lnet/nidstr.h>
#include <lustre_cfg.h>
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
	char *suffix = NULL;
	char *paramname;
	size_t len = 0;
	char *tmp;

	if (popt->po_show_type) {
		if (S_ISDIR(st->st_mode))
			suffix = "/";
		else if (S_ISLNK(st->st_mode))
			suffix = "@";
		else if (st->st_mode & S_IWUSR)
			suffix = "=";

		if (suffix)
			len = strlen(suffix);
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
	} else
		tmp += strlen("/lustre/");

	/* Allocate return string */
	paramname = strdup(tmp);

	/* replace '/' with '.' to match conf_param and sysctl */
	for (tmp = strchr(paramname, '/'); tmp != NULL; tmp = strchr(tmp, '/'))
		*tmp = '.';

	/* Append the indicator to entries if needed. */
	if (popt->po_show_type && suffix != NULL) {
		tmp = paramname;
		paramname = realloc(tmp, strlen(tmp) + len + 1);
		if (paramname)
			strncat(paramname, suffix, len);
	}

	return paramname;
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

static void clean_path(char *path)
{
        char *tmp;

        /* If the input is in form Eg. obdfilter.*.stats */
        if (strchr(path, '.')) {
                tmp = path;
                while (*tmp != '\0') {
                        if ((*tmp == '.') &&
                            (tmp != path) && (*(tmp - 1) != '\\'))
                                *tmp = '/';
                        tmp ++;
                }
        }
        /* get rid of '\', glob doesn't like it */
        if ((tmp = strrchr(path, '\\')) != NULL) {
                char *tail = path + strlen(path);
                while (tmp != path) {
                        if (*tmp == '\\') {
                                memmove(tmp, tmp + 1, tail - tmp);
                                --tail;
                        }
                        --tmp;
                }
        }
}

/**
 * Read the value of parameter
 *
 * \param[in]		path		full path to the parameter
 * \param[in|out]	paramname	lctl parameter format of the
 *					parameter path
 * \param[in]		popt		set/get param options
 *
 * \retval 0 on success.
 * \retval -errno on error.
 */
static int
read_param(const char *path, char **paramname, struct param_opts *popt)
{
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
					*paramname, strerror(errno));
			}
			break;
		}
		/* Print the output in the format path=value if the
		 * value contains a new line character or can fit
		 * in a line, else print value on new line */
		if (*paramname && popt->po_show_path) {
			bool longbuf;

			longbuf = strnchr(buf, count - 1, '\n') != NULL ||
					  count + strlen(*paramname) >= 80;
			printf("%s=%s", *paramname, longbuf ? "\n" : buf);

			/* Make sure it doesn't print again while looping */
			free(*paramname);
			*paramname = NULL;

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
 * \param[in] paramname		lctl parameter format of the parameter path
 * \param[in] popt		set/get param options
 * \param[in] value		value to set the parameter to
 *
 * \retval number of bytes written on success.
 * \retval -errno on error.
 */
static int
write_param(const char *path, const char *paramname, struct param_opts *popt,
	    const char *value)
{
	int fd, rc = 0;
	ssize_t count;

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
			"%s=%s: wrote only %d\n", path, value, rc);
	} else if (paramname) {
		printf("%s=%s\n", paramname, value);
	}
	close(fd);
	return rc;
}

/**
 * Perform a read, write or just a listing of a parameter
 *
 * \param[in] path		full path to the parameter
 * \param[in] paramname		lctl parameter format of the parameter path
 * \param[in] popt		set/get param options
 * \param[in] value		value to set the parameter if write operation
 * \param[in] mode		what operation to perform with the parameter
 *
 * \retval number of bytes written on success.
 * \retval -errno on error.
 */
static int
param_display(struct param_opts *popt, char *pattern, char *value, int mode)
{
	glob_t paths;
	int rc, i;

	rc = cfs_get_param_paths(&paths, "%s", pattern);
	if (rc != 0) {
		rc = -errno;
		if (!popt->po_recursive) {
			fprintf(stderr, "error: list_param: %s: %s\n",
				pattern, strerror(errno));
		}
		return rc;
	}

	for (i = 0; i  < paths.gl_pathc; i++) {
		char *paramname = NULL, *tmp;
		char pathname[PATH_MAX];
		struct stat st;

		if (stat(paths.gl_pathv[i], &st) == -1) {
			rc = -errno;
			break;
		}

		if (popt->po_show_path)
			paramname = display_name(paths.gl_pathv[i], &st, popt);

		switch (mode) {
		case R_OK:
			/* Read the contents of file to stdout */
			if (S_ISREG(st.st_mode))
				read_param(paths.gl_pathv[i], &paramname, popt);
			break;
		case W_OK:
			if (S_ISREG(st.st_mode)) {
				rc = write_param(paths.gl_pathv[i], paramname,
						 popt, value);
			}
			break;
		case F_OK:
		default:
			if (paramname)
				printf("%s\n", paramname);
			break;
		}

		if (paramname) {
			free(paramname);
			paramname = NULL;
		}

		/* This section handles recursive listings */
		if (!S_ISDIR(st.st_mode) || !popt->po_recursive)
			continue;

		/* First test if we are doing lctl xxx_param '*' */
		if (strncmp(pattern, "*", 1)) {
			paramname = strdup(pattern);
			if (!paramname) {
				rc = -ENOMEM;
				break;
			}

			/* Chop off '*' for pattern/pattern */
			tmp = strstr(paramname, "*");
			if (tmp)
				*tmp = '\0';

			/* Chop off start of path until we hit paramname */
			tmp = strstr(paths.gl_pathv[i], paramname);
			free(paramname);
			if (!tmp) {
				rc = -EINVAL;
				break;
			}
		} else {
			/* Handle lctl xxx_param '*' case */
			tmp = rindex(paths.gl_pathv[i], '/') + 1;
		}

		rc = snprintf(pathname, sizeof(pathname), "%s/*", tmp);
		if (rc < 0) {
			break;
		} else if (rc >= sizeof(pathname)) {
			rc = -EINVAL;
			break;
		}

		rc = param_display(popt, pathname, value, mode);
		if (rc != 0 || rc != -ENOENT)
			continue;
	}

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
		clean_path(path);

		rc = param_display(&popt, path, NULL, F_OK);
		if (rc < 0)
			return rc;
	}

	return 0;
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
		clean_path(path);

		rc = param_display(&popt, path, NULL,
				   popt.po_only_path ? F_OK : R_OK);
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
		rc = param_display(&popt, "nodemap/*", NULL, F_OK);
	} else if (strcmp("all", argv[1]) == 0) {
		rc = param_display(&popt, "nodemap/*/*", NULL, R_OK);
	} else {
		char	pattern[PATH_MAX];

		snprintf(pattern, sizeof(pattern), "nodemap/%s/*", argv[1]);
		rc = param_display(&popt, pattern, NULL, R_OK);
		if (rc == -ESRCH)
			fprintf(stderr, "error: nodemap_info: cannot find"
					"nodemap %s\n", argv[1]);
	}
	return rc;
}

static int setparam_cmdline(int argc, char **argv, struct param_opts *popt)
{
        int ch;

        popt->po_show_path = 1;
        popt->po_only_path = 0;
        popt->po_show_type = 0;
        popt->po_recursive = 0;
	popt->po_params2 = 0;
	popt->po_delete = 0;

	while ((ch = getopt(argc, argv, "nPd")) != -1) {
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
                default:
                        return -1;
                }
        }
        return optind;
}

int jt_lcfg_setparam(int argc, char **argv)
{
	int rc = 0, index, i;
	struct param_opts popt;
	char *path = NULL, *value = NULL;

	memset(&popt, 0, sizeof(popt));
	index = setparam_cmdline(argc, argv, &popt);
	if (index < 0 || index >= argc)
		return CMD_HELP;

	if (popt.po_params2)
		/* We can't delete parameters that were
		 * set with old conf_param interface */
		return jt_lcfg_mgsparam2(argc, argv, &popt);

	for (i = index; i < argc; i++) {
		value = strchr(argv[i], '=');
		if (value != NULL) {
			/* format: set_param a=b */
			*value = '\0';
			value++;
			path = argv[i];
			if (*value == '\0')
				break;
		} else {
			/* format: set_param a b */
			if (path == NULL) {
				path = argv[i];
				continue;
			} else {
				value = argv[i];
			}
		}

		clean_path(path);

		rc = param_display(&popt, path, value, W_OK);
		path = NULL;
		value = NULL;
	}
	if (path != NULL && (value == NULL || *value == '\0'))
		fprintf(stderr, "error: %s: setting %s=: %s\n",
			jt_cmdname(argv[0]), path, strerror(rc = EINVAL));

	return rc;
}
