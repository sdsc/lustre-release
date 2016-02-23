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
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2012, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#if HAVE_CONFIG_H
#  include "config.h"
#endif /* HAVE_CONFIG_H */

#include "mount_utils.h"
#include <mntent.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <config.h>
#include <lustre_disk.h>
#include <lustre_ver.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <linux/loop.h>
#include <dlfcn.h>

#include <libcfs/libcfs.h>
#include <lustre/lustre_idl.h>
#include <lustre_cfg.h>
#include <dirent.h>

extern char *progname;
extern int verbose;

#define vprint(fmt, arg...) if (verbose > 0) printf(fmt, ##arg)
#define verrprint(fmt, arg...) if (verbose >= 0) fprintf(stderr, fmt, ##arg)

static struct module_backfs_ops *backfs_ops[LDD_MT_LAST];

void fatal(void)
{
        verbose = 0;
        fprintf(stderr, "\n%s FATAL: ", progname);
}

int run_command(char *cmd, int cmdsz)
{
        char log[] = "/tmp/run_command_logXXXXXX";
        int fd = -1, rc;

        if ((cmdsz - strlen(cmd)) < 6) {
                fatal();
                fprintf(stderr, "Command buffer overflow: %.*s...\n",
                        cmdsz, cmd);
                return ENOMEM;
        }

        if (verbose > 1) {
                fprintf(stderr, "cmd: %s\n", cmd);
        } else {
                if ((fd = mkstemp(log)) >= 0) {
                        close(fd);
                        strcat(cmd, " >");
                        strcat(cmd, log);
                }
        }
        strcat(cmd, " 2>&1");

        /* Can't use popen because we need the rv of the command */
        rc = system(cmd);
        if (rc && (fd >= 0)) {
                char buf[128];
                FILE *fp;
                fp = fopen(log, "r");
                if (fp) {
                        while (fgets(buf, sizeof(buf), fp) != NULL) {
                                printf("   %s", buf);
                        }
                        fclose(fp);
                }
        }
        if (fd >= 0)
                remove(log);
        return rc;
}

int add_param(char *buf, char *key, char *val)
{
	int end = sizeof(((struct lustre_disk_data *)0)->ldd_params);
	int start = strlen(buf);
	int keylen = 0;

	if (key)
		keylen = strlen(key);
	if (start + 1 + keylen + strlen(val) >= end) {
		fprintf(stderr, "%s: params are too long-\n%s %s%s\n",
			progname, buf, key ? key : "", val);
		return 1;
	}

	sprintf(buf + start, " %s%s", key ? key : "", val);
	return 0;
}

int get_param(char *buf, char *key, char **val)
{
	int i, key_len = strlen(key);
	char *ptr;

	ptr = strstr(buf, key);
	if (ptr) {
		*val = strdup(ptr + key_len);
		if (*val == NULL)
			return ENOMEM;

		for (i = 0; i < strlen(*val); i++)
			if (((*val)[i] == ' ') || ((*val)[i] == '\0'))
				break;

		(*val)[i] = '\0';
		return 0;
	}

	return ENOENT;
}

int append_param(char *buf, char *key, char *val, char sep)
{
	int key_len, i, offset, old_val_len;
	char *ptr = NULL, str[1024];

	if (key)
		ptr = strstr(buf, key);

	/* key doesn't exist yet, so just add it */
	if (ptr == NULL)
		return add_param(buf, key, val);

	key_len = strlen(key);

	/* Copy previous values to str */
	for (i = 0; i < sizeof(str); ++i) {
		if ((ptr[i+key_len] == ' ') || (ptr[i+key_len] == '\0'))
			break;
		str[i] = ptr[i+key_len];
	}
	if (i == sizeof(str))
		return E2BIG;
	old_val_len = i;

	offset = old_val_len+key_len;

	/* Move rest of buf to overwrite previous key and value */
	for (i = 0; ptr[i+offset] != '\0'; ++i)
		ptr[i] = ptr[i+offset];

	ptr[i] = '\0';

	snprintf(str+old_val_len, sizeof(str)-old_val_len, "%c%s", sep, val);

	return add_param(buf, key, str);
}

char *strscat(char *dst, char *src, int buflen)
{
	dst[buflen - 1] = 0;
	if (strlen(dst) + strlen(src) >= buflen) {
		fprintf(stderr, "string buffer overflow (max %d): '%s' + '%s'"
			"\n", buflen, dst, src);
		exit(EOVERFLOW);
	}
	return strcat(dst, src);
}

char *strscpy(char *dst, char *src, int buflen)
{
	dst[0] = 0;
	return strscat(dst, src, buflen);
}

int check_mtab_entry(char *spec1, char *spec2, char *mtpt, char *type)
{
	FILE *fp;
	struct mntent *mnt;

	fp = setmntent(MOUNTED, "r");
	if (fp == NULL)
		return 0;

	while ((mnt = getmntent(fp)) != NULL) {
		if ((strcmp(mnt->mnt_fsname, spec1) == 0 ||
		     strcmp(mnt->mnt_fsname, spec2) == 0) &&
		    (mtpt == NULL || strcmp(mnt->mnt_dir, mtpt) == 0) &&
		    (type == NULL || strcmp(mnt->mnt_type, type) == 0)) {
			endmntent(fp);
			return(EEXIST);
		}
	}
	endmntent(fp);

	return 0;
}

#define PROC_DIR	"/proc/"
static int mtab_is_proc(const char *mtab)
{
	char path[16];

	if (readlink(mtab, path, sizeof(path)) < 0)
		return 0;

	if (strncmp(path, PROC_DIR, strlen(PROC_DIR)))
		return 0;

	return 1;
}

int update_mtab_entry(char *spec, char *mtpt, char *type, char *opts,
		int flags, int freq, int pass)
{
	FILE *fp;
	struct mntent mnt;
	int rc = 0;

	/* Don't update mtab if it is linked to any file in /proc direcotry.*/
	if (mtab_is_proc(MOUNTED))
		return 0;

	mnt.mnt_fsname = spec;
	mnt.mnt_dir = mtpt;
	mnt.mnt_type = type;
	mnt.mnt_opts = opts ? opts : "";
	mnt.mnt_freq = freq;
	mnt.mnt_passno = pass;

	fp = setmntent(MOUNTED, "a+");
	if (fp == NULL) {
		fprintf(stderr, "%s: setmntent(%s): %s:",
			progname, MOUNTED, strerror (errno));
		rc = 16;
	} else {
		if ((addmntent(fp, &mnt)) == 1) {
			fprintf(stderr, "%s: addmntent: %s:",
				progname, strerror (errno));
			rc = 16;
		}
		endmntent(fp);
	}

	return rc;
}

/* Search for opt in mntlist, returning true if found.
 */
static int in_mntlist(char *opt, char *mntlist)
{
	char *ml, *mlp, *item, *ctx = NULL;

	if (!(ml = strdup(mntlist))) {
		fprintf(stderr, "%s: out of memory\n", progname);
		exit(1);
	}
	mlp = ml;
	while ((item = strtok_r(mlp, ",", &ctx))) {
		if (!strcmp(opt, item))
			break;
		mlp = NULL;
	}
	free(ml);
	return (item != NULL);
}

/* Issue a message on stderr for every item in wanted_mountopts that is not
 * present in mountopts.  The justwarn boolean toggles between error and
 * warning message.  Return an error count.
 */
int check_mountfsoptions(char *mountopts, char *wanted_mountopts)
{
	char *ml, *mlp, *item, *ctx = NULL;
	int errors = 0;

	if (!(ml = strdup(wanted_mountopts))) {
		fprintf(stderr, "%s: out of memory\n", progname);
		exit(1);
	}
	mlp = ml;
	while ((item = strtok_r(mlp, ",", &ctx))) {
		if (!in_mntlist(item, mountopts)) {
			fprintf(stderr, "%s: Error: mandatory mount option"
				" '%s' is missing\n", progname, item);
			errors++;
		}
		mlp = NULL;
	}
	free(ml);
	return errors;
}

/* Trim embedded white space, leading and trailing commas from string s.
 */
void trim_mountfsoptions(char *s)
{
	char *p;

	for (p = s; *p; ) {
		if (isspace(*p)) {
			memmove(p, p + 1, strlen(p + 1) + 1);
			continue;
		}
		p++;
	}

	while (s[0] == ',')
		memmove(&s[0], &s[1], strlen(&s[1]) + 1);

	p = s + strlen(s) - 1;
	while (p >= s && *p == ',')
		*p-- = '\0';
}

/* Setup a file in the first unused loop_device */
int loop_setup(struct mkfs_opts *mop)
{
	char loop_base[20];
	char l_device[64];
	int i, ret = 0;

	/* Figure out the loop device names */
	if (!access("/dev/loop0", F_OK | R_OK) ||
	    !access("/dev/loop-control", F_OK | R_OK)) {
		strcpy(loop_base, "/dev/loop\0");
	} else if (!access("/dev/loop/0", F_OK | R_OK)) {
		strcpy(loop_base, "/dev/loop/\0");
	} else {
		fprintf(stderr, "%s: can't access loop devices\n", progname);
		return EACCES;
	}

	/* Find unused loop device */
	for (i = 0; i < MAX_LOOP_DEVICES; i++) {
		char cmd[PATH_MAX];
		int cmdsz = sizeof(cmd);

#ifdef HAVE_LOOP_CTL_GET_FREE
		ret = open("/dev/loop-control", O_RDWR);
		if (ret < 0) {
			fprintf(stderr, "%s: can't access loop control\n", progname);
			return EACCES;
		}
		/* find or allocate a free loop device to use */
		i = ioctl(ret, LOOP_CTL_GET_FREE);
		if (i < 0) {
			fprintf(stderr, "%s: access loop control error\n", progname);
			return EACCES;
		}
		sprintf(l_device, "%s%d", loop_base, i);
#else
		sprintf(l_device, "%s%d", loop_base, i);
		if (access(l_device, F_OK | R_OK))
			break;
#endif
		snprintf(cmd, cmdsz, "losetup %s > /dev/null 2>&1", l_device);
		ret = system(cmd);

		/* losetup gets 1 (ret=256) for non-set-up device */
		if (ret) {
			/* Set up a loopback device to our file */
			snprintf(cmd, cmdsz, "losetup %s %s", l_device,
				 mop->mo_device);
			ret = run_command(cmd, cmdsz);
			if (ret == 256)
				/* someone else picked up this loop device
				 * behind our back */
				continue;
			if (ret) {
				fprintf(stderr, "%s: error %d on losetup: %s\n",
					progname, ret,
					ret >= 0 ? strerror(ret) : "");
				return ret;
			}
			strscpy(mop->mo_loopdev, l_device,
				sizeof(mop->mo_loopdev));
			return ret;
		}
	}

	fprintf(stderr, "%s: out of loop devices!\n", progname);
	return EMFILE;
}

int loop_cleanup(struct mkfs_opts *mop)
{
	char cmd[150];
	int ret = 0;

	if ((mop->mo_flags & MO_IS_LOOP) && *mop->mo_loopdev) {
		int tries;

		sprintf(cmd, "losetup -d %s", mop->mo_loopdev);
		for (tries = 0; tries < 3; tries++) {
			ret = run_command(cmd, sizeof(cmd));
			if (ret == 0)
				break;
			sleep(1);
		}
	}

	if (ret != 0)
		fprintf(stderr, "cannot cleanup %s: rc = %d\n",
			mop->mo_loopdev, ret);
	return ret;
}

int loop_format(struct mkfs_opts *mop)
{
	int fd;

	if (mop->mo_device_kb == 0) {
		fatal();
		fprintf(stderr, "loop device requires a --device-size= "
			"param\n");
		return EINVAL;
	}

	fd = creat(mop->mo_device, S_IRUSR|S_IWUSR);
	if (fd < 0) {
		fatal();
		fprintf(stderr, "%s: Unable to create backing store: %s\n",
			progname, strerror(errno));
		return errno;
	}

	if (ftruncate(fd, mop->mo_device_kb * 1024) != 0) {
		close(fd);
		fatal();
		fprintf(stderr, "%s: Unable to truncate backing store: %s\n",
			progname, strerror(errno));
		return errno;
	}

	close(fd);
	return 0;
}

#define DLSYM(prefix, sym, func)					\
	do {								\
		char _fname[64];					\
		snprintf(_fname, sizeof(_fname), "%s_%s", prefix, #func); \
		sym->func = (typeof(sym->func))dlsym(sym->dl_handle, _fname); \
	} while (0)

/**
 * Load plugin for a given mount_type from ${pkglibdir}/mount_osd_FSTYPE.so and
 * return struct of function pointers (will be freed in unloack_backfs_module).
 *
 * \param[in] mount_type	Mount type to load module for.
 * \retval Value of backfs_ops struct
 * \retval NULL if no module exists
 */
struct module_backfs_ops *load_backfs_module(enum ldd_mount_type mount_type)
{
	void *handle;
	char *error, filename[512], fsname[512], *name;
	struct module_backfs_ops *ops;

	/* This deals with duplicate ldd_mount_types resolving to same OSD layer
	 * plugin (e.g. ext3/ldiskfs/ldiskfs2 all being ldiskfs) */
	strncpy(fsname, mt_type(mount_type), sizeof(fsname));
	name = fsname + sizeof("osd-") - 1;

	/* change osd- to osd_ */
	fsname[sizeof("osd-") - 2] = '_';

	snprintf(filename, sizeof(filename), PLUGIN_DIR"/mount_%s.so", fsname);

	handle = dlopen(filename, RTLD_LAZY);

	/* Check for $LUSTRE environment variable from test-framework.
	 * This allows using locally built modules to be used.
	 */
	if (handle == NULL) {
		char *dirname;
		dirname = getenv("LUSTRE");
		if (dirname) {
			snprintf(filename, sizeof(filename),
				 "%s/utils/.libs/mount_%s.so",
				 dirname, fsname);
			handle = dlopen(filename, RTLD_LAZY);
		}
	}

	/* Do not clutter up console with missing types */
	if (handle == NULL)
		return NULL;

	ops = malloc(sizeof(*ops));
	if (ops == NULL) {
		dlclose(handle);
		return NULL;
	}

	ops->dl_handle = handle;
	dlerror(); /* Clear any existing error */

	DLSYM(name, ops, init);
	DLSYM(name, ops, fini);
	DLSYM(name, ops, read_ldd);
	DLSYM(name, ops, write_ldd);
	DLSYM(name, ops, is_lustre);
	DLSYM(name, ops, make_lustre);
	DLSYM(name, ops, prepare_lustre);
	DLSYM(name, ops, tune_lustre);
	DLSYM(name, ops, label_lustre);
	DLSYM(name, ops, rename_fsname);
	DLSYM(name, ops, enable_quota);

	error = dlerror();
	if (error != NULL) {
		fatal();
		fprintf(stderr, "%s\n", error);
		dlclose(handle);
		free(ops);
		return NULL;
	}

	/* optional methods */
	DLSYM(name, ops, fix_mountopts);

	return ops;
}

/**
 * Unload plugin and free backfs_ops structure. Must be called the same number
 * of times as load_backfs_module is.
 */
void unload_backfs_module(struct module_backfs_ops *ops)
{
	if (ops == NULL)
		return;

	dlclose(ops->dl_handle);
	free(ops);
}

/* Return true if backfs_ops has operations for the given mount_type. */
int backfs_mount_type_okay(enum ldd_mount_type mount_type)
{
	if (unlikely(mount_type >= LDD_MT_LAST || mount_type < 0)) {
		fatal();
		fprintf(stderr, "fs type out of range %d\n", mount_type);
		return 0;
	}
	if (backfs_ops[mount_type] == NULL) {
		fatal();
		fprintf(stderr, "unhandled/unloaded fs type %d '%s'\n",
			mount_type, mt_str(mount_type));
		return 0;
	}
	return 1;
}

/* Write the server config files */
int osd_write_ldd(struct mkfs_opts *mop)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->write_ldd(mop);

	else
		ret = EINVAL;

	return ret;
}

/* Read the server config files */
int osd_read_ldd(char *dev, struct lustre_disk_data *ldd)
{
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->read_ldd(dev, ldd);

	else
		ret = EINVAL;

	return ret;
}

/* Was this device formatted for Lustre */
int osd_is_lustre(char *dev, unsigned *mount_type)
{
	int i;

	vprint("checking for existing Lustre data: ");

	for (i = 0; i < LDD_MT_LAST; ++i) {
		if (backfs_ops[i] != NULL &&
		    backfs_ops[i]->is_lustre(dev, mount_type)) {
			vprint("found\n");
			return 1;
		}
	}

	vprint("not found\n");
	return 0;
}

/* Build fs according to type */
int osd_make_lustre(struct mkfs_opts *mop)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->make_lustre(mop);

	else
		ret = EINVAL;

	return ret;
}

int osd_prepare_lustre(struct mkfs_opts *mop,
		       char *wanted_mountopts, size_t len)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->prepare_lustre(mop,
							wanted_mountopts, len);

	else
		ret = EINVAL;

	return ret;
}

int osd_fix_mountopts(struct mkfs_opts *mop, char *mountopts, size_t len)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;

	if (!backfs_mount_type_okay(ldd->ldd_mount_type))
		return EINVAL;

	if (backfs_ops[ldd->ldd_mount_type]->fix_mountopts == NULL)
		return 0;

	return backfs_ops[ldd->ldd_mount_type]->fix_mountopts(mop, mountopts,
							      len);
}

int osd_tune_lustre(char *dev, struct mount_opts *mop)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->tune_lustre(dev, mop);

	else
		ret = EINVAL;

	return ret;
}

int osd_label_lustre(struct mount_opts *mop)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->label_lustre(mop);

	else
		ret = EINVAL;

	return ret;
}

/* Rename filesystem fsname */
int osd_rename_fsname(struct mkfs_opts *mop, const char *oldname)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->rename_fsname(mop,
								     oldname);
	else
		ret = EINVAL;

	return ret;
}

/* Enable quota accounting */
int osd_enable_quota(struct mkfs_opts *mop)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	int ret;

	if (backfs_mount_type_okay(ldd->ldd_mount_type))
		ret = backfs_ops[ldd->ldd_mount_type]->enable_quota(mop);

	else
		ret = EINVAL;

	return ret;
}

int osd_init(void)
{
	int i, rc, ret = EINVAL;

	for (i = 0; i < LDD_MT_LAST; ++i) {
		rc = 0;
		backfs_ops[i] = load_backfs_module(i);
		if (backfs_ops[i] != NULL)
			rc = backfs_ops[i]->init();
		if (rc != 0) {
			backfs_ops[i]->fini();
			unload_backfs_module(backfs_ops[i]);
			backfs_ops[i] = NULL;
		} else
			ret = 0;
	}

	return ret;
}

void osd_fini(void)
{
	int i;

	for (i = 0; i < LDD_MT_LAST; ++i) {
		if (backfs_ops[i] != NULL) {
			backfs_ops[i]->fini();
			unload_backfs_module(backfs_ops[i]);
			backfs_ops[i] = NULL;
		}
	}
}

__u64 get_device_size(char* device)
{
	int ret, fd;
	__u64 size = 0;

	fd = open(device, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "%s: cannot open %s: %s\n",
			progname, device, strerror(errno));
		return 0;
	}

#ifdef BLKGETSIZE64
	/* size in bytes. bz5831 */
	ret = ioctl(fd, BLKGETSIZE64, (void*)&size);
#else
	{
		__u32 lsize = 0;
		/* size in blocks */
		ret = ioctl(fd, BLKGETSIZE, (void*)&lsize);
		size = (__u64)lsize * 512;
	}
#endif
	close(fd);
	if (ret < 0) {
		fprintf(stderr, "%s: size ioctl failed: %s\n",
			progname, strerror(errno));
		return 0;
	}

	vprint("device size = "LPU64"MB\n", size >> 20);
	/* return value in KB */
	return size >> 10;
}

int file_create(char *path, __u64 size)
{
	__u64 size_max;
	int ret;
	int fd;

	/*
	 * Since "size" is in KB, the file offset it represents could overflow
	 * off_t.
	 */
	size_max = (off_t)1 << (_FILE_OFFSET_BITS - 1 - 10);
	if (size >= size_max) {
		fprintf(stderr, "%s: "LPU64" KB: Backing store size must be "
			"smaller than "LPU64" KB\n", progname, size, size_max);
		return EFBIG;
	}

	ret = access(path, F_OK);
	if (ret == 0) {
		ret = unlink(path);
		if (ret != 0)
			return errno;
	}

	fd = creat(path, S_IRUSR|S_IWUSR);
	if (fd < 0) {
		fatal();
		fprintf(stderr, "%s: Unable to create backing store: %s\n",
			progname, strerror(errno));
		return errno;
	}

	ret = ftruncate(fd, size * 1024);
	close(fd);
	if (ret != 0) {
		fatal();
		fprintf(stderr, "%s: Unable to truncate backing store: %s\n",
			progname, strerror(errno));
		return errno;
	}

	return 0;
}

struct lustre_cfg_entry {
	struct list_head lce_list;
	char		 lce_name[0];
};

static struct lustre_cfg_entry *lustre_cfg_entry_init(const char *name)
{
	struct lustre_cfg_entry *lce;

	lce = malloc(sizeof(*lce) + strlen(name));
	if (lce != NULL) {
		INIT_LIST_HEAD(&lce->lce_list);
		strcpy(lce->lce_name, name);
	}

	return lce;
}

static void lustre_cfg_entry_fini(struct lustre_cfg_entry *lce)
{
	free(lce);
}

static bool contain_valid_fsname(char *buf, const char *fsname,
				 int buflen, int namelen)
{
	if (buflen < namelen)
		return false;

	if (memcmp(buf, fsname, namelen) != 0)
		return false;

	if (buf[namelen] != '\0' && buf[namelen] != '-')
		return false;

	return true;
}

static void lustre_cfg_pad(struct llog_rec_hdr *rec, int len, int idx)
{
	struct llog_rec_tail *tail = (struct llog_rec_tail *)((char *)rec +
					len - sizeof(struct llog_rec_tail));

	rec->lrh_len = tail->lrt_len = cpu_to_le32(len);
	rec->lrh_index = tail->lrt_index = cpu_to_le32(idx);
	rec->lrh_type = cpu_to_le32(LLOG_PAD_MAGIC);
	rec->lrh_id = 0;
}

/**
 * Re-generate the config log record.
 *
 * This function parses the given config log record, if contains
 * filesystem name (old_fsname), then replace it with the new
 * filesystem name (new_fsname). The new config log record will
 * be written in the new RAM buffer that is different from the
 * buffer holding the old config log record.
 *
 * \param[in] w_rec		the buffer to hold new config log
 * \param[in] r_rec		the buffer that contains old config log
 * \param[in] new_fsname	the new filesystem name
 * \param[in] old_fsname	the old filesystem name
 * \param[in] idx		the index of the config log record
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
static int lustre_regen_cfg_rec(struct llog_rec_hdr *w_rec,
				struct llog_rec_hdr *r_rec,
				const char *new_fsname,
				const char *old_fsname, int idx)
{
	struct lustre_cfg *w_lcfg = (struct lustre_cfg *)(w_rec + 1);
	struct lustre_cfg *r_lcfg = (struct lustre_cfg *)(r_rec + 1);
	struct llog_rec_tail *w_tail;
	char *w_buf;
	char *r_buf;
	int r_buflen;
	int new_namelen = strlen(new_fsname);
	int old_namelen = strlen(old_fsname);
	int diff = new_namelen - old_namelen;
	__u32 cmd = le32_to_cpu(r_lcfg->lcfg_command);
	__u32 cnt = le32_to_cpu(r_lcfg->lcfg_bufcount);
	int w_len = sizeof(struct llog_rec_hdr)+ LCFG_HDR_SIZE(cnt) +
		    sizeof(struct llog_rec_tail);
	int i;

	*w_rec = *r_rec;
	*w_lcfg = *r_lcfg;
	switch (cmd) {
	case LCFG_MARKER: {
		struct cfg_marker *r_marker;
		struct cfg_marker *w_marker;
		int tgt_namelen;

		if (cnt != 2) {
			fprintf(stderr, "Unknown cfg marker entry with %d "
				"buffers\n", cnt);
			return -EINVAL;
		}

		/* buf[0] */
		r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[0]);
		w_lcfg->lcfg_buflens[0] = r_lcfg->lcfg_buflens[0];
		if (unlikely(r_buflen != 0)) {
			/* copy it directly if it is not empty */
			r_buf = lustre_cfg_buf(r_lcfg, 0);
			w_buf = lustre_cfg_buf(w_lcfg, 0);
			memcpy(w_buf, r_buf, r_buflen);
			w_len += cfs_size_round(r_buflen);
		}

		/* buf[1] */
		r_buf = lustre_cfg_buf(r_lcfg, 1);
		w_buf = lustre_cfg_buf(w_lcfg, 1);
		r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[1]);
		w_lcfg->lcfg_buflens[1] = r_lcfg->lcfg_buflens[1];
		r_marker = (struct cfg_marker *)r_buf;
		w_marker = (struct cfg_marker *)w_buf;
		*w_marker = *r_marker;
		w_len += cfs_size_round(r_buflen);
		if (unlikely(!contain_valid_fsname(r_marker->cm_tgtname,
						   old_fsname,
						   sizeof(r_marker->cm_tgtname),
						   old_namelen)))
			break;

		memcpy(w_marker->cm_tgtname, new_fsname, new_namelen);
		tgt_namelen = strlen(r_marker->cm_tgtname);
		if (tgt_namelen > old_namelen)
			memcpy(w_marker->cm_tgtname + new_namelen,
			       r_marker->cm_tgtname + old_namelen,
			       tgt_namelen - old_namelen);
		w_marker->cm_tgtname[tgt_namelen + diff] = '\0';
		break;
	}
	case LCFG_PARAM:
	case LCFG_SET_PARAM: {
		/* buf[0] */
		r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[0]);
		if (likely(r_buflen != 0)) {
			r_buf = lustre_cfg_buf(r_lcfg, 0);
			w_buf = lustre_cfg_buf(w_lcfg, 0);
			if (contain_valid_fsname(r_buf, old_fsname,
						 r_buflen, old_namelen)) {
				w_lcfg->lcfg_buflens[0] = cpu_to_le32(r_buflen +
								      diff);
				memcpy(w_buf, new_fsname, new_namelen);
				memcpy(w_buf + new_namelen, r_buf + old_namelen,
				       r_buflen - old_namelen);
				w_len += cfs_size_round(r_buflen + diff);
			} else {
				w_lcfg->lcfg_buflens[0] =
						r_lcfg->lcfg_buflens[0];
				memcpy(w_buf, r_buf, r_buflen);
				w_len += cfs_size_round(r_buflen);
			}
		} else {
			w_lcfg->lcfg_buflens[0] = 0;
		}

		for (i = 1; i < cnt; i++) {
			/* buf[i] is the param value, copy it directly */
			r_buf = lustre_cfg_buf(r_lcfg, i);
			w_buf = lustre_cfg_buf(w_lcfg, i);
			r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[i]);
			w_lcfg->lcfg_buflens[i] = r_lcfg->lcfg_buflens[i];
			memcpy(w_buf, r_buf, r_buflen);
			w_len += cfs_size_round(r_buflen);
		}
		break;
	}
	case LCFG_POOL_NEW:
	case LCFG_POOL_ADD:
	case LCFG_POOL_REM:
	case LCFG_POOL_DEL: {
		if (cnt < 3 || cnt > 4) {
			fprintf(stderr, "Unknown cfg pool (%x) entry with %d "
				"buffers\n", cmd, cnt);
			return -EINVAL;
		}

		for (i = 0; i < 2; i++) {
			r_buf = lustre_cfg_buf(r_lcfg, i);
			w_buf = lustre_cfg_buf(w_lcfg, i);
			r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[i]);
			w_lcfg->lcfg_buflens[i] = cpu_to_le32(r_buflen + diff);
			memcpy(w_buf, new_fsname, new_namelen);
			memcpy(w_buf + new_namelen, r_buf + old_namelen,
			       r_buflen - old_namelen);
			w_len += cfs_size_round(r_buflen + diff);
		}

		/* buf[2] is the pool name, copy it directly */
		r_buf = lustre_cfg_buf(r_lcfg, 2);
		w_buf = lustre_cfg_buf(w_lcfg, 2);
		r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[2]);
		w_lcfg->lcfg_buflens[2] = r_lcfg->lcfg_buflens[2];
		memcpy(w_buf, r_buf, r_buflen);
		w_len += cfs_size_round(r_buflen);

		if (cnt == 3)
			break;

		/* buf[3] is ostname */
		r_buf = lustre_cfg_buf(r_lcfg, 3);
		w_buf = lustre_cfg_buf(w_lcfg, 3);
		r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[3]);
		w_lcfg->lcfg_buflens[3] = cpu_to_le32(r_buflen + diff);
		memcpy(w_buf, new_fsname, new_namelen);
		memcpy(w_buf + new_namelen, r_buf + old_namelen,
		       r_buflen - old_namelen);
		w_len += cfs_size_round(r_buflen + diff);
		break;
	}
	case LCFG_SETUP: {
		if (cnt == 2) {
			r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[1]);
			if (r_buflen == sizeof(struct lov_desc) ||
			    r_buflen == sizeof(struct lmv_desc)) {
				char *r_uuid;
				char *w_uuid;
				int uuid_len;

				/* buf[0] */
				r_buf = lustre_cfg_buf(r_lcfg, 0);
				w_buf = lustre_cfg_buf(w_lcfg, 0);
				r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[0]);
				w_lcfg->lcfg_buflens[0] =
					cpu_to_le32(r_buflen + diff);
				memcpy(w_buf, new_fsname, new_namelen);
				memcpy(w_buf + new_namelen, r_buf + old_namelen,
				       r_buflen - old_namelen);
				w_len += cfs_size_round(r_buflen + diff);

				/* buf[1] */
				r_buf = lustre_cfg_buf(r_lcfg, 1);
				w_buf = lustre_cfg_buf(w_lcfg, 1);
				r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[1]);
				w_lcfg->lcfg_buflens[1] =
						r_lcfg->lcfg_buflens[1];
				if (r_buflen == sizeof(struct lov_desc)) {
					struct lov_desc *r_desc =
						(struct lov_desc *)r_buf;
					struct lov_desc *w_desc =
						(struct lov_desc *)w_buf;

					*w_desc = *r_desc;
					r_uuid = r_desc->ld_uuid.uuid;
					w_uuid = w_desc->ld_uuid.uuid;
					uuid_len = sizeof(r_desc->ld_uuid.uuid);
				} else {
					struct lmv_desc *r_desc =
						(struct lmv_desc *)r_buf;
					struct lmv_desc *w_desc =
						(struct lmv_desc *)w_buf;

					*w_desc = *r_desc;
					r_uuid = r_desc->ld_uuid.uuid;
					w_uuid = w_desc->ld_uuid.uuid;
					uuid_len = sizeof(r_desc->ld_uuid.uuid);
				}
				w_len += cfs_size_round(r_buflen);

				if (unlikely(!contain_valid_fsname(r_uuid,
					old_fsname, uuid_len, old_namelen)))
					break;

				memcpy(w_uuid, new_fsname, new_namelen);
				uuid_len = strlen(r_uuid);
				if (uuid_len > old_namelen)
					memcpy(w_uuid + new_namelen,
					       r_uuid + old_namelen,
					       uuid_len - old_namelen);
				w_uuid[uuid_len + diff] = '\0';
				break;
			} /* else case go through */
		} /* else case go through */
	}
	default: {
		for (i = 0; i < cnt; i++) {
			r_buflen = le32_to_cpu(r_lcfg->lcfg_buflens[i]);
			if (r_buflen == 0) {
				w_lcfg->lcfg_buflens[i] = 0;
				continue;
			}

			r_buf = lustre_cfg_buf(r_lcfg, i);
			w_buf = lustre_cfg_buf(w_lcfg, i);
			if (!contain_valid_fsname(r_buf, old_fsname,
						  r_buflen, old_namelen)) {
				w_lcfg->lcfg_buflens[i] =
					r_lcfg->lcfg_buflens[i];
				memcpy(w_buf, r_buf, r_buflen);
				w_len += cfs_size_round(r_buflen);
				continue;
			}

			w_len += cfs_size_round(r_buflen + diff);
			if (r_buflen == old_namelen) {
				w_lcfg->lcfg_buflens[i] =
					cpu_to_le32(new_namelen + 1);
				memcpy(w_buf, new_fsname, new_namelen);
				continue;
			}

			w_lcfg->lcfg_buflens[i] = cpu_to_le32(r_buflen + diff);
			memcpy(w_buf, new_fsname, new_namelen);
			memcpy(w_buf + new_namelen, r_buf + old_namelen,
			       r_buflen - old_namelen);
		}
		break;
	}
	}

	w_tail = (struct llog_rec_tail *)((char *)w_rec + w_len -
		sizeof(struct llog_rec_tail));
	w_rec->lrh_len = w_tail->lrt_len = cpu_to_le32(w_len);
	w_rec->lrh_index = w_tail->lrt_index = cpu_to_le32(idx);

	return 0;
}

/**
 * Re-generate config log.
 *
 * This function reads the given config log, for each record,
 * if contains filesystem name (old_fsname), then replace it
 * with the new filesystem name (new_fsname). The new config
 * log is recorded in RAM buffer temporarily, and will be
 * written back to the original config log file after all
 * the config log records handled. Finally, it will rename
 * the old config log filename (that contains the old_name)
 * to the new config log filename.
 *
 * \param[in] new_fsname	the new filesystem name
 * \param[in] old_fsname	the old filesystem name
 * \param[in] cfg_dir		point to the config logs directory
 * \param[in] old_cfg		the old config log filename
 *
 * \retval			0 for success
 * \retval			negative error number on failure
 */
static int lustre_regen_cfg(const char *new_fsname, const char *old_fsname,
			    const char *cfg_dir, const char *old_cfg)
{
	struct stat st;
	char filepnm[128];
	char filepnm_new[128];
	char *r_buf = NULL;
	char *w_buf = NULL;
	struct llog_log_hdr *r_llog_hdr;
	struct llog_log_hdr *w_llog_hdr;
	char *r_ptr;
	char *w_ptr;
	int fd = -1;
	int r_size;
	int w_size;
	int r_idx = 0;
	int w_idx = 0;
	int r_count;
	int w_count = 1;
	int boundary;
	int rc = 0;

	sprintf(filepnm, "%s/%s", cfg_dir, old_cfg);
	fd = open(filepnm, O_RDWR);
	if (fd < 0) {
		fprintf(stderr, "Cannot open %s: %s\n",
			filepnm, strerror(rc = errno));
		goto out;
	}

	rc = fstat(fd, &st);
	if (rc < 0) {
		fprintf(stderr, "Cannot stat %s for read: %s\n",
			filepnm, strerror(rc = errno));
		goto out;
	}

	if (unlikely(st.st_size == 0))
		goto out;

	r_size = st.st_size;
	r_buf = malloc(r_size);
	if (r_buf == NULL) {
		fprintf(stderr, "Cannot allocate RAM (%d) for read %s: %s\n",
			r_size, filepnm, strerror(rc = errno));
		goto out;
	}

	rc = read(fd, r_buf, r_size);
	if (rc != r_size) {
		fprintf(stderr, "Fail to read %s: %s, expect %d, read %d\n",
			filepnm, strerror(errno), r_size, rc);
		rc = errno;
		goto out;
	}

	if (strlen(new_fsname) <= strlen(old_fsname))
		w_size = r_size;
	else
		w_size = r_size * 2; /* large enough */

	w_buf = malloc(w_size);
	if (w_buf == NULL) {
		fprintf(stderr, "Cannot allocate RAM (%d) for write %s: %s\n",
			w_size, filepnm, strerror(rc = errno));
		goto out;
	}

	memset(w_buf, 0 , w_size);
	r_llog_hdr = (struct llog_log_hdr *)r_buf;
	w_llog_hdr = (struct llog_log_hdr *)w_buf;
	*w_llog_hdr = *r_llog_hdr;
	memset(w_llog_hdr->llh_bitmap, 0, sizeof(w_llog_hdr->llh_bitmap));
	ext2_set_bit(0, (void *)w_llog_hdr->llh_bitmap);
	r_count = le32_to_cpu(r_llog_hdr->llh_count) - 1;
	w_size = le32_to_cpu(r_llog_hdr->llh_hdr.lrh_len);
	r_ptr = r_buf + w_size;
	w_ptr = w_buf + w_size;
	boundary = (w_size & ~(LLOG_MIN_CHUNK_SIZE - 1)) + LLOG_MIN_CHUNK_SIZE;

	while (r_count > 0) {
		struct llog_rec_hdr *r_rec_hdr = (struct llog_rec_hdr*)r_ptr;
		struct llog_rec_hdr *w_rec_hdr = (struct llog_rec_hdr*)w_ptr;

		r_idx = le32_to_cpu(r_rec_hdr->lrh_index);
		if (ext2_test_bit(r_idx, (void *)r_llog_hdr->llh_bitmap)) {
			switch (le32_to_cpu(r_rec_hdr->lrh_type)) {
			case OBD_CFG_REC: {
				int left = boundary - w_size;

				w_idx++;
				rc = lustre_regen_cfg_rec(w_rec_hdr, r_rec_hdr,
							  new_fsname,
							  old_fsname, w_idx);
				if (rc != 0)
					goto out;

				w_size += le32_to_cpu(w_rec_hdr->lrh_len);
				if (w_size <= boundary - LLOG_MIN_REC_SIZE)
					goto forward;

				boundary += LLOG_MIN_CHUNK_SIZE;
				w_size -= le32_to_cpu(w_rec_hdr->lrh_len);
				lustre_cfg_pad(w_rec_hdr, left, w_idx);
				w_size += le32_to_cpu(w_rec_hdr->lrh_len);
				w_ptr = w_buf + w_size;
				w_rec_hdr = (struct llog_rec_hdr*)w_ptr;
				w_idx++;
				rc = lustre_regen_cfg_rec(w_rec_hdr, r_rec_hdr,
							  new_fsname,
							  old_fsname, w_idx);
				if (rc != 0)
					goto out;

				w_size += le32_to_cpu(w_rec_hdr->lrh_len);

forward:
				w_ptr = w_buf + w_size;
				ext2_set_bit(w_idx,
					     (void *)w_llog_hdr->llh_bitmap);
				w_count++;
				break;
			}
			case LLOG_PAD_MAGIC:
				/* skip padding */
				break;
			default:
				fprintf(stderr, "Unexpected cfg type: 0x%x\n",
					le32_to_cpu(r_rec_hdr->lrh_type));
				rc = -EINVAL;
				goto out;
			}

			r_count--;
		}

		r_ptr += le32_to_cpu(r_rec_hdr->lrh_len);
		if ((r_ptr - r_buf) > r_size) {
			fprintf(stderr, "%s is corrupted\n", old_cfg);
			rc = -EINVAL;
			goto out;
		}
	}

	w_llog_hdr->llh_count = cpu_to_le32(w_count);
	w_llog_hdr->llh_tail.lrt_index = cpu_to_le32(w_idx);

	rc = lseek64(fd, 0, SEEK_SET);
	if (rc < 0) {
		fprintf(stderr, "Fail to seek %s: %s\n",
			filepnm, strerror(rc = errno));
		goto out;
	}

	rc = write(fd, w_buf, w_size);
	if (rc != w_size) {
		fprintf(stderr, "Fail to write %s: %s, expect %d, read %d\n",
			filepnm, strerror(errno), w_size, rc);
		rc = errno;
		goto out;
	}

	if (w_size < r_size) {
		rc = ftruncate(fd, w_size);
		if (rc < 0) {
			fprintf(stderr, "Cannot truncate %s: %s\n",
				filepnm, strerror(rc = errno));
			goto out;
		}
	}

	sprintf(filepnm_new, "%s/%s%s", cfg_dir, new_fsname,
		old_cfg + strlen(old_fsname));
	rc = rename(filepnm, filepnm_new);
	if (rc < 0) {
		fprintf(stderr, "Cannot rename %s to %s: %s\n",
			filepnm, filepnm_new, strerror(rc = errno));
		goto out;
	}

out:
	if (fd >= 0)
		close(fd);
	if (r_buf != NULL)
		free(r_buf);
	if (w_buf != NULL)
		free(w_buf);
	return rc;
}

int lustre_rename_fsname(struct mkfs_opts *mop, const char *oldname,
			 const char *mntpt)
{
	struct lustre_disk_data *ldd = &mop->mo_ldd;
	char filepnm[128];
	char cfg_dir[128];
	DIR *dir = NULL;
	struct dirent64 *dirent;
	struct lustre_cfg_entry *lce;
	struct list_head cfg_list;
	int namelen = strlen(oldname);
	int ret;

	INIT_LIST_HEAD(&cfg_list);

	/* remove last_rcvd which can be re-created when re-mount. */
	sprintf(filepnm, "%s/%s", mntpt, LAST_RCVD);
	ret = unlink(filepnm);
	if (ret != 0 && errno != ENOENT) {
		if (errno != 0)
			ret = errno;
		fprintf(stderr, "Unable to unlink %s: %s\n",
			filepnm, strerror(ret));
		return ret;
	}

	sprintf(cfg_dir, "%s/%s", mntpt, MOUNT_CONFIGS_DIR);
	dir = opendir(cfg_dir);
	if (dir == NULL) {
		if (errno != 0)
			ret = errno;
		else
			ret = EINVAL;
		fprintf(stderr, "Unable to opendir %s: %s\n",
			cfg_dir, strerror(ret));
		return ret;
	}

	while ((dirent = readdir64(dir)) != NULL) {
		char *ptr;

		if (strlen(dirent->d_name) <= namelen)
			continue;

		ptr = strrchr(dirent->d_name, '-');
		if (ptr == NULL || (ptr - dirent->d_name) != namelen)
			continue;

		if (strncmp(dirent->d_name, oldname, namelen) != 0)
			continue;

		lce = lustre_cfg_entry_init(dirent->d_name);
		if (lce == NULL) {
			if (errno != 0)
				ret = errno;
			else
				ret = EINVAL;
			fprintf(stderr, "Fail to init item for %s: %s\n",
				dirent->d_name, strerror(ret));
			goto out;
		}

		list_add(&lce->lce_list, &cfg_list);
	}

	closedir(dir);
	dir = NULL;
	ret = 0;

	while (!list_empty(&cfg_list) && ret == 0) {
		lce = list_entry(cfg_list.next, struct lustre_cfg_entry,
				 lce_list);
		list_del(&lce->lce_list);
		if (IS_MGS(ldd)) {
			ret = lustre_regen_cfg(ldd->ldd_fsname, oldname,
					       cfg_dir, lce->lce_name);
		} else {
			sprintf(filepnm, "%s/%s", cfg_dir, lce->lce_name);
			ret = unlink(filepnm);
			if (ret != 0) {
				if (errno != 0)
					ret = errno;
				fprintf(stderr, "Fail to unlink %s: %s\n",
					filepnm, strerror(ret));
			}
		}
		lustre_cfg_entry_fini(lce);
	}

out:
	if (dir != NULL)
		closedir(dir);

	while (!list_empty(&cfg_list)) {
		lce = list_entry(cfg_list.next, struct lustre_cfg_entry,
				 lce_list);
		list_del(&lce->lce_list);
		lustre_cfg_entry_fini(lce);
	}
	return ret;
}
