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
 * Copyright  2009 Sun Microsystems, Inc. All rights reserved
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Author: Nathan Rutman <nathan.rutman@sun.com>
 *
 */

/* HSM copytool program for Posix filesystem-based HSM's.
 *
 * An HSM copytool daemon acts on action requests from Lustre to copy files
 * to and from an HSM archive system. This one in particular makes regular
 * Posix filesystem calls to a given path, where an HSM is presumably mounted.
 *
 * This particular tool can also import an existing HSM archive.
 */

#include <utime.h>
#include <sys/xattr.h>
#include <dirent.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustreapi.h>

#define TOOL 1
#define DAEMON 2
static int bintype;

/* Progress reporting period */
#define REPORT_INTERVAL 30
/* HSM hash subdir permissions */
#define DIR_PERM 0700
/* HSM hash file permissions */
#define FILE_PERM 0600

#define ONE_MB 0x100000
#define CHUNKSIZE  ONE_MB

#define V_ERR    1
#define V_WARN   2
#define V_STATUS 3
#define V_TRACE  4

/* copytool uses a 32b bitmask field to register with kuc
 * archive num = 0 => all
 * archive num from 1 to 32
 */
#define MAX_ARCHIVE_CNT (sizeof(__u32) * 8)

struct options {
	int	 err_major;
	int	 err_minor;
	int	 o_attr;
	int	 o_check;
	int	 o_dryrun;
	int	 o_err_abort;
	int	 o_shadow;
	int	 o_verbose;
	int	 o_xattr;
	int	 o_archive_cnt;
	int	 o_archive_id[MAX_ARCHIVE_CNT];
	int	 o_bw;
	char	*o_fsname;
	char	*o_hsmroot;
	char	*o_src; /* for import */
	char	*o_dst; /* for import */
};

/* everything else is zeroed */
struct options opt = {.o_verbose = V_STATUS,
	.o_xattr = 1, .o_attr = 1, .o_shadow = 1};

void verb(int level, const char *format, ...)
{
	va_list args;

	if (level > opt.o_verbose)
		return;
	va_start(args, format);
	vfprintf(stderr, format, args);
	va_end(args);
}

void usage(char *name)
{
	if (bintype == DAEMON)
		fprintf(stderr,
	"The Lustre HSM deamon acts on action requests from Lustre "
	"to copy files to and from an HSM archive system. This "
	"Posix-flavored daemon makes regular Posix filesystem calls "
	"to an HSM mounted at a given hsm_root.\n"
	" Usage:\n"
	"   %s [options] <lustre_fs_name>\n"
	" Options:\n"
	"   --commcheck   : Verify we can open the communication "
	"channel, then quit\n"
	"   --noattr      : Don't copy file attributes\n"
	"   --noshadow    : Don't create shadow namespace in archive\n"
	"   --noxattr     : Don't copy file extended attributes\n",
			name);
	else
		fprintf(stderr,
	"The Lustre HSM tool performs administrator-type actions "
	"on a Lustre HSM archive.\n"
	"This Posix-flavored tool can link an "
	"existing HSM namespace into a Lustre filesystem.\n"
	" Usage:\n"
	"   %s --import <src> <dst>\n"
	"      import an archived subtree at\n"
	"	<src> (relative to hsm_root) into the Lustre filesystem at\n"
	"	<dst> (absolute)\n",
			name);
	fprintf(stderr,
	"   --abort       : Abort operation on major error\n"
	"   --archive <#> : Archive number (repeatable)\n"
	"   --noexecute   : Don't execute, just show what would be done\n"
	"   --hsm_root <path> : Target HSM mount point\n"
	"   --quiet       : Produce less verbose output\n"
	"   --verbose     : Produce more verbose output\n"
	"   --bandwidth <#> : Bandwidth limit in MB/s\n"
		);

	exit(1);
}

int ct_parseopts(int argc, char **argv)
{
	struct option long_opts[] = {
		{"archive",   required_argument, 0,	       'A'},
		{"commcheck", no_argument,       &opt.o_check,     1},
		{"err-abort", no_argument,       &opt.o_err_abort, 1},
		{"help",      no_argument,       0,	       'h'},
		{"hsm_root",  required_argument, 0,	       'p'},
		{"import",    required_argument, 0,	       'i'},
		{"noexecute", no_argument,       &opt.o_dryrun,    1},
		{"noattr",    no_argument,       &opt.o_attr,      0},
		{"noshadow",  no_argument,       &opt.o_shadow,    0},
		{"noxattr",   no_argument,       &opt.o_xattr,     0},
		{"quiet",     no_argument,       0,	       'q'},
		{"verbose",   no_argument,       0,	       'v'},
		{"bandwidth", required_argument, 0,	       'b'},
		{0, 0, 0, 0}
	};
	int c;

	optind = 0;
	while ((c = getopt_long(argc, argv, "A:hi:p:qvb:u",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'A':
			if ((opt.o_archive_cnt >= MAX_ARCHIVE_CNT) ||
			    (atoi(optarg) >= MAX_ARCHIVE_CNT)) {
				verb(V_ERR, "error: %s: archive "
				     "number must be less than %d\n",
				     argv[0], MAX_ARCHIVE_CNT);
				return E2BIG;
			}
			opt.o_archive_id[opt.o_archive_cnt] = atoi(optarg);
			opt.o_archive_cnt++;
			break;
		case 'h':
			usage(argv[0]);
		case 'i':
			opt.o_src = optarg;
			if (optind >= argc) {
				verb(V_ERR, "error: --import requires 2 "
				     "arguments\n");
				usage(argv[0]);
			}
			opt.o_dst = argv[optind++];
			opt.o_fsname = opt.o_dst;
			break;
		case 'p':
			opt.o_hsmroot = optarg;
			break;
		case 'q':
			opt.o_verbose--;
			break;
		case 'v':
			opt.o_verbose++;
			break;
		case 'b':
			opt.o_bw = atoi(optarg) * 1000 * 1000;
			break;
		case 0:
			break;
		default:
			verb(V_ERR, "error: %s: option '%s' unrecognized\n",
			     argv[0], argv[optind - 1]);
			usage(argv[0]);
			return -EINVAL;
		}
	}
	/* set llapi message level */
	switch (opt.o_verbose) {
	case V_ERR:
		llapi_msg_set_level(LLAPI_MSG_ERROR);
		break;
	case V_WARN:
		llapi_msg_set_level(LLAPI_MSG_WARN);
		break;
	case V_STATUS:
		llapi_msg_set_level(LLAPI_MSG_NORMAL);
		break;
	case V_TRACE:
		llapi_msg_set_level(LLAPI_MSG_DEBUG);
		break;
	default:
		if (opt.o_verbose < V_ERR)
			llapi_msg_set_level(LLAPI_MSG_FATAL);
		else /* verb > TRACE */
			llapi_msg_set_level(LLAPI_MSG_DEBUG);
	}

	if (optind == argc - 1)  /* last argument is fsname */
		opt.o_fsname = argv[optind];

	if (opt.o_fsname == 0)
		usage(argv[0]);

	if (!opt.o_dryrun && !opt.o_hsmroot) {
		verb(V_WARN,
		     "No --hsm_root specified, assuming --noexecute.\n");
		opt.o_dryrun++;
	}

	if (opt.o_src && opt.o_src[0] == '/') {
		verb(V_ERR, "error: <src> path '%s' must be relative "
		     "(to --hsm_root).\n", opt.o_src);
		return -EINVAL;
	}

	if (opt.o_dst && opt.o_dst[0] != '/') {
		verb(V_ERR, "error: <dst> path '%s' must be absolute.\n",
		     opt.o_dst);
		return -EINVAL;
	}

	return 0;
}

/* mkdir -p path */
int ct_mkdir_p(char *path)
{
	char *ptr = path;
	int rc;

	while (*ptr == '/')
		ptr++;

	while ((ptr = strchr(ptr, '/')) != NULL) {
		*ptr = '\0';
		rc = mkdir(path, DIR_PERM);
		*ptr = '/';
		if (rc < 0 && errno != EEXIST) {
			verb(V_ERR, "mkdir %s (%s)\n", path, strerror(errno));
			return -errno;
		}
		ptr++;
	}
	return 0;
}

static int ct_save_stripe(char *src, char *dst)
{
	char			 lov_file[PATH_MAX+1];
	int			 rc;
	struct lov_user_md	*lum;
	int			 lum_size, fd;

	sprintf(lov_file, "%s.lov", dst);
	verb(V_TRACE, "Saving stripe info of %s in %s\n", src, lov_file);
	lum_size = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
	lum = (struct lov_user_md *)malloc(lum_size);
	if (lum == NULL)
		return -ENOMEM;

	rc = llapi_file_get_stripe(src, lum);
	if (rc < 0) {
		verb(V_ERR, "Cannot get stripe info on %s (%d)\n", src, rc);
		goto out;

	}
	fd = open(lov_file, O_TRUNC | O_CREAT | O_WRONLY, 0700);
	if (fd < 0) {
		verb(V_ERR, "Cannot open %s (%s)\n", lov_file, strerror(errno));
		rc = -errno;
		goto out;
	}
	rc = write(fd, lum, lum_size);
	if (rc < 0) {
		verb(V_ERR, "Cannot write %d bytes to %s (%s)\n", lum_size,
		     lov_file, strerror(errno));
		close(fd);
		rc = -errno;
		goto out;
	}
	rc = close(fd);
	if (rc < 0) {
		verb(V_ERR, "Cannot close %s (%s)\n", lov_file,
		     strerror(errno));
		rc = -errno;
		goto out;
	}
	rc = 0;
out:
	free(lum);
	return rc;
}

static int get_lov_rules(char *src, char *parent,
			 struct lov_user_md_v3 *lum, int lum_size)
{
	char	 lov_file[PATH_MAX+1];
	int	 fd, rc;

	sprintf(lov_file, "%s.lov", src);
	verb(V_TRACE, "Reading stripe rules from %s for %s\n", lov_file, src);

	fd = open(lov_file, O_RDONLY);
	if (fd < 0) {
		verb(V_ERR, "Cannot open %s (%s)\n", lov_file, strerror(errno));
		goto use_default;
	}
	rc = read(fd, lum, lum_size);
	if (rc < 0) {
		verb(V_ERR, "Cannot read %d bytes to %s (%s)\n", lum_size,
		     lov_file, strerror(errno));
		close(fd);
		goto use_default;
	}
	close(fd);
	return 0;

use_default:
	rc = llapi_file_get_stripe(parent, (struct lov_user_md *)lum);
	if (rc == -ENODATA) {
		/* no stripe, use the default */
		lum->lmm_magic = LOV_USER_MAGIC;
		lum->lmm_pattern = LOV_PATTERN_RAID0;
		lum->lmm_stripe_size = 0;
		lum->lmm_stripe_count = 0;
		lum->lmm_stripe_offset = -1;
		return 0;
	}
	if (rc < 0) {
		verb(V_ERR, "Cannot get stripe info on %s for %s (%d)\n",
		     parent, src, rc);
		return rc;
	}
	return 0;
}

static int ct_restore_stripe(char *src, char *dst, char *parent)
{
	int			 rc;
	struct lov_user_md_v3	*lum;
	int			 lum_size, fd;
	char			*pool;

	lum_size = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
	lum = (struct lov_user_md_v3 *)malloc(lum_size);
	if (lum == NULL)
		return -ENOMEM;

	rc = get_lov_rules(src, parent, lum, lum_size);
	if (rc < 0) {
		verb(V_ERR, "Cannot get stripe rules for %s\n", src);
		goto out;
	}

	if (lum->lmm_magic == LOV_USER_MAGIC_V3) {
		char fsname[MAX_OBD_NAME + 1];

		pool = lum->lmm_pool_name;
		llapi_search_fsname(dst, fsname);
		rc = llapi_search_ost(fsname, pool, NULL);
		if (rc < 0) {
			verb(V_ERR, "Pool %s does not exist, so we ignore it\n",
			     pool);
			lum->lmm_magic = LOV_USER_MAGIC_V1;
			pool = NULL;
		}
	} else {
		pool = NULL;
	}
	fd = llapi_file_open_pool(dst, O_WRONLY, 0700, lum->lmm_stripe_size,
				  0, lum->lmm_stripe_count,
				  lum->lmm_pattern, pool);
	if (fd < 0)
		verb(V_ERR, "Cannot set stripe on %s (%d)\n", dst, fd);
	else
		close(fd);
out:
	free(lum);
	return 0;
}

/* non-blocking read or write */
static int nonblock_rw(int wr, int fd, char *buf, int size)
{
	int rc;

	if (size == 0)
		return 0;

	if (wr)
		rc = write(fd, buf, size);
	else
		rc = read(fd, buf, size);

	if ((rc < 0) && (errno == -EAGAIN)) {
		fd_set set;
		struct timeval timeout;

		timeout.tv_sec = REPORT_INTERVAL;

		FD_ZERO(&set);
		FD_SET(fd, &set);
		if (wr)
			rc = select(FD_SETSIZE, NULL, &set, NULL, &timeout);
		else
			rc = select(FD_SETSIZE, &set, NULL, NULL, &timeout);
		if (rc < 0)
			return -errno;
		if (rc == 0)
			/* Timed out, we read nothing */
			return -EAGAIN;

		/* Should be available now */
		if (wr)
			rc = write(fd, buf, size);
		else
			rc = read(fd, buf, size);
	}
	if (rc == 0)
		/* EOF */
		return 0;
	if (rc < 0)
		return -errno;

	return rc;
}

/* \param rootpath for progress reporting */
int ct_copy_data(char *rootpath, char *src, char *dst,
		 struct hsm_action_item *hai, long hal_flags, size_t *file_size)
{
	struct hsm_progress	 hp;
	struct stat		 src_st = {}, dst_st = {};
	char			*buf;
	__u64			 wpos = 0, rpos = 0, rlen;
	time_t			 lastprint = time(0);
	time_t			 start, tempo;
	int			 rsize, wsize, bufoff = 0;
	int			 fd_src, fd_dst = -1;
	int			 rc = 0;
	int			 flags;

	verb(V_ERR, "going to copy data from %s to %s\n", src, dst);

	buf = malloc(CHUNKSIZE);
	if (!buf)
		return -ENOMEM;

	if (stat(src, &src_st) < 0) {
		verb(V_ERR, "stat %s failed (%s)\n", src, strerror(errno));
		return -errno;
	}
	if (file_size)
		*file_size = src_st.st_size;

	fd_src = open(src, O_RDONLY | O_NOATIME | O_NONBLOCK);
	if (fd_src < 0) {
		verb(V_ERR, "ropen %s failed (%s)\n", src, strerror(errno));
		rc = -errno;
		goto out;
	}
	rc = lseek(fd_src, hai->hai_extent.offset, SEEK_SET);
	if (rc < 0) {
		verb(V_ERR, "rseek %s to "LPU64" (len "LPU64" failed (%s)\n",
		     src, hai->hai_extent.offset, src_st.st_size,
		     strerror(errno));
		rc = -errno;
		goto out;
	}

	if (hai->hai_action == HSMA_RESTORE) {
		if (stat(dst, &dst_st) < 0) {
			verb(V_ERR, "stat %s failed (%s)\n", dst,
				    strerror(errno));
			return -errno;
		}
	}

	/* If extent is specified, don't truncate an old archived copy */
	flags = O_WRONLY;
	/* in ghost copy file mode, no need for grouplock, nor for
	 * non restore request */
	if ((hai->hai_action == HSMA_RESTORE) && !(hal_flags & HSM_GHOST_COPY))
		flags |= O_NONBLOCK;

	if (hai->hai_action == HSMA_ARCHIVE)
		flags |= ((hai->hai_extent.length == -1) ? O_TRUNC : 0) |
			 O_CREAT;
	fd_dst = open(dst, flags, FILE_PERM);
	if (fd_dst < 0) {
		verb(V_ERR, "wopen %s failed (%s)\n", dst, strerror(errno));
		rc = -errno;
		goto out;
	}

	rc = lseek(fd_dst, hai->hai_extent.offset, SEEK_SET);
	if (rc < 0) {
		verb(V_ERR, "wseek %s to "LPU64" failed (%s)\n", src,
		     hai->hai_extent.offset, strerror(errno));
		rc = -errno;
		goto out;
	}

	/* cookie is used to reference the file we are working on (ie fid)
	 * progress is made on data fid */
	hp.hp_cookie = hai->hai_cookie;
	hp.hp_fid = hai->hai_dfid;
	hp.hp_extent.offset = hai->hai_extent.offset;
	hp.hp_extent.length = 0;
	hp.hp_errval = 0;
	hp.hp_flags = 0;  /* Not done yet */
	rc = llapi_hsm_progress(rootpath, &hp);
	if (rc) {
		/* Action has been canceled or something wrong
		 * is happening. Stop copying data. */
		verb(V_ERR, "Progress returned err %d\n", rc);
		goto out;
	}

	errno = 0;
	/* Don't read beyond a given extent */
	rlen = (hai->hai_extent.length == -1LL) ?
		src_st.st_size : hai->hai_extent.length;
	start = time(0);
	while (wpos < rlen) {
		int chunk = (rlen - wpos > CHUNKSIZE) ? CHUNKSIZE : rlen - wpos;

		/* Only read more if we wrote everything in the buffer */
		if (wpos == rpos) {
			rsize = nonblock_rw(0, fd_src, buf, chunk);
			if (rsize == 0)
				/* EOF */
				break;
			if (rsize == -EAGAIN) {
				/* Timed out */
				rsize = 0;
				if (rpos == 0) {
					/* Haven't read anything yet, let's
					   give it back to the coordinator
					   for rescheduling */
					rc = -EAGAIN;
					break;
				}
			}
			if (rsize < 0) {
				verb(V_ERR, "read %s failed (%s)\n", src,
				     strerror(-rsize));
				rc = rsize;
				break;
			}
			rpos += rsize;
			bufoff = 0;
		}

		wsize = nonblock_rw(1, fd_dst, buf + bufoff, rpos - wpos);
		if (wsize == -EAGAIN)
			/* Timed out */
			wsize = 0;
		if (wsize < 0) {
			verb(V_ERR, "write %s failed (%s)\n", src,
			     strerror(-wsize));
			rc = wsize;
			break;
		}
		wpos += wsize;
		bufoff += wsize;

		if (opt.o_bw != 0) {
			tempo = (wpos - hai->hai_extent.offset);
			tempo -= ((time(0) - start) * opt.o_bw);
			tempo = tempo / opt.o_bw;
			if (tempo > 0)
				sleep(tempo);
		}

		if (time(0) >= lastprint + REPORT_INTERVAL) {
			lastprint = time(0);
			verb(V_TRACE, "%%%d ", 100 * wpos / rlen);
			hp.hp_extent.length = wpos;
			rc = llapi_hsm_progress(rootpath, &hp);
			if (rc) {
				/* Action has been canceled or something wrong
				 * is happening. Stop copying data. */
				verb(V_ERR, "Progress returned err %d\n", rc);
				goto out;
			}
		}

		rc = 0;
	}
	verb(V_TRACE, "\n");

	hai->hai_extent.length = wpos;

out:
	/*
	 * make sure the file is on disk before reporting success.
	 */
	if ((rc == 0) && (fd_dst >= 0)) {
		if (fsync(fd_dst)) {
			verb(V_ERR, "fsync %s failed (%s)\n",
			     dst, strerror(errno));
			rc = -errno;
		}
	}
	/*
	 * truncate and put group lock on restore
	 */
	if ((hai->hai_action == HSMA_RESTORE) && (fd_dst >= 0)) {
		/* size is taken from the archive this is done to support
		 * restore after a force release which lives the file with the
		 * wrong size (can big bigger than the new size)
		 */
		if (src_st.st_size < dst_st.st_size) {
			if (ftruncate(fd_dst, src_st.st_size) == -1) {
				rc = -errno;
				verb(V_ERR, "final truncate of %s to %lu "
					    "failed (%s)\n",
				dst, src_st.st_size, strerror(-rc));
				opt.err_major++;
			}
		}
	}

	free(buf);
	if (fd_src >= 0)
		close(fd_src);
	if (fd_dst >= 0)
		close(fd_dst);

	return rc;
}

/* Copy file attributes from file src to file dest */
int ct_copy_attr(char *src, char *dst)
{
	struct stat	st;
	struct utimbuf	time;

	if (stat(src, &st) < 0 ||
	    chmod(dst, st.st_mode) < 0 ||
	    chown(dst, st.st_uid, st.st_gid) < 0) {
		verb(V_ERR, "stat %s or chmod/chown %s failed (%s)\n",
		     src, dst, strerror(errno));
		return -errno;
	}

	time.actime = st.st_atime;
	time.modtime = st.st_mtime;
	if (utime(dst, &time) == -1)
		return -errno;
	return 0;
}

int ct_copy_xattr(char *src, char *dst, bool restore)
{
	char	names[1024], *ptr;
	char	buf[4*1024*1024];  /* 4M, arbitrary */
	ssize_t	namesize;
	int	rc;

	namesize = llistxattr(src, names, sizeof(names));
	if (namesize < 0)
		return -errno;

	ptr = names;
	errno = 0;
	while (ptr < names + namesize) {
		rc = lgetxattr(src, ptr, buf, sizeof(buf));
		if (rc < 0)
			return -errno;

		/* when we restore, we do not restore lustre
		 * xattr (some failed, some crash servers ...)
		 */
		if ((restore == false) ||
		    (strncmp(XATTR_TRUSTED_PREFIX, ptr,
			     sizeof(XATTR_TRUSTED_PREFIX) - 1) != 0)) {
			rc = lsetxattr(dst, ptr, buf, rc, 0);
			verb(V_TRACE, "lsetxattr on %s: %s rc=%d errno=%d\n",
			     dst, ptr, rc, errno);
			/* lustre.* attrs aren't supported on other fs's */
			if (rc < 0 && errno != EOPNOTSUPP) {
				verb(V_ERR, "lsetxattr on %s %s failed (%s)\n",
				     dst, ptr, strerror(errno));
				return -errno;
			}
		}
		ptr += strlen(ptr) + 1;
	}

	return 0;
}

static int ct_path_lustre(char *buf, char *rootpath, lustre_fid *fid)
{
	return sprintf(buf, "%s/%s/fid/"DFID_NOBRACE, rootpath, dot_lustre_name,
		       PFID(fid));
}

static int ct_path_target(char *buf, char *rootpath, lustre_fid *fid)
{
	return sprintf(buf, "%s/%04x/%04x/%04x/%04x/%04x/%04x/"DFID_NOBRACE,
		       rootpath,
		       (int)((fid)->f_seq >> 48 & 0xFFFF),
		       (int)((fid)->f_seq >> 32 & 0xFFFF),
		       (int)((fid)->f_seq >> 16 & 0xFFFF),
		       (int)((fid)->f_seq       & 0xFFFF),
			     (fid)->f_oid >> 16 & 0xFFFF,
			     (fid)->f_oid       & 0xFFFF,
		       PFID(fid));
}

int ct_retryable(int err)
{
	int rc;

	rc = (err == -ETIMEDOUT);
	return rc;
}

int ct_begin(char *rootpath, struct hsm_action_item *hai, struct hsm_copy *copy)
{
	char	 src[PATH_MAX];
	int	 rc;

	ct_path_lustre(src, rootpath, &hai->hai_fid);
	rc = llapi_hsm_copy_start(src, copy, hai);
	if (rc < 0) {
		verb(V_ERR, "copy start failed on %s (%s)\n", src,
		     strerror(-rc));
	}
	return rc;
}

int ct_fini(char *rootpath, struct hsm_copy *copy, struct hsm_action_item *hai,
	    int flags, int rc)
{
	struct hsm_progress	hp;
	char			src[PATH_MAX];

	verb(V_TRACE, "Action completed, notifying coordinator "
		      "cookie="LPX64", fid="DFID", flags=%d err=%d\n",
		      hai->hai_cookie, PFID(&hai->hai_fid),
		      flags, -rc);

	ct_path_lustre(src, rootpath, &hai->hai_fid);
	hp.hp_cookie = hai->hai_cookie;
	/* for data movement actions, data fid is used for progress */
	if ((hai->hai_action == HSMA_RESTORE) ||
	    (hai->hai_action == HSMA_ARCHIVE))
		hp.hp_fid = hai->hai_dfid;
	else
		hp.hp_fid = hai->hai_fid;
	/* hmm - should we even report minor errors? */
	hp.hp_errval = -rc;
	hp.hp_extent = hai->hai_extent;
	hp.hp_flags = flags;
	rc = llapi_hsm_copy_end(src, copy, &hp);
	if (rc == -ECANCELED)
		verb(V_ERR, "Completed action has been canceled: "
		     "cookie="LPX64", fid="DFID"\n", hai->hai_cookie,
		     PFID(&hai->hai_fid));
	else if (rc < 0)
		verb(V_ERR, "copy end failed on %s (%s)\n", src, strerror(-rc));
	else
		verb(V_TRACE, "copy end ok on %s (rc=%d)\n", src, rc);

	return rc;
}

int ct_archive(char *rootpath, struct hsm_action_item *hai, long hal_flags)
{
	struct hsm_copy	 copy;
	char		 src[PATH_MAX];
	char		 dst[PATH_MAX];
	int		 rc, rcf = 0;
	int		 rename_needed = 0;
	int		 flags = 0;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini_major;

	/* we fill archive so:
	 * source = data fid
	 * destination = lustre fid
	 */
	ct_path_lustre(src, rootpath, &hai->hai_dfid);
	ct_path_target(dst, opt.o_hsmroot, &hai->hai_fid);
	if (hai->hai_extent.length == -1) {
		/* whole file, write it to tmp location and atomically
		   replace old archived file */
		sprintf(dst, "%s_tmp", dst);
		/* we cannot rely on the same test because ct_copy_data()
		 * updates hai_extent.length */
		rename_needed = 1;
	}

	verb(V_STATUS, "archive file: %s to %s\n", src, dst);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini_major;
	}

	rc = ct_mkdir_p(dst);
	if (rc < 0) {
		verb(V_ERR, "mkdir_p %s failed (%s)\n", dst,
		     strerror(-rc));
		goto fini_major;
	}

	/* saving the file stripe is not critical */
	rc = ct_save_stripe(src, dst);
	if (rc < 0) {
		verb(V_ERR, "Cannot dave file striping info from "
			    "%s to %s (%s)\n",
		     src, dst, strerror(-rc));
	}

	rc = ct_copy_data(rootpath, src, dst, hai, hal_flags, NULL);
	if (rc < 0) {
		verb(V_ERR, "data copy failed from %s to %s (%s)\n",
		     src, dst, strerror(-rc));
		goto fini_major;
	}

	verb(V_TRACE, "data archive done: %s to %s\n", src, dst);

	/* attrs will remain on the MDS; no need to copy them, except possibly
	  for disaster recovery */
	if (opt.o_attr) {
		rc = ct_copy_attr(src, dst);
		if (rc < 0) {
			verb(V_ERR, "attr copy failed from %s to %s (%s)\n",
			     src, strerror(-rc));
			rcf = rc;
		}
		verb(V_TRACE, "attr file copied to archive: %s to %s\n",
		     src, dst);
	}

	/* xattrs will remain on the MDS; no need to copy them, except possibly
	 for disaster recovery */
	if (opt.o_xattr) {
		rc = ct_copy_xattr(src, dst, false);
		if (rc < 0) {
			verb(V_ERR, "xattr copy failed from %s to %s (%s)\n",
			     src, dst, strerror(-rc));
			rcf = rcf ? rcf : rc;
		}
		verb(V_TRACE, "xattr file copied to archive: %s to %s\n",
		     src, dst);
	}

	if (rename_needed == 1) {
		/* atomically replace old archived file */
		ct_path_target(src, opt.o_hsmroot, &hai->hai_fid);
		rc = rename(dst, src);
		if (rc < 0) {
			verb(V_ERR, "rename %s to %s failed (%s)\n", dst, src,
			     strerror(errno));
			rc = -errno;
			goto fini_major;
		}
		/* rename lov file */
		strcat(src, ".lov");
		strcat(dst, ".lov");
		rc = rename(dst, src);
		if (rc < 0)
			verb(V_ERR, "rename %s to %s failed (%s)\n", dst, src,
			     strerror(errno));
	}

	if (opt.o_shadow) {
		/* Create a namespace of softlinks that shadows the original
		   Lustre namespace.  This will only be current at
		   time-of-archive (won't follow renames).
		   WARNING: release won't kill these links; a manual
		   cleanup of dead links would be required.
		 */
		char		 buf[PATH_MAX];
		long long	 recno = -1;
		int		 linkno = 0;
		char		*ptr;
		int		 depth = 0;
		int		 sz;

		sprintf(buf, DFID, PFID(&hai->hai_fid));
		sprintf(src, "%s/shadow/", opt.o_hsmroot);

		ptr = opt.o_hsmroot;
		while (*ptr)
			(*ptr++ == '/') ? depth-- : 0;

		rc = llapi_fid2path(rootpath, buf, src + strlen(src),
				    sizeof(src) - strlen(src), &recno, &linkno);
		if (rc < 0) {
			verb(V_ERR, "fid2path %s failed (%d)\n", buf, rc);
			rcf = rcf ? rcf : rc;
			goto fini_minor;
		}

		/* Figure out how many parent dirs to symlink back */
		ptr = src;
		while (*ptr)
			(*ptr++ == '/') ? depth++ : 0;
		sprintf(buf, "..");
		while (--depth > 1)
			strcat(buf, "/..");

		ct_path_target(dst, buf, &hai->hai_fid);

		if (ct_mkdir_p(src)) {
			verb(V_ERR, "mkdir_p %s failed (%s)\n", src,
			     strerror(errno));
			rcf = rcf ? rcf : -errno;
			goto fini_major;
		}
		/* symlink already exists ? */
		sz = readlink(src, buf, sizeof(buf));
		if (sz >= 0) {
			buf[sz] = '\0';
			if (sz == 0 || strncmp(buf, dst, sz) != 0) {
				if (unlink(src) && errno != ENOENT) {
					verb(V_ERR,
					     "unlink symlink %s failed (%s)\n",
					     src, strerror(errno));
					rcf = rcf ? rcf : -errno;
					goto fini_major;
				/* unlink old symlink done */
				verb(V_TRACE,
				     "remove old symlink %s pointing to %s\n",
				     src, buf);
				}
			} else {
				/* symlink already ok */
				verb(V_TRACE,
				     "symlink %s already pointing to %s\n",
				     src, dst);
				rcf = 0;
				goto fini_minor;
			}
		}
		if (symlink(dst, src)) {
			verb(V_ERR, "symlink %s to %s failed (%s)\n",  src, dst,
			     strerror(errno));
			rcf = rcf ? rcf : -errno;
			goto fini_major;
		}
		verb(V_TRACE, "symlink %s to %s done\n", src, dst);
	}
fini_minor:
	if (rcf)
		opt.err_minor++;

	rc = ct_fini(rootpath, &copy, hai, flags, rcf);
	return rc;

fini_major:
	opt.err_major++;
	unlink((const char *)dst);
	if (ct_retryable(rc))
		flags |= HP_FLAG_RETRY;

	rc = ct_fini(rootpath, &copy, hai, flags, rc);

	return rc;
}

int ct_restore(char *rootpath, struct hsm_action_item *hai, long hal_flags)
{
	struct hsm_copy	 copy;
	char		 src[PATH_MAX];
	char		 dst[PATH_MAX];
	char		 released[PATH_MAX], parent[PATH_MAX];
	char		*ptr;
	int		 rc;
	size_t		 file_size;
	long long	 recno;
	int		 linkno;
	char		 fid[FID_NOBRACE_LEN + 1];
	int		 flags = 0;
	int		 fdv = -1;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini;

	/* we fill lustre so:
	 * source = lustre fid in the backend
	 * destination = data fid = volatile file
	 */

	/* find the parent directory of the file to be restored:
	 * - find file full path name from fid
	 * - extract parent directory name
	 */
	sprintf(fid, DFID_NOBRACE, PFID(&hai->hai_fid));
	rc = llapi_fid2path(rootpath, fid, released,
			    sizeof(released), &recno, &linkno);
	if (rc != 0) {
		verb(V_ERR, "cannot get path for fid "DFID"(%d)\n",
		     PFID(&hai->hai_fid), rc);
		goto fini;
	}
	/* fid2path returns a relative path */
	sprintf(parent, "%s/%s", rootpath, released);
	/* remove file name */
	ptr = strrchr(parent, '/');
	if ((ptr == NULL) || (ptr == parent))
		strcpy(parent, "/");
	else
		*ptr = '\0';

	/* create a volatile file in lustre */
	fdv = llapi_create_volatile_idx(parent, 0, O_LOV_DELAY_CREATE);
	if (fdv < 0) {
		verb(V_ERR, "cannot create volatile file in %s (%d)\n",
		     parent, fdv);
		goto fini;
	}
	/* get the fid of the volatile file, set it as the data fid */
	rc = llapi_fd2fid(fdv, &hai->hai_dfid);
	if (rc < 0) {
		verb(V_ERR, "cannot get fid of volatile file in %d (%d)\n",
		     parent, rc);
		goto fini;
	}

	/* build the 2 path names from the 2 fids */
	ct_path_target(src, opt.o_hsmroot, &hai->hai_fid);
	ct_path_lustre(dst, rootpath, &hai->hai_dfid);

	verb(V_STATUS, "restore file %s to %s parent is %s\n",
	     src, dst, parent);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini;
	}

	/* the layout cannot be allocated through .fid so
	 * we have to restore a layout */
	rc = ct_restore_stripe(src, dst, parent);
	if (rc < 0) {
		verb(V_ERR, "cannot restore file striping info from "
			    "%s to %s (%s)\n",
		     src, dst, strerror(-rc));
		opt.err_major++;
		goto fini;
	}

	rc = ct_copy_data(rootpath, src, dst, hai, hal_flags, &file_size);
	if (rc < 0) {
		verb(V_ERR, "data copy failed from %s to %s (%s)\n", src, dst,
		     strerror(-rc));
		opt.err_major++;
		if (ct_retryable(rc))
			flags |= HP_FLAG_RETRY;
		goto fini;
	}

	verb(V_TRACE, "data restore done from %s to %s\n", src, dst);

	if (opt.o_xattr) {
		rc = ct_copy_xattr(src, dst, true);
		if (rc < 0) {
			verb(V_ERR, "xattr copy failed on %s (%s)\n", opt.o_src,
			     strerror(-rc));
			opt.err_minor++;
			goto fini;
		}
		verb(V_TRACE, "xattr restore done from %s to %s\n", src, dst);
	}

fini:
	rc = ct_fini(rootpath, &copy, hai, flags, rc);
	/* object swaping is done by cdt at copy end, so close of volatile file
	 * cannot be done before */
	if (fdv >= 0)
		close(fdv);

	return rc;
}

int ct_remove(char *rootpath, struct hsm_action_item *hai, long hal_flags)
{
	struct hsm_copy		copy;
	char			dst[PATH_MAX];
	int			rc;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini;

	ct_path_target(dst, opt.o_hsmroot, &hai->hai_fid);

	verb(V_STATUS, "remove file %s\n", dst);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini;
	}

	rc = unlink((const char *)dst);
	if (rc < 0) {
		rc = -errno;
		verb(V_ERR, "unlink %s failed (%s)\n", dst, strerror(-rc));
		opt.err_minor++;
		goto fini;
	}

fini:
	rc = ct_fini(rootpath, &copy, hai, 0, rc);
	return rc;
}

int ct_process_item(char *rootpath, struct hsm_action_item *hai, long hal_flags)
{
	int			rc = 0;

	if (opt.o_verbose >= V_TRACE || opt.o_dryrun) {
		/* Print the original path */
		char fid[128];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		sprintf(fid, DFID, PFID(&hai->hai_fid));
		verb(V_TRACE, "Item %s action %d reclen %d\n",
		     fid, hai->hai_action, hai->hai_len);
		verb(V_TRACE, " gid="LPX64" cookie="LPX64"\n",
		     hai->hai_gid, hai->hai_cookie);
		rc = llapi_fid2path(rootpath, fid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			verb(V_ERR, "fid2path %s failed (%d)\n", fid, rc);
		else
			verb(V_TRACE, "process file %s\n", path);
	}

	switch (hai->hai_action) {
	/* set opt.err_major, minor inside these functions */
	case HSMA_ARCHIVE:
		rc = ct_archive(rootpath, hai, hal_flags);
		break;
	case HSMA_RESTORE:
		rc = ct_restore(rootpath, hai, hal_flags);
		break;
	case HSMA_REMOVE:
		rc = ct_remove(rootpath, hai, hal_flags);
		break;
	case HSMA_CANCEL:
		verb(V_TRACE, "Cancel not implemented\n");
		/* Don't report progress to coordinator for this cookie:
		 * the copy function will get ECANCELED when reporting
		 * progress. */
		opt.err_minor++;
		return 0;
		break;
	default:
		verb(V_ERR, "unknown action %d\n", hai->hai_action);
		opt.err_minor++;
		ct_fini(rootpath, NULL, hai, 0, -EINVAL);
	}

	return 0;
}

struct ct_th_data {
	long hal_flags;
	char rootpath[PATH_MAX];
	struct hsm_action_item *hai;
};

static void *ct_thread(void *data)
{
	struct ct_th_data *cttd = (struct ct_th_data *)data;
	int rc;

	rc = ct_process_item(cttd->rootpath, cttd->hai, cttd->hal_flags);

	free(cttd->hai);
	free(cttd);
	pthread_exit((void *)(long)rc);
}

int ct_process_item_async(char *rootpath, struct hsm_action_item *hai,
			  long hal_flags)
{
	pthread_attr_t		 attr;
	pthread_t		 thread;
	struct ct_th_data	*data;
	int			 rc;

	data = malloc(sizeof(struct ct_th_data));
	if (!data)
		return -ENOMEM;
	data->hai = malloc(hai->hai_len);
	if (!data->hai) {
		free(data);
		return -ENOMEM;
	}
	memcpy(data->hai, hai, hai->hai_len);
	strcpy(data->rootpath, rootpath);
	data->hal_flags = hal_flags;

	rc = pthread_attr_init(&attr);
	if (rc) {
		verb(V_ERR, "pthread_attr_init: %s\n", strerror(rc));
		free(data->hai);
		free(data);
		return -rc;
	}
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	rc = pthread_create(&thread, &attr, ct_thread, data);
	if (rc)
		verb(V_ERR, "thread create: (%d) %s\n", rc, strerror(rc));

	pthread_attr_destroy(&attr);
	return 0;
}

static int ct_import_one(char *src, char *dst)
{
	char		newarc[PATH_MAX];
	lustre_fid	fid;
	struct stat	st;
	int		rc;

	verb(V_STATUS, "importing src %s to %s\n", src, dst);

	if (stat(src, &st) < 0) {
		verb(V_ERR, "stat %s failed (%s)\n", src, strerror(errno));
		return -errno;
	}

	if (opt.o_dryrun)
		return 0;

	rc = llapi_hsm_import(dst,
			      opt.o_archive_cnt ? opt.o_archive_id[0] : 0,
			      &st, 0, 0, 0, 0, NULL, &fid);
	if (rc < 0) {
		verb(V_ERR, "import %s to %s failed (%s)\n", src, dst,
		     strerror(-rc));
		return -rc;
	}

	ct_path_target(newarc, opt.o_hsmroot, &fid);

	rc = ct_mkdir_p(newarc);
	if (rc < 0) {
		verb(V_ERR, "mkdir_p %s failed (%s)\n", newarc, strerror(-rc));
		opt.err_major++;
		return rc;

	}

	/* Lots of choices now: mv, ln, ln -s ? */
	rc = link(src, newarc); /* hardlink */
	if (rc < 0) {
		verb(V_ERR, "link %s to %s failed (%s)\n", newarc, src,
		     strerror(errno));
		opt.err_major++;
		return -errno;
	}
	verb(V_TRACE, "imported %s at %s\n", src, newarc);

	return 0;
}

static char *path_concat(char *dirname, char *basename)
{
	char	*result;
	int	 dirlen = strlen(dirname);

	result = malloc(dirlen + strlen(basename) + 2);
	if (result == NULL)
		return NULL;

	memcpy(result, dirname, dirlen);
	result[dirlen] = '/';
	strcpy(result + dirlen + 1, basename);

	return result;
}

int ct_import_recurse(char *relpath)
{
	DIR		*dir;
	struct dirent	 ent, *cookie = NULL;
	char		*srcpath, *newpath;
	int		 rc;

	if (relpath == NULL)
		return -EINVAL;

	srcpath = path_concat(opt.o_hsmroot, relpath);
	if (srcpath == NULL) {
		opt.err_major++;
		return -ENOMEM;
	}

	dir = opendir(srcpath);
	if (dir == NULL) {
		/* Not a dir, or error */
		if (errno == ENOTDIR) {
			/* Single regular file case, treat o_dst as absolute
			   final location. */
			rc = ct_import_one(srcpath, opt.o_dst);
		} else {
			verb(V_ERR, "opendir %s failed (%s)\n", srcpath,
			     strerror(errno));
			opt.err_major++;
			rc = -errno;
		}
		free(srcpath);
		return rc;
	}
	free(srcpath);

	while (1) {
		rc = readdir_r(dir, &ent, &cookie);
		if (rc != 0) {
			verb(V_ERR, "readdir_r %s failed (%s)\n", relpath,
			     strerror(errno));
			opt.err_major++;
			rc = -errno;
			goto out;
		} else if ((rc == 0) && (cookie == NULL)) {
			/* end of directory */
			break;
		}

		if (!strcmp(ent.d_name, ".") ||
		    !strcmp(ent.d_name, ".."))
			continue;

		/* New relative path */
		newpath = path_concat(relpath, ent.d_name);
		if (newpath == NULL) {
			opt.err_major++;
			rc = -ENOMEM;
			goto out;
		}

		if (ent.d_type == DT_DIR) {
			rc = ct_import_recurse(newpath);
		} else {
			char src[PATH_MAX];
			char dst[PATH_MAX];
			sprintf(src, "%s/%s", opt.o_hsmroot, newpath);
			sprintf(dst, "%s/%s", opt.o_dst, newpath);
			/* Make the target dir in the Lustre fs */
			rc = ct_mkdir_p(dst);
			if (rc == 0) {
				/* Import the file */
				rc = ct_import_one(src, dst);
			} else {
				verb(V_ERR,
				     "ct_mkdir_p %s failed (%s)\n", dst,
				     strerror(-rc));
				opt.err_major++;
			}
		}

		if (rc) {
			verb(V_ERR, "importing %s failed\n", newpath);
			if (opt.err_major && opt.o_err_abort) {
				free(newpath);
				goto out;
			}
		}
		free(newpath);
	}

	rc = 0;
out:
	closedir(dir);
	return rc;
}

struct hsm_copytool_private *ctdata;

void handler(int signal)
{
	psignal(signal, "exiting");
	/* If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier. */
	llapi_hsm_copytool_fini(&ctdata);
	exit(1);
}

/* Daemon waits for messages from the kernel; run it in the background. */
int ct_daemon()
{
	char	rootpath[PATH_MAX];
	int	rc;

	rc = llapi_hsm_copytool_start(&ctdata, opt.o_fsname, 0,
				      opt.o_archive_cnt, opt.o_archive_id);
	if (rc < 0) {
		verb(V_ERR, "Can't start copytool interface: %s\n",
			strerror(-rc));
		return rc;
	}

	if (opt.o_check)
		return llapi_hsm_copytool_fini(&ctdata);

	signal(SIGINT, handler);

	while (1) {
		struct hsm_action_list *hal;
		struct hsm_action_item *hai;
		int msgsize, i = 0;

		verb(V_STATUS, "Waiting for message from kernel\n");

		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			verb(V_STATUS, "shutting down");
			break;
		} else if (rc == -EAGAIN) {
			continue; /* msg not for us */
		} else if (rc < 0) {
			verb(V_WARN, "Message receive: %s\n", strerror(-rc));
			opt.err_major++;
			if (opt.o_err_abort)
				break;
			else
				continue;
		}

		verb(V_TRACE, "Copytool fs=%s archive#=%d item_count=%d\n",
		       hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

		rc = llapi_search_rootpath(rootpath, hal->hal_fsname);
		if (rc < 0) {
			verb(V_ERR, "Can't find root path for fs=%s\n",
			       hal->hal_fsname);
			opt.err_major++;
			if (opt.o_err_abort)
				break;
			else
				continue;
		}

		hai = hai_zero(hal);
		while (++i <= hal->hal_count) {
			if ((char *)hai - (char *)hal > msgsize) {
				verb(V_ERR, "Item %d past end of message!\n",
				     i);
				opt.err_major++;
				rc = -EPROTO;
				break;
			}
			rc = ct_process_item_async(rootpath, hai,
						   hal->hal_flags);
			if (rc < 0)
				verb(V_ERR, "Item %d process err: %s\n", i,
				     strerror(-rc));
			if (opt.o_err_abort && opt.err_major)
				break;
			hai = hai_next(hai);
		}

		llapi_hsm_copytool_free(&hal);

		if (opt.o_err_abort && opt.err_major)
			break;
	}

	llapi_hsm_copytool_fini(&ctdata);

	return rc;
}

int main(int argc, char **argv)
{
	int	rc;

	if (strstr(argv[0], "hsmd"))
		bintype = DAEMON;
	else
		bintype = TOOL;

	rc = ct_parseopts(argc, argv);
	if (rc)
		return -rc;

	if (opt.o_src)
		rc = ct_import_recurse(opt.o_src);
	else
		rc = ct_daemon();

	verb(V_STATUS, "%s finished, errs: %d major, %d minor, rc=%d\n",
	     argv[0], opt.err_major, opt.err_minor, rc);

	return -rc;
}

