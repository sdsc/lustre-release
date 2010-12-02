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
 * http://www.gnu.org/licenses/gpl-2.0.htm
 *
 * GPL HEADER END
 */
/*
 * (C) Copyright 2012 Commissariat a l'energie atomique et aux energies
 *     alternatives
 *
 */
/* HSM copytool program for POSIX filesystem-based HSM's.
 *
 * An HSM copytool daemon acts on action requests from Lustre to copy files
 * to and from an HSM archive system. This one in particular makes regular
 * POSIX filesystem calls to a given path, where an HSM is presumably mounted.
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

/* Progress reporting period */
#define REPORT_INTERVAL 30
/* HSM hash subdir permissions */
#define DIR_PERM S_IRWXU
/* HSM hash file permissions */
#define FILE_PERM (S_IRUSR | S_IWUSR)

#define ONE_MB 0x100000

/* copytool uses a 32b bitmask field to register with kuc
 * archive num = 0 => all
 * archive num from 1 to 32
 */
#define MAX_ARCHIVE_CNT (sizeof(__u32) * 8)

enum ct_action {
	CA_IMPORT = 1,
	CA_REBIND,
	CA_MAXSEQ
};

struct options {
	int			 err_major;
	int			 err_minor;
	int			 o_attr;
	int			 o_check;
	int			 o_dryrun;
	int			 o_err_abort;
	int			 o_shadow;
	int			 o_verbose;
	int			 o_xattr;
	int			 o_archive_cnt;
	int			 o_archive_id[MAX_ARCHIVE_CNT];
	unsigned long long	 o_bw;
	size_t			 o_chunk_size;
	enum ct_action		 o_action;
	char			*o_fsname;
	char			*o_hsmroot;
	char			*o_src; /* for import, or rebind */
	char			*o_dst; /* for import, or rebind */
};

/* everything else is zeroed */
struct options opt = {
	.o_attr = 1,
	.o_shadow = 1,
	.o_verbose = LLAPI_MSG_WARN,
	.o_xattr = 1,
	.o_chunk_size = ONE_MB,
};

static char cmd_name[PATH_MAX] = "";

#define CT_ERROR(format, ...) \
	llapi_printf(LLAPI_MSG_ERROR, "%s: "format, cmd_name, ## __VA_ARGS__)
#define CT_DEBUG(format, ...) \
	llapi_printf(LLAPI_MSG_DEBUG, "%s: "format, cmd_name, ## __VA_ARGS__)
#define CT_WARN(format, ...) \
	llapi_printf(LLAPI_MSG_WARN, "%s: "format, cmd_name, ## __VA_ARGS__)
#define CT_TRACE(format, ...) \
	llapi_printf(LLAPI_MSG_INFO, "%s: "format, cmd_name, ## __VA_ARGS__)

void usage(const char *name, int rc)
{
	fprintf(stderr,
	"The Lustre HSM Posix copy tool can be used as a daemon "
	"or as a command line tool\n"
	"The Lustre HSM deamon acts on action requests from Lustre "
	"to copy files to and from an HSM archive system. This "
	"POSIX-flavored daemon makes regular POSIX filesystem calls "
	"to an HSM mounted at a given hsm_root.\n"
	" Usage:\n"
	"   %s [options] <lustre_fs_name>\n"
	" Options:\n"
	"   --commcheck   : Verify we can open the communication "
	"channel then quit\n"
	"   --noattr           : Don't copy file attributes\n"
	"   --noshadow         : Don't create shadow namespace in archive\n"
	"   --noxattr          : Don't copy file extended attributes\n"
	"The Lustre HSM tool performs administrator-type actions "
	"on a Lustre HSM archive.\n"
	"This POSIX-flavored tool can link an "
	"existing HSM namespace into a Lustre filesystem.\n"
	" Usage:\n"
	"   %s [options] --import <src> <dst> <fsname>\n"
	"      import an archived subtree at\n"
	"       <src> (relative to hsm_root) into the Lustre filesystem at\n"
	"       <dst> (absolute)\n"
	"   %s [options] --rebind <old_FID> <new_FID> <fsname>\n"
	"      rebind an entry in the HSM to a new FID\n"
	"       <old_FID> old FID the HSM entry is bound to\n"
	"       <new_FID> new FID to bind the HSM entry to\n"
	"   %s [options] --rebind <list_file> <fsname>\n"
	"      perform the rebind operation for all FID in the list file\n"
	"       each line of <list_file> consists of <old_FID> <new_FID>\n"
	"   %s [options] --max_sequence <fsname>\n"
	"       return the max fid sequence of archived files\n"
	"   --abort       : Abort operation on major error\n"
	"   --archive <#> : Archive number (repeatable)\n"
	"   --noexecute   : Don't execute, just show what would be done\n"
	"   --hsm_root <path> : Target HSM mount point\n"
	"   --quiet       : Produce less verbose output\n"
	"   --verbose     : Produce more verbose output\n"
	"   --bandwidth <bw>  : limit IO bandwidth (unit can be used,"
	" default is MB)\n"
	"   --chunk_size <sz> : io size used during data copy"
	" (unit can be used, default is MB)\n",
	cmd_name, cmd_name, cmd_name, cmd_name, cmd_name);

	exit(rc);
}

int ct_parseopts(const int argc, char *const *argv)
{
	struct option long_opts[] = {
		{"archive",	required_argument, NULL,		'A'},
		{"commcheck",	no_argument,	   &opt.o_check,	 1},
		{"err-abort",	no_argument,	   &opt.o_err_abort,	 1},
		{"help",	no_argument,	   NULL,		'h'},
		{"hsm_root",	required_argument, NULL,		'p'},
		{"import",	no_argument,	   NULL,		'i'},
		{"rebind",	no_argument,	   NULL,		'r'},
		{"max_seq",	no_argument,	   NULL,		'M'},
		{"noexecute",	no_argument,	   &opt.o_dryrun,	 1},
		{"noattr",	no_argument,	   &opt.o_attr,		 0},
		{"noshadow",	no_argument,	   &opt.o_shadow,	 0},
		{"noxattr",	no_argument,	   &opt.o_xattr,	 0},
		{"quiet",	no_argument,	   NULL,		'q'},
		{"verbose",	no_argument,	   NULL,		'v'},
		{"bandwidth",	required_argument, NULL,		'b'},
		{"chunk_size",	required_argument, NULL,		'c'},
		{0, 0, 0, 0}
	};
	int			 c;
	unsigned long long	 value;
	unsigned long long	 unit;


	optind = 0;
	while ((c = getopt_long(argc, argv, "A:b:c:hiMp:qruv",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'A':
			if ((opt.o_archive_cnt >= MAX_ARCHIVE_CNT) ||
			    (atoi(optarg) >= MAX_ARCHIVE_CNT)) {
				CT_ERROR("archive number must be less"
					 "than %d\n", MAX_ARCHIVE_CNT);
				return E2BIG;
			}
			opt.o_archive_id[opt.o_archive_cnt] = atoi(optarg);
			opt.o_archive_cnt++;
			break;
		case 'b': /* -b and -c have both a number with unit as arg */
		case 'c':
			unit = ONE_MB;
			if (llapi_parse_size(optarg, &value, &unit, 0) < 0) {
				CT_ERROR("bad value for -%c '%s'\n", c, optarg);
				usage(argv[0], 1);
			}
			if (c == 'c')
				opt.o_chunk_size = value;
			else
				opt.o_bw = value;
			break;
		case 'h':
			usage(argv[0], 0);
		case 'i':
			opt.o_action = CA_IMPORT;
			break;
		case 'M':
			opt.o_action = CA_MAXSEQ;
			break;
		case 'p':
			opt.o_hsmroot = optarg;
			break;
		case 'q':
			opt.o_verbose--;
			break;
		case 'r':
			opt.o_action = CA_REBIND;
			break;
		case 'v':
			opt.o_verbose++;
			break;
		case 0:
			break;
		default:
			CT_ERROR("option '%s' unrecognized,"
				 " use -h for valid options\n",
				 argv[optind - 1]);
			return -EINVAL;
		}
	}
	/* set llapi message level */
	llapi_msg_set_level(opt.o_verbose);

	switch (opt.o_action) {
	case CA_IMPORT:
		/* src dst fsname */
		if (argc != optind + 3) {
			CT_ERROR("--import requires 2 arguments\n");
			usage(argv[0], 1);
		}
		opt.o_src = argv[optind++];
		opt.o_dst = argv[optind++];
		break;
	case CA_REBIND:
		/* FID1 FID2 fsname or FILE fsname */
		if (argc == optind + 2) {
			opt.o_src = argv[optind++];
			opt.o_dst = NULL;
		} else if (argc == optind + 3) {
			opt.o_src = argv[optind++];
			opt.o_dst = argv[optind++];
		} else {
			CT_ERROR("--rebind requires 1 or 2 arguments\n");
			usage(argv[0], 1);
		}
		break;
	case CA_MAXSEQ:
	default:
		/* just fsname */
		break;
	}

	if (argc != optind + 1) {
		CT_ERROR("no fsname specified\n");
		usage(argv[0], 1);
	}
	opt.o_fsname = argv[optind];

	CT_TRACE("action=%d src=%s dst=%s fsname=%s\n",
		 opt.o_action, opt.o_src, opt.o_dst,
		 opt.o_fsname);

	if (!opt.o_dryrun && !opt.o_hsmroot) {
		CT_ERROR("no --hsm_root specified\n");
		usage(argv[0], 1);
	}

	if (opt.o_action == CA_IMPORT) {
		if (opt.o_src && opt.o_src[0] == '/') {
			CT_ERROR("<src> path '%s' must be relative"
				 " (to --hsm_root).\n", opt.o_src);
			return -EINVAL;
		}

		if (opt.o_dst && opt.o_dst[0] != '/') {
			CT_ERROR("<dst> path '%s' must be absolute.\n",
				 opt.o_dst);
			return -EINVAL;
		}
	}

	return 0;
}

/* mkdir -p path */
int ct_mkdir_p(const char *path)
{
	char	*saved, *ptr;
	int	 rc;

	ptr = strdup(path);
	saved = ptr;
	while (*ptr == '/')
		ptr++;

	while ((ptr = strchr(ptr, '/')) != NULL) {
		*ptr = '\0';
		rc = mkdir(saved, DIR_PERM);
		*ptr = '/';
		if (rc < 0 && errno != EEXIST) {
			CT_ERROR("'%s' mkdir failed (%s)\n", path,
				 strerror(errno));
			free(saved);
			return -errno;
		}
		ptr++;
	}
	free(saved);
	return 0;
}

static int ct_save_stripe(const char *src, const char *dst)
{
	char			 lov_file[PATH_MAX];
	int			 rc;
	struct lov_user_md	*lum;
	int			 lum_size, fd;

	snprintf(lov_file, sizeof(lov_file), "%s.lov", dst);
	CT_TRACE("Saving stripe info of '%s' in %s\n", src, lov_file);
	lum_size = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
	lum = malloc(lum_size);
	if (lum == NULL)
		return -ENOMEM;

	rc = llapi_file_get_stripe(src, lum);
	if (rc < 0) {
		CT_ERROR("'%s' cannot get stripe info on (%s)\n", src,
			 strerror(rc));
		goto out;

	}
	fd = open(lov_file, O_TRUNC | O_CREAT | O_WRONLY, FILE_PERM);
	if (fd < 0) {
		CT_ERROR("'%s' cannot open (%s)\n", lov_file, strerror(errno));
		rc = -errno;
		goto out;
	}
	rc = write(fd, lum, lum_size);
	if (rc < 0) {
		CT_ERROR("'%s' cannot write %d bytes (%s)\n",
			 lum_size, lov_file, strerror(errno));
		close(fd);
		rc = -errno;
		goto out;
	}
	rc = close(fd);
	if (rc < 0) {
		CT_ERROR("'%s' cannot close (%s)\n", lov_file, strerror(errno));
		rc = -errno;
		goto out;
	}
	rc = 0;
out:
	free(lum);
	return rc;
}

static int get_lov_rules(const char *src, const char *dst,
			 struct lov_user_md_v3 *lum, int lum_size)
{
	char	 lov_file[PATH_MAX];
	int	 fd, rc;

	snprintf(lov_file, sizeof(lov_file), "%s.lov", src);
	CT_TRACE("Reading stripe rules from '%s' for '%s'\n", lov_file, src);

	fd = open(lov_file, O_RDONLY);
	if (fd < 0) {
		CT_ERROR("'%s' cannot open (%s)\n", lov_file, strerror(errno));
		goto no_lov;
	}
	rc = read(fd, lum, lum_size);
	if (rc < 0) {
		CT_ERROR("'%s' cannot read %d bytes (%s)\n", lov_file, lum_size,
			 strerror(errno));
		close(fd);
		goto no_lov;
	}
	close(fd);

	if (lum->lmm_magic == LOV_USER_MAGIC_V3) {
		char fsname[MAX_OBD_NAME + 1];

		llapi_search_fsname(dst, fsname);
		rc = llapi_search_ost(fsname, lum->lmm_pool_name, NULL);
		if (rc < 0) {
			CT_ERROR("Pool '%s' does not exist, so we ignore it\n",
				 lum->lmm_pool_name);
			lum->lmm_magic = LOV_USER_MAGIC_V1;
		}
	}

	return 0;

no_lov:
	return -ENODATA;
}

static int ct_restore_stripe(const char *dst, const int fd_dst,
			     const struct lov_user_md_v3 *lum)
{
	int			 rc;
	int			 xattr_sz;

	if (lum->lmm_magic == LOV_USER_MAGIC_V1)
		xattr_sz = sizeof(struct lov_user_md_v1);
	else
		xattr_sz = sizeof(struct lov_user_md_v3);

	rc = fsetxattr(fd_dst, XATTR_LUSTRE_LOV, (void *)lum, xattr_sz,
		       XATTR_CREATE);

	if (rc == -1) {
		CT_ERROR("'%s' cannot set striping (%s)\n",
			 dst, strerror(errno));
		rc = -errno;
	}

	return rc;
}

/* non-blocking read or write */
static int nonblock_rw(const bool wr, const int fd, char *buf, int size)
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

int ct_copy_data(const char *rootpath, const char *src, const char *dst,
		 const int fd_src, const int fd_dst,
		 const struct hsm_action_item *hai, const long hal_flags,
		 size_t *file_size)
{
	struct hsm_progress	 hp;
	struct stat		 src_st = {}, dst_st = {};
	char			*buf;
	__u64			 wpos = 0, rpos = 0, rlen;
	time_t			 lastprint = time(0);
	int			 rsize, wsize, bufoff = 0;
	int			 rc = 0;

	CT_TRACE("going to copy data from '%s' to %s\n", src, dst);

	buf = malloc(opt.o_chunk_size);
	if (!buf)
		return -ENOMEM;

	if (fstat(fd_src, &src_st) == -1) {
		CT_ERROR("'%s' stat failed (%s)\n", src, strerror(errno));
		return -errno;
	}

	if (!S_ISREG(src_st.st_mode)) {
		CT_ERROR("'%s' not a regular file\n", src);
		return -EINVAL;
	}

	if (file_size)
		*file_size = src_st.st_size;

	rc = lseek(fd_src, hai->hai_extent.offset, SEEK_SET);
	if (rc < 0) {
		CT_ERROR("'%s' seek to read to "LPU64" (len "
			 LPU64") failed (%s)\n",
			 src, hai->hai_extent.offset, src_st.st_size,
			 strerror(errno));
		rc = -errno;
		goto out;
	}

	if (fstat(fd_dst, &dst_st) == -1) {
		CT_ERROR("'%s' stat failed (%s)\n", dst, strerror(errno));
		return -errno;
	}

	if (!S_ISREG(dst_st.st_mode)) {
		CT_ERROR("'%s' not a regular file\n", dst);
		return -EINVAL;
	}

	rc = lseek(fd_dst, hai->hai_extent.offset, SEEK_SET);
	if (rc < 0) {
		CT_ERROR("'%s' seek to write to "LPU64" failed (%s)\n", src,
			 hai->hai_extent.offset, strerror(errno));
		rc = -errno;
		goto out;
	}

	/* cookie is used to reference the file we are working on (ie FID)
	 * progress is made on data FID */
	hp.hp_cookie = hai->hai_cookie;
	hp.hp_fid = hai->hai_dfid;
	hp.hp_extent.offset = hai->hai_extent.offset;
	hp.hp_extent.length = 0;
	hp.hp_errval = 0;
	hp.hp_flags = 0;  /* Not done yet */
	rc = llapi_hsm_progress(rootpath, &hp);
	if (rc < 0) {
		/* Action has been canceled or something wrong
		 * is happening. Stop copying data. */
		CT_ERROR("%s->'%s' progress returned err %d\n", src, dst, rc);
		goto out;
	}

	errno = 0;
	/* Don't read beyond a given extent */
	rlen = (hai->hai_extent.length == -1LL) ?
		src_st.st_size : hai->hai_extent.length;
	while (wpos < rlen) {
		int chunk = (rlen - wpos > opt.o_chunk_size) ? opt.o_chunk_size
			    : rlen - wpos;

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
					 * give it back to the coordinator
					 * for rescheduling */
					rc = -EAGAIN;
					break;
				}
			}
			if (rsize < 0) {
				CT_ERROR("'%s' read failed (%s)\n", src,
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
			CT_ERROR("'%s' write failed (%s)\n", dst,
				 strerror(-wsize));
			rc = wsize;
			break;
		}
		wpos += wsize;
		bufoff += wsize;

		if (opt.o_bw != 0) {
			static unsigned long long	tot_bytes;
			static time_t			start_time, last_time;
			time_t				now = time(0);
			double				tot_time, excess;

			if (now > last_time + 5) {
				tot_bytes = 0;
				start_time = last_time = now;
			}

			tot_bytes += wsize;
			tot_time = now - start_time;
			if (tot_time < 1)
				tot_time = 1;
			excess = tot_bytes - tot_time * opt.o_bw;
			if (excess > 0)
				usleep(excess / opt.o_bw);
			last_time = now;
		}

		if (time(0) >= lastprint + REPORT_INTERVAL) {
			lastprint = time(0);
			CT_TRACE("%%%d ", 100 * wpos / rlen);
			hp.hp_extent.length = wpos;
			rc = llapi_hsm_progress(rootpath, &hp);
			if (rc < 0) {
				/* Action has been canceled or something wrong
				 * is happening. Stop copying data. */
				CT_ERROR("%s->'%s' progress returned err %d\n",
					 src, dst, rc);
				goto out;
			}
		}

		rc = 0;
	}
	CT_TRACE("\n");

out:
	/*
	 * make sure the file is on disk before reporting success.
	 */
	if ((rc == 0) && (fsync(fd_dst) != 0)) {
		CT_ERROR("'%s' fsync failed (%s)\n", dst, strerror(errno));
		rc = -errno;
	}
	/*
	 * truncate restored file
	 * size is taken from the archive this is done to support
	 * restore after a force release which leaves the file with the
	 * wrong size (can big bigger than the new size)
	 */
	if ((hai->hai_action == HSMA_RESTORE) &&
	    (src_st.st_size < dst_st.st_size)) {
		rc = ftruncate(fd_dst, src_st.st_size);
		if (rc == -1) {
			rc = -errno;
			CT_ERROR("'%s' final truncate to %lu failed (%s)\n",
				 dst, src_st.st_size, strerror(-rc));
			opt.err_major++;
		}
	}

	free(buf);

	return rc;
}

/* Copy file attributes from file src to file dest */
int ct_copy_attr(const char *src, const char *dst,
		 const int fd_src, const int fd_dst)
{
	struct stat	st;
	struct timeval	times[2];

	if (fstat(fd_src, &st) < 0) {
		CT_ERROR("'%s' stat failed (%s)\n",
		     src, strerror(errno));
		return -errno;
	}

	times[0].tv_sec = st.st_atime;
	times[0].tv_usec = 0;
	times[1].tv_sec = st.st_mtime;
	times[1].tv_usec = 0;
	if (fchmod(fd_dst, st.st_mode) == -1 ||
	    fchown(fd_dst, st.st_uid, st.st_gid) == -1 ||
	    futimes(fd_dst, times) == -1)
		CT_ERROR("'%s' fchmod fchown or futimes failed (%s)\n", src,
			 strerror(errno));
		return -errno;
	return 0;
}

int ct_copy_xattr(const char *src, const char *dst,
		  const int fd_src, const int fd_dst,
		  const bool restore)
{
	char	*names, *ptr;
	char	*buf;
	ssize_t	 size;
	int	 rc, names_sz, buf_sz;

	names_sz = ONE_MB;
	do {
		names = malloc(names_sz);
		if (names == NULL)
			return -ENOMEM;

		size = llistxattr(src, names, names_sz);
		if (size == -1) {
			free(names);
			if (errno != ERANGE)
				return -errno;

			/* buffer is too small, try a larger one */
			names_sz += ONE_MB;
			continue;
		}
		break;
	} while (1);

	buf_sz = 4 * ONE_MB;
	buf = malloc(buf_sz); /* arbitrary size */
	if (buf == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	ptr = names;
	errno = 0;
	rc = 0;
	while (ptr < names + size) {
		rc = lgetxattr(src, ptr, buf, buf_sz);
		if (rc < 0)
			return -errno;

		/* when we restore, we do not restore lustre
		 * xattr
		 */
		if ((restore == false) ||
		    (strncmp(XATTR_TRUSTED_PREFIX, ptr,
			     sizeof(XATTR_TRUSTED_PREFIX) - 1) != 0)) {
			rc = fsetxattr(fd_dst, ptr, buf, rc, 0);
			CT_TRACE("'%s' fsetxattr of '%s' rc=%d (%s)\n",
				 dst, ptr, rc, strerror(errno));
			/* lustre.* attrs aren't supported on other fs's */
			if (rc == -1 && errno != EOPNOTSUPP) {
				CT_ERROR("'%s' fsetxattr of '%s' failed (%s)\n",
					 dst, ptr, strerror(errno));
				return -errno;
			}
		}
		ptr += strlen(ptr) + 1;
		rc = 0;
	}

	free(buf);
out:
	free(names);
	return rc;
}

static int ct_path_lustre(char *buf, const int sz, const char *rootpath,
			  const lustre_fid *fid)
{
	return snprintf(buf, sz, "%s/%s/fid/"DFID_NOBRACE, rootpath,
			dot_lustre_name, PFID(fid));
}

static int ct_path_target(char *buf, const int sz, const char *rootpath,
			  const lustre_fid *fid)
{
	return snprintf(buf, sz, "%s/%04x/%04x/%04x/%04x/%04x/%04x/"
			DFID_NOBRACE, rootpath,
			(unsigned int)((fid)->f_seq >> 48 & 0xFFFF),
			(unsigned int)((fid)->f_seq >> 32 & 0xFFFF),
			(unsigned int)((fid)->f_seq >> 16 & 0xFFFF),
			(unsigned int)((fid)->f_seq       & 0xFFFF),
				       (fid)->f_oid >> 16 & 0xFFFF,
				       (fid)->f_oid       & 0xFFFF,
			PFID(fid));
}

int ct_retryable(const int err)
{
	int rc;

	rc = (err == -ETIMEDOUT);
	return rc;
}

int ct_begin(const char *rootpath, const struct hsm_action_item *hai,
	     struct hsm_copy *copy)
{
	char	 src[PATH_MAX];
	int	 rc;

	ct_path_lustre(src, sizeof(src), rootpath, &hai->hai_fid);
	rc = llapi_hsm_copy_start(rootpath, copy, hai);
	if (rc < 0)
		CT_ERROR("'%s' copy start failed (%s)\n", src, strerror(-rc));

	return rc;
}

int ct_fini(const char *rootpath, struct hsm_copy *copy,
	    const struct hsm_action_item *hai,
	    const int flags, const int ct_rc)
{
	struct hsm_progress	hp;
	char			lstr[PATH_MAX];
	int			rc;

	CT_TRACE("Action completed, notifying coordinator "
		 "cookie="LPX64", FID="DFID", flags=%d err=%d\n",
		 hai->hai_cookie, PFID(&hai->hai_fid),
		 flags, -ct_rc);

	ct_path_lustre(lstr, sizeof(lstr), rootpath, &hai->hai_fid);
	hp.hp_cookie = hai->hai_cookie;
	/* for data movement actions, data FID is used for progress */
	if ((hai->hai_action == HSMA_RESTORE) ||
	    (hai->hai_action == HSMA_ARCHIVE))
		hp.hp_fid = hai->hai_dfid;
	else
		hp.hp_fid = hai->hai_fid;
	/* hmm - should we even report minor errors? */
	hp.hp_errval = -ct_rc;
	hp.hp_extent = hai->hai_extent;
	hp.hp_flags = flags;
	rc = llapi_hsm_copy_end(lstr, copy, &hp);
	if (rc == -ECANCELED)
		CT_ERROR("'%s' completed action has been canceled: "
			 "cookie="LPX64", FID="DFID"\n", lstr, hai->hai_cookie,
			 PFID(&hai->hai_fid));
	else if (rc < 0)
		CT_ERROR("'%s' copy end failed (%s)\n", lstr, strerror(-rc));
	else
		CT_TRACE("'%s' copy end ok (rc=%d)\n", lstr, rc);

	return rc;
}

int ct_archive(const char *rootpath, const struct hsm_action_item *hai,
	       const long hal_flags)
{
	struct hsm_copy	 copy;
	char		 src[PATH_MAX];
	char		 dst[PATH_MAX];
	int		 rc, rcf = 0;
	bool		 rename_needed = false;
	int		 ct_flags = 0, open_flags;
	int		 fd_src = -1, fd_dst = -1;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini_major;

	/* we fill archive so:
	 * source = data FID
	 * destination = lustre FID
	 */
	ct_path_lustre(src, sizeof(src), rootpath, &hai->hai_dfid);
	ct_path_target(dst, sizeof(dst), opt.o_hsmroot, &hai->hai_fid);
	if (hai->hai_extent.length == -1) {
		/* whole file, write it to tmp location and atomically
		   replace old archived file */
		sprintf(dst, "%s_tmp", dst);
		/* we cannot rely on the same test because ct_copy_data()
		 * updates hai_extent.length */
		rename_needed = true;
	}

	CT_TRACE("'%s' archived to %s\n", src, dst);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini_major;
	}

	rc = ct_mkdir_p(dst);
	if (rc < 0) {
		CT_ERROR("'%s' mkdir_p failed (%s)\n", dst, strerror(-rc));
		goto fini_major;
	}

	fd_src = open(src, O_RDONLY | O_NOATIME | O_NONBLOCK | O_NOFOLLOW);
	if (fd_src == -1) {
		CT_ERROR("'%s' open read failed (%s)\n", src, strerror(errno));
		rc = -errno;
		goto fini_major;
	}

	open_flags = O_WRONLY | O_NOFOLLOW | O_NONBLOCK;
	/* If extent is specified, don't truncate an old archived copy */
	open_flags |= ((hai->hai_extent.length == -1) ? O_TRUNC : 0) | O_CREAT;

	fd_dst = open(dst, open_flags, FILE_PERM);
	if (fd_dst == -1) {
		CT_ERROR("'%s' open write failed (%s)\n", dst, strerror(errno));
		rc = -errno;
		goto fini_major;
	}

	/* saving stripe is not critical */
	rc = ct_save_stripe(src, dst);
	if (rc < 0)
		CT_ERROR("'%s' cannot save file striping info in '%s' (%s)\n",
			 src, dst, strerror(-rc));

	rc = ct_copy_data(rootpath, src, dst, fd_src, fd_dst,
			  hai, hal_flags, NULL);
	if (rc < 0) {
		CT_ERROR("'%s' data copy failed to '%s' (%s)\n",
			 src, dst, strerror(-rc));
		goto fini_major;
	}

	CT_TRACE("'%s' data archived to '%s' done\n", src, dst);

	/* attrs will remain on the MDS; no need to copy them, except possibly
	  for disaster recovery */
	if (opt.o_attr) {
		rc = ct_copy_attr(src, dst, fd_src, fd_dst);
		if (rc < 0) {
			CT_ERROR("'%s' attr copy failed to '%s' (%s)\n",
				 src, strerror(-rc));
			rcf = rc;
		}
		CT_TRACE("'%s' attr file copied to archive '%s'\n",
			 src, dst);
	}

	/* xattrs will remain on the MDS; no need to copy them, except possibly
	 for disaster recovery */
	if (opt.o_xattr) {
		rc = ct_copy_xattr(src, dst, fd_src, fd_dst, false);
		if (rc < 0) {
			CT_ERROR("'%s' xattr copy failed to '%s' (%s)\n",
				 src, dst, strerror(-rc));
			rcf = rcf ? rcf : rc;
		}
		CT_ERROR("'%s' xattr file copied to archive '%s'\n",
			 src, dst);
	}

	if (rename_needed == true) {
		char	 tmp_src[PATH_MAX];
		char	 tmp_dst[PATH_MAX];

		/* atomically replace old archived file */
		ct_path_target(src, sizeof(src), opt.o_hsmroot, &hai->hai_fid);
		rc = rename(dst, src);
		if (rc < 0) {
			CT_ERROR("'%s' renamed to '%s' failed (%s)\n", dst, src,
				 strerror(errno));
			rc = -errno;
			goto fini_major;
		}
		/* rename lov file */
		strcpy(tmp_src, src);
		strcpy(tmp_dst, dst);
		strcat(tmp_src, ".lov");
		strcat(tmp_dst, ".lov");
		rc = rename(tmp_dst, tmp_src);
		if (rc < 0)
			CT_ERROR("'%s' renamed to '%s' failed (%s)\n",
				 tmp_dst, tmp_src, strerror(errno));
	}

	if (opt.o_shadow) {
		/* Create a namespace of softlinks that shadows the original
		 * Lustre namespace.  This will only be current at
		 * time-of-archive (won't follow renames).
		 * WARNING: release won't kill these links; a manual
		 * cleanup of dead links would be required.
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
			CT_ERROR("'%s' fid2path failed (%d)\n", buf, rc);
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

		ct_path_target(dst, sizeof(dst), buf, &hai->hai_fid);

		if (ct_mkdir_p(src)) {
			CT_ERROR("'%s' mkdir_p failed (%s)\n", src,
				 strerror(errno));
			rcf = rcf ? rcf : -errno;
			goto fini_minor;
		}
		/* symlink already exists ? */
		sz = readlink(src, buf, sizeof(buf));
		if (sz >= 0) {
			buf[sz] = '\0';
			if (sz == 0 || strncmp(buf, dst, sz) != 0) {
				if (unlink(src) && errno != ENOENT) {
					CT_ERROR("'%s' unlink symlink failed "
						 "(%s)\n", src,
						 strerror(errno));
					rcf = rcf ? rcf : -errno;
					goto fini_minor;
				/* unlink old symlink done */
				CT_TRACE("'%s' remove old symlink pointing"
					 " to '%s'\n", src, buf);
				}
			} else {
				/* symlink already ok */
				CT_TRACE("'%s' symlink already pointing"
					 " to '%s'\n", src, dst);
				rcf = 0;
				goto fini_minor;
			}
		}
		if (symlink(dst, src)) {
			CT_ERROR("'%s' symlink to '%s' failed (%s)\n", src, dst,
				 strerror(errno));
			rcf = rcf ? rcf : -errno;
			goto fini_minor;
		}
		CT_TRACE("'%s' symlink to '%s' done\n", src, dst);
	}
fini_minor:
	if (rcf)
		opt.err_minor++;
	goto out;

fini_major:
	opt.err_major++;

	unlink(dst);
	if (ct_retryable(rc))
		ct_flags |= HP_FLAG_RETRY;

	rcf = rc;

out:
	if (fd_src != -1)
		close(fd_src);

	if (fd_dst != -1)
		close(fd_dst);

	rc = ct_fini(rootpath, &copy, hai, ct_flags, rcf);
	return rc;
}

int ct_restore(const char *rootpath, struct hsm_action_item *hai,
	       const long hal_flags)
{
	struct hsm_copy		 copy;
	char			 src[PATH_MAX];
	char			 dst[PATH_MAX];
	char			 released[PATH_MAX], parent[PATH_MAX];
	int			 rc;
	size_t			 file_size;
	long long		 recno;
	int			 linkno;
	char			 fid[FID_NOBRACE_LEN + 1];
	int			 flags = 0, volatile_flags;
	int			 fd_src = -1, fd_dst = -1;
	bool			 use_default_stripe;
	char			*ptr;
	struct lov_user_md_v3	*lum = NULL;
	int			 lum_size;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini;

	/* we fill lustre so:
	 * source = lustre FID in the backend
	 * destination = data FID = volatile file
	 */

	/* find the parent directory of the file to be restored:
	 * - find file full path name from FID
	 * - extract parent directory name
	 */
	sprintf(fid, DFID_NOBRACE, PFID(&hai->hai_fid));
	rc = llapi_fid2path(rootpath, fid, released,
			    sizeof(released), &recno, &linkno);
	if (rc != 0) {
		CT_ERROR(DFID": cannot get path for FID (%d)\n",
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

	/* build backend file name from released file FID */
	ct_path_target(src, sizeof(src), opt.o_hsmroot, &hai->hai_fid);

	/* get the lov ea from backend */
	lum_size = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
	lum = malloc(lum_size);
	if (lum == NULL) {
		rc = -ENOMEM;
		goto fini;
	}

	rc = get_lov_rules(src, parent, lum, lum_size);
	if (rc < 0) {
		CT_WARN("'%s' cannot get stripe rules, use default\n", src);
		use_default_stripe = true;
		volatile_flags = 0;
	} else {
		use_default_stripe = false;
		volatile_flags = O_LOV_DELAY_CREATE;
	}

	/* create a volatile file in lustre */
	fd_dst = llapi_create_volatile(parent, volatile_flags);
	if (fd_dst < 0) {
		CT_ERROR("'%s' cannot create volatile file (%d)\n",
			 parent, fd_dst);
		goto fini;
	}

	if (use_default_stripe == false) {
		rc = ct_restore_stripe(dst, fd_dst, lum);
		if (rc < 0) {
			CT_ERROR("'%s' cannot restore file striping info"
				 " from '%s' (%s)\n", dst, src, strerror(-rc));
			opt.err_major++;
			goto fini;
		}
	}

	/* get the FID of the volatile file, set it as the data FID */
	rc = llapi_fd2fid(fd_dst, &hai->hai_dfid);
	if (rc < 0) {
		CT_ERROR("'%s' cannot get FID of created volatile"
			 " file %d (%d)\n", parent, rc);
		goto fini;
	}

	/* build volatile "file name", for messages */
	snprintf(dst, sizeof(dst), "%s/{VOLATILE}="DFID, parent,
		 PFID(&hai->hai_dfid));

	CT_TRACE("'%s' restore data from '%s' in '%s'\n", released, src, dst);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini;
	}

	fd_src = open(src, O_RDONLY | O_NOATIME | O_NONBLOCK | O_NOFOLLOW);
	if (fd_src < 0) {
		CT_ERROR("'%s' open for read failed (%s)\n", src,
			 strerror(errno));
		rc = -errno;
		goto fini;
	}

	rc = ct_copy_data(rootpath, src, dst, fd_src, fd_dst,
			  hai, hal_flags, &file_size);
	if (rc < 0) {
		CT_ERROR("'%s' data copy to '%s' failed (%s)\n", src, dst,
			 strerror(-rc));
		opt.err_major++;
		if (ct_retryable(rc))
			flags |= HP_FLAG_RETRY;
		goto fini;
	}

	CT_TRACE("'%s' data restore done to %s\n", src, dst);

	if (opt.o_xattr) {
		rc = ct_copy_xattr(src, dst, fd_src, fd_dst, true);
		if (rc < 0) {
			CT_ERROR("'%s' xattr copy failed (%s)\n", opt.o_src,
				 strerror(-rc));
			opt.err_minor++;
			goto fini;
		}
		CT_TRACE("'%s' xattr restore done to %s\n", src, dst);
	}

fini:
	rc = ct_fini(rootpath, &copy, hai, flags, rc);
	/* object swaping is done by cdt at copy end, so close of volatile file
	 * cannot be done before */
	if (fd_src != -1)
		close(fd_src);

	if (fd_dst != -1)
		close(fd_dst);

	if (lum != NULL)
		free(lum);

	return rc;
}

int ct_remove(const char *rootpath, const struct hsm_action_item *hai,
	      const long hal_flags)
{
	struct hsm_copy		copy;
	char			dst[PATH_MAX];
	int			rc;

	rc = ct_begin(rootpath, hai, &copy);
	if (rc < 0)
		goto fini;

	ct_path_target(dst, sizeof(dst), opt.o_hsmroot, &hai->hai_fid);

	CT_TRACE("'%s' removed file\n", dst);

	if (opt.o_dryrun) {
		rc = 0;
		goto fini;
	}

	rc = unlink(dst);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR("'%s' unlink failed (%s)\n", dst, strerror(-rc));
		opt.err_minor++;
		goto fini;
	}

fini:
	rc = ct_fini(rootpath, &copy, hai, 0, rc);
	return rc;
}

int ct_process_item(const char *rootpath, struct hsm_action_item *hai,
		    const long hal_flags)
{
	int			rc = 0;

	if (opt.o_verbose >= LLAPI_MSG_INFO || opt.o_dryrun) {
		/* Print the original path */
		char fid[128];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		sprintf(fid, DFID, PFID(&hai->hai_fid));
		CT_TRACE("'%s' action %d reclen %d, cookie="LPX64"\n",
			 fid, hai->hai_action, hai->hai_len,
			 hai->hai_cookie);
		rc = llapi_fid2path(rootpath, fid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			CT_ERROR("'%s' fid2path failed (%d)\n", fid, rc);
		else
			CT_TRACE("'%s' processing file\n", path);
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
		CT_TRACE("'%s' cancel not implemented\n", rootpath);
		/* Don't report progress to coordinator for this cookie:
		 * the copy function will get ECANCELED when reporting
		 * progress. */
		opt.err_minor++;
		return 0;
		break;
	default:
		CT_ERROR("'%s' unknown action %d\n", rootpath, hai->hai_action);
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
	struct ct_th_data *cttd = data;
	int rc;

	rc = ct_process_item(cttd->rootpath, cttd->hai, cttd->hal_flags);

	free(cttd->hai);
	free(cttd);
	pthread_exit((void *)(long)rc);
}

int ct_process_item_async(const char *rootpath,
			  const struct hsm_action_item *hai,
			  const long hal_flags)
{
	pthread_attr_t		 attr;
	pthread_t		 thread;
	struct ct_th_data	*data;
	int			 rc;

	data = malloc(sizeof(*data));
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
	if (rc != 0) {
		CT_ERROR("'%s' pthread_attr_init: %s\n", rootpath,
			 strerror(rc));
		free(data->hai);
		free(data);
		return -rc;
	}
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	rc = pthread_create(&thread, &attr, ct_thread, data);
	if (rc != 0)
		CT_ERROR("'%s' thread create: (%s)\n", rootpath, strerror(rc));

	pthread_attr_destroy(&attr);
	return 0;
}

static int ct_import_one(const char *src, const char *dst)
{
	char		newarc[PATH_MAX];
	lustre_fid	fid;
	struct stat	st;
	int		rc;

	CT_TRACE("'%s' importing from %s\n", dst, src);

	if (stat(src, &st) < 0) {
		CT_ERROR("'%s' stat failed (%s)\n", src, strerror(errno));
		return -errno;
	}

	if (opt.o_dryrun)
		return 0;

	rc = llapi_hsm_import(dst,
			      opt.o_archive_cnt ? opt.o_archive_id[0] : 0,
			      &st, 0, 0, 0, 0, NULL, &fid);
	if (rc < 0) {
		CT_ERROR("'%s' import from '%s' failed (%s)\n", dst, src,
			 strerror(-rc));
		return -rc;
	}

	ct_path_target(newarc, sizeof(newarc), opt.o_hsmroot, &fid);

	rc = ct_mkdir_p(newarc);
	if (rc < 0) {
		CT_ERROR("'%s' mkdir_p failed (%s)\n", newarc, strerror(-rc));
		opt.err_major++;
		return rc;

	}

	/* Lots of choices now: mv, ln, ln -s ? */
	rc = link(src, newarc); /* hardlink */
	if (rc < 0) {
		CT_ERROR("'%s' link to '%s' failed (%s)\n", newarc, src,
			 strerror(errno));
		opt.err_major++;
		return -errno;
	}
	CT_TRACE("'%s' imported from '%s'=='%s'\n", dst, newarc, src);

	return 0;
}

static char *path_concat(const char *dirname, const char *basename)
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

int ct_import_recurse(const char *relpath)
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
			CT_ERROR("'%s' opendir failed (%s)\n", srcpath,
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
			CT_ERROR("'%s' readdir_r failed (%s)\n", relpath,
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
				CT_ERROR("'%s' ct_mkdir_p failed (%s)\n", dst,
					 strerror(-rc));
				opt.err_major++;
			}
		}

		if (rc != 0) {
			CT_ERROR("'%s' importing failed\n", newpath);
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

static int ct_rebind_one(const lustre_fid *old_fid, const lustre_fid *new_fid)
{
	char src[PATH_MAX];
	char dst[PATH_MAX];

	CT_TRACE("rebind "DFID" to "DFID"\n", PFID(old_fid), PFID(new_fid));

	ct_path_target(src, sizeof(src), opt.o_hsmroot, old_fid);
	ct_path_target(dst, sizeof(dst), opt.o_hsmroot, new_fid);

	if (!opt.o_dryrun) {
		ct_mkdir_p(dst);
		if (rename(src, dst)) {
			CT_ERROR("'%s' rename to '%s' failed (%s)\n", src, dst,
				 strerror(errno));
			return -errno;
		}
		/* rename lov file */
		strcat(src, ".lov");
		strcat(dst, ".lov");
		if (rename(src, dst))
			CT_ERROR("'%s' rename to '%s' failed (%s)\n", src, dst,
				 strerror(errno));

	}
	return 0;
}

bool fid_is_file(lustre_fid *fid)
{
	return fid_is_norm(fid) || fid_is_igif(fid);
}

static int ct_rebind()
{
	int		 rc;
	lustre_fid	 old_fid, new_fid;
	FILE		*filp;
	ssize_t		 r;
	char		*line = NULL;
	size_t		 len = 0;

	if (opt.o_dst) {
		if (sscanf(opt.o_src, SFID, RFID(&old_fid)) != 3 ||
		    !fid_is_file(&old_fid)) {
			CT_ERROR("'%s' invalid FID format\n", opt.o_src);
			return -EINVAL;
		}
		if (sscanf(opt.o_dst, SFID, RFID(&new_fid)) != 3 ||
		    !fid_is_file(&new_fid)) {
			CT_ERROR("'%s' invalid FID format\n", opt.o_dst);
			return -EINVAL;
		}
		return ct_rebind_one(&old_fid, &new_fid);
	} else {
		unsigned int nl = 0;
		unsigned int ok = 0;
		char fid2[2*FID_LEN];

		/* o_src is a list file */
		filp = fopen(opt.o_src, "r");
		if (filp == NULL) {
			rc = -errno;
			CT_ERROR("'%s' open failed (%s)\n", opt.o_src,
				 strerror(-rc));
			return rc;
		}
		/* each line consists of 2 FID */
		while ((r = getline(&line, &len, filp)) != -1) {
			nl++;
			if (r >= sizeof(fid2)) {
				CT_ERROR("'%s' line %u too long\n",
					 opt.o_src, nl);
				opt.err_major++;
				continue;
			}
			rc = sscanf(line, SFID" %s", RFID(&old_fid), fid2);
			if (rc != 4 || !fid_is_file(&old_fid)) {
				CT_ERROR("'%s' FID expected near '%s',"
					 " line %u\n",
					 opt.o_src, line, nl);
				opt.err_major++;
				continue;
			}
			rc = sscanf(fid2, SFID, RFID(&new_fid));
			if (rc != 3 || !fid_is_file(&new_fid)) {
				CT_ERROR("'%s' FID expected near '%s',"
					 " line %u\n",
					 opt.o_src, fid2, nl);
				opt.err_major++;
				continue;
			}
			if (ct_rebind_one(&old_fid, &new_fid))
				opt.err_major++;
			else
				ok++;
		}
		fclose(filp);
		if (line)
			free(line);
		/* return 0 if all rebinds were sucessful */
		CT_TRACE("'%s' %u lines read, %u rebind successful\n",
			 opt.o_src, nl, ok);
		return (ok == nl ? 0 : -1);
	}
}

static int ct_dir_level_max(const char *dirpath, __u16 *sub_seqmax)
{
	DIR		*dir;
	int		 rc;
	__u16		 sub_seq;
	struct dirent	 ent, *cookie = NULL;

	*sub_seqmax = 0;

	dir = opendir(dirpath);
	if (dir == NULL) {
		rc = -errno;
		CT_ERROR("'%s' failed to open directory (%s)\n", opt.o_hsmroot,
			 strerror(-rc));
		return rc;
	}
	while ((rc = readdir_r(dir, &ent, &cookie)) == 0) {
		if (cookie == NULL)
			/* end of directory.
			 * rc is 0 and seqmax contains the max value. */
			goto out;

		if (!strcmp(ent.d_name, ".") || !strcmp(ent.d_name, ".."))
			continue;

		if (sscanf(ent.d_name, "%hx", &sub_seq) != 1) {
			CT_TRACE("'%s' unexpected dirname format, "
				 "skip entry.\n", ent.d_name);
			continue;
		}
		if (sub_seq > *sub_seqmax)
			*sub_seqmax = sub_seq;
	}
	rc = -errno;
	CT_ERROR("'%s' readdir_r failed (%s)\n", dirpath, strerror(-rc));

out:
	closedir(dir);
	return rc;
}

static int ct_max_sequence()
{
	int   rc, i;
	char  path[PATH_MAX];
	__u64 seq = 0;
	__u16 subseq;

	strncpy(path, opt.o_hsmroot, sizeof(path));
	/* FID sequence is stored in top-level directory names:
	 * hsm_root/16bits (high weight)/16 bits/16 bits/16 bits (low weight).
	 */
	for (i = 0; i < 4; i++) {
		rc = ct_dir_level_max(path, &subseq);
		if (rc != 0)
			return rc;
		seq |= ((__u64)subseq << ((3-i) * 16));
		sprintf(path + strlen(path), "/%04x", subseq);
	}
	printf("max_sequence: %016Lx\n", seq);
	return 0;
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

	rc = daemon(1, 1);
	if (rc != 0)
		CT_ERROR("%d: cannot start as daemon (%s)", getpid(),
			 strerror(-rc));

	rc = llapi_hsm_copytool_start(&ctdata, opt.o_fsname, 0,
				      opt.o_archive_cnt, opt.o_archive_id);
	if (rc < 0) {
		CT_ERROR("%d: cannot start copytool interface: %s\n", getpid(),
			 strerror(-rc));
		return rc;
	}

	if (opt.o_check)
		return llapi_hsm_copytool_fini(&ctdata);

	signal(SIGINT, handler);
	signal(SIGTERM, handler);

	while (1) {
		struct hsm_action_list *hal;
		struct hsm_action_item *hai;
		int msgsize, i = 0;

		CT_TRACE("%d: waiting for message from kernel\n", getpid());

		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			CT_TRACE("%d: shutting down", getpid());
			break;
		} else if (rc == -EAGAIN) {
			continue; /* msg not for us */
		} else if (rc < 0) {
			CT_WARN("%d: message receive: (%s)\n", getpid(),
				strerror(-rc));
			opt.err_major++;
			if (opt.o_err_abort)
				break;
			else
				continue;
		}

		CT_TRACE("%d: copytool fs=%s archive#=%d item_count=%d\n",
			 getpid(), hal->hal_fsname, hal->hal_archive_id,
			 hal->hal_count);

		rc = llapi_search_rootpath(rootpath, hal->hal_fsname);
		if (rc < 0) {
			CT_ERROR("'%s' cannot find root path\n",
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
				CT_ERROR("'%s' item %d past end of message!\n",
					 rootpath, i);
				opt.err_major++;
				rc = -EPROTO;
				break;
			}
			rc = ct_process_item_async(rootpath, hai,
						   hal->hal_flags);
			if (rc < 0)
				CT_ERROR("'%s' item %d process err: %s\n",
					 rootpath, i, strerror(-rc));
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

	strncpy(cmd_name, basename(argv[0]), sizeof(cmd_name));
	rc = ct_parseopts(argc, argv);
	if (rc != 0)
		return -rc;

	switch (opt.o_action) {
	case CA_IMPORT:
		rc = ct_import_recurse(opt.o_src);
		break;
	case CA_REBIND:
		rc = ct_rebind();
		break;
	case CA_MAXSEQ:
		rc = ct_max_sequence();
		break;
	default:
		rc = ct_daemon();
		break;
	}

	if (opt.o_action != CA_MAXSEQ)
		CT_TRACE("%s(%d) finished, errs: %d major, %d minor,"
			     " rc=%d\n",
			     argv[0], getpid(), opt.err_major,
			     opt.err_minor, rc);

	return -rc;
}

