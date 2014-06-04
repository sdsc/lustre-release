/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * lustre/utils/liblustreapi_layout.c
 *
 * lustreapi library for layout calls for interacting with the layout of
 * Lustre files while hiding details of the internal data structures
 * from the user.
 *
 * Author: Ned Bass <bass6@llnl.gov>
 */

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <sys/xattr.h>

#include <libcfs/libcfs.h>
#include <lustre/lustreapi.h>
#include "lustreapi_internal.h"

/**
 * llapi_layout - an opaque data type abstracting the layout of a
 * Lustre file.
 *
 * Duplicate the fields we care about from struct lov_user_md_v3.
 * Deal with v1 versus v3 format issues only when we read or write
 * files.  Default to v3 format for new files.
 */
struct llapi_layout {
	uint32_t	llot_magic;
	uint64_t	llot_pattern;
	uint64_t	llot_stripe_size;
	uint64_t	llot_stripe_count;
	uint64_t	llot_stripe_offset;
	/** Indicates if llot_objects array has been initialized. */
	bool		llot_objects_are_valid;
	/* Add 1 so user always gets back a NULL-terminated string. */
	char	llot_pool_name[LOV_MAXPOOLNAME + 1];
	struct	lov_user_ost_data_v1 llot_objects[0];
};

/**
 * Swab parts of a struct lov_user_md that are relevant to
 * struct llapi_layout.
 *
 * XXX Rather than duplicating swabbing code here, we should eventually
 * refactor the needed functions in lustre/ptlrpc/pack_generic.c
 * into a library that can be shared between kernel and user code.
 */
static void
__llapi_layout_swab_lov_user_md(struct lov_user_md_v3 *lum)
{
	int i;
	struct lov_user_md_v1 *lumv1 = (struct lov_user_md_v1 *)lum;
	struct lov_user_ost_data *lod;

	__swab32s(&lum->lmm_magic);
	__swab32s(&lum->lmm_pattern);
	__swab32s(&lum->lmm_stripe_size);
	__swab16s(&lum->lmm_stripe_count);
	__swab16s(&lum->lmm_stripe_offset);

	if (lum->lmm_magic == LOV_USER_MAGIC_V1)
		lod = lumv1->lmm_objects;
	else
		lod = lum->lmm_objects;

	for (i = 0; i < lum->lmm_stripe_count; i++)
		__swab32s(&(lod[i].l_ost_idx));
}

/**
 * Private helper to allocate storage for a lustre_layout with
 * \a num_stripes stripes.
 */
static struct llapi_layout *__llapi_layout_alloc(int num_stripes)
{
	struct llapi_layout *layout;
	size_t size = sizeof(*layout) +
		(num_stripes * sizeof(layout->llot_objects[0]));

	layout = calloc(1, size);
	return layout;
}

/**
 * Private helper to copy the data from a lov_user_md_v3 to a
 * newly allocated lustre_layout.
 */
static struct llapi_layout *
__llapi_layout_from_lum(const struct lov_user_md_v3 *lum)
{
	struct llapi_layout *layout;
	const struct lov_user_md_v1 *lum_v1 = (struct lov_user_md_v1 *)lum;
	size_t objects_sz;

	objects_sz = lum->lmm_stripe_count * sizeof(lum->lmm_objects[0]);

	layout = __llapi_layout_alloc(lum->lmm_stripe_count);
	if (layout == NULL)
		return NULL;

	layout->llot_magic = LLAPI_LAYOUT_MAGIC;

	if (lum->lmm_pattern == 0)
		layout->llot_pattern = LLAPI_LAYOUT_DEFAULT;
	else if (lum->lmm_pattern == 1)
		layout->llot_pattern = LLAPI_LAYOUT_RAID0;
	else
		layout->llot_pattern = lum->lmm_pattern;

	if (lum->lmm_stripe_size == 0)
		layout->llot_stripe_size = LLAPI_LAYOUT_DEFAULT;
	else
		layout->llot_stripe_size = lum->lmm_stripe_size;

	if (lum->lmm_stripe_count == 0)
		layout->llot_stripe_count = LLAPI_LAYOUT_DEFAULT;
	else if (lum->lmm_stripe_count == -1)
		layout->llot_stripe_count = LLAPI_LAYOUT_WIDE;
	else
		layout->llot_stripe_count = lum->lmm_stripe_count;

	/* Don't copy lmm_stripe_offset: it is always zero
	 * when reading attributes. */

	if (lum->lmm_magic == LOV_USER_MAGIC_V3) {
		snprintf(layout->llot_pool_name, sizeof(layout->llot_pool_name),
			 "%s", lum->lmm_pool_name);
		memcpy(layout->llot_objects, lum->lmm_objects, objects_sz);
	} else {
		memcpy(layout->llot_objects, lum_v1->lmm_objects, objects_sz);
	}
	layout->llot_objects_are_valid = 1;

	return layout;
}

/**
 * Private helper to copy the data from a lustre_layout to a
 * newly allocated lov_user_md_v3.  The current version of this API
 * doesn't support specifying the OST index of arbitrary stripes, only
 * stripe 0 via lmm_stripe_offset.  Therefore the lmm_objects array is
 * essentially a read-only data structure so it is not copied here.
 */
static struct lov_user_md_v3 *
__llapi_layout_to_lum(const struct llapi_layout *layout)
{
	struct lov_user_md_v3 *lum;

	lum = malloc(sizeof(*lum));
	if (lum == NULL)
		return NULL;

	lum->lmm_magic = LOV_USER_MAGIC_V3;

	if (layout->llot_pattern == LLAPI_LAYOUT_DEFAULT)
		lum->lmm_pattern = 0;
	else if (layout->llot_pattern == LLAPI_LAYOUT_RAID0)
		lum->lmm_pattern = 1;
	else
		lum->lmm_pattern = layout->llot_pattern;

	if (layout->llot_stripe_size == LLAPI_LAYOUT_DEFAULT)
		lum->lmm_stripe_size = 0;
	else
		lum->lmm_stripe_size = layout->llot_stripe_size;

	if (layout->llot_stripe_count == LLAPI_LAYOUT_DEFAULT)
		lum->lmm_stripe_count = 0;
	else if (layout->llot_stripe_count == LLAPI_LAYOUT_WIDE)
		lum->lmm_stripe_count = -1;
	else
		lum->lmm_stripe_count = layout->llot_stripe_count;

	if (layout->llot_stripe_offset == LLAPI_LAYOUT_DEFAULT)
		lum->lmm_stripe_offset	= -1;
	else
		lum->lmm_stripe_offset	= layout->llot_stripe_offset;

	strncpy(lum->lmm_pool_name, layout->llot_pool_name,
		sizeof(lum->lmm_pool_name));

	return lum;
}

/* Get parent directory of a path. */
void __get_parent_dir(const char *path, char *buf, size_t size)
{
	char *p = NULL;

	strncpy(buf, path, size);
	p = strrchr(buf, '/');

	if (p != NULL) {
		if (p == buf)
			snprintf(buf, 2, "/");
		else
			*p = '\0';
	} else {
		snprintf(buf, 2, ".");
	}
}

/*
 * Substitute unspecified attribute values in \a recipient with
 * values from \a donor.
 */
static void __inherit_layout_attributes(const struct llapi_layout *donor,
					struct llapi_layout *recipient)
{
	if (recipient->llot_pattern == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_pattern = donor->llot_pattern;
	if (recipient->llot_stripe_size == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_stripe_size = donor->llot_stripe_size;
	if (recipient->llot_stripe_count == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_stripe_count = donor->llot_stripe_count;
}

static bool __is_fully_specified(const struct llapi_layout *layout)
{
	return (layout->llot_pattern != LLAPI_LAYOUT_DEFAULT &&
		layout->llot_stripe_size != LLAPI_LAYOUT_DEFAULT &&
		layout->llot_stripe_count != LLAPI_LAYOUT_DEFAULT);
}

/** Allocate and initialize a new layout. */
struct llapi_layout *llapi_layout_alloc(void)
{
	struct llapi_layout *layout;

	layout = __llapi_layout_alloc(0);
	if (layout == NULL)
		return layout;

	/* Set defaults. */
	layout->llot_magic = LLAPI_LAYOUT_MAGIC;
	layout->llot_pattern = LLAPI_LAYOUT_DEFAULT;
	layout->llot_stripe_size = LLAPI_LAYOUT_DEFAULT;
	layout->llot_stripe_count = LLAPI_LAYOUT_DEFAULT;
	layout->llot_stripe_offset = LLAPI_LAYOUT_DEFAULT;
	layout->llot_objects_are_valid = 0;
	layout->llot_pool_name[0] = '\0';

	return layout;
}

/**
 *  Populate an opaque data type containing the layout for the file
 *  referenced by open file descriptor \a fd.
 */
struct llapi_layout *llapi_layout_by_fd(int fd)
{
	size_t lum_len;
	struct lov_user_md_v3 *lum = NULL;
	struct llapi_layout *layout = NULL;

	lum_len = sizeof(*lum) +
		LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
	lum = malloc(lum_len);
	if (lum == NULL)
		return NULL;

	if (fgetxattr(fd, XATTR_LUSTRE_LOV, lum, lum_len) < 0) {
		/* If the filesystem does not support the "lustre."
		 * xattr namespace, the file must be on a non-lustre
		 * filesystem, so return ENOTTY per convention.
		 * If the file has no "lustre.lov" data, the file
		 * will inherit default values, so return a default
		 * layout. */
		if (errno == EOPNOTSUPP)
			errno = ENOTTY;
		else if (errno == ENODATA)
			layout = llapi_layout_alloc();
	} else {
		if (lum->lmm_magic == __swab32(LOV_USER_MAGIC_V1) ||
		    lum->lmm_magic == __swab32(LOV_USER_MAGIC_V3))
			__llapi_layout_swab_lov_user_md(lum);

		layout = __llapi_layout_from_lum(lum);
	}

	free(lum);
	return layout;
}

/**
 *  Populate an opaque data type containing the layout for the file at \a path.
 */
struct llapi_layout *llapi_layout_by_path(const char *path)
{
	struct llapi_layout *layout = NULL;
	int fd;

	fd = open(path, O_RDONLY);
	if (fd >= 0) {
		layout = llapi_layout_by_fd(fd);
		close(fd);
	}

	printf("%s %lx\n", path, layout ? layout->llot_pattern : 0xdeadbeef);
	return layout;
}

/*
 * Like llapi_layout_by_path(), but substitute expected inherited
 * attribute values for unspecified attributes.  Unspecified
 * attributes normally have the value LLAPI_LAYOUT_DEFAULT.
 */
struct llapi_layout *llapi_layout_expected(const char *path)
{
	struct llapi_layout	*path_layout;
	struct llapi_layout	*donor_layout;
	char		donor_path[PATH_MAX];
	struct stat st;
	int rc;

	path_layout = llapi_layout_by_path(path);
	if (path_layout == NULL) {
		if (errno != ENODATA)
			return NULL;

		path_layout = llapi_layout_alloc();
		if (path_layout == NULL)
			return NULL;
	}

	if (__is_fully_specified(path_layout))
		return path_layout;

	rc = stat(path, &st);
	if (rc < 0) {
		llapi_layout_free(path_layout);
		return NULL;
	}

	/* If path is a not a directory, inherit unspecified attributes
	 * from parent directory. */
	if (!S_ISDIR(st.st_mode)) {
		__get_parent_dir(path, donor_path, sizeof(donor_path));
		donor_layout = llapi_layout_by_path(donor_path);
		if (donor_layout != NULL) {
			__inherit_layout_attributes(donor_layout, path_layout);
			llapi_layout_free(donor_layout);
			if (__is_fully_specified(path_layout))
				return path_layout;
		}
	}

	/* Inherit remaining unspecified attributes from the filesystem root. */
	rc = llapi_search_mounts(path, 0, donor_path, NULL);
	if (rc < 0) {
		llapi_layout_free(path_layout);
		return NULL;
	}
	donor_layout = llapi_layout_by_path(donor_path);
	if (donor_layout == NULL) {
		llapi_layout_free(path_layout);
		return NULL;
	}

	__inherit_layout_attributes(donor_layout, path_layout);
	llapi_layout_free(donor_layout);

	return path_layout;
}

/**
 *  Populate an opaque data type containing layout for the file with
 *  Lustre file identifier string \a fidstr in filesystem \a lustre_dir.
 */
struct llapi_layout *llapi_layout_by_fid(const char *lustre_dir,
					 lustre_fid *fid)
{
	int fd;
	struct llapi_layout *layout = NULL;

	llapi_msg_set_level(LLAPI_MSG_OFF);
	fd = llapi_open_by_fid(lustre_dir, fid, O_RDONLY);

	if (fd < 0)
		return NULL;

	layout = llapi_layout_by_fd(fd);
	close(fd);
	return layout;
}

/** Free memory allocated for \a layout */
void llapi_layout_free(struct llapi_layout *layout)
{
	free(layout);
}

/** Read stripe count of \a layout. */
int llapi_layout_stripe_count_get(const struct llapi_layout *layout, uint64_t *count)
{
	if (layout == NULL || count == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*count = layout->llot_stripe_count;
	return 0;
}

/**
 * The llapi_layout API functions have these extra validity checks since
 * they use intuitively named macros to denote special behavior, whereas
 * the old API uses 0 and -1.
 */

static int __llapi_layout_stripe_count_is_valid(uint64_t stripe_count)
{
	return (stripe_count == LLAPI_LAYOUT_DEFAULT ||
		stripe_count == LLAPI_LAYOUT_WIDE ||
		(stripe_count != 0 && stripe_count != -1 &&
		 llapi_stripe_count_is_valid(stripe_count)));
}

static int __llapi_layout_stripe_size_is_valid(uint64_t stripe_size)
{
	return (stripe_size == LLAPI_LAYOUT_DEFAULT ||
		(stripe_size != 0 &&
		 llapi_stripe_size_is_valid(stripe_size) &&
		 !llapi_stripe_size_is_too_big(stripe_size)));
}

/** Modify stripe count of \a layout. */
int llapi_layout_stripe_count_set(struct llapi_layout *layout, uint64_t stripe_count)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	     !__llapi_layout_stripe_count_is_valid(stripe_count)) {
		errno = EINVAL;
		return -1;
	}
	layout->llot_stripe_count = stripe_count;
	return 0;
}

/** Read the size of each stripe in \a layout. */
int llapi_layout_stripe_size_get(const struct llapi_layout *layout, uint64_t *size)
{
	if (layout == NULL || size == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*size = layout->llot_stripe_size;
	return 0;
}

/** Set the size of each stripe in \a layout. */
int llapi_layout_stripe_size_set(struct llapi_layout *layout,
				 uint64_t stripe_size)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    !__llapi_layout_stripe_size_is_valid(stripe_size)) {
		errno = EINVAL;
		return -1;
	}

	layout->llot_stripe_size = stripe_size;
	return 0;
}

/** Read stripe pattern of \a layout. */
int llapi_layout_pattern_get(const struct llapi_layout *layout, uint64_t *pattern)
{
	if (layout == NULL || pattern == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*pattern = layout->llot_pattern;
	return 0;
}

/** Modify stripe pattern of \a layout. */
int llapi_layout_pattern_set(struct llapi_layout *layout, uint64_t pattern)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	if (pattern != LLAPI_LAYOUT_DEFAULT ||
	    pattern != LLAPI_LAYOUT_RAID0) {
		errno = EOPNOTSUPP;
		return -1;
	}
	layout->llot_pattern = pattern;
	return 0;
}

/**
 * Set the OST index in layout associated with stripe number
 * \a stripe_number to \a ost_index.
 * NB: this only works for stripe_number=0 today.
 */
int llapi_layout_ost_index_set(struct llapi_layout *layout, int stripe_number,
			       uint64_t ost_index)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    !llapi_stripe_index_is_valid(ost_index)) {
		errno = EINVAL;
		return -1;
	}

	if (stripe_number != 0) {
		errno = EOPNOTSUPP;
		return -1;
	}

	layout->llot_stripe_offset = ost_index;
	return 0;
}

/**
 * Return the OST idex associated with stripe \a stripe_number.
 * Stripes are indexed starting from zero.
 */
int llapi_layout_ost_index_get(const struct llapi_layout *layout,
			       uint64_t stripe_number, uint64_t *index)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    stripe_number >= layout->llot_stripe_count ||
	    index == NULL  || layout->llot_objects_are_valid == 0) {
		errno = EINVAL;
		return -1;
	}

	if (layout->llot_objects[stripe_number].l_ost_idx == -1)
		*index = LLAPI_LAYOUT_DEFAULT;
	else
		*index = layout->llot_objects[stripe_number].l_ost_idx;
	return 0;
}

/**
 * Store a string into \a dest which contains the name of the pool of OSTs
 * on which file objects in \a layout will be stored.
 */
int llapi_layout_pool_name_get(const struct llapi_layout *layout, char *dest,
			       size_t n)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    dest == NULL) {
		errno = EINVAL;
		return -1;
	}
	strncpy(dest, layout->llot_pool_name, n);
	return 0;
}

/**
 * Set the name of the pool of OSTs on which file objects in \a layout
 * will be stored to \a pool_name.
 */
int llapi_layout_pool_name_set(struct llapi_layout *layout,
			       const char *pool_name)
{
	char *ptr;

	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    pool_name == NULL) {
		errno = EINVAL;
		return -1;
	}

	/* Strip off any 'fsname.' portion. */
	ptr = strchr(pool_name, '.');
	if (ptr != NULL)
		pool_name = ptr + 1;

	if (strlen(pool_name) > LOV_MAXPOOLNAME) {
		errno = EINVAL;
		return -1;
	}

	strncpy(layout->llot_pool_name, pool_name,
		sizeof(layout->llot_pool_name));
	return 0;
}

/* Test whether layout names an OST pool that doesn't exist. */
bool llapi_layout_pool_absent(const struct llapi_layout *layout, char *fsname)
{
	char pool[LOV_MAXPOOLNAME + 1];

	if (layout == NULL || strlen(layout->llot_pool_name) == 0)
		return false;

	/* Make a copy due to const layout. */
	strncpy(pool, layout->llot_pool_name, sizeof(pool));

	return (llapi_search_ost(fsname, pool, NULL) < 0);
}

/**
 * Open a file with the specified \a layout with the name \a path
 * using permissions in \a mode and open() \a flags.  Return an open
 * file descriptor for the file.
 */
int llapi_layout_file_open(const char *path, int flags, mode_t mode,
			   const struct llapi_layout *layout)
{
	int fd;
	int rc;
	char fsname[PATH_MAX + 1] = "";

	if (path == NULL ||
	    (layout != NULL && layout->llot_magic != LLAPI_LAYOUT_MAGIC)) {
		errno = EINVAL;
		return -1;
	}

	/* Object creation must be postponed until after layout attributes
	 * have been applied. */
	if (layout != NULL && (flags & O_CREAT))
		flags |= O_LOV_DELAY_CREATE;

	/* Verify parent directory belongs to a lustre filesystem, and
	 * get filesystem name for llapi_search_ost(). */
	rc = llapi_search_fsname(path, fsname);
	if (rc < 0 || (strlen(fsname) == 0)) {
		errno = ENOTTY;
		return -1;
	}

	/* Verify pool exists, if layout has one. */
	if (llapi_layout_pool_absent(layout, fsname)) {
		errno = EINVAL;
		return -1;
	}

	fd = open(path, flags, mode);
	if (fd < 0)
		return -1;

	if (layout != NULL) {
		struct lov_user_md_v3 *lum = __llapi_layout_to_lum(layout);

		if (lum == NULL) {
			errno = ENOMEM;
			close(fd);
			fd = -1;
		}

		if (fd != -1) {
			if (fsetxattr(fd, XATTR_LUSTRE_LOV, (void *)lum,
				      sizeof(*lum), 0) < 0)  {
				close(fd);
				fd = -1;
			}
		}
		free(lum);
	}
	return fd;
}

/**
 * Create a file with the specified \a layout with the name \a path
 * using permissions in \a mode and open() \a flags.  Return an open
 * file descriptor for the new file.
 */
int llapi_layout_file_create(const char *path, int flags, int mode,
			     const struct llapi_layout *layout)
{
	return llapi_layout_file_open(path, flags|O_CREAT|O_EXCL, mode, layout);
}
