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

#include <lustre/lustreapi.h>
#include <lustre/lustre_idl.h>
#include "lustreapi_internal.h"

/**
 * An Opaque data type abstracting the layout of a Lustre file.
 *
 * Duplicate the fields we care about from struct lov_user_md_v3.
 * Deal with v1 versus v3 format issues only when we read or write
 * files.
 */
struct llapi_layout {
	uint32_t	llot_magic;
	uint64_t	llot_pattern;
	uint64_t	llot_stripe_size;
	uint64_t	llot_stripe_count;
	uint64_t	llot_stripe_offset;
	/** Indicates if llot_objects array has been initialized. */
	bool		llot_objects_are_valid;
	/* Add 1 so user always gets back a null terminated string. */
	char		llot_pool_name[LOV_MAXPOOLNAME + 1];
	struct		lov_user_ost_data_v1 llot_objects[0];
};

/**
 * Byte-swap the fields of struct lov_user_md.
 *
 * XXX Rather than duplicating swabbing code here, we should eventually
 * refactor the needed functions in lustre/ptlrpc/pack_generic.c
 * into a library that can be shared between kernel and user code.
 */
static void
llapi_layout_swab_lov_user_md(struct lov_user_md_v3 *lum)
{
	int i;
	struct lov_user_md_v1 *lumv1 = (struct lov_user_md_v1 *)lum;
	struct lov_user_ost_data *lod;

	__swab32s(&lum->lmm_magic);
	__swab32s(&lum->lmm_pattern);
	__swab32s(&lum->lmm_stripe_size);
	__swab16s(&lum->lmm_stripe_count);
	__swab16s(&lum->lmm_stripe_offset);

	if (lum->lmm_magic == LOV_MAGIC_V1)
		lod = lumv1->lmm_objects;
	else
		lod = lum->lmm_objects;

	for (i = 0; i < lum->lmm_stripe_count; i++)
		__swab32s(&(lod[i].l_ost_idx));
}

/**
 * Allocate storage for a llapi_layout with \a num_stripes stripes.
 *
 * \param[in] num_stripes	number of stripes in new layout
 *
 * \retval	valid pointer if allocation succeeds
 * \retval	NULL if allocation fails
 */
static struct llapi_layout *__llapi_layout_alloc(unsigned int num_stripes)
{
	struct llapi_layout *layout = NULL;
	size_t size = sizeof(*layout) +
		(num_stripes * sizeof(layout->llot_objects[0]));

	if (num_stripes > LOV_MAX_STRIPE_COUNT)
		errno = EINVAL;
	else
		layout = calloc(1, size);

	return layout;
}

/**
 * Copy the data from a lov_user_md_v3 to a newly allocated llapi_layout.
 *
 * The caller is responsible for freeing the returned pointer.
 *
 * \param[in] lum	LOV user metadata structure to copy data from
 *
 * \retval		valid llapi_layout pointer on success
 * \retval		NULL if memory allocation fails
 */
static struct llapi_layout *
llapi_layout_from_lum(const struct lov_user_md_v3 *lum)
{
	struct llapi_layout *layout;
	size_t objects_sz;
	int stripe_count = lum->lmm_stripe_count;

	if (stripe_count == -1)
		stripe_count = LOV_MAX_STRIPE_COUNT;

	objects_sz = stripe_count * sizeof(lum->lmm_objects[0]);

	layout = __llapi_layout_alloc(stripe_count);
	if (layout == NULL)
		return NULL;

	layout->llot_magic = LLAPI_LAYOUT_MAGIC;

	if (lum->lmm_pattern == 1)
		layout->llot_pattern = LLAPI_LAYOUT_RAID0;
	else
		/* Lustre only supports RAID0 for now. */
		layout->llot_pattern = lum->lmm_pattern;

	if (lum->lmm_stripe_size == 0)
		layout->llot_stripe_size = LLAPI_LAYOUT_DEFAULT;
	else
		layout->llot_stripe_size = lum->lmm_stripe_size;

	if (lum->lmm_stripe_count == -1)
		layout->llot_stripe_count = LLAPI_LAYOUT_WIDE;
	else if (lum->lmm_stripe_count == 0)
		layout->llot_stripe_count = LLAPI_LAYOUT_DEFAULT;
	else
		layout->llot_stripe_count = lum->lmm_stripe_count;

	/* Don't copy lmm_stripe_offset: it is always zero
	 * when reading attributes. */

	if (lum->lmm_magic == LOV_USER_MAGIC_V3) {
		snprintf(layout->llot_pool_name, sizeof(layout->llot_pool_name),
			 "%s", lum->lmm_pool_name);
		memcpy(layout->llot_objects, lum->lmm_objects, objects_sz);
	} else {
		const struct lov_user_md_v1 *lum_v1;
		lum_v1 = (struct lov_user_md_v1 *)lum;
		memcpy(layout->llot_objects, lum_v1->lmm_objects, objects_sz);
	}
	layout->llot_objects_are_valid = 1;

	return layout;
}

/**
 * Copy the data from a llapi_layout to a newly allocated lov_user_md_v3.
 *
 * The caller is responsible for freeing the returned pointer.
 *
 * The current version of this API doesn't support specifying the OST
 * index of arbitrary stripes, only stripe 0 via lmm_stripe_offset.
 * There is therefore no need to copy the lmm_objects array.
 *
 * \param[in] layout	the layout to copy from
 *
 * \retval	valid lov_user_md_v3 pointer on success
 * \retval	NULL if memory allocation fails
 */
static struct lov_user_md_v3 *
llapi_layout_to_lum(const struct llapi_layout *layout)
{
	struct lov_user_md_v3 *lum;
	size_t lum_size;

	lum_size = lov_user_md_size(0, LOV_USER_MAGIC_V3);
	lum = malloc(lum_size);
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

/**
 * Get the parent directory of a path.
 *
 * \param[in] path	path to get parent of
 * \param[out] buf	buffer in which to store parent path
 * \param[in] size	size in bytes of buffer \a buf
 */
static void get_parent_dir(const char *path, char *buf, size_t size)
{
	char *p = NULL;

	strncpy(buf, path, size);
	p = strrchr(buf, '/');

	if (p != NULL)
		*p = '\0';
	else if (size >= 2)
		strncpy(buf, ".", 2);
}

/**
 * Substitute unspecified attribute values in \a recipient with
 * values from \a donor.
 *
 * \param[in] donor	layout to inherit values from
 * \param[in] recipient	layout to receive inherited values
 */
static void inherit_layout_attributes(const struct llapi_layout *donor,
					struct llapi_layout *recipient)
{
	if (recipient->llot_pattern == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_pattern = donor->llot_pattern;
	if (recipient->llot_stripe_size == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_stripe_size = donor->llot_stripe_size;
	if (recipient->llot_stripe_count == LLAPI_LAYOUT_DEFAULT)
		recipient->llot_stripe_count = donor->llot_stripe_count;
}

static bool is_fully_specified(const struct llapi_layout *layout)
{
	return (layout->llot_pattern != LLAPI_LAYOUT_DEFAULT &&
		layout->llot_stripe_size != LLAPI_LAYOUT_DEFAULT &&
		layout->llot_stripe_count != LLAPI_LAYOUT_DEFAULT);
}

/**
 * Allocate and initialize a new layout.
 *
 * \retval	valid llapi_layout pointer on success
 * \retval	NULL if memory allocation fails
 */
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
 * Get the striping layout for the file referenced by file descriptor \a fd.
 *
 * If the filesystem does not support the "lustre." xattr namespace, the
 * file must be on a non-Lustre filesystem, so set errno to ENOTTY per
 * convention.  If the file has no "lustre.lov" data, the file will
 * inherit default values, so return a default layout.
 *
 * \param[in] fd	open file descriptor
 *
 * \retval	valid llapi_layout pointer on success
 * \retval	NULL if an error occurs
 */
struct llapi_layout *llapi_layout_by_fd(int fd)
{
	size_t lum_len;
	struct lov_user_md_v3 *lum = NULL;
	struct llapi_layout *layout = NULL;

	lum_len = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_MAGIC_V3);
	lum = malloc(lum_len);
	if (lum == NULL)
		return NULL;

	if (fgetxattr(fd, XATTR_LUSTRE_LOV, lum, lum_len) < 0) {
		if (errno == EOPNOTSUPP)
			errno = ENOTTY;
		else if (errno == ENODATA)
			layout = llapi_layout_alloc();
	} else {
		if (lum->lmm_magic == __swab32(LOV_MAGIC_V1) ||
		    lum->lmm_magic == __swab32(LOV_MAGIC_V3))
			llapi_layout_swab_lov_user_md(lum);

		layout = llapi_layout_from_lum(lum);
	}

	free(lum);
	return layout;
}

/**
 * Get the striping layout for the file at \a path.
 *
 * \retval	valid llapi_layout pointer on success
 * \retval	NULL if an error occurs
 */
struct llapi_layout *llapi_layout_by_path(const char *path)
{
	struct llapi_layout *layout = NULL;
	int fd;

	fd = open(path, O_RDONLY);
	if (fd >= 0) {
		layout = llapi_layout_by_fd(fd);
		int tmp = errno;
		close(fd);
		errno = tmp;
	}
	return layout;
}

/**
 * Get the expected striping layout for a file at \a path.
 *
 * Substitute expected inherited attribute values for unspecified
 * attributes.  Unspecified attributes may belong to directories and
 * never-written-to files, and indicate that default values will be
 * assigned when files are created or first written to.  Unspecified
 * attributes normally have the value LLAPI_LAYOUT_DEFAULT.
 *
 * The complete \a path need not refer to an existing file or directory,
 * but some leading portion of it must reside within a lustre filesystem.
 * A use case for this interface would be to obtain the literal striping
 * values that would be assigned to a new file in a given directory.
 *
 * \param[in] path	path for which to get the expected layout
 *
 * \retval	valid llapi_layout pointer on success
 * \retval	NULL if an error occurs
 */
struct llapi_layout *llapi_layout_expected(const char *path)
{
	struct llapi_layout	*path_layout;
	struct llapi_layout	*donor_layout;
	char			donor_path[PATH_MAX];
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

	if (is_fully_specified(path_layout))
		return path_layout;

	rc = stat(path, &st);
	if (rc < 0 && errno != ENOENT) {
		llapi_layout_free(path_layout);
		return NULL;
	}

	/* If path is a not a directory or doesn't exist, inherit unspecified
	 * attributes from parent directory. */
	if ((rc == 0 && !S_ISDIR(st.st_mode)) ||
	    (rc < 0 && errno == ENOENT)) {
		get_parent_dir(path, donor_path, sizeof(donor_path));
		donor_layout = llapi_layout_by_path(donor_path);
		if (donor_layout != NULL) {
			inherit_layout_attributes(donor_layout, path_layout);
			llapi_layout_free(donor_layout);
			if (is_fully_specified(path_layout))
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

	inherit_layout_attributes(donor_layout, path_layout);
	llapi_layout_free(donor_layout);

	return path_layout;
}

/**
 * Get the layout for the file with FID \a fidstr in filesystem \a lustre_dir.
 *
 * \param[in] lustre_dir	path within Lustre filesystem containing \a fid
 * \param[in] fid		Lustre identifier of file to get layout for
 *
 * \retval	valid llapi_layout pointer on success
 * \retval	NULL if an error occurs
 */
struct llapi_layout *llapi_layout_by_fid(const char *lustre_dir,
					 const lustre_fid *fid)
{
	int fd;
	int saved_msg_level = llapi_msg_get_level();
	int tmp;
	struct llapi_layout *layout = NULL;

	/* Prevent llapi internal routines from writing to console
	 * while executing this function, then restore previous message
	 * level. */
	llapi_msg_set_level(LLAPI_MSG_OFF);
	fd = llapi_open_by_fid(lustre_dir, fid, O_RDONLY);
	llapi_msg_set_level(saved_msg_level);

	if (fd < 0)
		return NULL;

	layout = llapi_layout_by_fd(fd);
	tmp = errno;
	close(fd);
	errno = tmp;
	return layout;
}

/** * Free memory allocated for \a layout. */
void llapi_layout_free(struct llapi_layout *layout)
{
	free(layout);
}

/**
 * Get the stripe count of \a layout.
 *
 * \param[in] layout	layout to get stripe count from
 * \param[out] count	integer to store stripe count in
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
 */
int llapi_layout_stripe_count_get(const struct llapi_layout *layout,
				  uint64_t *count)
{
	if (layout == NULL || count == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*count = layout->llot_stripe_count;
	return 0;
}

/*
 * The llapi_layout API functions have these extra validity checks since
 * they use intuitively named macros to denote special behavior, whereas
 * the old API uses 0 and -1.
 */

static int llapi_layout_stripe_count_is_valid(uint64_t stripe_count)
{
	return (stripe_count == LLAPI_LAYOUT_DEFAULT ||
		stripe_count == LLAPI_LAYOUT_WIDE ||
		(stripe_count != 0 && stripe_count != -1 &&
		 llapi_stripe_count_is_valid(stripe_count)));
}

static int llapi_layout_stripe_size_is_valid(uint64_t stripe_size)
{
	return (stripe_size == LLAPI_LAYOUT_DEFAULT ||
		(stripe_size != 0 &&
		 llapi_stripe_size_is_valid(stripe_size) &&
		 !llapi_stripe_size_is_too_big(stripe_size)));
}

/**
 * Set the stripe count of \a layout.
 *
 * \param[in] layout	layout to set stripe count in
 * \param[in] count	value to be set
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
 */
int llapi_layout_stripe_count_set(struct llapi_layout *layout,
				  uint64_t count)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	     !llapi_layout_stripe_count_is_valid(count)) {
		errno = EINVAL;
		return -1;
	}
	layout->llot_stripe_count = count;
	return 0;
}

/**
 * Get the stripe size of \a layout.
 *
 * \param[in] layout	layout to get stripe size from
 * \param[out] size	integer to store stripe size in
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
 */
int llapi_layout_stripe_size_get(const struct llapi_layout *layout,
				 uint64_t *size)
{
	if (layout == NULL || size == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*size = layout->llot_stripe_size;
	return 0;
}

/**
 * Set the stripe size of \a layout.
 *
 * \param[in] layout	layout to set stripe size in
 * \param[in] size	value to be set
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
 */
int llapi_layout_stripe_size_set(struct llapi_layout *layout,
				 uint64_t size)
{
	if (layout == NULL || layout->llot_magic != LLAPI_LAYOUT_MAGIC ||
	    !llapi_layout_stripe_size_is_valid(size)) {
		errno = EINVAL;
		return -1;
	}

	layout->llot_stripe_size = size;
	return 0;
}

/**
 * Get the RAID pattern of \a layout.
 *
 * \param[in] layout	layout to get pattern from
 * \param[out] pattern	integer to store pattern in
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
 */
int llapi_layout_pattern_get(const struct llapi_layout *layout,
			     uint64_t *pattern)
{
	if (layout == NULL || pattern == NULL ||
	    layout->llot_magic != LLAPI_LAYOUT_MAGIC) {
		errno = EINVAL;
		return -1;
	}
	*pattern = layout->llot_pattern;
	return 0;
}

/**
 * Set the RAID pattern of \a layout.
 *
 * \param[in] layout	layout to set pattern in
 * \param[in] pattern	value to be set
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid or RAID pattern
 *		is unsupported
 */
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
 * Set the OST index of stripe number \a stripe_number to \a ost_index.
 *
 * The index may only be set for stripe number 0 for now.
 *
 * \param[in] layout		layout to set OST index in
 * \param[in] stripe_number	stripe number to set index for
 * \param[in] ost_index		the index to set
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid or an unsupported stripe number
 *		was specified
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
 * Get the OST index associated with stripe \a stripe_number.
 *
 * Stripes are indexed starting from zero.
 *
 * \param[in] layout		layout to get index from
 * \param[in] stripe_number	stripe number to get index for
 * \param[out] index		integer to store index in
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
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
 *
 * Get the poolname of layout \a layout.
 *
 * \param[in] layout	layout to get poolname from
 * \param[out] dest	buffer to store poolname in
 * \param[in] n		size in bytes of buffer \a dest
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid
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
 * Set the name of the poolname of layout \a layout.
 *
 * \param[in] layout	layout to set poolname in
 * \param[in] pool_name	pool name to set
 *
 * \retval	0 on success
 * \retval	-1 if arguments are invalid or pool name is too long
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

/**
 * Open a file with a given \a layout.
 *
 * If \a layout is NULL this function acts as a simple wrapper for
 * open().  By convention, ENOTTY is returned in errno if \a path
 * refers to a non-Lustre file.
 *
 * \param[in] path	name of the file to open
 * \param[in] flags	open() flags
 * \param[in] mode	permissions to create new file with
 * \param[in] layout	layout to create new file with
 *
 * \retval		non-negative file descriptor on successful open
 * \retval		-1 if an error occurred
 */
int llapi_layout_file_open(const char *path, int flags, mode_t mode,
			   const struct llapi_layout *layout)
{
	int fd;
	int tmp_errno = errno;
	struct lov_user_md_v3 *lum;

	if (path == NULL ||
	    (layout != NULL && layout->llot_magic != LLAPI_LAYOUT_MAGIC)) {
		errno = EINVAL;
		return -1;
	}

	/* Object creation must be postponed until after layout attributes
	 * have been applied. */
	if (layout != NULL && (flags & O_CREAT))
		flags |= O_LOV_DELAY_CREATE;

	fd = open(path, flags, mode);

	if (layout == NULL || fd == -1)
		return fd;

	lum = llapi_layout_to_lum(layout);
	tmp_errno = errno;

	if (lum == NULL) {
		close(fd);
		errno = tmp_errno;
		return -1;
	}

	if (fsetxattr(fd, XATTR_LUSTRE_LOV, lum, sizeof(*lum), 0) < 0) {
		tmp_errno = errno == EOPNOTSUPP ? ENOTTY : errno;
		close(fd);
		fd = -1;
	}
	free(lum);
	errno = tmp_errno;

	return fd;
}

/**
 * Create a file with a given \a layout.
 *
 * Force O_CREAT and O_EXCL flags on so caller is assured that file was
 * created with the given \a layout on successful function return.
 *
 * \param[in] path	name of the file to open
 * \param[in] flags	open() flags
 * \param[in] mode	permissions to create new file with
 * \param[in] layout	layout to create new file with
 *
 * \retval		non-negative file descriptor on successful open
 * \retval		-1 if an error occurred
 */
int llapi_layout_file_create(const char *path, int flags, int mode,
			     const struct llapi_layout *layout)
{
	return llapi_layout_file_open(path, flags|O_CREAT|O_EXCL, mode, layout);
}
