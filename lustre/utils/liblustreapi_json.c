/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2014 Intel Corporation.
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
 * lustre/utils/liblustreapi_json.c
 *
 * lustreapi library for json calls
 *
 * Author: Michael MacDonald <michael.macdonald@intel.com>
 * Author: Bruno Faccini <bruno.faccini@intel.com>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <malloc.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/types.h>
#ifdef HAVE_LINUX_UNISTD_H
#include <linux/unistd.h>
#else
#include <unistd.h>
#endif

#include <liblustre.h>
#include <lustre/lustreapi.h>

/** Quick-n'-dirty JSON string escape routine.
 * \param[out]	out_string	JSON-escaped string, allocated here
 * \param[in]	in_string	Unescaped string
 *
 * \retval	0 on success.
 * \retval	-errno on error.
 *
 * http://json.org/
 * http://www.ietf.org/rfc/rfc4627.txt (section 2.5)
 */
int llapi_json_escape_string(char **out_string, char *in_string)
{
	int	i;
	char	escape_chars[] = {'\b', '\f', '\n', '\r', '\t', '"', '\\',
				  '\0'};
	char	*escaped_chars[] = {"\\\\b", "\\\\f", "\\\\n", "\\\\r",
				    "\\\\t", "\\\\\"", "\\\\\\\\"};
	char	*src = in_string;
	char	*idx, *dst, *tmp;
	char	*escaped_string;
	size_t	tmp_len, escaped_length = strlen(in_string);

	/* add up the extra space needed for the escapes */
	while (*src) {
		idx = strchr(escape_chars, *src);
		if (idx != NULL) {
			tmp = escaped_chars[idx - escape_chars];
			escaped_length += strlen(tmp);
		}
		src++;
	}

	escaped_string = calloc(1, escaped_length + 1);
	if (escaped_string == NULL)
		return -ENOMEM;

	src = in_string;
	dst = escaped_string;
	for (i = 0; *src && i <= escaped_length; i++) {
		idx = strchr(escape_chars, *src);
		if (idx != NULL) {
			tmp = escaped_chars[idx - escape_chars];
			tmp_len = strlen(tmp);
			memcpy(dst, tmp, tmp_len);
			dst += tmp_len;
			++src;
		} else {
			*dst = *src;
			++src;
			++dst;
		}
	}

	*dst = '\0';

	*out_string = escaped_string;

	return 0;
}

/** Write a list of JSON items to a filehandle.
 * \param	json_items	list of JSON items to be written
 * \param	fp		open filehandle to use for write
 *
 * \retval	0 on success.
 * \retval	-errno on error.
 */
int llapi_json_write_list(struct llapi_json_item_list **json_items, FILE *fp)
{
	int				i;
	char				*escaped_string = NULL;
	struct llapi_json_item_list	*list;
	struct llapi_json_item		*item;

	if (json_items == NULL || *json_items == NULL)
		return -EINVAL;

	list = *json_items;
	item = list->items;

	if (fprintf(fp, "{") < 0)
		return -errno;
	for (i = 0; i < list->item_count; i++) {
		if (item == NULL) {
			llapi_err_noerrno(LLAPI_MSG_ERROR,
					  "%d json items but %d is NULL!",
					  list->item_count, i);
			/* Don't bomb out here so that we still emit
			 * valid JSON. */
			break;
		}

		if (fprintf(fp, "\"%s\": ", item->key) < 0)
			return -errno;
		switch (item->type) {
		case LLAPI_JSON_INTEGER:
			if (fprintf(fp, "%d", item->value.integer) < 0)
				return -errno;
			break;
		case LLAPI_JSON_BIGNUM:
			if (fprintf(fp, LPU64, item->value.u64) < 0)
				return -errno;
			break;
		case LLAPI_JSON_REAL:
			if (fprintf(fp, "%f", item->value.real) < 0)
				return -errno;
			break;
		case LLAPI_JSON_STRING:
			if (llapi_json_escape_string(&escaped_string,
					       item->value.string) < 0)
				return -errno;

			if (fprintf(fp, "\"%s\"", escaped_string) < 0)
				if (escaped_string != NULL)
					free(escaped_string);
				return -errno;

			if (escaped_string != NULL)
				free(escaped_string);
			break;
		default:
			llapi_err_noerrno(LLAPI_MSG_ERROR,
				    "Invalid item type: %d", item->type);
			/* Ensure valid JSON */
			if (fprintf(fp, "\"\"") < 0)
				return -errno;
			break;
		}

		if (i < list->item_count - 1)
			if (fprintf(fp, ", ") < 0)
				return -errno;

		item = item->next;
	}
	if (fprintf(fp, "}\n") < 0)
		return -errno;

	return 0;
}

/** Create a list to hold JSON items.
 * \param[out]	json_items	Item list handle, allocated here
 *
 * \retval	0 on success.
 * \retval	-errno on error.
 */
int llapi_json_init_list(struct llapi_json_item_list **json_items)
{
	struct llapi_json_item_list	*new_list;

	new_list = calloc(1, sizeof(*new_list));
	if (new_list == NULL)
		return -ENOMEM;

	new_list->item_count = 0;

	*json_items = new_list;

	return 0;
}

/** Deallocate a list of JSON items.
 * \param	json_items	Item list handle, deallocated here
 *
 * \retval	0 on success.
 * \retval	-errno on error.
 */
int llapi_json_destroy_list(struct llapi_json_item_list **json_items)
{
	int				i;
	struct llapi_json_item_list	*list;
	struct llapi_json_item		*cur_item;
	struct llapi_json_item		*last_item;

	if (json_items == NULL || *json_items == NULL)
		return -EINVAL;

	list = *json_items;
	cur_item = list->items;

	for (i = 0; i < list->item_count; i++) {
		if (cur_item == NULL) {
			llapi_err_noerrno(LLAPI_MSG_ERROR,
					  "%d json items but %d is NULL!",
					  list->item_count, i);
			return -EINVAL;
		}

		if (cur_item->key != NULL)
			free(cur_item->key);

		if (cur_item->type == LLAPI_JSON_STRING
		    && cur_item->value.string != NULL)
			free(cur_item->value.string);

		last_item = cur_item;
		cur_item = last_item->next;
		free(last_item);
	}

	free(list);
	*json_items = NULL;

	return 0;
}

/** Add an item to a list of JSON items.
 * \param	json_items	Item list handle
 * \param	key		Item key name
 * \param	type		Item key type
 * \param	val		Item key value
 *
 * \retval	0 on success.
 * \retval	-errno on error.
 */
int llapi_json_add_item(struct llapi_json_item_list **json_items,
			char *key, __u32 type, void *val)
{
	struct llapi_json_item_list	*list;
	struct llapi_json_item		*new_item;

	if (json_items == NULL || *json_items == NULL)
		return -EINVAL;

	if (val == NULL)
		return -EINVAL;

	list = *json_items;

	new_item = calloc(1, sizeof(*new_item));
	if (new_item == NULL)
		return -ENOMEM;

	new_item->key = calloc(1, strlen(key) + 1);
	if (new_item->key == NULL)
		return -ENOMEM;

	strncpy(new_item->key, key, strlen(key));
	new_item->type = type;
	new_item->next = NULL;

	switch (new_item->type) {
	case LLAPI_JSON_INTEGER:
		new_item->value.integer = *(int *)val;
		break;
	case LLAPI_JSON_BIGNUM:
		new_item->value.u64 = *(__u64 *)val;
		break;
	case LLAPI_JSON_REAL:
		new_item->value.real = *(double *)val;
		break;
	case LLAPI_JSON_STRING:
		new_item->value.string = calloc(1, strlen((char *)val) + 1);
		if (new_item->value.string == NULL)
			return -ENOMEM;
		strncpy(new_item->value.string,
			(char *)val, strlen((char *)val));
		break;
	default:
		llapi_err_noerrno(LLAPI_MSG_ERROR, "Unknown JSON type: %d",
				  new_item->type);
		return -EINVAL;
	}

	if (list->item_count == 0) {
		list->items = new_item;
	} else {
		new_item->next = list->items;
		list->items = new_item;
	}
	list->item_count++;

	return 0;
}
