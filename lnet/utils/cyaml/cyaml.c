/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * Copyright (c) 2013, Intel Corporation, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 *
 * LGPL HEADER END
 *
 * Contributers:
 *   Amir Shehata
 */

/*
 *  The cYAML tree is constructed as an n-tree.
 *  root -> cmd 1
 *          ||
 *          \/
 *          cmd 2 -> attr1 -> attr2
 *				||
 *				\/
 *			      attr2.1 -> attr2.1.1 -> attr2.1.2
 */

#include <yaml.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <float.h>
#include <limits.h>
#include <ctype.h>
#include "cyaml.h"

#define INDENT		4
#define EXTRA_IND	2

/*
 * cYAML_print_info
 *   This structure contains print information
 *   required when printing the node
 */
struct cYAML_print_info {
	int level;
	int array_first_elem;
	int extra_ind;
};

/*
 *  cYAML_ll
 *  Linked list of different trees representing YAML
 *  documents.
 */
struct cYAML_ll {
	struct cYAML_ll *next;
	struct cYAML *obj;
	struct cYAML_print_info *print_info;
};

static void print_value(FILE *f, struct cYAML_ll *stack);

enum cYAML_handler_error {
	EN_YAML_ERROR_NONE = 0,
	EN_YAML_ERROR_UNEXPECTED_STATE = -1,
	EN_YAML_ERROR_NOT_SUPPORTED = -2,
	EN_YAML_ERROR_OUT_OF_MEM = -3,
	EN_YAML_ERROR_BAD_VALUE = -4,
	EN_YAML_ERROR_PARSE = -5,
};

enum cYAML_tree_state {
	EN_TREE_STATE_COMPLETE = 0,
	EN_TREE_STATE_INITED,
	EN_TREE_STATE_TREE_STARTED,
	EN_TREE_STATE_BLK_STARTED,
	EN_TREE_STATE_KEY,
	EN_TREE_STATE_KEY_FILLED,
	EN_TREE_STATE_VALUE,
	EN_TREE_STATE_SEQ_START,
};

struct cYAML_tree_node {
	struct cYAML_tree_node_s *next;
	struct cYAML *root;
	/* cur is the current node we're operating on */
	struct cYAML *cur;
	enum cYAML_tree_state state;
	int from_blk_map_start;
	/* represents the tree depth */
	struct cYAML_ll *ll;
};

typedef enum cYAML_handler_error (*yaml_token_handler)(yaml_token_t *token,
						struct cYAML_tree_node *);

static enum cYAML_handler_error yaml_parse_error(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_stream_start(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_stream_end(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_not_supported(yaml_token_t *token,
						struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_document_start(yaml_token_t *token,
						struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_document_end(yaml_token_t *token,
					       struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_blk_seq_start(yaml_token_t *token,
						struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_blk_mapping_start(yaml_token_t *token,
						struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_block_end(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_key(yaml_token_t *token,
				struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_value(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_scalar(yaml_token_t *token,
					struct cYAML_tree_node *tree);
static enum cYAML_handler_error yaml_entry_token(yaml_token_t *token,
					struct cYAML_tree_node *tree);

/* dispatch table */
yaml_token_handler dispatch_tbl[YAML_SCALAR_TOKEN+1] = {
	[YAML_NO_TOKEN] = yaml_parse_error,
	[YAML_STREAM_START_TOKEN] = yaml_stream_start,
	[YAML_STREAM_END_TOKEN] = yaml_stream_end,
	[YAML_VERSION_DIRECTIVE_TOKEN] = yaml_not_supported,
	[YAML_TAG_DIRECTIVE_TOKEN] = yaml_not_supported,
	[YAML_DOCUMENT_START_TOKEN] = yaml_document_start,
	[YAML_DOCUMENT_END_TOKEN] = yaml_document_end,
	[YAML_BLOCK_SEQUENCE_START_TOKEN] = yaml_blk_seq_start,
	[YAML_BLOCK_MAPPING_START_TOKEN] = yaml_blk_mapping_start,
	[YAML_BLOCK_END_TOKEN] = yaml_block_end,
	[YAML_FLOW_SEQUENCE_START_TOKEN] = yaml_not_supported,
	[YAML_FLOW_SEQUENCE_END_TOKEN] = yaml_not_supported,
	[YAML_FLOW_MAPPING_START_TOKEN] = yaml_not_supported,
	[YAML_FLOW_MAPPING_END_TOKEN] = yaml_not_supported,
	[YAML_BLOCK_ENTRY_TOKEN] = yaml_entry_token,
	[YAML_FLOW_ENTRY_TOKEN] = yaml_not_supported,
	[YAML_KEY_TOKEN] = yaml_key,
	[YAML_VALUE_TOKEN] = yaml_value,
	[YAML_ALIAS_TOKEN] = yaml_not_supported,
	[YAML_ANCHOR_TOKEN] = yaml_not_supported,
	[YAML_TAG_TOKEN] = yaml_not_supported,
	[YAML_SCALAR_TOKEN] = yaml_scalar,
};

static void cYAML_ll_free(struct cYAML_ll *ll)
{
	struct cYAML_ll *node;
	node = ll;
	while (node) {
		ll = ll->next;
		free(node->print_info);
		free(node);
		node = ll;
	}
}

static int cYAML_ll_push(struct cYAML *obj, struct cYAML_print_info *print_info,
			 struct cYAML_ll **list)
{
	struct cYAML_ll *node = calloc(sizeof(char),
				       sizeof(struct cYAML_ll));
	if (node == NULL)
		return -1;

	if (print_info) {
		node->print_info = calloc(sizeof(char),
					  sizeof(struct cYAML_print_info));
		if (node->print_info == NULL) {
			free(node);
			return -1;
		}
		*node->print_info = *print_info;
	}
	node->obj = obj;
	node->next = *list;
	*list = node;

	return 0;
}

static struct cYAML *cYAML_ll_pop(struct cYAML_ll **list,
				  struct cYAML_print_info **print_info)
{
	struct cYAML_ll *pop = *list;
	struct cYAML *obj = NULL;

	if (pop) {
		obj = pop->obj;
		if (print_info != NULL)
			*print_info = pop->print_info;
		*list = (*list)->next;
		free(pop);
	}

	return obj;
}

static int cYAML_ll_count(struct cYAML_ll *ll)
{
	int i = 0;
	struct cYAML_ll *node = ll;

	while (node) {
		i++;
		node = node->next;
	}

	return i;
}

static int cYAML_tree_init(struct cYAML_tree_node *tree)
{
	struct cYAML *obj = NULL, *cur = NULL;

	if (tree == NULL)
		return -1;

	obj = calloc(sizeof(char),
	      sizeof(struct cYAML));
	if (obj == NULL)
		return -1;

	if (tree->root) {
		/* append the node */
		cur = tree->root;
		while (cur->next != NULL)
			cur = cur->next;
		cur->next = obj;
	} else
		tree->root = obj;

	obj->type = EN_YAML_TYPE_OBJECT;

	tree->cur = obj;
	tree->state = EN_TREE_STATE_COMPLETE;

	if (tree->ll) {
		/* free it and start anew */
		cYAML_ll_free(tree->ll);
	}
	tree->ll = NULL;

	return 0;
}

static struct cYAML *create_child(struct cYAML *parent)
{
	struct cYAML *obj;

	if (parent == NULL)
		return NULL;

	obj = calloc(sizeof(char),
		     sizeof(struct cYAML));
	if (obj == NULL)
		return NULL;

	/* set the type to OBJECT and let the value change that */
	obj->type = EN_YAML_TYPE_OBJECT;

	parent->child = obj;

	return obj;
}

static struct cYAML *create_sibling(struct cYAML *sibling)
{
	struct cYAML *obj;

	if (sibling == NULL)
		return NULL;

	obj = calloc(sizeof(char),
		     sizeof(struct cYAML));
	if (obj == NULL)
		return NULL;

	/* set the type to OBJECT and let the value change that */
	obj->type = EN_YAML_TYPE_OBJECT;

	sibling->next = obj;
	obj->prev = sibling;

	return obj;
}

static void add_child(struct cYAML *parent, struct cYAML *node)
{
	struct cYAML *cur;

	if (parent && node) {
		if (parent->child == NULL) {
			parent->child = node;
			return;
		}

		cur = parent->child;

		while (cur->next)
			cur = cur->next;

		cur->next = node;
		node->prev = cur;
	}
}

/* Parse the input text to generate a number,
 * and populate the result into item. */
static bool parse_number(struct cYAML *item, const char *input)
{
	double n = 0, sign = 1, scale = 0;
	int subscale = 0, signsubscale = 1;
	const char *num = input;

	if (*num == '-') {
		sign = -1;
		num++;
	}

	if (*num == '0')
		num++;

	if (*num >= '1' && *num <= '9') {
		do {
			n = (n * 10.0) + (*num++ - '0');
		} while (*num >= '0' && *num <= '9');
	}

	if (*num == '.' && num[1] >= '0' && num[1] <= '9') {
		num++;
		do {
			n = (n * 10.0) + (*num++ - '0');
			scale--;
		} while (*num >= '0' && *num <= '9');
	}

	if (*num == 'e' || *num == 'E') {
		num++;
		if (*num == '+') {
			num++;
		} else if (*num == '-') {
			signsubscale = -1;
			num++;
		}
		while (*num >= '0' && *num <= '9')
			subscale = (subscale * 10) + (*num++ - '0');
	}

	/* check to see if the entire string is consumed.  If not then
	 * that means this is a string with a number in it */
	if (num != (input + strlen(input)))
		return false;

	/* number = +/- number.fraction * 10^+/- exponent */
	n = sign * n * pow(10.0, (scale + subscale * signsubscale));

	item->valuedouble = n;
	item->valueint = (int)n;
	item->type = EN_YAML_TYPE_NUMBER;

	return true;
}

static int assign_type_value(struct cYAML *obj, char *value)
{
	if (value == NULL)
		return -1;

	if (strncmp(value, "null", 4) == 0)
		obj->type = EN_YAML_TYPE_NULL;
	else if (strncmp(value, "false", 5) == 0) {
		obj->type = EN_YAML_TYPE_FALSE;
		obj->valueint = 0;
	} else if (strncmp(value, "true", 4) == 0) {
		obj->type = EN_YAML_TYPE_TRUE;
		obj->valueint = 1;
	} else if (*value == '-' || (*value >= '0' && *value <= '9')) {
		if (parse_number(obj, value) == 0) {
			obj->valuestring = strdup(value);
			obj->type = EN_YAML_TYPE_STRING;
		}
	} else {
		obj->valuestring = strdup(value);
		obj->type = EN_YAML_TYPE_STRING;
	}

	return 0;
}

/*
 * yaml_handle_token
 *  Builds the YAML tree rpresentation as the tokens are passed in
 *
 *  if token == STREAM_START && tree_state != COMPLETE
 *    something wrong. fail.
 *  else tree_state = INITIED
 *  if token == DOCUMENT_START && tree_state != COMPLETE || INITED
 *    something wrong, fail.
 *  else tree_state = TREE_STARTED
 *  if token == DOCUMENT_END
 *    tree_state = INITED if no STREAM START, else tree_state = COMPLETE
 *    erase everything on ll
 *  if token == STREAM_END && tree_state != INITED
 *    something wrong fail.
 *  else tree_state = COMPLETED
 *  if token == YAML_KEY_TOKEN && state != TREE_STARTED
 *    something wrong, fail.
 *  if token == YAML_SCALAR_TOKEN && state != KEY || VALUE
 *    fail.
 *  else if tree_state == KEY
 *     create a new sibling under the current head of the ll (if ll is
 *     empty insert the new node there and it becomes the root.)
 *    add the scalar value in the "string"
 *    tree_state = KEY_FILLED
 *  else if tree_state == VALUE
 *    try and figure out whether this is a double, int or string and store
 *    it appropriately
 *    state = TREE_STARTED
 * else if token == YAML_BLOCK_MAPPING_START_TOKEN && tree_state != VALUE
 *   fail
 * else push the current node on the ll && state = TREE_STARTED
 * if token == YAML_BLOCK_END_TOKEN && state != TREE_STARTED
 *   fail.
 * else pop the current token off the ll and make it the cur
 * if token == YAML_VALUE_TOKEN && state != KEY_FILLED
 *   fail.
 * else state = VALUE
 *
 */
static enum cYAML_handler_error yaml_parse_error(yaml_token_t *token,
						 struct cYAML_tree_node *tree)
{
	return EN_YAML_ERROR_PARSE;
}

static enum cYAML_handler_error yaml_stream_start(yaml_token_t *token,
						  struct cYAML_tree_node *tree)
{
	enum cYAML_handler_error rc;

	/* with each new stream initialize a new tree */
	rc = cYAML_tree_init(tree);

	if (rc != EN_YAML_ERROR_NONE)
		return rc;

	tree->state = EN_TREE_STATE_INITED;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_stream_end(yaml_token_t *token,
						struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_TREE_STARTED)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	tree->state = EN_TREE_STATE_INITED;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error
yaml_document_start(yaml_token_t *token, struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_INITED)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	/* go to started state since we're expecting more tokens to come */
	tree->state = EN_TREE_STATE_TREE_STARTED;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_document_end(yaml_token_t *token,
						  struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_COMPLETE)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	tree->state = EN_TREE_STATE_TREE_STARTED;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_key(yaml_token_t *token,
					 struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_BLK_STARTED)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	if (tree->from_blk_map_start == 0)
		tree->cur = create_sibling(tree->cur);
	tree->from_blk_map_start = 0;

	tree->state = EN_TREE_STATE_KEY;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_scalar(yaml_token_t *token,
					    struct cYAML_tree_node *tree)
{
	if (tree->state == EN_TREE_STATE_KEY) {
		/* assign the scalar value to the key that was created */
		tree->cur->string = strdup((const char *)token->data.scalar.value);

		tree->state = EN_TREE_STATE_KEY_FILLED;
	} else if (tree->state == EN_TREE_STATE_VALUE) {
		if (assign_type_value(tree->cur,
				      (char *)token->data.scalar.value))
			/* failed to assign a value */
			return EN_YAML_ERROR_BAD_VALUE;
		tree->state = EN_TREE_STATE_BLK_STARTED;
	} else
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_value(yaml_token_t *token,
					   struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_KEY_FILLED)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	tree->state = EN_TREE_STATE_VALUE;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_blk_seq_start(yaml_token_t *token,
						   struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_VALUE)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	/* Since a sequenc start event determines that this is the start
	 * of an array, then that means the current node we're at is an
	 * array and we need to flag it as such */
	tree->cur->type = EN_YAML_TYPE_ARRAY;
	tree->state = EN_TREE_STATE_SEQ_START;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_entry_token(yaml_token_t *token,
						 struct cYAML_tree_node *tree)
{
	struct cYAML *obj;

	if ((tree->state != EN_TREE_STATE_SEQ_START) &&
	    (tree->state != EN_TREE_STATE_BLK_STARTED))
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	if (tree->state == EN_TREE_STATE_SEQ_START) {
		obj = create_child(tree->cur);

		if (cYAML_ll_push(tree->cur, NULL, &(tree->ll)))
			return EN_YAML_ERROR_OUT_OF_MEM;

		tree->cur = obj;
	} else {
		tree->cur = create_sibling(tree->cur);
		tree->state = EN_TREE_STATE_SEQ_START;
	}

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error
yaml_blk_mapping_start(yaml_token_t *token,
		       struct cYAML_tree_node *tree)
{
	struct cYAML *obj;

	if ((tree->state != EN_TREE_STATE_VALUE) &&
	    (tree->state != EN_TREE_STATE_INITED) &&
	    (tree->state != EN_TREE_STATE_SEQ_START) &&
	    (tree->state != EN_TREE_STATE_TREE_STARTED))
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	/* block_mapping_start means we're entering another block
	 * indentation, so we need to go one level deeper
	 * create a child of cur */
	obj = create_child(tree->cur);

	/* push cur on the stack */
	if (cYAML_ll_push(tree->cur, NULL, &(tree->ll)))
		return EN_YAML_ERROR_OUT_OF_MEM;

	/* assing the new child to cur */
	tree->cur = obj;

	tree->state = EN_TREE_STATE_BLK_STARTED;

	tree->from_blk_map_start = 1;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_block_end(yaml_token_t *token,
					       struct cYAML_tree_node *tree)
{
	if (tree->state != EN_TREE_STATE_BLK_STARTED)
		return EN_YAML_ERROR_UNEXPECTED_STATE;

	tree->cur = cYAML_ll_pop(&(tree->ll), NULL);

	/* if you have popped all the way to the top level, then move to
	 * the complete state. */
	if (cYAML_ll_count(tree->ll) == 0)
		tree->state = EN_TREE_STATE_COMPLETE;

	return EN_YAML_ERROR_NONE;
}

static enum cYAML_handler_error yaml_not_supported(yaml_token_t *token,
						   struct cYAML_tree_node *tree)
{
	return EN_YAML_ERROR_NOT_SUPPORTED;
}

static bool clean_usr_data(struct cYAML *node, void *usr_data, void **out)
{
	cYAML_user_data_free_cb free_cb =
	  (cYAML_user_data_free_cb)usr_data;

	if (free_cb && node && node->user_data) {
		free_cb(node->user_data);
		node->user_data = NULL;
	}

	return true;
}

static bool free_node(struct cYAML *node, void *user_data, void **out)
{
	if (node)
		free(node);

	return true;
}

static bool find_obj_iter(struct cYAML *node, void *usr_data, void **out)
{
	char *name = (char *)usr_data;

	if ((node) && (node->string) &&
	    (strncmp(node->string, name, strlen(node->string)) == 0)) {
		*out = node;
		return false;
	}

	return true;
}

struct cYAML *cYAML_get_object_item(struct cYAML *parent, const char *name)
{
	struct cYAML *node;

	if (parent == NULL || parent->child == NULL || name == NULL)
		return NULL;

	node = parent->child;

	while (node &&
	       (strncmp(node->string, name, strlen(node->string)))) {
		node = node->next;
	}

	return node;
}

struct cYAML *cYAML_get_next_seq_item(struct cYAML *seq, struct cYAML **itm)
{
	if ((*itm != NULL) && ((*itm)->next != NULL)) {
		*itm = (*itm)->next;
		return (*itm)->child;
	}

	if ((*itm == NULL) && (seq->type == EN_YAML_TYPE_ARRAY)) {
		*itm = seq->child;
		return (*itm)->child;
	}

	return NULL;
}

void cYAML_tree_recursive_walk(struct cYAML *node, cYAML_walk_cb cb,
				      bool cb_first,
				      void *usr_data,
				      void **out)
{
	if (node == NULL)
		return;

	if (cb_first) {
		if (!cb(node, usr_data, out))
			return;
	}

	if (node->child)
		cYAML_tree_recursive_walk(node->child, cb,
					  cb_first, usr_data, out);

	if (node->next)
		cYAML_tree_recursive_walk(node->next, cb,
					  cb_first, usr_data, out);

	if (!cb_first) {
		if (!cb(node, usr_data, out))
			return;
	}
}

struct cYAML *cYAML_find_object(struct cYAML *root, const char *name)
{
	struct cYAML *found = NULL;

	cYAML_tree_recursive_walk(root, find_obj_iter, true,
				  (void *)name, (void **)&found);

	return found;
}

void cYAML_clean_usr_data(struct cYAML *node, cYAML_user_data_free_cb free_cb)
{
	cYAML_tree_recursive_walk(node, clean_usr_data, false, free_cb, NULL);
}

void cYAML_free_tree(struct cYAML *node)
{
	cYAML_tree_recursive_walk(node, free_node, false, NULL, NULL);
}

static inline void print_simple(FILE *f, struct cYAML *node,
				struct cYAML_print_info *cpi)
{
	int level = cpi->level;
	int ind = cpi->extra_ind;

	fprintf(f, "%*s""%s: %d\n", INDENT * level + ind, "", node->string,
		node->valueint);
}

static void print_string(FILE *f, struct cYAML *node,
			 struct cYAML_print_info *cpi)
{
	char *new_line;
	int level = cpi->level;
	int ind = cpi->extra_ind;

	if (cpi->array_first_elem)
		fprintf(f, "- ");

	new_line = strchr(node->valuestring, '\n');
	if (new_line == NULL)
		fprintf(f, "%*s""%s: %s\n", INDENT * level + ind, "",
			node->string, node->valuestring);
	else {
		int indent = 0;
		fprintf(f, "%*s""%s: ", INDENT * level + ind, "",
			node->string);
		char *l = node->valuestring;
		while (new_line) {
			*new_line = '\0';
			fprintf(f, "%*s""%s\n", indent, "", l);
			indent = INDENT * level + ind + strlen(node->string) + 2;
			*new_line = '\n';
			l = new_line+1;
			new_line = strchr(l, '\n');
		}
		fprintf(f, "%*s""%s\n", indent, "", l);
	}
}

static void print_number(FILE *f, struct cYAML *node,
			 struct cYAML_print_info *cpi)
{
	double d=node->valuedouble;
	int level = cpi->level;
	int ind = cpi->extra_ind;

	if (cpi->array_first_elem)
		fprintf(f, "- ");

	if ((fabs(((double)node->valueint) - d) <= DBL_EPSILON) &&
	    (d <= INT_MAX) && (d >= INT_MIN))
		fprintf(f, "%*s""%s: %d\n", INDENT * level + ind, "",
			node->string, node->valueint);
	else {
		if ((fabs(floor(d) - d) <= DBL_EPSILON) &&
		    (fabs(d) < 1.0e60))
			fprintf(f, "%*s""%s: %.0f\n", INDENT * level + ind, "",
				node->string, d);
		else if ((fabs(d) < 1.0e-6) || (fabs(d) > 1.0e9))
			fprintf(f, "%*s""%s: %e\n", INDENT * level + ind, "",
				node->string, d);
		else
			fprintf(f, "%*s""%s: %f\n", INDENT * level + ind, "",
				node->string, d);
	}
}

static void print_object(FILE *f, struct cYAML *node,
			 struct cYAML_ll *stack,
			 struct cYAML_print_info *cpi)
{
	struct cYAML_print_info print_info;
	struct cYAML *c = node->child;

	if (node->string != NULL)
		fprintf(f, "%*s""%s%s:\n", INDENT * cpi->level + cpi->extra_ind,
			"", (cpi->array_first_elem) ? "- " : "", node->string);

	print_info.level = (node->string != NULL) ? cpi->level + 1 : cpi->level;
	print_info.array_first_elem = (node->string == NULL) ?
	  cpi->array_first_elem : 0;
	print_info.extra_ind = 0;

	while (c) {
		if (cYAML_ll_push(c, &print_info, &stack) != 0)
			return;
		print_value(f, stack);
		c = c->next;
	}
}

static void print_array(FILE *f, struct cYAML *node,
			struct cYAML_ll *stack,
			struct cYAML_print_info *cpi)
{
	struct cYAML_print_info print_info;
	struct cYAML *c = node->child;

	if (node->string != NULL) {
		fprintf(f, "%*s""%s:\n", INDENT * cpi->level + cpi->extra_ind,
			"", node->string);
	}

	print_info.level = (node->string != NULL) ? cpi->level + 1 : cpi->level;
	print_info.array_first_elem =  1;
	print_info.extra_ind = EXTRA_IND;

	while (c) {
		if (cYAML_ll_push(c, &print_info, &stack) != 0)
			return;
		print_value(f, stack);
		print_info.array_first_elem = 0;
		c = c->next;
	}
}

static void print_value(FILE *f, struct cYAML_ll *stack)
{
	struct cYAML_print_info *cpi = NULL;
	struct cYAML *node = cYAML_ll_pop(&stack, &cpi);

	switch (node->type){
	case EN_YAML_TYPE_FALSE:
	case EN_YAML_TYPE_TRUE:
	case EN_YAML_TYPE_NULL:
		print_simple(f, node, cpi);
		break;
	case EN_YAML_TYPE_STRING:
		print_string(f, node, cpi);
		break;
	case EN_YAML_TYPE_NUMBER:
		print_number(f, node, cpi);
		break;
	case EN_YAML_TYPE_ARRAY:
		print_array(f, node, stack, cpi);
		break;
	case EN_YAML_TYPE_OBJECT:
		print_object(f, node, stack, cpi);
		break;
	default:
	break;
	}

	if (cpi != NULL)
		free(cpi);
}

void cYAML_print_tree(struct cYAML *node)
{
	struct cYAML_print_info print_info;
	struct cYAML_ll *list = NULL;

	memset(&print_info, 0, sizeof(struct cYAML_print_info));

	if (cYAML_ll_push(node, &print_info, &list) == 0)
		print_value(stderr, list);
}

void cYAML_print_tree2file(FILE *f, struct cYAML *node)
{
	struct cYAML_print_info print_info;
	struct cYAML_ll *list = NULL;

	memset(&print_info, 0, sizeof(struct cYAML_print_info));

	if (cYAML_ll_push(node, &print_info, &list) == 0)
		print_value(f, list);
}

static struct cYAML *insert_item(struct cYAML *parent, char *key,
				 enum cYAML_object_type type)
{
	struct cYAML *node = calloc(sizeof(char),
				    sizeof(struct cYAML));

	if (node == NULL)
		return NULL;

	if (key != NULL)
		node->string = strdup(key);

	node->type = type;

	add_child(parent, node);

	return node;
}

struct cYAML *cYAML_create_seq(struct cYAML *parent, char *key)
{
	return insert_item(parent, key, EN_YAML_TYPE_ARRAY);
}

struct cYAML *cYAML_create_seq_item(struct cYAML *seq)
{
	return insert_item(seq, NULL, EN_YAML_TYPE_OBJECT);
}

struct cYAML *cYAML_create_object(struct cYAML *parent, char *key)
{
	return insert_item(parent, key, EN_YAML_TYPE_OBJECT);
}

struct cYAML *cYAML_create_string(struct cYAML *parent, char *key, char *value)
{
	struct cYAML *node = calloc(sizeof(char),
			     sizeof(struct cYAML));
	if (node == NULL)
		return NULL;

	node->string = strdup(key);
	node->valuestring = strdup(value);
	node->type = EN_YAML_TYPE_STRING;

	add_child(parent, node);

	return node;
}

struct cYAML *cYAML_create_number(struct cYAML *parent, char *key, double value)
{
	struct cYAML *node = calloc(sizeof(char),
			     sizeof(struct cYAML));
	if (node == NULL)
		return NULL;

	node->string = strdup(key);
	node->valuedouble = value;
	node->valueint = (int)value;
	node->type = EN_YAML_TYPE_NUMBER;

	add_child(parent, node);

	return node;
}

void cYAML_insert_sibling(struct cYAML *root, struct cYAML *sibling)
{
	struct cYAML *last = NULL;
	if (root == NULL || sibling == NULL)
		return;

	last = root;
	while (last->next != NULL)
		last = last->next;

	last->next = sibling;
}

void cYAML_build_error(int rc, int seq_no, char *cmd,
		       char *entity, char *err_str,
		       struct cYAML **root)
{
	struct cYAML *r = NULL, *err, *s, *itm, *cmd_obj;
	if (root == NULL)
		return;

	/* add to the tail of the root that's passed in */
	if ((*root) == NULL) {
		*root = cYAML_create_object(NULL, NULL);
		if ((*root) == NULL)
			goto failed;
	}

	r = *root;

	/* look for the command */
	cmd_obj = cYAML_get_object_item(r, (const char *)cmd);
	if ((cmd_obj != NULL) && (cmd_obj->type == EN_YAML_TYPE_ARRAY))
		itm = cYAML_create_seq_item(cmd_obj);
	else if (cmd_obj == NULL) {
		s = cYAML_create_seq(r, cmd);
		itm = cYAML_create_seq_item(s);
	} else if ((cmd_obj != NULL) && (cmd_obj->type != EN_YAML_TYPE_ARRAY))
		goto failed;

	err = cYAML_create_object(itm, entity);
	if (err == NULL)
		goto failed;

	if ((seq_no >= 0) &&
	    (cYAML_create_number(err, "seqno", seq_no) == NULL))
		goto failed;

	if (cYAML_create_number(err, "errno", rc) == NULL)
		goto failed;

	if (cYAML_create_string(err, "descr", err_str) == NULL)
		goto failed;

	return;

failed:
	cYAML_free_tree(r);
	r = NULL;
}

struct cYAML *cYAML_build_tree(char *yaml_file,
			       const char *yaml_blk,
			       size_t yaml_blk_size,
			       struct cYAML **err_rc)
{
	yaml_parser_t parser;
	yaml_token_t token;
	struct cYAML_tree_node tree;
	enum cYAML_handler_error rc;
	yaml_token_type_t token_type;
	char err_str[256];

	int done = 0;

	memset(&tree, 0, sizeof(struct cYAML_tree_node));

	/* check input params */
	if (yaml_file == NULL && yaml_blk == NULL) {
		cYAML_build_error(-1, -1, "yaml", "builder",
				  "No YAML is provided",
				  err_rc);
		return NULL;
	}

	/* Create the Parser object. */
	yaml_parser_initialize(&parser);

	/* file alwyas takes precedence */
	if (yaml_file) {
		/* Set a file input. */
		FILE *input = fopen(yaml_file, "rb");
		if (input == NULL) {
			sprintf(err_str, "Failed to open file: %s", yaml_file);
			cYAML_build_error(-1, -1, "yaml", "builder",
					  err_str,
					  err_rc);
			return NULL;
		}

		yaml_parser_set_input_file(&parser, input);
	} else if (yaml_blk) {
		yaml_parser_set_input_string(&parser,
					     (const unsigned char *) yaml_blk,
					     yaml_blk_size);
	} else
		/* no input provided */
		return NULL;

	/* Read the event sequence. */
	while (!done) {
		/*
		* Go through the parser and build a cYAML representation
		* of the passed in YAML text
		*/
		yaml_parser_scan(&parser, &token);

		rc = dispatch_tbl[token.type](&token, &tree);
		if (rc != EN_YAML_ERROR_NONE) {
			sprintf(err_str, "Failed to handle token:%d [rc=%d]",
				token.type, rc);
			cYAML_build_error(-1, -1, "yaml", "builder",
					  err_str,
					  err_rc);
		}
		/* Are we finished? */
		done = ((rc != EN_YAML_ERROR_NONE) ||
			(token.type == YAML_STREAM_END_TOKEN) ||
			(token.type == YAML_NO_TOKEN));

		token_type = token.type;

		yaml_token_delete(&token);
	}

	/* Destroy the Parser object. */
	yaml_parser_delete(&parser);

	if ((token_type == YAML_STREAM_END_TOKEN) &&
	    (rc == EN_YAML_ERROR_NONE))
		return tree.root;

	cYAML_free_tree(tree.root);

	return NULL;
}
