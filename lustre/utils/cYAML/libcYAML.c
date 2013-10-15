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
#include "libcYAML.h"

typedef enum {
	EN_YAML_ERROR_NONE = 0,
	EN_YAML_ERROR_UNEXPECTED_STATE = -1,
	EN_YAML_ERROR_NOT_SUPPORTED = -2,
	EN_YAML_ERROR_OUT_OF_MEM = -3,
	EN_YAML_ERROR_BAD_VALUE = -4,
} cYAML_handler_error_t;

typedef enum {
	EN_TREE_STATE_COMPLETE = 0,
	EN_TREE_STATE_INITED,
	EN_TREE_STATE_TREE_STARTED,
	EN_TREE_STATE_BLK_STARTED,
	EN_TREE_STATE_KEY_SIBLING,
	EN_TREE_STATE_KEY_CHILD,
	EN_TREE_STATE_KEY_FILLED,
	EN_TREE_STATE_VALUE,
} cYAML_tree_state_t;

typedef struct cYAML_tree_node_s {
	struct cYAML_tree_node_s *next;
	cYAML *root;
	/* cur is the current node we're operating on */
	cYAML *cur;
	cYAML_tree_state_t state;
	/* represents the tree depth */
	cYAML_ll_t *ll;
} cYAML_tree_node_t;

typedef cYAML_handler_error_t (*yaml_token_handler)(yaml_token_t *token, cYAML_tree_node_t *);

static cYAML_handler_error_t yaml_parse_error(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_stream_start(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_stream_end(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_not_supported(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_document_start(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_document_end(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_blk_seq_start(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_blk_mapping_start(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_block_end(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_key(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_value(yaml_token_t *token, cYAML_tree_node_t *tree);
static cYAML_handler_error_t yaml_scalar(yaml_token_t *token, cYAML_tree_node_t *tree);

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
	[YAML_BLOCK_ENTRY_TOKEN] = yaml_not_supported,
	[YAML_FLOW_ENTRY_TOKEN] = yaml_not_supported,
	[YAML_KEY_TOKEN] = yaml_key,
	[YAML_VALUE_TOKEN] = yaml_value,
	[YAML_ALIAS_TOKEN] = yaml_not_supported,
	[YAML_ANCHOR_TOKEN] = yaml_not_supported,
	[YAML_TAG_TOKEN] = yaml_not_supported,
	[YAML_SCALAR_TOKEN] = yaml_scalar,
};

static int cYAML_tree_init(cYAML_tree_node_t *tree)
{
	cYAML *obj = NULL, *cur = NULL;

	if (!tree)
		return (-1);

	obj = calloc(sizeof(char),
	      sizeof(cYAML));
	if (!obj)
		return (-1);

	if (tree->root) {
		/* append the node */
		cur = tree->root;
		while (cur->next != NULL) {
			cur = cur->next;
		}
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

	return (0);
}

static cYAML *create_child(cYAML *parent, char *value)
{
	cYAML *obj;

	if (!parent)
		return (NULL);

	obj = calloc(sizeof(char),
		     sizeof(cYAML));
	if (!obj)
		return (NULL);

	obj->string = strdup(value);

	/* set the type to OBJECT and let the value change that */
	obj->type = EN_YAML_TYPE_OBJECT;

	parent->child = obj;

	return (obj);
}

static cYAML *create_sibling(cYAML *sibling, char *value)
{
	cYAML *obj;

	if (!sibling)
		return (NULL);

	obj = calloc(sizeof(char),
		     sizeof(cYAML));
	if (!obj)
		return (NULL);

	obj->string = strdup(value);

	/* set the type to OBJECT and let the value change that */
	obj->type = EN_YAML_TYPE_OBJECT;

	sibling->next = obj;
	obj->prev = sibling;

	return (obj);
}

static void add_child(cYAML *parent, cYAML *node)
{
	cYAML *cur;

	if (parent && node) {
		if (parent->child == NULL) {
			parent->child = node;
			return;
		}

		cur = parent->child;

		while (cur->next) {
			cur = cur->next;
		}

		cur->next = node;
	}
}

/* Parse the input text to generate a number, and populate the result into item. */
static bool parse_number(cYAML *item,const char *input)
{
	double n=0,sign=1,scale=0;
	int subscale=0,signsubscale=1;
	const char *num = input;

	if (*num=='-') {
		sign=-1;
		num++;
	}

	if (*num=='0')
		num++;

	if (*num>='1' && *num<='9') {
		do {
			n=(n*10.0)+(*num++ -'0');
		} while (*num>='0' && *num<='9');
	}

	if (*num=='.' && num[1]>='0' && num[1]<='9') {
		num++;
		do {
			n=(n*10.0)+(*num++ -'0');
			scale--;
		} while (*num>='0' && *num<='9');
	}

	if (*num=='e' || *num=='E') {
		num++;
		if (*num=='+') {
			num++;
		} else if (*num=='-') {
			signsubscale=-1;
			num++;
		}
		while (*num>='0' && *num<='9') {
			subscale=(subscale*10)+(*num++ - '0');
		}
	}

	/* check to see if the entire string is consumed.  If not then
	 * that means this is a string with a number in it */
	if (num != (input + strlen(input)))
		return false;

	/* number = +/- number.fraction * 10^+/- exponent */
	n=sign*n*pow(10.0,(scale+subscale*signsubscale));

	item->valuedouble = n;
	item->valueint = (int)n;
	item->type = EN_YAML_TYPE_NUMBER;

	return true;
}

static int assign_type_value(cYAML *obj, char *value)
{
	if (!value)
		return -1;

	if (!strncmp(value, "null", 4))
		obj->type = EN_YAML_TYPE_NULL;
	else if (!strncmp(value, "false", 5)) {
		obj->type = EN_YAML_TYPE_FALSE;
		obj->valueint = 0;
	} else if (!strncmp(value, "true", 4)) {
		obj->type = EN_YAML_TYPE_TRUE;
		obj->valueint = 1;
	} else if (*value=='-' || (*value>='0' && *value<='9')) {
		if (!parse_number(obj, value)) {
			obj->valuestring = strdup(value);
			obj->type = EN_YAML_TYPE_STRING;
		}
	}
	else {
		obj->valuestring = strdup(value);
		obj->type = EN_YAML_TYPE_STRING;
	}

	return (0);
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
static cYAML_handler_error_t yaml_parse_error(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	fprintf(stderr, "Parse Error\n");
	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_stream_start(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	cYAML_handler_error_t rc;

	/* with each new stream initialize a new tree */
	rc = cYAML_tree_init(tree);

	if (rc != EN_YAML_ERROR_NONE)
		return (rc);

	tree->state = EN_TREE_STATE_INITED;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_stream_end(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if (tree->state != EN_TREE_STATE_COMPLETE)
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	/* no state transition */

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_document_start(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	cYAML_handler_error_t rc;

	if ((tree->state != EN_TREE_STATE_COMPLETE) &&
	    (tree->state != EN_TREE_STATE_INITED))
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	/* start a new tree */
	if (tree->state == EN_TREE_STATE_COMPLETE) {
		if ((rc = cYAML_tree_init(tree)) != EN_YAML_ERROR_NONE)
			return (rc);
	}

	/* go to started state since we're expecting more tokens to come */
	tree->state = EN_TREE_STATE_TREE_STARTED;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_document_end(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if (tree->state != EN_TREE_STATE_TREE_STARTED)
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	tree->state = EN_TREE_STATE_COMPLETE;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_key(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if ((tree->state != EN_TREE_STATE_BLK_STARTED) &&
	    (tree->state != EN_TREE_STATE_TREE_STARTED))
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	tree->state = (tree->state == EN_TREE_STATE_BLK_STARTED) ?
	  EN_TREE_STATE_KEY_CHILD : EN_TREE_STATE_KEY_SIBLING;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_scalar(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	cYAML *obj;

	if (tree->state == EN_TREE_STATE_KEY_CHILD) {
		/* create a child of cur */
		obj = create_child(tree->cur,
				   (char*)token->data.scalar.value);

		/* push cur on the stack */
		if (cYAML_ll_push(tree->cur, &(tree->ll)))
			return (EN_YAML_ERROR_OUT_OF_MEM);

		/* assing the new child to cur */
		tree->cur = obj;

		tree->state = EN_TREE_STATE_KEY_FILLED;
	} else if (tree->state == EN_TREE_STATE_KEY_SIBLING) {
		obj = create_sibling(tree->cur,
				     (char*)token->data.scalar.value);
		if (!obj) {
			return (EN_YAML_ERROR_OUT_OF_MEM);
		}
		tree->cur = obj;

		tree->state = EN_TREE_STATE_KEY_FILLED;
	} else if (tree->state == EN_TREE_STATE_VALUE) {
		if (assign_type_value(tree->cur,
				      (char*)token->data.scalar.value))
			/* failed to assign a value */
			return (EN_YAML_ERROR_BAD_VALUE);
		tree->state = EN_TREE_STATE_TREE_STARTED;
	} else
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_value(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if (tree->state != EN_TREE_STATE_KEY_FILLED)
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	tree->state = EN_TREE_STATE_VALUE;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_blk_seq_start(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_blk_mapping_start(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if ((tree->state != EN_TREE_STATE_VALUE) &&
	    (tree->state != EN_TREE_STATE_TREE_STARTED))
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	tree->state = EN_TREE_STATE_BLK_STARTED;

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_block_end(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	if (tree->state != EN_TREE_STATE_TREE_STARTED)
		return (EN_YAML_ERROR_UNEXPECTED_STATE);

	tree->cur = cYAML_ll_pop(&(tree->ll));

	/* no state transition */

	return (EN_YAML_ERROR_NONE);
}

static cYAML_handler_error_t yaml_not_supported(yaml_token_t *token, cYAML_tree_node_t *tree)
{
	return (EN_YAML_ERROR_NOT_SUPPORTED);
}

static bool clean_usr_data(cYAML *node, void *usr_data, void **out)
{
	cYAML_user_data_free_cb free_cb =
	  (cYAML_user_data_free_cb)usr_data;

	if (free_cb && node && node->user_data) {
		free_cb(node->user_data);
		node->user_data = NULL;
	}

	return true;
}

static bool free_node(cYAML *node, void *user_data, void **out)
{
	if (node)
		free(node);

	return true;
}

static bool find_obj_iter(cYAML *node, void *usr_data, void **out)
{
	char *name = (char*)usr_data;

	if ((node) && (node->string) &&
	    (strncmp(node->string, name, strlen(node->string)) == 0)) {
		*out = node;
		return false;
	}

	return true;
}

extern void cYAML_ll_free(cYAML_ll_t *ll)
{
	cYAML_ll_t *node;
	node = ll;
	while (node) {
		ll = ll->next;
		free(node);
		node = ll;
	}
}

extern int cYAML_ll_push(cYAML *obj, cYAML_ll_t **list)
{
	cYAML_ll_t *node = calloc(sizeof(char),
				  sizeof(cYAML_ll_t));
	if (!node)
		return (-1);

	node->obj = obj;
	node->next = *list;
	*list = node;

	return 0;
}

extern cYAML *cYAML_ll_pop(cYAML_ll_t **list)
{
	cYAML_ll_t *pop = *list;
	cYAML *obj = NULL;

	if (pop) {
		obj = pop->obj;
		*list = (*list)->next;
		free(pop);
	}

	return (obj);
}

extern int cYAML_ll_check_instances (cYAML_ll_t *ll, cYAML *obj, int instances)
{
	cYAML_ll_t *node = ll;
	int i = instances;

	while (node) {
		if ((node->obj->string) && !strcmp(node->obj->string, obj->string)) {
			i--;
			if (i < 0)
				return -1;
		}
		node = node->next;
	}

	return 0;
}

extern cYAML *cYAML_get_object_item(cYAML *parent, const char *name)
{
	cYAML *node;

	if (!parent || !parent->child || !name)
		return NULL;

	node = parent->child;

	while (node &&
	       (strncmp(node->string, name, strlen(node->string)))) {
		node = node->next;
	}

	return node;
}

extern void cYAML_tree_recursive_walk(cYAML *node, cYAML_walk_cb cb,
				      bool cb_first,
				      void *usr_data,
				      void **out)
{
	if (!node)
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

extern cYAML *cYAML_find_object(cYAML *root, const char *name)
{
	cYAML *found = NULL;

	cYAML_tree_recursive_walk(root, find_obj_iter, true,
				  (void*)name, (void**)&found);

	return found;
}

extern void cYAML_clean_usr_data(cYAML *node, cYAML_user_data_free_cb free_cb)
{
	cYAML_tree_recursive_walk(node, clean_usr_data, false, free_cb, NULL);
}

extern void cYAML_free_tree(cYAML *node)
{
	cYAML_tree_recursive_walk(node, free_node, false, NULL, NULL);
}

void cYAML_print_tree_helper(FILE *f, cYAML *node, int level)
{
	int i;
	int llevel = level;

	if (!node)
		return;

	for (i = 0; i < level; i++)
		fprintf(f, "    ");
	if (node->type == EN_YAML_TYPE_NUMBER)
		fprintf(f, "%s: %f\n", node->string, node->valuedouble);
	else if ((node->type == EN_YAML_TYPE_TRUE) ||
		   (node->type == EN_YAML_TYPE_FALSE))
		fprintf(f, "%s: %d\n", node->string, node->valueint);
	else if (node->type == EN_YAML_TYPE_OBJECT) {
		if (node->string)
			fprintf(f, "%s:\n", node->string);
	} else {
		char *new_line;

		new_line = strchr(node->valuestring, '\n');
		if (!new_line)
			fprintf(f, "%s: %s\n", node->string, node->valuestring);
		else {
			fprintf(f, "%s: ", node->string);
			char *l = node->valuestring;
			while (new_line) {
				*new_line = '\0';
				fprintf(f, "%s\n", l);
				for (i = 0; i < level; i++)
					fprintf(f, "    ");
				for (i = 0; i < strlen(node->string) + 2; i++)
					fprintf(f, " ");
				*new_line = '\n';
				l = new_line+1;
				new_line = strchr(l, '\n');
			}
			fprintf(f, "%s\n", l);
		}
	}

	if (node->child) {
		llevel++;
		cYAML_print_tree_helper(f, node->child, llevel);
		llevel--;
	}

	if (node->next)
		cYAML_print_tree_helper(f, node->next, llevel);
}

extern void cYAML_print_tree(cYAML *node, int level)
{
	cYAML_print_tree_helper(stderr, node, level);
}

extern void cYAML_print_tree2file(FILE *f, cYAML *node, int level)
{
	cYAML_print_tree_helper(f, node, level);
}

extern cYAML *cYAML_create_object(cYAML *parent, char *key)
{
	cYAML *node = calloc(sizeof(char),
			     sizeof(cYAML));
	if (!node)
		return NULL;

	if (key)
		node->string = strdup(key);

	node->type = EN_YAML_TYPE_OBJECT;

	add_child(parent, node);

	return node;
}

extern cYAML *cYAML_create_string(cYAML *parent, char *key, char *value)
{
	cYAML *node = calloc(sizeof(char),
			     sizeof(cYAML));
	if (!node)
		return NULL;

	node->string = strdup(key);
	node->valuestring = strdup(value);
	node->type = EN_YAML_TYPE_STRING;

	add_child(parent, node);

	return node;
}

extern cYAML *cYAML_create_number(cYAML *parent, char *key, double value)
{
	cYAML *node = calloc(sizeof(char),
			     sizeof(cYAML));
	if (!node)
		return NULL;

	node->string = strdup(key);
	node->valuedouble = value;
	node->valueint = (int)value;
	node->type = EN_YAML_TYPE_NUMBER;

	add_child(parent, node);

	return node;
}

extern void cYAML_insert_sibling(cYAML *root, cYAML *sibling)
{
	cYAML *last = NULL;
	if (!root || !sibling)
		return;

	last = root;
	while (last->next != NULL) {
		last = last->next;
	}

	last->next = sibling;
}

extern void cYAML_build_error(int rc, int seq_no, char *cmd,
			      char *entity, char *err_str,
			      cYAML **root)
{
	cYAML *r = NULL, *err, *type, *ent;
	if (!root)
		return;

	/* add to the tail of the root that's passed in */
	if (!(*root)) {
		*root = cYAML_create_object(NULL, NULL);
		if (!(*root))
			goto failed;
	}

	r = *root;

	err = cYAML_create_object(r, "error");
	if (!err)
		goto failed;

	if ((seq_no >= 0) &&
	    !cYAML_create_number(err, "seqno", seq_no))
		goto failed;

	type = cYAML_create_object(err, cmd);
	if (!type)
		goto failed;

	ent = cYAML_create_object(type, entity);
	if (!ent)
		goto failed;

	if (!cYAML_create_number(ent, "errno", rc))
		goto failed;

	if (!cYAML_create_string(ent, "descr", err_str))
		goto failed;

	return;

failed:
	cYAML_free_tree(r);
	r = NULL;
}

extern cYAML *cYAML_build_tree(char *yaml_file,
			       const char *yaml_blk,
			       size_t yaml_blk_size,
			       cYAML **err_rc)
{
	yaml_parser_t parser;
	yaml_token_t token;
	cYAML_tree_node_t tree;
	cYAML_handler_error_t rc;
	yaml_token_type_t token_type;
	char err_str[256];

	int done = 0;

	memset(&tree, 0, sizeof(cYAML_tree_node_t));

	/* check input params */
	if (!yaml_file && !yaml_blk) {
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
		if (!input) {
			sprintf(err_str, "Failed to open file: %s", yaml_file);
			cYAML_build_error(-1, -1, "yaml", "builder",
					  err_str,
					  err_rc);
			return NULL;
		}

		yaml_parser_set_input_file(&parser, input);
	} else if (yaml_blk) {
		yaml_parser_set_input_string(&parser,
					     (const unsigned char*) yaml_blk,
					     yaml_blk_size);
	} else
		/* no input provided */
		return NULL;

	/* Read the event sequence. */
	while (!done) {
		/*
		* Go through the parser and build a command representation tree
		* for the command line parameters
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
		return (tree.root);

	cYAML_free_tree(tree.root);

	return NULL;
}
