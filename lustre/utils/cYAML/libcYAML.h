#ifndef CYAML_H
#define CYAML_H

#include <stdbool.h>

typedef enum {
	EN_YAML_TYPE_FALSE = 0,
	EN_YAML_TYPE_TRUE,
	EN_YAML_TYPE_NULL,
	EN_YAML_TYPE_NUMBER,
	EN_YAML_TYPE_STRING,
	EN_YAML_TYPE_ARRAY,
	EN_YAML_TYPE_OBJECT
} cYAML_object_type_t;

typedef struct cYAML {
	/* next/prev allow you to walk array/object chains. */
	struct cYAML *next, *prev;
	/* An array or object item will have a child pointer pointing
	   to a chain of the items in the array/object. */
	struct cYAML *child;
	/* The type of the item, as above. */
	cYAML_object_type_t type;

	/* The item's string, if type==EN_YAML_TYPE_STRING */
	char *valuestring;
	/* The item's number, if type==EN_YAML_TYPE_NUMBER */
	int valueint;
	/* The item's number, if type==EN_YAML_TYPE_NUMBER */
	double valuedouble;
	/* The item's name string, if this item is the child of,
	   or is in the list of subitems of an object. */
	char *string;
	/* user data which might need to be tracked per object */
	void *user_data;
} cYAML;

/*
 *  cYAML_tree_list_t
 *  Linked list of different trees representing YAML
 *  documents.
 */
typedef struct cYAML_ll_s {
	struct cYAML_ll_s *next;
	cYAML *obj;
} cYAML_ll_t;

typedef void (*cYAML_user_data_free_cb)(void *);

/*
 * cYAML_walk_cb
 *   Callback called when recursing through the tree
 *
 *   cYAML* - pointer to the node currently being visitied
 *   void* - user data passed to the callback.
 *   void** - output value from the callback
 *
 * Returns true to continue recursing.  false to stop recursing
 */
typedef bool (*cYAML_walk_cb)(cYAML *, void *, void**);

/* linked stack operations */
extern int cYAML_ll_push(cYAML *obj, cYAML_ll_t **list);
extern cYAML *cYAML_ll_pop(cYAML_ll_t **list);
extern void cYAML_ll_free(cYAML_ll_t *ll);

/*
 * cYAML_ll_check_instances
 *   Return 0 if there is < or == number instances of obj in ll, -1
 *   otherwise.
 *
 *   ll - linked list
 *   obj - object to check for in ll
 *   instnace - the cap of the number of appearances of obj in ll
 */
extern int cYAML_ll_check_instances(cYAML_ll_t *ll, cYAML *obj, int instances);
/*
 * cYAML_build_tree
 *   Build a tree representation of the YAML formated text passed in.
 *
 *   yaml_file - YAML file to parse and build tree representation
 *   yaml_blk - blk of YAML.  yaml_file takes precedence if both
 *   are defined.
 *   yaml_blk_size - length of the yaml block (obtained via strlen)
 */
extern cYAML *cYAML_build_tree(char *yaml_file,
			       const char *yaml_blk,
			       size_t yaml_blk_size,
			       cYAML **err_str);

/*
 * cYAML_print_tree
 *   Print the textual representation of a YAML tree to stderr
 *
 *   node - Node where you want to start printing
 *   level - should be 0 or a number which indicates the level
 *     of the node.
 */
extern void cYAML_print_tree(cYAML *node, int level);

/*
 * cYAML_print_tree2file
 *   Print the textual representation of a YAML tree to file
 *
 *   f - file to print to
 *   node - Node where you want to start printing
 *   level - should be 0 or a number which indicates the level
 *     of the node.
 */
extern void cYAML_print_tree2file(FILE *f, cYAML *node, int level);

/*
 * cYAML_free_tree
 *   Free the cYAML tree returned as part of the cYAML_build_tree
 *
 *   node - root of the tree to be freed
 */
extern void cYAML_free_tree(cYAML *node);

/*
 * cYAML_get_object_item
 *   Returns the cYAML object which key correspods to the name passed in
 *   This function searches only through the current level.
 *
 *   parent - is the parent object on which you want to conduct the search
 *   name - key name of the object you want to find.
 */
extern cYAML *cYAML_get_object_item(cYAML *parent, const char *name);

/*
 * cYAML_find_object
 *   Returns the cYAML object which key correspods to the name passed in
 *   this function searches the entire tree.
 *
 *   root - is the root of the tree on which you want to conduct the search
 *   name - key name of the object you want to find.
 */
extern cYAML *cYAML_find_object(cYAML *root, const char *name);

/*
 * cYAML_clean_usr_data
 *   walks the tree and for each node with some user data it calls the
 *   free_cb with the user data as a parameter.
 *
 *   node: node to start the walk from
 *   free_cb: cb to call to cleanup the user data
 */
extern void cYAML_clean_usr_data(cYAML *node, cYAML_user_data_free_cb free_cb);

/*
 * cYAML_tree_recursive_walk
 *  walk the tree recursively (depth first) and call a callback on each
 *  node.
 *
 *  node - node to start the walk from
 *  cb - callback to call on each node
 *  usr_data - opaque data passed to the callback
 *  out - out param from cb
 */
extern void cYAML_tree_recursive_walk(cYAML *node, cYAML_walk_cb cb,
				      bool cb_first, void *usr_data,
				      void **out);

/*
 * cYAML_create_object
 *  Creates a CYAML of type OBJECT
 *
 *  parent - parent node
 *  key - node key
 */
extern cYAML *cYAML_create_object(cYAML *parent, char *key);

/*
 * cYAML_create_string
 *   Creates a cYAML node of type STRING
 *
 *   parent - parent node
 *   key - node key
 *   value - value of node
 */
extern cYAML *cYAML_create_string(cYAML *parent, char *key, char *value);

/*
 * cYAML_create_string
 *   Creates a cYAML node of type STRING
 *
 *   parent - parent node
 *   key - node key
 *   value - value of node
 */
extern cYAML *cYAML_create_number(cYAML *parent, char *key, double value);

/*
 * cYAML_insert_sibling
 *   inserts one cYAML object as asibling to another
 *
 *   root - root node to have a siblign added to
 *   sibling - sibling to be added
 */
extern void cYAML_insert_sibling(cYAML *root, cYAML *sibling);

/*
 * cYAML_build_error
 *   Build a YAML error message given:
 *
 *   rc - return code to adde in the error
 *   seq_no - a sequence number to add in the error
 *   cmd - the command that failed.
 *   entity - command entity that failed.
 *   err_str - error string to add in the error
 *   root - the root to which to add the YAML error
 */
extern void cYAML_build_error(int rc, int seq_no, char *cmd,
			      char *entity, char *err_str,
			      cYAML **root);


#endif /* CYAML_H */
