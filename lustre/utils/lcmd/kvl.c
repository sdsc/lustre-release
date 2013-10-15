#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvl.h"

struct kvl_s *kvl_insert_key(struct kvl_s **kvl, char *key)
{
	struct kvl_s *new_node, *cur;

	if (!kvl)
		return NULL;

	cur = *kvl;

	new_node = calloc(sizeof(char), sizeof(struct kvl_s));
	if (!new_node)
		return NULL;

	new_node->key = strdup(key);
	if (!new_node->key) {
		free(new_node);
		return NULL;
	}

	/* insert as the head of the list */
	if (!cur) {
		*kvl = new_node;
		return new_node;
	}

	/* insert at the tail of the list */
	while (cur->next)
		cur = cur->next;

	cur->next = new_node;

	return new_node;
}

int kvl_insert_value(struct kvl_s *node, char *value)
{
	if ((!node) || (!value))
		return -1;

	node->value = strdup(value);
	if (!node->value)
		return -1;

	return 0;
}

struct kvl_s *kvl_find(struct kvl_s *kvl, char *key)
{
	struct kvl_s *cur = kvl;

	/*
	 * NOTE: the kvl->key might not necessarily be a complete name, so
	 * always check the length of the kvl->key against the passed in
	 * key
	 */
	while (cur) {
		if (strncmp(cur->key, key, strlen(cur->key)) == 0)
			return cur;
		cur = cur->next;
	}

	return NULL;
}

int kvl_convert_to_ui(struct kvl_s *kvl, unsigned int *v)
{
	char *end;

	if (kvl && kvl->value) {
		*v = strtoul(kvl->value, &end, 0);
		if (*end != 0) {
			fprintf(stderr, "Failed to parse [%s:%s]\n",
				kvl->key, kvl->value);
			return -1;
		}
	}

	return 0;
}

void kvl_free(struct kvl_s *kvl)
{
	struct kvl_s *free_node, *next;
	next = kvl;
	while (next) {
		free_node = next;
		next = next->next;
		free(free_node->key);
		free(free_node->value);
		free(free_node);
	}
}

