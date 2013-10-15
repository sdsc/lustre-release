#ifndef __KVL_H__
#define __KVL_H__

typedef struct kvl_s {
	struct kvl_s *next;
	char *key;
	char *value;
} kvl_t;

typedef int (*cmd_cb_t)(kvl_t *);

extern kvl_t *kvl_insert_key(kvl_t **kvl, char *key);
extern int kvl_insert_value(kvl_t *node, char *value);
extern kvl_t *kvl_find(kvl_t *kvl, char *key);
extern int kvl_convert_to_ui(kvl_t *kvl, unsigned int *v);
extern void kvl_free(kvl_t *kvl);
#endif
