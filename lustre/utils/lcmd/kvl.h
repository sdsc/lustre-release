#ifndef __KVL_H__
#define __KVL_H__

struct kvl_s {
	struct kvl_s *next;
	char *key;
	char *value;
};

typedef int (*cmd_cb_t)(struct kvl_s *);

extern struct kvl_s *kvl_insert_key(struct kvl_s **kvl, char *key);
extern int kvl_insert_value(struct kvl_s *node, char *value);
extern struct kvl_s *kvl_find(struct kvl_s *kvl, char *key);
extern int kvl_convert_to_ui(struct kvl_s *kvl, unsigned int *v);
extern void kvl_free(struct kvl_s *kvl);
#endif
