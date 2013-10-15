#ifndef LIBAUTO_COMPLETE_H
#define LIBAUTO_COMPLETE_H

#include <libcYAML.h>

#define LIST ('?')

extern char *cmd_generator(const char *text, int state);
extern char **cmd_completion(const char *text, int start, int end);
extern void cmd_display(char **matches, int num_matches, int max_length);
extern int parse_and_callback_tokenized(char **tokens, cYAML **err_rc);
extern int parse_and_callback(const char *input, cYAML **err_rc);
extern int init_auto_complete(cYAML *cmds, char *usr_lib);
extern void uninit_auto_complete();

#endif /* LIBAUTO_COMPLETE_H */
