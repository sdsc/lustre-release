#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <dlfcn.h>
#include <getopt.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <readline/chardefs.h>
#include <libauto_complete.h>
#include "kvl.h"

#define LIST_FORMAT "%s%*s%s"
#define LIST_SEP (' ')
#define LIB_PRE "lib"
#define LIB_POST ".so"
#define MAX_LIB_NAME 128
#define MAX_SEP_LEN 30
#define MAX_STR_LEN 256

enum e_match_rc {
	EN_MATCH_MATCH = 0,
	EN_MATCH_NO_MATCH,
	EN_MATCH_MULTI_MATCH,
	EN_MATCH_MATCH_VALUE,
	EN_MATCH_ERROR,
};

struct callback_ll_s {
	struct callback_ll_s *next;
	char *fn_name;
	cmd_cb_t cb;
};

/*
 * All the commands that we have to work with
 */
struct callback_ll_s *loaded_cbs;
cYAML *g_usr_cmds;
void *g_lib_handle;

static void free_assoc_data(void *usr_data)
{
	if (usr_data)
		free(usr_data);
	else
		fprintf(rl_outstream, "Unexpected: Called with NULL pointer\n");
}

static cmd_cb_t find_cb(char *fn_name)
{
	struct callback_ll_s *node = loaded_cbs;

	while (node) {
		if (!strncmp(node->fn_name, fn_name, strlen(fn_name)))
			return node->cb;
		node = node->next;
	}

	return NULL;
}

static int insert_cb(char *fn_name, cmd_cb_t cb)
{
	struct callback_ll_s *new_node, *cur;

	cur = loaded_cbs;

	new_node = calloc(sizeof(char),
			  sizeof(struct callback_ll_s));
	if (!new_node)
		return -1;

	new_node->cb = cb;
	new_node->fn_name = strdup(fn_name);
	if (!new_node->fn_name) {
		free(new_node);
		return -1;
	}

	/* insert as the head of the list */
	if (!cur) {
		loaded_cbs = new_node;
		return 0;
	}

	/* insert at the tail of the list */
	while (cur->next)
		cur = cur->next;
	cur->next = new_node;

	return 0;
}

static enum e_match_rc yaml_get_help_info(cYAML **last, char **cmd,
					  char **help, const char *text)
{
	cYAML *cj = *last;
	cYAML *value = NULL, *jhelp = NULL;

	if (cj == NULL) {
		*cmd = NULL;
		*help = NULL;
		*last = NULL;
		return EN_MATCH_NO_MATCH;
	}

	/* return MATCH_VALUE to indicate that what's being returned is
	 * the description of the value
	 */

	if ((cj->string) &&
		(strncmp(cj->string, "lnet_cmd_value",
			 strlen("lnet_cmd_value")) == 0)) {
		jhelp = cYAML_get_object_item(cj, "lnet_cmd_help");
		*cmd = "value";
		*help = jhelp->valuestring;
		*last = (*last)->next;
		return EN_MATCH_MATCH_VALUE;
	}

	if ((cj->string) &&
	    (strncmp(cj->string, "lnet_cmd", strlen("lnet_cmd")) == 0)) {
		*last = (*last)->next;
		return EN_MATCH_NO_MATCH;
	}

	/*
	 * don't display entries which past the number of instances
	 * allowed
	 */
	if ((cj->user_data) && (*((int *)(cj->user_data)) <= 0)) {
		*last = (*last)->next;
		return EN_MATCH_NO_MATCH;
	}

	value = cYAML_get_object_item(cj, "lnet_cmd_help");

	if (strncmp(cj->string, text, strlen(text)) == 0) {
		*cmd = cj->string;
		*help = (value) ? value->valuestring : NULL;
		*last = (*last)->next;
	} else {
		*last = (*last)->next;
		return EN_MATCH_NO_MATCH;
	}


	return EN_MATCH_MATCH;
}

static enum e_match_rc yaml_find_obj(char *token, cYAML *obj, cYAML **ret,
				     int *entire_word_matched)
{
	cYAML *cj;
	cYAML *p = obj;
	cYAML *found = NULL;
	int num_found = 0;
	int tok_len = strlen(token);

	/* reset to the beginning of the list */
	while (p->prev)
		p = p->prev;

	cj = p;

	while (cj) {
		if (strncmp(token, cj->string, tok_len) == 0) {
			found = cj;
			num_found++;
		}
		cj = cj->next;
	}

	if (num_found > 1) {
		/* set the return to the top value as we don't know which
		 * one was meant */
		*ret = p;
		return EN_MATCH_MULTI_MATCH;
	}

	if (num_found == 1) {
		*ret = found;
		if (entire_word_matched &&
		    (tok_len == strlen(found->string)))
			*entire_word_matched = 1;
		return EN_MATCH_MATCH;
	}

	return EN_MATCH_NO_MATCH;
}

cmd_cb_t lookup_cb(char *fn_name)
{
	cmd_cb_t cb;
	void *v;
	char *error;

	/*
	 * check to see if the library has already been loaded if not load
	 */
	cb = find_cb(fn_name);

	if (cb)
		return cb;

	/* clear existing errors */
	dlerror();

	*(void **)(&v) = dlsym(g_lib_handle, fn_name);
	error = dlerror();
	if (error != NULL) {
		fprintf(rl_outstream, "%s\n", error);
		cb = NULL;
	}

	cb = (cmd_cb_t)v;

	if (cb) {
		/* insert the cb handle on the list */
		if (insert_cb(fn_name, cb) != 0)
			/* even if we fail still return the callback, next
			 * time we'll just re load the cb
			 */
			fprintf(rl_outstream, "Failed to store cb\n");
	}

	return cb;
}

/*
 * yaml_match
 *
 *  . will be provided with the cYAML block that it should start the
 *  matching
 *  . Then it will try to see if this token is suppose to be paired with
 *  a value by looking for lnet_cmd_value
 *   . If so then it will consume and return the lnet_cmd_value yaml block
 *   . If not then it will try to match the token with a child of the yaml
 *   block provided
 *   . if no match is found it'll look for lnet_cmd_back, if it finds it,
 *   it'll store it and go back one, and then try to match there
 *   It'll repeat this until the back is 0.  If there is a back at the
 *   higher level it will use that.
 *   If a match is found then it will insert that yaml block at the top of
 *   the linked list and returns that.
 *   If more than one match is found, then it will fail the match as
 *   inconclusive and will return the top block which it was looking at.
 *   . the caller is expected to provide this yaml block
 *  If a callback is found at this level then it will return it.  It's up
 *  to the caller to decide whether to keep it or ignore it.
  */

static enum e_match_rc yaml_match(char *token, cYAML_ll_t **start, cmd_cb_t *cb,
				  char **cmd_match, int *entire_word_matched)
{
	cYAML *obj = (*start)->obj;
	cYAML *found = NULL;
	cYAML *value = NULL, *back = NULL;
	enum e_match_rc rc = EN_MATCH_NO_MATCH;
	cYAML_ll_t *cjl = NULL;
	int iter = 0, i, instances = 1;
	*cmd_match = NULL;

	if (strlen(token) != 0) {
		/* fist check that this yaml is suppose to have a value */
		value = cYAML_get_object_item(obj, "lnet_cmd_value");
		if (value) {
			/* stick this value at the top of the list  and return
			* since we'll consume this token as value */
			if (cYAML_ll_push(value, start) != 0) {
				printf("ran out of memory\n");
				return EN_MATCH_ERROR;
			}

			return EN_MATCH_MATCH_VALUE;
		}

		rc = yaml_find_obj(token, obj->child, &found,
				entire_word_matched);
	}

	if ((rc == EN_MATCH_NO_MATCH) || (strlen(token) == 0)) {
		back = cYAML_get_object_item(obj, "lnet_cmd_back");
		if (back) {
			cYAML *p;
			iter = back->valueint;
			for (i = 0; i < iter; i++) {
				p = cYAML_ll_pop(start);
				if (!p)
					break;
			}

			cjl = *start;

			if (strlen(token) != 0) {
				rc = yaml_find_obj(token, cjl->obj->child,
						   &found,
						   entire_word_matched);
				if ((rc == EN_MATCH_MULTI_MATCH) ||
				    (rc == EN_MATCH_NO_MATCH))
					return rc;

				p = found;

				*cmd_match = (found) ? found->string : NULL;
			} else {
				/* since there is nothign to try and match
				 * then go up one level to list all
				 * possibilities under that level
				 */
				return EN_MATCH_MULTI_MATCH;
			}

			if (p) {
				found = p;
				rc = EN_MATCH_MATCH;
				goto match;
			}

			return EN_MATCH_NO_MATCH;
		}
	}

match:
	if (rc != EN_MATCH_MATCH)
		return rc;

	/*
	* check to see how many times this option can be present in the
	* command line
	*/
	value = cYAML_get_object_item(obj, "lnet_cmd_instances");
	if (value)
		instances = obj->valueint;

	/* updated the number of match instances */
	if (found->user_data)
		(*((int *)(found->user_data)))--;
	else {
		found->user_data = calloc(sizeof(char),
					sizeof(int));
		if (!found->user_data)
			return EN_MATCH_NO_MATCH;
		*((int *)(found->user_data)) = instances - 1;
	}

	/*
	* If the number of instances is
	* exceeded then return NO MATCH
	*/
	if (*((int *)(found->user_data)) < 0)
		return EN_MATCH_NO_MATCH;

	/* return the exact string of the command */
	*cmd_match = (found) ? found->string : NULL;

	if (cYAML_ll_push(found, start) != 0) {
		printf("ran out of memory\n");
		return EN_MATCH_ERROR;
	}

	/* find the callback and return */
	if (cb) {
		value = cYAML_get_object_item(found, "lnet_cmd_callback");
		if (value)
			*cb = lookup_cb(value->valuestring);
	}


	return rc;
}

char *cmd_generator(const char *text, int state)
{
	char **tokens;
	int i = 0;
	enum e_match_rc rc = EN_MATCH_MULTI_MATCH;
	cYAML_ll_t *ll = NULL;
	static cYAML *last;
	char *cmd = NULL;
	char *help = NULL;
	int entire_word_matched = 0;
	char *match = NULL;

	if (!state) {
		ll = calloc(sizeof(char),
			sizeof(cYAML_ll_t));

		ll->obj = g_usr_cmds;

		last = NULL;

		history_word_delimiters = " ";

		/* will use what's actually on the command line */
		tokens = history_tokenize((char *)rl_line_buffer);

		if (tokens) {
			/* feed the text word for word into the
			 * YAML matcher function. Feed NULL to
			 * indicate end of matching */
			while (tokens[i] != NULL) {
				/* The YAML matcher function will return
				 * cYAML struct, which can be passed to
				 * a function to print out the help info */
				entire_word_matched = 0;
				rc = yaml_match(tokens[i], &ll, NULL,
						&cmd, &entire_word_matched);
				if ((rc == EN_MATCH_ERROR) ||
				    (rc == EN_MATCH_NO_MATCH))
					goto function_exit;
				else if (rc == EN_MATCH_MULTI_MATCH)
					break;
				i++;
			}

			if ((rc != EN_MATCH_MULTI_MATCH) &&
			    (strlen(text) == 0)) {
				rc = yaml_match((char *)text, &ll, NULL,
						&cmd, &entire_word_matched);
				if (rc == EN_MATCH_ERROR)
					goto function_exit;
			}

		}

		if ((rc == EN_MATCH_MATCH_VALUE) && (strlen(text) != 0))
			/* this means that we hit tab while we are still typing
			* a value, in this case we do not want to perform auto
			* complete
			*/
			goto function_exit;

#if RL_READLINE_VERSION >= 0x0600
		if ((cmd) && (rc == EN_MATCH_MATCH) && !entire_word_matched &&
		    (rl_completion_invoking_key == TAB)) {
#else
		if ((cmd) && (rc == EN_MATCH_MATCH) && !entire_word_matched) {
#endif
			/* this means that we matched on a partial word so
			 * and we hit tab, so just auto complete this word
			 */
			last = NULL;
			match = strdup(cmd);
			goto function_exit;
		}

		last = ll->obj->child;
	}


	/* if we get mutli match then print all of them */
	if (last) {
#if RL_READLINE_VERSION >= 0x0600
		char *dup = NULL;
#endif

		while (last &&
		       (rc = yaml_get_help_info(&last, &cmd, &help, text))
				== EN_MATCH_NO_MATCH) {
			}
#if RL_READLINE_VERSION >= 0x0600
		if (cmd && help && (rl_completion_invoking_key == LIST)) {
			int cmd_len = strlen(cmd);
			int help_len = strlen(help);
			int width;

			/* This will be parsed later on to get
			 * printed properly
			 */
			width = ((MAX_SEP_LEN - strlen(cmd)) < 0) ? 0 :
			  MAX_SEP_LEN - strlen(cmd);

			dup = calloc(sizeof(char),
				     cmd_len + help_len + width + 1);
			if (!dup)
				goto function_exit;

			sprintf(dup, LIST_FORMAT,
				cmd, width, "", help);
			match = dup;
			goto function_exit;
		} else if ((rc == EN_MATCH_MATCH) && cmd) {
			match = strdup(cmd);
			goto function_exit;
		}
#else
/* only handle exact matches for earlier versions of readline */
		if ((rc == EN_MATCH_MATCH) && cmd) {
			match = strdup(cmd);
			goto function_exit;
		}
#endif
	}

function_exit:
	if (ll)
		cYAML_ll_free(ll);
	if (!match)
		/*
		 * if we're returning NULL, which indicates the end of our
		 * matching process, ensure you clean up the tree from our user
		 * data
		 */
		cYAML_clean_usr_data(g_usr_cmds, free_assoc_data);
	return match;
}

/*
 * cmd_completion
 *	Gives all the possible entry points from what's given
 *
 *	text: entered text
 *	start: Start index
 *	end: end index
 */
char **cmd_completion(const char *text, int start, int end)
{
	char **matches;

	matches = (char **)NULL;

	if (start == 0)
		matches = rl_completion_matches((char *)text, &cmd_generator);

	return matches;
}

static void print_syntax_error(char **tokens, int err, char *out_buf)
{
	int i = 0, j;
	char *l = out_buf;
	sprintf(l, "Syntax error:\n");
	while (tokens[i] != NULL) {
		l += strlen(l);
		sprintf(l, "%s ", tokens[i]);
		i++;
	}
	l += strlen(l);
	sprintf(l, "\n");
	for (i = 0; i < err; i++) {
		for (j = 0; j < strlen(tokens[i]) + 1; j++) {
			l += strlen(l);
			sprintf(l, "-");
		}
	}
	for (i = 0; i < strlen(tokens[err]); i++) {
		l += strlen(l);
		sprintf(l, "^");
	}
}

/*
 * parse_and_callback_tokenized
 *	Parses the input given and if the parse was successful, execute
 *	the derived call back
 */
int parse_and_callback_tokenized(char **tokens, cYAML **err_rc)
{
	int i = 0, success = 0;
	cYAML_ll_t *ll;
	char *cmd = NULL;
	cmd_cb_t cb = NULL;
	enum e_match_rc rc;
	struct kvl_s *kvl = NULL, *kvl_node = NULL;
	char err_str[MAX_STR_LEN];

	ll = calloc(sizeof(char),
		    sizeof(cYAML_ll_t));

	ll->obj = g_usr_cmds;

	if (tokens) {
		/* feed the text word for word into the YAML matcher
		 * function. */
		while (tokens[i] != NULL) {
			/* The assumption is that there is only one callback
			 * per command */
			rc = yaml_match(tokens[i], &ll, &cb,
					&cmd, NULL);
			if ((rc == EN_MATCH_ERROR) ||
			    (rc == EN_MATCH_NO_MATCH)) {
				/* if at anytime we get a NO_MATCH, then
				 * parsing has failed */
				success = -1;
				print_syntax_error(tokens, i, err_str);
				cYAML_build_error(success, -1, "lcmd", "syntax",
						  err_str, err_rc);
				goto function_exit;
			} else if (rc == EN_MATCH_MATCH) {
				kvl_node = kvl_insert_key(&kvl, tokens[i]);
				if (!kvl_node) {
					success = -1;
					cYAML_build_error(success, -1,
							  "lcmd", "system",
							  "Out Of Memory",
							  err_rc);
					goto function_exit;
				}
			} else if (rc == EN_MATCH_MATCH_VALUE) {
				if (kvl_insert_value(kvl_node, tokens[i])) {
					success = -1;
					cYAML_build_error(success, -1, "lcmd",
							  "system",
							  "Out Of Memory",
							  err_rc);
					goto function_exit;
				}
			}
			i++;
		}
	}

	if (cb)
		success = cb(kvl);
	else {
		if (tokens) {
			char *e = err_str;
			sprintf(e, "No callback found for command:\n");
			i = 0;
			while (tokens[i] != NULL) {
				e += strlen(e);
				sprintf(e, "%s ", tokens[i]);
				i++;
			}
			cYAML_build_error(success, -1, "lcmd", "cmd_library",
					err_str, err_rc);
		}
		success = -1;
	}

function_exit:
	cYAML_ll_free(ll);
	/* always clean the assoc data at the end of the matching */
	cYAML_clean_usr_data(g_usr_cmds, free_assoc_data);
	if (kvl)
		free(kvl);

	return success;
}

/*
 * parse_and_callback
 *	Parses the input given and if the parse was successful, execute
 *	the derived call back
 */
int parse_and_callback(const char *input, cYAML **err_rc)
{
	/* feed the text word for word into the YAML matcher. */
	char **tokens;

	history_word_delimiters = " ";

	/* will use what's actually on the command line */
	tokens = history_tokenize((char *)rl_line_buffer);

	return parse_and_callback_tokenized(tokens, err_rc);
}

void cmd_display(char **matches, int num_matches, int max_length)
{
#if RL_READLINE_VERSION >= 0x0600
	int i = 0, width, cmd_len, help_len;
	char *cmd;
	char *help, *start;
	char *sep;

	if (rl_completion_invoking_key == LIST) {
		fprintf(rl_outstream, "\n");
		for (i = 0; i < num_matches+1; i++) {
			sep = strchr(matches[i], LIST_SEP);
			if (!sep)
				continue;
			cmd_len = sep - matches[i];
			start = sep;
			while (*start == LIST_SEP)
				start++;
			help_len = (matches[i] + strlen(matches[i])) - (start);

			/* calculate the size of the command and help */
			cmd = calloc(sizeof(char),
				     cmd_len + 1);
			if (!cmd)
				continue;

			help = calloc(sizeof(char),
				      help_len + 1);
			if (!help) {
				free(cmd);
				continue;
			}

			strncpy(cmd, matches[i], cmd_len);
			cmd[cmd_len] = '\0';
			strncpy(help, start, help_len);
			help[help_len] = '\0';

			width = ((MAX_SEP_LEN - strlen(cmd)) < 0) ? 0 :
			  MAX_SEP_LEN - strlen(cmd);

			fprintf(rl_outstream,
				LIST_FORMAT"\n",
				cmd, width,
				"", help);
			free(cmd);
			free(help);
		}
	} else
#else
		rl_display_match_list(matches, num_matches, max_length);
#endif /* RL_READLINE_VERSION == 0x0600 */
	rl_forced_update_display();
}

int init_auto_complete(cYAML *cmds, char *usr_lib)
{
	g_usr_cmds = cmds;

	if (g_lib_handle) {
		dlclose(g_lib_handle);
		g_lib_handle = NULL;
	}

	g_lib_handle = dlopen(usr_lib, RTLD_LAZY);
	if (!g_lib_handle) {
		fprintf(rl_outstream, "%s shared library not found\n",
			usr_lib);
		return -1;
	}

	return 0;
}

void uninit_auto_complete()
{
	g_usr_cmds = NULL;
	dlclose(g_lib_handle);
	g_lib_handle = NULL;
}
