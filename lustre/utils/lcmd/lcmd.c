#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <dlfcn.h>
#include <getopt.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <libcYAML.h>
#include <libauto_complete.h>

#define LUSTRE_CMD_FILE "lustre_cmds"
#define LUSTRE_CMD_LIB "liblustre_cfg_cmds.so"
#define CMD_LINE_PROMPT "lnet>>>"

#define CYAML_BUILD_PRINT_ERROR(rc, seq_no, cmd, entity, err_str, root) \
	do { \
		cYAML_build_error(rc, seq_no, cmd, entity, err_str, root); \
		cYAML_print_tree(*root, 0); \
		cYAML_free_tree(*root); \
	} while (0)

/*externs needed by getopt lib*/
extern char *optarg;
extern int optind;
extern int opterr;

static void lcmd_print_help(char **argv)
{
	fprintf(stderr, "%s [--cmd-file <file>] [--prompt-text <prompt>] "
			"[--interactive] [<command>]\n"
			"--cmd-file: specify the file describing the commands\n"
			"--prompt-text: prompt to display for interactive\n"
			"--interactive: continue to interactive mode possibly\n"
			"               after executing a command\n"
			"--cmd-lib: Specify the shared library where the command\n"
			"           functions are defined\n"
			"command: command to execute\n", argv[0]);
}

static void dump_history()
{
	HIST_ENTRY **l;
	int i;

	l = history_list();

	if (l) {
		for (i = 0; l[i] != NULL; i++)
			fprintf(rl_outstream, "%s\n",
				l[i]->line);
	}
}

static void remove_cmd_line_entry(char **argv, int index, int max)
{
	int i;
	argv[index] = NULL;

	for (i = index; i < max; i++)
		argv[i] = argv[i+1];
}

int main(int argc, char **argv)
{
	char *input, *cmd_lib = NULL;
	int cOpt;
	int success = 0;
	int interactive = 0;
	cYAML *lustre_cmds;
	cYAML *yaml_err = NULL;
	char *file = NULL, *prompt = NULL;
	const char *const short_options = "c:p:ih";
	const struct option long_options[] = {
		{"cmd-file", 1, NULL, 'c'},
		{"help", 0, NULL, 'h'},
		{"prompt-text", 1, NULL, 'p'},
		{"cmd-lib", 1, NULL, 'l'},
		{"interactive", 0, NULL, 'i'},
		{NULL, 0, NULL, 0}
	};
	char *cmd_line[argc+1];
	int i, r;
	int cmd_line_size = argc;

	memset(cmd_line, 0, sizeof(char *) * (argc + 1));
	/* copy the argv locally to be manipulated for the final execution */
	for (i = 0; i < argc; i++)
		cmd_line[i] = argv[i];

	/* ignore all unknown command line arguments.  These will be
	 * dealt with by the YAML parser */
	opterr = 0;

	/*now process command line arguments*/
	if (argc > 1) {
		while ((cOpt = getopt_long(argc, argv, short_options,
					   long_options, NULL)) != -1) {
			switch (cOpt) {
			case 'c':
				file = optarg;
				i = argc - cmd_line_size;
				r = optind-2-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				i = argc - cmd_line_size;
				r = optind-1-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				break;

			case 'i':
				interactive = 1;
				i = argc - cmd_line_size;
				r = optind-1-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				break;

			case 'l':
				cmd_lib = optarg;
				i = argc - cmd_line_size;
				r = optind-2-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				i = argc - cmd_line_size;
				r = optind-1-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				break;

			case 'p':
				prompt = optarg;
				i = argc - cmd_line_size;
				r = optind-2-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				i = argc - cmd_line_size;
				r = optind-1-i;
				remove_cmd_line_entry(cmd_line, r,
						      cmd_line_size--);
				break;

			case '?':
				continue;

			case 'h':
			default:
				lcmd_print_help(argv);
				return 1;
			}
		}
	}

	lustre_cmds = cYAML_build_tree(file ? file : LUSTRE_CMD_FILE, NULL, 0,
				       &yaml_err);
	if (!lustre_cmds) {
		cYAML_print_tree(yaml_err, 0);
		cYAML_free_tree(yaml_err);
		return -1;
	}

	/* init the auto complete library with the information it needs */
	if (init_auto_complete(lustre_cmds, cmd_lib ?
					cmd_lib : LUSTRE_CMD_LIB)) {
		CYAML_BUILD_PRINT_ERROR(-1, -1, "build", "auto_complete",
					"Failed to initialize "
					"auto-complete library",
					&yaml_err);
		return -1;
	}

	/*
	 * If the command is specified on the command line, then assume
	 * that we will execute the command and exit, unless the
	 * interactive option is specified
	 */
	if (cmd_line[1] && strlen(cmd_line[1])) {
		cmd_line[cmd_line_size] = NULL;
		success = parse_and_callback_tokenized(&cmd_line[1], &yaml_err);
		/* print YAML error if any */
		if (yaml_err) {
			cYAML_print_tree(yaml_err, 0);
			cYAML_free_tree(yaml_err);
		}
	}

	/*
	 * if interactive is not specified at the command line return now
	 */
	if (!interactive)
		return success;

	rl_attempted_completion_function = cmd_completion;
	rl_completion_entry_function = cmd_generator;
	rl_completion_display_matches_hook = cmd_display;
	rl_ignore_completion_duplicates = 0;

	/* initialize History */
	using_history();

	/* enable timestamping for history */
	history_write_timestamps = 1;

	for(;;) {
		yaml_err = NULL;

		/* Configure readline to auto-complete paths
		 * when the tab key is hit. */
		rl_bind_key(LIST, rl_possible_completions);
		rl_bind_key(TAB, rl_complete);

		input = readline(prompt ? prompt : CMD_LINE_PROMPT);

		if (!input) {
			fprintf(stderr, "\n");
			break;
		}

		/* Add input to history. */
		if (strlen(input) > 0)
			add_history(input);

		if (strcmp(input, "print cmd tree") == 0) {
			cYAML_print_tree(lustre_cmds, 0);
			continue;
		}

		if (strcmp(input, "history") == 0) {
			dump_history();
			continue;
		}

		if (strcmp(input, "clear history") == 0) {
			clear_history();
			continue;
		}

		if (strcmp(input, "reload cmd lib") == 0) {
			if (init_auto_complete(lustre_cmds, cmd_lib ?
						cmd_lib : LUSTRE_CMD_LIB)) {
				fprintf(stderr, "Failed to initialize "
						"auto-complete library\n");
				break;
			}
		}

		/* Check for end. */
		if ((strcmp(input, "quit") == 0) ||
		    (strcmp(input, "exit") == 0)) {
			break;
		}

		/* parse out the line */
		parse_and_callback(input, &yaml_err);

		/* print YAML error if any */
		if (yaml_err) {
			cYAML_print_tree(yaml_err, 0);
			cYAML_free_tree(yaml_err);
		}

		/* Free input. */
		free(input);
	}

	uninit_auto_complete();
	cYAML_free_tree(lustre_cmds);

	return 0;
}
