/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2014, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/libcfs/debug.c
 *
 * Author: Phil Schwan <phil@clusterfs.com>
 *
 */

# define DEBUG_SUBSYSTEM S_LNET

#include <linux/kthread.h>
#include <libcfs/libcfs.h>
#include "tracefile.h"

static char debug_file_name[1024];

unsigned int libcfs_subsystem_debug = ~0;
CFS_MODULE_PARM(libcfs_subsystem_debug, "i", int, 0644,
                "Lustre kernel debug subsystem mask");
EXPORT_SYMBOL(libcfs_subsystem_debug);

unsigned int libcfs_debug = (D_CANTMASK |
			     D_NETERROR | D_HA | D_CONFIG | D_IOCTL | D_LFSCK);
CFS_MODULE_PARM(libcfs_debug, "i", int, 0644,
                "Lustre kernel debug mask");
EXPORT_SYMBOL(libcfs_debug);

unsigned int libcfs_debug_mb;
CFS_MODULE_PARM(libcfs_debug_mb, "i", uint, 0644,
                "Total debug buffer size.");

unsigned int libcfs_printk = D_CANTMASK;
CFS_MODULE_PARM(libcfs_printk, "i", uint, 0644,
                "Lustre kernel debug console mask");

unsigned int libcfs_console_ratelimit = 1;
CFS_MODULE_PARM(libcfs_console_ratelimit, "i", uint, 0644,
                "Lustre kernel debug console ratelimit (0 to disable)");

unsigned int libcfs_console_max_delay;
CFS_MODULE_PARM(libcfs_console_max_delay, "l", uint, 0644,
                "Lustre kernel debug console max delay (jiffies)");

unsigned int libcfs_console_min_delay;
CFS_MODULE_PARM(libcfs_console_min_delay, "l", uint, 0644,
                "Lustre kernel debug console min delay (jiffies)");

unsigned int libcfs_console_backoff = CDEBUG_DEFAULT_BACKOFF;
CFS_MODULE_PARM(libcfs_console_backoff, "i", uint, 0644,
                "Lustre kernel debug console backoff factor");

unsigned int libcfs_debug_binary = 1;

unsigned int libcfs_stack = 3 * THREAD_SIZE / 4;
EXPORT_SYMBOL(libcfs_stack);

unsigned int libcfs_catastrophe;
EXPORT_SYMBOL(libcfs_catastrophe);

unsigned int libcfs_watchdog_ratelimit = 300;

unsigned int libcfs_panic_on_lbug = 1;
CFS_MODULE_PARM(libcfs_panic_on_lbug, "i", uint, 0644,
                "Lustre kernel panic on LBUG");

atomic_t libcfs_kmemory = ATOMIC_INIT(0);
EXPORT_SYMBOL(libcfs_kmemory);

atomic_t func_filter_enabled = ATOMIC_INIT(0);
EXPORT_SYMBOL(func_filter_enabled);

LIST_HEAD(func_filter_patts);
EXPORT_SYMBOL(func_filter_patts);

DEFINE_RWLOCK(func_filter_lock);

struct cfs_hash *libcfs_func_hash;
EXPORT_SYMBOL(libcfs_func_hash);

struct cfs_hash *libcfs_matched_func_hash;
EXPORT_SYMBOL(libcfs_matched_func_hash);

atomic_t func_filter_syncing = ATOMIC_INIT(0);

static wait_queue_head_t debug_ctlwq;

char libcfs_debug_file_path_arr[PATH_MAX] = LIBCFS_DEBUG_FILE_PATH_DEFAULT;

/* We need to pass a pointer here, but elsewhere this must be a const */
static char *libcfs_debug_file_path;
CFS_MODULE_PARM(libcfs_debug_file_path, "s", charp, 0644,
                "Path for dumping debug logs, "
                "set 'NONE' to prevent log dumping");

int libcfs_panic_in_progress;

#define HASH_FUNC_BKT_BITS 5
#define HASH_FUNC_CUR_BITS 7
#define HASH_FUNC_MAX_BITS 12

static void func_getref(struct func_desc *func)
{
	atomic_inc(&func->func_refcount);
}

static void func_putref(struct func_desc *func)
{
	if (atomic_dec_and_test(&func->func_refcount)) {
		LASSERT(hlist_unhashed(&func->func_hash));
		LIBCFS_FREE(func, sizeof(struct func_desc));
		EXIT;
	}
}

static void func_putref_locked(struct func_desc *func)
{
	LASSERT(atomic_read(&func->func_refcount) > 1);

	atomic_dec(&func->func_refcount);
}

static __u32 func_hashfn(struct cfs_hash *hash_body, const void *key,
			 unsigned mask)
{
	int i;
	__u32 result;
	char *func_name;

	result = 0;
	func_name = (char *)key;
	for (i = 0; i < 128; i++) {
		if (func_name[i] == '\0')
			break;
		result = (result << 4)^(result >> 28) ^ func_name[i];
	}
	return result % mask;
}

static void *func_key(struct hlist_node *hnode)
{
	struct func_desc *func;

	func = hlist_entry(hnode, struct func_desc, func_hash);
	return func->func_name;
}

static int
func_hashkey_keycmp(const void *key, struct hlist_node *compared_hnode)
{
	char *func_name;
	struct func_desc *func;

	func_name = (char *)key;
	func = hlist_entry(compared_hnode, struct func_desc, func_hash);
	return !strncmp(func_name, func->func_name, 128);
}

static void *func_hashobject(struct hlist_node *hnode)
{
	return hlist_entry(hnode, struct func_desc, func_hash);
}

static void func_hashrefcount_get(struct cfs_hash *hs, struct hlist_node *hnode)
{
	struct func_desc *func;

	func = hlist_entry(hnode, struct func_desc, func_hash);
	func_getref(func);
}

static void func_hashrefcount_put_locked(struct cfs_hash *hs,
					 struct hlist_node *hnode)
{
	struct func_desc *func;

	func = hlist_entry(hnode, struct func_desc, func_hash);
	func_putref_locked(func);
}

struct cfs_hash_ops func_hash_operations = {
	.hs_hash        = func_hashfn,
	.hs_key         = func_key,
	.hs_keycmp      = func_hashkey_keycmp,
	.hs_object      = func_hashobject,
	.hs_get         = func_hashrefcount_get,
	.hs_put_locked  = func_hashrefcount_put_locked,
};

static int libcfs_func_filter_init(void)
{
	libcfs_func_hash = cfs_hash_create("FUNCS",
					   HASH_FUNC_CUR_BITS,
					   HASH_FUNC_MAX_BITS,
					   HASH_FUNC_BKT_BITS, 0,
					   CFS_HASH_MIN_THETA,
					   CFS_HASH_MAX_THETA,
					   &func_hash_operations,
					   CFS_HASH_DEFAULT);
	if (!libcfs_func_hash)
		return -ENOMEM;

	libcfs_matched_func_hash = cfs_hash_create("MATCHED_FUNCS",
					   HASH_FUNC_CUR_BITS,
					   HASH_FUNC_MAX_BITS,
					   HASH_FUNC_BKT_BITS, 0,
					   CFS_HASH_MIN_THETA,
					   CFS_HASH_MAX_THETA,
					   &func_hash_operations,
					   CFS_HASH_DEFAULT);
	if (!libcfs_matched_func_hash) {
		cfs_hash_putref(libcfs_func_hash);
		return -ENOMEM;
	}

	return 0;
}

static void libcfs_func_filter_cleanup(void)
{
	cfs_hash_putref(libcfs_func_hash);
	cfs_hash_putref(libcfs_matched_func_hash);
}

int libcfs_func_pattern_match(const char *str)
{
	int matched = 0;
	struct func_pattern *pattern;
	struct func_desc *func;
	char *ptr;

	if (atomic_read(&func_filter_enabled) == 0)
		return 1;

	/* func filter hash syncing? go to direct check */
	if (atomic_read(&func_filter_syncing))
		goto direct_check;

	/* check whether in matched func hash table */
	func = cfs_hash_lookup(libcfs_matched_func_hash, str);
	if (func)
		return 1;

	/* check whether in func hash table */
	func = cfs_hash_lookup(libcfs_func_hash, str);
	if (func) {
		/* in total func hashtable, not in matched func hashtable,
		 * means not matched.
		 * */
		return 0;
	}

	/* create on desc and add into func hashtable */
	LIBCFS_ALLOC(func, sizeof(struct func_desc));
	if (func == NULL) {
		CWARN("ENOMEM, failed to alloc!\n");
		return 0;
	}
	atomic_set(&func->func_refcount, 1);

	strlcpy(func->func_name, str, sizeof(func->func_name));
	INIT_HLIST_NODE(&func->func_hash);

	cfs_hash_add(libcfs_func_hash, func->func_name, &func->func_hash);
direct_check:
	read_lock(&func_filter_lock);

	if (list_empty(&func_filter_patts)) {
		read_unlock(&func_filter_lock);
		return 1;
	}

	list_for_each_entry(pattern, &func_filter_patts, node) {
		char *regex = pattern->patt;
		int len = pattern->len;

		switch (pattern->type) {
		case MATCH_FULL:
		case MATCH_FRONT:
			if (strncasecmp(str, regex, len) == 0)
				matched = 1;
			break;
		case MATCH_MIDDLE:
			if (strstr(str, regex))
				matched = 1;
			break;
		case MATCH_END:
			ptr = strstr(str, regex);
			if (ptr && (ptr[len] == 0))
				matched = 1;
		case MATCH_MAX:
			CERROR("Bug, invalid pattern type:%d", MATCH_MAX);
			matched = 1;
			break;
		}

		if (matched)
			break;
	}

	read_unlock(&func_filter_lock);

	if (atomic_read(&func_filter_syncing))
		goto out;
	if (matched) {
		func = NULL;
		/* create an desc and add into matched func hashtable */
		LIBCFS_ALLOC(func, sizeof(struct func_desc));
		if (func == NULL) {
			CWARN("ENOMEM, failed to alloc!\n");
			return 0;
		}
		atomic_set(&func->func_refcount, 1);
		strlcpy(func->func_name, str, sizeof(func->func_name));
		INIT_HLIST_NODE(&func->func_hash);
		cfs_hash_add(libcfs_matched_func_hash,
			     func->func_name, &func->func_hash);
	}
out:
	return matched;
}
EXPORT_SYMBOL(libcfs_func_pattern_match);

static int valid_char(char ch)
{
	return ch != 0 && !isspace(ch) && ch != '+' &&
		ch != '-' && ch != '^' && ch != '$';
}

struct func_pattern_args {
	struct func_pattern	*pattern;
	int			op;
};

static int
libcfs_update_matched_func_hash(struct cfs_hash *hs, struct cfs_hash_bd *bd,
				struct hlist_node *hnode, void *data)
{
	struct func_desc *func;
	struct func_pattern_args *args = (struct func_pattern_args *)data;
	char *func_name;
	char *regex;
	char *ptr;
	int len;
	int matched = 0;
	int need_cmp_add = 0;

	func = hlist_entry(hnode, struct func_desc, func_hash);
	func_name = func->func_name;
	regex = args->pattern->patt;
	len = args->pattern->len;

	func = cfs_hash_lookup(libcfs_matched_func_hash, func->func_name);
	if (!func) {
		/* add new pattern */
		if (args->op)
			need_cmp_add = 1;
		else
			return 0;
	}

	switch (args->pattern->type) {
	case MATCH_FULL:
	case MATCH_FRONT:
		if (strncasecmp(func_name, regex, len) == 0)
			matched = 1;
		break;
	case MATCH_MIDDLE:
		if (strstr(func_name, regex))
			matched = 1;
		break;
	case MATCH_END:
		ptr = strstr(func_name, regex);
		if (ptr && (ptr[len] == 0))
			matched = 1;
	case MATCH_MAX:
		CERROR("Bug, invalid pattern type:%d", MATCH_MAX);
		return 1;
		break;
	}
	if (matched) {
		if (need_cmp_add) {
			func = NULL;
			/* create an desc and add into matched func hashtable */
			LIBCFS_ALLOC(func, sizeof(struct func_desc));
			if (func == NULL) {
				CWARN("ENOMEM, failed to alloc!\n");
				return -ENOMEM;
			}
			atomic_set(&func->func_refcount, 1);
			strlcpy(func->func_name, func_name,
				sizeof(func->func_name));
			INIT_HLIST_NODE(&func->func_hash);
			cfs_hash_add(libcfs_matched_func_hash,
				     func_name,
				     &func->func_hash);
		} else if (!args->op) {
			/* need delete this item */
			cfs_hash_del(libcfs_matched_func_hash,
				     func_name,
				     &func->func_hash);
			func_putref(func);
		}
	}
	return 0;
}

static void
libcfs_refresh_func_hashtable(struct func_pattern *pattern, int add)
{
	struct func_pattern_args op_args = {
		.pattern = pattern,
		.op = add,
	};
	atomic_set(&func_filter_syncing, 1);
	cfs_hash_for_each(libcfs_func_hash,
			  libcfs_update_matched_func_hash,
			  &op_args);
	atomic_set(&func_filter_syncing, 0);
}

static int libcfs_func_filter_set_pattern(const char *str)
{
	char op = 0;
	enum MATCH_TYPE type = MATCH_MIDDLE;
	int len, found = 0;
	struct func_pattern *pattern;
	struct func_pattern *new_patt;

	/* <str> must be a list of tokens separated by whitespace
	 * and optionally an operator ('+' or '-').
	 * */
	while (*str != 0) {
		while (isspace(*str))
			str++;
		if (*str == 0)
			break;
		if (*str == '+' || *str == '-') {
			op = *str++;
			while (isspace(*str))
				str++;
			/* trailing op */
			if (*str == 0)
				return -EINVAL;
		}

		if (*str == '^') {
			type = MATCH_FRONT;
			str++;
		}

		/* find token length */
		len = 0;
		while (valid_char(str[len]))
			len++;

		if (str[len] == '$')
			type = MATCH_END;

		if (op != '-') {
			new_patt = kzalloc(sizeof(struct func_pattern),
					   GFP_KERNEL);
			if (!new_patt)
				return -ENOMEM;
			new_patt->type = type;
			new_patt->len = len;
			strncpy(new_patt->patt, str, len);
			INIT_LIST_HEAD(&new_patt->node);
		}
		found = 0;
		write_lock(&func_filter_lock);
		list_for_each_entry(pattern, &func_filter_patts, node) {
			/* found a matched one */
			if (pattern->len == len &&
			    !strncasecmp(str, pattern->patt, len) &&
			    type == pattern->type) {
				found = 1;
				break;
			}
		}

		if (op == '-') {
			if (found) {
				list_del(&pattern->node);
				/* delete matched func */
				libcfs_refresh_func_hashtable(pattern, 0);
				kfree(pattern);
				/* empty? mark it as disabled */
				if (list_empty(&func_filter_patts))
					atomic_set(&func_filter_enabled, 0);
			} else {
				CWARN("You are trying to remove a "
				      "non-existent pattern\n");
			}
		} else {
			if (found) {
				kfree(new_patt);
			} else {
				list_add_tail(&new_patt->node,
					      &func_filter_patts);
				atomic_cmpxchg(&func_filter_enabled, 0, 1);
				/* add matched func */
				libcfs_refresh_func_hashtable(new_patt, 1);
			}
		}
		write_unlock(&func_filter_lock);

		str += len;
		if (type == MATCH_END)
			str++;
	}

	return 0;
}

int libcfs_func_filter_set(const char *str)
{
	int m = 0;
	int matched, n, t;
	struct func_pattern *pattern;
	struct func_pattern *tmp;

	/* Allow a number for backwards compatibility */
	for (n = strlen(str); n > 0; n--)
		if (!isspace(str[n-1]))
			break;
	matched = n;

	t = sscanf(str, "%i%n", &m, &matched);
	if (t >= 1 && matched == n) {
		/* don't print warning for lctl set_param func_filter=0 or -1 */
		if (m != 0 && m != -1) {
			CWARN("You are trying to use a numerical value for the "
				"func filter.\n");
			return -EINVAL;
		}
		/* disable func filter? clear the pattern list */
		write_lock(&func_filter_lock);
		list_for_each_entry_safe(pattern, tmp,
					 &func_filter_patts, node) {
			list_del(&pattern->node);
			kfree(&pattern);
		}
		atomic_set(&func_filter_enabled, 0);
		write_unlock(&func_filter_lock);
		return 0;
	}

	return libcfs_func_filter_set_pattern(str);
}
EXPORT_SYMBOL(libcfs_func_filter_set);

/* libcfs_debug_token2mask() expects the returned
 * string in lower-case */
static const char *libcfs_debug_subsys2str(int subsys)
{
	static const char *libcfs_debug_subsystems[] = LIBCFS_DEBUG_SUBSYS_NAMES;

	if (subsys >= ARRAY_SIZE(libcfs_debug_subsystems))
		return NULL;

	return libcfs_debug_subsystems[subsys];
}

/* libcfs_debug_token2mask() expects the returned
 * string in lower-case */
static const char *libcfs_debug_dbg2str(int debug)
{
	static const char *libcfs_debug_masks[] = LIBCFS_DEBUG_MASKS_NAMES;

	if (debug >= ARRAY_SIZE(libcfs_debug_masks))
		return NULL;

	return libcfs_debug_masks[debug];
}

int
libcfs_debug_mask2str(char *str, int size, int mask, int is_subsys)
{
        const char *(*fn)(int bit) = is_subsys ? libcfs_debug_subsys2str :
                                                 libcfs_debug_dbg2str;
        int           len = 0;
        const char   *token;
        int           i;

        if (mask == 0) {                        /* "0" */
                if (size > 0)
                        str[0] = '0';
                len = 1;
        } else {                                /* space-separated tokens */
                for (i = 0; i < 32; i++) {
                        if ((mask & (1 << i)) == 0)
                                continue;

                        token = fn(i);
                        if (token == NULL)              /* unused bit */
                                continue;

                        if (len > 0) {                  /* separator? */
                                if (len < size)
                                        str[len] = ' ';
                                len++;
                        }

                        while (*token != 0) {
                                if (len < size)
                                        str[len] = *token;
                                token++;
                                len++;
                        }
                }
        }

        /* terminate 'str' */
        if (len < size)
                str[len] = 0;
        else
                str[size - 1] = 0;

        return len;
}

int
libcfs_debug_str2mask(int *mask, const char *str, int is_subsys)
{
        const char *(*fn)(int bit) = is_subsys ? libcfs_debug_subsys2str :
                                                 libcfs_debug_dbg2str;
        int         m = 0;
        int         matched;
        int         n;
        int         t;

        /* Allow a number for backwards compatibility */

        for (n = strlen(str); n > 0; n--)
                if (!isspace(str[n-1]))
                        break;
        matched = n;

        if ((t = sscanf(str, "%i%n", &m, &matched)) >= 1 &&
            matched == n) {
                /* don't print warning for lctl set_param debug=0 or -1 */
                if (m != 0 && m != -1)
                        CWARN("You are trying to use a numerical value for the "
                              "mask - this will be deprecated in a future "
                              "release.\n");
                *mask = m;
                return 0;
        }

        return cfs_str2mask(str, fn, mask, is_subsys ? 0 : D_CANTMASK,
                            0xffffffff);
}

/**
 * Dump Lustre log to ::debug_file_path by calling tracefile_dump_all_pages()
 */
void libcfs_debug_dumplog_internal(void *arg)
{
	static time_t last_dump_time;
	time_t current_time;
	void *journal_info;

	journal_info = current->journal_info;
	current->journal_info = NULL;

	current_time = cfs_time_current_sec();

	if (strncmp(libcfs_debug_file_path_arr, "NONE", 4) != 0 &&
	    current_time > last_dump_time) {
		last_dump_time = current_time;
		snprintf(debug_file_name, sizeof(debug_file_name) - 1,
			 "%s.%ld." LPLD, libcfs_debug_file_path_arr,
			 current_time, (long_ptr_t)arg);
		printk(KERN_ALERT "LustreError: dumping log to %s\n",
		       debug_file_name);
		cfs_tracefile_dump_all_pages(debug_file_name);
		libcfs_run_debug_log_upcall(debug_file_name);
	}
	current->journal_info = journal_info;
}

static int libcfs_debug_dumplog_thread(void *arg)
{
	libcfs_debug_dumplog_internal(arg);
	wake_up(&debug_ctlwq);
	return 0;
}

void libcfs_debug_dumplog(void)
{
	wait_queue_t wait;
	struct task_struct    *dumper;
	ENTRY;

	/* we're being careful to ensure that the kernel thread is
	 * able to set our state to running as it exits before we
	 * get to schedule() */
	init_waitqueue_entry(&wait, current);
	set_current_state(TASK_INTERRUPTIBLE);
	add_wait_queue(&debug_ctlwq, &wait);

	dumper = kthread_run(libcfs_debug_dumplog_thread,
			     (void *)(long)current_pid(),
			     "libcfs_debug_dumper");
	if (IS_ERR(dumper))
		printk(KERN_ERR "LustreError: cannot start log dump thread:"
		       " %ld\n", PTR_ERR(dumper));
	else
		schedule();

	/* be sure to teardown if cfs_create_thread() failed */
	remove_wait_queue(&debug_ctlwq, &wait);
	set_current_state(TASK_RUNNING);
}
EXPORT_SYMBOL(libcfs_debug_dumplog);

int libcfs_debug_init(unsigned long bufsize)
{
	int    rc = 0;
	unsigned int max = libcfs_debug_mb;

	init_waitqueue_head(&debug_ctlwq);

	if (libcfs_console_max_delay <= 0 || /* not set by user or */
	    libcfs_console_min_delay <= 0 || /* set to invalid values */
	    libcfs_console_min_delay >= libcfs_console_max_delay) {
		libcfs_console_max_delay = CDEBUG_DEFAULT_MAX_DELAY;
		libcfs_console_min_delay = CDEBUG_DEFAULT_MIN_DELAY;
	}

	if (libcfs_debug_file_path != NULL) {
		strlcpy(libcfs_debug_file_path_arr,
			libcfs_debug_file_path,
			sizeof(libcfs_debug_file_path_arr));
	}

	/* If libcfs_debug_mb is set to an invalid value or uninitialized
	 * then just make the total buffers smp_num_cpus * TCD_MAX_PAGES */
	if (max > cfs_trace_max_debug_mb() || max < num_possible_cpus()) {
		max = TCD_MAX_PAGES;
	} else {
		max = (max / num_possible_cpus());
		max = (max << (20 - PAGE_CACHE_SHIFT));
	}

	rc = cfs_tracefile_init(max);
	if (rc)
		return rc;
	libcfs_register_panic_notifier();

	rc = libcfs_func_filter_init();
        return rc;
}

int libcfs_debug_cleanup(void)
{
        libcfs_unregister_panic_notifier();
        cfs_tracefile_exit();
	libcfs_func_filter_cleanup();
        return 0;
}

int libcfs_debug_clear_buffer(void)
{
        cfs_trace_flush_pages();
        return 0;
}

/* Debug markers, although printed by S_LNET
 * should not be be marked as such. */
#undef DEBUG_SUBSYSTEM
#define DEBUG_SUBSYSTEM S_UNDEFINED
int libcfs_debug_mark_buffer(const char *text)
{
        CDEBUG(D_TRACE,"***************************************************\n");
        LCONSOLE(D_WARNING, "DEBUG MARKER: %s\n", text);
        CDEBUG(D_TRACE,"***************************************************\n");

        return 0;
}
#undef DEBUG_SUBSYSTEM
#define DEBUG_SUBSYSTEM S_LNET

void libcfs_debug_set_level(unsigned int debug_level)
{
	printk(KERN_WARNING "Lustre: Setting portals debug level to %08x\n",
	       debug_level);
	libcfs_debug = debug_level;
}

long libcfs_log_return(struct libcfs_debug_msg_data *msgdata, long rc)
{
        libcfs_debug_msg(msgdata, "Process leaving (rc=%lu : %ld : %lx)\n",
                         rc, rc, rc);
        return rc;
}
EXPORT_SYMBOL(libcfs_log_return);

void libcfs_log_goto(struct libcfs_debug_msg_data *msgdata, const char *label,
                     long_ptr_t rc)
{
        libcfs_debug_msg(msgdata, "Process leaving via %s (rc=" LPLU " : " LPLD
                         " : " LPLX ")\n", label, (ulong_ptr_t)rc, rc, rc);
}
EXPORT_SYMBOL(libcfs_log_goto);
