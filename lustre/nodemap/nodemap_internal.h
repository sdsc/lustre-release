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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Joshua Walgenbach <jjw@iu.edu>
 */

#define _CLUSTER_INTERNAL_H

#include <linux/module.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/syscalls.h>
#include <linux/fcntl.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/string.h>
#include <linux/vmalloc.h>
#include <linux/sched.h>
#include <asm/uaccess.h>

#include <lustre_net.h>
#include <lnet/types.h>
#include <lustre_export.h>
#include <dt_object.h>
#include <lustre/lustre_idl.h>
#include <lustre_nodemap.h>
#include <lustre_capa.h>

#define MODULE_STRING "nodemap"

/* Default nobody uid and gid values */

#define NODEMAP_NOBODY_UID 99
#define NODEMAP_NOBODY_GID 99

#define BUILD_IP(IP) ((IP[0] << 24) | (IP[1] << 16) | (IP[2] << 8) | IP[3])

extern cfs_proc_dir_entry_t *proc_lustre_nodemap_root;
extern cfs_list_t nodemap_list;
extern unsigned int nodemap_highest_id;
extern unsigned int nodemap_idmap_active;
extern struct nodemap *default_nodemap;

int nodemap_procfs_init(void);
int nodemap_init_nodemap(char *nodemap_name, int def_nodemap,
			struct nodemap *nodemap);
void lprocfs_nodemap_init_vars(struct lprocfs_static_vars *lvars);

int rd_uidmap_test(char *, char **, off_t, int, int *, void *);
int wr_uidmap_test(struct file *, const char *, unsigned long, void *);
int rd_gidmap_test(char *, char **, off_t, int, int *, void *);
int wr_gidmap_test(struct file *, const char *, unsigned long, void *);
int rd_nid_test(char *, char **, off_t, int, int *, void *);
int wr_nid_test(struct file *, const char *, unsigned long, void *);
int check_nidstring(char *);

uid_t nodemap_map_uid(struct nodemap *, int, uid_t);
uid_t nodemap_simple_uidmap(lnet_nid_t, uid_t);
gid_t nodemap_map_gid(struct nodemap *, int, gid_t);
gid_t nodemap_simple_gidmap(lnet_nid_t, gid_t);

int nodemap_check_nid(lnet_nid_t *);

struct nodemap *nodemap_search_by_id(unsigned int id);
struct nodemap *nodemap_search_by_name(char *nodemap_name);

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		     int *eof, void *data);
int nodemap_wr_active(struct file *, const char *, unsigned long, void *);
int nodemap_rd_id(char *, char **, off_t, int, int *, void *);
int nodemap_rd_squash_uid(char *, char **, off_t, int, int *, void *);
int nodemap_wr_squash_uid(struct file *, const char *, unsigned long, void *);
int nodemap_rd_squash_gid(char *, char **, off_t, int, int *, void *);
int nodemap_wr_squash_gid(struct file *, const char *, unsigned long, void *);
int nodemap_rd_trusted(char *, char **, off_t, int, int *, void *);
int nodemap_wr_trusted(struct file *, const char *, unsigned long, void *);
int nodemap_rd_admin(char *, char **, off_t, int, int *, void *);
int nodemap_wr_admin(struct file *, const char *, unsigned long, void *);
int nodemap_proc_add_uidmap(struct file *file, const char __user *buffer,
			   unsigned long count, void *data);
int nodemap_proc_del_uidmap(struct file *file, const char *buffer,
			   unsigned longcount, void *data);
int nodemap_proc_add_gidmap(struct file *, const char *, unsigned long, void *);
int nodemap_proc_del_gidmap(struct file *, const char *, unsigned long, void *);
int nodemap_proc_add_nodemap(struct file *, const char *,
			unsigned long, void *);
int nodemap_proc_del_nodemap(struct file *, const char *,
			   unsigned long, void *);
int nodemap_cleanup_nodemaps(void);
struct nodemap_range *nodemap_init_range(void);

int nodemap_proc_add_range(struct file *, const char *,
		      unsigned long, void *);
int nodemap_proc_del_range(struct file *, const char *,
			 unsigned long, void *);
int range_insert(struct rb_root *, struct range_node *);
struct range_node *range_search(struct rb_root *, lnet_nid_t *);
int range_remove_tree(struct rb_root);

struct range_node *string_to_range(char *);
struct uidmap_node *string_to_uidmap(char *);
struct gidmap_node *string_to_gidmap(char *);
struct idmap_node *string_to_idmap(char *);

int uidmap_insert(struct nodemap *, struct uidmap_node *);
int uidmap_remove(struct nodemap *, struct uidmap_node *);
int uidmap_remove_trees(struct nodemap *);
struct uidmap_node *uidmap_search_by_local(struct nodemap *, uid_t);
struct uidmap_node *uidmap_search_by_remote(struct nodemap *, uid_t);

int gidmap_insert(struct nodemap *, struct gidmap_node *);
int gidmap_remove(struct nodemap *, struct gidmap_node *);
int gidmap_remove_trees(struct nodemap *);
struct gidmap_node *gidmap_search_by_local(struct nodemap *, gid_t);
struct gidmap_node *gidmap_search_by_remote(struct nodemap *, gid_t);

struct idmap_node *idmap_search_by_local(struct nodemap *, __u32, int);
struct idmap_node *idmap_search_by_remote(struct nodemap *, __u32, int);

int nid_range_compare(lnet_nid_t *, struct range_node *);

struct rb_node *nm_rb_next_postorder(const struct rb_node *);
struct rb_node *nm_rb_first_postorder(const struct rb_root *);

#define nm_rbtree_postorder_for_each_entry_safe(pos, n, root, field) \
	for (pos = rb_entry(nm_rb_first_postorder(root), typeof(*pos), field),\
		n = rb_entry(nm_rb_next_postorder(&pos->field), \
			typeof(*pos), field); \
		&pos->field; \
		pos = n, \
		n = rb_entry(nm_rb_next_postorder(&pos->field), \
			typeof(*pos), field))

