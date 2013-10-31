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

#define _NODEMAP_INTERNAL_H

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

extern cfs_proc_dir_entry_t *proc_lustre_nodemap_root;
extern cfs_list_t nodemap_list;
extern unsigned int nodemap_highest_id;
extern unsigned int nodemap_idmap_active;
extern struct nodemap *default_nodemap;

int nodemap_procfs_init(void);
struct nodemap *nodemap_init_nodemap(char *nodemap_name, int def_nodemap);
void lprocfs_nodemap_register(char *nodemap_name, int def_ndoemap,
			      struct nodemap *nodemap);
void lprocfs_nodemap_init_vars(struct lprocfs_static_vars *lvars);

int nodemap_rd_active(char *page, char **start, off_t off, int count,
		      int *eof, void *data);
int nodemap_wr_active(struct file *file, const char *buffer,
		      unsigned long count, void *data);
int nodemap_rd_id(char *page, char **start, off_t off, int count,
		  int *eof, void *data);
int nodemap_rd_squash_uid(char *page, char **start, off_t off, int count,
			  int *eof, void *data);
int nodemap_wr_squash_uid(struct file *file, const char *buffer,
			  unsigned long count, void *data);
int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			  int *eof, void *data);
int nodemap_wr_squash_gid(struct file *file, const char *buffer,
			  unsigned long count, void *data);
int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		       int *eof, void *data);
int nodemap_wr_trusted(struct file *file, const char *start,
		       unsigned long count, void *data);
int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		     int *eof, void *data);
int nodemap_wr_admin(struct file *file, const char *buffer,
		     unsigned long count, void *data);
int rd_nid_test(char *page, char **start, off_t off, int count,
		int *eof, void *data);
int wr_nid_test(struct file *file, const char *buffer,
		unsigned long count, void *data);
int rd_uidmap_test(char *page, char **start, off_t off, int count,
		int *eof, void *data);
int wr_uidmap_test(struct file *file, const char *buffer,
		unsigned long count, void *data);
int rd_gidmap_test(char *page, char **start, off_t off, int count,
		int *eof, void *data);
int wr_gidmap_test(struct file *file, const char *buffer,
		unsigned long count, void *data);
int nodemap_cleanup_nodemaps(void);
int range_insert(struct range_node *data);
int range_delete(struct range_node *data);
struct range_node *range_search(lnet_nid_t *nid);
struct idmap_node *idmap_init(char *idmap_string);
int idmap_insert(struct nodemap *nodemap, int node_type,
		 struct idmap_node *idmap);
int idmap_delete(struct nodemap *nodemap, int node_type,
		 struct idmap_node *idmap);
int idmap_delete_tree(struct nodemap *nodemap, int node_type);
struct idmap_node *idmap_search(struct nodemap *nodemap, int tree_type,
				int node_type, __u32 id);

struct rb_node *nm_rb_next_postorder(const struct rb_node *);
struct rb_node *nm_rb_first_postorder(const struct rb_root *);

#define nm_rbtree_postorder_for_each_entry_safe(pos, n,			\
						root, field)		\
	for (pos = rb_entry(nm_rb_first_postorder(root), typeof(*pos),	\
			    field),					\
		n = rb_entry(nm_rb_next_postorder(&pos->field),		\
			     typeof(*pos), field);			\
		&pos->field;						\
		pos = n,						\
		n = rb_entry(nm_rb_next_postorder(&pos->field),		\
			     typeof(*pos), field))
