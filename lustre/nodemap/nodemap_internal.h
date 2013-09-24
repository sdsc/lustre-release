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

#ifndef _NODEMAP_INTERNAL_H
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
int nodemap_init_nodemap(char *nodemap_name, int def_nodemap,
			struct nodemap *nodemap);
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
int nodemap_rd_squash_gid(char *page, char **start, off_t off, int count,
			  int *eof, void *data);
int nodemap_rd_trusted(char *page, char **start, off_t off, int count,
		       int *eof, void *data);
int nodemap_rd_admin(char *page, char **start, off_t off, int count,
		     int *eof, void *data);
#ifdef NODEMAP_PROC_DEBUG
int nodemap_wr_squash_uid(struct file *file, const char *buffer,
			  unsigned long count, void *data);
int nodemap_wr_squash_gid(struct file *file, const char *buffer,
			  unsigned long count, void *data);
int nodemap_wr_trusted(struct file *file, const char *start,
		       unsigned long count, void *data);
int nodemap_wr_admin(struct file *file, const char *buffer,
		     unsigned long count, void *data);
int nodemap_proc_add_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data);
int nodemap_proc_del_nodemap(struct file *file, const char *buffer,
			     unsigned long count, void *data);
#endif /* NODEMAP_PROC_DEBUG */
int nodemap_cleanup_nodemaps(void);
#endif /* _NODEMAP_INTERNAL_H */
