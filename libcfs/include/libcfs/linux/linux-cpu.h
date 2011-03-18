/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
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
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * libcfs/include/libcfs/linux/linux-mem.h
 *
 * Basic library routines.
 *
 * Author: liang@whamcloud.com
 */

#ifndef __LIBCFS_LINUX_CPU_H__
#define __LIBCFS_LINUX_CPU_H__

#ifndef __LIBCFS_LIBCFS_H__
#error Do not #include this file directly. #include <libcfs/libcfs.h> instead
#endif

#ifndef __KERNEL__
#error This include is only for kernel use.
#endif

#include <linux/cpu.h>
#include <linux/cpuset.h>
#include <linux/version.h>

#ifndef HAVE_CPU_ONLINE
#define cpu_online(cpu)         ((1 << cpu) & (cpu_online_map))
#endif

#ifdef CONFIG_SMP

#define HAVE_LIBCFS_SMP

typedef cpumask_t       cfs_hw_cpumask_t;

#define cfs_hw_cpu_id()                         smp_processor_id()
#define cfs_hw_cpus_set_allowed(t, mask)        set_cpus_allowed(t, mask)

#define cfs_hw_cpus_possible()                  num_possible_cpus()
#define cfs_hw_cpus_online()                    num_online_cpus()

#define cfs_hw_cpu_online(i)                    cpu_online(i)
#define cfs_hw_cpu_set(i, mask)                 cpu_set(i, mask)
#define cfs_hw_cpu_clear(i, mask)               cpu_clear(i, mask)
#define cfs_hw_cpu_isset(i, mask)               cpu_isset(i, mask)
#define cfs_hw_cpus_empty(mask)                 cpus_empty(mask)
#define cfs_hw_cpus_weight(mask)                cpus_weight(mask)

#define cfs_hw_cpu_any_online(mask)            (any_online_cpu(mask) != NR_CPUS)

#define cfs_hw_cpus_for_each(i, mask)           for_each_cpu_mask(i, mask)

#if !(defined(CONFIG_X86) && (LINUX_VERSION_CODE >= KERNEL_VERSION(2,4,21))) || defined(CONFIG_X86_64) || ((LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,0)) && !defined(CONFIG_X86_HT))

/* hyper-threading on each core */
#define cfs_hw_cpu_hts()        smp_num_siblings

#else

/* hyper-threading on each core */
#define cfs_hw_cpu_hts()        1

#endif

static inline void
cfs_get_online_hw_cpus(cfs_hw_cpumask_t *mask)
{
        memcpy(mask, &cpu_online_map, sizeof(cfs_hw_cpumask_t));
}

#ifdef CONFIG_NUMA

#define HAVE_LIBCFS_NUMA

#define SIMULATE_NUMA   0

#if SIMULATE_NUMA /* SIMULATE NUMA by SMP, debug only */

typedef cpumask_t      cfs_hw_nodemask_t;

#define cfs_hw_nodes_possible()                 num_possible_cpus()
#define cfs_hw_nodes_online()                   num_online_cpus()

#define cfs_hw_node_online(i)                   cpu_online(i)
#define cfs_hw_node_set(i, mask)                cpu_set(i, mask)
#define cfs_hw_node_clear(i, mask)              cpu_clear(i, mask)
#define cfs_hw_node_isset(i, mask)              cpu_isset(i, mask)
#define cfs_hw_nodes_empty(mask)                cpus_empty(mask)
#define cfs_hw_nodes_weight(mask)               cpus_weight(mask)

extern cpumask_t                                __cfs_node_to_cpumask[];
#define cfs_hw_node_to_cpumask(i)               __cfs_node_to_cpumask[i]

#define cfs_hw_nodes_for_each(i, mask)          for_each_cpu_mask(i, mask)

#else /* HW supports NUMA */

typedef nodemask_t      cfs_hw_nodemask_t;

#define cfs_hw_nodes_possible()                 num_possible_nodes()
#define cfs_hw_nodes_online()                   num_online_nodes()

#define cfs_hw_node_online(i)                   node_online(i)
#define cfs_hw_node_set(i, mask)                node_set(i, mask)
#define cfs_hw_node_clear(i, mask)              node_clear(i, mask)
#define cfs_hw_node_isset(i, mask)              node_isset(i, mask)
#define cfs_hw_node_to_cpumask(i)               node_to_cpumask(i)
#define cfs_hw_nodes_empty(mask)                nodes_empty(mask)
#define cfs_hw_nodes_weight(mask)               nodes_weight(mask)

#define cfs_hw_nodes_for_each(i, mask)          for_each_node_mask(i, mask)

#endif /* simulate NUMA */

static inline void
cfs_get_online_hw_nodes(cfs_hw_nodemask_t *mask)
{
        memcpy(mask, &node_online_map, sizeof(cfs_hw_nodemask_t));
}

#else /* No NUMA */
#define SIMULATE_NUMA 0
#endif /* with NUMA */

extern int  cfs_cpu_hotplug_setup(void);
extern void cfs_cpu_hotplug_cleanup(void);
/*
 * estimate reasonable number of cpunodes based on # cores
 * and RAM size on systems
 */
extern unsigned cfs_node_num_estimate(int mode);

#endif /* SMP */

#define __cfs_cacheline_aligned ____cacheline_aligned
#define __cfs_read_mostly       __read_mostly

#ifdef L1_CACHE_BYTES

#define CFS_CACHELINE_SIZE       L1_CACHE_BYTES

#endif

#endif /* __LIBCFS_LINUX_CPU_H__ */
