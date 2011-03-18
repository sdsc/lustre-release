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
 * libcfs/include/libcfs/libcfs_cpu.h
 *
 * cfs_cpunode
 *   . cfs_cpunode is virtual processing unit
 *
 *   . cfs_cpunode can present 1-N cores, or 1-N CPU sockets, or 1-N NUMA nodes,
 *     in other words, cfs_cpunode is a processors pool.
 *
 * cfs_cpumap
 *   . cfs_cpumap is a set of cfs_cpunodes
 *
 *   . while creating a cfs_cpumap, user can provide a bitmask of HW cores,
 *     only cores covered by bitmask will be presented by the cfs_cpumap.
 *
 *   . if pass-in NULL as bitmask, all online CPUs will be covered by cfs_cpumap
 *
 *   . user can speicify total number of cfs_cpunodes while creating a
 *     cfs_cpumap, ID of cfs_cpunode is always start from 0.
 *
 *     i.e: if there are 8 cores on the system, while creating a cfs_cpumap with
 *          node_num=8:
 *              core[0] = node[0], core[1] = node[1]
 *              ......
 *              core[6] = node[6], core[7] = node[7]
 *
 *          node_num=4:
 *              core[0, 1] = node[0], core[2, 3] = node[1]
 *              core[4, 5] = node[2], core[6, 7] = node[3]
 *
 *          node_num=1:
 *              core[0, 1, ... 7] = node[0]
 *
 *   . libcfs NUMA allocator, percpu data allocator, CPU affinity thread are
 *     built over cfs_cpumap, instead of HW CPUs.
 *
 *   . by default, there is a global cfs_cpumap to present all CPUs on system,
 *     user can specify node_num of the global cfs_cpumap while loading libcfs.
 *
 *   . most cases, Lustre modules should refer to the global cfs_cpumap,
 *     instead of accessing HW CPUs directly, so concurrency of Lustre can be
 *     configured by node_num of the global cfs_cpumap
 *
 *   . if node_num=1(all CPUs in one pool), lustre should work the same way
 *     as 2.0 or earlier verison
 *
 * Author: liang@whamcloud.com
 */

#ifndef __LIBCFS_CPU_H__
#define __LIBCFS_CPU_H__

#ifdef HAVE_LIBCFS_SMP
/**
 * default mode:
 * - if NUMA is enabled, each cpu_node is presented by one numa node
 * - if NUMA is disabled, only one cpu_node and it includes all cores
 */
#define CFS_CPU_MODE_AUTO       0

/**
 * can't support (or disable) NUMA, user can specify number of cpu nodes
 * by "cpu_node_num", which can be any number between 1 and HW cores
 */
#define CFS_CPU_MODE_UMA        1
/**
 * Fat processing cores machine (MDS), user can specify number of cpu nodes
 * by "cpu_node_num"
 */
#define CFS_CPU_MODE_FAT        2

/* virtual processing unit */
typedef struct {
        /** CPUs mask for this node */
        cfs_hw_cpumask_t        cpn_mask;
} cfs_cpunode_t;

/* a set of cfs_cpunodes */
typedef struct cfs_cpumap {
        /** version, reserved for hotplug */
        __u64                   cpm_version;
        /** physical CPUs mask */
        cfs_hw_cpumask_t        cpm_cpumask;
# ifdef HAVE_LIBCFS_NUMA
        /** NUMA nodes mask */
        cfs_hw_nodemask_t       cpm_nodemask;
# endif
        /** nodes array */
        cfs_cpunode_t          *cpm_nodes;
        /** map CPU ID to node ID */
        int                    *cpm_hwcpu_to_node;
        /** # of CPU nodes */
        unsigned int            cpm_node_num;
} cfs_cpumap_t;

/*
 * return total number of cpunodes in a cpumap
 */
static inline int
__cfs_cpu_node_num(cfs_cpumap_t *cpumap)
{
        return cpumap->cpm_node_num;
}

/*
 * return weight(# HW cores) of cpunode(i) in cfs_cpumap
 */
static inline int
__cfs_cpu_node_weight(cfs_cpumap_t *cpumap, int i)
{
        return cfs_hw_cpus_weight(cpumap->cpm_nodes[i].cpn_mask);
}

#define __cfs_cpu_hwmask(cpumap, i)     ((cpumap)->cpm_nodes[i].cpn_mask)
#define cfs_cpu_hwmask(i)               __cfs_cpu_hwmask(cfs_cpumap, i)

#else /* !HAVE_LIBCFS_SMP */

typedef struct cfs_cpumap {
        __u64                   cpm_version;        /* version */
        cfs_hw_cpumask_t        cpm_mask;           /* cpu mask */
} cfs_cpumap_t;

static inline int
__cfs_cpu_node_num(cfs_cpumap_t *cpumap)
{
        return 1;
}

static inline int
__cfs_cpu_node_weight(cfs_cpumap_t *cpumap, int i)
{
        return 1;
}

#define cfs_hw_cpu_hts()        1

#endif /* !HAVE_LIBCFS_SMP */

#ifndef CFS_CACHELINE_SIZE
# define CFS_CACHELINE_SIZE     64
#endif

#define CFS_CACHELINE_MASK   (~(CFS_CACHELINE_SIZE - 1ULL))

static inline __u64
cfs_cacheline_align(__u64 addr)
{
        return (addr + CFS_CACHELINE_SIZE - 1ULL) & CFS_CACHELINE_MASK;
}

#ifndef __cfs_cacheline_aligned
# define __cfs_cacheline_aligned
#endif

#ifndef __cfs_read_mostly
# define __cfs_read_mostly
#endif

#define CFS_CPU_ANY             (-1)

extern cfs_cpumap_t *cfs_cpumap;
/*
 * create a cfs_cpumap from bitmask of HW cores
 */
cfs_cpumap_t *cfs_cpumap_create_uma(cfs_hw_cpumask_t *mask, int node_num);

#ifdef HAVE_LIBCFS_NUMA
/*
 * create a cfs_cpumap from bitmask of NUMA nodes
 */
cfs_cpumap_t *cfs_cpumap_create_numa(cfs_hw_nodemask_t *mask, int node_num);
#endif
/*
 * create a cfs_cpumap based on default configuration of libcfs
 */
cfs_cpumap_t *cfs_cpumap_create(void);
/*
 * destroy a cfs_cpumap
 */
void cfs_cpumap_destroy(cfs_cpumap_t *cpumap);
/*
 * shadow current HW processor ID to cpunode ID by cpumap and return
 */
int  __cfs_cpu_current(cfs_cpumap_t *cpumap);
/*
 * shadow specified HW processor ID to cpunode ID by cpumap and return 
 */
int  __cfs_cpu_from_hwcpu(cfs_cpumap_t *cpumap, int hwcpu);
/*
 * bind current thread on a cpunode of cpumap
 */
int  __cfs_cpu_bind(cfs_cpumap_t *cpumap, int id);

#define cfs_cpu_current()       __cfs_cpu_current(cfs_cpumap)
#define cfs_cpu_bind(i)         __cfs_cpu_bind(cfs_cpumap, i)
#define cfs_cpu_from_hwcpu(i)   __cfs_cpu_from_hwcpu(cfs_cpumap, i)
#define cfs_cpu_node_num()      __cfs_cpu_node_num(cfs_cpumap)
#define cfs_cpu_node_weight(i)  __cfs_cpu_node_weight(cfs_cpumap, i)

#define cfs_cpumap_cpu_for_each(i, cpumap)     \
        for (i = 0; i < __cfs_cpu_node_num(cpumap); i++)

#define cfs_cpus_for_each(i)    cfs_cpumap_cpu_for_each(i, cfs_cpumap)

int  cfs_cpu_init(void);
void cfs_cpu_fini(void);

#endif
