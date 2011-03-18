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
 * Please see comments in libcfs/include/libcfs/libcfs_cpu.h for introduction
 *
 * Author: liang@whamcloud.com
 */

#ifndef EXPORT_SYMTAB
# define EXPORT_SYMTAB
#endif
#define DEBUG_SUBSYSTEM S_LNET

#include <libcfs/libcfs.h>
#include <lnet/lib-lnet.h>
#include <lnet/lnet.h>

/* Global cpumap */
cfs_cpumap_t             *cfs_cpumap __cfs_read_mostly = NULL;

#ifdef HAVE_LIBCFS_SMP

/* serialize */
cfs_spinlock_t            cfs_cpu_lock;
/* reserved for hotplug */
__u64                     cfs_cpu_version = 0;

/**
 * 0: CFS_CPU_MODE_AUTO
 * 1: CFS_CPU_MODE_UMA
 * 2: CFS_CPU_MODE_FAT
 */
static int                cpu_mode = CFS_CPU_MODE_FAT;
CFS_MODULE_PARM(cpu_mode, "i", int, 0444, "libcfs cpu mode");
/*
 * # of CPU nodes:
 * 0  : estimate best value based on RAM size
 * 1  : disable multiple nodes
 * >1 : specify number of nodes
 */
static int                cpu_node_num = 0;
CFS_MODULE_PARM(cpu_node_num, "i", int, 0444, "# of CPU node");

static char *
cfs_cpu_mode_str(int mode)
{
        switch(mode) {
        default:
                return "unknown";
        case CFS_CPU_MODE_AUTO:
                return "auto";
        case CFS_CPU_MODE_UMA:
                return "uma";
        case CFS_CPU_MODE_FAT:
                return "fat";
        }
}

static void
cfs_cpumap_free(cfs_cpumap_t *cpumap)
{
        if (cpumap->cpm_hwcpu_to_node != NULL) {
                LIBCFS_FREE_ALIGNED(cpumap->cpm_hwcpu_to_node,
                                    cfs_hw_cpus_possible() *
                                    sizeof(cpumap->cpm_hwcpu_to_node[0]));
        }

        if (cpumap->cpm_nodes != NULL) {
                LIBCFS_FREE_ALIGNED(cpumap->cpm_nodes,
                                    cpumap->cpm_node_num *
                                    sizeof(cpumap->cpm_nodes[0]));
        }

        LIBCFS_FREE_ALIGNED(cpumap, sizeof(cfs_cpumap_t));
}

static cfs_cpumap_t *
cfs_cpumap_alloc(int node_num)
{
        cfs_cpumap_t *cpumap;

        /*
         * layering violation with implementation of
         * cfs_node_alloc_aligned & cfs_node_free_aligned
         */
        LIBCFS_ALLOC_ALIGNED(cpumap, sizeof(cfs_cpumap_t));
        if (cpumap == NULL)
                return NULL;

        memset(cpumap, 0, sizeof(cfs_cpumap_t));
        cpumap->cpm_node_num = node_num;

        LIBCFS_ALLOC_ALIGNED(cpumap->cpm_hwcpu_to_node,
                             cfs_hw_cpus_possible() *
                             sizeof(cpumap->cpm_hwcpu_to_node[0]));
        if (cpumap->cpm_hwcpu_to_node == NULL)
                goto error;

        LIBCFS_ALLOC_ALIGNED(cpumap->cpm_nodes,
                             node_num * sizeof(cpumap->cpm_nodes[0]));
        if (cpumap->cpm_nodes != NULL)
                return cpumap;
 error:
        cfs_cpumap_free(cpumap);
        return NULL;
}

static void
cfs_cpumap_init(cfs_cpumap_t *cpumap)
{
        LASSERT (cpumap->cpm_node_num > 0);

        cfs_spin_lock(&cfs_cpu_lock);
        /* Reserved for hotplug */
        cpumap->cpm_version = cfs_cpu_version;
        cfs_spin_unlock(&cfs_cpu_lock);

        memset(cpumap->cpm_hwcpu_to_node,
               -1, cfs_hw_cpus_possible() * sizeof(int));
        memset(cpumap->cpm_nodes, 0,
               cpumap->cpm_node_num * sizeof(cfs_cpunode_t));
}

static int
cfs_cpumap_setup_uma(cfs_cpumap_t *cpumap)
{
        int     node;
        int     size;
        int     remainder;
        int     s;
        int     i;

        LASSERT (!cfs_hw_cpus_empty(cpumap->cpm_cpumask));
        cfs_cpumap_init(cpumap);

        /* average # of cores per node */
        size = cfs_hw_cpus_weight(cpumap->cpm_cpumask) /
               cpumap->cpm_node_num;
        LASSERT (size > 0);

        /* cpu_node_num is not aliquot of HW cores, we allow this
         * although don't suggest... */
        remainder = cfs_hw_cpus_weight(cpumap->cpm_cpumask) %
                    cpumap->cpm_node_num;
        node = s = 0;

        cfs_hw_cpus_for_each(i, cpumap->cpm_cpumask) {
                cfs_hw_cpu_set(i, cpumap->cpm_nodes[node].cpn_mask);

                LASSERT (cpumap->cpm_hwcpu_to_node[i] == -1);
                cpumap->cpm_hwcpu_to_node[i] = node;

                if (++s == size + !!remainder) {
                        if (remainder > 0)
                                remainder--;
                        node++;
                        s = 0;
                }
        }
        return 0;
}

#ifdef HAVE_LIBCFS_NUMA

static int
cfs_cpumap_setup_numa(cfs_cpumap_t *cpumap)
{
        cfs_hw_cpumask_t mask;
        int              node_num;
        int              node_sz;
        int              node_id;
        int              size;
        int              i;
        int              j;

        LASSERT (!cfs_hw_nodes_empty(cpumap->cpm_nodemask));

        cfs_cpumap_init(cpumap);

        node_num = cpumap->cpm_node_num;
        node_sz = cfs_hw_cpus_online() / node_num;
        LASSERT(node_sz > 0);

        node_id = size = 0;

        cfs_hw_nodes_for_each(i, cpumap->cpm_nodemask) {
                mask = cfs_hw_node_to_cpumask(i);

                if (cfs_hw_cpus_weight(mask) < node_sz ||
                    cfs_hw_cpus_weight(mask) % node_sz != 0) {
                        CERROR("Can't setup node %d, weight %d, size %d\n",
                               i, cfs_hw_cpus_weight(mask), node_sz);
                        return -EINVAL;
                }

                cfs_hw_cpus_for_each(j, mask) {
                        cfs_hw_cpu_set(j, cpumap->cpm_cpumask);
                        cfs_hw_cpu_set(j, cpumap->cpm_nodes[node_id].cpn_mask);
                        if (cpumap->cpm_hwcpu_to_node[j] != -1) {
                                /* it's not likely, but possible on
                                 * some HWs, we can do nothing */
                                CERROR("CPU %d is already in NUMA node %d\n",
                                       j, cpumap->cpm_hwcpu_to_node[j]);
                                return -EINVAL;
                        }

                        cpumap->cpm_hwcpu_to_node[j] = node_id;
                        if (++size == node_sz) {
                                node_id++;
                                size = 0;
                        }
                }

                if (size != 0) {
                        CERROR("Hotplug during setup of cpu node %d?\n", i);
                        return -EAGAIN;
                }
        }

        if (node_id == node_num)
                return 0;

        CERROR("Expect %d but actually get %d nodes\n", node_num, node_id);
        return -EINVAL;
}

#endif

static int
cfs_cpumap_setup(cfs_cpumap_t *cpumap)
{
        int     rc = 0;
        int     i;

#ifdef HAVE_LIBCFS_NUMA
        if (!cfs_hw_nodes_empty(cpumap->cpm_nodemask)) {
                rc = cfs_cpumap_setup_numa(cpumap);
        } else
#endif
        if (!cfs_hw_cpus_empty(cpumap->cpm_cpumask)) {
                rc = cfs_cpumap_setup_uma(cpumap);
        }
        else {
                CERROR("CPU and node mask are empty\n");
                return -EINVAL;
        }

        if (rc != 0) {
                CERROR("Failed to setup cpumap\n");
                return rc;
        }

        for (i = 0; i < cpumap->cpm_node_num; i++) {
                /* make sure all nodes are online */
                if (!cfs_hw_cpu_any_online(cpumap->cpm_nodes[i].cpn_mask)) {
                        CERROR("Node %d is not online\n", i);
                        return -EINVAL;
                }
        }

        return 0;
}

cfs_cpumap_t *
cfs_cpumap_create_uma(cfs_hw_cpumask_t *mask, int node_num)
{
        cfs_cpumap_t     *cpumap;
        cfs_hw_cpumask_t  cpumask;
        int               weight;
        int               i;

        if (mask == NULL) { /* all online CPUs */
                cfs_get_online_hw_cpus(&cpumask);
                mask = &cpumask;
        }

        /* bitmask is valid? */
        cfs_hw_cpus_for_each(i, *mask) {
                if (!cfs_hw_cpu_online(i)) {
                        CERROR("CPU %d is not online\n", i);
                        return NULL;
                }
        }

        weight = cfs_hw_cpus_weight(*mask);
        if (weight == 0) {
                CERROR("Empty cpu mask\n");
                return NULL;
        }

        if (node_num == 0)
                node_num = weight;

        /* specified node_num is valid? */
        if (node_num < 0 || node_num > weight) {
                CERROR("node number %d is invalid for cpumask(%d)\n",
                       node_num, weight);
                return NULL;
        }

        cpumap = cfs_cpumap_alloc(node_num);
        if (cpumap == NULL) {
                CERROR("Failed to allocate CPU map(%d)\n", node_num);
                return NULL;
        }

        memcpy(&cpumap->cpm_cpumask, mask, sizeof(*mask));
        if (cfs_cpumap_setup(cpumap) == 0)
                return cpumap;

        CERROR("Failed to setup CPU map from CPU mask\n");
        cfs_cpumap_free(cpumap);
        return NULL;
}

#ifdef HAVE_LIBCFS_NUMA
cfs_cpumap_t *
cfs_cpumap_create_numa(cfs_hw_nodemask_t *mask, int node_num)
{
        cfs_cpumap_t      *cpumap;
        cfs_hw_nodemask_t  nodemask;
        int                weight;
        int                i;

        if (mask == NULL) { /* all online nodes */
                cfs_get_online_hw_nodes(&nodemask);
                mask = &nodemask;
        }

        /* bitmask is valid? */
        cfs_hw_nodes_for_each(i, *mask) {
                if (!cfs_hw_node_online(i)) {
                        CERROR("node %d is not online\n", i);
                        return NULL;
                }
        }

        weight = cfs_hw_nodes_weight(*mask);
        if (weight == 0) {
                CERROR("Empty node mask\n");
                return NULL;
        }

        if (node_num == 0)
                node_num = weight;

        /* specified node_num is valid? */
        if (node_num < weight || node_num > cfs_hw_cpus_online()) {
                CERROR("Invalide node number %d, HW nodes %d, HW cores: %d\n",
                       node_num, weight, cfs_hw_cpus_online());
                return NULL;
        }

        cpumap = cfs_cpumap_alloc(node_num);
        if (cpumap == NULL) {
                CERROR("Failed to allocate CPU map (%d)\n", weight);
                return NULL;
        }

        memcpy(&cpumap->cpm_nodemask, mask, sizeof(*mask));
        if (cfs_cpumap_setup(cpumap) == 0)
                return cpumap;

        CERROR("Failed to setup CPU map from node mask\n");
        cfs_cpumap_free(cpumap);
        return NULL;
}

#endif

cfs_cpumap_t *
cfs_cpumap_create(void)
{
        /* create cpumap with default setting */
#ifdef HAVE_LIBCFS_NUMA
        return cpu_mode == CFS_CPU_MODE_UMA ?
               cfs_cpumap_create_uma(NULL, cpu_node_num) :
               cfs_cpumap_create_numa(NULL, cpu_node_num);
#else
        return cfs_cpumap_create_uma(NULL, cpu_node_num);
#endif
}

void
cfs_cpumap_destroy(cfs_cpumap_t *cpumap)
{
        cfs_cpumap_free(cpumap);
}

int
__cfs_cpu_current(cfs_cpumap_t *cpumap)
{
        int     cpu = cfs_hw_cpu_id();
        int     id  = cpumap->cpm_hwcpu_to_node[cpu];

        if (id < 0) {
                CDEBUG(D_TRACE, "Unexpected CPU id: %d\n", cpu);
                /* shadow the unknown cpu to a valid node ID */
                id = cpu % cpumap->cpm_node_num;

        }
        return id;
}

int
__cfs_cpu_from_hwcpu(cfs_cpumap_t *cpumap, int hwcpu)
{
        return cpumap->cpm_hwcpu_to_node[hwcpu];
}

int
__cfs_cpu_bind(cfs_cpumap_t *cpumap, int id)
{
        int     rc;

        if (id < 0 || id >= cpumap->cpm_node_num) {
                CERROR("Invalide CPU node id: %d\n", id);
                return -EINVAL;
        }

        if (cpumap == cfs_cpumap && cpumap->cpm_node_num == 1)
                return 0;

        rc = cfs_hw_cpus_set_allowed(cfs_current(),
                                     cpumap->cpm_nodes[id].cpn_mask);
        if (rc == 0)
                cfs_schedule();     /* switch to allowed CPU */
        return rc;
}

void
cfs_cpu_fini(void)
{
        cfs_cpumap_destroy(cfs_cpumap);
        cfs_cpu_hotplug_cleanup();
}

int
cfs_cpu_init(void)
{
        int     node_num;
        int     uma;

        LASSERT(cfs_cpumap == NULL);

        if (cpu_mode != CFS_CPU_MODE_AUTO &&
            cpu_mode != CFS_CPU_MODE_UMA &&
            cpu_mode != CFS_CPU_MODE_FAT)
                cpu_mode = CFS_CPU_MODE_AUTO;

#ifdef HAVE_LIBCFS_NUMA
        uma = cpu_mode == CFS_CPU_MODE_UMA;
#else
        uma = 1;
        if (cpu_mode == CFS_CPU_MODE_NUMA)
                cpu_mode = CFS_CPU_MODE_AUTO;
#endif

        spin_lock_init(&cfs_cpu_lock);
        node_num = cfs_node_num_estimate(cpu_mode);

        if (cpu_mode == CFS_CPU_MODE_AUTO || cpu_node_num == 0)
                cpu_node_num = node_num;

        if (cpu_node_num > node_num) {
                CWARN("Node num %d is larger than suggested %d\n",
                      cpu_node_num, node_num);
        }

        if (uma) { /* uniform... */
                cfs_hw_cpumask_t   cpumask;

                cfs_get_online_hw_cpus(&cpumask);
                cfs_cpumap = cfs_cpumap_create_uma(&cpumask, cpu_node_num);

        } else { /* NUMA is enabled */
#ifdef HAVE_LIBCFS_NUMA
                cfs_hw_nodemask_t   nodemask;

                cfs_get_online_hw_nodes(&nodemask);
                cfs_cpumap = cfs_cpumap_create_numa(&nodemask, cpu_node_num);
#endif
        }

        if (cfs_cpumap == NULL)
                return -1;

        LCONSOLE(0, "Phys CPU cores: %d, phys pages: "
                 "%lu, CPU mode: %s, nodes: %d\n",
                 cfs_hw_cpus_online(), cfs_physpages_num(),
                 cfs_cpu_mode_str(cpu_mode), cpu_node_num);
        cfs_cpu_hotplug_setup();
        return 0;
}

#else   /* !HAVE_LIBCFS_SMP */

#define CFS_CPU_VERSION_MAGIC           0xbabecafe

cfs_cpumap_t *
cfs_cpumap_create_uma(cfs_hw_cpumask_t *mask, int size)
{
        cfs_cpumap_t *cpumap;

        if (size != 0) {
                CERROR("Can't support cpu node size: %d\n", size);
                return NULL;
        }

        LIBCFS_ALLOC(cpumap, sizeof(cfs_cpumap_t));
        if (cpumap != NULL)
                cpumap->cpm_version = CFS_CPU_VERSION_MAGIC;

        return cpumap;
}

cfs_cpumap_t *
cfs_cpumap_create(void)
{
        return cfs_cpumap_create_uma(NULL, 0);
}

void
cfs_cpumap_destroy(cfs_cpumap_t *cpumap)
{
        LASSERT (cpumap->cpm_version == CFS_CPU_VERSION_MAGIC);
        LIBCFS_FREE(cpumap, sizeof(cfs_cpumap_t));
}

int
__cfs_cpu_current(cfs_cpumap_t *cpumap)
{
        return cfs_hw_cpu_id();
}

int
__cfs_cpu_from_hwcpu(cfs_cpumap_t *cpumap, int hwcpu)
{
        return hwcpu;
}

int
__cfs_cpu_bind(cfs_cpumap_t *cpumap, int id)
{
        return 0;
}

void
cfs_cpu_fini(void)
{
        if (cfs_cpumap != NULL)
                cfs_cpumap_destroy(cfs_cpumap);
}

int
cfs_cpu_init(void)
{
        cfs_cpumap = cfs_cpumap_create();

        return cfs_cpumap != NULL ? 0 : -1;
}

#endif /* HAVE_LIBCFS_SMP */

CFS_EXPORT_SYMBOL(cfs_cpumap);
CFS_EXPORT_SYMBOL(cfs_cpumap_create);
CFS_EXPORT_SYMBOL(cfs_cpumap_create_uma);
#ifdef HAVE_LIBCFS_NUMA
CFS_EXPORT_SYMBOL(cfs_cpumap_create_numa);
#endif
CFS_EXPORT_SYMBOL(cfs_cpumap_destroy);
CFS_EXPORT_SYMBOL(__cfs_cpu_current);
CFS_EXPORT_SYMBOL(__cfs_cpu_bind);
CFS_EXPORT_SYMBOL(__cfs_cpu_from_hwcpu);
