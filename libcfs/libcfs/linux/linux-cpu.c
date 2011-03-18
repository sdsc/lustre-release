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
 * Author: liang@whamcloud.com
 */

#define DEBUG_SUBSYSTEM S_LNET
#include <linux/cpu.h>

#include <libcfs/libcfs.h>

#ifdef HAVE_LIBCFS_SMP

extern spinlock_t       cfs_cpu_lock;
extern __u64            cfs_cpu_version;

#if SIMULATE_NUMA
cpumask_t __cfs_node_to_cpumask[NR_CPUS];
#endif

/*
 * generate reasonable number of cpunodes based on # cores and RAM size
 */
unsigned
cfs_node_num_estimate(int mode)
{
        unsigned        ncpus;
        unsigned        memgb;
        unsigned        nodes;

#ifdef HAVE_LIBCFS_NUMA
        nodes = cfs_hw_nodes_online();
#else
        nodes = 1;
#endif
        if (mode == CFS_CPU_MODE_AUTO)
                return nodes;

        LASSERT(mode == CFS_CPU_MODE_UMA || mode == CFS_CPU_MODE_FAT);

        /* round to 1GB if > 512M */
        memgb = (cfs_physpages_num() + (1U << (29 - CFS_PAGE_SHIFT))) >>
                (30 - CFS_PAGE_SHIFT);
        memgb = max(1U, memgb);

        if (memgb <= nodes)
                return nodes;

        ncpus = cfs_hw_cpus_online();

        LASSERT((cfs_hw_cpu_hts() >= 1) && (ncpus % cfs_hw_cpu_hts() == 0));
        ncpus /= cfs_hw_cpu_hts();

#if (BITS_PER_LONG == 32)
        /* config many CPU nodes on 32-bits system could consume
         * too much memory */
        return min(2, ncpus);
#else
        if (ncpus <= memgb)
                return ncpus;

        if (ncpus % 2 != 0)
                return ncpus;

        return max(1U, ncpus / 2);
#endif
}

#ifdef CONFIG_HOTPLUG_CPU
static int
cfs_cpu_notify(struct notifier_block *self, unsigned long action, void *hcpu)
{
        unsigned int  cpu = (unsigned long)hcpu;

        switch (action) {
        case CPU_DEAD:
        case CPU_DEAD_FROZEN:
        case CPU_ONLINE:
        case CPU_ONLINE_FROZEN:
                spin_lock(&cfs_cpu_lock);
                cfs_cpu_version++;
                spin_unlock(&cfs_cpu_lock);
        default:
                CWARN("Lustre: can't support CPU hotplug well now, "
                      "performance and stability could be impacted"
                      "[CPU %u notify: %lx]\n", cpu, action);
        }

        return NOTIFY_OK;
}

static struct notifier_block cfs_cpu_notifier =
{
        .notifier_call  = cfs_cpu_notify,
        .priority       = 0
};

#endif

void
cfs_cpu_hotplug_cleanup(void)
{
#if SIMULATE_NUMA
        int     i;

        memset(__cfs_node_to_cpumask, 0, sizeof(cpumask_t) * NR_CPUS);
        for (i = 0; i < NR_CPUS; i++)
                cpu_set(i, __cfs_node_to_cpumask[i]);
#endif
#ifdef CONFIG_HOTPLUG_CPU
        unregister_hotcpu_notifier(&cfs_cpu_notifier);
#endif
}

int
cfs_cpu_hotplug_setup(void)
{
#ifdef CONFIG_HOTPLUG_CPU
        register_hotcpu_notifier(&cfs_cpu_notifier);
#endif
        return 0;
}

#endif
