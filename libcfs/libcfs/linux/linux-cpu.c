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
 * version 2 along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.
 *
 * Copyright (c) 2012, 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * Author: liang@whamcloud.com
 */

#define DEBUG_SUBSYSTEM S_LNET

#include <linux/cpu.h>
#include <linux/sched.h>
#include <libcfs/libcfs.h>

#ifdef CONFIG_SMP

/**
 * modparam for setting number of partitions
 *
 *  0 : estimate best value based on cores or NUMA nodes
 *  1 : disable multiple partitions
 * >1 : specify number of partitions
 */
static int cpu_npartitions;
module_param(cpu_npartitions, int, 0444);
MODULE_PARM_DESC(cpu_npartitions, "# of CPU partitions");

/**
 * modparam for setting CPU partitions patterns:
 *
 * i.e: "0[0,1,2,3] 1[4,5,6,7]", number before bracket is CPU partition ID,
 *      number in bracket is processor ID (core or HT)
 *
 * i.e: "N 0[0,1] 1[2,3]" the first character 'N' means numbers in bracket
 *       are NUMA node ID, number before bracket is CPU partition ID.
 *
 * i.e: "N", shortcut expression to create CPT from NUMA & CPU topology
 *
 * NB: If user specified cpu_pattern, cpu_npartitions will be ignored
 */
static char *cpu_pattern = "N";
module_param(cpu_pattern, charp, 0444);
MODULE_PARM_DESC(cpu_pattern, "CPU partitions pattern");

/* return number of HTs in the same core of \a cpu */
int cfs_cpu_ht_nsiblings(int cpu)
{
	return cpumask_weight(topology_sibling_cpumask(cpu));
}
EXPORT_SYMBOL(cfs_cpu_ht_nsiblings);

void cfs_cpt_table_free(struct cfs_cpt_table *cptab)
{
	int i;

	if (cptab->ctb_cpu2cpt != NULL) {
		LIBCFS_FREE(cptab->ctb_cpu2cpt,
			    num_possible_cpus() *
			    sizeof(cptab->ctb_cpu2cpt[0]));
	}

	for (i = 0; cptab->ctb_parts != NULL && i < cptab->ctb_nparts; i++) {
		struct cfs_cpu_partition *part = &cptab->ctb_parts[i];

		if (part->cpt_nodemask != NULL) {
			LIBCFS_FREE(part->cpt_nodemask,
				    sizeof(*part->cpt_nodemask));
		}

		if (part->cpt_cpumask != NULL)
			LIBCFS_FREE(part->cpt_cpumask, cpumask_size());
	}

	if (cptab->ctb_parts != NULL) {
		LIBCFS_FREE(cptab->ctb_parts,
			    cptab->ctb_nparts * sizeof(cptab->ctb_parts[0]));
	}

	if (cptab->ctb_nodemask != NULL)
		LIBCFS_FREE(cptab->ctb_nodemask, sizeof(*cptab->ctb_nodemask));
	if (cptab->ctb_cpumask != NULL)
		LIBCFS_FREE(cptab->ctb_cpumask, cpumask_size());

	LIBCFS_FREE(cptab, sizeof(*cptab));
}
EXPORT_SYMBOL(cfs_cpt_table_free);

struct cfs_cpt_table *cfs_cpt_table_alloc(int ncpt)
{
	struct cfs_cpt_table *cptab;
	int i;

	LIBCFS_ALLOC(cptab, sizeof(*cptab));
	if (cptab == NULL)
		return NULL;

	cptab->ctb_nparts = ncpt;

	LIBCFS_ALLOC(cptab->ctb_cpumask, cpumask_size());
	LIBCFS_ALLOC(cptab->ctb_nodemask, sizeof(*cptab->ctb_nodemask));

	if (cptab->ctb_cpumask == NULL || cptab->ctb_nodemask == NULL)
		goto failed;

	LIBCFS_ALLOC(cptab->ctb_cpu2cpt,
		     num_possible_cpus() * sizeof(cptab->ctb_cpu2cpt[0]));
	if (cptab->ctb_cpu2cpt == NULL)
		goto failed;

	memset(cptab->ctb_cpu2cpt, -1,
	       num_possible_cpus() * sizeof(cptab->ctb_cpu2cpt[0]));

	LIBCFS_ALLOC(cptab->ctb_parts, ncpt * sizeof(cptab->ctb_parts[0]));
	if (cptab->ctb_parts == NULL)
		goto failed;

	for (i = 0; i < ncpt; i++) {
		struct cfs_cpu_partition *part = &cptab->ctb_parts[i];

		LIBCFS_ALLOC(part->cpt_cpumask, cpumask_size());
		LIBCFS_ALLOC(part->cpt_nodemask, sizeof(*part->cpt_nodemask));
		if (part->cpt_cpumask == NULL || part->cpt_nodemask == NULL)
			goto failed;
	}

	return cptab;

failed:
	cfs_cpt_table_free(cptab);
	return NULL;
}
EXPORT_SYMBOL(cfs_cpt_table_alloc);

int cfs_cpt_table_print(struct cfs_cpt_table *cptab, char *buf, int len)
{
	char *tmp = buf;
	int rc = 0;
	int cpt;

	for (cpt = 0; cpt < cptab->ctb_nparts; cpt++) {
		if (len > 0) {
			rc = snprintf(tmp, len, "%d\t: ", cpt);
			len -= rc;
		}

		if (len <= 0) {
			rc = -EFBIG;
			goto out;
		}
		tmp += rc;

		rc = cpulist_scnprintf(tmp, len,
				       cptab->ctb_parts[cpt].cpt_cpumask);
		len -= rc;
		if (len <= 0) {
			rc = -EFBIG;
			goto out;
		}
		tmp += rc;

		*tmp = '\n';
		tmp++;
		len--;
	}

out:
	if (rc < 0)
		return rc;

	return tmp - buf;
}
EXPORT_SYMBOL(cfs_cpt_table_print);

int cfs_cpt_number(struct cfs_cpt_table *cptab)
{
	return cptab->ctb_nparts;
}
EXPORT_SYMBOL(cfs_cpt_number);

int cfs_cpt_weight(struct cfs_cpt_table *cptab, int cpt)
{
	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	return cpt == CFS_CPT_ANY ?
	       cpumask_weight(cptab->ctb_cpumask) :
	       cpumask_weight(cptab->ctb_parts[cpt].cpt_cpumask);
}
EXPORT_SYMBOL(cfs_cpt_weight);

int cfs_cpt_online(struct cfs_cpt_table *cptab, int cpt)
{
	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	return cpt == CFS_CPT_ANY ?
	       cpumask_any_and(cptab->ctb_cpumask,
			       cpu_online_mask) < nr_cpu_ids :
	       cpumask_any_and(cptab->ctb_parts[cpt].cpt_cpumask,
			       cpu_online_mask) < nr_cpu_ids;
}
EXPORT_SYMBOL(cfs_cpt_online);

cpumask_t *cfs_cpt_cpumask(struct cfs_cpt_table *cptab, int cpt)
{
	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	return cpt == CFS_CPT_ANY ?
	       cptab->ctb_cpumask : cptab->ctb_parts[cpt].cpt_cpumask;
}
EXPORT_SYMBOL(cfs_cpt_cpumask);

nodemask_t *cfs_cpt_nodemask(struct cfs_cpt_table *cptab, int cpt)
{
	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	return cpt == CFS_CPT_ANY ?
	       cptab->ctb_nodemask : cptab->ctb_parts[cpt].cpt_nodemask;
}
EXPORT_SYMBOL(cfs_cpt_nodemask);

int cfs_cpt_set_cpu(struct cfs_cpt_table *cptab, int cpt, int cpu)
{
	int node;

	LASSERT(cpt >= 0 && cpt < cptab->ctb_nparts);

	if (cpu < 0 || cpu >= nr_cpu_ids || !cpu_online(cpu)) {
		CDEBUG(D_INFO, "CPU %d is invalid or it's offline\n", cpu);
		return 0;
	}

	if (cptab->ctb_cpu2cpt[cpu] != -1) {
		CDEBUG(D_INFO, "CPU %d is already in partition %d\n",
				cpu, cptab->ctb_cpu2cpt[cpu]);
		return 0;
	}

	cptab->ctb_cpu2cpt[cpu] = cpt;

	if (cpumask_test_cpu(cpu, cptab->ctb_cpumask)) {
		CDEBUG(D_INFO, "CPU %d is already in cpumask\n", cpu);
		return 0;
	}
	if (cpumask_test_cpu(cpu, cptab->ctb_parts[cpt].cpt_cpumask)) {
		CDEBUG(D_INFO, "CPU %d is already in partition %d cpumask\n",
				cpu, cptab->ctb_cpu2cpt[cpu]);
		return 0;
	}

	cpumask_set_cpu(cpu, cptab->ctb_cpumask);
	cpumask_set_cpu(cpu, cptab->ctb_parts[cpt].cpt_cpumask);

	node = cpu_to_node(cpu);
	cptab->ctb_parts[cpt].cpt_node = node;

	/* first CPU of @node in this CPT table */
	if (!node_isset(node, *cptab->ctb_nodemask))
		node_set(node, *cptab->ctb_nodemask);

	/* first CPU of @node in this partition */
	if (!node_isset(node, *cptab->ctb_parts[cpt].cpt_nodemask))
		node_set(node, *cptab->ctb_parts[cpt].cpt_nodemask);

	return 1;
}
EXPORT_SYMBOL(cfs_cpt_set_cpu);

void cfs_cpt_unset_cpu(struct cfs_cpt_table *cptab, int cpt, int cpu)
{
	int node;
	int i;

	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	if (cpu < 0 || cpu >= nr_cpu_ids) {
		CDEBUG(D_INFO, "Invalid CPU id %d\n", cpu);
		return;
	}

	if (cpt == CFS_CPT_ANY) {
		/* caller doesn't know the partition ID */
		cpt = cptab->ctb_cpu2cpt[cpu];
		if (cpt < 0) { /* not set in this CPT-table */
			CDEBUG(D_INFO, "Try to unset cpu %d which is "
				       "not in CPT-table %p\n", cpt, cptab);
			return;
		}

	} else if (cpt != cptab->ctb_cpu2cpt[cpu]) {
		CDEBUG(D_INFO, "CPU %d is not in CPU partition %d\n", cpu, cpt);
		return;
	}

	LASSERT(cpumask_test_cpu(cpu, cptab->ctb_parts[cpt].cpt_cpumask));
	LASSERT(cpumask_test_cpu(cpu, cptab->ctb_cpumask));

	cpumask_clear_cpu(cpu, cptab->ctb_parts[cpt].cpt_cpumask);
	cpumask_clear_cpu(cpu, cptab->ctb_cpumask);
	cptab->ctb_cpu2cpt[cpu] = -1;

	node = cpu_to_node(cpu);

	LASSERT(node_isset(node, *cptab->ctb_parts[cpt].cpt_nodemask));
	LASSERT(node_isset(node, *cptab->ctb_nodemask));

	for_each_cpu(i, cptab->ctb_parts[cpt].cpt_cpumask) {
		/* this CPT has other CPU belonging to this node? */
		if (cpu_to_node(i) == node)
			break;
	}

	if (i >= nr_cpu_ids)
		node_clear(node, *cptab->ctb_parts[cpt].cpt_nodemask);

	for_each_cpu(i, cptab->ctb_cpumask) {
		/* this CPT-table has other CPU belonging to this node? */
		if (cpu_to_node(i) == node)
			break;
	}

	if (i >= nr_cpu_ids)
		node_clear(node, *cptab->ctb_nodemask);
}
EXPORT_SYMBOL(cfs_cpt_unset_cpu);

int cfs_cpt_set_cpumask(struct cfs_cpt_table *cptab, int cpt,
			const cpumask_t *mask)
{
	int cpu;

	if (cpumask_weight(mask) == 0 ||
	    cpumask_any_and(mask, cpu_online_mask) >= nr_cpu_ids) {
		CDEBUG(D_INFO, "No online CPU is found in the CPU mask "
			       "for CPU partition %d\n", cpt);
		return 0;
	}

	for_each_cpu(cpu, mask) {
		if (cpu_online(cpu))
			if (!cfs_cpt_set_cpu(cptab, cpt, cpu))
				return 0;
	}

	return 1;
}
EXPORT_SYMBOL(cfs_cpt_set_cpumask);

void cfs_cpt_unset_cpumask(struct cfs_cpt_table *cptab, int cpt,
			   const cpumask_t *mask)
{
	int cpu;

	for_each_cpu(cpu, mask) {
		if (cpu_online(cpu))
			cfs_cpt_unset_cpu(cptab, cpt, cpu);
	}
}
EXPORT_SYMBOL(cfs_cpt_unset_cpumask);

int cfs_cpt_set_node(struct cfs_cpt_table *cptab, int cpt, int node)
{
	const cpumask_t *mask;
	int rc = 1;

	if (node < 0 || node >= MAX_NUMNODES) {
		CDEBUG(D_INFO, "Invalid NUMA id %d for CPU partition %d\n",
				node, cpt);
		return 0;
	}

	mask = cpumask_of_node(node);
	if (!cpumask_empty(mask))
		rc = cfs_cpt_set_cpumask(cptab, cpt, mask);
	cptab->ctb_parts[cpt].cpt_node = node;

	return rc;
}
EXPORT_SYMBOL(cfs_cpt_set_node);

void cfs_cpt_unset_node(struct cfs_cpt_table *cptab, int cpt, int node)
{
	const cpumask_t *mask;

	if (node < 0 || node >= MAX_NUMNODES) {
		CDEBUG(D_INFO, "Invalid NUMA id %d for CPU partition %d\n",
				node, cpt);
		return;
	}

	mask = cpumask_of_node(node);
	if (!cpumask_empty(mask))
		cfs_cpt_unset_cpumask(cptab, cpt, mask);
	cptab->ctb_parts[cpt].cpt_node = 0;
}
EXPORT_SYMBOL(cfs_cpt_unset_node);

int cfs_cpt_set_nodemask(struct cfs_cpt_table *cptab, int cpt,
			 const nodemask_t *mask)
{
	int node;

	for_each_node_mask(node, *mask) {
		if (!cfs_cpt_set_node(cptab, cpt, node))
			return 0;
	}

	return 1;
}
EXPORT_SYMBOL(cfs_cpt_set_nodemask);

void cfs_cpt_unset_nodemask(struct cfs_cpt_table *cptab, int cpt,
			    const nodemask_t *mask)
{
	int node;

	for_each_node_mask(node, *mask)
		cfs_cpt_unset_node(cptab, cpt, node);
}
EXPORT_SYMBOL(cfs_cpt_unset_nodemask);

int cfs_cpt_spread_node(struct cfs_cpt_table *cptab, int cpt)
{
	nodemask_t *mask;
	int weight;
	int rotor;
	int node = 0;

	/* convert CPU partition ID to HW node id */

	if (cpt < 0 || cpt >= cptab->ctb_nparts) {
		mask  = cptab->ctb_nodemask;
		rotor = cptab->ctb_spread_rotor++;
	} else {
		mask  = cptab->ctb_parts[cpt].cpt_nodemask;
		rotor = cptab->ctb_parts[cpt].cpt_spread_rotor++;
		node  = cptab->ctb_parts[cpt].cpt_node;
	}

	weight = nodes_weight(*mask);
	if (weight > 0) {
		rotor %= weight;

		for_each_node_mask(node, *mask) {
			if (rotor-- == 0)
				return node;
		}
	}

	return node;
}
EXPORT_SYMBOL(cfs_cpt_spread_node);

int cfs_cpt_current(struct cfs_cpt_table *cptab, int remap)
{
	int cpu = smp_processor_id();
	int cpt = cptab->ctb_cpu2cpt[cpu];

	if (cpt < 0) {
		if (!remap)
			return cpt;

		/* don't return negative value for safety of upper layer,
		 * instead we shadow the unknown cpu to a valid partition ID */
		cpt = cpu % cptab->ctb_nparts;
	}

	return cpt;
}
EXPORT_SYMBOL(cfs_cpt_current);

int cfs_cpt_of_cpu(struct cfs_cpt_table *cptab, int cpu)
{
	LASSERT(cpu >= 0 && cpu < nr_cpu_ids);

	return cptab->ctb_cpu2cpt[cpu];
}
EXPORT_SYMBOL(cfs_cpt_of_cpu);

int cfs_cpt_bind(struct cfs_cpt_table *cptab, int cpt)
{
	cpumask_t *cpumask;
	nodemask_t *nodemask;
	int cpu;
	int rc;

	LASSERT(cpt == CFS_CPT_ANY || (cpt >= 0 && cpt < cptab->ctb_nparts));

	if (cpt == CFS_CPT_ANY) {
		cpumask = cptab->ctb_cpumask;
		nodemask = cptab->ctb_nodemask;
	} else {
		cpumask = cptab->ctb_parts[cpt].cpt_cpumask;
		nodemask = cptab->ctb_parts[cpt].cpt_nodemask;
	}

	if (!cpumask_intersects(cpumask, cpu_online_mask)) {
		CDEBUG(D_INFO, "No online CPU found in CPU partition %d, did "
			"someone do CPU hotplug on system? You might need to "
			"reload Lustre modules to keep system working well.\n",
			cpt);
		return -ENODEV;
	}

	for_each_online_cpu(cpu) {
		if (cpumask_test_cpu(cpu, cpumask))
			continue;

		rc = set_cpus_allowed_ptr(current, cpumask);
		set_mems_allowed(*nodemask);
		if (rc == 0)
			schedule(); /* switch to allowed CPU */

		return rc;
	}

	/* don't need to set affinity because all online CPUs are covered */
	return 0;
}
EXPORT_SYMBOL(cfs_cpt_bind);

/**
 * Choose max to \a number CPUs from \a node and set them in \a cpt.
 * We always prefer to choose CPU in the same core/socket.
 */
static int cfs_cpt_choose_ncpus(struct cfs_cpt_table *cptab, int cpt,
				cpumask_t *node_mask, int number)
{
	cpumask_t *socket_mask = NULL;
	cpumask_t *core_mask = NULL;
	int rc = 0;
	int cpu;
	int i;

	LASSERT(number > 0);

	if (number >= cpumask_weight(node_mask)) {
		while (!cpumask_empty(node_mask)) {
			cpu = cpumask_first(node_mask);
			cpumask_clear_cpu(cpu, node_mask);

			if (!cpu_online(cpu))
				continue;

			rc = cfs_cpt_set_cpu(cptab, cpt, cpu);
			if (!rc)
				return -EINVAL;
		}
		return 0;
	}

	/* allocate scratch buffer */
	LIBCFS_ALLOC(socket_mask, cpumask_size());
	LIBCFS_ALLOC(core_mask, cpumask_size());
	if (socket_mask == NULL || core_mask == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	while (!cpumask_empty(node_mask)) {
		cpu = cpumask_first(node_mask);

		/* get cpumask for cores in the same socket */
		cpumask_and(socket_mask, topology_core_cpumask(cpu), node_mask);
		while (!cpumask_empty(socket_mask)) {
			/* get cpumask for hts in the same core */
			cpumask_and(core_mask,
				    topology_sibling_cpumask(cpu), node_mask);

			for_each_cpu(i, core_mask) {
				cpumask_clear_cpu(i, socket_mask);
				cpumask_clear_cpu(i, node_mask);

				if (!cpu_online(i))
					continue;

				rc = cfs_cpt_set_cpu(cptab, cpt, i);
				if (!rc) {
					rc = -EINVAL;
					goto out;
				}

				if (--number == 0)
					goto out;
			}
			cpu = cpumask_first(socket_mask);
		}
	}

out:
	if (core_mask != NULL)
		LIBCFS_FREE(core_mask, cpumask_size());
	if (socket_mask != NULL)
		LIBCFS_FREE(socket_mask, cpumask_size());
	return rc;
}

#define CPT_WEIGHT_MIN 4

static int cfs_cpt_num_estimate(void)
{
	int ncpu = num_online_cpus();
	int ncpt = 1;

	if (ncpu > CPT_WEIGHT_MIN) {
		int nthr = cfs_cpu_ht_nsiblings(smp_processor_id());

		for (ncpt = 2; ncpu > 2 * nthr * ncpt; ncpt++)
			/* nothing */;
	}

#if (BITS_PER_LONG == 32)
	/* config many CPU partitions on 32-bit system could consume
	 * too much memory */
	ncpt = min(2, ncpt);
#endif
	while (ncpu % ncpt != 0)
		ncpt--; /* worst case is 1 */

	return ncpt;
}

static struct cfs_cpt_table *cfs_cpt_table_create(int ncpt)
{
	struct cfs_cpt_table *cptab = NULL;
	cpumask_t *node_mask = NULL;
	int cpt = 0;
	int num;
	int rc;
	int i;

	rc = cfs_cpt_num_estimate();
	if (ncpt <= 0)
		ncpt = rc;

	if (ncpt > num_online_cpus() || ncpt > 4 * rc) {
		CWARN("CPU partition number %d is larger than suggested "
		      "value (%d), your system may have performance "
		      "issue or run out of memory while under pressure\n",
		      ncpt, rc);
	}

	if (num_online_cpus() % ncpt != 0) {
		CERROR("CPU number %d is not multiple of cpu_npartition %d, "
			"please try different cpu_npartitions value or "
			"set pattern string by cpu_pattern=STRING\n",
			num_online_cpus(), ncpt);
		rc = -EINVAL;
		goto failed;
	}

	cptab = cfs_cpt_table_alloc(ncpt);
	if (cptab == NULL) {
		CERROR("Failed to allocate CPU map(%d)\n", ncpt);
		rc = -ENOMEM;
		goto failed;
	}

	LIBCFS_ALLOC(node_mask, cpumask_size());
	if (node_mask == NULL) {
		CERROR("Failed to allocate scratch cpumask\n");
		rc = -ENOMEM;
		goto failed;
	}

	num = num_online_cpus() / ncpt;
	for_each_online_node(i) {
		cpumask_copy(node_mask, cpumask_of_node(i));

		while (cpt < ncpt && !cpumask_empty(node_mask)) {
			struct cfs_cpu_partition *part = &cptab->ctb_parts[cpt];
			int ncpu = cpumask_weight(part->cpt_cpumask);

			rc = cfs_cpt_choose_ncpus(cptab, cpt, node_mask,
						  num - ncpu);
			if (rc < 0)
				goto failed;

			ncpu = cpumask_weight(part->cpt_cpumask);
			LASSERT(num >= ncpu);
			if (num == ncpu)
				cpt++;
		}
	}

	if (cpt != ncpt ||
	    num != cpumask_weight(cptab->ctb_parts[ncpt - 1].cpt_cpumask)) {
		CERROR("Expect %d(%d) CPU partitions but got %d(%d), "
		       "some of CPUs are offline?\n",
		       cptab->ctb_nparts, num, cpt,
		       cpumask_weight(cptab->ctb_parts[ncpt - 1].cpt_cpumask));
		rc = -EINVAL;
		goto failed;
	}

	LIBCFS_FREE(node_mask, cpumask_size());

	return cptab;

failed:
	CERROR("Failed to setup CPU partition table with %d "
		"partitions, online HW nodes: %d, HW cpus: %d.\n",
		ncpt, num_online_nodes(), num_online_cpus());

	if (node_mask != NULL)
		LIBCFS_FREE(node_mask, cpumask_size());

	if (cptab != NULL)
		cfs_cpt_table_free(cptab);

	return ERR_PTR(rc);
}

static struct cfs_cpt_table *cfs_cpt_table_create_pattern(const char *pattern)
{
	struct cfs_cpt_table *cptab;
	char *pattern_dup;
	char *bracket;
	char *str;
	int node = 0;
	int ncpt = 0;
	int cpt  = 0;
	int high;
	int rc;
	int c;
	int i;

	pattern_dup = kstrdup(pattern, GFP_KERNEL);
	if (pattern_dup == NULL) {
		CERROR("Failed to duplicate pattern '%s'\n", pattern);
		return ERR_PTR(-ENOMEM);
	}

	str = cfs_trimwhite(pattern_dup);
	if (*str == 'n' || *str == 'N') {
		str++; /* skip 'N' char */
		node = 1; /* NUMA pattern */
		if (*str == '\0') {
			node = -1;
			for_each_online_node(i) {
				if (!cpumask_empty(cpumask_of_node(i)))
					ncpt++;
			}
		}
	}

	if (ncpt == 0) { /* scanning bracket which is mark of partition */
		for (bracket = str; bracket != NULL; bracket++, ncpt++)
			bracket = strchr(bracket, '[');
	}

	if (ncpt == 0 ||
	    (node && ncpt > num_online_nodes()) ||
	    (!node && ncpt > num_online_cpus())) {
		CERROR("Invalid pattern '%s', or too many partitions %d\n",
			pattern_dup, ncpt);
		rc = -EINVAL;
		goto err_free_str;
	}

	cptab = cfs_cpt_table_alloc(ncpt);
	if (cptab == NULL) {
		CERROR("Failed to allocate CPU partition table\n");
		rc = -ENOMEM;
		goto err_free_str;
	}

	if (node < 0) { /* shortcut to create CPT from NUMA & CPU topology */
		for_each_online_node(i) {
			if (cpumask_empty(cpumask_of_node(i)))
				continue;

			rc = cfs_cpt_set_node(cptab, cpt++, i);
			if (!rc) {
				rc = -EINVAL;
				goto err_free_table;
			}
		}
		kfree(pattern_dup);
		return cptab;
	}

	high = node ? MAX_NUMNODES - 1 : nr_cpu_ids - 1;

	for (str = cfs_trimwhite(str), c = 0; /* until break */; c++) {
		struct cfs_range_expr *range;
		struct cfs_expr_list *el;
		int n;

		bracket = strchr(str, '[');
		if (bracket == NULL) {
			if (*str != 0) {
				CERROR("Invalid pattern '%s'\n", str);
				rc = -EINVAL;
				goto err_free_table;
			} else if (c != ncpt) {
				CERROR("Expect %d partitions but found %d\n",
					ncpt, c);
				rc = -EINVAL;
				goto err_free_table;
			}
			break;
		}

		if (sscanf(str, "%d%n", &cpt, &n) < 1) {
			CERROR("Invalid CPU pattern '%s'\n", str);
			rc = -EINVAL;
			goto err_free_table;
		}

		if (cpt < 0 || cpt >= ncpt) {
			CERROR("Invalid partition id %d, total partitions %d\n",
				cpt, ncpt);
			rc = -EINVAL;
			goto err_free_table;
		}

		if (cfs_cpt_weight(cptab, cpt) != 0) {
			CERROR("Partition %d has already been set.\n", cpt);
			rc = -EPERM;
			goto err_free_table;
		}

		str = cfs_trimwhite(str + n);
		if (str != bracket) {
			CERROR("Invalid pattern '%s'\n", str);
			rc = -EINVAL;
			goto err_free_table;
		}

		bracket = strchr(str, ']');
		if (bracket == NULL) {
			CERROR("Missing right bracket for partition "
				"%d in '%s'\n", cpt, str);
			rc = -EINVAL;
			goto err_free_table;
		}

		rc = cfs_expr_list_parse(str, (bracket - str) + 1, 0, high,
					 &el);
		if (rc) {
			CERROR("Can't parse number range in '%s'\n", str);
			goto err_free_table;
		}

		list_for_each_entry(range, &el->el_exprs, re_link) {
			for (i = range->re_lo; i <= range->re_hi; i++) {
				if ((i - range->re_lo) % range->re_stride != 0)
					continue;

				rc = node ? cfs_cpt_set_node(cptab, cpt, i)
					  : cfs_cpt_set_cpu(cptab, cpt, i);
				if (!rc) {
					cfs_expr_list_free(el);
					rc = -EINVAL;
					goto err_free_table;
				}
			}
		}

		cfs_expr_list_free(el);

		if (!cfs_cpt_online(cptab, cpt)) {
			CERROR("No online CPU is found on partition %d\n", cpt);
			rc = -ENODEV;
			goto err_free_table;
		}

		str = cfs_trimwhite(bracket + 1);
	}

	kfree(pattern_dup);
	return cptab;

err_free_table:
	cfs_cpt_table_free(cptab);
err_free_str:
	kfree(pattern_dup);
	return ERR_PTR(rc);
}

#ifdef CONFIG_HOTPLUG_CPU
static int cfs_cpu_notify(struct notifier_block *self, unsigned long action,
			  void *hcpu)
{
	int cpu = (unsigned long)hcpu;
	bool warn;

	switch (action) {
	case CPU_DEAD:
	case CPU_DEAD_FROZEN:
	case CPU_ONLINE:
	case CPU_ONLINE_FROZEN:
	default:
		if (action != CPU_DEAD && action != CPU_DEAD_FROZEN) {
			CDEBUG(D_INFO, "CPU changed [cpu %u action %lx]\n",
			       cpu, action);
			break;
		}

		/* if all HTs in a core are offline, it may break affinity */
		warn = cpumask_any_and(topology_sibling_cpumask(cpu),
				       cpu_online_mask) >= nr_cpu_ids;
		CDEBUG(warn ? D_WARNING : D_INFO,
		       "Lustre: can't support CPU plug-out well now, "
		       "performance and stability could be impacted"
		       "[CPU %u action: %lx]\n", cpu, action);
	}

	return NOTIFY_OK;
}

static struct notifier_block cfs_cpu_notifier = {
	.notifier_call	= cfs_cpu_notify,
	.priority	= 0
};

#endif

void cfs_cpu_fini(void)
{
	if (!IS_ERR_OR_NULL(cfs_cpt_table))
		cfs_cpt_table_free(cfs_cpt_table);

#ifdef CONFIG_HOTPLUG_CPU
	unregister_hotcpu_notifier(&cfs_cpu_notifier);
#endif
}

int cfs_cpu_init(void)
{
	LASSERT(cfs_cpt_table == NULL);

#ifdef CONFIG_HOTPLUG_CPU
	register_hotcpu_notifier(&cfs_cpu_notifier);
#endif
	get_online_cpus();
	if (*cpu_pattern != 0) {
		cfs_cpt_table = cfs_cpt_table_create_pattern(cpu_pattern);
		if (IS_ERR(cfs_cpt_table)) {
			CERROR("Failed to create cptab from pattern '%s'\n",
				cpu_pattern);
			goto failed;
		}

	} else {
		cfs_cpt_table = cfs_cpt_table_create(cpu_npartitions);
		if (IS_ERR(cfs_cpt_table)) {
			CERROR("Failed to create cptab with npartitions %d\n",
				cpu_npartitions);
			goto failed;
		}
	}
	put_online_cpus();

	LCONSOLE(0, "HW NUMA nodes: %d, HW CPU cores: %d, npartitions: %d\n",
		 num_online_nodes(), num_online_cpus(),
		 cfs_cpt_number(cfs_cpt_table));
	return 0;

failed:
	put_online_cpus();
	cfs_cpu_fini();
	return PTR_ERR(cfs_cpt_table);
}

#endif
