#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/export.h>
#include <linux/cpumask.h>
#include <linux/cpu.h>
#include <linux/slab.h>
#include <linux/sched.h>
#include <linux/workqueue.h>
#include <linux/moduleparam.h>
#include <linux/mmu_context.h>

#include <libcfs/libcfs.h>
#include <libcfs/libcfs_ptask.h>

struct cfs_ptask_engine {
	struct padata_instance	*pte_pinst;
	struct workqueue_struct	*pte_wq;
	/*
	 * Cpumask for callback CPUs. It should be equal to
	 * serial cpumask of corresponding padata instance,
	 * so it is updated when padata notifies us about
	 * serial cpumask change.
	 */
	struct cfs_ptengine_cpumask { cpumask_var_t mask; } *pte_cpumask;
	struct notifier_block	 pte_nblock;

	int			 pte_weight;
};

static struct cfs_ptask_engine cfs_ptengine[PTT_MAX];

static char *hpri_cpulist = "";
module_param(hpri_cpulist, charp, 0644);
MODULE_PARM_DESC(hpri_cpulist, "List of CPUs available for HIGH PRI parallel tasks");

static char *clio_cpulist = "";
module_param(clio_cpulist, charp, 0644);
MODULE_PARM_DESC(clio_cpulist, "List of CPUs available for CLIO parallel tasks");

static char *rdah_cpulist = "";
module_param(rdah_cpulist, charp, 0644);
MODULE_PARM_DESC(rdah_cpulist, "List of CPUs available for Read-Ahead parallel tasks");

static unsigned long min_pio_pages = 256; /* 1Mb */
module_param(min_pio_pages, ulong, 0644);
MODULE_PARM_DESC(min_pio_pages, "Count of pages to start parallel I/O");

static int max_pio_tasks = 8;
module_param(max_pio_tasks, int, 0644);
MODULE_PARM_DESC(max_pio_tasks, "Max count of parallel I/O tasks");

static void cfs_ptask_serial(struct padata_priv *padata)
{
	struct cfs_ptask *ptask = cfs_padata2ptask(padata);

	if (cfs_ptask_need_complete(ptask)) {
		if (cfs_ptask_is_ordered(ptask))
			complete(&ptask->pt_completion);
	} else if (cfs_ptask_is_autofree(ptask)) {
		kfree(ptask);
	}
}

static void cfs_ptask_parallel(struct padata_priv *padata)
{
	struct cfs_ptask *ptask = cfs_padata2ptask(padata);
	mm_segment_t old_fs = get_fs();

	if (!cfs_ptask_is_atomic(ptask))
		local_bh_enable();

	if (cfs_ptask_use_user_mm(ptask) && ptask->pt_mm != NULL) {
		use_mm(ptask->pt_mm);
		set_fs(ptask->pt_fs);
	}

	if (ptask->pt_cbfunc != NULL)
		ptask->pt_result = ptask->pt_cbfunc(ptask);
	else
		ptask->pt_result = -ENOSYS;

	if (cfs_ptask_use_user_mm(ptask) && ptask->pt_mm != NULL) {
		set_fs(old_fs);
		unuse_mm(ptask->pt_mm);
		mmput(ptask->pt_mm);
		ptask->pt_mm = NULL;
	}

	if (cfs_ptask_need_complete(ptask) && !cfs_ptask_is_ordered(ptask))
		complete(&ptask->pt_completion);

	if (!cfs_ptask_is_atomic(ptask))
		local_bh_disable();

	padata_do_serial(padata);
}

static int cfs_do_parallel(struct cfs_ptask_engine *engine,
			   struct padata_priv *padata,
			   unsigned int *pcbcpu)
{
	unsigned int cpu_index, cpu, i;
	struct cfs_ptengine_cpumask *cpumask;
	struct cfs_ptask *ptask = cfs_padata2ptask(padata);
	int rc;

	cpu = *pcbcpu;

	rcu_read_lock_bh();
	cpumask = rcu_dereference_bh(engine->pte_cpumask);
	if (cpumask_test_cpu(cpu, cpumask->mask))
		goto cpu_ok;

	if (!cpumask_weight(cpumask->mask))
		goto cpu_ok;

	cpu_index = cpu % cpumask_weight(cpumask->mask);

	cpu = cpumask_first(cpumask->mask);
	for (i = 0; i < cpu_index; i++)
		cpu = cpumask_next(cpu, cpumask->mask);

	*pcbcpu = cpu;
cpu_ok:
	rcu_read_unlock_bh();

	if (cfs_ptask_need_complete(ptask))
		reinit_completion(&ptask->pt_completion);

	if (cfs_ptask_use_user_mm(ptask)) {
		ptask->pt_mm = get_task_mm(current);
		ptask->pt_fs = get_fs();
	}
	ptask->pt_result = -EINPROGRESS;

retry:
	rc = padata_do_parallel(engine->pte_pinst, padata, cpu);
	if (rc == -EBUSY && cfs_ptask_is_retry(ptask)) {
		/* too many tasks already in queue */
		schedule_timeout_uninterruptible(1);
		goto retry;
	}

	if (rc) {
		if (cfs_ptask_use_user_mm(ptask) && ptask->pt_mm != NULL) {
			mmput(ptask->pt_mm);
			ptask->pt_mm = NULL;
		}
		ptask->pt_result = rc;
	}

	return rc;
}

int cfs_ptask_submit(struct cfs_ptask *ptask, enum cfs_ptask_type ptype)
{
	struct padata_priv *padata = cfs_ptask2padata(ptask);

	if (ptype < PTT_MIN || ptype >= PTT_MAX)
		return -EINVAL;

	memset(padata, 0, sizeof(*padata));

	padata->parallel = cfs_ptask_parallel;
	padata->serial   = cfs_ptask_serial;

	return cfs_do_parallel(&cfs_ptengine[ptype], padata, &ptask->pt_cbcpu);
}
EXPORT_SYMBOL(cfs_ptask_submit);

int cfs_ptask_waitfor(struct cfs_ptask *ptask)
{
	if (!cfs_ptask_need_complete(ptask))
		return -EINVAL;

	wait_for_completion(&ptask->pt_completion);

	return 0;
}
EXPORT_SYMBOL(cfs_ptask_waitfor);

int cfs_ptask_init(struct cfs_ptask *ptask, cfs_ptask_cb_t cbfunc, void *cbdata,
		   unsigned int flags, int cpu)
{
	memset(ptask, 0, sizeof(*ptask));

	ptask->pt_flags  = flags;
	ptask->pt_cbcpu  = cpu;
	ptask->pt_mm     = NULL; /* will be set in cfs_do_parallel() */
	ptask->pt_fs     = get_fs();
	ptask->pt_cbfunc = cbfunc;
	ptask->pt_cbdata = cbdata;
	ptask->pt_result = -EAGAIN;

	if (cfs_ptask_need_complete(ptask)) {
		if (cfs_ptask_is_autofree(ptask))
			return -EINVAL;

		init_completion(&ptask->pt_completion);
	}

	if (cfs_ptask_is_atomic(ptask) && cfs_ptask_use_user_mm(ptask))
		return -EINVAL;

	return 0;
}
EXPORT_SYMBOL(cfs_ptask_init);

int cfs_ptasks_set_cpumask(enum cfs_ptask_type ptype, cpumask_var_t cpumask)
{
	cpumask_var_t tmpmask;
	int rc;

	if (ptype < PTT_MIN || ptype >= PTT_MAX)
		return -EINVAL;

	if (!alloc_cpumask_var(&tmpmask, GFP_KERNEL))
		return -ENOMEM;

	if (!cpumask_equal(cpumask, cpu_online_mask)) {
		cpumask_complement(tmpmask, cpumask);
		cpumask_and(tmpmask, tmpmask, cpu_online_mask);
	} else {
		cpumask_copy(tmpmask, cpu_online_mask);
	}

	rc = padata_set_cpumasks(cfs_ptengine[ptype].pte_pinst,
				 cpumask, tmpmask);

	free_cpumask_var(tmpmask);
	return rc;
}
EXPORT_SYMBOL(cfs_ptasks_set_cpumask);

int cfs_ptasks_weight(enum cfs_ptask_type ptype)
{
	if (ptype < PTT_MIN || ptype >= PTT_MAX)
		return -EINVAL;

	return cfs_ptengine[ptype].pte_weight;
}
EXPORT_SYMBOL(cfs_ptasks_weight);

int cfs_ptasks_estimate_tasks(enum cfs_ptask_type ptype)
{
	if (ptype < PTT_MIN || ptype >= PTT_MAX)
		return -EINVAL;

	return min(cfs_ptengine[ptype].pte_weight, max(max_pio_tasks, 1));
}
EXPORT_SYMBOL(cfs_ptasks_estimate_tasks);

size_t cfs_ptasks_estimate_chunk(enum cfs_ptask_type ptype,
				 size_t size, size_t *pntasks)
{
	size_t ntasks = 1;
	size_t chsize = size;

	if (ptype < PTT_MIN || ptype >= PTT_MAX ||
	    size < (min_pio_pages << (PAGE_SHIFT + 1)))
		goto out;

	for (ntasks = cfs_ptasks_estimate_tasks(ptype); ntasks > 1; ntasks--) {
		chsize = (size + ntasks - 1) / ntasks;
		if (chsize >= (min_pio_pages << PAGE_SHIFT))
			break;
	}
out:
	if (pntasks != NULL)
		*pntasks = ntasks;

	return chsize;
}
EXPORT_SYMBOL(cfs_ptasks_estimate_chunk);

static int cfs_ptask_cpumask_change_notify(struct notifier_block *self,
					   unsigned long val, void *data)
{
	struct cfs_ptask_engine *engine;
	struct cfs_ptengine_cpumask *new_cpumask, *old_cpumask;
	struct padata_cpumask *padata_cpumask = data;

	engine = container_of(self, struct cfs_ptask_engine, pte_nblock);

	if (val & PADATA_CPU_PARALLEL)
		engine->pte_weight = cpumask_weight(padata_cpumask->pcpu);

	if (val & PADATA_CPU_SERIAL) {
		new_cpumask = kmalloc(sizeof(*new_cpumask), GFP_KERNEL);
		if (new_cpumask == NULL)
			return -ENOMEM;
		if (!alloc_cpumask_var(&new_cpumask->mask, GFP_KERNEL)) {
			kfree(new_cpumask);
			return -ENOMEM;
		}

		old_cpumask = engine->pte_cpumask;

		cpumask_copy(new_cpumask->mask, padata_cpumask->cbcpu);
		rcu_assign_pointer(engine->pte_cpumask, new_cpumask);
		synchronize_rcu_bh();

		kfree(old_cpumask);
	}

	return 0;
}

static int cfs_ptengine_init(struct cfs_ptask_engine *engine,
			     enum cfs_ptask_type ptype, const char *name)
{
	cpumask_var_t tmpmask;
	cpumask_var_t allmask;
	struct cfs_ptengine_cpumask *cpumask;
	unsigned int wq_flags = WQ_MEM_RECLAIM | WQ_CPU_INTENSIVE;
	int rc = -ENOMEM;

	if (ptype == PTT_HPRI)
		wq_flags |= WQ_HIGHPRI;

	get_online_cpus();

	engine->pte_wq = alloc_workqueue("cfs_pt_%s", wq_flags, 1, name);
	if (engine->pte_wq == NULL)
		goto err;

	if (!alloc_cpumask_var(&allmask, GFP_KERNEL))
		goto err_destroy_workqueue;

	if (!alloc_cpumask_var(&tmpmask, GFP_KERNEL))
		goto err_free_allmask;

	cpumask = kmalloc(sizeof(*cpumask), GFP_KERNEL);
	if (cpumask == NULL)
		goto err_free_tmpmask;
	if (!alloc_cpumask_var(&cpumask->mask, GFP_KERNEL)) {
		kfree(cpumask);
		goto err_free_tmpmask;
	}

	cpumask_clear(tmpmask);
	switch (ptype) {
	case PTT_HPRI:
		if (*hpri_cpulist != '\0') {
			rc = cpulist_parse(hpri_cpulist, tmpmask);
			if (rc) {
				CERROR("Cannot parse option "
					"hpri_cpulist='%s'\n", hpri_cpulist);
			}
		}
		break;
	case PTT_CLIO:
		if (*clio_cpulist != '\0') {
			rc = cpulist_parse(clio_cpulist, tmpmask);
			if (rc) {
				CERROR("Cannot parse option "
					"clio_cpulist='%s'\n", clio_cpulist);
			}
		}
		break;
	case PTT_RDAH:
		if (*rdah_cpulist != '\0') {
			rc = cpulist_parse(rdah_cpulist, tmpmask);
			if (rc) {
				CERROR("Cannot parse option "
					"rdah_cpulist='%s'\n", rdah_cpulist);
			}
		}
		break;
	default:
		rc = -EINVAL;
		goto err_free_cpumask;
	}
	if (cpumask_empty(tmpmask)) {
		cpumask_copy(allmask, cpu_online_mask);
		while (!cpumask_empty(allmask)) {
			int cpu = cpumask_first(allmask);

			cpumask_set_cpu(cpu, tmpmask);
			cpumask_andnot(allmask, allmask, cpu_sibling_mask(cpu));
		}
	}
	if (!cpumask_equal(tmpmask, cpu_online_mask)) {
		cpumask_complement(cpumask->mask, tmpmask);
		cpumask_and(cpumask->mask, cpumask->mask, cpu_online_mask);
	} else {
		cpumask_copy(cpumask->mask, cpu_online_mask);
	}
	rcu_assign_pointer(engine->pte_cpumask, cpumask);

#ifdef CDEBUG_ENABLED
	if (1) { /* XXX: debug */
		char buf1[128] = ""; char buf2[128] = "";
		cpulist_scnprintf(buf1, sizeof(buf1), tmpmask);
		buf1[sizeof(buf1)-1] = '\0';
		cpulist_scnprintf(buf2, sizeof(buf2), cpumask->mask);
		buf2[sizeof(buf2)-1] = '\0';
		CDEBUG(D_INFO, "%s weight=%u plist='%s' cblist='%s'\n",
				name, cpumask_weight(tmpmask), buf1, buf2);
	} /* XXX: debug */
#endif

	engine->pte_weight = cpumask_weight(tmpmask);
	engine->pte_pinst = padata_alloc(engine->pte_wq, tmpmask, cpumask->mask);
	if (engine->pte_pinst == NULL) {
		CERROR("padata_alloc() error\n");
		rc = -ENOMEM;
		goto err_free_cpumask;
	}

	engine->pte_nblock.notifier_call = cfs_ptask_cpumask_change_notify;
	rc = padata_register_cpumask_notifier(engine->pte_pinst,
					      &engine->pte_nblock);
	if (rc)
		goto err_free_padata;

	free_cpumask_var(tmpmask);
	free_cpumask_var(allmask);
	put_online_cpus();
	return rc;

err_free_padata:
	padata_free(engine->pte_pinst);
err_free_cpumask:
	free_cpumask_var(cpumask->mask);
	kfree(cpumask);
err_free_tmpmask:
	free_cpumask_var(tmpmask);
err_free_allmask:
	free_cpumask_var(allmask);
err_destroy_workqueue:
	destroy_workqueue(engine->pte_wq);
err:
	put_online_cpus();
	return rc;
}

static void cfs_ptengine_fini(struct cfs_ptask_engine *engine)
{
	free_cpumask_var(engine->pte_cpumask->mask);
	kfree(engine->pte_cpumask);

	padata_stop(engine->pte_pinst);
	padata_unregister_cpumask_notifier(engine->pte_pinst,
					   &engine->pte_nblock);
	padata_free(engine->pte_pinst);
	destroy_workqueue(engine->pte_wq);
}

int cfs_ptasks_init(void)
{
	int rc;
	enum cfs_ptask_type ptype;
	static const char *ptt2name[PTT_MAX] = {
		[PTT_HPRI] = "hpri",
		[PTT_CLIO] = "clio",
		[PTT_RDAH] = "rdah"
	};

	for (ptype = PTT_MIN; ptype < PTT_MAX; ptype++) {
		rc = cfs_ptengine_init(&cfs_ptengine[ptype],
					ptype, ptt2name[ptype]);
		if (rc)
			goto err_free;

		padata_start(cfs_ptengine[ptype].pte_pinst);
	}

	return 0;

err_free:
	while (--ptype >= PTT_MIN)
		cfs_ptengine_fini(&cfs_ptengine[ptype]);
	return rc;
}
EXPORT_SYMBOL(cfs_ptasks_init);

void cfs_ptasks_fini(void)
{
	enum cfs_ptask_type ptype;

	for (ptype = PTT_MIN; ptype < PTT_MAX; ptype++) {
		cfs_ptengine_fini(&cfs_ptengine[ptype]);
	}
}
EXPORT_SYMBOL(cfs_ptasks_fini);
