#ifndef __LIBCFS_PTASK_H__
#define __LIBCFS_PTASK_H__

#include <linux/types.h>
#include <linux/kernel.h>
#include <linux/uaccess.h>
#include <linux/completion.h>
#ifdef CONFIG_PADATA
#include <linux/padata.h>
#else
struct padata_priv {};
struct padata_instance {};
#endif

enum cfs_ptask_type {
	PTT_HPRI = 0,
	PTT_CLIO,
	PTT_RDAH,
	PTT_MAX,
	PTT_MIN = PTT_HPRI
};

#define PTF_COMPLETE	(1 << 0)
#define PTF_AUTOFREE	(1 << 1)
#define PTF_ORDERED	(1 << 2)
#define PTF_USER_MM	(1 << 3)
#define PTF_ATOMIC	(1 << 4)
#define PTF_RETRY	(1 << 5)

struct cfs_ptask;
typedef int (*cfs_ptask_cb_t)(struct cfs_ptask *);

struct cfs_ptask {
	struct padata_priv	 pt_padata;
	struct completion	 pt_completion;
	unsigned int		 pt_flags;
	unsigned int		 pt_cbcpu;
	struct mm_struct	*pt_mm;
	mm_segment_t		 pt_fs;
	cfs_ptask_cb_t		 pt_cbfunc;
	void			*pt_cbdata;
	int			 pt_result;
};

static inline
struct padata_priv *cfs_ptask2padata(struct cfs_ptask *ptask)
{
	return &ptask->pt_padata;
}

static inline
struct cfs_ptask *cfs_padata2ptask(struct padata_priv *padata)
{
	return container_of(padata, struct cfs_ptask, pt_padata);
}

static inline
bool cfs_ptask_need_complete(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_COMPLETE;
}

static inline
bool cfs_ptask_is_autofree(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_AUTOFREE;
}

static inline
bool cfs_ptask_is_ordered(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_ORDERED;
}

static inline
bool cfs_ptask_use_user_mm(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_USER_MM;
}

static inline
bool cfs_ptask_is_atomic(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_ATOMIC;
}

static inline
bool cfs_ptask_is_retry(struct cfs_ptask *ptask)
{
	return ptask->pt_flags & PTF_RETRY;
}

static inline
int cfs_ptask_result(struct cfs_ptask *ptask)
{
	return ptask->pt_result;
}

int  cfs_ptasks_init(void);
void cfs_ptasks_fini(void);
int  cfs_ptasks_set_cpumask(enum cfs_ptask_type, cpumask_var_t);

int  cfs_ptasks_weight(enum cfs_ptask_type);
int  cfs_ptasks_estimate_tasks(enum cfs_ptask_type);
size_t cfs_ptasks_estimate_chunk(enum cfs_ptask_type, size_t, size_t *);

int  cfs_ptask_submit(struct cfs_ptask *, enum cfs_ptask_type);
int  cfs_ptask_waitfor(struct cfs_ptask *);
int  cfs_ptask_init(struct cfs_ptask *, cfs_ptask_cb_t, void *,
		    unsigned int, int);

#endif /* __LIBCFS_PTASK_H__ */
