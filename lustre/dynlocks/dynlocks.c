/*
 * Dynamic Locks
 *
 * struct dynlock is lockspace
 * one may request lock (exclusive or shared) for some value
 * in that lockspace
 *
 */

#include <linux/dynlocks.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/sched.h>

#define DYNLOCK_HANDLE_MAGIC	0xd19a10c
#define DYNLOCK_HANDLE_DEAD	0xd1956ee
#define DYNLOCK_LIST_MAGIC	0x11ee91e6

static struct kmem_cache * dynlock_cachep = NULL;

struct dynlock_handle {
	unsigned		dh_magic;
	struct list_head	dh_list;
	unsigned long		dh_value;	/* lock value */
	int			dh_refcount;	/* number of users */
	int			dh_readers;
	int			dh_writers;
	int			dh_pid;		/* holder of the lock */
	wait_queue_head_t	dh_wait;
};

int __init dynlock_cache_init(void)
{
	int rc = 0;

	/* printk(KERN_INFO "init dynlocks cache\n"); */
	dynlock_cachep = kmem_cache_create("dynlock_cache",
					 sizeof(struct dynlock_handle),
					 0,
					 SLAB_HWCACHE_ALIGN,
					 NULL);
	if (dynlock_cachep == NULL) {
		printk(KERN_ERR "Not able to create dynlock cache");
		rc = -ENOMEM;
	}
	return rc;
}

void dynlock_cache_exit(void)
{
	/* printk(KERN_INFO "exit dynlocks cache\n"); */
	kmem_cache_destroy(dynlock_cachep);
}

/*
 * dynlock_init
 *
 * initialize lockspace
 *
 */
void dynlock_init(struct dynlock *dl)
{
	spin_lock_init(&dl->dl_list_lock);
	INIT_LIST_HEAD(&dl->dl_list);
	dl->dl_magic = DYNLOCK_LIST_MAGIC;
}
EXPORT_SYMBOL(dynlock_init);

/*
 * dynlock_lock
 *
 * acquires lock (exclusive or shared) in specified lockspace
 * each lock in lockspace is allocated separately, so user have
 * to specify GFP flags.
 * routine returns pointer to lock. this pointer is intended to
 * be passed to dynlock_unlock
 *
 */
struct dynlock_handle *dynlock_lock(struct dynlock *dl, unsigned long value,
				    enum dynlock_type lt, gfp_t gfp)
{
	struct dynlock_handle *nhl = NULL;
	struct dynlock_handle *hl;

	BUG_ON(dl == NULL);
	BUG_ON(dl->dl_magic != DYNLOCK_LIST_MAGIC);

repeat:
	/* find requested lock in lockspace */
	spin_lock(&dl->dl_list_lock);
	BUG_ON(dl->dl_list.next == NULL);
	BUG_ON(dl->dl_list.prev == NULL);
	list_for_each_entry(hl, &dl->dl_list, dh_list) {
		BUG_ON(hl->dh_list.next == NULL);
		BUG_ON(hl->dh_list.prev == NULL);
		BUG_ON(hl->dh_magic != DYNLOCK_HANDLE_MAGIC);
		if (hl->dh_value == value) {
			/* lock is found */
			if (nhl) {
				/* someone else just allocated
				 * lock we didn't find and just created
				 * so, we drop our lock
				 */
				kmem_cache_free(dynlock_cachep, nhl);
				nhl = NULL;
			}
			hl->dh_refcount++;
			goto found;
		}
	}
	/* lock not found */
	if (nhl) {
		/* we already have allocated lock. use it */
		hl = nhl;
		nhl = NULL;
		list_add(&hl->dh_list, &dl->dl_list);
		goto found;
	}
	spin_unlock(&dl->dl_list_lock);

	/* lock not found and we haven't allocated lock yet. allocate it */
	nhl = kmem_cache_alloc(dynlock_cachep, gfp);
	if (nhl == NULL)
		return NULL;
	nhl->dh_refcount = 1;
	nhl->dh_value = value;
	nhl->dh_readers = 0;
	nhl->dh_writers = 0;
	nhl->dh_magic = DYNLOCK_HANDLE_MAGIC;
	init_waitqueue_head(&nhl->dh_wait);

	/* while lock is being allocated, someone else may allocate it
	 * and put onto to list. check this situation
	 */
	goto repeat;

found:
	if (lt == DLT_WRITE) {
		/* exclusive lock: user don't want to share lock at all
		 * NOTE: one process may take the same lock several times
		 * this functionaly is useful for rename operations */
		while ((hl->dh_writers && hl->dh_pid != current->pid) ||
				hl->dh_readers) {
			spin_unlock(&dl->dl_list_lock);
			wait_event(hl->dh_wait,
				hl->dh_writers == 0 && hl->dh_readers == 0);
			spin_lock(&dl->dl_list_lock);
		}
		hl->dh_writers++;
	} else {
		/* shared lock: user do not want to share lock with writer */
		while (hl->dh_writers) {
			spin_unlock(&dl->dl_list_lock);
			wait_event(hl->dh_wait, hl->dh_writers == 0);
			spin_lock(&dl->dl_list_lock);
		}
		hl->dh_readers++;
	}
	hl->dh_pid = current->pid;
	spin_unlock(&dl->dl_list_lock);

	return hl;
}
EXPORT_SYMBOL(dynlock_lock);


/*
 * dynlock_unlock
 *
 * user have to specify lockspace (dl) and pointer to lock structure
 * returned by dynlock_lock()
 *
 */
void dynlock_unlock(struct dynlock *dl, struct dynlock_handle *hl)
{
	int wakeup = 0;

	BUG_ON(dl == NULL);
	BUG_ON(hl == NULL);
	BUG_ON(dl->dl_magic != DYNLOCK_LIST_MAGIC);

	if (hl->dh_magic != DYNLOCK_HANDLE_MAGIC)
		printk(KERN_EMERG "wrong lock magic: %#x\n", hl->dh_magic);

	BUG_ON(hl->dh_magic != DYNLOCK_HANDLE_MAGIC);
	BUG_ON(hl->dh_writers != 0 && current->pid != hl->dh_pid);

	spin_lock(&dl->dl_list_lock);
	if (hl->dh_writers) {
		BUG_ON(hl->dh_readers != 0);
		hl->dh_writers--;
		if (hl->dh_writers == 0)
			wakeup = 1;
	} else if (hl->dh_readers) {
		hl->dh_readers--;
		if (hl->dh_readers == 0)
			wakeup = 1;
	} else {
		BUG();
	}
	if (wakeup) {
		hl->dh_pid = 0;
		wake_up(&hl->dh_wait);
	}
	if (--(hl->dh_refcount) == 0) {
		hl->dh_magic = DYNLOCK_HANDLE_DEAD;
		list_del(&hl->dh_list);
		kmem_cache_free(dynlock_cachep, hl);
	}
	spin_unlock(&dl->dl_list_lock);
}
EXPORT_SYMBOL(dynlock_unlock);

int dynlock_is_locked(struct dynlock *dl, unsigned long value)
{
	struct dynlock_handle *hl;
	int result = 0;

	/* find requested lock in lockspace */
	spin_lock(&dl->dl_list_lock);
	BUG_ON(dl->dl_list.next == NULL);
	BUG_ON(dl->dl_list.prev == NULL);
	list_for_each_entry(hl, &dl->dl_list, dh_list) {
		BUG_ON(hl->dh_list.next == NULL);
		BUG_ON(hl->dh_list.prev == NULL);
		BUG_ON(hl->dh_magic != DYNLOCK_HANDLE_MAGIC);
		if (hl->dh_value == value && hl->dh_pid == current->pid) {
			/* lock is found */
			result = 1;
			break;
		}
	}
	spin_unlock(&dl->dl_list_lock);
	return result;
}
EXPORT_SYMBOL(dynlock_is_locked);

MODULE_AUTHOR("Whamcloud, Inc. <http://www.whamcloud.com/>");
MODULE_DESCRIPTION("Lustre Dynamic Locks (dynlocks)");
MODULE_LICENSE("GPL");

module_init(dynlock_cache_init);
module_exit(dynlock_cache_exit);
