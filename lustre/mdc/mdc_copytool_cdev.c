#include <linux/fs.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/poll.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/workqueue.h>

#include <lustre_param.h>
#include <lustre/lustre_user.h>
#include <lustre/lustre_copytool_cdev.h>
#include "mdc_internal.h"

MODULE_LICENSE("GPL");

#ifndef list_prev_entry
#define list_prev_entry(pos, member) \
	list_entry((pos)->member.prev, typeof(*(pos)), member)
#endif
#ifndef list_last_entry
#define list_last_entry(ptr, type, member) \
	list_entry((ptr)->prev, type, member)
#endif

/* Necessary until Lustre stops supporting kernels < 2.6.32-573.12.1 */
#ifndef HAVE_KREF_GET_UNLESS_ZERO
static inline int __must_check kref_get_unless_zero(struct kref *kref)
{
	return atomic_add_unless(&kref->refcount, 1, 0);
}
#endif

/* We need twice the size the biggest hal_record can be because readers
 * keep a reference on the last message they read. If we don't, this
 * might cause a call "ct_cdev_hal_publish" to wait indefinitely
 */
#define QUEUE_SIZE_MAX ((HAL_MAXSIZE + sizeof(struct hal_record)) * 2)

#define CTDEV_NAME_LEN_MAX (LUSTRE_MAXFSNAME + 10) /* copytool-<fsname> */

/* The list of devices */
static LIST_HEAD(device_list);
/* Protects the device registration and deregistration, to acquire when you:
 * - modify "device_list"
 * - register/unregister a miscdevice
 */
static DECLARE_RWSEM(device_rwsem);


struct hal_record {
	struct ct_cdev		*hrec_ct_cdev;
	struct list_head	 hrec_list;
	struct kref		 hrec_ref;
	size_t			 hrec_hal_sz;
	struct hsm_action_list	 hrec_hal[];
};

/* Shortcut to the list of hal_records of a copytool char device */
#define hal_records_list dummy_hal_record.hrec_list
/* The copytool char device struct
 * (note that hal_records_list might be used like another member of that
 * struct)
 */
struct ct_cdev {
	/* To register the char device */
	struct miscdevice	 misc;
	char			 name[CTDEV_NAME_LEN_MAX];
	struct list_head	 list;
	struct kref		 ref;

	/* To operate the char device */
	struct hal_record	 dummy_hal_record;
	struct rw_semaphore	 hal_records_rwsem;
	struct workqueue_struct	*rd_wake_up_wqueue;
	struct work_struct	 rd_wake_up_work;
	wait_queue_head_t	 rd_queue;
	wait_queue_head_t	 wr_queue;
	atomic_t		 readers_number;
	atomic_t		 queue_size;
};

struct reader {
	struct ct_cdev		*reader_ct_cdev;
	struct hal_record	*reader_last_rec;
	/* To protect the archive_mask */
	struct mutex		 reader_archive_mask_mutex;
	/* Defaults to 0 => broadcast */
	int			 reader_archive_mask;
};

/**
 * Retrieve the fsname of an obd_device from its obd_name member
 */
static int obd_name2fsname(char *fsname, const char *obd_name)
{
	const char *minus; /* the 3rd '-' starting from the end of obd_name */
	int minuses_to_find = 3;

	/* obd_name looks something like this:
	 * <fsname>-MDT0000-mdc-<uuid>
	 * Don't use strrchr in case obd_name is not null terminated
	 */
	minus = obd_name + strnlen(obd_name, MAX_OBD_NAME);
	for (; minus > obd_name; minus--) {
		if (*minus == '-')
			if (--minuses_to_find <= 0)
				break;
	}
	if (minuses_to_find != 0 || minus - obd_name > LUSTRE_MAXFSNAME)
		/* Misformatted obd_name (should not happen) */
		return -EINVAL;

	strlcpy(fsname, obd_name, minus - obd_name + 1);
	return 0;
}

/**
 * Get the fsname a copytool char device is working for
 *
 * \retval	a pointer to the fsname the device is working for
 *
 * The device's name must have been initialized first
 */
static inline char *ct_cdev_fsname(struct ct_cdev *device)
{
	/* device->name = "copytool-<fsname>" */
	/*                 012...   9 */
	return device->name + 9;
}

/**
 * Find a copytool char device from an obd_device
 * One must hold device_rwsem while using this function
 *
 * \param obd	the obd_device that the char device is supposed to be linked to
 *
 * \retval	the char device linked to obd if it exists
 *		NULL otherwise
 */
static struct ct_cdev *obd2ct_cdev_locked(const struct obd_device *obd)
{
	char fsname[LUSTRE_MAXFSNAME + 1] = "";
	struct ct_cdev *device;

	if (obd_name2fsname(fsname, obd->obd_name)) {
		CERROR("Misformatted obd_name: '%s', expected <fsname>-<id>-<component>-<uuid>\n",
		       obd->obd_name);
		return NULL;
	}

	list_for_each_entry(device, &device_list, list) {
		if (strcmp(ct_cdev_fsname(device), fsname) == 0)
			return device;
	}

	return NULL;
}

/**
 * Is there enough space for count bytes in a given copytool char device ?
 */
static inline bool enough_space_left(struct ct_cdev *device, int count)
{
	return (atomic_read(&device->queue_size) + count +
		sizeof(struct hal_record) <= QUEUE_SIZE_MAX);
}

/**
 * Is this hal_record compatible with this archive_mask ?
 */
static bool right_channel(struct hal_record *hrec, struct reader *reader)
{
	int archive_mask;

	mutex_lock(&reader->reader_archive_mask_mutex);
	archive_mask = reader->reader_archive_mask;
	mutex_unlock(&reader->reader_archive_mask_mutex);
	return !archive_mask ||
	       archive_mask & (1 << (hrec->hrec_hal->hal_archive_id - 1));
}

/**
 * Is there a hal_record this reader can read ?
 */
static bool something_to_read(const struct reader *reader)
{
	/* No need to take a lock because even if a hal_record is being written
	 * list_is_last checks for equality between pointer. This will return
	 * the expected result
	 */
	return !list_is_last(&reader->reader_last_rec->hrec_list,
			     &reader->reader_ct_cdev->hal_records_list);
}

/**
 * Wake up a device's readers. This should be run every time a hal_record is
 * written.
 */
static void wake_up_readers(struct work_struct *work)
{
	struct ct_cdev *device = container_of(work, struct ct_cdev,
					      rd_wake_up_work);

	wake_up_interruptible(&device->rd_queue);
}

static void ct_cdev_deregister(struct kref *kref)
{
	struct ct_cdev *device = container_of(kref, struct ct_cdev,
					      ref);

	ENTRY;
	down_write(&device_rwsem);

	list_del(&device->list);

	up_write(&device_rwsem);

	misc_deregister(&device->misc);
	flush_workqueue(device->rd_wake_up_wqueue);
	destroy_workqueue(device->rd_wake_up_wqueue);
	OBD_FREE_PTR(device);
	EXIT;
}

/**
 * Adds a hsm_action_list to a copytool char device. The device is chosen
 * according to the obd provided.
 *
 * \param obd	the obd_device the copytool you want to access is working for
 * \param hal	the hsm_action_list to publish
 *
 * \retval	the number of bytes copied to the char device
 */
ssize_t ct_cdev_hal_publish(struct obd_device *obd, struct hsm_action_list *hal,
			    int flags)
{
	struct ct_cdev *device;
	size_t count = hal_size(hal);
	struct hal_record *hrec;
	int rc;
	int i;

	ENTRY;
	CDEBUG(D_HSM,
	       "ct_cdev: Writing hal '"LPX64"' (%zu o) on channel %i...\n",
	       hal->hal_compound_id, count, hal->hal_archive_id);

	/* Early checks */
	if (count > HAL_MAXSIZE)
		/* Should not happen */
		RETURN(-EINVAL);

	/* Find the device associated to the obd */
	down_read(&device_rwsem);

	mutex_lock(&obd->u.cli.cl_ct_cdev_mutex);
	device = obd->u.cli.cl_ct_cdev;
	mutex_unlock(&obd->u.cli.cl_ct_cdev_mutex);
	if (device == NULL || !kref_get_unless_zero(&device->ref)) {
		CERROR("No device found for obd: '%s'", obd->obd_name);
		up_read(&device_rwsem);
		RETURN(-EINVAL);
	}

	up_read(&device_rwsem);

	if (flags & O_NONBLOCK && !enough_space_left(device, count))
		GOTO(out, rc = -EWOULDBLOCK);

	if (atomic_read(&device->readers_number) == 0)
		/* Nothing to do */
		GOTO(out, rc = count);

	/* Create the hal_record that will hold the hsm_action_list */
	OBD_ALLOC(hrec, sizeof(*hrec) + count);
	if (hrec == NULL)
		GOTO(out, rc = -ENOMEM);
	hrec->hrec_ct_cdev = device;
	hrec->hrec_hal_sz = count;
	memcpy(hrec->hrec_hal, hal, count);

repeat:
	if (wait_event_interruptible(device->wr_queue,
				     enough_space_left(device, count) ||
				     flags & O_NONBLOCK)) {
		OBD_FREE(hrec, sizeof(*hrec) + hrec->hrec_hal_sz);
		GOTO(out, rc = -ERESTARTSYS);
	}

	down_write(&device->hal_records_rwsem);

	if (atomic_read(&device->readers_number) == 0) {
		up_write(&device->hal_records_rwsem);
		OBD_FREE(hrec, sizeof(*hrec) + hrec->hrec_hal_sz);
		GOTO(out, rc = count);
	}

	if (!enough_space_left(device, count)) {
		/* Another writer was woken up before us and filled the
		 * queue
		 */
		up_write(&device->hal_records_rwsem);
		if (flags & O_NONBLOCK) {
			OBD_FREE(hrec, sizeof(*hrec) + hrec->hrec_hal_sz);
			GOTO(out, rc = -EWOULDBLOCK);
		}
		goto repeat;
	}

	/* Set how many readers will see that hal_record */
	kref_init(&hrec->hrec_ref);	/* Initialize ref counter to 1 */
	for (i = atomic_read(&device->readers_number); i > 1; i--)
		kref_get(&hrec->hrec_ref);

	/* Publish the hal_record using an rcu method (cf. read) */
	list_add_tail_rcu(&hrec->hrec_list, &device->hal_records_list);
	/* Update the queue size */
	atomic_add(sizeof(*hrec) + count, &device->queue_size);

	up_write(&device->hal_records_rwsem);

	queue_work(device->rd_wake_up_wqueue, &device->rd_wake_up_work);

	CDEBUG(D_HSM, "ct_cdev: Wrote hal "LPX64" for the obd '%s'\n",
	       hal->hal_compound_id, obd->obd_name);

	GOTO(out, rc = count);

out:
	kref_put(&device->ref, ct_cdev_deregister);
	return rc;
}

/**
 * Finds a copytool char device from its inode
 */
static struct ct_cdev *inode2ct_cdev(struct inode *inode)
{
	struct ct_cdev *device = NULL;
	int minor = MINOR(inode->i_rdev);

	down_read(&device_rwsem);
	list_for_each_entry(device, &device_list, list) {
		if (device->misc.minor == minor)
			break;
	}
	up_read(&device_rwsem);

	return device;
}

static int ct_cdev_open(struct inode *inode, struct file *file)
{
	ENTRY;
	if (file->f_mode & FMODE_READ) {
		struct ct_cdev *device;
		struct reader *reader;

		down_read(&device_rwsem);

		device = inode2ct_cdev(inode);
		if (device == NULL || !kref_get_unless_zero(&device->ref)) {
			/* Unfortunate turn of events, the device is being
			 * unregistered as someone tries to open it.
			 */
			CDEBUG(D_HSM, "ct_cdev: device recently deregistered");
			up_read(&device_rwsem);
			RETURN(-ERESTARTSYS);
		}

		up_read(&device_rwsem);

		CDEBUG(D_HSM, "ct_cdev: Opening the device in reading mode...");

		/* Creating the reader */
		OBD_ALLOC_PTR(reader);
		if (reader == NULL)
			return -ENOMEM;

		reader->reader_ct_cdev = device;
		mutex_init(&reader->reader_archive_mask_mutex);

		down_read(&device->hal_records_rwsem);
		reader->reader_last_rec =
			list_last_entry(&device->hal_records_list,
					struct hal_record, hrec_list);

		if (!kref_get_unless_zero(&reader->reader_last_rec->hrec_ref)) {
			/* The last record of the records_list is almost removed
			 * (kref_put was called but the cleanup functions has
			 * not been run yet). Since records are removed from the
			 * list in the order they were inserted, the next
			 * last_record is actually the dummy record
			 */
			reader->reader_last_rec =
				list_entry(&device->hal_records_list,
					   struct hal_record, hrec_list);
		}

		atomic_inc(&device->readers_number);

		up_read(&device->hal_records_rwsem);

		file->private_data = reader;
	}

	RETURN(0);
}

static void cleanup_hal_record(struct kref *ref)
{
	struct hal_record *hrec = container_of(ref, struct hal_record,
					       hrec_ref);
	struct ct_cdev *device = hrec->hrec_ct_cdev;

	ENTRY;
	down_write(&device->hal_records_rwsem);

	atomic_sub(sizeof(*hrec) + hrec->hrec_hal_sz,
		   &device->queue_size);
	list_del(&hrec->hrec_list);

	up_write(&device->hal_records_rwsem);

	wake_up_interruptible(&device->wr_queue);
	OBD_FREE(hrec, sizeof(*hrec) + hrec->hrec_hal_sz);
	EXIT;
}

static int ct_cdev_release(struct inode *inode, struct file *file)
{
	ENTRY;
	if (file->f_mode & FMODE_READ) {
		struct reader *reader = file->private_data;
		struct ct_cdev *device = reader->reader_ct_cdev;
		struct hal_record *last_rec;
		struct hal_record *next_rec;

		CDEBUG(D_HSM, "ct_cdev: Closing a reader...");
		down_read(&device->hal_records_rwsem);

		last_rec = list_last_entry(&device->hal_records_list,
					   struct hal_record, hrec_list);
		/* Removing the reader */
		atomic_dec(&device->readers_number);

		up_read(&device->hal_records_rwsem);

		/* Clean up every hal_record the reader was scheduled to read */
		list_for_each_entry_safe_from(reader->reader_last_rec,
					      next_rec, &last_rec->hrec_list,
					      hrec_list)
			kref_put(&reader->reader_last_rec->hrec_ref,
				 cleanup_hal_record);
		/* Release the reference on the last hal_record */
		kref_put(&reader->reader_last_rec->hrec_ref,
			 cleanup_hal_record);

		OBD_FREE_PTR(reader);

		kref_put(&device->ref, ct_cdev_deregister);
	}

	RETURN(0);
}

static ssize_t ct_cdev_read(struct file *file, char __user *buff, size_t count,
			    loff_t *ppos)
{
	struct reader *reader = file->private_data;
	struct ct_cdev *device = reader->reader_ct_cdev;
	struct hal_record *last_rec = reader->reader_last_rec;
	struct hal_record *hrec;
	struct hal_record *next_rec;
	size_t rd_bytes = 0;

	ENTRY;
repeat:
	if (file->f_flags & O_NONBLOCK && !something_to_read(reader))
		RETURN(-EWOULDBLOCK);

	if (wait_event_interruptible(device->rd_queue,
				     something_to_read(reader)))
		RETURN(-ERESTARTSYS);

	/* Start at the right offset */
	hrec = reader->reader_last_rec;

	/* In order to only read perfectly sane hal_records, and since we are
	 * not holding any locks (which greatly improves efficiency), this part
	 * relies on rcu.
	 */
	rcu_read_lock();
	list_for_each_entry_continue_rcu(hrec, &device->hal_records_list,
					 hrec_list) {
		last_rec = hrec;
		if (!right_channel(hrec, reader))
			continue;
		if (hrec->hrec_hal_sz > count - rd_bytes) {
			last_rec = list_prev_entry(last_rec, hrec_list);
			break;
		}

		/* Cannot keep the rcu lock while calling copy_to_user as
		 * it might create a deadlock.
		 */
		rcu_read_unlock();
		if (copy_to_user(buff, hrec->hrec_hal, hrec->hrec_hal_sz))
			RETURN(-EFAULT);
		/* Acquire the lock again for the list-traversal operation */
		rcu_read_lock();

		buff += hrec->hrec_hal_sz;
		rd_bytes += hrec->hrec_hal_sz;
	}
	rcu_read_unlock();

	/* Set the offset for the next read while cleaning up hal_records */
	list_for_each_entry_safe_from(reader->reader_last_rec, next_rec,
				      &last_rec->hrec_list, hrec_list)
		kref_put(&reader->reader_last_rec->hrec_ref,
			 cleanup_hal_record);

	if (rd_bytes == 0 && !right_channel(last_rec, reader))
		/* There was not any message on our channel */
		goto repeat;

	CDEBUG(D_HSM, "ct_cdev: Read %zu bytes", rd_bytes);
	RETURN(rd_bytes);
}

#ifdef HAVE_UNLOCKED_IOCTL
static long ct_cdev_ioctl(struct file *file, unsigned int cmd,
			  unsigned long arg)
#else
static int ct_cdev_ioctl(struct inode *inode, struct file *file,
			 unsigned int cmd, unsigned long arg)
#endif
{
	int rc = 0;

	ENTRY;
	if (_IOC_TYPE(cmd) != CT_CDEV_IOC_MAGIC)
		RETURN(-ENOTTY);
	if (_IOC_NR(cmd) > CT_CDEV_IOC_MAXNR)
		RETURN(-ENOTTY);

	switch (cmd) {
	case CT_CDEV_IOC_SET_ARCHIVE_MASK:
		if (file->f_mode & FMODE_READ) {
			struct reader *reader = file->private_data;

			mutex_lock(&reader->reader_archive_mask_mutex);
			reader->reader_archive_mask = arg;
			mutex_unlock(&reader->reader_archive_mask_mutex);
		} else {
			rc = -ENOTTY;
		}
		break;
	default:
		RETURN(-ENOTTY);
	}
	RETURN(rc);
}

static unsigned int ct_cdev_poll(struct file *file, poll_table *wait)
{
	unsigned int mask = 0;

	if (file->f_mode & FMODE_READ) {
		struct reader *reader = file->private_data;
		struct ct_cdev *device = reader->reader_ct_cdev;

		poll_wait(file, &device->rd_queue, wait);
		if (something_to_read(reader))
			mask |= POLLIN | POLLRDNORM;
	}
	return mask;
}

static const struct file_operations ct_cdev_fops = {
	.owner		= THIS_MODULE,
	.open		= ct_cdev_open,
	.release	= ct_cdev_release,
	.read		= ct_cdev_read,
#ifdef HAVE_UNLOCKED_IOCTL
	.unlocked_ioctl	= ct_cdev_ioctl,
#else
	.ioctl		= ct_cdev_ioctl,
#endif
	.poll		= ct_cdev_poll,
};

int ct_cdev_init(struct obd_device *obd)
{
	struct ct_cdev *device;
	char fsname[LUSTRE_MAXFSNAME + 1];
	int rc = 0;

	ENTRY;
	mutex_init(&obd->u.cli.cl_ct_cdev_mutex);

repeat:
	down_write(&device_rwsem);
	device = obd2ct_cdev_locked(obd);
	if (device != NULL) {
		if (!kref_get_unless_zero(&device->ref)) {
			/* Unlikely to happen: the device is being destroyed
			 * right now
			 */
			up_write(&device_rwsem);
			goto repeat;
		}
		mutex_lock(&obd->u.cli.cl_ct_cdev_mutex);
		obd->u.cli.cl_ct_cdev = device;
		mutex_unlock(&obd->u.cli.cl_ct_cdev_mutex);
		up_write(&device_rwsem);
		RETURN(rc);
	}

	/* The device does not exist */
	OBD_ALLOC_PTR(device);
	if (device == NULL) {
		up_write(&device_rwsem);
		RETURN(-ENOMEM);
	}

	rc = obd_name2fsname(fsname, obd->obd_name);
	if (rc != 0) {
		CERROR("Misformatted obd_name: '%s', expected <fsname>-<id>-<component>-<uuid>\n",
		       obd->obd_name);
		goto out_free;
	}
	snprintf(device->name, CTDEV_NAME_LEN_MAX, "copytool-%s", fsname);
	device->misc.minor = MISC_DYNAMIC_MINOR;
	device->misc.name = device->name;
	device->misc.fops = &ct_cdev_fops;

	/* Init dummy hal_record */
	kref_init(&device->dummy_hal_record.hrec_ref);

	/* Reader/Writer semaphore */
	init_rwsem(&device->hal_records_rwsem);

	/* hal_records list */
	INIT_LIST_HEAD(&device->hal_records_list);

	/* Readers queue */
	init_waitqueue_head(&device->rd_queue);
	/* Readers waker workqueue */
	device->rd_wake_up_wqueue = create_singlethread_workqueue(device->name);
	if (device->rd_wake_up_wqueue == NULL) {
		rc = -ENOMEM;
		goto out_free;
	}

	/* Readers waking work */
	INIT_WORK(&device->rd_wake_up_work, wake_up_readers);

	/* Writers queue */
	init_waitqueue_head(&device->wr_queue);

	/* Global readers count */
	atomic_set(&device->readers_number, 0);

	/* Size of the elements in the list */
	atomic_set(&device->queue_size, 0);

	kref_init(&device->ref);

	/* Now we can actually register the device */
	rc = misc_register(&device->misc);
	if (rc != 0)
		goto out_cleanup;

	list_add(&device->list, &device_list);

	/* The obd's mutex that protect the access to the device's reference */
	mutex_lock(&obd->u.cli.cl_ct_cdev_mutex);
	obd->u.cli.cl_ct_cdev = device;
	mutex_unlock(&obd->u.cli.cl_ct_cdev_mutex);
	up_write(&device_rwsem);

	RETURN(rc);

out_cleanup:
	destroy_workqueue(device->rd_wake_up_wqueue);
out_free:
	OBD_FREE_PTR(device);
	up_write(&device_rwsem);
	RETURN(rc);
}

void ct_cdev_exit(struct obd_device *obd)
{
	struct ct_cdev *device;

	ENTRY;
	mutex_lock(&obd->u.cli.cl_ct_cdev_mutex);
	device = obd->u.cli.cl_ct_cdev;
	obd->u.cli.cl_ct_cdev = NULL;
	mutex_unlock(&obd->u.cli.cl_ct_cdev_mutex);
	kref_put(&device->ref, ct_cdev_deregister);
	EXIT;
}
