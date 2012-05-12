#!/bin/sed -f

# Script to cleanup libcfs macros, it runs against the tree at build time.
# Migrate libcfs to emulate Linux kernel APIs.
# http://jira.whamcloud.com/browse/LU-1346

################################################################################
# lock - spinlock, rw_semaphore, rwlock, completion, semaphore, mutex
#      - lock_kernel, unlock_kernel, lockdep

# spinlok
/typedef  *spinlock_t  *cfs_spinlock_t;/d
s/\bcfs_spinlock_t\b/spinlock_t/g
s/\bcfs_spin_lock_init\b/spin_lock_init/g
/# *define *\bspin_lock_init\b *( *\w* *) *\bspin_lock_init\b *( *\w* *)/d
s/\bcfs_spin_lock\b/spin_lock/g
/# *define *\bspin_lock\b *( *\w* *) *\bspin_lock\b *( *\w* *)/d
s/\bcfs_spin_lock_bh\b/spin_lock_bh/g
/# *define *\bspin_lock_bh\b *( *\w* *) *\bspin_lock_bh\b *( *\w* *)/d
s/\bcfs_spin_lock_bh_init\b/spin_lock_bh_init/g
/# *define *\bspin_lock_bh_init\b *( *\w* *) *\bspin_lock_bh_init\b *( *\w* *)/d
s/\bcfs_spin_unlock\b/spin_unlock/g
/# *define *\bspin_unlock\b *( *\w* *) *\bspin_unlock\b *( *\w* *)/d
s/\bcfs_spin_unlock_bh\b/spin_unlock_bh/g
/# *define *\bspin_unlock_bh\b *( *\w* *) *\bspin_unlock_bh\b *( *\w* *)/d
s/\bcfs_spin_trylock\b/spin_trylock/g
/# *define *\bspin_trylock\b *( *\w* *) *\bspin_trylock\b *( *\w* *)/d
s/\bcfs_spin_is_locked\b/spin_is_locked/g
/# *define *\bspin_is_locked\b *( *\w* *) *\bspin_is_locked\b *( *\w* *)/d

s/\bcfs_spin_lock_irq\b/spin_lock_irq/g
/# *define *\bspin_lock_irq\b *( *\w* *) *\bspin_lock_irq\b *( *\w* *)/d
s/\bcfs_spin_unlock_irq\b/spin_unlock_irq/g
/# *define *\bspin_unlock_irq\b *( *\w* *) *\bspin_unlock_irq\b *( *\w* *)/d
s/\bcfs_read_lock_irqsave\b/read_lock_irqsave/g
/# *define *\bread_lock_irqsave\b *( *\w* *, *\w* *) *\bread_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_write_lock_irqsave\b/write_lock_irqsave/g
/# *define *\bwrite_lock_irqsave\b *( *\w* *, *\w* *) *\bwrite_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_write_unlock_irqrestore\b/write_unlock_irqrestore/g
/# *define *\bwrite_unlock_irqrestore\b *( *\w* *, *\w* *) *\bwrite_unlock_irqrestore\b *( *\w* *, *\w* *)/d
s/\bcfs_spin_lock_irqsave\b/spin_lock_irqsave/g
/# *define *\bspin_lock_irqsave\b *( *\w* *, *\w* *) *\bspin_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_spin_unlock_irqrestore\b/spin_unlock_irqrestore/g
/# *define *\bspin_unlock_irqrestore\b *( *\w* *, *\w* *) *\bspin_unlock_irqrestore\b *( *\w* *, *\w* *)/d
s/\bCFS_SPIN_LOCK_UNLOCKED\b/SPIN_LOCK_UNLOCKED/g
/# *define *\bSPIN_LOCK_UNLOCKED\b *\bSPIN_LOCK_UNLOCKED\b/d

# rw_semaphore
s/\bcfs_rw_semaphore_t\b/rw_semaphore_t/g
s/\bcfs_init_rwsem\b/init_rwsem/g
/# *define *\binit_rwsem\b *( *\w* *) *\binit_rwsem\b *( *\w* *)/d
s/\bcfs_down_read\b/down_read/g
/# *define *\bdown_read\b *( *\w* *) *\bdown_read\b *( *\w* *)/d
s/\bcfs_down_read_trylock\b/down_read_trylock/g
/# *define *\bdown_read_trylock\b *( *\w* *) *\bdown_read_trylock\b *( *\w* *)/d
s/\bcfs_up_read\b/up_read/g
/# *define *\bup_read\b *( *\w* *) *\bup_read\b *( *\w* *)/d
s/\bcfs_down_write\b/down_write/g
/# *define *\bdown_write\b *( *\w* *) *\bdown_write\b *( *\w* *)/d
s/\bcfs_down_write_trylock\b/down_write_trylock/g
/# *define *\bdown_write_trylock\b *( *\w* *) *\bdown_write_trylock\b *( *\w* *)/d
s/\bcfs_up_write\b/up_write/g
/# *define *\bup_write\b *( *\w* *) *\bup_write\b *( *\w* *)/d
s/\bcfs_fini_rwsem\b/fini_rwsem/g
s/\bCFS_DECLARE_RWSEM\b/DECLARE_RWSEM/g
/# *define *\bDECLARE_RWSEM\b *( *\w* *) *\bDECLARE_RWSEM\b *( *\w* *)/d

s/\bcfs_semaphore\b/semaphore/g
s/\bcfs_rw_semaphore\b/rw_semaphore/g
s/\bcfs_init_completion_module\b/init_completion_module/g
s/\bcfs_call_wait_handler\b/call_wait_handler/g
s/\bcfs_wait_handler_t\b/wait_handler_t/g
s/\bcfs_mt_completion_t\b/mt_completion_t/g
s/\bcfs_mt_init_completion\b/mt_init_completion/g
s/\bcfs_mt_wait_for_completion\b/mt_wait_for_completion/g
s/\bcfs_mt_complete\b/mt_complete/g
s/\bcfs_mt_fini_completion\b/mt_fini_completion/g
s/\bcfs_mt_atomic_t\b/mt_atomic_t/g
s/\bcfs_mt_atomic_read\b/mt_atomic_read/g
s/\bcfs_mt_atomic_set\b/mt_atomic_set/g
s/\bcfs_mt_atomic_dec_and_test\b/mt_atomic_dec_and_test/g
s/\bcfs_mt_atomic_inc\b/mt_atomic_inc/g
s/\bcfs_mt_atomic_dec\b/mt_atomic_dec/g
s/\bcfs_mt_atomic_add\b/mt_atomic_add/g
s/\bcfs_mt_atomic_sub\b/mt_atomic_sub/g

# rwlock
/typedef  *rwlock_t  *cfs_rwlock_t;/d
s/\bcfs_rwlock_t\b/rwlock_t/g
s/\bcfs_rwlock_init\b/rwlock_init/g
/# *define *\brwlock_init\b *( *\w* *) *\brwlock_init\b *( *\w* *)/d
s/\bcfs_read_lock\b/read_lock/g
/# *define *\bread_lock\b *( *\w* *) *\bread_lock\b *( *\w* *)/d
s/\bcfs_read_unlock\b/read_unlock/g
/# *define *\bread_unlock\b *( *\w* *) *\bread_unlock\b *( *\w* *)/d
s/\bcfs_read_unlock_irqrestore\b/read_unlock_irqrestore/g
#/# *define *\bread_unlock_irqrestore\b *( *\w* *) *\bread_unlock_irqrestore\b *( *\w* *)/d
/#define read_unlock_irqrestore(lock,flags) \\/{N;d}
s/\bcfs_write_lock\b/write_lock/g
/# *define *\bwrite_lock\b *( *\w* *) *\bwrite_lock\b *( *\w* *)/d
s/\bcfs_write_unlock\b/write_unlock/g
/# *define *\bwrite_unlock\b *( *\w* *) *\bwrite_unlock\b *( *\w* *)/d
s/\bcfs_write_lock_bh\b/write_lock_bh/g
/# *define *\bwrite_lock_bh\b *( *\w* *) *\bwrite_lock_bh\b *( *\w* *)/d
s/\bcfs_write_unlock_bh\b/write_unlock_bh/g
/# *define *\bwrite_unlock_bh\b *( *\w* *) *\bwrite_unlock_bh\b *( *\w* *)/d
s/\bCFS_RW_LOCK_UNLOCKED\b/RW_LOCK_UNLOCKED/g
/# *define *\bRW_LOCK_UNLOCKED\b  *\bRW_LOCK_UNLOCKED\b */d

# completion
s/\bcfs_completion_t\b/completion_t/g
s/\bCFS_DECLARE_COMPLETION\b/DECLARE_COMPLETION/g
/# *define *\bDECLARE_COMPLETION\b *( *\w* *) *\bDECLARE_COMPLETION\b *( *\w* *)/d
s/\bCFS_INIT_COMPLETION\b/INIT_COMPLETION/g
/# *define *\bINIT_COMPLETION\b *( *\w* *) *\bINIT_COMPLETION\b *( *\w* *)/d
s/\bCFS_COMPLETION_INITIALIZER\b/COMPLETION_INITIALIZER/g
/# *define *\bCOMPLETION_INITIALIZER\b *( *\w* *) *\bCOMPLETION_INITIALIZER\b *( *\w* *)/d
s/\bcfs_init_completion\b/init_completion/g
/# *define *\binit_completion\b *( *\w* *) *\binit_completion\b *( *\w* *)/d
s/\bcfs_complete\b/complete/g
/# *define *\bcomplete\b *( *\w* *) *\bcomplete\b *( *\w* *)/d
s/\bcfs_wait_for_completion\b/wait_for_completion/g
/# *define *\bwait_for_completion\b *( *\w* *) *\bwait_for_completion\b *( *\w* *)/d
s/\bcfs_wait_for_completion_interruptible\b/wait_for_completion_interruptible/g
/#define wait_for_completion_interruptible(c) \\/{N;d}
s/\bcfs_complete_and_exit\b/complete_and_exit/g
/# *define *\bcomplete_and_exit\b *( *\w* *, *\w* *) *\bcomplete_and_exit\b *( *\w* *, *\w* *)/d
s/\bcfs_fini_completion\b/fini_completion/g

# semaphore
s/\bcfs_semaphore_t\b/semaphore_t/g
s/\bCFS_DEFINE_SEMAPHORE\b/DEFINE_SEMAPHORE/g
/# *define *\bDEFINE_SEMAPHORE\b *( *\w* *) *\bDEFINE_SEMAPHORE\b *( *\w* *)/d
s/\bcfs_sema_init\b/sema_init/g
/# *define *\bsema_init\b *( *\w* *, *\w* *) *\bsema_init\b *( *\w* *, *\w* *)/d
s/\bcfs_up\b/up/g
/# *define *\bup\b *( *\w* *) *\bup\b *( *\w* *)/d
s/\bcfs_down\b/down/g
/# *define *\bdown\b *( *\w* *) *\bdown\b *( *\w* *)/d
s/\bcfs_down_interruptible\b/down_interruptible/g
/# *define *\bdown_interruptible\b *( *\w* *) *\bdown_interruptible\b *( *\w* *)/d
s/\bcfs_down_trylock\b/down_trylock/g
/# *define *\bdown_trylock\b *( *\w* *) *\bdown_trylock\b *( *\w* *)/d

# mutex
s/\bcfs_mutex_t\b/mutex_t/g
s/\bCFS_DEFINE_MUTEX\b/DEFINE_MUTEX/g
/# *define *\DEFINE_MUTEX\b *( *name *) *\bDEFINE_MUTEX\b *( *name *)/d
s/\bcfs_mutex_init\b/mutex_init/g
/# *define *\bmutex_init\b *( *\w* *) *\bmutex_init\b *( *\w* *)/d
s/\bcfs_mutex_lock\b/mutex_lock/g
/# *define *\bmutex_lock\b *( *\w* *) *\bmutex_lock\b *( *\w* *)/d
s/\bcfs_mutex_unlock\b/mutex_unlock/g
/# *define *\bmutex_unlock\b *( *\w* *) *\bmutex_unlock\b *( *\w* *)/d
s/\bcfs_mutex_lock_interruptible\b/mutex_lock_interruptible/g
/# *define *\bmutex_lock_interruptible\b *( *\w* *) *\bmutex_lock_interruptible\b *( *\w* *)/d
s/\bcfs_mutex_trylock\b/mutex_trylock/g
/# *define *\bmutex_trylock\b *( *\w* *) *\bmutex_trylock\b *( *\w* *)/d
s/\bcfs_mutex_is_locked\b/mutex_is_locked/g
/# *define *\bmutex_is_locked\b *( *\w* *) *\bmutex_is_locked\b *( *\w* *)/d
s/\bcfs_mutex_destroy\b/mutex_destroy/g
/# *define *\bmutex_destroy\b *( *\w* *) *\bmutex_destroy\b *( *\w* *)/d

# lock_kernel, unlock_kernel
s/\bcfs_lock_kernel\b/lock_kernel/g
/# *define *\block_kernel\b *( *) *\block_kernel\b *( *)/d
s/\bcfs_unlock_kernel\b/unlock_kernel/g
/# *define *\bunlock_kernel\b *( *) *\bunlock_kernel\b *( *)/d

# lockdep
s/\bcfs_lock_class_key\b/lock_class_key/g
s/\bcfs_lock_class_key_t\b/lock_class_key_t/g
s/\bcfs_lockdep_set_class\b/lockdep_set_class/g
s/\bcfs_lockdep_off\b/lockdep_off/g
s/\bcfs_lockdep_on\b/lockdep_on/g
/# *define *\blockdep_off\b *( *) *\blockdep_off\b *( *)/d
/# *define *\blockdep_on\b *( *) *\blockdep_on\b *( *)/d
/# *define *\blockdep_set_class\b *( *\w* *, *\w* *) *\blockdep_set_class\b *( *\w* *, *\w* *)/d

s/\bcfs_mutex_lock_nested\b/mutex_lock_nested/g
#/# *define *\bmutex_lock_nested\b *( *\w* *, *\w* *) *\bmutex_lock_nested\b *( *\w* *, *\w* *)/d
/#define mutex_lock_nested(mutex, subclass) \\/{N;d}
s/\bcfs_spin_lock_nested\b/spin_lock_nested/g
/# *define *\bspin_lock_nested\b *( *\w* *, *\w* *) *\bspin_lock_nested\b *( *\w* *, *\w* *)/d
s/\bcfs_down_read_nested\b/down_read_nested/g
/# *define *\bdown_read_nested\b *( *\w* *, *\w* *) *\bdown_read_nested\b *( *\w* *, *\w* *)/d
s/\bcfs_down_write_nested\b/down_write_nested/g
/# *define *\bdown_write_nested\b *( *\w* *, *\w* *) *\bdown_write_nested\b *( *\w* *, *\w* *)/d

###############################################################################
# bitops

s/\bcfs_test_bit\b/test_bit/g
/# *define *\btest_bit\b *( *\w* *, *\w* *) *\btest_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_set_bit\b/set_bit/g
/# *define *\bset_bit\b *( *\w* *, *\w* *) *\bset_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_clear_bit\b/clear_bit/g
/# *define *\bclear_bit\b *( *\w* *, *\w* *) *\bclear_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_test_and_set_bit\b/test_and_set_bit/g
/# *define *\btest_and_set_bit\b *( *\w* *, *\w* *) *\btest_and_set_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_test_and_clear_bit\b/test_and_clear_bit/g
/# *define *\btest_and_clear_bit\b *( *\w* *, *\w* *) *\btest_and_clear_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_first_bit\b/find_first_bit/g
/# *define *\bfind_first_bit\b *( *\w* *, *\w* *) *\bfind_first_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_first_zero_bit\b/find_first_zero_bit/g
/# *define *\bfind_first_zero_bit\b *( *\w* *, *\w* *) *\bfind_first_zero_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_next_bit\b/find_next_bit/g
/# *define *\bfind_next_bit\b *( *\w* *, *\w* *, *\w* *) *\bfind_next_bit\b *( *\w* *, *\w* *, *\w* *)/d
s/\bcfs_find_next_zero_bit\b/find_next_zero_bit/g
/#define find_next_zero_bit(addr, size, off) \\/{N;d}
s/\bcfs_ffz\b/ffz/g
/# *define *\bffz\b *( *\w* *) *\bffz\b *( *\w* *)/d
s/\bcfs_ffs\b/ffs/g
/# *define *\bffs\b *( *\w* *) *\bffs\b *( *\w* *)/d
s/\bcfs_fls\b/fls/g
/# *define *\bfls\b *( *\w* *) *\bfls\b *( *\w* *)/d


################################################################################
# file operations

s/\bcfs_file_t\b/file_t/g
s/\bcfs_dentry_t\b/dentry_t/g
s/\bcfs_dirent_t\b/dirent_t/g
s/\bcfs_kstatfs_t\b/kstatfs_t/g
s/\bcfs_filp_size\b/filp_size/g
s/\bcfs_filp_poff\b/filp_poff/g
s/\bcfs_filp_open\b/filp_open/g
/# *define *\bfilp_open\b *( *\w* *, *\w* *, *\w* *) *\bfilp_open\b *( *\w* *, *\w* *, *\w* *)/d
s/\bcfs_do_fsync\b/do_fsync/g
s/\bcfs_filp_close\b/filp_close/g
/# *define *\bfilp_close\b *( *\w* *, *\w* *) *\bfilp_close\b *( *\w* *, *\w* *)/d
s/\bcfs_filp_read\b/filp_read/g
s/\bcfs_filp_write\b/filp_write/g
s/\bcfs_filp_fsync\b/filp_fsync/g
s/\bcfs_get_file\b/get_file/g
/# *define *\bget_file\b *( *\w* *) *\bget_file\b *( *\w* *)/d
s/\bcfs_get_fd\b/fget/g
/# *define *\bfget\b *( *\w* *) *\bfget\b *( *\w* *)/d
s/\bcfs_put_file\b/fput/g
/# *define *\bfput\b *( *\w* *) *\bfput\b *( *\w* *)/d
s/\bcfs_file_count\b/file_count/g
/# *define *\bfile_count\b *( *\w* *) *\bfile_count\b *( *\w* *)/d
s/\bCFS_INT_LIMIT\b/INT_LIMIT/g
s/\bCFS_OFFSET_MAX\b/OFFSET_MAX/g
s/\bcfs_flock_t\b/flock_t/g
s/\bcfs_flock_type\b/flock_type/g
s/\bcfs_flock_set_type\b/flock_set_type/g
s/\bcfs_flock_pid\b/flock_pid/g
s/\bcfs_flock_set_pid\b/flock_set_pid/g
s/\bcfs_flock_start\b/flock_start/g
s/\bcfs_flock_set_start\b/flock_set_start/g
s/\bcfs_flock_end\b/flock_end/g
s/\bcfs_flock_set_end\b/flock_set_end/g
s/\bcfs_user_write\b/user_write/g
s/\bCFS_IFSHIFT\b/IFSHIFT/g
s/\bCFS_IFTODT\b/IFTODT/g
s/\bCFS_DTTOIF\b/DTTOIF/g


#s/\bcfs_\b//g
#/# *define *\b\b *\b\b/d
#/# *define *\b\b *( *) *\b\b *( *)/d
#/# *define *\b\b *( *\w* *) *\b\b *( *\w* *)/d
#/# *define *\b\b *( *\w* *, *\w* *) *\b\b *( *\w* *, *\w* *)/d
#/# *define *\b\b *( *\w* *, *\w* *, *\w* *) *\b\b *( *\w* *, *\w* *, *\w* *)/d
