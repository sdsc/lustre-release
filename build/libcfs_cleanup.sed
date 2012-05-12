#!/bin/sed -f

# Script to cleanup libcfs macros, it runs against the tree at build time.
# Migrate libcfs to emulate Linux kernel APIs.
# http://jira.whamcloud.com/browse/LU-1346

# remove extra blank line
# /^$/{N;/^\n$/D}

################################################################################
# lock - spinlock, rw_semaphore, rwlock, completion, semaphore, mutex
#      - lock_kernel, unlock_kernel, lockdep

# spinlok
/typedef  *spinlock_t  *cfs_spinlock_t;/d
s/\bcfs_spinlock_t\b/spinlock_t/g
s/\bcfs_spin_lock_init\b/spin_lock_init/g
/#[ \t]*define[ \t]*\bspin_lock_init\b *( *\w* *)[ \t]*\bspin_lock_init\b *( *\w* *)/d
s/\bcfs_spin_lock\b/spin_lock/g
/#[ \t]*define[ \t]*\bspin_lock\b *( *\w* *)[ \t]*\bspin_lock\b *( *\w* *)/d
s/\bcfs_spin_lock_bh\b/spin_lock_bh/g
/#[ \t]*define[ \t]*\bspin_lock_bh\b *( *\w* *)[ \t]*\bspin_lock_bh\b *( *\w* *)/d
s/\bcfs_spin_lock_bh_init\b/spin_lock_bh_init/g
/#[ \t]*define[ \t]*\bspin_lock_bh_init\b *( *\w* *)[ \t]*\bspin_lock_bh_init\b *( *\w* *)/d
s/\bcfs_spin_unlock\b/spin_unlock/g
/#[ \t]*define[ \t]*\bspin_unlock\b *( *\w* *)[ \t]*\bspin_unlock\b *( *\w* *)/d
s/\bcfs_spin_unlock_bh\b/spin_unlock_bh/g
/#[ \t]*define[ \t]*\bspin_unlock_bh\b *( *\w* *)[ \t]*\bspin_unlock_bh\b *( *\w* *)/d
s/\bcfs_spin_trylock\b/spin_trylock/g
/#[ \t]*define[ \t]*\bspin_trylock\b *( *\w* *)[ \t]*\bspin_trylock\b *( *\w* *)/d
s/\bcfs_spin_is_locked\b/spin_is_locked/g
/#[ \t]*define[ \t]*\bspin_is_locked\b *( *\w* *)[ \t]*\bspin_is_locked\b *( *\w* *)/d

s/\bcfs_spin_lock_irq\b/spin_lock_irq/g
/#[ \t]*define[ \t]*\bspin_lock_irq\b *( *\w* *)[ \t]*\bspin_lock_irq\b *( *\w* *)/d
s/\bcfs_spin_unlock_irq\b/spin_unlock_irq/g
/#[ \t]*define[ \t]*\bspin_unlock_irq\b *( *\w* *)[ \t]*\bspin_unlock_irq\b *( *\w* *)/d
s/\bcfs_read_lock_irqsave\b/read_lock_irqsave/g
/#[ \t]*define[ \t]*\bread_lock_irqsave\b *( *\w* *, *\w* *)[ \t]*\bread_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_write_lock_irqsave\b/write_lock_irqsave/g
/#[ \t]*define[ \t]*\bwrite_lock_irqsave\b *( *\w* *, *\w* *)[ \t]*\bwrite_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_write_unlock_irqrestore\b/write_unlock_irqrestore/g
/#[ \t]*define[ \t]*\bwrite_unlock_irqrestore\b *( *\w* *, *\w* *)[ \t]*\bwrite_unlock_irqrestore\b *( *\w* *, *\w* *)/d
s/\bcfs_spin_lock_irqsave\b/spin_lock_irqsave/g
/#[ \t]*define[ \t]*\bspin_lock_irqsave\b *( *\w* *, *\w* *)[ \t]*\bspin_lock_irqsave\b *( *\w* *, *\w* *)/d
s/\bcfs_spin_unlock_irqrestore\b/spin_unlock_irqrestore/g
/#[ \t]*define[ \t]*\bspin_unlock_irqrestore\b *( *\w* *, *\w* *)[ \t]*\bspin_unlock_irqrestore\b *( *\w* *, *\w* *)/d
s/\bCFS_SPIN_LOCK_UNLOCKED\b/SPIN_LOCK_UNLOCKED/g
/#[ \t]*define[ \t]*\bSPIN_LOCK_UNLOCKED\b[ \t]*\bSPIN_LOCK_UNLOCKED\b/d

# rw_semaphore
s/\bcfs_rw_semaphore_t\b/rw_semaphore_t/g
s/\bcfs_init_rwsem\b/init_rwsem/g
/#[ \t]*define[ \t]*\binit_rwsem\b *( *\w* *)[ \t]*\binit_rwsem\b *( *\w* *)/d
s/\bcfs_down_read\b/down_read/g
/#[ \t]*define[ \t]*\bdown_read\b *( *\w* *)[ \t]*\bdown_read\b *( *\w* *)/d
s/\bcfs_down_read_trylock\b/down_read_trylock/g
/#[ \t]*define[ \t]*\bdown_read_trylock\b *( *\w* *)[ \t]*\bdown_read_trylock\b *( *\w* *)/d
s/\bcfs_up_read\b/up_read/g
/#[ \t]*define[ \t]*\bup_read\b *( *\w* *)[ \t]*\bup_read\b *( *\w* *)/d
s/\bcfs_down_write\b/down_write/g
/#[ \t]*define[ \t]*\bdown_write\b *( *\w* *)[ \t]*\bdown_write\b *( *\w* *)/d
s/\bcfs_down_write_trylock\b/down_write_trylock/g
/#[ \t]*define[ \t]*\bdown_write_trylock\b *( *\w* *)[ \t]*\bdown_write_trylock\b *( *\w* *)/d
s/\bcfs_up_write\b/up_write/g
/#[ \t]*define[ \t]*\bup_write\b *( *\w* *)[ \t]*\bup_write\b *( *\w* *)/d
s/\bcfs_fini_rwsem\b/fini_rwsem/g
s/\bCFS_DECLARE_RWSEM\b/DECLARE_RWSEM/g
/#[ \t]*define[ \t]*\bDECLARE_RWSEM\b *( *\w* *)[ \t]*\bDECLARE_RWSEM\b *( *\w* *)/d

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
/#[ \t]*define[ \t]*\brwlock_init\b *( *\w* *)[ \t]*\brwlock_init\b *( *\w* *)/d
s/\bcfs_read_lock\b/read_lock/g
/#[ \t]*define[ \t]*\bread_lock\b *( *\w* *)[ \t]*\bread_lock\b *( *\w* *)/d
s/\bcfs_read_unlock\b/read_unlock/g
/#[ \t]*define[ \t]*\bread_unlock\b *( *\w* *)[ \t]*\bread_unlock\b *( *\w* *)/d
s/\bcfs_read_unlock_irqrestore\b/read_unlock_irqrestore/g
#/#[ \t]*define[ \t]*\bread_unlock_irqrestore\b *( *\w* *)[ \t]*\bread_unlock_irqrestore\b *( *\w* *)/d
/#define read_unlock_irqrestore(lock,flags) \\/{N;d}
s/\bcfs_write_lock\b/write_lock/g
/#[ \t]*define[ \t]*\bwrite_lock\b *( *\w* *)[ \t]*\bwrite_lock\b *( *\w* *)/d
s/\bcfs_write_unlock\b/write_unlock/g
/#[ \t]*define[ \t]*\bwrite_unlock\b *( *\w* *)[ \t]*\bwrite_unlock\b *( *\w* *)/d
s/\bcfs_write_lock_bh\b/write_lock_bh/g
/#[ \t]*define[ \t]*\bwrite_lock_bh\b *( *\w* *)[ \t]*\bwrite_lock_bh\b *( *\w* *)/d
s/\bcfs_write_unlock_bh\b/write_unlock_bh/g
/#[ \t]*define[ \t]*\bwrite_unlock_bh\b *( *\w* *)[ \t]*\bwrite_unlock_bh\b *( *\w* *)/d
s/\bCFS_RW_LOCK_UNLOCKED\b/RW_LOCK_UNLOCKED/g
/#[ \t]*define[ \t]*\bRW_LOCK_UNLOCKED\b  *\bRW_LOCK_UNLOCKED\b */d

# completion
s/\bcfs_completion_t\b/completion_t/g
s/\bCFS_DECLARE_COMPLETION\b/DECLARE_COMPLETION/g
/#[ \t]*define[ \t]*\bDECLARE_COMPLETION\b *( *\w* *)[ \t]*\bDECLARE_COMPLETION\b *( *\w* *)/d
s/\bCFS_INIT_COMPLETION\b/INIT_COMPLETION/g
/#[ \t]*define[ \t]*\bINIT_COMPLETION\b *( *\w* *)[ \t]*\bINIT_COMPLETION\b *( *\w* *)/d
s/\bCFS_COMPLETION_INITIALIZER\b/COMPLETION_INITIALIZER/g
/#[ \t]*define[ \t]*\bCOMPLETION_INITIALIZER\b *( *\w* *)[ \t]*\bCOMPLETION_INITIALIZER\b *( *\w* *)/d
s/\bcfs_init_completion\b/init_completion/g
/#[ \t]*define[ \t]*\binit_completion\b *( *\w* *)[ \t]*\binit_completion\b *( *\w* *)/d
s/\bcfs_complete\b/complete/g
/#[ \t]*define[ \t]*\bcomplete\b *( *\w* *)[ \t]*\bcomplete\b *( *\w* *)/d
s/\bcfs_wait_for_completion\b/wait_for_completion/g
/#[ \t]*define[ \t]*\bwait_for_completion\b *( *\w* *)[ \t]*\bwait_for_completion\b *( *\w* *)/d
s/\bcfs_wait_for_completion_interruptible\b/wait_for_completion_interruptible/g
/#define wait_for_completion_interruptible(c) \\/{N;d}
s/\bcfs_complete_and_exit\b/complete_and_exit/g
/#[ \t]*define[ \t]*\bcomplete_and_exit\b *( *\w* *, *\w* *)[ \t]*\bcomplete_and_exit\b *( *\w* *, *\w* *)/d
s/\bcfs_fini_completion\b/fini_completion/g

# semaphore
s/\bcfs_semaphore_t\b/semaphore_t/g
s/\bCFS_DEFINE_SEMAPHORE\b/DEFINE_SEMAPHORE/g
/#[ \t]*define[ \t]*\bDEFINE_SEMAPHORE\b *( *\w* *)[ \t]*\bDEFINE_SEMAPHORE\b *( *\w* *)/d
s/\bcfs_sema_init\b/sema_init/g
/#[ \t]*define[ \t]*\bsema_init\b *( *\w* *, *\w* *)[ \t]*\bsema_init\b *( *\w* *, *\w* *)/d
s/\bcfs_up\b/up/g
/#[ \t]*define[ \t]*\bup\b *( *\w* *)[ \t]*\bup\b *( *\w* *)/d
s/\bcfs_down\b/down/g
/#[ \t]*define[ \t]*\bdown\b *( *\w* *)[ \t]*\bdown\b *( *\w* *)/d
s/\bcfs_down_interruptible\b/down_interruptible/g
/#[ \t]*define[ \t]*\bdown_interruptible\b *( *\w* *)[ \t]*\bdown_interruptible\b *( *\w* *)/d
s/\bcfs_down_trylock\b/down_trylock/g
/#[ \t]*define[ \t]*\bdown_trylock\b *( *\w* *)[ \t]*\bdown_trylock\b *( *\w* *)/d

# mutex
s/\bcfs_mutex_t\b/mutex_t/g
s/\bCFS_DEFINE_MUTEX\b/DEFINE_MUTEX/g
/#[ \t]*define[ \t]*\DEFINE_MUTEX\b *( *name *)[ \t]*\bDEFINE_MUTEX\b *( *name *)/d
s/\bcfs_mutex_init\b/mutex_init/g
/#[ \t]*define[ \t]*\bmutex_init\b *( *\w* *)[ \t]*\bmutex_init\b *( *\w* *)/d
s/\bcfs_mutex_lock\b/mutex_lock/g
/#[ \t]*define[ \t]*\bmutex_lock\b *( *\w* *)[ \t]*\bmutex_lock\b *( *\w* *)/d
s/\bcfs_mutex_unlock\b/mutex_unlock/g
/#[ \t]*define[ \t]*\bmutex_unlock\b *( *\w* *)[ \t]*\bmutex_unlock\b *( *\w* *)/d
s/\bcfs_mutex_lock_interruptible\b/mutex_lock_interruptible/g
/#[ \t]*define[ \t]*\bmutex_lock_interruptible\b *( *\w* *)[ \t]*\bmutex_lock_interruptible\b *( *\w* *)/d
s/\bcfs_mutex_trylock\b/mutex_trylock/g
/#[ \t]*define[ \t]*\bmutex_trylock\b *( *\w* *)[ \t]*\bmutex_trylock\b *( *\w* *)/d
s/\bcfs_mutex_is_locked\b/mutex_is_locked/g
/#[ \t]*define[ \t]*\bmutex_is_locked\b *( *\w* *)[ \t]*\bmutex_is_locked\b *( *\w* *)/d
s/\bcfs_mutex_destroy\b/mutex_destroy/g
/#[ \t]*define[ \t]*\bmutex_destroy\b *( *\w* *)[ \t]*\bmutex_destroy\b *( *\w* *)/d

# lock_kernel, unlock_kernel
# s/\bcfs_lock_kernel\b/lock_kernel/g
# /#[ \t]*define[ \t]*\block_kernel\b *( *)[ \t]*\block_kernel\b *( *)/d
# s/\bcfs_unlock_kernel\b/unlock_kernel/g
# /#[ \t]*define[ \t]*\bunlock_kernel\b *( *)[ \t]*\bunlock_kernel\b *( *)/d

# lockdep
s/\bcfs_lock_class_key\b/lock_class_key/g
s/\bcfs_lock_class_key_t\b/lock_class_key_t/g
s/\bcfs_lockdep_set_class\b/lockdep_set_class/g
s/\bcfs_lockdep_off\b/lockdep_off/g
s/\bcfs_lockdep_on\b/lockdep_on/g
/#[ \t]*define[ \t]*\blockdep_off\b *( *)[ \t]*\blockdep_off\b *( *)/d
/#[ \t]*define[ \t]*\blockdep_on\b *( *)[ \t]*\blockdep_on\b *( *)/d
/#[ \t]*define[ \t]*\blockdep_set_class\b *( *\w* *, *\w* *)[ \t]*\blockdep_set_class\b *( *\w* *, *\w* *)/d

s/\bcfs_mutex_lock_nested\b/mutex_lock_nested/g
#/#[ \t]*define[ \t]*\bmutex_lock_nested\b *( *\w* *, *\w* *)[ \t]*\bmutex_lock_nested\b *( *\w* *, *\w* *)/d
/#define mutex_lock_nested(mutex, subclass) \\/{N;d}
s/\bcfs_spin_lock_nested\b/spin_lock_nested/g
/#[ \t]*define[ \t]*\bspin_lock_nested\b *( *\w* *, *\w* *)[ \t]*\bspin_lock_nested\b *( *\w* *, *\w* *)/d
s/\bcfs_down_read_nested\b/down_read_nested/g
/#[ \t]*define[ \t]*\bdown_read_nested\b *( *\w* *, *\w* *)[ \t]*\bdown_read_nested\b *( *\w* *, *\w* *)/d
s/\bcfs_down_write_nested\b/down_write_nested/g
/#[ \t]*define[ \t]*\bdown_write_nested\b *( *\w* *, *\w* *)[ \t]*\bdown_write_nested\b *( *\w* *, *\w* *)/d

###############################################################################
# bitops

s/\bcfs_test_bit\b/test_bit/g
/#[ \t]*define[ \t]*\btest_bit\b *( *\w* *, *\w* *)[ \t]*\btest_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_set_bit\b/set_bit/g
/#[ \t]*define[ \t]*\bset_bit\b *( *\w* *, *\w* *)[ \t]*\bset_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_clear_bit\b/clear_bit/g
/#[ \t]*define[ \t]*\bclear_bit\b *( *\w* *, *\w* *)[ \t]*\bclear_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_test_and_set_bit\b/test_and_set_bit/g
/#[ \t]*define[ \t]*\btest_and_set_bit\b *( *\w* *, *\w* *)[ \t]*\btest_and_set_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_test_and_clear_bit\b/test_and_clear_bit/g
/#[ \t]*define[ \t]*\btest_and_clear_bit\b *( *\w* *, *\w* *)[ \t]*\btest_and_clear_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_first_bit\b/find_first_bit/g
/#[ \t]*define[ \t]*\bfind_first_bit\b *( *\w* *, *\w* *)[ \t]*\bfind_first_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_first_zero_bit\b/find_first_zero_bit/g
/#[ \t]*define[ \t]*\bfind_first_zero_bit\b *( *\w* *, *\w* *)[ \t]*\bfind_first_zero_bit\b *( *\w* *, *\w* *)/d
s/\bcfs_find_next_bit\b/find_next_bit/g
/#[ \t]*define[ \t]*\bfind_next_bit\b *( *\w* *, *\w* *, *\w* *)[ \t]*\bfind_next_bit\b *( *\w* *, *\w* *, *\w* *)/d
s/\bcfs_find_next_zero_bit\b/find_next_zero_bit/g
/#define find_next_zero_bit(addr, size, off) \\/{N;d}
s/\bcfs_ffz\b/ffz/g
/#[ \t]*define[ \t]*\bffz\b *( *\w* *)[ \t]*\bffz\b *( *\w* *)/d
s/\bcfs_ffs\b/ffs/g
/#[ \t]*define[ \t]*\bffs\b *( *\w* *)[ \t]*\bffs\b *( *\w* *)/d
s/\bcfs_fls\b/fls/g
/#[ \t]*define[ \t]*\bfls\b *( *\w* *)[ \t]*\bfls\b *( *\w* *)/d

################################################################################
# file operations

s/\bcfs_file_t\b/file_t/g
s/\bcfs_dentry_t\b/dentry_t/g
s/\bcfs_dirent_t\b/dirent_t/g
s/\bcfs_kstatfs_t\b/kstatfs_t/g
s/\bcfs_filp_size\b/filp_size/g
s/\bcfs_filp_poff\b/filp_poff/g
s/\bcfs_filp_open\b/filp_open/g
/#[ \t]*define[ \t]*\bfilp_open\b *( *\w* *, *\w* *, *\w* *)[ \t]*\bfilp_open\b *( *\w* *, *\w* *, *\w* *)/d
s/\bcfs_do_fsync\b/do_fsync/g
s/\bcfs_filp_close\b/filp_close/g
/#[ \t]*define[ \t]*\bfilp_close\b *( *\w* *, *\w* *)[ \t]*\bfilp_close\b *( *\w* *, *\w* *)/d
s/\bcfs_filp_read\b/filp_read/g
s/\bcfs_filp_write\b/filp_write/g
s/\bcfs_filp_fsync\b/filp_fsync/g
s/\bcfs_get_file\b/get_file/g
/#[ \t]*define[ \t]*\bget_file\b *( *\w* *)[ \t]*\bget_file\b *( *\w* *)/d
s/\bcfs_get_fd\b/fget/g
/#[ \t]*define[ \t]*\bfget\b *( *\w* *)[ \t]*\bfget\b *( *\w* *)/d
s/\bcfs_put_file\b/fput/g
/#[ \t]*define[ \t]*\bfput\b *( *\w* *)[ \t]*\bfput\b *( *\w* *)/d
s/\bcfs_file_count\b/file_count/g
/#[ \t]*define[ \t]*\bfile_count\b *( *\w* *)[ \t]*\bfile_count\b *( *\w* *)/d
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
#s/\bCFS_\b//g
#/typedef[ \t]*\b\b[ \t]*\b\b/d
#/#[ \t]*define[ \t]*\b\b[ \t]*\b\b/d
#/#[ \t]*define[ \t]*\b\b *( *)[ \t]*\b\b *( *)/d
#/#[ \t]*define[ \t]*\b\b *( *\w* *)[ \t]*\b\b *( *\w* *)/d
#/#[ \t]*define[ \t]*\b\b *( *\w* *, *\w* *)[ \t]*\b\b *( *\w* *, *\w* *)/d
#/#[ \t]*define[ \t]*\b\b *( *\w* *, *\w* *, *\w* *)[ \t]*\b\b *( *\w* *, *\w* *, *\w* *)/d
