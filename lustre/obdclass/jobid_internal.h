#include <libcfs/lucache.h>

extern struct upcall_cache_ops jobid_cache_upcall_cache_ops;
extern char obd_jobid_node[];
extern struct upcall_cache *obd_jobid_upcall;

struct jobid_cache_entry *jobid_cache_get(__u64 pid);
void jobid_cache_put(struct jobid_cache_entry *jobid);
void jobid_flush_cache(__u64 pid);

