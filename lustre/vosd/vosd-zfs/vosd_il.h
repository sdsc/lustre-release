/*
 * on-disk structure for vosd intent log entry
 * XXX Must be mindful of ZFS's dynamic block size mgmt when dealing with IL's.
 */
struct vosd_il_entry {
	/* write, punch, kv create / remove,etc. */
	enum vosd_il_op_type	vie_op;
	/* DAOS object */
	daos_objid_t vie_obj;
	/* location of write or punch */
	uint64_t		vie_off;
	/* size of write */
	size_t		  vie_len;
	/* additional log data consumed by entry */
	size_t		  vie_il_data_len;
	union {
		/* copy of zfs blkptr contents (zero copy) */
		zfs_blkptr_t	  vie_zfs_blkptr;
		/**
		 * XXX as an alternative to the above, we may just point to
		 *    a block region in a ZFS object
		 * size_t	  vie_zc_block;
		*/
		/* small embedded fragments or kv contents */
		char		  vie_data[];
	} u;
};

enum vosd_il_op_type {
	VOSD_IL_OP_WRITE   = 1 << 0,
	VOSD_IL_OP_ZCWRITE = 1 << 1,
	VOSD_IL_OP_PUNCH   = 1 << 2,
	VOSD_IL_OP_KV_ADD  = 1 << 3,
	VOSD_IL_OP_KV_DEL  = 1 << 4,
};

struct vosd_il_entry_handle {
	struct vosd_intent_log *vieh_il;
	struct vosd_il_entry   *vieh_entry;
	cfs_list_t              vieh_linkage;
	int                     vieh_state;
};

enum vosd_il_entry_state {
	VOSD_IL_ENTRY_NEW          = 1 << 0,
	VOSD_IL_ENTRY_WRITE_RDY    = 1 << 1,
	VOSD_IL_ENTRY_WRITE_DONE   = 1 << 2,
	VOSD_IL_ENTRY_FLATTEN_RDY  = 1 << 3,
	VOSD_IL_ENTRY_FLATTEN_DONE = 1 << 4,
};

/**
  Note on IL flattening - It's highly doubtful that a single thread
  will be suitable for flattening activities of a given IL because Large
  IL's may tie up a thread for very long time.  Additionally, this single
  thread flattening will affect flattening QoS when many IL's are involved,
  especially when large IL's exist admidst smaller ones.   The interfaces
  below intend to allow for stateful 'get' and 'put' of IL entries so that
  a single set of 'flattening' threads may operate on a IL's in a piecemeal
  manner.
*/
struct vosd_il_operations {
	/**
	 * Obtain the next available log entry in the IL.  Call with take
	 * the IL lock and update vil_next_entry based on 'len'.  This also
	 * increments vil_refcount.  This should allow backing allocations
	 * to be made from the arc cache and for the vosd_il_entry_handle
	 * buffer to act as a sink. This interface should also allow for
	 * multi-threaded flattening.
	 */
	struct vosd_il_entry_handle *
	(*vilo_entry_get)(struct vosd_intent_log *vi, enum vosd_il_op_type op,
			  size_t len);
	/**
	 * Called  when user is done with the entry.  Decrements ref count
	 * and removes entry from outstanding list.
	 */
	int (*vilo_entry_put)(struct vosd_intent_log *vi,
			      struct vosd_il_entry_handle *veh);
};

struct vosd_intent_log {
	const struct vosd_il_operations	*vil_ops;
	/* cursor used for logging and flattening */
	struct vosd_il_entry		*vil_next_entry;
	/* count accessors */
	cfs_atomic_t			vil_refcount;
	cfs_mutex_t			vil_lock;
	/* list of outstanding entries */
	cfs_list_t			vil_outstanding_list;
	cfs_list_t			vil_linkage;
	uint64_t			vil_num_intents;
	uint64_t			vil_num_zc_intents;
	int				vil_flags;
	/* XXX zfs_file_obj pointer should be added */
};

enum vosd_intent_log_flags {
	VOSD_IL_WRITE_ACCEPT   = 1 << 0,
	VOSD_IL_FLATTEN_INPROG = 1 << 1,
	VOSD_IL_FLATTEN_DONE   = 1 << 2,
};
