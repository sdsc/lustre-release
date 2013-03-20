/**
 * Create / destroy intent log objects.  IL objects reside in the
 * shardroot dataset.  These calls will also perform add / remove operations
 * in the shardroot device.
 */
static int (*vosd_shardroot_il_create)(struct vosd_shardroot_dev *vsr,
				       daos_epoch_t epoch);

static int (*vosd_shardroot_il_destroy)(struct vosd_shardroot_dev *vsr,
					daos_epoch_t epoch);
struct vosd_intent_log;

struct vosd_shardroot_dev_operations {
	/**
	 * Lookup a snapshot in the shardroot and return an initialized
	 * shard dev if it exists.  Presumably, this will be called via
	 * osd_mount().
	 */
	struct vosd_shard_dev *
	(*vsrdo_snap_get)(struct vosd_shardroot_dev *vsr,
			  daos_epoch_t epoch);
	/**
	 * Signify that shard device operations have completed.
	 * Called at osd_umount().
	 */
	int (*vsrdo_snap_put)(struct vosd_shardroot_dev *vsr,
			      struct vosd_shard_dev *vsd);
	/**
	 * Add / remove a snapshot to the shardroot device.  Add / remove
	 * operations must be applied in the same TXG as the shard dev's
	 * dataset snapshot.  Snapshots may reside in the shardroot device
	 * as files or  directories with a pointer to the snapshot dataset.
	 * This will ease admistrative use versus the use of an OI.
	 */
	int (*vsrdo_snap_insert)(struct vosd_shardroot_dev *vsr,
				 const struct vosd_shard_dev *vsd);

	int (*vsrdo_snap_remove)(struct vosd_shardroot_dev *vsr,
				 const struct vosd_shard_dev *vsd);
	/**
	 * Lookup an intent long in the shardroot and apply the proper
	 * referencing.
	 */
	struct vosd_intent_log *
	(*vsrdo_il_get)(struct vosd_shardroot_dev *vsr,
			daos_epoch_t epoch);

	int (*vsrdo_il_put)(struct vosd_shardroot_dev *vsr,
			    struct vosd_intent_log *vil);
};

struct vosd_shardroot_dev {
	const struct vosd_shardroot_dev_operations	*vrd_ops;
	/**
	 * lookup per-epoch intent logs and shard dev snapshots.  IL's and
	 * snaps will be maintained in a human readable directory hierarchy
	 */
	const struct vosd_shardroot_lookup_operations	*vrd_lookup_ops;
	/**
	 * pointers to the datasets which are started by default
	 */
	struct vosd_shard_dev				*vrd_hce;
	struct vosd_shard_dev				*vrd_stage;
	/**
	 * track snapshots which have been mounted as 'devices'
	 */
	cfs_list_t					vrd_snap_list;
	/**
	 * track intent logs which live in this shardroot dev
	 */
	cfs_list_t					vrd_il_list;
	cfs_mutex_t					vrd_lock;
	/**
	 * point to contents of shardroot 'superblock'
	 */
	struct vosd_shardroot_ondisk			*vrd_vso;
	/* XXX zfs_dataset pointer */
};

struct vosd_shardroot_ondisk {
	daos_epoch_t vso_hce;
	/* XXX probably need additional items such as the container layout. */
};

/**
 * Calls for dealing with shardroot creation and destruction
 */
struct vosd_shardroot_dev *vosd_shardroot_dev_create(daos_shard_seq_t seq);
int vosd_shardroot_dev_destroy(daos_shard_seq_t seq);


struct vosd_shard_dev_operations {
	int    (*vsdo_commit)(struct vosd_shard_dev *vsd, daos_epoch_t epoch,
			      int flags);
	int    (*vsdo_rollback)(struct vosd_shard_dev *vsd,
				daos_epoch_t epoch);
};

struct vosd_shard_dev {
	struct vosd_shardroot_dev              *vsd_root;
	const struct vosd_shard_dev_operations *vsd_ops;
	cfs_list_t                              vsd_linkage;
	int                                     vsd_state;
	/* XXX zfs_dataset pointer */
};

/**
 * Called by vsd_commit() if the flag is set to 'flush'.
 */
static int vosd_shard_dev_flush(struct vosd_shard_dev *vs,
				daos_epoch_t epoch);
/**
 * Called by vsd_commit() if the flag is set to 'sync'.
 */
static int vosd_shard_dev_snapshot(struct vosd_shard_dev *vs);

/**
 * Initiates the replay of intents into the staging dataset.
 * XXX This process could take a very long time so the method of execution
 *     and completion notificaion must be determined.  It may be the case
 *     that 10^3 or even 10^4 shards may be flattening at any one time so one
 *     thread per shard may not be practical.
 */
static int vosd_shard_dev_il_flatten(struct vosd_shard_dev *vs,
				     daos_epoch_t epoch);

struct vosd_object {
	struct lu_object                   vo_lu;
	const struct vo_object_operations *vo_object_ops;
	const struct vo_index_operations  *vo_index_ops;
	const struct vo_body_operations   *vo_body_ops;
}

/**
 * XXX Obtaining the IL structure for a given DAOS object:
 * Johann has suggested that we access the IL for a given object via
 * dio_lookup() where the FID == seq.lowestbitsoffid.reserved_fid_for_IL.
 * dio_lookup() would then call into the shardroot dev to retrieve the IL
 * information (if it wasn't already obtained).
 * With this method how does a vosd_object call into
 * vosd_shardroot_dev_operations?  Or, in other words, how does a vosd object
 * reference a shard dev and/or shardroot dev?  From my reading of the code,
 * the current APIs don't work in this manner.
 */

