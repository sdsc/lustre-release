/*
 * SPECIAL LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of Contract No. B599860,
 * and the terms of the LGPL License.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */
/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * (C) Copyright 2012, 2013 Intel Corporation
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 or (at your discretion) any later version.
 * (LGPL) version 2.1 accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * LGPL HEADER END
 */
/*
 * This file is part of lustre/DAOS
 *
 * lustre/include/daos/daos_api.h
 *
 * Author: Liang Zhen  <liang.zhen@intel.com>
 */
#ifndef __DAOS_API_H__
#define __DAOS_API_H__

#include <daos/daos_types.h>

#ifndef __KERNEL__

/***********************************************************************
 * Event-Queue (EQ) and Event
 *
 * EQ is a queue that contains events inside.
 * All DAOS APIs are asynchronous, events occur on completion of DAOS APIs.
 * While calling DAOS API, user should pre-alloate event and pass it
 * into function, function will return immediately but doesn't mean it
 * has completed, i.e: I/O might still be in-flight, the only way that
 * user can know completion of operation is getting the event back by
 * calling daos_eq_poll().
 *
 * NB: if NULL is passed into DAOS API as event, function will be
 *     synchronous.
 ***********************************************************************/

/**
 * create an Event Queue
 *
 * \param eq [OUT]	returned EQ handle
 *
 * \return		zero on success, negative value if error
 */
int
daos_eq_create(daos_handle_t *eqh);

/**
 * Destroy an Event Queue, it wait -EBUSY if EQ is not empty.
 *
 * \param eqh [IN]	EQ to finalize
 * \param ev [IN]	pointer to completion event
 *
 * \return		zero on success, EBUSY if there's any inflight event
 */
int
daos_eq_destroy(daos_handle_t eqh);

/**
 * Retrieve completion events from an EQ
 *
 * \param eqh [IN]	EQ handle
 * \param wait_inf [IN]	wait only if there's inflight event
 * \param timeout [IN]	how long is caller going to wait (micro-second)
 * 			if \a timeout > 0,
 * 			it can also be DAOS_EQ_NOWAIT, DAOS_EQ_WAIT
 * \param eventn [IN]	size of \a events array, returned number of events
 * 			should always be less than or equal to \a eventn
 * \param events [OUT]	pointer to returned events array
 *
 * \return		>= 0	returned number of events
 * 			< 0	negative value if error
 */
int
daos_eq_poll(daos_handle_t eqh, int wait_inf,
	     int64_t timeout, int eventn, daos_event_t **events);

/**
 * Query how many outstanding events in EQ, if \a events is not NULL,
 * these events will be stored into it.
 * Events returned by query are still owned by DAOS, it's not allowed to
 * finalize or free events returned by this function, but it's allowed
 * to call daos_event_abort() to abort inflight operation.
 * Also, status of returned event could be still in changing, for example,
 * returned "inflight" event can be turned to "completed" before acessing.
 * It's user's responsibility to guarantee that returned events would be
 * freed by polling process.
 *
 * \param eqh [IN]	EQ handle
 * \param mode [IN]	query mode
 * \param eventn [IN]	size of \a events array
 * \param events [OUT]	pointer to returned events array
 * \return		>= 0	returned number of events
 * 			 < 0	negative value if error
 */
int
daos_eq_query(daos_handle_t eqh, daos_ev_query_t query,
	      unsigned int eventn, daos_event_t **events);

/**
 * Initialize a new event for \a eq
 *
 * \param ev [IN]	event to initialize
 * \param eqh [IN]	where the event to be queued on, it's ignored if
 * 			\a parent is specified
 * \param parent [IN]	"parent" event, it can be NULL if no parent event.
 * 			If it's not NULL, caller will never see completion
 * 			of this event, instead he will only see completion
 * 			of \a parent when all children of \a parent are
 * 			completed.
 *
 * \return		zero on success, negative value if error
 */
int
daos_event_init(daos_event_t *ev, daos_handle_t eqh, daos_event_t *parent);

/**
 * Finalize an event. If event has been passed into any DAOS API, it can only
 * be finalized when it's been polled out from EQ, even it's aborted by
 * calling daos_event_abort().
 * Event will be removed from child-list of parent event if it's initialized
 * with parent. If \a ev itself is a parent event, then this function will
 * finalize all child events and \a ev.
 *
 * \param ev [IN]	event to finialize
 *
 * \return		zero on success, negative value if error
 */
int
daos_event_fini(daos_event_t *ev);

/**
 * Get the next child event of \a ev, it will return the first child event
 * if \a child is NULL.
 *
 * \param parent [IN]	parent event
 * \param child [IN]	current child event.
 *
 * \return		the next child event after \a child, or NULL if it's
 *			the last one.
 */
daos_event_t *
daos_event_next(daos_event_t *parent, daos_event_t *child);

/**
 * Try to abort operations associated with this event.
 * If \a ev is a parent event, this call will abort all child operations.
 *
 * \param ev [IN]	event (operation) to abort
 *
 * \return		zero on success, negative value if error
 */
int
daos_event_abort(daos_event_t *ev);

/***********************************************************************
 * Query DAOS storage layout and target information
 ***********************************************************************/

/**
 * Open system container which contains storage layout and detail
 * information of each target.
 * This system container is invisible to namespace, and it can't be
 * modified by DAOS API.
 * daos_sys_open will get reference of highest committed epoch of the
 * system container, which means all queries will only get information
 * within this epoch.
 *
 * \param daos_path [IN]	path to mount of filesystem
 * \param handle [OUT]		returned handle of context
 * \param ev [IN]		pointer to completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_sys_open(const char *daos_path,
	      daos_handle_t *handle, daos_event_t *ev);

/**
 * Close system container and release refcount of the epoch
 *
 * \param handle [IN]		handle of DAOS context
 * \param ev [IN]		pointer to completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_sys_close(daos_handle_t handle, daos_event_t *ev);

/**
 * Listen on change of target status
 *
 * \param handle [IN]		handle of DAOS system container
 * \param n_tevs [IN/OUT]	number of target events
 * \param tevs [OUT]		array of target events
 * \param ev [IN]		pointer to completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_sys_listen(daos_handle_t handle, unsigned int *n_tevs,
		daos_target_event_t *tevs, daos_event_t *ev);
/**
 * Query storage tree topology of DAOS
 *
 * \param handle [IN]		handle of DAOS sys-container
 * \param loc [IN]		input location of cage/rack/node/target
 * \param depth [IN]		subtree depth of this query, if depth is 0
 * 				then querying the whole subtree.
 * \param n_lks [IN/OUT]	if \a lks is NULL, number of subtree traversal
 * 				footprints is returned (traversal depth is
 * 				limited by \a depth), otherwise it's input
 * 				parameter which is array size of \a lks
 * \param lks [OUT]		array to store footprint of preorder subtree
 * 				traversal
 * 				a) loc::lc_cage is DAOS_LOC_UNKNOWN, returned
 * 				   footprints for different depth:
 * 				   0/4+: whole tree traversal
 * 				   1: cages traversal
 * 				   2: cages and racks traversal
 * 				   3: cages, racks and node traversal
 *
 * 				b) loc::lc_cage is specified, loc::lc_rack is
 * 				   DAOS_LOC_UNKNOWN, returned footprints for
 * 				   different depth:
 * 				   0/3+: subtree traversal of this cage
 * 				   1: racks traversal under this cage
 * 				   2: racks and node traversal
 *
 * 				c) loc::lc_cage and loc::lc_rack are specified,
 * 				   loc::lc_node is DAOS_LOC_UNKNOWN, returned
 * 				   footprints for different depth:
 * 				   0/2+: subtree traversal of this rack
 * 				   1: nodes traversal under this rack
 *
 * 				c) loc::lc_cage, loc::lc_rack loc::node are
 * 				   specified, returned footprints:
 * 				   any: targets traversal of this node
 *
 * \param ev [IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_sys_query(daos_handle_t handle, daos_location_t *loc,
	       unsigned int depth, unsigned int *n_lks,
	       daos_loc_key_t *lks, daos_event_t *ev);

/*
 * Query detail information of a storage target
 *
 * \param handle [IN]		handle of DAOS sys-container
 * \param target [IN]		location of a target
 * \param info [OUT]		detail information of the target
 * \param failover [OUT]	it can be NULL, if it's not NULL, failover
 * 				nodes location of given target is returned
 * \param ev [IN]		pointer to completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_sys_query_target(daos_handle_t handle, unsigned int target,
		      daos_target_info_t *info, daos_event_t *ev);

/***********************************************************************
 * Container data structures and functions
 *
 * DAOS container is a special file which exists in POSIX namespace
 * But user can only change/access content of a container via DAOS APIs.
 * A container can contain any number of shards (shard is kind of virtual
 * storage target), and can contain infinite number of DAOS objects.
 ***********************************************************************/

/**
 * Open a DAOS container
 *
 * Collective open & close:
 * ------------------------
 * If there're thousands or more processes want to open a same container
 * for read/write, server might suffer from open storm, also if all these
 * processes want to close container at the same time after they have
 * done their job, server will suffer from close storm as well. That's
 * the reason DAOS needs to support collective open/close.
 *
 * Collective open means one process can open a container for all his
 * sibling processes, this process only needs to send one request to
 * server and tell server it's a collective open, after server confirmed
 * this open, he can broadcast open representation to all his siblings,
 * all siblings can then access the container w/o sending open request
 * to server.
 *
 * After all sibling processes done their job, they need to call close to
 * release local handle, only the close called by opener will do the real
 * close.
 *
 * \param path [IN]	path to container in POSIX namespace
 * \param mode [IN]	open mode, see above comment
 * \param nprocess [IN]	it's a collective open if nprocess > 1
 * 			it's the number of processes will share this open
 * \param coh [IN/OUT]	returned container handle
 * \param event [IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_container_open(const char *path, unsigned int mode,
		    unsigned int nprocess, daos_handle_t *coh,
		    daos_event_t *event);

/**
 * close a DAOS container and release open handle.
 * This is real regular close if \a coh is not a handle from collective
 * open. If \a coh a collectively opened handle, and it's called by opener,
 * then it will do the real close for container, otherwise it only release
 * local open handle.
 *
 * \param coh [IN]	container handle
 * \param event	[IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_container_close(daos_handle_t coh, daos_event_t *event);

/**
 * destroy a DAOS container and all shards
 *
 * \param path [IN]	POSIX name path to container
 * \param event	[IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_container_unlink(const char *path, daos_event_t *event);

/**
 * create snapshot for a container based on its last durable epoch
 *
 * \param path [IN]	POSIX name path to container
 * \param snapshot [IN]	path of snapshot
 *
 * \return		zero on success, negative value if error
 */
int
daos_container_snapshot(const char *path, const char *snapshot,
			daos_event_t *event);

/**
 * return shards information, the highest committed epoch etc
 *
 * \param path [IN]	POSIX name path to container
 * \param info [OUT]
 * \param event [IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_container_query(const char *path,
		     daos_container_info_t *info, daos_event_t *event);


/**
 * Listen on layout change of container
 *
 * \param handle [IN]		handle of DAOS container
 * \param n_sevs [IN/OUT]	number of shard events
 * \param sevs [OUT]		array of shard events
 * \param ev [IN]		pointer to completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_container_listen(daos_handle_t coh, unsigned int *n_sevs,
		      daos_shard_event_t *sevs, daos_event_t *event);

/***********************************************************************
 * collective operation APIs
 ***********************************************************************/

/**
 * Convert a local handle to global representation data which can be
 * shared with peer processes, handle has to be container handle or
 * epoch scope handle, otherwise error will be returned.
 * This function can only be called by the process did collective open.
 *
 * \param handle [IN]	container or epoch scope handle
 * \param global[OUT]	buffer to store container information
 * \param size[IN/OUT]	buffer size to store glolal representation data,
 * 			if \a global is NULL, required buffer size is
 * 			returned, otherwise it's the size of \a global.
 *
 * \return		zero on success, negative value if error
 */
int
daos_local2global(daos_handle_t handle,
		  void *global, unsigned int *size);

/**
 * Create a local handle for global representation data.
 * see details in \a daos_container_open and \a daos_local2global
 *
 * \param coh [OUT]	returned handle
 * \param global[IN]	global (shared) representation of a collectively
 * 			opened container/epoch scope
 * \param size[IN]	bytes number of \a global
 *
 * \return		zero on success, negative value if error
 *
 * Example:
 * process-A:
 *     daos_container_open(..., DAOS_CMODE_RD, 2, &coh, ...);
 *     daos_local2global(coh, NULL, &size);
 *     gdata = malloc(size);
 *     daos_local2global(coh, gdata, &size);
 *     <send gdata to process-B>
 *     <start to access container via coh>
 *
 * process-B:
 *     <receive gdata from process-A>
 *     daos_global2local(gdata, size, &coh, ...);
 *     <start to access container via coh>
 */
int
daos_global2local(void *global, unsigned int size,
		  daos_handle_t *handle, daos_event_t *ev);

/***********************************************************************
 * Shard API
 *
 * Container is application namespace, and shard is virtual storage target
 * of container, user can add any number of shard into a container, or
 * disable shard for a container so shard is invisible to that container.
 * user need to specify a shard while create object in a container.
 ***********************************************************************/

/**
 * Add new shards to a container
 *
 * \param coh [IN]		container owns this shard
 * \param epoch [IN]		writable epoch of this container
 * \param n_targets [IN]	array size of \a targets
 * \param targets [IN]		array of targets
 * \param shards [IN]		specified shard ID array
 * \param event [IN]		completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_shard_add(daos_handle_t coh, daos_epoch_t epoch,
	       unsigned int n_targets, unsigned int *targets,
	       unsigned int *shards, daos_event_t *event);

/**
 * disable a shard for a container
 *
 * \param coh [IN]		container owns this shard
 * \param epoch [IN]		writable epoch of this container
 * \param n_shards [IN]		array size of \a shards
 * \param shards [IN]		shards to disable
 * \param event [IN]		completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_shard_disable(daos_handle_t coh, daos_epoch_t epoch,
		   unsigned int n_shards, unsigned int *shards,
		   daos_event_t *event);

/**
 * query a shard, i.e: placement information, number of objects etc.
 *
 * \param coh [IN]		container handle
 * \param epoch [IN]		epoch of this container
 * \param shard [IN]		shard ID
 * \param sinfo [OUT]		returned shard information
 * \param event [IN]		completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_shard_query(daos_handle_t coh, daos_epoch_t epoch, unsigned int shard,
		 daos_shard_info_t *info, daos_event_t *event);

/**
 * Flush all (changed) changes up to give epoch to a shard
 *
 * \param coh [IN]		container handle
 * \param epoch [IN]		epoch of this container
 * \param shard [IN]		shard ID
 * \param event [IN]		completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_shard_flush(daos_handle_t coh, daos_epoch_t epoch,
		 unsigned int shard, daos_event_t *event);

/**
 * enumerate non-empty object IDs in a shard
 *
 * \param coh [IN]		container handle
 * \param epoch [IN]		epoch of this container
 * \param shard [IN]		shard ID
 * \param anchor [IN/OUT]	anchor for the next object ID
 * \param oidn [IN]		size of \a objids array
 * \param oids [OUT]		returned object IDs.
 * \param event [IN]		completion event
 *
 * \return			zero on success, negative value if error
 */
int
daos_shard_list_obj(daos_handle_t coh, daos_epoch_t epoch,
		    unsigned int shard, daos_off_t *anchor,
		    daos_size_t oidn, daos_obj_id_t *oids,
		    daos_event_t *event);

/**************************************************************
 * Object API
 **************************************************************/
/**
 * open a DAOS object for I/O
 * DAOS always assume all objects are existed (filesystem actually
 * needs to CROW, CReate On Write), which means user doesn't need to
 * explictly create/destroy object, also, size of object is infinite
 * large, read an empty object will just get all-zero buffer.
 *
 * \param coh [IN]	container handle
 * \param oid [IN]	object to open
 * \param mode [IN]	open mode: DAOS_OMODE_RO/WR/RW
 * \param oh [OUT]	returned object handle
 * \param event [IN]	pointer to completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_object_open(daos_handle_t coh,
		 daos_obj_id_t oid, unsigned int mode,
		 daos_handle_t *oh, daos_event_t *event);

/**
 * close a DAOS object for I/O, object handle is invalid after this.
 *
 * \param oh [IN]	open handle of object
 *
 * \return		zero on success, negative value if error
 */
int
daos_object_close(daos_handle_t oh, daos_event_t *event);

/**
 * read data from DAOS object, read from non-existed data will
 * just return zeros.
 *
 * \param oh [IN]	object handle
 * \param epoch [IN] 	epoch to read
 * \param mmd [IN]	memory buffers for read, it's an arry of buffer + size
 * \param iod [IN]	source of DAOS object read, it's an array of
 * 			offset + size
 * \param event [IN]	completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_object_read(daos_handle_t oh, daos_epoch_t epoch,
		 daos_mmd_t *mmd, daos_iod_t *iod, daos_event_t *event);

/**
 * write data in \a mmd into DAOS object
 * User should always give an epoch value for write, epoch can be
 * any value larger than the HCE, write to epoch number smaller than HCE
 * will get error.
 *
 * \param oh [IN]	object handle
 * \param epoch [IN] 	epoch to write
 * \param mmd [IN]	memory buffers for write, it's an array of buffer + size
 * \param iod [IN]	destination of DAOS object write, it's an array of
 * 			offset + size
 * \param event [IN]	completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_object_write(daos_handle_t oh, daos_epoch_t epoch,
		  daos_mmd_t *mmd, daos_iod_t *iod,
		  daos_event_t *event);

/**
 * flush all (cached) writes up to the give epoch to a object
 *
 * \param oh [IN]	object handle
 * \param epoch [IN] 	epoch to flush
 * \param event [IN]	completion event
 */
int
daos_object_flush(daos_handle_t oh,
		  daos_epoch_t epoch, daos_event_t *event);

/**
 * discard data between \a begin and \a end of an object, all data will
 * be discarded if begin is 0 and end is -1.
 *
 * This will remove backend FS inode and space if punch it to zero
 *
 * \param coh [IN]	container handle
 * \param epoch [IN]	writable epoch of this container
 * \param oid [IN]	object ID
 * \param begin [IN]	start offset, 0 means begin of the object
 * \param end [IN]	end offset, -1 means end of the object
 * \param event [IN]	completion event
 *
 * \return		zero on success, negative value if error
 */
int
daos_object_punch(daos_handle_t coh, daos_epoch_t epoch,
		  daos_obj_id_t oid, daos_off_t begin, daos_off_t end,
		  daos_event_t *event);

/**************************************************************
 * Epoch & Epoch functions
 *
 * Version numbers, called epochs, serve as transaction identifiers and are
 * passed in all DAOS I/O operations.
 *
 * Epochs are totally ordered.  An epoch becomes consistent only after all
 * prior epochs are consistent and all writes in the epoch itself have
 * completed. It is therefore an error to attempt to write in a consistent
 * epoch since all valid writes in such epochs have completed already.
 * Writes belonging to epochs that can never become consistent (e.g. due to
 * some failure) are discarded. Readers may query the current highest
 * committed epoch (HCE) number and use it on reads to ensure they see
 * consistent data. DAOS effectively retains a view of the HCE given
 * to any reader while such readers remain to provide read consistency and
 * allow concurrent writers to make progress. When all readers for an old
 * view have departed the view becomes inaccessible and space is reclaimed.
 *
 * Epochs are used within an epoch scope. Each epoch scope covers a unique
 * set of filesystem entities that may be affected by transactions using
 * the scope. Epoch scopes may not overlap - i.e. cover the same filesystem
 * entities. Currently there is a 1:1 mapping between epoch scopes and DAOS
 * containers. A single epoch scope covers a single DAOS container and exists
 * for the lifetime of the container therefore transactions may only span
 * a single container and all transactions within a container are executed
 * in the same epoch scope and exist in the same total order. Note that the
 * lifetime and coverage of an epoch scope may be made more flexible in the
 * future.
 **************************************************************/

/**
 * Open an epoch scope on the specified container(s). Epoch returned is the
 * highest committed epoch (HCE) of container and it is guaranteed not to
 * disappear until it is slipped or the epoch scope is closed.
 *
 * \param coh [IN]	container handle for this epoch
 * \param esh[OUT]	handle of epoch sequence
 * \param hce[OUT]	returned HCE
 * \param event [IN]	pointer to completion event
 */
int
daos_epoch_scope_open(daos_handle_t coh, daos_epoch_id_t *hce,
		      daos_handle_t *esh, daos_event_t *ev);

/**
 * Closes the epoch scope. 
 * If ‘error’ is set, all writes in epochs that are not yet marked
 * consistent are rolled back. Behaviour is undefined if ‘error’ is zero
 * but writes in epochs that have yet to be marked consistent are
 * outstanding since it's impossible to determine whether all writes
 * in all epochs have been marked consistent until the epoch scope is
 * closed by all processes holding it open.
 *
 * \param esh [IN]	epoch sequence to close
 * \param error	[IN]	error code
 * \param event [IN]	pointer to completion event
 */
int
daos_epoch_scope_close(daos_handle_t esh, int error, daos_event_t *ev);

/**
 * Complete when \a epoch is durable. If *epoch is DAOS_EPOCH_HCE, it
 * sets \a epoch to the actual highest committed epoch and completes
 * immediately. If \a ev is NULL, it completes immediately but returns
 * failure if epoch is not currently durable. If is completes successfully,
 * the reference on the epoch sequence’s previous durable epoch is moved
 * to the epoch returned.
 *
 * \param esh [IN]	epoch sequence handle
 * \param epoch[IN/OUT] epoch to slip to, if it's DAOS_EPOCH_HCE or the
 *			given epoch is garbage collected, then epoch number
 *			of HCE is returned.
 * \param ev [IN]	pointer to completion event
 */
int
daos_epoch_slip(daos_handle_t esh,
		daos_epoch_id_t *epoch, daos_event_t *ev);

/**
 * Returns an epoch number that will "catch up" with epoch number usage
 * by other processes sharing the same epoch scope.
 * This ensures that processes executing a series of long running
 * transactions do not delay short running transactions executed in the
 * same epoch scope.
 *
 * \param esh [IN]	epoch sequence handle
 * \param epoch[OUT]    returned "catch up" epoch which is best for write
 * \param ev [IN]	pointer to completion event
 */
int
daos_epoch_catchup(daos_handle_t esh,
		   daos_epoch_id_t *epoch, daos_event_t *ev);

/**
 * Signals that all writes associated with this epoch sequence up to and
 * including ‘epoch’ have completed. Completes when this epoch becomes
 * durable.
 * If commit is failed, it’s just like the commit hasn’t happened yet, HCE
 * remains it was, so user can find out failed shards and disable them,
 * and commit again.
 * If it's a fatal error, user should call daos_epoch_scope_close with
 * setting abort flag to discard changes.
 *
 * \param esh [IN]		epoch sequence handle
 * \param epoch[OUT]		epoch to complete
 * \param sync[OUT]		DAOS will take a snapshot for this commit
 * 				(no aggregation)
 * \param shard_failed [OUT]	failed shared ID when there is error on commit
 * \param ev [IN]		pointer to completion event
 */
int
daos_epoch_commit(daos_handle_t esh, daos_epoch_id_t epoch,
		  int sync, int *shard_failed, daos_event_t *ev);

#endif /* __KERNEL__ */
#endif /* __DAOS_API_H__ */
