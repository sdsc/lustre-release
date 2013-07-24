#!/bin/bash
#
# This script is used to test large size LUN support in Lustre.
#
################################################################################
set -e

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

# Variable to run mdsrate
THREADS_PER_CLIENT=${THREADS_PER_CLIENT:-5}    # thread(s) per client node
MACHINEFILE=${MACHINEFILE:-$TMP/$TESTSUITE.machines}
NODES_TO_USE=${NODES_TO_USE:-$CLIENTS}
NUM_CLIENTS=$(get_node_count ${NODES_TO_USE//,/ })

# bug number:
ALWAYS_EXCEPT="$LARGE_LUN_EXCEPT"

build_test_filter
LARGE_LUN_RESTORE_MOUNT=false
if is_mounted $MOUNT || is_mounted $MOUNT2; then
	LARGE_LUN_RESTORE_MOUNT=true
fi
# Unmount and cleanup the Lustre filesystem
cleanupall
load_modules

FULL_MODE=${FULL_MODE:-false}
RUN_FSCK=${RUN_FSCK:-true}
# if SLOW=yes, enable the FULL_MODE
[[ $SLOW = yes ]] && FULL_MODE=true
#########################################################################
# Dump the super block information for the filesystem present on device.
run_dumpe2fs() {
	local facet=$1
	local dev=$2

	log "dump the super block information on $facet device $dev"
	local cmd="$DUMPE2FS -h $dev"
	do_facet $facet "$cmd"
}

# Run resize2fs on MDT or OST device.
run_resize2fs() {
	local node=$1
	local target_dev=$2
	local extra_opts=$3
	local resize=$4

	local cmd="resize2fs $extra_opts $target_dev $resize"
	echo $cmd
	local rc=0
	do_node $node $cmd || rc=$?

	return $rc
}

# Report Lustre filesystem disk space usage and inodes usage of each MDT/OST.
client_df() {
	local mnt_pnt=$1
	local cmd

	cmd="df -h"
	echo -e "\n# $cmd"
	eval $cmd

	cmd="lfs df -h $mnt_pnt"
	echo -e "\n# $cmd"
	eval $cmd

	cmd="lfs df -i $mnt_pnt"
	echo -e "\n# $cmd"
	eval $cmd
}

# Cleanup the directories and files created by llverfs utility.
cleanup_dirs() {
	local target=$1
	local mnt=${2:-$MOUNT}
	local cmd="rm -rf $mnt/{*.filecount,dir*}"
	do_facet $target "$cmd"
}

# Run mdsrate.
run_mdsrate() {
	generate_machine_file $NODES_TO_USE $MACHINEFILE ||
		error "can not generate machinefile"

	# set the default stripe count for files in this test to one
	local testdir=$MOUNT/mdsrate
	mkdir -p $testdir
	chmod 0777 $testdir
	$LFS setstripe $testdir -i 0 -c 1
	get_stripe $testdir

	local num_dirs=$THREADS_PER_CLIENT
	[[ $num_dirs -eq 0 ]] && num_dirs=1
	local free_inodes=$(lfs df -i $MOUNT | grep "OST:0" | awk '{print $4}')
	local num_files
	num_files=$((free_inodes / num_dirs))

	local command="$MDSRATE $MDSRATE_DEBUG --create --verbose \
		--ndirs $num_dirs --dirfmt '$testdir/dir%d' \
		--nfiles $num_files --filefmt 'file%%d'"

	echo "# $command"
	mpi_run -np $((NUM_CLIENTS * THREADS_PER_CLIENT)) -machinefile \
		$MACHINEFILE $command

	if [ ${PIPESTATUS[0]} != 0 ]; then
		error "mdsrate create failed"
	fi
}

# Get the block count of the filesystem.
get_block_count() {
	local facet=$1
	local device=$2
	local bcount

	bcount=$(do_facet $facet "$DUMPE2FS -h $device 2>&1" |
		awk '/^Block count:/ {print $3}')
	echo $bcount
}

# Run e2fsck on MDS and OST
do_fsck() {
	$RUN_FSCK || return
	local dev
	run_e2fsck $(facet_host $SINGLEMDS) $(mdsdevname ${SINGLEMDS//mds/}) \
		"-y" || error "run e2fsck error"
	for num in $(seq $OSTCOUNT); do
		dev=$(ostdevname $num)
		run_e2fsck $(facet_host ost${num}) $dev "-y" ||
			error "run e2fsck error"
	done
}

# Run resize and e2fsck
do_resize() {
	local facet=$1
	local dev=$2

	stopall
	run_e2fsck $facet $dev "-y"
	# Get the original number of blocks for the file system, then resize to
	# 1) smaller than minimum allowable fs size; expect failure
	# 2) the maximum number of blocks allowed
	# 3) the minimum number of blocks allowed
	# 4) and, if possible, something inbetween the min and max
	fs_blks=$(get_block_count $facet $dev)
	run_resize2fs $facet $dev "-p -d 30" "10"
	[[ $? -ne 1 ]] &&
		error "resize2fs returned $?, expected 1 on too small resize"
	run_resize2fs $facet $dev "-p -d 30"
	[[ $? -ne 0 ]] &&
		error "resize2fs returned $?, expected 0 for maximum resize"
	max_blks=$(get_block_count $facet $dev)
	run_resize2fs $facet $dev "-p -d 30 -M"
	[[ $? -ne 0 ]] &&
		error "resize2fs returned $?, expected 0 for minimum resize"
	min_blks=$(get_block_count $facet $dev)

	if [ $min_blks -ne $max_blks ]; then
		fcount=$(( ( max_blks - min_blks )/2 + min_blks ))
		resize_blks=$(run_resize2fs $facet $dev "-d 30 -p" "$fcount" |
			     awk '/blocks long/ {print $7}')
		[[ $? -ne 0 ]] &&
			error "resize2fs returned $?, expected 0 for resize"
		[[ $resize_blks -ne $fcount ]] &&
			error "resize2fs returned $resize_blks, expected $fcount"
	fi

	# Return file system to original size and run e2fsck
	resize_blks=$(run_resize2fs $facet $dev "-p -d 30" "$fs_blks" |
		     awk '/blocks long/ {print $7}')
	[[ $? -ne 0 ]] &&
		error "resize2fs returned $?, expected 0 for original size resize"
	[[ $fs_blks -ne $resize_blks ]] &&
		error "Unable to resize back to original ${fs_blks}. New size $resize_blks"
	run_e2fsck $facet $dev "-y" || error "run e2fsck error"
}

################################## Main Flow ###################################
trap cleanupall EXIT

test_1 () {
	local dev
	for num in $(seq $OSTCOUNT); do
		dev=$(ostdevname $num)
		log "run llverdev on the OST $dev"
		do_rpc_nodes $(facet_host ost${num}) run_llverdev $dev -vpf ||
			error "llverdev on $dev failed!"
	done
}
run_test 1 "run llverdev on raw LUN"

test_2 () {
	local dev
	local ostmnt

	for num in $(seq $OSTCOUNT); do
		dev=$(ostdevname $num)
		ostmnt=$(facet_mntpt ost${num})

		# Mount the OST as an ldiskfs filesystem.
		log "mount the OST $dev as a ldiskfs filesystem"
		add ost${num} $(mkfs_opts ost${num}) $FSTYPE_OPT --reformat \
			$(ostdevname $num) > /dev/null ||
			error "format ost${num} error"
		run_dumpe2fs ost${num} $dev
		do_facet ost${num} mount -t $(facet_fstype ost${num}) $dev \
			$ostmnt "$OST_MOUNT_OPTS"

		# Run llverfs on the mounted ldiskfs filesystem in partial mode
		# to ensure that the kernel can perform filesystem operations
		# on the complete device without any errors.
		log "run llverfs in partial mode on the OST ldiskfs $ostmnt"
		do_rpc_nodes $(facet_host ost${num}) run_llverfs $ostmnt -vpl \
			"no" || error "run_llverfs error on ldiskfs"

		# Unmount the OST.
		log "unmount the OST $dev"
		stop ost${num}

		# After llverfs is run on the ldiskfs filesystem in partial
		# mode, a full e2fsck should be run to catch any errors early.
		$RUN_FSCK && run_e2fsck $(facet_host ost${num}) $dev "-y" ||
			error "run e2fsck error"

		if $FULL_MODE; then
			log "full mode, mount the OST $dev as a ldiskfs again"
			do_facet ost${num} mount -t $(facet_fstype ost${num}) \
				$dev $ostmnt "$OST_MOUNT_OPTS"
			cleanup_dirs ost${num} $ostmnt
			do_facet ost${num} "sync"

			run_dumpe2fs ost${num} $dev

			# Run llverfs on the mounted ldiskfs filesystem in full
			# mode to ensure that the kernel can perform filesystem
			# operations on the complete device without any errors.
			log "run llverfs in full mode on OST ldiskfs $ostmnt"
			do_rpc_nodes $(facet_host ost${num}) run_llverfs \
				$ostmnt -vl "no" ||
				error "run_llverfs error on ldiskfs"

			# Unmount the OST.
			log "unmount the OST $dev"
			stop ost${num}

			# After llverfs is run on the ldiskfs filesystem in
			# full mode, a full e2fsck should be run to catch any
			#  errors early.
			$RUN_FSCK && run_e2fsck $(facet_host ost${num}) $dev \
				"-y" || error "run e2fsck error"
		fi
	done
}
run_test 2 "run llverfs on OST ldiskfs filesystem"

test_3 () {
	[ -z "$CLIENTS" ] && skip_env "CLIENTS not defined, skipping" && return
	[ -z "$MPIRUN" ] && skip_env "MIPRUN not defined, skipping" && return
	[ -z "$MDSRATE" ] && skip_env "MDSRATE not defined, skipping" && return
	[ ! -x $MDSRATE ] && skip_env "$MDSRATE not built, skipping" && return
	# Setup the Lustre filesystem.
	log "setup the lustre filesystem"
	REFORMAT="yes" check_and_setup_lustre

	log "run mdsrate to use up the free inodes."
	# Run the mdsrate test suite.
	run_mdsrate
	client_df $MOUNT

	sync; sleep 5; sync
	stopall
	do_fsck
}
run_test 3 "use up free inodes on the OST with mdsrate"

test_4 () {
	# Setup the Lustre filesystem.
	log "setup the lustre filesystem"
	REFORMAT="yes" check_and_setup_lustre
	local dev

	for num in $(seq $OSTCOUNT); do
		dev=$(ostdevname $num)
		run_dumpe2fs ost${num} $dev
	done

	# Run llverfs on the mounted Lustre filesystem both in partial and
	# full mode to to fill the filesystem and verify the file contents.
	log "run llverfs in partial mode on the Lustre filesystem $MOUNT"
	run_llverfs $MOUNT -vp "no" || error "run_llverfs error on lustre"
	client_df $MOUNT

	sync; sleep 5; sync
	stopall
	do_fsck

	if $FULL_MODE; then
		# Setup the Lustre filesystem again.
		log "setup the lustre filesystem again"
		setupall

		cleanup_dirs client $MOUNT
		sync
		client_df $MOUNT

		for num in $(seq $OSTCOUNT); do
			dev=$(ostdevname $num)
			run_dumpe2fs ost${num} $dev
		done

		log "run llverfs in full mode on the Lustre filesystem $MOUNT"
		run_llverfs $MOUNT -vl "no" ||
			error "run_llverfs error on lustre"
		client_df $MOUNT

		sync; sleep 5; sync
		stopall
		do_fsck
	fi
}
run_test 4 "run llverfs on lustre filesystem"

test_5 () {
	[ "$(facet_fstype $SINGLEMDS)" != ldiskfs ] &&
		skip "Only applicable to ldiskfs-based MDTs" && return

	# Setup the Lustre filesystem and create files
	setupall

	local mdshost=$(facet_host $SINGLEMDS)
	local mdsdev=$(mdsdevname ${SINGLEMDS//mds/})

	test_mkdir -p $DIR/$tdir || error "mkdir $tdir failed"
	createmany -o $DIR/$tdir/t- 1000 ||
		error "create files on remote directory failed"

	do_resize $mdshost $mdsdev
}
run_test 5 "run resize2fs on MDT"

test_6 () {
	[ "$(facet_fstype ost${OSTCOUNT})" != ldiskfs ] &&
	skip "Only applicable to ldiskfs-based OSTs" && return

	# Setup the Lustre filesystem and create files
	setupall

	local osthost=$(facet_host ost${OSTCOUNT})
	local ostdev=$(ostdevname $OSTCOUNT)

	test_mkdir -p $DIR/$tdir || error "mkdir $tdir failed"
	createmany -o $DIR/$tdir/t- 1000 ||
		error "create files on remote directory failed"

	do_resize $osthost $ostdev
}
run_test 6 "run resize2fs on OST"

complete $SECONDS
$LARGE_LUN_RESTORE_MOUNT && setupall
check_and_cleanup_lustre
exit_status
