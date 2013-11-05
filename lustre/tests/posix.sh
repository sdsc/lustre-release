#!/bin/bash
#set -vx
set -e

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

POSIX_DIR=${POSIX_DIR:-"$LUSTRE/tests/posix"}
POSIX_SRC=${POSIX_SRC:-"/usr/src/posix"}
BASELINE_FS=${BASELINE_FS:-"ext4"}

# SLES does not support read-write access to an ext4 file system by default
[[ -e /etc/SuSE-release ]] && BASELINE_FS=ext3

if [[ $(facet_fstype $SINGLEMDS) = zfs ]]; then
	BASELINE_FS=zfs
	! do_facet $SINGLEMDS "which $ZFS $ZPOOL >/dev/null 2>&1" &&
		skip_env "need $ZFS and $ZPOOL commands on $SINGLEMDS" && exit 0

	POSIX_ZPOOL=$FSNAME-posix
	POSIX_ZFS=$POSIX_ZPOOL/${POSIX_ZPOOL##$FSNAME-}
fi

if [[ -z $SHARED_DIRECTORY ]] || ! check_shared_dir $SHARED_DIRECTORY; then
	skip_env "SHARED_DIRECTORY should be accessible on all nodes"
	exit 0
fi

# cleanup the Lustre filesystem
cleanupall

build_test_filter

cleanup_loop_dev() {
	local facet=$1
	local mnt=$2
	local dev=$3
	local file=$4

	# if we only have 2 args, we will search for dev
	if [[ $# = 2 ]]; then
		do_facet $facet "
			dev=\\\$(losetup -a 2>&1) | grep \($mnt\) | cut -d: -f1;
			[[ -z \\\$dev ]] || losetup -d \\\$dev"
	else # we need all args
		[[ -z $mnt ]] || [[ -z $dev ]] || [[ -z $file ]] &&
			error "Can't cleanup loop device"
		do_facet $facet "
			umount $mnt;
			losetup -d $dev && rm -rf $mnt;
			rm -f $file"
	fi

	[[ $BASELINE_FS != zfs ]] || destroy_zpool $facet $POSIX_ZPOOL
}

setup_loop_dev() {
	local facet=$1
	local mnt=$2
	local dev=$3
	local file=$4
	local rc=0

	echo "Make a loop file system with $file on $dev on $facet"
	do_facet $facet "dd if=/dev/zero of=$file bs=1024k count=500 >/dev/null"
	if ! do_facet $facet "losetup $dev $file"; then
		rc=$?
		echo "can't set up $dev for $file"
		return $rc
	fi

	if [[ $BASELINE_FS = zfs ]]; then
		create_zpool $facet $POSIX_ZPOOL $dev || return ${PIPESTATUS[0]}
		create_zfs $facet $POSIX_ZFS || return ${PIPESTATUS[0]}
		dev=$POSIX_ZFS

	elif ! do_facet $facet "mkfs.$BASELINE_FS $dev"; then
		rc=$?
		echo "mkfs.$BASELINE_FS on $dev failed"
		return $rc
	fi

	do_facet $facet "mkdir -p $mnt" || return ${PIPESTATUS[0]}
	if ! do_facet $facet "mount -t $BASELINE_FS $dev $mnt"; then
		rc=$?
		echo "mount $BASELINE_FS failed"
		return $rc
	fi
	echo
	return $rc
}

test_1() {
	local allnodes="$(comma_list $(nodes_list))"
	local tfile="$TMP/$BASELINE_FS-file"
	local mntpnt="$SHARED_DIRECTORY/$TESTSUITE/$BASELINE_FS"
	local loopdev
	local rc=0

	# We start at loop1 because posix build uses loop0
	loopdev=$(do_facet $SINGLEMDS \
		"[[ -b /dev/loop/1 ]] && loopbase=/dev/loop/;
		[[ -b /dev/loop1 ]] && loopbase=/dev/loop;
		if [[ -n \\\$loopbase ]]; then
			for i in \\\$(seq 7); do
				losetup \\\$loopbase\\\$i >/dev/null 2>&1 &&
					continue;
				loopdev=\\\$loopbase\\\$i;
				break;
			done;
		fi;
		echo -n \\\$loopdev")
	[[ -z $loopdev ]] && error "Can not find loop device"

	if ! setup_loop_dev $SINGLEMDS $mntpnt $loopdev $tfile; then
		cleanup_loop_dev $SINGLEMDS $mntpnt $loopdev $tfile
		error "Setup loop device failed"
	fi

	# copy the source over to baseline filesystem mount point
	if ! do_facet $SINGLEMDS "cp -af $POSIX_SRC/*.* $mntpnt"; then
		cleanup_loop_dev $SINGLEMDS $mntpnt $loopdev $tfile
		error "Copy POSIX test suite failed"
	fi

	export POSIX_SRC=$mntpnt
	. $POSIX_DIR/posix.cfg

	setup_posix_users $allnodes
	if ! setup_posix $SINGLEMDS; then
		delete_posix_users $allnodes
		cleanup_loop_dev $SINGLEMDS $POSIX_SRC
		cleanup_loop_dev $SINGLEMDS $mntpnt $loopdev $tfile
		error "Setup POSIX test suite failed"
	fi

	log "Run POSIX test against lustre filesystem"
	check_and_setup_lustre
	run_posix $MOUNT compare ||
		error_noexit "Run POSIX testsuite on $MOUNT failed"

	[[ ! -d "$MOUNT/TESTROOT" ]] || rm -fr $MOUNT/TESTROOT
	delete_posix_users $allnodes
	cleanup_loop_dev $SINGLEMDS $POSIX_SRC
	cleanup_loop_dev $SINGLEMDS $mntpnt $loopdev $tfile
}
run_test 1 "install, build, run posix on $BASELINE_FS and lustre, then compare"

complete $SECONDS
check_and_cleanup_lustre
exit_status
