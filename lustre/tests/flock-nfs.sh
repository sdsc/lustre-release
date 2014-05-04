#!/bin/bash
#
#

set -e

ONLY=${ONLY:-"$*"}

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

. $LUSTRE/tests/setup-nfs.sh

# bug number:
ALWAYS_EXCEPT=""

NFSVERSION=3
EXPORT_OPTS="rw"

build_test_filter
check_and_setup_lustre

# first unmount all the lustre client
cleanup_mount $MOUNT
# mount lustre on mds
lustre_client=${NFS_SERVER:-$(facet_active_host $SINGLEMDS)}
NFS_CLIENTS=${NFS_CLIENTS:-$CLIENTS}
NFS_CLIENTS=$(exclude_items_from_list $NFS_CLIENTS $lustre_client)
CL_MNT_OPT=""
zconf_mount_clients $lustre_client $MOUNT "$CL_MNT_OPT" ||
    error "mount lustre on $lustre_client failed"

assert_DIR
#set -ex

NFS_MNT=$MOUNT
LUSTRE_MNT=$MOUNT
LOCKF=lockfile
[ -z $NFS_CLIENTS ] && error "NFS_CLIENTS is empty"

nfs_stop() {
	cleanup_nfs "$NFS_MNT" "$lustre_client" "$NFS_CLIENTS" || \
	        error_noexit false "failed to cleanup nfs"
	echo "NFS stopped"
}

cleanup() {
	nfs_stop
	cleanupall -f
	cleanup_client3
}

trap cleanup SIGHUP SIGINT SIGTERM

nfs_start() {
	do_nodes $NFS_CLIENTS "umount -f $NFS_MNT"
	# setup the nfs
	if ! setup_nfs "$NFSVERSION" "$NFS_MNT" "$lustre_client" "$NFS_CLIENTS"; then
	    error_noexit false "setup nfs failed!"
	    cleanup_nfs "$NFS_MNT" "$lustre_client" "$NFS_CLIENTS" || \
	        error_noexit false "failed to cleanup nfs"
	    check_and_cleanup_lustre
	    exit
	fi

	echo "NFS server mounted"
	do_nodes $NFS_CLIENTS "flock $NFS_MNT/$LOCKF -c 'echo test lock obtained'"
}

cleanup_client3() {
	do_nodes $NFS_CLIENTS "killall flock"
	do_nodes $NFS_CLIENTS "umount -f $NFS_MNT"
	echo "client cleanup successful"
}

test_1a() {
	[ -z $TEST_DIR_LTP_FLOCK ] && \
		skip_env "TEST_DIR_LTP_FLOCK is empty" && return
	check_and_setup_lustre
	nfs_start || return 1
	for i in `ssh root@$NFS_CLIENTS "ls $TEST_DIR_LTP_FLOCK|sort"`
	do
		do_nodes $NFS_CLIENTS "export TMPDIR=$NFS_MNT;$TEST_DIR_LTP_FLOCK/$i"
	done
	ssh root@$NFS_CLIENT "umount $NFS_MNT"
	nfs_stop
}
run_test 1a "LTP testsuite"

test_1b() {
	LOCKTESTS=${LOCKTESTS:-$(which locktests 2> /dev/null || true)}
	echo $LOCKTESTS
	[ x$LOCKTESTS = x ] &&
		{ skip_env "locktests not found" && return; }
	check_and_setup_lustre
	nfs_start || return 1
	do_nodes $NFS_CLIENTS "$LOCKTESTS -n 50 -f $NFS_MNT/locktests"
	nfs_stop
}
run_test 1b "locktests"

test_2b() {
	check_and_setup_lustre
	nfs_start || return 1
	ssh -f root@$NFS_CLIENTS "flock -e $NFS_MNT/LOCKF -c 'sleep 10'"
	sleep 1
	umount /mnt/mds1
	echo "MDS unmounted"
	nfs_stop
	cleanupall -f || error "cleanup failed"
	cleanup_client3
}
run_test 2b "simple cleanup"

test_3a() {
	check_and_setup_lustre
	flock -e $LUSTRE_MNT/$LOCKF -c 'sleep 10' &
	sleep 1
	flock -e $LUSTRE_MNT/$LOCKF -c 'sleep 5' &
	sleep 1
	echo "umount -f /mnt/mds1"
	umount -f /mnt/mds1 || true
	killall flock
	echo "umount -f $LUSTRE_MNT"
	umount -f $LUSTRE_MNT || true
	cleanupall -f || error "cleanup failed"
}
run_test 3a "MDS umount with blocked flock"

test_3b() {
	check_and_setup_lustre
	nfs_start || return 1
	flock -e $LUSTRE_MNT/$LOCKF -c 'sleep 15' &
	sleep 1
	ssh -f root@$NFS_CLIENTS "flock -e $NFS_MNT/$LOCKF -c 'sleep 10'"
	sleep 1
	echo "umount -f $LUSTRE_MNT"
	umount -f $LUSTRE_MNT || true
	nfs_stop
	cleanupall -f || error "cleanup failed"
	cleanup_client3
}
run_test 3b "cleanup with blocked lock"

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
