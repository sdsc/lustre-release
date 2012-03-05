#!/bin/bash
#set -vx
set -e

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

build_test_filter
check_and_setup_lustre

export POSIX_DIR=${POSIX_DIR:-"$LUSTRE/tests/posix"}
. $POSIX_DIR/posix.cfg

cleanup_loop_dev() {
    local mnt=$1
    local dev=$2
    local file=$3

    [ -z $mnt ] || [ -z $dev ] || [ -z $file ] &&
        error "Can't cleanup loop device"

    umount -f $mntpnt 2>/dev/null
    losetup -d $dev 2>/dev/null
    rm -f $file 2>/dev/null
}

setup_loop_dev() {
    local mnt=$1
    local dev=$2
    local file=$3
    local rc=0

    echo "Make a loop file system with $file on $dev"
    dd if=/dev/zero of=$file bs=1024k count=100 > /dev/null
    if ! losetup $dev $file; then
        rc=$?
        echo "can't set up $dev for $file"
        return $rc
    fi
    if ! mkfs.ext4 $dev; then
        rc=$?
        echo "mkfs.ext4 on $dev failed"
        return $rc
    fi
    mkdir -p ${mnt}
    if ! mount -t ext4 $dev $mnt; then
        rc=$?
        echo "mount ext4 failed"
        return $rc
    fi
    return $rc
}

test_1() {
    local allnodes="$(comma_list $(nodes_list))"
    local tfile="$TMP/ext4-file"
    local mntpnt=/mnt/ext4
    local loopbase
    local loopdev
    local rc=0

    # prepare, build, and install posix
    setup_posix
    [ -b /dev/loop/0 ] && loopbase=/dev/loop/
    [ -b /dev/loop0 ] && loopbase=/dev/loop
    [ -z "$loopbase" ] &&
        delete_posix_users &&
        error "/dev/loop/0 and /dev/loop0 gone?"

    for i in `seq 0 7`; do
        losetup $loopbase$i > /dev/null 2>&1 && continue
        loopdev=$loopbase$i
        break
    done

    [ -z "$loopdev" ] &&
        delete_posix_users &&
        error "Can not find loop device"

    if ! setup_loop_dev $mntpnt $loopdev $tfile; then
        delete_posix_users
        cleanup_loop_dev "$mntpnt" "$loopdev" "$tfile"
        error "Setup loop device failed"
    fi

    log "Run POSIX test against ext4 filesystem"
    if ! run_posix $mntpnt; then
        cleanup_loop_dev "$mntpnt" "$loopdev" "$tfile"
        delete_posix_users
        error "Run POSIX testsuite on $mntpnt failed"
    fi
    # save the result as baseline
    rm -fr $RESULT_DIR/lustre_baseline > /dev/null
    if ! mv $RESULT_DIR/lustre_report $RESULT_DIR/lustre_baseline; then
        delete_posix_users
        error "Failed to save lustre_report"
    fi
    cleanup_loop_dev "$mntpnt" "$loopdev" "$tfile"

    setup_posix_users $allnodes
    log "Run POSIX test against lustre filesystem"
    run_posix $MOUNT compare || \
        error_noexit "Run POSIX testsuite on $MOUNT failed"

    [ -d "$MOUNT/TESTROOT" ] && rm -fr $MOUNT/TESTROOT
    delete_posix_users $allnodes
}
run_test 1 "build, install, run posix on ext4 and lustre, then compare"

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
