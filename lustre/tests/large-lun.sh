#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:
#
# This script is used to test large size LUN support in Lustre.
#
################################################################################
set -e

LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

build_test_filter
stopall
load_modules

CLEANUP=${CLEANUP:-true}
FULL_MODE=${FULL_MODE:-true}
VERIFY_LDISKFS=${VERIFY_LDISKFS:-true}
RUN_FSCK=${RUN_FSCK:-true}
RUN_MDSRATE=${RUN_MDSRATE:-true}
RUN_LLVERDEV=${RUN_LLVERDEV:-true}
RUN_LLVERFS=${RUN_LLVERFS:-true}
THREADS_PER_CLIENT=${THREADS_PER_CLIENT:-25}

#########################################################################
# Convert the device size to KB.
size_in_KB() {
    local str="$1"
    local num=${str:0:${#str}-1}

    case ${str:${#str}-1} in
        k|K ) num=$num;;
        m|M ) num=$((num << 10));;
        g|G ) num=$((num << 20));;
        t|T ) num=$((num << 30));;
        * ) num=$str;;
    esac
    echo $num
}

# Dump the super block information for the filesystem present on device.
run_dumpe2fs() {
    local facet=$1
    local dev=$2

    log "dump the super block information on $node device $dev"
    local cmd="$DUMPE2FS -h $dev"
    echo "# $cmd"
    do_facet $facet "eval $cmd"
}

# Report Lustre filesystem disk space usage and inodes usage of each MDT/OST.
client_df() {
    echo "client_df"
    local mnt_pnt=$1
    local cmd

    cmd="df -h"
    echo -e "\n# $cmd"
    do_facet client "eval $cmd"

    cmd="lfs df -h $mnt_pnt"
    echo -e "\n# $cmd"
    do_facet client "eval $cmd"

    cmd="lfs df -i $mnt_pnt"
    echo -e "\n# $cmd"
    do_facet client "eval $cmd"
}


# Cleanup the directories and files created by llverfs utility.
cleanup_dirs() {
    local target=$1
    local mnt=${2:-$MOUNT}
    local cmd="time rm -rf $mnt/{*.filecount,dir*}"
    echo -e "\n# $cmd"
    do_facet $target "eval $cmd"
}

# Run mdsrate.
run_mdsrate() {
    log "run test mdsrate"
    local cmd
    
    cmd="$cmd MDSSIZE=$(size_in_KB $MDSSIZE)"
    cmd="$cmd OSTSIZE=$(size_in_KB $OSTSIZE)"

    local num_dirs=$THREADS_PER_CLIENT
    [ num_dirs == 0 ] || num_dirs=1
    local free_inodes=$(lfs df -i $MOUNT | grep "OST:0" | awk '{print $4}')
    local num_files
    num_files=$((free_inodes / num_dirs))
    cmd="$cmd NUM_DIRS=$num_dirs NUM_FILES=$num_files"
    cmd="$cmd THREADS_PER_CLIENT=$THREADS_PER_CLIENT"
    cmd="$cmd bash mdsrate-create.sh"

    echo -e "# $cmd\n"
    do_facet client "eval $cmd"
}

################################## Main Flow ###################################
trap cleanupall EXIT

test_1 () {
    log "run llverdev on the OST $OSTDEV1"
    do_rpc_nodes $(facet_host ost1) run_llverdev $(ostdevname 1) -vpf
    [ $? -eq 0 ] || error "llverdev failed!"
}
run_test 1 "run llverdev on raw LUN"

test_2 () {
    local rc=0
    local ostmnt=$(facet_mntpt ost1)
    
    run_dumpe2fs ost1 $OSTDEV1

    # Mount the OST as an ldiskfs filesystem.
    log "mount the OST $OSTDEV1 as a ldiskfs filesystem"
    add ost1 $(mkfs_opts ost1) $FSTYPE_OPT --reformat `ostdevname 1` > /dev/null || \
                        error "format ost1 error" 
    do_node $ost_HOST mount -t $FSTYPE $OSTDEV1 $ostmnt "$OST_MOUNT_OPTS"

    # Run llverfs on the mounted ldiskfs filesystem in partial mode to
    # ensure that the kernel can perform filesystem operations on the complete
    # device without any errors.
    log "run llverfs in partial mode on the OST ldiskfs filesystem $ostmnt"
    do_rpc_nodes $(facet_host ost1) run_llverfs $ostmnt -vpl "no" || \
                        error "run_llverfs error on ldiskfs"

    # Unmount the OST.
    log "unmount the OST $OSTDEV1"
    stop ost1

    # After llverfs is run on the ldiskfs filesystem in partial mode,
    # a full e2fsck should be run to catch any errors early.
    $RUN_FSCK && run_e2fsck $ost_HOST $OSTDEV1 "-fy" || error "run e2fsck error" 
    
    if $FULL_MODE; then
        log "full mode,mount the OST $OSTDEV1 as an ldiskfs filesystem again"
        do_node $ost_HOST mount -t $FSTYPE $OSTDEV1 $ostmnt "$OST_MOUNT_OPTS"
        cleanup_dirs ost1 $ostmnt
        do_facet ost1 "sync"
            
        run_dumpe2fs ost1 $OSTDEV1

        # Run llverfs on the mounted ldiskfs filesystem in full mode to
        # ensure that the kernel can perform filesystem operations on
        # the complete device without any errors.
        log "run llverfs in full mode on the OST ldiskfs filesystem $ostmnt"
        do_rpc_nodes $(facet_host ost1) run_llverfs $ostmnt -vl "no" || \
                        error "run_llverfs error on ldiskfs"

        # Unmount the OST.
        log "unmount the OST $OSTDEV1"
        stop ost1

        # After llverfs is run on the ldiskfs filesystem in full mode,
        # a full e2fsck should be run to catch any errors early.
        $RUN_FSCK && run_e2fsck $ost_HOST $OSTDEV1 "-fy" || error "run e2fsck error"
    fi
}
run_test 2 "run llverfs on OST ldiskfs filesystem"

test_3 () {
    # Setup the Lustre filesystem.
    log "setup the lustre filesystem"
    stopall
    formatall
    setupall

    if $RUN_MDSRATE; then
        # Run the mdsrate test suite.
        run_mdsrate
        client_df $MOUNT

        do_facet client sync; sleep 5; sync
        stopall
        $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy" || error "run e2fsck error"
        $RUN_FSCK && run_e2fsck $ost_HOST $OSTDEV1 "-fy" || error "run e2fsck error"
    fi
}
run_test 3 "use up free inodes on the OST with mdsrate"

test_4 () {
    # Setup the Lustre filesystem.
    log "setup the lustre filesystem"
    stopall
    formatall
    setupall

    run_dumpe2fs ost1 $OSTDEV1

    # Run llverfs on the mounted Lustre filesystem both in partial and
    # full mode to to fill the filesystem and verify the file contents.
    rc=0
    log "run llverfs in partial mode on the Lustre filesystem $MOUNT"
    do_rpc_nodes $(facet_host client) run_llverfs $MOUNT -vp "no" || \
                  error "run_llverfs error on lustre"
    client_df $MOUNT
    [ $rc -ne 0 ] && return $rc

    sync; sleep 5; sync
    stopall
    $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy"
    $RUN_FSCK && run_e2fsck $ost_HOST $OSTDEV1 "-fy"

    if $FULL_MODE; then
        # Setup the Lustre filesystem again.
        log "setup the lustre filesystem again"
        setupall

        cleanup_dirs client $MOUNT
        do_facet client "sync"
        client_df $MOUNT
        run_dumpe2fs ost1 $OSTDEV1
        
        log "run llverfs in full mode on the Lustre filesystem $MOUNT"
        do_rpc_nodes $(facet_host client) run_llverfs $MOUNT -vl "no" || \
                    error "run_llverfs error on lustre"
        client_df $MOUNT
        [ $rc -ne 0 ] && return $rc

        sync; sleep 5; sync
        stopall
        $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy"
        $RUN_FSCK && run_e2fsck $ost_HOST $OSTDEV1 "-fy"
    fi

    # Cleanup the Lustre filesystem.
    $CLEANUP && cleanupall
}
run_test 4 "run llverfs on lustre filesystem"

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
