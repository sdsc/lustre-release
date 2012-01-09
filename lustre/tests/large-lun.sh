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
assert_env CLIENTS MDSRATE MPIRUN
init_logging

#Variable to run mdsrate
THREADS_PER_CLIENT=${THREADS_PER_CLIENT:-5}    # thread(s) per client node
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
LOG=${TESTSUITELOG:-$TMP/$(basename $0 .sh).log}
NODES_TO_USE=${NODES_TO_USE:-$CLIENTS}
NUM_CLIENTS=$(get_node_count ${NODES_TO_USE//,/ })

[ ! -x $MDSRATE ] && error "$MDSRATE not built."

#bug number:
ALWAYS_EXCEPT="$LARGE_LUN_EXCEPT"

build_test_filter
stopall
# unmount and cleanup the Lustre filesystem
LARGE_LUN_RESTORE_MOUNT=false
if is_mounted $MOUNT || is_mounted $MOUNT2; then
    cleanupall
    LARGE_LUN_RESTORE_MOUNT=true
fi
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

# Report Lustre filesystem disk space usage and inodes usage of each MDT/OST.
client_df() {
    echo "client_df"
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
    local cmd="time rm -rf $mnt/{*.filecount,dir*}"
    do_facet $target "$cmd"
}

# Run mdsrate.
run_mdsrate() {
    generate_machine_file $NODES_TO_USE $MACHINEFILE || \
        error "can not generate machinefile"

    # set the default stripe count for files in this test to one
    TESTDIR=$MOUNT/mdsrate
    mkdir -p $TESTDIR
    chmod 0777 $TESTDIR
    $LFS setstripe $TESTDIR -i 0 -c 1
    get_stripe $TESTDIR

    # make sure we start with a clean slate
    rm -f $LOG

    local num_dirs=$THREADS_PER_CLIENT
    [ num_dirs == 0 ] || num_dirs=1
    local free_inodes=$(lfs df -i $MOUNT | grep "OST:0" | awk '{print $4}')
    local num_files
    num_files=$((free_inodes / num_dirs))

    COMMAND="$MDSRATE $MDSRATE_DEBUG --create --verbose \
        --ndirs $num_dirs --dirfmt '$TESTDIR/dir%d' \
        --nfiles $num_files --filefmt 'file%%d'"

    echo "# $COMMAND"
    mpi_run -np $((NUM_CLIENTS * THREADS_PER_CLIENT)) -machinefile $MACHINEFILE \
        $COMMAND 2>&1 | tee $LOG

    if [ ${PIPESTATUS[0]} != 0 ]; then
    #   [ -f $LOG ] && sed -e "s/^/log: /" $LOG
        error "mdsrate create failed"
    fi
}

################################## Main Flow ###################################
trap cleanupall EXIT

test_1 () {
    local dev
    for num in `seq $OSTCOUNT`; do
        dev=$(ostdevname $num)
        log "run llverdev on the OST $dev"
        do_rpc_nodes $(facet_host ost${num}) run_llverdev $dev -vpf
        [ $? -eq 0 ] || error "llverdev failed!"
    done
}
run_test 1 "run llverdev on raw LUN"

test_2 () {
    local rc=0
    local dev
    local ostmnt

    for num in `seq $OSTCOUNT`; do
        dev=$(ostdevname $num)
        ostmnt=$(facet_mntpt ost${num})
        run_dumpe2fs ost$num $dev

        # Mount the OST as an ldiskfs filesystem.
        log "mount the OST $dev as a ldiskfs filesystem"
        add ost$num $(mkfs_opts ost${num}) $FSTYPE_OPT --reformat `ostdevname $num` > /dev/null || \
                        error "format ost$num error"
        do_facet ost$num mount -t $FSTYPE $dev $ostmnt "$OST_MOUNT_OPTS"

        # Run llverfs on the mounted ldiskfs filesystem in partial mode to
        # ensure that the kernel can perform filesystem operations on the complete
        # device without any errors.
        log "run llverfs in partial mode on the OST ldiskfs filesystem $ostmnt"
        do_rpc_nodes $(facet_host ost${num}) run_llverfs $ostmnt -vpl "no" || \
                        error "run_llverfs error on ldiskfs"

        # Unmount the OST.
        log "unmount the OST $dev"
        stop ost$num

        # After llverfs is run on the ldiskfs filesystem in partial mode,
        # a full e2fsck should be run to catch any errors early.
        $RUN_FSCK && run_e2fsck $(facet_host ost${num}) $dev "-fy" || error "run e2fsck error"

        if $FULL_MODE; then
            log "full mode,mount the OST $dev as an ldiskfs filesystem again"
            do_facet ost$num mount -t $FSTYPE $dev $ostmnt "$OST_MOUNT_OPTS"
            cleanup_dirs ost$num $ostmnt
            do_facet ost$num "sync"

            run_dumpe2fs ost$num $dev

            # Run llverfs on the mounted ldiskfs filesystem in full mode to
            # ensure that the kernel can perform filesystem operations on
            # the complete device without any errors.
            log "run llverfs in full mode on the OST ldiskfs filesystem $ostmnt"
            do_rpc_nodes $(facet_host ost${num}) run_llverfs $ostmnt -vl "no" || \
                        error "run_llverfs error on ldiskfs"

            # Unmount the OST.
            log "unmount the OST $dev"
            stop ost$num

            # After llverfs is run on the ldiskfs filesystem in full mode,
            # a full e2fsck should be run to catch any errors early.
            $RUN_FSCK && run_e2fsck $(facet_host ost${num}) $dev "-fy" || error "run e2fsck error"
        fi
    done
}
run_test 2 "run llverfs on OST ldiskfs filesystem"

test_3 () {
    # Setup the Lustre filesystem.
    log "setup the lustre filesystem"
    stopall
    formatall
    setupall

    log "run mdsrate to use up the free inodes."
    # Run the mdsrate test suite.
    run_mdsrate
    client_df $MOUNT

    do_facet client sync; sleep 5; sync
    stopall
    $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy" || error "run e2fsck error"
    if [ "$RUN_FSCK" = "true" ]; then
        for num in `seq $OSTCOUNT`; do
            dev=$(ostdevname $num)
            run_e2fsck $(facet_host ost${num}) $dev "-fy" || error "run e2fsck error"
        done
    fi
}
run_test 3 "use up free inodes on the OST with mdsrate"

test_4 () {
    # Setup the Lustre filesystem.
    log "setup the lustre filesystem"
    stopall
    formatall
    setupall
    local dev

    for num in `seq $OSTCOUNT`; do
        dev=$(ostdevname $num)
        run_dumpe2fs ost$num $dev
    done

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
    $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy" || error "run e2fsck error"
    if [ "$RUN_FSCK" = "true" ]; then
        for num in `seq $OSTCOUNT`; do
            dev=$(ostdevname $num)
            run_e2fsck $(facet_host ost${num}) $dev "-fy" || error "run e2fsck error"
        done
    fi


    if $FULL_MODE; then
        # Setup the Lustre filesystem again.
        log "setup the lustre filesystem again"
        setupall

        cleanup_dirs client $MOUNT
        do_facet client "sync"
        client_df $MOUNT
        
        for num in `seq $OSTCOUNT`; do
            dev=$(ostdevname $num)
            run_dumpe2fs ost$num $dev
        done

        log "run llverfs in full mode on the Lustre filesystem $MOUNT"
        do_rpc_nodes $(facet_host client) run_llverfs $MOUNT -vl "no" || \
                    error "run_llverfs error on lustre"
        client_df $MOUNT
        [ $rc -ne 0 ] && return $rc

        sync; sleep 5; sync
        stopall
        $RUN_FSCK && run_e2fsck $mds_HOST $MDSDEV "-fy" || error "run e2fsck error"
        if [ "$RUN_FSCK" = "true" ]; then
            for num in `seq $OSTCOUNT`; do
                dev=$(ostdevname $num)
                run_e2fsck $(facet_host ost${num}) $dev "-fy" || error "run e2fsck error"
            done
        fi
    fi
}
run_test 4 "run llverfs on lustre filesystem"

complete $(basename $0) $SECONDS
$LARGE_LUN_RESTORE_MOUNT && setupall
check_and_cleanup_lustre
exit_status
