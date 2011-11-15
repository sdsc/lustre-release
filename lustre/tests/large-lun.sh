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

cleanupall

TMP=${TMP:-/tmp}

LARGE_LUN="yes"
#All target will be on LUN_HOST for this test
LUN_NODE=${LUN_NODE:-ost}
LUN_HOST=${LUN_HOST:-$ost_HOST}
mds_HOST=$LUN_HOST
mdsfailover_HOST=$LUN_HOST
mds1_HOST=$LUN_HOST
mds1failover_HOST=$LUN_HOST
mgs_HOST=$LUN_HOST
ost_HOST=$LUN_HOST
ostfailover_HOST=$LUN_HOST
MGSNID=$(h2$NETTYPE $mgs_HOST)

CREATE_LUN=${CREATE_LUN:-true}
FORMAT=${FORMAT:-true}
SETUP=${SETUP:-true}
CLEANUP=${CLEANUP:-true}
FULL_MODE=${FULL_MODE:-true}
VERIFY_LDISKFS=${VERIFY_LDISKFS:-true}
RUN_FSCK=${RUN_FSCK:-true}
RUN_ACCSM=${RUN_ACCSM:-true}
RUN_LLVERDEV=${RUN_LLVERDEV:-true}
RUN_LLVERFS=${RUN_LLVERFS:-true}
RUN_MMP=${RUN_MMP:-true}

ACC_SM_ONLY=${ACC_SM_ONLY:-"sanity"}
ACC_SM_EXCEPT=${ACC_SM_EXCEPT:-""}
THREADS_PER_CLIENT=${THREADS_PER_CLIENT:-25}
SIZE_400M=419430400
SIZE_16T=167772160000
LARGE_DISKCOUNT=${LARGE_DISKCOUNT:-2}
LARGE_DISKBASE=${LARGE_DISKBASE:-"$TMP/large_disks"}
LARGE_DISKS=${LARGE_DISKS:-""}
LARGE_LOOPDEVS=${LARGE_LOOPDEVS:-""}

VG_NAME=${VG_NAME:-"large_vg"}

MDT_LV=${MDT_LV:-"mdt_lv"}
MGS_LV=${MGS_LV:-"mgs_lv"}
export MDT1_LUN_DEV=/dev/$VG_NAME/$MDT_LV
export MDT_LUN_DEV=$MDT1_LUN_DEV

OST1_LV=${OST1_LV:-"ost1_lv"}
OST2_LV=${OST2_LV:-"ost2_lv"}
OST3_LV=${OST3_LV:-"ost3_lv"}
OST4_LV=${OST4_LV:-"ost4_lv"}
OST5_LV=${OST5_LV:-"ost5_lv"}
OST6_LV=${OST6_LV:-"ost6_lv"}
OST_LV=$OST1_LV

export OST1_LUN_DEV=/dev/$VG_NAME/$OST1_LV
export OST2_LUN_DEV=/dev/$VG_NAME/$OST2_LV
export OST3_LUN_DEV=/dev/$VG_NAME/$OST3_LV
export OST4_LUN_DEV=/dev/$VG_NAME/$OST4_LV
export OST5_LUN_DEV=/dev/$VG_NAME/$OST5_LV
export OST6_LUN_DEV=/dev/$VG_NAME/$OST6_LV
export OST_LUN_DEV=$OST1_LUN_DEV

OST_LUN_SIZE=${OST_LUN_SIZE:-"16G"}
MDT_LUN_SIZE=${MDT_LUN_SIZE:-"10G"}
MGS_LUN_SIZE=${MGS_LUN_SIZE:-"1G"}

OST_MNT=${OST_MNT:-/mnt/ost1}
#########################################################################

add_output_marker() {
    local MSG="$*"

    local asterisks="================================================================================"
    local length=$(( (78 - ${#MSG}) / 2))

    MSG="${asterisks:0:$length} $MSG ${asterisks:0:$length}"

    echo -e "\n$MSG"

    lsmod | grep -q lnet || modprobe lnet
    lctl mark "$MSG" 2> /dev/null || true
}


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

get_loopbase() {
    local LOOPBASE
    do_facet $target "ls /dev/loop/0 > /dev/null 2>&1" && LOOPBASE=/dev/loop/
    do_facet $target "ls /dev/loop0 > /dev/null 2>&1" && LOOPBASE=/dev/loop
    echo -n $LOOPBASE
}

# Cleanup device
cleanup_pv() {
    local target=$1
    # Remove the old LV/VG/PV.
    echo "LV remove $VG_NAME" 
    do_facet $target "lvremove -f $VG_NAME > /dev/null 2>&1 || true"
    echo "VG remove $VG_NAME" 
    do_facet $target "vgremove $VG_NAME > /dev/null 2>&1 || true"
    echo "PV remove $LARGE_DISKS" 
    do_facet $target "pvremove $LARGE_DISKS > /dev/null 2>&1 || true"

    # Cleanup loopdev
    LOOPBASE=$(get_loopbase)
    [ -z "$LOOPBASE" ] && echo "/dev/loop/0 and /dev/loop0 gone?" && return
    # Erase the partition table by zeroing the first sector of the whole disk.
    for num in `seq $LARGE_DISKCOUNT`; do
	local diskname=${LARGE_DISKBASE}${num}
	local loopname=${LOOPBASE}${num}
    	do_facet $target "dd if=/dev/zero of=$diskname bs=512 count=1 > /dev/null 2>&1"
    	[ -z "$diskname" ] || do_facet $target "losetup -d $loopname 2>&1"
    done
}

# Create PVs.
create_pv() {
    local target=$1
    local diskname
    local loopname

    # Setup loopdev 
    LOOPBASE=$(get_loopbase)
    [ -z "$LOOPBASE" ] && echo "/dev/loop/0 and /dev/loop0 gone?" && return

    for num in `seq $LARGE_DISKCOUNT`; do
        # Disk to be used
	diskname=${LARGE_DISKBASE}${num}
        LARGE_DISKS="$LARGE_DISKS $diskname"
        # Create disks
    	do_facet $target "dd if=/dev/zero of=$diskname bs=${SIZE_400M} count=1 > /dev/null 2>&1"
    	do_facet $target "$TRUNCATE $diskname ${SIZE_16T}"
        # Setup loopdev
        loopname=${LOOPBASE}${num}
        for i in `seq 1 7`; do
            do_facet $target "losetup $loopname $diskname" 2>&1 || continue
            LARGE_LOOPDEVS="$LARGE_LOOPDEVS $loopname"
            break
        done 
    done

    # Create PVs 
    [ -z "$LARGE_LOOPDEVS" ] && error "couldn't find empty loop device"
    do_facet $target pvcreate -f $LARGE_LOOPDEVS ||	\
	           error "could not pvcreate $LARGE_LOOPDEVS"
}

# Create VG.
create_vg() {
    local target=$1

    [ -z "$LARGE_LOOPDEVS" ] && error "couldn't find empty loop device"
    echo "$VG_NAME $LARGE_LOOPDEVS"
    do_facet $target vgcreate $VG_NAME $LARGE_LOOPDEVS
    do_facet $target vgdisplay $VG_NAME
}

# Create LVs.
create_lv() {
    local target=$1
    if ! combined_mgs_mds ; then
        do_facet $target lvcreate -L $MGS_LUN_SIZE -n $MGS_LV $VG_NAME
    fi

    do_facet $target lvcreate -L $MDT_LUN_SIZE -n $MDT_LV $VG_NAME
    for num in `seq $OSTCOUNT`; do
        lu_name=OST${num}_LV
        do_facet $target lvcreate -L $OST_LUN_SIZE -n ${!lu_name} $VG_NAME
    done
    do_facet $target lvdisplay $VG_NAME
}

# llverdev - verify a block device is functioning properly over its full size.
# Run llverdev on the raw device in partial mode to ensure that there are no
# driver/LVM bugs.
run_llverdev() {
    local target=$1
    local dev=$2

    local cmd="time llverdev -vpf $dev"
    echo "# $cmd"
    do_facet $target "eval $cmd"
}

# Create LUN
create_lun () {
    local target=$1

    add_output_marker "create the PVs"
    create_pv $target
    add_output_marker "create the VG"
    create_vg $target
    add_output_marker "create the LVs"
    create_lv $target

    if $RUN_LLVERDEV; then
        add_output_marker "run llverdev on the OST $OST1_LUN_DEV"
        run_llverdev $target $OST1_LUN_DEV
    fi
}

# Dump the super block information for the filesystem present on device.
run_dumpe2fs() {
    local dev=$1

    add_output_marker "dump the super block information on $dev"
    local cmd="dumpe2fs -h $dev"
    echo "# $cmd"
    do_facet $LUN_NODE "eval $cmd"
}

# Mount the MDT/OST.
mount_target() {
    local type=$1
    shift
    local dev=$1
    shift
    local mnt_pnt=$1
    shift
    local opts="$@"

    do_facet $LUN_NODE "grep -q " $mnt_pnt " /proc/mounts" && return 0
    do_facet $LUN_NODE "mkdir -p $mnt_pnt"
    local cmd="mount -t $type $opts $dev $mnt_pnt"
    echo -e "\n# $cmd"
    do_facet $LUN_NODE "eval $cmd"
    do_facet $LUN_NODE "mount | grep " $mnt_pnt ""
    do_facet $LUN_NODE "df -h $mnt_pnt"
}

# Unmount the Client/MDT/OST.
unmount_target() {
    local mnt_pnt=$1

    do_facet $LUN_NODE "grep -q " $mnt_pnt " /proc/mounts" || return 0

    local cmd="umount $mnt_pnt"
    echo "# $cmd"
    do_facet $LUN_NODE "eval $cmd"
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

# llverfs - filesystem verification tool.
run_llverfs() {
    local target=$1
    local mnt_pnt=$2
    shift
    local opts="$@"
    local log

    local cmd="time llverfs $opts $mnt_pnt"
    echo "# $cmd"
    do_facet $target "eval $cmd" || {
        log=/tmp/llverfs.debug_log.$(date +%s)
        echo "Dumping lctl debug log to $log"
        do_facet $target lctl dk > $log
    }

    return ${PIPESTATUS[0]}
}

# Cleanup the directories and files created by llverfs utility.
cleanup_dirs() {
    local target=$1
    local mnt=${2:-$MOUNT}
    local cmd="time rm -rf $mnt/{*.filecount,dir*}"
    echo -e "\n# $cmd"
    do_facet $target "eval $cmd"
}

# e2fsck - check the on-disk filesystem structure.
run_e2fsck() {
    local dev=$1
    shift
    local opts=${@:-"-fy -tt"}

    add_output_marker "run e2fsck on device $dev"
    local cmd="time e2fsck $opts $dev"
    echo "# $cmd"
    do_facet $LUN_NODE "eval $cmd" || true
}

# Enable the MMP feature.
enable_mmp() {
    local dev=$1

    add_output_marker "enable the MMP feature on $dev"
    local cmd="time tune2fs -O mmp $dev"
    echo "# $cmd"
    do_facet $LUN_NODE "eval $cmd"
}

# Disable the MMP feature.
disable_mmp() {
    local dev=$1

    add_output_marker "disable the MMP feature on $dev"
    local cmd="time tune2fs -O ^mmp $dev"
    echo "# $cmd"
    do_facet $LUN_NODE "eval $cmd"
}

# Check whether the MMP feature is enabled or not.
mmp_is_enabled() {
    local dev=$1

    local cmd="dumpe2fs -h $dev 2>&1 | grep mmp"
    echo -e "\n# $cmd"
    do_facet $LUN_NODE "eval $cmd"
}

# Run acc-sm tests.
run_acc_sm() {
    local cmd

    add_output_marker "run tests: $ACC_SM_ONLY"
    
    cmd="$cmd MDSSIZE=$(size_in_KB $MDT_LUN_SIZE) mds_HOST=$LUN_HOST ost_HOST=$LUN_HOST mgs_HOST=$LUN_HOST"
    cmd="$cmd OSTSIZE=$(size_in_KB $OST_LUN_SIZE)"
    cmd="$cmd SETUP=: CLEANUP=: SLOW=$SLOW LARGE_LUN='yes'"

    [ -n "$ACC_SM_ONLY" ] && cmd="$cmd ACC_SM_ONLY='$ACC_SM_ONLY'"
    [ -z "$ONLY" ] && cmd="$cmd $ACC_SM_EXCEPT" || cmd="$cmd ONLY='$ONLY'"
    [[ " $ACC_SM_ONLY " = *\ obdfilter-survey\ * ]] && cmd="$cmd size=65536"

    if [[ " $ACC_SM_ONLY " = *\ mdsrate-create\ * ]]; then
        local num_dirs=$THREADS_PER_CLIENT
        local free_inodes=$(lfs df -i $MOUNT | grep "OST:0" | awk '{print $4}')
        local num_files
        [ num_dirs == 0 ] || num_dirs=1
        num_files=$((free_inodes / num_dirs))
        cmd="$cmd NUM_DIRS=$num_dirs NUM_FILES=$num_files"
        cmd="$cmd THREADS_PER_CLIENT=$THREADS_PER_CLIENT"
        cmd="$cmd bash mdsrate-create.sh"
    else
        cmd="$cmd bash acceptance-small.sh"
    fi

    echo -e "# $cmd\n"
    do_facet client "eval $cmd"
}

verify_ldiskfs () {
    run_dumpe2fs $OST_LUN_DEV

    # Mount the OST as an ldiskfs filesystem.
    add_output_marker "mount the OST $OST_LUN_DEV as an ldiskfs filesystem"
    mount_target ldiskfs $OST_LUN_DEV $OST_MNT "$OST_MOUNT_OPTS"

    # Run llverfs on the mounted ldiskfs filesystem in partial mode to
    # ensure that the kernel can perform filesystem operations on the complete
    # device without any errors.
    add_output_marker \
        "run llverfs in partial mode on the OST ldiskfs filesystem $OST_MNT"
    run_llverfs $LUN_NODE $OST_MNT -vpl

    # Unmount the OST.
    add_output_marker "unmount the OST $OST_LUN_DEV"
    unmount_target $OST_MNT

    # After llverfs is run on the ldiskfs filesystem in partial mode,
    # a full e2fsck should be run to catch any errors early.
    $RUN_FSCK && run_e2fsck $OST_LUN_DEV

    add_output_marker "mount the OST $OST_LUN_DEV as an ldiskfs filesystem again"
    mount_target ldiskfs $OST_LUN_DEV $OST_MNT "$OST_MOUNT_OPTS"

    cleanup_dirs $LUN_NODE $OST_MNT
    do_facet $LUN_NODE "sync"
    if $FULL_MODE; then
        run_dumpe2fs $OST_LUN_DEV

        # Run llverfs on the mounted ldiskfs filesystem in full mode to
        # ensure that the kernel can perform filesystem operations on
        # the complete device without any errors.
        add_output_marker \
            "run llverfs in full mode on the OST ldiskfs filesystem $OST_MNT"
        run_llverfs $LUN_NODE $OST_MNT -vl
    fi

    # Unmount the OST.
    add_output_marker "unmount the OST $OST_LUN_DEV"
    unmount_target $OST_MNT

    if $FULL_MODE; then
        # After llverfs is run on the ldiskfs filesystem in full mode,
        # a full e2fsck should be run to catch any errors early.
        $RUN_FSCK && run_e2fsck $OST_LUN_DEV

        add_output_marker "reformat the MDT $MDT_LUN_DEV OST $OST_LUN_DEV"
        formatall
    fi
}

do_llverfs () {
    run_dumpe2fs $OST_LUN_DEV

    # Run llverfs on the mounted Lustre filesystem both in partial and
    # full mode to to fill the filesystem and verify the file contents.
    rc=0
    add_output_marker \
        "run llverfs in partial mode on the Lustre filesystem $MOUNT"
    run_llverfs client $MOUNT -vp || rc=${PIPESTATUS[0]}
    client_df $MOUNT
    [ $rc -ne 0 ] && exit $rc

    sync; sleep 5; sync
    cleanupall
    $RUN_FSCK && run_e2fsck $MDT_LUN_DEV
    $RUN_FSCK && run_e2fsck $OST_LUN_DEV

    if $FULL_MODE; then
        #add_output_marker "reformat the OST $OST_LUN_DEV"
        #format_ost

        # Setup the Lustre filesystem again.
        setupall

        cleanup_dirs client $MOUNT
        do_facet client sync
        client_df $MOUNT
        run_dumpe2fs $OST_LUN_DEV

        add_output_marker \
            "run llverfs in full mode on the Lustre filesystem $MOUNT"
        run_llverfs client $MOUNT -vl || rc=${PIPESTATUS[0]}
        client_df $MOUNT
        [ $rc -ne 0 ] && exit $rc

        sync; sleep 5; sync
        cleanupall
        $RUN_FSCK && run_e2fsck $MDT_LUN_DEV
        $RUN_FSCK && run_e2fsck $OST_LUN_DEV
    fi
}

cleanup_lun() {
    cleanupall
    cleanup_pv $LUN_NODE
}

################################## Main Flow ###################################
test_1 () {
    add_output_marker "test started at: $(date)"
    STARTTIME=$(date +%s)

    cleanup_pv $LUN_NODE
    if $CREATE_LUN; then
        create_lun $LUN_NODE
    fi

    if $FORMAT; then
        add_output_marker "format the MDT $MDT_LUN_DEV OST $OST_LUN_DEV"
        formatall
    fi

    trap cleanup_lun EXIT

    if $VERIFY_LDISKFS; then
        verify_ldiskfs
    fi

    formatall
    # Setup the Lustre filesystem.
    $SETUP && setupall

    if $RUN_ACCSM; then
        # Run the acceptance-small test suite.
        run_acc_sm
        client_df $MOUNT
        
        do_facet client sync; sleep 5; sync
        cleanupall
        $RUN_FSCK && run_e2fsck $MDT_LUN_DEV
        $RUN_FSCK && run_e2fsck $OST_LUN_DEV

        add_output_marker "format the MDT $MDT_LUN_DEV OST $OST1_LUN_DEV"
        formatall
        # Setup the Lustre filesystem again.
        setupall
    fi

    if $RUN_LLVERFS; then
        do_llverfs
    fi

    trap - EXIT

    # Cleanup the Lustre filesystem.
    $CLEANUP && cleanupall

    if $RUN_MMP; then
        enable_mmp $OST_LUN_DEV
        mmp_is_enabled $OST_LUN_DEV
        setupall
        cleanupall
        disable_mmp $OST_LUN_DEV
        mmp_is_enabled $OST_LUN_DEV || true
    fi

    cleanup_pv $LUN_NODE

    add_output_marker "test duration: $(($(date +%s) - $STARTTIME))s"
    add_output_marker "test stopped at: $(date)"
}
run_test 1 "large lun"

complete $(basename $0) $SECONDS
exit_status
