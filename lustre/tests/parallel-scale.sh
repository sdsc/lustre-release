#!/bin/bash
#
#set -vx

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

# bug number:
ALWAYS_EXCEPT="$PARALLEL_SCALE_EXCEPT"

# common setup
#
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
clients=${CLIENTS:-$HOSTNAME}
generate_machine_file $clients $MACHINEFILE || \
    error "Failed to generate machine file"
num_clients=$(get_node_count ${clients//,/ })

#
# compilbench
#
cbench_DIR=${cbench_DIR:-""}
cbench_IDIRS=${cbench_IDIRS:-4}
# FIXME: wiki page requirements is 30, do we really need 30 ?
cbench_RUNS=${cbench_RUNS:-4}

if [ "$SLOW" = "no" ]; then
    cbench_IDIRS=2
    cbench_RUNS=2
fi

#
# metabench
#
METABENCH=${METABENCH:-$(which metabench 2> /dev/null || true)}
mbench_NFILES=${mbench_NFILES:-30400}
[ "$SLOW" = "no" ] && mbench_NFILES=10000
# threads per client
mbench_THREADS=${mbench_THREADS:-4}

#
# simul
#
SIMUL=${SIMUL:=$(which simul 2> /dev/null || true)}
# threads per client
simul_THREADS=${simul_THREADS:-2}
simul_REP=${simul_REP:-20}
[ "$SLOW" = "no" ] && simul_REP=2

#
# mib
#
MIB=${MIB:=$(which mib 2> /dev/null || true)}
# threads per client
mib_THREADS=${mib_THREADS:-2}
mib_xferSize=${mib_xferSize:-1m}
mib_xferLimit=${mib_xferLimit:-5000}
mib_timeLimit=${mib_timeLimit:-300}

#
# MDTEST
#
MDTEST=${MDTEST:=$(which mdtest 2> /dev/null || true)}
# threads per client
mdtest_THREADS=${mdtest_THREADS:-2}
mdtest_nFiles=${mdtest_nFiles:-"100000"}
# We devide the files by number of core
mdtest_nFiles=$((mdtest_nFiles/mdtest_THREADS/num_clients))
mdtest_iteration=${mdtest_iteration:-1}

#
# connectathon
#
cnt_DIR=${cnt_DIR:-""}
cnt_NRUN=${cnt_NRUN:-10}
[ "$SLOW" = "no" ] && cnt_NRUN=2

#
# cascading rw
#
CASC_RW=${CASC_RW:-$(which cascading_rw 2> /dev/null || true)}
# threads per client
casc_THREADS=${casc_THREADS:-2}
casc_REP=${casc_REP:-300}
[ "$SLOW" = "no" ] && casc_REP=10

#
# IOR
#
IOR=${IOR:-$(which IOR 2> /dev/null || true)}
# threads per client
ior_THREADS=${ior_THREADS:-2}
ior_iteration=${ior_iteration:-1}
ior_blockSize=${ior_blockSize:-6}	# Gb
ior_xferSize=${ior_xferSize:-2m}
ior_type=${ior_type:-POSIX}
ior_DURATION=${ior_DURATION:-30}	# minutes
[ "$SLOW" = "no" ] && ior_DURATION=5

#
# write_append_truncate
#
# threads per client
write_THREADS=${write_THREADS:-8}
write_REP=${write_REP:-10000}
[ "$SLOW" = "no" ] && write_REP=100

#
# write_disjoint
#
WRITE_DISJOINT=${WRITE_DISJOINT:-$(which write_disjoint 2> /dev/null || true)}
# threads per client
wdisjoint_THREADS=${wdisjoint_THREADS:-4}
wdisjoint_REP=${wdisjoint_REP:-10000}
[ "$SLOW" = "no" ] && wdisjoint_REP=100

#
# parallel_grouplock
#
#
PARALLEL_GROUPLOCK=${PARALLEL_GROUPLOCK:-\
    $(which parallel_grouplock 2> /dev/null || true)}
parallel_grouplock_MINTASKS=${parallel_grouplock_MINTASKS:-5}

. $LUSTRE/tests/functions.sh

build_test_filter
check_and_setup_lustre

test_compilebench() {
    run_compilebench
}
run_test compilebench "compilebench"

test_metabench() {
    run_metabench
}
run_test metabench "metabench"

test_simul() {
    run_simul
}
run_test simul "simul"

test_mdtestssf() {
    test_mdtest "ssf"
}
run_test mdtestssf "mdtestssf"

test_mdtestfpp() {
    test_mdtest "fpp"
}
run_test mdtestfpp "mdtestfpp"

test_connectathon() {
    run_connectathon
}
run_test connectathon "connectathon"

test_iorssf() {
    test_ior "ssf"
}
run_test iorssf "iorssf"

test_iorfpp() {
    test_ior "fpp"
}
run_test iorfpp "iorfpp"

test_mib() {
    run_mib
}
run_test mib "mib"

test_cascading_rw() {
    run_cascading_rw
}
run_test cascading_rw "cascading_rw"

test_write_append_truncate() {
    run_write_append_truncate
}
run_test write_append_truncate "write_append_truncate"

test_write_disjoint() {
    run_write_disjoint
}
run_test write_disjoint "write_disjoint"

test_parallel_grouplock() {
    run_parallel_grouplock
}
run_test parallel_grouplock "parallel_grouplock"

statahead_NUMMNTPTS=${statahead_NUMMNTPTS:-5}
statahead_NUMFILES=${statahead_NUMFILES:-500000}

cleanup_statahead () {
    trap 0

    local clients=$1
    local mntpt_root=$2
    local num_mntpts=$3

    for i in $(seq 0 $num_mntpts);do
        zconf_umount_clients $clients ${mntpt_root}$i ||
            error_exit "Failed to umount lustre on ${mntpt_root}$i"
    done
}

test_statahead () {
    run_statahead
}
run_test statahead "statahead test, multiple clients"

# bug 17764 accessing files via nfs,
# ASSERTION(!mds_inode_is_orphan(dchild->d_inode)) failed
test_nfsread_orphan_file() {
    if [ ! "$NFSCLIENT" ]; then
        skip "not NFSCLIENT mode, skipped"
        return
    fi

    # copy file to lustre server
    local nfsserver=$(nfs_server $MOUNT)
    do_nodev $nfsserver cp /etc/passwd $DIR/$tfile
    zconf_mount $nfsserver $MOUNT2

    # open, wait, unlink and close
    rmultiop_start --uniq unlink $nfsserver $DIR/$tfile o_uc
    echo "1. unlinker on NFS server $nfsserver opened the file $DIR/$tfile"
    sleep 1

    # open $DIR2/$tfile and wait
    rmultiop_start --uniq open $nfsserver $DIR2/$tfile o_c
    echo "2. open on NFS server $nfsserver opened the file $DIR2/$tfile"
    sleep 1

    # open $DIR/$tfile on nfs client, wait, read
    multiop_bg_pause $DIR/$tfile o_r10c
    NFSREADPID=$!
    echo "3. NFS client readder opened the file $DIR/$tfile"
    sleep 1

    # let unlink to go
    rmultiop_stop --uniq unlink $nfsserver
    echo "4. unlink, close completed"
    sleep 1

    # let nfs read to go
    kill -USR1 $NFSREADPID
    echo "5. NFS client read completed"

    wait $NFSREADPID

    rmultiop_stop --uniq open $nfsserver
    zconf_umount $nfsserver $MOUNT2
}
run_test nfsread_orphan_file "accessing files via nfs, bug 17764"

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
