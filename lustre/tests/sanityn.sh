#!/bin/bash
#
#set -vx

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

. $LUSTRE/tests/setup-nfs.sh
# first unmount all the lustre client
cleanup_mount $MOUNT
# mount lustre on mds
lustre_client=$(facet_host $(get_facets MDS) | tail -1)
zconf_mount_clients $lustre_client $MOUNT "-o user_xattr,acl,flock,32bitapi"

# setup the nfs
setup_nfs "4" "$MOUNT" "$lustre_client" "$CLIENTS"

export NFSCLIENT=yes
export FAIL_ON_ERROR=false

sh $LUSTRE/tests/parallel-scale.sh

# cleanup nfs
cleanup_nfs "$MOUNT" "$lustre_client" "$CLIENTS"

zconf_umount_clients $lustre_client $MOUNT
zconf_mount_clients $CLIENTS $MOUNT

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
# common setup
#
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
clients=${CLIENTS:-$HOSTNAME}
generate_machine_file $clients $MACHINEFILE || error "Failed to generate machine file"
num_clients=$(get_node_count ${clients//,/ })


# compilbench
#
cbench_DIR=${cbench_DIR:-""}
cbench_IDIRS=${cbench_IDIRS:-4}
cbench_RUNS=${cbench_RUNS:-4}	# FIXME: wiki page requirements is 30, do we really need 30 ?

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
# connectathon
#
cnt_DIR=${cnt_DIR:-""}
cnt_NRUN=${cnt_NRUN:-10}
[ "$SLOW" = "no" ] && cnt_NRUN=2

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

build_test_filter
check_and_setup_lustre

get_mpiuser_id $MPI_USER
MPI_RUNAS=${MPI_RUNAS:-"runas -u $MPI_USER_UID -g $MPI_USER_GID"}
$GSS_KRB5 && refresh_krb5_tgt $MPI_USER_UID $MPI_USER_GID $MPI_RUNAS


print_opts () {
    local var

    echo OPTIONS:

    for i in $@; do
        var=$i
        echo "${var}=${!var}"
    done
    [ -e $MACHINEFILE ] && cat $MACHINEFILE
}

# Takes:
# 5 min * cbench_RUNS
#        SLOW=no     10 mins
#        SLOW=yes    50 mins
# Space estimation:
#        compile dir kernel-1 680MB
#        required space       680MB * cbench_IDIRS = ~7 Gb

test_compilebench() {
    print_opts cbench_DIR cbench_IDIRS cbench_RUNS

    [ x$cbench_DIR = x ] &&
        { skip_env "compilebench not found" && return; }

    [ -e $cbench_DIR/compilebench ] || \
        { skip_env "No compilebench build" && return; }

    local space=$(df -P $DIR | tail -n 1 | awk '{ print $4 }')
    if [ $space -le $((680 * 1024 * cbench_IDIRS)) ]; then
        cbench_IDIRS=$(( space / 680 / 1024))
        [ $cbench_IDIRS = 0 ] && \
            skip_env "Need free space atleast 680 Mb, have $space" && return

        log free space=$space, reducing initial dirs to $cbench_IDIRS
    fi
    # FIXME:
    # t-f _base needs to be modifyed to set properly tdir
    # for new "test_foo" functions names
    # local testdir=$DIR/$tdir
    local testdir=$DIR/d0.compilebench
    mkdir -p $testdir

    local savePWD=$PWD
    cd $cbench_DIR
    local cmd="./compilebench -D $testdir -i $cbench_IDIRS -r $cbench_RUNS --makej"

    log "$cmd"

    local rc=0
    eval $cmd
    rc=$?

    cd $savePWD
    [ $rc = 0 ] || error "compilebench failed: $rc"
    rm -rf $testdir
}
run_test compilebench "compilebench"

test_metabench() {
    [ x$METABENCH = x ] &&
        { skip_env "metabench not found" && return; }

    # FIXME
    # Need space estimation here.

    print_opts METABENCH clients mbench_NFILES mbench_THREADS

    local testdir=$DIR/d0.metabench
    mkdir -p $testdir
    # mpi_run uses mpiuser
    chmod 0777 $testdir

    # -C             Run the file creation tests.
    # -S             Run the file stat tests.
    # -c nfile       Number of files to be used in each test.
    # -k             Cleanup.  Remove the test directories.
    local cmd="$METABENCH -w $testdir -c $mbench_NFILES -C -S -k"
    echo "+ $cmd"

    # find out if we need to use srun by checking $SRUN_PARTITION
    if [ "$SRUN_PARTITION" ]; then
        $SRUN $SRUN_OPTIONS -D $testdir -w $clients -N $num_clients \
            -n $((num_clients * mbench_THREADS)) -p $SRUN_PARTITION -- $cmd
    else
        mpi_run -np $((num_clients * $mbench_THREADS)) -machinefile ${MACHINEFILE} $cmd
    fi

    local rc=$?
    if [ $rc != 0 ] ; then
        error "metabench failed! $rc"
    fi
    rm -rf $testdir
}
run_test metabench "metabench"

test_connectathon() {
    print_opts cnt_DIR cnt_NRUN

    [ x$cnt_DIR = x ] &&
        { skip_env "connectathon dir not found" && return; }

    [ -e $cnt_DIR/runtests ] || \
        { skip_env "No connectathon runtests found" && return; }

    local testdir=$DIR/d0.connectathon
    mkdir -p $testdir

    local savePWD=$PWD
    cd $cnt_DIR

    #
    # cthon options (must be in this order)
    #
    # -N numpasses - will be passed to the runtests script.  This argument
    #         is optional.  It specifies the number of times to run
    #         through the tests.
    #
    # One of these test types
    #    -b  basic
    #    -g  general
    #    -s  special
    #    -l  lock
    #    -a  all of the above
    #
    # -f      a quick functionality test
    #

    tests="-b -g -s"
    # Include lock tests unless we're running on nfsv4
    local fstype=$(df -TP $testdir | awk 'NR==2  {print $2}')
    echo "$testdir: $fstype"
    if [[ $fstype != "nfs4" ]]; then
        tests="$tests -l"
    fi
    echo "tests: $tests"
    for test in $tests; do
        local cmd="./runtests -N $cnt_NRUN $test -f $testdir"
        local rc=0

        log "$cmd"
        eval $cmd
        rc=$?
        [ $rc = 0 ] || error "connectathon failed: $rc"
    done

    cd $savePWD
    rm -rf $testdir
}
run_test connectathon "connectathon"

test_ior() {
    local type=${1:="ssf"}

    [ x$IOR = x ] &&
        { skip_env "IOR not found" && return; }

    local space=$(df -P $DIR | tail -n 1 | awk '{ print $4 }')
    echo "+ $ior_blockSize * 1024 * 1024 * $num_clients * $ior_THREADS "
    if [ $((space / 2)) -le $(( ior_blockSize * 1024 * 1024 * num_clients * ior_THREADS)) ]; then
        echo "+ $space * 9/10 / 1024 / 1024 / $num_clients / $ior_THREADS"
        ior_blockSize=$(( space /2 /1024 /1024 / num_clients / ior_THREADS ))
        [ $ior_blockSize = 0 ] && \
            skip_env "Need free space more than ($num_clients * $ior_THREADS )Gb: $((num_clients*ior_THREADS *1024 *1024*2)), have $space" && return

        echo "free space=$space, Need: $num_clients x $ior_THREADS x $ior_blockSize Gb (blockSize reduced to $ior_blockSize Gb)"
    fi

    print_opts IOR ior_THREADS ior_DURATION MACHINEFILE

    local testdir=$DIR/d0.ior
    mkdir -p $testdir
    # mpi_run uses mpiuser
    chmod 0777 $testdir
    if [ "$NFSCLIENT" ]; then
        setstripe_nfsserver $testdir -c -1 ||
            { error "setstripe on nfsserver failed" && return 1; }
    else
        $LFS setstripe $testdir -c -1 ||
            { error "setstripe failed" && return 2; }
    fi
    #
    # -b N  blockSize -- contiguous bytes to write per task  (e.g.: 8, 4k, 2m, 1g)"
    # -o S  testFileName
    # -t N  transferSize -- size of transfer in bytes (e.g.: 8, 4k, 2m, 1g)"
    # -w    writeFile -- write file"
    # -r    readFile -- read existing file"
    # -T    maxTimeDuration -- max time in minutes to run tests"
    # -k    keepFile -- keep testFile(s) on program exit

    local cmd="$IOR -a $ior_type -b ${ior_blockSize}g -o $testdir/iorData -t $ior_xferSize -v -w -r -i $ior_iteration -T $ior_DURATION -k"
    [ $type = "fpp" ] && cmd="$cmd -F"

    echo "+ $cmd"
    # find out if we need to use srun by checking $SRUN_PARTITION
    if [ "$SRUN_PARTITION" ]; then
        $SRUN $SRUN_OPTIONS -D $testdir -w $clients -N $num_clients \
            -n $((num_clients * ior_THREADS)) -p $SRUN_PARTITION -- $cmd
    else
        mpi_run -np $((num_clients * $ior_THREADS)) -machinefile ${MACHINEFILE} $cmd
    fi

    local rc=$?
    if [ $rc != 0 ] ; then
        error "ior failed! $rc"
    fi
    rm -rf $testdir
}

test_iorssf() {
    test_ior "ssf"
}
run_test iorssf "iorssf"

test_iorfpp() {
    test_ior "fpp"
}
run_test iorfpp "iorfpp"

# cleanup nfs
cleanup_nfs "$MOUNT" "$lustre_client" "$CLIENTS"

zconf_umount_clients $lustre_client $MOUNT
zconf_mount_clients $CLIENTS $MOUNT

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
