#!/bin/bash
#
#set -vx

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

#              bug 20670
ALWAYS_EXCEPT="parallel_grouplock $PARALLEL_SCALE_EXCEPT"

# common setup
#
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
clients=${CLIENTS:-$HOSTNAME}
generate_machine_file $clients $MACHINEFILE || error "Failed to generate machine file"
num_clients=$(get_node_count ${clients//,/ })

#
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
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
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
ior_blockSize=${ior_blockSize:-6}	# Gb
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
PARALLEL_GROUPLOCK=${PARALLEL_GROUPLOCK:-$(which parallel_grouplock 2> /dev/null || true)}
parallel_grouplock_MINTASKS=${parallel_grouplock_MINTASKS:-5}

#
# statahead
#
statahead_NUMMNTPTS=${statahead_NUMMNTPTS:-5}
statahead_NUMFILES=${statahead_NUMFILES:-500000}

. $LUSTRE/tests/functions.sh

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

test_connectathon() {
    run_connectathon
}
run_test connectathon "connectathon"

test_iorssf() {
    run_ior "ssf"
}
run_test iorssf "iorssf"

test_iorfpp() {
    run_ior "fpp"
}
run_test iorfpp "iorfpp"

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

test_statahead () {
    run_statahead
}
run_test statahead "statahead test, multiple clients"

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
