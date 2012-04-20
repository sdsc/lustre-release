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
MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
clients=${CLIENTS:-$HOSTNAME}
generate_machine_file $clients $MACHINEFILE || \
    error "Failed to generate machine file"
num_clients=$(get_node_count ${clients//,/ })

# compilbench
if [ "$SLOW" = "no" ]; then
    cbench_IDIRS=2
    cbench_RUNS=2
fi

# metabench
[ "$SLOW" = "no" ] && mbench_NFILES=10000

# simul
[ "$SLOW" = "no" ] && simul_REP=2

# connectathon
[ "$SLOW" = "no" ] && cnt_NRUN=2

# cascading rw
[ "$SLOW" = "no" ] && casc_REP=10

# IOR
[ "$SLOW" = "no" ] && ior_DURATION=5

# write_append_truncate
[ "$SLOW" = "no" ] && write_REP=100

# write_disjoint
[ "$SLOW" = "no" ] && wdisjoint_REP=100

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

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
