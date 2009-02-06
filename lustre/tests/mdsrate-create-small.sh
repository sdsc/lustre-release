#!/bin/bash
#
# This test was used in a set of CMD3 tests (cmd3-3 test). 

LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}

assert_env CLIENTS MDSRATE SINGLECLIENT MPIRUN

MACHINEFILE=${MACHINEFILE:-$TMP/$(basename $0 .sh).machines}
TESTDIR=$MOUNT

# Requirements
NUM_FILES=${NUM_FILES:-1000000}
TIME_PERIOD=${TIME_PERIOD:-600}                        # seconds

# Local test variables
TESTDIR_SINGLE="${TESTDIR}/single"
TESTDIR_MULTI="${TESTDIR}/multi"

LOG=${TESTSUITELOG:-$TMP/$(basename $0 .sh).log}
CLIENT=$SINGLECLIENT
NODES_TO_USE=${NODES_TO_USE:-$CLIENTS}
NUM_CLIENTS=$(get_node_count ${NODES_TO_USE//,/ })
# XXX - this needs to be determined given the number of MDTs and the number
#       of clients.
THREADS_PER_CLIENT=3                   # threads/client for multi client test
if [ $NUM_CLIENTS -gt 50 ]; then
    THREADS_PER_CLIENT=1
fi

[ ! -x ${MDSRATE} ] && error "${MDSRATE} not built."

# Make sure we start with a clean slate
rm -f ${LOG} PI*

log "===== $0 ====== " 

check_and_setup_lustre

IFree=$(inodes_available)
if [ $IFree -lt $NUM_FILES ]; then
    NUM_FILES=$IFree
fi
  
generate_machine_file $NODES_TO_USE $MACHINEFILE || error "can not generate machinefile"

$LFS setstripe $TESTDIR -i 0 -c 1
get_stripe $TESTDIR

if [ -n "$NOSINGLE" ]; then
    echo "NO Tests on single client."
else
    if [ -n "$NOCREATE" ]; then
        echo "NO Test for creates for a single client."
    else
        do_node ${CLIENT} "rm -rf $TESTDIR_SINGLE"

        log "===== $0 ### 1 NODE CREATE ###"
        echo "Running creates on 1 node(s)."

        COMMAND="${MDSRATE} ${MDSRATE_DEBUG} --create --time ${TIME_PERIOD}
                    --nfiles $NUM_FILES --dir ${TESTDIR_SINGLE} --filefmt 'f%%d'"
        echo "+ ${COMMAND}"
        mpi_run -np 1 -machinefile ${MACHINEFILE} ${COMMAND} | tee ${LOG}

        if [ ${PIPESTATUS[0]} != 0 ]; then
        [ -f $LOG ] && cat $LOG
            error "mpirun ... mdsrate ... failed, aborting"
        fi
    fi

    if [ -n "$NOUNLINK" ]; then
        echo "NO Test for unlinks for a single client."
    else
        log "===== $0 ### 1 NODE UNLINK ###"
        echo "Running unlinks on 1 node(s)."

        COMMAND="${MDSRATE} ${MDSRATE_DEBUG} --unlink --time ${TIME_PERIOD}
                     --nfiles ${NUM_FILES} --dir ${TESTDIR_SINGLE} --filefmt 'f%%d'"
        echo "+ ${COMMAND}"
        mpi_run -np 1 -machinefile ${MACHINEFILE} ${COMMAND} | tee ${LOG}

        if [ ${PIPESTATUS[0]} != 0 ]; then
        [ -f $LOG ] && cat $LOG
            error "mpirun ... mdsrate ... failed, aborting"
        fi
    fi
fi

IFree=$(inodes_available)
if [ $IFree -lt $NUM_FILES ]; then
    NUM_FILES=$IFree
fi

if [ -n "$NOMULTI" ]; then
    echo "NO tests on multiple nodes."
else
    if [ -n "$NOCREATE" ]; then
        echo "NO test for create on multiple nodes."
    else
        do_node $CLIENT rm -rf $TESTDIR_MULTI

        log "===== $0 ### $NUM_CLIENTS NODES CREATE ###"
        echo "Running creates on ${NUM_CLIENTS} node(s) with $THREADS_PER_CLIENT threads per client."

        COMMAND="${MDSRATE} ${MDSRATE_DEBUG} --create --time ${TIME_PERIOD}
                    --nfiles $NUM_FILES --dir ${TESTDIR_MULTI} --filefmt 'f%%d'"
        echo "+ ${COMMAND}"
        mpi_run -np $((NUM_CLIENTS * THREADS_PER_CLIENT)) -machinefile ${MACHINEFILE} \
            ${COMMAND} | tee ${LOG}
        if [ ${PIPESTATUS[0]} != 0 ]; then
            [ -f $LOG ] && cat $LOG
            error "mpirun ... mdsrate ... failed, aborting"
        fi
    fi

    if [ -n "$NOUNLINK" ]; then
        echo "NO Test for unlinks multiple nodes."
    else
        log "===== $0 ### $NUM_CLIENTS NODES UNLINK ###"
        echo "Running unlinks on ${NUM_CLIENTS} node(s) with $THREADS_PER_CLIENT threads per client."

        COMMAND="${MDSRATE} ${MDSRATE_DEBUG} --unlink --time ${TIME_PERIOD}
                      --nfiles ${NUM_FILES} --dir ${TESTDIR_MULTI} --filefmt 'f%%d'"
        echo "+ ${COMMAND}"
        mpi_run -np $((NUM_CLIENTS * THREADS_PER_CLIENT)) -machinefile ${MACHINEFILE} \
            ${COMMAND} | tee ${LOG}
        if [ ${PIPESTATUS[0]} != 0 ]; then
            [ -f $LOG ] && cat $LOG
            error "mpirun ... mdsrate ... failed, aborting"
        fi
    fi
fi

equals_msg `basename $0`: test complete, cleaning up
rm -f $MACHINEFILE 
check_and_cleanup_lustre
#rm -f $LOG

exit 0
