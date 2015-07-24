#!/bin/bash

TMP=${TMP:-/tmp}

TESTLOG_PREFIX=${TESTLOG_PREFIX:-$TMP/recovery-mds-scale}
TESTNAME=${TESTNAME:-""}
[ -n "$TESTNAME" ] && TESTLOG_PREFIX=$TESTLOG_PREFIX.$TESTNAME

LOG=$TESTLOG_PREFIX.$(basename $0 .sh)_stdout.$(hostname -s).log
DEBUGLOG=$(echo $LOG | sed 's/\(.*\)stdout/\1debug/')

mkdir -p ${LOG%/*}

rm -f $LOG $DEBUGLOG
exec 2>$DEBUGLOG
set -x

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh

assert_env MOUNT END_RUN_FILE LOAD_PID_FILE NODENUM MDSCOUNT LFS

trap signaled TERM

# recovery-*-scale scripts use this to signal the client loads to die
echo $$ >$LOAD_PID_FILE

TESTDIR=$MOUNT/d0.dbench-$(hostname)
rm -rf $TESTDIR

CONTINUE=true
while [ ! -e "$END_RUN_FILE" ] && $CONTINUE; do
	echoerr "$(date +'%F %H:%M:%S'): dbench run starting"
	MDT_IDX=$((NODENUM % MDSCOUNT))
	test_mkdir -i $MDT_IDX -c$MDSCOUNT $TESTDIR
	$LFS setdirstripe -D -c$MDSCOUNT $TESTDIR
	sync
	rundbench -D $TESTDIR 2 1>$LOG &
	load_pid=$!

	wait $load_pid
	if [ ${PIPESTATUS[0]} -eq 0 ]; then
		echoerr "$(date +'%F %H:%M:%S'): dbench succeeded"
		cd $TMP
		rm -rf $TESTDIR
		echoerr "$(date +'%F %H:%M:%S'): dbench run finished"
	else
		echoerr "$(date +'%F %H:%M:%S'): dbench failed"
		if [ -z "$ERRORS_OK" ]; then
			echo $(hostname) >> $END_RUN_FILE
		fi

		if [ $BREAK_ON_ERROR ]; then
			# break
			CONTINUE=false
		fi
	fi
done

echoerr "$(date +'%F %H:%M:%S'): dbench run exiting"
