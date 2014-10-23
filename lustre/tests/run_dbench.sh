#!/bin/bash

TMP=${TMP:-/tmp}
TESTDIR=${TESTDIR:-$MOUNT/d0.dbench-$(hostname)}

TESTLOG_PREFIX=${TESTLOG_PREFIX:-$TMP/recovery-mds-scale}
TESTNAME=${TESTNAME:-""}
[ -n "$TESTNAME" ] && TESTLOG_PREFIX=$TESTLOG_PREFIX.$TESTNAME

LOG=$TESTLOG_PREFIX.$(basename $0 .sh)_stdout.$(hostname -s).log
DEBUGLOG=$(echo $LOG | sed 's/\(.*\)stdout/\1debug/')

mkdir -p ${LOG%/*}

rm -f $LOG $DEBUGLOG
exec 2>$DEBUGLOG
set -x

. $(dirname $0)/test-framework.sh
. $(dirname $0)/functions.sh


assert_env MOUNT END_RUN_FILE LOAD_PID_FILE
assert_env MOUNT END_RUN_FILE LOAD_PID_FILE LFS CLIENT_COUNT MDTCOUNT

trap signaled TERM
rm -rf $TESTDIR

# recovery-*-scale scripts use this to signal the client loads to die
echo $$ >$LOAD_PID_FILE

CONTINUE=true

while [ ! -e "$END_RUN_FILE" ] && $CONTINUE; do
	echoerr "$(date +'%F %H:%M:%S'): dbench run starting"

	MDT_IDX=$((RANDOM % MDTCOUNT))
	test_mkdir -i $MDT_IDX -c$MDTCOUNT $TESTDIR || {
		echoerr "create $TESTDIR fails"
		echo $(hostname) >> $END_RUN_FILE
		break
	}
	$LFS setdirstripe -D -c$MDTCOUNT $TESTDIR

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
