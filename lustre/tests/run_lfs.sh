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

assert_env MOUNT END_RUN_FILE LOAD_PID_FILE LFS MDSCOUNT

trap signaled TERM

# recovery-*-scale scripts use this to signal the client loads to die
echo $$ >$LOAD_PID_FILE

TESTDIR=$MOUNT/d0.lfs-$(hostname)
REMOTE_DIR=$TESTDIR/remote_dir
rm -rf $TESTDIR
mkdir -p $TESTDIR

CONTINUE=true
while [[ ! -e "$END_RUN_FILE" ]] && $CONTINUE; do
	echoerr "$(date +'%F %H:%M:%S'): lfs run starting"
	ret=0

	if [[ -e $REMOTE_DIR ]]; then
		rm -rf $REMOTE_DIR || ret=${PIPESTATUS[0]}
	else
		MDT_IDX=$((RANDOM % MDSCOUNT))
		$LFS mkdir -i $MDT_IDX -c$MDSCOUNT $REMOTE_DIR ||
					ret=${PIPESTATUS[0]}
	fi

	if [[ $ret -eq 0 ]]; then
		echoerr "$(date +'%F %H:%M:%S'): create/rm remote dir succeeded"
	else
		echoerr "$(date +'%F %H:%M:%S'): create/rm remote dir failed"
		if [[ -z "$ERRORS_OK" ]]; then
			echo $(hostname) >> $END_RUN_FILE
		fi
		if [[ $BREAK_ON_ERROR ]]; then
			#break
			CONTINUE=false
		fi
	fi
done

echoerr "$(date +'%F %H:%M:%S'): lfs run exiting"
