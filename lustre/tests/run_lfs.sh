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

. $(dirname $0)/functions.sh

assert_env MOUNT END_RUN_FILE LOAD_PID_FILE LFS MDTCOUNT

trap signaled TERM

# if MACHINEFILE set and exists -- use it
if [ -z $MACHINEFILE ] || [ ! -e $MACHINEFILE ]; then
	MACHINEFILE=$TMP/$(basename $0)-$(hostname).machines
	echo $(hostname) >$MACHINEFILE
fi

THREADS_PER_CLIENT=${THREADS_PER_CLIENT:-3}
NUM_CLIENTS=$(cat $MACHINEFILE | wc -l)

# recovery-*-scale scripts use this to signal the client loads to die
echo $$ >$LOAD_PID_FILE

TESTDIR=${TESTDIR:-$MOUNT/d0.lfs-$(hostname)}

mkdir -p $TESTDIR
chmod -R 777 $TESTDIR

CONTINUE=true
while [ ! -e "$END_RUN_FILE" ] && $CONTINUE; do
	echoerr "$(date +'%F %H:%M:%S'): LFS run starting"
	MDT_IDX=$((RANDOM % MDTCOUNT))
	ret=0

	if [ -e $TESTDIR/remote_dir ]; then
		rm -rf $TESTDIR/remote_dir || ret=$?
	else
		$LFS mkdir -i $MDT_IDX $TESTDIR/remote_dir || ret=$?
	fi
	if [ $ret -ne 0 ]; then
		echoerr "$(date +'%F %H:%M:%S'): create remote dir failed"
		if [ -z "$ERRORS_OK" ]; then
			echo $(hostname) >> $END_RUN_FILE
		fi
		if [ $BREAK_ON_ERROR ]; then
			#break
			CONTINUE=false
		fi
	fi
done

echoerr "$(date +'%F %H:%M:%S'):  LFS run exiting"
