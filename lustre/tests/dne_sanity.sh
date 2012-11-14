#!/bin/bash

set -e

ONLY=${ONLY:-"$*"}

# bug number for skipped test:	      12652 12652

# Tests that fail on uml
[ "$UML" = "true" ] && EXCEPT="$EXCEPT 7"

SRCDIR=`dirname $0`
PATH=$PWD/$SRCDIR:$SRCDIR:$SRCDIR/../utils:$PATH

SAVE_PWD=$PWD

export NAME=${NAME:-local}

LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
CLEANUP=${CLEANUP:-:}
SETUP=${SETUP:-:}
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

LFS=${LFS:-lfs}
FAIL_ON_ERROR=false

SETUP=${SETUP:-:}
TRACE=${TRACE:-""}

MDSCOUNT=${MDSCOUNT:-2}
[ $MDSCOUNT -le 1 ] &&
	skip "This can only be run with multiple MDTs try MDSCOUNT=n" && exit 0

check_and_setup_lustre

DIR=${DIR:-$MOUNT}

ORIGIN_DIR=$DIR

prepare_remote_directories()
{
	local mdtidx
	local rc=0
	local i;

	for i in `seq 1 $MDSCOUNT`; do
		rm -rf $ORIGIN_DIR/remote_dir$i
		$LFS mkdir -i $((i - 1)) $ORIGIN_DIR/remote_dir$i || rc=$?
		if [ $rc != 0 ]; then
			echo "can not create remote dir for MDT$i"
			break;
		fi
	done

	return $rc
}

prepare_remote_directories || error "Can not create remote directory"

test_sanity()
{
	[ ! -f sanity.sh ] && skip_env "No sanity.sh skipping" && return

	local index
	local pid
	local rpids
	local excepts="17i 17k 24v 24x 24y 24z 27A 27n 27o 27p 27q 27r 27v"
	excepts="$excepts 27w 27x 27y 27z 29 30c 31f 32a 32b 32c 32d 32i 32j"
	excepts="$excepts 32k 32l 32q 32r 33c 33d 34f 34g 34h 36f 36g 36h 39d"
	excepts="$excepts 39e 39f 39j 39h 39i 39j 39l 39m 42a 42b 42e 45 46 49"
	excepts="$excepts 51bb 51d 53 54c 56 57 60 61 62 63 64 65 66 68 69"
	excepts="$excepts 72 73 74 76 77 78 79 80 81 82 100 101 102d 102f 102j"
	excepts="$excepts 104 105d 107 115 116 117 118 119d 120 121 123 124"
	excepts="$excepts 127 129 132 133 140 150 151 152 154 155 156 160 163"
	excepts="$excepts 170 171 180 182 200 205 215 216 217 220 221 222 223"
	excepts="$excepts 224 225 226 230 900"

	for index in `seq 1 $MDSCOUNT`; do
		DIR=$ORIGIN_DIR/remote_dir${index} PARALLEL=yes \
			SANITY_EXCEPT="$excepts" ONLY="$ONLY" sh sanity.sh &
		pid=$?
        	rpids="$rpids $pid"
	done

	echo sanity pids: $rpids
	for pid in $rpids; do
		wait $pid
		rc=$?
		echo "pid=$pid rc=$rc"
		if [ $rc != 0 ]; then
	    		rrc=$((rrc + 1))
		fi
	done
	return $rrc
}

run_test sanity "Run sanity parallel on different remote directories"

check_and_cleanup_lustre
