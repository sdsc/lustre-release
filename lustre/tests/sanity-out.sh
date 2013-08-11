#!/bin/bash
# -*- tab-width: 4; indent-tabs-mode: t; -*-
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#
# e.g. ONLY="22 23" or ONLY="`seq 32 39`" or EXCEPT="31"
set -e

ONLY=${ONLY:-"$*"}

SRCDIR=$(cd $(dirname $0); echo $PWD)
export PATH=$PATH:/sbin

TMP=${TMP:-/tmp}

CHECKSTAT=${CHECKSTAT:-"checkstat -v"}
CREATETEST=${CREATETEST:-createtest}
LFS=${LFS:-lfs}
LFIND=${LFIND:-"$LFS find"}
LVERIFY=${LVERIFY:-ll_dirstripe_verify}
LCTL=${LCTL:-lctl}
MCREATE=${MCREATE:-mcreate}
OPENFILE=${OPENFILE:-openfile}
OPENUNLINK=${OPENUNLINK:-openunlink}
export MULTIOP=${MULTIOP:-multiop}
READS=${READS:-"reads"}
MUNLINK=${MUNLINK:-munlink}
SOCKETSERVER=${SOCKETSERVER:-socketserver}
SOCKETCLIENT=${SOCKETCLIENT:-socketclient}
MEMHOG=${MEMHOG:-memhog}
DIRECTIO=${DIRECTIO:-directio}
ACCEPTOR_PORT=${ACCEPTOR_PORT:-988}
UMOUNT=${UMOUNT:-"umount -d"}
STRIPES_PER_OBJ=-1
CHECK_GRANT=${CHECK_GRANT:-"yes"}
GRANT_CHECK_LIST=${GRANT_CHECK_LIST:-""}
export PARALLEL=${PARALLEL:-"no"}

export NAME=${NAME:-local}

SAVE_PWD=$PWD

CLEANUP=${CLEANUP:-:}
SETUP=${SETUP:-:}
TRACE=${TRACE:-""}
LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/${NAME}.sh}
init_logging

[ "$SLOW" = "no" ] && EXCEPT_SLOW=""

FAIL_ON_ERROR=false

cleanup() {
	echo -n "cln.."
	pgrep ll_sa > /dev/null && { echo "There are ll_sa thread not exit!"; exit 20; }
	cleanupall ${FORCE} $* || { echo "FAILed to clean up"; exit 20; }
}
setup() {
	echo -n "mnt.."
	load_modules
	setupall || exit 10
	echo "done"
}

if [ "$ONLY" == "cleanup" ]; then
       sh llmountcleanup.sh
       exit 0
fi

check_and_setup_lustre

DIR=${DIR:-$MOUNT}
assert_DIR

build_test_filter

if [ "${ONLY}" = "MOUNT" ] ; then
	echo "Lustre is up, please go on"
	exit
fi

test_1() {
	mkdir -p $DIR/$tdir
	createmany -o $DIR/$tdir/f 100
	unlinkmany $DIR/$tdir/f 100
	wait_delete_completed
	return 0
}
run_test 1 "batched OST destroys"

test_2() {
	mkdir -p $DIR/$tdir
	createmany -o $DIR/$tdir/f 100
	unlinkmany $DIR/$tdir/f 100
	replay_barrier $SINGLEMDS
	fail $SINGLEMDS
	wait_delete_completed
	return 0
}
run_test 2 "errors in OUT"

complete $SECONDS
check_and_cleanup_lustre
exit_status
