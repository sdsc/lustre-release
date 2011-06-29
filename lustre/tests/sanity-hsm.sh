#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#
# Run test by setting NOSETUP=true when ltest has setup env for us
#
# exit on error
set -e
set +o monitor

SRCDIR=`dirname $0`
export PATH=$PWD/$SRCDIR:$SRCDIR:$PWD/$SRCDIR/utils:$PATH:/sbin

ONLY=${ONLY:-"$*"}
SANITY_HSM_EXCEPT=${SANITY_HSM_EXCEPT:-""}
ALWAYS_EXCEPT="$SANITY_HSM_EXCEPT"
# bug number for skipped test:
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

[ "$ALWAYS_EXCEPT$EXCEPT" ] && \
        echo "Skipping tests: `echo $ALWAYS_EXCEPT $EXCEPT`"

TMP=${TMP:-/tmp}

ORIG_PWD=${PWD}

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging


SANITYLOG=${TESTSUITELOG:-$TMP/$(basename $0 .sh).log}
FAIL_ON_ERROR=false

[ "$SANITYLOG" ] && rm -f $SANITYLOG || true
check_and_setup_lustre

DIR=${DIR:-$MOUNT}
assert_DIR

build_test_filter


test_1() {
        mkdir -p $DIR/$tdir
        chmod 777 $DIR/$tdir

        TESTFILE=$DIR/$tdir/file
        $RUNAS touch $TESTFILE

        # User flags
        $RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" \
           || error "wrong initial hsm state"
        $RUNAS $LFS hsm_set --norelease $TESTFILE || error "user could not change hsm flags"
        $RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000010)" \
           || error "wrong hsm state, should be: --norelease"
        $RUNAS $LFS hsm_clear --norelease $TESTFILE || error "user could not clear hsm flags"
        $RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" \
           || error "wrong hsm state, should be empty"

        # User could not change those flags...
        $RUNAS $LFS hsm_set --exists $TESTFILE && error "user should not set this flag"
        $RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" \
           || error "wrong hsm state, should be empty"

        # ...but root can
        $LFS hsm_set --exists $TESTFILE || error "root could not change hsm flags"
        $LFS hsm_state $TESTFILE | grep -q "(0x00000001)" \
           || error "wrong hsm state, should be: --exists"
        $LFS hsm_clear --exists $TESTFILE || error "root could not clear hsm state"
        $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" \
           || error "wrong hsm state, should be empty"

}
run_test 1 "lfs hsm flags root/non-root access"

log "cleanup: ======================================================"
cd $ORIG_PWD
check_and_cleanup_lustre
echo '=========================== finished ==============================='
[ -f "$SANITYLOG" ] && cat $SANITYLOG && grep -q FAIL $SANITYLOG && exit 1 || true
echo "$0: completed"
