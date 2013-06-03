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

[ "$ALWAYS_EXCEPT$EXCEPT" ] &&
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
	env
	umask

	mkdir -p $DIR/$tdir
	chmod 777 $DIR/$tdir

	ls -ld $DIF $DIR/$tdir

	TESTFILE=$DIR/$tdir/file
	$RUNAS touch $TESTFILE

	# User flags
	local state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong initial hsm state $state"

	$RUNAS $LFS hsm_set --norelease $TESTFILE ||
		error "user could not change hsm flags"
	state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000010)" ]] ||
		error "wrong hsm state $state, should be: --norelease"

	$RUNAS $LFS hsm_clear --norelease $TESTFILE ||
		error "user could not clear hsm flags"
	state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong hsm state $state, should be empty"

	# User could not change those flags...
	$RUNAS $LFS hsm_set --exists $TESTFILE &&
		error "user should not set this flag"
	state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong hsm state $state, should be empty"

	# ...but root can
	$LFS hsm_set --exists $TESTFILE ||
		error "root could not change hsm flags"
	state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	$LFS hsm_clear --exists $TESTFILE ||
		error "root could not clear hsm state"
	state=$($RUNAS $LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong hsm state $state, should be empty"
}
run_test 1 "lfs hsm flags root/non-root access"

test_2() {
	mkdir -p $DIR/$tdir
	TESTFILE=$DIR/$tdir/file
	touch $TESTFILE

	# New files are not dirty
	local state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong hsm state $state, should be empty"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $TESTFILE || error "user could not change hsm flags"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# chmod do not put the file dirty
	chmod 600 $TESTFILE || error "could not chmod test file"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# chown do not put the file dirty
	chown $RUNAS_ID $TESTFILE || error "could not chown test file"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# truncate put the file dirty
	$TRUNCATE $TESTFILE 1 || error "could not truncate test file"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000003)" ]] ||
		error "wrong hsm state $state, should be 0x00000003"

	$LFS hsm_clear --dirty $TESTFILE || error "could not clear hsm flags"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"
}
run_test 2 "Check file dirtyness when doing setattr"

test_3() {
	mkdir -p $DIR/$tdir
	TESTFILE=$DIR/$tdir/file

	# New files are not dirty
	cp -p /etc/passwd $TESTFILE
	local state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000000)" ]] ||
		error "wrong hsm state $state, should be empty"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $TESTFILE ||
		error "user could not change hsm flags"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# Reading a file, does not set dirty
	cat $TESTFILE > /dev/null || error "could not read file"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# Open for write without modifying data, does not set dirty
	openfile -f O_WRONLY $TESTFILE || error "could not open test file"
	state=$($LFS hsm_state $TESTFILE | cut -f 2 -d" ")
	[[ $state == "(0x00000001)" ]] ||
		error "wrong hsm state $state, should be: --exists"

	# Append to a file sets it dirty
	cp -p /etc/passwd $TESTFILE.append || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.append ||
		error "user could not change hsm flags"
	dd if=/etc/passwd of=$TESTFILE.append bs=1 count=3 \
	   conv=notrunc oflag=append status=noxfer ||
		error "could not append to test file"
	state=$($LFS hsm_state $TESTFILE.append | cut -f 2 -d" ")
	[[ $state == "(0x00000003)" ]] ||
		error "wrong hsm state $state, should be 0x00000003"

	# Modify a file sets it dirty
	cp -p /etc/passwd $TESTFILE.modify || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.modify ||
		error "user could not change hsm flags"
	dd if=/dev/zero of=$TESTFILE.modify bs=1 count=3 \
	   conv=notrunc status=noxfer ||
		error "could not modify test file"
	state=$($LFS hsm_state $TESTFILE.modify | cut -f 2 -d" ")
	[[ $state == "(0x00000003)" ]] ||
		error "wrong hsm state $state, should be 0x00000003"

	# Open O_TRUNC sets dirty
	cp -p /etc/passwd $TESTFILE.trunc || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.trunc ||
		error "user could not change hsm flags"
	cp /etc/group $TESTFILE.trunc || error "could not override a file"
	state=$($LFS hsm_state $TESTFILE.trunc | cut -f 2 -d" ")
	[[ $state == "(0x00000003)" ]] ||
		error "wrong hsm state $state, should be 0x00000003"

	# Mmapped a file sets dirty
	cp -p /etc/passwd $TESTFILE.mmap || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.mmap ||
		error "user could not change hsm flags"
	multiop $TESTFILE.mmap OSMWUc || error "could not mmap a file"
	state=$($LFS hsm_state $TESTFILE.mmap | cut -f 2 -d" ")
	[[ $state == "(0x00000003)" ]] ||
		error "wrong hsm state $state, should be 0x00000003"
}
run_test 3 "Check file dirtyness when opening for write"

log "cleanup: ======================================================"
cd $ORIG_PWD
check_and_cleanup_lustre
echo '=========================== finished ==============================='
[ -f "$SANITYLOG" ] && cat $SANITYLOG && grep -q FAIL $SANITYLOG && exit 1 ||
	true
echo "$0: completed"
