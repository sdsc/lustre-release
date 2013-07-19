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

MULTIOP=${MULTIOP:-multiop}
OPENFILE=${OPENFILE:-openfile}
MCREATE=${MCREATE:-mcreate}

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

if [ $MDSCOUNT -ge 2 ]; then
	skip_env "Only run with single MDT for now" && exit
fi

if [ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.3.61) ]; then
	skip_env "Need MDS version at least 2.3.61" && exit
fi

DIR=${DIR:-$MOUNT}
assert_DIR

build_test_filter

# $RUNAS_ID may get set incorrectly somewhere else
[ $UID -eq 0 -a $RUNAS_ID -eq 0 ] &&
	error "\$RUNAS_ID set to 0, but \$UID is also 0!"

check_runas_id $RUNAS_ID $RUNAS_GID $RUNAS

copytool_cleanup() {
	# TODO: add copytool cleanup code here!
	return
}

copytool_setup() {
	# TODO: add copytool setup code here!
	return
}

fail() {
	copytool_cleanup
	error $*
}

path2fid() {
	$LFS path2fid $1 | tr -d '[]'
}

make_small() {
	local file2=${1/$DIR/$DIR}
	dd if=/dev/urandom of=$file2 count=2 bs=1M
		path2fid $1
}

test_1() {
	mkdir -p $DIR/$tdir
	chmod 0777 $DIR/$tdir

	local f=$DIR/$tdir/file
	$RUNAS touch $f || error "cannot touch $f as $RUNAS_UID"

	# User flags
	$RUNAS $LFS hsm_state $f | grep -q "(0x00000000)" ||
	   error "wrong initial hsm state"
	$RUNAS $LFS hsm_set --norelease $f ||
	   error "user could not change hsm flags"
	$RUNAS $LFS hsm_state $f | grep -q "(0x00000010)" ||
	   error "wrong hsm state, should be: --norelease"
	$RUNAS $LFS hsm_clear --norelease $f ||
	   error "user could not clear hsm flags"
	$RUNAS $LFS hsm_state $f | grep -q "(0x00000000)" ||
	   error "wrong hsm state, should be empty"

	# User could not change those flags...
	$RUNAS $LFS hsm_set --exists $f &&
	   error "user should not set this flag"
	$RUNAS $LFS hsm_state $f | grep -q "(0x00000000)" ||
	   error "wrong hsm state, should be empty"

	# ...but root can
	$LFS hsm_set --exists $f ||
	   error "root could not change hsm flags"
	$LFS hsm_state $f | grep -q "(0x00000001)" ||
	    error "wrong hsm state, should be: --exists"
	$LFS hsm_clear --exists $f ||
	    error "root could not clear hsm state"
	$LFS hsm_state $f | grep -q "(0x00000000)" ||
	    error "wrong hsm state, should be empty"
}
run_test 1 "lfs hsm flags root/non-root access"

test_2() {
	mkdir -p $DIR/$tdir
	f=$DIR/$tdir/file
	touch $f

	# New files are not dirty
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		error "wrong hsm state: !0x0"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $f || error "user could not change hsm flags"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# chmod do not put the file dirty
	chmod 600 $f || error "could not chmod test file"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# chown do not put the file dirty
	chown $RUNAS_ID $f || error "could not chown test file"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# truncate put the file dirty
	$TRUNCATE $f 1 || error "could not truncate test file"
	$LFS hsm_state $f | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"
	$LFS hsm_clear --dirty $f || error "could not clear hsm flags"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"
}
run_test 2 "Check file dirtyness when doing setattr"

test_3() {
	mkdir -p $DIR/$tdir
	local f=$DIR/$tdir/file

	# New files are not dirty
	cp -p /etc/passwd $f
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		error "wrong hsm state: !0x0"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $f ||
		error "user could not change hsm flags"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Reading a file, does not set dirty
	cat $f > /dev/null || error "could not read file"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Open for write without modifying data, does not set dirty
	$OPENFILE -f O_WRONLY $f || error "could not open test file"
	$LFS hsm_state $f | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Append to a file sets it dirty
	cp -p /etc/passwd $f.append || error "could not create file"
	$LFS hsm_set --exists $f.append ||
		error "user could not change hsm flags"
	dd if=/etc/passwd of=$f.append bs=1 count=3 \
	   conv=notrunc oflag=append status=noxfer ||
		error "could not append to test file"
	$LFS hsm_state $f.append | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Modify a file sets it dirty
	cp -p /etc/passwd $f.modify || error "could not create file"
	$LFS hsm_set --exists $f.modify ||
		error "user could not change hsm flags"
	dd if=/dev/zero of=$f.modify bs=1 count=3 \
	   conv=notrunc status=noxfer ||
		error "could not modify test file"
	$LFS hsm_state $f.modify | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Open O_TRUNC sets dirty
	cp -p /etc/passwd $f.trunc || error "could not create file"
	$LFS hsm_set --exists $f.trunc ||
		error "user could not change hsm flags"
	cp /etc/group $f.trunc || error "could not override a file"
	$LFS hsm_state $f.trunc | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Mmapped a file sets dirty
	cp -p /etc/passwd $f.mmap || error "could not create file"
	$LFS hsm_set --exists $f.mmap ||
		error "user could not change hsm flags"
	$MULTIOP $f.mmap OSMWUc || error "could not mmap a file"
	$LFS hsm_state $f.mmap | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"
}
run_test 3 "Check file dirtyness when opening for write"

test_20() {
	mkdir -p $DIR/$tdir

	local f=$DIR/$tdir/sample
	touch $f

	# Could not release a non-archived file
	$LFS hsm_release $f && error "release should not succeed"

	# For following tests, we must test them with HS_ARCHIVED set
	$LFS hsm_set --exists --archived $f || error "could not add flag"

	# Could not release a file if no-release is set
	$LFS hsm_set --norelease $f || error "could not add flag"
	$LFS hsm_release $f && error "release should not succeed"
	$LFS hsm_clear --norelease $f || error "could not remove flag"

	# Could not release a file if lost
	$LFS hsm_set --lost $f || error "could not add flag"
	$LFS hsm_release $f && error "release should not succeed"
	$LFS hsm_clear --lost $f || error "could not remove flag"

	# Could not release a file if dirty
	$LFS hsm_set --dirty $f || error "could not add flag"
	$LFS hsm_release $f && error "release should not succeed"
	$LFS hsm_clear --dirty $f || error "could not remove flag"

}
run_test 20 "Release is not permitted"

test_21() {
	# test needs a running copytool
	copytool_setup

	mkdir -p $DIR/$tdir
	local f=$DIR/$tdir/test_release

	# Create a file and check its states
	local fid=$(make_small $f)
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		fail "wrong clean hsm state"

#	$LFS hsm_archive $f || fail "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $f || fail "could not archive file"

	[ $(stat -c "%b" $f) -ne "0" ] || fail "wrong block number"
	local sz=$(stat -c "%s" $f)
	[ $sz -ne "0" ] || fail "file size should not be zero"

	# Release and check states
	$LFS hsm_release $f || fail "could not release file"
	$LFS hsm_state $f | grep -q " (0x0000000d)" ||
		fail "wrong released hsm state"
	[ $(stat -c "%b" $f) -eq "0" ] || fail "wrong block number"
	[ $(stat -c "%s" $f) -eq $sz ] || fail "wrong file size"

	# Check we can release an file without stripe info
	f=$f.nolov
	$MCREATE $f
	fid=$(path2fid $f)
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		fail "wrong clean hsm state"

#	$LFS hsm_archive $f || fail "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $f || fail "could not archive file"

	# Release and check states
	$LFS hsm_release $f || fail "could not release file"
	$LFS hsm_state $f | grep -q " (0x0000000d)" ||
		fail "wrong released hsm state"

	copytool_cleanup
}
run_test 21 "Simple release tests"

test_22() {
	# test needs a running copytool
	copytool_setup

	mkdir -p $DIR/$tdir

	local f=$DIR/$tdir/test_release
	local swap=$DIR/$tdir/test_swap

	# Create a file and check its states
	local fid=$(make_small $f)
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		fail "wrong clean hsm state"

#	$LFS hsm_archive $f || fail "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $f || fail "could not archive file"

	# Release and check states
	$LFS hsm_release $f || fail "could not release file"
	$LFS hsm_state $f | grep -q " (0x0000000d)" ||
		fail "wrong released hsm state"

	make_small $swap || fail "could not create $swap"
	$LFS swap_layouts $swap $f && fail "swap_layouts should failed"

	true
	copytool_cleanup
}
run_test 22 "Could not swap a release file"


test_23() {
	# test needs a running copytool
	copytool_setup

	mkdir -p $DIR/$tdir

	local f=$DIR/$tdir/test_mtime

	# Create a file and check its states
	local fid=$(make_small $f)
	$LFS hsm_state $f | grep -q " (0x00000000)"  ||
		fail "wrong clean hsm state"

#	$LFS hsm_archive $f || fail "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $f || fail "could not archive file"

	# Set modification time in the past
	touch -m -a -d @978261179 $f

	# Release and check states
	$LFS hsm_release $f || fail "could not release file"
	$LFS hsm_state $f | grep -q " (0x0000000d)" ||
		fail "wrong released hsm state"
	local MTIME=$(stat -c "%Y" $f)
	local ATIME=$(stat -c "%X" $f)
	[ $MTIME -eq "978261179" ] || fail "bad mtime: $MTIME"
	[ $ATIME -eq "978261179" ] || fail "bad atime: $ATIME"

	copytool_cleanup
}
run_test 23 "Release does not change a/mtime (utime)"

test_24() {
	# test needs a running copytool
	copytool_setup

	mkdir -p $DIR/$tdir

	local f=$DIR/$tdir/test_mtime

	# Create a file and check its states
	local fid=$(make_small $f)
	$LFS hsm_state $f | grep -q " (0x00000000)" ||
		fail "wrong clean hsm state"

	# ensure mtime is different
	sleep 1
	echo "append" >> $f
	local MTIME=$(stat -c "%Y" $f)
	local ATIME=$(stat -c "%X" $f)

#	$LFS hsm_archive $f || fail "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $f || fail "could not archive file"

	# Release and check states
	$LFS hsm_release $f || fail "could not release file"
	$LFS hsm_state $f | grep -q " (0x0000000d)" ||
		fail "wrong released hsm state"

	[ "$(stat -c "%Y" $f)" -eq "$MTIME" ] ||
		fail "mtime should be $MTIME"

#	[ "$(stat -c "%X" $f)" -eq "$ATIME" ] ||
#		fail "atime should be $ATIME"

	copytool_cleanup
}
run_test 24 "Release does not change a/mtime (i/o)"

log "cleanup: ======================================================"
cd $ORIG_PWD
check_and_cleanup_lustre
echo '=========================== finished ==============================='
[ -f "$SANITYLOG" ] && cat $SANITYLOG && grep -q FAIL $SANITYLOG && exit 1 ||
	true
echo "$0: completed"
