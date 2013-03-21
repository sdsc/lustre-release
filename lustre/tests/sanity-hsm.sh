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

if [ $MDSCOUNT -ge 2 ]; then
	skip_env "Only run with single MDT for now" && exit
fi

if [ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.3.61) ]; then
	skip_env "Need MDS version at least 2.3.61" && exit
fi

DIR=${DIR:-$MOUNT}
assert_DIR

build_test_filter

path2fid() {
        $LFS path2fid $1 | tr -d '[]'
}

make_small() {
#        local file2=${1/$DIR/$DIR2}
        local file2=${1/$DIR/$DIR}
        dd if=/dev/urandom of=$file2 count=2 bs=1M
        path2fid $1
}

test_1() {
	mkdir -p $DIR/$tdir
	chmod 777 $DIR/$tdir

	TESTFILE=$DIR/$tdir/file
	$RUNAS touch $TESTFILE

	# User flags
	$RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" ||
	   error "wrong initial hsm state"
	$RUNAS $LFS hsm_set --norelease $TESTFILE ||
	   error "user could not change hsm flags"
	$RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000010)" ||
	   error "wrong hsm state, should be: --norelease"
	$RUNAS $LFS hsm_clear --norelease $TESTFILE ||
	   error "user could not clear hsm flags"
	$RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" ||
	   error "wrong hsm state, should be empty"

	# User could not change those flags...
	$RUNAS $LFS hsm_set --exists $TESTFILE &&
	   error "user should not set this flag"
	$RUNAS $LFS hsm_state $TESTFILE | grep -q "(0x00000000)" ||
	   error "wrong hsm state, should be empty"

	# ...but root can
	$LFS hsm_set --exists $TESTFILE ||
	   error "root could not change hsm flags"
	$LFS hsm_state $TESTFILE | grep -q "(0x00000001)" ||
	    error "wrong hsm state, should be: --exists"
	$LFS hsm_clear --exists $TESTFILE ||
	    error "root could not clear hsm state"
	$LFS hsm_state $TESTFILE | grep -q "(0x00000000)" ||
	    error "wrong hsm state, should be empty"
}
run_test 1 "lfs hsm flags root/non-root access"

test_2() {
	mkdir -p $DIR/$tdir
	TESTFILE=$DIR/$tdir/file
	touch $TESTFILE

	# New files are not dirty
	$LFS hsm_state $TESTFILE | grep -q " (0x00000000)" ||
		error "wrong hsm state: !0x0"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $TESTFILE || error "user could not change hsm flags"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# chmod do not put the file dirty
	chmod 600 $TESTFILE || error "could not chmod test file"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# chown do not put the file dirty
	chown $RUNAS_ID $TESTFILE || error "could not chown test file"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# truncate put the file dirty
	./truncate $TESTFILE 1 || error "could not truncate test file"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"
	$LFS hsm_clear --dirty $TESTFILE || error "could not clear hsm flags"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"
}
run_test 2 "Check file dirtyness when doing setattr"

test_3() {
	mkdir -p $DIR/$tdir
	TESTFILE=$DIR/$tdir/file

	# New files are not dirty
	cp -p /etc/passwd $TESTFILE
	$LFS hsm_state $TESTFILE | grep -q " (0x00000000)" ||
		error "wrong hsm state: !0x0"

	# For test, we simulate an archived file.
	$LFS hsm_set --exists $TESTFILE ||
		error "user could not change hsm flags"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Reading a file, does not set dirty
	cat $TESTFILE > /dev/null || error "could not read file"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Open for write without modifying data, does not set dirty
	openfile -f O_WRONLY $TESTFILE || error "could not open test file"
	$LFS hsm_state $TESTFILE | grep -q " (0x00000001)" ||
		error "wrong hsm state: !0x1"

	# Append to a file sets it dirty
	cp -p /etc/passwd $TESTFILE.append || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.append ||
		error "user could not change hsm flags"
	dd if=/etc/passwd of=$TESTFILE.append bs=1 count=3 \
	   conv=notrunc oflag=append status=noxfer ||
		error "could not append to test file"
	$LFS hsm_state $TESTFILE.append | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Modify a file sets it dirty
	cp -p /etc/passwd $TESTFILE.modify || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.modify ||
		error "user could not change hsm flags"
	dd if=/dev/zero of=$TESTFILE.modify bs=1 count=3 \
	   conv=notrunc status=noxfer ||
		error "could not modify test file"
	$LFS hsm_state $TESTFILE.modify | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Open O_TRUNC sets dirty
	cp -p /etc/passwd $TESTFILE.trunc || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.trunc ||
		error "user could not change hsm flags"
	cp /etc/group $TESTFILE.trunc || error "could not override a file"
	$LFS hsm_state $TESTFILE.trunc | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"

	# Mmapped a file sets dirty
	cp -p /etc/passwd $TESTFILE.mmap || error "could not create file"
	$LFS hsm_set --exists $TESTFILE.mmap ||
		error "user could not change hsm flags"
	multiop $TESTFILE.mmap OSMWUc || error "could not mmap a file"
	$LFS hsm_state $TESTFILE.mmap | grep -q " (0x00000003)" ||
		error "wrong hsm state: !0x3"
}
run_test 3 "Check file dirtyness when opening for write"

test_20() {
	mkdir -p $DIR/$tdir

	FILE=$DIR/$tdir/sample
	touch $FILE

	# Could not release a non-archived file
	$LFS hsm_release $FILE && error "release should not succeed"

	# For following tests, we must test them with HS_ARCHIVED set
	$LFS hsm_set --exists --archived $FILE || error "could not add flag"

	# Could not release a file if no-release is set
	$LFS hsm_set --norelease $FILE || error "could not add flag"
	$LFS hsm_release $FILE && error "release should not succeed"
	$LFS hsm_clear --norelease $FILE || error "could not remove flag"

	# Could not release a file if lost
	$LFS hsm_set --lost $FILE || error "could not add flag"
	$LFS hsm_release $FILE && error "release should not succeed"
	$LFS hsm_clear --lost $FILE || error "could not remove flag"

	# Could not release a file if dirty
	$LFS hsm_set --dirty $FILE || error "could not add flag"
	$LFS hsm_release $FILE && error "release should not succeed"
	$LFS hsm_clear --dirty $FILE || error "could not remove flag"

}
run_test 20 "Release is not permitted"

test_21a() {
	# test needs a running copytool
#	copytool_setup

	mkdir -p $DIR/$tdir

	LFILE=$DIR/$tdir/test_release

	# Create a file and check its states
	fid=$(make_small $LFILE)
	$LFS hsm_state $LFILE | grep -q " (0x00000000)" \
	   || error "wrong clean hsm state"

#	$LFS hsm_archive $LFILE || error "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $LFILE || error "could not archive file"

	[ $(stat -c "%b" $LFILE) -ne "0" ] || error "wrong block number"
	FSIZE=$(stat -c "%s" $LFILE)
	[ $FSIZE -ne "0" ] || error "file size should not be zero"

	# Release and check states
	$LFS hsm_release $LFILE || error "could not release file"
	$LFS hsm_state $LFILE | grep -q " (0x0000000d)" \
	   || error "wrong released hsm state"
	[ $(stat -c "%b" $LFILE) -eq "0" ] || error "wrong block number"
	[ $(stat -c "%s" $LFILE) -eq $FSIZE ] || error "wrong file size"


	# Check we can release an file without stripe info
	LFILE=$LFILE.nolov
	mcreate $LFILE
	fid=$($LFS path2fid $LFILE | tr -d [ | tr -d ])
	$LFS hsm_state $LFILE | grep -q " (0x00000000)" \
	   || error "wrong clean hsm state"

#	$LFS hsm_archive $LFILE || error "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $LFILE || error "could not archive file"

	# Release and check states
	$LFS hsm_release $LFILE || error "could not release file"
	$LFS hsm_state $LFILE | grep -q " (0x0000000d)" \
	   || error "wrong released hsm state"

#	copytool_cleanup
}
run_test 21a "Simple release tests"

test_22() {
	# test needs a running copytool
#	copytool_setup

	mkdir -p $DIR/$tdir

	LFILE=$DIR/$tdir/test_release
	LSWAP=$DIR/$tdir/test_swap

	# Create a file and check its states
	fid=$(make_small $LFILE)
	$LFS hsm_state $LFILE | grep -q " (0x00000000)" \
	   || error "wrong clean hsm state"

#	$LFS hsm_archive $LFILE || error "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $LFILE || error "could not archive file"

	# Release and check states
	$LFS hsm_release $LFILE || error "could not release file"
	$LFS hsm_state $LFILE | grep -q " (0x0000000d)" \
	   || error "wrong released hsm state"

	make_small $LSWAP || error "could not create $LSWAP"
	$LFS swap_layouts $LSWAP $LFILE && error "swap_layouts should failed"

	true
#	copytool_cleanup
}
run_test 22 "Could not swap a release file"


test_23() {
	# test needs a running copytool
#	copytool_setup

	mkdir -p $DIR/$tdir

	LFILE=$DIR/$tdir/test_mtime

	# Create a file and check its states
	fid=$(make_small $LFILE)
	$LFS hsm_state $LFILE | grep -q " (0x00000000)" \
	   || error "wrong clean hsm state"

#	$LFS hsm_archive $LFILE || error "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $LFILE || error "could not archive file"

	# Set modification time in the past
	touch -m -a -t 0012311212.59 $LFILE

	# Release and check states
	$LFS hsm_release $LFILE || error "could not release file"
	$LFS hsm_state $LFILE | grep -q " (0x0000000d)" \
	   || error "wrong released hsm state"
	[ $(stat -c "%Y" $LFILE) -eq "978261179" ] || error "bad mtime"
	[ $(stat -c "%X" $LFILE) -eq "978261179" ] || error "bad atime"

#	copytool_cleanup
}
run_test 23 "Release does not change a/mtime (utime)"

test_24() {
	# test needs a running copytool
#	copytool_setup

	mkdir -p $DIR/$tdir

	LFILE=$DIR/$tdir/test_mtime

	# Create a file and check its states
	fid=$(make_small $LFILE)
	$LFS hsm_state $LFILE | grep -q " (0x00000000)" \
	   || error "wrong clean hsm state"

	# make mtime is different
	sleep 1
	echo "append" >> $LFILE
	MTIME=$(stat -c "%Y" $LFILE)
	ATIME=$(stat -c "%X" $LFILE)

#	$LFS hsm_archive $LFILE || error "could not archive file"
#	wait_request_state $fid ARCHIVE SUCCEED
	$LFS hsm_set --archived --exist $LFILE || error "could not archive file"

	# Release and check states
	$LFS hsm_release $LFILE || error "could not release file"
	$LFS hsm_state $LFILE | grep -q " (0x0000000d)" \
	   || error "wrong released hsm state"

	[ "$(stat -c "%Y" $LFILE)" -eq "$MTIME" ] \
	   || error "mtime should be $MTIME"
#	[ "$(stat -c "%X" $LFILE)" -eq "$ATIME" ] \
#	   || error "atime should be $ATIME"

#	copytool_cleanup
}
run_test 24 "Release does not change a/mtime (i/o)"

log "cleanup: ======================================================"
cd $ORIG_PWD
check_and_cleanup_lustre
echo '=========================== finished ==============================='
[ -f "$SANITYLOG" ] && cat $SANITYLOG && grep -q FAIL $SANITYLOG && exit 1 ||
	true
echo "$0: completed"
