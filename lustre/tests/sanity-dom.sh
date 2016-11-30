#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#

set -e

ONLY=${ONLY:-"$*"}
ALWAYS_EXCEPT="$SANITY_DOM_EXCEPT"
[ "$SLOW" = "no" ] && EXCEPT_SLOW=""
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}

. $LUSTRE/tests/test-framework.sh
CLEANUP=${CLEANUP:-:}
SETUP=${SETUP:-:}
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

MULTIOP=${MULTIOP:-multiop}
OPENFILE=${OPENFILE:-openfile}
MOUNT_2=${MOUNT_2:-"yes"}
FAIL_ON_ERROR=false

check_and_setup_lustre

# $RUNAS_ID may get set incorrectly somewhere else
if [[ $UID -eq 0 && $RUNAS_ID -eq 0 ]]; then
	skip_env "\$RUNAS_ID set to 0, but \$UID is also 0!" && exit
fi
check_runas_id $RUNAS_ID $RUNAS_GID $RUNAS

build_test_filter

DOM="yes"
DOM_SIZE=${DOM_SIZE:-"40960K"}
OSC="mdc"

lfs setstripe -P mdt -S $DOM_SIZE $DIR1

mkdir -p $MOUNT2
mount_client $MOUNT2

lctl set_param debug=0xffffffff 2> /dev/null

test_1() {
	dd if=/dev/zero of=$DIR1/$tfile bs=7k count=1 || error "write 1"
	$TRUNCATE $DIR2/$tfile 1000 || error "truncate"
	dd if=/dev/zero of=$DIR1/$tfile bs=3k count=1 seek=1 || error "write 2"
	$CHECKSTAT -t file -s 6144 $DIR2/$tfile || error "stat"
	rm $DIR1/$tfile
}
run_test 1 "write a file on one mount, truncate on the other, write again"

test_2() {
	SZ1=234852
	dd if=/dev/zero of=$DIR/$tfile bs=1M count=1 seek=4 || return 1
	dd if=/dev/zero bs=$SZ1 count=1 >> $DIR/$tfile || return 2
	dd if=$DIR/$tfile of=$DIR/${tfile}_left bs=1M skip=5 || return 3
	SZ2=`ls -l $DIR/${tfile}_left | awk '{print $5}'`
	[[ "$SZ1" -eq "$SZ2" ]] ||
		error "Error reading at the end of the file $tfile"
}
run_test 2 "Write with a seek, append, read from a single mountpoint"

# TODO: tests below are from sanityn but their adapted version should be
# put here: 31 32 34 71 75

test_sn16() {
	local file1=$DIR1/$tfile
	local file2=$DIR2/$tfile

	dd if=/dev/zero of=$file1 bs=$DOM_SIZE count=1 oflag=sync
	dd if=/dev/zero of=$file2 bs=$DOM_SIZE count=1 oflag=sync
	rm -f $file1

	touch $file1
	fsx -c 50 -p 1 -N 1000 -l $DOM_SIZE -S 0 -d -d $file1 $file2
}
run_test sn16 "$FSXNUM iterations of dual-mount fsx"

test_sanity()
{
	local SAVE_ONLY=$ONLY

	[ ! -f sanity.sh ] && skip_env "No sanity.sh skipping" && return
	# XXX: to fix 45
	ONLY="36 39 40 41 42 43 46 56r 101e 119a 131 150 155a 155b 155c \
		155d 207 241 251" OSC="mdc" DOM="yes" sh sanity.sh
	ONLY=$SAVE_ONLY
}
run_test sanity "Run sanity with Data-on-MDT files"

test_sanityn()
{
	local SAVE_ONLY=$ONLY

	[ ! -f sanity.sh ] && skip_env "No sanity.sh skipping" && return
	# XXX: to fix 60
	ONLY="1 2 4 5 6 7 8 9 10 11 12 14 17 19 20 23 27 39 51a 51c 51d" \
		OSC="mdc" DOM="yes" sh sanityn.sh
	ONLY=$SAVE_ONLY
}
run_test sanityn "Run sanityn with Data-on-MDT files"

complete $SECONDS
check_and_cleanup_lustre
exit_status
