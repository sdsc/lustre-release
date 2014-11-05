#!/bin/bash

set -e

# bug number:
# 2 - need to think how to similate crash at ZIL replay
ALWAYS_EXCEPT="2   $SANITY_ZIL_EXCEPT"

SAVE_PWD=$PWD
PTLDEBUG=${PTLDEBUG:--1}
LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
SETUP=${SETUP:-""}
CLEANUP=${CLEANUP:-""}
MOUNT_2=${MOUNT_2:-"yes"}
export MULTIOP=${MULTIOP:-multiop}
. $LUSTRE/tests/test-framework.sh

init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

remote_mds_nodsh && skip "remote MDS with nodsh" && exit 0

[ "$SLOW" = "no" ] && EXCEPT_SLOW="21b"

[[ $(facet_fstype $SINGLEMDS) == zfs ]] &&
# bug number for skipped test:	      LU-2230
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 21b"

build_test_filter

check_and_setup_lustre
MOUNTED=$(mounted_lustre_filesystems)
if ! $(echo $MOUNTED' ' | grep -w -q $MOUNT2' '); then
    zconf_mount $HOSTNAME $MOUNT2
    MOUNTED2=yes
fi

[ "$(facet_fstype $SINGLEMDS)" != "zfs" ] &&
	skip "only for zfs MDT" && return 0

assert_DIR
rm -rf $DIR/[df][0-9]*

[ "$DAEMONFILE" ] && $LCTL debug_daemon start $DAEMONFILE $DAEMONSIZE

# if there is no CLIENT1 defined, some tests can be ran on localhost
CLIENT1=${CLIENT1:-$HOSTNAME}
# if CLIENT2 doesn't exist then use CLIENT1 instead
# All tests should use CLIENT2 with MOUNT2 only therefore it will work if
# $CLIENT2 == CLIENT1
# Exception is the test which need two separate nodes
CLIENT2=${CLIENT2:-$CLIENT1}

start_mdt() {
	local num=$1
	local facet=mds$num
	local dev=$(mdsdevname $num)
	shift 1

	echo "start mds service on `facet_active_host $facet`"
	start $facet ${dev} $MDS_MOUNT_OPTS $@ || return 94
}

stop_mdt() {
	local num=$1
	local facet=mds$num
	local dev=$(mdsdevname $num)
	shift 1

	echo "stop mds service on `facet_active_host $facet`"
	# These tests all use non-failover stop
	stop $facet -f || return 97
}


# verify ZIL replay works
test_1() {
	replay_barrier $SINGLEMDS
	touch $MOUNT/$tfile || error "can't create a file"
	umount -f $MOUNT
	fail_abort $SINGLEMDS || error "can't restart"
	# check recovered transno
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t file $MOUNT/$tfile ||
		error "$CHECKSTAT $DIR/$tfile attribute check failed"
}
run_test 1 "ZIL recover a request from missing client"

test_2() {
	replay_barrier $SINGLEMDS
	touch $MOUNT/$tfile || error "can't create a file"
	umount $MOUNT
	#lctl set_param -n osd*.*MDT*.force_sync 1
	stop_mdt 1

#define OBD_FAIL_OSD_ZIL_REPLAY				0x198
	do_facet mds $LCTL set_param fail_loc=0x80000198
	start_mdt 1 -o abort_recovery && error "started with ZIL abort?"

	do_facet mds $LCTL set_param fail_loc=0
	start_mdt 1 -o abort_recovery || error "can't start"

	# check recovered transno
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t file $MOUNT/$tfile ||
		error "$CHECKSTAT $DIR/$tfile attribute check failed"
}
run_test 2 "crash at ZIL replay"

test_3() {
	replay_barrier $SINGLEMDS
	touch $MOUNT/$tfile || error "can't create a file"
	umount -f $MOUNT
	#lctl set_param -n osd*.*MDT*.force_sync 1
	stop_mdt 1

#define OBD_FAIL_OSD_LOG_REPLAY				0x199
	do_facet mds $LCTL set_param fail_loc=0x80000199
	start_mdt 1 -o abort_recovery && error "started with log abort?"

	do_facet mds $LCTL set_param fail_loc=0
	start_mdt 1 -o abort_recovery || error "can't start"

	# check recovered transno
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t file $MOUNT/$tfile ||
		error "$CHECKSTAT $DIR/$tfile attribute check failed"
}
run_test 3 "failed log replay"

test_4() {
	replay_barrier ost1
	cp /etc/services $MOUNT/$tfile
	sync
	cancel_lru_locks osc
	lctl set_param -n osd*.*OST*.force_sync 1
	umount -f $MOUNT
	fail_abort ost1
	# check recovered transno
	zconf_mount `hostname` $MOUNT || error "mount fais"
	cmp /etc/services $MOUNT/$tfile || error "something went wrong"
}
run_test 4 "ZIL recovers a write request from missing client"

test_5() {
	local save_nr=`cat /sys/module/obdclass/parameters/lu_cache_nr`
	replay_barrier $SINGLEMDS
	echo 10 > /sys/module/obdclass/parameters/lu_cache_nr
	mkdir $MOUNT/$tdir
	createmany -o $MOUNT/$tdir/f- 5
	unlinkmany $MOUNT/$tdir/f- 5
	umount -f $MOUNT
	fail_abort $SINGLEMDS
	# check recovered transno
	echo $save_nr > /sys/module/obdclass/parameters/lu_cache_nr
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t dir $MOUNT/$tdir ||
		error "$CHECKSTAT $DIR/$tdir attribute check failed"
}
run_test 5 "versions must not go down because of object reload"

test_6() {
	local OSTIDX=0
	local OST=$(ostname_from_index $OSTIDX)

#define OBD_FAIL_SEQ_OSP_EXHAUST	 0x1003
	replay_barrier ost1
	do_facet ost1 $LCTL set_param fail_loc=0x80001003
	mkdir $MOUNT/$tdir
	createmany -o $MOUNT/$tdir/f- 500
	do_facet ost1 $LCTL set_param fail_loc=0
	umount -f $MOUNT
	fail_abort ost1
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t dir $MOUNT/$tdir ||
		error "$CHECKSTAT $DIR/$tdir attribute check failed"
}
run_test 6 "OST object creation from ZIL (fid_is_on_ost)"

t30_job() {
	while true; do
		createmany -o $1/f- 50
		unlinkmany $1/f- 50
	done
}

test_31() {
	local PID1
	local PID2
	[ "$(facet_fstype $SINGLEMDS)" != "zfs" ] &&
		skip "only for zfs MDT" && return 0

	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl set_param -n osd*.*.zil 1"

	mkdir $MOUNT1/$tdir
	mkdir $MOUNT1/$tdir/1
	mkdir $MOUNT1/$tdir/2

	t30_job $MOUNT1/$tdir/1 &
	PID1=$!
	t30_job $MOUNT2/$tdir/2 &
	PID2=$!

	for i in `seq 10`; do
		sleep 2
		replay_barrier $SINGLEMDS
		sleep 3
		fail $SINGLEMDS
	done
	kill $PID1
	kill $PID2
	wait
}
run_test 31 "ZIL: out-of-order transactions"

# few benchmarks
test_50_single() {
	date
	rm -rf $DIR/$tdir
	sync_all_data
	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.stats clear"
	mkdir $DIR/$tdir
	createmany -o $DIR/$tdir/f $1
	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.force_sync 1"
	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl get_param -n osd*.*MDT*.stats" | grep '\(sync\|zil\)'
	date
}

test_50() {
	debugsave
	lctl set_param debug=0

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 0"
	echo "=== no ZIL ==="
	test_50_single 20000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 1"
	echo "=== with ZIL ==="
	test_50_single 20000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 0"
	echo "=== no ZIL ==="
	test_50_single 50000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 1"
	echo "=== with ZIL ==="
	test_50_single 50000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 0"
	echo "=== no ZIL ==="
	test_50_single 100000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 1"
	echo "=== with ZIL ==="
	test_50_single 100000

	do_nodes $(comma_list $(mdts_nodes)) \
	    "lctl set_param -n osd*.*MDT*.zil 1"

	rm -rf $DIR/$tdir
	debugrestore

	return 0
}
run_test 50 "check ZIL overhead"

run_dbench_and_sync() {
	sync_all_data
	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl set_param -n osd*.*.stats clear"

	local NPROC=$(grep -c ^processor /proc/cpuinfo)
	[ $NPROC -gt 2 ] && NPROC=2
	sh rundbench $NPROC

	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl get_param -n osd*.*.stats" | grep '\(sync\|zil\)'
}

test_51() {
	debugsave
	lctl set_param debug=0

	echo "=== with ZIL ==="
	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl set_param -n osd*.*.zil 1"
	run_dbench_and_sync

	echo "=== no ZIL ==="
	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl set_param -n osd*.*.zil 0"
	run_dbench_and_sync

	do_nodes $(comma_list $(all_server_nodes)) \
	    "lctl set_param -n osd*.*.zil 0"

	debugrestore
}
run_test 51

SIZE=${SIZE:-40960}
COUNT=${COUNT:-2500}
# The FSXNUM reduction for ZFS is needed until ORI-487 is fixed.
# We don't want to skip it entirely, but ZFS is VERY slow and cannot
# pass a 2500 operation dual-mount run within the time limit.
if [ "$(facet_fstype ost1)" = "zfs" ]; then
	FSXNUM=$((COUNT / 5))
	FSXP=1
elif [ "$SLOW" = "yes" ]; then
	FSXNUM=$((COUNT * 5))
	FSXP=500
else
	FSXNUM=$COUNT
	FSXP=100
fi

test_fsx() {
	local file1=$DIR1/$tfile
	local file2=$DIR2/$tfile

	# to allocate grant because it may run out due to test_15.
	lfs setstripe -c -1 $file1
	dd if=/dev/zero of=$file1 bs=$STRIPE_BYTES count=$OSTCOUNT oflag=sync
	dd if=/dev/zero of=$file2 bs=$STRIPE_BYTES count=$OSTCOUNT oflag=sync
	rm -f $file1

	lfs setstripe -c -1 $file1 # b=10919
	echo "start with $FSXP and $FSXNUM"
	fsx -c 50 -p $FSXP -N $FSXNUM -l $((SIZE * 256)) -S 0 $file1 $file2
}

test_52() {
	[[ $(facet_fstype ost1) != zfs ]] && return 0
	local soc="obdfilter.*.sync_on_lock_cancel"
	local soc_old=$(do_facet ost1 lctl get_param -n $soc | head -n1)
	local before
	local after
	FSXNUM=$COUNT
	FSXP=100

	do_nodes $(osts_nodes) lctl set_param $soc=always
	wait_delete_completed

	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl set_param -n osd*.*OST*.zil 1"
	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl set_param -n osd*.*OST*.stats clear"

	before=`date +%s`
	test_fsx
	after=`date +%s`
	echo "run completed in " $((after-before))
	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl get_param -n osd*.*OST*.stats" | grep '\(sync\|zil\)'

	rm -f $DIR1/$tfile
	wait_delete_completed

	# it's very slow w/o ZIL
	FSXNUM=$((COUNT / 5))
	FSXP=1
	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl set_param -n osd*.*OST*.zil 0"
	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl set_param -n osd*.*OST*.stats clear"
	before=`date +%s`
	test_fsx
	after=`date +%s`
	echo "run completed in " $((after-before))
	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl get_param -n osd*.*OST*.stats" | grep '\(sync\|zil\)'

	do_nodes $(comma_list $(osts_nodes)) \
	    "lctl set_param -n osd*.*OST*.zil 1"
	do_nodes $(osts_nodes) lctl set_param $soc=$soc_old
}
run_test 52 "fsx"


complete $SECONDS
#SLEEP=$((`date +%s` - $NOW))
#[ $SLEEP -lt $TIMEOUT ] && sleep $SLEEP
[ "$MOUNTED2" = yes ] && zconf_umount $HOSTNAME $MOUNT2 || true
check_and_cleanup_lustre
exit_status
