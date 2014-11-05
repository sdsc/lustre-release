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


#
#lctl get_param -n mdt.lustre-MDT*.recovery_status
# ensure ZIL replay works
test_1() {
	replay_barrier $SINGLEMDS
	touch $MOUNT/$tfile || error "can't create a file"
	umount -f $MOUNT
	fail_abort $SINGLEMDS
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

#define OBD_FAIL_OSD_ZIL_REPLAY				0x197
	do_facet mds $LCTL set_param fail_loc=0x80000197
	start_mdt 1 -o abort_recovery && error "started with ZIL abort?"

	do_facet mds $LCTL set_param fail_loc=0
	start_mdt 1 -o abort_recovery || error "can't start"

	# check recovered transno
	zconf_mount `hostname` $MOUNT || error "mount fais"
	$CHECKSTAT -t file $MOUNT/$tfile ||
		error "$CHECKSTAT $DIR/$tfile attribute check failed"
}
run_test 2 "failed ZIL replay"

test_3() {
	replay_barrier $SINGLEMDS
	touch $MOUNT/$tfile || error "can't create a file"
	umount -f $MOUNT
	#lctl set_param -n osd*.*MDT*.force_sync 1
	stop_mdt 1

#define OBD_FAIL_OSD_LOG_REPLAY				0x198
	do_facet mds $LCTL set_param fail_loc=0x80000198
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
run_test 4 "ZIL recover a write request from missing client"

#run_test 5 "replay ZIL in two steps (crash during ZIL replay)"

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
	t30_job $MOUNT1/$tdir/2 &
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

complete $SECONDS
#SLEEP=$((`date +%s` - $NOW))
#[ $SLEEP -lt $TIMEOUT ] && sleep $SLEEP
[ "$MOUNTED2" = yes ] && zconf_umount $HOSTNAME $MOUNT2 || true
check_and_cleanup_lustre
exit_status
