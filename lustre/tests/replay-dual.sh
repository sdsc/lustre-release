#!/bin/sh

set -e

LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh

init_test_env $@

. ${CONFIG:=$LUSTRE/tests/cfg/local.sh}

SETUP=${SETUP:-"setup"}
CLEANUP=${CLEANUP:-"cleanup"}

gen_config() {
    rm -f $XMLCONFIG
    add_mds mds --dev $MDSDEV --size $MDSSIZE
    if [ ! -z "$mdsfailover_HOST" ]; then
	 add_mdsfailover mds --dev $MDSDEV --size $MDSSIZE
    fi
    
    add_lov lov1 mds --stripe_sz $STRIPE_BYTES \
	--stripe_cnt $STRIPES_PER_OBJ --stripe_pattern 0
    add_ost ost --lov lov1 --dev $OSTDEV --size $OSTSIZE --failover
    add_ost ost2 --lov lov1 --dev ${OSTDEV}-2 --size $OSTSIZE  --failover
    add_client client mds --lov lov1 --path $MOUNT
}



build_test_filter

cleanup() {
    # make sure we are using the primary MDS, so the config log will
    # be able to clean up properly.
    activemds=`facet_active mds`
    if [ $activemds != "mds" ]; then
        fail mds
    fi

    umount $MOUNT2 || true
    umount $MOUNT  || true
    rmmod llite

    # b=3941
    # In mds recovery, the mds will clear orphans in ost by 
    # mds_lov_clear_orphan, which will sent the request to ost and waiting for
    # the reply, if we stop mds at this time, we will got the obd_refcount > 1 
    # errors, because mds_lov_clear_orphan grab a export of mds, 
    # so the obd_refcount of mds will not be zero. So, wait a while before
    # stop mds. This bug needs further work.
    sleep 5

    stop mds ${FORCE}
    stop ost2 ${FORCE}
    stop ost ${FORCE}  --dump cleanup-dual.log
}

if [ "$ONLY" == "cleanup" ]; then
    sysctl -w portals.debug=0
    cleanup
    exit
fi

setup() {
    gen_config
    start ost --reformat $OSTLCONFARGS 
    start ost2 --reformat $OSTLCONFARGS 
    start mds $MDSLCONFARGS --reformat
    grep " $MOUNT " /proc/mounts || zconf_mount `hostname` $MOUNT
    grep " $MOUNT2 " /proc/mounts || zconf_mount `hostname` $MOUNT2

#    echo $TIMEOUT > /proc/sys/lustre/timeout
}

$SETUP
[ "$DAEMONFILE" ] && $LCTL debug_daemon start $DAEMONFILE $DAEMONSIZE

test_1() {
    touch $MOUNT1/a
    replay_barrier mds
    touch $MOUNT2/b

    fail mds
    checkstat $MOUNT2/a || return 1
    checkstat $MOUNT1/b || return 2
    rm $MOUNT2/a $MOUNT1/b
    checkstat $MOUNT1/a && return 3
    checkstat $MOUNT2/b && return 4
    return 0
}

run_test 1 "|X| simple create"


test_2() {
    replay_barrier mds
    mkdir $MOUNT1/adir

    fail mds
    checkstat $MOUNT2/adir || return 1
    rmdir $MOUNT2/adir
    checkstat $MOUNT2/adir && return 2
    return 0
}

run_test 2 "|X| mkdir adir"

test_3() {
    replay_barrier mds
    mkdir $MOUNT1/adir
    mkdir $MOUNT2/adir/bdir

    fail mds
    checkstat $MOUNT2/adir      || return 1
    checkstat $MOUNT1/adir/bdir || return 2
    rmdir $MOUNT2/adir/bdir $MOUNT1/adir
    checkstat $MOUNT1/adir      && return 3
    checkstat $MOUNT2/adir/bdir && return 4
    return 0
}

run_test 3 "|X| mkdir adir, mkdir adir/bdir "

test_4() {
    mkdir $MOUNT1/adir
    replay_barrier mds
    mkdir $MOUNT1/adir  && return 1
    mkdir $MOUNT2/adir/bdir

    fail mds
    checkstat $MOUNT2/adir      || return 2
    checkstat $MOUNT1/adir/bdir || return 3

    rmdir $MOUNT2/adir/bdir $MOUNT1/adir
    checkstat $MOUNT1/adir      && return 4
    checkstat $MOUNT2/adir/bdir && return 5
    return 0
}

run_test 4 "|X| mkdir adir (-EEXIST), mkdir adir/bdir "


test_5() {
    # multiclient version of replay_single.sh/test_8
    mcreate $MOUNT1/a
    multiop $MOUNT2/a o_tSc &
    pid=$!
    # give multiop a chance to open
    sleep 1 
    rm -f $MOUNT1/a
    replay_barrier mds
    kill -USR1 $pid
    wait $pid || return 1

    fail mds
    [ -e $MOUNT2/a ] && return 2
    return 0
}
run_test 5 "open, unlink |X| close"


test_6() {
    mcreate $MOUNT1/a
    multiop $MOUNT2/a o_c &
    pid1=$!
    multiop $MOUNT1/a o_c &
    pid2=$!
    # give multiop a chance to open
    sleep 1 
    rm -f $MOUNT1/a
    replay_barrier mds
    kill -USR1 $pid1
    wait $pid1 || return 1

    fail mds
    kill -USR1 $pid2
    wait $pid2 || return 1
    [ -e $MOUNT2/a ] && return 2
    return 0
}
run_test 6 "open1, open2, unlink |X| close1 [fail mds] close2"

test_7() {
    mcreate $MOUNT1/a
    multiop $MOUNT2/a o_c &
    pid1=$!
    multiop $MOUNT1/a o_c &
    pid2=$!
    # give multiop a chance to open
    sleep 1
    rm -f $MOUNT1/a
    replay_barrier mds
    kill -USR1 $pid2
    wait $pid2 || return 1

    fail mds
    kill -USR1 $pid1
    wait $pid1 || return 1
    [ -e $MOUNT2/a ] && return 2
    return 0
}
run_test 7 "open1, open2, unlink |X| close2 [fail mds] close1"

test_8() {
    replay_barrier mds
    drop_reint_reply "mcreate $MOUNT1/$tfile"    || return 1
    fail mds
    checkstat $MOUNT2/$tfile || return 2
    rm $MOUNT1/$tfile || return 3

    return 0
}
run_test 8 "replay of resent request"

test_9() {
    replay_barrier mds
    mcreate $MOUNT1/$tfile-1
    mcreate $MOUNT2/$tfile-2
    # drop first reint reply
    sysctl -w lustre.fail_loc=0x80000119
    fail mds
    sysctl -w lustre.fail_loc=0

    rm $MOUNT1/$tfile-[1,2] || return 1

    return 0
}
run_test 9 "resending a replayed create"

test_10() {
    mcreate $MOUNT1/$tfile-1
    replay_barrier mds
    munlink $MOUNT1/$tfile-1
    mcreate $MOUNT2/$tfile-2
    # drop first reint reply
    sysctl -w lustre.fail_loc=0x80000119
    fail mds
    sysctl -w lustre.fail_loc=0

    checkstat $MOUNT1/$tfile-1 && return 1
    checkstat $MOUNT1/$tfile-2 || return 2
    rm $MOUNT1/$tfile-2

    return 0
}
run_test 10 "resending a replayed unlink"

test_11() {
    replay_barrier mds
    mcreate $MOUNT1/$tfile-1
    mcreate $MOUNT2/$tfile-2
    mcreate $MOUNT1/$tfile-3
    mcreate $MOUNT2/$tfile-4
    mcreate $MOUNT1/$tfile-5
    # drop all reint replies for a while
    sysctl -w lustre.fail_loc=0x0119
    facet_failover mds
    #sleep for while, let both clients reconnect and timeout
    sleep $((TIMEOUT * 2))
    sysctl -w lustre.fail_loc=0

    rm $MOUNT1/$tfile-[1-5] || return 1

    return 0
}
run_test 11 "both clients timeout during replay"

test_12() {
    replay_barrier mds

    multiop $DIR/$tfile mo_c &
    MULTIPID=$!
    sleep 5

    # drop first enqueue
    sysctl -w lustre.fail_loc=0x80000302
    facet_failover mds
    df $MOUNT || return 1
    sysctl -w lustre.fail_loc=0

    ls $DIR/$tfile
    $CHECKSTAT -t file $DIR/$tfile || return 2
    kill -USR1 $MULTIPID || return 3
    wait $MULTIPID || return 4
    rm $DIR/$tfile

    return 0
}
run_test 12 "open resend timeout"

test_13() {
    multiop $DIR/$tfile mo_c &
    MULTIPID=$!
    sleep 5

    replay_barrier mds

    kill -USR1 $MULTIPID || return 3
    wait $MULTIPID || return 4

    # drop close 
    sysctl -w lustre.fail_loc=0x80000115
    facet_failover mds
    df $MOUNT || return 1
    sysctl -w lustre.fail_loc=0

    ls $DIR/$tfile
    $CHECKSTAT -t file $DIR/$tfile || return 2
    rm $DIR/$tfile

    return 0
}
run_test 13 "close resend timeout"

test_14() {
    replay_barrier mds
    createmany -o $MOUNT1/$tfile- 25
    createmany -o $MOUNT2/$tfile-2- 1
    createmany -o $MOUNT1/$tfile-3- 25
    umount $MOUNT2

    facet_failover mds
    # expect failover to fail
    df $MOUNT && return 1

    # first 25 files shouuld have been 
    # replayed 
    unlinkmany $MOUNT1/$tfile- 25 || return 2

    zconf_mount `hostname` $MOUNT2
    return 0
}
run_test 14 "timeouts waiting for lost client during replay"

test_15() {
    replay_barrier mds
    createmany -o $MOUNT1/$tfile- 25
    createmany -o $MOUNT2/$tfile-2- 1
    umount $MOUNT2

    facet_failover mds
    df $MOUNT || return 1

    unlinkmany $MOUNT1/$tfile- 25 || return 2

    zconf_mount `hostname` $MOUNT2
    return 0
}
run_test 15 "timeout waiting for lost client during replay, 1 client completes"

test_16() {
    replay_barrier mds
    createmany -o $MOUNT1/$tfile- 25
    createmany -o $MOUNT2/$tfile-2- 1
    umount $MOUNT2

    facet_failover mds
    sleep $TIMEOUT
    facet_failover mds
    df $MOUNT || return 1

    unlinkmany $MOUNT1/$tfile- 25 || return 2

    zconf_mount `hostname` $MOUNT2
    return 0

}
run_test 16 "fail MDS during recovery (3571)"

test_17() {
    createmany -o $MOUNT1/$tfile- 25
    createmany -o $MOUNT2/$tfile-2- 1

    # Make sure the disconnect is lost
    replay_barrier ost
    umount $MOUNT2

    facet_failover ost
    sleep $TIMEOUT
    facet_failover ost
    df $MOUNT || return 1

    unlinkmany $MOUNT1/$tfile- 25 || return 2

    zconf_mount `hostname` $MOUNT2
    return 0

}
run_test 17 "fail OST during recovery (3571)"

# cleanup with blocked enqueue fails until timer elapses (MDS busy), wait for it
export NOW=0
                                                                                
test_18() {     # bug 3822 - evicting client with enqueued lock
	set -vx
	mkdir -p $MOUNT1/$tdir
	touch $MOUNT1/$tdir/f0
#define OBD_FAIL_LDLM_ENQUEUE_BLOCKED    0x30b
	statmany -s $MOUNT1/$tdir/f 500 &
	OPENPID=$!
	NOW=`date +%s`
	do_facet mds sysctl -w lustre.fail_loc=0x8000030b  # hold enqueue
	sleep 1
#define OBD_FAIL_LDLM_BL_CALLBACK        0x305
	do_facet client sysctl -w lustre.fail_loc=0x80000305  # drop cb, evict
	cancel_lru_locks MDC
	usleep 500 # wait to ensure first client is one that will be evicted
	openfile -f O_RDONLY $MOUNT2/$tdir/f0
	wait $OPENPID
	dmesg | grep "entering recovery in server" && \
		error "client not evicted" || true
}
run_test 18 "ldlm_handle_enqueue succeeds on evicted export (3822)"

if [ "$ONLY" != "setup" ]; then
	equals_msg test complete, cleaning up
	SLEEP=$((`date +%s` - $NOW))
	[ $SLEEP -lt $TIMEOUT ] && sleep $SLEEP
	$CLEANUP
fi
