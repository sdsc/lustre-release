#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#

set -e

ONLY=${ONLY:-"$*"}
ALWAYS_EXCEPT="$SANITY_SCRUB_EXCEPT"
[ "$SLOW" = "no" ] && EXCEPT_SLOW=""
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

[ "$ALWAYS_EXCEPT$EXCEPT" ] && \
	echo "Skipping tests: `echo $ALWAYS_EXCEPT $EXCEPT`"

SRCDIR=`dirname $0`
export PATH=$PWD/$SRCDIR:$SRCDIR:$PWD/$SRCDIR/../utils:$PATH:/sbin
export NAME=${NAME:-local}

LCTL=${LCTL:-lctl}
TMP=${TMP:-/tmp}
LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

require_dsh_mds || exit 0

check_and_setup_lustre

DIR=${DIR:-$MOUNT}
assert_DIR

build_test_filter

# XXX: under DNE mode, need some work to specify the file metadata location.

MDTDEV="${FSNAME}-MDT0000"
START_SCRUB="do_facet $SINGLEMDS $LCTL scrub_start -M ${MDTDEV} -t OI"
STOP_SCRUB="do_facet $SINGLEMDS $LCTL scrub_stop -M ${MDTDEV}"
SHOW_SCRUB="do_facet $SINGLEMDS \
                $LCTL get_param -n osd-ldiskfs.${MDTDEV}.oi_scrub"

scrub_prep() {
	local nfiles=$1

	formatall
	setupall

	mkdir -p $DIR/$tdir
	cp $LUSTRE/tests/*.sh $DIR/$tdir/
	[ ${nfiles} -gt 0 ] && createmany -o $DIR/$tdir/$tfile ${nfiles}

	umount_client $MOUNT || error "Fail to stop client!"
	stop $SINGLEMDS || error "Fail to stop MDS!"
}

scrub_backup_restore() {
	local devname=$1
	local tmpfile=$2
	local mntpt=$(facet_mntpt brpt)
	local metaea=${TMP}/${tmpfile}.ea
	local metadata=${TMP}/${tmpfile}.tgz

	# step 1: build mount point
	do_facet $SINGLEMDS mkdir -p $mntpt
	# step 2: cleanup old backup
	do_facet $SINGLEMDS rm -f $metaea $metadata
	# step 3: mount dev
	do_facet $SINGLEMDS mount -t $FSTYPE $MDS_MOUNT_OPTS $devname \
		$mntpt || return 1
	# step 4: backup metaea
	do_facet $SINGLEMDS \
		"cd $mntpt; getfattr -R -d -m '.*' -P . > $metaea && cd -" || \
		return 2
	# step 5: backup metadata
	do_facet $SINGLEMDS tar zcf $metadata -C $mntpt/ . || return 3
	# step 6: umount
	do_facet $SINGLEMDS umount -d $mntpt || return 4
	# step 7: reformat dev
	add $SINGLEMDS $(mkfs_opts mds) $FSTYPE_OPT --reformat $devname > \
		/dev/null || return 5
	# step 8: mount dev
	do_facet $SINGLEMDS mount -t $FSTYPE $MDS_MOUNT_OPTS $devname \
		$mntpt || return 6
	# step 9: restore metadata
	do_facet $SINGLEMDS tar zxfp $metadata -C $mntpt || return 7
	# step 10: restore metaea
	do_facet $SINGLEMDS \
		"cd $mntpt; setfattr --restore=$metaea && cd - " || return 8
	# step 11: remove recovery logs
	do_facet $SINGLEMDS rm -f $mntpt/OBJECTS/* $mntpt/CATALOGS || return 9
	# step 12: umount dev
	do_facet $SINGLEMDS umount -d $mntpt || return 10
	# step 13: cleanup tmp backup
	do_facet $SINGLEMDS rm -f $metaea $metadata
}

test_0() {
	scrub_prep 0
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS || \
		error "(1) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | grep "Status:" | awk '{print $2}'`
	[ -z "$STATUS" ] || error "(2) Expect empty, but got '$STATUS'"

	mount_client $MOUNT || error "(3) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(4) File diff failed unexpected!"
}
run_test 0 "Do not auto trigger OI Scrub for non-backup/restore case"

test_1() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS || \
		error "(2) Fail to start MDS!"

	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(3) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(4) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(5) File diff failed unexpected!"
}
run_test 1 "Trigger OI Scrub when MDT mounts for backup/restore case"

test_2() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	mount_client $MOUNT || error "(5) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null && error "(6) File diff succeed unexpected!"
	stopall
}
run_test 2 "Do not trigger OI Scrub when MDT mounts if 'noscrub' specified"

test_3() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	$START_SCRUB || error "(5) Fail to start OI Scrub!"
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(6) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(7) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(8) File diff failed unexpected!"
}
run_test 3 "Start OI Scrub manually"

test_4() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	do_facet $SINGLEMDS \
                $LCTL set_param -n mdd.${MDTDEV}.auto_scrub 1
	mount_client $MOUNT || error "(5) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(6) File diff failed unexpected!"

	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(7) Expect 'completed', but got '$STATUS'"
}
run_test 4 "Trigger OI Scrub automatically if inconsistent OI mapping was found"

test_5() {
	scrub_prep 1000
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

        # OI scrub should run with full speed under restored case
	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -s 100 || \
		error "(5) Fail to start OI Scrub!"
	sleep 4
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(6) Expect 'completed', but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2

        # OI scrub should run with limited speed under non-restored case
	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -s 100 -r || \
		error "(7) Fail to start OI Scrub!"
	sleep 4
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(8) Expect 'scanning', but got '$STATUS'"

        # Do NOT ignore that there are 128 pre-fetched items.
        # So the max speed may be (128 + 100 * 4) / 4 = 132
        # And there may be some time error, so the max speed may be more large.
	SPEED=`$SHOW_SCRUB | sed -n '22'p | awk '{print $3}'`
	[ $SPEED -gt 144 ] && \
		error "(9) Unexpected speed $SPEED, should not more than 144"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(10) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(11) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(12) File diff failed unexpected!"
}
run_test 5 "OI Scrub speed control"

test_6() {
	scrub_prep 2000
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	$START_SCRUB || error "(5) Fail to start OI Scrub!"
        sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(6) Expect 'completed', but got '$STATUS'"

	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -s 100 -r || \
		error "(7) Fail to start OI Scrub!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(8) Expect 'scanning', but got '$STATUS'"

	$STOP_SCRUB
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "paused" ] || \
		error "(9) Expect 'paused', but got '$STATUS'"

	$START_SCRUB || error "(10) Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(11) Expect 'scanning', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_FATAL         0x191
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000191
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "failed" ] || \
		error "(12) Expect 'failed', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2
	do_facet $SINGLEMDS
                $LCTL scrub_start -M ${MDTDEV} -t OI -n on -s 100 || \
		error "(13) Fail to start OI Scrub!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(14) Expect 'scanning', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "dryrun" ] || \
		error "(15) Expect 'dryrun', but got '$FLAGS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(16) Expect 'completed', but got '$STATUS'"
}
run_test 6 "OI Scrub status checking"

test_7() {
	scrub_prep 1000
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	$START_SCRUB || error "(5) Fail to start OI Scrub!"
        sleep 3
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(6) Expect 'completed', but got '$STATUS'"

	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -s 100 -r || \
		error "(7) Fail to start OI Scrub!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(8) Expect 'scanning', but got '$STATUS'"

	$STOP_SCRUB
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "paused" ] || \
		error "(9) Expect 'paused', but got '$STATUS'"

	POSITION0=`$SHOW_SCRUB | sed -n '15'p | awk '{print $4}'`
	POSITION0=$((POSITION0 + 1))
	$START_SCRUB || error "(10) Fail to start OI Scrub!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(11) Expect 'scanning', but got '$STATUS'"

	POSITION1=`$SHOW_SCRUB | sed -n '6'p |awk '{print $4}'`
	[ $POSITION0 -eq $POSITION1 ] || \
		error "(12) Expect position: $POSITION0, but got $POSITION1"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(13) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(14) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(15) File diff failed unexpected!"
}
run_test 7 "OI Scrub resumes from last checkpoint"

test_8() {
	scrub_prep 500
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

#define OBD_FAIL_OSD_SCRUB_DELAY         0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	$START_SCRUB || error "(5) Fail to start OI Scrub!"
	mount_client $MOUNT || error "(6) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(7) File diff failed unexpected!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(8) Expect 'scanning', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(9) Expect 'inconsistent', but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
        sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(10) Expect 'completed', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | grep "inconsistent"`
	[ -z "$FLAGS" ] || error "(11) Unexpect 'inconsistent'"
}
run_test 8 "System is available during OI Scrub scanning"

test_9() {
	scrub_prep 500
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

#define OBD_FAIL_OSD_SCRUB_DELAY         0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	do_facet $SINGLEMDS
                $LCTL scrub_start -M ${MDTDEV} -t OI -n on || \
		error "(5) Fail to start OI Scrub!"
	mount_client $MOUNT || error "(6) Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "(7) File diff failed unexpected!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(8) Expect 'scanning', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2 $3}'`
	[ "$FLAGS" = "dryrun,inconsistent" ] || \
		error "(9) Expect 'dryrun,inconsistent', but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
        sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(10) Expect 'completed', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | grep "inconsistent"`
	[ -z "$FLAGS" ] || error "(11) Unexpect 'inconsistent'"
}
run_test 9 "System is available during OI Scrub scanning with dryrun"

test_10() {
	scrub_prep 1000
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] || \
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] || \
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	$START_SCRUB || error "(5) Fail to start OI Scrub!"
        sleep 3
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(6) Expect 'completed', but got '$STATUS'"

	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -e abort -s 100 -r || \
		error "(7) Fail to start OI Scrub!"

#define OBD_FAIL_OSD_SCRUB_ERROR         0x192
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000192
        sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "failed" ] || \
		error "(8) Expect 'failed', but got '$STATUS'"

	FAILED=`$SHOW_SCRUB | sed -n '19'p | awk '{print $2}'`
        [ $FAILED -eq 1 ] || \
                error "(9) Expect failure count 1, but got $FAILED"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS \
                $LCTL scrub_start -M ${MDTDEV} -t OI -e continue -s 100 || \
		error "(10) Fail to start OI Scrub!"

	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(11) Expect 'scanning', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_ERROR         0x192
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000192
        sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] || \
		error "(12) Expect 'scanning', but got '$STATUS'"

	FAILED=`$SHOW_SCRUB | sed -n '19'p | awk '{print $2}'`
        [ $FAILED -eq 2 ] || \
                error "(13) Expect failure count 2, but got $FAILED"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDTDEV}.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | sed -n '3'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] || \
		error "(14) Expect 'completed', but got '$STATUS'"
}
run_test 10 "Verify failout flag"

# cleanup the system at last
formatall
complete $(basename $0) $SECONDS
exit_status
