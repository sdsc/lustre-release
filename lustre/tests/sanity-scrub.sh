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

START_SCRUB="do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000"
STOP_SCRUB="do_facet $SINGLEMDS $LCTL scrub_stop -M ${FSNAME}-MDT0000"
SHOW_SCRUB="do_facet $SINGLEMDS $LCTL scrub_show -M ${FSNAME}-MDT0000 -i"

scrub_prep() {
	count=$1

	formatall
	setupall
	$SHOW_SCRUB | grep "Current Status: init" || error "Fail to init test!"

	mkdir -p $DIR/$tdir
	cp $LUSTRE/tests/*.sh $DIR/$tdir
	[ $count -gt 0 ] && createmany -o $DIR/$tdir/$tfile $count

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
		error "Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "init" ] || error "expect 'init', but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"
}
run_test 0 "Do not auto trigger OI Scrub for non-backup/restore case"

test_1() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS || \
		error "Fail to start MDS!"
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "completed" ] || \
		error "expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"
}
run_test 1 "Trigger OI Scrub when MDT mounts for backup/restore case"

test_2() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null && error "File diff succeed unexpected!"
	stopall
}
run_test 2 "Do not trigger OI Scrub when MDT mounts if 'noscrub' specified"

test_3() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	$START_SCRUB || error "Fail to start OI Scrub!"
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "completed" ] || \
		error "expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"
}
run_test 3 "Start OI Scrub manually"

test_4() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param -n mdd.${FSNAME}-MDT0000.noscrub 0
	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"

	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "completed" ] || \
		error "expect 'completed', but got '$STATUS'"
}
run_test 4 "Trigger OI Scrub automatically if inconsistent OI mapping was found"

test_5() {
	scrub_prep 100
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -s 5 || \
		error "Fail to start OI Scrub!"
	sleep 6
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding', but got '$STATUS'"

	SPEED=`$SHOW_SCRUB | grep "average speed" | awk '{print $3}'`
	[ $SPEED -gt 5 ] && \
		error "Unexpected speed $SPEED, should not more than 5"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${FSNAME}-MDT0000.scrub_speed_limit 10
	sleep 6
	SPEED=`$SHOW_SCRUB | grep "average speed" | awk '{print $3}'`
	[ $SPEED -gt 5 ] || \
		error "Unexpected speed $SPEED, should not less than 5"
	[ $SPEED -gt 10 ] && \
		error "Unexpected speed $SPEED, should not more than 10"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"
}
run_test 5 "OI Scrub speed control"

test_6() {
	scrub_prep 100
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -s 5 || \
		error "Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding', but got '$STATUS'"

	$STOP_SCRUB
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "paused" ] || \
		error "expect 'paused', but got '$STATUS'"

	$START_SCRUB || error "Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding', but got '$STATUS'"

#define OBD_FAIL_MDS_SCRUB_FAILURE       0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000190
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "failed" ] || \
		error "expect 'failed', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -n on || \
		error "Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "dryrun-scanning" ] || \
		error "expect 'dryrun-scanning', but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${FSNAME}-MDT0000.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "dryrun-scanned" ] || \
		error "expect 'dryrun-scanned', but got '$STATUS'"
}
run_test 6 "OI Scrub status checking"

test_7() {
	scrub_prep 100
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -s 5 || \
		error "Fail to start OI Scrub (1)!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding', but got '$STATUS'"

	sleep 2
#define OBD_FAIL_MDS_SCRUB_FAILURE       0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000190
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "failed" ] || \
		error "expect 'failed', but got '$STATUS'"

	POSITION0=`$SHOW_SCRUB | grep "last checkpoint position" | \
		awk '{print $4}'`
	POSITION0=$(( $POSITION0 + 1 ))

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	$START_SCRUB || error "Fail to start OI Scrub (2)!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding', but got '$STATUS'"

	POSITION1=`$SHOW_SCRUB | grep "latest start position" |awk '{print $4}'`
	[ $POSITION0 -eq $POSITION1 ] || \
		error "expect position: $POSITION0, but got $POSITION1"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${FSNAME}-MDT0000.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "completed" ] || \
		error "expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"
}
run_test 7 "OI Scrub resumes from last checkpoint"

test_8() {
	scrub_prep 100
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -s 5 || \
		error "Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding' (1), but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"

	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "rebuilding" ] || \
		error "expect 'rebuilding' (2), but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${FSNAME}-MDT0000.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "completed" ] || \
		error "expect 'completed', but got '$STATUS'"
}
run_test 8 "System is available during OI Scrub scanning"

test_9() {
	scrub_prep 100
	scrub_backup_restore $(mdsdevname 1) $tfile || \
		error "Fail to backup/restore!"
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub || \
		error "Fail to start MDS!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "restored" ] || \
		error "expect 'restored', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL scrub_start -M ${FSNAME}-MDT0000 -s 5 \
                -n on || error "Fail to start OI Scrub!"
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "dryrun-scanning" ] || \
		error "expect 'dryrun-scanning' (1), but got '$STATUS'"

	mount_client $MOUNT || error "Fail to start client!"
	diff $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh > \
		/dev/null || error "File diff failed unexpected!"

	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "dryrun-scanning" ] || \
		error "expect 'dryrun-scanning' (2), but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${FSNAME}-MDT0000.scrub_speed_limit 0
	sleep 2
	STATUS=`$SHOW_SCRUB | grep "Current Status" | awk '{print $3}'`
	[ "$STATUS" = "dryrun-scanned" ] || \
		error "expect 'dryrun-scanned', but got '$STATUS'"
}
run_test 9 "System is available during OI Scrub scanning with dryrun"

# cleanup the system at last
formatall
complete $(basename $0) $SECONDS
exit_status
