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

[ "$ALWAYS_EXCEPT$EXCEPT" ] &&
	echo "Skipping tests: `echo $ALWAYS_EXCEPT $EXCEPT`"

SRCDIR=`dirname $0`
export PATH=$PWD/$SRCDIR:$SRCDIR:$PWD/$SRCDIR/../utils:$PATH:/sbin
export NAME=${NAME:-local}

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

MDTDEV="${FSNAME}-MDT0000"
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

scrub_remove_recreate() {
	local devname=$1
	local mntpt=$(facet_mntpt brpt)

	# step 1: build mount point
	do_facet $SINGLEMDS mkdir -p $mntpt
	# step 2: mount dev
	do_facet $SINGLEMDS mount -t $FSTYPE $MDS_MOUNT_OPTS $devname \
		$mntpt || return 1
	# step 3: remove OI files
	do_facet $SINGLEMDS rm -f $mntpt/oi.16*
	# step 4: umount
	do_facet $SINGLEMDS umount -d $mntpt || return 2
	# OI files will be recreated when mounted as lustre next time.
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
		"cd $mntpt; getfattr -R -d -m '.*' -P . > $metaea && cd -" ||
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
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS ||
		error "(1) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(2) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ -z "$FLAGS" ] || error "(3) Expect empty flags, but got '$FLAGS'"

	mount_client $MOUNT || error "(4) Fail to start client!"

	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(5) File diff failed unexpected!"
}
run_test 0 "Do not auto trigger OI scrub for non-backup/restore case"

test_1() {
	scrub_prep 0
	scrub_remove_recreate $(mdsdevname 1) ||
		error "(1) Fail to remove/recreate!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS ||
		error "(2) Fail to start MDS!"

	sleep 3
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(3) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(4) Fail to start client!"

	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(5) File diff failed unexpected!"
}
run_test 1 "Trigger OI scrub when MDT mounts for OI files remove/recreate case"

test_2() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS ||
		error "(2) Fail to start MDS!"

	sleep 3
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(3) Expect 'completed', but got '$STATUS'"

	mount_client $MOUNT || error "(4) Fail to start client!"

	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(5) File diff failed unexpected!"
}
run_test 2 "Trigger OI scrub when MDT mounts for backup/restore case"

test_3() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] ||
		error "(4) Expect 'inconsistent', but got '$FLAGS'"
	stopall
}
run_test 3 "Do not trigger OI scrub when MDT mounts if 'noscrub' specified"

test_4() {
	scrub_prep 0
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] ||
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	mount_client $MOUNT || error "(5) Fail to start client!"

	do_facet $SINGLEMDS \
		$LCTL set_param -n osd-ldiskfs.${MDTDEV}.auto_scrub 1
	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(6) File diff failed unexpected!"

	sleep 3
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"
}
run_test 4 "Trigger OI scrub automatically if inconsistent OI mapping was found"

test_5() {
	scrub_prep 1500
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] ||
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	mount_client $MOUNT || error "(5) Fail to start client!"

	do_facet $SINGLEMDS \
		$LCTL set_param -n osd-ldiskfs.${MDTDEV}.auto_scrub 1
#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(6) File diff failed unexpected!"

	umount_client $MOUNT || error "(7) Fail to stop client!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(8) Expect 'scanning', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_CRASH	 0x191
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000191
	sleep 4
	stop $SINGLEMDS || error "(9) Fail to stop MDS!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(10) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "crashed" ] ||
		error "(11) Expect 'crashed', but got '$STATUS'"

	stop $SINGLEMDS || error "(12) Fail to stop MDS!"

#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS ||
		error "(13) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(14) Expect 'scanning', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_FATAL	 0x192
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000192
	sleep 4
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "failed" ] ||
		error "(15) Expect 'failed', but got '$STATUS'"

	mount_client $MOUNT || error "(16) Fail to start client!"

#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	stat $DIR/$tdir/${tfile}1000 ||
		error "(17) Fail to stat $DIR/$tdir/${tfile}1000!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(18) Expect 'scanning', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(19) Expect 'completed', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ -z "$FLAGS" ] || error "(20) Expect empty flags, but got '$FLAGS'"
}
run_test 5 "OI scrub state machine"

test_6() {
	scrub_prep 1000
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] ||
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	mount_client $MOUNT || error "(5) Fail to start client!"

	do_facet $SINGLEMDS \
		$LCTL set_param -n osd-ldiskfs.${MDTDEV}.auto_scrub 1
#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(6) File diff failed unexpected!"

	# Fail the OI scrub to guarantee there is at least on checkpoint
#define OBD_FAIL_OSD_SCRUB_FATAL	 0x192
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000192
	sleep 4
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "failed" ] ||
		error "(7) Expect 'failed', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	stat $DIR/$tdir/${tfile}800 ||
		error "(8) Fail to stat $DIR/$tdir/${tfile}800!"

	umount_client $MOUNT || error "(9) Fail to stop client!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(10) Expect 'scanning', but got '$STATUS'"

#define OBD_FAIL_OSD_SCRUB_CRASH	 0x191
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80000191
	sleep 4
	POSITION0=`$SHOW_SCRUB | sed -n '11'p | awk '{print $4}'`
	POSITION0=$((POSITION0 + 1))

	stop $SINGLEMDS || error "(11) Fail to stop MDS!"

#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS ||
		error "(12) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(13) Expect 'scanning', but got '$STATUS'"

	POSITION1=`$SHOW_SCRUB | sed -n '10'p |awk '{print $4}'`
	[ $POSITION0 -eq $POSITION1 ] ||
		error "(14) Expect position: $POSITION0, but got $POSITION1"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(15) Expect 'completed', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ -z "$FLAGS" ] || error "(16) Expect empty flags, but got '$FLAGS'"
}
run_test 6 "OI scrub resumes from last checkpoint"

test_7() {
	scrub_prep 500
	scrub_backup_restore $(mdsdevname 1) $tfile ||
		error "(1) Fail to backup/restore!"

	start $SINGLEMDS $(mdsdevname 1) $MDS_MOUNT_OPTS,noscrub ||
		error "(2) Fail to start MDS!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent" ] ||
		error "(4) Expect 'inconsistent', but got '$FLAGS'"

	mount_client $MOUNT || error "(5) Fail to start client!"

	do_facet $SINGLEMDS \
		$LCTL set_param -n osd-ldiskfs.${MDTDEV}.auto_scrub 1
#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x190
	diff -q $LUSTRE/tests/test-framework.sh $DIR/$tdir/test-framework.sh ||
		error "(6) File diff failed unexpected!"

	stat $DIR/$tdir/${tfile}300 ||
		error "(7) Fail to stat $DIR/$tdir/${tfile}300!"

	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "scanning" ] ||
		error "(8) Expect 'scanning', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ "$FLAGS" = "inconsistent,auto" ] ||
		error "(9) Expect 'inconsistent,auto', but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	sleep 5
	STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	[ "$STATUS" = "completed" ] ||
		error "(10) Expect 'completed', but got '$STATUS'"

	FLAGS=`$SHOW_SCRUB | sed -n '5'p | awk '{print $2}'`
	[ -z "$FLAGS" ] || error "(11) Expect empty flags, but got '$FLAGS'"
}
run_test 7 "System is available during OI scrub scanning"

# cleanup the system at last
formatall
complete $(basename $0) $SECONDS
exit_status
