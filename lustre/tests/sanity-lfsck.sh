#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#

set -e

ONLY=${ONLY:-"$*"}
ALWAYS_EXCEPT="$SANITY_LFSCK_EXCEPT"
[ "$SLOW" = "no" ] && EXCEPT_SLOW=""
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

[ $(facet_fstype $SINGLEMDS) != "ldiskfs" ] &&
	skip "test LFSCK only for ldiskfs" && exit 0
[ $(facet_fstype ost1) != ldiskfs ] &&
	skip "test LFSCK only for ldiskfs" && exit 0
require_dsh_mds || exit 0

MCREATE=${MCREATE:-mcreate}
SAVED_MDSSIZE=${MDSSIZE}
SAVED_OSTSIZE=${OSTSIZE}
# use small MDS + OST size to speed formatting time
# do not use too small MDSSIZE/OSTSIZE, which affect the default journal size
MDSSIZE=100000
OSTSIZE=100000

check_and_setup_lustre

[[ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.3.60) ]] &&
	skip "Need MDS version at least 2.3.60" && check_and_cleanup_lustre &&
	exit 0

[[ $(lustre_version_code ost1) -lt $(version_code 2.4.50) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 11 12"

build_test_filter

$LCTL set_param debug=+lfsck > /dev/null || true

MDT_DEV="${FSNAME}-MDT0000"
OST_DEV="${FSNAME}-OST0000"
OST_DEV2="${FSNAME}-OST0001"
MDT_DEVNAME=$(mdsdevname ${SINGLEMDS//mds/})
START_NAMESPACE="do_facet $SINGLEMDS \
		$LCTL lfsck_start -M ${MDT_DEV} -t namespace"
START_LAYOUT="do_facet $SINGLEMDS \
		$LCTL lfsck_start -M ${MDT_DEV} -t layout"
START_LAYOUT_ON_OST="do_facet ost1 $LCTL lfsck_start -M ${OST_DEV} -t layout"
STOP_LFSCK="do_facet $SINGLEMDS $LCTL lfsck_stop -M ${MDT_DEV}"
SHOW_NAMESPACE="do_facet $SINGLEMDS \
		$LCTL get_param -n mdd.${MDT_DEV}.lfsck_namespace"
SHOW_LAYOUT="do_facet $SINGLEMDS \
		$LCTL get_param -n mdd.${MDT_DEV}.lfsck_layout"
SHOW_LAYOUT_ON_OST="do_facet ost1 \
		$LCTL get_param -n obdfilter.${OST_DEV}.lfsck_layout"
SHOW_LAYOUT_ON_OST2="do_facet ost2 \
		$LCTL get_param -n obdfilter.${OST_DEV2}.lfsck_layout"
MOUNT_OPTS_SCRUB="-o user_xattr"
MOUNT_OPTS_NOSCRUB="-o user_xattr,noscrub"

lfsck_prep() {
	local ndirs=$1
	local nfiles=$2
	local igif=$3

	echo "formatall"
	formatall > /dev/null

	echo "setupall"
	setupall > /dev/null

	if [ ! -z $igif ]; then
		#define OBD_FAIL_FID_IGIF	0x1504
		do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1504
	fi

	echo "preparing... ${nfiles} * ${ndirs} files will be created."
	mkdir -p $DIR/$tdir
	cp $LUSTRE/tests/*.sh $DIR/
	for ((i = 0; i < ${ndirs}; i++)); do
		mkdir $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j = 0; j < ${nfiles}; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
		mkdir $DIR/$tdir/e${i}
	done

	if [ ! -z $igif ]; then
		touch $DIR/$tdir/dummy
		do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	fi

	echo "prepared."
	cleanup_mount $MOUNT > /dev/null || error "Fail to stop client!"
	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "Fail to stop MDS!"
}

test_0() {
	lfsck_prep 10 10
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	#define OBD_FAIL_LFSCK_DELAY1		0x1600
	do_facet $SINGLEMDS $LCTL set_param fail_val=3
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1600
	$START_NAMESPACE || error "(2) Fail to start LFSCK for namespace!"

	$SHOW_NAMESPACE || error "Fail to monitor LFSCK (3)"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(4) Expect 'scanning-phase1', but got '$STATUS'"

	$STOP_LFSCK || error "(5) Fail to stop LFSCK!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "stopped" ] ||
		error "(6) Expect 'stopped', but got '$STATUS'"

	$START_NAMESPACE || error "(7) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(8) Expect 'scanning-phase1', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(9) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -eq 0 ] ||
		error "(10) Expect nothing to be repaired, but got: $repaired"

	local scanned1=$($SHOW_NAMESPACE | awk '/^success_count/ { print $2 }')
	$START_NAMESPACE -r || error "(11) Fail to reset LFSCK!"
	sleep 3

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(12) Expect 'completed', but got '$STATUS'"

	local scanned2=$($SHOW_NAMESPACE | awk '/^success_count/ { print $2 }')
	[ $((scanned1 + 1)) -eq $scanned2 ] ||
		error "(13) Expect success $((scanned1 + 1)), but got $scanned2"

	echo "stopall, should NOT crash LU-3649"
	stopall > /dev/null
}
run_test 0 "Control LFSCK manually"

test_1a() {
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_FID_INDIR	0x1501
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1501
	touch $DIR/$tdir/dummy

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Fail to repair crashed FID-in-dirent: $repaired"

	mount_client $MOUNT || error "(6) Fail to start client!"

	#define OBD_FAIL_FID_LOOKUP	0x1505
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1505
	ls $DIR/$tdir/ > /dev/null || error "(7) no FID-in-dirent."

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 1a "LFSCK can find out and repair crashed FID-in-dirent"

test_1b()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_FID_INLMA	0x1502
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1502
	touch $DIR/$tdir/dummy

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT
	#define OBD_FAIL_FID_NOLMA	0x1506
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1506
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Fail to repair missed FID-in-LMA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	mount_client $MOUNT || error "(6) Fail to start client!"

	#define OBD_FAIL_FID_LOOKUP	0x1505
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1505
	stat $DIR/$tdir/dummy > /dev/null || error "(7) no FID-in-LMA."

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 1b "LFSCK can find out and repair missed FID-in-LMA"

test_2a() {
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_CRASH	0x1603
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1603
	touch $DIR/$tdir/dummy

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	mount_client $MOUNT || error "(6) Fail to start client!"

	stat $DIR/$tdir/dummy | grep "Links: 1" > /dev/null ||
		error "(7) Fail to stat $DIR/$tdir/dummy"

	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	local dummyname=$($LFS fid2path $DIR $dummyfid)
	[ "$dummyname" == "$DIR/$tdir/dummy" ] ||
		error "(8) Fail to repair linkEA: $dummyfid $dummyname"
}
run_test 2a "LFSCK can find out and repair crashed linkEA entry"

test_2b()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	touch $DIR/$tdir/dummy

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase2/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	mount_client $MOUNT || error "(6) Fail to start client!"

	stat $DIR/$tdir/dummy | grep "Links: 1" > /dev/null ||
		error "(7) Fail to stat $DIR/$tdir/dummy"

	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	local dummyname=$($LFS fid2path $DIR $dummyfid)
	[ "$dummyname" == "$DIR/$tdir/dummy" ] ||
		error "(8) Fail to repair linkEA: $dummyfid $dummyname"
}
run_test 2b "LFSCK can find out and remove invalid linkEA entry"

test_2c()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_MORE2	0x1605
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1605
	touch $DIR/$tdir/dummy

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase2/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	mount_client $MOUNT || error "(6) Fail to start client!"

	stat $DIR/$tdir/dummy | grep "Links: 1" > /dev/null ||
		error "(7) Fail to stat $DIR/$tdir/dummy"

	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	local dummyname=$($LFS fid2path $DIR $dummyfid)
	[ "$dummyname" == "$DIR/$tdir/dummy" ] ||
		error "(8) Fail to repair linkEA: $dummyfid $dummyname"
}
run_test 2c "LFSCK can find out and remove repeated linkEA entry"

test_4()
{
	lfsck_prep 3 3
	mds_backup_restore $SINGLEMDS || error "(1) Fail to backup/restore!"
	echo "start $SINGLEMDS with disabling OI scrub"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_NOSCRUB > /dev/null ||
		error "(2) Fail to start MDS!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(4) Fail to start LFSCK for namespace!"

	sleep 5
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(5) Expect 'scanning-phase1', but got '$STATUS'"

	local FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ "$FLAGS" == "inconsistent" ] ||
		error "(6) Expect 'inconsistent', but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"

	FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ -z "$FLAGS" ] || error "(8) Expect empty flags, but got '$FLAGS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -ge 9 ] ||
		error "(9) Fail to repair crashed linkEA: $repaired"

	mount_client $MOUNT || error "(10) Fail to start client!"

	#define OBD_FAIL_FID_LOOKUP	0x1505
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1505
	ls $DIR/$tdir/ > /dev/null || error "(11) no FID-in-dirent."

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 4 "FID-in-dirent can be rebuilt after MDT file-level backup/restore"

test_5()
{
	lfsck_prep 1 1 1
	mds_backup_restore $SINGLEMDS 1 || error "(1) Fail to backup/restore!"
	echo "start $SINGLEMDS with disabling OI scrub"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_NOSCRUB > /dev/null ||
		error "(2) Fail to start MDS!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(4) Fail to start LFSCK for namespace!"

	sleep 5
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(5) Expect 'scanning-phase1', but got '$STATUS'"

	local FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ "$FLAGS" == "inconsistent,upgrade" ] ||
		error "(6) Expect 'inconsistent,upgrade', but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"

	FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ -z "$FLAGS" ] || error "(8) Expect empty flags, but got '$FLAGS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -ge 2 ] ||
		error "(9) Fail to repair crashed linkEA: $repaired"

	mount_client $MOUNT || error "(10) Fail to start client!"

	#define OBD_FAIL_FID_LOOKUP	0x1505
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1505
	stat $DIR/$tdir/dummy > /dev/null || error "(11) no FID-in-LMA."

	ls $DIR/$tdir/ > /dev/null || error "(12) no FID-in-dirent."

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	local dummyname=$($LFS fid2path $DIR $dummyfid)
	[ "$dummyname" == "$DIR/$tdir/dummy" ] ||
		error "(13) Fail to generate linkEA: $dummyfid $dummyname"
}
run_test 5 "LFSCK can handle IFIG object upgrading"

test_6a() {
	lfsck_prep 10 10
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	#define OBD_FAIL_LFSCK_DELAY1		0x1600
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1600
	$START_NAMESPACE || error "(2) Fail to start LFSCK for namespace!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) Expect 'scanning-phase1', but got '$STATUS'"

	# Sleep 3 sec to guarantee at least one object processed by LFSCK
	sleep 3
	# Fail the LFSCK to guarantee there is at least one checkpoint
	#define OBD_FAIL_LFSCK_FATAL1		0x1608
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80001608
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "failed" ] ||
		error "(4) Expect 'failed', but got '$STATUS'"

	local POSITION0=$($SHOW_NAMESPACE |
			  awk '/^last_checkpoint_position/ { print $2 }' |
			  tr -d ',')

	#define OBD_FAIL_LFSCK_DELAY1		0x1600
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1600
	$START_NAMESPACE || error "(5) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(6) Expect 'scanning-phase1', but got '$STATUS'"

	local POSITION1=$($SHOW_NAMESPACE |
			  awk '/^latest_start_position/ { print $2 }' |
			  tr -d ',')
	[ $POSITION0 -lt $POSITION1 ] ||
		error "(7) Expect larger than: $POSITION0, but got $POSITION1"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(8) Expect 'completed', but got '$STATUS'"
}
run_test 6a "LFSCK resumes from last checkpoint (1)"

test_6b() {
	lfsck_prep 10 10
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(2) Fail to start LFSCK for namespace!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) Expect 'scanning-phase1', but got '$STATUS'"

	# Sleep 3 sec to guarantee at least one object processed by LFSCK
	sleep 3
	# Fail the LFSCK to guarantee there is at least one checkpoint
	#define OBD_FAIL_LFSCK_FATAL2		0x1609
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80001609
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "failed" ] ||
		error "(4) Expect 'failed', but got '$STATUS'"

	local POSITION0=$($SHOW_NAMESPACE |
			  awk '/^last_checkpoint_position/ { print $4 }')

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(5) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(6) Expect 'scanning-phase1', but got '$STATUS'"

	local POSITION1=$($SHOW_NAMESPACE |
			  awk '/^latest_start_position/ { print $4 }')
	if [ $POSITION0 -gt $POSITION1 ]; then
		[ $POSITION1 -eq 0 -a $POSITION0 -eq $((POSITION1 + 1)) ] ||
		error "(7) Expect larger than: $POSITION0, but got $POSITION1"
	fi

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(8) Expect 'completed', but got '$STATUS'"
}
run_test 6b "LFSCK resumes from last checkpoint (2)"

test_7a()
{
	lfsck_prep 10 10
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(2) Fail to start LFSCK for namespace!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) Expect 'scanning-phase1', but got '$STATUS'"

	# Sleep 3 sec to guarantee at least one object processed by LFSCK
	sleep 3
	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "(4) Fail to stop MDS!"

	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(5) Fail to start MDS!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(6) Expect 'scanning-phase1', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"
}
run_test 7a "non-stopped LFSCK should auto restarts after MDS remount (1)"

test_7b()
{
	lfsck_prep 2 2
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i = 0; i < 20; i++)); do
		touch $DIR/$tdir/dummy${i}
	done

	#define OBD_FAIL_LFSCK_DELAY3		0x1602
	do_facet $SINGLEMDS $LCTL set_param fail_val=1
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1602
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase2" ] ||
		error "(4) Expect 'scanning-phase2', but got '$STATUS'"

	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "(5) Fail to stop MDS!"

	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(6) Fail to start MDS!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase2" ] ||
		error "(7) Expect 'scanning-phase2', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(8) Expect 'completed', but got '$STATUS'"
}
run_test 7b "non-stopped LFSCK should auto restarts after MDS remount (2)"

test_8()
{
	lfsck_prep 20 20
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(2) Expect 'init', but got '$STATUS'"

	mount_client $MOUNT || error "(3) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_CRASH	0x1603
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1603
	mkdir $DIR/$tdir/crashed

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i = 0; i < 5; i++)); do
		touch $DIR/$tdir/dummy${i}
	done

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_val=2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(4) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(5) Expect 'scanning-phase1', but got '$STATUS'"

	$STOP_LFSCK || error "(6) Fail to stop LFSCK!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "stopped" ] ||
		error "(7) Expect 'stopped', but got '$STATUS'"

	$START_NAMESPACE || error "(8) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(9) Expect 'scanning-phase1', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_FATAL2		0x1609
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x80001609
	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "failed" ] ||
		error "(10) Expect 'failed', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY1		0x1600
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1600
	$START_NAMESPACE || error "(11) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(12) Expect 'scanning-phase1', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_CRASH		0x160a
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x160a
	sleep 5

	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "(13) Fail to stop MDS!"

	#define OBD_FAIL_LFSCK_NO_AUTO		0x160b
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x160b

	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(14) Fail to start MDS!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "crashed" ] ||
		error "(15) Expect 'crashed', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(16) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(17) Expect 'scanning-phase1', but got '$STATUS'"

	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "(18) Fail to stop MDS!"

	#define OBD_FAIL_LFSCK_NO_AUTO		0x160b
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x160b

	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(19) Fail to start MDS!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "paused" ] ||
		error "(20) Expect 'paused', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY3		0x1602
	do_facet $SINGLEMDS $LCTL set_param fail_val=2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1602

	$START_NAMESPACE || error "(21) Fail to start LFSCK for namespace!"
	sleep 2
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase2" ] ||
		error "(22) Expect 'scanning-phase2', but got '$STATUS'"

	local FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ "$FLAGS" == "scanned-once,inconsistent" ] ||
		error "(23) Expect 'scanned-once,inconsistent',but got '$FLAGS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	sleep 2
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(24) Expect 'completed', but got '$STATUS'"

	FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ -z "$FLAGS" ] || error "(25) Expect empty flags, but got '$FLAGS'"

}
run_test 8 "LFSCK state machine"

test_9a() {
	if [ -z "$(grep "processor.*: 1" /proc/cpuinfo)" ]; then
		skip "Testing on UP system, the speed may be inaccurate."
		return 0
	fi

	lfsck_prep 70 70
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(2) Expect 'init', but got '$STATUS'"

	local BASE_SPEED1=100
	local RUN_TIME1=10
	$START_NAMESPACE -s $BASE_SPEED1 || error "(3) Fail to start LFSCK!"

	sleep $RUN_TIME1
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) Expect 'scanning-phase1', but got '$STATUS'"

	local SPEED=$($SHOW_NAMESPACE |
		      awk '/^average_speed_phase1/ { print $2 }')

	# There may be time error, normally it should be less than 2 seconds.
	# We allow another 20% schedule error.
	local TIME_DIFF=2
	# MAX_MARGIN = 1.2 = 12 / 10
	local MAX_SPEED=$((BASE_SPEED1 * (RUN_TIME1 + TIME_DIFF) / \
			   RUN_TIME1 * 12 / 10))
	[ $SPEED -lt $MAX_SPEED ] ||
		error "(4) Got speed $SPEED, expected less than $MAX_SPEED"

	# adjust speed limit
	local BASE_SPEED2=300
	local RUN_TIME2=10
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit $BASE_SPEED2
	sleep $RUN_TIME2

	SPEED=$($SHOW_NAMESPACE | awk '/^average_speed_phase1/ { print $2 }')
	# MIN_MARGIN = 0.8 = 8 / 10
	local MIN_SPEED=$(((BASE_SPEED1 * (RUN_TIME1 - TIME_DIFF) + \
			    BASE_SPEED2 * (RUN_TIME2 - TIME_DIFF)) / \
			   (RUN_TIME1 + RUN_TIME2) * 8 / 10))
	[ $SPEED -gt $MIN_SPEED ] ||
		error "(5) Got speed $SPEED, expected more than $MIN_SPEED"

	# MAX_MARGIN = 1.2 = 12 / 10
	MAX_SPEED=$(((BASE_SPEED1 * (RUN_TIME1 + TIME_DIFF) + \
		      BASE_SPEED2 * (RUN_TIME2 + TIME_DIFF)) / \
		     (RUN_TIME1 + RUN_TIME2) * 12 / 10))
	[ $SPEED -lt $MAX_SPEED ] ||
		error "(6) Got speed $SPEED, expected less than $MAX_SPEED"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 0
	sleep 5
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"
}
run_test 9a "LFSCK speed control (1)"

test_9b() {
	if [ -z "$(grep "processor.*: 1" /proc/cpuinfo)" ]; then
		skip "Testing on UP system, the speed may be inaccurate."
		return 0
	fi

	lfsck_prep 0 0
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	echo "Another preparing... 50 * 50 files (with error) will be created."
	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i = 0; i < 50; i++)); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j = 0; j < 50; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
	done

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_NO_DOUBLESCAN	0x160c
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x160c
	$START_NAMESPACE || error "(4) Fail to start LFSCK!"

	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "stopped" ] ||
		error "(5) Expect 'stopped', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	local BASE_SPEED1=50
	local RUN_TIME1=10
	$START_NAMESPACE -s $BASE_SPEED1 || error "(6) Fail to start LFSCK!"

	sleep $RUN_TIME1
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase2" ] ||
		error "(7) Expect 'scanning-phase2', but got '$STATUS'"

	local SPEED=$($SHOW_NAMESPACE |
		      awk '/^average_speed_phase2/ { print $2 }')
	# There may be time error, normally it should be less than 2 seconds.
	# We allow another 20% schedule error.
	local TIME_DIFF=2
	# MAX_MARGIN = 1.2 = 12 / 10
	local MAX_SPEED=$((BASE_SPEED1 * (RUN_TIME1 + TIME_DIFF) / \
			  RUN_TIME1 * 12 / 10))
	[ $SPEED -lt $MAX_SPEED ] ||
		error "(8) Got speed $SPEED, expected less than $MAX_SPEED"

	# adjust speed limit
	local BASE_SPEED2=150
	local RUN_TIME2=10
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit $BASE_SPEED2
	sleep $RUN_TIME2

	SPEED=$($SHOW_NAMESPACE | awk '/^average_speed_phase2/ { print $2 }')
	# MIN_MARGIN = 0.8 = 8 / 10
	local MIN_SPEED=$(((BASE_SPEED1 * (RUN_TIME1 - TIME_DIFF) + \
			    BASE_SPEED2 * (RUN_TIME2 - TIME_DIFF)) / \
			   (RUN_TIME1 + RUN_TIME2) * 8 / 10))
	[ $SPEED -gt $MIN_SPEED ] ||
		error "(9) Got speed $SPEED, expected more than $MIN_SPEED"

	# MAX_MARGIN = 1.2 = 12 / 10
	MAX_SPEED=$(((BASE_SPEED1 * (RUN_TIME1 + TIME_DIFF) + \
		      BASE_SPEED2 * (RUN_TIME2 + TIME_DIFF)) / \
		     (RUN_TIME1 + RUN_TIME2) * 12 / 10))
	[ $SPEED -lt $MAX_SPEED ] ||
		error "(10) Got speed $SPEED, expected less than $MAX_SPEED"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 0
	sleep 5
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(11) Expect 'completed', but got '$STATUS'"
}
run_test 9b "LFSCK speed control (2)"

test_10()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_CRASH	0x1603
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1603
	for ((i = 0; i < 1000; i = $((i+2)))); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j = 0; j < 5; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
	done

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i = 1; i < 1000; i = $((i+2)))); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j = 0; j < 5; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
	done

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	ln $DIR/$tdir/f200 $DIR/$tdir/d200/dummy

	umount_client $MOUNT
	mount_client $MOUNT || error "(3) Fail to start client!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(4) Expect 'init', but got '$STATUS'"

	$START_NAMESPACE -s 100 || error "(5) Fail to start LFSCK!"

	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(6) Expect 'scanning-phase1', but got '$STATUS'"

	ls -ailR $MOUNT > /dev/null || error "(7) Fail to ls!"

	touch $DIR/$tdir/d198/a0 || error "(8) Fail to touch!"

	mkdir $DIR/$tdir/d199/a1 || error "(9) Fail to mkdir!"

	unlink $DIR/$tdir/f200 || error "(10) Fail to unlink!"

	rm -rf $DIR/$tdir/d201 || error "(11) Fail to rmdir!"

	mv $DIR/$tdir/f202 $DIR/$tdir/d203/ || error "(12) Fail to rename!"

	ln $DIR/$tdir/f204 $DIR/$tdir/d205/a3 || error "(13) Fail to hardlink!"

	ln -s $DIR/$tdir/d206 $DIR/$tdir/d207/a4 ||
		error "(14) Fail to softlink!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(15) Expect 'scanning-phase1', but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 0
	umount_client $MOUNT
	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(16) Expect 'completed', but got '$STATUS'"
}
run_test 10 "System is available during LFSCK scanning"

test_11a() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$SETSTRIPE -c 1 -i 0 $DIR/$tdir
	createmany -o $DIR/$tdir/f 64

	echo "stop ost1"
	stop ost1 > /dev/null || error "(1) Fail to stop ost1"

	ost_remove_lastid 0 || error "(2) Fail to remove LAST_ID"

	echo "start ost1"
	start ost1 $(ostdevname 1) $MOUNT_OPTS_NOSCRUB > /dev/null ||
		error "(3) Fail to start ost1"

	local STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(4) Expect 'init', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_DELAY4		0x160e
	do_facet ost1 $LCTL set_param fail_val=3
	do_facet ost1 $LCTL set_param fail_loc=0x160e

	$START_LAYOUT_ON_OST || error "(5) Fail to start LFSCK on OST!"

	wait_update_facet ost1 "$LCTL get_param -n \
		obdfilter.${OST_DEV}.lfsck_layout |
		awk '/^flags/ { print \\\$2 }'" "crashed_lastid" 60 || {
		$SHOW_LAYOUT_ON_OST
		error_noexit "(6) Failed to get the expected 'crashed_lastid'"
		return 1
	}

	do_facet ost1 $LCTL set_param fail_val=0
	do_facet ost1 $LCTL set_param fail_loc=0
	sleep 3

	STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(7) Expect 'completed', but got '$STATUS'"

	FLAGS=$($SHOW_LAYOUT_ON_OST | awk '/^flags/ { print $2 }')
	[ -z "$FLAGS" ] || error "(8) Expect empty flags, but got '$FLAGS'"
}
run_test 11a "LFSCK can rebuild lost last_id"

test_11b() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$SETSTRIPE -c 1 -i 0 $DIR/$tdir

	#define OBD_FAIL_LFSCK_SKIP_LASTID	0x160d
	do_facet ost1 $LCTL set_param fail_loc=0x160d
	createmany -o $DIR/$tdir/f 64
	local lastid1=$(do_facet ost1 "lctl get_param -n \
		obdfilter.${ost1_svc}.last_id" | grep 0x100000000 |
		awk -F: '{ print $2 }')

	umount_client $MOUNT
	echo "stop ost1"
	stop ost1 || error "(1) Fail to stop ost1"

	#define OBD_FAIL_OST_ENOSPC              0x215
	do_facet ost1 $LCTL set_param fail_val=0
	do_facet ost1 $LCTL set_param fail_loc=0x215

	echo "start ost1"
	start ost1 $(ostdevname 1) $OST_MOUNT_OPTS ||
		error "(2) Fail to start ost1"

	local STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(3) Expect 'init', but got '$STATUS'"

	for ((i = 0; i < 60; i++)); do
		lastid2=$(do_facet ost1 "lctl get_param -n \
			obdfilter.${ost1_svc}.last_id" | grep 0x100000000 |
			awk -F: '{ print $2 }')
		[ ! -z $lastid2 ] && break;
		sleep 1
	done

	[ $lastid1 -gt $lastid2 ] ||
		error "(4) expect lastid1 [ $lastid1 ] > lastid2 [ $lastid2 ]"

	$START_LAYOUT_ON_OST || error "(5) Fail to start LFSCK on OST!"
	sleep 3

	local STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(6) Expect 'completed', but got '$STATUS'"

	echo "stop ost1"
	stop ost1 || error "(7) Fail to stop ost1"

	echo "start ost1"
	start ost1 $(ostdevname 1) $OST_MOUNT_OPTS ||
		error "(8) Fail to start ost1"

	sleep 8
	lastid2=$(do_facet ost1 "lctl get_param -n \
		obdfilter.${ost1_svc}.last_id" | grep 0x100000000 |
		awk -F: '{ print $2 }')
	[ $lastid1 -eq $lastid2 ] ||
		error "(9) expect lastid1 [ $lastid1 ] == lastid2 [ $lastid2 ]"

	do_facet ost1 $LCTL set_param fail_loc=0
}
run_test 11b "LFSCK can rebuild crashed last_id"

test_12() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir

	#define OBD_FAIL_LFSCK_BAD_LMMOI	0x160f
	do_facet ost1 $LCTL set_param fail_loc=0x160f
	createmany -o $DIR/$tdir/f 32
	do_facet ost1 $LCTL set_param fail_loc=0

	echo "stopall to cleanup object cache"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_others/ { print $2 }')
	[ $repaired -eq 32 ] ||
		error "(3) Fail to repair crashed lmm_oi: $repaired"
}
run_test 12 "LFSCK can repair crashed lmm_oi"

test_13() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir

	#define OBD_FAIL_LFSCK_DANGLING	0x1610
	do_facet ost1 $LCTL set_param fail_loc=0x1610
	createmany -o $DIR/$tdir/f 64
	do_facet ost1 $LCTL set_param fail_loc=0

	echo "stopall to cleanup object cache"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	ls -ail $DIR/$tdir > /dev/null 2>&1 && error "(1) ls should fail."

	$START_LAYOUT || error "(2) Fail to start LFSCK for layout!"
	sleep 3

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(3) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_dangling/ { print $2 }')
	[ $repaired -eq 32 ] ||
		error "(4) Fail to repair dangling reference: $repaired"

	ls -ail $DIR/$tdir > /dev/null || error "(5) ls should success."
}
run_test 13 "LFSCK can repair MDT-object with dangling reference"

test_14a() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir
	touch $DIR/$tdir/guard

	#define OBD_FAIL_LFSCK_UNMATCHED_PAIR1	0x1611
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1611
	createmany -o $DIR/$tdir/f 1
	chown 1.1 $DIR/$tdir/f0
	sync
	sleep 2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	echo "stopall to cleanup object cache"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_unmatched_pair/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Fail to repair unmatched pair: $repaired"
}
run_test 14a "LFSCK can repair unmatched MDT-object/OST-object pair (1)"

test_14b() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir
	touch $DIR/$tdir/guard

	#define OBD_FAIL_LFSCK_UNMATCHED_PAIR2	0x1612
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1612
	createmany -o $DIR/$tdir/f 1
	chown 1.1 $DIR/$tdir/f0
	sync
	sleep 2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	echo "stopall to cleanup object cache"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_unmatched_pair/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Fail to repair unmatched pair: $repaired"
}
run_test 14b "LFSCK can repair unmatched MDT-object/OST-object pair (2)"

test_15() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir
	touch $DIR/$tdir/guard

	#define OBD_FAIL_LFSCK_BAD_OWNER	0x1613
	do_facet ost1 $LCTL set_param fail_loc=0x1613
	createmany -o $DIR/$tdir/f 1
	chown 1.1 $DIR/$tdir/f0
	sync
	sleep 2
	do_facet ost1 $LCTL set_param fail_loc=0

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_inconsistent_owner/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Fail to repair inconsistent owner: $repaired"
}
run_test 15 "LFSCK can repair inconsistent MDT-object/OST-object owner"

test_16() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir

	do_facet $SINGLEMDS $LCTL set_param fail_val=0
	#define OBD_FAIL_LFSCK_MULTIPLE_REF	0x1614
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1614

	touch $DIR/$tdir/guard
	chown 1.1 $DIR/$tdir/guard
	sync
	sleep 1
	createmany -o $DIR/$tdir/f 1

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	do_facet $SINGLEMDS $LCTL set_param fail_val=0

	echo "stopall to cleanup object cache"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_multiple_referenced/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Fail to repair multiple references: $repaired"

	echo "foo" > $DIR/$tdir/f0 || error "(4) Fail to write f0."
	local size=$(ls -l $DIR/$tdir/guard | awk '{ print $5 }')
	[ $size -eq 0 ] || error "Unexpected size: $size"
}
run_test 16 "LFSCK can repair multiple references"

test_17() {
	[ $MDSCOUNT -lt 2 ] &&
		skip "We need at least 2 MDSes for test_17" && exit 0

	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	for ((k = 0; k < $MDSCOUNT; k++)); do
		local STATUS=$(do_facet mds$((k + 1)) $LCTL get_param -n \
				mdd.${FSNAME}-MDT000${k}.lfsck_namespace |
				awk '/^status/ { print $2 }')
		[ "$STATUS" == "init" ] ||
			error "(1) ${k} Expect 'init', but got '$STATUS'"

		$LFS mkdir -i ${k} $DIR/${k}
		createmany -o $DIR/${k}/f 100
	done

	do_facet mds1 $LCTL lfsck_start -M ${FSNAME}-MDT0000 -t namespace -A \
		-s 10 || error "(2) Fail to start LFSCK on all devices!"

	for ((k = 0; k < $MDSCOUNT; k++)); do
		local STATUS=$(do_facet mds$((k + 1)) $LCTL get_param -n \
				mdd.${FSNAME}-MDT000${k}.lfsck_namespace |
				awk '/^status/ { print $2 }')
		[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) ${k} Expect 'scanning-phase1', but got '$STATUS'"
	done

	do_facet mds1 $LCTL lfsck_stop -M ${FSNAME}-MDT0000 -A ||
		error "(4) Fail to stop LFSCK on all devices!"

	for ((k = 0; k < $MDSCOUNT; k++)); do
		local STATUS=$(do_facet mds$((k + 1)) $LCTL get_param -n \
				mdd.${FSNAME}-MDT000${k}.lfsck_namespace |
				awk '/^status/ { print $2 }')
		[ "$STATUS" == "stopped" ] ||
			error "(5) ${k} Expect 'stopped', but got '$STATUS'"
	done

	do_facet mds1 $LCTL lfsck_start -M ${FSNAME}-MDT0000 -t namespace -A \
		-s 0 || error "(6) Fail to start LFSCK on all devices!"

	sleep 3
	for ((k = 0; k < $MDSCOUNT; k++)); do
		local STATUS=$(do_facet mds$((k + 1)) $LCTL get_param -n \
				mdd.${FSNAME}-MDT000${k}.lfsck_namespace |
				awk '/^status/ { print $2 }')
		[ "$STATUS" == "completed" ] ||
			error "(7) ${k} Expect 'completed', but got '$STATUS'"
	done
}
run_test 17 "single command to trigger LFSCK on all devices"

test_18a() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir

	echo "foo" > $DIR/$tdir/a0
	echo "guard" > $DIR/$tdir/a1

        cancel_lru_locks osc
	umount_client $MOUNT || error "(1) Fail to stop client!"
	mount_client $MOUNT || error "(2) Fail to start client!"

	do_facet ost1 $LCTL set_param -n \
		obdfilter.${FSNAME}-OST0000.fail_on_inconsistency 1
	#define OBD_FAIL_LFSCK_INVALID_PFID	0x1615
	$LCTL set_param fail_loc=0x1615

	cat $DIR/$tdir/a0 && error "(3) Read should be denied!"
	$LCTL set_param fail_loc=0
}
run_test 18a "OST-object inconsistency self detect"

test_18b() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir
	$LFS setstripe -c 1 -i 0 $DIR/$tdir

	#define OBD_FAIL_LFSCK_UNMATCHED_PAIR1	0x1611
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1611
	createmany -o $DIR/$tdir/f 1
	chown 1.1 $DIR/$tdir/f0
	sync
	sleep 2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	local repaired=$(do_facet ost1 $LCTL get_param -n \
			obdfilter.${FSNAME}-OST0000.inconsistency_self_cure |
			awk '/^repaired/ { print $2 }')
	[ $repaired -eq 0 ] || error "(1) Expected 0 repaired, but got $repaired"

	echo "foo" >> $DIR/$tdir/f0

        cancel_lru_locks osc
	umount_client $MOUNT || error "(2) Fail to stop client!"
	mount_client $MOUNT || error "(3) Fail to start client!"

	do_facet ost1 $LCTL set_param -n \
		obdfilter.${FSNAME}-OST0000.fail_on_inconsistency 1
	cat $DIR/$tdir/f0 || error "(4) Read should not be denied!"

	repaired=$(do_facet ost1 $LCTL get_param -n \
		obdfilter.${FSNAME}-OST0000.inconsistency_self_cure |
		awk '/^repaired/ { print $2 }')
	[ $repaired -eq 1 ] || error "(5) Expected 1 repaired, but got $repaired"
}
run_test 18b "OST-object inconsistency self repair"

# The target MDT-object is there, but related stripe information is partly lost,
# Re-generate the lost layout EA entries.
test_19a() {
	[ $OSTCOUNT -lt 2 ] &&
		skip "We need at least 2 OSTs for test_19a" && exit 0

	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir/a1
	mkdir -p $DIR/$tdir/a2
	$LFS setstripe -c 1 -i 0 -s 1M $DIR/$tdir/a1
	$LFS setstripe -c 2 -i 0 -s 1M $DIR/$tdir/a2
	dd if=/dev/zero of=$DIR/$tdir/a1/f1 bs=1M count=2
	dd if=/dev/zero of=$DIR/$tdir/a2/f2 bs=1M count=2
	$LFS getstripe $DIR/$tdir/a1/f1
	$LFS getstripe $DIR/$tdir/a2/f2
	sync
        cancel_lru_locks osc

	#define OBD_FAIL_LFSCK_LOST_STRIPE 0x1616
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1616
	chown 1.1 $DIR/$tdir/a1/f1
	chown 1.1 $DIR/$tdir/a2/f2
	sync
	sleep 2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	echo "stopall"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	local SIZE=$(ls -il $DIR/$tdir/a1/f1 | awk '{ print $6 }')
	[ $SIZE -ne 2097152 ] || error "(1) Expect incorrect file1 size"

	SIZE=$(ls -il $DIR/$tdir/a2/f2 | awk '{ print $6 }')
	[ $SIZE -ne 2097152 ] || error "(2) Expect incorrect file2 size"

	$START_LAYOUT || error "(3) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT_ON_OST2 | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed' on ost2, but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT_ON_OST2 |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(5) Expect 1 objects fixed on ost2, but got: $repaired"

	STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(6) Expect 'completed' on ost1, but got '$STATUS'"

	repaired=$($SHOW_LAYOUT_ON_OST |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 2 ] ||
		error "(7) Expect 2 objects fixed on ost1, but got: $repaired"

	STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(8) Expect 'completed' on mds, but got '$STATUS'"

	repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 3 ] ||
		error "(9) Expect 3 objects fixed on mds, but got: $repaired"

	$LFS getstripe $DIR/$tdir/a1/f1
	$LFS getstripe $DIR/$tdir/a2/f2

	SIZE=$(ls -il $DIR/$tdir/a1/f1 | awk '{ print $6 }')
	[ $SIZE -eq 2097152 ] ||
		error "(10) Expect correct file1 size, but got $SIZE"

	SIZE=$(ls -il $DIR/$tdir/a2/f2 | awk '{ print $6 }')
	[ $SIZE -eq 2097152 ] ||
		error "(11) Expect correct file2 size, but got $SIZE"
}
run_test 19a "Find out orphan OST-object and repair it (1)"

# The target MDT-object is lost, recreate it under /lost+found
test_19b() {
	[ $OSTCOUNT -lt 2 ] &&
		skip "We need at least 2 OSTs for test_19a" && exit 0

	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir/a1
	mkdir -p $DIR/$tdir/a2
	$LFS setstripe -c 1 -i 0 -s 1M $DIR/$tdir/a1
	$LFS setstripe -c 2 -i 0 -s 1M $DIR/$tdir/a2
	dd if=/dev/zero of=$DIR/$tdir/a1/f1 bs=1M count=2
	dd if=/dev/zero of=$DIR/$tdir/a2/f2 bs=1M count=2
	$LFS getstripe $DIR/$tdir/a1/f1
	$LFS getstripe $DIR/$tdir/a2/f2
	sync
        cancel_lru_locks osc

	#define OBD_FAIL_LFSCK_LOST_MDTOBJ	0x1617
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1617
	rm -f $DIR/$tdir/a1/f1
	rm -f $DIR/$tdir/a2/f2
	sync
	sleep 2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	echo "stopall"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT_ON_OST2 | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed' on ost2, but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT_ON_OST2 |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Expect 1 objects fixed on ost2, but got: $repaired"

	STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed' on ost1, but got '$STATUS'"

	repaired=$($SHOW_LAYOUT_ON_OST |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 2 ] ||
		error "(5) Expect 2 objects fixed on ost1, but got: $repaired"

	STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(6) Expect 'completed' on mds, but got '$STATUS'"

	repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 3 ] ||
		error "(7) Expect 3 objects fixed on mds, but got: $repaired"
}
run_test 19b "Find out orphan OST-object and repair it (2)"

# The target MDT-object layout EA slot is occpuied by new created OST-object
# when repair dangling reference case. Replace the new created OST-object is
# not modified.
test_19c() {
	echo "stopall"
	stopall > /dev/null
	echo "formatall"
	formatall > /dev/null
	echo "setupall"
	setupall > /dev/null

	mkdir -p $DIR/$tdir/a1
	$LFS setstripe -c 1 -i 0 -s 1M $DIR/$tdir/a1
	dd if=/dev/zero of=$DIR/$tdir/a1/f1 bs=1M count=1
	dd if=/dev/zero of=$DIR/$tdir/a1/f2 bs=1M count=1
	$LFS getstripe $DIR/$tdir/a1/f1
	$LFS getstripe $DIR/$tdir/a1/f2
	sync
        cancel_lru_locks osc

	#define OBD_FAIL_LFSCK_CHANGE_STRIPE	0x1618
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1618
	chown 1.1 $DIR/$tdir/a1/f2
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	rm -f $DIR/$tdir/a1/f1
	sync
	sleep 1
	echo "stopall"
	stopall > /dev/null
	echo "setupall"
	setupall > /dev/null

	$LFS getstripe $DIR/$tdir/a1/f2

	$START_LAYOUT || error "(1) Fail to start LFSCK for layout!"
	sleep 2

	local STATUS=$($SHOW_LAYOUT | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(2) Expect 'completed' on mds, but got '$STATUS'"

	local repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_dangling/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(3) Fail to repair dangling reference: $repaired"

	repaired=$($SHOW_LAYOUT |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(4) Expect 1 objects fixed on mds, but got: $repaired"

	STATUS=$($SHOW_LAYOUT_ON_OST | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(5) Expect 'completed' on ost1, but got '$STATUS'"

	repaired=$($SHOW_LAYOUT_ON_OST |
			 awk '/^repaired_orphan/ { print $2 }')
	[ $repaired -eq 1 ] ||
		error "(6) Expect 1 objects fixed on ost1, but got: $repaired"
	$LFS getstripe $DIR/$tdir/a1/f2
}
run_test 19c "Find out orphan OST-object and repair it (3)"

$LCTL set_param debug=-lfsck > /dev/null || true

# restore MDS/OST size
MDSSIZE=${SAVED_MDSSIZE}
OSTSIZE=${SAVED_OSTSIZE}

# cleanup the system at last
formatall

complete $SECONDS
exit_status
