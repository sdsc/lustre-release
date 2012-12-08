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

[ $(facet_fstype $SINGLEMDS) != ldiskfs ] &&
	skip "test LFSCK only for ldiskfs" && exit 0
[[ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.3.90) ]] &&
	skip "Need MDS version at least 2.3.90" && exit 0
require_dsh_mds || exit 0

SAVED_MDSSIZE=${MDSSIZE}
SAVED_OSTSIZE=${OSTSIZE}
# use small MDS + OST size to speed formatting time
# do not use too small MDSSIZE/OSTSIZE, which affect the default journal size
MDSSIZE=100000
OSTSIZE=100000

check_and_setup_lustre
build_test_filter

MDT_DEV="${FSNAME}-MDT0000"
MDT_DEVNAME=$(mdsdevname ${SINGLEMDS//mds/})
START_NAMESPACE="do_facet $SINGLEMDS \
		$LCTL lfsck_start -M ${MDT_DEV} -t namespace"
STOP_LFSCK="do_facet $SINGLEMDS $LCTL lfsck_stop -M ${MDT_DEV}"
SHOW_NAMESPACE="do_facet $SINGLEMDS \
		$LCTL get_param -n mdd.${MDT_DEV}.lfsck_namespace"
MOUNT_OPTS_SCRUB="-o user_xattr"
MOUNT_OPTS_NOSCRUB="-o user_xattr,noscrub"

lfsck_prep() {
	local ndirs=$1
	local nfiles=$2
	local igif=$3

	echo "formatall"
	formatall > /dev/null

	if [ ! -z $igif ]; then
		#define OBD_FAIL_FID_IGIF	0x1503
		do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1503
	fi

	echo "setupall"
	setupall > /dev/null

	echo "preparing... ${nfiles} files will be created."
	mkdir -p $DIR/$tdir
	cp $LUSTRE/tests/*.sh $DIR/$tdir/
	for ((i=0; i<${ndirs}; i++)); do
		mkdir $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j=0; j<${nfiles}; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
		mkdir $DIR/$tdir/e${i}
	done

	if [ ! -z $igif ]; then
		touch $DIR/$tdir/dummy
	fi

	echo "prepared."
	cleanup_mount $MOUNT > /dev/null || error "Fail to stop client!"
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
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

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed FID-in-dirent: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 1a "LFSCK can find and repair crashed FID-in-dirent"

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

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair missed FID-in-LMA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 1b "LFSCK can find and repair missed FID-in-LMA"

test_2a() {
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_CRASH	0x1603
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1603
	touch $DIR/$tdir/dummy

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase1/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	stat $MOUNT/.lustre/fid/$dummyfid > /dev/null ||
		error "(6) Fail to stat dummy through linkEA"
}
run_test 2a "LFSCK can find and repair crashed linkEA"

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

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^updated_phase2/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
}
run_test 2b "LFSCK can find out and remove invalid linkEA entry"

test_2c()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"
	touch $DIR/$tdir/dummy

	#define OBD_FAIL_LFSCK_LINKEA_LESS	0x1605
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1605
	ln $DIR/$tdir/dummy $DIR/$tdir/dummy2

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^ents_added/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	stat $DIR/$tdir/dummy2 > /dev/null ||
		error "(6) Fail to stat $DIR/$tdir/dummy2"
}
run_test 2c "LFSCK can find out and recover missed name entry"

test_3a()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"
	touch $DIR/$tdir/dummy

	#define OBD_FAIL_LFSCK_NLINK_MORE	0x1606
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1606
	ln $DIR/$tdir/dummy $DIR/$tdir/dummy2

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^nlinks_repaired/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	stat $DIR/$tdir/dummy2 | grep "Links: 2" > /dev/null ||
		error "(6) Fail to stat $DIR/$tdir/dummy2"
}
run_test 3a "LFSCK can find and repair incorrect object nlink (1)"

test_3b()
{
	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"
	touch $DIR/$tdir/dummy

	#define OBD_FAIL_LFSCK_NLINK_LESS	0x1607
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1607
	ln $DIR/$tdir/dummy $DIR/$tdir/dummy2

	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 3
	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(4) Expect 'completed', but got '$STATUS'"

	local repaired=$($SHOW_NAMESPACE |
			 awk '/^nlinks_repaired/ { print $2 }')
	[ $repaired -ne 1 ] ||
		error "(5) Fail to repair crashed linkEA: $repaired"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0

	stat $DIR/$tdir/dummy2 | grep "Links: 2" > /dev/null ||
		error "(6) Fail to stat $DIR/$tdir/dummy2"
}
run_test 3b "LFSCK can find and repair incorrect object nlink (2)"

test_4()
{
	lfsck_prep 5 5
	mds_backup_restore || error "(1) Fail to backup/restore!"
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

	sleep 3
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
	[ $repaired -ge 2 ] ||
		error "(9) Fail to repair crashed linkEA: $repaired"
}
run_test 4 "FID-in-dirent can be rebuilt after MDT file-level backup/restore"

test_5()
{
	lfsck_prep 1 1 1
	mds_backup_restore 1 || error "(1) Fail to backup/restore!"
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

	sleep 3
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(5) Expect 'scanning-phase1', but got '$STATUS'"

	local FLAGS=$($SHOW_NAMESPACE | awk '/^flags/ { print $2 }')
	[ "$FLAGS" == "upgrade" ] ||
		error "(6) Expect 'upgrade', but got '$FLAGS'"

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

	local dummyfid=$($LFS path2fid $DIR/$tdir/dummy)
	stat $MOUNT/.lustre/fid/$dummyfid > /dev/null ||
		error "(10) Fail to stat dummy through linkEA"
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
		[ $POSITION1 -eq 0 -a $POSITINO0 -eq $((POSITION1 + 1)) ] ||
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
	for ((i=0; i<5; i++)); do
		touch $DIR/$tdir/dummy${i}
	done

	#define OBD_FAIL_LFSCK_DELAY3		0x1602
	do_facet $SINGLEMDS $LCTL set_param fail_val=3
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1602
	$START_NAMESPACE || error "(3) Fail to start LFSCK for namespace!"

	sleep 5
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
	for ((i=0; i<5; i++)); do
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

	#define OBD_FAIL_LFSCK_DELAY2		0x1601
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1601
	$START_NAMESPACE || error "(11) Fail to start LFSCK for namespace!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(12) Expect 'scanning-phase1', but got '$STATUS'"

	#define OBD_FAIL_LFSCK_CRASH		0x160a
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x8000160a
	sleep 3

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
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1603

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

	$START_NAMESPACE -s 100 || error "(3) Fail to start LFSCK!"

	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(3) Expect 'scanning-phase1', but got '$STATUS'"

	local SPEED=$($SHOW_NAMESPACE |
		      awk '/^average_speed_phase1/ { print $2 }')
	# (100 * (10 + 1)) / 10 = 110
	[ $SPEED -gt 110 ] &&
		error "(4) Unexpected speed $SPEED, should not more than 110"

	# adjust speed limit
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 300
	sleep 10

	SPEED=$($SHOW_NAMESPACE | awk '/^average_speed_phase1/ { print $2 }')
	# (100 * (10 - 1) + 300 * (10 - 1)) / 20 = 180
	[ $SPEED -lt 180 ] &&
		error "(5) Unexpected speed $SPEED, should not less than 180"

	# (100 * (10 + 1) + 300 * (10 + 1)) / 20 = 220
	[ $SPEED -gt 220 ] &&
		error "(6) Unexpected speed $SPEED, should not more than 220"

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

	lfsck_prep 1 1
	echo "start $SINGLEMDS"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	mount_client $MOUNT || error "(2) Fail to start client!"

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i=0; i<70; i++)); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j=0; j<70; j++)); do
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
	$START_NAMESPACE -s 100 || error "(6) Fail to start LFSCK!"

	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase2" ] ||
		error "(7) Expect 'scanning-phase2', but got '$STATUS'"

	local SPEED=$($SHOW_NAMESPACE |
		      awk '/^average_speed_phase2/ { print $2 }')
	# (100 * (10 + 1)) / 10 = 110
	[ $SPEED -gt 110 ] &&
		error "(8) Unexpected speed $SPEED, should not more than 110"

	# adjust speed limit
	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 300
	sleep 10

	SPEED=$($SHOW_NAMESPACE | awk '/^average_speed_phase2/ { print $2 }')
	# (100 * (10 - 1) + 300 * (10 - 1)) / 20 = 180
	[ $SPEED -lt 180 ] &&
		error "(9) Unexpected speed $SPEED, should not less than 180"

	# (100 * (10 + 1) + 300 * (10 + 1)) / 20 = 220
	[ $SPEED -gt 220 ] &&
		error "(10) Unexpected speed $SPEED, should not more than 220"

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
	for ((i=0; i<1000; i=$((i+3)))); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j=0; j<5; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
	done

	#define OBD_FAIL_LFSCK_LINKEA_MORE	0x1604
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1604
	for ((i=1; i<1000; i=$((i+3)))); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j=0; j<5; j++)); do
			touch $DIR/$tdir/d${i}/f${j}
		done
	done

	#define OBD_FAIL_LFSCK_LINKEA_LESS	0x1605
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x1605
	for ((i=2; i<1000; i=$((i+3)))); do
		mkdir -p $DIR/$tdir/d${i}
		touch $DIR/$tdir/f${i}
		for ((j=0; j<5; j++)); do
			ln touch $DIR/$tdir/f${i} $DIR/$tdir/d${i}/f${j}
		done
	done

	umount_client $MOUNT || error "(3) Fail to stop client!"

	mount_client $MOUNT || error "(4) Fail to start client!"

	local STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "init" ] ||
		error "(5) Expect 'init', but got '$STATUS'"

	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	$START_NAMESPACE -s 100 || error "(6) Fail to start LFSCK!"

	sleep 10
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(7) Expect 'scanning-phase1', but got '$STATUS'"

	ls -ailR $MOUNT > /dev/null || error "(8) Fail to ls!"

	touch $DIR/$tdir/d198/a0 || error "(9) Fail to touch!"

	mkdir $DIR/$tdir/d199/a1 || error "(10) Fail to mkdir!"

	unlink $DIR/$tdir/d200/f2 || error "(11) Fail to unlink!"

	rmdir -rf $DIR/$tdir/d201 || error "(12) Fail to rmdir!"

	mv $DIR/$tdir/f202 $DIR/$tdir/d203/ || error "(13) Fail to rename!"

	ln $DIR/$tdir/f204 $DIR/$tdir/d205/a3 || "error (14) Fail to hardlink!"

	ln -s $DIR/$tdir/d206 $DIR/$tdir/d207/a4 ||
		"error (15) Fail to softlink!"

	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "scanning-phase1" ] ||
		error "(16) Expect 'scanning-phase1', but got '$STATUS'"

	do_facet $SINGLEMDS \
		$LCTL set_param -n mdd.${MDT_DEV}.lfsck_speed_limit 0
	sleep 5
	STATUS=$($SHOW_NAMESPACE | awk '/^status/ { print $2 }')
	[ "$STATUS" == "completed" ] ||
		error "(17) Expect 'completed', but got '$STATUS'"
}
run_test 10 "System is available during LFSCK scanning"

# restore the ${facet}_MKFS_OPTS variables
for facet in MGS MDS OST; do
	opts=SAVED_${facet}_MKFS_OPTS
	if [[ -n ${!opts} ]]; then
		eval ${facet}_MKFS_OPTS=\"${!opts}\"
	fi
done

# restore MDS/OST size
MDSSIZE=${SAVED_MDSSIZE}
OSTSIZE=${SAVED_OSTSIZE}

# cleanup the system at last
formatall

complete $SECONDS
exit_status
