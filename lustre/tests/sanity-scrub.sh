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

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

require_dsh_mds || exit 0

SAVED_MDSSIZE=${MDSSIZE}
SAVED_OSTSIZE=${OSTSIZE}
SAVED_OSTCOUNT=${OSTCOUNT}
# use small MDS + OST size to speed formatting time
# do not use too small MDSSIZE/OSTSIZE, which affect the default journal size
# 200M MDT device can guarantee uninitialized groups during the OI scrub
MDSSIZE=200000
OSTSIZE=100000
# no need too much OSTs, to reduce the format/start/stop overhead
[ $OSTCOUNT -gt 4 ] && OSTCOUNT=4

MOUNT_2=""

# build up a clean test environment.
formatall
setupall

[ $(facet_fstype $SINGLEMDS) != "ldiskfs" ] &&
	skip "test OI scrub only for ldiskfs" && check_and_cleanup_lustre &&
	exit 0
[ $(facet_fstype ost1) != "ldiskfs" ] &&
	skip "test OI scrub only for ldiskfs" && check_and_cleanup_lustre &&
	exit 0
[[ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.2.90) ]] &&
	skip "Need MDS version at least 2.2.90" && check_and_cleanup_lustre &&
	exit 0

[[ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.3.90) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 1a"

[[ $(lustre_version_code $SINGLEMDS) -le $(version_code 2.6.50) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 4"

[[ $(lustre_version_code $SINGLEMDS) -le $(version_code 2.4.1) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 15"

[[ $(lustre_version_code $SINGLEMDS) -lt $(version_code 2.4.90) ]] &&
[[ $(lustre_version_code $SINGLEMDS) -ge $(version_code 2.4.50) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 15"

[[ $(lustre_version_code ost1) -lt $(version_code 2.4.50) ]] &&
	ALWAYS_EXCEPT="$ALWAYS_EXCEPT 11 12 13 14"

[[ $(lustre_version_code $SINGLEMDS) -ge $(version_code 2.5.59) ]] &&
	SCRUB_ONLY="-t scrub"

build_test_filter

MDT_DEV="${FSNAME}-MDT0000"
OST_DEV="${FSNAME}-OST0000"
MDT_DEVNAME=$(mdsdevname ${SINGLEMDS//mds/})

scrub_start() {
	local error_id=$1
	local n

	# use "lfsck_start -A" when we no longer need testing interop
	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL lfsck_start -M $(facet_svc mds$n) \
			$SCRUB_ONLY "$@" ||
			error "($error_id) Failed to start OI scrub on mds$n"
	done
}

scrub_stop() {
	local error_id=$1
	local n

	# use "lfsck_stop -A" when we no longer need testing interop
	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL lfsck_stop -M $(facet_svc mds$n) ||
			error "($error_id) Failed to stop OI scrub on mds$n"
	done
}

scrub_status() {
	local n=$1

	do_facet mds$n $LCTL get_param -n \
		osd-ldiskfs.$(facet_svc mds$n).oi_scrub
}

START_SCRUB="do_facet $SINGLEMDS $LCTL lfsck_start -M ${MDT_DEV} $SCRUB_ONLY"
START_SCRUB_ON_OST="do_facet ost1 $LCTL lfsck_start -M ${OST_DEV} $SCRUB_ONLY"
STOP_SCRUB="do_facet $SINGLEMDS $LCTL lfsck_stop -M ${MDT_DEV}"
SHOW_SCRUB="do_facet $SINGLEMDS \
		$LCTL get_param -n osd-ldiskfs.${MDT_DEV}.oi_scrub"
SHOW_SCRUB_ON_OST="do_facet ost1 \
		$LCTL get_param -n osd-ldiskfs.${OST_DEV}.oi_scrub"
MOUNT_OPTS_SCRUB="-o user_xattr"
MOUNT_OPTS_NOSCRUB="-o user_xattr,noscrub"

scrub_prep() {
	local nfiles=$1
	local n

	check_mount_and_prep

	echo "preparing... $(date)"
	for n in $(seq $MDSCOUNT); do
		echo "creating $nfiles files on mds$n"
		if [ $n -eq 1 ]; then
			mkdir $DIR/$tdir/mds$n ||
				error "Failed to create directory mds$n"
		else
			$LFS mkdir -i $((n - 1)) $DIR/$tdir/mds$n ||
				error "Failed to create remote directory mds$n"
		fi
		cp $LUSTRE/tests/*.sh $DIR/$tdir/mds$n ||
			error "Failed to copy files to mds$n"
		if [[ $nfiles -gt 0 ]]; then
			createmany -m $DIR/$tdir/mds$n/$tfile $nfiles > \
				/dev/null || error "createmany failed on mds$n"
		fi
	done
	echo "prepared $(date)."
	cleanup_mount $MOUNT > /dev/null || error "Fail to stop client!"
	for n in $(seq $MDSCOUNT); do
		echo "stop mds$n"
		stop mds$n > /dev/null || error "Fail to stop MDS$n!"
	done
}

scrub_start_mds() {
	local error_id=$1
	local opts=$2
	local n

	for n in $(seq $MDSCOUNT); do
		start mds$n $(mdsdevname $n) $opts >/dev/null ||
			error "($error_id) Failed to start mds$n"
	done
}

scrub_stop_mds() {
	local error_id=$1
	local n

	for n in $(seq $MDSCOUNT); do
		echo "stopping mds$n"
		stop mds$n >/dev/null ||
			error "($error_id) Failed to stop mds$n"
	done
}

scrub_check_status() {
	local error_id=$1
	local expected=$2
	local n

	for n in $(seq $MDSCOUNT); do
		wait_update_facet mds$n "$LCTL get_param -n \
			osd-ldiskfs.$(facet_svc mds$n).oi_scrub |
			awk '/^status/ { print \\\$2 }'" "$expected" 6 ||
			error "($error_id) Expected '$expected' on mds$n"
	done
}

scrub_check_flags() {
	local error_id=$1
	local expected=$2
	local actual
	local n

	for n in $(seq $MDSCOUNT); do
		actual=$(do_facet mds$n $LCTL get_param -n \
			osd-ldiskfs.$(facet_svc mds$n).oi_scrub |
			awk '/^flags/ { print $2 }')
		if [ "$actual" != "$expected" ]; then
			error "($error_id) Expected '$expected' on mds$n, but" \
			       "got '$actual'"
		fi
	done
}

scrub_check_params() {
	local error_id=$1
	local expected=$2
	local actual
	local n

	for n in $(seq $MDSCOUNT); do
		actual=$(do_facet mds$n $LCTL get_param -n \
			osd-ldiskfs.$(facet_svc mds$n).oi_scrub |
			awk '/^param/ { print $2 }')
		if [ "$actual" != "$expected" ]; then
			error "($error_id) Expected '$expected' on mds$n, but" \
			       "got '$actual'"
		fi
	done
}

scrub_check_repaired() {
	local error_id=$1
	local expected=$2
	local actual
	local n

	for n in $(seq $MDSCOUNT); do
		actual=$(do_facet mds$n $LCTL get_param -n \
			osd-ldiskfs.$(facet_svc mds$n).oi_scrub |
			awk '/^updated/ { print $2 }')

		if [ $expected -eq 0 -a $actual -ne 0 ]; then
			error "($error_id) Expected no repaired on mds$n, but" \
			       "got '$actual'"
		fi

		if [ $expected -ne 0 -a $actual -lt $expected ]; then
			error "($error_id) Expected '$expected' on mds$n, but" \
			       "got '$actual'"
		fi
	done
}

scrub_check_data() {
	local error_id=$1
	local n

	for n in $(seq $MDSCOUNT); do
		diff -q $LUSTRE/tests/test-framework.sh \
			$DIR/$tdir/mds$n/test-framework.sh ||
			error "($error_id) File data check failed"
	done
}

scrub_check_data2() {
	local filename=$1
	local error_id=$2
	local n

	for n in $(seq $MDSCOUNT); do
		diff -q $LUSTRE/tests/$filename \
			$DIR/$tdir/mds$n/$filename ||
			error "($error_id) File data check failed"
	done
}

scrub_remove_ois() {
	local error_id=$1
	local index=$2
	local n

	for n in $(seq $MDSCOUNT); do
		mds_remove_ois mds$n $index ||
			error "($error_id) Failed to remove OI .$index on mds$n"
	done
}

scrub_backup_restore() {
	local error_id=$1
	local igif=$2
	local n

	for n in $(seq $MDSCOUNT); do
		mds_backup_restore mds$n $igif ||
			error "(error_id) Backup/restore on mds$n failed"
	done
}

scrub_enable_auto() {
	local n

	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL set_param -n \
			osd-ldiskfs.$(facet_svc mds$n).auto_scrub 1
	done
}

full_scrub_ratio() {
	[[ $(lustre_version_code $SINGLEMDS) -le $(version_code 2.6.50) ]] &&
		return

	local ratio=$1
	local n

	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL set_param -n \
			osd-ldiskfs.$(facet_svc mds$n).full_scrub_ratio $ratio
	done
}

full_scrub_speed() {
	local speed=$1
	local n

	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL set_param -n \
			osd-ldiskfs.$(facet_svc mds$n).full_scrub_speed $speed
	done
}

test_0() {
	scrub_prep 0
	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 1 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 2 init
	scrub_check_flags 3 ""
	mount_client $MOUNT || error "(4) Fail to start client!"
	scrub_check_data 5
}
run_test 0 "Do not auto trigger OI scrub for non-backup/restore case"

test_1a() {
	scrub_prep 0
	echo "start $SINGLEMDS without disabling OI scrub"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_SCRUB > /dev/null ||
		error "(1) Fail to start MDS!"

	local FLAGS=$($SHOW_SCRUB | awk '/^flags/ { print $2 }')
	[ -z "$FLAGS" ] || error "(3) Expect empty flags, but got '$FLAGS'"

	mount_client $MOUNT || error "(4) Fail to start client!"
	#define OBD_FAIL_OSD_FID_MAPPING			0x193
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0x193
	# update .lustre OI mapping
	touch $MOUNT/.lustre
	do_facet $SINGLEMDS $LCTL set_param fail_loc=0
	umount_client $MOUNT || error "(5) Fail to stop client!"

	echo "stop $SINGLEMDS"
	stop $SINGLEMDS > /dev/null || error "(6) Fail to stop MDS!"

	echo "start $SINGLEMDS with disabling OI scrub"
	start $SINGLEMDS $MDT_DEVNAME $MOUNT_OPTS_NOSCRUB > /dev/null ||
		error "(7) Fail to start MDS!"

	local FLAGS=$($SHOW_SCRUB | awk '/^flags/ { print $2 }')
	[ "$FLAGS" == "inconsistent" ] ||
		error "(9) Expect 'inconsistent', but got '$FLAGS'"
}
run_test 1a "Auto trigger initial OI scrub when server mounts"

test_1b() {
	scrub_prep 0
	scrub_remove_ois 1
	echo "start MDTs without disabling OI scrub"
	scrub_start_mds 2 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 3 completed
	mount_client $MOUNT || error "(4) Fail to start client!"
	scrub_check_data 5
}
run_test 1b "Trigger OI scrub when MDT mounts for OI files remove/recreate case"

test_1c() {
	local index

	# OI files to be removed:
	# idx 0: oi.16.0
	# idx 2: oi.16.{2,4,8,16,32}
	# idx 3: oi.16.{3,9,27}
	for index in 0 2 3; do
		scrub_prep 0
		scrub_remove_ois 1 $index
		echo "start MDTs with OI scrub disabled"
		scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
		scrub_check_flags 3 recreated
		scrub_start 4
		scrub_check_status 5 completed
		scrub_check_flags 6 ""
	done
}
run_test 1c "Auto detect kinds of OI file(s) removed/recreated cases"

test_2() {
	scrub_prep 0
	scrub_backup_restore 1
	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 2 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 3 completed
	mount_client $MOUNT || error "(4) Fail to start client!"
	scrub_check_data 5
}
run_test 2 "Trigger OI scrub when MDT mounts for backup/restore case"

# test_3 is obsolete, it will be covered by test_5.
test_3() {
	formatall > /dev/null
	setupall > /dev/null

	scrub_prep 0
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 3 init
	scrub_check_flags 4 inconsistent
}
#run_test 3 "Do not trigger OI scrub when MDT mounts if 'noscrub' specified"

test_4a() {
	scrub_prep 0
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto
	full_scrub_ratio 0
	scrub_check_data 6
	sleep 3

	scrub_check_status 7 completed
	scrub_check_flags 8 ""

	local -a updated0
	for n in $(seq $MDSCOUNT); do
		updated0[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
	done

	scrub_check_data2 sanity-scrub.sh 9
	sleep 3

	local -a updated1
	for n in $(seq $MDSCOUNT); do
		updated1[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -eq ${updated1[$n]} ] ||
			error "(10) NOT auto trigger full scrub as expected"
	done
}
run_test 4a "Auto trigger OI scrub if bad OI mapping was found (1)"

test_4b() {
	scrub_prep 5
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto
	full_scrub_ratio 10
	full_scrub_speed 10000
	scrub_check_data 6
	sleep 3

	scrub_check_status 7 completed
	scrub_check_flags 8 ""

	local -a updated0
	for n in $(seq $MDSCOUNT); do
		updated0[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
	done

	scrub_check_data2 sanity-scrub.sh 9
	sleep 3

	scrub_check_status 10 completed
	scrub_check_flags 11 ""

	local -a updated1
	for n in $(seq $MDSCOUNT); do
		updated1[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -lt ${updated1[$n]} ] ||
			error "(12) Auto trigger full scrub unexpectedly"
	done

	for n in $(seq $MDSCOUNT); do
		ls -l $DIR/$tdir/mds$n/*.sh > /dev/null ||
			error "(13) fail to ls"
	done
	sleep 3

	scrub_check_status 14 completed
	scrub_check_flags 15 ""

	for n in $(seq $MDSCOUNT); do
		updated0[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -gt ${updated1[$n]} ] ||
			error "(16) Auto trigger full scrub unexpectedly"
	done

	for n in $(seq $MDSCOUNT); do
		ls -l $DIR/$tdir/mds$n/${tfile}1 || error "(17) fail to ls"
	done
	sleep 3

	for n in $(seq $MDSCOUNT); do
		updated1[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -eq ${updated1[$n]} ] ||
			error "(18) NOT auto trigger full scrub as expected"
	done
}
run_test 4b "Auto trigger OI scrub if bad OI mapping was found (2)"

test_4c() {
	scrub_prep 500
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto
	full_scrub_ratio 2
	full_scrub_speed 20
	scrub_check_data 6
	sleep 3

	scrub_check_status 7 completed
	scrub_check_flags 8 ""

	local -a updated0
	for n in $(seq $MDSCOUNT); do
		updated0[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
	done

	scrub_check_data2 sanity-scrub.sh 9
	sleep 3

	scrub_check_status 10 completed
	scrub_check_flags 11 ""

	local -a updated1
	for n in $(seq $MDSCOUNT); do
		updated1[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -lt ${updated1[$n]} ] ||
			error "(12) Auto trigger full scrub unexpectedly"
	done

	for n in $(seq $MDSCOUNT); do
		ls -l $DIR/$tdir/mds$n/*.sh > /dev/null ||
			error "(13) fail to ls"
	done
	sleep 3

	scrub_check_status 14 completed
	scrub_check_flags 15 ""

	for n in $(seq $MDSCOUNT); do
		updated0[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -gt ${updated1[$n]} ] ||
			error "(16) Auto trigger full scrub unexpectedly"
	done

	for n in $(seq $MDSCOUNT); do
		ls -l $DIR/$tdir/mds$n/${tfile}1 || error "(17) fail to ls"
	done
	sleep 3

	for n in $(seq $MDSCOUNT); do
		updated1[$n]=$(scrub_status $n |
			       awk '/^sf_items_updated_prior/ { print $2 }')
		[ ${updated0[$n]} -eq ${updated1[$n]} ] ||
			error "(18) NOT auto trigger full scrub as expected"
	done
}
run_test 4c "Auto trigger OI scrub if bad OI mapping was found (3)"

test_5() {
	formatall > /dev/null
	setupall > /dev/null

	scrub_prep 1000
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 3 init
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	full_scrub_ratio 0
	scrub_check_data 6
	umount_client $MOUNT || error "(7) Fail to stop client!"
	scrub_check_status 8 scanning

	#define OBD_FAIL_OSD_SCRUB_CRASH	 0x191
	do_nodes $(comma_list $(mdts_nodes)) $LCTL set_param fail_loc=0x191

	sleep 4
	scrub_stop_mds 9

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 10 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 11 crashed
	scrub_stop_mds 12

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 13 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 14 scanning

	#define OBD_FAIL_OSD_SCRUB_FATAL	 0x192
	do_nodes $(comma_list $(mdts_nodes)) $LCTL set_param fail_loc=0x192

	scrub_check_status 15 failed
	mount_client $MOUNT || error "(16) Fail to start client!"

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	local n
	for n in $(seq $MDSCOUNT); do
		stat $DIR/$tdir/mds$n/${tfile}800 ||
			error "(17) Failed to stat mds$n/${tfile}800"
	done

	scrub_check_status 18 scanning

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 19 completed
	scrub_check_flags 20 ""
}
run_test 5 "OI scrub state machine"

test_6() {
	scrub_prep 1000
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=2 fail_loc=0x190

	full_scrub_ratio 0
	scrub_check_data 6

	# Sleep 5 sec to guarantee at least one object processed by OI scrub
	sleep 5
	# Fail the OI scrub to guarantee there is at least one checkpoint
	#define OBD_FAIL_OSD_SCRUB_FATAL	 0x192
	do_nodes $(comma_list $(mdts_nodes)) $LCTL set_param fail_loc=0x192

	scrub_check_status 7 failed

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	local n
	for n in $(seq $MDSCOUNT); do
		# stat will re-trigger OI scrub
		stat $DIR/$tdir/mds$n/${tfile}800 ||
			error "(8) Failed to stat mds$n/${tfile}800"
	done

	umount_client $MOUNT || error "(9) Fail to stop client!"
	scrub_check_status 10 scanning

	#define OBD_FAIL_OSD_SCRUB_CRASH	 0x191
	do_nodes $(comma_list $(mdts_nodes)) $LCTL set_param fail_loc=0x191

	sleep 4
	local -a position0
	for n in $(seq $MDSCOUNT); do
		position0[$n]=$(scrub_status $n |
			awk '/^last_checkpoint_position/ {print $2}')
		position0[$n]=$((${position0[$n]} + 1))
	done

	scrub_stop_mds 11

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 12 "$MOUNT_OPTS_SCRUB"

	scrub_check_status 13 scanning

	local -a position1
	for n in $(seq $MDSCOUNT); do
		position1[$n]=$(scrub_status $n |
			awk '/^latest_start_position/ {print $2}')
		if [ ${position0[$n]} -ne ${position1[$n]} ]; then
			error "(14) Expected position ${position0[$n]}, but" \
				"got ${position1[$n]}"
		fi
	done

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 15 completed
	scrub_check_flags 16 ""
}
run_test 6 "OI scrub resumes from last checkpoint"

test_7() {
	scrub_prep 500
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	full_scrub_ratio 0
	scrub_check_data 6

	local n
	for n in $(seq $MDSCOUNT); do
		stat $DIR/$tdir/mds$n/${tfile}300 ||
			error "(7) Failed to stat mds$n/${tfile}300!"
	done

	scrub_check_status 8 scanning
	scrub_check_flags 9 inconsistent,auto

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 10 completed
	scrub_check_flags ""
}
run_test 7 "System is available during OI scrub scanning"

test_8() {
	scrub_prep 128
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=1 fail_loc=0x190

	scrub_start 5
	scrub_check_status 6 scanning
	scrub_stop 7
	scrub_check_status 8 stopped
	scrub_start 9
	scrub_check_status 10 scanning

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 11 completed
	scrub_check_flags 12 ""
}
run_test 8 "Control OI scrub manually"

test_9() {
	if [ -z "$(grep "processor.*: 1" /proc/cpuinfo)" ]; then
		skip "Testing on UP system, the speed may be inaccurate."
		return 0
	fi

	scrub_prep 6000
	scrub_backup_restore 1

	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent

	local BASE_SPEED1=100
	local RUN_TIME1=10
	# OI scrub should run with full speed under inconsistent case
	scrub_start 5 -s $BASE_SPEED1

	sleep $RUN_TIME1
	scrub_check_status 6 completed
	scrub_check_flags 7 ""
	# OI scrub should run with limited speed under non-inconsistent case
	scrub_start 8 -s $BASE_SPEED1 -r

	sleep $RUN_TIME1
	scrub_check_status 9 scanning

	# Do NOT ignore that there are 1024 pre-fetched items. And there
	# may be time error, normally it should be less than 2 seconds.
	# We allow another 20% schedule error.
	local PRE_FETCHED=1024
	local TIME_DIFF=2
	# MAX_MARGIN = 1.2 = 12 / 10
	local MAX_SPEED=$(((PRE_FETCHED + BASE_SPEED1 * \
		(RUN_TIME1 + TIME_DIFF)) / RUN_TIME1 * 12 / 10))
	local n
	for n in $(seq $MDSCOUNT); do
		local SPEED=$(scrub_status $n | \
			awk '/^average_speed/ { print $2 }')
		[ $SPEED -lt $MAX_SPEED ] ||
			error "(10) Got speed $SPEED, expected less than" \
				"$MAX_SPEED"
	done

	# adjust speed limit
	local BASE_SPEED2=300
	local RUN_TIME2=10
	for n in $(seq $MDSCOUNT); do
		do_facet mds$n $LCTL set_param -n \
			mdd.$(facet_svc mds$n).lfsck_speed_limit $BASE_SPEED2
	done
	sleep $RUN_TIME2

	# MIN_MARGIN = 0.8 = 8 / 10
	local MIN_SPEED=$(((PRE_FETCHED + \
			    BASE_SPEED1 * (RUN_TIME1 - TIME_DIFF) + \
			    BASE_SPEED2 * (RUN_TIME2 - TIME_DIFF)) / \
			   (RUN_TIME1 + RUN_TIME2) * 8 / 10))
	# MAX_MARGIN = 1.2 = 12 / 10
	MAX_SPEED=$(((PRE_FETCHED + \
		      BASE_SPEED1 * (RUN_TIME1 + TIME_DIFF) + \
		      BASE_SPEED2 * (RUN_TIME2 + TIME_DIFF)) / \
		     (RUN_TIME1 + RUN_TIME2) * 12 / 10))
	for n in $(seq $MDSCOUNT); do
		SPEED=$(scrub_status $n | awk '/^average_speed/ { print $2 }')
		[ $SPEED -gt $MIN_SPEED ] ||
			error "(11) Got speed $SPEED, expected more than" \
				"$MIN_SPEED"
		[ $SPEED -lt $MAX_SPEED ] ||
			error "(12) Got speed $SPEED, expected less than" \
				"$MAX_SPEED"

		do_facet mds$n $LCTL set_param -n \
				mdd.$(facet_svc mds$n).lfsck_speed_limit 0
	done

	scrub_check_status 13 completed
}
run_test 9 "OI scrub speed control"

test_10a() {
	scrub_prep 0
	scrub_backup_restore 1
	echo "starting mds$n with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent
	mount_client $MOUNT || error "(5) Fail to start client!"
	scrub_enable_auto

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=1 fail_loc=0x190

	full_scrub_ratio 0
	scrub_check_data 6
	scrub_check_status 7 scanning
	umount_client $MOUNT || error "(8) Fail to stop client!"
	scrub_stop_mds 9
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 10 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 11 paused
	scrub_stop_mds 12
	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 13 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 14 scanning

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 15 completed
	scrub_check_flags 16 ""
}
run_test 10a "non-stopped OI scrub should auto restarts after MDS remount (1)"

# test_10b is obsolete, it will be coverded by related sanity-lfsck tests.
test_10b() {
	scrub_prep 0
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_flags 4 inconsistent

	#define OBD_FAIL_OSD_SCRUB_DELAY	 0x190
	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_val=3 fail_loc=0x190

	scrub_start 5
	scrub_check_status 6 scanning
	scrub_stop_mds 7
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 8 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 9 paused
	scrub_stop_mds 10
	echo "starting MDTs without disabling OI scrub"
	scrub_start_mds 11 "$MOUNT_OPTS_SCRUB"
	scrub_check_status 12 scanning

	do_nodes $(comma_list $(mdts_nodes)) \
		$LCTL set_param fail_loc=0 fail_val=0

	scrub_check_status 13 completed
	scrub_check_flags 14 ""
}
#run_test 10b "non-stopped OI scrub should auto restarts after MDS remount (2)"

test_11() {
	local CREATED=100
	local n

	check_mount_and_prep

	for n in $(seq $MDSCOUNT); do
		$LFS mkdir -i $((n - 1)) $DIR/$tdir/mds$n ||
			error "(1) Fail to mkdir $DIR/$tdir/mds$n"

		createmany -o $DIR/$tdir/mds$n/f $CREATED ||
			error "(2) Fail to create under $tdir/mds$n"
	done

	# reset OI scrub start point by force
	scrub_start 3 -r
	scrub_check_status 4 completed

	declare -a checked0
	declare -a checked1

	# OI scrub should skip the new created objects for the first accessing
	# notice we're creating a new llog for every OST on every startup
	# new features can make this even less stable, so we only check
	# that the number of skipped files is less than 2x the number of files
	local MAXIMUM=$((CREATED * 2))
	local MINIMUM=$((CREATED + 1)) # files + directory
	for n in $(seq $MDSCOUNT); do
		local SKIPPED=$(scrub_status $n | awk '/^noscrub/ { print $2 }')
		[ $SKIPPED -ge $MAXIMUM -o $SKIPPED -lt $MINIMUM ] &&
			error "(5) Expect [ $MINIMUM , $MAXIMUM ) objects" \
				"skipped on mds$n, but got $SKIPPED"

		checked0[$n]=$(scrub_status $n | awk '/^checked/ { print $2 }')
	done

	# reset OI scrub start point by force
	scrub_start 6 -r
	scrub_check_status 7 completed

	# OI scrub should skip the new created object only once
	for n in $(seq $MDSCOUNT); do
		SKIPPED=$(scrub_status $n | awk '/^noscrub/ { print $2 }')
		checked1[$n]=$(scrub_status $n | awk '/^checked/ { print $2 }')

		[ ${checked0[$n]} -ne ${checked1[$n]} -o $SKIPPED -eq 0 ] ||
			error "(8) Expect 0 objects skipped on mds$n, but" \
				"got $SKIPPED"
	done
}
run_test 11 "OI scrub skips the new created objects only once"

test_12() {
	check_mount_and_prep
	$SETSTRIPE -c 1 -i 0 $DIR/$tdir

	local count=$(precreated_ost_obj_count 0 0)

	#define OBD_FAIL_OSD_COMPAT_INVALID_ENTRY		0x195
	do_facet ost1 $LCTL set_param fail_loc=0x195
	createmany -o $DIR/$tdir/f $((count + 32))

	umount_client $MOUNT || error "(1) Fail to stop client!"

	stop ost1 || error "(2) Fail to stop ost1"

	#define OBD_FAIL_OST_NODESTROY		 0x233
	do_facet ost1 $LCTL set_param fail_loc=0x233

	start ost1 $(ostdevname 1) $MOUNT_OPTS_NOSCRUB ||
		error "(3) Fail to start ost1"

	mount_client $MOUNT || error "(4) Fail to start client!"

	ls -ail $DIR/$tdir > /dev/null 2>&1 && error "(5) ls should fail"

	$START_SCRUB_ON_OST -r || error "(6) Fail to start OI scrub on OST!"

	do_facet ost1 $LCTL set_param fail_loc=0
	wait_update_facet ost1 "$LCTL get_param -n \
		osd-ldiskfs.$(facet_svc ost1).oi_scrub |
		awk '/^status/ { print \\\$2 }'" "completed" 6 ||
		error "(7) Expected '$expected' on ost1"

	ls -ail $DIR/$tdir > /dev/null || {
		$SHOW_SCRUB_ON_OST
		error "(8) ls should succeed"
	}
}
run_test 12 "OI scrub can rebuild invalid /O entries"

test_13() {
	check_mount_and_prep
	$SETSTRIPE -c 1 -i 0 $DIR/$tdir

	local count=$(precreated_ost_obj_count 0 0)

	#define OBD_FAIL_OSD_COMPAT_NO_ENTRY		0x196
	do_facet ost1 $LCTL set_param fail_loc=0x196
	createmany -o $DIR/$tdir/f $((count + 32))
	do_facet ost1 $LCTL set_param fail_loc=0

	umount_client $MOUNT || error "(1) Fail to stop client!"

	stop ost1 || error "(2) Fail to stop ost1"

	start ost1 $(ostdevname 1) $MOUNT_OPTS_NOSCRUB ||
		error "(3) Fail to start ost1"

	mount_client $MOUNT || error "(4) Fail to start client!"

	ls -ail $DIR/$tdir > /dev/null 2>&1 && error "(5) ls should fail"

	$START_SCRUB_ON_OST -r || error "(6) Fail to start OI scrub on OST!"

	wait_update_facet ost1 "$LCTL get_param -n \
		osd-ldiskfs.$(facet_svc ost1).oi_scrub |
		awk '/^status/ { print \\\$2 }'" "completed" 6 ||
		error "(7) Expected '$expected' on ost1"

	ls -ail $DIR/$tdir > /dev/null || error "(8) ls should succeed"
}
run_test 13 "OI scrub can rebuild missed /O entries"

test_14() {
	check_mount_and_prep
	$SETSTRIPE -c 1 -i 0 $DIR/$tdir

	local count=$(precreated_ost_obj_count 0 0)

	#define OBD_FAIL_OSD_COMPAT_NO_ENTRY		0x196
	do_facet ost1 $LCTL set_param fail_loc=0x196
	createmany -o $DIR/$tdir/f $((count + 32))
	do_facet ost1 $LCTL set_param fail_loc=0

	umount_client $MOUNT || error "(1) Fail to stop client!"

	stop ost1 || error "(2) Fail to stop ost1"

	echo "run e2fsck"
	run_e2fsck $(facet_host ost1) $(ostdevname 1) "-y" ||
		error "(3) Fail to run e2fsck error"

	start ost1 $(ostdevname 1) $OST_MOUNT_OPTS ||
		error "(4) Fail to start ost1"

	mount_client $MOUNT || error "(5) Fail to start client!"

	local LF_REPAIRED=$($SHOW_SCRUB_ON_OST |
			    awk '/^lf_reparied/ { print $2 }')
	[ $LF_REPAIRED -gt 0 ] ||
		error "(6) Some entry under /lost+found should be repaired"

	ls -ail $DIR/$tdir > /dev/null || error "(7) ls should succeed"
}
run_test 14 "OI scrub can repair objects under lost+found"

test_15() {
	# skip test_15 for LU-4182
	[ $MDSCOUNT -ge 2 ] && skip "skip now for >= 2 MDTs" && return
	local server_version=$(lustre_version_code $SINGLEMDS)
	scrub_prep 20
	scrub_backup_restore 1
	echo "starting MDTs with OI scrub disabled"
	scrub_start_mds 2 "$MOUNT_OPTS_NOSCRUB"
	scrub_check_status 3 init
	scrub_check_flags 4 inconsistent

	# run under dryrun mode
	if [ $server_version -lt $(version_code 2.5.58) ]; then
		scrub_start 5 --dryrun on
	else
		scrub_start 5 --dryrun
	fi
	scrub_check_status 6 completed
	scrub_check_flags 7 inconsistent
	scrub_check_params 8 dryrun
	scrub_check_repaired 9 20

	# run under dryrun mode again
	if [ $server_version -lt $(version_code 2.5.58) ]; then
		scrub_start 10 --dryrun on
	else
		scrub_start 10 --dryrun
	fi
	scrub_check_status 11 completed
	scrub_check_flags 12 inconsistent
	scrub_check_params 13 dryrun
	scrub_check_repaired 14 20

	# run under normal mode
	#
	# Lustre-2.x (x <= 5) used "-n off" to disable dryrun which does not
	# work under Lustre-2.y (y >= 6), the test script should be fixed as
	# "-noff" or "--dryrun=off" or nothing by default.
	if [ $server_version -lt $(version_code 2.5.58) ]; then
		scrub_start 15 --dryrun off
	else
		scrub_start 15
	fi
	scrub_check_status 16 completed
	scrub_check_flags 17 ""
	scrub_check_params 18 ""
	scrub_check_repaired 19 20

	# run under normal mode again
	if [ $server_version -lt $(version_code 2.5.58) ]; then
		scrub_start 20 --dryrun off
	else
		scrub_start 20
	fi
	scrub_check_status 21 completed
	scrub_check_flags 22 ""
	scrub_check_params 23 ""
	scrub_check_repaired 24 0
}
run_test 15 "Dryrun mode OI scrub"

# restore MDS/OST size
MDSSIZE=${SAVED_MDSSIZE}
OSTSIZE=${SAVED_OSTSIZE}
OSTCOUNT=${SAVED_OSTCOUNT}

# cleanup the system at last
formatall

complete $SECONDS
exit_status
