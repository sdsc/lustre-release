#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#
# e.g. ONLY="22 23" or ONLY="`seq 32 39`" or EXCEPT="31"
set -e

ONLY=${ONLY:-"$*"}
# bug number for skipped test:
ALWAYS_EXCEPT=${ALWAYS_EXCEPT:-"$SANITY_SELINUX_EXCEPT"}
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

SRCDIR=$(dirname $0)
SAVE_PWD=$PWD

LUSTRE=${LUSTRE:-$(dirname $0)/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

require_dsh_mds || exit 0

[ "$SLOW" = "no" ] && EXCEPT_SLOW="xxx"

# $RUNAS_ID may get set incorrectly somewhere else
[ $UID -eq 0 -a $RUNAS_ID -eq 0 ] &&
	error "RUNAS_ID set to 0, but UID is also 0!"

#
# global variables of this sanity
#

check_selinux() {
	echo -n "Checking SELinux environment... "
	local SELINUX_STATUS=`getenforce`
	[ "$SELINUX_STATUS" == "Enforcing" ] || error "SELinux must be enabled"
	local SELINUX_POLICY=`sestatus | awk -F':' '$1 == "Loaded policy name"{print $2}' | xargs`
	if [ -z "$SELINUX_POLICY" ]; then
		SELINUX_POLICY=`sestatus | awk -F':' '$1 == "Policy from config file"{print $2}' | xargs`
	fi
	[ "$SELINUX_POLICY" == "targeted" ] || error "Accepting only targeted policy"
	echo "$SELINUX_STATUS, $SELINUX_POLICY"
}

check_selinux

# we want double mount
MOUNT_2=${MOUNT_2:-"yes"}
check_and_setup_lustre

rm -rf $DIR/[df][0-9]*

check_runas_id $RUNAS_ID $RUNAS_ID $RUNAS

build_test_filter

umask 077

check_selinux_xattr() {
	local mds=$1
	local devname=$2
	local mds_path=$3
	local mntpt=$(facet_mntpt $mds)

	mount_ldiskfs $mds || error "mount -t ldiskfs $mds failed"
	local xattrval=$(do_facet $mds getfattr -n security.selinux \
				${mntpt}/ROOT/$mds_path \
			| awk -F"=" '$1=="security.selinux" {print $2}')

	unmount_ldiskfs $mds || error "umount $mds failed"

	echo $xattrval
}


test_1() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		{ skip "Only applicable to ldiskfs-based MDTs" && return; }

	local DEVNAME=$(mdsdevname 1)
	local tdir=${DIR}/d6
	local FILENAME=${tdir}/df1
	local mds_path=${FILENAME#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	touch $FILENAME || error "cannot touch $FILENAME"

	local XATTRVAL=`check_selinux_xattr "mds1" $DEVNAME $mds_path`

	rm -f $FILENAME
	rmdir $tdir

	[ -z "$XATTRVAL" -o "x$XATTRVAL" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 1 "create file and check security.selinux xattr is set on MDT"

test_2a() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		{ skip "Only applicable to ldiskfs-based MDTs" && return; }

	local DEVNAME=$(mdsdevname 1)
	local tdir=${DIR}/d7
	local DIRNAME=${tdir}/dir1
	local mds_path=${DIRNAME#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	mkdir $DIRNAME || error "cannot mkdir $DIRNAME"

	local XATTRVAL=`check_selinux_xattr "mds1" $DEVNAME $mds_path`

	rmdir $DIRNAME
	rmdir $tdir

	[ -z "$XATTRVAL" -o "x$XATTRVAL" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 2a "create dir (mkdir) and check security.selinux xattr is set on MDT"

test_2b() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		{ skip "Only applicable to ldiskfs-based MDTs" && return; }

	local DEVNAME=$(mdsdevname 1)
	local tdir=$DIR/d7
	local DIRNAME=$tdir/dir1
	local mds_path=${DIRNAME#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	$LFS mkdir -c0 $DIRNAME || error "cannot 'lfs mkdir' $DIRNAME"

	local XATTRVAL=`check_selinux_xattr "mds1" $DEVNAME $mds_path`

	rmdir $DIRNAME

	[ -z "$XATTRVAL" -o "x$XATTRVAL" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	$LFS setdirstripe -i0 $DIRNAME || error "cannot 'lfs setdirstripe' $DIRNAME"

	local XATTRVAL=`check_selinux_xattr "mds1" $DEVNAME $mds_path`

	rmdir $DIRNAME
	rmdir $tdir

	[ -z "$XATTRVAL" -o "x$XATTRVAL" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 2b "create dir with lfs and check security.selinux xattr is set on MDT"

test_3() {
	local FILENAME=$DIR/df1

	# get current mapping of runasid, and save it
	UNAME=`getent passwd $RUNAS_ID | cut -d: -f1`
	SENAME=`semanage login -l | awk -v uname=$UNAME '$1==uname {print $2}'`
	SERANGE=`semanage login -l | awk -v uname=$UNAME '$1==uname {print $3}'`

	# change mapping of runasid to unconfined_u
	semanage login -a -s unconfined_u $UNAME ||
		error "unable to map $UNAME to unconfined_u"

	# "access" Lustre
	echo "${UNAME} mapped as unconfined_u: touch $FILENAME"
	$PDSH ${UNAME}@localhost "touch $FILENAME" ||
		error "can't touch $FILENAME"
	echo "${UNAME} mapped as unconfined_u: rm -f $FILENAME"
	$PDSH ${UNAME}@localhost "rm -f $FILENAME" ||
		error "can't remove $FILENAME"

	# restore original mapping of runasid
	if [ -n "$SENAME" ]; then
		if [ -n "$SERANGE" ]; then
			semanage login -a -s $SENAME -r $SERANGE $UNAME ||
				error "unable to restore mapping for $UNAME"
		else
			semanage login -a -s $SENAME $UNAME ||
				error "unable to restore mapping for $UNAME"
		fi
	else
		semanage login -d $UNAME
	fi

	return 0
}
run_test 3 "access with unconfined user"

test_4() {
	local FILENAME=$DIR/df1

	# get current mapping of runasid, and save it
	UNAME=`getent passwd $RUNAS_ID | cut -d: -f1`
	SENAME=`semanage login -l | awk -v uname=$UNAME '$1==uname {print $2}'`
	SERANGE=`semanage login -l | awk -v uname=$UNAME '$1==uname {print $3}'`

	# change mapping of runasid to guest_u
	semanage login -a -s guest_u $UNAME ||
		error "unable to map $UNAME to guest_u"

	# "access" Lustre
	echo "${UNAME} mapped as guest_u: touch $FILENAME"
	$PDSH ${UNAME}@localhost "touch $FILENAME" &&
		error "touch $FILENAME should have failed"

	# change mapping of runasid to user_u
	semanage login -a -s user_u $UNAME ||
		error "unable to map $UNAME to user_u"

	# "access" Lustre
	echo "${UNAME} mapped as user_u: touch $FILENAME"
	$PDSH ${UNAME}@localhost "touch $FILENAME" ||
		error "can't touch $FILENAME"
	echo "${UNAME} mapped as user_u: rm -f $FILENAME"
	$PDSH ${UNAME}@localhost "rm -f $FILENAME" ||
		error "can't remove $FILENAME"

	# restore original mapping of runasid
	if [ -n "$SENAME" ]; then
		if [ -n "$SERANGE" ]; then
			semanage login -a -s $SENAME -r $SERANGE $UNAME ||
				error "unable to restore mapping for $UNAME"
		else
			semanage login -a -s $SENAME $UNAME ||
				error "unable to restore mapping for $UNAME"
		fi
	else
		semanage login -d $UNAME
	fi

	return 0
}
run_test 4 "access with specific SELinux user"

test_5() {
	local FILENAME=$DIR/df1
	local newsecctx="nfs_t"

	# create file
	touch $FILENAME || error "cannot touch $FILENAME"

	# change sec context
	chcon -t $newsecctx $FILENAME
	ls -lZ $FILENAME

	# purge client's cache
	sync ; echo 3 > /proc/sys/vm/drop_caches

	# get sec context
	ls -lZ $FILENAME
	local secctxseen=`ls -lZ $FILENAME | awk '{print $4}' | cut -d: -f3`

	# cleanup
	rm -f $FILENAME

	[ "$newsecctx" == "$secctxseen" ] ||
		error "sec context seen from 1st mount point is not correct"

	return 0
}
run_test 5 "security context retrieval from MDT xattr"

test_10() {
	local FILENAME1=$DIR/df1
	local FILENAME2=$DIR2/df1
	local newsecctx="nfs_t"

	# create file from 1st mount point
	touch $FILENAME1 || error "cannot touch $FILENAME1"
	ls -lZ $FILENAME1

	# change sec context from 2nd mount point
	chcon -t $newsecctx $FILENAME2
	ls -lZ $FILENAME2

	# get sec context from 1st mount point
	ls -lZ $FILENAME1
	local secctxseen=`ls -lZ $FILENAME1 | awk '{print $4}' | cut -d: -f3`

	# cleanup
	rm -f $FILENAME1

	[ "$newsecctx" == "$secctxseen" ] ||
		error "sec context seen from 1st mount point is not correct"

	return 0
}
run_test 10 "[consistency] concurrent security context change"

test_20a() {
	local UNAME=`getent passwd $RUNAS_ID | cut -d: -f1`
	local FILENAME1=$DIR/df1
	local FILENAME2=$DIR2/df1
	local REQ_DELAY=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$REQ_DELAY"
	#define OBD_FAIL_LLITE_CREATE_ND_PAUSE   0x1408
	do_facet client "lctl set_param fail_loc=0x1408"

	# create file on first mount point
	$PDSH ${UNAME}@localhost "touch $FILENAME1" &
	TOUCHPID=$!
	sleep 5

	if [[ -z "`ps h -o comm -p $TOUCHPID`" ]]; then
		error "touch failed to sleep, pid=$TOUCHPID"
	fi

	# get sec info on second mount point
	if [ -e "$FILENAME2" ]; then
		SECINFO2=`ls -lZ $FILENAME2 | awk '{print $4}'`
	fi

	# get sec info on first mount point
	wait $TOUCHPID
	SECINFO1=`ls -lZ $FILENAME1 | awk '{print $4}'`

	# cleanup
	rm -f $FILENAME1

	# compare sec contexts
	[ -z "$SECINFO2" -o "$SECINFO1" == "$SECINFO2" ] || \
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20a "[atomicity] concurrent access from another client (file)"

test_20b() {
	local UNAME=`getent passwd $RUNAS_ID | cut -d: -f1`
	local DIRNAME1=$DIR/dd1
	local DIRNAME2=$DIR2/dd1
	local REQ_DELAY=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$REQ_DELAY"
	#define OBD_FAIL_LLITE_NEWNODE_PAUSE     0x1409
	do_facet client "lctl set_param fail_loc=0x1409"

	# create file on first mount point
	$PDSH ${UNAME}@localhost "mkdir $DIRNAME1" &
	MKDIRPID=$!
	sleep 5

	if [[ -z "`ps h -o comm -p $MKDIRPID`" ]]; then
		error "mkdir failed to sleep, pid=$MKDIRPID"
	fi

	# get sec info on second mount point
	if [ -e "$DIRNAME2" ]; then
		SECINFO2=`ls -ldZ $DIRNAME2 | awk '{print $4}'`
	else
		SECINFO2=""
	fi

	# get sec info on first mount point
	wait $MKDIRPID
	SECINFO1=`ls -ldZ $DIRNAME1 | awk '{print $4}'`

	# cleanup
	rmdir $DIRNAME1

	# compare sec contexts
	[ -z "$SECINFO2" -o "$SECINFO1" == "$SECINFO2" ] || \
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20b "[atomicity] concurrent access from another client (dir)"

test_20c() {
	local UNAME=`getent passwd $RUNAS_ID | cut -d: -f1`
	local DIRNAME1=$DIR/dd1
	local DIRNAME2=$DIR2/dd1
	local REQ_DELAY=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$REQ_DELAY"
	#define OBD_FAIL_LLITE_SETDIRSTRIPE_PAUSE     0x140a
	do_facet client "lctl set_param fail_loc=0x140a"

	# create file on first mount point
	lfs mkdir -c0 $DIRNAME1 &
	MKDIRPID=$!
	sleep 5

	if [[ -z "`ps h -o comm -p $MKDIRPID`" ]]; then
		error "lfs mkdir failed to sleep, pid=$MKDIRPID"
	fi

	# get sec info on second mount point
	if [ -e "$DIRNAME2" ]; then
		SECINFO2=`ls -ldZ $DIRNAME2 | awk '{print $4}'`
	else
		SECINFO2=""
	fi

	# get sec info on first mount point
	wait $MKDIRPID
	SECINFO1=`ls -ldZ $DIRNAME1 | awk '{print $4}'`

	# cleanup
	rmdir $DIRNAME1

	# compare sec contexts
	[ -z "$SECINFO2" -o "$SECINFO1" == "$SECINFO2" ] || \
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20c "[atomicity] concurrent access from another client (dir via lfs)"


complete $SECONDS
check_and_cleanup_lustre
exit_status

