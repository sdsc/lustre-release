#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#
# e.g.  ONLY="22 23" or ONLY="`seq 32 39`" or EXCEPT="31"
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
	local selinux_status=$(getenforce)
	if [ "$selinux_status" != "Enforcing" ]; then
	    echo "SELinux is currently in $selinux_status mode," \
		 "but it must be enforced to run sanity-selinux"
	    exit 0
	fi
	local selinux_policy=$(sestatus |
			     awk -F':' '$1 == "Loaded policy name"{print $2}' |
			     xargs)
	if [ -z "$selinux_policy" ]; then
	    selinux_policy=$(sestatus |
			 awk -F':' '$1 == "Policy from config file"{print $2}' |
			 xargs)
	fi
	[ "$selinux_policy" == "targeted" ] ||
	    error "Accepting only targeted policy"
	echo "$selinux_status, $selinux_policy"
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

	stop $mds
	mount_ldiskfs $mds || error "mount -t ldiskfs $mds failed"
	local xattrval=$(do_facet $mds getfattr -n security.selinux \
				${mntpt}/ROOT/$mds_path |
			 awk -F"=" '$1=="security.selinux" {print $2}')

	unmount_ldiskfs $mds || error "umount $mds failed"
	start $mds $devname $MDS_MOUNT_OPTS

	echo $xattrval
}


test_1() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		skip "Only applicable to ldiskfs-based MDTs" && return;

	local devname=$(mdsdevname 1)
	local tdir=${DIR}/d6
	local filename=${tdir}/df1
	local mds_path=${filename#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	touch $filename || error "cannot touch $filename"

	local xattrval=$(check_selinux_xattr "mds1" $devname $mds_path)

	rm -f $filename
	rmdir $tdir

	[ -z "$xattrval" -o "x$xattrval" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 1 "create file and check security.selinux xattr is set on MDT"

test_2a() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		{ skip "Only applicable to ldiskfs-based MDTs" && return; }

	local devname=$(mdsdevname 1)
	local tdir=${DIR}/d7
	local dirname=${tdir}/dir1
	local mds_path=${dirname#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	mkdir $dirname || error "cannot mkdir $dirname"

	local xattrval=$(check_selinux_xattr "mds1" $devname $mds_path)

	rmdir $dirname
	rmdir $tdir

	[ -z "$xattrval" -o "x$xattrval" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 2a "create dir (mkdir) and check security.selinux xattr is set on MDT"

test_2b() {
	[[ $(facet_fstype "mds1") != ldiskfs ]] &&
		{ skip "Only applicable to ldiskfs-based MDTs" && return; }

	local devname=$(mdsdevname 1)
	local tdir=$DIR/d7
	local dirname=$tdir/dir1
	local mds_path=${dirname#$MOUNT}

	mds_path=${mds_path#/}

	$LFS setdirstripe -i0 -c1 $tdir || error "create dir failed"
	$LFS mkdir -c0 $dirname || error "cannot 'lfs mkdir' $dirname"

	local xattrval=$(check_selinux_xattr "mds1" $devname $mds_path)

	rmdir $dirname

	[ -z "$xattrval" -o "x$xattrval" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	$LFS setdirstripe -i0 $dirname ||
	    error "cannot 'lfs setdirstripe' $dirname"

	xattrval=$(check_selinux_xattr "mds1" $devname $mds_path)

	rmdir $dirname
	rmdir $tdir

	[ -z "$xattrval" -o "x$xattrval" == "x\"\"" ] &&
		error "security.selinux xattr is not set"

	return 0
}
run_test 2b "create dir with lfs and check security.selinux xattr is set on MDT"

test_3() {
	local filename=$DIR/df1

	# get current mapping of runasid, and save it
	local uname=$(getent passwd $RUNAS_ID | cut -d: -f1)
	local sename=$(semanage login -l |
			      awk -v uname=$uname '$1==uname {print $2}')
	local serange=$(semanage login -l |
			 awk -v uname=$uname '$1==uname {print $3}')

	# change mapping of runasid to unconfined_u
	semanage login -a -s unconfined_u $uname ||
		error "unable to map $uname to unconfined_u"

	# "access" Lustre
	echo "${uname} mapped as unconfined_u: touch $filename"
	$PDSH ${uname}@localhost "touch $filename" ||
		error "can't touch $filename"
	echo "${uname} mapped as unconfined_u: rm -f $filename"
	$PDSH ${uname}@localhost "rm -f $filename" ||
		error "can't remove $filename"

	# restore original mapping of runasid
	if [ -n "$sename" ]; then
		if [ -n "$serange" ]; then
			semanage login -a -s $sename -r $serange $uname ||
				error "unable to restore mapping for $uname"
		else
			semanage login -a -s $sename $uname ||
				error "unable to restore mapping for $uname"
		fi
	else
		semanage login -d $uname
	fi

	return 0
}
run_test 3 "access with unconfined user"

test_4() {
	local filename=$DIR/df1

	# get current mapping of runasid, and save it
	local uname=$(getent passwd $RUNAS_ID | cut -d: -f1)
	local sename=$(semanage login -l |
			      awk -v uname=$uname '$1==uname {print $2}')
	local serange=$(semanage login -l |
			 awk -v uname=$uname '$1==uname {print $3}')

	# change mapping of runasid to guest_u
	semanage login -a -s guest_u $uname ||
		error "unable to map $uname to guest_u"

	# "access" Lustre
	echo "${uname} mapped as guest_u: touch $filename"
	$PDSH ${uname}@localhost "touch $filename" &&
		error "touch $filename should have failed"

	# change mapping of runasid to user_u
	semanage login -a -s user_u $uname ||
		error "unable to map $uname to user_u"

	# "access" Lustre
	echo "${uname} mapped as user_u: touch $filename"
	$PDSH ${uname}@localhost "touch $filename" ||
		error "can't touch $filename"
	echo "${uname} mapped as user_u: rm -f $filename"
	$PDSH ${uname}@localhost "rm -f $filename" ||
		error "can't remove $filename"

	# restore original mapping of runasid
	if [ -n "$sename" ]; then
		if [ -n "$serange" ]; then
			semanage login -a -s $sename -r $serange $uname ||
				error "unable to restore mapping for $uname"
		else
			semanage login -a -s $sename $uname ||
				error "unable to restore mapping for $uname"
		fi
	else
		semanage login -d $uname
	fi

	return 0
}
run_test 4 "access with specific SELinux user"

test_5() {
	local filename=$DIR/df1
	local newsecctx="nfs_t"

	# create file
	touch $filename || error "cannot touch $filename"

	# change sec context
	chcon -t $newsecctx $filename
	ls -lZ $filename

	# purge client's cache
	sync ; echo 3 > /proc/sys/vm/drop_caches

	# get sec context
	ls -lZ $filename
	local secctxseen=$(ls -lZ $filename | awk '{print $4}' | cut -d: -f3)

	# cleanup
	rm -f $filename

	[ "$newsecctx" == "$secctxseen" ] ||
		error "sec context seen from 1st mount point is not correct"

	return 0
}
run_test 5 "security context retrieval from MDT xattr"

test_10() {
	local filename1=$DIR/df1
	local filename2=$DIR2/df1
	local newsecctx="nfs_t"

	# create file from 1st mount point
	touch $filename1 || error "cannot touch $filename1"
	ls -lZ $filename1

	# change sec context from 2nd mount point
	chcon -t $newsecctx $filename2
	ls -lZ $filename2

	# get sec context from 1st mount point
	ls -lZ $filename1
	local secctxseen=$(ls -lZ $filename1 | awk '{print $4}' | cut -d: -f3)

	# cleanup
	rm -f $filename1

	[ "$newsecctx" == "$secctxseen" ] ||
		error "sec context seen from 1st mount point is not correct"

	return 0
}
run_test 10 "[consistency] concurrent security context change"

test_20a() {
	local uname=$(getent passwd $RUNAS_ID | cut -d: -f1)
	local filename1=$DIR/df1
	local filename2=$DIR2/df1
	local req_delay=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$req_delay"
	#define OBD_FAIL_LLITE_CREATE_FILE_PAUSE   0x1409
	do_facet client "lctl set_param fail_loc=0x1409"

	# create file on first mount point
	$PDSH ${uname}@localhost "touch $filename1" &
	local touchpid=$!
	sleep 5

	if [[ -z "$(ps h -o comm -p $touchpid)" ]]; then
		error "touch failed to sleep, pid=$touchpid"
	fi

	# get sec info on second mount point
	if [ -e "$filename2" ]; then
		secinfo2=$(ls -lZ $filename2 | awk '{print $4}')
	fi

	# get sec info on first mount point
	wait $touchpid
	secinfo1=$(ls -lZ $filename1 | awk '{print $4}')

	# cleanup
	rm -f $filename1

	# compare sec contexts
	[ -z "$secinfo2" -o "$secinfo1" == "$secinfo2" ] ||
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20a "[atomicity] concurrent access from another client (file)"

test_20b() {
	local uname=$(getent passwd $RUNAS_ID | cut -d: -f1)
	local dirname1=$DIR/dd1
	local dirname2=$DIR2/dd1
	local req_delay=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$req_delay"
	#define OBD_FAIL_LLITE_NEWNODE_PAUSE     0x140a
	do_facet client "lctl set_param fail_loc=0x140a"

	# create file on first mount point
	$PDSH ${uname}@localhost "mkdir $dirname1" &
	local mkdirpid=$!
	sleep 5

	if [[ -z "$(ps h -o comm -p $mkdirpid)" ]]; then
		error "mkdir failed to sleep, pid=$mkdirpid"
	fi

	# get sec info on second mount point
	if [ -e "$dirname2" ]; then
		secinfo2=$(ls -ldZ $dirname2 | awk '{print $4}')
	else
		secinfo2=""
	fi

	# get sec info on first mount point
	wait $mkdirpid
	secinfo1=$(ls -ldZ $dirname1 | awk '{print $4}')

	# cleanup
	rmdir $dirname1

	# compare sec contexts
	[ -z "$secinfo2" -o "$secinfo1" == "$secinfo2" ] || \
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20b "[atomicity] concurrent access from another client (dir)"

test_20c() {
	local dirname1=$DIR/dd1
	local dirname2=$DIR2/dd1
	local req_delay=20

	# sleep some time in ll_create_nd()
	do_facet client "lctl set_param fail_val=$req_delay"
	#define OBD_FAIL_LLITE_SETDIRSTRIPE_PAUSE     0x140b
	do_facet client "lctl set_param fail_loc=0x140b"

	# create file on first mount point
	lfs mkdir -c0 $dirname1 &
	local mkdirpid=$!
	sleep 5

	if [[ -z "$(ps h -o comm -p $mkdirpid)" ]]; then
		error "lfs mkdir failed to sleep, pid=$mkdirpid"
	fi

	# get sec info on second mount point
	if [ -e "$dirname2" ]; then
		secinfo2=$(ls -ldZ $dirname2 | awk '{print $4}')
	else
		secinfo2=""
	fi

	# get sec info on first mount point
	wait $mkdirpid
	secinfo1=$(ls -ldZ $dirname1 | awk '{print $4}')

	# cleanup
	rmdir $dirname1

	# compare sec contexts
	[ -z "$secinfo2" -o "$secinfo1" == "$secinfo2" ] || \
		error "sec context seen from 2nd mount point is not correct"

	return 0
}
run_test 20c "[atomicity] concurrent access from another client (dir via lfs)"


complete $SECONDS
check_and_cleanup_lustre
exit_status

