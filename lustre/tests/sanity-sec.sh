#!/bin/bash
#
# Run select tests by setting ONLY, or as arguments to the script.
# Skip specific tests by setting EXCEPT.
#

set -e

ONLY=${ONLY:-"$*"}
# bug number for skipped test: 19430 19967 19967
ALWAYS_EXCEPT="                2     5     6    $SANITY_SEC_EXCEPT"
# UPDATE THE COMMENT ABOVE WITH BUG NUMBERS WHEN CHANGING ALWAYS_EXCEPT!

[ "$ALWAYS_EXCEPT$EXCEPT" ] && \
    echo "Skipping tests: $ALWAYS_EXCEPT $EXCEPT"

SRCDIR=`dirname $0`
export PATH=$PWD/$SRCDIR:$SRCDIR:$PWD/$SRCDIR/../utils:$PATH:/sbin
export NAME=${NAME:-local}

LUSTRE=${LUSTRE:-`dirname $0`/..} 
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

RUNAS="runas"

WTL=${WTL:-"$LUSTRE/tests/write_time_limit"}

CONFDIR=/etc/lustre
PERM_CONF=$CONFDIR/perm.conf
FAIL_ON_ERROR=false

HN=$(/bin/hostname -s)
NODEMAPS=10
RANGES=3
IPADDRS=30
IDS=200

require_dsh_mds || exit 0
require_dsh_ost || exit 0

ID0=${ID0:-500}
ID1=${ID1:-501}
USER0=`cat /etc/passwd|grep :$ID0:$ID0:|cut -d: -f1`
USER1=`cat /etc/passwd|grep :$ID1:$ID1:|cut -d: -f1`

[ -z "$USER0" ] && \
	echo "Please add user0 (uid=$ID0 gid=$ID0)! Skip sanity-sec" && exit 0

[ -z "$USER1" ] && \
	echo "Please add user1 (uid=$ID1 gid=$ID1)! Skip sanity-sec" && exit 0

check_and_setup_lustre

sec_cleanup() {
	if [ "$I_MOUNTED" = "yes" ]; then
		cleanupall -f || error "sec_cleanup"
	fi
}

DIR=${DIR:-$MOUNT}
[ -z "`echo $DIR | grep $MOUNT`" ] && \
	error "$DIR not in $MOUNT" && sec_cleanup && exit 1

[ `echo $MOUNT | wc -w` -gt 1 ] && \
	echo "NAME=$MOUNT mounted more than once" && sec_cleanup && exit 0

[ $MDSCOUNT -gt 1 ] && \
	echo "skip multi-MDS test" && sec_cleanup && exit 0

# for GSS_SUP
GSS_REF=$(lsmod | grep ^ptlrpc_gss | awk '{print $3}')
if [ ! -z "$GSS_REF" -a "$GSS_REF" != "0" ]; then
	GSS_SUP=1
	echo "with GSS support"
else
	GSS_SUP=0
	echo "without GSS support"
fi

MDT="`do_facet $SINGLEMDS "lctl get_param -N mdt.\*MDT\*.stats 2>/dev/null | cut -d"." -f2" || true`"
[ -z "$MDT" ] && error "fail to get MDT device" && exit 1
do_facet $SINGLEMDS "mkdir -p $CONFDIR"
IDENTITY_FLUSH=mdt.$MDT.identity_flush
MDSCAPA=mdt.$MDT.capa
CAPA_TIMEOUT=mdt.$MDT.capa_timeout
MDSSECLEVEL=mdt.$MDT.sec_level

# for CLIENT_TYPE
if [ -z "$(lctl get_param -n llite.*.client_type | grep remote 2>/dev/null)" ]; then
	CLIENT_TYPE="local"
	echo "local client"
else
	CLIENT_TYPE="remote"
	echo "remote client"
fi

SAVE_PWD=$PWD

build_test_filter

sec_login() {
	local user=$1
	local group=$2

	if ! $RUNAS -u $user krb5_login.sh; then
		error "$user login kerberos failed."
		exit 1
	fi

	if ! $RUNAS -u $user -g $group ls $DIR > /dev/null 2>&1; then
		$RUNAS -u $user lfs flushctx -k
		$RUNAS -u $user krb5_login.sh
                if ! $RUNAS -u $user -g $group ls $DIR > /dev/null 2>&1; then
                        error "init $user $group failed."
                        exit 2
                fi
	fi
}

declare -a identity_old

sec_setup() {
       	for num in `seq $MDSCOUNT`; do
       		switch_identity $num true || identity_old[$num]=$?
       	done

	if ! $RUNAS -u $ID0 ls $DIR > /dev/null 2>&1; then
		sec_login $USER0 $USER0
	fi

	if ! $RUNAS -u $ID1 ls $DIR > /dev/null 2>&1; then
		sec_login $USER1 $USER1
	fi
}
sec_setup

# run as different user
test_0() {
	umask 0022

	chmod 0755 $DIR || error "chmod (1)"
	rm -rf $DIR/$tdir || error "rm (1)"
	mkdir -p $DIR/$tdir || error "mkdir (1)"

	if [ "$CLIENT_TYPE" = "remote" ]; then
		do_facet $SINGLEMDS "echo '* 0 normtown' > $PERM_CONF"
	        do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
		chown $USER0 $DIR/$tdir && error "chown (1)"
		do_facet $SINGLEMDS "echo '* 0 rmtown' > $PERM_CONF"
	        do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	else
		chown $USER0 $DIR/$tdir || error "chown (2)"
	fi

	$RUNAS -u $ID0 ls $DIR || error "ls (1)"
        rm -f $DIR/f0 || error "rm (2)"
	$RUNAS -u $ID0 touch $DIR/f0 && error "touch (1)"
	$RUNAS -u $ID0 touch $DIR/$tdir/f1 || error "touch (2)"
	$RUNAS -u $ID1 touch $DIR/$tdir/f2 && error "touch (3)"
	touch $DIR/$tdir/f3 || error "touch (4)"
	chown root $DIR/$tdir || error "chown (3)"
	chgrp $USER0 $DIR/$tdir || error "chgrp (1)"
	chmod 0775 $DIR/$tdir || error "chmod (2)"
	$RUNAS -u $ID0 touch $DIR/$tdir/f4 || error "touch (5)"
	$RUNAS -u $ID1 touch $DIR/$tdir/f5 && error "touch (6)"
	touch $DIR/$tdir/f6 || error "touch (7)"
	rm -rf $DIR/$tdir || error "rm (3)"

	if [ "$CLIENT_TYPE" = "remote" ]; then
		do_facet $SINGLEMDS "rm -f $PERM_CONF"
	        do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	fi
}
run_test 0 "uid permission ============================="

# setuid/gid
test_1() {
	[ $GSS_SUP = 0 ] && skip "without GSS support." && return

	if [ "$CLIENT_TYPE" = "remote" ]; then
		do_facet $SINGLEMDS "echo '* 0 rmtown' > $PERM_CONF"
	        do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	fi

	rm -rf $DIR/$tdir
	mkdir -p $DIR/$tdir

	chown $USER0 $DIR/$tdir || error "chown (1)"
	$RUNAS -u $ID1 -v $ID0 touch $DIR/$tdir/f0 && error "touch (2)"
	echo "enable uid $ID1 setuid"
	do_facet $SINGLEMDS "echo '* $ID1 setuid' >> $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	$RUNAS -u $ID1 -v $ID0 touch $DIR/$tdir/f1 || error "touch (3)"

	chown root $DIR/$tdir || error "chown (4)"
	chgrp $USER0 $DIR/$tdir || error "chgrp (5)"
	chmod 0770 $DIR/$tdir || error "chmod (6)"
	$RUNAS -u $ID1 -g $ID1 touch $DIR/$tdir/f2 && error "touch (7)"
	$RUNAS -u $ID1 -g $ID1 -j $ID0 touch $DIR/$tdir/f3 && error "touch (8)"
	echo "enable uid $ID1 setuid,setgid"
	do_facet $SINGLEMDS "echo '* $ID1 setuid,setgid' > $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	$RUNAS -u $ID1 -g $ID1 -j $ID0 touch $DIR/$tdir/f4 || error "touch (9)"
	$RUNAS -u $ID1 -v $ID0 -g $ID1 -j $ID0 touch $DIR/$tdir/f5 || error "touch (10)"

	rm -rf $DIR/$tdir

	do_facet $SINGLEMDS "rm -f $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
}
run_test 1 "setuid/gid ============================="

run_rmtacl_subtest() {
    $SAVE_PWD/rmtacl/run $SAVE_PWD/rmtacl/$1.test
    return $?
}

# remote_acl
# for remote client only
test_2 () {
	[ "$CLIENT_TYPE" = "local" ] && \
		skip "remote_acl for remote client only" && return
    	[ -z "$(lctl get_param -n mdc.*-mdc-*.connect_flags | grep ^acl)" ] && \
		skip "must have acl enabled" && return
    	[ -z "$(which setfacl 2>/dev/null)" ] && \
		skip "could not find setfacl" && return
	[ "$UID" != 0 ] && skip "must run as root" && return

	do_facet $SINGLEMDS "echo '* 0 rmtacl,rmtown' > $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"

	sec_login root root
	sec_login bin bin
	sec_login daemon daemon
	sec_login games users

    	SAVE_UMASK=`umask`
    	umask 0022
    	cd $DIR

        echo "performing cp ..."
        run_rmtacl_subtest cp || error "cp"
    	echo "performing getfacl-noacl..."
    	run_rmtacl_subtest getfacl-noacl || error "getfacl-noacl"
    	echo "performing misc..."
    	run_rmtacl_subtest misc || error "misc"
    	echo "performing permissions..."
    	run_rmtacl_subtest permissions || error "permissions"
    	echo "performing setfacl..."
    	run_rmtacl_subtest setfacl || error "setfacl"

    	# inheritance test got from HP
    	echo "performing inheritance..."
    	cp $SAVE_PWD/rmtacl/make-tree .
    	chmod +x make-tree
    	run_rmtacl_subtest inheritance || error "inheritance"
    	rm -f make-tree

    	cd $SAVE_PWD
    	umask $SAVE_UMASK

	do_facet $SINGLEMDS "rm -f $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
}
run_test 2 "rmtacl ============================="

# rootsquash
# root_squash will be redesigned in Lustre 1.7
test_3() {
        skip "root_squash will be redesigned in Lustre 1.7" && return
}
run_test 3 "rootsquash ============================="

# bug 3285 - supplementary group should always succeed.
# NB: the supplementary groups are set for local client only,
# as for remote client, the groups of the specified uid on MDT
# will be obtained by upcall /sbin/l_getidentity and used.
test_4() {
	if [ "$CLIENT_TYPE" = "remote" ]; then
		do_facet $SINGLEMDS "echo '* 0 rmtown' > $PERM_CONF"
	        do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
	fi

	rm -rf $DIR/$tdir
        mkdir -p $DIR/$tdir
        chmod 0771 $DIR/$tdir
        chgrp $ID0 $DIR/$tdir
	$RUNAS -u $ID0 ls $DIR/$tdir || error "setgroups (1)"
	if [ "$CLIENT_TYPE" = "local" ]; then
		do_facet $SINGLEMDS "echo '* $ID1 setgrp' > $PERM_CONF"
		do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
		$RUNAS -u $ID1 -G1,2,$ID0 ls $DIR/$tdir || error "setgroups (2)"
	fi
	$RUNAS -u $ID1 -G1,2 ls $DIR/$tdir && error "setgroups (3)"
	rm -rf $DIR/$tdir

	do_facet $SINGLEMDS "rm -f $PERM_CONF"
	do_facet $SINGLEMDS "lctl set_param -n $IDENTITY_FLUSH=-1"
}
run_test 4 "set supplementary group ==============="

mds_capability_timeout() {
        [ $# -lt 1 ] && echo "Miss mds capability timeout value" && return 1

        echo "Set mds capability timeout as $1 seconds"
	do_facet $SINGLEMDS "lctl set_param -n $CAPA_TIMEOUT=$1"
        return 0
}

mds_sec_level_switch() {
        [ $# -lt 1 ] && echo "Miss mds sec level switch value" && return 1

        case $1 in
                0) echo "Disable capa for all clients";;
                1) echo "Enable capa for remote client";;
		3) echo "Enable capa for all clients";;
                *) echo "Invalid mds sec level switch value" && return 2;;
        esac

	do_facet $SINGLEMDS "lctl set_param -n $MDSSECLEVEL=$1"
        return 0
}

oss_sec_level_switch() {
        [ $# -lt 1 ] && echo "Miss oss sec level switch value" && return 1

        case $1 in
                0) echo "Disable capa for all clients";;
                1) echo "Enable capa for remote client";;
		3) echo "Enable capa for all clients";;
                *) echo "Invalid oss sec level switch value" && return 2;;
        esac

	for i in `seq $OSTCOUNT`; do
		local j=`expr $i - 1`
		local OST="`do_facet ost$i "lctl get_param -N obdfilter.\*OST\*$j/stats 2>/dev/null | cut -d"." -f2" || true`"
                [ -z "$OST" ] && return 3
		do_facet ost$i "lctl set_param -n obdfilter.$OST.sec_level=$1"
	done
        return 0
}

mds_capability_switch() {
        [ $# -lt 1 ] && echo "Miss mds capability switch value" && return 1

        case $1 in
                0) echo "Turn off mds capability";;
                3) echo "Turn on mds capability";;
                *) echo "Invalid mds capability switch value" && return 2;;
        esac

	do_facet $SINGLEMDS "lctl set_param -n $MDSCAPA=$1"
        return 0
}

oss_capability_switch() {
        [ $# -lt 1 ] && echo "Miss oss capability switch value" && return 1

        case $1 in
                0) echo "Turn off oss capability";;
                1) echo "Turn on oss capability";;
                *) echo "Invalid oss capability switch value" && return 2;;
        esac

	for i in `seq $OSTCOUNT`; do
		local j=`expr $i - 1`
		local OST="`do_facet ost$i "lctl get_param -N obdfilter.\*OST\*$j/stats 2>/dev/null | cut -d"." -f2" || true`"
                [ -z "$OST" ] && return 3
		do_facet ost$i "lctl set_param -n obdfilter.$OST.capa=$1"
	done
        return 0
}

turn_mds_capa_on() {
        mds_capability_switch 3 || return 1
	mds_sec_level_switch 3	|| return 2
        return 0
}

turn_oss_capa_on() {
        oss_capability_switch 1 || return 1
	oss_sec_level_switch 3	|| return 2
        return 0
}

turn_capability_on() {
        local capa_timeout=${1:-"1800"}

        # To turn on fid capability for the system,
        # there is a requirement that fid capability
        # is turned on on all MDS/OSS servers before
        # client mount.

	turn_mds_capa_on || return 1
	turn_oss_capa_on || return 2
        mds_capability_timeout $capa_timeout || return 3
        remount_client $MOUNT || return 4
        return 0
}

turn_mds_capa_off() {
	mds_sec_level_switch 0	|| return 1
        mds_capability_switch 0 || return 2
        return 0
}

turn_oss_capa_off() {
	oss_sec_level_switch 0	|| return 1
        oss_capability_switch 0 || return 2
        return 0
}

turn_capability_off() {
        # to turn off fid capability, you can just do
        # it in a live system. But, please turn off
        # capability of all OSS servers before MDS servers.

	turn_oss_capa_off || return 1
	turn_mds_capa_off || return 2
        return 0
}

# We demonstrate that access to the objects in the filesystem are not
# accessible without supplying secrets from the MDS by disabling a
# proc variable on the mds so that it does not supply secrets. We then
# try and access objects which result in failure.
test_5() {
        local file=$DIR/f5

	[ $GSS_SUP = 0 ] && skip "without GSS support." && return
	if ! remote_mds; then
                skip "client should be separated from server."
                return
        fi

	rm -f $file

	turn_capability_off
	if [ $? != 0 ]; then
		error "turn_capability_off"
		return 1
	fi

        turn_oss_capa_on
	if [ $? != 0 ]; then
		error "turn_oss_capa_on"
		return 2
	fi

	if [ "$CLIENT_TYPE" = "remote" ]; then
		remount_client $MOUNT && return 3
		turn_oss_capa_off
		return 0
	else
        	remount_client $MOUNT || return 4
	fi

        # proc variable disabled -- access to the objects in the filesystem
        # is not allowed 
        echo "Should get Write error here : (proc variable are disabled "\
	     "-- access to the objects in the filesystem is denied."
	$WTL $file 30
	if [ $? == 0 ]; then
        	error "Write worked well even though secrets not supplied."
		return 5
        fi

        turn_capability_on
	if [ $? != 0 ]; then
		error "turn_capability_on"
		return 6
	fi

        sleep 5

        # proc variable enabled, secrets supplied -- write should work now
        echo "Should not fail here : (proc variable enabled, secrets supplied "\
	     "-- write should work now)."
	$WTL $file 30
	if [ $? != 0 ]; then
        	error "Write failed even though secrets supplied."
		return 7
        fi

	turn_capability_off
	if [ $? != 0 ]; then
		error "turn_capability_off"
		return 8
	fi
	rm -f $file
}
run_test 5 "capa secrets ========================="

# Expiry: A test program is performing I/O on a file. It has credential
# with an expiry half a minute later. While the program is running the
# credentials expire and no automatic extensions or renewals are
# enabled. The program will demonstrate an I/O failure.
test_6() {
        local file=$DIR/f6

	[ $GSS_SUP = 0 ] && skip "without GSS support." && return
	if ! remote_mds; then
                skip "client should be separated from server."
                return
        fi

	turn_capability_off
	if [ $? != 0 ]; then
		error "turn_capability_off"
		return 1
	fi

	rm -f $file

        turn_capability_on 30
	if [ $? != 0 ]; then
		error "turn_capability_on 30"
		return 2
	fi

        # Token expiry
	$WTL $file 60
	if [ $? != 0 ]; then
		error "$WTL $file 60"
		return 3
	fi

	# Reset MDS capability timeout
	mds_capability_timeout 30
	if [ $? != 0 ]; then
		error "mds_capability_timeout 30"
		return 4
	fi

	$WTL $file 60 &
	local PID=$!
	sleep 5

        # To disable automatic renew, only need turn capa off on MDS.
	turn_mds_capa_off
	if [ $? != 0 ]; then
		error "turn_mds_capa_off"
		return 5
	fi

	echo "We expect I/O failure."
        wait $PID
	if [ $? == 0 ]; then
		echo "no I/O failure got."
		return 6
	fi

	turn_capability_off
	if [ $? != 0 ]; then
		error "turn_capability_off"
		return 7
	fi
	rm -f $file
}
run_test 6 "capa expiry ========================="

test_7() {
	for i in $(eval echo {0..$NODEMAPS}); do
		do_facet mgs $LCTL nodemap_add ${HN}_${i}
		err="nodemap_add ${HN}_${i} failed with $rc"
		[[ $rc == 0 ]] && error $err && return 1
		out=$(do_facet mgs $LCTL get_param nodemap.${HN}_${i}.id)
		rc=$(echo $out | grep -c ${HN}_${i})
		err="nodemap_add ${HN}_${i} check failed with $rc"
		[[ $rc == 0 ]] && error $err && return 2
	done
	return 0
}
run_test 7 "nodemap create"

test_8() {
	for i in $(eval echo {0..$NODEMAPS}); do
		out=$(do_facet mgs $LCTL nodemap_add ${HN}_${i} 2>&1)
		rc=$(echo $out | grep -c error)
		err="nodemap_add allowed duplicates with $rc"
		[[ $rc == 0 ]] && error $err && return 1
	done
	return 0
}
run_test 8 "nodemap reject duplicates"

test_9() {
	subnet=0
	cmd="$LCTL nodemap_add_range"
	for i in $(eval echo {0..$NODEMAPS}); do
		for j in $(eval echo {0..$RANGES}); do
			range="10.${subnet}.${j}.0:10.${subnet}.${j}.253"
			out=$(do_facet mgs ${cmd} ${HN}_${i} $range 2>&1)
			rc=$?
			[[ $rc != 0 ]] && return 1
		done

		subnet=$(expr $subnet + 1)
	done
}
run_test 9 "nodemap range add"

test_10() {
	subnet=0
	cmd="$LCTL nodemap_add_range"
	for i in $(eval echo {0..$NODEMAPS}); do
		for j in $(eval echo {0..$RANGES}); do
			range="10.${subnet}.${j}.0:10.${subnet}.${j}.253"
			out=$(do_facet mgs ${cmd} ${HN}_${i} $range 2>&1)
			rc=$?
			[[ $rc == 0 ]] && return 1
		done

		subnet=$(expr $subnet + 1)
	done

}
run_test 10 "nodemap reject duplicate ranges"

test_11() {
	proc[0]="admin_nodemap"
	proc[1]="trusted_nodemap"
	option[0]="admin"
	option[1]="trusted"
	modify="do_facet mgs $LCTL nodemap_modify"
	get_param="do_facet mgs $LCTL get_param"
	for idx in `seq 0 1`; do
		for i in $(eval echo {0..$NODEMAPS}); do
			h=$(echo ${HN}_${i})
			out=$($modify $h ${option[$idx]} 1)
			val=$($get_param nodemap.$h.${proc[$idx]})
			[[ $val != "nodemap.$h.${proc[$idx]}=1" ]]	\
			    && return 1
			out=$($modify $h ${option[$idx]} 0)
			val=$($get_param nodemap.$h.${proc[$idx]})
			[[ $val != "nodemap.$h.${proc[$idx]}=0" ]]	\
			   && return 1
		done
	done
	return 0
}
run_test 11 "nodemap test flags"

test_12() {
	for i in $(eval echo {0..$NODEMAPS}); do
		h=$(echo ${HN}_${i})
		out=$(do_facet mgs $LCTL nodemap_modify $h squash_uid 88)
		val=$(do_facet mgs $LCTL get_param nodemap.$h.squash_uid)
		[[ $val != "nodemap.$h.squash_uid=88" ]] && return 1

		out=$(do_facet mgs $LCTL nodemap_modify $h squash_uid 99)
		val=$(do_facet mgs $LCTL get_param nodemap.$h.squash_uid)
		[[ $val != "nodemap.$h.squash_uid=99" ]] && return 1

		out=$(do_facet mgs $LCTL nodemap_modify $h squash_gid 88)
		val=$(do_facet mgs $LCTL get_param nodemap.$h.squash_gid)
		[[ $val != "nodemap.$h.squash_gid=88" ]] && return 1

		out=$(do_facet mgs $LCTL nodemap_modify $h squash_gid 99)
		val=$(do_facet mgs $LCTL get_param nodemap.$h.squash_gid)
		[[ $val != "nodemap.$h.squash_gid=99" ]] && return 1
	done
	return 0
}
run_test 12 "nodemap squash ids"

test_13() {
	subnet=0
	for i in $(eval echo {0..$NODEMAPS}); do
		for j in $(eval echo {0..$RANGES}); do
			for k in $(eval echo {0..$IPADDRS}); do
				nid="10.$subnet.$j.$k"
				val=$(do_facet mgs $LCTL nodemap_test_nid $nid)
				nm=$(echo $val | awk -F: '{ print $1 }')
				[[ $nm != ${HN}_${i} ]] && return 1
			done
		done
		subnet=$(expr $subnet + 1)
	done
}
run_test 13 "nodemap test nid lookups"

test_14() {
	for i in $(eval echo {0..$IPADDRS}); do
		val=$(do_facet mgs $LCTL nodemap_test_nid 11.0.0.${i})
		nodemap=$(echo $val | awk -F: '{ print $1 }')
		[[ $nodemap != "default" ]] && return 1
	done
	return 0
}
run_test 14 "nodemap default nid lookups"

test_15() {
	MAX_ID=$(expr $IDS + 500)
	for i in $(eval echo {0..$NODEMAPS}); do
		for j in $( eval echo {500..$MAX_ID}); do
			k=$(expr $j + 1)
			h=$(echo ${HN}_${i})
			u_add=$(echo nodemap_add_uidmap)
			g_add=$(echo nodemap_add_gidmap)
			err=$(do_facet mgs $LCTL $u_add $h $j:$k 2>&1)
			rc=$?
			msg="add_uidmap failed with rc=$rc"
			[[ $rc != 0 ]] && error $msg && return 1

			err=$(do_facet mgs $LCTL $g_add $h $j:$k 2>&1)
			rc=$?
			msg="add_gidmap failed with rc=$rc"
			[[ $rc != 0 ]] && error $msg && return 2
		done
	done
	return 0
}
run_test 15 "add uid/gid mappings"

test_16() {
	out=$($LCTL set_param nodemap.active=1)
	subnet=0
	max_id=$(expr $IDS + 500)
	for j in $(eval echo {0..$RANGES}); do
		for k in $(eval echo {0..$IPADDRS}); do
			for uid in $(eval echo {500..$max_id}); do
				nid="10.$subnet.$j.$k"
				cmd="$LCTL nodemap_test_uid $nid $uid"
				val=$($cmd)
				local_uid=$(echo $val | awk '{ print $2 }')
				test_uid=$(expr $uid + 1)
				msg="test_uid $cmd failed"
				[[ $local_uid != $test_uid ]] && 	\
				    error $msg && return 1

				cmd="$LCTL nodemap_test_gid $nid $uid"
				val=$($cmd)
				local_gid=$(echo $val | awk '{ print $2 }')
				msg="test_gid $cmd failed"
				[[ $local_gid != $test_uid ]] &&	\
				   error $msg && return 2
			done
		done
		subnet=$(expr $subnet + 1)
	done
	return 0
}
run_test 16 "check mappings with mapping enabled (no admin)"

test_17() {
	out=$($LCTL set_param nodemap.active=1)
	for i in $( eval echo {0..$NODEMAPS}); do
		out=$($LCTL nodemap_modify ${HN}_${i} trusted 1)
	done

	subnet=0
	max_id=`expr $IDS + 500`
	for j in $(eval echo {0..$RANGES}); do
		for k in $(eval echo {0..$IPADDRS}); do
			for uid in $(eval echo {500..$max_id}); do
				nid="10.$subnet.$j.$k"
				cmd="$LCTL nodemap_test_uid $nid $uid"
				val=$($cmd)
				local_uid=$(echo $val | awk '{ print $2 }')
				[[ $local_uid != $uid ]] &&	\
				   error "test_uid $cmd failed" && return 1

				cmd="$LCTL nodemap_test_gid $nid $uid"
				val=$($cmd)
				local_gid=$(echo $val | awk '{ print $2 }')
				[[ $local_gid != $uid ]] &&	\
				   error "test_gid $cmd failed" && return 2
			done
		done
		subnet=$(expr $subnet + 1)
	done
	for i in $(eval echo {0..$NODEMAPS}); do
		out=$($LCTL nodemap_modify ${HN}_${i} trusted 0)
	done
	return 0
}
run_test 17 "check mappings with mapping enabled (trusted)"

test_18() {
	out=$($LCTL set_param nodemap.active=1)
	for i in $( eval echo {0..$NODEMAPS}); do
		out=$($LCTL nodemap_modify ${HN}_${i} admin 1)
	done

	subnet=0
	for j in $(eval echo {0..$RANGES}); do
		for k in $(eval echo {0..$IPADDRS}); do
			nid="10.$subnet.$j.$j"
			cmd="$LCTL nodemap_test_uid $nid 0"
			val=$($cmd)
			local_uid=$(echo $val | awk '{ print $2 }')
			[[ $local_uid != 0 ]] &&	\
			   error "admin $cmd failed" && return 1

			cmd="$LCTL nodemap_test_gid $nid 0"
			val=$($cmd)
			local_gid=$(echo $val | awk '{ print $2 }')
			[[ $local_gid != 0 ]] &&	\
			   error "admin $cmd failed" && return 2
		done
		subnet=$(expr $subnet + 1)
	done

	for i in $(eval echo {0..$NODEMAPS}); do
		out=$($LCTL nodemap_modify ${HN}_${i} admin 0)
	done

}
run_test 18 "check mappings with mapping enabled (admin)"

test_19() {
	out=$($LCTL set_param nodemap.active=1)
	proc[0]="admin_nodemap"
	proc[1]="trusted_nodemap"
	option[0]="admin"
	option[1]="trusted"
	for idx in `seq 0 1`; do
		out=$($LCTL nodemap_modify default ${option[$idx]} 1)
		val=$($LCTL get_param nodemap.default.${proc[$idx]})
		rel=$(echo $val | awk -F= '{print $2 }')
		[[ $rel != 1 ]] &&	\
		   error "default ${option[$idx]} $rel" && return 1

		out=$($LCTL nodemap_modify default ${option[$idx]} 0)
		val=$($LCTL get_param nodemap.default.${proc[$idx]})
		rel=$(echo $val | awk -F= '{print $2 }')
		[[ $rel != 0 ]] &&	\
		   error "default ${option[$idx]} $rel" && return 2
	done

	out=$($LCTL nodemap_modify default squash_uid 88)
	val=$($LCTL get_param nodemap.default.squash_uid)
	rel=$(echo $val | awk -F= '{print $2 }')
	[[ $rel != 88 ]] && error "squash_uid $rel" && return 3;
	out=$($LCTL nodemap_modify default squash_uid 99)

	out=$($LCTL nodemap_modify default squash_gid 88)
	val=$($LCTL get_param nodemap.default.squash_gid)
	rel=$(echo $val | awk -F= '{print $2 }')
	[[ $rel != 88 ]] && error "squash_gid $rel" && return 4;

	out=$($LCTL nodemap_modify default squash_gid 99)
}
run_test 19 "test default"

test_20() {
	subnet=0
	cmd="$LCTL nodemap_del_range"
	for i in $(eval echo {0..$NODEMAPS}); do
		out=$($cmd ${HN}_${i} 10.$subnet.0.0:10.$subnet.0.253)
		rc=$?
		[[ $rc != 0 ]] && error "range delete with rc=$rc" && return 1

		out=$($LCTL nodemap_del_uidmap ${HN}_${i} 502)
		rc=$?
		[[ $rc != 0 ]] && error "uid delete with rc=$rc" && return 2

		out=$($LCTL nodemap_del_gidmap ${HN}_${i} 502)
		rc=$?
		[[ $rc != 0 ]] && error "gid delete with rc=$rc" && return 3

		subnet=$(expr $subnet + 1)
	done

	out=$($LCTL nodemap_del ${HN}_0)
	rc=$?
	[[ $rc != 0 ]] && error "delete ${HN}_0 fail" && return 4

	out=$($LCTL nodemap_del default)
	rc=$?
	[[ $rc == 0 ]] && error "deleted default nodemap" && return 5

	return 0
}
run_test 20 "remove nodemaps, mappings, and ranges"

log "cleanup: ======================================================"

sec_unsetup() {
       	for num in `seq $MDSCOUNT`; do
		if [ "${identity_old[$num]}" = 1 ]; then
       			switch_identity $num false || identity_old[$num]=$?
		fi
       	done

	$RUNAS -u $ID0 ls $DIR
	$RUNAS -u $ID1 ls $DIR
}
sec_unsetup

sec_cleanup

complete $SECONDS
exit_status
