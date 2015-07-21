#!/bin/bash
export PATH=`dirname $0`/../utils:$PATH
NAME=${NAME:-local}

LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}

[ -n "$LOAD" ] && load_modules && exit 0
load_modules || exit 0


rc=0
rmmod_remote=0
fstype=$(facet_fstype $SINGLEMDS)
loopdev=""
MDT_DEVNAME=$(mdsdevname ${SINGLEMDS//mds/})

do_facet ost1 "lsmod | grep -q osdbench || "                      \
	      "{ insmod ${LUSTRE}/obdecho/osdbench.ko || "        \
	      "modprobe osdbench; }" && rmmod_remote=1

add $SINGLEMDS $(mkfs_opts $SINGLEMDS $MDT_DEVNAME) \
	--backfstype $(facet_fstype $SINGLEMDS) \
	--reformat $MDT_DEVNAME $(mdsvdevname 1) \
	>/dev/null || exit 10

if [ $fstype == zfs ]; then
	import_zpool $SINGLEMDS || echo "can't import"
	osdname="osd-zfs"
	targetdev=$MDT_DEVNAME
elif [ $fstype == ldiskfs ]; then
	osdname="osd-ldiskfs"
	if [ ! -b $MDT_DEVNAME ]; then
		loopdev=$(losetup -f)
		losetup $loopdev $MDT_DEVNAME || error "can't setup loop"
		targetdev=$loopdev
	else
		targetdev=$MDT_DEVNAME
	fi
else
	error "not supported fstype: $fstype"
fi

$LCTL <<-EOF
	attach $osdname obe-osd obe_osd_UUID
	cfg_device obe-osd
	setup $targetdev 0 noacl $SINGLEMDS
EOF

$LCTL <<-EOF
	attach osdbench obe obe_UUID
	cfg_device obe
	setup obe-osd
EOF

[ -n "$SETUP" ] && exit 0

osdbench -d obe -f create_object $* || echo "osdbench failed"

[ -n "$NOCLEANUP" ] && exit 0

$LCTL <<-EOF
	device obe
	cleanup
	detach
EOF
# OSD shutdowns automatically when the last user (osdbench) disconnects
sleep 1

if [ -n "$loopdev" ]; then
	losetup -d $loopdev
fi

#target=$(do_facet ost1 $LCTL dl | awk '/obdfilter/ {print $4;exit}')
#[[ -n $target ]] && { obdecho_test $target ost1 || rc=1; }
[ $rmmod_remote -eq 1 ] && do_facet ost1 "rmmod osdbench"

cleanupall -f

