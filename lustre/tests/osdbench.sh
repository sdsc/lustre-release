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
num=10
fstype=$(facet_fstype $SINGLEMDS)
loopdev=""
MDT_DEVNAME=$(mdsdevname ${SINGLEMDS//mds/})

do_facet ost1 "lsmod | grep -q osdbench || "                      \
	      "{ insmod ${LUSTRE}/obdecho/osdbench.ko || "        \
	      "modprobe osdbench; }" && rmmod_remote=1

if [ $fstype == zfs ]; then
	zpool destroy lustre-mdt10 || echo haha
fi

add mds$num $(mkfs_opts mds$num $(mdsdevname $SINGLEMDS)) \
	--reformat $MDT_DEVNAME $(mdsdevname $SINGLEMDS) \
	>/dev/null || exit 10

if [ $fstype == zfs ]; then
	zpool list -H lustre-mdt10 || echo "not listed"
	zpool import -d /tmp lustre-mdt10 || echo "import"
	osdname="osd-zfs"
	targetdev=$(mdsdevname $num)
elif [ $fstype == ldiskfs ]; then
	osdname="osd-ldiskfs"
	loopdev=$(losetup -f)
	echo losetup $loopdev $MDT_DEVNAME
	losetup $loopdev $MDT_DEVNAME
	targetdev=$loopdev
else
	error "not support fstype: $fstype"
fi

$LCTL <<-EOF
	attach $osdname obe-osd obe_osd_UUID
	cfg_device obe-osd
	setup $targetdev 0 noacl lustre-MDT0009
EOF

echo "setup osdbench"
$LCTL <<-EOF
	attach osdbench obe obe_UUID
	cfg_device obe
	setup obe-osd
EOF

[ -n "$SETUP" ] && exit 0

osdbench -d obe || echo "osdbench failed"

[ -n "$NOCLEANUP" ] && exit 0

$LCTL <<-EOF
	device obe
	cleanup
	detach
EOF
# OSD shutdowns automatically when the last user (osdbench) disconnects
sleep 1

if [ -n $loopdev ]; then
	losetup -f $loopdev
fi

#target=$(do_facet ost1 $LCTL dl | awk '/obdfilter/ {print $4;exit}')
#[[ -n $target ]] && { obdecho_test $target ost1 || rc=1; }
[ $rmmod_remote -eq 1 ] && do_facet ost1 "rmmod osdbench"

cleanupall -f

