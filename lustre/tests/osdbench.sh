#!/bin/bash
export PATH=`dirname $0`/../utils:$PATH
NAME=${NAME:-local}

export FSTYPE=zfs
LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}

[ -n "$LOAD" ] && load_modules && exit 0
load_modules || exit 0


rc=0
rmmod_remote=0
num=10

do_facet ost1 "lsmod | grep -q osdbench || "                      \
	      "{ insmod ${LUSTRE}/obdecho/osdbench.ko || "        \
	      "modprobe osdbench; }" && rmmod_remote=1

echo destroy
zpool destroy lustre-mdt10 || echo haha
echo "format"
add mds$num $(mkfs_opts mds$num $(mdsdevname ${num})) \
	--reformat $(mdsdevname $num) $TMP/osdbench.img \
	>/dev/null || exit 10

zpool list -H lustre-mdt10 || echo "not listed"
zpool import -d /tmp lustre-mdt10 || echo "import"

echo "setup ozd-zfs"
$LCTL <<-EOF
	attach osd-zfs obe-osd obe_osd_UUID
	cfg_device obe-osd
	setup $(mdsdevname $num) 0 noacl lustre-MDT0009
EOF

echo "setup osdbench"
$LCTL <<-EOF
	attach osdbench obe obe_UUID
	cfg_device obe
	setup obe-osd
EOF

osdbench obe || echo "osdbench failed"

$LCTL <<-EOF
	device obe
	cleanup
	detach
EOF
# OSD shutdowns automatically when the last user (osdbench) disconnects
sleep 1

#target=$(do_facet ost1 $LCTL dl | awk '/obdfilter/ {print $4;exit}')
#[[ -n $target ]] && { obdecho_test $target ost1 || rc=1; }
[ $rmmod_remote -eq 1 ] && do_facet ost1 "rmmod osdbench"

cleanupall -f

