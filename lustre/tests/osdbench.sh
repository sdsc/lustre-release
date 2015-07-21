#!/bin/bash

export PATH=`dirname $0`/../utils:$PATH
NAME=${NAME:-local}
PTLDEBUG=${PTLDEBUG:-0}

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
	--reformat $MDT_DEVNAME $(mdsvdevname 1) >/dev/null || exit 10

if [ $fstype == zfs ]; then
	import_zpool $SINGLEMDS || echo "can't import"
	osdname="osd-zfs"
	targetdev=$MDT_DEVNAME
	echo 1 > /sys/module/zfs/parameters/zfs_prefetch_disable
	echo 0 > /sys/module/zfs/parameters/zfs_mdcomp_disable
	echo 6 > /sys/module/zfs/parameters/spa_asize_inflation
	echo 1663800934 >/sys/module/zfs/parameters/zfs_dirty_data_max
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

#	-d <device's name>
#	-T <total transactions>
#	-t <threads to use>
#	-p <transactions per ioctl
#	-s <sequence offset to FID_SEQ_NORMAL>
#	-I - use index as multislot
#	-f <test> - run specific load
TOTAL=${TOTAL:-100000}
THREADS=${THREADS:-1}
OSDTEST=${OSDTEST:-create_object}

#echo 0x20000000 >/proc/sys/lnet/debug
if [ -n "$PERF" ]; then
	perf record -a >/dev/null &
	PERFPID=$!
fi
osdbench -d obe -f $OSDTEST -t $THREADS -p 10 -T $TOTAL $* || echo "osdbench failed"
if [ -n "$PERF" ]; then
	/usr/bin/kill $PERFPID
	set +e
	wait $PERFPID
	perf report --stdio >/tmp/osdbench-${FSTYPE}-${TOTAL}-${THREADS}.profile
fi
lctl dk | sort -n -k4 -t: >/tmp/ilog
#find /proc/fs/lustre -name site_stats |xargs cat
#find /proc/fs/lustre -name stats | while read NN; do
#	echo $NN
#	cat $NN
#done

for i in `find /proc/fs/lustre/ -type f -name dtstats` \
	 `find /proc -name osp_stats`; do
	echo $i
	grep '[[]usec[]]' $i | gawk '{print $1,$2,$7/$2}'
	echo
done

if false; then
for i in /proc/fs/lustre/osd-*/*/stats; do
	echo -n "ave.new_inode "
	grep new_inode $i | gawk '{ave=$7/$2; print ave;}'
	echo -n "ave.oi_inode "
	grep "oi_insert" $i | gawk '{ave=$7/$2; print ave;}'
done
grep '\[usec\]' /proc/fs/lustre/osdbench/*/stats | \
	gawk '{ave=$7/$2; printf "%15s\t%u\t%f\n",$1,$2,ave;}'
fi

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
#[ $rmmod_remote -eq 1 ] && do_facet ost1 "rmmod osdbench"

#cleanupall -f

