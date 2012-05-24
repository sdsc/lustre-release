#!/bin/bash

set -e

SRCDIR=`dirname $0`
export PATH=$PWD/$SRCDIR:$SRCDIR:$PWD/$SRCDIR/../utils:$PATH:/sbin
export NAME=${NAME:-local}

TMP=${TMP:-/tmp}
LUSTRE=${LUSTRE:-`dirname $0`/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
NTHREADS=${NTHREADS:-0}
UNIT=${UNIT:-0}
EJOURNAL=${EJOURNAL:-""}

require_dsh_mds || exit 0

if [ $# -ne 4 ]; then
	echo "$0 backup min max factor"
	echo "backup: file-level backup/restore or OI files remove/recreate"
	echo "min   : min file count for the test set"
	echo "max   : max file count for the test set"
	echo "factor: how to enlarge the test set for each cycle"
	exit 1
fi

BACKUP=$1
MINCOUNT=$2
MAXCOUNT=$3
FACTOR=$4
BASECOUNT=0
RCMD="do_facet ${SINGLEMDS}"
RLCTL="${RCMD} ${LCTL}"
MDTDEV="${FSNAME}-MDT0000"
SHOW_SCRUB="${RLCTL} get_param -n osd-ldiskfs.${MDTDEV}.oi_scrub"

if [ ${NTHREADS} -eq 0 ]; then
	CPUCORE=`${RCMD} cat /proc/cpuinfo | grep "processor.*:" | wc -l`
	NTHREADS=$((CPUCORE * 3))
fi

reformat_external_journal() {
	if [ ! -z ${EJOURNAL} ]; then
		${RCMD} mke2fs -O journal_dev ${EJOURNAL} || return 1
	fi
}

stopall
load_modules
reformat_external_journal
add ${SINGLEMDS} $(mkfs_opts mds) --backfstype ldiskfs --reformat \
	$(mdsdevname 1) > /dev/null || exit 2
init_logging

scrub_attach() {
${RLCTL} <<EOF
attach echo_client scrub-MDT0000 scrub-MDT0000_UUID
setup lustre-MDT0000 mdd
EOF
}

scrub_detach() {
${RLCTL} <<EOF
device scrub-MDT0000
cleanup
device scrub-MDT0000
detach
EOF
}

scrub_cleanup() {
	unload_modules

	for ((i=0; i<8; i++)); do
		${RCMD} losetup -d /dev/loop${i} > /dev/null 2>&1
	done
}

scrub_create_nfiles() {
	local total=$1
	local lbase=$2
	local threads=$3
	local ldir="/test-${lbase}"
	local cycle=0
	local count=${UNIT}

	while true; do
		[ ${count} -eq 0 -o  ${count} -gt ${total} ] && count=${total}
		local usize=$((count / NTHREADS))
		[ ${usize} -eq 0 ] && break
		local tdir=${ldir}-${cycle}-

		echo "[cycle: ${cycle}] [threads: ${threads}]"\
		     "[files: ${count}] [basedir: ${tdir}]"
		start ${SINGLEMDS} $(mdsdevname 1) $MDS_MOUNT_OPTS || \
			error "Fail to start MDS!"
		scrub_attach
		local echodev=`${RLCTL} dl | grep echo_client | awk '{print $1}'`

${RLCTL} <<EOF
cfg_device ${echodev}
test_mkdir ${tdir}
EOF

for ((j=1; j<${threads}; j++)); do
${RLCTL} <<EOF
cfg_device ${echodev}
test_mkdir ${tdir}${j}
EOF
done

${RLCTL} <<EOF
cfg_device ${echodev}
--threads ${threads} 0 ${echodev} test_create \
	-d ${tdir} -D ${threads} -b ${lbase} -c 0 -n ${usize}
EOF

		scrub_detach
		stop ${SINGLEMDS} || error "Fail to stop MDS!"

		total=$((total - usize * NTHREADS))
		[ ${total} -eq 0 ] && break
		lbase=$((lbase + usize))
		cycle=$((cycle + 1))
	done
}

scrub_backup_restore() {
	local devname=$1
	local mntpt=$(facet_mntpt brpt)
	local metaea=${TMP}/scrub_backup_restore.ea
	local metadata=${TMP}/scrub_backup_restore.tgz

	# step 1: build mount point
	${RCMD} mkdir -p $mntpt
	# step 2: cleanup old backup
	${RCMD} rm -f $metaea $metadata
	# step 3: mount dev
	${RCMD} mount -t ldiskfs $MDS_MOUNT_OPTS $devname $mntpt || return 1
	# step 4: backup metaea
	${RCMD} "cd $mntpt; getfattr -R -d -m '.*' -P . > $metaea && cd -" || \
		return 2
	# step 5: backup metadata
	${RCMD} tar zcf $metadata -C $mntpt/ . > /dev/null 2>&1 || return 3
	# step 6: umount
	${RCMD} umount -d $mntpt || return 4
	# step 7: reformat external journal if needed
	reformat_external_journal || return 5
	# step 8: reformat dev
	add ${SINGLEMDS} $(mkfs_opts mds) --backfstype ldiskfs --reformat \
		$devname > /dev/null || return 6
	# step 9: mount dev
	${RCMD} mount -t ldiskfs $MDS_MOUNT_OPTS $devname $mntpt || return 7
	# step 10: restore metadata
	${RCMD} tar zxfp $metadata -C $mntpt > /dev/null 2>&1 || return 8
	# step 11: restore metaea
	${RCMD} "cd $mntpt; setfattr --restore=$metaea && cd - " || return 9
	# step 12: remove recovery logs
	${RCMD} rm -f $mntpt/OBJECTS/* $mntpt/CATALOGS
	# step 13: umount dev
	${RCMD} umount -d $mntpt || return 10
	# step 14: cleanup tmp backup
	${RCMD} rm -f $metaea $metadata
}

scrub_remove_recreate() {
	local devname=$1
	local mntpt=$(facet_mntpt brpt)

	# step 1: build mount point
	${RCMD} mkdir -p $mntpt
	# step 2: mount dev
	${RCMD} mount -t ldiskfs $MDS_MOUNT_OPTS $devname $mntpt || return 1
	# step 3: remove OI files
	${RCMD} rm -f $mntpt/oi.16*
	# step 4: umount
	${RCMD} umount -d $mntpt || return 2
	# OI files will be recreated when mounted as lustre next time.
}

echo "===== OI scrub performance test start at: `date` ====="

for ((i=$MINCOUNT; i<=$MAXCOUNT; i=$((i * FACTOR)))); do
	nfiles=$((i - BASECOUNT))

	stime=`date +%s`
	echo "+++++ start to create for ${i} files set at: `date` +++++"
	scrub_create_nfiles ${nfiles} ${BASECOUNT} ${NTHREADS} || \
		error "Fail to create files!"
	echo "+++++ end to create for ${i} files set at: `date` +++++"
	etime=`date +%s`
	delta=$((etime - stime))
	[ $delta -gt 0 ] || delta=1
	echo "create ${nfiles} files used ${delta} seconds"
	echo "everage create speed is $((nfiles / delta))/sec"

	BASECOUNT=${i}
	if [ ${BACKUP} -ne 0 ]; then
		stime=`date +%s`
		echo "backup/restore ${i} files start at: `date`"
		scrub_backup_restore $(mdsdevname 1) || \
			error "Fail to backup/restore!"
		echo "backup/restore ${i} files end at: `date`"
		etime=`date +%s`
		delta=$((etime - stime))
		[ $delta -gt 0 ] || delta=1
		echo "backup/restore ${i} files used ${delta} seconds"
		echo "everage backup/restore speed is $((i / delta))/sec"
	else
		scrub_remove_recreate $(mdsdevname 1) || \
			error "Fail to remove/recreate!"
	fi

	echo "----- start to rebuild OI for ${i} files set at: `date` -----"
	start ${SINGLEMDS} $(mdsdevname 1) $MDS_MOUNT_OPTS || \
		error "Fail to start MDS!"

	while true; do
		STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
		[ "$STATUS" = "completed" ] && break
		sleep 3 # check status every 3 seconds
	done

	echo "----- end to rebuild OI for ${i} files set at: `date` -----"
	RTIME=`$SHOW_SCRUB | sed -n '18'p | awk '{print $3}'`
	echo "rebuild OI for ${i} files used ${RTIME} seconds"
	SPEED=`$SHOW_SCRUB | sed -n '19'p | awk '{print $3}'`
	echo "everage rebuild speed is ${SPEED}/sec"
	stop ${SINGLEMDS} || error "Fail to stop MDS!"
done

echo "===== OI scrub performance test stop at: `date` ====="

# cleanup the system at last
scrub_cleanup
complete $(basename $0) $SECONDS
exit_status
