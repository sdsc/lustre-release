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

require_dsh_mds || exit 0

if [ $# -ne 4 ]; then
        echo "$0 backup min max factor"
        echo "backup: file-level backup/restore or OI files remove/recreate"
        echo "min   : min file count for the test set"
        echo "max   : max file count for the test set"
        echo "factor: how to enlarge the test set for each cycle"
        exit 1
fi

RCMD="do_facet ${SINGLEMDS}"
RLCTL="${RCMD} ${LCTL}"
MDTDEV="${FSNAME}-MDT0000"
SHOW_SCRUB="${RLCTL} get_param -n osd-ldiskfs.${MDTDEV}.oi_scrub"
BASEDIR="/tests"

stopall
load_modules
${RCMD} insmod ${LUSTRE}/obdecho/obdecho.ko
add ${SINGLEMDS} $(mkfs_opts mds) --backfstype ldiskfs --reformat \
        $(mdsdevname 1) > /dev/null || exit 2
start ${SINGLEMDS} $(mdsdevname 1) $MDS_MOUNT_OPTS
init_logging

${RLCTL} <<EOF
attach echo_client scrub-MDT0000 scrub-MDT0000_UUID
setup lustre-MDT0000 mdd
EOF

NTHREADS=`${RCMD} cat /proc/cpuinfo | grep "processor.*:" | wc -l`
ECHODEV=`${RLCTL} dl | grep echo_client | awk '{print $1}'`

${RLCTL} <<EOF
cfg_device ${ECHODEV}
test_mkdir ${BASEDIR}
EOF

for ((i=1; i<${NTHREADS}; i++)); do
${RLCTL} <<EOF
cfg_device ${ECHODEV}
test_mkdir ${BASEDIR}${i}
EOF
done

scrub_cleanup() {
${RLCTL} <<EOF
device scrub-MDT0000
cleanup
device scrub-MDT0000
detach
EOF

        sleep 2
        ${RCMD} rmmod obdecho

        stop ${SINGLEMDS}
        unload_modules

        for ((i=0; i<8; i++)); do
                ${RCMD} losetup -d /dev/loop${i} > /dev/null 2>&1
        done
}

trap scrub_cleanup EXIT

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
	# step 7: reformat dev
	add ${SINGLEMDS} $(mkfs_opts mds) --backfstype ldiskfs --reformat \
                $devname > /dev/null || return 5
	# step 8: mount dev
	${RCMD} mount -t ldiskfs $MDS_MOUNT_OPTS $devname $mntpt || return 6
	# step 9: restore metadata
	${RCMD} tar zxfp $metadata -C $mntpt > /dev/null 2>&1 || return 7
	# step 10: restore metaea
	${RCMD} "cd $mntpt; setfattr --restore=$metaea && cd - " || return 8
	# step 11: remove recovery logs
	${RCMD} rm -f $mntpt/OBJECTS/* $mntpt/CATALOGS
	# step 12: umount dev
	${RCMD} umount -d $mntpt || return 9
	# step 13: cleanup tmp backup
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

BACKUP=$1
MINCOUNT=$2
MAXCOUNT=$3
FACTOR=$4
BASECOUNT=0

echo "===== OI scrub performance test start at: `date` ====="

for ((i=$MINCOUNT; i<=$MAXCOUNT; i=$((i * FACTOR)))); do
        nfiles=$((i / NTHREADS - BASECOUNT))

${RLCTL} <<EOF
cfg_device ${ECHODEV}
--threads ${NTHREADS} 0 ${ECHODEV} test_create \
        -d ${BASEDIR} -D ${NTHREADS} -b ${BASECOUNT} -c 0 -n ${nfiles}
device scrub-MDT0000
cleanup
device scrub-MDT0000
detach
EOF

        BASECOUNT=$((i / NTHREADS))
        stop ${SINGLEMDS} || error "Fail to stop MDS!"

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

        echo "----- start to rebuild OI for ${i} files at: `date` -----"
        start ${SINGLEMDS} $(mdsdevname 1) $MDS_MOUNT_OPTS || \
                error "Fail to start MDS!"

        while true; do
	        STATUS=`$SHOW_SCRUB | sed -n '4'p | awk '{print $2}'`
	        if [ "$STATUS" = "completed" ]; then
                        break;
                else
                        sleep 3 # check status every 3 seconds
                fi
        done

        RTIME=`$SHOW_SCRUB | sed -n '18'p | awk '{print $3}'`
        echo "rebuild OI for ${i} files used ${RTIME} seconds"
        SPEED=`$SHOW_SCRUB | sed -n '19'p | awk '{print $3}'`
        echo "everage rebuild speed is ${SPEED}/sec"

${RLCTL} <<EOF
attach echo_client scrub-MDT0000 scrub-MDT0000_UUID
setup lustre-MDT0000 mdd
EOF
done

echo "OI scrub performance test stop at: `date`"

# cleanup the system at last
scrub_cleanup
complete $(basename $0) $SECONDS
exit_status
