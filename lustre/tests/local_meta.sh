COUNT=${COUNT:-1024}
NTHR=${NTHR:-1}
NAMELEN=${NAMELEN:-16}
TESTDIR=${TESTDIR:-testdir}
UNLINK=${UNLINK:-no}
DEBUG=${DEBUG:-no}
LMETA=${LMETA:-local_meta}
INTERVAL=${INTERVAL:-2000}
OTHR=${OTHR:-0}

MDS="prime@tcp"

mount|grep "/mnt/lustre" > /dev/null
[ $? -ne 0 ] && echo "please mount lustre before running test" && exit

for (( i=1; i <= NTHR; i++ )) ; do
	MNTPOINT=/mnt/lustre${i}
	#echo $MNTPOINT
	mkdir $MNTPOINT 2> /dev/null
	mount -t lustre $MDS:/lustre $MNTPOINT
done

# mount|grep "/mnt/lustre"
if [ "$DEBUG" == "no" ]; then
        echo 0 > /proc/sys/lnet/debug
        echo 0 > /proc/sys/lnet/subsystem_debug
fi

echo START opencreate `date`
$LMETA -d -p $TESTDIR -l $COUNT -n $NAMELEN -t $NTHR -o $OTHR -i $INTERVAL
echo END opencreate `date` ======================

if [ "$UNLINK" != "no" ]; then
        echo START unlink `date`
        $LMETA -p $TESTDIR -l $COUNT -n $NAMELEN -t $NTHR -o $OTHR -i $INTERVAL -o 1 -r
        echo END unlink `date` ======================
fi

for (( i=1; i <= NTHR; i++ )) ; do
	MNTPOINT=/mnt/lustre${i}
	umount $MNTPOINT	
done
