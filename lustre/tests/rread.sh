FSNAME=${FSNAME:-lustre}

# facet hosts
mds_HOST=${mds_HOST:-mds1}
mdsfailover_HOST=${mdsfailover_HOST}
mds1_HOST=${mds1_HOST:-$mds_HOST}
mds1failover_HOST=${mds1failover_HOST:-$mdsfailover_HOST}
mgs_HOST=${mgs_HOST:-$mds_HOST}
ost_HOST=${ost_HOST:-oss1}
ostfailover_HOST=${ostfailover_HOST}
#CLIENTS="client1,client2"
CLIENTS=client1
#RCLIENTS="client2"
CLIENTCOUNT=1
CLIENT1=client1
#CLIENT2=client2

TMP=${TMP:-/tmp}

DAEMONSIZE=${DAEMONSIZE:-500}
#MDSDEV=${MDSDEV:-$TMP/${FSNAME}-mdt1}
MDSDEV1="/dev/xvdb"
MDSCOUNT=${MDSCOUNT:-1}
MDSDEVBASE=${MDSDEVBASE:-$TMP/${FSNAME}-mdt}
MDSSIZE=${MDSSIZE:-1000000}
MDSOPT=${MDSOPT:-"--mountfsoptions=errors=remount-ro,iopen_nopriv,user_xattr,acl"}
# MDSJOURNALSIZE=${MDSJOURNALSIZE:-32}

MGSDEV=${MGSDEV:-$MDSDEV1}
MGSSIZE=${MGSSIZE:-$MDSSIZE}


OSTCOUNT=${OSTCOUNT:-6}
OSTDEVBASE=${OSTDEVBASE:-$TMP/${FSNAME}-ost}
OSTSIZE=${OSTSIZE:-1000000}
OSTOPT=""
# Can specify individual ost devs with
OSTDEV1="/dev/xvdb"
OSTDEV2="/dev/xvdc"
OSTDEV3="/dev/xvdd"
OSTDEV4="/dev/xvde"
OSTDEV5="/dev/xvdf"
OSTDEV6="/dev/xvdg"
# on specific hosts with
# ost1_HOST="uml2"

NETTYPE=${NETTYPE:-tcp}
MGSNID=${MGSNID:-`h2$NETTYPE $mgs_HOST`}
FSTYPE=${FSTYPE:-ldiskfs}
STRIPE_BYTES=${STRIPE_BYTES:-1048576}
STRIPES_PER_OBJ=${STRIPES_PER_OBJ:-0}
SINGLEMDS=${SINGLEMDS:-"mds1"}
TIMEOUT=${TIMEOUT:-20}
PTLDEBUG=${PTLDEBUG:-0x33f0404}
DEBUG_SIZE=${DEBUG_SIZE:-10}
SUBSYSTEM=${SUBSYSTEM:- 0xffb7e3ff}

MKFSOPT=""
MOUNTOPT=""
[ "x$MDSJOURNALSIZE" != "x" ] &&
    MKFSOPT=$MKFSOPT" -J size=$MDSJOURNALSIZE"
[ "x$MDSISIZE" != "x" ] &&
    MKFSOPT=$MKFSOPT" -i $MDSISIZE"
[ "x$MKFSOPT" != "x" ] &&
    MKFSOPT="--mkfsoptions=\\\"$MKFSOPT\\\""
[ "x$MDSCAPA" != "x" ] &&
    MKFSOPT="--param mdt.capa=$MDSCAPA"
[ "x$mdsfailover_HOST" != "x" ] &&
    MOUNTOPT=$MOUNTOPT" --failnode=`h2$NETTYPE $mdsfailover_HOST`"
[ "x$STRIPE_BYTES" != "x" ] &&
    MOUNTOPT=$MOUNTOPT" --param lov.stripesize=$STRIPE_BYTES"
[ "x$STRIPES_PER_OBJ" != "x" ] &&
    MOUNTOPT=$MOUNTOPT" --param lov.stripecount=$STRIPES_PER_OBJ"
[ "x$L_GETIDENTITY" != "x" ] &&
    MOUNTOPT=$MOUNTOPT" --param mdt.identity_upcall=$L_GETIDENTITY"
MDS_MKFS_OPTS="--mgs --mdt --fsname=$FSNAME --device-size=$MDSSIZE --param sys.timeout=$TIMEOUT $MKFSOPT $MOUNTOPT $MDSOPT"

MKFSOPT=""
MOUNTOPT=""
[ "x$OSTJOURNALSIZE" != "x" ] &&
    MKFSOPT=$MKFSOPT" -J size=$OSTJOURNALSIZE"
[ "x$MKFSOPT" != "x" ] &&
    MKFSOPT="--mkfsoptions=\\\"$MKFSOPT\\\""
[ "x$OSSCAPA" != "x" ] &&
    MKFSOPT="--param ost.capa=$OSSCAPA"
[ "x$ostfailover_HOST" != "x" ] &&
    MOUNTOPT=$MOUNTOPT" --failnode=`h2$NETTYPE $ostfailover_HOST`"
OST_MKFS_OPTS="--ost --fsname=$FSNAME --device-size=$OSTSIZE --mgsnode=$MGSNID --param sys.timeout=$TIMEOUT $MKFSOPT $MOUNTOPT $OSTOPT"

MDS_MOUNT_OPTS=${MDS_MOUNT_OPTS:-"-o user_xattr,acl"}
OST_MOUNT_OPTS=${OST_MOUNT_OPTS:-""}

#client
MOUNT=${MOUNT:-/mnt/${FSNAME}}
MOUNT1=${MOUNT1:-$MOUNT}
MOUNT2=${MOUNT2:-${MOUNT}2}
MOUNTOPT=${MOUNTOPT:-"user_xattr,acl"}
[ "x$RMTCLIENT" != "x" ] &&
	MOUNTOPT=$MOUNTOPT",remote_client"
DIR=${DIR:-$MOUNT}
DIR1=${DIR:-$MOUNT1}
DIR2=${DIR2:-$MOUNT2}

if [ $UID -ne 0 ]; then
        log "running as non-root uid $UID"
        RUNAS_ID="$UID"
        RUNAS_GID=`id -g $USER`
        RUNAS=""
else
        RUNAS_ID=${RUNAS_ID:-500}
        RUNAS_GID=${RUNAS_GID:-$RUNAS_ID}
        RUNAS=${RUNAS:-"runas -u $RUNAS_ID"}
fi

#PDSH="pdsh -S -Rssh -w"
PDSH=${PDSH:-no_dsh}
FAILURE_MODE=${FAILURE_MODE:-SOFT} # or HARD
POWER_DOWN=${POWER_DOWN:-"powerman --off"}
POWER_UP=${POWER_UP:-"powerman --on"}
SLOW=${SLOW:-no}
FAIL_ON_ERROR=${FAIL_ON_ERROR:-true}

MPIRUN=$(which mpirun 2>/dev/null) || true
MPIRUN_OPTIONS="-mca boot ssh  -mca btl tcp,self"
MPI_USER=${MPI_USER:-mpiuser}
SINGLECLIENT=$(hostname)

cbench_DIR=/data/src/benchmarks/compilebench.hg
cnt_DIR=/data/src/benchmarks/cthon04


LOAD_MODULES_REMOTE=true
