MDSCOUNT=${MDSCOUNT:-1}
mds1_HOST=${mds1_HOST:-`hostname`}
mds1failover_HOST=${mds1failover_HOST:-""}
ost1_HOST=${ost1_HOST:-"`hostname`"}
ost2_HOST=${ost2_HOST:-"`hostname`"}
EXTRA_OSTS=${EXTRA_OSTS:-"`hostname`"}
client_HOST="'*'"
LIVE_CLIENT=${LIVE_CLIENT:-"`hostname`"}
# This should always be a list, not a regexp
FAIL_CLIENTS=${FAIL_CLIENTS:-""}

NETTYPE=${NETTYPE:-tcp}
TIMEOUT=${TIMEOUT:-30}
PTLDEBUG=${PTLDEBUG:-0x3f0400}
SUBSYSTEM=${SUBSYSTEM:- 0xffb7e3ff}
MOUNT=${MOUNT:-"/mnt/lustre"}
#CLIENT_UPCALL=${CLIENT_UPCALL:-`pwd`/client-upcall-mdev.sh}
UPCALL=${CLIENT_UPCALL:-`pwd`/replay-single-upcall.sh}

MDSDEV=${MDSDEV:-$TMP/mds1-`hostname`}
MDSSIZE=${MDSSIZE:-10000} #50000000
MDSJOURNALSIZE=${MDSJOURNALSIZE:-0}

OSTDEV=${OSTDEV:-"$TMP/ost%d-`hostname`"}
OSTSIZE=${OSTSIZE:=10000} #50000000
OSTJOURNALSIZE=${OSTJOURNALSIZE:-0}

FSTYPE=${FSTYPE:-ext3}
STRIPE_BYTES=${STRIPE_BYTES:-524288} #1048576
STRIPES_PER_OBJ=${STRIPES_PER_OBJ:-0}

FAILURE_MODE=${FAILURE_MODE:-SOFT} # or HARD
POWER_DOWN=${POWER_DOWN:-"powerman --off"}
POWER_UP=${POWER_UP:-"powerman --on"}

PDSH=no_dsh
