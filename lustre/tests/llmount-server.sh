#!/bin/sh

export PATH=/sbin:/usr/sbin:$PATH

SRCDIR="`dirname $0`"
. $SRCDIR/common.sh

NETWORK=tcp
LOCALHOST=dev4
SERVER=dev4
PORT=1234

setup_portals
setup_lustre

new_fs ext2 /tmp/ost 6000000
OST=${LOOPDEV}
MDSFS=ext2
new_fs ${MDSFS} /tmp/mds 50000
MDS=${LOOPDEV}

$OBDCTL <<EOF
device 0
attach mds MDSDEV
setup ${MDS} ${MDSFS}
device 1
attach obdext2 OBDEXT2DEV
setup ${OST}
device 2
attach ost OSTDEV
setup 1
device 3
attach ptlrpc RPCDEV
setup
device 4
attach ldlm LDLMDEV
setup
quit
EOF
