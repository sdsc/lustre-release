#!/bin/bash

config=${1:-mcrlov.xml}

LMC="../utils/lmc -m $config"

# TCP/IP servers
SERVERS="ba-ost-1  ba-ost-2"
ROUTER=dev5
MDS=dev7
TMP=${TMP:-/tmp}

# Elan clients
CLIENT_LO=dev2
CLIENT_HI=dev25

TCPBUF=1048576
 

h2elan () {
    echo $1 | sed 's/[^0-9]*//g'
}

h2tcp () {
    echo "${1}"
}

[ -f $config ] && rm $config

# Client node
${LMC} --add net --node client --nid '*' --nettype elan || exit 1
# Router node
${LMC} --add net --router --node $ROUTER --tcpbuf $TCPBUF --nid `h2tcp $ROUTER`  --nettype tcp || exit 1
${LMC} --add net --node $ROUTER --nid `h2elan $ROUTER` --nettype elan|| exit 1
${LMC} --add route --node $ROUTER --gw `h2elan $ROUTER` --lo `h2elan $CLIENT_LO` --hi `h2elan $CLIENT_HI` --nettype elan || exit 2

${LMC} --add net --node $MDS --nid `h2elan $MDS` --nettype elan || exit 1
${LMC} --add mds --node $MDS --mds mds1 --dev $TMP/mds1 --size 100000 || exit 1
${LMC} --add lov --lov lov1 --mds mds1 --stripe_sz 1048576 --stripe_cnt 0 --stripe_pattern 0 || exit 1

${LMC} --add mtpt --node client --path /mnt/lustre --clientoptions async --mds mds1 --lov lov1

for s in $SERVERS
 do
   # server node
   ${LMC} --add net --node $s --tcpbuf $TCPBUF --nid $s --nettype tcp || exit 1
   # route to server
   ${LMC} --add route --node $ROUTER --nettype tcp --gw `h2tcp $ROUTER` --lo $s || exit 2
   # the device on the server
   #${LMC} --format --lov lov1 --node $s --ost bluearc || exit 3
   ${LMC} --add ost  --lov lov1 --node $s --dev bluearc --format || exit 3
done
