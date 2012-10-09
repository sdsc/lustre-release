#!/bin/bash
#
#set -vx

LUSTRE=${LUSTRE:-$(dirname $0)/..}
sh $LUSTRE/tests/parallel-scale-nfs.sh 4
