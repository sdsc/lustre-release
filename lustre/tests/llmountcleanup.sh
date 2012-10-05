#!/bin/bash
LUSTRE=${LUSTRE:-$(dirname $0)/..}
export PATH=$LUSTRE/utils:$PATH
NAME=${NAME:-local}

. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}

cleanupall -f
