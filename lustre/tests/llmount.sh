#!/bin/bash

LUSTRE=${LUSTRE:-$(dirname $0)/..}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}

[ -n "$LOAD" ] && load_modules && exit 0
[ -z "$NOFORMAT" ] && formatall
[ -z "$NOSETUP" ] && setupall
