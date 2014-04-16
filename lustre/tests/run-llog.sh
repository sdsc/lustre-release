#!/bin/bash
set -x

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}

load_llog_test() {
    grep -q llog_test /proc/modules && return
    # Module should have been placed with other lustre modules...
    modprobe llog_test 2>&1 | grep -v "llog_test not found"
    grep -q llog_test /proc/modules && return
    # But maybe we're running from a developer tree...
    insmod $LUSTRE/obdclass/llog_test.ko
    grep -q llog_test /proc/modules && return
    echo "Unable to load llog_test module!"
    false
    return
}

PATH=$(dirname $0):$LUSTRE/utils:$PATH
TMP=${TMP:-/tmp}

MGS=$($LCTL dl | awk '/mgs/ { print $4 }')
[ -z "$MGS" ] && echo "$0: SKIP: no MGS available, skipping llog test" && exit 0

load_llog_test || exit 0
$LCTL modules > $TMP/ogdb-$(hostname)
echo "NOW reload debugging syms.."

# Using ignore_errors will allow lctl to cleanup even if the test fails.
RC=0
$LCTL <<EOT || RC=2
attach llog_test llt_name llt_uuid
setup $MGS
device llt_name
ignore_errors
cleanup
detach
#EOC
EOT
rmmod -vw llog_test || RC2=3
[ $RC -eq 0 -a "$RC2" ] && RC=$RC2

exit $RC
