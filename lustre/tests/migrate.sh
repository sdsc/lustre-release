#!/bin/bash
# migrate directory between MDTs, used by sanityn.sh 80b
#
#set -vx
set -e

MIGRATE_DIR=$1
MDTCOUNT=${MDTCOUNT:-$($LFS df $MIGRATE_DIR 2> /dev/null | grep -c MDT)}
echo "migrate directories among ${MDTCOUNT}MDTs"
while true; do
	rc=0
	mdt_idx=$((RANDOM % MDTCOUNT))
	$LFS mv -M $mdt_idx $MIGRATE_DIR 2&>/dev/null || rc=$?
	[ $rc -ne 0 -o $rc -ne 16 ] || break
done

echo "migrate directory failed $rc"
return $rc
