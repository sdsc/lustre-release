#!/bin/sh

X=`ps auxwww | grep -E 'hudson[0-9]+\.sh' | sed 's/.*\/tmp\///'`
echo /tmp/$X
echo "==== HUDSON START ===="
cat /tmp/$X
echo "==== HUDSON END ===="

exit 1

# NOTE: Please avoid bashisms (bash specific syntax) in this script

set -e
pw="$PWD"
for dir in libcfs lnet lustre snmp ; do
	ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $pw/$dir/autoconf"
done

libtoolize -q
aclocal -I $pw/config $ACLOCAL_FLAGS
autoheader
automake -a -c
autoconf
