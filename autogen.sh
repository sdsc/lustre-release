#!/bin/sh

# NOTE: Please avoid bashisms (bash specific syntax) in this script

# die a horrible death.  All output goes to stderr.
#
die()
{
	echo "bootstrap failure:  $*"
	echo Aborting
	exit 1
} 1>&2

run_cmd()
{
	echo -n "Running $*"
	eval "$@" || die "command exited with code $?"
	echo
}

for dir in "libcfs lnet lustre snmp portals"; do
	ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $PWD/$dir/autoconf"
done
run_cmd "aclocal -I $PWD/config $ACLOCAL_FLAGS"
run_cmd "autoheader"
run_cmd "automake -a -c"
run_cmd autoconf

cd libsysio
echo "bootstrapping in libsysio..."
run_cmd "sh autogen.sh"
