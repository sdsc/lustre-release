#!/bin/sh

# NOTE: Please avoid bashisms (bash specific syntax) in this script

run_cmd()
{
	cmd="$@"
	echo -n "Running $cmd"
	eval $cmd
	res=$?
	if [ $res -ne 0 ]; then
		echo " failed: $res"
		echo "Aborting"
		exit 1
	fi
	echo
}

run_cmd "aclocal -I $PWD/build/autoconf"
run_cmd "autoheader"
run_cmd "automake -a -c"
run_cmd autoconf
