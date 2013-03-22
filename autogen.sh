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

# install Lustre Git commit hooks by default - LU-2083
for HOOK in commit-msg prepare-commit-msg; do
	[ -e .git/hooks/$HOOK ] || ln -sf ../../build/$HOOK .git/hooks/
done

echo "Checking for a complete tree..."
REQUIRED_DIRS="build libcfs lnet lustre"
OPTIONAL_DIRS="snmp portals"
CONFIGURE_DIRS="libsysio lustre-iokit ldiskfs"

for dir in $REQUIRED_DIRS ; do
	test -d "$dir" || \
		die "Your tree seems to be missing $dir.
Please read README.lustrecvs for details."

	ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $PWD/$dir/autoconf"
done
# optional directories for Lustre
for dir in $OPTIONAL_DIRS; do
	if [ -d "$dir" ] ; then
		ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $PWD/$dir/autoconf"
	fi
done

PWD_SAVE=$PWD
autogen --version >/dev/null 2>&1 && {
	cd build/bit-masks || \
		die "cannot change directory to build/bit-masks"
	run_cmd make lustre_dlm_flags.h top_builddir=$PWD_SAVE
	cd $PWD_SAVE
}

run_cmd "aclocal $ACLOCAL_FLAGS"
run_cmd "autoheader"
run_cmd "automake -a -c"
run_cmd autoconf

# bootstrap in these directories
for dir in $CONFIGURE_DIRS; do
	if [ -d $dir ] ; then
		cd $dir
		echo "bootstrapping in $dir..."
		run_cmd "sh autogen.sh"
	fi
	cd $PWD_SAVE
done
