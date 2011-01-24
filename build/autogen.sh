#!/bin/bash

# taken from gnome-common/macros2/autogen.sh
compare_versions() {
    ch_min_version=$1
    ch_actual_version=$2
    ch_status=0
    IFS="${IFS=         }"; ch_save_IFS="$IFS"; IFS="."
    set $ch_actual_version
    for ch_min in $ch_min_version; do
        ch_cur=`echo $1 | sed 's/[^0-9].*$//'`; shift # remove letter suffixes
        if [ -z "$ch_min" ]; then break; fi
        if [ -z "$ch_cur" ]; then ch_status=1; break; fi
        if [ $ch_cur -gt $ch_min ]; then break; fi
        if [ $ch_cur -lt $ch_min ]; then ch_status=1; break; fi
    done
    IFS="$ch_save_IFS"
    return $ch_status
}

error_msg() {
	echo "$cmd is $1.  version $required is required to build Lustre."

	exit 1
}

check_version() {
    local tool
    local cmd
    local required
    local version

    tool=$1
    cmd=$2
    required=$3
    echo -n "checking for $cmd $required... "
    if ! $cmd --version >/dev/null ; then
	error_msg "missing"
    fi
    version=$($cmd --version | awk "/$tool \(GNU/ { print \$4 }")
    echo "found $version"
    if ! compare_versions "$required" "$version" ; then
	error_msg "too old"
    fi
}

echo "Checking for a complete tree..."
if [ -d kernel_patches ] ; then
    # This is ldiskfs
    REQUIRED_DIRS="build"
    CONFIGURE_DIRS=""
else
    REQUIRED_DIRS="build libcfs lnet lustre"
    OPTIONAL_DIRS="snmp portals"
    CONFIGURE_DIRS="libsysio lustre-iokit ldiskfs spl zfs"
fi

for dir in $REQUIRED_DIRS ; do
    if [ ! -d "$dir" ] ; then
	cat >&2 <<EOF
Your tree seems to be missing $dir.
Please read README.lustrecvs for details.
EOF
	exit 1
    fi
    ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $PWD/$dir/autoconf"
done
# optional directories for Lustre
for dir in $OPTIONAL_DIRS; do
    if [ -d "$dir" ] ; then
	ACLOCAL_FLAGS="$ACLOCAL_FLAGS -I $PWD/$dir/autoconf"
    fi
done

for AMVER in 1.9 1.10 1.11; do
     [ "$(which automake-$AMVER 2> /dev/null)" ] && break
done

[ "${AMVER#1.}" -ge "10" ] && AMOPT="-W no-portability"

check_version automake automake-$AMVER "1.9"
check_version autoconf autoconf "2.57"

echo "Running aclocal-$AMVER $ACLOCAL_FLAGS..."
aclocal-$AMVER $ACLOCAL_FLAGS || exit 1
echo "Running autoheader..."
autoheader || exit 1
echo "Running automake-$AMVER..."
automake-$AMVER -a -c $AMOPT || exit 1
echo "Running autoconf..."
autoconf || exit 1

export ACLOCAL="aclocal-$AMVER"
export AUTOMAKE="automake-$AMVER"

# Run autogen.sh in these directories
for dir in $CONFIGURE_DIRS; do
    if [ -d $dir ] ; then
        pushd $dir >/dev/null
        echo "Running autogen for $dir..."
        sh autogen.sh || exit $?
        popd >/dev/null
    fi
done
