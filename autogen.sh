#!/bin/sh

# NOTE: Please avoid bashisms (bash specific syntax) in this script

set -e

libtoolize -q
aclocal
autoheader
automake -a -c
autoconf
