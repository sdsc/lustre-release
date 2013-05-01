#!/bin/sh

MDIR=/lib/modules/`uname -r`/lustre
mkdir -p $MDIR

echo "Removing Lustre modules from "$MDIR

rm -f $MDIR/*
depmod -a
rm -f /sbin/mount.lustre
rm -f /usr/sbin/l_getidentity
