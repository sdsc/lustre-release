#!/bin/bash

DIR=$1
MAX=$2
MAX_MB=8

OSTCOUNT=${OSTCOUNT:-$(lfs df $DIR 2> /dev/null | grep -c OST)}

while /bin/true ; do 
	file=$((RANDOM % MAX))
	# $RANDOM is between 0 and 32767, and we want $SIZE is in 64kB units
	SIZE=$((RANDOM * MAX_MB / 32 / 64))
	#echo "file_create: FILE=$DIR/$file SIZE=$SIZE"
	[ $OSTCOUNT -gt 0 ] &&
		lfs setstripe -c $((RANDOM % OSTCOUNT)) $DIR/$file 2> /dev/null
	dd if=/dev/zero of=$DIR/$file bs=64k count=$SIZE 2> /dev/null
done

