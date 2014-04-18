#!/bin/bash

DIR=$1
MAX=$2

OSTCOUNT=${OSTCOUNT:-$(lfs df $DIR 2> /dev/null | grep -c OST)}

while /bin/true ; do
	file=$((RANDOM % MAX))
	stripecount=$((RANDOM % (OSTCOUNT + 1)))
	blk_arg=""
	[ $(($file % 8)) -eq 0 ] && blk_arg="--block"
	[ $OSTCOUNT -gt 0 ] &&
		$LFS migrate ${blk_arg} -c $stripecount $DIR/$file 2> /dev/null
done
