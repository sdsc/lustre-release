#!/bin/bash

DIR=$1
MAX=$2

OSTCOUNT=${OSTCOUNT:-$(lfs df $DIR 2> /dev/null | grep -c OST)}
[ $OSTCOUNT -gt 0] || exit 1

while true; do
	file=$((RANDOM % MAX))
	stripecount=$((RANDOM % (OSTCOUNT + 1)))
	blk_arg=""
	[ $(($file % 8)) -eq 0 ] && blk_arg="--block"
	$LFS migrate ${blk_arg} -c $stripecount $DIR/$file 2> /dev/null
done
