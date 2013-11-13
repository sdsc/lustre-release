#!/bin/bash

DIR=$1
MAX=$2
OSTCOUNT=${OSTCOUNT:-$(lfs df $DIR 2> /dev/null | grep -c OST)}

block_size=${BLOCK_SIZE:-1024}
max_block_count=${MAX_BLOCK_COUNT:-262144} # 256 MB
max_stripe_count=${MAX_STRIPE_COUNT:-$OSTCOUNT}

exec 2> /dev/null

while true; do
	file=$((RANDOM % MAX))
	block_count=$((RANDOM * (max_block_count + 1) / 32768))

	if [ $max_stripe_count -gt 0 ]; then
		stripe_count=$((RANDOM % (max_stripe_count + 1)))
		lfs setstripe -c $stripe_count $DIR/$file
	fi

	dd if=/dev/zero of=$DIR/$file bs=$block_size count=$block_count
done
