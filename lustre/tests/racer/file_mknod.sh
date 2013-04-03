#!/bin/bash

DIR=$1
MAX=$2

if ! [ -x "$MULTIOP" ]; then
    echo "$0: cannot find multiop executable, expected '$MULTIOP'" >&2
    exit 1
fi

while true; do
	file=$DIR/$((RANDOM % MAX))
	$MULTIOP $file m 2> /dev/null
done
