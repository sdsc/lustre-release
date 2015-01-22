#!/bin/bash

DIR=$1
MAX=$2

MCREATE=${MCREATE:-mcreate}

while true; do
	file=$DIR/$((RANDOM % MAX))
	$MCREATE $file 2> /dev/null
done
