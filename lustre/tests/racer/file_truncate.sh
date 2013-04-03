#!/bin/bash

DIR=$1
MAX=$2

while true; do
	file=$DIR/$((RANDOM % MAX))
	truncate --no-create --size=$RANDOM $file 2> /dev/null
done
