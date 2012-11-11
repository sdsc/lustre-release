#!/bin/bash

DIR=$1
MAX=$2

while /bin/true ; do
    file=$((RANDOM % $MAX))
    new_file=$((RANDOM % MAX))
    $LFS swap_layouts $DIR/$file $DIR/$new_file 2>/dev/null ||
	echo "swapped layout $DIR/$file $DIR/$new_file"
    sleep $((RANDOM%5+1))
done
