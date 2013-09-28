#!/bin/bash

LFS=${LFS:-lfs}
DIR=$1
MAX=$2
MDTCOUNT=$3

while /bin/true ; do
	remote_dir=$((RANDOM % MAX))
	file=$((RANDOM % MAX))
	mdt_idx=$((RANDOM % MDTCOUNT))
	mkdir -p $DIR
	$LFS mkdir -i $mdt_idx $DIR/$remote_dir > /dev/null 2>&1
	echo "abcd" > $DIR/$remote_dir/$file 2> /dev/null
	$LFS getdirstripe $DIR/$remote_dir > /dev/null 2>&1
done
