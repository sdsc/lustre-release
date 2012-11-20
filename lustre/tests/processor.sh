#!/bin/bash

function kill_processors {
	killall processor.sh
}


function eat_processor {
	while true; do
		bas=1
	done
}
function print_help {
	echo "Usage: $0 -m [MULT] -k -h";
	echo "Start idle cpu threads min, is 1 per processor ";
	echo "-m [MULT] : Processes per cpu: Default is 2x";
	echo "-k : Stop all processing";
	echo "-h : Help info";
}

mult=2
while getopts "km:h" opt;
do
	case $opt in
	  k)
	    kill_processors ;;
	  m)
	    mult="$OPTARG" ;;
	  h)
	    print_help exit 1 ;;
	  \?)
	    print_help exit 1 ;;
	esac
done
echo $mult
nr_cpu=`cat /proc/cpuinfo | grep "processor" | sort -u | wc -l`

for ((cpu=0;cpu<$((nr_cpu*mult));cpu++))
do
	eat_processor&
done
