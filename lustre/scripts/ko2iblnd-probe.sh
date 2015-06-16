#!/bin/sh

# flags of IB devices which present
QIB=0
HFI=0
MLX=0

# no profile name by default
PROFILE=""

INFINIBAND="/sys/class/infiniband"

if [ -d $INFINIBAND ]; then
	for dev in `ls -d $INFINIBAND/* | sed -e "s#^$INFINIBAND/##" -e 's#_[0-9]*$##'`; do
		ver=`echo $dev | sed -ne 's#[^0-9]*##p'`
		case $dev in
		hfi*) HFI=1 ;;
		qib*) QIB=1 ;;
		mlx*) [ $MLX -eq 0 -o $ver -lt $MLX ] && MLX=$ver ;;
		esac
	done

	# Set profile name according priority
	if [ $HFI -ne 0 ]; then
		PROFILE="-opa"
	elif [ $QIB -ne 0 ]; then
		PROFILE="-opa"
	elif [ $MLX -eq 5 ]; then
		PROFILE="-mlx5"
	fi
fi

exec /sbin/modprobe --ignore-install ko2iblnd$PROFILE $CMDLINE_OPTS
