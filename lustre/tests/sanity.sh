#!/bin/bash
#
#set -vx

LUSTRE=${LUSTRE:-$(cd $(dirname $0)/..; echo $PWD)}
. $LUSTRE/tests/test-framework.sh
init_test_env $@
. ${CONFIG:=$LUSTRE/tests/cfg/$NAME.sh}
init_logging

. $LUSTRE/tests/setup-nfs.sh
# first unmount all the lustre client
cleanup_mount $MOUNT
# mount lustre on mds
lustre_client=$(facet_host $(get_facets MDS) | tail -1)
zconf_mount_clients $lustre_client $MOUNT

# setup the nfs
setup_nfs "3" "$MOUNT" "$lustre_client" "$CLIENTS"

sh $LUSTRE/tests/parallel-scale-nfs.sh

# cleanup nfs
cleanup_nfs "$MOUNT" "$lustre_client" "$CLIENTS"

zconf_umount_clients $lustre_client $MOUNT
zconf_mount_clients $CLIENTS $MOUNT

complete $(basename $0) $SECONDS
check_and_cleanup_lustre
exit_status
