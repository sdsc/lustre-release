#!/bin/bash
# vim:expandtab:shiftwidth=4:softtabstop=4:tabstop=4:
#set -x
EXPORT_OPTS=${EXPORT_OPTS:-"rw,async,no_root_squash"}

setup_nfs() {
    NFS_VER=${1}
    MNTPNT=${2}
    LUSTRE_CLIENT=${3}
    NFS_CLIENTS=${4}

    echo "Exporting Lustre filesystem..."

    if [ "$NFS_VER" = "4" ]; then
        EXPORT_OPTS="$EXPORT_OPTS,fsid=0"
        do_nodes $LUSTRE_CLIENT "mkdir -p /var/lib/nfs/v4recovery"
    fi

    do_nodes $LUSTRE_CLIENT,$NFS_CLIENTS "grep -q rpc_pipefs' ' /proc/mounts ||\
        { mkdir -p /var/lib/nfs/rpc_pipefs && \
        mount -t rpc_pipefs sunrpc /var/lib/nfs/rpc_pipefs; }"
    sleep 5

    do_nodes $LUSTRE_CLIENT "export PATH=\$PATH:/sbin:/usr/sbin; \
        service nfs restart"

    do_nodes $NFS_CLIENTS "export PATH=\$PATH:/sbin:/usr/sbin; \
        service rpcidmapd restart"

    do_nodes $LUSTRE_CLIENT "export PATH=\$PATH:/sbin:/usr/sbin; \
        exportfs -o $EXPORT_OPTS *:$MNTPNT && exportfs -v"

    echo -e "\nMounting NFS clients (version $NFS_VER)..."

    do_nodes $NFS_CLIENTS "mkdir -p $MNTPNT"
    if [ "$NFS_VER" = "4" ]; then
        do_nodes $NFS_CLIENTS \
            "mount -t nfs$NFS_VER -o async $LUSTRE_CLIENT:/ $MNTPNT"
    else
        do_nodes $NFS_CLIENTS \
            "mount -t nfs -o nfsvers=$NFS_VER,async \
                $LUSTRE_CLIENT:$MNTPNT $MNTPNT"
    fi
}

cleanup_nfs() {
    MNTPNT=${1}
    LUSTRE_CLIENT=${2}
    NFS_CLIENTS=${3}

    echo -e "\nUnmounting NFS clients..."
    do_nodes $NFS_CLIENTS "umount -f $MNTPNT"

    echo -e "\nUnexporting Lustre filesystem..."
    do_nodes $NFS_CLIENTS "export PATH=\$PATH:/sbin:/usr/sbin; \
        service rpcidmapd stop"

    do_nodes $LUSTRE_CLIENT "export PATH=\$PATH:/sbin:/usr/sbin; \
        service nfs stop"

    do_nodes $LUSTRE_CLIENT "export PATH=\$PATH:/sbin:/usr/sbin; \
        exportfs -u *:$MNTPNT"

    do_nodes $LUSTRE_CLIENT "export PATH=\$PATH:/sbin:/usr/sbin; \
        exportfs -v"
}
