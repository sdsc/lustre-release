#ifndef LIBCFS_DLC_H
#define LIBCFS_DLC_H

#include <lnet/lib-types.h>
#include <libcfs/libcfs_ioctl.h>
#define MAX_NUM_SHOW_ENTRIES 32

struct libcfs_ioctl_net_config_s {
	char ni_interfaces[LNET_MAX_INTERFACES][LNET_MAX_STR_LEN];
	__u32 ni_status;
	__u32 cpts[LNET_MAX_SHOW_NUM_CPT];
};

struct libcfs_ioctl_pool_cfg_s {
	struct {
		int npages;
		int nbuffers;
		int credits;
		int mincredits;
	} pools[LNET_NRBPOOLS];
};

struct libcfs_ioctl_config_data_s {
	struct libcfs_ioctl_hdr hdr;

	__u32 ioc_net;
	__u32 ioc_count;
	__u64 ioc_nid;
	__u32 ioc_ncpts;

	union {
		struct {
			__u32 hop;
			__u32 priority;
			__u32 flags;
		} route;
		struct {
			char intf[LNET_MAX_STR_LEN];
			int peer_to;
			int peer_cr;
			int peer_buf_cr;
			int credits;
		} net;
		struct {
			int enable;
			int tiny;
			int small;
			int large;
		} buffers;
	} ioc_config_u;

	char ioc_bulk[0];
};

struct libcfs_ioctl_peer {
	struct libcfs_ioctl_hdr hdr;
	__u32 ioc_count;
	__u64 ioc_nid;

	union {
		struct {
			char aliveness[LNET_MAX_STR_LEN];
			int refcount;
			int ni_peertxcredits;
			int peertxcredits;
			int peerrtrcredits;
			int peerminrtrcredtis;
			int peertxqnob;
			int ncpt;
		} peer_credits;
	} lnd_u;
};

struct libcfs_ioctl_lnet_stats {
	struct libcfs_ioctl_hdr hdr;
	lnet_counters_t cntrs;
};


#endif /* LIBCFS_DLC_H */
