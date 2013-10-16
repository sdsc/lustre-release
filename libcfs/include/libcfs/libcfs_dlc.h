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
	__u32 ioc_net;
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
		struct {
			__u32 local_ip;
			__u32 peer_ip;
			__u32 peer_port;
			__u32 conn_count;
			__u32 shared_count;
			__u32 pid;
		} socklnd;
		struct {
			__u32 state;
			__u32 sent_hello;
			__u32 peer_ref_count;
			__u32 pid;
			__u32 nsendq_nactiveq;
			__u32 credits_outstanding_creidts;
			__u64 incarnation;
			__u64 next_matchbits;
			__u64 last_matchbits_seen;
		} ptllnd;
		struct {
			__u32 share_count;
			__u32 peer_ip;
			__u32 peer_port;
		} ralnd;
		struct {
			__u64 peer_stamp;
			__u32 dev_id;
			__u32 peer_status;
			__u32 peer_ref_count;
			__u32 fmaq_len;
			__u32 nfma;
			__u32 tx_seq;
			__u32 rx_seq;
			__u32 nrdma;
		} gnilnd;
		struct {
			__u32 peer_ref_count;
			__u32 connecting;
			__u32 accepting;
			__u32 active_conn;
			__u32 waiting_conn;
		} o2iblnd;
		struct {
			__u32 peer_ref_count;
		} mxlnd;
	} lnd_u;
};

struct ioctl_tx_queue {
	int tx_sending;
	int tx_queued;
	int tx_waiting;
	int tx_status;
	unsigned long tx_deadline;
	__u64 tx_cookie;
	__u8 tx_msg_type;
	__u8 tx_msg_credits;
};

typedef enum {
	TX_QUEUE_NOOPS = 0,
	TX_QUEUE_CR,
	TX_QUEUE_NCR,
	TX_QUEUE_RSRVD,
	TX_QUEUE_ACTIVE,
	TX_QUEUE_MAX,
} tx_conn_queue_type_t;

struct libcfs_ioctl_conn {
	struct libcfs_ioctl_hdr hdr;
	__u32 ioc_net;
	__u32 ioc_count;
	__u64 ioc_nid;
	__u32 ioc_detail;

	union {
		struct {
			__u32 tx_buf_size;
			__u32 nagle;
			__u32 peer_ip;
			__u32 peer_port;
			__u32 local_ip;
			__u32 type;
			__u32 cpt;
			__u32 rx_buf_size;
			__u32 pid;
		} socklnd;
		struct {
			__u32 rad_id;
		} ralnd;
		struct {
			__u32 gnd_id;
		} gnilnd;
		struct {
			__u32 path_mtu;
			tx_conn_queue_type_t q_type;
			__u32 num_entries;
			struct ioctl_tx_queue tx_q[MAX_NUM_SHOW_ENTRIES];
		} o2iblnd;
	} lnd_u;
};

struct libcfs_ioctl_lnet_stats {
	struct libcfs_ioctl_hdr hdr;
	lnet_counters_t cntrs;
};


#endif /* LIBCFS_DLC_H */
