/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (C) 2015, Trustees of Indiana University
 *
 * Author: Jeremy Filizetti <jfilizet@iu.edu>
 */

#ifndef SK_UTILS_H
#define SK_UTILS_H

#include <gssapi/gssapi.h>
#include <keyutils.h>
#include <lustre/lustre_idl.h>
#include <openssl/dh.h>
#include <sys/types.h>

#include "lsupport.h"

/* SHA256 is used for HMAC and cluster hash */
#define SK_HASH_LEN 32

/* Some limits and defaults */
#define SK_CONF_VERSION 1
#define SK_GENERATOR 2
#define SK_SESSION_MAX_KEYLEN 1024
#define SK_MAX_KEYLEN 128
#define SK_IV_SIZE 16
#define LUSTRE_NODEMAP_NAME_LENGTH 16
#define MAX_MGSNIDS 16

/* String consisting of "lustre:fsname:nodemap_hash" */
#define SK_DESCRIPTION_SIZE (9 + MTI_NAME_MAXLEN + LUSTRE_NODEMAP_NAME_LENGTH)

enum sk_key_type {
	SK_TYPE_INVALID = 0,
	SK_TYPE_CLIENT = 1,
	SK_TYPE_SERVER = 2,
	SK_TYPE_MGS = 3,
};

/* This is the packed structure format of key files that are distributed.
 * The on disk format should be store in big-endian. */
struct sk_keyfile_config {
	/* File format version */
	uint32_t	skc_version;
	/* HMAC algorithm used for message integrity */
	uint16_t	skc_hmac_alg;
	/* Crypt algorithm used for privacy mode */
	uint16_t	skc_crypt_alg;
	/* Number of seconds that a context is valid after it is created from
	 * this keyfile */
	uint32_t	skc_expire;
	/* Length of shared key in skc_shared_key */
	uint32_t	skc_shared_keylen;
	/* Minimum length of the session keys using this keyfile */
	uint32_t	skc_session_keylen;
	/* Array of MGS NIDs to load key's for.  This is for the client since
	 * the upcall only knows the target name which is MGC<IP>@<NET>
	 * Only needed when mounting with mgssec */
	lnet_nid_t	skc_mgsnids[MAX_MGSNIDS];
	/* File system name for this key.  It can be unused for MGS only keys */
	char		skc_fsname[MTI_NAME_MAXLEN + 1];
	/* Nodemap name for this key.  Used by the server side to verify the
	 * client is in the correct nodemap */
	char		skc_nodemap[LUSTRE_NODEMAP_NAME_LENGTH + 1];
	/* Shared key */
	unsigned char	skc_shared_key[SK_MAX_KEYLEN];
} __attribute__((packed));

/* Format passed to the kernel from userspace */
struct sk_kernel_ctx {
	uint32_t	skc_version;
	uint16_t	skc_hmac_alg;
	uint16_t	skc_crypt_alg;
	uint32_t	skc_expire;
	gss_buffer_desc	skc_shared_key;
	gss_buffer_desc	skc_iv;
	gss_buffer_desc	skc_session_key;
};

/* Structure used in context initiation to hold all necessary data */
struct sk_cred {
	uint32_t		 sc_session_keylen;
	uint32_t		 sc_flags;
	gss_buffer_desc		 sc_p;
	gss_buffer_desc		 sc_pub_key;
	gss_buffer_desc		 sc_tgt;
	gss_buffer_desc		 sc_nodemap_hash;
	gss_buffer_desc		 sc_hmac;
	struct sk_kernel_ctx	 sc_kctx;
	DH			*sc_params;
};

void sk_init_logging(char *program, int verbose, int fg);
struct sk_keyfile_config *sk_read_file(char *filename);
int sk_load_keyfile(char *path, enum sk_key_type type);
void sk_config_disk_to_cpu(struct sk_keyfile_config *config);
void sk_config_cpu_to_disk(struct sk_keyfile_config *config);
int sk_validate_config(const struct sk_keyfile_config *config);
uint32_t sk_verify_hash(const char *string,
			const gss_buffer_desc *current_hash);
struct sk_cred *sk_create_cred(const char *fsname, const char *cluster,
			       const uint32_t flags);
uint32_t sk_gen_params(struct sk_cred *skc, bool initiator);
int sk_sign_bufs(struct sk_cred *skc, gss_buffer_desc *bufs, const int numbufs,
		 gss_buffer_desc *hmac_buf);
uint32_t sk_verify_hmac(struct sk_cred *skc, gss_buffer_desc *bufs,
			const int numbufs, gss_buffer_desc *hmac);
void sk_free_cred(struct sk_cred *skc);
uint32_t sk_compute_key(struct sk_cred *skc, const gss_buffer_desc *pub_key);
int sk_serialize_kctx(struct sk_cred *skc, gss_buffer_desc *ctx_token);
int sk_decode_netstring(gss_buffer_desc *bufs, int numbufs,
			gss_buffer_desc *ns);
int sk_encode_netstring(gss_buffer_desc *bufs, int numbufs,
			gss_buffer_desc *ns);

#endif /* SK_UTILS_H */
