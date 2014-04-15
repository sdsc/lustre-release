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
 * Copyright (C) 2013, Trustees of Indiana University
 * Author: Andrew Korty <ajk@iu.edu>
 */

#define SK_DEFAULT_HASH "hmac(sha256)"

#define SK_KG_TOK_MIC_MSG                  0x0404
#define SK_KG_TOK_WRAP_MSG                 0x0504

#define SK_FLAG_SENDER_IS_ACCEPTOR         0x01
#define SK_FLAG_WRAP_CONFIDENTIAL          0x02
#define SK_FLAG_ACCEPTOR_SUBKEY            0x04

static spinlock_t sk_seq_lock;

struct sk_keyblock {
	rawobj_t		 kb_key;
	struct crypto_blkcipher	*kb_tfm;
};

struct sk_context {
	__u64			skc_seq_send;
	__u64			skc_seq_recv;
	__u32			skc_enctype;
	struct sk_keyblock	skc_keye;        /* encryption */
	struct sk_keyblock	skc_keyi;        /* integrity */
	struct sk_keyblock	skc_keyc;        /* checksum */
};

struct sk_header {
	__u16	skh_tok_id;	/* token id */
	__u8	skh_flags;	/* acceptor flags */
	__u8	skh_filler;	/* 0xff */
	__u16	skh_ec;		/* extra count */
	__u16	skh_rrc;	/* right rotation count */
	__u64	skh_seq;	/* sequence number */
	__u8	skh_cksum[0];	/* checksum */
};

struct sk_enctype {
	char		*ske_dispname;
	char		*ske_enc_name;		/* linux tfm name */
	char		*ske_hash_name;		/* linux tfm name */
	int		 ske_enc_mode;		/* linux tfm mode */
	int		 ske_hash_size;		/* checksum size */
	int		 ske_conf_size;		/* confounder size */
	unsigned int	 ske_hash_hmac:1;	/* is hmac? */
};

#define SK_ENCTYPE_AES128_CTS_HMAC_SHA1_256	0x0001
