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

#define DEBUG_SUBSYSTEM S_SEC
#ifdef __KERNEL__
#include <linux/init.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/crypto.h>
#include <linux/mutex.h>
#else
#include <liblustre.h>
#endif

#include <obd.h>
#include <obd_class.h>
#include <obd_support.h>

#include "gss_err.h"
#include "gss_internal.h"
#include "gss_api.h"
#include "gss_asn1.h"
#include "gss_sk.h"

static struct sk_enctype sk_enctypes[] = {
	[SK_ENCTYPE_AES128_CTS_HMAC_SHA1_256] = {	/* aes128-cts */
		"aes128-cts-hmac-sha1-256",
		"ctr(aes)",
		"hmac(sha1)",
		0,
		12,
		16,
		1,
	},
};

#define SK_MAX_ENCTYPES (sizeof sk_enctypes / sizeof(struct sk_enctype))

static
int sk_init_keys(struct sk_context *skctx)
{
	/* import key from keyring */

	return 0;
}

static
__u32 sk_delete_context(struct sk_context *skctx)
{
	return GSS_S_COMPLETE;
}

static
void buf_to_sg(struct scatterlist *sg, void *ptr, int len)
{
	sg_init_table(sg, 1);
	sg_set_buf(sg, ptr, len);
}

static inline
int sk_digest_hmac(struct crypto_hash *tfm, rawobj_t *key,
		struct sk_header *skhdr, int msgcnt, rawobj_t *msgs,
		int iovcnt, lnet_kiov_t *iovs, rawobj_t *checksum)
{
	struct hash_desc	desc;
	struct scatterlist	sg[1];
	int			msgidx;
	int			i;

	crypto_hash_setkey(tfm, key->data, key->len);
	desc.tfm = tfm;
	desc.flags = 0;

	crypto_hash_init(&desc);

	for (msgidx = 0; msgidx < msgcnt; msgidx++) {
		if (msgs[msgidx].len == 0)
			continue;
		buf_to_sg(sg, (char *) msgs[msgidx].data, msgs[msgidx].len);
		crypto_hash_update(&desc, sg, msgs[msgidx].len);
	}

	for (i = 0; i < iovcnt; i++) {
		if (iovs[i].kiov_len == 0)
			continue;

		sg_set_page(&sg[0], iovs[i].kiov_page, iovs[i].kiov_len,
			    iovs[i].kiov_offset);
		crypto_hash_update(&desc, sg, iovs[i].kiov_len);
	}

	if (skhdr) {
		buf_to_sg(sg, (char *)skhdr, sizeof *skhdr);
		crypto_hash_update(&desc, sg, sizeof *skhdr);
	}

	return crypto_hash_final(&desc, checksum->data);
}

static
__s32 sk_make_checksum(__u32 enctype, struct sk_keyblock *kb,
		struct sk_header *skhdr, int msgcnt, rawobj_t *msgs, int iovcnt,
		lnet_kiov_t *iovs, rawobj_t *cksum)
{
	struct sk_enctype	*ske = &sk_enctypes[enctype];
	struct crypto_hash	*tfm;
	__u32			 code = GSS_S_FAILURE;
	int			 rc;

	tfm = crypto_alloc_hash(ske->ske_hash_name, 0, 0);
	if (tfm == NULL) {
		CERROR("failed to alloc TFM: %s\n", ske->ske_hash_name);
		return GSS_S_FAILURE;
	}

	cksum->len = crypto_hash_digestsize(tfm);
	OBD_ALLOC_LARGE(cksum->data, cksum->len);
	if (!cksum->data) {
		cksum->len = 0;
		goto out_tfm;
	}

	rc = sk_digest_hmac(tfm, &kb->kb_key, skhdr, msgcnt, msgs, iovcnt, iovs,
			cksum);

	if (rc == 0)
		code = GSS_S_COMPLETE;
out_tfm:
	crypto_free_hash(tfm);
	return code;
}

static
void fill_sk_header(struct sk_context *skctx, struct sk_header *skhdr,
		int privacy)
{
	unsigned char acceptor_flag;

	if (privacy) {
		skhdr->skh_tok_id = cpu_to_be16(SK_KG_TOK_WRAP_MSG);
		skhdr->skh_flags = SK_FLAG_WRAP_CONFIDENTIAL;
		skhdr->skh_ec = cpu_to_be16(0);
		skhdr->skh_rrc = cpu_to_be16(0);
	} else {
		skhdr->skh_tok_id = cpu_to_be16(SK_KG_TOK_MIC_MSG);
		skhdr->skh_flags = acceptor_flag;
		skhdr->skh_ec = cpu_to_be16(0xffff);
		skhdr->skh_rrc = cpu_to_be16(0xffff);
	}

	skhdr->skh_filler = 0xff;
	spin_lock(&sk_seq_lock);
	skhdr->skh_seq = cpu_to_be64(skctx->skc_seq_send++);
	spin_unlock(&sk_seq_lock);
}

static
__u32 verify_sk_header(struct sk_context *skctx,
		struct sk_header *skhdr, int privacy)
{
	__u16         tok_id, ec_rrc;

	if (privacy) {
		tok_id = SK_KG_TOK_WRAP_MSG;
		ec_rrc = 0x0;
	} else {
		tok_id = SK_KG_TOK_MIC_MSG;
		ec_rrc = 0xffff;
	}

	/* sanity checks */
	if (be16_to_cpu(skhdr->skh_tok_id) != tok_id) {
		CERROR("bad shared key token id\n");
		return GSS_S_DEFECTIVE_TOKEN;
	}
	if (privacy && (skhdr->skh_flags & SK_FLAG_WRAP_CONFIDENTIAL) == 0) {
		CERROR("missing confidential flag\n");
		return GSS_S_BAD_SIG;
	}
	if (skhdr->skh_filler != 0xff) {
		CERROR("bad filler\n");
		return GSS_S_DEFECTIVE_TOKEN;
	}
	if (be16_to_cpu(skhdr->skh_ec) != ec_rrc ||
		be16_to_cpu(skhdr->skh_rrc) != ec_rrc) {
		CERROR("bad EC or RRC\n");
		return GSS_S_DEFECTIVE_TOKEN;
	}

	return GSS_S_COMPLETE;
}

static
__u32 sk_import_context(struct sk_context *skctx, rawobj_t *inbuf)
{
	return 0;
}

static
__u32 gss_import_sec_context_sk(rawobj_t *inbuf, struct gss_ctx *gss_context)
{
	struct sk_context *skctx;
	unsigned int rc;

	if (inbuf == NULL || inbuf->data == NULL)
		return GSS_S_FAILURE;

	OBD_ALLOC_PTR(skctx);
	if (skctx == NULL)
		return GSS_S_FAILURE;

	rc = sk_import_context(skctx, inbuf);

	if (rc == 0)
		rc = sk_init_keys(skctx);

	if (rc) {
		sk_delete_context(skctx);
		OBD_FREE_PTR(skctx);

		return GSS_S_FAILURE;
	}

	gss_context->internal_ctx_id = skctx;
	CDEBUG(D_SEC, "succesfully imported sk context\n");

	return GSS_S_COMPLETE;
}

static
__u32 gss_copy_reverse_context_sk(struct gss_ctx *gss_context_old,
				    struct gss_ctx *gss_context_new)
{
	struct sk_context *skctx_old;
	struct sk_context *skctx_new;

	OBD_ALLOC_PTR(skctx_new);
	if (skctx_new == NULL)
		return GSS_S_FAILURE;

	skctx_old = gss_context_old->internal_ctx_id;
	memcpy(skctx_new, skctx_old, sizeof *skctx_new);
	gss_context_new->internal_ctx_id = skctx_new;
	CDEBUG(D_SEC, "succesfully copied reverse sk context\n");

	return GSS_S_COMPLETE;
}

static
__u32 gss_inquire_context_sk(struct gss_ctx *gss_context,
			       unsigned long *endtime)
{
	*endtime = 0;
	return GSS_S_COMPLETE;
}

static
__u32 gss_get_mic_sk(struct gss_ctx *gss_context,
		     int message_count,
		     rawobj_t *messages,
		     int iov_count,
		     lnet_kiov_t *iovs,
		     rawobj_t *token)
{
	struct sk_context	*skctx = gss_context->internal_ctx_id;
	struct sk_enctype	*ske = &sk_enctypes[skctx->skc_enctype];
	struct sk_header	*skhdr;
	rawobj_t		 cksum = RAWOBJ_EMPTY;

	/* fill shared secret header */
	LASSERT(token->len >= sizeof *skhdr);
	skhdr = (struct sk_header *)token->data;
	fill_sk_header(skctx, skhdr, 0);

	/* generate checksum */
	if (sk_make_checksum(skctx->skc_enctype, &skctx->skc_keyc, skhdr,
				message_count, messages, iov_count, iovs,
				&cksum))
		return GSS_S_FAILURE;

	LASSERT(cksum.len >= ske->ske_hash_size);
	LASSERT(token->len >= sizeof *skhdr + ske->ske_hash_size);
	memcpy(skhdr + 1, cksum.data + cksum.len - ske->ske_hash_size,
		ske->ske_hash_size);

	token->len = sizeof *skhdr + ske->ske_hash_size;
	rawobj_free(&cksum);

	return GSS_S_COMPLETE;
}

static
__u32 gss_verify_mic_sk(struct gss_ctx *gss_context,
			int message_count,
			rawobj_t *messages,
			int iov_count,
			lnet_kiov_t *iovs,
			rawobj_t *token)
{
	struct sk_context	*skctx = gss_context->internal_ctx_id;
	struct sk_enctype	*ske = &sk_enctypes[skctx->skc_enctype];
	struct sk_header	*skhdr;
	rawobj_t		 cksum = RAWOBJ_EMPTY;
	__u32			 major_status;

	if (token->len < sizeof(*skhdr)) {
		CERROR("short signature: %u\n", token->len);
		return GSS_S_DEFECTIVE_TOKEN;
	}

	skhdr = (struct sk_header *)token->data;

	major_status = verify_sk_header(skctx, skhdr, 0);
	if (major_status != GSS_S_COMPLETE) {
		CERROR("bad shared key header\n");
		return major_status;
	}

	if (token->len < sizeof *skhdr + ske->ske_hash_size) {
		CERROR("short signature: %u, require %lud\n", token->len,
			(int) sizeof *skhdr + ske->ske_hash_size);
		return GSS_S_FAILURE;
	}

	if (sk_make_checksum(skctx->skc_enctype, &skctx->skc_keyc, skhdr,
				message_count, messages, iov_count, iovs,
				&cksum)) {
		CERROR("failed to make checksum\n");
		return GSS_S_FAILURE;
	}

	LASSERT(cksum.len >= ske->ske_hash_size);
	if (memcmp(skhdr + 1, cksum.data + cksum.len - ske->ske_hash_size,
			ske->ske_hash_size)) {
		CERROR("shared key checksum mismatch\n");
		rawobj_free(&cksum);
		return GSS_S_BAD_SIG;
	}

	rawobj_free(&cksum);

	return GSS_S_COMPLETE;
}

static
int sk_encrypt_rawobjs(struct crypto_blkcipher *tfm,
		int mode_ecb,
		int inobj_cnt,
		rawobj_t *inobjs,
		rawobj_t *outobj,
		int enc)
{
	struct blkcipher_desc desc;
	struct scatterlist    src, dst;
	__u8                  local_iv[16] = {0}, *buf;
	__u32                 datalen = 0;
	int                   i, rc;
	ENTRY;

	buf = outobj->data;
	desc.tfm  = tfm;
	desc.info = local_iv;
	desc.flags = 0;

	for (i = 0; i < inobj_cnt; i++) {
		LASSERT(buf + inobjs[i].len <= outobj->data + outobj->len);

		buf_to_sg(&src, inobjs[i].data, inobjs[i].len);
		buf_to_sg(&dst, buf, outobj->len - datalen);

		if (mode_ecb) {
			if (enc)
				rc = crypto_blkcipher_encrypt(&desc, &dst, &src,
							src.length);
			else
				rc = crypto_blkcipher_decrypt(&desc, &dst, &src,
							src.length);
		} else {
			if (enc)
				rc = crypto_blkcipher_encrypt_iv(&desc, &dst,
								&src,
								src.length);
			else
				rc = crypto_blkcipher_decrypt_iv(&desc, &dst,
								&src,
								src.length);
		}

		if (rc) {
			CERROR("encrypt error %d\n", rc);
			RETURN(rc);
		}

		datalen += inobjs[i].len;
		buf += inobjs[i].len;
	}

	outobj->len = datalen;
	RETURN(0);
}

static
int add_padding(rawobj_t *msg, int msg_buflen, int blocksize)
{
	int padding;

	padding = (blocksize - (msg->len & (blocksize - 1))) &
		(blocksize - 1);
	if (!padding)
		return 0;

	if (msg->len + padding > msg_buflen) {
		CERROR("bufsize %u too small: datalen %u, padding %u\n",
			msg_buflen, msg->len, padding);
		return -EINVAL;
	}

	memset(msg->data + msg->len, padding, padding);
	msg->len += padding;
	return 0;
}

static
__u32 gss_wrap_sk(struct gss_ctx *gss_context, rawobj_t *gss_header,
		rawobj_t *msg, int msg_buflen, rawobj_t *token)
{
	struct sk_context	*skctx = gss_context->internal_ctx_id;
	struct sk_enctype	*ske = &sk_enctypes[skctx->skc_enctype];
	struct sk_header	*skhdr;
	int			 blocksize;
	rawobj_t		 cksum = RAWOBJ_EMPTY;
	rawobj_t		 data_desc[3], cipher;
	__u8			 conf[GSS_MAX_CIPHER_BLOCK];
	int			 rc    = 0;

	LASSERT(ske);
	LASSERT(ske->ske_conf_size <= GSS_MAX_CIPHER_BLOCK);
	LASSERT(skctx->skc_keye.kb_tfm == NULL ||
		ske->ske_conf_size >=
		crypto_blkcipher_blocksize(skctx->skc_keye.kb_tfm));

	/*
	 * final token format:
	 * ---------------------------------------------------
	 * | sk header | cipher text | checksum (16 bytes) |
	 * ---------------------------------------------------
	 */

	/* fill sk header */
	LASSERT(token->len >= sizeof *skhdr);
	skhdr = (struct sk_header *)token->data;
	fill_sk_header(skctx, skhdr, 1);

	/* generate confounder */
	cfs_get_random_bytes(conf, ske->ske_conf_size);

	LASSERT(skctx->skc_keye.kb_tfm);
	blocksize = crypto_blkcipher_blocksize(skctx->skc_keye.kb_tfm);

	/* padding the message */
	if (add_padding(msg, msg_buflen, blocksize))
		return GSS_S_FAILURE;

	/*
	 * clear text layout for checksum:
	 * ------------------------------------------------------
	 * | confounder | gss header | clear msgs | sk header |
	 * ------------------------------------------------------
	 */
	data_desc[0].data = conf;
	data_desc[0].len = ske->ske_conf_size;
	data_desc[1].data = gss_header->data;
	data_desc[1].len = gss_header->len;
	data_desc[2].data = msg->data;
	data_desc[2].len = msg->len;

	/* compute checksum */
	if (sk_make_checksum(skctx->skc_enctype, &skctx->skc_keyi, skhdr, 3,
				data_desc, 0, NULL, &cksum))
		return GSS_S_FAILURE;

	/*
	 * clear text layout for encryption:
	 * -----------------------------------------
	 * | confounder | clear msgs | sk header |
	 * -----------------------------------------
	 */
	data_desc[0].data = conf;
	data_desc[0].len = ske->ske_conf_size;
	data_desc[1].data = msg->data;
	data_desc[1].len = msg->len;
	data_desc[2].data = (__u8 *)skhdr;
	data_desc[2].len = sizeof *skhdr;

	/* cipher text will be directly inplace */
	cipher.data = (__u8 *)(skhdr + 1);
	cipher.len = token->len - sizeof *skhdr;

	rc = sk_encrypt_rawobjs(skctx->skc_keye.kb_tfm, 0, 3, data_desc,
				&cipher, 1);

	if (rc != 0) {
		rawobj_free(&cksum);
		return GSS_S_FAILURE;
	}

	/* fill in checksum */
	LASSERT(token->len >= sizeof(*skhdr) + cipher.len + ske->ske_hash_size);
	memcpy((char *)(skhdr + 1) + cipher.len,
		cksum.data + cksum.len - ske->ske_hash_size,
		ske->ske_hash_size);
	rawobj_free(&cksum);

	return GSS_S_COMPLETE;
}


static
__u32 gss_unwrap_sk(struct gss_ctx *gss_context, rawobj_t *gss_header,
		      rawobj_t *token, rawobj_t *message)
{
	struct sk_context	*skctx = gss_context->internal_ctx_id;
	struct sk_enctype	*ske = &sk_enctypes[skctx->skc_enctype];
	struct sk_header	*skhdr;
	unsigned char		*tmpbuf;
	int			 blocksize, bodysize;
	rawobj_t		 cipher_in, plain_out;
	__u32			 major_status;

	LASSERT(ske);

	if (token->len < sizeof(*skhdr)) {
		CERROR("short signature: %u\n", token->len);
		return GSS_S_DEFECTIVE_TOKEN;
	}

	skhdr = (struct sk_header *)token->data;

	major_status = verify_sk_header(skctx, skhdr, 1);
	if (major_status != GSS_S_COMPLETE) {
		CERROR("bad shared key header\n");
		return major_status;
	}

	/* block size */
	LASSERT(skctx->skc_keye.kb_tfm);
	blocksize = crypto_blkcipher_blocksize(skctx->skc_keye.kb_tfm);

	/* expected token layout:
	 * ----------------------------------------
	 * | sk header | cipher text | checksum |
	 * ----------------------------------------
	 */
	bodysize = token->len - sizeof *skhdr - ske->ske_hash_size;

	if (bodysize % blocksize) {
		CERROR("odd bodysize %d\n", bodysize);
		return GSS_S_DEFECTIVE_TOKEN;
	}

	if (bodysize <= ske->ske_conf_size + sizeof *skhdr) {
		CERROR("incomplete token: bodysize %d\n", bodysize);
		return GSS_S_DEFECTIVE_TOKEN;
	}

	if (message->len < bodysize - ske->ske_conf_size - sizeof *skhdr) {
		CERROR("buffer too small: %u, require %d\n",
			message->len, bodysize - ske->ske_conf_size);
		return GSS_S_FAILURE;
	}

	/* decrypting */
	OBD_ALLOC_LARGE(tmpbuf, bodysize);
	if (!tmpbuf)
		return GSS_S_FAILURE;

	major_status = GSS_S_FAILURE;

	cipher_in.data = (__u8 *)(skhdr + 1);
	cipher_in.len = bodysize;
	plain_out.data = tmpbuf;
	plain_out.len = bodysize;

	return GSS_S_COMPLETE;
}

static
__u32 gss_prep_bulk_sk(struct gss_ctx *gss_context,
			 struct ptlrpc_bulk_desc *desc)
{
	return GSS_S_COMPLETE;
}

static
__u32 gss_wrap_bulk_sk(struct gss_ctx *gss_context,
			 struct ptlrpc_bulk_desc *desc, rawobj_t *token,
			 int adj_nob)
{
	return GSS_S_COMPLETE;
}

static
__u32 gss_unwrap_bulk_sk(struct gss_ctx *gss_context,
			   struct ptlrpc_bulk_desc *desc,
			   rawobj_t *token, int adj_nob)
{
	return GSS_S_COMPLETE;
}

static
void gss_delete_sec_context_sk(void *internal_context)
{
	struct sk_context *skctx = internal_context;

	OBD_FREE_PTR(skctx);
}

int gss_display_sk(struct gss_ctx *gss_context, char *buf, int bufsize)
{
	return snprintf(buf, bufsize, "sk");
}

static struct gss_api_ops gss_sk_ops = {
	.gss_import_sec_context     = gss_import_sec_context_sk,
	.gss_copy_reverse_context   = gss_copy_reverse_context_sk,
	.gss_inquire_context        = gss_inquire_context_sk,
	.gss_get_mic                = gss_get_mic_sk,
	.gss_verify_mic             = gss_verify_mic_sk,
	.gss_wrap                   = gss_wrap_sk,
	.gss_unwrap                 = gss_unwrap_sk,
	.gss_prep_bulk              = gss_prep_bulk_sk,
	.gss_wrap_bulk              = gss_wrap_bulk_sk,
	.gss_unwrap_bulk            = gss_unwrap_bulk_sk,
	.gss_delete_sec_context     = gss_delete_sec_context_sk,
	.gss_display                = gss_display_sk,
};

static struct subflavor_desc gss_sk_sfs[] = {
	{
		.sf_subflavor   = SPTLRPC_SUBFLVR_SKI,
		.sf_qop         = 0,
		.sf_service     = SPTLRPC_SVC_INTG,
		.sf_name        = "ski"
	},
	{
		.sf_subflavor   = SPTLRPC_SUBFLVR_SKPI,
		.sf_qop         = 0,
		.sf_service     = SPTLRPC_SVC_PRIV,
		.sf_name        = "skpi"
	},
};

/*
 * currently we leave module owner NULL
 */
static struct gss_api_mech gss_sk_mech = {
	.gm_owner       = NULL, /*THIS_MODULE, */
	.gm_name        = "sk",
	.gm_oid         = (rawobj_t) {
		12,
		"\053\006\001\004\001\311\146\215\126\001\000\001",
	},
	.gm_ops         = &gss_sk_ops,
	.gm_sf_num      = 2,
	.gm_sfs         = gss_sk_sfs,
};

int __init init_sk_module(void)
{
	int status;

	spin_lock_init(&sk_seq_lock);

	status = lgss_mech_register(&gss_sk_mech);
	if (status)
		CERROR("Failed to register sk gss mechanism!\n");

	return status;
}

void cleanup_sk_module(void)
{
	lgss_mech_unregister(&gss_sk_mech);
}
