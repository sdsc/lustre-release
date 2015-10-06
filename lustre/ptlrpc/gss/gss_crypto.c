/*
 * Modifications for Lustre
 *
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 *
 * Copyright (c) 2011, 2014, Intel Corporation.
 *
 * Author: Eric Mei <ericm@clusterfs.com>
 */

/*
 *  linux/net/sunrpc/gss_krb5_mech.c
 *  linux/net/sunrpc/gss_krb5_crypto.c
 *  linux/net/sunrpc/gss_krb5_seal.c
 *  linux/net/sunrpc/gss_krb5_seqnum.c
 *  linux/net/sunrpc/gss_krb5_unseal.c
 *
 *  Copyright (c) 2001 The Regents of the University of Michigan.
 *  All rights reserved.
 *
 *  Andy Adamson <andros@umich.edu>
 *  J. Bruce Fields <bfields@umich.edu>
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the University nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 *  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 *  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#define DEBUG_SUBSYSTEM S_SEC

#include <libcfs/linux/linux-crypto.h>
#include <obd.h>
#include <obd_support.h>

#include "gss_internal.h"
#include "gss_crypto.h"

int gss_keyblock_init(struct gss_keyblock *kb, char *alg_name, int alg_mode)
{
	kb->kb_tfm = crypto_alloc_blkcipher(alg_name, alg_mode, 0);
	if (IS_ERR(kb->kb_tfm)) {
		CERROR("failed to alloc tfm: %s, mode %d\n", alg_name,
		       alg_mode);
		return -1;
	}

	if (crypto_blkcipher_setkey(kb->kb_tfm, kb->kb_key.data,
				    kb->kb_key.len)) {
		CERROR("failed to set %s key, len %d\n", alg_name,
		       kb->kb_key.len);
		return -1;
	}

	return 0;
}

void gss_keyblock_free(struct gss_keyblock *kb)
{
	rawobj_free(&kb->kb_key);
	if (kb->kb_tfm)
		crypto_free_blkcipher(kb->kb_tfm);
}

int gss_keyblock_dup(struct gss_keyblock *new, struct gss_keyblock *kb)
{
	return rawobj_dup(&new->kb_key, &kb->kb_key);
}

int gss_get_bytes(char **ptr, const char *end, void *res, int len)
{
	char *p, *q;
	p = *ptr;
	q = p + len;
	if (q > end || q < p)
		return -1;
	memcpy(res, p, len);
	*ptr = q;
	return 0;
}

int gss_get_rawobj(char **ptr, const char *end, rawobj_t *res)
{
	char   *p, *q;
	__u32   len;

	p = *ptr;
	if (gss_get_bytes(&p, end, &len, sizeof(len)))
		return -1;

	q = p + len;
	if (q > end || q < p)
		return -1;

	/* Support empty objects */
	if (len != 0) {
		OBD_ALLOC_LARGE(res->data, len);
		if (!res->data)
			return -1;
	} else {
		res->len = len;
		res->data = NULL;
		return 0;
	}

	res->len = len;
	memcpy(res->data, p, len);
	*ptr = q;
	return 0;
}

int gss_get_keyblock(char **ptr, const char *end,
		     struct gss_keyblock *kb, __u32 keysize)
{
	char *buf;

	OBD_ALLOC_LARGE(buf, keysize);
	if (buf == NULL)
		return -1;

	if (gss_get_bytes(ptr, end, buf, keysize)) {
		OBD_FREE(buf, keysize);
		return -1;
	}

	kb->kb_key.len = keysize;
	kb->kb_key.data = buf;
	return 0;
}

void gss_buf_to_sg(struct scatterlist *sg, void *ptr, int len)
{
	sg_init_table(sg, 1);
	sg_set_buf(sg, ptr, len);
}

__u32 gss_crypt_generic(struct crypto_blkcipher *tfm,
			int decrypt,
			void *iv,
			void *in,
			void *out,
			int length)
{
	struct blkcipher_desc desc;
	struct scatterlist    sg;
	__u8 local_iv[16] = {0};
	__u32 ret = -EINVAL;

	LASSERT(tfm);
	desc.tfm = tfm;
	desc.info = local_iv;
	desc.flags = 0;

	if (length % crypto_blkcipher_blocksize(tfm) != 0) {
		CERROR("output length %d mismatch blocksize %d\n",
		       length, crypto_blkcipher_blocksize(tfm));
		goto out;
	}

	if (crypto_blkcipher_ivsize(tfm) > 16) {
		CERROR("iv size too large %d\n", crypto_blkcipher_ivsize(tfm));
		goto out;
	}

	if (iv)
		memcpy(local_iv, iv, crypto_blkcipher_ivsize(tfm));

	memcpy(out, in, length);
	gss_buf_to_sg(&sg, out, length);

	if (decrypt)
		ret = crypto_blkcipher_decrypt_iv(&desc, &sg, &sg, length);
	else
		ret = crypto_blkcipher_encrypt_iv(&desc, &sg, &sg, length);

out:
	return ret;
}

inline int gss_digest_hmac(struct crypto_hash *tfm,
			   rawobj_t *key,
			   rawobj_t *hdr,
			   int msgcnt, rawobj_t *msgs,
			   int iovcnt, lnet_kiov_t *iovs,
			   rawobj_t *cksum)
{
	struct hash_desc desc;
	struct scatterlist sg[1];
	int i;
	int rc;

	rc = crypto_hash_setkey(tfm, key->data, key->len);
	if (rc)
		return rc;

	desc.tfm = tfm;
	desc.flags = 0;

	rc = crypto_hash_init(&desc);
	if (rc)
		return rc;

	for (i = 0; i < msgcnt; i++) {
		if (msgs[i].len == 0)
			continue;
		gss_buf_to_sg(sg, (char *) msgs[i].data, msgs[i].len);
		rc = crypto_hash_update(&desc, sg, msgs[i].len);
		if (rc)
			return rc;
	}

	for (i = 0; i < iovcnt; i++) {
		if (iovs[i].kiov_len == 0)
			continue;

		sg_init_table(sg, 1);
		sg_set_page(&sg[0], iovs[i].kiov_page, iovs[i].kiov_len,
			    iovs[i].kiov_offset);
		rc = crypto_hash_update(&desc, sg, iovs[i].kiov_len);
		if (rc)
			return rc;
	}

	if (hdr) {
		gss_buf_to_sg(sg, (char *) hdr->data, sizeof(hdr->len));
		rc = crypto_hash_update(&desc, sg, sizeof(hdr->len));
		if (rc)
			return rc;
	}

	return crypto_hash_final(&desc, cksum->data);
}

inline int gss_digest_norm(struct crypto_hash *tfm,
			   struct gss_keyblock *kb,
			   rawobj_t *hdr,
			   int msgcnt, rawobj_t *msgs,
			   int iovcnt, lnet_kiov_t *iovs,
			   rawobj_t *cksum)
{
	struct hash_desc   desc;
	struct scatterlist sg[1];
	int                i;
	int                rc;

	LASSERT(kb->kb_tfm);
	desc.tfm = tfm;
	desc.flags = 0;

	rc = crypto_hash_init(&desc);
	if (rc)
		return rc;

	for (i = 0; i < msgcnt; i++) {
		if (msgs[i].len == 0)
			continue;
		gss_buf_to_sg(sg, (char *) msgs[i].data, msgs[i].len);
		rc = crypto_hash_update(&desc, sg, msgs[i].len);
		if (rc)
			return rc;
	}

	for (i = 0; i < iovcnt; i++) {
		if (iovs[i].kiov_len == 0)
			continue;

		sg_init_table(sg, 1);
		sg_set_page(&sg[0], iovs[i].kiov_page, iovs[i].kiov_len,
			    iovs[i].kiov_offset);
		rc = crypto_hash_update(&desc, sg, iovs[i].kiov_len);
		if (rc)
			return rc;
	}

	if (hdr) {
		gss_buf_to_sg(sg, (char *) hdr, sizeof(*hdr));
		rc = crypto_hash_update(&desc, sg, sizeof(*hdr));
		if (rc)
			return rc;
	}

	rc = crypto_hash_final(&desc, cksum->data);
	if (rc)
		return rc;

	return gss_crypt_generic(kb->kb_tfm, 0, NULL, cksum->data,
				 cksum->data, cksum->len);
}

int gss_add_padding(rawobj_t *msg, int msg_buflen, int blocksize)
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

int gss_crypt_rawobjs(struct crypto_blkcipher *tfm,
		      int use_internal_iv,
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

		gss_buf_to_sg(&src, inobjs[i].data, inobjs[i].len);
		gss_buf_to_sg(&dst, buf, outobj->len - datalen);

		if (use_internal_iv) {
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
