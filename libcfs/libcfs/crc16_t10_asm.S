/**
* Copyright (c) 2012, Intel Corporation
*
* Copyright 2012 Xyratex Technology Limited
*
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are
* met:
*
* * Redistributions of source code must retain the above copyright
*   notice, this list of conditions and the following disclaimer.
*
* * Redistributions in binary form must reproduce the above copyright
*   notice, this list of conditions and the following disclaimer in the
*   documentation and/or other materials provided with the
*   distribution.
*
* * Neither the name of the Intel Corporation nor the names of its
*   contributors may be used to endorse or promote products derived from
*   this software without specific prior written permission.
*
*
* THIS SOFTWARE IS PROVIDED BY INTEL CORPORATION ""AS IS"" AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
* PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL INTEL CORPORATION OR
* CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
* EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
* PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
* PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
* LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
* NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
* This code base on intel source - Fast CRC16 Code for T10 DIF.
* www.intel.com/p/en_US/embedded/hwsw/software/crc-license?id=6075&iid=6076
*
* Input buffer address is 16 bytes aligned, buffer size is 64 bytes aligned,
* initial crc value is equal to zero(same as crc_t10dif).
* rewrite by Alexander Boyko <alexander_boyko@xyratex.com>
*/

#define REG_NUM_INVALID		100
	.macro R32_NUM opd r32
	\opd = REG_NUM_INVALID
	.ifc \r32,%eax
	\opd = 0
	.endif
	.ifc \r32,%ecx
	\opd = 1
	.endif
	.ifc \r32,%edx
	\opd = 2
	.endif
	.ifc \r32,%ebx
	\opd = 3
	.endif
	.ifc \r32,%esp
	\opd = 4
	.endif
	.ifc \r32,%ebp
	\opd = 5
	.endif
	.ifc \r32,%esi
	\opd = 6
	.endif
	.ifc \r32,%edi
	\opd = 7
	.endif
	.endm

	.macro XMM_NUM opd xmm
	\opd = REG_NUM_INVALID
	.ifc \xmm,%xmm0
	\opd = 0
	.endif
	.ifc \xmm,%xmm1
	\opd = 1
	.endif
	.ifc \xmm,%xmm2
	\opd = 2
	.endif
	.ifc \xmm,%xmm3
	\opd = 3
	.endif
	.ifc \xmm,%xmm4
	\opd = 4
	.endif
	.ifc \xmm,%xmm5
	\opd = 5
	.endif
	.ifc \xmm,%xmm6
	\opd = 6
	.endif
	.ifc \xmm,%xmm7
	\opd = 7
	.endif
	.ifc \xmm,%xmm8
	\opd = 8
	.endif
	.ifc \xmm,%xmm9
	\opd = 9
	.endif
	.ifc \xmm,%xmm10
	\opd = 10
	.endif
	.ifc \xmm,%xmm11
	\opd = 11
	.endif
	.ifc \xmm,%xmm12
	\opd = 12
	.endif
	.ifc \xmm,%xmm13
	\opd = 13
	.endif
	.ifc \xmm,%xmm14
	\opd = 14
	.endif
	.ifc \xmm,%xmm15
	\opd = 15
	.endif
	.endm

	.macro PFX_OPD_SIZE
	.byte 0x66
	.endm

	.macro PFX_REX opd1 opd2 W=0
	.if ((\opd1 | \opd2) & 8) || \W
	.byte 0x40 | ((\opd1 & 8) >> 3) | ((\opd2 & 8) >> 1) | (\W << 3)
	.endif
	.endm

	.macro MODRM mod opd1 opd2
	.byte \mod | (\opd1 & 7) | ((\opd2 & 7) << 3)
	.endm

	.macro PCLMULQDQ imm8 xmm1 xmm2
	XMM_NUM clmul_opd1 \xmm1
	XMM_NUM clmul_opd2 \xmm2
	PFX_OPD_SIZE
	PFX_REX clmul_opd1 clmul_opd2
	.byte 0x0f, 0x3a, 0x44
	MODRM 0xc0 clmul_opd1 clmul_opd2
	.byte \imm8
	.endm

	.macro PEXTRD imm8 xmm1 reg1
	XMM_NUM extrd_opd2 \xmm1
	R32_NUM extrd_opd1 \reg1
	PFX_OPD_SIZE
	PFX_REX extrd_opd1 extrd_opd2
	.byte 0x0f, 0x3a, 0x16
	MODRM 0xc0 extrd_opd1 extrd_opd2
	.byte \imm8
	.endm

#ifdef __x86_64__
#define BUF	%rdi
#define LEN	%rsi
#else
#define BUF	%eax
#define LEN	%edx
#endif

.align 4, 0x90
.text
.globl  crc16_T10DIF_64x
crc16_T10DIF_64x:

	/* don`t use the 16-bit initial_crc value, it`s equal to 0 */

#ifndef __x86_64__
	/* This is for position independent code(-fPIC) support for 32bit */
	call	delta
delta:
	pop	%ecx
#endif

#ifdef __x86_64__
	movdqa .SHUF_MASK(%rip), %xmm6
#else
	movdqa .SHUF_MASK - delta(%ecx), %xmm6
#endif

	/* receive the initial 64B data */
	movdqa	0x00(BUF), %xmm0
	movdqa	0x10(BUF), %xmm1
	movdqa	0x20(BUF), %xmm2
	movdqa	0x30(BUF), %xmm3

	pshufb	%xmm6, %xmm0
	pshufb  %xmm6, %xmm1
	pshufb  %xmm6, %xmm2
	pshufb  %xmm6, %xmm3

#ifdef __x86_64__
	movdqa .rk15(%rip), %xmm7
#else
	movdqa .rk15 - delta(%ecx), %xmm7
#endif

	sub	$128, LEN
	jl	_64B_left

	/*fold 64B at a time. This section of the code folds 4 xmm registers
	  in parallel */
_fold_64B_loop:
	/* update the buffer p.quader */
	add	$64, BUF
	prefetchnta (BUF)
	movdqa	%xmm0, %xmm4
	movdqa	%xmm1, %xmm5
	PCLMULQDQ	0x00, %xmm7, %xmm0
	PCLMULQDQ	0x11, %xmm7, %xmm4
	PCLMULQDQ	0x00, %xmm7, %xmm1
	PCLMULQDQ	0x11, %xmm7, %xmm5
	xorps	%xmm4, %xmm0
	xorps	%xmm5, %xmm1
	movdqa	0x00(BUF), %xmm4
	movdqa	0x10(BUF), %xmm5
	pshufb	%xmm6, %xmm4
	pshufb	%xmm6, %xmm5
	pxor	%xmm4, %xmm0
	pxor	%xmm5, %xmm1

	movdqa	%xmm2, %xmm4
	movdqa	%xmm3, %xmm5
	PCLMULQDQ	0x00, %xmm7, %xmm2
	PCLMULQDQ	0x11, %xmm7, %xmm4
	PCLMULQDQ	0x00, %xmm7, %xmm3
	PCLMULQDQ	0x11, %xmm7, %xmm5
	xorps	%xmm4, %xmm2
	xorps	%xmm5, %xmm3
	movdqa	0x20(BUF), %xmm4
	movdqa	0x30(BUF), %xmm5
	pshufb	%xmm6, %xmm4
	pshufb	%xmm6, %xmm5
	pxor	%xmm4, %xmm2
	pxor	%xmm5, %xmm3


	sub $64, LEN

	/* check if there is another 64B in the buffer to be able to fold */
	jge	_fold_64B_loop

_64B_left:
	add	$64, BUF

	/* the 64B of folded data is in 4 of the xmm registers: xmm0, xmm1,
	   xmm2, xmm3
	   fold the 4 xmm registers to 1 xmm register with different constants
	*/

#ifdef __x86_64__
	movdqa .rk17(%rip), %xmm7
#else
	movdqa .rk17 - delta(%ecx), %xmm7
#endif

	movdqa	%xmm0, %xmm4
	PCLMULQDQ	0x11, %xmm7, %xmm0
	PCLMULQDQ	0x00, %xmm7, %xmm4
	pxor	%xmm4, %xmm3
	xorps	%xmm0, %xmm3

#ifdef __x86_64__
	movdqa .rk19(%rip), %xmm7
#else
	movdqa .rk19 - delta(%ecx), %xmm7
#endif

	movdqa	%xmm1, %xmm4
	PCLMULQDQ	0x11, %xmm7, %xmm1
	PCLMULQDQ	0x00, %xmm7, %xmm4
	pxor	%xmm4, %xmm3
	xorps	%xmm1, %xmm3

#ifdef __x86_64__
	movdqa .rk1(%rip), %xmm7
#else
	movdqa .rk1 - delta(%ecx), %xmm7
#endif

	movdqa	%xmm2, %xmm4
	PCLMULQDQ	0x11, %xmm7, %xmm2
	PCLMULQDQ	0x00, %xmm7, %xmm4
	pxor	%xmm4, %xmm3
	xorps	%xmm2, %xmm3

	add	$64, LEN

	/* compute crc of a 128-bit value */
#ifdef __x86_64__
	movdqa .rk5(%rip), %xmm7
#else
	movdqa .rk5 - delta(%ecx), %xmm7
#endif
	movdqa	%xmm3, %xmm0

	/* 64b fold */
	PCLMULQDQ	0x01, %xmm7, %xmm3
	pslldq	$8, %xmm0
	pxor	%xmm0, %xmm3

	/* 32b fold */
	movdqa	%xmm3, %xmm0

#ifdef __x86_64__
	pand .mask(%rip), %xmm0
#else
	pand .mask - delta(%ecx), %xmm0
#endif
	psrldq	$12, %xmm3
	PCLMULQDQ	0x10, %xmm7, %xmm3
	pxor	%xmm0, %xmm3

	/* barrett reduction */
_barrett:
#ifdef __x86_64__
	movdqa .rk7(%rip), %xmm7
#else
	movdqa .rk7 - delta(%ecx), %xmm7
#endif
	movdqa	%xmm3, %xmm0
	PCLMULQDQ	0x01, %xmm7, %xmm3
	pslldq	$4, %xmm3
	PCLMULQDQ	0x11, %xmm7, %xmm3
	pslldq	$4, %xmm3
	pxor	%xmm0, %xmm3
	PEXTRD	0x01, %xmm3, %eax

_cleanup:
	/* scale the result back to 16 bits */
	shr	$16, %eax
	ret


.data
.align 16
.rk1:
	.quad 0x2d56000000000000
.rk2:
	.quad 0x06df000000000000
.rk5:
	.quad 0x2d56000000000000
.rk6:
	.quad 0x1368000000000000
.rk7:
	.quad 0x00000001f65a57f8
.rk8:
	.quad 0x000000018bb70000
.rk15:
	.quad 0x044c000000000000
.rk16:
	.quad 0xe658000000000000
.rk17:
	.quad 0xad18000000000000
.rk18:
	.quad 0xa497000000000000
.rk19:
	.quad 0x6ee3000000000000
.rk20:
	.quad 0xe7b5000000000000

.align 16, 0x90
.mask:
	.octa 0x00000000FFFFFFFFFFFFFFFFFFFFFFFF
.SHUF_MASK:
	.octa 0x000102030405060708090A0B0C0D0E0F
