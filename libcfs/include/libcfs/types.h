#ifndef LIBCFS_TYPES_H_
#define LIBCFS_TYPES_H_

#if defined(__KERNEL__) || defined(LUSTRE_BUILD)
# include <linux/types.h>
# define LIBCFS_TYPES_USE_SU
# define LIBCFS_TYPES_HAVE_SU
# if defined(_ASM_GENERIC_INT_L64_H)
#  define LIBCFS_TYPES_PRI_64 "l"
# elif defined(_ASM_GENERIC_INT_LL64_H)
#  define LIBCFS_TYPES_PRI_64 "ll"
# endif /* _ASM_GENERIC_INT_LL64_H */
#endif /* __KERNEL__ || LUSTRE_BUILD */

#ifndef __KERNEL__
# include <stdbool.h>
# include <stddef.h>
#endif  /* !__KERNEL__ */

#if !defined(LIBCFS_TYPES_DEF) && \
    !defined(LIBCFS_TYPES_USE_SU) && \
    !defined(LIBCFS_TYPES_USE_L64) && \
    !defined(LIBCFS_TYPES_USE_LL64)
# if defined(__linux__)
#  if defined(__alpha__)
#   define LIBCFS_TYPES_USE_L64
#  elif defined(__ia64__)
#   define LIBCFS_TYPES_USE_L64
#  elif defined(__powerpc64__)
#   define LIBCFS_TYPES_USE_L64
#  elif defined(__mips64)
#   define LIBCFS_TYPES_USE_L64
#  else /* __mips64 */
#   define LIBCFS_TYPES_USE_LL64
#  endif /* __other__ */
# else /* __linux__ */
#  error "define one of LIBCFS_TYPES_{DEF,USE_{SU,L64,LL64}}"
# endif /* !__linux__ */
#endif /* !LIBCFS_TYPES_{DEF,USE_{SU,L64,LL64}} */

#if defined(LIBCFS_TYPES_USE_SU)
typedef __s8 lu_int8_t;
typedef __s16 lu_int16_t;
typedef __s32 lu_int32_t;
typedef __s64 lu_int64_t;
typedef __u8 lu_uint8_t;
typedef __u16 lu_uint16_t;
typedef __u32 lu_uint32_t;
typedef __u64 lu_uint64_t;
#elif defined(LIBCFS_TYPES_USE_L64)
# define LIBCFS_TYPES_PRI_64 "l"
typedef signed char lu_int8_t;
typedef signed short lu_int16_t;
typedef signed int lu_int32_t;
typedef signed long lu_int64_t;
typedef unsigned char lu_uint8_t;
typedef unsigned short lu_uint16_t;
typedef unsigned int lu_uint32_t;
typedef unsigned long lu_uint64_t;
#elif defined(LIBCFS_TYPES_USE_LL64)
# define LIBCFS_TYPES_PRI_64 "ll"
typedef signed char lu_int8_t;
typedef signed short lu_int16_t;
typedef signed int lu_int32_t;
typedef signed long long lu_int64_t;
typedef unsigned char lu_uint8_t;
typedef unsigned short lu_uint16_t;
typedef unsigned int lu_uint32_t;
typedef unsigned long long lu_uint64_t;
#endif /* !LIBCFS_TYPES_USE_LL64 */

/* Only define __{u,s}XX and LP*64 if we're building Lustre. */
#ifdef LUSTRE_BUILD
# ifndef LIBCFS_TYPES_HAVE_SU
typedef lu_int8_t __s8;
typedef lu_int16_t __s16;
typedef lu_int32_t __s32;
typedef lu_int64_t __s64;
typedef lu_uint8_t __u8;
typedef lu_uint16_t __u16;
typedef lu_uint32_t __u32;
typedef lu_uint64_t __u64;
# endif /* !LIBCFS_TYPES_HAVE_SU */

# ifndef LIBCFS_TYPES_PRI_64
#  error "define LIBCFS_TYPES_PRI_64"
# endif /* !LIBCFS_TYPES_PRI_LL64 */

# define LPF64 LIBCFS_TYPES_PRI_64
# define LPU64 "%"LPF64"u"
# define LPD64 "%"LPF64"d"
# define LPX64 "%#"LPF64"x"
# define LPX64i "%"LPF64"x"
# define LPO64 "%#"LPF64"o"

# define LPLU "%lu"
# define LPLD "%ld"
# define LPLX "%#lx"
# define LPPID "%d"
#endif /* LUSTRE_BUILD */

#endif /* LIBCFS_TYPES_H_ */
