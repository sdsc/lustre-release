#ifndef LIBCFS_TYPES_H_
#define LIBCFS_TYPES_H_

#ifdef LUSTRE_BUILD
# include <linux/types.h>
# ifdef __KERNEL__
#  if defined(_ASM_GENERIC_INT_L64_H)
#   define LIBCFS_TYPES_PRI_64 "l"
#  elif defined(_ASM_GENERIC_INT_LL64_H)
#   define LIBCFS_TYPES_PRI_64 "ll"
#  endif /* _ASM_GENERIC_INT_LL64_H */
# endif /* __KERNEL__ */
#endif /* LUSTRE_BUILD */

#ifndef __KERNEL__
# include <stdbool.h>
# include <stddef.h>
# include <stdint.h>
#endif  /* !__KERNEL__ */

/* Only define __{u,s}XX and LP*64 if we're building Lustre. */
#ifdef LUSTRE_BUILD
# ifndef __KERNEL__
# include <inttypes.h>
#  ifdef __PRI64_PREFIX
#   define LIBCFS_TYPES_PRI_64 __PRI64_PREFIX
#  endif /* __PRI64_PREFIX */
# endif /* !__KERNEL__ */

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
