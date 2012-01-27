dnl Checks for OFED
AC_DEFUN([LN_CONFIG_OFED_SPEC],
[
	# 2.6.39
	AC_MSG_CHECKING([if OFED has rdma_set_reuseaddr])
	LB_LINUX_TRY_COMPILE([
		#include <linux/version.h>
		#include <linux/pci.h>
		#if !HAVE_GFP_T
		typedef int gfp_t;
		#endif
		#include <rdma/rdma_cm.h>
	],[
		rdma_set_reuseaddr(NULL, 1);
		return 0;
	],[
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_OFED_RDMA_SET_REUSEADDR, 1,
			  [rdma_set_reuse defined])
	],[
		AC_MSG_RESULT(no)
	])
])
