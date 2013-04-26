# Lustre daos test Makefile
AM_CPPFLAGS = $(LLCPPFLAGS) -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DLUSTRE_UTILS
AM_CFLAGS = $(LLCFLAGS)
LIBCFS :=  $(top_builddir)/libcfs/libcfs/libcfs.a
LIBDAOS := $(top_builddir)/lustre/utils/libdaos.a

DAOS_TEST = daos_test.c daos_test.h

noinst_PROGRAMS = daos_eq
testdir = $(libdir)/lustre/tests
test_SCRIPTS = $(noinst_PROGRAMS)

daos_eq_SOURCES=daos_eq.c $(DAOS_TEST)
daos_eq_LDADD=$(LIBDAOS) -lrt -lm $(PTHREAD_LIBS) $(LIBCFS)
