# lcmd Makefile

SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS=$(LLCPPFLAGS) -I$(top_builddir)/lustre/utils -I$(top_builddir)/lustre/utils/cYAML -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/libcfs/libcfs -L$(top_builddir)/lustre/utils -L$(top_builddir)/lustre/utils/cYAML -L$(top_builddir)/lustre/utils/libyaml
LIBCFS := $(top_builddir)/libcfs/libcfs/libcfs.a $(top_builddir)/libcfs/libcfs/libcfsutil.a
SWIG_INCLUDES=-I/usr/include -I/usr/include/linux -I/usr/lib64/gcc/i686-pc-mingw32/4.4.6/include/

lib_LIBRARIES = liblustreconfigapi.a
noinst_LIBRARIES = liblustreconfigapitmp.a

liblustreconfigapitmp_a_SOURCES := liblustreconfigapi.c liblustreconfigapi.h
liblustreconfigapitmp_a_CPPFLAGS := $(AM_CPPFLAGS)
#liblustreconfigapitmp_a_LDFLAGS := $(AM_LDFLAGS)

# build static and share lib lustre config library
liblustreconfigapi.a : liblustreconfigapitmp.a
	rm -f liblustreconfigapi.a liblustreconfigapi.so
	$(CC) $(LDFLAGS) $(AM_LDFLAGS) -shared -o liblustreconfigapi.so `$(AR) -t liblustreconfigapitmp.a`
	mv liblustreconfigapitmp.a liblustreconfigapi.a

install-exec-hook:
	cp liblustreconfigapi.so  $(DESTDIR)$(libdir)/

EXTRA_DIST =
