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

# TODO: Need to use a standard 2.7.5 python and SWIG installations
# build static and share lib lustre config library
liblustreconfigapi.a : liblustreconfigapitmp.a
	rm -f liblustreconfigapi.a liblustreconfigapi.so
	$(CC) $(LDFLAGS) $(AM_LDFLAGS) -shared -o liblustreconfigapi.so `$(AR) -t liblustreconfigapitmp.a`
	swig -python -includeall -D__x86_64__ $(SWIG_INCLUDES) $(AM_CPPFLAGS) liblustreconfigapi.i
	$(CC) -g -Wall -fPIC -c -I$(HOME)/Python-2.7.5/install/include/python2.7 $(AM_CPPFLAGS) liblustreconfigapi_wrap.c liblustreconfigapi_swig_exta.c
	$(CC) $(LDFLAGS) $(AM_LDFLAGS) -lcYAML -lreadline -lyaml -Wl,--whole-archive $(LIBCFS) -Wl,--no-whole-archive -shared -o _lustreconfigapi.so liblustreconfigapi_wrap.o liblustreconfigapi_swig_exta.o `$(AR) -t liblustreconfigapitmp.a`
	mv liblustreconfigapitmp.a liblustreconfigapi.a

install-exec-hook:
	cp liblustreconfigapi.so  $(DESTDIR)$(libdir)/
	cp _lustreconfigapi.so $(DESTDIR)$(libdir)/
	cp lustreconfigapi.py $(DESTDIR)$(libdir)/

EXTRA_DIST = liblustreconfigapi_swig_exta.c liblustreconfigapi.i
