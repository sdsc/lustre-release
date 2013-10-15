SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS=$(LLCPPFLAGS) -I$(top_builddir)/lnet/utils \
			  -I$(top_builddir)/lnet/utils/cyaml \
			  -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/libcfs/libcfs
LIBCFS := $(top_builddir)/libcfs/libcfs/libcfs.a \
	  $(top_builddir)/libcfs/libcfs/libcfsutil.a

lib_LIBRARIES = liblnetconfig.a
noinst_LIBRARIES = liblnetconfigtmp.a

CYAML := $(top_builddir)/lnet/utils/cyaml/cyaml.c \
	 $(top_builddir)/lnet/utils/cyaml/cyaml.h
liblnetconfigtmp_a_SOURCES := liblnetconfig.c liblnetconfig.h $(CYAML)
liblnetconfigtmp_a_CPPFLAGS := $(AM_CPPFLAGS)

# build static and share lib lnet config library
liblnetconfig.a : liblnetconfigtmp.a
	rm -f liblnetconfig.a liblnetconfig.so
	$(CC) $(LDFLAGS) $(AM_LDFLAGS) -shared -o liblnetconfig.so `$(AR) -t liblnetconfigtmp.a`
	mv liblnetconfigtmp.a liblnetconfig.a

install-exec-hook:
	cp liblnetconfig.so  $(DESTDIR)$(libdir)/

EXTRA_DIST =
