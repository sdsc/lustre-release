# lcmd Makefile

SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS=$(LLCPPFLAGS) -I$(top_builddir)/lustre/utils -I$(top_builddir)/lustre/utils/cYAML -I$(top_builddir)/lustre/utils/libyaml -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/libcfs/libcfs -L$(top_builddir)/lustre/utils
LIBCFS := $(top_builddir)/libcfs/libcfs/libcfs.a $(top_builddir)/libcfs/libcfs/libcfsutil.a

libcYAMLtmp_a_SOURCES = libcYAML.c libcYAML.h
libcYAMLtmp_a_CPPFLAGS = $(AM_CPPFLAGS)

lib_LIBRARIES = libcYAML.a
noinst_LIBRARIES = libcYAMLtmp.a

# build static and shared lib cYAML
libcYAML.a : libcYAMLtmp.a
	rm -f libcYAML.a libcYAML.so
	$(CC) $(LDFLAGS) -shared -o libcYAML.so `$(AR) -t libcYAMLtmp.a`
	mv libcYAMLtmp.a libcYAML.a

install-exec-hook: libcYAML.so
	@$(NORMAL_INSTALL)
	$(mkinstalldirs) $(DESTDIR)$(libdir)
	@list=$< ; for p in $$list; do \
	  if test -f $$p; then \
	    f="`echo $$p | sed -e 's|^.*/||'`"; \
	    echo " $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f"; \
	    $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f; \
	  else :; fi; \
	done


EXTRA_DIST =
