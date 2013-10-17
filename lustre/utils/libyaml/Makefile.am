# libyaml Makefile
SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS= -DHAVE_CONFIG_H

lib_LIBRARIES = libyaml.a
noinst_LIBRARIES = libyamltmp.a

libyamltmp_a_SOURCES := yaml_private.h libyaml.c reader.c scanner.c parser.c loader.c writer.c emitter.c dumper.c config.h
libyamltmp_a_CPPFLAGS := $(AM_CPPFLAGS)

# build static and share lib 
libyaml.a : libyamltmp.a
	rm -f libyaml.a libyaml.so libyaml-0.so.2
	$(CC) $(LDFLAGS) -shared -o libyaml.so `$(AR) -t libyamltmp.a`
	mv libyamltmp.a libyaml.a

install-exec-hook:
	cp libyaml.so  $(DESTDIR)$(libdir)/

EXTRA_DIST = yaml.h
