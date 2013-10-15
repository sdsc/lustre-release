# lcmd Makefile

SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS=$(LLCPPFLAGS) -I$(top_builddir)/lustre/utils -I$(top_builddir)/lustre/utils/lcmd -I$(top_builddir)/lustre/utils/libyaml -I$(top_builddir)/lustre/utils/lustreconfigapi -I$(top_builddir)/lustre/utils/cYAML -I$(top_builddir)/libcfs/include/ -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/libcfs/libcfs -L$(top_builddir)/lustre/utils -L$(top_builddir)/lustre/utils/cYAML -L$(top_builddir)/lustre/utils/libyaml -L$(top_builddir)/lustre/utils/lustreconfigapi -L$(top_builddir)/lustre/utils/lcmd

L_IOCTL := $(top_builddir)/libcfs/libcfs/util/l_ioctl.c
L_KERNELCOMM := $(top_builddir)/libcfs/libcfs/kernel_user_comm.c

COMMON_SRC = kvl.c kvl.h $(L_IOCTL) $(LKERNELCOMM)
LIBCFS := $(top_builddir)/libcfs/libcfs/libcfs.a $(top_builddir)/libcfs/libcfs/libcfsutil.a
LIBCFS1 := -lcfs -lcfsutil

sbin_PROGRAMS = lcmd

lcmd_SOURCES = lcmd.c lustre_cmds $(COMMON_SRC)

lcmd_CFLAGS = -O0 $(AM_CFLAGS) $(CFLAGS) $(KRBCFLAGS) -O0
lcmd_LDADD =   -lm -ldl -lreadline -lcYAML -lyaml -llustreconfigapi -lauto_complete $(KRBLIBS) $(LIBCFS1)
lcmd_LDFLAGS = $(AM_LDFLAGS) $(KRBLDFLAGS)
lcmd_CPPFLAGS = $(AM_CPPFLAGS)

lib_LIBRARIES =  libauto_complete.a liblustre_cfg_cmds.a
noinst_LIBRARIES =  libauto_completetmp.a liblustre_cfg_cmdstmp.a

liblustre_cfg_cmdstmp_a_SOURCES = liblustre_cfg_cmds.c $(COMMON_SRC)
liblustre_cfg_cmdstmp_a_CPPFLAGS = $(AM_CPPFLAGS)

liblustre_cfg_cmds.a : liblustre_cfg_cmdstmp.a
	rm -f liblustre_cfg_cmds.a liblustre_cfg_cmds.so
	$(CC) $(LDFLAGS) -shared -o liblustre_cfg_cmds.so `$(AR) -t liblustre_cfg_cmdstmp.a`
	mv liblustre_cfg_cmdstmp.a liblustre_cfg_cmds.a

libauto_completetmp_a_SOURCES = libauto_complete.c libauto_complete.h $(COMMON_SRC)
libauto_completetmp_a_CPPFLAGS = $(AM_CPPFLAGS)

libauto_complete.a : libauto_completetmp.a
	rm -f libauto_complete.a libauto_complete.so
	$(CC) $(LDFLAGS) -shared -o libauto_complete.so `$(AR) -t libauto_completetmp.a`
	mv libauto_completetmp.a libauto_complete.a


install-exec-hook:
	cp liblustre_cfg_cmds.so  $(DESTDIR)$(libdir)/
	cp libauto_complete.so $(DESTDIR)$(libdir)/
	cp lustre_cmds $(DESTDIR)$(libdir)/

EXTRA_DIST =
