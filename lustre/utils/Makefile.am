# Administration utilities Makefile

AM_CFLAGS  := -fPIC -D_GNU_SOURCE \
	      -D_LARGEFILE64_SOURCE=1 -D_FILE_OFFSET_BITS=64 -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/lnet/utils

MNTMODLDFLAGS := -rdynamic -ldl
MNTMODCFLAGS := -DPLUGIN_DIR=\"${pkglibdir}\"

LIBPTLCTL := $(top_builddir)/lnet/utils/libptlctl.a \
	     $(top_builddir)/libcfs/libcfs/libcfsutil.a \
	     $(top_builddir)/libcfs/libcfs/libcfs.a

if TESTS
EXTRA_PROGRAMS = wirecheck
endif

if UTILS

if GSS
SUBDIRS = gss
endif

# mount only finds helpers in /sbin
rootsbin_PROGRAMS = mount.lustre
bin_SCRIPTS   = llstat llobdstat plot-llstat
bin_PROGRAMS  = lfs
sbin_SCRIPTS  = ldlm_debug_upcall
sbin_PROGRAMS = lctl l_getidentity llverfs lustre_rsync

if TESTS
bin_PROGRAMS  += req_layout
sbin_PROGRAMS += wiretest
endif # TESTS

if SERVER
sbin_PROGRAMS += mkfs.lustre tunefs.lustre llverdev lr_reader \
		 ll_decode_filter_fid llog_reader
endif
if LIBPTHREAD
sbin_PROGRAMS += lhsmtool_posix
endif

pkglib_LTLIBRARIES =
lib_LIBRARIES = liblustreapi.a
if LDISKFS_ENABLED
lib_LIBRARIES += libiam.a
endif
noinst_LIBRARIES = liblustreapitmp.a
endif # UTILS

lctl_SOURCES = lustre_lfsck.c obd.c lustre_cfg.c lctl.c obdctl.h
lctl_LDADD :=  liblustreapi.a $(LIBPTLCTL) $(LIBREADLINE) $(PTHREAD_LIBS)
lctl_DEPENDENCIES := $(LIBPTLCTL) liblustreapi.a

lfs_SOURCES = lfs.c
lfs_LDADD := liblustreapi.a $(LIBPTLCTL) $(LIBREADLINE)
lfs_DEPENDENCIES := $(LIBPTLCTL) liblustreapi.a

lustre_rsync_SOURCES = lustre_rsync.c obd.c lustre_cfg.c lustre_rsync.h
lustre_rsync_LDADD :=  liblustreapi.a $(LIBPTLCTL) $(LIBREADLINE) $(PTHREAD_LIBS)
lustre_rsync_DEPENDENCIES := $(LIBPTLCTL) liblustreapi.a

if EXT2FS_DEVEL
EXT2FSLIB = -lext2fs
E2PLIB = -le2p
else
E2PLIB =
EXT2FSLIB =
endif

if BLKID
BLKIDLIB = -lblkid
else
BLKIDLIB =
endif

llverfs_LDADD := $(EXT2FSLIB) $(E2PLIB)

llverdev_LDADD := $(EXT2FSLIB) $(BLKIDLIB)

liblustreapitmp_a_SOURCES = liblustreapi.c liblustreapi_hsm.c \
			    liblustreapi_nodemap.c lustreapi_internal.h \
			    liblustreapi_json.c liblustreapi_layout.c \
			    liblustreapi_lease.c liblustreapi_util.c \
			    liblustreapi_kernelconn.c liblustreapi_param.c \
			    $(top_builddir)/libcfs/libcfs/util/string.c \
			    $(top_builddir)/libcfs/libcfs/util/param.c \
			    liblustreapi_ladvise.c
if UTILS
LIB_TARGETS = liblustreapi.so
if LDISKFS_ENABLED
LIB_TARGETS += mount_osd_ldiskfs.so
endif
if ZFS_ENABLED
LIB_TARGETS += mount_osd_zfs.so
endif

# build static and shared lib lustreapi
liblustreapi.a : liblustreapitmp.a
	rm -f liblustreapi.a liblustreapi.so
	$(CC) $(LDFLAGS) -shared -o liblustreapi.so `$(AR) -t liblustreapitmp.a`
	mv liblustreapitmp.a liblustreapi.a

install-exec-hook:
	@$(NORMAL_INSTALL)
	$(mkinstalldirs) $(DESTDIR)$(libdir)
	$(mkinstalldirs) $(DESTDIR)$(libdir)/lustre
	for p in $(LIB_TARGETS); do \
	  if test -f $$p; then \
	    f="`echo $$p | sed -e 's|^.*/||'`"; \
	    if `echo -n $$p | grep -q mount`; then \
		echo " $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/lustre/$$f"; \
		$(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/lustre/$$f; \
	    else \
		echo " $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f"; \
		$(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f; \
	    fi; \
	  else :; fi; \
	done
endif # UTILS

if LDISKFS_ENABLED
libiam_a_SOURCES = libiam.c
endif

req_layout_SOURCES = req-layout.c

llog_reader_SOURCES = llog_reader.c
llog_reader_LDADD := $(LIBPTLCTL) liblustreapi.a
llog_reader_DEPENDENCIES := $(LIBPTLCTL) liblustreapi.a

lr_reader_SOURCES = lr_reader.c

if UTILS

if ZFS_ENABLED
lib_LIBRARIES += libmount_utils_zfs.a
noinst_LIBRARIES += libmount_osd_zfs.a

libmount_osd_zfs_a_SOURCES = libmount_utils_zfs.c
libmount_osd_zfs_a_CPPFLAGS := -DHAVE_IOCTL_IN_UNISTD_H $(ZFS_LIBZFS_INCLUDE)

libmount_utils_zfs.a : libmount_osd_zfs.a
	$(CC) $(LDFLAGS) $(MNTMODLDFLAGS) -shared -export-dynamic -pthread \
			 $(ZFS_LIBZFS_LDFLAGS) -o mount_osd_zfs.so \
			 `$(AR) -t libmount_osd_zfs.a`
endif # ZFS_ENABLED

if LDISKFS_ENABLED
lib_LIBRARIES += libmount_utils_ldiskfs.a
noinst_LIBRARIES += libmount_osd_ldiskfs.a

libmount_osd_ldiskfs_a_SOURCES = libmount_utils_ldiskfs.c
libmount_osd_ldiskfs_a_CPPFLAGS := $(SELINUX)

libmount_utils_ldiskfs.a : libmount_osd_ldiskfs.a
	$(CC) $(LDFLAGS) $(MNTMODLDFLAGS) -shared -export-dynamic \
			 $(SELINUX) -o mount_osd_ldiskfs.so \
			 `$(AR) -t libmount_osd_ldiskfs.a`
endif # LDISKFS_ENABLED

mount_lustre_SOURCES = mount_lustre.c mount_utils.c mount_utils.h
mount_lustre_CPPFLAGS := ${MNTMODCFLAGS}
mount_lustre_LDFLAGS  := ${MNTMODLDFLAGS}
mount_lustre_LDADD    := $(LIBPTLCTL) $(SELINUX) $(LDLIBMOUNT)
mount_lustre_DEPENDENCIES := $(LIBPTLCTL)

mkfs_lustre_SOURCES = mkfs_lustre.c mount_utils.c mount_utils.h
mkfs_lustre_CPPFLAGS := -UTUNEFS ${MNTMODCFLAGS}
mkfs_lustre_LDFLAGS  := ${MNTMODLDFLAGS}
mkfs_lustre_LDADD    := $(LIBPTLCTL) $(LDLIBMOUNT)
mkfs_lustre_DEPENDENCIES := $(LIBPTLCTL)

tunefs_lustre_SOURCES = mkfs_lustre.c mount_utils.c mount_utils.h
tunefs_lustre_CPPFLAGS := -DTUNEFS ${MNTMODCFLAGS}
tunefs_lustre_LDFLAGS  := ${MNTMODLDFLAGS}
tunefs_lustre_LDADD    := $(mkfs_lustre_LDADD)
tunefs_lustre_DEPENDENCIES := $(mkfs_lustre_DEPENDENCIES)

l_getidentity_SOURCES = l_getidentity.c
l_getidentity_LDADD := $(LIBPTLCTL)
l_getidentity_DEPENDENCIES := $(LIBPTLCTL)

lhsmtool_posix_SOURCES = lhsmtool_posix.c
lhsmtool_posix_LDADD := liblustreapi.a $(LIBPTLCTL) $(PTHREAD_LIBS)
lhsmtool_posix_DEPENDENCIES := liblustreapi.a $(LIBPTLCTL)

wirecheck_SOURCES = wirecheck.c
wirecheck_CPPFLAGS := -DCC="\"$(CC)\""

wiretest_SOURCES = wiretest.c

endif # UTILS

EXTRA_DIST = llstat llobdstat plot-llstat ldlm_debug_upcall

# NOTE: this should only be run on i386.
newwiretest: wirehdr.c wirecheck
	cp wirehdr.c wiretest.c
	LANG=C ./wirecheck >> wiretest.c
	cp ../ptlrpc/wirehdr.c ../ptlrpc/wiretest.c
	LANG=C ./wirecheck >> ../ptlrpc/wiretest.c
