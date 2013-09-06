AUTOMAKE_OPTIONS = foreign
SUBDIRS = ldiskfs
EXTRA_DIST = @PACKAGE_TARNAME@.spec
EXTRA_DIST += kernel_patches
EXTRA_DIST += config

dist-hook:
	find $(distdir) -name .deps -o \
			-name .git -o \
			-name .#* | xargs rm -rf
	$(MAKE) $(AM_MAKEFLAGS) \
	  top_distdir="$(top_distdir)" distdir="$(distdir)" \
	  module-dist-hook

rpms: @PACKAGE_TARNAME@.spec dist Makefile
	@CONFIGURE_ARGS=""; \
	for arg in $(ac_configure_args); do \
		case $$arg in \
			--with-release=* ) ;; \
			--enable-tests | --disable-tests ) ;; \
			--with-linux=* | --with-linux-obj=* ) ;; \
			* ) CONFIGURE_ARGS="$$CONFIGURE_ARGS '$$arg'" ;; \
		esac; \
	done; \
	RPMARGS="--define \"configure_args $$CONFIGURE_ARGS\""; \
	if [ -n "@LINUX@" ]; then \
		RPMARGS="$$RPMARGS --define \"kdir @LINUX@\""; \
		if [ -n "@LINUX_OBJ@" -a "@LINUX_OBJ@" != "@LINUX@" ]; then \
			RPMARGS="$$RPMARGS --define \"kobjdir @LINUX_OBJ@\""; \
		fi; \
	fi; \
	echo "Building ldiskfs RPM with $$RPMARGS"; \
	eval rpmbuild $$RPMARGS -ta $(distdir).tar.gz

srpm: @PACKAGE_TARNAME@.spec dist Makefile
	eval rpmbuild $$RPMARGS -ta $(distdir).tar.gz

# this only needs to be done if disting stand-alone (i.e. not as a
# subdir of lustre
module-dist-hook:
	if ! grep "AC_INIT(\[Lustre\], \[LUSTRE_VERSION\], \[http:\/\/bugs\.whamcloud\.com\/], \[lustre\])" ../configure.ac; then \
	    if [ -f META ]; then \
		cp META $(distdir)/META; \
	    fi; \
	fi
