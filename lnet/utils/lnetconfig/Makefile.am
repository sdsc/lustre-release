#
# LGPL HEADER START
#
# DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the GNU Lesser General Public License
# (LGPL) version 2.1 or (at your discretion) any later version.
# (LGPL) version 2.1 accompanies this distribution, and is available at
# http://www.gnu.org/licenses/lgpl-2.1.html
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# LGPL HEADER END

#
# Copyright (c) 2013, Intel Corporation.
#

# Author:
#   Amir Shehata <amir.shehata@intel.com>
#

SUBDIRS =

AM_CFLAGS=$(LLCFLAGS)
AM_CPPFLAGS=$(LLCPPFLAGS) -I$(top_builddir)/lnet/utils \
			  -I$(top_builddir)/lnet/utils/cyaml \
			  -DLUSTRE_UTILS=1
AM_LDFLAGS := -L$(top_builddir)/libcfs/libcfs

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
