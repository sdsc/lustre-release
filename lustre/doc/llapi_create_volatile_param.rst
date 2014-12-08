===========================
llapi_create_volatile_param
===========================

--------------------------
Lustre API file management
--------------------------

:Author: Frank Zago for Cray Inc.
:Date:   2015-01-14
:Manual section: 3
:Manual group: Lustre User API


SYNOPSIS
========

**#include <lustre/lustreapi.h>**

**int llapi_create_volatile_param(const char \***\ directory\ **,
int** mdt_idx\ **, int** open_flags\ **, mode_t** mode\ **, const
struct llapi_stripe_param \***\ stripe_param\ **)**

**int llapi_create_volatile_idx(char \***\ directory\ **, int** mdt_idx\ **, int** mode\ **)**

**int llapi_create_volatile(char \***\ directory\ **, int** mode\ **)**


DESCRIPTION
===========

These three functions create a temporary, volatile file on a Lustre
filesystem. The created file is not visible with **ls(1)**. Once the file
is closed, or the owning process dies, the file is permanently removed
from the filesystem.

This function will also work on a non-Lustre filesystem, where the
file is created then deleted, leaving only the file descriptor to
access the file. This is not strictly equivalent because there is a
small window during which the file is visible to users (provided they
have access to the *directory*).

**llapi_create_volatile_idx** and **llapi_create_volatile** are
passthrough, and call **llapi_create_volatile_param** to do the work.

The *directory* parameter indicates where to create the file on the
Lustre filesystem.

*mdt_idx* is the MDT index onto which create the file. To use a
default MDT, set it to -1.

*open_flags* and *mode* are the same as **open(2)**.

*stripe_param* describes the striping information. If it is NULL, then
the default for the directory is used.


RETURN VALUE
============

**llapi_create_volatile_param**, **llapi_create_volatile_idx** and
**llapi_create_volatile** return a file descriptor on success. They
all return a negative errno on failure.


ERRORS
======

The negative errno can be, but is not limited to:

**-EINVAL** An invalid value was passed.

**-ENOMEM** Not enough memory to allocate a resource.


SEE ALSO
========

**lustreapi**\ (7)

See the Lustre sources for a use case of this API.
