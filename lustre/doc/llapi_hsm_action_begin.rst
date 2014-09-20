======================
llapi_hsm_action_begin
======================

------------------------------
Lustre API copytool management
------------------------------

:Author: Frank Zago
:Date:   2014-09-20
:Manual section: 3
:Manual group: Lustre HSM User API


SYNOPSIS
========

**#include <lustre/lustreapi.h>**

**int llapi_hsm_action_begin(struct hsm_copyaction_private \*\***\ phcp\ **,
const struct hsm_copytool_private \***\ ct\ **, const struct
hsm_action_item \***\ hai\ **, int** restore_mdt_index\ **, int**
restore_open_flags\ **, bool** is_error\ **)**

**int llapi_hsm_action_end(struct hsm_copyaction_private \*\***\ phcp\ **,
const struct hsm_extent \***\ he\ **, int** hp_flags\ **, int** errval\ **)**

**int llapi_hsm_action_progress(struct hsm_copyaction_private \***\ hcp\ **,
const struct hsm_extent \***\ he\ **, __u64** total\ **, int** hp_flags\ **)**

**int llapi_hsm_action_get_dfid(const struct hsm_copyaction_private \***\ hcp\ **,
lustre_fid  \***\ fid\ **)**

**int llapi_hsm_action_get_fd(const struct hsm_copyaction_private \***\ hcp\ **)**

DESCRIPTION
===========

When a copytool is ready to process an HSM action, received through
**llapi_hsm_copytool_recv**\ (), it must call
**llapi_hsm_action_begin**\ (). *ct* is the opaque copytools handle
previously returned by **llapi_hsm_copytool_register**\ (). *hai* is
the request. *restore_mdt_index* and *restore_open_flags* are only
used for an **HSMA_RESTORE** type of request. *restore_mdt_index* is
the MDT index on which to create the restored file, or -1 for
default. If the copytool doesn't intend to process the request, it
should set *is_error* to **true**, and then call
**llapi_hsm_action_end**\ ().

While performing a copy (i.e. the HSM request is either
**HSMA_ARCHIVE** or **HSMA_RESTORE**), the copytool can inform Lustre
of the progress of the operation with **llapi_hsm_action_progress**\
(). *he* is the interval (*offset*, *length*) of the data
copied. *length* is the total length that is expected to be
transfered. *hp_flags* should be 0. The progress can be checked on any
Lustre client by calling **llapi_hsm_current_action**\ (), or by using
**lfs hsm_action**.

Once the HSM request has been performed,
**llapi_hsm_action_progress**\ () must be called to free-up the
allocated ressources. *errval* is set to 0 on success. On error, it
must be an errno, and hp_flags can be set to **HP_FLAG_RETRY** if the
request is retryable, 0 otherwise. *he* is the interval (*offset*,
*length*) of the data copied. It can be the *hai_extent* of the HSM
request.

For a restore operation, a volatile file, invisible to ls, is
created. **llapi_hsm_action_get_fd**\ () will return a file descriptor
to it, and **llapi_hsm_action_get_dfid**\ () will retrieve its FID.

RETURN VALUE
============

**llapi_hsm_action_get_fd**\ () returns a file descriptor on
success. The other functions return 0 on success. All functions return
a negative errno on failure.

ERRORS
======

The negative errno can be, but is not limited to:

**-EINVAL** An invalid value was passed, the copytool is not opened, ...

**-ENOMEM** Not enough memory to allocate a ressource.

SEE ALSO
========

**llapi_hsm_copytool_register**\ (3), **llapi_hsm_copytool_recv**\ (3),
**lustreapi**\ (7), **lfs**\ (1)

See *lhsmtool_posix.c* in the Lustre sources for a use case of this
API.
