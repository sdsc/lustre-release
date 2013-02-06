/*  -*- buffer-read-only: t -*- vi: set ro:
 * 
 * DO NOT EDIT THIS FILE   (lustre_dlm_flags.h)
 * 
 * It has been AutoGen-ed  February  6, 2013 at 03:11:43 PM by AutoGen 5.17.0pre6
 * From the definitions    l_flags.def
 * and the template file   l_flags
 *
 * lustre is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * lustre is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef CRASH_EXTENSION
#ifdef  NEED_LDLM_LOCK_DEFINES
#define HAVE_LDLM_LOCK_DEFINES
#undef  NEED_LDLM_LOCK_DEFINES

/** ldlm_lock.l_fl_lock_changed */
#define LDLM_FL_LOCK_CHANGED         0x0000000000000001ULL // bit  0

/** ldlm_lock.l_fl_block_granted */
#define LDLM_FL_BLOCK_GRANTED        0x0000000000000002ULL // bit  1

/** ldlm_lock.l_fl_block_conv */
#define LDLM_FL_BLOCK_CONV           0x0000000000000004ULL // bit  2

/** ldlm_lock.l_fl_block_wait */
#define LDLM_FL_BLOCK_WAIT           0x0000000000000008ULL // bit  3

/** ldlm_lock.l_fl_ast_sent */
#define LDLM_FL_AST_SENT             0x0000000000000020ULL // bit  5

/** ldlm_lock.l_fl_replay */
#define LDLM_FL_REPLAY               0x0000000000000100ULL // bit  8

/** ldlm_lock.l_fl_intent_only */
#define LDLM_FL_INTENT_ONLY          0x0000000000000200ULL // bit  9

/** ldlm_lock.l_fl_has_intent */
#define LDLM_FL_HAS_INTENT           0x0000000000001000ULL // bit 12

/** ldlm_lock.l_fl_discard_data */
#define LDLM_FL_DISCARD_DATA         0x0000000000010000ULL // bit 16

/** ldlm_lock.l_fl_no_timeout */
#define LDLM_FL_NO_TIMEOUT           0x0000000000020000ULL // bit 17

/** ldlm_lock.l_fl_block_nowait */
#define LDLM_FL_BLOCK_NOWAIT         0x0000000000040000ULL // bit 18

/** ldlm_lock.l_fl_test_lock */
#define LDLM_FL_TEST_LOCK            0x0000000000080000ULL // bit 19

/** ldlm_lock.l_fl_cancel_on_block */
#define LDLM_FL_CANCEL_ON_BLOCK      0x0000000000800000ULL // bit 23

/** ldlm_lock.l_fl_deny_on_contention */
#define LDLM_FL_DENY_ON_CONTENTION   0x0000000040000000ULL // bit 30

/** ldlm_lock.l_fl_ast_discard_data */
#define LDLM_FL_AST_DISCARD_DATA     0x0000000080000000ULL // bit 31

/** ldlm_lock.l_fl_fail_loc */
#define LDLM_FL_FAIL_LOC             0x0000000100000000ULL // bit 32

/** ldlm_lock.l_fl_skipped */
#define LDLM_FL_SKIPPED              0x0000000200000000ULL // bit 33

/** ldlm_lock.l_fl_cbpending */
#define LDLM_FL_CBPENDING            0x0000000400000000ULL // bit 34

/** ldlm_lock.l_fl_wait_noreproc */
#define LDLM_FL_WAIT_NOREPROC        0x0000000800000000ULL // bit 35

/** ldlm_lock.l_fl_cancel */
#define LDLM_FL_CANCEL               0x0000001000000000ULL // bit 36

/** ldlm_lock.l_fl_local_only */
#define LDLM_FL_LOCAL_ONLY           0x0000002000000000ULL // bit 37

/** ldlm_lock.l_fl_failed */
#define LDLM_FL_FAILED               0x0000004000000000ULL // bit 38

/** ldlm_lock.l_fl_canceling */
#define LDLM_FL_CANCELING            0x0000008000000000ULL // bit 39

/** ldlm_lock.l_fl_local */
#define LDLM_FL_LOCAL                0x0000010000000000ULL // bit 40

/** ldlm_lock.l_fl_lvb_ready */
#define LDLM_FL_LVB_READY            0x0000020000000000ULL // bit 41

/** ldlm_lock.l_fl_kms_ignore */
#define LDLM_FL_KMS_IGNORE           0x0000040000000000ULL // bit 42

/** ldlm_lock.l_fl_cp_reqd */
#define LDLM_FL_CP_REQD              0x0000080000000000ULL // bit 43

/** ldlm_lock.l_fl_cleaned */
#define LDLM_FL_CLEANED              0x0000100000000000ULL // bit 44

/** ldlm_lock.l_fl_atomic_cb */
#define LDLM_FL_ATOMIC_CB            0x0000200000000000ULL // bit 45

/** ldlm_lock.l_fl_bl_ast */
#define LDLM_FL_BL_AST               0x0000400000000000ULL // bit 46

/** ldlm_lock.l_fl_bl_done */
#define LDLM_FL_BL_DONE              0x0000800000000000ULL // bit 47

/** ldlm_lock.l_fl_no_lru */
#define LDLM_FL_NO_LRU               0x0001000000000000ULL // bit 48

/** ldlm_lock.l_destroyed */
#define LDLM_FL_DESTROYED            0x0002000000000000ULL // bit 49

/** ldlm_lock.l_SERVER_LOCK */
#define LDLM_FL_SERVER_LOCK          0x0004000000000000ULL // bit 50

/** ldlm_lock.l_res_locked */
#define LDLM_FL_RES_LOCKED           0x0008000000000000ULL // bit 51

/** ldlm_lock.l_waited */
#define LDLM_FL_WAITED               0x0010000000000000ULL // bit 52

/** ldlm_lock.l_ns_srv */
#define LDLM_FL_NS_SRV               0x0020000000000000ULL // bit 53

#define LDLM_FL_ALL_FLAGS_MASK       0x003FFFFFC08F132FULL

#define LDLM_FL_AST_MASK             0x0000000080000000ULL
#define LDLM_FL_INHERIT_MASK         0x0000000000800000ULL
#define LDLM_FL_LOCAL_ONLY_MASK      0x003FFFFF00000000ULL
#define LDLM_FL_ON_WIRE_MASK         0x00000000C08F132FULL

/** Mask of flags inherited from parent lock when doing intents. */
#define LDLM_INHERIT_FLAGS           LDLM_FL_INHERIT_MASK
/** Mask of Flags sent in AST lock_flags to map into the receiving lock. */
#define LDLM_AST_FLAGS               LDLM_FL_AST_MASK
#endif /* NEED_LDLM_LOCK_DEFINES */

#ifdef  NEED_LDLM_LOCK_FIELDS
#define HAVE_LDLM_LOCK_FIELDS
#undef  NEED_LDLM_LOCK_FIELDS

	union {
		__u64	l_flags;
		struct {
			__u64	/**
				 * extent, mode, or resource changed */
				l_fl_lock_changed        : 1,

				/**
				 * Server placed lock on granted list, or a
				 * recovering client wants the lock added to
				 * the granted list, no questions asked. */
				l_fl_block_granted       : 1,

				/**
				 * Server placed lock on conv list, or a
				 * recovering client wants the lock added to
				 * the conv list, no questions asked. */
				l_fl_block_conv          : 1,

				/**
				 * Server placed lock on wait list, or a
				 * recovering client wants the lock added to
				 * the wait list, no questions asked. */
				l_fl_block_wait          : 1,

				/** filler bit 4 */
				l_fl_filler_4 : 1,

				/**
				 * blocking or cancel packet was queued for
				 * sending. */
				l_fl_ast_sent            : 1,

				/** filler bit 6 */
				l_fl_filler_6 : 1,

				/** filler bit 7 */
				l_fl_filler_7 : 1,

				/**
				 * Lock is being replayed.  This could probably
				 * be implied by the fact that one of
				 * BLOCK_{GRANTED,CONV,WAIT} is set, but that
				 * is pretty dangerous. */
				l_fl_replay              : 1,

				/**
				 * Don't grant lock, just do intent. */
				l_fl_intent_only         : 1,

				/** filler bit 10 */
				l_fl_filler_10 : 1,

				/** filler bit 11 */
				l_fl_filler_11 : 1,

				/**
				 * lock request has intent */
				l_fl_has_intent          : 1,

				/** filler bit 13 */
				l_fl_filler_13 : 1,

				/** filler bit 14 */
				l_fl_filler_14 : 1,

				/** filler bit 15 */
				l_fl_filler_15 : 1,

				/**
				 * discard (no writeback) on cancel */
				l_fl_discard_data        : 1,

				/**
				 * Blocked by group lock - wait indefinitely */
				l_fl_no_timeout          : 1,

				/**
				 * Server told not to wait if blocked.  For
				 * AGL, OST will not send glimpse callback. */
				l_fl_block_nowait        : 1,

				/**
				 * return blocking lock */
				l_fl_test_lock           : 1,

				/** filler bit 20 */
				l_fl_filler_20 : 1,

				/** filler bit 21 */
				l_fl_filler_21 : 1,

				/** filler bit 22 */
				l_fl_filler_22 : 1,

				/**
				 * Immediatelly cancel such locks when they
				 * block some other locks.  Send cancel
				 * notification to original lock holder, but
				 * expect no reply.  This is for clients (like
				 * liblustre) that cannot be expected to
				 * reliably response to blocking AST. */
				l_fl_cancel_on_block     : 1,

				/** filler bit 24 */
				l_fl_filler_24 : 1,

				/** filler bit 25 */
				l_fl_filler_25 : 1,

				/** filler bit 26 */
				l_fl_filler_26 : 1,

				/** filler bit 27 */
				l_fl_filler_27 : 1,

				/** filler bit 28 */
				l_fl_filler_28 : 1,

				/** filler bit 29 */
				l_fl_filler_29 : 1,

				/**
				 * measure lock contention and return -EUSERS
				 * if locking contention is high */
				l_fl_deny_on_contention  : 1,

				/**
				 * These are flags that are mapped into the
				 * flags and ASTs of blocking locks Add
				 * FL_DISCARD to blocking ASTs */
				l_fl_ast_discard_data    : 1,

				/**
				 * Used for marking lock as a target for -EINTR
				 * while cp_ast sleep emulation + race with
				 * upcoming bl_ast. */
				l_fl_fail_loc            : 1,

				/**
				 * Used while processing the unused list to
				 * know that we have already handled this lock
				 * and decided to skip it. */
				l_fl_skipped             : 1,

				/**
				 * this lock is being destroyed */
				l_fl_cbpending           : 1,

				/**
				 * not a real flag, not saved in lock */
				l_fl_wait_noreproc       : 1,

				/**
				 * cancellation callback already run */
				l_fl_cancel              : 1,

				/**
				 * whatever */
				l_fl_local_only          : 1,

				/**
				 * don't run the cancel callback under
				 * ldlm_cli_cancel_unused */
				l_fl_failed              : 1,

				/**
				 * lock cancel has already been sent */
				l_fl_canceling           : 1,

				/**
				 * local lock (ie, no srv/cli split) */
				l_fl_local               : 1,

				/**
				 * XXX FIXME: This is being added to b_size as
				 * a low-risk fix to the fact that the LVB
				 * filling happens _after_ the lock has been
				 * granted, so another thread can match it
				 * before the LVB has been updated.  As a dirty
				 * hack, we set LDLM_FL_LVB_READY only after
				 * we've done the LVB poop.  this is only
				 * needed on LOV/OSC now, where LVB is actually
				 * used and callers must set it in input flags.
				 * 
				 * The proper fix is to do the granting inside
				 * of the completion AST, which can be replaced
				 * with a LVB-aware wrapping function for OSC
				 * locks.  That change is pretty high-risk,
				 * though, and would need a lot more testing. */
				l_fl_lvb_ready           : 1,

				/**
				 * A lock contributes to the known minimum size
				 * (KMS) calculation until it has finished the
				 * part of its cancelation that performs write
				 * back on its dirty pages.  It can remain on
				 * the granted list during this whole time.
				 * Threads racing to update the KMS after
				 * performing their writeback need to know to
				 * exclude each other's locks from the
				 * calculation as they walk the granted list. */
				l_fl_kms_ignore          : 1,

				/**
				 * completion AST to be executed */
				l_fl_cp_reqd             : 1,

				/**
				 * cleanup_resource has already handled the
				 * lock */
				l_fl_cleaned             : 1,

				/**
				 * optimization hint: LDLM can run blocking
				 * callback from current context w/o involving
				 * separate thread.  in order to decrease cs
				 * rate */
				l_fl_atomic_cb           : 1,

				/**
				 * It may happen that a client initiates two
				 * operations, e.g.  unlink and mkdir, such
				 * that the server sends a blocking AST for
				 * conflicting locks to this client for the
				 * first operation, whereas the second
				 * operation has canceled this lock and is
				 * waiting for rpc_lock which is taken by the
				 * first operation.  LDLM_FL_BL_AST is set by
				 * ldlm_callback_handler() in the lock to
				 * prevent the Early Lock Cancel (ELC) code
				 * from cancelling it.
				 * 
				 * LDLM_FL_BL_DONE is to be set by
				 * ldlm_cancel_callback() when lock cache is
				 * dropped to let ldlm_callback_handler()
				 * return EINVAL to the server.  It is used
				 * when ELC RPC is already prepared and is
				 * waiting for rpc_lock, too late to send a
				 * separate CANCEL RPC. */
				l_fl_bl_ast              : 1,

				/**
				 * whatever */
				l_fl_bl_done             : 1,

				/**
				 * Don't put lock into the LRU list, so that it
				 * is not canceled due to aging.  Used by MGC
				 * locks, they are cancelled only at unmount or
				 * by callback. */
				l_fl_no_lru              : 1,

				/**
				 * Set for locks that were removed from class
				 * hash table and will be destroyed when last
				 * reference to them is released.  Set by
				 * ldlm_lock_destroy_internal().
				 * 
				 * Protected by lock and resource locks. */
				l_destroyed           : 1,

				/**
				 * flag whether this is a server namespace lock */
				l_server_lock         : 1,

				/**
				 * it's set in lock_res_and_lock() and unset in
				 * unlock_res_and_lock().
				 * 
				 * NB: compared with check_res_locked(),
				 * checking this bit is cheaper.  Also,
				 * spin_is_locked() is deprecated for kernel
				 * code; one reason is because it works only
				 * for SMP so user needs to add extra macros
				 * like LASSERT_SPIN_LOCKED for uniprocessor
				 * kernels. */
				l_res_locked          : 1,

				/**
				 * It's set once we call
				 * ldlm_add_waiting_lock_res_locked() to start
				 * the lock-timeout timer and it will never be
				 * reset.
				 * 
				 * Protected by lock_res_and_lock(). */
				l_waited              : 1,

				/**
				 * Flag whether this is a server namespace
				 * lock. */
				l_ns_srv              : 1;
		};
	};
#endif /* NEED_LDLM_LOCK_FIELDS */

#else /* CRASH_EXTENSION */
#include "defs.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
static char const * const fill_fmts[] = {
    "%8s",  "%7s",  "%10s", "%10s", "%11s", "%12s", "%11s", "%11s", "%14s",
    "%9s",  "%10s", "%10s", "%10s", "%10s", "%10s", "%10s", "%8s",  "%10s",
    "%8s",  "%11s", "%10s", "%10s", "%10s", "%5s",  "%10s", "%10s", "%10s",
    "%10s", "%10s", "%10s", "%2s",  "%4s",  "%12s", "%13s", "%11s", "%7s",
    "%14s", "%10s", "%14s", "%11s", "%15s", "%11s", "%10s", "%13s", "%13s",
    "%11s", "%14s", "%13s", "%14s", "%11s", "%9s",  "%10s", "%14s", "%14s"; };

static char const * const flag_names[] = {
    "LOCK_CHANGED",       "BLOCK_GRANTED",      "BLOCK_CONV",
    "BLOCK_WAIT",         "INVALID-4",          "AST_SENT",
    "INVALID-6",          "INVALID-7",          "REPLAY",
    "INTENT_ONLY",        "INVALID-10",         "INVALID-11",
    "HAS_INTENT",         "INVALID-13",         "INVALID-14",
    "INVALID-15",         "DISCARD_DATA",       "NO_TIMEOUT",
    "BLOCK_NOWAIT",       "TEST_LOCK",          "INVALID-20",
    "INVALID-21",         "INVALID-22",         "CANCEL_ON_BLOCK",
    "INVALID-24",         "INVALID-25",         "INVALID-26",
    "INVALID-27",         "INVALID-28",         "INVALID-29",
    "DENY_ON_CONTENTION", "AST_DISCARD_DATA",   "FAIL_LOC",
    "SKIPPED",            "CBPENDING",          "WAIT_NOREPROC",
    "CANCEL",             "LOCAL_ONLY",         "FAILED",
    "CANCELING",          "LOCAL",              "LVB_READY",
    "KMS_IGNORE",         "CP_REQD",            "CLEANED",
    "ATOMIC_CB",          "BL_AST",             "BL_DONE",
    "NO_LRU",             "DESTROYED",          "SERVER_LOCK",
    "RES_LOCKED",         "WAITED",             "NS_SRV"; };

static void
print_bits(unsigned long long v)
{
    static char const new_line[] = "\n";
    char const * space_fmt = new_line + 1;
    int ix = 0;
    int ct = 0;

    if ((v & ~LDLM_FL_ALL_FLAGS_MASK) != 0) {
        printf("undefined bits: 0x%016llX\n", v & ~LDLM_FL_ALL_FLAGS_MASK);
        v &= LDLM_FL_ALL_FLAGS_MASK;
    }

    for (ix = 0; v != 0ULL; ix++, v >>= 1) {
        if ((v & 0x1ULL) == 0)
            continue;

        printf(space_fmt, "");
        if ((++ct & 0x03) == 0)
            space_fmt = new_line;
        else
            space_fmt = fill_fmts[ix];
        fputs(flag_names[ix], stdout);
    }
}

void
cmd_ldlm_lock_flags(void)
{
    char * p = args[1];
    char * e;
    unsigned long long v;
    if (p == NULL) {
        printf("no argument\n");
        return;
    }

    v = strtoull(p, &e, 0);
    if (*e != '\0') {
        errno = 0;
        v = strtoull(p, &e, 16);
        if ((errno != 0) || (*e != '\0')) {
            printf("invalid number: %s\n", p);
            return;
        }
    }

    print_bits(v);
}

char * help_ldlm_lock_flags[] = {
    "ldlm_lock_flags",
    "flag bit names for ldlm_lock",
    "<numeric-value>",
    "The names of the bits that are set in the numeric value are printed.",
    NULL
};
#endif /* CRASH_EXTENSION */

