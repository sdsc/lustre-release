/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#define DEBUG_SUBSYSTEM S_LNET

#include "selftest.h"


enum {
	LST_INIT_NONE		= 0,
	LST_INIT_WI,
	LST_INIT_RPC,
	LST_INIT_FW,
	LST_INIT_CONSOLE
};

extern int lstcon_console_init(void);
extern int lstcon_console_fini(void);

static int lst_init_step = LST_INIT_NONE;

struct cfs_wi_sched *lst_sched_serial;
struct cfs_wi_sched *lst_sched_test;

void
lnet_selftest_fini (void)
{
        switch (lst_init_step) {
#ifdef __KERNEL__
                case LST_INIT_CONSOLE:
                        lstcon_console_fini();
#endif
                case LST_INIT_FW:
                        sfw_shutdown();
                case LST_INIT_RPC:
                        srpc_shutdown();
		case LST_INIT_WI:
			if (lst_sched_test != NULL) {
				cfs_wi_sched_destroy(lst_sched_test);
				lst_sched_test = NULL;
			}

			if (lst_sched_serial != NULL) {
				cfs_wi_sched_destroy(lst_sched_serial);
				lst_sched_serial = NULL;
			}
                case LST_INIT_NONE:
                        break;
                default:
                        LBUG();
        }
        return;
}


void
lnet_selftest_structure_assertion(void)
{
        CLASSERT(sizeof(srpc_msg_t) == 160);
        CLASSERT(sizeof(srpc_test_reqst_t) == 70);
        CLASSERT(offsetof(srpc_msg_t, msg_body.tes_reqst.tsr_concur) == 72);
        CLASSERT(offsetof(srpc_msg_t, msg_body.tes_reqst.tsr_ndest) == 78);
        CLASSERT(sizeof(srpc_stat_reply_t) == 136);
        CLASSERT(sizeof(srpc_stat_reqst_t) == 28);
}

int
lnet_selftest_init (void)
{
	int	nthrs;
        int	rc;

	rc = cfs_wi_sched_create("lst_s", lnet_cpt_table(), CFS_CPT_ANY,
				 1, &lst_sched_serial);
	if (rc != 0)
		return rc;

	lst_init_step = LST_INIT_WI;
	nthrs = cfs_cpt_weight(lnet_cpt_table(), CFS_CPT_ANY);

	rc = cfs_wi_sched_create("lst_t", lnet_cpt_table(), CFS_CPT_ANY,
				 nthrs, &lst_sched_test);
	if (rc != 0)
		goto error;

        rc = srpc_startup();
        if (rc != 0) {
                CERROR("LST can't startup rpc\n");
                goto error;
        }
        lst_init_step = LST_INIT_RPC;

        rc = sfw_startup();
        if (rc != 0) {
                CERROR("LST can't startup framework\n");
                goto error;
        }
        lst_init_step = LST_INIT_FW;

#ifdef __KERNEL__
        rc = lstcon_console_init();
        if (rc != 0) {
                CERROR("LST can't startup console\n");
                goto error;
        }
        lst_init_step = LST_INIT_CONSOLE;  
#endif

        return 0;
error:
        lnet_selftest_fini();
        return rc;
}

#ifdef __KERNEL__

MODULE_DESCRIPTION("LNet Selftest");
MODULE_LICENSE("GPL");

cfs_module(lnet, "0.9.0", lnet_selftest_init, lnet_selftest_fini);

#else

int
selftest_wait_events (void)
{
        int evts = 0;

        for (;;) {
                /* Consume all pending events */
                while (srpc_check_event(0))
                        evts++;
                evts += stt_check_events();
                evts += swi_check_events();
                if (evts != 0) break;

                /* Nothing happened, block for events */
                evts += srpc_check_event(stt_poll_interval());
                /* We may have blocked, check for expired timers */
                evts += stt_check_events();
                if (evts == 0) /* timed out and still no event */
                        break;
        }

        return evts;
}

#endif
