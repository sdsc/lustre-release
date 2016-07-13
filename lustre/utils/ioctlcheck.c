/*
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
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2016, Intel Corporation.
 */

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "const_check.h"

#include <uapi_kernelcomm.h>
#include <lustre_ioctl.h>

static void check_ioctl_fsfilt(void)
{
	BLANK_LINE();
	COMMENT("FSFILT IOCTL");
	CHECK_VALUE_X(FSFILT_IOC_GETFLAGS);
	CHECK_VALUE_X(FSFILT_IOC_SETFLAGS);
	CHECK_VALUE_X(FSFILT_IOC_GETVERSION);
	CHECK_VALUE_X(FSFILT_IOC_SETVERSION);
	CHECK_VALUE_X(FSFILT_IOC_GETVERSION_OLD);
	CHECK_VALUE_X(FSFILT_IOC_SETVERSION_OLD);
}

static void check_ioctl_obd(void)
{
	BLANK_LINE();
	COMMENT("OBD IOCTL");
	CHECK_VALUE_X(OBD_IOC_CREATE);
	CHECK_VALUE_X(OBD_IOC_DESTROY);
	CHECK_VALUE_X(OBD_IOC_SETATTR);
	CHECK_VALUE_X(OBD_IOC_GETATTR);
	CHECK_VALUE_X(OBD_IOC_READ);
	CHECK_VALUE_X(OBD_IOC_WRITE);
	CHECK_VALUE_X(OBD_IOC_STATFS);
	CHECK_VALUE_X(OBD_IOC_SYNC);
	CHECK_VALUE_X(OBD_IOC_BRW_READ);
	CHECK_VALUE_X(OBD_IOC_BRW_WRITE);
	CHECK_VALUE_X(OBD_IOC_NAME2DEV);
	CHECK_VALUE_X(OBD_IOC_UUID2DEV);
	CHECK_VALUE_X(OBD_IOC_GETNAME);
	CHECK_VALUE_X(OBD_IOC_GETMDNAME);
	CHECK_VALUE_X(OBD_IOC_GETDTNAME);
	CHECK_VALUE_X(OBD_IOC_LOV_GET_CONFIG);
	CHECK_VALUE_X(OBD_IOC_CLIENT_RECOVER);
	CHECK_VALUE_X(OBD_IOC_PING_TARGET);
	CHECK_VALUE_X(OBD_IOC_NO_TRANSNO);
	CHECK_VALUE_X(OBD_IOC_SET_READONLY);
	CHECK_VALUE_X(OBD_IOC_ABORT_RECOVERY);
	CHECK_VALUE_X(OBD_GET_VERSION);
	CHECK_VALUE_X(OBD_IOC_CHANGELOG_SEND);
	CHECK_VALUE_X(OBD_IOC_GETDEVICE);
	CHECK_VALUE_X(OBD_IOC_FID2PATH);

	CHECK_VALUE_X(OBD_IOC_QUOTACTL);

	CHECK_VALUE_X(OBD_IOC_CHANGELOG_REG);
	CHECK_VALUE_X(OBD_IOC_CHANGELOG_DEREG);
	CHECK_VALUE_X(OBD_IOC_CHANGELOG_CLEAR);
	CHECK_VALUE_X(OBD_IOC_PROCESS_CFG);
	CHECK_VALUE_X(OBD_IOC_PARAM);
	CHECK_VALUE_X(OBD_IOC_POOL);
	CHECK_VALUE_X(OBD_IOC_REPLACE_NIDS);
	CHECK_VALUE_X(OBD_IOC_CATLOGLIST);
	CHECK_VALUE_X(OBD_IOC_LLOG_INFO);
	CHECK_VALUE_X(OBD_IOC_LLOG_PRINT);
	CHECK_VALUE_X(OBD_IOC_LLOG_CANCEL);
	CHECK_VALUE_X(OBD_IOC_LLOG_REMOVE);
	CHECK_VALUE_X(OBD_IOC_LLOG_CHECK);
	CHECK_VALUE_X(OBD_IOC_NODEMAP);
	CHECK_VALUE_X(OBD_IOC_GET_OBJ_VERSION);
	/* CHECK_VALUE_X(OBD_IOC_GET_MNTOPT); tobe removed */
	CHECK_VALUE_X(OBD_IOC_ECHO_MD);
	CHECK_VALUE_X(OBD_IOC_ECHO_ALLOC_SEQ);
	CHECK_VALUE_X(OBD_IOC_START_LFSCK);
	CHECK_VALUE_X(OBD_IOC_STOP_LFSCK);
	CHECK_VALUE_X(OBD_IOC_QUERY_LFSCK);
}

static void check_ioctl_ll(void)
{
	BLANK_LINE();
	COMMENT("LL IOCTL");
	CHECK_VALUE_X(LL_IOC_GETFLAGS);
	CHECK_VALUE_X(LL_IOC_SETFLAGS);
	CHECK_VALUE_X(LL_IOC_CLRFLAGS);
	CHECK_VALUE_X(LL_IOC_LOV_SETSTRIPE);
	CHECK_VALUE_X(LL_IOC_LOV_GETSTRIPE);
	CHECK_VALUE_X(LL_IOC_LOV_SETEA);
	CHECK_VALUE_X(LL_IOC_GROUP_LOCK);
	CHECK_VALUE_X(LL_IOC_GROUP_UNLOCK);
	CHECK_VALUE_X(LL_IOC_FLUSHCTX);
	CHECK_VALUE_X(LL_IOC_GETOBDCOUNT);
	CHECK_VALUE_X(LL_IOC_LLOOP_ATTACH);
	CHECK_VALUE_X(LL_IOC_LLOOP_DETACH);
	CHECK_VALUE_X(LL_IOC_LLOOP_INFO);
	CHECK_VALUE_X(LL_IOC_LLOOP_DETACH_BYDEV);
	CHECK_VALUE_X(LL_IOC_PATH2FID);
	CHECK_VALUE_X(LL_IOC_GET_CONNECT_FLAGS);
	CHECK_VALUE_X(LL_IOC_GET_MDTIDX);
	CHECK_VALUE_X(LL_IOC_FUTIMES_3);

	CHECK_VALUE_X(LL_IOC_HSM_STATE_GET);
	CHECK_VALUE_X(LL_IOC_HSM_STATE_SET);
	CHECK_VALUE_X(LL_IOC_HSM_CT_START);
	CHECK_VALUE_X(LL_IOC_HSM_COPY_START);
	CHECK_VALUE_X(LL_IOC_HSM_COPY_END);
	CHECK_VALUE_X(LL_IOC_HSM_PROGRESS);
	CHECK_VALUE_X(LL_IOC_HSM_REQUEST);
	CHECK_VALUE_X(LL_IOC_DATA_VERSION);
	CHECK_VALUE_X(LL_IOC_LOV_SWAP_LAYOUTS);
	CHECK_VALUE_X(LL_IOC_HSM_ACTION);

	CHECK_VALUE_X(LL_IOC_LMV_SETSTRIPE);
	CHECK_VALUE_X(LL_IOC_LMV_GETSTRIPE);
	CHECK_VALUE_X(LL_IOC_REMOVE_ENTRY);
	CHECK_VALUE_X(LL_IOC_SET_LEASE);
	CHECK_VALUE_X(LL_IOC_GET_LEASE);
	CHECK_VALUE_X(LL_IOC_HSM_IMPORT);
	CHECK_VALUE_X(LL_IOC_LMV_SET_DEFAULT_STRIPE);
	CHECK_VALUE_X(LL_IOC_MIGRATE);
	CHECK_VALUE_X(LL_IOC_FID2MDTIDX);
	CHECK_VALUE_X(LL_IOC_GETPARENT);
	CHECK_VALUE_X(LL_IOC_LADVISE);
	CHECK_VALUE_X(LL_IOC_MDC_GETINFO);
}

static void system_string(char *cmdline, char *str, int len)
{
	int   fds[2];
	int   rc;
	pid_t pid;

	rc = pipe(fds);
	if (rc != 0)
		abort();

	pid = fork();
	if (pid == 0) {
		/* child */
		int   fd = fileno(stdout);

		rc = dup2(fds[1], fd);
		if (rc != fd)
			abort();

		exit(system(cmdline));
		/* notreached */
	} else if ((int)pid < 0) {
		abort();
	} else {
		FILE *f = fdopen(fds[0], "r");

		if (f == NULL)
			abort();

		close(fds[1]);

		if (fgets(str, len, f) == NULL)
			abort();

		if (waitpid(pid, &rc, 0) != pid)
			abort();

		if (!WIFEXITED(rc) || WEXITSTATUS(rc) != 0)
			abort();

		if (strnlen(str, len) == len)
			str[len - 1] = 0;

		if (str[strlen(str) - 1] == '\n' || str[strlen(str) - 1] == ' ')
			str[strlen(str) - 1] = 0;

		fclose(f);
	}
}

int
main(int argc, char **argv)
{
	char unameinfo[80];
	char gccinfo[80];

	system_string("uname -a", unameinfo, sizeof(unameinfo));
	system_string(CC " -v 2>&1 | tail -1", gccinfo, sizeof(gccinfo));

	BLANK_LINE();
	printf("void lustre_assert_ioctl_constants(void)\n"
	       "{\n"
	       "	/* IOCTL assertions generated by 'ioctlcheck'\n"
	       "	 * (make -C lustre/utils newwiretest)\n"
	       "	 * running on %s\n"
	       "	 * with %s */\n"
	       "\n", unameinfo, gccinfo);

	check_ioctl_fsfilt();
	check_ioctl_ll();
	check_ioctl_obd();

	printf("}\n");

	return 0;
}
