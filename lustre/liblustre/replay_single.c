/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 *
 * Lustre Light user test program
 *
 *  Copyright (c) 2002, 2003 Cluster File Systems, Inc.
 *
 *   This file is part of Lustre, http://www.lustre.org.
 *
 *   Lustre is free software; you can redistribute it and/or
 *   modify it under the terms of version 2 of the GNU General Public
 *   License as published by the Free Software Foundation.
 *
 *   Lustre is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Lustre; if not, write to the Free Software
 *   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#define _BSD_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/queue.h>
#include <signal.h>

#include <sysio.h>
#include <mount.h>

#include "test_common.h"



static char mds_server[1024] = {0,};
static char barrier_script[1024] = {0,};
static char failover_script[1024] = {0,};
static char barrier_cmd[1024] = {0,};
static char failover_cmd[1024] = {0,};

static void replay_barrier()
{
        int rc;

        if ((rc = system(barrier_cmd))) {
                printf("excute barrier error: %d\n", rc);
                exit(rc);
        }
}

static void mds_failover()
{
        int rc;

        if ((rc = system(failover_cmd))) {
                printf("excute failover error: %d\n", rc);
                exit(rc);
        }
}


#define ENTRY(str)                                                      \
        do {                                                            \
                char buf[100];                                          \
                int len;                                                \
                sprintf(buf, "===== START: %s ", (str));                \
                len = strlen(buf);                                      \
                if (len < 79) {                                         \
                        memset(buf+len, '=', 100-len);                  \
                        buf[79] = '\n';                                 \
                        buf[80] = 0;                                    \
                }                                                       \
                printf("%s", buf);                                      \
        } while (0)

#define LEAVE()                                                         \
        do {                                                            \
                printf("----- END TEST successfully ---");              \
                printf("-----------------------------");                \
                printf("-------------------\n");                        \
        } while (0)

void t0()
{
        ENTRY("empty replay");
        replay_barrier();
        mds_failover();
        LEAVE();
}

void t1()
{
        char *path="/mnt/lustre/f1";
        ENTRY("simple create");

        replay_barrier();
        t_create(path);
        mds_failover();
        t_check_stat(path, NULL);
        t_unlink(path);
        LEAVE();
}

void t2a()
{
        char *path="/mnt/lustre/f2a";
        ENTRY("touch");

        replay_barrier();
        t_touch(path);
        mds_failover();
        t_check_stat(path, NULL);
        t_unlink(path);
        LEAVE();
}

void t2b()
{
        char *path="/mnt/lustre/f2b";
        ENTRY("mcreate+touch");

        t_create(path);
        replay_barrier();
        t_touch(path);
        mds_failover();
        t_check_stat(path, NULL);
        t_unlink(path);
        LEAVE();
}


void n_create_delete(int nfiles)
{
        char *base="/mnt/lustre/f3_";
        char path[100];
        char str[100];
        int i;

        replay_barrier();
        for (i = 0; i < nfiles; i++) {
                sprintf(path, "%s%d\n", base, i);
                sprintf(str, "TEST#%d CONTENT\n", i);
                t_echo_create(path, str);
        }
        mds_failover();
        for (i = 0; i < nfiles; i++) {
                sprintf(path, "%s%d\n", base, i);
                sprintf(str, "TEST#%d CONTENT\n", i);
                t_grep(path, str);
        }
        replay_barrier();
        for (i = 0; i < nfiles; i++) {
                sprintf(path, "%s%d\n", base, i);
                t_unlink(path);
        }
        mds_failover();
        for (i = 0; i < nfiles; i++) {
                sprintf(path, "%s%d\n", base, i);
                t_check_stat_fail(path);
        }
        LEAVE();
}

void t3a()
{
        ENTRY("10 create/delete");
        n_create_delete(10);
        LEAVE();
}

void t3b()
{
        ENTRY("60 create/delete(>1'st block precreated)");
        n_create_delete(60);
        LEAVE();
}

void t4()
{
        char *dir="/mnt/lustre/d4";
        char *path="/mnt/lustre/d4/f1";
        ENTRY("mkdir + contained create");

        replay_barrier();
        t_mkdir(dir);
        t_create(path);
        mds_failover();
        t_check_stat(dir, NULL);
        t_check_stat(path, NULL);
        sleep(2); /* wait for log process thread */

        replay_barrier();
        t_unlink(path);
        t_rmdir(dir);
        mds_failover();
        t_check_stat_fail(dir);
        t_check_stat_fail(path);
        LEAVE();
}

void t5()
{
        char *dir="/mnt/lustre/d5";
        char *path="/mnt/lustre/d5/f1";
        ENTRY("mkdir |X| contained create");

        t_mkdir(dir);
        replay_barrier();
        t_create(path);
        mds_failover();
        t_check_stat(dir, NULL);
        t_check_stat(path, NULL);
        t_unlink(path);
        t_rmdir(dir);
        LEAVE();
}

void t6()
{
        char *path="/mnt/lustre/f6";
        int fd;
        ENTRY("open |X| close");

        replay_barrier();
        t_create(path);
        fd = t_open(path);
        sleep(1);
        mds_failover();
        t_check_stat(path, NULL);
        t_close(fd);
        t_unlink(path);
        LEAVE();
}

void t7()
{
        char *path="/mnt/lustre/f7";
        char *path2="/mnt/lustre/f7-2";
        ENTRY("create |X| rename unlink");

        t_create(path);
        replay_barrier();
        t_rename(path, path2);
        mds_failover();
        t_check_stat_fail(path);
        t_check_stat(path2, NULL);
        t_unlink(path2);
}

void t8()
{
        char *path="/mnt/lustre/f8";
        char *path2="/mnt/lustre/f8-2";
        ENTRY("create open write rename |X| create-old-name read");

        t_create(path);
        t_echo_create(path, "old");
        t_rename(path, path2);
        replay_barrier();
        t_echo_create(path, "new");
        mds_failover();
        t_grep(path, "new");
        t_grep(path2, "old");
        t_unlink(path);
        t_unlink(path2);
}

void t9()
{
        char *path="/mnt/lustre/f9";
        char *path2="/mnt/lustre/f9-2";
        ENTRY("|X| open(O_CREAT), unlink, touch new, unlink new");

        replay_barrier();
        t_create(path);
        t_unlink(path);
        t_create(path2);
        mds_failover();
        t_check_stat_fail(path);
        t_check_stat(path2, NULL);
        t_unlink(path2);
}

void t10()
{
        char *path="/mnt/lustre/f10";
        char *path2="/mnt/lustre/f10-2";
        ENTRY("|X| mcreate, open write, rename");

        replay_barrier();
        t_create(path);
        t_echo_create(path, "old");
        t_rename(path, path2);
        t_grep(path2, "old");
        mds_failover();
        t_grep(path2, "old");
        t_unlink(path2);
}

extern int portal_debug;
extern int portal_subsystem_debug;

extern void __liblustre_setup_(void);
extern void __liblustre_cleanup_(void);

void usage(const char *cmd)
{
        printf("Usage: \t%s --target mdsnid:/mdsname/profile -s mds_hostname "
                "-b \"barrier cmd\" -f \"failover cmd\"\n", cmd);
        printf("       \t%s --dumpfile dumpfile -s mds_hostname -b \"barrier cmd\" "
                "-f \"failover cmd\"\n", cmd);
        exit(-1);
}

void test_ssh()
{
        char cmd[1024];

        sprintf(cmd, "ssh %s cat /dev/null", mds_server);
        if (system(cmd)) {
                printf("ssh can't access server node: %s\n", mds_server);
                exit(-1);
        }
}

int main(int argc, char * const argv[])
{
        int opt_index, c;
        static struct option long_opts[] = {
                {"target", 1, 0, 0},
                {"dumpfile", 1, 0, 0},
                {0, 0, 0, 0}
        };

        if (argc < 4)
                usage(argv[0]);

        while ((c = getopt_long(argc, argv, "s:b:f:", long_opts, &opt_index)) != -1) {
                switch (c) {
                case 0: {
                        if (!optarg[0])
                                usage(argv[0]);

                        if (!strcmp(long_opts[opt_index].name, "target")) {
                                setenv(ENV_LUSTRE_MNTTGT, optarg, 1);
                        } else if (!strcmp(long_opts[opt_index].name, "dumpfile")) {
                                setenv(ENV_LUSTRE_DUMPFILE, optarg, 1);
                        } else
                                usage(argv[0]);
                        break;
                }
                case 's':
                        strcpy(mds_server, optarg);
                        break;
                case 'b':
                        strcpy(barrier_script, optarg);
                        break;
                case 'f':
                        strcpy(failover_script, optarg);
                        break;
                default:
                        usage(argv[0]);
                }
        }

        if (optind != argc)
                usage(argv[0]);
        if (!strlen(mds_server) || !strlen(barrier_script) ||
            !strlen(failover_script))
                usage(argv[0]);

        test_ssh();

        /* prepare remote command */
        sprintf(barrier_cmd, "ssh %s \"%s\"", mds_server, barrier_script);
        sprintf(failover_cmd, "ssh %s \"%s\"", mds_server, failover_script);

        __liblustre_setup_();

        t0();
        t1();
        t2a();
        t2b();
        t3a();
        t3b();
        t4();
        t5();
        t6();
        t7();
        t8();
        t9();
        t10();

	printf("liblustre is about shutdown\n");
        __liblustre_cleanup_();

	printf("complete successfully\n");
	return 0;
}
