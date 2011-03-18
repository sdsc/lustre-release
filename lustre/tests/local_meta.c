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
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 *
 * lustre/tests/many_create.c
 *
 * Author: liang@whamcloud.com
 */
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <getopt.h>
#include <libcfs/libcfs.h>
#include <lustre/lustre_user.h>
#include <lnet/lnetctl.h>
#include <libcfs/libcfsutil.h>
#include <lustre/liblustreapi.h>

#ifndef __USE_GNU
#define __USE_GNU
#endif
#include <sched.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <pthread.h>

#define LMD_DEBUG       (0)
#define MOUNT_POINT     "/mnt/lustre"
#define TEST_DIR        "dir"

#define difftime(a, b)                                          \
        ((double)(a)->tv_sec - (b)->tv_sec +                    \
        ((double)((a)->tv_usec - (b)->tv_usec) / 1000000))

enum {
        LMD_OP_OPENCREATE       = 1,
        LMD_OP_UNLINK           = 7,
};

static char    *lmd_tpath = NULL;
static mode_t   lmd_mode = 0644;
static unsigned lmd_oflags = O_CREAT | O_RDWR;
static unsigned lmd_single_mount = 0;
static unsigned lmd_nlen = 20;
static unsigned lmd_loop = 10;
static unsigned lmd_nthreads = 1;
static unsigned lmd_output_interval = 1000;
static unsigned lmd_output_thread = 0;

struct lmd_shared_data {
        l_mutex_t       ls_mutex;
        l_cond_t        ls_cond;
        struct  timeval ls_start;;
        int             ls_thr_waiting;
        int             ls_thr_starting;
        int             ls_thr_running;
        int             ls_thr_next;
        int             ls_stopping;
        float           ls_times;
        unsigned        ls_iops;
        unsigned        ls_iops2;
};

static char *
lmd_opc_str(int opc)
{
        switch (opc) {
        default:
                return "unknown";
        case LMD_OP_OPENCREATE:
                return "opencreate";
        case LMD_OP_UNLINK:
                return "unlink";
        }
}

static struct lmd_shared_data *lmd_data;

static int lmd_shmem_setup(void)
{
        pthread_mutexattr_t mattr;
        pthread_condattr_t  cattr;
        int     rc;

        /* Create new segment */
        int shmid = shmget(IPC_PRIVATE, sizeof(*lmd_data), 0600);

        if (shmid == -1) {
                fprintf(stderr, "Can't create shared data: %s\n",
                        strerror(errno));
                return -1;
        }

        /* Attatch to new segment */
        lmd_data = (struct lmd_shared_data *)shmat(shmid, NULL, 0);

        if (lmd_data == (struct lmd_shared_data *)(-1)) {
                fprintf(stderr, "Can't attach shared data: %s\n",
                        strerror(errno));
                lmd_data = NULL;
                return -1;
        }

        /* Mark segment as destroyed, so it will disappear when we exit.
         * Forks will inherit attached segments, so we should be OK.
         */
        if (shmctl(shmid, IPC_RMID, NULL) == -1) {
                fprintf(stderr, "Can't destroy shared data: %s\n",
                        strerror(errno));
		return -1;
        }

        memset(lmd_data, 0, sizeof(*lmd_data));
        lmd_data->ls_thr_next = 1;

        pthread_mutexattr_init(&mattr);
        pthread_condattr_init(&cattr);

        rc = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
        if (rc != 0) {
                fprintf(stderr, "Can't set shared mutex attr\n");
                return -1;
        }

        rc = pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
        if (rc != 0) {
                fprintf(stderr, "Can't set shared cond attr\n");
                return -1;
        }

        pthread_mutex_init(&lmd_data->ls_mutex, &mattr);
        pthread_cond_init(&lmd_data->ls_cond, &cattr);

        pthread_mutexattr_destroy(&mattr);
        pthread_condattr_destroy(&cattr);

	return 0;
}

static void lmd_shmem_lock(void)
{
        l_mutex_lock(&lmd_data->ls_mutex);
}

static void lmd_shmem_unlock(void)
{
        l_mutex_unlock(&lmd_data->ls_mutex);
}

static void lmd_shmem_wait(int thread, char *event)
{
        lmd_data->ls_thr_waiting++;
#if LMD_DEBUG
        fprintf(stdout, "thread %d is waiting for %s, total %d waiters\n",
                thread, event, lmd_data->ls_thr_waiting);
#endif

	l_cond_wait(&lmd_data->ls_cond, &lmd_data->ls_mutex);

        lmd_data->ls_thr_waiting--;
#if LMD_DEBUG
        fprintf(stdout, "thread %d is waken up from %s, %d waiters\n",
                thread, event, lmd_data->ls_thr_waiting);
#endif
}

static void lmd_shmem_broadcast(int thread, char *event)
{
#if LMD_DEBUG
        fprintf(stdout, "thread %d is broadcasting %s to %d waters\n",
                thread, event, lmd_data->ls_thr_waiting);
#endif
	l_cond_broadcast(&lmd_data->ls_cond);
}

static int lmd_shmem_stopping(void)
{
        return lmd_data->ls_stopping;
}

static void lmd_shmem_stop(void)
{
        lmd_data->ls_stopping = 1;
}

static void
lmd_run_test(int thread, int opc)
{
        char   *pname;
        char    mypath[1024];
        char    fname[1024];
        struct  timeval start;
        struct  timeval end;
        int     unlink_miss;
        float   diff;
        int     tint;
        int     len;
        int     rc;
        int     i;

        if (lmd_single_mount) {
                sprintf(mypath, "%s/%s", MOUNT_POINT,
                        lmd_tpath == NULL ? TEST_DIR : lmd_tpath);
        } else {
                sprintf(mypath, "%s%d/%s", MOUNT_POINT, thread,
                        lmd_tpath == NULL ? TEST_DIR : lmd_tpath);
	}

        rc = (opc == LMD_OP_OPENCREATE) ? mkdir(mypath, 0755) : 0;
        if (rc != 0 && errno != EEXIST) {
                fprintf(stderr, "Failed to create test dir %s: %d\n",
                        mypath, errno);
                lmd_shmem_stop();
                goto start;
        }

        rc = chdir(mypath);
        if (rc < 0) {
                fprintf(stderr, "thread %d can't chdir to %s\n",
                        thread, mypath);
                lmd_shmem_stop();
                goto start;
        }

#if LMD_DEBUG
        fprintf(stdout, "thread %d %s [%d] in progress...\n",
                thread, lmd_opc_str(opc), lmd_loop);
#endif

 start:
        lmd_shmem_lock();

        lmd_data->ls_thr_starting--;
        lmd_data->ls_thr_running++;

        if (lmd_data->ls_thr_running == lmd_nthreads || lmd_shmem_stopping()) {
                lmd_data->ls_times = 0;
                lmd_shmem_broadcast(thread, "START");
                gettimeofday(&lmd_data->ls_start, NULL);
        } else {
                lmd_shmem_wait(thread, "START");
        }
	lmd_shmem_unlock();

        gettimeofday(&start, NULL);

        pname = fname;

        len = max(2, lmd_nlen - 14); /* 14 is serial number len */
        for (pname = fname, i = 0; i < len; i++, pname++)
                *pname = 'a' + thread % 26;

        unlink_miss = 0;
        tint = lmd_output_thread == 0 ? 0 : lmd_nthreads / lmd_output_thread;

        for (i = 1; !lmd_shmem_stopping() && i <= lmd_loop; i++) {
                sprintf(pname, "_%04d_%08d", thread, i);

                switch (opc) {
                default:
                        LBUG();
                case LMD_OP_OPENCREATE:
                        rc = open(fname, lmd_oflags, lmd_mode);
                        if (rc >= 0) {
                                close(rc);
                                rc = 0;
                        }
                        break;
                case LMD_OP_UNLINK:
                        rc = unlink(fname);
                        if (rc != 0 && errno == ENOENT) {
                                unlink_miss++;
                                rc = 0;
                        }
                        break;
                }
                if (rc != 0) {
                        fprintf(stderr, "can't %s file %s: %d\n",
                                lmd_opc_str(opc), fname, errno);
                        lmd_shmem_stop();
                        break;
                }

                if (lmd_output_interval == 0 || tint == 0)
                        continue;

                if (i % lmd_output_interval != 0 || (thread - 1) % tint != 0)
                        continue;

                gettimeofday(&end, NULL);
                diff = difftime(&end, &start);
                fprintf(stdout, "thread %d %s %d files after %d seconds\n",
                        thread, lmd_opc_str(opc), i, (int)diff);
        }

        gettimeofday(&end, NULL);
        diff = difftime(&end, &start);

        if (unlink_miss != 0) {
                fprintf(stdout,
                        "WARNING...unlink missed %d files\n", unlink_miss);
        }

        lmd_shmem_lock();

        lmd_data->ls_times += diff;
        lmd_data->ls_thr_running--;

        if (lmd_data->ls_thr_running == 0) {
                diff = lmd_data->ls_times / lmd_nthreads;
                lmd_data->ls_iops = (lmd_loop * lmd_nthreads) / diff;

                gettimeofday(&end, NULL);
                diff = difftime(&end, &lmd_data->ls_start);
                lmd_data->ls_iops2 = (lmd_loop * lmd_nthreads) / diff;

                fprintf(stdout,
                        "The last thread exits after %d seconds\n", (int)diff);
                lmd_shmem_broadcast(thread, "DONE");
        }
        lmd_shmem_unlock();

#if LMD_DEBUG
        fprintf(stdout, "thread %d is exiting\n", thread);
#endif
}

int
main(int argc, char **argv)
{
        int     opc = LMD_OP_OPENCREATE;
        int	rc;
        int	i;
        int	c;

        while ((c = getopt(argc, argv, "t:l:p:n:o:i:dhsr")) != -1) {
                switch (c) {
                default:
                        fprintf(stderr, "Unknown parameter\n");
                        rc = -1;
                case 'h':
                        fprintf(stdout,
                                "%s [-p DIR] [-l COUNT] [-n NLEN]...\n"
                                "\t-p TEST_DIR, test directory\n"
                                "\t-l LOOP_COUNT, default is 10\n"
                                "\t-t NTHRS, number of threads\n"
                                "\t-d delay object creation\n"
                                "\t-s single mount point\n"
                                "\t-r remove files\n"
                                "\t-o N threads show progress\n"
                                "\t-i output interval\n"
                                "\t-n NAMELEN, name length\n", argv[0]);
                        rc = 0;
                        return rc;
                case 'r':
                        opc = LMD_OP_UNLINK;
                        break;
                case 'o':
                        lmd_output_thread = atoi(optarg);
                        break;
                case 'i':
                        lmd_output_interval = atoi(optarg);
                        break;
                case 'l':
                        lmd_loop = atoi(optarg);
                        if (lmd_loop <= 0 || lmd_loop > (1 << 30)) {
                                fprintf(stderr, "Invalid count %d\n", lmd_loop);
                                return -1;
                        }
			break;
                case 't':
                        lmd_nthreads = atoi(optarg);
                        if (lmd_nthreads <= 0 || lmd_nthreads > 512) {
                                fprintf(stderr,
                                        "Invalid nthr %d\n", lmd_nthreads);
                                return -1;
                        }
			break;

                case 'p':
                        lmd_tpath = optarg;
                        break;
                case 's':
                        lmd_single_mount = 1;
                        break;
                case 'n':
                        lmd_nlen = atoi(optarg);
                        if (lmd_nlen < 16 || lmd_nlen > 255) {
                                fprintf(stderr, "invalid nlen %d, it must "
                                        "be between 16 and 255\n", lmd_nlen);
                                return -1;
                        }
                        break;
                case 'd':
                        lmd_oflags |= O_LOV_DELAY_CREATE;
                        break;
                }
        }

        rc = lmd_shmem_setup();
        if (rc < 0)
                return -1;

        fprintf(stdout,
                "%d threads, each %s %d file (nlen %d) in %s%s\n",
                lmd_nthreads, lmd_opc_str(opc), lmd_loop, lmd_nlen,
                lmd_tpath == NULL ? TEST_DIR : lmd_tpath,
                opc == LMD_OP_OPENCREATE ? ((lmd_oflags & O_LOV_DELAY_CREATE) ?
                                            " with O_LOV_DELAY_CREATE" :
                                            " w/o O_LOV_DELAY_CREATE") : "");

        if (lmd_output_thread > lmd_nthreads)
                lmd_output_thread = lmd_nthreads;

        if (lmd_output_interval > lmd_loop)
                lmd_output_interval = lmd_loop;
        if (lmd_output_interval < 100)
                lmd_output_interval = 100;

        fprintf(stdout, "\t %d thread output progress for each %d files\n",
                        lmd_output_thread, lmd_output_interval);

        for (i = 1; i <= lmd_nthreads; i++) {
                int pid; /* fake pid */

                lmd_shmem_lock();
                lmd_data->ls_thr_starting++;
                pid = lmd_data->ls_thr_next++;
                lmd_shmem_unlock();

#if LMD_DEBUG
                fprintf(stdout, "creating thread: %d\n", pid);
#endif

                rc = fork();
                if (rc == 0) { /* child */
                        lmd_run_test(pid, opc);
                        return 0;

                } else if (rc > 0) { /* parent */
#if LMD_DEBUG
                        fprintf(stdout, "thread %d is created: %d\n", pid, rc);
#endif
                        continue;
                }

                /* parent with error */
                lmd_shmem_lock();

                lmd_data->ls_thr_starting--;
                lmd_shmem_stop();
                lmd_shmem_broadcast(0, "STOP");

                lmd_shmem_unlock();

                fprintf(stderr, "Failed to create thread: %d\n", i);
                break;
        }

        lmd_shmem_lock();

        while (lmd_data->ls_thr_running > 0 ||
               lmd_data->ls_thr_starting > 0)
                lmd_shmem_wait(0, "DONE");

        lmd_shmem_unlock();

        if (lmd_shmem_stopping()) {
                fprintf(stderr, "test failed\n");
                return 0;
        }

        fprintf(stdout,
                "%d threads %s total %d file, IOPS %d / sec, IOPS2 %d / sec\n",
                lmd_nthreads, lmd_opc_str(opc), lmd_loop * lmd_nthreads,
                lmd_data->ls_iops, lmd_data->ls_iops2);
        return 0;
}
