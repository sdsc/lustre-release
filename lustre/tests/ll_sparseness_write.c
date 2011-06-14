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
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>

#define BUFSIZE (1024*1024)

/* Function: pwrite character '+' to <filename> at <offset> (man pwrite)
 * Return:   0 success
 *           1 failure */
int main(int argc, char**argv)
{
        int p_size;
        unsigned int offset;
        unsigned int len = 1;
        char *filename;
        int fd;
        char buf[BUFSIZE];
        char *end;

        if(argc != 3 && argc != 4) {
                fprintf(stderr, "Usage: %s <filename> <offset> [size]\n", argv[0]);
                exit(1);
        }

        filename = argv[1];
        offset = strtoul(argv[2], &end, 10);
        if (*end) {
                fprintf(stderr, "<offset> parameter should be integer\n");
                exit(1);
        }

        if (argc == 4) {
                len = strtoul(argv[3], &end, 10);
                if (*end || len > BUFSIZE) {
                        fprintf(stderr, "size is too large or incorrect\n");
                        exit(2);
                }
        }

        fd = open(filename, O_CREAT|O_RDWR, 0644);
        if (fd == -1) {
                fprintf(stderr, "Opening %s fails (%s)\n",
                        filename, strerror(errno));
                return 1;
        }

        /* write the character '+' at offset */
        memset(buf, '+', len);
        p_size = pwrite(fd, buf, len, offset);
        if (p_size != len) {
                fprintf(stderr, "pwrite %s returned(%d/%d/%d)\n",
                        filename, len, p_size, errno);
                close(fd);
                return 1;
        }

        close(fd);
        return 0;
}
