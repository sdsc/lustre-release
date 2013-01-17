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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 *
 * Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2013, Intel Corporation.
 *
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#define CHUNK (32768 * 1024)   /*bytes 32mb or so */
#define CHUNK_KB (CHUNK/1024)

/*MINSPACE should consider the the size of CHUNK */
#define MINSPACE (160 * 1024) /* 160mb or so */

struct data {
	struct data *next;
	int *chunk;
};

struct data head;

/* Return size of free ram and swap space */
long unsigned freespace(void)
{
	struct sysinfo si;
	sysinfo(&si);
	return ((si.freeswap + si.freeram) * si.mem_unit)/1024;
}

long unsigned totalspace(void)
{
	struct sysinfo si;
	sysinfo(&si);
	return ((si.totalswap + si.totalram) * si.mem_unit)/1024;
}

void usage(const char *prog, FILE *out)
{
	fprintf(out, "usage: %s [-a] [allocsize]\n", prog);
	fprintf(out, " -a consume all of memory to 100mb in a loop,");
	fprintf(out, " allocsize is ignored\n");
	fprintf(out, " allocsize is kbytes, or number[KMGP] (P = pages)\n");
	exit(out == stderr);
}

int main(int argc, char *argv[])
{
	long long int i, kbtotal = 0, kballoc = 0;
	unsigned long size;	
	int status, automode = 0;
	struct data *current;
	pid_t pid;

	if (argc == 2) {
		char *end = NULL;
		kbtotal = strtoull(argv[1], &end, 0);

		switch(*end) {
		case '-':
			if (*(end+1) == 'a') {
				automode = 1;
				break;
			}
		case 'g':
		case 'G':
			kbtotal *= 1024;
		case 'm':
		case 'M':
			kbtotal *= 1024;
		case '\0':
		case 'k':
		case 'K':
			break;
		case 'p':
		case 'P':
			kbtotal *= 4;
			break;
		default:
			usage(argv[0], stderr);
			break;
		}
	}

	if (automode) {
		printf("Starting automatic mode\n");
		kbtotal = totalspace();
	}

	if (kbtotal == 0)
		usage(argv[0], stderr);

alloc_start:
	printf("[%d] allocating %lld kbytes in %u kbyte chunks\n",
					getpid(), kbtotal, CHUNK/1024);

	size = (unsigned long) sizeof(struct data);
	current = &head;
	do {
		current->next = (struct data *) malloc(size);
		if (current == NULL) {
			fprintf(stderr, "malloc(%lu) failed (%lld/%lld)\n",
						size, kballoc, kbtotal);
			break;
		}

		current->chunk = (int *) malloc(CHUNK);
		if (current == NULL) {
			fprintf(stderr, "malloc(%u) failed (%lld/%lld)\n",
				CHUNK, kballoc, kbtotal);
			break;
		}

		madvise(current->chunk, CHUNK, MADV_DONTFORK);
		madvise(current, sizeof(struct data), MADV_DONTFORK);

		printf("touching %p ([%lld-%lld]/%lld)\n", current, kballoc,
					kballoc + CHUNK_KB - 1, kbtotal);

		for (i = 0; i < CHUNK/sizeof(int); i++)
			current->chunk[i] = 0xdeadbeef;

		kballoc += CHUNK_KB;
		if (!automode && kballoc >= kbtotal)
			break;

		current = current->next;
		current->next = NULL;

	} while (freespace() > MINSPACE);

	printf("touched %lld kbytes\n", kballoc);

	/* In some situations a single process may not be able to consume all
	 * of memory.  Fork a child to help.
	 */
	if (automode && (freespace() > MINSPACE)) {
		pid = fork();
		if (pid == -1) {
			fprintf(stderr, "fork failed\n");
			exit(-1);
		}
		if (pid == 0) { /*CHILD */
			goto alloc_start;
			/*We never get here */
		} else { /*PARENT */
			wait(&status);
		}
	}

	/* Memhog will verify the data.  Don't do this for automode as you
	 * don't want to hold the system to that tight of memory for much time.
	 */
	if (!automode) {
		printf("verifying %lld kbytes in %u kbyte chunks\n",
							kbtotal, CHUNK/1024);
		current = &head;
		kballoc = 0;
		do {
			kballoc += CHUNK_KB;
			printf("verifying %p (%lld/%lld)\n", current,
							kballoc, kbtotal);

			for (i = 0; i < CHUNK/sizeof(int); i++)
				if (current->chunk[i] != 0xdeadbeef)
					fprintf(stderr, "verify failed %x !="
						"%x at %p\n",
						current->chunk[i],
						0xdeadbeef,
						current);
			current = current->next;
		} while (current->next != NULL);

		printf("verified %lld kbytes\n", kballoc);
	}

	return 0;
}
