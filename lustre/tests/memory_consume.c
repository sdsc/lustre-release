/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License version 2 for more details.  A copy is
 * included in the COPYING file that accompanied this code.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * GPL HEADER END
 *
 * Copyright (c) 2012 Intel Corporation.
 */
/*
 *A simple program to consume all but 100mb of Ram and Swap.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

#define MINFREE		100000000 /*Bytes about 100mb */

struct meg {
	struct meg *next;
	char junk[102398];
};

struct meg head;

/* Return size of free ram and swap space in bytes */
long unsigned freespace(void)
{
	struct sysinfo si;
	sysinfo(&si);
	return (si.freeswap + si.freeram) * si.mem_unit;
}

/* Allocate as much memory as possible.  ENOMEM = exit */
void big_alloc()
{

	struct meg *current;
	current = &head;
	do {
		current->next = (struct meg *) malloc(sizeof(struct meg));
		madvise(current->next, sizeof(struct meg), MADV_DONTFORK);
		current = current->next;
	} while ((current != NULL) && (freespace() > MINFREE));

	if (freespace() <= MINFREE) {
		/*free space is exhausted */
		printf("Freespace is now %lu bytes we are done\n", freespace());
		exit(0);
	}
}

int main(int argc, char *argv[])
{
	int status;
	pid_t pid;

	printf("%lu total bytes free\n", freespace());

alloc_start:
	big_alloc();

	printf("%lu bytes left, time to fork\n", freespace());

	pid = fork();
	if (pid == -1) {
		perror("fork");
		exit(-1);
	}
	if (pid == 0) {	/*CHILD */
		goto alloc_start;
		/*we never get here */
	} else {	/*PARENT */
		wait(&status);
	}
}
