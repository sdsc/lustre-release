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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * GPL HEADER END
 */

/*
 * Copyright (c) 2016 DDN Storage
 * Author: Sebastien Buisson sbuisson@ddn.com
 */

/*
 * lustre/utils/l_getsepol.c
 * Userland helper to retrieve SELinux policy information.
 */

#include <sys/types.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <libgen.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <syslog.h>
#include <stdarg.h>
#include <fcntl.h>
#include <stddef.h>
#include <ctype.h>
#include <dirent.h>

#include <openssl/sha.h>

#include <libcfs/util/param.h>
#include <lustre/lustre_user.h>
#include <lustre/lustre_idl.h>


static char *progname;

static void errlog(const char *fmt, ...)
{
	va_list args;

	openlog(progname, LOG_PERROR | LOG_PID, LOG_AUTHPRIV);

	va_start(args, fmt);
	vsyslog(LOG_NOTICE, fmt, args);
	va_end(args);

	closelog();
}

/* Determine if SELinux is in permissive or enforcing mode.
 * Read info from (OLD)SELINUXMNT/enforce */
static int sepol_get_mode(int *enforce)
{
	int fd;
	char path[25];
	char buffer[2] = "";
	int rc;
/* Definitions taken from libselinux/src/policy.h */
#define SELINUXMNT "/sys/fs/selinux"
#define OLDSELINUXMNT "/selinux"

	snprintf(path, sizeof(path), "%s/enforce", SELINUXMNT);
	fd = open(path, O_RDONLY);
	if (fd == -1) {
		/* Try with old path */
		snprintf(path, sizeof(path), "%s/enforce", OLDSELINUXMNT);
		fd = open(path, O_RDONLY);
		if (fd == -1) {
			errlog("can't open SELinux file %s: %s\n",
			       path, strerror(errno));
			return -ENOENT;
		}
	}

	/* Read file */
	if (read(fd, buffer, 1) == -1) {
		errlog("can't read SELinux file %s: %s\n",
		       path, strerror(errno));
		return -EINVAL;
	}

	/* Close file */
	rc = close(fd);
	if (rc == -1)
		return -errno;

	if (sscanf(buffer, "%d", enforce) != 1)
		return -EINVAL;

	return 0;
}

/* Try to read policy name from SELINUXCONFIG */
static int sepol_get_name(char **policy_type, int *policy_len)
{
/* Definitions taken from libselinux/src/selinux_config.c */
#define SELINUXDIR "/etc/selinux/"
#define SELINUXCONFIG SELINUXDIR "config"
#define SELINUXTYPETAG "SELINUXTYPE="
	int fd;
	char path[] = SELINUXCONFIG;
	ssize_t count = 1024;
	int offset = 0;
	char *buffer, *p, *q;
	int alloc = 1024;
	int rc = 0;

	/* Open config file */
	fd = open(path, O_RDONLY);
	if (fd == -1) {
		errlog("can't open SELinux config file %s: %s\n",
		       path, strerror(errno));
		return -ENOENT ;
	}

	/* Read config file */
	buffer = malloc(alloc);
	if (buffer == NULL)
		return -ENOMEM;
	memset(buffer, 0, alloc);

	p = buffer;
	while (count == 1024) {
		count = read(fd, p, count);
		if (count < 0) {
			errlog("can't read SELinux config file %s\n", path);
			rc = -EINVAL;
			goto out_free;
		}
		p += count;
		offset = p - buffer;
		if (offset >= alloc) {
			/* realloc buffer */
			alloc = 2*alloc;
			p = buffer;
			buffer = malloc(alloc);
			if (buffer == NULL) {
				buffer = p;
				alloc = offset;
				rc = -ENOMEM;
				goto out_free;
			}
			memset(buffer, 0, alloc);
			memcpy(buffer, p, offset);
			free(p);
			p = buffer + offset;
		}
	}

	/* Close config file */
	rc = close(fd);
	if (rc == -1)
		return -errno;

	/* Find policy name in config file */
	p = strstr(buffer, SELINUXTYPETAG);
	while (p != NULL) {
		q = p-1;
		while ((*q != '\n') && isspace(*q))
			q--;
		if (*q != '\n')
			p = strstr(p+1, SELINUXTYPETAG);
		else
			break;
	}
	if (p != NULL) {
		p += strlen(SELINUXTYPETAG);
		q = p;
		while (!isspace(*p) && !iscntrl(*p))
			p++;
		*p = '\0';
		*policy_len = strlen(q) + 1;
		*policy_type = malloc(*policy_len);
		if (*policy_type == NULL) {
			rc = -ENOMEM;
			goto out_free;
		}
		strncpy(*policy_type, q, *policy_len);
	} else {
		rc = -EINVAL;
	}

out_free:
	free(buffer);
	return rc;
}

/* Read binary SELinux policy, and compute checksum */
static int sepol_get_policy_data(char *policy_type, unsigned char *polver,
				 unsigned char **hashval, int *hashsize)
{
	int fd;
	char dir_path[100];
	char entry_path[100];
	DIR *dp;
	struct dirent *entry;
	struct stat statbuf;
	char buffer[1024];
	ssize_t count = 1024;
	SHA_CTX ctx;
	int rc;

	snprintf(dir_path, sizeof(dir_path), "%s%s/policy",
		 SELINUXDIR, policy_type);

	dp = opendir(dir_path);
	if (dp == NULL) {
		errlog("cannot open directory %s: %s\n",
		       dir_path, strerror(errno));
		return -ENOENT;
	}
	while ((entry = readdir(dp)) != NULL) {
		snprintf(entry_path, sizeof(entry_path), "%s/%s",
			 dir_path, entry->d_name);
		lstat(entry_path, &statbuf);
		if (S_ISREG(statbuf.st_mode)) {
			/* there is only one file here */
			char *p = strchr(entry->d_name, '.');
			if (p != NULL) {
				*polver = (unsigned char)strtoul(p + 1,
								 NULL, 10);
				break;
			}
		}
	}
	closedir(dp);
	if (entry == NULL) {
		errlog("cannot find policy file in directory %s: %s\n",
		       dir_path, strerror(errno));
		return -ENOENT;
	}

	/* Open policy file */
	fd = open(entry_path, O_RDONLY);
	if (fd == -1) {
		errlog("can't open SELinux policy file %s: %s\n", entry_path,
		       strerror(errno));
		return -ENOENT;
	}

	/* Read policy file */
	SHA1_Init(&ctx);
	while (count == 1024) {
		count = read(fd, buffer, count);
		if (count < 0) {
			errlog("can't read SELinux policy file %s\n",
			       entry_path);
			rc = -errno;
			close(fd);
			return rc;
		}
		SHA1_Update(&ctx, buffer, count);
	}

	/* Close policy file */
	rc = close(fd);
	if (rc == -1)
		return -errno;

	*hashsize = SHA_DIGEST_LENGTH;
	*hashval = malloc(*hashsize);
	if (*hashval == NULL)
		return -ENOMEM;

	SHA1_Final(*hashval, &ctx);

	return 0;
}

/**
 * Calculate SELinux status information.
 * String that represents SELinux status info has the following format:
 * <mode>:<policy name>:<policy version>:<policy checksum>
 * <mode> is a digit equal to 0 for SELinux Permissive mode,
 * and 1 for Enforcing mode.
 * When called from kernel space, it requires 4 args:
 * - obd type
 * - obd name
 * - pointer to request
 * - pointer to import
 * When called from user space, it will print calculated info
 * to stdout.
 */
int main(int argc, char **argv)
{
	struct sepol_downcall_data *data = NULL;
	unsigned long int ptr;
	glob_t path;
	int fd, size;
	int enforce;
	char *policy_type = NULL;
	int policy_len;
	unsigned char polver = 0;
	unsigned char *hashval = NULL;
	int hashsize = 0;
	char *p;
	int idx;
	int rc = 0;

	progname = basename(argv[0]);

	size = offsetof(struct sepol_downcall_data,
			sdd_sepol[LUSTRE_NODEMAP_SEPOL_LENGTH + 1]);
	data = malloc(size);
	if (!data) {
		errlog("malloc sepol downcall data(%d) failed!\n", size);
		rc = -ENOMEM;
		goto out;
	}

	memset(data, 0, size);
	data->sdd_magic = SEPOL_DOWNCALL_MAGIC;

	/* Determine if SELinux is in permissive or enforcing mode.
	 * Read info from (OLD)SELINUXMNT/enforce */
	rc = sepol_get_mode(&enforce);
	if (rc < 0)
		goto out;

	/* Try to read policy name from SELINUXCONFIG */
	rc = sepol_get_name(&policy_type, &policy_len);
	if (rc < 0)
		goto out;

	/* Read binary SELinux policy, and compute checksum */
	rc = sepol_get_policy_data(policy_type, &polver, &hashval, &hashsize);
	if (rc < 0)
		goto out_poltyp;

	/* Pull all info together and generate string
	 * to represent SELinux policy information */
	rc = snprintf(data->sdd_sepol, LUSTRE_NODEMAP_SEPOL_LENGTH + 1,
		      "%.1d:%s:%u:", enforce, policy_type, polver);
	if (rc >= LUSTRE_NODEMAP_SEPOL_LENGTH + 1) {
		rc = -EMSGSIZE;
		goto out_hashval;
	}

	p = data->sdd_sepol + strlen(data->sdd_sepol);
	size = LUSTRE_NODEMAP_SEPOL_LENGTH + 1 - strlen(data->sdd_sepol);
	for (idx = 0; idx < hashsize; idx++) {
		rc = snprintf(p, size, "%02x",
			      (unsigned char)(hashval[idx]));
		p += 2;
		size -= 2;
		if (size < 0 || rc >= size) {
			rc = -EMSGSIZE;
			goto out_hashval;
		}
	}
	data->sdd_sepol_len = p - data->sdd_sepol;

	size = offsetof(struct sepol_downcall_data,
			sdd_sepol[data->sdd_sepol_len]);

	if (argc < 5) {
		if (size == 0 || data->sdd_sepol_len == 0)
			errlog("failed to get SELinux status info\n");
		printf("SELinux status info: %.*s\n",
		       data->sdd_sepol_len, data->sdd_sepol);
		return rc;
	}

	ptr = strtoul(argv[3], NULL, 16);
	data->sdd_req = (void *)ptr;
	ptr = strtoul(argv[4], NULL, 16);
	data->sdd_imp = (void *)ptr;

	/* Send SELinux policy info to kernelspace */
	rc = cfs_get_param_paths(&path, "%s/%s/srpc_sepol", argv[1], argv[2]);
	if (rc != 0) {
		rc = -errno;
		goto out;
	}

	fd = open(path.gl_pathv[0], O_WRONLY);
	if (fd < 0) {
		errlog("can't open file '%s':%s\n", path.gl_pathv[0],
		       strerror(errno));
		rc = -errno;
		goto out_params;
	}

	rc = write(fd, data, size);
	close(fd);
	if (rc != size) {
		errlog("partial write ret %d: %s\n", rc, strerror(errno));
		rc = -errno;
	} else {
		rc = 0;
	}


out_params:
	cfs_free_param_data(&path);
out_hashval:
	free(hashval);
out_poltyp:
	free(policy_type);
out:
	if (data != NULL)
		free(data);

	return rc;
}
