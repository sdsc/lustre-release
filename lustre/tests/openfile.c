/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */

#if 0
#define DEBUG
#endif

/* for O_DIRECTORY and O_DIRECT */
#define _GNU_SOURCE

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <lustre/lustre_user.h>

typedef struct flag_mapping {
       const char *string;
       const int  flag;
} FLAG_MAPPING;

FLAG_MAPPING flag_table[] = {
       {"O_RDONLY", O_RDONLY},
       {"O_WRONLY", O_WRONLY},
       {"O_RDWR", O_RDWR},
       {"O_CREAT", O_CREAT},
       {"O_EXCL", O_EXCL},
       {"O_NOCTTY", O_NOCTTY},
       {"O_TRUNC", O_TRUNC},
       {"O_APPEND", O_APPEND},
       {"O_NONBLOCK", O_NONBLOCK},
       {"O_NDELAY", O_NDELAY},
       {"O_SYNC", O_SYNC},
#ifdef O_DIRECT
       {"O_DIRECT", O_DIRECT},
#endif
       {"O_LARGEFILE", O_LARGEFILE},
       {"O_DIRECTORY", O_DIRECTORY},
       {"O_NOFOLLOW", O_NOFOLLOW},
       {"O_LOV_DELAY_CREATE", O_LOV_DELAY_CREATE},
       {"", -1}
};

void Usage_and_abort(void)
{
       fprintf(stderr, "Usage: openfile -f flags [ -m mode ] filename \n");
       fprintf(stderr, "e.g. openfile -f O_RDWR:O_CREAT -m 0755 /etc/passwd\n");
       exit(-1);
}

int main(int argc, char** argv)
{
        int    fd;
        int    flags=0;
        mode_t mode=0644;
        char*  fname=NULL;
        int    mode_set=0;
        int    flag_set=0;
        int    file_set=0;
        int    c;
        int    save_errno;
        char*  cloned_flags = NULL;

        if (argc == 1)
                Usage_and_abort();

        while ((c = getopt (argc, argv, "f:m:")) != -1) {
                switch (c) {
                case 'f': {
                        char *tmp;

                        cloned_flags = (char *)malloc(strlen(optarg)+1);
                        if (cloned_flags == NULL) {
                                fprintf(stderr, "Insufficient memory.\n");
                                exit(-1);
                        }

                        strncpy(cloned_flags, optarg, strlen(optarg)+1);
                        for (tmp = strtok(optarg, ":|"); tmp;
                             tmp = strtok(NULL, ":|")) {
                                int i = 0;
#ifdef DEBUG
                                printf("flags = %s\n",tmp);
#endif
                                flag_set = 1;
                                for (i = 0; flag_table[i].flag != -1; i++) {
                                        if (!strcmp(tmp, flag_table[i].string)){
                                                flags |= flag_table[i].flag;
                                                break;
                                        }
                                }

                                if (flag_table[i].flag == -1) {
                                        fprintf(stderr, "No such flag: %s\n",
                                                tmp);
                                        exit(-1);
                                }
                        }
#ifdef DEBUG
                        printf("flags = %x\n", flags);
#endif
                        break;
                }
                case 'm':
#ifdef DEBUG
                        printf("mode = %s\n", optarg);
#endif
                        mode = strtol(optarg, NULL, 8);
                        mode_set = 1;
#ifdef DEBUG
                        printf("mode = %o\n", mode);
#endif
                        break;
                default:
                        fprintf(stderr, "Bad parameters.\n");
                        Usage_and_abort();
                }
        }

        if (optind == argc) {
                fprintf(stderr, "Bad parameters.\n");
                Usage_and_abort();
        }

        fname = argv[optind];
        file_set = 1;

        if (!flag_set || !file_set) {
                fprintf(stderr, "Missing flag or file-name\n");
                exit(-1);
        }


        if (flags & O_CREAT)
                fd = open(fname, flags, mode);
        else
                fd = open(fname, flags);

        save_errno = errno;

        if (fd != -1) {
                printf("Succeed in opening file \"%s\"(flags=%s",
                       fname, cloned_flags);

                if (mode_set)
                        printf(", mode=%o", mode);
                printf(")\n");
                close(fd);
                return 0;
        }

        fprintf(stderr, "Error in opening file \"%s\"(flags=%s",
                fname, cloned_flags);
        if (mode_set)
                fprintf(stderr, ", mode=%o", mode);
        fprintf(stderr, ") %d: %s\n", save_errno, strerror(save_errno));

        return save_errno;
}
