/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */

#include <sys/types.h>
#include <attr/xattr.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void usage(char *prog) {
        printf("Usage: %s <pathname> <xattr name>\n", prog);
}

/* Simple program to test the getxattr return value. */
int main(int argc, char *argv[]) {

        if (argc != 3) {
                usage(argv[0]);
                exit(1);
        }

        char *path = argv[1];
        char *xattr = argv[2];

        ssize_t ret_null = getxattr(path, xattr, NULL, 0);

        char *buf = (char *)malloc(ret_null);
        ssize_t ret_buf = lgetxattr(path, xattr, buf, ret_null);

        if (ret_null != ret_buf) {
                fprintf(stderr, "getxattr returned inconsistent sizes!\n");
                fprintf(stderr, "getxattr(%s, %s, NULL, 0) = %zi\n",
                        path, xattr, ret_null);
                fprintf(stderr, "getxattr(%s, %s, %p, %zi) = %zi\n",
                        path, xattr, buf, ret_null, ret_buf);
                exit(1);
        }

        return 0;
}
