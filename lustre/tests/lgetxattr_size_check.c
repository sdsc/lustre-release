#include <sys/types.h>
#include <attr/xattr.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void usage(char *prog)
{
	printf("Usage: %s <pathname> <xattr name>\n", prog);
}

/* Simple program to test the lgetxattr return value. */
int main(int argc, char *argv[])
{
	char *path, *xattr, *buf;

	if (argc != 3) {
		usage(argv[0]);
		exit(1);
	}

	path = argv[1];
	xattr = argv[2];

	ssize_t ret_null = lgetxattr(path, xattr, NULL, 0);

	buf = (char *)malloc(ret_null);
	ssize_t ret_buf = lgetxattr(path, xattr, buf, ret_null);

	if (ret_null != ret_buf) {
		fprintf(stderr, "lgetxattr returned inconsistent sizes!\n");
		fprintf(stderr, "lgetxattr(%s, %s, NULL, 0) = %zi\n",
			path, xattr, ret_null);
		fprintf(stderr, "lgetxattr(%s, %s, %p, %zi) = %zi\n",
			path, xattr, buf, ret_null, ret_buf);
		exit(1);
	}

	return 0;
}
