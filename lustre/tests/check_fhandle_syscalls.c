#include <stdio.h>
#include <stdlib.h>
#include <asm/unistd.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

struct file_handle {
	unsigned int	handle_bytes;
	int		handle_type;
	void		*f_handle;
};

int main(int argc, char *argv[])
{
	int ret;
	int fd, mnt_id, mnt_fd;
	char buf[100];
	struct file_handle fh;
	fh.handle_type = 0;
	fh.f_handle = malloc(100);
	fh.handle_bytes = 128;
	errno = 0;
	ret = syscall(303, NULL, argv[1], &fh, &mnt_id, AT_SYMLINK_FOLLOW);
	if (ret) {
		perror("Error:");
		exit(1);
	}
	printf("%d\n", fh.handle_bytes);
	mnt_fd = open(argv[2], O_DIRECTORY);
	if (mnt_id <= 0) {
		perror("Error:");
		exit(1);
	}
	fd = syscall(304, mnt_fd, &fh, O_RDONLY);
	if (fd <= 0) {
		perror("Error:");
		exit(1);
	}
	memset(buf, 0 , 100);
	while (read(fd, buf, 100) > 0) {
		printf("%s", buf);
		memset(buf, 0, 100);
	}
	return 0;
}

