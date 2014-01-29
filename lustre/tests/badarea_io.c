#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	int fd = open(argv[1], O_WRONLY);

	if (fd == -1) {
		perror(argv[1]);
		return -1;
	}

	write(fd, (void *)0x4096000, 5);
	perror("write");

	close(fd);

	fd = open(argv[1], O_RDONLY);
	read(fd, (void *)0x4096000, 5);
	perror("read");

	close(fd);

	return 0;
}
