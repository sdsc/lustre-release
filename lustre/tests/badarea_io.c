#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	int rc;
	int fd = open(argv[1], O_WRONLY);

	if (fd == -1) {
		perror(argv[1]);
		return -1;
	}

	/* We need rc because Sles11 compiler warns against unchecked
	 * return value of read and write */
	rc = write(fd, (void *)0x4096000, 5);
	perror("write");

	close(fd);

	/* Tame the compiler spooked about rc assigned, but not used */
	if (!rc)
		rc = 1; // Value does not matter.

	fd = open(argv[1], O_RDONLY);
	rc = read(fd, (void *)0x4096000, 5);
	perror("read");

	close(fd);

	/* Tame the compiler spooked about rc assigned, but not used */
	if (!rc)
		return -1; // Not really important.

	return 0;
}
