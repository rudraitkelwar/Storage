#include "common/net.h"
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>

int read_exact(int fd, void* buf, size_t n) {
  char* p = static_cast<char*>(buf);
  size_t off = 0;
  while (off < n) {
    ssize_t r = ::recv(fd, p + off, n - off, 0);
    if (r == 0) return 0;                 // peer closed
    if (r < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    off += static_cast<size_t>(r);
  }
  return 1;
}

int write_all(int fd, const void* buf, size_t n) {
  const char* p = static_cast<const char*>(buf);
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::send(fd, p + off, n - off, 0);
    if (w < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    off += static_cast<size_t>(w);
  }
  return 1;
}
