// common/net.h
#pragma once
#include <cstddef>

int read_exact(int fd, void* buf, size_t n);
int write_all(int fd, const void* buf, size_t n);
