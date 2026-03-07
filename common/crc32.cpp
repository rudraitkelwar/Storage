#include "common/crc32.h"

static uint32_t table[256];
static bool inited = false;

static void init_table() {
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int k = 0; k < 8; k++) {
      c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
    }
    table[i] = c;
  }
  inited = true;
}

uint32_t crc32(const void* data, size_t n) {
  if (!inited) init_table();
  const unsigned char* p = static_cast<const unsigned char*>(data);
  uint32_t c = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; i++) {
    c = table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
  }
  return c ^ 0xFFFFFFFFu;
}
