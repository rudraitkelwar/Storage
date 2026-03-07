// common/protocol.h
#pragma once
#include <cstdint>

enum class Op : uint8_t { READ=1, WRITE=2 };

#pragma pack(push, 1)
struct MsgHdr {
  uint8_t  op;       // Op
  uint64_t offset;   // bytes
  uint32_t len;      // bytes
  uint32_t crc32;    // for WRITE payload; for READ can be 0
};
#pragma pack(pop)

enum class Status : uint8_t { OK = 0, ERR = 1 };

#pragma pack(push, 1)
struct ReadRespHdr
{
  uint8_t status;
  uint32_t len;
  uint32_t crc32;
}
#pragma pack(pop)
