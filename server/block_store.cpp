#include "server/block_store.h"
#include "common/protocol.h"
#include "common/crc32.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstring>

static bool aligned_4k(uint64_t x) { return (x % kBlockSize) == 0; }

BlockStore::BlockStore(const std::string& dir, uint64_t volume_bytes)
  : dir_(dir),
    data_path_(dir + "/data.bin"),
    wal_path_(dir + "/wal.log"),
    volume_bytes_(volume_bytes) {}

BlockStore::~BlockStore() {
  if (data_fd_ >= 0) ::close(data_fd_);
  if (wal_fd_ >= 0) ::close(wal_fd_);
}

bool BlockStore::ensure_size(int fd, uint64_t size, std::string& err) {
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    err = "ftruncate failed: " + std::string(std::strerror(errno));
    return false;
  }
  return true;
}

bool BlockStore::open_or_create_files(std::string& err) {
  ::mkdir(dir_.c_str(), 0755);

  data_fd_ = ::open(data_path_.c_str(), O_CREAT | O_RDWR, 0644);
  if (data_fd_ < 0) { err = "open data.bin failed: " + std::string(std::strerror(errno)); return false; }

  wal_fd_ = ::open(wal_path_.c_str(), O_CREAT | O_RDWR, 0644);
  if (wal_fd_ < 0) { err = "open wal.log failed: " + std::string(std::strerror(errno)); return false; }

  if (!ensure_size(data_fd_, volume_bytes_, err)) return false;
  return true;
}

// WAL record layout:
// [seq(uint64)][offset(uint64)][len(uint32)][crc(uint32)][payload(len)]
bool BlockStore::append_wal(uint64_t offset, uint32_t len, uint32_t crc, const char* payload, std::string& err) {
  uint64_t seq = seq_++;

  if (::lseek(wal_fd_, 0, SEEK_END) < 0) {
    err = "lseek wal end failed: " + std::string(std::strerror(errno));
    return false;
  }

  auto write_u64 = [&](uint64_t v) -> bool {
    if (::write(wal_fd_, &v, sizeof(v)) != (ssize_t)sizeof(v)) { err = "write wal u64 failed"; return false; }
    return true;
  };
  auto write_u32 = [&](uint32_t v) -> bool {
    if (::write(wal_fd_, &v, sizeof(v)) != (ssize_t)sizeof(v)) { err = "write wal u32 failed"; return false; }
    return true;
  };

  if (!write_u64(seq)) return false;
  if (!write_u64(offset)) return false;
  if (!write_u32(len)) return false;
  if (!write_u32(crc)) return false;

  if (::write(wal_fd_, payload, len) != (ssize_t)len) {
    err = "write wal payload failed: " + std::string(std::strerror(errno));
    return false;
  }

  // Make WAL durable: "OK" means crash-safe for logged writes.
  // write() alone is not durable; sync is required for durability semantics. [Durability nuance]
  if (::fdatasync(wal_fd_) != 0) {
    err = "fdatasync(wal) failed: " + std::string(std::strerror(errno));
    return false;
  }
  return true;
}

bool BlockStore::replay_wal(std::string& err) {
  if (::lseek(wal_fd_, 0, SEEK_SET) < 0) {
    err = "lseek wal start failed: " + std::string(std::strerror(errno));
    return false;
  }

  while (true) {
    uint64_t seq, offset;
    uint32_t len, crc;

    ssize_t r = ::read(wal_fd_, &seq, sizeof(seq));
    if (r == 0) break; // EOF
    if (r != (ssize_t)sizeof(seq)) { err = "wal truncated (seq)"; return false; }

    if (::read(wal_fd_, &offset, sizeof(offset)) != (ssize_t)sizeof(offset)) { err = "wal truncated (offset)"; return false; }
    if (::read(wal_fd_, &len, sizeof(len)) != (ssize_t)sizeof(len)) { err = "wal truncated (len)"; return false; }
    if (::read(wal_fd_, &crc, sizeof(crc)) != (ssize_t)sizeof(crc)) { err = "wal truncated (crc)"; return false; }

    if (len == 0 || (len % kBlockSize) != 0) { err = "bad wal len"; return false; }
    if (offset + len > volume_bytes_) { err = "wal write out of range"; return false; }

    std::string payload(len, '\0');
    ssize_t got = ::read(wal_fd_, payload.data(), len);
    if (got != (ssize_t)len) { err = "wal truncated (payload)"; return false; }

    uint32_t actual = crc32(payload.data(), payload.size());
    if (actual != crc) { err = "wal crc mismatch"; return false; }

    // Apply to data.bin
    ssize_t pw = ::pwrite(data_fd_, payload.data(), len, (off_t)offset);
    if (pw != (ssize_t)len) { err = "pwrite(data) failed in replay"; return false; }

    (void)seq; // kept for future use
  }

  // Rotate/truncate WAL so startup doesn't get slower forever.
  if (::ftruncate(wal_fd_, 0) != 0) {
    err = "truncate wal failed: " + std::string(std::strerror(errno));
    return false;
  }
  if (::lseek(wal_fd_, 0, SEEK_SET) < 0) {
    err = "lseek wal after truncate failed";
    return false;
  }
  return true;
}

bool BlockStore::read(uint64_t offset, uint32_t len, std::string& out, std::string& err) {
  std::lock_guard<std::mutex> g(mtx_);
  if (!aligned_4k(offset) || !aligned_4k(len) || len == 0) { err = "unaligned read"; return false; }
  if (offset + len > volume_bytes_) { err = "read out of range"; return false; }

  out.assign(len, '\0');
  ssize_t pr = ::pread(data_fd_, out.data(), len, (off_t)offset);
  if (pr != (ssize_t)len) { err = "pread failed: " + std::string(std::strerror(errno)); return false; }
  return true;
}

bool BlockStore::write(uint64_t offset, const std::string& data, std::string& err) {
  std::lock_guard<std::mutex> g(mtx_);
  uint32_t len = (uint32_t)data.size();
  if (!aligned_4k(offset) || !aligned_4k(len) || len == 0) { err = "unaligned write"; return false; }
  if (offset + len > volume_bytes_) { err = "write out of range"; return false; }

  uint32_t c = crc32(data.data(), data.size());

  // WAL first, then apply to data. WAL enables crash recovery.
  if (!append_wal(offset, len, c, data.data(), err)) return false

  ssize_t pw = ::pwrite(data_fd_, data.data(), len, (off_t)offset);
  if (pw != (ssize_t)len) { err = "pwrite failed: " + std::string(std::strerror(errno)); return false; }

  // Optional: fdatasync(data_fd_) if you want data file durability immediately,
  // but this is expensive; WAL durability is the core property to demonstrate.
  return true;
}

bool BlockStore::replay_wal(std::string& err); // already defined above

// Call this once from server startup
bool init_store(BlockStore& bs, std::string& err) {
  if (!bs.open_or_create_files(err)) return false;
  if (!bs.replay_wal(err)) return false;
  return true;
}
