#pragma once
#include <string>
#include <mutex>
#include <cstdint>

class BlockStore {
public:
  BlockStore(const std::string& dir, uint64_t volume_bytes);
  ~BlockStore();

  bool read(uint64_t offset, uint32_t len, std::string& out, std::string& err);
  bool write(uint64_t offset, const std::string& data, std::string& err);

private:
  bool open_or_create_files(std::string& err);
  bool ensure_size(int fd, uint64_t size, std::string& err);
  bool replay_wal(std::string& err);
  bool append_wal(uint64_t offset, uint32_t len, uint32_t crc, const char* payload, std::string& err);

private:
  std::string dir_;
  std::string data_path_;
  std::string wal_path_;
  uint64_t volume_bytes_;
  int data_fd_{-1};
  int wal_fd_{-1};
  uint64_t seq_{0};
  std::mutex mtx_;
};
