// Copyright 2024, DuckDB contributors. Adapted for QLever.
// This shim provides a minimal FileHandle class using POSIX I/O for block-level
// read/write operations.

#pragma once

#include <string>

#include "storage/compat/duckdb_types.h"

namespace duckdb {

// Minimal FileHandle using POSIX file descriptors (pread/pwrite/fsync).
struct FileHandle {
 public:
  FileHandle(int fd, std::string path, bool owns_fd = true);
  ~FileHandle();

  // No copy.
  FileHandle(const FileHandle&) = delete;
  FileHandle& operator=(const FileHandle&) = delete;

  // Move.
  FileHandle(FileHandle&& other) noexcept;
  FileHandle& operator=(FileHandle&& other) noexcept;

  // Read `nr_bytes` from the file at `location` into `buffer`.
  void Read(data_ptr_t buffer, idx_t nr_bytes, idx_t location);

  // Write `nr_bytes` from `buffer` to the file at `location`.
  void Write(data_ptr_t buffer, idx_t nr_bytes, idx_t location);

  // Sync (fsync) the file to disk.
  void Sync();

  // Truncate the file to `new_length` bytes.
  void Truncate(int64_t new_length);

  // Get the file size.
  int64_t GetFileSize() const;

  // Whether this is an on-disk file (always true for our POSIX handle).
  bool OnDiskFile() const { return true; }

  // The file path.
  std::string path;

 private:
  int fd_;
  bool owns_fd_;
};

// Open a file. Returns a unique_ptr<FileHandle>.
// Flags: 'r' = read-only, 'w' = read-write (create if not exists),
//        'c' = create new file.
unique_ptr<FileHandle> OpenFile(const std::string& path, const char* mode);

}  // namespace duckdb
