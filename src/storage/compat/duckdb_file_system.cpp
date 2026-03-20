// Copyright 2024, DuckDB contributors. Adapted for QLever.

#include "storage/compat/duckdb_file_system.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "storage/compat/duckdb_exceptions.h"

namespace duckdb {

FileHandle::FileHandle(int fd, std::string path_p, bool owns_fd)
    : path(std::move(path_p)), fd_(fd), owns_fd_(owns_fd) {}

FileHandle::~FileHandle() {
  if (owns_fd_ && fd_ >= 0) {
    ::close(fd_);
  }
}

FileHandle::FileHandle(FileHandle&& other) noexcept
    : path(std::move(other.path)), fd_(other.fd_), owns_fd_(other.owns_fd_) {
  other.fd_ = -1;
  other.owns_fd_ = false;
}

FileHandle& FileHandle::operator=(FileHandle&& other) noexcept {
  if (this != &other) {
    if (owns_fd_ && fd_ >= 0) {
      ::close(fd_);
    }
    path = std::move(other.path);
    fd_ = other.fd_;
    owns_fd_ = other.owns_fd_;
    other.fd_ = -1;
    other.owns_fd_ = false;
  }
  return *this;
}

void FileHandle::Read(data_ptr_t buffer, idx_t nr_bytes, idx_t location) {
  idx_t bytes_read = 0;
  while (bytes_read < nr_bytes) {
    auto result = ::pread(fd_, buffer + bytes_read, nr_bytes - bytes_read,
                          static_cast<off_t>(location + bytes_read));
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw IOException("Failed to read from file \"%s\": %s", path.c_str(),
                        strerror(errno));
    }
    if (result == 0) {
      throw IOException(
          "Failed to read from file \"%s\": unexpected end of file",
          path.c_str());
    }
    bytes_read += static_cast<idx_t>(result);
  }
}

void FileHandle::Write(data_ptr_t buffer, idx_t nr_bytes, idx_t location) {
  idx_t bytes_written = 0;
  while (bytes_written < nr_bytes) {
    auto result =
        ::pwrite(fd_, buffer + bytes_written, nr_bytes - bytes_written,
                 static_cast<off_t>(location + bytes_written));
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw IOException("Failed to write to file \"%s\": %s", path.c_str(),
                        strerror(errno));
    }
    bytes_written += static_cast<idx_t>(result);
  }
}

void FileHandle::Sync() {
  if (::fsync(fd_) != 0) {
    throw IOException("Failed to fsync file \"%s\": %s", path.c_str(),
                      strerror(errno));
  }
}

void FileHandle::Truncate(int64_t new_length) {
  if (::ftruncate(fd_, new_length) != 0) {
    throw IOException("Failed to truncate file \"%s\": %s", path.c_str(),
                      strerror(errno));
  }
}

int64_t FileHandle::GetFileSize() const {
  struct stat st;
  if (::fstat(fd_, &st) != 0) {
    throw IOException("Failed to get file size for \"%s\": %s", path.c_str(),
                      strerror(errno));
  }
  return st.st_size;
}

unique_ptr<FileHandle> OpenFile(const std::string& path, const char* mode) {
  int flags = 0;
  mode_t file_mode = 0644;

  if (mode[0] == 'r') {
    flags = O_RDONLY;
  } else if (mode[0] == 'w') {
    flags = O_RDWR | O_CREAT;
  } else if (mode[0] == 'c') {
    flags = O_RDWR | O_CREAT | O_TRUNC;
  }

  int fd = ::open(path.c_str(), flags, file_mode);
  if (fd < 0) {
    throw IOException("Failed to open file \"%s\": %s", path.c_str(),
                      strerror(errno));
  }
  return make_uniq<FileHandle>(fd, path);
}

}  // namespace duckdb
