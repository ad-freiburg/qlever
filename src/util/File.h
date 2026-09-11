// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef QLEVER_SRC_UTIL_FILE_H
#define QLEVER_SRC_UTIL_FILE_H

#include <absl/strings/str_cat.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>

#include "backports/filesystem.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Log.h"

namespace ad_utility {
//! Wrapper class for file access. Is supposed to provide
//! methods that allow fast access to binary files.
//! Also features methods for dealing with ASCII files
//! but is less efficient then stl functions due
//! to the lack of buffering.
//! Many methods are copies from the CompleteSearch File.h
class File {
 private:
  using string = std::string;

  string name_;
  FILE* file_;

 public:
  //! Default constructor
  File() {
    file_ = NULL;
    name_ = "";
  }

  //! Constructor that creates an instance from the
  //! file system.
  File(const char* filename, const char* mode) : name_(filename) {
    open(filename, mode);
  }

  File(const string& filename, const char* mode) : name_(filename) {
    open(filename, mode);
  }

  // Files are move-only types.
  File(const File&) = delete;
  File& operator=(const File&) = delete;

  File& operator=(File&& rhs) noexcept {
    if (isOpen()) {
      close();
    }

    file_ = std::exchange(rhs.file_, nullptr);
    name_ = std::move(rhs.name_);
    return *this;
  }

  File(File&& rhs) noexcept
      : name_{std::move(rhs.name_)}, file_{std::exchange(rhs.file_, nullptr)} {}

  //! Destructor closes file if still open
  ~File() {
    if (isOpen()) close();
  }

  //! OPEN FILE (exit with error if fails, returns true otherwise)
  bool open(const char* filename, const char* mode) {
    file_ = fopen(filename, mode);
    if (file_ == NULL) {
      std::stringstream err;
      err << "! ERROR opening file \"" << filename << "\" with mode \"" << mode
          << "\" (" << strerror(errno) << ")" << std::endl;
      throw std::runtime_error(std::move(err).str());
    }
    name_ = filename;
    return true;
  }

  // overload  for c++ strings
  bool open(const string& filename, const char* mode) {
    return open(filename.c_str(), mode);
  }

  //! checks if the file is open.
  [[nodiscard]] bool isOpen() const { return (file_ != NULL); }

  //! Return the underlying file descriptor.
  [[nodiscard]] int fd() const {
    assert(file_);
    return fileno(file_);
  }

  // Return a new `File` for the same underlying file by duplicating the file
  // descriptor. In contrast to opening the file again by name, this also
  // works when the file has been renamed since it was opened (which happens
  // to the files of the active index when a rebuilt index is swapped in at
  // runtime, see #2832).
  //
  // NOTE: The duplicate shares the file offset with the original, so on the
  // two `File`s only the positioned `read` overload (which uses `pread`) can
  // be used independently; the sequential `read`/`seek` interface must not
  // be mixed across duplicates.
  [[nodiscard]] File duplicateForReading() const {
    AD_CONTRACT_CHECK(isOpen());
    int newFd = ::dup(fd());
    AD_CONTRACT_CHECK(newFd != -1, "Duplicating the file descriptor for file ",
                      name_, " failed");
    FILE* newFile = ::fdopen(newFd, "r");
    if (newFile == nullptr) {
      ::close(newFd);
    }
    AD_CONTRACT_CHECK(newFile != nullptr);
    File result;
    result.name_ = name_;
    result.file_ = newFile;
    return result;
  }

  //! Close file.
  bool close() {
    if (not isOpen()) {
      return true;
    }
    if (fclose(file_) != 0) {
      std::cout << "! ERROR closing file \"" << name_ << "\" ("
                << strerror(errno) << ")" << std::endl;
      exit(1);
    }
    file_ = NULL;
    return true;
  }

  bool empty() { return sizeOfFile() == 0; }

  const std::string& name() const { return name_; }

  // read from current file pointer position
  // returns the number of bytes read
  size_t read(void* targetBuffer, size_t nofBytesToRead) {
    assert(file_);
    return fread(targetBuffer, (size_t)1, nofBytesToRead, file_);
  }

  // write to current file pointer position
  // returns number of bytes written
  size_t write(const void* sourceBuffer, size_t nofBytesToWrite) {
    assert(file_);
    return fwrite(sourceBuffer, (size_t)1, nofBytesToWrite, file_);
  }

  void flush() { fflush(file_); }

  //! Seeks a position in the file.
  //! Sets the file position indicator for the stream.
  //! The new position is obtained by adding seekOffset
  //! bytes to the position seekOrigin.
  //! Returns true on success
  bool seek(off_t seekOffset, int seekOrigin) {
    assert((seekOrigin == SEEK_SET) || (seekOrigin == SEEK_CUR) ||
           (seekOrigin == SEEK_END));
    assert(file_);
    return fseeko(file_, seekOffset, seekOrigin) == 0;
  }

  //! Read nofBytesToRead bytes from file starting at the given offset.
  //! Returns the number of bytes read or the error returned by pread()
  //! which is < 0
  ssize_t read(void* targetBuffer, size_t nofBytesToRead, off_t offset) const {
    assert(file_);
    const int fd = fileno(file_);
    size_t bytesRead = 0;
    auto* to = static_cast<uint8_t*>(targetBuffer);
    while (bytesRead < nofBytesToRead) {
      size_t toRead = nofBytesToRead - bytesRead;

      const ssize_t ret = pread(fd, to + bytesRead, toRead, offset + bytesRead);

      if (ret < 0) {
        return ret;
      }
      bytesRead += ret;
    }
    return bytesRead;
  }

  //! Returns the number of bytes from the beginning
  //! is 0 on opening. Later equal the number of bytes written.
  //! -1 is returned when an error occurs
  [[nodiscard]] off_t tell() const {
    assert(file_);
    off_t returnValue = ftello(file_);
    if (returnValue == (off_t)-1) {
      std::cerr << "\n ERROR in tell() : " << strerror(errno) << std::endl;
      exit(1);
    }
    return returnValue;
  }

  //! Returns the size of the file as off_t.
  off_t sizeOfFile() {
    seek((off_t)0, SEEK_END);
    off_t sizeOfFile = tell();
    return sizeOfFile;
  }

  // Return the byte offset of the last offset and
  // the offset itself.
  std::pair<off_t, off_t> getLastOffset() {
    AD_CONTRACT_CHECK(file_);
    // Read the last off_t.
    const off_t lastOffsetOffset = sizeOfFile() - sizeof(off_t);
    off_t lastOffset;
    read(&lastOffset, sizeof(off_t), lastOffsetOffset);

    return {lastOffsetOffset, lastOffset};
  }
};

/**
 * @brief Delete the file at a given path
 * @param path
 */
inline void deleteFile(const ql::filesystem::path& path,
                       bool warnOnFailure = true) {
  if (!ql::filesystem::remove(path)) {
    if (warnOnFailure) {
      AD_LOG_WARN << "Deletion of file '" << path << "' was not successful"
                  << std::endl;
    }
  }
}

namespace detail {
template <typename Stream, bool forWriting, typename... Args>
Stream makeFilestream(const ql::filesystem::path& path, Args&&... args) {
  Stream stream{path.string(), AD_FWD(args)...};
  std::string_view mode = forWriting ? "for writing" : "for reading";
  if (!stream.is_open()) {
    std::string error =
        absl::StrCat("Could not open file \"", path.string(), "\" ", mode,
                     ". Possible causes: The file does not exist or the "
                     "permissions are insufficient. The absolute path is \"",
                     ql::filesystem::absolute(path).string(), "\".");
    throw std::runtime_error{error};
  }
  return stream;
}
}  // namespace detail

// Open and return a std::ifstream from a given filename and optional
// additional `args`. Throw an exception stating the filename and the absolute
// path when the file can't be opened.
template <typename... Args>
std::ifstream makeIfstream(const ql::filesystem::path& path, Args&&... args) {
  return detail::makeFilestream<std::ifstream, false>(path, AD_FWD(args)...);
}

// Similar to `makeIfstream`, but returns `std::ofstream`
template <typename... Args>
std::ofstream makeOfstream(const ql::filesystem::path& path, Args&&... args) {
  return detail::makeFilestream<std::ofstream, true>(path, AD_FWD(args)...);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FILE_H
