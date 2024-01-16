// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once
#include <absl/strings/str_cat.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

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

  File& operator=(File&& rhs) noexcept {
    if (isOpen()) {
      close();
    }

    file_ = rhs.file_;
    rhs.file_ = nullptr;
    name_ = std::move(rhs.name_);
    return *this;
  }

  File(File&& rhs) : name_{std::move(rhs.name_)}, file_{rhs.file_} {
    rhs.file_ = nullptr;
  }

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

  // returns the byte offset of the last off_t
  // the off_t itself is passed back by reference
  off_t getLastOffset(off_t* lastOffset) {
    assert(file_);
    // read the last off_t
    const off_t lastOffsetOffset = sizeOfFile() - sizeof(off_t);
    read(lastOffset, sizeof(off_t), lastOffsetOffset);

    return lastOffsetOffset;
  }
};

/**
 * @brief Delete the file at a given path
 * @param path
 */
inline void deleteFile(const std::filesystem::path& path,
                       bool warnOnFailure = true) {
  if (!std::filesystem::remove(path)) {
    if (warnOnFailure) {
      LOG(WARN) << "Deletion of file '" << path << "' was not successful"
                << std::endl;
    }
  }
}

namespace detail {
template <typename Stream, bool forWriting>
Stream makeFilestream(const std::filesystem::path& path, auto&&... args) {
  Stream stream{path.string(), AD_FWD(args)...};
  std::string_view mode = forWriting ? "for writing" : "for reading";
  if (!stream.is_open()) {
    std::string error =
        absl::StrCat("Could not open file \"", path.string(), "\" ", mode,
                     ". Possible causes: The file does not exist or the "
                     "permissions are insufficient. The absolute path is \"",
                     std::filesystem::absolute(path).string(), "\".");
    throw std::runtime_error{error};
  }
  return stream;
}
}  // namespace detail

// Open and return a std::ifstream from a given filename and optional
// additionals `args`. Throw an exception stating the filename and the absolute
// path when the file can't be opened.
std::ifstream makeIfstream(const std::filesystem::path& path, auto&&... args) {
  return detail::makeFilestream<std::ifstream, false>(path, AD_FWD(args)...);
}

// Similar to `makeIfstream`, but returns `std::ofstream`
std::ofstream makeOfstream(const std::filesystem::path& path, auto&&... args) {
  return detail::makeFilestream<std::ofstream, true>(path, AD_FWD(args)...);
}

}  // namespace ad_utility
