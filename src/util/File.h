// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "./Exception.h"
#include "Log.h"
#include "Timer.h"

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

namespace ad_utility {
//! Wrapper class for file access. Is supposed to provide
//! methods that allow fast access to binary files.
//! Also features methods for dealing with ASCII files
//! but is less efficient then stl functions due
//! to the lack of buffering.
//! Many methods are copies from the CompleteSearch File.h
class File {
 private:
  string _name;
  FILE* _file;

 public:
  //! Default constructor
  File() {
    _file = NULL;
    _name = "";
  }

  //! Constructor that creates an instance from the
  //! file system.
  File(const char* filename, const char* mode) : _name(filename) {
    open(filename, mode);
  }

  File(const string& filename, const char* mode) : _name(filename) {
    open(filename, mode);
  }

  // TODO<joka921> This copies non-owning pointer semantics which is really
  // dangerous. This operator should be deleted.
  File& operator=(const File&) = default;

  File& operator=(File&& rhs) noexcept {
    _file = rhs._file;
    rhs._file = nullptr;
    _name = std::move(rhs._name);
    return *this;
  }

  File(File&& rhs) : _name{std::move(rhs._name)}, _file{rhs._file} {
    rhs._file = nullptr;
  }

  // TODO<joka921> should also be deleted!
  //! Copy constructor.
  //! Does not copy the file in the file system!
  File(const File& orig) {
    _name = orig._name;
    open(_name.c_str(), "r");
    assert(_file);
  }

  //! Destructor closes file if still open
  ~File() {
    if (isOpen()) close();
  }

  //! OPEN FILE (exit with error if fails, returns true otherwise)
  bool open(const char* filename, const char* mode) {
    _file = fopen(filename, mode);
    if (_file == NULL) {
      std::stringstream err;
      err << "! ERROR opening file \"" << filename << "\" with mode \"" << mode
          << "\" (" << strerror(errno) << ")" << endl
          << endl;
      throw std::runtime_error(err.str());
    }
    _name = filename;
    return true;
  }

  // overload  for c++ strings
  bool open(const string& filename, const char* mode) {
    return open(filename.c_str(), mode);
  }

  //! checks if the file is open.
  [[nodiscard]] bool isOpen() const { return (_file != NULL); }

  //! Close file.
  bool close() {
    if (not isOpen()) {
      return true;
    }
    if (fclose(_file) != 0) {
      cout << "! ERROR closing file \"" << _name << "\" (" << strerror(errno)
           << ")" << endl
           << endl;
      exit(1);
    }
    _file = NULL;
    return true;
  }

  bool empty() { return sizeOfFile() == 0; }

  // read from current file pointer position
  // returns the number of bytes read
  size_t readFromBeginning(void* targetBuffer, size_t nofBytesToRead) {
    return read(targetBuffer, nofBytesToRead, (off_t)0);
  }

  // read from current file pointer position
  // returns the number of bytes read
  size_t read(void* targetBuffer, size_t nofBytesToRead) {
    assert(_file);
    return fread(targetBuffer, (size_t)1, nofBytesToRead, _file);
  }

  // Reads a line from the file into param line and interprets it as ASCII.
  // Removes the \n from the end of the string.
  // Returns null on EOF when no chars have been read.
  // Throws an Exception if the buffer is not sufficiently large.
  // Warning: std::getTriple which works on a file stream
  // is generally more performant (it uses a general stream buffer
  // to wrap the file)
  // and appeds char by char to the string.
  bool readLine(string* line, char* buf, size_t bufferSize) {
    assert(_file);
    char* retVal = fgets(buf, bufferSize, _file);
    size_t stringLength = strlen(buf);
    if (retVal && stringLength > 0 && buf[stringLength - 1] == '\n') {
      // Remove the trailing \n
      buf[stringLength - 1] = 0;
    } else if (retVal && !isAtEof()) {
      // Stopped inside a line because the end of the buffer was reached.
      AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
               "Buffer too small when reading from file: " + _name +
                   ". "
                   "Or the line contains a 0 character.");
    }
    *line = buf;
    return retVal;
  }

  // write to current file pointer position
  // returns number of bytes written
  size_t write(const void* sourceBuffer, size_t nofBytesToWrite) {
    assert(_file);
    return fwrite(sourceBuffer, (size_t)1, nofBytesToWrite, _file);
  }

  // write to current file pointer position
  // returns number of bytes written
  void writeLine(const string& line) {
    assert(_file);
    write(line.c_str(), line.size());
    write("\n", 1);
  }

  void flush() { fflush(_file); }

  bool isAtEof() {
    assert(_file);
    return feof(_file) != 0;
  }

  //! Read an ASCII file into a vector of strings.
  //! Each line represents an entry.
  void readIntoVector(vector<string>* target, char* buf, size_t bufferSize) {
    string line;
    while (readLine(&line, buf, bufferSize)) {
      target->push_back(line);
    }
  }

  //! Seeks a position in the file.
  //! Sets the file position indicator for the stream.
  //! The new position is obtained by adding seekOffset
  //! bytes to the position seekOrigin.
  //! Returns true on success
  bool seek(off_t seekOffset, int seekOrigin) {
    assert((seekOrigin == SEEK_SET) || (seekOrigin == SEEK_CUR) ||
           (seekOrigin == SEEK_END));
    assert(_file);
    return fseeko(_file, seekOffset, seekOrigin) == 0;
  }

  //! Read nofBytesToRead bytes from file starting at the given offset.
  //! Returns the number of bytes read or the error returned by pread()
  //! which is < 0
  ssize_t read(void* targetBuffer, size_t nofBytesToRead, off_t offset,
               ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) {
    assert(_file);
    const int fd = fileno(_file);
    size_t bytesRead = 0;
    auto* to = static_cast<uint8_t*>(targetBuffer);
    size_t batchSize =
        timer ? 1024 * 1024 * 64 : std::numeric_limits<size_t>::max();
    while (bytesRead < nofBytesToRead) {
      size_t toRead = std::min(nofBytesToRead - bytesRead, batchSize);

      const ssize_t ret = pread(fd, to + bytesRead, toRead, offset + bytesRead);

      if (timer) {
        timer->wlock()->checkTimeoutAndThrow();
      }
      if (ret < 0) {
        return ret;
      }
      bytesRead += ret;
    }
    return bytesRead;
  }

  //! get the underlying file descriptor
  [[nodiscard]] int getFileDescriptor() const { return fileno(_file); }

  //! Returns the number of bytes from the beginning
  //! is 0 on opening. Later equal the number of bytes written.
  //! -1 is returned when an error occurs
  [[nodiscard]] off_t tell() const {
    assert(_file);
    off_t returnValue = ftello(_file);
    if (returnValue == (off_t)-1) {
      cerr << "\n ERROR in tell() : " << strerror(errno) << endl << endl;
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
    assert(_file);
    // read the last off_t
    const off_t lastOffsetOffset = sizeOfFile() - sizeof(off_t);
    read(lastOffset, sizeof(off_t), lastOffsetOffset);

    return lastOffsetOffset;
  }

  // Static method to check if a file exists.
  static bool exists(const std::filesystem::path& path) {
    return std::filesystem::exists(path);
  }
};

/**
 * @brief Delete the file at a given path
 * @param path
 */
inline void deleteFile(const std::filesystem::path& path) {
  if (!std::filesystem::remove(path)) {
    LOG(WARN) << "Deletion of file '" << path << "' was not successful"

              << std::endl;
  }
}

}  // namespace ad_utility
