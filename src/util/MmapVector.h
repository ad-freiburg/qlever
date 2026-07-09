// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_MMAPVECTOR_H
#define QLEVER_SRC_UTIL_MMAPVECTOR_H

#include <cstdint>
#include <exception>

#include "util/File.h"

namespace ad_utility {
// _________________________________________________________________________
class InvalidFileException : public std::exception {
  const char* what() const noexcept override {
    return "Error reading meta data of  Mmap file: Maybe magic number is "
           "missing or there is a version mismatch\n";
  }
};

// The metadata trailer that the (now removed) `MmapVector` stored at the very
// end of its backing file, and that QLever still writes and reads to stay
// on-disk compatible with files produced by older versions. After the array
// data (and possibly some unused capacity) follow the `size`, `capacity`, and
// `bytesize` of the array, plus a magic number and a version that are used for
// validation when reading. Only `size_` is actually consumed by the current
// readers; `capacity_` and `bytesize_` are retained solely for compatibility.
struct MmapVectorMetaData {
  uint64_t size_ = 0;
  uint64_t capacity_ = 0;
  uint64_t bytesize_ = 0;

  static constexpr uint32_t magicNumber_ = 7601577;
  static constexpr uint32_t version_ = 0;
  // The total number of bytes that the trailer occupies on disk.
  static constexpr off_t numBytes = 3 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

  // Append the trailer to the current write position of `file`, which has to be
  // positioned at the end of the array data.
  void writeToFile(File& file) const {
    file.write(&size_, sizeof(size_));
    file.write(&capacity_, sizeof(capacity_));
    file.write(&bytesize_, sizeof(bytesize_));
    file.write(&magicNumber_, sizeof(magicNumber_));
    file.write(&version_, sizeof(version_));
  }

  // Read the trailer from the end of `file` and validate the magic number and
  // the version. Throws an `InvalidFileException` if the file is too small or
  // if the magic number or the version do not match. Note that the offsets
  // themselves are read separately, so only the trailer is touched here.
  static MmapVectorMetaData readFromFile(File& file) {
    const off_t fileSize = file.sizeOfFile();
    if (fileSize < numBytes) {
      throw InvalidFileException();
    }
    off_t offset = fileSize - numBytes;
    auto readField = [&file, &offset](auto& field) {
      file.read(&field, sizeof(field), offset);
      offset += static_cast<off_t>(sizeof(field));
    };
    MmapVectorMetaData metaData;
    uint32_t magicNumber = 0;
    uint32_t version = 0;
    readField(metaData.size_);
    readField(metaData.capacity_);
    readField(metaData.bytesize_);
    readField(magicNumber);
    readField(version);
    if (magicNumber != magicNumber_ || version != version_) {
      throw InvalidFileException{};
    }
    return metaData;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MMAPVECTOR_H
