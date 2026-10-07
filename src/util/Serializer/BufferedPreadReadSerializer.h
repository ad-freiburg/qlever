// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDPREADREADSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDPREADREADSERIALIZER_H

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>

#include "backports/memory.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {

// A `ReadSerializer` that reads a file sequentially, starting at an arbitrary
// byte offset, through a buffer of fixed size. The buffer is refilled via
// positioned reads (`pread`, see the corresponding overload of
// `ad_utility::File::read`), which neither use nor change the file position.
// Several instances can therefore read the same (shared) file concurrently and
// independently of each other, each from its own position.
//
// A single read that is larger than the buffer (for example a long string) is
// supported: the bytes that are still in the buffer are copied, and the rest
// is read directly into the target without going through the buffer.
class BufferedPreadReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;

 private:
  std::shared_ptr<const ad_utility::File> file_;
  size_t bufferCapacity_;
  std::unique_ptr<char[]> buffer_;
  // The byte offset in the file that corresponds to the start of the
  // `buffer_`.
  uint64_t bufferStartOffset_;
  // The number of valid bytes in the `buffer_`.
  size_t bufferSize_ = 0;
  // The position of the next byte to be read within the `buffer_`.
  size_t positionInBuffer_ = 0;

 public:
  // Create a serializer that reads the `file` starting at the byte offset
  // `startOffset` through a buffer of `bufferSize` bytes.
  BufferedPreadReadSerializer(std::shared_ptr<const ad_utility::File> file,
                              uint64_t startOffset, MemorySize bufferSize)
      : file_{std::move(file)},
        bufferCapacity_{bufferSize.getBytes()},
        // NOTE: `make_unique_for_overwrite` (as opposed to `make_unique`)
        // doesn't zero-initialize the buffer.
        buffer_{ql::make_unique_for_overwrite<char[]>(bufferCapacity_)},
        bufferStartOffset_{startOffset} {
    AD_CONTRACT_CHECK(file_ != nullptr && file_->isOpen());
    AD_CONTRACT_CHECK(bufferCapacity_ > 0);
  }

  // Read the next `numBytes` bytes into `target`. Throw a
  // `SerializationException` if the end of the file is reached before.
  void serializeBytes(char* target, size_t numBytes) {
    // First copy what is still available in the buffer.
    size_t numFromBuffer = std::min(numBytes, bufferSize_ - positionInBuffer_);
    std::memcpy(target, buffer_.get() + positionInBuffer_, numFromBuffer);
    positionInBuffer_ += numFromBuffer;
    target += numFromBuffer;
    numBytes -= numFromBuffer;
    if (numBytes == 0) {
      return;
    }
    // The buffer is now exhausted. If the remaining request is at least as
    // large as the buffer, read it directly into the `target`, which saves a
    // copy and works for requests of any size.
    if (numBytes >= bufferCapacity_) {
      uint64_t offset = getSerializationPosition();
      readExactly(target, numBytes, offset);
      bufferStartOffset_ = offset + numBytes;
      bufferSize_ = 0;
      positionInBuffer_ = 0;
      return;
    }
    refill();
    if (bufferSize_ < numBytes) {
      throwEndOfFile();
    }
    std::memcpy(target, buffer_.get(), numBytes);
    positionInBuffer_ = numBytes;
  }

  // Return the byte offset in the file of the next byte that will be read.
  uint64_t getSerializationPosition() const {
    return bufferStartOffset_ + positionInBuffer_;
  }

 private:
  // Discard the (completely consumed) buffer and fill it with the bytes that
  // directly follow it in the file. Near the end of the file, fewer than
  // `bufferCapacity_` bytes may be read.
  void refill() {
    AD_CORRECTNESS_CHECK(positionInBuffer_ == bufferSize_);
    bufferStartOffset_ += bufferSize_;
    ssize_t numRead = file_->read(buffer_.get(), bufferCapacity_,
                                  static_cast<off_t>(bufferStartOffset_));
    throwIfReadFailed(numRead);
    bufferSize_ = static_cast<size_t>(numRead);
    positionInBuffer_ = 0;
  }

  // Read exactly `numBytes` bytes at the given `offset` into the `target`, and
  // throw if that is not possible.
  void readExactly(char* target, size_t numBytes, uint64_t offset) const {
    ssize_t numRead = file_->read(target, numBytes, static_cast<off_t>(offset));
    throwIfReadFailed(numRead);
    if (static_cast<size_t>(numRead) < numBytes) {
      throwEndOfFile();
    }
  }

  // Throw if `numRead` (the result of `File::read`) signals an error.
  void throwIfReadFailed(ssize_t numRead) const {
    if (numRead < 0) {
      throw SerializationException{
          absl::StrCat("Reading from the file `", file_->name(), "` failed")};
    }
  }

  // Throw the exception for a read past the end of the file.
  [[noreturn]] void throwEndOfFile() const {
    throw SerializationException{
        absl::StrCat("Tried to read past the end of the file `", file_->name(),
                     "` with a `BufferedPreadReadSerializer`")};
  }
};

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_BUFFEREDPREADREADSERIALIZER_H
