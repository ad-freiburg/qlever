// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/ParallelBuffer.h"

#include "util/AsyncStream.h"
#include "util/File.h"
#include "util/StringUtils.h"

namespace {
// A range that synchronously reads blocks of (at most) `blocksize` bytes from
// `file` until the end of the file is reached.
struct FileBlockReader
    : ad_utility::InputRangeFromGet<ParallelBuffer::BufferType> {
  FileBlockReader(ad_utility::File file, size_t blocksize)
      : file_{std::move(file)}, blocksize_{blocksize} {}

  std::optional<ParallelBuffer::BufferType> get() override {
    ParallelBuffer::BufferType buf(blocksize_);
    auto numBytesRead = file_.read(buf.data(), blocksize_);
    if (numBytesRead == 0) {
      return std::nullopt;
    }
    buf.resize(numBytesRead);
    return buf;
  }

 private:
  ad_utility::File file_;
  size_t blocksize_;
};
}  // namespace

// _________________________________________________________________________
ParallelFileBuffer::ParallelFileBuffer(size_t blocksize,
                                       const std::string& filename)
    : ParallelBuffer{blocksize},
      stream_{ad_utility::streams::runStreamAsync(
          FileBlockReader{ad_utility::File{filename, "r"}, blocksize}, 1)} {}

// ___________________________________________________________________________
std::optional<ParallelBuffer::BufferType> ParallelFileBuffer::getNextBlock() {
  return stream_.get();
}

// _____________________________________________________________________________
std::optional<ParallelBuffer::BufferType>
ParallelBufferWithEndRegex::getNextBlock() {
  // Get the block of data read asynchronously after the previous call
  // to `getNextBlock`.
  auto rawInput = rawBuffer_->getNextBlock();

  // If there was no more data, return the remainder or `std::nullopt` if
  // it is empty.
  if (!rawInput || exhausted_) {
    exhausted_ = true;
    if (remainder_.empty()) {
      return std::nullopt;
    }
    auto copy = std::move(remainder_);
    // The C++ standard does not require that remainder_ is empty after the
    // move, but we need it to be empty to make the logic above work.
    remainder_.clear();
    return copy;
  }

  auto endPosition =
      findEndPosition_(std::string_view{rawInput->data(), rawInput->size()});

  // If no end of a statement was found at all, report an error, except when
  // this is the last block (then `getNextBlock` will return `std::nullopt`, and
  // we simply concatenate it to the remainder).
  if (!endPosition) {
    if (rawBuffer_->getNextBlock()) {
      throw std::runtime_error(absl::StrCat(
          "The pattern ", description_,
          " which marks the end of a statement was not found in the current "
          "input batch (that was not the last one) of size ",
          ad_utility::insertThousandSeparator(std::to_string(rawInput->size()),
                                              ','),
          "; possible fixes are: "
          "use `--parser-buffer-size` to increase the buffer size or "
          "use `--parallel-parsing false` to disable parallel parsing"));
    }
    endPosition = rawInput->size();
    exhausted_ = true;
  }

  // Concatenate the remainder (part after `endRegex_`) of the block from the
  // previous round with the part of the block until `endRegex_` from this
  // round.
  BufferType result;
  result.reserve(remainder_.size() + *endPosition);
  result.insert(result.end(), remainder_.begin(), remainder_.end());
  result.insert(result.end(), rawInput->begin(),
                rawInput->begin() + *endPosition);
  remainder_.clear();
  remainder_.insert(remainder_.end(), rawInput->begin() + *endPosition,
                    rawInput->end());
  return result;
}
