//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./ParallelBuffer.h"

#include "util/StringUtils.h"

// _________________________________________________________________________
void ParallelFileBuffer::open(const std::string& filename) {
  file_.open(filename, "r");
  eof_ = false;
  buf_.resize(blocksize_);
  auto task = [&file = this->file_, bs = this->blocksize_,
               &buf = this->buf_]() { return file.read(buf.data(), bs); };
  fut_ = std::async(task);
}

// ___________________________________________________________________________
std::optional<ParallelBuffer::BufferType> ParallelFileBuffer::getNextBlock() {
  AD_CONTRACT_CHECK(file_.isOpen());
  if (eof_) {
    return std::nullopt;
  }
  AD_CORRECTNESS_CHECK(fut_.valid());
  auto numBytesRead = fut_.get();
  if (numBytesRead == 0) {
    eof_ = true;
    return std::nullopt;
  }
  buf_.resize(numBytesRead);
  std::optional<BufferType> ret = std::move(buf_);

  buf_.resize(blocksize_);
  auto getNextBlock = [&file = this->file_, bs = this->blocksize_,
                       &buf = this->buf_]() {
    return file.read(buf.data(), bs);
  };
  fut_ = std::async(getNextBlock);

  return ret;
}

// ____________________________________________________________________________
std::optional<size_t> ParallelBufferWithEndRegex::findRegexNearEnd(
    const BufferType& vec, const re2::RE2& regex) {
  size_t inputSize = vec.size();
  AD_CORRECTNESS_CHECK(inputSize > 0);
  size_t chunkSize = std::min(1000UL, inputSize);
  re2::StringPiece regexResult;
  bool match = false;
  while (true) {
    auto startIdx = inputSize - chunkSize;
    auto regexInput = re2::StringPiece{vec.data() + startIdx, chunkSize};

    match = RE2::PartialMatch(regexInput, regex, &regexResult);
    if (match) {
      break;
    }

    if (chunkSize == inputSize) {
      break;
    }
    chunkSize = std::min(chunkSize * 2, inputSize);
  }
  if (!match) {
    return std::nullopt;
  }

  // regexResult.data() is a pointer to the beginning of the match, vec.data()
  // is a pointer to the beginning of the total input.
  return regexResult.data() + regexResult.size() - vec.data();
}

// _____________________________________________________________________________
std::optional<ParallelBuffer::BufferType>
ParallelBufferWithEndRegex::getNextBlock() {
  // Get the block of data read asynchronously after the previous call
  // to `getNextBlock`.
  auto rawInput = rawBuffer_.getNextBlock();

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

  // Find `endRegex_` in the data (searching from the back, in chunks of
  // exponentially increasing size). Note that this does not necessarily
  // find the last match of `endRegex_` in the data, but the first match in the
  // last chunk (from the back), where there is a match.
  auto endPosition = findRegexNearEnd(rawInput.value(), endRegex_);

  // If no match was found at all, report an error, except when this is the
  // last block (then `getNextBlock` will return `std::nullopt`, and we simply
  // concatenate it to the remainder).
  if (!endPosition) {
    if (rawBuffer_.getNextBlock()) {
      throw std::runtime_error(absl::StrCat(
          "The regex ", endRegexAsString_,
          " which marks the end of a statement was not found in the current "
          "input batch (that was not the last one) of size ",
          ad_utility::insertThousandSeparator(std::to_string(rawInput->size()),
                                              ','),
          "; possible fixes are: "
          "use `--parser-buffer-size` to increase the buffer size or "
          "use `--parse-parallel false` to disable parallel parsing"));
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
