//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./ParallelBuffer.h"

// _________________________________________________________________________
void ParallelFileBuffer::open(const string& filename) {
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
  auto rawInput = rawBuffer_.getNextBlock();
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

  auto endPosition = findRegexNearEnd(rawInput.value(), endRegex_);
  if (!endPosition) {
    if (rawBuffer_.getNextBlock()) {
      throw std::runtime_error(absl::StrCat(
          "The regex \"", endRegexAsString_,
          "\" which marks the end of a statement was not found at "
          "all within a single batch that was not the last one. Please "
          "increase the FILE_BUFFER_SIZE "
          "or set \"parallel-parsing: false\" in the settings file."));
    }
    // This was the last (possibly incomplete) block, simply concatenate
    endPosition = rawInput->size();
    exhausted_ = true;
  }
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
