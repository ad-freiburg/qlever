//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./ParallelBuffer.h"

// _________________________________________________________________________
void ParallelFileBuffer::open(const string& filename) {
  _file.open(filename, "r");
  _eof = false;
  _buf.resize(_blocksize);
  auto task = [&file = this->_file, bs = this->_blocksize,
               &buf = this->_buf]() { return file.read(buf.data(), bs); };
  _fut = std::async(task);
}

// ___________________________________________________________________________
std::optional<ParallelBuffer::BufferType> ParallelFileBuffer::getNextBlock() {
  if (_eof) {
    return std::nullopt;
  }

  AD_CONTRACT_CHECK(_file.isOpen() && _fut.valid());
  auto numBytesRead = _fut.get();
  if (numBytesRead == 0) {
    _eof = true;
    return std::nullopt;
  }
  _buf.resize(numBytesRead);
  std::optional<BufferType> ret = std::move(_buf);

  _buf.resize(_blocksize);
  auto getNextBlock = [&file = this->_file, bs = this->_blocksize,
                       &buf = this->_buf]() {
    return file.read(buf.data(), bs);
  };
  _fut = std::async(getNextBlock);

  return ret;
}

// ____________________________________________________________________________
std::optional<size_t> ParallelBufferWithEndRegex::findRegexNearEnd(
    const BufferType& vec, const re2::RE2& regex) {
  size_t chunkSize = 1000;
  size_t inputSize = vec.size();
  re2::StringPiece regexResult;
  bool match = false;
  while (true) {
    if (chunkSize >= inputSize) {
      break;
    }

    auto startIdx = inputSize - chunkSize;
    auto regexInput = re2::StringPiece{vec.data() + startIdx, chunkSize};

    match = RE2::PartialMatch(regexInput, regex, &regexResult);
    if (match) {
      break;
    }

    if (chunkSize == inputSize - 1) {
      break;
    }
    chunkSize = std::min(chunkSize * 2, inputSize - 1);
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
  auto rawInput = _rawBuffer.getNextBlock();
  if (!rawInput || _exhausted) {
    _exhausted = true;
    if (_remainder.empty()) {
      return std::nullopt;
    }
    auto copy = std::move(_remainder);
    // The C++ standard does not require that _remainder is empty after the
    // move, but we need it to be empty to make the logic above work.
    _remainder.clear();
    return copy;
  }

  auto endPosition = findRegexNearEnd(rawInput.value(), _endRegex);
  if (!endPosition) {
    if (_rawBuffer.getNextBlock()) {
      throw std::runtime_error(
          "The regex which marks the end of a statement was not found at "
          "all within a single batch that was not the last one. Please "
          "increase the FILE_BUFFER_SIZE "
          "or choose a different parser");
    }
    // This was the last (possibly incomplete) block, simply concatenate
    endPosition = rawInput->size();
    _exhausted = true;
  }
  BufferType result;
  result.reserve(_remainder.size() + *endPosition);
  result.insert(result.end(), _remainder.begin(), _remainder.end());
  result.insert(result.end(), rawInput->begin(),
                rawInput->begin() + *endPosition);
  _remainder.clear();
  _remainder.insert(_remainder.end(), rawInput->begin() + *endPosition,
                    rawInput->end());
  return result;
}
