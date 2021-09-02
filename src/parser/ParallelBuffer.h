// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once
#include <re2/re2.h>

#include <future>
#include <optional>
#include <string>
#include <vector>

#include "../util/File.h"

/**
 * @brief Abstract base class for certain input buffers.
 *
 * Is able to return a whole block of bytes from some kind of
 * input file/stream via the call to getNextBlock()
 * If It is computationally expensive to get the next bytes (e.g.
 * because they have to be decompressed) this can be done asynchronously
 * between two calls to getNextBlock.
 */
class ParallelBuffer {
 public:
  ParallelBuffer() = default;
  /**
   * @brief Specify the size of the blocks that are to be retrieved
   * @param blocksize  the blocksize (in bytes)
   */
  ParallelBuffer(size_t blocksize) : _blocksize(blocksize) {}
  virtual ~ParallelBuffer() = default;
  /**
   * @brief Open a file (in the sense of unix files, can also be a
   * (network/pipe) stream.
   * @param filename Name of the file which is to be opened.
   */
  virtual void open(const string& filename) = 0;
  /**
   * @brief Get (approximately) the next _blocksize bytes from the inut stream.
   *
   * Only valid after a call to open().
   *
   * @return The next bytes or std::nullopt to signal EOF.
   */
  virtual std::optional<std::vector<char>> getNextBlock() = 0;

  const size_t& blocksize() const { return _blocksize; }

 protected:
  size_t _blocksize = 100 * (2 << 20);
};

/**
 * @brief Read the bytes from a file/stream and just pass them directly without
 * any modification.
 *
 * The next block is also read in parallel in case we have a
 * really slow filesystem etc.
 */
class ParallelFileBuffer : public ParallelBuffer {
 public:
  ParallelFileBuffer() : ParallelBuffer(){};
  ParallelFileBuffer(size_t blocksize) : ParallelBuffer(blocksize) {}

  virtual void open(const string& filename) override {
    _file.open(filename, "r");
    _eof = false;
    _buf.resize(_blocksize);
    auto task = [&file = this->_file, bs = this->_blocksize,
                 &buf = this->_buf]() { return file.read(buf.data(), bs); };
    _fut = std::async(task);
  }

  // _____________________________________________________
  virtual std::optional<std::vector<char>> getNextBlock() override {
    if (_eof) {
      return std::nullopt;
    }

    AD_CHECK(_file.isOpen() && _fut.valid());
    auto numBytesRead = _fut.get();
    if (numBytesRead == 0) {
      _eof = true;
      return std::nullopt;
    }
    _buf.resize(numBytesRead);
    std::optional<std::vector<char>> ret = std::move(_buf);

    _buf.resize(_blocksize);
    auto task = [&file = this->_file, bs = this->_blocksize,
                 &buf = this->_buf]() { return file.read(buf.data(), bs); };
    _fut = std::async(task);

    return ret;
  }

 private:
  ad_utility::File _file;
  bool _eof = true;
  std::vector<char> _buf;
  std::future<size_t> _fut;
};

class ParallelBufferWithEndRegex : public ParallelBuffer {
 public:
  ParallelBufferWithEndRegex(size_t blocksize, std::string endRegex)
      : ParallelBuffer{blocksize}, _endRegex{endRegex} {}
  std::optional<std::vector<char>> getNextBlock() override {
    auto rawInput = _rawBuffer.getNextBlock();
    if (!rawInput || _exhausted) {
      _exhausted = true;
      if (_remainder.empty()) {
        return std::nullopt;
      }
      // make sure that the _remainder is really empty, b.c. this is important
      // for the logic above
      auto copy = std::move(_remainder);
      _remainder.clear();
      return copy;
    }

    // TODO:: is this really correct when we are at the end?
    auto endPosition = findRegexNearEnd(rawInput.value(), _endRegex);
    if (!endPosition) {
      if (_rawBuffer.getNextBlock()) {
        throw std::runtime_error(
            "The regex which is guaranteed to end a statement was not found at "
            "all within a single Batch. Please increase the FILE_BUFFER_SIZE "
            "or choose a different parser");
      }
      // this was the last (possibly incomplete) block, simply concatenate
      endPosition = rawInput->size();
      _exhausted = true;
    }
    std::vector<char> result;
    result.reserve(_remainder.size() + *endPosition);
    result.insert(result.end(), _remainder.begin(), _remainder.end());
    result.insert(result.end(), rawInput->begin(),
                  rawInput->begin() + *endPosition);
    _remainder.clear();
    _remainder.insert(_remainder.end(), rawInput->begin() + *endPosition,
                      rawInput->end());
    return result;
  }
  void open(const string& filename) override { _rawBuffer.open(filename); }

 private:
  static std::optional<size_t> findRegexNearEnd(const std::vector<char>& vec,
                                                const re2::RE2& regex) {
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

    return regexResult.data() + regexResult.size() - vec.data();
  }
  ParallelFileBuffer _rawBuffer{_blocksize};
  std::vector<char> _remainder;
  re2::RE2 _endRegex;
  bool _exhausted = false;
};
